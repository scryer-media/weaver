//! Tests for the direct-store coverage checkpoint (plan 135, D6).
//!
//! Fixture names are invented throughout — never real media titles.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use weaver_model::files::FileRole;

use super::barrier::{
    BARRIER_DIRTY_AGE, BARRIER_DIRTY_BYTES, BARRIER_FAILURE_BACKOFF, BARRIER_FAILURE_BACKOFF_MAX,
    BarrierDemand, BarrierDrain, BarrierError, BarrierStep, BarrierTrigger, CoverageBarrier,
    CoveragePersist, DatabaseCoveragePersist, DestinationSync, RoutedWrite, WriteRefused,
};
use super::plan::DirectSetPlan;
use super::restart::{
    CoverageRejection, DestinationProbe, ExpectedSet, ProbedDestination, complete_files,
    coverage_skip_plan, refetch_floors, restore_job, restore_set, restore_set_with_probe,
};
use super::router::{
    CrcRuns, DemotionReason, DirectSetRouter, HoldsScratch, SparseImage,
    restored_volume_is_confirmed,
};
use super::snapshot::{
    CoverageSnapshot, DestinationClaim, DestinationExtent, SNAPSHOT_MAGIC, SNAPSHOT_SCHEMA_VERSION,
    SnapshotError, VolumeFloor, decode, encode,
};
use super::{ByteRanges, DirectStoreGate, parse_enabled};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::{FileSpec, JobSpec, SegmentSpec};

const JOB: JobId = JobId(7701);
const SET: &str = "Silver.Horizon.S01E04";
const PLAN_DIGEST: [u8; 32] = [0xA5; 32];
const OTHER_DIGEST: [u8; 32] = [0x5A; 32];

// ---------------------------------------------------------------------------
// Recording test doubles. One struct implements all three barrier traits, so
// the recorded operation log is a single interleaved order.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Drain,
    Sync(String),
    Write { set_name: String, bytes: usize },
    Delete { set_name: String },
}

#[derive(Debug, Default)]
struct Journal {
    ops: Vec<Op>,
    fail_drain: Option<String>,
    fail_sync: Option<String>,
    fail_write: Option<String>,
    committed: Option<Vec<u8>>,
}

/// One shared journal behind all three barrier traits, so the recorded order is
/// a single interleaved log across drain, sync and persist.
#[derive(Debug, Clone, Default)]
struct Recorder {
    journal: Arc<Mutex<Journal>>,
}

impl Recorder {
    fn with<T>(&self, apply: impl FnOnce(&mut Journal) -> T) -> T {
        apply(&mut self.journal.lock().unwrap())
    }

    fn ops(&self) -> Vec<Op> {
        self.with(|journal| journal.ops.clone())
    }

    fn committed(&self) -> Option<Vec<u8>> {
        self.with(|journal| journal.committed.clone())
    }

    fn fail_drain(&self, error: &str) {
        self.with(|journal| journal.fail_drain = Some(error.to_string()));
    }

    fn fail_sync(&self, error: &str) {
        self.with(|journal| journal.fail_sync = Some(error.to_string()));
    }

    fn fail_write(&self, error: &str) {
        self.with(|journal| journal.fail_write = Some(error.to_string()));
    }

    fn writes(&self) -> usize {
        self.ops()
            .iter()
            .filter(|op| matches!(op, Op::Write { .. }))
            .count()
    }

    fn deletes(&self) -> usize {
        self.ops()
            .iter()
            .filter(|op| matches!(op, Op::Delete { .. }))
            .count()
    }

    fn synced(&self) -> Vec<String> {
        self.ops()
            .iter()
            .filter_map(|op| match op {
                Op::Sync(path) => Some(path.clone()),
                _ => None,
            })
            .collect()
    }

    fn steps(&self) -> Vec<&'static str> {
        self.ops()
            .iter()
            .map(|op| match op {
                Op::Drain => "drain",
                Op::Sync(_) => "sync",
                Op::Write { .. } => "write",
                Op::Delete { .. } => "delete",
            })
            .collect()
    }
}

impl BarrierDrain for Recorder {
    fn drain(&mut self) -> Result<(), String> {
        self.with(|journal| {
            if let Some(error) = journal.fail_drain.clone() {
                return Err(error);
            }
            journal.ops.push(Op::Drain);
            Ok(())
        })
    }
}

impl DestinationSync for Recorder {
    fn sync(&mut self, relative_path: &str) -> Result<(), String> {
        self.with(|journal| {
            if let Some(error) = journal.fail_sync.clone() {
                return Err(error);
            }
            journal.ops.push(Op::Sync(relative_path.to_string()));
            Ok(())
        })
    }
}

impl CoveragePersist for Recorder {
    fn write(&mut self, _job_id: JobId, set_name: &str, blob: &[u8]) -> Result<(), String> {
        self.with(|journal| {
            if let Some(error) = journal.fail_write.clone() {
                return Err(error);
            }
            journal.ops.push(Op::Write {
                set_name: set_name.to_string(),
                bytes: blob.len(),
            });
            journal.committed = Some(blob.to_vec());
            Ok(())
        })
    }

    fn delete(&mut self, _job_id: JobId, set_name: &str) -> Result<(), String> {
        self.with(|journal| {
            journal.ops.push(Op::Delete {
                set_name: set_name.to_string(),
            });
            journal.committed = None;
            Ok(())
        })
    }
}

/// Drives the barrier with one recorder standing in for all three traits.
fn run_barrier(
    barrier: &mut CoverageBarrier,
    recorder: &Recorder,
    trigger: BarrierTrigger,
) -> Result<super::barrier::BarrierReport, BarrierError> {
    run_barrier_at(barrier, recorder, trigger, Instant::now())
}

/// [`run_barrier`] on a synthetic clock, for the failure-backoff tests.
fn run_barrier_at(
    barrier: &mut CoverageBarrier,
    recorder: &Recorder,
    trigger: BarrierTrigger,
    now: Instant,
) -> Result<super::barrier::BarrierReport, BarrierError> {
    let (mut drain, mut sync, mut persist) = (recorder.clone(), recorder.clone(), recorder.clone());
    barrier.barrier(trigger, now, &mut drain, &mut sync, &mut persist)
}

fn write(volume_index: u32, source_offset: u64, len: u64, member_index: u32) -> RoutedWrite {
    RoutedWrite {
        volume_index,
        source_offset,
        len,
        member_index,
        destination_offset: source_offset,
    }
}

fn sample_barrier() -> CoverageBarrier {
    let mut barrier = CoverageBarrier::new(JOB, SET, PLAN_DIGEST);
    barrier.register_volume(0, 0);
    barrier.register_volume(1, 1);
    barrier.register_destination(0, "silver-horizon.mkv.direct.partial");
    barrier.register_destination(1, "silver-horizon.nfo.direct.partial");
    barrier
}

fn sample_snapshot() -> CoverageSnapshot {
    CoverageSnapshot {
        generation: 3,
        plan_digest: PLAN_DIGEST,
        destinations: vec![DestinationClaim {
            member_index: 0,
            relative_path: "silver-horizon.mkv.direct.partial".to_string(),
            extents: vec![DestinationExtent { start: 0, end: 60 }],
        }],
        floors: vec![VolumeFloor {
            volume_index: 0,
            file_index: 0,
            floor: 60,
            complete: false,
        }],
    }
}

/// The plan facts [`sample_snapshot`] was written against: one volume, mapped
/// to NZB file 0.
fn sample_expected() -> ExpectedSet {
    ExpectedSet {
        plan_digest: PLAN_DIGEST,
        volume_files: HashMap::from([(0u32, 0u32)]),
        fact_volumes: HashSet::from([0u32]),
    }
}

// ---------------------------------------------------------------------------
// Range set
// ---------------------------------------------------------------------------

#[test]
fn byte_ranges_coalesce_and_report_only_newly_covered_bytes() {
    let mut ranges = ByteRanges::new();
    assert_eq!(ranges.insert(0, 100), 100);
    assert_eq!(ranges.insert(200, 100), 100);
    assert_eq!(ranges.len(), 2);

    // Bridging the hole adds only the hole's bytes, never the overlap.
    assert_eq!(ranges.insert(50, 200), 100);
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges.ranges(), &[(0, 300)]);

    // A repaired span rewriting covered bytes is not new dirty work.
    assert_eq!(ranges.insert(10, 20), 0);
    assert_eq!(ranges.covered(), 300);
}

#[test]
fn byte_ranges_floor_is_contiguous_from_zero_only() {
    let mut ranges = ByteRanges::new();
    ranges.insert(1_000, 500);
    assert_eq!(
        ranges.contiguous_from_zero(),
        0,
        "coverage above a hole must not raise the floor"
    );
    ranges.insert(0, 1_000);
    assert_eq!(ranges.contiguous_from_zero(), 1_500);
}

#[test]
fn byte_ranges_survive_offsets_beyond_four_gibibytes() {
    let base = 5 * 1024 * 1024 * 1024u64;
    let mut ranges = ByteRanges::new();
    ranges.insert(base, 4 * 1024 * 1024 * 1024);
    assert_eq!(ranges.end(), base + 4 * 1024 * 1024 * 1024);
    assert_eq!(ranges.covered(), 4 * 1024 * 1024 * 1024);
}

#[test]
fn byte_ranges_trim_below_keeps_coverage_above_the_floor() {
    let mut ranges = ByteRanges::new();
    ranges.insert(0, 100);
    ranges.insert(500, 100);
    ranges.trim_below(100);
    assert_eq!(ranges.ranges(), &[(500, 600)]);
    assert_eq!(ranges.contiguous_from_zero(), 0);
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

#[test]
fn snapshot_round_trips_exactly() {
    let snapshot = CoverageSnapshot {
        generation: 12,
        plan_digest: PLAN_DIGEST,
        destinations: vec![
            DestinationClaim {
                member_index: 0,
                relative_path: "silver-horizon.mkv.direct.partial".to_string(),
                extents: vec![
                    DestinationExtent {
                        start: 0,
                        end: 4096,
                    },
                    DestinationExtent {
                        start: 8192,
                        end: 5 * 1024 * 1024 * 1024,
                    },
                ],
            },
            DestinationClaim {
                member_index: 4,
                relative_path: "silver-horizon.envelope".to_string(),
                extents: vec![DestinationExtent { start: 0, end: 17 }],
            },
        ],
        floors: vec![
            VolumeFloor {
                volume_index: 0,
                file_index: 3,
                floor: 4096,
                complete: false,
            },
            VolumeFloor {
                volume_index: 1,
                file_index: 4,
                floor: 6 * 1024 * 1024 * 1024,
                complete: false,
            },
        ],
    };

    let blob = encode(&snapshot).unwrap();
    assert_eq!(&blob[..4], &SNAPSHOT_MAGIC);
    assert_eq!(
        u16::from_le_bytes([blob[4], blob[5]]),
        SNAPSHOT_SCHEMA_VERSION
    );
    assert_eq!(decode(&blob).unwrap(), snapshot);
}

#[test]
fn snapshot_encoding_is_deterministic_regardless_of_input_order() {
    let ordered = sample_snapshot();
    let mut shuffled = ordered.clone();
    shuffled.destinations.push(DestinationClaim {
        member_index: 9,
        relative_path: "amber-circuit.envelope".to_string(),
        extents: vec![DestinationExtent { start: 0, end: 8 }],
    });
    shuffled.destinations.reverse();
    shuffled.floors.push(VolumeFloor {
        volume_index: 5,
        file_index: 5,
        floor: 11,
        complete: false,
    });
    shuffled.floors.reverse();

    let mut canonical = ordered;
    canonical.destinations.push(DestinationClaim {
        member_index: 9,
        relative_path: "amber-circuit.envelope".to_string(),
        extents: vec![DestinationExtent { start: 0, end: 8 }],
    });
    canonical.floors.push(VolumeFloor {
        volume_index: 5,
        file_index: 5,
        floor: 11,
        complete: false,
    });

    assert_eq!(encode(&shuffled).unwrap(), encode(&canonical).unwrap());
    assert_eq!(encode(&canonical).unwrap(), encode(&canonical).unwrap());
    assert_eq!(decode(&encode(&shuffled).unwrap()).unwrap(), canonical);
}

#[test]
fn two_thousand_volume_snapshot_round_trips_in_a_sane_blob() {
    let floors = (0..2_000u32)
        .map(|volume_index| VolumeFloor {
            volume_index,
            file_index: volume_index,
            floor: 50 * 1024 * 1024 * u64::from(volume_index + 1),
            complete: false,
        })
        .collect::<Vec<_>>();
    let snapshot = CoverageSnapshot {
        generation: 400,
        plan_digest: PLAN_DIGEST,
        destinations: vec![DestinationClaim {
            member_index: 0,
            relative_path: "silver-horizon.s01.mkv.direct.partial".to_string(),
            extents: vec![DestinationExtent {
                start: 0,
                end: 100 * 1024 * 1024 * 1024,
            }],
        }],
        floors,
    };

    let blob = encode(&snapshot).unwrap();
    assert!(
        blob.len() < 128 * 1024,
        "2 000 floors encoded to {} bytes, which is not a sane checkpoint row",
        blob.len()
    );
    assert_eq!(decode(&blob).unwrap(), snapshot);
}

#[test]
fn snapshot_decode_refuses_an_unknown_schema_version() {
    let mut blob = encode(&sample_snapshot()).unwrap();
    let future = SNAPSHOT_SCHEMA_VERSION + 1;
    blob[4..6].copy_from_slice(&future.to_le_bytes());

    assert_eq!(
        decode(&blob),
        Err(SnapshotError::UnsupportedVersion {
            found: future,
            supported: SNAPSHOT_SCHEMA_VERSION,
        }),
        "a newer writer's blob must be refused outright, never partially trusted"
    );
}

#[test]
fn snapshot_decode_refuses_a_bad_magic_and_a_truncated_frame() {
    let mut blob = encode(&sample_snapshot()).unwrap();
    blob[0] = b'X';
    assert_eq!(decode(&blob), Err(SnapshotError::BadMagic));

    assert_eq!(decode(b"WDS"), Err(SnapshotError::Truncated { len: 3 }));
    assert_eq!(decode(&[]), Err(SnapshotError::Truncated { len: 0 }));
}

#[test]
fn snapshot_decode_refuses_a_structurally_invalid_body() {
    // Bypass `encode`'s canonicalization to forge an out-of-order body.
    let forge = |snapshot: &CoverageSnapshot| {
        let mut blob = Vec::new();
        blob.extend_from_slice(&SNAPSHOT_MAGIC);
        blob.extend_from_slice(&SNAPSHOT_SCHEMA_VERSION.to_le_bytes());
        blob.extend_from_slice(&rmp_serde::to_vec(snapshot).unwrap());
        blob
    };

    let mut unsorted = sample_snapshot();
    unsorted.floors = vec![
        VolumeFloor {
            volume_index: 4,
            file_index: 4,
            floor: 10,
            complete: false,
        },
        VolumeFloor {
            volume_index: 1,
            file_index: 1,
            floor: 10,
            complete: false,
        },
    ];
    assert!(matches!(
        decode(&forge(&unsorted)),
        Err(SnapshotError::Malformed(_))
    ));

    let mut inverted = sample_snapshot();
    inverted.destinations[0].extents = vec![DestinationExtent { start: 90, end: 10 }];
    assert!(matches!(
        decode(&forge(&inverted)),
        Err(SnapshotError::Malformed(_))
    ));

    assert!(matches!(
        decode(&forge_garbage()),
        Err(SnapshotError::Malformed(_))
    ));
}

fn forge_garbage() -> Vec<u8> {
    let mut blob = Vec::new();
    blob.extend_from_slice(&SNAPSHOT_MAGIC);
    blob.extend_from_slice(&SNAPSHOT_SCHEMA_VERSION.to_le_bytes());
    blob.extend_from_slice(b"not messagepack at all");
    blob
}

#[test]
fn snapshot_decode_refuses_trailing_bytes_after_the_body() {
    let mut blob = encode(&sample_snapshot()).unwrap();
    let exact = blob.len();
    blob.extend_from_slice(b"\x00");
    assert!(
        matches!(decode(&blob), Err(SnapshotError::Malformed(_))),
        "a row that is not exactly one snapshot is not a snapshot"
    );

    blob.truncate(exact);
    blob.extend_from_slice(&encode(&sample_snapshot()).unwrap());
    assert!(
        matches!(decode(&blob), Err(SnapshotError::Malformed(_))),
        "two concatenated snapshots must not decode as the first one"
    );

    blob.truncate(exact);
    assert!(decode(&blob).is_ok(), "the exact body still decodes");
}

#[test]
fn snapshot_decode_refuses_a_destination_path_that_escapes_the_working_directory() {
    // Every claimed path is joined onto the working directory at restart, so a
    // path that escapes it would have restart probing — and phase 4 writing —
    // outside the job.
    for path in [
        "../../../../etc/hosts",
        "silver-horizon/../../outside.partial",
        "/etc/hosts",
        "",
        "silver-horizon\0.partial",
        "./",
    ] {
        let mut snapshot = sample_snapshot();
        snapshot.destinations[0].relative_path = path.to_string();
        let blob = encode(&snapshot).unwrap();
        assert!(
            matches!(decode(&blob), Err(SnapshotError::Malformed(_))),
            "destination path {path:?} must be refused at decode"
        );
    }

    let mut snapshot = sample_snapshot();
    snapshot.destinations[0].relative_path = "nested/dir/silver-horizon.mkv.direct.partial".into();
    let blob = encode(&snapshot).unwrap();
    assert!(
        decode(&blob).is_ok(),
        "an ordinary nested relative path is still fine"
    );
}

// ---------------------------------------------------------------------------
// Barrier triggers
// ---------------------------------------------------------------------------

#[test]
fn barrier_triggers_on_the_byte_threshold_under_sustained_feed() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    let chunk = 16 * 1024 * 1024u64;
    let mut offset = 0u64;

    while barrier.due(start).is_none() {
        barrier
            .record_write(&write(0, offset, chunk, 0), start)
            .unwrap();
        offset += chunk;
        assert!(offset <= BARRIER_DIRTY_BYTES + chunk, "trigger never fired");
    }

    assert_eq!(barrier.due(start), Some(BarrierTrigger::DirtyBytes));
    assert_eq!(barrier.dirty_bytes(), BARRIER_DIRTY_BYTES);
}

#[test]
fn barrier_triggers_on_the_timer_when_idle() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    barrier.record_write(&write(0, 0, 4_096, 0), start).unwrap();

    assert_eq!(barrier.due(start), None);
    assert_eq!(
        barrier.due(start + BARRIER_DIRTY_AGE - Duration::from_millis(1)),
        None
    );
    assert_eq!(
        barrier.due(start + BARRIER_DIRTY_AGE),
        Some(BarrierTrigger::DirtyAge),
        "an idle set must still checkpoint on the timer"
    );
}

#[test]
fn barrier_is_not_due_without_dirty_bytes() {
    let barrier = sample_barrier();
    let start = Instant::now();
    assert_eq!(barrier.due(start + Duration::from_secs(3_600)), None);
}

#[test]
fn rewritten_spans_do_not_re_dirty_the_set() {
    let mut barrier = sample_barrier();
    let now = Instant::now();
    assert_eq!(
        barrier.record_write(&write(0, 0, 1_000, 0), now).unwrap(),
        1_000
    );
    assert_eq!(
        barrier.record_write(&write(0, 0, 1_000, 0), now).unwrap(),
        0,
        "a repaired span overwriting covered bytes is not new unique work"
    );
    assert_eq!(barrier.dirty_bytes(), 1_000);
}

#[test]
fn out_of_order_writes_count_as_dirty_even_with_a_stalled_floor() {
    let mut barrier = sample_barrier();
    let now = Instant::now();
    barrier
        .record_write(&write(0, 4_096, 4_096, 0), now)
        .unwrap();

    assert_eq!(barrier.dirty_bytes(), 4_096);
    let recorder = Recorder::default();
    let report = run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyBytes).unwrap();
    assert_eq!(
        report.published_floors.get(&0),
        Some(&0),
        "a hole at offset zero must keep the floor stalled"
    );
}

// ---------------------------------------------------------------------------
// Refused writes
// ---------------------------------------------------------------------------

#[test]
fn a_write_for_an_unregistered_member_is_refused_whole() {
    let mut barrier = sample_barrier();
    let now = Instant::now();
    barrier.record_write(&write(0, 0, 4_096, 0), now).unwrap();
    let recorder = Recorder::default();
    run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyBytes).unwrap();
    let published = barrier.published_floors().clone();

    // Member 9 was never registered. The write carries no relative path, so
    // nothing here could register it, and half-recording it would advance the
    // source floor over bytes whose destination is never claimed or synced.
    let refused = barrier
        .try_record_write(&write(0, 4_096, 4_096, 9), now)
        .unwrap_err();

    assert_eq!(
        refused,
        WriteRefused::UnregisteredMember { member_index: 9 }
    );
    assert_eq!(
        barrier.dirty_bytes(),
        0,
        "a refused write must not make anything dirty"
    );
    assert!(
        barrier.touched_destinations().is_empty(),
        "a refused write must not mark anything for sync"
    );
    assert_eq!(barrier.published_floors(), &published);

    let recorder = Recorder::default();
    let report = run_barrier(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::Shutdown),
    )
    .unwrap();
    assert_eq!(
        report.published_floors.get(&0),
        Some(&4_096),
        "the refused span is refetched on restart, never claimed as covered"
    );
}

#[test]
fn a_write_for_an_unregistered_volume_is_refused_whole() {
    let mut barrier = sample_barrier();
    let now = Instant::now();

    let refused = barrier
        .try_record_write(&write(7, 0, 4_096, 0), now)
        .unwrap_err();

    assert_eq!(
        refused,
        WriteRefused::UnregisteredVolume { volume_index: 7 }
    );
    assert_eq!(barrier.dirty_bytes(), 0);
    assert!(barrier.touched_destinations().is_empty());

    let recorder = Recorder::default();
    let report = run_barrier(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::Finalization),
    )
    .unwrap();
    assert!(
        !report.published_floors.contains_key(&7),
        "an unregistered volume has no NZB file index, so it must never get a floor"
    );
}

// ---------------------------------------------------------------------------
// Barrier ordering and failure handling
// ---------------------------------------------------------------------------

#[test]
fn barrier_syncs_before_it_persists_and_publishes_last() {
    let mut barrier = sample_barrier();
    let now = Instant::now();
    barrier.record_write(&write(0, 0, 4_096, 0), now).unwrap();
    barrier.record_write(&write(1, 0, 512, 1), now).unwrap();

    let recorder = Recorder::default();
    let report = run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyBytes).unwrap();

    assert_eq!(
        recorder.steps(),
        vec!["drain", "sync", "sync", "write"],
        "the recorded trait calls must run drain, then every sync, then the persist"
    );
    assert_eq!(
        report.steps,
        vec![
            BarrierStep::Drain,
            BarrierStep::Sync,
            BarrierStep::Persist,
            BarrierStep::Publish,
        ],
        "floors are published only after the checkpoint is durable"
    );
    assert_eq!(report.generation, 1);
    assert_eq!(report.synced_destinations, 2);
    assert_eq!(barrier.published_floors().get(&0), Some(&4_096));
    assert_eq!(barrier.dirty_bytes(), 0);
    assert!(barrier.touched_destinations().is_empty());
}

#[test]
fn a_file_touched_early_in_an_interval_is_synced_by_that_intervals_barrier() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    // The .nfo member is written once at the very start of the interval and
    // never again; the .mkv member keeps taking writes right up to the barrier.
    barrier.record_write(&write(1, 0, 512, 1), start).unwrap();
    for index in 0..8u64 {
        barrier
            .record_write(
                &write(0, index * 4_096, 4_096, 0),
                start + Duration::from_millis(index),
            )
            .unwrap();
    }

    let recorder = Recorder::default();
    run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyAge).unwrap();

    assert_eq!(
        recorder.synced(),
        vec![
            "silver-horizon.mkv.direct.partial".to_string(),
            "silver-horizon.nfo.direct.partial".to_string(),
        ],
        "the published floors cover the whole interval, so every file the \
         interval touched must be synced — not only the final batch"
    );
}

#[test]
fn a_failed_barrier_keeps_the_touched_set_so_the_next_one_syncs_it() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    barrier.record_write(&write(1, 0, 512, 1), start).unwrap();

    let recorder = Recorder::default();
    recorder.fail_write("disk full");
    let error = run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyAge).unwrap_err();
    assert_eq!(error.step(), BarrierStep::Persist);

    // A different member takes the only write of the second interval.
    barrier
        .record_write(&write(0, 0, 4_096, 0), start + Duration::from_secs(1))
        .unwrap();

    let recorder = Recorder::default();
    run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyAge).unwrap();
    assert_eq!(
        recorder.synced(),
        vec![
            "silver-horizon.mkv.direct.partial".to_string(),
            "silver-horizon.nfo.direct.partial".to_string(),
        ],
        "a file touched before a failed barrier is still synced by the next \
         successful one"
    );
}

#[test]
fn a_drain_failure_leaves_the_previous_checkpoint_authoritative() {
    let (mut barrier, recorder) = barrier_with_one_committed_checkpoint();
    let baseline = recorder.committed().unwrap();
    barrier
        .record_write(&write(0, 4_096, 4_096, 0), Instant::now())
        .unwrap();

    recorder.fail_drain("write pool wedged");
    let error = run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyAge).unwrap_err();

    assert_eq!(error.step(), BarrierStep::Drain);
    assert_eq!(recorder.committed(), Some(baseline));
    assert_eq!(recorder.writes(), 1, "no second row was written");
    assert_eq!(
        recorder.deletes(),
        0,
        "the prior checkpoint was not retired"
    );
    assert_eq!(barrier.generation(), 1);
    assert_eq!(barrier.published_floors().get(&0), Some(&4_096));
    assert!(barrier.dirty_bytes() > 0, "transient state was not cleared");
}

#[test]
fn a_sync_failure_leaves_the_previous_checkpoint_authoritative() {
    let (mut barrier, recorder) = barrier_with_one_committed_checkpoint();
    let baseline = recorder.committed().unwrap();
    barrier
        .record_write(&write(0, 4_096, 4_096, 0), Instant::now())
        .unwrap();

    recorder.fail_sync("fsync failed");
    let error = run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyAge).unwrap_err();

    assert_eq!(error.step(), BarrierStep::Sync);
    assert_eq!(recorder.committed(), Some(baseline));
    assert_eq!(recorder.writes(), 1);
    assert_eq!(barrier.generation(), 1);
    assert_eq!(barrier.published_floors().get(&0), Some(&4_096));
}

#[test]
fn a_persist_failure_leaves_the_previous_checkpoint_authoritative() {
    let (mut barrier, recorder) = barrier_with_one_committed_checkpoint();
    let baseline = recorder.committed().unwrap();
    barrier
        .record_write(&write(0, 4_096, 4_096, 0), Instant::now())
        .unwrap();

    recorder.fail_write("transaction rolled back");
    let error = run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyAge).unwrap_err();

    assert_eq!(error.step(), BarrierStep::Persist);
    assert_eq!(recorder.committed(), Some(baseline));
    assert_eq!(barrier.generation(), 1);
    assert_eq!(
        barrier.published_floors().get(&0),
        Some(&4_096),
        "the floor from the last durable checkpoint stays published"
    );
}

/// Step 4 can only fail by dying: publish is pure in-memory bookkeeping, and
/// `DIRECT_STORE_BARRIER_PUBLISH` aborts the process. What a restart then sees
/// is the row step 3 committed, which is exactly the interval's floors — and
/// nothing that happened after it.
#[test]
fn a_crash_between_persist_and_publish_leaves_the_committed_checkpoint_authoritative() {
    let mut barrier = sample_barrier();
    let now = Instant::now();
    barrier.record_write(&write(0, 0, 8_192, 0), now).unwrap();
    let recorder = Recorder::default();
    run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyBytes).unwrap();

    // Writes keep landing after the committed barrier — the interval that the
    // crash throws away. Without these the test cannot tell a crash-resume
    // apart from a happy path, because both would show the same floors.
    barrier
        .record_write(&write(0, 8_192, 8_192, 0), now + Duration::from_millis(1))
        .unwrap();
    barrier
        .record_write(&write(1, 0, 512, 1), now + Duration::from_millis(2))
        .unwrap();
    assert_eq!(barrier.dirty_bytes(), 8_704);

    // The process dies before those bytes reach a barrier. Restart reads the
    // committed row instead.
    drop(barrier);
    let snapshot = decode(recorder.committed().unwrap().as_slice()).unwrap();
    let resumed = CoverageBarrier::resume(JOB, SET, &snapshot);

    assert_eq!(resumed.generation(), 1);
    assert_eq!(
        resumed.published_floors().get(&0),
        Some(&8_192),
        "the resumed floor is the committed one, not the one the lost writes would have raised"
    );
    assert_eq!(
        resumed.published_floors().get(&1),
        Some(&0),
        "a volume whose only bytes landed after the barrier resumes at floor zero"
    );
    let claims = |member_index: u32| {
        snapshot
            .destinations
            .iter()
            .find(|claim| claim.member_index == member_index)
            .map(|claim| claim.extents.clone())
    };
    assert_eq!(
        claims(0),
        Some(vec![DestinationExtent {
            start: 0,
            end: 8_192
        }]),
        "the committed claim stops where the barrier did"
    );
    assert_eq!(
        claims(1),
        None,
        "the second member was first written after the barrier, so it is not in the row at \
         all — a claim over zero bytes is omitted, because restart's destination probe would \
         otherwise refuse the row over a file that claims nothing and does not exist yet"
    );
    assert_eq!(
        resumed.dirty_bytes(),
        0,
        "resumed coverage is durable, not dirty"
    );
    assert!(
        resumed.touched_destinations().is_empty(),
        "the lost interval's sync set died with the process"
    );
}

// ---------------------------------------------------------------------------
// Failure backoff
// ---------------------------------------------------------------------------

#[test]
fn a_failed_barrier_backs_the_age_trigger_off_instead_of_busy_looping() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    barrier.record_write(&write(0, 0, 4_096, 0), start).unwrap();

    let recorder = Recorder::default();
    recorder.fail_write("transaction rolled back");

    let first_attempt = start + BARRIER_DIRTY_AGE;
    assert_eq!(barrier.due(first_attempt), Some(BarrierTrigger::DirtyAge));
    run_barrier_at(
        &mut barrier,
        &recorder,
        BarrierTrigger::DirtyAge,
        first_attempt,
    )
    .unwrap_err();

    assert_eq!(barrier.consecutive_failures(), 1);
    assert_eq!(
        barrier.cooldown_until(),
        Some(first_attempt + BARRIER_FAILURE_BACKOFF)
    );
    // The failed barrier kept its dirty bytes, so `dirty_since` is still old:
    // without the cooldown this arm would be due again on the very next poll,
    // and the caller would retry a failing transaction as fast as it can loop.
    assert_eq!(barrier.due(first_attempt + Duration::from_millis(1)), None);
    assert_eq!(
        barrier.due(first_attempt + BARRIER_FAILURE_BACKOFF - Duration::from_millis(1)),
        None
    );
    assert_eq!(
        barrier.due(first_attempt + BARRIER_FAILURE_BACKOFF),
        Some(BarrierTrigger::DirtyAge),
        "one attempt per cooldown, not one per poll"
    );

    let second_attempt = first_attempt + BARRIER_FAILURE_BACKOFF;
    run_barrier_at(
        &mut barrier,
        &recorder,
        BarrierTrigger::DirtyAge,
        second_attempt,
    )
    .unwrap_err();
    assert_eq!(barrier.consecutive_failures(), 2);
    assert_eq!(
        barrier.cooldown_until(),
        Some(second_attempt + 2 * BARRIER_FAILURE_BACKOFF),
        "each consecutive failure doubles the cooldown"
    );

    let mut attempt = second_attempt;
    for _ in 0..12 {
        attempt += Duration::from_secs(3_600);
        run_barrier_at(&mut barrier, &recorder, BarrierTrigger::DirtyAge, attempt).unwrap_err();
    }
    assert_eq!(
        barrier.cooldown_until(),
        Some(attempt + BARRIER_FAILURE_BACKOFF_MAX),
        "the doubling is capped, so a wedged set still retries every few minutes"
    );
    assert_eq!(recorder.writes(), 0, "no barrier ever committed");
}

#[test]
fn the_byte_threshold_is_not_damped_by_the_failure_cooldown() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    barrier.record_write(&write(0, 0, 4_096, 0), start).unwrap();
    let recorder = Recorder::default();
    recorder.fail_write("disk full");
    run_barrier_at(&mut barrier, &recorder, BarrierTrigger::DirtyAge, start).unwrap_err();
    assert_eq!(barrier.due(start), None, "the age trigger is in cooldown");

    let chunk = 16 * 1024 * 1024u64;
    let mut offset = 4_096u64;
    while barrier.dirty_bytes() < BARRIER_DIRTY_BYTES {
        barrier
            .record_write(&write(0, offset, chunk, 0), start)
            .unwrap();
        offset += chunk;
    }

    assert_eq!(
        barrier.due(start),
        Some(BarrierTrigger::DirtyBytes),
        "256 MiB of dirty bytes is too much work to sit on while a cooldown runs"
    );
}

#[test]
fn a_demand_is_served_during_the_cooldown_and_success_clears_it() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    barrier.record_write(&write(0, 0, 4_096, 0), start).unwrap();

    let failing = Recorder::default();
    let attempt = start + BARRIER_DIRTY_AGE;
    failing.fail_write("database is down");
    assert_eq!(barrier.due(attempt), Some(BarrierTrigger::DirtyAge));
    run_barrier_at(&mut barrier, &failing, BarrierTrigger::DirtyAge, attempt).unwrap_err();
    assert!(barrier.cooldown_until().is_some());

    // One second into a five-second cooldown, the server shuts down. The age
    // trigger is damped at that instant; the demand is not.
    let during = attempt + Duration::from_secs(1);
    assert_eq!(barrier.due(during), None);

    let recorder = Recorder::default();
    let report = run_barrier_at(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::Shutdown),
        during,
    )
    .unwrap();

    assert_eq!(recorder.writes(), 1, "a demanded barrier is never damped");
    assert_eq!(report.generation, 1);
    assert_eq!(
        barrier.consecutive_failures(),
        0,
        "success resets the failure count"
    );
    assert_eq!(
        barrier.cooldown_until(),
        None,
        "success clears the cooldown outright"
    );

    let resumed = during + Duration::from_secs(1);
    barrier
        .record_write(&write(0, 4_096, 4_096, 0), resumed)
        .unwrap();
    assert_eq!(
        barrier.due(resumed + BARRIER_DIRTY_AGE),
        Some(BarrierTrigger::DirtyAge),
        "the next interval's age trigger is eligible on its own schedule again"
    );
}

fn barrier_with_one_committed_checkpoint() -> (CoverageBarrier, Recorder) {
    let mut barrier = sample_barrier();
    let now = Instant::now();
    barrier.record_write(&write(0, 0, 4_096, 0), now).unwrap();
    let recorder = Recorder::default();
    run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyBytes).unwrap();
    assert_eq!(barrier.generation(), 1);
    (barrier, recorder)
}

#[test]
fn a_demanded_barrier_is_served_whatever_the_triggers_say() {
    let mut barrier = sample_barrier();
    barrier
        .record_write(&write(0, 0, 16, 0), Instant::now())
        .unwrap();
    let recorder = Recorder::default();

    let report = run_barrier(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::Shutdown),
    )
    .unwrap();
    assert_eq!(
        report.trigger,
        BarrierTrigger::Demand(BarrierDemand::Shutdown)
    );
    assert_eq!(recorder.writes(), 1);
}

#[test]
fn retiring_a_set_deletes_exactly_one_row() {
    let (mut barrier, recorder) = barrier_with_one_committed_checkpoint();
    barrier.retire(&mut recorder.clone()).unwrap();

    assert_eq!(recorder.deletes(), 1);
    assert_eq!(recorder.committed(), None);
    assert_eq!(barrier.generation(), 0);
    assert!(barrier.published_floors().is_empty());
}

#[test]
fn a_barrier_after_a_retire_claims_nothing_the_retired_set_claimed() {
    let (mut barrier, recorder) = barrier_with_one_committed_checkpoint();
    let start = Instant::now();
    barrier.record_write(&write(1, 0, 512, 1), start).unwrap();

    // Repair is about to rewrite those destination bytes, so the coverage goes
    // with the row.
    barrier.retire(&mut recorder.clone()).unwrap();

    assert_eq!(barrier.generation(), 0);
    assert!(barrier.published_floors().is_empty());
    assert_eq!(
        barrier.dirty_bytes(),
        0,
        "retired coverage is not dirty work"
    );
    assert!(barrier.touched_destinations().is_empty());
    assert_eq!(
        barrier.due(start + Duration::from_secs(3_600)),
        None,
        "a retired set has nothing left to checkpoint"
    );

    // Registration went with it too, so nothing can be recorded until the
    // caller rebuilds the routing.
    assert_eq!(
        barrier.try_record_write(&write(0, 0, 4_096, 0), start),
        Err(WriteRefused::UnregisteredMember { member_index: 0 })
    );

    let after = Recorder::default();
    run_barrier(
        &mut barrier,
        &after,
        BarrierTrigger::Demand(BarrierDemand::PhaseChange),
    )
    .unwrap();
    let snapshot = decode(after.committed().unwrap().as_slice()).unwrap();

    assert_eq!(
        snapshot.generation, 1,
        "the generation restarts with the coverage"
    );
    assert!(
        snapshot.destinations.is_empty(),
        "a barrier after a retire must never claim extents in files repair just rewrote"
    );
    assert!(
        snapshot.floors.is_empty(),
        "and it must never republish a floor the retired row derived"
    );
}

// ---------------------------------------------------------------------------
// One row per set, independent of volume count
// ---------------------------------------------------------------------------

fn barrier_with_volumes(volume_count: u32) -> CoverageBarrier {
    let mut barrier = CoverageBarrier::new(JOB, SET, PLAN_DIGEST);
    barrier.register_destination(0, "silver-horizon.s01.mkv.direct.partial");
    let now = Instant::now();
    for volume_index in 0..volume_count {
        barrier.register_volume(volume_index, volume_index);
        barrier
            .record_write(&write(volume_index, 0, 4_096, 0), now)
            .unwrap();
    }
    barrier
}

#[test]
fn a_barrier_issues_one_row_per_set_regardless_of_volume_count() {
    let mut small = barrier_with_volumes(2);
    let small_recorder = Recorder::default();
    let small_report =
        run_barrier(&mut small, &small_recorder, BarrierTrigger::DirtyBytes).unwrap();

    let mut large = barrier_with_volumes(2_000);
    let large_recorder = Recorder::default();
    let large_report =
        run_barrier(&mut large, &large_recorder, BarrierTrigger::DirtyBytes).unwrap();

    assert_eq!(small_recorder.writes(), 1);
    assert_eq!(
        large_recorder.writes(),
        small_recorder.writes(),
        "statement count must be independent of volume count"
    );
    assert_eq!(large_recorder.deletes(), 0);
    assert_eq!(
        large_recorder.steps(),
        small_recorder.steps(),
        "a 2 000-volume set must issue the same operations as a 2-volume one"
    );
    assert_eq!(small_report.published_floors.len(), 2);
    assert_eq!(large_report.published_floors.len(), 2_000);
    assert!(
        large_report.snapshot_bytes < 128 * 1024,
        "2 000-volume checkpoint row is {} bytes",
        large_report.snapshot_bytes
    );
}

#[test]
fn successive_barriers_advance_the_generation_and_replace_the_row() {
    let (mut barrier, recorder) = barrier_with_one_committed_checkpoint();
    barrier
        .record_write(&write(0, 4_096, 4_096, 0), Instant::now())
        .unwrap();
    let report = run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyAge).unwrap();

    assert_eq!(report.generation, 2);
    assert_eq!(recorder.writes(), 2, "two barriers, two replacing writes");
    let committed = decode(recorder.committed().unwrap().as_slice()).unwrap();
    assert_eq!(committed.generation, 2);
    assert_eq!(committed.floor_for_volume(0), Some(8_192));
}

// ---------------------------------------------------------------------------
// Restart
// ---------------------------------------------------------------------------

fn write_destination(dir: &Path, relative_path: &str, len: usize) {
    std::fs::write(dir.join(relative_path), vec![0u8; len]).unwrap();
}

#[tokio::test]
async fn restart_accepts_a_valid_row_and_yields_floors() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 60);
    let blob = encode(&sample_snapshot()).unwrap();

    let snapshot = restore_set(temp_dir.path(), &blob, &sample_expected())
        .await
        .unwrap();
    assert_eq!(snapshot.generation, 3);
    assert_eq!(refetch_floors(&snapshot), HashMap::from([(0u32, 60u64)]));
}

#[tokio::test]
async fn restart_accepts_a_destination_longer_than_the_claim() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 4_096);
    let blob = encode(&sample_snapshot()).unwrap();

    assert!(
        restore_set(temp_dir.path(), &blob, &sample_expected())
            .await
            .is_ok(),
        "file length never implies coverage: a longer file is expected, not truncated"
    );
}

#[tokio::test]
async fn restart_refuses_a_missing_destination() {
    let temp_dir = tempfile::tempdir().unwrap();
    let blob = encode(&sample_snapshot()).unwrap();

    assert_eq!(
        restore_set(temp_dir.path(), &blob, &sample_expected()).await,
        Err(CoverageRejection::MissingDestination {
            path: "silver-horizon.mkv.direct.partial".to_string(),
        })
    );
}

#[tokio::test]
async fn restart_refuses_a_short_destination() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 59);
    let blob = encode(&sample_snapshot()).unwrap();

    assert_eq!(
        restore_set(temp_dir.path(), &blob, &sample_expected()).await,
        Err(CoverageRejection::ShortDestination {
            path: "silver-horizon.mkv.direct.partial".to_string(),
            claimed: 60,
            actual: 59,
        })
    );
}

#[tokio::test]
async fn restart_refuses_a_plan_digest_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 60);
    let blob = encode(&sample_snapshot()).unwrap();

    let expected = ExpectedSet {
        plan_digest: OTHER_DIGEST,
        ..sample_expected()
    };
    assert_eq!(
        restore_set(temp_dir.path(), &blob, &expected).await,
        Err(CoverageRejection::PlanDigestMismatch),
        "a plan-digest mismatch is a hard stop, never partial trust"
    );
}

#[tokio::test]
async fn restart_refuses_a_checkpoint_whose_probe_never_completed() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 60);
    let blob = encode(&sample_snapshot()).unwrap();

    // The probe panics — a bug in it, or a runtime torn down under it during
    // startup. "Could not check" must never come out as "checked, fine".
    let rejection = restore_set_with_probe(temp_dir.path(), &blob, &sample_expected(), |_| {
        panic!("destination probe died");
    })
    .await
    .unwrap_err();

    assert!(
        matches!(rejection, CoverageRejection::ProbeFailed { .. }),
        "a failed probe must refuse the snapshot, got {rejection:?}"
    );
}

#[tokio::test]
async fn restart_refuses_a_probe_that_skipped_destinations() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 60);
    let blob = encode(&sample_snapshot()).unwrap();

    // A probe that answers for nothing would otherwise walk an empty loop and
    // accept a checkpoint having validated zero destinations.
    let rejection = restore_set_with_probe(temp_dir.path(), &blob, &sample_expected(), |_| {
        Vec::<ProbedDestination>::new()
    })
    .await
    .unwrap_err();

    assert!(
        matches!(rejection, CoverageRejection::ProbeFailed { .. }),
        "an incomplete probe is not an acceptance, got {rejection:?}"
    );
}

#[tokio::test]
async fn restart_probes_every_claimed_destination_under_the_working_directory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let blob = encode(&sample_snapshot()).unwrap();
    let expected_path = temp_dir.path().join("silver-horizon.mkv.direct.partial");

    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    restore_set_with_probe(temp_dir.path(), &blob, &sample_expected(), move |probes| {
        *recorded.lock().unwrap() = probes.clone();
        probes
            .into_iter()
            .map(|probe| ProbedDestination {
                relative_path: probe.relative_path,
                claimed: probe.claimed,
                actual: Some(probe.claimed),
            })
            .collect()
    })
    .await
    .unwrap();

    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[DestinationProbe {
            path: expected_path,
            relative_path: "silver-horizon.mkv.direct.partial".to_string(),
            claimed: 60,
        }]
    );
}

#[tokio::test]
async fn restart_refuses_a_flipped_file_index() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 60);
    let mut snapshot = sample_snapshot();
    snapshot.floors[0].file_index = 1;
    let blob = encode(&snapshot).unwrap();

    assert_eq!(
        restore_set(temp_dir.path(), &blob, &sample_expected()).await,
        Err(CoverageRejection::FileIndexMismatch {
            volume_index: 0,
            expected: Some(0),
            found: 1,
        }),
        "the plan owns the volume-to-file mapping: a flipped index would skip \
         another file's segments"
    );

    // A volume the plan does not have at all is the same refusal.
    let blob = encode(&sample_snapshot()).unwrap();
    let expected = ExpectedSet {
        volume_files: HashMap::new(),
        ..sample_expected()
    };
    assert_eq!(
        restore_set(temp_dir.path(), &blob, &expected).await,
        Err(CoverageRejection::FileIndexMismatch {
            volume_index: 0,
            expected: None,
            found: 0,
        })
    );
}

#[tokio::test]
async fn restart_refuses_a_zero_generation_row() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 60);
    let mut snapshot = sample_snapshot();
    snapshot.generation = 0;
    let blob = encode(&snapshot).unwrap();

    assert_eq!(
        restore_set(temp_dir.path(), &blob, &sample_expected()).await,
        Err(CoverageRejection::InvalidGeneration)
    );
}

#[tokio::test]
async fn restart_deletes_every_row_it_refuses() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 60);

    let rows = HashMap::from([
        (SET.to_string(), encode(&sample_snapshot()).unwrap()),
        (
            "Amber.Circuit".to_string(),
            encode(&sample_snapshot()).unwrap(),
        ),
        ("Copper.Meridian".to_string(), vec![b'j', b'u', b'n', b'k']),
    ]);
    let expected = HashMap::from([
        (SET.to_string(), sample_expected()),
        ("Copper.Meridian".to_string(), sample_expected()),
    ]);

    let recorder = Recorder::default();
    let outcome = restore_job(
        DirectStoreGate::Enabled,
        JOB,
        temp_dir.path(),
        rows,
        &expected,
        &mut recorder.clone(),
    )
    .await;

    assert_eq!(outcome.accepted.len(), 1);
    assert!(outcome.accepted.contains_key(SET));
    assert_eq!(outcome.rejected.len(), 2);
    assert_eq!(
        outcome.rejected,
        vec![
            ("Amber.Circuit".to_string(), CoverageRejection::UnknownSet),
            (
                "Copper.Meridian".to_string(),
                CoverageRejection::Decode(SnapshotError::Truncated { len: 4 })
            ),
        ]
    );
    assert_eq!(recorder.deletes(), 2);
    assert_eq!(recorder.writes(), 0, "restart never writes a checkpoint");
}

#[tokio::test]
async fn a_disabled_gate_ignores_rows_without_deleting_them() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rows = HashMap::from([(SET.to_string(), encode(&sample_snapshot()).unwrap())]);
    let expected = HashMap::from([(SET.to_string(), sample_expected())]);

    let recorder = Recorder::default();
    let outcome = restore_job(
        DirectStoreGate::Disabled,
        JOB,
        temp_dir.path(),
        rows,
        &expected,
        &mut recorder.clone(),
    )
    .await;

    assert!(outcome.accepted.is_empty());
    assert!(outcome.rejected.is_empty());
    assert_eq!(outcome.ignored, 1);
    assert!(
        recorder.ops().is_empty(),
        "a disabled gate must tolerate existing rows, not destroy them"
    );
}

// ---------------------------------------------------------------------------
// Floor to segment derivation
// ---------------------------------------------------------------------------

fn direct_job_spec() -> JobSpec {
    JobSpec {
        name: "Silver Horizon".to_string(),
        password: None,
        total_bytes: 60,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "silver-horizon.part01.rar".to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                SegmentSpec {
                    ordinal: 0,
                    article_number: 1,
                    bytes: 10,
                    message_id: "one@example.invalid".to_string(),
                },
                SegmentSpec {
                    ordinal: 1,
                    article_number: 2,
                    bytes: 20,
                    message_id: "two@example.invalid".to_string(),
                },
                SegmentSpec {
                    ordinal: 2,
                    article_number: 3,
                    bytes: 30,
                    message_id: "three@example.invalid".to_string(),
                },
            ],
        }],
    }
}

fn segment(segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: 0,
        },
        segment_number,
    }
}

#[test]
fn coverage_skip_plan_skips_only_whole_segments_below_the_floor() {
    let spec = direct_job_spec();
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::from([(0u32, 30u64)]), &HashSet::new());

    assert_eq!(plan.skip.len(), 2);
    assert!(plan.skip.contains(&segment(0)));
    assert!(plan.skip.contains(&segment(1)));
    assert_eq!(plan.file_progress.get(&0), Some(&30));
}

#[test]
fn coverage_skip_plan_does_not_skip_a_partial_segment() {
    let spec = direct_job_spec();
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::from([(0u32, 25u64)]), &HashSet::new());

    assert_eq!(plan.skip, [segment(0)].into_iter().collect());
    assert_eq!(plan.file_progress.get(&0), Some(&10));
}

#[test]
fn coverage_skip_plan_never_consults_destination_length() {
    // No file exists anywhere: for a direct set the source volume never does.
    // The legacy path would clamp to `metadata.len()` and zero this floor.
    let spec = direct_job_spec();
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::from([(0u32, 60u64)]), &HashSet::new());

    assert_eq!(plan.skip.len(), 3);
    assert_eq!(plan.file_progress.get(&0), Some(&60));
}

#[test]
fn coverage_skip_plan_leaves_unlisted_files_alone() {
    let spec = direct_job_spec();
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::new(), &HashSet::new());
    assert!(plan.skip.is_empty());
    assert!(plan.file_progress.is_empty());
}

#[test]
fn refetch_floors_take_the_lowest_floor_for_a_repeated_file_index() {
    let mut snapshot = sample_snapshot();
    snapshot.floors = vec![
        VolumeFloor {
            volume_index: 0,
            file_index: 0,
            floor: 900,
            complete: false,
        },
        VolumeFloor {
            volume_index: 1,
            file_index: 0,
            floor: 100,
            complete: false,
        },
    ];
    assert_eq!(refetch_floors(&snapshot), HashMap::from([(0u32, 100u64)]));
}

// ---------------------------------------------------------------------------
// The real database seam
// ---------------------------------------------------------------------------

fn direct_active_job() -> crate::ActiveJob {
    crate::ActiveJob {
        job_id: JOB,
        nzb_hash: [0xAA; 32],
        nzb_path: std::path::PathBuf::from("/tmp/silver-horizon.nzb"),
        nzb_zstd: crate::ingest::compress_nzb_bytes(
            br#"<?xml version="1.0" encoding="UTF-8"?>
            <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
              <file poster="poster" date="1700000000" subject="sample">
                <groups><group>alt.binaries.test</group></groups>
                <segments>
                  <segment bytes="10" number="1">abc@example.invalid</segment>
                </segments>
              </file>
            </nzb>"#,
        )
        .unwrap(),
        output_dir: std::path::PathBuf::from("/tmp/silver-horizon"),
        created_at: 1_700_000_000,
        category: None,
        metadata: vec![],
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
    }
}

/// The barrier's persist step against a real database rather than a test
/// double. The codec's blob-size test proves something about memory; this
/// proves the same blob survives the round trip it will actually make —
/// encode, one replaced row, read back, decode, every floor intact.
#[test]
fn a_two_thousand_floor_checkpoint_round_trips_through_the_database() {
    let database = crate::Database::open_in_memory().unwrap();
    database.create_active_job(&direct_active_job()).unwrap();
    let mut persist = DatabaseCoveragePersist::new(database.clone());

    let mut barrier = CoverageBarrier::new(JOB, SET, PLAN_DIGEST);
    barrier.register_destination(0, "silver-horizon.s01.mkv.direct.partial");
    let now = Instant::now();
    for volume_index in 0..2_000u32 {
        barrier.register_volume(volume_index, volume_index);
        barrier
            .record_write(
                &RoutedWrite {
                    volume_index,
                    source_offset: 0,
                    len: 50 * 1024 * 1024,
                    member_index: 0,
                    destination_offset: 50 * 1024 * 1024 * u64::from(volume_index),
                },
                now,
            )
            .unwrap();
    }

    let recorder = Recorder::default();
    let (mut drain, mut sync) = (recorder.clone(), recorder.clone());
    let report = barrier
        .barrier(
            BarrierTrigger::Demand(BarrierDemand::Shutdown),
            now,
            &mut drain,
            &mut sync,
            &mut persist,
        )
        .unwrap();
    assert_eq!(report.published_floors.len(), 2_000);

    let rows = database.load_direct_coverage(JOB).unwrap();
    assert_eq!(rows.len(), 1, "one row per set, whatever the volume count");
    let blob = &rows[SET];
    assert_eq!(
        blob.len(),
        report.snapshot_bytes,
        "the stored row is byte for byte the blob the barrier encoded"
    );

    let snapshot = decode(blob).unwrap();
    assert_eq!(snapshot.generation, 1);
    assert_eq!(snapshot.floors.len(), 2_000);
    for volume_index in 0..2_000u32 {
        assert_eq!(
            snapshot.floor_for_volume(volume_index),
            Some(50 * 1024 * 1024),
            "volume {volume_index} lost its floor in the database"
        );
        assert_eq!(
            snapshot.floors[volume_index as usize].file_index,
            volume_index
        );
    }
    assert_eq!(
        snapshot.destinations[0].claimed_len(),
        50 * 1024 * 1024 * 2_000,
        "the destination claim spans every volume's extent"
    );

    // A second barrier replaces the row rather than appending to it.
    barrier
        .record_write(
            &RoutedWrite {
                volume_index: 0,
                source_offset: 50 * 1024 * 1024,
                len: 4_096,
                member_index: 0,
                destination_offset: 100 * 1024 * 1024 * 1024,
            },
            now,
        )
        .unwrap();
    barrier
        .barrier(
            BarrierTrigger::Demand(BarrierDemand::Shutdown),
            now,
            &mut drain,
            &mut sync,
            &mut persist,
        )
        .unwrap();
    let rows = database.load_direct_coverage(JOB).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(decode(&rows[SET]).unwrap().generation, 2);

    barrier.retire(&mut persist).unwrap();
    assert!(
        database.load_direct_coverage(JOB).unwrap().is_empty(),
        "retiring the set deletes its row"
    );
}

// ---------------------------------------------------------------------------
// Gate
// ---------------------------------------------------------------------------

#[test]
fn the_gate_defaults_off_and_only_explicit_on_words_enable_it() {
    assert!(!parse_enabled(None), "phase 3 ships the gate off");
    assert!(!parse_enabled(Some("")));
    assert!(!parse_enabled(Some("0")));
    assert!(!parse_enabled(Some("off")));
    assert!(!parse_enabled(Some("maybe")));
    assert!(parse_enabled(Some("1")));
    assert!(parse_enabled(Some(" TRUE ")));
    assert!(parse_enabled(Some("on")));
    assert!(parse_enabled(Some("yes")));
}

// ---------------------------------------------------------------------------
// Router internals: the pieces every routed byte passes through
// ---------------------------------------------------------------------------

#[test]
fn crc_runs_compose_neighbours_and_ignore_an_overlapping_re_insert() {
    let payload: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let whole = weaver_par2::checksum::crc32(&payload);

    // Fed out of order — the tail, then the head, then the middle that joins
    // them — which is the only order the router ever guarantees.
    let mut runs = CrcRuns::default();
    runs.insert(400, 200, weaver_par2::checksum::crc32(&payload[400..600]));
    assert_eq!(runs.compose(0, 600), None, "a gap is not a composition");
    runs.insert(0, 100, weaver_par2::checksum::crc32(&payload[..100]));
    assert_eq!(runs.compose(0, 600), None);
    runs.insert(100, 300, weaver_par2::checksum::crc32(&payload[100..400]));

    assert_eq!(
        runs.compose(0, 600),
        Some(whole),
        "adjacent runs compose to the whole-space CRC32"
    );
}

#[test]
fn crc_runs_compose_any_sub_range_that_lands_on_run_boundaries() {
    // M3: a merged-only composition could answer for the whole space and
    // nothing else, so a covered range that stopped short of it — a held tail,
    // a volume that stopped mid-download — was reconstructed with no reference
    // value at all. Every prefix and interior span a coverage map can name for
    // wholly routed articles has to compose.
    let payload: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let mut runs = CrcRuns::default();
    for (start, len) in [(0usize, 100usize), (100, 300), (400, 200)] {
        runs.insert(
            start as u64,
            len as u64,
            weaver_par2::checksum::crc32(&payload[start..start + len]),
        );
    }

    for (start, len) in [
        (0usize, 100usize),
        (0, 400),
        (0, 600),
        (100, 300),
        (100, 500),
        (400, 200),
    ] {
        assert_eq!(
            runs.compose(start as u64, len as u64),
            Some(weaver_par2::checksum::crc32(&payload[start..start + len])),
            "the sub-range at {start}+{len} lands on run boundaries and must compose"
        );
    }

    // A range that starts or ends inside a run has no composed value, and
    // "no value" is a refusal everywhere it is read — never a pass.
    for (start, len) in [(50u64, 100u64), (0, 350), (150, 100), (0, 700)] {
        assert_eq!(
            runs.compose(start, len),
            None,
            "the sub-range at {start}+{len} cuts a run and must not compose"
        );
    }
    assert_eq!(runs.compose(0, 0), None, "an empty range is not a checksum");
}

#[test]
fn crc_runs_refuse_every_shape_of_overlap() {
    let base = |start: u64, len: u64| {
        let mut runs = CrcRuns::default();
        runs.insert(100, 100, 0xAAAA_AAAA);
        runs.insert(start, len, 0xBBBB_BBBB);
        runs
    };
    let untouched = base(0, 0);

    // Straddling the front, straddling the back, wholly inside, wholly
    // containing, and exactly duplicate: none may advance the composition.
    for (start, len) in [(50, 100), (150, 100), (120, 10), (50, 200), (100, 100)] {
        assert_eq!(
            base(start, len),
            untouched,
            "an overlapping run at {start}+{len} must be ignored"
        );
    }

    // Butting up against either edge is not an overlap, and must compose.
    assert!(base(200, 50).compose(100, 150).is_some());
    assert!(base(50, 50).compose(50, 150).is_some());

    // A zero-length run is a no-op, not an insertion at offset zero.
    assert_eq!(base(0, 0), untouched);
}

#[test]
fn a_sparse_image_reads_across_run_boundaries_and_stops_at_every_hole() {
    use std::io::{Read, Seek, SeekFrom};

    let mut chunks: std::collections::BTreeMap<u64, std::sync::Arc<[u8]>> =
        std::collections::BTreeMap::new();
    chunks.insert(0u64, std::sync::Arc::from(&[1u8, 2, 3, 4][..]));
    chunks.insert(4u64, std::sync::Arc::from(&[5u8, 6][..]));
    chunks.insert(16u64, std::sync::Arc::from(&[9u8, 9, 9][..]));
    let mut image = SparseImage::from_chunks(&chunks);

    // Adjacent runs are still separate runs: a read stops at the boundary and
    // the next read continues, which is exactly what `read_exact` loops on.
    let mut out = [0u8; 6];
    let first = image.read(&mut out).unwrap();
    assert_eq!(first, 4);
    let second = image.read(&mut out[first..]).unwrap();
    assert_eq!(second, 2);
    assert_eq!(out, [1, 2, 3, 4, 5, 6]);

    // The hole reads as EOF, which the header walk turns into a clean stop.
    assert_eq!(image.read(&mut out).unwrap(), 0);

    // Seeking over a data area and landing inside a later run is the whole
    // point: that is how a second member's header is reached.
    assert_eq!(image.seek(SeekFrom::Start(17)).unwrap(), 17);
    let mut tail = [0u8; 4];
    assert_eq!(image.read(&mut tail).unwrap(), 2);
    assert_eq!(&tail[..2], &[9, 9]);

    // Seeks past everything staged succeed and read as EOF.
    assert_eq!(image.seek(SeekFrom::Start(1 << 40)).unwrap(), 1 << 40);
    assert_eq!(image.read(&mut tail).unwrap(), 0);

    // Relative seeks are supported; end-relative ones are refused, because the
    // image has no end — the last staged run is wherever the last article
    // happened to reach.
    image.seek(SeekFrom::Start(2)).unwrap();
    assert_eq!(image.seek(SeekFrom::Current(2)).unwrap(), 4);
    assert_eq!(image.seek(SeekFrom::Current(-3)).unwrap(), 1);
    let refused = image.seek(SeekFrom::End(0)).unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::Unsupported);
}

fn envelope_plan() -> DirectSetPlan {
    DirectSetPlan {
        set_name: "Silver.Horizon.S01E05".to_string(),
        volumes: [(0u32, 0u32), (1, 1)].into_iter().collect(),
        files: [(0u32, 0u32), (1, 1)].into_iter().collect(),
        working_dir: std::path::PathBuf::from("/tmp/silver-horizon"),
    }
}

/// Envelope v2 replaces phase 4's `envelope_offsets_split_each_volume_slot…`
/// test, which asserted a 64 KiB half-slot layout that no longer exists: there
/// is no slot arithmetic to overflow, because a byte's envelope offset *is* its
/// physical offset in the volume.
#[test]
fn each_volume_owns_a_separate_sparse_envelope_file() {
    let plan = envelope_plan();

    assert_eq!(
        plan.envelope_relative_path(0),
        "Silver.Horizon.S01E05.vol00000.envelope"
    );
    assert_eq!(
        plan.envelope_relative_path(7),
        "Silver.Horizon.S01E05.vol00007.envelope",
        "zero padding keeps a lexical listing of a 2 000-volume set in volume order"
    );
    assert_ne!(
        plan.envelope_relative_path(0),
        plan.envelope_relative_path(1),
        "two volumes must never share an envelope file — the offsets inside one \
         are physical, so they would collide byte for byte"
    );
    assert_eq!(
        plan.envelope_paths(),
        vec![plan.envelope_path(0), plan.envelope_path(1)],
        "the set owns exactly one envelope per planned volume"
    );

    // The checkpoint blob carries destination paths and revalidates them at
    // restart with the RAR member-path validator, so an envelope name that the
    // validator refuses would make every barrier unreadable.
    for volume_index in [0u32, 7, u32::MAX] {
        assert!(
            crate::pipeline::extraction::validate_sanitized_rar_member_path(
                &plan.envelope_relative_path(volume_index)
            )
            .is_ok(),
            "envelope paths must survive the same validator the snapshot codec applies"
        );
    }
}

#[test]
fn envelope_destination_keys_count_down_from_the_top_of_the_member_space() {
    use super::set::envelope_destination_key;

    assert_eq!(envelope_destination_key(0), u32::MAX);
    assert_eq!(envelope_destination_key(1), u32::MAX - 1);
    assert_ne!(envelope_destination_key(0), envelope_destination_key(1));
    // Member ids are handed out from zero upwards, so the two bands only meet
    // at an unreachable set size. The barrier keys destinations by this number
    // and a collision would silently merge a member's claim with an envelope's.
    assert!(
        envelope_destination_key(2_000) > 2_000,
        "a 2 000-volume set's envelope keys must stay clear of its member ids"
    );
}

#[test]
fn byte_ranges_report_exactly_the_sub_ranges_they_do_not_cover() {
    let mut ranges = ByteRanges::new();
    ranges.insert(100, 100);
    ranges.insert(300, 100);

    // Wholly covered, wholly missing, and every partial straddle.
    assert_eq!(ranges.missing(120, 40), vec![]);
    assert_eq!(ranges.missing(200, 100), vec![(200, 300)]);
    assert_eq!(ranges.missing(50, 100), vec![(50, 100)]);
    assert_eq!(ranges.missing(150, 100), vec![(200, 250)]);
    assert_eq!(
        ranges.missing(0, 500),
        vec![(0, 100), (200, 300), (400, 500)]
    );

    // Degenerate windows contribute nothing rather than a zero-length gap.
    assert_eq!(ranges.missing(120, 0), vec![]);
    assert_eq!(ranges.missing(u64::MAX, 2), vec![]);

    // An empty set is missing everything asked of it.
    assert_eq!(ByteRanges::new().missing(7, 3), vec![(7, 10)]);
}

#[test]
fn member_partials_keep_their_directory_and_hostile_names_are_refused() {
    let plan = envelope_plan();

    assert_eq!(
        plan.member_partial_path("Silver.Horizon.S01E05.mkv"),
        Ok("Silver.Horizon.S01E05.mkv.direct.partial".to_string())
    );
    // The directory component survives: the partial lives beside where the
    // member will land, not flattened into the working directory root.
    assert_eq!(
        plan.member_partial_path("Silver.Horizon/S01E05.mkv"),
        Ok("Silver.Horizon/S01E05.mkv.direct.partial".to_string())
    );
    // A backslash names a directory on every platform: Windows treats it as a
    // separator, and everywhere else the same rewrite the extractor applies to
    // its own destinations turns it into one. Both sides agree, which is what
    // lets the partial be renamed onto the member's output path.
    assert_eq!(
        plan.member_partial_path("Silver.Horizon\\S01E05.mkv"),
        Ok("Silver.Horizon/S01E05.mkv.direct.partial".to_string())
    );
    assert_eq!(
        plan.member_partial_path("./nested/./S01E05.mkv"),
        Ok("nested/S01E05.mkv.direct.partial".to_string())
    );

    // Wave 1 runs a raw header name through `weaver_unrar::sanitize_path` before
    // the validator, which is what the incremental extractor does — D3's
    // "sanitize-don't-reject" rule. A traversal is therefore *stripped* rather
    // than refused, exactly as the extractor strips it, and only a name that
    // sanitizes to nothing at all has no destination. The invariant that matters
    // is unchanged: whatever comes out is confined to the working directory.
    for hostile in ["../escape.mkv", "/absolute.mkv", "a/../../escape.mkv"] {
        let resolved = plan
            .member_partial_path(hostile)
            .unwrap_or_else(|()| panic!("{hostile} sanitizes to a usable name"));
        let path = std::path::Path::new(&resolved);
        assert!(
            !path.is_absolute()
                && path
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_))),
            "{hostile} resolved to {resolved}, which is not confined to the working directory"
        );
    }
    for empty in ["", ".", "./"] {
        assert!(
            plan.member_partial_path(empty).is_err(),
            "{empty} names nothing at all and must not resolve to a destination"
        );
    }

    // Two members whose sanitized paths collide are an archive the extractor
    // refuses outright, so the key that decides it has to agree with the key
    // `ensure_unique_sanitized_rar_member_paths` folds.
    assert_eq!(
        DirectSetPlan::member_collision_key("./Silver.Horizon.nfo"),
        DirectSetPlan::member_collision_key("SILVER.HORIZON.NFO"),
        "the collision key is the sanitized path, case-folded"
    );
    assert_ne!(
        DirectSetPlan::member_collision_key("Silver.Horizon.nfo"),
        DirectSetPlan::member_collision_key("Silver.Horizon/S01E05.mkv")
    );
}

#[test]
fn destination_names_stay_inside_the_filename_ceiling_with_their_suffix() {
    // Both suffixes are appended to a string an NZB supplies, so both could push
    // the component past what the filesystem accepts — and the failure was a
    // demotion on the set's first routed byte, reported as
    // `DestinationWriteFailed`, which says nothing about the name (nit).
    let limit = weaver_model::files::DOWNLOAD_FILENAME_MAX_BYTES;
    let long = "S".repeat(400);
    let plan = DirectSetPlan {
        set_name: long.clone(),
        volumes: [(0u32, 0u32), (7, 7)].into_iter().collect(),
        files: [(0u32, 0u32), (7, 7)].into_iter().collect(),
        working_dir: std::path::PathBuf::from("/tmp/silver-horizon"),
    };

    for volume_index in [0u32, 7, u32::MAX] {
        let envelope = plan.envelope_relative_path(volume_index);
        assert!(
            envelope.len() <= limit,
            "envelope {volume_index} is {} bytes: {envelope}",
            envelope.len()
        );
        assert!(
            envelope.ends_with(".envelope") && envelope.contains(&format!(".vol{volume_index:05}")),
            "the clamp shortens the stem and keeps the whole suffix, got {envelope}"
        );
    }
    // Two volumes of a long-named set must still name two different files.
    assert_ne!(
        plan.envelope_relative_path(0),
        plan.envelope_relative_path(7)
    );

    let partial = plan
        .member_partial_path(&format!("Silver.Horizon/{long}.mkv"))
        .expect("a long member name resolves");
    let (parent, name) = partial.rsplit_once('/').expect("the directory survives");
    assert_eq!(parent, "Silver.Horizon");
    assert!(
        name.len() <= limit && name.ends_with(".direct.partial"),
        "only the last component is clamped, and the suffix survives whole: {name}"
    );
}

#[test]
fn retiring_a_set_that_never_built_a_barrier_still_deletes_its_row() {
    // A set can be resumed from a checkpoint written before a restart and then
    // demote before its layout names a member again — `FormatMismatch` and
    // `UnparsableVolume` both land there. That is exactly the case where the
    // row exists and the in-memory controller does not, so the delete cannot be
    // conditional on the controller (M1).
    let mut set = super::set::DirectSet::new(JOB, envelope_plan());
    let recorder = Recorder::default();
    let mut persist = recorder.clone();

    set.retire(&mut persist).unwrap();

    assert_eq!(
        recorder.ops(),
        vec![Op::Delete {
            set_name: "Silver.Horizon.S01E05".to_string()
        }],
        "the row is deleted by (job, set name) whether or not a barrier exists"
    );
}

#[test]
fn a_finalized_set_refuses_to_be_demoted() {
    // The two terminal states are mutually exclusive, and finalization is the
    // one that already renamed members onto their destinations. Demoting after
    // it would delete completed output (B5).
    let mut set = super::set::DirectSet::new(JOB, envelope_plan());
    set.mark_finalized();
    set.demote(super::router::DemotionReason::HoldsBudgetExceeded);

    assert!(set.is_finalized());
    assert!(!set.is_demoted());
}

// ---------------------------------------------------------------------------
// The hybrid virtual-volume provider (plan 135, phase 5 wave 1)
// ---------------------------------------------------------------------------

/// A hand-built virtual volume over one envelope and two member partials.
///
/// The physical layout is deliberately the awkward one: header, member A, a gap
/// of envelope, member B, trailer — so a whole-volume read crosses four
/// destination boundaries in both directions.
struct ProviderFixture {
    _dir: tempfile::TempDir,
    volume: super::provider::VirtualVolume,
    /// The bytes a conventionally downloaded volume would have held.
    conventional: Vec<u8>,
}

const PROVIDER_HEADER: usize = 40;
const PROVIDER_MEMBER_A: usize = 300;
const PROVIDER_GAP: usize = 24;
const PROVIDER_MEMBER_B: usize = 180;
const PROVIDER_TRAILER: usize = 16;

/// The physical ranges this fixture's envelope file actually received: the
/// non-member regions, clipped to what was covered.
///
/// Derived from the fixture's own layout rather than from the `extents` the
/// [`super::provider::VirtualVolume`] is given, because the failure the provider
/// has to survive is exactly an extent going missing — a map derived from the
/// extents would hand the missing member's range straight back to the envelope.
fn provider_envelope_covered(covered: &ByteRanges) -> ByteRanges {
    let member_a_at = PROVIDER_HEADER as u64;
    let member_b_at = (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP) as u64;
    let mut envelope = ByteRanges::new();
    for (start, len) in [
        (0u64, PROVIDER_HEADER as u64),
        (member_a_at + PROVIDER_MEMBER_A as u64, PROVIDER_GAP as u64),
        (
            member_b_at + PROVIDER_MEMBER_B as u64,
            PROVIDER_TRAILER as u64,
        ),
    ] {
        let end = start + len;
        for &(covered_start, covered_end) in covered.ranges() {
            let overlap_start = covered_start.max(start);
            let overlap_end = covered_end.min(end);
            if overlap_start < overlap_end {
                envelope.insert(overlap_start, overlap_end - overlap_start);
            }
        }
    }
    envelope
}

fn provider_fixture(covered: ByteRanges) -> ProviderFixture {
    provider_fixture_with_extents(covered, true)
}

/// `with_extents == false` builds the volume the router used to hand the
/// provider once a routed member turned ineligible: the bytes are covered, the
/// partial still holds them, and the extent that says so is gone.
fn provider_fixture_with_extents(covered: ByteRanges, with_extents: bool) -> ProviderFixture {
    use std::io::{Seek, SeekFrom, Write};

    let dir = tempfile::tempdir().unwrap();
    let total =
        PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B + PROVIDER_TRAILER;
    // Distinct per offset, so a read that returns the *wrong* file's bytes at
    // the right length still fails.
    let conventional: Vec<u8> = (0..total).map(|index| (index % 251) as u8).collect();

    let member_a_at = PROVIDER_HEADER;
    let member_b_at = PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP;

    // The envelope is a sparse image of the volume: every non-member byte at its
    // true physical offset, holes where the members were routed away.
    let envelope = dir.path().join("silver.horizon.vol00000.envelope");
    let mut file = std::fs::File::create(&envelope).unwrap();
    for (offset, len) in [
        (0usize, PROVIDER_HEADER),
        (member_a_at + PROVIDER_MEMBER_A, PROVIDER_GAP),
        (member_b_at + PROVIDER_MEMBER_B, PROVIDER_TRAILER),
    ] {
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(&conventional[offset..offset + len]).unwrap();
    }
    drop(file);

    let partial_a = dir.path().join("Silver.Horizon.S01E01.mkv.direct.partial");
    std::fs::write(
        &partial_a,
        &conventional[member_a_at..member_a_at + PROVIDER_MEMBER_A],
    )
    .unwrap();
    let partial_b = dir.path().join("Silver.Horizon.S01E01.nfo.direct.partial");
    std::fs::write(
        &partial_b,
        &conventional[member_b_at..member_b_at + PROVIDER_MEMBER_B],
    )
    .unwrap();

    let extents = if with_extents {
        vec![
            super::router::MemberExtent {
                member_id: 0,
                physical_offset: member_a_at as u64,
                logical_offset: 0,
                len: PROVIDER_MEMBER_A as u64,
            },
            super::router::MemberExtent {
                member_id: 1,
                physical_offset: member_b_at as u64,
                logical_offset: 0,
                len: PROVIDER_MEMBER_B as u64,
            },
        ]
    } else {
        Vec::new()
    };
    let envelope_covered = provider_envelope_covered(&covered);

    ProviderFixture {
        volume: super::provider::VirtualVolume {
            volume_index: 0,
            envelope,
            extents,
            partials: std::sync::Arc::new(
                [(0u32, partial_a), (1u32, partial_b)].into_iter().collect(),
            ),
            covered,
            envelope_covered,
            len: total as u64,
        },
        _dir: dir,
        conventional,
    }
}

fn whole_volume_covered() -> ByteRanges {
    let mut covered = ByteRanges::new();
    covered.insert(
        0,
        (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B + PROVIDER_TRAILER)
            as u64,
    );
    covered
}

#[test]
fn a_virtual_volume_reads_back_exactly_what_a_downloaded_volume_would_hold() {
    use std::io::{Read, Seek};

    let fixture = provider_fixture(whole_volume_covered());
    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut reader = provider.open(0).expect("the volume is registered");
    assert_eq!(reader.len(), fixture.conventional.len() as u64);

    // A sequential sweep crosses envelope -> member A -> envelope -> member B ->
    // envelope. `read_to_end` loops over the short reads each boundary produces.
    let mut sequential = Vec::new();
    reader.read_to_end(&mut sequential).unwrap();
    assert_eq!(
        sequential, fixture.conventional,
        "a whole-volume sequential read must be byte-identical to the real volume"
    );

    // Every read that straddles a boundary, in one-byte steps around it, using
    // `read_exact` — which is what the RAR header walk and PAR2 both use.
    let boundaries = [
        PROVIDER_HEADER,
        PROVIDER_HEADER + PROVIDER_MEMBER_A,
        PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP,
        PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B,
    ];
    for boundary in boundaries {
        for span in [1usize, 2, 8, 33] {
            let start = boundary - span;
            // The last boundary is the volume's own tail, so clamp rather than
            // asking for bytes past the end — that is a different test.
            let len = (span * 2).min(fixture.conventional.len() - start);
            let mut reader = provider.open(0).unwrap();
            reader.seek(std::io::SeekFrom::Start(start as u64)).unwrap();
            let mut out = vec![0u8; len];
            reader.read_exact(&mut out).unwrap();
            assert_eq!(
                out,
                &fixture.conventional[start..start + len],
                "a read straddling the boundary at {boundary} by {span} bytes must be exact"
            );
        }
    }
}

#[test]
fn a_virtual_volume_seeks_the_way_a_file_does() {
    use std::io::{Read, Seek, SeekFrom};

    let fixture = provider_fixture(whole_volume_covered());
    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut reader = provider.open(0).unwrap();
    let total = fixture.conventional.len() as u64;

    assert_eq!(reader.seek(SeekFrom::Start(100)).unwrap(), 100);
    assert_eq!(reader.seek(SeekFrom::Current(50)).unwrap(), 150);
    assert_eq!(reader.seek(SeekFrom::Current(-100)).unwrap(), 50);
    // Unlike the router's in-memory image, a virtual volume has a real length,
    // so `End` means what it does on a file — the whole reason PAR2's readers
    // can use it at all.
    assert_eq!(reader.seek(SeekFrom::End(0)).unwrap(), total);
    assert_eq!(reader.read(&mut [0u8; 8]).unwrap(), 0, "EOF is EOF");
    assert_eq!(reader.seek(SeekFrom::End(-4)).unwrap(), total - 4);
    let mut tail = [0u8; 4];
    reader.read_exact(&mut tail).unwrap();
    assert_eq!(tail, fixture.conventional[fixture.conventional.len() - 4..]);
}

#[test]
fn a_virtual_volume_reports_a_hole_rather_than_inventing_zeros() {
    use std::io::{Read, Seek, SeekFrom};

    // The failure this is named for, constructed exactly (B1): every byte of the
    // volume is covered, the member partials hold their bytes, and the extent
    // list that says which file owns them is **empty** — the shape the provider
    // was handed once a routed member's eligibility flipped at chain close and
    // `map_physical_range` stopped calling its packed range a member.
    //
    // The envelope file is 560 bytes long and sparse from 40 to 340, so a plain
    // read there succeeds and returns zeros. Nothing downstream can tell those
    // from data: reconstruction would write them into a volume file under a
    // published floor and the articles would never be fetched again.
    let fixture = provider_fixture_with_extents(whole_volume_covered(), false);
    let member_a_at = PROVIDER_HEADER as u64;
    assert!(
        std::fs::metadata(&fixture.volume.envelope).unwrap().len()
            > member_a_at + PROVIDER_MEMBER_A as u64,
        "the envelope must be long enough to answer the member's offsets with zeros"
    );
    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut reader = provider.open(0).unwrap();
    reader.seek(SeekFrom::Start(member_a_at + 100)).unwrap();
    let error = reader
        .read(&mut [0u8; 16])
        .expect_err("a covered range no source backs must not read as data");
    assert!(
        super::provider::is_hole(&error),
        "a covered range with no extent must report a hole, got {error}"
    );

    // The header before it still reads: the envelope answers for what the
    // envelope was actually written, and only that.
    let mut reader = provider.open(0).unwrap();
    let mut head = vec![0u8; PROVIDER_HEADER];
    reader.read_exact(&mut head).unwrap();
    assert_eq!(head, &fixture.conventional[..PROVIDER_HEADER]);
    assert!(super::provider::is_hole(
        &reader.read(&mut [0u8; 16]).unwrap_err()
    ));

    // And with the extent restored the same bytes read back as the real volume,
    // so the refusal above is about the missing extent and nothing else.
    let honest = provider_fixture(whole_volume_covered());
    let provider = super::provider::HybridVolumeProvider::new(vec![honest.volume.clone()]);
    let mut reader = provider.open(0).unwrap();
    let mut whole = Vec::new();
    reader.read_to_end(&mut whole).unwrap();
    assert_eq!(whole, honest.conventional);
}

#[test]
fn a_virtual_volume_reports_an_uncovered_range_as_a_hole() {
    use std::io::{Read, Seek, SeekFrom};

    // Everything but a window inside member A and a window inside the trailer.
    let mut covered = ByteRanges::new();
    let hole_start = (PROVIDER_HEADER + 100) as u64;
    let hole_end = (PROVIDER_HEADER + 200) as u64;
    covered.insert(0, hole_start);
    covered.insert(
        hole_end,
        (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B) as u64 - hole_end,
    );
    let fixture = provider_fixture(covered);
    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);

    // A read starting inside the hole fails, and fails *distinguishably*: a
    // caller has to be able to tell "not downloaded" from "the disk is broken".
    let mut reader = provider.open(0).unwrap();
    reader.seek(SeekFrom::Start(hole_start + 10)).unwrap();
    let error = reader.read(&mut [0u8; 16]).unwrap_err();
    assert!(
        super::provider::is_hole(&error),
        "a read inside a hole must report a hole, got {error}"
    );

    // A read that *runs into* the hole returns the bytes before it and then
    // fails, rather than silently stopping short as if at EOF.
    let mut reader = provider.open(0).unwrap();
    reader.seek(SeekFrom::Start(hole_start - 8)).unwrap();
    let mut out = vec![0u8; 32];
    let read = reader.read(&mut out).unwrap();
    assert_eq!(read, 8, "the read stops at the edge of the hole");
    assert_eq!(
        &out[..8],
        &fixture.conventional[hole_start as usize - 8..hole_start as usize]
    );
    let error = reader.read(&mut out).unwrap_err();
    assert!(super::provider::is_hole(&error));

    // The trailer was never covered either, so the volume's own tail is a hole
    // even though the file it would come from exists and is long enough.
    let mut reader = provider.open(0).unwrap();
    reader
        .seek(SeekFrom::Start(
            (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B) as u64,
        ))
        .unwrap();
    assert!(super::provider::is_hole(
        &reader.read(&mut out).unwrap_err()
    ));

    // And a plain I/O failure is *not* a hole, so the two never get confused.
    let missing = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
    assert!(!super::provider::is_hole(&missing));
}

#[test]
fn a_deleted_partial_reads_as_a_hole_not_as_an_infrastructure_failure() {
    use std::io::{Read, Seek, SeekFrom};

    let fixture = provider_fixture(whole_volume_covered());
    let partial = fixture
        .volume
        .partials
        .get(&1)
        .expect("member 1 has a partial")
        .clone();
    std::fs::remove_file(&partial).unwrap();

    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut reader = provider.open(0).unwrap();
    reader
        .seek(SeekFrom::Start(
            (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP) as u64,
        ))
        .unwrap();
    let error = reader.read(&mut [0u8; 16]).unwrap_err();
    assert!(
        super::provider::is_hole(&error),
        "a destination that is not there holds no bytes, which is what a hole is"
    );

    // The rest of the volume still reads: one missing partial does not poison
    // the envelope or its sibling.
    let mut reader = provider.open(0).unwrap();
    let mut head = vec![0u8; PROVIDER_HEADER + PROVIDER_MEMBER_A];
    reader.read_exact(&mut head).unwrap();
    assert_eq!(head, &fixture.conventional[..head.len()]);
}

#[test]
fn the_volume_provider_trait_refuses_a_volume_the_set_does_not_have() {
    use weaver_unrar::VolumeProvider;

    let fixture = provider_fixture(whole_volume_covered());
    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    assert!(provider.get_volume(0).is_ok());
    assert!(
        provider.get_volume(1).is_err(),
        "an unregistered volume is unavailable, not an empty one"
    );
    assert!(provider.volume(0).is_some());
    assert!(provider.volume(9).is_none());
}

// ---------------------------------------------------------------------------
// D8's reconstruction sweep, and the verification that gates it
// ---------------------------------------------------------------------------

/// One article per 100 bytes of the fixture volume, which is the granularity the
/// coverage map's boundaries actually fall on.
fn provider_article_crcs(conventional: &[u8]) -> CrcRuns {
    let mut runs = CrcRuns::default();
    let mut offset = 0usize;
    while offset < conventional.len() {
        let end = (offset + 100).min(conventional.len());
        runs.insert(
            offset as u64,
            (end - offset) as u64,
            weaver_par2::checksum::crc32(&conventional[offset..end]),
        );
        offset = end;
    }
    runs
}

fn reconstruction_target(
    fixture: &ProviderFixture,
    path: std::path::PathBuf,
    covered: ByteRanges,
    crcs: CrcRuns,
) -> super::reconstruct::VolumeReconstruction {
    super::reconstruct::VolumeReconstruction {
        volume_index: 0,
        path,
        len: fixture.conventional.len() as u64,
        assembly_complete: false,
        covered,
        crcs,
    }
}

#[test]
fn a_partially_covered_volume_is_rebuilt_and_verified_run_by_run() {
    // M3: the composition is over the runs it was fed, so a covered prefix that
    // stops short of the whole volume still has a reference value. Before that,
    // only a range exactly equal to a merged run had one — which a partial
    // volume never is — and the sweep wrote it with nothing checking it.
    let fixture = provider_fixture(whole_volume_covered());
    let crcs = provider_article_crcs(&fixture.conventional);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");

    // A prefix that crosses the header into member A, and a second run above a
    // hole — both on article boundaries, neither equal to the whole volume.
    let mut covered = ByteRanges::new();
    covered.insert(0, 200);
    covered.insert(300, 100);

    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let rebuilt = super::reconstruct::reconstruct_volumes(
        &provider,
        &[reconstruction_target(
            &fixture,
            path.clone(),
            covered,
            crcs.clone(),
        )],
    )
    .expect("every covered run composes a reference and matches it");

    assert_eq!(rebuilt[0].contiguous, 200, "the floor stops at the hole");
    assert!(!rebuilt[0].complete);
    let written = std::fs::read(&path).unwrap();
    assert_eq!(
        &written[..200],
        &fixture.conventional[..200],
        "the covered prefix is byte-exact"
    );
    assert_eq!(
        &written[300..400],
        &fixture.conventional[300..400],
        "so is the covered run above the hole"
    );
    assert!(
        written[200..300].iter().all(|byte| *byte == 0),
        "the hole is left for the refetch to fill"
    );
}

#[test]
fn a_covered_run_with_no_composed_reference_refuses_to_be_rebuilt() {
    // B1(c): "verify where available" is the wrong default for a sweep that
    // reads through sparse files. A run with no reference value is refused, and
    // the fallback refetches — which is always correct — rather than putting
    // bytes nothing checked under a published floor.
    let fixture = provider_fixture(whole_volume_covered());
    let crcs = provider_article_crcs(&fixture.conventional);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");

    // Ends inside an article, so no composition can vouch for it.
    let mut covered = ByteRanges::new();
    covered.insert(0, 250);

    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let failure = super::reconstruct::reconstruct_volumes(
        &provider,
        &[reconstruction_target(&fixture, path.clone(), covered, crcs)],
    )
    .expect_err("a run with no reference must not be written");
    assert_eq!(
        failure,
        super::reconstruct::ReconstructionFailure::UnverifiableRun {
            volume_index: 0,
            offset: 0,
        }
    );
    assert!(
        !path.exists(),
        "a refused reconstruction leaves nothing for the refetch to write around"
    );
}

#[test]
fn a_rebuilt_run_that_fails_its_reference_falls_back_to_refetching() {
    let fixture = provider_fixture(whole_volume_covered());
    // Corrupt one member byte after the CRCs were taken, which is exactly what a
    // partial that came back wrong — or an envelope answering a member's offsets
    // with zeros — looks like from here.
    let mut corrupted = fixture.conventional.clone();
    corrupted[PROVIDER_HEADER + 10] ^= 0xFF;
    let crcs = provider_article_crcs(&corrupted);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");

    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let failure = super::reconstruct::reconstruct_volumes(
        &provider,
        &[reconstruction_target(
            &fixture,
            path.clone(),
            whole_volume_covered(),
            crcs,
        )],
    )
    .expect_err("a run that disagrees with its composed CRC32 must not be trusted");
    assert!(matches!(
        failure,
        super::reconstruct::ReconstructionFailure::ChecksumMismatch {
            volume_index: 0,
            ..
        }
    ));
    assert!(!path.exists());
}

#[test]
fn a_rebuild_truncates_a_stale_file_already_sitting_at_the_volumes_path() {
    // An interrupted earlier attempt can leave a longer file where the volume
    // goes. Its tail sits above everything the sweep writes and would be read as
    // the volume's own bytes (nit).
    let fixture = provider_fixture(whole_volume_covered());
    let crcs = provider_article_crcs(&fixture.conventional);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");
    std::fs::write(&path, vec![0xEEu8; fixture.conventional.len() + 4096]).unwrap();

    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    super::reconstruct::reconstruct_volumes(
        &provider,
        &[reconstruction_target(
            &fixture,
            path.clone(),
            whole_volume_covered(),
            crcs,
        )],
    )
    .expect("the volume is wholly covered and verifiable");

    assert_eq!(
        std::fs::read(&path).unwrap(),
        fixture.conventional,
        "the rebuilt volume is the volume, with no stale tail past its end"
    );
}

// ---------------------------------------------------------------------------
// The PAR2 `FileAccess` adapter over virtual volumes (D5)
// ---------------------------------------------------------------------------

use super::par2_access::{DirectVolumeFileAccess, VirtualPar2Volume};
use weaver_par2::FileAccess;

/// A PAR2 set describing one file with **descriptions only** — no IFSC packet,
/// so no slice checksums.
///
/// That is the shape D5 names: with no per-slice data the verifier falls back to
/// a whole-file MD5, which is the read that degrades into thousands of ranged
/// reads across member partials unless the adapter offers a real sequential
/// reader.
fn descriptor_only_par2_set(filename: &str, bytes: &[u8]) -> weaver_par2::Par2FileSet {
    let file_id = weaver_par2::FileId::from_bytes([7u8; 16]);
    weaver_par2::Par2FileSet {
        recovery_set_id: weaver_par2::RecoverySetId::from_bytes([3; 16]),
        slice_size: 64,
        recovery_file_ids: vec![file_id],
        non_recovery_file_ids: Vec::new(),
        files: HashMap::from([(
            file_id,
            weaver_par2::FileDescription {
                file_id,
                hash_full: weaver_par2::checksum::md5(bytes),
                hash_16k: weaver_par2::checksum::md5(&bytes[..bytes.len().min(16 * 1024)]),
                length: bytes.len() as u64,
                par2_name: filename.to_string(),
                filename: filename.to_string(),
            },
        )]),
        slice_checksums: HashMap::new(),
        recovery_slices: std::collections::BTreeMap::new(),
        creator: None,
    }
}

/// The adapter under test, over the provider fixture's single virtual volume.
fn virtual_file_access(
    fixture: &ProviderFixture,
    par2_set: &weaver_par2::Par2FileSet,
    base_dir: &Path,
) -> (DirectVolumeFileAccess, weaver_par2::FileId) {
    let file_id = par2_set.recovery_file_ids[0];
    let inner = weaver_par2::PlacementFileAccess::new(
        base_dir.to_path_buf(),
        par2_set,
        std::collections::HashMap::new(),
    );
    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    (
        DirectVolumeFileAccess::new(
            inner,
            provider,
            &[VirtualPar2Volume {
                par2_file_id: file_id,
                volume_index: fixture.volume.volume_index,
            }],
        ),
        file_id,
    )
}

#[test]
fn a_no_ifsc_whole_file_md5_streams_through_the_sequential_reader() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = provider_fixture(whole_volume_covered());
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());
    let counters = access.counters();

    assert!(
        weaver_par2::verify_full_hash(&par2_set, &file_id, &access)
            .expect("the description names a registered virtual volume"),
        "the whole-file MD5 of a virtual volume must match the volume the \
         conventional gate would have written"
    );
    assert!(
        counters.sequential_opens() > 0,
        "the whole-file MD5 must stream through `open_sequential_reader`"
    );
    assert_eq!(
        counters.ranged_reads(),
        0,
        "D5 requires the sequential path so a no-IFSC set does not degrade into \
         ranged reads across member partials"
    );
}

#[test]
fn the_adapter_answers_existence_length_and_ranges_like_a_downloaded_volume() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = provider_fixture(whole_volume_covered());
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());

    assert!(access.file_exists(&file_id));
    assert_eq!(
        access.file_length(&file_id),
        Some(fixture.conventional.len() as u64)
    );
    // A range that crosses the header, a member extent, the envelope gap and the
    // second member — every source the overlay has — in one call.
    let range = access
        .read_file_range(&file_id, 8, (fixture.conventional.len() - 12) as u64)
        .expect("a covered range reads");
    assert_eq!(
        range,
        fixture.conventional[8..fixture.conventional.len() - 4],
        "a ranged read must reassemble the source volume across every backing file"
    );
    assert!(
        access
            .read_file_range(&file_id, 0, 1)
            .is_ok_and(|bytes| bytes == fixture.conventional[..1]),
        "the first byte comes from the envelope"
    );
}

#[test]
fn a_hole_reads_as_a_short_file_and_never_as_zeros() {
    let dir = tempfile::tempdir().unwrap();
    // Everything below the second member is covered; the tail is not.
    let mut covered = ByteRanges::new();
    let hole_at = (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP) as u64;
    covered.insert(0, hole_at);
    let fixture = provider_fixture(covered);
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());

    // The length is still the volume's — coverage says what is *there*, not how
    // long the volume is — so a short read is what tells the pass the file is
    // damaged, exactly as a truncated volume file would.
    assert_eq!(
        access.file_length(&file_id),
        Some(fixture.conventional.len() as u64)
    );
    let read = access
        .read_file_range(&file_id, 0, fixture.conventional.len() as u64)
        .expect("a read that starts inside coverage succeeds");
    assert_eq!(
        read.len() as u64,
        hole_at,
        "the read must stop at the hole rather than fabricating the rest"
    );
    assert_eq!(
        read,
        fixture.conventional[..hole_at as usize],
        "and the bytes it did return must be the volume's"
    );
    assert!(
        !weaver_par2::verify_full_hash(&par2_set, &file_id, &access).unwrap_or(true),
        "a volume with a hole must not verify"
    );
}

#[test]
fn an_interior_hole_refuses_the_sequential_reader_rather_than_lying_through_it() {
    use std::io::Read;

    let dir = tempfile::tempdir().unwrap();
    // A hole in the middle of member A, with the whole rest of the volume —
    // member A's tail, the envelope gap, member B and the trailer — present.
    let hole_at = (PROVIDER_HEADER + 64) as u64;
    let hole_len = 32u64;
    let total =
        (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B + PROVIDER_TRAILER)
            as u64;
    let mut covered = ByteRanges::new();
    covered.insert(0, hole_at);
    covered.insert(hole_at + hole_len, total - hole_at - hole_len);
    let fixture = provider_fixture(covered);
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());

    // The underlying stream really does stop at the first hole — a `Read` has no
    // way to say "skip 32 bytes, then resume" — so every byte after an interior
    // gap is unreachable through it even though both the partial and the
    // envelope still hold those bytes.
    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut streamed = Vec::new();
    let mut reader = provider.open(0).expect("the volume is registered");
    let _ = reader.read_to_end(&mut streamed);
    assert_eq!(
        streamed.len() as u64,
        hole_at,
        "the sequential sweep stops at the first hole; it cannot skip it"
    );

    // Which is why the adapter does not offer one. Wave 2 did, and every sweep
    // that consumed it — the no-IFSC whole-file MD5, PAR2's batched slice pass —
    // saw a file ending at the first hole and reported every slice after an
    // interior gap damaged, intact bytes included. Wave 2 only produced a
    // verdict, so the cost was a demotion that refetched slightly more than it
    // had to; phase 6 sizes a *repair* from that same count, and a repair sized
    // from "damaged" rather than "absent" spends recovery blocks rebuilding good
    // slices — enough of them and a repairable set reads as unrepairable.
    assert!(
        access
            .open_sequential_reader(&file_id)
            .expect("the volume is registered")
            .is_none(),
        "the adapter must refuse the sequential reader for an interior hole and \
         let the caller fall back to the ranged, per-slice path"
    );
    assert_eq!(access.counters().sequential_refusals(), 1);

    // Addressed directly, the bytes past the gap are all there — which is what
    // makes the paragraph above a property of the *stream*, not of the data.
    let after = access
        .read_file_range(&file_id, hole_at + hole_len, 128)
        .expect("a read that starts inside coverage succeeds");
    assert_eq!(
        after,
        fixture.conventional[(hole_at + hole_len) as usize..(hole_at + hole_len) as usize + 128],
        "the bytes after an interior hole are present and readable when addressed"
    );

    let hole_slice = (hole_at + hole_len) / par2_set.slice_size;
    assert!(
        hole_slice + 1 < total / par2_set.slice_size,
        "the fixture must have slices *after* the interior hole for that claim to bite"
    );
}

#[test]
fn a_write_to_a_virtual_volume_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = provider_fixture(whole_volume_covered());
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (mut access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());

    // Repair over a virtual volume is phase 6. Wave 2 must fail loudly rather
    // than write a recovered slice into a file the set does not own.
    let error = access
        .write_file_range(&file_id, 0, b"repaired")
        .expect_err("a virtual volume has nowhere to put a repaired slice");
    assert!(
        error.to_string().contains("virtual"),
        "the refusal should say why, got {error}"
    );
}

// ---------------------------------------------------------------------------
// Restart primitives (plan 135, D6) — the pieces the end-to-end restart tests
// exercise only in combination, where a wrong answer in one can be masked by
// another. B1 and B2 both hid here.
// ---------------------------------------------------------------------------

fn floor_entry(volume_index: u32, file_index: u32, floor: u64, complete: bool) -> VolumeFloor {
    VolumeFloor {
        volume_index,
        file_index,
        floor,
        complete,
    }
}

#[test]
fn complete_files_names_only_the_files_every_entry_agrees_are_complete() {
    let mut snapshot = sample_snapshot();
    snapshot.floors = vec![
        floor_entry(0, 0, 900, true),
        floor_entry(1, 1, 100, false),
        floor_entry(2, 2, 500, true),
    ];
    assert_eq!(complete_files(&snapshot), HashSet::from([0u32, 2]));

    // A malformed blob repeating a file index is resolved the safe way: one
    // entry saying "not complete" refutes the file, whatever the others claim.
    // Anything else would skip segments on the strength of the entry that
    // happened to be read last.
    snapshot.floors = vec![
        floor_entry(0, 0, 900, true),
        floor_entry(1, 0, 100, false),
        floor_entry(2, 2, 500, true),
    ];
    assert_eq!(complete_files(&snapshot), HashSet::from([2u32]));
}

#[test]
fn coverage_skip_plan_skips_every_segment_of_a_complete_file() {
    let spec = direct_job_spec();
    // The floor is deliberately far below the file: a complete volume's floor
    // counts *decoded* bytes while the spec's segment sizes are yEnc-encoded, so
    // the two never meet and the `complete` bit is the only thing that can say
    // the file is finished.
    let plan = coverage_skip_plan(
        JOB,
        &spec,
        &HashMap::from([(0u32, 30u64)]),
        &HashSet::from([0u32]),
    );

    assert_eq!(
        plan.skip,
        [segment(0), segment(1), segment(2)].into_iter().collect(),
        "a complete file skips every segment, not only the ones under its floor"
    );
    assert_eq!(
        plan.file_progress.get(&0),
        Some(&60),
        "and its progress is the file's whole declared size"
    );

    // Without the bit the very same floor skips only what it covers. This is the
    // difference B1 turned into a zombie: the bit is worth three segments here
    // and worth the whole job's correctness when it is wrong.
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::from([(0u32, 30u64)]), &HashSet::new());
    assert_eq!(plan.skip.len(), 2);
}

#[tokio::test]
async fn restart_refuses_a_row_claiming_a_volume_the_layout_has_no_facts_for() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_destination(temp_dir.path(), "silver-horizon.mkv.direct.partial", 60);
    let blob = encode(&sample_snapshot()).unwrap();

    // M5. The layout rebuild is tolerant of a missing volume — it contributes no
    // member, so the plan digest is unchanged and every other check passes — and
    // the set then cannot classify a byte of it.
    let expected = ExpectedSet {
        fact_volumes: HashSet::new(),
        ..sample_expected()
    };
    assert_eq!(
        restore_set(temp_dir.path(), &blob, &expected).await,
        Err(CoverageRejection::UnclassifiableVolume { volume_index: 0 }),
        "a row claiming coverage in a volume with no cached facts must be refused, not \
         accepted into a set that can never place those bytes"
    );

    // A volume the row claims *nothing* in is not a reason to retire the row:
    // refusing on it would redownload sets whose later volumes had simply not
    // started.
    let mut unstarted = sample_snapshot();
    unstarted.floors = vec![floor_entry(0, 0, 0, false)];
    let blob = encode(&unstarted).unwrap();
    assert!(
        restore_set(temp_dir.path(), &blob, &expected).await.is_ok(),
        "a zero floor with no completion claims nothing and must not refuse the row"
    );
}

#[test]
fn a_restored_volume_is_confirmed_only_by_a_proof_it_actually_has() {
    let mut whole = ByteRanges::new();
    whole.insert(0, 1_000);
    let mut short = ByteRanges::new();
    short.insert(0, 400);
    let mut gapped = ByteRanges::new();
    gapped.insert(0, 400);
    gapped.insert(600, 400);

    // Proof one: the cached facts carry an end-of-archive record saying more
    // volumes follow, which is the last header this volume can hold.
    assert!(restored_volume_is_confirmed(&short, None, true));

    // Proof two: the checkpoint calls the volume complete *and* the coverage in
    // front of us is contiguous to its whole decoded length.
    assert!(restored_volume_is_confirmed(&whole, Some(1_000), false));

    // The claim is checked, not taken. A row whose `complete` bit disagrees with
    // its own coverage — a pre-B1 writer's latched bit, a torn row — proves
    // nothing.
    assert!(!restored_volume_is_confirmed(&short, Some(1_000), false));
    assert!(!restored_volume_is_confirmed(&gapped, Some(1_000), false));

    // And neither proof means unconfirmed, which is what the completion seam
    // turns into an explicit demotion rather than a silently held tail.
    assert!(!restored_volume_is_confirmed(&short, None, false));
    assert!(!restored_volume_is_confirmed(&whole, None, false));
}

// ---------------------------------------------------------------------------
// The restart gate re-arm, over a router rebuilt from cached facts
// ---------------------------------------------------------------------------

/// One member header record, in the shape a split RAR5 member has. Callers set
/// the four fields that differ between a chain's parts on the value returned.
fn member_facts(
    name: &str,
    data_offset: u64,
    data_size: u64,
    unpacked_size: u64,
) -> weaver_unrar::RarVolumeMemberFacts {
    weaver_unrar::RarVolumeMemberFacts {
        order: 0,
        name: name.to_string(),
        name_raw: None,
        unpacked_size: Some(unpacked_size),
        data_crc32: None,
        data_blake2_hash: None,
        version: None,
        packed_crc32: None,
        packed_blake2_hash: None,
        packed_hash_uses_mac: false,
        split_before: false,
        split_after: false,
        is_directory: false,
        is_encrypted: false,
        host_os: None,
        attributes: None,
        owner: None,
        mtime_ns: None,
        ctime_ns: None,
        atime_ns: None,
        data_offset,
        data_size,
        compression_method: 0,
        compression_version: 0,
        compression_solid: false,
        dict_size: 0,
        use_hash_mac: false,
        redirection_type: None,
        redirection_target: None,
        redirection_target_raw: None,
        redirection_target_is_directory: false,
    }
}

fn volume_facts(
    volume_number: u32,
    more_volumes: bool,
    members: Vec<weaver_unrar::RarVolumeMemberFacts>,
) -> weaver_unrar::RarVolumeFacts {
    weaver_unrar::RarVolumeFacts {
        // RAR5.
        format: 5,
        volume_number,
        more_volumes,
        is_solid: false,
        is_encrypted: false,
        is_volume: true,
        has_recovery_record: false,
        is_locked: false,
        has_authenticity_verification: false,
        has_locator: false,
        quick_open_offset: None,
        recovery_record_offset: None,
        original_name: None,
        original_name_raw: None,
        original_creation_time_ns: None,
        members,
        services: Vec::new(),
    }
}

const REARM_PART: u64 = 400;
const REARM_MEMBER: &str = "Silver.Horizon.S01E04.mkv";

/// A router rebuilt exactly the way restore rebuilds one: from cached facts for
/// a two-volume set holding a single member split across both, with the whole
/// member seeded as restart coverage.
fn rearm_router() -> DirectSetRouter {
    let plan = DirectSetPlan {
        set_name: SET.to_string(),
        volumes: [(0u32, 0u32), (1u32, 1u32)].into_iter().collect(),
        files: [(0u32, 0u32), (1u32, 1u32)].into_iter().collect(),
        working_dir: std::path::PathBuf::from("/nonexistent"),
    };
    let mut router = DirectSetRouter::new(plan);
    let facts = std::collections::BTreeMap::from([
        (
            0u32,
            volume_facts(0, true, {
                // The chain's first part: continues into volume 1, and carries
                // the CRC32 of *its own* packed bytes the way RAR5 states it.
                let mut first = member_facts(REARM_MEMBER, 64, REARM_PART, REARM_PART * 2);
                first.split_after = true;
                first.packed_crc32 = Some(0x1111_1111);
                vec![first]
            }),
        ),
        (
            1u32,
            volume_facts(1, false, {
                // The final part: closes the chain and carries the whole-member
                // CRC32 instead.
                let mut last = member_facts(REARM_MEMBER, 64, REARM_PART, REARM_PART * 2);
                last.split_before = true;
                last.data_crc32 = Some(0x2222_2222);
                vec![last]
            }),
        ),
    ]);
    router.restore_layout(&facts).expect("the facts rebuild");
    let partial = format!("{REARM_MEMBER}.direct.partial");
    router
        .restore_member_coverage(&partial, &[(0, REARM_PART * 2)])
        .expect("the member is in the rebuilt layout");
    router
}

#[test]
fn the_restart_read_plan_splits_a_members_coverage_at_its_part_boundaries() {
    let plan = rearm_router().restart_read_plan();

    assert_eq!(
        plan.iter()
            .map(|run| (run.logical_offset, run.len))
            .collect::<Vec<_>>(),
        vec![(0, REARM_PART), (REARM_PART, REARM_PART)],
        "one seeded range spanning two parts must be read as two runs: the composition \
         the re-read feeds is per part, so a run straddling a boundary composes against \
         no reference at all"
    );
    assert!(
        plan.iter()
            .all(|run| run.relative_partial.ends_with(".direct.partial")),
        "every run names the partial that holds it, so the reader opens each file once"
    );
}

#[test]
fn a_rearm_run_the_layout_cannot_place_demotes_instead_of_failing_open() {
    let mut router = rearm_router();

    // M4. A member id the layout does not have. Returning `Ok(())` here — which
    // it used to — leaves the seeded range in place, so `try_verify_member`
    // refuses the member forever while the completion gate re-reads it on every
    // check: a set that neither finalizes nor demotes.
    assert_eq!(
        router.note_restored_member_crc(4242, 0, REARM_PART, 0x1111_1111),
        Err(DemotionReason::RestartRearmUnplaceable),
    );

    // Same verdict for an offset no part of a real member covers.
    let mut router = rearm_router();
    assert_eq!(
        router.note_restored_member_crc(0, REARM_PART * 9, REARM_PART, 0x1111_1111),
        Err(DemotionReason::RestartRearmUnplaceable),
    );
}

#[test]
fn a_rearm_run_that_disagrees_with_its_parts_checksum_demotes() {
    let mut router = rearm_router();
    assert_eq!(
        router.note_restored_member_crc(0, 0, REARM_PART, 0xDEAD_BEEF),
        Err(DemotionReason::PartChecksumMismatch),
        "a byte that changed on disk while the process was down must fail here, which is \
         the whole point of re-reading rather than trusting the checkpoint"
    );
}

#[test]
fn a_rearm_run_that_matches_clears_its_seeded_range() {
    let mut router = rearm_router();
    assert!(router.has_restart_seeded_coverage());
    router
        .note_restored_member_crc(0, 0, REARM_PART, 0x1111_1111)
        .expect("the first part composes to its header's packed CRC32");
    assert!(
        router.has_restart_seeded_coverage(),
        "the member's second part is still seeded"
    );
    assert_eq!(
        router.restart_read_plan().len(),
        1,
        "and a second pass reads only what the first did not clear"
    );
}

// ---------------------------------------------------------------------------
// The holds scratch and its region index (D2)
// ---------------------------------------------------------------------------

#[test]
fn holds_scratch_hands_out_stable_regions_and_reads_them_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");
    let mut scratch = HoldsScratch::new(path.clone(), 1024);

    let first = scratch.append(b"header bytes").unwrap();
    let second = scratch.append(b"payload").unwrap();
    assert_eq!(
        (first, second),
        (0, "header bytes".len() as u64),
        "regions are handed out as append offsets, and the index that names them is the \
         offset itself"
    );
    assert_eq!(
        scratch.bytes(),
        ("header bytes".len() + "payload".len()) as u64
    );
    assert_eq!(
        scratch.read(second, "payload".len() as u64).as_deref(),
        Some(&b"payload"[..]),
        "a region reads back exactly what was appended at it, positionally, so a later \
         append cannot disturb it"
    );
    assert_eq!(
        scratch.read(first, "header bytes".len() as u64).as_deref(),
        Some(&b"header bytes"[..]),
        "including one written before it — the file is write-once per region"
    );

    scratch.discard();
    assert!(!path.exists(), "discard deletes the file it created");
    assert_eq!(scratch.bytes(), 0);
}

#[test]
fn holds_scratch_refuses_an_append_past_its_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");
    let mut scratch = HoldsScratch::new(path.clone(), 8);

    scratch.append(b"12345").unwrap();
    assert_eq!(
        scratch.append(b"678901"),
        Err(DemotionReason::HoldsScratchCeiling),
        "the ceiling is checked before the write, so a breach costs nothing on disk"
    );
    assert_eq!(
        scratch.bytes(),
        5,
        "and leaves the append cursor where it was"
    );
}

#[test]
fn a_scratch_that_never_appended_a_byte_leaves_nothing_behind() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");

    // Never opened at all: nothing to delete, and nothing to blame for a
    // neighbouring file that happens to share the path.
    let mut scratch = HoldsScratch::new(path.clone(), 1024);
    std::fs::write(&path, b"someone else's file").unwrap();
    scratch.discard();
    assert!(
        path.exists(),
        "a scratch that never opened its file must not delete whatever is at that path"
    );
    std::fs::remove_file(&path).unwrap();

    // Opened and appended: deleted, cursor reset, and idempotent.
    let mut scratch = HoldsScratch::new(path.clone(), 1024);
    scratch.append(b"held").unwrap();
    assert!(path.exists());
    scratch.discard();
    scratch.discard();
    assert!(!path.exists());
}

// ---------------------------------------------------------------------------
// Phase 6: damage accounting over an interior hole (D8's opening note)
// ---------------------------------------------------------------------------

/// A PAR2 set describing one file with **slice checksums**, which is what makes
/// per-slice damage attribution a question at all.
fn sliced_par2_set(filename: &str, bytes: &[u8], slice_size: u64) -> weaver_par2::Par2FileSet {
    let file_id = weaver_par2::FileId::from_bytes([11u8; 16]);
    let mut checksums = Vec::new();
    let mut offset = 0u64;
    while offset < bytes.len() as u64 {
        let end = (offset + slice_size).min(bytes.len() as u64);
        let mut state = weaver_par2::SliceChecksumState::new();
        state.update(&bytes[offset as usize..end as usize]);
        let (crc32, md5) = state.finalize((end - offset < slice_size).then_some(slice_size));
        checksums.push(weaver_par2::SliceChecksum { crc32, md5 });
        offset = end;
    }
    weaver_par2::Par2FileSet {
        recovery_set_id: weaver_par2::RecoverySetId::from_bytes([4; 16]),
        slice_size,
        recovery_file_ids: vec![file_id],
        non_recovery_file_ids: Vec::new(),
        files: HashMap::from([(
            file_id,
            weaver_par2::FileDescription {
                file_id,
                hash_full: weaver_par2::checksum::md5(bytes),
                hash_16k: weaver_par2::checksum::md5(&bytes[..bytes.len().min(16 * 1024)]),
                length: bytes.len() as u64,
                par2_name: filename.to_string(),
                filename: filename.to_string(),
            },
        )]),
        slice_checksums: HashMap::from([(file_id, checksums)]),
        recovery_slices: std::collections::BTreeMap::new(),
        creator: None,
    }
}

/// The provider fixture's volume with one **interior** hole: everything is
/// covered except `[hole_start, hole_end)`, which sits in the middle of member A
/// with healthy bytes on both sides.
fn covered_with_interior_hole(hole_start: u64, hole_end: u64) -> ByteRanges {
    let total =
        (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B + PROVIDER_TRAILER)
            as u64;
    let mut covered = ByteRanges::new();
    covered.insert(0, hole_start);
    covered.insert(hole_end, total - hole_end);
    covered
}

/// Which slices `verify_slices` calls damaged, as a set of indices.
fn damaged_slice_indices(valid: &[bool]) -> Vec<usize> {
    valid
        .iter()
        .enumerate()
        .filter_map(|(index, valid)| (!*valid).then_some(index))
        .collect()
}

const HOLE_SLICE_SIZE: u64 = 64;

#[test]
fn an_interior_hole_damages_only_the_slices_it_touches() {
    // The wave-2 review note, as a test. The sequential sweep
    // `verify_slices_batched_md5` prefers stops at the first hole and reports
    // every slice after it damaged — on this fixture that is 5 slices instead of
    // 2 — and the repair those numbers size rebuilds three healthy slices with
    // recovery blocks it did not need to spend.
    let dir = tempfile::tempdir().unwrap();
    let hole = (100u64, 180u64);
    let fixture = provider_fixture(covered_with_interior_hole(hole.0, hole.1));
    let par2_set = sliced_par2_set(
        "silver.horizon.part01.rar",
        &fixture.conventional,
        HOLE_SLICE_SIZE,
    );
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());
    let counters = access.counters();

    let valid = weaver_par2::verify_slices(&par2_set, &file_id, &access)
        .expect("the description names a registered virtual volume");
    let damaged = damaged_slice_indices(&valid);

    // Only the slices the hole actually overlaps: `[100, 180)` at a 64-byte
    // slice size is slices 1 and 2.
    let expected: Vec<usize> = (0..valid.len())
        .filter(|index| {
            let start = *index as u64 * HOLE_SLICE_SIZE;
            let end = start + HOLE_SLICE_SIZE;
            start < hole.1 && hole.0 < end
        })
        .collect();
    assert_eq!(
        damaged, expected,
        "an interior hole must damage only the slices that touch it; a count \
         inflated by the sequential sweep sizes the repair from healthy slices \
         and can flip repairable to unrepairable"
    );
    assert!(
        counters.sequential_refusals() > 0,
        "the adapter must refuse the sequential reader for a volume with an \
         interior hole — that refusal is what makes the count above accurate"
    );
    assert!(
        damaged.len() < valid.len() - 1,
        "non-vacuity: the fixture must have healthy slices *after* the hole, or \
         the accounting fix would be untestable here"
    );
}

#[test]
fn interior_hole_verdicts_match_a_physically_sparse_volume() {
    // Verdict parity is the acceptance rule for the seam choice: whatever the
    // adapter answers, it must be what the same job would have concluded with
    // the gate off, where the missing articles leave a sparse file with real
    // zeros in the hole.
    let dir = tempfile::tempdir().unwrap();
    let hole = (100u64, 180u64);
    let fixture = provider_fixture(covered_with_interior_hole(hole.0, hole.1));
    let filename = "silver.horizon.part01.rar";
    let par2_set = sliced_par2_set(filename, &fixture.conventional, HOLE_SLICE_SIZE);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());
    let virtual_valid = weaver_par2::verify_slices(&par2_set, &file_id, &access)
        .expect("the virtual volume verifies");

    // The same volume as the conventional path would have left it: written at
    // its offsets, with a filesystem hole where the articles never arrived.
    let physical_dir = tempfile::tempdir().unwrap();
    let path = physical_dir.path().join(filename);
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(&fixture.conventional[..hole.0 as usize])
            .unwrap();
        file.seek(SeekFrom::Start(hole.1)).unwrap();
        file.write_all(&fixture.conventional[hole.1 as usize..])
            .unwrap();
    }
    let physical = weaver_par2::PlacementFileAccess::new(
        physical_dir.path().to_path_buf(),
        &par2_set,
        HashMap::new(),
    );
    let physical_valid =
        weaver_par2::verify_slices(&par2_set, &file_id, &physical).expect("the file verifies");

    assert_eq!(
        virtual_valid, physical_valid,
        "a direct set's verdict must be the verdict the same damage produces on \
         a real sparse volume, slice for slice"
    );
}

#[test]
fn a_truncated_volume_still_takes_the_sequential_path() {
    // The refusal is scoped to *interior* holes on purpose. A volume covered
    // from zero and stopping short reads exactly like a truncated file, which is
    // what a sequential sweep already reports correctly — so the fast path D5
    // requires survives for every shape except the one that lies.
    let dir = tempfile::tempdir().unwrap();
    let mut covered = ByteRanges::new();
    covered.insert(0, 200);
    let fixture = provider_fixture(covered);
    let par2_set = sliced_par2_set(
        "silver.horizon.part01.rar",
        &fixture.conventional,
        HOLE_SLICE_SIZE,
    );
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());
    let counters = access.counters();

    let valid = weaver_par2::verify_slices(&par2_set, &file_id, &access)
        .expect("the truncated volume verifies");
    assert_eq!(
        damaged_slice_indices(&valid),
        (3..valid.len()).collect::<Vec<_>>(),
        "a volume covered to byte 200 has three whole 64-byte slices and nothing else"
    );
    assert_eq!(
        counters.sequential_refusals(),
        0,
        "a prefix-readable volume must keep the sequential reader"
    );
    assert!(counters.sequential_opens() > 0);
}

#[test]
fn readable_prefix_reports_the_shape_the_reader_can_answer() {
    let whole = provider_fixture(whole_volume_covered());
    assert_eq!(
        whole.volume.readable_prefix(),
        Some(whole.conventional.len() as u64),
        "a fully covered volume reads end to end"
    );
    assert!(!whole.volume.has_interior_hole());

    let holed = provider_fixture(covered_with_interior_hole(100, 180));
    assert_eq!(holed.volume.readable_prefix(), None);
    assert!(holed.volume.has_interior_hole());

    // Covered, but by no source that holds the bytes: the extents that said the
    // members owned them are gone (B1's ineligible-member case), so those ranges
    // are holes however loudly the volume-level map claims coverage.
    let unbacked = provider_fixture_with_extents(whole_volume_covered(), false);
    let member_a_at = PROVIDER_HEADER as u64;
    let member_b_at = (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP) as u64;
    assert_eq!(
        unbacked.volume.readable_ranges(),
        vec![
            (0, member_a_at),
            (
                member_a_at + PROVIDER_MEMBER_A as u64,
                member_a_at + PROVIDER_MEMBER_A as u64 + PROVIDER_GAP as u64
            ),
            (
                member_b_at + PROVIDER_MEMBER_B as u64,
                member_b_at + PROVIDER_MEMBER_B as u64 + PROVIDER_TRAILER as u64
            ),
        ],
        "the readable image is what a *source* holds, never what the volume map \
         claims — a byte with no source is a hole, which is the invariant that \
         keeps fabricated zeros out of a reconstruction"
    );
    assert!(
        unbacked.volume.has_interior_hole(),
        "and the shape query says so, so nothing offers a stream over it"
    );
}

// ---------------------------------------------------------------------------
// Phase 6: CRC composition under repair (D3/D4)
// ---------------------------------------------------------------------------

#[test]
fn overwrite_replaces_a_run_and_reports_no_gap_when_it_lines_up() {
    let mut runs = CrcRuns::default();
    runs.insert(0, 100, 0x1111_1111);
    runs.insert(100, 100, 0x2222_2222);

    let gaps = runs.overwrite(100, 100, 0x3333_3333);
    assert!(
        gaps.is_empty(),
        "a rewrite that covers whole runs leaves nothing composed by nothing"
    );
    assert_eq!(
        runs.compose(0, 200),
        Some(weaver_par2::checksum::Crc32CombineOp::new(100).combine(0x1111_1111, 0x3333_3333)),
        "the composition must carry the repaired value, not the value the \
         damaged bytes produced"
    );
}

#[test]
fn overwrite_leaves_the_uncovered_edges_as_stale_gaps() {
    let mut runs = CrcRuns::default();
    runs.insert(0, 100, 0x1111_1111);

    // A slice-shaped rewrite inside one article-shaped run: the article's value
    // describes bytes that no longer exist, and the bytes on either side of the
    // rewrite are left vouched for by nothing.
    let gaps = runs.overwrite(40, 20, 0x4444_4444);
    assert_eq!(
        gaps,
        vec![(0, 40), (60, 100)],
        "both edges of the discarded run become stale gaps"
    );
    assert_eq!(
        runs.compose(0, 100),
        None,
        "and until they are re-read the range composes to nothing, which every \
         caller treats as refuse rather than pass"
    );

    // Closing them is what re-arms the composition.
    let head = runs.overwrite(0, 40, 0x5555_5555);
    let tail = runs.overwrite(60, 40, 0x6666_6666);
    assert!(head.is_empty() && tail.is_empty());
    let expected = weaver_par2::checksum::Crc32CombineOp::new(20).combine(0x5555_5555, 0x4444_4444);
    let expected = weaver_par2::checksum::Crc32CombineOp::new(40).combine(expected, 0x6666_6666);
    assert_eq!(runs.compose(0, 100), Some(expected));
}

#[test]
fn insert_still_clips_a_duplicate_after_a_repair_overwrote_the_same_run() {
    // The distinction phase 6 turns on. A repair moved the bytes, so the
    // composition moved with them; a duplicate article carrying the *old* bytes
    // must not move it back, and `insert`'s overlap refusal is what stops it.
    let mut runs = CrcRuns::default();
    runs.insert(0, 100, 0xDEAD_BEEF);
    runs.overwrite(0, 100, 0xFEED_FACE);

    runs.insert(0, 100, 0xDEAD_BEEF);
    assert_eq!(
        runs.compose(0, 100),
        Some(0xFEED_FACE),
        "a duplicate must clip against the repaired run, never replace it"
    );
}

/// A one-volume set holding one whole stored member, rebuilt from facts the way
/// restore rebuilds one — so a test can drive [`DirectSetRouter`]'s drain
/// without a parseable RAR image in front of it.
///
/// `member` is the member's final (post-repair) bytes: the layout's whole-member
/// CRC32 is taken over them, so the set verifies exactly when the composition
/// ends up describing the repaired image and not the damaged one.
fn straddle_router(member: &[u8], header_bytes: u64) -> (DirectSetRouter, u32) {
    let plan = DirectSetPlan {
        set_name: SET.to_string(),
        volumes: [(0u32, 0u32)].into_iter().collect(),
        files: [(0u32, 0u32)].into_iter().collect(),
        working_dir: std::path::PathBuf::from("/nonexistent"),
    };
    let mut router = DirectSetRouter::new(plan);
    let facts = std::collections::BTreeMap::from([(
        0u32,
        volume_facts(0, false, {
            let mut only = member_facts(
                STRADDLE_MEMBER,
                header_bytes,
                member.len() as u64,
                member.len() as u64,
            );
            // One part, chain closed, so the whole-member gate is armed the
            // moment the composition covers it. No packed CRC32: layer 1 would
            // otherwise fire on the *damaged* prefix during the set-up drain and
            // demote before the repair the test is about ever happens.
            only.data_crc32 = Some(weaver_par2::checksum::crc32(member));
            vec![only]
        }),
    )]);
    router.restore_layout(&facts).expect("the facts rebuild");
    let member_id = router
        .member_partials()
        .first()
        .map(|(member_id, _, _)| *member_id)
        .expect("the member was adopted");
    (router, member_id)
}

const STRADDLE_MEMBER: &str = "Silver.Horizon.S01E25.mkv";

#[test]
fn a_drain_run_straddling_repaired_and_duplicate_bytes_splits_at_the_boundary() {
    // F4, the load-bearing gap. The drain's `replace` flag is all-or-nothing per
    // emitted run, and the two things that fix a run's extent do it for
    // unrelated reasons: `map_physical_range` splits at member boundaries, and
    // `pending` coalesces everything that abuts. So a repair routinely produces
    // **one** member run covering repaired and unrepaired bytes together — and
    // that run used to take `replace = false`, which makes `CrcRuns::insert`
    // refuse it as overlapping. The bytes reach the partial correctly and the
    // composition keeps describing the wire-damaged ones, so the member fails a
    // gate it should pass and the set demotes, throwing away a repair that
    // worked.
    const HEADER: u64 = 64;
    const MEMBER: u64 = 400;
    let damaged: Vec<u8> = (0..MEMBER as u32)
        .map(|index| (index % 251) as u8)
        .collect();
    let mut repaired = damaged.clone();
    // The repair rewrites the second half and fills a tail the set never had.
    for (index, byte) in repaired.iter_mut().enumerate().skip(200) {
        *byte = ((index * 7 + 3) % 256) as u8;
    }

    let (mut router, member_id) = straddle_router(&repaired, HEADER);

    // The set's own download: the member's first 300 bytes, damaged. Short of
    // the member, so neither gate fires yet and the set-up cannot demote.
    router.stage_for_test(0, HEADER, &damaged[..300]);
    router.drain_for_test(0).expect("the first drain routes");

    // Now the shape. `[100, 200)` re-enters as an ordinary duplicate — bytes the
    // router staged and could not place, which is the only way unrepaired bytes
    // sit next to repaired ones in one pending run — and `[200, 400)` as the
    // repair. `pending` coalesces the two into `[100, 400)`, and the layout maps
    // that as a single member run.
    router.force_stage_for_test(0, HEADER + 100, &damaged[100..200], false);
    router.force_stage_for_test(0, HEADER + 200, &repaired[200..], true);
    let spans = router
        .drain_for_test(0)
        .expect("the straddling drain routes");
    assert_eq!(
        spans.iter().map(|span| span.bytes.len()).sum::<usize>(),
        300,
        "both halves must still reach the member's partial — the bug was never \
         about the bytes, only about what the composition then claims about them"
    );

    // The repaired sub-range **overwrote**: that is what discards the article's
    // old value and leaves its uncovered head as a stale gap. Without the split
    // there is no overwrite, so there is no gap either — and nothing to re-read,
    // which is how the damaged value survived in silence.
    assert!(
        router.has_stale_gaps(),
        "the repaired sub-range must have overwritten the composition, which is \
         what opens the stale gap D4's re-read then closes"
    );
    assert_eq!(
        router
            .stale_gap_read_plan()
            .iter()
            .map(|run| (run.member_id, run.logical_offset, run.len))
            .collect::<Vec<_>>(),
        vec![(member_id, 0, 200)],
        "and exactly the head of the discarded article is stale: the duplicate \
         sub-range clipped, so it neither re-inserted its own value nor widened \
         the gap"
    );

    // Closing the gap the way `reread_direct_stale_gaps` does. It composes to
    // the repaired image, so the whole-member gate passes — the repair survived.
    router
        .note_restored_member_crc(
            member_id,
            0,
            200,
            weaver_par2::checksum::crc32(&repaired[..200]),
        )
        .expect("the re-read closes the gap");
    assert!(
        !router.has_stale_gaps(),
        "one pass over the plan closes every gap it named"
    );
    assert!(
        router.all_members_verified(),
        "and the member must verify against the *repaired* whole-member CRC32; \
         a composition still carrying the damaged run demotes a set whose bytes \
         on disk are correct"
    );
}

// ---------------------------------------------------------------------------
// Phase 6: D8's ordering, and what a crash in its window costs
// ---------------------------------------------------------------------------

#[test]
fn deleting_the_row_before_a_repair_keeps_the_coverage_the_provider_reads() {
    // D8's ordering rule, and the distinction that makes it survivable. The row
    // has to go *before* a repair rewrites the bytes it claims — otherwise a
    // crash mid-repair leaves floors over half-rewritten data. What must **not**
    // go with it is the controller: a repair leaves every destination in place,
    // at the same offsets, holding better bytes, so its account of what is on
    // disk is still exactly right — and it is also what the hybrid provider
    // reads to answer the re-verify that follows.
    let recorder = Recorder::default();
    let mut barrier = sample_barrier();
    barrier.register_volume(0, 0);
    barrier.register_destination(0, "Silver.Horizon.S01E04.mkv.direct.partial");
    barrier
        .record_write(&write(0, 0, 4096, 0), Instant::now())
        .unwrap();
    run_barrier(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::Pause),
    )
    .unwrap();
    assert_eq!(barrier.generation(), 1);
    let covered_before = barrier.volume_coverage(0);
    assert_eq!(covered_before.as_ref().map(ByteRanges::covered), Some(4096));

    barrier.delete_committed_row(&mut recorder.clone()).unwrap();
    assert_eq!(recorder.deletes(), 1, "the durable row is gone");
    assert_eq!(
        barrier.volume_coverage(0),
        covered_before,
        "and the in-memory coverage is untouched, so the re-verify still reads a \
         volume rather than a hole"
    );
    assert_eq!(
        barrier.destination_coverage(0).map(ByteRanges::covered),
        Some(4096),
        "the destination claim survives too — it is what says the envelope or \
         partial really received a byte, which no volume-level map can answer"
    );

    // And the next barrier writes a fresh row rather than an increment of one
    // that no longer exists.
    barrier
        .record_write(&write(0, 4096, 4096, 0), Instant::now())
        .unwrap();
    run_barrier(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::RepairRecreate),
    )
    .unwrap();
    assert_eq!(
        barrier.generation(),
        1,
        "the generation restarts from zero, because the row it would have \
         incremented was deleted"
    );
}

#[test]
fn a_crash_between_the_row_delete_and_the_next_barrier_leaves_nothing_to_trust() {
    // The deliberately lossy half, stated as a test. Between the delete and the
    // barrier that recreates coverage there is no row at all, so a restart in
    // that window finds nothing, claims nothing, and redownloads the set — which
    // is the bounded cost D8 accepts in exchange for not having to selectively
    // lower per-volume floors around the repaired ranges.
    let recorder = Recorder::default();
    let mut barrier = sample_barrier();
    barrier.register_volume(0, 0);
    barrier.register_destination(0, "Silver.Horizon.S01E04.mkv.direct.partial");
    barrier
        .record_write(&write(0, 0, 4096, 0), Instant::now())
        .unwrap();
    run_barrier(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::Pause),
    )
    .unwrap();
    assert!(recorder.committed().is_some());

    barrier.delete_committed_row(&mut recorder.clone()).unwrap();
    assert!(
        recorder.committed().is_none(),
        "nothing durable survives the delete, so a restart here has no floors to \
         trust and refetches the set"
    );
}

// ---------------------------------------------------------------------------
// Phase 6: what the repair is sized from (D8)
// ---------------------------------------------------------------------------

#[test]
fn damaged_ranges_name_only_the_slices_par2_called_invalid() {
    use super::repair::damaged_ranges;

    // Slices 1 and 4 damaged, on a file whose last slice is short.
    let valid = [true, false, true, true, false];
    assert_eq!(
        damaged_ranges(&valid, 64, 300),
        vec![(64, 128), (256, 300)],
        "the repair is sized from the per-slice verdict, and the tail slice is \
         clipped to the file rather than to the slice size"
    );
    assert_eq!(
        damaged_ranges(&[true, true], 64, 128),
        Vec::new(),
        "a clean file contributes nothing to rewrite"
    );
    assert_eq!(
        damaged_ranges(&[false, false, false], 64, 192),
        vec![(0, 192)],
        "adjacent damaged slices coalesce into one rewrite"
    );
}

#[test]
fn a_rewrite_widens_to_whole_articles_so_the_volume_composition_stays_exact() {
    use super::repair::widen_to_articles;

    // Three articles of 100 bytes. A 64-byte slice-shaped rewrite at 120 cuts
    // the second one in half.
    let extents = std::collections::BTreeMap::from([
        (0u32, (0u64, 100u64)),
        (1, (100, 100)),
        (2, (200, 100)),
    ]);
    assert_eq!(
        widen_to_articles(&[(120, 184)], &extents, 300),
        vec![(100, 200)],
        "the rewrite is read back as whole articles, so the volume's yEnc \
         composition is replaced run for run and leaves no stale gap"
    );
    assert_eq!(
        widen_to_articles(&[(0, 300)], &extents, 300),
        vec![(0, 300)],
        "a rewrite that already covers whole articles is unchanged"
    );
    // A range no article ever reached — the set never received a byte of it —
    // has no run to half-cover, so widening it would only read bytes nothing
    // asked for.
    let sparse = std::collections::BTreeMap::from([(0u32, (0u64, 100u64))]);
    assert_eq!(
        widen_to_articles(&[(200, 264)], &sparse, 300),
        vec![(200, 264)]
    );
}
