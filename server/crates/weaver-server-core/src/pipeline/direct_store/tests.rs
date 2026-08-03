//! Tests for the direct-store coverage checkpoint (plan 135, D6).
//!
//! Fixture names are invented throughout — never real media titles.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use weaver_model::files::FileRole;

use super::barrier::{
    BARRIER_DIRTY_AGE, BARRIER_DIRTY_BYTES, BARRIER_FAILURE_BACKOFF, BARRIER_FAILURE_BACKOFF_MAX,
    BarrierDemand, BarrierDrain, BarrierError, BarrierStep, BarrierTrigger, CoverageBarrier,
    CoveragePersist, DatabaseCoveragePersist, DestinationSync, RoutedWrite, WriteRefused,
};
use super::restart::{
    CoverageRejection, DestinationProbe, ExpectedSet, ProbedDestination, coverage_skip_plan,
    refetch_floors, restore_job, restore_set, restore_set_with_probe,
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
        }],
    }
}

/// The plan facts [`sample_snapshot`] was written against: one volume, mapped
/// to NZB file 0.
fn sample_expected() -> ExpectedSet {
    ExpectedSet {
        plan_digest: PLAN_DIGEST,
        volume_files: HashMap::from([(0u32, 0u32)]),
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
            },
            VolumeFloor {
                volume_index: 1,
                file_index: 4,
                floor: 6 * 1024 * 1024 * 1024,
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
        },
        VolumeFloor {
            volume_index: 1,
            file_index: 1,
            floor: 10,
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
            .unwrap()
    };
    assert_eq!(
        claims(0),
        vec![DestinationExtent {
            start: 0,
            end: 8_192
        }],
        "the committed claim stops where the barrier did"
    );
    assert!(
        claims(1).is_empty(),
        "the second member was first written after the barrier, so nothing claims it"
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
        plan_digest: PLAN_DIGEST,
        volume_files: HashMap::new(),
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
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::from([(0u32, 30u64)]));

    assert_eq!(plan.skip.len(), 2);
    assert!(plan.skip.contains(&segment(0)));
    assert!(plan.skip.contains(&segment(1)));
    assert_eq!(plan.file_progress.get(&0), Some(&30));
}

#[test]
fn coverage_skip_plan_does_not_skip_a_partial_segment() {
    let spec = direct_job_spec();
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::from([(0u32, 25u64)]));

    assert_eq!(plan.skip, [segment(0)].into_iter().collect());
    assert_eq!(plan.file_progress.get(&0), Some(&10));
}

#[test]
fn coverage_skip_plan_never_consults_destination_length() {
    // No file exists anywhere: for a direct set the source volume never does.
    // The legacy path would clamp to `metadata.len()` and zero this floor.
    let spec = direct_job_spec();
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::from([(0u32, 60u64)]));

    assert_eq!(plan.skip.len(), 3);
    assert_eq!(plan.file_progress.get(&0), Some(&60));
}

#[test]
fn coverage_skip_plan_leaves_unlisted_files_alone() {
    let spec = direct_job_spec();
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::new());
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
        },
        VolumeFloor {
            volume_index: 1,
            file_index: 0,
            floor: 100,
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
