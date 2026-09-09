//! Recording test doubles. One struct implements all three barrier traits, so
//! Range set
//! Codec
//! Barrier triggers
//! Refused writes
//! Barrier ordering and failure handling
//! Failure backoff
//! The stamped plan digest tracks the set's facts
//! Retiring one destination (task_9ee23560)
//! One row per set, independent of volume count
//! Restart
//! Floor to segment derivation
//! The real database seam
//! Gate
//! Router internals: the pieces every routed byte passes through
//! The hybrid virtual-volume provider
//! The re-encrypting overlay
//! The reconstruction sweep, and the verification that gates it

use super::*;

// ---------------------------------------------------------------------------
// Recording test doubles. One struct implements all three barrier traits, so
// the recorded operation log is a single interleaved order.
// ---------------------------------------------------------------------------

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
                relative_path: "silver-horizon.mkv.f0.direct.partial".to_string(),
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
                crypt: None,
            },
            DestinationClaim {
                member_index: 4,
                relative_path: "silver-horizon.envelope".to_string(),
                extents: vec![DestinationExtent { start: 0, end: 17 }],
                crypt: None,
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
        crypt: None,
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
        crypt: None,
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
            relative_path: "silver-horizon.s01.mkv.f0.direct.partial".to_string(),
            extents: vec![DestinationExtent {
                start: 0,
                end: 100 * 1024 * 1024 * 1024,
            }],
            crypt: None,
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
    // path that escapes it would have restart probing — and the writer —
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
    snapshot.destinations[0].relative_path =
        "nested/dir/silver-horizon.mkv.f0.direct.partial".into();
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
            "silver-horizon.mkv.f0.direct.partial".to_string(),
            "silver-horizon.nfo.f0.direct.partial".to_string(),
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
            "silver-horizon.mkv.f0.direct.partial".to_string(),
            "silver-horizon.nfo.f0.direct.partial".to_string(),
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
// The stamped plan digest tracks the set's facts
// ---------------------------------------------------------------------------

#[test]
fn a_digest_change_re_stamps_the_row_without_touching_the_coverage() {
    let (mut barrier, recorder) = barrier_with_one_committed_checkpoint();
    let start = Instant::now();
    let before = decode(recorder.committed().unwrap().as_slice()).unwrap();
    assert_eq!(before.plan_digest, PLAN_DIGEST);
    assert_eq!(
        barrier.due(start),
        None,
        "a barrier that has just committed everything it holds is not due"
    );

    // The set adopted a member it had not seen, so the digest it routes under is
    // no longer the one the committed row carries.
    barrier.set_plan_digest(OTHER_DIGEST);
    assert_eq!(
        barrier.due(start),
        Some(BarrierTrigger::PlanDigestChanged),
        "a committed row whose digest has gone stale would be refused at restart, so the \
         barrier must re-stamp it — with no dirty byte in sight"
    );
    assert_eq!(
        barrier.dirty_bytes(),
        0,
        "non-vacuity: nothing was written between the two barriers"
    );

    run_barrier(&mut barrier, &recorder, BarrierTrigger::PlanDigestChanged).unwrap();
    let after = decode(recorder.committed().unwrap().as_slice()).unwrap();

    assert_eq!(after.plan_digest, OTHER_DIGEST);
    assert_eq!(after.generation, 2, "the row is replaced, not appended to");
    // The load-bearing half: discovering a member moves nobody's bytes, so the
    // coverage the previous row published is carried over exactly.
    assert_eq!(after.destinations, before.destinations);
    assert_eq!(after.floors, before.floors);
    assert_eq!(
        barrier.due(start),
        None,
        "the row now carries the current digest"
    );
}

#[test]
fn a_digest_change_before_the_first_row_forces_no_barrier() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    barrier.set_plan_digest(OTHER_DIGEST);

    assert_eq!(
        barrier.due(start),
        None,
        "there is no committed row to go stale, so a set that is still discovering its \
         members must not barrier once per member"
    );
}

#[test]
fn a_stale_digest_is_damped_by_the_failure_cooldown() {
    let (mut barrier, recorder) = barrier_with_one_committed_checkpoint();
    let start = Instant::now();
    barrier.set_plan_digest(OTHER_DIGEST);
    recorder.fail_write("database is down");

    let error = run_barrier_at(
        &mut barrier,
        &recorder,
        BarrierTrigger::PlanDigestChanged,
        start,
    )
    .unwrap_err();
    assert!(matches!(error, BarrierError::Persist(_)));

    // The condition survives the failure — the row still carries the old digest —
    // so without the cooldown this arm would be due again on the very next turn
    // of the pipeline loop, forever.
    assert_eq!(
        barrier.due(start + BARRIER_FAILURE_BACKOFF / 2),
        None,
        "a failing re-stamp must not busy-loop"
    );
    assert_eq!(
        barrier.due(start + BARRIER_FAILURE_BACKOFF + Duration::from_millis(1)),
        Some(BarrierTrigger::PlanDigestChanged),
        "and it must be retried once the cooldown is over"
    );
}

// ---------------------------------------------------------------------------
// Retiring one destination (task_9ee23560)
// ---------------------------------------------------------------------------

#[test]
fn a_retired_destination_is_dropped_from_the_next_snapshot_and_stays_dropped() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    barrier.record_write(&write(0, 0, 4_096, 0), start).unwrap();
    barrier.record_write(&write(1, 0, 2_048, 1), start).unwrap();

    // The migration moved member 1's bytes into the envelope and unlinked its
    // partial.
    assert!(
        barrier.retire_destination(1, "silver-horizon.nfo.f0.direct.partial"),
        "the claim named exactly this path"
    );

    let recorder = Recorder::default();
    run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyBytes).unwrap();
    let snapshot = decode(recorder.committed().unwrap().as_slice()).unwrap();

    assert_eq!(
        snapshot
            .destinations
            .iter()
            .map(|claim| claim.relative_path.as_str())
            .collect::<Vec<_>>(),
        vec!["silver-horizon.mkv.f0.direct.partial"],
        "a checkpoint must not claim a destination that no longer exists"
    );
    assert!(
        !recorder
            .synced()
            .contains(&"silver-horizon.nfo.f0.direct.partial".to_string()),
        "and it must not fsync it either — the sync step opens with create(true), and a \
         failure there fails the whole barrier"
    );
    // The rule, not a zero-byte claim: a destination that is gone is omitted
    // exactly as one that never received a byte is.
    assert!(
        snapshot
            .destinations
            .iter()
            .all(|claim| claim.claimed_len() > 0)
    );
    // The source floor is untouched: those bytes are still durable, in the
    // envelope the migration wrote them to.
    assert_eq!(snapshot.floor_for_volume(1), Some(2_048));

    // It cannot come back through a resume either, because the row it would come
    // back from no longer names it.
    let mut resumed = CoverageBarrier::resume(JOB, SET, &snapshot);
    assert_eq!(resumed.destination_coverage(1), None);
    assert_eq!(
        resumed.try_record_write(&write(1, 0, 16, 1), start),
        Err(WriteRefused::UnregisteredMember { member_index: 1 }),
        "a retired destination is unregistered: a write naming it is refused whole"
    );
}

#[test]
fn retiring_a_destination_whose_path_does_not_match_keeps_the_claim() {
    let mut barrier = sample_barrier();
    let start = Instant::now();
    barrier.record_write(&write(1, 0, 2_048, 1), start).unwrap();

    assert!(
        !barrier.retire_destination(1, "some-other-member.f0.direct.partial"),
        "member ids are in-run counters; retiring on the id alone would drop claims on \
         bytes that really are on disk"
    );
    assert!(barrier.destination_coverage(1).is_some());
}

// ---------------------------------------------------------------------------
// One row per set, independent of volume count
// ---------------------------------------------------------------------------

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

#[tokio::test]
async fn restart_accepts_a_valid_row_and_yields_floors() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        60,
    );
    let blob = encode(&sample_snapshot()).unwrap();

    let snapshot = restore_set(&roots, &blob, &sample_expected())
        .await
        .unwrap();
    assert_eq!(snapshot.generation, 3);
    assert_eq!(refetch_floors(&snapshot), HashMap::from([(0u32, 60u64)]));
}

#[tokio::test]
async fn restart_accepts_a_destination_longer_than_the_claim() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        4_096,
    );
    let blob = encode(&sample_snapshot()).unwrap();

    assert!(
        restore_set(&roots, &blob, &sample_expected()).await.is_ok(),
        "file length never implies coverage: a longer file is expected, not truncated"
    );
}

#[tokio::test]
async fn restart_refuses_a_missing_destination() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    let blob = encode(&sample_snapshot()).unwrap();

    assert_eq!(
        restore_set(&roots, &blob, &sample_expected()).await,
        Err(CoverageRejection::MissingDestination {
            path: "silver-horizon.mkv.f0.direct.partial".to_string(),
        })
    );
}

#[tokio::test]
async fn restart_refuses_a_short_destination() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        59,
    );
    let blob = encode(&sample_snapshot()).unwrap();

    assert_eq!(
        restore_set(&roots, &blob, &sample_expected()).await,
        Err(CoverageRejection::ShortDestination {
            path: "silver-horizon.mkv.f0.direct.partial".to_string(),
            claimed: 60,
            actual: 59,
        })
    );
}

#[tokio::test]
async fn restart_refuses_a_plan_digest_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        60,
    );
    let blob = encode(&sample_snapshot()).unwrap();

    let expected = ExpectedSet {
        plan_digest: OTHER_DIGEST,
        ..sample_expected()
    };
    assert_eq!(
        restore_set(&roots, &blob, &expected).await,
        Err(CoverageRejection::PlanDigestMismatch),
        "a plan-digest mismatch is a hard stop, never partial trust"
    );
}

/// The digest still discriminates. Re-stamping it as a set's members are
/// discovered makes the *label* track the plan; it must not make the label stop
/// meaning anything, or a row written against genuinely different member facts
/// would be trusted for coverage it cannot describe.
#[tokio::test]
async fn restart_refuses_a_row_written_under_different_member_facts() {
    let plan = envelope_plan();
    let one = plan.digest(&[("Silver.Horizon.S01E05.mkv".to_string(), 4_096)]);
    let grown = plan.digest(&[
        ("Silver.Horizon.S01E05.mkv".to_string(), 4_096),
        ("Silver.Horizon.S01E05.nfo".to_string(), 128),
    ]);
    let resized = plan.digest(&[("Silver.Horizon.S01E05.mkv".to_string(), 8_192)]);
    let renamed = plan.digest(&[("Silver.Horizon.S01E06.mkv".to_string(), 4_096)]);
    assert_ne!(one, grown, "a member the set had not seen is a new digest");
    assert_ne!(one, resized, "so is a declared size that moved");
    assert_ne!(one, renamed, "so is a different member");

    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        60,
    );
    let blob = encode(&CoverageSnapshot {
        plan_digest: one,
        ..sample_snapshot()
    })
    .unwrap();

    for (label, digest) in [("renamed", renamed), ("resized", resized)] {
        let expected = ExpectedSet {
            plan_digest: digest,
            ..sample_expected()
        };
        assert_eq!(
            restore_set(&roots, &blob, &expected).await,
            Err(CoverageRejection::PlanDigestMismatch),
            "a row written against different member facts ({label}) must still be refused"
        );
    }

    let expected = ExpectedSet {
        plan_digest: one,
        ..sample_expected()
    };
    assert!(
        restore_set(&roots, &blob, &expected).await.is_ok(),
        "non-vacuity: the same facts are accepted"
    );
}

#[tokio::test]
async fn restart_refuses_a_checkpoint_whose_probe_never_completed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        60,
    );
    let blob = encode(&sample_snapshot()).unwrap();

    // The probe panics — a bug in it, or a runtime torn down under it during
    // startup. "Could not check" must never come out as "checked, fine".
    let rejection = restore_set_with_probe(&roots, &blob, &sample_expected(), |_| {
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
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        60,
    );
    let blob = encode(&sample_snapshot()).unwrap();

    // A probe that answers for nothing would otherwise walk an empty loop and
    // accept a checkpoint having validated zero destinations.
    let rejection = restore_set_with_probe(&roots, &blob, &sample_expected(), |_| {
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
async fn restart_probes_every_claimed_member_destination_under_the_staging_root() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    let blob = encode(&sample_snapshot()).unwrap();
    // The staging root, not the working directory: a member claim names payload,
    // and payload is born on the complete volume.
    let expected_path = roots
        .destination_dir
        .join("silver-horizon.mkv.f0.direct.partial");
    assert!(
        !expected_path.starts_with(&roots.working_dir),
        "the two roots must be genuinely different for this to prove anything"
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    restore_set_with_probe(&roots, &blob, &sample_expected(), move |probes| {
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
            relative_path: "silver-horizon.mkv.f0.direct.partial".to_string(),
            claimed: 60,
        }]
    );
}

#[tokio::test]
async fn restart_refuses_a_flipped_file_index() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        60,
    );
    let mut snapshot = sample_snapshot();
    snapshot.floors[0].file_index = 1;
    let blob = encode(&snapshot).unwrap();

    assert_eq!(
        restore_set(&roots, &blob, &sample_expected()).await,
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
        restore_set(&roots, &blob, &expected).await,
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
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        60,
    );
    let mut snapshot = sample_snapshot();
    snapshot.generation = 0;
    let blob = encode(&snapshot).unwrap();

    assert_eq!(
        restore_set(&roots, &blob, &sample_expected()).await,
        Err(CoverageRejection::InvalidGeneration)
    );
}

#[tokio::test]
async fn restart_deletes_every_row_it_refuses() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        60,
    );

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
        &roots,
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
    let roots = sample_roots(temp_dir.path());
    let rows = HashMap::from([(SET.to_string(), encode(&sample_snapshot()).unwrap())]);
    let expected = HashMap::from([(SET.to_string(), sample_expected())]);

    let recorder = Recorder::default();
    let outcome = restore_job(
        DirectStoreGate::Disabled,
        JOB,
        &roots,
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
    barrier.register_destination(0, "silver-horizon.s01.mkv.f0.direct.partial");
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
fn the_env_override_recognises_both_directions_and_defers_when_it_cannot() {
    // Absent and unrecognised both mean "the variable does not apply", so the
    // config value decides. A typo'd override must never silently disable a
    // feature an operator turned on in config.
    assert_eq!(parse_enabled(None), None);
    assert_eq!(parse_enabled(Some("")), None);
    assert_eq!(parse_enabled(Some("maybe")), None);
    assert_eq!(parse_enabled(Some("0")), Some(false));
    assert_eq!(parse_enabled(Some("off")), Some(false));
    assert_eq!(parse_enabled(Some(" FALSE ")), Some(false));
    assert_eq!(parse_enabled(Some("no")), Some(false));
    assert_eq!(parse_enabled(Some("1")), Some(true));
    assert_eq!(parse_enabled(Some(" TRUE ")), Some(true));
    assert_eq!(parse_enabled(Some("on")), Some(true));
    assert_eq!(parse_enabled(Some("yes")), Some(true));
}

#[test]
fn settings_resolve_env_over_config_over_default() {
    use super::super::router::HOLDS_SCRATCH_CEILING_BYTES;
    use super::super::{DirectStoreEnv, DirectStoreSettings, HostFacts};
    use crate::settings::DirectStoreOverrides;

    let unknown = HostFacts::UNKNOWN;
    // Nothing configured anywhere: on, at the 1 GiB default ceiling.
    let defaults = DirectStoreSettings::resolve_parts(None, DirectStoreEnv::default(), unknown);
    assert_eq!(defaults, DirectStoreSettings::default());
    assert!(defaults.gate.is_enabled(), "the default is on");
    assert_eq!(
        defaults.holds_scratch_ceiling_bytes,
        HOLDS_SCRATCH_CEILING_BYTES
    );
    assert_eq!(HOLDS_SCRATCH_CEILING_BYTES, 1024 * 1024 * 1024);

    // Config alone decides when the environment says nothing.
    let config = DirectStoreOverrides {
        enabled: Some(true),
        holds_scratch_ceiling_bytes: Some(4096),
        ..Default::default()
    };
    let configured =
        DirectStoreSettings::resolve_parts(Some(&config), DirectStoreEnv::default(), unknown);
    assert!(configured.gate.is_enabled());
    assert_eq!(configured.holds_scratch_ceiling_bytes, 4096);

    // The env override wins in both directions — that is what makes it a kill
    // switch rather than a second way to say the same thing.
    let killed = DirectStoreSettings::resolve_parts(
        Some(&config),
        DirectStoreEnv {
            enabled: Some(false),
            scratch_ceiling: Some(8192),
            ..Default::default()
        },
        unknown,
    );
    assert!(!killed.gate.is_enabled(), "env off beats config on");
    assert_eq!(killed.holds_scratch_ceiling_bytes, 8192);
    let forced = DirectStoreSettings::resolve_parts(
        Some(&DirectStoreOverrides {
            enabled: Some(false),
            ..Default::default()
        }),
        DirectStoreEnv {
            enabled: Some(true),
            ..Default::default()
        },
        unknown,
    );
    assert!(forced.gate.is_enabled(), "env on beats config off");
    assert_eq!(
        forced.holds_scratch_ceiling_bytes, HOLDS_SCRATCH_CEILING_BYTES,
        "an unset ceiling override falls through config to the default"
    );
}

/// The process-wide limits follow the host when nothing configures them, and
/// config and the environment override them like every other field.
#[test]
fn settings_derive_the_shared_limits_from_the_host() {
    use super::super::router::HOLDS_SCRATCH_CEILING_BYTES;
    use super::super::{DirectStoreEnv, DirectStoreSettings, HostFacts};
    use crate::settings::DirectStoreOverrides;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    let resolve = |config: Option<&DirectStoreOverrides>, env, host| {
        DirectStoreSettings::resolve_parts(config, env, host)
    };
    let none = DirectStoreEnv::default();

    // A host that says nothing: four sets' worth of RAM, four sets' worth of
    // scratch, and the extractor's smallest reserve.
    let unknown = resolve(None, none, HostFacts::UNKNOWN);
    assert_eq!(unknown.holds_resident_limit_bytes, 256 * MIB);
    assert_eq!(
        unknown.holds_scratch_total_bytes,
        4 * HOLDS_SCRATCH_CEILING_BYTES
    );
    assert_eq!(unknown.holds_disk_reserve_bytes, 512 * MIB);

    // A sixteenth of usable memory, clamped to one set's budget below and
    // sixteen above: an 8 GiB box, a 512 MiB container, a 64 GiB server.
    let host = |memory: u64, fs: u64| HostFacts {
        total_memory_bytes: Some(memory),
        working_fs_total_bytes: Some(fs),
    };
    assert_eq!(
        resolve(None, none, host(8 * GIB, 100 * GIB)).holds_resident_limit_bytes,
        512 * MIB
    );
    assert_eq!(
        resolve(None, none, host(512 * MIB, 100 * GIB)).holds_resident_limit_bytes,
        64 * MIB,
        "a small container still affords one set its whole budget"
    );
    assert_eq!(
        resolve(None, none, host(64 * GIB, 100 * GIB)).holds_resident_limit_bytes,
        GIB,
        "a large box does not turn holds into a RAM cache"
    );
    // A twentieth of the filesystem, on the extractor's clamp.
    assert_eq!(
        resolve(None, none, host(8 * GIB, 100 * GIB)).holds_disk_reserve_bytes,
        5 * GIB
    );
    assert_eq!(
        resolve(None, none, host(8 * GIB, 2 * GIB)).holds_disk_reserve_bytes,
        512 * MIB
    );
    assert_eq!(
        resolve(None, none, host(8 * GIB, 2048 * GIB)).holds_disk_reserve_bytes,
        20 * GIB
    );

    // The scratch total tracks the per-set ceiling it multiplies, wherever
    // that ceiling came from.
    let config = DirectStoreOverrides {
        holds_scratch_ceiling_bytes: Some(4096),
        ..Default::default()
    };
    assert_eq!(
        resolve(Some(&config), none, HostFacts::UNKNOWN).holds_scratch_total_bytes,
        4 * 4096
    );

    // Config beats the host, and the environment beats config, field by field.
    let config = DirectStoreOverrides {
        holds_resident_limit_bytes: Some(1000),
        holds_scratch_total_bytes: Some(2000),
        holds_disk_reserve_bytes: Some(0),
        ..Default::default()
    };
    let configured = resolve(Some(&config), none, host(8 * GIB, 100 * GIB));
    assert_eq!(configured.holds_resident_limit_bytes, 1000);
    assert_eq!(configured.holds_scratch_total_bytes, 2000);
    assert_eq!(
        configured.holds_disk_reserve_bytes, 0,
        "zero is a real answer: no reserve"
    );
    let overridden = resolve(
        Some(&config),
        DirectStoreEnv {
            resident_limit: Some(10),
            scratch_total: Some(20),
            disk_reserve: Some(30),
            ..Default::default()
        },
        host(8 * GIB, 100 * GIB),
    );
    assert_eq!(overridden.holds_resident_limit_bytes, 10);
    assert_eq!(overridden.holds_scratch_total_bytes, 20);
    assert_eq!(overridden.holds_disk_reserve_bytes, 30);
    assert_eq!(
        overridden.holds_limits(),
        super::super::accountant::HoldsLimits {
            resident_bytes: 10,
            scratch_bytes: 20,
            disk_reserve_bytes: 30,
        }
    );
}

#[test]
fn settings_resolve_reads_the_config_table() {
    use super::super::DirectStoreSettings;
    use crate::settings::DirectStoreOverrides;

    let mut config = crate::settings::Config {
        data_dir: "/tmp/weaver-direct-store".to_string(),
        intermediate_dir: None,
        complete_dir: None,
        buffer_pool: None,
        tuner: None,
        servers: Vec::new(),
        categories: Vec::new(),
        retry: None,
        max_download_speed: None,
        cleanup_after_extract: None,
        isp_bandwidth_cap: None,
        ip_replacement_trial_extra_connections: None,
        watch_folder: crate::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: crate::jobs::DuplicatePolicy::default(),
        direct_store: None,
        direct_unpack: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    };
    // Skipped rather than asserted when the developer running the suite has an
    // override exported: `resolve` reads the real process environment, and the
    // precedence rule itself is covered above with the environment injected.
    if super::super::env().any_set() {
        return;
    }

    // An absent table is "every default", which is what an existing install's
    // config file looks like — and the default is ON: direct store is the
    // shipping posture, with the config table and the environment kill switch
    // as the ways out.
    assert!(DirectStoreSettings::resolve(&config).gate.is_enabled());

    config.direct_store = Some(DirectStoreOverrides {
        enabled: Some(false),
        holds_scratch_ceiling_bytes: Some(64 * 1024 * 1024),
        ..Default::default()
    });
    assert!(
        !DirectStoreSettings::resolve(&config).gate.is_enabled(),
        "an explicit config off overrides the default-on"
    );

    config.direct_store = Some(DirectStoreOverrides {
        enabled: Some(true),
        holds_scratch_ceiling_bytes: Some(64 * 1024 * 1024),
        ..Default::default()
    });
    let resolved = DirectStoreSettings::resolve(&config);
    assert!(
        resolved.gate.is_enabled(),
        "the config table reaches the gate"
    );
    assert_eq!(
        resolved.holds_scratch_ceiling_bytes,
        64 * 1024 * 1024,
        "the configured ceiling reaches the resolved settings"
    );
}

// ---------------------------------------------------------------------------
// Router internals: the pieces every routed byte passes through
// ---------------------------------------------------------------------------

#[test]
fn crc_runs_compose_neighbours_and_ignore_an_overlapping_re_insert() {
    let payload: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let whole = par2_rs::checksum::crc32(&payload);

    // Fed out of order — the tail, then the head, then the middle that joins
    // them — which is the only order the router ever guarantees.
    let mut runs = CrcRuns::default();
    runs.insert(400, 200, par2_rs::checksum::crc32(&payload[400..600]));
    assert_eq!(runs.compose(0, 600), None, "a gap is not a composition");
    runs.insert(0, 100, par2_rs::checksum::crc32(&payload[..100]));
    assert_eq!(runs.compose(0, 600), None);
    runs.insert(100, 300, par2_rs::checksum::crc32(&payload[100..400]));

    assert_eq!(
        runs.compose(0, 600),
        Some(whole),
        "adjacent runs compose to the whole-space CRC32"
    );
}

#[test]
fn crc_runs_compose_any_sub_range_that_lands_on_run_boundaries() {
    // A merged-only composition could answer for the whole space and nothing
    // else, so a covered range that stopped short of it — a held tail, a volume
    // that stopped mid-download — was reconstructed with no reference value at
    // all. Every prefix and interior span a coverage map can name for wholly
    // routed articles has to compose.
    let payload: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let mut runs = CrcRuns::default();
    for (start, len) in [(0usize, 100usize), (100, 300), (400, 200)] {
        runs.insert(
            start as u64,
            len as u64,
            par2_rs::checksum::crc32(&payload[start..start + len]),
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
            Some(par2_rs::checksum::crc32(&payload[start..start + len])),
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
fn crc_runs_promote_only_wholly_covered_atoms_for_reconstruction() {
    let mut runs = CrcRuns::default();
    for start in [0u64, 100, 200, 300, 400] {
        runs.insert(start, 100, start as u32);
    }

    let mut available = ByteRanges::default();
    available.insert(0, 250); // trails into the 200..300 atom
    available.insert(275, 225); // starts inside it, then covers two atoms

    let materializable = runs.materializable_coverage(&available);
    assert_eq!(
        materializable.ranges(),
        &[(0, 200), (300, 500)],
        "partial leading and trailing atoms stay provisional while adjacent complete atoms coalesce"
    );
    for &(start, end) in materializable.ranges() {
        assert!(
            runs.compose(start, end - start).is_some(),
            "every promoted range must remain exactly composable"
        );
    }
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

/// The split this plan exists to state: **payload** goes to the staging root on
/// the complete volume, **working data** stays in the intermediate directory.
///
/// Both halves are asserted, because only asserting the first would pass for a
/// change that moved the whole set — and moving the holds scratch onto the
/// complete volume would put a write-once append log, read back one paged region
/// at a time, on a network filesystem for no benefit at all.
#[test]
fn member_payload_resolves_under_the_staging_root_and_scratch_under_the_working_dir() {
    let plan = envelope_plan();
    assert_ne!(
        plan.working_dir, plan.destination_dir,
        "non-vacuity: a fixture whose roots are equal proves nothing"
    );

    for member in [
        "Silver.Horizon.S01E05.mkv",
        "Extras/Behind.The.Scenes.mkv",
        "Extras\\Windows.Separated.mkv",
    ] {
        let partial = plan.destination_path(&plan.member_partial_path(member).unwrap());
        let destination = plan.member_output_path(member).unwrap();
        for path in [&partial, &destination] {
            assert!(
                path.starts_with(&plan.destination_dir),
                "{member}: payload must be born on the complete volume, got {}",
                path.display()
            );
            assert!(
                !path.starts_with(&plan.working_dir),
                "{member}: no payload path may resolve under the working directory, got {}",
                path.display()
            );
        }
    }

    // Working data, every kind of it.
    for (label, path) in [
        ("holds scratch", plan.holds_scratch_path()),
        ("envelope", plan.envelope_path(0)),
        ("repair scratch", plan.repair_path(1)),
    ] {
        assert!(
            path.starts_with(&plan.working_dir),
            "{label} is working data and must stay in the intermediate directory, got {}",
            path.display()
        );
        assert!(
            !path.starts_with(&plan.destination_dir),
            "{label} must not be written onto the complete volume, got {}",
            path.display()
        );
    }
}

/// The tripwire for the rule a cross-device rename would break.
///
/// Direct-store performs exactly one rename — `finalize_direct_set` turning a
/// member's `.direct.partial` into the member — and its two sides must resolve
/// under the *same* root. A temp-then-rename whose temp is created in the
/// working directory returns `EXDEV` the moment intermediate and complete are
/// different filesystems, which is precisely the copy this split removes; a unit
/// test cannot make two filesystems, but it can hold the derivation to the rule.
#[test]
fn the_commit_rename_never_crosses_a_root() {
    let plan = envelope_plan();
    for member in ["Silver.Horizon.S01E05.mkv", "Extras/Behind.The.Scenes.mkv"] {
        let source = plan.destination_path(&plan.member_partial_path(member).unwrap());
        let target = plan.member_output_path(member).unwrap();
        assert_eq!(
            source.parent().map(std::path::Path::to_path_buf),
            target.parent().map(std::path::Path::to_path_buf),
            "{member}: a commit is a rename, so its two sides must share a directory"
        );
    }
}

/// A destination key decides the root, and it is the key rather than the path
/// text that decides it.
#[test]
fn a_barrier_destination_resolves_against_the_root_its_key_names() {
    let plan = envelope_plan();
    for volume_index in plan.volumes.keys().copied().collect::<Vec<_>>() {
        let key = super::super::set::envelope_destination_key(volume_index);
        assert!(plan.is_envelope_destination(key));
        assert_eq!(
            plan.barrier_destination_path(key, &plan.envelope_relative_path(volume_index)),
            plan.envelope_path(volume_index),
            "an envelope claim resolves in the working directory"
        );
    }
    // Member ids are handed out from zero and never reach the envelope band.
    let partial = plan
        .member_partial_path("Silver.Horizon.S01E05.mkv")
        .unwrap();
    for member_id in [0u32, 1, 2, 4_096] {
        assert!(!plan.is_envelope_destination(member_id));
        assert_eq!(
            plan.barrier_destination_path(member_id, &partial),
            plan.destination_path(&partial),
            "a member claim resolves in the staging root"
        );
    }
}

/// Every derived namespace carries the set discriminator, because every one of
/// them can be reached by two sets of one job:
/// member names are shared freely between archives, and `sanitize_dirname` is
/// many-to-one, so `A/B` and `A_B` are one stem. The discriminator — the set's
/// lowest NZB file index, unique per job by construction — is what keeps them
/// two files. The holds scratch had this from the start; the other three
/// namespaces found out the slow way.
#[test]
fn two_sets_of_one_job_never_share_a_derived_path() {
    let first = envelope_plan();
    let mut second = DirectSetPlan {
        set_name: "Silver.Horizon/S01E05".to_string(),
        volumes: [(0u32, 2u32), (1, 3)].into_iter().collect(),
        files: [(0u32, 2u32), (1, 3)].into_iter().collect(),
        identity: None,
        working_dir: first.working_dir.clone(),
        destination_dir: first.destination_dir.clone(),
    };
    // The set names sanitize to one stem, so without the discriminator every
    // set-derived path below would be equal.
    assert_ne!(
        first.envelope_relative_path(0),
        second.envelope_relative_path(0),
        "envelopes must not collide across sets whose names sanitize identically"
    );
    assert_ne!(
        first.repair_relative_path(0),
        second.repair_relative_path(0),
        "repair scratch must not collide either"
    );
    assert_ne!(
        first.holds_scratch_relative_path(),
        second.holds_scratch_relative_path(),
        "holds scratch keeps the property it always had"
    );
    // The member-derived path has no set component at all, so an ordinary
    // shared member name is all it takes.
    assert_ne!(
        first.member_partial_path("Silver.Horizon.S01E05.mkv"),
        second.member_partial_path("Silver.Horizon.S01E05.mkv"),
        "a member name shared by two sets must map to two partials"
    );
    // And the final destination deliberately stays undiscriminated: it is the
    // user-visible name, resolved by rename order exactly as two conventionally
    // extracted archives resolve by extraction order.
    assert_eq!(
        first.member_output_path("Silver.Horizon.S01E05.mkv"),
        second.member_output_path("Silver.Horizon.S01E05.mkv"),
    );
    // Restart derives the same discriminator from the same spec: it is the
    // minimum file index, not arrival order, so it cannot move between runs.
    second.volumes = [(1u32, 3u32), (0, 2)].into_iter().collect();
    assert!(
        second
            .member_partial_path("x.mkv")
            .unwrap()
            .contains(".f2.")
    );
}

/// Envelope v2 replaces the first shape's
/// `envelope_offsets_split_each_volume_slot…` test, which asserted a 64 KiB
/// half-slot layout that no longer exists: there is no slot arithmetic to
/// overflow, because a byte's envelope offset *is* its physical offset in the
/// volume.
#[test]
fn each_volume_owns_a_separate_sparse_envelope_file() {
    let plan = envelope_plan();

    assert_eq!(
        plan.envelope_relative_path(0),
        "Silver.Horizon.S01E05.f0.vol00000.envelope"
    );
    assert_eq!(
        plan.envelope_relative_path(7),
        "Silver.Horizon.S01E05.f0.vol00007.envelope",
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
    use super::super::set::envelope_destination_key;

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
        Ok("Silver.Horizon.S01E05.mkv.f0.direct.partial".to_string())
    );
    // The directory component survives: the partial lives beside where the
    // member will land, not flattened into the working directory root.
    assert_eq!(
        plan.member_partial_path("Silver.Horizon/S01E05.mkv"),
        Ok("Silver.Horizon/S01E05.mkv.f0.direct.partial".to_string())
    );
    // A backslash names a directory on every platform: Windows treats it as a
    // separator, and everywhere else the same rewrite the extractor applies to
    // its own destinations turns it into one. Both sides agree, which is what
    // lets the partial be renamed onto the member's output path.
    assert_eq!(
        plan.member_partial_path("Silver.Horizon\\S01E05.mkv"),
        Ok("Silver.Horizon/S01E05.mkv.f0.direct.partial".to_string())
    );
    assert_eq!(
        plan.member_partial_path("./nested/./S01E05.mkv"),
        Ok("nested/S01E05.mkv.f0.direct.partial".to_string())
    );

    // The router runs a raw header name through `unrar_rs::sanitize_path`
    // before the validator, which is what the incremental extractor does — the
    // "sanitize-don't-reject" rule. A traversal is therefore *stripped* rather
    // than refused, exactly as the extractor strips it, and only a name that
    // sanitizes to nothing at all has no destination. The invariant that
    // matters is unchanged: whatever comes out is confined to the working
    // directory.
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
        identity: None,
        working_dir: std::path::PathBuf::from("/tmp/silver-horizon"),
        destination_dir: std::path::PathBuf::from("/tmp/complete/.weaver-staging/1"),
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
    // conditional on the controller.
    let mut set = super::super::set::DirectSet::new(JOB, envelope_plan());
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
fn repair_batches_do_not_recreate_a_checkpoint_between_replacement_stripes() {
    repair_batch_checkpoint(false);
}

#[test]
fn repair_batches_keep_checkpoints_fenced_after_a_failed_closing_stripe() {
    repair_batch_checkpoint(true);
}

fn repair_batch_checkpoint(corrupt: bool) {
    use super::super::set::DirectSet;

    let image = vec![42; 400];
    let (router, _) = straddle_router(&image, 64);
    let mut set = DirectSet::new(JOB, router.plan().clone());
    set.router = router;
    set.router.stage_for_test(0, 64, &image);
    let spans = set.router.drain_for_test(0).unwrap();
    set.record_writes(&spans, Instant::now());
    let recorder = Recorder::default();
    let checkpoint = |set: &mut DirectSet| {
        set.run_barrier(
            BarrierTrigger::Demand(BarrierDemand::RepairRecreate),
            Instant::now(),
            &mut recorder.clone(),
            &mut recorder.clone(),
            &mut recorder.clone(),
        )
    };
    checkpoint(&mut set).unwrap().unwrap();
    assert!(recorder.committed().is_some());
    set.delete_checkpoint_row(&mut recorder.clone()).unwrap();
    assert!(recorder.committed().is_none());

    let spans = set
        .router
        .route_repaired_batch(0, &[(64, Arc::from(&image[..200]))], &[], false, false)
        .unwrap();
    set.record_writes(&spans, Instant::now());
    let operations = recorder.ops();
    assert!(checkpoint(&mut set).is_none());
    assert_eq!(
        recorder.ops(),
        operations,
        "no drain, sync or persist during the replacement"
    );
    assert!(recorder.committed().is_none());

    let mut tail = image[200..].to_vec();
    if corrupt {
        tail[0] ^= 1;
    }
    let result = set
        .router
        .route_repaired_batch(0, &[(264, Arc::from(tail))], &[], false, true);
    if corrupt {
        assert!(result.is_err());
        assert!(set.router.repair_batch_in_progress());
        assert!(!set.router.all_members_verified());
        assert!(checkpoint(&mut set).is_none());
        assert_eq!(recorder.ops(), operations);
        assert!(recorder.committed().is_none());
        return;
    }
    let spans = result.unwrap();
    set.record_writes(&spans, Instant::now());
    assert!(set.router.all_members_verified());
    checkpoint(&mut set).unwrap().unwrap();
    assert!(recorder.committed().is_some());
}

#[test]
fn a_finalized_set_refuses_to_be_demoted() {
    // The two terminal states are mutually exclusive, and finalization is the
    // one that already renamed members onto their destinations. Demoting after
    // it would delete completed output.
    let mut set = super::super::set::DirectSet::new(JOB, envelope_plan());
    set.mark_finalized();
    set.demote(super::super::router::DemotionReason::HoldsBudgetExceeded);

    assert!(set.is_finalized());
    assert!(!set.is_demoted());
}

// ---------------------------------------------------------------------------
// The hybrid virtual-volume provider
// ---------------------------------------------------------------------------

/// A hold is a posted byte too. The bytes a router could not route yet — an
/// encrypted member's edge block waiting for the article on the other side of a
/// hole — sit in staging, and the virtual volume serves them from there: read
/// as posted, claimed as coverage, and composed against the article CRC like
/// every placed byte around them.
#[test]
fn a_virtual_volume_serves_its_holds_as_posted_bytes() {
    use std::io::Read;

    // Member A's bytes 100..140 were never placed; they are held instead.
    let total = whole_volume_covered().end();
    let mut covered = ByteRanges::new();
    covered.insert(0, 100);
    covered.insert(140, total - 140);
    let mut fixture = provider_fixture(covered.clone());
    let held: Arc<[u8]> = Arc::from(&fixture.conventional[100..140]);
    fixture.volume.held = Arc::new(vec![super::super::provider::HeldRun::memory(
        100, held, 0, 40,
    )]);

    assert_eq!(
        fixture.volume.readable_prefix(),
        Some(fixture.conventional.len() as u64),
        "with the hold, the volume reads as one whole run from zero"
    );
    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut reader = provider.open(0).expect("volume 0 is registered");
    let mut read = Vec::new();
    reader
        .read_to_end(&mut read)
        .expect("the whole volume reads");
    assert_eq!(
        read, fixture.conventional,
        "held bytes read back exactly as posted"
    );

    // And the sweep composes across them: the article holding the gap is whole
    // again once the hold counts as covered.
    let crcs = provider_article_crcs(&fixture.conventional);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");
    let mut with_holds = covered;
    with_holds.insert(100, 40);
    let rebuilt = sweep_volumes(
        &provider,
        &[reconstruction_target(
            &fixture,
            path.clone(),
            with_holds,
            crcs,
        )],
        super::super::sparse::SparseMarking::Platform,
    )
    .expect("every article composes once the hold is in the coverage");
    assert_eq!(rebuilt[0].contiguous, fixture.conventional.len() as u64);
    assert_eq!(std::fs::read(&path).unwrap(), fixture.conventional);
}

/// A hold the budget paged out is served from the scratch image, positionally,
/// when a read lands on it. The provider owns no copy: what the budget paged
/// out stays out, whatever the size of the holds.
#[test]
fn a_virtual_volume_reads_a_paged_hold_from_the_scratch_on_demand() {
    use std::io::{Read, Seek, SeekFrom};

    let total = whole_volume_covered().end();
    let mut covered = ByteRanges::new();
    covered.insert(0, 100);
    covered.insert(140, total - 140);
    let mut fixture = provider_fixture(covered);

    // The router paged the hold behind an earlier region, so the run's scratch
    // offset is not its physical offset and a read has to use the right one.
    let mut scratch = super::super::router::HoldsScratch::new(
        fixture._dir.path().join(".weaver-holds.silver.horizon.f0"),
        1 << 20,
    );
    scratch
        .append(b"an earlier hold that was placed since")
        .unwrap();
    let paged_at = scratch.append(&fixture.conventional[100..140]).unwrap();
    let pin = scratch.pin().unwrap();
    fixture.volume.held = Arc::new(vec![super::super::provider::HeldRun::scratch(
        100, pin, paged_at, 40,
    )]);

    assert_eq!(
        fixture.volume.readable_prefix(),
        Some(fixture.conventional.len() as u64),
        "a paged hold claims and sources its bytes exactly like a resident one"
    );
    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut reader = provider.open(0).expect("volume 0 is registered");
    let mut read = Vec::new();
    reader
        .read_to_end(&mut read)
        .expect("the whole volume reads");
    assert_eq!(
        read, fixture.conventional,
        "the paged hold reads back exactly as posted, through the scratch"
    );

    // A read starting inside the run offsets into the scratch region.
    reader.seek(SeekFrom::Start(120)).unwrap();
    let mut tail = [0u8; 20];
    reader.read_exact(&mut tail).unwrap();
    assert_eq!(&tail, &fixture.conventional[120..140]);
}

#[test]
fn a_virtual_volume_reads_back_exactly_what_a_downloaded_volume_would_hold() {
    use std::io::{Read, Seek};

    let fixture = provider_fixture(whole_volume_covered());
    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
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
    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
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

    // The failure this is named for, constructed exactly: every byte of the
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
    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut reader = provider.open(0).unwrap();
    reader.seek(SeekFrom::Start(member_a_at + 100)).unwrap();
    let error = reader
        .read(&mut [0u8; 16])
        .expect_err("a covered range no source backs must not read as data");
    assert!(
        super::super::provider::is_hole(&error),
        "a covered range with no extent must report a hole, got {error}"
    );

    // The header before it still reads: the envelope answers for what the
    // envelope was actually written, and only that.
    let mut reader = provider.open(0).unwrap();
    let mut head = vec![0u8; PROVIDER_HEADER];
    reader.read_exact(&mut head).unwrap();
    assert_eq!(head, &fixture.conventional[..PROVIDER_HEADER]);
    assert!(super::super::provider::is_hole(
        &reader.read(&mut [0u8; 16]).unwrap_err()
    ));

    // And with the extent restored the same bytes read back as the real volume,
    // so the refusal above is about the missing extent and nothing else.
    let honest = provider_fixture(whole_volume_covered());
    let provider = super::super::provider::HybridVolumeProvider::new(vec![honest.volume.clone()]);
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
    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);

    // A read starting inside the hole fails, and fails *distinguishably*: a
    // caller has to be able to tell "not downloaded" from "the disk is broken".
    let mut reader = provider.open(0).unwrap();
    reader.seek(SeekFrom::Start(hole_start + 10)).unwrap();
    let error = reader.read(&mut [0u8; 16]).unwrap_err();
    assert!(
        super::super::provider::is_hole(&error),
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
    assert!(super::super::provider::is_hole(&error));

    // The trailer was never covered either, so the volume's own tail is a hole
    // even though the file it would come from exists and is long enough.
    let mut reader = provider.open(0).unwrap();
    reader
        .seek(SeekFrom::Start(
            (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B) as u64,
        ))
        .unwrap();
    assert!(super::super::provider::is_hole(
        &reader.read(&mut out).unwrap_err()
    ));

    // And a plain I/O failure is *not* a hole, so the two never get confused.
    let missing = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
    assert!(!super::super::provider::is_hole(&missing));
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

    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut reader = provider.open(0).unwrap();
    reader
        .seek(SeekFrom::Start(
            (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP) as u64,
        ))
        .unwrap();
    let error = reader.read(&mut [0u8; 16]).unwrap_err();
    assert!(
        super::super::provider::is_hole(&error),
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
    use unrar_rs::VolumeProvider;

    let fixture = provider_fixture(whole_volume_covered());
    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    assert!(provider.get_volume(0).is_ok());
    assert!(
        provider.get_volume(1).is_err(),
        "an unregistered volume is unavailable, not an empty one"
    );
    assert!(provider.volume(0).is_some());
    assert!(provider.volume(9).is_none());
}

// ---------------------------------------------------------------------------
// The re-encrypting overlay
// ---------------------------------------------------------------------------

#[test]
fn the_overlay_reads_an_encrypted_member_back_as_it_was_posted() {
    use std::io::Read;

    // The whole point of the crypt-state restore in one assertion: what is on
    // disk is plaintext, and what comes out of the provider is the cipher that
    // was posted — including the final block, whose plaintext runs past the
    // member's end into the retained tail padding.
    let dir = tempfile::tempdir().unwrap();
    let (posted, plain, crypt, covered) = encrypted_member_facts(3000, 256);
    assert_ne!(posted[..plain.len()], plain[..], "the fixture must encrypt");
    let facts = crypt
        .cipher_facts(plain.len() as u64, &covered)
        .expect("a sized member has read-side facts");
    let volume = cipher_volume(dir.path(), &plain, facts, plain.len() as u64);
    let provider = super::super::provider::HybridVolumeProvider::new(vec![volume]);

    let mut reader = provider.open(0).expect("registered");
    let mut read_back = Vec::new();
    reader.read_to_end(&mut read_back).unwrap();
    assert_eq!(
        read_back,
        posted[..plain.len()],
        "a sequential sweep must reproduce the posted stream exactly"
    );
    let counters = provider.cipher_counters();
    assert_eq!(counters.refusals(), 0);
    assert_eq!(
        counters.chained_bytes(),
        0,
        "a sequential sweep carries its own chain and must never re-encrypt a \
         byte twice"
    );
}

#[test]
fn a_ranged_read_across_every_checkpoint_boundary_reproduces_the_posted_bytes() {
    // The checkpoint risk in one line: a stale or wrong 16-byte seed corrupts
    // exactly the first block of a read and leaves the rest correct, which no
    // checksum downstream could attribute. So every window that starts and ends
    // on either side of a boundary is read on its own, through a *fresh* reader
    // each time, so nothing rides the previous read's chain.
    let dir = tempfile::tempdir().unwrap();
    // Two strides plus change, decrypted in stride-crossing pieces, so the
    // checkpoint map has both frontier and strided entries in it.
    let stride = super::super::router::crypt::CHECKPOINT_STRIDE as usize;
    let (posted, plain, crypt, covered) =
        encrypted_member_facts(stride * 2 + 3000, stride / 2 + 48);
    let facts = crypt
        .cipher_facts(plain.len() as u64, &covered)
        .expect("a sized member has read-side facts");
    assert!(
        facts.checkpoint_count() >= 3,
        "non-vacuity: the fixture must retain strided checkpoints, got {}",
        facts.checkpoint_count()
    );
    let volume = cipher_volume(dir.path(), &plain, facts, plain.len() as u64);
    let provider = super::super::provider::HybridVolumeProvider::new(vec![volume]);

    let mut offsets: Vec<u64> = Vec::new();
    for boundary in [stride as u64, (stride * 2) as u64] {
        // Straddling, exactly on, just under and just over — in both block-
        // aligned and misaligned form.
        offsets.extend([
            boundary - 33,
            boundary - 16,
            boundary - 1,
            boundary,
            boundary + 1,
            boundary + 16,
        ]);
    }
    // And an offset far below every checkpoint, which has no reachable seed and
    // must take the sequential fallback rather than guess one.
    offsets.push(64);
    offsets.push(0);

    let mut reads = 0u64;
    for offset in offsets {
        for len in [1u64, 15, 16, 17, 4096] {
            let end = (offset + len).min(plain.len() as u64);
            if end <= offset {
                continue;
            }
            // A fresh reader per window, so nothing rides the previous read's
            // chain and every one of these really does seed itself.
            let mut reader = provider.open(0).expect("registered");
            std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(offset)).unwrap();
            let mut got = vec![0u8; (end - offset) as usize];
            std::io::Read::read_exact(&mut reader, &mut got).unwrap();
            assert_eq!(
                got,
                posted[offset as usize..end as usize],
                "a ranged read at {offset} for {len} must reproduce the posted bytes"
            );
            reads += 1;
        }
    }
    let counters = provider.cipher_counters();
    assert_eq!(counters.refusals(), 0, "nothing here may refuse");
    assert!(
        counters.seeded_from_checkpoint() > 0,
        "the checkpoints must actually have been used"
    );
    assert!(
        counters.seeded_from_start() > 0,
        "and the no-reachable-checkpoint case must really have taken the \
         sequential fallback"
    );
    // The bound the stride exists for: a read below the first checkpoint chains
    // from the member's start, and every other one chains at most one stride —
    // never the whole member, which is what a frontier-only checkpoint map would
    // have cost.
    assert!(
        counters.chained_bytes() <= reads * super::super::router::crypt::CHECKPOINT_STRIDE,
        "checkpoint misses must stay bounded by the stride: {} chained over {reads} reads",
        counters.chained_bytes()
    );
}

#[test]
fn a_member_whose_tail_padding_is_not_whole_refuses_its_final_block() {
    use std::io::Read;

    // The tail padding, read from the other end. The final cipher block covers
    // bytes past `unpacked_size` that no destination holds, so without them it
    // cannot be re-encrypted — and neither can the destination bytes *inside*
    // it. Fabricating a padding would produce a structurally perfect cipher
    // block that is not the one that was posted, which PAR2 would report as
    // damage in a byte-perfect volume.
    let dir = tempfile::tempdir().unwrap();
    let material = unrar_rs::derive_rar5_material("moonlit-harbour", &CIPHER_SALT, CIPHER_LG2)
        .expect("derivable");
    let facts = unrar_rs::RarVolumeMemberEncryptionFacts {
        version: 0,
        kdf_count_lg2: CIPHER_LG2,
        salt: CIPHER_SALT,
        iv: CIPHER_IV,
        psw_check_present: false,
        psw_check: None,
    };
    let payload_len = 3000usize;
    let cipher_len = payload_len.div_ceil(16) * 16;
    let plain: Vec<u8> = (0..payload_len).map(|index| (index % 251) as u8).collect();

    // Everything decrypted and every destination byte covered — but the padding
    // never retained, which is what a run that stopped before the final
    // article's second half leaves behind.
    let mut crypt = super::super::router::crypt::MemberCrypt::new(
        super::super::router::crypt::MemberKeys {
            key: unrar_rs::MemberCipherKey::Aes256(material.key),
            hash_key: Some(material.hash_key),
            iv: CIPHER_IV,
        },
        &unrar_rs::MemberKeying::Rar5(facts),
    );
    crypt.observe(&unrar_rs::EncryptedStore {
        format: unrar_rs::ArchiveFormat::Rar5,
        crypt: Some(facts),
        rar4_salt: None,
        cipher_size: Some(cipher_len as u64),
        tail_padding: Some((cipher_len - payload_len) as u8),
        resolved: true,
    });
    let mut covered = ByteRanges::new();
    covered.insert(0, payload_len as u64);
    let read_side = crypt
        .cipher_facts(payload_len as u64, &covered)
        .expect("a sized member still has facts");
    assert!(
        read_side.tail_plain().is_none(),
        "non-vacuity: the padding must really be missing"
    );

    let volume = cipher_volume(dir.path(), &plain, read_side, payload_len as u64);
    let provider = super::super::provider::HybridVolumeProvider::new(vec![volume]);
    let mut reader = provider.open(0).expect("registered");
    let mut read_back = Vec::new();
    let error = reader
        .read_to_end(&mut read_back)
        .expect_err("the final block has no byte-exact source");
    assert!(
        super::super::provider::is_hole(&error),
        "the refusal must read as a hole — 'refetch this' — rather than as a \
         broken disk, got {error}"
    );

    // And the refusal is scoped to the block it is about: everything below the
    // final cipher block still has a byte-exact source and is still served.
    let last_block = (payload_len / 16) * 16;
    let mut reader = provider.open(0).expect("registered");
    let mut below = vec![0u8; last_block];
    reader
        .read_exact(&mut below)
        .expect("every whole block below the final one is reproducible");
    let mut reader = provider.open(0).expect("registered");
    std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(last_block as u64)).unwrap();
    let error = reader
        .read(&mut [0u8; 8])
        .expect_err("the final block itself has nothing to re-encrypt from");
    assert!(super::super::provider::is_hole(&error), "got {error}");
}

#[test]
fn the_overlay_refuses_a_member_whose_plaintext_has_a_hole_below_the_read() {
    // CBC's own consequence, stated so it is not rediscovered as a bug: cipher
    // block N needs block N−1, so a gap anywhere below an offset makes every
    // byte above it unreproducible. The partial is a sparse file, so the gap
    // reads back as zeros — well-formed plaintext that would encrypt into
    // well-formed cipher nothing ever posted.
    let dir = tempfile::tempdir().unwrap();
    let (_posted, plain, crypt, _) = encrypted_member_facts(3000, 256);
    // Same member, same partial — but a destination coverage map with a gap in
    // its middle, which is what a run that lost an article leaves behind.
    let mut holed = ByteRanges::new();
    holed.insert(0, 1000);
    holed.insert(2000, 1000);
    let facts = crypt
        .cipher_facts(plain.len() as u64, &holed)
        .expect("a sized member still has facts");
    let volume = cipher_volume(dir.path(), &plain, facts, plain.len() as u64);
    let provider = super::super::provider::HybridVolumeProvider::new(vec![volume]);

    let mut reader = provider.open(0).expect("registered");
    std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(2048)).unwrap();
    let error = std::io::Read::read(&mut reader, &mut [0u8; 64])
        .expect_err("bytes above a gap cannot be re-encrypted");
    assert!(
        super::super::provider::is_hole(&error),
        "an unreproducible range is a hole, got {error}"
    );
    assert!(provider.cipher_counters().refusals() > 0);
}

// ---------------------------------------------------------------------------
// The reconstruction sweep, and the verification that gates it
// ---------------------------------------------------------------------------

#[test]
fn a_partially_covered_volume_is_rebuilt_and_verified_run_by_run() {
    // The composition is over the runs it was fed, so a covered prefix that
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

    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let rebuilt = sweep_volumes(
        &provider,
        &[reconstruction_target(
            &fixture,
            path.clone(),
            covered,
            crcs.clone(),
        )],
        super::super::sparse::SparseMarking::Platform,
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
fn materialized_segments_above_a_hole_stay_committed_without_advancing_the_floor() {
    let extents =
        std::collections::BTreeMap::from([(0, (0, 100)), (1, (100, 100)), (2, (200, 100))]);
    let mut coverage = ByteRanges::new();
    coverage.insert(0, 100);
    coverage.insert(200, 100);

    let (kept, floor) = super::super::reconstruct::segments_on_disk(&extents, &coverage, 100);

    assert_eq!(kept, vec![0, 2]);
    assert_eq!(floor, 100, "only the contiguous prefix persists");
}

#[test]
fn a_covered_run_with_no_composed_reference_refuses_only_the_articles_it_covers() {
    // "Verify where available" is the wrong default for a sweep that reads
    // through sparse files: a chunk with no reference value is refused rather
    // than put under a published floor.
    //
    // The refusal is charged to that chunk alone. A covered range is a *merged*
    // run — on a real volume it is every article the coverage ever abutted — so
    // charging it to the range would mean one unclosable frontier costing every
    // article below it. That frontier is exactly the shape a demotion catching a
    // volume mid-download produces, which is why it must cost the frontier and
    // nothing else.
    let fixture = provider_fixture(whole_volume_covered());
    let crcs = provider_article_crcs(&fixture.conventional);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");

    // Two whole articles and then a frontier that stops inside a third, which no
    // composition can vouch for.
    let mut covered = ByteRanges::new();
    covered.insert(0, 250);

    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let rebuilt = super::super::reconstruct::reconstruct_volumes(
        &provider,
        &[reconstruction_target(&fixture, path.clone(), covered, crcs)],
        super::super::sparse::SparseMarking::Platform,
    );
    assert_eq!(
        rebuilt[0].failure,
        Some(
            super::super::reconstruct::ReconstructionFailure::UnverifiableRun {
                volume_index: 0,
                offset: 200,
            }
        ),
        "the refusal names the frontier, not the start of the merged run"
    );
    assert_eq!(
        rebuilt[0].verified.ranges(),
        &[(0, 200)],
        "the two whole articles below the frontier are still vouched for"
    );
    assert_eq!(rebuilt[0].contiguous, 200);
    assert!(!rebuilt[0].complete);

    let written = std::fs::read(&path).unwrap();
    assert_eq!(
        &written[..200],
        &fixture.conventional[..200],
        "the verified articles are byte-exact"
    );
    assert!(
        written[200..250].iter().all(|byte| *byte == 0),
        "the refused frontier is left as a hole for the refetch rather than bytes \
         nothing checked"
    );
}

/// The repair scratch carries a run that stops inside an article through: the
/// composition vouches for it up to the last article boundary, and the placed
/// prefix of the article it stops inside is written after that with no
/// reference. An encrypted member's frontier before a hole is always this shape
/// — its final cipher block waits for the block after it — and PAR2 needs the
/// slices it judged valid there as repair input, so refusing the run demoted
/// every encrypted set the moment it needed a repair.
#[test]
fn a_repair_scratch_carries_a_run_that_stops_inside_an_article() {
    let fixture = provider_fixture(whole_volume_covered());
    let crcs = provider_article_crcs(&fixture.conventional);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");

    // Two whole articles and half of a third.
    let mut covered = ByteRanges::new();
    covered.insert(0, 250);

    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let rebuilt = sweep_volumes(
        &provider,
        &[repair_scratch_target(&fixture, path.clone(), covered, crcs)],
        super::super::sparse::SparseMarking::Platform,
    )
    .expect("the whole articles verify and the partial one is carried through");

    assert_eq!(
        rebuilt[0].contiguous, 200,
        "the floor stops at the last article boundary the composition vouched for"
    );
    assert!(!rebuilt[0].complete);
    let written = std::fs::read(&path).unwrap();
    assert_eq!(
        &written[..250],
        &fixture.conventional[..250],
        "every placed byte is in the scratch, the carried prefix included"
    );
    assert!(
        written[250..].iter().all(|byte| *byte == 0),
        "nothing past the placed frontier is invented"
    );
}

/// The carried remainder is exactly one article's placed prefix: a run that
/// starts off a boundary, or reaches past the article the composition knows,
/// is bytes no article record accounts for and is refused as before.
#[test]
fn a_carried_remainder_must_be_the_prefix_of_one_known_article() {
    let fixture = provider_fixture(whole_volume_covered());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");
    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);

    // Starts inside an article: nothing composes from 50.
    let mut off_boundary = ByteRanges::new();
    off_boundary.insert(50, 200);
    let failure = sweep_volumes(
        &provider,
        &[repair_scratch_target(
            &fixture,
            path.clone(),
            off_boundary,
            provider_article_crcs(&fixture.conventional),
        )],
        super::super::sparse::SparseMarking::Platform,
    )
    .expect_err("a run that starts off an article boundary has no reference at all");
    assert_eq!(
        failure,
        super::super::reconstruct::ReconstructionFailure::UnverifiableRun {
            volume_index: 0,
            offset: 50,
        }
    );

    // The composition knows the first two articles only, and the run reaches
    // into a third no record accounts for.
    let mut into_the_unknown = ByteRanges::new();
    into_the_unknown.insert(0, 250);
    let failure = sweep_volumes(
        &provider,
        &[repair_scratch_target(
            &fixture,
            path.clone(),
            into_the_unknown,
            provider_article_crcs(&fixture.conventional[..200]),
        )],
        super::super::sparse::SparseMarking::Platform,
    )
    .expect_err("a remainder past the last known article is not a prefix of one");
    assert_eq!(
        failure,
        super::super::reconstruct::ReconstructionFailure::UnverifiableRun {
            volume_index: 0,
            offset: 200,
        }
    );
    // The two articles the composition does know were verified and written on
    // the way to that refusal; only the remainder past them is left as a hole.
    let written = std::fs::read(&path).unwrap();
    assert_eq!(&written[..200], &fixture.conventional[..200]);
    assert!(
        written[200..].iter().all(|byte| *byte == 0),
        "a refused remainder leaves a hole rather than bytes no article record accounts for"
    );
}

/// Carrying the remainder does not loosen the check on what comes before it:
/// the whole articles of the run are still verified against the composition.
#[test]
fn a_carried_remainder_still_verifies_the_articles_before_it() {
    let fixture = provider_fixture(whole_volume_covered());
    let mut corrupted = fixture.conventional.clone();
    corrupted[PROVIDER_HEADER + 10] ^= 0xFF;
    let crcs = provider_article_crcs(&corrupted);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");

    let mut covered = ByteRanges::new();
    covered.insert(0, 250);

    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let failure = sweep_volumes(
        &provider,
        &[repair_scratch_target(&fixture, path.clone(), covered, crcs)],
        super::super::sparse::SparseMarking::Platform,
    )
    .expect_err("the verified head still has to match its reference");
    assert_eq!(
        failure,
        super::super::reconstruct::ReconstructionFailure::ChecksumMismatch {
            volume_index: 0,
            offset: 0,
        }
    );
    // The bytes the sweep read before the reference disagreed are on disk. They
    // are harmless because they are not in `verified`: every article over them
    // is refetched and overwritten, and no floor is published across them.
    assert!(path.exists());
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

    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let rebuilt = super::super::reconstruct::reconstruct_volumes(
        &provider,
        &[reconstruction_target(
            &fixture,
            path.clone(),
            whole_volume_covered(),
            crcs,
        )],
        super::super::sparse::SparseMarking::Platform,
    );
    assert!(matches!(
        rebuilt[0].failure,
        Some(
            super::super::reconstruct::ReconstructionFailure::ChecksumMismatch {
                volume_index: 0,
                ..
            }
        )
    ));
    // The corrupted byte is in the volume's first article, and the cost is that
    // article. `verified` is the only claim the caller may act on, so what
    // matters is that it does not name the disagreeing range — and that it does
    // still name the articles beside it, which is the difference between one
    // article off the wire and the whole volume.
    assert!(
        !rebuilt[0].verified.missing(0, 100).is_empty(),
        "an article that disagrees with its composed CRC32 must not be trusted"
    );
    assert!(
        rebuilt[0].verified.missing(100, 460).is_empty(),
        "every article the corruption does not touch is still vouched for"
    );
    assert_eq!(
        rebuilt[0].contiguous, 0,
        "no floor may be published over a volume whose first article is refused"
    );
    assert!(!rebuilt[0].complete);
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

    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    sweep_volumes(
        &provider,
        &[reconstruction_target(
            &fixture,
            path.clone(),
            whole_volume_covered(),
            crcs,
        )],
        super::super::sparse::SparseMarking::Platform,
    )
    .expect("the volume is wholly covered and verifiable");

    assert_eq!(
        std::fs::read(&path).unwrap(),
        fixture.conventional,
        "the rebuilt volume is the volume, with no stale tail past its end"
    );
}
