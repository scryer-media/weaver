use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::*;

const TEST_BOUNDS: &[f64] = &[0.001, 0.01, 0.1];

#[test]
fn histogram_assigns_observations_to_the_first_bound_that_covers_them() {
    let histogram = AtomicHistogram::new(TEST_BOUNDS);
    // Upper-inclusive: exactly on a bound lands in that bound's bucket.
    histogram.observe(Duration::from_micros(500)); // 0.0005 -> bucket 0
    histogram.observe(Duration::from_millis(1)); // 0.001   -> bucket 0
    histogram.observe(Duration::from_millis(5)); // 0.005   -> bucket 1
    histogram.observe(Duration::from_millis(100)); // 0.1   -> bucket 2
    histogram.observe(Duration::from_millis(101)); // 0.101 -> +Inf

    let snapshot = histogram.snapshot();
    assert_eq!(snapshot.bounds, TEST_BOUNDS);
    assert_eq!(snapshot.counts.len(), TEST_BOUNDS.len() + 1);
    assert_eq!(snapshot.counts, vec![2, 1, 1, 1]);
    assert_eq!(snapshot.count, 5);
}

#[test]
fn histogram_sum_is_reported_in_seconds() {
    let histogram = AtomicHistogram::new(TEST_BOUNDS);
    histogram.observe(Duration::from_millis(250));
    histogram.observe(Duration::from_millis(750));
    let snapshot = histogram.snapshot();
    assert!(
        (snapshot.sum - 1.0).abs() < 1e-9,
        "sum was {}",
        snapshot.sum
    );
    assert_eq!(snapshot.count, 2);
}

#[test]
fn histogram_snapshot_cumulative_counts_accumulate_left_to_right() {
    let histogram = AtomicHistogram::new(TEST_BOUNDS);
    histogram.observe(Duration::from_micros(100));
    histogram.observe(Duration::from_millis(5));
    histogram.observe(Duration::from_millis(5));
    histogram.observe(Duration::from_secs(1));

    let snapshot = histogram.snapshot();
    assert_eq!(snapshot.counts, vec![1, 2, 0, 1]);
    assert_eq!(snapshot.cumulative_counts(), vec![1, 3, 3, 4]);
    assert_eq!(
        *snapshot.cumulative_counts().last().unwrap(),
        snapshot.count,
        "the +Inf cumulative bucket must equal the total count"
    );
}

#[test]
fn histogram_empty_snapshot_has_a_slot_per_bucket() {
    let snapshot = HistogramSnapshot::empty(ARTICLE_LATENCY_BOUNDS);
    assert_eq!(snapshot.counts.len(), ARTICLE_LATENCY_BOUNDS.len() + 1);
    assert!(snapshot.counts.iter().all(|count| *count == 0));
    assert_eq!(snapshot.count, 0);
    assert_eq!(snapshot.sum, 0.0);
}

#[test]
fn every_shipped_bound_set_fits_the_fixed_bucket_array_and_ascends() {
    for bounds in [
        ARTICLE_LATENCY_BOUNDS,
        DISK_WRITE_DURATION_BOUNDS,
        DECODE_TASK_DURATION_BOUNDS,
        EXTRACT_MEMBER_DURATION_BOUNDS,
        JOB_DURATION_BOUNDS,
        STAGE_DURATION_BOUNDS,
        DB_OP_DURATION_BOUNDS,
        HTTP_REQUEST_DURATION_BOUNDS,
    ] {
        assert!(
            bounds.len() <= HISTOGRAM_MAX_BUCKETS,
            "bound set of {} exceeds the fixed array",
            bounds.len()
        );
        assert!(
            bounds.windows(2).all(|pair| pair[0] < pair[1]),
            "bounds must ascend"
        );
    }
}

#[test]
fn histogram_observe_is_safe_under_concurrency() {
    let histogram = Arc::new(AtomicHistogram::new(TEST_BOUNDS));
    let threads = 8;
    let per_thread = 500;
    let mut handles = Vec::new();
    for thread in 0..threads {
        let histogram = Arc::clone(&histogram);
        handles.push(std::thread::spawn(move || {
            // Deterministic spread across three buckets and the overflow.
            let duration = match thread % 4 {
                0 => Duration::from_micros(100),
                1 => Duration::from_millis(5),
                2 => Duration::from_millis(50),
                _ => Duration::from_secs(2),
            };
            for _ in 0..per_thread {
                histogram.observe(duration);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("observer thread panicked");
    }

    let snapshot = histogram.snapshot();
    assert_eq!(snapshot.count, threads * per_thread);
    assert_eq!(
        snapshot.counts.iter().sum::<u64>(),
        threads * per_thread,
        "no observation may be lost or double-counted"
    );
    assert_eq!(
        snapshot.counts,
        vec![
            2 * per_thread,
            2 * per_thread,
            2 * per_thread,
            2 * per_thread
        ]
    );
}

#[test]
fn server_counters_emit_every_outcome_recovery_pair_even_at_zero() {
    let counters = ServerCounters::new(7);
    counters.note_attempt(ServerAttemptOutcomeKind::Success, false);
    counters.note_attempt(ServerAttemptOutcomeKind::NotFound, true);
    counters.note_attempt(ServerAttemptOutcomeKind::NotFound, true);
    counters.observe_latency(Duration::from_millis(20));

    let snapshot = counters.snapshot(3);
    assert_eq!(snapshot.stable_server_id, 7);
    assert_eq!(snapshot.server_idx, 3);
    assert_eq!(snapshot.attempts.len(), 12);
    let success = snapshot
        .attempts
        .iter()
        .find(|row| row.outcome == "success" && !row.recovery)
        .expect("success row");
    assert_eq!(success.count, 1);
    let not_found_recovery = snapshot
        .attempts
        .iter()
        .find(|row| row.outcome == "not_found" && row.recovery)
        .expect("recovery not_found row");
    assert_eq!(not_found_recovery.count, 2);
    let quota = snapshot
        .attempts
        .iter()
        .find(|row| row.outcome == "quota_blocked" && !row.recovery)
        .expect("quota_blocked row must exist at zero");
    assert_eq!(quota.count, 0);
    assert_eq!(snapshot.article_latency.count, 1);
}

#[test]
fn quota_outcomes_are_the_only_ones_without_a_round_trip() {
    for outcome in ServerAttemptOutcomeKind::ALL {
        assert_eq!(
            outcome.has_round_trip(),
            outcome != ServerAttemptOutcomeKind::QuotaBlocked
        );
    }
}

#[test]
fn registry_reuses_counters_for_a_stable_id_across_activations() {
    let registry = ServerMetricsRegistry::new();
    let generation_one = registry.activate(&[10, 20]);
    generation_one[0].note_attempt(ServerAttemptOutcomeKind::Success, false);
    generation_one[1].note_attempt(ServerAttemptOutcomeKind::Success, false);

    // A reload reorders the servers and drops one; the surviving ids keep their
    // lifetime totals and the new one starts clean.
    let generation_two = registry.activate(&[20, 30]);
    assert_eq!(generation_two.len(), 2);
    assert!(Arc::ptr_eq(&generation_two[0], &generation_one[1]));

    let snapshot = registry.snapshot();
    assert_eq!(snapshot.len(), 2);
    assert_eq!(snapshot[0].stable_server_id, 20);
    assert_eq!(snapshot[0].server_idx, 0);
    let carried = snapshot[0]
        .attempts
        .iter()
        .find(|row| row.outcome == "success" && !row.recovery)
        .expect("success row");
    assert_eq!(carried.count, 1, "lifetime totals survive a reload");
    assert_eq!(snapshot[1].stable_server_id, 30);
    let fresh = snapshot[1]
        .attempts
        .iter()
        .find(|row| row.outcome == "success" && !row.recovery)
        .expect("success row");
    assert_eq!(fresh.count, 0);
}

#[test]
fn registry_snapshot_is_empty_before_any_activation() {
    let registry = ServerMetricsRegistry::new();
    assert!(registry.snapshot().is_empty());
}

#[test]
fn job_lifecycle_counts_submissions_and_terminals_by_category() {
    let metrics = JobLifecycleMetrics::new();
    metrics.note_submitted("api", "movies");
    metrics.note_submitted("api", "movies");
    metrics.note_submitted("rss", "");
    metrics.note_finished(
        JobResultKind::Complete,
        "movies",
        Some(Duration::from_secs(120)),
    );
    metrics.note_finished(JobResultKind::Cancelled, "", None);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.submitted.len(), 2);
    let api = snapshot
        .submitted
        .iter()
        .find(|row| row.origin == "api")
        .expect("api row");
    assert_eq!(api.category, "movies");
    assert_eq!(api.count, 2);
    let rss = snapshot
        .submitted
        .iter()
        .find(|row| row.origin == "rss")
        .expect("rss row");
    assert_eq!(rss.category, "");

    assert_eq!(snapshot.finished.len(), 2);
    let complete_duration = snapshot
        .job_duration
        .iter()
        .find(|(result, _)| *result == "complete")
        .expect("complete duration");
    assert_eq!(complete_duration.1.count, 1);
    let cancelled_duration = snapshot
        .job_duration
        .iter()
        .find(|(result, _)| *result == "cancelled")
        .expect("cancelled duration");
    assert_eq!(
        cancelled_duration.1.count, 0,
        "a terminal without an elapsed measurement must not fabricate one"
    );
}

#[test]
fn job_lifecycle_snapshot_pre_creates_every_enumerated_series() {
    let snapshot = JobLifecycleMetrics::new().snapshot();
    assert_eq!(snapshot.job_duration.len(), JobResultKind::ALL.len());
    assert_eq!(snapshot.stage_duration.len(), JobStageKind::ALL.len());
    assert_eq!(
        snapshot.verifications.len(),
        VerificationOutcomeKind::ALL.len()
    );
    assert_eq!(snapshot.repairs.len(), StageOutcomeKind::ALL.len());
    assert_eq!(snapshot.extractions.len(), StageOutcomeKind::ALL.len());
    assert!(snapshot.submitted.is_empty());
    assert!(snapshot.bytes_by_category.is_empty());
}

#[test]
fn job_lifecycle_records_stage_and_post_processing_outcomes() {
    let metrics = JobLifecycleMetrics::new();
    metrics.note_stage_duration(JobStageKind::Download, Duration::from_secs(30));
    metrics.note_stage_duration(JobStageKind::Repair, Duration::from_secs(3));
    metrics.note_verification(VerificationOutcomeKind::Damaged);
    metrics.note_repair(StageOutcomeKind::Complete, 12);
    metrics.note_extraction(StageOutcomeKind::Failed);
    metrics.note_file_missing(4);

    let snapshot = metrics.snapshot();
    let download = snapshot
        .stage_duration
        .iter()
        .find(|(stage, _)| *stage == "download")
        .expect("download stage");
    assert_eq!(download.1.count, 1);
    assert_eq!(
        snapshot
            .verifications
            .iter()
            .find(|(outcome, _)| *outcome == "damaged")
            .expect("damaged")
            .1,
        1
    );
    assert_eq!(snapshot.repair_slices_repaired_total, 12);
    assert_eq!(
        snapshot
            .extractions
            .iter()
            .find(|(outcome, _)| *outcome == "failed")
            .expect("failed")
            .1,
        1
    );
    assert_eq!(snapshot.files_missing_total, 1);
    assert_eq!(snapshot.missing_segments_total, 4);
}

#[test]
fn category_byte_counters_are_shared_per_category_and_none_maps_to_empty() {
    let metrics = JobLifecycleMetrics::new();
    let movies = metrics.category_bytes_counter(Some("movies"));
    let movies_again = metrics.category_bytes_counter(Some("movies"));
    assert!(Arc::ptr_eq(&movies, &movies_again));
    let uncategorised = metrics.category_bytes_counter(None);
    let empty = metrics.category_bytes_counter(Some(""));
    assert!(Arc::ptr_eq(&uncategorised, &empty));

    movies.fetch_add(1_000, Ordering::Relaxed);
    movies_again.fetch_add(500, Ordering::Relaxed);
    uncategorised.fetch_add(7, Ordering::Relaxed);

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.bytes_by_category,
        vec![(String::new(), 7), ("movies".to_string(), 1_500),]
    );
}

#[test]
fn pipeline_histograms_report_none_until_a_stage_is_actually_timed() {
    let histograms = PipelineHistograms::new();
    let snapshot = histograms.snapshot();
    assert_eq!(snapshot.disk_write_duration.count, 0);
    assert!(snapshot.decode_task_duration.is_none());
    assert!(snapshot.extract_member_duration.is_none());

    histograms.observe_disk_write(Duration::from_millis(2));
    histograms.observe_extract_member(Duration::from_secs(4));
    let snapshot = histograms.snapshot();
    assert_eq!(snapshot.disk_write_duration.count, 1);
    assert!(snapshot.decode_task_duration.is_none());
    assert_eq!(
        snapshot
            .extract_member_duration
            .expect("extract member timed")
            .count,
        1
    );
}

#[test]
fn db_runtime_metrics_track_in_flight_blocked_and_latency() {
    let metrics = DbRuntimeMetrics::new("postgres", 8);
    let first = metrics.note_submission_started();
    let second = metrics.note_submission_started();
    assert_eq!(metrics.snapshot().in_flight, 2);

    metrics.note_submission_blocked();
    metrics.note_submission_finished(Duration::from_millis(4));
    drop(first);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.engine, "postgres");
    assert_eq!(snapshot.concurrency, 8);
    assert_eq!(snapshot.in_flight, 1);
    assert_eq!(snapshot.blocked_submissions_total, 1);
    assert_eq!(snapshot.op_duration.count, 1);
    drop(second);
    assert_eq!(metrics.snapshot().in_flight, 0);
}

/// An operation that bails out before it is answered (worker stopped, worker
/// panicked) must still release its in-flight slot: the guard, not a manual
/// call, owns the decrement.
#[test]
fn db_runtime_in_flight_is_released_when_the_guard_drops_early() {
    let metrics = DbRuntimeMetrics::new("sqlite", 1);
    {
        let _in_flight = metrics.note_submission_started();
        assert_eq!(metrics.snapshot().in_flight, 1);
        // No `note_submission_finished`: simulate an early `?` return.
    }
    assert_eq!(metrics.snapshot().in_flight, 0);
    assert_eq!(metrics.snapshot().op_duration.count, 0);
}
