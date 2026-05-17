use super::*;

use crate::{DownloadState, JobId, PostState, RunState};

fn sample_snapshot(
    bytes_downloaded: u64,
    bytes_decoded: u64,
    bytes_committed: u64,
    current_download_speed: u64,
    queue_depth: usize,
    articles_per_sec: f64,
    decode_rate_mbps: f64,
) -> MetricsSnapshot {
    MetricsSnapshot {
        bytes_downloaded,
        bytes_decoded,
        bytes_committed,
        download_queue_depth: queue_depth,
        decode_pending: queue_depth / 2,
        commit_pending: queue_depth / 3,
        write_buffered_bytes: current_download_speed * 2,
        write_buffered_segments: queue_depth + 1,
        direct_write_evictions: 0,
        segments_downloaded: bytes_downloaded / 100,
        segments_decoded: bytes_decoded / 100,
        segments_committed: bytes_committed / 100,
        articles_not_found: 0,
        decode_errors: 0,
        verify_active: 1,
        repair_active: 0,
        extract_active: 0,
        disk_write_latency_us: 200,
        segments_retried: 0,
        segments_failed_permanent: 0,
        current_download_speed,
        crc_errors: 0,
        recovery_queue_depth: 0,
        articles_per_sec,
        decode_rate_mbps,
    }
}

fn job_info(job_id: u64, status: JobStatus) -> JobInfo {
    JobInfo {
        job_id: JobId(job_id),
        job_hash: None,
        name: format!("job-{job_id}"),
        status,
        download_state: DownloadState::Queued,
        post_state: PostState::Idle,
        run_state: RunState::Active,
        progress: 0.0,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        password: None,
        category: None,
        metadata: Vec::new(),
        output_dir: None,
        error: None,
        created_at_epoch_ms: 0.0,
    }
}

#[test]
fn record_metrics_history_sample_round_trips_raw_points() {
    let db = Database::open_in_memory().unwrap();
    db.record_metrics_history_sample(
        100,
        &sample_snapshot(1_000, 700, 500, 120, 4, 5.0, 2.5),
        &[
            job_info(1, JobStatus::Queued),
            job_info(2, JobStatus::Downloading),
        ],
    )
    .unwrap();
    db.record_metrics_history_sample(
        110,
        &sample_snapshot(1_300, 900, 650, 180, 3, 6.0, 3.0),
        &[
            job_info(1, JobStatus::Downloading),
            job_info(2, JobStatus::Paused),
        ],
    )
    .unwrap();

    let rows = db
        .list_metrics_history_chunks(MetricsHistoryTier::Raw10s)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let chunk: RawMetricsHistoryChunk = deserialize_chunk(&rows[0].body_zstd).unwrap();
    assert_eq!(chunk.points.len(), 2);
    assert_eq!(chunk.points[0].timestamp_epoch_sec, 100);
    assert_eq!(chunk.points[1].timestamp_epoch_sec, 110);

    let result = db
        .read_metrics_history(MetricsHistoryTier::Raw10s, 0, 200)
        .unwrap();
    assert_eq!(result.resolution_sec, RAW_METRICS_RESOLUTION_SECS);
    let MetricsHistoryQueryData::Raw(points) = result.data else {
        panic!("expected raw metrics history points");
    };
    assert_eq!(points.len(), 2);
    assert_eq!(points[0].counter_values[0], 1_000.0);
    assert_eq!(points[1].gauge_values[0], 180.0);
    assert_eq!(points[0].job_status_values[0], 1.0);
    assert_eq!(points[1].job_status_values[2], 1.0);
}

#[test]
fn record_metrics_history_sample_builds_avg_and_peak_rollups() {
    let db = Database::open_in_memory().unwrap();
    db.record_metrics_history_sample(
        10,
        &sample_snapshot(0, 0, 0, 100, 3, 4.0, 1.5),
        &[
            job_info(1, JobStatus::Queued),
            job_info(2, JobStatus::Queued),
        ],
    )
    .unwrap();
    db.record_metrics_history_sample(
        20,
        &sample_snapshot(1_000, 800, 600, 220, 6, 8.0, 3.0),
        &[
            job_info(1, JobStatus::Queued),
            job_info(2, JobStatus::Queued),
            job_info(3, JobStatus::Queued),
            job_info(4, JobStatus::Queued),
        ],
    )
    .unwrap();
    db.record_metrics_history_sample(
        30,
        &sample_snapshot(1_900, 1_500, 1_200, 160, 2, 6.0, 2.0),
        &[job_info(1, JobStatus::Queued)],
    )
    .unwrap();

    let result = db
        .read_metrics_history(MetricsHistoryTier::Rollup5m, 0, 300)
        .unwrap();
    let MetricsHistoryQueryData::Rollup(points) = result.data else {
        panic!("expected rollup metrics history points");
    };
    assert_eq!(points.len(), 1);
    let point = &points[0];
    assert_eq!(point.timestamp_epoch_sec, 300);
    assert_eq!(point.counter_values[0].end, 1_900.0);
    assert!((point.counter_values[0].avg_rate - 95.0).abs() < 0.001);
    assert!((point.counter_values[0].peak_rate - 100.0).abs() < 0.001);
    assert!((point.gauge_values[0].avg - 160.0).abs() < 0.001);
    assert_eq!(point.gauge_values[0].peak, 220.0);
    assert!((point.job_status_values[0].avg - (7.0 / 3.0)).abs() < 0.001);
    assert_eq!(point.job_status_values[0].peak, 4.0);

    let hourly = db
        .read_metrics_history(MetricsHistoryTier::Rollup1h, 0, 3_600)
        .unwrap();
    let MetricsHistoryQueryData::Rollup(hourly_points) = hourly.data else {
        panic!("expected hourly rollup metrics history points");
    };
    assert_eq!(hourly_points.len(), 1);
    assert_eq!(hourly_points[0].timestamp_epoch_sec, 3_600);
    assert!((hourly_points[0].counter_values[0].avg_rate - 95.0).abs() < 0.001);
    assert_eq!(hourly_points[0].gauge_values[0].peak, 220.0);
}

#[test]
fn hourly_rollups_weight_sparse_buckets_by_coverage() {
    let db = Database::open_in_memory().unwrap();
    {
        let conn = db.conn();

        let mut sparse_bucket = RollupMetricsHistoryPoint {
            timestamp_epoch_sec: 300,
            counter_values: [CounterRollupValue::default(); NUM_COUNTER_METRICS],
            gauge_values: [GaugeRollupValue::default(); NUM_GAUGE_METRICS],
            job_status_values: [GaugeRollupValue::default(); NUM_JOB_STATUS_METRICS],
        };
        sparse_bucket.counter_values[0] = CounterRollupValue {
            end: 100.0,
            avg_rate: 100.0,
            peak_rate: 100.0,
            avg_rate_weight_sec: 10.0,
        };
        sparse_bucket.gauge_values[0] = GaugeRollupValue {
            avg: 50.0,
            peak: 50.0,
            sample_count: 1,
        };
        sparse_bucket.job_status_values[0] = GaugeRollupValue {
            avg: 4.0,
            peak: 4.0,
            sample_count: 1,
        };
        upsert_rollup_point(&conn, MetricsHistoryTier::Rollup5m, sparse_bucket).unwrap();

        let mut dense_bucket = RollupMetricsHistoryPoint {
            timestamp_epoch_sec: 600,
            counter_values: [CounterRollupValue::default(); NUM_COUNTER_METRICS],
            gauge_values: [GaugeRollupValue::default(); NUM_GAUGE_METRICS],
            job_status_values: [GaugeRollupValue::default(); NUM_JOB_STATUS_METRICS],
        };
        dense_bucket.counter_values[0] = CounterRollupValue {
            end: 390.0,
            avg_rate: 10.0,
            peak_rate: 20.0,
            avg_rate_weight_sec: 290.0,
        };
        dense_bucket.gauge_values[0] = GaugeRollupValue {
            avg: 10.0,
            peak: 12.0,
            sample_count: 29,
        };
        dense_bucket.job_status_values[0] = GaugeRollupValue {
            avg: 1.0,
            peak: 2.0,
            sample_count: 29,
        };
        upsert_rollup_point(&conn, MetricsHistoryTier::Rollup5m, dense_bucket).unwrap();

        refresh_rollup_1h_bucket(&conn, 600).unwrap();
    }

    let MetricsHistoryQueryData::Rollup(hourly_points) = db
        .read_metrics_history(MetricsHistoryTier::Rollup1h, 0, 3_600)
        .unwrap()
        .data
    else {
        panic!("expected hourly rollup points");
    };
    assert_eq!(hourly_points.len(), 1);
    assert!((hourly_points[0].counter_values[0].avg_rate - 13.0).abs() < 0.001);
    assert_eq!(hourly_points[0].counter_values[0].peak_rate, 100.0);
    assert!((hourly_points[0].gauge_values[0].avg - 11.333_333_333).abs() < 0.001);
    assert_eq!(hourly_points[0].gauge_values[0].peak, 50.0);
    assert!((hourly_points[0].job_status_values[0].avg - 1.1).abs() < 0.001);
    assert_eq!(hourly_points[0].job_status_values[0].peak, 4.0);
}

#[test]
fn prune_metrics_history_discards_expired_points_per_tier() {
    let db = Database::open_in_memory().unwrap();
    let now = 10 * 24 * 60 * 60;

    {
        let conn = db.conn();
        upsert_raw_point(
            &conn,
            RawMetricsHistoryPoint::from_snapshot(
                now - RAW_METRICS_RETENTION_SECS - RAW_METRICS_RESOLUTION_SECS,
                &sample_snapshot(100, 50, 25, 10, 1, 1.0, 0.5),
                &[],
            ),
        )
        .unwrap();
        upsert_raw_point(
            &conn,
            RawMetricsHistoryPoint::from_snapshot(
                now,
                &sample_snapshot(200, 100, 50, 20, 1, 1.0, 0.5),
                &[],
            ),
        )
        .unwrap();
        prune_metrics_history(&conn, MetricsHistoryTier::Raw10s, now).unwrap();
    }

    let MetricsHistoryQueryData::Raw(raw_points) = db
        .read_metrics_history(MetricsHistoryTier::Raw10s, 0, now)
        .unwrap()
        .data
    else {
        panic!("expected raw points");
    };
    assert_eq!(raw_points.len(), 1);
    assert_eq!(raw_points[0].timestamp_epoch_sec, now);

    {
        let conn = db.conn();
        upsert_rollup_point(
            &conn,
            MetricsHistoryTier::Rollup5m,
            RollupMetricsHistoryPoint {
                timestamp_epoch_sec: now - ROLLUP_5M_RETENTION_SECS - ROLLUP_5M_RESOLUTION_SECS,
                counter_values: [CounterRollupValue::default(); NUM_COUNTER_METRICS],
                gauge_values: [GaugeRollupValue::default(); NUM_GAUGE_METRICS],
                job_status_values: [GaugeRollupValue::default(); NUM_JOB_STATUS_METRICS],
            },
        )
        .unwrap();
        upsert_rollup_point(
            &conn,
            MetricsHistoryTier::Rollup5m,
            RollupMetricsHistoryPoint {
                timestamp_epoch_sec: now,
                counter_values: [CounterRollupValue::default(); NUM_COUNTER_METRICS],
                gauge_values: [GaugeRollupValue::default(); NUM_GAUGE_METRICS],
                job_status_values: [GaugeRollupValue::default(); NUM_JOB_STATUS_METRICS],
            },
        )
        .unwrap();
        prune_metrics_history(&conn, MetricsHistoryTier::Rollup5m, now).unwrap();
    }

    let MetricsHistoryQueryData::Rollup(five_minute_points) = db
        .read_metrics_history(MetricsHistoryTier::Rollup5m, 0, now)
        .unwrap()
        .data
    else {
        panic!("expected 5m rollup points");
    };
    assert_eq!(five_minute_points.len(), 1);
    assert_eq!(five_minute_points[0].timestamp_epoch_sec, now);

    {
        let conn = db.conn();
        upsert_rollup_point(
            &conn,
            MetricsHistoryTier::Rollup1h,
            RollupMetricsHistoryPoint {
                timestamp_epoch_sec: now - ROLLUP_1H_RETENTION_SECS - ROLLUP_1H_RESOLUTION_SECS,
                counter_values: [CounterRollupValue::default(); NUM_COUNTER_METRICS],
                gauge_values: [GaugeRollupValue::default(); NUM_GAUGE_METRICS],
                job_status_values: [GaugeRollupValue::default(); NUM_JOB_STATUS_METRICS],
            },
        )
        .unwrap();
        upsert_rollup_point(
            &conn,
            MetricsHistoryTier::Rollup1h,
            RollupMetricsHistoryPoint {
                timestamp_epoch_sec: now,
                counter_values: [CounterRollupValue::default(); NUM_COUNTER_METRICS],
                gauge_values: [GaugeRollupValue::default(); NUM_GAUGE_METRICS],
                job_status_values: [GaugeRollupValue::default(); NUM_JOB_STATUS_METRICS],
            },
        )
        .unwrap();
        prune_metrics_history(&conn, MetricsHistoryTier::Rollup1h, now).unwrap();
    }

    let MetricsHistoryQueryData::Rollup(hourly_points) = db
        .read_metrics_history(MetricsHistoryTier::Rollup1h, 0, now)
        .unwrap()
        .data
    else {
        panic!("expected hourly rollup points");
    };
    assert_eq!(hourly_points.len(), 1);
    assert_eq!(hourly_points[0].timestamp_epoch_sec, now);
}
