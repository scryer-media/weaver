use std::fmt::Display;
use std::io::Cursor;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::persistence::Database;
use crate::{JobInfo, JobStatus, MetricsSnapshot, StateError};

const METRICS_HISTORY_COMPRESSION_LEVEL: i32 = 3;
pub(crate) const LEGACY_METRICS_RETENTION_SECS: i64 = 24 * 60 * 60;
pub const RAW_METRICS_RESOLUTION_SECS: i64 = 10;
pub const ROLLUP_5M_RESOLUTION_SECS: i64 = 5 * 60;
pub const ROLLUP_1H_RESOLUTION_SECS: i64 = 60 * 60;
pub const RAW_METRICS_RETENTION_SECS: i64 = 60 * 60;
pub const ROLLUP_5M_RETENTION_SECS: i64 = 24 * 60 * 60;
pub const ROLLUP_1H_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;
const RAW_CHUNK_SPAN_SECS: i64 = 60 * 60;
const ROLLUP_CHUNK_SPAN_SECS: i64 = 24 * 60 * 60;

pub const COUNTER_METRIC_KEYS: [&str; NUM_COUNTER_METRICS] = [
    "weaver_pipeline_bytes_downloaded_total",
    "weaver_pipeline_bytes_decoded_total",
    "weaver_pipeline_bytes_committed_total",
    "weaver_pipeline_segments_downloaded_total",
    "weaver_pipeline_segments_decoded_total",
    "weaver_pipeline_segments_committed_total",
    "weaver_pipeline_segments_retried_total",
    "weaver_pipeline_segments_failed_permanent_total",
    "weaver_pipeline_articles_not_found_total",
    "weaver_pipeline_decode_errors_total",
    "weaver_pipeline_crc_errors_total",
];

pub const GAUGE_METRIC_KEYS: [&str; NUM_GAUGE_METRICS] = [
    "weaver_pipeline_current_download_speed_bytes_per_second",
    "weaver_pipeline_download_queue_depth",
    "weaver_pipeline_decode_pending",
    "weaver_pipeline_commit_pending",
    "weaver_pipeline_recovery_queue_depth",
    "weaver_pipeline_verify_active",
    "weaver_pipeline_repair_active",
    "weaver_pipeline_extract_active",
    "weaver_pipeline_write_buffered_bytes",
    "weaver_pipeline_write_buffered_segments",
    "weaver_pipeline_disk_write_latency_microseconds",
    "weaver_pipeline_articles_per_second",
    "weaver_pipeline_decode_rate_mebibytes_per_second",
];

pub const JOB_STATUS_KEYS: [&str; NUM_JOB_STATUS_METRICS] = [
    "queued",
    "downloading",
    "paused",
    "checking",
    "verifying",
    "queued_repair",
    "repairing",
    "queued_extract",
    "extracting",
    "moving",
    "failed",
    "complete",
];

const NUM_COUNTER_METRICS: usize = 11;
const NUM_GAUGE_METRICS: usize = 13;
const NUM_JOB_STATUS_METRICS: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsHistoryTier {
    Raw10s,
    Rollup5m,
    Rollup1h,
}

impl MetricsHistoryTier {
    pub const fn resolution_sec(self) -> i64 {
        match self {
            Self::Raw10s => RAW_METRICS_RESOLUTION_SECS,
            Self::Rollup5m => ROLLUP_5M_RESOLUTION_SECS,
            Self::Rollup1h => ROLLUP_1H_RESOLUTION_SECS,
        }
    }

    const fn retention_sec(self) -> i64 {
        match self {
            Self::Raw10s => RAW_METRICS_RETENTION_SECS,
            Self::Rollup5m => ROLLUP_5M_RETENTION_SECS,
            Self::Rollup1h => ROLLUP_1H_RETENTION_SECS,
        }
    }

    const fn chunk_span_sec(self) -> i64 {
        match self {
            Self::Raw10s => RAW_CHUNK_SPAN_SECS,
            Self::Rollup5m | Self::Rollup1h => ROLLUP_CHUNK_SPAN_SECS,
        }
    }

    fn from_resolution_sec(resolution_sec: i64) -> Result<Self, StateError> {
        match resolution_sec {
            RAW_METRICS_RESOLUTION_SECS => Ok(Self::Raw10s),
            ROLLUP_5M_RESOLUTION_SECS => Ok(Self::Rollup5m),
            ROLLUP_1H_RESOLUTION_SECS => Ok(Self::Rollup1h),
            other => Err(StateError::Database(format!(
                "unsupported metrics history resolution {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct CounterRollupValue {
    pub end: f64,
    pub avg_rate: f64,
    pub peak_rate: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct GaugeRollupValue {
    pub avg: f64,
    pub peak: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawMetricsHistoryPoint {
    pub timestamp_epoch_sec: i64,
    pub counter_values: [f64; NUM_COUNTER_METRICS],
    pub gauge_values: [f64; NUM_GAUGE_METRICS],
    pub job_status_values: [f64; NUM_JOB_STATUS_METRICS],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollupMetricsHistoryPoint {
    pub timestamp_epoch_sec: i64,
    pub counter_values: [CounterRollupValue; NUM_COUNTER_METRICS],
    pub gauge_values: [GaugeRollupValue; NUM_GAUGE_METRICS],
    pub job_status_values: [GaugeRollupValue; NUM_JOB_STATUS_METRICS],
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct RawMetricsHistoryChunk {
    points: Vec<RawMetricsHistoryPoint>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct RollupMetricsHistoryChunk {
    points: Vec<RollupMetricsHistoryPoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsHistoryChunkRow {
    pub resolution_sec: i64,
    pub chunk_start_epoch_sec: i64,
    pub body_zstd: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricsHistoryQueryData {
    Raw(Vec<RawMetricsHistoryPoint>),
    Rollup(Vec<RollupMetricsHistoryPoint>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricsHistoryQueryResult {
    pub resolution_sec: i64,
    pub data: MetricsHistoryQueryData,
}

fn db_err(error: impl Display) -> StateError {
    StateError::Database(error.to_string())
}

impl RawMetricsHistoryPoint {
    pub fn from_snapshot(
        timestamp_epoch_sec: i64,
        snapshot: &MetricsSnapshot,
        jobs: &[JobInfo],
    ) -> Self {
        Self {
            timestamp_epoch_sec,
            counter_values: [
                snapshot.bytes_downloaded as f64,
                snapshot.bytes_decoded as f64,
                snapshot.bytes_committed as f64,
                snapshot.segments_downloaded as f64,
                snapshot.segments_decoded as f64,
                snapshot.segments_committed as f64,
                snapshot.segments_retried as f64,
                snapshot.segments_failed_permanent as f64,
                snapshot.articles_not_found as f64,
                snapshot.decode_errors as f64,
                snapshot.crc_errors as f64,
            ],
            gauge_values: [
                snapshot.current_download_speed as f64,
                snapshot.download_queue_depth as f64,
                snapshot.decode_pending as f64,
                snapshot.commit_pending as f64,
                snapshot.recovery_queue_depth as f64,
                snapshot.verify_active as f64,
                snapshot.repair_active as f64,
                snapshot.extract_active as f64,
                snapshot.write_buffered_bytes as f64,
                snapshot.write_buffered_segments as f64,
                snapshot.disk_write_latency_us as f64,
                snapshot.articles_per_sec,
                snapshot.decode_rate_mbps,
            ],
            job_status_values: job_status_counts(jobs),
        }
    }
}

impl Database {
    pub fn record_metrics_history_sample(
        &self,
        recorded_at_epoch_sec: i64,
        snapshot: &MetricsSnapshot,
        jobs: &[JobInfo],
    ) -> Result<(), StateError> {
        let recorded_at_epoch_sec = quantize_raw_timestamp(recorded_at_epoch_sec);
        let raw_point = RawMetricsHistoryPoint::from_snapshot(recorded_at_epoch_sec, snapshot, jobs);

        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;

        upsert_raw_point(&tx, raw_point.clone())?;
        refresh_rollup_5m_bucket(&tx, recorded_at_epoch_sec)?;
        refresh_rollup_1h_bucket(&tx, recorded_at_epoch_sec)?;
        prune_metrics_history(&tx, MetricsHistoryTier::Raw10s, recorded_at_epoch_sec)?;
        prune_metrics_history(&tx, MetricsHistoryTier::Rollup5m, recorded_at_epoch_sec)?;
        prune_metrics_history(&tx, MetricsHistoryTier::Rollup1h, recorded_at_epoch_sec)?;

        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn read_metrics_history(
        &self,
        tier: MetricsHistoryTier,
        since_epoch_sec: i64,
        until_epoch_sec: i64,
    ) -> Result<MetricsHistoryQueryResult, StateError> {
        let conn = self.conn();
        match tier {
            MetricsHistoryTier::Raw10s => Ok(MetricsHistoryQueryResult {
                resolution_sec: tier.resolution_sec(),
                data: MetricsHistoryQueryData::Raw(
                    load_raw_points_between(&conn, tier, since_epoch_sec, until_epoch_sec)?,
                ),
            }),
            MetricsHistoryTier::Rollup5m | MetricsHistoryTier::Rollup1h => {
                Ok(MetricsHistoryQueryResult {
                    resolution_sec: tier.resolution_sec(),
                    data: MetricsHistoryQueryData::Rollup(
                        load_rollup_points_between(&conn, tier, since_epoch_sec, until_epoch_sec)?,
                    ),
                })
            }
        }
    }

    pub fn list_metrics_history_chunks(
        &self,
        tier: MetricsHistoryTier,
    ) -> Result<Vec<MetricsHistoryChunkRow>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT resolution_sec, chunk_start_epoch_sec, body_zstd
                 FROM metrics_history_chunks
                 WHERE resolution_sec = ?1
                 ORDER BY chunk_start_epoch_sec ASC",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([tier.resolution_sec()], |row| {
                Ok(MetricsHistoryChunkRow {
                    resolution_sec: row.get(0)?,
                    chunk_start_epoch_sec: row.get(1)?,
                    body_zstd: row.get(2)?,
                })
            })
            .map_err(db_err)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)?);
        }
        Ok(out)
    }

    pub fn delete_all_metrics_history(&self) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute("DELETE FROM metrics_history_chunks", [])
            .map_err(db_err)?;
        Ok(())
    }
}

fn quantize_raw_timestamp(timestamp_epoch_sec: i64) -> i64 {
    align_down_epoch(timestamp_epoch_sec, RAW_METRICS_RESOLUTION_SECS)
}

fn align_down_epoch(value: i64, interval_sec: i64) -> i64 {
    value - value.rem_euclid(interval_sec)
}

fn align_up_epoch(value: i64, interval_sec: i64) -> i64 {
    let remainder = value.rem_euclid(interval_sec);
    if remainder == 0 {
        value
    } else {
        value + (interval_sec - remainder)
    }
}

fn chunk_start_for_timestamp(timestamp_epoch_sec: i64, tier: MetricsHistoryTier) -> i64 {
    align_down_epoch(timestamp_epoch_sec, tier.chunk_span_sec())
}

fn serialize_chunk<T: Serialize>(payload: &T) -> Result<Vec<u8>, StateError> {
    let encoded = rmp_serde::to_vec(payload).map_err(db_err)?;
    zstd::bulk::compress(&encoded, METRICS_HISTORY_COMPRESSION_LEVEL).map_err(db_err)
}

fn deserialize_chunk<T: DeserializeOwned>(blob: &[u8]) -> Result<T, StateError> {
    let decoded = zstd::stream::decode_all(Cursor::new(blob)).map_err(db_err)?;
    rmp_serde::from_slice(&decoded).map_err(db_err)
}

fn load_chunk_blob(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
) -> Result<Option<Vec<u8>>, StateError> {
    conn.query_row(
        "SELECT body_zstd
         FROM metrics_history_chunks
         WHERE resolution_sec = ?1 AND chunk_start_epoch_sec = ?2",
        rusqlite::params![tier.resolution_sec(), chunk_start_epoch_sec],
        |row| row.get(0),
    )
    .optional()
    .map_err(db_err)
}

fn load_raw_chunk(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
) -> Result<RawMetricsHistoryChunk, StateError> {
    debug_assert!(matches!(tier, MetricsHistoryTier::Raw10s));
    match load_chunk_blob(conn, tier, chunk_start_epoch_sec)? {
        Some(blob) => deserialize_chunk(&blob),
        None => Ok(RawMetricsHistoryChunk::default()),
    }
}

fn load_rollup_chunk(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
) -> Result<RollupMetricsHistoryChunk, StateError> {
    debug_assert!(!matches!(tier, MetricsHistoryTier::Raw10s));
    match load_chunk_blob(conn, tier, chunk_start_epoch_sec)? {
        Some(blob) => deserialize_chunk(&blob),
        None => Ok(RollupMetricsHistoryChunk::default()),
    }
}

fn store_chunk<T: Serialize>(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
    payload: &T,
) -> Result<(), StateError> {
    let body_zstd = serialize_chunk(payload)?;
    conn.execute(
        "INSERT OR REPLACE INTO metrics_history_chunks (
             resolution_sec,
             chunk_start_epoch_sec,
             body_zstd
         ) VALUES (?1, ?2, ?3)",
        rusqlite::params![tier.resolution_sec(), chunk_start_epoch_sec, body_zstd],
    )
    .map_err(db_err)?;
    Ok(())
}

fn delete_chunk(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
) -> Result<(), StateError> {
    conn.execute(
        "DELETE FROM metrics_history_chunks
         WHERE resolution_sec = ?1 AND chunk_start_epoch_sec = ?2",
        rusqlite::params![tier.resolution_sec(), chunk_start_epoch_sec],
    )
    .map_err(db_err)?;
    Ok(())
}

fn select_chunk_rows_between(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    since_epoch_sec: i64,
    until_epoch_sec: i64,
) -> Result<Vec<MetricsHistoryChunkRow>, StateError> {
    if until_epoch_sec < since_epoch_sec {
        return Ok(Vec::new());
    }

    let first_chunk_start = chunk_start_for_timestamp(since_epoch_sec, tier);
    let last_chunk_start = chunk_start_for_timestamp(until_epoch_sec, tier);
    let mut stmt = conn
        .prepare(
            "SELECT resolution_sec, chunk_start_epoch_sec, body_zstd
             FROM metrics_history_chunks
             WHERE resolution_sec = ?1
               AND chunk_start_epoch_sec >= ?2
               AND chunk_start_epoch_sec <= ?3
             ORDER BY chunk_start_epoch_sec ASC",
        )
        .map_err(db_err)?;
    let rows = stmt
        .query_map(
            rusqlite::params![tier.resolution_sec(), first_chunk_start, last_chunk_start],
            |row| {
                Ok(MetricsHistoryChunkRow {
                    resolution_sec: row.get(0)?,
                    chunk_start_epoch_sec: row.get(1)?,
                    body_zstd: row.get(2)?,
                })
            },
        )
        .map_err(db_err)?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(db_err)?);
    }
    Ok(out)
}

fn upsert_raw_point(
    conn: &rusqlite::Connection,
    point: RawMetricsHistoryPoint,
) -> Result<(), StateError> {
    let tier = MetricsHistoryTier::Raw10s;
    let chunk_start_epoch_sec = chunk_start_for_timestamp(point.timestamp_epoch_sec, tier);
    let mut chunk = load_raw_chunk(conn, tier, chunk_start_epoch_sec)?;
    upsert_sorted_point(
        &mut chunk.points,
        point,
        |value| value.timestamp_epoch_sec,
        replace_raw_point,
    );
    store_chunk(conn, tier, chunk_start_epoch_sec, &chunk)
}

fn upsert_rollup_point(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    point: RollupMetricsHistoryPoint,
) -> Result<(), StateError> {
    let chunk_start_epoch_sec = chunk_start_for_timestamp(point.timestamp_epoch_sec, tier);
    let mut chunk = load_rollup_chunk(conn, tier, chunk_start_epoch_sec)?;
    upsert_sorted_point(
        &mut chunk.points,
        point,
        |value| value.timestamp_epoch_sec,
        replace_rollup_point,
    );
    store_chunk(conn, tier, chunk_start_epoch_sec, &chunk)
}

fn upsert_sorted_point<T, F, R>(
    points: &mut Vec<T>,
    point: T,
    timestamp: F,
    replace: R,
) where
    F: Fn(&T) -> i64,
    R: Fn(&mut T, T),
{
    match points.binary_search_by_key(&timestamp(&point), |entry| timestamp(entry)) {
        Ok(index) => replace(&mut points[index], point),
        Err(index) => points.insert(index, point),
    }
}

fn replace_raw_point(current: &mut RawMetricsHistoryPoint, next: RawMetricsHistoryPoint) {
    *current = next;
}

fn replace_rollup_point(current: &mut RollupMetricsHistoryPoint, next: RollupMetricsHistoryPoint) {
    *current = next;
}

fn refresh_rollup_5m_bucket(
    conn: &rusqlite::Connection,
    recorded_at_epoch_sec: i64,
) -> Result<(), StateError> {
    let bucket_end_epoch_sec = align_up_epoch(recorded_at_epoch_sec, ROLLUP_5M_RESOLUTION_SECS);
    let bucket_start_epoch_sec = bucket_end_epoch_sec - ROLLUP_5M_RESOLUTION_SECS;
    let raw_points = load_raw_points_between(
        conn,
        MetricsHistoryTier::Raw10s,
        bucket_start_epoch_sec + 1,
        bucket_end_epoch_sec,
    )?;
    if raw_points.is_empty() {
        return Ok(());
    }

    let point = RollupMetricsHistoryPoint {
        timestamp_epoch_sec: bucket_end_epoch_sec,
        counter_values: aggregate_counter_rollups(&raw_points),
        gauge_values: aggregate_gauge_rollups(&raw_points),
        job_status_values: aggregate_job_status_rollups(&raw_points),
    };
    upsert_rollup_point(conn, MetricsHistoryTier::Rollup5m, point)
}

fn refresh_rollup_1h_bucket(
    conn: &rusqlite::Connection,
    recorded_at_epoch_sec: i64,
) -> Result<(), StateError> {
    let bucket_end_epoch_sec = align_up_epoch(recorded_at_epoch_sec, ROLLUP_1H_RESOLUTION_SECS);
    let bucket_start_epoch_sec = bucket_end_epoch_sec - ROLLUP_1H_RESOLUTION_SECS;
    let rollup_points = load_rollup_points_between(
        conn,
        MetricsHistoryTier::Rollup5m,
        bucket_start_epoch_sec + 1,
        bucket_end_epoch_sec,
    )?;
    if rollup_points.is_empty() {
        return Ok(());
    }

    let point = RollupMetricsHistoryPoint {
        timestamp_epoch_sec: bucket_end_epoch_sec,
        counter_values: aggregate_counter_rollups_from_rollups(&rollup_points),
        gauge_values: aggregate_gauge_rollups_from_rollups(&rollup_points),
        job_status_values: aggregate_job_rollups_from_rollups(&rollup_points),
    };
    upsert_rollup_point(conn, MetricsHistoryTier::Rollup1h, point)
}

fn prune_metrics_history(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    now_epoch_sec: i64,
) -> Result<(), StateError> {
    let cutoff_epoch_sec = now_epoch_sec - tier.retention_sec();
    let boundary_chunk_start = chunk_start_for_timestamp(cutoff_epoch_sec, tier);

    conn.execute(
        "DELETE FROM metrics_history_chunks
         WHERE resolution_sec = ?1
           AND chunk_start_epoch_sec < ?2",
        rusqlite::params![tier.resolution_sec(), boundary_chunk_start],
    )
    .map_err(db_err)?;

    match tier {
        MetricsHistoryTier::Raw10s => {
            let mut chunk = load_raw_chunk(conn, tier, boundary_chunk_start)?;
            chunk.points
                .retain(|point| point.timestamp_epoch_sec >= cutoff_epoch_sec);
            if chunk.points.is_empty() {
                delete_chunk(conn, tier, boundary_chunk_start)?;
            } else {
                store_chunk(conn, tier, boundary_chunk_start, &chunk)?;
            }
        }
        MetricsHistoryTier::Rollup5m | MetricsHistoryTier::Rollup1h => {
            let mut chunk = load_rollup_chunk(conn, tier, boundary_chunk_start)?;
            chunk.points
                .retain(|point| point.timestamp_epoch_sec >= cutoff_epoch_sec);
            if chunk.points.is_empty() {
                delete_chunk(conn, tier, boundary_chunk_start)?;
            } else {
                store_chunk(conn, tier, boundary_chunk_start, &chunk)?;
            }
        }
    }

    Ok(())
}

fn load_raw_points_between(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    since_epoch_sec: i64,
    until_epoch_sec: i64,
) -> Result<Vec<RawMetricsHistoryPoint>, StateError> {
    let rows = select_chunk_rows_between(conn, tier, since_epoch_sec, until_epoch_sec)?;
    let mut points = Vec::new();
    for row in rows {
        let chunk: RawMetricsHistoryChunk = deserialize_chunk(&row.body_zstd)?;
        points.extend(
            chunk
                .points
                .into_iter()
                .filter(|point| {
                    point.timestamp_epoch_sec >= since_epoch_sec
                        && point.timestamp_epoch_sec <= until_epoch_sec
                }),
        );
    }
    points.sort_by_key(|point| point.timestamp_epoch_sec);
    Ok(points)
}

fn load_rollup_points_between(
    conn: &rusqlite::Connection,
    tier: MetricsHistoryTier,
    since_epoch_sec: i64,
    until_epoch_sec: i64,
) -> Result<Vec<RollupMetricsHistoryPoint>, StateError> {
    let rows = select_chunk_rows_between(conn, tier, since_epoch_sec, until_epoch_sec)?;
    let mut points = Vec::new();
    for row in rows {
        let chunk: RollupMetricsHistoryChunk = deserialize_chunk(&row.body_zstd)?;
        points.extend(
            chunk
                .points
                .into_iter()
                .filter(|point| {
                    point.timestamp_epoch_sec >= since_epoch_sec
                        && point.timestamp_epoch_sec <= until_epoch_sec
                }),
        );
    }
    points.sort_by_key(|point| point.timestamp_epoch_sec);
    Ok(points)
}

fn aggregate_counter_rollups(
    points: &[RawMetricsHistoryPoint],
) -> [CounterRollupValue; NUM_COUNTER_METRICS] {
    std::array::from_fn(|index| aggregate_single_counter_rollup(points, index))
}

fn aggregate_single_counter_rollup(
    points: &[RawMetricsHistoryPoint],
    index: usize,
) -> CounterRollupValue {
    let end = points
        .last()
        .map(|point| point.counter_values[index])
        .unwrap_or_default();
    let avg_rate = if points.len() >= 2 {
        let first = &points[0];
        let last = &points[points.len() - 1];
        let elapsed = (last.timestamp_epoch_sec - first.timestamp_epoch_sec) as f64;
        if elapsed > 0.0 {
            ((last.counter_values[index] - first.counter_values[index]).max(0.0)) / elapsed
        } else {
            0.0
        }
    } else {
        0.0
    };

    let mut peak_rate = 0.0;
    for window in points.windows(2) {
        let previous = &window[0];
        let current = &window[1];
        let elapsed = (current.timestamp_epoch_sec - previous.timestamp_epoch_sec) as f64;
        if elapsed <= 0.0 {
            continue;
        }
        let delta = (current.counter_values[index] - previous.counter_values[index]).max(0.0);
        peak_rate = peak_rate.max(delta / elapsed);
    }

    CounterRollupValue {
        end,
        avg_rate,
        peak_rate,
    }
}

fn aggregate_gauge_rollups(points: &[RawMetricsHistoryPoint]) -> [GaugeRollupValue; NUM_GAUGE_METRICS] {
    std::array::from_fn(|index| {
        let values = points.iter().map(|point| point.gauge_values[index]);
        aggregate_gauge_rollup_values(values)
    })
}

fn aggregate_job_status_rollups(
    points: &[RawMetricsHistoryPoint],
) -> [GaugeRollupValue; NUM_JOB_STATUS_METRICS] {
    std::array::from_fn(|index| {
        let values = points.iter().map(|point| point.job_status_values[index]);
        aggregate_gauge_rollup_values(values)
    })
}

fn aggregate_counter_rollups_from_rollups(
    points: &[RollupMetricsHistoryPoint],
) -> [CounterRollupValue; NUM_COUNTER_METRICS] {
    std::array::from_fn(|index| CounterRollupValue {
        end: points
            .last()
            .map(|point| point.counter_values[index].end)
            .unwrap_or_default(),
        avg_rate: mean(points.iter().map(|point| point.counter_values[index].avg_rate)),
        peak_rate: points
            .iter()
            .map(|point| point.counter_values[index].peak_rate)
            .fold(0.0, f64::max),
    })
}

fn aggregate_gauge_rollups_from_rollups(
    points: &[RollupMetricsHistoryPoint],
) -> [GaugeRollupValue; NUM_GAUGE_METRICS] {
    std::array::from_fn(|index| GaugeRollupValue {
        avg: mean(points.iter().map(|point| point.gauge_values[index].avg)),
        peak: points
            .iter()
            .map(|point| point.gauge_values[index].peak)
            .fold(0.0, f64::max),
    })
}

fn aggregate_job_rollups_from_rollups(
    points: &[RollupMetricsHistoryPoint],
) -> [GaugeRollupValue; NUM_JOB_STATUS_METRICS] {
    std::array::from_fn(|index| GaugeRollupValue {
        avg: mean(points.iter().map(|point| point.job_status_values[index].avg)),
        peak: points
            .iter()
            .map(|point| point.job_status_values[index].peak)
            .fold(0.0, f64::max),
    })
}

fn aggregate_gauge_rollup_values(values: impl Iterator<Item = f64>) -> GaugeRollupValue {
    let mut total = 0.0;
    let mut count = 0_u64;
    let mut peak = 0.0;
    for value in values {
        total += value;
        count += 1;
        peak = peak.max(value);
    }

    GaugeRollupValue {
        avg: if count == 0 { 0.0 } else { total / count as f64 },
        peak,
    }
}

fn mean(values: impl Iterator<Item = f64>) -> f64 {
    let mut total = 0.0;
    let mut count = 0_u64;
    for value in values {
        total += value;
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        total / count as f64
    }
}

fn job_status_counts(jobs: &[JobInfo]) -> [f64; NUM_JOB_STATUS_METRICS] {
    let mut counts = [0.0; NUM_JOB_STATUS_METRICS];
    for job in jobs {
        if let Some(index) = job_status_index(&job.status) {
            counts[index] += 1.0;
        }
    }
    counts
}

fn job_status_index(status: &JobStatus) -> Option<usize> {
    let label = match status {
        JobStatus::Queued => "queued",
        JobStatus::Downloading => "downloading",
        JobStatus::Paused => "paused",
        JobStatus::Checking => "checking",
        JobStatus::Verifying => "verifying",
        JobStatus::QueuedRepair => "queued_repair",
        JobStatus::Repairing => "repairing",
        JobStatus::QueuedExtract => "queued_extract",
        JobStatus::Extracting => "extracting",
        JobStatus::Moving => "moving",
        JobStatus::Failed { .. } => "failed",
        JobStatus::Complete => "complete",
    };
    JOB_STATUS_KEYS.iter().position(|candidate| *candidate == label)
}

#[cfg(test)]
mod tests;
