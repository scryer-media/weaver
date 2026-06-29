use std::fmt::Display;
use std::io::Cursor;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx, StoreDatastore};
use crate::{JobInfo, JobStatus, MetricsSnapshot, StateError};

const METRICS_HISTORY_COMPRESSION_LEVEL: i32 = 3;
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
    "weaver_pipeline_download_failures_article_not_found_total",
    "weaver_pipeline_download_failures_capacity_unavailable_total",
    "weaver_pipeline_download_failures_transient_total",
    "weaver_pipeline_download_failures_auth_total",
    "weaver_pipeline_download_failures_permanent_total",
    "weaver_pipeline_articles_not_found_total",
    "weaver_pipeline_decode_errors_total",
    "weaver_pipeline_crc_errors_total",
    "weaver_pipeline_download_pressure_stalls_total",
    "weaver_pipeline_download_pressure_stall_duration_seconds",
];

pub const GAUGE_METRIC_KEYS: [&str; NUM_GAUGE_METRICS] = [
    "weaver_pipeline_current_download_speed_bytes_per_second",
    "weaver_pipeline_download_queue_depth",
    "weaver_pipeline_active_downloads",
    "weaver_pipeline_active_decodes",
    "weaver_pipeline_decode_pending",
    "weaver_pipeline_decode_pending_bytes",
    "weaver_pipeline_decode_active_bytes",
    "weaver_pipeline_commit_pending",
    "weaver_pipeline_recovery_queue_depth",
    "weaver_pipeline_verify_active",
    "weaver_pipeline_repair_active",
    "weaver_pipeline_extract_active",
    "weaver_pipeline_write_buffered_bytes",
    "weaver_pipeline_write_buffered_segments",
    "weaver_pipeline_decode_pressure_soft_limit_bytes",
    "weaver_pipeline_decode_pressure_hard_limit_bytes",
    "weaver_pipeline_write_pressure_soft_limit_bytes",
    "weaver_pipeline_write_pressure_hard_limit_bytes",
    "weaver_pipeline_download_pressure_state",
    "weaver_pipeline_download_pressure_reason",
    "weaver_pipeline_download_pressure_current_stall_seconds",
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

const NUM_COUNTER_METRICS: usize = 18;
const NUM_GAUGE_METRICS: usize = 24;
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
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct CounterRollupValue {
    pub end: f64,
    pub avg_rate: f64,
    pub peak_rate: f64,
    #[serde(default)]
    pub avg_rate_weight_sec: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct GaugeRollupValue {
    pub avg: f64,
    pub peak: f64,
    #[serde(default)]
    pub sample_count: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawMetricsHistoryPoint {
    pub timestamp_epoch_sec: i64,
    #[serde(
        serialize_with = "serialize_metric_array",
        deserialize_with = "deserialize_padded_metric_array"
    )]
    pub counter_values: [f64; NUM_COUNTER_METRICS],
    #[serde(
        serialize_with = "serialize_metric_array",
        deserialize_with = "deserialize_padded_metric_array"
    )]
    pub gauge_values: [f64; NUM_GAUGE_METRICS],
    #[serde(
        serialize_with = "serialize_metric_array",
        deserialize_with = "deserialize_padded_metric_array"
    )]
    pub job_status_values: [f64; NUM_JOB_STATUS_METRICS],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollupMetricsHistoryPoint {
    pub timestamp_epoch_sec: i64,
    #[serde(
        serialize_with = "serialize_metric_array",
        deserialize_with = "deserialize_padded_metric_array"
    )]
    pub counter_values: [CounterRollupValue; NUM_COUNTER_METRICS],
    #[serde(
        serialize_with = "serialize_metric_array",
        deserialize_with = "deserialize_padded_metric_array"
    )]
    pub gauge_values: [GaugeRollupValue; NUM_GAUGE_METRICS],
    #[serde(
        serialize_with = "serialize_metric_array",
        deserialize_with = "deserialize_padded_metric_array"
    )]
    pub job_status_values: [GaugeRollupValue; NUM_JOB_STATUS_METRICS],
}

fn serialize_metric_array<S, T, const N: usize>(
    values: &[T; N],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    values.as_slice().serialize(serializer)
}

fn deserialize_padded_metric_array<'de, D, T, const N: usize>(
    deserializer: D,
) -> Result<[T; N], D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default + Copy,
{
    let values = Vec::<T>::deserialize(deserializer)?;
    let mut padded = [T::default(); N];
    for (slot, value) in padded.iter_mut().zip(values) {
        *slot = value;
    }
    Ok(padded)
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
                snapshot.download_failures_article_not_found as f64,
                snapshot.download_failures_capacity_unavailable as f64,
                snapshot.download_failures_transient as f64,
                snapshot.download_failures_auth as f64,
                snapshot.download_failures_permanent as f64,
                snapshot.articles_not_found as f64,
                snapshot.decode_errors as f64,
                snapshot.crc_errors as f64,
                snapshot.download_pressure_stalls_total as f64,
                snapshot.download_pressure_stall_duration_ms as f64 / 1000.0,
            ],
            gauge_values: [
                snapshot.current_download_speed as f64,
                snapshot.download_queue_depth as f64,
                snapshot.active_downloads as f64,
                snapshot.active_decodes as f64,
                snapshot.decode_pending as f64,
                snapshot.decode_pending_bytes as f64,
                snapshot.decode_active_bytes as f64,
                snapshot.commit_pending as f64,
                snapshot.recovery_queue_depth as f64,
                snapshot.verify_active as f64,
                snapshot.repair_active as f64,
                snapshot.extract_active as f64,
                snapshot.write_buffered_bytes as f64,
                snapshot.write_buffered_segments as f64,
                snapshot.decode_pressure_soft_limit_bytes as f64,
                snapshot.decode_pressure_hard_limit_bytes as f64,
                snapshot.write_pressure_soft_limit_bytes as f64,
                snapshot.write_pressure_hard_limit_bytes as f64,
                snapshot.download_pressure_state.as_code() as f64,
                snapshot.download_pressure_reason.as_code() as f64,
                snapshot.download_pressure_current_stall_ms as f64 / 1000.0,
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
        let raw_point =
            RawMetricsHistoryPoint::from_snapshot(recorded_at_epoch_sec, snapshot, jobs);

        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "record_metrics_history_sample", |tx| {
                let raw_point = raw_point.clone();
                Box::pin(async move {
                    upsert_raw_point_tx(tx, raw_point).await?;
                    refresh_rollup_5m_bucket_tx(tx, recorded_at_epoch_sec).await?;
                    refresh_rollup_1h_bucket_tx(tx, recorded_at_epoch_sec).await?;
                    prune_metrics_history_tx(tx, MetricsHistoryTier::Raw10s, recorded_at_epoch_sec)
                        .await?;
                    prune_metrics_history_tx(
                        tx,
                        MetricsHistoryTier::Rollup5m,
                        recorded_at_epoch_sec,
                    )
                    .await?;
                    prune_metrics_history_tx(
                        tx,
                        MetricsHistoryTier::Rollup1h,
                        recorded_at_epoch_sec,
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn read_metrics_history(
        &self,
        tier: MetricsHistoryTier,
        since_epoch_sec: i64,
        until_epoch_sec: i64,
    ) -> Result<MetricsHistoryQueryResult, StateError> {
        let datastore = self.datastore();
        match tier {
            MetricsHistoryTier::Raw10s => self.run_sql_blocking(async move {
                Ok(MetricsHistoryQueryResult {
                    resolution_sec: tier.resolution_sec(),
                    data: MetricsHistoryQueryData::Raw(
                        load_raw_points_between(&datastore, tier, since_epoch_sec, until_epoch_sec)
                            .await?,
                    ),
                })
            }),
            MetricsHistoryTier::Rollup5m | MetricsHistoryTier::Rollup1h => {
                self.run_sql_blocking(async move {
                    Ok(MetricsHistoryQueryResult {
                        resolution_sec: tier.resolution_sec(),
                        data: MetricsHistoryQueryData::Rollup(
                            load_rollup_points_between(
                                &datastore,
                                tier,
                                since_epoch_sec,
                                until_epoch_sec,
                            )
                            .await?,
                        ),
                    })
                })
            }
        }
    }

    pub fn list_metrics_history_chunks(
        &self,
        tier: MetricsHistoryTier,
    ) -> Result<Vec<MetricsHistoryChunkRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT resolution_sec, chunk_start_epoch_sec, body_zstd
                 FROM metrics_history_chunks
                 WHERE resolution_sec = {}
                 ORDER BY chunk_start_epoch_sec ASC",
                &[SqlArg::I64(tier.resolution_sec())],
            )
            .await?;
            rows.into_iter().map(metrics_chunk_row_from_row).collect()
        })
    }

    pub fn delete_all_metrics_history(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM metrics_history_chunks",
                &[],
            )
            .await?;
            Ok(())
        })
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

async fn load_chunk_blob_tx(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
) -> Result<Option<Vec<u8>>, StateError> {
    tx.fetch_optional(
        "SELECT body_zstd
         FROM metrics_history_chunks
         WHERE resolution_sec = {} AND chunk_start_epoch_sec = {}",
        &[
            SqlArg::I64(tier.resolution_sec()),
            SqlArg::I64(chunk_start_epoch_sec),
        ],
    )
    .await?
    .map(|row| row.bytes("body_zstd"))
    .transpose()
}

async fn load_raw_chunk_tx(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
) -> Result<RawMetricsHistoryChunk, StateError> {
    debug_assert!(matches!(tier, MetricsHistoryTier::Raw10s));
    match load_chunk_blob_tx(tx, tier, chunk_start_epoch_sec).await? {
        Some(blob) => deserialize_chunk(&blob),
        None => Ok(RawMetricsHistoryChunk::default()),
    }
}

async fn load_rollup_chunk_tx(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
) -> Result<RollupMetricsHistoryChunk, StateError> {
    debug_assert!(!matches!(tier, MetricsHistoryTier::Raw10s));
    match load_chunk_blob_tx(tx, tier, chunk_start_epoch_sec).await? {
        Some(blob) => deserialize_chunk(&blob),
        None => Ok(RollupMetricsHistoryChunk::default()),
    }
}

async fn store_chunk_tx<T: Serialize>(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
    payload: &T,
) -> Result<(), StateError> {
    let body_zstd = serialize_chunk(payload)?;
    tx.execute(
        "INSERT INTO metrics_history_chunks (
             resolution_sec,
             chunk_start_epoch_sec,
             body_zstd
         ) VALUES ({}, {}, {})
         ON CONFLICT(resolution_sec, chunk_start_epoch_sec) DO UPDATE SET
            body_zstd = excluded.body_zstd",
        &[
            SqlArg::I64(tier.resolution_sec()),
            SqlArg::I64(chunk_start_epoch_sec),
            SqlArg::Bytes(body_zstd),
        ],
    )
    .await?;
    Ok(())
}

async fn delete_chunk_tx(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    chunk_start_epoch_sec: i64,
) -> Result<(), StateError> {
    tx.execute(
        "DELETE FROM metrics_history_chunks
         WHERE resolution_sec = {} AND chunk_start_epoch_sec = {}",
        &[
            SqlArg::I64(tier.resolution_sec()),
            SqlArg::I64(chunk_start_epoch_sec),
        ],
    )
    .await?;
    Ok(())
}

async fn select_chunk_rows_between(
    datastore: &StoreDatastore,
    tier: MetricsHistoryTier,
    since_epoch_sec: i64,
    until_epoch_sec: i64,
) -> Result<Vec<MetricsHistoryChunkRow>, StateError> {
    if until_epoch_sec < since_epoch_sec {
        return Ok(Vec::new());
    }

    let first_chunk_start = chunk_start_for_timestamp(since_epoch_sec, tier);
    let last_chunk_start = chunk_start_for_timestamp(until_epoch_sec, tier);
    let rows = SqlRuntime::fetch_all(
        datastore.read_exec(),
        "SELECT resolution_sec, chunk_start_epoch_sec, body_zstd
         FROM metrics_history_chunks
         WHERE resolution_sec = {}
           AND chunk_start_epoch_sec >= {}
           AND chunk_start_epoch_sec <= {}
         ORDER BY chunk_start_epoch_sec ASC",
        &[
            SqlArg::I64(tier.resolution_sec()),
            SqlArg::I64(first_chunk_start),
            SqlArg::I64(last_chunk_start),
        ],
    )
    .await?;
    rows.into_iter().map(metrics_chunk_row_from_row).collect()
}

async fn select_chunk_rows_between_tx(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    since_epoch_sec: i64,
    until_epoch_sec: i64,
) -> Result<Vec<MetricsHistoryChunkRow>, StateError> {
    if until_epoch_sec < since_epoch_sec {
        return Ok(Vec::new());
    }

    let first_chunk_start = chunk_start_for_timestamp(since_epoch_sec, tier);
    let last_chunk_start = chunk_start_for_timestamp(until_epoch_sec, tier);
    let rows = tx
        .fetch_all(
            "SELECT resolution_sec, chunk_start_epoch_sec, body_zstd
             FROM metrics_history_chunks
             WHERE resolution_sec = {}
               AND chunk_start_epoch_sec >= {}
               AND chunk_start_epoch_sec <= {}
             ORDER BY chunk_start_epoch_sec ASC",
            &[
                SqlArg::I64(tier.resolution_sec()),
                SqlArg::I64(first_chunk_start),
                SqlArg::I64(last_chunk_start),
            ],
        )
        .await?;
    rows.into_iter().map(metrics_chunk_row_from_row).collect()
}

async fn upsert_raw_point_tx(
    tx: &mut SqlTx<'_>,
    point: RawMetricsHistoryPoint,
) -> Result<(), StateError> {
    let tier = MetricsHistoryTier::Raw10s;
    let chunk_start_epoch_sec = chunk_start_for_timestamp(point.timestamp_epoch_sec, tier);
    let mut chunk = load_raw_chunk_tx(tx, tier, chunk_start_epoch_sec).await?;
    upsert_sorted_point(
        &mut chunk.points,
        point,
        |value| value.timestamp_epoch_sec,
        replace_raw_point,
    );
    store_chunk_tx(tx, tier, chunk_start_epoch_sec, &chunk).await
}

async fn upsert_rollup_point_tx(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    point: RollupMetricsHistoryPoint,
) -> Result<(), StateError> {
    let chunk_start_epoch_sec = chunk_start_for_timestamp(point.timestamp_epoch_sec, tier);
    let mut chunk = load_rollup_chunk_tx(tx, tier, chunk_start_epoch_sec).await?;
    upsert_sorted_point(
        &mut chunk.points,
        point,
        |value| value.timestamp_epoch_sec,
        replace_rollup_point,
    );
    store_chunk_tx(tx, tier, chunk_start_epoch_sec, &chunk).await
}

fn upsert_sorted_point<T, F, R>(points: &mut Vec<T>, point: T, timestamp: F, replace: R)
where
    F: Fn(&T) -> i64,
    R: Fn(&mut T, T),
{
    match points.binary_search_by_key(&timestamp(&point), timestamp) {
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

async fn refresh_rollup_5m_bucket_tx(
    tx: &mut SqlTx<'_>,
    recorded_at_epoch_sec: i64,
) -> Result<(), StateError> {
    let bucket_end_epoch_sec = align_up_epoch(recorded_at_epoch_sec, ROLLUP_5M_RESOLUTION_SECS);
    let bucket_start_epoch_sec = bucket_end_epoch_sec - ROLLUP_5M_RESOLUTION_SECS;
    let raw_points = load_raw_points_between_tx(
        tx,
        MetricsHistoryTier::Raw10s,
        bucket_start_epoch_sec + 1,
        bucket_end_epoch_sec,
    )
    .await?;
    if raw_points.is_empty() {
        return Ok(());
    }

    let point = RollupMetricsHistoryPoint {
        timestamp_epoch_sec: bucket_end_epoch_sec,
        counter_values: aggregate_counter_rollups(&raw_points),
        gauge_values: aggregate_gauge_rollups(&raw_points),
        job_status_values: aggregate_job_status_rollups(&raw_points),
    };
    upsert_rollup_point_tx(tx, MetricsHistoryTier::Rollup5m, point).await
}

async fn refresh_rollup_1h_bucket_tx(
    tx: &mut SqlTx<'_>,
    recorded_at_epoch_sec: i64,
) -> Result<(), StateError> {
    let bucket_end_epoch_sec = align_up_epoch(recorded_at_epoch_sec, ROLLUP_1H_RESOLUTION_SECS);
    let bucket_start_epoch_sec = bucket_end_epoch_sec - ROLLUP_1H_RESOLUTION_SECS;
    let rollup_points = load_rollup_points_between_tx(
        tx,
        MetricsHistoryTier::Rollup5m,
        bucket_start_epoch_sec + 1,
        bucket_end_epoch_sec,
    )
    .await?;
    if rollup_points.is_empty() {
        return Ok(());
    }

    let point = RollupMetricsHistoryPoint {
        timestamp_epoch_sec: bucket_end_epoch_sec,
        counter_values: aggregate_counter_rollups_from_rollups(&rollup_points),
        gauge_values: aggregate_gauge_rollups_from_rollups(&rollup_points),
        job_status_values: aggregate_job_rollups_from_rollups(&rollup_points),
    };
    upsert_rollup_point_tx(tx, MetricsHistoryTier::Rollup1h, point).await
}

async fn prune_metrics_history_tx(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    now_epoch_sec: i64,
) -> Result<(), StateError> {
    let cutoff_epoch_sec = now_epoch_sec - tier.retention_sec();
    let boundary_chunk_start = chunk_start_for_timestamp(cutoff_epoch_sec, tier);

    tx.execute(
        "DELETE FROM metrics_history_chunks
         WHERE resolution_sec = {}
           AND chunk_start_epoch_sec < {}",
        &[
            SqlArg::I64(tier.resolution_sec()),
            SqlArg::I64(boundary_chunk_start),
        ],
    )
    .await?;

    match tier {
        MetricsHistoryTier::Raw10s => {
            let mut chunk = load_raw_chunk_tx(tx, tier, boundary_chunk_start).await?;
            chunk
                .points
                .retain(|point| point.timestamp_epoch_sec >= cutoff_epoch_sec);
            if chunk.points.is_empty() {
                delete_chunk_tx(tx, tier, boundary_chunk_start).await?;
            } else {
                store_chunk_tx(tx, tier, boundary_chunk_start, &chunk).await?;
            }
        }
        MetricsHistoryTier::Rollup5m | MetricsHistoryTier::Rollup1h => {
            let mut chunk = load_rollup_chunk_tx(tx, tier, boundary_chunk_start).await?;
            chunk
                .points
                .retain(|point| point.timestamp_epoch_sec >= cutoff_epoch_sec);
            if chunk.points.is_empty() {
                delete_chunk_tx(tx, tier, boundary_chunk_start).await?;
            } else {
                store_chunk_tx(tx, tier, boundary_chunk_start, &chunk).await?;
            }
        }
    }

    Ok(())
}

async fn load_raw_points_between(
    datastore: &StoreDatastore,
    tier: MetricsHistoryTier,
    since_epoch_sec: i64,
    until_epoch_sec: i64,
) -> Result<Vec<RawMetricsHistoryPoint>, StateError> {
    let rows = select_chunk_rows_between(datastore, tier, since_epoch_sec, until_epoch_sec).await?;
    let mut points = Vec::new();
    for row in rows {
        let chunk: RawMetricsHistoryChunk = deserialize_chunk(&row.body_zstd)?;
        points.extend(chunk.points.into_iter().filter(|point| {
            point.timestamp_epoch_sec >= since_epoch_sec
                && point.timestamp_epoch_sec <= until_epoch_sec
        }));
    }
    points.sort_by_key(|point| point.timestamp_epoch_sec);
    Ok(points)
}

async fn load_raw_points_between_tx(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    since_epoch_sec: i64,
    until_epoch_sec: i64,
) -> Result<Vec<RawMetricsHistoryPoint>, StateError> {
    let rows = select_chunk_rows_between_tx(tx, tier, since_epoch_sec, until_epoch_sec).await?;
    let mut points = Vec::new();
    for row in rows {
        let chunk: RawMetricsHistoryChunk = deserialize_chunk(&row.body_zstd)?;
        points.extend(chunk.points.into_iter().filter(|point| {
            point.timestamp_epoch_sec >= since_epoch_sec
                && point.timestamp_epoch_sec <= until_epoch_sec
        }));
    }
    points.sort_by_key(|point| point.timestamp_epoch_sec);
    Ok(points)
}

async fn load_rollup_points_between(
    datastore: &StoreDatastore,
    tier: MetricsHistoryTier,
    since_epoch_sec: i64,
    until_epoch_sec: i64,
) -> Result<Vec<RollupMetricsHistoryPoint>, StateError> {
    let rows = select_chunk_rows_between(datastore, tier, since_epoch_sec, until_epoch_sec).await?;
    let mut points = Vec::new();
    for row in rows {
        let chunk: RollupMetricsHistoryChunk = deserialize_chunk(&row.body_zstd)?;
        points.extend(chunk.points.into_iter().filter(|point| {
            point.timestamp_epoch_sec >= since_epoch_sec
                && point.timestamp_epoch_sec <= until_epoch_sec
        }));
    }
    points.sort_by_key(|point| point.timestamp_epoch_sec);
    Ok(points)
}

async fn load_rollup_points_between_tx(
    tx: &mut SqlTx<'_>,
    tier: MetricsHistoryTier,
    since_epoch_sec: i64,
    until_epoch_sec: i64,
) -> Result<Vec<RollupMetricsHistoryPoint>, StateError> {
    let rows = select_chunk_rows_between_tx(tx, tier, since_epoch_sec, until_epoch_sec).await?;
    let mut points = Vec::new();
    for row in rows {
        let chunk: RollupMetricsHistoryChunk = deserialize_chunk(&row.body_zstd)?;
        points.extend(chunk.points.into_iter().filter(|point| {
            point.timestamp_epoch_sec >= since_epoch_sec
                && point.timestamp_epoch_sec <= until_epoch_sec
        }));
    }
    points.sort_by_key(|point| point.timestamp_epoch_sec);
    Ok(points)
}

fn metrics_chunk_row_from_row(row: SqlRow) -> Result<MetricsHistoryChunkRow, StateError> {
    Ok(MetricsHistoryChunkRow {
        resolution_sec: row.i64("resolution_sec")?,
        chunk_start_epoch_sec: row.i64("chunk_start_epoch_sec")?,
        body_zstd: row.bytes("body_zstd")?,
    })
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

    let mut peak_rate = 0.0_f64;
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
        avg_rate_weight_sec: if points.len() >= 2 {
            let first = &points[0];
            let last = &points[points.len() - 1];
            (last.timestamp_epoch_sec - first.timestamp_epoch_sec).max(0) as f64
        } else {
            0.0
        },
    }
}

fn aggregate_gauge_rollups(
    points: &[RawMetricsHistoryPoint],
) -> [GaugeRollupValue; NUM_GAUGE_METRICS] {
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
    std::array::from_fn(|index| {
        let avg_rate_weight_sec: f64 = points
            .iter()
            .map(|point| point.counter_values[index].avg_rate_weight_sec)
            .sum();
        CounterRollupValue {
            end: points
                .last()
                .map(|point| point.counter_values[index].end)
                .unwrap_or_default(),
            avg_rate: weighted_mean(points.iter().map(|point| {
                (
                    point.counter_values[index].avg_rate,
                    point.counter_values[index].avg_rate_weight_sec,
                )
            })),
            peak_rate: points
                .iter()
                .map(|point| point.counter_values[index].peak_rate)
                .fold(0.0, f64::max),
            avg_rate_weight_sec,
        }
    })
}

fn aggregate_gauge_rollups_from_rollups(
    points: &[RollupMetricsHistoryPoint],
) -> [GaugeRollupValue; NUM_GAUGE_METRICS] {
    std::array::from_fn(|index| {
        let sample_count: u32 = points
            .iter()
            .map(|point| point.gauge_values[index].sample_count)
            .sum();
        GaugeRollupValue {
            avg: weighted_mean(points.iter().map(|point| {
                (
                    point.gauge_values[index].avg,
                    point.gauge_values[index].sample_count as f64,
                )
            })),
            peak: points
                .iter()
                .map(|point| point.gauge_values[index].peak)
                .fold(0.0, f64::max),
            sample_count,
        }
    })
}

fn aggregate_job_rollups_from_rollups(
    points: &[RollupMetricsHistoryPoint],
) -> [GaugeRollupValue; NUM_JOB_STATUS_METRICS] {
    std::array::from_fn(|index| {
        let sample_count: u32 = points
            .iter()
            .map(|point| point.job_status_values[index].sample_count)
            .sum();
        GaugeRollupValue {
            avg: weighted_mean(points.iter().map(|point| {
                (
                    point.job_status_values[index].avg,
                    point.job_status_values[index].sample_count as f64,
                )
            })),
            peak: points
                .iter()
                .map(|point| point.job_status_values[index].peak)
                .fold(0.0, f64::max),
            sample_count,
        }
    })
}

fn aggregate_gauge_rollup_values(values: impl Iterator<Item = f64>) -> GaugeRollupValue {
    let mut total = 0.0;
    let mut count = 0_u64;
    let mut peak = 0.0_f64;
    for value in values {
        total += value;
        count += 1;
        peak = peak.max(value);
    }

    GaugeRollupValue {
        avg: if count == 0 {
            0.0
        } else {
            total / count as f64
        },
        peak,
        sample_count: count as u32,
    }
}

fn weighted_mean(values: impl Iterator<Item = (f64, f64)>) -> f64 {
    let mut weighted_total = 0.0;
    let mut total_weight = 0.0;
    for (value, weight) in values {
        if weight <= 0.0 {
            continue;
        }
        weighted_total += value * weight;
        total_weight += weight;
    }
    if total_weight == 0.0 {
        0.0
    } else {
        weighted_total / total_weight
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
    JOB_STATUS_KEYS
        .iter()
        .position(|candidate| *candidate == label)
}

#[cfg(test)]
mod tests;
