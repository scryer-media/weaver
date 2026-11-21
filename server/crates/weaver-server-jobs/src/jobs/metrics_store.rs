// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::control::JobInfo;
use super::model::JobStatus;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use weaver_server_base::operations::metrics::MetricsSnapshot;

pub const RAW_METRICS_RESOLUTION_SECS: i64 = 10;
pub const ROLLUP_5M_RESOLUTION_SECS: i64 = 5 * 60;
pub const ROLLUP_1H_RESOLUTION_SECS: i64 = 60 * 60;
pub const RAW_METRICS_RETENTION_SECS: i64 = 60 * 60;
pub const ROLLUP_5M_RETENTION_SECS: i64 = 24 * 60 * 60;
pub const ROLLUP_1H_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;
const RAW_CHUNK_SPAN_SECS: i64 = 60 * 60;
const ROLLUP_CHUNK_SPAN_SECS: i64 = 24 * 60 * 60;

// The key of a slot whose metric is gone. Stored history keeps its values by
// position, so the slot stays where it was: nothing new is written to it and
// no series is read from it.
pub const RETIRED_METRIC_KEY: &str = "";

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
    "weaver_pipeline_download_restart_durable_lead_blocked_total",
    "weaver_pipeline_download_pressure_stall_duration_seconds",
];

pub const GAUGE_METRIC_KEYS: [&str; NUM_GAUGE_METRICS] = [
    "weaver_pipeline_current_download_speed_bytes_per_second",
    "weaver_pipeline_download_queue_depth",
    "weaver_pipeline_articles_in_flight",
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
    RETIRED_METRIC_KEY,
    "weaver_pipeline_articles_per_second",
    "weaver_pipeline_decode_rate_mebibytes_per_second",
    "weaver_pipeline_write_pending_bytes",
    "weaver_pipeline_uu_spooled_bytes",
    "weaver_pipeline_uu_spooled_segments",
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
    "queued_post_processing",
    "post_processing",
    "failed",
    "complete",
    "awaiting_queue_scripts",
];

pub const NUM_COUNTER_METRICS: usize = 19;
pub const NUM_GAUGE_METRICS: usize = 27;
pub const NUM_JOB_STATUS_METRICS: usize = 15;

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

    pub const fn retention_sec(self) -> i64 {
        match self {
            Self::Raw10s => RAW_METRICS_RETENTION_SECS,
            Self::Rollup5m => ROLLUP_5M_RETENTION_SECS,
            Self::Rollup1h => ROLLUP_1H_RETENTION_SECS,
        }
    }

    pub const fn chunk_span_sec(self) -> i64 {
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

impl RawMetricsHistoryPoint {
    pub fn from_snapshot(
        timestamp_epoch_sec: i64,
        snapshot: &MetricsSnapshot,
        jobs: &JobStatusCounts,
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
                snapshot.download_restart_durable_lead_blocked_total as f64,
                snapshot.download_pressure_stall_duration_ms as f64 / 1000.0,
            ],
            gauge_values: [
                snapshot.current_download_speed as f64,
                snapshot.download_queue_depth as f64,
                snapshot.articles_in_flight as f64,
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
                // Once the loop's write timing: retired, kept for the position.
                0.0,
                snapshot.articles_per_sec,
                snapshot.decode_rate_mbps,
                snapshot.write_pending_bytes as f64,
                snapshot.uu_spooled_bytes as f64,
                snapshot.uu_spooled_segments as f64,
            ],
            job_status_values: jobs.0,
        }
    }
}

// How many jobs are in each status, in `JOB_STATUS_KEYS` order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JobStatusCounts([f64; NUM_JOB_STATUS_METRICS]);

impl Default for JobStatusCounts {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl JobStatusCounts {
    pub const EMPTY: Self = Self([0.0; NUM_JOB_STATUS_METRICS]);

    pub fn from_statuses<'a>(statuses: impl IntoIterator<Item = &'a JobStatus>) -> Self {
        Self::default().with_statuses(statuses)
    }

    pub fn with_statuses<'a>(self, statuses: impl IntoIterator<Item = &'a JobStatus>) -> Self {
        let mut counts = self.0;
        for status in statuses {
            if let Some(index) = job_status_index(status) {
                counts[index] += 1.0;
            }
        }
        Self(counts)
    }

    pub fn from_jobs(jobs: &[JobInfo]) -> Self {
        Self::from_statuses(jobs.iter().map(|job| &job.status))
    }

    // Jobs whose status is named `key` in `JOB_STATUS_KEYS`.
    pub fn count(&self, key: &str) -> f64 {
        JOB_STATUS_KEYS
            .iter()
            .position(|candidate| *candidate == key)
            .map_or(0.0, |index| self.0[index])
    }
}

fn job_status_index(status: &JobStatus) -> Option<usize> {
    let label = match status {
        JobStatus::AwaitingQueueScripts => "awaiting_queue_scripts",
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
        JobStatus::QueuedPostProcessing => "queued_post_processing",
        JobStatus::PostProcessing => "post_processing",
        // The history chunks persist a fixed-width status array, so a new
        // key would orphan every stored chunk; clients read Partial as failed.
        JobStatus::Failed { .. } | JobStatus::Partial => "failed",
        JobStatus::Complete => "complete",
        // Torrent states fold into the nearest stored key for the same reason.
        JobStatus::FetchingMetadata | JobStatus::Stalled => "downloading",
        JobStatus::CheckingFiles => "checking",
        JobStatus::Seeding => "complete",
    };
    JOB_STATUS_KEYS
        .iter()
        .position(|candidate| *candidate == label)
}
