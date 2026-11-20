// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod async_ops;
pub mod backup;
pub mod disk {
    pub use weaver_server_disk::operations::disk::*;

    #[cfg(test)]
    mod tests;
}
pub mod maintenance;
pub mod metrics_store;
pub mod recovery;
pub mod usage_history;

pub use weaver_server_base::operations::{instrumentation, metrics};

pub use async_ops::{
    ArchiveHistoryAcceptOutcome, ArchiveHistoryAcceptance, ArchiveHistorySettlement,
    AsyncOperationState, AsyncOperationTargetState, HistoryDeleteOperationInsertError,
    HistoryDeleteOperationPayload, HistoryDeleteOperationRow, HistoryDeleteOperationSummary,
    HistoryDeleteRowState, HistoryDeleteTargetWork, HistoryOperationAction,
};
pub use backup::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupService, BackupServiceError,
    BackupStatus, CategoryRemapInput, CategoryRemapRequirement, PendingRestoreOutcome,
    RestoreOptions, RestoreReport, StableStateExport, apply_pending_restore,
};
pub use disk::{
    Capacity, CapacityDebits, CapacityReader, CapacityReading, DiskProbeError, DiskSpace,
    LateClassification, OnDemandSpace, Reach, StorageCapacity, StorageRoot, VolumeBuffers,
    VolumeWriters, is_out_of_space, is_quota_exceeded,
};
pub use instrumentation::{
    ARTICLE_LATENCY_BOUNDS, AtomicHistogram, DB_OP_DURATION_BOUNDS, DECODE_TASK_DURATION_BOUNDS,
    DbRuntimeMetrics, DbRuntimeMetricsSnapshot, DiskSpaceSnapshot, EXTRACT_MEMBER_DURATION_BOUNDS,
    HISTOGRAM_MAX_BUCKETS, HTTP_REQUEST_DURATION_BOUNDS, HistogramSnapshot, HttpMetricsSnapshot,
    HttpRequestCount, JOB_DURATION_BOUNDS, JobFinishCount, JobLifecycleMetrics,
    JobLifecycleMetricsSnapshot, JobResultKind, JobStageKind, JobSubmissionCount,
    PipelineHistograms, PipelineHistogramsSnapshot, ProcessMetricsSnapshot, STAGE_DURATION_BOUNDS,
    ServerArticleOutcomes, ServerAttemptCount, ServerAttemptOutcomeKind, ServerCounters,
    ServerMetricsRegistry, ServerMetricsSnapshot, StageOutcomeKind, VerificationOutcomeKind,
};
pub use maintenance::spawn_maintenance_worker;
pub use metrics_store::{
    COUNTER_METRIC_KEYS, CounterRollupValue, GAUGE_METRIC_KEYS, GaugeRollupValue, JOB_STATUS_KEYS,
    JobStatusCounts, MetricsHistoryCadence, MetricsHistoryChunkRow, MetricsHistoryQueryData,
    MetricsHistoryQueryResult, MetricsHistoryTier, MetricsHistoryWritePlan,
    RAW_METRICS_RESOLUTION_SECS, RAW_METRICS_RETENTION_SECS, RETIRED_METRIC_KEY,
    ROLLUP_1H_RESOLUTION_SECS, ROLLUP_1H_RETENTION_SECS, ROLLUP_5M_RESOLUTION_SECS,
    ROLLUP_5M_RETENTION_SECS, RawMetricsHistoryPoint, RollupMetricsHistoryPoint,
};
pub use recovery::{
    RecoveredServerState, RestoreCandidate, recover_server_state, recover_server_state_with,
};
pub(crate) use recovery::{apply_password_override, read_stored_files, restore_request};
pub use usage_history::{
    FinishedJobCounts, JOBS_FINISHED, JOBS_REPAIRED, REMOVED_SERVER_ID_PREFIX, RecoveryBytes,
    RemovedServerUsage, ServerStateReading, ServerUsageReading, StageReading, TorrentUsageReading,
    USAGE_FLUSH_INTERVAL_SECS, USAGE_RETENTION_DAYS, UsageConsumerKind, UsageConsumerSummary,
    UsageConsumerTotal, UsageCounts, UsageDayRow, UsageFlush, UsagePeriod, UsageReading,
    UsageRecorder, UsageStart, UsageSummary, period_bounds, summarize_usage, summary_read_from,
    week_start,
};
