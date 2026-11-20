// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_composition::operations::*;
pub mod async_ops;
pub mod backup;
pub use backup::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupService, BackupServiceError,
    BackupStatus, CategoryRemapInput, CategoryRemapRequirement, PendingRestoreOutcome,
    RestoreOptions, RestoreReport, StableStateExport, apply_pending_restore,
};
pub mod health;
pub mod metrics_store;
pub use metrics_store::{MetricsHistoryCadence, MetricsHistoryWritePlan};
pub mod maintenance;
pub use maintenance::spawn_maintenance_worker;
pub mod logs;
pub use health::{
    BrowseDirectoryError, CreateDirectoryError, DirectoryBrowseEntry, DirectoryBrowseListing,
    browse_directories, create_directory,
};
pub use logs::snapshot_service_logs;
pub mod usage_history;
pub use usage_history::{
    FinishedJobCounts, JOBS_FINISHED, JOBS_REPAIRED, REMOVED_SERVER_ID_PREFIX, RecoveryBytes,
    RemovedServerUsage, ServerStateReading, ServerUsageReading, StageReading, TorrentUsageReading,
    USAGE_FLUSH_INTERVAL_SECS, USAGE_RETENTION_DAYS, UsageConsumerKind, UsageConsumerSummary,
    UsageConsumerTotal, UsageCounts, UsageDayRow, UsageFlush, UsagePeriod, UsageReading,
    UsageRecorder, UsageStart, UsageSummary, period_bounds, summarize_usage, summary_read_from,
    week_start,
};
