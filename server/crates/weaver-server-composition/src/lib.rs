// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The composed server below the download pipeline: the composed Database with
// its caches and writer queue, and the domain services built on it that do not
// drive the scheduler. The pipeline crate re-exports these modules under their
// old paths and adds the parts that do.
// Pinned as blanket in the route guard, as the pipeline's was: the tests
// that moved here with their files trip the one lint that carries both the
// route ban and the writer gate, mostly with their own fixture SQL writes.
#![cfg_attr(
    test,
    allow(
        clippy::disallowed_methods,
        reason = "tests write their own fixture rows and dial their own loopback fixtures"
    )
)]
#![deny(clippy::await_holding_lock)]

pub mod bandwidth;
pub mod history;
pub mod ingest;
pub mod jobs;
pub use weaver_server_migrations::migration_assets;
use weaver_server_migrations::migration_hook_ids;
pub mod operations;
pub mod persistence;
pub mod post_processing;
pub mod proxies;
pub mod rss;
pub mod runtime;
pub mod schema_migrations;
pub use weaver_server_migrations::schema_upgrade;
pub mod servers;
pub mod settings;
pub mod speed_test;
pub mod support;
pub mod tags;
pub mod torrents;

pub use weaver_server_base::{clock, e2e_clock, e2e_failpoint, error, events, security};

pub use bandwidth::rate_limiter::TokenBucket;
pub use error::Error;
pub use history::{
    CLIENT_REQUEST_ID_ATTRIBUTE_KEY, HistoryEditFields, HistoryFilter, HistoryMetadataEquals,
    HistoryOrder, HistoryOrderKey, IntegrationEventRow, JobEvent, JobHistoryRow,
    history_release_name, history_search_columns, is_public_history_attribute_key,
    normalize_history_search_text, parse_history_metadata, public_history_attributes,
    split_history_metadata,
};
pub use jobs::{
    ActiveJob, ArchiveHistoryOutcome, ArchiveHistoryResult, CallerScopedIdempotency,
    CancelJobsOutcome, DownloadBlockKind, DownloadBlockState, DownloadState, DuplicateAction,
    DuplicateAdmission, DuplicateAdmissionRequest, DuplicateBackfillEntry, DuplicateBackfillSource,
    DuplicateBackfillState, DuplicateDecision, DuplicateJobLifecycle, DuplicateJobSummary,
    DuplicateMode, DuplicatePolicy, EgressQuotaBlock, FieldUpdate, FileSpec, FingerprintEvidence,
    FingerprintKind, HISTORY_DELETE_BATCH_SIZE, HardwareProfileInForce, HistoryDeleteBatchOutcome,
    HistoryDeleteOutcome, JobFingerprint, JobId, JobInfo, JobPhase, JobPhaseProgress, JobSpec,
    JobStatus, JobUpdate, MessageId, MessageIds, NntpRuntimeActivation, NzbFileId, PhaseCounters,
    PostState, Protocol, QueueMoveTarget, RecoveredJob, RestartRead, RestoreJobRequest, RunState,
    SchedulerError, SegmentId, SegmentSpec, SemanticCandidateSnapshot, SemanticCandidateSource,
    SemanticCandidateState, SemanticDuplicate, SemanticDuplicateLifecycleEvent,
    SemanticPromotionClaim, SemanticPromotionState, SemanticTerminalCause, ServerId,
    ServerTransportHealth, SubmissionOrigin, classify_semantic_terminal_cause,
    derive_legacy_job_status, epoch_ms_now, job_status_from_persisted_str,
    normalize_semantic_duplicate_key, record_semantic_duplicate_lifecycle_metric,
    runtime_lanes_from_status_snapshot, semantic_duplicate_lifecycle_metrics_snapshot,
};
pub use operations::instrumentation::{
    ARTICLE_LATENCY_BOUNDS, AtomicHistogram, DB_OP_DURATION_BOUNDS, DECODE_TASK_DURATION_BOUNDS,
    DbRuntimeMetrics, DbRuntimeMetricsSnapshot, DiskSpaceSnapshot, EXTRACT_MEMBER_DURATION_BOUNDS,
    HISTOGRAM_MAX_BUCKETS, HTTP_REQUEST_DURATION_BOUNDS, HistogramSnapshot, HttpMetricsSnapshot,
    HttpRequestCount, JOB_DURATION_BOUNDS, JobFinishCount, JobLifecycleMetrics,
    JobLifecycleMetricsSnapshot, JobResultKind, JobStageKind, JobSubmissionCount,
    PipelineHistograms, PipelineHistogramsSnapshot, ProcessMetricsSnapshot, STAGE_DURATION_BOUNDS,
    ServerArticleOutcomes, ServerAttemptCount, ServerAttemptOutcomeKind, ServerCounters,
    ServerMetricsRegistry, ServerMetricsSnapshot, StageOutcomeKind, VerificationOutcomeKind,
};
pub use operations::metrics::{
    DownloadPressureReason, DownloadPressureState, MetricsSnapshot, PAR3_MEMORY_CATEGORIES,
    PAR3_SLOTS, PAR3_STALL_THRESHOLD_MS, Par3AdmissionReason, Par3EngineNarrowing,
    Par3EngineRefusal, Par3MetricsSnapshot, Par3OutcomeClass, Par3Phase, Par3SlotSnapshot,
    Par3Stage, PipelineMetrics, SchedulerBlockClause, par3_memory_category_names,
};
pub use operations::{
    ArchiveHistoryAcceptOutcome, ArchiveHistoryAcceptance, ArchiveHistorySettlement,
    AsyncOperationState, AsyncOperationTargetState, COUNTER_METRIC_KEYS, CounterRollupValue,
    GAUGE_METRIC_KEYS, GaugeRollupValue, HistoryDeleteOperationInsertError,
    HistoryDeleteOperationPayload, HistoryDeleteOperationRow, HistoryDeleteOperationSummary,
    HistoryDeleteRowState, HistoryDeleteTargetWork, HistoryOperationAction, JOB_STATUS_KEYS,
    MetricsHistoryChunkRow, MetricsHistoryQueryData, MetricsHistoryQueryResult, MetricsHistoryTier,
    RETIRED_METRIC_KEY, RawMetricsHistoryPoint, RollupMetricsHistoryPoint, StableStateExport,
};
pub use persistence::{Database, LoadError, Refusal, StateError};
pub use rss::{RssFeedRow, RssRuleAction, RssRuleRow, RssSeenDecision, RssSeenItemRow};
pub use runtime::affinity::{
    install_tokio_worker_affinity, pin_current_thread_for_hot_download_path,
};
pub use runtime::hardware_profile::{HardwareProfile, ProfileTuning};
pub use runtime::tuning::{RuntimeTuner, TunedParameters};
pub use weaver_server_auth::auth::{ApiKeyRow, AuthCredentials};
