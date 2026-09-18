pub mod application_upgrade;
pub mod auth;
pub mod bandwidth;
pub mod categories;
pub mod e2e_clock;
pub mod e2e_failpoint;
pub mod error;
pub mod events;
pub mod history;
pub mod ingest;
pub mod jobs;
pub mod migration_assets;
mod migration_hook_ids;
pub mod operations;
pub mod persistence;
pub mod pipeline;
pub mod post_processing;
pub mod proxies;
pub mod rss;
pub mod runtime;
pub mod schema_migrations;
pub mod schema_upgrade;
pub mod security;
pub mod servers;
pub mod settings;
pub mod update_check;
/// Transitional, removed in 0.9.1. See the module docs.
pub mod upgrade_compat;
pub mod watch_folder;

pub use auth::{ApiKeyRow, AuthCredentials};
pub use bandwidth::rate_limiter::TokenBucket;
pub use error::Error;
pub use history::{
    CLIENT_REQUEST_ID_ATTRIBUTE_KEY, HistoryFilter, HistoryMetadataEquals, IntegrationEventRow,
    JobEvent, JobHistoryRow, is_public_history_attribute_key, parse_history_metadata,
    public_history_attributes, split_history_metadata,
};
pub use jobs::{
    ActiveFileProgress, ActiveJob, ActivePar2File, CallerScopedIdempotency, DownloadBlockKind,
    DownloadBlockState, DownloadState, DuplicateAction, DuplicateAdmission,
    DuplicateAdmissionRequest, DuplicateBackfillEntry, DuplicateBackfillSource,
    DuplicateBackfillState, DuplicateDecision, DuplicateJobLifecycle, DuplicateJobSummary,
    DuplicateMode, DuplicatePolicy, ExtractionChunk, FieldUpdate, FileSpec, FingerprintEvidence,
    FingerprintKind, JobFingerprint, JobId, JobInfo, JobPhase, JobPhaseProgress, JobSpec, JobState,
    JobStatus, JobUpdate, MessageId, NntpRuntimeActivation, NzbFileId, PhaseCounters, PostState,
    QueueMoveTarget, RecoveredJob, RestoreJobRequest, RunState, SchedulerCommand, SchedulerError,
    SchedulerHandle, SegmentId, SegmentSpec, SemanticCandidateSnapshot, SemanticCandidateSource,
    SemanticCandidateState, SemanticDuplicate, SemanticDuplicateLifecycleEvent,
    SemanticPromotionClaim, SemanticPromotionState, SemanticTerminalCause, ServerId,
    ServerTransportHealth, SharedPipelineState, SubmissionOrigin, classify_semantic_terminal_cause,
    derive_legacy_job_status, epoch_ms_now, job_status_from_persisted_str,
    normalize_semantic_duplicate_key, record_semantic_duplicate_lifecycle_metric,
    runtime_lanes_from_status_snapshot, semantic_duplicate_lifecycle_metrics_snapshot,
};
pub use operations::instrumentation::{
    ARTICLE_LATENCY_BOUNDS, AtomicHistogram, DB_OP_DURATION_BOUNDS, DECODE_TASK_DURATION_BOUNDS,
    DISK_WRITE_DURATION_BOUNDS, DbRuntimeMetrics, DbRuntimeMetricsSnapshot, DiskSpaceSnapshot,
    EXTRACT_MEMBER_DURATION_BOUNDS, HISTOGRAM_MAX_BUCKETS, HTTP_REQUEST_DURATION_BOUNDS,
    HistogramSnapshot, HttpMetricsSnapshot, HttpRequestCount, JOB_DURATION_BOUNDS, JobFinishCount,
    JobLifecycleMetrics, JobLifecycleMetricsSnapshot, JobResultKind, JobStageKind,
    JobSubmissionCount, PipelineHistograms, PipelineHistogramsSnapshot, ProcessMetricsSnapshot,
    STAGE_DURATION_BOUNDS, ServerAttemptCount, ServerAttemptOutcomeKind, ServerCounters,
    ServerMetricsRegistry, ServerMetricsSnapshot, StageOutcomeKind, VerificationOutcomeKind,
};
pub use operations::metrics::{
    DownloadPressureReason, DownloadPressureState, MetricsSnapshot, PAR3_MEMORY_CATEGORIES,
    PAR3_SLOTS, PAR3_STALL_THRESHOLD_MS, Par3AdmissionReason, Par3EngineNarrowing,
    Par3EngineRefusal, Par3MetricsSnapshot, Par3OutcomeClass, Par3Phase, Par3SlotSnapshot,
    Par3Stage, PipelineMetrics, par3_memory_category_names,
};
pub use operations::{
    AsyncOperationState, AsyncOperationTargetState, COUNTER_METRIC_KEYS, CounterRollupValue,
    GAUGE_METRIC_KEYS, GaugeRollupValue, HistoryDeleteOperationInsertError,
    HistoryDeleteOperationPayload, HistoryDeleteOperationRow, HistoryDeleteOperationSummary,
    HistoryDeleteRowState, HistoryDeleteTargetWork, JOB_STATUS_KEYS, MetricsHistoryChunkRow,
    MetricsHistoryQueryData, MetricsHistoryQueryResult, MetricsHistoryTier, RawMetricsHistoryPoint,
    RollupMetricsHistoryPoint, StableStateExport,
};
pub use persistence::{Database, StateError};
pub use pipeline::Pipeline;
pub use pipeline::download::{DownloadQueue, DownloadWork};
pub use rss::{RssFeedRow, RssRuleAction, RssRuleRow, RssSeenItemRow};
pub use runtime::affinity::{
    install_tokio_worker_affinity, pin_current_thread_for_hot_download_path,
};
pub use runtime::tuning::{RuntimeTuner, TunedParameters};

/// Allocation counter for the tests that assert a hot path allocates nothing.
///
/// Test builds only. It forwards every request to the system allocator and
/// counts allocations *per thread*, which is the only way to prove from inside
/// a multi-threaded test binary that one closure performed none: a
/// process-wide counter would pick up every other test running beside it.
#[cfg(test)]
pub(crate) mod alloc_probe {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    // `const`-initialized and `Drop`-free, so reading or writing it cannot
    // itself allocate or re-enter the allocator during thread teardown.
    thread_local! {
        static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    }

    fn record() {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
    }

    pub(crate) struct Counting;

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record();
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record();
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    /// Allocations the calling thread has made so far.
    pub(crate) fn allocations() -> u64 {
        ALLOCATIONS.try_with(Cell::get).unwrap_or(0)
    }
}

#[cfg(test)]
#[global_allocator]
static ALLOCATOR: alloc_probe::Counting = alloc_probe::Counting;
