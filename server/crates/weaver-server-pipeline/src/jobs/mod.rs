// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod archive_password;
pub(crate) use archive_password::JobsArchivePasswordInternalQueries;
pub mod assembly;
pub mod class;
pub mod diagnostic_names;
pub mod diagnostic_tables;
pub use weaver_server_jobs::jobs::download_summary;
mod download_summary_persistence;
#[cfg(test)]
pub(crate) use download_summary_persistence::JobsDownloadSummaryPersistenceInternalQueries;
pub use download_summary_persistence::JobsDownloadSummaryPersistenceQueries;
pub use weaver_server_jobs::jobs::duplicate;
pub mod duplicate_persistence;
pub mod error;
pub(crate) mod feed_verdict;
pub(crate) mod graph_checkpoint;
pub mod handle;
pub mod model;
pub(crate) use weaver_server_jobs::jobs::nzb_persistence;
pub(crate) mod outline_persistence;
pub mod persistence;
pub use weaver_server_jobs::jobs::phase_progress;
pub mod queries;
pub mod record;
pub mod report;
pub mod repository;
pub use weaver_server_jobs::jobs::server_attribution;
mod server_attribution_persistence;
pub(crate) use server_attribution_persistence::JobsServerAttributionPersistenceInternalQueries;
pub mod service;
pub use support_facts_persistence::JobsSupportFactsPersistenceQueries;
pub use weaver_server_jobs::jobs::support_facts;
use weaver_server_jobs::jobs::support_facts_persistence;
pub mod working_dir;

pub use weaver_server_base::jobs::ids;

pub use class::{
    AddRefusal, ByProtocol, ClassIntake, ClassRate, ClassRates, JobClass, JobClasses, Owed,
    OwnedRows, PipelineClass, RefusalReason, Route, Submission, TunnelGiveBack,
};
pub use duplicate::{
    CallerScopedIdempotency, DuplicateAction, DuplicateDecision, DuplicateJobLifecycle,
    DuplicateMatch, DuplicateMode, DuplicatePolicy, FingerprintEvidence, FingerprintKind,
    JobFingerprint, SemanticDuplicate, SemanticDuplicateLifecycleEvent, SemanticTerminalCause,
    SubmissionOrigin, classify_semantic_terminal_cause, duplicate_admission_metrics_snapshot,
    normalize_semantic_duplicate_key, record_duplicate_admission_metric,
    record_semantic_duplicate_lifecycle_metric, semantic_duplicate_lifecycle_metrics_snapshot,
};
pub use duplicate_persistence::{
    DuplicateAdmission, DuplicateAdmissionRequest, DuplicateBackfillEntry, DuplicateBackfillSource,
    DuplicateBackfillState, DuplicateJobSummary, SemanticAdmission, SemanticCandidateSnapshot,
    SemanticCandidateSource, SemanticCandidateState, SemanticPromotionClaim,
    SemanticPromotionState,
};
pub use error::SchedulerError;
pub use handle::{
    AddJobOptions, ArchiveHistoryOutcome, ArchiveHistoryResult, CancelJobsOutcome,
    DownloadBlockKind, DownloadBlockState, EgressQuotaBlock, FINISHED_JOBS_RUNTIME_CAP,
    HISTORY_DELETE_BATCH_SIZE, HardwareProfileInForce, HistoryDeleteBatchOutcome,
    HistoryDeleteOutcome, Hold, HoldOrigin, HoldOwner, HoldScope, HoldSet, JobInfo,
    NntpRuntimeActivation, QueueMoveTarget, RestartRead, RestoreJobRequest, SchedulerHandle,
    ServerTransportHealth, SharedPipelineState,
};
pub use ids::{ConnectionId, JobId, MessageId, NzbFileId, SegmentId, ServerId};
pub use model::{
    ArchivePasswordCandidate, ArchivePasswordSource, ClassHold, ClassNames, DownloadState,
    FieldUpdate, FileSpec, HeldClass, JobSpec, JobState, JobStatus, JobUpdate, MessageIds,
    PostState, Protocol, RunState, SegmentSpec, derive_legacy_job_status, epoch_ms_now,
    job_status_from_persisted_str, runtime_lanes_from_status_snapshot,
};
pub use phase_progress::{JobPhase, JobPhaseProgress, PhaseAttemptCounters, PhaseCounters};
pub use record::{ActiveFileIdentity, ActiveJob, FileIdentitySource, RecoveredJob};
