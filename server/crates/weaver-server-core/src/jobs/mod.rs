pub mod assembly;
pub mod duplicate;
pub mod duplicate_persistence;
pub mod error;
pub mod handle;
pub mod ids;
pub mod model;
pub mod persistence;
pub mod phase_progress;
pub mod queries;
pub mod record;
pub mod repository;
pub mod service;
pub mod working_dir;

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
    AddJobOptions, DownloadBlockKind, DownloadBlockState, FINISHED_JOBS_RUNTIME_CAP, JobInfo,
    RestoreJobRequest, SchedulerCommand, SchedulerHandle, SharedPipelineState,
};
pub use ids::{ConnectionId, JobId, MessageId, NzbFileId, SegmentId, ServerId};
pub use model::{
    ArchivePasswordCandidate, ArchivePasswordSource, DownloadState, FieldUpdate, FileSpec, JobSpec,
    JobState, JobStatus, JobUpdate, PostState, RunState, SegmentSpec, derive_legacy_job_status,
    epoch_ms_now, job_status_from_persisted_str, runtime_lanes_from_status_snapshot,
};
pub use phase_progress::{JobPhase, JobPhaseProgress, PhaseAttemptCounters, PhaseCounters};
pub use record::{
    ActiveFileIdentity, ActiveFileProgress, ActiveJob, ActivePar2File, ExtractionChunk,
    FileIdentitySource, RecoveredJob,
};
