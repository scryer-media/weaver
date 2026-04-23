pub mod assembly;
pub mod error;
pub mod handle;
pub mod ids;
pub mod model;
pub mod persistence;
pub mod queries;
pub mod record;
pub mod repository;
pub mod service;
pub mod working_dir;

pub use error::SchedulerError;
pub use handle::{
    DownloadBlockKind, DownloadBlockState, JobInfo, RestoreJobRequest, SchedulerCommand,
    SchedulerHandle, SharedPipelineState,
};
pub use ids::{ConnectionId, JobId, MessageId, NzbFileId, SegmentId, ServerId};
pub use model::{
    DownloadState, FieldUpdate, FileSpec, JobSpec, JobState, JobStatus, JobUpdate, PostState,
    RunState, SegmentSpec, derive_legacy_job_status, epoch_ms_now, job_status_from_persisted_str,
    runtime_lanes_from_status_snapshot,
};
pub use record::{
    ActiveFileIdentity, ActiveFileProgress, ActiveJob, ActivePar2File, CommittedSegment,
    ExtractionChunk, FileIdentitySource, RecoveredJob,
};
