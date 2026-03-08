use weaver_core::id::JobId;

/// Errors that can occur within the scheduler.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("job {0} not found")]
    JobNotFound(JobId),

    #[error("job {0} already exists")]
    JobExists(JobId),

    #[error("assembly error: {0}")]
    Assembly(#[from] weaver_assembly::AssemblyError),

    #[error("state error: {0}")]
    State(#[from] weaver_state::StateError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("channel closed")]
    ChannelClosed,

    #[error("{0}")]
    Other(String),
}
