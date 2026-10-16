// Why an archive-runtime step failed, as a value the job owner can route on.
//
// Each variant names the step that met the failure and keeps the typed cause
// beside it. The conversion into `ExtractionError` decides the class once:
// a step that lost its authority to a newer generation is stale topology,
// which a refresh can change; a broken file system or a broken invariant is
// terminal.

use std::fmt;
use std::io;

use super::super::error::{ExtractionError, ExtractionErrorClass};
use super::super::safety::ExtractionFailure;
use super::super::session::SessionError;
use crate::pipeline::direct_store::router::DemotionReason;

#[derive(Debug)]
pub(crate) enum RuntimeError {
    // The session lifecycle refused the step.
    Session {
        step: &'static str,
        source: SessionError,
    },
    // The nested routing graph refused the step.
    Routing {
        step: &'static str,
        source: DemotionReason,
    },
    Io {
        step: &'static str,
        source: io::Error,
    },
    // A worker task panicked or was cancelled before it joined.
    Join {
        step: &'static str,
        source: tokio::task::JoinError,
    },
    Checkpoint {
        step: &'static str,
        source: rmp_serde::encode::Error,
    },
    Extraction(ExtractionError),
    // The owner's session was revoked while the step ran, so its result
    // belongs to a generation nothing reads any more.
    Revoked,
    // The job's memory budget had no room for the named reservation.
    Admission(&'static str),
    // The job owner stopped receiving namespace events.
    CoordinatorClosed,
    // Two namespaces claimed one output path.
    OutputCollision(String),
    // A piece of runtime state the step depends on is absent or contradicts
    // the step. Names the state.
    Invariant(&'static str),
    // A collaborator outside the runtime refused with a sentence rather than
    // a typed cause.
    Refused {
        step: &'static str,
        reason: String,
    },
}

impl RuntimeError {
    pub(crate) fn session(step: &'static str) -> impl FnOnce(SessionError) -> Self {
        move |source| Self::Session { step, source }
    }

    pub(crate) fn routing(step: &'static str) -> impl FnOnce(DemotionReason) -> Self {
        move |source| Self::Routing { step, source }
    }

    pub(crate) fn io(step: &'static str) -> impl FnOnce(io::Error) -> Self {
        move |source| Self::Io { step, source }
    }

    pub(crate) fn join(step: &'static str) -> impl FnOnce(tokio::task::JoinError) -> Self {
        move |source| Self::Join { step, source }
    }

    pub(crate) fn checkpoint(step: &'static str) -> impl FnOnce(rmp_serde::encode::Error) -> Self {
        move |source| Self::Checkpoint { step, source }
    }

    pub(crate) fn refused<E: fmt::Display>(step: &'static str) -> impl FnOnce(E) -> Self {
        move |reason| Self::Refused {
            step,
            reason: reason.to_string(),
        }
    }

    // The class the pipeline routes this failure by.
    pub(crate) fn class(&self) -> ExtractionErrorClass {
        match self {
            Self::Revoked
            | Self::Session {
                source: SessionError::Stale,
                ..
            } => ExtractionErrorClass::StaleTopology,
            Self::Extraction(error) => error.class(),
            Self::Session { .. }
            | Self::Routing { .. }
            | Self::Io { .. }
            | Self::Join { .. }
            | Self::Checkpoint { .. }
            | Self::Admission(_)
            | Self::CoordinatorClosed
            | Self::OutputCollision(_)
            | Self::Invariant(_)
            | Self::Refused { .. } => ExtractionErrorClass::Terminal,
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session { step, source } => write!(formatter, "{step}: {source}"),
            Self::Routing { step, source } => write!(formatter, "{step}: {source}"),
            Self::Io { step, source } => write!(formatter, "{step}: {source}"),
            Self::Join { step, source } => write!(formatter, "{step}: {source}"),
            Self::Checkpoint { step, source } => write!(formatter, "{step}: {source}"),
            Self::Extraction(error) => error.fmt(formatter),
            Self::Revoked => formatter.write_str("archive owner revoked"),
            Self::Admission(reservation) => {
                write!(formatter, "{reservation} memory admission unavailable")
            }
            Self::CoordinatorClosed => formatter.write_str("archive coordinator closed"),
            Self::OutputCollision(path) => {
                write!(
                    formatter,
                    "archive output collides with another namespace: {path}"
                )
            }
            Self::Invariant(state) => write!(formatter, "archive runtime invariant: {state}"),
            Self::Refused { step, reason } => write!(formatter, "{step}: {reason}"),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Session { source, .. } => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::Join { source, .. } => Some(source),
            Self::Checkpoint { source, .. } => Some(source),
            Self::Extraction(error) => Some(error),
            Self::Routing { .. }
            | Self::Revoked
            | Self::Admission(_)
            | Self::CoordinatorClosed
            | Self::OutputCollision(_)
            | Self::Invariant(_)
            | Self::Refused { .. } => None,
        }
    }
}

impl From<ExtractionError> for RuntimeError {
    fn from(error: ExtractionError) -> Self {
        Self::Extraction(error)
    }
}

impl From<ExtractionFailure> for RuntimeError {
    fn from(failure: ExtractionFailure) -> Self {
        Self::Extraction(ExtractionError::rejected(failure))
    }
}

impl From<RuntimeError> for ExtractionError {
    fn from(error: RuntimeError) -> Self {
        match error {
            RuntimeError::Extraction(error) => error,
            other => ExtractionError::new(other.class(), other.to_string()),
        }
    }
}

// The job owner and the worker still report failures as sentences.
impl From<RuntimeError> for String {
    fn from(error: RuntimeError) -> Self {
        error.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lost_authority_is_stale_topology() {
        for error in [
            RuntimeError::Revoked,
            RuntimeError::session("step")(SessionError::Stale),
        ] {
            assert!(ExtractionError::from(error).is(ExtractionErrorClass::StaleTopology));
        }
    }

    #[test]
    fn broken_state_is_terminal() {
        for error in [
            RuntimeError::session("step")(SessionError::InvalidTransition),
            RuntimeError::io("step")(io::Error::other("disk")),
            RuntimeError::Invariant("roots"),
            RuntimeError::routing("step")(DemotionReason::ConflictingVolumeFacts),
        ] {
            assert!(ExtractionError::from(error).is(ExtractionErrorClass::Terminal));
        }
    }

    #[test]
    fn extraction_failures_keep_their_class() {
        let error = RuntimeError::from(ExtractionError::recoverable("crc"));
        assert!(ExtractionError::from(error).is(ExtractionErrorClass::Recoverable));
    }
}
