// Why an extraction failed, as a class the pipeline routes on, beside the
// message it reports.
//
// The class is decided where the failure is still a typed engine error: a
// `RarError`, a 7z `Error`, a `ZipError`, an `io::Error`, or one of weaver's
// own refusals. Nothing downstream reads the message to decide what to do
// with a failure. The message is for logs, events and the job's failure
// reason, and its wording may change freely.

use std::io;

use super::safety::{ExtractionFailure, JobExtractionBudget};

mod adapters;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExtractionErrorClass {
    // The job's extraction budget refused the work, or was stopped. The job
    // fails on it, and every other worker of the job stops.
    Rejected,
    // No supplied password opens the archive or member.
    PasswordRequired,
    // The archive's own structure is refused before a member is read: two
    // members that land on one path, or a path that escapes the output root.
    // No repair or retry changes the names an archive carries.
    StructureRefused,
    // The extraction ran ahead of what the set's topology knows: a volume
    // the reader asked for is not there yet, or a member is not where the
    // headers placed it. A topology refresh can change the answer.
    StaleTopology,
    // The process ran out of file descriptors. Retried later as it stands.
    CapacityPressure,
    // A RAR set has no first volume to open from.
    MissingFirstVolume,
    // The archive's bytes are wrong: a checksum that does not match, a block
    // that does not decode, a read that came back short. A repair of those
    // bytes can change the answer.
    Recoverable,
    // Anything else. No repair or retry is known to change it.
    Terminal,
}

#[derive(Debug, Clone)]
pub(crate) struct ExtractionError {
    class: ExtractionErrorClass,
    message: String,
    rejection: Option<ExtractionFailure>,
}

impl ExtractionError {
    pub(crate) fn new(class: ExtractionErrorClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
            rejection: None,
        }
    }

    pub(crate) fn terminal(message: impl Into<String>) -> Self {
        Self::new(ExtractionErrorClass::Terminal, message)
    }

    pub(crate) fn recoverable(message: impl Into<String>) -> Self {
        Self::new(ExtractionErrorClass::Recoverable, message)
    }

    pub(crate) fn password_required(message: impl Into<String>) -> Self {
        Self::new(ExtractionErrorClass::PasswordRequired, message)
    }

    pub(crate) fn structure_refused(message: impl Into<String>) -> Self {
        Self::new(ExtractionErrorClass::StructureRefused, message)
    }

    pub(crate) fn stale_topology(message: impl Into<String>) -> Self {
        Self::new(ExtractionErrorClass::StaleTopology, message)
    }

    pub(crate) fn missing_first_volume(message: impl Into<String>) -> Self {
        Self::new(ExtractionErrorClass::MissingFirstVolume, message)
    }

    pub(crate) fn rejected(failure: ExtractionFailure) -> Self {
        Self {
            class: ExtractionErrorClass::Rejected,
            message: failure.to_string(),
            rejection: Some(failure),
        }
    }

    // `message` reported, classed by the I/O error it describes.
    pub(crate) fn from_io(message: impl Into<String>, error: &io::Error) -> Self {
        Self::new(io_error_class(error), message)
    }

    // The I/O error itself, rendered as it stands.
    pub(crate) fn io(error: &io::Error) -> Self {
        Self::from_io(error.to_string(), error)
    }

    pub(crate) fn from_rar(message: impl Into<String>, error: &unrar_rs::RarError) -> Self {
        Self::new(rar_error_class(error), message)
    }

    pub(crate) fn from_zip(message: impl Into<String>, error: &zip::result::ZipError) -> Self {
        Self::new(zip_error_class(error), message)
    }

    pub(crate) fn class(&self) -> ExtractionErrorClass {
        self.class
    }

    pub(crate) fn is(&self, class: ExtractionErrorClass) -> bool {
        self.class == class
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn rejection(&self) -> Option<&ExtractionFailure> {
        self.rejection.as_ref()
    }

    // The same failure under a reworded message.
    pub(crate) fn map_message(mut self, reword: impl FnOnce(String) -> String) -> Self {
        self.message = reword(self.message);
        self
    }

    // The failure as the worker that met it reports it. A worker whose budget
    // has stopped ended on the budget's refusal, whatever error the stop
    // surfaced through: a read that the budget failed, a decoder that gave up
    // when its output was refused.
    pub(crate) fn settle(mut self, budget: &JobExtractionBudget) -> Self {
        if self.class == ExtractionErrorClass::Rejected {
            return self;
        }
        if let Some(failure) = budget.stopped_failure() {
            self.class = ExtractionErrorClass::Rejected;
            self.rejection = Some(failure);
        }
        self
    }

    // The reason a job keeps when it fails on this. A content-policy refusal
    // keeps its own sentence, which the Jobs screen shows as it stands.
    pub(crate) fn job_failure_reason(&self) -> String {
        match &self.rejection {
            Some(failure)
                if failure.reason() == super::safety::ExtractionRejectionReason::ContentPolicy =>
            {
                failure.job_failure_reason()
            }
            _ => self.message.clone(),
        }
    }
}

impl std::fmt::Display for ExtractionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ExtractionError {}

impl From<ExtractionFailure> for ExtractionError {
    fn from(failure: ExtractionFailure) -> Self {
        Self::rejected(failure)
    }
}

impl From<ExtractionError> for String {
    fn from(error: ExtractionError) -> Self {
        error.message
    }
}

// A weaver failure that carries no cause a repair or retry could change,
// reported as terminal where it happens.
pub(crate) trait OrTerminal<T> {
    fn or_terminal(self) -> Result<T, ExtractionError>;
}

impl<T> OrTerminal<T> for Result<T, String> {
    fn or_terminal(self) -> Result<T, ExtractionError> {
        self.map_err(ExtractionError::terminal)
    }
}

// An I/O error's class: from the typed error it carries when it carries one,
// from its OS error code, and from the rendering of a crate that hands its
// cause over only as text.
pub(crate) fn io_error_class(error: &io::Error) -> ExtractionErrorClass {
    typed_io_error_class(error)
        .or_else(|| adapters::untyped_io_class(error))
        .unwrap_or(ExtractionErrorClass::Terminal)
}

fn typed_io_error_class(error: &io::Error) -> Option<ExtractionErrorClass> {
    if let Some(inner) = error.get_ref() {
        if let Some(error) = inner.downcast_ref::<ExtractionError>() {
            return Some(error.class());
        }
        if inner.downcast_ref::<ExtractionFailure>().is_some() {
            return Some(ExtractionErrorClass::Rejected);
        }
        if let Some(error) = inner.downcast_ref::<unrar_rs::RarError>() {
            return Some(rar_error_class(error));
        }
        if let Some(error) = inner.downcast_ref::<unrar_rs::VolumeProviderError>() {
            return Some(match error {
                unrar_rs::VolumeProviderError::Unavailable { .. } => {
                    ExtractionErrorClass::StaleTopology
                }
                unrar_rs::VolumeProviderError::Io(error) => io_error_class(error),
            });
        }
        if let Some(error) = inner.downcast_ref::<sevenz_turbo::Error>() {
            return Some(sevenz_error_class(error));
        }
        if let Some(error) = inner.downcast_ref::<zip::result::ZipError>() {
            return Some(zip_error_class(error));
        }
        if let Some(error) = inner.downcast_ref::<crate::pipeline::capacity::SourceOpenError>() {
            return Some(io_error_class(error.cause()));
        }
        if let Some(error) = inner.downcast_ref::<io::Error>() {
            return Some(io_error_class(error));
        }
    }
    if crate::pipeline::capacity::is_fd_capacity_io_error(error) {
        return Some(ExtractionErrorClass::CapacityPressure);
    }
    // EAGAIN. Its OS rendering says "temporarily unavailable", which the
    // batch route has always read as a volume the provider did not have.
    if error.raw_os_error().is_some() && error.kind() == io::ErrorKind::WouldBlock {
        return Some(ExtractionErrorClass::StaleTopology);
    }
    None
}

// An I/O error unrar-rs returned. unrar-rs 0.11 hands some causes over as
// text only; those are read back before the renderings every engine shares.
fn rar_io_error_class(error: &io::Error) -> ExtractionErrorClass {
    typed_io_error_class(error)
        .or_else(|| adapters::unrar_0_11_erased_class(&error.to_string()))
        .or_else(|| adapters::untyped_io_class(error))
        .unwrap_or(ExtractionErrorClass::Terminal)
}

pub(crate) fn rar_error_class(error: &unrar_rs::RarError) -> ExtractionErrorClass {
    use unrar_rs::RarError;
    match error {
        RarError::Io(error) => rar_io_error_class(error),
        RarError::InvalidSignature
        | RarError::TruncatedHeader { .. }
        | RarError::HeaderCrcMismatch { .. }
        | RarError::DataCrcMismatch { .. }
        | RarError::PackedDataCrcMismatch { .. } => ExtractionErrorClass::Recoverable,
        RarError::CorruptArchive { detail }
            if adapters::unrar_0_11_block_header_checksum_mismatch(detail) =>
        {
            ExtractionErrorClass::Recoverable
        }
        error if crate::pipeline::Pipeline::rar_error_is_password_related(error) => {
            ExtractionErrorClass::PasswordRequired
        }
        // A BLAKE2 mismatch is the same damage as a CRC mismatch, and so is
        // data cut short; both have always been terminal here, and stay so
        // until the routing is ruled on.
        _ => ExtractionErrorClass::Terminal,
    }
}

pub(crate) fn sevenz_error_class(error: &sevenz_turbo::Error) -> ExtractionErrorClass {
    use sevenz_turbo::{BlockErrorKind, Error};
    match error {
        Error::PasswordRequired => ExtractionErrorClass::PasswordRequired,
        Error::BlockDecode {
            kind: BlockErrorKind::Corrupted | BlockErrorKind::ChecksumMismatch,
            ..
        } => ExtractionErrorClass::Recoverable,
        Error::BlockDecode { kind, message, .. } => {
            adapters::sevenz_0_27_block_error_class(*kind, message, &error.to_string())
        }
        Error::ChecksumVerificationFailed | Error::NextHeaderCrcMismatch => {
            ExtractionErrorClass::Recoverable
        }
        Error::Io(error, _) | Error::FileOpen(error, _) | Error::MaybeBadPassword(error) => {
            io_error_class(error)
        }
        Error::Other(text) if adapters::sevenz_0_27_other_names_checksum(text) => {
            ExtractionErrorClass::Recoverable
        }
        _ => ExtractionErrorClass::Terminal,
    }
}

pub(crate) fn zip_error_class(error: &zip::result::ZipError) -> ExtractionErrorClass {
    use zip::result::ZipError;
    match error {
        ZipError::Io(error) => io_error_class(error),
        ZipError::InvalidPassword => ExtractionErrorClass::PasswordRequired,
        ZipError::InvalidArchive(detail)
            if adapters::zip_8_6_unicode_extra_field_checksum(detail) =>
        {
            ExtractionErrorClass::Recoverable
        }
        _ => ExtractionErrorClass::Terminal,
    }
}

// Whether a 7z header listing failed because a Zstandard frame needs a
// larger window than the listing allowed it. The reader's own memory refusals
// are typed; the zstd decoder's is text.
pub(crate) fn zstd_frame_exceeds_window(error: &io::Error) -> bool {
    adapters::zstd_0_13_frame_exceeds_window(&error.to_string())
}

#[cfg(test)]
mod tests;
