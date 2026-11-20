// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Why an extraction failed, as a class the pipeline routes on, beside the
// message it reports.
//
// The class is decided where the failure is still a typed engine error: a
// `RarError`, a 7z `Error`, a `ZipError`, an `io::Error`, or one of weaver's
// own refusals. Nothing downstream reads the message to decide what to do
// with a failure. The message is for logs, events and the job's failure
// reason, and its wording may change freely.

use std::io;

use super::safety::ExtractionFailure;

mod adapters;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExtractionErrorClass {
    // The job's extraction budget refused the work, or was stopped. The job
    // fails on it, and every other worker of the job stops.
    Rejected,
    // No supplied password opens the archive or member.
    PasswordRequired,
    // The extraction ran ahead of what the set's topology knows: a volume
    // the reader asked for is not there yet, or a member is not where the
    // headers placed it. A topology refresh can change the answer.
    StaleTopology,
    // The process ran out of file descriptors. Retried later as it stands.
    CapacityPressure,
    // The archive's bytes are wrong: a checksum that does not match, a block
    // that does not decode, a read that came back short. A repair of those
    // bytes can change the answer.
    Recoverable,
    // Anything else. No repair or retry is known to change it.
    Terminal,
}

// What a job's failure reason opens with when no supplied password opened the
// archive. Integrations read it to tell a password failure from any other.
pub(crate) const PASSWORD_REQUIRED_MARKER: &str = "WEAVER_PASSWORD_REQUIRED: ";

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

    pub(crate) fn class(&self) -> ExtractionErrorClass {
        self.class
    }

    pub(crate) fn rejection(&self) -> Option<&ExtractionFailure> {
        self.rejection.as_ref()
    }

    // The reason a job keeps when it fails on this. A content-policy refusal
    // keeps its own sentence, which the Jobs screen shows as it stands. A
    // password failure carries the marker an integration reads it by, which
    // is the one piece of the reason a consumer is promised.
    pub(crate) fn job_failure_reason(&self) -> String {
        match &self.rejection {
            Some(failure)
                if failure.reason() == super::safety::ExtractionRejectionReason::ContentPolicy =>
            {
                failure.job_failure_reason()
            }
            _ if self.class == ExtractionErrorClass::PasswordRequired => {
                format!("{PASSWORD_REQUIRED_MARKER}{}", self.message)
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
                unrar_rs::VolumeProviderError::Io { error, .. } => io_error_class(error),
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

pub(crate) fn rar_error_class(error: &unrar_rs::RarError) -> ExtractionErrorClass {
    use unrar_rs::RarError;
    match error {
        RarError::Io(error) => io_error_class(error),
        RarError::InvalidSignature
        | RarError::TruncatedHeader { .. }
        | RarError::HeaderCrcMismatch { .. }
        | RarError::BlockHeaderChecksumMismatch { .. }
        | RarError::DataCrcMismatch { .. }
        | RarError::PackedDataCrcMismatch { .. } => ExtractionErrorClass::Recoverable,
        // A limit, a refused allocation or a fault in the decoder says nothing
        // about the archive's bytes, so no repair of them is attempted.
        RarError::ResourceLimit { .. }
        | RarError::AllocationFailed { .. }
        | RarError::Internal { .. } => ExtractionErrorClass::Terminal,
        RarError::EncryptedArchive
        | RarError::EncryptedMember { .. }
        | RarError::InvalidPassword
        | RarError::WrongPassword { .. } => ExtractionErrorClass::PasswordRequired,
        // A BLAKE2 mismatch is the same damage as a CRC mismatch, and so is
        // data cut short; both have always been terminal here, and stay so
        // until the routing is ruled on.
        _ => ExtractionErrorClass::Terminal,
    }
}

// A 7z error's class, read from the error a block error locates rather than
// from the block's kind: a block fails as `Corrupted` on a cancellation, a
// limit or a fault in its coder too, and none of those is damage.
pub(crate) fn sevenz_error_class(error: &sevenz_turbo::Error) -> ExtractionErrorClass {
    use sevenz_turbo::Error;
    if error.is_password_required() {
        return ExtractionErrorClass::PasswordRequired;
    }
    if error.has_checksum_mismatch() {
        return ExtractionErrorClass::Recoverable;
    }
    match error {
        // A coder that finds its stream damaged fails the block's read with
        // the kind that says so.
        Error::BlockDecode { cause, .. } => match cause.as_ref() {
            Error::Io(error, _) => {
                typed_io_error_class(error).unwrap_or_else(|| match error.kind() {
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => {
                        ExtractionErrorClass::Recoverable
                    }
                    _ => io_error_class(error),
                })
            }
            cause => sevenz_error_class(cause),
        },
        Error::Corrupted(_) => ExtractionErrorClass::Recoverable,
        Error::Io(error, _) | Error::FileOpen(error, _) | Error::MaybeBadPassword(error) => {
            io_error_class(error)
        }
        _ => ExtractionErrorClass::Terminal,
    }
}

pub(crate) fn zip_error_class(error: &zip::result::ZipError) -> ExtractionErrorClass {
    use zip::result::ZipError;
    match error {
        ZipError::Io(error) => io_error_class(error),
        ZipError::InvalidPassword => ExtractionErrorClass::PasswordRequired,
        _ => ExtractionErrorClass::Terminal,
    }
}

#[cfg(test)]
mod tests;
