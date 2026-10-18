// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What an extraction failure means for the job, decided where the failure is
// still a typed library error.
//
// Three answers: no supplied password opens it (`NeedsPassword`); the
// archive's bytes are wrong, a checksum that does not match, a block that
// does not decode, a read that came back short, so a repair of those bytes
// can change the answer (`Unrepairable` until one does); and anything else,
// which no repair or retry is known to change (`Unsupported`).
//

use std::io;

use ::zip::result::ZipError;
use weaver_graph::{Emit, Why};

const PASSWORD: Emit = Emit::NeedsPassword;
const DAMAGED: Emit = Emit::Refuse(Why::Unrepairable);
const TERMINAL: Emit = Emit::Refuse(Why::Unsupported);

pub(crate) fn sevenz(error: &sevenz_turbo::Error) -> Emit {
    use sevenz_turbo::{BlockErrorKind, Error};
    match error {
        // A run reads verified bytes only, so a decode that fails under a
        // candidate key cannot be damage: what is left is the key, and the
        // next candidate is tried.
        Error::PasswordRequired | Error::MaybeBadPassword(_) => PASSWORD,
        Error::BlockDecode {
            kind: BlockErrorKind::Corrupted | BlockErrorKind::ChecksumMismatch,
            ..
        } => DAMAGED,
        Error::BlockDecode { kind, message, .. } => {
            sevenz_0_28_block_error(*kind, message, &error.to_string())
        }
        Error::ChecksumVerificationFailed | Error::NextHeaderCrcMismatch => DAMAGED,
        Error::Io(error, _) | Error::FileOpen(error, _) => self::io(error),
        Error::Other(text) if sevenz_0_28_other_names_checksum(text) => DAMAGED,
        _ => TERMINAL,
    }
}

// An entry the library refuses to open without a password is the zip form of
// an encrypted member no supplied password decrypts.
pub(crate) fn zip(error: &ZipError) -> Emit {
    match error {
        ZipError::Io(error) => self::io(error),
        ZipError::InvalidPassword => PASSWORD,
        ZipError::UnsupportedArchive(why) if *why == ZipError::PASSWORD_REQUIRED => PASSWORD,
        ZipError::InvalidArchive(detail) if zip_8_6_unicode_extra_field_checksum(detail) => DAMAGED,
        _ => TERMINAL,
    }
}

// A read that failed inside an entry.
pub(crate) fn zip_read(error: &io::Error, encrypted: bool) -> Emit {
    match error.kind() {
        // A run reads verified bytes only, so a checksum that fails in
        // enciphered content cannot be damage: what is left is the key.
        io::ErrorKind::InvalidData if encrypted => PASSWORD,
        _ => self::io(error),
    }
}

// An I/O error: from the typed error it carries when it carries one, and from
// the rendering of a crate that hands its cause over only as text.
pub(crate) fn io(error: &io::Error) -> Emit {
    if let Some(inner) = error.get_ref() {
        if let Some(error) = inner.downcast_ref::<sevenz_turbo::Error>() {
            return sevenz(error);
        }
        if let Some(error) = inner.downcast_ref::<ZipError>() {
            return zip(error);
        }
        if let Some(error) = inner.downcast_ref::<io::Error>() {
            return self::io(error);
        }
    }
    let text = error.to_string();
    let damaged = std_read_exact_eof(&text)
        || zstd_0_13_checksum_mismatch(&text)
        || zip_8_6_crc32_mismatch(error);
    if damaged { DAMAGED } else { TERMINAL }
}

// Causes that reach weaver only as rendered text. Each reads one crate's
// rendering of one cause, because that crate hands the cause over as text and
// nothing else, and each is pinned by a test against the rendering the pinned
// crate version produces, so a bump that rewords it fails the test rather
// than moving a failure from one answer to another.

// sevenz-turbo 0.28.0: `Error::BlockDecode` keeps its cause only as the
// rendered `message`, the debug form of the error the block failed on. The
// password, the I/O kind, and a checksum failure wrapped in an I/O error are
// all read back from it.
fn sevenz_0_28_block_error(
    kind: sevenz_turbo::BlockErrorKind,
    message: &str,
    rendering: &str,
) -> Emit {
    if kind == sevenz_turbo::BlockErrorKind::Password && message == "PasswordRequired" {
        return PASSWORD;
    }
    if kind == sevenz_turbo::BlockErrorKind::Io
        && matches!(
            sevenz_0_28_block_io_error_kind(message),
            Some(io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof)
        )
    {
        return DAMAGED;
    }
    let lower = rendering.to_ascii_lowercase();
    let damaged = lower.contains("checksum")
        || lower.contains("crc mismatch")
        || lower.contains("crcmismatch")
        || std_read_exact_eof(&lower)
        || zstd_0_13_checksum_mismatch(rendering);
    if damaged { DAMAGED } else { TERMINAL }
}

// The kind of the read failure behind an I/O block error. Every shape of
// `io::Error`'s debug form names the kind before anything else:
// `Kind(UnexpectedEof)`, or `kind: InvalidData` in the custom, message and OS
// shapes. The first kind named is the reader's own, never one quoted inside
// its message.
fn sevenz_0_28_block_io_error_kind(message: &str) -> Option<io::ErrorKind> {
    let start = ["kind: ", "Kind("]
        .iter()
        .filter_map(|marker| message.find(marker).map(|at| at + marker.len()))
        .min()?;
    let rest = &message[start..];
    let named = &rest[..rest
        .find(|c: char| !c.is_ascii_alphanumeric())
        .unwrap_or(rest.len())];
    match named {
        "InvalidData" => Some(io::ErrorKind::InvalidData),
        "UnexpectedEof" => Some(io::ErrorKind::UnexpectedEof),
        _ => None,
    }
}

// sevenz-turbo 0.28.0: `Error::Other` raised outside a block carries only
// text. One of its texts names a checksum, and a 7z failure naming one is
// kept for repair.
fn sevenz_0_28_other_names_checksum(text: &str) -> bool {
    text.to_ascii_lowercase().contains("checksum")
}

// std: `Read::read_exact` past the end of its reader. Its error is a constant
// with no typed cause beyond `UnexpectedEof`, which other short reads share
// with their own wording. Matched as a substring because a reader may embed it
// in a message of its own.
fn std_read_exact_eof(text: &str) -> bool {
    text.contains("failed to fill whole buffer")
}

// zstd 0.13 (libzstd 1.5.7): a frame whose content checksum does not match
// is an `io::Error` carrying `ZSTD_getErrorName`'s text. zstd exposes no
// typed error code through its `Read` adapters.
fn zstd_0_13_checksum_mismatch(text: &str) -> bool {
    text.contains("Restored data doesn't match checksum")
}

// zip 8.6.0: a member whose CRC-32 does not match is an `InvalidData`
// `io::Error` with this text, raised from its `Crc32Reader`.
fn zip_8_6_crc32_mismatch(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidData && error.to_string().contains("Invalid checksum")
}

// zip 8.6.0: an Info-ZIP Unicode extra field whose CRC-32 does not match the
// name it shadows is `ZipError::InvalidArchive` with this detail.
fn zip_8_6_unicode_extra_field_checksum(detail: &str) -> bool {
    detail.starts_with("CRC32 checksum failed on Unicode extra field")
}

#[cfg(test)]
mod tests;
