// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Causes that reach weaver only as rendered text.
//
// Each function reads one crate's rendering of one cause, because that crate
// hands the cause over as text and nothing else. Every one is pinned by a
// test against the rendering the pinned crate version actually produces, so a
// crate bump that rewords it fails the test rather than silently moving a
// failure from one route to another. Nothing outside this module reads an
// error's text to decide what to do with it.

use std::io;

use super::ExtractionErrorClass;

// An I/O error whose cause is only its text: classed by the renderings below,
// or `None` when none of them is in it.
pub(super) fn untyped_io_class(error: &io::Error) -> Option<ExtractionErrorClass> {
    let text = error.to_string();
    let recoverable = std_read_exact_eof(&text)
        || zstd_0_13_checksum_mismatch(&text)
        || flate2_1_1_gzip_checksum_mismatch(&text)
        || zip_8_6_crc32_mismatch(error)
        || tar_0_4_header_checksum_mismatch(&text);
    recoverable.then_some(ExtractionErrorClass::Recoverable)
}

// std: `Read::read_exact` past the end of its reader. Its error is a constant
// with no typed cause beyond `UnexpectedEof`, which other short reads share
// with their own wording. Matched as a substring because a reader may embed it
// in a message of its own.
fn std_read_exact_eof(text: &str) -> bool {
    text.contains("failed to fill whole buffer")
}

// unrar-rs 0.11.0 renders three causes into text before weaver sees them:
// an OS error its reader opened a volume on (`read_error_to_rar` keeps only
// the message), a volume provider's refusal and a header or signature error
// on a continuation volume (both `io::Error::other(e.to_string())`). Read
// back here in the order the batch route has always weighed them: a volume
// the provider does not have, then file-descriptor exhaustion, then damaged
// bytes.
//
// Retired by unrar-rs `feature/typed-error-causes` ("unrar-rs: keep typed
// causes on reader, provider and block-header errors"), which keeps all
// three typed. Delete this function and its call when weaver's unrar-rs pin
// moves to a release that carries it.
pub(super) fn unrar_0_11_erased_class(text: &str) -> Option<ExtractionErrorClass> {
    let lower = text.to_ascii_lowercase();
    // `VolumeProviderError::Unavailable` renders "volume {n} unavailable:
    // {reason}"; the static provider's reason is "not registered". EAGAIN's
    // OS rendering ("Resource temporarily unavailable") has always matched
    // this too.
    if lower.contains("unavailable") || lower.contains("not registered") {
        return Some(ExtractionErrorClass::StaleTopology);
    }
    if os_error_code(text).is_some_and(crate::pipeline::capacity::is_fd_capacity_raw_os_error)
        || text.contains(crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER)
    {
        return Some(ExtractionErrorClass::CapacityPressure);
    }
    // The renderings of `RarError::{HeaderCrcMismatch, DataCrcMismatch,
    // PackedDataCrcMismatch, TruncatedHeader, InvalidSignature}` and of the
    // block-header checksum below.
    let damaged = lower.contains("checksum")
        || lower.contains("crc mismatch")
        || lower.contains("truncated header at offset")
        || lower.contains("not a rar archive (bad signature)");
    damaged.then_some(ExtractionErrorClass::Recoverable)
}

// std: the trailing "(os error N)" every OS error renders with.
fn os_error_code(text: &str) -> Option<i32> {
    let start = text.rfind("(os error ")? + "(os error ".len();
    let rest = &text[start..];
    rest[..rest.find(')')?].parse().ok()
}

// unrar-rs 0.11.0: a RAR5 compressed block whose header checksum does not
// match is `RarError::CorruptArchive` with this detail, the same variant as
// structural corruption no repair touches.
//
// Retired by unrar-rs `feature/typed-error-causes`, which raises it as
// `RarError::BlockHeaderChecksumMismatch`. Delete this function, its arm in
// `rar_error_class`, and add the new variant to the recoverable arm when
// weaver's unrar-rs pin moves to a release that carries it.
pub(super) fn unrar_0_11_block_header_checksum_mismatch(detail: &str) -> bool {
    detail.starts_with("RAR5 block header checksum mismatch")
}

// sevenz-turbo 0.27.0: `Error::BlockDecode` keeps its cause only as the
// rendered `message`, the debug form of the error the block failed on. The
// password, the I/O kind, and a checksum failure wrapped in an I/O error are
// all read back from it.
//
// Retired by sevenz-turbo `feature/typed-error-causes` ("error: keep the
// cause of a block error typed"), which keeps the cause as `BlockDecode`'s
// `cause` and answers `io_error()`, `is_password_required()` and
// `has_checksum_mismatch()`. Delete this function and its call when weaver's
// sevenz-turbo pin moves to a release that carries it.
pub(super) fn sevenz_0_27_block_error_class(
    kind: sevenz_turbo::BlockErrorKind,
    message: &str,
    rendering: &str,
) -> ExtractionErrorClass {
    if kind == sevenz_turbo::BlockErrorKind::Password && message == "PasswordRequired" {
        return ExtractionErrorClass::PasswordRequired;
    }
    if kind == sevenz_turbo::BlockErrorKind::Io
        && matches!(
            sevenz_0_27_block_io_error_kind(message),
            Some(io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof)
        )
    {
        return ExtractionErrorClass::Recoverable;
    }
    let lower = rendering.to_ascii_lowercase();
    let damaged = lower.contains("checksum")
        || lower.contains("crc mismatch")
        || lower.contains("crcmismatch")
        || std_read_exact_eof(&lower)
        || zstd_0_13_checksum_mismatch(rendering);
    if damaged {
        ExtractionErrorClass::Recoverable
    } else {
        ExtractionErrorClass::Terminal
    }
}

// The kind of the read failure behind an I/O block error. Every shape of
// `io::Error`'s debug form names the kind before anything else:
// `Kind(UnexpectedEof)`, or `kind: InvalidData` in the custom, message and OS
// shapes. The first kind named is the reader's own, never one quoted inside
// its message.
fn sevenz_0_27_block_io_error_kind(message: &str) -> Option<io::ErrorKind> {
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

// sevenz-turbo 0.27.0: `Error::Other` raised outside a block carries only
// text. One of its texts names a checksum, and a 7z failure naming one has
// always been kept for repair.
pub(super) fn sevenz_0_27_other_names_checksum(text: &str) -> bool {
    text.to_ascii_lowercase().contains("checksum")
}

// zstd 0.13 (libzstd 1.5.7): a frame whose content checksum does not match
// is an `io::Error` carrying `ZSTD_getErrorName`'s text. zstd exposes no
// typed error code through its `Read` adapters.
fn zstd_0_13_checksum_mismatch(text: &str) -> bool {
    text.contains("Restored data doesn't match checksum")
}

// zstd 0.13 (libzstd 1.5.7): a frame whose window is larger than the decoder
// was allowed. Same channel as the checksum above.
pub(super) fn zstd_0_13_frame_exceeds_window(text: &str) -> bool {
    text.contains("too much memory")
}

// flate2 1.1.10: a gzip member whose CRC-32 or length trailer does not match
// is an `InvalidInput` `io::Error` with this text and no typed cause.
fn flate2_1_1_gzip_checksum_mismatch(text: &str) -> bool {
    text.contains("corrupt gzip stream does not have a matching checksum")
}

// zip 8.6.0: a member whose CRC-32 does not match is an `InvalidData`
// `io::Error` with this text, raised from its `Crc32Reader`.
fn zip_8_6_crc32_mismatch(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidData && error.to_string().contains("Invalid checksum")
}

// zip 8.6.0: an Info-ZIP Unicode extra field whose CRC-32 does not match the
// name it shadows is `ZipError::InvalidArchive` with this detail.
pub(super) fn zip_8_6_unicode_extra_field_checksum(detail: &str) -> bool {
    detail.starts_with("CRC32 checksum failed on Unicode extra field")
}

// tar 0.4.46: an entry header whose checksum does not match is an `Other`
// `io::Error` with this text.
fn tar_0_4_header_checksum_mismatch(text: &str) -> bool {
    text.contains("archive header checksum mismatch")
}

#[cfg(test)]
mod tests;
