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
        || zip_9_crc32_mismatch(error)
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

// zstd 0.13 (libzstd 1.5.7): a frame whose content checksum does not match
// is an `io::Error` carrying `ZSTD_getErrorName`'s text. zstd exposes no
// typed error code through its `Read` adapters.
fn zstd_0_13_checksum_mismatch(text: &str) -> bool {
    text.contains("Restored data doesn't match checksum")
}

// flate2 1.1.10: a gzip member whose CRC-32 or length trailer does not match
// is an `InvalidInput` `io::Error` with this text and no typed cause.
fn flate2_1_1_gzip_checksum_mismatch(text: &str) -> bool {
    text.contains("corrupt gzip stream does not have a matching checksum")
}

// zip 9.0: a member whose CRC-32 does not match is an `InvalidData`
// `io::Error` with this text, raised from its `Crc32Reader`.
fn zip_9_crc32_mismatch(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidData && error.to_string().contains("Invalid checksum")
}

// tar 0.4.46: an entry header whose checksum does not match is an `Other`
// `io::Error` with this text.
fn tar_0_4_header_checksum_mismatch(text: &str) -> bool {
    text.contains("archive header checksum mismatch")
}

#[cfg(test)]
mod tests;
