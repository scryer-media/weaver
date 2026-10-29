// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Each rendering against the one the pinned crate produces, and each answer
// a typed error gets. Where the crate can be made to fail for real, it is.

use std::io::{Cursor, Read, Write};

use sevenz_turbo::Error;

use super::*;

// A block error over the error the block failed on, which gives its kind.
fn block(cause: Error) -> Error {
    cause.in_block(0, 32)
}

// A block whose reader failed.
fn read_failure(error: io::Error) -> Error {
    block(Error::Io(error, "".into()))
}

#[test]
fn a_short_read_exact_is_damage() {
    let error = Cursor::new([0u8; 3]).read_exact(&mut [0u8; 8]).unwrap_err();
    assert!(std_read_exact_eof(&error.to_string()));
    assert_eq!(io(&error), DAMAGED);
}

#[test]
fn a_zip_member_whose_crc_fails_is_damage() {
    let payload = b"one stored member, read back with one byte changed";
    let mut writer = ::zip::ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file(
            "member.bin",
            ::zip::write::SimpleFileOptions::default()
                .compression_method(::zip::CompressionMethod::Stored),
        )
        .unwrap();
    writer.write_all(payload).unwrap();
    let mut archive = writer.finish().unwrap().into_inner();
    let at = archive
        .windows(payload.len())
        .position(|window| window == payload)
        .unwrap();
    archive[at] ^= 0x01;
    let mut zip = ::zip::ZipArchive::new(Cursor::new(archive)).unwrap();
    let error = zip
        .by_index(0)
        .unwrap()
        .read_to_end(&mut Vec::new())
        .unwrap_err();
    assert!(zip_8_6_crc32_mismatch(&error));
    assert_eq!(io(&error), DAMAGED);
    assert_eq!(super::zip(&ZipError::Io(error)), DAMAGED);
}

#[test]
fn sevenz_block_errors_are_read_from_their_cause() {
    for error in [
        read_failure(io::ErrorKind::UnexpectedEof.into()),
        read_failure(io::Error::new(
            io::ErrorKind::InvalidData,
            "kind: NotFound quoted in the message",
        )),
        read_failure(io::Error::new(io::ErrorKind::UnexpectedEof, "short read")),
        block(Error::Corrupted("detail".into())),
        block(Error::ChecksumVerificationFailed),
    ] {
        assert_eq!(sevenz(&error), DAMAGED, "{error}");
    }
    for error in [
        block(Error::Unsupported("detail".into())),
        read_failure(io::ErrorKind::PermissionDenied.into()),
        read_failure(io::Error::new(
            io::ErrorKind::StorageFull,
            "kind: InvalidData quoted in the message",
        )),
    ] {
        assert_eq!(sevenz(&error), TERMINAL, "{error}");
    }
    assert_eq!(sevenz(&block(Error::PasswordRequired)), PASSWORD);
}

// Verified bytes that fail to decode under a key indict the key, inside a
// block as outside one.
#[test]
fn a_maybe_bad_password_in_a_block_asks_for_another() {
    let error = io::Error::new(io::ErrorKind::InvalidData, "bad padding");
    assert_eq!(sevenz(&block(Error::MaybeBadPassword(error))), PASSWORD);
}

// Verified bytes that fail to decode under a key indict the key, whatever
// read failure the reader reports.
#[test]
fn a_maybe_bad_password_asks_for_another() {
    let short = Cursor::new([0u8; 3]).read_exact(&mut [0u8; 8]).unwrap_err();
    assert_eq!(sevenz(&Error::MaybeBadPassword(short)), PASSWORD);
    let other = io::Error::new(io::ErrorKind::InvalidData, "bad padding");
    assert_eq!(sevenz(&Error::MaybeBadPassword(other)), PASSWORD);
}

#[test]
fn a_bad_read_in_an_encrypted_zip_entry_asks_for_another_password() {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "Invalid checksum");
    assert_eq!(zip_read(&bad(), true), PASSWORD);
    assert_eq!(zip_read(&bad(), false), DAMAGED);
}

// Only failures that name damage are damage, a checksum carried up a read
// chain among them; anything else, an unknown shape or a text, no repair is
// known to change.
#[test]
fn a_7z_failure_that_names_no_damage_is_terminal() {
    for error in [
        Error::NextHeaderCrcMismatch,
        Error::ChecksumVerificationFailed,
        Error::Io(
            io::Error::other(Error::ChecksumVerificationFailed),
            "".into(),
        ),
    ] {
        assert_eq!(sevenz(&error), DAMAGED, "{error}");
    }
    for error in [
        Error::ExternalUnsupported,
        Error::Other("bad property".into()),
    ] {
        assert_eq!(sevenz(&error), TERMINAL, "{error}");
    }
}

#[test]
fn an_invalid_zip_is_not_an_unsupported_posted_file() {
    assert_eq!(
        super::zip(&ZipError::InvalidArchive("Invalid zip header".into())),
        Emit::Refuse(Why::InvalidArchive)
    );
    assert_eq!(
        super::zip(&ZipError::InvalidArchive(
            "CRC32 checksum failed on Unicode extra field".into()
        )),
        DAMAGED
    );
    assert_eq!(super::zip(&ZipError::InvalidPassword), PASSWORD);
    assert_eq!(
        super::zip(&ZipError::UnsupportedArchive("unsupported compression")),
        TERMINAL
    );
}
