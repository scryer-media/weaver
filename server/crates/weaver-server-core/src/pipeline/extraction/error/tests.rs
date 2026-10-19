// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::extraction::ExtractionLimits;
use std::sync::Arc;

fn test_budget() -> (tempfile::TempDir, Arc<JobExtractionBudget>) {
    let dir = tempfile::TempDir::new().unwrap();
    let limits = Arc::new(ExtractionLimits {
        max_job_bytes: 1 << 40,
        max_member_bytes: 1 << 40,
        max_entries: 1_000,
        max_ratio: 100,
        ratio_floor_bytes: 1 << 30,
        max_seconds: 3_600,
        min_free_bytes: 1,
        max_memory_bytes: 1 << 30,
    });
    let budget = JobExtractionBudget::new(
        limits,
        dir.path().to_path_buf(),
        1 << 30,
        0,
        0,
        crate::operations::metrics::PipelineMetrics::new(),
    )
    .unwrap();
    (dir, budget)
}

#[test]
fn an_io_error_is_classed_by_the_typed_error_it_carries() {
    let rar = io::Error::other(unrar_rs::RarError::DataCrcMismatch {
        member: "member.bin".into(),
        expected: 1,
        actual: 2,
    });
    assert_eq!(io_error_class(&rar), ExtractionErrorClass::Recoverable);

    let unavailable = io::Error::other(unrar_rs::VolumeProviderError::Unavailable {
        volume: 2,
        reason: "pending".into(),
    });
    assert_eq!(
        io_error_class(&unavailable),
        ExtractionErrorClass::StaleTopology
    );

    let sevenz = io::Error::other(sevenz_turbo::Error::PasswordRequired);
    assert_eq!(
        io_error_class(&sevenz),
        ExtractionErrorClass::PasswordRequired
    );

    let zip = io::Error::from(zip::result::ZipError::InvalidPassword);
    assert_eq!(io_error_class(&zip), ExtractionErrorClass::PasswordRequired);

    let nested = io::Error::other(ExtractionError::structure_refused("names collide"));
    assert_eq!(
        io_error_class(&nested),
        ExtractionErrorClass::StructureRefused
    );
}

#[test]
fn descriptor_exhaustion_is_capacity_pressure_whatever_its_wording() {
    #[cfg(unix)]
    let codes = [libc::EMFILE, libc::ENFILE];
    // Native ERROR_TOO_MANY_OPEN_FILES and the supported CRT fallback.
    #[cfg(not(unix))]
    let codes = [4, 24];
    for code in codes {
        assert_eq!(
            io_error_class(&io::Error::from_raw_os_error(code)),
            ExtractionErrorClass::CapacityPressure
        );
    }
    #[cfg(unix)]
    let terminal = [libc::EIO];
    // ERROR_INVALID_DATA and ERROR_CRC, not CRT ENFILE despite its value.
    #[cfg(not(unix))]
    let terminal = [13, 23];
    for code in terminal {
        assert_eq!(
            io_error_class(&io::Error::from_raw_os_error(code)),
            ExtractionErrorClass::Terminal
        );
    }
}

#[test]
fn rar_errors_keep_their_routes() {
    use unrar_rs::RarError;
    let recoverable = [
        RarError::InvalidSignature,
        RarError::TruncatedHeader { offset: 1 },
        RarError::HeaderCrcMismatch {
            expected: 1,
            actual: 2,
        },
        RarError::PackedDataCrcMismatch {
            member: "member.bin".into(),
            volume: 0,
            expected: 1,
            actual: 2,
        },
    ];
    for error in &recoverable {
        assert_eq!(
            rar_error_class(error),
            ExtractionErrorClass::Recoverable,
            "{error:?}"
        );
    }
    let terminal = [
        RarError::Blake2Mismatch {
            member: "member.bin".into(),
        },
        RarError::TruncatedData { offset: 1 },
        RarError::UnsupportedFilter { filter_type: 9 },
        RarError::resource_limit(
            unrar_rs::LimitKind::UnpackedSize,
            5,
            4,
            Some("member.bin".into()),
        ),
        RarError::AllocationFailed { bytes: 1 << 30 },
        RarError::Internal {
            detail: "a worker stopped".into(),
        },
    ];
    for error in &terminal {
        assert_eq!(
            rar_error_class(error),
            ExtractionErrorClass::Terminal,
            "{error:?}"
        );
    }
    assert_eq!(
        rar_error_class(&RarError::WrongPassword {
            member: "member.bin".into()
        }),
        ExtractionErrorClass::PasswordRequired
    );
    // A limit carried through a read is still a limit.
    let limit = RarError::resource_limit(unrar_rs::LimitKind::DataSegment, 9, 4, None);
    assert_eq!(
        rar_error_class(&RarError::from(io::Error::other(limit))),
        ExtractionErrorClass::Terminal
    );
    let refusal = io::Error::other(unrar_rs::VolumeProviderError::Io {
        volume: 2,
        error: io::Error::from_raw_os_error(libc::EMFILE),
    });
    assert_eq!(
        rar_error_class(&RarError::Io(refusal)),
        ExtractionErrorClass::CapacityPressure
    );
}

// Member index and data offset of the first compressed file in a RAR5
// archive.
fn rar5_first_compressed_file_data(archive: &[u8]) -> (usize, usize) {
    fn vint(bytes: &[u8], at: &mut usize) -> u64 {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let byte = bytes[*at];
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return value;
            }
            shift += 7;
        }
    }
    let mut at = 8;
    let mut index = 0;
    loop {
        let mut cursor = at + 4;
        let header_size = vint(archive, &mut cursor) as usize;
        let body = cursor;
        let header_type = vint(archive, &mut cursor);
        let flags = vint(archive, &mut cursor);
        if flags & 0x01 != 0 {
            vint(archive, &mut cursor);
        }
        let data_size = if flags & 0x02 != 0 {
            vint(archive, &mut cursor) as usize
        } else {
            0
        };
        let data = body + header_size;
        if header_type == 2 && data_size > 0 {
            let file_flags = vint(archive, &mut cursor);
            vint(archive, &mut cursor);
            vint(archive, &mut cursor);
            if file_flags & 0x02 != 0 {
                cursor += 4;
            }
            if file_flags & 0x04 != 0 {
                cursor += 4;
            }
            let method = (vint(archive, &mut cursor) >> 7) & 0x07;
            if method != 0 {
                return (index, data);
            }
            index += 1;
        }
        at = data + data_size;
    }
}

#[test]
fn a_rar5_block_header_that_fails_its_checksum_is_recoverable() {
    let mut archive = std::fs::read(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rar5/rar5_multifile_lz.rar"),
    )
    .unwrap();
    let (index, data) = rar5_first_compressed_file_data(&archive);
    // A compressed block opens with its flags and then its header checksum.
    archive[data + 1] ^= 0x01;
    let mut rar = unrar_rs::RarArchive::open(std::io::Cursor::new(archive)).unwrap();
    let error = rar
        .by_index(index)
        .and_then(|entry| entry.copy_to(&mut std::io::sink()))
        .expect_err("the block header fails its checksum");
    assert!(
        matches!(
            error,
            unrar_rs::RarError::BlockHeaderChecksumMismatch { .. }
        ),
        "{error:?}"
    );
    assert_eq!(rar_error_class(&error), ExtractionErrorClass::Recoverable);
}

#[test]
fn sevenz_errors_are_classed_by_the_error_a_block_locates() {
    use sevenz_turbo::Error;
    let block = |cause: Error| cause.in_block(0, 32);
    let io = |error: io::Error| Error::Io(error, "".into());
    let recoverable = [
        Error::ChecksumVerificationFailed,
        Error::NextHeaderCrcMismatch,
        Error::Corrupted("a damaged header".into()),
        block(Error::ChecksumVerificationFailed),
        block(Error::Corrupted("an invalid LZMA lc/lp/pb byte".into())),
        block(io(io::ErrorKind::UnexpectedEof.into())),
        block(io(io::Error::new(
            io::ErrorKind::InvalidData,
            "a damaged stream",
        ))),
        block(io(io::Error::other(Error::ChecksumVerificationFailed))),
        block(Error::MaybeBadPassword(io::Error::other(
            Error::ChecksumVerificationFailed,
        ))),
    ];
    for error in &recoverable {
        assert_eq!(
            sevenz_error_class(error),
            ExtractionErrorClass::Recoverable,
            "{error:?}"
        );
    }
    // None of these says anything about the archive's bytes.
    let terminal = [
        block(Error::Cancelled),
        block(io(io::Error::other(Error::Cancelled))),
        block(Error::Internal("a worker stopped".into())),
        block(Error::MemoryLimited {
            limit_bytes: 1,
            required_bytes: 2,
        }),
        block(Error::UnsupportedCompressionMethod("BCJ2".into())),
        block(Error::Other("the caller's own failure".into())),
        block(io(io::Error::from_raw_os_error(libc::EIO))),
        block(io(io::ErrorKind::PermissionDenied.into())),
        Error::MaybeBadPassword(io::Error::new(io::ErrorKind::InvalidData, "bad key")),
    ];
    for error in &terminal {
        assert_eq!(
            sevenz_error_class(error),
            ExtractionErrorClass::Terminal,
            "{error:?}"
        );
    }
    for error in [Error::PasswordRequired, block(Error::PasswordRequired)] {
        assert_eq!(
            sevenz_error_class(&error),
            ExtractionErrorClass::PasswordRequired
        );
    }
    for error in [
        io(io::Error::from_raw_os_error(libc::EMFILE)),
        block(io(io::Error::from_raw_os_error(libc::EMFILE))),
    ] {
        assert_eq!(
            sevenz_error_class(&error),
            ExtractionErrorClass::CapacityPressure
        );
    }
}

#[test]
fn zip_errors_keep_their_routes() {
    use zip::result::ZipError;
    assert_eq!(
        zip_error_class(&ZipError::InvalidPassword),
        ExtractionErrorClass::PasswordRequired
    );
    assert_eq!(
        zip_error_class(&ZipError::UnsupportedArchive("multi-disk")),
        ExtractionErrorClass::Terminal
    );
}

#[test]
fn a_worker_error_under_a_stopped_budget_is_the_budgets_rejection() {
    let (_dir, budget) = test_budget();
    let error = ExtractionError::recoverable("a read failed");
    assert!(
        error
            .clone()
            .settle(&budget)
            .is(ExtractionErrorClass::Recoverable)
    );

    let failure = budget.reject_unsafe_path("escapes the root");
    let settled = error.settle(&budget);
    assert!(settled.is(ExtractionErrorClass::Rejected));
    assert_eq!(
        settled.rejection().map(|r| r.reason()),
        Some(failure.reason())
    );
}

#[test]
fn a_content_policy_rejection_keeps_its_own_sentence() {
    let (_dir, budget) = test_budget();
    let failure = budget.reject_content_policy("an unwanted extension");
    let error = ExtractionError::from(failure.clone());
    assert!(error.is(ExtractionErrorClass::Rejected));
    assert_eq!(error.job_failure_reason(), failure.job_failure_reason());
}

#[test]
fn a_password_failure_reason_opens_with_the_marker_integrations_read() {
    let error = ExtractionError::password_required("no supplied password opened the archive");
    assert_eq!(
        error.job_failure_reason(),
        "WEAVER_PASSWORD_REQUIRED: no supplied password opened the archive"
    );
    assert_eq!(error.message(), "no supplied password opened the archive");
    assert_eq!(
        ExtractionError::terminal("a read failed").job_failure_reason(),
        "a read failed"
    );
}
