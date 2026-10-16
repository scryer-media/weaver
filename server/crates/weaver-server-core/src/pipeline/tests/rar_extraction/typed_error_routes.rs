// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A full-set extraction failure is routed on the class its engine's error
// mapped to: damaged bytes wait for recovery, a structure no repair changes
// and a password no candidate opened end the job. Each error here comes
// from the engine's own error value, never from a message.

use super::*;
use crate::pipeline::{ExtractionError, ExtractionErrorClass};
use std::io::{Read, Write};

fn gzip_decode_error(bytes: &[u8]) -> std::io::Error {
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut sink = Vec::new();
    decoder
        .read_to_end(&mut sink)
        .expect_err("the stream must not decode")
}

fn gzip_with_bad_trailer() -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(b"payload bytes for a gzip member")
        .unwrap();
    let mut bytes = encoder.finish().unwrap();
    let crc = bytes.len() - 8;
    bytes[crc] ^= 0xff;
    bytes
}

fn sevenz_block(kind: sevenz_turbo::BlockErrorKind, message: &str) -> ExtractionError {
    crate::pipeline::completion::finalize::extract::sevenz_extraction_error(
        &sevenz_turbo::Error::BlockDecode {
            block_index: 0,
            packed_offset: 32,
            kind,
            message: message.into(),
        },
    )
}

// (engine, set name, error, class the engine's error maps to)
fn engine_errors() -> Vec<(
    &'static str,
    &'static str,
    ExtractionError,
    ExtractionErrorClass,
)> {
    use sevenz_turbo::BlockErrorKind;
    use zip::result::ZipError;
    let zip_crc = ZipError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Invalid checksum",
    ));
    vec![
        (
            "rar",
            "archive.rar",
            ExtractionError::from_rar(
                "rar data",
                &unrar_rs::RarError::DataCrcMismatch {
                    member: "sample.mkv".into(),
                    expected: 1,
                    actual: 2,
                },
            ),
            ExtractionErrorClass::Recoverable,
        ),
        (
            "rar",
            "archive.rar",
            ExtractionError::from_rar(
                "rar filter",
                &unrar_rs::RarError::UnsupportedFilter { filter_type: 9 },
            ),
            ExtractionErrorClass::Terminal,
        ),
        (
            "rar",
            "archive.rar",
            ExtractionError::from_rar(
                "rar password",
                &unrar_rs::RarError::WrongPassword {
                    member: "sample.mkv".into(),
                },
            ),
            ExtractionErrorClass::PasswordRequired,
        ),
        (
            "7z",
            "archive.7z",
            sevenz_block(BlockErrorKind::Corrupted, "data error"),
            ExtractionErrorClass::Recoverable,
        ),
        (
            "7z",
            "archive.7z",
            sevenz_block(
                BlockErrorKind::UnsupportedMethod,
                "UnsupportedCompressionMethod(\"ARM64\")",
            ),
            ExtractionErrorClass::Terminal,
        ),
        (
            "7z",
            "archive.7z",
            crate::pipeline::completion::finalize::extract::sevenz_extraction_error(
                &sevenz_turbo::Error::PasswordRequired,
            ),
            ExtractionErrorClass::PasswordRequired,
        ),
        (
            "zip",
            "archive.zip",
            ExtractionError::from_zip("zip member", &zip_crc),
            ExtractionErrorClass::Recoverable,
        ),
        (
            "zip",
            "archive.zip",
            ExtractionError::from_zip("zip archive", &ZipError::UnsupportedArchive("multi-disk")),
            ExtractionErrorClass::Terminal,
        ),
        (
            "zip",
            "archive.zip",
            ExtractionError::from_zip("zip password", &ZipError::InvalidPassword),
            ExtractionErrorClass::PasswordRequired,
        ),
        (
            "gzip",
            "archive.gz",
            ExtractionError::io(&gzip_decode_error(&gzip_with_bad_trailer())),
            ExtractionErrorClass::Recoverable,
        ),
        (
            "gzip",
            "archive.gz",
            ExtractionError::io(&gzip_decode_error(b"not a gzip stream at all")),
            ExtractionErrorClass::Terminal,
        ),
    ]
}

#[test]
fn each_engine_error_maps_to_its_class() {
    for (engine, _, error, class) in engine_errors() {
        assert_eq!(error.class(), class, "{engine}: {error}");
    }
}

#[tokio::test]
async fn full_set_failures_are_routed_on_their_class() {
    for (offset, (engine, set_name, error, class)) in engine_errors().into_iter().enumerate() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30770 + offset as u64);
        let spec = standalone_job_spec("Typed Full Set Route", &[("sample.bin".to_string(), 100)]);
        insert_active_job(&mut pipeline, job_id, spec).await;
        pipeline
            .inflight_extractions
            .entry(job_id)
            .or_default()
            .insert(set_name.to_string());

        pipeline
            .handle_extraction_done(ExtractionDone::FullSet {
                job_id,
                set_name: set_name.to_string(),
                result: Err(error),
            })
            .await;

        let failed = matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        );
        match class {
            ExtractionErrorClass::Recoverable => {
                assert!(
                    !failed,
                    "{engine} {class:?}: damaged bytes wait for recovery"
                );
                assert!(
                    pipeline
                        .failed_extractions
                        .get(&job_id)
                        .is_some_and(|sets| sets.contains(set_name)),
                    "{engine} {class:?}: the set waits for recovery"
                );
            }
            _ => assert!(failed, "{engine} {class:?}: the job ends"),
        }
    }
}
