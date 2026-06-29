use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

use crate::StateError;
use crate::jobs::assembly::{DetectedArchiveIdentity, DetectedArchiveKind};
use crate::jobs::{ActiveFileIdentity, FileIdentitySource};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::{
    ActiveFileProgress, ActiveJob, ActivePar2File, Database, ExtractionChunk, HistoryFilter,
    HistoryMetadataEquals, JobHistoryRow, JobId,
};

fn fetch_i64(db: &Database, sql: impl Into<String>, args: Vec<SqlArg>) -> i64 {
    let datastore = db.datastore();
    let sql = sql.into();
    db.run_sql_blocking(async move {
        let row = SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args)
            .await?
            .ok_or_else(|| StateError::Database(format!("query returned no rows: {sql}")))?;
        row.i64_at(0)
    })
    .unwrap()
}

fn fetch_i64_pair(db: &Database, sql: impl Into<String>, args: Vec<SqlArg>) -> (i64, i64) {
    let datastore = db.datastore();
    let sql = sql.into();
    db.run_sql_blocking(async move {
        let row = SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args)
            .await?
            .ok_or_else(|| StateError::Database(format!("query returned no rows: {sql}")))?;
        Ok((row.i64_at(0)?, row.i64_at(1)?))
    })
    .unwrap()
}

fn fetch_text(db: &Database, sql: impl Into<String>, args: Vec<SqlArg>, column: &str) -> String {
    let datastore = db.datastore();
    let sql = sql.into();
    let column = column.to_string();
    db.run_sql_blocking(async move {
        let row = SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args)
            .await?
            .ok_or_else(|| StateError::Database(format!("query returned no rows: {sql}")))?;
        row.text(&column)
    })
    .unwrap()
}

fn execute_sql(db: &Database, sql: impl Into<String>, args: Vec<SqlArg>) {
    let datastore = db.datastore();
    let sql = sql.into();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.read_exec(), &sql, &args).await?;
        Ok(())
    })
    .unwrap()
}

fn sample_nzb_zstd() -> Vec<u8> {
    crate::ingest::compress_nzb_bytes(
        br#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="sample">
            <groups><group>alt.binaries.test</group></groups>
            <segments>
              <segment bytes="10" number="1">abc@test</segment>
            </segments>
          </file>
        </nzb>"#,
    )
    .unwrap()
}

fn sample_job(id: u64) -> ActiveJob {
    ActiveJob {
        job_id: JobId(id),
        nzb_hash: [0xAA; 32],
        nzb_path: PathBuf::from(format!("/tmp/test_{id}.nzb")),
        nzb_zstd: sample_nzb_zstd(),
        output_dir: PathBuf::from(format!("/tmp/output_{id}")),
        created_at: 1700000000 + id,
        category: None,
        metadata: vec![],
    }
}

#[test]
fn create_and_load_active_job() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    let job = &jobs[&JobId(1)];
    assert_eq!(job.nzb_path, PathBuf::from("/tmp/test_1.nzb"));
    assert_eq!(job.status, "queued");
    assert!(job.complete_files.is_empty());
}

#[test]
fn create_active_job_with_file_identities_roundtrips_initial_state() {
    let db = Database::open_in_memory().unwrap();
    let identities = vec![
        ActiveFileIdentity {
            file_index: 0,
            source_filename: "archive.rar".to_string(),
            current_filename: "archive.rar".to_string(),
            canonical_filename: None,
            classification: Some(DetectedArchiveIdentity {
                kind: DetectedArchiveKind::Rar,
                set_name: "archive".to_string(),
                volume_index: Some(0),
            }),
            classification_source: FileIdentitySource::Declared,
        },
        ActiveFileIdentity {
            file_index: 1,
            source_filename: "archive.r00".to_string(),
            current_filename: "archive.r00".to_string(),
            canonical_filename: Some("archive.part02.rar".to_string()),
            classification: Some(DetectedArchiveIdentity {
                kind: DetectedArchiveKind::Rar,
                set_name: "archive".to_string(),
                volume_index: Some(1),
            }),
            classification_source: FileIdentitySource::Probe,
        },
    ];

    db.create_active_job_with_file_identities(&sample_job(1), &identities)
        .unwrap();
    db.complete_file(JobId(1), 0, "archive.rar", &[0x10; 16])
        .unwrap();
    db.complete_file(JobId(1), 1, "archive.r00", &[0x11; 16])
        .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    let job = &jobs[&JobId(1)];
    assert_eq!(job.status, "queued");
    assert_eq!(job.file_identities.len(), 2);
    assert_eq!(job.file_identities.get(&0), Some(&identities[0]));
    assert_eq!(job.file_identities.get(&1), Some(&identities[1]));
}

#[test]
fn upsert_and_load_file_progress() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.upsert_file_progress_batch(&[
        ActiveFileProgress {
            job_id: JobId(1),
            file_index: 0,
            contiguous_bytes_written: 8 * 1024 * 1024,
        },
        ActiveFileProgress {
            job_id: JobId(1),
            file_index: 1,
            contiguous_bytes_written: 1024,
        },
    ])
    .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(
        jobs[&JobId(1)].file_progress.get(&0).copied(),
        Some(8 * 1024 * 1024)
    );
    assert_eq!(jobs[&JobId(1)].file_progress.get(&1).copied(), Some(1024));
}

#[test]
fn bulk_file_progress_cross_sqlite_bind_chunk_boundary_and_keeps_max() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    let low_progress = (0..325)
        .map(|file_index| ActiveFileProgress {
            job_id: JobId(1),
            file_index,
            contiguous_bytes_written: 1024,
        })
        .collect::<Vec<_>>();
    let high_progress = (0..325)
        .map(|file_index| ActiveFileProgress {
            job_id: JobId(1),
            file_index,
            contiguous_bytes_written: 2048 + u64::from(file_index),
        })
        .collect::<Vec<_>>();

    db.upsert_file_progress_batch(&high_progress).unwrap();
    db.upsert_file_progress_batch(&low_progress).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    let progress = &jobs[&JobId(1)].file_progress;
    assert_eq!(progress.len(), 325);
    assert_eq!(progress.get(&0).copied(), Some(2048));
    assert_eq!(progress.get(&324).copied(), Some(2048 + 324));
}

#[test]
fn bulk_file_progress_same_batch_duplicates_keep_max() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    db.upsert_file_progress_batch(&[
        ActiveFileProgress {
            job_id: JobId(1),
            file_index: 0,
            contiguous_bytes_written: 2048,
        },
        ActiveFileProgress {
            job_id: JobId(1),
            file_index: 0,
            contiguous_bytes_written: 1024,
        },
    ])
    .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs[&JobId(1)].file_progress.get(&0).copied(), Some(2048));
}

#[test]
#[ignore = "performance guard; run explicitly when comparing DB write throughput"]
fn perf_upsert_10k_file_progress_rows() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    let progress = (0..10_000)
        .map(|file_index| ActiveFileProgress {
            job_id: JobId(1),
            file_index,
            contiguous_bytes_written: 4096 + u64::from(file_index),
        })
        .collect::<Vec<_>>();

    let started = Instant::now();
    db.upsert_file_progress_batch(&progress).unwrap();
    let elapsed = started.elapsed();

    eprintln!(
        "upsert_file_progress_batch: {} rows in {:?} ({:.0} rows/sec)",
        progress.len(),
        elapsed,
        progress.len() as f64 / elapsed.as_secs_f64()
    );
}

#[test]
fn complete_file_and_load() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.upsert_file_progress_batch(&[ActiveFileProgress {
        job_id: JobId(1),
        file_index: 0,
        contiguous_bytes_written: 4096,
    }])
    .unwrap();
    db.complete_file(JobId(1), 0, "data.rar", &[0x11; 16])
        .unwrap();
    db.complete_file(JobId(1), 1, "data.r00", &[0x22; 16])
        .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs[&JobId(1)].complete_files.len(), 2);
    assert_eq!(jobs[&JobId(1)].file_progress.get(&0), None);

    let hashes = db.load_complete_file_hashes(JobId(1)).unwrap();
    assert_eq!(hashes[&0], [0x11; 16]);
    assert_eq!(hashes[&1], [0x22; 16]);
}

#[test]
fn set_status() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.set_active_job_status(JobId(1), "verifying", None)
        .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs[&JobId(1)].status, "verifying");
}

#[test]
fn set_runtime_state_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.set_active_job_runtime(
        JobId(1),
        "paused",
        None,
        None,
        None,
        None,
        Some(12_345.0),
        Some(67_890.0),
        Some("queued_extract"),
        None,
        None,
    )
    .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    let recovered = &jobs[&JobId(1)];
    assert_eq!(recovered.status, "paused");
    assert_eq!(recovered.queued_repair_at_epoch_ms, Some(12_345.0));
    assert_eq!(recovered.queued_extract_at_epoch_ms, Some(67_890.0));
    assert_eq!(
        recovered.paused_resume_status.as_deref(),
        Some("queued_extract")
    );
}

#[test]
fn detected_archive_identities_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.complete_file(JobId(1), 4, "split.003", &[0x44; 16])
        .unwrap();
    db.save_detected_archive_identity(
        JobId(1),
        4,
        &DetectedArchiveIdentity {
            kind: DetectedArchiveKind::SevenZipSplit,
            set_name: "51273aad56a8b904e96928935278a627".to_string(),
            volume_index: Some(2),
        },
    )
    .unwrap();
    db.save_detected_archive_identity(
        JobId(1),
        5,
        &DetectedArchiveIdentity {
            kind: DetectedArchiveKind::Rar,
            set_name: "incomplete".to_string(),
            volume_index: Some(0),
        },
    )
    .unwrap();

    let loaded = db.load_detected_archive_identities(JobId(1)).unwrap();
    assert_eq!(
        loaded.get(&4),
        Some(&DetectedArchiveIdentity {
            kind: DetectedArchiveKind::SevenZipSplit,
            set_name: "51273aad56a8b904e96928935278a627".to_string(),
            volume_index: Some(2),
        })
    );

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(
        jobs[&JobId(1)].detected_archives,
        HashMap::from([(
            4,
            DetectedArchiveIdentity {
                kind: DetectedArchiveKind::SevenZipSplit,
                set_name: "51273aad56a8b904e96928935278a627".to_string(),
                volume_index: Some(2),
            }
        )])
    );
}

#[test]
fn archive_job_moves_to_history() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    let history = JobHistoryRow {
        job_id: 1,
        job_hash: None,
        name: "test.nzb".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 1_000_000,
        downloaded_bytes: 1_000_000,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: Some("/tmp/output_1".to_string()),
        nzb_path: Some("/tmp/test_1.nzb".to_string()),
        created_at: 1700000001,
        completed_at: 1700001000,
        metadata: Some(
            serde_json::to_string(&vec![
                ("archive_key".to_string(), "archive-value".to_string()),
                (
                    crate::history::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                    "req-archive".to_string(),
                ),
            ])
            .unwrap(),
        ),
        last_diagnostic_id: None,
        last_diagnostic_uploaded_at_epoch_ms: None,
    };
    db.archive_job(JobId(1), &history).unwrap();

    // Active tables should be empty.
    let jobs = db.load_active_jobs().unwrap();
    assert!(jobs.is_empty());

    // History should have the entry.
    let hist = db.list_job_history(&HistoryFilter::default()).unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].name, "test.nzb");
    assert_eq!(
        db.count_job_history(&HistoryFilter {
            metadata_equals: Some(HistoryMetadataEquals {
                key: "archive_key".to_string(),
                value: "archive-value".to_string(),
            }),
            ..Default::default()
        })
        .unwrap(),
        1
    );
    assert_eq!(
        db.count_job_history(&HistoryFilter {
            metadata_has_key: Some(crate::history::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string()),
            ..Default::default()
        })
        .unwrap(),
        0
    );
}

#[test]
fn archive_job_conflict_preserves_persisted_nzb_bytes_when_active_row_is_gone() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    let mut history = JobHistoryRow {
        job_id: 1,
        job_hash: None,
        name: "test.nzb".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 1_000_000,
        downloaded_bytes: 1_000_000,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: Some("/tmp/output_1".to_string()),
        nzb_path: Some("/tmp/original.nzb".to_string()),
        created_at: 1_700_000_001,
        completed_at: 1_700_000_010,
        metadata: None,
        last_diagnostic_id: None,
        last_diagnostic_uploaded_at_epoch_ms: None,
    };
    db.archive_job(JobId(1), &history).unwrap();
    let (_, expected_nzb_zstd) = db.load_history_job_persisted_nzb(1).unwrap().unwrap();

    history.status = "failed".to_string();
    history.error_message = Some("late archive rewrite".to_string());
    history.nzb_path = None;
    db.archive_job(JobId(1), &history).unwrap();

    let (path, nzb_zstd) = db.load_history_job_persisted_nzb(1).unwrap().unwrap();
    assert_eq!(path, PathBuf::from("job-1.nzb"));
    assert_eq!(nzb_zstd, expected_nzb_zstd);
    assert!(db.get_job_history(1).unwrap().unwrap().nzb_path.is_none());
}

#[test]
fn delete_active_job_cleans_all() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.upsert_file_progress_batch(&[ActiveFileProgress {
        job_id: JobId(1),
        file_index: 0,
        contiguous_bytes_written: 4096,
    }])
    .unwrap();
    db.complete_file(JobId(1), 0, "f.rar", &[0; 16]).unwrap();

    db.delete_active_job(JobId(1)).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert!(jobs.is_empty());
}

#[test]
fn mark_file_incomplete_clears_progress_rows() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.upsert_file_progress_batch(&[ActiveFileProgress {
        job_id: JobId(1),
        file_index: 0,
        contiguous_bytes_written: 1234,
    }])
    .unwrap();

    db.mark_file_incomplete(JobId(1), 0).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert!(jobs[&JobId(1)].file_progress.is_empty());
}

#[test]
fn max_job_id_all_spans_both_tables() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(5)).unwrap();

    let history = JobHistoryRow {
        job_id: 10,
        job_hash: None,
        name: "old.nzb".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 0,
        completed_at: 0,
        metadata: None,
        last_diagnostic_id: None,
        last_diagnostic_uploaded_at_epoch_ms: None,
    };
    db.insert_job_history(&history).unwrap();

    assert_eq!(db.max_job_id_all().unwrap(), 10);
}

#[test]
fn multiple_jobs_isolated() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.create_active_job(&sample_job(2)).unwrap();

    db.delete_active_job(JobId(1)).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    assert!(jobs.contains_key(&JobId(2)));
}

#[test]
fn metadata_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    let mut job = sample_job(1);
    job.metadata = vec![
        ("title".to_string(), "My Movie".to_string()),
        ("year".to_string(), "2024".to_string()),
    ];
    job.category = Some("movies".to_string());
    db.create_active_job(&job).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    let recovered = &jobs[&JobId(1)];
    assert_eq!(recovered.category, Some("movies".to_string()));
    assert_eq!(recovered.metadata.len(), 2);
    assert_eq!(
        recovered.metadata[0],
        ("title".to_string(), "My Movie".to_string())
    );
}

#[test]
fn par2_metadata_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.set_par2_metadata(JobId(1), 384000, 8).unwrap();
    // Overwrite should work (INSERT OR REPLACE).
    db.set_par2_metadata(JobId(1), 768000, 16).unwrap();

    let (slice, blocks) = fetch_i64_pair(
        &db,
        "SELECT slice_size, recovery_block_count FROM active_par2 WHERE job_id = 1",
        vec![],
    );
    assert_eq!(slice, 768000);
    assert_eq!(blocks, 16);
}

#[test]
fn par2_file_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.upsert_par2_file(JobId(1), 4, "repair.vol00+01.par2", 12, false)
        .unwrap();
    db.set_par2_file_promotion(JobId(1), 4, true).unwrap();

    let files = db.load_par2_files(JobId(1)).unwrap();
    assert_eq!(
        files.get(&4),
        Some(&ActivePar2File {
            file_index: 4,
            filename: "repair.vol00+01.par2".to_string(),
            recovery_block_count: 12,
            promoted: true,
        })
    );
}

#[test]
fn active_file_identity_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.complete_file(JobId(1), 7, "show.part001.rar", &[0x77; 16])
        .unwrap();
    db.save_file_identity(
        JobId(1),
        &ActiveFileIdentity {
            file_index: 7,
            source_filename: "51273aad56a8b904e96928935278a627.101".to_string(),
            current_filename: "show.part001.rar".to_string(),
            canonical_filename: Some("show.part001.rar".to_string()),
            classification: Some(DetectedArchiveIdentity {
                kind: DetectedArchiveKind::Rar,
                set_name: "show".to_string(),
                volume_index: Some(1),
            }),
            classification_source: FileIdentitySource::Par2,
        },
    )
    .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    let recovered = &jobs[&JobId(1)];
    let identity = recovered
        .file_identities
        .get(&7)
        .expect("persisted file identity should reload");
    assert_eq!(
        identity.source_filename,
        "51273aad56a8b904e96928935278a627.101"
    );
    assert_eq!(identity.current_filename, "show.part001.rar");
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some("show.part001.rar")
    );
    assert_eq!(identity.classification_source, FileIdentitySource::Par2);
    assert!(matches!(
        identity
            .classification
            .as_ref()
            .map(|classification| &classification.kind),
        Some(DetectedArchiveKind::Rar)
    ));
    assert_eq!(
        fetch_text(
            &db,
            "SELECT filename FROM active_files WHERE job_id = {} AND file_index = {}",
            vec![SqlArg::I64(1), SqlArg::I64(7)],
            "filename",
        ),
        "show.part001.rar"
    );

    db.save_file_identity(
        JobId(1),
        &ActiveFileIdentity {
            file_index: 7,
            source_filename: "51273aad56a8b904e96928935278a627.101".to_string(),
            current_filename: "show.part01.rar".to_string(),
            canonical_filename: Some("show.part01.rar".to_string()),
            classification: Some(DetectedArchiveIdentity {
                kind: DetectedArchiveKind::Rar,
                set_name: "show".to_string(),
                volume_index: Some(0),
            }),
            classification_source: FileIdentitySource::Par2,
        },
    )
    .unwrap();
    assert_eq!(
        fetch_text(
            &db,
            "SELECT filename FROM active_files WHERE job_id = {} AND file_index = {}",
            vec![SqlArg::I64(1), SqlArg::I64(7)],
            "filename",
        ),
        "show.part01.rar"
    );
}

#[test]
fn restart_runtime_state_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    db.replace_failed_extractions(
        JobId(1),
        &HashSet::from(["E10.mkv".to_string(), "E15.mkv".to_string()]),
    )
    .unwrap();
    db.set_active_job_normalization_retried(JobId(1), true)
        .unwrap();
    db.replace_verified_suspect_volumes(JobId(1), "show", &HashSet::from([37u32, 38u32]))
        .unwrap();

    let failed = db.load_failed_extractions(JobId(1)).unwrap();
    assert_eq!(
        failed,
        HashSet::from(["E10.mkv".to_string(), "E15.mkv".to_string()])
    );
    assert!(db.load_active_job_normalization_retried(JobId(1)).unwrap());
    assert_eq!(
        db.load_verified_suspect_volumes(JobId(1)).unwrap(),
        HashMap::from([("show".to_string(), HashSet::from([37u32, 38u32]))])
    );
}

#[test]
fn extracted_member_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("movie.mkv");
    std::fs::write(&output_path, b"movie").unwrap();
    db.add_extracted_member(JobId(1), "movie.mkv", &output_path)
        .unwrap();

    let count = fetch_i64(
        &db,
        "SELECT COUNT(*) FROM active_extracted WHERE job_id = 1",
        vec![],
    );
    assert_eq!(count, 1);

    let jobs = db.load_active_jobs().unwrap();
    assert!(jobs[&JobId(1)].extracted_members.contains("movie.mkv"));

    std::fs::write(&output_path, b"movie-with-extra-bytes").unwrap();
    let jobs = db.load_active_jobs().unwrap();
    assert!(!jobs[&JobId(1)].extracted_members.contains("movie.mkv"));

    std::fs::remove_file(&output_path).unwrap();
    let jobs = db.load_active_jobs().unwrap();
    assert!(!jobs[&JobId(1)].extracted_members.contains("movie.mkv"));
}

#[test]
fn extraction_chunk_replace_append_and_clear_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    db.replace_member_chunks(
        JobId(1),
        "set",
        "movie.mkv",
        &[
            ExtractionChunk {
                member_name: "movie.mkv".into(),
                volume_index: 0,
                bytes_written: 111,
                temp_path: "/tmp/chunk0".into(),
                start_offset: 0,
                end_offset: 111,
                verified: true,
                appended: false,
            },
            ExtractionChunk {
                member_name: "movie.mkv".into(),
                volume_index: 1,
                bytes_written: 222,
                temp_path: "/tmp/chunk1".into(),
                start_offset: 111,
                end_offset: 333,
                verified: true,
                appended: false,
            },
        ],
    )
    .unwrap();

    db.mark_chunk_appended(JobId(1), "set", "movie.mkv", 0)
        .unwrap();

    let chunks = db.get_extraction_chunks(JobId(1), "set").unwrap();
    assert_eq!(chunks.len(), 2);
    assert!(chunks.iter().any(|c| c.volume_index == 0 && c.appended));
    assert!(
        chunks
            .iter()
            .any(|c| c.volume_index == 1 && c.verified && !c.appended)
    );

    db.clear_member_chunks(JobId(1), "set", "movie.mkv")
        .unwrap();
    assert!(
        db.get_extraction_chunks(JobId(1), "set")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn extraction_chunk_replace_only_overwrites_target_member() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    db.replace_member_chunks(
        JobId(1),
        "set",
        "movie_a.mkv",
        &[ExtractionChunk {
            member_name: "movie_a.mkv".into(),
            volume_index: 0,
            bytes_written: 100,
            temp_path: "/tmp/a0".into(),
            start_offset: 0,
            end_offset: 100,
            verified: true,
            appended: false,
        }],
    )
    .unwrap();
    db.replace_member_chunks(
        JobId(1),
        "set",
        "movie_b.mkv",
        &[ExtractionChunk {
            member_name: "movie_b.mkv".into(),
            volume_index: 0,
            bytes_written: 200,
            temp_path: "/tmp/b0".into(),
            start_offset: 0,
            end_offset: 200,
            verified: true,
            appended: false,
        }],
    )
    .unwrap();

    db.replace_member_chunks(
        JobId(1),
        "set",
        "movie_a.mkv",
        &[ExtractionChunk {
            member_name: "movie_a.mkv".into(),
            volume_index: 1,
            bytes_written: 300,
            temp_path: "/tmp/a1".into(),
            start_offset: 100,
            end_offset: 400,
            verified: true,
            appended: false,
        }],
    )
    .unwrap();

    let chunks = db.get_extraction_chunks(JobId(1), "set").unwrap();
    assert_eq!(chunks.len(), 2);
    assert!(
        chunks
            .iter()
            .any(|c| c.member_name == "movie_a.mkv" && c.volume_index == 1)
    );
    assert!(
        chunks
            .iter()
            .any(|c| c.member_name == "movie_b.mkv" && c.bytes_written == 200)
    );
    assert!(
        !chunks
            .iter()
            .any(|c| c.member_name == "movie_a.mkv" && c.volume_index == 0)
    );
}

#[test]
fn archive_headers_roundtrip_and_delete() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    db.save_archive_headers(JobId(1), "set-a", &[1, 2, 3])
        .unwrap();
    db.save_archive_headers(JobId(1), "set-b", &[4, 5]).unwrap();

    assert_eq!(
        db.load_archive_headers(JobId(1), "set-a").unwrap(),
        Some(vec![1, 2, 3])
    );

    let all = db.load_all_archive_headers(JobId(1)).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all["set-b"], vec![4, 5]);

    db.delete_archive_headers(JobId(1), "set-a").unwrap();
    assert_eq!(db.load_archive_headers(JobId(1), "set-a").unwrap(), None);
    assert_eq!(db.load_all_archive_headers(JobId(1)).unwrap().len(), 1);
}

#[test]
fn deleted_volume_statuses_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    db.set_volume_status(JobId(1), "set-a", 0, true, true, true)
        .unwrap();
    db.set_volume_status(JobId(1), "set-a", 1, true, true, false)
        .unwrap();
    db.set_volume_status(JobId(1), "set-b", 4, true, true, true)
        .unwrap();

    assert_eq!(
        db.load_deleted_volume_statuses(JobId(1)).unwrap(),
        vec![("set-a".to_string(), 0), ("set-b".to_string(), 4)]
    );
}

#[test]
fn archive_job_uses_requested_job_id_for_history_row() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(7)).unwrap();

    db.archive_job(
        JobId(7),
        &JobHistoryRow {
            job_id: 999,
            job_hash: None,
            name: "mismatch.nzb".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 10,
            downloaded_bytes: 10,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: Some("/tmp/output_7".to_string()),
            nzb_path: Some("/tmp/test_7.nzb".to_string()),
            created_at: 1_700_000_001,
            completed_at: 1_700_000_010,
            metadata: None,
            last_diagnostic_id: None,
            last_diagnostic_uploaded_at_epoch_ms: None,
        },
    )
    .unwrap();

    let archived = db.get_job_history(7).unwrap().unwrap();
    assert_eq!(archived.job_id, 7);
    assert_eq!(archived.name, "mismatch.nzb");
    assert!(db.get_job_history(999).unwrap().is_none());
    assert!(db.load_active_jobs().unwrap().is_empty());
}

#[test]
fn late_active_state_writes_noop_after_archive() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.archive_job(
        JobId(1),
        &JobHistoryRow {
            job_id: 1,
            job_hash: None,
            name: "test.nzb".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 10,
            downloaded_bytes: 10,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: Some("/tmp/output_1".to_string()),
            nzb_path: Some("/tmp/test_1.nzb".to_string()),
            created_at: 1_700_000_001,
            completed_at: 1_700_000_010,
            metadata: None,
            last_diagnostic_id: None,
            last_diagnostic_uploaded_at_epoch_ms: None,
        },
    )
    .unwrap();

    db.upsert_file_progress_batch(&[ActiveFileProgress {
        job_id: JobId(1),
        file_index: 0,
        contiguous_bytes_written: 4096,
    }])
    .unwrap();
    db.complete_file(JobId(1), 0, "late.rar", &[0x11; 16])
        .unwrap();
    db.set_par2_metadata(JobId(1), 384000, 8).unwrap();
    db.upsert_par2_file(JobId(1), 4, "repair.par2", 12, false)
        .unwrap();
    db.replace_failed_extractions(JobId(1), &HashSet::from(["bad.mkv".to_string()]))
        .unwrap();
    db.add_failed_extraction(JobId(1), "another.mkv").unwrap();
    db.replace_verified_suspect_volumes(JobId(1), "show", &HashSet::from([1u32, 2u32]))
        .unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("movie.mkv");
    std::fs::write(&output_path, b"movie").unwrap();
    db.add_extracted_member(JobId(1), "movie.mkv", &output_path)
        .unwrap();
    db.insert_extraction_chunk(&crate::jobs::record::ActiveExtractionChunk {
        job_id: JobId(1),
        set_name: "set".into(),
        member_name: "movie.mkv".into(),
        volume_index: 0,
        bytes_written: 123,
        temp_path: "/tmp/chunk0".into(),
        start_offset: 0,
        end_offset: 123,
    })
    .unwrap();
    db.replace_member_chunks(
        JobId(1),
        "set",
        "movie.mkv",
        &[ExtractionChunk {
            member_name: "movie.mkv".into(),
            volume_index: 0,
            bytes_written: 123,
            temp_path: "/tmp/chunk0".into(),
            start_offset: 0,
            end_offset: 123,
            verified: true,
            appended: false,
        }],
    )
    .unwrap();
    db.save_archive_headers(JobId(1), "set", &[1, 2, 3])
        .unwrap();
    db.save_rar_volume_facts(JobId(1), "set", 0, &[4, 5, 6])
        .unwrap();
    db.set_volume_status(JobId(1), "set", 0, true, true, true)
        .unwrap();

    for table in [
        "active_file_progress",
        "active_files",
        "active_par2",
        "active_par2_files",
        "active_failed_extractions",
        "active_rar_verified_suspect",
        "active_extracted",
        "active_extraction_chunks",
        "active_archive_headers",
        "active_rar_volume_facts",
        "active_detected_archives",
        "active_volume_status",
    ] {
        let count = fetch_i64(&db, format!("SELECT COUNT(*) FROM {table}"), vec![]);
        assert_eq!(count, 0, "{table} should remain empty after late writes");
    }
}

#[test]
fn prune_orphan_active_state_removes_only_orphans() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.upsert_file_progress_batch(&[ActiveFileProgress {
        job_id: JobId(1),
        file_index: 0,
        contiguous_bytes_written: 2048,
    }])
    .unwrap();

    execute_sql(
        &db,
        "INSERT INTO active_file_progress
         (job_id, file_index, contiguous_bytes_written)
         VALUES (100, 0, 1024)",
        vec![],
    );
    execute_sql(
        &db,
        "INSERT INTO active_archive_headers (job_id, set_name, headers)
         VALUES (101, 'set', x'0102')",
        vec![],
    );
    execute_sql(
        &db,
        "INSERT INTO active_detected_archives
         (job_id, file_index, kind, set_name, volume_index)
         VALUES (102, 0, 'rar', 'set', 0)",
        vec![],
    );

    let counts = db.prune_orphan_active_state().unwrap();
    assert_eq!(counts.active_file_progress, 1);
    assert_eq!(counts.active_archive_headers, 1);
    assert_eq!(counts.active_detected_archives, 1);
    assert_eq!(counts.total_removed(), 3);

    let remaining_progress = fetch_i64(
        &db,
        "SELECT COUNT(*) FROM active_file_progress WHERE job_id = 1",
        vec![],
    );
    assert_eq!(remaining_progress, 1);
}
