use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::jobs::assembly::{DetectedArchiveIdentity, DetectedArchiveKind};
use crate::jobs::{ActiveFileIdentity, FileIdentitySource};
use crate::{
    ActiveFileProgress, ActiveJob, ActivePar2File, CommittedSegment, Database, ExtractionChunk,
    HistoryFilter, JobHistoryRow, JobId,
};

fn sample_job(id: u64) -> ActiveJob {
    ActiveJob {
        job_id: JobId(id),
        nzb_hash: [0xAA; 32],
        nzb_path: PathBuf::from(format!("/tmp/test_{id}.nzb")),
        output_dir: PathBuf::from(format!("/tmp/output_{id}")),
        created_at: 1700000000 + id,
        category: None,
        metadata: vec![],
    }
}

fn sample_segments(job_id: u64, count: u32) -> Vec<CommittedSegment> {
    (0..count)
        .map(|i| CommittedSegment {
            job_id: JobId(job_id),
            file_index: 0,
            segment_number: i,
            file_offset: i as u64 * 768000,
            decoded_size: 768000,
            crc32: 0xDEADBEEF,
        })
        .collect()
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
    assert!(job.committed_segments.is_empty());
    assert!(job.complete_files.is_empty());
}

#[test]
fn commit_and_load_segments() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.commit_segments(&sample_segments(1, 50)).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs[&JobId(1)].committed_segments.len(), 50);
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
fn duplicate_segments_ignored() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    let segs = sample_segments(1, 3);
    db.commit_segments(&segs).unwrap();
    db.commit_segments(&segs).unwrap(); // duplicates

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs[&JobId(1)].committed_segments.len(), 3);
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
        Some(12_345.0),
        Some(67_890.0),
        Some("queued_extract"),
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
    assert_eq!(jobs[&JobId(1)].detected_archives, loaded);
}

#[test]
fn archive_job_moves_to_history() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.commit_segments(&sample_segments(1, 10)).unwrap();

    let history = JobHistoryRow {
        job_id: 1,
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
        metadata: None,
    };
    db.archive_job(JobId(1), &history).unwrap();

    // Active tables should be empty.
    let jobs = db.load_active_jobs().unwrap();
    assert!(jobs.is_empty());

    // History should have the entry.
    let hist = db.list_job_history(&HistoryFilter::default()).unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].name, "test.nzb");
}

#[test]
fn delete_active_job_cleans_all() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.commit_segments(&sample_segments(1, 5)).unwrap();
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
    };
    db.insert_job_history(&history).unwrap();

    assert_eq!(db.max_job_id_all().unwrap(), 10);
}

#[test]
fn multiple_jobs_isolated() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.create_active_job(&sample_job(2)).unwrap();
    db.commit_segments(&sample_segments(1, 3)).unwrap();
    db.commit_segments(&sample_segments(2, 5)).unwrap();

    db.delete_active_job(JobId(1)).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[&JobId(2)].committed_segments.len(), 5);
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

    let conn = db.conn();
    let (slice, blocks): (i64, u32) = conn
        .query_row(
            "SELECT slice_size, recovery_block_count FROM active_par2 WHERE job_id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
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
    db.add_extracted_member(JobId(1), "movie.mkv", Path::new("/tmp/output_1/movie.mkv"))
        .unwrap();

    let conn = db.conn();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM active_extracted WHERE job_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
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
fn late_active_state_writes_noop_after_archive() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.archive_job(
        JobId(1),
        &JobHistoryRow {
            job_id: 1,
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
        },
    )
    .unwrap();

    db.commit_segments(&sample_segments(1, 3)).unwrap();
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
    db.add_extracted_member(JobId(1), "movie.mkv", Path::new("/tmp/output_1/movie.mkv"))
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

    let conn = db.conn();
    for table in [
        "active_segments",
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
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table} should remain empty after late writes");
    }
}

#[test]
fn prune_orphan_active_state_removes_only_orphans() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.commit_segments(&sample_segments(1, 1)).unwrap();
    db.upsert_file_progress_batch(&[ActiveFileProgress {
        job_id: JobId(1),
        file_index: 0,
        contiguous_bytes_written: 2048,
    }])
    .unwrap();

    {
        let conn = db.conn();
        conn.execute(
            "INSERT INTO active_segments
             (job_id, file_index, segment_number, file_offset, decoded_size, crc32)
             VALUES (99, 0, 0, 0, 10, 123)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO active_file_progress
             (job_id, file_index, contiguous_bytes_written)
             VALUES (100, 0, 1024)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO active_archive_headers (job_id, set_name, headers)
             VALUES (101, 'set', x'0102')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO active_detected_archives
             (job_id, file_index, kind, set_name, volume_index)
             VALUES (102, 0, 'rar', 'set', 0)",
            [],
        )
        .unwrap();
    }

    let counts = db.prune_orphan_active_state().unwrap();
    assert_eq!(counts.active_segments, 1);
    assert_eq!(counts.active_file_progress, 1);
    assert_eq!(counts.active_archive_headers, 1);
    assert_eq!(counts.active_detected_archives, 1);
    assert_eq!(counts.total_removed(), 4);

    let conn = db.conn();
    let remaining_segments: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM active_segments WHERE job_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let remaining_progress: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM active_file_progress WHERE job_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining_segments, 1);
    assert_eq!(remaining_progress, 1);
}
