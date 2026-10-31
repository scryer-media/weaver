// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::jobs::persistence::CompletedHashProvenance;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::StateError;
use crate::jobs::assembly::{DetectedArchiveIdentity, DetectedArchiveKind};
use crate::jobs::{ActiveFileIdentity, FileIdentitySource};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::{ActiveJob, Database, HistoryFilter, HistoryMetadataEquals, JobHistoryRow, JobId};

fn fetch_i64(db: &Database, sql: impl Into<String>, args: Vec<SqlArg>) -> i64 {
    let datastore = db.datastore();
    let sql = sql.into();
    db.run_sql_blocking_read(async move {
        let row = SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args)
            .await?
            .ok_or_else(|| StateError::Database(format!("query returned no rows: {sql}")))?;
        row.i64_at(0)
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
    db.run_sql_blocking_read(async move {
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
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: None,
        supplied_password_candidates: Vec::new(),
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
fn complete_file_and_load() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.complete_file(JobId(1), 0, "data.rar", &[0x11; 16])
        .unwrap();
    db.complete_file(JobId(1), 1, "data.r00", &[0x22; 16])
        .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs[&JobId(1)].complete_files.len(), 2);

    let hashes = db.load_complete_file_hashes_any(JobId(1)).unwrap();
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
fn archive_job_moves_to_history() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    db.save_job_script_effects(
        1,
        &crate::post_processing::effects::JobScriptEffects {
            parameters: [("script_key".into(), "script-value".into())].into(),
            directory: Some("/tmp/script-working".into()),
            final_directory: Some("/tmp/script-final".into()),
            marked_bad: true,
        },
    )
    .unwrap();

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
        server_attribution: None,
    };
    db.archive_job(JobId(1), &history).unwrap();

    // Active tables should be empty.
    let jobs = db.load_active_jobs().unwrap();
    assert!(jobs.is_empty());

    // History should have the entry.
    let hist = db.list_job_history(&HistoryFilter::default()).unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].name, "test.nzb");
    assert_eq!(hist[0].output_dir, history.output_dir);
    assert!(hist[0].metadata.as_ref().unwrap().contains("script-value"));
    assert!(db.job_script_effects(1).unwrap().marked_bad);
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
    // Restart removes the old history bundle before running the new attempt.
    db.delete_job_history(1).unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    let effects = db.job_script_effects(1).unwrap();
    assert!(effects.parameters.is_empty());
    assert!(effects.directory.is_none());
    assert!(effects.final_directory.is_none());
    assert!(!effects.marked_bad);
}

#[test]
fn postprocessing_relocation_replaces_the_earlier_queue_destination() {
    use crate::post_processing::directives::Directive;
    use crate::post_processing::effects::{apply_job_directive, apply_queue_directive};
    use crate::post_processing::model::PipelineOutcome;
    use crate::post_processing::runner::{CompatibilityFacts, JobExecutionContext};

    let temp = tempfile::tempdir().unwrap();
    let complete = temp.path().canonicalize().unwrap();
    let first = complete.join("first");
    let relocated = complete.join("relocated");
    std::fs::create_dir(&first).unwrap();
    let db = Database::open_in_memory().unwrap();
    let mut active = sample_job(1);
    active.output_dir = first.clone();
    db.create_active_job(&active).unwrap();
    let mut context = JobExecutionContext {
        job_id: 1,
        name: "relocation".into(),
        nzb_filename: "relocation.nzb".into(),
        category: None,
        group: None,
        source_url: None,
        working_directory: first.clone(),
        final_directory: first.clone(),
        pipeline_outcome: PipelineOutcome::Succeeded,
        par_status: 0,
        unpack_status: 0,
        compatibility: CompatibilityFacts {
            complete_dir: Some(complete),
            ..Default::default()
        },
    };
    apply_queue_directive(
        &db,
        &mut context,
        Directive::FinalDirectory(first.to_string_lossy().into_owned()),
    )
    .unwrap();
    std::fs::write(first.join("payload.bin"), b"payload").unwrap();
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&first).unwrap();
    std::fs::rename(&first, &relocated).unwrap();
    apply_job_directive(
        &db,
        &mut context,
        Directive::Directory(relocated.to_string_lossy().into_owned()),
    )
    .unwrap();
    apply_job_directive(
        &db,
        &mut context,
        Directive::Parameter {
            name: "later".into(),
            value: "value".into(),
        },
    )
    .unwrap();
    let effects = db.job_script_effects(1).unwrap();
    assert_eq!(effects.directory, Some(relocated.clone()));
    assert!(effects.final_directory.is_none());
    assert_eq!(context.working_directory, relocated);
    assert_eq!(context.final_directory, relocated);
    assert!(context.compatibility.final_directory_override.is_none());
    assert_eq!(
        db.load_active_jobs().unwrap()[&JobId(1)].output_dir,
        relocated
    );
    assert!(crate::jobs::working_dir::is_weaver_owned_output_dir(
        &relocated
    ));
    assert_eq!(
        std::fs::read(relocated.join("payload.bin")).unwrap(),
        b"payload"
    );
    assert!(!first.exists());
}

#[test]
fn script_parameter_budget_refuses_growth_without_partial_persistence() {
    use crate::post_processing::effects::JobScriptEffects;
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    let initial = JobScriptEffects {
        parameters: [("first".into(), "a".repeat(512 * 1024))].into(),
        ..Default::default()
    };
    db.save_job_script_effects(1, &initial).unwrap();
    let excess = JobScriptEffects {
        parameters: [("second".into(), "b".repeat(512 * 1024))].into(),
        ..Default::default()
    };
    assert!(
        db.save_job_script_effects(1, &excess)
            .unwrap_err()
            .to_string()
            .contains("1 MiB")
    );
    assert_eq!(
        db.job_script_effects(1).unwrap().parameters,
        initial.parameters
    );
    assert!(
        !db.load_active_jobs().unwrap()[&JobId(1)]
            .metadata
            .iter()
            .any(|(name, _)| name == "second")
    );
}

#[test]
fn metadata_edit_refreshes_the_warm_script_effects_cache() {
    use crate::jobs::model::{FieldUpdate, JobUpdate};
    use crate::post_processing::effects::JobScriptEffects;
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(7)).unwrap();
    let emitted = JobScriptEffects {
        parameters: [("chain".into(), "emitted".into())].into(),
        ..Default::default()
    };
    db.save_job_script_effects(7, &emitted).unwrap();
    assert_eq!(
        db.job_script_effects(7).unwrap().parameters,
        emitted.parameters
    );

    db.update_active_job(
        JobId(7),
        &JobUpdate {
            metadata: FieldUpdate::Set(vec![("edited".into(), "value".into())]),
            ..Default::default()
        },
    )
    .unwrap();

    let parameters = db.job_script_effects(7).unwrap().parameters;
    assert_eq!(parameters.get("edited").map(String::as_str), Some("value"));
    assert_eq!(parameters.get("chain").map(String::as_str), Some(""));
}

#[test]
fn cancellation_archive_preserves_directives_newer_than_the_runtime_snapshot() {
    use crate::post_processing::effects::JobScriptEffects;
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(42)).unwrap();
    let history = JobHistoryRow {
        job_id: 42,
        job_hash: None,
        name: "cancelled-script".into(),
        status: "cancelled".into(),
        error_message: None,
        total_bytes: 10,
        downloaded_bytes: 10,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: Some("/tmp/old-output".into()),
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: Some(r#"[["chain","stale"],["keep","input"]]"#.into()),
        server_attribution: None,
    };
    let effects = JobScriptEffects {
        parameters: [("chain".into(), "emitted".into())].into_iter().collect(),
        directory: Some(PathBuf::from("/tmp/moved-output")),
        ..Default::default()
    };
    db.save_job_script_effects(42, &effects).unwrap();
    db.archive_job(JobId(42), &history).unwrap();
    let archived = db.get_job_history(42).unwrap().unwrap();
    let parameters: HashMap<String, String> =
        serde_json::from_str::<Vec<(String, String)>>(&archived.metadata.unwrap())
            .unwrap()
            .into_iter()
            .collect();
    assert_eq!(parameters["chain"], "emitted");
    assert_eq!(parameters["keep"], "input");
    assert_eq!(archived.output_dir.as_deref(), Some("/tmp/moved-output"));
    assert!(db.save_job_script_effects(42, &effects).is_err());
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
        server_attribution: None,
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
    db.complete_file(JobId(1), 0, "f.rar", &[0; 16]).unwrap();

    db.delete_active_job(JobId(1)).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert!(jobs.is_empty());
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
        server_attribution: None,
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
            server_attribution: None,
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
            server_attribution: None,
        },
    )
    .unwrap();

    db.complete_file(JobId(1), 0, "late.rar", &[0x11; 16])
        .unwrap();

    for table in [
        "active_file_progress",
        "active_files",
        "active_extracted",
        "active_detected_archives",
    ] {
        let count = fetch_i64(&db, format!("SELECT COUNT(*) FROM {table}"), vec![]);
        assert_eq!(count, 0, "{table} should remain empty after late writes");
    }
}

#[test]
fn prune_orphan_active_state_removes_only_orphans() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    execute_sql(
        &db,
        "INSERT INTO active_file_progress
         (job_id, file_index, coverage)
         VALUES (1, 0, x'00'), (100, 0, x'00')",
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
    assert_eq!(counts.active_detected_archives, 1);
    assert_eq!(counts.total_removed(), 2);

    let remaining_progress = fetch_i64(
        &db,
        "SELECT COUNT(*) FROM active_file_progress WHERE job_id = 1",
        vec![],
    );
    assert_eq!(remaining_progress, 1);
}

fn bulk_identity(file_index: u32, current_filename: &str) -> ActiveFileIdentity {
    ActiveFileIdentity {
        file_index,
        source_filename: format!("source-{file_index}.bin"),
        current_filename: current_filename.to_string(),
        canonical_filename: None,
        classification: Some(DetectedArchiveIdentity {
            kind: DetectedArchiveKind::Rar,
            set_name: "show".to_string(),
            volume_index: Some(file_index),
        }),
        classification_source: FileIdentitySource::Probe,
    }
}

#[test]
fn save_file_identities_bulk_upserts_and_updates_active_files_filename() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    // active_files rows must exist for the sibling filename update to match.
    db.complete_file(JobId(1), 0, "old-0.rar", &[0x10; 16])
        .unwrap();
    db.complete_file(JobId(1), 1, "old-1.rar", &[0x11; 16])
        .unwrap();

    let identities = vec![
        bulk_identity(0, "show.part01.rar"),
        bulk_identity(1, "show.part02.rar"),
    ];
    db.save_file_identities(JobId(1), &identities).unwrap();

    let jobs = db.load_active_jobs().unwrap();
    let recovered = &jobs[&JobId(1)];
    assert_eq!(recovered.file_identities.len(), 2);
    assert_eq!(
        recovered
            .file_identities
            .get(&0)
            .map(|i| i.current_filename.as_str()),
        Some("show.part01.rar")
    );
    // The sibling active_files.filename update mirrors save_file_identity.
    assert_eq!(
        fetch_text(
            &db,
            "SELECT filename FROM active_files WHERE job_id = {} AND file_index = {}",
            vec![SqlArg::I64(1), SqlArg::I64(0)],
            "filename",
        ),
        "show.part01.rar"
    );
    assert_eq!(
        fetch_text(
            &db,
            "SELECT filename FROM active_files WHERE job_id = {} AND file_index = {}",
            vec![SqlArg::I64(1), SqlArg::I64(1)],
            "filename",
        ),
        "show.part02.rar"
    );

    // Re-running with a new current_filename upserts in place (idempotent shape).
    let identities = vec![bulk_identity(0, "show.part1.rar")];
    db.save_file_identities(JobId(1), &identities).unwrap();
    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(
        jobs[&JobId(1)]
            .file_identities
            .get(&0)
            .map(|i| i.current_filename.as_str()),
        Some("show.part1.rar")
    );
    assert_eq!(
        fetch_text(
            &db,
            "SELECT filename FROM active_files WHERE job_id = {} AND file_index = {}",
            vec![SqlArg::I64(1), SqlArg::I64(0)],
            "filename",
        ),
        "show.part1.rar"
    );
}

#[test]
fn save_file_identities_guards_absent_job() {
    let db = Database::open_in_memory().unwrap();
    // No active job row: neither the identity upsert nor the filename update runs.
    db.save_file_identities(JobId(404), &[bulk_identity(0, "ghost.rar")])
        .unwrap();
    let count = fetch_i64(&db, "SELECT COUNT(*) FROM active_file_identities", vec![]);
    assert_eq!(count, 0);
}

#[test]
fn save_file_identities_chunks_beyond_sqlite_bind_limit() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    // 1200 identities at 9 binds/row exceeds the ~900 SQLite bind budget.
    let identities: Vec<ActiveFileIdentity> = (0..1200)
        .map(|i| bulk_identity(i, &format!("file-{i}.rar")))
        .collect();
    db.save_file_identities(JobId(1), &identities).unwrap();

    let count = fetch_i64(
        &db,
        "SELECT COUNT(*) FROM active_file_identities WHERE job_id = 1",
        vec![],
    );
    assert_eq!(count, 1200);
}

#[test]
fn complete_files_bulk_upserts_hashes_and_is_idempotent() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();

    let entries = vec![
        (0u32, "data.rar".to_string(), Some([0x11; 16])),
        (1u32, "data.r00".to_string(), None),
    ];
    db.complete_files(JobId(1), &entries, CompletedHashProvenance::Streamed)
        .unwrap();

    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs[&JobId(1)].complete_files.len(), 2);
    let hashes = db.load_complete_file_hashes_any(JobId(1)).unwrap();
    assert_eq!(hashes[&0], [0x11; 16]);
    // The hashless entry is complete but carries no md5.
    assert!(!hashes.contains_key(&1));

    // Re-run upserts the filename/hash in place.
    let entries = vec![(0u32, "data-renamed.rar".to_string(), Some([0x22; 16]))];
    db.complete_files(JobId(1), &entries, CompletedHashProvenance::Streamed)
        .unwrap();
    assert_eq!(
        fetch_text(
            &db,
            "SELECT filename FROM active_files WHERE job_id = {} AND file_index = {}",
            vec![SqlArg::I64(1), SqlArg::I64(0)],
            "filename",
        ),
        "data-renamed.rar"
    );
    let hashes = db.load_complete_file_hashes_any(JobId(1)).unwrap();
    assert_eq!(hashes[&0], [0x22; 16]);
}

#[test]
fn complete_files_guards_absent_job() {
    let db = Database::open_in_memory().unwrap();
    db.complete_files(
        JobId(7),
        &[(0u32, "ghost.rar".to_string(), Some([0x01; 16]))],
        CompletedHashProvenance::Streamed,
    )
    .unwrap();
    let count = fetch_i64(&db, "SELECT COUNT(*) FROM active_files", vec![]);
    assert_eq!(count, 0);
}

#[test]
fn complete_files_chunks_beyond_sqlite_bind_limit() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_job(1)).unwrap();
    // 1500 files at 4 binds/row exceeds the ~900 SQLite bind budget.
    let entries: Vec<(u32, String, Option<[u8; 16]>)> = (0..1500)
        .map(|i| (i, format!("file-{i}.rar"), Some([(i % 256) as u8; 16])))
        .collect();
    db.complete_files(JobId(1), &entries, CompletedHashProvenance::Streamed)
        .unwrap();

    let count = fetch_i64(
        &db,
        "SELECT COUNT(*) FROM active_files WHERE job_id = 1",
        vec![],
    );
    assert_eq!(count, 1500);
}

// A job submitted with a password says so on its history row, which keeps no
// password; one submitted without says nothing.
#[test]
fn archive_job_records_that_the_job_had_a_submitted_password() {
    use crate::history::attributes::{
        SUBMITTED_PASSWORD_ATTRIBUTE_KEY, history_metadata_has_password,
    };

    let db = Database::open_in_memory().unwrap();
    let mut with_password = sample_job(1);
    with_password.password_override = Some("submitted-secret".into());
    db.create_active_job(&with_password).unwrap();
    let mut without_password = sample_job(2);
    without_password.password_override = Some(String::new());
    db.create_active_job(&without_password).unwrap();

    for job_id in [1, 2] {
        let history = JobHistoryRow {
            job_id,
            job_hash: None,
            name: format!("test-{job_id}.nzb"),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 1_000,
            downloaded_bytes: 1_000,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: Some(format!("/tmp/output_{job_id}")),
            nzb_path: None,
            created_at: 1700000001,
            completed_at: 1700001000,
            metadata: Some(
                serde_json::to_string(&vec![("Origin".to_string(), "e2e".to_string())]).unwrap(),
            ),
            server_attribution: None,
        };
        db.archive_job(JobId(job_id), &history).unwrap();
    }

    let metadata = |job_id| {
        crate::parse_history_metadata(
            db.get_job_history(job_id)
                .unwrap()
                .unwrap()
                .metadata
                .as_deref(),
        )
    };
    let first = metadata(1);
    assert!(history_metadata_has_password(&first));
    assert!(first.contains(&("Origin".to_string(), "e2e".to_string())));
    assert!(first.contains(&(
        SUBMITTED_PASSWORD_ATTRIBUTE_KEY.to_string(),
        "1".to_string()
    )));
    assert!(
        !first
            .iter()
            .any(|(_, value)| value.contains("submitted-secret"))
    );
    assert!(!history_metadata_has_password(&metadata(2)));
}
