// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The archive-password harvest and the persisted NZB it reads: what
// admission keeps, what the first harvest reads from the row and remembers,
// and what a reprocess or a redownload rebuilds from the row.

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::*;

fn sample_nzb_zstd_with_password(password: &str) -> Vec<u8> {
    let nzb = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <head><meta type="password">{password}</meta></head>
          <file poster="poster" date="1700000000" subject="Silver Horizon.Sample.rar">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">msgid@example.com</segment></segments>
          </file>
        </nzb>"#
    );
    crate::ingest::compress_nzb_bytes(nzb.as_bytes()).unwrap()
}

async fn database(temp_dir: &tempfile::TempDir) -> sqlx::SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(temp_dir.path().join("weaver.db"))
                .create_if_missing(false),
        )
        .await
        .unwrap()
}

// Overwrites the job's persisted NZB with bytes that cannot be parsed, so a
// harvest that still returns the NZB's candidates provably did not read it.
async fn corrupt_persisted_nzb(temp_dir: &tempfile::TempDir, job_id: JobId) {
    set_persisted_nzb(temp_dir, job_id, Some(vec![0xFFu8; 64])).await;
}

async fn set_persisted_nzb(temp_dir: &tempfile::TempDir, job_id: JobId, nzb_zstd: Option<Vec<u8>>) {
    let pool = database(temp_dir).await;
    let updated = sqlx::query("UPDATE active_jobs SET nzb_zstd = ? WHERE job_id = ?")
        .bind(nzb_zstd)
        .bind(job_id.0 as i64)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    pool.close().await;
}

// The harvest as the pair these tests assert on: the candidates, and whether
// the job remembers the half it read from its row.
fn harvest(pipeline: &Pipeline, job_id: JobId) -> (Vec<ArchivePasswordCandidate>, bool) {
    let candidates = pipeline.archive_password_candidates_for_job(job_id);
    let remembered = pipeline
        .jobs
        .get(&job_id)
        .is_some_and(|state| state.nzb_password_candidates.get().is_some());
    (candidates, remembered)
}

// The job as one restored from its row holds it: nothing of its NZB's half
// in memory, so the first harvest reads the row.
fn forget_harvest(pipeline: &mut Pipeline, job_id: JobId) {
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .nzb_password_candidates
        .take();
}

fn nzb_half_candidates() -> Vec<ArchivePasswordCandidate> {
    vec![
        ArchivePasswordCandidate::new(ArchivePasswordSource::NzbMeta, "meta-key".to_string()),
        ArchivePasswordCandidate::new(
            ArchivePasswordSource::FilenameConvention,
            "harbour-key".to_string(),
        ),
    ]
}

#[tokio::test]
async fn add_job_records_streamed_nzb_hash_in_active_jobs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30036);
    let spec = standalone_job_spec("Streamed Hash", &[("episode.mkv".to_string(), 123)]);
    let nzb_path = PathBuf::from(format!("job-{}.nzb", job_id.0));
    let nzb_zstd = sample_nzb_zstd();
    let expected_hash = crate::ingest::hash_persisted_nzb_bytes(&nzb_zstd);

    pipeline
        .add_job(
            job_id,
            spec,
            nzb_path,
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();

    let pool = database(&temp_dir).await;
    let stored_hash: Vec<u8> =
        sqlx::query_scalar("SELECT nzb_hash FROM active_jobs WHERE job_id = ?")
            .bind(job_id.0 as i64)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored_hash, expected_hash);
}

#[tokio::test]
async fn add_job_keeps_nzb_password_candidates_so_the_harvest_never_rereads_the_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30037);
    let mut spec = standalone_job_spec("Silver Horizon", &[("episode.mkv".to_string(), 123)]);
    spec.password = Some("spec-key".to_string());
    pipeline
        .add_job(
            job_id,
            spec,
            PathBuf::from("Silver Horizon {{harbour-key}}.nzb"),
            sample_nzb_zstd_with_password("meta-key"),
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();

    // Before any archive file completes: the persisted NZB is now unreadable,
    // so every candidate below came from what admission derived.
    corrupt_persisted_nzb(&temp_dir, job_id).await;

    let mut expected = nzb_half_candidates();
    expected.insert(
        0,
        ArchivePasswordCandidate::new(ArchivePasswordSource::Explicit, "spec-key".to_string()),
    );
    for _ in 0..2 {
        let (candidates, remembered) = harvest(&pipeline, job_id);
        assert!(remembered);
        assert!(candidates == expected);
    }
}

#[tokio::test]
async fn the_first_harvest_parses_the_persisted_nzb_and_later_ones_read_memory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30038);
    let spec = standalone_job_spec("Silver Horizon", &[("episode.mkv".to_string(), 123)]);
    // A job that entered without the NZB in hand: the first harvest has to
    // read the row.
    insert_active_job_with_persisted_nzb_named(
        &mut pipeline,
        job_id,
        spec,
        sample_nzb_zstd_with_password("meta-key"),
        Some("Silver Horizon {{harbour-key}}.nzb"),
    )
    .await;
    forget_harvest(&mut pipeline, job_id);

    let (first, remembered) = harvest(&pipeline, job_id);
    assert!(remembered);
    assert!(first == nzb_half_candidates());

    corrupt_persisted_nzb(&temp_dir, job_id).await;

    let (second, remembered) = harvest(&pipeline, job_id);
    assert!(remembered);
    assert!(
        second == nzb_half_candidates(),
        "a later harvest must not read the persisted NZB again"
    );
}

#[tokio::test]
async fn a_harvest_that_finds_no_persisted_nzb_remembers_the_empty_list() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30040);
    let spec = standalone_job_spec("Silver Horizon", &[("episode.mkv".to_string(), 123)]);
    insert_active_job_with_persisted_nzb_named(
        &mut pipeline,
        job_id,
        spec,
        sample_nzb_zstd_with_password("meta-key"),
        Some("Silver Horizon {{harbour-key}}.nzb"),
    )
    .await;
    forget_harvest(&mut pipeline, job_id);
    set_persisted_nzb(&temp_dir, job_id, None).await;

    let (first, remembered) = harvest(&pipeline, job_id);
    assert!(remembered);
    assert!(first.is_empty());

    // Bytes that appear afterwards are not read: the first harvest's answer
    // stands for the rest of the job.
    set_persisted_nzb(
        &temp_dir,
        job_id,
        Some(sample_nzb_zstd_with_password("meta-key")),
    )
    .await;
    let (second, remembered) = harvest(&pipeline, job_id);
    assert!(remembered);
    assert!(
        second.is_empty(),
        "a later harvest must not read the persisted NZB again"
    );
}

#[tokio::test]
async fn a_harvest_whose_nzb_failed_to_parse_reads_the_row_again() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30039);
    let spec = standalone_job_spec("Silver Horizon", &[("episode.mkv".to_string(), 123)]);
    insert_active_job_with_persisted_nzb(&mut pipeline, job_id, spec, vec![0xFFu8; 64]).await;

    for _ in 0..2 {
        let (candidates, remembered) = harvest(&pipeline, job_id);
        assert!(!remembered, "a parse failure is never remembered");
        assert!(candidates.is_empty());
    }

    // The row read again finds the NZB once it parses.
    set_persisted_nzb(
        &temp_dir,
        job_id,
        Some(sample_nzb_zstd_with_password("meta-key")),
    )
    .await;
    let (candidates, remembered) = harvest(&pipeline, job_id);
    assert!(remembered);
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.value())
            .collect::<Vec<_>>(),
        ["meta-key"]
    );
}

#[tokio::test]
async fn archive_password_override_is_durable_for_history_reprocess_and_redownload() {
    for reprocess in [false, true] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30902);
        let mut row = history_row_with_output_dir(
            job_id,
            "Archive Fixture",
            "failed",
            temp_dir.path().join("unused-output"),
        );
        row.output_dir = None;
        row.error_message = Some("password required".into());
        insert_history_row_with_nzb_zstd(
            &pipeline.db,
            &row,
            &sample_nzb_zstd_with_password("metadata-key"),
        );
        if reprocess {
            pipeline
                .reprocess_job_with_password(job_id, Some("replacement-key".into()))
                .await
                .unwrap();
        } else {
            pipeline
                .redownload_job_with_password(job_id, Some("replacement-key".into()))
                .await
                .unwrap();
        }
        assert_eq!(
            pipeline.jobs.get(&job_id).unwrap().spec.password.as_deref(),
            Some("replacement-key")
        );
        assert_eq!(
            pipeline.archive_password_candidates_for_job(job_id)[0].value(),
            "replacement-key"
        );
        let restored = pipeline.db.load_active_jobs().unwrap();
        assert_eq!(
            restored.get(&job_id).unwrap().password_override.as_deref(),
            Some("replacement-key")
        );
    }
}

// A reprocess over nothing on disk fetches the job again: its spec is
// rebuilt from the history row's persisted NZB, and its articles are queued.
async fn reprocess_rebuilds_from_history(job_id: JobId, name: &str, status: &str) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut row = history_row_with_output_dir(
        job_id,
        name,
        status,
        temp_dir.path().join("unused-output-dir"),
    );
    row.output_dir = None;
    if status == "failed" {
        row.error_message = Some("boom".to_string());
    }
    row.category = Some("tv".to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    insert_history_row_with_nzb_zstd(&pipeline.db, &row, &sample_nzb_zstd());

    pipeline.reprocess_job(job_id).await.unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Queued);
    assert_eq!(state.spec.files.len(), 1);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert_eq!(
        state.spec.metadata,
        vec![
            ("source".to_string(), "history".to_string()),
            (
                "weaver.original_title".to_string(),
                format!("job-{}", job_id.0),
            ),
        ]
    );
    assert!(!state.download_queue.is_empty());
}

#[tokio::test]
async fn reprocess_job_rebuilds_failed_history_from_streamed_persisted_nzb() {
    reprocess_rebuilds_from_history(JobId(30037), "Failed History Job", "failed").await;
}

#[tokio::test]
async fn reprocess_job_rebuilds_complete_history_from_streamed_persisted_nzb() {
    reprocess_rebuilds_from_history(JobId(30039), "Complete History Job", "complete").await;
}
