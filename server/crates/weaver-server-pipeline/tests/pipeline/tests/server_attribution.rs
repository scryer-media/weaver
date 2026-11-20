// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::{Duration, Instant};
use tempfile::TempDir;
use weaver_server_pipeline::history::queries::HistoryQueries;
use weaver_server_pipeline::jobs::queries::JobsQueries;
use weaver_server_pipeline::jobs::repository::JobsRepositoryQueries;
use weaver_server_pipeline::pipeline::tests::{
    attribution_fixture::*,
    history_fixture::{database, drain_pipeline},
    history_row_with_output_dir, insert_active_job_with_persisted_nzb, new_direct_pipeline,
    rar_job_spec, retire_pipeline_database, sample_nzb_zstd,
};
use weaver_server_pipeline::{JobId, JobStatus, RestoreJobRequest};

#[tokio::test]
async fn attribution_checkpoint_survives_repeated_active_job_restores() {
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let id = JobId(79801);
    let spec = rar_job_spec(
        "Attribution restore",
        &[("payload.bin".into(), vec![7; 32])],
    );
    insert_active_job_with_persisted_nzb(&mut pipeline, id, spec.clone(), sample_nzb_zstd()).await;
    for (server, bytes) in [(7, 900), (9, 100)] {
        note_article(&mut pipeline, id, server, bytes);
        checkpoint_at(&mut pipeline, Instant::now() + Duration::from_secs(60));
        assert!(dirty_jobs(&pipeline).contains(&id));
        checkpoint_at(&mut pipeline, Instant::now() - Duration::from_secs(5));
    }
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(dirty_jobs(&pipeline).is_empty());
    retire_pipeline_database(pipeline).await;

    for round in 0..2 {
        let (mut restored, _, _) = new_direct_pipeline(&temp).await;
        let saved = database(&restored)
            .load_active_jobs()
            .unwrap()
            .remove(&id)
            .unwrap();
        restored
            .restore_job(RestoreJobRequest {
                job_id: id,
                job_hash: [0; 32],
                spec: spec.clone(),
                detected_archives: saved.detected_archives,
                file_identities: saved.file_identities,
                status: JobStatus::Paused,
                download_state: None,
                post_state: None,
                run_state: None,
                queued_repair_at_epoch_ms: None,
                queued_extract_at_epoch_ms: None,
                paused_resume_status: None,
                paused_resume_download_state: None,
                paused_resume_post_state: None,
                working_dir: saved.output_dir,
                restart: None,
            })
            .await
            .unwrap();
        let ledger = attribution(&restored, id);
        assert_eq!(ledger.contributions()[0].server_id, 7);
        assert_eq!(ledger.contributions()[0].wire_bytes, 900);
        assert_eq!(ledger.contributions()[1].wire_bytes, 100 + round * 50);
        note_article(&mut restored, id, 9, 50);
        // Shutdown must save the final interval even without file checkpoints.
        drain_pipeline(&mut restored).await;
        assert_eq!(
            stored(&restored, id).contributions()[1].wire_bytes,
            150 + round * 50
        );
        retire_pipeline_database(restored).await;
    }
}

#[tokio::test]
async fn attribution_checkpoint_cannot_recreate_a_removed_job() {
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let id = JobId(79802);
    let spec = rar_job_spec(
        "Attribution removal",
        &[("payload.bin".into(), vec![7; 32])],
    );
    insert_active_job_with_persisted_nzb(&mut pipeline, id, spec, sample_nzb_zstd()).await;
    note_article(&mut pipeline, id, 7, 900);
    flush(&mut pipeline);
    database(&pipeline).flush_write_queue().await.unwrap();
    let row =
        history_row_with_output_dir(id, "Attribution removal", "complete", temp.path().into());
    database(&pipeline).archive_job(id, &row).unwrap();
    clear_floor(&mut pipeline, id);
    assert!(!dirty_jobs(&pipeline).contains(&id));
    let mut ledger =
        weaver_server_pipeline::jobs::server_attribution::JobServerAttribution::default();
    ledger.note_article(7, 900);
    // A delayed writer targets only an existing row, never an INSERT/UPSERT.
    let missing = id;
    save(&pipeline, missing, ledger.to_storage_json().unwrap());
    assert!(
        !database(&pipeline)
            .load_active_jobs()
            .unwrap()
            .contains_key(&missing)
    );
    let archived = database(&pipeline).get_job_history(id.0).unwrap().unwrap();
    assert_eq!(archived.server_attribution, ledger.to_storage_json());
}
