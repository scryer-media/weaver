use super::*;

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
        pipeline
            .jobs
            .get_mut(&id)
            .unwrap()
            .server_attribution
            .note_article(server, bytes);
        pipeline.dirty_server_attribution.insert(id);
        pipeline.server_attribution_checkpoint_at = Instant::now() + Duration::from_secs(60);
        pipeline.checkpoint_server_attribution_if_due();
        assert!(pipeline.dirty_server_attribution.contains(&id));
        pipeline.server_attribution_checkpoint_at = Instant::now() - Duration::from_secs(5);
        pipeline.checkpoint_server_attribution_if_due();
    }
    pipeline.db.flush_write_queue().await.unwrap();
    assert!(pipeline.dirty_server_attribution.is_empty());
    assert!(pipeline.pending_file_progress.is_empty());
    drop(pipeline);

    for round in 0..2 {
        let (mut restored, _, _) = new_direct_pipeline(&temp).await;
        let saved = restored.db.load_active_jobs().unwrap().remove(&id).unwrap();
        restored
            .restore_job(RestoreJobRequest {
                job_id: id,
                job_hash: [0; 32],
                spec: spec.clone(),
                complete_files: saved.complete_files,
                file_progress: saved.file_progress,
                detected_archives: saved.detected_archives,
                file_identities: saved.file_identities,
                extracted_members: saved.extracted_members,
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
            })
            .await
            .unwrap();
        let ledger = &restored.jobs[&id].server_attribution;
        assert_eq!(ledger.contributions()[0].server_id, 7);
        assert_eq!(ledger.contributions()[0].wire_bytes, 900);
        assert_eq!(ledger.contributions()[1].wire_bytes, 100 + round * 50);
        restored
            .jobs
            .get_mut(&id)
            .unwrap()
            .server_attribution
            .note_article(9, 50);
        restored.dirty_server_attribution.insert(id);
        // Shutdown must save the final interval even without file checkpoints.
        restored.drain().await;
        assert_eq!(
            restored
                .db
                .load_active_server_attribution(id)
                .unwrap()
                .contributions()[1]
                .wire_bytes,
            150 + round * 50
        );
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
    pipeline
        .jobs
        .get_mut(&id)
        .unwrap()
        .server_attribution
        .note_article(7, 900);
    pipeline.dirty_server_attribution.insert(id);
    pipeline.flush_server_attribution();
    pipeline.db.flush_write_queue().await.unwrap();
    let row =
        history_row_with_output_dir(id, "Attribution removal", "complete", temp.path().into());
    pipeline.db.archive_job(id, &row).unwrap();
    pipeline.clear_job_progress_floor_runtime(id);
    assert!(!pipeline.dirty_server_attribution.contains(&id));
    let mut ledger = crate::jobs::server_attribution::JobServerAttribution::default();
    ledger.note_article(7, 900);
    // A delayed writer targets only an existing row, never an INSERT/UPSERT.
    let missing = id;
    pipeline
        .db
        .save_active_server_attribution(vec![(missing, ledger.to_storage_json().unwrap())])
        .unwrap();
    assert!(
        !pipeline
            .db
            .load_active_jobs()
            .unwrap()
            .contains_key(&missing)
    );
    let archived = pipeline.db.get_job_history(id.0).unwrap().unwrap();
    assert_eq!(archived.server_attribution, ledger.to_storage_json());
}
