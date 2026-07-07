use super::*;

#[tokio::test]
async fn restore_job_rehydrates_detected_obfuscated_split_7z_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10077);
    let fixture_files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let obfuscated_files: Vec<(String, Vec<u8>)> = fixture_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.{}", index + 10),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Restore Obfuscated Split 7z", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
        persist_completed_file_hash(&pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    drop(pipeline);
    let (mut restored, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: recovered.file_progress,
            complete_files: recovered.complete_files,
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: recovered.output_dir,
        })
        .await
        .unwrap();

    let restored_file = restored
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: 1,
        })
        .unwrap();
    assert!(matches!(
        restored_file.role(),
        weaver_model::files::FileRole::Unknown | weaver_model::files::FileRole::SplitFile { .. }
    ));
    assert!(matches!(
        restored.classified_role_for_file(job_id, restored_file),
        weaver_model::files::FileRole::SevenZipSplit { .. }
    ));
    assert_eq!(
        restored
            .detected_archive_identity(job_id, restored_file.file_id())
            .map(|detected| detected.set_name.as_str()),
        Some("51273aad56a8b904e96928935278a627")
    );

    let set_name = "51273aad56a8b904e96928935278a627";
    let topology = restored
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for(set_name))
        .cloned()
        .expect("restored 7z topology should exist");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::SevenZip
    );
    assert_eq!(
        topology.expected_volume_count,
        Some(obfuscated_files.len() as u32)
    );
    assert_eq!(topology.complete_volumes.len(), obfuscated_files.len());
    let _ = complete_dir;
}

#[tokio::test]
async fn delete_history_removes_intermediate_output_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30020);
    let output_dir = intermediate_dir.join("history-cleanup-job");
    tokio::fs::create_dir_all(&output_dir).await.unwrap();
    tokio::fs::write(output_dir.join("leftover.bin"), b"leftover")
        .await
        .unwrap();

    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "History Cleanup",
            "failed",
            output_dir.clone(),
        ))
        .unwrap();

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::DeleteHistory {
            job_id,
            delete_files: false,
            reply,
        })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("delete history reply should arrive")
        .unwrap()
        .unwrap();

    assert!(!output_dir.exists());
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn delete_history_removes_db_only_history_row() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30023);
    let output_dir = intermediate_dir.join("history-db-only-job");
    tokio::fs::create_dir_all(&output_dir).await.unwrap();

    let row = history_row_with_output_dir(job_id, "History DB NZB", "failed", output_dir);
    pipeline.db.insert_job_history(&row).unwrap();

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::DeleteHistory {
            job_id,
            delete_files: false,
            reply,
        })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("delete history reply should arrive")
        .unwrap()
        .unwrap();

    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn delete_all_history_keeps_complete_output_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let failed_job_id = JobId(30021);
    let complete_job_id = JobId(30022);
    let failed_output_dir = intermediate_dir.join("failed-history-job");
    let complete_output_dir = complete_dir.join("complete-history-job");

    tokio::fs::create_dir_all(&failed_output_dir).await.unwrap();
    tokio::fs::create_dir_all(&complete_output_dir)
        .await
        .unwrap();
    tokio::fs::write(failed_output_dir.join("partial.mkv"), b"partial")
        .await
        .unwrap();
    tokio::fs::write(complete_output_dir.join("episode.mkv"), b"complete")
        .await
        .unwrap();

    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            failed_job_id,
            "Failed History Cleanup",
            "failed",
            failed_output_dir.clone(),
        ))
        .unwrap();
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            complete_job_id,
            "Complete History Cleanup",
            "complete",
            complete_output_dir.clone(),
        ))
        .unwrap();

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::DeleteAllHistory {
            delete_files: false,
            reply,
        })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("delete all history reply should arrive")
        .unwrap()
        .unwrap();

    assert!(!failed_output_dir.exists());
    assert!(complete_output_dir.exists());
    assert!(
        pipeline
            .db
            .list_job_history(&crate::HistoryFilter::default())
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn record_job_history_purges_terminal_job_runtime_and_queue_metrics() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Terminal Runtime Cleanup", &files);
    let job_id = JobId(30033);
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.update_queue_metrics();
    assert!(
        pipeline
            .metrics
            .download_queue_depth
            .load(Ordering::Relaxed)
            > 0
    );

    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Complete;
    pipeline.record_job_history(job_id);
    pipeline.db.flush_write_queue().await.unwrap();

    assert!(pipeline.db.load_active_jobs().unwrap().is_empty());
    let history = pipeline.db.get_job_history(job_id.0).unwrap();
    assert!(history.is_some());
    assert_eq!(history.unwrap().status, "complete");
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert_eq!(
        pipeline
            .metrics
            .download_queue_depth
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .recovery_queue_depth
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        pipeline
            .finished_jobs
            .iter()
            .any(|job| job.job_id == job_id)
    );
}

#[tokio::test]
async fn record_job_history_caps_finished_job_runtime_cache() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let base_job_id = 900_000;
    pipeline.finished_jobs = (0..crate::jobs::FINISHED_JOBS_RUNTIME_CAP)
        .map(|offset| finished_job_info(JobId(base_job_id + offset as u64)))
        .collect();
    let evicted_job_id = JobId(base_job_id + crate::jobs::FINISHED_JOBS_RUNTIME_CAP as u64 - 1);

    let job_id = JobId(base_job_id + crate::jobs::FINISHED_JOBS_RUNTIME_CAP as u64 + 1);
    let spec = standalone_job_spec(
        "Finished Runtime Cap",
        &[("finished-runtime-cap.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Complete;

    pipeline.record_job_history(job_id);
    pipeline.db.flush_write_queue().await.unwrap();

    assert_eq!(
        pipeline.finished_jobs.len(),
        crate::jobs::FINISHED_JOBS_RUNTIME_CAP
    );
    assert_eq!(pipeline.finished_jobs.first().unwrap().job_id, job_id);
    assert!(
        pipeline
            .finished_jobs
            .iter()
            .all(|job| job.job_id != evicted_job_id)
    );
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_some());
}

#[tokio::test]
async fn record_job_history_retains_failed_job_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30034);
    let spec = standalone_job_spec(
        "Failed History Retention",
        &[("episode.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let expected_nzb_path = pipeline
        .db
        .load_active_job_persisted_nzb(job_id)
        .unwrap()
        .unwrap()
        .0;

    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Failed {
        error: "boom".to_string(),
    };
    pipeline.record_job_history(job_id);
    pipeline.db.flush_write_queue().await.unwrap();

    let history = pipeline.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "failed");
    assert_eq!(
        history.nzb_path.as_deref(),
        Some(expected_nzb_path.to_str().unwrap())
    );
    let (_, nzb_zstd) = pipeline
        .db
        .load_history_job_persisted_nzb(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(nzb_zstd.unwrap(), sample_nzb_zstd());
}

#[tokio::test]
async fn record_job_history_retains_complete_job_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30036);
    let spec = standalone_job_spec(
        "Complete History Retention",
        &[("episode.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let expected_nzb_path = pipeline
        .db
        .load_active_job_persisted_nzb(job_id)
        .unwrap()
        .unwrap()
        .0;

    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Complete;
    pipeline.record_job_history(job_id);
    pipeline.db.flush_write_queue().await.unwrap();

    let history = pipeline.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "complete");
    assert_eq!(
        history.nzb_path.as_deref(),
        Some(expected_nzb_path.to_str().unwrap())
    );
    let (_, nzb_zstd) = pipeline
        .db
        .load_history_job_persisted_nzb(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(nzb_zstd.unwrap(), sample_nzb_zstd());
}

#[tokio::test]
async fn restore_job_uses_per_file_progress_floor_for_reporting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30035);
    let spec = standalone_job_spec(
        "Restore Progress Floor",
        &[("a.bin".to_string(), 100), ("b.bin".to_string(), 100)],
    );
    let working_dir = temp_dir.path().join("restore-progress-floor");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    tokio::fs::write(working_dir.join("b.bin"), vec![0u8; 100])
        .await
        .unwrap();

    let file_progress = HashMap::from([(0u32, 40u64), (1u32, 100u64)]);

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress,
            complete_files: HashSet::from([NzbFileId {
                job_id,
                file_index: 0,
            }]),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.downloaded_bytes, 200);
    assert_eq!(state.restored_download_floor_bytes, 200);
    assert_eq!(Pipeline::effective_downloaded_bytes(state), 200);
    assert!((Pipeline::effective_progress(state) - 1.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn restore_job_skips_eager_delete_for_ownerless_restored_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let empty_rar = build_empty_rar_volume();
    let files = vec![("ownerless.part01.rar".to_string(), empty_rar)];
    let spec = rar_job_spec("RAR Ownerless Restore", &files);
    let job_id = JobId(30031);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
        write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
        working_dir
    };

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::from([NzbFileId {
                job_id,
                file_index: 0,
            }]),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    assert!(working_dir.join("ownerless.part01.rar").exists());
    assert!(
        !restored
            .eagerly_deleted
            .get(&job_id)
            .is_some_and(|deleted| deleted.contains("ownerless.part01.rar"))
    );
    let plan = restored
        .rar_sets
        .get(&(job_id, "ownerless".to_string()))
        .and_then(|state| state.plan.as_ref())
        .expect("ownerless RAR restore should produce a plan");
    let decision = plan
        .delete_decisions
        .get(&0)
        .expect("ownerless restore should keep volume 0 audited");
    assert!(decision.owners.is_empty());
    assert!(!decision.ownership_eligible);
}

#[tokio::test]
async fn restore_job_rehydrates_existing_deterministic_staging_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31207);
    let spec = standalone_job_spec(
        "Restore staged extraction",
        &[("sample.bin".to_string(), 100)],
    );
    let working_dir = temp_dir.path().join("restore-staged-extraction");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    let staging_dir = pipeline.deterministic_extraction_staging_dir(job_id);
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.staging_dir.clone()),
        Some(staging_dir)
    );
}

#[tokio::test]
async fn restore_job_normalizes_persisted_checking_to_queued_when_work_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30092);
    let spec = standalone_job_spec(
        "Restore Checking Pending",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    let working_dir = temp_dir.path().join("restore-checking-pending");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Checking,
            download_state: Some(crate::jobs::model::DownloadState::Checking),
            post_state: Some(crate::jobs::model::PostState::Idle),
            run_state: Some(crate::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Queued
    ));
}

#[tokio::test]
async fn restore_job_normalizes_persisted_checking_to_complete_when_no_work_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30093);
    let spec = standalone_job_spec(
        "Restore Checking Complete",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    let working_dir = temp_dir.path().join("restore-checking-complete");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::from([
                NzbFileId {
                    job_id,
                    file_index: 0,
                },
                NzbFileId {
                    job_id,
                    file_index: 1,
                },
            ]),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Checking,
            download_state: Some(crate::jobs::model::DownloadState::Checking),
            post_state: Some(crate::jobs::model::PostState::Idle),
            run_state: Some(crate::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Complete
    ));
}

#[tokio::test]
async fn restore_queued_postprocessing_schedules_completion_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31201);
    let spec = standalone_job_spec("Restore queued repair", &[("sample.bin".to_string(), 100)]);
    let working_dir = temp_dir.path().join("restore-queued-repair");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::QueuedRepair,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
}

#[tokio::test]
async fn pause_clears_stale_completion_rechecks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31231);

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "paused-job", temp_dir.path().join("paused-job")),
    );
    pipeline.schedule_job_completion_check(job_id);
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );

    pipeline.pause_job_runtime(job_id).unwrap();
    assert!(pipeline.pending_completion_checks.is_empty());

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Paused)
    );
}
