// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// History deletion finishes without a pipeline turn.
async fn delete_history_batch(
    pipeline: &mut Pipeline,
    job_ids: &[JobId],
    delete_files: bool,
) -> crate::HistoryDeleteBatchOutcome {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .clone()
        .delete_history(job_ids.iter().copied().collect(), delete_files)
        .await
        .unwrap()
}

async fn retained_placement_fixture(
    temp: &tempfile::TempDir,
) -> (Pipeline, RestoreJobRequest, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let job_id = JobId(49802);
    let files: Vec<_> = ["a.bin", "b.bin", "c.bin"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| (name.to_string(), vec![index as u8; 32]))
        .collect();
    let spec = rar_job_spec("Blocked placement", &files);
    let entries: String = files.iter().enumerate().map(|(index, (name, _))| format!(
        r#"<file poster="fixture" date="0" subject="&quot;{name}&quot;"><groups><group>alt.test</group></groups><segments><segment bytes="32" number="1">placement-{index}@fixture</segment></segments></file>"#,
    )).collect();
    let nzb = format!(
        r#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">{entries}</nzb>"#
    );
    let working_dir = insert_active_job_with_persisted_nzb(
        &mut pipeline,
        job_id,
        spec.clone(),
        crate::ingest::compress_nzb_bytes(nzb.as_bytes()).unwrap(),
    )
    .await;
    for (index, (name, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
        persist_completed_file_hash(&pipeline, job_id, index as u32, name, bytes).await;
    }
    let identities: Vec<_> = (0..3)
        .map(|file_index| {
            pipeline
                .effective_file_identity(job_id, NzbFileId { job_id, file_index })
                .unwrap()
        })
        .collect();
    pipeline
        .db
        .save_file_identities(job_id, &identities)
        .unwrap();
    pipeline.db.flush_write_queue().await.unwrap();
    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    let plan = par2_rs::PlacementPlan {
        exact: vec![],
        swaps: vec![],
        unresolved: vec![],
        conflicts: vec![],
        renames: [("a.bin", "b.bin"), ("b.bin", "c.bin"), ("c.bin", "a.bin")]
            .into_iter()
            .enumerate()
            .map(|(index, (source, destination))| par2_rs::PlacementEntry {
                file_id: par2_rs::FileId::from_bytes([index as u8; 16]),
                current_name: source.into(),
                correct_name: destination.into(),
            })
            .collect(),
    };
    let bindings = plan
        .renames
        .iter()
        .enumerate()
        .map(|(index, entry)| placement::Binding {
            file_index: index as u32,
            filename: entry.correct_name.clone(),
        })
        .collect();
    drop(
        placement::begin(&working_dir, &plan, bindings)
            .unwrap()
            .unwrap(),
    );
    let journal_dir = std::fs::read_dir(&working_dir)
        .unwrap()
        .map(Result::unwrap)
        .find(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".weaver-placement-")
        })
        .unwrap()
        .path();
    let request = RestoreJobRequest {
        job_id,
        job_hash: [0; 32],
        spec,
        complete_files: recovered.complete_files,
        file_progress: recovered.file_progress,
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
        working_dir,
    };
    (pipeline, request, journal_dir)
}

async fn placement_identity_write_fault(pipeline: &Pipeline, enabled: bool) {
    let datastore = pipeline.db.datastore();
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(&datastore, "test_identity_fault", |tx| {
        Box::pin(async move {
            let sql = if enabled {
                "CREATE TRIGGER reject_placement_identity BEFORE INSERT ON active_file_identities BEGIN SELECT RAISE(ABORT, 'synthetic identity failure'); END"
            } else { "DROP TRIGGER reject_placement_identity" };
            tx.execute(sql, &[]).await?;
            Ok(())
        })
    }).await.unwrap();
}

#[tokio::test]
async fn blocked_placement_restore_stays_visible_across_restart_and_resume() {
    for fault in [
        "corrupt",
        "collision",
        "binding",
        "identity_write",
        "completion",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (pipeline, mut request, journal_dir) = retained_placement_fixture(&temp).await;
        let job_id = request.job_id;
        let journal_path = journal_dir.join("journal.json");
        let original = std::fs::read(&journal_path).unwrap();
        match fault {
            "corrupt" => std::fs::write(&journal_path, b"{broken").unwrap(),
            "binding" => {
                let mut journal: serde_json::Value = serde_json::from_slice(&original).unwrap();
                journal["bindings"][0]["file_index"] = 999.into();
                std::fs::write(&journal_path, serde_json::to_vec(&journal).unwrap()).unwrap();
            }
            "collision" => {
                std::fs::rename(
                    request.working_dir.join("b.bin"),
                    request.working_dir.join("retained-a.bin"),
                )
                .unwrap();
                std::fs::write(request.working_dir.join("b.bin"), [9; 32]).unwrap();
            }
            "identity_write" => placement_identity_write_fault(&pipeline, true).await,
            "completion" => std::fs::write(journal_dir.join("retained-note"), b"keep").unwrap(),
            _ => unreachable!(),
        }
        retire_pipeline_database(pipeline).await;
        for _ in 0..2 {
            let (mut restored, _, _) = new_direct_pipeline(&temp).await;
            let handle =
                SchedulerHandle::new(restored.event_tx.clone(), restored.shared_state.clone());
            let watermark = handle.job_revision();
            handle.restore_job(request.clone()).await.unwrap();
            assert!(handle.job_revision() > watermark);
            assert_eq!(
                handle
                    .get_job(job_id)
                    .unwrap()
                    .download_wait_reason
                    .as_deref(),
                Some("Restoring saved job")
            );
            assert!(!restored.jobs.contains_key(&job_id));
            restored.apply_queue_intents().await;
            let info = restored
                .list_jobs()
                .into_iter()
                .find(|job| job.job_id == job_id)
                .unwrap();
            assert_eq!(info.status, JobStatus::Paused, "{fault}");
            assert!(
                info.error
                    .as_deref()
                    .unwrap()
                    .contains("Placement recovery blocked")
            );
            assert_eq!(info.downloaded_bytes, 96);
            assert_eq!(info.total_files, 3);
            assert_eq!(info.completed_files, 3);
            assert!(restored.jobs[&job_id].download_queue.is_empty());
            assert!(restored.jobs[&job_id].recovery_queue.is_empty());
            assert!(restored.blocked_restores.contains_key(&job_id));
            SchedulerHandle::new(restored.event_tx.clone(), restored.shared_state.clone())
                .resume_all()
                .await
                .unwrap();
            restored.apply_queue_intents().await;
            restored.dispatch_downloads();
            restored.check_job_completion(job_id).await;
            assert_eq!(restored.active_downloads, 0);
            assert_eq!(restored.jobs[&job_id].status, JobStatus::Paused);
            assert!(restored.resume_restored_job(job_id).await.is_err());
            handle.resume_job(job_id).await.unwrap();
            restored.apply_queue_intents().await;
            restored.apply_queue_intents().await;
            assert!(!restored.nzb_admitted.contains(&job_id), "{fault}");
            assert!(!restored.db.load_nzb_admissions().unwrap().contains(&job_id));
            assert!(
                restored
                    .db
                    .load_nzb_pause_releases()
                    .unwrap()
                    .contains(&job_id)
            );
            assert!(journal_dir.exists());
            let row = restored
                .db
                .load_active_jobs()
                .unwrap()
                .remove(&job_id)
                .unwrap();
            assert_eq!(row.status, "paused");
            assert!(
                row.error
                    .as_deref()
                    .unwrap()
                    .contains("Placement recovery blocked")
            );
            let startup = crate::operations::recovery::recover_server_state(
                &restored.db,
                temp.path(),
                &restored.intermediate_dir,
            )
            .await
            .unwrap();
            assert!(
                startup
                    .initial_history
                    .iter()
                    .all(|job| job.job_id != job_id)
            );
            request = startup
                .to_restore
                .into_iter()
                .find(|job| job.job_id == job_id)
                .unwrap()
                .request;
            assert_eq!(request.status, JobStatus::Paused);
            retire_pipeline_database(restored).await;
        }
        let (mut restored, _, _) = new_direct_pipeline(&temp).await;
        restored.restore_job(request.clone()).await.unwrap();
        match fault {
            "corrupt" | "binding" => std::fs::write(&journal_path, original).unwrap(),
            "collision" => {
                std::fs::remove_file(request.working_dir.join("b.bin")).unwrap();
                std::fs::rename(
                    request.working_dir.join("retained-a.bin"),
                    request.working_dir.join("b.bin"),
                )
                .unwrap();
            }
            "identity_write" => placement_identity_write_fault(&restored, false).await,
            "completion" => std::fs::remove_file(journal_dir.join("retained-note")).unwrap(),
            _ => unreachable!(),
        }
        restored.resume_restored_job(job_id).await.unwrap();
        assert!(!restored.blocked_restores.contains_key(&job_id));
        assert!(
            restored
                .list_jobs()
                .iter()
                .find(|job| job.job_id == job_id)
                .unwrap()
                .error
                .is_none()
        );
        assert!(restored.jobs[&job_id].download_queue.is_empty(), "{fault}");
        assert!(!journal_dir.exists());
        for (index, name) in ["b.bin", "c.bin", "a.bin"].iter().enumerate() {
            assert_eq!(
                std::fs::read(request.working_dir.join(name)).unwrap(),
                [index as u8; 32]
            );
        }
    }
}

#[tokio::test]
async fn blocked_restore_recovered_on_startup_stays_paused_and_cancel_clears_gate() {
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, mut request, journal_dir) = retained_placement_fixture(&temp).await;
    let path = journal_dir.join("journal.json");
    let original = std::fs::read(&path).unwrap();
    std::fs::write(&path, b"broken").unwrap();
    retire_pipeline_database(pipeline).await;
    let (mut restored, _, _) = new_direct_pipeline(&temp).await;
    restored.restore_job(request.clone()).await.unwrap();
    let job_id = request.job_id;
    let peer = JobId(49803);
    insert_active_job(
        &mut restored,
        peer,
        rar_job_spec("Peer", &[("peer.bin".into(), vec![1; 32])]),
    )
    .await;
    let order = restored.job_order.clone();
    assert!(restored.resume_restored_job(job_id).await.is_err());
    assert_eq!(restored.job_order, order);
    request.status = JobStatus::Paused;
    retire_pipeline_database(restored).await;
    std::fs::write(&path, &original).unwrap();
    let (mut restored, _, _) = new_direct_pipeline(&temp).await;
    restored.restore_job(request.clone()).await.unwrap();
    assert_eq!(restored.jobs[&job_id].status, JobStatus::Paused);
    assert!(!restored.blocked_restores.contains_key(&job_id));
    assert!(restored.list_jobs()[0].error.is_none());
    assert!(restored.jobs[&job_id].download_queue.is_empty());
    restored.dispatch_downloads();
    assert_eq!(restored.active_downloads, 0);
    restored.resume_restored_job(job_id).await.unwrap();

    // A second failure demonstrates cancel cleanup without needing recovery.
    std::fs::create_dir_all(&journal_dir).unwrap();
    std::fs::write(&path, b"broken").unwrap();
    retire_pipeline_database(restored).await;
    let (mut restored, _, _) = new_direct_pipeline(&temp).await;
    restored.restore_job(request).await.unwrap();
    assert!(restored.blocked_restores.contains_key(&job_id));
    let done = cancel_one_job(
        &mut restored,
        job_id,
        crate::jobs::handle::CancellationOrigin::User,
    )
    .await;
    done.unwrap();
    assert!(!restored.blocked_restores.contains_key(&job_id));
    assert!(!restored.jobs.contains_key(&job_id));
}

#[tokio::test]
async fn blocked_restore_resumes_into_downloading_whatever_status_was_stored() {
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, mut request, journal_dir) = retained_placement_fixture(&temp).await;
    std::fs::write(journal_dir.join("journal.json"), b"broken").unwrap();
    retire_pipeline_database(pipeline).await;
    request.status = JobStatus::Paused;
    request.paused_resume_status = Some(JobStatus::Extracting);
    let job_id = request.job_id;
    let (mut restored, _, _) = new_direct_pipeline(&temp).await;
    restored.restore_job(request).await.unwrap();
    assert!(restored.blocked_restores.contains_key(&job_id));
    assert_eq!(
        restored.jobs[&job_id].paused_resume_status,
        Some(JobStatus::Downloading)
    );
    let row = restored
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    assert_eq!(row.paused_resume_status.as_deref(), Some("downloading"));
}

#[tokio::test]
async fn blocked_non_paused_restore_resumes_into_the_status_its_lanes_name() {
    for (status, download_state, post_state, resume, persisted) in [
        (
            JobStatus::Extracting,
            crate::jobs::model::DownloadState::Complete,
            crate::jobs::model::PostState::Extracting,
            JobStatus::Downloading,
            "downloading",
        ),
        (
            JobStatus::Queued,
            crate::jobs::model::DownloadState::Queued,
            crate::jobs::model::PostState::Idle,
            JobStatus::Queued,
            "queued",
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (pipeline, mut request, journal_dir) = retained_placement_fixture(&temp).await;
        std::fs::write(journal_dir.join("journal.json"), b"broken").unwrap();
        retire_pipeline_database(pipeline).await;
        request.status = status.clone();
        request.download_state = Some(download_state);
        request.post_state = Some(post_state);
        let job_id = request.job_id;
        let (mut restored, _, _) = new_direct_pipeline(&temp).await;
        restored.restore_job(request).await.unwrap();
        assert!(
            restored.blocked_restores.contains_key(&job_id),
            "{status:?}"
        );
        assert_eq!(
            restored.jobs[&job_id].status,
            JobStatus::Paused,
            "{status:?}"
        );
        assert_eq!(
            restored.jobs[&job_id].paused_resume_status,
            Some(resume),
            "{status:?}"
        );
        let row = restored
            .db
            .load_active_jobs()
            .unwrap()
            .remove(&job_id)
            .unwrap();
        assert_eq!(
            row.paused_resume_status.as_deref(),
            Some(persisted),
            "{status:?}"
        );
    }
}

#[tokio::test]
async fn placement_replay_invalidates_verdicts_before_completion_and_post_repair() {
    for phase in ["installation", "partial_binding", "rollback"] {
        for caller in ["apply", "completion", "post_repair"] {
            let temp = tempfile::tempdir().unwrap();
            let (mut pipeline, request, journal_dir) = retained_placement_fixture(&temp).await;
            let job_id = request.job_id;
            if phase == "rollback" {
                let path = journal_dir.join("journal.json");
                let mut journal: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                journal["phase"] = "RollbackVacate".into();
                std::fs::write(path, serde_json::to_vec(&journal).unwrap()).unwrap();
            } else if phase == "partial_binding" {
                let mut identity = request.file_identities[&0].clone();
                identity.current_filename = "b.bin".into();
                identity.canonical_filename = Some("b.bin".into());
                pipeline.db.save_file_identity(job_id, &identity).unwrap();
                pipeline.set_file_identity(job_id, identity).unwrap();
            }
            let index = build_test_par2_index("a.bin", &[0; 32], 16);
            let set = Arc::new(par2_rs::Par2FileSet::from_files(&[&index]).unwrap());
            let set_id = set.recovery_set_id;
            let verification = par2_rs::VerificationResult {
                files: vec![],
                recovery_blocks_available: 0,
                total_missing_blocks: 0,
                repairable: par2_rs::verify::Repairability::NotNeeded,
            };
            let runtime = pipeline
                .ensure_par2_runtime(job_id)
                .ensure_set_runtime(set_id);
            runtime.set = Some(Arc::clone(&set));
            runtime.settled = true;
            runtime.failure = Some("stale failure".into());
            runtime.pending_repair = Some(PendingPar2Repair {
                recovery_set_id: set_id,
                slice_size: 16,
                described_file_ids: set.recovery_file_ids.clone(),
                blocks_needed: 1,
                damaged: 1,
                verification: verification.clone(),
            });
            pipeline.par2_verified.insert(job_id);
            pipeline.remove_pending_completion_check(job_id);
            let empty_plan = par2_rs::PlacementPlan {
                exact: vec![],
                swaps: vec![],
                renames: vec![],
                unresolved: vec![],
                conflicts: vec![],
            };
            match caller {
                "apply" => assert_eq!(
                    pipeline
                        .apply_placement_plan_for_retry_or_repair(
                            job_id,
                            request.working_dir.clone(),
                            &empty_plan,
                        )
                        .await
                        .unwrap(),
                    placement::ApplyOutcome::Reverify
                ),
                "completion" => pipeline.check_job_completion(job_id).await,
                "post_repair" => {
                    let outcome = par2_rs::Par2RepairOutcome {
                        status: par2_rs::Par2RepairStatus::Repaired,
                        files_complete: 3,
                        files_renamed: 0,
                        files_damaged: 0,
                        files_missing: 0,
                        available_blocks: 0,
                        missing_blocks: 0,
                        recovery_blocks_available: 0,
                        recovery_blocks_used: 0,
                        bytes_copied: 0,
                        bytes_reconstructed: 0,
                        packets: Default::default(),
                        scan: Default::default(),
                        carry: Default::default(),
                        verification: verification.clone(),
                    };
                    pipeline
                        .finish_par2_repair(
                            job_id,
                            set,
                            request.working_dir.clone(),
                            &verification,
                            outcome,
                            false,
                        )
                        .await;
                }
                _ => unreachable!(),
            }
            let runtime = pipeline.par2_runtime[&job_id].set_runtime(set_id).unwrap();
            assert!(!runtime.settled, "{phase}: {caller}");
            assert!(runtime.failure.is_none());
            assert!(runtime.pending_repair.is_none());
            assert!(!pipeline.par2_verified.contains(&job_id));
            assert!(pipeline.pending_completion_checks.contains(&job_id));
            assert!(!is_terminal_status(&pipeline.jobs[&job_id].status));
            assert!(!journal_dir.exists());
            assert_eq!(
                pipeline
                    .apply_placement_plan_for_retry_or_repair(
                        job_id,
                        request.working_dir.clone(),
                        &empty_plan,
                    )
                    .await
                    .unwrap(),
                placement::ApplyOutcome::Applied,
                "no replay needs no extra pass"
            );
            let names = if phase == "rollback" {
                ["a.bin", "b.bin", "c.bin"]
            } else {
                ["b.bin", "c.bin", "a.bin"]
            };
            for (index, name) in names.iter().enumerate() {
                assert_eq!(
                    std::fs::read(request.working_dir.join(name)).unwrap(),
                    [index as u8; 32]
                );
            }
        }
    }
}

#[tokio::test]
async fn restore_job_replays_placement_before_building_download_queue() {
    for boundary in 1..=7 {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let job_id = JobId(49801);
        let files: Vec<_> = ["a.bin", "b.bin", "c.bin"]
            .into_iter()
            .enumerate()
            .map(|(index, name)| (name.to_string(), vec![index as u8; 32]))
            .collect();
        let spec = rar_job_spec("Placement restart", &files);
        insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        for (index, (name, bytes)) in files.iter().enumerate() {
            write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
            persist_completed_file_hash(&pipeline, job_id, index as u32, name, bytes).await;
        }
        let working_dir = pipeline.jobs[&job_id].working_dir.clone();
        let identities: Vec<_> = (0..3)
            .map(|file_index| {
                pipeline
                    .effective_file_identity(job_id, NzbFileId { job_id, file_index })
                    .unwrap()
            })
            .collect();
        pipeline
            .db
            .save_file_identities(job_id, &identities)
            .unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "pipeline::completion::finalize::placement::journal::tests::placement_crash_child",
                "--nocapture",
            ])
            .env("WEAVER_PLACEMENT_CRASH_ROOT", &working_dir)
            .env("WEAVER_PLACEMENT_CRASH_BOUNDARY", boundary.to_string())
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(77),
            "child did not reach boundary {boundary}: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        if boundary == 7 {
            // Filesystem placement finished and only the first identity was
            // committed before the process stopped. Reapplying a name map
            // would rotate this file a second time.
            let mut identity = pipeline
                .effective_file_identity(
                    job_id,
                    NzbFileId {
                        job_id,
                        file_index: 0,
                    },
                )
                .unwrap();
            identity.current_filename = "b.bin".into();
            identity.canonical_filename = Some("b.bin".into());
            pipeline.db.save_file_identity(job_id, &identity).unwrap();
        }
        let recovered = pipeline
            .db
            .load_active_jobs()
            .unwrap()
            .remove(&job_id)
            .unwrap();
        retire_pipeline_database(pipeline).await;
        let (mut restored, _, _) = new_direct_pipeline(&temp).await;
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
                working_dir: working_dir.clone(),
            })
            .await
            .unwrap();
        let state = &restored.jobs[&job_id];
        assert!(
            state.download_queue.is_empty(),
            "boundary {boundary} must not redownload staged bytes"
        );
        for (index, destination) in ["b.bin", "c.bin", "a.bin"].iter().enumerate() {
            assert_eq!(
                &state.file_identities[&(index as u32)].current_filename,
                destination
            );
            assert_eq!(
                std::fs::read(working_dir.join(destination)).unwrap(),
                [index as u8; 32]
            );
        }
        assert!(
            crate::pipeline::placement::recover(&working_dir)
                .unwrap()
                .transactions
                .is_empty()
        );
    }
}

#[tokio::test]
async fn scheduling_memory_admission_preserves_placements_and_counts_other_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let mut spec = standalone_job_spec("Memory admission", &[("small.bin".to_string(), 16)]);
    let single = spec.scheduling_memory_estimate();
    let repeated = spec.files[0].clone();
    spec.files.push(repeated);
    assert_eq!(spec.scheduling_memory_estimate(), single * 2);
    let required = spec.scheduling_memory_estimate();
    pipeline.process_memory_budget = Arc::new(ProcessMemoryBudget::new(required));
    let first = pipeline
        .check_job_memory_admission(JobId(31001), &spec)
        .unwrap();
    assert_eq!(pipeline.process_memory_budget.reserved_bytes(), required);
    assert!(matches!(
        pipeline.check_job_memory_admission(JobId(31001), &spec),
        Err(crate::SchedulerError::InvalidInput(_))
    ));
    drop(first);
    assert_eq!(pipeline.process_memory_budget.reserved_bytes(), 0);
    pipeline.process_memory_budget = Arc::new(ProcessMemoryBudget::new(required * 2));
    insert_active_job(&mut pipeline, JobId(31001), spec.clone()).await;
    let first = pipeline
        .check_job_memory_admission(JobId(31001), &spec)
        .unwrap();
    pipeline.job_scheduling_memory.insert(JobId(31001), first);
    assert!(
        pipeline
            .check_job_memory_admission(JobId(31002), &spec)
            .is_ok()
    );
    let competing_work = pipeline
        .process_memory_budget
        .try_reserve_retained(1)
        .unwrap();
    assert!(
        pipeline
            .check_job_memory_admission(JobId(31002), &spec)
            .is_err()
    );
    drop(competing_work);
    pipeline.purge_terminal_job_runtime(JobId(31001));
    assert_eq!(pipeline.process_memory_budget.reserved_bytes(), 0);
    assert!(
        pipeline
            .check_job_memory_admission(JobId(31001), &spec)
            .is_ok()
    );
}

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
    retire_pipeline_database(pipeline).await;
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
    // Historical versions wrote an empty marker; deletion upgrades it from
    // the durable job record without changing the existing history action.
    tokio::fs::write(
        crate::jobs::working_dir::working_dir_marker_path(&output_dir),
        [],
    )
    .await
    .unwrap();
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

    let outcome = delete_history_batch(&mut pipeline, &[job_id], false).await;
    assert!(outcome.refused.is_empty());

    assert!(!output_dir.exists());
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn delete_history_with_a_stale_marker_removes_the_row_and_keeps_the_dir() {
    // A marker that names this job but whose hash the directory no longer
    // reproduces used to abort the whole delete, leaving the record behind
    // for good. The record goes; the directory stays and is reported.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30024);
    let output_dir = intermediate_dir.join("history-stale-marker-job");
    tokio::fs::create_dir_all(&output_dir).await.unwrap();
    tokio::fs::write(
        crate::jobs::working_dir::working_dir_marker_path(&output_dir),
        format!("weaver-job-v1:{}:{}\n", job_id.0, "0".repeat(64)),
    )
    .await
    .unwrap();
    tokio::fs::write(output_dir.join("leftover.bin"), b"leftover")
        .await
        .unwrap();
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "History Stale Marker",
            "failed",
            output_dir.clone(),
        ))
        .unwrap();

    let outcome = delete_history_batch(&mut pipeline, &[job_id], false).await;

    assert_eq!(
        outcome.deleted[&job_id].as_ref().unwrap().left_in_place,
        vec![output_dir.clone()]
    );
    assert!(output_dir.join("leftover.bin").exists());
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn delete_history_reports_a_failed_cleanup_as_deleted_with_its_error() {
    // The record goes in the delete transaction; the directory only after the
    // job's scripts have stopped. A cleanup that fails in between must not
    // pass for a record that survived: the id is deleted, and its error rides
    // with it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let failing = JobId(30026);
    let clean = JobId(30027);
    let mut dirs = BTreeMap::new();
    for (job_id, name) in [
        (failing, "history-cleanup-fails"),
        (clean, "history-cleanup-ok"),
    ] {
        let output_dir = intermediate_dir.join(name);
        tokio::fs::create_dir_all(&output_dir).await.unwrap();
        tokio::fs::write(
            crate::jobs::working_dir::working_dir_marker_path(&output_dir),
            [],
        )
        .await
        .unwrap();
        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                job_id,
                name,
                "failed",
                output_dir.clone(),
            ))
            .unwrap();
        dirs.insert(job_id, output_dir);
    }
    // A script still running for the failing job holds its cleanup at the
    // script-stop barrier, after its record is gone and before its directory
    // goes.
    let running = crate::post_processing::events::hold_test_event_run(
        &pipeline.db,
        failing.0,
        "history-cleanup-barrier",
    );

    pipeline.report_all_for_test();
    let delete = tokio::spawn(
        pipeline
            .shared_state
            .queue
            .clone()
            .delete_history([failing, clean].into_iter().collect(), false),
    );
    let db = pipeline.db.clone();
    crate::post_processing::events::wait_for_test_script_change(&db, || {
        db.get_job_history(failing.0).unwrap().is_none()
    })
    .await;
    // The directory now names another job, so its removal is refused.
    tokio::fs::write(
        crate::jobs::working_dir::working_dir_marker_path(&dirs[&failing]),
        format!("weaver-job-v1:{}:{}\n", 99_999, "0".repeat(64)),
    )
    .await
    .unwrap();
    drop(running);
    let outcome = delete.await.unwrap().unwrap();

    assert!(outcome.refused.is_empty(), "{:?}", outcome.refused);
    let error = outcome.deleted[&failing]
        .as_ref()
        .expect_err("the failing id's cleanup error is reported");
    assert!(
        error.to_string().contains("no longer belongs"),
        "unexpected cleanup error: {error}"
    );
    assert!(outcome.deleted[&clean].is_ok());
    for job_id in [failing, clean] {
        assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
    }
    assert!(dirs[&failing].exists());
    assert!(!dirs[&clean].exists());
}

#[tokio::test]
async fn delete_history_removes_db_only_history_row() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30023);
    let output_dir = intermediate_dir.join("history-db-only-job");
    tokio::fs::create_dir_all(&output_dir).await.unwrap();
    tokio::fs::write(
        crate::jobs::working_dir::working_dir_marker_path(&output_dir),
        [],
    )
    .await
    .unwrap();

    let row = history_row_with_output_dir(job_id, "History DB NZB", "failed", output_dir);
    pipeline.db.insert_job_history(&row).unwrap();

    let outcome = delete_history_batch(&mut pipeline, &[job_id], false).await;
    assert!(outcome.refused.is_empty());

    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn history_deletion_waits_for_scripts_without_blocking_commands() {
    for delete_all in [false, true] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30025);
        let output_dir = intermediate_dir.join("script-delete-job");
        tokio::fs::create_dir_all(&output_dir).await.unwrap();
        tokio::fs::write(
            crate::jobs::working_dir::working_dir_marker_path(&output_dir),
            [],
        )
        .await
        .unwrap();
        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                job_id,
                "Script Delete",
                "failed",
                output_dir.clone(),
            ))
            .unwrap();
        let running = crate::post_processing::events::hold_test_event_run(
            &pipeline.db,
            job_id.0,
            "deletion-barrier",
        );
        // The batch form also carries a second, script-free history row.
        let other_job = JobId(30027);
        let mut job_ids = BTreeSet::from([job_id]);
        if delete_all {
            pipeline
                .db
                .insert_job_history(&history_row_with_output_dir(
                    other_job,
                    "Script-free Delete",
                    "failed",
                    intermediate_dir.join("script-free-delete-job"),
                ))
                .unwrap();
            job_ids.insert(other_job);
        }
        pipeline.report_all_for_test();
        let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
        let mut deletion = Box::pin(handle.delete_history_batch(job_ids, false));
        tokio::select! {
            biased;
            result = &mut deletion => panic!("delete completed before its script barrier: {result:?}"),
            () = std::future::ready(()) => {},
        }
        assert!(output_dir.exists());
        let (diagnostics, ()) = tokio::join!(handle.pipeline_diagnostics(), async {
            pipeline.answer_diagnostics_request();
        });
        diagnostics.unwrap();
        for reprocess in [false, true] {
            let result = if reprocess {
                handle.reprocess_job(job_id).await
            } else {
                handle.redownload_job(job_id).await
            };
            assert!(matches!(result, Err(SchedulerError::Conflict(_))));
        }

        // A job that completes while deletion drains must survive delete-all's completion.
        let newer_job = JobId(30026);
        insert_active_job(
            &mut pipeline,
            newer_job,
            standalone_job_spec("Newer completion", &[("new.bin".into(), 1)]),
        )
        .await;
        pipeline.jobs.get_mut(&newer_job).unwrap().status = JobStatus::Complete;
        drop(running);
        let outcome = deletion.await.unwrap();
        assert!(outcome.refused.is_empty());
        assert!(outcome.deleted.contains_key(&job_id));
        assert_eq!(outcome.deleted.contains_key(&other_job), delete_all);
        assert!(!output_dir.exists());
        assert!(pipeline.jobs.contains_key(&newer_job));
        assert!(
            pipeline
                .shared_state
                .queue
                .read(|inner| inner.history_operations.is_empty())
        );
    }
}

#[tokio::test]
async fn delete_history_batch_keeps_complete_output_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let failed_job_id = JobId(30021);
    let complete_job_id = JobId(30022);
    let failed_output_dir = intermediate_dir.join("failed-history-job");
    let complete_output_dir = complete_dir.join("complete-history-job");

    tokio::fs::create_dir_all(&failed_output_dir).await.unwrap();
    tokio::fs::write(
        crate::jobs::working_dir::working_dir_marker_path(&failed_output_dir),
        [],
    )
    .await
    .unwrap();
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

    let outcome =
        delete_history_batch(&mut pipeline, &[failed_job_id, complete_job_id], false).await;
    assert!(outcome.refused.is_empty());

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
    pipeline.record_job_history(job_id, None);
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
            .finished_jobs()
            .await
            .into_iter()
            .any(|job| job.job_id == job_id)
    );
}

#[tokio::test]
async fn record_job_history_caps_finished_job_runtime_cache() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let base_job_id = 900_000;
    pipeline.shared_state.queue.write(|inner| {
        let history = (0..crate::jobs::FINISHED_JOBS_RUNTIME_CAP)
            .map(|offset| finished_job_info(JobId(base_job_id + offset as u64)))
            .collect::<Vec<_>>()
            .into();
        pipeline.shared_state.queue.replace_history(inner, history);
        crate::pipeline::queue::Change::changed(())
    });
    let evicted_job_id = JobId(base_job_id + crate::jobs::FINISHED_JOBS_RUNTIME_CAP as u64 - 1);

    let job_id = JobId(base_job_id + crate::jobs::FINISHED_JOBS_RUNTIME_CAP as u64 + 1);
    let spec = standalone_job_spec(
        "Finished Runtime Cap",
        &[("finished-runtime-cap.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Complete;

    pipeline.record_job_history(job_id, None);
    pipeline.db.flush_write_queue().await.unwrap();

    assert_eq!(
        pipeline.finished_jobs().await.len(),
        crate::jobs::FINISHED_JOBS_RUNTIME_CAP
    );
    assert_eq!(
        pipeline.finished_jobs().await.first().unwrap().job_id,
        job_id
    );
    assert!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
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
    pipeline.record_job_history(job_id, None);
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
    pipeline.record_job_history(job_id, None);
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

// Park the serialized database writer until the returned sender is dropped, so
// a queued archive cannot commit while the test inspects the event stream.
fn hold_database_writer(pipeline: &Pipeline) -> std::sync::mpsc::Sender<()> {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    pipeline
        .db
        .try_queue_write("test_hold_database_writer", move |_db| {
            let _ = release_rx.recv();
            Ok(())
        })
        .unwrap();
    release_tx
}

async fn wait_for_job_event(
    events: &mut broadcast::Receiver<PipelineEvent>,
    is_expected: impl Fn(&PipelineEvent) -> bool,
) {
    async {
        loop {
            let event = events.recv().await.expect("pipeline event channel closed");
            if is_expected(&event) {
                return;
            }
        }
    }
    .await;
}

#[tokio::test]
async fn job_completed_event_publishes_after_history_archive_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30040);
    let spec = standalone_job_spec(
        "Durable Completion Event",
        &[("durable-completion.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let release = hold_database_writer(&pipeline);
    pipeline.complete_job_after_terminal_post_processing(job_id);

    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobCompleted { .. })),
        "completion must not be announced while the history archive is still queued"
    );
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id: id } if *id == job_id),
    )
    .await;

    // The facade refuses history lookups for jobs the scheduler still lists as
    // live, so the finished snapshot has to be published by the time the event
    // is observable.
    assert!(
        pipeline
            .shared_state
            .list_jobs()
            .iter()
            .all(|info| info.job_id != job_id || is_terminal_status(&info.status)),
        "completed job must be published as finished before its terminal event"
    );

    // Deliberately no flush_write_queue: a subscriber that reacts to the
    // terminal event must find the row through the same reads the history
    // facade uses.
    let archived = pipeline.db.get_job_history(job_id.0).unwrap();
    assert_eq!(
        archived.map(|row| row.status),
        Some("complete".to_string()),
        "history row must be readable as soon as the completion event is observed"
    );
    let listed = pipeline
        .db
        .list_job_history(&crate::HistoryFilter::default())
        .unwrap();
    assert!(listed.iter().any(|row| row.job_id == job_id.0));
}

#[tokio::test]
async fn job_failed_event_publishes_after_history_archive_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30041);
    let spec = standalone_job_spec(
        "Durable Failure Event",
        &[("durable-failure.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let release = hold_database_writer(&pipeline);
    pipeline.fail_job(job_id, "durable failure".to_string());

    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobFailed { .. })),
        "failure must not be announced while the history archive is still queued"
    );
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobFailed { job_id: id, .. } if *id == job_id),
    )
    .await;

    let archived = pipeline.db.get_job_history(job_id.0).unwrap();
    assert_eq!(
        archived.map(|row| row.status),
        Some("failed".to_string()),
        "history row must be readable as soon as the failure event is observed"
    );
    let listed = pipeline
        .db
        .list_job_history(&crate::HistoryFilter::default())
        .unwrap();
    assert!(listed.iter().any(|row| row.job_id == job_id.0));
}

#[tokio::test]
async fn failed_history_row_keeps_why_the_job_failed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let cases = [
        (
            30051,
            JobStatus::Repairing,
            "not enough recovery blocks",
            crate::history::HistoryFailureKind::Repair,
        ),
        (
            30052,
            JobStatus::Extracting,
            "invalid password for encrypted archive",
            crate::history::HistoryFailureKind::Password,
        ),
        (
            30053,
            JobStatus::Extracting,
            "write failed: No space left on device (os error 28)",
            crate::history::HistoryFailureKind::DiskFull,
        ),
        (
            30054,
            JobStatus::Extracting,
            "unexpected end of archive",
            crate::history::HistoryFailureKind::Unpack,
        ),
        (
            30055,
            JobStatus::Moving,
            "could not move the output",
            crate::history::HistoryFailureKind::Move,
        ),
        (
            30056,
            JobStatus::Downloading,
            "too many articles are missing",
            crate::history::HistoryFailureKind::Health,
        ),
    ];
    for (id, status, error, expected) in cases {
        let job_id = JobId(id);
        let spec = standalone_job_spec(
            &format!("Failure Kind {id}"),
            &[(format!("failure-kind-{id}.mkv"), 123)],
        );
        insert_active_job(&mut pipeline, job_id, spec).await;
        pipeline.jobs.get_mut(&job_id).unwrap().status = status;
        pipeline.fail_job(job_id, error.to_string());
        wait_for_job_event(
            &mut events,
            |event| matches!(event, PipelineEvent::JobFailed { job_id: failed, .. } if *failed == job_id),
        )
        .await;
        let states = pipeline.db.nzbget_history_states(&[id]).unwrap();
        assert_eq!(states[&id].failure_kind, Some(expected), "{error}");
    }
}

async fn update_job(pipeline: &mut Pipeline, job_id: JobId, update: crate::jobs::JobUpdate) {
    pipeline.report_all_for_test();
    pipeline
        .shared_state
        .queue
        .update_job(job_id, update)
        .unwrap();
    pipeline.apply_queue_intents().await;
}

fn rename(name: &str) -> crate::jobs::JobUpdate {
    crate::jobs::JobUpdate {
        name: Some(name.to_string()),
        ..Default::default()
    }
}

#[tokio::test]
async fn renaming_a_quiet_job_preserves_existing_payload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    for (id, written) in [(30061, false), (30062, true)] {
        let job_id = JobId(id);
        let spec = standalone_job_spec(
            &format!("Before Rename {id}"),
            &[(format!("rename-{id}.mkv"), 123)],
        );
        let old_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        crate::jobs::working_dir::mark_weaver_owned_working_dir(
            &intermediate_dir,
            &old_dir,
            job_id,
        )
        .unwrap();
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Queued;
        state.download_state = crate::jobs::model::DownloadState::Queued;
        if written {
            std::fs::write(old_dir.join("rename.part"), b"written").unwrap();
        }

        let new_name = format!("After Rename {id}");
        update_job(&mut pipeline, job_id, rename(&new_name)).await;

        let state = &pipeline.jobs[&job_id];
        assert_eq!(state.spec.name, new_name);
        assert_eq!(
            crate::ingest::completed_folder_name(&state.spec.name, &state.spec.metadata),
            new_name
        );
        assert_eq!(state.working_dir, intermediate_dir.join(&new_name));
        assert!(!old_dir.exists());
        assert!(crate::jobs::working_dir::is_weaver_owned_working_dir(
            &state.working_dir
        ));
        if written {
            assert_eq!(
                std::fs::read(state.working_dir.join("rename.part")).unwrap(),
                b"written"
            );
        }
    }
}

#[tokio::test]
async fn a_rename_survives_restore_through_the_metadata() {
    let mut metadata = vec![("drone".to_string(), "abc".to_string())];
    crate::ingest::set_renamed_title(&mut metadata, "Renamed Release");
    let nzb = weaver_nzb::parse_nzb(&sample_nzb_bytes()).unwrap();
    let spec =
        crate::ingest::nzb_to_spec(&nzb, std::path::Path::new("Original.nzb"), None, metadata);
    assert_eq!(spec.name, "Renamed Release");
    assert_eq!(
        crate::ingest::original_release_title(&spec.name, &spec.metadata),
        "Renamed Release"
    );
}

#[tokio::test]
async fn parking_a_job_keeps_its_working_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30063);
    let spec = standalone_job_spec("Parked Job", &[("parked.mkv".to_string(), 123)]);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    std::fs::write(working_dir.join("parked.part"), b"kept").unwrap();
    let done = cancel_one_job(
        &mut pipeline,
        job_id,
        crate::jobs::handle::CancellationOrigin::UserKeepingFiles,
    )
    .await;
    done.unwrap();
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert_eq!(
        pipeline
            .db
            .get_job_history(job_id.0)
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    // Nothing was scheduled to remove the directory.
    assert!(working_dir.join("parked.part").exists());
}

#[tokio::test]
async fn updating_history_edits_the_row_and_the_finished_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30064);
    let spec = standalone_job_spec("Finished Job", &[("finished.mkv".to_string(), 123)]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.fail_job(job_id, "too many articles are missing".to_string());
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobFailed { job_id: failed, .. } if *failed == job_id),
    )
    .await;

    let update = crate::jobs::JobUpdate {
        name: Some("Renamed Finished".to_string()),
        category: crate::jobs::FieldUpdate::Set("tv".to_string()),
        ..Default::default()
    };
    pipeline
        .shared_state
        .queue
        .clone()
        .update_history(job_id, update)
        .await
        .unwrap();

    let row = pipeline.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(row.name, "Renamed Finished");
    assert_eq!(row.category.as_deref(), Some("tv"));
    let metadata = crate::history::parse_history_metadata(row.metadata.as_deref());
    assert_eq!(
        crate::ingest::renamed_title(&metadata),
        Some("Renamed Finished")
    );
    let finished = pipeline.shared_state.get_job(job_id).unwrap();
    assert_eq!(finished.name, "Renamed Finished");
    assert_eq!(finished.category.as_deref(), Some("tv"));

    // A job still in the queue is not a history row; an unknown id is not
    // found.
    let queued = JobId(30065);
    let spec = standalone_job_spec("Still Queued", &[("queued.mkv".to_string(), 123)]);
    insert_active_job(&mut pipeline, queued, spec).await;
    for (id, conflict) in [(queued, true), (JobId(30066), false)] {
        pipeline.report_all_for_test();
        let error = pipeline
            .shared_state
            .queue
            .clone()
            .update_history(id, rename("Nope"))
            .await
            .unwrap_err();
        if conflict {
            assert!(
                matches!(error, crate::SchedulerError::Conflict(_)),
                "{error}"
            );
        } else {
            assert!(
                matches!(error, crate::SchedulerError::JobNotFound(_)),
                "{error}"
            );
        }
    }
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

    let run = |end| {
        crate::jobs::assembly::FileCoverage::from_ordered_runs([
            crate::jobs::assembly::CoverageRun {
                first_ordinal: 0,
                ordinals: 1,
                start: 0,
                end,
            },
        ])
    };
    let file_progress = HashMap::from([(0u32, run(40)), (1u32, run(100))]);

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
        retire_pipeline_database(pipeline).await;
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
