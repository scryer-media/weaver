// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::queries::JobsQueries;
use crate::post_processing::instances::PostProcessingInstancesQueries;
use crate::post_processing::settings::PostProcessingSettingsQueries;
use crate::tags::TagsQueries;

#[tokio::test]
async fn metrics_refresh_advances_without_pipeline_turns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    tokio::time::pause();
    // Bytes move only while the actor says there is something to drive.
    pipeline.shared_state.queue.set_driving(true);
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let mut changes = handle.subscribe_job_changes();
    for downloaded in [1, 2, 99] {
        let watermark = *changes.borrow_and_update();
        pipeline
            .metrics
            .bytes_downloaded
            .store(downloaded, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(250)).await;
        while pipeline.shared_state.metrics_snapshot().bytes_downloaded != downloaded {
            changes.changed().await.unwrap();
        }
        assert!(*changes.borrow() > watermark);
    }
}

#[tokio::test]
async fn moving_progress_advances_without_another_pipeline_turn() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    tokio::time::pause();
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let job_id = JobId(99);
    pipeline.shared_state.queue.write(|inner| {
        inner.push_job(crate::pipeline::queue::JobRow::new(finished_job_info(
            job_id,
        )));
        crate::pipeline::queue::Change::changed(())
    });
    assert!(!pipeline.shared_state.queue.driving());
    let counters = pipeline.phase_begin(job_id, JobPhase::Moving, Some(100));
    assert!(pipeline.shared_state.queue.driving());
    let mut changes = handle.subscribe_job_changes();
    for completed in [25, 60, 100] {
        counters.completed_bytes.store(completed, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(250)).await;
        loop {
            let phase = pipeline
                .shared_state
                .get_job(job_id)
                .unwrap()
                .phase_progress;
            if phase[0].completed_bytes == completed {
                assert_eq!(phase[0].phase, JobPhase::Moving);
                assert_eq!(phase[0].total_bytes, 100);
                assert_eq!(phase[0].progress_percent, completed as f32);
                break;
            }
            changes.changed().await.unwrap();
        }
    }
}

#[tokio::test]
async fn phase_end_removes_only_the_ended_phase_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(99);

    pipeline.shared_state.queue.write(|inner| {
        inner.push_job(crate::pipeline::queue::JobRow::new(finished_job_info(
            job_id,
        )));
        crate::pipeline::queue::Change::changed(())
    });
    let repairing = pipeline.phase_begin(job_id, JobPhase::Repairing, Some(100));
    repairing.completed_bytes.store(25, Ordering::Relaxed);
    let extracting = pipeline.phase_begin(job_id, JobPhase::Extracting, Some(200));
    extracting.completed_bytes.store(50, Ordering::Relaxed);
    pipeline.shared_state.queue.sample_counters(
        &pipeline.metrics,
        &pipeline.event_tx,
        std::time::Instant::now(),
    );
    assert_eq!(
        pipeline
            .shared_state
            .get_job(job_id)
            .unwrap()
            .phase_progress
            .len(),
        2
    );
    pipeline.phase_end(job_id, JobPhase::Extracting);
    let remaining = pipeline
        .shared_state
        .get_job(job_id)
        .unwrap()
        .phase_progress;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].phase, JobPhase::Repairing);
    pipeline.clear_job_phase_progress_runtime(job_id);
    assert!(
        pipeline
            .shared_state
            .get_job(job_id)
            .unwrap()
            .phase_progress
            .is_empty()
    );
}

#[tokio::test]
async fn a_submitted_nzb_takes_its_tags() {
    let harness = TestHarness::new().await;
    let submitted = crate::ingest::submit_nzb_bytes_with_options(
        &harness.db,
        &harness.handle,
        &harness.config,
        &sample_nzb_bytes(),
        Some("Tagged.nzb".to_string()),
        None,
        None,
        Vec::new(),
        crate::ingest::SubmissionOptions {
            tags: vec![" tv ".to_string(), "".to_string(), "HD".to_string()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(harness.db.job_tags(submitted.job_id).unwrap(), ["HD", "tv"]);

    harness.shutdown().await;
}

#[tokio::test]
async fn submit_nzb_persists_zstd_and_creates_active_job() {
    let harness = TestHarness::new().await;
    let nzb_bytes = sample_nzb_bytes();

    let submitted = submit_nzb_bytes(
        &harness.db,
        &harness.handle,
        &harness.config,
        &nzb_bytes,
        Some("Silver Horizon.Sample.nzb".to_string()),
        None,
        None,
        vec![("source".to_string(), "test".to_string())],
    )
    .await
    .unwrap();

    wait_until(|| {
        harness
            .db
            .load_active_jobs()
            .map(|jobs| jobs.contains_key(&submitted.job_id))
            .unwrap_or(false)
    })
    .await;

    let info = harness.handle.get_job(submitted.job_id).unwrap();
    assert_eq!(info.status, JobStatus::Queued);
    assert!(
        info.metadata
            .contains(&("source".to_string(), "test".to_string()))
    );
    assert!(info.metadata.iter().any(|(key, value)| {
        key == "weaver.original_title" && value == "Silver Horizon.Sample"
    }));

    let (stored_path, stored_nzb) = harness
        .db
        .load_active_job_persisted_nzb(submitted.job_id)
        .unwrap()
        .and_then(|(path, nzb_zstd)| nzb_zstd.map(|nzb_zstd| (path, nzb_zstd)))
        .unwrap();
    assert_eq!(stored_path, PathBuf::from("Silver Horizon.Sample.nzb"));
    assert!(stored_nzb.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]));

    let decoded = crate::ingest::decode_persisted_nzb_bytes(&stored_nzb).unwrap();
    assert_eq!(decoded, nzb_bytes);

    harness.shutdown().await;
}

#[tokio::test]
async fn rename_preserves_an_existing_working_directory_collision() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10067);
    let old_dir = intermediate_dir.join("original");
    std::fs::create_dir(&old_dir).unwrap();
    crate::jobs::working_dir::mark_weaver_owned_working_dir(&intermediate_dir, &old_dir, job_id)
        .unwrap();
    let mut state = minimal_job_state(job_id, "renamed", old_dir.clone());
    state.download_state = crate::jobs::model::DownloadState::Queued;
    pipeline.jobs.insert(job_id, state);

    std::fs::create_dir(intermediate_dir.join("renamed")).unwrap();
    let collision = intermediate_dir.join("renamed.#10067");
    std::fs::create_dir(&collision).unwrap();
    std::fs::write(collision.join("keep.bin"), b"another job's data").unwrap();

    assert!(
        !pipeline
            .rename_working_dir_when_quiet(job_id, "renamed")
            .await
    );

    assert_eq!(
        std::fs::read(collision.join("keep.bin")).unwrap(),
        b"another job's data"
    );
    assert_eq!(pipeline.jobs[&job_id].working_dir, old_dir);
    assert!(crate::jobs::working_dir::is_weaver_owned_working_dir(
        &old_dir
    ));
}

// Each failure in a row doubles the wait before the next try, up to a cap.
#[test]
fn a_rename_backoff_doubles_up_to_its_cap() {
    let now = tokio::time::Instant::now();
    let mut retry = crate::pipeline::RenameRetry::default();
    let mut waits = Vec::new();
    for _ in 0..12 {
        retry.failed(now);
        waits.push((retry.retry_at.unwrap() - now).as_secs());
    }
    assert_eq!(waits, [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 300]);
}

// A rename that fails is not tried again until its backoff runs out, and a
// job busy with a move is renamed by the first turn after the move ends.
// Neither leaves a walk over the queue due.
#[tokio::test]
async fn a_failed_rename_waits_out_its_backoff_and_a_busy_one_needs_no_walk() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10068);
    let old_dir = intermediate_dir.join("original");
    std::fs::create_dir(&old_dir).unwrap();
    crate::jobs::working_dir::mark_weaver_owned_working_dir(&intermediate_dir, &old_dir, job_id)
        .unwrap();
    let mut state = minimal_job_state(job_id, "renamed", old_dir.clone());
    state.download_state = crate::jobs::model::DownloadState::Queued;
    pipeline.jobs.insert(job_id, state);
    pipeline.shared_state.queue.write(|inner| {
        let mut row = crate::pipeline::queue::JobRow::new(finished_job_info(job_id));
        row.intents.rename_to = Some("renamed".to_string());
        inner.push_job(row);
        crate::pipeline::queue::Change::changed(())
    });
    std::fs::create_dir(intermediate_dir.join("renamed")).unwrap();
    let collision = intermediate_dir.join("renamed.#10068");
    std::fs::create_dir(&collision).unwrap();

    let before = tokio::time::Instant::now();
    pipeline.pending_renames.entry(job_id).or_default();
    pipeline.retry_pending_renames().await;
    let retry = &pipeline.pending_renames[&job_id];
    assert_eq!(retry.failures, 1);
    let retry_at = retry.retry_at.expect("a backoff after the failure");
    assert!(retry_at >= before + std::time::Duration::from_secs(1));
    assert_eq!(pipeline.next_rename_retry_at(), Some(retry_at));
    assert!(!pipeline.queue_walk_due);

    // Inside the backoff nothing is tried.
    let far = tokio::time::Instant::now() + std::time::Duration::from_secs(3600);
    pipeline.pending_renames.get_mut(&job_id).unwrap().retry_at = Some(far);
    pipeline.retry_pending_renames().await;
    assert_eq!(pipeline.pending_renames[&job_id].failures, 1);
    assert_eq!(pipeline.pending_renames[&job_id].retry_at, Some(far));

    // Once it runs out the rename is tried, and fails again.
    pipeline.pending_renames.get_mut(&job_id).unwrap().retry_at = Some(tokio::time::Instant::now());
    pipeline.retry_pending_renames().await;
    assert_eq!(pipeline.pending_renames[&job_id].failures, 2);

    // A move holds the folder: the turn passes without a try or a walk.
    std::fs::remove_dir(intermediate_dir.join("renamed")).unwrap();
    std::fs::remove_dir(&collision).unwrap();
    pipeline.pending_renames.get_mut(&job_id).unwrap().retry_at = Some(tokio::time::Instant::now());
    pipeline.inflight_moves.insert(job_id);
    pipeline.retry_pending_renames().await;
    assert_eq!(pipeline.pending_renames[&job_id].failures, 2);
    assert_eq!(pipeline.pending_renames[&job_id].retry_at, None);
    assert_eq!(pipeline.next_rename_retry_at(), None);
    assert!(!pipeline.queue_walk_due);
    assert_eq!(pipeline.jobs[&job_id].working_dir, old_dir);

    pipeline.inflight_moves.remove(&job_id);
    pipeline.retry_pending_renames().await;
    assert!(pipeline.pending_renames.is_empty());
    assert_eq!(
        pipeline.jobs[&job_id].working_dir,
        intermediate_dir.join("renamed")
    );
    let asked = pipeline.shared_state.queue.read(|inner| {
        inner
            .job(job_id)
            .map(|row| row.intents.rename_to.clone())
            .expect("the job's row")
    });
    assert_eq!(asked, None);
}

#[tokio::test]
async fn move_to_complete_uses_unique_destination_for_duplicate_job_names() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10067);
    let job_name = "Silver Horizon Beyond the Vale";
    let payload_dir =
        "Silver Horizon.Beyond.Journeys.End.S01.1080p.BluRay.Opus2.0.x265.DUAL-Anitsu";
    let episode_name = "Silver Horizon.Beyond.Journeys.End.S01E01.mkv";

    let existing_dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(job_name));
    tokio::fs::create_dir_all(existing_dest.join(payload_dir))
        .await
        .unwrap();
    tokio::fs::write(
        existing_dest.join(payload_dir).join(episode_name),
        b"existing",
    )
    .await
    .unwrap();

    let working_dir = intermediate_dir.join(format!(
        "{}.#{}",
        crate::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));
    tokio::fs::create_dir_all(working_dir.join(payload_dir))
        .await
        .unwrap();
    tokio::fs::write(working_dir.join(payload_dir).join(episode_name), b"new")
        .await
        .unwrap();

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, job_name, working_dir.clone()),
    );
    pipeline.job_order.push(job_id);
    pipeline.report_all_for_test();
    pipeline.start_move_to_complete(job_id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let dest = done.dest.clone();
    assert!(done.result.is_ok());
    pipeline.handle_move_to_complete_done(done).await;
    settle_script_admissions(&mut pipeline).await;
    let expected_dest = complete_dir.join(format!(
        "{}.#{}",
        crate::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));

    assert_eq!(dest, expected_dest);
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .any(|job| job.job_id == job_id
                && job.output_dir.as_deref() == Some(expected_dest.to_str().unwrap()))
    );
    assert!(!working_dir.exists());
    assert_eq!(
        tokio::fs::read(dest.join(payload_dir).join(episode_name))
            .await
            .unwrap(),
        b"new"
    );
    assert_eq!(
        tokio::fs::read(existing_dest.join(payload_dir).join(episode_name))
            .await
            .unwrap(),
        b"existing"
    );
}

#[tokio::test]
async fn completion_check_during_post_processing_does_not_finalize_again() {
    for status in [JobStatus::QueuedPostProcessing, JobStatus::PostProcessing] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(10070);
        let job_name = "Copper Lantern Late Completion Check";
        let working_dir = intermediate_dir.join("copper-lantern");
        tokio::fs::create_dir_all(&working_dir).await.unwrap();
        tokio::fs::write(working_dir.join("payload.mkv"), b"payload")
            .await
            .unwrap();
        let mut state = minimal_job_state(job_id, job_name, working_dir.clone());
        state.status = status.clone();
        pipeline.jobs.insert(job_id, state);

        // A late completion check landing while scripts run must not run the
        // final move a second time: that claims `<name>.#<id>` and repoints the
        // job's output directory out from under the running script.
        pipeline.check_job_completion(job_id).await;
        settle_inflight_moves(&mut pipeline).await;

        assert_eq!(job_status_for_assert(&pipeline, job_id).unwrap(), status);
        assert!(working_dir.join("payload.mkv").exists());
        let mut entries = tokio::fs::read_dir(&complete_dir).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "{status:?}: completion check published into {}",
            complete_dir.display()
        );
    }
}

#[tokio::test]
async fn unacceptable_extension_rejection_never_starts_a_final_move_or_scripts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10069);
    let job_name = "Rejected Before Publication";
    pipeline
        .db
        .save_post_processing_settings(&crate::post_processing::model::PostProcessingSettings {
            execution_enabled: true,
            unacceptable_extensions: vec!["exe".into()],
            unwanted_extension_action:
                crate::post_processing::model::UnwantedExtensionAction::FailJob,
            ..Default::default()
        })
        .unwrap();
    pipeline
        .db
        .create_script_instance(crate::post_processing::instances::ScriptInstanceDraft::new(
            crate::post_processing::model::ScriptName::new("sentinel.sh").unwrap(),
            crate::post_processing::instances::InstanceTrigger::PostProcessing,
        ))
        .unwrap();

    let working_dir = intermediate_dir.join("rejected-before-publication");
    tokio::fs::create_dir_all(working_dir.join("nested"))
        .await
        .unwrap();
    tokio::fs::write(working_dir.join("nested/payload.EXE"), b"rejected")
        .await
        .unwrap();
    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, job_name, working_dir.clone()),
    );
    pipeline.job_order.push(job_id);
    pipeline.report_all_for_test();
    pipeline.start_move_to_complete(job_id).await.unwrap();

    settle_inflight_moves(&mut pipeline).await;
    assert!(pipeline.inflight_terminal_post_processing.is_empty());
    assert!(
        !complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(job_name))
            .exists()
    );
    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    let JobStatus::Failed { error } = status else {
        panic!("unacceptable extension must fail the job");
    };
    assert_eq!(error, "unwanted extension '.exe' in 'nested/payload.EXE'");
    assert!(
        pipeline
            .db
            .job_post_processing_summary(job_id.0)
            .unwrap()
            .is_none()
    );
    assert!(
        working_dir.exists(),
        "the rejected source was never published"
    );
}

#[tokio::test]
async fn hard_write_pressure_latches_until_soft_watermark() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1000;

    pipeline
        .metrics
        .write_buffered_bytes
        .store(1000, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Hard.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::Write.as_code()
    );

    pipeline
        .metrics
        .write_buffered_bytes
        .store(701, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Hard.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_stalls_total
            .load(Ordering::Relaxed),
        1
    );

    pipeline
        .metrics
        .write_buffered_bytes
        .store(699, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Clear.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::None.as_code()
    );
}

#[tokio::test]
async fn delayed_retry_carries_exclusions_across_a_pool_rebuild() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(hosts_client(&[0, 1]));
    let job_id = JobId(40041);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let segment_id = SegmentId {
        file_id,
        segment_number: 0,
    };
    let spec = JobSpec {
        name: "Stale Exclusion Rebuild".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "payload.bin".to_string(),
            role: FileRole::from_filename("payload.bin"),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 128,
                message_id: "stale-exclusion@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
    }

    let make_work = || DownloadWork {
        counters: None,
        segment_id,
        message_id: MessageId::new("stale-exclusion@example.com"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 100,
        byte_estimate: 128,
        retry_count: 1,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: weaver_nntp::ServerSet::from([0]),
        avoid_server: None,
    };

    // A rebuild swapped the two servers after this retry was scheduled under
    // gen 0: server 0 of that pool is server 1 of the new one, and the retry
    // keeps it tried under that number.
    pipeline.activate_nntp(hosts_client(&[1, 0]), 4, 1);
    pipeline.note_retry_scheduled(segment_id);
    pipeline.receive_retry_work(super::RetryWork {
        scheduled_pool_generation: 0,
        infrastructure_retry: false,
        hold: crate::pipeline::RetryHold::Ordinary,
        work: make_work(),
    });
    let queued = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .expect("stale retry should still requeue");
    assert_eq!(queued.segment_id, segment_id);
    assert_eq!(
        queued.exclude_servers,
        [1],
        "server exclusions are carried by server identity across a pool rebuild"
    );

    // Scheduled under a generation no layout is held for: nothing can be
    // read from its numbers, so the retry tries every server.
    pipeline.note_retry_scheduled(segment_id);
    pipeline.receive_retry_work(super::RetryWork {
        scheduled_pool_generation: 77,
        infrastructure_retry: false,
        hold: crate::pipeline::RetryHold::Ordinary,
        work: make_work(),
    });
    let queued = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .expect("an unknown-generation retry should still requeue");
    assert!(queued.exclude_servers.is_empty());

    // No rebuild since scheduling: exclusions are still valid and preserved.
    pipeline.note_retry_scheduled(segment_id);
    pipeline.receive_retry_work(super::RetryWork {
        scheduled_pool_generation: 1,
        infrastructure_retry: false,
        hold: crate::pipeline::RetryHold::Ordinary,
        work: make_work(),
    });
    let queued = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .expect("same-generation retry should requeue");
    assert_eq!(
        queued.exclude_servers,
        vec![0],
        "current-generation exclusions must be preserved"
    );
}

#[tokio::test]
async fn restored_post_processing_is_interrupted_without_rerunning_scripts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31237);
    let spec = JobSpec {
        name: "interrupted-post-processing".to_string(),
        password: None,
        files: vec![],
        total_bytes: 0,
        category: None,
        metadata: vec![],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::PostProcessing;
    // The executor stamps this marker before the first script runs.
    pipeline
        .db
        .mark_job_post_processing_running(job_id.0)
        .unwrap();
    // The startup scan is the whole recovery: a `running` marker can only have
    // been left behind by a process that died mid-pass.
    assert_eq!(
        pipeline.db.recover_interrupted_post_processing().unwrap(),
        1
    );
    pipeline.schedule_job_completion_check(job_id);

    assert!(pipeline.recover_restored_terminal_post_processing(
        job_id,
        Some(crate::pipeline::RestoredPostProcessing::read(
            &pipeline.db,
            job_id,
            None
        ))
    ));
    settle_failed_releases(&mut pipeline).await;

    assert!(!pipeline.pending_completion_checks.contains(&job_id));
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(matches!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .find(|job| job.job_id == job_id)
            .map(|job| job.status),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn restored_post_processing_that_already_finished_archives_as_complete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31238);
    insert_delivered_job(
        &mut pipeline,
        job_id,
        "completed-post-processing",
        "payload.mkv",
        b"payload",
    )
    .await;
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::PostProcessing;
    pipeline
        .db
        .save_job_post_processing_results(
            job_id.0,
            crate::post_processing::model::PostProcessingSummary::Succeeded,
            &[],
        )
        .unwrap();
    // A job whose summary is already terminal is not swept by the scan.
    assert_eq!(
        pipeline.db.recover_interrupted_post_processing().unwrap(),
        0
    );
    pipeline.schedule_job_completion_check(job_id);

    assert!(pipeline.recover_restored_terminal_post_processing(
        job_id,
        Some(crate::pipeline::RestoredPostProcessing::read(
            &pipeline.db,
            job_id,
            None
        ))
    ));

    assert!(!pipeline.pending_completion_checks.contains(&job_id));
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(matches!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .find(|job| job.job_id == job_id)
            .map(|job| job.status),
        Some(JobStatus::Complete)
    ));
}

// The wire frame regression guard: BODY must carry the **bracketed**
// message-id.
//
// `DownloadWork::message_id` stores the bare id (the NZB parser strips the
// brackets), so a lane that borrows it directly emits `BODY segment-0@…`.
// That is a legal article-*number* reference, and every real provider answers
// 430 to it — a total, silent download failure. The fake provider here is
// strict about brackets, so this test fails outright on a regression.
#[tokio::test]
async fn failed_final_move_marks_job_failed_instead_of_complete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10068);
    let job_name = "Silver Horizon Broken Final Move";
    let working_dir =
        insert_delivered_job(&mut pipeline, job_id, job_name, "payload.mkv", b"payload").await;
    // Nothing is left to deliver from when the move runs.
    std::fs::remove_dir_all(&working_dir).unwrap();
    hear_graph_mail(&mut pipeline, job_id).await;
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(matches!(status, JobStatus::Failed { .. }), "{status:?}");
    let JobStatus::Failed { error } = &status else {
        unreachable!();
    };
    assert!(error.contains("no delivery source exists"), "{error}");
    assert!(
        !complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(job_name))
            .exists()
    );
}

#[tokio::test]
async fn sparse_article_numbers_commit_cleanly_with_dense_ordinals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20009);
    let filename = "sparse.bin";
    let payload_size = 128u32;
    let total_size = payload_size as u64 * 3;
    let spec = JobSpec {
        name: "Sparse Segment Ordinals".to_string(),
        password: None,
        total_bytes: total_size,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    ordinal: 0,
                    article_number: 1,
                    bytes: payload_size,
                    message_id: "sparse-0@example.com".to_string(),
                },
                segment_spec! {
                    ordinal: 1,
                    article_number: 4,
                    bytes: payload_size,
                    message_id: "sparse-1@example.com".to_string(),
                },
                segment_spec! {
                    ordinal: 2,
                    article_number: 6,
                    bytes: payload_size,
                    message_id: "sparse-2@example.com".to_string(),
                },
            ],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    for _ in 0..3u32 {
        // Each article is fetched as dispatch hands it out.
        let work = pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .pop()
            .unwrap();
        let segment_number = work.segment_id.segment_number;
        let payload = vec![segment_number as u8 + 1; payload_size as usize];
        let raw = encode_article_part(
            filename,
            &payload,
            segment_number + 1,
            3,
            segment_number as u64 * payload_size as u64 + 1,
            total_size,
        );

        pipeline.articles_in_flight += 1;
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id,
                runtime_generation: 0,
                segment_id: work.segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: true,
            })
            .await;
    }

    drain_decode_results(&mut pipeline, 3).await;
    // The graph job's runs and its completion check settle it.
    hear_graph_mail(&mut pipeline, job_id).await;
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        3
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}
