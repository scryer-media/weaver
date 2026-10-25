// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A server refusing the credentials has answered for the articles it was
// asked for; a server quarantined for transport or capacity has not. The
// first is a verdict, booked; the second is a wait.

use super::*;

// Admit the job the way a submission is admitted, so its durable candidate
// row records the terminal cause the archive writes.
fn admitted_job_id(pipeline: &Pipeline, spec: &JobSpec, hash: u8) -> JobId {
    let request = crate::DuplicateAdmissionRequest {
        evidence: crate::FingerprintEvidence::from_validated_spec(spec, [hash; 32]),
        mode: crate::DuplicateMode::Score,
        semantic: crate::SemanticDuplicate::from_source(&spec.name, 10),
        semantic_source: Some(crate::SemanticCandidateSource {
            nzb_zstd: sample_nzb_zstd(),
            filename: Some(format!("{}.nzb", spec.name)),
            password: None,
            category: None,
            metadata: vec![],
        }),
        origin: crate::SubmissionOrigin::Api,
        idempotency: None,
        policy: crate::DuplicatePolicy::default(),
    };
    let crate::DuplicateAdmission::Accepted { job_id, .. } =
        pipeline.db.admit_duplicate_submission(&request).unwrap()
    else {
        panic!("the job must be admitted");
    };
    job_id
}

// Two servers: index 0 keeps five days of retention, index 1 keeps
// everything. A job ten days old can be served only by index 1, which then
// refuses the pool's credentials.
async fn refused_job(pipeline: &mut Pipeline, hash: u8, segments: usize) -> JobId {
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
    let spec = segmented_job_spec(
        &format!("Refused {hash}"),
        "payload.bin",
        &vec![128; segments],
    );
    let job_id = admitted_job_id(pipeline, &spec, hash);
    insert_active_job(pipeline, job_id, spec).await;
    age_job_days(pipeline, job_id, 10);
    pipeline.clear_job_retention_excludes(job_id);
    assert_eq!(pipeline.job_retention_excludes(job_id).as_slice(), &[0]);
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_failure(1, true);
    assert!(pipeline.nntp.pool().refuses_credentials(1));
    job_id
}

// Lease the job's first queued article to an owned lane and answer the lease
// with what the pool really says for it, the way the lane task would.
async fn lease_and_acquire(pipeline: &mut Pipeline, job_id: JobId) -> VecDeque<DownloadResult> {
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    pipeline.hold_test_lane_works(vec![work.clone()], false);
    let effective_exclude_servers =
        pipeline.effective_exclude_servers(job_id, &work.exclude_servers);
    let Err(error) = pipeline
        .nntp
        .try_acquire_blocking_body_lane(&[], &effective_exclude_servers)
    else {
        panic!("a lane opened with no server that may serve the article");
    };
    let lease = DownloadBatchLease {
        lane_id: 0,
        job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: Vec::new(),
        completion_critical: false,
        effective_exclude_servers,
        dial_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works: vec![work],
    };
    pipeline.active_download_connections = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 1);
    pipeline.note_download_lane_started(DownloadLaneMode::Sequential);
    let mut pending = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed { lease, error },
        &mut pending,
    );
    pending
}

async fn drain_completion_checks(pipeline: &mut Pipeline) {
    while let Some(job_id) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(job_id).await;
    }
}

// What the refusal makes of the job in the pass that answers it: the
// articles only the refusing server could serve are booked missing rather
// than queued again, no stall is left to watch, and the job ends failed for
// the server, not for articles missing from Usenet.
async fn assert_failed_for_the_refusal(pipeline: &mut Pipeline, job_id: JobId, segments: u64) {
    let booked = pipeline.metrics.articles_not_found.load(Ordering::Relaxed);
    assert!(booked >= 1, "the refused article is booked, not retried");
    assert_eq!(pipeline.pending_retries_by_job.get(&job_id).copied(), None);
    assert_eq!(pipeline.infrastructure_retries.len(), 0);
    if let Some(state) = pipeline.jobs.get(&job_id) {
        assert!(
            state.download_queue.is_empty(),
            "nothing left to be refused again"
        );
        assert_eq!(booked, segments);
    }
    drain_completion_checks(pipeline).await;
    pipeline.dispatch_downloads();
    assert!(pipeline.download_dispatch_stall.is_none());
    assert!(!pipeline.jobs.contains_key(&job_id), "the job is terminal");
    pipeline.db.flush_write_queue().await.unwrap();
    assert_eq!(
        pipeline
            .db
            .get_job_history(job_id.0)
            .unwrap()
            .map(|row| row.status),
        Some("failed".to_string())
    );
    assert_eq!(
        pipeline
            .db
            .semantic_candidate_snapshot(job_id)
            .unwrap()
            .and_then(|candidate| candidate.terminal_cause),
        Some(crate::jobs::SemanticTerminalCause::ServerAuthQuotaOrOutage)
    );
}

#[tokio::test]
async fn a_credentials_refusal_from_the_last_server_fails_the_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = refused_job(&mut pipeline, 60, 4).await;

    let pending = lease_and_acquire(&mut pipeline, job_id).await;
    assert_eq!(pending.len(), 1, "the refused article takes the answer");
    assert_eq!(pending[0].source_server_idx, Some(1));
    for result in pending {
        pipeline.handle_download_done(result).await;
    }

    assert_failed_for_the_refusal(&mut pipeline, job_id, 4).await;
}

#[tokio::test]
async fn a_credentials_refusal_retries_on_a_server_that_may_still_serve() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0, 0]));
    let job_id = JobId(41961);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Refused Once", "payload.bin", &[128]),
    )
    .await;
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_failure(0, true);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::Auth,
                "server 0 refuses credentials",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    // An auth refusal is an infrastructure retry: it parks for its delay,
    // and the parked work no longer names the refusing server as a
    // candidate.
    assert_eq!(pipeline.infrastructure_retries.len(), 1);
    let due = pipeline.infrastructure_retries.next_deadline().unwrap();
    let ready = pipeline.infrastructure_retries.take_due(due);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].work.exclude_servers, vec![0]);
    assert!(!pipeline.no_server_can_serve(job_id, &ready[0].work.exclude_servers));
}

#[tokio::test]
async fn a_restored_queued_job_whose_last_server_refuses_credentials_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
    let mut spec = segmented_job_spec("Refused Restored", "payload.bin", &[128, 128, 128]);
    let job_id = admitted_job_id(&pipeline, &spec, 62);
    let posted = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 10 * 86_400;
    for file in &mut spec.files {
        file.posted_at_epoch = Some(posted);
    }
    let working_dir = temp_dir.path().join("refused-restored");
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
            status: JobStatus::Queued,
            download_state: Some(crate::jobs::model::DownloadState::Queued),
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
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_failure(1, true);
    assert_eq!(pipeline.jobs[&job_id].download_queue.len(), 3);

    let pending = lease_and_acquire(&mut pipeline, job_id).await;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].source_server_idx, Some(1));
    for result in pending {
        pipeline.handle_download_done(result).await;
    }

    assert_failed_for_the_refusal(&mut pipeline, job_id, 3).await;
}
