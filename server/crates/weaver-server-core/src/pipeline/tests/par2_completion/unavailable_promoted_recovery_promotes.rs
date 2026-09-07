//! `par2_completion` tests, part of a mechanical split of the original file.

use super::*;

#[tokio::test]
async fn unavailable_promoted_recovery_promotes_next_candidate_before_failing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30094);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let first_recovery_filename = "repair.vol00+01.par2";
    let second_recovery_filename = "repair.vol01+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let first_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 2,
        },
        segment_number: 0,
    };
    let second_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 3,
        },
        segment_number: 0,
    };
    let spec = JobSpec {
        name: "Promoted Recovery Candidate Fallback".to_string(),
        password: None,
        total_bytes: original_payload.len() as u64 + par2_bytes.len() as u64 + 96,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "fallback-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "fallback-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "fallback-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: first_recovery_filename.to_string(),
                role: FileRole::from_filename(first_recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 32,
                    message_id: "fallback-recovery-small@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: second_recovery_filename.to_string(),
                role: FileRole::from_filename(second_recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "fallback-recovery-large@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state.recovery_queue.push(DownloadWork {
            segment_id: first_segment,
            message_id: MessageId::new("fallback-recovery-small@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 1000,
            byte_estimate: 32,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
        state.recovery_queue.push(DownloadWork {
            segment_id: second_segment,
            message_id: MessageId::new("fallback-recovery-large@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 0),
        &[
            (1, index_filename, 0, false),
            (2, first_recovery_filename, 1, false),
            (3, second_recovery_filename, 1, false),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    // The promotion is decided by the detached read's verdict.
    settle_par2_analysis_work(&mut pipeline).await;

    assert!(pipeline.is_promoted_recovery_file(job_id, 2));
    assert!(!pipeline.is_promoted_recovery_file(job_id, 3));
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let queued = state.download_queue.drain_all();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].segment_id, first_segment);
    }

    pipeline.mark_promoted_recovery_segment_unavailable(first_segment);
    pipeline.check_job_completion(job_id).await;
    settle_par2_analysis_work(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    assert!(pipeline.is_promoted_recovery_file(job_id, 3));
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let queued = state.download_queue.drain_all();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].segment_id, second_segment);
    }

    pipeline.mark_promoted_recovery_segment_unavailable(second_segment);
    pipeline.check_job_completion(job_id).await;
    settle_par2_analysis_work(&mut pipeline).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should fail only after promoted recovery candidates are exhausted");
    };
    assert!(error.contains("only 0 recovery blocks available in NZB"));
}

#[tokio::test]
async fn active_recovery_from_another_job_does_not_satisfy_promoted_recovery_wait() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let waiting_job_id = JobId(30095);
    let other_job_id = JobId(30096);
    let spec = segmented_job_spec("Promoted Recovery Isolation", "payload.bin", &[128]);
    insert_active_job(&mut pipeline, waiting_job_id, spec.clone()).await;
    insert_active_job(&mut pipeline, other_job_id, spec).await;
    pipeline
        .ensure_par2_runtime(waiting_job_id)
        .files
        .entry(1)
        .or_default()
        .promoted = true;

    pipeline.active_downloads_by_file.insert(
        NzbFileId {
            job_id: other_job_id,
            file_index: 1,
        },
        1,
    );
    assert!(!pipeline.promoted_recovery_file_has_pending_work(waiting_job_id, 1));

    pipeline.active_downloads_by_file.insert(
        NzbFileId {
            job_id: waiting_job_id,
            file_index: 1,
        },
        1,
    );
    assert!(pipeline.promoted_recovery_file_has_pending_work(waiting_job_id, 1));
}

#[tokio::test]
async fn direct_payload_par2_repair_fails_when_recovery_is_insufficient() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30081);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| ((value * 7) % 251) as u8).collect();
    let damaged_payload = vec![0u8; original_payload.len()];
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0x55; 64];
    let spec = JobSpec {
        name: "Direct Payload PAR2 Failure".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "payload-fail-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "payload-fail-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "payload-fail-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "payload-fail-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.note_released_download_result_pending(job_id, 512);
    pipeline.check_job_completion(job_id).await;

    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "pending released download results must defer PAR2 fail-fast"
    );

    pipeline.finish_released_download_result_processing(job_id, 512);
    pipeline.check_job_completion(job_id).await;
    // The fail-fast verdict is what the detached analysis brings back.
    settle_par2_analysis_work(&mut pipeline).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should have failed when recovery blocks are insufficient");
    };
    assert!(error.contains("not repairable"));
}

#[tokio::test]
async fn extracted_archive_job_finalizes_without_reverifying_missing_par2_index() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30084);
    let files = build_multifile_multivolume_rar_set();
    let mut spec = rar_job_spec("RAR Finalize Skips Missing PAR2 Index", &files);
    spec.total_bytes += 128;
    spec.files.push(FileSpec {
        filename: "repair.par2".to_string(),
        role: FileRole::from_filename("repair.par2"),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 64,
            message_id: "rar-finalize-par2-index@example.com".to_string(),
        }],
    });
    spec.files.push(FileSpec {
        filename: "repair.vol00+01.par2".to_string(),
        role: FileRole::from_filename("repair.vol00+01.par2"),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 64,
            message_id: "rar-finalize-par2-recovery@example.com".to_string(),
        }],
    });
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
        let file_id = NzbFileId {
            job_id,
            file_index: file_index as u32,
        };
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state
                .assembly
                .file_mut(file_id)
                .unwrap()
                .commit_segment(0, bytes.len() as u32)
                .unwrap();
        }
        pipeline
            .refresh_archive_state_for_completed_file(job_id, file_id, false)
            .await;
    }
    drain_rar_refreshes(&mut pipeline).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(&files[0].0, &files[0].1, 64, 0),
        &[],
    );

    let extraction_staging_dir = pipeline.extraction_staging_dir(job_id);
    for (member_name, bytes) in [
        ("E01.mkv", b"episode-a-payload".as_slice()),
        ("E02.mkv", b"episode-b-payload".as_slice()),
    ] {
        let (output_path, _) = Pipeline::member_output_paths(&extraction_staging_dir, member_name);
        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&output_path, bytes).await.unwrap();
        pipeline
            .db
            .add_extracted_member(job_id, member_name, &output_path)
            .unwrap();
        pipeline
            .extracted_members
            .entry(job_id)
            .or_default()
            .insert(member_name.to_string());
    }
    // This fixture models restart recovery after extraction already completed.
    // Do not leave the eager extraction workers spawned during setup in scope.
    for ((rar_job_id, _), set_state) in pipeline.rar_sets.iter_mut() {
        if *rar_job_id == job_id {
            set_state.active_workers = 0;
            set_state.in_flight_members.clear();
        }
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    for (filename, _) in &files {
        tokio::fs::remove_file(working_dir.join(filename))
            .await
            .unwrap();
    }

    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    let output_dir = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "RAR Finalize Skips Missing PAR2 Index",
    ));
    assert!(output_dir.join("E01.mkv").exists());
    assert!(output_dir.join("E02.mkv").exists());
}

#[test]
fn quick_par2_verification_uses_verifying_failpoint() {
    assert_eq!(
        Pipeline::par2_verification_started_failpoint_name(),
        crate::e2e_failpoint::STATUS_ENTER_VERIFYING
    );
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::Idle,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::Verifying,
            crate::jobs::model::RunState::Active,
        ),
        Some(Pipeline::par2_verification_started_failpoint_name())
    );
}

#[tokio::test]
async fn clean_verify_after_swap_correction_preserves_retry_frontier_after_eager_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30017);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Swap Retry Frontier", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let part03 = working_dir.join("show.part03.rar");
    let part04 = working_dir.join("show.part04.rar");
    let swap_tmp = working_dir.join("show.swap.tmp");
    tokio::fs::rename(&part03, &swap_tmp).await.unwrap();
    tokio::fs::rename(&part04, &part03).await.unwrap();
    tokio::fs::rename(&swap_tmp, &part04).await.unwrap();

    for (file_index, (filename, _)) in files.iter().enumerate() {
        let current_bytes = tokio::fs::read(working_dir.join(filename)).await.unwrap();
        persist_completed_file_hash(
            &pipeline,
            job_id,
            file_index as u32,
            filename,
            &current_bytes,
        )
        .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part01.rar"))
        .await
        .unwrap();
    tokio::fs::remove_file(working_dir.join("show.part02.rar"))
        .await
        .unwrap();

    pipeline.eagerly_deleted.insert(
        job_id,
        ["show.part01.rar".to_string(), "show.part02.rar".to_string()]
            .into_iter()
            .collect(),
    );
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should still exist after normalization retry");
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
    assert!(!plan.waiting_on_volumes.contains(&0));
    assert!(plan.waiting_on_volumes.is_disjoint(&plan.deletion_eligible));
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E02.mkv")
    );
    assert_eq!(
        plan.delete_decisions
            .get(&2)
            .expect("volume 2 decision should exist")
            .owners,
        vec!["E02.mkv".to_string()]
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental retry batch"),
    }
}

#[tokio::test]
async fn health_probe_candidates_skip_par2_segments() {
    let spec = standalone_with_par2_job_spec("Probe Candidates", 128, 64);

    let probes = Pipeline::health_probe_candidates(&spec);

    assert_eq!(probes, vec!["payload@example.com".to_string()]);
}

#[tokio::test]
async fn probe_projection_uses_only_payload_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30022);
    let spec = JobSpec {
        name: "Probe Projection".to_string(),
        password: None,
        total_bytes: 592,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "payload-a.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "payload-a@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload-b.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "payload-b@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 16,
                    message_id: "repair-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.vol00+01.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 320,
                    message_id: "repair-volume@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.activate_health_probes(job_id);
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round: 0,
        total: 2,
        missed: 1,
        done: true,
        inconclusive: false,
    });

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.probe_projected_failed_bytes, 128);
    assert_eq!(
        state.failed_bytes, 0,
        "the projection is a health signal, not a terminal-state fact, so the ledger stays empty"
    );
    assert_eq!(state.last_health_probe_failed_bytes, 128);
    assert!(matches!(state.status, JobStatus::Downloading));
}

#[tokio::test]
async fn reconcile_job_progress_leaves_terminal_recovery_to_restore_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    let spec = standalone_job_spec(
        "Restored Checking Complete",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.health_probing = false;
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.download_state = crate::jobs::model::DownloadState::Checking;
        state.refresh_legacy_status();
    }

    pipeline.reconcile_job_progress(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Checking
    ));
    assert!(matches!(state.status, JobStatus::Checking));
}

#[tokio::test]
async fn health_below_critical_without_par2_still_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30024);
    let spec = standalone_job_spec("No PAR2 Health Fail", &[("payload.bin".to_string(), 100)]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 20;
    }

    pipeline.check_health(job_id);

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn health_below_critical_with_par2_defers_to_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30025);
    let spec = JobSpec {
        name: "PAR2 Health Defers".to_string(),
        password: None,
        total_bytes: 300,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "payload-a.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 100,
                    message_id: "par2-health-a@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload-b.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 100,
                    message_id: "par2-health-b@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.vol00+01.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 100,
                    message_id: "par2-health-repair@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 200;
    }

    pipeline.check_health(job_id);

    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert!(pipeline.pending_completion_checks.contains(&job_id));
}

#[tokio::test]
async fn repair_queue_limits_to_one_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_a = JobId(31001);
    let job_b = JobId(31002);

    pipeline.jobs.insert(
        job_a,
        minimal_job_state(job_a, "repair-a", temp_dir.path().join("repair-a")),
    );
    pipeline.jobs.insert(
        job_b,
        minimal_job_state(job_b, "repair-b", temp_dir.path().join("repair-b")),
    );

    assert!(pipeline.maybe_start_repair(job_a).await);
    assert_eq!(
        pipeline.jobs.get(&job_a).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);

    assert!(!pipeline.maybe_start_repair(job_b).await);
    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);

    pipeline.transition_postprocessing_status(job_a, JobStatus::Downloading, Some("downloading"));

    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_b]
    );
}

#[tokio::test]
async fn restore_repairing_preserves_status_and_slot_ownership() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31205);
    let spec = standalone_job_spec("Restore repairing", &[("sample.bin".to_string(), 100)]);
    let working_dir = temp_dir.path().join("restore-repairing");
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
            status: JobStatus::Repairing,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: Some(42_000.0),
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.queued_repair_at_epoch_ms),
        Some(42_000.0)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
}

#[tokio::test]
async fn repair_queue_promotion_reserves_slot_and_keeps_queue_age() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_a = JobId(31211);
    let job_b = JobId(31212);
    let job_c = JobId(31213);

    pipeline.jobs.insert(
        job_a,
        minimal_job_state(job_a, "repair-a", temp_dir.path().join("repair-a")),
    );
    pipeline.jobs.insert(
        job_b,
        minimal_job_state(job_b, "repair-b", temp_dir.path().join("repair-b")),
    );
    pipeline.jobs.insert(
        job_c,
        minimal_job_state(job_c, "repair-c", temp_dir.path().join("repair-c")),
    );

    assert!(pipeline.maybe_start_repair(job_a).await);
    assert!(!pipeline.maybe_start_repair(job_b).await);
    let queued_at = pipeline
        .jobs
        .get(&job_b)
        .and_then(|state| state.queued_repair_at_epoch_ms)
        .unwrap();

    assert!(!pipeline.maybe_start_repair(job_b).await);
    assert_eq!(
        pipeline
            .jobs
            .get(&job_b)
            .and_then(|state| state.queued_repair_at_epoch_ms),
        Some(queued_at)
    );

    pipeline.transition_postprocessing_status(job_a, JobStatus::Downloading, Some("downloading"));

    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_b]
    );

    assert!(!pipeline.maybe_start_repair(job_c).await);
    assert_eq!(
        pipeline.jobs.get(&job_c).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
}

#[tokio::test]
async fn pause_rejects_queued_repair_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_a = JobId(31221);
    let job_b = JobId(31222);

    pipeline.jobs.insert(
        job_a,
        minimal_job_state(job_a, "repair-a", temp_dir.path().join("repair-a")),
    );
    pipeline.jobs.insert(
        job_b,
        minimal_job_state(job_b, "repair-b", temp_dir.path().join("repair-b")),
    );

    assert!(pipeline.maybe_start_repair(job_a).await);
    assert!(!pipeline.maybe_start_repair(job_b).await);
    let queued_at = pipeline
        .jobs
        .get(&job_b)
        .and_then(|state| state.queued_repair_at_epoch_ms)
        .unwrap();

    let error = pipeline.pause_job_runtime(job_b).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("pause is only supported in queued or downloading states")
    );
    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_b)
            .and_then(|state| state.queued_repair_at_epoch_ms),
        Some(queued_at)
    );
}

/// A clean two-segment payload plus its index verifies and completes.
///
/// The payload's first segment stops mid-block, so block 0 is only ever staged
/// across an article boundary while block 1 closes whole — the shape that keeps
/// this honest about a job the in-stream grid cannot claim outright.
#[tokio::test]
async fn a_clean_job_verifies_and_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30461);
    let payload_filename = "Silver.Horizon.S01E01.mkv";
    let index_filename = "Silver.Horizon.S01E01.par2";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let par2_bytes = build_test_par2_index(payload_filename, &payload, 64);
    let spec = split_payload_par2_job_spec(
        "Clean Split Payload",
        payload_filename,
        payload.len() as u32,
        index_filename,
        par2_bytes.len() as u32,
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    submit_par2_index(&mut pipeline, job_id, index_filename, &par2_bytes).await;
    submit_split_payload(&mut pipeline, job_id, payload_filename, &payload).await;
    drain_job_to_completion(&mut pipeline, job_id).await;

    assert!(pipeline.par2_verified.contains(&job_id));

    pump_pipeline_runtime_queues(&mut pipeline).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
}

/// The same job with one payload byte flipped reaches the authoritative
/// analyzer exactly once and fails as unrepairable — no quick arm may conclude
/// verification over bytes that contradict the recovery set.
#[tokio::test]
async fn a_damaged_job_runs_the_authoritative_analyzer_and_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30463);
    let payload_filename = "Silver.Horizon.S01E02.mkv";
    let index_filename = "Silver.Horizon.S01E02.par2";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged = payload.clone();
    damaged[70] ^= 0xFF;
    let par2_bytes = build_test_par2_index(payload_filename, &payload, 64);
    let spec = split_payload_par2_job_spec(
        "Damaged Split Payload",
        payload_filename,
        payload.len() as u32,
        index_filename,
        par2_bytes.len() as u32,
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    submit_par2_index(&mut pipeline, job_id, index_filename, &par2_bytes).await;
    {
        // Direct decode injection bypasses dispatch. Once index parsing rebuilds
        // the queue, model the payload work as already leased before injecting
        // its decoded results so phantom queued work cannot force a second pass.
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    submit_split_payload(&mut pipeline, job_id, payload_filename, &damaged).await;
    drain_job_to_completion(&mut pipeline, job_id).await;

    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { error }) if error.contains("not repairable")
    ));
}

#[tokio::test]
async fn waiting_on_present_volumes_is_not_repair_ready_until_a_volume_is_truly_absent() {
    // `WaitingForVolumes` covers two different situations, and PAR2
    // repair-readiness must only fire for one of them. A set mid
    // swap-correction waits on volume *numbers* while every actual volume sits
    // parsed on disk under mismatched numbering — that wait is answered by the
    // cached-header retry, and treating it as missing-volume repair readiness
    // sent a swap job into damaged-path analysis, promoted recovery blocks it
    // had no use for, and emitted verification events its fixture forbids. A
    // volume that is genuinely absent — no facts, no file — is the shape where
    // PAR2 really is the only move.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30031);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "RAR Swap Waiting",
            &[
                ("show.part1.rar".to_string(), 512),
                ("show.part2.rar".to_string(), 512),
                ("show.part3.rar".to_string(), 512),
            ],
        ),
    )
    .await;
    for filename in ["show.part1.rar", "show.part2.rar", "show.part3.rar"] {
        tokio::fs::write(working_dir.join(filename), b"volume-bytes")
            .await
            .unwrap();
    }

    let set_key = (job_id, "show".to_string());
    pipeline.rar_sets.insert(
        set_key.clone(),
        crate::pipeline::archive::rar_state::RarSetState {
            facts: [
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
                (2u32, dummy_rar_volume_facts(2)),
            ]
            .into_iter()
            .collect(),
            volume_files: [
                (0u32, "show.part1.rar".to_string()),
                (1u32, "show.part2.rar".to_string()),
                (2u32, "show.part3.rar".to_string()),
            ]
            .into_iter()
            .collect(),
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                member_dependencies: Default::default(),
                // The swap transient: waiting on 1..=3 while parsed volumes are
                // 0..=2 — the waited numbers that exist are all present, and 3
                // is a phantom of the mislabeling.
                waiting_on_volumes: [1, 2, 3].into_iter().collect(),
                deletion_eligible: Default::default(),
                delete_decisions: Default::default(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: Default::default(),
                    complete_volumes: Default::default(),
                    expected_volume_count: None,
                    members: Vec::new(),
                    unresolved_spans: Vec::new(),
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    assert!(
        pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id),
        "the broad phase-based predicate must still read this as waiting"
    );
    // `insert_active_job` queued the spec's segments, so the pipeline still
    // owes this job payload work — and mid-download, an absent volume is just
    // a volume that has not arrived yet. Nothing in `WaitingForVolumes` may
    // qualify while anything is en route (the demotion-refetch and swap
    // transients both fired the predicate 10 seconds into a job this way).
    assert!(
        !pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "pending download work means absence proves nothing yet"
    );

    // Quiet the pipeline; the rest of the contract is about exhaustion.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
    }
    assert!(
        !pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "present waiting volumes mean the volume-0 retry is owed its chance — \
         not PAR2"
    );

    // Same set once the wait is only on a volume nothing can produce — the
    // missing-middle shape. Now readiness must fire.
    if let Some(plan) = pipeline
        .rar_sets
        .get_mut(&set_key)
        .and_then(|set_state| set_state.plan.as_mut())
    {
        plan.waiting_on_volumes = [3].into_iter().collect();
    }
    assert!(
        pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "a wait no retry can answer is exactly the repair-readiness shape"
    );

    // And the same missing-middle shape stops qualifying the moment any
    // pipeline work reappears — a delayed retry here stands in for any of the
    // pending-work arms. (A health probe is deliberately not one of them: it
    // moves no byte, so it can never turn absence back into arrival.)
    pipeline.pending_retries_by_job.insert(job_id, 1);
    assert!(
        !pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "absence while anything is en route is not absence"
    );
    pipeline.pending_retries_by_job.remove(&job_id);
    assert!(
        pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "and it qualifies again once that retry is gone"
    );
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.health_probing = true;
    }
    assert!(
        pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "a probe in flight is not pending work and must not hold repair back"
    );
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.health_probing = false;
    }

    // AwaitingRepair qualifies with an empty waiting list — the livelocked
    // small-repair family sits exactly there — and it stays unconditional:
    // the extraction machinery itself concluded only repair moves the set,
    // pending work or not.
    if let Some(plan) = pipeline
        .rar_sets
        .get_mut(&set_key)
        .and_then(|set_state| set_state.plan.as_mut())
    {
        plan.phase = crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair;
        plan.waiting_on_volumes = Default::default();
    }
    assert!(
        pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "AwaitingRepair is unconditional — even while the pipeline is busy"
    );
}

/// The job 11737 shape, end to end.
///
/// A standalone payload with one article that never arrived, a recovery block
/// that covers the hole, and a PAR2 repair that puts the file right. The
/// article bitmap is *not* backfilled by the repair — nothing rewrites history
/// — so the job used to fail its final completeness veto despite holding a
/// verified, byte-correct output.
///
/// Once PAR2 has repaired and re-verified a protected output, that verification
/// is authoritative and the bitmap is diagnostic history. This is exactly what
/// NZBGet pins in `test_parchecker_repair`, which makes a segment unavailable
/// and asserts `SUCCESS/PAR`.
#[tokio::test]
async fn missing_article_repaired_by_par2_completes_despite_incomplete_bitmap() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(11737);
    let payload_filename = "silver.horizon.mkv";
    let index_filename = "silver.horizon.par2";
    let recovery_filename = "silver.horizon.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    // The second article never landed, so its slice is a hole on disk.
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "Silver Horizon Missing Article".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "silver-horizon-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "silver-horizon-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "silver-horizon-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "silver-horizon-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    let payload_file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        // Only the first article ever arrived. This is the whole point of the
        // test: the bitmap stays one segment short for the rest of the job.
        state
            .assembly
            .file_mut(payload_file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    assert!(
        !pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(payload_file_id)
            .unwrap()
            .is_complete(),
        "precondition: the payload must start with a hole in its article bitmap"
    );
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "a repaired, re-verified payload must not fail for its article bitmap; {}",
        debug_job_state(&pipeline, job_id)
    );

    let drained = drain_job_events(&mut events, job_id);
    let repair_completes: Vec<u32> = drained
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::RepairComplete {
                slices_repaired, ..
            } => Some(*slices_repaired),
            _ => None,
        })
        .collect();
    assert_eq!(
        repair_completes.len(),
        1,
        "exactly one RepairComplete; events = {drained:?}"
    );
    // Counted from the pre-repair verdict. Read off the post-repair result it
    // would always be zero, because a repair that succeeded leaves no missing
    // blocks behind to count.
    assert_eq!(
        repair_completes[0], 1,
        "the one reconstructed slice must be reported"
    );

    let repair_complete_at = drained
        .iter()
        .position(|event| matches!(event, PipelineEvent::RepairComplete { .. }))
        .unwrap();
    assert!(
        !drained[repair_complete_at..]
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobFailed { .. })),
        "RepairComplete must never be followed by the job failing; events = {drained:?}"
    );

    // The promotion itself is durable, so a restart cannot resurrect the hole:
    // the completed row carries the recovery set's digest, not the pre-repair
    // bytes. (The in-memory assembly is gone by now — a completed job is
    // dropped from the map, which is itself proof the veto let it through.)
    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_eq!(
        trusted.get(&payload_file_id.file_index).copied(),
        Some(par2_rs::checksum::md5(&original_payload)),
        "the repaired payload must persist its verified digest"
    );

    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(
            "Silver Horizon Missing Article",
        ));
    assert_eq!(
        tokio::fs::read(output_dir.join(payload_filename))
            .await
            .unwrap(),
        original_payload,
        "the delivered file must be the repaired bytes"
    );
}

/// A payload posted under an obfuscated name reconciles through PAR2 content
/// identity, not string equality.
///
/// The reconciler this replaces matched the verification's filename against the
/// assembly's stored names and did nothing at all when no exact string matched:
/// no promotion, no error, just a silently unreconciled file for the
/// completeness veto downstream to fail the whole job on. An obfuscated post is
/// exactly the case that never matches — the subject lies about the name and
/// tells the truth about the bytes — so the binding has to ask the bytes.
#[tokio::test]
async fn obfuscated_payload_reconciles_through_par2_content_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30311);
    let posted_filename = "a7f3e91c9b2d4e6f.bin";
    let described_filename = "Silver Horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();

    let spec = JobSpec {
        name: "Obfuscated PAR2 Reconciliation".to_string(),
        password: None,
        total_bytes: payload.len() as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: posted_filename.to_string(),
            role: FileRole::from_filename(posted_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "obfuscated-0@example.com".to_string(),
                },
                segment_spec! {
                    number: 1,
                    bytes: 64,
                    message_id: "obfuscated-1@example.com".to_string(),
                },
            ],
        }],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::write(working_dir.join(posted_filename), &payload)
        .await
        .unwrap();

    let payload_file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(payload_file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    // The bytes are what the obfuscation did not touch, and what the content
    // binding reads.
    pipeline
        .file_prefix_16k
        .insert(payload_file_id, payload.clone());

    let par2_set = build_repairable_par2_set(described_filename, &payload, 64, 1);
    let par2_file_id = par2_set.recovery_file_ids[0];
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: described_filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true, true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .expect("reconciliation must not error");

    assert_eq!(
        report.completed, 1,
        "the verified payload must be promoted even though its posted name \
         matches no description; report = {report:?}"
    );
    assert!(
        report.unbound.is_empty(),
        "a content-bound file is not unbound; report = {report:?}"
    );
    assert!(report.contested.is_empty(), "report = {report:?}");
    assert!(report.length_mismatch.is_empty(), "report = {report:?}");
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(payload_file_id)
            .unwrap()
            .is_complete(),
        "promotion must fill the article bitmap"
    );
    assert_eq!(
        pipeline
            .current_filename_for_file_id(job_id, payload_file_id)
            .as_deref(),
        Some(posted_filename),
        "without a canonical file on disk reconciliation must retain the installed alias"
    );
}

#[tokio::test]
async fn reconciliation_adopts_the_verified_canonical_file_over_a_duplicate_alias() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30329);
    let source_filename = "a7f3e91c9b2d4e6f.bin";
    let duplicate_filename = "Silver Horizon.duplicate1.mkv";
    let canonical_filename = "Silver Horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, file_id) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Verified Canonical PAR2 Reconciliation",
        source_filename,
        &payload,
    )
    .await;
    tokio::fs::write(working_dir.join(duplicate_filename), &payload)
        .await
        .unwrap();
    tokio::fs::write(working_dir.join(canonical_filename), &payload)
        .await
        .unwrap();
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: file_id.file_index,
                source_filename: source_filename.to_string(),
                current_filename: duplicate_filename.to_string(),
                canonical_filename: Some(duplicate_filename.to_string()),
                classification: None,
                classification_source: FileIdentitySource::Par2,
            },
        )
        .unwrap();
    pipeline.file_prefix_16k.insert(file_id, payload.clone());

    let par2_set = build_repairable_par2_set(canonical_filename, &payload, 64, 1);
    let verification = complete_verification_for(&par2_set, canonical_filename);
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .unwrap();

    assert_eq!(report.completed, 1);
    let identity = pipeline.file_identity(job_id, file_id).unwrap();
    assert_eq!(identity.source_filename, source_filename);
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(canonical_filename)
    );
    assert_eq!(identity.classification_source, FileIdentitySource::Par2);
}

/// Two assembly files that both answer to one description bind to neither.
///
/// Content cannot break a tie that two files both satisfy, so the binding is
/// refused outright and named. The reconciler this replaces resolved the same
/// contest first-writer-wins — whichever file the iteration happened to reach
/// first was promoted, and the other silently was not.
#[tokio::test]
async fn contested_par2_binding_is_refused_and_named() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30312);
    let described_filename = "Silver Horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    // Two obfuscated posts of byte-identical content: both answer to the one
    // description, by content, equally well.
    let first_posted = "aaaa1111.bin";
    let second_posted = "bbbb2222.bin";

    let file_spec = |filename: &str, tag: &str| FileSpec {
        filename: filename.to_string(),
        role: FileRole::from_filename(filename),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 128,
            message_id: format!("contested-{tag}@example.com"),
        }],
    };
    let spec = JobSpec {
        name: "Contested PAR2 Binding".to_string(),
        password: None,
        total_bytes: (payload.len() * 2) as u64,
        category: None,
        metadata: vec![],
        files: vec![file_spec(first_posted, "a"), file_spec(second_posted, "b")],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for name in [first_posted, second_posted] {
        tokio::fs::write(working_dir.join(name), &payload)
            .await
            .unwrap();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    for file_index in 0..2u32 {
        pipeline
            .file_prefix_16k
            .insert(NzbFileId { job_id, file_index }, payload.clone());
    }

    let par2_set = build_repairable_par2_set(described_filename, &payload, 64, 1);
    let par2_file_id = par2_set.recovery_file_ids[0];
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: described_filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true, true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .expect("a contest is reported, not an error from the pass itself");

    assert_eq!(
        report.completed, 0,
        "neither contender may be promoted on a guess; report = {report:?}"
    );
    assert_eq!(
        report.contested,
        vec![described_filename.to_string()],
        "the contest must name the description it could not resolve"
    );
}

/// An unprotected file short of articles is reported, and delivered anyway.
///
/// Job 10000 forced this: a 1.09 GB payload that PAR2 repaired and re-verified,
/// failed because a 738 KB `.nfo` no recovery set ever covered was missing a few
/// articles. Health 999. Both oracles ship that job — NZBGet's `FAILURE/HEALTH`
/// requires par to have been *skipped*, and SABnzbd never derives a failure from
/// missing articles at all — and weaver's final move relocates the working
/// directory wholesale, so the bytes reach the user regardless. Refusing the job
/// destroys a good download to report damage on a text file.
///
/// The distinction still has to survive in the *message*, because a protected
/// file left incomplete means something entirely different.
#[tokio::test]
async fn unprotected_incomplete_file_is_reported_but_does_not_fail_the_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30313);
    let protected_filename = "silver.horizon.mkv";
    let unprotected_filename = "extras.nfo";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();

    let spec = JobSpec {
        name: "Unprotected Missing File".to_string(),
        password: None,
        total_bytes: (payload.len() + 128) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: protected_filename.to_string(),
                role: FileRole::from_filename(protected_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "unprotected-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: unprotected_filename.to_string(),
                role: FileRole::from_filename(unprotected_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "unprotected-nfo-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "unprotected-nfo-1@example.com".to_string(),
                    },
                ],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::write(working_dir.join(protected_filename), &payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        // The NFO lost an article and nothing protects it.
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 1,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }

    // The recovery set covers the payload only.
    let par2_set = build_repairable_par2_set(protected_filename, &payload, 64, 1);
    let par2_file_id = par2_set.recovery_file_ids[0];
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: protected_filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true, true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    pipeline
        .reconcile_and_classify_par2_verification(job_id, &verification, false, "clean PAR2 test")
        .await
        .expect("an unprotected file short of articles must not fail the job");

    // The payload was promoted; only the uncovered NFO is still short.
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: 0
            })
            .unwrap()
            .is_complete(),
        "the PAR2-verified payload must still be promoted"
    );

    let report = pipeline
        .classify_incomplete_after_par2(
            job_id,
            &crate::pipeline::completion::finalize::check::Par2Reconciliation::default(),
            "clean PAR2 test",
        )
        .expect("the uncovered file must still be reported");
    assert_eq!(
        report.unproven_protected, 0,
        "a file no recovery set covers is not a protected defect: {}",
        report.message
    );
    assert!(
        report.message.contains("unprotected"),
        "the report must say the file was unprotected, not blame reconciliation: {}",
        report.message
    );
    assert!(
        report.message.contains(unprotected_filename),
        "the report must name the file that came up short: {}",
        report.message
    );
    assert!(
        !report.message.contains(protected_filename),
        "the repaired, verified payload must not be implicated: {}",
        report.message
    );
}

/// A verdict vouching for bytes that are at neither name is refused.
///
/// The presence gate was relaxed to unblock direct-store, whose routing volumes
/// are verified through the set's own access layer and legitimately have no
/// file. Relaxing it for *every* binding went too far: an ordinary file that is
/// simply gone would be promoted to complete on the strength of a verdict about
/// bytes that are nowhere.
#[tokio::test]
async fn missing_ordinary_file_is_refused_by_the_presence_gate() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30320);
    let payload_filename = "silver.horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, file_id) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Missing Ordinary File",
        payload_filename,
        &payload,
    )
    .await;

    // No file is ever written: this job is not direct-store, so nothing excuses
    // the absence.
    assert!(!working_dir.join(payload_filename).exists());
    let par2_set = build_repairable_par2_set(payload_filename, &payload, 64, 1);
    let verification = complete_verification_for(&par2_set, payload_filename);
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .unwrap();

    assert_eq!(
        report.completed, 0,
        "a file that is not on disk must not be called complete; report = {report:?}"
    );
    assert_eq!(
        report.length_mismatch.len(),
        1,
        "the absence must be reported, not swallowed; report = {report:?}"
    );
    assert!(
        !pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(file_id)
            .unwrap()
            .is_complete()
    );
}

/// Two descriptions claiming one assembly file bind to neither.
///
/// The mirror of the `by_identity` contest. `or_insert` used to keep whichever
/// description was visited first and drop the other silently, calling the file
/// complete under one of two names with no reason to prefer either.
#[tokio::test]
async fn two_descriptions_claiming_one_file_are_contested() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30321);
    let posted_filename = "silver_horizon.mkv";
    // Sanitizing turns `:` into `_`, so this description answers to the very
    // same name as the one above — two descriptions, one assembly file.
    let colliding_filename = "silver:horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, _) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Contested Inverse",
        posted_filename,
        &payload,
    )
    .await;
    tokio::fs::write(working_dir.join(posted_filename), &payload)
        .await
        .unwrap();

    let par2_set = build_repairable_par2_set_for_files(
        &[(posted_filename, &payload), (colliding_filename, &payload)],
        64,
        1,
    );
    let ids: Vec<par2_rs::FileId> = par2_set.recovery_file_ids.clone();
    let verification = par2_rs::VerificationResult {
        files: ids
            .iter()
            .map(|id| par2_rs::verify::FileVerification {
                file_id: *id,
                filename: par2_set.file_description(id).unwrap().filename.clone(),
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: vec![true, true],
                missing_slice_count: 0,
            })
            .collect(),
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .unwrap();

    assert_eq!(
        report.completed, 0,
        "neither claim may promote the file; report = {report:?}"
    );
    assert!(
        !report.contested.is_empty(),
        "the contest must be named; report = {report:?}"
    );
}

/// Accepted repairs shed their leftovers; failed ones keep them.
#[tokio::test]
async fn repair_leftovers_are_purged_only_after_acceptance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30322);
    let payload_filename = "silver.horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, _) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Repair Leftovers",
        payload_filename,
        &payload,
    )
    .await;
    let par2_filename = "silver.horizon.par2";
    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, 64, 1),
        &[],
    );

    // What the directory held before the repair.
    pipeline
        .par2_pre_repair_dir_entries
        .insert(job_id, HashSet::from([payload_filename.to_string()]));
    // What the repair left behind: the damaged original, renamed aside.
    let leftover = working_dir.join(format!("{payload_filename}.1"));
    tokio::fs::write(&leftover, &payload).await.unwrap();
    // A recovery file the set describes is not a leftover even though it is new.
    tokio::fs::write(working_dir.join(par2_filename), b"par2")
        .await
        .unwrap();

    pipeline.purge_par2_repair_leftovers(job_id);

    assert!(
        !leftover.exists(),
        "an accepted repair must not leave its damaged original to be delivered"
    );
    assert!(
        working_dir.join(payload_filename).exists(),
        "the repaired payload must survive"
    );
    assert!(
        !pipeline.par2_pre_repair_dir_entries.contains_key(&job_id),
        "the snapshot must not survive into the next attempt"
    );

    // Now the failure path: the snapshot is dropped unread, so nothing is
    // deleted and the evidence stays.
    let second_leftover = working_dir.join("silver.horizon.mkv.2");
    tokio::fs::write(&second_leftover, &payload).await.unwrap();
    pipeline
        .par2_pre_repair_dir_entries
        .insert(job_id, HashSet::from([payload_filename.to_string()]));
    pipeline.fail_par2_repair(job_id, "post-repair verification failed".to_string());
    assert!(
        second_leftover.exists(),
        "a failed repair must keep its artefacts for diagnosis"
    );
    assert!(!pipeline.par2_pre_repair_dir_entries.contains_key(&job_id));
}

/// A protected file left incomplete whose verified bytes are nowhere still
/// fails the job; one whose bytes are on disk only warns.
///
/// The invariant frees the job from its article bitmap, not from its bytes. If
/// reconciliation could not promote a protected file *and* the bytes it was
/// vouched for cannot be found, delivering would ship a hole under a
/// verification claiming otherwise — the one case still worth refusing.
#[tokio::test]
async fn protected_defect_fails_only_when_the_bytes_are_gone() {
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let payload_filename = "silver_horizon.mkv";

    // Bytes gone: the verdict vouches for a file that is not there.
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30323);
        let (working_dir, _) = incomplete_protected_payload_job(
            &mut pipeline,
            job_id,
            "Protected Bytes Gone",
            payload_filename,
            &payload,
        )
        .await;
        assert!(!working_dir.join(payload_filename).exists());
        let par2_set = build_repairable_par2_set(payload_filename, &payload, 64, 1);
        let verification = complete_verification_for(&par2_set, payload_filename);
        install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

        let error = pipeline
            .reconcile_and_classify_par2_verification(job_id, &verification, false, "clean PAR2")
            .await
            .expect_err("a protected file whose bytes are gone must fail the job");
        assert!(
            error.contains("nowhere on disk"),
            "the failure must say the bytes could not be found: {error}"
        );
    }

    // Bytes present, but two descriptions contest the binding: a reconciliation
    // defect over a file that is demonstrably intact. Warn, deliver.
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30324);
        let (working_dir, _) = incomplete_protected_payload_job(
            &mut pipeline,
            job_id,
            "Protected Bytes Present",
            payload_filename,
            &payload,
        )
        .await;
        tokio::fs::write(working_dir.join(payload_filename), &payload)
            .await
            .unwrap();
        let par2_set = build_repairable_par2_set_for_files(
            &[
                (payload_filename, &payload),
                ("silver:horizon.mkv", &payload),
            ],
            64,
            1,
        );
        let ids = par2_set.recovery_file_ids.clone();
        let verification = par2_rs::VerificationResult {
            files: ids
                .iter()
                .map(|id| par2_rs::verify::FileVerification {
                    file_id: *id,
                    filename: par2_set.file_description(id).unwrap().filename.clone(),
                    status: par2_rs::verify::FileStatus::Complete,
                    valid_slices: vec![true, true],
                    missing_slice_count: 0,
                })
                .collect(),
            recovery_blocks_available: 1,
            total_missing_blocks: 0,
            repairable: par2_rs::verify::Repairability::NotNeeded,
        };
        install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

        pipeline
            .reconcile_and_classify_par2_verification(job_id, &verification, false, "clean PAR2")
            .await
            .expect("a defect over bytes that are demonstrably present must not fail the job");
    }
}

/// A stray on disk is never renamed into a duplicate of a verified file.
///
/// The pre-repair backup par2-rs leaves behind still matches the description
/// over its first 16 KiB, so the renamer offers to move it onto the canonical
/// name. That name is taken by the file the repair just produced, so the
/// allocator used to mint a `.duplicateN` sibling — and the final move, which
/// relocates the whole directory, delivered both copies.
#[tokio::test]
async fn stray_file_is_not_renamed_into_a_duplicate() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30325);
    let payload_filename = "silver_horizon.mkv";
    // Job 10000's shape: the damage sits far past the 16 KiB window the
    // renamer matches on, so the leftover still looks exactly like the
    // description it came from.
    let payload: Vec<u8> = (0..65536u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, _) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Stray Rename Guard",
        payload_filename,
        &payload,
    )
    .await;

    // The repaired file at its canonical name, and the damaged original the
    // repair renamed aside — `unique_backup_path`'s `.1` suffix, which par2-rs
    // does not count as one of its own generated artifacts.
    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    let mut damaged = payload.clone();
    damaged[32768..].fill(0);
    let leftover = working_dir.join(format!("{payload_filename}.1"));
    tokio::fs::write(&leftover, &damaged).await.unwrap();
    assert_eq!(
        par2_rs::checksum::md5(&damaged[..16384]),
        par2_rs::checksum::md5(&payload[..16384]),
        "precondition: the leftover must still match the description's 16 KiB window"
    );

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, 1024, 1),
        &[],
    );

    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    let entries: Vec<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .collect();
    assert!(
        !entries.iter().any(|name| name.contains("duplicate")),
        "a stray must never become a duplicate of a verified file; entries = {entries:?}"
    );
    assert_eq!(
        std::fs::read(working_dir.join(payload_filename)).unwrap(),
        payload,
        "the canonical file must still be the good one"
    );
}

#[tokio::test]
async fn a_rename_suggestion_with_an_ambiguous_prefix_hash_is_dropped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30326);
    let posted_filename = "a9e3d04c.bin";
    let payload: Vec<u8> = (0..49_152u32).map(|value| (value % 251) as u8).collect();
    let mut alternate = payload.clone();
    alternate[16_384..].fill(0x5a);
    let working_dir = install_par2_rename_candidate(
        &mut pipeline,
        job_id,
        posted_filename,
        &payload,
        &[
            ("silver-horizon.mkv", &payload),
            ("ivory-meadow.mkv", &alternate),
        ],
    )
    .await;

    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    assert!(
        working_dir.join(posted_filename).exists(),
        "a shared 16 KiB hash must not choose either target"
    );
    assert!(!working_dir.join("silver-horizon.mkv").exists());
    assert!(!working_dir.join("ivory-meadow.mkv").exists());
}

#[tokio::test]
async fn a_split_fragment_rename_suggestion_is_dropped_before_target_exists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30327);
    let fragment_filename = "onyx-prairie.mkv.001";
    let payload: Vec<u8> = (0..49_152u32).map(|value| (value % 239) as u8).collect();
    let working_dir = install_par2_rename_candidate(
        &mut pipeline,
        job_id,
        fragment_filename,
        &payload,
        &[("onyx-prairie.mkv", &payload)],
    )
    .await;
    assert!(!working_dir.join("onyx-prairie.mkv").exists());

    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    assert!(
        working_dir.join(fragment_filename).exists(),
        "the fragment must remain under its own name"
    );
    assert!(
        !working_dir.join("onyx-prairie.mkv").exists(),
        "the target was free, so keeping it absent proves the suggestion was filtered"
    );
}

#[tokio::test]
async fn signature_detected_unknown_par2_authenticates_and_renames_payload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(303271);
    let par2_filename = "b7f3c11d";
    let payload_filename = "8a2e94bc";
    let canonical_filename = "movie.mkv";
    let payload: Vec<u8> = (0..49_152u32).map(|value| (value % 233) as u8).collect();
    let par2 = build_test_par2_index(canonical_filename, &payload, 1024);
    let spec = JobSpec {
        name: "Misnamed PAR2 Carrier".to_string(),
        password: None,
        total_bytes: (par2.len() + payload.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: par2_filename.to_string(),
                role: FileRole::from_filename(par2_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2.len() as u32,
                    message_id: "misnamed-par2@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload.len() as u32,
                    message_id: "misnamed-payload@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, par2_filename, &par2).await;
    write_and_complete_file(&mut pipeline, job_id, 1, payload_filename, &payload).await;

    let par2_file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    pipeline
        .file_prefix_16k
        .insert(par2_file_id, par2[..64].to_vec());
    pipeline
        .file_declared_size
        .insert(par2_file_id, par2.len() as u64);
    pipeline.note_par2_metadata_signature(par2_file_id, Some(par2.len() as u64));
    assert!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&0))
            .is_some_and(|file| file.signature_candidate),
        "a structural PAR2 header makes an unknown file eligible for authenticated parsing"
    );

    load_par2_index(&mut pipeline, job_id, 0).await;
    assert!(pipeline.par2_set(job_id).is_some());
    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    assert!(working_dir.join(par2_filename).exists());
    assert!(!working_dir.join(payload_filename).exists());
    assert_eq!(
        std::fs::read(working_dir.join(canonical_filename)).unwrap(),
        payload,
        "only authenticated FileDesc metadata may rename the opaque payload"
    );
}

#[tokio::test]
async fn a_unique_same_length_obfuscated_rename_still_lands() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30328);
    let posted_filename = "d90f27ac.bin";
    let correct_filename = "silver-horizon.mkv";
    let payload: Vec<u8> = (0..49_152u32).map(|value| (value % 241) as u8).collect();
    let working_dir = install_par2_rename_candidate(
        &mut pipeline,
        job_id,
        posted_filename,
        &payload,
        &[(correct_filename, &payload)],
    )
    .await;

    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    assert!(!working_dir.join(posted_filename).exists());
    assert_eq!(
        std::fs::read(working_dir.join(correct_filename)).unwrap(),
        payload,
        "a unique suggestion with matching length must still deobfuscate"
    );
}

/// Two pairs of files posted under each other's names, nothing damaged: the
/// repairer must never be asked to fix a set whose only fault is placement.
///
/// The repairer's own scanner reports every one of them `Renamed`, which makes
/// `needs_repair()` true, and the old ladder read that as "repair required" —
/// with zero damaged slices and zero blocks needed, the tell that there was
/// nothing to repair. Running it anyway installed each file at its canonical
/// name and left the displaced originals behind as `<name>.N`, after which the
/// job's own post-repair pass could no longer tell a backup from the file it had
/// been displaced by. Placement is the whole job here, and the plan for it has
/// to come from a directory scan: a swap is two files each holding the other's
/// content, and only something that looks at what is on each name can see it.
#[tokio::test]
async fn a_placement_only_verdict_is_placed_without_running_the_repairer() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30344);
    let job_name = "Silver Horizon Swapped Parts";
    let described: Vec<(&str, Vec<u8>)> = vec![
        ("silver-horizon-a.bin", misplacement_payload(1)),
        ("silver-horizon-b.bin", misplacement_payload(2)),
        ("silver-horizon-c.bin", misplacement_payload(3)),
        ("silver-horizon-d.bin", misplacement_payload(4)),
    ];
    // a<->b and c<->d, exactly as posted: two physical swaps, four misplaced
    // files, not one damaged byte anywhere.
    let on_disk = vec![
        described[1].1.clone(),
        described[0].1.clone(),
        described[3].1.clone(),
        described[2].1.clone(),
    ];
    let working_dir =
        misplaced_payload_par2_job(&mut pipeline, job_id, job_name, &described, &on_disk, 1).await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_analyze_calls, 1,
        "precondition: this shape reaches the repairer-analysis arm, which is \
         where the misplacement is seen at all"
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 0,
        "a set with nothing damaged has nothing for the repairer to write"
    );
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 1,
        "the analysis is answered with one scanned whole-set pass — the scan is \
         what sees a swap — and that answer stands as the set's verdict"
    );
    assert_eq!(
        drain_job_repair_complete(&mut repair_events, job_id),
        0,
        "and no repair happened, so none may be announced"
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    for (filename, bytes) in &described {
        assert_eq!(
            tokio::fs::read(output_dir.join(filename)).await.unwrap(),
            *bytes,
            "{filename} must be delivered holding its own content"
        );
    }
    assert!(
        !working_dir.exists() || std::fs::read_dir(&working_dir).unwrap().next().is_none(),
        "and nothing may be left behind in the working directory"
    );
}

/// Partial quick evidence is recognized and the set still settles correctly.
///
/// Half the set is proven by zero-read digest evidence; the other half is a
/// swapped pair with no evidence at all. The quick pass now reports Partial
/// instead of throwing the proof away, and the swap is still seen and fixed —
/// through the repairer-analysis arm, whose whole-set pass places by 16 KiB
/// prefix proposal rather than the old full-MD5 directory scan, so the set is
/// read once there instead of twice. The analysis arm does not yet consume
/// the partial evidence to narrow its read to the unproven pair — that
/// consumption exists only at the verification fallback today — which is why
/// this test pins one authoritative pass, not a selective one.
#[tokio::test]
async fn partial_quick_evidence_is_reported_and_the_swap_still_settles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30346);
    let job_name = "Silver Horizon Partial Evidence";
    let described: Vec<(&str, Vec<u8>)> = vec![
        ("silver-horizon-a.bin", misplacement_payload(11)),
        ("silver-horizon-b.bin", misplacement_payload(12)),
        ("silver-horizon-c.bin", misplacement_payload(13)),
        ("silver-horizon-d.bin", misplacement_payload(14)),
    ];
    // a and b sit at their own names; c and d are posted under each other's.
    let on_disk = vec![
        described[0].1.clone(),
        described[1].1.clone(),
        described[3].1.clone(),
        described[2].1.clone(),
    ];
    let working_dir =
        misplaced_payload_par2_job(&mut pipeline, job_id, job_name, &described, &on_disk, 1).await;

    // Measured-digest evidence for the first two files: the quick pass's
    // digest arm proves each without a read. Deliberately NOT grid evidence —
    // a fully gridded job earns the strong-decode skip and never owes the
    // authoritative pass this test exists to narrow. The swapped pair carries
    // no evidence of any kind — unproven, not distrusted.
    for file_index in 0..2u32 {
        let file_id = NzbFileId { job_id, file_index };
        let payload = described[file_index as usize].1.clone();
        pipeline
            .ensure_par2_runtime(job_id)
            .completed_checksums
            .insert(
                file_id,
                crate::pipeline::CompletedFileChecksum {
                    md5: Some(par2_rs::checksum::md5(&payload)),
                    crc32: par2_rs::checksum::crc32(&payload),
                    all_parts_crc_verified: false,
                },
            );
    }

    settle_job_completion(&mut pipeline, job_id).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_quick_partial_verify_calls, 1,
        "the quick pass must conclude partially, not inconclusively"
    );
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 1,
        "the analysis arm answers with exactly one pass — the scan that used \
         to double it is gone; analyze={} execute={}",
        pipeline.par2_repairer_analyze_calls, pipeline.par2_repairer_execute_calls,
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 0,
        "a swap with nothing damaged writes nothing"
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    for (filename, bytes) in &described {
        assert_eq!(
            tokio::fs::read(output_dir.join(filename)).await.unwrap(),
            *bytes,
            "{filename} must be delivered intact"
        );
    }
    assert!(
        !working_dir.exists() || std::fs::read_dir(&working_dir).unwrap().next().is_none(),
        "and nothing may be left behind in the working directory"
    );
}

/// Damage alongside misplacement still runs the repairer, and the files it
/// placed are read back where it placed them.
///
/// This is the row the placement-only rule must not swallow: one holed file
/// means slices to reconstruct, so the ladder below it — capacity, promotion,
/// repair — is exactly what the job needs. The renamed files then come back
/// through the post-repair read at their canonical names, because that is where
/// the repair put them.
#[tokio::test]
async fn damage_alongside_misplacement_still_repairs_and_reads_back_the_placed_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30345);
    let job_name = "Silver Horizon Swapped And Holed";
    let described: Vec<(&str, Vec<u8>)> = vec![
        ("silver-horizon-a.bin", misplacement_payload(5)),
        ("silver-horizon-b.bin", misplacement_payload(6)),
        ("silver-horizon-c.bin", misplacement_payload(7)),
    ];
    let mut holed = described[2].1.clone();
    holed[64..].fill(0);
    let on_disk = vec![described[1].1.clone(), described[0].1.clone(), holed];
    let working_dir =
        misplaced_payload_par2_job(&mut pipeline, job_id, job_name, &described, &on_disk, 1).await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 1,
        "a damaged slice is still a repair"
    );
    assert_eq!(
        drain_job_repair_complete(&mut repair_events, job_id),
        1,
        "and a repair that held is announced"
    );
    let splits = pipeline
        .par2_post_repair_read_splits
        .last()
        .copied()
        .expect("the repair tail ran its post-repair read");
    assert_eq!(
        splits,
        (0usize, 3usize),
        "nothing was complete at its canonical name before the repair, so \
         nothing may be carried: two misplaced files and one holed one all get \
         read back where the repair installed them"
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    for (filename, bytes) in &described {
        assert_eq!(
            tokio::fs::read(output_dir.join(filename)).await.unwrap(),
            *bytes,
            "{filename} must be delivered holding its own content"
        );
    }
    assert!(
        !working_dir.exists() || std::fs::read_dir(&working_dir).unwrap().next().is_none(),
        "and the repair's backups may not follow the payload out"
    );
}

/// The field shape: a clean swapped pair under canonical names, downloaded
/// metadata-early, so no whole-file MD5 was ever streamed and no article closed
/// a block on the dual-CRC grid. Quick verify has no evidence keyed to content
/// — only names and lengths — and names are exactly what a swap makes lie, so it
/// must stay inconclusive and leave the authoritative read to decide.
///
/// The per-file loop finds neither a closed in-stream block verdict nor a
/// current-generation measured digest, so neither protected description is
/// matched and the final unresolved check refuses the verdict. The companion test
/// `a_misplaced_pair_proven_by_measured_digests_returns_a_swap_plan` shows the
/// identical fixture resolving the swap the moment a digest is present, which is
/// what pins the absence of evidence — not a broken fixture — as the cause here.
#[tokio::test]
async fn a_misplaced_pair_with_no_content_evidence_is_correctly_inconclusive() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30360);
    let a = misplacement_payload(1);
    let b = misplacement_payload(2);
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Swap No Evidence",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[Some(b.clone()), Some(a.clone())],
    )
    .await;

    let file0 = NzbFileId {
        job_id,
        file_index: 0,
    };
    let file1 = NzbFileId {
        job_id,
        file_index: 1,
    };
    // Pin the arm. Neither file carries a closed in-stream block verdict ...
    assert!(pipeline.block_crc_verdicts(file0).is_none());
    assert!(pipeline.block_crc_verdicts(file1).is_none());
    // ... nor a current-generation measured digest, in the runtime or the db.
    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert!(!runtime.completed_checksums.contains_key(&file0));
    assert!(!runtime.completed_checksums.contains_key(&file1));
    let persisted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert!(
        persisted.is_empty(),
        "no persisted digest either: {persisted:?}"
    );

    let result = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error on a clean-but-unproven shape");
    assert!(
        result.is_none(),
        "with no digest and no grid verdict the content is unproven, so the \
         quick pass must stay inconclusive and let the authoritative read decide"
    );
    assert_eq!(
        pipeline.par2_quick_verify_calls, 0,
        "an inconclusive pass never counts as a quick verification"
    );
}

/// A complete file outside the current recovery set must not veto a digest
/// match for that set merely because the unrelated file has no MD5. This is the
/// multi-set late-discovery shape: an earlier grid-only payload remains in the
/// assembly while a later set is proved by the digest its own payload streamed.
#[tokio::test]
async fn an_unrelated_grid_only_file_does_not_veto_a_digest_proven_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30365);
    let unrelated = misplacement_payload(13);
    let target = misplacement_payload(14);
    let unrelated_name = "earlier-grid-only.bin";
    let target_name = "late-digest.bin";
    let spec = standalone_job_spec(
        "Silver Horizon Independent Late Set",
        &[
            (unrelated_name.to_string(), unrelated.len() as u32),
            (target_name.to_string(), target.len() as u32),
        ],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, unrelated_name, &unrelated).await;
    write_and_complete_file(&mut pipeline, job_id, 1, target_name, &target).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(target_name.to_string(), target.clone())]),
        &[],
    );
    set_measured_md5(&mut pipeline, job_id, 1, &target);
    assert!(
        !pipeline
            .par2_runtime(job_id)
            .unwrap()
            .completed_checksums
            .contains_key(&NzbFileId {
                job_id,
                file_index: 0,
            }),
        "the unrelated earlier payload deliberately has no whole-file digest"
    );

    let par2_set = Arc::clone(pipeline.par2_set(job_id).expect("served recovery set"));
    let (verification, plan, evidence) = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error")
        .expect("the current set is fully proved by its own payload digest");

    assert_eq!(verification.files.len(), 1);
    assert_eq!(plan.exact.len(), 1);
    assert!(plan.unresolved.is_empty());
    assert_ne!(evidence, QuickPar2Evidence::Grid);
}

/// The same swapped pair, now carrying the trusted whole-file MD5 that a
/// non-metadata-early download would have streamed. The measured digest keys the
/// match to the description its *content* reproduces, not the one its current
/// name implies, so the swap already resolves today: `Ok(Some(..))` with a
/// two-entry swap plan and a clean verdict. This is the control that proves the
/// swap machinery is sound and the inconclusive verdict above is caused solely
/// by absent content evidence.
#[tokio::test]
async fn a_misplaced_pair_proven_by_measured_digests_returns_a_swap_plan() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30361);
    let a = misplacement_payload(3);
    let b = misplacement_payload(4);
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Swap Measured",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[Some(b.clone()), Some(a.clone())],
    )
    .await;
    set_measured_md5(&mut pipeline, job_id, 0, &b);
    set_measured_md5(&mut pipeline, job_id, 1, &a);

    let (verification, plan, evidence) = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error")
        .expect("measured digests prove the content, so the swap resolves clean");

    assert_eq!(plan.swaps.len(), 1, "the two files are one swap pair");
    assert!(plan.exact.is_empty());
    assert!(plan.renames.is_empty());
    assert!(plan.unresolved.is_empty());
    assert!(plan.conflicts.is_empty());
    assert_eq!(verification.files.len(), 2);
    assert_ne!(
        evidence,
        QuickPar2Evidence::Grid,
        "measured digests must not claim the in-stream-grid-only settlement marker"
    );
    let (left, right) = &plan.swaps[0];
    let mut correct = [left.correct_name.as_str(), right.correct_name.as_str()];
    correct.sort_unstable();
    assert_eq!(correct, ["silver-horizon-a.bin", "silver-horizon-b.bin"]);
    assert_eq!(pipeline.par2_quick_verify_calls, 1);
}

/// Fail-closed: damage alongside the swap. `silver-horizon-a.bin` holds its
/// partner's clean bytes (a valid swap half), but `silver-horizon-b.bin` holds
/// bytes that reproduce no description's hash. The damaged file matches nothing,
/// its partner description is left unresolved, and the pass refuses — a damaged
/// file must never leave this path with a clean verdict, even with digests
/// present that would otherwise resolve the swap.
#[tokio::test]
async fn a_damaged_file_in_the_misplaced_shape_is_never_quick_verified() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30362);
    let a = misplacement_payload(5);
    let b = misplacement_payload(6);
    let mut damaged = b.clone();
    damaged[10] ^= 0xFF; // same length, reproduces no description's hash
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Swap With Damage",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[Some(b.clone()), Some(damaged.clone())],
    )
    .await;
    set_measured_md5(&mut pipeline, job_id, 0, &b);
    set_measured_md5(&mut pipeline, job_id, 1, &damaged);

    let result = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error");
    assert!(
        result.is_none(),
        "a file whose bytes match no description leaves its partner unresolved, \
         so the pass must refuse"
    );
    assert_eq!(pipeline.par2_quick_verify_calls, 0);
}

/// Fail-closed: one hash claimed by two descriptions. Both descriptions carry
/// the same `hash_full`, so a file matching it is ambiguous — the match count
/// for the chosen id trips two, the id is dropped as a conflict, and its partner
/// description is left unresolved. Ambiguity of this kind is exactly what the
/// authoritative read owns, so the quick pass refuses.
#[tokio::test]
async fn a_hash_claimed_by_two_descriptions_is_never_quick_verified() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30363);
    let shared = misplacement_payload(7);
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Shared Hash",
        &[
            ("silver-horizon-a.bin", shared.clone()),
            ("silver-horizon-b.bin", shared.clone()),
        ],
        &[Some(shared.clone()), Some(shared.clone())],
    )
    .await;
    set_measured_md5(&mut pipeline, job_id, 0, &shared);
    set_measured_md5(&mut pipeline, job_id, 1, &shared);

    let result = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error");
    assert!(
        result.is_none(),
        "a hash two descriptions answer to is ambiguous, so the pass must refuse"
    );
    assert_eq!(pipeline.par2_quick_verify_calls, 0);
}

/// Fail-closed: a swap whose partner never arrived. One file is present at
/// `silver-horizon-b.bin`'s canonical name but holds `silver-horizon-a.bin`'s
/// content; the file that should hold the other half never completed. The
/// present half resolves to its content's description, but the absent partner's
/// description has no disk file, so it is left unresolved and the pass refuses.
#[tokio::test]
async fn a_swap_whose_partner_is_absent_is_never_quick_verified() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30364);
    let a = misplacement_payload(11);
    let b = misplacement_payload(12);
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Absent Partner",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[None, Some(a.clone())],
    )
    .await;
    set_measured_md5(&mut pipeline, job_id, 1, &a);

    let result = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error");
    assert!(
        result.is_none(),
        "the partner description has no disk file, so the pass must refuse"
    );
    assert_eq!(pipeline.par2_quick_verify_calls, 0);
}

/// Quarantining the damaged source beside repaired canonical bytes must not
/// force a second read of the whole recovery set.
#[tokio::test]
async fn repaired_obfuscated_rar_quarantine_keeps_the_selective_repair_tail() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30347);
    let mut files = build_multifile_multivolume_rar_set();
    let source_filename = "ae282dbe64861b7171e041b55057e3dd.40";
    let canonical_filename = files[1].0.clone();
    let duplicate_filename = "show.part02.duplicate1.rar";
    files[1].1.resize(20 * 1024, 0);
    let repaired_volume = files[1].1.clone();
    let mut damaged_volume = repaired_volume.clone();
    damaged_volume[17 * 1024] = 1;
    let notes_filename = "show.nfo";
    let notes = b"repair-tail fixture".to_vec();
    let index_filename = "show.par2";
    let par2_bytes = build_test_par2_index_for_files(
        &[
            (files[0].0.as_str(), files[0].1.as_slice()),
            (canonical_filename.as_str(), repaired_volume.as_slice()),
            (notes_filename, notes.as_slice()),
        ],
        1024,
    );
    let posted_files = vec![
        files[0].clone(),
        (source_filename.to_string(), damaged_volume.clone()),
        files[2].clone(),
        files[3].clone(),
        (notes_filename.to_string(), notes.clone()),
        (index_filename.to_string(), par2_bytes.clone()),
    ];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Obfuscated RAR Selective Repair Tail", &posted_files),
    )
    .await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for file_index in [0usize, 2, 3] {
        write_and_complete_rar_volume(
            &mut pipeline,
            job_id,
            file_index as u32,
            &posted_files[file_index].0,
            &posted_files[file_index].1,
        )
        .await;
    }
    write_and_complete_file(&mut pipeline, job_id, 4, notes_filename, &notes).await;
    write_and_complete_file(&mut pipeline, job_id, 5, index_filename, &par2_bytes).await;
    tokio::fs::write(working_dir.join(source_filename), &damaged_volume)
        .await
        .unwrap();
    pipeline.file_prefix_16k.insert(
        NzbFileId {
            job_id,
            file_index: 1,
        },
        damaged_volume[..16 * 1024].to_vec(),
    );

    pipeline.par2_pre_repair_dir_entries.insert(
        job_id,
        posted_files
            .iter()
            .map(|(filename, _)| filename.clone())
            .collect(),
    );
    let stale_headers = shortened_e01_rar_headers(&files);
    let rar_key = (job_id, "show".to_string());
    let generation_before_rebind = pipeline
        .rar_sets
        .get(&rar_key)
        .expect("the partial RAR set should already be registered")
        .extraction_generation;
    pipeline
        .rar_sets
        .get_mut(&rar_key)
        .expect("the partial RAR set should already be registered")
        .cached_headers = Some(stale_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, "show", &stale_headers)
        .unwrap();
    tokio::fs::write(working_dir.join(&canonical_filename), &repaired_volume)
        .await
        .unwrap();

    let par2_set = build_repairable_par2_set_for_files(
        &[
            (files[0].0.as_str(), files[0].1.as_slice()),
            (canonical_filename.as_str(), repaired_volume.as_slice()),
            (notes_filename, notes.as_slice()),
        ],
        1024,
        1,
    );
    let carried_rar_id = par2_set.recovery_file_ids[0];
    let repaired_rar_id = par2_set.recovery_file_ids[1];
    let carried_notes_id = par2_set.recovery_file_ids[2];
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        par2_set,
        &[(5, index_filename, 0, false)],
    );
    let par2_set = pipeline.par2_set(job_id).cloned().unwrap();

    let valid_slices = |len: usize| vec![true; len.div_ceil(1024)];
    let mut damaged_slices = valid_slices(repaired_volume.len());
    *damaged_slices.last_mut().unwrap() = false;
    let pre_repair = par2_rs::VerificationResult {
        files: vec![
            par2_rs::verify::FileVerification {
                file_id: carried_rar_id,
                filename: files[0].0.clone(),
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: valid_slices(files[0].1.len()),
                missing_slice_count: 0,
            },
            par2_rs::verify::FileVerification {
                file_id: repaired_rar_id,
                filename: canonical_filename.clone(),
                status: par2_rs::verify::FileStatus::Damaged(1),
                valid_slices: damaged_slices,
                missing_slice_count: 1,
            },
            par2_rs::verify::FileVerification {
                file_id: carried_notes_id,
                filename: notes_filename.to_string(),
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: valid_slices(notes.len()),
                missing_slice_count: 0,
            },
        ],
        recovery_blocks_available: 1,
        total_missing_blocks: 1,
        repairable: par2_rs::verify::Repairability::Repairable {
            blocks_needed: 1,
            blocks_available: 1,
        },
    };
    let outcome = par2_rs::Par2RepairOutcome {
        status: par2_rs::Par2RepairStatus::Repaired,
        files_complete: 3,
        files_renamed: 1,
        files_damaged: 0,
        files_missing: 0,
        available_blocks: 1,
        missing_blocks: 0,
        recovery_blocks_available: 1,
        recovery_blocks_used: 1,
        bytes_copied: 0,
        bytes_reconstructed: 1024,
        packets: par2_rs::PacketDiagnostics::default(),
        scan: par2_rs::ScanDiagnostics::default(),
        carry: par2_rs::repairer::CarryDiagnostics::default(),
        verification: pre_repair.clone(),
    };

    pipeline
        .finish_par2_repair(
            job_id,
            Arc::clone(&par2_set),
            working_dir.clone(),
            &pre_repair,
            outcome,
            false,
        )
        .await;
    drain_rar_refreshes(&mut pipeline).await;

    assert_eq!(pipeline.par2_post_repair_read_splits, vec![(2, 1)]);
    assert_eq!(pipeline.par2_selective_verify_calls, 1);
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "moving the damaged source to a duplicate name did not alter canonical bytes"
    );
    assert_eq!(drain_job_repair_complete(&mut repair_events, job_id), 1);
    assert_eq!(
        tokio::fs::read(working_dir.join(&canonical_filename))
            .await
            .unwrap(),
        repaired_volume
    );
    assert!(
        !working_dir.join(duplicate_filename).exists(),
        "the quarantine copy should be swept after the aggregate settles"
    );
    let identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 1,
            },
        )
        .unwrap();
    assert_eq!(identity.source_filename, source_filename);
    assert_eq!(identity.current_filename, canonical_filename);
    assert!(
        pipeline
            .rar_sets
            .get(&rar_key)
            .unwrap()
            .extraction_generation
            > generation_before_rebind,
        "canonical identity rebinding must invalidate older refresh generations"
    );

    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, "show");
    assert_eq!(
        volume_paths.get(&1),
        Some(&working_dir.join(&canonical_filename))
    );
    let cached_headers = pipeline
        .load_rar_snapshot(job_id, "show")
        .expect("canonical reconciliation should rebuild the complete RAR snapshot");
    let cached_archive =
        unrar_rs::RarArchive::deserialize_headers_with_password(&cached_headers, None::<String>)
            .unwrap();
    let cached_e01 = cached_archive
        .metadata()
        .members
        .into_iter()
        .find(|member| member.name == "E01.mkv")
        .unwrap();
    assert_eq!(cached_e01.volumes.first_volume, 0);
    assert_eq!(cached_e01.volumes.last_volume, 1);
    let mut archive = Pipeline::open_rar_archive_from_snapshot_or_disk(
        crate::pipeline::extraction::RarArchiveSnapshotOpenRequest {
            set_name: "show",
            volume_paths: volume_paths.clone(),
            password_candidates: Vec::new(),
            cached_headers: Some(cached_headers),
            shared_kdf_cache: Arc::new(unrar_rs::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            requested_members: None,
            already_extracted: None,
            budget: None,
        },
    )
    .unwrap()
    .value;
    let output_dir = working_dir.join("repair-tail-output");
    std::fs::create_dir_all(&output_dir).unwrap();
    let member_index = archive.find_member_sanitized("E01.mkv").unwrap();
    Pipeline::extract_rar_member_to_output(
        &mut archive,
        crate::pipeline::extraction::RarExtractionContext::new(
            &volume_paths,
            &pipeline.event_tx,
            job_id,
            "show",
            &output_dir,
            &unrar_rs::ExtractOptions {
                verify: true,
                password: None,
                restore_owners: false,
            },
        ),
        member_index,
    )
    .unwrap();
    assert_eq!(
        std::fs::read(output_dir.join("E01.mkv")).unwrap(),
        b"episode-a-payload"
    );
}

/// A free canonical rename is still only a 16 KiB identity match until the
/// settled-layout pass proves the full digest.
#[tokio::test]
async fn canonical_non_recovery_rename_rejects_corruption_after_the_prefix() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30348);
    let payload_filename = "silver-horizon.mkv";
    let posted_notes_filename = "9f2c1a5e.dat";
    let notes_filename = "silver-horizon.bin";
    let payload = misplacement_payload(31);
    let notes: Vec<u8> = (0..20 * 1024).map(|index| (index % 251) as u8).collect();
    let mut corrupted_notes = notes.clone();
    corrupted_notes[17 * 1024] ^= 1;
    let spec = standalone_job_spec(
        "Canonical Non-Recovery Tail Guard",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            (
                posted_notes_filename.to_string(),
                corrupted_notes.len() as u32,
            ),
        ],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        posted_notes_filename,
        &corrupted_notes,
    )
    .await;

    let mut par2_set = build_repairable_par2_set_for_files(&[(payload_filename, &payload)], 64, 1);
    describe_non_recovery_file(&mut par2_set, notes_filename, &notes);
    let payload_id = par2_set.recovery_file_ids[0];
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);
    let par2_set = pipeline.par2_set(job_id).cloned().unwrap();
    let pre_repair = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: payload_id,
            filename: payload_filename.to_string(),
            status: par2_rs::verify::FileStatus::Damaged(1),
            valid_slices: vec![true, false],
            missing_slice_count: 1,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 1,
        repairable: par2_rs::verify::Repairability::Repairable {
            blocks_needed: 1,
            blocks_available: 1,
        },
    };
    let outcome = par2_rs::Par2RepairOutcome {
        status: par2_rs::Par2RepairStatus::Repaired,
        files_complete: 1,
        files_renamed: 0,
        files_damaged: 0,
        files_missing: 0,
        available_blocks: 1,
        missing_blocks: 0,
        recovery_blocks_available: 1,
        recovery_blocks_used: 1,
        bytes_copied: 0,
        bytes_reconstructed: 64,
        packets: par2_rs::PacketDiagnostics::default(),
        scan: par2_rs::ScanDiagnostics::default(),
        carry: par2_rs::repairer::CarryDiagnostics::default(),
        verification: pre_repair.clone(),
    };

    pipeline
        .finish_par2_repair(
            job_id,
            Arc::clone(&par2_set),
            working_dir.clone(),
            &pre_repair,
            outcome,
            false,
        )
        .await;

    assert_eq!(
        pipeline.par2_selective_verify_calls, 2,
        "two selective passes and nothing more: the post-repair read of what \
         the repair rewrote, then the canonical re-proof of what moved; \
         authoritative={}",
        pipeline.par2_authoritative_verify_calls,
    );
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "and nothing may re-read the files the selective post-repair pass \
         already proved in place"
    );
    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "the strict canonical pass must reject a tail that the 16 KiB identity prefix cannot see"
    );
    assert_eq!(
        tokio::fs::read(working_dir.join(notes_filename))
            .await
            .unwrap(),
        corrupted_notes,
        "the failure is proof, not another rename or a destructive cleanup"
    );
}
