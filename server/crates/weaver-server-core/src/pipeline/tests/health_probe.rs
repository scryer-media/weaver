use super::*;

#[tokio::test]
async fn upstream_probe_does_not_misclassify_unknown_numeric_plain_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10075);
    let files = vec![
        (
            "51273aad56a8b904e96928935278a627.10".to_string(),
            b"plain-a".to_vec(),
        ),
        (
            "51273aad56a8b904e96928935278a627.11".to_string(),
            b"plain-b".to_vec(),
        ),
    ];
    let spec = rar_job_spec("Unknown Numeric Plain Files", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .archive_topologies()
            .is_empty()
    );

    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Unknown Numeric Plain Files",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("51273aad56a8b904e96928935278a627.10").exists());
    assert!(dest.join("51273aad56a8b904e96928935278a627.11").exists());
}

#[tokio::test]
async fn excluded_source_not_found_retries_without_marking_health_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20011);
    let spec = segmented_job_spec("Excluded Source Not Found", "retry.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::ArticleNotFound,
                "article not found",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: vec![0],
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert!(pipeline.pending_completion_checks.is_empty());

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let work = pipeline
        .retry_rx
        .try_recv()
        .expect("excluded-source miss should requeue the segment")
        .work;
    assert_eq!(work.exclude_servers, vec![0]);
    assert_eq!(work.retry_count, 1);
}

#[tokio::test]
async fn probe_activation_keeps_queues_live_and_completion_clears_health_probing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30020);
    let spec = standalone_job_spec(
        "Probe Reset",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
            ("probe-c.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.activate_health_probes(job_id);

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.health_probing);
        // A probe rides alongside the download instead of standing in front
        // of it, so it never moves the job out of Downloading.
        assert!(matches!(state.status, JobStatus::Downloading));
        assert_eq!(state.download_queue.len(), 3);
        assert_eq!(state.recovery_queue.len(), 0);
        assert!(state.held_segments.is_empty());
    }

    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round: 0,
        total: 1,
        missed: 0,
        done: true,
        inconclusive: false,
    });

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!state.health_probing);
    assert!(matches!(state.status, JobStatus::Downloading));
    assert!(state.held_segments.is_empty());
    assert_eq!(state.download_queue.len(), 3);
    assert_eq!(state.recovery_queue.len(), 0);
}

#[tokio::test]
async fn critical_health_starts_probe_without_immediate_fail_fast() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30025);
    let spec = standalone_job_spec(
        "Critical Probe Fence",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
            ("probe-c.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 46;
    }

    pipeline.check_health(job_id);

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.health_probing);
        // A probe rides alongside the download instead of standing in front
        // of it, so it never moves the job out of Downloading.
        assert!(matches!(state.status, JobStatus::Downloading));
    }

    pipeline.check_health(job_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(state.health_probing);
    // A probe rides alongside the download instead of standing in front
    // of it, so it never moves the job out of Downloading.
    assert!(matches!(state.status, JobStatus::Downloading));
}

#[tokio::test]
async fn probe_completion_inconclusive_restores_queues_without_health_damage() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30021);
    let spec = standalone_job_spec(
        "Probe Inconclusive",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
            ("probe-c.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 10;
    }

    pipeline.activate_health_probes(job_id);
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round: 0,
        total: 0,
        missed: 0,
        done: true,
        inconclusive: true,
    });

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!state.health_probing);
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.failed_bytes, 10);
    assert_eq!(state.last_health_probe_failed_bytes, 10);
    assert_eq!(state.next_health_probe_failed_bytes, 11);
    assert!(state.held_segments.is_empty());
    assert_eq!(state.download_queue.len(), 3);
}

#[tokio::test]
async fn inconclusive_final_probe_still_enforces_critical_health() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30027);
    let spec = standalone_job_spec(
        "Probe Inconclusive Critical",
        &[
            ("critical-a.bin".to_string(), 100),
            ("critical-b.bin".to_string(), 100),
            ("critical-c.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.jobs.get_mut(&job_id).unwrap().failed_bytes = 50;

    pipeline.activate_health_probes(job_id);
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round: 0,
        total: 0,
        missed: 0,
        done: true,
        inconclusive: true,
    });

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert!(!pipeline.pending_completion_checks.contains(&job_id));
}

#[test]
fn health_probe_samples_rotate_across_rounds() {
    let first = Pipeline::health_probe_sample_indices(120, 0);
    let second = Pipeline::health_probe_sample_indices(120, 1);

    assert_eq!(first.len(), second.len());
    assert_ne!(first, second);
    assert_eq!(first.first().copied(), Some(0));
    assert_eq!(second.first().copied(), Some(1));
}

#[tokio::test]
async fn probe_completion_does_not_immediately_reenter_checking() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30021);
    let spec = standalone_job_spec(
        "Probe Reentry Guard",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
            ("probe-c.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 10;
    }

    pipeline.activate_health_probes(job_id);
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round: 0,
        total: 1,
        missed: 0,
        done: true,
        inconclusive: false,
    });

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!state.health_probing);
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.last_health_probe_failed_bytes, 10);
    assert!(state.next_health_probe_failed_bytes > 10);
}

#[tokio::test]
async fn clean_probe_waits_for_material_new_damage_before_rearming() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30022);
    let spec = JobSpec {
        name: "Probe Hysteresis".to_string(),
        password: None,
        total_bytes: 300 * 1024 * 1024,
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
                    bytes: 100 * 1024 * 1024,
                    message_id: "probe-hysteresis-a@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload-b.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 100 * 1024 * 1024,
                    message_id: "probe-hysteresis-b@example.com".to_string(),
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
                    bytes: 100 * 1024 * 1024,
                    message_id: "probe-hysteresis-repair@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 10 * 1024 * 1024;
    }

    pipeline.activate_health_probes(job_id);
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round: 0,
        total: 10,
        missed: 0,
        done: true,
        inconclusive: false,
    });

    let rearm_watermark = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .next_health_probe_failed_bytes;
    assert!(rearm_watermark > 10 * 1024 * 1024 + 1);

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = rearm_watermark - 1;
    }
    pipeline.check_health(job_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!state.health_probing);
    assert!(matches!(state.status, JobStatus::Downloading));

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = rearm_watermark;
    }
    pipeline.check_health(job_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(state.health_probing);
    // A probe rides alongside the download instead of standing in front
    // of it, so it never moves the job out of Downloading.
    assert!(matches!(state.status, JobStatus::Downloading));
}

#[tokio::test]
async fn missed_probe_rearms_on_next_failed_byte() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30023);
    let spec = JobSpec {
        name: "Probe Immediate Rearm".to_string(),
        password: None,
        total_bytes: 300 * 1024 * 1024,
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
                    bytes: 100 * 1024 * 1024,
                    message_id: "probe-rearm-a@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload-b.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 100 * 1024 * 1024,
                    message_id: "probe-rearm-b@example.com".to_string(),
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
                    bytes: 100 * 1024 * 1024,
                    message_id: "probe-rearm-repair@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 10 * 1024 * 1024;
    }

    pipeline.activate_health_probes(job_id);
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round: 0,
        total: 10,
        missed: 1,
        done: true,
        inconclusive: false,
    });

    let rearm_watermark = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .next_health_probe_failed_bytes;
    // A probe that missed re-arms on the next byte of new damage, measured
    // against the figure the health policy decides on: the booked ledger, or
    // this round's projection when it estimated more of the payload dead.
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(
        rearm_watermark,
        Pipeline::health_decision_failed_bytes(state) + 1
    );
    assert!(
        state.probe_projected_failed_bytes > state.failed_bytes,
        "the projection is what the round learned, and it stays out of the ledger"
    );

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = rearm_watermark;
    }
    pipeline.check_health(job_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(state.health_probing);
    // A probe rides alongside the download instead of standing in front
    // of it, so it never moves the job out of Downloading.
    assert!(matches!(state.status, JobStatus::Downloading));
}

/// A probe still in flight must not postpone PAR2 recovery promotion.
///
/// A volume the poster never uploaded leaves the job with nothing more to
/// download and a probe running against a soft timeout. The completion
/// checkpoint is what promotes the recovery blocks that repair the hole, and
/// counting the probe as pending pipeline work held that promotion — and every
/// second of repair and extraction behind it — until the probe gave up.
#[tokio::test]
async fn health_probe_in_flight_does_not_delay_recovery_promotion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30311);
    let payload_filename = "silver.horizon.mkv";
    let index_filename = "silver.horizon.par2";
    let recovery_filename = "silver.horizon.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let spec = JobSpec {
        name: "Withheld Volume Probe Overlap".to_string(),
        password: None,
        total_bytes: original_payload.len() as u64 + par2_bytes.len() as u64 + 64,
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
                        message_id: "probe-overlap-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "probe-overlap-payload-1@example.com".to_string(),
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
                    message_id: "probe-overlap-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "probe-overlap-volume@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &damaged_payload).await;
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 2,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("probe-overlap-volume@example.com"),
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
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 0),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, false),
        ],
    );

    pipeline.activate_health_probes(job_id);
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.health_probing);
        // A probe rides alongside the download instead of standing in front
        // of it, so it never moves the job out of Downloading.
        assert!(matches!(state.status, JobStatus::Downloading));
    }

    // The checkpoint stays shut because the job still owes download work — not
    // because a probe is in flight.
    pipeline.check_job_completion(job_id).await;
    assert!(
        !pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .download_queue
            .has_recovery_work()
    );

    // The last segment settles. The probe has nothing left to say, so it is
    // retired rather than waited on.
    pipeline.active_download_passes.insert(job_id);
    pipeline.maybe_finish_download_pass(job_id);
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(!state.health_probing);
        // The drain hands the job on to PAR2 verification. Retiring the probe
        // no longer pushes a status of its own back over that, because the
        // probe never claimed the status in the first place.
        assert!(
            matches!(state.status, JobStatus::Verifying),
            "{}",
            debug_job_state(&pipeline, job_id)
        );
    }
    assert!(pipeline.pending_completion_checks.contains(&job_id));

    pipeline.check_job_completion(job_id).await;
    // The damaged-path read is detached; the promotion it asks for happens when
    // its verdict lands back on the pipeline task.
    settle_par2_analysis_work(&mut pipeline).await;
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .download_queue
            .has_recovery_work(),
        "recovery must be promoted without waiting for the probe to time out"
    );
}

/// Retiring the probe must not leave an unrecoverable job parked.
///
/// Before the probe was retired at drain time, a job with no recovery data and
/// articles it could never fetch reached its failure through the probe's own
/// verdict. With the probe retired instead, the completion checkpoint has to
/// carry that verdict on its own: the pass is over, files are still short, and
/// nothing can repair them.
#[tokio::test]
async fn a_retired_probe_still_lets_an_unrecoverable_job_fail() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30312);
    let spec = standalone_job_spec(
        "Retired Probe Unrecoverable",
        &[
            ("silver-horizon-a.bin".to_string(), 100),
            ("silver-horizon-b.bin".to_string(), 100),
            ("silver-horizon-c.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 200;
    }
    pipeline.activate_health_probes(job_id);
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.health_probing);
        // A probe rides alongside the download instead of standing in front
        // of it, so it never moves the job out of Downloading.
        assert!(matches!(state.status, JobStatus::Downloading));
    }

    // Every article the job could try has reached a terminal state.
    pipeline.active_download_passes.insert(job_id);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.maybe_finish_download_pass(job_id);
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(!state.health_probing, "the probe should have been retired");
    }
    assert!(pipeline.pending_completion_checks.contains(&job_id));

    pipeline.check_job_completion(job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "an unrecoverable job must fail once the probe is retired: {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("no PAR2 metadata is available for repair"),
        "{error}"
    );
}

/// The decode that settles last must retire the probe, not just the download
/// result that preceded it.
///
/// Live ordering, which the drain-time tests above never reproduce: the last
/// article's download result is processed while its decode is still queued.
/// The drain sequence therefore runs with an unsettled decode on the books —
/// pending download work by definition — and correctly refuses to retire the
/// probe. If nothing re-runs it when that decode lands, the job sits in
/// `Checking` behind a probe with nothing left to say until the probe's own
/// soft timeout fires, holding the completion checkpoint and PAR2 recovery
/// promotion for the whole of it.
#[tokio::test]
async fn the_last_decode_to_settle_retires_the_probe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30320);
    let filename = "silver-horizon-tail.bin";
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Last Decode Retires Probe", filename, &[8, 8]),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment(&mut pipeline, file_id, 0, 0, b"horizon0", filename, None).await;

    pipeline.activate_health_probes(job_id);
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.health_probing);
        // A probe rides alongside the download instead of standing in front
        // of it, so it never moves the job out of Downloading.
        assert!(matches!(state.status, JobStatus::Downloading));
    }

    let last_segment = SegmentId {
        file_id,
        segment_number: 1,
    };
    park_job_on_its_final_decode(&mut pipeline, last_segment, 8);

    // The download-result path's own drain attempt. It cannot retire the probe
    // yet, and that is correct — the article it just delivered is still
    // decoding.
    pipeline.maybe_finish_download_pass(job_id);
    assert!(
        pipeline.jobs.get(&job_id).unwrap().health_probing,
        "an unsettled decode is pending download work: {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_queued_decode(&mut pipeline, file_id, 1, 8, b"horizon1", filename).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(
        !state.health_probing,
        "the settling decode left nothing for the probe to say, so it must be \
         retired instead of waited out: {}",
        debug_job_state(&pipeline, job_id)
    );
    assert!(
        !matches!(state.status, JobStatus::Checking),
        "a retired probe must not leave the job parked in Checking: {}",
        debug_job_state(&pipeline, job_id)
    );
    assert!(
        pipeline.pending_completion_checks.contains(&job_id),
        "retiring the probe must hand the job straight to the completion \
         checkpoint"
    );
}

const PROBE_POLICY_PAYLOAD: &str = "silver.horizon.part01.rar";
const PROBE_POLICY_INDEX: &str = "silver.horizon.par2";
const PROBE_POLICY_VOLUME: &str = "silver.horizon.vol00+08.par2";

/// A posting with a recovery set: one payload file in eight 64-byte articles,
/// the set's index, and one recovery volume.
///
/// Returns the spec alongside the payload bytes and the index bytes, because
/// both are needed to install a matching PAR2 runtime.
fn probe_policy_par2_job(name: &str) -> (JobSpec, Vec<u8>, Vec<u8>) {
    let payload: Vec<u8> = (0..512u32).map(|value| (value % 251) as u8).collect();
    let index_bytes = build_test_par2_index(PROBE_POLICY_PAYLOAD, &payload, 64);
    let spec = JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: payload.len() as u64 + index_bytes.len() as u64 + 64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: PROBE_POLICY_PAYLOAD.to_string(),
                role: FileRole::from_filename(PROBE_POLICY_PAYLOAD),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: (0..8u32)
                    .map(|number| {
                        segment_spec! {
                            number: number,
                            bytes: 64,
                            message_id: format!("probe-policy-payload-{number}@example.com"),
                        }
                    })
                    .collect(),
            },
            FileSpec {
                filename: PROBE_POLICY_INDEX.to_string(),
                role: FileRole::from_filename(PROBE_POLICY_INDEX),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: index_bytes.len() as u32,
                    message_id: "probe-policy-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: PROBE_POLICY_VOLUME.to_string(),
                role: FileRole::from_filename(PROBE_POLICY_VOLUME),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "probe-policy-volume@example.com".to_string(),
                }],
            },
        ],
    };
    (spec, payload, index_bytes)
}

fn probe_policy_payload_segment(job_id: JobId, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number,
    }
}

/// One withheld volume against a recovery set that covers it must not probe.
///
/// The probe exists to recognise a release nobody uploaded early enough to
/// abandon it. A single file lost out of a posting whose PAR2 set already
/// carries more blocks than the hole needs is not that: the answer is known,
/// the repair is already scheduled, and the sample would spend round trips
/// re-deriving a verdict the recovery set has settled. Take the coverage away
/// and the same damage is worth asking about again.
#[tokio::test]
async fn par2_capacity_covering_the_damage_suppresses_the_probe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30330);
    let (spec, payload, _index_bytes) = probe_policy_par2_job("Covered Withheld Volume");
    insert_active_job(&mut pipeline, job_id, spec).await;
    // Nothing has landed, so the dead-release shape gate is wide open and the
    // recovery set's coverage is the only thing deciding this.
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(PROBE_POLICY_PAYLOAD, &payload, 64, 8),
        &[
            (1, PROBE_POLICY_INDEX, 0, false),
            (2, PROBE_POLICY_VOLUME, 8, false),
        ],
    );

    // One article of the payload is gone. Eight recovery blocks against a
    // single lost 64-byte slice is coverage to spare.
    assert!(pipeline.book_terminal_segment(
        probe_policy_payload_segment(job_id, 0),
        SegmentTerminalState::Missing
    ));
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert_eq!(state.failed_bytes, 64);
        assert!(
            !state.health_probing,
            "a hole the recovery set already covers is not worth sampling: {}",
            debug_job_state(&pipeline, job_id)
        );
    }

    // The same posting, the same single failing file, but a recovery set one
    // block wide against four lost slices. The shortfall is real, so the round
    // is worth running.
    let short_job_id = JobId(30334);
    let (short_spec, short_payload, _) = probe_policy_par2_job("Uncovered Withheld Volume");
    insert_active_job(&mut pipeline, short_job_id, short_spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        short_job_id,
        build_repairable_par2_set(PROBE_POLICY_PAYLOAD, &short_payload, 64, 1),
        &[
            (1, PROBE_POLICY_INDEX, 0, false),
            (2, PROBE_POLICY_VOLUME, 1, false),
        ],
    );
    for segment_number in 0..4u32 {
        assert!(pipeline.book_terminal_segment(
            probe_policy_payload_segment(short_job_id, segment_number),
            SegmentTerminalState::Missing
        ));
    }
    assert!(
        pipeline.jobs.get(&short_job_id).unwrap().health_probing,
        "damage past what the set can cover must still reach the probe: {}",
        debug_job_state(&pipeline, short_job_id)
    );
}

/// One file failing while the rest of the job lands is not a dead release.
///
/// This is the withheld-volume shape without a recovery set to settle it, and
/// it is the case the 2% threshold on its own got wrong: a job that has
/// delivered most of its bytes and lost one file is healthy enough that a
/// sample tells nobody anything. A release that really is gone looks the other
/// way round — the failures run far ahead of what has landed.
#[tokio::test]
async fn a_single_file_failing_while_the_job_lands_does_not_probe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30331);
    let spec = standalone_job_spec(
        "Landing Job Loses One File",
        &many_standalone_files("landing", 20),
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes = 8 * 512;

    assert!(pipeline.book_terminal_segment(
        SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0
            },
            segment_number: 0,
        },
        SegmentTerminalState::Missing
    ));
    assert!(
        !pipeline.jobs.get(&job_id).unwrap().health_probing,
        "one file short of a job that is landing is not a dead release: {}",
        debug_job_state(&pipeline, job_id)
    );

    // The same single file, now against a job that has delivered nothing.
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes = 0;
    pipeline.check_health(job_id);
    assert!(
        pipeline.jobs.get(&job_id).unwrap().health_probing,
        "losses running ahead of the payload are exactly the probe's case: {}",
        debug_job_state(&pipeline, job_id)
    );
}

/// Damage spread across files with no recovery data probes, and a round that
/// comes back entirely missing abandons the job.
#[tokio::test]
async fn damage_across_files_without_recovery_probes_and_all_missing_aborts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30332);
    let spec = standalone_job_spec("Dead Release", &many_standalone_files("dead", 20));
    insert_active_job(&mut pipeline, job_id, spec).await;

    for file_index in 0..2u32 {
        assert!(pipeline.book_terminal_segment(
            SegmentId {
                file_id: NzbFileId { job_id, file_index },
                segment_number: 0,
            },
            SegmentTerminalState::Missing
        ));
    }
    assert!(
        pipeline.jobs.get(&job_id).unwrap().health_probing,
        "losses in more than one file are the dead-release shape: {}",
        debug_job_state(&pipeline, job_id)
    );

    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round: 0,
        total: 10,
        missed: 10,
        done: true,
        inconclusive: false,
    });

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "a job with no recovery data and nothing on the wire must abort: {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(error.contains("all 10 samples missing"), "{error}");
}

/// The first booked failure promotes recovery, without a checkpoint.
///
/// Recovery blocks used to sit parked in the job's recovery queue until the
/// completion checkpoint ran, which is after the whole payload has settled — so
/// the articles a repair cannot start without were fetched strictly after every
/// byte they were meant to overlap with. Damage is known the moment a segment
/// is booked terminal, and so is the capacity of a loaded recovery set, so the
/// promotion happens there instead and the blocks ride the same lanes as the
/// payload.
#[tokio::test]
async fn booked_damage_promotes_recovery_before_any_checkpoint() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30333);
    let (spec, payload, index_bytes) = probe_policy_par2_job("Early Recovery Promotion");
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 1, PROBE_POLICY_INDEX, &index_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.recovery_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 2,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("probe-policy-volume@example.com"),
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
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(PROBE_POLICY_PAYLOAD, &payload, 64, 8),
        &[
            (1, PROBE_POLICY_INDEX, 0, false),
            (2, PROBE_POLICY_VOLUME, 8, false),
        ],
    );
    assert!(
        !pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .download_queue
            .has_recovery_work()
    );

    assert!(pipeline.book_terminal_segment(
        probe_policy_payload_segment(job_id, 3),
        SegmentTerminalState::Missing
    ));

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(
        state.download_queue.has_recovery_work(),
        "booked damage against a loaded set promotes its recovery at once: {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        state.recovery_queue.len(),
        0,
        "the parked block is the one that moved"
    );
    assert!(
        pipeline.pending_completion_checks.is_empty(),
        "and it did not need a checkpoint to get there"
    );

    // Idempotent: a second failure in the same file must not re-promote work
    // that is already queued, or double-count the capacity it consumed.
    let promoted_len = pipeline.jobs.get(&job_id).unwrap().download_queue.len();
    assert!(pipeline.book_terminal_segment(
        probe_policy_payload_segment(job_id, 4),
        SegmentTerminalState::Missing
    ));
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        promoted_len
    );
}
