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
        assert!(matches!(state.status, JobStatus::Checking));
        assert_eq!(state.download_queue.len(), 3);
        assert_eq!(state.recovery_queue.len(), 0);
        assert!(state.held_segments.is_empty());
    }

    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
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
        assert!(matches!(state.status, JobStatus::Checking));
    }

    pipeline.check_health(job_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(state.health_probing);
    assert!(matches!(state.status, JobStatus::Checking));
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
    assert!(state.held_segments.is_empty());
    assert_eq!(state.download_queue.len(), 3);
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
    assert!(matches!(state.status, JobStatus::Checking));
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
    assert_eq!(
        rearm_watermark,
        pipeline.jobs.get(&job_id).unwrap().failed_bytes + 1
    );

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = rearm_watermark;
    }
    pipeline.check_health(job_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(state.health_probing);
    assert!(matches!(state.status, JobStatus::Checking));
}
