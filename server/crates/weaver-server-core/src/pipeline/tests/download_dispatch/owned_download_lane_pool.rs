//! `download_dispatch` tests, part of a mechanical split of the original file.

use super::*;

#[tokio::test]
async fn owned_download_lane_pool_respects_configured_connection_count_at_startup() {
    let low_temp_dir = tempfile::tempdir().unwrap();
    let (low_pipeline, _, _) = new_direct_pipeline_with_buffers(
        &low_temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        3,
    )
    .await;

    assert_eq!(low_pipeline.owned_download_lane_pool.worker_count(), 3);
    assert_eq!(low_pipeline.tuner.params().max_concurrent_downloads, 3);

    let high_temp_dir = tempfile::tempdir().unwrap();
    let (high_pipeline, _, _) = new_direct_pipeline_with_buffers(
        &high_temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        24,
    )
    .await;

    assert_eq!(high_pipeline.owned_download_lane_pool.worker_count(), 24);
    assert_eq!(high_pipeline.tuner.params().max_concurrent_downloads, 24);
}

#[tokio::test]
async fn par2_metadata_bootstrap_defers_payload_until_checkpoint_plan_is_known() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
    let job_id = JobId(40143);
    let payload = (0..(7 * 128))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut grid64 = placement_par2_file_set(&[("payload.mkv".to_string(), payload.clone())]);
    grid64.slice_size = 64;
    let mut grid96 = placement_par2_file_set(&[("payload.mkv".to_string(), payload.clone())]);
    grid96.recovery_set_id = par2_rs::RecoverySetId::from_bytes([10; 16]);
    grid96.slice_size = 96;

    let index64 = build_test_par2_index("payload.mkv", &payload, 64);
    let index96 = build_test_par2_index("payload.mkv", &payload, 96);
    let payload_segments = (0..7)
        .map(|number| {
            segment_spec! {
                number: number,
                bytes: 128,
                message_id: format!("payload-{number}@example.com"),
            }
        })
        .collect::<Vec<_>>();
    let spec = JobSpec {
        name: "PAR2 Metadata Bootstrap".to_string(),
        password: None,
        total_bytes: (index64.len() + index96.len() + payload.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "00-grid64.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: index64.len() as u32,
                    message_id: "grid64@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "01-grid96.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: index96.len() as u32,
                    message_id: "grid96@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload.mkv".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: payload_segments,
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    let pressure = pipeline.refresh_download_pressure();
    let grid64_id = grid64.recovery_set_id;
    let grid96_id = grid96.recovery_set_id;
    let index64_priority = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .download_queue
        .peek_next_matching(|work| work.segment_id.file_id.file_index == 0)
        .expect("the first explicit index must be the ordinary queue head")
        .priority;

    let index64_lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("the first explicit index must lease first");
    assert_eq!(index64_lease.works.len(), 1);
    assert!(matches!(
        index64_lease.checkpoint_plan,
        weaver_yenc::CheckpointPlan::None
    ));
    assert!(
        index64_lease
            .works
            .iter()
            .all(|work| work.segment_id.file_id.file_index == 0)
    );
    assert!(
        !index64_lease.compatibility.is_recovery
            && index64_lease
                .works
                .iter()
                .all(|work| !work.is_recovery && work.priority == index64_priority),
        "explicit indexes must retain primary lane and accounting classification"
    );

    {
        let runtime = pipeline.ensure_par2_runtime(job_id);
        runtime.served = Some(grid64_id);
        runtime.ensure_set_runtime(grid64_id).set = Some(std::sync::Arc::new(grid64));
        runtime.files.insert(
            0,
            Par2FileRuntime {
                filename: "00-grid64.par2".to_string(),
                discovery: Par2DiscoveryState::Parsed {
                    set_ids: vec![grid64_id],
                },
                ..Default::default()
            },
        );
    }
    pipeline.refresh_par2_checkpoint_plan(job_id);
    assert!(pipeline.promote_par2_metadata(job_id));
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        assert_eq!(
            state.download_queue.reprioritize_matching(|work| {
                (work.segment_id.file_id.file_index == 2)
                    .then_some(super::repair::PROMOTED_RECOVERY_PRIORITY - 1)
            }),
            7
        );
        assert_eq!(
            state
                .download_queue
                .peek_next_matching(|_| true)
                .map(|work| work.segment_id.file_id.file_index),
            Some(1),
            "metadata promotion must preserve the explicit index's primary priority"
        );
    }

    let index96_lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("the second explicit index must lease before the payload");
    assert_eq!(index96_lease.works.len(), 1);
    assert!(matches!(
        index96_lease.checkpoint_plan,
        weaver_yenc::CheckpointPlan::Single(_)
    ));
    assert!(
        index96_lease
            .works
            .iter()
            .all(|work| work.segment_id.file_id.file_index == 1)
    );
    assert!(
        index96_lease
            .works
            .iter()
            .all(|work| !work.is_recovery && work.priority == index64_priority)
    );

    assert!(
        pipeline
            .try_lease_initial_download_batch_for_test(job_id, pressure)
            .is_none(),
        "payload work must wait for PAR2 metadata instead of receiving a stale None plan"
    );
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 7);

    let runtime = pipeline.ensure_par2_runtime(job_id);
    runtime.ensure_set_runtime(grid96_id).set = Some(std::sync::Arc::new(grid96));
    runtime.files.insert(
        1,
        Par2FileRuntime {
            filename: "01-grid96.par2".to_string(),
            discovery: Par2DiscoveryState::Parsed {
                set_ids: vec![grid96_id],
            },
            ..Default::default()
        },
    );
    pipeline.refresh_par2_checkpoint_plan(job_id);
    assert!(pipeline.par2_metadata_discovery_closed(job_id));

    let payload_lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("payload work must lease after both grids are known");
    assert!(
        !payload_lease.works.is_empty(),
        "the released payload lease must contain work"
    );
    assert!(
        payload_lease
            .works
            .iter()
            .all(|work| work.segment_id.file_id.file_index == 2)
    );
    assert!(matches!(
        payload_lease.checkpoint_plan,
        weaver_yenc::CheckpointPlan::Multi(_)
    ));
}

#[tokio::test]
async fn par2_metadata_bootstrap_claims_every_explicit_index_in_one_primary_batch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
    let job_id = JobId(40146);
    let spec = JobSpec {
        name: "Batched Explicit PAR2 Metadata Bootstrap".to_string(),
        password: None,
        total_bytes: 256,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "00-first.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "batched-first@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "01-second.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "batched-second@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "batched-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "batched-payload-1@example.com".to_string(),
                    },
                ],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    let priority = FileRole::Par2 {
        is_index: true,
        recovery_block_count: 0,
    }
    .download_priority();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let mut queued = state.download_queue.drain_all();
        for work in &mut queued {
            work.priority = priority;
        }
        queued.sort_by_key(|work| work.segment_id.file_id.file_index);
        for work in queued {
            state.download_queue.push(work);
        }
    }
    pipeline.hot_dispatch_job = Some(job_id);
    let pressure = pipeline.refresh_download_pressure();

    let lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("same-priority explicit indexes must batch");
    assert_eq!(lease.works.len(), 2);
    assert!(
        lease.works.iter().all(|work| {
            work.segment_id.file_id.file_index < 2
                && !work.is_recovery
                && !work.completion_critical
                && work.priority == priority
        }),
        "the bootstrap lease must contain only primary explicit-index work"
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .download_queue
            .count_matching(|work| work.segment_id.file_id.file_index == 2)
            == 2,
        "an otherwise compatible ordinary payload must remain queued"
    );
    let payload_head = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .download_queue
        .peek_next_matching(|_| true)
        .map(|work| work.segment_id)
        .unwrap();
    for _ in 0..2 {
        assert!(
            pipeline
                .try_lease_initial_download_batch_for_test(job_id, pressure)
                .is_none(),
            "in-flight indexes keep compatible payload work behind the gate"
        );
    }
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .download_queue
            .peek_next_matching(|_| true)
            .map(|work| work.segment_id),
        Some(payload_head),
        "a blocked lease attempt must not rotate equal-priority payload work"
    );
    assert!(matches!(
        pipeline.par2_discovery_state_for_candidate(job_id, 0),
        Par2DiscoveryState::MetadataCarrierQueued { .. }
    ));
    assert!(matches!(
        pipeline.par2_discovery_state_for_candidate(job_id, 1),
        Par2DiscoveryState::MetadataCarrierQueued { .. }
    ));
    assert!(
        !pipeline
            .list_jobs()
            .into_iter()
            .find(|job| job.job_id == job_id)
            .expect("bootstrapping job remains visible")
            .fetching_repair_data,
        "ordinary explicit-index bootstrap must not surface as completion-critical recovery fetch"
    );

    let second_index = lease
        .works
        .iter()
        .find(|work| work.segment_id.file_id.file_index == 1)
        .expect("the second explicit index must be in the lease")
        .segment_id;
    pipeline.mark_promoted_recovery_segment_unavailable(second_index);
    assert!(pipeline.promote_par2_metadata(job_id));
    assert!(matches!(
        pipeline.par2_discovery_state_for_candidate(job_id, 1),
        Par2DiscoveryState::Exhausted { .. }
    ));
}

#[tokio::test]
async fn par2_metadata_bootstrap_blocks_compatible_payload_refill_until_plan_is_published() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
    let job_id = JobId(40147);
    let payload = b"refill-payload".to_vec();
    let mut grid = placement_par2_file_set(&[("payload.bin".to_string(), payload.clone())]);
    grid.slice_size = 8;
    let spec = JobSpec {
        name: "PAR2 Bootstrap Refill".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "index.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "refill-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload.len() as u32,
                    message_id: "refill-payload@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    let priority = FileRole::Par2 {
        is_index: true,
        recovery_block_count: 0,
    }
    .download_priority();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let mut queued = state.download_queue.drain_all();
        for work in &mut queued {
            work.priority = priority;
        }
        queued.sort_by_key(|work| work.segment_id.file_id.file_index);
        for work in queued {
            state.download_queue.push(work);
        }
    }
    let pressure = pipeline.refresh_download_pressure();

    let index_lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("the explicit index must establish the primary compatibility");
    assert!(
        index_lease
            .works
            .iter()
            .all(|work| work.segment_id.file_id.file_index == 0)
    );
    assert!(
        pipeline
            .try_lease_refill_download_batch_for_test(
                job_id,
                index_lease.compatibility.clone(),
                pressure,
            )
            .is_none(),
        "an open bootstrap must requeue a compatible payload refill"
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .download_queue
            .count_matching(|work| work.segment_id.file_id.file_index == 1),
        1
    );

    let grid_id = grid.recovery_set_id;
    {
        let runtime = pipeline.ensure_par2_runtime(job_id);
        runtime.served = Some(grid_id);
        runtime.ensure_set_runtime(grid_id).set = Some(std::sync::Arc::new(grid));
        runtime.files.insert(
            0,
            Par2FileRuntime {
                filename: "index.par2".to_string(),
                discovery: Par2DiscoveryState::Parsed {
                    set_ids: vec![grid_id],
                },
                ..Default::default()
            },
        );
    }
    pipeline.refresh_par2_checkpoint_plan(job_id);

    let payload_refill = pipeline
        .try_lease_refill_download_batch_for_test(job_id, index_lease.compatibility, pressure)
        .expect("the payload refill must resume once the plan is published");
    assert!(
        payload_refill
            .works
            .iter()
            .all(|work| work.segment_id.file_id.file_index == 1)
    );
    assert!(matches!(
        payload_refill.checkpoint_plan,
        weaver_yenc::CheckpointPlan::Single(_)
    ));
}

#[tokio::test]
async fn par2_metadata_bootstrap_blocks_compatible_ip_replacement_trial_payloads() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
    let job_id = JobId(40148);
    let spec = JobSpec {
        name: "PAR2 Bootstrap IP Trial".to_string(),
        password: None,
        total_bytes: 320,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "index.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "trial-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: (0..4)
                    .map(|number| {
                        segment_spec! {
                            number: number,
                            bytes: 64,
                            message_id: format!("trial-payload-{number}@example.com"),
                        }
                    })
                    .collect(),
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    let priority = FileRole::Par2 {
        is_index: true,
        recovery_block_count: 0,
    }
    .download_priority();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let mut queued = state.download_queue.drain_all();
        for work in &mut queued {
            work.priority = priority;
        }
        queued.sort_by_key(|work| work.segment_id.file_id.file_index);
        for work in queued {
            state.download_queue.push(work);
        }
    }

    assert!(
        pipeline
            .try_lease_ip_replacement_trial_batch_for_test(job_id, 0)
            .is_none(),
        "an IP trial must not build its sample from an open index plus ordinary payloads"
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .download_queue
            .count_matching(|work| work.segment_id.file_id.file_index == 1),
        4,
        "all compatible payload samples must remain queued"
    );
    assert!(matches!(
        pipeline.par2_discovery_state_for_candidate(job_id, 0),
        Par2DiscoveryState::MetadataCarrierQueued { .. }
    ));
}

#[tokio::test]
async fn par2_metadata_bootstrap_releases_payload_after_an_explicit_index_is_unavailable() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
    let job_id = JobId(40144);
    let spec = JobSpec {
        name: "Missing Explicit PAR2 Index Bootstrap".to_string(),
        password: None,
        total_bytes: 192,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "missing.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "missing-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "missing-index-payload@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    let pressure = pipeline.refresh_download_pressure();

    let index_lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("the explicit index must lease before the payload");
    assert!(
        !index_lease.compatibility.is_recovery
            && index_lease.works.iter().all(|work| !work.is_recovery),
        "the unavailable callback must track an explicit primary index"
    );
    pipeline.mark_promoted_recovery_segment_unavailable(index_lease.works[0].segment_id);

    let payload_lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("terminally unavailable metadata must not leave payload blocked forever");
    assert!(
        payload_lease
            .works
            .iter()
            .all(|work| work.segment_id.file_id.file_index == 1)
    );
    assert!(pipeline.par2_metadata_discovery_closed(job_id));
}

#[tokio::test]
async fn par2_metadata_bootstrap_does_not_hold_payload_for_late_indexless_discovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
    let job_id = JobId(40145);
    let payload = b"late-carrier-payload".to_vec();
    let mut grid = placement_par2_file_set(&[("payload.bin".to_string(), payload.clone())]);
    grid.slice_size = 8;
    let index = build_test_par2_index("payload.bin", &payload, 8);
    let spec = JobSpec {
        name: "Late Indexless PAR2 Carrier Bootstrap".to_string(),
        password: None,
        total_bytes: (index.len() + payload.len() + 64) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "00-index.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: index.len() as u32,
                    message_id: "late-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "01-late.vol00+01.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "late-carrier@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload.len() as u32,
                    message_id: "late-payload@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    let pressure = pipeline.refresh_download_pressure();

    let index_lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("the explicit index must lease first");
    assert!(
        index_lease
            .works
            .iter()
            .all(|work| { work.segment_id.file_id.file_index == 0 && !work.is_recovery })
    );

    let grid_id = grid.recovery_set_id;
    {
        let runtime = pipeline.ensure_par2_runtime(job_id);
        runtime.served = Some(grid_id);
        runtime.ensure_set_runtime(grid_id).set = Some(std::sync::Arc::new(grid));
        runtime.files.insert(
            0,
            Par2FileRuntime {
                filename: "00-index.par2".to_string(),
                discovery: Par2DiscoveryState::Parsed {
                    set_ids: vec![grid_id],
                },
                ..Default::default()
            },
        );
    }
    pipeline.refresh_par2_checkpoint_plan(job_id);

    let payload_lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("a late indexless candidate must not become a payload barrier");
    assert!(
        !payload_lease.compatibility.is_recovery
            && payload_lease
                .works
                .iter()
                .all(|work| { work.segment_id.file_id.file_index == 2 && !work.is_recovery }),
        "the known explicit grid must flow into the payload lease"
    );
    assert!(matches!(
        payload_lease.checkpoint_plan,
        weaver_yenc::CheckpointPlan::Single(_)
    ));
    assert!(
        !pipeline.par2_metadata_discovery_closed(job_id),
        "completion must still discover the late indexless set"
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .recovery_queue
            .count_matching(|work| work.segment_id.file_id.file_index == 1),
        1,
        "late recovery work stays parked until completion discovery"
    );
}

/// Recovery rides the same owned lanes as ordinary work.
///
/// Recovery used to be pushed onto the async pool and pinned to sequential
/// mode: the work a job is *waiting on* to finish paid a cold dial and gave
/// back the round trip pipelining exists to hide. A recovery lease is now an
/// ordinary lease as far as lane selection and depth are concerned.
#[tokio::test]
async fn a_recovery_lease_takes_an_owned_lane_at_the_ordinary_depth() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40143);
    // A plaintext server: an owned lane is no longer a TLS-only arrangement.
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "plain.example.invalid".to_string(),
                port: 119,
                tls: false,
                ..Default::default()
            },
            max_connections: 2,
            ..Default::default()
        }],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(1),
    }));
    let work = DownloadWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 1,
            },
            segment_number: 1,
        },
        message_id: MessageId::new("recovery-lane@example.invalid"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 0,
        byte_estimate: 1024,
        retry_count: 0,
        is_recovery: true,
        completion_critical: true,
        exclude_servers: Vec::new(),
        avoid_server: None,
    };
    let compatibility = DownloadBatchCompatibility::from_work(&work);
    let lease = DownloadBatchLease {
        job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        server_modes: Vec::new(),
        compatibility,
        effective_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works: vec![work],
    };

    assert!(
        pipeline.should_use_owned_blocking_lane(&lease),
        "a recovery lease must be eligible for a cached owned lane"
    );

    let pressure = pipeline.refresh_download_pressure();
    let ordinary = pipeline.choose_download_lane_mode(job_id, false, pressure);
    let pressure = pipeline.refresh_download_pressure();
    let recovery = pipeline.choose_download_lane_mode(job_id, true, pressure);
    assert_eq!(
        recovery, ordinary,
        "recovery must not be pinned to a shallower lane than ordinary work"
    );
}

#[tokio::test]
async fn recovery_async_handoff_keeps_owned_lane_caches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let work = DownloadWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: JobId(40141),
                file_index: 1,
            },
            segment_number: 1,
        },
        message_id: MessageId::new("recovery-handoff@example.invalid"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 0,
        byte_estimate: 1024,
        retry_count: 0,
        is_recovery: true,
        completion_critical: false,
        exclude_servers: Vec::new(),
        avoid_server: None,
    };
    let compatibility = DownloadBatchCompatibility::from_work(&work);
    let lease = DownloadBatchLease {
        job_id: work.segment_id.file_id.job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        server_modes: Vec::new(),
        compatibility,
        effective_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works: vec![work],
    };

    assert_eq!(pipeline.owned_download_lane_pool.reset_calls(), 0);
    pipeline.spawn_download_batch(lease);
    assert_eq!(
        pipeline.owned_download_lane_pool.reset_calls(),
        0,
        "async-only recovery reclaims idle owned permits per starved server instead of resetting the owned fleet"
    );
}

#[tokio::test]
async fn hot_lease_work_limit_scales_with_lane_throughput() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40142);
    let article_bytes: u32 = 768 * 1024;

    // No measured throughput yet: cold-start batch, not the full 64 — the
    // first dispatch wave must not blind-lease several volumes on a slow link.
    assert_eq!(
        pipeline.hot_lease_work_limit(
            job_id,
            DownloadLaneMode::Pipelined { depth: 4 },
            article_bytes
        ),
        16
    );

    // 120MB/s across 10 lanes -> 12MB/s per lane -> 2s runway is ~30 articles.
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 10);
    pipeline
        .hot_dispatch_throughput_window
        .record(Instant::now(), 240_000_000);
    assert_eq!(
        pipeline.hot_lease_work_limit(
            job_id,
            DownloadLaneMode::Pipelined { depth: 4 },
            article_bytes
        ),
        30
    );

    // 7.5MB/s across 10 lanes -> under one article of runway -> clamp to the
    // lane's pipeline depth so slow links cycle back to the queue head.
    pipeline.hot_dispatch_throughput_window.clear();
    pipeline
        .hot_dispatch_throughput_window
        .record(Instant::now(), 15_000_000);
    assert_eq!(
        pipeline.hot_lease_work_limit(
            job_id,
            DownloadLaneMode::Pipelined { depth: 4 },
            article_bytes
        ),
        4
    );

    // The floor is the lane's own depth, so a deeper rung never leases less
    // than one full pipeline's worth of work.
    assert_eq!(
        pipeline.hot_lease_work_limit(
            job_id,
            DownloadLaneMode::Pipelined { depth: 8 },
            article_bytes
        ),
        8
    );
    assert_eq!(
        pipeline.hot_lease_work_limit(job_id, DownloadLaneMode::Sequential, article_bytes),
        1
    );
}

/// Lease one batch per lane the way a dispatch wave does, keeping the two
/// connection counters the fair-share divisor reads in step with it.
fn lease_one_wave(pipeline: &mut Pipeline, job_id: JobId, lanes: usize) -> Vec<usize> {
    let mut leased = Vec::new();
    for _ in 0..lanes {
        let pressure = pipeline.refresh_download_pressure();
        let Some(lease) = pipeline.try_lease_initial_download_batch_for_test(job_id, pressure)
        else {
            break;
        };
        leased.push(lease.works.len());
        pipeline.active_download_connections += 1;
        *pipeline
            .active_download_connections_by_job
            .entry(job_id)
            .or_default() += 1;
    }
    leased
}

/// A job's tail must be split across every lane instead of being handed to
/// whichever lane leases first.
///
/// With a full runway the first lane used to take the whole remainder (up to
/// 64 articles), so the last lane drained its batch alone at one article per
/// round trip while the rest of the fleet had already finished. That tail is
/// invisible at zero latency and costs a large fraction of a second per job
/// at 100 ms.
#[tokio::test]
async fn hot_lease_splits_a_job_tail_across_every_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        8,
    )
    .await;

    let job_id = JobId(40143);
    let files = many_standalone_files("lane-tail", 33);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Lane Tail", &files),
    )
    .await;
    pipeline.hot_dispatch_job = Some(job_id);
    // Measured throughput, so the runway limit is the full 64-article batch:
    // the bound under test must be the fair share, not the cold-start clamp.
    pipeline
        .hot_dispatch_throughput_window
        .record(Instant::now(), 240_000_000);

    let leased = lease_one_wave(&mut pipeline, job_id, 8);

    assert_eq!(leased.len(), 8, "every lane must get work: {leased:?}");
    assert!(
        leased.iter().all(|count| *count >= 1),
        "no lane may lease an empty batch: {leased:?}"
    );
    let fair_share = 33usize.div_ceil(8);
    assert!(
        leased.iter().all(|count| *count <= fair_share),
        "no lane may take more than one lane's share of the remainder: {leased:?}"
    );
    assert!(
        leased.iter().sum::<usize>() <= 33,
        "a wave cannot lease more than the queue holds: {leased:?}"
    );
}

/// Promoted work is drained by every parked lane, not by whichever lane the
/// next dispatch pass happens to start first.
///
/// A targeted recovery promotion lands as one burst of completion-critical
/// work while every lane of the job is parked on `NoWork`. Sized by the
/// runway alone, the first lane leased the entire promoted set and fetched it
/// serially; the fair share splits it so the whole fleet drains it in
/// parallel.
#[tokio::test]
async fn promoted_completion_critical_work_spreads_across_parked_lanes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        8,
    )
    .await;

    let job_id = JobId(40144);
    let files = many_standalone_files("promoted-recovery", 45);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Promoted Recovery", &files),
    )
    .await;
    let promoted = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .promote_matching_to_completion_critical_with_rank(|_| Some((0, None)));
    assert_eq!(promoted, 45);
    pipeline.hot_dispatch_job = Some(job_id);
    pipeline
        .hot_dispatch_throughput_window
        .record(Instant::now(), 240_000_000);

    // Every lane of the job is parked: this is the state a promotion wakes.
    assert!(pipeline.active_download_connections_by_job.is_empty());
    assert_eq!(pipeline.active_download_connections, 0);

    let leased = lease_one_wave(&mut pipeline, job_id, 8);

    assert_eq!(
        leased.len(),
        8,
        "the promoted set must reach every lane: {leased:?}"
    );
    let fair_share = 45usize.div_ceil(8);
    assert!(
        leased.iter().all(|count| (1..=fair_share).contains(count)),
        "no lane may take the whole promoted set: {leased:?}"
    );
}

#[tokio::test]
async fn shutdown_drain_consumes_inflight_download_results() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: JobId(42),
            file_index: 0,
        },
        segment_number: 1,
    };

    pipeline.active_downloads = 1;
    pipeline
        .active_downloads_by_job
        .insert(segment_id.file_id.job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment_id.file_id, 1);

    pipeline
        .download_done_tx
        .send(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
                "shutdown test",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), pipeline.drain())
        .await
        .expect("shutdown drain should consume queued download result");

    assert_eq!(pipeline.active_downloads, 0);
    assert!(
        !pipeline
            .active_downloads_by_job
            .contains_key(&segment_id.file_id.job_id)
    );
    assert!(
        !pipeline
            .active_downloads_by_file
            .contains_key(&segment_id.file_id)
    );
}

#[tokio::test]
async fn transient_retry_backoff_does_not_fail_job_early() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20008);
    let spec = segmented_job_spec("Retry Backoff Guard", "retry.bin", &[128, 128]);
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
                DownloadFailureKind::EstablishedTransport,
                "connection reset by peer",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, 0);
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );

    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should remain in the batched infrastructure queue");
    assert_eq!(retry.retry_count, 0);

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id: retry.segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
                "connection reset by peer",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES,
            exclude_servers: retry.exclude_servers,
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    assert_eq!(pipeline.jobs[&job_id].failed_bytes, 0);
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(retry.retry_count, MAX_SEGMENT_RETRIES);
}

/// A transport-failure retry must point away from the server that just
/// failed when an alternative exists (`avoid_server`), without entering the
/// 430-exhaustion ledger — otherwise sustained stochastic stall on a
/// healthy-looking primary can burn every retry on the same server while a
/// clean backup idles, and one transient timeout could later help declare an
/// article missing.
#[tokio::test]
async fn transport_failure_retry_rotates_off_the_failed_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0, 0]));
    let job_id = JobId(20009);
    let spec = segmented_job_spec("Transport Rotation", "rotation.bin", &[128]);
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
                DownloadFailureKind::EstablishedTransport,
                "article fetch soft timeout (15s)",
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

    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should requeue");
    assert_eq!(retry.avoid_server, Some(0));
    assert!(
        retry.exclude_servers.is_empty(),
        "rotation must not enter the article-not-found exhaustion ledger"
    );
    assert_eq!(retry.retry_count, 0);
}

/// With a single configured server there is nowhere to rotate to: the retry
/// must stay eligible for that server instead of dead-ending.
#[tokio::test]
async fn transport_failure_retry_keeps_single_server_eligible() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    let job_id = JobId(20010);
    let spec = segmented_job_spec("Transport Rotation Solo", "rotation-solo.bin", &[128]);
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
                DownloadFailureKind::EstablishedTransport,
                "article fetch soft timeout (15s)",
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

    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should requeue");
    assert_eq!(retry.avoid_server, None);
    assert!(retry.exclude_servers.is_empty());
}

/// A backfill server is not a rotation target: the avoid hint joins the
/// lease's effective excludes, and excluding the whole fill tier would
/// unlock backfill (`fill_servers_exhausted`) for work the fill server can
/// serve on the next attempt — backfill is reserved for missing articles.
#[tokio::test]
async fn transport_failure_retry_does_not_rotate_toward_backfill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let servers = vec![
        weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "fill.example.com".into(),
                ..Default::default()
            },
            max_connections: 2,
            ..weaver_nntp::pool::ServerPoolConfig::default()
        },
        weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "backfill.example.com".into(),
                ..Default::default()
            },
            max_connections: 2,
            backfill: true,
            ..weaver_nntp::pool::ServerPoolConfig::default()
        },
    ];
    pipeline.nntp = std::sync::Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers,
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        },
    ));
    let job_id = JobId(20011);
    let spec = segmented_job_spec("Transport Rotation Backfill", "rotation-bf.bin", &[128]);
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
                DownloadFailureKind::EstablishedTransport,
                "article fetch soft timeout (15s)",
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

    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should requeue");
    assert_eq!(
        retry.avoid_server, None,
        "rotation must not manufacture fill-tier exhaustion"
    );
}

#[test]
fn lane_acquire_failure_preserves_retry_semantics() {
    for error in [
        weaver_nntp::NntpError::SoftTimeout(15),
        weaver_nntp::NntpError::TruncatedMultilineBody,
        weaver_nntp::NntpError::MalformedMultilineTerminator,
    ] {
        let failure = DownloadFailure::from_nntp(error);
        assert_eq!(failure.kind, DownloadFailureKind::EstablishedTransport);
        assert!(failure.kind.preserves_article_retry_budget());
        assert!(failure.kind.infrastructure_wait_reason().is_some());
    }
    let unavailable = DownloadFailure::from_lane_acquire_failure(None);
    assert_eq!(unavailable.kind, DownloadFailureKind::LaneUnavailable);

    let pool_miss =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::PoolExhausted));
    assert_eq!(pool_miss.kind, DownloadFailureKind::CapacityUnavailable);

    let timeout =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::SoftTimeout(15)));
    assert_eq!(timeout.kind, DownloadFailureKind::ConnectionEstablishment);

    let io_failure = DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::Io(
        std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused"),
    )));
    assert_eq!(
        io_failure.kind,
        DownloadFailureKind::ConnectionEstablishment
    );

    let auth_failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::AuthenticationFailed,
    ));
    assert_eq!(auth_failure.kind, DownloadFailureKind::Auth);

    let setup_failure =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::NoSuchGroup));
    assert_eq!(setup_failure.kind, DownloadFailureKind::ContentOrProtocol);
}

#[tokio::test]
async fn group_discovery_at_retry_limit_preserves_the_article_for_a_grouped_retry() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20024);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Group discovery", "group.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::from_nntp(
                weaver_nntp::NntpError::NoGroupSelected,
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.failed_bytes, 0);
    assert_eq!(state.status, JobStatus::Downloading);
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(retry.segment_id, segment_id);
    assert_eq!(retry.retry_count, MAX_SEGMENT_RETRIES);
    assert!(retry.exclude_servers.is_empty());
    assert_eq!(
        retry.avoid_server, None,
        "the same provider can select the learned group"
    );

    let setup =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::NoGroupSelected));
    assert!(setup.kind.preserves_article_retry_budget());
}

#[test]
fn infrastructure_failures_preserve_article_retry_budget() {
    for kind in [
        DownloadFailureKind::CapacityUnavailable,
        DownloadFailureKind::ConnectionEstablishment,
        DownloadFailureKind::EstablishedTransport,
        DownloadFailureKind::Auth,
        DownloadFailureKind::ServerQuota,
        DownloadFailureKind::LaneUnavailable,
        DownloadFailureKind::Unrequested,
    ] {
        assert!(kind.preserves_article_retry_budget(), "kind={kind:?}");
    }
    for kind in [
        DownloadFailureKind::ArticleNotFound,
        DownloadFailureKind::ContentOrProtocol,
    ] {
        assert!(!kind.preserves_article_retry_budget(), "kind={kind:?}");
    }
    assert!(
        DownloadFailureKind::EstablishedTransport
            .infrastructure_wait_reason()
            .is_some()
    );
}

#[tokio::test]
async fn pool_capacity_failure_at_retry_limit_does_not_poison_health() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20013);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Pool Capacity Guard", "retry.bin", &[128]);
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
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::CapacityUnavailable,
                "no connections available",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES,
            exclude_servers: vec![0],
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert!(pipeline.unavailable_promoted_recovery_segments.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );

    assert_eq!(
        pipeline
            .download_wait_by_job
            .get(&job_id)
            .map(|wait| wait.reason),
        Some("provider connection capacity unavailable")
    );
    tokio::task::yield_now().await;
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("generation wake should requeue parked infrastructure work");
    assert_eq!(retry.segment_id, segment_id);
    assert_eq!(retry.retry_count, MAX_SEGMENT_RETRIES);
    assert_eq!(retry.exclude_servers, vec![0]);
    assert!(!pipeline.download_wait_by_job.contains_key(&job_id));
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn body_lane_unavailable_at_retry_limit_requeues_without_article_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20014);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Lane Acquire Guard", "retry.bin", &[128]);
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
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::LaneUnavailable,
                "failed to acquire BODY lane",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: vec![0, 1],
            release_connection_slot: false,
        })
        .await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert!(pipeline.unavailable_promoted_recovery_segments.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    tokio::task::yield_now().await;
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        pipeline
            .download_wait_by_job
            .get(&job_id)
            .map(|wait| wait.reason),
        Some("no eligible NNTP server")
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(pipeline.retry_rx.try_recv().is_err());
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("generation wake should requeue parked pre-BODY work without retry charge");
    assert_eq!(retry.segment_id, segment_id);
    assert_eq!(retry.retry_count, MAX_SEGMENT_RETRIES + 1);
    assert_eq!(retry.exclude_servers, vec![0, 1]);
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        0
    );
    assert!(!pipeline.download_wait_by_job.contains_key(&job_id));
}

#[tokio::test]
async fn generation_wake_requeues_ten_thousand_segments_in_one_batch() {
    const RETRY_COUNT: usize = 10_000;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20020);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Generation Batch", "retry.bin", &[128]),
    )
    .await;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
    pipeline.pending_retries_by_job.insert(job_id, RETRY_COUNT);
    pipeline.download_wait_by_job.insert(
        job_id,
        DownloadWaitStatus {
            reason: "no eligible NNTP server",
            retry_at_epoch_ms: None,
            pending_count: RETRY_COUNT,
        },
    );

    for segment_number in 0..RETRY_COUNT as u32 {
        let segment_id = SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        };
        pipeline.pending_retries_by_segment.insert(segment_id, 1);
        pipeline.schedule_infrastructure_retry(
            None,
            RetryWork {
                scheduled_pool_generation: pipeline.pool_generation,
                infrastructure_retry: true,
                work: DownloadWork {
                    segment_id,
                    message_id: crate::jobs::ids::MessageId::new(&format!(
                        "<batch-{segment_number}>"
                    )),
                    groups: std::sync::Arc::from(Vec::<String>::new()),
                    priority: 0,
                    byte_estimate: 128,
                    retry_count: MAX_SEGMENT_RETRIES,
                    is_recovery: false,
                    completion_critical: false,
                    exclude_servers: vec![0],
                    avoid_server: None,
                },
            },
        );
    }

    let mut job_changes = pipeline.shared_state.subscribe_job_changes();
    let previous_revision = *job_changes.borrow();
    assert_eq!(pipeline.wake_all_infrastructure_retries(), RETRY_COUNT);
    pipeline.publish_snapshot();
    job_changes.changed().await.unwrap();

    assert_eq!(*job_changes.borrow(), previous_revision.wrapping_add(1));
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        RETRY_COUNT
    );
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    assert!(pipeline.pending_retries_by_segment.is_empty());
    assert!(!pipeline.download_wait_by_job.contains_key(&job_id));
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn stale_generation_transport_failure_is_requeued_without_poisoning_health() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20015);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Generation Fence", "retry.bin", &[128]),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.pool_generation = 2;
    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 1,
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
                "old generation connection reset",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 10,
            exclude_servers: vec![0],
            release_connection_slot: true,
        })
        .await;

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    assert_eq!(state.failed_bytes, 0);
    let queued = state.download_queue.drain_all();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].segment_id, segment_id);
    assert_eq!(queued[0].retry_count, MAX_SEGMENT_RETRIES + 10);
    assert!(queued[0].exclude_servers.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .nntp_generation_recovery_requeues
            .load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn stale_generation_success_does_not_update_new_lane_health() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.pool_generation = 2;
    pipeline.active_downloads = 1;
    pipeline.active_download_connections = 1;

    pipeline.release_download_result(&DownloadResult {
        runtime_generation: 1,
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: JobId(20017),
                file_index: 0,
            },
            segment_number: 0,
        },
        data: Ok(DownloadPayload::Raw(Bytes::from_static(b"article"))),
        attempts: Vec::new(),
        lane_observation: Some(DownloadLaneObservation {
            server_idx: Some(0),
            mode: DownloadLaneMode::Pipelined { depth: 2 },
            supports_pipelining: true,
            latency: Some(Duration::from_millis(5)),
            transfer: Some(Duration::from_millis(20)),
            payload_bytes: 7,
            policy_elapsed: Duration::from_millis(25),
            pressure_clear: true,
            batch_complete: true,
            batch_clean: true,
            unresolved_count: 0,
            connection_discarded: false,
        }),
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: Vec::new(),
        release_connection_slot: true,
    });

    assert_eq!(
        pipeline
            .metrics
            .download_pipeline_trial_success_total
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn terminal_accounting_is_idempotent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20016);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Terminal Accounting", "retry.bin", &[128, 10_000]),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.book_failed_segment(segment_id);
    pipeline.book_failed_segment(segment_id);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, 128);
    assert_eq!(pipeline.segment_terminal_states.len(), 1);
    assert_eq!(pipeline.derived_failed_bytes(job_id), 128);

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Failed {
            error: "retry requested".to_string(),
        };
    }
    pipeline.reprocess_job(job_id).await.unwrap();
    assert!(!pipeline.segment_terminal_states.contains_key(&segment_id));
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, 0);

    // Reprocessing a job that is still resident rebuilds its assembly with
    // every segment already committed, so the ledger has nothing left to book:
    // delivery is a terminal state too, and it outranks a failure result.
    pipeline.book_failed_segment(segment_id);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, 0);
    assert!(!pipeline.segment_terminal_states.contains_key(&segment_id));
    pipeline.purge_terminal_job_runtime(job_id);
    assert!(!pipeline.segment_terminal_states.contains_key(&segment_id));
}

/// Every segment of a file dies, some of them only after a retry that also
/// died. The ledger is the sum of the declared segment sizes, counted once
/// each — not once per attempt.
#[tokio::test]
async fn a_file_whose_every_segment_dies_books_each_declared_size_exactly_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20018);
    let segment_sizes = [1_000u32, 2_000, 3_000, 4_000];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    {
        // Health policy is not what is under test. A recovery set large enough
        // to make the critical threshold zero keeps a wholly dead file in the
        // deferral arm instead of aborting the job out from under the ledger.
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.next_health_probe_failed_bytes = u64::MAX;
        state.par2_bytes = state.spec.total_bytes;
    }

    let segment = |segment_number| SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number,
    };

    // Each ordinal is retired twice over, through both wire-outcome paths, in
    // the order a real pass produces: a first attempt gives up, the retry that
    // followed it gives up as well.
    for segment_number in 0..segment_sizes.len() as u32 {
        pipeline.book_terminal_segment(segment(segment_number), SegmentTerminalState::Missing);
        pipeline.book_terminal_segment(
            segment(segment_number),
            SegmentTerminalState::RetriesExhausted,
        );
    }

    let expected: u64 = segment_sizes.iter().map(|bytes| *bytes as u64).sum();
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, expected);
    assert_eq!(pipeline.derived_failed_bytes(job_id), expected);
    assert_eq!(
        pipeline.segment_terminal_states.len(),
        segment_sizes.len(),
        "a segment holds one terminal state, and the first one it reached"
    );
    assert!(
        pipeline
            .segment_terminal_states
            .values()
            .all(|state| *state == SegmentTerminalState::Missing)
    );
}

/// A mixed pass: some segments fail, retry, and fail again; others fail once
/// and then arrive. Only the terminal failures are booked, and the segments
/// that landed contribute nothing however many times they failed first.
#[tokio::test]
async fn segments_that_arrive_after_a_failure_leave_the_ledger_untouched() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20019);
    let segment_sizes = [1_000u32, 2_000, 3_000, 4_000];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.next_health_probe_failed_bytes = u64::MAX;
        state.par2_bytes = state.spec.total_bytes;
    }

    let segment = |segment_number| SegmentId {
        file_id,
        segment_number,
    };

    // Ordinals 1 and 3 fail once, then arrive on the retry.
    pipeline.book_terminal_segment(segment(0), SegmentTerminalState::Missing);
    pipeline.book_terminal_segment(segment(2), SegmentTerminalState::DecodeExhausted);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(file_id).unwrap();
        file.commit_segment(1, 2_000).unwrap();
        file.commit_segment(3, 4_000).unwrap();
    }
    // A losing racer for an ordinal that already landed reports afterwards.
    pipeline.book_terminal_segment(segment(1), SegmentTerminalState::RetriesExhausted);
    pipeline.book_terminal_segment(segment(3), SegmentTerminalState::Missing);
    // And the two dead ordinals exhaust their own retries.
    pipeline.book_terminal_segment(segment(0), SegmentTerminalState::RetriesExhausted);
    pipeline.book_terminal_segment(segment(2), SegmentTerminalState::RetriesExhausted);

    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, 4_000);
    assert_eq!(pipeline.derived_failed_bytes(job_id), 4_000);
    assert_eq!(pipeline.segment_terminal_states.len(), 2);
}

/// `weaver_files_missing_total` counts **files**, not failed segments, and it
/// counts them where the download pass gives up rather than at each permanent
/// segment failure: two dead segments in one file are one missing file.
#[tokio::test]
async fn exhausted_download_counts_one_missing_file_with_its_missing_segments() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20017);
    let segment_sizes = [4_000u32; 20];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;

    // Two of the twenty segments exhaust their retries. Health probes are not
    // what is under test, so hold them off; the surviving 90% health is above
    // the no-PAR2 critical threshold either way, so the job stays alive to
    // reach the completion check below.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.next_health_probe_failed_bytes = u64::MAX;
    }
    for segment_number in 0..2 {
        pipeline.book_failed_segment(SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        });
    }
    assert!(
        pipeline.jobs.contains_key(&job_id),
        "two permanent failures must not be enough to abort the job"
    );

    // The queues drain with the file still short: nothing else is coming.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    let before = pipeline.metrics.job_lifecycle.snapshot();
    assert_eq!(before.files_missing_total, 0);
    assert_eq!(before.missing_segments_total, 0);

    pipeline.check_job_completion(job_id).await;

    let snapshot = pipeline.metrics.job_lifecycle.snapshot();
    // One file, counted once, carrying every segment it never received — not
    // one row per booked segment failure.
    assert_eq!(snapshot.files_missing_total, 1);
    assert_eq!(
        snapshot.missing_segments_total,
        u64::from(segment_sizes.len() as u32)
    );
}

/// The completion check re-enters many times per job, so the per-file guard is
/// what keeps `weaver_files_missing_total` from climbing on every pass.
#[tokio::test]
async fn missing_files_are_counted_once_across_repeated_completion_checks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20019);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec(
            "Silver Horizon Encore",
            "encore.mkv",
            &[4_000, 4_000, 4_000],
        ),
    )
    .await;

    for _ in 0..5 {
        pipeline.note_incomplete_files_after_download_drain(job_id);
    }

    let snapshot = pipeline.metrics.job_lifecycle.snapshot();
    assert_eq!(snapshot.files_missing_total, 1);
    assert_eq!(snapshot.missing_segments_total, 3);
}

/// A job with no PAR2 set can never produce an `intact`/`damaged`/`missing`
/// verdict, so it is accounted for as `unverifiable` at its terminal
/// transition instead of being left out of `weaver_verifications_total`.
#[tokio::test]
async fn job_without_par2_records_one_unverifiable_verification() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20018);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon Reprise", "reprise.mkv", &[4_000, 4_000]),
    )
    .await;
    assert!(pipeline.par2_set(job_id).is_none());

    fn unverifiable(pipeline: &Pipeline) -> u64 {
        pipeline
            .metrics
            .job_lifecycle
            .snapshot()
            .verifications
            .iter()
            .find(|(result, _)| *result == "unverifiable")
            .map(|(_, count)| *count)
            .expect("unverifiable is one of the pre-declared verification results")
    }

    assert_eq!(unverifiable(&pipeline), 0);

    // A live job reaching the decision twice contributes one row: the guard
    // set is claimed by the first, and a real verdict would have claimed it
    // first of all.
    pipeline.note_job_unverifiable_if_no_par2_set(job_id);
    pipeline.note_job_unverifiable_if_no_par2_set(job_id);
    assert_eq!(unverifiable(&pipeline), 1);

    // The terminal transition itself is one of those decisions, so a failure
    // after the fact adds nothing.
    pipeline.fail_job(job_id, "no article ever landed".to_string());
    assert_eq!(unverifiable(&pipeline), 1);
}

/// The same terminal transition on a job that *does* carry a recovery set
/// records nothing: that job's verdict is the verification pass's to report.
#[tokio::test]
async fn job_with_par2_set_records_no_unverifiable_verification() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20020);
    let payload_filename = "payload.mkv";
    let payload: Vec<u8> = (0..32u32).map(|value| (value % 251) as u8).collect();
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon Recovered",
            &[(payload_filename.to_string(), payload.len() as u32)],
        ),
    )
    .await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(payload_filename.to_string(), payload)]),
        &[],
    );
    assert!(pipeline.par2_set(job_id).is_some());

    pipeline.note_job_unverifiable_if_no_par2_set(job_id);
    pipeline.fail_job(job_id, "disk went away".to_string());

    let verifications = pipeline.metrics.job_lifecycle.snapshot().verifications;
    assert!(
        verifications.iter().all(|(_, count)| *count == 0),
        "a job with a recovery set must not be attributed to any verdict here: {verifications:?}"
    );
}

#[tokio::test]
async fn server_quota_lane_failure_parks_until_retry_at_without_lane_spin() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20997);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Server Quota Guard", "retry.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        weaver_nntp::transfer::StableServerId(7),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 128,
                generation: 9,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    let _reservation = control.try_reserve(128).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("quota must reject the next reservation");
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));
    assert_eq!(failure.kind, DownloadFailureKind::ServerQuota);
    assert!(
        failure
            .retry_after
            .is_some_and(|delay| delay > Duration::from_secs(20))
    );

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        pipeline.retry_rx.try_recv().is_err(),
        "server quota retry must wait for retry_at or an explicit policy change"
    );

    pipeline.jobs.remove(&job_id);
    pipeline.shared_state.publish_jobs(Vec::new());
    let cancelled = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("job removal must wake an indefinite quota waiter")
        .expect("retry channel must stay open");
    pipeline.receive_retry_work(cancelled);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
}

#[tokio::test]
async fn quota_acquire_failure_requeues_smaller_tail_for_independent_selection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21002);
    let large_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let tail_segment = SegmentId {
        segment_number: 1,
        ..large_segment
    };
    let large_bytes = 5_000;
    let tail_bytes = 1_000;
    let spec = segmented_job_spec(
        "Heterogeneous Quota Lease",
        "heterogeneous.bin",
        &[large_bytes, tail_bytes],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 2;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 2);

    let policy = std::sync::Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            Database::open_in_memory().unwrap(),
            &[],
        )
        .unwrap(),
    );
    let stable_id = weaver_nntp::transfer::StableServerId(17);
    let control = policy.transfer_registry().configure(
        stable_id,
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 10_000,
                generation: 17,
                retry_at: None,
            }),
        },
    );
    let reservation = control.try_reserve(5_000).unwrap();
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![
            weaver_nntp::pool::ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: "heterogeneous-fill.example.com".into(),
                    ..Default::default()
                },
                stable_id,
                transfer_control: Some(std::sync::Arc::clone(&control)),
                max_connections: 1,
                group: 0,
                backfill: false,
                retention_days: 0,
            },
            weaver_nntp::pool::ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: "heterogeneous-backfill.example.com".into(),
                    ..Default::default()
                },
                stable_id: weaver_nntp::transfer::StableServerId(18),
                transfer_control: None,
                max_connections: 1,
                group: 0,
                backfill: true,
                retention_days: 0,
            },
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    }));
    pipeline.shared_state.set_server_transfer_policy(policy);

    let large_estimate = Pipeline::bandwidth_reservation_estimate(large_bytes);
    let blocked = pipeline
        .nntp
        .body_server_selection_with_estimate(&[], large_estimate)
        .await;
    assert!(blocked.eligible.is_empty());
    let failure =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::quota_blocked(
            blocked
                .quota_blocked
                .expect("large first work must be blocked"),
        )));
    let first_failure = crate::pipeline::download::lane_acquire_failure_for_work(&failure, 0);
    let tail_failure = crate::pipeline::download::lane_acquire_failure_for_work(&failure, 1);
    assert_eq!(first_failure.kind, DownloadFailureKind::ServerQuota);
    assert_eq!(tail_failure.kind, DownloadFailureKind::Unrequested);

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id: large_segment,
            data: Err(DownloadError::Fetch(first_failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 4,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;
    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id: tail_segment,
            data: Err(DownloadError::Fetch(tail_failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 7,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    assert!(pipeline.server_quota_parked.contains(&large_segment));
    assert!(!pipeline.server_quota_parked.contains(&tail_segment));
    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1),
        "only the tested large work should be quota-parked"
    );
    let tail_work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("untested tail must re-enter the download queue");
    assert_eq!(tail_work.segment_id, tail_segment);
    assert_eq!(tail_work.retry_count, 7);
    let tail_selection = pipeline
        .nntp
        .body_server_selection_with_estimate(
            &tail_work.exclude_servers,
            Pipeline::bandwidth_reservation_estimate(tail_work.byte_estimate),
        )
        .await;
    assert_eq!(tail_selection.eligible, vec![weaver_nntp::ServerId(0)]);
    assert!(
        !tail_selection.eligible.contains(&weaver_nntp::ServerId(1)),
        "reselecting the tail must not unlock backfill"
    );

    pipeline.jobs.remove(&job_id);
    pipeline.shared_state.publish_jobs(Vec::new());
    let cancelled = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("job removal must wake the parked large work")
        .expect("retry channel must stay open");
    pipeline.receive_retry_work(cancelled);
    drop(reservation);
}

#[tokio::test]
async fn server_quota_reservation_refund_wakes_parked_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20998);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Server Quota Refund", "refund.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let policy = std::sync::Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            Database::open_in_memory().unwrap(),
            &[],
        )
        .unwrap(),
    );
    let control = policy.transfer_registry().configure(
        weaver_nntp::transfer::StableServerId(8),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 128,
                generation: 10,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    pipeline.shared_state.set_server_transfer_policy(policy);
    let reservation = control.try_reserve(128).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("quota must reject while the estimate is reserved");
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(pipeline.retry_rx.try_recv().is_err());

    drop(reservation);

    let retry = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("reservation refund must wake quota-parked work")
        .expect("retry channel must stay open");
    assert_eq!(retry.work.segment_id, segment_id);
    pipeline.receive_retry_work(retry);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
}

#[tokio::test]
async fn server_quota_source_failure_keeps_backfill_locked_and_fails_over_to_fill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20999);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Server Quota Failover", "failover.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        weaver_nntp::transfer::StableServerId(9),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 128,
                generation: 11,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    let _reservation = control.try_reserve(128).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("quota must reject the next reservation");
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));

    let selection_server = |stable_id: u32,
                            backfill: bool,
                            transfer_control: Option<
        std::sync::Arc<weaver_nntp::transfer::ServerTransferControl>,
    >| weaver_nntp::pool::ServerPoolConfig {
        server: weaver_nntp::ServerConfig {
            host: format!("quota-selection-{stable_id}.example.com"),
            ..Default::default()
        },
        stable_id: weaver_nntp::transfer::StableServerId(stable_id),
        transfer_control,
        max_connections: 1,
        group: 0,
        backfill,
        retention_days: 0,
    };
    let two_fills = NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(9, false, Some(std::sync::Arc::clone(&control))),
            selection_server(11, false, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    pipeline.nntp = std::sync::Arc::new(two_fills);

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    let retry = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("a quota-blocked source must immediately fail over")
        .expect("retry channel must stay open");
    assert_eq!(retry.work.segment_id, segment_id);
    assert_eq!(
        retry.work.exclude_servers,
        Vec::<usize>::new(),
        "quota-blocked sources must not count as exhausted fills and unlock backfill"
    );
    assert!(!pipeline.server_quota_parked.contains(&segment_id));
    assert_ne!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::ServerQuota
    );

    let fill_and_backfill = NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(9, false, Some(std::sync::Arc::clone(&control))),
            selection_server(10, true, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    let quota_retry = fill_and_backfill
        .body_server_selection_with_estimate(&retry.work.exclude_servers, 1)
        .await;
    assert!(quota_retry.eligible.is_empty());
    assert!(quota_retry.quota_blocked.is_some());
    let missing_or_retention_retry = fill_and_backfill
        .body_server_selection_with_estimate(&[0], 1)
        .await;
    assert_eq!(
        missing_or_retention_retry.eligible,
        vec![weaver_nntp::ServerId(1)],
        "generic missing/retention exhaustion must still unlock backfill"
    );

    let two_fills = NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(9, false, Some(std::sync::Arc::clone(&control))),
            selection_server(11, false, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    let fill_failover = two_fills
        .body_server_selection_with_estimate(&retry.work.exclude_servers, 1)
        .await;
    assert_eq!(fill_failover.eligible, vec![weaver_nntp::ServerId(1)]);

    pipeline.receive_retry_work(retry);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
}

#[tokio::test]
async fn server_quota_source_failure_parks_while_only_backfill_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21000);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Server Quota Backfill Lock", "backfill-lock.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        weaver_nntp::transfer::StableServerId(12),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 128,
                generation: 12,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    let reservation = control.try_reserve(128).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("quota must reject the next reservation");
    let selection_server = |stable_id: u32,
                            backfill: bool,
                            transfer_control: Option<
        std::sync::Arc<weaver_nntp::transfer::ServerTransferControl>,
    >| weaver_nntp::pool::ServerPoolConfig {
        server: weaver_nntp::ServerConfig {
            host: format!("quota-park-{stable_id}.example.com"),
            ..Default::default()
        },
        stable_id: weaver_nntp::transfer::StableServerId(stable_id),
        transfer_control,
        max_connections: 1,
        group: 0,
        backfill,
        retention_days: 0,
    };
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(12, false, Some(std::sync::Arc::clone(&control))),
            selection_server(13, true, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    }));
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        pipeline.retry_rx.try_recv().is_err(),
        "quota must not unlock the backfill tier or spin an immediate retry"
    );
    assert!(pipeline.server_quota_parked.contains(&segment_id));
    assert_eq!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::ServerQuota
    );

    pipeline.jobs.remove(&job_id);
    pipeline.shared_state.publish_jobs(Vec::new());
    let cancelled = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("job removal must wake the quota waiter")
        .expect("retry channel must stay open");
    pipeline.receive_retry_work(cancelled);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    drop(reservation);
}

#[tokio::test]
async fn other_fill_refund_wakes_manual_quota_park_without_unlocking_backfill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21001);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Manual Quota Refund", "manual-refund.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let policy = std::sync::Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            Database::open_in_memory().unwrap(),
            &[],
        )
        .unwrap(),
    );
    let first_stable_id = weaver_nntp::transfer::StableServerId(14);
    let second_stable_id = weaver_nntp::transfer::StableServerId(15);
    let manual_quota = |generation| weaver_nntp::transfer::ServerTransferConfig {
        rate_bytes_per_sec: 0,
        quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
            limit_bytes: 128,
            generation,
            retry_at: None,
        }),
    };
    let first = policy
        .transfer_registry()
        .configure(first_stable_id, manual_quota(14));
    let second = policy
        .transfer_registry()
        .configure(second_stable_id, manual_quota(15));
    let first_reservation = first.try_reserve(128).unwrap();
    let second_reservation = second.try_reserve(128).unwrap();
    let selection_server = |stable_id: weaver_nntp::transfer::StableServerId,
                            backfill: bool,
                            transfer_control: Option<
        std::sync::Arc<weaver_nntp::transfer::ServerTransferControl>,
    >| weaver_nntp::pool::ServerPoolConfig {
        server: weaver_nntp::ServerConfig {
            host: format!("manual-quota-{}.example.com", stable_id.0),
            ..Default::default()
        },
        stable_id,
        transfer_control,
        max_connections: 1,
        group: 0,
        backfill,
        retention_days: 0,
    };
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(first_stable_id, false, Some(std::sync::Arc::clone(&first))),
            selection_server(
                second_stable_id,
                false,
                Some(std::sync::Arc::clone(&second)),
            ),
            selection_server(weaver_nntp::transfer::StableServerId(16), true, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    }));
    pipeline.shared_state.set_server_transfer_policy(policy);

    let blocked = pipeline
        .nntp
        .body_server_selection_with_estimate(&[], 1)
        .await;
    assert!(blocked.eligible.is_empty());
    let rejection = blocked
        .quota_blocked
        .expect("both normal fills must be quota-blocked");
    assert!(rejection.retry_at.is_none());
    let selected_stable_id = rejection.stable_server_id;
    let selected_local_revision = rejection.capacity_revision;
    let (selected_reservation, other_reservation, other_server) =
        if selected_stable_id == first_stable_id {
            (
                first_reservation,
                second_reservation,
                weaver_nntp::ServerId(1),
            )
        } else {
            (
                second_reservation,
                first_reservation,
                weaver_nntp::ServerId(0),
            )
        };
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        pipeline.retry_rx.try_recv().is_err(),
        "manual quota has no timer and must remain parked before capacity changes"
    );
    assert!(pipeline.server_quota_parked.contains(&segment_id));

    drop(other_reservation);

    let retry = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("a non-selected fill refund must wake parked work")
        .expect("retry channel must stay open");
    assert_eq!(retry.work.segment_id, segment_id);
    assert!(retry.work.exclude_servers.is_empty());
    let selected = if selected_stable_id == first_stable_id {
        &first
    } else {
        &second
    };
    assert_eq!(
        selected.quota_rejection_for(1).unwrap().capacity_revision,
        selected_local_revision,
        "the selected rejection remains locally current; only the other fill changed"
    );
    let resumed = pipeline
        .nntp
        .body_server_selection_with_estimate(&retry.work.exclude_servers, 1)
        .await;
    assert_eq!(resumed.eligible, vec![other_server]);
    assert!(
        !resumed.eligible.contains(&weaver_nntp::ServerId(2)),
        "quota capacity must not unlock the backfill tier"
    );

    pipeline.receive_retry_work(retry);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    drop(selected_reservation);
}

#[tokio::test]
async fn server_quota_lane_park_does_not_increment_error_counter() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let before = pipeline
        .metrics
        .download_lane_parks_error_total
        .load(Ordering::Relaxed);

    pipeline.note_download_lane_started(DownloadLaneMode::Sequential);
    pipeline.note_download_lane_released(DownloadLaneMode::Sequential, LaneParkReason::ServerQuota);

    assert_eq!(
        pipeline
            .metrics
            .download_lane_parks_error_total
            .load(Ordering::Relaxed),
        before
    );
}

#[tokio::test]
async fn retention_excludes_derive_from_job_age_and_server_windows() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40150);
    let spec = segmented_job_spec("Retention Derivation", "old.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());

    // Unknown post date: never retention-skipped.
    assert!(pipeline.job_retention_excludes(job_id).is_empty());

    // Older than server 0's five-day window: server 0 is excluded, and the
    // effective set merges with per-article failure excludes without dupes.
    age_job_days(&mut pipeline, job_id, 10);
    pipeline.clear_job_retention_excludes(job_id);
    assert_eq!(pipeline.job_retention_excludes(job_id).as_slice(), &[0]);
    assert_eq!(pipeline.effective_exclude_servers(job_id, &[1]), vec![1, 0]);
    assert_eq!(pipeline.effective_exclude_servers(job_id, &[0]), vec![0]);

    // Younger than every window: nothing excluded.
    age_job_days(&mut pipeline, job_id, 1);
    pipeline.clear_job_retention_excludes(job_id);
    assert!(pipeline.job_retention_excludes(job_id).is_empty());
}

#[tokio::test]
async fn article_not_found_exhaustion_counts_retention_excluded_servers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40151);
    let spec = segmented_job_spec("Retention Exhaustion", "aged.bin", &[128, 128, 128, 128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
    age_job_days(&mut pipeline, job_id, 10);

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    // A miss on the deep server (index 1) plus the retention-excluded
    // short server (index 0) exhausts both servers: the article books
    // failed bytes instead of retrying forever.
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
            source_server_idx: Some(1),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    // Exhaustion (not a retry) is the proof: the non-exhausted path
    // schedules a retry and never bumps articles_not_found. With no PAR2 in
    // the spec the booked failure also fails the job outright.
    assert_eq!(
        pipeline
            .metrics
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "retention-excluded server must count toward exhaustion"
    );
    assert_eq!(pipeline.pending_retries_by_job.get(&job_id).copied(), None);
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_none_or(|state| state.failed_bytes == 128),
        "failed bytes must be booked (job may already be archived by health)"
    );
}

#[tokio::test]
async fn fully_retention_excluded_job_books_missing_instead_of_requeueing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40152);
    let spec = segmented_job_spec("Beyond All Retention", "ancient.bin", &[128, 128, 128, 128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[5, 5]));
    age_job_days(&mut pipeline, job_id, 10);

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    // Every server is retention-excluded, so the lane path never fetches —
    // the failure surfaces as LaneUnavailable. It must be booked as a
    // missing article, not requeued without budget forever.
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
                DownloadFailureKind::LaneUnavailable,
                "failed to acquire BODY lane",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline
            .metrics
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "an unserveable article must be booked missing"
    );
    assert_eq!(pipeline.pending_retries_by_job.get(&job_id).copied(), None);
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_none_or(|state| state.failed_bytes == 128),
        "failed bytes must be booked (job may already be archived by health)"
    );
}

#[tokio::test]
async fn traced_article_not_found_retries_other_servers_without_retry_budget() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20013);
    let spec = segmented_job_spec("Source Not Found", "retry.bin", &[128]);
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
                "article not found on source server",
            )),
            attempts: vec![weaver_nntp::client::FetchAttemptTrace {
                server_idx: 0,
                remote_ip: None,
                elapsed: Duration::from_millis(5),
                outcome: weaver_nntp::client::FetchAttemptOutcome::NotFound,
                error: Some("article not found".to_string()),
            }],
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
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
    assert!(pipeline.pending_completion_checks.is_empty());

    let work = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("source miss should requeue against another server")
        .expect("retry channel must stay open")
        .work;
    assert_eq!(work.exclude_servers, vec![0]);
    assert_eq!(work.retry_count, 0);
}

#[tokio::test]
async fn recovery_article_not_found_does_not_mark_health_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20012);
    let spec = standalone_with_par2_job_spec("Recovery Miss", 128, 64);
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
                    file_index: 2,
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
            origin: DownloadResultOrigin::Recovery,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn exhausted_incomplete_download_fails_instead_of_hanging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20009);
    let filename = "stalled.bin";
    let first_segment_size = 6400u32;
    let second_segment_size = 128u32;
    let total_size = (first_segment_size + second_segment_size) as u64;
    let spec = segmented_job_spec(
        "Incomplete Exhausted",
        filename,
        &[first_segment_size, second_segment_size],
    );
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
            data: Ok(DownloadPayload::Raw(encode_article_part(
                filename,
                &vec![1u8; first_segment_size as usize],
                1,
                2,
                1,
                total_size,
            ))),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;
    drain_decode_results(&mut pipeline, 1).await;

    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .complete_data_file_count(),
        0
    );

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
                segment_number: 1,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::ContentOrProtocol,
                "malformed BODY response",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );

    while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(queued_job).await;
    }

    let status = job_status_for_assert(&pipeline, job_id).expect("job should be archived");
    assert!(
        matches!(status, JobStatus::Failed { .. }),
        "status: {status:?}"
    );
}

#[tokio::test]
async fn download_pass_finishes_when_only_optional_recovery_queue_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20010);
    let spec = segmented_job_spec("Optional Recovery Only", "payload.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Downloading;
        state.download_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("recovery-0@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 1000,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
    }

    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 0);

    assert!(!pipeline.pending_completion_checks.contains(&job_id));
    assert!(!pipeline.job_has_pending_download_pipeline_work(job_id));

    pipeline.maybe_finish_download_pass(job_id);

    assert!(!pipeline.active_download_passes.contains(&job_id));
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().recovery_queue.len(),
        1,
        "optional recovery files should stay parked until promoted"
    );
}
