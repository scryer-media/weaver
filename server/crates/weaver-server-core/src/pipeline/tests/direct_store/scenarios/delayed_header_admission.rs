use super::*;

const JOB: JobId = JobId(48002);
const MEMBER: &str = "Silver.Horizon.Diagnostic.mkv";

// Original valid RAR5 data; scale the production resident/scratch ratio down
// so the regression exercises the real router without gigabytes of fixtures.
fn fixture() -> (Vec<u8>, Vec<(String, Vec<u8>)>) {
    let payload: Vec<u8> = (0..84 * 6400usize).map(|i| (i % 251) as u8).collect();
    let volumes = single_member_store_set(MEMBER, &payload, 84);
    (payload, volumes)
}

fn segment(file: u32, article: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: file,
        },
        segment_number: article,
    }
}

async fn prepared(temp: &TempDir, volumes: &[(String, Vec<u8>)]) -> Pipeline {
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        temp,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(4096);
    pipeline.direct_store.set_holds_scratch_ceiling(65536);
    insert_active_job(
        &mut pipeline,
        JOB,
        direct_store_job_spec("Silver Horizon", volumes),
    )
    .await;
    // A late split-member header, after 65 normally completed volumes.
    for (file, article) in in_order_arrivals(65).into_iter().chain([(65, 1)]) {
        take_queued_segment(&mut pipeline, JOB, segment(file, article));
        submit_volume_article(&mut pipeline, JOB, volumes, file, article).await;
    }
    pipeline.hot_dispatch_job = Some(JOB);
    pipeline
}

#[tokio::test]
async fn delayed_header_admission_prevents_valid_archive_demotion_and_reopens() {
    let temp = tempfile::tempdir().unwrap();
    let (payload, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
    // Keep the header in flight while later volumes are fetched and decoded.
    let header = pipeline
        .jobs
        .get_mut(&JOB)
        .unwrap()
        .download_queue
        .pop_first_matching(|work| work.segment_id == segment(65, 0))
        .unwrap();
    pipeline
        .rate_limit_reservations
        .insert(header.segment_id, header.byte_estimate as u64);
    let mut later = 0;
    loop {
        let pressure = pipeline.refresh_download_pressure();
        assert_eq!(pressure.state, DownloadPressureState::Clear);
        let Some(lease) = pipeline.try_lease_initial_download_batch_for_test(JOB, pressure) else {
            break;
        };
        let set = pipeline.direct_store.set(JOB, 0).unwrap();
        let leased_bytes: u64 = lease
            .works
            .iter()
            .map(|work| work.byte_estimate as u64)
            .sum();
        assert!(
            set.router.staged_bytes() + header.byte_estimate as u64 + leased_bytes
                <= set.router.holds_admission_limit(),
            "the whole batch must fit, including in-flight work"
        );
        for work in lease.works {
            let id = work.segment_id;
            submit_volume_article(
                &mut pipeline,
                JOB,
                &volumes,
                id.file_id.file_index,
                id.segment_number,
            )
            .await;
            later += 1;
        }
        assert!(
            later < 36,
            "lookahead must stop before the known scratch breach"
        );
    }
    assert!(later > 2, "healthy lookahead must remain available");
    assert!(!pipeline.job_has_dispatchable_work_for_test(JOB));
    assert!(!pipeline.direct_store.set(JOB, 0).unwrap().is_demoted());

    // Existing sockets and IP replacement trials cannot bypass this cap,
    // including the refill fallback which changes compatibility class.
    let next = pipeline
        .jobs
        .get(&JOB)
        .unwrap()
        .download_queue
        .peek_next_matching(|_| true)
        .unwrap();
    let mut compatibility = DownloadBatchCompatibility::from_work(next);
    compatibility.completion_critical = !compatibility.completion_critical;
    let pressure = pipeline.refresh_download_pressure();
    assert!(
        pipeline
            .try_lease_refill_download_batch_for_test(JOB, compatibility, pressure)
            .is_none()
    );
    assert!(
        pipeline
            .try_lease_ip_replacement_trial_batch_for_test(JOB, 0)
            .is_none()
    );

    let other = JobId(48003);
    insert_active_job(
        &mut pipeline,
        other,
        segmented_job_spec("Other", "other.bin", &[100; 64]),
    )
    .await;
    assert!(pipeline.job_has_dispatchable_work_for_test(other));
    assert!(
        pipeline
            .try_lease_initial_download_batch_for_test(other, pressure)
            .is_some(),
        "a blocked set must leave unrelated jobs leasable"
    );

    // Model a retry returning the delayed header to the queue. With all prior
    // arrivals drained, the probe must get through as a single article.
    pipeline.rate_limit_reservations.remove(&header.segment_id);
    pipeline
        .jobs
        .get_mut(&JOB)
        .unwrap()
        .download_queue
        .push(header);
    let probe = pipeline
        .try_lease_initial_download_batch_for_test(JOB, pressure)
        .unwrap();
    assert_eq!(probe.works.len(), 1);
    assert_eq!(probe.works[0].segment_id, segment(65, 0));
    submit_volume_article(&mut pipeline, JOB, &volumes, 65, 0).await;
    assert!(
        pipeline
            .direct_store
            .set(JOB, 0)
            .unwrap()
            .router
            .staged_bytes()
            < 4096
    );

    while !pipeline.jobs.get(&JOB).unwrap().download_queue.is_empty() {
        let pressure = pipeline.refresh_download_pressure();
        let lease = pipeline
            .try_lease_initial_download_batch_for_test(JOB, pressure)
            .expect("routing the header must reopen ordinary work");
        for work in lease.works {
            let id = work.segment_id;
            submit_volume_article(
                &mut pipeline,
                JOB,
                &volumes,
                id.file_id.file_index,
                id.segment_number,
            )
            .await;
        }
    }
    let set = pipeline.direct_store.set(JOB, 0).unwrap();
    assert!(!set.is_demoted());
    assert!(set.is_finalized());
    assert_eq!(
        std::fs::read(Pipeline::member_output_paths(&payload_root(&temp, JOB), MEMBER).0).unwrap(),
        payload
    );
}

#[tokio::test]
async fn delayed_header_admission_counts_each_arrival_stage() {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
    let id = segment(65, 0);
    take_queued_segment(&mut pipeline, JOB, id);
    let ceiling = pipeline
        .direct_store
        .set(JOB, 0)
        .unwrap()
        .router
        .holds_admission_limit();
    for stage in 0..4 {
        match stage {
            0 => {
                pipeline.rate_limit_reservations.insert(id, ceiling);
            }
            1 => {
                pipeline.pending_decode.push_back(PendingDecodeWork {
                    segment_id: id,
                    raw: Bytes::from(vec![0; ceiling as usize]),
                    source_server_idx: None,
                    exclude_servers: Vec::new(),
                });
            }
            2 => {
                pipeline.note_decode_started(id, ceiling);
            }
            3 => {
                pipeline
                    .pending_released_download_result_bytes_by_job
                    .insert(JOB, ceiling);
            }
            _ => unreachable!(),
        }
        assert!(
            !pipeline.job_has_dispatchable_work_for_test(JOB),
            "stage {stage} must retain its reservation"
        );
        let pressure = pipeline.refresh_download_pressure();
        assert!(
            pipeline
                .try_lease_initial_download_batch_for_test(JOB, pressure)
                .is_none()
        );
        pipeline.rate_limit_reservations.clear();
        pipeline.pending_decode.clear();
        pipeline.active_decode_bytes.clear();
        pipeline.active_decodes_by_job.clear();
        pipeline.active_decodes_by_file.clear();
        pipeline
            .pending_released_download_result_bytes_by_job
            .clear();
        assert!(
            pipeline.job_has_dispatchable_work_for_test(JOB),
            "stage {stage} must release admission when drained"
        );
    }
}

#[tokio::test]
async fn delayed_header_uncontrolled_arrivals_still_reproduce_scratch_demotion() {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
    let mut demoted = false;
    // Bypass admission deliberately: this is the pre-fix arrival sequence,
    // and also proves the router's hard safety fallback remains in force.
    for file in 66..84 {
        for article in 0..2 {
            take_queued_segment(&mut pipeline, JOB, segment(file, article));
            submit_volume_article(&mut pipeline, JOB, &volumes, file, article).await;
            if pipeline.direct_store.set(JOB, 0).unwrap().is_demoted() {
                demoted = true;
                break;
            }
        }
        if demoted {
            break;
        }
    }
    assert!(demoted);
    assert!(format!("{:?}", pipeline.direct_store.sets_for(JOB)).contains("HoldsScratchCeiling"));
}

#[tokio::test]
async fn delayed_header_admission_lends_connections_but_obeys_global_hard_pressure() {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
    let limit = pipeline
        .direct_store
        .set(JOB, 0)
        .unwrap()
        .router
        .holds_admission_limit();
    take_queued_segment(&mut pipeline, JOB, segment(65, 0));
    pipeline
        .rate_limit_reservations
        .insert(segment(65, 0), limit);
    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(JOB, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment(65, 0).file_id, 1);
    pipeline.active_download_connections = 1;
    pipeline.active_download_connections_by_job.insert(JOB, 1);
    let peer = JobId(48004);
    insert_active_job(
        &mut pipeline,
        peer,
        segmented_job_spec("Peer", "peer.bin", &[100; 64]),
    )
    .await;
    let now = Instant::now();
    pipeline.hot_dispatch_started_at = Some(now - Duration::from_secs(5));
    pipeline.hot_dispatch_underfill_since = Some(now - Duration::from_secs(2));

    pipeline
        .metrics
        .write_buffered_bytes
        .store(u64::MAX, Ordering::Relaxed);
    pipeline.dispatch_downloads();
    assert_eq!(
        pipeline.refresh_download_pressure().state,
        DownloadPressureState::Hard
    );
    assert!(
        !pipeline
            .active_download_connections_by_job
            .contains_key(&peer)
    );

    pipeline
        .metrics
        .write_buffered_bytes
        .store(0, Ordering::Relaxed);
    pipeline.hot_dispatch_underfill_since = Some(now - Duration::from_secs(2));
    pipeline.dispatch_downloads();
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&JOB),
        Some(&1)
    );
    assert!(
        pipeline
            .active_download_connections_by_job
            .get(&peer)
            .copied()
            .unwrap_or(0)
            > 0,
        "the actual dispatcher must lend free connections past a capped hot queue"
    );
}
