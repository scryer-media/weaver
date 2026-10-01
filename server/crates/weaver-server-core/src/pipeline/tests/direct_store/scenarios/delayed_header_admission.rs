use super::*;

const JOB: JobId = JobId(48002);
const MEMBER: &str = "Silver.Horizon.Diagnostic.mkv";

#[tokio::test]
async fn delayed_header_probe_must_scan_past_blocked_critical_head() {
    check_blocked_header_probe(false).await;
}

#[tokio::test]
async fn delayed_header_probe_preserves_priority_over_an_earlier_confirmed_volume_retry() {
    check_blocked_header_probe(true).await;
}

async fn check_blocked_header_probe(earlier_confirmed_retry: bool) {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
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
    while let Some(lease) = pipeline.lease_for_server_for_test(0) {
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
    pipeline.rate_limit_reservations.remove(&header.segment_id);
    pipeline.note_retry_scheduled(header.segment_id);
    while let Some(lease) = pipeline.lease_for_server_for_test(0) {
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
    pipeline.note_retry_requeued(header.segment_id);
    let queue = &mut pipeline.jobs.get_mut(&JOB).unwrap().download_queue;
    let mut later = queue
        .pop_first_matching(|work| work.segment_id.file_id.file_index > 65)
        .unwrap();
    // Later identity/header discovery is completion-critical and can lead
    // a returned ordinary header retry, as in an obfuscated volume probe wave.
    later.completion_critical = true;
    queue.push(later);
    queue.push(header);
    assert!(
        queue
            .peek_next_matching(|work| work.segment_id.file_id.file_index > 65)
            .is_some()
    );
    assert!(
        queue
            .peek_first_matching(|work| work.segment_id == segment(65, 0))
            .is_some()
    );
    if earlier_confirmed_retry {
        // Header discovery retains priority over a retry in a volume whose
        // header is already known. An ordinal-only minimum would hide the
        // missing header behind this parked retry instead.
        pipeline.note_retry_scheduled(segment(64, 1));
    }
    let probe = pipeline.lease_for_server_for_test(0);
    assert!(
        probe.is_some(),
        "queued delayed header must remain admissible behind a blocked critical head"
    );
    assert_eq!(probe.unwrap().works[0].segment_id, segment(65, 0));
}

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
    let mut pipeline = scaled_pipeline(temp).await;
    admit_delayed_header_job(&mut pipeline, JOB, "Silver Horizon", volumes).await;
    pipeline
}

async fn scaled_pipeline(temp: &TempDir) -> Pipeline {
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
    pipeline
}

fn job_segment(job_id: JobId, file: u32, article: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: file,
        },
        segment_number: article,
    }
}

async fn admit_delayed_header_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    name: &str,
    volumes: &[(String, Vec<u8>)],
) {
    insert_active_job(pipeline, job_id, direct_store_job_spec(name, volumes)).await;
    // A late split-member header, after 65 normally completed volumes.
    for (file, article) in in_order_arrivals(65).into_iter().chain([(65, 1)]) {
        take_queued_segment(pipeline, job_id, job_segment(job_id, file, article));
        submit_volume_article(pipeline, job_id, volumes, file, article).await;
    }
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
        assert_eq!(
            pipeline.refresh_download_pressure().state,
            DownloadPressureState::Clear
        );
        let Some(lease) = pipeline.lease_for_server_for_test(0) else {
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

    // The cap holds against the queue itself, not against one caller: the
    // scheduler has nothing left for this server while the set is full.
    assert!(pipeline.lease_for_server_for_test(0).is_none());

    // A blocked set must not idle the link. The set's job is still first in
    // dispatch order, so the only way the next job can be served is the spill
    // walk stepping past it.
    let other = JobId(48003);
    insert_active_job(
        &mut pipeline,
        other,
        segmented_job_spec("Other", "other.bin", &[100; 64]),
    )
    .await;
    assert!(pipeline.job_has_dispatchable_work_for_test(other));
    assert_eq!(
        pipeline.current_hot_job(),
        Some(JOB),
        "the blocked set's job is still the hot one"
    );
    let spill = pipeline
        .lease_for_server_for_test(0)
        .expect("a blocked set must leave unrelated jobs leasable");
    assert_eq!(spill.job_id, other);
    // The peer job has served its purpose; the rest of this test is about the
    // set's own queue, which the hot-job rule would otherwise let it share.
    pipeline.jobs.remove(&other);
    pipeline.job_order.retain(|job_id| *job_id != other);

    // Model a retry returning the delayed header to the queue. With all prior
    // arrivals drained, the probe must get through as a single article.
    pipeline.rate_limit_reservations.remove(&header.segment_id);
    pipeline
        .jobs
        .get_mut(&JOB)
        .unwrap()
        .download_queue
        .push(header);
    let probe = pipeline.lease_for_server_for_test(0).unwrap();
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
        let lease = pipeline
            .lease_for_server_for_test(0)
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
        assert!(pipeline.lease_for_server_for_test(0).is_none());
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
async fn delayed_header_admission_spills_to_a_peer_job_but_obeys_global_hard_pressure() {
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
        "the dispatcher must walk past a capped hot queue rather than idle the link"
    );
}

/// The articles behind the delayed header, in arrival order. Every one of them
/// is staged in the set's holds until the header arrives.
fn held_arrivals() -> impl Iterator<Item = (u32, u32)> {
    (66..84).flat_map(|file| [(file, 0), (file, 1)])
}

fn article_len(volumes: &[(String, Vec<u8>)], file: u32, article: u32) -> u64 {
    let (start, end) = article_extent(volumes[file as usize].1.len(), article, 2);
    (end - start) as u64
}

#[tokio::test]
async fn holds_admission_is_capped_by_the_shared_scratch_total_across_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = scaled_pipeline(&temp).await;
    // Two sets' own admission limits (three quarters of 4 KiB + 64 KiB each)
    // sum well past a shared scratch total of one set's ceiling.
    let shared_scratch = 65536;
    pipeline.direct_store.set_holds_limits(
        crate::pipeline::direct_store::accountant::HoldsLimits {
            resident_bytes: 8192,
            scratch_bytes: shared_scratch,
            disk_reserve_bytes: 0,
        },
    );
    let peer = JobId(48005);
    admit_delayed_header_job(&mut pipeline, JOB, "Silver Horizon", &volumes).await;
    admit_delayed_header_job(&mut pipeline, peer, "Amber Coast", &volumes).await;

    // The first set takes held arrivals for as long as its admission allows.
    let mut first_arrivals = held_arrivals();
    for (file, article) in first_arrivals.by_ref() {
        let room = pipeline
            .direct_store
            .set(JOB, 0)
            .unwrap()
            .router
            .holds_admission_room(0);
        if article_len(&volumes, file, article) > room {
            break;
        }
        take_queued_segment(&mut pipeline, JOB, job_segment(JOB, file, article));
        submit_volume_article(&mut pipeline, JOB, &volumes, file, article).await;
        assert!(!pipeline.direct_store.set(JOB, 0).unwrap().is_demoted());
    }
    assert!(
        pipeline.direct_store.holds_accountant().scratch_bytes() > shared_scratch / 2,
        "the first set must have paged most of the shared scratch"
    );

    // The second set's own limit alone would still admit far more than the
    // shared total has left.
    let router = &pipeline.direct_store.set(peer, 0).unwrap().router;
    let own_room = router
        .holds_admission_limit()
        .saturating_sub(router.staged_bytes());
    let room = router.holds_admission_room(0);
    assert!(
        room < own_room / 2,
        "the shared remainder must cap the second set: {room} vs its own {own_room}"
    );

    // Admitted as dispatch admits it, the second set routes without either
    // set demoting, and the shared scratch is never overrun.
    let mut peer_arrivals = held_arrivals().peekable();
    let mut admitted = 0;
    while let Some(&(file, article)) = peer_arrivals.peek() {
        let room = pipeline
            .direct_store
            .set(peer, 0)
            .unwrap()
            .router
            .holds_admission_room(0);
        if article_len(&volumes, file, article) > room {
            break;
        }
        peer_arrivals.next();
        take_queued_segment(&mut pipeline, peer, job_segment(peer, file, article));
        submit_volume_article(&mut pipeline, peer, &volumes, file, article).await;
        admitted += 1;
    }
    assert!(admitted > 0, "the shared remainder still admits some work");
    assert!(!pipeline.direct_store.set(JOB, 0).unwrap().is_demoted());
    assert!(!pipeline.direct_store.set(peer, 0).unwrap().is_demoted());
    assert!(pipeline.direct_store.holds_accountant().scratch_bytes() <= shared_scratch);

    // What the per-set figure alone admitted: arrivals inside the second
    // set's own limit page past the shared total and demote it.
    for (file, article) in peer_arrivals {
        let router = &pipeline.direct_store.set(peer, 0).unwrap().router;
        if router.staged_bytes() + article_len(&volumes, file, article)
            > router.holds_admission_limit()
        {
            break;
        }
        take_queued_segment(&mut pipeline, peer, job_segment(peer, file, article));
        submit_volume_article(&mut pipeline, peer, &volumes, file, article).await;
        if pipeline.direct_store.set(peer, 0).unwrap().is_demoted() {
            break;
        }
    }
    assert!(
        format!("{:?}", pipeline.direct_store.sets_for(peer)).contains("HoldsScratchCeiling"),
        "the per-set figure alone admits arrivals the shared scratch cannot page"
    );
}

#[tokio::test]
async fn holds_scratch_ceiling_demotion_reconstructs_held_volumes_without_refetching_them() {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
    // Four whole volumes held behind the delayed header, most of them paged.
    let held: Vec<(u32, u32)> = held_arrivals().take(8).collect();
    for &(file, article) in &held {
        take_queued_segment(&mut pipeline, JOB, segment(file, article));
        submit_volume_article(&mut pipeline, JOB, &volumes, file, article).await;
    }
    let set = pipeline.direct_store.set(JOB, 0).unwrap();
    assert!(!set.is_demoted());
    assert!(
        set.router.scratch_bytes() > 0,
        "the held volumes must sit partly on scratch"
    );

    pipeline
        .demote_direct_set(
            JOB,
            0,
            crate::pipeline::direct_store::router::DemotionReason::HoldsScratchCeiling,
        )
        .await;
    settle_direct_post_repair_work(&mut pipeline).await;

    let working = pipeline.jobs.get(&JOB).unwrap().working_dir.clone();
    for file in 66..70usize {
        assert_eq!(
            std::fs::read(working.join(&volumes[file].0)).unwrap(),
            volumes[file].1,
            "held volume {file} must be rebuilt byte-exactly from its holds"
        );
    }
    let queued = queued_segments(&mut pipeline, JOB);
    for arrival in &held {
        assert!(
            !queued.contains(arrival),
            "held article {arrival:?} must not be fetched again"
        );
    }
    assert!(
        queued.contains(&(65, 0)),
        "the header that never arrived is still owed"
    );
    assert_eq!(
        pipeline.direct_store.holds_accountant().scratch_bytes(),
        0,
        "the preserved scratch is released once the sweep has read it"
    );
}
