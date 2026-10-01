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

/// Every article of the job's queue, in the order a handout scans them, put
/// back exactly as it was. `edit` rewrites the list before it is requeued.
fn requeue_in_scan_order(
    pipeline: &mut Pipeline,
    edit: impl FnOnce(&mut Vec<DownloadWork>),
) -> Vec<DownloadWork> {
    // A pending unlock re-rank would reorder the queue under the handout.
    pipeline.apply_rar_unlock_priorities_if_dirty(JOB);
    let queue = &mut pipeline.jobs.get_mut(&JOB).unwrap().download_queue;
    let mut order = Vec::new();
    while let Some(work) = queue.pop() {
        order.push(work);
    }
    edit(&mut order);
    for work in &order {
        queue.push(work.clone());
    }
    order
}

fn handed_out(pipeline: &mut Pipeline, want: usize) -> Vec<SegmentId> {
    let pressure = pipeline.refresh_download_pressure();
    assert_eq!(pressure.state, DownloadPressureState::Clear);
    match pipeline.next_works(0, want, None, pressure) {
        crate::pipeline::download::scheduler::Handout::Works(works) => {
            works.iter().map(|work| work.segment_id).collect()
        }
        crate::pipeline::download::scheduler::Handout::Idle => Vec::new(),
        _ => panic!("no whole-link gate or lane share applies here"),
    }
}

#[tokio::test]
async fn a_handout_charges_each_article_it_takes_against_the_set_budget() {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
    // The header is in flight, so the set is busy and probes nothing.
    take_queued_segment(&mut pipeline, JOB, segment(65, 0));
    let queued = requeue_in_scan_order(&mut pipeline, |order| {
        order.truncate(3);
        for (work, bytes) in order.iter_mut().zip([1000, 3000, 1000]) {
            work.byte_estimate = bytes;
        }
    });
    assert_eq!(queued.len(), 3);
    assert!(pipeline.pending_decode.is_empty());
    assert!(pipeline.active_decode_bytes.is_empty());
    assert!(
        !pipeline
            .pending_released_download_result_bytes_by_job
            .contains_key(&JOB)
    );
    let set = pipeline.direct_store.set(JOB, 0).unwrap();
    let room = set
        .router
        .holds_admission_limit()
        .checked_sub(set.router.staged_bytes())
        .unwrap();
    assert!(room > 2500);
    // 2500 bytes of room are left: the first article fits, the second does
    // not fit beside it, and the third fits in what the first left over.
    pipeline
        .rate_limit_reservations
        .insert(segment(65, 0), room - 2500);

    assert_eq!(
        handed_out(&mut pipeline, 3),
        vec![queued[0].segment_id, queued[2].segment_id],
    );
    assert_eq!(pipeline.jobs[&JOB].download_queue.len(), 1);
}

#[tokio::test]
async fn a_header_probe_is_handed_out_alone_when_the_set_budget_is_spent() {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
    let router = &mut pipeline.direct_store.set_mut(JOB, 0).unwrap().router;
    router.set_holds_budget(0);
    router.set_holds_scratch_ceiling(0);
    assert_eq!(router.holds_admission_limit(), 0);
    assert!(pipeline.rate_limit_reservations.is_empty());
    assert!(
        pipeline
            .active_downloads_by_file
            .values()
            .all(|count| *count == 0)
    );
    assert!(
        pipeline
            .active_decodes_by_file
            .values()
            .all(|count| *count == 0)
    );
    requeue_in_scan_order(&mut pipeline, |_| {});

    // Nothing fits, but the earliest unread header is exempt; once this
    // handout has taken it the set is busy, and the exemption closes behind
    // it for the rest of the handout.
    assert_eq!(handed_out(&mut pipeline, 3), vec![segment(65, 0)]);
}

/// The scheduler counters the per-file early-out is judged by.
fn scan_counters(pipeline: &Pipeline) -> (u64, u64, u64) {
    use crate::operations::metrics::SchedulerBlockClause;
    let metrics = &pipeline.metrics;
    (
        metrics
            .download_scheduler_scan_items_skipped_total
            .load(Ordering::Relaxed),
        metrics
            .download_scheduler_scan_no_match_total
            .load(Ordering::Relaxed),
        metrics.download_scheduler_hot_blocked_total[SchedulerBlockClause::DirectStore.index()]
            .load(Ordering::Relaxed),
    )
}

/// Leave only the queued articles of volumes 66 to 68, plus the header
/// article of volume 65 when `with_header` is set.
fn keep_three_volumes(pipeline: &mut Pipeline, with_header: bool) -> Vec<DownloadWork> {
    requeue_in_scan_order(pipeline, |order| {
        order.retain(|work| {
            let file = work.segment_id.file_id.file_index;
            (66..=68).contains(&file) || (with_header && work.segment_id == segment(65, 0))
        });
    })
}

#[tokio::test]
async fn a_set_with_no_room_for_any_queued_files_smallest_article_is_answered_without_a_scan() {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
    // The header is in flight, so the set is busy and probes nothing.
    take_queued_segment(&mut pipeline, JOB, segment(65, 0));
    let queued = keep_three_volumes(&mut pipeline, false);
    let smallest = queued
        .iter()
        .map(|work| u64::from(work.byte_estimate))
        .min()
        .unwrap();
    assert!(
        queued
            .iter()
            .map(|work| work.segment_id.file_id.file_index)
            .collect::<HashSet<_>>()
            .len()
            == 3
    );
    let set = pipeline.direct_store.set(JOB, 0).unwrap();
    let room = set
        .router
        .holds_admission_limit()
        .checked_sub(set.router.staged_bytes())
        .unwrap();
    // One byte short of the smallest article of any of the three files.
    pipeline
        .rate_limit_reservations
        .insert(segment(65, 0), room - (smallest - 1));
    let (skipped, no_match, budget_blocks) = scan_counters(&pipeline);

    assert!(handed_out(&mut pipeline, 3).is_empty());

    assert_eq!(
        scan_counters(&pipeline),
        (skipped, no_match, budget_blocks + 1),
        "no article was looked at, no scan came up empty, and the block is the set's budget"
    );
    assert_eq!(pipeline.jobs[&JOB].download_queue.len(), queued.len());
}

#[tokio::test]
async fn a_probe_exempt_header_gets_past_the_per_file_early_out() {
    let temp = tempfile::tempdir().unwrap();
    let (_, volumes) = fixture();
    let mut pipeline = prepared(&temp, &volumes).await;
    let router = &mut pipeline.direct_store.set_mut(JOB, 0).unwrap().router;
    router.set_holds_budget(0);
    router.set_holds_scratch_ceiling(0);
    assert!(pipeline.rate_limit_reservations.is_empty());
    keep_three_volumes(&mut pipeline, true);
    let (_, _, budget_blocks) = scan_counters(&pipeline);

    // No article fits, but the set is idle and its earliest unread header is
    // exempt, so its file passes the per-file answer and the scan finds it.
    assert_eq!(handed_out(&mut pipeline, 3), vec![segment(65, 0)]);
    assert_eq!(scan_counters(&pipeline).2, budget_blocks);
}
