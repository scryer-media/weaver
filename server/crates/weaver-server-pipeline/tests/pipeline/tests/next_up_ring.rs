// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The ring of jobs next up through every queue change the API makes: moves
// up, down, to the top, to the bottom and to a position, priority edits,
// pauses, resumes and removals. After each turn the ring holds the jobs now
// next in dispatch order, and the jobs that took a slot and have not
// activated; a read that comes back for a job no longer wanted is dropped.

use weaver_model::files::FileRole;
use weaver_server_ingest::ingest::outline::read_stored_lists;
use weaver_server_pipeline::pipeline::tests::{
    activate_job,
    dispatch_order_fixture::ring as dispatch_ring,
    finish_cancel_jobs,
    next_up_fixture::*,
    nzb_zstd_for,
    queue_fixture::{apply_intents, installed_spec},
    queue_scale_fixture::{
        NEXT_UP_DEPTH, admitted_ids, held_ahead, nzb_parses, settle_next_up, stored_spec,
    },
    segmented_job_spec,
    window_fixture::{QUEUE_WINDOW, admitted, articles_active, articles_queued, has_installed},
};
use weaver_server_pipeline::{
    FileSpec, JobId, JobSpec, JobUpdate, MessageIds, Pipeline, QueueMoveTarget, SegmentSpec,
};

fn ids(ids: impl IntoIterator<Item = u64>) -> Vec<JobId> {
    let mut ids: Vec<JobId> = ids.into_iter().map(JobId).collect();
    ids.sort_unstable();
    ids
}

// The ring holds `next`, read, beside the jobs that took a slot and have not
// activated, and nothing else; never more than the depth past those.
fn assert_ring(pipeline: &Pipeline, next: &[u64]) {
    assert_eq!(next.len(), NEXT_UP_DEPTH);
    let waiting_admitted: Vec<u64> = admitted_ids(pipeline)
        .into_iter()
        .filter(|id| has_installed(pipeline, *id) && articles_queued(pipeline, *id))
        .map(|id| id.0)
        .collect();
    let mut held_expected: Vec<u64> = next.to_vec();
    held_expected.extend(
        waiting_admitted
            .iter()
            .copied()
            .filter(|id| next_up_ids(pipeline).contains(&JobId(*id))),
    );
    assert_eq!(held_ahead(pipeline), ids(held_expected));
    assert_eq!(next_up_ids(pipeline), held_ahead(pipeline));
    assert!(next_up_ids(pipeline).len() <= NEXT_UP_DEPTH + waiting_admitted.len());
    assert_eq!(
        dispatch_ring(pipeline)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        next.iter().copied().map(JobId).collect(),
    );
}

#[tokio::test]
async fn the_ring_follows_every_queue_move() {
    let (_temp, mut pipeline, handle) = ring(2).await;
    // Jobs 1 and 2 hold the slots; 3, 4 and 5 are next.
    assert_ring(&pipeline, &[3, 4, 5]);

    // Up: job 10 to the third row.
    handle
        .reorder_job(JobId(10), QueueMoveTarget::Offset(-7))
        .await
        .unwrap();
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[3, 10, 4]);

    // Down: job 3 five rows.
    handle
        .reorder_job(JobId(3), QueueMoveTarget::Offset(5))
        .await
        .unwrap();
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[10, 4, 5]);

    // To the top, ahead of the slots.
    handle
        .reorder_job(JobId(50), QueueMoveTarget::Top)
        .await
        .unwrap();
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[50, 10, 4]);

    // To the bottom.
    handle
        .reorder_job(JobId(50), QueueMoveTarget::Bottom)
        .await
        .unwrap();
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[10, 4, 5]);

    // To a position, as the facades move one: the offset from where it stands.
    let offset = move_offset(&pipeline, JobId(70), JobId(4));
    handle
        .reorder_job(JobId(70), QueueMoveTarget::Offset(offset))
        .await
        .unwrap();
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[10, 70, 4]);

    // A priority raise from far down the queue.
    handle
        .update_job(
            JobId(90),
            JobUpdate {
                metadata_edits: vec![("priority".into(), Some("high".into()))],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[90, 10, 70]);

    // A ring job paused leaves; resumed, it returns.
    handle.pause_job(JobId(10)).await.unwrap();
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[90, 70, 4]);
    handle.resume_job(JobId(10)).await.unwrap();
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[90, 10, 70]);

    // A ring job removed leaves for good.
    handle.cancel_job(JobId(70)).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[90, 10, 4]);
}

// A read still out for a job that leaves the ring is dropped when it comes
// back. A job that leaves and returns before its first read is heard holds
// only the read of its return, once.
#[tokio::test]
async fn a_read_for_a_job_no_longer_next_is_dropped() {
    let (_temp, mut pipeline, handle) = ring(2).await;
    assert_ring(&pipeline, &[3, 4, 5]);

    // In, then out before its read is heard.
    let job = JobId(20);
    handle
        .reorder_job(job, QueueMoveTarget::Offset(-17))
        .await
        .unwrap();
    let mut held = settle_window_holding_reads(&mut pipeline).await;
    let read = last_read(&pipeline);
    assert!(next_up_ids(&pipeline).contains(&job));
    assert!(!held_ahead(&pipeline).contains(&job));
    handle
        .reorder_job(job, QueueMoveTarget::Bottom)
        .await
        .unwrap();
    held.extend(settle_window_holding_reads(&mut pipeline).await);
    assert!(!next_up_ids(&pipeline).contains(&job));
    hold_reads_through(&mut pipeline, &mut held, read).await;
    hear(&mut pipeline, &mut held, &[read]).await;
    assert!(!next_up_ids(&pipeline).contains(&job));
    held.deliver_all(&mut pipeline).await;
    settle_next_up(&mut pipeline).await;
    assert_ring(&pipeline, &[3, 4, 5]);

    // In, out and in again with neither read heard: the first is dropped,
    // the second is held.
    let job = JobId(30);
    handle.reorder_job(job, QueueMoveTarget::Top).await.unwrap();
    let mut held = settle_window_holding_reads(&mut pipeline).await;
    let out = last_read(&pipeline);
    handle
        .reorder_job(job, QueueMoveTarget::Bottom)
        .await
        .unwrap();
    held.extend(settle_window_holding_reads(&mut pipeline).await);
    handle.reorder_job(job, QueueMoveTarget::Top).await.unwrap();
    held.extend(settle_window_holding_reads(&mut pipeline).await);
    let back = last_read(&pipeline);
    assert!(back > out);
    hold_reads_through(&mut pipeline, &mut held, out).await;
    hear(&mut pipeline, &mut held, &[out]).await;
    assert!(next_up_ids(&pipeline).contains(&job));
    assert!(!held_ahead(&pipeline).contains(&job));
    hold_reads_through(&mut pipeline, &mut held, back).await;
    hear(&mut pipeline, &mut held, &[back]).await;
    assert!(held_ahead(&pipeline).contains(&job));
    held.deliver_all(&mut pipeline).await;
    settle_next_up(&mut pipeline).await;
    assert_ring(&pipeline, &[30, 3, 4]);
}

type Parts = Vec<Vec<(u32, u32, u32, MessageIds)>>;

fn lists(spec: &JobSpec) -> Parts {
    parts(spec.files.iter().map(|file| file.segments.as_slice()))
}

fn parts<'a>(lists: impl IntoIterator<Item = &'a [SegmentSpec]>) -> Parts {
    lists
        .into_iter()
        .map(|list| {
            list.iter()
                .map(|segment| {
                    (
                        segment.ordinal,
                        segment.article_number,
                        segment.bytes,
                        segment.message_ids.clone(),
                    )
                })
                .collect()
        })
        .collect()
}

// A job moved to the top from outside the queue window as a slot comes free
// is installed by the window first, so it enters the ring, and then takes
// the slot. Here it takes the slot before its read ahead is heard: it
// activates on the read every activation makes, and the read ahead, heard
// later, is dropped.
#[tokio::test]
async fn a_job_moved_to_the_top_from_outside_the_window_activates_on_its_own_read() {
    let (_temp, mut pipeline, handle) = ring(2).await;
    let far = JobId(QUEUE_WINDOW as u64 + 50);
    assert!(!has_installed(&pipeline, far));
    handle.reorder_job(far, QueueMoveTarget::Top).await.unwrap();
    handle.cancel_job(JobId(1)).await.unwrap();
    let mut held = settle_window_holding_reads(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    held.extend(settle_window_holding_reads(&mut pipeline).await);
    let read = last_read(&pipeline);
    assert!(has_installed(&pipeline, far));
    assert!(admitted(&pipeline, far));
    assert!(next_up_ids(&pipeline).contains(&far));
    assert!(!held_ahead(&pipeline).contains(&far));

    let expected = stored_spec(&pipeline, far);
    activate_job(&mut pipeline, far).await;
    assert!(!next_up_ids(&pipeline).contains(&far));
    hold_reads_through(&mut pipeline, &mut held, read).await;
    held.deliver_all(&mut pipeline).await;
    settle_next_up(&mut pipeline).await;
    assert!(!next_up_ids(&pipeline).contains(&far));
    assert!(articles_active(&pipeline, far));
    assert_eq!(
        lists(installed_spec(&pipeline, far).unwrap()),
        lists(&expected)
    );
    assert_ring(&pipeline, &[3, 4, 5]);
}

// A job moved to the top from outside the window with every slot taken is
// installed and enters the ring as any job next up does.
#[tokio::test]
async fn a_job_moved_to_the_top_from_outside_the_window_is_read_ahead() {
    let (_temp, mut pipeline, handle) = ring(2).await;
    let far = JobId(QUEUE_WINDOW as u64 + 50);
    handle.reorder_job(far, QueueMoveTarget::Top).await.unwrap();
    turn(&mut pipeline).await;
    assert!(has_installed(&pipeline, far));
    assert_ring(&pipeline, &[far.0, 3, 4]);
}

// A job that takes a slot once its lists are read ahead activates on them,
// with no read of its own, and they are the lists a read gives.
#[tokio::test]
async fn a_job_read_ahead_activates_on_the_lists_held() {
    let (_temp, mut pipeline, handle) = ring(2).await;
    let job_id = JobId(3);
    assert!(held_ahead(&pipeline).contains(&job_id));
    handle.cancel_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    turn(&mut pipeline).await;
    assert!(admitted(&pipeline, job_id));
    assert!(held_ahead(&pipeline).contains(&job_id));
    assert_ring(&pipeline, &[4, 5, 6]);

    let expected = stored_spec(&pipeline, job_id);
    let parses = nzb_parses();
    activate_job(&mut pipeline, job_id).await;
    assert_eq!(nzb_parses(), parses);
    assert!(!next_up_ids(&pipeline).contains(&job_id));
    assert!(articles_active(&pipeline, job_id));
    assert_eq!(
        lists(installed_spec(&pipeline, job_id).unwrap()),
        lists(&expected)
    );
}

// Seeded jobs of several files, parts posted under one id or several, stored
// as NZBs and read back as the ring reads them: the lists the ring packs
// unpack to the lists read, in order and whole.
#[test]
fn held_lists_unpack_to_the_lists_read() {
    let mut seed: u64 = 0x5eed_1d1e;
    let mut next = |below: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % below
    };
    let mut reposted = 0;
    for job in 0..40u32 {
        let files = 1 + next(6) as usize;
        let mut spec = segmented_job_spec(&format!("Held {job}"), "unused.bin", &[]);
        spec.files = (0..files)
            .map(|file| {
                let parts = 1 + next(60) as u32;
                FileSpec {
                    filename: format!("held-{job}-{file}.bin"),
                    role: FileRole::Standalone,
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: (0..parts)
                        .map(|ordinal| {
                            let ids = 1 + next(3) as usize;
                            SegmentSpec {
                                ordinal,
                                article_number: ordinal + 1,
                                bytes: 1 + next(800_000) as u32,
                                message_ids: MessageIds::new(
                                    (0..ids)
                                        .map(|id| {
                                            format!(
                                                "part{ordinal}.{id}.{}@held-{job}-{file}.example",
                                                "x".repeat(next(40) as usize)
                                            )
                                        })
                                        .collect(),
                                ),
                            }
                        })
                        .collect(),
                }
            })
            .collect();
        let nzb_zstd = nzb_zstd_for(&spec);
        let read = read_stored_lists(&nzb_zstd, std::path::Path::new("held.nzb")).unwrap();
        reposted += read
            .lists
            .iter()
            .flatten()
            .filter(|part| part.message_ids.all().len() > 1)
            .count();
        let held = PackedLists::pack(&read.lists).expect("fresh lists pack");
        // One block of ids and fixed records: no string or list per part.
        let parts_held = read.lists.iter().map(Vec::len).sum::<usize>();
        let ids_held = read
            .lists
            .iter()
            .flatten()
            .map(|part| part.message_ids.all().len())
            .sum::<usize>();
        let id_bytes = read
            .lists
            .iter()
            .flatten()
            .flat_map(|part| part.message_ids.all())
            .map(String::len)
            .sum::<usize>();
        assert_eq!(
            held.heap_bytes(),
            id_bytes + 4 * ids_held + 16 * parts_held + 4 * read.lists.len()
        );
        assert_eq!(
            parts(held.unpack().iter().map(Vec::as_slice)),
            parts(read.lists.iter().map(Vec::as_slice)),
            "job {job}"
        );
    }
    assert!(reposted > 0);
}

// A part whose fetch has moved past its first id is not held packed: the job
// reads its lists as it activates.
#[test]
fn a_part_past_its_first_id_is_not_held() {
    let mut ids = MessageIds::new(vec!["a@x".into(), "b@x".into()]);
    let fresh = SegmentSpec {
        ordinal: 0,
        article_number: 1,
        bytes: 10,
        message_ids: ids.clone(),
    };
    let held = PackedLists::pack(&[Vec::new(), vec![fresh.clone()]]).unwrap();
    assert_eq!(
        parts(held.unpack().iter().map(Vec::as_slice)),
        parts([&[][..], std::slice::from_ref(&fresh)]),
    );
    assert!(ids.spend());
    let spent = SegmentSpec {
        ordinal: 0,
        article_number: 1,
        bytes: 10,
        message_ids: ids,
    };
    assert!(PackedLists::pack(&[vec![spent]]).is_none());
}
