// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The ring of jobs next up through every queue change the API makes: moves
// up, down, to the top, to the bottom and to a position, priority edits,
// pauses, resumes and removals. After each turn the ring holds the jobs now
// next in dispatch order, and the jobs that took a slot and have not
// activated; a read that comes back for a job no longer wanted is dropped.

use super::*;
use crate::jobs::handle::QueueMoveTarget;
use crate::pipeline::db_reply::DbReply;
use crate::pipeline::next_up::NEXT_UP_DEPTH;
use crate::pipeline::queue_window::QUEUE_WINDOW;

const QUEUED: u64 = 200;

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Ring NZB {id}"), "payload.bin", &[512; 4]);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("ring-{id}.nzb")),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// Turns until the window has nothing left to install.
async fn settle_window(pipeline: &mut Pipeline) {
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
}

// Hear the database's answers until no list read ahead is out.
async fn settle_next_up(pipeline: &mut Pipeline) {
    while pipeline.next_up.reading() {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        pipeline.handle_db_reply(reply).await;
    }
}

// Turns as `settle_window` does, but every read ahead that comes back is
// held back unheard and returned, so the test chooses when the ring hears
// it: the pipeline hears only the replies the test hands it.
async fn settle_window_holding_reads(pipeline: &mut Pipeline) -> Vec<DbReply> {
    let mut held = Vec::new();
    loop {
        pipeline.run_queue_intents().await;
        let mut walk_again = false;
        while pipeline.db_replies_out() {
            let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
            if matches!(reply, DbReply::NextUp { .. }) {
                held.push(reply);
                continue;
            }
            walk_again |= matches!(
                reply,
                DbReply::Slot {
                    committed: true,
                    ..
                }
            );
            let installs = matches!(reply, DbReply::Installs(_));
            pipeline.handle_db_reply(reply).await;
            walk_again |= installs && !pipeline.restores_waiting;
        }
        if !walk_again && !pipeline.restores_waiting {
            return held;
        }
    }
}

// Wait, holding them back unheard, until the ring's read numbered `read` is
// among the reads held.
async fn hold_reads_through(pipeline: &mut Pipeline, held: &mut Vec<DbReply>, read: u64) {
    let back = |held: &Vec<DbReply>| {
        held.iter()
            .any(|reply| matches!(reply, DbReply::NextUp { read: number, .. } if *number == read))
    };
    while !back(held) {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        if matches!(reply, DbReply::NextUp { .. }) {
            held.push(reply);
        } else {
            pipeline.handle_db_reply(reply).await;
        }
    }
}

// Hand the ring the reads held back numbered `reads`, in that order.
async fn hear(pipeline: &mut Pipeline, held: &mut Vec<DbReply>, reads: &[u64]) {
    for read in reads {
        let index = held
            .iter()
            .position(|reply| matches!(reply, DbReply::NextUp { read: number, .. } if number == read))
            .expect("the read is held");
        let reply = held.remove(index);
        pipeline.handle_db_reply(reply).await;
    }
}

async fn ring(slots: u32) -> (tempfile::TempDir, Pipeline, SchedulerHandle) {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(slots).unwrap())
        .await
        .unwrap();
    for id in 1..=QUEUED {
        enqueue(&mut pipeline, id).await;
    }
    settle_window(&mut pipeline).await;
    settle_next_up(&mut pipeline).await;
    (temp, pipeline, handle)
}

fn ids(ids: impl IntoIterator<Item = u64>) -> Vec<JobId> {
    let mut ids: Vec<JobId> = ids.into_iter().map(JobId).collect();
    ids.sort_unstable();
    ids
}

// The ring holds `next`, read, beside the jobs that took a slot and have not
// activated, and nothing else; never more than the depth past those.
fn assert_ring(pipeline: &Pipeline, next: &[u64]) {
    assert_eq!(next.len(), NEXT_UP_DEPTH);
    let waiting_admitted: Vec<u64> = pipeline
        .nzb_admitted
        .iter()
        .filter(|id| {
            pipeline.jobs.get(id).is_some_and(|state| {
                matches!(state.articles, crate::jobs::model::JobArticles::Queued(_))
            })
        })
        .map(|id| id.0)
        .collect();
    let mut held_expected: Vec<u64> = next.to_vec();
    held_expected.extend(
        waiting_admitted
            .iter()
            .copied()
            .filter(|id| pipeline.next_up.ids().contains(&JobId(*id))),
    );
    assert_eq!(pipeline.next_up.held(), ids(held_expected));
    assert_eq!(pipeline.next_up.ids(), pipeline.next_up.held());
    assert!(pipeline.next_up.ids().len() <= NEXT_UP_DEPTH + waiting_admitted.len());
    assert_eq!(
        pipeline.next_up_jobs().into_iter().collect::<std::collections::BTreeSet<_>>(),
        next.iter().copied().map(JobId).collect(),
    );
}

async fn turn(pipeline: &mut Pipeline) {
    settle_window(pipeline).await;
    settle_next_up(pipeline).await;
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
    handle.reorder_job(JobId(50), QueueMoveTarget::Top).await.unwrap();
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
    let (from, to) = pipeline.shared_state.queue.read(|inner| {
        (
            inner.job_position(JobId(70)).unwrap() as i64,
            inner.job_position(JobId(4)).unwrap() as i64,
        )
    });
    handle
        .reorder_job(JobId(70), QueueMoveTarget::Offset(to - from))
        .await
        .unwrap();
    turn(&mut pipeline).await;
    assert_ring(&pipeline, &[10, 70, 4]);

    // A priority raise from far down the queue.
    handle
        .update_job(
            JobId(90),
            crate::jobs::JobUpdate {
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
    pipeline.apply_queue_intents().await;
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
    let read = pipeline.next_up.last_read();
    assert!(pipeline.next_up.ids().contains(&job));
    assert!(!pipeline.next_up.held().contains(&job));
    handle
        .reorder_job(job, QueueMoveTarget::Bottom)
        .await
        .unwrap();
    held.extend(settle_window_holding_reads(&mut pipeline).await);
    assert!(!pipeline.next_up.ids().contains(&job));
    hold_reads_through(&mut pipeline, &mut held, read).await;
    hear(&mut pipeline, &mut held, &[read]).await;
    assert!(!pipeline.next_up.ids().contains(&job));
    for reply in held.drain(..) {
        pipeline.handle_db_reply(reply).await;
    }
    settle_next_up(&mut pipeline).await;
    assert_ring(&pipeline, &[3, 4, 5]);

    // In, out and in again with neither read heard: the first is dropped,
    // the second is held.
    let job = JobId(30);
    handle.reorder_job(job, QueueMoveTarget::Top).await.unwrap();
    let mut held = settle_window_holding_reads(&mut pipeline).await;
    let out = pipeline.next_up.last_read();
    handle
        .reorder_job(job, QueueMoveTarget::Bottom)
        .await
        .unwrap();
    held.extend(settle_window_holding_reads(&mut pipeline).await);
    handle.reorder_job(job, QueueMoveTarget::Top).await.unwrap();
    held.extend(settle_window_holding_reads(&mut pipeline).await);
    let back = pipeline.next_up.last_read();
    assert!(back > out);
    hold_reads_through(&mut pipeline, &mut held, out).await;
    hear(&mut pipeline, &mut held, &[out]).await;
    assert!(pipeline.next_up.ids().contains(&job));
    assert!(!pipeline.next_up.held().contains(&job));
    hold_reads_through(&mut pipeline, &mut held, back).await;
    hear(&mut pipeline, &mut held, &[back]).await;
    assert!(pipeline.next_up.held().contains(&job));
    for reply in held.drain(..) {
        pipeline.handle_db_reply(reply).await;
    }
    settle_next_up(&mut pipeline).await;
    assert_ring(&pipeline, &[30, 3, 4]);
}

type Parts = Vec<Vec<(u32, u32, u32, crate::jobs::MessageIds)>>;

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

fn fresh(pipeline: &Pipeline, job_id: JobId) -> JobSpec {
    let spec = &pipeline.jobs[&job_id].spec;
    crate::pipeline::activation::read_stored_spec(
        &pipeline.db,
        job_id,
        spec.category.clone(),
        spec.metadata.clone(),
    )
    .unwrap()
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
    assert!(!pipeline.jobs.contains_key(&far));
    handle.reorder_job(far, QueueMoveTarget::Top).await.unwrap();
    handle.cancel_job(JobId(1)).await.unwrap();
    let mut held = settle_window_holding_reads(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    held.extend(settle_window_holding_reads(&mut pipeline).await);
    let read = pipeline.next_up.last_read();
    assert!(pipeline.jobs.contains_key(&far));
    assert!(pipeline.nzb_admitted.contains(&far));
    assert!(pipeline.next_up.ids().contains(&far));
    assert!(!pipeline.next_up.held().contains(&far));

    let expected = fresh(&pipeline, far);
    activate_job(&mut pipeline, far).await;
    assert!(!pipeline.next_up.ids().contains(&far));
    hold_reads_through(&mut pipeline, &mut held, read).await;
    for reply in held.drain(..) {
        pipeline.handle_db_reply(reply).await;
    }
    settle_next_up(&mut pipeline).await;
    assert!(!pipeline.next_up.ids().contains(&far));
    let state = &pipeline.jobs[&far];
    assert!(matches!(
        state.articles,
        crate::jobs::model::JobArticles::Active(_)
    ));
    assert_eq!(lists(&state.spec), lists(&expected));
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
    assert!(pipeline.jobs.contains_key(&far));
    assert_ring(&pipeline, &[far.0, 3, 4]);
}

// A job that takes a slot once its lists are read ahead activates on them,
// with no read of its own, and they are the lists a read gives.
#[tokio::test]
async fn a_job_read_ahead_activates_on_the_lists_held() {
    let (_temp, mut pipeline, handle) = ring(2).await;
    let job_id = JobId(3);
    assert!(pipeline.next_up.held().contains(&job_id));
    handle.cancel_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    turn(&mut pipeline).await;
    assert!(pipeline.nzb_admitted.contains(&job_id));
    assert!(pipeline.next_up.held().contains(&job_id));
    assert_ring(&pipeline, &[4, 5, 6]);

    let expected = fresh(&pipeline, job_id);
    let parses = crate::ingest::NZB_PARSES.load(std::sync::atomic::Ordering::Relaxed);
    activate_job(&mut pipeline, job_id).await;
    assert_eq!(
        crate::ingest::NZB_PARSES.load(std::sync::atomic::Ordering::Relaxed),
        parses
    );
    assert!(!pipeline.next_up.ids().contains(&job_id));
    let state = &pipeline.jobs[&job_id];
    assert!(matches!(
        state.articles,
        crate::jobs::model::JobArticles::Active(_)
    ));
    assert_eq!(lists(&state.spec), lists(&expected));
}

// Seeded jobs of several files, parts posted under one id or several, stored
// as NZBs and read back as the ring reads them: the lists the ring packs
// unpack to the lists read, in order and whole.
#[test]
fn held_lists_unpack_to_the_lists_read() {
    use crate::pipeline::next_up::HeldLists;
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
                                message_ids: crate::jobs::MessageIds::new(
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
        let read = crate::ingest::outline::read_stored_lists(
            &nzb_zstd,
            std::path::Path::new("held.nzb"),
        )
        .unwrap();
        reposted += read
            .lists
            .iter()
            .flatten()
            .filter(|part| part.message_ids.all().len() > 1)
            .count();
        let held = HeldLists::pack(&read.lists).expect("fresh lists pack");
        // One block of ids and fixed records: no string or list per part.
        let parts_held = read.lists.iter().map(Vec::len).sum::<usize>();
        let ids_held = read.lists.iter().flatten().map(|part| part.message_ids.all().len()).sum::<usize>();
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
    use crate::pipeline::next_up::HeldLists;
    let mut ids = crate::jobs::MessageIds::new(vec!["a@x".into(), "b@x".into()]);
    let fresh = SegmentSpec {
        ordinal: 0,
        article_number: 1,
        bytes: 10,
        message_ids: ids.clone(),
    };
    let held = HeldLists::pack(&[Vec::new(), vec![fresh.clone()]]).unwrap();
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
    assert!(HeldLists::pack(&[vec![spent]]).is_none());
}
