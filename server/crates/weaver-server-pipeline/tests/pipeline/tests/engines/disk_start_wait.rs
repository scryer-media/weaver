// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A started job whose write finds the disk full gives back what its start
// promised and is held; it goes on only on a reading begun after the
// failure that leaves room for what it needs, and a resume while the disk is
// short leaves it held, its row showing the figures.

use super::disk_archive::{PLENTY, RESERVE, SECOND, finish_disk_job, hears, on_disk_posting};
use super::disk_reserve::*;
use super::harness::{ARTICLE, JOB, Post, admit, payload};
use std::sync::{Arc, atomic::AtomicU64, atomic::Ordering};
use weaver_server_composition::operations::StorageRoot;
use weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON;
use weaver_server_pipeline::jobs::model::JobStatus;
use weaver_server_pipeline::pipeline::Pipeline;
use weaver_server_pipeline::pipeline::tests::disk_start_fixture::{promised_here, shown};
use weaver_server_pipeline::pipeline::tests::queue_fixture::{apply_intents, cancel_batch, handle};

// What each root's own count holds promised.
fn promised_by_root(pipeline: &Pipeline) -> [u64; 3] {
    StorageRoot::ALL.map(|root| promised_here(pipeline, root))
}

#[derive(Clone, Copy, Debug)]
enum End {
    Pause,
    Cancel,
    Finish,
}

// 1 (review). A job whose write failed on the complete folder, both folders
// on one disk, starts again with what it has left promised on the working
// disk's count, where its posts land; it is paused, cancelled or finishes,
// and nothing stays promised on any root.
#[tokio::test]
async fn a_start_after_a_hold_on_the_complete_root_gives_back_where_it_was_promised() {
    for end in [End::Pause, End::Cancel, End::Finish] {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let body = payload(4 * ARTICLE, 73);
        let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
        let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
        apply_intents(&mut pipeline).await;
        // The write fails with the disk showing no room.
        room.store(0, Ordering::Release);
        hold_on_root(&mut pipeline, StorageRoot::Complete);
        park_disk_held_jobs(&mut pipeline);
        old_graph_heard(&mut pipeline).await;
        assert_eq!(promised_by_root(&pipeline), [0, 0, 0], "{end:?}: held");

        hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
        assert!(!full_disk_held(&pipeline), "{end:?}: room lets it go");
        let by_root = promised_by_root(&pipeline);
        assert!(
            by_root[1] > 0 && by_root[2] == 0,
            "{end:?}: the start is promised on the working disk's count, by root {by_root:?}"
        );
        restarts(&mut pipeline).await;

        match end {
            End::Pause => {
                handle(&pipeline).pause_job(JOB).await.unwrap();
                apply_intents(&mut pipeline).await;
                apply_intents(&mut pipeline).await;
            }
            End::Cancel => {
                cancel_batch(&mut pipeline, &[JOB]).await;
            }
            End::Finish => {
                finish_disk_job(&mut pipeline, &scenario).await;
                assert_eq!(status(&pipeline), Some(JobStatus::Complete));
            }
        }
        assert_eq!(
            promised_by_root(&pipeline),
            [0, 0, 0],
            "{end:?}: nothing stays promised"
        );
    }
}

// 7b (review). A job held in its download by a quota, let go on its figure
// as a quota hold is, starts with what it has left promised on the working
// disk, as one let go on room does, and nothing stays promised once it
// completes.
#[tokio::test]
async fn a_quota_hold_let_go_on_its_figure_is_promised_what_it_has_left() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 74);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    apply_intents(&mut pipeline).await;
    hold_on_quota(&mut pipeline);
    park_disk_held_jobs(&mut pipeline);
    old_graph_heard(&mut pipeline).await;
    assert_eq!(promised_by_root(&pipeline), [0, 0, 0], "held");
    let left = full_disk_start_promise(&pipeline);
    assert!(left > 0, "it has something left to fetch");

    let (rise, quota) = full_disk_rise(&pipeline);
    assert!(quota, "a quota hold");
    let mark = full_disk_mark(&pipeline).expect("a quota hold has a mark");
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        mark - RESERVE + rise.expect("what it waits to rise by"),
    );
    assert!(!full_disk_held(&pipeline), "the figure lets it go");
    assert_eq!(
        promised_by_root(&pipeline),
        [0, left, 0],
        "its start is promised what it has left"
    );
    restarts(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    assert_eq!(
        promised_by_root(&pipeline),
        [0, 0, 0],
        "nothing stays promised"
    );
}

// 5 (review). Two jobs held in their download as steps that each need 60
// units free are let go on one reading of 100: each is judged on what its
// step needs, and promised only what it has left to fetch, so both go and
// neither is failed. Their steps then book their own room, and the second
// waits there (graph namespace test
// `two_steps_let_go_on_one_room_book_it_in_turn_and_the_second_waits`).
#[tokio::test]
async fn two_step_holds_let_go_on_one_room_both_go_and_neither_fails() {
    const UNIT: u64 = 1 << 20;
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk_posting(
        &room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 75))],
    );
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    add_second(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    for job in [JOB, SECOND] {
        hold_step_of(&mut pipeline, job, RESERVE + 60 * UNIT);
    }
    park_disk_held_jobs(&mut pipeline);
    old_graphs_heard(&mut pipeline).await;
    assert_eq!(promised_by_root(&pipeline), [0, 0, 0], "both held");
    let left = [JOB, SECOND].map(|job| full_disk_start_promise_of(&pipeline, job));
    assert!(
        left.iter().all(|left| *left < 20 * UNIT),
        "each has little left to fetch: {left:?}"
    );

    hears(&mut pipeline, &room, StorageRoot::Intermediate, 100 * UNIT);
    for job in [JOB, SECOND] {
        assert!(!disk_held(&pipeline, job), "{job:?} goes");
        assert!(
            !matches!(status_of(&pipeline, job), Some(JobStatus::Failed { .. })),
            "{job:?} is not failed"
        );
    }
    assert_eq!(
        promised_by_root(&pipeline),
        [0, left[0] + left[1], 0],
        "each start is promised what it has left, not what its step needs"
    );
}

// 9c. With its posts promised at its start, a write that finds the disk full
// all the same holds the job for a full disk, not failed: what the start
// promised and did not land is given back, and the job completes once there
// is room.
#[tokio::test]
async fn a_started_job_whose_write_finds_the_disk_full_gives_back_its_promise_and_is_held() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 71);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    // The job is put in its slot as a restored one is: the next turn
    // promises what it still has to fetch.
    apply_intents(&mut pipeline).await;
    assert_eq!(
        working(&pipeline).promised(),
        body.len() as u64,
        "the start promised its posts"
    );
    room.store(0, Ordering::Release);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));

    until_full(&mut pipeline, &scenario).await;
    assert_eq!(
        working(&pipeline).promised(),
        0,
        "the held job holds no promise"
    );
    full_disk_freed(&mut pipeline, &scenario, &room).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// 11. A job held for a full disk in its download, its disk read full before
// the write failed: that reading heard again lets nothing go, though the disk
// has room by then; a reading begun after it with one byte less than the job
// needs keeps it held, and so does a resume, its row showing what it needs
// and what is free. A reading with the room it needs lets it go, and it
// completes.
#[tokio::test]
async fn a_full_disk_hold_goes_on_only_at_a_later_reading_with_the_room_it_needs() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 72);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    fill_after(None);

    // The reading from before the failure is heard again and lets nothing
    // go, though the disk has room now.
    room.store(PLENTY, Ordering::Release);
    hear_disk_reading(&mut pipeline);
    assert!(full_disk_held(&pipeline), "the older reading wakes nothing");

    let need = full_disk_start_need(&pipeline);
    assert!(need > 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, need - 1);
    assert!(full_disk_held(&pipeline), "one byte short");

    resume_disk_held_job(&mut pipeline);
    apply_intents(&mut pipeline).await;
    assert!(full_disk_held(&pipeline), "a resume while short stays held");
    assert_eq!(
        shown(&pipeline, JOB.0),
        (
            Some(DISK_SPACE_WAIT_REASON.to_string()),
            Some(need),
            Some(need - 1)
        )
    );

    hears(&mut pipeline, &room, StorageRoot::Intermediate, need);
    old_graph_heard(&mut pipeline).await;
    assert!(!full_disk_held(&pipeline), "the room it needs lets it go");
    restarts(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
}
