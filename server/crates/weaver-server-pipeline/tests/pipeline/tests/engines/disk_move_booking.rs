// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A final move whose destination lies on another filesystem copies, and
// books what it copies on the destination's disk before it writes a byte,
// judged as a start is: by a fresh reading above the reserve, less what is
// promised there. With no room the job waits as a move whose copy found the
// disk full does; as the copy lands its booking settles, and what was not
// written goes back when the move ends. A rename books nothing.

use super::disk_archive::{PLENTY, RESERVE, hears, on_disk_posting, waiting};
use super::disk_reserve::{delivered, disk_held, dispatch_reason, file_named};
use super::harness::{ARTICLE, JOB, Post, Scenario, admit, payload};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use weaver_server_composition::operations::StorageRoot;
use weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON;
use weaver_server_pipeline::jobs::model::JobStatus;
use weaver_server_pipeline::jobs::working_dir::stamp_working_dir;
use weaver_server_pipeline::pipeline::Pipeline;
use weaver_server_pipeline::pipeline::tests::disk_start_fixture::{GIB, add_sized, shown, started};
use weaver_server_pipeline::pipeline::tests::engines::{
    disk_move_fixture::*,
    lifecycle_fixture::{drive, job_status},
};
use weaver_server_pipeline::pipeline::tests::finish_cancel_jobs;
use weaver_server_pipeline::pipeline::tests::queue_fixture::{
    apply_intents, cleanup_pending, handle, working_dir as working_dir_of,
};

const FILES: usize = 12;
// What the job's files hold together.
const BYTES: u64 = (FILES * ARTICLE) as u64;
// A job started beside the one that moves.
const OTHER: u64 = 93_013;

fn files() -> Vec<(String, Vec<u8>)> {
    (0..FILES as u8)
        .map(|n| (format!("quay{n:02}.bin"), payload(ARTICLE, 120 + n)))
        .collect()
}

struct Split {
    _temp_dir: tempfile::TempDir,
    pipeline: Pipeline,
    scenario: Scenario,
    complete_dir: PathBuf,
    working_dir: PathBuf,
    complete_room: Arc<AtomicU64>,
}

// The job of twelve files, its working folder on one disk with plenty of
// room and the complete folder on a disk of its own reading `complete`
// over the reserve.
async fn split_job(complete: u64) -> Split {
    let room = Arc::new(AtomicU64::new(4 * PLENTY));
    let complete_room = Arc::new(AtomicU64::new(complete));
    let posts = files()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, pipeline, complete_dir) =
        admit_split_move(&scenario, room, Arc::clone(&complete_room)).await;
    let working_dir = working_dir_of(&pipeline, JOB).clone();
    stamp_working_dir(intermediate_directory(&pipeline), &working_dir, JOB)
        .await
        .unwrap();
    Split {
        _temp_dir,
        pipeline,
        scenario,
        complete_dir,
        working_dir,
        complete_room,
    }
}

// Every file is delivered once, whole, and nothing is left where the job
// was worked on.
fn all_delivered(complete_dir: &Path, working_dir: &Path) {
    let delivered = delivered(complete_dir);
    assert_eq!(
        delivered.len(),
        FILES,
        "each file once: {:?}",
        delivered.keys()
    );
    for (name, body) in files() {
        assert_eq!(file_named(&delivered, &name), body.as_slice(), "{name}");
    }
    assert!(!working_dir.exists(), "the sources are gone");
}

// The job waits for room for its move, not failed, with no move running.
fn move_held(pipeline: &Pipeline) {
    assert!(disk_held(pipeline, JOB));
    assert!(
        !matches!(job_status(pipeline, JOB), Some(JobStatus::Failed { .. })),
        "the job is not failed"
    );
    assert!(waiting(pipeline));
    assert!(!move_inflight(pipeline));
    assert_eq!(
        dispatch_reason(pipeline).as_deref(),
        Some(DISK_SPACE_WAIT_REASON)
    );
}

// The job let go runs its move to the end and completes.
async fn move_goes_on(split: &mut Split) {
    assert!(!disk_held(&split.pipeline, JOB), "the job was let go");
    resume_full_disk_moves(&mut split.pipeline).await;
    assert!(move_inflight(&split.pipeline), "its move goes on");
    drive(&mut split.pipeline, &split.scenario, JOB, |_| false).await;
    assert_eq!(job_status(&split.pipeline, JOB), Some(JobStatus::Complete));
}

// What the complete disk's ledger held promised once the move booked, told
// on a channel the test reads after.
fn booked_told(pipeline: &Pipeline) -> std::sync::mpsc::Receiver<u64> {
    let (told, booked) = std::sync::mpsc::channel();
    when_booked(pipeline, move |promised| told.send(promised).unwrap());
    booked
}

// The move books its twelve files on the complete disk before it moves the
// first, and as they land the booking settles: nothing stays promised, and
// what the ledger has free is less by what landed, not by what was booked
// and given back.
#[tokio::test]
async fn a_move_across_filesystems_books_its_copy_and_settles_it_as_it_lands() {
    let mut split = split_job(PLENTY).await;
    let booked = booked_told(&split.pipeline);
    let before = complete_room_now(&split.pipeline).unwrap();
    assert_eq!(complete_promised(&split.pipeline), 0);

    drive(&mut split.pipeline, &split.scenario, JOB, |_| false).await;
    assert_eq!(job_status(&split.pipeline, JOB), Some(JobStatus::Complete));
    assert_eq!(
        booked.try_recv(),
        Ok(BYTES),
        "the copy was booked whole first"
    );
    assert_eq!(complete_promised(&split.pipeline), 0, "the booking settled");
    assert_eq!(
        complete_room_now(&split.pipeline),
        Some(before - BYTES),
        "what was booked landed"
    );
    all_delivered(&split.complete_dir, &split.working_dir);
}

// The complete disk has one byte too few above its reserve for the copy: the
// move writes nothing and the job waits, shown waiting for disk space, for
// the copy and the reserve both. A reading still short keeps it waiting;
// one with room for the copy lets it go, and the move books it and runs.
#[tokio::test]
async fn a_move_with_no_room_for_its_copy_waits_before_writing_and_goes_on_with_room() {
    let mut split = split_job(PLENTY).await;
    complete_reads_before_move(&split.pipeline, &split.complete_room, BYTES - 1);
    drive(&mut split.pipeline, &split.scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    move_held(&split.pipeline);
    assert_eq!(full_disk_needs(&split.pipeline), Some(BYTES + RESERVE));
    assert!(
        delivered(&split.complete_dir).is_empty(),
        "nothing was written"
    );
    assert_eq!(
        std::fs::read_dir(&split.working_dir)
            .unwrap()
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("quay"))
            .count(),
        FILES,
        "every file stays where it was"
    );
    assert_eq!(
        complete_promised(&split.pipeline),
        0,
        "nothing stays booked"
    );

    hears(
        &mut split.pipeline,
        &split.complete_room,
        StorageRoot::Complete,
        BYTES - 1,
    );
    move_held(&split.pipeline);

    let booked = booked_told(&split.pipeline);
    hears(
        &mut split.pipeline,
        &split.complete_room,
        StorageRoot::Complete,
        BYTES,
    );
    move_goes_on(&mut split).await;
    assert_eq!(booked.try_recv(), Ok(BYTES));
    assert_eq!(complete_promised(&split.pipeline), 0);
    assert_eq!(register_on_complete(&split.pipeline), 0);
    all_delivered(&split.complete_dir, &split.working_dir);
}

// While the move holds its booking, a job whose start needs a GiB on the
// complete disk, which reads a GiB and half the copy over its reserve, is
// refused for room there, shown waiting for disk space. Once the move ends
// and the disk is read again, it starts.
#[tokio::test]
async fn a_start_is_refused_the_room_a_booked_move_holds() {
    let mut split = split_job(PLENTY).await;
    complete_reads_before_move(&split.pipeline, &split.complete_room, GIB + BYTES / 2);
    let (told, booked) = tokio::sync::oneshot::channel();
    let (go, gone) = std::sync::mpsc::channel::<()>();
    when_booked(&split.pipeline, move |promised| {
        told.send(promised).unwrap();
        gone.recv().unwrap();
    });
    drive(&mut split.pipeline, &split.scenario, JOB, move_inflight).await;
    assert_eq!(booked.await, Ok(BYTES), "the move holds its booking");

    add_sized(&mut split.pipeline, OTHER, 1).await;
    assert!(!started(&split.pipeline).contains(&OTHER), "refused");
    assert_eq!(
        shown(&split.pipeline, OTHER).0.as_deref(),
        Some(DISK_SPACE_WAIT_REASON)
    );

    go.send(()).unwrap();
    drive(&mut split.pipeline, &split.scenario, JOB, |_| false).await;
    assert_eq!(job_status(&split.pipeline, JOB), Some(JobStatus::Complete));
    assert_eq!(complete_promised(&split.pipeline), 0);
    assert!(!started(&split.pipeline).contains(&OTHER));

    hears(
        &mut split.pipeline,
        &split.complete_room,
        StorageRoot::Complete,
        GIB + BYTES / 2,
    );
    apply_intents(&mut split.pipeline).await;
    assert!(started(&split.pipeline).contains(&OTHER), "it starts");
}

// A job waiting for room for its copy holds nothing booked and cannot be
// paused; cancelled, it leaves nothing booked or promised on the complete
// disk, and nothing in its destination.
#[tokio::test]
async fn a_move_waiting_for_room_for_its_copy_is_cancelled_holding_nothing() {
    let mut split = split_job(PLENTY).await;
    complete_reads_before_move(&split.pipeline, &split.complete_room, 0);
    drive(&mut split.pipeline, &split.scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    move_held(&split.pipeline);
    assert_eq!(complete_promised(&split.pipeline), 0);

    let queue = handle(&split.pipeline);
    assert!(
        queue.pause_job(JOB).await.is_err(),
        "a held move is not paused"
    );
    assert_eq!(complete_promised(&split.pipeline), 0);

    queue.cancel_job(JOB).await.unwrap();
    apply_intents(&mut split.pipeline).await;
    finish_cancel_jobs(&mut split.pipeline).await;
    while cleanup_pending(&split.pipeline, JOB) {
        tokio::task::yield_now().await;
    }
    assert!(!disk_held(&split.pipeline, JOB));
    assert_eq!(complete_promised(&split.pipeline), 0);
    assert_eq!(register_on_complete(&split.pipeline), 0);
    assert!(delivered(&split.complete_dir).is_empty());
}

// On one filesystem the move renames, and books nothing.
#[tokio::test]
async fn a_rename_on_one_filesystem_books_nothing() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let posts = files()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    copy_across(true);
    let booked = booked_told(&pipeline);

    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(job_status(&pipeline, JOB), Some(JobStatus::Complete));
    assert!(booked.try_recv().is_err(), "no booking was made");
    assert_eq!(complete_promised(&pipeline), 0);
    let delivered = delivered(&complete_dir);
    assert_eq!(delivered.len(), FILES);
    for (name, body) in files() {
        assert_eq!(file_named(&delivered, &name), body.as_slice(), "{name}");
    }
}

// A move whose destination fills after five files gives back what it booked
// for the seven it did not write. Let go, it books only those seven.
#[tokio::test]
async fn a_move_resumed_after_a_hold_books_only_what_is_left() {
    let mut split = split_job(PLENTY).await;
    let first = booked_told(&split.pipeline);
    destination_room(Some(5));
    drive(&mut split.pipeline, &split.scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    move_held(&split.pipeline);
    assert_eq!(first.try_recv(), Ok(BYTES));
    assert_eq!(complete_promised(&split.pipeline), 0, "the rest went back");
    let whole = delivered(&split.complete_dir)
        .values()
        .filter(|body| body.len() == ARTICLE)
        .count();
    assert_eq!(whole, 5);

    destination_room(None);
    let second = booked_told(&split.pipeline);
    hears(
        &mut split.pipeline,
        &split.complete_room,
        StorageRoot::Complete,
        2 * PLENTY,
    );
    move_goes_on(&mut split).await;
    assert_eq!(second.try_recv(), Ok(BYTES - 5 * ARTICLE as u64));
    assert_eq!(complete_promised(&split.pipeline), 0);
    all_delivered(&split.complete_dir, &split.working_dir);
}
