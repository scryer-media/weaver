// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::Path;
use weaver_server_pipeline::jobs::working_dir::stamp_working_dir;
use weaver_server_pipeline::pipeline::tests::engines::{
    disk_move_fixture::*,
    lifecycle_fixture::{drive, job_status},
};
use weaver_server_pipeline::pipeline::tests::queue_fixture::{
    cleanup_pending, working_dir as working_dir_of,
};

// Twelve plain files, each of one article.
fn twelve() -> Vec<(String, Vec<u8>)> {
    (0..12u8)
        .map(|n| (format!("harbor{n:02}.bin"), payload(ARTICLE, 50 + n)))
        .collect()
}

// The job's final move found its destination full: it waits, not failed,
// shown waiting for disk space, with no move running.
fn assert_move_held(pipeline: &Pipeline) {
    assert!(disk_held(pipeline, JOB));
    assert!(
        !matches!(job_status(pipeline, JOB), Some(JobStatus::Failed { .. })),
        "the job is not failed"
    );
    assert!(waiting(pipeline));
    assert!(!move_inflight(pipeline));
    assert_eq!(
        dispatch_reason(pipeline).as_deref(),
        Some(weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
}

// The job let go starts its move again at the top of the loop's next turn,
// and runs to its end: every file is delivered once, whole, into `under`,
// and nothing is left where it was worked on.
async fn move_goes_on(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    under: &Path,
    working_dir: &Path,
) {
    assert!(!disk_held(pipeline, JOB), "the job was let go");
    resume_full_disk_moves(pipeline).await;
    assert!(
        move_inflight(pipeline),
        "its move goes on: {:?}",
        job_status(pipeline, JOB)
    );
    drive(pipeline, scenario, JOB, |_| false).await;
    assert_eq!(job_status(pipeline, JOB), Some(JobStatus::Complete));
    let files = delivered(under);
    assert_eq!(files.len(), 12, "each file once: {:?}", files.keys());
    for (name, body) in twelve() {
        assert_eq!(file_named(&files, &name), body.as_slice(), "{name}");
    }
    assert!(!working_dir.exists(), "the sources are gone");
    assert!(!waiting(pipeline));
}

// A job of twelve files is moved to its destination, which fills after the
// fifth: the job waits for room, shown waiting for disk space, and is not
// failed. A part for it that finds no room in the spool meanwhile is
// dropped, and does not fail it either. A reading with no more room keeps
// it waiting. Once room is made the move goes on from what it left: the
// file cut short is moved again whole, and the job completes with its
// twelve files, each once, and its working directory gone.
#[tokio::test]
async fn a_final_move_whose_destination_fills_waits_for_room_and_delivers_every_file() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let working_dir = working_dir_of(&pipeline, JOB).clone();
    // The working directory carries the job's ownership marker, as every
    // directory a job is added with does.
    stamp_working_dir(intermediate_directory(&pipeline), &working_dir, JOB)
        .await
        .unwrap();
    let output = complete_ledger(&pipeline);

    destination_room(Some(5));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    assert_move_held(&pipeline);
    // The device of the file that found no room was never read: the job
    // waits on that file's own disk, not on a root taken from a folder
    // above it.
    assert!(!output.waited_on());
    assert_eq!(waited_paths(&pipeline), 1);
    let whole = delivered(&complete_dir)
        .iter()
        .filter(|(_, bytes)| bytes.len() == ARTICLE)
        .count();
    assert_eq!(whole, 5, "five files moved before the disk filled");
    assert!(working_dir.exists(), "the rest stays where it was");

    // A part of the job finds no room in the spool: nothing reads it now.
    move_spool_filled(&mut pipeline);
    assert_move_held(&pipeline);

    hears(&mut pipeline, &room, StorageRoot::Complete, 0);
    assert_move_held(&pipeline);

    destination_room(None);
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert_eq!(waited_paths(&pipeline), 0);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// The job's category delivers to a folder on a disk of its own, which
// fills during the move: the job waits on that folder's disk, which is read
// only while the job waits on it. Room on the complete folder's disk does
// not let it go; room on its own does.
#[tokio::test]
async fn a_final_move_to_a_folder_on_its_own_disk_waits_on_that_disk() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let library_room = Arc::new(AtomicU64::new(0));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (temp_dir, mut pipeline, _) =
        admit_library_move(&scenario, Arc::clone(&room), Arc::clone(&library_room)).await;
    let library = temp_dir.path().join("library-disk");
    let working_dir = working_dir_of(&pipeline, JOB).clone();
    // The working directory carries the job's ownership marker, as every
    // directory a job is added with does.
    stamp_working_dir(intermediate_directory(&pipeline), &working_dir, JOB)
        .await
        .unwrap();
    let output = complete_ledger(&pipeline);
    assert_eq!(waited_paths(&pipeline), 0);

    destination_room(Some(5));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert_eq!(
        waited_paths(&pipeline),
        1,
        "the folder's disk is read while the job waits on it"
    );
    assert!(
        !output.waited_on() && !working(&pipeline).waited_on(),
        "it waits on neither root's disk"
    );
    destination_room(None);

    // The folder's disk is read with the complete root: its first reading
    // since the failure is all the job has to compare with.
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert_move_held(&pipeline);
    // The roots read more room still; the folder's disk reads none made.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 4 * PLENTY);
    hears(&mut pipeline, &room, StorageRoot::Complete, 4 * PLENTY);
    assert_move_held(&pipeline);

    library_room.store(2 * PLENTY, Ordering::Release);
    hears(&mut pipeline, &room, StorageRoot::Complete, 4 * PLENTY);
    assert_eq!(
        waited_paths(&pipeline),
        0,
        "no one waits on the folder's disk, and it is read no more"
    );
    move_goes_on(&mut pipeline, &scenario, &library, &working_dir).await;
}

// One file, on disks that keep `KEPT` free, whose destination has no room
// for it: the job as its final move starts, and the file's size.
async fn one_large_file(
    room: &Arc<AtomicU64>,
) -> (
    tempfile::TempDir,
    Pipeline,
    std::path::PathBuf,
    Scenario,
    Vec<u8>,
) {
    let body = payload(3 * ARTICLE, 71);
    let scenario = on_disk_keeping(room, vec![Post::plain("harbor.bin", body.clone())], KEPT);
    let (temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    destination_room(Some(0));
    drive(&mut pipeline, &scenario, JOB, move_inflight).await;
    assert!(move_inflight(&pipeline), "the move started");
    (temp_dir, pipeline, complete_dir, scenario, body)
}

// The one file is delivered once, whole, and the job completes.
async fn one_file_delivered(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    complete_dir: &Path,
    body: &[u8],
) {
    assert!(!disk_held(pipeline, JOB), "the job was let go");
    resume_full_disk_moves(pipeline).await;
    assert!(move_inflight(pipeline), "its move goes on");
    drive(pipeline, scenario, JOB, |_| false).await;
    assert_eq!(job_status(pipeline, JOB), Some(JobStatus::Complete));
    let files = delivered(complete_dir);
    assert_eq!(files.len(), 1, "the file once: {:?}", files.keys());
    assert_eq!(file_named(&files, "harbor.bin"), body);
    assert!(!waiting(pipeline));
}

// A file larger than the room its destination has: the copy fills the
// disk, fails, and removes what it wrote, so the disk shows that room free
// again with none made. A reading taken while the copy was writing showed
// less. The job is held once, and the room the copy gave back does not let
// it go, though it is more than the disk showed before: only a reading that
// shows the room the file takes does. The file is then delivered once.
#[tokio::test]
async fn a_move_whose_copy_gave_its_room_back_waits_for_the_room_the_file_takes() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, scenario, body) = one_large_file(&room).await;
    let size = body.len() as u64;

    // The disk is read while the copy is part written.
    hears(&mut pipeline, &room, StorageRoot::Complete, size / 3);
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert_eq!(
        full_disk_needs(&pipeline),
        Some(size),
        "the move needs the room its file takes"
    );
    assert!(
        delivered(&complete_dir).is_empty(),
        "the copy removed what it wrote"
    );

    // The disk shows what the copy gave back: more than it showed during
    // the write, less than the file takes.
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * size / 3);
    assert_move_held(&pipeline);
    hears(&mut pipeline, &room, StorageRoot::Complete, size - KEPT - 1);
    assert_move_held(&pipeline);

    destination_room(None);
    hears(&mut pipeline, &room, StorageRoot::Complete, size);
    one_file_delivered(&mut pipeline, &scenario, &complete_dir, &body).await;
}

// The disk read before the copy failed showed room enough for the file.
// That reading, heard again once the job is held, does not let it go: only
// a reading whose probe began after the failure counts.
#[tokio::test]
async fn a_reading_from_before_a_move_failed_does_not_let_the_job_go() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, scenario, body) = one_large_file(&room).await;
    // The disk fills as the copy is written; nothing reads it yet.
    room.store(body.len() as u64 / 3, Ordering::Release);
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert!(full_disk_needs(&pipeline).is_some());

    // The newest reading is the one from before the failure, or one taken
    // since that shows the disk as it is: neither lets the job go.
    hear_disk_reading(&mut pipeline);
    assert_move_held(&pipeline);

    destination_room(None);
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    one_file_delivered(&mut pipeline, &scenario, &complete_dir, &body).await;
}

// Twelve files whose move is held after the fifth: the job, where it works,
// and where it delivers. `stamped` gives the working directory its
// ownership marker.
async fn held_after_five(
    room: &Arc<AtomicU64>,
    stamped: bool,
) -> (
    tempfile::TempDir,
    Pipeline,
    std::path::PathBuf,
    std::path::PathBuf,
    Scenario,
) {
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(room, posts);
    let (temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let working_dir = working_dir_of(&pipeline, JOB).clone();
    if stamped {
        stamp_working_dir(intermediate_directory(&pipeline), &working_dir, JOB)
            .await
            .unwrap();
    }
    destination_room(Some(5));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    assert_move_held(&pipeline);
    destination_room(None);
    // The disk reads no room over the reserve.
    hears(&mut pipeline, room, StorageRoot::Complete, 0);
    assert_move_held(&pipeline);
    (temp_dir, pipeline, complete_dir, working_dir, scenario)
}

// The temp folder is changed while a job is held in its final move: the
// job's working directory stays where it is, and once room is made the move
// goes on from it and delivers every file, none left behind.
#[tokio::test]
async fn a_temp_folder_changed_during_a_held_move_loses_no_file() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;

    let elsewhere = temp_dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    retarget_intermediate(&mut pipeline, elsewhere);
    hear_disk_reading(&mut pipeline);
    assert_move_held(&pipeline);
    assert!(working_dir.exists(), "the working directory stays");
    assert_eq!(working_dir_of(&pipeline, JOB), &working_dir);

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// The folder a held move delivers into is removed during the hold, as a
// user making room does, the five files already moved with it: once room
// shows, the move goes on, makes the folder again, and delivers each of the
// seven files still where the job worked, once and whole.
#[tokio::test]
async fn a_destination_removed_during_a_held_move_is_made_again() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;
    let moved: Vec<String> = delivered(&complete_dir)
        .iter()
        .filter(|(_, bytes)| bytes.len() == ARTICLE)
        .map(|(name, _)| name.clone())
        .collect();
    assert_eq!(moved.len(), 5);
    for entry in std::fs::read_dir(&complete_dir).unwrap() {
        std::fs::remove_dir_all(entry.unwrap().path()).unwrap();
    }
    assert!(delivered(&complete_dir).is_empty());

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(!disk_held(&pipeline, JOB), "the job was let go");
    resume_full_disk_moves(&mut pipeline).await;
    assert!(move_inflight(&pipeline), "its move goes on");
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(job_status(&pipeline, JOB), Some(JobStatus::Complete));
    let files = delivered(&complete_dir);
    assert_eq!(files.len(), 7, "each file left, once: {:?}", files.keys());
    for (name, body) in twelve() {
        if !moved.iter().any(|gone| gone.ends_with(&name)) {
            assert_eq!(file_named(&files, &name), body.as_slice(), "{name}");
        }
    }
    assert!(!working_dir.exists(), "the sources are gone");
}

// A held move whose working directory carries no ownership marker goes on
// all the same: the job knows its own directory, and nothing looks for it.
#[tokio::test]
async fn a_held_move_whose_working_directory_has_no_marker_goes_on() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, false).await;
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// The folder a job delivers into cannot be made for want of room: the job
// is held, not failed, waiting on that disk. Once room is made the move
// claims its folder and delivers every file.
#[tokio::test]
async fn a_destination_that_cannot_be_made_for_want_of_room_holds_the_job() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let working_dir = working_dir_of(&pipeline, JOB).clone();
    let output = complete_ledger(&pipeline);

    claim_full(true);
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert!(output.waited_on(), "it waits on the destination's disk");
    assert_eq!(full_disk_needs(&pipeline), None);
    // The job's completion is looked at again while it waits: no move
    // starts.
    recheck_move_completion(&mut pipeline).await;
    assert_move_held(&pipeline);

    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    assert_move_held(&pipeline);

    claim_full(false);
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(!output.waited_on());
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// A path whose disk was never read is waited on as itself, with no look at
// its device as the job is held. So is a path below a root's folder whose
// own device was never read: the folder above it may lie on another disk.
// Only the root's own folder, whose device is known, is that root's disk.
#[tokio::test]
async fn a_failing_path_on_a_disk_not_known_is_waited_on_as_itself() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk(&room);
    let (temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let capacity = capacity_ref(&pipeline);
    let unseen = temp_dir.path().join("never-read").join("folder");
    weaver_server_disk::runtime::fs::test_devices::assign(intermediate_directory(&pipeline), 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete_dir, 2);
    for root in StorageRoot::ALL {
        let begun = capacity.probes_begun(root);
        capacity.request_refresh(root);
        capacity.wait_for_reading_begun_after(root, begun);
    }
    assert_eq!(
        capacity.known_root_of(&complete_dir),
        Some(StorageRoot::Complete)
    );
    assert_eq!(
        capacity.known_root_of(intermediate_directory(&pipeline)),
        Some(StorageRoot::Intermediate)
    );
    let below = complete_dir.join("ember").join("a.bin");
    assert_eq!(capacity.known_root_of(&below), None);
    assert_eq!(
        capacity.known_root_of(&intermediate_directory(&pipeline).join("ember")),
        None
    );
    assert_eq!(capacity.known_root_of(&unseen), None);

    let asks = weaver_server_disk::operations::disk::same_filesystem_asks();
    hold_restored_move(&mut pipeline, &below);
    assert!(
        disk_held_on_path(&pipeline, JOB),
        "a path below a known folder is waited on as itself"
    );
    assert_eq!(capacity.waited_paths(), 1);
    assert!(drop_disk_hold(&mut pipeline));
    assert_eq!(capacity.waited_paths(), 0);
    hold_restored_move(&mut pipeline, &unseen);
    assert!(disk_held_on_path(&pipeline, JOB), "the path is waited on");
    assert_eq!(capacity.waited_paths(), 1);
    assert_eq!(
        weaver_server_disk::operations::disk::same_filesystem_asks(),
        asks,
        "no device was asked after"
    );
}

// The cleanup of a cancelled job has run.
async fn cleaned_up(pipeline: &Pipeline) {
    while cleanup_pending(pipeline, JOB) {
        tokio::task::yield_now().await;
    }
}

// A job held in its final move cannot be paused, and can be cancelled, with
// its history deleted or kept: no hold, no wait on any disk, no working
// directory and no part-filled destination are left.
#[tokio::test]
async fn a_job_held_in_its_final_move_is_cancelled_and_deleted_clean() {
    for delete in [false, true] {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let (_temp_dir, mut pipeline, complete_dir, working_dir, _scenario) =
            held_after_five(&room, true).await;
        let output = complete_ledger(&pipeline);
        assert!(std::fs::read_dir(&complete_dir).unwrap().next().is_some());

        let handle = handle(&pipeline);
        assert!(
            handle.pause_job(JOB).await.is_err(),
            "a job held in its move is not paused"
        );
        if delete {
            handle.cancel_job_and_delete_history(JOB).await.unwrap();
        } else {
            handle.cancel_job(JOB).await.unwrap();
        }
        apply_intents(&mut pipeline).await;
        finish_cancel_jobs(&mut pipeline).await;
        assert!(!disk_held(&pipeline, JOB), "no hold is left");
        assert!(!waiting(&pipeline));
        assert!(!output.waited_on());
        assert_eq!(waited_paths(&pipeline), 0);
        assert!(!move_destination_reserved(&pipeline));
        cleaned_up(&pipeline).await;
        assert!(!working_dir.exists(), "the working directory is gone");
        assert!(
            std::fs::read_dir(&complete_dir).unwrap().next().is_none(),
            "the part-filled destination is gone"
        );
    }
}

// Post-processing is paused while a job is held in its final move: room
// made does not start the move, as it starts no first move. The job goes on
// when post-processing resumes.
#[tokio::test]
async fn a_held_move_let_go_waits_while_post_processing_is_paused() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;
    let handle = handle(&pipeline);
    handle.pause_post_processing().await.unwrap();
    apply_intents(&mut pipeline).await;

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert_move_held(&pipeline);
    resume_full_disk_moves(&mut pipeline).await;
    assert!(!move_inflight(&pipeline), "no move starts");

    handle.resume_post_processing().await.unwrap();
    apply_intents(&mut pipeline).await;
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// A name the destination and the working directory both hold when a held
// move goes on: the destination keeps what it holds, as on a first move,
// and every other file is delivered.
#[tokio::test]
async fn a_held_move_keeps_a_name_the_destination_already_holds() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;
    // A name no copy was cut short on: the names that were are moved again.
    let waiting = "harbor-extra.bin";
    assert!(
        cut_short_copies(&pipeline)
            .iter()
            .all(|path| !path.ends_with(waiting))
    );
    std::fs::write(working_dir.join(waiting), b"from the working directory").unwrap();
    let folder = std::fs::read_dir(&complete_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(folder.join(waiting), b"already here").unwrap();

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(!disk_held(&pipeline, JOB), "the job was let go");
    resume_full_disk_moves(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    let files = delivered(&complete_dir);
    assert_eq!(
        std::fs::read(folder.join(waiting)).unwrap(),
        b"already here",
        "{:?}",
        files.keys()
    );
    for (name, body) in twelve() {
        assert_eq!(file_named(&files, &name), body.as_slice(), "{name}");
    }
    assert!(
        !matches!(job_status(&pipeline, JOB), Some(JobStatus::Failed { .. })),
        "{:?}",
        job_status(&pipeline, JOB)
    );
}

// The copy that found no room left what it wrote in the destination. The
// hold records that name, and the move let go replaces it from the working
// directory: the file is whole, and there once.
#[tokio::test]
async fn a_copy_cut_short_that_stays_in_the_destination_is_delivered_whole() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    keep_partial_copy(true);
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;
    keep_partial_copy(false);
    // Every file past the fifth found no room.
    let cut_short = cut_short_copies(&pipeline);
    assert_eq!(cut_short.len(), 7, "{cut_short:?}");
    for partial in &cut_short {
        assert_eq!(
            std::fs::metadata(partial).unwrap().len(),
            ARTICLE as u64 / 2,
            "half of the file stands in the destination"
        );
        assert!(
            working_dir.join(partial.file_name().unwrap()).exists(),
            "its source stays"
        );
    }

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// A job held before it claimed its destination is let go. A cancel taken
// before its move begins wins: no move starts, and the job is cancelled
// clean. Once its move has begun, in the one step before anything is
// awaited, a cancel is refused as for any moving job. Never both.
#[tokio::test]
async fn a_cancel_and_a_move_let_go_never_both_win() {
    for cancel_first in [true, false] {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let posts = twelve()
            .into_iter()
            .map(|(name, body)| Post::plain(&name, body))
            .collect();
        let scenario = on_disk_posting(&room, posts);
        let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
        let working_dir = working_dir_of(&pipeline, JOB).clone();
        claim_full(true);
        drive(&mut pipeline, &scenario, JOB, |pipeline| {
            disk_held(pipeline, JOB)
        })
        .await;
        assert_move_held(&pipeline);
        claim_full(false);
        hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
        hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
        assert!(!disk_held(&pipeline, JOB), "the job was let go");
        let handle = handle(&pipeline);

        if cancel_first {
            handle.cancel_job(JOB).await.unwrap();
            assert!(
                !begin_let_go_move(&mut pipeline),
                "the cancel stops the move"
            );
            resume_full_disk_moves(&mut pipeline).await;
            assert!(!move_inflight(&pipeline), "no move starts");
            assert!(!move_destination_reserved(&pipeline));
            apply_intents(&mut pipeline).await;
            finish_cancel_jobs(&mut pipeline).await;
            cleaned_up(&pipeline).await;
            assert!(!working_dir.exists(), "the working directory is gone");
            assert!(delivered(&complete_dir).is_empty(), "nothing was delivered");
        } else {
            assert!(begin_let_go_move(&mut pipeline), "the move begins");
            assert!(
                handle.cancel_job(JOB).await.is_err(),
                "a job whose move has begun is not cancelled"
            );
            // The move goes on as begun.
            move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
        }
    }
}

// A job held after part of its files were moved; the temp folder is then
// pointed elsewhere, and weaver starts again: the job comes back from its
// moving row, which names the destination and where the move comes from.
// With or without an ownership marker on the working directory, the move is
// not taken for finished: it goes on from the recorded source, and every
// file is delivered.
#[tokio::test]
async fn a_restart_during_a_held_move_delivers_every_file_from_the_recorded_source() {
    for stamped in [true, false] {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let (temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
            held_after_five(&room, stamped).await;
        assert_eq!(
            delivery_source(&pipeline),
            Some(working_dir.as_path()),
            "the move's source is kept with the job"
        );
        let facts = successful_move_facts(&pipeline);
        assert_eq!(facts.source.as_deref(), Some(working_dir.as_path()));
        let stored: weaver_server_pipeline::post_processing::model::PostProcessingFacts =
            serde_json::from_str(&serde_json::to_string(&facts).unwrap()).unwrap();
        assert_eq!(
            stored.source.as_deref(),
            Some(working_dir.as_path()),
            "the moving row carries it"
        );
        let dest = cut_short_copies(&pipeline)[0]
            .parent()
            .unwrap()
            .to_path_buf();

        let elsewhere = temp_dir.path().join("elsewhere");
        std::fs::create_dir_all(&elsewhere).unwrap();
        retarget_intermediate(&mut pipeline, elsewhere);

        // The job as a restart restores it from its moving row: no hold, no
        // destination reserved, the row's directory as where it stands, and
        // the source the row recorded.
        assert!(drop_disk_hold(&mut pipeline));
        resume_recorded_move(&mut pipeline, dest, stored.source).await;
        assert!(
            move_inflight(&pipeline),
            "the move goes on: {:?}",
            job_status(&pipeline, JOB)
        );
        drive(&mut pipeline, &scenario, JOB, |_| false).await;
        assert_eq!(job_status(&pipeline, JOB), Some(JobStatus::Complete));
        let files = delivered(&complete_dir);
        assert_eq!(files.len(), 12, "each file once: {:?}", files.keys());
        for (name, body) in twelve() {
            assert_eq!(file_named(&files, &name), body.as_slice(), "{name}");
        }
        assert!(!working_dir.exists(), "the sources are gone");
    }
}

// The row that says a move began is refused for want of room on the
// database's disk: nothing moves, the job is held on the data folder's
// disk and shown waiting for disk space, not failed. A reading that shows
// no more keeps it waiting. Once more shows, the row is written, once, and
// the move delivers every file.
#[tokio::test]
async fn a_moving_row_the_database_disk_has_no_room_for_holds_the_job_and_is_written_again() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let working_dir = working_dir_of(&pipeline, JOB).clone();
    stamp_working_dir(intermediate_directory(&pipeline), &working_dir, JOB)
        .await
        .unwrap();

    refuse_moving_row(weaver_server_pipeline::StateError::OutOfSpace(
        "database or disk is full".into(),
    ));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    assert_move_held(&pipeline);
    // The harness keeps the data folder on the complete root's disk, so
    // the hold reads that root.
    assert_eq!(moving_rows_written(), 0);
    assert!(
        delivered(&complete_dir).is_empty(),
        "nothing moved before the row was written"
    );
    assert!(working_dir.exists());

    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    assert_move_held(&pipeline);

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
    assert_eq!(moving_rows_written(), 1, "the moving row was written once");
}

// The database's folder is on a disk of its own, apart from the data
// folder's: a job whose moving row found no room waits on the database's
// disk. More room on every root, the data folder's among them, does not let
// it go; more on the database's disk does.
#[tokio::test]
async fn a_moving_row_refused_waits_on_the_database_folder_not_the_data_folder() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let database_room = Arc::new(AtomicU64::new(0));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (temp_dir, mut pipeline, complete_dir) =
        admit_database_move(&scenario, Arc::clone(&room), Arc::clone(&database_room)).await;
    assert_eq!(database_directory(&pipeline), temp_dir.path());
    assert_ne!(database_directory(&pipeline), data_directory(&pipeline));
    let working_dir = working_dir_of(&pipeline, JOB).clone();
    stamp_working_dir(intermediate_directory(&pipeline), &working_dir, JOB)
        .await
        .unwrap();

    refuse_moving_row(weaver_server_pipeline::StateError::OutOfSpace(
        "database or disk is full".into(),
    ));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        disk_held(pipeline, JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert!(disk_held_on_path(&pipeline, JOB), "it waits on a path");
    assert_eq!(waited_paths(&pipeline), 1);

    // Every root shows more, the data folder's with them: the database's
    // disk shows none made.
    for root in StorageRoot::ALL {
        hears(&mut pipeline, &room, root, 4 * PLENTY);
    }
    hears(&mut pipeline, &room, StorageRoot::Complete, 8 * PLENTY);
    assert_move_held(&pipeline);

    database_room.store(4 * PLENTY, Ordering::Release);
    hears(&mut pipeline, &room, StorageRoot::Complete, 8 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// A moving row whose write fails for any other reason fails the job, as
// before.
#[tokio::test]
async fn a_moving_row_refused_for_another_reason_fails_the_job() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    refuse_moving_row(weaver_server_pipeline::StateError::Database(
        "the row is malformed".into(),
    ));
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert!(!disk_held(&pipeline, JOB));
    assert!(
        matches!(job_status(&pipeline, JOB), Some(JobStatus::Failed { .. })),
        "{:?}",
        job_status(&pipeline, JOB)
    );
    assert!(delivered(&complete_dir).is_empty());
}
