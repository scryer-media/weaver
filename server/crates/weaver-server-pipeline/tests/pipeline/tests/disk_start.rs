// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job starts only when what it still has to fetch fits on its disk, by a
// fresh good reading, less the reserve and everything promised there; those
// bytes are then promised to it until it stops. Each test sets the disks'
// readings itself and counts what a pass does, never how long it takes.

use weaver_server_composition::JobId;
use weaver_server_composition::operations::{StorageRoot, recover_server_state};
use weaver_server_pipeline::jobs::handle::{DISK_SPACE_WAIT_REASON, DISK_TOO_SMALL_WAIT_REASON};
use weaver_server_pipeline::pipeline::tests::{
    disk_start_fixture::*,
    history_fixture::database,
    new_direct_pipeline,
    nzb_admission_fixture::{enqueue_saved, prepare_add},
    nzb_zstd_for,
    queue_fixture::{apply_intents, cancel_batch, handle as queue_handle, report_job},
    queue_scale_fixture::{actor_awaited_db_calls, actor_thread_db_calls, admission_visits, sql},
    retire_pipeline_database, segmented_job_spec,
    window_fixture::settle,
};
use weaver_server_pipeline::{Pipeline, SchedulerHandle};

const RESERVE: u64 = GIB;
const BIG: u64 = 1024 * GIB;

// A pipeline with `slots` slots on one disk with `free` free.
async fn queue(
    temp: &tempfile::TempDir,
    slots: u32,
    free: u64,
) -> (Pipeline, SchedulerHandle, StartDisk) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let disk = StartDisk::under(&mut pipeline, RESERVE, free, BIG);
    let handle = slots_of(&mut pipeline, slots).await;
    (pipeline, handle, disk)
}

async fn slots_of(pipeline: &mut Pipeline, slots: u32) -> SchedulerHandle {
    let handle = queue_handle(pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(slots).unwrap())
        .await
        .unwrap();
    apply_intents(pipeline).await;
    handle
}

fn disk_space(need: u64, free: Option<u64>) -> (Option<String>, Option<u64>, Option<u64>) {
    (Some(DISK_SPACE_WAIT_REASON.to_string()), Some(need), free)
}

// The pause is taken and its slot let go, as two turns do.
async fn pause(pipeline: &mut Pipeline, handle: &SchedulerHandle, id: u64) {
    handle.pause_job(JobId(id)).await.unwrap();
    apply_intents(pipeline).await;
    apply_intents(pipeline).await;
}

// 1. A job that needs more than the disk leaves above the reserve takes no
// slot, says it waits for disk space with what it needs and what is free,
// and is promised nothing.
#[tokio::test]
async fn a_job_that_does_not_fit_takes_no_slot_and_shows_why() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, _disk) = queue(&temp, 2, RESERVE + 3 * GIB).await;
    add_sized(&mut pipeline, 1, 4).await;

    assert!(started(&pipeline).is_empty());
    assert_eq!(shown(&pipeline, 1), disk_space(4 * GIB, Some(3 * GIB)));
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 0);
    assert!(kept_for_disk(&pipeline));
}

// 2. A job larger than the whole disk above the reserve is told the disk is
// too small, with the most it holds, and a smaller job behind it starts.
#[tokio::test]
async fn a_job_the_disk_could_never_hold_lets_a_smaller_one_start() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let total = RESERVE + 5 * GIB;
    let _disk = StartDisk::under(&mut pipeline, RESERVE, total, total);
    slots_of(&mut pipeline, 2).await;
    add_sized(&mut pipeline, 1, 6).await;
    add_sized(&mut pipeline, 2, 2).await;

    assert_eq!(started(&pipeline), [2]);
    assert_eq!(
        shown(&pipeline, 1),
        (
            Some(DISK_TOO_SMALL_WAIT_REASON.to_string()),
            Some(6 * GIB),
            Some(5 * GIB)
        )
    );
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 2 * GIB);
}

// 3. Four 8 GiB jobs on 20 GiB free with 1 GiB kept: two start and two wait,
// shown with what is left. A reading with more room rings in and starts the
// third; the fourth waits on.
#[tokio::test]
async fn four_jobs_start_as_far_as_the_room_goes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, disk) = queue(&temp, 4, 20 * GIB).await;
    for id in 1..=4 {
        add_sized(&mut pipeline, id, 8).await;
    }
    assert_eq!(started(&pipeline), [1, 2]);
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 16 * GIB);
    for id in [3, 4] {
        assert_eq!(shown(&pipeline, id), disk_space(8 * GIB, Some(3 * GIB)));
    }

    disk.reads(&mut pipeline, 28 * GIB);
    apply_intents(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1, 2, 3]);
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 24 * GIB);
    assert_eq!(shown(&pipeline, 4), disk_space(8 * GIB, Some(3 * GIB)));
}

// 4. A big job that does not fit is passed by each smaller one that does,
// again and again, and starts once a reading shows its room.
#[tokio::test]
async fn smaller_jobs_pass_a_big_one_until_it_fits() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle, disk) = queue(&temp, 5, RESERVE + 7 * GIB).await;
    add_sized(&mut pipeline, 1, 10).await;
    for id in 2..=4 {
        add_sized(&mut pipeline, id, 2).await;
    }
    assert_eq!(started(&pipeline), [2, 3, 4]);
    assert_eq!(shown(&pipeline, 1), disk_space(10 * GIB, Some(GIB)));

    // A slot and its room come free: the next small job takes them, the
    // big one is passed again.
    add_sized(&mut pipeline, 5, 2).await;
    assert_eq!(started(&pipeline), [2, 3, 4]);
    pause(&mut pipeline, &handle, 2).await;
    assert_eq!(started(&pipeline), [3, 4, 5]);
    assert_eq!(shown(&pipeline, 1), disk_space(10 * GIB, Some(GIB)));

    disk.reads(&mut pipeline, RESERVE + 20 * GIB);
    apply_intents(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1, 3, 4, 5]);
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 16 * GIB);
}

// 5. A pause gives back the promise with the slot, and a resume is judged
// again: on a disk that now has less room, it waits.
#[tokio::test]
async fn a_pause_gives_back_its_promise_and_a_resume_is_judged_again() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle, disk) = queue(&temp, 2, RESERVE + 5 * GIB).await;
    add_sized(&mut pipeline, 1, 4).await;
    assert_eq!(started(&pipeline), [1]);
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 4 * GIB);

    pause(&mut pipeline, &handle, 1).await;
    assert!(started(&pipeline).is_empty());
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 0);

    disk.reads(&mut pipeline, RESERVE + 3 * GIB);
    handle.resume_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(started(&pipeline).is_empty(), "short, it waits");
    assert_eq!(shown(&pipeline, 1), disk_space(4 * GIB, Some(3 * GIB)));

    disk.reads(&mut pipeline, RESERVE + 5 * GIB);
    apply_intents(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1]);
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 4 * GIB);
}

// 6. After a restart, the jobs that held a slot are promised what they still
// have to fetch before any other job is judged; while one of them has no
// row yet, no other job starts.
#[tokio::test]
async fn a_restart_promises_the_held_slots_before_any_other_start() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, _disk) = queue(&temp, 3, BIG).await;
    enqueue_saved(&mut pipeline, 1).await;
    enqueue_saved(&mut pipeline, 2).await;
    assert_eq!(started(&pipeline), [1, 2]);
    let need = remaining(&pipeline, 1).unwrap();
    assert!(need > 0);
    retire_pipeline_database(pipeline).await;

    // Room for the two held slots and one byte short of a third job.
    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let _disk = StartDisk::under(&mut pipeline, RESERVE, RESERVE + 3 * need - 1, BIG);
    let handle = slots_of(&mut pipeline, 3).await;
    // A new job before the held slots' rows are read does not start.
    enqueue_saved(&mut pipeline, 3).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1, 2]);
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 0);

    let recovered = recover_server_state(
        database(&pipeline),
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    // The new job is in the queue already; the held slots' rows come back.
    for candidate in recovered.to_restore {
        if candidate.request.job_id != JobId(3) {
            handle.restore_job(candidate.request).await.unwrap();
        }
    }
    settle(&mut pipeline).await;
    // The held slots are promised first, and the new job is judged on what
    // they leave.
    assert_eq!(started(&pipeline), [1, 2]);
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 2 * need);
    assert_eq!(shown(&pipeline, 3), disk_space(need, Some(need - 1)));
}

// 7. A failed probe leaves only a stale reading, and starts wait for a good
// one; the readings in between bring no walk. The next good reading lets
// the job start through the ring. A reading older than twice the interval
// holds a start too.
#[tokio::test]
async fn a_stale_or_old_reading_holds_a_start_until_a_good_one() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, disk) = queue(&temp, 3, RESERVE + 10 * GIB).await;
    disk.fails(&mut pipeline);
    add_sized(&mut pipeline, 1, 2).await;
    assert!(started(&pipeline).is_empty());
    assert_eq!(shown(&pipeline, 1), disk_space(2 * GIB, None));

    let walked = walks();
    disk.fails(&mut pipeline);
    apply_intents(&mut pipeline).await;
    assert_eq!(walks(), walked, "a stale reading brings no walk");
    assert!(started(&pipeline).is_empty());

    disk.reads(&mut pipeline, RESERVE + 10 * GIB);
    apply_intents(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1]);
    assert!(!kept_for_disk(&pipeline));

    // The good reading grows older than twice the interval: the next job
    // waits for a fresh one.
    disk.ages(&pipeline, 2 * INTERVAL + std::time::Duration::from_secs(1));
    add_sized(&mut pipeline, 2, 2).await;
    assert_eq!(started(&pipeline), [1]);
    assert_eq!(shown(&pipeline, 2), disk_space(2 * GIB, None));
}

// 8. At first start-up the disks are not read until the start-up gate
// opens: a job waits for the first reading, and starts once it comes.
#[tokio::test]
async fn a_job_waits_for_the_first_reading_behind_the_start_up_gate() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let disk = StartDisk::gated(&mut pipeline, RESERVE, RESERVE + 10 * GIB, BIG);
    slots_of(&mut pipeline, 2).await;
    add_sized(&mut pipeline, 1, 2).await;
    assert!(started(&pipeline).is_empty());
    assert_eq!(shown(&pipeline, 1), disk_space(2 * GIB, None));

    disk.open(&mut pipeline);
    apply_intents(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1]);
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 2 * GIB);
}

// 12. A queue of ten thousand rows with every slot taken: each walk visits
// one candidate at most, as the walk before the disk rule did, asks the disk
// nothing itself and reads nothing from the database.
#[tokio::test]
async fn a_long_queue_walks_as_before_and_reads_nothing() {
    const LONG: u64 = 10_000;
    const SLOTS: u32 = 4;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle, _disk) = queue(&temp, SLOTS, BIG).await;
    // The rows go into the queue first and are acted on in one turn, so the
    // seed costs one walk, not one per row.
    for id in 1..=LONG {
        let spec = segmented_job_spec(&format!("Queued NZB {id}"), "payload.bin", &[512; 4]);
        let nzb = nzb_zstd_for(&spec);
        prepare_add(
            &pipeline,
            JobId(id),
            spec,
            format!("queued-{id}.nzb").into(),
            nzb,
        )
        .await;
    }
    apply_intents(&mut pipeline).await;
    assert_eq!(started(&pipeline).len(), SLOTS as usize);

    let looks =
        weaver_server_disk::operations::disk::DISK_LOOKS.load(std::sync::atomic::Ordering::Relaxed);
    let (walked, visited) = (walks(), admission_visits());
    let (awaited, called) = (actor_awaited_db_calls(), actor_thread_db_calls());
    let moves: Vec<_> = (0..100)
        .map(|step| {
            (
                JobId(LONG - step),
                weaver_server_composition::QueueMoveTarget::Offset(-3),
            )
        })
        .collect();
    handle.reorder_jobs(moves).await.unwrap();
    apply_intents(&mut pipeline).await;
    let walked = walks() - walked;
    assert!(walked > 0);
    assert!(
        admission_visits() - visited <= walked,
        "no walk looked at more than one candidate, the slots being full"
    );
    assert_eq!(
        weaver_server_disk::operations::disk::DISK_LOOKS.load(std::sync::atomic::Ordering::Relaxed),
        looks
    );
    assert_eq!(actor_awaited_db_calls(), awaited);
    assert_eq!(actor_thread_db_calls(), called);
}

// Rows `ids` of `gib` GiB each go into the queue and are acted on in one
// turn, so the seed costs one walk, not one per row.
async fn seed(pipeline: &mut Pipeline, ids: std::ops::RangeInclusive<u64>, gib: u64) {
    for id in ids {
        let spec = sized_spec(id, gib);
        let nzb = nzb_zstd_for(&spec);
        prepare_add(
            pipeline,
            JobId(id),
            spec,
            format!("sized-{id}.nzb").into(),
            nzb,
        )
        .await;
    }
    apply_intents(pipeline).await;
}

// 4 (review). On a disk smaller than any of ten thousand queued rows, a walk
// refuses each row by the disk's size, read once, and neither looks at the
// slots nor takes the ledger's booking lock for any of them.
#[tokio::test]
async fn rows_too_large_for_the_disk_take_no_slot_look_and_no_booking_lock() {
    const LONG: u64 = 10_000;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let _disk = StartDisk::under(&mut pipeline, RESERVE, RESERVE + 2 * GIB, RESERVE + 2 * GIB);
    let handle = slots_of(&mut pipeline, 2).await;
    seed(&mut pipeline, 1..=LONG, 4).await;
    assert!(started(&pipeline).is_empty());
    assert_eq!(
        shown(&pipeline, 1),
        (
            Some(DISK_TOO_SMALL_WAIT_REASON.to_string()),
            Some(4 * GIB),
            Some(2 * GIB)
        )
    );

    let (walked, locked, visited) = (walks(), booking_locks(), admission_visits());
    let moves: Vec<_> = (0..100)
        .map(|step| {
            (
                JobId(LONG - step),
                weaver_server_composition::QueueMoveTarget::Offset(-3),
            )
        })
        .collect();
    handle.reorder_jobs(moves).await.unwrap();
    apply_intents(&mut pipeline).await;
    let (walked, locked, visited) = (
        walks() - walked,
        booking_locks() - locked,
        admission_visits() - visited,
    );
    assert!(walked > 0);
    assert!(
        locked <= walked,
        "{walked} walks over {LONG} rows too large took the booking lock {locked} times"
    );
    assert_eq!(
        visited, 0,
        "{walked} walks over {LONG} rows too large looked at the slots {visited} times"
    );
}

const SLAB: u64 = 64 << 20;

// 2 (review). Ten thousand rows wait behind one kept for the disk while a
// started job lands slab after slab of what it was promised. Each landing
// moves the promise to landed and frees nothing, and the readings after
// them show the same room: no walk runs, and no reading is asked for, however
// many slabs land.
#[tokio::test]
async fn landings_of_a_started_job_bring_no_walk_and_no_probe() {
    const LONG: u64 = 10_000;
    const SLABS: u64 = 32;
    let free = RESERVE + 5 * GIB;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, disk) = queue(&temp, 2, free).await;
    seed(&mut pipeline, 1..=LONG, 4).await;
    assert_eq!(started(&pipeline), vec![1]);
    assert!(kept_for_disk(&pipeline));
    apply_intents(&mut pipeline).await;

    let (walked, asked) = (walks(), refresh_asks());
    for slab in 1..=SLABS {
        lands(&pipeline, SLAB);
        disk.reads(&mut pipeline, free - slab * SLAB);
        apply_intents(&mut pipeline).await;
    }
    let (walked, asked) = (walks() - walked, refresh_asks() - asked);
    assert!(
        walked <= 1 && asked <= 1,
        "{SLABS} slabs landed: {walked} walks, {asked} readings asked for"
    );
    assert_eq!(started(&pipeline), vec![1]);
}

// 14. With the complete folder on a filesystem of its own, a start needs
// room on both and is promised on both; on one filesystem it is promised
// once.
#[tokio::test]
async fn a_start_is_promised_on_each_filesystem_once() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let disk = StartDisk::split(
        &mut pipeline,
        RESERVE,
        (RESERVE + 10 * GIB, RESERVE + 3 * GIB),
        BIG,
    );
    slots_of(&mut pipeline, 2).await;
    add_sized(&mut pipeline, 1, 4).await;
    assert!(started(&pipeline).is_empty(), "the complete disk is short");
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 0);
    assert_eq!(promised_on(&pipeline, StorageRoot::Complete), 0);

    disk.complete_reads(&mut pipeline, RESERVE + 10 * GIB);
    apply_intents(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1]);
    assert_eq!(promised_here(&pipeline, StorageRoot::Intermediate), 4 * GIB);
    assert_eq!(promised_here(&pipeline, StorageRoot::Complete), 4 * GIB);

    let one = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, _disk) = queue(&one, 2, RESERVE + 10 * GIB).await;
    add_sized(&mut pipeline, 1, 4).await;
    assert_eq!(started(&pipeline), [1]);
    assert_eq!(promised_here(&pipeline, StorageRoot::Intermediate), 4 * GIB);
    assert_eq!(promised_here(&pipeline, StorageRoot::Complete), 0);
    assert_eq!(promised_on(&pipeline, StorageRoot::Complete), 4 * GIB);
}

// What each root's own count holds promised.
fn promised_by_root(pipeline: &Pipeline) -> [u64; 3] {
    StorageRoot::ALL.map(|root| promised_here(pipeline, root))
}

// 1 (review). On one filesystem and with the complete folder on its own,
// a job paused and a job cancelled leave nothing promised on any root.
#[tokio::test]
async fn a_paused_or_cancelled_start_leaves_nothing_promised_on_any_root() {
    for split in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let _disk = if split {
            StartDisk::split(
                &mut pipeline,
                RESERVE,
                (RESERVE + 10 * GIB, RESERVE + 10 * GIB),
                BIG,
            )
        } else {
            StartDisk::under(&mut pipeline, RESERVE, RESERVE + 10 * GIB, BIG)
        };
        let handle = slots_of(&mut pipeline, 2).await;
        add_sized(&mut pipeline, 1, 2).await;
        add_sized(&mut pipeline, 2, 3).await;
        assert_eq!(started(&pipeline), [1, 2], "split {split}");
        let output = if split { 5 * GIB } else { 0 };
        assert_eq!(
            promised_by_root(&pipeline),
            [0, 5 * GIB, output],
            "split {split}: both started"
        );

        pause(&mut pipeline, &handle, 1).await;
        let outcome = cancel_batch(&mut pipeline, &[JobId(2)]).await;
        assert!(
            outcome.iter().all(|(_, result)| result.is_ok()),
            "{outcome:?}"
        );
        // The slot it held is let go on the turn after, as a pause's is.
        apply_intents(&mut pipeline).await;
        assert!(
            started(&pipeline).is_empty(),
            "split {split}: {:?} still started, by root {:?}",
            started(&pipeline),
            promised_by_root(&pipeline)
        );
        assert_eq!(
            promised_by_root(&pipeline),
            [0, 0, 0],
            "split {split}: nothing stays promised"
        );
    }
}

// 7e (review). Jobs that held a slot, stored with no outline, restart with
// rows that say nothing of their files until their NZB is read: the bytes
// they still have to fetch are promised once their rows say them, and no
// other job takes that room first.
#[tokio::test]
async fn held_slots_restored_with_no_outline_are_promised_what_they_have_left() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, _disk) = queue(&temp, 3, BIG).await;
    enqueue_saved(&mut pipeline, 1).await;
    enqueue_saved(&mut pipeline, 2).await;
    assert_eq!(started(&pipeline), [1, 2]);
    let need = remaining(&pipeline, 1).unwrap();
    assert!(need > 0);
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    sql(database(&pipeline), "DELETE FROM active_job_outlines").await;
    sql(
        database(&pipeline),
        "UPDATE active_jobs SET nzb_title = NULL, nzb_password = NULL, nzb_files = NULL",
    )
    .await;
    let _disk = StartDisk::under(&mut pipeline, RESERVE, RESERVE + 3 * need - 1, BIG);
    let handle = slots_of(&mut pipeline, 3).await;
    enqueue_saved(&mut pipeline, 3).await;
    apply_intents(&mut pipeline).await;
    let recovered = recover_server_state(
        database(&pipeline),
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        if candidate.request.job_id != JobId(3) {
            handle.restore_job(candidate.request).await.unwrap();
        }
    }
    // Each turn is looked at: no held slot is promised less than it has
    // left, and the new job never starts on the room they hold.
    for _ in 0..8 {
        apply_intents(&mut pipeline).await;
        assert!(!started(&pipeline).contains(&3), "the new job waits");
        let promised = promised_on(&pipeline, StorageRoot::Intermediate);
        assert!(
            promised == 0 || promised == 2 * need,
            "{promised} promised for two held slots of {need} each"
        );
    }
    settle(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1, 2]);
    assert_eq!(promised_on(&pipeline, StorageRoot::Intermediate), 2 * need);
    assert_eq!(shown(&pipeline, 3), disk_space(need, Some(need - 1)));
}

// A job that held its slot across a restart, partly fetched, with the
// complete folder on a filesystem of its own, is promised there only what
// it still has to fetch: what it fetched before is not promised again.
#[tokio::test]
async fn a_restored_slot_is_promised_on_the_complete_disk_only_what_it_has_left() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, _disk) = queue(&temp, 3, BIG).await;
    enqueue_saved(&mut pipeline, 1).await;
    assert_eq!(started(&pipeline), [1]);
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let _disk = StartDisk::split(&mut pipeline, RESERVE, (BIG, BIG), BIG);
    let handle = slots_of(&mut pipeline, 3).await;
    let recovered = recover_server_state(
        database(&pipeline),
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    settle(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1]);
    let left = remaining(&pipeline, 1).unwrap();
    eprintln!(
        "DBG left {left} graph {} working {} complete {}",
        has_graph(&pipeline, 1),
        promised_here(&pipeline, StorageRoot::Intermediate),
        promised_here(&pipeline, StorageRoot::Complete)
    );
    assert!(left > 0);
    assert_eq!(promised_here(&pipeline, StorageRoot::Intermediate), left);
    assert_eq!(
        promised_here(&pipeline, StorageRoot::Complete),
        left,
        "the complete disk is promised what is left, not the whole job"
    );
}

// With the complete folder on a filesystem of its own, a job partly fetched
// (2 GiB left of 8) that the complete disk cannot take shows the figures of
// that disk: all it delivers there, and the room there. Readings of the same
// disks bring no walk.
#[tokio::test]
async fn a_start_refused_by_the_complete_disk_shows_its_figures_and_waits_quietly() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let disk = StartDisk::split(
        &mut pipeline,
        RESERVE,
        (RESERVE + 100 * GIB, RESERVE + 3 * GIB),
        BIG,
    );
    slots_of(&mut pipeline, 2).await;
    let spec = sized_spec(1, 8);
    let nzb = nzb_zstd_for(&spec);
    prepare_add(&pipeline, JobId(1), spec, "sized-1.nzb".into(), nzb).await;
    says_left(&pipeline, 1, 2 * GIB);
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert!(started(&pipeline).is_empty(), "the complete disk is short");
    assert_eq!(shown(&pipeline, 1), disk_space(8 * GIB, Some(3 * GIB)));

    let walked = walks();
    for _ in 0..5 {
        disk.complete_reads(&mut pipeline, RESERVE + 3 * GIB);
        apply_intents(&mut pipeline).await;
    }
    assert_eq!(walks(), walked, "unchanged readings bring no walk");
    assert_eq!(shown(&pipeline, 1), disk_space(8 * GIB, Some(3 * GIB)));

    disk.complete_reads(&mut pipeline, RESERVE + 8 * GIB);
    apply_intents(&mut pipeline).await;
    assert_eq!(
        started(&pipeline),
        [1],
        "room on the complete disk lets it start"
    );
}

// A job waiting to start for its disk keeps its figures across a report of
// its row.
#[tokio::test]
async fn a_report_of_a_job_kept_for_the_disk_keeps_its_figures() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, _disk) = queue(&temp, 2, RESERVE + 3 * GIB).await;
    add_sized(&mut pipeline, 1, 4).await;
    let before = shown(&pipeline, 1);
    assert_eq!(before, disk_space(4 * GIB, Some(3 * GIB)));
    report_job(&mut pipeline, JobId(1));
    apply_intents(&mut pipeline).await;
    assert_eq!(
        shown(&pipeline, 1),
        before,
        "the figures stay with a report"
    );
}

// Room promised to a torrent let go from a full disk is not promised again
// to a usenet start, and the job waiting for it says how much is free.
#[tokio::test]
async fn a_start_is_not_promised_room_a_torrent_was_let_go_on() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle, disk) = queue(&temp, 2, RESERVE + 5 * GIB).await;
    torrent_let_go_on_working(&pipeline, 4 * GIB);
    add_sized(&mut pipeline, 1, 4).await;
    assert!(
        started(&pipeline).is_empty(),
        "4 GiB of 5 are the torrent's"
    );
    assert_eq!(shown(&pipeline, 1), disk_space(4 * GIB, Some(GIB)));

    // The torrent's step ends; the next reading lets the job start.
    torrent_step_ends(&pipeline);
    disk.reads(&mut pipeline, RESERVE + 5 * GIB);
    apply_intents(&mut pipeline).await;
    assert_eq!(started(&pipeline), [1]);
}

// With one slot and a job the disk can never hold at the head, a job behind
// it kept for disk space says so, not that it waits for a slot.
#[tokio::test]
async fn a_job_too_large_for_the_disk_does_not_hide_why_the_next_waits() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let _disk = StartDisk::under(&mut pipeline, RESERVE, RESERVE + 3 * GIB, RESERVE + 5 * GIB);
    slots_of(&mut pipeline, 1).await;
    add_sized(&mut pipeline, 1, 6).await;
    add_sized(&mut pipeline, 2, 4).await;
    assert!(started(&pipeline).is_empty());
    assert_eq!(
        shown(&pipeline, 1),
        (
            Some(DISK_TOO_SMALL_WAIT_REASON.to_string()),
            Some(6 * GIB),
            Some(5 * GIB)
        )
    );
    assert_eq!(shown(&pipeline, 2), disk_space(4 * GIB, Some(3 * GIB)));
}
