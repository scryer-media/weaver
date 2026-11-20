// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::{DownloadState, JobStatus, PostState, RunState};

// Two staging removals out for one id: the first to end leaves it held.
#[test]
fn staging_removals_are_counted() {
    let (mut pending, id) = (PendingCleanup::default(), JobId(1));
    pending.hold(id, CleanupHolder::Staging);
    pending.hold(id, CleanupHolder::Staging);
    pending.hold(id, CleanupHolder::Folders);
    assert!(!pending.done(id, CleanupHolder::Folders));
    assert!(!pending.done(id, CleanupHolder::Staging));
    assert!(pending.contains(&id));
    assert!(pending.done(id, CleanupHolder::Staging));
    assert!(!pending.contains(&id));
}

fn downloading(id: u64) -> JobInfo {
    JobInfo {
        protocol: crate::jobs::Protocol::Nzb,
        job_id: JobId(id),
        job_hash: None,
        name: format!("job-{id}"),
        status: JobStatus::Downloading,
        download_state: DownloadState::Downloading,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state: PostState::Idle,
        run_state: RunState::Active,
        progress: 0.0,
        total_bytes: 100,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: 1,
        completed_files: 0,
        remaining_par_files: 0,
        file_remaining_bytes: Vec::new(),
        file_parked_bytes: Vec::new(),
        remaining_bytes: 0,
        wanted_bytes: 0,
        password: None,
        category: None,
        metadata: Vec::new(),
        output_dir: None,
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        server_attribution: Vec::new(),
        disk_need_bytes: None,
        disk_free_bytes: None,
        created_at_epoch_ms: 0.0,
    }
}

// A row the pipeline last reported with a lane holding it.
fn active_row(id: u64) -> JobRow {
    let mut row = JobRow::new(downloading(id));
    row.report.download_active = true;
    row.report.download_work_pending = true;
    row
}

#[test]
fn queue_global_pause_reports_every_held_download_as_paused() {
    let mut globals = Globals::default();
    globals
        .holds
        .hold(crate::jobs::Hold::manual(crate::jobs::HoldOwner::All));
    for row in [active_row(1), JobRow::new(downloading(2))] {
        let job = row.info(&globals);
        assert_eq!(job.run_state, RunState::Paused);
        assert_eq!(job.download_state, DownloadState::Queued);
    }

    let mut speed_test = Globals::default();
    speed_test.holds.hold(crate::jobs::Hold {
        origin: crate::jobs::HoldOrigin::SpeedTest,
        scope: crate::jobs::HoldScope::Everything,
        owner: crate::jobs::HoldOwner::All,
    });
    assert_eq!(active_row(3).info(&speed_test).run_state, RunState::Paused);

    // Post-processing is not a download the global pause stops.
    let mut extracting = downloading(4);
    extracting.status = JobStatus::Extracting;
    extracting.download_state = DownloadState::Complete;
    extracting.post_state = PostState::Extracting;
    assert_eq!(
        JobRow::new(extracting).info(&globals).run_state,
        RunState::Active
    );
    assert_eq!(
        active_row(5).info(&Globals::default()).run_state,
        RunState::Active
    );
}

#[test]
fn queue_job_holding_no_lane_while_work_is_owed_reads_queued() {
    let mut row = active_row(1);
    row.report.download_active = false;
    let job = row.info(&Globals::default());
    assert_eq!(job.download_state, DownloadState::Queued);
    assert_eq!(job.run_state, RunState::Active);

    // A lane holds it: Downloading.
    assert_eq!(
        active_row(2).info(&Globals::default()).download_state,
        DownloadState::Downloading
    );

    // Payload in and the pipeline draining: not a wait for lanes.
    let mut finalizing = active_row(3);
    finalizing.report.download_active = false;
    finalizing.report.job.finalizing_download = true;
    assert_eq!(
        finalizing.info(&Globals::default()).download_state,
        DownloadState::Downloading
    );
}

#[test]
fn queue_job_waiting_for_lanes_keeps_the_dispatcher_wait_reason() {
    let mut row = active_row(1);
    row.report.download_active = false;
    row.report.job.download_wait_reason = Some("checkpoint".into());
    let job = row.info(&Globals::default());
    assert_eq!(job.download_state, DownloadState::Queued);
    assert_eq!(job.download_wait_reason.as_deref(), Some("checkpoint"));
}

#[test]
fn queue_pause_then_resume_report_the_intent_before_the_pipeline_applies_it() {
    let queue = Queue::new(vec![downloading(1)]);
    queue.write(|inner| {
        inner.jobs[0].report.download_active = true;
        inner.jobs[0].report.download_work_pending = true;
        Change::changed(())
    });
    let view = |queue: &Queue| queue.read(|inner| inner.jobs[0].info(&inner.globals));

    queue.set_job_paused(JobId(1), true).unwrap();
    let paused = view(&queue);
    assert_eq!(paused.status, JobStatus::Paused);
    assert_eq!(paused.run_state, RunState::Paused);
    assert_eq!(paused.download_state, DownloadState::Queued);

    // The pipeline applies the pause and reports it.
    queue.write(|inner| {
        let mut row = inner.row_mut_at(0);
        row.report.seen_intent_seq = row.intents.seq;
        row.report.job.status = JobStatus::Paused;
        row.report.job.download_state = DownloadState::Queued;
        row.report.job.run_state = RunState::Paused;
        row.report.download_active = false;
        Change::changed(())
    });
    assert_eq!(view(&queue).run_state, RunState::Paused);

    queue.set_job_paused(JobId(1), false).unwrap();
    let resumed = view(&queue);
    assert_eq!(resumed.status, JobStatus::Queued);
    assert_eq!(resumed.run_state, RunState::Active);
    assert_eq!(resumed.download_state, DownloadState::Queued);

    // A resume under the global pause is still held by it.
    let held = queue.read(|inner| {
        let mut globals = inner.globals.clone();
        globals
            .holds
            .hold(crate::jobs::Hold::manual(crate::jobs::HoldOwner::All));
        inner.jobs[0].info(&globals)
    });
    assert_eq!(held.run_state, RunState::Paused);
}

// The count the queue keeps, checked against a walk of the rows as the
// system query used to make it.
fn waiting_count(queue: &Queue) -> usize {
    queue.read(|inner| {
        let walked = inner
            .jobs
            .iter()
            .filter(|row| {
                matches!(
                    row.view(&inner.globals).status(),
                    JobStatus::Queued | JobStatus::Downloading | JobStatus::Checking
                )
            })
            .count();
        assert_eq!(inner.waiting, walked);
        assert_eq!(inner.work_waiting(), walked > 0);
        walked
    })
}

fn with_status(id: u64, status: JobStatus) -> JobInfo {
    JobInfo {
        status,
        ..downloading(id)
    }
}

// What the pipeline does when it has applied a row's intents.
fn apply_intents(queue: &Queue, id: u64, status: JobStatus) {
    queue.write(|inner| {
        let mut row = inner.job_mut(JobId(id)).unwrap();
        row.report.seen_intent_seq = row.intents.seq;
        row.report.job.status = status;
        Change::changed(())
    });
}

#[test]
fn queue_count_of_jobs_with_work_waiting_matches_a_walk_through_every_change() {
    // Built from rows: one of each waiting status, and two that do not wait.
    let queue = Queue::new(vec![
        with_status(1, JobStatus::Queued),
        with_status(2, JobStatus::Downloading),
        with_status(3, JobStatus::Checking),
        with_status(4, JobStatus::Extracting),
        with_status(5, JobStatus::Paused),
    ]);
    assert_eq!(waiting_count(&queue), 3);

    // A row added, waiting and not.
    queue.write(|inner| {
        inner.push_job(JobRow::new(with_status(6, JobStatus::Queued)));
        inner.push_job(JobRow::new(with_status(7, JobStatus::Complete)));
        Change::changed(())
    });
    assert_eq!(waiting_count(&queue), 4);

    // A pause asked for counts before the pipeline applies it, and the
    // same after it does.
    queue.set_job_paused(JobId(1), true).unwrap();
    assert_eq!(waiting_count(&queue), 3);
    apply_intents(&queue, 1, JobStatus::Paused);
    assert_eq!(waiting_count(&queue), 3);

    // A resume asked for counts before the pipeline applies it too.
    queue.set_job_paused(JobId(1), false).unwrap();
    assert_eq!(waiting_count(&queue), 4);
    apply_intents(&queue, 1, JobStatus::Queued);
    assert_eq!(waiting_count(&queue), 4);

    // A pause the pipeline refuses is taken back.
    queue.set_job_paused(JobId(2), true).unwrap();
    assert_eq!(waiting_count(&queue), 3);
    queue.write(|inner| {
        let mut row = inner.job_mut(JobId(2)).unwrap();
        row.report.seen_intent_seq = row.intents.seq;
        row.intents.paused = None;
        Change::changed(())
    });
    assert_eq!(waiting_count(&queue), 4);

    // A batch pause, part of it already paused.
    for outcome in queue.set_jobs_paused([JobId(3), JobId(5)], true).values() {
        outcome.as_ref().unwrap();
    }
    assert_eq!(waiting_count(&queue), 3);

    // The pipeline reports a job on, out of the waiting statuses and back.
    queue.report_jobs(vec![with_status(6, JobStatus::Extracting)]);
    assert_eq!(waiting_count(&queue), 2);
    queue.report_jobs(vec![with_status(4, JobStatus::Downloading)]);
    assert_eq!(waiting_count(&queue), 3);

    // The whole-queue pause moves no row's status.
    queue.write(|inner| {
        inner
            .globals
            .holds
            .hold(crate::jobs::Hold::manual(crate::jobs::HoldOwner::All));
        Change::changed(())
    });
    assert_eq!(waiting_count(&queue), 3);
    queue.write(|inner| {
        inner.globals.holds.hold(crate::jobs::Hold {
            origin: crate::jobs::HoldOrigin::SpeedTest,
            scope: crate::jobs::HoldScope::Everything,
            owner: crate::jobs::HoldOwner::All,
        });
        inner
            .globals
            .holds
            .release(crate::jobs::HoldOrigin::Manual, crate::jobs::HoldOwner::All);
        Change::changed(())
    });
    assert_eq!(waiting_count(&queue), 3);

    // A reorder keeps every row.
    queue
        .reorder_jobs(vec![(JobId(4), crate::jobs::handle::QueueMoveTarget::Top)])
        .unwrap();
    assert_eq!(waiting_count(&queue), 3);

    // A job finishing is written to its row, then the row leaves.
    queue.write(|inner| {
        let position = inner.job_position(JobId(4)).unwrap();
        let mut row = inner.row_mut_at(position);
        row.report.job = with_status(4, JobStatus::Complete);
        row.report.retiring = true;
        Change::changed(())
    });
    assert_eq!(waiting_count(&queue), 2);
    queue.write(|inner| {
        inner.remove_job(JobId(4));
        inner.remove_job(JobId(2));
        Change::changed(())
    });
    assert_eq!(waiting_count(&queue), 1);

    // Rows dropped in bulk.
    queue.write(|inner| {
        inner.retain_jobs(|row| row.id != JobId(1));
        Change::changed(())
    });
    assert_eq!(waiting_count(&queue), 0);
    queue.write(|inner| {
        inner.retain_jobs(|_| false);
        Change::changed(())
    });
    assert_eq!(waiting_count(&queue), 0);
}
