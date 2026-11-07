// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// A job dispatch will not serve is re-visited by every dispatch pass, and
// passes come in bursts. Reporting it once per pass wrote hundreds of
// identical lines a second — synchronously, on the pipeline actor thread —
// for as long as it lasted. One line a job a window is the budget.
#[tokio::test]
async fn an_ineligible_job_reports_at_most_once_a_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41601);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Withheld Job", &many_standalone_files("withheld", 4)),
    )
    .await;

    // Articles queued, a status that dispatches, a slot held, and still
    // nothing handed out: this is the shape the stall reports. Here the
    // job's row carries a pause the pipeline has not applied yet.
    pipeline.nzb_admitted.insert(job_id);
    pipeline.shared_state.queue.write(|inner| {
        inner.job_mut(job_id).unwrap().intents.paused = Some(true);
        crate::pipeline::queue::Change::changed(())
    });
    assert!(
        !pipeline.jobs[&job_id]
            .articles
            .active()
            .unwrap()
            .download_queue
            .is_empty()
    );

    // Every pass runs at an instant the test names, so the window is the
    // passes', not the machine's.
    let now = std::time::Instant::now();
    pipeline.dispatch_downloads_at(now);
    assert_eq!(
        pipeline
            .dispatch_ineligible_log_throttle
            .last_emitted_at(job_id),
        Some(now),
        "the first pass of a window reports the stalled job"
    );

    let window = crate::pipeline::download::DISPATCH_INELIGIBLE_LOG_INTERVAL;
    let last_inside = now + window - std::time::Duration::from_millis(1);
    for _ in 0..64 {
        pipeline.dispatch_downloads_at(now);
    }
    pipeline.dispatch_downloads_at(last_inside);
    assert_eq!(
        pipeline
            .dispatch_ineligible_log_throttle
            .last_emitted_at(job_id),
        Some(now),
        "every later pass inside the window is counted, not logged again"
    );

    pipeline.dispatch_downloads_at(now + window);
    assert_eq!(
        pipeline
            .dispatch_ineligible_log_throttle
            .last_emitted_at(job_id),
        Some(now + window),
        "the first pass of the next window reports it again"
    );
}

// Jobs waiting for a slot while no job holding one downloads are one
// condition: one line a window for all of them, and no window of their own.
// A line a job, at ten thousand queued jobs, was ten thousand warnings and
// debug dumps a minute.
#[tokio::test]
async fn jobs_waiting_for_a_slot_report_once_a_window_together() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let waiting: Vec<JobId> = (0..300).map(|offset| JobId(41700 + offset)).collect();
    for job_id in &waiting {
        insert_active_job(
            &mut pipeline,
            *job_id,
            standalone_job_spec("Waiting Job", &many_standalone_files("waiting", 1)),
        )
        .await;
        pipeline.nzb_admitted.remove(job_id);
    }

    let now = std::time::Instant::now();
    pipeline.dispatch_downloads_at(now);
    assert_eq!(
        pipeline.dispatch_slot_wait_log_throttle.last_emitted_at(()),
        Some(now)
    );
    for job_id in &waiting {
        assert_eq!(
            pipeline
                .dispatch_ineligible_log_throttle
                .last_emitted_at(*job_id),
            None
        );
    }
    let window = crate::pipeline::download::DISPATCH_INELIGIBLE_LOG_INTERVAL;
    pipeline.dispatch_downloads_at(now + window - std::time::Duration::from_millis(1));
    assert_eq!(
        pipeline.dispatch_slot_wait_log_throttle.last_emitted_at(()),
        Some(now)
    );
    pipeline.dispatch_downloads_at(now + window);
    assert_eq!(
        pipeline.dispatch_slot_wait_log_throttle.last_emitted_at(()),
        Some(now + window)
    );
}

// A job whose download is over has nothing for dispatch to hand out, whether
// it is extracting, verifying or moving its output. Reporting each one as a
// dispatch stall — with a dump of its recent debug lines — filled the log with
// thousands of alarms about jobs that were working normally.
#[tokio::test]
async fn a_job_past_its_download_is_not_a_stall() {
    for (offset, status) in [
        JobStatus::Downloading,
        JobStatus::Verifying,
        JobStatus::Repairing,
        JobStatus::QueuedExtract,
        JobStatus::Extracting,
        JobStatus::Moving,
        JobStatus::QueuedPostProcessing,
        JobStatus::PostProcessing,
    ]
    .into_iter()
    .enumerate()
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(41610 + offset as u64);
        insert_active_job(
            &mut pipeline,
            job_id,
            standalone_job_spec("Finished Download", &many_standalone_files("finished", 2)),
        )
        .await;
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state
                .articles
                .active_mut()
                .unwrap()
                .download_queue
                .drain_all();
            state.status = status.clone();
        }

        pipeline.dispatch_downloads();

        assert_eq!(
            pipeline
                .dispatch_ineligible_log_throttle
                .last_emitted_at(job_id),
            None,
            "{status:?} with nothing queued is not waiting on dispatch"
        );
    }
}

// Recovery parked behind a phase that dispatches nothing is waiting for that
// phase to end, not stalled; reporting it as a stall sent operators looking
// for a fault in every job that moved its output with recovery still queued.
#[tokio::test]
async fn parked_recovery_behind_a_moving_job_is_not_a_stall() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41604);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Moving Job", &many_standalone_files("moving", 2)),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let parked = state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .drain_all();
        for work in parked {
            state
                .articles
                .active_mut()
                .unwrap()
                .recovery_queue
                .push(work);
        }
        state.status = JobStatus::Moving;
    }

    pipeline.dispatch_downloads();

    assert_eq!(
        pipeline
            .dispatch_ineligible_log_throttle
            .last_emitted_at(job_id),
        None,
        "a moving job with parked recovery is idle by design"
    );
}

// A verifying job keeps its slot through verification, and its recovery
// articles stay parked until a seek round promotes them, so nothing of it is
// meant to dispatch. Counting the parked recovery as stalled work raised a
// slot-wait warning for every job queued behind it while it verified.
#[tokio::test]
async fn a_verifying_job_holding_its_slot_with_recovery_parked_is_not_a_stall() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let verifying = JobId(41620);
    let waiting = JobId(41621);
    insert_active_job(
        &mut pipeline,
        verifying,
        standalone_job_spec("Verifying Job", &many_standalone_files("verifying", 3)),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&verifying).unwrap();
        let active = state.articles.active_mut().unwrap();
        for work in active.download_queue.drain_all() {
            active.recovery_queue.push(work);
        }
        state.status = JobStatus::Verifying;
    }
    insert_active_job(
        &mut pipeline,
        waiting,
        standalone_job_spec("Waiting Job", &many_standalone_files("waiting", 1)),
    )
    .await;
    pipeline.nzb_admitted.remove(&waiting);
    assert!(pipeline.nzb_admitted.contains(&verifying));
    assert_eq!(pipeline.jobs[&verifying].articles.queued_download_len(), 0);
    assert!(pipeline.jobs[&verifying].articles.queued_recovery_len() > 0);

    let now = std::time::Instant::now();
    pipeline.dispatch_downloads_at(now);

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(
        pipeline.dispatch_slot_wait_log_throttle.last_emitted_at(()),
        None,
        "a slot held by a verifying job is held by design"
    );
    assert_eq!(
        pipeline
            .dispatch_ineligible_log_throttle
            .last_emitted_at(verifying),
        None
    );
    assert_eq!(pipeline.last_download_dispatch_stall_log_at, None);
}
