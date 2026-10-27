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

    // Articles queued, a status that dispatches, and still nothing handed out:
    // this is the shape the stall reports. Here the job holds no admission.
    pipeline.nzb_admitted.remove(&job_id);
    assert!(!pipeline.jobs[&job_id].download_queue.is_empty());

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
            state.download_queue.drain_all();
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
        let parked = state.download_queue.drain_all();
        for work in parked {
            state.recovery_queue.push(work);
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
