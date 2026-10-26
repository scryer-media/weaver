// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

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
