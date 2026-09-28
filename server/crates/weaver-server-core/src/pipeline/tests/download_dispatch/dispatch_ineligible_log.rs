use super::*;

/// A job dispatch will not serve is re-visited by every dispatch pass, and
/// passes come in bursts. Reporting it once per pass wrote hundreds of
/// identical lines a second — synchronously, on the pipeline actor thread —
/// for as long as it lasted. One line a job a window is the budget.
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
    // this is the shape the stall reports.
    let probe = pipeline.owned_download_lane_pool.probe_handle(job_id.0);
    let _held = probe.hold_dispatch_for_starved_probe();
    assert!(!pipeline.jobs[&job_id].download_queue.is_empty());

    pipeline.dispatch_downloads();
    let first = pipeline
        .dispatch_ineligible_log_throttle
        .last_emitted_at(job_id)
        .expect("the first pass of a window reports the stalled job");

    for _ in 0..64 {
        pipeline.dispatch_downloads();
    }

    assert_eq!(
        pipeline
            .dispatch_ineligible_log_throttle
            .last_emitted_at(job_id),
        Some(first),
        "every later pass inside the window is counted, not logged again"
    );
}

/// A job whose download is over has nothing for dispatch to hand out, whether
/// it is extracting, verifying or moving its output. Reporting each one as a
/// dispatch stall — with a dump of its recent debug lines — filled the log with
/// thousands of alarms about jobs that were working normally.
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

/// Recovery parked behind a phase that dispatches nothing is waiting for that
/// phase to end, not stalled; reporting it as a stall sent operators looking
/// for a fault in every job that moved its output with recovery still queued.
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

/// The probe rides the download lanes, so a job that is holding every lane
/// starves the batch that is trying to decide whether its release exists at
/// all. Withholding one handout from that job is what frees a lane for it.
#[tokio::test]
async fn a_job_starving_its_own_probe_stands_down_for_one_handout() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41602);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Probe Starved Job", &many_standalone_files("starved", 4)),
    )
    .await;

    assert_eq!(
        pipeline.download_scheduler_eligible_jobs(),
        vec![job_id],
        "precondition: the job has work and is dispatchable"
    );

    let probe = pipeline.owned_download_lane_pool.probe_handle(job_id.0);
    let held = probe.hold_dispatch_for_starved_probe();
    assert!(
        pipeline.download_scheduler_eligible_jobs().is_empty(),
        "the job is not offered the handout its own probe is waiting behind"
    );

    drop(held);
    assert_eq!(
        pipeline.download_scheduler_eligible_jobs(),
        vec![job_id],
        "and it is dispatchable again the moment the probe is answered"
    );
}

/// A settlement that is deferred for the same reason on every pass is worth
/// one line, not one line a pass. The reason is fingerprinted in job state, so
/// a changed reason speaks up again and a removed job starts over.
#[tokio::test]
async fn a_deferred_settlement_announces_itself_once_per_reason() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41603);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Deferred Settlement", &many_standalone_files("deferred", 2)),
    )
    .await;

    assert!(
        pipeline.note_demoted_materialization_block(job_id, 11),
        "the first pass says why the settlement is deferred"
    );
    assert!(
        !pipeline.note_demoted_materialization_block(job_id, 11),
        "an unchanged reason is not worth repeating"
    );
    assert!(
        pipeline.note_demoted_materialization_block(job_id, 12),
        "a changed reason is news again"
    );
    assert!(!pipeline.note_demoted_materialization_block(job_id, 12));

    pipeline.jobs.remove(&job_id);
    assert!(
        pipeline.note_demoted_materialization_block(job_id, 12),
        "a job that is no longer known carries no fingerprint to suppress by"
    );
}
