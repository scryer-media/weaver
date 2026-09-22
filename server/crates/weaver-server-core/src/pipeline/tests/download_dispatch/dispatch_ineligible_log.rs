use super::*;

/// A job in a phase that dispatches nothing is re-visited by every dispatch
/// pass, and passes come in bursts. Reporting it once per pass wrote hundreds
/// of identical lines a second — synchronously, on the pipeline actor thread —
/// for as long as the phase lasted. One line a job a window is the budget.
#[tokio::test]
async fn an_ineligible_job_reports_at_most_once_a_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41601);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Extracting Job", &many_standalone_files("extracting", 2)),
    )
    .await;

    // Extracting with both queues empty: nothing to dispatch, and none of the
    // explained idle shapes either, so this is the line the stall reports.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue.drain_all();
        state.status = JobStatus::Extracting;
    }

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
