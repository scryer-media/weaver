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
