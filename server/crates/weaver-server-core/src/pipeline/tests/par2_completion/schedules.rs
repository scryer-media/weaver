use super::*;

#[tokio::test]
async fn analysis_receipt_invalidation_schedules_never_retain_an_old_verdict() {
    for teardown in [false, true] {
        for apply_first in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
            let job = JobId(30971);
            insert_damaged_job_ready_for_analysis(&mut pipeline, job, "analysis-schedules").await;
            pipeline.check_job_completion(job).await;
            assert!(pipeline.par2_analysis_in_flight.contains_key(&job));
            // Hold the actual result. Worker scheduling cannot choose which
            // side of invalidation its receipt lands on.
            let done = next_par2_analysis_done(&mut pipeline).await;
            let mut done = Some(done);
            for apply in [apply_first, !apply_first] {
                if apply {
                    pipeline
                        .handle_par2_analysis_done(done.take().unwrap())
                        .await;
                } else if teardown {
                    pipeline.clear_par2_runtime_state(job);
                } else {
                    pipeline.invalidate_par2_session_for_identity_rebind(job);
                }
            }
            assert!(
                !pipeline.par2_analysis_results.contains_key(&job),
                "teardown={teardown} apply_first={apply_first}"
            );
            assert!(!pipeline.par2_analysis_in_flight.contains_key(&job));
            assert!(!pipeline.par2_verified.contains(&job));
            if !teardown {
                pipeline.check_job_completion(job).await;
                assert_eq!(pipeline.par2_repairer_analyze_calls, 2);
                settle_par2_analysis_work(&mut pipeline).await;
                pump_pipeline_runtime_queues(&mut pipeline).await;
                assert_eq!(
                    job_status_for_assert(&pipeline, job),
                    Some(JobStatus::Complete)
                );
            }
        }
    }
}

#[tokio::test]
async fn corrupt_duplicate_cannot_displace_valid_recovery_in_a_retained_session() {
    for corrupt_first in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
        let job = JobId(30970);
        let mut fixture =
            install_parked_recovery_par2_job(&mut pipeline, job, "Recovery packet schedules").await;
        pipeline.check_job_completion(job).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
        assert_eq!(pipeline.par2_repairer_execute_calls, 0);
        let valid = fixture.recovery_volume.clone();
        let mut corrupt = valid.clone();
        *corrupt.last_mut().unwrap() ^= 0x80;
        fixture.recovery_volume = if corrupt_first {
            [corrupt, valid].concat()
        } else {
            [valid, corrupt].concat()
        };
        land_parked_recovery_volume(&mut pipeline, job, &fixture).await;
        pipeline.check_job_completion(job).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        assert_eq!(
            job_status_for_assert(&pipeline, job),
            Some(JobStatus::Complete),
            "corrupt_first={corrupt_first}: {}",
            debug_job_state(&pipeline, job)
        );
        // Completion may move the working directory; its delivered bytes are
        // checked through the normal output tree by the fixture's payload name.
        let output = complete
            .join("Recovery packet schedules")
            .join(PARKED_RECOVERY_PAYLOAD);
        assert_eq!(std::fs::read(output).unwrap(), fixture.original_payload);
    }
}

#[tokio::test]
async fn a_duplicate_write_rejects_the_analysis_of_its_previous_bytes() {
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let job = JobId(30972);
    insert_damaged_job_ready_for_analysis(&mut pipeline, job, "analysis-duplicate").await;
    pipeline.check_job_completion(job).await;
    let done = next_par2_analysis_done(&mut pipeline).await;
    let old_work_id = done.work_id;
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id: job,
            file_index: 0,
        },
        0,
        0,
        &[0x42; 64],
        "payload.mkv",
        None,
    )
    .await;
    assert!(
        pipeline
            .par2_analysis_in_flight
            .get(&job)
            .is_none_or(|work| work.work_id != old_work_id),
        "the duplicate rewrote the source, so the old analysis must be fenced out"
    );
    pipeline.handle_par2_analysis_done(done).await;
    assert!(!pipeline.par2_analysis_results.contains_key(&job));
    settle_par2_analysis_work(&mut pipeline).await;
}
