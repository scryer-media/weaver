use super::*;
use crate::jobs::support_facts::GapPosition;

fn segment(job_id: JobId, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number,
    }
}

async fn insert_tolerant_job(pipeline: &mut Pipeline, id: JobId, spec: JobSpec) {
    insert_active_job(pipeline, id, spec).await;
    // Health policy is not under test: keep a damaged job from aborting.
    let state = pipeline.jobs.get_mut(&id).unwrap();
    state.next_health_probe_failed_bytes = u64::MAX;
    state.par2_bytes = state.spec.total_bytes;
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
}

#[tokio::test]
async fn gaps_and_demotion_are_booked_once_restored_without_doubling_and_archived() {
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let id = JobId(79901);
    let spec = segmented_job_spec("Quiet Meadow", "quiet-meadow.bin", &[100, 200, 300, 400]);
    insert_tolerant_job(&mut pipeline, id, spec.clone()).await;

    pipeline.note_demotion_support_fact(id, "par2_damaged");
    pipeline.book_failed_segment(segment(id, 1));
    // A second result for the same segment is not a second gap.
    pipeline.book_failed_segment(segment(id, 1));
    pipeline.book_terminal_segment(segment(id, 3), SegmentTerminalState::RetriesExhausted);
    {
        let facts = &pipeline.jobs[&id].support_facts;
        let demotion = facts.demotion.as_ref().unwrap();
        assert_eq!(demotion.reason, "par2_damaged");
        assert_eq!(demotion.stage, "downloading");
        assert_eq!(facts.gaps.missing, 1);
        assert_eq!(facts.gaps.failed, 1);
        assert_eq!(
            facts.gaps.sample,
            vec![GapPosition(0, 1), GapPosition(0, 3)]
        );
    }
    assert!(pipeline.dirty_support_facts.contains(&id));
    pipeline.flush_support_facts();
    assert!(pipeline.dirty_support_facts.is_empty());
    pipeline.db.flush_write_queue().await.unwrap();
    assert_eq!(
        pipeline.db.load_job_support_facts(id).unwrap().gaps.missing,
        1
    );
    retire_pipeline_database(pipeline).await;

    let (mut restored, _, _) = new_direct_pipeline(&temp).await;
    let saved = restored.db.load_active_jobs().unwrap().remove(&id).unwrap();
    restored
        .restore_job(RestoreJobRequest {
            job_id: id,
            job_hash: [0; 32],
            spec: spec.clone(),
            complete_files: saved.complete_files,
            file_progress: saved.file_progress,
            detected_archives: saved.detected_archives,
            file_identities: saved.file_identities,
            extracted_members: saved.extracted_members,
            status: JobStatus::Paused,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: saved.output_dir,
        })
        .await
        .unwrap();
    {
        let facts = &restored.jobs[&id].support_facts;
        assert_eq!(facts.gaps.missing, 1);
        assert_eq!(facts.gaps.failed, 1);
        assert_eq!(facts.demotion.as_ref().unwrap().reason, "par2_damaged");
    }
    {
        let state = restored.jobs.get_mut(&id).unwrap();
        state.next_health_probe_failed_bytes = u64::MAX;
        state.par2_bytes = state.spec.total_bytes;
    }
    // The resumed download asks again for what it does not hold and books the
    // same gap a second time; the summary starts over instead of doubling.
    restored.book_failed_segment(segment(id, 1));
    {
        let facts = &restored.jobs[&id].support_facts;
        assert_eq!(facts.gaps.missing, 1);
        assert_eq!(facts.gaps.failed, 0);
        assert_eq!(facts.gaps.sample, vec![GapPosition(0, 1)]);
        assert_eq!(facts.demotion.as_ref().unwrap().sets, 1);
    }

    // The finish writes the facts ahead of the archive, which copies them.
    restored.persist_support_facts_before_archive(id);
    restored.db.flush_write_queue().await.unwrap();
    let row = history_row_with_output_dir(id, "Quiet Meadow", "failed", temp.path().into());
    restored.db.archive_job(id, &row).unwrap();
    assert!(!restored.db.load_active_jobs().unwrap().contains_key(&id));
    let archived = restored.db.load_job_support_facts(id).unwrap();
    assert_eq!(archived.gaps.missing, 1);
    assert_eq!(archived.demotion.unwrap().reason, "par2_damaged");
    retire_pipeline_database(restored).await;
}

#[tokio::test]
async fn retrying_a_job_drops_its_gaps_but_keeps_its_demotion() {
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let id = JobId(79902);
    let spec = segmented_job_spec("Amber Tide", "amber-tide.bin", &[128, 10_000]);
    insert_tolerant_job(&mut pipeline, id, spec).await;
    pipeline.note_demotion_support_fact(id, "member_checksum_mismatch");
    pipeline.book_failed_segment(segment(id, 0));
    assert_eq!(pipeline.jobs[&id].support_facts.gaps.missing, 1);

    pipeline.clear_terminal_segment_failures(id);
    let facts = &pipeline.jobs[&id].support_facts;
    assert!(facts.gaps.is_empty());
    assert_eq!(
        facts.demotion.as_ref().unwrap().reason,
        "member_checksum_mismatch"
    );
    retire_pipeline_database(pipeline).await;
}
