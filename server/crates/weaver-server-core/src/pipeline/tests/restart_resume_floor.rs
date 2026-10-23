// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job that resumes after a restart must resume from what it wrote, and keep
// advancing its durable floor from there.
//
// A file's checkpoint is its written coverage: the runs of articles on disk,
// wherever they sit. A restart commits those articles straight into the
// assembly without fetching them, so they never enter the write reorder
// buffer, and the buffer has to be told where each restored run stops. If it
// is not, a part after a restored run sits above a cursor that never reaches
// it, and the file can only drain by backlog eviction.

use super::*;

use crate::jobs::handle::RestoreJobRequest;
use crate::pipeline::tests::yenc_compatibility::deliver;

// Declared segment sizes are ENCODED, the payloads are DECODED, and the two
// deliberately disagree here: coverage records where each article's decoded
// bytes landed, so nothing on the restart path may turn a declared size into
// a file offset, and a fixture where the two agree would never show it.
const DECLARED_SEGMENT_BYTES: u32 = 8;
const DECODED_PAYLOADS: [&[u8]; 5] = [b"head", b"tail", b"more", b"tail", b"last"];

fn decoded_offset(ordinal: usize) -> u64 {
    DECODED_PAYLOADS[..ordinal]
        .iter()
        .map(|payload| payload.len() as u64)
        .sum()
}

// The coverage checkpoint a previous run left for the `written` articles.
fn coverage_of(written: &[usize]) -> crate::jobs::assembly::FileCoverage {
    crate::jobs::assembly::FileCoverage::from_ordered_runs(written.iter().map(|&ordinal| {
        crate::jobs::assembly::CoverageRun {
            first_ordinal: ordinal as u32,
            ordinals: 1,
            start: decoded_offset(ordinal),
            end: decoded_offset(ordinal) + DECODED_PAYLOADS[ordinal].len() as u64,
        }
    }))
}

// Bring up a job in exactly the state a restart leaves it in: the `written`
// articles on disk at their decoded offsets, a coverage checkpoint over them,
// and those articles committed without ever being fetched. Returns the
// ordinals the restore queued, in ordinal order.
async fn restore_resumed_job(
    temp: &TempDir,
    id: u64,
    written: &[usize],
    status: JobStatus,
) -> (Pipeline, NzbFileId, PathBuf, Vec<u32>) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let job_id = JobId(id);
    let spec = segmented_job_spec(
        "restart resume",
        "sample.bin",
        &[DECLARED_SEGMENT_BYTES; DECODED_PAYLOADS.len()],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
    let path = working_dir.join("sample.bin");

    // What the previous run had written when it stopped, holes included.
    let mut on_disk = Vec::new();
    for &ordinal in written {
        let start = decoded_offset(ordinal) as usize;
        let payload = DECODED_PAYLOADS[ordinal];
        if on_disk.len() < start + payload.len() {
            on_disk.resize(start + payload.len(), 0);
        }
        on_disk[start..start + payload.len()].copy_from_slice(payload);
    }
    tokio::fs::write(&path, &on_disk).await.unwrap();
    let file_progress = if written.is_empty() {
        HashMap::new()
    } else {
        HashMap::from([(0u32, coverage_of(written))])
    };

    // The restart itself: the runtime state built by `insert_active_job` is
    // dropped and rebuilt from the durable record, the way a fresh process
    // would build it.
    pipeline.jobs.remove(&job_id);
    pipeline.job_order.retain(|id| *id != job_id);
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            complete_files: HashSet::new(),
            file_progress,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status,
            download_state: None,
            post_state: None,
            run_state: Some(crate::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();
    assert!(
        !pipeline.blocked_restores.contains_key(&job_id),
        "the fixture must restore cleanly"
    );

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.next_health_probe_failed_bytes = u64::MAX;
    state.par2_bytes = state.spec.total_bytes;
    // These tests deliver the results of already dispatched requests.
    let mut queued = Vec::new();
    while let Some(work) = state.download_queue.pop() {
        queued.push(work.segment_id.segment_number);
    }
    queued.sort_unstable();

    (
        pipeline,
        NzbFileId {
            job_id,
            file_index: 0,
        },
        path,
        queued,
    )
}

fn work_for(file_id: NzbFileId, segment_number: u32) -> DownloadWork {
    DownloadWork {
        counters: None,
        segment_id: SegmentId {
            file_id,
            segment_number,
        },
        message_id: MessageId::new("resume-lead@example.com"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 0,
        byte_estimate: DECLARED_SEGMENT_BYTES,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: Vec::new(),
        avoid_server: None,
    }
}

// The admission verdict is compared through its metric label: the decision
// type itself is private to the download worker.
const WITHIN_LIMIT: &str = "within_limit";

#[tokio::test]
async fn a_restored_file_requeues_only_the_articles_its_coverage_misses() {
    let temp = tempfile::tempdir().unwrap();
    // An early gap with everything after it written: the crash that used to
    // come back at the gap and refetch the rest of the file.
    let (mut pipeline, file_id, path, queued) =
        restore_resumed_job(&temp, 41001, &[0, 2, 3], JobStatus::Downloading).await;
    let job_id = file_id.job_id;

    assert_eq!(
        queued,
        vec![1, 4],
        "only what the checkpoint misses is owed"
    );
    let covered = 3 * u64::from(DECLARED_SEGMENT_BYTES);
    let state = &pipeline.jobs[&job_id];
    assert_eq!(state.downloaded_bytes, covered);
    assert_eq!(state.restored_download_floor_bytes, covered);
    assert!(Pipeline::effective_progress(state) > 0.0);
    assert_eq!(
        pipeline.persisted_file_progress.get(&file_id).copied(),
        Some(covered)
    );
    assert_eq!(
        pipeline.durable_download_floor_bytes_for_job(job_id),
        covered
    );

    // The gap's part follows the first restored run, so it drains in order at
    // its own offset; the last part follows the second run, and does too.
    deliver(
        &mut pipeline,
        file_id,
        1,
        decoded_offset(1),
        DECODED_PAYLOADS[1],
        true,
    )
    .await;
    assert_eq!(pipeline.write_buffers[&file_id].buffered_len(), 0);
    deliver(
        &mut pipeline,
        file_id,
        4,
        decoded_offset(4),
        DECODED_PAYLOADS[4],
        true,
    )
    .await;
    assert_eq!(std::fs::read(&path).unwrap(), b"headtailmoretaillast");
    assert!(
        pipeline.jobs[&job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .is_complete()
    );
}

#[tokio::test]
async fn a_restored_job_whose_coverage_holds_every_article_refetches_nothing() {
    let temp = tempfile::tempdir().unwrap();
    // Downloaded end to end and verifying when it stopped, with no completed
    // file row yet: its coverage alone has to carry it.
    let (pipeline, file_id, _, queued) =
        restore_resumed_job(&temp, 41002, &[0, 1, 2, 3, 4], JobStatus::Verifying).await;
    let job_id = file_id.job_id;

    assert!(queued.is_empty(), "nothing is refetched, got {queued:?}");
    let state = &pipeline.jobs[&job_id];
    assert!(state.recovery_queue.is_empty());
    assert_eq!(state.downloaded_bytes, state.spec.total_bytes);
    assert!((Pipeline::effective_progress(state) - 1.0).abs() < f64::EPSILON);
    assert!(state.assembly.file(file_id).unwrap().is_complete());
    assert_eq!(state.status, JobStatus::Verifying);
}

// A file with no coverage is not resumed, and nothing about it may change: its
// cursor starts at zero because its first part really is coming.
#[tokio::test]
async fn a_file_without_restored_coverage_still_starts_at_zero() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path, queued) =
        restore_resumed_job(&temp, 41003, &[], JobStatus::Downloading).await;
    let job_id = file_id.job_id;

    assert_eq!(queued, vec![0, 1, 2, 3, 4]);
    assert_eq!(pipeline.jobs[&job_id].restored_download_floor_bytes, 0);
    assert!(
        !pipeline.jobs[&job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .follows_restored_run(0)
    );

    deliver(
        &mut pipeline,
        file_id,
        1,
        decoded_offset(1),
        DECODED_PAYLOADS[1],
        true,
    )
    .await;
    assert_eq!(
        pipeline.write_buffers[&file_id].buffered_len(),
        1,
        "a part that is not the file's first cannot be written in order"
    );
    deliver(
        &mut pipeline,
        file_id,
        0,
        decoded_offset(0),
        DECODED_PAYLOADS[0],
        true,
    )
    .await;
    pipeline.flush_file_progress_batch("test.barrier");
    assert_eq!(pipeline.durable_download_floor_bytes_for_job(job_id), 8);
    assert_eq!(std::fs::read(&path).unwrap(), b"headtail");
}

// The crawl: a restored job whose first missing article stays parked while
// every later one arrives. The later articles reach disk out of order, and
// that is coverage, so the floor follows them and the job keeps ordinary
// handouts instead of dropping to one article at a time.
#[tokio::test]
async fn a_gap_early_in_a_restored_file_never_holds_the_job_to_one_article() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _, _) =
        restore_resumed_job(&temp, 41004, &[0], JobStatus::Downloading).await;
    let job_id = file_id.job_id;
    let limit = Pipeline::restart_durable_lead_limit_bytes();
    let restored_floor = pipeline.durable_download_floor_bytes_for_job(job_id);
    assert_eq!(restored_floor, u64::from(DECLARED_SEGMENT_BYTES));

    // Article 1 never arrives; 2, 3 and 4 do, and are written past the gap.
    for ordinal in [2usize, 3, 4] {
        deliver(
            &mut pipeline,
            file_id,
            ordinal as u32,
            decoded_offset(ordinal),
            DECODED_PAYLOADS[ordinal],
            true,
        )
        .await;
    }
    pipeline.flush_quiescent_write_backlog().await;
    assert!(
        !pipeline.jobs[&job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .has_segment(1),
        "the gap is still open"
    );
    pipeline.flush_file_progress_batch("test.barrier");
    let written_past_gap = 3 * DECODED_PAYLOADS[2].len() as u64;
    assert_eq!(
        pipeline.durable_download_floor_bytes_for_job(job_id),
        restored_floor + written_past_gap,
        "articles written past the gap are durable"
    );

    // Accepted bytes placed so that a floor frozen at the gap would leave the
    // job over its lead limit, and the coverage floor leaves room for a lease
    // of more than one article.
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes = limit + 4;
    let work = work_for(file_id, 1);
    let admission = pipeline.checkpoint_admission(job_id);
    assert_eq!(admission.decision(&work, &[]).as_str(), WITHIN_LIMIT);
    assert_eq!(
        admission.decision(&work, &[work_for(file_id, 1)]).as_str(),
        WITHIN_LIMIT,
        "a gap must not drop the job to one article at a time"
    );
}

// The guard still bites where it should: written coverage that no barrier has
// recorded is lost to a restart, so it stays in the lead until the barrier
// runs, and running it releases the job.
#[tokio::test]
async fn a_checkpoint_behind_its_writes_throttles_until_the_barrier_runs() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _, _) =
        restore_resumed_job(&temp, 41005, &[0], JobStatus::Downloading).await;
    let job_id = file_id.job_id;
    let limit = Pipeline::restart_durable_lead_limit_bytes();
    let restored_floor = pipeline.durable_download_floor_bytes_for_job(job_id);

    for ordinal in [2usize, 3, 4] {
        deliver(
            &mut pipeline,
            file_id,
            ordinal as u32,
            decoded_offset(ordinal),
            DECODED_PAYLOADS[ordinal],
            true,
        )
        .await;
    }
    pipeline.flush_quiescent_write_backlog().await;
    assert!(
        pipeline.pending_file_progress.contains_key(&file_id),
        "the writes are coverage the barrier has yet to record"
    );
    assert_eq!(
        pipeline.durable_download_floor_bytes_for_job(job_id),
        restored_floor,
        "unrecorded coverage is not durable"
    );

    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes = limit + 4;
    let work = work_for(file_id, 1);
    assert_ne!(
        pipeline
            .checkpoint_admission(job_id)
            .decision(&work, &[])
            .as_str(),
        WITHIN_LIMIT,
        "a checkpoint genuinely behind holds the job back"
    );

    pipeline.flush_file_progress_batch("test.barrier");
    assert!(pipeline.pending_file_progress.is_empty());
    assert_eq!(
        pipeline
            .checkpoint_admission(job_id)
            .decision(&work, &[])
            .as_str(),
        WITHIN_LIMIT,
        "the barrier releases it"
    );
}
