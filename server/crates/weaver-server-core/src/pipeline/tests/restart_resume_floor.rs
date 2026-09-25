//! A job that resumes after a restart must keep advancing its durable floor.
//!
//! The restored prefix is committed straight into assembly and never enters the
//! write reorder buffer, so the buffer's cursor has to be told where those bytes
//! stop. If it is not, every arriving part sits above a cursor still at zero,
//! the sequential drain never releases anything, and the file's contiguous
//! floor — the only input to the job's durable floor — stays pinned at the value
//! the restart credited it for the rest of the file's life.

use super::*;

use crate::jobs::handle::RestoreJobRequest;
use crate::pipeline::tests::yenc_compatibility::deliver;

/// Declared segment sizes are ENCODED, the payloads are DECODED, and the two
/// deliberately disagree here: a prefix of 8 decoded bytes covers two parts on
/// disk while a walk over declared sizes can only prove one. That gap is the
/// whole reason the resumed cursor cannot be a byte count carried over from the
/// restart, and a fixture where the two agree would never show it.
const DECLARED_SEGMENT_BYTES: u32 = 8;
const DECODED_PAYLOADS: [&[u8]; 5] = [b"head", b"tail", b"more", b"tail", b"last"];

fn decoded_offset(ordinal: usize) -> u64 {
    DECODED_PAYLOADS[..ordinal]
        .iter()
        .map(|payload| payload.len() as u64)
        .sum()
}

/// Bring up a job in exactly the state a restart leaves it in: a partial file on
/// disk, a persisted contiguous floor over it, and the ordinals that floor
/// covers committed without ever being fetched.
async fn restore_resumed_job(
    temp: &TempDir,
    id: u64,
    persisted_floor: u64,
) -> (Pipeline, NzbFileId, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let job_id = JobId(id);
    let spec = segmented_job_spec(
        "restart resume",
        "sample.bin",
        &[DECLARED_SEGMENT_BYTES; DECODED_PAYLOADS.len()],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
    let path = working_dir.join("sample.bin");

    // What the previous run had written when it stopped.
    let mut on_disk = Vec::new();
    for payload in DECODED_PAYLOADS {
        if on_disk.len() as u64 >= persisted_floor {
            break;
        }
        on_disk.extend_from_slice(payload);
    }
    on_disk.truncate(persisted_floor as usize);
    tokio::fs::write(&path, &on_disk).await.unwrap();

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
            file_progress: HashMap::from([(0u32, persisted_floor)]),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: Some(crate::jobs::model::DownloadState::Downloading),
            post_state: Some(crate::jobs::model::PostState::Idle),
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
    state.download_queue.drain_all();

    (
        pipeline,
        NzbFileId {
            job_id,
            file_index: 0,
        },
        path,
    )
}

#[tokio::test]
async fn resumed_file_advances_its_durable_floor_past_the_restart_credit() {
    let temp = tempfile::tempdir().unwrap();
    // Two parts' worth of decoded bytes are on disk; a walk over the declared
    // sizes can only account for the first, so the second is refetched.
    let (mut pipeline, file_id, path) = restore_resumed_job(&temp, 41001, 8).await;
    let job_id = file_id.job_id;

    let restored_floor = pipeline.jobs[&job_id].restored_download_floor_bytes;
    assert_eq!(restored_floor, 8);
    assert_eq!(
        pipeline.persisted_file_progress.get(&file_id).copied(),
        Some(8)
    );
    assert_eq!(
        pipeline.durable_download_floor_bytes_for_job(job_id),
        restored_floor
    );

    // Out of order on purpose: the part that follows the restored prefix is
    // rarely the first to decode with a pool of connections behind it.
    deliver(
        &mut pipeline,
        file_id,
        2,
        decoded_offset(2),
        DECODED_PAYLOADS[2],
        true,
    )
    .await;
    assert_eq!(
        pipeline.durable_download_floor_bytes_for_job(job_id),
        restored_floor,
        "a part past the prefix cannot be written in order on its own"
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

    // The prefix's own part arrived, so the cursor now knows where the bytes on
    // disk stop and both parts drained in order.
    assert_eq!(
        pipeline.pending_file_progress.get(&file_id).copied(),
        Some(12),
        "the contiguous floor must move past the restart credit"
    );
    let advanced_floor = pipeline.durable_download_floor_bytes_for_job(job_id);
    assert_eq!(advanced_floor, 12);
    assert!(advanced_floor > restored_floor);
    assert!(
        !pipeline.jobs[&job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .is_complete(),
        "the floor must advance while the file is still being downloaded"
    );
    assert_eq!(
        pipeline.write_buffers[&file_id].buffered_len(),
        0,
        "nothing may be left parked behind a cursor that has been positioned"
    );

    // The undurable lead the restart guard measures is what is accepted above
    // the durable floor, so an advancing floor is what keeps it bounded.
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes = 40;
    assert_eq!(
        pipeline.estimated_undurable_download_bytes_for_job(job_id),
        40 - advanced_floor
    );

    // The bytes landed where they belong, prefix included.
    assert_eq!(std::fs::read(&path).unwrap(), b"headtailmore");

    for ordinal in [3usize, 4] {
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
async fn resumed_file_advances_its_durable_floor_on_in_order_arrivals() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = restore_resumed_job(&temp, 41002, 8).await;
    let job_id = file_id.job_id;
    let restored_floor = pipeline.jobs[&job_id].restored_download_floor_bytes;

    let mut floors = Vec::new();
    for (ordinal, payload) in DECODED_PAYLOADS.iter().enumerate().skip(1) {
        deliver(
            &mut pipeline,
            file_id,
            ordinal as u32,
            decoded_offset(ordinal),
            payload,
            true,
        )
        .await;
        floors.push(pipeline.durable_download_floor_bytes_for_job(job_id));
    }

    // The first arrival re-covers ground the restart already credited, so it is
    // the second onwards that has to show movement.
    assert_eq!(floors, vec![8, 12, 16, 40]);
    assert!(floors[1] > restored_floor);
    assert_eq!(std::fs::read(&path).unwrap(), b"headtailmoretaillast");
}

/// A file with no persisted floor is not resumed, and nothing about it may
/// change: its cursor starts at zero because its first part really is coming.
#[tokio::test]
async fn a_file_without_a_restored_prefix_still_starts_at_zero() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, path) = restore_resumed_job(&temp, 41003, 0).await;
    let job_id = file_id.job_id;

    assert_eq!(pipeline.jobs[&job_id].restored_download_floor_bytes, 0);
    assert!(
        pipeline.jobs[&job_id]
            .assembly
            .file(file_id)
            .unwrap()
            .resume_anchor_ordinal()
            .is_none()
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
        pipeline.durable_download_floor_bytes_for_job(job_id),
        0,
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
    assert_eq!(pipeline.durable_download_floor_bytes_for_job(job_id), 8);
    assert_eq!(std::fs::read(&path).unwrap(), b"headtail");
}

/// The restart guard's own units: it hands out one article at a time exactly
/// while the bytes accepted above the durable floor exceed its lead limit, so a
/// floor that cannot move is a job that never leaves that state. The limit is a
/// process-wide constant, so the fixture is written relative to it rather than
/// by lowering it.
#[tokio::test]
async fn an_advancing_floor_brings_a_resumed_job_back_within_the_restart_lead() {
    // The admission verdict is compared through its metric label: the decision
    // type itself is private to the download worker.
    const WITHIN_LIMIT: &str = "within_limit";

    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id, _) = restore_resumed_job(&temp, 41004, 8).await;
    let job_id = file_id.job_id;
    let limit = Pipeline::restart_durable_lead_limit_bytes();

    let work = DownloadWork {
        segment_id: SegmentId {
            file_id,
            segment_number: 4,
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
    };

    // Accepted bytes placed so that the guard is over its limit at the floor the
    // restart credited, and back under it after one contiguous drain.
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes = limit + 4;
    assert_ne!(
        pipeline
            .checkpoint_admission(job_id)
            .decision(&work, &[])
            .as_str(),
        WITHIN_LIMIT,
        "the pinned floor must leave the job outside the lead limit"
    );

    deliver(
        &mut pipeline,
        file_id,
        2,
        decoded_offset(2),
        DECODED_PAYLOADS[2],
        true,
    )
    .await;
    deliver(
        &mut pipeline,
        file_id,
        1,
        decoded_offset(1),
        DECODED_PAYLOADS[1],
        true,
    )
    .await;
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes = limit + 4;

    assert_eq!(pipeline.durable_download_floor_bytes_for_job(job_id), 12);
    assert_eq!(
        pipeline
            .checkpoint_admission(job_id)
            .decision(&work, &[])
            .as_str(),
        WITHIN_LIMIT,
        "an advanced floor must return the job to ordinary handouts"
    );
}
