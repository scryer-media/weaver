// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An article no server may fetch spends each message-id the NZB gave its
// part, in NZB order, before it is booked missing. However many ids the part
// carries, the booking does not nest in itself once per id.

use super::*;

// What one booking left behind, for comparing runs.
#[derive(Debug, PartialEq, Eq)]
struct BookingOutcome {
    spent_ids: Vec<String>,
    id_in_use: Option<String>,
    terminal_state: Option<SegmentTerminalState>,
    articles_not_found: u64,
    failed_bytes: Option<u64>,
    status: Option<JobStatus>,
    queued: Option<usize>,
    download_pass_open: bool,
    completion_check_pending: bool,
}

fn alternate_ids(count: usize) -> Vec<String> {
    (0..count)
        .map(|idx| format!("alt-{idx}@example.com"))
        .collect()
}

// A two-article job whose first part carries `ids`, older than the only
// server's retention, so no server may fetch any of its ids. The download
// pass is open and the queues are empty.
async fn job_beyond_retention(
    pipeline: &mut Pipeline,
    job_id: JobId,
    ids: Vec<String>,
) -> SegmentId {
    pipeline.nntp = std::sync::Arc::new(retention_client(&[5]));
    let mut spec = segmented_job_spec("Alternate Ids Beyond Retention", "aged.bin", &[128, 128]);
    spec.files[0].segments[0].message_ids = crate::jobs::MessageIds::new(ids);
    insert_active_job(pipeline, job_id, spec).await;
    age_job_days(pipeline, job_id, 10);
    pipeline.clear_job_retention_excludes(job_id);
    {
        let active = pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap();
        active.download_queue = DownloadQueue::new();
        active.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_download_passes.insert(job_id);
    SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    }
}

fn outcome(pipeline: &Pipeline, segment_id: SegmentId) -> BookingOutcome {
    let job_id = segment_id.file_id.job_id;
    let state = pipeline.jobs.get(&job_id);
    BookingOutcome {
        spent_ids: pipeline.spent_message_ids.clone(),
        id_in_use: state.map(|state| {
            state.spec.files[0].segments[0]
                .message_ids
                .in_use()
                .to_string()
        }),
        terminal_state: pipeline.segment_terminal_states.get(&segment_id).copied(),
        articles_not_found: pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        failed_bytes: state.map(|state| state.failed_bytes),
        status: state.map(|state| state.status.clone()),
        queued: state.and_then(|state| {
            let active = state.articles.active()?;
            Some(active.download_queue.len() + active.recovery_queue.len())
        }),
        download_pass_open: pipeline.active_download_passes.contains(&job_id),
        completion_check_pending: pipeline.pending_completion_checks.contains(&job_id),
    }
}

// The article's own retry, requeued with no server left to fetch it.
async fn requeue_unservable(ids: Vec<String>) -> (BookingOutcome, usize) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let segment_id = job_beyond_retention(&mut pipeline, JobId(41980), ids).await;
    let work = pipeline
        .download_retry_work(segment_id, 0, Vec::new())
        .unwrap();
    pipeline.enqueue_download_work(work);
    (
        outcome(&pipeline, segment_id),
        pipeline.unservable_booking_depth.1,
    )
}

// The article queued, retired by the sweep that books every queued article
// no server may fetch.
async fn sweep_unservable(ids: Vec<String>) -> (BookingOutcome, usize, usize) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let segment_id = job_beyond_retention(&mut pipeline, JobId(41981), ids).await;
    let work = pipeline
        .download_retry_work(segment_id, 0, Vec::new())
        .unwrap();
    pipeline
        .jobs
        .get_mut(&segment_id.file_id.job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .push(work);
    let retired = pipeline.retire_unservable_queued_work(segment_id.file_id.job_id);
    (
        outcome(&pipeline, segment_id),
        pipeline.unservable_booking_depth.1,
        retired,
    )
}

// What either path leaves for a part of `ids`: every id spent in NZB order,
// the last one in use, and the article booked missing with the download pass
// finished and the job's completion check queued.
fn expected_outcome(ids: &[String], status: Option<JobStatus>) -> BookingOutcome {
    BookingOutcome {
        spent_ids: ids.to_vec(),
        id_in_use: ids.last().cloned(),
        terminal_state: Some(SegmentTerminalState::Missing),
        articles_not_found: 1,
        failed_bytes: Some(128),
        status,
        queued: Some(0),
        download_pass_open: false,
        completion_check_pending: true,
    }
}

#[tokio::test]
async fn an_unservable_requeue_spends_every_id_then_books_missing() {
    let ids = alternate_ids(4);
    let (outcome, _) = requeue_unservable(ids.clone()).await;
    assert_eq!(
        outcome,
        expected_outcome(&ids, Some(JobStatus::Downloading))
    );
}

#[tokio::test]
async fn the_unservable_sweep_spends_every_id_then_books_missing() {
    let ids = alternate_ids(4);
    let (outcome, _, retired) = sweep_unservable(ids.clone()).await;
    assert_eq!(retired, 1);
    assert_eq!(
        outcome,
        expected_outcome(&ids, Some(JobStatus::Downloading))
    );
}

#[tokio::test]
async fn many_thousand_alternate_ids_do_not_deepen_the_booking() {
    let few = alternate_ids(4);
    let many = alternate_ids(20_000);

    let (few_outcome, few_depth) = requeue_unservable(few.clone()).await;
    let (many_outcome, many_depth) = requeue_unservable(many.clone()).await;
    assert_eq!(
        few_outcome,
        expected_outcome(&few, Some(JobStatus::Downloading))
    );
    assert_eq!(
        many_outcome,
        expected_outcome(&many, Some(JobStatus::Downloading))
    );
    assert_eq!(many_depth, few_depth, "the booking nests once per id");
    assert_eq!(many_depth, 1);

    let (few_outcome, few_depth, _) = sweep_unservable(few.clone()).await;
    let (many_outcome, many_depth, retired) = sweep_unservable(many.clone()).await;
    assert_eq!(retired, 1);
    assert_eq!(
        few_outcome,
        expected_outcome(&few, Some(JobStatus::Downloading))
    );
    assert_eq!(
        many_outcome,
        expected_outcome(&many, Some(JobStatus::Downloading))
    );
    assert_eq!(many_depth, few_depth, "the booking nests once per id");
    assert_eq!(many_depth, 1);
}
