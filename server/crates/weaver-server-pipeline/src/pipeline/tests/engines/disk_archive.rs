// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::actor_drive::{drive, limits};
use super::harness::{ARTICLE, JOB, Post, Scenario, admit, payload};
use crate::JobId;
use crate::history::queries::HistoryQueries;
use crate::operations::{DiskSpace, StorageCapacity, StorageRoot};
use crate::persistence::ArchiveNews;
use crate::pipeline::{Pipeline, PipelineEvent};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
pub const RESERVE: u64 = 1 << 30;

pub const PLENTY: u64 = 1 << 30;

// The room value at which a look at the disk fails.
pub const UNREADABLE: u64 = u64::MAX;

pub(super) const NEVER: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);

pub fn on_disk_posting(room: &Arc<AtomicU64>, posts: Vec<Post>) -> Scenario {
    on_disk_keeping(room, posts, RESERVE)
}

// The same, the job held to keep `reserve` free and the disks reading
// `room` bytes over it.
pub fn on_disk_keeping(room: &Arc<AtomicU64>, posts: Vec<Post>, reserve: u64) -> Scenario {
    let room = Arc::clone(room);
    Scenario::new(posts)
        .limits(limits(move |limits| limits.min_free_bytes = reserve))
        .setup(move |pipeline| {
            let roots = [
                pipeline.script_data_dir.clone(),
                pipeline.intermediate_dir.clone(),
                pipeline.complete_dir.clone(),
            ];
            let room = Arc::clone(&room);
            let probe = Arc::new(move |_: &Path| match room.load(Ordering::Acquire) {
                UNREADABLE => {
                    Err(weaver_server_disk::operations::disk::DiskProbeError::InvalidPath)
                }
                room => Ok(DiskSpace {
                    total_bytes: RESERVE * 8,
                    available_bytes: reserve + room,
                }),
            });
            let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
            for root in StorageRoot::ALL {
                capacity.wait_for_probes(root, 1);
            }
            pipeline.storage_capacity = Arc::new(capacity);
        })
}

// `root` is looked at afresh, by a probe begun after this call, reading
// `value` bytes over the reserve, and the loop hears that reading.
pub fn hears(pipeline: &mut Pipeline, room: &AtomicU64, root: StorageRoot, value: u64) {
    room.store(value, Ordering::Release);
    let begun = pipeline.storage_capacity.probes_begun(root);
    pipeline.storage_capacity.request_refresh(root);
    pipeline
        .storage_capacity
        .wait_for_reading_begun_after(root, begun);
    pipeline.disk_reading();
}

// A second job beside `JOB`, with one small file.
pub const SECOND: JobId = JobId(93_012);

pub fn one_file(room: &Arc<AtomicU64>) -> Scenario {
    on_disk_posting(
        room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 97))],
    )
}

// The next `count` history archives find no room on the database's disk.
fn refuse_archives(count: usize) {
    use crate::persistence::connection::ARCHIVE_REFUSED;
    let mut refused = ARCHIVE_REFUSED.lock().unwrap();
    for _ in 0..count {
        refused.push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    }
}

pub fn archives_yet_to_refuse() -> usize {
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .len()
}

pub fn archives_written() -> u64 {
    crate::persistence::connection::ARCHIVES_WRITTEN.load(Ordering::Relaxed)
}

// The loop hears what the database answered, until `until` holds.
async fn hears_database(pipeline: &mut Pipeline, until: impl Fn(&Pipeline) -> bool) {
    while !until(pipeline) {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        pipeline.handle_db_reply(reply).await;
    }
}

// Every write queued so far has run, and the loop has heard all the writer
// said of them: the writer speaks before the flush behind it answers.
pub async fn writer_heard(pipeline: &mut Pipeline) {
    pipeline.db.flush_write_queue().await.unwrap();
    while let Ok(reply) = pipeline.db_reply_rx.try_recv() {
        pipeline.handle_db_reply(reply).await;
    }
}

// How many kept archives the pipeline counts, if it waits for any.
pub fn kept_count(pipeline: &Pipeline) -> Option<usize> {
    pipeline.archive_wait.as_ref().map(|wait| wait.kept)
}

pub fn wait_shown(pipeline: &Pipeline) -> Option<String> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .find(|row| row.id == JOB)
            .and_then(|row| row.report.job.download_wait_reason.clone())
    })
}

// The job ends with its archive refused `refused` times for want of room,
// and the pipeline has heard that the writer keeps it.
pub async fn kept_job(
    room: &Arc<AtomicU64>,
    refused: usize,
) -> (
    tempfile::TempDir,
    Pipeline,
    tokio::sync::broadcast::Receiver<PipelineEvent>,
) {
    let scenario = one_file(room);
    let (temp_dir, mut pipeline, _) = admit(&scenario).await;
    let events = pipeline.event_tx.subscribe();
    refuse_archives(refused);
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    hears_database(&mut pipeline, |pipeline| pipeline.archive_wait.is_some()).await;
    assert_eq!(kept_count(&pipeline), Some(1));
    (temp_dir, pipeline, events)
}

pub const RISE: u64 = crate::pipeline::full_disk::ARCHIVE_RETRY_RISE;

pub fn in_queue(pipeline: &Pipeline) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.jobs.iter().any(|row| row.id == JOB))
}

pub fn retiring(pipeline: &Pipeline) -> bool {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .any(|row| row.id == JOB && row.report.retiring)
    })
}

pub async fn admit_archive(scenario: &Scenario) -> (tempfile::TempDir, Pipeline) {
    let (root, pipeline, _) = admit(scenario).await;
    (root, pipeline)
}
pub async fn finish_disk_job(pipeline: &mut Pipeline, scenario: &Scenario) {
    drive(pipeline, scenario, JOB, |_| false).await;
}
pub fn subscribe(pipeline: &Pipeline) -> tokio::sync::broadcast::Receiver<PipelineEvent> {
    pipeline.event_tx.subscribe()
}
pub fn shown(pipeline: &Pipeline) -> Option<crate::JobInfo> {
    pipeline.shared_state.get_job(JOB)
}
pub fn waited_paths(pipeline: &Pipeline) -> usize {
    pipeline.storage_capacity.waited_paths()
}
pub fn waiting(pipeline: &Pipeline) -> bool {
    pipeline.waits_for_disk_room()
}
pub fn kept(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.db.kept_archive_jobs()
}
pub fn history(pipeline: &Pipeline) -> Option<crate::JobHistoryRow> {
    pipeline.db.get_job_history(JOB.0).unwrap()
}
pub async fn flush(pipeline: &Pipeline) {
    pipeline.db.flush_write_queue().await.unwrap();
}
pub fn keep_free(pipeline: &mut Pipeline, bytes: u64) {
    Arc::make_mut(&mut pipeline.extraction_limits).min_free_bytes = bytes;
}
pub fn archive_kept(pipeline: &mut Pipeline, id: JobId) {
    pipeline.archive_news(ArchiveNews::Kept(id));
}
pub fn archive_let_go(pipeline: &mut Pipeline, id: JobId, written: bool) {
    pipeline.archive_news(ArchiveNews::LetGo {
        job_id: id,
        written,
    });
}
pub fn archive_more(pipeline: &mut Pipeline, from: usize) {
    pipeline.archive_news(ArchiveNews::More { from });
}
pub fn refuse_malformed() {
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::Database("the row is malformed".into()));
}
pub fn panic_next_pass() {
    crate::persistence::connection::ARCHIVE_PASS_PANICS.store(true, Ordering::Relaxed);
}
pub fn retry_at_drain(pipeline: &Pipeline) {
    pipeline.retry_kept_archives_at_drain();
}
pub fn warn_of_kept(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.warn_of_kept_archives()
}
pub async fn first_write_silent(pipeline: &mut Pipeline) -> bool {
    while let Ok(reply) = pipeline.db_reply_rx.try_recv() {
        if matches!(reply, crate::pipeline::db_reply::DbReply::Archive(_)) {
            return false;
        }
        pipeline.handle_db_reply(reply).await;
    }
    true
}
