// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub fn install_at_status(
    pipeline: &mut Pipeline,
    id: JobId,
    name: &str,
    dir: PathBuf,
    status: JobStatus,
) {
    let mut state = minimal_job_state(id, name, dir);
    state.status = status;
    pipeline.jobs.insert(id, state);
}

pub fn recover_terminal(pipeline: &mut Pipeline, id: JobId) -> bool {
    let restored = crate::pipeline::RestoredPostProcessing::read(&pipeline.db, id, None);
    pipeline.recover_restored_terminal_post_processing(id, Some(restored))
}

pub fn set_client(pipeline: &mut Pipeline, hosts: &[usize]) {
    pipeline.nntp = Arc::new(hosts_client(hosts));
}

pub fn reset_download_queue(pipeline: &mut Pipeline, id: JobId) {
    pipeline
        .jobs
        .get_mut(&id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
}

pub fn receive_ordinary_retry(pipeline: &mut Pipeline, work: DownloadWork, generation: u64) {
    pipeline.note_retry_scheduled(work.segment_id);
    pipeline.receive_retry_work(RetryWork {
        scheduled_pool_generation: generation,
        infrastructure_retry: false,
        hold: crate::pipeline::RetryHold::Ordinary,
        work,
    });
}

pub fn pop_work(
    pipeline: &mut Pipeline,
    id: JobId,
) -> Option<crate::pipeline::download::QueueItem> {
    pipeline
        .jobs
        .get_mut(&id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
}
