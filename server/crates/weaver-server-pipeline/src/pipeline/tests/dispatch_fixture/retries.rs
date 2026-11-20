// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub struct ParkedRetry(RetryWork);

impl ParkedRetry {
    pub fn work(&self) -> &DownloadWork {
        &self.0.work
    }
}

impl DispatchFixture {
    pub async fn body_availability(&self) -> weaver_nntp::pool::BodyServerAvailability {
        self.0
            .nntp
            .body_server_availability(
                weaver_nntp::ServerSet::EMPTY,
                weaver_nntp::ServerSet::EMPTY,
                0,
            )
            .await
    }

    pub fn take_due_retries(&mut self, at: tokio::time::Instant) -> Vec<ParkedRetry> {
        self.0
            .infrastructure_retries
            .take_due(at)
            .into_iter()
            .map(ParkedRetry)
            .collect()
    }

    pub fn requeue_retry(&mut self, retry: ParkedRetry) {
        self.0.receive_retry_work(retry.0);
    }

    pub fn parked_retry_count(&self) -> usize {
        self.0.infrastructure_retries.len()
    }

    pub fn take_parked_retry_works(&mut self) -> Vec<DownloadWork> {
        self.0
            .infrastructure_retries
            .wake_all()
            .into_iter()
            .map(|retry| retry.work)
            .collect()
    }

    pub fn has_download_wait(&self, id: JobId) -> bool {
        self.0.download_wait_by_job.contains_key(&id)
    }

    pub fn drain_download_queue_and_get_groups(&mut self, file_id: NzbFileId) -> Arc<[String]> {
        let active = self.job_mut(file_id.job_id).articles.active_mut().unwrap();
        active.download_queue.drain_all();
        Arc::clone(active.download_queue.file_groups(file_id).unwrap())
    }
}
