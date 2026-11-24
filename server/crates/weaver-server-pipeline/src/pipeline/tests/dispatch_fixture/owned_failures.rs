// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub struct FailedFetch(DownloadResult);

impl FailedFetch {
    fn new(segment: SegmentId, kind: DownloadFailureKind, message: &str) -> Self {
        Self(DownloadResult {
            lane_id: 0,
            job_id: segment.file_id.job_id,
            segment_id: segment,
            runtime_generation: 0,
            data: Err(DownloadError::fetch(kind, message)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            release_connection_slot: true,
        })
    }
    pub fn missing(segment: SegmentId, message: &str) -> Self {
        Self::new(segment, DownloadFailureKind::ArticleNotFound, message)
    }
    pub fn auth(segment: SegmentId, message: &str) -> Self {
        Self::new(segment, DownloadFailureKind::Auth, message)
    }
    pub fn transport(segment: SegmentId, message: &str) -> Self {
        Self::new(segment, DownloadFailureKind::EstablishedTransport, message)
    }
    pub fn unavailable(segment: SegmentId, message: &str) -> Self {
        Self::new(segment, DownloadFailureKind::LaneUnavailable, message)
    }
    pub fn without_lane_release(mut self) -> Self {
        self.0.release_connection_slot = false;
        self
    }
    pub fn on_server(mut self, server: usize) -> Self {
        self.0.source_server_idx = Some(server);
        self
    }
    pub fn recovery(mut self) -> Self {
        self.0.origin = DownloadResultOrigin::Recovery;
        self
    }
    pub fn retry_count(mut self, count: u32) -> Self {
        self.0.retry_count = count;
        self
    }
    pub fn excluding(mut self, set: weaver_nntp::ServerSet) -> Self {
        self.0.exclude_servers = set;
        self
    }
    pub fn attempts(mut self, attempts: Vec<weaver_nntp::client::FetchAttemptTrace>) -> Self {
        self.0.attempts = attempts;
        self
    }
}
impl DispatchFixture {
    pub async fn failed_fetch(&mut self, failure: FailedFetch) {
        self.0.handle_download_done(failure.0).await;
    }
    pub fn pending_retries(&self, id: JobId) -> Option<usize> {
        self.0.pending_retries_by_job.get(&id).copied()
    }
    pub fn completion_checks_empty(&self) -> bool {
        self.0.pending_completion_checks.is_empty()
    }
    pub fn age_post(&mut self, id: JobId, days: u64) {
        age_job_days(&mut self.0, id, days);
    }
    pub fn clear_retention_excludes(&mut self, id: JobId) {
        self.0.clear_job_retention_excludes(id);
    }
    pub fn retention_excludes(&self, id: JobId) -> weaver_nntp::ServerSet {
        self.0.job_retention_excludes(id)
    }
    pub fn effective_excludes(
        &mut self,
        id: JobId,
        failed: weaver_nntp::ServerSet,
    ) -> weaver_nntp::ServerSet {
        self.0.effective_exclude_servers(id, failed)
    }
    pub async fn reprocess_and_activate(&mut self, job: JobId) {
        self.0.reprocess_job(job).await.unwrap();
        activate_job(&mut self.0, job).await;
    }
    pub fn book_failed_segment(&mut self, segment: SegmentId) -> bool {
        self.0.book_failed_segment(segment)
    }
    pub fn derived_failed_bytes(&self, id: JobId) -> u64 {
        self.0.derived_failed_bytes(id)
    }
    pub fn missing_bytes(&self, id: JobId) -> u64 {
        self.0.missing_bytes(id)
    }
    pub fn graph_article_given_up(&self, segment: SegmentId) -> bool {
        self.0.graph_article_given_up(segment)
    }
}
