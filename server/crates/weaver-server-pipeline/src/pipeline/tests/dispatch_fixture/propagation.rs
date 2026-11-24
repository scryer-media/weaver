// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// propagation delay

use super::*;
use crate::history::queries::HistoryQueries;
use crate::{PostState, RunState};

pub const PROPAGATION_WAIT_REASON: &str = crate::jobs::handle::PROPAGATION_WAIT_REASON;

impl DispatchFixture {
    pub async fn with_roots(temp: &TempDir) -> (Self, PathBuf, PathBuf) {
        let (pipeline, intermediate, complete) = audited_pipeline(temp).await;
        (Self(pipeline), intermediate, complete)
    }

    pub fn staging_dir(&self, id: JobId) -> PathBuf {
        self.0
            .complete_dir
            .join(".weaver-staging")
            .join(id.0.to_string())
    }

    pub fn make_job_staging_dir(dir: &Path, id: JobId) -> std::io::Result<()> {
        crate::jobs::working_dir::make_job_staging_dir(dir, id)
    }

    pub fn insert_history_with_nzb(&self, row: &crate::JobHistoryRow, nzb: &[u8]) {
        insert_history_row_with_nzb_zstd(&self.0.db, row, nzb);
    }

    pub fn in_history(&self, id: JobId) -> bool {
        self.0.db.get_job_history(id.0).unwrap().is_some()
    }

    pub async fn finished_jobs(&mut self) -> Vec<JobInfo> {
        self.0.finished_jobs().await
    }

    pub async fn redownload_job(&mut self, id: JobId) -> Result<(), SchedulerError> {
        self.0.redownload_job_with_password(id, None).await
    }

    pub async fn restore_job(&mut self, request: RestoreJobRequest) -> Result<(), SchedulerError> {
        self.0.restore_job(request).await
    }

    pub fn status_failpoint(
        old_post: PostState,
        old_run: RunState,
        new_post: PostState,
        new_run: RunState,
    ) -> Option<&'static str> {
        Pipeline::status_enter_failpoint_for_transition(old_post, old_run, new_post, new_run)
    }

    pub async fn reconcile_job_progress(&mut self, id: JobId) {
        self.0.reconcile_job_progress(id).await;
    }

    pub fn refund_rate(&mut self, bytes: u64) {
        self.0.rate_limiter.refund(bytes);
    }

    pub fn rate_should_wait(&mut self) -> bool {
        self.0.rate_limiter.should_wait()
    }

    pub fn reserve_rate(&mut self, id: SegmentId, bytes: u64) {
        self.0.rate_limit_reservations.insert(id, bytes);
    }

    pub async fn finish_rate_limited_body(&mut self, segment_id: SegmentId, bytes: usize) {
        self.0
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 0,
                segment_id,
                data: Ok(DownloadPayload::Raw(Bytes::from(vec![0; bytes]))),
                attempts: vec![],
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: true,
            })
            .await;
    }

    pub fn force_propagation_delay(&mut self, delay: Duration) {
        self.0.propagation_delay_forced = Some(delay);
    }

    pub fn set_local_propagation_delay(&mut self, delay: Duration) {
        self.0.propagation_delay = delay;
    }

    pub fn propagation_hold_until(&mut self, id: JobId) -> Option<Instant> {
        self.0.propagation_hold_until(id)
    }

    pub fn next_propagation_delay(&self) -> Option<Duration> {
        self.0.next_propagation_delay()
    }

    pub fn cached_propagation(&self, id: JobId) -> Option<(Instant, i64)> {
        self.0.propagation_ready_at.get(&id).copied()
    }

    pub fn no_cached_propagation(&self) -> bool {
        self.0.propagation_ready_at.is_empty()
    }

    pub fn elapse_cached_propagation(&mut self, id: JobId, duration: Duration) {
        self.0.propagation_ready_at.get_mut(&id).unwrap().0 -= duration;
    }

    pub fn publish_jobs(&self) {
        self.0.report_all_for_test();
    }

    pub fn list_jobs(&self) -> Vec<JobInfo> {
        self.0.list_jobs()
    }

    pub async fn apply_propagation_setting(&mut self, seconds: u32) {
        self.0.report_all_for_test();
        SchedulerHandle::new(self.0.event_tx.clone(), self.0.shared_state.clone())
            .set_propagation_delay(seconds)
            .await
            .unwrap();
        self.0.apply_queue_intents().await;
    }

    pub fn set_runtime_status(&mut self, id: JobId, status: JobStatus) {
        let state = self.job_mut(id);
        state.status = status;
        state.refresh_runtime_lanes_from_status();
    }
}
