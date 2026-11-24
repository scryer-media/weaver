// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::download::scheduler::Handout;

// Hold a job's payload behind the restart checkpoint: an enforced durable
// lead with a progress article already in flight refuses every non-recovery
// article until the pipeline catches up. `false` when the job has no payload
// left to hold back.
pub(in crate::pipeline::tests) fn try_block_on_checkpoint(
    pipeline: &mut Pipeline,
    job_id: JobId,
) -> bool {
    let Some(segment_id) = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .articles
        .active()
        .unwrap()
        .download_queue
        .peek_in_class(false)
        .map(|work| work.segment_id)
    else {
        return false;
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .restored_download_floor_bytes = 1;
    pipeline
        .checkpoint_progress_articles
        .insert(job_id, (0, segment_id));
    true
}

impl DispatchFixture {
    pub async fn scheduler(temp_dir: &TempDir) -> Self {
        Self(new_scheduler_pipeline(temp_dir).await.0)
    }

    pub fn ask_with_spill(
        &mut self,
        server: usize,
        want: usize,
        spill: Option<JobId>,
    ) -> Vec<DownloadWork> {
        let pressure = self.0.refresh_download_pressure();
        match self.0.next_works(server, want, spill, pressure) {
            Handout::Works(works) => works,
            Handout::Idle => Vec::new(),
            Handout::Yield(reason) => panic!("a whole-link gate fired unexpectedly: {reason:?}"),
            Handout::Saturated(wake) => {
                panic!("a caller with no lane share was reported saturated: {wake:?}")
            }
        }
    }

    pub fn metrics(&self) -> &PipelineMetrics {
        &self.0.metrics
    }

    pub fn try_block_on_checkpoint(&mut self, id: JobId) -> bool {
        try_block_on_checkpoint(&mut self.0, id)
    }

    pub fn block_on_checkpoint(&mut self, id: JobId) {
        assert!(
            self.try_block_on_checkpoint(id),
            "the job under test needs queued payload"
        );
    }

    pub fn unblock_checkpoint(&mut self, id: JobId) {
        self.0.checkpoint_progress_articles.remove(&id);
        self.0
            .jobs
            .get_mut(&id)
            .unwrap()
            .restored_download_floor_bytes = 0;
        self.0.download_restart_durable_lead_retry_after.remove(&id);
    }

    pub fn checkpoint_retry_pending(&self, id: JobId) -> bool {
        self.0
            .download_restart_durable_lead_retry_after
            .contains_key(&id)
    }

    pub fn cache_retention_excludes(&mut self, id: JobId, excluded: weaver_nntp::ServerSet) {
        self.0
            .job_retention_exclude_cache
            .get_mut()
            .unwrap()
            .insert(id, (Instant::now(), excluded));
    }

    pub fn set_soft_write_pressure(&self) {
        let (_, _, soft, _) = self.0.download_pressure_limits();
        self.0
            .metrics
            .write_buffered_bytes
            .store(soft, Ordering::Relaxed);
    }

    pub fn has_soft_pressure(&mut self) -> bool {
        self.0.refresh_download_pressure().state == DownloadPressureState::Soft
    }

    pub fn set_fill_servers(&mut self, servers: Option<weaver_nntp::ServerSet>) {
        self.0.download_pass_fill_servers = servers;
    }

    pub fn rotation_alternatives_for_pass(
        ranked: Option<&[usize]>,
        backfill: &[bool],
        idle: &HashMap<usize, usize>,
    ) -> Option<weaver_nntp::ServerSet> {
        Pipeline::rotation_alternatives_for_pass(ranked, backfill, idle)
    }
}
