// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::new_direct_pipeline_with_backlog;
use crate::jobs::HoldOrigin;
use crate::proxies::Bandwidth;
use crate::settings::queries::SettingsQueries;
use crate::{Pipeline, SchedulerHandle, SharedPipelineState};
use tempfile::TempDir;

/// An isolated actor whose bandwidth settings can be applied one queue turn at a time.
pub struct BandwidthFixture(Pipeline);

impl BandwidthFixture {
    pub async fn new(temp: &TempDir, backlog: usize, connections: usize) -> Self {
        let (pipeline, _, _) = new_direct_pipeline_with_backlog(temp, backlog, connections).await;
        Self(pipeline)
    }

    pub fn handle(&self) -> SchedulerHandle {
        SchedulerHandle::new(self.0.event_tx.clone(), self.0.shared_state.clone())
    }

    pub fn reported_handle(&self) -> SchedulerHandle {
        self.0.report_all_for_test();
        self.handle()
    }

    pub async fn apply_queue_intents(&mut self) {
        self.0.apply_queue_intents().await;
    }

    pub fn state(&self) -> &SharedPipelineState {
        &self.0.shared_state
    }

    pub fn rate(&self) -> Option<u64> {
        self.0.rate_limiter.rate()
    }

    pub fn scheduled_bandwidth(&self) -> Option<Bandwidth> {
        self.0.scheduled_bandwidth
    }

    pub fn configured_bandwidth(&self) -> Bandwidth {
        self.0.configured_bandwidth
    }

    pub fn global_pause(&self) -> Option<HoldOrigin> {
        self.0.global_pause()
    }

    pub fn persisted_pause(&self) -> Option<String> {
        self.0.db.get_setting("global_paused").unwrap()
    }

    pub fn publish_scheduled_limit(&mut self, limit: Bandwidth) {
        self.0.scheduled_bandwidth = Some(limit);
        self.0.apply_global_rate();
        self.0.publish_download_block();
    }

    pub fn record_bandwidth(&self, bytes: u64) {
        self.0.shared_state.queue.counters.bandwidth.record(bytes);
    }

    pub fn sample_bandwidth(&self) {
        self.0.shared_state.queue.sample_bandwidth(true).unwrap();
    }
}
