// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// propagation delay

use super::acquire_failures::AcquireLease;
use super::*;
use crate::{DownloadState, PostState};
use std::sync::atomic::AtomicU64;

pub struct RateCounters(Arc<crate::pipeline::queue::JobCounters>);

impl RateCounters {
    pub fn set_downloaded(&self, bytes: u64) {
        self.0.downloaded.store(bytes, Ordering::Relaxed);
    }

    pub fn set_decoded(&self, bytes: u64) {
        self.0.decoded.store(bytes, Ordering::Relaxed);
    }

    pub fn credit_body(&self, bytes: usize) {
        crate::pipeline::download::owned_lane::credit_transfer(
            &self.0,
            &Ok(DownloadPayload::Raw(vec![0; bytes].into())),
        );
    }
}

impl DispatchFixture {
    pub fn book_lane_zero(&mut self, work: DownloadWork) -> AcquireLease {
        let job_id = work.segment_id.file_id.job_id;
        self.hold_lane_work(vec![work.clone()], false);
        let lease = AcquireLease(DownloadBatchLease {
            body_activity: Arc::default(),
            lane_id: 0,
            job_id,
            runtime_generation: self.0.pool_generation,
            lane_mode: DownloadLaneMode::Sequential,
            server_modes: Vec::new(),
            completion_critical: false,
            effective_exclude_servers: weaver_nntp::ServerSet::EMPTY,
            dial_exclude_servers: weaver_nntp::ServerSet::EMPTY,
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            pressure_clear: true,
            repeated: None,
            works: vec![work],
        });
        self.set_active_connections(1);
        self.set_job_active_connections(job_id, 1);
        self.start_sequential_lane();

        lease
    }

    pub fn lane_reset_calls(&self) -> usize {
        self.0.owned_download_lane_pool.reset_calls()
    }

    pub fn soft_dispatch_after(&self) -> Option<Instant> {
        self.0.download_pressure_soft_dispatch_after
    }

    pub fn set_soft_dispatch_after(&mut self, at: Instant) {
        self.0.download_pressure_soft_dispatch_after = Some(at);
    }

    pub fn clear_active_passes(&mut self) {
        self.0.active_download_passes.clear();
    }

    pub fn clear_in_flight(&mut self, id: JobId) {
        self.0.articles_in_flight_by_job.remove(&id);
    }

    pub fn set_total_in_flight(&mut self, count: usize) {
        self.0.articles_in_flight = count;
    }

    pub fn job_has_decode(&self, id: JobId) -> bool {
        self.0.active_decodes_by_job.get(&id).is_some()
    }

    pub fn job_has_download_activity(&self, id: JobId) -> bool {
        self.0.job_has_current_download_activity(id)
    }

    pub fn publish_job(&self, id: JobId) {
        self.0.report_job(id);
    }

    pub fn begin_download_phase(&mut self, id: JobId, bytes: u64) {
        self.0.phase_begin(id, JobPhase::Downloading, Some(bytes));
    }

    pub fn download_counters(&self, id: JobId) -> RateCounters {
        RateCounters(self.0.shared_state.queue.job_counters(id).unwrap())
    }

    pub fn link_download_counters(&self, id: JobId) -> RateCounters {
        let counters = self.download_counters(id);
        counters
            .0
            .totals
            .get_or_init(|| self.0.shared_state.queue.counters.clone());
        counters
    }

    pub fn sample_counters(&self, at: Instant) {
        self.0
            .shared_state
            .queue
            .sample_counters(&self.0.metrics, &self.0.event_tx, at);
    }

    pub fn record_wire(&self, bytes: u64) {
        self.0.shared_state.queue.counters.bandwidth.record(bytes);
    }

    pub fn mark_graph_volume_arrived(&mut self, id: JobId, file: u32) {
        self.0
            .graph_jobs
            .get_mut(&id)
            .unwrap()
            .arrived_before(|volume, _| volume != weaver_graph::VolumeIndex(file));
    }

    pub fn publish_download_block(&mut self) {
        self.0.publish_download_block();
    }

    pub async fn apply_current_holds(&mut self) {
        self.0.apply_holds(self.0.shared_state.holds()).await;
    }

    pub fn egress_quota_lock_count(&self) -> u64 {
        self.0.shared_state.egress_quota_block_lock_count()
    }

    pub fn refresh_server_quota(&self) {
        self.0.refresh_server_quota_block_presentation();
    }

    pub fn update_queue_metrics(&self) {
        self.0.update_queue_metrics();
    }

    pub fn no_server_quota_parks(&self) -> bool {
        self.0.server_quota_parked.is_empty()
    }

    pub fn add_minimal_job(&mut self, id: JobId, name: &str, folder: PathBuf) {
        self.0.jobs.insert(id, minimal_job_state(id, name, folder));
    }

    pub fn add_extracting_job(
        &mut self,
        id: JobId,
        name: &str,
        folder: PathBuf,
        download: Option<DownloadState>,
    ) {
        let mut state = minimal_job_state(id, name, folder);
        if let Some(download) = download {
            state.download_state = download;
        }
        state.post_state = PostState::Extracting;
        state.refresh_legacy_status();
        self.0.jobs.insert(id, state);
    }

    pub fn install_stalled_lane(&mut self, segment_id: SegmentId) {
        let job_id = segment_id.file_id.job_id;
        self.0.download_lane_owners.insert(
            Pipeline::next_download_lane_id(),
            DownloadLaneOwner {
                body_activity: Vec::new(),
                job_id,
                mode: DownloadLaneMode::Sequential,
                completion_critical: false,
                server_idx: Some(0),
                connection: false,
                outstanding: HashMap::from([(
                    segment_id,
                    DownloadWork {
                        counters: None,
                        segment_id,
                        message_id: weaver_server_base::jobs::ids::MessageId::new(
                            "stalled-article",
                        ),
                        groups: Arc::from(vec![]),
                        priority: 0,
                        byte_estimate: 256,
                        retry_count: 0,
                        is_recovery: false,
                        completion_critical: false,
                        exclude_servers: weaver_nntp::ServerSet::from([]),
                        avoid_server: None,
                    },
                )]),
                outstanding_bytes: 256,
            },
        );
    }

    pub fn install_body_activity_lane(
        &mut self,
        live: JobId,
        stale: JobId,
        bytes: &Arc<AtomicU64>,
        old_bytes: &Arc<AtomicU64>,
    ) {
        self.0.download_lane_owners.insert(
            Pipeline::next_download_lane_id(),
            DownloadLaneOwner {
                body_activity: vec![
                    (live, self.0.pool_generation, Arc::downgrade(bytes)),
                    (
                        stale,
                        self.0.pool_generation.wrapping_add(1),
                        Arc::downgrade(old_bytes),
                    ),
                ],
                job_id: live,
                mode: DownloadLaneMode::Sequential,
                completion_critical: false,
                server_idx: Some(0),
                connection: false,
                outstanding: HashMap::new(),
                outstanding_bytes: 0,
            },
        );
    }
}
