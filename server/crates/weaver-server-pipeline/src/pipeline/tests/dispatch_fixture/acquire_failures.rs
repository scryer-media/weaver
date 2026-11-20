// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::download::owned_lane::TestRoutePress;
use crate::pipeline::download::transport::DownloadLaneMode;
use weaver_nntp::client::BlockingBodyLaneAcquireError;

pub const DISPATCH_STALL_LOG_INTERVAL: Duration =
    crate::pipeline::download::DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
pub const RETRY_STORM_THRESHOLD: u64 = crate::pipeline::download::DOWNLOAD_RETRY_STORM_THRESHOLD;

pub struct AcquireLease(pub(super) DownloadBatchLease);

impl AcquireLease {
    pub fn works(&self) -> &[DownloadWork] {
        &self.0.works
    }
}

pub struct RoutePress(TestRoutePress);

impl RoutePress {
    pub fn stands(&self) -> bool {
        self.0.stands()
    }
}

fn lease_for(pipeline: &mut Pipeline, job_id: JobId) -> DownloadBatchLease {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let works: Vec<_> = state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all()
        .into_iter()
        .map(|item| item.dispatched(&state.spec))
        .collect();
    assert!(!works.is_empty(), "the fixture job has queued work");
    let completion_critical = works.iter().any(|work| work.completion_critical);
    pipeline.hold_test_lane_works(works.clone(), false);
    DownloadBatchLease {
        body_activity: Arc::default(),
        lane_id: 0,
        job_id,
        runtime_generation: 0,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: vec![(0, DownloadLaneMode::Sequential)],
        completion_critical,
        effective_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        dial_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        repeated: None,
        works,
    }
}

impl DispatchFixture {
    pub fn acquire_lease(&mut self, id: JobId) -> AcquireLease {
        AcquireLease(lease_for(&mut self.0, id))
    }

    pub fn acquire_counted_lease(&mut self, id: JobId) -> AcquireLease {
        let mut lease = self.acquire_lease(id);
        let counters = self.0.shared_state.queue.job_counters(id).unwrap();
        for work in &mut lease.0.works {
            work.counters = Some(Arc::clone(&counters));
        }
        lease
    }

    pub fn report_route_press(&self, lease: &AcquireLease) -> RoutePress {
        RoutePress(TestRoutePress::refused(&self.0.shared_state, &lease.0))
    }

    pub fn take_dispatch_wake(&mut self) -> bool {
        self.0.take_download_dispatch_wake()
    }

    pub fn set_active_connections(&mut self, count: usize) {
        self.0.active_download_connections = count;
    }

    pub fn set_job_active_connections(&mut self, id: JobId, count: usize) {
        self.0.active_download_connections_by_job.insert(id, count);
    }

    pub fn refuse_acquire(
        &mut self,
        lease: AcquireLease,
        error: BlockingBodyLaneAcquireError,
    ) -> LaneResults {
        let mut pending = VecDeque::new();
        self.0.handle_owned_download_lane_event(
            OwnedDownloadLaneEvent::AcquireFailed {
                lease: lease.0,
                error,
            },
            &mut pending,
        );
        LaneResults(pending)
    }

    pub async fn hear_refused_acquires(&mut self, mut pending: LaneResults) {
        while let Some(result) = pending.0.pop_front() {
            self.0.handle_download_done(result).await;
        }
    }

    pub async fn refuse_full_route(&mut self, lease: AcquireLease) {
        let id = lease.0.job_id;
        self.0.active_download_connections += 1;
        *self
            .0
            .active_download_connections_by_job
            .entry(id)
            .or_default() += 1;
        let pending = self.refuse_acquire(
            lease,
            BlockingBodyLaneAcquireError::RouteCapacity(weaver_nntp::NntpError::Route(Arc::new(
                weaver_tunnel::pipe::DialError::SessionBudget,
            ))),
        );
        self.hear_refused_acquires(pending).await;
    }

    pub fn last_refusal_log(&self, id: JobId) -> Option<Instant> {
        self.0
            .owned_lane_acquire_failure_log_throttle
            .last_emitted_at(id)
    }

    pub fn wait_reason(&self, id: JobId) -> Option<&'static str> {
        self.0.download_wait_by_job.get(&id).map(|wait| wait.reason)
    }

    pub fn parked_works_with_deadlines(
        &self,
    ) -> impl Iterator<Item = (Option<tokio::time::Instant>, &DownloadWork)> {
        self.0
            .infrastructure_retries
            .iter_with_deadlines()
            .map(|(at, retry)| (at, &retry.work))
    }

    pub fn clear_pending_retries(&mut self, id: JobId) {
        self.0.pending_retries_by_job.remove(&id);
    }

    pub fn publish_download_drained(&self) {
        self.0.publish_download_drained();
    }

    pub fn record_acquire_failure_time(&mut self, at: Instant) {
        self.0.last_owned_lane_acquire_failure_at = Some(at);
    }

    pub fn log_lanes_under_cap(&mut self, at: Instant, limit: usize) {
        self.0.log_download_lanes_under_cap(at, limit);
    }

    pub fn lanes_under_cap_since(&self) -> Option<Instant> {
        self.0.download_lanes_under_cap_since
    }

    pub fn lanes_under_cap_reported(&self) -> bool {
        self.0.download_lanes_under_cap_reported
    }

    pub fn log_retry_storm(&mut self, at: Instant) {
        self.0.log_download_retry_storm(at);
    }

    pub fn retry_storm_active(&self) -> bool {
        self.0.download_retry_storm.is_some()
    }

    pub fn lane_owners_empty(&self) -> bool {
        self.0.download_lane_owners.is_empty()
    }
}
