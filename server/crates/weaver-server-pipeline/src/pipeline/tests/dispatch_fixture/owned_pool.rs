// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::lane_holdings::LaneLease;
use super::retries::ParkedRetry;
use super::*;

#[derive(Clone, Debug)]
pub struct FetchCause(DownloadError);

impl FetchCause {
    pub fn nntp(error: weaver_nntp::NntpError) -> Self {
        Self(DownloadError::from_nntp(error))
    }

    pub fn unanswered(message: &str) -> Self {
        Self(DownloadError::fetch(
            DownloadFailureKind::Unanswered,
            message,
        ))
    }

    pub fn settled_group_refusal() -> Self {
        let mut failure = DownloadFailure::from_nntp(weaver_nntp::NntpError::NoGroupSelected);
        failure.settle_group_refusal(true, false);
        Self(DownloadError::Fetch(failure))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailureKind(pub(super) DownloadFailureKind);

impl FailureKind {
    pub const CAPACITY_UNAVAILABLE: Self = Self(DownloadFailureKind::CapacityUnavailable);
    pub const CONNECTION_ESTABLISHMENT: Self = Self(DownloadFailureKind::ConnectionEstablishment);
    pub const ESTABLISHED_TRANSPORT: Self = Self(DownloadFailureKind::EstablishedTransport);
    pub const AUTH: Self = Self(DownloadFailureKind::Auth);
    pub const SERVER_QUOTA: Self = Self(DownloadFailureKind::ServerQuota);
    pub const LANE_UNAVAILABLE: Self = Self(DownloadFailureKind::LaneUnavailable);
    pub const ROUTE_CAPACITY: Self = Self(DownloadFailureKind::RouteCapacity);
    pub const UNREQUESTED: Self = Self(DownloadFailureKind::Unrequested);
    pub const UNANSWERED: Self = Self(DownloadFailureKind::Unanswered);
    pub const ARTICLE_NOT_FOUND: Self = Self(DownloadFailureKind::ArticleNotFound);
    pub const CONTENT_OR_PROTOCOL: Self = Self(DownloadFailureKind::ContentOrProtocol);

    pub fn preserves_article_retry_budget(self) -> bool {
        self.0.preserves_article_retry_budget()
    }

    pub fn infrastructure_wait_reason(self) -> Option<&'static str> {
        self.0.infrastructure_wait_reason()
    }
}

pub struct FailureClassification(DownloadFailure);

impl FailureClassification {
    pub fn retry_after(&self) -> Option<Duration> {
        self.0.retry_after
    }

    pub fn for_work(&self, index: usize) -> Self {
        Self(crate::pipeline::download::lane_acquire_failure_for_work(
            &self.0, index,
        ))
    }

    pub fn from_nntp(error: weaver_nntp::NntpError) -> Self {
        Self(DownloadFailure::from_nntp(error))
    }

    pub fn from_lane_acquire_failure(error: Option<&weaver_nntp::NntpError>) -> Self {
        Self(DownloadFailure::from_lane_acquire_failure(error))
    }

    pub fn kind(&self) -> FailureKind {
        FailureKind(self.0.kind)
    }

    pub fn preserves_article_retry_budget(&self) -> bool {
        self.0.preserves_article_retry_budget()
    }
}

impl DispatchFixture {
    pub fn owned_worker_count(&self) -> usize {
        self.0.owned_download_lane_pool.worker_count()
    }

    pub fn max_concurrent_downloads(&self) -> usize {
        self.0.tuner.params().max_concurrent_downloads
    }

    pub fn unbooked_lease(&self, work: DownloadWork) -> LaneLease {
        LaneLease(DownloadBatchLease {
            body_activity: Arc::default(),
            lane_id: 0,
            job_id: work.segment_id.file_id.job_id,
            runtime_generation: self.0.pool_generation,
            lane_mode: DownloadLaneMode::Sequential,
            server_modes: Vec::new(),
            completion_critical: work.completion_critical,
            effective_exclude_servers: weaver_nntp::ServerSet::EMPTY,
            dial_exclude_servers: weaver_nntp::ServerSet::EMPTY,
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            pressure_clear: true,
            repeated: None,
            works: vec![work],
        })
    }

    pub fn spawn_owned_lease(&mut self, lease: LaneLease) {
        self.0.spawn_download_batch(lease.0);
    }

    pub fn refill_want_at_depth(&self, depth: Option<u8>) -> usize {
        let mode = match depth {
            None => DownloadLaneMode::Sequential,
            Some(depth) => DownloadLaneMode::Pipelined { depth },
        };
        self.0.download_refill_want(mode)
    }

    pub fn rate_limited(&self) -> bool {
        self.0.rate_limiter.is_limited()
    }

    pub async fn queue_shutdown_failure(&self, segment_id: SegmentId) {
        self.0
            .download_done_tx
            .send(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 0,
                segment_id,
                data: Err(DownloadError::fetch(
                    DownloadFailureKind::EstablishedTransport,
                    "shutdown test",
                )),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: true,
            })
            .await
            .unwrap();
    }

    pub async fn drain_shutdown_results(&mut self) {
        self.0.drain().await;
    }

    pub async fn return_fetch_cause(
        &mut self,
        segment_id: SegmentId,
        retry_count: u32,
        error: FetchCause,
    ) {
        self.0
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 0,
                segment_id,
                data: Err(error.0),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: Some(0),
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: true,
            })
            .await;
    }

    pub fn transport_streak(&self, id: SegmentId) -> Option<u32> {
        self.0
            .transport_failure_streaks
            .get(&id)
            .map(|streak| streak.failures)
    }
}

impl DispatchFixture {
    pub fn seed_retry_wait(&mut self, job: JobId, count: usize) {
        self.0.pending_retries_by_job.insert(job, count);
        self.0.download_wait_by_job.insert(
            job,
            DownloadWaitStatus {
                reason: "no eligible NNTP server",
                retry_at_epoch_ms: None,
                pending_count: count,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
            },
        );
    }

    pub fn park_generation_retry(&mut self, work: DownloadWork) {
        self.0.pending_retries_by_segment.insert(work.segment_id, 1);
        self.0.schedule_infrastructure_retry(
            None,
            RetryWork {
                scheduled_pool_generation: self.0.pool_generation,
                infrastructure_retry: true,
                hold: crate::pipeline::RetryHold::Ordinary,
                work,
            },
        );
    }

    pub fn no_pending_segment_retries(&self) -> bool {
        self.0.pending_retries_by_segment.is_empty()
    }

    pub fn set_pool_generation(&mut self, generation: u64) {
        self.0.pool_generation = generation;
    }

    pub async fn return_old_transport_failure(&mut self, segment_id: SegmentId, retries: u32) {
        self.0
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 1,
                segment_id,
                data: Err(DownloadError::fetch(
                    DownloadFailureKind::EstablishedTransport,
                    "old generation connection reset",
                )),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: Some(0),
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: retries,
                exclude_servers: weaver_nntp::ServerSet::from([0]),
                release_connection_slot: true,
            })
            .await;
    }

    pub fn release_old_success(&mut self, segment_id: SegmentId) {
        self.0.release_download_result(&DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 1,
            segment_id,
            data: Ok(DownloadPayload::Raw(Bytes::from_static(b"article"))),
            attempts: Vec::new(),
            lane_observation: Some(DownloadLaneObservation {
                server_idx: Some(0),
                mode: DownloadLaneMode::Pipelined { depth: 2 },
                supports_pipelining: true,
                latency_sample: Some(Duration::from_millis(5)),
                cold: false,
                transfer: Some(Duration::from_millis(20)),
                payload_bytes: 7,
                policy_elapsed: Duration::from_millis(25),
                pressure_clear: true,
                fed: false,
                batch_complete: true,
                batch_clean: true,
                unresolved_count: 0,
                connection_discarded: false,
                answered_pipelined: false,
            }),
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            release_connection_slot: true,
        });
    }

    pub async fn next_retry(&mut self) -> ParkedRetry {
        ParkedRetry(
            self.0
                .retry_rx
                .recv()
                .await
                .expect("retry channel must stay open"),
        )
    }

    pub async fn return_acquire_failure(
        &mut self,
        segment_id: SegmentId,
        source: Option<usize>,
        retry_count: u32,
        failure: FailureClassification,
    ) {
        self.0
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 0,
                segment_id,
                data: Err(DownloadError::Fetch(failure.0)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: source,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: false,
            })
            .await;
    }

    pub fn server_quota_parked(&self, id: SegmentId) -> bool {
        self.0.server_quota_parked.contains(&id)
    }

    pub fn remove_job_and_clear_publication(&mut self, job: JobId) {
        self.0.jobs.remove(&job);
        self.0.shared_state.publish_jobs(Vec::new());
    }

    pub fn bandwidth_reservation_estimate(decoded_bytes: u32) -> u64 {
        Pipeline::bandwidth_reservation_estimate(decoded_bytes)
    }

    pub fn release_sequential_lane_for_quota(&mut self) {
        self.0
            .note_download_lane_released(DownloadLaneMode::Sequential, LaneParkReason::ServerQuota);
    }
}
