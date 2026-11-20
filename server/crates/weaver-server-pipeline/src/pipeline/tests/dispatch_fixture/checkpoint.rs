// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use tokio::sync::oneshot;

pub async fn restored_with_parked_retries(temp: &TempDir, hot: JobId) -> DispatchFixture {
    let (mut pipeline, _, _) = audited_pipeline_with_backlog(temp, 16 * 1024 * 1024, 4).await;
    insert_active_job(
        &mut pipeline,
        hot,
        segmented_job_spec("Checkpoint progress", "hot.bin", &[1024 * 1024; 2048]),
    )
    .await;
    let state = pipeline.jobs.get_mut(&hot).unwrap();
    state.restored_download_floor_bytes = 2904;
    let in_hand = Pipeline::restart_durable_lead_limit_bytes() + 2904 + 1024 * 1024;
    state.downloaded_bytes = in_hand;
    // The gate counts what the job's root holds, each article once.
    pipeline
        .graph_jobs
        .get_mut(&hot)
        .expect("the job runs on the graph")
        .add_in_hand_for_test(in_hand);
    let mut fixture = DispatchFixture(pipeline);
    for _ in 0..130 {
        fixture.park_checkpoint_retry(hot, None);
    }
    fixture
}

impl DispatchFixture {
    pub fn pop_checkpoint_work(&mut self, id: JobId) -> DownloadWork {
        let state = self.job_mut(id);
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .pop_first_matching(|_| true)
            .unwrap()
            .dispatched(&state.spec)
    }

    pub fn park_checkpoint_retry(&mut self, id: JobId, delay: Option<Duration>) {
        let work = self.pop_checkpoint_work(id);
        self.0.note_retry_scheduled(work.segment_id);
        self.0.schedule_infrastructure_retry(
            delay,
            RetryWork {
                scheduled_pool_generation: self.0.pool_generation,
                infrastructure_retry: true,
                hold: crate::pipeline::RetryHold::Ordinary,
                work,
            },
        );
    }
    pub fn held_connections(&self, id: JobId) -> Option<usize> {
        self.0.active_download_connections_by_job.get(&id).copied()
    }
    pub fn tracked_in_flight(&self, id: JobId) -> Option<usize> {
        self.0.articles_in_flight_by_job.get(&id).copied()
    }
    pub fn hold_downloads(&self, paused: bool) {
        if paused {
            self.0
                .shared_state
                .hold(crate::jobs::Hold::manual(crate::jobs::HoldOwner::All));
        } else {
            self.0
                .shared_state
                .release(crate::jobs::HoldOrigin::Manual, crate::jobs::HoldOwner::All);
        }
    }
    pub fn schedule_checkpoint(&mut self, id: JobId, at: Instant) {
        self.0
            .download_restart_durable_lead_retry_after
            .insert(id, at);
    }
    pub fn checkpoint_delay(&self) -> Option<Duration> {
        self.0.next_restart_durable_lead_retry_delay()
    }
    pub fn consume_due_checkpoints(&mut self) {
        self.0.consume_due_checkpoint_rechecks();
    }
    pub async fn pause_all_through_queue(&mut self, paused: bool) {
        self.0.report_all_for_test();
        let handle = SchedulerHandle::new(self.0.event_tx.clone(), self.0.shared_state.clone());
        if paused {
            handle.pause_all().await.unwrap();
        } else {
            handle.resume_all().await.unwrap();
        }
        self.0.apply_queue_intents().await;
    }
    pub fn no_checkpoint_rechecks(&self) -> bool {
        self.0.download_restart_durable_lead_retry_after.is_empty()
    }
    pub fn checkpoint_booking(&self, id: JobId) -> Option<(u64, SegmentId)> {
        self.0.checkpoint_progress_articles.get(&id).copied()
    }
    pub fn outstanding_work(&self, lane: u64, segment: SegmentId) -> &DownloadWork {
        &self.0.download_lane_owners[&lane].outstanding[&segment]
    }
    pub fn rate_reservations(&self) -> &HashMap<SegmentId, u64> {
        &self.0.rate_limit_reservations
    }
    pub fn set_rate(&mut self, rate: Option<u64>) {
        self.0.rate_limiter.set_rate(rate);
    }
    pub fn charge_rate(&mut self, bytes: u64) {
        self.0.rate_limiter.consume(bytes);
    }
    pub fn rate_ready_in(&mut self) -> Duration {
        self.0.rate_limiter.time_until_ready()
    }
    pub fn has_download_activity(&self, id: JobId) -> bool {
        self.0.job_last_download_activity.contains_key(&id)
    }
    pub async fn finish_retired_result(
        &mut self,
        lane: u64,
        work: &DownloadWork,
        answer: Result<&'static [u8], &str>,
        releases: bool,
    ) {
        let data = answer
            .map(|bytes| DownloadPayload::Raw(Bytes::from_static(bytes)))
            .map_err(|error| {
                DownloadError::fetch(DownloadFailureKind::EstablishedTransport, error)
            });
        let segment_id = work.segment_id;
        self.0
            .handle_download_done(DownloadResult {
                lane_id: lane,
                job_id: segment_id.file_id.job_id,
                segment_id,
                runtime_generation: self.0.pool_generation,
                data,
                attempts: vec![],
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: work.retry_count,
                exclude_servers: work.exclude_servers,
                release_connection_slot: releases,
            })
            .await;
    }
    pub async fn refill_sequential_lane(&mut self, lane_id: u64) -> Option<Vec<DownloadWork>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.0
            .handle_download_lane_refill_request(DownloadLaneRefillRequest {
                lane_id,
                runtime_generation: self.0.pool_generation,
                server_idx: 0,
                supports_pipelining: true,
                current_mode: DownloadLaneMode::Sequential,
                response_tx,
            });
        response_rx.await.unwrap().lease.map(|lease| lease.works)
    }
    pub fn return_unrequested(&mut self, lane_id: u64, work: DownloadWork) {
        self.0.handle_owned_download_lane_event(
            OwnedDownloadLaneEvent::BatchComplete {
                lane_id,
                results: vec![],
                unrequested_works: vec![work],
                stats: Default::default(),
                ack: None,
            },
            &mut VecDeque::new(),
        );
    }
    pub fn accept_unavailable_result(
        &mut self,
        lane_id: u64,
        segment_id: SegmentId,
    ) -> ReleasedResults {
        let mut pending = VecDeque::new();
        self.0.handle_owned_download_lane_event(
            OwnedDownloadLaneEvent::BatchComplete {
                lane_id,
                results: vec![DownloadResult {
                    lane_id,
                    job_id: segment_id.file_id.job_id,
                    segment_id,
                    runtime_generation: self.0.pool_generation,
                    data: Err(DownloadError::fetch(
                        DownloadFailureKind::LaneUnavailable,
                        "accepted before retirement",
                    )),
                    attempts: vec![],
                    lane_observation: None,
                    source_server_idx: None,
                    origin: DownloadResultOrigin::NormalPrimary,
                    retry_count: 0,
                    exclude_servers: weaver_nntp::ServerSet::EMPTY,
                    release_connection_slot: false,
                }],
                unrequested_works: vec![],
                stats: Default::default(),
                ack: None,
            },
            &mut pending,
        );
        ReleasedResults(pending)
    }
    pub async fn process_released_result(&mut self, results: &mut ReleasedResults) {
        self.0
            .process_released_download_done(results.0.pop_front().unwrap())
            .await;
    }
    pub fn retire_stalled_lanes(&mut self, id: JobId) -> usize {
        self.0.retire_stalled_download_lanes(id)
    }
    pub fn has_released_results(&self, id: JobId) -> bool {
        self.0
            .pending_released_download_results_by_job
            .contains_key(&id)
    }
    pub fn has_dispatchable_work(&mut self, id: JobId) -> bool {
        self.0.job_has_dispatchable_work_for_test(id)
    }
    pub fn set_active_decodes(&mut self, id: JobId, count: usize) {
        self.0.active_decodes_by_job.insert(id, count);
    }
    pub fn lease_for_server(&mut self, server: usize) -> Option<Vec<DownloadWork>> {
        self.0
            .lease_for_server_for_test(server)
            .map(|lease| lease.works)
    }
    pub fn diagnostics(&self) -> diagnostics::PipelineDiagnostics {
        self.0.diagnostics_snapshot()
    }
    pub fn durable_lead_limit() -> u64 {
        Pipeline::restart_durable_lead_limit_bytes()
    }
}

pub struct ReleasedResults(VecDeque<DownloadResult>);

impl ReleasedResults {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
