// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub struct LaneLease(pub(super) DownloadBatchLease);

impl LaneLease {
    pub fn job_id(&self) -> JobId {
        self.0.job_id
    }

    pub fn id(&self) -> u64 {
        self.0.lane_id
    }

    pub fn works(&self) -> &[DownloadWork] {
        &self.0.works
    }

    pub fn works_mut(&mut self) -> &mut Vec<DownloadWork> {
        &mut self.0.works
    }
}

// Lease `take` of the job's queued articles to a fresh lane, booked the way a
// dispatch pass books it.
fn lease_to_new_lane(pipeline: &mut Pipeline, job_id: JobId, take: usize) -> DownloadBatchLease {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let works: Vec<DownloadWork> = (0..take)
        .map(|_| {
            state
                .articles
                .active_mut()
                .unwrap()
                .download_queue
                .pop()
                .expect("the job has queued work")
                .dispatched(&state.spec)
        })
        .collect();
    lease_works_to_new_lane(pipeline, job_id, works)
}

fn lease_works_to_new_lane(
    pipeline: &mut Pipeline,
    job_id: JobId,
    works: Vec<DownloadWork>,
) -> DownloadBatchLease {
    let lease = DownloadBatchLease {
        body_activity: Arc::default(),
        lane_id: Pipeline::next_download_lane_id(),
        job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: vec![(0, DownloadLaneMode::Sequential)],
        completion_critical: false,
        effective_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        dial_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        checkpoint_plans: Default::default(),
        pressure_clear: true,
        repeated: None,
        works,
    };
    pipeline.activate_download_batch_lease(&lease, &Pipeline::activation_items(&lease), true);
    lease
}

// A transport failure for `segment_id` from `lane_id`, which keeps its
// connection for the rest of its batch.
fn transport_failure(lane_id: u64, segment_id: SegmentId) -> DownloadResult {
    DownloadResult {
        lane_id,
        job_id: segment_id.file_id.job_id,
        runtime_generation: 0,
        segment_id,
        data: Err(DownloadError::fetch(
            DownloadFailureKind::EstablishedTransport,
            "connection reset",
        )),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        release_connection_slot: false,
    }
}

// The whole of a one-part file, already decoded by `lane_id`.
fn decoded_whole_file(
    lane_id: u64,
    segment_id: SegmentId,
    filename: &str,
    payload: &[u8],
) -> DownloadResult {
    DownloadResult {
        lane_id,
        job_id: segment_id.file_id.job_id,
        runtime_generation: 0,
        segment_id,
        data: Ok(DownloadPayload::Decoded(DecodeResult {
            encoding: SegmentEncoding::Yenc,
            segment_id,
            raw_size: payload.len() as u64,
            yenc_layout: YencLayoutAssertions {
                file_size: payload.len() as u64,
                part: Some(1),
                total: Some(1),
                begin: Some(1),
                end: Some(payload.len() as u64),
            },
            crc_valid: true,
            part_crc_verified: false,
            part_crc: par2_rs::checksum::crc32(payload),
            truncation_suspected: false,
            expected_file_crc: None,
            data: DecodedChunk::from(payload.to_vec()),
            yenc_name: filename.to_string(),
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            segments: Vec::new(),
        })),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        release_connection_slot: true,
    }
}

fn batch_complete(
    lane_id: u64,
    results: Vec<DownloadResult>,
    unrequested_works: Vec<DownloadWork>,
) -> (OwnedDownloadLaneEvent, std::sync::mpsc::Receiver<()>) {
    let (ack, ack_rx) = std::sync::mpsc::sync_channel(1);
    (
        OwnedDownloadLaneEvent::BatchComplete {
            lane_id,
            results,
            unrequested_works,
            stats: weaver_nntp::blocking::BlockingLaneStats::default(),
            ack: Some(ack),
        },
        ack_rx,
    )
}

impl DispatchFixture {
    pub fn pop_download_work(&mut self, id: JobId) -> DownloadWork {
        let state = self.job_mut(id);
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .pop()
            .unwrap()
            .dispatched(&state.spec)
    }

    pub fn lease_to_new_lane(&mut self, id: JobId, take: usize) -> LaneLease {
        LaneLease(lease_to_new_lane(&mut self.0, id, take))
    }

    pub fn lease_works_to_new_lane(&mut self, id: JobId, works: Vec<DownloadWork>) -> LaneLease {
        LaneLease(lease_works_to_new_lane(&mut self.0, id, works))
    }

    pub fn restore_stopped_lease(&mut self, lease: LaneLease) {
        self.0.restore_stopped_owned_lane_lease(lease.0);
    }

    pub fn next_lane_id() -> u64 {
        Pipeline::next_download_lane_id()
    }

    pub fn lane_owned(&self, lane: u64) -> bool {
        self.0.download_lane_owners.contains_key(&lane)
    }

    pub fn active_connections(&self) -> usize {
        self.0.active_download_connections
    }

    pub fn in_flight(&self, id: JobId) -> usize {
        self.0
            .articles_in_flight_by_job
            .get(&id)
            .copied()
            .unwrap_or(0)
    }

    pub fn lane_holdings_discrepancies(&self) -> Vec<String> {
        self.0.lane_holdings_discrepancies()
    }

    pub fn count_unheld_article(&mut self, id: JobId) {
        self.0.articles_in_flight += 1;
        *self.0.articles_in_flight_by_job.entry(id).or_default() += 1;
    }

    pub fn mark_idle_past_stall(&mut self, id: JobId) {
        self.0.job_last_download_activity.insert(
            id,
            std::time::Instant::now() - STALLED_DOWNLOAD_IDLE_THRESHOLD - Duration::from_secs(1),
        );
    }

    pub fn pause_stalled_downloads(&mut self) {
        self.0.auto_pause_stalled_downloads();
    }

    pub fn pause_job_runtime(&mut self, id: JobId) -> Result<(), SchedulerError> {
        self.0.pause_job_runtime(id)
    }

    pub fn resume_job_runtime(&mut self, id: JobId) -> Result<(), SchedulerError> {
        self.0.resume_job_runtime(id)
    }

    pub fn pending_download_work(&self, id: JobId) -> bool {
        self.0.job_has_pending_download_pipeline_work(id)
    }

    pub fn clear_completion_checks(&mut self) {
        self.0.pending_completion_checks.clear();
    }

    pub fn completion_checks(&self) -> Vec<JobId> {
        self.0.pending_completion_checks.iter().copied().collect()
    }

    pub fn rebook_sequential_lane(&mut self, lane: u64, id: JobId, works: Vec<DownloadWork>) {
        self.0
            .rebook_download_lane_owner(lane, id, 0, DownloadLaneMode::Sequential, false, works);
    }

    pub async fn fail_transport(&mut self, lane: u64, segment: SegmentId) {
        self.0
            .handle_download_done(transport_failure(lane, segment))
            .await;
    }

    pub async fn download_decoded_file(
        &mut self,
        lane: u64,
        segment: SegmentId,
        filename: &str,
        payload: &[u8],
    ) {
        self.0
            .handle_download_done(decoded_whole_file(lane, segment, filename, payload))
            .await;
    }

    pub fn finish_transport_batch(
        &mut self,
        lane: u64,
        failures: &[SegmentId],
        unrequested: Vec<DownloadWork>,
    ) -> std::sync::mpsc::Receiver<()> {
        let results = failures
            .iter()
            .map(|&segment| transport_failure(lane, segment))
            .collect();
        let (event, ack) = batch_complete(lane, results, unrequested);
        self.0
            .handle_owned_download_lane_event(event, &mut VecDeque::new());
        ack
    }

    pub fn park_on_error(&mut self, lane_id: u64, job_id: JobId) {
        self.0.handle_download_lane_parked(DownloadLaneParked {
            lane_id,
            job_id,
            mode: DownloadLaneMode::Sequential,
            completion_critical: false,
            reason: LaneParkReason::Error,
            release_connection_slot: true,
        });
    }
}
