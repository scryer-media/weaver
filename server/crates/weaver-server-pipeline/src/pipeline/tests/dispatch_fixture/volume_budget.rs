// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::lane_holdings::LaneLease;
use super::*;
use crate::pipeline::graph::disk::VolumeLeases;
use tokio::sync::oneshot;
use weaver_nntp::client::BlockingBodyLaneAcquireError;
use weaver_server_disk::operations::disk::{volume::Volume, writer::Hold};

pub enum VolumeFetchFailure {
    Transport,
    Missing,
}

pub struct VolumeResult(DownloadResult);

pub struct RoomRefill(oneshot::Receiver<DownloadLaneRefillResponse>);

impl RoomRefill {
    pub fn try_recv(&mut self) -> Result<Option<LaneLease>, oneshot::error::TryRecvError> {
        self.0.try_recv().map(|answer| answer.lease.map(LaneLease))
    }
}

pub fn volume_lease_limit(volume: &Volume) -> u64 {
    volume.lease_limit()
}

// The decoded result of `work`, from `lane_id`: the article whole, at its
// place in its file.
fn arrived(
    pipeline: &Pipeline,
    lane_id: u64,
    work: &DownloadWork,
    filename: String,
    bytes: u32,
) -> DownloadResult {
    let segment_id = work.segment_id;
    let file = pipeline.jobs[&segment_id.file_id.job_id]
        .articles
        .assembly()
        .and_then(|assembly| assembly.file(segment_id.file_id))
        .expect("active test file assembly");
    let offset = u64::from(segment_id.segment_number) * u64::from(bytes);
    let payload = vec![segment_id.segment_number as u8; bytes as usize];
    DownloadResult {
        lane_id,
        job_id: segment_id.file_id.job_id,
        runtime_generation: 0,
        segment_id,
        data: Ok(DownloadPayload::Decoded(DecodeResult {
            encoding: SegmentEncoding::Yenc,
            segment_id,
            raw_size: u64::from(bytes),
            yenc_layout: YencLayoutAssertions {
                file_size: file.total_bytes(),
                part: Some(segment_id.segment_number + 1),
                total: Some(file.total_segments()),
                begin: Some(offset + 1),
                end: Some(offset + u64::from(bytes)),
            },
            crc_valid: true,
            part_crc_verified: false,
            part_crc: par2_rs::checksum::crc32(&payload),
            truncation_suspected: false,
            expected_file_crc: None,
            data: DecodedChunk::from(payload),
            yenc_name: filename,
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            segments: Vec::new(),
        })),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        release_connection_slot: false,
    }
}

fn failed(lane_id: u64, work: &DownloadWork, failure: VolumeFetchFailure) -> DownloadResult {
    let kind = match failure {
        VolumeFetchFailure::Transport => DownloadFailureKind::EstablishedTransport,
        VolumeFetchFailure::Missing => DownloadFailureKind::ArticleNotFound,
    };
    DownloadResult {
        lane_id,
        job_id: work.segment_id.file_id.job_id,
        runtime_generation: 0,
        segment_id: work.segment_id,
        data: Err(DownloadError::fetch(kind, "refused")),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        release_connection_slot: false,
    }
}

// A result comes back the way the loop takes one in: released, booked as
// waiting, then handed on.
async fn hand_in(pipeline: &mut Pipeline, result: DownloadResult) {
    if !pipeline.release_download_result(&result) {
        return;
    }
    pipeline.note_released_download_result_pending(
        result.job_id,
        Pipeline::released_download_result_lead_bytes(&result),
    );
    pipeline.process_released_download_done(result).await;
}

// Wait until the volume's writer has written everything the job queued.
fn written(pipeline: &Pipeline, job_id: JobId) {
    pipeline.graph_jobs[&job_id]
        .disk()
        .unwrap()
        .barrier()
        .wait();
}

fn batch_complete(
    lane_id: u64,
    results: Vec<DownloadResult>,
    unrequested_works: Vec<DownloadWork>,
) -> OwnedDownloadLaneEvent {
    OwnedDownloadLaneEvent::BatchComplete {
        lane_id,
        results,
        unrequested_works,
        stats: weaver_nntp::blocking::BlockingLaneStats::default(),
        ack: None,
    }
}

// A lane booked on server 0, holding a connection, with nothing held.
fn book_lane(pipeline: &mut Pipeline, job_id: JobId) -> u64 {
    let lane_id = Pipeline::next_download_lane_id();
    pipeline.download_lane_owners.insert(
        lane_id,
        DownloadLaneOwner {
            body_activity: Vec::new(),
            job_id,
            mode: DownloadLaneMode::Pipelined { depth: 8 },
            completion_critical: false,
            server_idx: Some(0),
            connection: true,
            outstanding: HashMap::new(),
            outstanding_bytes: 0,
        },
    );
    pipeline.active_download_connections += 1;
    *pipeline
        .active_download_connections_by_job
        .entry(job_id)
        .or_default() += 1;
    lane_id
}

fn ask(pipeline: &mut Pipeline, lane_id: u64) -> RoomRefill {
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        lane_id,
        runtime_generation: 0,
        server_idx: 0,
        supports_pipelining: true,
        current_mode: DownloadLaneMode::Pipelined { depth: 8 },
        response_tx,
    });
    RoomRefill(response_rx)
}

impl DispatchFixture {
    pub async fn with_backlog(temp: &TempDir, base: usize, connections: usize) -> Self {
        Self(
            audited_pipeline_with_backlog(temp, base, connections)
                .await
                .0,
        )
    }

    pub async fn with_threaded_writers(temp: &TempDir, base: usize, connections: usize) -> Self {
        crate::pipeline::graph::disk::threads::with_real_writers(Self::with_backlog(
            temp,
            base,
            connections,
        ))
        .await
    }

    pub fn set_volume_budget(&self, hard: u64) {
        self.0.volume_buffers.set_figure(hard, u64::MAX);
    }

    pub fn lane_and_volume_reservations(&self) -> (u64, u64) {
        let held = self
            .0
            .download_lane_owners
            .values()
            .flat_map(|owner| owner.outstanding.values())
            .map(|work| Pipeline::reserved_bytes_of(work.byte_estimate))
            .sum();
        let reserved = self
            .0
            .volume_reserved_by_job
            .values()
            .map(|row| row.bytes())
            .sum();
        (held, reserved)
    }

    pub fn volume_reservations_empty(&self) -> bool {
        self.0.volume_reserved_by_job.is_empty()
    }

    pub fn volume_arrived_empty(&self) -> bool {
        self.0.volume_arrived_by_job.is_empty()
    }

    pub fn activate_lease(&mut self, lease: &LaneLease) {
        self.0
            .activate_download_batch_lease(&lease.0, &Pipeline::activation_items(&lease.0), true);
    }

    pub fn release_lane_reservation(&mut self, work: &DownloadWork) {
        self.0.release_lane_work(work);
    }

    pub fn reserve_lane_work(&mut self, work: &DownloadWork) -> bool {
        self.0.reserve_lane_work(work)
    }

    pub fn arrived_volume_result(
        &self,
        lane: u64,
        work: &DownloadWork,
        filename: String,
        bytes: u32,
    ) -> VolumeResult {
        VolumeResult(arrived(&self.0, lane, work, filename, bytes))
    }

    pub fn failed_volume_result(
        lane: u64,
        work: &DownloadWork,
        kind: VolumeFetchFailure,
    ) -> VolumeResult {
        VolumeResult(failed(lane, work, kind))
    }

    pub async fn handle_volume_result(&mut self, result: VolumeResult) {
        self.0.handle_download_done(result.0).await;
    }

    pub async fn hand_in_volume_result(&mut self, result: VolumeResult) {
        hand_in(&mut self.0, result.0).await;
    }

    pub fn wait_for_volume_writes(&self, id: JobId) {
        written(&self.0, id);
    }

    pub fn release_waiting_volume_result(&mut self, result: &VolumeResult) -> bool {
        if !self.0.release_download_result(&result.0) {
            return false;
        }
        self.0.note_released_download_result_pending(
            result.0.job_id,
            Pipeline::released_download_result_lead_bytes(&result.0),
        );
        true
    }

    pub async fn process_released_volume_result(&mut self, result: VolumeResult) {
        self.0.process_released_download_done(result.0).await;
    }

    pub async fn process_released_volume_batch(&mut self, pending: LaneResults) {
        for result in pending.0 {
            self.0.process_released_download_done(result).await;
        }
    }

    pub fn finish_volume_batch(
        &mut self,
        lane: u64,
        results: Vec<VolumeResult>,
        unrequested: Vec<DownloadWork>,
    ) -> LaneResults {
        let mut pending = VecDeque::new();
        self.0.handle_owned_download_lane_event(
            batch_complete(
                lane,
                results.into_iter().map(|result| result.0).collect(),
                unrequested,
            ),
            &mut pending,
        );
        LaneResults(pending)
    }

    pub fn fail_volume_acquire(
        &mut self,
        lease: LaneLease,
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

    pub fn park_refill_on_error(&mut self, lease: &LaneLease) {
        self.0.handle_download_lane_parked(DownloadLaneParked {
            lane_id: lease.id(),
            job_id: lease.job_id(),
            mode: lease.0.lane_mode,
            completion_critical: false,
            reason: LaneParkReason::Error,
            release_connection_slot: true,
        });
    }

    pub fn move_job_volume(&mut self, id: JobId, volume: &Arc<Volume>) {
        let submitter = self.0.volume_writers.submitter(volume, id.0);
        self.0
            .graph_jobs
            .get_mut(&id)
            .unwrap()
            .move_disk_for_test(crate::pipeline::graph::disk::JobDisk::new(submitter, None));
    }

    pub fn hold_volume_writer(&self, volume: &Arc<Volume>) -> Hold {
        self.0.volume_writers.hold(volume)
    }

    pub fn book_volume_lane(&mut self, id: JobId) -> u64 {
        book_lane(&mut self.0, id)
    }

    pub fn ask_volume_refill(&mut self, lane: u64) -> RoomRefill {
        ask(&mut self.0, lane)
    }

    pub fn abandon_sequential_refill(&mut self, lane: u64) {
        let (response_tx, response_rx) = oneshot::channel();
        drop(response_rx);
        self.0
            .handle_download_lane_refill_request(DownloadLaneRefillRequest {
                lane_id: lane,
                runtime_generation: 0,
                server_idx: 0,
                supports_pipelining: false,
                current_mode: DownloadLaneMode::Sequential,
                response_tx,
            });
    }

    pub fn service_held_refills(&mut self) {
        self.0.service_held_download_refills();
    }

    pub fn refill_want(&self, lease: &LaneLease) -> usize {
        self.0.download_refill_want(lease.0.lane_mode)
    }
}
