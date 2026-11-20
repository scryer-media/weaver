// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::lane_holdings::LaneLease;
use super::*;
use crate::pipeline::graph::disk::VolumeLeases;
use tokio::sync::oneshot;
use weaver_server_disk::operations::disk::volume::Volume;

pub struct SequentialRefill(oneshot::Receiver<DownloadLaneRefillResponse>);

pub struct RefillAnswer {
    pub lease: Option<LaneLease>,
    reason: LaneParkReason,
}

impl RefillAnswer {
    fn from_reply(reply: DownloadLaneRefillResponse) -> Self {
        Self {
            lease: reply.lease.map(LaneLease),
            reason: reply.park_reason,
        }
    }

    pub fn yielded_to_probe(&self) -> bool {
        self.reason == LaneParkReason::ProbeYield
    }
}

impl SequentialRefill {
    pub async fn answer(self) -> RefillAnswer {
        RefillAnswer::from_reply(self.0.await.unwrap())
    }

    pub fn try_recv(&mut self) -> Result<RefillAnswer, oneshot::error::TryRecvError> {
        self.0.try_recv().map(RefillAnswer::from_reply)
    }
}

pub fn volume_lease_room(volume: &Volume) -> u64 {
    volume.lease_room()
}

impl LaneLease {
    pub fn critical(&self) -> bool {
        self.0.completion_critical
    }

    pub fn effective_excludes(&self) -> weaver_nntp::ServerSet {
        self.0.effective_exclude_servers
    }

    pub fn dial_excludes(&self) -> weaver_nntp::ServerSet {
        self.0.dial_exclude_servers
    }

    pub fn server_depths(&self) -> Vec<(usize, Option<u8>)> {
        self.0
            .server_modes
            .iter()
            .map(|&(server, mode)| {
                (
                    server,
                    match mode {
                        DownloadLaneMode::Sequential => None,
                        DownloadLaneMode::Pipelined { depth } => Some(depth),
                    },
                )
            })
            .collect()
    }
}

// Retag the queue so every remaining item sits at `head_priority`, and drop
// one item at `lane_priority` — the shape a direct-store volume boundary
// leaves behind when a lane's batch ends one volume short of the head.
fn split_queue_priorities(
    pipeline: &mut Pipeline,
    job_id: JobId,
    lane_priority: u32,
    head_priority: u32,
) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let mut works = state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all();
    assert!(
        works.len() >= 2,
        "the boundary case needs a queue with work on both sides"
    );
    let mut lane_sample = works.pop().unwrap();
    lane_sample.priority = lane_priority;
    for work in &mut works {
        work.priority = head_priority;
    }
    for work in works {
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .push(work);
    }
}

// A lane booked as `job_id`'s, holding a connection, so the refill has an
// owner entry to re-book and the connection gauges have something to move.
fn book_connected_lane(pipeline: &mut Pipeline, job_id: JobId, completion_critical: bool) -> u64 {
    let lane_id = Pipeline::next_download_lane_id();
    pipeline.download_lane_owners.insert(
        lane_id,
        DownloadLaneOwner {
            body_activity: Vec::new(),
            job_id,
            mode: DownloadLaneMode::Sequential,
            completion_critical,
            server_idx: Some(0),
            connection: true,
            outstanding: HashMap::new(),
            outstanding_bytes: 0,
        },
    );
    pipeline.active_download_connections = 1;
    *pipeline
        .active_download_connections_by_job
        .entry(job_id)
        .or_default() = 1;
    if completion_critical {
        pipeline.book_completion_critical_connection(job_id);
    }
    lane_id
}

// Splits the queue so the completion-critical heap holds `critical` items and
// the ordinary heap the rest.
fn split_queue_classes(pipeline: &mut Pipeline, job_id: JobId, critical: usize) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let mut works = state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all();
    assert!(
        works.len() > critical,
        "the class split needs work left over for the payload lane"
    );
    for work in works.iter_mut().take(critical) {
        work.completion_critical = true;
    }
    for work in works {
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .push(work);
    }
}

impl DispatchFixture {
    pub fn split_queue_priorities(&mut self, id: JobId, lane: u32, head: u32) {
        split_queue_priorities(&mut self.0, id, lane, head);
    }

    pub fn split_queue_classes(&mut self, id: JobId, critical: usize) {
        split_queue_classes(&mut self.0, id, critical);
    }

    pub fn ask_sequential_refill(&mut self, lane_id: u64) -> SequentialRefill {
        let (response_tx, response_rx) = oneshot::channel();
        self.0
            .handle_download_lane_refill_request(DownloadLaneRefillRequest {
                lane_id,
                runtime_generation: 0,
                server_idx: 0,
                supports_pipelining: false,
                current_mode: DownloadLaneMode::Sequential,
                response_tx,
            });
        SequentialRefill(response_rx)
    }

    pub fn book_connected_lane(&mut self, id: JobId, critical: bool) -> u64 {
        book_connected_lane(&mut self.0, id, critical)
    }

    pub fn park_no_work(&mut self, id: JobId, release: bool) {
        self.0.handle_download_lane_parked(DownloadLaneParked {
            lane_id: 0,
            job_id: id,
            mode: DownloadLaneMode::Sequential,
            completion_critical: false,
            reason: LaneParkReason::NoWork,
            release_connection_slot: release,
        });
    }

    pub fn set_dispatch_wake(&mut self, wake: bool) {
        self.0.download_dispatch_wake = wake;
    }

    pub fn dispatch_wake(&self) -> bool {
        self.0.download_dispatch_wake
    }

    pub fn change_sequential_lane_mode(&mut self, depth: u8) {
        self.0.note_download_lane_mode_changed(
            DownloadLaneMode::Sequential,
            DownloadLaneMode::Pipelined { depth },
        );
    }

    pub fn start_sequential_lane(&mut self) {
        self.0
            .note_download_lane_started(DownloadLaneMode::Sequential);
    }

    pub fn release_sequential_lane(&mut self) {
        self.0
            .note_download_lane_released(DownloadLaneMode::Sequential, LaneParkReason::NoWork);
    }

    pub fn active_critical_connections(&self) -> usize {
        self.0.active_completion_critical_connections
    }

    pub fn active_job_critical_connections(&self, id: JobId) -> Option<usize> {
        self.0
            .active_completion_critical_connections_by_job
            .get(&id)
            .copied()
    }

    pub fn hot_job(&self) -> Option<JobId> {
        self.0.current_hot_job()
    }

    pub fn lane_owner_job(&self, lane: u64) -> JobId {
        self.0.download_lane_owners[&lane].job_id
    }

    pub fn lane_holdings(&self, lane: u64) -> usize {
        self.0.download_lane_holdings(lane)
    }

    pub fn held_refills(&self) -> usize {
        self.0.held_download_refills.len()
    }

    pub fn remove_lane_outstanding(&mut self, lane: u64, segment: SegmentId) {
        self.0
            .download_lane_owners
            .get_mut(&lane)
            .unwrap()
            .outstanding
            .remove(&segment);
    }

    pub fn note_arrived_result(&mut self, id: JobId, bytes: u64) {
        self.0.note_arrived_result_for_test(id, bytes);
    }

    pub fn hand_on_result(&mut self, id: JobId, bytes: u64) {
        self.0.hand_on_result_for_test(id, bytes);
    }

    pub fn volume_holds_budget(&self, id: JobId) -> bool {
        self.0.volume_holds_budget(id)
    }

    pub fn note_all_lane_results_arrived(&mut self, lane: u64, job: JobId) {
        let segments: Vec<_> = self.0.download_lane_owners[&lane]
            .outstanding
            .values()
            .map(|work| (work.segment_id, u64::from(work.byte_estimate)))
            .collect();
        for (segment, bytes) in segments {
            assert!(self.0.accept_lane_result(lane, segment, bytes));
            self.0.note_released_download_result_pending(job, bytes);
        }
    }

    pub async fn add_job_on_another_device(
        &mut self,
        id: JobId,
        spec: JobSpec,
        elsewhere: PathBuf,
    ) {
        let intermediate = std::mem::replace(&mut self.0.intermediate_dir, elsewhere);
        self.add_job(id, spec).await;
        self.0.intermediate_dir = intermediate;
    }

    pub fn room_held_refills(&self) -> usize {
        self.0.room_held_download_refills_for_test()
    }

    pub fn set_waiting_probe(&mut self, server: usize) {
        self.0.probe_waits_on_for_test = Some(server);
    }

    pub fn request_job_cancel(&self, id: JobId) -> bool {
        self.0.report_all_for_test();
        self.0
            .shared_state
            .queue
            .cancel_jobs(
                [id].into_iter().collect(),
                crate::jobs::handle::CancellationOrigin::User,
            )
            .iter()
            .all(|(_, result)| result.is_ok())
    }

    pub async fn finish_requested_cancellations(&mut self) {
        self.0.apply_queue_intents().await;
        finish_cancel_jobs(&mut self.0).await;
    }

    pub fn age_held_refills_past_idle_window(&mut self) {
        self.0.age_held_download_refills_past_idle_window_for_test();
    }
}
