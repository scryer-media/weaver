// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::lane_holdings::LaneLease;
use super::*;

impl DispatchFixture {
    pub fn events(&self) -> tokio::sync::broadcast::Receiver<PipelineEvent> {
        self.0.event_tx.subscribe()
    }

    pub fn finish_download_pass(&mut self, id: JobId) {
        self.0.emit_download_finished_if_active(id);
    }

    pub async fn pump_completion(&mut self, id: JobId) {
        self.0.schedule_job_completion_check(id);
        pump_pipeline_runtime_queues(&mut self.0).await;
    }

    pub fn finalizing_download(&self, id: JobId) -> bool {
        self.0.jobs_finalizing_download.contains(&id)
    }

    pub async fn add_delivered_job(
        &mut self,
        id: JobId,
        name: &str,
        filename: &str,
        payload: &[u8],
    ) {
        insert_delivered_job(&mut self.0, id, name, filename, payload).await;
    }

    pub fn push_pending_decode(&mut self, segment_id: SegmentId, raw: Bytes) {
        self.0.pending_decode.push_back(PendingDecodeWork {
            segment_id,
            raw,
            servers: PoolServers::stamped(
                self.0.pool_generation,
                None,
                weaver_nntp::ServerSet::EMPTY,
            ),
        });
    }

    pub fn clear_pending_decode(&mut self) {
        self.0.pending_decode.clear();
    }

    pub fn set_decode_budget(&mut self, bytes: usize) {
        self.0.decode_backlog_budget_bytes = bytes;
    }

    pub fn set_write_budget(&mut self, bytes: usize) {
        self.0.write_backlog_budget_bytes = bytes;
    }

    pub fn hard_pressure_stall_started(&self) -> bool {
        self.0.download_pressure_hard_stall_started_at.is_some()
    }

    pub fn age_hard_pressure_stall(&mut self, by: Duration) {
        if let Some(started) = &mut self.0.download_pressure_hard_stall_started_at {
            *started -= by;
        }
    }

    pub fn refresh_pressure(&mut self) {
        self.0.refresh_download_pressure();
    }

    pub fn within_durable_lead(&self, id: JobId, work: &DownloadWork) -> bool {
        self.0
            .primary_download_within_restart_durable_lead(id, work)
    }

    pub fn set_released_count(&mut self, id: JobId, count: usize) {
        self.0
            .pending_released_download_results_by_job
            .insert(id, count);
    }

    pub fn released_count(&self, id: JobId) -> Option<usize> {
        self.0
            .pending_released_download_results_by_job
            .get(&id)
            .copied()
    }

    pub fn released_bytes(&self, id: JobId) -> Option<u64> {
        self.0
            .pending_released_download_result_bytes_by_job
            .get(&id)
            .copied()
    }

    pub fn set_released_bytes(&mut self, id: JobId, bytes: u64) {
        self.0
            .pending_released_download_result_bytes_by_job
            .insert(id, bytes);
    }

    pub fn note_result_pending(&mut self, id: JobId, bytes: u64) {
        self.0.note_released_download_result_pending(id, bytes);
    }

    pub async fn process_released_body(&mut self, segment_id: SegmentId, raw: Bytes) {
        self.0
            .process_released_download_done(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 0,
                segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: vec![],
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: false,
            })
            .await;
    }

    pub async fn settle_failed_releases(&mut self) {
        settle_failed_releases(&mut self.0).await;
    }

    pub fn held_for_file(&self, id: NzbFileId) -> Option<usize> {
        self.0.articles_in_flight_by_file.get(&id).copied()
    }

    pub fn finish_decoded_lane_batch(
        &mut self,
        segment_id: SegmentId,
        decoded_payload: &[u8],
    ) -> (LaneResults, std::sync::mpsc::Receiver<()>) {
        let (ack, ack_rx) = std::sync::mpsc::sync_channel(1);
        let mut pending = VecDeque::new();
        self.0.handle_owned_download_lane_event(
            OwnedDownloadLaneEvent::BatchComplete {
                lane_id: 0,
                results: vec![DownloadResult {
                    lane_id: 0,
                    job_id: segment_id.file_id.job_id,
                    runtime_generation: 0,
                    segment_id,
                    data: Ok(DownloadPayload::Decoded(DecodeResult {
                        encoding: SegmentEncoding::Yenc,
                        segment_id,
                        raw_size: decoded_payload.len() as u64,
                        yenc_layout: YencLayoutAssertions {
                            file_size: decoded_payload.len() as u64,
                            part: Some(1),
                            total: Some(1),
                            begin: Some(1),
                            end: Some(decoded_payload.len() as u64),
                        },
                        crc_valid: true,
                        part_crc_verified: false,
                        part_crc: par2_rs::checksum::crc32(decoded_payload),
                        truncation_suspected: false,
                        expected_file_crc: None,
                        data: DecodedChunk::from(decoded_payload.to_vec()),
                        yenc_name: "owned.bin".to_string(),
                        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                        segments: Vec::new(),
                    })),
                    attempts: vec![],
                    lane_observation: None,
                    source_server_idx: None,
                    origin: DownloadResultOrigin::NormalPrimary,
                    retry_count: 0,
                    exclude_servers: weaver_nntp::ServerSet::from([]),
                    release_connection_slot: false,
                }],
                unrequested_works: vec![],
                stats: weaver_nntp::blocking::BlockingLaneStats::default(),
                ack: Some(ack),
            },
            &mut pending,
        );

        (LaneResults(pending), ack_rx)
    }

    pub fn return_tail(
        &mut self,
        work: DownloadWork,
    ) -> (LaneResults, std::sync::mpsc::Receiver<()>) {
        let (ack, ack_rx) = std::sync::mpsc::sync_channel(1);
        let mut pending = VecDeque::new();
        self.0.handle_owned_download_lane_event(
            OwnedDownloadLaneEvent::BatchComplete {
                lane_id: 0,
                results: vec![],
                unrequested_works: vec![work],
                stats: weaver_nntp::blocking::BlockingLaneStats::default(),
                ack: Some(ack),
            },
            &mut pending,
        );
        (LaneResults(pending), ack_rx)
    }

    pub fn model_uu_file(&mut self, id: NzbFileId) {
        self.0.uu_files.insert(id, UuFileAssembly::default());
    }

    pub fn set_uu_spool_max_segments(&mut self, count: usize) {
        self.0.uu_spool_max_segments = count;
    }

    pub fn set_uu_available_bytes(&mut self, bytes: u64) {
        self.0.uu_spool_available_bytes_for_test = Some(Some(bytes));
    }

    pub fn uu_spool_capped(&mut self) -> bool {
        self.0.uu_spool_admission_capped(0)
    }

    pub fn cut_server_lease(&mut self, server: usize) -> Option<LaneLease> {
        self.0.lease_for_server_for_test(server).map(LaneLease)
    }

    pub fn hold_lane_work(&mut self, work: Vec<DownloadWork>, connection: bool) {
        self.0.hold_test_lane_works(work, connection);
    }

    pub async fn settle_lane_dials(&mut self) {
        settle_lane_dials(&mut self.0).await;
    }

    pub fn finish_test_lanes(&mut self) {
        self.0.finish_test_lanes();
    }

    pub fn set_job_critical_connections(&mut self, id: JobId, count: usize) {
        self.0
            .active_completion_critical_connections_by_job
            .insert(id, count);
    }

    pub fn server_refill_want(&mut self, server: usize) -> usize {
        let pressure = self.0.refresh_download_pressure();
        let mode = self.0.download_lane_mode_for_server(server, pressure, true);
        self.0.download_refill_want(mode)
    }

    pub fn seat_idle_lanes(&mut self, id: JobId, count: usize, servers: usize) {
        for index in 0..count {
            self.0.download_lane_owners.insert(
                Pipeline::next_download_lane_id(),
                DownloadLaneOwner {
                    body_activity: Vec::new(),
                    job_id: id,
                    mode: DownloadLaneMode::Sequential,
                    completion_critical: false,
                    server_idx: Some(index % servers),
                    connection: true,
                    outstanding: HashMap::new(),
                    outstanding_bytes: 0,
                },
            );
        }
        self.set_active_connections(count);
        self.set_job_active_connections(id, count);
    }

    pub fn hold_critical_segment(&mut self, segment: SegmentId) {
        let mut work = super::results::held_works(&[segment]);
        work[0].completion_critical = true;
        self.0.hold_test_lane_works(work, true);
    }
}
