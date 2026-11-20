// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
pub use crate::pipeline::download::scheduler::{Handout, LaneShare, SaturationWake, YieldReason};
use crate::pipeline::graph::disk::VolumeLeases;
use weaver_server_disk::operations::disk::volume::Volume;

impl DispatchFixture {
    pub fn shared_state(&self) -> &SharedPipelineState {
        &self.0.shared_state
    }
    pub fn schedule_replay_paused(&self) -> bool {
        self.0.shared_state.schedule_replay_paused()
    }
    pub fn set_handoff_draining(&mut self, draining: bool) {
        self.0.nntp_handoff_draining = draining;
    }
    pub fn ask_handout(&mut self, server: usize, want: usize, lane: Option<LaneShare>) -> Handout {
        let pressure = self.0.refresh_download_pressure();
        self.0
            .next_works_for_lane(server, want, lane, None, pressure)
    }
    pub fn set_connection_limit(&mut self, limit: usize) {
        self.0.tuner.set_connection_limit(limit);
    }
    pub fn lane_share_of_job(&self, job: JobId, depth: usize) -> usize {
        self.0.lane_share_of_job(job, depth)
    }
    pub fn set_in_flight_articles(&mut self, id: JobId, count: usize) {
        self.0.articles_in_flight_by_job.insert(id, count);
    }
    pub fn volume(&self, id: JobId) -> Arc<Volume> {
        Arc::clone(
            self.0.graph_jobs[&id]
                .disk()
                .expect("the job writes through its volume's writer")
                .working()
                .volume(),
        )
    }
    pub fn lease_limit(volume: &Volume) -> u64 {
        volume.lease_limit()
    }
    pub fn volume_room(&self, id: JobId) -> Option<u64> {
        self.0.volume_room(id)
    }
    pub fn ask_room_wake(&self, id: JobId) -> bool {
        self.0.ask_room_wake(id)
    }
    pub fn lease_to_lane(&mut self, server: usize, lane: LaneShare, works: Vec<DownloadWork>) {
        let pressure = self.0.refresh_download_pressure();
        let mode = self.0.download_lane_mode_for_server(server, pressure, true);
        self.0
            .lease_for_handout(lane.lane_id, server, mode, pressure, works)
            .expect("the handout is leased");
    }
    // A lane booked as holding `holds` of `job_id`'s articles, at `depth`.
    pub fn lane_holding(
        &mut self,
        server: usize,
        job_id: JobId,
        holds: usize,
        depth: usize,
    ) -> LaneShare {
        let lane_id = Pipeline::next_download_lane_id();
        let outstanding: std::collections::HashMap<SegmentId, DownloadWork> = (0..holds)
            .map(|index| {
                let segment_id = SegmentId {
                    file_id: NzbFileId {
                        job_id,
                        file_index: u32::MAX,
                    },
                    segment_number: index as u32,
                };
                let work = DownloadWork {
                    counters: None,
                    segment_id,
                    message_id: weaver_server_base::jobs::ids::MessageId::new(&format!(
                        "held-{}-{index}@share.test",
                        job_id.0
                    )),
                    groups: Arc::from(Vec::new()),
                    priority: 0,
                    byte_estimate: 512,
                    retry_count: 0,
                    is_recovery: false,
                    completion_critical: false,
                    exclude_servers: weaver_nntp::ServerSet::EMPTY,
                    avoid_server: None,
                };
                (segment_id, work)
            })
            .collect();
        self.0.download_lane_owners.insert(
            lane_id,
            DownloadLaneOwner {
                body_activity: Vec::new(),
                job_id,
                mode: DownloadLaneMode::Sequential,
                completion_critical: false,
                server_idx: Some(server),
                connection: true,
                outstanding_bytes: outstanding
                    .values()
                    .map(|work| u64::from(work.byte_estimate))
                    .sum(),
                outstanding,
            },
        );
        LaneShare { lane_id, depth }
    }
}
