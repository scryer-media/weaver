// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{DispatchFixture, Duration, Instant, NntpClient};
use crate::jobs::ServerTransportHealth;
use crate::pipeline::download::{DownloadPressure, transport::DownloadLaneMode};

pub const UNCLEAN_BURST_WINDOW: Duration =
    crate::pipeline::download::transport::UNCLEAN_BURST_WINDOW;

#[derive(Clone, Copy)]
pub struct Pressure(DownloadPressure);

impl DispatchFixture {
    pub fn seed_explorers(&mut self, client: NntpClient) {
        self.0.nntp = std::sync::Arc::new(client);
        self.0.seed_download_lane_explorers();
    }

    pub fn pressure(&mut self) -> Pressure {
        Pressure(self.0.refresh_download_pressure())
    }

    /// The leased pipeline depth, or None for a sequential lease.
    pub fn pipelined_depth(
        &self,
        server: usize,
        pressure: Pressure,
        negotiated: bool,
    ) -> Option<u8> {
        match self
            .0
            .download_lane_mode_for_server(server, pressure.0, negotiated)
        {
            DownloadLaneMode::Sequential => None,
            DownloadLaneMode::Pipelined { depth } => Some(depth),
        }
    }

    pub fn note_timing(&mut self, server: usize, latency: Duration, transfer: Duration) {
        let explorer = self
            .0
            .download_lane_runtime
            .servers
            .get_mut(&server)
            .expect("server is seeded");
        explorer.note_latency(latency);
        explorer.note_transfer(transfer);
    }

    pub fn note_unclean(&mut self, server: usize, at: Instant) {
        self.0
            .download_lane_runtime
            .servers
            .get_mut(&server)
            .expect("server is seeded")
            .note_unclean_batch(at);
    }

    pub fn pinned(&self, server: usize) -> bool {
        self.0.download_lane_runtime.servers[&server].pinned_sequential()
    }

    pub fn client(&self) -> &NntpClient {
        &self.0.nntp
    }

    pub fn transport_health(&mut self) -> Vec<ServerTransportHealth> {
        self.0.publish_download_transport_health();
        self.0.shared_state.download_transport_health()
    }
}
