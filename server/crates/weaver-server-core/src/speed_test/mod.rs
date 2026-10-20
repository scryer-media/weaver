// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Operator-run speed tests over one consumer's route: a bandwidth pull from
// a public source, or a Usenet pull of a test NZB on the server's own
// connections with the queue paused.

mod adaptive;
mod bandwidth;
mod failure;
#[cfg(any(test, feature = "test-support"))]
pub mod fixture;
mod http;
mod model;
mod natpmp;
mod nearest;
mod overrides;
mod progress;
pub mod providers;
mod runner;
mod service;
pub(crate) mod torrent;
#[cfg(any(test, feature = "test-support"))]
pub mod torrent_fixture;
mod torrent_net;
mod upload;
mod usenet;

#[cfg(test)]
mod tests;

pub use failure::SpeedTestFailure;
pub use model::*;
pub(crate) use natpmp::gateway_of;
pub use overrides::{TestedLeg, TestedRoute};
pub use providers::{PROVIDERS, ProviderInfo, ProviderServer};
pub use service::{
    ServiceInner, SpeedTestError, SpeedTestMetricsSnapshot, SpeedTestService,
    speed_test_metrics_snapshot,
};
pub use torrent::{
    DEBIAN_TORRENTS, P2pOutcome, P2pReport, P2pRung, P2pStep, PeerUpload, TestTorrentSource,
};
