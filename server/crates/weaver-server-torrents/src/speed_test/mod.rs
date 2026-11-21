// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Operator-run speed tests over one consumer's route: a bandwidth pull from
// a public source, or a Usenet pull of a test NZB on the server's own
// connections with the queue paused.

use weaver_server_speedtest::speed_test::adaptive;
mod bandwidth;
pub use weaver_server_composition::speed_test::failure;
#[cfg(any(test, feature = "test-support"))]
pub use weaver_server_composition::speed_test::fixture;
use weaver_server_composition::speed_test::http;
pub use weaver_server_composition::speed_test::model;
mod nearest;
use weaver_server_composition::speed_test::overrides;
use weaver_server_composition::speed_test::progress;
pub use weaver_server_composition::speed_test::providers;
use weaver_server_speedtest::speed_test::runner;
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
pub use overrides::{TestedLeg, TestedRoute};
pub use providers::{PROVIDERS, ProviderInfo, ProviderServer};
#[cfg(any(test, feature = "test-support"))]
pub use runner::HandClock;
pub use runner::RunClock;
pub use service::{
    ServiceInner, SpeedTestError, SpeedTestMetricsSnapshot, SpeedTestService,
    speed_test_metrics_snapshot,
};
pub use torrent::{
    DEBIAN_TORRENTS, P2pOutcome, P2pReport, P2pRung, P2pStep, P2pTest, PeerUpload,
    TestTorrentSource,
};
