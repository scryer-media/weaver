// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The speed test's runs as the rest of the server sees them, the HTTP legs
// they fetch over, the progress they record, and the test torrent's report.
// The runs themselves live in weaver-server-speedtest.

pub use weaver_server_speedtest::speed_test::failure;
#[cfg(any(test, feature = "test-support"))]
pub mod fixture;
pub mod http;
pub mod model;
pub mod overrides;
pub mod progress;
pub mod providers;
pub mod torrent;

pub use failure::SpeedTestFailure;
pub use model::*;
pub use overrides::{TestedLeg, TestedRoute};
pub use providers::{PROVIDERS, ProviderInfo, ProviderServer};
pub use torrent::{
    DEBIAN_TORRENTS, P2pOutcome, P2pReport, P2pRung, P2pStep, PeerUpload, TestTorrentSource,
};
