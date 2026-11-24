// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent client, and the speed test whose runs share its sessions'
//! routes and fakes: both over the pipeline they feed.

// Pinned as blanket in the route guard, as the pipeline's is: one lint
// carries both the route ban and the writer gate, and the tests moved here
// with the torrent client and its speed test write their own rows and dial
// their own loopback fixtures.
#![cfg_attr(
    test,
    allow(
        clippy::disallowed_methods,
        reason = "tests write their own fixture rows and dial their own loopback fixtures"
    )
)]
#![deny(clippy::await_holding_lock)]

use weaver_server_composition::{
    Database, LoadError, SchedulerError, StateError, operations, persistence, post_processing,
    proxies, tags,
};
use weaver_server_orchestration::{bandwidth, history, ingest, servers};
use weaver_server_pipeline::{SchedulerHandle, jobs, pipeline, settings};

#[cfg(test)]
use weaver_server_composition::{PipelineMetrics, RssSeenDecision};
#[cfg(test)]
use weaver_server_orchestration::{rss, watch_folder};
#[cfg(test)]
use weaver_server_pipeline::SharedPipelineState;

pub mod speed_test;
pub mod torrents;

#[cfg(test)]
mod tests;
