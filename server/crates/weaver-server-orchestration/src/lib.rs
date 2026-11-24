// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The services that drive the scheduler from outside it: intake
//! submission, feeds, watch folders, the bandwidth schedule, runtime reload,
//! the servers service and the history delete queue.

#![deny(clippy::await_holding_lock)]

// The composed Database, the domain code composed over it and the
// pipeline, under the paths this crate's code names. Imported, not
// re-exported: the facade of the old paths is weaver-server-core's.
use weaver_server_composition::*;
use weaver_server_pipeline::{SchedulerHandle, jobs, settings, torrents};
#[cfg(any(test, feature = "test-support"))]
use weaver_server_pipeline::{SharedPipelineState, pipeline};

pub mod bandwidth;
pub mod history;
pub mod ingest;
pub mod rss;
pub mod runtime;
pub mod servers;
pub mod watch_folder;
