// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Pinned as blanket in the route guard, as the pipeline's is: one lint
// carries both the route ban and the writer gate, and the application
// upgrade's tests, moved here with it, build a plain client for their local
// test server.
#![cfg_attr(
    test,
    allow(
        clippy::disallowed_methods,
        reason = "tests build a plain client for their own loopback servers"
    )
)]

// The facade of the old weaver-server-pipeline paths, for the API, the app
// and xtask: the composed Database and its domain code, the orchestration
// services, and the pipeline's own modules and types. Core's own modules
// below shadow the glob where they extend a module.
pub use weaver_server_composition::*;
pub use weaver_server_orchestration::{
    bandwidth, history, ingest, rss, runtime, servers, watch_folder,
};
pub use weaver_server_pipeline::{
    DownloadQueue, DownloadWork, JobState, Pipeline, SchedulerHandle, SharedPipelineState, jobs,
    pipeline, settings,
};
pub mod auth;
pub use weaver_server_settings::categories;
pub mod application_upgrade;
pub mod operations;
pub mod persistence;
pub mod proxies;
pub mod support;
pub use weaver_server_torrents::{speed_test, torrents};
pub use weaver_torrent::core::torrent_metainfo::MAX_TORRENT_FILE_BYTES;
pub mod update_check;
// Transitional, removed in 0.9.1. See the module docs.
pub mod upgrade_compat;
