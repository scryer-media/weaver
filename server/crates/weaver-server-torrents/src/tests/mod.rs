// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Tests moved up from weaver-server-pipeline: each drives the torrents
// crate's service, sessions or leg fakes.

// The pipeline's PostgreSQL test schema, compiled into these tests too: it
// stays test-only, in one file.
#[path = "../../../weaver-server-composition/src/persistence/connection/test_schema.rs"]
pub(crate) mod test_schema;

mod network_metrics;
mod network_test_via;
mod network_torrent;
mod proxy_runtime;
mod rss_torrents;
mod watch_folder_torrents;

// Re-executed as the real supervisor by the pipeline's
// `tag_reporting_queue_script_for_test`.
#[test]
fn post_processing_supervisor_child() {
    weaver_server_composition::post_processing::events::post_processing_supervisor_child_for_test();
}
