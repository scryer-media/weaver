// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent service's tests, moved out of the crate's own test build:
//! the same tests under the same names, compiled on their own.

// Pinned as blanket in the route guard, as the crate's own test build is:
// one lint carries both the route ban and the writer gate, and these tests
// write their own rows and dial their own loopback fixtures.
#![allow(
    clippy::disallowed_methods,
    reason = "tests write their own fixture rows and dial their own loopback fixtures"
)]
#![deny(clippy::await_holding_lock)]

use weaver_server_composition::{
    ArchiveHistoryOutcome, ArchiveHistoryResult, JobHistoryRow, LoadError, PipelineMetrics,
    SchedulerError, StateError, operations, persistence, post_processing, proxies,
    schema_migrations, tags,
};
use weaver_server_orchestration::{bandwidth, history, ingest};
use weaver_server_pipeline::{jobs, pipeline, settings};

mod torrents {
    pub use weaver_server_torrent_store::torrents::{files, intake, layout, writes};
    pub use weaver_server_torrents::torrents::*;

    pub mod storage {
        pub use weaver_server_torrent_store::torrents::storage::*;
    }

    pub mod sessions {
        pub use weaver_server_torrents::torrents::sessions::*;

        pub mod tests {
            pub use weaver_server_torrents::torrents::sessions::fakes::*;
        }
    }

    pub mod service {
        pub use weaver_server_torrents::torrents::service::*;

        // The module's own imports, which its tests read through `super::*`.
        use crate::jobs::JobId;
        use crate::jobs::handle::{HoldSet, QueueMoveTarget, SchedulerHandle};
        use crate::jobs::model::{JobStatus, Protocol};
        use crate::persistence::Database;
        use crate::pipeline::ExtractionLimits;
        use crate::settings::model::TrackerRule;
        use crate::torrents::layout::PieceLayout;
        use crate::torrents::metrics::SessionSeries;
        use crate::torrents::sessions::Sessions;
        use crate::torrents::status::{self, Engine, Held};
        use crate::torrents::{
            FilePriority, KeptIntent, TorrentCounters, TorrentFile, TorrentIntent, TorrentRecord,
            TorrentState, TrackerEntry,
        };
        use std::collections::{BTreeMap, BTreeSet, HashMap};
        use std::path::{Path, PathBuf};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use tokio::sync::watch;
        use weaver_server_base::clock::{Clock, SystemClock};
        use weaver_server_disk::operations::disk::CapacityReader;
        pub(crate) use weaver_server_pipeline::torrents::service::{
            LiveHeld, finish_retiring, remove_history_files,
        };
        pub(crate) use weaver_server_torrent_store::torrents::metainfo::metainfo_is_private;
        pub(crate) use weaver_server_torrent_store::torrents::storage::CHECK_EVERYTHING_FAILS;
        use weaver_server_torrents::torrents::service::disk_room::NoRoom;
        use weaver_server_torrents::torrents::service::live::{Live, Timed};
        use weaver_torrent::core::{Id20, Id32};
        use weaver_torrent::{AddTorrentOptions, ManagedTorrent, TorrentTotals};

        mod tests;
    }
}

// The pipeline's PostgreSQL test schema, compiled into these tests too: it
// stays test-only, in one file.
#[path = "../../weaver-server-composition/src/persistence/connection/test_schema.rs"]
mod test_schema;

mod tests {
    pub(crate) use super::test_schema;

    // Re-executed as the real supervisor by the pipeline's
    // `tag_reporting_queue_script_for_test`.
    #[test]
    fn post_processing_supervisor_child() {
        weaver_server_composition::post_processing::events::post_processing_supervisor_child_for_test();
    }
}
