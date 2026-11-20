// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_torrent_store::torrents::queries::TorrentsQueries;
pub use weaver_server_torrent_store::torrents::queries::{
    CategoryIds, RestoredTorrent, TorrentListFilter, TorrentsInternalQueries, count,
};

#[cfg(test)]
pub(crate) use weaver_server_torrent_store::torrents::queries::{
    count_statements, list_statements,
};

impl weaver_server_torrent_store::torrents::queries::TorrentDatabase for crate::Database {
    #[cfg(any(test, feature = "test-support"))]
    fn torrent_read_counter(&self) -> Option<&std::sync::Arc<std::sync::atomic::AtomicUsize>> {
        Some(&self.torrent_reads)
    }
}
