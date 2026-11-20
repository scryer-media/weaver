// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_torrents::torrents::queries::TorrentsQueries;
pub(crate) use weaver_server_torrents::torrents::queries::{
    CategoryIds, RestoredTorrent, TorrentListFilter, TorrentsInternalQueries, count,
};

#[cfg(test)]
pub(crate) use weaver_server_torrents::torrents::queries::{count_statements, list_statements};

impl weaver_server_torrents::torrents::queries::TorrentDatabase for crate::Database {
    #[cfg(test)]
    fn torrent_read_counter(&self) -> Option<&std::sync::Arc<std::sync::atomic::AtomicUsize>> {
        Some(&self.torrent_reads)
    }
}
