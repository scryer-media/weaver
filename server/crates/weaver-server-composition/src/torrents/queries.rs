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

impl weaver_server_torrent_store::torrents::history_persistence::TorrentHistoryDatabase
    for crate::Database
{
    fn insert_job_history_tx<'a>(
        tx: &'a mut crate::persistence::sql_runtime::SqlTx<'_>,
        entry: &'a crate::history::record::JobHistoryRow,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), crate::StateError>> + Send + 'a>,
    > {
        Box::pin(crate::history::persistence::insert_job_history_tx(
            tx, entry,
        ))
    }
}
