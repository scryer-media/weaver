// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::history::record::JobHistoryRow;
use crate::persistence::sql_runtime::{SqlRuntime, SqlTx};
use weaver_server_history::history::persistence::unique_runs;
pub use weaver_server_torrent_store::torrents::history_persistence::*;
pub use weaver_server_torrent_store::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
pub use weaver_server_torrent_store::torrents::persistence::{
    MadeRead, RetiringMark, TorrentSeedWrites, TorrentSettingRows,
    TorrentsPersistenceInsertPlacedTorrentInternalQueries,
    TorrentsPersistenceLiveHeldPathsInternalQueries, update_column_tx,
};
use weaver_server_torrent_store::torrents::persistence::{
    delete_torrent_tx, delete_torrents_tx, stored,
};

impl crate::persistence::DatabaseWriterExecutor {
    // A torrent's job's history row, written in one transaction on the
    // writer's path, which keeps it for want of room as it keeps an NZB
    // job's archive. When the job `retires` the torrent leaves in the same
    // write, as `delete_torrent` removes it: no reader finds the job in
    // neither, so an event raised for it while it was live is still queued
    // for it after. Otherwise the torrent stays and seeds on.
    pub(crate) fn write_torrent_history(
        &self,
        entry: &JobHistoryRow,
        retires: bool,
        files: &[HistoryFile],
    ) -> Result<(), StateError> {
        let id = stored(entry.job_id)?;
        let datastore = self.datastore();
        let entry = entry.clone();
        let files = files.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "write_torrent_history", |tx| {
                let entry = entry.clone();
                let files = files.clone();
                Box::pin(async move { torrent_history_tx(tx, id, &entry, retires, &files).await })
            })
            .await
        })
        .inspect(|()| self.count_history_write())
    }

    // The history rows of a batch of torrents' jobs (`write_torrent_history`
    // of each, in order), in one transaction: all of them are written, or
    // none is.
    pub(crate) fn write_torrent_histories(
        &self,
        entries: Vec<TorrentHistoryWrite>,
    ) -> Result<(), StateError> {
        for entry in &entries {
            stored(entry.0.job_id)?;
        }
        let entries = std::sync::Arc::new(entries);
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "write_torrent_histories", |tx| {
                let entries = std::sync::Arc::clone(&entries);
                Box::pin(async move {
                    // Each run of distinct jobs at once, as each of its
                    // entries one by one would write it.
                    for run in unique_runs(&entries, |(entry, ..)| entry.job_id) {
                        let rows: Vec<&JobHistoryRow> =
                            run.iter().map(|(entry, ..)| entry).collect();
                        crate::history::persistence::insert_job_histories_tx(tx, &rows).await?;
                        let lists = run
                            .iter()
                            .map(|(entry, _, files)| Ok((stored(entry.job_id)?, &files[..])))
                            .collect::<Result<Vec<_>, StateError>>()?;
                        write_histories_files_tx(tx, &lists).await?;
                        let retiring: Vec<i64> = run
                            .iter()
                            .zip(&lists)
                            .filter(|((_, retires, _), _)| *retires)
                            .map(|(_, (id, _))| *id)
                            .collect();
                        delete_torrents_tx(tx, &retiring).await?;
                    }
                    // The seam refuses the whole transaction once its rows
                    // are in it, so none of them may be left.
                    #[cfg(any(test, feature = "test-support"))]
                    if HISTORIES_REFUSED.swap(false, std::sync::atomic::Ordering::SeqCst) {
                        return Err(StateError::OutOfSpace("database or disk is full".into()));
                    }
                    Ok(())
                })
            })
            .await
        })
        .inspect(|()| self.count_history_write())
    }
}

// One torrent's job's history row in a batch's write: the row, whether the
// torrent `retires` with it, and the files it lists.
pub(crate) type TorrentHistoryWrite = (JobHistoryRow, bool, std::sync::Arc<[HistoryFile]>);

// A torrent's job's history row in `tx`, the files it lists beside it, and
// the torrent gone when it `retires`.
async fn torrent_history_tx(
    tx: &mut SqlTx<'_>,
    id: i64,
    entry: &JobHistoryRow,
    retires: bool,
    files: &[HistoryFile],
) -> Result<(), StateError> {
    crate::history::persistence::insert_job_history_tx(tx, entry).await?;
    write_history_files_tx(tx, id, files).await?;
    if retires {
        delete_torrent_tx(tx, id).await?;
    }
    Ok(())
}
