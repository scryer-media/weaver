// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx};
use crate::torrents::{
    Infohash, TorrentCounters, TorrentFile, TorrentIntent, TorrentRecord, TorrentSource,
    TrackerEntry,
};

pub(crate) const TORRENT_COLUMNS: &str = "job_id, infohash_v1, infohash_v2, name, source, metainfo, \
     private, category_id, added_at_ms, state, forced, completed_at_ms, storage_dir, \
     policy_override, tracker_rule_id";

fn corrupt(what: &str, value: impl std::fmt::Display) -> StateError {
    StateError::Database(format!("stored torrent {what} is not valid: {value}"))
}

fn hash<const N: usize>(row: &SqlRow, column: &str) -> Result<Option<[u8; N]>, StateError> {
    row.opt_bytes(column)?
        .map(|bytes| {
            <[u8; N]>::try_from(bytes.as_slice()).map_err(|_| corrupt(column, bytes.len()))
        })
        .transpose()
}

fn count(row: &SqlRow, column: &str) -> Result<u64, StateError> {
    let value = row.i64(column)?;
    u64::try_from(value).map_err(|_| corrupt(column, value))
}

fn opt_u32(row: &SqlRow, column: &str) -> Result<Option<u32>, StateError> {
    row.opt_i64(column)?
        .map(|value| u32::try_from(value).map_err(|_| corrupt(column, value)))
        .transpose()
}

pub(crate) fn torrent_from_row(row: &SqlRow) -> Result<TorrentRecord, StateError> {
    let source = row.text("source")?;
    let intent = row.text("state")?;
    let policy_override = row
        .opt_text("policy_override")?
        .map(|json| serde_json::from_str(&json).map_err(|error| corrupt("policy", error)))
        .transpose()?;
    Ok(TorrentRecord {
        job_id: JobId(count(row, "job_id")?),
        infohash_v1: hash(row, "infohash_v1")?,
        infohash_v2: hash(row, "infohash_v2")?,
        name: row.text("name")?,
        source: TorrentSource::parse(&source).ok_or_else(|| corrupt("source", &source))?,
        metainfo: row.opt_bytes("metainfo")?,
        private: row.bool("private")?,
        category_id: opt_u32(row, "category_id")?,
        added_at_ms: row.i64("added_at_ms")?,
        intent: TorrentIntent::parse(&intent).ok_or_else(|| corrupt("intent", &intent))?,
        forced: row.bool("forced")?,
        completed_at_ms: row.opt_i64("completed_at_ms")?,
        storage_dir: row.opt_text("storage_dir")?,
        policy_override,
        tracker_rule_id: opt_u32(row, "tracker_rule_id")?,
    })
}

fn infohash_filter(infohash: Infohash) -> (&'static str, SqlArg) {
    match infohash {
        Infohash::V1(hash) => ("infohash_v1 = {}", SqlArg::Bytes(hash.to_vec())),
        Infohash::V2(hash) => ("infohash_v2 = {}", SqlArg::Bytes(hash.to_vec())),
    }
}

pub(crate) async fn torrent_by_infohash_tx(
    tx: &mut SqlTx<'_>,
    infohash: Infohash,
) -> Result<Option<TorrentRecord>, StateError> {
    let (filter, arg) = infohash_filter(infohash);
    tx.fetch_optional(
        &format!("SELECT {TORRENT_COLUMNS} FROM torrents WHERE {filter}"),
        &[arg],
    )
    .await?
    .as_ref()
    .map(torrent_from_row)
    .transpose()
}

impl Database {
    pub fn get_torrent(&self, job_id: JobId) -> Result<Option<TorrentRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("SELECT {TORRENT_COLUMNS} FROM torrents WHERE job_id = {{}}"),
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .as_ref()
            .map(torrent_from_row)
            .transpose()
        })
    }

    pub fn get_torrent_by_infohash(
        &self,
        infohash: Infohash,
    ) -> Result<Option<TorrentRecord>, StateError> {
        let datastore = self.datastore();
        let (filter, arg) = infohash_filter(infohash);
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("SELECT {TORRENT_COLUMNS} FROM torrents WHERE {filter}"),
                &[arg],
            )
            .await?
            .as_ref()
            .map(torrent_from_row)
            .transpose()
        })
    }

    // Every torrent, in the order they were added.
    pub fn list_torrents(&self) -> Result<Vec<TorrentRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!("SELECT {TORRENT_COLUMNS} FROM torrents ORDER BY added_at_ms, job_id"),
                &[],
            )
            .await?;
            rows.iter().map(torrent_from_row).collect()
        })
    }

    // The torrents of one category, or with `None` those with no category, in
    // the order they were added.
    pub fn list_torrents_by_category(
        &self,
        category_id: Option<u32>,
    ) -> Result<Vec<TorrentRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = match category_id {
                Some(id) => {
                    SqlRuntime::fetch_all(
                        datastore.read_exec(),
                        &format!(
                            "SELECT {TORRENT_COLUMNS} FROM torrents WHERE category_id = {{}}
                              ORDER BY added_at_ms, job_id"
                        ),
                        &[SqlArg::I64(i64::from(id))],
                    )
                    .await?
                }
                None => {
                    SqlRuntime::fetch_all(
                        datastore.read_exec(),
                        &format!(
                            "SELECT {TORRENT_COLUMNS} FROM torrents WHERE category_id IS NULL
                              ORDER BY added_at_ms, job_id"
                        ),
                        &[],
                    )
                    .await?
                }
            };
            rows.iter().map(torrent_from_row).collect()
        })
    }

    pub fn list_torrent_files(&self, job_id: JobId) -> Result<Vec<TorrentFile>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT file_index, path, length, selected FROM torrent_files
                  WHERE job_id = {} ORDER BY file_index",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            rows.iter()
                .map(|row| {
                    let index = row.i64("file_index")?;
                    Ok(TorrentFile {
                        index: u32::try_from(index).map_err(|_| corrupt("file index", index))?,
                        path: row.text("path")?,
                        length: count(row, "length")?,
                        selected: row.bool("selected")?,
                    })
                })
                .collect()
        })
    }

    pub fn list_torrent_trackers(&self, job_id: JobId) -> Result<Vec<TrackerEntry>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT tier, url FROM torrent_trackers WHERE job_id = {} ORDER BY tier, url",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            rows.iter()
                .map(|row| {
                    let tier = row.i64("tier")?;
                    Ok(TrackerEntry {
                        tier: u32::try_from(tier).map_err(|_| corrupt("tracker tier", tier))?,
                        url: row.text("url")?,
                    })
                })
                .collect()
        })
    }

    pub fn get_torrent_counters(
        &self,
        job_id: JobId,
    ) -> Result<Option<TorrentCounters>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT uploaded_bytes, downloaded_bytes, finished_duration_s, last_activity_ms,
                        last_announce_uploaded_bytes, updated_at_ms
                   FROM torrent_counters WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .map(|row| {
                Ok(TorrentCounters {
                    uploaded_bytes: count(&row, "uploaded_bytes")?,
                    downloaded_bytes: count(&row, "downloaded_bytes")?,
                    finished_duration_s: count(&row, "finished_duration_s")?,
                    last_activity_ms: row.opt_i64("last_activity_ms")?,
                    last_announce_uploaded_bytes: count(&row, "last_announce_uploaded_bytes")?,
                    updated_at_ms: row.i64("updated_at_ms")?,
                })
            })
            .transpose()
        })
    }
}
