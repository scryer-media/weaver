// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx};
use crate::torrents::model::{parse_file_priorities, parse_named_by};
use crate::torrents::{
    ContentLayout, FilePriority, Infohash, TorrentCounters, TorrentIntent, TorrentRecord, TorrentSource,
    TrackerEntry,
};

pub(crate) const TORRENT_COLUMNS: &str = "job_id, infohash_v1, infohash_v2, name, source, metainfo, \
     category_id, added_at_ms, state, forced, completed_at_ms, storage_dir, \
     policy_override, unpack, metadata, upload_limit, download_limit, \
     sequential, first_last_piece_first, save_path, content_layout, files_dir, seed_mode, move_to, \
     queue_position";

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

fn tracker_entry(row: &SqlRow) -> Result<TrackerEntry, StateError> {
    let tier = row.i64("tier")?;
    let named_by = row.text("named_by")?;
    Ok(TrackerEntry {
        tier: u32::try_from(tier).map_err(|_| corrupt("tracker tier", tier))?,
        url: row.text("url")?,
        named_by: parse_named_by(&named_by).ok_or_else(|| corrupt("tracker naming", &named_by))?,
    })
}

fn opt_u64(row: &SqlRow, column: &str) -> Result<Option<u64>, StateError> {
    row.opt_i64(column)?
        .map(|value| u64::try_from(value).map_err(|_| corrupt(column, value)))
        .transpose()
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
    let metadata = row
        .opt_text("metadata")?
        .map(|json| serde_json::from_str(&json).map_err(|error| corrupt("metadata", error)))
        .transpose()?
        .unwrap_or_default();
    Ok(TorrentRecord {
        job_id: JobId(count(row, "job_id")?),
        infohash_v1: hash(row, "infohash_v1")?,
        infohash_v2: hash(row, "infohash_v2")?,
        name: row.text("name")?,
        source: TorrentSource::parse(&source).ok_or_else(|| corrupt("source", &source))?,
        metainfo: row.opt_bytes("metainfo")?.map(bytes::Bytes::from),
        category_id: opt_u32(row, "category_id")?,
        added_at_ms: row.i64("added_at_ms")?,
        intent: TorrentIntent::parse(&intent).ok_or_else(|| corrupt("intent", &intent))?,
        forced: row.bool("forced")?,
        completed_at_ms: row.opt_i64("completed_at_ms")?,
        storage_dir: row.opt_text("storage_dir")?,
        policy_override,
        unpack: row.opt_bool("unpack")?,
        metadata,
        upload_limit: opt_u64(row, "upload_limit")?,
        download_limit: opt_u64(row, "download_limit")?,
        sequential: row.bool("sequential")?,
        first_last_piece_first: row.bool("first_last_piece_first")?,
        save_path: row.opt_text("save_path")?,
        content_layout: row
            .opt_text("content_layout")?
            .map(|layout| ContentLayout::parse(&layout).ok_or_else(|| corrupt("layout", &layout)))
            .transpose()?,
        files_dir: row.opt_text("files_dir")?,
        seed_mode: row.bool("seed_mode")?,
        move_to: row.opt_text("move_to")?,
        queue_position: row.i64("queue_position")?,
    })
}

// The torrents going by any of `infohashes`; it matches none when empty.
fn infohash_filter(infohashes: &[Infohash]) -> (String, Vec<SqlArg>) {
    let (columns, args): (Vec<_>, Vec<_>) = infohashes
        .iter()
        .map(|infohash| match infohash {
            Infohash::V1(hash) => ("infohash_v1 = {}", SqlArg::Bytes(hash.to_vec())),
            Infohash::V2(hash) => ("infohash_v2 = {}", SqlArg::Bytes(hash.to_vec())),
        })
        .unzip();
    let filter = if columns.is_empty() {
        "1 = 0".to_owned()
    } else {
        columns.join(" OR ")
    };
    (filter, args)
}

pub(crate) async fn torrent_by_infohash_tx(
    tx: &mut SqlTx<'_>,
    infohashes: &[Infohash],
) -> Result<Option<TorrentRecord>, StateError> {
    let (filter, args) = infohash_filter(infohashes);
    tx.fetch_optional(
        &format!("SELECT {TORRENT_COLUMNS} FROM torrents WHERE {filter}"),
        &args,
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

    // The torrent going by any of `infohashes`.
    pub fn get_torrent_by_infohash(
        &self,
        infohashes: &[Infohash],
    ) -> Result<Option<TorrentRecord>, StateError> {
        let datastore = self.datastore();
        let (filter, args) = infohash_filter(infohashes);
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("SELECT {TORRENT_COLUMNS} FROM torrents WHERE {filter}"),
                &args,
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

    // The operator's level for each file of a torrent, in the metainfo's
    // order, as far as one was stored; a file past them is normal.
    pub fn torrent_file_priorities(&self, job_id: JobId) -> Result<Vec<FilePriority>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT file_priorities FROM torrents WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let Some(value) = row
                .map(|row| row.opt_text("file_priorities"))
                .transpose()?
                .flatten()
            else {
                return Ok(Vec::new());
            };
            parse_file_priorities(&value).ok_or_else(|| corrupt("file priorities", &value))
        })
    }

    pub fn list_torrent_trackers(&self, job_id: JobId) -> Result<Vec<TrackerEntry>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT tier, url, named_by FROM torrent_trackers WHERE job_id = {} ORDER BY tier, url",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            rows.iter().map(tracker_entry).collect()
        })
    }

    /// Every torrent's trackers, by torrent, each in the order
    /// [`Self::list_torrent_trackers`] gives.
    pub fn torrent_trackers_by_job(
        &self,
    ) -> Result<std::collections::BTreeMap<JobId, Vec<TrackerEntry>>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, tier, url, named_by FROM torrent_trackers ORDER BY job_id, tier, url",
                &[],
            )
            .await?;
            let mut trackers = std::collections::BTreeMap::<JobId, Vec<TrackerEntry>>::new();
            for row in &rows {
                let job_id = count(row, "job_id")?;
                trackers
                    .entry(JobId(job_id))
                    .or_default()
                    .push(tracker_entry(row)?);
            }
            Ok(trackers)
        })
    }

    // Every host a torrent has announced to, in order.
    pub fn list_tracker_hosts(&self) -> Result<Vec<String>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT host FROM tracker_hosts ORDER BY host",
                &[],
            )
            .await?;
            rows.iter().map(|row| Ok(row.text("host")?)).collect()
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
                "SELECT uploaded_bytes, downloaded_bytes, active_duration_s,
                        last_seen_complete_ms, finished_duration_s, last_activity_ms
                   FROM torrent_counters WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .map(|row| {
                Ok(TorrentCounters {
                    uploaded_bytes: count(&row, "uploaded_bytes")?,
                    downloaded_bytes: count(&row, "downloaded_bytes")?,
                    active_duration_s: count(&row, "active_duration_s")?,
                    last_seen_complete_ms: row.opt_i64("last_seen_complete_ms")?,
                    finished_duration_s: count(&row, "finished_duration_s")?,
                    last_activity_ms: row.opt_i64("last_activity_ms")?,
                })
            })
            .transpose()
        })
    }
}
