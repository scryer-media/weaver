// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx, max_rows_for_engine};
use crate::settings::SeedingPolicy;
use crate::torrents::model::{parse_file_priorities, parse_named_by};
use crate::torrents::{
    ContentLayout, FilePriority, Infohash, TorrentCounters, TorrentIntent, TorrentRecord,
    TorrentSource, TrackerEntry,
};

pub(crate) const TORRENT_COLUMNS: &str = "job_id, infohash_v1, infohash_v2, name, source, metainfo, \
     category_id, added_at_ms, state, forced, completed_at_ms, storage_dir, \
     policy_override, unpack, unpack_in_force, metadata, upload_limit, download_limit, \
     sequential, first_last_piece_first, save_path, content_layout, files_dir, seed_mode, move_to, \
     queue_position, pool_member";

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

pub(crate) fn count(row: &SqlRow, column: &str) -> Result<u64, StateError> {
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
    let policy_override = SeedingPolicy::kept(
        row.opt_text("policy_override")?
            .map(|json| serde_json::from_str(&json).map_err(|error| corrupt("policy", error)))
            .transpose()?,
    );
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
        unpack_in_force: row.opt_bool("unpack_in_force")?,
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
        pool_member: opt_u32(row, "pool_member")?,
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

/// The categories a torrent list keeps, by id: the torrents in one of
/// `ids`, and, when `none` is set, those in no category, which is no id or
/// one that is not among `known`, the ids the settings name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CategoryIds {
    pub(crate) ids: Vec<u32>,
    pub(crate) none: bool,
    pub(crate) known: Vec<u32>,
}

/// Which torrents a list reads from the table: every one, or those a
/// category match and named hashes keep.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TorrentListFilter {
    pub(crate) categories: Option<CategoryIds>,
    /// The torrents whose hash is one of these: the v1 hash, or the v2 hash
    /// cut to 20 bytes for a torrent with no v1 hash.
    pub(crate) hashes: Option<Vec<[u8; 20]>>,
}

impl TorrentListFilter {
    // Its predicate after `WHERE`, its binds pushed onto `args`, with the
    // named hashes of `hashes`, a chunk of its own; `1 = 1` keeps every
    // torrent.
    fn predicate(&self, hashes: Option<&[[u8; 20]]>, args: &mut Vec<SqlArg>) -> String {
        let mut terms = Vec::new();
        if let Some(categories) = &self.categories {
            let mut kept = Vec::new();
            if !categories.ids.is_empty() {
                kept.push(format!("category_id IN ({})", binds(categories.ids.len())));
                args.extend(categories.ids.iter().map(|id| SqlArg::I64(i64::from(*id))));
            }
            if categories.none {
                kept.push("category_id IS NULL".to_owned());
                if !categories.known.is_empty() {
                    kept.push(format!(
                        "category_id NOT IN ({})",
                        binds(categories.known.len())
                    ));
                    args.extend(
                        categories
                            .known
                            .iter()
                            .map(|id| SqlArg::I64(i64::from(*id))),
                    );
                } else {
                    kept.push("category_id IS NOT NULL".to_owned());
                }
            }
            terms.push(match kept.len() {
                0 => "1 = 0".to_owned(),
                _ => format!("({})", kept.join(" OR ")),
            });
        }
        if let Some(hashes) = hashes {
            // A v2-only torrent goes by its v2 hash cut to 20 bytes: every v2
            // hash that starts with those bytes lies between them padded
            // with zeros and them padded with 0xff. The partial index on v2
            // hashes holds only torrents with no v1 hash, the predicate's
            // own words.
            let named = hashes
                .iter()
                .map(|hash| {
                    args.push(SqlArg::Bytes(hash.to_vec()));
                    args.push(SqlArg::Bytes(padded(hash, 0x00)));
                    args.push(SqlArg::Bytes(padded(hash, 0xff)));
                    "infohash_v1 = {} OR (infohash_v1 IS NULL AND infohash_v2 BETWEEN {} AND {})"
                })
                .collect::<Vec<_>>();
            terms.push(match named.len() {
                0 => "1 = 0".to_owned(),
                _ => format!("({})", named.join(" OR ")),
            });
        }
        match terms.len() {
            0 => "1 = 1".to_owned(),
            _ => terms.join(" AND "),
        }
    }

    // The binds its category match takes in each statement.
    fn category_binds(&self) -> usize {
        self.categories.as_ref().map_or(0, |categories| {
            categories.ids.len()
                + if categories.none {
                    categories.known.len()
                } else {
                    0
                }
        })
    }
}

// `n` placeholders, comma-separated.
fn binds(n: usize) -> String {
    vec!["{}"; n].join(", ")
}

// A 20-byte hash padded to a v2 hash's 32 bytes with `fill`.
fn padded(hash: &[u8; 20], fill: u8) -> Vec<u8> {
    let mut bytes = hash.to_vec();
    bytes.resize(32, fill);
    bytes
}

// The torrents `filter` keeps among `hashes`, in the order they were added,
// each once: one statement per chunk of hashes that fits the engine's binds
// beside the category's.
async fn named_torrents(
    datastore: &crate::persistence::sql_runtime::StoreDatastore,
    filter: &TorrentListFilter,
    hashes: &[[u8; 20]],
) -> Result<Vec<JobId>, StateError> {
    let fixed = filter.category_binds().div_ceil(3);
    let per = max_rows_for_engine(datastore.engine(), 3)
        .saturating_sub(fixed)
        .max(1);
    let mut kept: Vec<(i64, u64)> = Vec::new();
    for chunk in hashes.chunks(per) {
        let mut args = Vec::new();
        let predicate = filter.predicate(Some(chunk), &mut args);
        let rows = SqlRuntime::fetch_all(
            datastore.read_exec(),
            &format!("SELECT added_at_ms, job_id FROM torrents WHERE {predicate}"),
            &args,
        )
        .await?;
        for row in &rows {
            kept.push((row.i64("added_at_ms")?, count(row, "job_id")?));
        }
    }
    kept.sort_unstable();
    kept.dedup();
    Ok(kept.into_iter().map(|(_, job_id)| JobId(job_id)).collect())
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
    // One read of the rows a torrent's restore or attach reads, counted
    // under test so a test pins how many a boot makes.
    pub(crate) fn torrent_read(&self) {
        #[cfg(test)]
        self.torrent_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

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

    // The job of every torrent, read alone, for a start that needs only
    // which torrents it brings back.
    pub fn torrent_job_ids(&self) -> Result<Vec<JobId>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows =
                SqlRuntime::fetch_all(datastore.read_exec(), "SELECT job_id FROM torrents", &[])
                    .await?;
            rows.iter()
                .map(|row| Ok(JobId(count(row, "job_id")?)))
                .collect()
        })
    }

    /// The torrents `filter` keeps, in the order they were added: the
    /// `limit` after the first `offset`, every one after it with no limit.
    /// Without named hashes the page is cut in the statement, which walks
    /// the order's index; named hashes are read in chunks that fit the
    /// engine's binds, a page of them at most as many torrents as named.
    pub(crate) fn torrent_list_ids(
        &self,
        filter: &TorrentListFilter,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Vec<JobId>, StateError> {
        let datastore = self.datastore();
        let filter = filter.clone();
        self.run_sql_blocking_read(async move {
            let Some(hashes) = &filter.hashes else {
                let mut args = Vec::new();
                let predicate = filter.predicate(None, &mut args);
                let sql = format!(
                    "SELECT job_id FROM torrents WHERE {predicate}
                      ORDER BY added_at_ms, job_id LIMIT {{}} OFFSET {{}}"
                );
                args.push(SqlArg::I64(limit.map_or(i64::MAX, |limit| {
                    i64::try_from(limit).unwrap_or(i64::MAX)
                })));
                args.push(SqlArg::I64(i64::try_from(offset).unwrap_or(i64::MAX)));
                let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                return rows
                    .iter()
                    .map(|row| Ok(JobId(count(row, "job_id")?)))
                    .collect();
            };
            let mut kept = named_torrents(&datastore, &filter, hashes).await?;
            let end = limit.map_or(kept.len(), |limit| offset.saturating_add(limit));
            kept.truncate(end.min(kept.len()));
            kept.drain(..offset.min(kept.len()));
            Ok(kept)
        })
    }

    /// How many torrents `filter` keeps, counted in the statement.
    pub(crate) fn count_torrent_list(
        &self,
        filter: &TorrentListFilter,
    ) -> Result<usize, StateError> {
        let datastore = self.datastore();
        let filter = filter.clone();
        self.run_sql_blocking_read(async move {
            if let Some(hashes) = &filter.hashes {
                return Ok(named_torrents(&datastore, &filter, hashes).await?.len());
            }
            let mut args = Vec::new();
            let predicate = filter.predicate(None, &mut args);
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("SELECT COUNT(*) AS kept FROM torrents WHERE {predicate}"),
                &args,
            )
            .await?;
            Ok(row.map(|row| count(&row, "kept")).transpose()?.unwrap_or(0) as usize)
        })
    }

    // Every torrent, in the order they were added.
    pub fn list_torrents(&self) -> Result<Vec<TorrentRecord>, StateError> {
        self.torrent_read();
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
        self.torrent_read();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT file_priorities FROM torrents WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            row.as_ref()
                .map_or_else(|| Ok(Vec::new()), file_priorities_from_row)
        })
    }

    pub fn list_torrent_trackers(&self, job_id: JobId) -> Result<Vec<TrackerEntry>, StateError> {
        self.torrent_read();
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
        self.torrent_read();
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
            rows.iter().map(|row| row.text("host")).collect()
        })
    }

    pub fn get_torrent_counters(
        &self,
        job_id: JobId,
    ) -> Result<Option<TorrentCounters>, StateError> {
        self.torrent_read();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT uploaded_bytes, downloaded_bytes, active_duration_s,
                        last_seen_complete_ms, finished_duration_s, last_activity_ms,
                        consecutive_seeding_s, failed_bytes, redundant_bytes
                   FROM torrent_counters WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .as_ref()
            .map(counters_from_row)
            .transpose()
        })
    }

    // Every torrent, in the order they were added, each with its files'
    // levels and the pieces it checked in seed mode: what a boot reads of
    // the torrents' rows, in one read. A row that does not read fails the
    // read, as [`Self::list_torrents`] does; a level or seed-mode column
    // that does not read is that torrent's alone.
    pub(crate) fn restored_torrents(&self) -> Result<Vec<RestoredTorrent>, StateError> {
        self.torrent_read();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!(
                    "SELECT {TORRENT_COLUMNS}, file_priorities, seed_verified \
                       FROM torrents ORDER BY added_at_ms, job_id"
                ),
                &[],
            )
            .await?;
            rows.iter()
                .map(|row| {
                    Ok(RestoredTorrent {
                        record: torrent_from_row(row)?,
                        levels: file_priorities_from_row(row),
                        seed_verified: row.opt_bytes("seed_verified"),
                    })
                })
                .collect()
        })
    }

    // Why unpacking left each torrent short, as its history keeps it failed or partial, by
    // torrent, in one read: what [`Self::get_job_history`] gives a torrent's
    // restore, for all of them.
    pub(crate) fn failed_torrent_histories(
        &self,
    ) -> Result<std::collections::BTreeMap<JobId, String>, StateError> {
        self.torrent_read();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT h.job_id, h.error_message FROM job_history h \
                   JOIN torrents t ON t.job_id = h.job_id \
                  WHERE h.status IN ('failed', 'partial') AND h.error_message IS NOT NULL",
                &[],
            )
            .await?;
            rows.iter()
                .map(|row| Ok((JobId(count(row, "job_id")?), row.text("error_message")?)))
                .collect()
        })
    }
}

// One torrent's row as a boot reads it.
pub(crate) struct RestoredTorrent {
    pub(crate) record: TorrentRecord,
    pub(crate) levels: Result<Vec<FilePriority>, StateError>,
    pub(crate) seed_verified: Result<Option<Vec<u8>>, StateError>,
}

pub(crate) fn counters_from_row(row: &SqlRow) -> Result<TorrentCounters, StateError> {
    Ok(TorrentCounters {
        uploaded_bytes: count(row, "uploaded_bytes")?,
        downloaded_bytes: count(row, "downloaded_bytes")?,
        active_duration_s: count(row, "active_duration_s")?,
        last_seen_complete_ms: row.opt_i64("last_seen_complete_ms")?,
        finished_duration_s: count(row, "finished_duration_s")?,
        last_activity_ms: row.opt_i64("last_activity_ms")?,
        consecutive_seeding_s: count(row, "consecutive_seeding_s")?,
        failed_bytes: count(row, "failed_bytes")?,
        redundant_bytes: count(row, "redundant_bytes")?,
    })
}

// The operator's level for each file, as far as one was stored.
fn file_priorities_from_row(row: &SqlRow) -> Result<Vec<FilePriority>, StateError> {
    let Some(value) = row.opt_text("file_priorities")? else {
        return Ok(Vec::new());
    };
    parse_file_priorities(&value).ok_or_else(|| corrupt("file priorities", &value))
}
