// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::encryption::EncryptionKey;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx, max_rows_for_engine};
use crate::settings::SeedingPolicy;
use crate::torrents::model::{parse_file_priorities, parse_named_by};
use crate::torrents::{
    ContentLayout, FilePriority, Infohash, KeptIntent, TorrentCounters, TorrentRecord,
    TorrentSource, TrackerEntry,
};

pub(crate) const TORRENT_COLUMNS: &str = "job_id, infohash_v1, infohash_v2, name, source, metainfo, \
     category_id, added_at_ms, state, forced, completed_at_ms, storage_dir, \
     policy_override, unpack, unpack_in_force, metadata, upload_limit, download_limit, \
     sequential, first_last_piece_first, save_path, content_layout, files_dir, seed_mode, move_to, \
     queue_position, pool_member, private, unpack_over, download_ends";

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

// A tracker row, its URL opened; the error is why the URL did not open.
fn tracker_entry(
    row: &SqlRow,
    key: Option<&EncryptionKey>,
) -> Result<Result<TrackerEntry, String>, StateError> {
    let tier = row.i64("tier")?;
    let named_by = row.text("named_by")?;
    let url = match super::sealed::open_url(key, &row.text("url")?) {
        Ok(url) => url,
        Err(error) => return Ok(Err(error)),
    };
    Ok(Ok(TrackerEntry {
        tier: u32::try_from(tier).map_err(|_| corrupt("tracker tier", tier))?,
        url,
        named_by: parse_named_by(&named_by).ok_or_else(|| corrupt("tracker naming", &named_by))?,
    }))
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

pub(crate) fn torrent_from_row(
    row: &SqlRow,
    key: Option<&EncryptionKey>,
) -> Result<TorrentRecord, StateError> {
    let (record, sealed) = torrent_and_seal(row, key)?;
    if let Some(error) = sealed {
        tracing::warn!(job_id = record.job_id.0, error = %error, "a torrent's stored metainfo does not decrypt");
    }
    Ok(record)
}

// A torrent's row, its metainfo opened, and why it did not open when it
// did not: the torrent then reads with no metainfo.
pub(crate) fn torrent_and_seal(
    row: &SqlRow,
    key: Option<&EncryptionKey>,
) -> Result<(TorrentRecord, Option<String>), StateError> {
    let (metainfo, sealed) = match row.opt_bytes("metainfo")? {
        None => (None, None),
        Some(stored) => match super::sealed::open_metainfo(key, stored) {
            Ok(metainfo) => (Some(bytes::Bytes::from(metainfo)), None),
            Err(error) => (None, Some(error)),
        },
    };
    let source = row.text("source")?;
    let state = row.text("state")?;
    let kept = KeptIntent::parse(&state).ok_or_else(|| corrupt("intent", &state))?;
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
    let record = TorrentRecord {
        job_id: JobId(count(row, "job_id")?),
        infohash_v1: hash(row, "infohash_v1")?,
        infohash_v2: hash(row, "infohash_v2")?,
        name: row.text("name")?,
        source: TorrentSource::parse(&source).ok_or_else(|| corrupt("source", &source))?,
        metainfo,
        category_id: opt_u32(row, "category_id")?,
        added_at_ms: row.i64("added_at_ms")?,
        kept,
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
        private: row.bool("private")?,
        unpack_over: row.bool("unpack_over")?,
        download_ends: opt_u64(row, "download_ends")?.unwrap_or_default(),
    };
    Ok((record, sealed))
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
/// category match, named hashes and a privacy keep.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TorrentListFilter {
    pub(crate) categories: Option<CategoryIds>,
    /// The torrents whose hash is one of these: the v1 hash, or the v2 hash
    /// cut to 20 bytes for a torrent with no v1 hash.
    pub(crate) hashes: Option<Vec<[u8; 20]>>,
    /// The torrents whose metainfo marks them private, or those it does not.
    pub(crate) private: Option<bool>,
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
            // with zeros and them padded with 0xff.
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
        if let Some(private) = self.private {
            terms.push("private = {}".to_owned());
            args.push(SqlArg::Bool(private));
        }
        match terms.len() {
            0 => "1 = 1".to_owned(),
            _ => terms.join(" AND "),
        }
    }

    // Its category match as arms that keep no torrent twice, each with its
    // binds: one per category id, which walks the index of that category
    // in the order of adding, and one for the torrents in no category.
    // Every torrent is one arm, `1 = 1`, and a match of nothing is none.
    // With a privacy, each arm keeps that privacy's torrents alone, and
    // walks the index of that privacy instead.
    fn arms(&self) -> Vec<(String, Vec<SqlArg>)> {
        let arms = self.category_arms();
        let Some(private) = self.private else {
            return arms;
        };
        arms.into_iter()
            .map(|(predicate, mut binds)| {
                binds.push(SqlArg::Bool(private));
                let predicate = match predicate.as_str() {
                    "1 = 1" => "private = {}".to_owned(),
                    _ => format!("{predicate} AND private = {{}}"),
                };
                (predicate, binds)
            })
            .collect()
    }

    // Its category match as `arms` gives it, with no privacy.
    fn category_arms(&self) -> Vec<(String, Vec<SqlArg>)> {
        let Some(categories) = &self.categories else {
            return vec![("1 = 1".to_owned(), Vec::new())];
        };
        let mut arms: Vec<(String, Vec<SqlArg>)> = categories
            .ids
            .iter()
            .map(|id| {
                (
                    "category_id = {}".to_owned(),
                    vec![SqlArg::I64(i64::from(*id))],
                )
            })
            .collect();
        if categories.none {
            arms.push(if categories.known.is_empty() {
                ("1 = 1".to_owned(), Vec::new())
            } else {
                (
                    format!(
                        "(category_id IS NULL OR category_id NOT IN ({}))",
                        binds(categories.known.len())
                    ),
                    categories
                        .known
                        .iter()
                        .map(|id| SqlArg::I64(i64::from(*id)))
                        .collect(),
                )
            });
        }
        arms
    }

    // The binds its category match and privacy take in each statement.
    fn category_binds(&self) -> usize {
        usize::from(self.private.is_some())
            + self.categories.as_ref().map_or(0, |categories| {
                categories.ids.len()
                    + if categories.none {
                        categories.known.len()
                    } else {
                        0
                    }
            })
    }
}

// The most arms one compound statement joins: under SQLite's default
// `SQLITE_MAX_COMPOUND_SELECT` of 500.
const MAX_ARMS: usize = 400;

// `arms` cut into the groups one statement each reads: as many as fit the
// engine's binds, beside the page's two, and `MAX_ARMS`.
fn arm_groups(
    engine: crate::persistence::sql_runtime::SqlEngine,
    arms: Vec<(String, Vec<SqlArg>)>,
) -> Vec<Vec<(String, Vec<SqlArg>)>> {
    let limit = max_rows_for_engine(engine, 1).saturating_sub(2);
    let mut groups: Vec<Vec<(String, Vec<SqlArg>)>> = Vec::new();
    let mut taken = 0;
    for arm in arms {
        let full = groups
            .last()
            .is_none_or(|group| group.len() == MAX_ARMS || taken + arm.1.len() > limit);
        if full {
            groups.push(Vec::new());
            taken = 0;
        }
        taken += arm.1.len();
        groups.last_mut().expect("pushed").push(arm);
    }
    groups
}

// The statement that reads `group`'s torrents, `added_at_ms` and `job_id`,
// in the order they were added, then its `LIMIT` and `OFFSET`; their binds
// pushed onto `args`. Each arm walks its own index in that order and the
// compound's `ORDER BY` merges them, so no arm is sorted. A torrent a
// removal left retiring is in no list, as in no other read of the live
// torrents.
fn ordered_arms(group: Vec<(String, Vec<SqlArg>)>, args: &mut Vec<SqlArg>) -> String {
    let selects: Vec<String> = group
        .into_iter()
        .map(|(predicate, binds)| {
            args.extend(binds);
            format!(
                "SELECT added_at_ms, job_id FROM torrents WHERE retiring IS NULL AND ({predicate})"
            )
        })
        .collect();
    format!(
        "{} ORDER BY added_at_ms, job_id LIMIT {{}} OFFSET {{}}",
        selects.join(" UNION ALL ")
    )
}

// The statements that read the page at `offset` of at most `limit` of the
// torrents `filter`'s category match keeps, with no named hashes, each with
// its binds. One statement cuts the page itself; several each read as far
// as the page's end, for the page to be cut from them merged.
pub(crate) fn list_statements(
    engine: crate::persistence::sql_runtime::SqlEngine,
    filter: &TorrentListFilter,
    offset: usize,
    limit: Option<usize>,
) -> Vec<(String, Vec<SqlArg>)> {
    let bound = |n: usize| SqlArg::I64(i64::try_from(n).unwrap_or(i64::MAX));
    let groups = arm_groups(engine, filter.arms());
    let one = groups.len() == 1;
    groups
        .into_iter()
        .map(|group| {
            let mut args = Vec::new();
            let sql = ordered_arms(group, &mut args);
            if one {
                args.push(limit.map_or(SqlArg::I64(i64::MAX), bound));
                args.push(bound(offset));
            } else {
                args.push(limit.map_or(SqlArg::I64(i64::MAX), |limit| {
                    bound(offset.saturating_add(limit))
                }));
                args.push(bound(0));
            }
            (sql, args)
        })
        .collect()
}

// The statements that count the torrents `filter`'s category match and
// privacy keep, with no named hashes, each with its binds: one per group of
// arms, whose counts add up, since no torrent is in two arms.
pub(crate) fn count_statements(
    engine: crate::persistence::sql_runtime::SqlEngine,
    filter: &TorrentListFilter,
) -> Vec<(String, Vec<SqlArg>)> {
    arm_groups(engine, filter.arms())
        .into_iter()
        .map(|group| {
            let mut args = Vec::new();
            let predicate = group
                .into_iter()
                .map(|(predicate, binds)| {
                    args.extend(binds);
                    format!("({predicate})")
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            (
                format!(
                    "SELECT COUNT(*) AS kept FROM torrents WHERE retiring IS NULL AND ({predicate})"
                ),
                args,
            )
        })
        .collect()
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
            &format!(
                "SELECT added_at_ms, job_id FROM torrents WHERE retiring IS NULL AND ({predicate})"
            ),
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

// The job of the torrent kept under one of `infohashes`: its id alone,
// which is all an add that finds one reads.
pub(crate) async fn torrent_by_infohash_tx(
    tx: &mut SqlTx<'_>,
    infohashes: &[Infohash],
) -> Result<Option<JobId>, StateError> {
    let (filter, args) = infohash_filter(infohashes);
    tx.fetch_optional(
        &format!("SELECT job_id FROM torrents WHERE {filter}"),
        &args,
    )
    .await?
    .as_ref()
    .map(|row| count(row, "job_id").map(JobId))
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
        let key = self.encryption_key().cloned();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("SELECT {TORRENT_COLUMNS} FROM torrents WHERE job_id = {{}}"),
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .as_ref()
            .map(|row| torrent_from_row(row, key.as_ref()))
            .transpose()
        })
    }

    // The torrent going by any of `infohashes`.
    pub fn get_torrent_by_infohash(
        &self,
        infohashes: &[Infohash],
    ) -> Result<Option<TorrentRecord>, StateError> {
        let key = self.encryption_key().cloned();
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
            .map(|row| torrent_from_row(row, key.as_ref()))
            .transpose()
        })
    }

    // The job of every torrent a start brings back, read alone: never one a
    // removal left retiring, which goes to history instead.
    pub fn torrent_job_ids(&self) -> Result<Vec<JobId>, StateError> {
        Ok(self.torrent_rows()?.0)
    }

    // Every torrent row's job, those that come back live and those a
    // removal left retiring, in one read.
    pub(crate) fn torrent_rows(&self) -> Result<(Vec<JobId>, Vec<JobId>), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, CASE WHEN retiring IS NULL THEN 0 ELSE 1 END AS retiring \
                 FROM torrents",
                &[],
            )
            .await?;
            let (mut live, mut retiring) = (Vec::new(), Vec::new());
            for row in &rows {
                let job_id = JobId(count(row, "job_id")?);
                if row.i64("retiring")? == 0 {
                    live.push(job_id);
                } else {
                    retiring.push(job_id);
                }
            }
            Ok((live, retiring))
        })
    }

    // Which of `job_ids` have a torrent row: one statement per bind-sized
    // chunk, each served by the torrents' key.
    pub(crate) fn torrent_job_ids_among(
        &self,
        job_ids: &[JobId],
    ) -> Result<std::collections::BTreeSet<JobId>, StateError> {
        if job_ids.is_empty() {
            return Ok(Default::default());
        }
        let datastore = self.datastore();
        let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        self.run_sql_blocking_read(async move {
            let mut torrents = std::collections::BTreeSet::new();
            let rows_per =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 1);
            for chunk in ids.chunks(rows_per) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args = chunk.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!("SELECT job_id FROM torrents WHERE job_id IN ({placeholders})"),
                    &args,
                )
                .await?;
                for row in rows {
                    torrents.insert(JobId(count(&row, "job_id")?));
                }
            }
            Ok(torrents)
        })
    }

    /// The torrents `filter` keeps, in the order they were added: the
    /// `limit` after the first `offset`, every one after it with no limit.
    /// Without named hashes the page is cut in the statement, each of whose
    /// arms walks an index in that order; named hashes are read in chunks
    /// that fit the engine's binds, a page of them at most as many torrents
    /// as named.
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
                let statements = list_statements(datastore.engine(), &filter, offset, limit);
                let one = statements.len() == 1;
                let mut kept: Vec<(i64, u64)> = Vec::new();
                for (sql, args) in statements {
                    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                    for row in &rows {
                        kept.push((row.i64("added_at_ms")?, count(row, "job_id")?));
                    }
                }
                if !one {
                    kept.sort_unstable();
                    let end = limit.map_or(kept.len(), |limit| offset.saturating_add(limit));
                    kept.truncate(end.min(kept.len()));
                    kept.drain(..offset.min(kept.len()));
                }
                return Ok(kept.into_iter().map(|(_, job_id)| JobId(job_id)).collect());
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
            // The arms keep no torrent twice, so their counts add up.
            let mut kept = 0;
            for (sql, args) in count_statements(datastore.engine(), &filter) {
                let row = SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args).await?;
                kept += row.map(|row| count(&row, "kept")).transpose()?.unwrap_or(0) as usize;
            }
            Ok(kept)
        })
    }

    // Every torrent, in the order they were added.
    pub fn list_torrents(&self) -> Result<Vec<TorrentRecord>, StateError> {
        self.torrent_read();
        let key = self.encryption_key().cloned();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!(
                    "SELECT {TORRENT_COLUMNS} FROM torrents WHERE retiring IS NULL \
                     ORDER BY added_at_ms, job_id"
                ),
                &[],
            )
            .await?;
            rows.iter()
                .map(|row| torrent_from_row(row, key.as_ref()))
                .collect()
        })
    }

    // The torrents of one category, or with `None` those with no category, in
    // the order they were added.
    pub fn list_torrents_by_category(
        &self,
        category_id: Option<u32>,
    ) -> Result<Vec<TorrentRecord>, StateError> {
        let key = self.encryption_key().cloned();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = match category_id {
                Some(id) => {
                    SqlRuntime::fetch_all(
                        datastore.read_exec(),
                        &format!(
                            "SELECT {TORRENT_COLUMNS} FROM torrents WHERE category_id = {{}}
                              AND retiring IS NULL ORDER BY added_at_ms, job_id"
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
                              AND retiring IS NULL ORDER BY added_at_ms, job_id"
                        ),
                        &[],
                    )
                    .await?
                }
            };
            rows.iter()
                .map(|row| torrent_from_row(row, key.as_ref()))
                .collect()
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

    // Its URLs are sealed, so they are put in order once opened: by tier,
    // then URL. One that does not open fails the read.
    pub fn list_torrent_trackers(&self, job_id: JobId) -> Result<Vec<TrackerEntry>, StateError> {
        self.torrent_read();
        let key = self.encryption_key().cloned();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT tier, url, named_by FROM torrent_trackers WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            let mut trackers = rows
                .iter()
                .map(|row| {
                    tracker_entry(row, key.as_ref())?.map_err(|error| {
                        StateError::Database(format!(
                            "a tracker URL of torrent {} does not decrypt: {error}",
                            job_id.0
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            trackers.sort_by(|a, b| (a.tier, &a.url).cmp(&(b.tier, &b.url)));
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

    // The jobs of the next stored torrents after `after`, none past
    // `through` nor left retiring by a removal, `limit` at most, each with
    // the bytes its stored metainfo takes: what a start reads before a
    // page, by the table's key, to bound the page by its bytes and not its
    // rows alone. No metainfo is read for it.
    pub(crate) fn torrent_page_sizes(
        &self,
        after: Option<JobId>,
        through: JobId,
        limit: usize,
    ) -> Result<Vec<(JobId, u64)>, StateError> {
        self.torrent_read();
        let args = [
            SqlArg::I64(after.map_or(-1, |job_id| job_id.0 as i64)),
            SqlArg::I64(through.0 as i64),
            SqlArg::I64(i64::try_from(limit).unwrap_or(i64::MAX)),
        ];
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!(
                    "{METAINFO_SIZES} AND job_id > {{}} AND job_id <= {{}} \
                     ORDER BY job_id LIMIT {{}}"
                ),
                &args,
            )
            .await?;
            rows.iter().map(sized).collect()
        })
    }

    // `torrent_page_sizes` for the jobs named, by job id.
    pub(crate) fn torrent_sizes_of(&self, jobs: &[JobId]) -> Result<Vec<(JobId, u64)>, StateError> {
        self.torrent_read();
        let named: Vec<i64> = jobs.iter().map(|job_id| job_id.0 as i64).collect();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut sizes = Vec::with_capacity(named.len());
            for chunk in named.chunks(max_rows_for_engine(datastore.engine(), 1)) {
                let args: Vec<SqlArg> = chunk.iter().copied().map(SqlArg::I64).collect();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "{METAINFO_SIZES} AND job_id IN ({}) ORDER BY job_id",
                        binds(chunk.len())
                    ),
                    &args,
                )
                .await?;
                for row in &rows {
                    sizes.push(sized(row)?);
                }
            }
            Ok(sizes)
        })
    }

    // The torrents of `jobs` as a start brings them back, never one a
    // removal left retiring, by job id. Each comes with its files' levels,
    // the pieces it checked in seed mode, its trackers, its ledger, saved
    // peers and kept pieces, and why unpacking left it short: what a boot
    // reads of a torrent, four statements a page, each by its table's key.
    // A row that does not read fails the read, as [`Self::list_torrents`]
    // does; a level, a seed-mode column, a tracker URL or a part of the
    // ledger that does not read is that torrent's alone.
    pub(crate) fn restored_torrents(
        &self,
        jobs: &[JobId],
    ) -> Result<Vec<RestoredTorrent>, StateError> {
        self.torrent_read();
        let key = self.encryption_key().cloned();
        let datastore = self.datastore();
        let named: Vec<i64> = jobs.iter().map(|job_id| job_id.0 as i64).collect();
        #[cfg(test)]
        let reads = self.torrent_reads.clone();
        self.run_sql_blocking_read(async move {
            let select = format!(
                "SELECT {TORRENT_COLUMNS}, file_priorities, seed_verified, held_set, \
                        CASE WHEN held_set THEN NULL ELSE outputs END AS unheld_outputs \
                   FROM torrents WHERE retiring IS NULL"
            );
            let per = max_rows_for_engine(datastore.engine(), 1);
            let mut rows = Vec::new();
            for chunk in named.chunks(per) {
                let args: Vec<SqlArg> = chunk.iter().copied().map(SqlArg::I64).collect();
                rows.extend(
                    SqlRuntime::fetch_all(
                        datastore.read_exec(),
                        &format!(
                            "{select} AND job_id IN ({}) ORDER BY job_id",
                            binds(chunk.len())
                        ),
                        &args,
                    )
                    .await?,
                );
            }
            let mut restored = rows
                .into_iter()
                .map(|row| {
                    // The stored metainfo is given up as it is opened: no
                    // page holds its torrents' sealed and opened bytes both.
                    let (record, sealed) = torrent_and_seal(&row, key.as_ref())?;
                    Ok(RestoredTorrent {
                        record,
                        sealed,
                        levels: file_priorities_from_row(&row),
                        seed_verified: row.opt_bytes("seed_verified"),
                        held_set: row.bool("held_set")?,
                        unheld_outputs: row
                            .opt_text("unheld_outputs")?
                            .map_or(Some(Vec::new()), |text| serde_json::from_str(&text).ok()),
                        trackers: Ok(Vec::new()),
                        counters: None,
                        failure: None,
                    })
                })
                .collect::<Result<Vec<_>, StateError>>()?;
            if restored.is_empty() {
                return Ok(restored);
            }
            let at: std::collections::HashMap<u64, usize> = restored
                .iter()
                .enumerate()
                .map(|(index, row)| (row.record.job_id.0, index))
                .collect();
            let jobs: Vec<i64> = restored
                .iter()
                .map(|row| row.record.job_id.0 as i64)
                .collect();
            for chunk in jobs.chunks(per) {
                let args: Vec<SqlArg> = chunk.iter().copied().map(SqlArg::I64).collect();
                let among = binds(chunk.len());
                #[cfg(test)]
                reads.fetch_add(4, std::sync::atomic::Ordering::Relaxed);
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, tier, url, named_by FROM torrent_trackers \
                          WHERE job_id IN ({among})"
                    ),
                    &args,
                )
                .await?;
                for row in &rows {
                    let Some(&index) = at.get(&count(row, "job_id")?) else {
                        continue;
                    };
                    match (
                        &mut restored[index].trackers,
                        tracker_entry(row, key.as_ref())?,
                    ) {
                        (Ok(list), Ok(entry)) => list.push(entry),
                        (list @ Ok(_), Err(error)) => *list = Err(error),
                        (Err(_), _) => {}
                    }
                }
                for (job_id, row) in
                    crate::torrents::persistence::counters_among(&datastore, &among, &args).await?
                {
                    if let Some(&index) = at.get(&job_id.0) {
                        restored[index].counters = Some(row);
                    }
                }
                // Each file's time as taken with the pieces, in one read of
                // the page by the table's key; a row that does not read is
                // that file's alone, judged by its size.
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, file, modified_ns FROM torrent_file_stamps \
                          WHERE job_id IN ({among})"
                    ),
                    &args,
                )
                .await?;
                for row in &rows {
                    let Some(&index) = at.get(&count(row, "job_id")?) else {
                        continue;
                    };
                    let (Ok(file), Ok(time)) = (row.i64("file"), row.i64("modified_ns")) else {
                        continue;
                    };
                    if let (Ok(file), Some(counters)) =
                        (u32::try_from(file), restored[index].counters.as_mut())
                    {
                        counters.stamps.push((file, time));
                    }
                }
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, error_message FROM job_history \
                          WHERE job_id IN ({among}) AND status IN ('failed', 'partial') \
                            AND error_message IS NOT NULL"
                    ),
                    &args,
                )
                .await?;
                for row in &rows {
                    if let Some(&index) = at.get(&count(row, "job_id")?) {
                        restored[index].failure = Some(row.text("error_message")?);
                    }
                }
            }
            for list in restored
                .iter_mut()
                .filter_map(|row| row.trackers.as_mut().ok())
            {
                list.sort_by(|a, b| (a.tier, &a.url).cmp(&(b.tier, &b.url)));
            }
            Ok(restored)
        })
    }

    // The last job a torrent row has: a start reads the torrents as far as
    // it, by the table's key. None with no torrent.
    pub(crate) fn last_torrent_job(&self) -> Result<Option<JobId>, StateError> {
        self.torrent_read();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(job_id) AS last FROM torrents",
                &[],
            )
            .await?;
            row.map(|row| row.opt_i64("last"))
                .transpose()?
                .flatten()
                .map(|last| {
                    u64::try_from(last)
                        .map(JobId)
                        .map_err(|_| corrupt("job", last))
                })
                .transpose()
        })
    }
}

// The bytes each stored torrent's metainfo takes, read from the value's
// length alone.
const METAINFO_SIZES: &str = "SELECT job_id, CAST(COALESCE(length(metainfo), 0) AS BIGINT) AS bytes \
                                FROM torrents WHERE retiring IS NULL";

fn sized(row: &SqlRow) -> Result<(JobId, u64), StateError> {
    Ok((JobId(count(row, "job_id")?), count(row, "bytes")?))
}

// One torrent's rows as a boot reads them.
pub(crate) struct RestoredTorrent {
    pub(crate) record: TorrentRecord,
    // Why its stored metainfo did not decrypt, when it did not.
    pub(crate) sealed: Option<String>,
    pub(crate) levels: Result<Vec<FilePriority>, StateError>,
    pub(crate) seed_verified: Result<Option<Vec<u8>>, StateError>,
    // Whether its held paths are its own; when not, what unpacking placed
    // beside its files, read only for it, to write them from; None when
    // that list does not read.
    pub(crate) held_set: bool,
    pub(crate) unheld_outputs: Option<Vec<String>>,
    // Its trackers, in the order [`Database::list_torrent_trackers`] gives,
    // or why one of its URLs did not decrypt.
    pub(crate) trackers: Result<Vec<TrackerEntry>, String>,
    // Its ledger, saved peers and kept pieces; None with no row of them.
    pub(crate) counters: Option<crate::torrents::persistence::CountersRow>,
    // Why unpacking left it short, as its history keeps it failed or
    // partial: what [`Database::get_job_history`] gives a torrent's restore.
    pub(crate) failure: Option<String>,
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
