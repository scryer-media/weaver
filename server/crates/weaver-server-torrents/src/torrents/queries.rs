// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::torrents::model::{
    ContentLayout, FilePriority, Infohash, KeptIntent, TorrentCounters, TorrentRecord,
    TorrentSource, TrackerEntry, UnpackEnd, UnpackResult,
};
use crate::torrents::model::{parse_file_priorities, parse_named_by};
use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::encryption::EncryptionKey;
use weaver_server_db::persistence::sql_runtime::{
    SqlArg, SqlRow, SqlRuntime, SqlTx, max_rows_for_engine,
};
use weaver_server_settings::settings::SeedingPolicy;

pub const TORRENT_COLUMNS: &str = "job_id, infohash_v1, infohash_v2, name, source, metainfo, \
     category_id, added_at_ms, state, forced, completed_at_ms, storage_dir, \
     policy_override, unpack, unpack_in_force, metadata, upload_limit, download_limit, \
     sequential, first_last_piece_first, save_path, content_layout, files_dir, seed_mode, move_to, \
     queue_position, pool_member, private, unpack_over, download_ends, unpack_end, unpack_why";

pub fn corrupt(what: &str, value: impl std::fmt::Display) -> StateError {
    StateError::Database(format!("stored torrent {what} is not valid: {value}"))
}

fn hash<const N: usize>(row: &SqlRow, column: &str) -> Result<Option<[u8; N]>, StateError> {
    row.opt_bytes(column)?
        .map(|bytes| {
            <[u8; N]>::try_from(bytes.as_slice()).map_err(|_| corrupt(column, bytes.len()))
        })
        .transpose()
}

pub fn count(row: &SqlRow, column: &str) -> Result<u64, StateError> {
    let value = row.i64(column)?;
    u64::try_from(value).map_err(|_| corrupt(column, value))
}

// A tracker row, its URL opened; the error is why the URL did not open.
pub fn tracker_entry(
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

pub fn torrent_from_row(
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
pub fn torrent_and_seal(
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
        unpack_end: unpack_end_of(row)?,
        download_ends: opt_u64(row, "download_ends")?.unwrap_or_default(),
    };
    Ok((record, sealed))
}

// How its unpacking came out, as its row keeps it; none while it is not
// over, or when it never unpacked.
fn unpack_end_of(row: &SqlRow) -> Result<Option<UnpackEnd>, StateError> {
    let Some(result) = row.opt_text("unpack_end")? else {
        return Ok(None);
    };
    let result = UnpackResult::parse(&result).ok_or_else(|| corrupt("unpack result", &result))?;
    Ok(Some(UnpackEnd {
        result,
        why: row.opt_text("unpack_why")?,
    }))
}

// The torrents going by any of `infohashes`; it matches none when empty.
pub fn infohash_filter(infohashes: &[Infohash]) -> (String, Vec<SqlArg>) {
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
pub struct CategoryIds {
    pub ids: Vec<u32>,
    pub none: bool,
    pub known: Vec<u32>,
}

/// Which torrents a list reads from the table: every one, or those a
/// category match, named hashes and a privacy keep.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TorrentListFilter {
    pub categories: Option<CategoryIds>,
    /// The torrents whose hash is one of these: the v1 hash, or the v2 hash
    /// cut to 20 bytes for a torrent with no v1 hash.
    pub hashes: Option<Vec<[u8; 20]>>,
    /// The torrents whose metainfo marks them private, or those it does not.
    pub private: Option<bool>,
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
    engine: weaver_server_db::persistence::sql_runtime::SqlEngine,
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
pub fn list_statements(
    engine: weaver_server_db::persistence::sql_runtime::SqlEngine,
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
pub fn count_statements(
    engine: weaver_server_db::persistence::sql_runtime::SqlEngine,
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
pub fn binds(n: usize) -> String {
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
pub async fn named_torrents(
    datastore: &weaver_server_db::persistence::sql_runtime::StoreDatastore,
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
pub async fn torrent_by_infohash_tx(
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

pub fn counters_from_row(row: &SqlRow) -> Result<TorrentCounters, StateError> {
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
pub fn file_priorities_from_row(row: &SqlRow) -> Result<Vec<FilePriority>, StateError> {
    let Some(value) = row.opt_text("file_priorities")? else {
        return Ok(Vec::new());
    };
    parse_file_priorities(&value).ok_or_else(|| corrupt("file priorities", &value))
}
