// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Tags: free labels on a job, keyed by its id, the same for a Usenet job and
//! a torrent. A job keeps its tags into history. Nothing in Weaver reads a tag
//! to decide anything: no rule, route, schedule, seeding policy or queue
//! order looks at one.
//!
//! The rules follow qBittorrent's (`base/tag.cpp`, `base/tagset.h`):
//!
//! - A tag is trimmed of white space at both ends.
//! - An empty tag, or one with a comma in it, is not a tag. It is left out,
//!   never an error.
//! - Two tags are the same only when they match exactly: "TV" and "tv" are
//!   two tags.
//! - Tags are listed in natural order, case aside first, then by case.
//! - A list of tags is comma separated; empty parts are skipped.
//! - Putting a tag on a job makes the tag too. Deleting a tag takes it off
//!   every job.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::StateError;
use crate::jobs::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, max_rows_for_tx};

/// A tag as given, trimmed; `None` when it is not a tag: empty, or with a
/// comma in it (qBittorrent `Tag::isValid`).
pub fn clean_tag(raw: &str) -> Option<String> {
    let tag = raw.trim();
    (!tag.is_empty() && !tag.contains(',')).then(|| tag.to_owned())
}

/// The tags a comma-separated list names, each trimmed, in the order given
/// and each once. Empty parts are skipped (qBittorrent splits with
/// `Qt::SkipEmptyParts`), and a part that is not a tag is left out.
pub fn split_tags(list: &str) -> Vec<String> {
    clean_tags(list.split(','))
}

/// The tags among `raw`, each trimmed, in the order given and each once.
pub fn clean_tags<'a>(raw: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut seen = HashSet::new();
    raw.into_iter()
        .filter_map(clean_tag)
        .filter(|tag| seen.insert(tag.clone()))
        .collect()
}

/// The order tags are listed in: natural order with case aside, then by
/// case, so "tag2" comes before "tag10" and "TV" before "tv" (qBittorrent
/// `TagLessThan`).
pub fn tag_order(left: &str, right: &str) -> Ordering {
    natural(left, right, true).then_with(|| natural(left, right, false))
}

/// `tags` sorted in [`tag_order`], each once.
pub fn sorted_tags(mut tags: Vec<String>) -> Vec<String> {
    tags.sort_by(|left, right| tag_order(left, right));
    tags.dedup();
    tags
}

// Two strings compared a character at a time, a run of digits against a
// run of digits by its value (qBittorrent `Utils::Compare::naturalCompare`).
// A string that runs out first is the smaller.
fn natural(left: &str, right: &str, fold_case: bool) -> Ordering {
    let left: Vec<char> = left.chars().collect();
    let right: Vec<char> = right.chars().collect();
    let fold = |ch: char| {
        if fold_case {
            ch.to_lowercase().next().unwrap_or(ch)
        } else {
            ch
        }
    };
    let (mut at_left, mut at_right) = (0, 0);
    loop {
        if at_left == left.len() || at_right == right.len() {
            return left.len().cmp(&right.len());
        }
        let (one, other) = (fold(left[at_left]), fold(right[at_right]));
        if one.is_ascii_digit() && other.is_ascii_digit() {
            let number = |chars: &[char], at: &mut usize| {
                let start = *at;
                while *at < chars.len() && chars[*at].is_ascii_digit() {
                    *at += 1;
                }
                chars[start..*at].to_vec()
            };
            let one = number(&left, &mut at_left);
            let other = number(&right, &mut at_right);
            // The longer run is the larger number; runs of one length
            // compare digit by digit.
            let order = one.len().cmp(&other.len()).then_with(|| one.cmp(&other));
            if order != Ordering::Equal {
                return order;
            }
        } else if one == other {
            at_left += 1;
            at_right += 1;
        } else {
            return one.cmp(&other);
        }
    }
}

/// Every tag made, with how many jobs carry it, queued or in history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagCount {
    pub name: String,
    pub jobs: u64,
}

/// The tags as stored, read whole: every tag made, and each job's tags.
#[derive(Debug, Default)]
pub struct TagSnapshot {
    revision: u64,
    made: Vec<String>,
    by_job: HashMap<u64, Vec<String>>,
}

impl TagSnapshot {
    /// The revision of the tags this snapshot shows. It moves on every
    /// change to them.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// A job's tags, in [`tag_order`].
    pub fn of(&self, job_id: JobId) -> &[String] {
        self.by_job.get(&job_id.0).map_or(&[], Vec::as_slice)
    }

    /// Every tag made, in [`tag_order`].
    pub fn made(&self) -> &[String] {
        &self.made
    }

    /// Every tag made, with how many jobs carry it.
    pub fn counts(&self) -> Vec<TagCount> {
        let mut counts: HashMap<&str, u64> = HashMap::new();
        for tags in self.by_job.values() {
            for tag in tags {
                *counts.entry(tag.as_str()).or_default() += 1;
            }
        }
        self.made
            .iter()
            .map(|name| TagCount {
                name: name.clone(),
                jobs: counts.get(name.as_str()).copied().unwrap_or(0),
            })
            .collect()
    }

    /// Whether a job passes a tag filter: it carries `tag`, or, for the
    /// empty tag, carries none (qBittorrent `TorrentFilter::matchTag`).
    pub fn passes(&self, job_id: JobId, tag: &str) -> bool {
        let tags = self.of(job_id);
        let tag = tag.trim();
        if tag.is_empty() {
            return tags.is_empty();
        }
        tags.iter().any(|carried| carried == tag)
    }
}

// What the database keeps of the tags in memory: the last snapshot read,
// dropped on every change, and the revision of the changes.
pub(crate) struct TagCache {
    state: Mutex<CacheState>,
    changes: watch::Sender<u64>,
}

#[derive(Default)]
struct CacheState {
    snapshot: Option<Arc<TagSnapshot>>,
    // Moves on every change. A snapshot read across a change is not kept.
    generation: u64,
}

impl Default for TagCache {
    fn default() -> Self {
        Self {
            state: Mutex::new(CacheState::default()),
            changes: watch::channel(0).0,
        }
    }
}

impl TagCache {
    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // Drops the snapshot and tells every watcher the tags changed.
    pub(crate) fn changed(&self) {
        let generation = {
            let mut state = self.lock();
            state.snapshot = None;
            state.generation += 1;
            state.generation
        };
        self.changes.send_replace(generation);
    }
}

// One statement's worth of ids at a time, each as a bind.
fn id_args(ids: &[i64]) -> (String, Vec<SqlArg>) {
    (
        vec!["{}"; ids.len()].join(", "),
        ids.iter().copied().map(SqlArg::I64).collect(),
    )
}

fn stored_ids(job_ids: &[JobId]) -> Result<Vec<i64>, StateError> {
    let mut ids = job_ids
        .iter()
        .map(|job_id| {
            i64::try_from(job_id.0).map_err(|_| StateError::Database("job id is too large".into()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

// The ids among `ids` of a job Weaver has: queued, a torrent, or in history.
async fn known_jobs_tx(tx: &mut SqlTx<'_>, ids: &[i64]) -> Result<Vec<i64>, StateError> {
    let mut known = BTreeSet::new();
    for chunk in ids.chunks(max_rows_for_tx(tx, 3)) {
        let (placeholders, args) = id_args(chunk);
        let sql = format!(
            "SELECT job_id FROM active_jobs WHERE job_id IN ({placeholders})
             UNION SELECT job_id FROM torrents WHERE job_id IN ({placeholders})
             UNION SELECT job_id FROM job_history WHERE job_id IN ({placeholders})"
        );
        let args: Vec<SqlArg> = args.iter().cloned().cycle().take(args.len() * 3).collect();
        for row in tx.fetch_all(&sql, &args).await? {
            known.insert(row.i64("job_id")?);
        }
    }
    Ok(known.into_iter().collect())
}

async fn make_tags_tx(tx: &mut SqlTx<'_>, tags: &[String]) -> Result<(), StateError> {
    for tag in tags {
        tx.execute(
            "INSERT INTO tags (name) VALUES ({}) ON CONFLICT(name) DO NOTHING",
            &[SqlArg::Text(tag.clone())],
        )
        .await?;
    }
    Ok(())
}

async fn put_tags_tx(tx: &mut SqlTx<'_>, ids: &[i64], tags: &[String]) -> Result<(), StateError> {
    // A tag is made as a job takes it: put on no job, it is not made
    // (qBittorrent `TorrentImpl::addTag`).
    if ids.is_empty() {
        return Ok(());
    }
    make_tags_tx(tx, tags).await?;
    for &id in ids {
        for tag in tags {
            tx.execute(
                "INSERT INTO job_tags (job_id, tag) VALUES ({}, {})
                 ON CONFLICT(job_id, tag) DO NOTHING",
                &[SqlArg::I64(id), SqlArg::Text(tag.clone())],
            )
            .await?;
        }
    }
    Ok(())
}

// Takes `tags` off each job, or every tag when `tags` is `None`.
async fn take_tags_tx(
    tx: &mut SqlTx<'_>,
    ids: &[i64],
    tags: Option<&[String]>,
) -> Result<(), StateError> {
    for chunk in ids.chunks(max_rows_for_tx(tx, 1)) {
        let (placeholders, mut args) = id_args(chunk);
        match tags {
            None => {
                tx.execute(
                    &format!("DELETE FROM job_tags WHERE job_id IN ({placeholders})"),
                    &args,
                )
                .await?;
            }
            Some(tags) => {
                for tag in tags {
                    args.push(SqlArg::Text(tag.clone()));
                    tx.execute(
                        &format!(
                            "DELETE FROM job_tags WHERE job_id IN ({placeholders}) AND tag = {{}}"
                        ),
                        &args,
                    )
                    .await?;
                    args.pop();
                }
            }
        }
    }
    Ok(())
}

// One change to the tags, with its tags already cleaned.
#[derive(Clone)]
enum Change {
    // Make these tags.
    Make(Vec<String>),
    // Delete these tags, taking them off every job.
    Delete(Vec<String>),
    // Put these tags on each job Weaver has, making them.
    Add(Vec<String>),
    // Put these tags on each job named, Weaver has it yet or not.
    Put(Vec<String>),
    // Take these tags off each job.
    Remove(Vec<String>),
    // Take every tag off each job.
    Clear,
    // Give each job Weaver has exactly these tags.
    Set(Vec<String>),
}

impl Change {
    async fn apply(&self, tx: &mut SqlTx<'_>, ids: Vec<i64>) -> Result<(), StateError> {
        match self {
            Self::Make(tags) => make_tags_tx(tx, tags).await,
            Self::Delete(tags) => {
                for tag in tags {
                    let arg = [SqlArg::Text(tag.clone())];
                    tx.execute("DELETE FROM job_tags WHERE tag = {}", &arg)
                        .await?;
                    tx.execute("DELETE FROM tags WHERE name = {}", &arg).await?;
                }
                Ok(())
            }
            Self::Add(tags) => {
                let ids = known_jobs_tx(tx, &ids).await?;
                put_tags_tx(tx, &ids, tags).await
            }
            Self::Put(tags) => put_tags_tx(tx, &ids, tags).await,
            Self::Remove(tags) => take_tags_tx(tx, &ids, Some(tags)).await,
            Self::Clear => take_tags_tx(tx, &ids, None).await,
            Self::Set(tags) => {
                let ids = known_jobs_tx(tx, &ids).await?;
                take_tags_tx(tx, &ids, None).await?;
                put_tags_tx(tx, &ids, tags).await
            }
        }
    }
}

// Every tag row of the jobs `job_ids` names goes, with their history.
pub(crate) async fn delete_job_tags_tx(
    tx: &mut SqlTx<'_>,
    job_ids: &[i64],
) -> Result<(), StateError> {
    take_tags_tx(tx, job_ids, None).await
}

impl Database {
    /// Every tag and each job's tags. Read from the database once, then from
    /// memory until the tags next change.
    pub fn tag_snapshot(&self) -> Result<Arc<TagSnapshot>, StateError> {
        let generation = {
            let state = self.tags.lock();
            if let Some(snapshot) = &state.snapshot {
                return Ok(snapshot.clone());
            }
            state.generation
        };
        let datastore = self.datastore();
        let (made, rows) = self.run_sql_blocking_read(async move {
            let made = SqlRuntime::fetch_all(datastore.read_exec(), "SELECT name FROM tags", &[])
                .await?
                .iter()
                .map(|row| row.text("name"))
                .collect::<Result<Vec<_>, _>>()?;
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, tag FROM job_tags",
                &[],
            )
            .await?
            .iter()
            .map(|row| Ok((row.i64("job_id")?, row.text("tag")?)))
            .collect::<Result<Vec<_>, StateError>>()?;
            Ok((made, rows))
        })?;
        let mut by_job: HashMap<u64, Vec<String>> = HashMap::new();
        for (job_id, tag) in rows {
            by_job.entry(job_id as u64).or_default().push(tag);
        }
        for tags in by_job.values_mut() {
            *tags = sorted_tags(std::mem::take(tags));
        }
        let snapshot = Arc::new(TagSnapshot {
            revision: generation,
            made: sorted_tags(made),
            by_job,
        });
        let mut state = self.tags.lock();
        if state.generation == generation {
            state.snapshot = Some(snapshot.clone());
        }
        Ok(snapshot)
    }

    /// The tags as last read, when nothing changed them since: read from
    /// memory alone, never from the database.
    pub fn cached_tag_snapshot(&self) -> Option<Arc<TagSnapshot>> {
        self.tags.lock().snapshot.clone()
    }

    /// The revision of the tags: it moves on every change to them.
    pub fn tag_revision(&self) -> u64 {
        *self.tags.changes.borrow()
    }

    /// Wakes on every change to the tags.
    pub fn subscribe_tag_changes(&self) -> watch::Receiver<u64> {
        self.tags.changes.subscribe()
    }

    /// One job's tags, in [`tag_order`]: what a script for the job is told.
    pub fn job_tags(&self, job_id: JobId) -> Result<Vec<String>, StateError> {
        Ok(self.tag_snapshot()?.of(job_id).to_vec())
    }

    // Makes one change to the tags in one transaction, then tells every
    // watcher the tags changed.
    fn change_tags(
        &self,
        op_name: &'static str,
        job_ids: &[JobId],
        change: Change,
    ) -> Result<(), StateError> {
        let ids = stored_ids(job_ids)?;
        let datastore = self.datastore();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, op_name, |tx| {
                let ids = ids.clone();
                let change = change.clone();
                Box::pin(async move { change.apply(tx, ids).await })
            })
            .await
        });
        self.tags.changed();
        result
    }

    /// Makes each tag in `tags` that is a tag (qBittorrent `createTags`).
    pub fn create_tags<'a>(
        &self,
        tags: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), StateError> {
        let tags = clean_tags(tags);
        if tags.is_empty() {
            return Ok(());
        }
        self.change_tags("create_tags", &[], Change::Make(tags))
    }

    /// Deletes each tag in `tags`, taking it off every job that carries it
    /// (qBittorrent `deleteTags`).
    pub fn delete_tags<'a>(
        &self,
        tags: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), StateError> {
        let tags = clean_tags(tags);
        if tags.is_empty() {
            return Ok(());
        }
        self.change_tags("delete_tags", &[], Change::Delete(tags))
    }

    /// Puts `tags` on each job Weaver has among `job_ids`, making any tag
    /// not made yet (qBittorrent `addTags`). An id of no job is passed over.
    pub fn add_job_tags<'a>(
        &self,
        job_ids: &[JobId],
        tags: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), StateError> {
        let tags = clean_tags(tags);
        if tags.is_empty() || job_ids.is_empty() {
            return Ok(());
        }
        self.change_tags("add_job_tags", job_ids, Change::Add(tags))
    }

    /// Takes `tags` off each job among `job_ids`; with no tag given, takes
    /// every tag off them (qBittorrent `removeTags`). The tags stay made.
    pub fn remove_job_tags<'a>(
        &self,
        job_ids: &[JobId],
        tags: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), StateError> {
        let tags = clean_tags(tags);
        if job_ids.is_empty() {
            return Ok(());
        }
        let change = if tags.is_empty() {
            Change::Clear
        } else {
            Change::Remove(tags)
        };
        self.change_tags("remove_job_tags", job_ids, change)
    }

    /// Gives each job Weaver has among `job_ids` exactly `tags`: the tags it
    /// carries and `tags` does not name come off, the rest go on, making any
    /// not made yet (qBittorrent `setTags`).
    pub fn set_job_tags<'a>(
        &self,
        job_ids: &[JobId],
        tags: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), StateError> {
        let tags = clean_tags(tags);
        if job_ids.is_empty() {
            return Ok(());
        }
        self.change_tags("set_job_tags", job_ids, Change::Set(tags))
    }

    /// Puts the tags a job is submitted with on it as it is added, before
    /// it is in the queue's tables.
    pub fn tag_new_job<'a>(
        &self,
        job_id: JobId,
        tags: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), StateError> {
        let tags = clean_tags(tags);
        if tags.is_empty() {
            return Ok(());
        }
        self.change_tags("tag_new_job", &[job_id], Change::Put(tags))
    }

    /// Takes every tag off a job that was never added after all.
    pub fn untag_job(&self, job_id: JobId) -> Result<(), StateError> {
        self.change_tags("untag_job", &[job_id], Change::Clear)
    }

    /// Every tag made, with how many jobs carry it, in [`tag_order`].
    pub fn tag_counts(&self) -> Result<Vec<TagCount>, StateError> {
        Ok(self.tag_snapshot()?.counts())
    }
}

#[cfg(test)]
pub(crate) mod tests;
