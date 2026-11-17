// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;

use crate::StateError;
use crate::history::record::JobHistoryRow;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use crate::settings::SeedingPolicy;
use crate::settings::model::tracker_host;
use crate::torrents::model::{HistoryEnd, file_priorities_str, named_by_str};
use crate::torrents::queries::{TORRENT_COLUMNS, count, counters_from_row, torrent_by_infohash_tx};
use crate::torrents::store::Kept;
use crate::torrents::{
    FilePriority, KeptIntent, TorrentCounters, TorrentInsert, TorrentIntent, TorrentRecord,
    TrackerEntry,
};
use weaver_torrent::ResumePeers;

// A tracker row holds where the tracker sits in the announce list and who
// named it; how announcing to it goes is read from the session.
const TRACKER_INSERT: &str = "INSERT INTO torrent_trackers (job_id, tier, url, named_by) VALUES ";
const TRACKER_ROW: &str = "({}, {}, {}, {})";
// Every host a torrent has announced to stays known after the torrent is
// gone, so a tracker rule can be picked from them. The URL is not kept: a
// private tracker's carries the operator's passkey.
const TRACKER_HOST_INSERT: &str = "INSERT INTO tracker_hosts (host) VALUES ";
const TRACKER_HOST_ROW: &str = "({})";
const TRACKER_HOST_CONFLICT: &str = " ON CONFLICT (host) DO NOTHING";

/// Seed mode as a pass over the torrents found it, written together by
/// [`Database::write_torrent_seed_modes`].
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TorrentSeedWrites {
    /// Left seed mode: no check passed in it is kept.
    pub(crate) left: Vec<JobId>,
    /// The pieces each still in it has checked, a bitfield as the wire's.
    pub(crate) verified: Vec<(JobId, Vec<u8>)>,
}

impl TorrentSeedWrites {
    pub(crate) fn is_empty(&self) -> bool {
        self.left.is_empty() && self.verified.is_empty()
    }
}

// The statement that writes a torrent's ledger and the peers and bans it
// begins with, each only when given; none when neither is.
fn resume_update(
    job_id: JobId,
    counters: Option<&TorrentCounters>,
    peers: Option<&ResumePeers>,
) -> Result<Option<(String, Vec<SqlArg>)>, StateError> {
    let mut sets = Vec::new();
    let mut args = Vec::new();
    if let Some(counters) = counters {
        sets.push(
            "uploaded_bytes = {}, downloaded_bytes = {}, active_duration_s = {}, \
             last_seen_complete_ms = {}, finished_duration_s = {}, last_activity_ms = {}, \
             consecutive_seeding_s = {}, failed_bytes = {}, redundant_bytes = {}",
        );
        args.extend([
            SqlArg::I64(stored(counters.uploaded_bytes)?),
            SqlArg::I64(stored(counters.downloaded_bytes)?),
            SqlArg::I64(stored(counters.active_duration_s)?),
            SqlArg::OptI64(counters.last_seen_complete_ms),
            SqlArg::I64(stored(counters.finished_duration_s)?),
            SqlArg::OptI64(counters.last_activity_ms),
            SqlArg::I64(stored(counters.consecutive_seeding_s)?),
            SqlArg::I64(stored(counters.failed_bytes)?),
            SqlArg::I64(stored(counters.redundant_bytes)?),
        ]);
    }
    if let Some(peers) = peers {
        sets.push("peers = {}, banned_peers = {}, found_peers = {}");
        args.extend([
            SqlArg::OptText(stored_list(&peers.peers)?),
            SqlArg::OptText(stored_list(&peers.banned)?),
            SqlArg::OptText(stored_list(&peers.found)?),
        ]);
    }
    if sets.is_empty() {
        return Ok(None);
    }
    args.push(SqlArg::I64(stored(job_id.0)?));
    let sql = format!(
        "UPDATE torrent_counters SET {} WHERE job_id = {{}}",
        sets.join(", ")
    );
    Ok(Some((sql, args)))
}

// The one place a torrent's tracker rows are written: each row, and the
// host of each, recorded as `tracker_host` spells it.
async fn insert_trackers(
    tx: &mut SqlTx<'_>,
    job_id: i64,
    trackers: &[TrackerEntry],
) -> Result<(), StateError> {
    // As many rows a statement as its binds allow.
    for chunk in trackers.chunks(crate::persistence::sql_runtime::max_rows_for_tx(tx, 4)) {
        let sql = format!(
            "{TRACKER_INSERT}{}",
            vec![TRACKER_ROW; chunk.len()].join(", ")
        );
        let mut args = Vec::with_capacity(chunk.len() * 4);
        for tracker in chunk {
            args.extend([
                SqlArg::I64(job_id),
                SqlArg::I64(i64::from(tracker.tier)),
                SqlArg::Text(tracker.url.clone()),
                SqlArg::Text(named_by_str(tracker.named_by).to_owned()),
            ]);
        }
        tx.execute(&sql, &args).await?;
    }
    let hosts: Vec<String> = trackers
        .iter()
        .filter_map(|tracker| tracker_host(&tracker.url))
        .collect::<std::collections::BTreeSet<String>>()
        .into_iter()
        .collect();
    for chunk in hosts.chunks(crate::persistence::sql_runtime::max_rows_for_tx(tx, 1)) {
        let sql = format!(
            "{TRACKER_HOST_INSERT}{}{TRACKER_HOST_CONFLICT}",
            vec![TRACKER_HOST_ROW; chunk.len()].join(", ")
        );
        let args: Vec<SqlArg> = chunk.iter().cloned().map(SqlArg::Text).collect();
        tx.execute(&sql, &args).await?;
    }
    Ok(())
}

// One column of each torrent `rows` names, each row its id and the value,
// as `UPDATE ... CASE` statements of as many rows as their binds allow.
// A torrent named twice takes the value named last, as one statement per
// row in turn left it.
async fn update_column_tx(
    tx: &mut SqlTx<'_>,
    column: &str,
    rows: &[(i64, SqlArg)],
) -> Result<(), StateError> {
    let mut seen = std::collections::HashSet::with_capacity(rows.len());
    let mut last: Vec<&(i64, SqlArg)> = rows
        .iter()
        .rev()
        .filter(|(id, _)| seen.insert(*id))
        .collect();
    last.reverse();
    // Three binds a row: the CASE arm's id and value, and the id in the
    // list.
    for chunk in last.chunks(crate::persistence::sql_runtime::max_rows_for_tx(tx, 3)) {
        let sql = format!(
            "UPDATE torrents SET {column} = CASE job_id {}END WHERE job_id IN ({})",
            "WHEN {} THEN {} ".repeat(chunk.len()),
            vec!["{}"; chunk.len()].join(", ")
        );
        let mut args = Vec::with_capacity(chunk.len() * 3);
        for (id, value) in chunk {
            args.push(SqlArg::I64(*id));
            args.push(value.clone());
        }
        args.extend(chunk.iter().map(|(id, _)| SqlArg::I64(*id)));
        tx.execute(&sql, &args).await?;
    }
    Ok(())
}

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
                Box::pin(async move {
                    crate::history::persistence::insert_job_history_tx(tx, &entry).await?;
                    write_history_files_tx(tx, id, &files).await?;
                    if retires {
                        delete_torrent_tx(tx, id).await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }
}

impl Database {
    // A removal's mark, before anything of the torrent goes: the flag that
    // it is leaving, and whether its files go with it (`delete_files`). A
    // stop from then on brings it back retiring, never live, and the next
    // start finishes the removal from its row. One small UPDATE.
    pub(crate) fn mark_torrent_retiring(
        &self,
        job_id: JobId,
        delete_files: bool,
    ) -> Result<(), StateError> {
        #[cfg(test)]
        match MARK_REFUSED.load(std::sync::atomic::Ordering::Relaxed) {
            1 => return Err(StateError::OutOfSpace("database or disk is full".into())),
            2 => return Err(StateError::Database("the mark was refused".into())),
            _ => {}
        }
        let args = vec![
            SqlArg::Text(RetiringMark::of(delete_files).as_str().to_owned()),
            SqlArg::I64(stored(job_id.0)?),
        ];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET retiring = {} WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    // The removal marks of a batch of torrents (`mark_torrent_retiring`),
    // in one transaction of few statements.
    pub(crate) fn mark_torrents_retiring(&self, marks: &[(JobId, bool)]) -> Result<(), StateError> {
        #[cfg(test)]
        match MARK_REFUSED.load(std::sync::atomic::Ordering::Relaxed) {
            1 => return Err(StateError::OutOfSpace("database or disk is full".into())),
            2 => return Err(StateError::Database("the mark was refused".into())),
            _ => {}
        }
        if marks.is_empty() {
            return Ok(());
        }
        let rows = marks
            .iter()
            .map(|&(job_id, delete_files)| {
                Ok((
                    stored(job_id.0)?,
                    SqlArg::Text(RetiringMark::of(delete_files).as_str().to_owned()),
                ))
            })
            .collect::<Result<Vec<_>, StateError>>()?;
        let rows = std::sync::Arc::new(rows);
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "mark_torrents_retiring", |tx| {
                let rows = std::sync::Arc::clone(&rows);
                Box::pin(async move { update_column_tx(tx, "retiring", &rows).await })
            })
            .await
        })
    }

    // Whether a removal has marked the torrent of `job_id` (`retiring`): an
    // add of the same torrent is refused as still being removed, not as a
    // duplicate. One read by key, only when an add finds the torrent.
    pub(crate) fn torrent_is_retiring(&self, job_id: JobId) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT retiring FROM torrents WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_text("retiring"))
                .transpose()?
                .flatten()
                .is_some())
        })
    }

    // Every torrent a removal left retiring, with what of its row finishing
    // the removal takes, and the history row its job has, if any. One
    // statement, served by the retiring index.
    pub(crate) fn retiring_torrents(&self) -> Result<Vec<RetiringRow>, StateError> {
        #[cfg(test)]
        if RETIRING_READ_FAILS.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(StateError::Database(
                "the retiring torrents do not read".into(),
            ));
        }
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT t.job_id, t.retiring, t.name, t.infohash_v1, t.infohash_v2, t.metainfo, \
                        t.storage_dir, t.files_dir, t.outputs, t.adopted, t.added_at_ms, \
                        t.completed_at_ms, h.status AS had_status, \
                        h.error_message AS had_error, h.total_bytes AS had_total, \
                        h.downloaded_bytes AS had_downloaded, h.category AS had_category, \
                        h.metadata AS had_metadata \
                   FROM torrents t LEFT JOIN job_history h ON h.job_id = t.job_id \
                  WHERE t.retiring IS NOT NULL",
                &[],
            )
            .await?;
            rows.iter().map(retiring_row).collect()
        })
    }

    // Each of `rows`, a retiring torrent's history row and the files it
    // lists, to history and out of the torrents: all in one transaction,
    // and when that fails, each in its own, so one row that cannot be
    // written holds back none of the others. A row left retiring never
    // loads as a live torrent; the service shows it and writes it through
    // the kept archives (`TorrentService::retry_retiring`).
    pub(crate) fn archive_retiring_torrents(
        &self,
        rows: Vec<(JobId, JobHistoryRow, Vec<HistoryFile>)>,
    ) -> RetiringArchived {
        let mut archived = RetiringArchived::default();
        if rows.is_empty() {
            return archived;
        }
        match self.write_retiring(&rows) {
            Ok(written) => archived.written = written,
            Err(error) => {
                tracing::warn!(error = %error, "could not archive the retiring torrents at once; each goes on its own");
                for row in rows {
                    let job_id = row.0;
                    match self.write_retiring(std::slice::from_ref(&row)) {
                        Ok(one) => archived.written.extend(one),
                        Err(error) => {
                            tracing::warn!(job_id = job_id.0, error = %error, "could not archive a retiring torrent; it stays retiring and is retried");
                            archived.left.push(job_id);
                        }
                    }
                }
            }
        }
        archived
    }

    // `rows` to history, out of the torrents, in one transaction.
    fn write_retiring(
        &self,
        rows: &[(JobId, JobHistoryRow, Vec<HistoryFile>)],
    ) -> Result<Vec<JobId>, StateError> {
        #[cfg(test)]
        if let Some((job_id, ..)) = rows
            .iter()
            .find(|(job_id, ..)| RETIRING_REFUSED.lock().unwrap().contains(job_id))
        {
            return Err(StateError::Database(format!(
                "refused the retiring torrent {}",
                job_id.0
            )));
        }
        let rows = rows.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "archive_retiring_torrents", |tx| {
                let rows = rows.clone();
                Box::pin(async move {
                    let mut written = Vec::with_capacity(rows.len());
                    for (job_id, entry, files) in &rows {
                        let id = stored(job_id.0)?;
                        crate::history::persistence::insert_job_history_tx(tx, entry).await?;
                        write_history_files_tx(tx, id, files).await?;
                        delete_torrent_tx(tx, id).await?;
                        written.push(*job_id);
                    }
                    Ok(written)
                })
            })
            .await
        })
    }
}

// Test seam: the retiring torrents a start's archive refuses to write, in
// a batch or alone.
#[cfg(test)]
pub(crate) static RETIRING_REFUSED: std::sync::Mutex<Vec<JobId>> =
    std::sync::Mutex::new(Vec::new());

// Test seam: a removal's mark is refused, 1 for want of room, 2 otherwise.
#[cfg(test)]
pub(crate) static MARK_REFUSED: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

// Test seam: the retiring torrents do not read.
#[cfg(test)]
pub(crate) static RETIRING_READ_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// What a start's archive of the retiring torrents came to: the jobs written
// to history, and those left retiring, never live, for the service to retry.
#[derive(Debug, Default)]
pub(crate) struct RetiringArchived {
    pub(crate) written: Vec<JobId>,
    pub(crate) left: Vec<JobId>,
}

// The mark a removal leaves on a torrent's row: its files kept, or asked
// to go with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetiringMark {
    Kept,
    Deleted,
}

impl RetiringMark {
    pub(crate) const ALL: [Self; 2] = [Self::Kept, Self::Deleted];

    pub(crate) const fn of(delete_files: bool) -> Self {
        if delete_files {
            Self::Deleted
        } else {
            Self::Kept
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Kept => "kept",
            Self::Deleted => "deleted",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|mark| mark.as_str() == value)
    }
}

// A retiring torrent's row, as finishing its removal at a start reads it.
#[derive(Debug)]
pub(crate) struct RetiringRow {
    pub(crate) job_id: JobId,
    // Whether its removal asked for its files.
    pub(crate) delete_files: bool,
    pub(crate) metainfo: Option<Vec<u8>>,
    pub(crate) storage_dir: Option<String>,
    pub(crate) files_dir: Option<String>,
    // None when it does not read.
    pub(crate) outputs: Option<Vec<String>>,
    pub(crate) adopted: Option<super::model::Adopted>,
    // Its history row as its torrents row gives it, kept from the one its
    // job has where there is one (a failed finish's status and why).
    pub(crate) history: JobHistoryRow,
}

fn retiring_row(row: &crate::persistence::sql_runtime::SqlRow) -> Result<RetiringRow, StateError> {
    let job_id = JobId(count(row, "job_id")?);
    let completed = row.opt_i64("completed_at_ms")?;
    let added = row.i64("added_at_ms")?;
    let hash = row
        .opt_bytes("infohash_v1")?
        .or(row.opt_bytes("infohash_v2")?);
    let had = row.opt_text("had_status")?;
    let status = had.clone().unwrap_or_else(|| {
        if completed.is_some() {
            HistoryEnd::Complete
        } else {
            HistoryEnd::Cancelled
        }
        .as_str()
        .to_owned()
    });
    let storage_dir = row.opt_text("storage_dir")?;
    let history = JobHistoryRow {
        job_id: job_id.0,
        job_hash: hash,
        name: row.text("name")?,
        status,
        error_message: if had.is_some() {
            row.opt_text("had_error")?
        } else {
            None
        },
        total_bytes: row
            .opt_i64("had_total")?
            .map_or(0, |bytes| bytes.max(0) as u64),
        downloaded_bytes: row
            .opt_i64("had_downloaded")?
            .map_or(0, |bytes| bytes.max(0) as u64),
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: row.opt_text("had_category")?,
        output_dir: storage_dir.clone(),
        nzb_path: None,
        created_at: added / 1000,
        completed_at: completed.unwrap_or(added) / 1000,
        metadata: row.opt_text("had_metadata")?,
        server_attribution: None,
        protocol: crate::jobs::Protocol::Torrent,
        archived_at: None,
    };
    Ok(RetiringRow {
        job_id,
        // A mark of any other word keeps its files, as one never asked.
        delete_files: RetiringMark::parse(&row.text("retiring")?) == Some(RetiringMark::Deleted),
        metainfo: row.opt_bytes("metainfo")?,
        storage_dir,
        files_dir: row.opt_text("files_dir")?,
        outputs: match row.opt_text("outputs")? {
            Some(text) => serde_json::from_str(&text).ok(),
            None => Some(Vec::new()),
        },
        // Never written: what was there before is not known, and the
        // start's delete takes none of the torrent's files.
        adopted: match row.opt_text("adopted")? {
            Some(text) => serde_json::from_str(&text).ok(),
            None => None,
        },
        history,
    })
}

// A file a torrent's job wrote, as its history row lists it: an absolute
// path, and whether it goes `whole`, with what is in it (what unpacking
// placed), or only as a file (the torrent's own).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct HistoryFile {
    pub(crate) path: String,
    pub(crate) whole: bool,
}

// The files a torrent's job wrote, listed with its history row in `tx` in
// place of any list before: an archive from history removes these. In
// bind-sized chunks, each one statement.
async fn write_history_files_tx(
    tx: &mut SqlTx<'_>,
    job_id: i64,
    files: &[HistoryFile],
) -> Result<(), StateError> {
    tx.execute(
        "DELETE FROM job_history_files WHERE job_id = {}",
        &[SqlArg::I64(job_id)],
    )
    .await?;
    // One row a path: one listed twice goes whole if either says so.
    let mut files: Vec<&HistoryFile> = files.iter().collect();
    files.sort_unstable_by(|a, b| a.path.cmp(&b.path).then(b.whole.cmp(&a.whole)));
    files.dedup_by(|later, kept| later.path == kept.path);
    let rows = crate::persistence::sql_runtime::max_rows_for_tx(tx, 3);
    for chunk in files.chunks(rows) {
        let values = vec!["({}, {}, {})"; chunk.len()].join(", ");
        let mut args = Vec::with_capacity(chunk.len() * 3);
        for file in chunk {
            args.push(SqlArg::I64(job_id));
            args.push(SqlArg::Text(file.path.clone()));
            args.push(SqlArg::Bool(file.whole));
        }
        tx.execute(
            &format!("INSERT INTO job_history_files (job_id, path, whole) VALUES {values}"),
            &args,
        )
        .await?;
    }
    Ok(())
}

// The files a torrent's history row lists, for an archive from history.
impl Database {
    pub(crate) fn job_history_files(
        &self,
        job_ids: &[JobId],
    ) -> Result<std::collections::BTreeMap<JobId, Vec<HistoryFile>>, StateError> {
        if job_ids.is_empty() {
            return Ok(Default::default());
        }
        let datastore = self.datastore();
        let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        self.run_sql_blocking_read(async move {
            let mut files = std::collections::BTreeMap::<JobId, Vec<HistoryFile>>::new();
            let rows_per =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 1);
            for chunk in ids.chunks(rows_per) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args = chunk.iter().copied().map(SqlArg::I64).collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, path, whole FROM job_history_files WHERE job_id IN ({placeholders})"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    files
                        .entry(JobId(count(&row, "job_id")?))
                        .or_default()
                        .push(HistoryFile {
                            path: row.text("path")?,
                            whole: row.bool("whole")?,
                        });
                }
            }
            Ok(files)
        })
    }
}

impl Database {
    // The list of every torrent job's history row among `job_ids`, empty for
    // one that lists nothing: a torrent's job is removed from by its list
    // alone, never as an NZB job's output folder is, whatever it lists. One
    // batched read of the lists and one of which rows are torrents', each
    // served by its key.
    pub(crate) fn torrent_history_lists(
        &self,
        job_ids: &[JobId],
    ) -> Result<std::collections::BTreeMap<JobId, Vec<HistoryFile>>, StateError> {
        let mut lists = self.job_history_files(job_ids)?;
        if job_ids.is_empty() {
            return Ok(lists);
        }
        let datastore = self.datastore();
        let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        let torrents = self.run_sql_blocking_read(async move {
            let mut torrents = Vec::new();
            let rows_per =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 1);
            for chunk in ids.chunks(rows_per) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let mut args = vec![SqlArg::Text(
                    crate::jobs::Protocol::Torrent.as_str().to_owned(),
                )];
                args.extend(chunk.iter().copied().map(SqlArg::I64));
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id FROM job_history WHERE protocol = {{}} AND job_id IN ({placeholders})"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    torrents.push(JobId(count(&row, "job_id")?));
                }
            }
            Ok(torrents)
        })?;
        for job_id in torrents {
            lists.entry(job_id).or_default();
        }
        Ok(lists)
    }

    // Whether `job_id` is a torrent's job in history: one read of its
    // protocol by the history key.
    pub(crate) fn history_job_is_torrent(&self, job_id: JobId) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 AS found FROM job_history WHERE job_id = {} AND protocol = {}",
                &[
                    SqlArg::I64(id),
                    SqlArg::Text(crate::jobs::Protocol::Torrent.as_str().to_owned()),
                ],
            )
            .await?;
            Ok(row.is_some())
        })
    }

    // Which of `dirs`, each with the job whose marker it carries, that job's
    // history row lists: a torrent's kept unpacking folder, recorded as an
    // NZB job's working folder is, which a start-up sweep leaves for an
    // archive to take. One statement per bind-sized chunk, each served by
    // the list's key.
    pub(crate) fn listed_history_dirs(
        &self,
        dirs: &[(JobId, PathBuf)],
    ) -> Result<std::collections::HashSet<PathBuf>, StateError> {
        if dirs.is_empty() {
            return Ok(Default::default());
        }
        let pairs = dirs
            .iter()
            .map(|(job_id, dir)| Ok((stored(job_id.0)?, dir.to_string_lossy().into_owned())))
            .collect::<Result<Vec<_>, StateError>>()?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut listed = std::collections::HashSet::new();
            let rows_per =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 2);
            for chunk in pairs.chunks(rows_per) {
                let filters = vec!["(job_id = {} AND path = {})"; chunk.len()].join(" OR ");
                let args = chunk
                    .iter()
                    .flat_map(|(job_id, path)| [SqlArg::I64(*job_id), SqlArg::Text(path.clone())])
                    .collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!("SELECT path FROM job_history_files WHERE {filters}"),
                    &args,
                )
                .await?;
                for row in rows {
                    listed.insert(PathBuf::from(row.text("path")?));
                }
            }
            Ok(listed)
        })
    }
}

impl Database {
    // The paths live torrents hold (`torrent_held_paths`), never one a
    // removal left retiring, that are one of `keys`, lie under one, or are
    // above one, each key spelled as the filesystem spells it. Served by
    // the path index: one statement for the paths under each bind-sized
    // chunk of `keys`, and one for each chunk of the folders above them.
    // What comes back is bounded by what live torrents hold there, never by
    // how many share a folder above it.
    pub(crate) fn live_held_paths(
        &self,
        keys: &[PathBuf],
    ) -> Result<std::collections::BTreeSet<PathBuf>, StateError> {
        use crate::persistence::sql_runtime::SqlEngine;
        if keys.is_empty() {
            return Ok(Default::default());
        }
        let separator = std::path::MAIN_SEPARATOR;
        let after = char::from_u32(u32::from(separator) + 1).unwrap_or(separator);
        let under: Vec<(String, String, String)> = keys
            .iter()
            .map(|key| {
                let key = key.to_string_lossy().trim_end_matches(separator).to_owned();
                (
                    key.clone(),
                    format!("{key}{separator}"),
                    format!("{key}{after}"),
                )
            })
            .collect();
        let above: Vec<String> = keys
            .iter()
            .flat_map(|key| key.ancestors().skip(1))
            .map(|key| key.to_string_lossy().into_owned())
            .filter(|key| !key.is_empty())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            // Ordered by bytes on both engines, so a range over a folder's
            // path is the paths under it.
            let path = match datastore.engine() {
                SqlEngine::Sqlite => "h.path".to_owned(),
                SqlEngine::Postgres => "h.path COLLATE \"C\"".to_owned(),
            };
            let select = "SELECT DISTINCT h.path FROM torrent_held_paths h \
                          JOIN torrents t ON t.job_id = h.job_id \
                          WHERE t.retiring IS NULL AND";
            let mut found = std::collections::BTreeSet::new();
            let rows_per =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 3);
            for chunk in under.chunks(rows_per) {
                let filters = vec![
                    format!("{path} = {{}} OR ({path} >= {{}} AND {path} < {{}})");
                    chunk.len()
                ]
                .join(" OR ");
                let args = chunk
                    .iter()
                    .flat_map(|(key, from, to)| {
                        [
                            SqlArg::Text(key.clone()),
                            SqlArg::Text(from.clone()),
                            SqlArg::Text(to.clone()),
                        ]
                    })
                    .collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!("{select} ({filters})"),
                    &args,
                )
                .await?;
                for row in rows {
                    found.insert(PathBuf::from(row.text("path")?));
                }
            }
            let rows_per =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 1);
            for chunk in above.chunks(rows_per) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args: Vec<SqlArg> = chunk.iter().cloned().map(SqlArg::Text).collect();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!("{select} {path} IN ({placeholders})"),
                    &args,
                )
                .await?;
                for row in rows {
                    found.insert(PathBuf::from(row.text("path")?));
                }
            }
            Ok(found)
        })
    }

    // What each of `torrents` holds, in place of what it held, in one
    // transaction: its rows are its own from now on.
    pub(crate) fn set_torrents_held(
        &self,
        torrents: &[(JobId, Vec<String>)],
    ) -> Result<(), StateError> {
        if torrents.is_empty() {
            return Ok(());
        }
        let torrents = torrents
            .iter()
            .map(|(job_id, held)| Ok((stored(job_id.0)?, held.clone())))
            .collect::<Result<Vec<_>, StateError>>()?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrents_held", |tx| {
                let torrents = torrents.clone();
                Box::pin(async move {
                    for (id, held) in &torrents {
                        tx.execute(
                            "DELETE FROM torrent_held_paths WHERE job_id = {}",
                            &[SqlArg::I64(*id)],
                        )
                        .await?;
                        insert_held_tx(tx, *id, held).await?;
                        tx.execute(
                            "UPDATE torrents SET held_set = {} WHERE job_id = {}",
                            &[SqlArg::Bool(true), SqlArg::I64(*id)],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // `held` added to what the torrent holds, before its files move there,
    // and what it holds marked to be written again, in one transaction: a
    // file that reaches the new folder is held from the moment it does, and
    // a move that never ends leaves the next start to write what it holds.
    pub(crate) fn add_torrent_held(
        &self,
        job_id: JobId,
        held: &[String],
    ) -> Result<(), StateError> {
        let id = stored(job_id.0)?;
        let held = held.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "add_torrent_held", |tx| {
                let held = held.clone();
                Box::pin(async move {
                    insert_held_tx(tx, id, &held).await?;
                    tx.execute(
                        "UPDATE torrents SET held_set = {} WHERE job_id = {}",
                        &[SqlArg::Bool(false), SqlArg::I64(id)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    // What the torrents not leaving hold, each row as written, or only the
    // rows of `job_id`. One statement: by the table's key for one torrent,
    // and every row once at a start, as the restore reads its torrents.
    pub(crate) fn held_rows(
        &self,
        job_id: Option<JobId>,
    ) -> Result<Vec<(JobId, String)>, StateError> {
        let only = job_id.map(|job_id| stored(job_id.0)).transpose()?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let select = "SELECT h.job_id, h.path FROM torrent_held_paths h \
                          JOIN torrents t ON t.job_id = h.job_id \
                          WHERE t.retiring IS NULL";
            let rows = match only {
                Some(id) => {
                    SqlRuntime::fetch_all(
                        datastore.read_exec(),
                        &format!("{select} AND h.job_id = {{}}"),
                        &[SqlArg::I64(id)],
                    )
                    .await?
                }
                None => SqlRuntime::fetch_all(datastore.read_exec(), select, &[]).await?,
            };
            rows.into_iter()
                .map(|row| {
                    let id = u64::try_from(row.i64("job_id")?)
                        .map_err(|error| StateError::Database(error.to_string()))?;
                    Ok((JobId(id), row.text("path")?))
                })
                .collect()
        })
    }

    // Each `(job, was, now)` row of what a torrent holds spelled `now` in
    // place of `was`, in one transaction: a row that is gone by then, as a
    // move or a removal took it, stays gone. Two statements per chunk,
    // whatever the number of torrents.
    pub(crate) fn respell_held(&self, rows: &[(JobId, String, String)]) -> Result<(), StateError> {
        if rows.is_empty() {
            return Ok(());
        }
        let rows = rows
            .iter()
            .map(|(job_id, was, now)| Ok((stored(job_id.0)?, was.clone(), now.clone())))
            .collect::<Result<Vec<_>, StateError>>()?;
        let datastore = self.datastore();
        let rows_per = crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 3);
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "respell_held", |tx| {
                let rows = rows.clone();
                Box::pin(async move {
                    for chunk in rows.chunks(rows_per) {
                        let values = vec!["({}, {}, {})"; chunk.len()].join(", ");
                        let args = chunk
                            .iter()
                            .flat_map(|(id, was, now)| {
                                [
                                    SqlArg::I64(*id),
                                    SqlArg::Text(was.clone()),
                                    SqlArg::Text(now.clone()),
                                ]
                            })
                            .collect::<Vec<_>>();
                        tx.execute(
                            &format!(
                                "WITH v (job_id, was, now) AS (VALUES {values}) \
                                 INSERT INTO torrent_held_paths (job_id, path) \
                                 SELECT v.job_id, v.now FROM v WHERE EXISTS \
                                 (SELECT 1 FROM torrent_held_paths h \
                                   WHERE h.job_id = v.job_id AND h.path = v.was) \
                                 ON CONFLICT (job_id, path) DO NOTHING"
                            ),
                            &args,
                        )
                        .await?;
                        let filters = vec!["(job_id = {} AND path = {})"; chunk.len()].join(" OR ");
                        let args = chunk
                            .iter()
                            .flat_map(|(id, was, _)| [SqlArg::I64(*id), SqlArg::Text(was.clone())])
                            .collect::<Vec<_>>();
                        tx.execute(
                            &format!("DELETE FROM torrent_held_paths WHERE {filters}"),
                            &args,
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // One torrent's [`Self::set_torrents_held`].
    pub(crate) fn set_torrent_held(
        &self,
        job_id: JobId,
        held: Vec<String>,
    ) -> Result<(), StateError> {
        self.set_torrents_held(&[(job_id, held)])
    }
}

// `held` added to what torrent `id` holds, in `tx`, a bind-sized chunk per
// statement; a path it holds already stays one row.
async fn insert_held_tx(tx: &mut SqlTx<'_>, id: i64, held: &[String]) -> Result<(), StateError> {
    let held: Vec<&String> = held
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let engine = match tx {
        SqlTx::Sqlite(_) => crate::persistence::sql_runtime::SqlEngine::Sqlite,
        SqlTx::Postgres(_) => crate::persistence::sql_runtime::SqlEngine::Postgres,
    };
    let rows_per = crate::persistence::sql_runtime::max_rows_for_engine(engine, 2);
    for chunk in held.chunks(rows_per) {
        let values = vec!["({}, {})"; chunk.len()].join(", ");
        let args = chunk
            .iter()
            .flat_map(|path| [SqlArg::I64(id), SqlArg::Text((*path).clone())])
            .collect::<Vec<_>>();
        tx.execute(
            &format!(
                "INSERT INTO torrent_held_paths (job_id, path) VALUES {values} \
                 ON CONFLICT (job_id, path) DO NOTHING"
            ),
            &args,
        )
        .await?;
    }
    Ok(())
}

fn stored(value: u64) -> Result<i64, StateError> {
    i64::try_from(value)
        .map_err(|_| StateError::Database(format!("{value} exceeds the database range")))
}

// What was on disk before a torrent's files, as JSON; NULL when nothing.
// Removes the torrent with its files, trackers, ledger and checkpoint, in
// `tx`. Returns whether it was there.
async fn delete_torrent_tx(tx: &mut SqlTx<'_>, id: i64) -> Result<bool, StateError> {
    let changed = tx
        .execute("DELETE FROM torrents WHERE job_id = {}", &[SqlArg::I64(id)])
        .await?;
    tx.execute(
        "DELETE FROM torrent_held_paths WHERE job_id = {}",
        &[SqlArg::I64(id)],
    )
    .await?;
    for table in crate::jobs::graph_checkpoint::TABLES {
        tx.execute(
            &format!("DELETE FROM {table} WHERE job_id = {{}}"),
            &[SqlArg::I64(id)],
        )
        .await?;
    }
    Ok(changed > 0)
}

// What was there before, written even when nothing was: a row whose
// `adopted` was never written (a row from before it was kept) reads as not
// known, never as nothing there (`retiring_row`).
fn adopted_text(adopted: &super::model::Adopted) -> Result<Option<String>, StateError> {
    Some(serde_json::to_string(adopted))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))
}

// A list as a JSON array of its strings; NULL when empty.
fn stored_list<T: serde::Serialize>(items: &[T]) -> Result<Option<String>, StateError> {
    (!items.is_empty())
        .then(|| serde_json::to_string(items))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))
}

fn listed<T: serde::de::DeserializeOwned>(
    column: &str,
    json: Option<String>,
) -> Result<Vec<T>, StateError> {
    json.map(|json| {
        serde_json::from_str(&json).map_err(|error| {
            StateError::Database(format!("stored torrent {column} is not valid: {error}"))
        })
    })
    .transpose()
    .map(Option::unwrap_or_default)
}

// Which saved peers were found elsewhere than the torrent's trackers and
// the operator. The marks never cost the peers or bans they sit beside: a
// value that does not read is no marks.
fn found_marks(
    job_id: JobId,
    json: Result<Option<String>, StateError>,
) -> Vec<std::net::SocketAddr> {
    json.and_then(|json| listed("found_peers", json))
        .unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "a torrent's saved peer marks did not read; its peers restore unmarked");
            Vec::new()
        })
}

fn torrent_args(torrent: &TorrentRecord) -> Result<Vec<SqlArg>, StateError> {
    let policy_override = SeedingPolicy::kept(torrent.policy_override)
        .map(|policy| serde_json::to_string(&policy))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))?;
    let metadata = (!torrent.metadata.is_empty())
        .then(|| serde_json::to_string(&torrent.metadata))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))?;
    Ok(vec![
        SqlArg::I64(stored(torrent.job_id.0)?),
        SqlArg::OptBytes(torrent.infohash_v1.map(|hash| hash.to_vec())),
        SqlArg::OptBytes(torrent.infohash_v2.map(|hash| hash.to_vec())),
        SqlArg::Text(torrent.name.clone()),
        SqlArg::Text(torrent.source.as_str().to_string()),
        SqlArg::OptBytes(torrent.metainfo.as_deref().map(<[u8]>::to_vec)),
        SqlArg::OptI64(torrent.category_id.map(i64::from)),
        SqlArg::I64(torrent.added_at_ms),
        SqlArg::Text(torrent.kept.as_str().to_string()),
        SqlArg::Bool(torrent.forced),
        SqlArg::OptI64(torrent.completed_at_ms),
        SqlArg::OptText(torrent.storage_dir.clone()),
        SqlArg::OptText(policy_override),
        SqlArg::OptBool(torrent.unpack),
        SqlArg::OptBool(torrent.unpack_in_force),
        SqlArg::OptText(metadata),
        SqlArg::OptI64(torrent.upload_limit.map(stored).transpose()?),
        SqlArg::OptI64(torrent.download_limit.map(stored).transpose()?),
        SqlArg::Bool(torrent.sequential),
        SqlArg::Bool(torrent.first_last_piece_first),
        SqlArg::OptText(torrent.save_path.clone()),
        SqlArg::OptText(
            torrent
                .content_layout
                .map(|layout| layout.as_str().to_owned()),
        ),
        SqlArg::OptText(torrent.files_dir.clone()),
        SqlArg::Bool(torrent.seed_mode),
        SqlArg::OptText(torrent.move_to.clone()),
        SqlArg::I64(torrent.queue_position),
        SqlArg::OptI64(torrent.pool_member.map(i64::from)),
    ])
}

impl Database {
    // Add a torrent with its trackers and an empty seeding ledger,
    // unless a torrent with the same infohash is already a job.
    pub fn insert_torrent(
        &self,
        torrent: &TorrentRecord,
        trackers: &[TrackerEntry],
    ) -> Result<TorrentInsert, StateError> {
        self.insert_placed_torrent(torrent, trackers, &super::model::Adopted::default(), None)
    }

    // [`Self::insert_torrent`], with what was already on disk where its
    // files go, in the same transaction.
    pub(crate) fn insert_placed_torrent(
        &self,
        torrent: &TorrentRecord,
        trackers: &[TrackerEntry],
        adopted: &super::model::Adopted,
        held: Option<&[String]>,
    ) -> Result<TorrentInsert, StateError> {
        let adopted = adopted_text(adopted)?;
        // What it holds, or, not known, its whole folder as spelled, until
        // its rows are written.
        let held_set = held.is_some();
        let held: Vec<String> = match held {
            Some(held) => held.to_vec(),
            None => torrent
                .files_dir
                .clone()
                .or_else(|| torrent.storage_dir.clone())
                .into_iter()
                .collect(),
        };
        let infohashes = torrent.infohashes();
        if infohashes.is_empty() {
            return Err(StateError::Database(
                "a torrent needs a v1 or a v2 infohash".to_string(),
            ));
        }
        let job_id = torrent.job_id;
        let torrent_args = torrent_args(torrent)?;
        let id = stored(job_id.0)?;
        let trackers = trackers.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_torrent", |tx| {
                let infohashes = infohashes.clone();
                let torrent_args = torrent_args.clone();
                let trackers = trackers.clone();
                let adopted = adopted.clone();
                let held = held.clone();
                Box::pin(async move {
                    if let Some(existing) = torrent_by_infohash_tx(tx, &infohashes).await? {
                        return Ok(TorrentInsert::Duplicate(existing.job_id));
                    }
                    // One placeholder per value: the column list and the
                    // values are both TORRENT_COLUMNS' order.
                    let values = vec!["{}"; torrent_args.len()].join(", ");
                    tx.execute(
                        &format!("INSERT INTO torrents ({TORRENT_COLUMNS}) VALUES ({values})"),
                        &torrent_args,
                    )
                    .await?;
                    insert_trackers(tx, id, &trackers).await?;
                    insert_held_tx(tx, id, &held).await?;
                    if held_set {
                        tx.execute(
                            "UPDATE torrents SET held_set = {} WHERE job_id = {}",
                            &[SqlArg::Bool(true), SqlArg::I64(id)],
                        )
                        .await?;
                    }
                    if let Some(adopted) = adopted.clone() {
                        tx.execute(
                            "UPDATE torrents SET adopted = {} WHERE job_id = {}",
                            &[SqlArg::Text(adopted), SqlArg::I64(id)],
                        )
                        .await?;
                    }
                    tx.execute(
                        "INSERT INTO torrent_counters (job_id) VALUES ({})",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                    Ok(TorrentInsert::Inserted)
                })
            })
            .await
        })
    }

    // What the operator asked of the torrent, and no other column: its
    // completion is written apart, so a pause racing a completion keeps
    // both. Returns whether the torrent exists.
    pub fn update_torrent_intent(
        &self,
        job_id: JobId,
        intent: TorrentIntent,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::Text(intent.as_str().to_string()),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("state", args)
    }

    // Seed mode as a pass found it, in one transaction: the torrents that
    // left it, as `clear_torrent_seed_mode` writes one, and the pieces each
    // still in it checked, as `set_torrent_seed_verified` writes one. Each
    // list goes in bind-limit chunks, one statement a chunk. The statements
    // are built once, the bitfields moved into them: a retry of the
    // transaction copies none.
    pub(crate) fn write_torrent_seed_modes(
        &self,
        writes: TorrentSeedWrites,
    ) -> Result<(), StateError> {
        use crate::persistence::sql_runtime::max_rows_for_engine;

        if writes.is_empty() {
            return Ok(());
        }
        let datastore = self.datastore();
        let engine = datastore.engine();
        let mut statements: Vec<(String, Vec<SqlArg>)> = Vec::new();
        // One bind for the seed mode, then the ids.
        let rows = max_rows_for_engine(engine, 1).saturating_sub(1).max(1);
        for chunk in writes.left.chunks(rows) {
            let placeholders = vec!["{}"; chunk.len()].join(", ");
            let mut args = Vec::with_capacity(chunk.len() + 1);
            args.push(SqlArg::Bool(false));
            for job_id in chunk {
                args.push(SqlArg::I64(stored(job_id.0)?));
            }
            statements.push((
                format!(
                    "UPDATE torrents SET seed_mode = {{}}, seed_verified = NULL WHERE job_id IN ({placeholders})"
                ),
                args,
            ));
        }
        // Each row binds its id and bitfield in the CASE and its id in the
        // list.
        let rows = max_rows_for_engine(engine, 3);
        let mut verified = writes.verified.into_iter().peekable();
        while verified.peek().is_some() {
            let chunk: Vec<(i64, Vec<u8>)> = verified
                .by_ref()
                .take(rows)
                .map(|(job_id, pieces)| Ok((stored(job_id.0)?, pieces)))
                .collect::<Result<_, StateError>>()?;
            let ids: Vec<i64> = chunk.iter().map(|(id, _)| *id).collect();
            let mut args = Vec::with_capacity(chunk.len() * 3);
            for (id, pieces) in chunk {
                args.push(SqlArg::I64(id));
                args.push(SqlArg::OptBytes(Some(pieces)));
            }
            args.extend(ids.iter().copied().map(SqlArg::I64));
            let cases = vec!["WHEN {} THEN {}"; ids.len()].join(" ");
            let placeholders = vec!["{}"; ids.len()].join(", ");
            statements.push((
                format!(
                    "UPDATE torrents SET seed_verified = CASE job_id {cases} END WHERE job_id IN ({placeholders})"
                ),
                args,
            ));
        }
        let statements = std::sync::Arc::new(statements);
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "write_torrent_seed_modes", |tx| {
                let statements = std::sync::Arc::clone(&statements);
                Box::pin(async move {
                    for (sql, args) in statements.iter() {
                        tx.execute(sql, args).await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // Each group's kept intent for every torrent of its ids, and no other
    // column, in one transaction: one statement per bind-limit chunk of
    // each group's ids.
    pub(crate) fn update_torrent_intents(
        &self,
        groups: &[(KeptIntent, &[JobId])],
    ) -> Result<(), StateError> {
        let groups = groups
            .iter()
            .filter(|(_, job_ids)| !job_ids.is_empty())
            .map(|(kept, job_ids)| {
                let ids = job_ids
                    .iter()
                    .map(|job_id| stored(job_id.0))
                    .collect::<Result<Vec<_>, StateError>>()?;
                Ok((kept.as_str(), ids))
            })
            .collect::<Result<Vec<_>, StateError>>()?;
        if groups.is_empty() {
            return Ok(());
        }
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_torrent_intents", |tx| {
                let groups = groups.clone();
                Box::pin(async move {
                    // The intent takes one bind of each statement.
                    let rows = crate::persistence::sql_runtime::max_rows_for_tx(tx, 1)
                        .saturating_sub(1)
                        .max(1);
                    for (state, ids) in &groups {
                        for chunk in ids.chunks(rows) {
                            let placeholders = vec!["{}"; chunk.len()].join(", ");
                            let mut args = vec![SqlArg::Text((*state).to_owned())];
                            args.extend(chunk.iter().copied().map(SqlArg::I64));
                            tx.execute(
                                &format!(
                                    "UPDATE torrents SET state = {{}} WHERE job_id IN ({placeholders})"
                                ),
                                &args,
                            )
                            .await?;
                        }
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // When the torrent completed. Returns whether the torrent exists.
    pub fn set_torrent_completed(&self, job_id: JobId, at_ms: i64) -> Result<bool, StateError> {
        let args = vec![SqlArg::I64(at_ms), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("completed_at_ms", args)
    }

    // The torrent's ledger. Returns whether the torrent exists.
    pub fn update_torrent_counters(
        &self,
        job_id: JobId,
        counters: &TorrentCounters,
    ) -> Result<bool, StateError> {
        self.save_torrent_resume(job_id, Some(counters), None)
    }

    // The peers and bans a torrent begins its next start with. Returns
    // whether the torrent exists.
    pub fn save_torrent_peers(
        &self,
        job_id: JobId,
        peers: &ResumePeers,
    ) -> Result<bool, StateError> {
        self.save_torrent_resume(job_id, None, Some(peers))
    }

    // The row a torrent's next start reads as it ran: its ledger, and the
    // peers and bans it begins with, each written only when given, in one
    // statement. The row is apart from the one that holds the metainfo, so
    // writing it as the torrent runs never writes the metainfo again.
    // Returns whether the torrent exists.
    pub fn save_torrent_resume(
        &self,
        job_id: JobId,
        counters: Option<&TorrentCounters>,
        peers: Option<&ResumePeers>,
    ) -> Result<bool, StateError> {
        let Some((sql, args)) = resume_update(job_id, counters, peers)? else {
            return Ok(false);
        };
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(datastore.write_exec(), &sql, &args).await?;
            Ok(changed > 0)
        })
    }

    // The pieces the torrent has in its files, and its ledger and the peers
    // and bans it begins with as they stood with them, each written only
    // when given, in one transaction: a crash recovers the pieces and the
    // counters of one moment.
    pub(crate) fn save_torrent_checkpoint(
        &self,
        job_id: JobId,
        pieces: Option<&Kept>,
        counters: Option<&TorrentCounters>,
        peers: Option<&ResumePeers>,
    ) -> Result<(), StateError> {
        let resume = resume_update(job_id, counters, peers)?;
        let pieces = pieces
            .map(|kept| -> Result<_, StateError> {
                Ok(vec![
                    SqlArg::OptBytes(Some(kept.bits.clone())),
                    SqlArg::Bool(kept.checking),
                    SqlArg::I64(stored(job_id.0)?),
                ])
            })
            .transpose()?;
        if pieces.is_none() && resume.is_none() {
            return Ok(());
        }
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_torrent_checkpoint", |tx| {
                let pieces = pieces.clone();
                let resume = resume.clone();
                Box::pin(async move {
                    if let Some(args) = &pieces {
                        tx.execute(
                            "UPDATE torrent_counters SET pieces = {}, pieces_checking = {} \
                             WHERE job_id = {}",
                            args,
                        )
                        .await?;
                    }
                    if let Some((sql, args)) = &resume {
                        tx.execute(sql, args).await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // The pieces [`Self::save_torrent_checkpoint`] last kept; None when it
    // kept none.
    pub(crate) fn torrent_pieces(&self, job_id: JobId) -> Result<Option<Kept>, StateError> {
        self.torrent_read();
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT pieces, pieces_checking FROM torrent_counters WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            let Some(row) = row else { return Ok(None) };
            let Some(bits) = row.opt_bytes("pieces")? else {
                return Ok(None);
            };
            Ok(Some(Kept {
                bits,
                checking: row.bool("pieces_checking")?,
            }))
        })
    }

    // Unpacking is over, and its outputs are where they stay: the files it
    // placed beside the torrent's own are kept on its row, with those an
    // earlier unpacking kept there; `dropped`, the names kept that hold no
    // file, go from it; `held` joins what the torrent holds, so a history
    // archive leaves them; and its job's checkpoint goes. One transaction:
    // a restart finds the placing recorded whole or not at all. Run on the
    // torrent's own lane, so no write of the job's checkpoint queued before
    // it lands after.
    pub(crate) fn finish_torrent_unpack(
        &self,
        job_id: JobId,
        outputs: &[String],
        dropped: &[String],
        held: &[String],
    ) -> Result<(), StateError> {
        let id = stored(job_id.0)?;
        let outputs = self.outputs_with(job_id, outputs, dropped)?;
        let held = held.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "finish_torrent_unpack", |tx| {
                let (outputs, held) = (outputs.clone(), held.clone());
                Box::pin(async move {
                    tx.execute(
                        "UPDATE torrents SET outputs = {} WHERE job_id = {}",
                        &[SqlArg::OptText(outputs), SqlArg::I64(id)],
                    )
                    .await?;
                    insert_held_tx(tx, id, &held).await?;
                    for table in crate::jobs::graph_checkpoint::TABLES {
                        tx.execute(
                            &format!("DELETE FROM {table} WHERE job_id = {{}}"),
                            &[SqlArg::I64(id)],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // What a torrent's row says the torrent made, written whole in one
    // statement: `outputs`, the files unpacking placed, and `adopted`, what
    // was there before the torrent (None for not known). A move of its
    // placed files writes it before any file moves, to no output and every
    // file adopted, so a move that stops midway leaves a row that names no
    // file as the torrent's to delete; and again when a move fails, with
    // what the move left as it was. False when the torrent has no row.
    pub(crate) fn set_torrent_made(
        &self,
        job_id: JobId,
        outputs: &[String],
        adopted: Option<&super::model::Adopted>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(stored_list(outputs)?),
            SqlArg::OptText(adopted.map(adopted_text).transpose()?.flatten()),
            SqlArg::I64(stored(job_id.0)?),
        ];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET outputs = {}, adopted = {} WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // A move of a torrent's placed files, recorded whole: the folder its
    // files are in and its save path are `to`; `outputs` are the files
    // unpacking placed, under the names the move left them at; `adopted`
    // is what was there before the torrent with the files the move found
    // at `to` and kept, None when it is not known; and `held` is what the
    // torrent holds from now on, in place of what it held. One transaction,
    // so a restart never finds the new folder beside the names of the old
    // one.
    pub(crate) fn record_torrent_files_move(
        &self,
        job_id: JobId,
        to: &str,
        outputs: &[String],
        adopted: Option<&super::model::Adopted>,
        held: &[String],
    ) -> Result<(), StateError> {
        let id = stored(job_id.0)?;
        let to = to.to_owned();
        let outputs = stored_list(outputs)?;
        let adopted = adopted.map(adopted_text).transpose()?.flatten();
        let held = held.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "record_torrent_files_move", |tx| {
                let (to, outputs, adopted, held) =
                    (to.clone(), outputs.clone(), adopted.clone(), held.clone());
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE torrents SET files_dir = {}, save_path = {}, outputs = {}, \
                             adopted = {}, held_set = {} WHERE job_id = {}",
                            &[
                                SqlArg::OptText(Some(to.clone())),
                                SqlArg::OptText(Some(to)),
                                SqlArg::OptText(outputs),
                                SqlArg::OptText(adopted),
                                SqlArg::Bool(true),
                                SqlArg::I64(id),
                            ],
                        )
                        .await?;
                    if changed == 0 {
                        return Err(StateError::Database(
                            "the torrent has no row to record its move on".to_owned(),
                        ));
                    }
                    tx.execute(
                        "DELETE FROM torrent_held_paths WHERE job_id = {}",
                        &[SqlArg::I64(id)],
                    )
                    .await?;
                    insert_held_tx(tx, id, &held).await?;
                    Ok(())
                })
            })
            .await
        })
    }

    // The row's outputs with `outputs` added and `unplaced` taken out, as
    // the row keeps them.
    fn outputs_with(
        &self,
        job_id: JobId,
        outputs: &[String],
        unplaced: &[String],
    ) -> Result<Option<String>, StateError> {
        let unplaced: std::collections::HashSet<&String> = unplaced.iter().collect();
        let mut kept = self.torrent_outputs(job_id)?;
        kept.retain(|output| !unplaced.contains(output));
        let mut seen: std::collections::HashSet<String> = kept.iter().cloned().collect();
        for output in outputs {
            if seen.insert(output.clone()) {
                kept.push(output.clone());
            }
        }
        (!kept.is_empty())
            .then(|| serde_json::to_string(&kept))
            .transpose()
            .map_err(|error| StateError::Database(error.to_string()))
    }

    // The files [`Self::finish_torrent_unpack`] kept, relative to the
    // directory the torrent's files are in.
    pub(crate) fn torrent_outputs(&self, job_id: JobId) -> Result<Vec<String>, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT outputs FROM torrents WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            let Some(text) = row
                .map(|row| row.opt_text("outputs"))
                .transpose()?
                .flatten()
            else {
                return Ok(Vec::new());
            };
            serde_json::from_str(&text).map_err(|error| StateError::Database(error.to_string()))
        })
    }

    // What was on disk where the torrent's files go before Weaver wrote
    // them; nothing kept is nothing adopted.
    #[cfg(test)]
    pub(crate) fn set_torrent_adopted(
        &self,
        job_id: JobId,
        adopted: &super::model::Adopted,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(adopted_text(adopted)?),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("adopted", args)
    }

    // A row from before what was there was kept: `adopted` never written.
    #[cfg(test)]
    pub(crate) fn forget_torrent_adopted(&self, job_id: JobId) -> Result<bool, StateError> {
        let args = vec![SqlArg::OptText(None), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("adopted", args)
    }

    // What was on disk where the torrent's files go before Weaver wrote
    // them; None when that was never written (a row from before it was
    // kept), which is not known and takes no file.
    pub(crate) fn torrent_adopted(
        &self,
        job_id: JobId,
    ) -> Result<Option<super::model::Adopted>, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT adopted FROM torrents WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            let Some(text) = row
                .map(|row| row.opt_text("adopted"))
                .transpose()?
                .flatten()
            else {
                return Ok(None);
            };
            serde_json::from_str(&text)
                .map(Some)
                .map_err(|error| StateError::Database(error.to_string()))
        })
    }

    // Removes the torrent with its files, trackers, ledger and checkpoint.
    pub fn delete_torrent(&self, job_id: JobId) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_torrent", |tx| {
                Box::pin(async move { delete_torrent_tx(tx, id).await })
            })
            .await
        })
    }

    // The torrent's category. Returns whether the torrent exists.
    pub fn set_torrent_category(
        &self,
        job_id: JobId,
        category_id: Option<u32>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptI64(category_id.map(i64::from)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("category_id", args)
    }

    // Whether the torrent runs past the download queue. Returns whether the
    // torrent exists.
    pub fn set_torrent_forced(&self, job_id: JobId, forced: bool) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(forced), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("forced", args)
    }

    // The torrent's own seeding policy; one whose every leaf inherits is
    // kept as none. Returns whether the torrent exists.
    pub fn set_torrent_policy_override(
        &self,
        job_id: JobId,
        policy_override: Option<SeedingPolicy>,
    ) -> Result<bool, StateError> {
        let policy_override = SeedingPolicy::kept(policy_override)
            .map(|policy| serde_json::to_string(&policy))
            .transpose()
            .map_err(|error| StateError::Database(error.to_string()))?;
        let args = vec![
            SqlArg::OptText(policy_override),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("policy_override", args)
    }

    // The torrent's trackers, as edited (libtorrent `write_resume_data`
    // keeps them as its `trackers` and `tiers`). Returns whether the torrent
    // exists.
    pub fn set_torrent_trackers(
        &self,
        job_id: JobId,
        trackers: &[TrackerEntry],
    ) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let trackers = trackers.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_trackers", |tx| {
                let trackers = trackers.clone();
                Box::pin(async move {
                    let known = tx
                        .fetch_optional(
                            "SELECT job_id FROM torrents WHERE job_id = {}",
                            &[SqlArg::I64(id)],
                        )
                        .await?;
                    if known.is_none() {
                        return Ok(false);
                    }
                    tx.execute(
                        "DELETE FROM torrent_trackers WHERE job_id = {}",
                        &[SqlArg::I64(id)],
                    )
                    .await?;
                    insert_trackers(tx, id, &trackers).await?;
                    Ok(true)
                })
            })
            .await
        })
    }

    // Whether the torrent's archives are extracted, fixed when its job is
    // built. Returns whether the torrent exists.
    pub fn set_torrent_unpack_in_force(
        &self,
        job_id: JobId,
        unpack: bool,
    ) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(unpack), SqlArg::I64(stored(job_id.0)?)];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET unpack_in_force = {} WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // A magnet's metainfo once its peers sent it: the hashes and name it
    // carries, the file, the directory its files go to and the folder they
    // sit in when that is not it, the files and trackers it lists, and what
    // it holds there (`torrent_held_paths`), all in one transaction: a crash
    // leaves the torrent holding its whole folder as a magnet does, or what
    // its files are, never a row with the files known and the paths not.
    // Returns whether the torrent exists.
    #[allow(clippy::too_many_arguments)]
    pub fn set_torrent_metainfo(
        &self,
        job_id: JobId,
        infohash_v1: Option<[u8; 20]>,
        infohash_v2: Option<[u8; 32]>,
        name: &str,
        metainfo: &[u8],
        storage_dir: &str,
        files_dir: Option<&str>,
        trackers: &[TrackerEntry],
        held: &[String],
        adopted: &super::model::Adopted,
    ) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let adopted = adopted_text(adopted)?;
        let head = vec![
            SqlArg::OptBytes(infohash_v1.map(|hash| hash.to_vec())),
            SqlArg::OptBytes(infohash_v2.map(|hash| hash.to_vec())),
            SqlArg::Text(name.to_owned()),
            SqlArg::Bytes(metainfo.to_vec()),
            SqlArg::Text(storage_dir.to_owned()),
            SqlArg::OptText(files_dir.map(str::to_owned)),
            SqlArg::Bool(true),
            SqlArg::OptText(adopted),
            SqlArg::I64(id),
        ];
        let trackers = trackers.to_vec();
        let held = held.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_metainfo", |tx| {
                let head = head.clone();
                let trackers = trackers.clone();
                let held = held.clone();
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE torrents SET infohash_v1 = {}, infohash_v2 = {}, name = {}, metainfo = {},
                                     storage_dir = {}, files_dir = {}, held_set = {}, adopted = {}
                              WHERE job_id = {}",
                            &head,
                        )
                        .await?;
                    if changed == 0 {
                        return Ok(false);
                    }
                    tx.execute(
                        "DELETE FROM torrent_trackers WHERE job_id = {}",
                        &[SqlArg::I64(id)],
                    )
                    .await?;
                    insert_trackers(tx, id, &trackers).await?;
                    tx.execute(
                        "DELETE FROM torrent_held_paths WHERE job_id = {}",
                        &[SqlArg::I64(id)],
                    )
                    .await?;
                    insert_held_tx(tx, id, &held).await?;
                    Ok(true)
                })
            })
            .await
        })
    }

    // The directory a torrent's files now live under. What it holds is
    // marked to be written again in the same statement, so a crash before
    // that write leaves the next start to write it (`held_set`). Returns
    // whether the torrent exists.
    pub fn set_torrent_storage_dir(&self, job_id: JobId, dir: &str) -> Result<bool, StateError> {
        let args = vec![SqlArg::Text(dir.to_owned()), SqlArg::I64(stored(job_id.0)?)];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET storage_dir = {}, held_set = FALSE WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // The torrent's own upload and download limits in bytes per second:
    // `None` keeps a limit as it is, `Some(None)` is unlimited and zero
    // closed. Returns whether the torrent exists.
    pub fn set_torrent_rate_limits(
        &self,
        job_id: JobId,
        upload: Option<Option<u64>>,
        download: Option<Option<u64>>,
    ) -> Result<bool, StateError> {
        let mut args = Vec::new();
        for limit in [upload, download] {
            args.push(SqlArg::I64(i64::from(limit.is_some())));
            args.push(SqlArg::OptI64(limit.flatten().map(stored).transpose()?));
        }
        args.push(SqlArg::I64(stored(job_id.0)?));
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET \
                 upload_limit = CASE WHEN {} = 1 THEN {} ELSE upload_limit END, \
                 download_limit = CASE WHEN {} = 1 THEN {} ELSE download_limit END \
                 WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // The operator's level for every file of a torrent, in the metainfo's
    // order, written whole. Returns whether the torrent exists.
    pub fn set_torrent_file_priorities(
        &self,
        job_id: JobId,
        levels: &[FilePriority],
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(file_priorities_str(levels)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("file_priorities", args)
    }

    // Whether the torrent fetches its pieces in order. Returns whether the
    // torrent exists.
    pub fn set_torrent_sequential(&self, job_id: JobId, on: bool) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(on), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("sequential", args)
    }

    // Whether each of the torrent's files has its first and last pieces
    // fetched first. Returns whether the torrent exists.
    pub fn set_torrent_first_last_piece_first(
        &self,
        job_id: JobId,
        on: bool,
    ) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(on), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("first_last_piece_first", args)
    }

    // Where the torrent's files sit when not in its storage directory,
    // what it holds marked to be written again as `set_torrent_storage_dir`
    // marks it.
    pub fn set_torrent_files_dir(
        &self,
        job_id: JobId,
        dir: Option<&str>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(dir.map(str::to_owned)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET files_dir = {}, held_set = FALSE WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // The directory the torrent's files go under, as asked or as it moved.
    pub fn set_torrent_save_path(
        &self,
        job_id: JobId,
        dir: Option<&str>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(dir.map(str::to_owned)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("save_path", args)
    }

    // The torrent's session left seed mode: from here its pieces are what
    // its job has, and none needs the check it passed in seed mode kept.
    pub fn clear_torrent_seed_mode(&self, job_id: JobId) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(false), SqlArg::I64(stored(job_id.0)?)];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET seed_mode = {}, seed_verified = NULL WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // The pieces that passed their check in seed mode, a bitfield as the
    // wire's: the first piece the high bit of the first byte (libtorrent's
    // `verified_pieces` in resume data).
    pub fn set_torrent_seed_verified(
        &self,
        job_id: JobId,
        pieces: &[u8],
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptBytes(Some(pieces.to_vec())),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("seed_verified", args)
    }

    // The pieces [`Self::set_torrent_seed_verified`] last kept; None when it
    // kept none.
    pub fn torrent_seed_verified(&self, job_id: JobId) -> Result<Option<Vec<u8>>, StateError> {
        self.torrent_read();
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT seed_verified FROM torrents WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_bytes("seed_verified"))
                .transpose()?
                .flatten())
        })
    }

    // The pool member the torrent is pinned to, or none.
    pub fn set_torrent_pool_member(
        &self,
        job_id: JobId,
        member: Option<u32>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptI64(member.map(i64::from)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("pool_member", args)
    }

    // The torrent moved where it was to move once complete, or will not.
    pub fn clear_torrent_move_to(&self, job_id: JobId) -> Result<bool, StateError> {
        let args = vec![SqlArg::OptText(None), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("move_to", args)
    }

    // Where each torrent stands in the download queue, in one transaction.
    pub fn set_torrent_queue_positions(
        &self,
        positions: &[(JobId, i64)],
    ) -> Result<(), StateError> {
        if positions.is_empty() {
            return Ok(());
        }
        let rows = positions
            .iter()
            .map(|&(job_id, position)| Ok((stored(job_id.0)?, SqlArg::I64(position))))
            .collect::<Result<Vec<_>, StateError>>()?;
        let rows = std::sync::Arc::new(rows);
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_queue_positions", |tx| {
                let rows = std::sync::Arc::clone(&rows);
                Box::pin(async move { update_column_tx(tx, "queue_position", &rows).await })
            })
            .await
        })
    }

    // The categories and the forced flags of a batch of torrents, each its
    // one column and no other, in one transaction of few statements.
    pub(crate) fn set_torrent_categories_and_forced(
        &self,
        categories: &[(JobId, Option<u32>)],
        forced: &[(JobId, bool)],
    ) -> Result<(), StateError> {
        if categories.is_empty() && forced.is_empty() {
            return Ok(());
        }
        let categories = categories
            .iter()
            .map(|&(job_id, category_id)| {
                Ok((
                    stored(job_id.0)?,
                    SqlArg::OptI64(category_id.map(i64::from)),
                ))
            })
            .collect::<Result<Vec<_>, StateError>>()?;
        let forced = forced
            .iter()
            .map(|&(job_id, forced)| Ok((stored(job_id.0)?, SqlArg::Bool(forced))))
            .collect::<Result<Vec<_>, StateError>>()?;
        let columns = std::sync::Arc::new((categories, forced));
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_categories_and_forced", |tx| {
                let columns = std::sync::Arc::clone(&columns);
                Box::pin(async move {
                    update_column_tx(tx, "category_id", &columns.0).await?;
                    update_column_tx(tx, "forced", &columns.1).await
                })
            })
            .await
        })
    }

    // Write one column of a torrent and no other, so the writers of
    // different columns never undo each other.
    fn set_torrent_column(&self, column: &str, args: Vec<SqlArg>) -> Result<bool, StateError> {
        let sql = format!("UPDATE torrents SET {column} = {{}} WHERE job_id = {{}}");
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(datastore.write_exec(), &sql, &args).await?;
            Ok(changed > 0)
        })
    }

    // The peers and bans a torrent last saved; none when it saved none or
    // is not a torrent.
    pub fn torrent_peers(&self, job_id: JobId) -> Result<ResumePeers, StateError> {
        self.torrent_read();
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        let row = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT peers, banned_peers, found_peers FROM torrent_counters WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await
        })?;
        let Some(row) = row else {
            return Ok(ResumePeers::default());
        };
        Ok(ResumePeers {
            peers: listed("peers", row.opt_text("peers")?)?,
            banned: listed("banned_peers", row.opt_text("banned_peers")?)?,
            found: found_marks(job_id, row.opt_text("found_peers")),
        })
    }

    // Every torrent's ledger, saved peers and kept pieces, by torrent, in
    // one read: what [`Self::get_torrent_counters`],
    // [`Self::torrent_peers`] and [`Self::torrent_pieces`] give a
    // torrent's restore, for all of them. Each part reads on its own, so
    // one that does not read is that part of that torrent's alone.
    pub(crate) fn torrent_counters_by_job(
        &self,
    ) -> Result<std::collections::BTreeMap<JobId, CountersRow>, StateError> {
        self.torrent_read();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, uploaded_bytes, downloaded_bytes, active_duration_s,
                        last_seen_complete_ms, finished_duration_s, last_activity_ms,
                        consecutive_seeding_s, failed_bytes, redundant_bytes, pieces,
                        pieces_checking, peers, banned_peers, found_peers
                   FROM torrent_counters",
                &[],
            )
            .await?;
            rows.iter()
                .map(|row| {
                    let job_id = JobId(count(row, "job_id")?);
                    let kept = row.opt_bytes("pieces").and_then(|bits| {
                        bits.map(|bits| {
                            Ok(Kept {
                                bits,
                                checking: row.bool("pieces_checking")?,
                            })
                        })
                        .transpose()
                    });
                    let peers = row
                        .opt_text("peers")
                        .and_then(|peers| listed("peers", peers))
                        .and_then(|peers| {
                            let banned = row
                                .opt_text("banned_peers")
                                .and_then(|banned| listed("banned_peers", banned))?;
                            let found = found_marks(job_id, row.opt_text("found_peers"));
                            Ok(ResumePeers {
                                peers,
                                banned,
                                found,
                            })
                        });
                    Ok((
                        job_id,
                        CountersRow {
                            counters: counters_from_row(row),
                            peers,
                            kept,
                        },
                    ))
                })
                .collect()
        })
    }
}

// One torrent's counters row as a boot reads it.
pub(crate) struct CountersRow {
    pub(crate) counters: Result<TorrentCounters, StateError>,
    pub(crate) peers: Result<ResumePeers, StateError>,
    pub(crate) kept: Result<Option<Kept>, StateError>,
}
