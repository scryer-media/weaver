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
use crate::torrents::model::{file_priorities_str, named_by_str};
use crate::torrents::queries::{TORRENT_COLUMNS, count, counters_from_row, torrent_by_infohash_tx};
use crate::torrents::store::Kept;
use crate::torrents::{
    FilePriority, TorrentCounters, TorrentInsert, TorrentIntent, TorrentRecord, TrackerEntry,
};
use weaver_torrent::ResumePeers;

// A tracker row holds where the tracker sits in the announce list and who
// named it; how announcing to it goes is read from the session.
const TRACKER_INSERT: &str =
    "INSERT INTO torrent_trackers (job_id, tier, url, named_by) VALUES ({}, {}, {}, {})";
// Every host a torrent has announced to stays known after the torrent is
// gone, so a tracker rule can be picked from them. The URL is not kept: a
// private tracker's carries the operator's passkey.
const TRACKER_HOST_INSERT: &str =
    "INSERT INTO tracker_hosts (host) VALUES ({}) ON CONFLICT (host) DO NOTHING";

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
        sets.push("peers = {}, banned_peers = {}");
        args.extend([
            SqlArg::OptText(stored_list(&peers.peers)?),
            SqlArg::OptText(stored_list(&peers.banned)?),
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
    for tracker in trackers {
        let args = [
            SqlArg::I64(job_id),
            SqlArg::I64(i64::from(tracker.tier)),
            SqlArg::Text(tracker.url.clone()),
            SqlArg::Text(named_by_str(tracker.named_by).to_owned()),
        ];
        tx.execute(TRACKER_INSERT, &args).await?;
    }
    let hosts: std::collections::BTreeSet<String> = trackers
        .iter()
        .filter_map(|tracker| tracker_host(&tracker.url))
        .collect();
    for host in hosts {
        tx.execute(TRACKER_HOST_INSERT, &[SqlArg::Text(host)])
            .await?;
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

impl crate::persistence::DatabaseWriterExecutor {
    // A removal's history row the database's disk had no room for, kept on
    // the torrent's own row: a stop before room comes leaves it there, and
    // the next start writes it, as it archives an NZB job left terminal in
    // the active table. The torrent's row goes once the history row is
    // written, and this with it. One statement, the smallest write that
    // keeps it, on a disk that just refused one.
    pub(crate) fn keep_torrent_retiring(
        &self,
        entry: &JobHistoryRow,
        files: &[HistoryFile],
    ) -> Result<(), StateError> {
        let id = stored(entry.job_id)?;
        let kept = serde_json::to_string(&Retiring {
            history: entry.clone(),
            files: files.to_vec(),
        })
        .map_err(|error| StateError::Database(error.to_string()))?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET retiring = {} WHERE job_id = {}",
                &[SqlArg::Text(kept), SqlArg::I64(id)],
            )
            .await?;
            Ok(())
        })
    }
}

impl Database {
    // Every torrent a removal left retiring, its kept history row and list
    // as written, or why it does not read. Served by the retiring index.
    pub(crate) fn retiring_torrents(&self) -> Result<Vec<(JobId, KeptRow)>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, retiring FROM torrents WHERE retiring IS NOT NULL",
                &[],
            )
            .await?;
            rows.iter()
                .map(|row| {
                    let job_id = JobId(count(row, "job_id")?);
                    let kept = serde_json::from_str::<Retiring>(&row.text("retiring")?)
                        .map(|kept| (kept.history, kept.files))
                        .map_err(|error| {
                            format!("a torrent's kept history row is not valid: {error}")
                        });
                    Ok((job_id, kept))
                })
                .collect()
        })
    }

    // Start-up: each torrent a removal left retiring, its history row kept
    // for want of room as the last run stopped, goes to history with that
    // row and leaves the torrents: all in one transaction, and when that
    // fails, each in its own, so one row that cannot be written holds back
    // none of the others. A row left retiring never loads as a live torrent;
    // the service shows it and writes it through the kept archives
    // (`TorrentService::retry_retiring`). The jobs written.
    pub(crate) fn archive_retiring_torrents(&self) -> Result<RetiringArchived, StateError> {
        let mut archived = RetiringArchived::default();
        let mut rows = Vec::new();
        for (job_id, kept) in self.retiring_torrents()? {
            match kept {
                Ok((history, files)) => rows.push((job_id, history, files)),
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "a removed torrent's kept history row does not read; it stays retiring");
                    archived.left.push(job_id);
                }
            }
        }
        if rows.is_empty() {
            return Ok(archived);
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
        Ok(archived)
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

// What a start's archive of the retiring torrents came to: the jobs written
// to history, and those left retiring, never live, for the service to retry.
#[derive(Debug, Default)]
pub(crate) struct RetiringArchived {
    pub(crate) written: Vec<JobId>,
    pub(crate) left: Vec<JobId>,
}

// A retiring torrent's kept history row and the files it lists, or why
// that does not read.
pub(crate) type KeptRow = Result<(JobHistoryRow, Vec<HistoryFile>), String>;

// A removal's history row kept on the torrent, with the files it lists.
#[derive(serde::Serialize, serde::Deserialize)]
struct Retiring {
    history: JobHistoryRow,
    files: Vec<HistoryFile>,
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

// A live torrent whose files may lie under a folder history removes from:
// the folder its files are in, its metainfo, and what unpacking placed
// beside them, relative to that folder.
pub(crate) struct LiveFiles {
    pub(crate) job_id: JobId,
    pub(crate) base: String,
    pub(crate) metainfo: Option<Vec<u8>>,
    pub(crate) outputs: Vec<String>,
}

impl Database {
    // The live torrents, never one a removal left retiring, whose files may
    // lie under one of `dirs`: those whose folder, where their files are or
    // the one claimed for them, is one of `dirs`, lies under one, or holds
    // one. Served by the folder columns' indexes: one statement for the
    // folders under each bind-sized chunk of `dirs`, and one for each chunk
    // of the folders above them. A torrent found twice is read once.
    pub(crate) fn live_torrents_over(
        &self,
        dirs: &[PathBuf],
    ) -> Result<Vec<LiveFiles>, StateError> {
        use crate::persistence::sql_runtime::SqlEngine;
        if dirs.is_empty() {
            return Ok(Vec::new());
        }
        let separator = std::path::MAIN_SEPARATOR;
        let after = char::from_u32(u32::from(separator) + 1).unwrap_or(separator);
        let under: Vec<(String, String, String)> = dirs
            .iter()
            .map(|dir| {
                let dir = dir.to_string_lossy().trim_end_matches(separator).to_owned();
                (
                    dir.clone(),
                    format!("{dir}{separator}"),
                    format!("{dir}{after}"),
                )
            })
            .collect();
        let above: Vec<String> = dirs
            .iter()
            .flat_map(|dir| dir.ancestors().skip(1))
            .map(|dir| dir.to_string_lossy().into_owned())
            .filter(|dir| !dir.is_empty())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            // Ordered by bytes on both engines, so a range over a folder's
            // path is the paths under it.
            let column = |name: &str| match datastore.engine() {
                SqlEngine::Sqlite => name.to_owned(),
                SqlEngine::Postgres => format!("{name} COLLATE \"C\""),
            };
            let (storage, files) = (column("storage_dir"), column("files_dir"));
            let select = "SELECT job_id, storage_dir, files_dir, metainfo, outputs FROM torrents \
                          WHERE retiring IS NULL AND";
            let mut found = std::collections::BTreeMap::new();
            let mut read =
                |rows: Vec<crate::persistence::sql_runtime::SqlRow>| -> Result<(), StateError> {
                    for row in rows {
                        let job_id = JobId(count(&row, "job_id")?);
                        let Some(base) =
                            row.opt_text("files_dir")?.or(row.opt_text("storage_dir")?)
                        else {
                            continue;
                        };
                        let outputs = match row.opt_text("outputs")? {
                            Some(text) => serde_json::from_str(&text)
                                .map_err(|error| StateError::Database(error.to_string()))?,
                            None => Vec::new(),
                        };
                        found.entry(job_id).or_insert(LiveFiles {
                            job_id,
                            base,
                            metainfo: row.opt_bytes("metainfo")?,
                            outputs,
                        });
                    }
                    Ok(())
                };
            let rows_per =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 6);
            for chunk in under.chunks(rows_per) {
                let mut filters = Vec::with_capacity(chunk.len() * 2);
                let mut args = Vec::with_capacity(chunk.len() * 6);
                for (dir, from, to) in chunk {
                    for column in [&storage, &files] {
                        filters.push(format!(
                            "{column} = {{}} OR ({column} >= {{}} AND {column} < {{}})"
                        ));
                        args.extend([
                            SqlArg::Text(dir.clone()),
                            SqlArg::Text(from.clone()),
                            SqlArg::Text(to.clone()),
                        ]);
                    }
                }
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!("{select} ({})", filters.join(" OR ")),
                    &args,
                )
                .await?;
                read(rows)?;
            }
            let rows_per =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 2);
            for chunk in above.chunks(rows_per) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args: Vec<SqlArg> = chunk
                    .iter()
                    .chain(chunk)
                    .cloned()
                    .map(SqlArg::Text)
                    .collect();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "{select} ({storage} IN ({placeholders}) OR {files} IN ({placeholders}))"
                    ),
                    &args,
                )
                .await?;
                read(rows)?;
            }
            Ok(found.into_values().collect())
        })
    }
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
    for table in crate::jobs::graph_checkpoint::TABLES {
        tx.execute(
            &format!("DELETE FROM {table} WHERE job_id = {{}}"),
            &[SqlArg::I64(id)],
        )
        .await?;
    }
    Ok(changed > 0)
}

fn adopted_text(adopted: &super::model::Adopted) -> Result<Option<String>, StateError> {
    (!adopted.is_empty())
        .then(|| serde_json::to_string(adopted))
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
        SqlArg::Text(torrent.intent.as_str().to_string()),
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
        self.insert_placed_torrent(torrent, trackers, &super::model::Adopted::default())
    }

    // [`Self::insert_torrent`], with what was already on disk where its
    // files go, in the same transaction.
    pub(crate) fn insert_placed_torrent(
        &self,
        torrent: &TorrentRecord,
        trackers: &[TrackerEntry],
        adopted: &super::model::Adopted,
    ) -> Result<TorrentInsert, StateError> {
        let adopted = adopted_text(adopted)?;
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

    // Unpacking is over: the files it placed beside the torrent's own are
    // kept on its row, with those an earlier unpacking that a crash cut
    // short kept there, those kept for it that it did not place go from it,
    // and its job's checkpoint goes, in one transaction. Run on the
    // torrent's own lane, so no write of the job's checkpoint queued before
    // it lands after.
    pub(crate) fn finish_torrent_unpack(
        &self,
        job_id: JobId,
        outputs: &[String],
        unplaced: &[String],
    ) -> Result<(), StateError> {
        let id = stored(job_id.0)?;
        let outputs = self.outputs_with(job_id, outputs, unplaced)?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "finish_torrent_unpack", |tx| {
                let outputs = outputs.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE torrents SET outputs = {} WHERE job_id = {}",
                        &[SqlArg::OptText(outputs), SqlArg::I64(id)],
                    )
                    .await?;
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

    // The files unpacking is about to place beside the torrent's own, kept
    // on its row with those it kept before: a crash before
    // [`Self::finish_torrent_unpack`] leaves none its delete does not know.
    pub(crate) fn keep_torrent_outputs(
        &self,
        job_id: JobId,
        outputs: &[String],
    ) -> Result<(), StateError> {
        let id = stored(job_id.0)?;
        let outputs = self.outputs_with(job_id, outputs, &[])?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET outputs = {} WHERE job_id = {}",
                &[SqlArg::OptText(outputs), SqlArg::I64(id)],
            )
            .await?;
            Ok(())
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

    // What [`Self::set_torrent_adopted`] kept.
    pub(crate) fn torrent_adopted(
        &self,
        job_id: JobId,
    ) -> Result<super::model::Adopted, StateError> {
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
                return Ok(super::model::Adopted::default());
            };
            serde_json::from_str(&text).map_err(|error| StateError::Database(error.to_string()))
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
    // carries, the file, the directory its files go to, and the files and
    // trackers it lists. Returns whether the torrent exists.
    #[allow(clippy::too_many_arguments)]
    pub fn set_torrent_metainfo(
        &self,
        job_id: JobId,
        infohash_v1: Option<[u8; 20]>,
        infohash_v2: Option<[u8; 32]>,
        name: &str,
        metainfo: &[u8],
        storage_dir: &str,
        trackers: &[TrackerEntry],
    ) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let head = vec![
            SqlArg::OptBytes(infohash_v1.map(|hash| hash.to_vec())),
            SqlArg::OptBytes(infohash_v2.map(|hash| hash.to_vec())),
            SqlArg::Text(name.to_owned()),
            SqlArg::Bytes(metainfo.to_vec()),
            SqlArg::Text(storage_dir.to_owned()),
            SqlArg::I64(id),
        ];
        let trackers = trackers.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_metainfo", |tx| {
                let head = head.clone();
                let trackers = trackers.clone();
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE torrents SET infohash_v1 = {}, infohash_v2 = {}, name = {}, metainfo = {},
                                     storage_dir = {}
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
                    Ok(true)
                })
            })
            .await
        })
    }

    // The directory a torrent's files now live under. Returns whether the
    // torrent exists.
    pub fn set_torrent_storage_dir(&self, job_id: JobId, dir: &str) -> Result<bool, StateError> {
        let args = vec![SqlArg::Text(dir.to_owned()), SqlArg::I64(stored(job_id.0)?)];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET storage_dir = {} WHERE job_id = {}",
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

    // Where the torrent's files sit when not in its storage directory.
    pub fn set_torrent_files_dir(
        &self,
        job_id: JobId,
        dir: Option<&str>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(dir.map(str::to_owned)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("files_dir", args)
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
        let rows = positions
            .iter()
            .map(|&(job_id, position)| {
                Ok(vec![SqlArg::I64(position), SqlArg::I64(stored(job_id.0)?)])
            })
            .collect::<Result<Vec<_>, StateError>>()?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_queue_positions", |tx| {
                let rows = rows.clone();
                Box::pin(async move {
                    for args in &rows {
                        tx.execute(
                            "UPDATE torrents SET queue_position = {} WHERE job_id = {}",
                            args,
                        )
                        .await?;
                    }
                    Ok(())
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
                "SELECT peers, banned_peers FROM torrent_counters WHERE job_id = {}",
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
                        pieces_checking, peers, banned_peers
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
                            Ok(ResumePeers { peers, banned })
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
