// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use super::model::HistoryEnd;
use super::persistence::{RetiringMark, delete_torrent_tx, stored, update_column_tx};
use super::queries::{TorrentDatabase, count};
use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database as SqlDatabase;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use weaver_server_history::history::record::JobHistoryRow;

// A database that writes a torrent's job's history row in a transaction:
// the history row, its attributes and its duplicate identity's move, as the
// history persistence writes them (`insert_job_history_tx`). The composed
// database forwards to that function.
pub trait TorrentHistoryDatabase: TorrentDatabase + 'static {
    fn insert_job_history_tx<'a>(
        tx: &'a mut SqlTx<'_>,
        entry: &'a JobHistoryRow,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>>;
}

pub trait TorrentsPersistenceMarkTorrentRetiringInternalQueries {
    fn mark_torrent_retiring(&self, job_id: JobId, delete_files: bool) -> Result<(), StateError>;
    fn mark_torrents_retiring(&self, marks: &[(JobId, bool)]) -> Result<(), StateError>;
    fn torrent_is_retiring(&self, job_id: JobId) -> Result<bool, StateError>;
    fn retiring_torrents(&self) -> Result<Vec<RetiringRow>, StateError>;
    fn archive_retiring_torrents(
        &self,
        rows: Vec<(JobId, JobHistoryRow, Vec<HistoryFile>)>,
    ) -> RetiringArchived;
}

impl<D: TorrentHistoryDatabase> TorrentsPersistenceMarkTorrentRetiringInternalQueries for D {
    // A removal's mark, before anything of the torrent goes: the flag that
    // it is leaving, and whether its files go with it (`delete_files`). A
    // stop from then on brings it back retiring, never live, and the next
    // start finishes the removal from its row. One small UPDATE.
    fn mark_torrent_retiring(&self, job_id: JobId, delete_files: bool) -> Result<(), StateError> {
        #[cfg(any(test, feature = "test-support"))]
        match MARK_REFUSED.load(std::sync::atomic::Ordering::Relaxed) {
            1 => return Err(StateError::OutOfSpace("database or disk is full".into())),
            2 => return Err(StateError::Database("the mark was refused".into())),
            _ => {}
        }
        let args = vec![
            SqlArg::Text(RetiringMark::of(delete_files).as_str().to_owned()),
            SqlArg::I64(stored(job_id.0)?),
        ];
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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
    fn mark_torrents_retiring(&self, marks: &[(JobId, bool)]) -> Result<(), StateError> {
        #[cfg(any(test, feature = "test-support"))]
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
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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
    fn torrent_is_retiring(&self, job_id: JobId) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
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
    fn retiring_torrents(&self) -> Result<Vec<RetiringRow>, StateError> {
        #[cfg(any(test, feature = "test-support"))]
        if RETIRING_READ_FAILS.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(StateError::Database(
                "the retiring torrents do not read".into(),
            ));
        }
        let key = self.borrow().encryption_key().cloned();
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
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
            rows.iter()
                .map(|row| retiring_row(row, key.as_ref()))
                .collect()
        })
    }

    // Each of `rows`, a retiring torrent's history row and the files it
    // lists, to history and out of the torrents: all in one transaction,
    // and when that fails, each in its own, so one row that cannot be
    // written holds back none of the others. A row left retiring never
    // loads as a live torrent; the service shows it and writes it through
    // the kept archives (`TorrentService::retry_retiring`).
    fn archive_retiring_torrents(
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
}

trait TorrentsPersistenceWriteRetiringLocalQueries {
    fn write_retiring(
        &self,
        rows: &[(JobId, JobHistoryRow, Vec<HistoryFile>)],
    ) -> Result<Vec<JobId>, StateError>;
}

impl<D: TorrentHistoryDatabase> TorrentsPersistenceWriteRetiringLocalQueries for D {
    // `rows` to history, out of the torrents, in one transaction.
    fn write_retiring(
        &self,
        rows: &[(JobId, JobHistoryRow, Vec<HistoryFile>)],
    ) -> Result<Vec<JobId>, StateError> {
        #[cfg(any(test, feature = "test-support"))]
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
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "archive_retiring_torrents", |tx| {
                let rows = rows.clone();
                Box::pin(async move {
                    let mut written = Vec::with_capacity(rows.len());
                    for (job_id, entry, files) in &rows {
                        let id = stored(job_id.0)?;
                        D::insert_job_history_tx(tx, entry).await?;
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
#[cfg(any(test, feature = "test-support"))]
pub static RETIRING_REFUSED: std::sync::Mutex<Vec<JobId>> = std::sync::Mutex::new(Vec::new());

// Test seam: the next batch of torrent history rows is refused for want of
// room, as one transaction, after its rows are written in it.
#[cfg(any(test, feature = "test-support"))]
pub static HISTORIES_REFUSED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: a removal's mark is refused, 1 for want of room, 2 otherwise.
#[cfg(any(test, feature = "test-support"))]
pub static MARK_REFUSED: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

// Test seam: the retiring torrents do not read.
#[cfg(any(test, feature = "test-support"))]
pub static RETIRING_READ_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// What a start's archive of the retiring torrents came to: the jobs written
// to history, and those left retiring, never live, for the service to retry.
#[derive(Debug, Default)]
pub struct RetiringArchived {
    pub written: Vec<JobId>,
    pub(crate) left: Vec<JobId>,
}

// A retiring torrent's row, as finishing its removal at a start reads it.
#[derive(Debug)]
pub struct RetiringRow {
    pub job_id: JobId,
    // Whether its removal asked for its files.
    pub delete_files: bool,
    pub(crate) metainfo: Option<Vec<u8>>,
    pub(crate) storage_dir: Option<String>,
    pub(crate) files_dir: Option<String>,
    // None when it does not read.
    pub(crate) outputs: Option<Vec<String>>,
    pub adopted: Option<super::model::Adopted>,
    // Its history row as its torrents row gives it, kept from the one its
    // job has where there is one (a failed finish's status and why).
    pub(crate) history: JobHistoryRow,
}

fn retiring_row(
    row: &weaver_server_db::persistence::sql_runtime::SqlRow,
    key: Option<&weaver_server_db::persistence::encryption::EncryptionKey>,
) -> Result<RetiringRow, StateError> {
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
        protocol: weaver_server_jobs::jobs::model::Protocol::Torrent,
        archived_at: None,
    };
    Ok(RetiringRow {
        job_id,
        // A mark of any other word keeps its files, as one never asked.
        delete_files: RetiringMark::parse(&row.text("retiring")?) == Some(RetiringMark::Deleted),
        // One that does not open keeps its files, as one whose metainfo
        // does not read.
        metainfo: row
            .opt_bytes("metainfo")?
            .and_then(|stored| match super::sealed::open_metainfo(key, stored) {
                Ok(metainfo) => Some(metainfo),
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "a removed torrent's metainfo does not decrypt; its files stay");
                    None
                }
            }),
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
pub struct HistoryFile {
    pub path: String,
    pub whole: bool,
}

// The files a torrent's job wrote, listed with its history row in `tx` in
// place of any list before: an archive from history removes these. In
// bind-sized chunks, each one statement.
pub async fn write_history_files_tx(
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
    let rows = weaver_server_db::persistence::sql_runtime::max_rows_for_tx(tx, 3);
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
pub trait TorrentsPersistenceJobHistoryFilesInternalQueries {
    fn job_history_files(
        &self,
        job_ids: &[JobId],
    ) -> Result<std::collections::BTreeMap<JobId, Vec<HistoryFile>>, StateError>;
}

impl TorrentsPersistenceJobHistoryFilesInternalQueries for SqlDatabase {
    fn job_history_files(
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
                weaver_server_db::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 1);
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

pub trait TorrentsPersistenceTorrentHistoryListsInternalQueries {
    fn torrent_history_lists(
        &self,
        job_ids: &[JobId],
    ) -> Result<std::collections::BTreeMap<JobId, Vec<HistoryFile>>, StateError>;
    fn history_job_is_torrent(&self, job_id: JobId) -> Result<bool, StateError>;
    fn listed_history_dirs(
        &self,
        dirs: &[(JobId, PathBuf)],
    ) -> Result<std::collections::HashSet<PathBuf>, StateError>;
}

impl TorrentsPersistenceTorrentHistoryListsInternalQueries for SqlDatabase {
    // The list of every torrent job's history row among `job_ids`, empty for
    // one that lists nothing: a torrent's job is removed from by its list
    // alone, never as an NZB job's output folder is, whatever it lists. One
    // batched read of the lists and one of which rows are torrents', each
    // served by its key.
    fn torrent_history_lists(
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
                weaver_server_db::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 1);
            for chunk in ids.chunks(rows_per) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let mut args = vec![SqlArg::Text(
                    weaver_server_jobs::jobs::model::Protocol::Torrent.as_str().to_owned(),
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
    fn history_job_is_torrent(&self, job_id: JobId) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 AS found FROM job_history WHERE job_id = {} AND protocol = {}",
                &[
                    SqlArg::I64(id),
                    SqlArg::Text(
                        weaver_server_jobs::jobs::model::Protocol::Torrent
                            .as_str()
                            .to_owned(),
                    ),
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
    fn listed_history_dirs(
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
            let rows_per = weaver_server_db::persistence::sql_runtime::max_rows_for_engine(
                datastore.engine(),
                2,
            );
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
