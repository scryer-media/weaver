// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::cache::HistoryCacheQueries;
use super::database::HistoryDatabase;
// History state that only the NZBGet-compatible API reads or writes. It lives
// on the history row, so erasing the row erases it too, and the web UI never
// sees it.

use std::collections::HashMap;

use super::failure_kind::HistoryFailureKind;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, max_rows_for_tx};
use weaver_server_jobs::jobs::model::{FieldUpdate, JobUpdate};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NzbgetHistoryState {
    // Removed from the client's default history listing by HistoryDelete.
    pub hidden: bool,
    // Why the job failed, when it failed and weaver kept the kind.
    pub failure_kind: Option<HistoryFailureKind>,
    // How the client marked the row with HistoryMark*.
    pub mark: Option<NzbgetMarkStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NzbgetMarkStatus {
    Bad,
    Good,
    Success,
}

impl NzbgetMarkStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bad => "BAD",
            Self::Good => "GOOD",
            Self::Success => "SUCCESS",
        }
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "BAD" => Some(Self::Bad),
            "GOOD" => Some(Self::Good),
            "SUCCESS" => Some(Self::Success),
            _ => None,
        }
    }
}

pub trait HistoryNzbgetStateQueries {
    fn set_nzbget_history_hidden(&self, job_id: u64, hidden: bool) -> Result<bool, StateError>;
    fn archive_nzbget_history(&self, job_ids: &[u64]) -> Result<u64, StateError>;
    fn set_nzbget_history_rows_hidden(
        &self,
        job_ids: &[u64],
        hidden: bool,
    ) -> Result<u64, StateError>;
    fn update_job_history_fields(
        &self,
        job_id: u64,
        update: &JobUpdate,
    ) -> Result<bool, StateError>;
    fn set_nzbget_history_mark(
        &self,
        job_id: u64,
        mark: NzbgetMarkStatus,
    ) -> Result<bool, StateError>;
    fn set_history_failure_kind(
        &self,
        job_id: u64,
        kind: Option<HistoryFailureKind>,
    ) -> Result<bool, StateError>;
    fn nzbget_history_states(
        &self,
        job_ids: &[u64],
    ) -> Result<HashMap<u64, NzbgetHistoryState>, StateError>;
}

impl<D: HistoryDatabase> HistoryNzbgetStateQueries for D {
    // Hide or unhide a history row from the NZBGet-compatible API. Returns
    // false when no history row exists for the job.
    fn set_nzbget_history_hidden(&self, job_id: u64, hidden: bool) -> Result<bool, StateError> {
        Ok(self.set_nzbget_history_rows_hidden(&[job_id], hidden)? > 0)
    }

    // NZBGet's non-final HistoryDelete: archive the entries of `job_ids` and
    // hide from the NZBGet-compatible API the ones the archive took or had
    // already archived. Returns how many were hidden.
    //
    // The archive is accepted first, so an entry leaves the client's history
    // only with its archive queued or done. One another operation holds, or a
    // job back in the queue, stays listed, and the client's next remove tries
    // again. A hide that fails after the accept leaves the entry listed; the
    // next remove finds it archived and hides it.
    fn archive_nzbget_history(&self, job_ids: &[u64]) -> Result<u64, StateError> {
        let taken = self.accept_history_archive(job_ids)?;
        if taken.is_empty() {
            return Ok(0);
        }
        self.set_nzbget_history_rows_hidden(&taken, true)
    }

    // Hide or unhide the history rows of `job_ids` in one transaction, in
    // statements of as many ids as the engine binds. Returns how many rows
    // exist for the ids; an id with no row changes nothing.
    fn set_nzbget_history_rows_hidden(
        &self,
        job_ids: &[u64],
        hidden: bool,
    ) -> Result<u64, StateError> {
        let ids = job_ids
            .iter()
            .map(|id| {
                i64::try_from(*id).map_err(|_| StateError::Database("job id is too large".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if ids.is_empty() {
            return Ok(0);
        }
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_nzbget_history_hidden", |tx| {
                let ids = ids.clone();
                Box::pin(async move {
                    let mut changed = 0;
                    // One bind for `hidden`, one for each id.
                    for chunk in ids.chunks(max_rows_for_tx(tx, 1) - 1) {
                        let placeholders = vec!["{}"; chunk.len()].join(", ");
                        let args = std::iter::once(SqlArg::I64(i64::from(hidden)))
                            .chain(chunk.iter().copied().map(SqlArg::I64))
                            .collect::<Vec<_>>();
                        changed += tx
                            .execute(
                                &format!(
                                    "UPDATE job_history SET nzbget_hidden = {{}} \
                                     WHERE job_id IN ({placeholders})"
                                ),
                                &args,
                            )
                            .await?;
                    }
                    Ok(changed)
                })
            })
            .await
        })
    }

    // Rename, recategorise or re-parameterise a history row. A rename is kept
    // in the metadata too, so a later redownload or reprocess keeps it.
    // Returns false when no history row exists for the job.
    fn update_job_history_fields(
        &self,
        job_id: u64,
        update: &JobUpdate,
    ) -> Result<bool, StateError> {
        let job_id = i64::try_from(job_id)
            .map_err(|_| StateError::Database("job id is too large".into()))?;
        let update = update.clone();
        let datastore = self.borrow().datastore();
        let found = self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_job_history_fields", |tx| {
                let update = update.clone();
                Box::pin(async move {
                    let lookup = match tx {
                        SqlTx::Postgres(_) => {
                            "SELECT name, metadata FROM job_history WHERE job_id = {} FOR UPDATE"
                        }
                        SqlTx::Sqlite(_) => {
                            "SELECT name, metadata FROM job_history WHERE job_id = {}"
                        }
                    };
                    let row = tx.fetch_optional(lookup, &[SqlArg::I64(job_id)]).await?;
                    let Some(row) = row else {
                        return Ok(false);
                    };
                    let metadata = if !update.metadata.is_unchanged()
                        || !update.metadata_edits.is_empty()
                        || update.name.is_some()
                    {
                        let mut metadata = super::attributes::parse_history_metadata(
                            row.opt_text("metadata")?.as_deref(),
                        );
                        update.apply_to_metadata(&mut metadata);
                        if let Some(name) = &update.name {
                            weaver_server_base::ingest::metadata::set_renamed_title(&mut metadata, name);
                        }
                        Some(weaver_server_jobs::jobs::metadata_json::metadata_json(&metadata)?)
                    } else {
                        None
                    };
                    let update_name = update.name.clone();
                    if let Some(name) = update.name {
                        tx.execute(
                            "UPDATE job_history SET name = {} WHERE job_id = {}",
                            &[SqlArg::Text(name), SqlArg::I64(job_id)],
                        )
                        .await?;
                    }
                    let category = match update.category {
                        FieldUpdate::Unchanged => None,
                        FieldUpdate::Clear => Some(None),
                        FieldUpdate::Set(category) => Some(Some(category)),
                    };
                    if let Some(category) = category {
                        tx.execute(
                            "UPDATE job_history SET category = {} WHERE job_id = {}",
                            &[SqlArg::OptText(category), SqlArg::I64(job_id)],
                        )
                        .await?;
                    }
                    if let Some(metadata) = metadata {
                        // The full release name and search key follow the
                        // row's name and metadata, and a rename always
                        // rewrites the metadata.
                        let name = match &update_name {
                            Some(name) => name.clone(),
                            None => row.text("name")?,
                        };
                        let (release_name, search_key) =
                            super::attributes::history_search_columns(&name, metadata.as_deref());
                        tx.execute(
                            "UPDATE job_history SET metadata = {}, release_name = {}, search_key = {}
                              WHERE job_id = {}",
                            &[
                                SqlArg::OptText(metadata),
                                SqlArg::Text(release_name),
                                SqlArg::Text(search_key),
                                SqlArg::I64(job_id),
                            ],
                        )
                        .await?;
                    }
                    Ok(true)
                })
            })
            .await
        })?;
        self.invalidate_job_history_cache(job_id as u64);
        Ok(found)
    }

    // Record how the client marked a history row. Returns false when no
    // history row exists for the job.
    fn set_nzbget_history_mark(
        &self,
        job_id: u64,
        mark: NzbgetMarkStatus,
    ) -> Result<bool, StateError> {
        let job_id = i64::try_from(job_id)
            .map_err(|_| StateError::Database("job id is too large".into()))?;
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_nzbget_history_mark", |tx| {
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE job_history SET nzbget_mark_status = {} WHERE job_id = {}",
                            &[SqlArg::Text(mark.as_str().to_string()), SqlArg::I64(job_id)],
                        )
                        .await?;
                    Ok(changed > 0)
                })
            })
            .await
        })
    }

    // Record why a history row's job failed, or clear it. Returns false when
    // no history row exists for the job.
    fn set_history_failure_kind(
        &self,
        job_id: u64,
        kind: Option<HistoryFailureKind>,
    ) -> Result<bool, StateError> {
        let job_id = i64::try_from(job_id)
            .map_err(|_| StateError::Database("job id is too large".into()))?;
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_history_failure_kind", |tx| {
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE job_history SET failure_kind = {} WHERE job_id = {}",
                            &[
                                SqlArg::OptText(kind.map(|kind| kind.as_str().to_string())),
                                SqlArg::I64(job_id),
                            ],
                        )
                        .await?;
                    Ok(changed > 0)
                })
            })
            .await
        })
    }

    // The facade-only state for each listed history row. Rows absent from the
    // map have no history row.
    fn nzbget_history_states(
        &self,
        job_ids: &[u64],
    ) -> Result<HashMap<u64, NzbgetHistoryState>, StateError> {
        if job_ids.is_empty() {
            return Ok(HashMap::new());
        }
        // Internal u64 job ids, inlined as decimal integers.
        let ids = job_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT job_id, nzbget_hidden, failure_kind, nzbget_mark_status FROM job_history WHERE job_id IN ({ids})"
        );
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &[]).await?;
            let mut states = HashMap::with_capacity(rows.len());
            for row in rows {
                states.insert(
                    row.i64("job_id")? as u64,
                    NzbgetHistoryState {
                        hidden: row.i64("nzbget_hidden")? != 0,
                        failure_kind: row
                            .opt_text("failure_kind")?
                            .as_deref()
                            .and_then(HistoryFailureKind::from_persisted),
                        mark: row
                            .opt_text("nzbget_mark_status")?
                            .as_deref()
                            .and_then(NzbgetMarkStatus::from_persisted),
                    },
                );
            }
            Ok(states)
        })
    }
}
