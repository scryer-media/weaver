// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// History state that only the NZBGet-compatible API reads or writes. It lives
// on the history row, so erasing the row erases it too, and the web UI never
// sees it.

use std::collections::HashMap;

use crate::StateError;
use crate::history::HistoryFailureKind;
use crate::jobs::{FieldUpdate, JobUpdate};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

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

impl Database {
    // Hide or unhide a history row from the NZBGet-compatible API. Returns
    // false when no history row exists for the job.
    pub fn set_nzbget_history_hidden(&self, job_id: u64, hidden: bool) -> Result<bool, StateError> {
        let job_id = i64::try_from(job_id)
            .map_err(|_| StateError::Database("job id is too large".into()))?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_nzbget_history_hidden", |tx| {
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE job_history SET nzbget_hidden = {} WHERE job_id = {}",
                            &[SqlArg::I64(i64::from(hidden)), SqlArg::I64(job_id)],
                        )
                        .await?;
                    Ok(changed > 0)
                })
            })
            .await
        })
    }

    // Rename, recategorise or re-parameterise a history row. A rename is kept
    // in the metadata too, so a later redownload or reprocess keeps it.
    // Returns false when no history row exists for the job.
    pub fn update_job_history_fields(
        &self,
        job_id: u64,
        update: &JobUpdate,
    ) -> Result<bool, StateError> {
        let job_id = i64::try_from(job_id)
            .map_err(|_| StateError::Database("job id is too large".into()))?;
        let mut update = update.clone();
        if let Some(name) = &update.name {
            let mut metadata = match &update.metadata {
                FieldUpdate::Set(metadata) => metadata.clone(),
                FieldUpdate::Clear => Vec::new(),
                FieldUpdate::Unchanged => self
                    .get_job_history(job_id as u64)?
                    .map(|row| crate::history::parse_history_metadata(row.metadata.as_deref()))
                    .unwrap_or_default(),
            };
            crate::ingest::set_renamed_title(&mut metadata, name);
            update.metadata = FieldUpdate::Set(metadata);
        }
        let metadata = match &update.metadata {
            FieldUpdate::Unchanged => None,
            FieldUpdate::Clear => Some(None),
            FieldUpdate::Set(metadata) => Some(crate::jobs::persistence::metadata_json(metadata)?),
        };
        let datastore = self.datastore();
        let found = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_job_history_fields", |tx| {
                let update = update.clone();
                let metadata = metadata.clone();
                Box::pin(async move {
                    let found = tx
                        .fetch_optional(
                            "SELECT job_id FROM job_history WHERE job_id = {}",
                            &[SqlArg::I64(job_id)],
                        )
                        .await?
                        .is_some();
                    if !found {
                        return Ok(false);
                    }
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
                        tx.execute(
                            "UPDATE job_history SET metadata = {} WHERE job_id = {}",
                            &[SqlArg::OptText(metadata), SqlArg::I64(job_id)],
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
    pub fn set_nzbget_history_mark(
        &self,
        job_id: u64,
        mark: NzbgetMarkStatus,
    ) -> Result<bool, StateError> {
        let job_id = i64::try_from(job_id)
            .map_err(|_| StateError::Database("job id is too large".into()))?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
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
    pub fn set_history_failure_kind(
        &self,
        job_id: u64,
        kind: Option<HistoryFailureKind>,
    ) -> Result<bool, StateError> {
        let job_id = i64::try_from(job_id)
            .map_err(|_| StateError::Database("job id is too large".into()))?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
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
    pub fn nzbget_history_states(
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
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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
