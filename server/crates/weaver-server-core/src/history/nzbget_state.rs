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
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};

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
        let update = update.clone();
        let datastore = self.datastore();
        let found = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_job_history_fields", |tx| {
                let update = update.clone();
                Box::pin(async move {
                    let lookup = match tx {
                        SqlTx::Postgres(_) => {
                            "SELECT metadata FROM job_history WHERE job_id = {} FOR UPDATE"
                        }
                        SqlTx::Sqlite(_) => "SELECT metadata FROM job_history WHERE job_id = {}",
                    };
                    let row = tx.fetch_optional(lookup, &[SqlArg::I64(job_id)]).await?;
                    let Some(row) = row else {
                        return Ok(false);
                    };
                    let metadata = if !update.metadata.is_unchanged()
                        || !update.metadata_edits.is_empty()
                        || update.name.is_some()
                    {
                        let mut metadata = crate::history::parse_history_metadata(
                            row.opt_text("metadata")?.as_deref(),
                        );
                        update.apply_to_metadata(&mut metadata);
                        if let Some(name) = &update.name {
                            crate::ingest::set_renamed_title(&mut metadata, name);
                        }
                        Some(crate::jobs::persistence::metadata_json(&metadata)?)
                    } else {
                        None
                    };
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

#[cfg(test)]
mod tests {
    use super::*;

    fn database_with_history() -> Database {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&crate::JobHistoryRow {
            job_id: 41,
            job_hash: None,
            name: "history fixture".into(),
            status: "complete".into(),
            error_message: None,
            total_bytes: 100,
            downloaded_bytes: 100,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: Some(
                serde_json::to_string(&[
                    ("priority", "NORMAL"),
                    ("obsolete", "remove me"),
                    ("untouched", "original"),
                ])
                .unwrap(),
            ),
            server_attribution: None,
        })
        .unwrap();
        db
    }

    #[test]
    fn history_metadata_edits_and_rename_preserve_intervening_updates() {
        let db = database_with_history();
        // Warm the row cache before either edit; both updates invalidate it.
        db.get_job_history(41).unwrap().unwrap();
        let update = JobUpdate {
            metadata_edits: vec![
                ("PRIORITY".into(), Some("HIGH".into())),
                ("OBSOLETE".into(), None),
            ],
            name: Some("renamed history".into()),
            ..Default::default()
        };
        db.update_job_history_fields(
            41,
            &JobUpdate {
                metadata_edits: vec![("later".into(), Some("other client".into()))],
                ..Default::default()
            },
        )
        .unwrap();
        db.update_job_history_fields(41, &update).unwrap();

        let row = db.get_job_history(41).unwrap().unwrap();
        assert_eq!(row.name, "renamed history");
        let metadata = crate::history::parse_history_metadata(row.metadata.as_deref());
        assert!(metadata.contains(&("untouched".into(), "original".into())));
        assert!(metadata.contains(&("later".into(), "other client".into())));
        assert!(metadata.contains(&("PRIORITY".into(), "HIGH".into())));
        assert_eq!(
            metadata
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case("priority"))
                .count(),
            1
        );
        assert!(
            !metadata
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case("obsolete"))
        );
        assert_eq!(
            crate::ingest::renamed_title(&metadata),
            Some("renamed history")
        );
    }

    #[test]
    fn history_rename_only_and_metadata_replacement_keep_their_semantics() {
        let db = database_with_history();
        db.update_job_history_fields(
            41,
            &JobUpdate {
                name: Some("rename only".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let row = db.get_job_history(41).unwrap().unwrap();
        let metadata = crate::history::parse_history_metadata(row.metadata.as_deref());
        assert!(metadata.contains(&("untouched".into(), "original".into())));
        assert_eq!(crate::ingest::renamed_title(&metadata), Some("rename only"));

        db.update_job_history_fields(
            41,
            &JobUpdate {
                metadata: FieldUpdate::Set(vec![("replacement".into(), "initial".into())]),
                metadata_edits: vec![("REPLACEMENT".into(), Some("final".into()))],
                name: Some("replacement title".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let row = db.get_job_history(41).unwrap().unwrap();
        let metadata = crate::history::parse_history_metadata(row.metadata.as_deref());
        assert_eq!(metadata.len(), 2);
        assert!(metadata.contains(&("REPLACEMENT".into(), "final".into())));
        assert_eq!(
            crate::ingest::renamed_title(&metadata),
            Some("replacement title")
        );

        db.update_job_history_fields(
            41,
            &JobUpdate {
                metadata: FieldUpdate::Clear,
                metadata_edits: vec![("after-clear".into(), Some("value".into()))],
                ..Default::default()
            },
        )
        .unwrap();
        let row = db.get_job_history(41).unwrap().unwrap();
        assert_eq!(
            crate::history::parse_history_metadata(row.metadata.as_deref()),
            vec![("after-clear".into(), "value".into())]
        );
        assert!(
            !db.update_job_history_fields(42, &JobUpdate::default())
                .unwrap()
        );
    }
}
