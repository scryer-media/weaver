// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Durable state for post-processing: settings, the scripts root, and the
// per-job script results appended to the job's own rows.
//
// Settings live in the settings KV, and results live on `active_jobs` /
// `job_history` beside the summary the rest of the product already reads.
// What runs is kept as script instances, in tables of their own.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::model::{
    PostProcessingFacts, PostProcessingResume, PostProcessingSettings, PostProcessingSummary,
    ScriptResult, StartedScript,
};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, max_rows_for_engine};
use crate::persistence::{Database, StateError};

pub(crate) const SETTINGS_KEY: &str = "post_processing.settings.v2";
pub(crate) const SCRIPT_DIRECTORY_KEY: &str = "post_processing.script_directory.v1";

#[derive(Debug, thiserror::Error)]
pub enum ScriptDirectoryError {
    #[error("scripts directory must be an absolute path")]
    RelativePath,
    #[error("scripts directory is not a directory: {0}")]
    NotDirectory(PathBuf),
    #[error("scripts directory could not be prepared: {0}")]
    Io(#[from] std::io::Error),
}

// Create, canonicalize, and prove that a scripts root can be listed.
//
// A read-only bind mount is valid when it already exists: only creating a
// previously absent directory requires write access to its parent.
pub fn normalize_script_directory(path: &Path) -> Result<PathBuf, ScriptDirectoryError> {
    if !path.is_absolute() {
        return Err(ScriptDirectoryError::RelativePath);
    }
    fs::create_dir_all(path)?;
    let canonical = fs::canonicalize(path)?;
    if !canonical.is_dir() {
        return Err(ScriptDirectoryError::NotDirectory(canonical));
    }
    fs::read_dir(&canonical)?;
    Ok(canonical)
}

impl Database {
    // Return the persisted scripts root, seeding it exactly once when absent.
    //
    // Environment input is intentionally accepted only here; a stored value
    // is always authoritative after this first settlement.
    pub fn initialize_post_processing_script_directory(
        &self,
        data_dir: &Path,
        env_seed: Option<&Path>,
    ) -> Result<PathBuf, StateError> {
        let directory = match self.get_setting(SCRIPT_DIRECTORY_KEY)? {
            Some(directory) => PathBuf::from(directory),
            None => {
                let candidate = env_seed
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| data_dir.join("scripts"));
                let directory = normalize_script_directory(&candidate)
                    .map_err(|error| StateError::Database(error.to_string()))?;
                self.set_setting(SCRIPT_DIRECTORY_KEY, &directory.to_string_lossy())?;
                directory
            }
        };
        Ok(directory)
    }

    pub fn post_processing_script_directory(&self) -> Result<PathBuf, StateError> {
        self.get_setting(SCRIPT_DIRECTORY_KEY)?
            .map(PathBuf::from)
            .ok_or_else(|| {
                StateError::Database("post-processing scripts directory is not initialized".into())
            })
    }

    // Persist a validated directory and, in the same transaction, turn off
    // every instance: each names a script, and a name in the new directory
    // is not the script the operator wired up. What was saved in them is
    // kept. Script files themselves are never touched.
    pub fn replace_post_processing_script_directory(
        &self,
        directory: &Path,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let directory = directory.to_string_lossy().to_string();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "replace_post_processing_script_directory",
                |tx| {
                    let directory = directory.clone();
                    Box::pin(async move {
                        let existing = tx
                            .fetch_optional(
                                "SELECT value FROM settings WHERE key = {}",
                                &[SqlArg::Text(SCRIPT_DIRECTORY_KEY.to_string())],
                            )
                            .await?
                            .map(|row| row.text("value"))
                            .transpose()?;
                        if existing.as_deref() == Some(directory.as_str()) {
                            return Ok(false);
                        }
                        tx.execute(
                            "INSERT INTO settings (key, value) VALUES ({}, {})
                             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            &[
                                SqlArg::Text(SCRIPT_DIRECTORY_KEY.to_string()),
                                SqlArg::Text(directory),
                            ],
                        )
                        .await?;
                        tx.execute(
                            "UPDATE script_instances SET enabled = {}",
                            &[SqlArg::Bool(false)],
                        )
                        .await?;
                        Ok(true)
                    })
                },
            )
            .await
        });
        self.invalidate_queue_script_admission();
        result
    }

    // The stored settings, or the defaults when none are stored or the stored
    // value does not parse. A read that failed is still an error.
    pub fn post_processing_settings(&self) -> Result<PostProcessingSettings, StateError> {
        Ok(stored_settings(self.get_setting(SETTINGS_KEY)?.as_deref()))
    }

    // Read the settings and the scripts root from one database snapshot.
    pub(crate) fn post_processing_script_admission(
        &self,
    ) -> Result<(PostProcessingSettings, PathBuf), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT key, value FROM settings WHERE key IN ({}, {})",
                &[
                    SqlArg::Text(SETTINGS_KEY.to_string()),
                    SqlArg::Text(SCRIPT_DIRECTORY_KEY.to_string()),
                ],
            )
            .await?;
            let mut settings = None;
            let mut script_directory = None;
            for row in rows {
                match row.text("key")?.as_str() {
                    SETTINGS_KEY => settings = Some(row.text("value")?),
                    SCRIPT_DIRECTORY_KEY => script_directory = Some(row.text("value")?),
                    _ => {}
                }
            }
            Ok((
                stored_settings(settings.as_deref()),
                script_directory.map(PathBuf::from).ok_or_else(|| {
                    StateError::Database(
                        "post-processing scripts directory is not initialized".into(),
                    )
                })?,
            ))
        })
    }

    pub fn save_post_processing_settings(
        &self,
        settings: &PostProcessingSettings,
    ) -> Result<(), StateError> {
        let settings = settings.clone().normalized().map_err(state_err)?;
        let previous = self.post_processing_settings()?;
        let result = self.set_setting(SETTINGS_KEY, &to_json(&settings)?);
        self.invalidate_queue_script_admission();
        result?;
        if retention_lowered(&previous, &settings) {
            self.request_script_output_trim();
        }
        Ok(())
    }

    // Save a full settings update while optionally retaining the extension
    // policy already stored in the settings document. The read, merge, and
    // write share one transaction so an omitted GraphQL field cannot erase a
    // concurrent extension-policy update.
    pub fn save_post_processing_settings_preserving_extensions(
        &self,
        settings: PostProcessingSettings,
        preserve_extensions: bool,
    ) -> Result<PostProcessingSettings, StateError> {
        self.merge_post_processing_settings(
            "save_post_processing_settings_preserving_extensions",
            move |stored| {
                let mut settings = settings.clone();
                if preserve_extensions {
                    settings.unacceptable_extensions = stored.unacceptable_extensions.clone();
                }
                settings
            },
        )
    }

    // Replace the unwanted extension list and keep every other setting as
    // stored, in the same transaction as the read so a concurrent save of the
    // other settings is not undone.
    pub fn save_unacceptable_extensions(
        &self,
        extensions: Vec<String>,
    ) -> Result<PostProcessingSettings, StateError> {
        self.merge_post_processing_settings("save_unacceptable_extensions", move |stored| {
            PostProcessingSettings {
                unacceptable_extensions: extensions.clone(),
                ..stored.clone()
            }
        })
    }

    // Read the stored settings, merge an update into them, normalize, and
    // write the result back, all in one transaction.
    fn merge_post_processing_settings<M>(
        &self,
        op_name: &'static str,
        merge: M,
    ) -> Result<PostProcessingSettings, StateError>
    where
        M: Fn(&PostProcessingSettings) -> PostProcessingSettings + Send + Sync + 'static,
    {
        let datastore = self.datastore();
        let default_settings = to_json(&PostProcessingSettings::default())?;
        let merge = std::sync::Arc::new(merge);
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                op_name,
                |tx| {
                    let default_settings = default_settings.clone();
                    let merge = std::sync::Arc::clone(&merge);
                    Box::pin(async move {
                        tx.execute(
                            "INSERT INTO settings (key, value) VALUES ({}, {})\n                             ON CONFLICT(key) DO NOTHING",
                            &[
                                SqlArg::Text(SETTINGS_KEY.to_string()),
                                SqlArg::Text(default_settings),
                            ],
                        )
                        .await?;
                        let select_sql = match tx {
                            SqlTx::Postgres(_) => {
                                "SELECT value FROM settings WHERE key = {} FOR UPDATE"
                            }
                            SqlTx::Sqlite(_) => "SELECT value FROM settings WHERE key = {}",
                        };
                        let stored = tx
                            .fetch_optional(select_sql, &[SqlArg::Text(SETTINGS_KEY.to_string())])
                            .await?
                            .ok_or_else(|| {
                                StateError::Database(
                                    "post-processing settings disappeared during update".into(),
                                )
                            })?
                            .text("value")?;
                        let stored = stored_settings(Some(&stored));
                        let settings = merge(&stored).normalized().map_err(state_err)?;
                        let lowered = retention_lowered(&stored, &settings);
                        tx.execute(
                            "INSERT INTO settings (key, value) VALUES ({}, {})\n                             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            &[
                                SqlArg::Text(SETTINGS_KEY.to_string()),
                                SqlArg::Text(to_json(&settings)?),
                            ],
                        )
                        .await?;
                        Ok((settings, lowered))
                    })
                },
            )
            .await
        });
        self.invalidate_queue_script_admission();
        let (settings, lowered) = result?;
        if lowered {
            self.request_script_output_trim();
        }
        Ok(settings)
    }

    // Stamp a job's script results and rollup summary onto whichever of its rows exist.
    pub fn save_job_post_processing_results(
        &self,
        job_id: u64,
        summary: PostProcessingSummary,
        results: &[ScriptResult],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        let summary = summary.as_str().to_string();
        let results_json = if results.is_empty() {
            None
        } else {
            Some(to_json(&results)?)
        };
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_job_post_processing_results", |tx| {
                let summary = summary.clone();
                let results_json = results_json.clone();
                Box::pin(async move {
                    for table in ["active_jobs", "job_history"] {
                        tx.execute(
                            &format!(
                                "UPDATE {table} SET post_processing_summary = {{}},
                                        script_results_json = {{}}
                                  WHERE job_id = {{}}"
                            ),
                            &[
                                SqlArg::Text(summary.clone()),
                                SqlArg::OptText(results_json.clone()),
                                SqlArg::I64(job_id),
                            ],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // The job's script results, preferring the live row and falling back to history.
    pub fn job_post_processing_results(
        &self,
        job_id: u64,
    ) -> Result<Vec<ScriptResult>, StateError> {
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        self.run_sql_blocking_read(async move {
            for table in ["active_jobs", "job_history"] {
                let row = SqlRuntime::fetch_optional(
                    datastore.read_exec(),
                    &format!("SELECT script_results_json FROM {table} WHERE job_id = {{}}"),
                    &[SqlArg::I64(job_id)],
                )
                .await?;
                if let Some(row) = row
                    && let Some(json) = row.opt_text("script_results_json")?
                {
                    return from_json::<Vec<ScriptResult>>(&json);
                }
            }
            Ok(vec![])
        })
    }

    // The script results of each of `job_ids` that has any recorded, the same
    // results `job_post_processing_results` returns for it: a live row's
    // results win over its history row's. A job with none is left out.
    pub fn job_post_processing_results_many(
        &self,
        job_ids: &[u64],
    ) -> Result<std::collections::HashMap<u64, Vec<ScriptResult>>, StateError> {
        let mut wanted = job_ids
            .iter()
            .map(|job_id| job_id_i64(*job_id))
            .collect::<Result<Vec<_>, _>>()?;
        wanted.sort_unstable();
        wanted.dedup();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let chunk_size = max_rows_for_engine(datastore.engine(), 1);
            let mut found = std::collections::HashMap::new();
            for table in ["active_jobs", "job_history"] {
                let unresolved: Vec<i64> = wanted
                    .iter()
                    .copied()
                    .filter(|job_id| !found.contains_key(&(*job_id as u64)))
                    .collect();
                for chunk in unresolved.chunks(chunk_size) {
                    let placeholders = vec!["{}"; chunk.len()].join(", ");
                    let rows = SqlRuntime::fetch_all(
                        datastore.read_exec(),
                        &format!(
                            "SELECT job_id, script_results_json FROM {table}
                              WHERE job_id IN ({placeholders})
                                AND script_results_json IS NOT NULL"
                        ),
                        &chunk.iter().copied().map(SqlArg::I64).collect::<Vec<_>>(),
                    )
                    .await?;
                    for row in rows {
                        let job_id = row.i64("job_id")? as u64;
                        if let Some(json) = row.opt_text("script_results_json")? {
                            found.insert(job_id, from_json::<Vec<ScriptResult>>(&json)?);
                        }
                    }
                }
            }
            found.retain(|_, results: &mut Vec<ScriptResult>| !results.is_empty());
            Ok(found)
        })
    }

    // Record that a job's scripts have started, so a crash is recoverable.
    pub fn mark_job_post_processing_running(&self, job_id: u64) -> Result<(), StateError> {
        self.mark_job_post_processing_resumed(job_id, &PostProcessingResume::default())
    }

    // Record that a job's pass is under way again, carrying over the entries
    // that had started and the results the earlier pass left. A fresh pass
    // carries over nothing.
    pub fn mark_job_post_processing_resumed(
        &self,
        job_id: u64,
        resume: &PostProcessingResume,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        let started = to_json(&resume.started)?;
        let results = if resume.results.is_empty() {
            None
        } else {
            Some(to_json(&resume.results)?)
        };
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE active_jobs SET post_processing_summary = 'running',
                        post_processing_started = {},
                        script_results_json = {}
                  WHERE job_id = {}",
                &[
                    SqlArg::Text(started),
                    SqlArg::OptText(results),
                    SqlArg::I64(job_id),
                ],
            )
            .await?;
            Ok(())
        })
    }

    // Record the entries of a job's list that have started, written before
    // the newest of them starts.
    pub fn record_job_scripts_started(
        &self,
        job_id: u64,
        started: &[StartedScript],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        let started = to_json(&started)?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE active_jobs SET post_processing_started = {} WHERE job_id = {}",
                &[SqlArg::Text(started), SqlArg::I64(job_id)],
            )
            .await?;
            Ok(())
        })
    }

    // Record the results a job's pass has so far, leaving its summary as
    // `running`, so a restart keeps what already finished.
    pub fn save_job_post_processing_progress(
        &self,
        job_id: u64,
        results: &[ScriptResult],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        let results = if results.is_empty() {
            None
        } else {
            Some(to_json(&results)?)
        };
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE active_jobs SET script_results_json = {} WHERE job_id = {}",
                &[SqlArg::OptText(results), SqlArg::I64(job_id)],
            )
            .await?;
            Ok(())
        })
    }

    // Record what a job's scripts are told about how its download ended,
    // written when the pass begins.
    pub fn record_job_post_processing_facts(
        &self,
        job_id: u64,
        facts: &PostProcessingFacts,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        let facts = to_json(facts)?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE active_jobs SET post_processing_outcome = {} WHERE job_id = {}",
                &[SqlArg::Text(facts), SqlArg::I64(job_id)],
            )
            .await?;
            Ok(())
        })
    }

    // What a restored job's scripts were told when its pass began, or `None`
    // when no pass began under a weaver that kept it.
    pub fn job_post_processing_facts(
        &self,
        job_id: u64,
    ) -> Result<Option<PostProcessingFacts>, StateError> {
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT post_processing_outcome FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id)],
            )
            .await?;
            match row
                .map(|row| row.opt_text("post_processing_outcome"))
                .transpose()?
                .flatten()
            {
                Some(json) => Ok(Some(from_json::<PostProcessingFacts>(&json)?)),
                None => Ok(None),
            }
        })
    }

    // What an interrupted pass left for a restored job, or `None` when the
    // job has no record of which entries started, as a job whose pass began
    // under an older weaver does not.
    pub fn job_post_processing_resume(
        &self,
        job_id: u64,
    ) -> Result<Option<PostProcessingResume>, StateError> {
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        self.run_sql_blocking_read(async move {
            let Some(row) = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT post_processing_started, script_results_json
                   FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id)],
            )
            .await?
            else {
                return Ok(None);
            };
            let Some(started) = row.opt_text("post_processing_started")? else {
                return Ok(None);
            };
            let results = match row.opt_text("script_results_json")? {
                Some(json) => from_json::<Vec<ScriptResult>>(&json)?,
                None => vec![],
            };
            Ok(Some(PostProcessingResume {
                started: from_json::<Vec<StartedScript>>(&started)?,
                results,
            }))
        })
    }

    // Mark every job that was mid-post-processing when weaver stopped.
    //
    // One statement replaces the old run/attempt sweep: a `running` summary is
    // only ever left behind by a process that died, because a completed pass
    // always overwrites it.
    pub fn recover_interrupted_post_processing(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE active_jobs SET post_processing_summary = 'interrupted'
                  WHERE post_processing_summary = 'running'",
                &[],
            )
            .await
        })
    }

    // The summary currently recorded for a job, used by restart recovery.
    pub fn job_post_processing_summary(
        &self,
        job_id: u64,
    ) -> Result<Option<PostProcessingSummary>, StateError> {
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT post_processing_summary FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id)],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_text("post_processing_summary"))
                .transpose()?
                .flatten()
                .and_then(|value| PostProcessingSummary::from_persisted(&value)))
        })
    }
}

// Whether `next` keeps fewer runs than `previous` did. Raising a limit
// deletes nothing, so it needs no trim.
fn retention_lowered(previous: &PostProcessingSettings, next: &PostProcessingSettings) -> bool {
    let (previous, next) = (&previous.event_scripts, &next.event_scripts);
    next.script_output_runs_per_job < previous.script_output_runs_per_job
        || next.script_output_failed_runs_per_job < previous.script_output_failed_runs_per_job
}

fn job_id_i64(job_id: u64) -> Result<i64, StateError> {
    i64::try_from(job_id).map_err(|_| StateError::Database("job id is out of range".into()))
}

// Every read of the stored settings goes through here. A stored value that
// does not parse is logged, by key and never by what it holds, and the
// defaults stand in; the value is left as stored until the operator saves.
fn stored_settings(raw: Option<&str>) -> PostProcessingSettings {
    let Some(raw) = raw else {
        return PostProcessingSettings::default();
    };
    from_json(raw).unwrap_or_else(|_| {
        tracing::error!(
            setting = SETTINGS_KEY,
            "the stored post-processing settings do not parse; the defaults apply until they are saved again"
        );
        PostProcessingSettings::default()
    })
}

fn to_json<T: Serialize>(value: &T) -> Result<String, StateError> {
    serde_json::to_string(value).map_err(state_err)
}

fn from_json<T: for<'de> Deserialize<'de>>(value: &str) -> Result<T, StateError> {
    serde_json::from_str(value).map_err(state_err)
}

fn state_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn script_result(exit_code: i32) -> ScriptResult {
        ScriptResult {
            script: crate::post_processing::model::ScriptName::new("notify.sh").unwrap(),
            instance_id: None,
            instance_name: None,
            event: Default::default(),
            adapter: crate::post_processing::model::ScriptAdapter::Nzbget,
            status: crate::post_processing::model::ScriptStatus::Succeeded,
            exit_code: Some(exit_code),
            duration_ms: 5,
            output_tail: String::new(),
            output_id: None,
            background: false,
            output_truncated: false,
            error_message: None,
            finished_at_epoch_ms: 3,
        }
    }

    fn history_row(job_id: u64) -> crate::JobHistoryRow {
        crate::JobHistoryRow {
            job_id,
            job_hash: None,
            name: format!("Invented.Release.{job_id}"),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 1,
            downloaded_bytes: 1,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1_000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
            server_attribution: None,
            files_removed_at: None,
        }
    }

    fn active_job(job_id: u64) -> crate::ActiveJob {
        crate::ActiveJob {
            job_id: crate::JobId(job_id),
            nzb_hash: [0xBC; 32],
            nzb_path: PathBuf::from(format!("/invented/job-{job_id}.nzb")),
            nzb_zstd: Vec::new(),
            output_dir: PathBuf::from(format!("/invented/job-{job_id}")),
            created_at: 1_700_000_000,
            category: None,
            metadata: Vec::new(),
            status: "queued",
            download_state: "queued",
            post_state: "idle",
            run_state: "active",
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            password_override: None,
            supplied_password_candidates: Vec::new(),
        }
    }

    fn set_results_json(db: &Database, table: &'static str, job_id: u64, json: Option<&str>) {
        let datastore = db.datastore();
        let json = json.map(str::to_string);
        db.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                &format!("UPDATE {table} SET script_results_json = {{}} WHERE job_id = {{}}"),
                &[SqlArg::OptText(json), SqlArg::I64(job_id as i64)],
            )
            .await?;
            Ok(())
        })
        .unwrap();
    }

    // The batched read gives every job what the one-job read gives it,
    // including a live row winning over its history row and an empty live
    // result hiding a non-empty history one.
    #[test]
    fn batched_script_results_match_the_one_job_read() {
        let db = Database::open_in_memory().unwrap();
        let live_results = vec![script_result(1)];
        let history_results = vec![script_result(2), script_result(3)];
        let live_json = to_json(&live_results).unwrap();
        let history_json = to_json(&history_results).unwrap();

        // 1: live row only. 2: history row only. 3: both, both set.
        // 4: both, live unset. 5: both, live set to an empty list.
        // 6: neither row. 7: history row with nothing recorded.
        db.create_active_job(&active_job(1)).unwrap();
        set_results_json(&db, "active_jobs", 1, Some(&live_json));
        db.insert_job_history(&history_row(2)).unwrap();
        set_results_json(&db, "job_history", 2, Some(&history_json));
        for job_id in [3, 4, 5] {
            db.create_active_job(&active_job(job_id)).unwrap();
            db.insert_job_history(&history_row(job_id)).unwrap();
            set_results_json(&db, "job_history", job_id, Some(&history_json));
        }
        set_results_json(&db, "active_jobs", 3, Some(&live_json));
        set_results_json(&db, "active_jobs", 5, Some("[]"));
        db.insert_job_history(&history_row(7)).unwrap();

        let ids = [1, 2, 3, 4, 5, 6, 7, 3, 2];
        let batched = db.job_post_processing_results_many(&ids).unwrap();
        let mut one_by_one = std::collections::HashMap::new();
        for job_id in ids {
            let results = db.job_post_processing_results(job_id).unwrap();
            if !results.is_empty() {
                one_by_one.insert(job_id, results);
            }
        }
        assert_eq!(batched, one_by_one);
        assert_eq!(batched.get(&3), Some(&live_results));
        assert_eq!(batched.get(&4), Some(&history_results));
        assert!(!batched.contains_key(&5));
        assert!(db.job_post_processing_results_many(&[]).unwrap().is_empty());
    }

    #[test]
    fn stored_settings_from_earlier_shapes_still_load() {
        let db = Database::open_in_memory().unwrap();
        // As 0.14 stored it: no event-script fields at all.
        db.set_setting(
            SETTINGS_KEY,
            r#"{"executionEnabled":true,"concurrency":2,"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":["exe"]}"#,
        )
        .unwrap();
        let settings = db.post_processing_settings().unwrap();
        assert!(settings.execution_enabled);
        assert_eq!(settings.concurrency, 2);
        assert_eq!(settings.event_scripts.script_output_runs_per_job, 32);
        assert_eq!(settings.event_scripts.script_output_failed_runs_per_job, 8);

        // With output settings that no longer exist: they are ignored, and
        // the next save leaves them out.
        db.set_setting(
            SETTINGS_KEY,
            r#"{"eventScriptConcurrency":2,"eventScriptTimeoutSeconds":300,"fileDownloadedEventInterval":0,"scriptOutputCeilingBytes":1048576,"scriptOutputRunsPerJob":5,"scriptOutputRingBytes":67108864,"scriptOutputRunCapBytes":2097152,"executionEnabled":false,"concurrency":4,"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":[],"globalScriptsRun":"always"}"#,
        )
        .unwrap();
        // So is the event-script concurrency the one concurrency setting
        // replaced.
        let settings = db.post_processing_settings().unwrap();
        assert_eq!(settings.event_scripts.script_output_runs_per_job, 5);
        assert_eq!(settings.event_scripts.script_output_failed_runs_per_job, 8);
        db.save_post_processing_settings(&settings).unwrap();
        let stored = db.get_setting(SETTINGS_KEY).unwrap().unwrap();
        assert!(!stored.contains("scriptOutputCeilingBytes"));
        assert!(!stored.contains("scriptOutputRingBytes"));
        assert!(!stored.contains("scriptOutputRunCapBytes"));
        assert!(!stored.contains("eventScriptConcurrency"));
        assert!(stored.contains(r#""concurrency":4"#));
        assert!(stored.contains(r#""scriptOutputFailedRunsPerJob":8"#));
    }

    #[test]
    fn saving_the_extension_list_keeps_every_other_setting() {
        let db = Database::open_in_memory().unwrap();
        let mut settings = PostProcessingSettings {
            concurrency: 3,
            python_interpreter: Some("/usr/bin/python3".into()),
            ..PostProcessingSettings::default()
        };
        settings.event_scripts.script_output_runs_per_job = 7;
        db.save_post_processing_settings(&settings).unwrap();

        let saved = db
            .save_unacceptable_extensions(vec![" ISO ".into(), "exe".into(), "iso".into()])
            .unwrap();
        assert_eq!(saved.unacceptable_extensions, vec!["exe", "iso"]);
        let stored = db.post_processing_settings().unwrap();
        assert_eq!(stored.unacceptable_extensions, vec!["exe", "iso"]);
        assert_eq!(stored.concurrency, 3);
        assert_eq!(
            stored.python_interpreter.as_deref(),
            Some("/usr/bin/python3")
        );
        assert_eq!(stored.event_scripts.script_output_runs_per_job, 7);

        // An empty list turns the check off; a pattern it cannot read is refused.
        assert!(
            db.save_unacceptable_extensions(Vec::new())
                .unwrap()
                .unacceptable_extensions
                .is_empty()
        );
        assert!(db.save_unacceptable_extensions(vec!["a/b".into()]).is_err());
        assert!(
            db.post_processing_settings()
                .unwrap()
                .unacceptable_extensions
                .is_empty()
        );
    }

    #[test]
    fn every_read_of_a_stored_value_that_does_not_parse_gets_the_defaults() {
        let db = Database::open_in_memory().unwrap();
        db.initialize_post_processing_script_directory(
            &std::env::temp_dir(),
            Some(&std::env::temp_dir()),
        )
        .unwrap();
        db.set_setting(SETTINGS_KEY, "{unreadable").unwrap();
        assert_eq!(
            db.post_processing_settings().unwrap(),
            PostProcessingSettings::default()
        );
        let (admitted, _) = db.post_processing_script_admission().unwrap();
        assert_eq!(admitted, PostProcessingSettings::default());
        // Reads leave the value as stored.
        assert_eq!(
            db.get_setting(SETTINGS_KEY).unwrap().as_deref(),
            Some("{unreadable")
        );
    }

    #[test]
    fn a_save_replaces_a_stored_value_that_does_not_parse() {
        let db = Database::open_in_memory().unwrap();
        let settings = PostProcessingSettings {
            concurrency: 3,
            ..PostProcessingSettings::default()
        };
        db.set_setting(SETTINGS_KEY, "{unreadable").unwrap();
        db.save_post_processing_settings(&settings).unwrap();
        assert_eq!(db.post_processing_settings().unwrap().concurrency, 3);

        // The merging saves start from the defaults in its place.
        db.set_setting(SETTINGS_KEY, "{unreadable").unwrap();
        let saved = db.save_unacceptable_extensions(vec!["exe".into()]).unwrap();
        assert_eq!(
            saved,
            PostProcessingSettings {
                unacceptable_extensions: vec!["exe".into()],
                ..PostProcessingSettings::default()
            }
        );
        db.set_setting(SETTINGS_KEY, "{unreadable").unwrap();
        let saved = db
            .save_post_processing_settings_preserving_extensions(settings.clone(), true)
            .unwrap();
        assert_eq!(saved.concurrency, 3);
        assert_eq!(
            saved.unacceptable_extensions,
            PostProcessingSettings::default().unacceptable_extensions
        );
        assert_eq!(db.post_processing_settings().unwrap(), saved);
    }
}
