//! Durable state for post-processing: settings, the scripts root, and the
//! per-job script results appended to the job's own rows.
//!
//! Settings live in the settings KV, and results live on `active_jobs` /
//! `job_history` beside the summary the rest of the product already reads.
//! What runs is kept as script instances, in tables of their own.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::model::{
    PostProcessingFacts, PostProcessingResume, PostProcessingSettings, PostProcessingSummary,
    ScriptResult, StartedScript,
};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use crate::persistence::{Database, StateError};

const SETTINGS_KEY: &str = "post_processing.settings.v2";
const SCRIPT_DIRECTORY_KEY: &str = "post_processing.script_directory.v1";

#[derive(Debug, thiserror::Error)]
pub enum ScriptDirectoryError {
    #[error("scripts directory must be an absolute path")]
    RelativePath,
    #[error("scripts directory is not a directory: {0}")]
    NotDirectory(PathBuf),
    #[error("scripts directory could not be prepared: {0}")]
    Io(#[from] std::io::Error),
}

/// Create, canonicalize, and prove that a scripts root can be listed.
///
/// A read-only bind mount is valid when it already exists: only creating a
/// previously absent directory requires write access to its parent.
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
    /// Return the persisted scripts root, seeding it exactly once when absent.
    ///
    /// Environment input is intentionally accepted only here; a stored value
    /// is always authoritative after this first settlement.
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

    /// Persist a validated directory and, in the same transaction, turn off
    /// every instance: each names a script, and a name in the new directory
    /// is not the script the operator wired up. What was saved in them is
    /// kept. Script files themselves are never touched.
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

    pub fn post_processing_settings(&self) -> Result<PostProcessingSettings, StateError> {
        self.get_setting(SETTINGS_KEY)?
            .map(|raw| from_json(&raw))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    /// Read the settings and the scripts root from one database snapshot.
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
                settings
                    .as_deref()
                    .map(from_json)
                    .transpose()?
                    .unwrap_or_default(),
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

    /// Save a full settings update while optionally retaining the extension
    /// policy already stored in the settings document. The read, merge, and
    /// write share one transaction so an omitted GraphQL field cannot erase a
    /// concurrent extension-policy update.
    pub fn save_post_processing_settings_preserving_extensions(
        &self,
        settings: PostProcessingSettings,
        preserve_extensions: bool,
    ) -> Result<PostProcessingSettings, StateError> {
        let datastore = self.datastore();
        let default_settings = to_json(&PostProcessingSettings::default())?;
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "save_post_processing_settings_preserving_extensions",
                |tx| {
                    let default_settings = default_settings.clone();
                    let settings = settings.clone();
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
                        let stored = from_json::<PostProcessingSettings>(&stored)?;
                        let mut settings = settings;
                        if preserve_extensions {
                            settings.unacceptable_extensions = stored.unacceptable_extensions.clone();
                        }
                        let settings = settings.normalized().map_err(state_err)?;
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

    /// Stamp a job's script results and rollup summary onto whichever of its rows exist.
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

    /// The job's script results, preferring the live row and falling back to history.
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

    /// Record that a job's scripts have started, so a crash is recoverable.
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
                datastore.read_exec(),
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
                datastore.read_exec(),
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
                datastore.read_exec(),
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
                datastore.read_exec(),
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

    /// Mark every job that was mid-post-processing when weaver stopped.
    ///
    /// One statement replaces the old run/attempt sweep: a `running` summary is
    /// only ever left behind by a process that died, because a completed pass
    /// always overwrites it.
    pub fn recover_interrupted_post_processing(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE active_jobs SET post_processing_summary = 'interrupted'
                  WHERE post_processing_summary = 'running'",
                &[],
            )
            .await
        })
    }

    /// The summary currently recorded for a job, used by restart recovery.
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

/// Whether `next` keeps fewer runs than `previous` did. Raising a limit
/// deletes nothing, so it needs no trim.
fn retention_lowered(previous: &PostProcessingSettings, next: &PostProcessingSettings) -> bool {
    let (previous, next) = (&previous.event_scripts, &next.event_scripts);
    next.script_output_runs_per_job < previous.script_output_runs_per_job
        || next.script_output_failed_runs_per_job < previous.script_output_failed_runs_per_job
}

fn job_id_i64(job_id: u64) -> Result<i64, StateError> {
    i64::try_from(job_id).map_err(|_| StateError::Database("job id is out of range".into()))
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
}
