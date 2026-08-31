//! Durable state for post-processing: settings, script lists, script options,
//! and the per-job script results appended to the job's own rows.
//!
//! There are no post-processing tables. Lists and options live in the settings
//! KV, and results live on `active_jobs` / `job_history` beside the summary the
//! rest of the product already reads.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::model::{
    OptionName, OptionValue, PostProcessingSettings, PostProcessingSummary, ResolvedOption,
    ScriptListEntry, ScriptLists, ScriptName, ScriptResult, SecretOptionValue,
};
use crate::persistence::encryption::{decrypt_value, encrypt_value};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::persistence::{Database, StateError};

/// v2 deliberately starts from defaults: the 0.9 model runs every enabled script
/// in the directory, so the master switch has to be turned on again knowingly.
const SETTINGS_KEY: &str = "post_processing.settings.v2";
const SCRIPT_LISTS_KEY: &str = "post_processing.script_lists.v1";
const SCRIPT_OPTIONS_KEY: &str = "post_processing.script_options.v1";
const SCRIPT_DIRECTORY_KEY: &str = "post_processing.script_directory.v1";

/// Job metadata key carrying a submission-time script override from a facade.
pub const JOB_SCRIPT_OVERRIDE_METADATA_KEY: &str = "weaver.post_processing.scripts";

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSecretOption {
    name: OptionName,
    ciphertext: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoredScriptOptions {
    #[serde(default)]
    plain: Vec<ResolvedOption>,
    #[serde(default)]
    secrets: Vec<StoredSecretOption>,
}

/// Encrypted option values captured with a post-processing admission.
#[derive(Debug, Clone, Default)]
pub(crate) struct ScriptOptionsSnapshot(BTreeMap<String, StoredScriptOptions>);

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
        if let Some(directory) = self.get_setting(SCRIPT_DIRECTORY_KEY)? {
            return Ok(PathBuf::from(directory));
        }
        let candidate = env_seed
            .map(Path::to_path_buf)
            .unwrap_or_else(|| data_dir.join("scripts"));
        let directory = normalize_script_directory(&candidate)
            .map_err(|error| StateError::Database(error.to_string()))?;
        self.set_setting(SCRIPT_DIRECTORY_KEY, &directory.to_string_lossy())?;
        Ok(directory)
    }

    pub fn post_processing_script_directory(&self) -> Result<PathBuf, StateError> {
        self.get_setting(SCRIPT_DIRECTORY_KEY)?
            .map(PathBuf::from)
            .ok_or_else(|| {
                StateError::Database("post-processing scripts directory is not initialized".into())
            })
    }

    /// Persist a validated directory and drop every name-based assignment in
    /// the same transaction. Script files themselves are never touched.
    pub fn replace_post_processing_script_directory(
        &self,
        directory: &Path,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let directory = directory.to_string_lossy().to_string();
        self.run_sql_blocking(async move {
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
                        for key in [SCRIPT_LISTS_KEY, SCRIPT_OPTIONS_KEY] {
                            tx.execute(
                                "DELETE FROM settings WHERE key = {}",
                                &[SqlArg::Text(key.to_string())],
                            )
                            .await?;
                        }
                        Ok(true)
                    })
                },
            )
            .await
        })
    }

    pub fn post_processing_settings(&self) -> Result<PostProcessingSettings, StateError> {
        self.get_setting(SETTINGS_KEY)?
            .map(|raw| from_json(&raw))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    /// Read the settings, script lists, and scripts root from one database
    /// snapshot so post-processing admission cannot pair an old list with a
    /// directory that has just replaced it.
    pub(crate) fn post_processing_script_admission(
        &self,
    ) -> Result<
        (
            PostProcessingSettings,
            ScriptLists,
            PathBuf,
            ScriptOptionsSnapshot,
        ),
        StateError,
    > {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT key, value FROM settings WHERE key IN ({}, {}, {}, {})",
                &[
                    SqlArg::Text(SETTINGS_KEY.to_string()),
                    SqlArg::Text(SCRIPT_LISTS_KEY.to_string()),
                    SqlArg::Text(SCRIPT_DIRECTORY_KEY.to_string()),
                    SqlArg::Text(SCRIPT_OPTIONS_KEY.to_string()),
                ],
            )
            .await?;
            let mut settings = None;
            let mut lists = None;
            let mut script_directory = None;
            let mut options = None;
            for row in rows {
                match row.text("key")?.as_str() {
                    SETTINGS_KEY => settings = Some(row.text("value")?),
                    SCRIPT_LISTS_KEY => lists = Some(row.text("value")?),
                    SCRIPT_DIRECTORY_KEY => script_directory = Some(row.text("value")?),
                    SCRIPT_OPTIONS_KEY => options = Some(row.text("value")?),
                    _ => {}
                }
            }
            Ok((
                settings
                    .as_deref()
                    .map(from_json)
                    .transpose()?
                    .unwrap_or_default(),
                lists
                    .as_deref()
                    .map(from_json)
                    .transpose()?
                    .unwrap_or_default(),
                script_directory.map(PathBuf::from).ok_or_else(|| {
                    StateError::Database(
                        "post-processing scripts directory is not initialized".into(),
                    )
                })?,
                ScriptOptionsSnapshot(
                    options
                        .as_deref()
                        .map(from_json)
                        .transpose()?
                        .unwrap_or_default(),
                ),
            ))
        })
    }

    pub fn save_post_processing_settings(
        &self,
        settings: &PostProcessingSettings,
    ) -> Result<(), StateError> {
        let settings = settings.clone().normalized().map_err(state_err)?;
        self.set_setting(SETTINGS_KEY, &to_json(&settings)?)
    }

    pub fn post_processing_script_lists(&self) -> Result<ScriptLists, StateError> {
        self.get_setting(SCRIPT_LISTS_KEY)?
            .map(|raw| from_json(&raw))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    pub fn save_post_processing_script_lists(&self, lists: &ScriptLists) -> Result<(), StateError> {
        self.set_setting(SCRIPT_LISTS_KEY, &to_json(lists)?)
    }

    /// Options for `script`, with secrets decrypted for execution.
    pub fn post_processing_script_options(
        &self,
        script: &ScriptName,
    ) -> Result<Vec<ResolvedOption>, StateError> {
        self.resolve_post_processing_script_options(
            &self.post_processing_script_options_snapshot()?,
            script,
        )
    }

    pub(crate) fn post_processing_script_options_snapshot(
        &self,
    ) -> Result<ScriptOptionsSnapshot, StateError> {
        Ok(ScriptOptionsSnapshot(self.stored_script_options()?))
    }

    pub(crate) fn resolve_post_processing_script_options(
        &self,
        snapshot: &ScriptOptionsSnapshot,
        script: &ScriptName,
    ) -> Result<Vec<ResolvedOption>, StateError> {
        let Some(entry) = snapshot.0.get(script.as_str()) else {
            return Ok(vec![]);
        };
        let key = self.encryption_key();
        let mut options = entry.plain.clone();
        for secret in &entry.secrets {
            let key = key.ok_or_else(|| {
                StateError::Database("encryption key is required to load secret options".into())
            })?;
            let plaintext = decrypt_value(key, &secret.ciphertext).map_err(state_err)?;
            options.push(ResolvedOption::new(
                secret.name.clone(),
                OptionValue::Secret(SecretOptionValue::for_execution(plaintext)),
            ));
        }
        Ok(options)
    }

    /// Replace the stored options for `script`. Secret values go through the
    /// settings-encryption envelope so they never sit in the KV in cleartext.
    pub fn save_post_processing_script_options(
        &self,
        script: &ScriptName,
        options: &[ResolvedOption],
    ) -> Result<(), StateError> {
        let key = self.encryption_key();
        let mut entry = StoredScriptOptions::default();
        for option in options {
            match option.value() {
                OptionValue::Secret(value) => {
                    let key = key.ok_or_else(|| {
                        StateError::Database(
                            "encryption key is required to store secret options".into(),
                        )
                    })?;
                    entry.secrets.push(StoredSecretOption {
                        name: option.name().clone(),
                        ciphertext: encrypt_value(key, value.expose_for_execution())
                            .map_err(state_err)?,
                    });
                }
                _ => entry.plain.push(option.clone()),
            }
        }
        let mut stored = self.stored_script_options()?;
        if entry.plain.is_empty() && entry.secrets.is_empty() {
            stored.remove(script.as_str());
        } else {
            stored.insert(script.as_str().to_string(), entry);
        }
        self.set_setting(SCRIPT_OPTIONS_KEY, &to_json(&stored)?)
    }

    /// Script names that currently have stored options, for the settings UI.
    pub fn post_processing_scripts_with_options(&self) -> Result<Vec<String>, StateError> {
        Ok(self.stored_script_options()?.into_keys().collect())
    }

    fn stored_script_options(&self) -> Result<BTreeMap<String, StoredScriptOptions>, StateError> {
        self.get_setting(SCRIPT_OPTIONS_KEY)?
            .map(|raw| from_json(&raw))
            .transpose()
            .map(Option::unwrap_or_default)
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
        let datastore = self.datastore();
        let job_id = job_id_i64(job_id)?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE active_jobs SET post_processing_summary = 'running' WHERE job_id = {}",
                &[SqlArg::I64(job_id)],
            )
            .await?;
            Ok(())
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

/// Read a submission-time script override out of a job's metadata.
///
/// The NZBGet facade is the only writer today: it turns `<ScriptName>:` NZB
/// parameters into this list. Unparseable names are dropped rather than failing
/// the job, matching how NZBGet ignores parameters it does not recognise.
pub fn job_script_override(metadata: &[(String, String)]) -> Option<Vec<ScriptListEntry>> {
    let raw = metadata
        .iter()
        .find(|(key, _)| key == JOB_SCRIPT_OVERRIDE_METADATA_KEY)
        .map(|(_, value)| value.as_str())?;
    Some(
        raw.split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .filter_map(|name| ScriptName::new(name).ok())
            .map(ScriptListEntry::new)
            .collect(),
    )
}

/// Encode a submission-time script override for storage in job metadata.
pub fn encode_job_script_override(scripts: &[ScriptName]) -> String {
    scripts
        .iter()
        .map(ScriptName::as_str)
        .collect::<Vec<_>>()
        .join(",")
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
