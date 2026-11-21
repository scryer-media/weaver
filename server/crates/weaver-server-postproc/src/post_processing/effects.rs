// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::post_processing::database::PostprocDatabase;
use crate::post_processing::events::PostProcessingEventsInvalidateQueueScriptAdmissionInternalQueries;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use weaver_server_settings::categories::queries::CategoriesQueries;

use serde::{Deserialize, Serialize};

use super::directives::Directive;
use super::runner::JobExecutionContext;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRuntime};

// Applied directives survive process termination independently of runner results.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobScriptEffects {
    pub parameters: BTreeMap<String, String>,
    pub directory: Option<PathBuf>,
    pub final_directory: Option<PathBuf>,
    pub marked_bad: bool,
}

impl weaver_server_jobs::jobs::database::ScriptEffectsParameters for JobScriptEffects {
    fn parameters_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.parameters
    }
}

impl JobScriptEffects {
    pub fn apply_to_context(&self, context: &mut JobExecutionContext) {
        self.merge_parameters(&mut context.compatibility.parameters);
        if let Some(path) = &self.directory {
            context.working_directory = path.clone();
            context.final_directory = path.clone();
        }
        context.compatibility.final_directory_override = self.final_directory.clone();
        context.compatibility.marked_bad |= self.marked_bad;
    }

    pub fn merge_parameters(&self, parameters: &mut Vec<(String, String)>) {
        for (name, value) in &self.parameters {
            parameters.retain(|(key, _)| key != name);
            if !value.is_empty() {
                parameters.push((name.clone(), value.clone()));
            }
        }
    }
}

pub trait PostProcessingEffectsQueries {
    fn job_script_effects(&self, job_id: u64) -> Result<JobScriptEffects, StateError>;
    fn save_job_script_effects(
        &self,
        job_id: u64,
        effects: &JobScriptEffects,
    ) -> Result<JobScriptEffects, StateError>;
}

impl<D: PostprocDatabase> PostProcessingEffectsQueries for D {
    fn job_script_effects(&self, job_id: u64) -> Result<JobScriptEffects, StateError> {
        self.warm_script_effects()?;
        Ok(self
            .script_runtime()
            .effects_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(|cache| cache.get(&job_id))
            .cloned()
            .unwrap_or_default())
    }

    fn save_job_script_effects(
        &self,
        job_id: u64,
        effects: &JobScriptEffects,
    ) -> Result<JobScriptEffects, StateError> {
        let datastore = self.datastore();
        let state = serde_json::to_string(effects)
            .map_err(|error| StateError::Database(error.to_string()))?;
        let _writer = self
            .script_runtime()
            .effects_writer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let saved = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "apply_script_directive", |tx| {
                let state = state.clone();
                Box::pin(async move {
                    tx.execute("UPDATE script_output_state SET next_seq = next_seq WHERE singleton = 1", &[]).await?;
                    let Some(active) = tx.fetch_optional("SELECT metadata FROM active_jobs WHERE job_id = {}", &[SqlArg::I64(job_id as i64)]).await? else {
                        return Err(StateError::Database("script job is no longer active".into()));
                    };
                    let mut current: JobScriptEffects = tx.fetch_optional("SELECT state FROM script_job_state WHERE job_id = {}", &[SqlArg::I64(job_id as i64)]).await?
                        .map(|row| serde_json::from_str(&row.text("state")?).map_err(|error| StateError::Database(error.to_string()))).transpose()?.unwrap_or_default();
                    let change: JobScriptEffects = serde_json::from_str(&state).map_err(|error| StateError::Database(error.to_string()))?;
                    current.parameters.extend(change.parameters);
                    super::directives::validate_parameter_size(current.parameters.iter().map(|(name, value)| (name.as_str(), value.as_str()))).map_err(StateError::Database)?;
                    if let Some(directory) = change.directory {
                        current.directory = Some(directory);
                        current.final_directory = None;
                    }
                    current.final_directory = change.final_directory.or(current.final_directory);
                    current.marked_bad |= change.marked_bad;
                    let mut metadata: Vec<(String, String)> = active.opt_text("metadata")?.map(|value| serde_json::from_str(&value).map_err(|error| StateError::Database(error.to_string()))).transpose()?.unwrap_or_default();
                    current.merge_parameters(&mut metadata);
                    super::directives::validate_parameter_size(metadata.iter().map(|(name, value)| (name.as_str(), value.as_str()))).map_err(StateError::Database)?;
                    tx.execute("UPDATE active_jobs SET metadata = {} WHERE job_id = {}", &[SqlArg::Text(serde_json::to_string(&metadata).map_err(|error| StateError::Database(error.to_string()))?), SqlArg::I64(job_id as i64)]).await?;
                    if let Some(directory) = &current.directory {
                        tx.execute("UPDATE active_jobs SET output_dir = {} WHERE job_id = {}", &[SqlArg::Text(directory.to_string_lossy().into_owned()), SqlArg::I64(job_id as i64)]).await?;
                    }
                    tx.execute("INSERT INTO script_job_state (job_id, state) VALUES ({}, {}) ON CONFLICT(job_id) DO UPDATE SET state = excluded.state", &[SqlArg::I64(job_id as i64), SqlArg::Text(serde_json::to_string(&current).map_err(|error| StateError::Database(error.to_string()))?)]).await?;
                    Ok(current)
                })
            }).await
        })?;
        if let Some(cache) = self
            .script_runtime()
            .effects_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            cache.insert(job_id, saved.clone());
        }
        Ok(saved)
    }
}

pub trait PostProcessingEffectsInternalQueries {
    fn job_script_effects_in_hand(&self, job_id: u64) -> Option<JobScriptEffects>;
    fn warm_script_effects(&self) -> Result<(), StateError>;
    fn lock_script_effects_writer(&self) -> std::sync::MutexGuard<'_, ()>;
    fn cache_script_effects(&self, job_id: u64, effects: JobScriptEffects);
    fn forget_script_effects(&self, job_id: u64);
    fn forget_script_effects_for(&self, job_ids: &[u64]);
}

impl<D: PostprocDatabase> PostProcessingEffectsInternalQueries for D {
    // The same, from memory alone: none while the effects were never read,
    // so a caller that must not read the database asks this.
    fn job_script_effects_in_hand(&self, job_id: u64) -> Option<JobScriptEffects> {
        self.script_runtime()
            .effects_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(|cache| cache.get(&job_id).cloned().unwrap_or_default())
    }

    fn warm_script_effects(&self) -> Result<(), StateError> {
        if self
            .script_runtime()
            .effects_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
        {
            return Ok(());
        }
        let _writer = self
            .script_runtime()
            .effects_writer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self
            .script_runtime()
            .effects_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
        {
            return Ok(());
        }
        let datastore = self.datastore();
        let cache = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, state FROM script_job_state",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| {
                Ok((
                    row.i64("job_id")? as u64,
                    serde_json::from_str(&row.text("state")?)
                        .map_err(|error| StateError::Database(error.to_string()))?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, StateError>>()
        })?;
        *self
            .script_runtime()
            .effects_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(cache);
        Ok(())
    }

    // Serialises writes to `script_job_state` with the effects cache.
    fn lock_script_effects_writer(&self) -> std::sync::MutexGuard<'_, ()> {
        self.script_runtime()
            .effects_writer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    // Callers hold `lock_script_effects_writer` across the committed write.
    fn cache_script_effects(&self, job_id: u64, effects: JobScriptEffects) {
        if let Some(cache) = self
            .script_runtime()
            .effects_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            cache.insert(job_id, effects);
        }
    }

    fn forget_script_effects(&self, job_id: u64) {
        self.forget_script_effects_for(&[job_id]);
    }

    fn forget_script_effects_for(&self, job_ids: &[u64]) {
        let _writer = self
            .script_runtime()
            .effects_writer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(cache) = self
            .script_runtime()
            .effects_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            for job_id in job_ids {
                cache.remove(job_id);
            }
        }
    }
}

pub fn apply_job_directive<D: PostprocDatabase>(
    db: &D,
    context: &mut JobExecutionContext,
    directive: Directive,
) -> Result<(), String> {
    apply_persisted_directive(db, context, directive, false)
}

pub fn apply_queue_directive<D: PostprocDatabase>(
    db: &D,
    context: &mut JobExecutionContext,
    directive: Directive,
) -> Result<(), String> {
    apply_persisted_directive(db, context, directive, true)
}

fn apply_persisted_directive<D: PostprocDatabase>(
    db: &D,
    context: &mut JobExecutionContext,
    directive: Directive,
    final_destination: bool,
) -> Result<(), String> {
    let mut effects = JobScriptEffects::default();
    match directive {
        Directive::Parameter { name, value } => {
            effects.parameters.insert(name, value);
        }
        Directive::MarkBad => effects.marked_bad = true,
        Directive::Directory(path) => {
            effects.directory = Some(validate_postprocessing_directory(
                db,
                context,
                Path::new(&path),
            )?)
        }
        Directive::FinalDirectory(path) => {
            effects.final_directory = Some(if final_destination {
                validate_directory(db, context, Path::new(&path))?
            } else {
                validate_postprocessing_directory(db, context, Path::new(&path))?
            })
        }
        _ => return Err("command is not valid for a persisted job".into()),
    }
    let effects = db
        .save_job_script_effects(context.job_id, &effects)
        .map_err(|error| error.to_string())?;
    db.notify_script_effects(context.job_id);
    effects.apply_to_context(context);
    Ok(())
}

fn configured_destination_root(path: &Path) -> Result<PathBuf, String> {
    let mut prefix = std::path::absolute(path).map_err(|error| error.to_string())?;
    let mut suffix = Vec::new();
    loop {
        match prefix.canonicalize() {
            Ok(mut resolved) => {
                for component in suffix.into_iter().rev() {
                    if component == ".." {
                        resolved.pop();
                    } else {
                        resolved.push(component);
                    }
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(component) = prefix.components().next_back() else {
                    return Err(error.to_string());
                };
                if !matches!(
                    component,
                    std::path::Component::Normal(_) | std::path::Component::ParentDir
                ) {
                    return Err(error.to_string());
                }
                suffix.push(component.as_os_str().to_os_string());
                prefix.pop();
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

pub fn validate_directory<D: PostprocDatabase>(
    db: &D,
    context: &JobExecutionContext,
    path: &Path,
) -> Result<PathBuf, String> {
    validate_directory_owner(db, context, path, false)
}

pub fn validate_postprocessing_directory<D: PostprocDatabase>(
    db: &D,
    context: &JobExecutionContext,
    path: &Path,
) -> Result<PathBuf, String> {
    let relocated = weaver_server_jobs::jobs::working_dir::is_relocated_output_dir(
        path,
        &context.working_directory,
        weaver_server_base::jobs::ids::JobId(context.job_id),
    );
    let path = validate_directory_owner(db, context, path, relocated)?;
    if context.working_directory.canonicalize().ok().as_ref() != Some(&path) && !relocated {
        return Err(
            "post-processing directory must be this job's current or relocated owned output".into(),
        );
    }
    if relocated {
        weaver_server_jobs::jobs::working_dir::mark_weaver_owned_output_dir(
            &path,
            weaver_server_base::jobs::ids::JobId(context.job_id),
        )
        .map_err(|error| error.to_string())?;
    }
    Ok(path)
}

fn validate_directory_owner<D: PostprocDatabase>(
    db: &D,
    context: &JobExecutionContext,
    path: &Path,
    relocated: bool,
) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err("script directory must be absolute".into());
    }
    let path = path
        .canonicalize()
        .map_err(|_| "script directory must exist")?;
    if !path.is_dir() {
        return Err("script directory is not a directory".into());
    }
    let mut roots: Vec<PathBuf> = context
        .compatibility
        .complete_dir
        .as_ref()
        .map(|root| configured_destination_root(root))
        .transpose()?
        .into_iter()
        .collect();
    roots.extend(
        db.list_categories()
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter_map(|category| {
                category.dest_dir.map(PathBuf::from).or_else(|| {
                    context
                        .compatibility
                        .complete_dir
                        .as_ref()
                        .map(|root| root.join(category.name))
                })
            })
            .map(|root| configured_destination_root(&root))
            .collect::<Result<Vec<_>, _>>()?,
    );
    if roots.iter().any(|root| root.starts_with(&path)) {
        return Err("script directory contains a configured complete or category root".into());
    }
    let root = roots
        .iter()
        .find(|root| path.starts_with(root))
        .ok_or("script directory is outside the complete and category roots")?;
    let current = context.working_directory.canonicalize().ok();
    for ancestor in path.ancestors().take_while(|ancestor| *ancestor != root) {
        weaver_server_jobs::jobs::working_dir::check_script_directory_owner(
            ancestor,
            weaver_server_base::jobs::ids::JobId(context.job_id),
        )
        .map_err(|error| error.to_string())?;
        if ancestor
            .join(weaver_server_jobs::jobs::working_dir::OUTPUT_DIR_MARKER)
            .exists()
            && current.as_deref() != Some(ancestor)
            && !(relocated && ancestor == path)
        {
            return Err("script directory has an existing output ownership marker".into());
        }
    }
    // Include ancestors: a subdirectory of another job is not an unowned target.
    let datastore = db.datastore();
    let job_id = context.job_id as i64;
    let foreign = db.run_sql_blocking_read(async move {
        SqlRuntime::fetch_all(datastore.read_exec(), "SELECT output_dir FROM active_jobs WHERE job_id <> {} UNION ALL SELECT output_dir FROM job_history WHERE job_id <> {}", &[SqlArg::I64(job_id), SqlArg::I64(job_id)]).await?
            .into_iter().map(|row| row.opt_text("output_dir")).collect::<Result<Vec<_>, _>>()
    }).map_err(|error| error.to_string())?;
    for owned in foreign.into_iter().flatten() {
        if let Ok(owned) = Path::new(&owned).canonicalize()
            && (path.starts_with(&owned) || owned.starts_with(&path))
        {
            return Err("script directory belongs to another job".into());
        }
    }
    Ok(path)
}
