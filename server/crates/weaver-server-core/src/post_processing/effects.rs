use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::directives::Directive;
use super::runner::JobExecutionContext;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::persistence::{Database, StateError};

/// Applied directives survive process termination independently of runner results.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobScriptEffects {
    pub parameters: BTreeMap<String, String>,
    pub directory: Option<PathBuf>,
    pub final_directory: Option<PathBuf>,
    pub marked_bad: bool,
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

impl Database {
    pub fn job_script_effects(&self, job_id: u64) -> Result<JobScriptEffects, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT state FROM script_job_state WHERE job_id = {}",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?
            .map(|row| {
                serde_json::from_str(&row.text("state")?)
                    .map_err(|error| StateError::Database(error.to_string()))
            })
            .transpose()
            .map(Option::unwrap_or_default)
        })
    }

    pub fn save_job_script_effects(
        &self,
        job_id: u64,
        effects: &JobScriptEffects,
    ) -> Result<JobScriptEffects, StateError> {
        let datastore = self.datastore();
        let state = serde_json::to_string(effects)
            .map_err(|error| StateError::Database(error.to_string()))?;
        self.run_sql_blocking(async move {
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
                    current.directory = change.directory.or(current.directory);
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
        })
    }
}

pub fn apply_job_directive(
    db: &Database,
    context: &mut JobExecutionContext,
    directive: Directive,
) -> Result<(), String> {
    apply_persisted_directive(db, context, directive, false)
}

pub(crate) fn apply_queue_directive(
    db: &Database,
    context: &mut JobExecutionContext,
    directive: Directive,
) -> Result<(), String> {
    apply_persisted_directive(db, context, directive, true)
}

fn apply_persisted_directive(
    db: &Database,
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

pub(crate) fn validate_directory(
    db: &Database,
    context: &JobExecutionContext,
    path: &Path,
) -> Result<PathBuf, String> {
    validate_directory_owner(db, context, path, false)
}

fn validate_postprocessing_directory(
    db: &Database,
    context: &JobExecutionContext,
    path: &Path,
) -> Result<PathBuf, String> {
    let relocated =
        crate::jobs::working_dir::is_relocated_output_dir(path, &context.working_directory);
    let path = validate_directory_owner(db, context, path, relocated)?;
    if context.working_directory.canonicalize().ok().as_ref() != Some(&path) && !relocated {
        return Err(
            "post-processing directory must be this job's current or relocated owned output".into(),
        );
    }
    if relocated {
        crate::jobs::working_dir::mark_weaver_owned_output_dir(&path)
            .map_err(|error| error.to_string())?;
    }
    Ok(path)
}

fn validate_directory_owner(
    db: &Database,
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
        crate::jobs::working_dir::check_script_directory_owner(
            ancestor,
            crate::JobId(context.job_id),
        )
        .map_err(|error| error.to_string())?;
        if ancestor
            .join(crate::jobs::working_dir::OUTPUT_DIR_MARKER)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::categories::CategoryConfig;
    use crate::post_processing::model::PipelineOutcome;
    use crate::post_processing::runner::CompatibilityFacts;

    #[test]
    fn directory_directives_cannot_claim_shared_destination_roots() {
        let data = tempfile::tempdir().unwrap();
        let complete = data.path().join("complete");
        let category = complete.join("shared").join("tv");
        let default_category = complete.join("movies");
        let future_parent = complete.join("future");
        std::fs::create_dir_all(&future_parent).unwrap();
        let job_output = category.join("job");
        std::fs::create_dir_all(&job_output).unwrap();
        std::fs::create_dir_all(&default_category).unwrap();
        let db = Database::open_in_memory().unwrap();
        for category in [
            CategoryConfig {
                id: 1,
                name: "tv".into(),
                dest_dir: Some(category.to_string_lossy().into_owned()),
                aliases: String::new(),
            },
            CategoryConfig {
                id: 2,
                name: "movies".into(),
                dest_dir: None,
                aliases: String::new(),
            },
            CategoryConfig {
                id: 3,
                name: "future".into(),
                dest_dir: Some(future_parent.join("tv").to_string_lossy().into_owned()),
                aliases: String::new(),
            },
        ] {
            db.insert_category(&category).unwrap();
        }
        let context = JobExecutionContext {
            job_id: 1,
            name: "job".into(),
            nzb_filename: "job.nzb".into(),
            category: Some("tv".into()),
            group: None,
            source_url: None,
            working_directory: data.path().into(),
            final_directory: data.path().into(),
            pipeline_outcome: PipelineOutcome::Succeeded,
            par_status: 0,
            unpack_status: 0,
            compatibility: CompatibilityFacts {
                complete_dir: Some(complete.clone()),
                ..Default::default()
            },
        };
        for shared in [
            &complete,
            &category,
            &default_category,
            &complete.join("shared"),
            &future_parent,
        ] {
            assert!(
                validate_directory(&db, &context, shared)
                    .unwrap_err()
                    .contains("configured complete or category root"),
                "shared root must never become job-owned: {}",
                shared.display()
            );
        }
        assert_eq!(
            validate_directory(&db, &context, &job_output).unwrap(),
            job_output.canonicalize().unwrap()
        );
        let current = category.join("current-job");
        let relocated = category.join("relocated-job");
        std::fs::create_dir(&current).unwrap();
        std::fs::write(current.join("job.bin"), b"job").unwrap();
        crate::jobs::working_dir::mark_weaver_owned_output_dir(&current).unwrap();
        let mut context = context;
        context.working_directory = current.clone();
        context.final_directory = current.clone();
        std::fs::write(job_output.join("unrelated.bin"), b"unrelated").unwrap();
        for directive in [
            Directive::Directory(job_output.to_string_lossy().into_owned()),
            Directive::FinalDirectory(job_output.to_string_lossy().into_owned()),
        ] {
            assert!(
                apply_job_directive(&db, &mut context, directive)
                    .unwrap_err()
                    .contains("current or relocated owned output")
            );
            assert_eq!(context.working_directory, current);
            assert!(context.compatibility.final_directory_override.is_none());
            assert_eq!(
                std::fs::read(job_output.join("unrelated.bin")).unwrap(),
                b"unrelated"
            );
            assert!(
                !job_output
                    .join(crate::jobs::working_dir::OUTPUT_DIR_MARKER)
                    .exists()
            );
        }
        std::fs::rename(&current, &relocated).unwrap();
        assert_eq!(
            validate_postprocessing_directory(&db, &context, &relocated).unwrap(),
            relocated.canonicalize().unwrap()
        );
        assert!(crate::jobs::working_dir::is_weaver_owned_output_dir(
            &relocated
        ));
        assert_eq!(std::fs::read(relocated.join("job.bin")).unwrap(), b"job");
    }
}
