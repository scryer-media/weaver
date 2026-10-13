use std::path::PathBuf;

use super::events::{EventContext, wake_queue};
use super::model::{PipelineOutcome, QueueEvent};
use super::runner::{CompatibilityFacts, JobExecutionContext};
use crate::{Database, JobId, StateError};

fn context(
    db: &Database,
    category: Option<String>,
    event: QueueEvent,
) -> Result<EventContext, StateError> {
    let config = db.load_config()?;
    Ok(EventContext::from_job(
        &JobExecutionContext {
            job_id: 0,
            name: String::new(),
            nzb_filename: String::new(),
            category,
            group: None,
            source_url: None,
            working_directory: PathBuf::from(&config.data_dir),
            final_directory: PathBuf::from(&config.data_dir),
            pipeline_outcome: PipelineOutcome::Succeeded,
            par_status: 0,
            unpack_status: 0,
            compatibility: CompatibilityFacts {
                data_dir: Some(PathBuf::from(&config.data_dir)),
                complete_dir: Some(PathBuf::from(config.complete_dir())),
                intermediate_dir: Some(PathBuf::from(config.intermediate_dir())),
                ..Default::default()
            },
        },
        event,
    ))
}

// How a URL submission ended, as reported to URL_COMPLETED scripts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UrlStatus {
    // The NZB was fetched and accepted into the queue.
    Success,
    // The NZB could not be fetched.
    Failure,
    // The NZB was fetched but rejected on submission.
    ScanFailure,
}

impl UrlStatus {
    fn as_env(self) -> &'static str {
        match self {
            Self::Success => "SUCCESS",
            Self::Failure => "FAILURE",
            Self::ScanFailure => "SCAN_FAILURE",
        }
    }
}

pub async fn url_completed(
    db: &Database,
    url: &str,
    category: Option<&str>,
    status: UrlStatus,
) -> Result<(), StateError> {
    let worker_db = db.clone();
    let category = category.map(str::to_string);
    let url = url.to_string();
    let admitted = tokio::task::spawn_blocking(move || {
        let mut context = context(&worker_db, category, QueueEvent::UrlCompleted)?;
        context.job_id = None;
        context.env.insert("NZBNA_URL".into(), url);
        context
            .env
            .insert("NZBNA_URLSTATUS".into(), status.as_env().into());
        Ok::<_, StateError>(
            worker_db
                .enqueue_script_event(&context, chrono::Utc::now().timestamp_millis())?
                .is_some(),
        )
    })
    .await
    .map_err(|error| StateError::Database(error.to_string()))??;
    if admitted {
        wake_queue(db.clone());
    }
    Ok(())
}

pub fn mark_history_good(db: &Database, job_id: JobId) -> Result<bool, StateError> {
    mark_history(db, job_id, crate::history::NzbgetMarkStatus::Good)
}

// Record how a client marked a history row and raise NZB_MARKED. GOOD and BAD
// also settle the row's duplicate candidate, as the web UI's marks do.
pub fn mark_history(
    db: &Database,
    job_id: JobId,
    mark: crate::history::NzbgetMarkStatus,
) -> Result<bool, StateError> {
    let changed = match mark {
        crate::history::NzbgetMarkStatus::Good => db.mark_semantic_candidate_good(job_id)?,
        crate::history::NzbgetMarkStatus::Bad => db.mark_semantic_candidate_bad(job_id)?,
        crate::history::NzbgetMarkStatus::Success => false,
    };
    db.set_nzbget_history_mark(job_id.0, mark)?;
    let Some(history) = db.get_job_history(job_id.0)? else {
        return Ok(changed);
    };
    let mut context = context(db, history.category, QueueEvent::NzbMarked)?;
    context.job_id = Some(job_id.0);
    context
        .env
        .insert("NZBNA_NZBID".into(), job_id.0.to_string());
    context
        .env
        .insert("NZBNA_LASTID".into(), job_id.0.to_string());
    context.env.insert("NZBNA_NZBNAME".into(), history.name);
    context
        .env
        .insert("NZBNA_MARKSTATUS".into(), mark.as_str().into());
    if let Some(path) = history.output_dir {
        context.env.insert("NZBNA_DIRECTORY".into(), path.clone());
        if std::path::Path::new(&path).is_dir() {
            context.cwd = path.into();
        }
    }
    context.facts.parameters = history
        .metadata
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))?
        .unwrap_or_default();
    if db
        .enqueue_script_event(&context, chrono::Utc::now().timestamp_millis())?
        .is_some()
    {
        wake_queue(db.clone());
    }
    Ok(changed)
}
