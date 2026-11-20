// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::post_processing::database::PostprocDatabase;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use super::events::{EventContext, has_subscriber, run_event};
use super::model::ScriptEventLabel;
use super::runner::CompatibilityFacts;
use weaver_server_ingest::ingest::submission_options::{SubmissionOptions, SubmitNzbError};
use weaver_server_settings::settings::model::SharedConfig;

const MAX_SCAN_INPUT_BYTES: u64 = 256 * 1024 * 1024;
static SCAN_SCRATCH_ADMISSION: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
pub const ADD_TO_TOP_KEY: &str = "weaver.submission.add_to_top";
pub const SOURCE_URL_KEY: &str = "weaver.submission.source_url";

// The URL a job was submitted from, as its submission recorded it.
pub fn source_url(metadata: &[(String, String)]) -> Option<String> {
    metadata
        .iter()
        .find(|(key, _)| key == SOURCE_URL_KEY)
        .map(|(_, value)| value.clone())
}

pub fn numeric_priority(value: &str) -> i64 {
    value
        .parse()
        .unwrap_or_else(|_| match value.to_ascii_uppercase().as_str() {
            "HIGH" => 50,
            "LOW" => -50,
            _ => 0,
        })
}

pub fn context(category: Option<String>, parameters: Vec<(String, String)>) -> EventContext {
    EventContext {
        job_id: None,
        event: ScriptEventLabel::Scan,
        category,
        cwd: PathBuf::new(),
        env: BTreeMap::new(),
        facts: CompatibilityFacts {
            parameters,
            ..Default::default()
        },
        instances: None,
        scratch: None,
    }
}

// Whether a scan instance would run for an incoming NZB.
pub async fn enabled<D: PostprocDatabase>(
    db: &D,
    category: Option<&str>,
    metadata: &[(String, String)],
) -> Result<bool, weaver_server_db::persistence::StateError> {
    let db = db.clone();
    let context = context(category.map(str::to_string), metadata.to_vec());
    tokio::task::spawn_blocking(move || has_subscriber(&db, &context))
        .await
        .map_err(|error| weaver_server_db::persistence::StateError::Database(error.to_string()))?
}

pub struct ScannedSubmission {
    pub source: std::fs::File,
    pub scratch: ScanScratch,
    pub filename: Option<String>,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
    pub options: SubmissionOptions,
}

// The directory a scan works in. It goes, and another scan is admitted, once
// the import and every script still reading it have let go, including on
// cancellation.
#[derive(Clone)]
pub struct ScanScratch(pub Arc<ScanScratchFiles>);

pub struct ScanScratchFiles {
    pub directory: tempfile::TempDir,
    pub _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ScanScratch {
    fn path(&self) -> &std::path::Path {
        self.0.directory.path()
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn scan_reader<R: Read + Send + 'static, D: PostprocDatabase>(
    db: &D,
    config: &SharedConfig,
    source: R,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    mut options: SubmissionOptions,
) -> Result<ScannedSubmission, SubmitNzbError> {
    let mut context = context(category, metadata);
    {
        let config = config.read().await;
        context.facts.data_dir = Some(PathBuf::from(&config.data_dir));
        context.facts.complete_dir = Some(PathBuf::from(config.complete_dir()));
        context.facts.intermediate_dir = Some(PathBuf::from(config.intermediate_dir()));
    }
    context.facts.password = password;
    let data_dir = context
        .facts
        .data_dir
        .clone()
        .expect("configured data directory");
    let permit = SCAN_SCRATCH_ADMISSION
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(8)))
        .clone()
        .acquire_owned()
        .await
        .expect("scan scratch admission is never closed");
    let scratch = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&data_dir)?;
        let directory = tempfile::Builder::new()
            .prefix("script-scan-")
            .tempdir_in(data_dir)?;
        let scratch = ScanScratch(Arc::new(ScanScratchFiles {
            directory,
            _permit: permit,
        }));
        let mut file = std::fs::File::create(scratch.path().join("input.nzb"))?;
        copy_scan_input(source, &mut file, MAX_SCAN_INPUT_BYTES)?;
        file.flush()?;
        Ok::<_, std::io::Error>(scratch)
    })
    .await
    .map_err(|error| SubmitNzbError::Save(std::io::Error::other(error)))?
    .map_err(SubmitNzbError::Save)?;
    let input = scratch.path().join("input.nzb");
    context.cwd = scratch.path().to_path_buf();
    context.scratch = Some(scratch.0.clone());
    let parameter = |key: &str, fallback: &str| {
        context
            .facts
            .parameters
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| fallback.into())
    };
    context.env = [
        (
            "NZBNP_DIRECTORY",
            scratch.path().to_string_lossy().into_owned(),
        ),
        ("NZBNP_FILENAME", input.to_string_lossy().into_owned()),
        ("NZBNP_NZBNAME", filename.clone().unwrap_or_default()),
        ("NZBNP_URL", parameter(SOURCE_URL_KEY, "")),
        (
            "NZBNP_CATEGORY",
            context.category.clone().unwrap_or_default(),
        ),
        (
            "NZBNP_PRIORITY",
            numeric_priority(&parameter("priority", "0")).to_string(),
        ),
        ("NZBNP_TOP", parameter(ADD_TO_TOP_KEY, "0")),
        ("NZBNP_PAUSED", i32::from(options.add_paused).to_string()),
        (
            "NZBNP_DUPEKEY",
            parameter(
                "nzbget.dupe_key",
                options
                    .semantic_duplicate
                    .as_ref()
                    .map_or("", |duplicate| duplicate.normalized_key.as_str()),
            ),
        ),
        (
            "NZBNP_DUPESCORE",
            parameter(
                "nzbget.dupe_score",
                &options
                    .semantic_duplicate
                    .as_ref()
                    .map_or(0, |duplicate| duplicate.score)
                    .to_string(),
            ),
        ),
        (
            "NZBNP_DUPEMODE",
            parameter("nzbget.dupe_mode", options.duplicate_mode.as_str()).to_ascii_uppercase(),
        ),
    ]
    .into_iter()
    .map(|(key, value)| (key.into(), value))
    .collect();
    let initial_env = context.env.clone();
    let run_id = scratch
        .path()
        .file_name()
        .expect("scratch name")
        .to_string_lossy()
        .into_owned();
    let results = run_event(db, &mut context, &run_id, None, None).await?;
    if let Some(result) = results.iter().find(|result| scan_run_incomplete(result)) {
        return Err(SubmitNzbError::Save(std::io::Error::other(format!(
            "scan script {} did not complete: {}",
            result.label(),
            result.status.as_str(),
        ))));
    }
    let source = std::fs::File::open(&input).map_err(|error| {
        SubmitNzbError::Save(if error.kind() == std::io::ErrorKind::NotFound {
            tracing::warn!("NZB removed by scan script");
            std::io::Error::other("removed by scan script")
        } else {
            error
        })
    })?;
    if source.metadata().map_err(SubmitNzbError::Save)?.len() > MAX_SCAN_INPUT_BYTES {
        return Err(SubmitNzbError::Save(std::io::Error::other(
            "rewritten NZB exceeds scan input limit",
        )));
    }
    apply_scan_environment_changes(&mut context, &initial_env, &mut options);
    Ok(ScannedSubmission {
        source,
        scratch,
        filename: context
            .env
            .remove("NZBNP_NZBNAME")
            .filter(|name| !name.is_empty())
            .or(filename),
        category: context.category,
        metadata: context.facts.parameters,
        options,
    })
}

pub fn apply_scan_environment_changes(
    context: &mut EventContext,
    initial_env: &BTreeMap<String, String>,
    options: &mut SubmissionOptions,
) {
    let changed = context
        .env
        .iter()
        .filter(|(key, value)| initial_env.get(*key) != Some(*value))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    if let Some(value) = changed.get("NZBNP_PAUSED") {
        options.add_paused = value == "1";
    }
    for (env, key) in [
        ("NZBNP_PRIORITY", "priority"),
        ("NZBNP_TOP", ADD_TO_TOP_KEY),
        ("NZBNP_DUPEKEY", "nzbget.dupe_key"),
        ("NZBNP_DUPESCORE", "nzbget.dupe_score"),
        ("NZBNP_DUPEMODE", "nzbget.dupe_mode"),
    ] {
        if let Some(value) = changed.get(env) {
            context.facts.parameters.retain(|(name, _)| name != key);
            let value = if key == "priority" {
                match numeric_priority(value).cmp(&0) {
                    std::cmp::Ordering::Less => "LOW",
                    std::cmp::Ordering::Equal => "NORMAL",
                    std::cmp::Ordering::Greater => "HIGH",
                }
                .to_string()
            } else {
                value.clone()
            };
            context.facts.parameters.push((key.into(), value));
        }
    }
    if changed.contains_key("NZBNP_DUPEKEY") || changed.contains_key("NZBNP_DUPESCORE") {
        let key = context
            .env
            .get("NZBNP_DUPEKEY")
            .map(String::as_str)
            .unwrap_or("");
        let score = context
            .env
            .get("NZBNP_DUPESCORE")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        options.semantic_duplicate =
            weaver_server_jobs::jobs::duplicate::SemanticDuplicate::from_source(key, score);
    }
    if let Some(mode) = changed
        .get("NZBNP_DUPEMODE")
        .and_then(|mode| weaver_server_jobs::jobs::duplicate::DuplicateMode::from_persisted(mode))
    {
        options.duplicate_mode = mode;
    }
}

pub fn copy_scan_input(
    source: impl Read,
    target: &mut impl Write,
    limit: u64,
) -> std::io::Result<()> {
    let length = std::io::copy(&mut source.take(limit + 1), target)?;
    if length > limit {
        return Err(std::io::Error::other("NZB exceeds scan input limit"));
    }
    Ok(())
}

pub fn scan_run_incomplete(result: &super::model::ScriptResult) -> bool {
    use super::model::ScriptStatus;
    matches!(
        result.status,
        ScriptStatus::Cancelled | ScriptStatus::TimedOut | ScriptStatus::Interrupted
    ) || (result.status == ScriptStatus::Failed && result.exit_code.is_none())
}
