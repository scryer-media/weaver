use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use super::events::{EventContext, has_subscriber, run_event};
use super::model::ScriptEventLabel;
use super::runner::CompatibilityFacts;
use crate::Database;
use crate::ingest::{SubmissionOptions, SubmitNzbError};
use crate::settings::SharedConfig;

const MAX_SCAN_INPUT_BYTES: u64 = 256 * 1024 * 1024;
static SCAN_SCRATCH_ADMISSION: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
pub const ADD_TO_TOP_KEY: &str = "weaver.submission.add_to_top";
pub const SOURCE_URL_KEY: &str = "weaver.submission.source_url";

pub(crate) fn numeric_priority(value: &str) -> i64 {
    value
        .parse()
        .unwrap_or_else(|_| match value.to_ascii_uppercase().as_str() {
            "HIGH" => 50,
            "LOW" => -50,
            _ => 0,
        })
}

fn context(category: Option<String>, parameters: Vec<(String, String)>) -> EventContext {
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
pub async fn enabled(
    db: &Database,
    category: Option<&str>,
    metadata: &[(String, String)],
) -> Result<bool, crate::StateError> {
    let db = db.clone();
    let context = context(category.map(str::to_string), metadata.to_vec());
    tokio::task::spawn_blocking(move || has_subscriber(&db, &context))
        .await
        .map_err(|error| crate::StateError::Database(error.to_string()))?
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
pub struct ScanScratch(Arc<ScanScratchFiles>);

struct ScanScratchFiles {
    directory: tempfile::TempDir,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ScanScratch {
    fn path(&self) -> &std::path::Path {
        self.0.directory.path()
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn scan_reader<R: Read + Send + 'static>(
    db: &Database,
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

fn apply_scan_environment_changes(
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
        options.semantic_duplicate = crate::SemanticDuplicate::from_source(key, score);
    }
    if let Some(mode) = changed
        .get("NZBNP_DUPEMODE")
        .and_then(|mode| crate::DuplicateMode::from_persisted(mode))
    {
        options.duplicate_mode = mode;
    }
}

fn copy_scan_input(source: impl Read, target: &mut impl Write, limit: u64) -> std::io::Result<()> {
    let length = std::io::copy(&mut source.take(limit + 1), target)?;
    if length > limit {
        return Err(std::io::Error::other("NZB exceeds scan input limit"));
    }
    Ok(())
}

fn scan_run_incomplete(result: &super::model::ScriptResult) -> bool {
    use super::model::ScriptStatus;
    matches!(
        result.status,
        ScriptStatus::Cancelled | ScriptStatus::TimedOut | ScriptStatus::Interrupted
    ) || (result.status == ScriptStatus::Failed && result.exit_code.is_none())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_only_applies_changed_environment_fields() {
        let mut context = context(None, vec![("priority".into(), "50".into())]);
        context.env = [
            ("NZBNP_PRIORITY".into(), "50".into()),
            ("NZBNP_TOP".into(), "0".into()),
            ("NZBNP_PAUSED".into(), "0".into()),
            ("NZBNP_DUPEKEY".into(), String::new()),
            ("NZBNP_DUPESCORE".into(), "0".into()),
            ("NZBNP_DUPEMODE".into(), "SCORE".into()),
        ]
        .into();
        let initial_env = context.env.clone();
        let mut options = SubmissionOptions::default();
        let original_options = options.clone();

        apply_scan_environment_changes(&mut context, &initial_env, &mut options);
        assert_eq!(
            context.facts.parameters,
            vec![("priority".into(), "50".into())]
        );
        assert_eq!(options, original_options);

        context.env.insert("NZBNP_PRIORITY".into(), "-50".into());
        context.env.insert("NZBNP_PAUSED".into(), "1".into());
        apply_scan_environment_changes(&mut context, &initial_env, &mut options);
        assert_eq!(
            context.facts.parameters,
            vec![("priority".into(), "LOW".into())]
        );
        assert!(options.add_paused);
    }

    #[test]
    fn compressed_scan_input_stops_at_the_decoded_limit() {
        let compressed = zstd::stream::encode_all(&vec![b'x'; 4096][..], 3).unwrap();
        let source = zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).unwrap();
        let mut written = Vec::new();
        let error = copy_scan_input(source, &mut written, 32).unwrap_err();
        assert!(error.to_string().contains("scan input limit"));
        assert_eq!(written.len(), 33);
        let mut written = Vec::new();
        copy_scan_input(&b"1234"[..], &mut written, 4).unwrap();
        assert_eq!(written, b"1234");
    }

    #[test]
    fn scan_rejects_incomplete_runs_but_ignores_completed_exit_codes() {
        use super::super::model::{ScriptAdapter, ScriptName, ScriptResult, ScriptStatus};
        let mut result = ScriptResult {
            script: ScriptName::new("scan.py").unwrap(),
            instance_id: None,
            instance_name: None,
            event: ScriptEventLabel::Scan,
            output_id: None,
            background: false,
            adapter: ScriptAdapter::Nzbget,
            status: ScriptStatus::Failed,
            exit_code: None,
            duration_ms: 0,
            output_tail: String::new(),
            output_truncated: false,
            error_message: None,
            finished_at_epoch_ms: 0,
        };
        assert!(scan_run_incomplete(&result));
        result.status = ScriptStatus::TimedOut;
        result.exit_code = Some(137);
        assert!(scan_run_incomplete(&result));
        result.status = ScriptStatus::Cancelled;
        assert!(scan_run_incomplete(&result));
        result.status = ScriptStatus::Succeeded;
        result.exit_code = Some(17);
        assert!(!scan_run_incomplete(&result));
    }

    #[tokio::test]
    async fn scan_reader_rejects_input_when_execution_cannot_start() {
        use super::super::instances::{InstanceTrigger, ScriptInstanceDraft};
        use super::super::model::{PostProcessingSettings, ScriptName};
        let db = Database::open_in_memory().unwrap();
        let data = tempfile::tempdir().unwrap();
        let scripts = db
            .initialize_post_processing_script_directory(data.path(), None)
            .unwrap();
        std::fs::write(scripts.join("scan.py"), "### NZBGET SCAN SCRIPT ###\n").unwrap();
        db.save_post_processing_settings(&PostProcessingSettings {
            execution_enabled: true,
            ..Default::default()
        })
        .unwrap();
        db.create_script_instance(ScriptInstanceDraft::new(
            ScriptName::new("scan.py").unwrap(),
            InstanceTrigger::Scan,
        ))
        .unwrap();
        let selected =
            super::super::listing::resolve_script(&scripts, &ScriptName::new("scan.py").unwrap())
                .unwrap();
        assert!(
            selected
                .manifest
                .kinds()
                .contains(&super::super::model::ScriptKind::Scan)
        );
        let metadata = vec![(
            SOURCE_URL_KEY.into(),
            "https://source.test/invalid\0nzb".into(),
        )];
        assert!(enabled(&db, None, &metadata).await.unwrap());
        let mut config = db.load_config().unwrap();
        config.data_dir = data.path().to_string_lossy().into_owned();
        let config = std::sync::Arc::new(tokio::sync::RwLock::new(config));
        // A malformed internal URL makes NZBNP_URL construction fail before
        // spawn. Unlike invalid NZBPR parameters, required event inputs cannot
        // be omitted. A configured scan must reject this infrastructure failure.
        let result = scan_reader(
            &db,
            &config,
            std::io::Cursor::new(b"unscanned input"),
            Some("input.nzb".into()),
            None,
            None,
            metadata,
            Default::default(),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("unexecuted scan accepted the input"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("scan script scan.py did not complete: failed")
        );
    }

    #[tokio::test]
    async fn scratch_admission_is_held_until_directory_cleanup() {
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_path_buf();
        let scratch = ScanScratch(Arc::new(ScanScratchFiles {
            directory,
            _permit: admission.clone().acquire_owned().await.unwrap(),
        }));
        let pending = admission.acquire_owned();
        tokio::pin!(pending);
        tokio::select! {
            biased;
            _ = &mut pending => panic!("a retained scratch directory must occupy admission"),
            () = std::future::ready(()) => {},
        }
        assert!(path.is_dir());
        drop(scratch);
        let _permit = pending.await.unwrap();
        assert!(!path.exists());
    }
}
