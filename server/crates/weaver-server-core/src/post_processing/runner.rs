//! Process execution for one script.
//!
//! The SABnzbd and NZBGet environment contracts are the load-bearing asset here:
//! the existing ecosystem of scripts runs unmodified because `SAB_*`,
//! `NZBPP_*`, `NZBPO_*` and `NZBOP_*` are built exactly as those programs build
//! them, and the exit codes are interpreted the same way.

use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{mpsc, watch};

use super::directives::{ScriptOutputEvent, parse_line, valid_parameter_name};
use super::model::{
    OptionValue, PipelineOutcome, ResolvedOption, ScriptAdapter, ScriptEventLabel, ScriptManifest,
};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_TERMINATION_GRACE: Duration = Duration::from_secs(10);
/// A user cancellation must not inherit an arbitrarily long script shutdown grace.
const MAX_USER_CANCELLATION_GRACE: Duration = Duration::from_secs(5);
/// Per-script output retained on the job. Anything beyond this keeps the tail.
pub const MAX_SCRIPT_OUTPUT_BYTES: u64 = 1024 * 1024;
pub const MAX_LOGICAL_LINE_BYTES: usize = 64 * 1024;

const SUPERVISOR_ARG: &str = "__post-processing-supervisor";
const MAX_SUPERVISOR_REQUEST_BYTES: u64 = 2 * 1024 * 1024;
const SUPERVISOR_LAUNCHED: &[u8] = b"weaver-script-launched-v1\n";

fn user_cancellation_grace(grace: Duration) -> Duration {
    grace.min(MAX_USER_CANCELLATION_GRACE)
}

#[derive(Debug, Clone, Default)]
pub struct InterpreterConfig {
    pub python: Option<PathBuf>,
    pub powershell: Option<PathBuf>,
    pub batch: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct JobExecutionContext {
    pub job_id: u64,
    pub name: String,
    pub nzb_filename: String,
    pub category: Option<String>,
    pub group: Option<String>,
    pub source_url: Option<String>,
    pub working_directory: PathBuf,
    pub final_directory: PathBuf,
    pub pipeline_outcome: PipelineOutcome,
    pub par_status: i32,
    pub unpack_status: i32,
    pub compatibility: CompatibilityFacts,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum NzbgetScriptStatus {
    #[default]
    None,
    Failure,
    Success,
}

impl NzbgetScriptStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Failure => "FAILURE",
            Self::Success => "SUCCESS",
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CompatibilityFacts {
    pub parameters: Vec<(String, String)>,
    pub marked_bad: bool,
    pub final_directory_override: Option<PathBuf>,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub health_milli: u32,
    pub critical_health_milli: u32,
    #[serde(skip)]
    pub password: Option<String>,
    pub failure_message: Option<String>,
    pub data_dir: Option<PathBuf>,
    pub intermediate_dir: Option<PathBuf>,
    pub complete_dir: Option<PathBuf>,
    pub temp_dir: Option<PathBuf>,
    pub app_dir: Option<PathBuf>,
    pub previous_script_status: NzbgetScriptStatus,
}

#[derive(Debug, Clone)]
pub struct ScriptExecutionRequest {
    pub manifest: ScriptManifest,
    /// Package directory for a manifest package, or the scripts directory for a bare script.
    pub root: PathBuf,
    pub options: Vec<ResolvedOption>,
    pub context: JobExecutionContext,
    pub timeout: Option<Duration>,
    pub termination_grace: Duration,
    pub interpreters: InterpreterConfig,
    #[doc(hidden)]
    pub supervisor_executable: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ExecutionDisposition {
    Succeeded,
    /// NZBGet exit 95: the script decided it had nothing to do.
    Skipped,
    /// A SABnzbd script exited nonzero, which SABnzbd records as a warning.
    Warned,
    Failed,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScriptExecutionResult {
    pub disposition: ExecutionDisposition,
    pub exit_code: Option<i32>,
    /// Captured stdout/stderr, already truncated to the tail budget and redacted.
    pub output: Vec<u8>,
    pub output_truncated: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("script entrypoint is unavailable or unsafe")]
    InvalidEntrypoint,
    #[error("script environment value is invalid")]
    InvalidEnvironment,
    #[error("script timeout is too large for this platform")]
    InvalidTimeout,
    #[error("post-processing supervisor protocol failed: {0}")]
    SupervisorProtocol(String),
    #[error("post-processing process failed: {0}")]
    Io(#[from] io::Error),
}

#[derive(Clone, Serialize, Deserialize)]
struct SupervisorRequest {
    program: PathBuf,
    args: Vec<OsStringWire>,
    env: BTreeMap<OsStringWire, OsStringWire>,
    cwd: PathBuf,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
struct OsStringWire(String);

impl OsStringWire {
    fn from_os(value: impl Into<OsString>) -> Result<Self, RunnerError> {
        value
            .into()
            .into_string()
            .map(Self)
            .map_err(|_| RunnerError::InvalidEnvironment)
    }

    fn into_os(self) -> OsString {
        OsString::from(self.0)
    }
}

/// Run one script to completion, honouring cancellation and the timeout.
pub async fn execute_script(
    request: ScriptExecutionRequest,
    cancellation: Option<watch::Receiver<bool>>,
) -> Result<ScriptExecutionResult, RunnerError> {
    execute_script_observed(request, cancellation, None, MAX_SCRIPT_OUTPUT_BYTES).await
}

pub async fn execute_script_observed(
    request: ScriptExecutionRequest,
    cancellation: Option<watch::Receiver<bool>>,
    events: Option<mpsc::Sender<ScriptOutputEvent>>,
    output_ceiling: u64,
) -> Result<ScriptExecutionResult, RunnerError> {
    let mut secrets = request
        .options
        .iter()
        .filter_map(|option| match option.value() {
            OptionValue::Secret(value) if !value.expose_for_execution().is_empty() => {
                Some(value.expose_for_execution().as_bytes().to_vec())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if let Some(password) = request
        .context
        .compatibility
        .password
        .as_deref()
        .filter(|password| !password.is_empty())
    {
        secrets.push(password.as_bytes().to_vec());
    }
    if let Some(source_url) = request.context.source_url.as_deref() {
        append_source_url_secrets(&mut secrets, source_url);
    }

    let adapter = request.manifest.adapter();
    let display_name = request.manifest.display_name().to_string();
    let grace = if request.termination_grace.is_zero() {
        DEFAULT_TERMINATION_GRACE
    } else {
        request.termination_grace
    };
    if request
        .timeout
        .is_some_and(|duration| Instant::now().checked_add(duration).is_none())
        || Instant::now().checked_add(grace).is_none()
    {
        return Err(RunnerError::InvalidTimeout);
    }

    let mut prepared = prepare_execution(&request)?;
    prepared.capture = CapturePolicy {
        secrets: Arc::new(secrets.clone()),
        event: (adapter == ScriptAdapter::Nzbget).then_some(ScriptEventLabel::PostProcessing),
        events,
        ceiling: output_ceiling.clamp(MAX_LOGICAL_LINE_BYTES as u64, 8 * 1024 * 1024),
    };
    tracing::info!(
        script = %display_name,
        adapter = adapter.as_str(),
        job_id = request.context.job_id,
        timeout_seconds = request.timeout.map(|duration| duration.as_secs()),
        "starting post-processing script"
    );
    let started = Instant::now();
    let mut result = execute_supervised(prepared, request.timeout, grace, cancellation).await;
    if let Ok(result) = result.as_mut() {
        result.output = redact_bytes(&result.output, &secrets);
        if let Some(message) = &mut result.error_message {
            *message = redact_string(message, &secrets);
        }
    }
    match &result {
        Ok(result) => tracing::info!(
            script = %display_name,
            result = ?result.disposition,
            exit_code = result.exit_code,
            duration_ms = started.elapsed().as_millis() as u64,
            output_truncated = result.output_truncated,
            "post-processing script finished"
        ),
        Err(error) => tracing::info!(
            script = %display_name,
            error = %error,
            duration_ms = started.elapsed().as_millis() as u64,
            "post-processing script could not run"
        ),
    }
    result.map_err(|error| redact_runner_error(error, &secrets))
}

/// An invocation without job lifecycle authority. All kinds share this runner.
pub struct ExecutionSpec {
    pub manifest: ScriptManifest,
    pub root: PathBuf,
    pub options: Vec<ResolvedOption>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub argv: Vec<OsString>,
    pub timeout: Option<Duration>,
    pub termination_grace: Duration,
    pub kind: ScriptEventLabel,
    pub run_id: String,
    pub facts: CompatibilityFacts,
    pub interpreters: InterpreterConfig,
    pub supervisor_executable: Option<PathBuf>,
    pub output_ceiling: u64,
}

pub async fn execute_spec(
    spec: ExecutionSpec,
    cancellation: Option<watch::Receiver<bool>>,
    events: Option<mpsc::Sender<ScriptOutputEvent>>,
) -> Result<ScriptExecutionResult, RunnerError> {
    let root = fs::canonicalize(&spec.root)?;
    let entrypoint = fs::canonicalize(root.join(spec.manifest.entrypoint()))?;
    if !entrypoint.starts_with(&root) || !entrypoint.is_file() {
        return Err(RunnerError::InvalidEntrypoint);
    }
    let (program, mut args) = resolve_program(&entrypoint, &spec.interpreters)?;
    args.extend(spec.argv);
    let mut env = sanitized_platform_environment()?;
    insert_nzbget_global_options(&mut env, &spec.facts)?;
    insert_compat_options(&mut env, "NZBPO", &spec.options)?;
    insert_parameters(&mut env, &spec.facts.parameters, &spec.manifest)?;
    let source_url = spec
        .env
        .get("NZBNA_URL")
        .or_else(|| spec.env.get("NZBNP_URL"))
        .cloned();
    for (name, value) in spec.env {
        insert_env(&mut env, &name, &value)?;
    }
    let mut secrets = spec
        .options
        .iter()
        .filter_map(|option| match option.value() {
            OptionValue::Secret(value) if !value.expose_for_execution().is_empty() => {
                Some(value.expose_for_execution().as_bytes().to_vec())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if let Some(password) = spec.facts.password.filter(|value| !value.is_empty()) {
        secrets.push(password.into_bytes());
    }
    if let Some(source_url) = source_url.as_deref() {
        append_source_url_secrets(&mut secrets, source_url);
    }
    let prepared = PreparedExecution {
        supervisor_executable: spec.supervisor_executable,
        supervisor: SupervisorRequest {
            program,
            args: args
                .into_iter()
                .map(OsStringWire::from_os)
                .collect::<Result<_, _>>()?,
            env,
            cwd: fs::canonicalize(spec.cwd)?,
        },
        adapter: spec.manifest.adapter(),
        capture: CapturePolicy {
            secrets: Arc::new(secrets.clone()),
            event: Some(spec.kind.clone()),
            events,
            ceiling: spec
                .output_ceiling
                .clamp(MAX_LOGICAL_LINE_BYTES as u64, 8 * 1024 * 1024),
        },
    };
    tracing::info!(run_id = %spec.run_id, event = %spec.kind, "starting script");
    let mut result = execute_supervised(
        prepared,
        spec.timeout,
        spec.termination_grace.max(Duration::from_millis(1)),
        cancellation,
    )
    .await
    .map_err(|error| redact_runner_error(error, &secrets))?;
    if result.exit_code.is_some()
        && !matches!(
            result.disposition,
            ExecutionDisposition::Cancelled | ExecutionDisposition::TimedOut
        )
    {
        result.disposition = match spec.kind {
            ScriptEventLabel::Feed(_) if result.exit_code != Some(93) => {
                ExecutionDisposition::Failed
            }
            ScriptEventLabel::Feed(_)
            | ScriptEventLabel::Queue(_)
            | ScriptEventLabel::Scan
            | ScriptEventLabel::Scheduler(_) => ExecutionDisposition::Succeeded,
            ScriptEventLabel::PostProcessing => result.disposition,
        };
        if result.disposition == ExecutionDisposition::Succeeded {
            result.error_message = None;
        }
    }
    if let Some(message) = &mut result.error_message {
        *message = redact_string(message, &secrets);
    }
    Ok(result)
}

#[derive(Clone)]
struct CapturePolicy {
    secrets: Arc<Vec<Vec<u8>>>,
    event: Option<ScriptEventLabel>,
    events: Option<mpsc::Sender<ScriptOutputEvent>>,
    ceiling: u64,
}

impl Default for CapturePolicy {
    fn default() -> Self {
        Self {
            secrets: Arc::new(Vec::new()),
            event: None,
            events: None,
            ceiling: MAX_SCRIPT_OUTPUT_BYTES,
        }
    }
}

struct PreparedExecution {
    supervisor_executable: Option<PathBuf>,
    supervisor: SupervisorRequest,
    adapter: ScriptAdapter,
    capture: CapturePolicy,
}

fn prepare_execution(request: &ScriptExecutionRequest) -> Result<PreparedExecution, RunnerError> {
    let root = fs::canonicalize(&request.root)?;
    let entrypoint = fs::canonicalize(root.join(request.manifest.entrypoint()))?;
    if !entrypoint.starts_with(&root) || !entrypoint.is_file() {
        return Err(RunnerError::InvalidEntrypoint);
    }

    let (program, mut args) = resolve_program(&entrypoint, &request.interpreters)?;
    let mut env = sanitized_platform_environment()?;
    let adapter_args = adapter_environment_and_args(request, &mut env)?;
    args.extend(adapter_args);

    let final_directory = fs::canonicalize(&request.context.final_directory)?;

    Ok(PreparedExecution {
        supervisor_executable: request.supervisor_executable.clone(),
        supervisor: SupervisorRequest {
            program,
            args: args
                .into_iter()
                .map(OsStringWire::from_os)
                .collect::<Result<_, _>>()?,
            env,
            cwd: final_directory,
        },
        adapter: request.manifest.adapter(),
        capture: CapturePolicy::default(),
    })
}

fn resolve_program(
    entrypoint: &Path,
    interpreters: &InterpreterConfig,
) -> Result<(PathBuf, Vec<OsString>), RunnerError> {
    let extension = entrypoint
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "py" => Ok((
            interpreters
                .python
                .clone()
                .unwrap_or_else(|| PathBuf::from("python3")),
            vec![entrypoint.as_os_str().to_owned()],
        )),
        "ps1" => Ok((
            interpreters
                .powershell
                .clone()
                .unwrap_or_else(|| PathBuf::from("pwsh")),
            vec![
                OsString::from("-NoProfile"),
                OsString::from("-NonInteractive"),
                OsString::from("-File"),
                entrypoint.as_os_str().to_owned(),
            ],
        )),
        "bat" | "cmd" => {
            let interpreter = interpreters
                .batch
                .clone()
                .or_else(|| std::env::var_os("COMSPEC").map(PathBuf::from))
                .unwrap_or_else(|| PathBuf::from("cmd.exe"));
            Ok((
                interpreter,
                vec![
                    OsString::from("/D"),
                    OsString::from("/S"),
                    OsString::from("/C"),
                    entrypoint.as_os_str().to_owned(),
                ],
            ))
        }
        _ => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if fs::metadata(entrypoint)?.permissions().mode() & 0o111 == 0
                    && let Some((interpreter, interpreter_args)) = parse_shebang(entrypoint)?
                {
                    let mut args = interpreter_args;
                    args.push(entrypoint.as_os_str().to_owned());
                    return Ok((interpreter, args));
                }
            }
            Ok((entrypoint.to_path_buf(), vec![]))
        }
    }
}

#[cfg(unix)]
fn parse_shebang(entrypoint: &Path) -> Result<Option<(PathBuf, Vec<OsString>)>, RunnerError> {
    let mut file = fs::File::open(entrypoint)?;
    let mut bytes = [0_u8; 4096];
    let count = file.read(&mut bytes)?;
    let first = String::from_utf8_lossy(&bytes[..count]);
    let Some(line) = first
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("#!"))
    else {
        return Ok(None);
    };
    let mut words = line.split_ascii_whitespace();
    let Some(program) = words.next() else {
        return Ok(None);
    };
    Ok(Some((
        PathBuf::from(program),
        words.map(OsString::from).collect(),
    )))
}

fn adapter_environment_and_args(
    request: &ScriptExecutionRequest,
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
) -> Result<Vec<OsString>, RunnerError> {
    let context = &request.context;
    match request.manifest.adapter() {
        ScriptAdapter::Sabnzbd => {
            let status = sab_pipeline_status(&context.pipeline_outcome).to_string();
            let script_name = request
                .manifest
                .compatibility_name()
                .map(|name| name.as_str())
                .unwrap_or_else(|| request.manifest.entrypoint());
            for (name, value) in [
                ("SAB_VERSION", env!("CARGO_PKG_VERSION").to_string()),
                ("SAB_NZO_ID", context.job_id.to_string()),
                ("SAB_FINAL_NAME", context.name.clone()),
                ("SAB_FILENAME", context.nzb_filename.clone()),
                ("SAB_CAT", context.category.clone().unwrap_or_default()),
                ("SAB_GROUP", context.group.clone().unwrap_or_default()),
                (
                    "SAB_COMPLETE_DIR",
                    path_text(&context.final_directory)?.to_string(),
                ),
                ("SAB_STATUS", "Running".to_string()),
                ("SAB_PP_STATUS", status.clone()),
                (
                    "SAB_FAIL_MSG",
                    context
                        .compatibility
                        .failure_message
                        .clone()
                        .unwrap_or_default(),
                ),
                ("SAB_URL", context.source_url.clone().unwrap_or_default()),
                ("SAB_FAILURE_URL", String::new()),
                ("SAB_BYTES", context.compatibility.total_bytes.to_string()),
                (
                    "SAB_BYTES_DOWNLOADED",
                    context.compatibility.downloaded_bytes.to_string(),
                ),
                (
                    "SAB_BYTES_TRIED",
                    context.compatibility.downloaded_bytes.to_string(),
                ),
                (
                    "SAB_PASSWORD",
                    context.compatibility.password.clone().unwrap_or_default(),
                ),
                ("SAB_REPAIR", i32::from(context.par_status != 0).to_string()),
                (
                    "SAB_UNPACK",
                    i32::from(context.unpack_status != 0).to_string(),
                ),
                ("SAB_SCRIPT", script_name.to_string()),
            ] {
                insert_env(env, name, &value)?;
            }
            for unavailable in [
                "SAB_CORRECT_PASSWORD",
                "SAB_DUPLICATE",
                "SAB_DUPLICATE_KEY",
                "SAB_ENCRYPTED",
                "SAB_OVERSIZED",
                "SAB_PP",
                "SAB_PRIORITY",
                "SAB_UNWANTED_EXT",
            ] {
                insert_env(env, unavailable, "")?;
            }
            if let Some(app_dir) = context.compatibility.app_dir.as_deref() {
                insert_env(env, "SAB_PROGRAM_DIR", path_text(app_dir)?)?;
            }
            insert_options(env, "SAB_OPTION_", &request.options)?;
            Ok(vec![
                context.final_directory.as_os_str().to_owned(),
                OsString::from(&context.nzb_filename),
                OsString::from(&context.name),
                OsString::new(),
                OsString::from(context.category.as_deref().unwrap_or_default()),
                OsString::from(context.group.as_deref().unwrap_or_default()),
                OsString::from(status),
                OsString::new(),
            ])
        }
        ScriptAdapter::Nzbget => {
            let status = nzbget_pipeline_status(context);
            let total_status = status.split_once('/').map_or(status, |(total, _)| total);
            insert_env(env, "NZBPP_NZBID", &context.job_id.to_string())?;
            insert_env(env, "NZBPP_NZBNAME", &context.name)?;
            insert_env(env, "NZBPP_DIRECTORY", path_text(&context.final_directory)?)?;
            insert_env(env, "NZBPP_NZBFILENAME", &context.nzb_filename)?;
            insert_env(env, "NZBPP_QUEUEDFILE", &context.nzb_filename)?;
            insert_env(
                env,
                "NZBPP_URL",
                context.source_url.as_deref().unwrap_or_default(),
            )?;
            insert_env(
                env,
                "NZBPP_FINALDIR",
                path_text(
                    context
                        .compatibility
                        .final_directory_override
                        .as_deref()
                        .unwrap_or(&context.final_directory),
                )?,
            )?;
            insert_env(
                env,
                "NZBPP_CATEGORY",
                context.category.as_deref().unwrap_or_default(),
            )?;
            insert_env(env, "NZBPP_STATUS", status)?;
            insert_env(env, "NZBPP_TOTALSTATUS", total_status)?;
            insert_env(
                env,
                "NZBPP_SCRIPTSTATUS",
                context.compatibility.previous_script_status.as_str(),
            )?;
            insert_env(env, "NZBPP_PARSTATUS", &context.par_status.to_string())?;
            insert_env(
                env,
                "NZBPP_UNPACKSTATUS",
                &context.unpack_status.to_string(),
            )?;
            insert_env(
                env,
                "NZBPP_HEALTH",
                &context.compatibility.health_milli.to_string(),
            )?;
            insert_env(
                env,
                "NZBPP_CRITICALHEALTH",
                &context.compatibility.critical_health_milli.to_string(),
            )?;
            insert_compat_options(env, "NZBPO", &request.options)?;
            insert_nzbget_global_options(env, &context.compatibility)?;
            insert_parameters(env, &context.compatibility.parameters, &request.manifest)?;
            Ok(vec![])
        }
    }
}

fn sab_pipeline_status(outcome: &PipelineOutcome) -> i32 {
    match outcome {
        PipelineOutcome::Succeeded => 0,
        PipelineOutcome::Failed { stage, .. } => match stage {
            super::model::PipelineFailureStage::Verify
            | super::model::PipelineFailureStage::Repair => 1,
            super::model::PipelineFailureStage::Extract
            | super::model::PipelineFailureStage::Move => 2,
            super::model::PipelineFailureStage::Download => -1,
        },
    }
}

fn nzbget_pipeline_status(context: &JobExecutionContext) -> &'static str {
    if context.compatibility.marked_bad {
        return "FAILURE/BAD";
    }
    match &context.pipeline_outcome {
        PipelineOutcome::Succeeded if context.par_status == 2 || context.unpack_status == 2 => {
            "SUCCESS/ALL"
        }
        PipelineOutcome::Succeeded => "SUCCESS/HEALTH",
        PipelineOutcome::Failed { stage, .. } => match stage {
            super::model::PipelineFailureStage::Download => "FAILURE/HEALTH",
            super::model::PipelineFailureStage::Verify
            | super::model::PipelineFailureStage::Repair => "FAILURE/PAR",
            super::model::PipelineFailureStage::Extract => "FAILURE/UNPACK",
            super::model::PipelineFailureStage::Move => "FAILURE/MOVE",
        },
    }
}

fn insert_parameters(
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
    parameters: &[(String, String)],
    manifest: &ScriptManifest,
) -> Result<(), RunnerError> {
    let script = manifest
        .compatibility_name()
        .map(|name| name.as_str())
        .unwrap_or(manifest.entrypoint());
    for (name, value) in parameters {
        if !valid_parameter_name(name) || value.contains('\0') {
            continue;
        }
        insert_special_env(env, "NZBPR", name, value)?;
        if let Some((prefix, option)) = name.split_once(':')
            && prefix.eq_ignore_ascii_case(script)
            && !option.is_empty()
        {
            insert_special_env(env, "NZBPR", option, value)?;
        }
    }
    Ok(())
}

fn insert_nzbget_global_options(
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
    facts: &CompatibilityFacts,
) -> Result<(), RunnerError> {
    insert_special_env(env, "NZBOP", "Version", env!("CARGO_PKG_VERSION"))?;
    for (name, value) in [
        ("AppDir", facts.app_dir.as_deref()),
        ("MainDir", facts.data_dir.as_deref()),
        ("InterDir", facts.intermediate_dir.as_deref()),
        ("DestDir", facts.complete_dir.as_deref()),
        ("TempDir", facts.temp_dir.as_deref()),
    ] {
        if let Some(value) = value {
            insert_special_env(env, "NZBOP", name, path_text(value)?)?;
        }
    }
    Ok(())
}

fn sanitized_platform_environment() -> Result<BTreeMap<OsStringWire, OsStringWire>, RunnerError> {
    const ALLOWED: &[&str] = &[
        "PATH",
        "HOME",
        "USERPROFILE",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "PATHEXT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "TZ",
    ];
    let mut env = BTreeMap::new();
    for name in ALLOWED {
        if let Some(value) = std::env::var_os(name) {
            env.insert(OsStringWire::from_os(*name)?, OsStringWire::from_os(value)?);
        }
    }
    Ok(env)
}

fn insert_env(
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
    name: &str,
    value: &str,
) -> Result<(), RunnerError> {
    if name.contains(['\0', '=']) || value.contains('\0') {
        return Err(RunnerError::InvalidEnvironment);
    }
    env.insert(OsStringWire::from_os(name)?, OsStringWire::from_os(value)?);
    Ok(())
}

fn insert_options(
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
    prefix: &str,
    options: &[ResolvedOption],
) -> Result<(), RunnerError> {
    for option in options {
        let name = format!("{prefix}{}", env_name(option.name().as_str()));
        insert_env(env, &name, &option_value_text(option.value()))?;
    }
    Ok(())
}

fn insert_special_env(
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
    prefix: &str,
    name: &str,
    value: &str,
) -> Result<(), RunnerError> {
    let original = format!("{prefix}_{name}");
    insert_env(env, &original, value)?;
    let normalized = env_name(&original);
    if normalized != original {
        insert_env(env, &normalized, value)?;
    }
    Ok(())
}

fn insert_compat_options(
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
    prefix: &str,
    options: &[ResolvedOption],
) -> Result<(), RunnerError> {
    for option in options {
        insert_special_env(
            env,
            prefix,
            option.name().as_str(),
            &option_value_text(option.value()),
        )?;
    }
    Ok(())
}

fn option_value_text(value: &OptionValue) -> String {
    match value {
        OptionValue::String(value) => value.clone(),
        OptionValue::Integer(value) => value.to_string(),
        OptionValue::Number(value) => value.to_string(),
        OptionValue::Boolean(value) => if *value { "yes" } else { "no" }.to_string(),
        OptionValue::Secret(value) => value.expose_for_execution().to_string(),
    }
}

/// A fetched URL can carry a credential in userinfo or a query parameter.
/// Keep ordinary source URLs visible, but protect credential-bearing ones before
/// their script output is parsed into logs, directives or retained output.
fn append_source_url_secrets(secrets: &mut Vec<Vec<u8>>, source_url: &str) {
    let Ok(url) = reqwest::Url::parse(source_url) else {
        return;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return;
    }
    let mut components = Vec::new();
    let mut sensitive = false;
    if !url.username().is_empty() {
        sensitive = true;
        if url.password().is_none() && url.username().len() >= 4 {
            components.push(url.username().as_bytes().to_vec());
        }
    }
    if let Some(password) = url.password().filter(|password| !password.is_empty()) {
        sensitive = true;
        if password.len() >= 4 {
            components.push(password.as_bytes().to_vec());
        }
    }
    if let Some(query) = url.query() {
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=')
                && sensitive_url_query_key(key)
                && !value.is_empty()
            {
                sensitive = true;
                if value.len() >= 4 {
                    components.push(value.as_bytes().to_vec());
                }
            }
        }
        for (key, value) in url.query_pairs() {
            if sensitive_url_query_key(&key) && !value.is_empty() {
                sensitive = true;
                if value.len() >= 4 {
                    components.push(value.as_bytes().to_vec());
                }
            }
        }
    }
    if sensitive {
        secrets.push(source_url.as_bytes().to_vec());
        secrets.extend(components);
    }
}

fn sensitive_url_query_key(key: &str) -> bool {
    let key = key.replace(['_', '-'], "").to_ascii_lowercase();
    matches!(
        key.as_str(),
        "apikey"
            | "accesstoken"
            | "token"
            | "auth"
            | "authorization"
            | "password"
            | "passwd"
            | "secret"
            | "signature"
            | "sig"
            | "key"
            | "xamzsignature"
            | "xamzcredential"
            | "xamzsecuritytoken"
            | "xgoogsignature"
            | "xgoogcredential"
            | "xgoogsecuritytoken"
    )
}

fn redact_bytes(input: &[u8], secrets: &[Vec<u8>]) -> Vec<u8> {
    // Capture emits complete lines independently. Multiline credentials must
    // therefore also redact each nonempty line before logs or directives leave
    // the capture task. Prefer longer matches when secret values overlap.
    let mut patterns: Vec<&[u8]> = secrets
        .iter()
        .flat_map(|secret| {
            std::iter::once(secret.as_slice()).chain(
                secret
                    .split(|byte| *byte == b'\n')
                    .map(|line| line.strip_suffix(b"\r").unwrap_or(line)),
            )
        })
        .filter(|secret| !secret.is_empty())
        .collect();
    patterns.sort_unstable_by(|left, right| right.len().cmp(&left.len()).then(left.cmp(right)));
    patterns.dedup();
    let mut output = input.to_vec();
    for secret in patterns {
        let mut cursor = 0;
        while cursor + secret.len() <= output.len() {
            let Some(offset) = output[cursor..]
                .windows(secret.len())
                .position(|candidate| candidate == secret)
            else {
                break;
            };
            let start = cursor + offset;
            output.splice(start..start + secret.len(), b"[REDACTED]".iter().copied());
            cursor = start + b"[REDACTED]".len();
        }
    }
    output
}

fn redact_string(input: &str, secrets: &[Vec<u8>]) -> String {
    String::from_utf8_lossy(&redact_bytes(input.as_bytes(), secrets)).into_owned()
}

fn redact_runner_error(error: RunnerError, secrets: &[Vec<u8>]) -> RunnerError {
    if secrets.is_empty() {
        return error;
    }
    match error {
        RunnerError::SupervisorProtocol(message) => {
            RunnerError::SupervisorProtocol(redact_string(&message, secrets))
        }
        RunnerError::Io(_) => {
            RunnerError::Io(io::Error::other("post-processing I/O operation failed"))
        }
        other => other,
    }
}

fn env_name(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn path_text(path: &Path) -> Result<&str, RunnerError> {
    path.to_str().ok_or(RunnerError::InvalidEnvironment)
}

async fn execute_supervised(
    prepared: PreparedExecution,
    timeout: Option<Duration>,
    grace: Duration,
    cancellation: Option<watch::Receiver<bool>>,
) -> Result<ScriptExecutionResult, RunnerError> {
    let executable = prepared
        .supervisor_executable
        .clone()
        .map(Ok)
        .unwrap_or_else(std::env::current_exe)?;
    let mut command = Command::new(executable);
    command
        .arg(SUPERVISOR_ARG)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
    }
    let spawn_started = Instant::now();
    let mut child = command.spawn()?;
    let supervisor_pid = child.id();
    crate::runtime::perf_probe::record("pp.runner.spawn_supervisor", spawn_started.elapsed());
    let request_json = serde_json::to_vec(&prepared.supervisor)
        .map_err(|error| RunnerError::SupervisorProtocol(error.to_string()))?;
    let request_length = u64::try_from(request_json.len())
        .map_err(|_| RunnerError::SupervisorProtocol("supervisor request is too large".into()))?;
    if request_length > MAX_SUPERVISOR_REQUEST_BYTES {
        return Err(RunnerError::SupervisorProtocol(
            "supervisor request is too large".into(),
        ));
    }
    let mut stdin = child.stdin.take().ok_or_else(|| {
        RunnerError::SupervisorProtocol("supervisor stdin was unavailable".into())
    })?;
    stdin.write_all(&request_length.to_le_bytes()).await?;
    stdin.write_all(&request_json).await?;

    let output = Arc::new(Mutex::new(BoundedOutput {
        ceiling: prepared.capture.ceiling,
        ..Default::default()
    }));
    let stdout = child.stdout.take().ok_or_else(|| {
        RunnerError::SupervisorProtocol("supervisor stdout was unavailable".into())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        RunnerError::SupervisorProtocol("supervisor stderr was unavailable".into())
    })?;
    let stdout_task = tokio::spawn(capture_supervised_stdout(
        stdout,
        output.clone(),
        prepared.capture.clone(),
    ));
    let stderr_task = tokio::spawn(capture_stream(
        stderr,
        output.clone(),
        prepared.capture.clone(),
    ));

    let deadline = timeout
        .map(|timeout| {
            Instant::now()
                .checked_add(timeout)
                .ok_or(RunnerError::InvalidTimeout)
        })
        .transpose()?;
    let mut cancellation = cancellation;
    // Exit, cancellation and the timeout are all awaited together rather than
    // polled: waiting on the child reports the exit as soon as it happens, so a
    // script producing no output costs no wall clock beyond its own runtime.
    let (status, forced) = {
        let cancelled = async {
            match cancellation.as_mut() {
                Some(receiver) => loop {
                    if *receiver.borrow() {
                        return;
                    }
                    if receiver.changed().await.is_err() {
                        // Sender gone: nothing can cancel us any more.
                        std::future::pending::<()>().await;
                    }
                },
                // No cancellation channel: never fires.
                None => std::future::pending::<()>().await,
            }
        };
        let timed_out = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            status = child.wait() => (Some(status?), None),
            () = cancelled => {
                terminate_supervisor(
                    &mut child,
                    supervisor_pid,
                    user_cancellation_grace(grace),
                )
                .await?;
                (None, Some(ExecutionDisposition::Cancelled))
            }
            () = timed_out => {
                terminate_supervisor(&mut child, supervisor_pid, grace).await?;
                (None, Some(ExecutionDisposition::TimedOut))
            }
        }
    };
    drop(stdin);
    let (stdout_result, stderr_result) = tokio::join!(stdout_task, stderr_task);
    let launched =
        stdout_result.map_err(|error| RunnerError::SupervisorProtocol(error.to_string()))??;
    stderr_result.map_err(|error| RunnerError::SupervisorProtocol(error.to_string()))??;
    if !launched && forced.is_none() {
        return Err(RunnerError::SupervisorProtocol(
            "supervisor did not confirm script launch".into(),
        ));
    }
    let captured = Arc::try_unwrap(output)
        .map_err(|_| RunnerError::SupervisorProtocol("output collector remained shared".into()))?
        .into_inner()
        .map_err(|_| RunnerError::SupervisorProtocol("output collector was poisoned".into()))?;
    let exit_code = status.as_ref().and_then(ExitStatus::code);
    let disposition = forced.unwrap_or_else(|| adapter_disposition(prepared.adapter, exit_code));
    if prepared.adapter == ScriptAdapter::Nzbget && exit_code == Some(92) {
        // NZBGet's par-check request has no successor: repair is native and
        // already authoritative by the time scripts run.
        tracing::info!(
            "post-processing script requested a PAR check (exit 92); weaver's repair stage is authoritative"
        );
    }
    let output_truncated = captured.truncated;
    Ok(ScriptExecutionResult {
        disposition,
        exit_code,
        output: captured.into_bytes(),
        output_truncated,
        error_message: match disposition {
            ExecutionDisposition::TimedOut => Some("post-processing script timed out".into()),
            ExecutionDisposition::Cancelled => Some("post-processing script was cancelled".into()),
            ExecutionDisposition::Failed | ExecutionDisposition::Warned => Some(match exit_code {
                Some(code) => format!("post-processing script exited with status {code}"),
                None => "post-processing script terminated without an exit status".into(),
            }),
            _ => None,
        },
    })
}

struct BoundedOutput {
    lines: VecDeque<Vec<u8>>,
    bytes: u64,
    truncated: bool,
    ceiling: u64,
}

impl Default for BoundedOutput {
    fn default() -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            truncated: false,
            ceiling: MAX_SCRIPT_OUTPUT_BYTES,
        }
    }
}

impl BoundedOutput {
    fn push(&mut self, line: Vec<u8>) {
        self.bytes = self.bytes.saturating_add(line.len() as u64);
        self.lines.push_back(line);
        while self.bytes > self.ceiling && self.lines.len() > 1 {
            let removed = self.lines.pop_front().expect("non-empty");
            self.bytes = self.bytes.saturating_sub(removed.len() as u64);
            self.truncated = true;
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.lines.into_iter().flatten().collect()
    }
}

async fn capture_supervised_stdout<R: AsyncRead + Unpin>(
    mut reader: R,
    output: Arc<Mutex<BoundedOutput>>,
    policy: CapturePolicy,
) -> Result<bool, io::Error> {
    let mut preamble = vec![0; SUPERVISOR_LAUNCHED.len()];
    let launched = match reader.read_exact(&mut preamble).await {
        Ok(_) => preamble == SUPERVISOR_LAUNCHED,
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => false,
        Err(error) => return Err(error),
    };
    if launched {
        capture_stream(reader, output, policy).await?;
    } else {
        // Always drain the pipe, even when a failed supervisor never launched
        // the script. Its exit status must not masquerade as a script exit.
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
    }
    Ok(launched)
}

async fn capture_stream<R: AsyncRead + Unpin>(
    mut reader: R,
    output: Arc<Mutex<BoundedOutput>>,
    policy: CapturePolicy,
) -> Result<(), io::Error> {
    let mut pending = Vec::new();
    let mut oversized = false;
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        for part in buffer[..count].split_inclusive(|byte| *byte == b'\n') {
            if !oversized {
                if pending.len() + part.len() > MAX_LOGICAL_LINE_BYTES {
                    // Discard a fragmented logical line in full. This also prevents
                    // secrets spanning a chunk boundary from escaping redaction.
                    pending.clear();
                    oversized = true;
                    let mut output = output.lock().expect("output collector poisoned");
                    output.truncated = true;
                    output.push(b"[oversized script line omitted]\n".to_vec());
                } else {
                    pending.extend_from_slice(part);
                }
            }
            if part.last() == Some(&b'\n') {
                if !oversized {
                    capture_line(std::mem::take(&mut pending), &output, &policy).await;
                }
                oversized = false;
            }
        }
    }
    if !pending.is_empty() {
        capture_line(pending, &output, &policy).await;
    }
    Ok(())
}

async fn capture_line(line: Vec<u8>, output: &Arc<Mutex<BoundedOutput>>, policy: &CapturePolicy) {
    let newline = line.last() == Some(&b'\n');
    let redacted = redact_bytes(&line, &policy.secrets);
    let (tail, event) = if let Some(kind) = &policy.event {
        let (mut tail, event) = parse_line(kind, &String::from_utf8_lossy(&redacted));
        if !tail.is_empty() && newline {
            tail.push('\n');
        }
        (tail.into_bytes(), event)
    } else {
        (redacted, None)
    };
    if !tail.is_empty() {
        output.lock().expect("output collector poisoned").push(tail);
    }
    if let (Some(sender), Some(event)) = (&policy.events, event) {
        let _ = sender.send(event).await;
    }
}

/// SABnzbd records any nonzero exit as a warning; NZBGet defines 93/94/95.
fn adapter_disposition(adapter: ScriptAdapter, exit_code: Option<i32>) -> ExecutionDisposition {
    match (adapter, exit_code) {
        (ScriptAdapter::Sabnzbd, Some(0)) => ExecutionDisposition::Succeeded,
        (ScriptAdapter::Sabnzbd, _) => ExecutionDisposition::Warned,
        (ScriptAdapter::Nzbget, Some(92 | 93)) => ExecutionDisposition::Succeeded,
        (ScriptAdapter::Nzbget, Some(95)) => ExecutionDisposition::Skipped,
        (ScriptAdapter::Nzbget, _) => ExecutionDisposition::Failed,
    }
}

async fn terminate_supervisor(
    child: &mut tokio::process::Child,
    pid: Option<u32>,
    grace: Duration,
) -> Result<(), RunnerError> {
    #[cfg(unix)]
    if let Some(pid) = pid {
        let pid = i32::try_from(pid).map_err(|_| RunnerError::InvalidEntrypoint)?;
        // SAFETY: a negative PID targets only the supervisor-created process group.
        unsafe {
            libc::kill(-pid, libc::SIGTERM);
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(grace)
            .ok_or(RunnerError::InvalidTimeout)?;
        // Do not reap the leader during the grace period. It can exit on TERM
        // while a descendant ignores the signal. Keeping its PID reserved also
        // prevents the process-group identity from being reused before KILL.
        tokio::time::sleep_until(deadline).await;
        // SAFETY: the unreaped leader still reserves this process-group ID.
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
        let _ = child.wait().await?;
        return Ok(());
    }
    let _ = (pid, grace);
    child.kill().await?;
    let _ = child.wait().await?;
    Ok(())
}

/// Hidden same-binary supervisor entrypoint. Call before normal CLI/config initialization.
pub fn maybe_run_supervisor_from_process_args() -> Option<i32> {
    (std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new(SUPERVISOR_ARG)))
        .then(run_supervisor_stdio)
}

pub fn run_supervisor_stdio() -> i32 {
    match run_supervisor_stdio_inner() {
        Ok(code) => code,
        Err(error) => {
            let _ = writeln!(io::stderr(), "post-processing supervisor failed: {error}");
            127
        }
    }
}

fn run_supervisor_stdio_inner() -> Result<i32, RunnerError> {
    #[cfg(windows)]
    let _job = WindowsJob::assign_current_process()?;
    let mut stdin = io::stdin();
    let request = read_supervisor_request(&mut stdin)?;
    let mut command = std::process::Command::new(request.program);
    command
        .args(request.args.into_iter().map(OsStringWire::into_os))
        .env_clear()
        .envs(
            request
                .env
                .into_iter()
                .map(|(key, value)| (key.into_os(), value.into_os())),
        )
        .current_dir(request.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| RunnerError::SupervisorProtocol("child stdout was unavailable".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| RunnerError::SupervisorProtocol("child stderr was unavailable".into()))?;
    let parent_pipe_lost = Arc::new(AtomicBool::new(false));
    let parent_liveness = parent_pipe_lost.clone();
    std::thread::spawn(move || {
        let mut byte = [0_u8; 1];
        loop {
            match stdin.read(&mut byte) {
                Ok(0) | Err(_) => {
                    parent_liveness.store(true, Ordering::Release);
                    break;
                }
                Ok(_) => {}
            }
        }
    });
    let announced = {
        let mut output = io::stdout().lock();
        output
            .write_all(SUPERVISOR_LAUNCHED)
            .and_then(|()| output.flush())
    };
    if announced.is_err() {
        terminate_on_parent_pipe_loss(&mut child);
        return Ok(125);
    }
    let stdout_thread = relay_thread(stdout, io::stdout(), parent_pipe_lost.clone());
    let stderr_thread = relay_thread(stderr, io::stderr(), parent_pipe_lost.clone());
    let status = loop {
        if parent_pipe_lost.load(Ordering::Acquire) {
            terminate_on_parent_pipe_loss(&mut child);
            return Ok(125);
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    let _ = stdout_thread.join();
    let _ = stderr_thread.join();
    Ok(status.code().unwrap_or(126))
}

fn read_supervisor_request<R: Read>(reader: &mut R) -> Result<SupervisorRequest, RunnerError> {
    let mut length = [0_u8; 8];
    reader.read_exact(&mut length)?;
    let length = u64::from_le_bytes(length);
    if length > MAX_SUPERVISOR_REQUEST_BYTES {
        return Err(RunnerError::SupervisorProtocol(
            "supervisor request is too large".into(),
        ));
    }
    let length = usize::try_from(length)
        .map_err(|_| RunnerError::SupervisorProtocol("supervisor request is too large".into()))?;
    let mut bytes = vec![0_u8; length];
    reader.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| RunnerError::SupervisorProtocol(error.to_string()))
}

fn relay_thread<R, W>(
    mut reader: R,
    mut writer: W,
    parent_pipe_lost: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            let count = match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => count,
                Err(_) => break,
            };
            if writer.write_all(&buffer[..count]).is_err() || writer.flush().is_err() {
                parent_pipe_lost.store(true, Ordering::Release);
                break;
            }
        }
    })
}

fn terminate_on_parent_pipe_loss(_child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        // SAFETY: the supervisor is launched as the leader of a dedicated process group;
        // signaling group zero terminates the supervisor and all of its descendants.
        unsafe {
            libc::kill(0, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = _child.kill();
    }
}

#[cfg(test)]
pub(crate) fn adapter_contract_for_test(
    request: &ScriptExecutionRequest,
) -> Result<(Vec<String>, BTreeMap<String, String>), RunnerError> {
    let mut env = BTreeMap::new();
    let args = adapter_environment_and_args(request, &mut env)?
        .into_iter()
        .map(|value| {
            value
                .into_string()
                .map_err(|_| RunnerError::InvalidEnvironment)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let env = env
        .into_iter()
        .map(|(key, value)| {
            let key = key
                .into_os()
                .into_string()
                .map_err(|_| RunnerError::InvalidEnvironment)?;
            let value = value
                .into_os()
                .into_string()
                .map_err(|_| RunnerError::InvalidEnvironment)?;
            Ok((key, value))
        })
        .collect::<Result<_, RunnerError>>()?;
    Ok((args, env))
}

#[cfg(test)]
pub(crate) fn adapter_disposition_for_test(
    adapter: ScriptAdapter,
    exit_code: Option<i32>,
) -> ExecutionDisposition {
    adapter_disposition(adapter, exit_code)
}

#[cfg(test)]
pub(crate) fn bounded_output_for_test(lines: Vec<Vec<u8>>) -> (Vec<u8>, bool) {
    let mut captured = BoundedOutput::default();
    for line in lines {
        captured.push(line);
    }
    let truncated = captured.truncated;
    (captured.into_bytes(), truncated)
}

#[cfg(test)]
pub(crate) fn redact_bytes_for_test(input: &[u8], secrets: &[Vec<u8>]) -> Vec<u8> {
    redact_bytes(input, secrets)
}

#[cfg(test)]
pub(crate) fn cancellation_grace_for_test(grace: Duration) -> Duration {
    user_cancellation_grace(grace)
}

#[cfg(test)]
mod capture_tests {
    use super::*;
    use crate::post_processing::directives::Directive;

    #[tokio::test]
    async fn credential_bearing_source_urls_are_redacted_before_log_emission() {
        let url = "https://account:password123@example.invalid/file?api_key=token123";
        let aws_url = "https://example.invalid/file?X-Amz-Signature=awssecret123";
        let google_url = "https://example.invalid/file?X-Goog-Signature=googsecret123";
        let mut secrets = Vec::new();
        append_source_url_secrets(&mut secrets, url);
        append_source_url_secrets(&mut secrets, aws_url);
        append_source_url_secrets(&mut secrets, google_url);
        let output = Arc::new(Mutex::new(BoundedOutput::default()));
        let (sender, mut receiver) = mpsc::channel(4);
        let policy = CapturePolicy {
            secrets: Arc::new(secrets),
            event: Some(ScriptEventLabel::Scan),
            events: Some(sender),
            ceiling: MAX_SCRIPT_OUTPUT_BYTES,
        };
        let lines = format!(
            "[INFO] {url}\n[WARNING] password123 token123 awssecret123 googsecret123\n[INFO] {aws_url}\n[INFO] {google_url}\n"
        );
        capture_stream(lines.as_bytes(), output.clone(), policy)
            .await
            .unwrap();
        assert_eq!(
            receiver.recv().await,
            Some(ScriptOutputEvent::Log {
                level: crate::post_processing::directives::ScriptLogLevel::Info,
                text: "[REDACTED]".into(),
            })
        );
        assert_eq!(
            receiver.recv().await,
            Some(ScriptOutputEvent::Log {
                level: crate::post_processing::directives::ScriptLogLevel::Warning,
                text: "[REDACTED] [REDACTED] [REDACTED] [REDACTED]".into(),
            })
        );
        for _ in 0..2 {
            assert_eq!(
                receiver.recv().await,
                Some(ScriptOutputEvent::Log {
                    level: crate::post_processing::directives::ScriptLogLevel::Info,
                    text: "[REDACTED]".into(),
                })
            );
        }
        assert!(receiver.recv().await.is_none());
        let captured = Arc::try_unwrap(output).ok().unwrap().into_inner().unwrap();
        let text = String::from_utf8(captured.into_bytes()).unwrap();
        assert!(!text.contains(url));
        assert!(!text.contains("password123"));
        assert!(!text.contains("token123"));
        assert!(!text.contains("awssecret123"));
        assert!(!text.contains("googsecret123"));

        let benign = "https://example.invalid/file?page=2";
        let mut secrets = Vec::new();
        append_source_url_secrets(&mut secrets, benign);
        assert!(secrets.is_empty());
        assert_eq!(redact_string(benign, &secrets), benign);
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn cancellation_kills_a_descendant_that_ignores_term() {
        use std::os::unix::process::CommandExt;

        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(r#"sh -c 'trap "" TERM; printf ready; while :; do :; done' & wait"#)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        command.as_std_mut().process_group(0);
        let mut supervisor = command.spawn().unwrap();
        let pid = supervisor.id();
        let mut output = supervisor.stdout.take().unwrap();
        let mut ready = [0; 5];
        output.read_exact(&mut ready).await.unwrap();
        assert_eq!(&ready, b"ready");

        terminate_supervisor(&mut supervisor, pid, Duration::from_secs(1))
            .await
            .unwrap();
        // The descendant holds this pipe directly. EOF proves it exited;
        // merely observing the supervisor exit cannot establish that.
        let mut remainder = Vec::new();
        output.read_to_end(&mut remainder).await.unwrap();
        assert!(remainder.is_empty());
    }

    #[tokio::test]
    async fn multiline_secrets_are_redacted_before_capture_events_leave() {
        let output = Arc::new(Mutex::new(BoundedOutput::default()));
        let (sender, mut receiver) = mpsc::channel(8);
        let policy = CapturePolicy {
            secrets: Arc::new(vec![b"secret-alpha\r\n\r\nsecret-beta".to_vec()]),
            event: Some(ScriptEventLabel::Scan),
            events: Some(sender),
            ceiling: MAX_SCRIPT_OUTPUT_BYTES,
        };
        let input = b"secret-alpha\r\n\r\nsecret-beta\n[INFO] secret-alpha\n[NZB] NZBPR_Token=secret-beta\n";
        capture_stream(input.as_slice(), output.clone(), policy)
            .await
            .unwrap();
        assert_eq!(
            receiver.recv().await,
            Some(ScriptOutputEvent::Log {
                level: crate::post_processing::directives::ScriptLogLevel::Info,
                text: "[REDACTED]".into(),
            })
        );
        assert_eq!(
            receiver.recv().await,
            Some(ScriptOutputEvent::Directive(Directive::Parameter {
                name: "Token".into(),
                value: "[REDACTED]".into(),
            }))
        );
        assert!(receiver.recv().await.is_none());
        let captured = Arc::try_unwrap(output).ok().unwrap().into_inner().unwrap();
        let text = String::from_utf8(captured.into_bytes()).unwrap();
        assert!(!text.contains("secret-alpha"));
        assert!(!text.contains("secret-beta"));
        assert!(text.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn capture_redacts_before_directives_and_omits_oversized_lines() {
        let output = Arc::new(Mutex::new(BoundedOutput::default()));
        let (sender, mut receiver) = mpsc::channel(8);
        let policy = CapturePolicy {
            secrets: Arc::new(vec![b"sensitive-value".to_vec()]),
            event: Some(ScriptEventLabel::Scan),
            events: Some(sender),
            ceiling: MAX_SCRIPT_OUTPUT_BYTES,
        };
        let mut input = b"[NZB] NZBPR_Token=sensitive-value\n[INFO] sensitive-value\r\n".to_vec();
        input.extend_from_slice(b"[NZB] NZBPR_TooLong=");
        input.extend(vec![b'x'; MAX_LOGICAL_LINE_BYTES]);
        input.extend_from_slice(b"\n[NZB] NZBPR_After=yes\n");
        capture_stream(input.as_slice(), output.clone(), policy)
            .await
            .unwrap();
        assert_eq!(
            receiver.recv().await,
            Some(ScriptOutputEvent::Directive(Directive::Parameter {
                name: "Token".into(),
                value: "[REDACTED]".into()
            }))
        );
        assert_eq!(
            receiver.recv().await,
            Some(ScriptOutputEvent::Log {
                level: crate::post_processing::directives::ScriptLogLevel::Info,
                text: "[REDACTED]".into()
            })
        );
        assert_eq!(
            receiver.recv().await,
            Some(ScriptOutputEvent::Directive(Directive::Parameter {
                name: "After".into(),
                value: "yes".into()
            }))
        );
        assert!(receiver.recv().await.is_none());
        let captured = Arc::try_unwrap(output).ok().unwrap().into_inner().unwrap();
        assert!(captured.truncated);
        let text = String::from_utf8(captured.into_bytes()).unwrap();
        assert!(!text.contains("sensitive-value"));
        assert!(!text.contains("[INFO]"));
        assert!(!text.contains("TooLong"));
    }
}

#[cfg(windows)]
struct WindowsJob(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
impl WindowsJob {
    fn assign_current_process() -> Result<Self, RunnerError> {
        use std::mem::size_of;
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        };
        use windows_sys::Win32::System::Threading::GetCurrentProcess;

        // SAFETY: Windows API calls receive initialized structures and valid process handles.
        unsafe {
            let handle = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if handle.is_null() {
                return Err(io::Error::last_os_error().into());
            }
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const _,
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            ) == 0
                || AssignProcessToJobObject(handle, GetCurrentProcess()) == 0
            {
                let error = io::Error::last_os_error();
                CloseHandle(handle);
                return Err(error.into());
            }
            Ok(Self(handle))
        }
    }
}

#[cfg(windows)]
impl Drop for WindowsJob {
    fn drop(&mut self) {
        // SAFETY: handle is owned by this guard and closed exactly once.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}
