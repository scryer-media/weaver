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
use tokio::sync::watch;

use super::model::{OptionValue, PipelineOutcome, ResolvedOption, ScriptAdapter, ScriptManifest};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_TERMINATION_GRACE: Duration = Duration::from_secs(10);
/// Per-script output retained on the job. Anything beyond this keeps the tail.
pub const MAX_SCRIPT_OUTPUT_BYTES: u64 = 256 * 1024;
pub const MAX_LOGICAL_LINE_BYTES: usize = 64 * 1024;

const SUPERVISOR_ARG: &str = "__post-processing-supervisor";
const MAX_SUPERVISOR_REQUEST_BYTES: u64 = 2 * 1024 * 1024;

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

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
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

#[derive(Debug, Clone, Default)]
pub struct CompatibilityFacts {
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub health_milli: u32,
    pub critical_health_milli: u32,
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

    let prepared = prepare_execution(&request)?;
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

struct PreparedExecution {
    supervisor_executable: Option<PathBuf>,
    supervisor: SupervisorRequest,
    adapter: ScriptAdapter,
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

    let working_directory = fs::canonicalize(&request.context.working_directory)?;

    Ok(PreparedExecution {
        supervisor_executable: request.supervisor_executable.clone(),
        supervisor: SupervisorRequest {
            program,
            args: args
                .into_iter()
                .map(OsStringWire::from_os)
                .collect::<Result<_, _>>()?,
            env,
            cwd: working_directory,
        },
        adapter: request.manifest.adapter(),
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
                    path_text(&context.working_directory)?.to_string(),
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
                context.working_directory.as_os_str().to_owned(),
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
            insert_env(
                env,
                "NZBPP_DIRECTORY",
                path_text(&context.working_directory)?,
            )?;
            insert_env(env, "NZBPP_NZBFILENAME", &context.nzb_filename)?;
            insert_env(env, "NZBPP_QUEUEDFILE", &context.nzb_filename)?;
            insert_env(
                env,
                "NZBPP_URL",
                context.source_url.as_deref().unwrap_or_default(),
            )?;
            insert_env(env, "NZBPP_FINALDIR", path_text(&context.final_directory)?)?;
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

fn redact_bytes(input: &[u8], secrets: &[Vec<u8>]) -> Vec<u8> {
    let mut output = input.to_vec();
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        let mut cursor = 0;
        while cursor + secret.len() <= output.len() {
            let Some(offset) = output[cursor..]
                .windows(secret.len())
                .position(|candidate| candidate == secret.as_slice())
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

    let output = Arc::new(Mutex::new(BoundedOutput::default()));
    let stdout = child.stdout.take().ok_or_else(|| {
        RunnerError::SupervisorProtocol("supervisor stdout was unavailable".into())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        RunnerError::SupervisorProtocol("supervisor stderr was unavailable".into())
    })?;
    let stdout_task = tokio::spawn(capture_stream(stdout, output.clone()));
    let stderr_task = tokio::spawn(capture_stream(stderr, output.clone()));

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
                terminate_supervisor(&mut child, supervisor_pid, grace).await?;
                (None, Some(ExecutionDisposition::Cancelled))
            }
            () = timed_out => {
                terminate_supervisor(&mut child, supervisor_pid, grace).await?;
                (None, Some(ExecutionDisposition::TimedOut))
            }
        }
    };
    drop(stdin);
    stdout_task
        .await
        .map_err(|error| RunnerError::SupervisorProtocol(error.to_string()))??;
    stderr_task
        .await
        .map_err(|error| RunnerError::SupervisorProtocol(error.to_string()))??;
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

#[derive(Default)]
struct BoundedOutput {
    lines: VecDeque<Vec<u8>>,
    bytes: u64,
    truncated: bool,
}

impl BoundedOutput {
    fn push(&mut self, line: Vec<u8>) {
        self.bytes = self.bytes.saturating_add(line.len() as u64);
        self.lines.push_back(line);
        while self.bytes > MAX_SCRIPT_OUTPUT_BYTES && self.lines.len() > 1 {
            let removed = self.lines.pop_front().expect("non-empty");
            self.bytes = self.bytes.saturating_sub(removed.len() as u64);
            self.truncated = true;
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.lines.into_iter().flatten().collect()
    }
}

async fn capture_stream<R: AsyncRead + Unpin>(
    mut reader: R,
    output: Arc<Mutex<BoundedOutput>>,
) -> Result<(), io::Error> {
    let mut pending = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        pending.extend_from_slice(&buffer[..count]);
        while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
            let line = pending.drain(..=newline).collect::<Vec<_>>();
            output.lock().expect("output collector poisoned").push(line);
        }
        while pending.len() > MAX_LOGICAL_LINE_BYTES {
            let line = pending.drain(..MAX_LOGICAL_LINE_BYTES).collect::<Vec<_>>();
            output.lock().expect("output collector poisoned").push(line);
        }
    }
    if !pending.is_empty() {
        output
            .lock()
            .expect("output collector poisoned")
            .push(std::mem::take(&mut pending));
    }
    Ok(())
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
        let deadline = Instant::now()
            .checked_add(grace)
            .ok_or(RunnerError::InvalidTimeout)?;
        while Instant::now() < deadline {
            if child.try_wait()?.is_some() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // SAFETY: same process-group contract as above.
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
        let _ = child.wait().await?;
        return Ok(());
    }
    let _ = pid;
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
