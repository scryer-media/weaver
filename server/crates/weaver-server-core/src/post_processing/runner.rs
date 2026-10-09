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
    OptionValue, PipelineOutcome, ResolvedOption, ScriptEventLabel, ScriptManifest,
};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_TERMINATION_GRACE: Duration = Duration::from_secs(10);
/// A user cancellation must not inherit an arbitrarily long script shutdown grace.
const MAX_USER_CANCELLATION_GRACE: Duration = Duration::from_secs(5);
/// The most of a run's output that is kept: the newest bytes, stdout and
/// stderr interleaved as they arrived.
pub const MAX_SCRIPT_OUTPUT_BYTES: u64 = 32 * 1024;
/// How much of a stream is read at once.
const READ_CHUNK_BYTES: usize = 8 * 1024;
/// The longest line that is read for directives. Past this a line is only
/// output, and only its tail is kept.
pub const MAX_LOGICAL_LINE_BYTES: usize = MAX_SCRIPT_OUTPUT_BYTES as usize + READ_CHUNK_BYTES;
const REDACTED: &[u8] = b"[REDACTED]";

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
    pub go: Option<PathBuf>,
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

/// Which run this is, as the script itself is told.
#[derive(Debug, Clone, Default)]
pub struct RunIdentity {
    pub run_id: String,
    pub instance_id: String,
    pub instance_name: String,
    pub trigger: String,
    /// Where the script can call weaver back, and the token that lets it for
    /// as long as this run lasts.
    pub api_url: Option<String>,
    pub token: Option<String>,
    /// Where lines the script sends through the API join what it printed.
    pub output: OutputInjector,
}

impl RunIdentity {
    /// A fresh run of `instance`.
    pub fn of(instance: &super::instances::ScriptInstance) -> Result<Self, RunnerError> {
        let mut entropy = [0_u8; 12];
        getrandom::fill(&mut entropy).map_err(|error| RunnerError::Io(io::Error::other(error)))?;
        Ok(Self {
            run_id: hex::encode(entropy),
            instance_id: instance.id.clone(),
            instance_name: instance.name.clone(),
            trigger: instance.trigger.to_string(),
            api_url: None,
            token: None,
            output: OutputInjector::default(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ScriptExecutionRequest {
    pub manifest: ScriptManifest,
    /// Package directory for a manifest package, or the scripts directory for a bare script.
    pub root: PathBuf,
    pub options: Vec<ResolvedOption>,
    pub context: JobExecutionContext,
    pub identity: RunIdentity,
    pub timeout: Option<Duration>,
    pub termination_grace: Duration,
    pub interpreters: InterpreterConfig,
    #[doc(hidden)]
    pub supervisor_executable: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ExecutionDisposition {
    Succeeded,
    /// Exit 95: the script decided it had nothing to do.
    Skipped,
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
    /// Every byte the script wrote, including what `output` no longer holds.
    pub output_bytes: u64,
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
    /// The program is `go run`, whose exit status is not the script's own.
    go_run: bool,
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
    execute_script_observed(request, cancellation, None).await
}

pub async fn execute_script_observed(
    request: ScriptExecutionRequest,
    cancellation: Option<watch::Receiver<bool>>,
    events: Option<mpsc::Sender<ScriptOutputEvent>>,
) -> Result<ScriptExecutionResult, RunnerError> {
    execute_script_tapped(request, cancellation, events, None).await
}

/// [`execute_script_observed`], with each captured line also handed to `tap`
/// as it arrives.
pub async fn execute_script_tapped(
    request: ScriptExecutionRequest,
    cancellation: Option<watch::Receiver<bool>>,
    events: Option<mpsc::Sender<ScriptOutputEvent>>,
    tap: Option<OutputTap>,
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
    if let Some(token) = request.identity.token.as_deref() {
        secrets.push(token.as_bytes().to_vec());
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
        event: Some(ScriptEventLabel::PostProcessing),
        events,
        tap,
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
        // Every line was redacted as it was captured; this pass only catches a
        // secret written across lines, and must not grow the output past the
        // ring.
        let redacted = redact_bytes(&result.output, &secrets);
        if redacted != result.output {
            let mut ring = BoundedOutput::default();
            ring.push(redacted);
            result.output_truncated |= ring.truncated;
            result.output = ring.into_bytes();
        }
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
    pub identity: RunIdentity,
    pub facts: CompatibilityFacts,
    pub interpreters: InterpreterConfig,
    pub supervisor_executable: Option<PathBuf>,
}

pub async fn execute_spec(
    spec: ExecutionSpec,
    cancellation: Option<watch::Receiver<bool>>,
    events: Option<mpsc::Sender<ScriptOutputEvent>>,
) -> Result<ScriptExecutionResult, RunnerError> {
    execute_spec_tapped(spec, cancellation, events, None).await
}

/// [`execute_spec`], with each captured line also handed to `tap` as it
/// arrives.
pub async fn execute_spec_tapped(
    spec: ExecutionSpec,
    cancellation: Option<watch::Receiver<bool>>,
    events: Option<mpsc::Sender<ScriptOutputEvent>>,
    tap: Option<OutputTap>,
) -> Result<ScriptExecutionResult, RunnerError> {
    let root = fs::canonicalize(&spec.root)?;
    let entrypoint = fs::canonicalize(root.join(spec.manifest.entrypoint()))?;
    if !entrypoint.starts_with(&root) || !entrypoint.is_file() {
        return Err(RunnerError::InvalidEntrypoint);
    }
    let (program, mut args) = resolve_program(&entrypoint, &spec.interpreters)?;
    args.extend(spec.argv);
    let go_run = is_go_source(&entrypoint);
    let mut env = sanitized_platform_environment()?;
    if go_run {
        insert_go_environment(&mut env, &spec.facts)?;
    }
    insert_nzbget_global_options(&mut env, &spec.facts)?;
    insert_compat_options(&mut env, "NZBPO", &spec.options)?;
    insert_options(&mut env, "SAB_OPTION_", &spec.options)?;
    insert_weaver_env(&mut env, &spec.identity, &spec.facts, &spec.options)?;
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
    if let Some(token) = spec.identity.token {
        secrets.push(token.into_bytes());
    }
    if let Some(source_url) = source_url.as_deref() {
        append_source_url_secrets(&mut secrets, source_url);
    }
    let prepared = PreparedExecution {
        injector: spec.identity.output,
        supervisor_executable: spec.supervisor_executable,
        supervisor: SupervisorRequest {
            program,
            args: args
                .into_iter()
                .map(OsStringWire::from_os)
                .collect::<Result<_, _>>()?,
            env,
            cwd: fs::canonicalize(spec.cwd)?,
            go_run,
        },
        capture: CapturePolicy {
            secrets: Arc::new(secrets.clone()),
            event: Some(spec.kind.clone()),
            events,
            tap,
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
    if let Some(message) = &mut result.error_message {
        *message = redact_string(message, &secrets);
    }
    Ok(result)
}

/// Receives each line of a run's captured output as it arrives: redacted, and
/// with the commands the script issued already taken out.
pub type OutputTap = Arc<dyn Fn(&[u8]) + Send + Sync>;

#[derive(Clone, Default)]
struct CapturePolicy {
    secrets: Arc<Vec<Vec<u8>>>,
    event: Option<ScriptEventLabel>,
    events: Option<mpsc::Sender<ScriptOutputEvent>>,
    tap: Option<OutputTap>,
}

struct PreparedExecution {
    supervisor_executable: Option<PathBuf>,
    supervisor: SupervisorRequest,
    capture: CapturePolicy,
    injector: OutputInjector,
}

/// Adds lines to a run's captured output that the script did not print: what
/// it logged through the API. It reaches the output only while the script is
/// running; a line offered at any other time is dropped.
#[derive(Clone, Default)]
pub struct OutputInjector(Arc<Mutex<Option<InjectionTarget>>>);

struct InjectionTarget {
    output: Arc<Mutex<BoundedOutput>>,
    secrets: Arc<Vec<Vec<u8>>>,
    tap: Option<OutputTap>,
}

impl std::fmt::Debug for OutputInjector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("OutputInjector")
    }
}

impl OutputInjector {
    fn target(&self) -> std::sync::MutexGuard<'_, Option<InjectionTarget>> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn bind(&self, output: &Arc<Mutex<BoundedOutput>>, policy: &CapturePolicy) -> BoundInjector {
        *self.target() = Some(InjectionTarget {
            output: output.clone(),
            secrets: policy.secrets.clone(),
            tap: policy.tap.clone(),
        });
        BoundInjector(self.clone())
    }

    /// `text` with the run's secrets taken out, or `None` when the script is
    /// not running and nothing is known about what must be kept out.
    pub(crate) fn redact(&self, text: &str) -> Option<String> {
        let target = self.target();
        let target = target.as_ref()?;
        Some(redact_string(text, &target.secrets))
    }

    /// Add `text` to the output as lines of its own. False when the script is
    /// not running.
    pub(crate) fn push(&self, text: &str) -> bool {
        let target = self.target();
        let Some(target) = target.as_ref() else {
            return false;
        };
        let mut line = redact_string(text, &target.secrets).into_bytes();
        line.push(b'\n');
        if let Some(tap) = &target.tap {
            tap(&line);
        }
        let mut output = target
            .output
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        output.written = output.written.saturating_add(line.len() as u64);
        output.push(line);
        true
    }
}

/// Lets go of the output when the capture that owns it is over.
struct BoundInjector(OutputInjector);

impl Drop for BoundInjector {
    fn drop(&mut self) {
        *self.0.target() = None;
    }
}

fn prepare_execution(request: &ScriptExecutionRequest) -> Result<PreparedExecution, RunnerError> {
    let root = fs::canonicalize(&request.root)?;
    let entrypoint = fs::canonicalize(root.join(request.manifest.entrypoint()))?;
    if !entrypoint.starts_with(&root) || !entrypoint.is_file() {
        return Err(RunnerError::InvalidEntrypoint);
    }

    let (program, mut args) = resolve_program(&entrypoint, &request.interpreters)?;
    let go_run = is_go_source(&entrypoint);
    let mut env = sanitized_platform_environment()?;
    if go_run {
        insert_go_environment(&mut env, &request.context.compatibility)?;
    }
    let adapter_args = adapter_environment_and_args(request, &mut env)?;
    args.extend(adapter_args);

    let final_directory = fs::canonicalize(&request.context.final_directory)?;

    Ok(PreparedExecution {
        injector: request.identity.output.clone(),
        supervisor_executable: request.supervisor_executable.clone(),
        supervisor: SupervisorRequest {
            program,
            args: args
                .into_iter()
                .map(OsStringWire::from_os)
                .collect::<Result<_, _>>()?,
            env,
            cwd: final_directory,
            go_run,
        },
        capture: CapturePolicy::default(),
    })
}

fn script_extension(entrypoint: &Path) -> String {
    entrypoint
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn is_go_source(entrypoint: &Path) -> bool {
    script_extension(entrypoint) == "go"
}

fn resolve_program(
    entrypoint: &Path,
    interpreters: &InterpreterConfig,
) -> Result<(PathBuf, Vec<OsString>), RunnerError> {
    match script_extension(entrypoint).as_str() {
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
        // A Go script is one source file, compiled and run by `go run`. Go
        // reads every leading argument that ends in `.go` as another source
        // file, so a first script argument spelled that way fails the build.
        "go" => Ok((
            interpreters
                .go
                .clone()
                .unwrap_or_else(|| PathBuf::from("go")),
            vec![OsString::from("run"), entrypoint.as_os_str().to_owned()],
        )),
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
    // Every script is handed every family of variables, so one written for
    // either client runs as it is and a new one can use weaver's own.
    let sab_status = sab_pipeline_status(&context.pipeline_outcome).to_string();
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
        ("SAB_PP_STATUS", sab_status.clone()),
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
    let arguments = vec![
        context.final_directory.as_os_str().to_owned(),
        OsString::from(&context.nzb_filename),
        OsString::from(&context.name),
        OsString::new(),
        OsString::from(context.category.as_deref().unwrap_or_default()),
        OsString::from(context.group.as_deref().unwrap_or_default()),
        OsString::from(sab_status),
        OsString::new(),
    ];

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
    let final_directory = context
        .compatibility
        .final_directory_override
        .as_deref()
        .unwrap_or(&context.final_directory);
    for (name, value) in [
        ("WEAVER_JOB_ID", context.job_id.to_string()),
        ("WEAVER_JOB_NAME", context.name.clone()),
        (
            "WEAVER_CATEGORY",
            context.category.clone().unwrap_or_default(),
        ),
        (
            "WEAVER_DIRECTORY",
            path_text(&context.final_directory)?.to_string(),
        ),
        (
            "WEAVER_FINAL_DIRECTORY",
            path_text(final_directory)?.to_string(),
        ),
        ("WEAVER_STATUS", total_status.to_string()),
    ] {
        insert_env(env, name, &value)?;
    }
    insert_weaver_env(
        env,
        &request.identity,
        &context.compatibility,
        &request.options,
    )?;
    Ok(arguments)
}

/// What weaver tells every run about itself: which run and instance this is,
/// where weaver keeps its files, and the instance's inputs.
fn insert_weaver_env(
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
    identity: &RunIdentity,
    facts: &CompatibilityFacts,
    options: &[ResolvedOption],
) -> Result<(), RunnerError> {
    for (name, value) in [
        ("WEAVER_RUN_ID", Some(identity.run_id.as_str())),
        ("WEAVER_INSTANCE_ID", Some(identity.instance_id.as_str())),
        (
            "WEAVER_INSTANCE_NAME",
            Some(identity.instance_name.as_str()),
        ),
        ("WEAVER_TRIGGER", Some(identity.trigger.as_str())),
        ("WEAVER_API_URL", identity.api_url.as_deref()),
        ("WEAVER_RUN_TOKEN", identity.token.as_deref()),
    ] {
        if let Some(value) = value {
            insert_env(env, name, value)?;
        }
    }
    insert_env(env, "WEAVER_VERSION", env!("CARGO_PKG_VERSION"))?;
    for (name, directory) in [
        ("WEAVER_DATA_DIR", facts.data_dir.as_deref()),
        ("WEAVER_COMPLETE_DIR", facts.complete_dir.as_deref()),
    ] {
        if let Some(directory) = directory {
            insert_env(env, name, path_text(directory)?)?;
        }
    }
    insert_options(env, "WEAVER_INPUT_", options)
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

/// The Go build cache, kept under weaver's data directory.
const GO_BUILD_CACHE_DIR: &str = ".weaver-go-cache";

/// What `go run` needs beyond the platform environment. Go will not build
/// without a build cache and looks for one under a home directory the daemon
/// may not have, so the cache is weaver's own: nothing is written beside the
/// scripts, and a read-only scripts directory still works. `GOPROXY=off` keeps
/// a build off the network, which is why a Go script is a single file that
/// imports the standard library only.
fn insert_go_environment(
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
    facts: &CompatibilityFacts,
) -> Result<(), RunnerError> {
    if let Some(data_dir) = facts.data_dir.as_deref() {
        // Go refuses a relative cache path.
        let cache = std::path::absolute(data_dir)?.join(GO_BUILD_CACHE_DIR);
        insert_env(env, "GOCACHE", path_text(&cache)?)?;
    }
    insert_env(env, "GOPROXY", "off")
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

pub(super) fn option_value_text(value: &OptionValue) -> String {
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
    let patterns = redaction_patterns(secrets);
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
            output.splice(start..start + secret.len(), REDACTED.iter().copied());
            cursor = start + REDACTED.len();
        }
    }
    output
}

/// What redaction looks for, longest first.
fn redaction_patterns(secrets: &[Vec<u8>]) -> Vec<&[u8]> {
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
    patterns
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
    let output = Arc::new(Mutex::new(BoundedOutput::default()));
    // Bound before the script is launched, so that nothing it asks for can
    // arrive first, and let go of before the output is taken back below.
    let injected = prepared.injector.bind(&output, &prepared.capture);
    stdin.write_all(&request_length.to_le_bytes()).await?;
    stdin.write_all(&request_json).await?;

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
    drop(injected);
    let captured = Arc::try_unwrap(output)
        .map_err(|_| RunnerError::SupervisorProtocol("output collector remained shared".into()))?
        .into_inner()
        .map_err(|_| RunnerError::SupervisorProtocol("output collector was poisoned".into()))?;
    let exit_code = status.as_ref().and_then(ExitStatus::code);
    let disposition = forced.unwrap_or_else(|| exit_disposition(exit_code));
    if exit_code == Some(92) {
        // NZBGet's par-check request has no successor: repair is native and
        // already authoritative by the time scripts run.
        tracing::info!(
            "post-processing script requested a PAR check (exit 92); weaver's repair stage is authoritative"
        );
    }
    let output_truncated = captured.truncated;
    let output_bytes = captured.written;
    Ok(ScriptExecutionResult {
        disposition,
        exit_code,
        output: captured.into_bytes(),
        output_bytes,
        output_truncated,
        error_message: match disposition {
            ExecutionDisposition::TimedOut => Some("post-processing script timed out".into()),
            ExecutionDisposition::Cancelled => Some("post-processing script was cancelled".into()),
            ExecutionDisposition::Failed => Some(match exit_code {
                Some(code) => format!("post-processing script exited with status {code}"),
                None => "post-processing script terminated without an exit status".into(),
            }),
            _ => None,
        },
    })
}

/// The newest [`MAX_SCRIPT_OUTPUT_BYTES`] of a run's output. Older bytes are
/// dropped as newer ones arrive, mid-line if need be, so the ring never holds
/// more than it hands over.
#[derive(Default)]
struct BoundedOutput {
    ring: VecDeque<u8>,
    /// Every byte the script wrote, kept or not.
    written: u64,
    /// Set exactly when a byte of output was dropped.
    truncated: bool,
    /// Bytes the capture tasks hold as unfinished lines, which will displace
    /// the oldest kept bytes once they arrive.
    pending: usize,
    /// The most the ring and the unfinished lines held at once.
    #[cfg(test)]
    peak: usize,
}

impl BoundedOutput {
    const CAPACITY: usize = MAX_SCRIPT_OUTPUT_BYTES as usize;

    fn push(&mut self, bytes: Vec<u8>) {
        let mut bytes = bytes.as_slice();
        if bytes.len() > Self::CAPACITY {
            bytes = &bytes[bytes.len() - Self::CAPACITY..];
            self.truncated = true;
        }
        let over = (self.ring.len() + bytes.len()).saturating_sub(Self::CAPACITY);
        self.drop_oldest(over);
        self.ring.extend(bytes);
        self.observe();
    }

    fn drop_oldest(&mut self, count: usize) {
        let count = count.min(self.ring.len());
        if count > 0 {
            self.ring.drain(..count);
            self.truncated = true;
        }
    }

    /// Record how much a capture task now holds as an unfinished line, and
    /// make room for it: together they stay within the ring's capacity and
    /// one read. Only a line longer than a read can drop kept bytes early.
    fn hold_pending(&mut self, before: usize, now: usize) {
        self.pending = self.pending.saturating_sub(before).saturating_add(now);
        let over =
            (self.ring.len() + self.pending).saturating_sub(Self::CAPACITY + READ_CHUNK_BYTES);
        self.drop_oldest(over);
        self.observe();
    }

    #[cfg(test)]
    fn observe(&mut self) {
        self.peak = self.peak.max(self.ring.len() + self.pending);
    }

    #[cfg(not(test))]
    fn observe(&mut self) {}

    /// The kept bytes. When the oldest were dropped, a character the cut went
    /// through is dropped whole rather than handed over in part.
    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::from(self.ring);
        if self.truncated {
            let partial = bytes
                .iter()
                .take(3)
                .take_while(|byte| **byte & 0b1100_0000 == 0b1000_0000)
                .count();
            bytes.drain(..partial);
        }
        bytes
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
    // Nothing here holds more than one unfinished line, and a line is held
    // only up to the ring's capacity and one read: past that its oldest bytes
    // are dropped as newer ones arrive, before the line is complete.
    let mut pending = Vec::new();
    let mut held = 0;
    let mut oversized = false;
    let mut head_lost = false;
    let mut buffer = [0_u8; READ_CHUNK_BYTES];
    let hold = |held: &mut usize, now: usize| {
        if *held == now {
            return;
        }
        output
            .lock()
            .expect("output collector poisoned")
            .hold_pending(std::mem::replace(held, now), now);
    };
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        {
            let mut output = output.lock().expect("output collector poisoned");
            output.written = output.written.saturating_add(count as u64);
        }
        for part in buffer[..count].split_inclusive(|byte| *byte == b'\n') {
            let overflow = (pending.len() + part.len()).saturating_sub(MAX_LOGICAL_LINE_BYTES);
            if overflow > 0 {
                // A part is never longer than a read, so it always fits.
                pending.drain(..overflow);
                oversized = true;
                head_lost = true;
            }
            pending.extend_from_slice(part);
            hold(&mut held, pending.len());
            if part.last() == Some(&b'\n') {
                let line = std::mem::take(&mut pending);
                hold(&mut held, 0);
                if std::mem::take(&mut oversized) {
                    capture_oversized_line(&line, std::mem::take(&mut head_lost), &output, &policy);
                } else {
                    capture_line(line, &output, &policy).await;
                }
            }
        }
    }
    hold(&mut held, 0);
    if oversized {
        capture_oversized_line(&pending, head_lost, &output, &policy);
    } else if !pending.is_empty() {
        capture_line(pending, &output, &policy).await;
    }
    Ok(())
}

/// Keep the tail of a line too long to read for directives. Its head is gone,
/// so it is never taken as a directive, only kept as output.
fn capture_oversized_line(
    line: &[u8],
    head_lost: bool,
    output: &Arc<Mutex<BoundedOutput>>,
    policy: &CapturePolicy,
) {
    let tail = redacted_tail(line, head_lost, BoundedOutput::CAPACITY, &policy.secrets);
    if let Some(tap) = &policy.tap {
        tap(&tail);
    }
    let mut output = output.lock().expect("output collector poisoned");
    output.truncated = true;
    output.push(tail);
}

/// The last `keep` bytes of `line`, redacted. A secret the cut would go
/// through is replaced whole, so no part of it is kept. When the line's head
/// was already dropped (`head_lost`), a secret may have begun before what is
/// held: if what is held begins with the end of a secret that reaches past the
/// cut, that end is treated as the secret, at the cost of sometimes replacing
/// a few bytes that only looked like one.
fn redacted_tail(line: &[u8], head_lost: bool, keep: usize, secrets: &[Vec<u8>]) -> Vec<u8> {
    let patterns = redaction_patterns(secrets);
    let mut cut = line.len().saturating_sub(keep);
    let mut straddled = false;
    loop {
        let begun_before = patterns.iter().filter(|_| head_lost).filter_map(|secret| {
            (1..secret.len())
                .map(|start| &secret[start..])
                .find(|rest| rest.len() > cut && line.starts_with(rest))
                .map(<[u8]>::len)
        });
        let reach = patterns
            .iter()
            .filter_map(|secret| {
                let first = cut.saturating_sub(secret.len() - 1);
                let last = (cut + secret.len() - 1).min(line.len());
                line.get(first..last)?
                    .windows(secret.len())
                    .rposition(|candidate| candidate == *secret)
                    .map(|offset| first + offset + secret.len())
                    .filter(|end| *end > cut)
            })
            .chain(begun_before)
            .max();
        match reach {
            Some(end) => {
                cut = end;
                straddled = true;
            }
            None => break,
        }
    }
    let mut tail = if straddled {
        REDACTED.to_vec()
    } else {
        Vec::new()
    };
    tail.extend(redact_bytes(&line[cut..], secrets));
    tail
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
        if let Some(tap) = &policy.tap {
            tap(&tail);
        }
        output.lock().expect("output collector poisoned").push(tail);
    }
    if let (Some(sender), Some(event)) = (&policy.events, event) {
        let _ = sender.send(event).await;
    }
}

/// One reading of an exit status for every script and every trigger, so a
/// script may use whichever convention it was written for: 0 and NZBGet's 92
/// and 93 are success, 95 is "nothing to do", and anything else is a failure.
fn exit_disposition(exit_code: Option<i32>) -> ExecutionDisposition {
    match exit_code {
        Some(0 | 92 | 93) => ExecutionDisposition::Succeeded,
        Some(95) => ExecutionDisposition::Skipped,
        _ => ExecutionDisposition::Failed,
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
    let go_run = request.go_run;
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
    let stderr_tail = stderr_thread.join().unwrap_or_default();
    let code = status.code().unwrap_or(126);
    Ok(if go_run {
        go_run_exit_code(code, &stderr_tail)
    } else {
        code
    })
}

/// How much of the end of a relayed stream is kept for [`go_run_exit_code`].
const RELAY_TAIL_BYTES: usize = 64;

/// `go run` exits 1 when the program it built exits with anything but zero,
/// and says which status that was in the last line it writes to stderr,
/// `exit status N`. Read the script's own status back out of that line, so 93
/// or 95 from a Go script means what it means from any other.
fn go_run_exit_code(code: i32, stderr_tail: &[u8]) -> i32 {
    if code != 1 {
        return code;
    }
    String::from_utf8_lossy(stderr_tail)
        .trim_end()
        .rsplit_once("exit status ")
        .and_then(|(_, status)| status.parse().ok())
        .unwrap_or(code)
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
) -> std::thread::JoinHandle<Vec<u8>>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 16 * 1024];
        // The end of what was relayed, for a caller that reads the last line.
        let mut tail = Vec::new();
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
            tail.extend_from_slice(&buffer[..count]);
            let excess = tail.len().saturating_sub(RELAY_TAIL_BYTES);
            tail.drain(..excess);
        }
        tail
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
    adapter_contract(request)
}

/// The arguments and environment the adapter hands a post-processing script,
/// without the platform environment it inherits.
pub(crate) fn adapter_contract(
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
pub(crate) fn exit_disposition_for_test(exit_code: Option<i32>) -> ExecutionDisposition {
    exit_disposition(exit_code)
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
    async fn capture_without_event_policy_retains_redacted_lines_without_structured_events() {
        let output = Arc::new(Mutex::new(BoundedOutput::default()));
        let (sender, mut receiver) = mpsc::channel(4);
        let policy = CapturePolicy {
            secrets: Arc::new(vec![b"private-value".to_vec()]),
            event: None,
            events: Some(sender),
            tap: None,
        };
        let input = b"ordinary private-value\r\n[INFO] private-value\n[NZB] NZBPR_Token=private-value\nunterminated private-value";
        capture_stream(input.as_slice(), output.clone(), policy)
            .await
            .unwrap();
        assert!(receiver.recv().await.is_none());
        let captured = Arc::try_unwrap(output).ok().unwrap().into_inner().unwrap();
        assert!(!captured.truncated);
        assert_eq!(
            String::from_utf8(captured.into_bytes()).unwrap(),
            "ordinary [REDACTED]\r\n[INFO] [REDACTED]\n[NZB] NZBPR_Token=[REDACTED]\nunterminated [REDACTED]"
        );
    }

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
            tap: None,
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
            tap: None,
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
    async fn capture_redacts_before_directives_and_keeps_only_the_tail_of_oversized_lines() {
        let output = Arc::new(Mutex::new(BoundedOutput::default()));
        let (sender, mut receiver) = mpsc::channel(8);
        let policy = CapturePolicy {
            secrets: Arc::new(vec![b"sensitive-value".to_vec()]),
            event: Some(ScriptEventLabel::Scan),
            events: Some(sender),
            tap: None,
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
        assert!(
            text.ends_with("xxxx\n"),
            "the line's tail is kept as output"
        );
    }

    /// Run `input` through capture without directive reading.
    async fn capture(input: &[u8], secrets: Vec<Vec<u8>>) -> (Vec<u8>, bool, u64) {
        let output = Arc::new(Mutex::new(BoundedOutput::default()));
        let policy = CapturePolicy {
            secrets: Arc::new(secrets),
            ..Default::default()
        };
        capture_stream(input, output.clone(), policy).await.unwrap();
        let captured = Arc::try_unwrap(output).ok().unwrap().into_inner().unwrap();
        let (truncated, written) = (captured.truncated, captured.written);
        (captured.into_bytes(), truncated, written)
    }

    const CAP: usize = MAX_SCRIPT_OUTPUT_BYTES as usize;

    fn tail(input: &[u8]) -> &[u8] {
        &input[input.len().saturating_sub(CAP)..]
    }

    #[tokio::test]
    async fn many_short_lines_keep_exactly_the_last_32_kib() {
        let input = (0..10_000)
            .flat_map(|i| format!("line {i}\n").into_bytes())
            .collect::<Vec<_>>();
        let (kept, truncated, written) = capture(&input, Vec::new()).await;
        assert!(truncated);
        assert_eq!(kept.len(), CAP);
        assert_eq!(kept, tail(&input));
        assert_eq!(written, input.len() as u64);
    }

    #[tokio::test]
    async fn a_line_larger_than_the_ring_keeps_its_last_32_kib() {
        // Larger than the ring but still read whole for directives.
        let mut input = b"head-marker ".to_vec();
        input.extend((0..36 * 1024).map(|i| b'a' + (i % 26) as u8));
        input.push(b'\n');
        assert!(input.len() <= MAX_LOGICAL_LINE_BYTES);
        let (kept, truncated, written) = capture(&input, Vec::new()).await;
        assert!(truncated);
        assert_eq!(kept, tail(&input));
        assert_eq!(written, input.len() as u64);

        // Past the directive limit, so held only as its tail while it arrives.
        let mut input = b"head-marker ".to_vec();
        input.extend((0..300 * 1024).map(|i| b'a' + (i % 26) as u8));
        input.extend_from_slice(b" end\n");
        let (kept, truncated, written) = capture(&input, Vec::new()).await;
        assert!(truncated);
        assert_eq!(kept, tail(&input));
        assert_eq!(written, input.len() as u64);
    }

    #[tokio::test]
    async fn a_stream_without_a_trailing_newline_keeps_its_tail() {
        let mut input = (0..5_000)
            .flat_map(|i| format!("row {i}\n").into_bytes())
            .collect::<Vec<_>>();
        input.extend_from_slice(b"unterminated last words");
        let (kept, truncated, _) = capture(&input, Vec::new()).await;
        assert!(truncated);
        assert_eq!(kept, tail(&input));
        assert!(kept.ends_with(b"unterminated last words"));

        let mut input = b"x".repeat(100 * 1024);
        input.extend_from_slice(b"no newline at all");
        let (kept, truncated, _) = capture(&input, Vec::new()).await;
        assert!(truncated);
        assert_eq!(kept, tail(&input));
    }

    #[tokio::test]
    async fn a_character_straddling_the_cut_is_dropped_whole() {
        // A two- and a three-byte character, each with its second byte the
        // first of the last 32 KiB.
        for prefix in ["\u{e9}".as_bytes().to_vec(), "\u{20ac}".as_bytes().to_vec()] {
            let mut input = b"older\n".to_vec();
            input.extend_from_slice(&prefix);
            input.extend(std::iter::repeat_n(b'z', CAP - prefix.len()));
            input.push(b'\n');
            let (kept, truncated, _) = capture(&input, Vec::new()).await;
            assert!(truncated);
            let text = String::from_utf8(kept.clone()).expect("the cut is on a boundary");
            assert!(!text.contains('\u{fffd}'));
            assert_eq!(kept, &input[input.len() - CAP + prefix.len() - 1..]);
            assert!(kept.len() < CAP);
        }

        // The same holds for a line held only as its tail.
        let mut input = "\u{20ac}".repeat(40 * 1024).into_bytes();
        input.push(b'\n');
        let (kept, truncated, _) = capture(&input, Vec::new()).await;
        assert!(truncated);
        let text = String::from_utf8(kept.clone()).expect("the cut is on a boundary");
        assert!(text.chars().all(|c| c == '\u{20ac}' || c == '\n'));
        assert!(tail(&input).ends_with(&kept));
    }

    #[tokio::test]
    async fn output_of_exactly_32_kib_is_kept_whole() {
        let mut input = b"q".repeat(CAP - 1);
        input.push(b'\n');
        let (kept, truncated, written) = capture(&input, Vec::new()).await;
        assert!(!truncated, "nothing was dropped");
        assert_eq!(kept, input);
        assert_eq!(written, CAP as u64);

        input.push(b'!');
        let (kept, truncated, _) = capture(&input, Vec::new()).await;
        assert!(truncated, "one byte was dropped");
        assert_eq!(kept, &input[1..]);
    }

    #[tokio::test]
    async fn stdout_and_stderr_share_one_ring_in_arrival_order() {
        let output = Arc::new(Mutex::new(BoundedOutput::default()));
        // stdout, stderr, stdout: each stream's capture writes the one ring.
        for text in ["first\n", "second\n", "third\n"] {
            capture_stream(text.as_bytes(), output.clone(), CapturePolicy::default())
                .await
                .unwrap();
        }
        let captured = Arc::try_unwrap(output).ok().unwrap().into_inner().unwrap();
        assert_eq!(captured.into_bytes(), b"first\nsecond\nthird\n");
    }

    #[tokio::test]
    async fn a_secret_cut_by_the_tail_of_an_oversized_line_is_not_kept_in_part() {
        let secret = b"very-secret-token-value".to_vec();
        // Place the secret so the last 32 KiB begins in its middle.
        let mut input = b"y".repeat(100 * 1024);
        input.extend_from_slice(&secret);
        input.extend(std::iter::repeat_n(b'y', CAP - 5));
        input.push(b'\n');
        let (kept, truncated, _) = capture(&input, vec![secret.clone()]).await;
        assert!(truncated);
        assert_eq!(kept.len(), CAP);
        // Cut at the same place without redaction, the kept bytes would begin
        // with the secret's last four.
        assert!(tail(&input).starts_with(b"alue"));
        let text = String::from_utf8(kept).unwrap();
        assert!(!text.contains("alue"), "{}", &text[..16]);
        assert!(text.ends_with("yyy\n"));
    }

    /// Feed `input` to capture through a pipe and return the most the ring
    /// and the unfinished line ever held at once.
    async fn peak_held(input: Vec<u8>) -> (usize, Vec<u8>) {
        let output = Arc::new(Mutex::new(BoundedOutput::default()));
        let (mut writer, reader) = tokio::io::duplex(READ_CHUNK_BYTES);
        let feed = tokio::spawn(async move {
            writer.write_all(&input).await.unwrap();
            input
        });
        capture_stream(reader, output.clone(), CapturePolicy::default())
            .await
            .unwrap();
        let input = feed.await.unwrap();
        let captured = Arc::try_unwrap(output).ok().unwrap().into_inner().unwrap();
        assert_eq!(captured.pending, 0, "nothing is left unaccounted");
        assert_eq!(captured.written, input.len() as u64);
        let peak = captured.peak;
        assert_eq!(captured.into_bytes(), tail(&input));
        (peak, input)
    }

    #[tokio::test]
    async fn megabytes_of_output_never_hold_more_than_the_ring_and_one_read() {
        let bound = CAP + READ_CHUNK_BYTES;
        let lines = (0..400_000)
            .flat_map(|i| format!("progress {i} of many\n").into_bytes())
            .collect::<Vec<_>>();
        assert!(lines.len() > 4 * 1024 * 1024);
        let (peak, _) = peak_held(lines).await;
        assert!(peak <= bound, "held {peak} bytes");
        assert!(peak >= CAP, "the ring was full");

        let unbroken = (0..6 * 1024 * 1024)
            .map(|i| b'a' + (i % 26) as u8)
            .collect::<Vec<_>>();
        let (peak, _) = peak_held(unbroken).await;
        assert!(peak <= bound, "held {peak} bytes");

        // Long lines that each need more than one read, then short ones.
        let mut mixed = Vec::new();
        for i in 0..200 {
            mixed.extend(std::iter::repeat_n(b'm', 20_000 + i * 97));
            mixed.push(b'\n');
            mixed.extend_from_slice(b"short\n");
        }
        let (peak, _) = peak_held(mixed).await;
        assert!(peak <= bound, "held {peak} bytes");
    }

    #[tokio::test]
    async fn a_secret_longer_than_a_read_cut_by_the_tail_is_not_kept_in_part() {
        let secret = (0..12 * 1024)
            .map(|i| b'A' + (i % 26) as u8)
            .collect::<Vec<_>>();
        // Its first bytes fall before what the capture still holds, and its
        // last 100 are the first of the last 32 KiB.
        let mut input = b"z".repeat(100 * 1024);
        input.extend_from_slice(&secret);
        input.extend(std::iter::repeat_n(b'z', CAP - 101));
        input.push(b'\n');
        assert_eq!(&tail(&input)[..100], &secret[secret.len() - 100..]);
        let (kept, truncated, _) = capture(&input, vec![secret.clone()]).await;
        assert!(truncated);
        assert!(kept.len() <= CAP);
        assert!(kept.starts_with(REDACTED));
        assert!(
            kept[REDACTED.len()..]
                .iter()
                .all(|byte| !byte.is_ascii_uppercase()),
            "no byte of the secret is kept"
        );
    }

    #[test]
    fn the_ring_never_holds_more_than_it_hands_over() {
        let mut ring = BoundedOutput::default();
        for size in [1, CAP - 1, 7, CAP + 9, 3] {
            ring.push(vec![b'k'; size]);
            assert!(ring.ring.len() <= CAP);
        }
        assert!(ring.truncated);
        assert_eq!(ring.into_bytes().len(), CAP);
    }
}

#[cfg(test)]
mod go_run_tests {
    use super::*;

    fn text_env(env: BTreeMap<OsStringWire, OsStringWire>) -> BTreeMap<String, String> {
        env.into_iter()
            .map(|(key, value)| (key.0, value.0))
            .collect()
    }

    #[test]
    fn a_go_source_file_is_handed_to_go_run() {
        let entrypoint = Path::new("/scripts/Report.GO");
        assert!(is_go_source(entrypoint));
        assert!(!is_go_source(Path::new("/scripts/go")));
        let (program, args) = resolve_program(entrypoint, &InterpreterConfig::default()).unwrap();
        assert_eq!(program, Path::new("go"));
        assert_eq!(args, ["run", "/scripts/Report.GO"]);

        let configured = InterpreterConfig {
            go: Some(PathBuf::from("/opt/go/bin/go")),
            ..InterpreterConfig::default()
        };
        let (program, args) = resolve_program(entrypoint, &configured).unwrap();
        assert_eq!(program, Path::new("/opt/go/bin/go"));
        assert_eq!(args, ["run", "/scripts/Report.GO"]);
    }

    #[test]
    fn go_run_builds_into_a_cache_under_the_data_directory_and_stays_off_the_network() {
        let data = tempfile::tempdir().unwrap();
        let mut env = BTreeMap::new();
        let facts = CompatibilityFacts {
            data_dir: Some(data.path().into()),
            ..CompatibilityFacts::default()
        };
        insert_go_environment(&mut env, &facts).unwrap();
        let env = text_env(env);
        assert_eq!(
            Path::new(&env["GOCACHE"]),
            data.path().join(".weaver-go-cache")
        );
        assert_eq!(env["GOPROXY"], "off");
        assert_eq!(env.len(), 2);

        // Go refuses a cache path that is not absolute.
        let mut env = BTreeMap::new();
        let facts = CompatibilityFacts {
            data_dir: Some(PathBuf::from("data")),
            ..CompatibilityFacts::default()
        };
        insert_go_environment(&mut env, &facts).unwrap();
        let cache = PathBuf::from(&text_env(env)["GOCACHE"]);
        assert!(cache.is_absolute());
        assert!(cache.ends_with("data/.weaver-go-cache"));

        // Without a data directory Go is left to find its own cache.
        let mut env = BTreeMap::new();
        insert_go_environment(&mut env, &CompatibilityFacts::default()).unwrap();
        assert_eq!(
            text_env(env),
            BTreeMap::from([("GOPROXY".to_string(), "off".to_string())])
        );
    }

    #[test]
    fn the_status_go_run_reports_for_the_script_becomes_the_exit_code() {
        assert_eq!(go_run_exit_code(1, b"exit status 93\n"), 93);
        assert_eq!(
            go_run_exit_code(1, b"[INFO] done\r\nexit status 95\r\n"),
            95
        );
        // The script ended its own stderr without a line break.
        assert_eq!(go_run_exit_code(1, b"no line breakexit status 2\n"), 2);
        assert_eq!(go_run_exit_code(1, b"exit status 93\nexit status 1\n"), 1);
        // A build that failed reports no status of the script's.
        assert_eq!(go_run_exit_code(1, b"./x.go:3:15: undefined: nothing\n"), 1);
        assert_eq!(go_run_exit_code(1, b"exit status 93 or so\n"), 1);
        assert_eq!(go_run_exit_code(1, b""), 1);
        // Only the status `go run` itself fails with is read this way.
        assert_eq!(go_run_exit_code(0, b"exit status 93\n"), 0);
        assert_eq!(go_run_exit_code(2, b"exit status 93\n"), 2);
    }

    #[test]
    fn a_relay_keeps_the_end_of_what_it_passed_on() {
        let lost = Arc::new(AtomicBool::new(false));
        let mut input = vec![b'x'; 40 * 1024];
        input.extend_from_slice(b"\nexit status 93\n");
        let tail = relay_thread(io::Cursor::new(input), io::sink(), lost.clone())
            .join()
            .unwrap();
        assert_eq!(tail.len(), RELAY_TAIL_BYTES);
        assert_eq!(go_run_exit_code(1, &tail), 93);

        let tail = relay_thread(io::Cursor::new(b"short".to_vec()), io::sink(), lost.clone())
            .join()
            .unwrap();
        assert_eq!(tail, b"short");
        assert!(!lost.load(Ordering::Acquire));
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
