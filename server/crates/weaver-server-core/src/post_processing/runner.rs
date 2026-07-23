use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::watch;

use super::model::{
    ApprovedFilesystemRoot, ExtensionAdapter, ExtensionManifest, PipelineOutcome, ResolvedOption,
    ResolvedOptionValue, TimeoutPolicy,
};
use super::persistence::{LogStream, MAX_LOGICAL_LINE_BYTES, MAX_PERSISTED_ATTEMPT_OUTPUT_BYTES};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_TERMINATION_GRACE: Duration = Duration::from_secs(10);
const SUPERVISOR_ARG: &str = "__post-processing-supervisor";
const MAX_SUPERVISOR_REQUEST_BYTES: u64 = 2 * 1024 * 1024;
const MAX_WEBHOOK_RETRIES: u32 = 10;
const MAX_CONTROL_OUTPUT_BYTES: u64 = 4 * 1024 * 1024;

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
pub struct ExtensionExecutionRequest {
    pub attempt_id: String,
    pub manifest: ExtensionManifest,
    pub managed_path: PathBuf,
    pub options: Vec<ResolvedOption>,
    pub approved_roots: Vec<ApprovedFilesystemRoot>,
    pub context: JobExecutionContext,
    pub timeout_policy: TimeoutPolicy,
    pub termination_grace: Duration,
    pub interpreters: InterpreterConfig,
    pub control_token: Option<String>,
    pub diagnostic_command: Option<String>,
    #[doc(hidden)]
    pub supervisor_executable: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ExecutionDisposition {
    Succeeded,
    Skipped,
    Failed,
    RepairRequested,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct ControlEffects {
    pub directory: Option<PathBuf>,
    pub final_directory: Option<PathBuf>,
    pub parameters: BTreeMap<String, String>,
    pub mark_bad: bool,
    pub repair_requested: bool,
    pub progress: Option<serde_json::Value>,
    pub metadata: BTreeMap<String, serde_json::Value>,
    pub artifacts: Vec<PathBuf>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CapturedOutputLine {
    pub sequence: u64,
    pub stream: LogStream,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExtensionExecutionResult {
    pub disposition: ExecutionDisposition,
    pub exit_code: Option<i32>,
    pub output: Vec<CapturedOutputLine>,
    pub output_truncated: bool,
    pub effects: ControlEffects,
    pub error_message: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("managed extension entrypoint is unavailable or unsafe")]
    InvalidEntrypoint,
    #[error("managed extension package no longer matches its approved digest")]
    UntrustedPackage,
    #[error("extension interpreter is unavailable: {0}")]
    MissingInterpreter(&'static str),
    #[error("extension environment value is invalid")]
    InvalidEnvironment,
    #[error("extension timeout is too large for this platform")]
    InvalidTimeout,
    #[error("extension control command requested a path outside approved roots")]
    UnapprovedPath,
    #[error("extension control effect contains sensitive data")]
    SensitiveControlEffect,
    #[error("this extension adapter does not support manifest diagnostics")]
    UnsupportedDiagnosticAdapter,
    #[error("webhook extension configuration is invalid: {0}")]
    InvalidWebhookConfiguration(String),
    #[error("webhook extension request failed: {0}")]
    WebhookRequest(String),
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

struct PreparedExecution {
    supervisor_executable: Option<PathBuf>,
    supervisor: SupervisorRequest,
    adapter: ExtensionAdapter,
    roots: Vec<PathBuf>,
}

pub async fn execute_extension(
    request: ExtensionExecutionRequest,
    cancellation: Option<watch::Receiver<bool>>,
) -> Result<ExtensionExecutionResult, RunnerError> {
    execute_extension_inner(request, cancellation, None).await
}

pub async fn execute_extension_with_spawn_callback<F>(
    request: ExtensionExecutionRequest,
    cancellation: Option<watch::Receiver<bool>>,
    on_spawn: F,
) -> Result<ExtensionExecutionResult, RunnerError>
where
    F: FnOnce() -> Result<(), RunnerError> + Send + 'static,
{
    execute_extension_inner(request, cancellation, Some(Box::new(on_spawn))).await
}

async fn execute_extension_inner(
    request: ExtensionExecutionRequest,
    cancellation: Option<watch::Receiver<bool>>,
    on_spawn: Option<Box<dyn FnOnce() -> Result<(), RunnerError> + Send>>,
) -> Result<ExtensionExecutionResult, RunnerError> {
    super::discovery::verify_managed_extension(
        &request.managed_path,
        request.manifest.revision().digest(),
    )
    .map_err(|_| RunnerError::UntrustedPackage)?;
    let attempt_id = request.attempt_id.clone();
    let mut secrets = request
        .options
        .iter()
        .filter_map(|option| match option.value() {
            ResolvedOptionValue::Secret(value) if !value.expose_for_execution().is_empty() => {
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
    if let Some(token) = request
        .control_token
        .as_deref()
        .filter(|token| !token.is_empty())
    {
        secrets.push(token.as_bytes().to_vec());
    }
    let extension_id = request
        .manifest
        .revision()
        .extension_id()
        .as_str()
        .to_string();
    let timeout = timeout_duration(request.timeout_policy);
    let grace = if request.termination_grace.is_zero() {
        DEFAULT_TERMINATION_GRACE
    } else {
        request.termination_grace
    };
    if timeout.is_some_and(|duration| Instant::now().checked_add(duration).is_none())
        || Instant::now().checked_add(grace).is_none()
    {
        return Err(RunnerError::InvalidTimeout);
    }
    let adapter = request.manifest.adapter();
    let prepared = (adapter != ExtensionAdapter::Webhook)
        .then(|| prepare_execution(&request))
        .transpose()?;
    tracing::info!(
        attempt_id,
        extension_id,
        adapter = ?adapter,
        timeout_seconds = timeout.map(|duration| duration.as_secs()),
        "starting post-processing attempt"
    );
    let started = Instant::now();
    let mut result = if let Some(prepared) = prepared {
        execute_supervised(prepared, timeout, grace, cancellation, on_spawn).await
    } else {
        execute_webhook(&request, timeout, cancellation, on_spawn).await
    };
    result = result
        .and_then(|mut result| {
            redact_execution_result(&mut result, &secrets)?;
            Ok(result)
        })
        .map_err(|error| redact_runner_error(error, &secrets));
    match &result {
        Ok(result) => tracing::info!(
            attempt_id,
            extension_id,
            result = ?result.disposition,
            exit_code = result.exit_code,
            duration_ms = started.elapsed().as_millis() as u64,
            output_truncated = result.output_truncated,
            "post-processing attempt finished"
        ),
        Err(error) => tracing::info!(
            attempt_id,
            extension_id,
            error = %error,
            duration_ms = started.elapsed().as_millis() as u64,
            "post-processing attempt could not run"
        ),
    }
    result
}

fn prepare_execution(
    request: &ExtensionExecutionRequest,
) -> Result<PreparedExecution, RunnerError> {
    let managed = fs::canonicalize(&request.managed_path)?;
    let entrypoint = fs::canonicalize(managed.join(request.manifest.entrypoint()))?;
    if !entrypoint.starts_with(&managed) || !entrypoint.is_file() {
        return Err(RunnerError::InvalidEntrypoint);
    }

    let (program, mut args) = resolve_program(&entrypoint, &request.interpreters)?;
    let mut env = sanitized_platform_environment()?;
    let adapter_args = adapter_environment_and_args(request, &mut env)?;
    args.extend(adapter_args);
    if let Some(token) = &request.control_token {
        insert_env(&mut env, "WEAVER_PP_CONTROL_TOKEN", token)?;
    }
    insert_env(
        &mut env,
        "WEAVER_PP_ATTEMPT_ID",
        request.attempt_id.as_str(),
    )?;

    let working_directory = fs::canonicalize(&request.context.working_directory)?;
    let final_directory = fs::canonicalize(&request.context.final_directory)?;
    let mut roots = vec![working_directory.clone(), final_directory];
    for root in &request.approved_roots {
        match fs::canonicalize(root.as_str()) {
            Ok(root) => roots.push(root),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    roots.sort();
    roots.dedup();

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
        roots,
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

fn native_context(request: &ExtensionExecutionRequest) -> serde_json::Value {
    let context = &request.context;
    serde_json::json!({
        "schemaVersion": 1,
        "attemptId": request.attempt_id,
        "command": request.diagnostic_command.as_deref(),
        "job": {
            "id": context.job_id,
            "name": context.name,
            "nzbFilename": context.nzb_filename,
            "category": context.category,
            "workingDirectory": context.working_directory,
            "finalDirectory": context.final_directory,
        },
        "pipelineOutcome": context.pipeline_outcome,
        "options": option_object(&request.options),
    })
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
    request: &ExtensionExecutionRequest,
    env: &mut BTreeMap<OsStringWire, OsStringWire>,
) -> Result<Vec<OsString>, RunnerError> {
    let context = &request.context;
    match request.manifest.adapter() {
        ExtensionAdapter::Native => {
            if let Some(command) = request.diagnostic_command.as_deref() {
                insert_env(env, "WEAVER_PP_COMMAND", command)?;
            }
            insert_env(
                env,
                "WEAVER_PP_CONTEXT",
                &serde_json::to_string(&native_context(request))
                    .map_err(|error| RunnerError::SupervisorProtocol(error.to_string()))?,
            )?;
            Ok(vec![])
        }
        ExtensionAdapter::Sabnzbd => {
            if request.diagnostic_command.is_some() {
                return Err(RunnerError::UnsupportedDiagnosticAdapter);
            }
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
        ExtensionAdapter::Nzbget => {
            if let Some(command) = request.diagnostic_command.as_deref() {
                insert_env(env, "NZBCP_COMMAND", command)?;
                insert_compat_options(env, "NZBPO", &request.options)?;
                insert_nzbget_global_options(env, &context.compatibility)?;
                return Ok(vec![]);
            }
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
        ExtensionAdapter::Webhook => Err(RunnerError::SupervisorProtocol(
            "adapter used the wrong execution path".into(),
        )),
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

fn option_object(options: &[ResolvedOption]) -> serde_json::Map<String, serde_json::Value> {
    options
        .iter()
        .map(|option| {
            (
                option.name().as_str().to_string(),
                option_value_json(option.value()),
            )
        })
        .collect()
}

fn resolved_option<'a>(
    request: &'a ExtensionExecutionRequest,
    name: &str,
) -> Option<&'a ResolvedOptionValue> {
    request
        .options
        .iter()
        .find(|option| option.name().as_str().eq_ignore_ascii_case(name))
        .map(ResolvedOption::value)
}

fn option_value_text(value: &ResolvedOptionValue) -> String {
    match value {
        ResolvedOptionValue::String(value) => value.clone(),
        ResolvedOptionValue::Integer(value) => value.to_string(),
        ResolvedOptionValue::Number(value) => value.to_string(),
        ResolvedOptionValue::Boolean(value) => if *value { "yes" } else { "no" }.to_string(),
        ResolvedOptionValue::Secret(value) => value.expose_for_execution().to_string(),
    }
}

fn option_value_json(value: &ResolvedOptionValue) -> serde_json::Value {
    match value {
        ResolvedOptionValue::String(value) => serde_json::Value::String(value.clone()),
        ResolvedOptionValue::Integer(value) => (*value).into(),
        ResolvedOptionValue::Number(value) => serde_json::Value::Number(value.clone()),
        ResolvedOptionValue::Boolean(value) => (*value).into(),
        ResolvedOptionValue::Secret(value) => {
            serde_json::Value::String(value.expose_for_execution().to_string())
        }
    }
}

fn redact_execution_result(
    result: &mut ExtensionExecutionResult,
    secrets: &[Vec<u8>],
) -> Result<(), RunnerError> {
    ensure_effect_paths_are_not_sensitive(&result.effects, secrets)?;
    redact_output_lines(&mut result.output, secrets);
    redact_string_map(&mut result.effects.parameters, secrets);
    if let Some(progress) = &mut result.effects.progress {
        redact_json(progress, secrets);
    }
    let metadata = std::mem::take(&mut result.effects.metadata);
    for (key, mut value) in metadata {
        redact_json(&mut value, secrets);
        result
            .effects
            .metadata
            .insert(redact_string(&key, secrets), value);
    }
    if let Some(message) = &mut result.error_message {
        *message = redact_string(message, secrets);
    }
    Ok(())
}

fn ensure_effect_paths_are_not_sensitive(
    effects: &ControlEffects,
    secrets: &[Vec<u8>],
) -> Result<(), RunnerError> {
    let sensitive = effects
        .directory
        .iter()
        .chain(effects.final_directory.iter())
        .chain(effects.artifacts.iter())
        .any(|path| contains_secret(path.to_string_lossy().as_bytes(), secrets));
    if sensitive {
        return Err(RunnerError::SensitiveControlEffect);
    }
    Ok(())
}

fn redact_output_lines(lines: &mut [CapturedOutputLine], secrets: &[Vec<u8>]) {
    for stream in [LogStream::Stdout, LogStream::Stderr, LogStream::System] {
        let indexes = lines
            .iter()
            .enumerate()
            .filter_map(|(index, line)| (line.stream == stream).then_some(index))
            .collect::<Vec<_>>();
        let combined = indexes
            .iter()
            .flat_map(|index| lines[*index].bytes.iter().copied())
            .collect::<Vec<_>>();
        let mut mask = vec![false; combined.len()];
        for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
            let mut cursor = 0;
            while cursor + secret.len() <= combined.len() {
                let Some(offset) = combined[cursor..]
                    .windows(secret.len())
                    .position(|candidate| candidate == secret.as_slice())
                else {
                    break;
                };
                let start = cursor + offset;
                mask[start..start + secret.len()].fill(true);
                cursor = start + 1;
            }
        }
        let mut offset = 0;
        for index in indexes {
            let length = lines[index].bytes.len();
            lines[index].bytes =
                redact_masked_bytes(&lines[index].bytes, &mask[offset..offset + length]);
            offset += length;
        }
    }
}

fn redact_masked_bytes(input: &[u8], mask: &[bool]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    let mut cursor = 0;
    while cursor < input.len() {
        if !mask[cursor] {
            output.push(input[cursor]);
            cursor += 1;
            continue;
        }
        output.extend_from_slice(b"[REDACTED]");
        while cursor < input.len() && mask[cursor] {
            cursor += 1;
        }
    }
    output
}

fn redact_string_map(values: &mut BTreeMap<String, String>, secrets: &[Vec<u8>]) {
    let original = std::mem::take(values);
    for (key, value) in original {
        values.insert(redact_string(&key, secrets), redact_string(&value, secrets));
    }
}

fn contains_secret(input: &[u8], secrets: &[Vec<u8>]) -> bool {
    secrets
        .iter()
        .filter(|secret| !secret.is_empty())
        .any(|secret| {
            input
                .windows(secret.len())
                .any(|candidate| candidate == secret.as_slice())
        })
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

fn redact_json(value: &mut serde_json::Value, secrets: &[Vec<u8>]) {
    match value {
        serde_json::Value::String(value) => *value = redact_string(value, secrets),
        serde_json::Value::Array(values) => {
            values
                .iter_mut()
                .for_each(|value| redact_json(value, secrets));
        }
        serde_json::Value::Object(values) => {
            let original = std::mem::take(values);
            for (key, mut value) in original {
                redact_json(&mut value, secrets);
                values.insert(redact_string(&key, secrets), value);
            }
        }
        _ => {}
    }
}

fn redact_runner_error(error: RunnerError, secrets: &[Vec<u8>]) -> RunnerError {
    if secrets.is_empty() {
        return error;
    }
    match error {
        RunnerError::InvalidWebhookConfiguration(message) => {
            RunnerError::InvalidWebhookConfiguration(redact_string(&message, secrets))
        }
        RunnerError::WebhookRequest(message) => {
            RunnerError::WebhookRequest(redact_string(&message, secrets))
        }
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

fn timeout_duration(policy: TimeoutPolicy) -> Option<Duration> {
    match policy {
        TimeoutPolicy::Default24Hours => Some(DEFAULT_TIMEOUT),
        TimeoutPolicy::Finite(seconds) => Some(Duration::from_secs(seconds.get())),
        TimeoutPolicy::Unlimited => None,
    }
}

async fn execute_webhook(
    request: &ExtensionExecutionRequest,
    timeout: Option<Duration>,
    cancellation: Option<watch::Receiver<bool>>,
    on_spawn: Option<Box<dyn FnOnce() -> Result<(), RunnerError> + Send>>,
) -> Result<ExtensionExecutionResult, RunnerError> {
    let operation = execute_webhook_with_retries(request, cancellation, on_spawn);
    if let Some(timeout) = timeout {
        match tokio::time::timeout(timeout, operation).await {
            Ok(result) => result,
            Err(_) => Ok(terminal_webhook_result(
                ExecutionDisposition::TimedOut,
                None,
                Some("webhook attempt timed out".into()),
            )),
        }
    } else {
        operation.await
    }
}

async fn execute_webhook_with_retries(
    request: &ExtensionExecutionRequest,
    mut cancellation: Option<watch::Receiver<bool>>,
    on_spawn: Option<Box<dyn FnOnce() -> Result<(), RunnerError> + Send>>,
) -> Result<ExtensionExecutionResult, RunnerError> {
    let url = webhook_url(request)?;
    let retries = webhook_retries(request)?;
    let payload = webhook_payload(request);
    let body = serde_json::to_vec(&payload)
        .map_err(|error| RunnerError::InvalidWebhookConfiguration(error.to_string()))?;
    let hmac_signature = webhook_secret(request, "webhook_hmac_secret")
        .map(|secret| {
            let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_| {
                RunnerError::InvalidWebhookConfiguration("invalid HMAC secret".into())
            })?;
            mac.update(&body);
            Ok::<_, RunnerError>(format!(
                "sha256={}",
                hex::encode(mac.finalize().into_bytes())
            ))
        })
        .transpose()?;
    let bearer = webhook_secret(request, "webhook_bearer_token");
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| RunnerError::WebhookRequest("could not build HTTP client".into()))?;

    if let Some(on_spawn) = on_spawn {
        on_spawn()?;
    }
    if cancellation
        .as_ref()
        .is_some_and(|receiver| *receiver.borrow())
    {
        return Ok(terminal_webhook_result(
            ExecutionDisposition::Cancelled,
            None,
            Some("webhook attempt cancelled".into()),
        ));
    }

    for retry in 0..=retries {
        let mut builder = client
            .post(url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", &request.attempt_id)
            .header("X-Weaver-Attempt-ID", &request.attempt_id)
            .body(body.clone());
        if let Some(signature) = &hmac_signature {
            builder = builder.header("X-Weaver-Signature", signature);
        }
        if let Some(token) = bearer.as_deref() {
            builder = builder.bearer_auth(token);
        }

        let response = tokio::select! {
            response = builder.send() => response,
            () = wait_for_cancellation(&mut cancellation) => {
                return Ok(terminal_webhook_result(
                    ExecutionDisposition::Cancelled,
                    None,
                    Some("webhook attempt cancelled".into()),
                ));
            }
        };
        match response {
            Ok(response)
                if retry < retries
                    && (response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || response.status().is_server_error()) =>
            {
                tracing::debug!(
                    attempt_id = request.attempt_id,
                    retry,
                    status = response.status().as_u16(),
                    "retrying post-processing webhook"
                );
            }
            Ok(response) => return webhook_response_result(response).await,
            Err(error) if retry < retries => {
                tracing::debug!(
                    attempt_id = request.attempt_id,
                    retry,
                    timeout = error.is_timeout(),
                    connect = error.is_connect(),
                    "retrying post-processing webhook after transport failure"
                );
            }
            Err(_) => return Err(RunnerError::WebhookRequest("transport failure".into())),
        }

        let backoff = Duration::from_millis(250_u64.saturating_mul(1_u64 << retry.min(6)));
        tokio::select! {
            () = tokio::time::sleep(backoff) => {}
            () = wait_for_cancellation(&mut cancellation) => {
                return Ok(terminal_webhook_result(
                    ExecutionDisposition::Cancelled,
                    None,
                    Some("webhook attempt cancelled".into()),
                ));
            }
        }
    }
    Err(RunnerError::WebhookRequest(
        "webhook retry loop ended unexpectedly".into(),
    ))
}

async fn wait_for_cancellation(cancellation: &mut Option<watch::Receiver<bool>>) {
    let Some(receiver) = cancellation else {
        std::future::pending::<()>().await;
        return;
    };
    if *receiver.borrow() {
        return;
    }
    let _ = receiver.changed().await;
}

fn webhook_url(request: &ExtensionExecutionRequest) -> Result<reqwest::Url, RunnerError> {
    let raw = resolved_option(request, "webhook_url")
        .map(option_value_text)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            RunnerError::InvalidWebhookConfiguration("webhook_url is required".into())
        })?;
    let url = reqwest::Url::parse(&raw)
        .map_err(|error| RunnerError::InvalidWebhookConfiguration(error.to_string()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(RunnerError::InvalidWebhookConfiguration(
            "webhook_url must use http or https".into(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(RunnerError::InvalidWebhookConfiguration(
            "webhook_url must not contain user information".into(),
        ));
    }
    Ok(url)
}

fn webhook_retries(request: &ExtensionExecutionRequest) -> Result<u32, RunnerError> {
    let Some(value) = resolved_option(request, "webhook_retries") else {
        return Ok(2);
    };
    let retries = option_value_text(value).parse::<u32>().map_err(|_| {
        RunnerError::InvalidWebhookConfiguration(
            "webhook_retries must be an unsigned integer".into(),
        )
    })?;
    if retries > MAX_WEBHOOK_RETRIES {
        return Err(RunnerError::InvalidWebhookConfiguration(format!(
            "webhook_retries cannot exceed {MAX_WEBHOOK_RETRIES}"
        )));
    }
    Ok(retries)
}

fn webhook_secret(request: &ExtensionExecutionRequest, name: &str) -> Option<String> {
    resolved_option(request, name)
        .map(option_value_text)
        .filter(|value| !value.is_empty())
}

fn webhook_payload(request: &ExtensionExecutionRequest) -> serde_json::Value {
    let mut context = native_context(request);
    let options = request
        .options
        .iter()
        .filter(|option| {
            !matches!(option.value(), ResolvedOptionValue::Secret(_))
                && !option
                    .name()
                    .as_str()
                    .to_ascii_lowercase()
                    .starts_with("webhook_")
        })
        .map(|option| {
            (
                option.name().as_str().to_string(),
                option_value_json(option.value()),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    context["options"] = options.into();
    context
}

async fn webhook_response_result(
    mut response: reqwest::Response,
) -> Result<ExtensionExecutionResult, RunnerError> {
    let status = response.status();
    let header = format!("HTTP {}\n", status.as_u16()).into_bytes();
    let max_body = usize::try_from(MAX_PERSISTED_ATTEMPT_OUTPUT_BYTES)
        .unwrap_or(usize::MAX)
        .saturating_sub(header.len());
    let mut body = Vec::new();
    let mut truncated = false;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| RunnerError::WebhookRequest("response body read failed".into()))?
    {
        let remaining = max_body.saturating_sub(body.len());
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
    }
    let mut output = vec![CapturedOutputLine {
        sequence: 0,
        stream: LogStream::Stdout,
        bytes: header,
    }];
    for (index, bytes) in body.chunks(MAX_LOGICAL_LINE_BYTES).enumerate() {
        output.push(CapturedOutputLine {
            sequence: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
            stream: LogStream::Stdout,
            bytes: bytes.to_vec(),
        });
    }
    let succeeded = status.is_success();
    Ok(ExtensionExecutionResult {
        disposition: if succeeded {
            ExecutionDisposition::Succeeded
        } else {
            ExecutionDisposition::Failed
        },
        exit_code: Some(i32::from(status.as_u16())),
        output,
        output_truncated: truncated,
        effects: ControlEffects::default(),
        error_message: (!succeeded).then(|| format!("webhook returned HTTP {}", status.as_u16())),
    })
}

fn terminal_webhook_result(
    disposition: ExecutionDisposition,
    exit_code: Option<i32>,
    error_message: Option<String>,
) -> ExtensionExecutionResult {
    ExtensionExecutionResult {
        disposition,
        exit_code,
        output: vec![],
        output_truncated: false,
        effects: ControlEffects::default(),
        error_message,
    }
}

async fn execute_supervised(
    prepared: PreparedExecution,
    timeout: Option<Duration>,
    grace: Duration,
    cancellation: Option<watch::Receiver<bool>>,
    on_spawn: Option<Box<dyn FnOnce() -> Result<(), RunnerError> + Send>>,
) -> Result<ExtensionExecutionResult, RunnerError> {
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
    let mut child = command.spawn()?;
    let supervisor_pid = child.id();
    if let Some(on_spawn) = on_spawn
        && let Err(error) = on_spawn()
    {
        terminate_supervisor(&mut child, supervisor_pid, grace).await?;
        return Err(error);
    }
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
    let sequence = Arc::new(AtomicU64::new(0));
    let stdout = child.stdout.take().ok_or_else(|| {
        RunnerError::SupervisorProtocol("supervisor stdout was unavailable".into())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        RunnerError::SupervisorProtocol("supervisor stderr was unavailable".into())
    })?;
    let stdout_task = tokio::spawn(capture_stream(
        stdout,
        LogStream::Stdout,
        output.clone(),
        sequence.clone(),
        prepared.adapter,
    ));
    let stderr_task = tokio::spawn(capture_stream(
        stderr,
        LogStream::Stderr,
        output.clone(),
        sequence,
        prepared.adapter,
    ));

    let deadline = timeout
        .map(|timeout| {
            Instant::now()
                .checked_add(timeout)
                .ok_or(RunnerError::InvalidTimeout)
        })
        .transpose()?;
    let mut cancellation = cancellation;
    let (status, forced) = loop {
        if let Some(status) = child.try_wait()? {
            break (Some(status), None);
        }
        if cancellation
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow())
        {
            terminate_supervisor(&mut child, supervisor_pid, grace).await?;
            break (None, Some(ExecutionDisposition::Cancelled));
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            terminate_supervisor(&mut child, supervisor_pid, grace).await?;
            break (None, Some(ExecutionDisposition::TimedOut));
        }
        if let Some(receiver) = cancellation.as_mut() {
            let _ = receiver.has_changed();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
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
    let mut lines = captured.lines.into_iter().collect::<Vec<_>>();
    lines.sort_by_key(|line| line.sequence);
    if captured.control_overflow {
        return Err(RunnerError::SupervisorProtocol(
            "post-processing control output exceeded its logical-line or 4 MiB cumulative limit"
                .into(),
        ));
    }
    let mut control_lines = captured.control_lines;
    control_lines.sort_by_key(|line| line.sequence);
    let mut effects = parse_control_effects(prepared.adapter, &control_lines)?;
    validate_effect_paths(&mut effects, &prepared.roots)?;
    let exit_code = status.as_ref().and_then(ExitStatus::code);
    let disposition = forced.unwrap_or_else(|| adapter_disposition(prepared.adapter, exit_code));
    if disposition == ExecutionDisposition::RepairRequested {
        effects.repair_requested = true;
    }
    Ok(ExtensionExecutionResult {
        disposition,
        exit_code,
        output: lines,
        output_truncated: captured.truncated,
        effects,
        error_message: match disposition {
            ExecutionDisposition::TimedOut => Some("post-processing attempt timed out".into()),
            ExecutionDisposition::Cancelled => Some("post-processing attempt was cancelled".into()),
            ExecutionDisposition::Failed => Some(match exit_code {
                Some(code) => format!("post-processing script exited with status {code}"),
                None => "post-processing script terminated without an exit status".into(),
            }),
            _ => None,
        },
    })
}

#[derive(Default)]
struct BoundedOutput {
    lines: VecDeque<CapturedOutputLine>,
    bytes: u64,
    truncated: bool,
    control_lines: Vec<CapturedOutputLine>,
    control_bytes: u64,
    control_overflow: bool,
}

impl BoundedOutput {
    fn push(&mut self, line: CapturedOutputLine, adapter: ExtensionAdapter) {
        if is_control_line(adapter, &line) {
            self.control_bytes = self.control_bytes.saturating_add(line.bytes.len() as u64);
            if self.control_bytes <= MAX_CONTROL_OUTPUT_BYTES {
                self.control_lines.push(line.clone());
            } else {
                self.control_overflow = true;
            }
        }
        self.bytes = self.bytes.saturating_add(line.bytes.len() as u64);
        self.lines.push_back(line);
        while self.bytes > MAX_PERSISTED_ATTEMPT_OUTPUT_BYTES && self.lines.len() > 1 {
            let removed = self.lines.remove(1).expect("line after header exists");
            self.bytes = self.bytes.saturating_sub(removed.bytes.len() as u64);
            self.truncated = true;
        }
    }
}

async fn capture_stream<R: AsyncRead + Unpin>(
    mut reader: R,
    stream: LogStream,
    output: Arc<Mutex<BoundedOutput>>,
    sequence: Arc<AtomicU64>,
    adapter: ExtensionAdapter,
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
            let mut line = pending.drain(..=newline).collect::<Vec<_>>();
            split_and_capture(&mut line, stream, &output, &sequence, adapter);
        }
        while pending.len() > MAX_LOGICAL_LINE_BYTES {
            if is_control_prefix(adapter, stream, &pending) {
                output
                    .lock()
                    .expect("output collector poisoned")
                    .control_overflow = true;
            }
            let mut line = pending.drain(..MAX_LOGICAL_LINE_BYTES).collect::<Vec<_>>();
            split_and_capture(&mut line, stream, &output, &sequence, adapter);
        }
    }
    if !pending.is_empty() {
        split_and_capture(&mut pending, stream, &output, &sequence, adapter);
    }
    Ok(())
}

fn split_and_capture(
    bytes: &mut Vec<u8>,
    stream: LogStream,
    output: &Arc<Mutex<BoundedOutput>>,
    sequence: &AtomicU64,
    adapter: ExtensionAdapter,
) {
    output.lock().expect("output collector poisoned").push(
        CapturedOutputLine {
            sequence: sequence.fetch_add(1, Ordering::Relaxed),
            stream,
            bytes: std::mem::take(bytes),
        },
        adapter,
    );
}

fn is_control_line(adapter: ExtensionAdapter, line: &CapturedOutputLine) -> bool {
    is_control_prefix(adapter, line.stream, &line.bytes)
}

fn is_control_prefix(adapter: ExtensionAdapter, stream: LogStream, bytes: &[u8]) -> bool {
    stream == LogStream::Stdout
        && match adapter {
            ExtensionAdapter::Native => bytes.starts_with(b"[WEAVER] "),
            ExtensionAdapter::Nzbget => bytes.starts_with(b"[NZB] "),
            ExtensionAdapter::Sabnzbd | ExtensionAdapter::Webhook => false,
        }
}

fn adapter_disposition(adapter: ExtensionAdapter, exit_code: Option<i32>) -> ExecutionDisposition {
    match (adapter, exit_code) {
        (ExtensionAdapter::Native, Some(0)) | (ExtensionAdapter::Sabnzbd, Some(0)) => {
            ExecutionDisposition::Succeeded
        }
        (ExtensionAdapter::Native, Some(10)) => ExecutionDisposition::Skipped,
        (ExtensionAdapter::Nzbget, Some(92)) => ExecutionDisposition::RepairRequested,
        (ExtensionAdapter::Nzbget, Some(93)) => ExecutionDisposition::Succeeded,
        (ExtensionAdapter::Nzbget, Some(95)) => ExecutionDisposition::Skipped,
        _ => ExecutionDisposition::Failed,
    }
}

fn parse_control_effects(
    adapter: ExtensionAdapter,
    lines: &[CapturedOutputLine],
) -> Result<ControlEffects, RunnerError> {
    let mut effects = ControlEffects::default();
    for line in lines.iter().filter(|line| line.stream == LogStream::Stdout) {
        let text = String::from_utf8_lossy(&line.bytes);
        let text = text.trim_end_matches(['\r', '\n']);
        match adapter {
            ExtensionAdapter::Native => {
                let Some(frame) = text.strip_prefix("[WEAVER] ") else {
                    continue;
                };
                let value: serde_json::Value = serde_json::from_str(frame).map_err(|error| {
                    RunnerError::SupervisorProtocol(format!(
                        "invalid native control frame: {error}"
                    ))
                })?;
                apply_native_frame(&mut effects, value)?;
            }
            ExtensionAdapter::Nzbget => {
                let Some(command) = text.strip_prefix("[NZB] ") else {
                    continue;
                };
                apply_nzbget_command(&mut effects, command)?;
            }
            ExtensionAdapter::Sabnzbd | ExtensionAdapter::Webhook => {}
        }
    }
    Ok(effects)
}

fn apply_native_frame(
    effects: &mut ControlEffects,
    value: serde_json::Value,
) -> Result<(), RunnerError> {
    let object = value.as_object().ok_or_else(|| {
        RunnerError::SupervisorProtocol("native control frame must be an object".into())
    })?;
    let kind = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            RunnerError::SupervisorProtocol("native control frame is missing type".into())
        })?;
    match kind {
        "progress" => effects.progress = Some(value),
        "metadata" => {
            let key = object
                .get("key")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    RunnerError::SupervisorProtocol("metadata frame is missing key".into())
                })?;
            let value = object
                .get("value")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            effects.metadata.insert(key.to_string(), value);
        }
        "artifact" => {
            let path = object
                .get("path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    RunnerError::SupervisorProtocol("artifact frame is missing path".into())
                })?;
            effects.artifacts.push(PathBuf::from(path));
        }
        "directory" => {
            let path = object
                .get("path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    RunnerError::SupervisorProtocol("directory frame is missing path".into())
                })?;
            effects.directory = Some(PathBuf::from(path));
        }
        "finalDirectory" => {
            let path = object
                .get("path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    RunnerError::SupervisorProtocol("finalDirectory frame is missing path".into())
                })?;
            effects.final_directory = Some(PathBuf::from(path));
        }
        "bad" => effects.mark_bad = true,
        "repair" => effects.repair_requested = true,
        other => {
            return Err(RunnerError::SupervisorProtocol(format!(
                "unsupported native control frame type {other}"
            )));
        }
    }
    Ok(())
}

fn apply_nzbget_command(effects: &mut ControlEffects, command: &str) -> Result<(), RunnerError> {
    if let Some(path) = command.strip_prefix("DIRECTORY=") {
        effects.directory = Some(PathBuf::from(path));
    } else if let Some(path) = command.strip_prefix("FINALDIR=") {
        effects.final_directory = Some(PathBuf::from(path));
    } else if let Some(parameter) = command.strip_prefix("NZBPR_") {
        let (name, value) = parameter.split_once('=').ok_or_else(|| {
            RunnerError::SupervisorProtocol("invalid NZBPR control command".into())
        })?;
        if name.is_empty() || name.chars().any(char::is_control) {
            return Err(RunnerError::SupervisorProtocol(
                "invalid NZBPR parameter name".into(),
            ));
        }
        effects
            .parameters
            .insert(name.to_string(), value.to_string());
    } else if command == "MARK=BAD" {
        effects.mark_bad = true;
    } else {
        return Err(RunnerError::SupervisorProtocol(
            "unsupported NZBGet control command".into(),
        ));
    }
    Ok(())
}

fn validate_effect_paths(
    effects: &mut ControlEffects,
    roots: &[PathBuf],
) -> Result<(), RunnerError> {
    for path in effects
        .directory
        .iter_mut()
        .chain(effects.final_directory.iter_mut())
        .chain(effects.artifacts.iter_mut())
    {
        let canonical = canonicalize_effect_path(path)?;
        if !roots.iter().any(|root| canonical.starts_with(root)) {
            return Err(RunnerError::UnapprovedPath);
        }
        *path = canonical;
    }
    Ok(())
}

fn canonicalize_effect_path(path: &Path) -> Result<PathBuf, RunnerError> {
    if !path.is_absolute() {
        return Err(RunnerError::UnapprovedPath);
    }
    fs::canonicalize(path).map_err(|_| RunnerError::UnapprovedPath)
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
    request: &ExtensionExecutionRequest,
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
pub(crate) fn control_effects_for_test(
    adapter: ExtensionAdapter,
    lines: &[CapturedOutputLine],
) -> Result<ControlEffects, RunnerError> {
    parse_control_effects(adapter, lines)
}

#[cfg(test)]
pub(crate) fn bounded_output_for_test(
    adapter: ExtensionAdapter,
    lines: Vec<Vec<u8>>,
) -> Result<(Vec<CapturedOutputLine>, ControlEffects, bool), RunnerError> {
    let mut captured = BoundedOutput::default();
    for (sequence, bytes) in lines.into_iter().enumerate() {
        captured.push(
            CapturedOutputLine {
                sequence: sequence as u64,
                stream: LogStream::Stdout,
                bytes,
            },
            adapter,
        );
    }
    if captured.control_overflow {
        return Err(RunnerError::SupervisorProtocol(
            "post-processing control output exceeded its logical-line or 4 MiB cumulative limit"
                .into(),
        ));
    }
    let effects = parse_control_effects(adapter, &captured.control_lines)?;
    Ok((
        captured.lines.into_iter().collect(),
        effects,
        captured.truncated,
    ))
}

#[cfg(test)]
pub(crate) fn redact_bytes_for_test(input: &[u8], secrets: &[Vec<u8>]) -> Vec<u8> {
    redact_bytes(input, secrets)
}

#[cfg(test)]
pub(crate) fn redact_execution_result_for_test(
    result: &mut ExtensionExecutionResult,
    secrets: &[Vec<u8>],
) -> Result<(), RunnerError> {
    redact_execution_result(result, secrets)
}

#[cfg(test)]
pub(crate) fn webhook_url_for_test(
    request: &ExtensionExecutionRequest,
) -> Result<reqwest::Url, RunnerError> {
    webhook_url(request)
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
