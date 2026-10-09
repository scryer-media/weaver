//! Running a script instance on request against made-up inputs.
//!
//! A test run hands the script what a real run of the instance would:
//! the same variables and arguments, built from a download that does not exist
//! and a scratch directory that is removed when the run ends. Commands the
//! script issues, whether it prints them or sends them through the API, are
//! reported and never applied, and nothing about the run is stored: it is kept
//! in memory for as long as it takes to read.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use super::callbacks::{RunAction, RunRequest, RunRequests};
use super::directives::{Directive, ScriptOutputEvent};
use super::events::EventContext;
use super::executor::{execution_refusal, strict_security_enabled};
use super::instances::InstanceTrigger;
use super::listing::resolve_script;
use super::model::{
    PipelineOutcome, QueueEvent, ScriptAdapter, ScriptEventLabel, ScriptName, ScriptStatus,
};
use super::runner::{
    CompatibilityFacts, ExecutionDisposition, ExecutionSpec, InterpreterConfig,
    JobExecutionContext, MAX_SCRIPT_OUTPUT_BYTES, OutputTap, RunIdentity, RunnerError,
    ScriptExecutionRequest, ScriptExecutionResult, adapter_contract, execute_script_tapped,
    execute_spec_tapped,
};
use crate::Database;
use crate::settings::SharedConfig;

/// Test runs allowed at once. One more is refused instead of queued: a test is
/// something an operator is watching.
const RUNNING_TESTS: usize = 4;
/// Ended test runs kept so that their result can still be read.
const KEPT_TESTS: usize = 16;
/// Commands reported for one test run.
const REPORTED_COMMANDS: usize = 256;

/// The download a test run is about. Its id is far above any weaver hands out,
/// so a script that calls back with it reaches nothing.
const TEST_JOB_ID: u64 = 2_000_000_000;
const TEST_JOB_NAME: &str = "Weaver.Test.Download";
const TEST_FILE_NAME: &str = "weaver-test.txt";
const TEST_FILE: &str = "Made up by weaver for a script test run.\n";
const TEST_URL: &str = "https://example.invalid/Weaver.Test.Download.nzb";
const TEST_NZB: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="weaver-test@example.invalid" date="0" subject="Weaver.Test.Download [1/1] - &quot;weaver-test.txt&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="41" number="1">weaver-test-1@example.invalid</segment></segments>
  </file>
</nzb>
"#;
const TEST_FEED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Weaver test feed</title>
    <link>https://example.invalid/</link>
    <description>Made up by weaver for a script test run.</description>
    <item>
      <title>Weaver.Test.Download</title>
      <link>https://example.invalid/Weaver.Test.Download.nzb</link>
      <enclosure url="https://example.invalid/Weaver.Test.Download.nzb" length="41" type="application/x-nzb"/>
    </item>
  </channel>
</rss>
"#;

/// Variables a run is handed that are not made up for the test: the
/// instance's own inputs, weaver's own directories, per-download parameters,
/// and what lets the script call weaver back.
const NOT_MADE_UP: [&str; 7] = [
    "NZBPO_",
    "NZBOP_",
    "NZBPR_",
    "SAB_OPTION_",
    "WEAVER_INPUT_",
    "WEAVER_API_URL",
    "WEAVER_RUN_TOKEN",
];

/// What a test run of an instance stands in for.
fn test_event(trigger: InstanceTrigger) -> ScriptEventLabel {
    match trigger {
        InstanceTrigger::PostProcessing => ScriptEventLabel::PostProcessing,
        InstanceTrigger::Queue(event) => ScriptEventLabel::Queue(event),
        InstanceTrigger::Scan => ScriptEventLabel::Scan,
        InstanceTrigger::Schedule => ScriptEventLabel::Scheduler(0),
        InstanceTrigger::Feed => ScriptEventLabel::Feed(0),
    }
}

/// Why a test run was not started.
#[derive(Debug, thiserror::Error)]
pub enum ScriptTestError {
    #[error("{0}")]
    Refused(&'static str),
    #[error("the script instance does not exist")]
    NotFound,
    #[error("{0}")]
    Unavailable(String),
    #[error("{RUNNING_TESTS} test runs are already in progress")]
    Busy,
    #[error("could not prepare the test run: {0}")]
    Setup(String),
}

fn setup(error: impl std::fmt::Display) -> ScriptTestError {
    ScriptTestError::Setup(error.to_string())
}

/// How a test run ended.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScriptTestOutcome {
    pub status: ScriptStatus,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub error_message: Option<String>,
}

/// A test run as it stands.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScriptTestSnapshot {
    pub id: String,
    pub instance_id: String,
    pub instance_name: String,
    pub script: ScriptName,
    pub event: ScriptEventLabel,
    pub adapter: ScriptAdapter,
    pub started_at_epoch_ms: i64,
    /// The run is ended after this long.
    pub timeout_seconds: u64,
    /// The variables made up for this run, in name order. The instance's own
    /// inputs are left out: they are real, and some are secret.
    pub inputs: Vec<(String, String)>,
    /// The arguments made up for this run, in order.
    pub arguments: Vec<String>,
    /// What the script has printed so far, or all of it once the run has ended.
    pub log: String,
    pub log_truncated: bool,
    /// Commands the script issued, in order. A test run applies none of them.
    pub commands: Vec<String>,
    pub commands_truncated: bool,
    /// `None` while the script is still running.
    pub outcome: Option<ScriptTestOutcome>,
}

struct TestState {
    snapshot: ScriptTestSnapshot,
    output: Vec<u8>,
}

struct TestRun {
    cancel: watch::Sender<bool>,
    changed: tokio::sync::Notify,
    ceiling: usize,
    state: Mutex<TestState>,
}

impl TestRun {
    fn state(&self) -> std::sync::MutexGuard<'_, TestState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn snapshot(&self) -> ScriptTestSnapshot {
        let state = self.state();
        let mut snapshot = state.snapshot.clone();
        snapshot.log = String::from_utf8_lossy(&state.output).into_owned();
        snapshot
    }

    fn running(&self) -> bool {
        self.state().snapshot.outcome.is_none()
    }

    /// Add a line the script printed, keeping the newest output once the log
    /// is over its ceiling.
    fn append(&self, line: &[u8]) {
        {
            let mut state = self.state();
            state.output.extend_from_slice(line);
            if state.output.len() > self.ceiling {
                let excess = state.output.len() - self.ceiling;
                let cut = state.output[excess..]
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(excess, |newline| excess + newline + 1);
                state.output.drain(..cut);
                state.snapshot.log_truncated = true;
            }
        }
        self.changed.notify_waiters();
    }

    fn command(&self, command: String) {
        {
            let mut state = self.state();
            if state.snapshot.commands.len() < REPORTED_COMMANDS {
                state.snapshot.commands.push(command);
            } else {
                state.snapshot.commands_truncated = true;
            }
        }
        self.changed.notify_waiters();
    }

    fn finish(&self, result: Result<ScriptExecutionResult, RunnerError>, duration: Duration) {
        {
            let mut state = self.state();
            let (status, exit_code, error_message) = match result {
                Ok(result) => {
                    state.output = result.output;
                    state.snapshot.log_truncated = result.output_truncated;
                    (
                        match result.disposition {
                            ExecutionDisposition::Succeeded => ScriptStatus::Succeeded,
                            ExecutionDisposition::Skipped => ScriptStatus::Skipped,
                            ExecutionDisposition::Failed => ScriptStatus::Failed,
                            ExecutionDisposition::TimedOut => ScriptStatus::TimedOut,
                            ExecutionDisposition::Cancelled => ScriptStatus::Cancelled,
                        },
                        result.exit_code,
                        result.error_message,
                    )
                }
                Err(error) => (ScriptStatus::Failed, None, Some(error.to_string())),
            };
            state.snapshot.outcome = Some(ScriptTestOutcome {
                status,
                exit_code,
                duration_ms: duration.as_millis() as u64,
                error_message,
            });
        }
        self.changed.notify_waiters();
    }
}

/// The test runs in progress and the last few that ended, oldest first.
#[derive(Default)]
pub(crate) struct TestRuns(Mutex<VecDeque<Arc<TestRun>>>);

impl TestRuns {
    fn runs(&self) -> std::sync::MutexGuard<'_, VecDeque<Arc<TestRun>>> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn admit(&self, run: Arc<TestRun>) -> Result<(), ScriptTestError> {
        let mut runs = self.runs();
        if runs.iter().filter(|run| run.running()).count() >= RUNNING_TESTS {
            return Err(ScriptTestError::Busy);
        }
        runs.push_back(run);
        let mut ended = runs.iter().filter(|run| !run.running()).count();
        runs.retain(|run| {
            if ended > KEPT_TESTS && !run.running() {
                ended -= 1;
                false
            } else {
                true
            }
        });
        Ok(())
    }

    fn find(&self, id: &str) -> Option<Arc<TestRun>> {
        self.runs()
            .iter()
            .find(|run| run.state().snapshot.id == id)
            .cloned()
    }
}

struct Directories {
    data: PathBuf,
    intermediate: PathBuf,
    complete: PathBuf,
}

enum Execution {
    PostProcessing(Box<ScriptExecutionRequest>),
    Event(Box<ExecutionSpec>),
}

/// Run the instance `instance_id` against made-up inputs and return the run
/// as it stands. Read it again with [`Database::script_test`]. Whether the
/// instance is turned on makes no difference to a test.
pub async fn start_script_test(
    db: &Database,
    config: &SharedConfig,
    instance_id: String,
    supervisor_executable: Option<PathBuf>,
) -> Result<ScriptTestSnapshot, ScriptTestError> {
    let directories = {
        let config = config.read().await;
        Directories {
            data: PathBuf::from(&config.data_dir),
            intermediate: PathBuf::from(config.intermediate_dir()),
            complete: PathBuf::from(config.complete_dir()),
        }
    };
    let worker = db.clone();
    let (run, execution, scratch, requests) = tokio::task::spawn_blocking(move || {
        prepare(&worker, &instance_id, &directories, supervisor_executable)
    })
    .await
    .map_err(setup)??;
    let snapshot = run.snapshot();
    tokio::spawn(execute(run, execution, scratch, requests));
    Ok(snapshot)
}

fn prepare(
    db: &Database,
    instance_id: &str,
    directories: &Directories,
    supervisor_executable: Option<PathBuf>,
) -> Result<(Arc<TestRun>, Execution, tempfile::TempDir, RunRequests), ScriptTestError> {
    let settings = db.post_processing_settings().map_err(setup)?;
    if let Some(reason) = execution_refusal(&settings, strict_security_enabled()) {
        return Err(ScriptTestError::Refused(reason));
    }
    let instance = db
        .script_instance(instance_id)
        .map_err(setup)?
        .ok_or(ScriptTestError::NotFound)?;
    let options = db
        .script_instance_run_inputs(&instance.id)
        .map_err(setup)?
        .ok_or(ScriptTestError::NotFound)?;
    let root = db.post_processing_script_directory().map_err(setup)?;
    let script = resolve_script(&root, &instance.script)
        .map_err(|error| ScriptTestError::Unavailable(error.to_string()))?;
    let event = test_event(instance.trigger);
    // Held to the limit a real run of the instance would have.
    let time_limit = instance.time_limit(&settings);
    let timeout_seconds = time_limit.as_secs();

    std::fs::create_dir_all(&directories.data).map_err(setup)?;
    let scratch = tempfile::Builder::new()
        .prefix("script-test-")
        .tempdir_in(&directories.data)
        .map_err(setup)?;
    // An instance kept to some categories is tested as a download in the
    // first of them.
    let category = instance.categories.first().cloned();
    let job = simulated_job(scratch.path(), category, directories).map_err(setup)?;

    let mut id = [0_u8; 16];
    getrandom::fill(&mut id).map_err(setup)?;
    let id = hex::encode(id);
    let mut identity = RunIdentity {
        run_id: id.clone(),
        ..RunIdentity::of(&instance).map_err(setup)?
    };
    let adapter = script.manifest.adapter();
    let timeout = Some(time_limit);
    let termination_grace = Duration::from_secs(settings.termination_grace_seconds);
    let interpreters = InterpreterConfig {
        python: settings.python_interpreter.as_ref().map(PathBuf::from),
        powershell: settings.powershell_interpreter.as_ref().map(PathBuf::from),
        batch: settings.batch_interpreter.as_ref().map(PathBuf::from),
    };
    let context = match instance.trigger {
        InstanceTrigger::PostProcessing => None,
        InstanceTrigger::Queue(event) => Some(simulated_queue_event(&job, event)),
        InstanceTrigger::Scan => Some(simulated_scan(&job, scratch.path()).map_err(setup)?),
        InstanceTrigger::Schedule => Some(jobless(
            &job,
            scratch.path(),
            event.clone(),
            [("NZBSP_TASKID", "0".to_string())],
        )),
        InstanceTrigger::Feed => Some(simulated_feed(&job, scratch.path()).map_err(setup)?),
    };
    // The made-up download the run is about, when it is about one: the same
    // id the script is handed as `WEAVER_JOB_ID`.
    let job_id = context
        .as_ref()
        .map_or(Some(job.job_id), |context| context.job_id);
    // The script can call back as it could in a real run. What it asks for
    // comes here, where none of it is applied.
    let requests = db.open_script_run(&mut identity, job_id, &event, timeout, true);
    let (execution, inputs, arguments) = match context {
        None => {
            let request = ScriptExecutionRequest {
                manifest: script.manifest,
                root: script.root,
                options,
                context: job,
                identity,
                timeout,
                termination_grace,
                interpreters,
                supervisor_executable,
            };
            let (arguments, env) = adapter_contract(&request).map_err(setup)?;
            let inputs = env
                .into_iter()
                .filter(|(name, _)| !NOT_MADE_UP.iter().any(|prefix| name.starts_with(prefix)))
                .collect();
            (
                Execution::PostProcessing(Box::new(request)),
                inputs,
                arguments,
            )
        }
        Some(context) => {
            let mut env = context.weaver_env();
            env.extend(context.env);
            let inputs = env.clone().into_iter().collect();
            let spec = ExecutionSpec {
                manifest: script.manifest,
                root: script.root,
                options,
                cwd: context.cwd,
                env,
                argv: Vec::new(),
                timeout,
                termination_grace,
                kind: context.event,
                run_id: format!("test:{id}"),
                identity,
                facts: context.facts,
                interpreters,
                supervisor_executable,
            };
            (Execution::Event(Box::new(spec)), inputs, Vec::new())
        }
    };

    let (cancel, _) = watch::channel(false);
    let run = Arc::new(TestRun {
        cancel,
        changed: tokio::sync::Notify::new(),
        ceiling: MAX_SCRIPT_OUTPUT_BYTES as usize,
        state: Mutex::new(TestState {
            snapshot: ScriptTestSnapshot {
                id,
                instance_id: instance.id,
                instance_name: instance.name,
                script: instance.script,
                event,
                adapter,
                started_at_epoch_ms: chrono::Utc::now().timestamp_millis(),
                timeout_seconds,
                inputs,
                arguments,
                log: String::new(),
                log_truncated: false,
                commands: Vec::new(),
                commands_truncated: false,
                outcome: None,
            },
            output: Vec::new(),
        }),
    });
    db.script_runtime.tests.admit(run.clone())?;
    Ok((run, execution, scratch, requests))
}

async fn execute(
    run: Arc<TestRun>,
    execution: Execution,
    scratch: tempfile::TempDir,
    mut requests: RunRequests,
) {
    let started = Instant::now();
    let cancellation = run.cancel.subscribe();
    let (sender, mut receiver) = mpsc::channel(64);
    let tap: OutputTap = {
        let run = run.clone();
        Arc::new(move |line: &[u8]| run.append(line))
    };
    let commands = async {
        loop {
            tokio::select! {
                event = receiver.recv() => match event {
                    Some(ScriptOutputEvent::Directive(directive)) => {
                        run.command(command_text(&directive));
                    }
                    // The text of a log line is already in the output the tap
                    // is handed.
                    Some(ScriptOutputEvent::Log { .. }) => {}
                    None => break,
                },
                request = requests.next() => {
                    let RunRequest { action, reply } = request;
                    let outcome = match action {
                        RunAction::Command(directive) => {
                            run.command(command_text(&directive));
                            Ok(())
                        }
                        RunAction::Log { text, .. } => requests.log(&text).map(drop),
                        RunAction::Fail(reason) => requests.fail(&reason).map(|()| {
                            run.command(format!(
                                "FAIL={}",
                                requests.failure().unwrap_or_default()
                            ));
                        }),
                    };
                    // The script may have stopped waiting for the answer.
                    let _ = reply.send(outcome);
                }
            }
        }
    };
    let execution = async {
        match execution {
            Execution::PostProcessing(request) => {
                execute_script_tapped(*request, Some(cancellation), Some(sender), Some(tap)).await
            }
            Execution::Event(spec) => {
                execute_spec_tapped(*spec, Some(cancellation), Some(sender), Some(tap)).await
            }
        }
    };
    let (mut result, ()) = tokio::join!(execution, commands);
    // Failing is how the run came out, not something done to a download, so
    // a test shows it as a real run would.
    if let Ok(result) = &mut result {
        requests.settle(result);
    }
    // The run is over: its token is worth nothing from here on.
    drop(requests);
    match tokio::task::spawn_blocking(move || scratch.close()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(%error, "could not remove a script test run's scratch directory");
        }
        Err(error) => {
            tracing::warn!(%error, "could not remove a script test run's scratch directory");
        }
    }
    run.finish(result, started.elapsed());
}

/// The download a test run is about: verified, unpacked and complete.
fn simulated_job(
    scratch: &Path,
    category: Option<String>,
    directories: &Directories,
) -> std::io::Result<JobExecutionContext> {
    let directory = scratch.join(TEST_JOB_NAME);
    std::fs::create_dir(&directory)?;
    std::fs::write(directory.join(TEST_FILE_NAME), TEST_FILE)?;
    let bytes = TEST_FILE.len() as u64;
    Ok(JobExecutionContext {
        job_id: TEST_JOB_ID,
        name: TEST_JOB_NAME.into(),
        nzb_filename: format!("{TEST_JOB_NAME}.nzb"),
        category,
        group: None,
        source_url: None,
        working_directory: directory.clone(),
        final_directory: directory,
        pipeline_outcome: PipelineOutcome::Succeeded,
        par_status: 2,
        unpack_status: 2,
        compatibility: CompatibilityFacts {
            total_bytes: bytes,
            downloaded_bytes: bytes,
            health_milli: 1000,
            critical_health_milli: 1000,
            data_dir: Some(directories.data.clone()),
            intermediate_dir: Some(directories.intermediate.clone()),
            complete_dir: Some(directories.complete.clone()),
            ..Default::default()
        },
    })
}

fn simulated_queue_event(job: &JobExecutionContext, event: QueueEvent) -> EventContext {
    let mut context = EventContext::from_job(job, event);
    let reported: &[(&str, &str)] = match event {
        QueueEvent::NzbDeleted => &[("NZBNA_DELETESTATUS", "MANUAL")],
        QueueEvent::NzbMarked => &[("NZBNA_MARKSTATUS", "BAD")],
        QueueEvent::UrlCompleted => &[("NZBNA_URL", TEST_URL), ("NZBNA_URLSTATUS", "SUCCESS")],
        QueueEvent::FileDownloaded
        | QueueEvent::NzbAdded
        | QueueEvent::NzbNamed
        | QueueEvent::NzbDownloaded => &[],
    };
    for (name, value) in reported {
        context.env.insert((*name).into(), (*value).into());
    }
    context
}

/// An event with no download behind it, run in the scratch directory.
fn jobless<const N: usize>(
    job: &JobExecutionContext,
    scratch: &Path,
    event: ScriptEventLabel,
    env: [(&str, String); N],
) -> EventContext {
    EventContext {
        job_id: None,
        event,
        category: job.category.clone(),
        cwd: scratch.to_path_buf(),
        env: env
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect::<BTreeMap<_, _>>(),
        facts: job.compatibility.clone(),
        instances: None,
        scratch: None,
    }
}

fn simulated_scan(job: &JobExecutionContext, scratch: &Path) -> std::io::Result<EventContext> {
    let input = scratch.join("input.nzb");
    std::fs::write(&input, TEST_NZB)?;
    Ok(jobless(
        job,
        scratch,
        ScriptEventLabel::Scan,
        [
            ("NZBNP_DIRECTORY", scratch.to_string_lossy().into_owned()),
            ("NZBNP_FILENAME", input.to_string_lossy().into_owned()),
            ("NZBNP_NZBNAME", job.nzb_filename.clone()),
            ("NZBNP_URL", String::new()),
            ("NZBNP_CATEGORY", job.category.clone().unwrap_or_default()),
            ("NZBNP_PRIORITY", "0".into()),
            ("NZBNP_TOP", "0".into()),
            ("NZBNP_PAUSED", "0".into()),
            ("NZBNP_DUPEKEY", String::new()),
            ("NZBNP_DUPESCORE", "0".into()),
            ("NZBNP_DUPEMODE", "SCORE".into()),
        ],
    ))
}

fn simulated_feed(job: &JobExecutionContext, scratch: &Path) -> std::io::Result<EventContext> {
    let input = scratch.join("feed.xml");
    std::fs::write(&input, TEST_FEED)?;
    Ok(jobless(
        job,
        scratch,
        ScriptEventLabel::Feed(0),
        [
            ("NZBFP_FEEDID", "0".to_string()),
            ("NZBFP_FILENAME", input.to_string_lossy().into_owned()),
        ],
    ))
}

/// A command as the script wrote it, after the `[NZB]` marker.
fn command_text(directive: &Directive) -> String {
    let flag = |value: &bool| if *value { "1" } else { "0" };
    match directive {
        Directive::Parameter { name, value } => format!("NZBPR_{name}={value}"),
        Directive::Directory(value) => format!("DIRECTORY={value}"),
        Directive::FinalDirectory(value) => format!("FINALDIR={value}"),
        Directive::MarkBad => "MARK=BAD".into(),
        Directive::Name(value) => format!("NZBNAME={value}"),
        Directive::Category(value) => format!("CATEGORY={value}"),
        Directive::Priority(value) => format!("PRIORITY={value}"),
        Directive::Top(value) => format!("TOP={}", flag(value)),
        Directive::Paused(value) => format!("PAUSED={}", flag(value)),
        Directive::DupeKey(value) => format!("DUPEKEY={value}"),
        Directive::DupeScore(value) => format!("DUPESCORE={value}"),
        Directive::DupeMode(value) => format!("DUPEMODE={}", value.as_str()),
    }
}

impl Database {
    /// The test run `id` as it stands, or `None` when there is no such run or
    /// it is no longer kept.
    pub fn script_test(&self, id: &str) -> Option<ScriptTestSnapshot> {
        Some(self.script_runtime.tests.find(id)?.snapshot())
    }

    /// Ask the test run `id` to stop. False when there is no such run or it
    /// has already ended.
    pub fn cancel_script_test(&self, id: &str) -> bool {
        let Some(run) = self.script_runtime.tests.find(id) else {
            return false;
        };
        if !run.running() {
            return false;
        }
        // Not `send`: a run that has not reached its script yet has nothing
        // listening, and must still find the request when it does.
        run.cancel.send_replace(true);
        true
    }

    /// The test run `id` once `reached` holds for it, however long that
    /// takes, or `None` when there is no such run.
    pub async fn script_test_when(
        &self,
        id: &str,
        reached: impl Fn(&ScriptTestSnapshot) -> bool,
    ) -> Option<ScriptTestSnapshot> {
        let run = self.script_runtime.tests.find(id)?;
        loop {
            let changed = run.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let snapshot = run.snapshot();
            if reached(&snapshot) {
                return Some(snapshot);
            }
            changed.await;
        }
    }
}
