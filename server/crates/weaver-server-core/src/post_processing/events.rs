use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use super::callbacks::{RunAction, RunRequest, RunRequests};
use super::directives::{Directive, ScriptLogLevel, ScriptOutputEvent};
use super::executor::{execution_refusal, strict_security_enabled};
use super::instances::{InstanceTrigger, ScriptInstance};
use super::listing::DiscoveredScript;
use super::model::{
    PostProcessingSettings, QueueEvent, ScriptAdapter, ScriptEventLabel, ScriptResult, ScriptStatus,
};
use super::runner::{
    CompatibilityFacts, ExecutionDisposition, ExecutionSpec, InterpreterConfig,
    JobExecutionContext, RunIdentity, execute_spec,
};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::persistence::{Database, StateError};

type EventCancellations = BTreeMap<String, (Option<u64>, watch::Sender<bool>)>;
type BackgroundRuns = BTreeMap<u64, (Option<u64>, watch::Sender<bool>)>;

/// Fire-and-forget runs allowed at once. They are bounded apart from the
/// scripts weaver waits for, so neither takes a turn from the other.
const BACKGROUND_RUNS: usize = 32;

struct BackgroundLane {
    turns: std::sync::Arc<tokio::sync::Semaphore>,
    runs: std::sync::Mutex<BackgroundRuns>,
    next: std::sync::atomic::AtomicU64,
    settled: tokio::sync::Notify,
}

impl Default for BackgroundLane {
    fn default() -> Self {
        Self {
            turns: std::sync::Arc::new(tokio::sync::Semaphore::new(BACKGROUND_RUNS)),
            runs: Default::default(),
            next: Default::default(),
            settled: Default::default(),
        }
    }
}

/// One fire-and-forget run, registered from the moment it is decided on so
/// that a cancel reaches it while it still waits for its turn.
pub(crate) struct BackgroundRun {
    runtime: std::sync::Arc<ScriptRuntime>,
    id: u64,
    cancellation: watch::Receiver<bool>,
}

impl BackgroundRun {
    pub(crate) fn register(db: &Database, job_id: Option<u64>) -> Self {
        let runtime = db.script_runtime.clone();
        let id = runtime
            .background
            .next
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (cancel, cancellation) = watch::channel(false);
        runtime
            .background
            .runs
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(id, (job_id, cancel));
        Self {
            runtime,
            id,
            cancellation,
        }
    }

    /// The run's turn and its cancel signal, or `None` when it was cancelled
    /// while it waited for the turn.
    pub(crate) async fn turn(
        &self,
    ) -> Option<(tokio::sync::OwnedSemaphorePermit, watch::Receiver<bool>)> {
        let mut cancellation = self.cancellation.clone();
        let turn = background_turn(&self.runtime, &mut cancellation).await?;
        Some((turn, cancellation))
    }
}

impl Drop for BackgroundRun {
    fn drop(&mut self) {
        self.runtime
            .background
            .runs
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.id);
        self.runtime.background.settled.notify_waiters();
    }
}

/// A turn among the fire-and-forget runs, or `None` when `cancellation` fired
/// first.
async fn background_turn(
    runtime: &std::sync::Arc<ScriptRuntime>,
    cancellation: &mut watch::Receiver<bool>,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    if *cancellation.borrow() {
        return None;
    }
    tokio::select! {
        biased;
        turn = runtime.background.turns.clone().acquire_owned() => turn.ok(),
        _ = cancellation.changed() => None,
    }
}

#[derive(Default)]
pub(crate) struct ScriptRuntime {
    queue: tokio::sync::Mutex<()>,
    changed: tokio::sync::Notify,
    running: std::sync::Mutex<usize>,
    capacity_changed: tokio::sync::Notify,
    cancellations: std::sync::Mutex<EventCancellations>,
    claimed: std::sync::Mutex<EventCancellations>,
    worker_active: std::sync::atomic::AtomicBool,
    queue_requested: std::sync::atomic::AtomicBool,
    failed_runs: std::sync::Mutex<BTreeMap<String, String>>,
    effects: std::sync::Mutex<(BTreeSet<u64>, Option<watch::Sender<()>>)>,
    pub(super) effects_cache:
        std::sync::Mutex<Option<BTreeMap<u64, super::effects::JobScriptEffects>>>,
    pub(super) effects_writer: std::sync::Mutex<()>,
    admission_hint: std::sync::Mutex<(u64, Option<AdmissionHint>)>,
    pub(super) dispatch_jobs: std::sync::Mutex<(u64, Option<super::instances::DispatchJobs>)>,
    /// The schedule jobs the script evaluator works from, read once after
    /// each change to the jobs rather than on every tick.
    pub(super) schedule_jobs: std::sync::Mutex<(
        u64,
        Option<std::sync::Arc<Vec<super::instances::ScriptInstance>>>,
    )>,
    admissions: std::sync::Mutex<QueueAdmissions>,
    durable_events_seen: std::sync::atomic::AtomicBool,
    background: BackgroundLane,
    pub(super) tests: super::test_run::TestRuns,
    pub(super) live: std::sync::Arc<super::callbacks::LiveRuns>,
    pub(super) trim: super::output::RetentionTrim,
}

#[derive(Clone, Copy)]
enum AdmissionHint {
    Disabled,
    NoConfiguredScripts,
    Possible,
}

struct QueueAdmission {
    context: EventContext,
    completion: Option<tokio::sync::oneshot::Sender<Result<Option<String>, StateError>>>,
}

#[derive(Default)]
struct QueueAdmissions {
    pending: VecDeque<QueueAdmission>,
    running: bool,
    active_job: Option<u64>,
}

struct EventPermit(std::sync::Arc<ScriptRuntime>);

impl Drop for EventPermit {
    fn drop(&mut self) {
        *self
            .0
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner()) -= 1;
        self.0.capacity_changed.notify_waiters();
    }
}

struct RunRegistration {
    runtime: std::sync::Arc<ScriptRuntime>,
    run_id: String,
    forwarding: Option<tokio::task::JoinHandle<()>>,
}

struct QueueClaimRegistration {
    runtime: std::sync::Arc<ScriptRuntime>,
    run_id: String,
    cancellation: watch::Receiver<bool>,
}

impl Drop for QueueClaimRegistration {
    fn drop(&mut self) {
        self.runtime
            .claimed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.run_id);
        self.runtime.changed.notify_waiters();
    }
}

impl Drop for RunRegistration {
    fn drop(&mut self) {
        if let Some(task) = &self.forwarding {
            task.abort();
        }
        self.runtime
            .cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.run_id);
        self.runtime.changed.notify_waiters();
    }
}

/// Snapshot of an event's inputs. Jobless events have no synthetic job id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventContext {
    pub job_id: Option<u64>,
    pub event: ScriptEventLabel,
    pub category: Option<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub facts: CompatibilityFacts,
    /// The instances to run in place of the ones saved for the event's
    /// trigger. A queued event never carries any: the saved ones are read
    /// when its turn comes.
    #[serde(skip)]
    pub instances: Option<Vec<ScriptInstance>>,
    /// Scratch files the run reads. A script nothing waits for keeps them
    /// until it ends, after whatever raised the event has gone on.
    #[serde(skip)]
    pub scratch: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
}

impl EventContext {
    pub fn from_job(context: &JobExecutionContext, event: QueueEvent) -> Self {
        let parameter = |key: &str, fallback: &str| {
            context
                .compatibility
                .parameters
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
                .unwrap_or_else(|| fallback.into())
        };
        let env = [
            ("NZBNA_EVENT", event.as_str().into()),
            ("NZBNA_NZBID", context.job_id.to_string()),
            ("NZBNA_LASTID", context.job_id.to_string()),
            ("NZBNA_NZBNAME", context.name.clone()),
            ("NZBNA_FILENAME", context.nzb_filename.clone()),
            ("NZBNA_QUEUEDFILE", String::new()),
            (
                "NZBNA_DIRECTORY",
                context.working_directory.to_string_lossy().into_owned(),
            ),
            ("NZBNA_URL", context.source_url.clone().unwrap_or_default()),
            (
                "NZBNA_CATEGORY",
                context.category.clone().unwrap_or_default(),
            ),
            (
                "NZBNA_PRIORITY",
                super::scan::numeric_priority(&parameter("priority", "0")).to_string(),
            ),
            ("NZBNA_DELETESTATUS", "NONE".into()),
            (
                "NZBNA_MARKSTATUS",
                if context.compatibility.marked_bad {
                    "BAD"
                } else {
                    "NONE"
                }
                .into(),
            ),
            ("NZBNA_URLSTATUS", "NONE".into()),
            ("NZBNA_DUPEKEY", parameter("nzbget.dupe_key", "")),
            ("NZBNA_DUPESCORE", parameter("nzbget.dupe_score", "0")),
            ("NZBNA_DUPEMODE", parameter("nzbget.dupe_mode", "SCORE")),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value))
        .collect();
        Self {
            job_id: Some(context.job_id),
            event: ScriptEventLabel::Queue(event),
            category: context.category.clone(),
            cwd: context.working_directory.clone(),
            env,
            facts: context.compatibility.clone(),
            instances: None,
            scratch: None,
        }
    }

    /// What weaver's own variables say about the thing the event is about.
    pub(super) fn weaver_env(&self) -> BTreeMap<String, String> {
        let mut env = BTreeMap::new();
        if let Some(job_id) = self.job_id {
            env.insert("WEAVER_JOB_ID".to_string(), job_id.to_string());
        }
        if let Some(name) = ["NZBNA_NZBNAME", "NZBNP_NZBNAME"]
            .into_iter()
            .find_map(|key| self.env.get(key))
        {
            env.insert("WEAVER_JOB_NAME".into(), name.clone());
        }
        env.insert(
            "WEAVER_CATEGORY".into(),
            self.category.clone().unwrap_or_default(),
        );
        env.insert(
            "WEAVER_DIRECTORY".into(),
            self.cwd.to_string_lossy().into_owned(),
        );
        env
    }
}

/// A turn among the event scripts weaver waits for, or `None` when the run was
/// cancelled while it waited for one.
async fn event_turn(
    db: &Database,
    runtime: &std::sync::Arc<ScriptRuntime>,
    cancellation: &mut watch::Receiver<bool>,
) -> Result<Option<EventPermit>, StateError> {
    loop {
        let changed = runtime.capacity_changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let limit = usize::from(
            db.post_processing_settings()?
                .event_scripts
                .event_script_concurrency,
        );
        {
            let mut running = runtime
                .running
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if *running < limit {
                *running += 1;
                return Ok(Some(EventPermit(runtime.clone())));
            }
        }
        tokio::select! {
            _ = changed => {},
            _ = cancellation.changed() => return Ok(None),
        }
    }
}

/// The instances an event runs: the ones handed to it, or the ones saved for
/// its trigger and category.
pub fn selected_scripts(
    db: &Database,
    context: &EventContext,
) -> Result<Vec<ScriptInstance>, StateError> {
    if let Some(instances) = &context.instances {
        return Ok(instances
            .iter()
            .filter(|instance| instance.enabled)
            .cloned()
            .collect());
    }
    db.script_instances_for(&context.event, context.category.as_deref())
}

/// Record whether a queue event could have anything to run, and return
/// whether script execution is refused altogether.
pub(crate) fn refresh_admission_hint(db: &Database) -> Result<bool, StateError> {
    let (revision, cached) = *db
        .script_runtime
        .admission_hint
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    // Every save of the settings or of an instance drops the hint, so one
    // that is still held answers for both reads below. A refusal is always
    // worked out again: it also follows the strict-security switch, which
    // nothing saves, and costs one settings read with no instance scan.
    if matches!(
        cached,
        Some(AdmissionHint::NoConfiguredScripts | AdmissionHint::Possible)
    ) && !strict_security_enabled()
    {
        return Ok(false);
    }
    let settings = db.post_processing_settings()?;
    let disabled = execution_refusal(&settings, strict_security_enabled()).is_some();
    let hint =
        if disabled {
            AdmissionHint::Disabled
        } else if db.script_instances()?.iter().any(|instance| {
            instance.enabled && matches!(instance.trigger, InstanceTrigger::Queue(_))
        }) {
            AdmissionHint::Possible
        } else {
            AdmissionHint::NoConfiguredScripts
        };
    {
        let mut cache = db
            .script_runtime
            .admission_hint
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if cache.0 == revision {
            cache.1 = Some(hint);
        }
    }
    Ok(disabled)
}

pub fn has_subscriber(db: &Database, context: &EventContext) -> Result<bool, StateError> {
    if refresh_admission_hint(db)? {
        return Ok(false);
    }
    Ok(!selected_scripts(db, context)?.is_empty())
}

/// Run the event's instances in order. A scan script may move the download to
/// another category, so the instances are read again before each one. An
/// instance the event does not wait for is started at its place in the order
/// and left to finish on its own; it has no part in what this returns.
pub async fn run_event(
    db: &Database,
    context: &mut EventContext,
    run_id: &str,
    cancellation: Option<watch::Receiver<bool>>,
    supervisor_executable: Option<PathBuf>,
) -> Result<Vec<ScriptResult>, StateError> {
    let settings = db.post_processing_settings()?;
    if execution_refusal(&settings, strict_security_enabled()).is_some() {
        super::run_metrics::record_refusal(super::run_metrics::RunKind::of(&context.event, false));
        return Ok(Vec::new());
    }
    let runtime = db.script_runtime.clone();
    let (cancel_tx, mut cancel_rx) = watch::channel(
        cancellation
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow()),
    );
    runtime
        .cancellations
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(run_id.into(), (context.job_id, cancel_tx.clone()));
    let forwarding = cancellation.map(|mut external| {
        tokio::spawn(async move {
            while external.changed().await.is_ok() {
                if *external.borrow_and_update() {
                    cancel_tx.send_replace(true);
                    break;
                }
            }
        })
    });
    let _registration = RunRegistration {
        runtime: runtime.clone(),
        run_id: run_id.into(),
        forwarding,
    };
    if *cancel_rx.borrow() {
        return Ok(Vec::new());
    }
    let cancellation = Some(cancel_rx.clone());
    let mut root = db.post_processing_script_directory()?;
    let mut turn = None;
    let mut visited = BTreeSet::<String>::new();
    let mut results = Vec::new();
    loop {
        let settings = db.post_processing_settings()?;
        if execution_refusal(&settings, strict_security_enabled()).is_some() {
            break;
        }
        if let Some(job_id) = context.job_id {
            db.refresh_script_job_inputs(job_id, &mut context.facts)?;
        }
        let Some(entry) = selected_scripts(db, context)?
            .into_iter()
            .find(|entry| !visited.contains(&entry.id))
        else {
            break;
        };
        visited.insert(entry.id.clone());
        if cancellation.as_ref().is_some_and(|cancel| *cancel.borrow()) {
            break;
        }
        if context.event == ScriptEventLabel::Scan
            && context
                .env
                .get("NZBNP_FILENAME")
                .is_some_and(|path| !std::path::Path::new(path).exists())
        {
            break;
        }
        // The instance says what runs and when; nothing the script declares
        // about itself is consulted.
        let script =
            super::listing::resolve_script(&root, &entry.script).map_err(|error| error.to_string());
        if let Some(job_id) = context.job_id {
            let effects = db.job_script_effects(job_id)?;
            effects.merge_parameters(&mut context.facts.parameters);
            context.facts.marked_bad |= effects.marked_bad;
            if effects.marked_bad && context.event != ScriptEventLabel::Queue(QueueEvent::NzbMarked)
            {
                break;
            }
        }
        let run = EntryRun {
            entry,
            script,
            settings,
            run_id: run_id.into(),
            supervisor_executable: supervisor_executable.clone(),
            background: false,
        };
        if !run.entry.blocking {
            let run = EntryRun {
                background: true,
                ..run
            };
            if matches!(context.event, ScriptEventLabel::Scheduler(_)) {
                // Nothing waits for a scheduled run but the rule that started
                // it, which must still see it end to keep its runs from
                // overlapping. It only gives up its place among the scripts
                // that are waited for.
                turn = None;
                let kind = super::run_metrics::RunKind::of(&context.event, true);
                let Some(_turn) =
                    super::run_metrics::waiting(kind, background_turn(&runtime, &mut cancel_rx))
                        .await
                else {
                    break;
                };
                results.push(run_entry(db, context, run, cancellation.clone()).await?);
            } else {
                spawn_background_entry(db, context, run);
            }
            continue;
        }
        if turn.is_none() {
            let kind = super::run_metrics::RunKind::of(&context.event, false);
            let Some(acquired) =
                super::run_metrics::waiting(kind, event_turn(db, &runtime, &mut cancel_rx)).await?
            else {
                break;
            };
            turn = Some(acquired);
            // The wait may have been long. Everything this entry was chosen
            // from is read again now that its turn has come.
            root = db.post_processing_script_directory()?;
            visited.remove(&run.entry.id);
            continue;
        }
        results.push(run_entry(db, context, run, cancellation.clone()).await?);
    }
    Ok(results)
}

/// One instance of an event, ready to run. `script` is why it cannot, when
/// its script is not in the scripts directory.
struct EntryRun {
    entry: ScriptInstance,
    script: Result<DiscoveredScript, String>,
    settings: PostProcessingSettings,
    run_id: String,
    supervisor_executable: Option<PathBuf>,
    background: bool,
}

/// Start an entry nothing waits for. It takes its turn among the other
/// fire-and-forget runs and records its own result.
fn spawn_background_entry(db: &Database, context: &EventContext, mut run: EntryRun) {
    let registration = BackgroundRun::register(db, context.job_id);
    let db = db.clone();
    let mut context = context.clone();
    tokio::spawn(async move {
        let kind = super::run_metrics::RunKind::of(&context.event, true);
        let Some((_turn, cancellation)) =
            super::run_metrics::waiting(kind, registration.turn()).await
        else {
            return;
        };
        match db.post_processing_settings() {
            Ok(settings) if execution_refusal(&settings, strict_security_enabled()).is_none() => {
                run.settings = settings;
            }
            _ => return,
        }
        if let Some(job_id) = context.job_id
            && let Err(error) = db.refresh_script_job_inputs(job_id, &mut context.facts)
        {
            tracing::warn!(event = %context.event, %error, "could not refresh a fire-and-forget script's inputs");
        }
        if let Err(error) = run_entry(&db, &mut context, run, Some(cancellation)).await {
            tracing::warn!(event = %context.event, %error, "could not record a fire-and-forget script run");
        }
    });
}

/// Execute one entry and record what it did.
async fn run_entry(
    db: &Database,
    context: &mut EventContext,
    run: EntryRun,
    cancellation: Option<watch::Receiver<bool>>,
) -> Result<ScriptResult, StateError> {
    let EntryRun {
        entry,
        script,
        settings,
        run_id,
        supervisor_executable,
        background,
    } = run;
    let started = Instant::now();
    let _running = super::run_metrics::RunningGuard::enter(super::run_metrics::RunKind::of(
        &context.event,
        background,
    ));
    let mut not_started = script.is_err();
    let (adapter, status, exit_code, (output, output_bytes), output_truncated, error_message) =
        match script {
            // Nothing ran, so nothing failed: the operator is told, and whatever
            // raised the event goes on.
            Err(error) => (
                ScriptAdapter::Sabnzbd,
                ScriptStatus::Warning,
                None,
                (Vec::new(), 0),
                false,
                Some(error),
            ),
            Ok(script) => {
                let adapter = script.manifest.adapter();
                let prepared = db
                    .script_instance_run_inputs(&entry.id)
                    .map_err(|error| error.to_string())
                    .and_then(|inputs| {
                        inputs.ok_or_else(|| "the script job no longer exists".to_string())
                    })
                    .and_then(|inputs| {
                        RunIdentity::of(&entry)
                            .map(|identity| (inputs, identity))
                            .map_err(|error| error.to_string())
                    });
                let execution = match prepared {
                    Err(error) => {
                        not_started = true;
                        Err(error)
                    }
                    Ok((inputs, mut identity)) => {
                        let mut env = context.weaver_env();
                        env.extend(context.env.clone());
                        let timeout = entry.time_limit(&settings);
                        // The run is live, and its token good, until this is
                        // dropped at the end of the block.
                        let mut requests = db.open_script_run(
                            &mut identity,
                            context.job_id,
                            &context.event,
                            Some(timeout),
                            false,
                        );
                        let spec = ExecutionSpec {
                            manifest: script.manifest,
                            root: script.root,
                            options: inputs,
                            cwd: context.cwd.clone(),
                            env,
                            argv: Vec::new(),
                            timeout: Some(timeout),
                            termination_grace: Duration::from_secs(
                                settings.termination_grace_seconds,
                            ),
                            kind: context.event.clone(),
                            run_id,
                            identity,
                            facts: context.facts.clone(),
                            interpreters: InterpreterConfig {
                                python: settings.python_interpreter.as_ref().map(PathBuf::from),
                                powershell: settings
                                    .powershell_interpreter
                                    .as_ref()
                                    .map(PathBuf::from),
                                batch: settings.batch_interpreter.as_ref().map(PathBuf::from),
                                go: settings.go_interpreter.as_ref().map(PathBuf::from),
                            },
                            supervisor_executable,
                        };
                        let (sender, receiver) = mpsc::channel(64);
                        let (execution, ()) = tokio::join!(
                            execute_spec(spec, cancellation, Some(sender)),
                            consume_events(db, context, receiver, &mut requests),
                        );
                        execution
                            .map(|mut result| {
                                requests.settle(&mut result);
                                result
                            })
                            .map_err(|error| error.to_string())
                    }
                };
                match execution {
                    Ok(result) => (
                        adapter,
                        match result.disposition {
                            ExecutionDisposition::Succeeded => ScriptStatus::Succeeded,
                            ExecutionDisposition::Skipped => ScriptStatus::Skipped,
                            ExecutionDisposition::Failed => ScriptStatus::Failed,
                            ExecutionDisposition::TimedOut => ScriptStatus::TimedOut,
                            ExecutionDisposition::Cancelled => ScriptStatus::Cancelled,
                        },
                        result.exit_code,
                        (result.output, result.output_bytes),
                        result.output_truncated,
                        result.error_message,
                    ),
                    Err(error) => (
                        adapter,
                        ScriptStatus::Failed,
                        None,
                        (Vec::new(), 0),
                        false,
                        Some(error),
                    ),
                }
            }
        };
    let result = ScriptResult {
        script: entry.script,
        instance_id: Some(entry.id),
        instance_name: Some(entry.name),
        event: context.event.clone(),
        output_id: None,
        background,
        adapter,
        status,
        exit_code,
        duration_ms: started.elapsed().as_millis() as u64,
        output_tail: String::new(),
        output_truncated,
        error_message,
        finished_at_epoch_ms: chrono::Utc::now().timestamp_millis(),
    };
    super::run_metrics::record_finished(&result, !not_started);
    let result = super::output::retain_output(
        db.clone(),
        context.job_id,
        result,
        output,
        output_bytes,
        settings.event_scripts.clone(),
    )
    .await?;
    if matches!(
        result.status,
        ScriptStatus::Failed | ScriptStatus::TimedOut | ScriptStatus::Cancelled
    ) && let Some(job_id) = context.job_id
        && let Err(error) = db.insert_job_event(
            job_id,
            result.finished_at_epoch_ms,
            "ScriptWarning",
            &format!(
                "{} {}: {}",
                result.event,
                result.label(),
                result.status.as_str()
            ),
            None,
        )
    {
        // The result itself is retained; a missed timeline entry must not
        // stop the scripts after this one.
        tracing::warn!(%error, "could not record a script warning");
    }
    Ok(result)
}

/// Take what the script prints and what it asks for through the API until it
/// has ended. Both are applied here, one at a time.
async fn consume_events(
    db: &Database,
    context: &mut EventContext,
    mut receiver: mpsc::Receiver<ScriptOutputEvent>,
    requests: &mut RunRequests,
) {
    let mut logs = String::new();
    let mut severity = ScriptLogLevel::Debug;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await;
    loop {
        tokio::select! {
            event = receiver.recv() => match event {
                Some(ScriptOutputEvent::Directive(directive)) => {
                    if let Err(error) = apply_to_context(db, context, directive).await {
                        severity = severity.max(ScriptLogLevel::Warning);
                        append_log(&mut logs, &format!("Invalid command: {error}"));
                    }
                }
                Some(ScriptOutputEvent::Log { level, text }) => {
                    severity = severity.max(level);
                    append_log(&mut logs, &format!("{level:?}: {text}"));
                }
                None => break,
            },
            request = requests.next() => {
                let RunRequest { action, reply } = request;
                let outcome = match action {
                    RunAction::Command(directive) => apply_to_context(db, context, directive).await,
                    RunAction::Log { level, text } => requests.log(&text).map(|text| {
                        severity = severity.max(level);
                        append_log(&mut logs, &format!("{level:?}: {text}"));
                    }),
                    RunAction::Fail(reason) => requests.fail(&reason),
                };
                // The script may have stopped waiting for the answer.
                let _ = reply.send(outcome);
            }
            _ = tick.tick(), if !logs.is_empty() => { flush_logs(db, context, &mut logs, severity).await; severity = ScriptLogLevel::Debug; }
        }
    }
    // The script has ended: what is left goes now, not at the next tick.
    if !logs.is_empty() {
        flush_logs(db, context, &mut logs, severity).await;
    }
}

/// Apply one command to what the event is about. The context is left as it
/// was when the command is refused.
async fn apply_to_context(
    db: &Database,
    context: &mut EventContext,
    directive: Directive,
) -> Result<(), String> {
    let worker_db = db.clone();
    let mut next_context = context.clone();
    *context = tokio::task::spawn_blocking(move || {
        apply_directive(&worker_db, &mut next_context, directive)?;
        Ok::<_, String>(next_context)
    })
    .await
    .map_err(|error| error.to_string())
    .and_then(std::convert::identity)?;
    Ok(())
}

pub(super) fn append_log(logs: &mut String, text: &str) {
    const MARKER: &str = "\n[script log batch truncated]\n";
    if logs.ends_with(MARKER) {
        return;
    }
    if logs.len().saturating_add(text.len()).saturating_add(1) <= 8192 {
        logs.push_str(text);
        logs.push('\n');
        return;
    }
    let limit = 8192 - MARKER.len();
    if logs.len() > limit {
        let mut end = limit;
        while !logs.is_char_boundary(end) {
            end -= 1;
        }
        logs.truncate(end);
    } else {
        let mut end = text.len().min(limit - logs.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        logs.push_str(&text[..end]);
    }
    logs.push_str(MARKER);
}

async fn flush_logs(
    db: &Database,
    context: &EventContext,
    logs: &mut String,
    severity: ScriptLogLevel,
) {
    if let Some(job_id) = context.job_id {
        record_log_batch(db, job_id, std::mem::take(logs), severity).await;
    } else {
        tracing::info!(event = %context.event, output = %logs, "script output");
        logs.clear();
    }
}

pub(super) async fn record_log_batch(
    db: &Database,
    job_id: u64,
    logs: String,
    severity: ScriptLogLevel,
) {
    let db = db.clone();
    match tokio::task::spawn_blocking(move || {
        db.insert_job_event(
            job_id,
            chrono::Utc::now().timestamp_millis(),
            severity.event_kind(),
            &logs,
            None,
        )
    })
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => tracing::warn!(%error, "could not record script log"),
        Err(error) => tracing::warn!(%error, "script log persistence task failed"),
    }
}

fn apply_directive(
    db: &Database,
    context: &mut EventContext,
    directive: Directive,
) -> Result<(), String> {
    if let Some(job_id) = context.job_id {
        let mut job = JobExecutionContext {
            job_id,
            name: String::new(),
            nzb_filename: String::new(),
            category: context.category.clone(),
            group: None,
            source_url: None,
            working_directory: context.cwd.clone(),
            final_directory: context.cwd.clone(),
            pipeline_outcome: super::model::PipelineOutcome::Succeeded,
            par_status: 0,
            unpack_status: 0,
            compatibility: context.facts.clone(),
        };
        let directive = if let Directive::Directory(path) = directive {
            Directive::FinalDirectory(path)
        } else {
            directive
        };
        super::effects::apply_queue_directive(db, &mut job, directive)?;
        context.facts = job.compatibility;
        return Ok(());
    }
    match directive {
        Directive::Parameter { name, value } => {
            let mut parameters = context.facts.parameters.clone();
            parameters.retain(|(key, _)| key != &name);
            if !value.is_empty() {
                parameters.push((name, value));
            }
            super::directives::validate_parameter_size(
                parameters
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str())),
            )?;
            context.facts.parameters = parameters;
        }
        Directive::Name(value) => {
            context.env.insert("NZBNP_NZBNAME".into(), value);
        }
        Directive::Category(value) => {
            context.category = Some(value.clone());
            context.env.insert("NZBNP_CATEGORY".into(), value);
        }
        Directive::Priority(value) => {
            context
                .env
                .insert("NZBNP_PRIORITY".into(), value.to_string());
        }
        Directive::Top(value) => {
            context
                .env
                .insert("NZBNP_TOP".into(), i32::from(value).to_string());
        }
        Directive::Paused(value) => {
            context
                .env
                .insert("NZBNP_PAUSED".into(), i32::from(value).to_string());
        }
        Directive::DupeKey(value) => {
            context.env.insert("NZBNP_DUPEKEY".into(), value);
        }
        Directive::DupeScore(value) => {
            context
                .env
                .insert("NZBNP_DUPESCORE".into(), value.to_string());
        }
        Directive::DupeMode(value) => {
            context.env.insert(
                "NZBNP_DUPEMODE".into(),
                format!("{value:?}").to_ascii_uppercase(),
            );
        }
        _ => return Err("command requires a job".into()),
    }
    Ok(())
}

impl Database {
    pub(crate) fn invalidate_queue_script_admission(&self) {
        let mut cache = self
            .script_runtime
            .admission_hint
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        cache.0 = cache.0.wrapping_add(1);
        cache.1 = None;
        drop(cache);
        let mut dispatch = self
            .script_runtime
            .dispatch_jobs
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        dispatch.0 = dispatch.0.wrapping_add(1);
        dispatch.1 = None;
        drop(dispatch);
        let mut jobs = self
            .script_runtime
            .schedule_jobs
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        jobs.0 = jobs.0.wrapping_add(1);
        jobs.1 = None;
        drop(jobs);
        // Refresh on the cold settings-write path, so arrival decisions can
        // distinguish blocking scripts without holding the actor behind SQL.
        if let Err(error) = self.warm_script_dispatch_jobs() {
            tracing::warn!(%error, "script dispatch cache could not be refreshed");
        }
    }

    pub(crate) fn queue_scripts_possible(&self) -> bool {
        !matches!(
            self.script_runtime
                .admission_hint
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .1,
            Some(AdmissionHint::Disabled | AdmissionHint::NoConfiguredScripts)
        )
    }

    pub(crate) fn queue_barrier_possible(&self, job_id: u64) -> bool {
        self.queue_scripts_possible()
            || self
                .script_runtime
                .durable_events_seen
                .load(std::sync::atomic::Ordering::Acquire)
            || self.has_pending_script_admission(job_id)
    }

    /// Preserve actor event order while resolving manifests and writing SQL off
    /// the actor. Only one pending file notification per job is necessary.
    pub(crate) fn admit_queue_script_event(
        &self,
        context: EventContext,
        barrier: bool,
    ) -> tokio::sync::oneshot::Receiver<Result<Option<String>, StateError>> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut admissions = self
            .script_runtime
            .admissions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !barrier
            && context.event == ScriptEventLabel::Queue(QueueEvent::FileDownloaded)
            && admissions.pending.iter().any(|entry| {
                entry.context.job_id == context.job_id && entry.context.event == context.event
            })
        {
            let _ = sender.send(Ok(None));
            return receiver;
        }
        admissions.pending.push_back(QueueAdmission {
            context,
            completion: Some(sender),
        });
        if !admissions.running {
            admissions.running = true;
            let db = self.clone();
            tokio::spawn(async move {
                drain_admissions(db).await;
            });
        }
        receiver
    }

    fn has_pending_script_admission(&self, job_id: u64) -> bool {
        let admissions = self
            .script_runtime
            .admissions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        admissions.active_job == Some(job_id)
            || admissions
                .pending
                .iter()
                .any(|entry| entry.context.job_id == Some(job_id))
    }

    pub(crate) fn subscribe_script_effects(&self) -> watch::Receiver<()> {
        let (sender, receiver) = watch::channel(());
        self.script_runtime
            .effects
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .1 = Some(sender);
        receiver
    }

    pub(crate) fn notify_script_effects(&self, job_id: u64) {
        let mut effects = self
            .script_runtime
            .effects
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if effects.1.is_some() && effects.0.insert(job_id) {
            effects
                .1
                .as_ref()
                .expect("subscriber checked")
                .send_replace(());
        }
    }

    pub(crate) fn take_script_effects(&self) -> BTreeSet<u64> {
        std::mem::take(
            &mut self
                .script_runtime
                .effects
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .0,
        )
    }
    pub fn cancel_event_scripts(&self, job_id: u64) {
        for registry in [
            &self.script_runtime.claimed,
            &self.script_runtime.cancellations,
        ] {
            for (job, sender) in registry
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .values()
            {
                if *job == Some(job_id) {
                    sender.send_replace(true);
                }
            }
        }
        self.cancel_background_scripts(job_id);
    }

    /// Signal every fire-and-forget run of `job_id` to stop. Returns whether
    /// there was one.
    pub fn cancel_background_scripts(&self, job_id: u64) -> bool {
        let mut cancelled = false;
        for (job, sender) in self
            .script_runtime
            .background
            .runs
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
        {
            if *job == Some(job_id) {
                sender.send_replace(true);
                cancelled = true;
            }
        }
        cancelled
    }

    /// Resolves once no fire-and-forget run is registered. Nothing in weaver
    /// waits on this; it exists so a test can.
    #[doc(hidden)]
    pub async fn background_scripts_settled(&self) {
        loop {
            let settled = self.script_runtime.background.settled.notified();
            tokio::pin!(settled);
            settled.as_mut().enable();
            if self
                .script_runtime
                .background
                .runs
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
            {
                return;
            }
            settled.await;
        }
    }
    pub fn enqueue_script_event(
        &self,
        context: &EventContext,
        now_ms: i64,
    ) -> Result<Option<String>, StateError> {
        self.enqueue_script_event_inner(context, now_ms, false)
    }

    fn enqueue_script_event_inner(
        &self,
        context: &EventContext,
        now_ms: i64,
        require_job: bool,
    ) -> Result<Option<String>, StateError> {
        let ScriptEventLabel::Queue(event) = context.event else {
            return Err(StateError::Database("only queue events are durable".into()));
        };
        if !has_subscriber(self, context)? {
            return Ok(None);
        }
        self.script_runtime
            .durable_events_seen
            .store(true, std::sync::atomic::Ordering::Release);
        let interval = self
            .post_processing_settings()?
            .event_scripts
            .file_downloaded_event_interval;
        if event == QueueEvent::FileDownloaded && interval < 0 {
            return Ok(None);
        }
        let payload = serde_json::to_string(context)
            .map_err(|error| StateError::Database(error.to_string()))?;
        let job_id = context.job_id.map(|id| id as i64);
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "enqueue_script_event", |tx| {
                let payload = payload.clone();
                Box::pin(async move {
                    let lock = match tx {
                        crate::persistence::sql_runtime::SqlTx::Postgres(_) => "SELECT next_seq FROM script_output_state WHERE singleton = 1 FOR UPDATE",
                        crate::persistence::sql_runtime::SqlTx::Sqlite(_) => "SELECT next_seq FROM script_output_state WHERE singleton = 1",
                    };
                    tx.fetch_optional(lock, &[]).await?.ok_or_else(|| StateError::Database("script sequence is missing".into()))?;
                    if require_job && let Some(job_id) = job_id
                        && tx.fetch_optional("SELECT job_id FROM active_jobs WHERE job_id = {} UNION ALL SELECT job_id FROM job_history WHERE job_id = {} LIMIT 1", &[SqlArg::I64(job_id), SqlArg::I64(job_id)]).await?.is_none() {
                        return Ok(None);
                    }
                    if event == QueueEvent::FileDownloaded
                        && tx.fetch_optional("SELECT run_id FROM script_event_queue WHERE job_id IS NOT DISTINCT FROM {} AND event = {} AND (state = 'queued' OR created_at > {}) LIMIT 1", &[SqlArg::OptI64(job_id), SqlArg::Text(event.as_str().into()), SqlArg::I64(if interval == 0 { i64::MAX } else { now_ms.saturating_sub(interval.saturating_mul(1000)) })]).await?.is_some() { return Ok(None); }
                    let seq = tx.fetch_optional("UPDATE script_output_state SET next_seq = next_seq + 1 WHERE singleton = 1 RETURNING next_seq", &[]).await?.ok_or_else(|| StateError::Database("script sequence is missing".into()))?.i64("next_seq")?;
                    if event == QueueEvent::NzbDownloaded {
                        tx.execute("DELETE FROM script_event_queue WHERE job_id IS NOT DISTINCT FROM {} AND state = 'queued'", &[SqlArg::OptI64(job_id)]).await?;
                    }
                    let run_id = format!("script-event-{seq}");
                    tx.execute("INSERT INTO script_event_queue (run_id, job_id, event, priority, seq, state, payload, created_at) VALUES ({}, {}, {}, {}, {}, 'queued', {}, {})", &[SqlArg::Text(run_id.clone()), SqlArg::OptI64(job_id), SqlArg::Text(event.as_str().into()), SqlArg::I32(event as i32), SqlArg::I64(seq), SqlArg::Text(payload), SqlArg::I64(now_ms)]).await?;
                    Ok(Some(run_id))
                })
            }).await
        })
    }

    pub fn queue_script_count(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            Ok(SqlRuntime::fetch_optional(datastore.read_exec(), "SELECT COUNT(*) AS count FROM script_event_queue WHERE state IN ('queued', 'started')", &[]).await?.map(|row| row.i64("count")).transpose()?.unwrap_or(0) as u64)
        })
    }

    pub fn recover_script_events(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        let seen = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "recover_script_events", |tx| Box::pin(async move {
                tx.execute("UPDATE script_event_queue SET state = 'interrupted' WHERE state = 'started'", &[]).await?;
                Ok(tx.fetch_optional("SELECT run_id FROM script_event_queue LIMIT 1", &[]).await?.is_some())
            })).await
        })?;
        self.script_runtime
            .durable_events_seen
            .store(seen, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub fn script_event_finished(&self, run_id: &str) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let run_id = run_id.to_string();
        self.run_sql_blocking_read(async move {
            Ok(SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT state FROM script_event_queue WHERE run_id = {}",
                &[SqlArg::Text(run_id)],
            )
            .await?
            .map(|row| row.text("state"))
            .transpose()?
            .is_none_or(|state| state == "done" || state == "interrupted"))
        })
    }

    pub fn downloaded_script_event(
        &self,
        job_id: u64,
    ) -> Result<Option<(String, bool)>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(datastore.read_exec(), "SELECT run_id, state FROM script_event_queue WHERE job_id = {} AND event = 'NZB_DOWNLOADED' ORDER BY seq DESC LIMIT 1", &[SqlArg::I64(job_id as i64)]).await?
                .map(|row| Ok((row.text("run_id")?, matches!(row.text("state")?.as_str(), "done" | "interrupted")))).transpose()
        })
    }

    fn claim_script_event(
        &self,
    ) -> Result<Option<(String, EventContext, QueueClaimRegistration)>, StateError> {
        let datastore = self.datastore();
        let runtime = self.script_runtime.clone();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "claim_script_event", |tx| {
                let runtime = runtime.clone();
                Box::pin(async move {
                tx.execute("UPDATE script_output_state SET next_seq = next_seq WHERE singleton = 1", &[]).await?;
                loop {
                let Some(row) = tx.fetch_optional("SELECT run_id, payload FROM script_event_queue WHERE state = 'queued' ORDER BY priority DESC, seq LIMIT 1", &[]).await? else { return Ok(None); };
                let run_id = row.text("run_id")?;
                let context: EventContext = match serde_json::from_str(&row.text("payload")?) {
                    Ok(context) => context,
                    Err(error) => {
                        let payload = serde_json::json!({"script_event_error": format!("queue event payload is invalid: {error}")}).to_string();
                        tx.execute("UPDATE script_event_queue SET state = 'done', payload = {} WHERE run_id = {}", &[SqlArg::Text(payload), SqlArg::Text(run_id)]).await?;
                        continue;
                    }
                };
                tx.execute("UPDATE script_event_queue SET state = 'started' WHERE run_id = {}", &[SqlArg::Text(run_id.clone())]).await?;
                // Register before the claim commits: deletion takes the same
                // SQL state lock and cannot erase the row in an admission gap.
                let (cancel, cancellation) = watch::channel(false);
                runtime.claimed.lock().unwrap_or_else(|error| error.into_inner()).insert(run_id.clone(), (context.job_id, cancel));
                let claim = QueueClaimRegistration { runtime, run_id: run_id.clone(), cancellation };
                return Ok(Some((run_id, context, claim)));
                }
            })}).await
        });
        self.notify_script_events_changed();
        result
    }

    pub(crate) fn notify_script_events_changed(&self) {
        self.script_runtime.changed.notify_waiters();
    }

    fn script_run_active(&self, run_id: &str) -> bool {
        self.script_runtime
            .claimed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(run_id)
            || self
                .script_runtime
                .cancellations
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .contains_key(run_id)
    }

    fn active_script_run_for_job(&self, job_id: u64) -> Option<String> {
        self.script_runtime
            .claimed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .find(|(_, (job, _))| *job == Some(job_id))
            .map(|(run, _)| run.clone())
            .or_else(|| {
                self.script_runtime
                    .cancellations
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .iter()
                    .find(|(_, (job, _))| *job == Some(job_id))
                    .map(|(run, _)| run.clone())
            })
    }

    #[cfg(test)]
    pub(crate) fn finish_script_event_for_test(&self, run_id: &str) -> Result<(), StateError> {
        self.finish_script_event(run_id)?;
        self.script_runtime.changed.notify_waiters();
        Ok(())
    }

    fn finish_script_event(&self, run_id: &str) -> Result<(), StateError> {
        self.finish_script_event_with_error(run_id, None)
    }

    fn finish_script_event_with_error(
        &self,
        run_id: &str,
        error: Option<String>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let finished_run = run_id.to_string();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "finish_script_event", |tx| {
                let run_id = finished_run.clone();
                let error = error.clone();
                Box::pin(async move {
                    if let Some(error) = error {
                        tx.execute("UPDATE script_event_queue SET payload = {} WHERE run_id = {}", &[SqlArg::Text(serde_json::json!({"script_event_error": error}).to_string()), SqlArg::Text(run_id.clone())]).await?;
                    }
                    tx.execute("UPDATE script_event_queue SET state = 'done' WHERE run_id = {}", &[SqlArg::Text(run_id.clone())]).await?;
                    tx.execute("DELETE FROM script_event_queue WHERE run_id IN (SELECT older.run_id FROM script_event_queue older JOIN script_event_queue latest ON older.job_id IS NOT DISTINCT FROM latest.job_id AND older.event = latest.event WHERE latest.run_id = {} AND older.seq < latest.seq AND older.state IN ('done', 'interrupted') LIMIT 128)", &[SqlArg::Text(run_id)]).await?;
                    Ok(())
                })
            }).await
        });
        if let Err(error) = &result {
            self.script_runtime
                .failed_runs
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .insert(run_id.into(), error.to_string());
        } else {
            self.script_runtime
                .failed_runs
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(run_id);
        }
        self.script_runtime
            .claimed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(run_id);
        self.notify_script_events_changed();
        result
    }

    pub(crate) fn script_event_error(&self, run_id: &str) -> Result<Option<String>, StateError> {
        if let Some(error) = self
            .script_runtime
            .failed_runs
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(run_id)
            .cloned()
        {
            return Ok(Some(error));
        }
        let datastore = self.datastore();
        let run_id = run_id.to_string();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT payload FROM script_event_queue WHERE run_id = {} AND state = 'done'",
                &[SqlArg::Text(run_id)],
            )
            .await?;
            Ok(row
                .map(|row| row.text("payload"))
                .transpose()?
                .and_then(|payload| serde_json::from_str::<serde_json::Value>(&payload).ok())
                .and_then(|value| {
                    value
                        .get("script_event_error")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                }))
        })
    }
}

pub async fn wait_for_event(db: &Database, run_id: &str) -> Result<(), StateError> {
    wake_queue(db.clone());
    loop {
        let changed = db.script_runtime.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let worker_db = db.clone();
        let worker_run = run_id.to_string();
        let (finished, error) = tokio::task::spawn_blocking(move || -> Result<_, StateError> {
            Ok((
                worker_db.script_event_finished(&worker_run)?,
                worker_db.script_event_error(&worker_run)?,
            ))
        })
        .await
        .map_err(|error| StateError::Database(error.to_string()))??;
        if !db.script_run_active(run_id) {
            if let Some(error) = error {
                return Err(StateError::Database(error));
            }
            if finished {
                return Ok(());
            }
        }
        changed.await;
    }
}

/// Terminal scripts also wait for deletion/health queue events. A streamed BAD
/// directive stops downloading immediately without racing the remaining lines
/// of that queue run against terminal post-processing and archival.
pub async fn wait_for_job_events(db: &Database, job_id: u64) -> Result<(), StateError> {
    wait_for_job_events_inner(db, job_id, true).await
}

pub(crate) async fn wait_for_job_events_stopped(
    db: &Database,
    job_id: u64,
) -> Result<(), StateError> {
    wait_for_job_events_inner(db, job_id, false).await
}

async fn wait_for_job_events_inner(
    db: &Database,
    job_id: u64,
    report_outcome_errors: bool,
) -> Result<(), StateError> {
    let mut failure = None;
    loop {
        let changed = db.script_runtime.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if db.has_pending_script_admission(job_id) {
            changed.await;
            continue;
        }
        let worker_db = db.clone();
        let pending = tokio::task::spawn_blocking(move || {
        let datastore = worker_db.datastore();
        worker_db.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(datastore.read_exec(), "SELECT run_id FROM script_event_queue WHERE job_id = {} AND state IN ('queued', 'started') ORDER BY seq LIMIT 1", &[SqlArg::I64(job_id as i64)]).await?.map(|row| row.text("run_id")).transpose()
        })
        }).await.map_err(|error| StateError::Database(error.to_string()))??;
        let Some(run_id) = pending.or_else(|| db.active_script_run_for_job(job_id)) else {
            return failure.map_or(Ok(()), Err);
        };
        if let Err(error) = wait_for_event(db, &run_id).await {
            if report_outcome_errors {
                failure.get_or_insert(error);
            }
            let worker_db = db.clone();
            let finished =
                tokio::task::spawn_blocking(move || worker_db.script_event_finished(&run_id))
                    .await
                    .map_err(|error| StateError::Database(error.to_string()))??;
            if !finished {
                changed.await;
            }
        }
    }
}

async fn drain_admissions(db: Database) {
    loop {
        let admission = {
            let mut admissions = db
                .script_runtime
                .admissions
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let Some(admission) = admissions.pending.pop_front() else {
                admissions.running = false;
                admissions.active_job = None;
                db.notify_script_events_changed();
                return;
            };
            admissions.active_job = admission.context.job_id;
            admission
        };
        let worker_db = db.clone();
        let result = tokio::task::spawn_blocking(move || {
            if admission.context.event == ScriptEventLabel::Queue(QueueEvent::NzbDownloaded)
                && let Some(job_id) = admission.context.job_id
                && let Some((run_id, _)) = worker_db.downloaded_script_event(job_id)?
            {
                return Ok(Some(run_id));
            }
            worker_db.enqueue_script_event_inner(
                &admission.context,
                chrono::Utc::now().timestamp_millis(),
                true,
            )
        })
        .await
        .unwrap_or_else(|error| {
            Err(StateError::Database(format!(
                "queue script admission task failed: {error}"
            )))
        });
        if matches!(&result, Ok(Some(_))) {
            wake_queue(db.clone());
        }
        if let Err(error) = &result {
            tracing::warn!(%error, "could not admit queue script event");
        }
        if let Some(completion) = admission.completion {
            let _ = completion.send(result);
        }
        db.script_runtime
            .admissions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active_job = None;
        db.notify_script_events_changed();
    }
}

pub async fn drain_queue(db: Database) -> Result<(), StateError> {
    let _worker = db.script_runtime.queue.lock().await;
    // Retry the single failed terminal write before claiming another item. A
    // persistent SQL failure therefore cannot grow the fallback error cache.
    let failures = db
        .script_runtime
        .failed_runs
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    for (run_id, error) in failures {
        let worker_db = db.clone();
        tokio::task::spawn_blocking(move || {
            worker_db.finish_script_event_with_error(&run_id, Some(error))
        })
        .await
        .map_err(|error| StateError::Database(error.to_string()))??;
    }
    loop {
        let worker_db = db.clone();
        let Some((run_id, mut context, claim)) =
            tokio::task::spawn_blocking(move || worker_db.claim_script_event())
                .await
                .map_err(|error| StateError::Database(error.to_string()))??
        else {
            break;
        };
        let execution: Result<(), StateError> = async {
            let deleted = if let Some(job_id) = context.job_id {
                let worker_db = db.clone();
                tokio::task::spawn_blocking(move || {
                    let datastore = worker_db.datastore();
                    worker_db.run_sql_blocking_read(async move {
                        Ok(SqlRuntime::fetch_optional(
                            datastore.read_exec(),
                            "SELECT job_id FROM active_jobs WHERE job_id = {}",
                            &[SqlArg::I64(job_id as i64)],
                        )
                        .await?
                        .is_none())
                    })
                })
                .await
                .map_err(|error| StateError::Database(error.to_string()))??
            } else {
                false
            };
            let allow_deleted = matches!(
                context.event,
                ScriptEventLabel::Queue(QueueEvent::NzbDeleted | QueueEvent::NzbMarked)
            );
            if !deleted || allow_deleted {
                run_event(
                    &db,
                    &mut context,
                    &run_id,
                    Some(claim.cancellation.clone()),
                    None,
                )
                .await?;
            }
            Ok(())
        }
        .await;
        let error = execution.err().map(|error| error.to_string());
        if let Some(error) = &error {
            tracing::warn!(%run_id, %error, "queue script run failed");
            // Keep the failure on the job's timeline; the job itself carries on.
            if let Some(job_id) = context.job_id {
                let worker_db = db.clone();
                let message = format!("{}: scripts could not run: {error}", context.event);
                let recorded = tokio::task::spawn_blocking(move || {
                    worker_db.insert_job_event(
                        job_id,
                        chrono::Utc::now().timestamp_millis(),
                        "ScriptWarning",
                        &message,
                        None,
                    )
                })
                .await;
                if !matches!(recorded, Ok(Ok(()))) {
                    tracing::warn!(%run_id, ?recorded, "could not record the queue script failure");
                }
            }
        }
        let worker_db = db.clone();
        tokio::task::spawn_blocking(move || match error {
            Some(error) => worker_db.finish_script_event_with_error(&run_id, Some(error)),
            None => worker_db.finish_script_event(&run_id),
        })
        .await
        .map_err(|error| StateError::Database(error.to_string()))??;
        db.script_runtime.changed.notify_waiters();
    }
    Ok(())
}

pub fn wake_queue(db: Database) {
    use std::sync::atomic::Ordering::SeqCst;
    db.script_runtime.queue_requested.store(true, SeqCst);
    if db.script_runtime.worker_active.swap(true, SeqCst) {
        return;
    }
    tokio::spawn(async move {
        loop {
            db.script_runtime.queue_requested.store(false, SeqCst);
            if let Err(error) = drain_queue(db.clone()).await {
                db.script_runtime.changed.notify_waiters();
                tracing::warn!(%error, "queue script coordinator failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            db.script_runtime.worker_active.store(false, SeqCst);
            if !db.script_runtime.queue_requested.load(SeqCst)
                || db.script_runtime.worker_active.swap(true, SeqCst)
            {
                break;
            }
        }
    });
}

#[cfg(test)]
#[path = "event_tests.rs"]
mod tests;

#[cfg(test)]
pub(crate) fn hold_test_event_run(db: &Database, job_id: u64, run_id: &str) -> impl Drop + use<> {
    let (cancel, _receiver) = watch::channel(false);
    db.script_runtime
        .cancellations
        .lock()
        .unwrap()
        .insert(run_id.into(), (Some(job_id), cancel));
    RunRegistration {
        runtime: db.script_runtime.clone(),
        run_id: run_id.into(),
        forwarding: None,
    }
}
