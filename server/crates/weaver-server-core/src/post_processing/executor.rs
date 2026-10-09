//! Bounded, sequential execution of a job's post-processing instances.
//!
//! This is the whole scheduler: a semaphore sized by the concurrency setting
//! admits jobs, and each admitted job runs its scripts one after another. The
//! semaphore's FIFO is the queue, exactly as SABnzbd's post-processing worker
//! and NZBGet's post thread are.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};

use super::instances::ScriptInstance;
use super::listing::{self, ListingError};
use super::model::{
    PostProcessingSummary, ScriptAdapter, ScriptEventLabel, ScriptResult, ScriptStatus,
    merge_post_processing_summary,
};
use super::runner::{
    ExecutionDisposition, InterpreterConfig, JobExecutionContext, NzbgetScriptStatus, RunIdentity,
    ScriptExecutionRequest, execute_script_observed,
};
use crate::persistence::{Database, StateError};

const MAX_CONCURRENCY: usize = 8;
pub const SCRIPT_EVENT_KIND: &str = "PostProcessingScript";
pub const SCRIPT_OUTPUT_EVENT_KIND: &str = "PostProcessingScriptOutput";

type CancellationRegistry = Arc<Mutex<HashMap<u64, watch::Sender<bool>>>>;

/// Process-local post-processing counters.
///
/// The run/attempt tables that used to answer the `/metrics` scrape are gone,
/// so the same series are served from the executor itself — the same shape as
/// the duplicate-admission counters elsewhere in this crate. Gauges are exact;
/// counters reset with the process, which is what a Prometheus counter contract
/// already expects.
mod counters {
    use super::AtomicU64;

    pub(super) static QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
    pub(super) static ACTIVE: AtomicU64 = AtomicU64::new(0);
    pub(super) static DURATION_COUNT: AtomicU64 = AtomicU64::new(0);
    pub(super) static DURATION_SUM_MILLIS: AtomicU64 = AtomicU64::new(0);
    pub(super) static SUCCEEDED: AtomicU64 = AtomicU64::new(0);
    pub(super) static FAILED: AtomicU64 = AtomicU64::new(0);
    pub(super) static SKIPPED: AtomicU64 = AtomicU64::new(0);
    pub(super) static TIMED_OUT: AtomicU64 = AtomicU64::new(0);
    pub(super) static CANCELLED: AtomicU64 = AtomicU64::new(0);
    pub(super) static INTERRUPTED: AtomicU64 = AtomicU64::new(0);
    pub(super) static TRUNCATED: AtomicU64 = AtomicU64::new(0);
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct PostProcessingMetricsSnapshot {
    pub queue_depth: u64,
    pub active_attempts: u64,
    pub duration_count: u64,
    pub duration_sum_millis: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub skipped: u64,
    pub timed_out: u64,
    pub cancelled: u64,
    pub interrupted: u64,
    pub truncated: u64,
}

pub fn metrics_snapshot() -> PostProcessingMetricsSnapshot {
    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    PostProcessingMetricsSnapshot {
        queue_depth: load(&counters::QUEUE_DEPTH),
        active_attempts: load(&counters::ACTIVE),
        duration_count: load(&counters::DURATION_COUNT),
        duration_sum_millis: load(&counters::DURATION_SUM_MILLIS),
        succeeded: load(&counters::SUCCEEDED),
        failed: load(&counters::FAILED),
        skipped: load(&counters::SKIPPED),
        timed_out: load(&counters::TIMED_OUT),
        cancelled: load(&counters::CANCELLED),
        interrupted: load(&counters::INTERRUPTED),
        truncated: load(&counters::TRUNCATED),
    }
}

fn record_script_metrics(result: &ScriptResult) {
    counters::DURATION_COUNT.fetch_add(1, Ordering::Relaxed);
    counters::DURATION_SUM_MILLIS.fetch_add(result.duration_ms, Ordering::Relaxed);
    let counter = match result.status {
        ScriptStatus::Succeeded => &counters::SUCCEEDED,
        ScriptStatus::Skipped => &counters::SKIPPED,
        ScriptStatus::Warning | ScriptStatus::Failed => &counters::FAILED,
        ScriptStatus::TimedOut => &counters::TIMED_OUT,
        ScriptStatus::Cancelled => &counters::CANCELLED,
    };
    counter.fetch_add(1, Ordering::Relaxed);
    if result.output_truncated {
        counters::TRUNCATED.fetch_add(1, Ordering::Relaxed);
    }
}

/// Guard that keeps a gauge honest across every early return.
struct GaugeGuard(&'static AtomicU64);

impl GaugeGuard {
    fn enter(gauge: &'static AtomicU64) -> Self {
        gauge.fetch_add(1, Ordering::Relaxed);
        Self(gauge)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PostProcessingExecutorError {
    #[error("post-processing persistence failed: {0}")]
    State(#[from] StateError),
    #[error("post-processing executor was shut down")]
    Shutdown,
}

/// What the job's post-processing produced.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JobPostProcessingReport {
    pub summary: PostProcessingSummary,
    pub results: Vec<ScriptResult>,
}

/// The scripts root and the instances captured when a job enters
/// post-processing. An instance's inputs are read when its turn comes.
#[derive(Clone)]
pub struct PostProcessingJobAdmission {
    scripts_directory: PathBuf,
    scripts: Vec<ScriptInstance>,
}

impl PostProcessingJobAdmission {
    pub fn has_enabled_entries(&self) -> bool {
        self.scripts.iter().any(|instance| instance.enabled)
    }
}

#[derive(Clone)]
pub struct PostProcessingExecutor {
    db: Database,
    scripts_directory: Arc<RwLock<PathBuf>>,
    concurrency: Arc<Semaphore>,
    /// Admission gate for the NZBGet facade's `pausepost`/`resumepost`, which is
    /// the only reason a pause survives: it gates admission, never a running
    /// script, exactly as the RPC has always behaved.
    paused: watch::Sender<bool>,
    cancellations: CancellationRegistry,
    /// Test hook: integration tests point this at the built `weaver` binary,
    /// because a test harness cannot serve as its own process supervisor.
    #[doc(hidden)]
    supervisor_executable: Option<PathBuf>,
}

/// What became of one entry of a job's list.
enum Attempt {
    /// The script ran. Its result and output are already kept.
    Ran(ScriptResult),
    /// The script is not a post-processing script.

    /// The script could not be started.
    NotStarted(ScriptResult),
}

struct CancellationRegistration {
    job_id: u64,
    registry: CancellationRegistry,
    forwarder: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        self.registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.job_id);
        if let Some(forwarder) = self.forwarder.take() {
            forwarder.abort();
        }
    }
}

impl PostProcessingExecutor {
    /// `concurrency` sizes the admission semaphore for the process lifetime; a
    /// changed setting takes effect on the next restart, as it did before.
    pub fn new(db: Database, scripts_directory: PathBuf, concurrency: usize) -> Self {
        let (paused, _) = watch::channel(false);
        Self {
            db,
            scripts_directory: Arc::new(RwLock::new(scripts_directory)),
            concurrency: Arc::new(Semaphore::new(concurrency.clamp(1, MAX_CONCURRENCY))),
            paused,
            cancellations: Arc::new(Mutex::new(HashMap::new())),
            supervisor_executable: None,
        }
    }

    #[doc(hidden)]
    pub fn with_supervisor_executable(mut self, executable: PathBuf) -> Self {
        self.supervisor_executable = Some(executable);
        self
    }

    pub fn pause(&self) {
        self.paused.send_replace(true);
    }

    pub fn resume(&self) {
        self.paused.send_replace(false);
    }

    pub fn is_paused(&self) -> bool {
        *self.paused.borrow()
    }

    /// Future post-processing jobs use `directory`; jobs that already entered
    /// execution retain their admission-time snapshot.
    pub fn set_script_directory(&self, directory: PathBuf) {
        *self
            .scripts_directory
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = directory;
    }

    /// Snapshot the configured root for a post-processing admission.
    pub fn script_directory(&self) -> PathBuf {
        self.scripts_directory
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Signal every in-flight script for `job_id` to stop, the ones nothing
    /// waits for included. Returns whether a pass was there to stop.
    pub fn cancel_job(&self, job_id: u64) -> bool {
        self.db.cancel_background_scripts(job_id);
        let sender = self
            .cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&job_id)
            .cloned();
        match sender {
            Some(sender) => {
                sender.send_replace(true);
                true
            }
            None => false,
        }
    }

    /// One statement marking jobs that were mid-post-processing when weaver stopped.
    pub fn recover_interrupted(&self) -> Result<u64, StateError> {
        let interrupted = self.db.recover_interrupted_post_processing()?;
        counters::INTERRUPTED.fetch_add(interrupted, Ordering::Relaxed);
        Ok(interrupted)
    }

    /// The instances a job in `category` should run, without executing
    /// anything.
    pub fn resolve_job_scripts(
        &self,
        category: Option<&str>,
    ) -> Result<Vec<ScriptInstance>, StateError> {
        self.db
            .script_instances_for(&ScriptEventLabel::PostProcessing, category)
    }

    /// Capture the root and the instances for a newly admitted job.
    pub fn admit_job_scripts(
        &self,
        category: Option<&str>,
    ) -> Result<Option<PostProcessingJobAdmission>, StateError> {
        let (settings, script_directory) = self.db.post_processing_script_admission()?;
        if !settings.execution_enabled {
            return Ok(None);
        }
        Ok(Some(PostProcessingJobAdmission {
            scripts_directory: script_directory,
            scripts: self.resolve_job_scripts(category)?,
        }))
    }

    pub fn execution_enabled(&self) -> Result<bool, StateError> {
        Ok(self.db.post_processing_settings()?.execution_enabled)
    }

    /// Run `scripts` for one job, sequentially, under the concurrency semaphore.
    pub async fn execute_job(
        &self,
        job_id: u64,
        scripts: Vec<ScriptInstance>,
        context: JobExecutionContext,
        cancellation: Option<watch::Receiver<bool>>,
        started: Option<oneshot::Sender<()>>,
    ) -> Result<JobPostProcessingReport, PostProcessingExecutorError> {
        self.execute_job_at_script_directory(
            self.script_directory(),
            job_id,
            scripts,
            context,
            cancellation,
            started,
        )
        .await
    }

    /// Execute one already-admitted job from its immutable scripts-root snapshot.
    pub async fn execute_job_at_script_directory(
        &self,
        scripts_directory: PathBuf,
        job_id: u64,
        scripts: Vec<ScriptInstance>,
        context: JobExecutionContext,
        cancellation: Option<watch::Receiver<bool>>,
        started: Option<oneshot::Sender<()>>,
    ) -> Result<JobPostProcessingReport, PostProcessingExecutorError> {
        self.execute_admitted_job(
            job_id,
            PostProcessingJobAdmission {
                scripts_directory,
                scripts,
            },
            context,
            cancellation,
            started,
        )
        .await
    }

    /// Execute one already-admitted job from its immutable configuration snapshot.
    pub async fn execute_admitted_job(
        &self,
        job_id: u64,
        admission: PostProcessingJobAdmission,
        mut context: JobExecutionContext,
        cancellation: Option<watch::Receiver<bool>>,
        started: Option<oneshot::Sender<()>>,
    ) -> Result<JobPostProcessingReport, PostProcessingExecutorError> {
        let settings = self.db.post_processing_settings()?;
        if let Some(reason) = execution_refusal(&settings, strict_security_enabled()) {
            tracing::info!(job_id, reason, "post-processing did not run");
            self.record_job_event(job_id, SCRIPT_EVENT_KIND, reason);
            return Ok(JobPostProcessingReport {
                summary: PostProcessingSummary::NotRun,
                results: vec![],
            });
        }
        let entries = admission
            .scripts
            .iter()
            .filter(|instance| instance.enabled)
            .cloned()
            .collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(JobPostProcessingReport {
                summary: PostProcessingSummary::NotRun,
                results: vec![],
            });
        }

        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        self.cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(job_id, cancel_tx.clone());
        let forwarder = cancellation.map(|mut external| {
            tokio::spawn(async move {
                loop {
                    if *external.borrow() {
                        cancel_tx.send_replace(true);
                        break;
                    }
                    if external.changed().await.is_err() {
                        break;
                    }
                }
            })
        });
        let _registration = CancellationRegistration {
            job_id,
            registry: Arc::clone(&self.cancellations),
            forwarder,
        };

        let _queued = GaugeGuard::enter(&counters::QUEUE_DEPTH);
        let mut pause_rx = self.paused.subscribe();
        while *pause_rx.borrow() {
            tokio::select! {
                changed = pause_rx.changed() => {
                    changed.map_err(|_| PostProcessingExecutorError::Shutdown)?;
                }
                _ = cancel_rx.changed() => {
                    return Ok(JobPostProcessingReport {
                        summary: PostProcessingSummary::Cancelled,
                        results: vec![],
                    });
                }
            }
        }
        // A list with nothing to wait for takes no place among the jobs that
        // are post-processing: it only starts its scripts and moves on.
        let _permit: Option<OwnedSemaphorePermit> = if entries.iter().any(|entry| entry.blocking) {
            tokio::select! {
                biased;
                permit = self.concurrency.clone().acquire_owned() => {
                    Some(permit.map_err(|_| PostProcessingExecutorError::Shutdown)?)
                }
                _ = cancel_rx.changed() => {
                    return Ok(JobPostProcessingReport {
                        summary: PostProcessingSummary::Cancelled,
                        results: vec![],
                    });
                }
            }
        } else {
            None
        };
        if *cancel_rx.borrow() {
            return Ok(JobPostProcessingReport {
                summary: PostProcessingSummary::Cancelled,
                results: vec![],
            });
        }
        drop(_queued);
        // Durable marker before the first script: if weaver dies now, the
        // startup scan finds the job and reports it as interrupted.
        self.db.mark_job_post_processing_running(job_id)?;
        if let Some(started) = started {
            let _ = started.send(());
        }

        let interpreters = InterpreterConfig {
            python: settings.python_interpreter.clone().map(PathBuf::from),
            powershell: settings.powershell_interpreter.clone().map(PathBuf::from),
            batch: settings.batch_interpreter.clone().map(PathBuf::from),
        };
        let termination_grace = Duration::from_secs(settings.termination_grace_seconds.max(1));

        let run_started = Instant::now();
        tracing::info!(
            job_id,
            script_count = entries.len(),
            "starting post-processing for job"
        );

        let mut summary = PostProcessingSummary::Succeeded;
        let mut results = Vec::with_capacity(entries.len());
        for entry in &entries {
            if *cancel_rx.borrow() {
                summary = merge_post_processing_summary(summary, PostProcessingSummary::Cancelled);
                break;
            }
            if !entry.blocking {
                self.start_background(
                    &admission,
                    entry,
                    &context,
                    &interpreters,
                    termination_grace,
                );
                continue;
            }
            let result = {
                let _active = GaugeGuard::enter(&counters::ACTIVE);
                match self
                    .attempt(
                        &admission,
                        entry,
                        &mut context,
                        &interpreters,
                        termination_grace,
                        Some(cancel_rx.clone()),
                        false,
                    )
                    .await
                {
                    Attempt::Ran(result) | Attempt::NotStarted(result) => result,
                }
            };
            record_script_metrics(&result);
            context.compatibility.previous_script_status = match result.status {
                ScriptStatus::Skipped if result.exit_code.is_none() => {
                    context.compatibility.previous_script_status
                }
                ScriptStatus::Succeeded | ScriptStatus::Skipped => {
                    if context.compatibility.previous_script_status == NzbgetScriptStatus::Failure {
                        NzbgetScriptStatus::Failure
                    } else {
                        NzbgetScriptStatus::Success
                    }
                }
                _ => NzbgetScriptStatus::Failure,
            };
            self.publish_script_events(job_id, &result);
            summary = merge_post_processing_summary(summary, result.status.summary());
            let cancelled = result.status == ScriptStatus::Cancelled;
            results.push(result);
            if cancelled {
                break;
            }
        }

        if results.is_empty() {
            summary = PostProcessingSummary::NotRun;
        }
        self.db
            .save_job_post_processing_results(job_id, summary, &results)?;
        tracing::info!(
            job_id,
            summary = summary.as_str(),
            duration_ms = run_started.elapsed().as_millis() as u64,
            "post-processing for job finished"
        );
        Ok(JobPostProcessingReport { summary, results })
    }

    /// Start an entry nothing waits for. It runs against the job as it stands
    /// when its turn comes, and what it returns has no part in how the job's
    /// post-processing ends.
    fn start_background(
        &self,
        admission: &PostProcessingJobAdmission,
        entry: &ScriptInstance,
        context: &JobExecutionContext,
        interpreters: &InterpreterConfig,
        termination_grace: Duration,
    ) {
        let registration = super::events::BackgroundRun::register(&self.db, Some(context.job_id));
        let executor = self.clone();
        let admission = admission.clone();
        let entry = entry.clone();
        let mut context = context.clone();
        let interpreters = interpreters.clone();
        tokio::spawn(async move {
            let Some((_turn, cancellation)) = registration.turn().await else {
                return;
            };
            let job_id = context.job_id;
            let attempt = {
                let _active = GaugeGuard::enter(&counters::ACTIVE);
                executor
                    .attempt(
                        &admission,
                        &entry,
                        &mut context,
                        &interpreters,
                        termination_grace,
                        Some(cancellation),
                        true,
                    )
                    .await
            };
            let result = match attempt {
                Attempt::Ran(result) => result,
                // The pass is over by the time this is known, so the list of
                // the job's runs is the only place left to say so.
                Attempt::NotStarted(result) => executor.keep_unstarted(job_id, result).await,
            };
            record_script_metrics(&result);
            executor.publish_script_events(job_id, &result);
        });
    }

    async fn keep_unstarted(&self, job_id: u64, result: ScriptResult) -> ScriptResult {
        let limits = match self.db.post_processing_settings() {
            Ok(settings) => settings.event_scripts,
            Err(error) => {
                tracing::warn!(job_id, %error, "could not record a script that did not start");
                return result;
            }
        };
        match super::output::retain_result(self.db.clone(), Some(job_id), result.clone(), limits)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(job_id, %error, "could not record a script that did not start");
                result
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn attempt(
        &self,
        admission: &PostProcessingJobAdmission,
        entry: &ScriptInstance,
        context: &mut JobExecutionContext,
        interpreters: &InterpreterConfig,
        termination_grace: Duration,
        cancellation: Option<watch::Receiver<bool>>,
        background: bool,
    ) -> Attempt {
        let started = Instant::now();
        let not_started = |mut result: ScriptResult| {
            result.background = background;
            Attempt::NotStarted(result)
        };
        let script = match listing::resolve_script(&admission.scripts_directory, &entry.script) {
            Ok(script) => script,
            Err(error) => {
                return not_started(unavailable_result(entry, started, &error));
            }
        };
        let adapter = script.manifest.adapter();
        // What the instance holds is what the script is given: nothing is
        // checked against the script's own declarations.
        let options = match self.db.script_instance_run_inputs(&entry.id) {
            Ok(Some(options)) => options,
            Ok(None) => {
                return not_started(failed_result(
                    entry,
                    adapter,
                    started,
                    "the script instance no longer exists".to_string(),
                ));
            }
            Err(error) => {
                return not_started(failed_result(entry, adapter, started, error.to_string()));
            }
        };
        let mut identity = match RunIdentity::of(entry) {
            Ok(identity) => identity,
            Err(error) => {
                return not_started(failed_result(entry, adapter, started, error.to_string()));
            }
        };
        let settings = match self.db.post_processing_settings() {
            Ok(settings) => settings,
            Err(error) => {
                return not_started(failed_result(entry, adapter, started, error.to_string()));
            }
        };
        if let Err(error) = self
            .db
            .refresh_script_job_inputs(context.job_id, &mut context.compatibility)
        {
            return not_started(failed_result(entry, adapter, started, error.to_string()));
        }
        if let Err(error) = self
            .db
            .job_script_effects(context.job_id)
            .map(|effects| effects.apply_to_context(context))
        {
            return not_started(failed_result(entry, adapter, started, error.to_string()));
        }
        let timeout = entry.time_limit(&settings);
        // The run is live, and its token good, until this is dropped when the
        // attempt is over.
        let mut requests = self.db.open_script_run(
            &mut identity,
            Some(context.job_id),
            &ScriptEventLabel::PostProcessing,
            Some(timeout),
            false,
        );
        let request = ScriptExecutionRequest {
            manifest: script.manifest,
            root: script.root,
            options,
            context: context.clone(),
            identity,
            timeout: Some(timeout),
            termination_grace,
            interpreters: interpreters.clone(),
            supervisor_executable: self.supervisor_executable.clone(),
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let (execution, ()) = tokio::join!(
            execute_script_observed(
                request,
                cancellation,
                Some(sender),
                settings.event_scripts.script_output_ceiling_bytes
            ),
            self.consume_script_events(context, receiver, &mut requests),
        );
        match execution {
            Ok(mut result) => {
                requests.settle(&mut result);
                drop(requests);
                let record = ScriptResult {
                    script: entry.script.clone(),
                    instance_id: Some(entry.id.clone()),
                    instance_name: Some(entry.name.clone()),
                    event: Default::default(),
                    output_id: None,
                    background,
                    adapter,
                    status: match result.disposition {
                        ExecutionDisposition::Succeeded => ScriptStatus::Succeeded,
                        ExecutionDisposition::Skipped => ScriptStatus::Skipped,
                        ExecutionDisposition::Failed => ScriptStatus::Failed,
                        ExecutionDisposition::TimedOut => ScriptStatus::TimedOut,
                        ExecutionDisposition::Cancelled => ScriptStatus::Cancelled,
                    },
                    exit_code: result.exit_code,
                    duration_ms: started.elapsed().as_millis() as u64,
                    output_tail: super::output::excerpt(&result.output),
                    output_truncated: result.output_truncated,
                    error_message: result.error_message,
                    finished_at_epoch_ms: now_epoch_ms(),
                };
                Attempt::Ran(
                    match super::output::retain_output(
                        self.db.clone(),
                        Some(context.job_id),
                        record.clone(),
                        result.output,
                        settings.event_scripts,
                    )
                    .await
                    {
                        Ok(record) => record,
                        Err(error) => {
                            tracing::warn!(job_id = context.job_id, %error, "could not retain script output");
                            record
                        }
                    },
                )
            }
            Err(error) => not_started(failed_result(entry, adapter, started, error.to_string())),
        }
    }

    fn publish_script_events(&self, job_id: u64, result: &ScriptResult) {
        let mut message = format!(
            "{} {}",
            result.script.as_str(),
            result.status.as_str().to_ascii_uppercase()
        );
        if let Some(code) = result.exit_code {
            message.push_str(&format!(" (exit {code})"));
        }
        message.push_str(&format!(" in {}ms", result.duration_ms));
        if let Some(error) = result.error_message.as_deref() {
            message.push_str(&format!(": {error}"));
        }
        self.record_job_event(job_id, SCRIPT_EVENT_KIND, &message);
    }

    /// Take what the script prints and what it asks for through the API until
    /// it has ended. Both are applied here, one at a time.
    async fn consume_script_events(
        &self,
        context: &mut JobExecutionContext,
        mut receiver: tokio::sync::mpsc::Receiver<super::directives::ScriptOutputEvent>,
        requests: &mut super::callbacks::RunRequests,
    ) {
        use super::callbacks::{RunAction, RunRequest};
        use super::directives::{ScriptLogLevel, ScriptOutputEvent};
        let mut buffer = String::new();
        let mut severity = ScriptLogLevel::Debug;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                event = receiver.recv() => match event {
                    Some(ScriptOutputEvent::Directive(directive)) => {
                        if let Err(error) = self.apply_to_job(context, directive).await {
                            severity = severity.max(ScriptLogLevel::Warning);
                            super::events::append_log(&mut buffer, &format!("Invalid command: {error}"));
                        }
                    }
                    Some(ScriptOutputEvent::Log { level, text }) => {
                        severity = severity.max(level);
                        super::events::append_log(&mut buffer, &format!("{level:?}: {text}"));
                    }
                    None => break,
                },
                request = requests.next() => {
                    let RunRequest { action, reply } = request;
                    let outcome = match action {
                        RunAction::Command(directive) => self.apply_to_job(context, directive).await,
                        RunAction::Log { level, text } => requests.log(&text).map(|text| {
                            severity = severity.max(level);
                            super::events::append_log(&mut buffer, &format!("{level:?}: {text}"));
                        }),
                        RunAction::Fail(reason) => requests.fail(&reason),
                    };
                    // The script may have stopped waiting for the answer.
                    let _ = reply.send(outcome);
                }
                _ = interval.tick(), if !buffer.is_empty() => {
                    super::events::record_log_batch(&self.db, context.job_id, std::mem::take(&mut buffer), severity).await;
                    severity = ScriptLogLevel::Debug;
                }
            }
        }
        // The script has ended: what is left goes now, not at the next tick.
        if !buffer.is_empty() {
            super::events::record_log_batch(&self.db, context.job_id, buffer, severity).await;
        }
    }

    /// Apply one command to the download. The context is left as it was when
    /// the command is refused.
    async fn apply_to_job(
        &self,
        context: &mut JobExecutionContext,
        directive: super::directives::Directive,
    ) -> Result<(), String> {
        let db = self.db.clone();
        let mut next_context = context.clone();
        *context = tokio::task::spawn_blocking(move || {
            super::effects::apply_job_directive(&db, &mut next_context, directive)?;
            Ok::<_, String>(next_context)
        })
        .await
        .map_err(|error| error.to_string())
        .and_then(std::convert::identity)?;
        Ok(())
    }

    fn record_job_event(&self, job_id: u64, kind: &str, message: &str) {
        if let Err(error) = self
            .db
            .insert_job_event(job_id, now_epoch_ms(), kind, message, None)
        {
            tracing::warn!(job_id, error = %error, "could not append a post-processing job event");
        }
    }
}

/// Why execution is refused, or `None` when it may proceed.
pub(crate) fn execution_refusal(
    settings: &super::model::PostProcessingSettings,
    strict_security: bool,
) -> Option<&'static str> {
    if strict_security {
        // Refused at run time on purpose: a startup refusal would be a time bomb
        // for an operator who set the variable long after enabling scripts.
        return Some("WEAVER_STRICT_SECURITY=1 refuses post-processing script execution");
    }
    (!settings.execution_enabled).then_some("post-processing script execution is disabled")
}

pub fn strict_security_enabled() -> bool {
    crate::security::parse_bool_env(crate::security::ENV_STRICT_SECURITY, false).unwrap_or(false)
}

fn unavailable_result(
    entry: &ScriptInstance,
    started: Instant,
    error: &ListingError,
) -> ScriptResult {
    // A renamed or edited script must not fail the job: the operator sees a
    // warning and an event, which is what both oracles do with a missing script.
    ScriptResult {
        script: entry.script.clone(),
        instance_id: Some(entry.id.clone()),
        instance_name: Some(entry.name.clone()),
        event: Default::default(),
        adapter: ScriptAdapter::Sabnzbd,
        output_id: None,
        background: false,
        status: ScriptStatus::Warning,
        exit_code: None,
        duration_ms: started.elapsed().as_millis() as u64,
        output_tail: String::new(),
        output_truncated: false,
        error_message: Some(error.to_string()),
        finished_at_epoch_ms: now_epoch_ms(),
    }
}

fn failed_result(
    entry: &ScriptInstance,
    adapter: ScriptAdapter,
    started: Instant,
    message: String,
) -> ScriptResult {
    ScriptResult {
        script: entry.script.clone(),
        instance_id: Some(entry.id.clone()),
        instance_name: Some(entry.name.clone()),
        event: Default::default(),
        adapter,
        status: ScriptStatus::Failed,
        output_id: None,
        background: false,
        exit_code: None,
        duration_ms: started.elapsed().as_millis() as u64,
        output_tail: String::new(),
        output_truncated: false,
        error_message: Some(message),
        finished_at_epoch_ms: now_epoch_ms(),
    }
}

fn now_epoch_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
