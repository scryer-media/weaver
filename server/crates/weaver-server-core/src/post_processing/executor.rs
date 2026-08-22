//! Bounded, sequential execution of a job's script list.
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

use super::listing::{self, ListingError};
use super::model::{
    PostProcessingSummary, ScriptAdapter, ScriptList, ScriptListEntry, ScriptLists, ScriptResult,
    ScriptStatus, merge_post_processing_summary,
};
use super::runner::{
    DEFAULT_TIMEOUT, ExecutionDisposition, InterpreterConfig, JobExecutionContext,
    NzbgetScriptStatus, ScriptExecutionRequest, execute_script,
};
use super::settings::ScriptOptionsSnapshot;
use crate::persistence::{Database, StateError};

const MAX_CONCURRENCY: usize = 8;
/// Output carried inline on the job's event stream. The full tail stays on the
/// job row, so the event log remains readable when a script is chatty.
const MAX_EVENT_OUTPUT_BYTES: usize = 8 * 1024;

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

/// All name-based configuration captured when a job enters post-processing.
pub struct PostProcessingJobAdmission {
    scripts_directory: PathBuf,
    scripts: ScriptList,
    options: ScriptOptionsSnapshot,
}

impl PostProcessingJobAdmission {
    pub fn has_enabled_entries(&self) -> bool {
        self.scripts.enabled_entries().next().is_some()
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

    /// Signal every in-flight script for `job_id` to stop.
    pub fn cancel_job(&self, job_id: u64) -> bool {
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

    /// Resolve the list a job should run, without executing anything.
    pub fn resolve_job_scripts(
        &self,
        category: Option<&str>,
        job_metadata: &[(String, String)],
    ) -> Result<ScriptList, StateError> {
        let lists = self.db.post_processing_script_lists()?;
        Ok(resolve_script_list(
            &lists,
            category,
            super::settings::job_script_override(job_metadata),
        ))
    }

    /// Atomically capture the root and resolved list for a newly admitted job.
    pub fn admit_job_scripts(
        &self,
        category: Option<&str>,
        job_metadata: &[(String, String)],
    ) -> Result<Option<PostProcessingJobAdmission>, StateError> {
        let (settings, lists, script_directory, options) =
            self.db.post_processing_script_admission()?;
        if !settings.execution_enabled {
            return Ok(None);
        }
        let scripts = resolve_script_list(
            &lists,
            category,
            super::settings::job_script_override(job_metadata),
        );
        Ok(Some(PostProcessingJobAdmission {
            scripts_directory: script_directory,
            scripts,
            options,
        }))
    }

    pub fn execution_enabled(&self) -> Result<bool, StateError> {
        Ok(self.db.post_processing_settings()?.execution_enabled)
    }

    /// Run `scripts` for one job, sequentially, under the concurrency semaphore.
    pub async fn execute_job(
        &self,
        job_id: u64,
        scripts: ScriptList,
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
        scripts: ScriptList,
        context: JobExecutionContext,
        cancellation: Option<watch::Receiver<bool>>,
        started: Option<oneshot::Sender<()>>,
    ) -> Result<JobPostProcessingReport, PostProcessingExecutorError> {
        let options = self.db.post_processing_script_options_snapshot()?;
        self.execute_admitted_job(
            job_id,
            PostProcessingJobAdmission {
                scripts_directory,
                scripts,
                options,
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
            .enabled_entries()
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
        let _permit: OwnedSemaphorePermit = tokio::select! {
            biased;
            permit = self.concurrency.clone().acquire_owned() => {
                permit.map_err(|_| PostProcessingExecutorError::Shutdown)?
            }
            _ = cancel_rx.changed() => {
                return Ok(JobPostProcessingReport {
                    summary: PostProcessingSummary::Cancelled,
                    results: vec![],
                });
            }
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
            let result = {
                let _active = GaugeGuard::enter(&counters::ACTIVE);
                self.execute_one(
                    &admission,
                    entry,
                    &context,
                    &interpreters,
                    termination_grace,
                    Some(cancel_rx.clone()),
                )
                .await
            };
            record_script_metrics(&result);
            context.compatibility.previous_script_status = match result.status {
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

    async fn execute_one(
        &self,
        admission: &PostProcessingJobAdmission,
        entry: &ScriptListEntry,
        context: &JobExecutionContext,
        interpreters: &InterpreterConfig,
        termination_grace: Duration,
        cancellation: Option<watch::Receiver<bool>>,
    ) -> ScriptResult {
        let started = Instant::now();
        let script = match listing::resolve_script(&admission.scripts_directory, &entry.script) {
            Ok(script) => script,
            Err(error) => {
                return unavailable_result(entry, started, &error);
            }
        };
        let supplied = match self
            .db
            .resolve_post_processing_script_options(&admission.options, &entry.script)
        {
            Ok(options) => options,
            Err(error) => {
                return failed_result(entry, script.manifest.adapter(), started, error.to_string());
            }
        };
        let options = match script.manifest.resolve_options(&supplied) {
            Ok(options) => options,
            Err(error) => {
                return failed_result(entry, script.manifest.adapter(), started, error.to_string());
            }
        };
        let adapter = script.manifest.adapter();
        let request = ScriptExecutionRequest {
            manifest: script.manifest,
            root: script.root,
            options,
            context: context.clone(),
            timeout: Some(
                entry
                    .timeout_seconds
                    .map(Duration::from_secs)
                    .unwrap_or(DEFAULT_TIMEOUT),
            ),
            termination_grace,
            interpreters: interpreters.clone(),
            supervisor_executable: self.supervisor_executable.clone(),
        };
        match execute_script(request, cancellation).await {
            Ok(result) => ScriptResult {
                script: entry.script.clone(),
                adapter,
                status: match result.disposition {
                    ExecutionDisposition::Succeeded => ScriptStatus::Succeeded,
                    ExecutionDisposition::Skipped => ScriptStatus::Skipped,
                    ExecutionDisposition::Warned => ScriptStatus::Warning,
                    ExecutionDisposition::Failed => ScriptStatus::Failed,
                    ExecutionDisposition::TimedOut => ScriptStatus::TimedOut,
                    ExecutionDisposition::Cancelled => ScriptStatus::Cancelled,
                },
                exit_code: result.exit_code,
                duration_ms: started.elapsed().as_millis() as u64,
                output_tail: String::from_utf8_lossy(&result.output).into_owned(),
                output_truncated: result.output_truncated,
                error_message: result.error_message,
                finished_at_epoch_ms: now_epoch_ms(),
            },
            Err(error) => failed_result(entry, adapter, started, error.to_string()),
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
        if !result.output_tail.trim().is_empty() {
            self.record_job_event(
                job_id,
                SCRIPT_OUTPUT_EVENT_KIND,
                &event_output_excerpt(&result.output_tail),
            );
        }
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

/// Category override, global default, or a submission-time override from a facade.
pub fn resolve_script_list(
    lists: &ScriptLists,
    category: Option<&str>,
    job_override: Option<Vec<ScriptListEntry>>,
) -> ScriptList {
    if let Some(entries) = job_override {
        return ScriptList::new(entries).unwrap_or_default();
    }
    lists.resolve(category).clone()
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

fn event_output_excerpt(output: &str) -> String {
    if output.len() <= MAX_EVENT_OUTPUT_BYTES {
        return output.to_string();
    }
    let mut start = output.len() - MAX_EVENT_OUTPUT_BYTES;
    while start < output.len() && !output.is_char_boundary(start) {
        start += 1;
    }
    format!("…{}", &output[start..])
}

fn unavailable_result(
    entry: &ScriptListEntry,
    started: Instant,
    error: &ListingError,
) -> ScriptResult {
    // A renamed or edited script must not fail the job: the operator sees a
    // warning and an event, which is what both oracles do with a missing script.
    ScriptResult {
        script: entry.script.clone(),
        adapter: ScriptAdapter::Sabnzbd,
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
    entry: &ScriptListEntry,
    adapter: ScriptAdapter,
    started: Instant,
    message: String,
) -> ScriptResult {
    ScriptResult {
        script: entry.script.clone(),
        adapter,
        status: ScriptStatus::Failed,
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
