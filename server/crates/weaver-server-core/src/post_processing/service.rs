use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};

use super::model::{
    AttemptStatus, ExtensionAdapter, FrozenPlan, OnFailure, OutcomeImpact, PipelineOutcome,
    PostProcessingSettings, PostProcessingSummary, RunStatus, RunWhen, TrustState,
};
use super::persistence::{PostProcessingRunRecord, TerminalIntent};
use super::runner::{
    ControlEffects, ExecutionDisposition, ExtensionExecutionRequest, InterpreterConfig,
    JobExecutionContext, NzbgetScriptStatus, RunnerError, execute_extension_with_spawn_callback,
};
use crate::persistence::{Database, StateError};

const MAX_CONCURRENCY: usize = 8;
const MAX_DISCOVERED_ARTIFACTS: usize = 10_000;
type CancellationRegistry = Arc<Mutex<HashMap<String, (u64, watch::Sender<bool>)>>>;
type AdmissionQueue = Arc<Mutex<Vec<(i64, String)>>>;

#[derive(Clone)]
pub struct PostProcessingService {
    db: Database,
    concurrency: Arc<Semaphore>,
    pause_tx: watch::Sender<bool>,
    cancellations: CancellationRegistry,
    admission_queue: AdmissionQueue,
    admission_epoch: watch::Sender<u64>,
    termination_grace: Duration,
}

struct CancellationRegistration {
    run_id: String,
    registry: CancellationRegistry,
    forwarder: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        self.registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.run_id);
        if let Some(forwarder) = self.forwarder.take() {
            forwarder.abort();
        }
    }
}

struct AdmissionRegistration {
    run_id: String,
    queue: AdmissionQueue,
    epoch: watch::Sender<u64>,
    admitted: bool,
}

impl AdmissionRegistration {
    fn new(
        run_id: String,
        queue_position: i64,
        queue: AdmissionQueue,
        epoch: watch::Sender<u64>,
    ) -> Self {
        let mut queued = queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !queued.iter().any(|(_, queued_id)| queued_id == &run_id) {
            queued.push((queue_position, run_id.clone()));
            queued.sort();
        }
        drop(queued);
        bump_epoch(&epoch);
        Self {
            run_id,
            queue,
            epoch,
            admitted: false,
        }
    }

    fn is_first(&self) -> bool {
        self.queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .first()
            .is_some_and(|(_, run_id)| run_id == &self.run_id)
    }

    fn mark_admitted(&mut self) {
        self.remove();
        self.admitted = true;
    }

    fn remove(&self) {
        self.queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|(_, run_id)| run_id != &self.run_id);
        bump_epoch(&self.epoch);
    }
}

impl Drop for AdmissionRegistration {
    fn drop(&mut self) {
        if !self.admitted {
            self.remove();
        }
    }
}

struct NotifyingPermit {
    _permit: OwnedSemaphorePermit,
    epoch: watch::Sender<u64>,
}

impl Drop for NotifyingPermit {
    fn drop(&mut self) {
        bump_epoch(&self.epoch);
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RunExecutionReport {
    pub run_id: Option<super::model::RunId>,
    pub summary: PostProcessingSummary,
    pub effects: Vec<ControlEffects>,
    pub repair_requested: bool,
    pub output_directory: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum PostProcessingServiceError {
    #[error("post-processing persistence failed: {0}")]
    State(#[from] StateError),
    #[error("post-processing runner failed: {0}")]
    Runner(#[from] RunnerError),
    #[error("post-processing run was not found")]
    RunNotFound,
    #[error("post-processing extension revision is missing from the managed store")]
    RevisionUnavailable,
    #[error("post-processing service was shut down")]
    Shutdown,
    #[error("post-processing queue order must contain every queued run exactly once")]
    InvalidQueueOrder,
}

impl PostProcessingService {
    pub fn new(db: Database, concurrency: usize) -> Self {
        Self::new_with_termination_grace(db, concurrency, Duration::from_secs(10))
    }

    pub fn new_with_termination_grace(
        db: Database,
        concurrency: usize,
        termination_grace: Duration,
    ) -> Self {
        let (pause_tx, _) = watch::channel(false);
        let (admission_epoch, _) = watch::channel(0);
        Self {
            db,
            concurrency: Arc::new(Semaphore::new(concurrency.clamp(1, MAX_CONCURRENCY))),
            pause_tx,
            cancellations: Arc::new(Mutex::new(HashMap::new())),
            admission_queue: Arc::new(Mutex::new(Vec::new())),
            admission_epoch,
            termination_grace,
        }
    }

    pub fn pause(&self) {
        self.pause_tx.send_replace(true);
    }

    pub fn resume(&self) {
        self.pause_tx.send_replace(false);
    }

    pub fn is_paused(&self) -> bool {
        *self.pause_tx.borrow()
    }

    pub fn queued_run_ids(&self) -> Vec<String> {
        self.admission_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(_, run_id)| run_id.clone())
            .collect()
    }

    pub fn reorder_queued_runs(
        &self,
        ordered_run_ids: &[String],
    ) -> Result<(), PostProcessingServiceError> {
        let requested = ordered_run_ids.iter().collect::<HashSet<_>>();
        let mut queue = self
            .admission_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = queue
            .iter()
            .map(|(_, run_id)| run_id)
            .collect::<HashSet<_>>();
        if requested.len() != ordered_run_ids.len()
            || requested.len() != queue.len()
            || requested != current
        {
            return Err(PostProcessingServiceError::InvalidQueueOrder);
        }
        self.db
            .reorder_queued_post_processing_runs(ordered_run_ids)?;
        *queue = ordered_run_ids
            .iter()
            .enumerate()
            .map(|(position, run_id)| {
                i64::try_from(position)
                    .map(|position| (position, run_id.clone()))
                    .map_err(|_| PostProcessingServiceError::InvalidQueueOrder)
            })
            .collect::<Result<Vec<_>, _>>()?;
        drop(queue);
        bump_epoch(&self.admission_epoch);
        Ok(())
    }

    pub fn cancel_job(&self, job_id: u64) -> bool {
        let senders = self
            .cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|(registered_job_id, _)| *registered_job_id == job_id)
            .map(|(_, sender)| sender.clone())
            .collect::<Vec<_>>();
        for sender in &senders {
            sender.send_replace(true);
        }
        !senders.is_empty()
    }

    pub fn recover_interrupted(&self) -> Result<u64, StateError> {
        self.db.recover_interrupted_post_processing(now_epoch_ms())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_and_execute(
        &self,
        job_id: u64,
        plan: &FrozenPlan,
        pipeline_outcome: PipelineOutcome,
        terminal_intent: TerminalIntent,
        context: JobExecutionContext,
        interpreters: InterpreterConfig,
        cancellation: Option<watch::Receiver<bool>>,
    ) -> Result<RunExecutionReport, PostProcessingServiceError> {
        self.create_and_execute_with_started(
            job_id,
            plan,
            pipeline_outcome,
            terminal_intent,
            context,
            interpreters,
            cancellation,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_and_execute_with_started(
        &self,
        job_id: u64,
        plan: &FrozenPlan,
        pipeline_outcome: PipelineOutcome,
        terminal_intent: TerminalIntent,
        context: JobExecutionContext,
        interpreters: InterpreterConfig,
        cancellation: Option<watch::Receiver<bool>>,
        started: Option<oneshot::Sender<()>>,
    ) -> Result<RunExecutionReport, PostProcessingServiceError> {
        if plan.steps().is_empty() {
            return Ok(RunExecutionReport {
                run_id: None,
                summary: PostProcessingSummary::NotRun,
                effects: vec![],
                repair_requested: false,
                output_directory: Some(context.working_directory.clone()),
            });
        }
        let run_id = self.db.create_post_processing_run(
            job_id,
            plan,
            &pipeline_outcome,
            terminal_intent,
            None,
            now_epoch_ms(),
        )?;
        let report = self
            .execute_existing_filtered(&run_id, context, interpreters, cancellation, None, started)
            .await?;
        Ok(report)
    }

    pub async fn execute_existing(
        &self,
        run_id: &super::model::RunId,
        context: JobExecutionContext,
        interpreters: InterpreterConfig,
        cancellation: Option<watch::Receiver<bool>>,
    ) -> Result<RunExecutionReport, PostProcessingServiceError> {
        self.execute_existing_filtered(run_id, context, interpreters, cancellation, None, None)
            .await
    }

    pub async fn execute_existing_with_started(
        &self,
        run_id: &super::model::RunId,
        context: JobExecutionContext,
        interpreters: InterpreterConfig,
        cancellation: Option<watch::Receiver<bool>>,
        started: Option<oneshot::Sender<()>>,
    ) -> Result<RunExecutionReport, PostProcessingServiceError> {
        self.execute_existing_filtered(run_id, context, interpreters, cancellation, None, started)
            .await
    }

    pub async fn execute_existing_selected(
        &self,
        run_id: &super::model::RunId,
        context: JobExecutionContext,
        interpreters: InterpreterConfig,
        cancellation: Option<watch::Receiver<bool>>,
        selected_step_indexes: HashSet<u32>,
    ) -> Result<RunExecutionReport, PostProcessingServiceError> {
        self.execute_existing_filtered(
            run_id,
            context,
            interpreters,
            cancellation,
            Some(selected_step_indexes),
            None,
        )
        .await
    }

    async fn execute_existing_filtered(
        &self,
        run_id: &super::model::RunId,
        context: JobExecutionContext,
        interpreters: InterpreterConfig,
        cancellation: Option<watch::Receiver<bool>>,
        selected_step_indexes: Option<HashSet<u32>>,
        started: Option<oneshot::Sender<()>>,
    ) -> Result<RunExecutionReport, PostProcessingServiceError> {
        let run = self
            .db
            .post_processing_run(run_id)?
            .ok_or(PostProcessingServiceError::RunNotFound)?;
        let (service_cancel_tx, mut service_cancel_rx) = watch::channel(false);
        self.cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                run_id.as_str().to_string(),
                (run.job_id, service_cancel_tx.clone()),
            );
        let forwarder = cancellation.map(|mut external| {
            let service_cancel_tx = service_cancel_tx.clone();
            tokio::spawn(async move {
                loop {
                    if *external.borrow() {
                        service_cancel_tx.send_replace(true);
                        break;
                    }
                    if external.changed().await.is_err() {
                        break;
                    }
                }
            })
        });
        let _registration = CancellationRegistration {
            run_id: run_id.as_str().to_string(),
            registry: Arc::clone(&self.cancellations),
            forwarder,
        };
        let mut pause = self.pause_tx.subscribe();
        let mut admission_changes = self.admission_epoch.subscribe();
        let mut admission = AdmissionRegistration::new(
            run_id.as_str().to_string(),
            run.queue_position,
            Arc::clone(&self.admission_queue),
            self.admission_epoch.clone(),
        );
        let _permit = loop {
            if *service_cancel_rx.borrow() {
                return self.cancel_queued_run(&run);
            }
            if !*pause.borrow() && admission.is_first() {
                match self.concurrency.clone().try_acquire_owned() {
                    Ok(permit) => {
                        admission.mark_admitted();
                        break NotifyingPermit {
                            _permit: permit,
                            epoch: self.admission_epoch.clone(),
                        };
                    }
                    Err(tokio::sync::TryAcquireError::Closed) => {
                        return Err(PostProcessingServiceError::Shutdown);
                    }
                    Err(tokio::sync::TryAcquireError::NoPermits) => {}
                }
            }
            tokio::select! {
                changed = pause.changed() => {
                    changed.map_err(|_| PostProcessingServiceError::Shutdown)?;
                }
                changed = service_cancel_rx.changed() => {
                    changed.map_err(|_| PostProcessingServiceError::Shutdown)?;
                }
                changed = admission_changes.changed() => {
                    changed.map_err(|_| PostProcessingServiceError::Shutdown)?;
                }
            }
        };
        self.db
            .mark_post_processing_run_running(run_id, now_epoch_ms())?;
        if let Some(started) = started {
            let _ = started.send(());
        }
        let started = Instant::now();
        tracing::info!(
            run_id = %run_id.as_str(),
            job_id = run.job_id,
            step_count = run.plan.steps().len(),
            "starting post-processing run"
        );
        let result = self
            .execute_steps(
                run,
                context,
                interpreters,
                Some(service_cancel_rx),
                selected_step_indexes.as_ref(),
            )
            .await;
        match &result {
            Ok(report) => tracing::info!(
                run_id = %run_id.as_str(),
                summary = ?report.summary,
                duration_ms = started.elapsed().as_millis() as u64,
                "post-processing run finished"
            ),
            Err(error) => tracing::info!(
                run_id = %run_id.as_str(),
                error = %error,
                duration_ms = started.elapsed().as_millis() as u64,
                "post-processing run could not finish"
            ),
        }
        if result.is_err()
            && let Err(error) = self.db.finish_post_processing_run(
                run_id,
                RunStatus::Failed,
                PostProcessingSummary::Failed,
                now_epoch_ms(),
            )
        {
            tracing::warn!(
                run_id = %run_id.as_str(),
                error = %error,
                "failed to persist terminal state for an aborted post-processing run"
            );
        }
        result
    }

    fn cancel_queued_run(
        &self,
        run: &PostProcessingRunRecord,
    ) -> Result<RunExecutionReport, PostProcessingServiceError> {
        self.db.finish_post_processing_run(
            &run.run_id,
            RunStatus::Cancelled,
            PostProcessingSummary::Cancelled,
            now_epoch_ms(),
        )?;
        tracing::info!(
            run_id = %run.run_id.as_str(),
            job_id = run.job_id,
            "cancelled queued post-processing run before an attempt started"
        );
        Ok(RunExecutionReport {
            run_id: Some(run.run_id.clone()),
            summary: PostProcessingSummary::Cancelled,
            effects: vec![],
            repair_requested: false,
            output_directory: None,
        })
    }

    async fn execute_steps(
        &self,
        run: PostProcessingRunRecord,
        mut context: JobExecutionContext,
        interpreters: InterpreterConfig,
        cancellation: Option<watch::Receiver<bool>>,
        selected_step_indexes: Option<&HashSet<u32>>,
    ) -> Result<RunExecutionReport, PostProcessingServiceError> {
        let mut summary = PostProcessingSummary::Succeeded;
        let mut effects = Vec::new();
        let mut repair_requested = false;
        let mut executed_attempt = false;
        let mut explicit_final_directory = false;
        let pipeline_succeeded = matches!(run.pipeline_outcome, PipelineOutcome::Succeeded);
        let mut cancellation = cancellation;
        let mut available_artifacts =
            collect_artifact_paths(&context.working_directory, MAX_DISCOVERED_ARTIFACTS);
        let settings = self.db.post_processing_settings()?;

        for step in run.plan.steps() {
            if selected_step_indexes.is_some_and(|selected| !selected.contains(&step.index())) {
                continue;
            }
            if !step_is_eligible(step.run_when(), pipeline_succeeded) {
                continue;
            }
            if !step
                .artifact_condition()
                .matches(available_artifacts.iter().map(PathBuf::as_path))
            {
                continue;
            }
            if cancellation
                .as_ref()
                .is_some_and(|receiver| *receiver.borrow())
            {
                summary = PostProcessingSummary::Cancelled;
                break;
            }
            let record = self
                .db
                .extension_revision(
                    step.revision().extension_id(),
                    step.revision().revision_id(),
                )?
                .ok_or(PostProcessingServiceError::RevisionUnavailable)?;
            if record.trust_state != TrustState::Approved {
                return Err(PostProcessingServiceError::RevisionUnavailable);
            }
            if !adapter_enabled(record.manifest.adapter(), &settings) {
                continue;
            }
            executed_attempt = true;
            let managed_path = record
                .managed_path
                .map(PathBuf::from)
                .ok_or(PostProcessingServiceError::RevisionUnavailable)?;
            let (control_token, control_token_hash) = issue_control_token()?;
            let attempt_id = self.db.enqueue_post_processing_attempt(
                &run.run_id,
                step,
                record.manifest.adapter(),
                Some(control_token_hash),
                now_epoch_ms(),
            )?;
            let command_summary = serde_json::json!({
                "adapter": record.manifest.adapter(),
                "entrypoint": record.manifest.entrypoint(),
                "revisionId": record.manifest.revision().revision_id(),
            });
            self.db.mark_post_processing_attempt_starting(
                &attempt_id,
                &command_summary,
                context.working_directory.to_string_lossy().as_ref(),
                now_epoch_ms(),
            )?;
            let request = ExtensionExecutionRequest {
                attempt_id: attempt_id.as_str().to_string(),
                manifest: record.manifest,
                managed_path,
                options: step.options().to_vec(),
                approved_roots: step.approved_roots().as_slice().to_vec(),
                context: context.clone(),
                timeout_policy: step.timeout_policy(),
                termination_grace: self.termination_grace,
                interpreters: interpreters.clone(),
                control_token: Some(control_token),
                diagnostic_command: None,
                supervisor_executable: None,
            };
            let db = self.db.clone();
            let spawned_attempt_id = attempt_id.clone();
            let result =
                execute_extension_with_spawn_callback(request, cancellation.clone(), move || {
                    if db
                        .mark_post_processing_attempt_running(&spawned_attempt_id)
                        .map_err(|error| RunnerError::SupervisorProtocol(error.to_string()))?
                    {
                        Ok(())
                    } else {
                        Err(RunnerError::SupervisorProtocol(
                            "attempt could not transition to running".into(),
                        ))
                    }
                })
                .await;
            match result {
                Ok(result) => {
                    let timestamp = now_epoch_ms();
                    for line in &result.output {
                        self.db.append_post_processing_log(
                            &attempt_id,
                            line.stream,
                            &line.bytes,
                            timestamp,
                        )?;
                    }
                    let (attempt_status, failed) = match result.disposition {
                        ExecutionDisposition::Succeeded => (AttemptStatus::Succeeded, false),
                        ExecutionDisposition::Skipped => (AttemptStatus::Skipped, false),
                        ExecutionDisposition::RepairRequested => {
                            repair_requested = context.par_status == 0;
                            (AttemptStatus::Succeeded, false)
                        }
                        ExecutionDisposition::Cancelled => (AttemptStatus::Cancelled, true),
                        ExecutionDisposition::TimedOut => (AttemptStatus::TimedOut, true),
                        ExecutionDisposition::Failed => (AttemptStatus::Failed, true),
                    };
                    self.db.finish_post_processing_attempt(
                        &attempt_id,
                        attempt_status,
                        result.exit_code,
                        result.error_message.clone(),
                        Some(super::persistence::encode_control_effects(&result.effects)?),
                        timestamp,
                    )?;
                    if let Some(directory) = result.effects.directory.as_ref() {
                        context.working_directory = directory.clone();
                        available_artifacts = collect_artifact_paths(
                            &context.working_directory,
                            MAX_DISCOVERED_ARTIFACTS,
                        );
                    }
                    if let Some(final_directory) = result.effects.final_directory.as_ref() {
                        context.final_directory = final_directory.clone();
                        explicit_final_directory = true;
                    }
                    available_artifacts.extend(result.effects.artifacts.iter().cloned());
                    match result.disposition {
                        ExecutionDisposition::Skipped => {}
                        ExecutionDisposition::Succeeded | ExecutionDisposition::RepairRequested => {
                            if context.compatibility.previous_script_status
                                != NzbgetScriptStatus::Failure
                            {
                                context.compatibility.previous_script_status =
                                    NzbgetScriptStatus::Success;
                            }
                        }
                        ExecutionDisposition::Failed
                        | ExecutionDisposition::Cancelled
                        | ExecutionDisposition::TimedOut => {
                            context.compatibility.previous_script_status =
                                NzbgetScriptStatus::Failure;
                        }
                    }
                    effects.push(result.effects);
                    if failed {
                        let failure_summary = match result.disposition {
                            ExecutionDisposition::Cancelled => PostProcessingSummary::Cancelled,
                            _ if step.outcome_impact() == OutcomeImpact::Warning => {
                                PostProcessingSummary::Warning
                            }
                            _ => PostProcessingSummary::Failed,
                        };
                        summary = merge_post_processing_summary(summary, failure_summary);
                        if step.on_failure() == OnFailure::Stop
                            || summary == PostProcessingSummary::Cancelled
                        {
                            break;
                        }
                    }
                }
                Err(error) => {
                    self.db.finish_post_processing_attempt(
                        &attempt_id,
                        AttemptStatus::Failed,
                        None,
                        Some(error.to_string()),
                        None,
                        now_epoch_ms(),
                    )?;
                    let failure_summary = if step.outcome_impact() == OutcomeImpact::Warning {
                        PostProcessingSummary::Warning
                    } else {
                        PostProcessingSummary::Failed
                    };
                    summary = merge_post_processing_summary(summary, failure_summary);
                    context.compatibility.previous_script_status = NzbgetScriptStatus::Failure;
                    if step.on_failure() == OnFailure::Stop {
                        break;
                    }
                }
            }
            if let Some(receiver) = cancellation.as_mut() {
                let _ = receiver.has_changed();
            }
        }

        if !executed_attempt && summary == PostProcessingSummary::Succeeded {
            summary = PostProcessingSummary::NotRun;
        }
        let run_status = match summary {
            PostProcessingSummary::Succeeded | PostProcessingSummary::Warning => {
                RunStatus::Succeeded
            }
            PostProcessingSummary::Cancelled => RunStatus::Cancelled,
            PostProcessingSummary::Interrupted => RunStatus::Interrupted,
            PostProcessingSummary::NotRun => RunStatus::Skipped,
            PostProcessingSummary::Failed => RunStatus::Failed,
        };
        self.db.finish_post_processing_run(
            &run.run_id,
            run_status,
            summary.clone(),
            now_epoch_ms(),
        )?;
        Ok(RunExecutionReport {
            run_id: Some(run.run_id),
            summary,
            effects,
            repair_requested,
            output_directory: Some(if explicit_final_directory {
                context.final_directory
            } else {
                context.working_directory
            }),
        })
    }
}

pub(super) fn merge_post_processing_summary(
    current: PostProcessingSummary,
    incoming: PostProcessingSummary,
) -> PostProcessingSummary {
    use PostProcessingSummary::{Cancelled, Failed, Interrupted, NotRun, Succeeded, Warning};

    match (current, incoming) {
        (Cancelled, _) | (_, Cancelled) => Cancelled,
        (Interrupted, _) | (_, Interrupted) => Interrupted,
        (Failed, _) | (_, Failed) => Failed,
        (Warning, _) | (_, Warning) => Warning,
        (Succeeded, _) | (_, Succeeded) => Succeeded,
        (NotRun, NotRun) => NotRun,
    }
}

fn step_is_eligible(run_when: RunWhen, pipeline_succeeded: bool) -> bool {
    match run_when {
        RunWhen::Always => true,
        RunWhen::PipelineSuccess => pipeline_succeeded,
        RunWhen::PipelineFailure => !pipeline_succeeded,
    }
}

fn adapter_enabled(adapter: ExtensionAdapter, settings: &PostProcessingSettings) -> bool {
    match adapter {
        ExtensionAdapter::Webhook => settings.webhooks_enabled,
        ExtensionAdapter::Native | ExtensionAdapter::Sabnzbd | ExtensionAdapter::Nzbget => true,
    }
}

fn collect_artifact_paths(root: &Path, limit: usize) -> Vec<PathBuf> {
    let mut artifacts = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        let Ok(entries) = std::fs::read_dir(directory) else {
            continue;
        };
        let mut entries = entries.flatten().collect::<Vec<_>>();
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file() {
                artifacts.push(entry.path());
                if artifacts.len() >= limit {
                    return artifacts;
                }
            }
        }
    }
    artifacts
}

fn issue_control_token() -> Result<(String, Vec<u8>), StateError> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| StateError::Database(error.to_string()))?;
    let token = format!("wpp_{}", hex::encode(bytes));
    let hash = blake3::hash(token.as_bytes()).as_bytes().to_vec();
    Ok((token, hash))
}

fn bump_epoch(epoch: &watch::Sender<u64>) {
    let next = epoch.borrow().wrapping_add(1);
    epoch.send_replace(next);
}

fn now_epoch_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
