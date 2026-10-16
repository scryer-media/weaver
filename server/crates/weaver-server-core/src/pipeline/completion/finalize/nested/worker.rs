mod engine;
mod live_rar;
mod publish;
mod repair;

use super::*;
use crate::pipeline::completion::finalize::extract::ArchiveEngine;
use crate::pipeline::extraction::output::channel::{ChannelOutput, StreamOwner};
use crate::pipeline::extraction::runtime::decoded::DecodedState;
use crate::pipeline::extraction::runtime::{ArchiveRuntime, NamespaceWork, RequestId};
use crate::pipeline::extraction::session::{InputVersion, SessionLease};
use crate::pipeline::extraction::{ExtractionErrorClass, JobExtractionBudget};
use engine::{ScopedDecode, ScopedEngine, ScopedReport, ScopedStream};

use crate::pipeline::extraction::runtime::limits::{
    BRIDGE_ADMISSION_BYTES, FRAME_CHANNEL_DEPTH, namespace_router_resources,
};

/// What the actor does with a finished scoped decode.
enum ScopedOutcome {
    /// A root set decoded without a namespace; the full-set path owns it.
    Delegate {
        set_name: String,
        result: Result<FullSetExtractionOutcome, ExtractionError>,
    },
    /// The input may be repaired and decoded again.
    Recover {
        failure: String,
    },
    Publish(FullSetExtractionOutcome),
    Fail(String),
}

impl ScopedOutcome {
    fn classify(
        work: &NamespaceWork,
        decoded: bool,
        result: Result<FullSetExtractionOutcome, ExtractionError>,
    ) -> Self {
        if !decoded && let Some(set_name) = &work.root_set {
            return Self::Delegate {
                set_name: set_name.clone(),
                result,
            };
        }
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) if is_recoverable(&error) => {
                return Self::Recover {
                    failure: error.to_string(),
                };
            }
            Err(error) => return Self::Fail(error.to_string()),
        };
        let Some((_, first)) = outcome.failed.first() else {
            return Self::Publish(outcome);
        };
        if outcome
            .failed
            .iter()
            .all(|(_, error)| is_recoverable(error))
        {
            Self::Recover {
                failure: first.to_string(),
            }
        } else {
            Self::Fail(format!("nested archive extraction failed: {first}"))
        }
    }
}

fn is_recoverable(error: &ExtractionError) -> bool {
    matches!(
        error.class(),
        ExtractionErrorClass::Recoverable
            | ExtractionErrorClass::MissingFirstVolume
            | ExtractionErrorClass::StaleTopology
    )
}

fn namespace_output_dir(working: &Path, owner: StreamOwner) -> PathBuf {
    working
        .join(crate::pipeline::direct_store::manifest::DIRECTORY)
        .join(format!(
            "namespace-{}-{}-{}",
            owner.root, owner.node, owner.generation
        ))
}

fn decoded_namespace_name(owner: StreamOwner) -> String {
    format!("decoded-{}-{}", owner.node, owner.generation)
}

// The deepest archive level this publication materializes, counted from the
// job's own files.
fn extracted_depth(runtime: &ArchiveRuntime) -> u32 {
    let decoded = runtime.decoded.iter().map(|state| {
        let local = state
            .graph
            .nodes
            .iter()
            .map(|node| node.depth)
            .max()
            .unwrap_or(1);
        runtime
            .sessions
            .depth(state.generation.owner())
            .unwrap_or(1)
            .saturating_add(local)
            .saturating_sub(2)
    });
    let completed = runtime.completed.iter().filter_map(|(owner, _, _)| {
        runtime
            .sessions
            .depth(*owner)
            .map(|depth| depth.saturating_sub(1))
    });
    decoded.chain(completed).max().unwrap_or(0)
}

// Root sets resume on the full-set path with every member their namespace
// produced, ordinary outputs first.
fn root_continuations(
    completed: &[publish::Completion],
) -> Vec<(String, FullSetExtractionOutcome)> {
    completed
        .iter()
        .filter_map(|(_, work, members)| {
            let set_name = work.root_set.clone()?;
            let extracted = work.ordinary.iter().chain(members).cloned().collect();
            Some((
                set_name,
                FullSetExtractionOutcome {
                    extracted,
                    failed: Vec::new(),
                    selected_password: work.selected_password.clone(),
                },
            ))
        })
        .collect()
}

impl Pipeline {
    pub(super) async fn spawn_scoped_filesystem_extraction(
        &mut self,
        job_id: JobId,
        scan_root: &Path,
        set_name: String,
        files: Vec<NestedArchiveFile>,
    ) -> Result<(), String> {
        let first = files.first().ok_or("empty nested archive set")?;
        let kind = first.archive_type;
        let inputs: Vec<PathBuf> = files
            .iter()
            .map(|file| scan_root.join(&file.relative_path))
            .collect();
        let working = self
            .jobs
            .get(&job_id)
            .ok_or("missing nested job")?
            .working_dir
            .clone();
        let containing = self
            .archive_runtime
            .get(&job_id)
            .and_then(|runtime| runtime.output_owners.get(&first.relative_path).copied());
        let runtime = self
            .archive_runtime
            .entry(job_id)
            .or_insert_with(ArchiveRuntime::new);
        runtime.set_roots(working, scan_root.to_owned());
        let lease = runtime
            .admit(containing, InputVersion(0))
            .map_err(|error| format!("nested archive admission failed: {error:?}"))?;
        let work = NamespaceWork {
            virtual_source: None,
            original_virtual_source: None,
            kind,
            set_name,
            volumes: files.iter().map(|file| file.volume_number).collect(),
            output: PathBuf::new(),
            input_names: files
                .iter()
                .map(|file| file.relative_path.clone())
                .collect(),
            original_inputs: inputs.clone(),
            inputs,
            destination: scan_root.to_owned(),
            outputs: BTreeMap::new(),
            root_set: None,
            ordinary: Vec::new(),
            selected_password: None,
        };
        runtime
            .consumed
            .extend(files.into_iter().map(|file| file.relative_path));
        self.spawn_scoped_selected_extraction(job_id, work, lease)
            .await
    }

    pub(crate) async fn spawn_scoped_selected_extraction(
        &mut self,
        job_id: JobId,
        work: NamespaceWork,
        lease: SessionLease,
    ) -> Result<(), String> {
        let input = work
            .virtual_source
            .as_ref()
            .map(|source| {
                self.archive_runtime
                    .get(&job_id)
                    .and_then(|runtime| runtime.gates.get(&(source.root_set.clone(), source.node)))
                    .cloned()
                    .ok_or("restored virtual source gate absent")
            })
            .transpose()?;
        self.spawn_scoped_engine(job_id, work, lease, input).await
    }

    pub(crate) async fn spawn_scoped_live_extraction(
        &mut self,
        job_id: JobId,
        work: NamespaceWork,
        lease: SessionLease,
        input: crate::pipeline::extraction::input::LiveArchiveInput,
    ) -> Result<(), String> {
        self.spawn_scoped_engine(job_id, work, lease, Some(input))
            .await
    }

    async fn spawn_scoped_engine(
        &mut self,
        job_id: JobId,
        mut work: NamespaceWork,
        lease: SessionLease,
        live: Option<crate::pipeline::extraction::input::LiveArchiveInput>,
    ) -> Result<(), String> {
        let owner = lease.owner;
        if live.is_none() && (work.inputs.is_empty() || work.inputs.len() != work.volumes.len()) {
            return Err("invalid selected archive volume roster".into());
        }
        let working = &self
            .jobs
            .get(&job_id)
            .ok_or("missing selected archive job")?
            .working_dir;
        let output = namespace_output_dir(working, owner);
        work.output = output.clone();
        work.outputs.clear();
        work.ordinary.clear();
        let (kind, set_name) = (work.kind, work.set_name.clone());
        let (volumes, inputs) = (work.volumes.clone(), work.inputs.clone());
        let runtime = self
            .archive_runtime
            .get_mut(&job_id)
            .ok_or("missing selected archive runtime")?;
        if let Some(input) = &live {
            runtime.live_inputs.push((owner, input.clone()));
        }
        let request = runtime
            .dispatch(owner, work)
            .map_err(|error| format!("selected archive dispatch: {error:?}"))?;
        let budget = self
            .extraction_budget(job_id, &output)
            .map_err(|error| error.to_string())?;
        let root = Arc::new(ExtractionRoot::open(&output)?);
        let permit = budget
            .task_permit_for_root(Arc::clone(&root))
            .map_err(|error| error.to_string())?;
        let passwords = self.archive_password_candidates_for_job(job_id).candidates;
        let policy = self.unacceptable_extension_policy_snapshot(job_id).await?;
        let counters = self.phase_begin(job_id, JobPhase::Extracting, None);
        let (decode, stream) = self.prepare_scoped_decode(job_id, owner, kind, &budget, &output)?;
        let pool = self.pp_pool.clone();
        let engine = ScopedEngine {
            job_id,
            set_name,
            volumes,
            inputs,
            live,
            passwords,
            policy,
            root,
            output_dir: output,
            budget,
            event_tx: self.event_tx.clone(),
            counters,
            sevenz_decode_memory: self.shared_state.sevenz_decode_memory_bytes(),
            threads: pool.current_num_threads(),
        };
        let report = ScopedReport {
            job_id,
            owner,
            request,
            lease: lease.clone(),
            done: self.extract_done_tx.clone(),
        };
        let runtime = self
            .archive_runtime
            .get_mut(&job_id)
            .ok_or("missing selected archive runtime")?;
        let task = tokio::task::spawn_blocking(move || {
            pool.install(move || {
                let _permit = permit;
                let _lease = lease;
                engine.run(decode)
            })
        });
        runtime.track_worker(owner, tokio::spawn(report.collect(task, stream)));
        Ok(())
    }

    fn prepare_scoped_decode(
        &mut self,
        job_id: JobId,
        owner: StreamOwner,
        kind: crate::jobs::assembly::ArchiveType,
        budget: &Arc<JobExtractionBudget>,
        output: &Path,
    ) -> Result<(ScopedDecode, Option<ScopedStream>), String> {
        Ok(match ArchiveEngine::of(kind) {
            ArchiveEngine::Simple(kind) => (ScopedDecode::Simple(kind), None),
            ArchiveEngine::Rar => {
                let (channel, stream) = self.open_scoped_stream(job_id, owner, budget, output)?;
                (ScopedDecode::Rar(channel), Some(stream))
            }
            ArchiveEngine::SevenZip => {
                let (channel, stream) = self.open_scoped_stream(job_id, owner, budget, output)?;
                (ScopedDecode::SevenZip(channel), Some(stream))
            }
        })
    }

    fn open_scoped_stream(
        &mut self,
        job_id: JobId,
        owner: StreamOwner,
        budget: &Arc<JobExtractionBudget>,
        output: &Path,
    ) -> Result<(ChannelOutput, ScopedStream), String> {
        let (sender, receiver) = tokio::sync::mpsc::channel(FRAME_CHANNEL_DEPTH);
        let admission = budget
            .try_reserve_memory(BRIDGE_ADMISSION_BYTES)
            .ok_or("archive bridge memory admission unavailable")?;
        let (channel, generation) = ChannelOutput::new(
            owner,
            admission,
            sender,
            tokio::runtime::Handle::current(),
            Arc::clone(budget),
        )
        .map_err(|error| error.to_string())?;
        let runtime = self
            .archive_runtime
            .get_mut(&job_id)
            .ok_or("missing selected archive runtime")?;
        runtime
            .sessions
            .attach_channel(generation.clone())
            .map_err(|error| format!("channel admission: {error:?}"))?;
        let sessions = crate::pipeline::extraction::session::ArchiveSessions::restore_checkpoint(
            &runtime.sessions.capture_checkpoint(),
            owner.root,
        )
        .map_err(|error| format!("session view: {error:?}"))?;
        let decoded = DecodedState::new(
            generation,
            sessions,
            crate::pipeline::direct_store::plan::DirectSetPlan::namespace(owner, output),
            namespace_router_resources(),
            Arc::clone(budget),
            output.to_owned(),
        )
        .map_err(|error| error.to_string())?;
        Ok((channel, ScopedStream { receiver, decoded }))
    }

    pub(crate) async fn handle_scoped_extraction_done(
        &mut self,
        job_id: JobId,
        owner: StreamOwner,
        request: RequestId,
        decoded: Option<Box<DecodedState>>,
        result: Result<FullSetExtractionOutcome, ExtractionError>,
    ) {
        let Some(work) = self
            .archive_runtime
            .get_mut(&job_id)
            .and_then(|runtime| runtime.complete(owner, request))
        else {
            return;
        };
        match ScopedOutcome::classify(&work, decoded.is_some(), result) {
            ScopedOutcome::Delegate { set_name, result } => {
                self.delegate_scoped_to_full_set(job_id, owner, set_name, result)
                    .await;
            }
            ScopedOutcome::Recover { failure } => {
                if let Some(decoded) = decoded {
                    decoded.generation.revoke();
                }
                self.recover_scoped(job_id, owner, work, failure).await;
            }
            ScopedOutcome::Publish(outcome) => {
                self.complete_scoped(job_id, owner, work, decoded, outcome)
                    .await;
            }
            ScopedOutcome::Fail(error) => self.fail_scoped(job_id, owner, error),
        }
    }

    async fn delegate_scoped_to_full_set(
        &mut self,
        job_id: JobId,
        owner: StreamOwner,
        set_name: String,
        result: Result<FullSetExtractionOutcome, ExtractionError>,
    ) {
        if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
            let _ = runtime.sessions.close(owner);
            if !runtime.pending() && runtime.decoded.is_empty() && runtime.consumed.is_empty() {
                self.archive_runtime.remove(&job_id);
            }
        }
        Box::pin(self.handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name,
            result,
        }))
        .await;
    }

    async fn recover_scoped(
        &mut self,
        job_id: JobId,
        owner: StreamOwner,
        work: NamespaceWork,
        failure: String,
    ) {
        if !self.virtual_repair_ready(job_id, &work) {
            if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
                runtime.deferred_repairs.push((owner, work));
            }
            return;
        }
        let error = match Box::pin(self.repair_scoped_failure(job_id, owner, work)).await {
            Ok(true) => return,
            Ok(false) => {
                format!("nested archive recovery could not change the failed input: {failure}")
            }
            Err(error) => format!("nested archive recovery failed for {failure}: {error}"),
        };
        self.fail_scoped(job_id, owner, error);
    }

    async fn complete_scoped(
        &mut self,
        job_id: JobId,
        owner: StreamOwner,
        mut work: NamespaceWork,
        decoded: Option<Box<DecodedState>>,
        outcome: FullSetExtractionOutcome,
    ) {
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return;
        };
        if runtime.sessions.close(owner).is_err() {
            return;
        }
        work.selected_password = outcome.selected_password;
        let members = match decoded {
            Some(decoded) => {
                match adopt_decoded(runtime, owner, &mut work, *decoded, outcome.extracted) {
                    Ok(members) => members,
                    Err(error) => return self.fail_scoped(job_id, owner, error),
                }
            }
            None => outcome.extracted,
        };
        if work.root_set.is_none()
            && runtime.graph_owners.get(&(work.set_name.clone(), 0)) == Some(&owner)
        {
            work.root_set = Some(work.set_name.clone());
        }
        runtime.completed.push((owner, work, members));
        if let Err(error) = self.drain_scoped_repairs(job_id).await {
            self.fail_job(job_id, error);
            return;
        }
        self.publish_scoped_completions(job_id).await;
    }

    fn fail_scoped(&mut self, job_id: JobId, owner: StreamOwner, error: String) {
        if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
            runtime.fail(owner);
        }
        self.fail_job(job_id, error);
    }

    pub(crate) async fn publish_scoped_completions(&mut self, job_id: JobId) {
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return;
        };
        if !runtime.publication_ready() || runtime.completed.is_empty() {
            return;
        }
        let depth = extracted_depth(runtime);
        let completed = std::mem::take(&mut runtime.completed);
        runtime.retained_work.extend(
            completed
                .iter()
                .map(|(owner, work, _)| (*owner, work.clone())),
        );
        let continuations = root_continuations(&completed);
        let destination = completed[0].1.destination.clone();
        let budget = match self.extraction_budget(job_id, &destination) {
            Ok(budget) => budget,
            Err(error) => {
                self.fail_job(job_id, error.to_string());
                return;
            }
        };
        let published =
            tokio::task::spawn_blocking(move || publish::publish(&destination, completed, &budget))
                .await
                .map_err(|error| error.to_string())
                .and_then(|published| published);
        let published = match published {
            Ok(published) => published,
            Err(error) => {
                self.fail_job(
                    job_id,
                    format!("nested archive publication failed: {error}"),
                );
                return;
            }
        };
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.extraction_depth = state.extraction_depth.max(depth);
        }
        if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
            runtime.output_owners.extend(published);
        }
        if continuations.is_empty() {
            self.schedule_job_completion_check(job_id);
        }
        for (set_name, outcome) in continuations {
            Box::pin(self.handle_extraction_done(ExtractionDone::FullSet {
                job_id,
                set_name,
                result: Ok(outcome),
            }))
            .await;
        }
    }
}

// A decoded namespace publishes its catalogued members from private backings;
// whatever else the engine reported is an ordinary output of the root.
fn adopt_decoded(
    runtime: &mut ArchiveRuntime,
    owner: StreamOwner,
    work: &mut NamespaceWork,
    decoded: DecodedState,
    extracted: Vec<String>,
) -> Result<Vec<String>, String> {
    use crate::pipeline::extraction::catalog::MemberCatalog;
    let streamed: HashSet<String> = decoded
        .catalog
        .members()
        .into_iter()
        .map(|fact| fact.name.to_owned())
        .collect();
    work.ordinary = extracted
        .into_iter()
        .filter(|member| !streamed.contains(member))
        .collect();
    work.outputs = decoded.publication().map_err(|error| error.to_string())?;
    let members = work.outputs.keys().cloned().collect();
    runtime.decoded.push(decoded);
    runtime.accept_namespace(&decoded_namespace_name(owner));
    Ok(members)
}
