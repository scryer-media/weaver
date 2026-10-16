use super::*;
use crate::pipeline::direct_store::plan::{DirectSetPlan, SetFormat};
use crate::pipeline::direct_store::router::RouterResources;
use crate::pipeline::extraction::JobExtractionBudget;
use crate::pipeline::extraction::output::channel::{ChannelOutput, Frame, Generation};
use crate::pipeline::*;

pub(crate) struct RootDispatch {
    pub(crate) channel: Option<ChannelOutput>,
    pub(crate) owner: StreamOwner,
    pub(crate) request: RequestId,
    receiver: tokio::sync::mpsc::Receiver<Frame>,
    generation: Generation,
    sessions: Option<ArchiveSessions>,
    _lease: SessionLease,
    plan: DirectSetPlan,
    budget: Arc<JobExtractionBudget>,
    private: PathBuf,
    job_id: JobId,
    event_tx: tokio::sync::mpsc::Sender<ExtractionDone>,
}

enum StreamInput {
    Physical {
        parts: Vec<(u32, PathBuf)>,
        kind: crate::jobs::assembly::ArchiveType,
    },
    Virtual {
        source: VirtualInputSource,
        names: BTreeMap<u32, String>,
    },
}

impl Pipeline {
    pub(crate) async fn retire_unusable_root_stream(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), ExtractionError> {
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return Ok(());
        };
        if runtime
            .graph_owners
            .get(&(set_name.to_owned(), 0))
            .is_some_and(|owner| !runtime.sessions.accepts(*owner))
        {
            runtime
                .retire_graph_namespace(set_name)
                .await
                .map_err(|error| {
                    ExtractionError::terminal(format!("root archive retirement: {error:?}"))
                })?;
        }
        Ok(())
    }

    pub(crate) fn admit_root_stream(
        &mut self,
        job_id: JobId,
        set_name: &str,
        parts: Vec<(u32, PathBuf)>,
        kind: crate::jobs::assembly::ArchiveType,
        destination: PathBuf,
        budget: Arc<JobExtractionBudget>,
    ) -> Result<RootDispatch, ExtractionError> {
        self.admit_stream(
            job_id,
            set_name,
            StreamInput::Physical { parts, kind },
            destination,
            budget,
        )
    }
    pub(crate) fn admit_virtual_chase_stream(
        &mut self,
        job_id: JobId,
        set_name: &str,
        source: VirtualInputSource,
        destination: PathBuf,
        budget: Arc<JobExtractionBudget>,
        input_names: BTreeMap<u32, String>,
    ) -> Result<RootDispatch, ExtractionError> {
        self.admit_stream(
            job_id,
            set_name,
            StreamInput::Virtual {
                source,
                names: input_names,
            },
            destination,
            budget,
        )
    }
    fn admit_stream(
        &mut self,
        job_id: JobId,
        set_name: &str,
        input: StreamInput,
        destination: PathBuf,
        budget: Arc<JobExtractionBudget>,
    ) -> Result<RootDispatch, ExtractionError> {
        let (parts, kind, source, names) = match input {
            StreamInput::Physical { parts, kind } => (parts, kind, None, BTreeMap::new()),
            StreamInput::Virtual { source, names } => (
                Vec::new(),
                crate::jobs::assembly::ArchiveType::Rar,
                Some(source),
                names,
            ),
        };
        let (mut volumes, inputs): (Vec<_>, Vec<_>) = parts.into_iter().unzip();
        if source.is_some() {
            volumes = names.keys().copied().collect();
        }
        let working = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| ExtractionError::terminal("missing root archive job"))?
            .working_dir
            .clone();
        let admission = budget
            .try_reserve_memory(512 * 1024)
            .ok_or_else(|| ExtractionError::terminal("root archive bridge memory unavailable"))?;
        let runtime = self
            .archive_runtime
            .entry(job_id)
            .or_insert_with(ArchiveRuntime::new);
        runtime.set_roots(working.clone(), destination.clone());
        let lease = if let Some(source) = &source {
            if !runtime
                .graph_owners
                .contains_key(&(source.root_set.clone(), source.node))
                && source.node == 0
            {
                runtime
                    .graph_lease(&source.root_set, 0, None, InputVersion(0))
                    .map_err(|e| {
                        ExtractionError::terminal(format!("virtual parent admission: {e:?}"))
                    })?;
            }
            let parent = *runtime
                .graph_owners
                .get(&(source.root_set.clone(), source.node))
                .ok_or_else(|| ExtractionError::terminal("virtual chase parent absent"))?;
            runtime.admit(Some(parent), InputVersion(0))
        } else {
            runtime.graph_lease(set_name, 0, None, InputVersion(0))
        }
        .map_err(|e| ExtractionError::terminal(format!("root archive admission: {e:?}")))?;
        let owner = lease.owner;
        let private = working
            .join(crate::pipeline::direct_store::manifest::DIRECTORY)
            .join(format!(
                "namespace-{}-{}-{}",
                owner.root, owner.node, owner.generation
            ));

        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let (channel, generation) = ChannelOutput::new(
            owner,
            admission,
            sender,
            tokio::runtime::Handle::current(),
            Arc::clone(&budget),
        )
        .map_err(|e| ExtractionError::terminal(e.to_string()))?;
        runtime
            .sessions
            .attach_channel(generation.clone())
            .map_err(|e| ExtractionError::terminal(format!("root channel admission: {e:?}")))?;
        let saved = runtime.sessions.capture_checkpoint();
        let sessions = ArchiveSessions::restore_checkpoint(&saved, owner.root)
            .map_err(|e| ExtractionError::terminal(format!("root session view: {e:?}")))?;
        let request = runtime
            .dispatch(
                owner,
                NamespaceWork {
                    virtual_source: source.clone(),
                    original_virtual_source: source.clone(),
                    kind,
                    set_name: set_name.to_owned(),
                    volumes,
                    output: private.clone(),
                    input_names: if source.is_some() {
                        names.into_values().collect()
                    } else {
                        inputs
                            .iter()
                            .map(|path| {
                                path.strip_prefix(&working)
                                    .or_else(|_| path.strip_prefix(&destination))
                                    .unwrap_or(path)
                                    .to_string_lossy()
                                    .into_owned()
                            })
                            .collect()
                    },
                    original_inputs: inputs.clone(),
                    inputs,
                    destination,
                    outputs: BTreeMap::new(),
                    root_set: Some(set_name.to_owned()),
                    ordinary: Vec::new(),
                    selected_password: None,
                },
            )
            .map_err(|e| ExtractionError::terminal(format!("root archive dispatch: {e:?}")))?;
        let plan = DirectSetPlan {
            set_name: format!("namespace-{}", owner.node),
            format: SetFormat::SevenZip,
            volumes: BTreeMap::from([(0, 0)]),
            files: HashMap::from([(0, 0)]),
            identity: None,
            unposted: Default::default(),
            working_dir: private.clone(),
            destination_dir: private.clone(),
        };
        Ok(RootDispatch {
            channel: Some(channel),
            owner,
            request,
            receiver,
            generation,
            sessions: Some(sessions),
            _lease: lease,
            plan,
            budget,
            private,
            job_id,
            event_tx: self.extract_done_tx.clone(),
        })
    }
}

impl RootDispatch {
    pub(crate) fn lease(&self) -> SessionLease {
        self._lease.clone()
    }
    pub(crate) async fn collect(
        mut self,
        worker: tokio::task::JoinHandle<Result<FullSetExtractionOutcome, ExtractionError>>,
    ) -> (
        Result<FullSetExtractionOutcome, ExtractionError>,
        Option<Box<decoded::DecodedState>>,
    ) {
        // No catalog, router, registry or private output directory is admitted
        // until an archive member actually appears. Ordinary members retain
        // the established file-output path and do not cross the bridge.
        self.channel.take();
        let mut state = None;
        let mut stream_error = None;
        loop {
            let frame = tokio::select! {
                biased;
                _ = self._lease.cancelled() => { self.generation.revoke(); stream_error = Some("archive generation retired".into()); break; },
                frame = self.receiver.recv() => match frame { Some(frame) => frame, None => break },
            };
            if state.is_none() {
                let resources = RouterResources {
                    accountant: Arc::default(),
                    holds_budget: 4 * 1024 * 1024,
                    scratch_ceiling: 8 * 1024 * 1024,
                    sparse: Default::default(),
                    migration_ceiling_bytes: 8 * 1024 * 1024,
                };
                match decoded::DecodedState::new(
                    self.generation.clone(),
                    self.sessions.take().expect("first frame session view"),
                    self.plan.clone(),
                    resources,
                    Arc::clone(&self.budget),
                    self.private.clone(),
                ) {
                    Ok(decoded) => state = Some(Box::new(decoded)),
                    Err(error) => {
                        stream_error = Some(error);
                        self.generation.revoke();
                        break;
                    }
                }
            }
            if let Err(error) = state
                .as_mut()
                .expect("initialized decoded state")
                .accept(frame)
                .await
            {
                stream_error = Some(error);
                self.generation.revoke();
                break;
            }
            if let Err(error) = state
                .as_mut()
                .expect("initialized decoded state")
                .publish_progress(self.job_id, &self.event_tx, &self._lease)
                .await
            {
                stream_error = Some(error);
                self.generation.revoke();
                break;
            }
        }
        drop(self.receiver);
        let mut result = worker.await.unwrap_or_else(|error| {
            Err(ExtractionError::terminal(format!(
                "root archive worker failed: {error}"
            )))
        });
        if result
            .as_ref()
            .is_ok_and(|outcome| outcome.failed.is_empty())
            && stream_error.is_none()
            && let Some(state) = &mut state
        {
            if let Err(error) = state.close().await {
                stream_error = Some(error);
            }
            if stream_error.is_none()
                && let Err(error) = state
                    .publish_progress(self.job_id, &self.event_tx, &self._lease)
                    .await
            {
                stream_error = Some(error);
            }
        }
        if let Some(error) = stream_error
            && result.is_ok()
        {
            result = Err(ExtractionError::terminal(error));
        }
        (result, state)
    }
}

#[derive(Clone)]
pub(crate) struct ScopedMemberOutput {
    channel: ChannelOutput,
    occurrence: usize,
}
impl ScopedMemberOutput {
    pub(crate) fn run<T, E>(&self, worker: impl FnOnce(&Self) -> Result<T, E>) -> Result<T, E> {
        self.channel.run(|_| worker(self))
    }
}
impl super::super::output::ArchiveOutput for ScopedMemberOutput {
    type Member = <ChannelOutput as super::super::output::ArchiveOutput>::Member;
    fn file(
        &self,
        _: usize,
        path: &std::path::Path,
        length: Option<u64>,
    ) -> std::io::Result<Self::Member> {
        self.channel.file(self.occurrence, path, length)
    }
    fn directory(&self, _: usize, path: &std::path::Path) -> std::io::Result<()> {
        self.channel.directory(self.occurrence, path)
    }
    fn directory_metadata(
        &self,
        _: usize,
        path: &std::path::Path,
        metadata: super::super::output::Metadata,
    ) -> std::io::Result<()> {
        self.channel
            .directory_metadata(self.occurrence, path, metadata)
    }
}

pub(super) struct PersistentSink {
    output: ChannelOutput,
    generation: Generation,
    request: RequestId,
    owner: StreamOwner,
    members: BTreeMap<String, usize>,
    finish: tokio::sync::oneshot::Sender<Result<FullSetExtractionOutcome, ExtractionError>>,
}

impl Pipeline {
    pub(crate) async fn scoped_archive_sink(
        &mut self,
        job_id: JobId,
        set_name: &str,
        member: &str,
        parts: Vec<(u32, PathBuf)>,
        kind: crate::jobs::assembly::ArchiveType,
    ) -> Result<ScopedMemberOutput, ExtractionError> {
        self.retire_unusable_root_stream(job_id, set_name).await?;
        if let Some(sink) = self
            .archive_runtime
            .get_mut(&job_id)
            .and_then(|runtime| runtime.sinks.get_mut(set_name))
        {
            let occurrence = if let Some(index) = sink.members.get(member) {
                *index
            } else {
                let index = sink.members.len();
                sink.members.insert(member.to_owned(), index);
                index
            };
            return Ok(ScopedMemberOutput {
                channel: sink.output.clone(),
                occurrence,
            });
        }
        let staging = self.extraction_staging_dir(job_id);
        let budget = self.extraction_budget(job_id, &staging)?;
        let mut dispatch =
            self.admit_root_stream(job_id, set_name, parts, kind, staging, budget)?;
        let output = dispatch.channel.take().expect("persistent archive channel");
        let owner = dispatch.owner;
        let request = dispatch.request.clone();
        let generation = dispatch.generation.clone();
        let runtime = self
            .archive_runtime
            .get_mut(&job_id)
            .expect("persistent archive runtime");
        runtime
            .requests
            .get_mut(&request.sequence)
            .expect("persistent archive request")
            .work
            .root_set = None;
        let mut members = BTreeMap::new();
        members.insert(member.to_owned(), 0);
        let (finish, finished) = tokio::sync::oneshot::channel();
        runtime.sinks.insert(
            set_name.to_owned(),
            PersistentSink {
                output: output.clone(),
                generation,
                request: request.clone(),
                owner,
                members,
                finish,
            },
        );
        let tx = self.extract_done_tx.clone();
        let completion_lease = dispatch.lease();
        let worker = tokio::spawn(async move {
            let producer = tokio::task::spawn_blocking(move || {
                finished.blocking_recv().unwrap_or_else(|_| {
                    Err(ExtractionError::terminal(
                        "persistent archive producer disappeared",
                    ))
                })
            });
            let (result, decoded) = dispatch.collect(producer).await;
            let _ = completion_lease
                .send_event(
                    &tx,
                    ExtractionDone::Scoped {
                        job_id,
                        owner,
                        request,
                        decoded,
                        result,
                    },
                )
                .await;
        });
        self.archive_runtime
            .get_mut(&job_id)
            .expect("persistent archive runtime")
            .track_worker(owner, worker);
        Ok(ScopedMemberOutput {
            channel: output,
            occurrence: 0,
        })
    }

    pub(crate) fn close_scoped_archive_sink(&mut self, job_id: JobId, set_name: &str) {
        let paths = self.volume_paths_for_rar_set(job_id, set_name);
        if let Some(runtime) = self.archive_runtime.get_mut(&job_id)
            && let Some(sink) = runtime.sinks.remove(set_name)
        {
            if let Some(request) = runtime.requests.get_mut(&sink.request.sequence) {
                let work = &mut request.work;
                let working = runtime
                    .checkpoint_roots
                    .as_ref()
                    .map(|(working, _)| working.clone());
                for (volume, path) in paths {
                    if !work.volumes.contains(&volume) {
                        work.volumes.push(volume);
                        work.inputs.push(path.clone());
                        work.original_inputs.push(path.clone());
                        work.input_names.push(
                            working
                                .as_ref()
                                .and_then(|root| path.strip_prefix(root).ok())
                                .unwrap_or(&path)
                                .to_string_lossy()
                                .into_owned(),
                        );
                    }
                }
            }
            let _ = sink.finish.send(Ok(FullSetExtractionOutcome {
                extracted: Vec::new(),
                failed: Vec::new(),
                selected_password: None,
            }));
        }
    }

    pub(crate) fn revoke_scoped_archive_sink(
        &mut self,
        job_id: JobId,
        set_name: &str,
        error: ExtractionError,
    ) {
        if let Some(runtime) = self.archive_runtime.get_mut(&job_id)
            && let Some(sink) = runtime.sinks.remove(set_name)
        {
            if let Some(extracted) = self.extracted_members.get_mut(&job_id) {
                for member in sink.members.keys() {
                    extracted.remove(member);
                }
            }
            if let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.to_owned())) {
                for member in sink.members.keys() {
                    state.in_flight_members.remove(member);
                }
            }
            sink.generation.revoke();
            let _ = sink.finish.send(Err(error));
        }
    }
}

impl ArchiveRuntime {
    pub(crate) fn scoped_decision_pending(&self, set_name: &str) -> bool {
        let Some(owner) = self.graph_owners.get(&(set_name.to_owned(), 0)) else {
            return false;
        };
        // A revoked channel cannot admit more members, but its joined result
        // still owns the failure and recovery decision. Keep that request until
        // the coordinator consumes it instead of replacing its generation.
        (!self.sinks.contains_key(set_name) || !self.sessions.accepts(*owner))
            && (self
                .requests
                .values()
                .any(|request| request.owner == *owner && request.work.root_set.is_none())
                || self
                    .deferred_repairs
                    .iter()
                    .any(|(pending, _)| pending == owner))
    }

    pub(super) fn retire_sinks(&mut self, owners: &[StreamOwner]) {
        self.sinks.retain(|_, sink| {
            if owners.contains(&sink.owner) {
                sink.generation.revoke();
                false
            } else {
                true
            }
        });
    }
    pub(crate) fn scoped_streamed_member(&self, set_name: &str, member: &str) -> bool {
        self.sinks
            .get(set_name)
            .is_some_and(|sink| sink.members.contains_key(member))
            || self.retained_work.iter().any(|(_, work)| {
                work.set_name == set_name
                    && self.decoded.iter().any(|state| {
                        use crate::pipeline::extraction::catalog::MemberCatalog;
                        state
                            .catalog
                            .members()
                            .iter()
                            .any(|fact| fact.name == member)
                    })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn repair_joins_persistent_collector_with_full_completion_queue() {
        let directory = tempfile::tempdir().unwrap();
        let private = directory.path().join("private");
        let budget = JobExtractionBudget::new(
            Arc::new(crate::pipeline::extraction::ExtractionLimits {
                max_job_bytes: 8192,
                max_member_bytes: 8192,
                max_entries: 32,
                max_ratio: 100,
                max_seconds: 60,
                min_free_bytes: 1,
                max_memory_bytes: 64 * 1024 * 1024,
            }),
            directory.path().to_owned(),
            8192,
            0,
            0,
            crate::operations::metrics::PipelineMetrics::new(),
        )
        .unwrap();
        let mut runtime = ArchiveRuntime::new();
        let lease = runtime.admit(None, InputVersion(0)).unwrap();
        let owner = lease.owner;
        let request = runtime
            .dispatch(
                owner,
                NamespaceWork {
                    kind: crate::jobs::assembly::ArchiveType::Rar,
                    set_name: "archive".into(),
                    volumes: Vec::new(),
                    output: private.clone(),
                    inputs: Vec::new(),
                    original_inputs: Vec::new(),
                    input_names: Vec::new(),
                    virtual_source: None,
                    original_virtual_source: None,
                    destination: directory.path().join("staging"),
                    outputs: BTreeMap::new(),
                    root_set: None,
                    ordinary: Vec::new(),
                    selected_password: None,
                },
            )
            .unwrap();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let (output, generation) = ChannelOutput::new(
            owner,
            budget.try_reserve_memory(4096).unwrap(),
            sender,
            tokio::runtime::Handle::current(),
            Arc::clone(&budget),
        )
        .unwrap();
        runtime.sessions.attach_channel(generation.clone()).unwrap();
        let sessions =
            ArchiveSessions::restore_checkpoint(&runtime.sessions.capture_checkpoint(), owner.root)
                .unwrap();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
        assert!(
            event_tx
                .try_send(ExtractionDone::ScopedNamespace {
                    job_id: JobId(1),
                    owner,
                    namespace: "occupied".into(),
                    producers: Vec::new(),
                    children: Vec::new(),
                })
                .is_ok()
        );
        let (finish, finished) = tokio::sync::oneshot::channel();
        runtime.sinks.insert(
            "archive".into(),
            PersistentSink {
                output: output.clone(),
                generation: generation.clone(),
                request: request.clone(),
                owner,
                members: BTreeMap::new(),
                finish,
            },
        );
        let dispatch = RootDispatch {
            channel: None,
            owner,
            request: request.clone(),
            receiver,
            generation,
            sessions: Some(sessions),
            _lease: lease.clone(),
            plan: DirectSetPlan {
                set_name: "archive".into(),
                format: SetFormat::SevenZip,
                volumes: BTreeMap::from([(0, 0)]),
                files: HashMap::from([(0, 0)]),
                identity: None,
                unposted: Default::default(),
                working_dir: private.clone(),
                destination_dir: private.clone(),
            },
            budget,
            private,
            job_id: JobId(1),
            event_tx: event_tx.clone(),
        };
        let (started, ready) = tokio::sync::oneshot::channel();
        runtime.track_worker(
            owner,
            tokio::spawn(async move {
                let producer = tokio::spawn(async move {
                    started.send(()).unwrap();
                    finished
                        .await
                        .unwrap_or_else(|_| Err(ExtractionError::terminal("producer retired")))
                });
                let (result, decoded) = dispatch.collect(producer).await;
                assert!(result.is_err());
                assert!(decoded.is_none());
                assert!(
                    !lease
                        .send_event(
                            &event_tx,
                            ExtractionDone::Scoped {
                                job_id: JobId(1),
                                owner,
                                request,
                                decoded,
                                result,
                            }
                        )
                        .await
                );
            }),
        );
        ready.await.unwrap();
        // Keep a producer clone alive: closing the sink alone cannot end recv.
        runtime.begin_repair(owner).await.unwrap();
        assert!(runtime.sinks.is_empty());
        assert!(runtime.workers.is_empty());
        assert!(
            runtime
                .sessions
                .prepare_replacement(owner.node, InputVersion(1))
                .is_ok()
        );
        assert!(matches!(
            event_rx.try_recv().unwrap(),
            ExtractionDone::ScopedNamespace { .. }
        ));
        assert!(event_rx.try_recv().is_err());
        drop(output);
    }
}
