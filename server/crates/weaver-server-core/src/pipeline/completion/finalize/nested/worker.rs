mod live_rar;
mod repair;

use super::*;
use crate::pipeline::extraction::output::channel::StreamOwner;
use crate::pipeline::extraction::runtime::{ArchiveRuntime, NamespaceWork, RequestId};
use crate::pipeline::extraction::session::InputVersion;

impl Pipeline {
    pub(super) async fn spawn_scoped_filesystem_extraction(
        &mut self,
        job_id: JobId,
        scan_root: &Path,
        set_name: String,
        files: Vec<NestedArchiveFile>,
    ) -> Result<(), String> {
        let kind = files[0].archive_type;
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
            .and_then(|runtime| runtime.output_owners.get(&files[0].relative_path).copied());
        let runtime = self
            .archive_runtime
            .entry(job_id)
            .or_insert_with(ArchiveRuntime::new);
        runtime.set_roots(working.clone(), scan_root.to_owned());
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
        for file in &files {
            runtime.consumed.insert(file.relative_path.clone());
        }
        self.spawn_scoped_selected_extraction(job_id, work, lease)
            .await
    }

    pub(crate) async fn spawn_scoped_selected_extraction(
        &mut self,
        job_id: JobId,
        work: NamespaceWork,
        lease: crate::pipeline::extraction::session::SessionLease,
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
        lease: crate::pipeline::extraction::session::SessionLease,
        input: crate::pipeline::extraction::input::LiveArchiveInput,
    ) -> Result<(), String> {
        self.spawn_scoped_engine(job_id, work, lease, Some(input))
            .await
    }

    async fn spawn_scoped_engine(
        &mut self,
        job_id: JobId,
        mut work: NamespaceWork,
        lease: crate::pipeline::extraction::session::SessionLease,
        live: Option<crate::pipeline::extraction::input::LiveArchiveInput>,
    ) -> Result<(), String> {
        let owner = lease.owner;
        let kind = work.kind;
        let set_name = work.set_name.clone();
        let inputs = work.inputs.clone();
        let volumes = work.volumes.clone();
        if live.is_none() && (inputs.len() != volumes.len() || inputs.is_empty()) {
            return Err("invalid selected archive volume roster".into());
        }
        let working = self
            .jobs
            .get(&job_id)
            .ok_or("missing selected archive job")?
            .working_dir
            .clone();
        let output = working
            .join(crate::pipeline::direct_store::manifest::DIRECTORY)
            .join(format!(
                "namespace-{}-{}-{}",
                owner.root, owner.node, owner.generation
            ));
        work.output = output.clone();
        work.outputs.clear();
        work.ordinary.clear();
        if let Some(input) = &live {
            self.archive_runtime
                .get_mut(&job_id)
                .ok_or("missing live archive runtime")?
                .live_inputs
                .push((owner, input.clone()));
        }
        let request = self
            .archive_runtime
            .get_mut(&job_id)
            .ok_or("missing selected archive runtime")?
            .dispatch(owner, work)
            .map_err(|e| format!("selected archive dispatch: {e:?}"))?;
        let budget = self
            .extraction_budget(job_id, &output)
            .map_err(|error| error.to_string())?;
        let root = Arc::new(ExtractionRoot::open(&output)?);
        let permit = budget
            .task_permit_for_root(Arc::clone(&root))
            .map_err(|error| error.to_string())?;
        let passwords = self.archive_password_candidates_for_job(job_id).candidates;
        let policy = self.unacceptable_extension_policy_snapshot(job_id).await?;
        let event_tx = self.event_tx.clone();
        let pool = self.pp_pool.clone();
        let threads = pool.current_num_threads();
        let memory = self.shared_state.sevenz_decode_memory_bytes();
        let counters = self.phase_begin(job_id, JobPhase::Extracting, None);
        let tx = self.extract_done_tx.clone();
        let streaming = if matches!(
            kind,
            crate::jobs::assembly::ArchiveType::SevenZip | crate::jobs::assembly::ArchiveType::Rar
        ) {
            use crate::pipeline::extraction::output::channel::ChannelOutput;
            let (sender, receiver) = tokio::sync::mpsc::channel(8);
            let (channel, generation) = ChannelOutput::new(
                owner,
                budget
                    .try_reserve_memory(512 * 1024)
                    .ok_or("archive bridge memory admission unavailable")?,
                sender,
                tokio::runtime::Handle::current(),
                Arc::clone(&budget),
            )
            .map_err(|e| e.to_string())?;
            let runtime = self
                .archive_runtime
                .get_mut(&job_id)
                .expect("admitted runtime");
            runtime
                .sessions
                .attach_channel(generation.clone())
                .map_err(|e| format!("channel admission: {e:?}"))?;
            let snapshot = runtime.sessions.capture_checkpoint();
            let sessions =
                crate::pipeline::extraction::session::ArchiveSessions::restore_checkpoint(
                    &snapshot, owner.root,
                )
                .map_err(|e| format!("session view: {e:?}"))?;
            let plan = crate::pipeline::direct_store::plan::DirectSetPlan {
                set_name: format!("namespace-{}", owner.node),
                format: crate::pipeline::direct_store::plan::SetFormat::SevenZip,
                volumes: BTreeMap::from([(0, 0)]),
                files: HashMap::from([(0, 0)]),
                identity: None,
                unposted: Default::default(),
                working_dir: output.clone(),
                destination_dir: output.clone(),
            };
            let resources = crate::pipeline::direct_store::router::RouterResources {
                accountant: Arc::default(),
                holds_budget: 4 * 1024 * 1024,
                scratch_ceiling: 8 * 1024 * 1024,
                sparse: Default::default(),
                migration_ceiling_bytes: 8 * 1024 * 1024,
            };
            let state = crate::pipeline::extraction::runtime::decoded::DecodedState::new(
                generation,
                sessions,
                plan,
                resources,
                Arc::clone(&budget),
                output.clone(),
            )?;
            Some((channel, receiver, state))
        } else {
            None
        };
        let (channel, mut receiver, mut decoded) = match streaming {
            Some((channel, receiver, state)) => (Some(channel), Some(receiver), Some(state)),
            None => (None, None, None),
        };
        let output_dir_for_rar = output.clone();
        let collector_lease = lease.clone();
        let worker = tokio::spawn(async move {
            let task = tokio::task::spawn_blocking(move || {
                pool.install(move || {
                let _permit = permit;
                let _lease = lease;
                use crate::jobs::assembly::ArchiveType as A;
                use super::super::extract::prepared;
                match kind {
                    A::Rar => {
                        let physical_root=Arc::clone(&root);
                        let channel=channel.expect("RAR stream channel");
                        channel.run(|output| {
                            let selected=crate::pipeline::extraction::output::select::SelectOutput{first:output,second:crate::pipeline::extraction::output::channel::PrivateTrackedFiles{output,root:&physical_root},select_first:|_:usize,path:&Path|matches!(weaver_model::files::FileRole::from_filename(&path.to_string_lossy()),weaver_model::files::FileRole::RarVolume{..}|weaver_model::files::FileRole::SevenZipArchive|weaver_model::files::FileRole::SevenZipSplit{..})};
                            if let Some(input)=live {
                                let password=live_rar::select_password(&input,&passwords,&budget)?;
                                crate::pipeline::direct_unpack::rar::extract_from_roster_to(crate::pipeline::direct_unpack::rar::RarStreamContext{provider:&input,volume_count:0,budget:&budget,password,counters:&counters,policy:&policy},&selected,|index|input.wait_volume(index).map_err(|e|e.to_string()),|_|Ok(true)).map_err(ExtractionError::recoverable)
                            }else {
                                prepared::PreparedRar{job_id,set_name,volume_paths:volumes.into_iter().zip(inputs).collect(),cached_headers:None,password_candidates:passwords,shared_kdf_cache:Arc::new(unrar_rs::crypto::KdfCache::new()),open_mode:crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,already_extracted:HashSet::new(),root,budget,policy,event_tx,output_dir:output_dir_for_rar,phase_counters:counters}.run_to(&selected)
                            }
                        })
                    },
                    A::SevenZip => {
                        let physical_root=Arc::clone(&root);
                        let prepared=prepared::PreparedSevenZip{job_id,set_name,part_candidates:volumes.into_iter().zip(inputs).collect(),uu_paths:HashSet::new(),passwords,root,budget,event_tx,phase_counters:counters,sevenz_decode_memory:memory,sevenz_decode_threads:u32::try_from(threads).unwrap_or(u32::MAX)};
                        let channel=channel.expect("7z stream channel");
                        channel.run(|output|{
                            let selected=crate::pipeline::extraction::output::select::SelectOutput{first:output,second:crate::pipeline::extraction::output::channel::PrivateTrackedFiles{output,root:&physical_root},select_first:|_:usize,path:&Path|matches!(weaver_model::files::FileRole::from_filename(&path.to_string_lossy()),weaver_model::files::FileRole::RarVolume{..}|weaver_model::files::FileRole::SevenZipArchive|weaver_model::files::FileRole::SevenZipSplit{..})};
                            if let Some(input)=live {prepared.run_reader_to(&selected,||Ok(input.reader()))}else{prepared.run_to(&selected)}
                        })
                    },
                    other => {
                        let kind = match other {
                            A::Zip => SimpleArchiveKind::Zip, A::Tar => SimpleArchiveKind::Tar,
                            A::TarGz => SimpleArchiveKind::TarGz, A::TarBz2 => SimpleArchiveKind::TarBz2,
                            A::TarXz => SimpleArchiveKind::TarXz, A::Gz => SimpleArchiveKind::Gz,
                            A::Deflate => SimpleArchiveKind::Deflate, A::Brotli => SimpleArchiveKind::Brotli,
                            A::Zstd => SimpleArchiveKind::Zstd, A::Bzip2 => SimpleArchiveKind::Bzip2,
                            A::Xz => SimpleArchiveKind::Xz, A::Split => SimpleArchiveKind::Split,
                            _ => unreachable!(),
                        };
                        prepared::PreparedSimple { job_id, set_name, kind, file_paths: inputs, passwords,
                            joined_output_already_present: None, root, budget, event_tx,
                            phase_counters: counters, xz_worker_threads: threads,
                        }.run()
                    }
                }
            })
            });
            let mut stream_error = None;
            if let (Some(receiver), Some(state)) = (&mut receiver, &mut decoded) {
                while let Some(frame) = receiver.recv().await {
                    if let Err(error) = state.accept(frame).await {
                        state.generation.revoke();
                        stream_error = Some(error);
                        break;
                    }
                    if let Err(error) = state.publish_progress(job_id, &tx, &collector_lease).await
                    {
                        state.generation.revoke();
                        stream_error = Some(error);
                        break;
                    }
                }
            }
            let mut result = task.await.unwrap_or_else(|error| {
                Err(ExtractionError::terminal(format!(
                    "namespace extraction worker failed: {error}"
                )))
            });
            if result
                .as_ref()
                .is_ok_and(|outcome| outcome.failed.is_empty())
                && stream_error.is_none()
                && let Some(state) = &mut decoded
            {
                if let Err(error) = state.close().await {
                    stream_error = Some(error);
                }
                if stream_error.is_none()
                    && let Err(error) = state.publish_progress(job_id, &tx, &collector_lease).await
                {
                    stream_error = Some(error);
                }
            }
            if let Some(error) = stream_error
                && result.is_ok()
            {
                result = Err(ExtractionError::terminal(error));
            }
            let _ = collector_lease
                .send_event(
                    &tx,
                    ExtractionDone::Scoped {
                        job_id,
                        owner,
                        request,
                        decoded: decoded.map(Box::new),
                        result,
                    },
                )
                .await;
        });
        self.archive_runtime
            .get_mut(&job_id)
            .expect("admitted runtime")
            .track_worker(owner, worker);
        Ok(())
    }

    pub(crate) async fn handle_scoped_extraction_done(
        &mut self,
        job_id: JobId,
        owner: StreamOwner,
        request: RequestId,
        decoded: Option<Box<crate::pipeline::extraction::runtime::decoded::DecodedState>>,
        result: Result<FullSetExtractionOutcome, ExtractionError>,
    ) {
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return;
        };
        let Some(mut work) = runtime.complete(owner, request) else {
            return;
        };
        if let Some(set_name) = work
            .root_set
            .as_ref()
            .filter(|_| decoded.is_none())
            .cloned()
        {
            let _ = runtime.sessions.close(owner);
            if !runtime.pending() && runtime.decoded.is_empty() && runtime.consumed.is_empty() {
                self.archive_runtime.remove(&job_id);
            }
            Box::pin(self.handle_extraction_done(ExtractionDone::FullSet {
                job_id,
                set_name,
                result,
            }))
            .await;
            return;
        }
        let recoverable = match &result {
            Ok(outcome) => {
                !outcome.failed.is_empty()
                    && outcome.failed.iter().all(|(_, error)| {
                        matches!(
                        error.class(),
                        crate::pipeline::extraction::ExtractionErrorClass::Recoverable
                            | crate::pipeline::extraction::ExtractionErrorClass::MissingFirstVolume
                            | crate::pipeline::extraction::ExtractionErrorClass::StaleTopology
                    )
                    })
            }
            Err(error) => matches!(
                error.class(),
                crate::pipeline::extraction::ExtractionErrorClass::Recoverable
                    | crate::pipeline::extraction::ExtractionErrorClass::MissingFirstVolume
                    | crate::pipeline::extraction::ExtractionErrorClass::StaleTopology
            ),
        };
        if recoverable {
            let failure = match &result {
                Ok(outcome) => outcome.failed[0].1.to_string(),
                Err(error) => error.to_string(),
            };
            if let Some(decoded) = decoded {
                decoded.generation.revoke();
                drop(decoded);
            }
            if !self.virtual_repair_ready(job_id, &work) {
                self.archive_runtime
                    .get_mut(&job_id)
                    .expect("admitted runtime")
                    .deferred_repairs
                    .push((owner, work));
                return;
            }
            match Box::pin(self.repair_scoped_failure(job_id, owner, work)).await {
                Ok(true) => return,
                Ok(false) => {
                    if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
                        runtime.fail(owner);
                    }
                    self.fail_job(
                        job_id,
                        format!(
                            "nested archive recovery could not change the failed input: {failure}"
                        ),
                    );
                }
                Err(error) => {
                    if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
                        runtime.fail(owner);
                    }
                    self.fail_job(
                        job_id,
                        format!("nested archive recovery failed for {failure}: {error}"),
                    );
                }
            }
            return;
        }
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return;
        };
        match result {
            Ok(outcome) if outcome.failed.is_empty() => {
                if runtime.sessions.close(owner).is_err() {
                    return;
                }
                work.selected_password = outcome.selected_password;
                let members = if let Some(decoded) = decoded {
                    {
                        use crate::pipeline::extraction::catalog::MemberCatalog;
                        let streamed: HashSet<String> = decoded
                            .catalog
                            .members()
                            .into_iter()
                            .map(|fact| fact.name.to_owned())
                            .collect();
                        work.ordinary = outcome
                            .extracted
                            .iter()
                            .filter(|member| !streamed.contains(*member))
                            .cloned()
                            .collect();
                    }
                    work.outputs = match decoded.publication() {
                        Ok(outputs) => outputs,
                        Err(error) => {
                            runtime.fail(owner);
                            self.fail_job(job_id, error);
                            return;
                        }
                    };
                    let members = work.outputs.keys().cloned().collect();
                    runtime.decoded.push(*decoded);
                    runtime
                        .accept_namespace(&format!("decoded-{}-{}", owner.node, owner.generation));
                    members
                } else {
                    outcome.extracted
                };
                if work.root_set.is_none()
                    && runtime.graph_owners.get(&(work.set_name.clone(), 0)) == Some(&owner)
                {
                    work.root_set = Some(work.set_name.clone());
                }
                runtime.completed.push((owner, work, members));
            }
            Ok(outcome) => {
                runtime.fail(owner);
                self.fail_job(
                    job_id,
                    format!("nested archive extraction failed: {}", outcome.failed[0].1),
                );
                return;
            }
            Err(error) => {
                runtime.fail(owner);
                self.fail_job(job_id, error.to_string());
                return;
            }
        }
        if let Err(error) = self.drain_scoped_repairs(job_id).await {
            self.fail_job(job_id, error);
            return;
        }
        self.publish_scoped_completions(job_id).await;
    }

    pub(crate) async fn publish_scoped_completions(&mut self, job_id: JobId) {
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return;
        };
        if !runtime.publication_ready() || runtime.completed.is_empty() {
            return;
        }
        let extracted_depth = runtime
            .decoded
            .iter()
            .map(|state| {
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
            })
            .chain(runtime.completed.iter().filter_map(|(owner, _, _)| {
                runtime
                    .sessions
                    .depth(*owner)
                    .map(|depth| depth.saturating_sub(1))
            }))
            .max()
            .unwrap_or(0);
        let completed = std::mem::take(&mut runtime.completed);
        runtime.retained_work.extend(
            completed
                .iter()
                .map(|(owner, work, _)| (*owner, work.clone())),
        );
        let continuations: Vec<_> = completed
            .iter()
            .filter_map(|(_, work, members)| {
                work.root_set.as_ref().map(|set_name| {
                    (
                        set_name.clone(),
                        work.ordinary
                            .iter()
                            .cloned()
                            .chain(members.iter().cloned())
                            .collect::<Vec<_>>(),
                        work.selected_password.clone(),
                    )
                })
            })
            .collect();
        let destination = completed[0].1.destination.clone();
        let budget = match self.extraction_budget(job_id, &destination) {
            Ok(budget) => budget,
            Err(error) => {
                self.fail_job(job_id, error.to_string());
                return;
            }
        };
        let result =
            tokio::task::spawn_blocking(move || -> Result<Vec<(String, StreamOwner)>, String> {
                let destination = ExtractionRoot::open(&destination)?;
                let mut names = HashSet::new();
                let mut metadata_sets = Vec::new();
                for (_, work, _) in &completed {
                    for ordinary in &work.ordinary {
                        if !names.insert(ordinary.clone()) {
                            return Err(format!("nested archive output collision: {ordinary}"));
                        }
                    }
                }
                // Validate every sibling before publishing any of them.
                for (_, work, members) in completed
                    .iter()
                    .filter(|(_, _, members)| !members.is_empty())
                {
                    let source = ExtractionRoot::open_existing(&work.output)?;
                    let metadata: BTreeMap<_, _> = source
                        .list_entries_no_links(&[])
                        .map_err(|error| error.to_string())?
                        .into_iter()
                        .collect();
                    for member in members {
                        let relative = Path::new(member);
                        if !names.insert(member.clone()) || destination.open_input(relative).is_ok()
                        {
                            return Err(format!("nested archive output collision: {member}"));
                        }
                        let backing = work
                            .outputs
                            .get(member)
                            .map(|path| {
                                path.strip_prefix(&work.output)
                                    .map_err(|_| "namespace backing escaped root")
                            })
                            .transpose()?
                            .unwrap_or(relative);
                        if !metadata.contains_key(backing) {
                            return Err(format!("nested output missing: {member}"));
                        }
                    }
                    metadata_sets.push(metadata);
                }
                let mut published = Vec::new();
                let mut directories = Vec::new();
                let apply_metadata =
                    |relative: &Path, metadata: &cap_std::fs::Metadata| -> Result<(), String> {
                        #[cfg(unix)]
                        let mode = {
                            use cap_std::fs::PermissionsExt;
                            Some(metadata.permissions().mode())
                        };
                        #[cfg(not(unix))]
                        let mode = None;
                        destination
                            .set_directory_metadata(
                                relative,
                                metadata.modified().ok().map(|time| time.into_std()),
                                metadata.accessed().ok().map(|time| time.into_std()),
                                mode,
                            )
                            .map_err(|error| error.to_string())
                    };
                for ((owner, work, members), metadata) in completed
                    .into_iter()
                    .filter(|(_, _, members)| !members.is_empty())
                    .zip(metadata_sets)
                {
                    let source = ExtractionRoot::open_existing(&work.output)?;
                    for member in members {
                        let relative = Path::new(&member);
                        let backing = work
                            .outputs
                            .get(&member)
                            .map(|path| {
                                path.strip_prefix(&work.output)
                                    .map_err(|_| "namespace backing escaped root")
                            })
                            .transpose()?
                            .unwrap_or(relative);
                        let metadata = metadata
                            .get(backing)
                            .ok_or("nested output metadata absent")?;
                        if metadata.is_dir() {
                            destination
                                .create_dir(relative, &budget)
                                .map_err(|error| error.to_string())?;
                            directories.push((relative.to_owned(), metadata.clone()));
                            published.push((member, owner));
                            continue;
                        }
                        let mut input = source
                            .open_input(backing)
                            .map_err(|error| error.to_string())?;
                        let mut output = destination
                            .create_file(Path::new(&member), &budget)
                            .map_err(|error| error.to_string())?;
                        std::io::copy(&mut input, &mut output)
                            .map_err(|error| error.to_string())?;
                        std::io::Write::flush(&mut output).map_err(|error| error.to_string())?;
                        drop(output);
                        apply_metadata(relative, metadata)?;
                        published.push((member, owner));
                    }
                }
                directories.sort_by_key(|(path, _)| std::cmp::Reverse(path.components().count()));
                for (relative, metadata) in directories {
                    apply_metadata(&relative, &metadata)?;
                }
                Ok(published)
            })
            .await;
        match result {
            Ok(Ok(published)) => {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.extraction_depth = state.extraction_depth.max(extracted_depth);
                }
                if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
                    runtime.output_owners.extend(published);
                }
                if continuations.is_empty() {
                    self.schedule_job_completion_check(job_id);
                }
                for (set_name, extracted, selected_password) in continuations {
                    Box::pin(self.handle_extraction_done(ExtractionDone::FullSet {
                        job_id,
                        set_name,
                        result: Ok(FullSetExtractionOutcome {
                            extracted,
                            failed: Vec::new(),
                            selected_password,
                        }),
                    }))
                    .await;
                }
            }
            error => self.fail_job(
                job_id,
                format!("nested archive publication failed: {error:?}"),
            ),
        }
    }
}
