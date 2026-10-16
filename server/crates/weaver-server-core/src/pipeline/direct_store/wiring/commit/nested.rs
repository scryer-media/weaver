use super::*;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

impl Pipeline {
    pub(crate) async fn refresh_direct_live_inputs(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), String> {
        let Some(set) = self
            .direct_store
            .sets_mut(job_id)
            .iter_mut()
            .find(|set| set.plan().set_name == set_name)
        else {
            return Ok(());
        };
        if set.is_delegated() || !set.take_live_refresh() {
            return Ok(());
        }
        let mut retired = std::collections::HashSet::new();
        if let (Some(graph), Some(runtime)) =
            (set.nested_graph(), self.archive_runtime.get(&job_id))
        {
            for (index, node) in graph.nodes.iter().enumerate() {
                let id = index as u32 + 1;
                if retired.contains(&node.parent) || runtime.graph_source_replaced(set_name, id) {
                    retired.insert(id);
                }
            }
        }
        let producers: Vec<_> = set
            .direct_producer_states()
            .into_iter()
            .filter(|(node, _, _, _)| !retired.contains(node))
            .collect();
        if producers.is_empty() {
            return Ok(());
        }
        let children: Vec<_> = set
            .live_child_descriptions()
            .into_iter()
            .filter(|child| !retired.contains(&child.node))
            .collect();
        let destination = set.plan().destination_dir.clone();
        let working = self
            .jobs
            .get(&job_id)
            .ok_or("missing direct job")?
            .working_dir
            .clone();
        self.publish_scoped_namespace(
            job_id,
            set_name,
            crate::pipeline::extraction::runtime::decoded::NamespaceProgress {
                producers,
                children,
            },
            working,
            destination,
            None,
        )
        .await
    }

    pub(crate) async fn publish_scoped_namespace(
        &mut self,
        job_id: JobId,
        set_name: &str,
        progress: crate::pipeline::extraction::runtime::decoded::NamespaceProgress,
        working: PathBuf,
        destination: PathBuf,
        speculative: Option<crate::pipeline::extraction::output::channel::StreamOwner>,
    ) -> Result<(), String> {
        let crate::pipeline::extraction::runtime::decoded::NamespaceProgress {
            producers,
            children,
        } = progress;
        use crate::pipeline::extraction::runtime::{
            ArchiveRuntime, NamespaceWork, VirtualInputSource,
        };
        use crate::pipeline::extraction::session::InputVersion;
        let runtime = self
            .archive_runtime
            .entry(job_id)
            .or_insert_with(ArchiveRuntime::new);
        runtime.set_roots(working, destination.clone());
        // Capture the previous execution mode before the binding pass admits
        // fresh child keys. A direct producer becoming a decoder invalidates
        // every result produced below its previous interpretation.
        for child in &children {
            runtime
                .prepare_graph_decoder(set_name, child.node, child.parent)
                .await
                .map_err(|error| format!("direct child transition: {error:?}"))?;
        }
        let mut bindings: Vec<_> = producers
            .iter()
            .map(|&(node, parent, generation, _)| (node, parent, generation))
            .chain(
                children
                    .iter()
                    .map(|child| (child.node, Some(child.parent), child.generation)),
            )
            .collect();
        bindings.sort_by_key(|entry| entry.0);
        for (node, parent, _generation) in bindings {
            runtime
                .graph_lease(set_name, node, parent, InputVersion(0))
                .map_err(|error| format!("direct namespace admission: {error:?}"))?;
        }
        for child in children {
            if self.archive_runtime[&job_id].graph_source_replaced(set_name, child.node) {
                continue;
            }
            let key = (set_name.to_owned(), child.node);
            let fresh = !self.archive_runtime[&job_id].gates.contains_key(&key);
            let runtime = self
                .archive_runtime
                .get_mut(&job_id)
                .expect("admitted runtime");
            let input = runtime.gates.entry(key).or_default().clone();
            input.publish(
                child.parts,
                child.roster_closed,
                &self.process_memory_budget,
            )?;
            if fresh && !runtime.has_replay_source(set_name, child.node) {
                let lease = runtime
                    .graph_lease(set_name, child.node, Some(child.parent), InputVersion(0))
                    .map_err(|error| format!("direct child admission: {error:?}"))?;
                let source = VirtualInputSource {
                    root_set: set_name.to_owned(),
                    node: child.node,
                };
                let work = NamespaceWork {
                    kind: match child.format {
                        crate::pipeline::direct_store::plan::SetFormat::Rar => {
                            crate::jobs::assembly::ArchiveType::Rar
                        }
                        crate::pipeline::direct_store::plan::SetFormat::SevenZip => {
                            crate::jobs::assembly::ArchiveType::SevenZip
                        }
                    },
                    set_name: child.set_name,
                    volumes: child.input_names.keys().copied().collect(),
                    input_names: child.input_names.into_values().collect(),
                    inputs: Vec::new(),
                    original_inputs: Vec::new(),
                    virtual_source: Some(source.clone()),
                    original_virtual_source: Some(source),
                    output: PathBuf::new(),
                    destination: destination.clone(),
                    outputs: Default::default(),
                    root_set: None,
                    ordinary: Vec::new(),
                    selected_password: None,
                };
                self.archive_runtime
                    .get_mut(&job_id)
                    .expect("admitted runtime")
                    .consumed
                    .extend(work.input_names.iter().cloned());
                self.spawn_scoped_live_extraction(job_id, work, lease, input)
                    .await?;
                if let Some(parent) = speculative {
                    let runtime = self.archive_runtime.get_mut(&job_id).expect("runtime");
                    runtime.mark_namespace_speculative(set_name, parent);
                }
            }
        }
        Box::pin(self.resume_scoped_replay(job_id)).await?;
        for (node, _, _, verified) in producers {
            if verified && !(speculative.is_some() && node == 0) {
                self.close_scoped_graph_owner(job_id, set_name, node)
                    .await?;
            }
        }
        self.drain_scoped_repairs(job_id).await
    }

    pub(crate) async fn drain_scoped_repairs(&mut self, job_id: JobId) -> Result<(), String> {
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return Ok(());
        };
        let deferred = std::mem::take(&mut runtime.deferred_repairs);
        for (owner, work) in deferred {
            if !self.virtual_repair_ready(job_id, &work) {
                self.archive_runtime
                    .get_mut(&job_id)
                    .expect("admitted runtime")
                    .deferred_repairs
                    .push((owner, work));
                continue;
            }
            if !Box::pin(self.repair_scoped_failure_with_inventory(
                job_id,
                owner,
                work,
                Default::default(),
            ))
            .await?
            {
                self.archive_runtime
                    .get_mut(&job_id)
                    .expect("admitted runtime")
                    .fail(owner);
                return Err("nested archive recovery could not change the failed input".into());
            }
        }
        Ok(())
    }

    pub(super) async fn rearm_direct_graph(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) -> Result<(), String> {
        use std::io::{Read, Seek, SeekFrom};
        trait Input: Read + Seek {}
        impl<T: Read + Seek> Input for T {}
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Ok(());
        };
        let plans = set.graph_rearm_plans();
        if plans.is_empty() {
            return Ok(());
        }
        let staging = set.plan().destination_dir.clone();
        let budget = self.extraction_budget(job_id, &staging)?;
        let root = Arc::new(crate::pipeline::extraction::ExtractionRoot::open(&staging)?);
        for plan in plans {
            let memory = budget
                .try_reserve_memory(64 * 1024)
                .ok_or("nested gate rearm memory admission unavailable")?;
            let task = budget
                .task_permit_for_root(Arc::clone(&root))
                .map_err(|error| error.to_string())?;
            let (node, generation) = (plan.node, plan.generation);
            let (root, staging, budget) = (Arc::clone(&root), staging.clone(), Arc::clone(&budget));
            let (runs, checksums) = tokio::task::spawn_blocking(move || -> std::io::Result<_> {
                let (_memory, _task) = (memory, task);
                let mut buffer = vec![0; 64 * 1024];
                let mut checksums = Vec::with_capacity(plan.runs.len());
                for run in &plan.runs {
                    let mut input: Box<dyn Input> = if let Some(image) = plan.nested.get(&run.member_id) {
                        Box::new(crate::pipeline::direct_store::provider::VirtualVolumeReader::<true>::new(image.clone(), Arc::default()))
                    } else {
                        let path = plan.physical.get(&run.member_id).ok_or_else(|| std::io::Error::other("rearm member backing absent"))?;
                        let relative = path.strip_prefix(&staging).map_err(std::io::Error::other)?;
                        Box::new(root.open_input(relative)?.into_std())
                    };
                    input.seek(SeekFrom::Start(run.logical_offset))?;
                    let mut hash = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
                    let mut remaining = run.len;
                    while remaining != 0 {
                        budget.check_active_io()?;
                        let count = remaining.min(buffer.len() as u64) as usize;
                        input.read_exact(&mut buffer[..count])?;
                        hash.update(&buffer[..count]);
                        remaining -= count as u64;
                    }
                    checksums.push(hash.finalize() as u32);
                }
                Ok((plan.runs, checksums))
            }).await.map_err(|error| error.to_string())?.map_err(|error| error.to_string())?;
            self.direct_store
                .set_mut(job_id, set_index)
                .ok_or("direct root vanished during gate rearm")?
                .apply_graph_rearm(node, generation, &runs, checksums)
                .map_err(|error| format!("nested gate rearm rejected: {error:?}"))?;
        }
        Ok(())
    }

    pub(super) async fn promote_direct_archive_graph(
        &mut self,
        job_id: JobId,
    ) -> Result<(), String> {
        let sets = self.direct_store.sets_for(job_id);
        let live: Vec<_> = sets
            .iter()
            .enumerate()
            .filter(|(_, set)| !set.is_finalized() && !set.is_demoted())
            .collect();
        if live.is_empty() {
            return Ok(());
        }
        if live
            .iter()
            .all(|(_, set)| set.has_nested_checkpoint_authority())
        {
            if live
                .iter()
                .any(|(_, set)| !set.nested_replay_members().is_empty())
            {
                self.replay_direct_archive_prefixes(job_id).await?;
                self.commit_archive_graph(job_id).await?;
            }
            return Ok(());
        }
        if !live
            .iter()
            .any(|(_, set)| set.has_nested_checkpoint_authority())
            && !live.iter().any(|(_, set)| {
                !crate::pipeline::direct_store::nested::catalog::archive_candidates(&set.router)
                    .is_empty()
            })
        {
            return Ok(());
        }
        // Before a header has supplied immutable layout facts, the ordinary
        // source path continues. Its already-placed prefix is replayed below.
        if live
            .iter()
            .any(|(_, set)| set.router.checkpoint_facts().is_empty())
        {
            return Ok(());
        }
        let all_indices: Vec<_> = live.iter().map(|(index, _)| *index).collect();
        for index in all_indices {
            self.await_direct_placement_io(job_id, index).await;
        }
        self.commit_archive_graph(job_id).await?;
        self.commit_archive_graph(job_id).await
    }

    pub(crate) async fn replay_direct_archive_prefixes(
        &mut self,
        job_id: JobId,
    ) -> Result<(), String> {
        let indices: Vec<_> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| {
                set.has_nested_checkpoint_authority()
                    && !set.is_demoted()
                    && !set.is_finalized()
                    && !set.is_delegated()
                    && !set.nested_replay_members().is_empty()
            })
            .map(|(index, _)| index)
            .collect();
        for index in indices {
            self.await_direct_placement_io(job_id, index).await;
            let staging = self
                .direct_store
                .set(job_id, index)
                .ok_or("direct root vanished during replay")?
                .plan()
                .destination_dir
                .clone();
            let budget = self.extraction_budget(job_id, &staging)?;
            let _memory = budget
                .try_reserve_memory(64 * 1024)
                .ok_or("nested replay memory admission unavailable")?;
            let members = self
                .direct_store
                .set(job_id, index)
                .ok_or("direct root vanished during graph replay")?
                .nested_replay_members();
            for (member, path, ranges) in members {
                let parent = path
                    .parent()
                    .ok_or("replay member has no parent")?
                    .to_path_buf();
                let name = path
                    .file_name()
                    .ok_or("replay member has no name")?
                    .to_owned();
                let file = tokio::task::spawn_blocking(move || {
                    crate::pipeline::extraction::ExtractionRoot::open(&parent)
                        .map_err(std::io::Error::other)?
                        .open_input(std::path::Path::new(&name))
                        .map(|file| file.into_std())
                })
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
                let mut file = tokio::fs::File::from_std(file);
                for (start, end) in ranges {
                    file.seek(std::io::SeekFrom::Start(start))
                        .await
                        .map_err(|error| error.to_string())?;
                    let mut offset = start;
                    while offset < end {
                        budget
                            .check_active_io()
                            .map_err(|error| error.to_string())?;
                        let size = (end - offset).min(64 * 1024) as usize;
                        let mut bytes = vec![0; size];
                        file.read_exact(&mut bytes)
                            .await
                            .map_err(|error| error.to_string())?;
                        let spans = self
                            .direct_store
                            .set_mut(job_id, index)
                            .ok_or("direct root vanished during graph replay")?
                            .forward_existing_member(member, offset, bytes.into())
                            .map_err(|error| format!("nested replay refused: {error:?}"))?;
                        self.try_place_direct_spans(job_id, index, &spans)
                            .await
                            .map_err(|error| {
                                format!("nested replay placement failed: {error:?}")
                            })?;
                        offset += size as u64;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn settle_direct_nested_materialization(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) -> Result<(), String> {
        self.await_direct_placement_io(job_id, set_index).await;
        loop {
            let Some(set) = self.direct_store.set(job_id, set_index) else {
                return Ok(());
            };
            let Some(plan) = set.prepare_nested_materialization() else {
                return Ok(());
            };
            let staging = set.plan().destination_dir.clone();
            let need = plan
                .writes()
                .try_fold(0u64, |total, (_, write)| total.checked_add(write.len))
                .ok_or("nested materialization byte count overflow")?;
            let budget = self.extraction_budget(job_id, &staging)?;
            if need > budget.job_limit_bytes() {
                return Err("nested materialization exceeds job disk admission".into());
            }
            let memory = budget
                .try_reserve_memory(64 * 1024)
                .ok_or("nested materialization memory admission unavailable")?;
            let root = Arc::new(crate::pipeline::extraction::ExtractionRoot::open(&staging)?);
            let task = budget
                .task_permit_for_root(Arc::clone(&root))
                .map_err(|error| error.to_string())?;
            let sparse = self.direct_store.sparse_marking();
            let reserve = self.direct_store.settings().holds_disk_reserve_bytes;
            let copied = tokio::task::spawn_blocking(move || {
                let (_memory, _task) = (memory, task);
                let space = crate::operations::disk::probe_disk_space(&staging)
                    .map_err(std::io::Error::other)?;
                if need > space.available_bytes.saturating_sub(reserve) {
                    return Err(std::io::Error::other(
                        "nested materialization disk admission unavailable",
                    ));
                }
                crate::pipeline::direct_store::nested::materialize::copy_inputs_to_checked(
                    plan,
                    root,
                    staging,
                    sparse,
                    || budget.check_active_io(),
                )
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            self.direct_store
                .set_mut(job_id, set_index)
                .ok_or("direct root vanished during materialization")?
                .apply_nested_materialization(copied)
                .map_err(|error| format!("nested materialization generation refused: {error:?}"))?;
            // Old descendant backings remain until a later retirement pass.
            self.commit_archive_graph(job_id).await?;
        }
    }
}
