use super::*;
use crate::pipeline::extraction::runtime::decoded::NamespaceProgress;
use crate::pipeline::extraction::runtime::rearm::RearmRead;
use crate::pipeline::extraction::runtime::{
    ArchiveRuntime, NamespaceWork, RuntimeError, VirtualInputSource, limits,
};
use crate::pipeline::extraction::session::InputVersion;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

fn placement_error(step: &'static str) -> impl FnOnce(DirectPlacementError) -> RuntimeError {
    move |error| match error {
        DirectPlacementError::Sparse { error, .. } | DirectPlacementError::Write(error) => {
            RuntimeError::io(step)(error)
        }
    }
}

fn open_staging_root(
    staging: &std::path::Path,
) -> Result<Arc<crate::pipeline::extraction::ExtractionRoot>, RuntimeError> {
    crate::pipeline::extraction::ExtractionRoot::open(staging)
        .map(Arc::new)
        .map_err(RuntimeError::refused("staging root"))
}

// The work a live child archive runs, read through its parent's gate.
fn child_work(
    set_name: &str,
    child: crate::pipeline::direct_store::set::LiveChildDescription,
    destination: PathBuf,
) -> NamespaceWork {
    let source = VirtualInputSource {
        root_set: set_name.to_owned(),
        node: child.node,
    };
    NamespaceWork {
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
        destination,
        outputs: Default::default(),
        root_set: None,
        ordinary: Vec::new(),
        selected_password: None,
    }
}

impl Pipeline {
    // Reports the failure as the sentence the job fails with.
    pub(crate) async fn refresh_direct_live_inputs(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), String> {
        self.refresh_live_inputs(job_id, set_name)
            .await
            .map_err(String::from)
    }

    async fn refresh_live_inputs(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), RuntimeError> {
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
            .ok_or(RuntimeError::Invariant("direct job"))?
            .working_dir
            .clone();
        self.publish_scoped_namespace(
            job_id,
            set_name,
            NamespaceProgress {
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
        progress: NamespaceProgress,
        working: PathBuf,
        destination: PathBuf,
        speculative: Option<crate::pipeline::extraction::output::channel::StreamOwner>,
    ) -> Result<(), RuntimeError> {
        let NamespaceProgress {
            producers,
            children,
        } = progress;
        let runtime = self
            .archive_runtime
            .entry(job_id)
            .or_insert_with(ArchiveRuntime::new);
        runtime.set_roots(working, destination.clone());
        runtime
            .bind_namespace(set_name, &producers, &children)
            .await?;
        for child in children {
            self.open_child_gate(job_id, set_name, child, &destination, speculative)
                .await?;
        }
        Box::pin(self.resume_scoped_replay(job_id)).await?;
        for (node, _, _, verified) in producers {
            if verified && !(speculative.is_some() && node == 0) {
                self.close_scoped_graph_owner(job_id, set_name, node)
                    .await?;
            }
        }
        self.drain_repairs(job_id).await
    }

    // Publishes the child's parts to its gate, and starts its extraction the
    // first time the gate opens.
    async fn open_child_gate(
        &mut self,
        job_id: JobId,
        set_name: &str,
        mut child: crate::pipeline::direct_store::set::LiveChildDescription,
        destination: &std::path::Path,
        speculative: Option<crate::pipeline::extraction::output::channel::StreamOwner>,
    ) -> Result<(), RuntimeError> {
        let runtime = self
            .archive_runtime
            .get_mut(&job_id)
            .ok_or(RuntimeError::Invariant("admitted archive runtime"))?;
        if runtime.graph_source_replaced(set_name, child.node) {
            return Ok(());
        }
        let key = (set_name.to_owned(), child.node);
        let fresh = !runtime.gates.contains_key(&key);
        let input = runtime.gates.entry(key).or_default().clone();
        input
            .publish(
                std::mem::take(&mut child.parts),
                child.roster_closed,
                &self.process_memory_budget,
            )
            .map_err(RuntimeError::refused("child gate publication"))?;
        if !fresh || runtime.has_replay_source(set_name, child.node) {
            return Ok(());
        }
        let lease = runtime
            .graph_lease(set_name, child.node, Some(child.parent), InputVersion(0))
            .map_err(RuntimeError::session("direct child admission"))?;
        let work = child_work(set_name, child, destination.to_owned());
        runtime.consumed.extend(work.input_names.iter().cloned());
        self.spawn_scoped_live_extraction(job_id, work, lease, input)
            .await
            .map_err(RuntimeError::refused("child extraction spawn"))?;
        if let Some(parent) = speculative
            && let Some(runtime) = self.archive_runtime.get_mut(&job_id)
        {
            runtime.mark_namespace_speculative(set_name, parent);
        }
        Ok(())
    }

    // Reports the failure as the sentence the job fails with.
    pub(crate) async fn drain_scoped_repairs(&mut self, job_id: JobId) -> Result<(), String> {
        self.drain_repairs(job_id).await.map_err(String::from)
    }

    async fn drain_repairs(&mut self, job_id: JobId) -> Result<(), RuntimeError> {
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return Ok(());
        };
        let deferred = std::mem::take(&mut runtime.deferred_repairs);
        for (owner, work) in deferred {
            if !self.virtual_repair_ready(job_id, &work) {
                self.archive_runtime
                    .get_mut(&job_id)
                    .ok_or(RuntimeError::Invariant("admitted archive runtime"))?
                    .deferred_repairs
                    .push((owner, work));
                continue;
            }
            let changed = Box::pin(self.repair_scoped_failure_with_inventory(
                job_id,
                owner,
                work,
                Default::default(),
            ))
            .await
            .map_err(RuntimeError::refused("nested archive recovery"))?;
            if !changed {
                if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
                    runtime.fail(owner);
                }
                return Err(RuntimeError::Invariant(
                    "nested archive recovery could not change the failed input",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn rearm_direct_graph(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) -> Result<(), RuntimeError> {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Ok(());
        };
        let plans = set.graph_rearm_plans();
        if plans.is_empty() {
            return Ok(());
        }
        let staging = set.plan().destination_dir.clone();
        let budget = self.extraction_budget(job_id, &staging)?;
        let root = open_staging_root(&staging)?;
        for plan in plans {
            let (runs, checksums) = RearmRead {
                runs: plan.runs,
                physical: plan.physical,
                nested: plan.nested,
                root: Arc::clone(&root),
                base: staging.clone(),
                budget: Arc::clone(&budget),
            }
            .checksum()
            .await?;
            self.direct_store
                .set_mut(job_id, set_index)
                .ok_or(RuntimeError::Invariant("direct root during gate rearm"))?
                .apply_graph_rearm(plan.node, plan.generation, &runs, checksums)
                .map_err(RuntimeError::routing("nested gate rearm"))?;
        }
        Ok(())
    }

    pub(super) async fn promote_direct_archive_graph(
        &mut self,
        job_id: JobId,
    ) -> Result<(), RuntimeError> {
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
                self.replay_archive_prefixes(job_id).await?;
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

    // Reports the failure as the sentence the job fails with.
    pub(crate) async fn replay_direct_archive_prefixes(
        &mut self,
        job_id: JobId,
    ) -> Result<(), String> {
        self.replay_archive_prefixes(job_id)
            .await
            .map_err(String::from)
    }

    pub(crate) async fn replay_archive_prefixes(
        &mut self,
        job_id: JobId,
    ) -> Result<(), RuntimeError> {
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
            self.replay_set_prefix(job_id, index).await?;
        }
        Ok(())
    }

    async fn replay_set_prefix(&mut self, job_id: JobId, index: usize) -> Result<(), RuntimeError> {
        const VANISHED: RuntimeError = RuntimeError::Invariant("direct root during graph replay");
        self.await_direct_placement_io(job_id, index).await;
        let set = self.direct_store.set(job_id, index).ok_or(VANISHED)?;
        let staging = set.plan().destination_dir.clone();
        let members = set.nested_replay_members();
        let budget = self.extraction_budget(job_id, &staging)?;
        let _memory = budget
            .try_reserve_memory(limits::MATERIALIZATION_PERMIT_BYTES)
            .ok_or(RuntimeError::Admission("nested replay"))?;
        for (member, path, ranges) in members {
            let mut file = open_replay_member(path).await?;
            for (start, end) in ranges {
                file.seek(std::io::SeekFrom::Start(start))
                    .await
                    .map_err(RuntimeError::io("nested replay seek"))?;
                let mut offset = start;
                while offset < end {
                    budget
                        .check_active_io()
                        .map_err(RuntimeError::io("nested replay"))?;
                    let size = (end - offset).min(limits::MATERIALIZATION_PERMIT_BYTES) as usize;
                    let mut bytes = vec![0; size];
                    file.read_exact(&mut bytes)
                        .await
                        .map_err(RuntimeError::io("nested replay read"))?;
                    let spans = self
                        .direct_store
                        .set_mut(job_id, index)
                        .ok_or(VANISHED)?
                        .forward_existing_member(member, offset, bytes.into())
                        .map_err(RuntimeError::routing("nested replay"))?;
                    self.try_place_direct_spans(job_id, index, &spans)
                        .await
                        .map_err(placement_error("nested replay placement"))?;
                    offset += size as u64;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn settle_direct_nested_materialization(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) -> Result<(), RuntimeError> {
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
                .ok_or(RuntimeError::Invariant(
                    "nested materialization byte count overflow",
                ))?;
            let budget = self.extraction_budget(job_id, &staging)?;
            if need > budget.job_limit_bytes() {
                return Err(RuntimeError::Admission("nested materialization disk"));
            }
            let memory = budget
                .try_reserve_memory(limits::MATERIALIZATION_PERMIT_BYTES)
                .ok_or(RuntimeError::Admission("nested materialization"))?;
            let root = open_staging_root(&staging)?;
            let task = budget.task_permit_for_root(Arc::clone(&root))?;
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
            .map_err(RuntimeError::join("nested materialization"))?
            .map_err(RuntimeError::io("nested materialization"))?;
            self.direct_store
                .set_mut(job_id, set_index)
                .ok_or(RuntimeError::Invariant(
                    "direct root during materialization",
                ))?
                .apply_nested_materialization(copied)
                .map_err(RuntimeError::routing("nested materialization generation"))?;
            // Old descendant backings remain until a later retirement pass.
            self.commit_archive_graph(job_id).await?;
        }
    }
}

// Opens a replay member through its own pinned parent directory.
async fn open_replay_member(path: PathBuf) -> Result<tokio::fs::File, RuntimeError> {
    let parent = path
        .parent()
        .ok_or(RuntimeError::Invariant("replay member has no parent"))?
        .to_path_buf();
    let name = path
        .file_name()
        .ok_or(RuntimeError::Invariant("replay member has no name"))?
        .to_owned();
    let file = tokio::task::spawn_blocking(move || {
        crate::pipeline::extraction::ExtractionRoot::open(&parent)
            .map_err(std::io::Error::other)?
            .open_input(std::path::Path::new(&name))
            .map(|file| file.into_std())
    })
    .await
    .map_err(RuntimeError::join("nested replay open"))?
    .map_err(RuntimeError::io("nested replay open"))?;
    Ok(tokio::fs::File::from_std(file))
}
