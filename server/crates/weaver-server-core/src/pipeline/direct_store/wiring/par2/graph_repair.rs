use super::*;
use crate::pipeline::extraction::runtime::{ArchiveRuntime, NamespaceWork, VirtualInputSource};
use crate::pipeline::extraction::session::InputVersion;
use std::collections::BTreeMap;

impl Pipeline {
    pub(super) async fn repair_graph_roots(
        &mut self,
        job_id: JobId,
        repairs: &[PreparedDirectRepair],
        par2: &par2_rs::Par2FileSet,
        overlay: &DirectPar2Overlay,
    ) -> Result<(), super::super::super::repair::DirectRepairFailure> {
        use super::super::super::repair::DirectRepairFailure;
        self.repair_graph_roots_inner(job_id, repairs, par2, overlay)
            .await
            .map_err(DirectRepairFailure::ExecuteFailed)
    }

    async fn repair_graph_roots_inner(
        &mut self,
        job_id: JobId,
        repairs: &[PreparedDirectRepair],
        par2: &par2_rs::Par2FileSet,
        overlay: &DirectPar2Overlay,
    ) -> Result<(), String> {
        let state = self.jobs.get(&job_id).ok_or("repair job absent")?;
        let working = state.working_dir.clone();
        let destination = self.deterministic_extraction_staging_dir(job_id);
        let mut extra = BTreeMap::new();
        for file in state.assembly.files() {
            let name = self.current_filename_for_file(job_id, file);
            let path = working.join(&name);
            if path.is_file() {
                extra.insert(name, path);
            }
        }
        // Freeze every explicitly bound virtual donor. None is ever exposed
        // as a writable original to a native recovery engine.
        let views = overlay
            .virtual_volumes_for(&self.direct_store, job_id)
            .ok_or("repair virtual source inventory unavailable")?;
        let mut named = Vec::new();
        for image in views {
            let binding = overlay
                .volumes
                .iter()
                .find(|binding| binding.volume_index == image.volume_index)
                .ok_or("repair virtual source binding absent")?;
            let name = par2
                .file_description(&binding.par2_file_id)
                .ok_or("repair virtual source descriptor absent")?
                .filename
                .clone();
            named.push((name, image));
        }
        let base = working.join(crate::pipeline::direct_store::manifest::DIRECTORY);
        std::fs::create_dir_all(&base).map_err(|error| error.to_string())?;
        let private = tempfile::Builder::new()
            .prefix("root-repair-")
            .tempdir_in(&base)
            .map_err(|error| error.to_string())?
            .keep();
        let budget = self.extraction_budget(job_id, &private)?;
        let memory = budget
            .try_reserve_memory(256 * 1024)
            .ok_or("root repair memory admission unavailable")?;
        let root =
            std::sync::Arc::new(crate::pipeline::extraction::ExtractionRoot::open(&private)?);
        let task = budget
            .task_permit_for_root(root)
            .map_err(|error| error.to_string())?;
        let maximum = budget.job_limit_bytes();
        let space = crate::operations::disk::probe_disk_space(&private)
            .map_err(|error| error.to_string())?;
        let maximum = maximum.min(
            space
                .available_bytes
                .saturating_sub(self.direct_store.settings().holds_disk_reserve_bytes),
        );
        let names: Vec<_> = named.iter().map(|(name, _)| name.clone()).collect();
        let sparse = self.direct_store.sparse_marking();
        let copy_budget = std::sync::Arc::clone(&budget);
        let paths = tokio::task::spawn_blocking(move || {
            let (_memory, _task) = (memory, task);
            crate::pipeline::direct_store::repair::graph::materialize_candidates_checked(
                &private.join("inputs"),
                named,
                maximum,
                sparse,
                || copy_budget.check_active_io(),
            )
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        let candidates: BTreeMap<_, _> = names.into_iter().zip(paths).collect();
        extra.extend(candidates.clone());
        for repair in repairs {
            let set = self
                .direct_store
                .set(job_id, repair.set_index)
                .ok_or("repair root absent")?;
            let plan = set.plan().clone();
            let mut input_names = Vec::new();
            let mut volumes = Vec::new();
            for (volume, file_index) in &plan.volumes {
                let binding = overlay
                    .volumes
                    .iter()
                    .find(|binding| binding.volume_index == *file_index)
                    .ok_or("repair root volume binding absent")?;
                input_names.push(
                    par2.file_description(&binding.par2_file_id)
                        .ok_or("repair root descriptor absent")?
                        .filename
                        .clone(),
                );
                volumes.push(*volume);
            }
            let inputs = input_names
                .iter()
                .map(|name| {
                    candidates
                        .get(name)
                        .cloned()
                        .ok_or("repair root candidate absent")
                })
                .collect::<Result<Vec<_>, _>>()?;
            let runtime = self
                .archive_runtime
                .entry(job_id)
                .or_insert_with(ArchiveRuntime::new);
            runtime.set_roots(working.clone(), destination.clone());
            let lease = runtime
                .graph_lease(&plan.set_name, 0, None, InputVersion(0))
                .map_err(|error| format!("repair root session: {error:?}"))?;
            let work = NamespaceWork {
                kind: plan.format.archive_type(),
                set_name: plan.set_name.clone(),
                volumes,
                output: working.join("unused-repair-output"),
                inputs,
                original_inputs: Vec::new(),
                input_names,
                virtual_source: None,
                original_virtual_source: Some(VirtualInputSource {
                    root_set: plan.set_name.clone(),
                    node: 0,
                }),
                destination: destination.clone(),
                outputs: BTreeMap::new(),
                root_set: Some(plan.set_name.clone()),
                ordinary: Vec::new(),
                selected_password: None,
            };
            if !self
                .repair_scoped_failure_with_inventory(job_id, lease.owner, work, extra.clone())
                .await?
            {
                return Err("immutable root repair made no verified progress".into());
            }
            let set = self
                .direct_store
                .set_mut(job_id, repair.set_index)
                .ok_or("repair root disappeared")?;
            set.note_repair_attempted();
            set.delegate_effective_extraction();
        }
        Ok(())
    }
}
