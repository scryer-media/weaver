use super::*;
use crate::pipeline::extraction::runtime::repair::{
    NamespaceInput, NamespaceRepairRequest, RepairCancellation, RepairFormat,
};
use crate::pipeline::extraction::{JobExtractionBudget, ProcessMemoryBudget};
use std::io::{Read, Write};

fn fingerprint(
    path: &Path,
    budget: &JobExtractionBudget,
) -> Result<(u64, blake3::Hash), RuntimeError> {
    let parent = path
        .parent()
        .ok_or(RuntimeError::RecoveryRefused("input has no parent"))?;
    let name = path
        .file_name()
        .ok_or(RuntimeError::RecoveryRefused("input has no name"))?;
    let mut file = ExtractionRoot::open(parent)
        .map_err(RuntimeError::refused("recovery input root"))?
        .open_input(Path::new(name))
        .map_err(RuntimeError::io("recovery input open"))?;
    let mut hash = blake3::Hasher::new();
    let mut length = 0u64;
    let mut buffer = [0; 64 * 1024];
    loop {
        budget
            .check_active_io()
            .map_err(|error| ExtractionError::io(&error))?;
        let count = file
            .read(&mut buffer)
            .map_err(RuntimeError::io("recovery input read"))?;
        if count == 0 {
            break;
        }
        length += count as u64;
        hash.update(&buffer[..count]);
    }
    Ok((length, hash.finalize()))
}

fn recover_inventory(
    owner: StreamOwner,
    inventory: BTreeMap<String, PathBuf>,
    private: PathBuf,
    process: Arc<ProcessMemoryBudget>,
    budget: Arc<JobExtractionBudget>,
    cancellation: RepairCancellation,
    disk_reserve: u64,
) -> Result<BTreeMap<String, PathBuf>, RuntimeError> {
    if inventory.len() > weaver_nzb::parser::MAX_NZB_FILES {
        return Err(RuntimeError::RecoveryRefused(
            "inventory exceeds file admission",
        ));
    }
    let mut aliases = BTreeMap::new();
    let mut lengths = BTreeMap::new();
    let mut bytes = 0u64;
    // Validate the complete namespace and copy allowance before creating any
    // source image. No native engine receives a writable original backing.
    for (name, path) in &inventory {
        crate::pipeline::repair::par3::paths::check_install_path(name)
            .map_err(RuntimeError::refused("recovery inventory path"))?;
        let relative = ExtractionRoot::parse_relative_path(name)
            .map_err(RuntimeError::refused("recovery inventory path"))?;
        let alias = name.to_lowercase();
        if relative != Path::new(name) || aliases.insert(alias, name).is_some() {
            return Err(RuntimeError::RecoveryRefused("inventory paths alias"));
        }
        let metadata = std::fs::symlink_metadata(path)
            .map_err(RuntimeError::io("recovery inventory metadata"))?;
        if !metadata.file_type().is_file() {
            return Err(RuntimeError::RecoveryRefused(
                "input is not an immutable regular file",
            ));
        }
        lengths.insert(name.clone(), metadata.len());
        bytes = bytes
            .checked_add(metadata.len())
            .filter(|bytes| *bytes <= budget.job_limit_bytes())
            .ok_or(RuntimeError::RecoveryRefused(
                "inventory exceeds disk admission",
            ))?;
    }
    for alias in aliases.keys() {
        if alias
            .match_indices('/')
            .any(|(index, _)| aliases.contains_key(&alias[..index]))
        {
            return Err(RuntimeError::RecoveryRefused(
                "inventory file collides with directory",
            ));
        }
    }
    let space = crate::operations::disk::probe_disk_space(&private)
        .map_err(RuntimeError::refused("recovery disk probe"))?;
    if bytes > space.available_bytes.saturating_sub(disk_reserve) {
        return Err(RuntimeError::RecoveryRefused(
            "inventory disk admission unavailable",
        ));
    }
    let source = private.join("sources");
    std::fs::create_dir(&source).map_err(RuntimeError::io("recovery source directory"))?;
    let mut effective = BTreeMap::new();
    for (name, path) in inventory {
        let destination = source.join(&name);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)
                .map_err(RuntimeError::io("recovery source directory"))?;
        }
        let parent = path
            .parent()
            .ok_or(RuntimeError::RecoveryRefused("source has no parent"))?;
        let file_name = path
            .file_name()
            .ok_or(RuntimeError::RecoveryRefused("source has no name"))?;
        let mut input = ExtractionRoot::open(parent)
            .map_err(RuntimeError::refused("recovery source root"))?
            .open_input(Path::new(file_name))
            .map_err(RuntimeError::io("recovery source open"))?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .map_err(RuntimeError::io("recovery source copy"))?;
        let mut buffer = [0; 64 * 1024];
        let mut remaining = lengths[&name];
        while remaining != 0 {
            budget
                .check_active_io()
                .map_err(|error| ExtractionError::io(&error))?;
            let count = remaining.min(buffer.len() as u64) as usize;
            input
                .read_exact(&mut buffer[..count])
                .map_err(RuntimeError::io("recovery source copy"))?;
            output
                .write_all(&buffer[..count])
                .map_err(RuntimeError::io("recovery source copy"))?;
            remaining -= count as u64;
        }
        if input
            .read(&mut buffer[..1])
            .map_err(RuntimeError::io("recovery source copy"))?
            != 0
        {
            return Err(RuntimeError::RecoveryRefused(
                "input length changed during immutable copy",
            ));
        }
        output
            .flush()
            .map_err(RuntimeError::io("recovery source copy"))?;
        effective.insert(name, destination);
    }
    let mut authenticated = BTreeMap::new();
    for round in 0..weaver_nzb::parser::MAX_NZB_FILES {
        let mut changed = false;
        for (format_index, format) in [RepairFormat::Par2, RepairFormat::Par3]
            .into_iter()
            .enumerate()
        {
            budget
                .check_active_io()
                .map_err(RuntimeError::io("namespace recovery"))?;
            let output = private.join(format!("pass-{round}-{format_index}"));
            std::fs::create_dir(&output).map_err(RuntimeError::io("recovery pass directory"))?;
            let space = crate::operations::disk::probe_disk_space(&private)
                .map_err(RuntimeError::refused("recovery disk probe"))?;
            let limit = budget
                .job_limit_bytes()
                .min(space.available_bytes.saturating_sub(disk_reserve) / 3);
            let native_memory = budget
                .max_memory_bytes()
                .saturating_sub(1024 * 1024)
                .min(64 * 1024 * 1024);
            let memory = budget
                .reserve_memory_wait(native_memory)
                .map_err(ExtractionError::from)?;
            let inputs = effective
                .iter()
                .map(|(name, path)| {
                    let length = std::fs::metadata(path)
                        .map_err(RuntimeError::io("recovery output metadata"))?
                        .len();
                    Ok(NamespaceInput {
                        name: name.clone(),
                        path: path.clone(),
                        ranges: std::iter::once(0..length).collect(),
                    })
                })
                .collect::<Result<_, RuntimeError>>()?;
            let outcome = NamespaceRepairRequest {
                owner,
                format,
                inputs,
                output: output.clone(),
                donors: private.join(format!("donors-{round}-{format_index}")),
                output_limit: limit,
                process: Arc::clone(&process),
                cancellation: cancellation.clone(),
                memory,
            }
            .execute()?;
            for member in outcome.verified {
                let name = member
                    .path
                    .to_str()
                    .ok_or(RuntimeError::RecoveryRefused("output path encoding"))?
                    .to_owned();
                let path = output.join(&member.path);
                let identity = fingerprint(&path, &budget)?;
                if identity.0 != member.length {
                    return Err(RuntimeError::RecoveryRefused("output length changed"));
                }
                if let Some(previous) = authenticated.insert(name.clone(), identity)
                    && previous != identity
                {
                    return Err(RuntimeError::RecoveryRefused(
                        "authenticated recovery sets disagree about namespace output",
                    ));
                }
                let same = effective
                    .get(&name)
                    .map(|previous| fingerprint(previous, &budget))
                    .transpose()?
                    .is_some_and(|previous| previous == identity);
                if !same {
                    effective.insert(name, path);
                    changed = true;
                }
            }
            if effective.len() > weaver_nzb::parser::MAX_NZB_FILES {
                return Err(RuntimeError::RecoveryRefused(
                    "recovered namespace exceeds file admission",
                ));
            }
        }
        if !changed {
            return Ok(effective);
        }
    }
    Err(RuntimeError::RecoveryRefused(
        "recovery exceeded progress admission",
    ))
}

impl Pipeline {
    fn virtual_repair_inventory(
        &self,
        job_id: JobId,
        source: &crate::pipeline::extraction::runtime::VirtualInputSource,
    ) -> Option<crate::pipeline::direct_store::nested::RepairInventory> {
        let runtime = self.archive_runtime.get(&job_id)?;
        let (graph, root): (
            _,
            &dyn crate::pipeline::direct_store::nested::catalog::RoutingNamespace,
        ) = if let Some(set) = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .find(|set| set.set_name() == source.root_set)
        {
            (set.nested_graph()?, &set.router)
        } else {
            let state = runtime.decoded.iter().find(|state| {
                let owner = state.generation.owner();
                source.root_set == decoded::namespace_name(owner)
            })?;
            let parent = state
                .graph
                .nodes
                .get(source.node.checked_sub(1)? as usize)?
                .parent;
            let owner = runtime
                .graph_owners
                .get(&(source.root_set.clone(), parent))?;
            if !runtime.sessions.ancestry_verified(*owner) {
                return None;
            }
            let mut inventory = state
                .graph
                .repair_inventory(&state.namespace(), source.node)?;
            if parent == 0 {
                // Root ordinary output uses FileOutput. Its producer's
                // explicit completion roster supplies recovery siblings.
                let producer = state.generation.owner();
                if let Some(work) = runtime
                    .completed
                    .iter()
                    .map(|(owner, work, _)| (owner, work))
                    .chain(
                        runtime
                            .retained_work
                            .iter()
                            .map(|(owner, work)| (owner, work)),
                    )
                    .find_map(|(owner, work)| (*owner == producer).then_some(work))
                    && !work.ordinary.is_empty()
                {
                    let root = ExtractionRoot::open(&work.destination).ok()?;
                    for name in &work.ordinary {
                        let relative = ExtractionRoot::parse_relative_path(name).ok()?;
                        if root.probe_directory(&relative).is_ok() {
                            continue;
                        }
                        let file = root.open_input(&relative).ok()?;
                        if !file.metadata().ok()?.is_file() {
                            return None;
                        }
                        if inventory
                            .siblings
                            .insert(name.clone(), work.destination.join(relative))
                            .is_some()
                        {
                            return None;
                        }
                    }
                }
            }
            return Some(inventory);
        };
        let parent = graph
            .nodes
            .get(source.node.checked_sub(1)? as usize)?
            .parent;
        let owner = runtime
            .graph_owners
            .get(&(source.root_set.clone(), parent))?;
        if !runtime.sessions.ancestry_verified(*owner) {
            return None;
        }
        graph.repair_inventory(root, source.node)
    }

    pub(crate) fn virtual_repair_ready(&self, job_id: JobId, work: &NamespaceWork) -> bool {
        work.virtual_source
            .as_ref()
            .is_none_or(|source| self.virtual_repair_inventory(job_id, source).is_some())
    }

    #[cfg(test)]
    pub(crate) fn virtual_repair_inventory_for_test(
        &self,
        job_id: JobId,
        source: &crate::pipeline::extraction::runtime::VirtualInputSource,
    ) -> Option<crate::pipeline::direct_store::nested::RepairInventory> {
        self.virtual_repair_inventory(job_id, source)
    }

    pub(crate) async fn repair_scoped_failure_with_inventory(
        &mut self,
        job_id: JobId,
        owner: StreamOwner,
        mut work: NamespaceWork,
        mut extra: BTreeMap<String, PathBuf>,
    ) -> Result<bool, RuntimeError> {
        if let Some(source) = &work.virtual_source {
            let inventory = self.virtual_repair_inventory(job_id, source).ok_or(
                RuntimeError::RecoveryRefused(
                    "virtual repair requires a verified containing namespace and closed inputs",
                ),
            )?;
            extra.extend(inventory.siblings);
            work.volumes = inventory.inputs.keys().copied().collect();
            work.input_names = inventory
                .inputs
                .values()
                .map(|(name, _)| name.clone())
                .collect();
            let images: Vec<_> = inventory.inputs.into_values().collect();
            let base = self
                .jobs
                .get(&job_id)
                .ok_or(RuntimeError::Invariant("repair job"))?
                .working_dir
                .join(crate::pipeline::direct_store::manifest::DIRECTORY);
            let private = tempfile::Builder::new()
                .prefix("virtual-repair-")
                .tempdir_in(&base)
                .map_err(RuntimeError::io("virtual repair directory"))?
                .keep();
            let budget = self.extraction_budget(job_id, &private)?;
            let memory = budget
                .try_reserve_memory(256 * 1024)
                .ok_or(RuntimeError::Admission("virtual repair"))?;
            let root = Arc::new(
                ExtractionRoot::open(&private)
                    .map_err(RuntimeError::refused("virtual repair root"))?,
            );
            let task = budget.task_permit_for_root(root)?;
            let maximum = budget.job_limit_bytes();
            let sparse = self.direct_store.sparse_marking();
            let reserve = self.direct_store.settings().holds_disk_reserve_bytes;
            let inputs = tokio::task::spawn_blocking(move || {
                let (_memory, _task) = (memory, task);
                let need = images
                    .iter()
                    .try_fold(0u64, |total, (_, image)| total.checked_add(image.len))
                    .ok_or_else(|| std::io::Error::other("virtual repair size overflow"))?;
                let space = crate::operations::disk::probe_disk_space(&private)
                    .map_err(std::io::Error::other)?;
                if need > space.available_bytes.saturating_sub(reserve) {
                    return Err(std::io::Error::other(
                        "virtual repair disk admission unavailable",
                    ));
                }
                crate::pipeline::direct_store::repair::graph::materialize_candidates_checked(
                    &private.join("inputs"),
                    images,
                    maximum,
                    sparse,
                    || budget.check_active_io(),
                )
            })
            .await
            .map_err(RuntimeError::join("virtual repair materialization"))?
            .map_err(RuntimeError::io("virtual repair materialization"))?;
            work.inputs = inputs;
        }
        let original = work.inputs.clone();
        let mut inventory = extra;
        // The inventory is the parent's explicit published namespace, never a
        // directory walk or a search through an ancestor's backing tree.
        if let Some(source) = &work.original_virtual_source {
            if let Some(original) = self.virtual_repair_inventory(job_id, source) {
                inventory.extend(original.siblings);
            }
        } else if let Some(runtime) = self.archive_runtime.get(&job_id) {
            let source = ExtractionRoot::open_existing(&work.destination)
                .map_err(RuntimeError::refused("recovery sibling root"))?;
            let producer = work
                .input_names
                .iter()
                .find_map(|name| runtime.output_owners.get(name))
                .copied();
            if let Some(producer) = producer {
                // Nested filesystem publication records its complete sibling
                // roster here; the job's root extraction roster omits these.
                for (name, owner) in &runtime.output_owners {
                    if *owner == producer && source.probe_directory(Path::new(name)).is_err() {
                        inventory.insert(name.clone(), work.destination.join(name));
                    }
                }
            } else if let Some(members) = self.extracted_members.get(&job_id) {
                for name in members {
                    if !runtime.output_owners.contains_key(name)
                        && source.probe_directory(Path::new(name)).is_err()
                    {
                        inventory.insert(name.clone(), work.destination.join(name));
                    }
                }
            }
        }
        for (name, path) in work.input_names.iter().zip(&work.inputs) {
            inventory.insert(name.clone(), path.clone());
        }
        inventory.retain(|_, path| path.is_file());
        self.archive_runtime
            .get_mut(&job_id)
            .ok_or(RuntimeError::Invariant("archive runtime"))?
            .begin_repair(owner)
            .await?;
        let parent = self
            .jobs
            .get(&job_id)
            .ok_or(RuntimeError::Invariant("archive job"))?
            .working_dir
            .join(crate::pipeline::direct_store::manifest::DIRECTORY);
        ExtractionRoot::open(&parent).map_err(RuntimeError::refused("recovery parent root"))?;
        let private = tempfile::Builder::new()
            .prefix("repair-")
            .tempdir_in(&parent)
            .map_err(RuntimeError::io("recovery directory"))?
            .keep();
        let budget = self.extraction_budget(job_id, &private)?;
        let memory = budget
            .try_reserve_memory(64 * 1024)
            .ok_or(RuntimeError::Admission("recovery inventory"))?;
        let root = Arc::new(
            ExtractionRoot::open(&private).map_err(RuntimeError::refused("recovery root"))?,
        );
        let task = budget.task_permit_for_root(root)?;
        let cancellation = RepairCancellation::default();
        let (watch_budget, watch_cancel) = (Arc::clone(&budget), cancellation.clone());
        let watch = tokio::spawn(async move {
            watch_budget.wait_until_stopped().await;
            watch_cancel.cancel();
        });
        let (process, reserve) = (
            Arc::clone(&self.process_memory_budget),
            self.direct_store.settings().holds_disk_reserve_bytes,
        );
        let worker_budget = Arc::clone(&budget);
        let effective = tokio::task::spawn_blocking(move || {
            let (_memory, _task) = (memory, task);
            recover_inventory(
                owner,
                inventory,
                private,
                process,
                worker_budget,
                cancellation,
                reserve,
            )
        })
        .await
        .map_err(RuntimeError::join("namespace recovery worker"));
        watch.abort();
        let effective = effective??;
        let mut selected: BTreeMap<String, (PathBuf, u32)> = work
            .input_names
            .iter()
            .zip(&work.inputs)
            .zip(&work.volumes)
            .map(|((name, path), volume)| {
                (
                    name.clone(),
                    (
                        effective.get(name).cloned().unwrap_or_else(|| path.clone()),
                        *volume,
                    ),
                )
            })
            .collect();
        for (name, path) in &effective {
            if selected.contains_key(name) {
                continue;
            }
            let role = weaver_model::files::FileRole::from_filename(name);
            if Pipeline::archive_type_for_role(&role) == Some(work.kind)
                && weaver_model::files::archive_base_name(name, &role).as_deref()
                    == Some(&work.set_name)
            {
                selected.insert(
                    name.clone(),
                    (path.clone(), Pipeline::archive_volume_number(&role)),
                );
            }
        }
        let mut selected: Vec<_> = selected.into_iter().collect();
        selected.sort_by_key(|(_, (_, volume))| *volume);
        let changed = if selected.len() != original.len() {
            true
        } else {
            let mut changed = false;
            for ((_, (path, _)), original) in selected.iter().zip(&original) {
                // Compare content under a fresh bounded admission; a different
                // private pathname alone does not justify another decode.
                let memory = budget
                    .try_reserve_memory(64 * 1024)
                    .ok_or(RuntimeError::Admission("recovery comparison"))?;
                let (path, original, budget) =
                    (path.clone(), original.clone(), Arc::clone(&budget));
                changed |= tokio::task::spawn_blocking(move || {
                    let _memory = memory;
                    Ok::<_, RuntimeError>(
                        fingerprint(&path, &budget)? != fingerprint(&original, &budget)?,
                    )
                })
                .await
                .map_err(RuntimeError::join("recovery comparison"))??;
            }
            changed
        };
        if !changed {
            return Ok(false);
        }
        work.volumes = selected.iter().map(|(_, (_, volume))| *volume).collect();
        let names = selected.iter().map(|(name, _)| name.clone()).collect();
        let inputs = selected.into_iter().map(|(_, (path, _))| path).collect();
        self.archive_runtime
            .get_mut(&job_id)
            .ok_or(RuntimeError::Invariant("archive runtime"))?
            .stage_repair_replacement(owner, work, inputs, names)?;
        self.commit_archive_graph(job_id).await?;
        let (work, lease) = self
            .archive_runtime
            .get_mut(&job_id)
            .ok_or(RuntimeError::Invariant("archive runtime"))?
            .finish_repair_replacement()?;
        Box::pin(self.spawn_scoped_selected_extraction(job_id, work, lease)).await?;
        Ok(true)
    }
}
