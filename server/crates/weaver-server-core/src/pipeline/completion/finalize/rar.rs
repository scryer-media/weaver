use super::*;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

impl Pipeline {
    fn rar_volume_numbers_by_filename(&self, job_id: JobId) -> HashMap<String, u32> {
        let mut volume_numbers = HashMap::new();
        let Some(state) = self.jobs.get(&job_id) else {
            return volume_numbers;
        };

        for topology in state.assembly.archive_topologies().values() {
            for (filename, &volume_number) in &topology.volume_map {
                volume_numbers.insert(filename.clone(), volume_number);
            }
        }

        volume_numbers
    }

    pub(crate) fn claim_clean_rar_volume(
        decision: &crate::pipeline::archive::rar_state::RarVolumeDeleteDecision,
    ) -> bool {
        decision.pending_owners.is_empty()
            && decision.failed_owners.is_empty()
            && !decision.unresolved_boundary
    }

    pub(crate) fn suspect_rar_volumes_for_job(&self, job_id: JobId) -> HashSet<u32> {
        let suspect: HashSet<u32> = self
            .rar_sets
            .iter()
            .filter(|((jid, _), _)| *jid == job_id)
            .flat_map(|(_, state)| {
                let mut volumes = state
                    .verified_suspect_volumes
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                if let Some(plan) = state.plan.as_ref() {
                    volumes.extend(plan.delete_decisions.iter().filter_map(
                        |(volume, decision)| {
                            (!decision.failed_owners.is_empty()
                                || !decision.pending_owners.is_empty()
                                || plan.waiting_on_volumes.contains(volume))
                            .then_some(*volume)
                        },
                    ));
                }
                volumes
            })
            .collect();
        suspect
    }

    pub(crate) fn apply_eager_delete_exclusions(
        &self,
        job_id: JobId,
        verification: &mut weaver_par2::VerificationResult,
    ) -> (u32, u32) {
        let eagerly_deleted_names: HashSet<&str> = self
            .eagerly_deleted
            .get(&job_id)
            .map(|s| s.iter().map(String::as_str).collect())
            .unwrap_or_default();
        let suspect_volumes = self.suspect_rar_volumes_for_job(job_id);
        let volume_numbers = self.rar_volume_numbers_by_filename(job_id);

        let mut skipped_blocks = 0u32;
        let mut retained_suspect_blocks = 0u32;
        for file_verification in &mut verification.files {
            if matches!(
                file_verification.status,
                weaver_par2::verify::FileStatus::Missing
            ) && eagerly_deleted_names.contains(file_verification.filename.as_str())
            {
                let Some(&volume_number) = volume_numbers.get(file_verification.filename.as_str())
                else {
                    continue;
                };
                if suspect_volumes.contains(&volume_number) {
                    retained_suspect_blocks = retained_suspect_blocks
                        .saturating_add(file_verification.missing_slice_count);
                    continue;
                }
                skipped_blocks += file_verification.missing_slice_count;
                file_verification.status = weaver_par2::verify::FileStatus::Complete;
                file_verification.valid_slices.fill(true);
                file_verification.missing_slice_count = 0;
            }
        }
        verification.total_missing_blocks = verification
            .total_missing_blocks
            .saturating_sub(skipped_blocks);
        verification.refresh_repairability();
        (skipped_blocks, retained_suspect_blocks)
    }

    pub(crate) fn recompute_volume_safety_from_verification(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) {
        let eagerly_deleted_names: HashSet<&str> = self
            .eagerly_deleted
            .get(&job_id)
            .map(|s| s.iter().map(String::as_str).collect())
            .unwrap_or_default();
        let suspect_volumes = self.suspect_rar_volumes_for_job(job_id);
        let volume_numbers = self.rar_volume_numbers_by_filename(job_id);

        let status_by_name: HashMap<&str, &weaver_par2::FileVerification> = verification
            .files
            .iter()
            .map(|file| (file.filename.as_str(), file))
            .collect();

        let plans: Vec<(String, HashSet<u32>)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .archive_topologies()
                .iter()
                .map(|(set_name, topo)| {
                    let mut suspect = HashSet::new();
                    for (filename, &volume_number) in &topo.volume_map {
                        if let Some(file) = status_by_name.get(filename.as_str()) {
                            match file.status {
                                weaver_par2::verify::FileStatus::Complete
                                | weaver_par2::verify::FileStatus::Renamed(_) => {}
                                weaver_par2::verify::FileStatus::Missing
                                    if eagerly_deleted_names.contains(filename.as_str())
                                        && !volume_numbers.get(filename.as_str()).is_some_and(
                                            |number| suspect_volumes.contains(number),
                                        ) => {}
                                weaver_par2::verify::FileStatus::Missing
                                | weaver_par2::verify::FileStatus::Damaged(_) => {
                                    suspect.insert(volume_number);
                                }
                            }
                        }
                    }
                    (set_name.clone(), suspect)
                })
                .collect()
        };

        self.db_fire_and_forget(move |db| {
            if let Err(error) = db.clear_verified_suspect_volumes(job_id) {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to clear persisted verified suspect RAR volumes"
                );
            }
        });

        for (set_name, suspect) in plans {
            if let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                state.verified_suspect_volumes = suspect.clone();
            }
            self.persist_verified_suspect_volumes(job_id, &set_name, &suspect);
        }
    }

    pub(crate) async fn refresh_rar_topology_after_normalization(
        &mut self,
        job_id: JobId,
        normalized_files: &HashSet<String>,
    ) -> Result<(), String> {
        if normalized_files.is_empty() {
            return Ok(());
        }

        let touched_sets: BTreeMap<String, HashSet<String>> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(());
            };

            state
                .assembly
                .files()
                .filter_map(|file| {
                    if !normalized_files.contains(file.filename()) {
                        return None;
                    }
                    match file.role() {
                        weaver_model::files::FileRole::RarVolume { .. } => {
                            weaver_model::files::archive_base_name(file.filename(), file.role())
                                .map(|set_name| (set_name, file.filename().to_string()))
                        }
                        _ => None,
                    }
                })
                .fold(BTreeMap::new(), |mut acc, (set_name, filename)| {
                    acc.entry(set_name).or_default().insert(filename);
                    acc
                })
        };

        let mut errors = Vec::new();
        for (set_name, touched_filenames) in touched_sets {
            match self
                .refresh_rar_volume_facts_for_set(job_id, &set_name, &touched_filenames)
                .await
            {
                Ok(()) => info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    "refreshed RAR topology after normalization"
                ),
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error,
                        "failed to refresh RAR topology after normalization; retaining previous snapshot and topology"
                    );
                    errors.push(format!("{set_name}: {error}"));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    pub(crate) fn has_active_rar_workers(&self, job_id: JobId) -> bool {
        self.rar_set_names_for_job(job_id).iter().any(|set_name| {
            self.rar_sets
                .get(&(job_id, set_name.clone()))
                .is_some_and(|state| state.active_workers > 0)
        })
    }

    fn placement_normalized_files(plan: &weaver_par2::PlacementPlan) -> HashSet<String> {
        let mut normalized_files = HashSet::new();
        for (left, right) in &plan.swaps {
            normalized_files.insert(left.correct_name.clone());
            normalized_files.insert(right.correct_name.clone());
        }
        for entry in &plan.renames {
            normalized_files.insert(entry.correct_name.clone());
        }
        normalized_files
    }

    pub(super) fn log_placement_plan(job_id: JobId, plan: &weaver_par2::PlacementPlan) {
        if plan.swaps.is_empty() && plan.renames.is_empty() {
            return;
        }

        let swap_pairs: Vec<String> = plan
            .swaps
            .iter()
            .map(|(left, right)| {
                format!(
                    "{} -> {} | {} -> {}",
                    left.current_name, left.correct_name, right.current_name, right.correct_name
                )
            })
            .collect();
        let renames: Vec<String> = plan
            .renames
            .iter()
            .map(|entry| format!("{} -> {}", entry.current_name, entry.correct_name))
            .collect();

        info!(
            job_id = job_id.0,
            swaps = ?swap_pairs,
            renames = ?renames,
            "placement scan identified remapped files"
        );
    }

    pub(super) async fn apply_placement_plan_for_retry_or_repair(
        &mut self,
        job_id: JobId,
        working_dir: PathBuf,
        plan: &weaver_par2::PlacementPlan,
    ) -> Result<(), String> {
        if plan.swaps.is_empty() && plan.renames.is_empty() {
            return Ok(());
        }

        let plan = plan.clone();
        let normalized_files = Self::placement_normalized_files(&plan);
        let plan_for_apply = plan.clone();
        let moved = tokio::task::spawn_blocking(move || {
            weaver_par2::apply_placement_plan(&working_dir, &plan_for_apply)
                .map_err(|e| format!("placement normalization failed: {e}"))
        })
        .await
        .map_err(|e| format!("placement normalization task panicked: {e}"))??;

        info!(
            job_id = job_id.0,
            swaps = plan.swaps.len(),
            renames = plan.renames.len(),
            moved,
            "applied placement normalization after verify"
        );

        self.refresh_rar_topology_after_normalization(job_id, &normalized_files)
            .await
    }

    pub(super) async fn recompute_rar_retry_frontier(&mut self, job_id: JobId) {
        for set_name in self.rar_set_names_for_job(job_id) {
            if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "failed to recompute RAR set while rebuilding retry frontier"
                );
            }
        }
    }

    pub(crate) fn invalid_rar_retry_frontier_reason(&self, job_id: JobId) -> Option<String> {
        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let mut has_incomplete_sets = false;

        for set_name in self.rar_set_names_for_job(job_id) {
            let Some(set_state) = self.rar_sets.get(&(job_id, set_name.clone())) else {
                continue;
            };
            let Some(plan) = set_state.plan.as_ref() else {
                continue;
            };
            let set_complete = !plan.member_names.is_empty()
                && plan
                    .member_names
                    .iter()
                    .all(|member| extracted.contains(member));
            if set_complete {
                continue;
            }

            has_incomplete_sets = true;

            let waiting_marked_deletable: Vec<u32> = plan
                .waiting_on_volumes
                .intersection(&plan.deletion_eligible)
                .copied()
                .collect();
            if !waiting_marked_deletable.is_empty() {
                return Some(format!(
                    "set '{set_name}' waiting volumes marked deletable: {:?}",
                    waiting_marked_deletable
                ));
            }

            let waiting_already_deleted: Vec<u32> = plan
                .waiting_on_volumes
                .iter()
                .copied()
                .filter(|volume| {
                    self.is_rar_volume_deleted(job_id, &plan.topology.volume_map, *volume)
                })
                .collect();
            if !waiting_already_deleted.is_empty() {
                return Some(format!(
                    "set '{set_name}' waiting volumes already deleted: {:?}",
                    waiting_already_deleted
                ));
            }

            if !plan.ready_members.is_empty()
                || matches!(
                    plan.phase,
                    crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet
                )
            {
                return None;
            }
        }

        if has_incomplete_sets {
            Some("no retryable work remains for incomplete RAR sets".to_string())
        } else {
            None
        }
    }

    pub(super) fn job_has_only_rar_archives(&self, job_id: JobId) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };

        if !state.assembly.archive_topologies().is_empty() {
            let has_rar = state
                .assembly
                .archive_topologies()
                .values()
                .any(|topology| topology.archive_type == crate::jobs::assembly::ArchiveType::Rar);
            let has_non_rar = state
                .assembly
                .archive_topologies()
                .values()
                .any(|topology| topology.archive_type != crate::jobs::assembly::ArchiveType::Rar);
            if has_non_rar {
                return false;
            }
            if has_rar {
                return true;
            }
        }

        let mut has_rar = false;
        for file in state.assembly.files() {
            match file.role() {
                weaver_model::files::FileRole::RarVolume { .. } => has_rar = true,
                weaver_model::files::FileRole::SevenZipArchive
                | weaver_model::files::FileRole::SevenZipSplit { .. } => return false,
                _ => {}
            }
        }

        has_rar
    }

    pub(super) fn rar_set_names_for_job(&self, job_id: JobId) -> Vec<String> {
        let mut set_names: HashSet<String> = HashSet::new();
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };

        for (set_name, topology) in state.assembly.archive_topologies() {
            if topology.archive_type == crate::jobs::assembly::ArchiveType::Rar {
                set_names.insert(set_name.clone());
            }
        }

        for (jid, set_name) in self.rar_sets.keys() {
            if *jid == job_id {
                set_names.insert(set_name.clone());
            }
        }

        for file in state.assembly.files() {
            if matches!(file.role(), weaver_model::files::FileRole::RarVolume { .. })
                && let Some(set_name) =
                    weaver_model::files::archive_base_name(file.filename(), file.role())
            {
                set_names.insert(set_name);
            }
        }
        let mut set_names: Vec<String> = set_names.into_iter().collect();
        set_names.sort();
        set_names
    }

    pub(super) async fn finalize_completed_archive_job(&mut self, job_id: JobId) {
        {
            let state = self.jobs.get(&job_id).unwrap();
            let mut cleanup_files: HashSet<String> = state
                .assembly
                .files()
                .filter(|f| {
                    matches!(
                        f.role(),
                        weaver_model::files::FileRole::Par2 { .. }
                            | weaver_model::files::FileRole::RarVolume { .. }
                            | weaver_model::files::FileRole::SevenZipArchive
                            | weaver_model::files::FileRole::SevenZipSplit { .. }
                    )
                })
                .map(|f| f.filename().to_string())
                .collect();
            for topology in state.assembly.archive_topologies().values() {
                cleanup_files.extend(topology.volume_map.keys().cloned());
            }
            let mut removed = 0u32;
            for filename in &cleanup_files {
                let Some(path) = self.resolve_job_input_path(job_id, filename) else {
                    continue;
                };
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => removed += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        warn!(
                            file = %path.display(),
                            error = %e,
                            "failed to clean up source file"
                        );
                    }
                }
            }
            info!(
                job_id = job_id.0,
                removed,
                total = cleanup_files.len(),
                "post-extraction cleanup complete"
            );
        }

        match self.maybe_start_nested_extraction(job_id).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => {
                self.fail_job(job_id, error);
                return;
            }
        }

        self.transition_postprocessing_status(job_id, JobStatus::Moving, None);
        let _ = self
            .event_tx
            .send(PipelineEvent::ExtractionComplete { job_id });

        if let Err(error) = self.move_to_complete(job_id).await {
            self.fail_job(job_id, error);
            return;
        }

        {
            let state = self.jobs.get_mut(&job_id).unwrap();
            state.status = JobStatus::Complete;
        }
        // Ensure DownloadFinished is journaled before JobCompleted so the
        // timeline shows an accurate download duration.
        if self.active_download_passes.remove(&job_id) {
            let _ = self
                .event_tx
                .send(PipelineEvent::DownloadFinished { job_id });
        }
        self.clear_par2_runtime_state(job_id);
        self.clear_job_rar_runtime(job_id);
        self.job_order.retain(|id| *id != job_id);
        let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
        self.record_job_history(job_id);
    }

    pub(super) async fn retry_archive_extraction_after_verify_or_repair(&mut self, job_id: JobId) {
        self.transition_postprocessing_status(job_id, JobStatus::Downloading, Some("downloading"));

        if self.job_has_only_rar_archives(job_id) {
            let set_names: Vec<String> = self
                .rar_sets
                .keys()
                .filter(|(jid, _)| *jid == job_id)
                .map(|(_, name)| name.clone())
                .collect();
            for set_name in set_names {
                let _ = self.recompute_rar_set_state(job_id, &set_name).await;
            }
            self.try_rar_extraction(job_id).await;
            return;
        }

        let already_extracted = self
            .extracted_archives
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let already_spawned = self
            .inflight_extractions
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let sets_to_extract: Vec<(String, crate::jobs::assembly::ArchiveType)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .archive_topologies()
                .iter()
                .filter(|(name, _)| {
                    !already_extracted.contains(*name) && !already_spawned.contains(*name)
                })
                .map(|(name, topo)| (name.clone(), topo.archive_type))
                .collect()
        };

        if !already_spawned.is_empty() && sets_to_extract.is_empty() {
            return;
        }

        if !sets_to_extract.is_empty() {
            if !self.maybe_start_extraction(job_id).await {
                return;
            }
            self.spawn_extractions(job_id, &sets_to_extract).await;
            return;
        }

        self.finalize_completed_archive_job(job_id).await;
    }

    pub(super) async fn retry_failed_archive_sources_without_par2(
        &mut self,
        job_id: JobId,
    ) -> Result<bool, String> {
        if self.normalization_retried.contains(&job_id) {
            return Ok(false);
        }

        let failed_entries = self
            .failed_extractions
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        if failed_entries.is_empty() {
            return Ok(false);
        }

        struct SourceRetryFile {
            file_id: NzbFileId,
            filename: String,
            work: Vec<DownloadWork>,
        }

        let (retry_files, retry_sets, retry_members, working_dir) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(false);
            };

            let mut file_indices = HashSet::new();
            let mut retry_sets: HashSet<String> = HashSet::new();
            let mut retry_members: HashSet<String> = HashSet::new();

            for failed in &failed_entries {
                if let Some(topo) = state.assembly.archive_topology_for(failed) {
                    retry_sets.insert(failed.clone());
                    retry_members.extend(topo.members.iter().map(|member| member.name.clone()));
                    for filename in topo.volume_map.keys() {
                        if let Some((index, _)) = state
                            .spec
                            .files
                            .iter()
                            .enumerate()
                            .find(|(_, file)| file.filename == *filename)
                        {
                            file_indices.insert(index as u32);
                        }
                    }
                    continue;
                }

                let mut matched_member = false;
                for (set_name, topo) in state.assembly.archive_topologies() {
                    if !topo.members.iter().any(|member| member.name == *failed) {
                        continue;
                    }
                    matched_member = true;
                    retry_sets.insert(set_name.clone());
                    retry_members.extend(topo.members.iter().map(|member| member.name.clone()));
                    for filename in topo.volume_map.keys() {
                        if let Some((index, _)) = state
                            .spec
                            .files
                            .iter()
                            .enumerate()
                            .find(|(_, file)| file.filename == *filename)
                        {
                            file_indices.insert(index as u32);
                        }
                    }
                    break;
                }

                if matched_member {
                    continue;
                }

                if let Some((index, _)) = state
                    .spec
                    .files
                    .iter()
                    .enumerate()
                    .find(|(_, file)| file.filename == *failed)
                {
                    file_indices.insert(index as u32);
                }
            }

            let retry_files = file_indices
                .into_iter()
                .filter_map(|file_index| {
                    let file = state.spec.files.get(file_index as usize)?;
                    let file_id = NzbFileId { job_id, file_index };
                    let work = file
                        .segments
                        .iter()
                        .map(|segment| DownloadWork {
                            segment_id: SegmentId {
                                file_id,
                                segment_number: segment.number,
                            },
                            message_id: crate::jobs::ids::MessageId::new(&segment.message_id),
                            groups: file.groups.clone(),
                            priority: file.role.download_priority(),
                            byte_estimate: segment.bytes,
                            retry_count: 0,
                            is_recovery: false,
                            exclude_servers: vec![0],
                        })
                        .collect();
                    Some(SourceRetryFile {
                        file_id,
                        filename: file.filename.clone(),
                        work,
                    })
                })
                .collect::<Vec<_>>();

            (
                retry_files,
                retry_sets,
                retry_members,
                state.working_dir.clone(),
            )
        };

        if retry_files.is_empty() {
            return Ok(false);
        }

        self.set_normalization_retried_state(job_id, true);
        self.replace_failed_extraction_members(job_id, HashSet::new());

        let mut clear_extracted_archives = false;
        if let Some(extracted_archives) = self.extracted_archives.get_mut(&job_id) {
            for set_name in &retry_sets {
                extracted_archives.remove(set_name);
            }
            clear_extracted_archives = extracted_archives.is_empty();
        }
        if clear_extracted_archives {
            self.extracted_archives.remove(&job_id);
        }

        let mut clear_inflight_extractions = false;
        if let Some(inflight_extractions) = self.inflight_extractions.get_mut(&job_id) {
            for set_name in &retry_sets {
                inflight_extractions.remove(set_name);
            }
            clear_inflight_extractions = inflight_extractions.is_empty();
        }
        if clear_inflight_extractions {
            self.inflight_extractions.remove(&job_id);
        }

        let mut clear_extracted_members = false;
        if let Some(extracted_members) = self.extracted_members.get_mut(&job_id) {
            for member_name in &retry_members {
                extracted_members.remove(member_name);
            }
            clear_extracted_members = extracted_members.is_empty();
        }
        if clear_extracted_members {
            self.extracted_members.remove(&job_id);
        }
        if !retry_members.is_empty() {
            self.clear_persisted_extracted_members(job_id);
        }

        for retry_file in &retry_files {
            let path = working_dir.join(&retry_file.filename);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "failed to remove corrupt archive {} before source retry: {error}",
                        path.display()
                    ));
                }
            }
            if let Err(error) = self
                .db
                .mark_file_incomplete(job_id, retry_file.file_id.file_index)
            {
                warn!(
                    job_id = job_id.0,
                    file_index = retry_file.file_id.file_index,
                    error = %error,
                    "failed to persist file invalidation before source retry"
                );
            }
        }

        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return Ok(false);
            };

            for mut retry_file in retry_files {
                if let Some(file_asm) = state.assembly.file_mut(retry_file.file_id) {
                    file_asm.reset();
                }

                for topo in state.assembly.archive_topologies_mut().values_mut() {
                    if let Some(&volume_number) = topo.volume_map.get(&retry_file.filename) {
                        topo.complete_volumes.remove(&volume_number);
                    }
                }

                for work in retry_file.work.drain(..) {
                    state.download_queue.push(work);
                }
            }
        }

        info!(
            job_id = job_id.0,
            files = retry_sets.len().max(1),
            failed = ?failed_entries,
            "re-queueing archive source files after extraction failure without PAR2"
        );

        self.transition_postprocessing_status(job_id, JobStatus::Downloading, Some("downloading"));
        Ok(true)
    }
}
