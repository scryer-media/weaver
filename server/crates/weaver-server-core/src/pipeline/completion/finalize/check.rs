use super::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanPar2IntegrityGate {
    None,
    WeakTransform,
    StrongDecode,
}

#[derive(Debug, Clone, Copy, Default)]
struct PromotedRecoveryPipelineState {
    download_queue_len: usize,
    download_queue_has_recovery: bool,
    recovery_queue_len: usize,
    promoted_par2_files: usize,
    incomplete_promoted_par2_files: usize,
    active_downloads_for_job: usize,
    active_recovery_global: usize,
    pending_promoted_decode: usize,
}

impl PromotedRecoveryPipelineState {
    fn has_inflight_promoted_recovery(self) -> bool {
        self.promoted_par2_files > 0
            && self.active_recovery_global > 0
            && self.active_downloads_for_job > 0
    }

    fn has_pending_work(self) -> bool {
        self.download_queue_has_recovery
            || self.incomplete_promoted_par2_files > 0
            || self.has_inflight_promoted_recovery()
            || self.pending_promoted_decode > 0
    }
}

fn summarize_rar_set_phase(
    set_name: &str,
    set_state: &crate::pipeline::archive::rar_state::RarSetState,
) -> String {
    let phase = set_state
        .plan
        .as_ref()
        .map(|plan| plan.phase)
        .unwrap_or(set_state.phase);
    let mut ready_members = set_state
        .plan
        .as_ref()
        .map(|plan| {
            plan.ready_members
                .iter()
                .map(|member| member.name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    ready_members.sort();
    let waiting_on_volumes = set_state
        .plan
        .as_ref()
        .map(|plan| {
            let mut waiting = plan.waiting_on_volumes.iter().copied().collect::<Vec<_>>();
            waiting.sort_unstable();
            waiting
        })
        .unwrap_or_default();
    let mut in_flight_members = set_state.in_flight_members.iter().cloned().collect::<Vec<_>>();
    in_flight_members.sort();
    let mut suspect_volumes = set_state
        .verified_suspect_volumes
        .iter()
        .copied()
        .collect::<Vec<_>>();
    suspect_volumes.sort_unstable();

    format!(
        "{set_name}: phase={phase:?} workers={} ready={ready_members:?} waiting={waiting_on_volumes:?} inflight={in_flight_members:?} suspect={suspect_volumes:?}",
        set_state.active_workers,
    )
}

impl Pipeline {
    fn current_rar_set_names_for_job(&self, job_id: JobId) -> HashSet<String> {
        let Some(state) = self.jobs.get(&job_id) else {
            return HashSet::new();
        };

        let mut set_names: HashSet<String> = state
            .assembly
            .files()
            .filter_map(|file| {
                self.effective_file_identity(job_id, file.file_id())
                    .and_then(|identity| identity.classification)
                    .and_then(|classification| {
                        matches!(
                            classification.kind,
                            crate::jobs::assembly::DetectedArchiveKind::Rar
                        )
                        .then_some(classification.set_name)
                    })
            })
            .collect();

        if set_names.is_empty() {
            set_names.extend(
                state
                    .assembly
                    .archive_topologies()
                    .iter()
                    .filter_map(|(set_name, topology)| {
                        matches!(topology.archive_type, crate::jobs::assembly::ArchiveType::Rar)
                            .then_some(set_name.clone())
                    }),
            );
        }

        set_names
    }

    pub(crate) fn job_has_live_rar_waiting_for_missing_volumes(&self, job_id: JobId) -> bool {
        let current_set_names = self.current_rar_set_names_for_job(job_id);

        self.rar_sets.iter().any(|((rar_job_id, set_name), set_state)| {
            *rar_job_id == job_id
                && (current_set_names.is_empty() || current_set_names.contains(set_name))
                && set_state.plan.as_ref().is_some_and(|plan| {
                    matches!(
                        plan.phase,
                        crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
                            | crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair
                    )
                })
        })
    }

    pub(crate) fn job_has_pending_download_pipeline_work(&self, job_id: JobId) -> bool {
        let has_queued_work = self
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.health_probing || !state.download_queue.is_empty());
        let has_inflight_downloads = self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_inflight_decodes = self
            .active_decodes_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_delayed_retries = self
            .pending_retries_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_pending_decode = self
            .pending_decode
            .iter()
            .any(|work| work.segment_id.file_id.job_id == job_id);
        let has_buffered_segments = self
            .write_buffers
            .iter()
            .any(|(file_id, write_buf)| file_id.job_id == job_id && write_buf.buffered_len() > 0);

        has_queued_work
            || has_inflight_downloads
            || has_inflight_decodes
            || has_delayed_retries
            || has_pending_decode
            || has_buffered_segments
    }

    fn promoted_recovery_pipeline_state(&self, job_id: JobId) -> PromotedRecoveryPipelineState {
        let (download_queue_len, download_queue_has_recovery, recovery_queue_len) = self
            .jobs
            .get(&job_id)
            .map(|state| {
                (
                    state.download_queue.len(),
                    state.download_queue.has_recovery_work(),
                    state.recovery_queue.len(),
                )
            })
            .unwrap_or((0, false, 0));
        let promoted_files: HashSet<u32> = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| file.promoted.then_some(file_index))
                    .collect()
            })
            .unwrap_or_default();
        let incomplete_promoted_par2_files = self
            .jobs
            .get(&job_id)
            .map(|state| {
                promoted_files
                    .iter()
                    .filter(|file_index| {
                        state
                            .assembly
                            .file(NzbFileId {
                                job_id,
                                file_index: **file_index,
                            })
                            .is_none_or(|file| !file.is_complete())
                    })
                    .count()
            })
            .unwrap_or(0);
        let pending_promoted_decode = self
            .pending_decode
            .iter()
            .filter(|work| {
                work.segment_id.file_id.job_id == job_id
                    && promoted_files.contains(&work.segment_id.file_id.file_index)
            })
            .count();

        PromotedRecoveryPipelineState {
            download_queue_len,
            download_queue_has_recovery,
            recovery_queue_len,
            promoted_par2_files: promoted_files.len(),
            incomplete_promoted_par2_files,
            active_downloads_for_job: self
                .active_downloads_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            active_recovery_global: self.active_recovery,
            pending_promoted_decode,
        }
    }

    fn job_has_promoted_recovery_pipeline_work(&self, job_id: JobId) -> bool {
        self.promoted_recovery_pipeline_state(job_id)
            .has_pending_work()
    }
}

impl Pipeline {
    fn clean_par2_integrity_gate(&self, job_id: JobId) -> CleanPar2IntegrityGate {
        let Some(state) = self.jobs.get(&job_id) else {
            return CleanPar2IntegrityGate::None;
        };

        let mut gate = CleanPar2IntegrityGate::None;
        for topology in state.assembly.archive_topologies().values() {
            let topology_gate = match topology.archive_type {
                crate::jobs::assembly::ArchiveType::Split
                | crate::jobs::assembly::ArchiveType::Tar => CleanPar2IntegrityGate::WeakTransform,
                crate::jobs::assembly::ArchiveType::SevenZip => {
                    if topology.volume_map.len() <= 1 {
                        CleanPar2IntegrityGate::WeakTransform
                    } else {
                        CleanPar2IntegrityGate::StrongDecode
                    }
                }
                crate::jobs::assembly::ArchiveType::Rar
                | crate::jobs::assembly::ArchiveType::Zip
                | crate::jobs::assembly::ArchiveType::TarGz
                | crate::jobs::assembly::ArchiveType::TarBz2
                | crate::jobs::assembly::ArchiveType::Gz
                | crate::jobs::assembly::ArchiveType::Deflate
                | crate::jobs::assembly::ArchiveType::Brotli
                | crate::jobs::assembly::ArchiveType::Zstd
                | crate::jobs::assembly::ArchiveType::Bzip2 => CleanPar2IntegrityGate::StrongDecode,
            };
            gate = match (gate, topology_gate) {
                (CleanPar2IntegrityGate::StrongDecode, _)
                | (_, CleanPar2IntegrityGate::StrongDecode) => CleanPar2IntegrityGate::StrongDecode,
                (CleanPar2IntegrityGate::WeakTransform, _)
                | (_, CleanPar2IntegrityGate::WeakTransform) => CleanPar2IntegrityGate::WeakTransform,
                _ => CleanPar2IntegrityGate::None,
            };
        }

        gate
    }

    async fn load_existing_complete_file_hashes(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, [u8; 16]>, String> {
        self.db_blocking(move |db| db.load_complete_file_hashes(job_id))
            .await
            .map_err(|error| format!("failed to load completed-file hashes: {error}"))
    }

    fn expected_hash_for_verified_file(
        file_id: NzbFileId,
        existing_hashes: &HashMap<u32, [u8; 16]>,
    ) -> [u8; 16] {
        existing_hashes
            .get(&file_id.file_index)
            .copied()
            .unwrap_or([0u8; 16])
    }

    async fn try_deobfuscate_files_with_par2(&mut self, job_id: JobId) -> usize {
        let Some(par2) = self.par2_set(job_id).cloned() else {
            return 0;
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };
        let rename_dir = state.working_dir.clone();

        if weaver_nzb::is_protected_media_structure(&rename_dir) {
            info!(
                job_id = job_id.0,
                "skipping PAR2 rename inside protected media structure"
            );
            return 0;
        }

        let suggestions = match weaver_par2::scan_for_renames(&rename_dir, &par2) {
            Ok(suggestions) => suggestions,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "PAR2 rename scan failed"
                );
                return 0;
            }
        };

        let file_rows: Vec<(NzbFileId, crate::jobs::record::ActiveFileIdentity, bool)> = state
            .assembly
            .files()
            .filter_map(|file| {
                self.effective_file_identity(job_id, file.file_id())
                    .map(|identity| (file.file_id(), identity, file.is_complete()))
            })
            .collect();
        let mut by_current = HashMap::<String, (NzbFileId, bool)>::new();
        let mut by_source = HashMap::<String, (NzbFileId, bool)>::new();
        let mut by_canonical = HashMap::<String, (NzbFileId, bool)>::new();
        for (file_id, identity, is_complete) in &file_rows {
            by_current.insert(identity.current_filename.clone(), (*file_id, *is_complete));
            by_source.insert(identity.source_filename.clone(), (*file_id, *is_complete));
            if let Some(canonical) = identity.canonical_filename.as_ref() {
                by_canonical.insert(canonical.clone(), (*file_id, *is_complete));
            }
        }
        let _ = state;

        let mut renamed = 0usize;
        let mut touched_files = Vec::<NzbFileId>::new();
        let mut touched_rar_files = HashMap::<String, HashSet<String>>::new();
        for suggestion in &suggestions {
            let old = &suggestion.current_path;
            let new = old.parent().unwrap().join(&suggestion.correct_name);
            let old_name = old
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default();
            let matched = by_current
                .get(&old_name)
                .copied()
                .or_else(|| by_source.get(&old_name).copied())
                .or_else(|| by_canonical.get(&old_name).copied());
            if old.file_name().map(|name| name.to_string_lossy().to_string())
                == Some(suggestion.correct_name.clone())
            {
                continue;
            }

            match std::fs::rename(old, &new) {
                Ok(()) => {
                    renamed += 1;
                    info!(
                        job_id = job_id.0,
                        from = %old.file_name().unwrap().to_string_lossy(),
                        to = %suggestion.correct_name,
                        "deobfuscated file via PAR2 metadata"
                    );
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        from = %old.display(),
                        to = %new.display(),
                        error = %error,
                        "PAR2 rename failed"
                    );
                }
            }

            if let Some((file_id, is_complete)) = matched {
                let Some((_, identity, _)) = file_rows
                    .iter()
                    .find(|(candidate_file_id, _, _)| *candidate_file_id == file_id)
                    .cloned()
                else {
                    continue;
                };
                let old_current_filename = identity.current_filename.clone();
                let old_rar_set_name =
                    identity.classification.as_ref().and_then(|classification| {
                        matches!(
                            classification.kind,
                            crate::jobs::assembly::DetectedArchiveKind::Rar
                        )
                        .then(|| classification.set_name.clone())
                    });
                let classification =
                    Self::canonical_archive_identity_from_filename(&suggestion.correct_name)
                        .or(identity.classification.clone());
                if let Some(set_name) = old_rar_set_name {
                    touched_rar_files
                        .entry(set_name)
                        .or_default()
                        .insert(old_current_filename);
                }
                let mut rebound_identity = identity;
                rebound_identity.current_filename = suggestion.correct_name.clone();
                rebound_identity.canonical_filename = Some(suggestion.correct_name.clone());
                rebound_identity.classification = classification;
                rebound_identity.classification_source =
                    crate::jobs::record::FileIdentitySource::Par2;
                if let Err(error) = self.set_file_identity(job_id, rebound_identity) {
                    warn!(
                        job_id = job_id.0,
                        file_index = file_id.file_index,
                        error = %error,
                        "failed to persist PAR2 deobfuscation identity"
                    );
                } else if is_complete {
                    touched_files.push(file_id);
                }
            }
        }

        for (set_name, touched_filenames) in &touched_rar_files {
            self.invalidate_archive_set_for_identity_rebind(job_id, set_name, touched_filenames);
        }
        for file_id in touched_files {
            self.refresh_archive_state_for_completed_file(job_id, file_id, false)
                .await;
        }

        if renamed > 0 {
            info!(job_id = job_id.0, renamed, "PAR2 deobfuscation complete");
        }

        renamed
    }

    async fn verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<weaver_par2::Par2FileSet>,
        working_dir: std::path::PathBuf,
        preserve_repairing_status: bool,
        emit_events: bool,
    ) -> Result<(weaver_par2::VerificationResult, weaver_par2::PlacementPlan), String> {
        if emit_events {
            if !preserve_repairing_status {
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Verifying,
                    Some("verifying"),
                );
            } else {
                info!(
                    job_id = job_id.0,
                    "rerunning PAR2 verification while preserving restored repair slot"
                );
            }
            let _ = self
                .event_tx
                .send(PipelineEvent::JobVerificationStarted { job_id });
            let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
            });
        }

        self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
        info!(job_id = job_id.0, "par2 verification started");

        let verify_dir = working_dir.clone();
        let pp_pool = self.pp_pool.clone();
        let verify_result = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || {
                let plan = weaver_par2::scan_placement(&verify_dir, &par2_set)
                    .map_err(|error| format!("placement scan failed: {error}"))?;
                if !plan.conflicts.is_empty() {
                    return Err(format!(
                        "placement scan found {} conflicting file matches",
                        plan.conflicts.len()
                    ));
                }

                let file_access =
                    weaver_par2::PlacementFileAccess::from_plan(verify_dir, &par2_set, &plan);
                Ok((weaver_par2::verify_all(&par2_set, &file_access), plan))
            })
        })
        .await;

        self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

        let (mut verification, placement_plan) = match verify_result {
            Ok(Ok(result)) => result,
            Ok(Err(message)) => return Err(message),
            Err(error) => return Err(format!("verification task panicked: {error}")),
        };
        Self::log_placement_plan(job_id, &placement_plan);

        let (skipped_blocks, retained_suspect_blocks) =
            self.apply_eager_delete_exclusions(job_id, &mut verification);
        if skipped_blocks > 0 {
            info!(
                job_id = job_id.0,
                skipped_blocks, "excluded eagerly-deleted CRC-verified volumes from damage count"
            );
        }
        if retained_suspect_blocks > 0 {
            info!(
                job_id = job_id.0,
                retained_suspect_blocks, "retained suspect eagerly-deleted volumes in damage count"
            );
        }

        self.recompute_volume_safety_from_verification(job_id, &verification);

        if emit_events {
            let _ = self.event_tx.send(PipelineEvent::JobVerificationComplete {
                job_id,
                passed: verification.total_missing_blocks == 0,
            });
        }

        Ok((verification, placement_plan))
    }

    async fn quick_verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<weaver_par2::Par2FileSet>,
        _working_dir: std::path::PathBuf,
    ) -> Result<Option<(weaver_par2::VerificationResult, weaver_par2::PlacementPlan)>, String>
    {
        let completed_hashes = self.load_existing_complete_file_hashes(job_id).await?;
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(None);
        };

        let mut current_hashes_by_name = HashMap::<String, [u8; 16]>::new();
        for file in state.assembly.files() {
            if !file.is_complete() {
                continue;
            }

            let file_id = file.file_id();
            let Some(file_hash) = completed_hashes.get(&file_id.file_index).copied() else {
                return Ok(None);
            };
            let identity = self.effective_file_identity(job_id, file_id);
            let current_filename = identity
                .as_ref()
                .map(|value| value.current_filename.as_str())
                .unwrap_or_else(|| file.filename());
            current_hashes_by_name.insert(current_filename.to_string(), file_hash);
        }

        let mut all_file_ids: Vec<weaver_par2::FileId> = par2_set
            .recovery_file_ids
            .iter()
            .chain(par2_set.non_recovery_file_ids.iter())
            .copied()
            .collect();
        all_file_ids.sort_unstable_by_key(|file_id| *file_id.as_bytes());
        all_file_ids.dedup();

        let mut hash_lookup = HashMap::<[u8; 16], Vec<(weaver_par2::FileId, String)>>::new();
        for file_id in &all_file_ids {
            let Some(desc) = par2_set.file_description(file_id) else {
                continue;
            };
            hash_lookup
                .entry(desc.hash_full)
                .or_default()
                .push((*file_id, desc.filename.clone()));
        }

        let mut matches = HashMap::<String, (weaver_par2::FileId, String)>::new();
        let mut match_counts = HashMap::<weaver_par2::FileId, u32>::new();
        for (current_name, file_hash) in current_hashes_by_name {
            let Some(candidates) = hash_lookup.get(&file_hash) else {
                continue;
            };

            if let Some((file_id, correct_name)) = candidates.first() {
                matches.insert(current_name.clone(), (*file_id, correct_name.clone()));
                *match_counts.entry(*file_id).or_default() += 1;
            }
        }

        let conflict_ids: HashSet<weaver_par2::FileId> = match_counts
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(file_id, _)| *file_id)
            .collect();
        matches.retain(|_, (file_id, _)| !conflict_ids.contains(file_id));

        let mut id_to_disk = HashMap::<weaver_par2::FileId, String>::new();
        for (disk_name, (file_id, _)) in &matches {
            id_to_disk.insert(*file_id, disk_name.clone());
        }

        let mut files = Vec::new();
        let mut exact = Vec::new();
        let mut swaps = Vec::new();
        let mut renames = Vec::new();
        let mut unresolved = Vec::new();
        let mut seen_swap = HashSet::<weaver_par2::FileId>::new();
        for file_id in all_file_ids.iter().copied() {
            let Some(desc) = par2_set.file_description(&file_id).cloned() else {
                continue;
            };

            if conflict_ids.contains(&file_id) {
                continue;
            }

            let Some(disk_name) = id_to_disk.get(&file_id).cloned() else {
                unresolved.push(file_id);
                continue;
            };

            if disk_name == desc.filename {
                exact.push(file_id);
            } else if !seen_swap.contains(&file_id) {
                let other_file_id = matches.get(desc.filename.as_str()).map(|(id, _)| *id);
                if let Some(other_id) = other_file_id
                    && other_id != file_id
                    && id_to_disk
                        .get(&other_id)
                        .is_some_and(|name| name == &desc.filename)
                {
                    let Some(other_desc) = par2_set.file_description(&other_id) else {
                        return Ok(None);
                    };
                    swaps.push((
                        weaver_par2::PlacementEntry {
                            file_id,
                            current_name: disk_name.clone(),
                            correct_name: desc.filename.clone(),
                        },
                        weaver_par2::PlacementEntry {
                            file_id: other_id,
                            current_name: desc.filename.clone(),
                            correct_name: other_desc.filename.clone(),
                        },
                    ));
                    seen_swap.insert(file_id);
                    seen_swap.insert(other_id);
                } else {
                    renames.push(weaver_par2::PlacementEntry {
                        file_id,
                        current_name: disk_name.clone(),
                        correct_name: desc.filename.clone(),
                    });
                }
            }

            let slice_count = par2_set.slice_count_for_file(desc.length) as usize;
            files.push(weaver_par2::verify::FileVerification {
                file_id,
                filename: desc.filename,
                status: weaver_par2::verify::FileStatus::Complete,
                valid_slices: vec![true; slice_count],
                missing_slice_count: 0,
            });
        }

        if !conflict_ids.is_empty() || !unresolved.is_empty() {
            return Ok(None);
        }

        Ok(Some((
            weaver_par2::VerificationResult {
                files,
                recovery_blocks_available: par2_set.recovery_block_count(),
                total_missing_blocks: 0,
                repairable: weaver_par2::verify::Repairability::NotNeeded,
            },
            weaver_par2::PlacementPlan {
                exact,
                swaps,
                renames,
                unresolved,
                conflicts: conflict_ids.into_iter().collect(),
            },
        )))
    }

    async fn reconcile_verified_par2_files(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) -> Result<usize, String> {
        let existing_hashes = self.load_existing_complete_file_hashes(job_id).await?;
        let files_to_complete: Vec<(NzbFileId, String, u64)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(0);
            };

            let mut by_name = HashMap::<String, (NzbFileId, u64, bool)>::new();
            for file in state.assembly.files() {
                let file_id = file.file_id();
                let total_bytes = file.total_bytes();
                let is_complete = file.is_complete();
                let identity = self.effective_file_identity(job_id, file_id);
                let current_filename = identity
                    .as_ref()
                    .map(|value| value.current_filename.clone())
                    .unwrap_or_else(|| file.filename().to_string());
                by_name
                    .entry(current_filename)
                    .or_insert((file_id, total_bytes, is_complete));
                if let Some(identity) = identity {
                    by_name.entry(identity.source_filename.clone()).or_insert((
                        file_id,
                        total_bytes,
                        is_complete,
                    ));
                    if let Some(canonical) = identity.canonical_filename {
                        by_name
                            .entry(canonical)
                            .or_insert((file_id, total_bytes, is_complete));
                    }
                }
            }

            let mut matched = HashMap::<NzbFileId, (String, u64)>::new();
            for file_verification in &verification.files {
                if !matches!(
                    file_verification.status,
                    weaver_par2::verify::FileStatus::Complete
                        | weaver_par2::verify::FileStatus::Renamed(_)
                ) {
                    continue;
                }

                let mut candidate_names = vec![file_verification.filename.clone()];
                if let weaver_par2::verify::FileStatus::Renamed(path) = &file_verification.status
                    && let Some(filename) = path.file_name()
                {
                    candidate_names.push(filename.to_string_lossy().to_string());
                }

                for candidate_name in &candidate_names {
                    let Some((file_id, total_bytes, is_complete)) =
                        by_name.get(candidate_name).copied()
                    else {
                        continue;
                    };
                    if is_complete {
                        break;
                    }
                    let current_filename = self
                        .current_filename_for_file_id(job_id, file_id)
                        .unwrap_or_else(|| candidate_name.clone());
                    matched
                        .entry(file_id)
                        .or_insert((current_filename, total_bytes));
                    break;
                }
            }

            matched
                .into_iter()
                .map(|(file_id, (filename, total_bytes))| (file_id, filename, total_bytes))
                .collect()
        };

        if files_to_complete.is_empty() {
            return Ok(0);
        }

        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return Ok(0);
            };
            for (file_id, _, _) in &files_to_complete {
                let Some(file) = state.assembly.file_mut(*file_id) else {
                    continue;
                };
                file.mark_complete();
            }
        }

        for (file_id, filename, total_bytes) in &files_to_complete {
            self.note_file_progress_floor(*file_id, *total_bytes, true);
            let file_index = file_id.file_index;
            let current_filename = filename.clone();
            let current_hash = Self::expected_hash_for_verified_file(*file_id, &existing_hashes);
            self.db_blocking(move |db| {
                db.complete_file(job_id, file_index, &current_filename, &current_hash)
            })
            .await
            .map_err(|error| {
                format!(
                    "failed to persist PAR2-reconciled file {}: {error}",
                    filename
                )
            })?;
            self.pending_file_progress.remove(file_id);
            self.persisted_file_progress.remove(file_id);
            self.file_hash_states.remove(file_id);
            self.file_hash_reread_required.remove(file_id);
            self.refresh_archive_state_for_completed_file(job_id, *file_id, true)
                .await;
        }

        Ok(files_to_complete.len())
    }

    async fn check_rar_job_completion(&mut self, job_id: JobId) {
        let set_names = self.rar_set_names_for_job(job_id);
        if set_names.is_empty() {
            return;
        }

        if self.has_active_rar_workers(job_id) {
            if self
                .jobs
                .get(&job_id)
                .is_some_and(|state| !matches!(state.status, JobStatus::Extracting))
            {
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Extracting,
                    Some("extracting"),
                );
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionReady { job_id });
            }
            return;
        }

        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let extracted_archives = self
            .extracted_archives
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let mut forced_recompute = false;
        let (fallback_sets, has_incomplete_sets, has_ready_incremental_work) = loop {
            let mut fallback_sets = Vec::new();
            let mut has_incomplete_sets = false;
            let mut has_ready_incremental_work = false;
            let mut impossible_sets = Vec::new();

            for set_name in &set_names {
                let set_state = self.rar_sets.get(&(job_id, set_name.clone()));
                let set_complete = extracted_archives.contains(set_name)
                    || set_state
                        .and_then(|state| state.plan.as_ref())
                        .is_some_and(|plan| {
                            !plan.member_names.is_empty()
                                && plan
                                    .member_names
                                    .iter()
                                    .all(|member| extracted.contains(member))
                        });
                if set_complete {
                    self.extracted_archives
                        .entry(job_id)
                        .or_default()
                        .insert(set_name.clone());
                    continue;
                }

                has_incomplete_sets = true;
                if let Some(state) = set_state
                    && let Some(plan) = state.plan.as_ref()
                {
                    if matches!(
                        plan.phase,
                        crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet
                    ) {
                        fallback_sets.push(set_name.clone());
                    } else if plan.ready_members.iter().any(|ready_member| {
                        self.rar_member_can_start_extraction(job_id, set_name, &ready_member.name)
                    }) {
                        has_ready_incremental_work = true;
                    } else if plan.waiting_on_volumes.is_empty() {
                        impossible_sets.push(set_name.clone());
                    }
                } else {
                    fallback_sets.push(set_name.clone());
                }
            }

            if impossible_sets.is_empty() {
                break (
                    fallback_sets,
                    has_incomplete_sets,
                    has_ready_incremental_work,
                );
            }

            if forced_recompute {
                let set_list = impossible_sets.join(", ");
                let msg = format!(
                    "invalid RAR state after recompute: sets [{set_list}] are incomplete with no ready members, no fallback, and no waiting volumes"
                );
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg);
                return;
            }

            forced_recompute = true;
            for set_name in impossible_sets {
                if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error,
                        "failed forced RAR recompute for impossible state"
                    );
                }
            }
        };

        if has_incomplete_sets {
            if (has_ready_incremental_work || !fallback_sets.is_empty())
                && !self.maybe_start_extraction(job_id).await
            {
                return;
            }

            if has_ready_incremental_work {
                self.try_rar_extraction(job_id).await;
                return;
            }

            for set_name in &fallback_sets {
                if let Err(error) = self.extract_rar_set(job_id, set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %error,
                        "failed to start RAR full-set extraction"
                    );
                    self.fail_job(job_id, error);
                    return;
                }
            }
            if !fallback_sets.is_empty() {
                return;
            }

            return;
        }

        self.finalize_completed_archive_job(job_id).await;
    }

    fn only_archive_residuals_or_loaded_par2_index_are_incomplete(&self, job_id: JobId) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        if self.job_has_active_extraction_tasks(job_id) {
            return false;
        }

        let extracted_archives = self
            .extracted_archives
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let extracted_members = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let par2_loaded = self.par2_set(job_id).is_some();
        let mut saw_incomplete = false;

        for file in state.assembly.files() {
            if file.is_complete() {
                continue;
            }
            match self.classified_role_for_file(job_id, file) {
                weaver_model::files::FileRole::Par2 {
                    is_index: false, ..
                } => {}
                weaver_model::files::FileRole::Par2 { is_index: true, .. } if par2_loaded => {
                    saw_incomplete = true;
                }
                _ => {
                    let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file)
                    else {
                        return false;
                    };
                    let set_complete = extracted_archives.contains(&set_name)
                        || self
                            .rar_sets
                            .get(&(job_id, set_name.clone()))
                            .and_then(|state| state.plan.as_ref())
                            .is_some_and(|plan| {
                                !plan.member_names.is_empty()
                                    && plan
                                        .member_names
                                        .iter()
                                        .all(|member| extracted_members.contains(member))
                            });
                    if !set_complete {
                        return false;
                    }
                    saw_incomplete = true;
                }
            }
        }

        saw_incomplete
    }

    /// Check if all data files in a job are complete, and trigger post-processing.
    ///
    /// PAR2 is treated as a repair tool only — damage is detected via yEnc CRC
    /// (per-segment) and RAR CRC (per-member extraction). If
    /// CRC failures occur, recovery files are promoted for download and repair
    /// runs from disk using `verify_all` + `plan_repair` + `execute_repair`.
    pub(crate) async fn check_job_completion(&mut self, job_id: JobId) {
        let current_status = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state.status.clone()
        };
        let (total_data_files, complete_data_files, failed_bytes, queued_downloads) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            (
                state.assembly.data_file_count(),
                state.assembly.complete_data_file_count(),
                state.failed_bytes,
                !state.download_queue.is_empty(),
            )
        };
        let has_incomplete_data_files = complete_data_files < total_data_files;

        // Step 1: Are all data files (non-recovery) complete?
        {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            if matches!(
                state.status,
                JobStatus::Paused
                    | JobStatus::Checking
                    | JobStatus::Moving
                    | JobStatus::Complete
                    | JobStatus::Failed { .. }
            ) {
                return;
            }
            // If no data files registered yet but there are still segments queued,
            // downloads haven't really started — don't prematurely leave Downloading.
            if total_data_files == 0 && queued_downloads {
                return;
            }
        }

        if matches!(current_status, JobStatus::QueuedRepair) {
            if self.active_repair_jobs() == 0 {
                self.promote_queued_repairs();
            }
            return;
        }

        let par2_bypassed = self.par2_bypassed.contains(&job_id);
        let par2_loaded = self.par2_set(job_id).is_some();
        let download_pipeline_exhausted = !self.job_has_pending_download_pipeline_work(job_id);
        let only_rar_archives = self.job_has_only_rar_archives(job_id);
        let par2_validation_needed = par2_loaded
            && !par2_bypassed
            && !self.par2_verified.contains(&job_id)
            && download_pipeline_exhausted
            && !self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id);
        let rar_waiting_for_missing_volumes = download_pipeline_exhausted
            && only_rar_archives
            && self.job_has_live_rar_waiting_for_missing_volumes(job_id);

        // Step 2: Check for CRC failures that need PAR2 repair.
        let has_crc_failures = self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|f| !f.is_empty());
        let clean_par2_integrity_gate = self.clean_par2_integrity_gate(job_id);
        let archive_extraction_applicable =
            self.extraction_readiness_for_job(job_id) != ExtractionReadiness::NotApplicable
                || only_rar_archives;
        let authoritative_par2_verification_needed = par2_validation_needed
            && (has_crc_failures
                || (has_incomplete_data_files && download_pipeline_exhausted)
                || rar_waiting_for_missing_volumes
                || matches!(current_status, JobStatus::Repairing));
        let quick_par2_verification_allowed = par2_validation_needed
            && !matches!(current_status, JobStatus::Repairing)
            && !(has_incomplete_data_files && download_pipeline_exhausted)
            && match clean_par2_integrity_gate {
                CleanPar2IntegrityGate::StrongDecode => {
                    only_rar_archives && (has_crc_failures || rar_waiting_for_missing_volumes)
                }
                CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None => true,
            };
        let needs_completion_repair_evaluation = has_crc_failures
            || (has_incomplete_data_files && download_pipeline_exhausted)
            || rar_waiting_for_missing_volumes
            || par2_validation_needed;

        if download_pipeline_exhausted && only_rar_archives {
            let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
            let inflight_extractions = self
                .inflight_extractions
                .get(&job_id)
                .map_or(0, HashSet::len);
            let has_active_rar_workers = self.has_active_rar_workers(job_id);
            let has_active_extraction_tasks =
                has_active_rar_workers || inflight_extractions > 0;
            let only_archive_residuals =
                self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id);
            let mut rar_set_state = self
                .rar_sets
                .iter()
                .filter(|((rar_job_id, _), _)| *rar_job_id == job_id)
                .map(|((_, set_name), set_state)| summarize_rar_set_phase(set_name, set_state))
                .collect::<Vec<_>>();
            rar_set_state.sort();
            let mut failed_extractions = self
                .failed_extractions
                .get(&job_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();
            failed_extractions.sort();

            info!(
                job_id = job_id.0,
                status = ?current_status,
                complete_data_files,
                total_data_files,
                failed_bytes,
                par2_loaded,
                has_crc_failures,
                rar_waiting_for_missing_volumes,
                has_active_rar_workers,
                inflight_extractions,
                has_active_extraction_tasks,
                only_archive_residuals,
                queued_downloads = promoted_recovery.download_queue_len,
                queued_promoted_recovery = promoted_recovery.download_queue_has_recovery,
                parked_recovery = promoted_recovery.recovery_queue_len,
                promoted_par2_files = promoted_recovery.promoted_par2_files,
                incomplete_promoted_par2_files = promoted_recovery.incomplete_promoted_par2_files,
                active_downloads_for_job = promoted_recovery.active_downloads_for_job,
                active_recovery_global = promoted_recovery.active_recovery_global,
                pending_promoted_decode = promoted_recovery.pending_promoted_decode,
                promoted_recovery_pending = promoted_recovery.has_pending_work(),
                failed_extractions = ?failed_extractions,
                rar_set_state = ?rar_set_state,
                "RAR completion checkpoint"
            );
        }

        if has_incomplete_data_files
            && !download_pipeline_exhausted
            && !self.job_has_active_extraction_tasks(job_id)
        {
            return;
        }

        // Don't finalize while concatenation is still pending.
        if self
            .pending_concat
            .get(&job_id)
            .is_some_and(|s| !s.is_empty())
        {
            debug!(
                job_id = job_id.0,
                "deferring completion — pending concatenation"
            );
            return;
        }

        if !has_crc_failures
            && self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id)
        {
            self.finalize_completed_archive_job(job_id).await;
            return;
        }

        if needs_completion_repair_evaluation && !par2_bypassed {
            if self.job_has_promoted_recovery_pipeline_work(job_id) {
                debug!(
                    job_id = job_id.0,
                    "deferring verify — promoted PAR2 recovery work is pending"
                );
                return;
            }

            if self.has_active_rar_workers(job_id) {
                info!(
                    job_id = job_id.0,
                    "deferring verify — active RAR extraction workers"
                );
                return;
            }

            let par2_set = self.par2_set(job_id).cloned();
            if quick_par2_verification_allowed
                && let Some(par2_set) = par2_set.as_ref()
            {
                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                match self
                    .quick_verify_par2_with_placement(
                        job_id,
                        Arc::clone(par2_set),
                        working_dir.clone(),
                    )
                    .await
                {
                    Ok(Some((verification, placement_plan))) => {
                        info!(
                            job_id = job_id.0,
                            "quick PAR2 verification passed for clean exhausted job"
                        );
                        Self::log_placement_plan(job_id, &placement_plan);

                        self.try_deobfuscate_files_with_par2(job_id).await;
                        if let Err(error) = self
                            .apply_placement_plan_for_retry_or_repair(
                                job_id,
                                working_dir.clone(),
                                &placement_plan,
                            )
                            .await
                        {
                            self.fail_job(job_id, error);
                            return;
                        }
                        self.retry_par2_authoritative_identity(job_id).await;
                        if let Err(error) = self
                            .reconcile_verified_par2_files(job_id, &verification)
                            .await
                        {
                            self.fail_job(job_id, error);
                            return;
                        }

                        let still_incomplete = self.jobs.get(&job_id).is_some_and(|state| {
                            state.assembly.complete_data_file_count()
                                < state.assembly.data_file_count()
                        });
                        if still_incomplete && !has_crc_failures {
                            let msg = "clean PAR2 quick verification but job still has incomplete data files after reconciliation".to_string();
                            warn!(job_id = job_id.0, error = %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

                        self.par2_verified.insert(job_id);

                        if has_crc_failures {
                            if self.normalization_retried.contains(&job_id) {
                                let msg =
                                    "clean PAR2 verification but extraction still failing after retry"
                                        .to_string();
                                warn!(job_id = job_id.0, error = %msg);
                                self.fail_job(job_id, msg);
                                return;
                            }

                            self.set_normalization_retried_state(job_id, true);
                            let failed_members = self
                                .failed_extractions
                                .get(&job_id)
                                .cloned()
                                .unwrap_or_default();
                            self.replace_failed_extraction_members(job_id, HashSet::new());
                            let cleared = failed_members.len();
                            self.recompute_rar_retry_frontier(job_id).await;
                            if let Some(reason) = self.invalid_rar_retry_frontier_reason(job_id) {
                                if !failed_members.is_empty() {
                                    self.replace_failed_extraction_members(job_id, failed_members);
                                }
                                let msg = format!(
                                    "invalid RAR retry frontier after placement correction: {reason}"
                                );
                                warn!(job_id = job_id.0, error = %msg);
                                self.fail_job(job_id, msg);
                                return;
                            }

                            info!(
                                job_id = job_id.0,
                                cleared,
                                "cleared failed extractions after quick verify — retrying"
                            );

                            self.retry_archive_extraction_after_verify_or_repair(job_id)
                                .await;
                            return;
                        }

                        if archive_extraction_applicable {
                            self.retry_archive_extraction_after_verify_or_repair(job_id)
                                .await;
                            return;
                        }

                        self.reconcile_job_progress(job_id).await;
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
                    Ok(None) => {
                        info!(
                            job_id = job_id.0,
                            "quick PAR2 verification was inconclusive — falling back to authoritative verify"
                        );
                    }
                    Err(message) => {
                        warn!(job_id = job_id.0, error = %message);
                        self.fail_job(job_id, message);
                        return;
                    }
                }
            }

            if par2_validation_needed && !authoritative_par2_verification_needed {
                match clean_par2_integrity_gate {
                    CleanPar2IntegrityGate::StrongDecode => {
                        info!(
                            job_id = job_id.0,
                            "skipping authoritative PAR2 verify for clean exhausted strong-decode job"
                        );

                        self.try_deobfuscate_files_with_par2(job_id).await;
                        self.retry_par2_authoritative_identity(job_id).await;
                        self.par2_verified.insert(job_id);

                        if archive_extraction_applicable {
                            self.retry_archive_extraction_after_verify_or_repair(job_id)
                                .await;
                            return;
                        }

                        self.reconcile_job_progress(job_id).await;
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
                    CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None => {}
                }
            }

            if let Some(par2_set) = par2_set {
                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                let (verification, placement_plan) = match self
                    .verify_par2_with_placement(
                        job_id,
                        Arc::clone(&par2_set),
                        working_dir.clone(),
                        matches!(current_status, JobStatus::Repairing),
                        true,
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(message) => {
                        warn!(job_id = job_id.0, error = %message);
                        self.fail_job(job_id, message);
                        return;
                    }
                };
                let damaged = verification.total_missing_blocks;
                let recovery_now = verification.recovery_blocks_available;
                let total_recovery_capacity = self.total_recovery_block_capacity(job_id);

                if damaged == 0 {
                    info!(
                        job_id = job_id.0,
                        "PAR2 verification passed — no damaged slices"
                    );

                    // Rename obfuscated files using PAR2 metadata even when
                    // verification is clean (files may be intact but obfuscated).
                    self.try_deobfuscate_files_with_par2(job_id).await;
                    if let Err(error) = self
                        .apply_placement_plan_for_retry_or_repair(
                            job_id,
                            working_dir.clone(),
                            &placement_plan,
                        )
                        .await
                    {
                        self.fail_job(job_id, error);
                        return;
                    }
                    self.retry_par2_authoritative_identity(job_id).await;
                    if let Err(error) = self
                        .reconcile_verified_par2_files(job_id, &verification)
                        .await
                    {
                        self.fail_job(job_id, error);
                        return;
                    }

                    let still_incomplete = self.jobs.get(&job_id).is_some_and(|state| {
                        state.assembly.complete_data_file_count() < state.assembly.data_file_count()
                    });
                    if still_incomplete && !has_crc_failures {
                        let msg = "clean PAR2 verification but job still has incomplete data files after reconciliation".to_string();
                        warn!(job_id = job_id.0, error = %msg);
                        self.fail_job(job_id, msg);
                        return;
                    }
                    self.par2_verified.insert(job_id);

                    if has_crc_failures {
                        if self.normalization_retried.contains(&job_id) {
                            let msg =
                                "clean PAR2 verification but extraction still failing after retry"
                                    .to_string();
                            warn!(job_id = job_id.0, error = %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

                        self.set_normalization_retried_state(job_id, true);
                        let failed_members = self
                            .failed_extractions
                            .get(&job_id)
                            .cloned()
                            .unwrap_or_default();
                        self.replace_failed_extraction_members(job_id, HashSet::new());
                        let cleared = failed_members.len();
                        self.recompute_rar_retry_frontier(job_id).await;
                        if let Some(reason) = self.invalid_rar_retry_frontier_reason(job_id) {
                            if !failed_members.is_empty() {
                                self.replace_failed_extraction_members(job_id, failed_members);
                            }
                            let msg = format!(
                                "invalid RAR retry frontier after placement correction: {reason}"
                            );
                            warn!(job_id = job_id.0, error = %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

                        info!(
                            job_id = job_id.0,
                            cleared,
                            "cleared failed extractions after authoritative verify — retrying"
                        );

                        self.retry_archive_extraction_after_verify_or_repair(job_id)
                            .await;
                        return;
                    }

                    if self.extraction_readiness_for_job(job_id)
                        != ExtractionReadiness::NotApplicable
                        || self.job_has_only_rar_archives(job_id)
                    {
                        self.retry_archive_extraction_after_verify_or_repair(job_id)
                            .await;
                        return;
                    }

                    self.reconcile_job_progress(job_id).await;
                    self.schedule_job_completion_check(job_id);
                    return;
                } else {
                    info!(
                        job_id = job_id.0,
                        damaged,
                        recovery_now,
                        total_recovery_capacity,
                        "PAR2 verification — damage detected"
                    );

                    if let Err(error) = self
                        .apply_placement_plan_for_retry_or_repair(
                            job_id,
                            working_dir.clone(),
                            &placement_plan,
                        )
                        .await
                    {
                        self.fail_job(job_id, error);
                        return;
                    }

                    if total_recovery_capacity < damaged {
                        self.fail_job(
                            job_id,
                            format!(
                                "not repairable: {damaged} damaged slices, only {total_recovery_capacity} recovery blocks advertised"
                            ),
                        );
                        return;
                    }

                    if recovery_now < damaged {
                        let promoted = self.promote_recovery_targeted(job_id, damaged);
                        let targeted_total = self.recovery_blocks_available_or_targeted(job_id);

                        // If all available/targeted recovery is still insufficient,
                        // fail immediately instead of waiting for downloads that
                        // won't help.
                        if targeted_total < damaged {
                            let msg = format!(
                                "not repairable: {damaged} damaged slices, \
                                 only {targeted_total} recovery blocks available in NZB"
                            );
                            warn!(job_id = job_id.0, %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

                        info!(
                            job_id = job_id.0,
                            damaged,
                            recovery_now,
                            targeted_total,
                            promoted_blocks = promoted,
                            "waiting for targeted recovery downloads before repair"
                        );
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                        return;
                    }

                    if !self.maybe_start_repair(job_id).await {
                        return;
                    }

                    let par2_for_repair = Arc::clone(&par2_set);
                    let repair_dir = working_dir.clone();

                    let pp_pool = self.pp_pool.clone();
                    let repair_result = tokio::task::spawn_blocking(move || {
                        pp_pool.install(move || {
                            crate::e2e_failpoint::maybe_delay("repair.task_start");
                            let mut file_access =
                                weaver_par2::DiskFileAccess::new(repair_dir, &par2_for_repair);
                            let plan = weaver_par2::plan_repair(&par2_for_repair, &verification)
                                .map_err(|e| format!("repair planning failed: {e}"))?;
                            let slices = plan.missing_slices.len() as u32;
                            weaver_par2::execute_repair(&plan, &par2_for_repair, &mut file_access)
                                .map_err(|e| format!("repair execution failed: {e}"))?;
                            Ok(slices)
                        })
                    })
                    .await;

                    let repair_outcome = match repair_result {
                        Ok(Ok(slices)) => Ok(slices),
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(format!("repair task panicked: {e}")),
                    };

                    match repair_outcome {
                        Ok(slices_repaired) => {
                            info!(job_id = job_id.0, slices_repaired, "PAR2 repair complete");
                            let _ = self.event_tx.send(PipelineEvent::RepairComplete {
                                job_id,
                                slices_repaired,
                            });

                            let (post_repair_verification, post_repair_placement_plan) = match self
                                .verify_par2_with_placement(
                                    job_id,
                                    Arc::clone(&par2_set),
                                    working_dir.clone(),
                                    true,
                                    false,
                                )
                                .await
                            {
                                Ok(result) => result,
                                Err(message) => {
                                    warn!(job_id = job_id.0, error = %message);
                                    self.fail_job(job_id, message);
                                    return;
                                }
                            };

                            if post_repair_verification.total_missing_blocks > 0 {
                                let msg = format!(
                                    "PAR2 repair completed but {} damaged slices remain",
                                    post_repair_verification.total_missing_blocks
                                );
                                warn!(job_id = job_id.0, error = %msg);
                                self.fail_job(job_id, msg);
                                return;
                            }

                            // Rename obfuscated files using PAR2 metadata (16KB hash matching).
                            // Must happen after repair and before extraction retry/finalize.
                            self.try_deobfuscate_files_with_par2(job_id).await;
                            if let Err(error) = self
                                .apply_placement_plan_for_retry_or_repair(
                                    job_id,
                                    working_dir.clone(),
                                    &post_repair_placement_plan,
                                )
                                .await
                            {
                                self.fail_job(job_id, error);
                                return;
                            }
                            self.retry_par2_authoritative_identity(job_id).await;
                            if let Err(error) = self
                                .reconcile_verified_par2_files(job_id, &post_repair_verification)
                                .await
                            {
                                self.fail_job(job_id, error);
                                return;
                            }

                            let still_incomplete = self.jobs.get(&job_id).is_some_and(|state| {
                                state.assembly.complete_data_file_count()
                                    < state.assembly.data_file_count()
                            });
                            if still_incomplete && !has_crc_failures {
                                let msg = "PAR2 repair completed but job still has incomplete data files after reconciliation".to_string();
                                warn!(job_id = job_id.0, error = %msg);
                                self.fail_job(job_id, msg);
                                return;
                            }
                            self.par2_verified.insert(job_id);
                            self.transition_postprocessing_status(
                                job_id,
                                JobStatus::Downloading,
                                Some("downloading"),
                            );

                            if has_crc_failures {
                                let cleared =
                                    self.failed_extractions.get(&job_id).map_or(0, HashSet::len);
                                self.replace_failed_extraction_members(job_id, HashSet::new());
                                if cleared > 0 {
                                    info!(
                                        job_id = job_id.0,
                                        cleared, "cleared failed extractions for post-repair retry"
                                    );
                                }

                                self.retry_archive_extraction_after_verify_or_repair(job_id)
                                    .await;
                                return;
                            }

                            self.reconcile_job_progress(job_id).await;
                            self.schedule_job_completion_check(job_id);
                            return;
                        }
                        Err(error_msg) => {
                            warn!(job_id = job_id.0, error = %error_msg, "PAR2 repair failed");
                            let _ = self.event_tx.send(PipelineEvent::RepairFailed {
                                job_id,
                                error: error_msg.clone(),
                            });
                            self.fail_job(job_id, error_msg);
                            return;
                        }
                    }
                }
            } else {
                if has_incomplete_data_files {
                    let msg = format!(
                        "download incomplete after exhausting retries: {complete_data_files}/{total_data_files} data files complete and no PAR2 metadata is available for repair"
                    );
                    warn!(job_id = job_id.0, error = %msg);
                    self.fail_job(job_id, msg);
                    return;
                }
                if has_crc_failures {
                    match self.retry_failed_archive_sources_without_par2(job_id).await {
                        Ok(true) => return,
                        Ok(false) => {}
                        Err(error) => {
                            self.fail_job(job_id, error);
                            return;
                        }
                    }
                }
                if rar_waiting_for_missing_volumes {
                    let reason = self.invalid_rar_retry_frontier_reason(job_id).unwrap_or_else(|| {
                        "RAR extraction stalled waiting for missing volumes after downloads finished"
                            .to_string()
                    });
                    let msg = format!("{reason}; no PAR2 metadata is available for repair");
                    warn!(job_id = job_id.0, error = %msg);
                    self.fail_job(job_id, msg);
                    return;
                }
                if !has_crc_failures {
                    match self.retry_failed_archive_sources_without_par2(job_id).await {
                        Ok(true) => return,
                        Ok(false) => {}
                        Err(error) => {
                            self.fail_job(job_id, error);
                            return;
                        }
                    }
                }

                let failed_members: Vec<String> = self
                    .failed_extractions
                    .get(&job_id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                let msg = format!(
                    "extraction CRC failures with no PAR2 data: {:?}",
                    failed_members
                );
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg);
                return;
            }
        } else if has_incomplete_data_files {
            if !download_pipeline_exhausted || self.job_has_active_extraction_tasks(job_id) {
                return;
            }
            let repair_context = if par2_bypassed {
                "PAR2 recovery is bypassed"
            } else if self.par2_set(job_id).is_some() {
                "PAR2 recovery did not become eligible"
            } else {
                "no PAR2 metadata is available for repair"
            };
            let byte_detail = if failed_bytes > 0 {
                format!(", {failed_bytes} bytes unavailable")
            } else {
                String::new()
            };
            let msg = format!(
                "download incomplete after exhausting retries: {complete_data_files}/{total_data_files} data files complete{byte_detail}, {repair_context}"
            );
            warn!(job_id = job_id.0, error = %msg);
            self.fail_job(job_id, msg);
            return;
        }

        if only_rar_archives {
            self.check_rar_job_completion(job_id).await;
            return;
        }

        if self.job_has_promoted_recovery_pipeline_work(job_id) {
            debug!(
                job_id = job_id.0,
                "deferring extraction — promoted PAR2 recovery work is pending"
            );
            return;
        }

        // Check extraction readiness.
        let readiness = self.extraction_readiness_for_job(job_id);
        match readiness {
            ExtractionReadiness::NotApplicable => {
                // A complete non-archive payload can still have explicitly
                // promoted PAR2 recovery segments in flight. Do not let stale
                // completion checks finalize damaged direct/gzip/etc. payloads
                // before those recovery files are decoded and repaired.
                if self.job_has_promoted_recovery_pipeline_work(job_id) {
                    debug!(
                        job_id = job_id.0,
                        "deferring completion — pending download pipeline work"
                    );
                    return;
                }
                if self
                    .reconcile_extracted_outputs_for_completion(job_id)
                    .await
                {
                    self.reconcile_job_progress(job_id).await;
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                if !par2_bypassed {
                    self.cleanup_par2_files(job_id).await;
                }
                // No archives — move to complete and finish.
                self.transition_postprocessing_status(job_id, JobStatus::Moving, None);
                if let Err(error) = self.move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                    return;
                }
                self.transition_completed_runtime(job_id);
                if self.active_download_passes.remove(&job_id) {
                    let _ = self
                        .event_tx
                        .send(PipelineEvent::DownloadFinished { job_id });
                }
                self.clear_par2_runtime_state(job_id);
                self.clear_job_rar_runtime(job_id);
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                info!(job_id = job_id.0, "job completed (no archives)");
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Ready => {
                // Collect sets that still need extraction (some may have been
                // extracted during the partial extraction phase).
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
                    let state = self.jobs.get(&job_id).unwrap();
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

                // If extractions are still in-flight, wait for them to complete.
                if !already_spawned.is_empty() && sets_to_extract.is_empty() {
                    return;
                }

                if !sets_to_extract.is_empty() {
                    // Spawn extraction tasks in the background.
                    // handle_extraction_done will re-enter check_job_completion
                    // when each set finishes, and we'll reach the empty branch below.
                    if !self.maybe_start_extraction(job_id).await {
                        return;
                    }

                    self.spawn_extractions(job_id, &sets_to_extract).await;
                    // Return — extraction runs in background.
                    // handle_extraction_done will call check_job_completion again.
                    return;
                }

                if self
                    .reconcile_extracted_outputs_for_completion(job_id)
                    .await
                {
                    self.reconcile_job_progress(job_id).await;
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                let cleanup_files: HashSet<String> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    let mut cleanup_files: HashSet<String> = state
                        .assembly
                        .files()
                        .filter(|f| {
                            matches!(
                                self.classified_role_for_file(job_id, f),
                                weaver_model::files::FileRole::Par2 { .. }
                                    | weaver_model::files::FileRole::RarVolume { .. }
                                    | weaver_model::files::FileRole::SevenZipArchive
                                    | weaver_model::files::FileRole::SevenZipSplit { .. }
                            )
                        })
                        .map(|f| self.current_filename_for_file(job_id, f))
                        .collect();
                    for topology in state.assembly.archive_topologies().values() {
                        cleanup_files.extend(topology.volume_map.keys().cloned());
                    }
                    cleanup_files
                };
                let nested_decision = match self.maybe_start_nested_extraction(job_id).await {
                    Ok(decision) => decision,
                    Err(error) => {
                        self.fail_job(job_id, error);
                        return;
                    }
                };
                match nested_decision {
                    NestedExtractionDecision::Started
                    | NestedExtractionDecision::NoNestedArchives => {
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
                        if matches!(nested_decision, NestedExtractionDecision::Started) {
                            return;
                        }
                    }
                    NestedExtractionDecision::PreserveOutputsAtDepthLimit => {}
                }

                self.transition_postprocessing_status(job_id, JobStatus::Moving, None);
                info!(job_id = job_id.0, "extraction complete");
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionComplete { job_id });

                // Move extracted files to complete directory.
                if let Err(error) = self.move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                    return;
                }

                self.transition_completed_runtime(job_id);
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
            ExtractionReadiness::Blocked { reason } => {
                self.fail_job(job_id, reason);
            }
            ExtractionReadiness::Partial {
                extractable,
                waiting_on,
            } => {
                // Some archives are ready (e.g. all 7z split files arrived)
                // while others are still downloading. Spawn what we can.
                let already_done = self
                    .extracted_archives
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let already_inflight = self
                    .inflight_extractions
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let to_spawn: Vec<(String, crate::jobs::assembly::ArchiveType)> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    extractable
                        .iter()
                        .filter(|name| {
                            !already_done.contains(*name) && !already_inflight.contains(*name)
                        })
                        .filter_map(|name| {
                            state
                                .assembly
                                .archive_topology_for(name)
                                .map(|topo| (name.clone(), topo.archive_type))
                        })
                        .collect()
                };

                if to_spawn.is_empty() {
                    return;
                }

                if !self.maybe_start_extraction(job_id).await {
                    return;
                }

                let spawned = self.spawn_extractions(job_id, &to_spawn).await;
                info!(
                    job_id = job_id.0,
                    spawned,
                    waiting = ?waiting_on,
                    "started extraction for ready archives, waiting on remaining"
                );
            }
        }
    }
}
