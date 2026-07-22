use super::*;
use crate::runtime::fs as runtime_fs;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use weaver_model::files::{
    allocate_unique_download_filename, forget_reserved_download_filename,
    reserve_download_filename, sanitize_download_filename,
};

const PAR2_REPAIR_MEMORY_LIMIT_ENV: &str = "WEAVER_PAR2_REPAIR_MEMORY_LIMIT_BYTES";
// Sizes the transient streaming repair buffers (the decode matrix has its own
// budget floor inside weaver-par2). 64 MiB measured within noise of far
// larger budgets on heavily damaged sets once streaming repair got its
// batched kernels, so the default stays small and repairs coexist with
// concurrent downloads; the env override remains for tuning.
const DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

fn default_par2_repair_memory_limit_bytes() -> usize {
    DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES
}

fn configured_par2_repair_memory_limit_bytes() -> usize {
    parse_par2_repair_memory_limit_bytes(
        std::env::var(PAR2_REPAIR_MEMORY_LIMIT_ENV).ok().as_deref(),
    )
}

fn parse_par2_repair_memory_limit_bytes(raw: Option<&str>) -> usize {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return default_par2_repair_memory_limit_bytes();
    };
    match value.parse::<usize>() {
        Ok(bytes) if bytes > 0 => bytes,
        _ => {
            let default_bytes = default_par2_repair_memory_limit_bytes();
            warn!(
                env = PAR2_REPAIR_MEMORY_LIMIT_ENV,
                value, default_bytes, "invalid PAR2 repair memory limit; using default"
            );
            default_bytes
        }
    }
}

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
    download_queue_promoted_recovery: usize,
    recovery_queue_len: usize,
    parked_promoted_recovery: usize,
    promoted_par2_files: usize,
    incomplete_promoted_par2_files: usize,
    active_promoted_downloads: usize,
    pending_promoted_retries: usize,
    pending_promoted_decode: usize,
    active_promoted_decodes: usize,
    write_buffered_promoted_recovery: usize,
    unavailable_promoted_recovery_segments: usize,
}

fn reserve_identity_filenames(
    identity: &crate::jobs::record::ActiveFileIdentity,
    occupied_filenames: &mut HashSet<String>,
) {
    reserve_download_filename(&identity.source_filename, occupied_filenames);
    reserve_download_filename(&identity.current_filename, occupied_filenames);
    if let Some(canonical) = identity.canonical_filename.as_ref() {
        reserve_download_filename(canonical, occupied_filenames);
    }
}

fn forget_identity_filenames(
    identity: &crate::jobs::record::ActiveFileIdentity,
    occupied_filenames: &mut HashSet<String>,
) {
    forget_reserved_download_filename(&identity.source_filename, occupied_filenames);
    forget_reserved_download_filename(&identity.current_filename, occupied_filenames);
    if let Some(canonical) = identity.canonical_filename.as_ref() {
        forget_reserved_download_filename(canonical, occupied_filenames);
    }
}

fn reserve_directory_filenames(dir: &Path, occupied_filenames: &mut HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if let Some(filename) = entry.file_name().to_str() {
            reserve_download_filename(filename, occupied_filenames);
        }
    }
}

impl PromotedRecoveryPipelineState {
    fn has_pending_work(self) -> bool {
        self.download_queue_promoted_recovery > 0
            || self.active_promoted_downloads > 0
            || self.pending_promoted_retries > 0
            || self.pending_promoted_decode > 0
            || self.active_promoted_decodes > 0
            || self.write_buffered_promoted_recovery > 0
    }
}

fn par2_verification_needs_repair(verification: &weaver_par2::VerificationResult) -> bool {
    verification.needs_repair()
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
    let mut in_flight_members = set_state
        .in_flight_members
        .iter()
        .cloned()
        .collect::<Vec<_>>();
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
            set_names.extend(state.assembly.archive_topologies().iter().filter_map(
                |(set_name, topology)| {
                    matches!(
                        topology.archive_type,
                        crate::jobs::assembly::ArchiveType::Rar
                    )
                    .then_some(set_name.clone())
                },
            ));
        }

        set_names
    }

    fn job_has_idle_startable_rar_work(&self, job_id: JobId) -> bool {
        self.rar_sets
            .iter()
            .filter(|((rar_job_id, _), _)| *rar_job_id == job_id)
            .any(|((_, set_name), set_state)| {
                set_state.active_workers == 0
                    && set_state.in_flight_members.is_empty()
                    && set_state.plan.as_ref().is_some_and(|plan| {
                        plan.ready_members.iter().any(|ready_member| {
                            self.rar_ready_member_is_startable_for_batch_extraction(
                                job_id,
                                set_name,
                                &ready_member.name,
                            )
                        })
                    })
            })
    }

    pub(crate) fn job_has_live_rar_waiting_for_missing_volumes(&self, job_id: JobId) -> bool {
        let current_set_names = self.current_rar_set_names_for_job(job_id);

        self.rar_sets
            .iter()
            .any(|((rar_job_id, set_name), set_state)| {
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

    pub(crate) fn job_has_pending_rar_refresh_for_current_sets(&self, job_id: JobId) -> bool {
        let current_set_names = self.current_rar_set_names_for_job(job_id);

        self.rar_refresh_state
            .iter()
            .any(|((refresh_job_id, set_name), refresh_state)| {
                *refresh_job_id == job_id
                    && (current_set_names.is_empty() || current_set_names.contains(set_name))
                    && (refresh_state.in_flight.is_some() || refresh_state.queued.is_some())
            })
    }

    pub(crate) fn job_has_incoherent_rar_waiting_state(&self, job_id: JobId) -> bool {
        let current_set_names = self.current_rar_set_names_for_job(job_id);

        self.rar_sets
            .iter()
            .any(|((rar_job_id, set_name), set_state)| {
                *rar_job_id == job_id
                    && (current_set_names.is_empty() || current_set_names.contains(set_name))
                    && set_state.active_workers == 0
                    && set_state.in_flight_members.is_empty()
                    && set_state.plan.as_ref().is_some_and(|plan| {
                        matches!(
                            plan.phase,
                            crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
                        ) && plan.waiting_on_volumes.is_empty()
                            && plan.ready_members.is_empty()
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
        let has_released_download_results = self
            .pending_released_download_results_by_job
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
        let has_file_crc_recovery = self
            .file_crc_recoveries
            .keys()
            .any(|file_id| file_id.job_id == job_id);

        has_queued_work
            || has_inflight_downloads
            || has_inflight_decodes
            || has_delayed_retries
            || has_released_download_results
            || has_pending_decode
            || has_buffered_segments
            || has_file_crc_recovery
    }

    fn promoted_recovery_pipeline_state(&self, job_id: JobId) -> PromotedRecoveryPipelineState {
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
        let (
            download_queue_len,
            download_queue_has_recovery,
            download_queue_promoted_recovery,
            recovery_queue_len,
            parked_promoted_recovery,
        ) = self
            .jobs
            .get(&job_id)
            .map(|state| {
                (
                    state.download_queue.len(),
                    state.download_queue.has_recovery_work(),
                    state.download_queue.count_matching(|work| {
                        work.is_recovery
                            && promoted_files.contains(&work.segment_id.file_id.file_index)
                    }),
                    state.recovery_queue.len(),
                    state.recovery_queue.count_matching(|work| {
                        work.is_recovery
                            && promoted_files.contains(&work.segment_id.file_id.file_index)
                    }),
                )
            })
            .unwrap_or((0, false, 0, 0, 0));
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
        let active_promoted_downloads = self
            .active_downloads_by_file
            .iter()
            .filter(|(file_id, _)| {
                file_id.job_id == job_id && promoted_files.contains(&file_id.file_index)
            })
            .map(|(_, count)| *count)
            .sum();
        let pending_promoted_retries = self
            .pending_retries_by_segment
            .iter()
            .filter(|(segment_id, _)| {
                segment_id.file_id.job_id == job_id
                    && promoted_files.contains(&segment_id.file_id.file_index)
            })
            .map(|(_, count)| *count)
            .sum();
        let active_promoted_decodes = self
            .active_decodes_by_file
            .iter()
            .filter(|(file_id, _)| {
                file_id.job_id == job_id && promoted_files.contains(&file_id.file_index)
            })
            .map(|(_, count)| *count)
            .sum();
        let write_buffered_promoted_recovery = self
            .write_buffers
            .iter()
            .filter(|(file_id, buffer)| {
                file_id.job_id == job_id
                    && promoted_files.contains(&file_id.file_index)
                    && buffer.buffered_len() > 0
            })
            .count();
        let unavailable_promoted_recovery_segments = self
            .unavailable_promoted_recovery_segments
            .iter()
            .filter(|segment_id| {
                segment_id.file_id.job_id == job_id
                    && promoted_files.contains(&segment_id.file_id.file_index)
            })
            .count();

        PromotedRecoveryPipelineState {
            download_queue_len,
            download_queue_has_recovery,
            download_queue_promoted_recovery,
            recovery_queue_len,
            parked_promoted_recovery,
            promoted_par2_files: promoted_files.len(),
            incomplete_promoted_par2_files,
            active_promoted_downloads,
            pending_promoted_retries,
            pending_promoted_decode,
            active_promoted_decodes,
            write_buffered_promoted_recovery,
            unavailable_promoted_recovery_segments,
        }
    }

    fn job_has_promoted_recovery_pipeline_work(&self, job_id: JobId, action: &'static str) -> bool {
        let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
        if promoted_recovery.has_pending_work() {
            debug!(
                job_id = job_id.0,
                action,
                queued_downloads = promoted_recovery.download_queue_len,
                download_queue_has_recovery = promoted_recovery.download_queue_has_recovery,
                queued_promoted_recovery = promoted_recovery.download_queue_promoted_recovery,
                parked_recovery = promoted_recovery.recovery_queue_len,
                parked_promoted_recovery = promoted_recovery.parked_promoted_recovery,
                promoted_par2_files = promoted_recovery.promoted_par2_files,
                incomplete_promoted_par2_files = promoted_recovery.incomplete_promoted_par2_files,
                active_promoted_downloads = promoted_recovery.active_promoted_downloads,
                pending_promoted_retries = promoted_recovery.pending_promoted_retries,
                pending_promoted_decode = promoted_recovery.pending_promoted_decode,
                active_promoted_decodes = promoted_recovery.active_promoted_decodes,
                write_buffered_promoted_recovery =
                    promoted_recovery.write_buffered_promoted_recovery,
                unavailable_promoted_recovery_segments =
                    promoted_recovery.unavailable_promoted_recovery_segments,
                "deferring completion work — promoted PAR2 recovery work is pending"
            );
            return true;
        }

        false
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
                | (_, CleanPar2IntegrityGate::WeakTransform) => {
                    CleanPar2IntegrityGate::WeakTransform
                }
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
        let mut occupied_filenames = HashSet::<String>::new();
        for (_, identity, _) in &file_rows {
            reserve_identity_filenames(identity, &mut occupied_filenames);
        }
        reserve_directory_filenames(&state.working_dir, &mut occupied_filenames);
        let _ = state;

        let mut renamed = 0usize;
        let mut touched_files = Vec::<NzbFileId>::new();
        let mut touched_rar_files = HashMap::<String, HashSet<String>>::new();
        for suggestion in &suggestions {
            let old = &suggestion.current_path;
            let requested_correct_name = sanitize_download_filename(&suggestion.correct_name);
            let old_name = old
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default();
            let matched = by_current
                .get(&old_name)
                .copied()
                .or_else(|| by_source.get(&old_name).copied())
                .or_else(|| by_canonical.get(&old_name).copied());
            let mut target_occupied = occupied_filenames.clone();
            if let Some((file_id, _)) = matched
                && let Some((_, identity, _)) = file_rows
                    .iter()
                    .find(|(candidate_file_id, _, _)| *candidate_file_id == file_id)
            {
                forget_identity_filenames(identity, &mut target_occupied);
            }
            let correct_name =
                allocate_unique_download_filename(&requested_correct_name, &mut target_occupied);
            let new = old.parent().unwrap().join(&correct_name);
            if old
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                == Some(correct_name.clone())
            {
                continue;
            }

            if new.exists() && !runtime_fs::paths_equivalent_for_placement(old, &new) {
                warn!(
                    job_id = job_id.0,
                    from = %old.display(),
                    to = %new.display(),
                    "PAR2 rename target already exists"
                );
                continue;
            }

            let renamed_successfully = match runtime_fs::rename_no_overwrite(old, &new) {
                Ok(()) => {
                    renamed += 1;
                    reserve_download_filename(&correct_name, &mut occupied_filenames);
                    info!(
                        job_id = job_id.0,
                        from = %old.file_name().unwrap().to_string_lossy(),
                        to = %correct_name,
                        "deobfuscated file via PAR2 metadata"
                    );
                    true
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        from = %old.display(),
                        to = %new.display(),
                        error = %error,
                        "PAR2 rename failed"
                    );
                    false
                }
            };
            if !renamed_successfully {
                continue;
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
                let classification = Self::canonical_archive_identity_from_filename(&correct_name)
                    .or(identity.classification.clone());
                if let Some(set_name) = old_rar_set_name {
                    touched_rar_files
                        .entry(set_name)
                        .or_default()
                        .insert(old_current_filename);
                }
                let mut rebound_identity = identity;
                rebound_identity.current_filename = correct_name.clone();
                rebound_identity.canonical_filename = Some(correct_name.clone());
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

    async fn run_par2_repairer(
        &mut self,
        job_id: JobId,
        par2_set: Arc<weaver_par2::Par2FileSet>,
        working_dir: std::path::PathBuf,
        repair: bool,
    ) -> Result<weaver_par2::Par2RepairOutcome, String> {
        #[cfg(test)]
        {
            if repair {
                self.par2_repairer_execute_calls += 1;
            } else {
                self.par2_repairer_analyze_calls += 1;
            }
        }

        let memory_limit = configured_par2_repair_memory_limit_bytes();
        // The analyze pass's scan carries into the execute pass so the
        // repair does not re-scan sources the analysis just hashed. The
        // engine re-stats every observed file and rescans on any drift.
        let scan_carry = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.scan_carry.clone());
        let phase_counters = repair.then(|| self.phase_begin(job_id, JobPhase::Repairing, None));
        let mut repair_task = tokio::task::spawn_blocking(move || {
            if repair {
                crate::e2e_failpoint::maybe_delay("repair.task_start");
            }
            let mut options = weaver_par2::Par2RepairerOptions::new(working_dir, Vec::new());
            options.file_set = Some((*par2_set).clone());
            options.repair = repair;
            options.memory_limit = Some(memory_limit);
            options.scan_carry = scan_carry;
            if let Some(counters) = phase_counters {
                options.progress = Some(Arc::new(move |update: weaver_par2::ProgressUpdate| {
                    if !matches!(
                        update.stage,
                        weaver_par2::ProgressStage::Repairing
                            | weaver_par2::ProgressStage::WritingRepaired
                    ) {
                        return;
                    }
                    counters
                        .completed_bytes
                        .fetch_max(update.bytes_processed, Ordering::Relaxed);
                    if let Some(total_bytes) = update.total_bytes {
                        counters
                            .total_bytes
                            .fetch_max(total_bytes, Ordering::Relaxed);
                    }
                }));
            }
            let repairer = weaver_par2::Par2Repairer::new(options);
            let (outcome, carry) = repairer
                .verify_or_repair_carrying()
                .map_err(|e| format!("PAR2 repairer failed: {e}"))?;
            if repair {
                match outcome.status {
                    weaver_par2::Par2RepairStatus::Verified
                    | weaver_par2::Par2RepairStatus::Repaired => {}
                    weaver_par2::Par2RepairStatus::RepairPossible
                    | weaver_par2::Par2RepairStatus::Insufficient
                    | weaver_par2::Par2RepairStatus::ResourceLimited => {
                        return Err(format!(
                            "PAR2 repairer did not complete repair: {:?}",
                            outcome.status
                        ));
                    }
                }
            }
            Ok((outcome, carry))
        });
        let repair_result = if repair {
            loop {
                tokio::select! {
                    result = &mut repair_task => break result,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        self.sample_phase_progress();
                    }
                }
            }
        } else {
            repair_task.await
        };

        if repair {
            self.phase_end(job_id, JobPhase::Repairing);
        }

        match repair_result {
            Ok(Ok((outcome, carry))) => {
                // Keep the analyze pass's scan for the execute pass; after a
                // repair the files just changed, so drop any stored carry.
                self.ensure_par2_runtime(job_id).scan_carry = if repair { None } else { carry };
                Ok(outcome)
            }
            Ok(Err(error)) => Err(error),
            Err(error) => Err(format!("repair task panicked: {error}")),
        }
    }

    async fn analyze_par2_with_repairer(
        &mut self,
        job_id: JobId,
        par2_set: Arc<weaver_par2::Par2FileSet>,
        working_dir: std::path::PathBuf,
        preserve_repairing_status: bool,
    ) -> Result<weaver_par2::Par2RepairOutcome, String> {
        if !preserve_repairing_status {
            self.transition_postprocessing_status(job_id, JobStatus::Verifying, Some("verifying"));
        } else {
            info!(
                job_id = job_id.0,
                "rerunning PAR2 analysis while preserving restored repair slot"
            );
        }
        self.emit_job_verification_started(job_id);
        let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
        });

        self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
        info!(job_id = job_id.0, "par2 damaged-path analysis started");

        let outcome_result = self
            .run_par2_repairer(job_id, par2_set, working_dir, false)
            .await;

        self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

        let mut outcome = outcome_result?;

        let (skipped_blocks, retained_suspect_blocks) =
            self.apply_eager_delete_exclusions(job_id, &mut outcome.verification);
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

        outcome.missing_blocks = outcome.verification.total_missing_blocks;
        self.recompute_volume_safety_from_verification(job_id, &outcome.verification);

        let _ = self.event_tx.send(PipelineEvent::JobVerificationComplete {
            job_id,
            passed: !par2_verification_needs_repair(&outcome.verification),
        });

        Ok(outcome)
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
            self.emit_job_verification_started(job_id);
            let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
            });
        }

        #[cfg(test)]
        {
            self.par2_authoritative_verify_calls += 1;
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
                passed: !par2_verification_needs_repair(&verification),
            });
        }

        Ok((verification, placement_plan))
    }

    fn emit_job_verification_started(&self, job_id: JobId) {
        let _ = self
            .event_tx
            .send(PipelineEvent::JobVerificationStarted { job_id });
    }

    async fn quick_verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<weaver_par2::Par2FileSet>,
        _working_dir: std::path::PathBuf,
    ) -> Result<Option<(weaver_par2::VerificationResult, weaver_par2::PlacementPlan)>, String> {
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
                .push((*file_id, sanitize_download_filename(&desc.filename)));
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
            let correct_filename = sanitize_download_filename(&desc.filename);

            if conflict_ids.contains(&file_id) {
                continue;
            }

            let Some(disk_name) = id_to_disk.get(&file_id).cloned() else {
                unresolved.push(file_id);
                continue;
            };

            if disk_name == correct_filename {
                exact.push(file_id);
            } else if !seen_swap.contains(&file_id) {
                let other_file_id = matches.get(correct_filename.as_str()).map(|(id, _)| *id);
                if let Some(other_id) = other_file_id
                    && other_id != file_id
                    && id_to_disk
                        .get(&other_id)
                        .is_some_and(|name| name == &correct_filename)
                {
                    let Some(other_desc) = par2_set.file_description(&other_id) else {
                        return Ok(None);
                    };
                    let other_correct_filename = sanitize_download_filename(&other_desc.filename);
                    swaps.push((
                        weaver_par2::PlacementEntry {
                            file_id,
                            current_name: disk_name.clone(),
                            correct_name: correct_filename.clone(),
                        },
                        weaver_par2::PlacementEntry {
                            file_id: other_id,
                            current_name: correct_filename.clone(),
                            correct_name: other_correct_filename,
                        },
                    ));
                    seen_swap.insert(file_id);
                    seen_swap.insert(other_id);
                } else {
                    renames.push(weaver_par2::PlacementEntry {
                        file_id,
                        current_name: disk_name.clone(),
                        correct_name: correct_filename.clone(),
                    });
                }
            }

            let slice_count = par2_set.slice_count_for_file(desc.length) as usize;
            files.push(weaver_par2::verify::FileVerification {
                file_id,
                filename: correct_filename,
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

        let complete_entries: Vec<(u32, String, Option<[u8; 16]>)> = files_to_complete
            .iter()
            .map(|(file_id, filename, _total_bytes)| {
                crate::runtime::perf_probe::record(
                    "download.file_progress.complete_file_row_covers_restart",
                    std::time::Duration::ZERO,
                );
                (
                    file_id.file_index,
                    filename.clone(),
                    Some(Self::expected_hash_for_verified_file(
                        *file_id,
                        &existing_hashes,
                    )),
                )
            })
            .collect();
        self.db_blocking(move |db| db.complete_files(job_id, &complete_entries))
            .await
            .map_err(|error| format!("failed to persist PAR2-reconciled files: {error}"))?;

        for (file_id, _filename, _total_bytes) in &files_to_complete {
            self.pending_file_progress.remove(file_id);
            self.persisted_file_progress.remove(file_id);
            self.file_hash_states.remove(file_id);
            self.expected_file_crcs.remove(file_id);
            self.file_hash_reread_required.remove(file_id);
            self.refresh_archive_state_for_completed_file(job_id, *file_id, true)
                .await;
        }

        Ok(files_to_complete.len())
    }

    async fn refresh_verified_complete_archive_topologies(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) -> usize {
        let file_ids =
            self.verified_complete_archive_file_ids_needing_refresh(job_id, verification);
        if !file_ids.is_empty() {
            info!(
                job_id = job_id.0,
                files = file_ids.len(),
                "refreshing archive topology from verified PAR2 outputs"
            );
        }
        for file_id in &file_ids {
            self.refresh_archive_state_for_completed_file(job_id, *file_id, false)
                .await;
        }
        file_ids.len()
    }

    pub(crate) fn verified_complete_archive_file_ids_needing_refresh(
        &self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) -> Vec<NzbFileId> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };

        let mut by_name = HashMap::<String, (NzbFileId, bool)>::new();
        for file in state.assembly.files() {
            if !file.is_complete() {
                continue;
            }

            let role = self.classified_role_for_file(job_id, file);
            if !Self::role_refreshes_archive_topology(&role) {
                continue;
            }

            let needs_refresh = self.archive_topology_needs_refresh(job_id, file, &role);
            let file_id = file.file_id();
            let identity = self.effective_file_identity(job_id, file_id);
            let current_filename = identity
                .as_ref()
                .map(|value| value.current_filename.clone())
                .unwrap_or_else(|| file.filename().to_string());
            Self::insert_par2_name_candidates(
                &mut by_name,
                &current_filename,
                file_id,
                needs_refresh,
            );
            if let Some(identity) = identity {
                Self::insert_par2_name_candidates(
                    &mut by_name,
                    &identity.source_filename,
                    file_id,
                    needs_refresh,
                );
                if let Some(canonical) = &identity.canonical_filename {
                    Self::insert_par2_name_candidates(
                        &mut by_name,
                        canonical,
                        file_id,
                        needs_refresh,
                    );
                }
            }
        }

        let mut matched = HashSet::new();
        for file_verification in &verification.files {
            let renamed = matches!(
                file_verification.status,
                weaver_par2::verify::FileStatus::Renamed(_)
            );
            if !matches!(
                file_verification.status,
                weaver_par2::verify::FileStatus::Complete
                    | weaver_par2::verify::FileStatus::Renamed(_)
            ) {
                continue;
            }

            for candidate_name in Self::par2_verification_candidate_names(file_verification) {
                let Some((file_id, needs_refresh)) = by_name.get(&candidate_name).copied() else {
                    continue;
                };
                if renamed || needs_refresh {
                    matched.insert(file_id);
                }
                break;
            }
        }

        let mut file_ids = matched.into_iter().collect::<Vec<_>>();
        file_ids.sort_by_key(|file_id| file_id.file_index);
        file_ids
    }

    fn role_refreshes_archive_topology(role: &weaver_model::files::FileRole) -> bool {
        matches!(
            role,
            weaver_model::files::FileRole::RarVolume { .. }
                | weaver_model::files::FileRole::SevenZipArchive
                | weaver_model::files::FileRole::SevenZipSplit { .. }
                | weaver_model::files::FileRole::SplitFile { .. }
                | weaver_model::files::FileRole::ZipArchive
                | weaver_model::files::FileRole::TarArchive
                | weaver_model::files::FileRole::TarGzArchive
                | weaver_model::files::FileRole::TarBz2Archive
                | weaver_model::files::FileRole::GzArchive
                | weaver_model::files::FileRole::DeflateArchive
                | weaver_model::files::FileRole::BrotliArchive
                | weaver_model::files::FileRole::ZstdArchive
                | weaver_model::files::FileRole::Bzip2Archive
        )
    }

    fn archive_topology_needs_refresh(
        &self,
        job_id: JobId,
        file: &crate::jobs::assembly::FileAssembly,
        role: &weaver_model::files::FileRole,
    ) -> bool {
        let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file) else {
            return false;
        };

        if matches!(role, weaver_model::files::FileRole::RarVolume { .. }) {
            return self
                .rar_sets
                .get(&(job_id, set_name))
                .is_none_or(|set_state| set_state.plan.is_none());
        }

        self.jobs
            .get(&job_id)
            .is_some_and(|state| state.assembly.archive_topology_for(&set_name).is_none())
    }

    fn insert_par2_name_candidates(
        by_name: &mut HashMap<String, (NzbFileId, bool)>,
        name: &str,
        file_id: NzbFileId,
        needs_refresh: bool,
    ) {
        for candidate in Self::name_and_basename_candidates(name) {
            by_name.entry(candidate).or_insert((file_id, needs_refresh));
        }
    }

    fn par2_verification_candidate_names(
        file_verification: &weaver_par2::verify::FileVerification,
    ) -> Vec<String> {
        let mut candidates = Self::name_and_basename_candidates(&file_verification.filename);
        if let weaver_par2::verify::FileStatus::Renamed(path) = &file_verification.status {
            Self::push_name_candidate(&mut candidates, &path.to_string_lossy());
            if let Some(filename) = path.file_name() {
                Self::push_name_candidate(&mut candidates, &filename.to_string_lossy());
            }
        }
        candidates
    }

    fn name_and_basename_candidates(name: &str) -> Vec<String> {
        let mut candidates = Vec::new();
        Self::push_name_candidate(&mut candidates, name);
        if let Some(basename) = name.rsplit(['/', '\\']).next()
            && basename != name
        {
            Self::push_name_candidate(&mut candidates, basename);
        }
        candidates
    }

    fn push_name_candidate(candidates: &mut Vec<String>, name: &str) {
        if name.is_empty() || candidates.iter().any(|candidate| candidate == name) {
            return;
        }
        candidates.push(name.to_string());
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

        self.reapply_promoted_recovery_queue(job_id);

        let par2_bypassed = self.par2_bypassed.contains(&job_id);
        let par2_loaded = self.par2_set(job_id).is_some();
        let download_pipeline_exhausted = !self.job_has_pending_download_pipeline_work(job_id);
        if download_pipeline_exhausted {
            self.emit_download_pipeline_drained_if_pending(job_id);
        }
        let only_rar_archives = self.job_has_only_rar_archives(job_id);
        let par2_primary_payload_ready = !has_incomplete_data_files || download_pipeline_exhausted;
        let par2_validation_needed = par2_loaded
            && !par2_bypassed
            && !self.par2_verified.contains(&job_id)
            && par2_primary_payload_ready
            && !self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id);
        let rar_waiting_for_missing_volumes = download_pipeline_exhausted
            && only_rar_archives
            && self.job_has_live_rar_waiting_for_missing_volumes(job_id);
        let pending_rar_refresh = download_pipeline_exhausted
            && only_rar_archives
            && self.job_has_pending_rar_refresh_for_current_sets(job_id);

        // Step 2: Check for CRC failures that need PAR2 repair.
        let has_crc_failures = self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|f| !f.is_empty());
        let clean_par2_integrity_gate = self.clean_par2_integrity_gate(job_id);
        let archive_extraction_applicable = self.extraction_readiness_for_job(job_id)
            != ExtractionReadiness::NotApplicable
            || only_rar_archives;
        let extension_repair_requested = self
            .post_processing_repair_return_to_terminal
            .contains(&job_id);
        let authoritative_par2_verification_needed = par2_validation_needed
            && (has_crc_failures
                || (has_incomplete_data_files && download_pipeline_exhausted)
                || rar_waiting_for_missing_volumes
                || matches!(current_status, JobStatus::Repairing)
                || extension_repair_requested
                || matches!(
                    clean_par2_integrity_gate,
                    CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None
                ));
        let quick_par2_verification_allowed = par2_validation_needed
            && !matches!(current_status, JobStatus::Repairing)
            && !extension_repair_requested
            && (!has_incomplete_data_files || !download_pipeline_exhausted)
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
        let exhausted_rar_activity = if download_pipeline_exhausted && only_rar_archives {
            let inflight_extractions = self
                .inflight_extractions
                .get(&job_id)
                .map_or(0, HashSet::len);
            let has_active_rar_workers = self.has_active_rar_workers(job_id);
            Some((
                inflight_extractions,
                has_active_rar_workers,
                has_active_rar_workers || inflight_extractions > 0,
            ))
        } else {
            None
        };
        let has_exhausted_rar_active_extraction_tasks =
            exhausted_rar_activity.is_some_and(|(_, _, active)| active);

        if download_pipeline_exhausted && only_rar_archives {
            let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
            let (inflight_extractions, has_active_rar_workers, has_active_extraction_tasks) =
                exhausted_rar_activity.unwrap_or((0, false, false));
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
                pending_rar_refresh,
                has_active_rar_workers,
                inflight_extractions,
                has_active_extraction_tasks,
                only_archive_residuals,
                queued_downloads = promoted_recovery.download_queue_len,
                download_queue_has_recovery = promoted_recovery.download_queue_has_recovery,
                queued_promoted_recovery = promoted_recovery.download_queue_promoted_recovery,
                parked_recovery = promoted_recovery.recovery_queue_len,
                parked_promoted_recovery = promoted_recovery.parked_promoted_recovery,
                promoted_par2_files = promoted_recovery.promoted_par2_files,
                incomplete_promoted_par2_files = promoted_recovery.incomplete_promoted_par2_files,
                active_promoted_downloads = promoted_recovery.active_promoted_downloads,
                pending_promoted_retries = promoted_recovery.pending_promoted_retries,
                pending_promoted_decode = promoted_recovery.pending_promoted_decode,
                active_promoted_decodes = promoted_recovery.active_promoted_decodes,
                write_buffered_promoted_recovery =
                    promoted_recovery.write_buffered_promoted_recovery,
                unavailable_promoted_recovery_segments =
                    promoted_recovery.unavailable_promoted_recovery_segments,
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

        if pending_rar_refresh {
            debug!(
                job_id = job_id.0,
                "deferring completion — RAR topology refresh pending"
            );
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

        if let Some(error) = self.ownerless_live_rar_plan_error_for_job(job_id) {
            self.fail_job(job_id, error);
            return;
        }

        if download_pipeline_exhausted
            && only_rar_archives
            && !has_crc_failures
            && !par2_validation_needed
            && !has_exhausted_rar_active_extraction_tasks
            && self.job_has_idle_startable_rar_work(job_id)
            && matches!(
                current_status,
                JobStatus::Downloading | JobStatus::QueuedExtract | JobStatus::Extracting
            )
        {
            info!(
                job_id = job_id.0,
                status = ?current_status,
                "restarting idle RAR extraction work"
            );
            self.try_rar_extraction(job_id).await;
            return;
        }

        if !has_crc_failures
            && self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id)
        {
            self.finalize_completed_archive_job(job_id).await;
            return;
        }

        if download_pipeline_exhausted
            && only_rar_archives
            && has_crc_failures
            && !has_exhausted_rar_active_extraction_tasks
            && matches!(
                current_status,
                JobStatus::QueuedExtract | JobStatus::Extracting
            )
        {
            info!(
                job_id = job_id.0,
                status = ?current_status,
                "normalizing idle RAR extraction status before repair evaluation"
            );
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
        }

        if rar_waiting_for_missing_volumes && self.job_has_incoherent_rar_waiting_state(job_id) {
            info!(
                job_id = job_id.0,
                "healing incoherent RAR waiting state before PAR2 verification"
            );
            self.retry_archive_extraction_after_verify_or_repair(job_id)
                .await;
            return;
        }

        if needs_completion_repair_evaluation && !par2_bypassed {
            if self.job_has_promoted_recovery_pipeline_work(job_id, "verify") {
                return;
            }

            let has_active_extraction_tasks = if download_pipeline_exhausted && only_rar_archives {
                has_exhausted_rar_active_extraction_tasks
            } else {
                self.job_has_active_extraction_tasks(job_id)
            };
            if has_active_extraction_tasks {
                info!(
                    job_id = job_id.0,
                    "deferring verify — active extraction workers"
                );
                return;
            }

            let par2_set = self.par2_set(job_id).cloned();
            if quick_par2_verification_allowed && let Some(par2_set) = par2_set.as_ref() {
                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                Self::trip_par2_verification_started_failpoint();
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
                        self.refresh_verified_complete_archive_topologies(job_id, &verification)
                            .await;
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
                                cleared, "cleared failed extractions after quick verify — retrying"
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
                if authoritative_par2_verification_needed {
                    let repair_analysis = match self
                        .analyze_par2_with_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            matches!(current_status, JobStatus::Repairing),
                        )
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(message) => {
                            warn!(job_id = job_id.0, error = %message);
                            self.fail_job(job_id, message);
                            return;
                        }
                    };
                    let verification = &repair_analysis.verification;
                    let damaged = verification.total_missing_blocks;
                    let recovery_now = repair_analysis.recovery_blocks_available;
                    let total_recovery_capacity = self.total_recovery_block_capacity(job_id);
                    let blocks_needed = match &verification.repairable {
                        weaver_par2::verify::Repairability::NotNeeded => 0,
                        weaver_par2::verify::Repairability::Repairable {
                            blocks_needed, ..
                        }
                        | weaver_par2::verify::Repairability::Insufficient {
                            blocks_needed, ..
                        } => *blocks_needed,
                        weaver_par2::verify::Repairability::ResourceLimited { .. } => 0,
                    };

                    if let weaver_par2::verify::Repairability::ResourceLimited { reason } =
                        &verification.repairable
                    {
                        let msg = format!("PAR2 verification resource limit exceeded: {reason}");
                        warn!(job_id = job_id.0, error = %msg);
                        self.fail_job(job_id, msg);
                        return;
                    }

                    if !par2_verification_needs_repair(verification) {
                        info!(job_id = job_id.0, "PAR2 analysis passed — no repair needed");

                        self.retry_par2_authoritative_identity(job_id).await;
                        self.refresh_verified_complete_archive_topologies(job_id, verification)
                            .await;
                        if let Err(error) = self
                            .reconcile_verified_par2_files(job_id, verification)
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
                                "cleared failed extractions after PAR2 analysis — retrying"
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

                    info!(
                        job_id = job_id.0,
                        damaged,
                        blocks_needed,
                        recovery_now,
                        total_recovery_capacity,
                        files_renamed = repair_analysis.files_renamed,
                        files_damaged = repair_analysis.files_damaged,
                        files_missing = repair_analysis.files_missing,
                        "PAR2 analysis — repair required"
                    );

                    if total_recovery_capacity < blocks_needed {
                        self.fail_job(
                            job_id,
                            format!(
                                "not repairable: {blocks_needed} damaged slices, only {total_recovery_capacity} recovery blocks advertised"
                            ),
                        );
                        return;
                    }

                    if recovery_now < blocks_needed {
                        let promoted = self.promote_recovery_targeted(job_id, blocks_needed);
                        let targeted_total = self.recovery_blocks_available_or_targeted(job_id);
                        let recovery_still_settling = promoted > 0
                            || self.job_has_pending_download_pipeline_work(job_id)
                            || self
                                .promoted_recovery_pipeline_state(job_id)
                                .has_pending_work();

                        if targeted_total < blocks_needed && !recovery_still_settling {
                            let msg = format!(
                                "not repairable: {blocks_needed} damaged slices, \
                                 only {targeted_total} recovery blocks available in NZB"
                            );
                            warn!(job_id = job_id.0, %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

                        info!(
                            job_id = job_id.0,
                            blocks_needed,
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

                    if matches!(
                        &verification.repairable,
                        weaver_par2::verify::Repairability::Insufficient { .. }
                            | weaver_par2::verify::Repairability::ResourceLimited { .. }
                    ) {
                        let msg = format!(
                            "not repairable: PAR2 analysis found incomplete critical repair metadata or unusable recovery despite {recovery_now} available recovery blocks"
                        );
                        warn!(job_id = job_id.0, error = %msg);
                        self.fail_job(job_id, msg);
                        return;
                    }

                    if !self.maybe_start_repair(job_id).await {
                        return;
                    }

                    match self
                        .run_par2_repairer(job_id, Arc::clone(&par2_set), working_dir.clone(), true)
                        .await
                    {
                        Ok(outcome) => {
                            self.recompute_volume_safety_from_verification(
                                job_id,
                                &outcome.verification,
                            );
                            if outcome.verification.total_missing_blocks > 0
                                || outcome.files_renamed > 0
                                || outcome.files_damaged > 0
                                || outcome.files_missing > 0
                            {
                                let msg = format!(
                                    "PAR2 repair completed but canonical outputs remain incomplete: missing_blocks={}, renamed={}, damaged={}, missing={}",
                                    outcome.verification.total_missing_blocks,
                                    outcome.files_renamed,
                                    outcome.files_damaged,
                                    outcome.files_missing
                                );
                                warn!(job_id = job_id.0, error = %msg);
                                let _ = self.event_tx.send(PipelineEvent::RepairFailed {
                                    job_id,
                                    error: msg.clone(),
                                });
                                self.fail_job(job_id, msg);
                                return;
                            }

                            let slices_repaired = outcome.recovery_blocks_used;
                            info!(
                                job_id = job_id.0,
                                status = ?outcome.status,
                                slices_repaired,
                                bytes_copied = outcome.bytes_copied,
                                bytes_reconstructed = outcome.bytes_reconstructed,
                                files_complete = outcome.files_complete,
                                files_renamed = outcome.files_renamed,
                                files_damaged = outcome.files_damaged,
                                files_missing = outcome.files_missing,
                                "PAR2 repair complete"
                            );
                            let _ = self.event_tx.send(PipelineEvent::RepairComplete {
                                job_id,
                                slices_repaired,
                            });

                            self.retry_par2_authoritative_identity(job_id).await;
                            self.refresh_verified_complete_archive_topologies(
                                job_id,
                                &outcome.verification,
                            )
                            .await;
                            if let Err(error) = self
                                .reconcile_verified_par2_files(job_id, &outcome.verification)
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

                let emit_verification_events = !has_crc_failures
                    || !self.par2_verified.contains(&job_id)
                    || authoritative_par2_verification_needed
                    || matches!(current_status, JobStatus::Repairing);
                let (verification, placement_plan) = match self
                    .verify_par2_with_placement(
                        job_id,
                        Arc::clone(&par2_set),
                        working_dir.clone(),
                        matches!(current_status, JobStatus::Repairing),
                        emit_verification_events,
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

                if let weaver_par2::verify::Repairability::ResourceLimited { reason } =
                    &verification.repairable
                {
                    let msg = format!("PAR2 verification resource limit exceeded: {reason}");
                    warn!(job_id = job_id.0, error = %msg);
                    self.fail_job(job_id, msg);
                    return;
                }

                if !par2_verification_needs_repair(&verification) {
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
                    self.refresh_verified_complete_archive_topologies(job_id, &verification)
                        .await;
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

                    if archive_extraction_applicable {
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

                    let repair_preview = match self
                        .run_par2_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            false,
                        )
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(message) => {
                            warn!(job_id = job_id.0, error = %message);
                            self.fail_job(job_id, message);
                            return;
                        }
                    };
                    let repairer_damaged = repair_preview.verification.total_missing_blocks;
                    let repairer_recovery_now = repair_preview.recovery_blocks_available;
                    if repairer_damaged != damaged || repairer_recovery_now != recovery_now {
                        info!(
                            job_id = job_id.0,
                            placement_damaged = damaged,
                            repairer_damaged,
                            placement_recovery = recovery_now,
                            repairer_recovery = repairer_recovery_now,
                            files_renamed = repair_preview.files_renamed,
                            available_blocks = repair_preview.available_blocks,
                            "PAR2 repairer scan adjusted repair requirements"
                        );
                    }
                    let damaged = repairer_damaged;
                    let recovery_now = repairer_recovery_now;

                    if let weaver_par2::verify::Repairability::ResourceLimited { reason } =
                        &repair_preview.verification.repairable
                    {
                        let msg = format!("PAR2 verification resource limit exceeded: {reason}");
                        warn!(job_id = job_id.0, error = %msg);
                        self.fail_job(job_id, msg);
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
                        let recovery_still_settling = promoted > 0
                            || self.job_has_pending_download_pipeline_work(job_id)
                            || self
                                .promoted_recovery_pipeline_state(job_id)
                                .has_pending_work();

                        // If all available/targeted recovery is still insufficient,
                        // fail immediately instead of waiting for downloads that
                        // won't help.
                        if targeted_total < damaged && !recovery_still_settling {
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

                    match self
                        .run_par2_repairer(job_id, Arc::clone(&par2_set), working_dir.clone(), true)
                        .await
                    {
                        Ok(outcome) => {
                            let slices_repaired = outcome.recovery_blocks_used;
                            info!(
                                job_id = job_id.0,
                                status = ?outcome.status,
                                slices_repaired,
                                bytes_copied = outcome.bytes_copied,
                                bytes_reconstructed = outcome.bytes_reconstructed,
                                files_complete = outcome.files_complete,
                                files_renamed = outcome.files_renamed,
                                files_damaged = outcome.files_damaged,
                                files_missing = outcome.files_missing,
                                "PAR2 repair complete"
                            );
                            let _ = self.event_tx.send(PipelineEvent::RepairComplete {
                                job_id,
                                slices_repaired,
                            });

                            self.emit_job_verification_started(job_id);
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

                            if par2_verification_needs_repair(&post_repair_verification) {
                                let msg = format!(
                                    "PAR2 repair completed but {} damaged slices or file placements remain",
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
                            self.refresh_verified_complete_archive_topologies(
                                job_id,
                                &post_repair_verification,
                            )
                            .await;
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
                if !par2_bypassed && self.promote_par2_metadata(job_id) {
                    info!(
                        job_id = job_id.0,
                        "waiting for PAR2 metadata download before repair evaluation"
                    );
                    self.transition_postprocessing_status(
                        job_id,
                        JobStatus::Downloading,
                        Some("downloading"),
                    );
                    return;
                }
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
            if !par2_bypassed && self.promote_par2_metadata(job_id) {
                info!(
                    job_id = job_id.0,
                    "waiting for PAR2 metadata download before incomplete-download failure"
                );
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Downloading,
                    Some("downloading"),
                );
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

        if self.job_has_promoted_recovery_pipeline_work(job_id, "extraction") {
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
                if self.job_has_promoted_recovery_pipeline_work(job_id, "completion") {
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
                if let Err(error) = self.start_move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                }
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

                info!(job_id = job_id.0, "extraction complete");
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionComplete { job_id });

                // Move extracted files to complete directory.
                if let Err(error) = self.start_move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                }
            }
            ExtractionReadiness::Blocked { reason } => {
                if reason.starts_with("archive topology not yet available") {
                    info!(
                        job_id = job_id.0,
                        reason = %reason,
                        "deferring completion until archive topology is available"
                    );
                    self.schedule_job_completion_check(job_id);
                    return;
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn par2_repair_memory_limit_defaults_when_unset() {
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(None),
            DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES
        );
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(Some("  ")),
            DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES
        );
    }

    #[test]
    fn par2_repair_memory_limit_accepts_positive_bytes() {
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(Some("134217728")),
            134_217_728
        );
    }

    #[test]
    fn par2_repair_memory_limit_rejects_invalid_or_zero_values() {
        let default_bytes = default_par2_repair_memory_limit_bytes();
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(Some("not-bytes")),
            default_bytes
        );
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(Some("0")),
            default_bytes
        );
    }

    #[test]
    fn incomplete_promoted_recovery_without_concrete_work_is_not_pending() {
        let state = PromotedRecoveryPipelineState {
            promoted_par2_files: 1,
            incomplete_promoted_par2_files: 1,
            ..Default::default()
        };

        assert!(!state.has_pending_work());
    }

    #[test]
    fn concrete_promoted_recovery_work_is_pending() {
        for state in [
            PromotedRecoveryPipelineState {
                download_queue_promoted_recovery: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                active_promoted_downloads: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                pending_promoted_retries: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                pending_promoted_decode: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                active_promoted_decodes: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                write_buffered_promoted_recovery: 1,
                ..Default::default()
            },
        ] {
            assert!(state.has_pending_work(), "{state:?}");
        }
    }

    #[test]
    fn parked_promoted_recovery_is_not_pending_until_reapplied() {
        let state = PromotedRecoveryPipelineState {
            parked_promoted_recovery: 1,
            ..Default::default()
        };

        assert!(!state.has_pending_work());
    }
}
