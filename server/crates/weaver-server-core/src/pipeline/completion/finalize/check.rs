use super::*;
use crate::pipeline::direct_store::wiring::{DirectDamageResolution, DirectPar2Resolution};
use crate::runtime::fs as runtime_fs;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use weaver_model::files::{
    allocate_unique_download_filename, forget_reserved_download_filename,
    reserve_download_filename, sanitize_download_filename,
};

const PAR2_REPAIR_MEMORY_LIMIT_ENV: &str = "WEAVER_PAR2_REPAIR_MEMORY_LIMIT_BYTES";
// Sizes the transient streaming repair buffers (the decode matrix has its own
// budget floor inside par2-rs). 64 MiB measured within noise of far
// larger budgets on heavily damaged sets once streaming repair got its
// batched kernels, so the default stays small and repairs coexist with
// concurrent downloads; the env override remains for tuning.
const DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
struct Par2SessionEvidenceCandidate {
    file_id: NzbFileId,
    path: std::path::PathBuf,
    logical_name: String,
    expected_length: u64,
    full_md5: Option<[u8; 16]>,
    crc32: u32,
    contiguous_assembly_proven: bool,
    bound_file_id: Option<par2_rs::FileId>,
}

type RetainedPar2SessionOutcome = (par2_rs::Par2RepairOutcome, Vec<NzbFileId>, bool);
type RetainedPar2SessionResult = Result<RetainedPar2SessionOutcome, String>;

fn committed_evidence_from_candidate(
    candidate: &Par2SessionEvidenceCandidate,
) -> Result<Option<par2_rs::CommittedFileEvidence>, String> {
    if let Some(md5) = candidate.full_md5 {
        return par2_rs::CommittedFileEvidence::from_full_md5_path(
            &candidate.path,
            &candidate.logical_name,
            candidate.expected_length,
            md5,
            candidate.bound_file_id,
        )
        .map(Some)
        .map_err(|error| format!("failed to capture PAR2 full-MD5 evidence: {error}"));
    }
    if !candidate.contiguous_assembly_proven {
        return Ok(None);
    }

    let mut first_16k = Vec::with_capacity(16 * 1024);
    File::open(&candidate.path)
        .and_then(|file| file.take(16 * 1024).read_to_end(&mut first_16k))
        .map_err(|error| {
            format!(
                "failed to capture first 16 KiB for retained PAR2 evidence {}: {error}",
                candidate.path.display()
            )
        })?;
    let proof = par2_rs::ContiguousAssemblyProof::try_new(
        candidate.expected_length,
        candidate.expected_length,
        candidate.expected_length,
        false,
        false,
        false,
        true,
    )
    .map_err(|error| format!("invalid contiguous PAR2 assembly proof: {error}"))?;
    par2_rs::CommittedFileEvidence::from_contiguous_assembly_path(
        &candidate.path,
        &candidate.logical_name,
        candidate.expected_length,
        candidate.crc32,
        par2_rs::checksum::md5(&first_16k),
        proof,
        candidate.bound_file_id,
    )
    .map(Some)
    .map_err(|error| format!("failed to capture PAR2 contiguous evidence: {error}"))
}

fn run_retained_par2_session(
    mut session: par2_rs::Par2RepairSession,
    candidates: Vec<Par2SessionEvidenceCandidate>,
    live_evidence: Vec<(std::path::PathBuf, par2_rs::SliceEvidence)>,
    repair: bool,
) -> (par2_rs::Par2RepairSession, RetainedPar2SessionResult) {
    for (path, evidence) in live_evidence {
        match session.add_slice_evidence(path, evidence) {
            Ok(()) | Err(par2_rs::Par2SessionError::EvidenceDoesNotMatch { .. }) => {}
            Err(error) => {
                return (
                    session,
                    Err(format!("failed to add live PAR2 slice evidence: {error}")),
                );
            }
        }
    }
    let mut admitted_file_ids = Vec::new();
    for candidate in candidates {
        let evidence = match committed_evidence_from_candidate(&candidate) {
            Ok(Some(evidence)) => evidence,
            Ok(None) | Err(_) => continue,
        };
        match session.add_committed_file(evidence) {
            Ok(()) => admitted_file_ids.push(candidate.file_id),
            Err(par2_rs::Par2SessionError::EvidenceDoesNotMatch { .. }) => {}
            Err(error) => {
                return (
                    session,
                    Err(format!("failed to add retained PAR2 evidence: {error}")),
                );
            }
        }
    }

    let mut retried_source_change = false;
    let mut result = if repair {
        if session.assessment().is_err() {
            session.analyze().and_then(|_| session.repair())
        } else {
            session.repair()
        }
    } else {
        session.analyze()
    };
    if should_retry_par2_source_change(&result, retried_source_change) {
        // One retry gets a fresh unresolved-only analysis. A second change is
        // returned to the caller instead of repeatedly trusting a moving path.
        retried_source_change = true;
        admitted_file_ids.clear();
        session.invalidate_all_sources();
        result = session.analyze();
        if result.is_ok() && repair {
            result = session.repair();
        }
    }
    (
        session,
        result
            .map(|outcome| (outcome, admitted_file_ids, retried_source_change))
            .map_err(|error| format!("retained PAR2 session failed: {error}")),
    )
}

fn should_retry_par2_source_change<T>(
    result: &Result<T, par2_rs::Par2SessionError>,
    already_retried: bool,
) -> bool {
    !already_retried && matches!(result, Err(par2_rs::Par2SessionError::SourceChanged { .. }))
}

fn ensure_par2_repair_completed(
    outcome: &par2_rs::Par2RepairOutcome,
    repair: bool,
) -> Result<(), String> {
    if !repair {
        return Ok(());
    }
    match outcome.status {
        par2_rs::Par2RepairStatus::Verified | par2_rs::Par2RepairStatus::Repaired => Ok(()),
        par2_rs::Par2RepairStatus::RepairPossible
        | par2_rs::Par2RepairStatus::Insufficient
        | par2_rs::Par2RepairStatus::ResourceLimited => Err(format!(
            "PAR2 repairer did not complete repair: {:?}",
            outcome.status
        )),
    }
}

fn default_par2_repair_memory_limit_bytes() -> usize {
    DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES
}

/// Missing blocks [`Pipeline::apply_direct_damage_adjustments`] moved out of (or
/// deliberately left in) a PAR2 verdict's damage count.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DamageAdjustments {
    /// Eagerly-deleted, CRC-verified RAR volumes excused as `Complete`.
    pub(crate) skipped_blocks: u32,
    /// Eagerly-deleted volumes that were *not* excused, because something still
    /// holds them suspect.
    pub(crate) retained_suspect_blocks: u32,
    /// Source volumes of a finalized direct set, excused as `Complete`.
    pub(crate) forgiven_direct_blocks: u32,
}

impl DamageAdjustments {
    pub(crate) fn any(self) -> bool {
        self.skipped_blocks > 0
            || self.retained_suspect_blocks > 0
            || self.forgiven_direct_blocks > 0
    }
}

pub(crate) fn configured_par2_repair_memory_limit_bytes() -> usize {
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

/// A clean PAR2 verdict reached without the authoritative pass, plus the two
/// failure strings that name which fast path produced it.
struct CleanPar2Verification {
    verification: par2_rs::VerificationResult,
    placement_plan: par2_rs::PlacementPlan,
    incomplete_message: &'static str,
    retry_message: &'static str,
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

fn par2_verification_needs_repair(verification: &par2_rs::VerificationResult) -> bool {
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

    /// Like [`Self::job_has_live_rar_waiting_for_missing_volumes`], but true
    /// only when the wait can no longer be answered by anything except
    /// recovery data — the distinction PAR2 repair-readiness needs and the
    /// phase alone cannot make.
    ///
    /// `WaitingForVolumes` is reported for two different situations. A volume
    /// that is genuinely absent — no parsed facts, no file on disk — is the
    /// repair case: nothing but recovery blocks can produce it. But a set mid
    /// swap-correction waits on volume *numbers* while every actual volume is
    /// present (the topology parsed them under mismatched numbering), and
    /// that wait is answered by the cached-header retry, not by PAR2. Treating
    /// the second as the first sent a swap job into damaged-path analysis,
    /// promoted 12 recovery blocks it had no use for, and emitted verification
    /// events its fixture forbids — phantom damage, wasted downloads, and a
    /// repair-first detour on a job the retry frontier was already fixing.
    ///
    /// So `WaitingForVolumes` qualifies only when some waited volume is truly
    /// absent *and* no waited volume is present — a non-empty
    /// [`present_waiting_rar_volumes`] means the volume-0 retry is still owed
    /// its chance to relink them — *and* the download pipeline is quiet.
    /// Absence only means "not coming" once nothing is en route: mid-download,
    /// a waited volume is absent simply because it has not arrived yet, and a
    /// just-demoted direct set is absent because its refetch was queued
    /// milliseconds ago. Both fired this predicate 10 seconds into a job and
    /// sent it through damaged-path analysis — phantom damage, 12 promoted
    /// recovery blocks with no use, and verification events the fixture
    /// forbids. The arm's design case — an interior volume the NZB never
    /// carried — is only *observable* at pipeline-quiet anyway, so the
    /// condition costs it nothing. (`job_has_pending_download_pipeline_work`
    /// deliberately excludes the parked recovery pool, so a job whose only
    /// remaining work is parked recovery still qualifies — that pool drains
    /// only through the promotion this predicate gates.)
    ///
    /// `AwaitingRepair` qualifies unconditionally: there the extraction
    /// machinery itself has concluded only repair moves the set forward, and
    /// its waiting list is legitimately empty.
    pub(crate) fn job_has_live_rar_waiting_for_absent_volumes(&self, job_id: JobId) -> bool {
        let current_set_names = self.current_rar_set_names_for_job(job_id);

        self.rar_sets
            .iter()
            .any(|((rar_job_id, set_name), set_state)| {
                if *rar_job_id != job_id
                    || !(current_set_names.is_empty() || current_set_names.contains(set_name))
                {
                    return false;
                }
                let Some(plan) = set_state.plan.as_ref() else {
                    return false;
                };
                match plan.phase {
                    crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair => true,
                    crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes => {
                        if self.job_has_pending_download_pipeline_work(job_id) {
                            return false;
                        }
                        let volume_paths = self.volume_paths_for_rar_set(job_id, set_name);
                        let some_volume_absent = plan.waiting_on_volumes.iter().any(|volume| {
                            !set_state.facts.contains_key(volume)
                                && !volume_paths.contains_key(volume)
                        });
                        some_volume_absent
                            && crate::pipeline::archive::topology::present_waiting_rar_volumes(
                                plan,
                                &set_state.facts,
                                &volume_paths,
                            )
                            .is_empty()
                    }
                    _ => false,
                }
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
                        promoted_files.contains(&work.segment_id.file_id.file_index)
                    }),
                    state.recovery_queue.len(),
                    state.recovery_queue.count_matching(|work| {
                        promoted_files.contains(&work.segment_id.file_id.file_index)
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

    /// Whether a wave of promoted PAR2 recovery is still moving through the
    /// pipeline for this job.
    ///
    /// `pub(crate)` because the direct-store seam asks the same question before
    /// it decides whether a damaged set should wait for recovery or demote, and
    /// "recovery is still coming" must mean exactly one thing across the two —
    /// a second, near-identical predicate is how one of them ends up waiting for
    /// work the other has already given up on.
    pub(crate) fn job_has_promoted_recovery_pipeline_work(
        &self,
        job_id: JobId,
        action: &'static str,
    ) -> bool {
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
            gate = Self::fold_integrity_gate(gate, topology_gate);
        }

        Self::fold_integrity_gate(gate, self.direct_rar_integrity_gate(job_id))
    }

    /// The operator's PAR2 repair workspace ceiling, for the one repair seam
    /// that does not go through `Par2Repairer`.
    ///
    /// Direct-store's repair-while-direct drives `plan_repair`/`execute_repair`
    /// itself, because the repairer is filesystem-bound and a virtual volume has
    /// no file to give it. Sharing the knob is the point: the limit has to mean
    /// the same thing whichever seam spends it, and a method on `Pipeline` is
    /// how that crosses the module boundary without re-exporting the parser.
    pub(in crate::pipeline) fn par2_repair_memory_limit_bytes(&self) -> usize {
        configured_par2_repair_memory_limit_bytes()
    }

    /// Job-wide fold. The **strongest** contribution wins, which is why a
    /// contribution has to be earned per archive rather than assumed per job: a
    /// `StrongDecode` from one archive suppresses the authoritative pass for
    /// every other archive in the job as well.
    fn fold_integrity_gate(
        gate: CleanPar2IntegrityGate,
        contribution: CleanPar2IntegrityGate,
    ) -> CleanPar2IntegrityGate {
        match (gate, contribution) {
            (CleanPar2IntegrityGate::StrongDecode, _)
            | (_, CleanPar2IntegrityGate::StrongDecode) => CleanPar2IntegrityGate::StrongDecode,
            (CleanPar2IntegrityGate::WeakTransform, _)
            | (_, CleanPar2IntegrityGate::WeakTransform) => CleanPar2IntegrityGate::WeakTransform,
            _ => CleanPar2IntegrityGate::None,
        }
    }

    /// What a job's **direct** RAR sets contribute to the clean-PAR2 integrity
    /// gate.
    ///
    /// A direct set never enters the archive topology by construction, so the
    /// loop above cannot see it and a job made entirely of direct sets reads
    /// `None` — which forces the authoritative pass, whose repair branch
    /// materializes every still-routing set before handing the repairer files it
    /// can write into. While the job is live that costs nothing, because live
    /// PAR2 verifies from the decode buffer and short-circuits the pass. **After
    /// a restart there is no decode buffer**: no article of a byte-complete set
    /// arrives, live PAR2 has nothing to hash, and a set that is byte-perfect on
    /// disk is materialized and redownloaded anyway.
    ///
    /// A **restored** RAR set has the same claim to `StrongDecode` a
    /// conventionally extracted one does, and for a stronger reason than
    /// routing alone: its checkpointed bytes were re-read off disk this run and
    /// re-composed through the member CRC32s by the gate re-arm, and its
    /// refetched bytes went through the router's gate on the way in. Three
    /// predicates make that claim honest, and each one is load-bearing:
    ///
    /// - **The job is only RAR archives.** The fold above is job-wide and the
    ///   strongest contribution wins, so a direct RAR set in a job that also
    ///   holds a conventional Split or Tar would suppress the authoritative pass
    ///   for the *split archive* — an archive whose integrity nothing in this job
    ///   has checked. A mixed job contributes nothing here.
    /// - **Every set was restored, and none is demoted.** A set this run
    ///   downloaded live is deliberately left alone: live PAR2 hashes its
    ///   articles out of the decode buffer, which catches damage in the *volume*
    ///   space that no member checksum can see — a corrupted recovery record, say
    ///   — and that detection must not be traded away. Live sets keep reaching the
    ///   authoritative pass exactly as before; this is only about the sets live
    ///   PAR2 could never have seen, because not one article of them arrived.
    /// - **No set is carrying restart-seeded coverage.** Bytes restored from a
    ///   checkpoint are covered and *unverified* until the re-arm re-reads
    ///   them; a set still holding them has decoded nothing and may not claim
    ///   decode strength. The re-arm runs at the download/verify boundary, so
    ///   by the time this matters a healthy set has cleared it — and one that
    ///   could not has demoted.
    ///
    /// # What this does and does not vouch for
    ///
    /// The member CRC32s vouch for the **member payloads** — the bytes that
    /// become output — and not for the volume-space bytes PAR2 describes. A
    /// volume's envelope regions (headers, a recovery record) are covered by no
    /// member checksum. That is the same trade the conventional RAR path makes
    /// when it contributes `StrongDecode`: there too the guarantee is that the
    /// extractor's own per-file CRC32 passed, not that every byte of every volume
    /// file matched its PAR2 description.
    fn direct_rar_integrity_gate(&self, job_id: JobId) -> CleanPar2IntegrityGate {
        let sets = self.direct_store.sets_for(job_id);
        if sets.is_empty() {
            return CleanPar2IntegrityGate::None;
        }
        if !self.job_has_only_rar_archives(job_id) {
            return CleanPar2IntegrityGate::None;
        }
        if !sets.iter().all(|set| {
            set.was_restored() && !set.is_demoted() && !set.has_restart_seeded_coverage()
        }) {
            return CleanPar2IntegrityGate::None;
        }
        CleanPar2IntegrityGate::StrongDecode
    }

    /// [`Self::direct_rar_integrity_gate`] as a bool, so the predicate can be
    /// pinned on its own.
    ///
    /// It is worth a direct test rather than only an end-to-end one: the
    /// contribution is job-wide and the strongest wins, so the interesting case —
    /// a mixed job that must contribute *nothing* — is a **negative**, and a
    /// negative asserted through a whole job gate passes just as well when the
    /// gate never got that far.
    #[cfg(test)]
    pub(crate) fn direct_rar_contributes_strong_decode(&self, job_id: JobId) -> bool {
        matches!(
            self.direct_rar_integrity_gate(job_id),
            CleanPar2IntegrityGate::StrongDecode
        )
    }

    async fn load_existing_complete_file_hashes(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, [u8; 16]>, String> {
        self.db_blocking(move |db| db.load_complete_file_hashes(job_id))
            .await
            .map_err(|error| format!("failed to load completed-file hashes: {error}"))
    }

    async fn par2_session_evidence_candidates(
        &self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Result<Vec<Par2SessionEvidenceCandidate>, String> {
        let completed_hashes = self.load_existing_complete_file_hashes(job_id).await?;
        let Some(runtime) = self.par2_runtime(job_id) else {
            return Ok(Vec::new());
        };
        let completed_checksums = runtime.completed_checksums.clone();
        let already_seeded = runtime.session_evidence_file_ids.clone();
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(Vec::new());
        };

        let mut candidates = Vec::new();
        for file in state.assembly.files() {
            let file_id = file.file_id();
            if !file.is_complete() || already_seeded.contains(&file_id) {
                continue;
            }

            let current_filename = self
                .effective_file_identity(job_id, file_id)
                .map(|identity| identity.current_filename)
                .unwrap_or_else(|| file.filename().to_string());
            let expected_length = file.total_bytes();
            let checksum = completed_checksums.get(&file_id).copied();
            let full_md5 = checksum
                .and_then(|checksum| checksum.md5)
                .or_else(|| completed_hashes.get(&file_id.file_index).copied());
            let bound_candidates = par2_set
                .recovery_file_ids
                .iter()
                .chain(par2_set.non_recovery_file_ids.iter())
                .filter_map(|par2_file_id| par2_set.file_description(par2_file_id))
                .filter(|description| {
                    description.length == expected_length
                        && match full_md5 {
                            Some(md5) => description.hash_full == md5,
                            None => {
                                sanitize_download_filename(&description.filename)
                                    == current_filename
                            }
                        }
                })
                .map(|description| description.file_id)
                .collect::<Vec<_>>();
            let bound_file_id = (bound_candidates.len() == 1).then_some(bound_candidates[0]);
            candidates.push(Par2SessionEvidenceCandidate {
                file_id,
                path: state.working_dir.join(&current_filename),
                logical_name: current_filename,
                expected_length,
                full_md5,
                crc32: checksum.map_or(0, |checksum| checksum.crc32),
                contiguous_assembly_proven: checksum.is_some_and(|checksum| {
                    checksum.all_parts_crc_verified
                        && file.received_bytes() == expected_length
                        && !file.has_duplicate_segments()
                        && !file.has_length_mismatch()
                }),
                bound_file_id,
            });
        }
        Ok(candidates)
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

        let suggestions = match par2_rs::scan_for_renames(&rename_dir, &par2) {
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
            // Renames move the bytes live verification bound to a name, so the
            // job's live state is retired rather than re-resolved.
            self.live_par2.remove_job(job_id);
            self.block_crcs.forget_job(job_id);
            info!(job_id = job_id.0, renamed, "PAR2 deobfuscation complete");
        }

        renamed
    }

    async fn run_par2_repairer(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        repair: bool,
    ) -> Result<par2_rs::Par2RepairOutcome, String> {
        #[cfg(test)]
        {
            if repair {
                self.par2_repairer_execute_calls += 1;
            } else {
                self.par2_repairer_analyze_calls += 1;
            }
        }

        if repair {
            // A repair rewrites bytes the live verifier never saw, so its
            // block state is retired rather than trusted afterwards.
            self.live_par2.remove_job(job_id);
            self.block_crcs.forget_job(job_id);
        }

        let memory_limit = configured_par2_repair_memory_limit_bytes();
        let phase_counters = repair.then(|| self.phase_begin(job_id, JobPhase::Repairing, None));
        let session_progress = phase_counters.as_ref().map(|counters| {
            let counters = Arc::clone(counters);
            Arc::new(move |update: par2_rs::ProgressUpdate| {
                if !matches!(
                    update.stage,
                    par2_rs::ProgressStage::Repairing | par2_rs::ProgressStage::WritingRepaired
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
            }) as par2_rs::ProgressCallback
        });

        let retained_session = match self
            .take_or_open_par2_repair_session(
                job_id,
                working_dir.clone(),
                memory_limit,
                session_progress,
                // By this point the repairer reads and writes real files: any
                // set still routing here materialized before it arrived.
                None,
            )
            .await
        {
            Ok(session) => session,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "retained PAR2 session unavailable; using one-shot repairer");
                None
            }
        };
        if let Some((session, newly_opened)) = retained_session {
            if newly_opened {
                self.ensure_par2_runtime(job_id)
                    .session_evidence_file_ids
                    .clear();
            }
            let candidates = match self
                .par2_session_evidence_candidates(job_id, &par2_set)
                .await
            {
                Ok(candidates) => candidates,
                Err(error) => {
                    self.restore_par2_repair_session(job_id, session);
                    if repair {
                        self.phase_end(job_id, JobPhase::Repairing);
                    }
                    return Err(error);
                }
            };
            self.settle_live_par2_job(job_id).await;
            let live_evidence = self.live_par2_strong_evidence(job_id);
            let mut repair_task = tokio::task::spawn_blocking(move || {
                if repair {
                    crate::e2e_failpoint::maybe_delay("repair.task_start");
                }
                run_retained_par2_session(session, candidates, live_evidence, repair)
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
            return match repair_result {
                Ok((session, Ok((outcome, admitted_file_ids, retried_source_change)))) => {
                    self.restore_par2_repair_session(job_id, session);
                    let runtime = self.ensure_par2_runtime(job_id);
                    if repair || retried_source_change {
                        runtime.session_evidence_file_ids.clear();
                        if repair && let Some(session) = runtime.session.as_mut() {
                            session.invalidate_all_sources();
                        }
                    } else {
                        runtime.session_evidence_file_ids.extend(admitted_file_ids);
                    }
                    ensure_par2_repair_completed(&outcome, repair)?;
                    Ok(outcome)
                }
                Ok((session, Err(error))) => {
                    self.restore_par2_repair_session(job_id, session);
                    Err(error)
                }
                Err(error) => Err(format!("retained PAR2 session task panicked: {error}")),
            };
        }

        let mut repair_task = tokio::task::spawn_blocking(move || {
            if repair {
                crate::e2e_failpoint::maybe_delay("repair.task_start");
            }
            let mut options = par2_rs::Par2RepairerOptions::new(working_dir, Vec::new());
            options.file_set = Some((*par2_set).clone());
            options.repair = repair;
            options.memory_limit = Some(memory_limit);
            if let Some(counters) = phase_counters {
                options.progress = Some(Arc::new(move |update: par2_rs::ProgressUpdate| {
                    if !matches!(
                        update.stage,
                        par2_rs::ProgressStage::Repairing | par2_rs::ProgressStage::WritingRepaired
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
            let repairer = par2_rs::Par2Repairer::new(options);
            let (outcome, _) = repairer
                .verify_or_repair_carrying()
                .map_err(|e| format!("PAR2 repairer failed: {e}"))?;
            ensure_par2_repair_completed(&outcome, repair)?;
            Ok(outcome)
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
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(format!("repair task panicked: {error}")),
        }
    }

    async fn analyze_par2_with_repairer(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        preserve_repairing_status: bool,
    ) -> Result<par2_rs::Par2RepairOutcome, String> {
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
        // The same forgiveness the verify path applies. This
        // pass is reachable with a finalized direct set whenever `par2_verified`
        // was cleared underneath it — an extension asking for PAR re-entry does
        // exactly that — and a finalized set's source volumes are absent by
        // design.
        let forgiven_direct_blocks =
            self.forgive_finalized_direct_volumes(job_id, &mut outcome.verification);
        if forgiven_direct_blocks > 0 {
            info!(
                job_id = job_id.0,
                forgiven_direct_blocks, "excluded finalized direct-store volumes from damage count"
            );
        }

        outcome.missing_blocks = outcome.verification.total_missing_blocks;
        self.recompute_volume_safety_from_verification(job_id, &outcome.verification);

        let passed = !par2_verification_needs_repair(&outcome.verification);
        self.note_job_verification_result(
            job_id,
            passed,
            outcome.verification.total_missing_blocks,
        );
        let _ = self
            .event_tx
            .send(PipelineEvent::JobVerificationComplete { job_id, passed });

        Ok(outcome)
    }

    async fn verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        preserve_repairing_status: bool,
        emit_events: bool,
    ) -> Result<(par2_rs::VerificationResult, par2_rs::PlacementPlan), String> {
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
        // A direct set's source volumes are not on disk, so the
        // pass reads them through the hybrid virtual-volume provider. Everything
        // else in the job — the PAR2 volumes, any conventional data file, a
        // demoted set's materialized volumes — keeps reading through
        // `PlacementFileAccess` exactly as before.
        let direct = self.direct_par2_overlay(job_id);
        let verify_result = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || {
                let mut plan = par2_rs::scan_placement(&verify_dir, &par2_set)
                    .map_err(|error| format!("placement scan failed: {error}"))?;
                if !plan.conflicts.is_empty() {
                    return Err(format!(
                        "placement scan found {} conflicting file matches",
                        plan.conflicts.len()
                    ));
                }

                let Some(direct) = direct else {
                    let file_access =
                        par2_rs::PlacementFileAccess::from_plan(verify_dir, &par2_set, &plan);
                    return Ok((par2_rs::verify_all(&par2_set, &file_access), plan));
                };

                // The scan walked a directory the direct volumes are absent
                // from, so it left every one of them `unresolved`. Reclassifying
                // them `exact` changes no behaviour today — the plan's only
                // consumers are `PlacementFileAccess::from_plan`,
                // `apply_placement_plan` and the plan log, and all three read
                // `swaps` and `renames` only, never `exact` or `unresolved`. It
                // is kept as defence: a direct volume *is* at its declared name
                // by construction (its identity is resolved by that name, in
                // `direct_par2_overlay`) and has no file to move, so the moment
                // anything does start reading these two lists it must see the
                // classification a correctly placed volume would have had, not
                // the one that invites a rename of a file that is not there.
                let direct_ids: HashSet<par2_rs::FileId> = direct
                    .volumes
                    .iter()
                    .map(|volume| volume.par2_file_id)
                    .collect();
                plan.unresolved
                    .retain(|file_id| !direct_ids.contains(file_id));
                for file_id in &direct.volumes {
                    if !plan.exact.contains(&file_id.par2_file_id) {
                        plan.exact.push(file_id.par2_file_id);
                    }
                }

                let inner = par2_rs::PlacementFileAccess::from_plan(verify_dir, &par2_set, &plan);
                // Taken before the provider is moved into the
                // access: for an encrypted set the pass reads posted bytes the
                // overlay re-derives, and these are the only numbers that say
                // what that cost. Zero for every unencrypted set.
                let cipher = direct.provider.cipher_counters();
                let file_access =
                    crate::pipeline::direct_store::par2_access::DirectVolumeFileAccess::new(
                        inner,
                        direct.provider,
                        &direct.volumes,
                    );
                let counters = file_access.counters();
                let verification = par2_rs::verify_all(&par2_set, &file_access);
                debug!(
                    virtual_volumes = direct.volumes.len(),
                    sequential_opens = counters.sequential_opens(),
                    // Volumes whose interior holes made the
                    // sequential sweep a lie, so the pass took the per-slice
                    // ranged path instead. Non-zero means the job paid for an
                    // accurate damage count, which is what a repair is sized
                    // from.
                    sequential_refusals = counters.sequential_refusals(),
                    ranged_reads = counters.ranged_reads(),
                    // The checkpoint bound, in production. `chained_bytes`
                    // is what checkpoint misses cost — bytes re-encrypted only
                    // to reach a ranged read's CBC seed and then discarded — and
                    // `seeded_from_start` counts the reads that had no reachable
                    // checkpoint at all. Both large against `reencrypted_bytes`
                    // means the checkpoint stride is too wide for this shape.
                    reencrypted_bytes = cipher.reencrypted_bytes(),
                    chained_bytes = cipher.chained_bytes(),
                    seeded_from_checkpoint = cipher.seeded_from_checkpoint(),
                    seeded_from_start = cipher.seeded_from_start(),
                    // A read the overlay would not answer: unreproducible posted
                    // bytes, which the pass sees as damage.
                    cipher_refusals = cipher.refusals(),
                    "authoritative PAR2 pass read a direct set's volumes virtually"
                );
                Ok((verification, plan))
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

        let adjustments = self.apply_direct_damage_adjustments(job_id, &mut verification);
        if adjustments.skipped_blocks > 0 {
            info!(
                job_id = job_id.0,
                skipped_blocks = adjustments.skipped_blocks,
                "excluded eagerly-deleted CRC-verified volumes from damage count"
            );
        }
        if adjustments.retained_suspect_blocks > 0 {
            info!(
                job_id = job_id.0,
                retained_suspect_blocks = adjustments.retained_suspect_blocks,
                "retained suspect eagerly-deleted volumes in damage count"
            );
        }
        if adjustments.forgiven_direct_blocks > 0 {
            info!(
                job_id = job_id.0,
                forgiven_direct_blocks = adjustments.forgiven_direct_blocks,
                "excluded finalized direct-store volumes from damage count"
            );
        }

        self.recompute_volume_safety_from_verification(job_id, &verification);

        if emit_events {
            let passed = !par2_verification_needs_repair(&verification);
            self.note_job_verification_result(job_id, passed, verification.total_missing_blocks);
            let _ = self
                .event_tx
                .send(PipelineEvent::JobVerificationComplete { job_id, passed });
        }

        Ok((verification, placement_plan))
    }

    /// What [`Pipeline::apply_direct_damage_adjustments`] moved, so each caller
    /// can log it in its own voice.
    ///
    /// Counts rather than a bool: "how many blocks were forgiven" is the number
    /// the operator needs to tell a job that was never damaged from one whose
    /// damage was excused.
    pub(crate) fn apply_direct_damage_adjustments(
        &self,
        job_id: JobId,
        verification: &mut par2_rs::VerificationResult,
    ) -> DamageAdjustments {
        let (skipped_blocks, retained_suspect_blocks) =
            self.apply_eager_delete_exclusions(job_id, verification);
        // A *finalized* direct set's source volumes were never written and never
        // will be: its partials are at their destinations and its envelopes are
        // gone, and the whole-member CRC32 gates plus this job's own earlier
        // PAR2 verdict are what let it commit in the first place. Every later
        // pass — and one conventional set failing extraction after the direct
        // set finalized is enough to cause one — would otherwise report those
        // volumes missing and either fail the job as unrepairable or have the
        // repairer write source volumes the job already finished without. Same
        // justification, same shape and the same position in the pass as
        // `apply_eager_delete_exclusions` above.
        let forgiven_direct_blocks = self.forgive_finalized_direct_volumes(job_id, verification);
        DamageAdjustments {
            skipped_blocks,
            retained_suspect_blocks,
            forgiven_direct_blocks,
        }
    }

    /// Records the job as PAR2-verified and releases any direct set that was
    /// holding its virtual volume image for the verifier.
    ///
    /// A par2-bearing direct set stays uncommitted until here: its envelopes and
    /// `.direct.partial`s *are* the source volumes, and finalization renames the
    /// partials away and deletes the envelopes. Releasing at the same statement
    /// that records the verdict is what keeps the two from drifting — there is
    /// no path that marks a job verified without also letting its sets commit.
    async fn mark_par2_verified(&mut self, job_id: JobId) {
        self.par2_verified.insert(job_id);
        self.finalize_ready_direct_sets(job_id).await;
    }

    fn emit_job_verification_started(&mut self, job_id: JobId) {
        // Low-frequency: a job enters PAR2 verification a handful of times, so
        // arming the stage timer here costs one clock read per pass and never
        // touches an article path.
        self.note_stage_started(
            job_id,
            crate::operations::instrumentation::JobStageKind::Verify,
        );
        let _ = self
            .event_tx
            .send(PipelineEvent::JobVerificationStarted { job_id });
    }

    /// Fold one job-level PAR2 verification verdict into the lifecycle metrics
    /// and close the verify stage timer.
    ///
    /// Low-frequency: one call per verification pass, never per segment. The
    /// four-way label is derived from what the pass actually produced — a pass
    /// that needs repair and found nothing at all on disk is `missing`, one
    /// that needs repair with blocks present is `damaged`.
    fn note_job_verification_result(&mut self, job_id: JobId, passed: bool, missing_blocks: u32) {
        use crate::operations::instrumentation::{JobStageKind, VerificationOutcomeKind};
        let outcome = if passed {
            VerificationOutcomeKind::Intact
        } else if missing_blocks > 0 {
            VerificationOutcomeKind::Missing
        } else {
            VerificationOutcomeKind::Damaged
        };
        // Claim the job before recording, so a later `unverifiable` fallback
        // cannot add a second row for a job an actual pass already ruled on.
        // Re-verification of the same job (verify, repair, verify again) is a
        // real second outcome and still counts, which is why the claim gates
        // only the fallback and not this.
        self.jobs_with_verification_outcome.insert(job_id);
        self.metrics.job_lifecycle.note_verification(outcome);
        self.note_stage_finished(job_id, JobStageKind::Verify);
    }

    /// Record that this job ended with no PAR2 verdict to be had.
    ///
    /// A job with no recovery set can never produce `intact`, `damaged` or
    /// `missing`: there is nothing to verify the payload against. Without
    /// this, such jobs contribute nothing at all to
    /// `weaver_verifications_total`, and the ratio of verified to unverified
    /// downloads — the thing an operator actually wants from that series — is
    /// unanswerable.
    ///
    /// Low-frequency: at most one call per job, at the terminal transition.
    /// The guard set is the same per-job set a real verdict claims, so the two
    /// can never both fire for one job.
    pub(in crate::pipeline) fn note_job_verification_unavailable(&mut self, job_id: JobId) {
        use crate::operations::instrumentation::VerificationOutcomeKind;
        if self.jobs_with_verification_outcome.insert(job_id) {
            self.metrics
                .job_lifecycle
                .note_verification(VerificationOutcomeKind::Unverifiable);
        }
    }

    /// Called at the two terminal transitions — the final move and job failure
    /// — to attribute a job that never had a recovery set.
    pub(in crate::pipeline) fn note_job_unverifiable_if_no_par2_set(&mut self, job_id: JobId) {
        if self.par2_set(job_id).is_none() {
            self.note_job_verification_unavailable(job_id);
        }
    }

    async fn quick_verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        _working_dir: std::path::PathBuf,
    ) -> Result<Option<(par2_rs::VerificationResult, par2_rs::PlacementPlan)>, String> {
        let completed_hashes = self.load_existing_complete_file_hashes(job_id).await?;
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(None);
        };

        let mut current_hashes_by_name = HashMap::<String, [u8; 16]>::new();
        // Live evidence may stand in for persisted whole-file hashes only
        // when it proves every described slice with CRC32+MD5 and a stable,
        // complete assembly identity. Any incomplete/ambiguous live state is
        // ignored and the existing persisted-MD5 quick path remains intact.
        let live_bindings = self.live_par2_complete_bindings(job_id);
        let mut live_matches_by_name = HashMap::<String, (par2_rs::FileId, String)>::new();
        for file in state.assembly.files() {
            if !file.is_complete() {
                continue;
            }

            let file_id = file.file_id();
            let identity = self.effective_file_identity(job_id, file_id);
            let current_filename = identity
                .as_ref()
                .map(|value| value.current_filename.as_str())
                .unwrap_or_else(|| file.filename());
            if let Some(par2_file_id) = live_bindings
                .as_ref()
                .and_then(|bindings| bindings.get(&file_id))
                .copied()
            {
                let Some(desc) = par2_set.file_description(&par2_file_id) else {
                    return Ok(None);
                };
                live_matches_by_name.insert(
                    current_filename.to_string(),
                    (par2_file_id, sanitize_download_filename(&desc.filename)),
                );
                continue;
            }
            if live_bindings.is_some() {
                // The live eligibility proof already covered every described
                // file. This is an auxiliary NZB file, not a PAR2 source.
                continue;
            }
            let Some(file_hash) = completed_hashes.get(&file_id.file_index).copied() else {
                return Ok(None);
            };
            current_hashes_by_name.insert(current_filename.to_string(), file_hash);
        }

        let mut all_file_ids: Vec<par2_rs::FileId> = par2_set
            .recovery_file_ids
            .iter()
            .chain(par2_set.non_recovery_file_ids.iter())
            .copied()
            .collect();
        all_file_ids.sort_unstable_by_key(|file_id| *file_id.as_bytes());
        all_file_ids.dedup();

        let mut hash_lookup = HashMap::<[u8; 16], Vec<(par2_rs::FileId, String)>>::new();
        for file_id in &all_file_ids {
            let Some(desc) = par2_set.file_description(file_id) else {
                continue;
            };
            hash_lookup
                .entry(desc.hash_full)
                .or_default()
                .push((*file_id, sanitize_download_filename(&desc.filename)));
        }

        let mut matches = live_matches_by_name;
        let mut match_counts = HashMap::<par2_rs::FileId, u32>::new();
        for (file_id, _) in matches.values() {
            *match_counts.entry(*file_id).or_default() += 1;
        }
        for (current_name, file_hash) in current_hashes_by_name {
            let Some(candidates) = hash_lookup.get(&file_hash) else {
                continue;
            };

            if let Some((file_id, correct_name)) = candidates.first() {
                matches.insert(current_name.clone(), (*file_id, correct_name.clone()));
                *match_counts.entry(*file_id).or_default() += 1;
            }
        }

        let conflict_ids: HashSet<par2_rs::FileId> = match_counts
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(file_id, _)| *file_id)
            .collect();
        matches.retain(|_, (file_id, _)| !conflict_ids.contains(file_id));

        let mut id_to_disk = HashMap::<par2_rs::FileId, String>::new();
        for (disk_name, (file_id, _)) in &matches {
            id_to_disk.insert(*file_id, disk_name.clone());
        }

        let mut files = Vec::new();
        let mut exact = Vec::new();
        let mut swaps = Vec::new();
        let mut renames = Vec::new();
        let mut unresolved = Vec::new();
        let mut seen_swap = HashSet::<par2_rs::FileId>::new();
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
                        par2_rs::PlacementEntry {
                            file_id,
                            current_name: disk_name.clone(),
                            correct_name: correct_filename.clone(),
                        },
                        par2_rs::PlacementEntry {
                            file_id: other_id,
                            current_name: correct_filename.clone(),
                            correct_name: other_correct_filename,
                        },
                    ));
                    seen_swap.insert(file_id);
                    seen_swap.insert(other_id);
                } else {
                    renames.push(par2_rs::PlacementEntry {
                        file_id,
                        current_name: disk_name.clone(),
                        correct_name: correct_filename.clone(),
                    });
                }
            }

            let slice_count = par2_set.slice_count_for_file(desc.length) as usize;
            files.push(par2_rs::verify::FileVerification {
                file_id,
                filename: correct_filename,
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: vec![true; slice_count],
                missing_slice_count: 0,
            });
        }

        if !conflict_ids.is_empty() || !unresolved.is_empty() {
            return Ok(None);
        }

        Ok(Some((
            par2_rs::VerificationResult {
                files,
                recovery_blocks_available: par2_set.recovery_block_count(),
                total_missing_blocks: 0,
                repairable: par2_rs::verify::Repairability::NotNeeded,
            },
            par2_rs::PlacementPlan {
                exact,
                swaps,
                renames,
                unresolved,
                conflicts: conflict_ids.into_iter().collect(),
            },
        )))
    }

    /// Shared completion handling for a clean PAR2 verdict.
    ///
    /// Every fast path that proves a job clean without the authoritative pass
    /// funnels through here, so their downstream effects — placement, identity,
    /// reconciliation, `par2_verified`, status transitions — are the same code,
    /// not parallel copies that can drift.
    async fn finish_clean_par2_verification(
        &mut self,
        job_id: JobId,
        working_dir: std::path::PathBuf,
        outcome: CleanPar2Verification,
        has_crc_failures: bool,
        archive_extraction_applicable: bool,
    ) {
        let CleanPar2Verification {
            verification,
            placement_plan,
            incomplete_message,
            retry_message,
        } = outcome;
        Self::log_placement_plan(job_id, &placement_plan);

        self.try_deobfuscate_files_with_par2(job_id).await;
        if let Err(error) = self
            .apply_placement_plan_for_retry_or_repair(job_id, working_dir, &placement_plan)
            .await
        {
            self.fail_job(job_id, error);
            return;
        }
        self.retry_par2_authoritative_identity(job_id).await;
        // Before refreshing topologies, adopt any RAR volume PAR2 rebuilt that
        // the NZB never carried. 0.7.9 calls this at each of its repair exits;
        // 0.8 funnels them through here, so one call covers them all. Without
        // it a repaired interior volume sits on disk under a name the assembly
        // has never heard of, extraction goes on waiting for it, and the repair
        // that just succeeded changes nothing.
        if let Err(error) = self
            .register_verified_par2_rar_outputs(job_id, &verification)
            .await
        {
            self.fail_job(job_id, error);
            return;
        }
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
            warn!(job_id = job_id.0, error = %incomplete_message);
            self.fail_job(job_id, incomplete_message.to_string());
            return;
        }

        self.mark_par2_verified(job_id).await;

        if has_crc_failures {
            if self.normalization_retried.contains(&job_id) {
                let msg = "clean PAR2 verification but extraction still failing after retry";
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg.to_string());
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
                let msg =
                    format!("invalid RAR retry frontier after placement correction: {reason}");
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg);
                return;
            }

            info!(
                job_id = job_id.0,
                cleared, retry_message, "cleared failed extractions after clean PAR2 verification"
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
    }

    async fn reconcile_verified_par2_files(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
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
                    par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
                ) {
                    continue;
                }

                let mut candidate_names = vec![file_verification.filename.clone()];
                if let par2_rs::verify::FileStatus::Renamed(path) = &file_verification.status
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
        verification: &par2_rs::VerificationResult,
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

    /// Register RAR volumes that PAR2 *rebuilt* and that the NZB never carried.
    ///
    /// Ported from release-0.7.9. A missing interior volume repaired from
    /// recovery blocks lands on disk under a name the job's assembly has never
    /// heard of, so extraction goes on believing the volume is absent and the
    /// repair achieves nothing. This walks a clean verification result, keeps
    /// the entries that are RAR volumes the job does not already know, and
    /// persists their parsed facts against the set.
    ///
    /// Paths are checked before use: a PAR2 description names its own file, so
    /// an absolute path or one containing `..` is refused rather than resolved.
    pub(crate) async fn register_verified_par2_rar_outputs(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> Result<usize, String> {
        let registered_filenames = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .map(|file| file.filename().to_string())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        let mut registered = 0;
        for file in &verification.files {
            if !matches!(file.status, par2_rs::verify::FileStatus::Complete)
                || registered_filenames.contains(&file.filename)
            {
                continue;
            }

            let path = Path::new(&file.filename);
            if file.filename.is_empty()
                || !path.is_relative()
                || !path
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_)))
            {
                return Err(format!(
                    "refusing unsafe PAR2-verified RAR output path {:?}",
                    file.filename
                ));
            }

            let role = weaver_model::files::FileRole::from_filename(&file.filename);
            let weaver_model::files::FileRole::RarVolume { volume_number } = role else {
                continue;
            };
            let Some(set_name) = weaver_model::files::archive_base_name(&file.filename, &role)
            else {
                continue;
            };
            let path = self
                .resolve_job_input_path(job_id, &file.filename)
                .ok_or_else(|| {
                    format!(
                        "PAR2 verified RAR output {} has no active job directory",
                        file.filename
                    )
                })?;
            if !path.is_file() {
                return Err(format!(
                    "PAR2 verified RAR output {} is missing from staging",
                    path.display()
                ));
            }

            let password_candidates = self.archive_password_candidates_for_set(job_id, &set_name);
            let facts = Self::parse_rar_volume_facts_from_path(path, password_candidates)
                .await
                .map_err(|error| {
                    format!(
                        "failed to parse PAR2-verified RAR output {}: {error}",
                        file.filename
                    )
                })?;
            if self.persist_rar_volume_facts(
                job_id,
                &set_name,
                &file.filename,
                Some(volume_number),
                facts,
            )? {
                registered += 1;
            }
        }

        if registered > 0 {
            info!(
                job_id = job_id.0,
                registered, "registered PAR2-verified RAR outputs absent from the NZB"
            );
        }
        Ok(registered)
    }

    pub(crate) fn verified_complete_archive_file_ids_needing_refresh(
        &self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
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
                par2_rs::verify::FileStatus::Renamed(_)
            );
            if !matches!(
                file_verification.status,
                par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
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
        file_verification: &par2_rs::verify::FileVerification,
    ) -> Vec<String> {
        let mut candidates = Self::name_and_basename_candidates(&file_verification.filename);
        if let par2_rs::verify::FileStatus::Renamed(path) = &file_verification.status {
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

        // A direct set whose job carries no PAR2 set to verify
        // against — bypassed, or no recovery article ever landed — would
        // otherwise wait forever for a verdict that is not coming. Asked here,
        // once per completion check, because this is where the job's PAR2 state
        // is settled; `mark_par2_verified` covers the verdict case.
        self.finalize_ready_direct_sets(job_id).await;

        let par2_bypassed = self.par2_bypassed.contains(&job_id);
        let par2_loaded = self.par2_set(job_id).is_some();
        let download_pipeline_exhausted = !self.job_has_pending_download_pipeline_work(job_id);
        if download_pipeline_exhausted {
            self.emit_download_pipeline_drained_if_pending(job_id);
            if has_incomplete_data_files {
                // The download pass is over and files are still short of
                // segments: this is where "cannot be assembled from articles"
                // becomes a fact rather than a race with work still in flight.
                self.note_incomplete_files_after_download_drain(job_id);
            }
        }
        let only_rar_archives = self.job_has_only_rar_archives(job_id);

        // A RAR set that PAR2 owes a verdict on, in the two shapes that reach
        // this while the downloads have not drained. Ordinarily an incomplete
        // data file defers validation until they do, which is right — PAR2
        // cannot tell a file still arriving from a damaged one — but in both
        // of these the wait is for something that is not coming, and the
        // authoritative pass is what says so.
        // Hoisted here by the 0.7.9 port: the repair-readiness predicates below
        // need it, and it used to be defined further down at "Step 2".
        let has_crc_failures = self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|failed| !failed.is_empty());
        // A clean PAR2 verdict says the described files hashed correctly. It does
        // not say the archives open. When extraction fails afterwards *and* the
        // set is left waiting on a volume, that verdict is stale evidence and
        // PAR2 has to be allowed to rule again.
        //
        // Declining 0.7.9's relaxation outright (2efa19d9) livelocked every PAR2
        // repair scenario in the corpus. `has_crc_failures` does re-open repair
        // *evaluation* — which is why the unit suite was satisfied — but every
        // PAR2 route inside that block is gated on `par2_validation_needed`, so
        // the check re-entered forever with nothing able to act: 1865 identical
        // completion checkpoints on one job, no repair, no verdict.
        //
        // Narrower than 0.7.9's blanket `!verified`, deliberately, and on the
        // *absent*-volumes predicate rather than the waiting phase: a job that
        // merely finalized its direct sets has a conventional failed member and
        // no set waiting on anything, so its verdict stands and the
        // repair-first branch stays skipped
        // (`a_finalized_direct_sets_volumes_are_not_missing_on_a_later_par2_pass`),
        // and a swap-corrected set whose volumes are all present is left to the
        // retry frontier rather than dragged back through PAR2.
        let par2_verdict_stale_after_failed_extraction = self.par2_verified.contains(&job_id)
            && has_crc_failures
            && self.job_has_live_rar_waiting_for_absent_volumes(job_id);
        let par2_verdict_open =
            !self.par2_verified.contains(&job_id) || par2_verdict_stale_after_failed_extraction;
        let par2_may_still_rule = par2_loaded && !par2_bypassed && par2_verdict_open;
        let extraction_settled = !self.job_has_active_extraction_tasks(job_id);
        // One: extraction was attempted and failed. The archives cannot be
        // opened now, and the reason may well be the volume that never came.
        // This is the same question the scheduler asks before it latches the
        // failed member out, and it is asked through the same predicate so the
        // two cannot answer differently — a latch with no verdict coming is a
        // stalled job.
        let failed_rar_par2_repair_ready = self.par2_recovery_evaluation_pending(job_id);
        // Two: extraction was never attempted, because a volume is missing.
        // Nothing lands in `failed_extractions` for this shape — there was no
        // failure, only an absence — so it needs naming separately or a job
        // whose interior volume never posted waits forever with the recovery
        // blocks that would rebuild it sitting right there.
        let missing_rar_volume_par2_repair_ready = par2_may_still_rule
            && extraction_settled
            && self.job_has_live_rar_waiting_for_absent_volumes(job_id)
            && (self.recovery_blocks_available_or_targeted(job_id) > 0
                || self
                    .jobs
                    .get(&job_id)
                    .is_some_and(|state| state.recovery_queue.has_recovery_work()));
        // 0.8 only: a direct set still taking articles has holes where its
        // outstanding ranges will go, and PAR2 cannot tell a hole from
        // corruption. Declaring a verdict owed while one is filling walks the
        // job into the authoritative branch, which then defers on exactly this
        // condition and returns — so the pass never runs and the escape has
        // achieved nothing but a later re-check.
        let rar_par2_repair_ready = (failed_rar_par2_repair_ready
            || missing_rar_volume_par2_repair_ready)
            && self.direct_sets_ready_for_authoritative_par2(job_id);

        let par2_primary_payload_ready =
            !has_incomplete_data_files || download_pipeline_exhausted || rar_par2_repair_ready;
        let par2_validation_needed = par2_loaded
            && !par2_bypassed
            // The other half of the coupled pair above. 0.7.9 relaxes this to
            // `(rar_par2_repair_ready || !verified)`; this is the same
            // relaxation narrowed to the state that actually needs it — a
            // verified job whose extraction failed and whose set is still
            // waiting on a volume. Moving only one of the two halves does
            // nothing: `rar_par2_repair_ready` cannot become true while
            // `par2_may_still_rule` is false, and validation cannot run while
            // this is false.
            && par2_verdict_open
            && par2_primary_payload_ready
            // The residuals check reads a job still missing archive pieces as
            // nothing to validate yet. That is the very state a repair-ready
            // RAR set is in, so it cannot be what turns validation away.
            && (rar_par2_repair_ready
                || !self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id));
        let rar_waiting_for_missing_volumes = download_pipeline_exhausted
            && only_rar_archives
            && self.job_has_live_rar_waiting_for_missing_volumes(job_id);
        let pending_rar_refresh = download_pipeline_exhausted
            && only_rar_archives
            && self.job_has_pending_rar_refresh_for_current_sets(job_id);

        // Step 2: Check for CRC failures that need PAR2 repair.
        let clean_par2_integrity_gate = self.clean_par2_integrity_gate(job_id);
        let archive_extraction_applicable = self.extraction_readiness_for_job(job_id)
            != ExtractionReadiness::NotApplicable
            || only_rar_archives;
        let extension_repair_requested = self
            .post_processing_repair_return_to_terminal
            .contains(&job_id);
        let authoritative_par2_verification_needed = par2_validation_needed
            && (rar_par2_repair_ready
                || has_crc_failures
                || (has_incomplete_data_files && download_pipeline_exhausted)
                || rar_waiting_for_missing_volumes
                || matches!(current_status, JobStatus::Repairing)
                || extension_repair_requested
                || matches!(
                    clean_par2_integrity_gate,
                    CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None
                ));
        // Shared by every fast path that skips the authoritative pass, so the
        // live short-circuit can never fire where the quick path would be
        // refused.
        let clean_par2_integrity_gate_allows_fast_path = match clean_par2_integrity_gate {
            CleanPar2IntegrityGate::StrongDecode => {
                only_rar_archives && (has_crc_failures || rar_waiting_for_missing_volumes)
            }
            CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None => true,
        };
        let quick_par2_verification_allowed = par2_validation_needed
            && !matches!(current_status, JobStatus::Repairing)
            && !extension_repair_requested
            // A set waiting on a volume that never posted needs the
            // *authoritative* analyzer: only that names the exact missing
            // blocks a recovery promotion has to target, and the quick pass has
            // nothing to answer from — those bytes are absent, not wrong.
            //
            // Narrower than 0.7.9's, deliberately. A *failed* extraction whose
            // files are all present is a case 0.8's quick pass settles on its
            // own — swap correction, and the eager-delete retry frontier — and
            // forcing the authoritative pass there loses both.
            && !missing_rar_volume_par2_repair_ready
            && (!has_incomplete_data_files || !download_pipeline_exhausted)
            && clean_par2_integrity_gate_allows_fast_path;
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
            // ...unless PAR2 owes this job's RAR sets a verdict. Waiting for
            // downloads that are not coming is how such a job stalls, and this
            // return is what keeps PAR2 from ever being asked.
            && !rar_par2_repair_ready
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

        // Standalone `.rev` files can rebuild a volume that never posted just as
        // well as one that failed CRC. Gating this on `has_crc_failures` alone
        // meant a set whose hole was known *before* any extraction was tried
        // — the common case, since the topology usually knows the missing
        // index from the neighbouring volumes' headers — went straight to "no
        // retryable work remains" without the recovery volumes ever being read.
        if download_pipeline_exhausted
            && only_rar_archives
            && (has_crc_failures || rar_waiting_for_missing_volumes)
            && !self.job_has_active_extraction_tasks(job_id)
            && self.job_has_rar_recovery_volume_files(job_id)
        {
            match self.try_restore_rar_recovery_volumes(job_id).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => warn!(
                    job_id = job_id.0,
                    error = %error,
                    "RAR recovery-volume restore failed; continuing with normal repair evaluation"
                ),
            }
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
            // Restored from 0.7.9 after an e2e run showed the cost of removing
            // it: one job re-ran `par2 damaged-path analysis` 21 times while
            // waiting for its promoted recovery to arrive, at ~64 slow par2
            // file scans, starving 75 other jobs into harness timeouts. The
            // analysis is what is expensive, and it has to be skipped *before*
            // it runs — 0.8's `job_has_promoted_recovery_pipeline_work` guard
            // below is too late to prevent the rescan.
            // Each pass of this analysis is a full, slow PAR2 scan, so the only
            // question that matters is whether re-running it can learn anything
            // new. Four e2e runs mapped the edges:
            //
            //  - Defer while the *parked* pool (`recovery_queue`) is non-empty
            //    and the job deadlocks: it is this analysis that promotes parked
            //    blocks, so the pool never drains (~1800 identical completion
            //    checkpoints per job, every PAR2-repair scenario in the corpus).
            //  - Defer on any narrower signal — recovery on the wire, parked
            //    work after a promotion — and heavy damage storms: the verdict
            //    keeps changing while the payload is still arriving, so the
            //    check re-scans on every tick that slips through (30+ slow
            //    scans, suite starved to 21 of 92).
            //
            // The scan can learn something exactly twice: once before any wave
            // has been promoted (it is the pass that decides what to promote),
            // and once after everything that could change its answer has
            // landed. In between — the wave still arriving, or the payload
            // still filling the very holes being counted — the verdict cannot
            // move, and the pass is pure cost. `job_has_pending_download_
            // pipeline_work` deliberately excludes the parked pool, so this
            // cannot re-create the deadlock above: a job whose only remaining
            // work is parked recovery is not "pending" here, analyses run, and
            // promotion drains the pool.
            let promoted_recovery_state = self.promoted_recovery_pipeline_state(job_id);
            if rar_par2_repair_ready
                && promoted_recovery_state.promoted_par2_files > 0
                && (promoted_recovery_state.incomplete_promoted_par2_files > 0
                    || self.job_has_pending_download_pipeline_work(job_id))
            {
                debug!(
                    job_id = job_id.0,
                    promoted_par2_files = promoted_recovery_state.promoted_par2_files,
                    incomplete_promoted_par2_files =
                        promoted_recovery_state.incomplete_promoted_par2_files,
                    "deferring repair evaluation — promoted recovery or payload is still arriving"
                );
                return;
            }
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

            // A suspect volume is damage the full pass deliberately keeps:
            // `apply_eager_delete_exclusions` retains its missing blocks
            // instead of forgiving them. Live block state says nothing about
            // that, so any suspect volume refuses every live short-circuit —
            // both the whole-pass skip below and the quick path's, which the
            // stateful session also lets stand in for the full
            // pass.
            let no_suspect_volumes = self.suspect_rar_volumes_for_job(job_id).is_empty();

            // Live in-stream block verification. Deliberately
            // conservative: it only applies to a clean, fully downloaded job
            // that is not mid-repair, and only when every recovery-set file is
            // matched with every one of its blocks proven Ok. Every other case
            // — a single Pending or Bad block, an unmatched file, an inactive
            // verifier — falls through to the passes below unchanged.
            let live_par2_short_circuit_allowed = self.live_par2.enabled()
                && par2_validation_needed
                && !has_crc_failures
                && !has_incomplete_data_files
                && !rar_waiting_for_missing_volumes
                && !extension_repair_requested
                && !matches!(current_status, JobStatus::Repairing)
                && clean_par2_integrity_gate_allows_fast_path
                && no_suspect_volumes;
            if live_par2_short_circuit_allowed && par2_set.is_some() {
                self.settle_live_par2_job(job_id).await;
                if let Some((verification, placement_plan)) =
                    self.live_par2_clean_verification(job_id).await
                {
                    let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                    Self::trip_par2_verification_started_failpoint();
                    self.live_par2.note_full_verify_skip();
                    let live_metrics = self.live_par2.metrics();
                    info!(
                        job_id = job_id.0,
                        files = verification.files.len(),
                        blocks_in_stream = live_metrics.strongly_verified_slices,
                        blocks_backfilled = live_metrics.backfill_reads,
                        blocks_settled = live_metrics.settle_reads,
                        partials_abandoned = live_metrics.partial_fallbacks,
                        partial_bytes_peak = live_metrics.disk_read_bytes,
                        "live PAR2 block verification proved the set clean — skipping the full verify pass"
                    );
                    self.finish_clean_par2_verification(
                        job_id,
                        working_dir,
                        CleanPar2Verification {
                            verification,
                            placement_plan,
                            incomplete_message:
                                "clean live PAR2 verification but job still has incomplete data files after reconciliation",
                            retry_message:
                                "cleared failed extractions after live verify — retrying",
                        },
                        has_crc_failures,
                        archive_extraction_applicable,
                    )
                    .await;
                    return;
                }
            }

            if quick_par2_verification_allowed && let Some(par2_set) = par2_set.as_ref() {
                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                self.settle_live_par2_job(job_id).await;
                // What this records is "live evidence stood in for the full
                // pass", so it takes the same evidence the full short-circuit
                // above demands — complete bindings alone say nothing about
                // the length the file actually has on disk. When that check
                // fails, the quick pass still succeeds on its own persisted
                // checksums; the credit simply does not belong to live.
                let live_short_circuit =
                    no_suspect_volumes && self.live_par2_clean_verification(job_id).await.is_some();
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
                        if live_short_circuit {
                            self.live_par2.note_full_verify_skip();
                        }
                        info!(
                            job_id = job_id.0,
                            "quick PAR2 verification passed for clean exhausted job"
                        );
                        self.finish_clean_par2_verification(
                            job_id,
                            working_dir.clone(),
                            CleanPar2Verification {
                                verification,
                                placement_plan,
                                incomplete_message:
                                    "clean PAR2 quick verification but job still has incomplete data files after reconciliation",
                                retry_message:
                                    "cleared failed extractions after quick verify — retrying",
                            },
                            has_crc_failures,
                            archive_extraction_applicable,
                        )
                        .await;
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
                        self.mark_par2_verified(job_id).await;

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
                // Two direct-store preconditions for *any*
                // authoritative pass below, whichever branch it takes. Both are
                // no-ops for a job with no live direct set, so a conventional
                // job reaches the same code it always did.
                //
                // A set that is still receiving articles has holes where its
                // outstanding ranges will go, and PAR2 cannot tell a hole from
                // corruption — so the pass waits for the same thing the branch
                // above waits for in `par2_primary_payload_ready`: the payload,
                // or the download pipeline draining. `needs_completion_repair_
                // evaluation` can be true well before either (another set's
                // extraction failing is enough), which is how a healthy
                // mid-download set would otherwise be demoted for damage that
                // is just bytes in flight.
                if !self.direct_sets_ready_for_authoritative_par2(job_id) {
                    debug!(
                        job_id = job_id.0,
                        "deferring PAR2 verification — a direct set is still downloading"
                    );
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                // A volume with no unambiguous PAR2 identity cannot be put
                // behind the overlay *or* attributed back to its set
                // afterwards, so it leaves direct mode before the pass rather
                // than being discovered as unattributable damage inside it.
                if self.demote_unbindable_direct_sets(job_id).await {
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                // A live direct set reaches this branch as a matter of course,
                // not as defence in depth: it contributes nothing to
                // `clean_par2_integrity_gate` — a direct set never enters the
                // archive topology — so a `None` gate sends it straight here,
                // damaged or not. The repairer cannot read a virtual volume, so
                // before it is allowed to force a whole-set materialization the
                // sets get one direct-aware verdict of their own: damage repairs
                // in place, and a clean verdict skips the repairer so the
                // ordinary verify path below can record it.
                let mut run_par2_repairer = authoritative_par2_verification_needed;
                if authoritative_par2_verification_needed {
                    match self
                        .resolve_direct_sets_before_par2_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                        )
                        .await
                    {
                        DirectPar2Resolution::Repaired => {
                            self.schedule_job_completion_check(job_id);
                            return;
                        }
                        DirectPar2Resolution::Clean => run_par2_repairer = false,
                        DirectPar2Resolution::Deferred => {
                            // The same wait the analysis below performs when it
                            // promotes recovery, reported the same way: the job
                            // is downloading again, because that is literally
                            // what it is doing.
                            //
                            // Deliberately *not* re-armed. Nothing this gate can
                            // do moves the answer — the sets are waiting on
                            // articles — and each lap costs a full PAR2 scan.
                            // The re-arm comes from the recovery itself: a
                            // completing PAR2 file merges its slices and checks
                            // the job, which is the one event that changes the
                            // verdict.
                            self.transition_postprocessing_status(
                                job_id,
                                JobStatus::Downloading,
                                Some("downloading"),
                            );
                            return;
                        }
                        DirectPar2Resolution::Unresolved => {}
                    }
                }
                if run_par2_repairer {
                    // The repairer reads and *writes* volume files through
                    // `DiskFileAccess`, which a virtual volume has none of. So
                    // any set still routing here — one whose repair refused, or
                    // one whose damage is not the reason the job is in this
                    // branch — materializes first, and the repairer sees real
                    // files.
                    if self.demote_live_direct_sets_for_par2_repair(job_id).await {
                        // Re-armed rather than left to the 30 s reconcile
                        // sweep: the job is one pass away from its verdict and
                        // the materialized volumes are already on disk.
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
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
                        par2_rs::verify::Repairability::NotNeeded => 0,
                        par2_rs::verify::Repairability::Repairable { blocks_needed, .. }
                        | par2_rs::verify::Repairability::Insufficient { blocks_needed, .. } => {
                            *blocks_needed
                        }
                        par2_rs::verify::Repairability::ResourceLimited { .. } => 0,
                    };

                    if let par2_rs::verify::Repairability::ResourceLimited { reason } =
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
                        self.mark_par2_verified(job_id).await;

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
                        par2_rs::verify::Repairability::Insufficient { .. }
                            | par2_rs::verify::Repairability::ResourceLimited { .. }
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
                                // Low-frequency: one observation per job-level repair, never on a
                                // per-segment path. Records the metric next to the event that already
                                // announces the same fact.
                                self.metrics.job_lifecycle.note_repair(
                                    crate::operations::instrumentation::StageOutcomeKind::Failed,
                                    0,
                                );
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
                            // Low-frequency: one observation per job-level repair, never on a
                            // per-segment path. Records the metric next to the event that already
                            // announces the same fact.
                            self.metrics.job_lifecycle.note_repair(
                                crate::operations::instrumentation::StageOutcomeKind::Complete,
                                u64::from(slices_repaired),
                            );
                            let _ = self.event_tx.send(PipelineEvent::RepairComplete {
                                job_id,
                                slices_repaired,
                            });

                            self.retry_par2_authoritative_identity(job_id).await;
                            if let Err(error) = self
                                .register_verified_par2_rar_outputs(job_id, &outcome.verification)
                                .await
                            {
                                self.fail_job(job_id, error);
                                return;
                            }
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
                            self.mark_par2_verified(job_id).await;
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
                            }

                            // A repaired interior RAR volume is only visible to the
                            // incremental scheduler after its synchronous refresh.
                            // Do that before scheduling another completion check, or a
                            // stale WaitingForVolumes plan can re-enter PAR2 forever.
                            if has_crc_failures
                                || self.job_has_live_rar_waiting_for_missing_volumes(job_id)
                            {
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
                            // Low-frequency: one observation per job-level repair, never on a
                            // per-segment path. Records the metric next to the event that already
                            // announces the same fact.
                            self.metrics.job_lifecycle.note_repair(
                                crate::operations::instrumentation::StageOutcomeKind::Failed,
                                0,
                            );
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
                // Damage on a virtual volume has nothing to repair *into* — the
                // bytes live in a member's partial and an envelope, and a
                // recovered slice belongs to neither — so the set materializes
                // **only its damaged volumes** into scratch, repairs those
                // while every clean volume is still read virtually, routes the
                // repaired spans back through the router and throws the scratch
                // away. The set stays direct and loses no output. Every refusal
                // along the way falls back to the whole-set demotion, which
                // materializes everything and hands the job to the conventional
                // repair path. Either way the job's next move is this gate
                // again, over bytes that changed.
                match self
                    .resolve_direct_sets_with_par2_damage(job_id, &verification)
                    .await
                {
                    DirectDamageResolution::Resolved => {
                        // Re-armed rather than left to the 30 s reconcile sweep:
                        // the repaired bytes are already in the partials, or the
                        // demotion has already materialized (or queued the
                        // refetch of) the volumes, so the next pass can run
                        // immediately.
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
                    DirectDamageResolution::Deferred => {
                        // Damage the merged recovery cannot cover but the
                        // recovery *set* can. The sets keep their outputs and
                        // their virtual volumes while the targeted recovery
                        // downloads; the job is reported as what it is doing.
                        // No re-arm — the completing recovery file merges its
                        // slices and checks the job itself, and a lap of this
                        // gate in the meantime is a slow scan that cannot reach
                        // a different answer.
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                        return;
                    }
                    DirectDamageResolution::Unresolved => {}
                }
                let damaged = verification.total_missing_blocks;
                let recovery_now = verification.recovery_blocks_available;
                let total_recovery_capacity = self.total_recovery_block_capacity(job_id);

                if let par2_rs::verify::Repairability::ResourceLimited { reason } =
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
                    self.mark_par2_verified(job_id).await;

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

                    if let par2_rs::verify::Repairability::ResourceLimited { reason } =
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
                            // Low-frequency: one observation per job-level repair, never on a
                            // per-segment path. Records the metric next to the event that already
                            // announces the same fact.
                            self.metrics.job_lifecycle.note_repair(
                                crate::operations::instrumentation::StageOutcomeKind::Complete,
                                u64::from(slices_repaired),
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
                            if let Err(error) = self
                                .register_verified_par2_rar_outputs(
                                    job_id,
                                    &post_repair_verification,
                                )
                                .await
                            {
                                self.fail_job(job_id, error);
                                return;
                            }
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
                            self.mark_par2_verified(job_id).await;
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
                            }

                            // A repaired interior RAR volume is only visible to the
                            // incremental scheduler after its synchronous refresh.
                            // Do that before scheduling another completion check, or a
                            // stale WaitingForVolumes plan can re-enter PAR2 forever.
                            if has_crc_failures
                                || self.job_has_live_rar_waiting_for_missing_volumes(job_id)
                            {
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
                            // Low-frequency: one observation per job-level repair, never on a
                            // per-segment path. Records the metric next to the event that already
                            // announces the same fact.
                            self.metrics.job_lifecycle.note_repair(
                                crate::operations::instrumentation::StageOutcomeKind::Failed,
                                0,
                            );
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
                // Low-frequency: one observation per job-level extraction, never on a
                // per-segment path. Records the metric next to the event that already
                // announces the same fact.
                self.metrics.job_lifecycle.note_extraction(
                    crate::operations::instrumentation::StageOutcomeKind::Complete,
                );
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
    fn quick_proof_uses_crc_verified_contiguous_assembly() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("payload.bin");
        let bytes = b"crc-verified-contiguous-payload";
        std::fs::write(&path, bytes).unwrap();
        let candidate = Par2SessionEvidenceCandidate {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            path,
            logical_name: "payload.bin".to_string(),
            expected_length: bytes.len() as u64,
            full_md5: None,
            crc32: par2_rs::checksum::crc32(bytes),
            contiguous_assembly_proven: true,
            bound_file_id: None,
        };

        let evidence = committed_evidence_from_candidate(&candidate)
            .unwrap()
            .expect("contiguous CRC-verified assembly should be quick-proved");
        assert!(evidence.assembly_proof().is_some());
        assert_eq!(evidence.assembly_crc32(), Some(candidate.crc32));
    }

    #[test]
    fn quick_proof_refuses_unproven_assembly() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("payload.bin");
        std::fs::write(&path, b"payload").unwrap();
        let candidate = Par2SessionEvidenceCandidate {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            path,
            logical_name: "payload.bin".to_string(),
            expected_length: 7,
            full_md5: None,
            crc32: 0,
            contiguous_assembly_proven: false,
            bound_file_id: None,
        };

        assert!(
            committed_evidence_from_candidate(&candidate)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn source_changed_retry_is_limited_to_one_fresh_analysis() {
        let changed: Result<(), par2_rs::Par2SessionError> =
            Err(par2_rs::Par2SessionError::SourceChanged {
                path: std::path::PathBuf::from("payload.bin"),
            });
        assert!(should_retry_par2_source_change(&changed, false));
        assert!(!should_retry_par2_source_change(&changed, true));
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
