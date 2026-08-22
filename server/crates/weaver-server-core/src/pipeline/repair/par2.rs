use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::*;
use crate::jobs::record::{ActiveFileIdentity, FileIdentitySource};
use crate::runtime::fs as runtime_fs;
use weaver_model::files::{
    allocate_unique_download_filename, forget_reserved_download_filename,
    reserve_download_filename, sanitize_download_filename,
};

pub(crate) const PROMOTED_RECOVERY_PRIORITY: u32 = 2;
const PAR2_PACKET_ALIGNMENT: u64 = 4;
const PAR2_RECOVERY_PACKET_OVERHEAD: u64 = 68; // 64-byte header + 4-byte exponent
const PAR2_RETAINED_SESSION_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const STATEFUL_PAR2_SESSION_ENV: &str = "WEAVER_STATEFUL_PAR2_SESSION";

fn parse_stateful_par2_session_enabled(raw: Option<&str>) -> bool {
    matches!(
        raw.map(str::trim),
        Some("1")
            | Some("true")
            | Some("TRUE")
            | Some("yes")
            | Some("YES")
            | Some("on")
            | Some("ON")
    )
}

fn stateful_par2_session_enabled() -> bool {
    parse_stateful_par2_session_enabled(std::env::var(STATEFUL_PAR2_SESSION_ENV).ok().as_deref())
}

pub(in crate::pipeline) fn select_par2_session_eviction<I>(
    sessions: I,
    protected: (JobId, par2_rs::RecoverySetId),
) -> Option<(JobId, par2_rs::RecoverySetId)>
where
    I: IntoIterator<Item = ((JobId, par2_rs::RecoverySetId), bool, Option<Instant>)>,
{
    let mut protected_available = false;
    let mut oldest_unprotected: Option<((JobId, par2_rs::RecoverySetId), Option<Instant>)> = None;
    for (key, has_session, last_used) in sessions {
        if !has_session {
            continue;
        }
        if key == protected {
            protected_available = true;
            continue;
        }
        if oldest_unprotected
            .as_ref()
            .is_none_or(|(_, oldest)| last_used < *oldest)
        {
            oldest_unprotected = Some((key, last_used));
        }
    }
    oldest_unprotected
        .map(|(key, _)| key)
        .or_else(|| protected_available.then_some(protected))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryCountSource {
    Exact,
    Calibrated,
    FilenameFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveryCandidate {
    file_index: u32,
    blocks: u32,
    total_bytes: u64,
    source: RecoveryCountSource,
}

/// Packets from one recovery set found while parsing an index file.
struct ParsedPar2Set {
    set_id: par2_rs::RecoverySetId,
    packets: Vec<par2_rs::Packet>,
}

fn par2_recovery_packet_size(slice_size: u64) -> u64 {
    let raw = slice_size.saturating_add(PAR2_RECOVERY_PACKET_OVERHEAD);
    let rem = raw % PAR2_PACKET_ALIGNMENT;
    if rem == 0 {
        raw
    } else {
        raw + (PAR2_PACKET_ALIGNMENT - rem)
    }
}

fn recovery_file_bytes(spec: &JobSpec, file_index: u32) -> Option<u64> {
    let file = spec.files.get(file_index as usize)?;
    Some(
        file.segments
            .iter()
            .map(|segment| segment.bytes as u64)
            .sum(),
    )
}

fn recovery_file_role(spec: &JobSpec, file_index: u32) -> Option<weaver_model::files::FileRole> {
    spec.files
        .get(file_index as usize)
        .map(|file| file.role.clone())
}

/// The name a PAR2 file's whole collection shares: the filename with its
/// `.par2` extension and any `.volNNN+CCC` part removed.
///
/// This is how a recovery volume is grouped onto a set when nothing has parsed
/// its packets yet — `holiday.mkv.vol00+08.par2` and `holiday.mkv.par2` both
/// reduce to `holiday.mkv`, so the volume is recognizably part of that
/// collection before a byte of it has been read. Case is folded because the
/// convention is not consistently cased on the wire, and the separator may be
/// `+` or `-` for the same reason.
///
/// `None` means the name does not follow the convention at all, which callers
/// read as "no opinion" rather than as "not this set" — an obfuscated posting
/// names nothing recognizably, and refusing its volumes would cost it every
/// recovery block it has.
fn par2_set_base_name(filename: &str) -> Option<String> {
    let lower = filename.trim().to_ascii_lowercase();
    let stem = lower.strip_suffix(".par2")?;
    let base = match stem.rfind('.') {
        Some(dot) if stem[dot..].starts_with(".vol") && stem[dot + 4..].contains(['+', '-']) => {
            &stem[..dot]
        }
        _ => stem,
    };
    (!base.is_empty()).then(|| base.to_string())
}

fn reserve_identity_filenames(
    identity: &ActiveFileIdentity,
    occupied_filenames: &mut HashSet<String>,
) {
    reserve_download_filename(&identity.source_filename, occupied_filenames);
    reserve_download_filename(&identity.current_filename, occupied_filenames);
    if let Some(canonical) = identity.canonical_filename.as_ref() {
        reserve_download_filename(canonical, occupied_filenames);
    }
}

fn forget_identity_filenames(
    identity: &ActiveFileIdentity,
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

fn compare_selection(
    lhs: (u64, usize, u32, &[u32]),
    rhs: (u64, usize, u32, &[u32]),
) -> std::cmp::Ordering {
    lhs.0
        .cmp(&rhs.0)
        .then_with(|| lhs.1.cmp(&rhs.1))
        .then_with(|| lhs.2.cmp(&rhs.2))
        .then_with(|| lhs.3.cmp(rhs.3))
}

fn select_recovery_file_indices(
    candidates: &[RecoveryCandidate],
    remaining_needed: u32,
) -> Vec<u32> {
    if remaining_needed == 0 || candidates.is_empty() {
        return Vec::new();
    }

    let mut ordered = candidates.to_vec();
    ordered.sort_by_key(|candidate| (candidate.total_bytes, candidate.file_index));

    if ordered.len() > 24 {
        let mut selected = Vec::new();
        let mut covered = 0u32;
        for candidate in ordered {
            if covered >= remaining_needed {
                break;
            }
            selected.push(candidate.file_index);
            covered = covered.saturating_add(candidate.blocks);
        }
        return selected;
    }

    let mut best: Option<(u64, usize, u32, Vec<u32>)> = None;
    let total_masks = 1u128 << ordered.len();
    for mask in 1u128..total_masks {
        let mut covered = 0u32;
        let mut total_bytes = 0u64;
        let mut file_indices = Vec::new();

        for (idx, candidate) in ordered.iter().enumerate() {
            if (mask & (1u128 << idx)) == 0 {
                continue;
            }
            covered = covered.saturating_add(candidate.blocks);
            total_bytes = total_bytes.saturating_add(candidate.total_bytes);
            file_indices.push(candidate.file_index);
        }

        if covered < remaining_needed {
            continue;
        }

        let overshoot = covered - remaining_needed;
        let current = (total_bytes, file_indices.len(), overshoot, file_indices);
        let replace = best.as_ref().is_none_or(|existing| {
            compare_selection(
                (current.0, current.1, current.2, current.3.as_slice()),
                (existing.0, existing.1, existing.2, existing.3.as_slice()),
            ) == std::cmp::Ordering::Less
        });
        if replace {
            best = Some(current);
        }
    }

    best.map(|(_, _, _, file_indices)| file_indices)
        .unwrap_or_else(|| {
            ordered
                .into_iter()
                .map(|candidate| candidate.file_index)
                .collect()
        })
}

fn unique_par2_binding_candidate(candidates: &[par2_rs::FileId]) -> Option<par2_rs::FileId> {
    let [candidate] = candidates else {
        return None;
    };
    Some(*candidate)
}

/// The one recovery-set description a pipeline file unambiguously identifies.
#[derive(Debug, Clone)]
pub(crate) struct Par2FileBinding {
    pub(crate) recovery_set_id: par2_rs::RecoverySetId,
    pub(crate) par2_file_id: par2_rs::FileId,
    pub(crate) described_length: u64,
    pub(crate) path: PathBuf,
    pub(crate) is_complete: bool,
}

impl Pipeline {
    pub(crate) fn canonical_archive_identity_from_filename(
        filename: &str,
    ) -> Option<crate::jobs::assembly::DetectedArchiveIdentity> {
        let role = weaver_model::files::FileRole::from_filename(filename);
        let set_name = weaver_model::files::archive_base_name(filename, &role)?;
        match role {
            weaver_model::files::FileRole::RarVolume { volume_number } => {
                Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name,
                    volume_index: Some(volume_number),
                })
            }
            weaver_model::files::FileRole::SevenZipArchive => {
                Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSingle,
                    set_name,
                    volume_index: None,
                })
            }
            weaver_model::files::FileRole::SevenZipSplit { number } => {
                Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSplit,
                    set_name,
                    volume_index: Some(number),
                })
            }
            _ => None,
        }
    }

    async fn apply_par2_authoritative_identity(
        &mut self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Result<(), String> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(());
        };

        let files: Vec<(
            NzbFileId,
            crate::jobs::record::ActiveFileIdentity,
            weaver_model::files::FileRole,
            bool,
        )> = state
            .assembly
            .files()
            .filter_map(|file| {
                self.effective_file_identity(job_id, file.file_id())
                    .map(|identity| {
                        (
                            file.file_id(),
                            identity,
                            self.classified_role_for_file(job_id, file),
                            file.is_complete(),
                        )
                    })
            })
            .collect();
        let working_dir = state.working_dir.clone();
        let old_set_by_topology_filename: HashMap<String, String> = state
            .assembly
            .archive_topologies()
            .iter()
            .flat_map(|(set_name, topology)| {
                topology
                    .volume_map
                    .keys()
                    .map(|filename| (filename.clone(), set_name.clone()))
            })
            .collect();
        let _ = state;

        let mut by_current = HashMap::<String, NzbFileId>::new();
        let mut by_source = HashMap::<String, NzbFileId>::new();
        let mut by_canonical = HashMap::<String, NzbFileId>::new();
        let mut by_rar_volume = HashMap::<u32, NzbFileId>::new();

        for (file_id, identity, role, _) in &files {
            by_current.insert(identity.current_filename.clone(), *file_id);
            by_source.insert(identity.source_filename.clone(), *file_id);
            if let Some(canonical) = identity.canonical_filename.as_ref() {
                by_canonical.insert(canonical.clone(), *file_id);
            }
            if let weaver_model::files::FileRole::RarVolume { volume_number } = role {
                by_rar_volume.insert(*volume_number, *file_id);
            }
        }
        let mut occupied_filenames = HashSet::<String>::new();
        for (_, identity, _, _) in &files {
            reserve_identity_filenames(identity, &mut occupied_filenames);
        }
        reserve_directory_filenames(&working_dir, &mut occupied_filenames);

        let mut touched_files = Vec::<NzbFileId>::new();
        let mut touched_rar_files = HashMap::<String, HashSet<String>>::new();
        let mut touched_complete_rar_sets = HashSet::<String>::new();
        let mut stale_rar_sets = HashSet::<String>::new();
        let mut rebound = 0usize;

        for desc in par2_set.files.values() {
            let canonical_filename = sanitize_download_filename(&desc.filename);
            let matched_file_id = by_current
                .get(&canonical_filename)
                .copied()
                .or_else(|| by_source.get(&canonical_filename).copied())
                .or_else(|| by_canonical.get(&canonical_filename).copied())
                .or_else(|| {
                    match weaver_model::files::FileRole::from_filename(&canonical_filename) {
                        weaver_model::files::FileRole::RarVolume { volume_number } => {
                            by_rar_volume.get(&volume_number).copied()
                        }
                        _ => None,
                    }
                });
            let Some(file_id) = matched_file_id else {
                continue;
            };

            let Some((_, identity, old_role, is_complete)) = files
                .iter()
                .find(|(candidate_file_id, _, _, _)| *candidate_file_id == file_id)
                .cloned()
            else {
                continue;
            };

            let old_current = identity.current_filename.clone();
            let mut target_occupied = occupied_filenames.clone();
            forget_identity_filenames(&identity, &mut target_occupied);
            let canonical_filename =
                allocate_unique_download_filename(&canonical_filename, &mut target_occupied);
            let filename_changed = old_current != canonical_filename;
            let old_rar_set_name = identity
                .classification
                .as_ref()
                .and_then(|classification| {
                    matches!(
                        classification.kind,
                        crate::jobs::assembly::DetectedArchiveKind::Rar
                    )
                    .then(|| classification.set_name.clone())
                })
                .or_else(|| {
                    matches!(old_role, weaver_model::files::FileRole::RarVolume { .. })
                        .then(|| weaver_model::files::archive_base_name(&old_current, &old_role))
                        .flatten()
                })
                .or_else(|| {
                    self.rar_sets
                        .iter()
                        .find(|((rar_job_id, _), state)| {
                            *rar_job_id == job_id
                                && state
                                    .volume_files
                                    .values()
                                    .any(|filename| filename == &old_current)
                        })
                        .map(|((_, set_name), _)| set_name.clone())
                })
                .or_else(|| old_set_by_topology_filename.get(&old_current).cloned());
            let old_path = working_dir.join(&old_current);
            let new_path = working_dir.join(&canonical_filename);
            let canonical_path_exists = new_path.exists();
            let canonical_path_is_same =
                runtime_fs::paths_equivalent_for_placement(&old_path, &new_path);
            let renamed_to_canonical = if filename_changed
                && is_complete
                && old_path.exists()
                && (!canonical_path_exists || canonical_path_is_same)
            {
                runtime_fs::rename_no_overwrite(&old_path, &new_path).map_err(|error| {
                    format!(
                        "failed to rename {} to {} from PAR2 metadata: {error}",
                        old_path.display(),
                        new_path.display()
                    )
                })?;
                true
            } else {
                false
            };
            let canonical_is_current = !filename_changed
                || renamed_to_canonical
                || (canonical_path_exists && !old_path.exists());

            let classification =
                Self::canonical_archive_identity_from_filename(&canonical_filename)
                    .or(identity.classification.clone());
            let new_rar_set_name = classification.as_ref().and_then(|classification| {
                matches!(
                    classification.kind,
                    crate::jobs::assembly::DetectedArchiveKind::Rar
                )
                .then(|| classification.set_name.clone())
            });
            if canonical_is_current && let Some(set_name) = old_rar_set_name.as_ref() {
                let set_changed = old_rar_set_name != new_rar_set_name;
                if filename_changed || set_changed {
                    let touched = touched_rar_files.entry(set_name.clone()).or_default();
                    touched.insert(old_current.clone());
                    if set_changed {
                        touched.insert(identity.source_filename.clone());
                        if let Some(canonical) = identity.canonical_filename.as_ref() {
                            touched.insert(canonical.clone());
                        }
                        stale_rar_sets.insert(set_name.clone());
                    }
                }
            }

            let mut rebound_identity = identity.clone();
            if canonical_is_current {
                rebound_identity.current_filename = canonical_filename.clone();
            }
            rebound_identity.canonical_filename = Some(canonical_filename.clone());
            rebound_identity.classification = classification;
            rebound_identity.classification_source = FileIdentitySource::Par2;
            let classification_changed = rebound_identity.classification != identity.classification;
            if rebound_identity == identity {
                continue;
            }
            self.set_file_identity(job_id, rebound_identity)?;
            reserve_download_filename(&canonical_filename, &mut occupied_filenames);

            if is_complete && canonical_is_current && (filename_changed || classification_changed) {
                touched_files.push(file_id);
                if let Some(set_name) = new_rar_set_name.clone() {
                    touched_complete_rar_sets.insert(set_name);
                }
            }
            rebound += 1;
        }

        for (set_name, touched_filenames) in &touched_rar_files {
            self.invalidate_archive_set_for_identity_rebind(job_id, set_name, touched_filenames);
        }

        for file_id in touched_files {
            self.refresh_archive_state_for_completed_file(job_id, file_id, false)
                .await;
        }

        for set_name in touched_complete_rar_sets {
            if !self.rar_sets.contains_key(&(job_id, set_name.clone())) {
                continue;
            }
            self.enqueue_rar_set_refresh(
                job_id,
                &set_name,
                self.latest_completed_rar_volume(job_id, &set_name),
                RefreshReason::IdentityRebind,
            );
        }

        for set_name in stale_rar_sets {
            self.clear_archive_set_if_unreferenced_and_idle(job_id, &set_name);
        }
        let empty_idle_sets = self
            .rar_sets
            .iter()
            .filter(|((rar_job_id, _), state)| {
                *rar_job_id == job_id
                    && state.volume_files.is_empty()
                    && state.active_workers == 0
                    && state.in_flight_members.is_empty()
            })
            .map(|((_, set_name), _)| set_name.clone())
            .collect::<Vec<_>>();
        for set_name in empty_idle_sets {
            self.purge_empty_rar_set_if_idle(job_id, &set_name);
        }

        if rebound > 0 {
            self.mark_rar_unlock_priorities_dirty(job_id);
            info!(
                job_id = job_id.0,
                rebound, "PAR2 canonical file identity applied"
            );
        }

        Ok(())
    }

    pub(crate) async fn retry_par2_authoritative_identity(&mut self, job_id: JobId) {
        let Some(par2_set) = self.par2_set(job_id).cloned() else {
            return;
        };

        if let Err(error) = self
            .apply_par2_authoritative_identity(job_id, par2_set.as_ref())
            .await
        {
            warn!(
                job_id = job_id.0,
                error = %error,
                "failed to retry authoritative PAR2 file identity"
            );
        }
    }

    pub(crate) fn par2_runtime(&self, job_id: JobId) -> Option<&crate::pipeline::Par2RuntimeState> {
        self.par2_runtime.get(&job_id)
    }

    pub(crate) fn par2_set(&self, job_id: JobId) -> Option<&Arc<Par2FileSet>> {
        self.par2_runtime(job_id)
            .and_then(crate::pipeline::Par2RuntimeState::served)
            .and_then(|set_runtime| set_runtime.set.as_ref())
    }

    pub(crate) fn par2_set_for(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> Option<&Arc<Par2FileSet>> {
        self.par2_runtime(job_id)
            .and_then(|runtime| runtime.set_runtime(set_id))
            .and_then(|set_runtime| set_runtime.set.as_ref())
    }

    pub(crate) fn par2_served_set_id(&self, job_id: JobId) -> Option<par2_rs::RecoverySetId> {
        self.par2_runtime(job_id)
            .and_then(crate::pipeline::Par2RuntimeState::served_set_id)
    }

    pub(crate) fn ensure_par2_runtime(
        &mut self,
        job_id: JobId,
    ) -> &mut crate::pipeline::Par2RuntimeState {
        self.par2_runtime.entry(job_id).or_default()
    }

    pub(crate) fn job_spec_has_par2_file(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            state
                .spec
                .files
                .iter()
                .any(|file| matches!(file.role, weaver_model::files::FileRole::Par2 { .. }))
        })
    }

    /// Every name a file could be described under, sanitized.
    ///
    /// Sanitized on the way out, because these are matched against sanitized
    /// descriptions. Comparing a raw posted name to a sanitized one silently
    /// loses the binding — and with it in-stream verification for that file —
    /// for every name that needed sanitizing at all.
    fn par2_binding_candidate_names(&self, file_id: NzbFileId) -> Option<HashSet<String>> {
        let state = self.jobs.get(&file_id.job_id)?;
        let file = state.assembly.file(file_id)?;
        let current_filename = self.current_filename_for_file(file_id.job_id, file);
        let mut names = HashSet::from([
            sanitize_download_filename(&current_filename),
            sanitize_download_filename(file.filename()),
        ]);
        if let Some(identity) = self.effective_file_identity(file_id.job_id, file_id) {
            names.insert(sanitize_download_filename(&identity.source_filename));
            if let Some(canonical) = identity.canonical_filename.as_ref() {
                names.insert(sanitize_download_filename(canonical));
            }
        }
        Some(names)
    }

    /// Whether a file is covered only by a set whose index never arrived.
    ///
    /// Parsed sets are all served by the completion gate.  The remaining case
    /// is a set known from foreign packets but lacking descriptions and an
    /// index, so no verifier or repairer can ever act on its claimed files.
    pub(in crate::pipeline) fn file_is_described_only_by_an_unservable_recovery_set(
        &self,
        file_id: NzbFileId,
    ) -> bool {
        let Some(runtime) = self.par2_runtime(file_id.job_id) else {
            return false;
        };
        let Some(names) = self.par2_binding_candidate_names(file_id) else {
            return false;
        };
        runtime
            .sets
            .iter()
            .filter(|(_, set_runtime)| !set_runtime.summary.describes)
            .any(|(_, set_runtime)| {
                set_runtime
                    .summary
                    .described_filenames
                    .iter()
                    .any(|described| names.contains(described))
            })
    }

    /// Bind one pipeline file to a description in one particular recovery set.
    ///
    /// This is the dual-CRC grid's name-to-description resolver: it is what
    /// [`Self::block_crc_verdicts`] and [`Self::in_stream_verified_par2_match`]
    /// use to decide which description a file's in-stream block verdicts are
    /// measured against. Ambiguity inside the set is refused outright — a name
    /// matching two descriptions yields no binding at all.
    pub(crate) fn resolve_par2_file_binding_in_set(
        &self,
        file_id: NzbFileId,
        set_id: par2_rs::RecoverySetId,
    ) -> Option<Par2FileBinding> {
        let set = self.par2_set_for(file_id.job_id, set_id)?;
        let state = self.jobs.get(&file_id.job_id)?;
        let file = state.assembly.file(file_id)?;
        let current_filename = self.current_filename_for_file(file_id.job_id, file);
        let names = self.par2_binding_candidate_names(file_id)?;
        let candidates = set
            .files
            .iter()
            .filter_map(|(par2_file_id, desc)| {
                names
                    .contains(&sanitize_download_filename(&desc.filename))
                    .then_some(*par2_file_id)
            })
            .collect::<Vec<_>>();
        let par2_file_id = match candidates.len() {
            // Name binding, unchanged, and it always wins.
            1 => candidates[0],
            // No name matched anything. An obfuscated post lies in its subject,
            // not in its bytes, so ask the bytes.
            0 => self.content_bound_par2_file_id(file_id, set)?,
            // Two descriptions answer to this file's name. That is an ambiguity
            // in the naming, and content cannot resolve it into a *name*
            // binding — it stays refused, exactly as before.
            _ => return None,
        };
        // The binding length is what PAR2 *describes*, never the NZB's
        // declared total: `<segment bytes=…>` is yEnc-**encoded** size, around
        // 1.03x the decoded bytes, so a declared total can never equal
        // `desc.length` for a real post. The meaningful length check is
        // decoded-vs-described and lives in
        // [`Self::in_stream_verified_par2_match`], which compares
        // `file.received_bytes()` against this same `desc.length`.
        let described_length = set.file_description(&par2_file_id)?.length;
        Some(Par2FileBinding {
            recovery_set_id: set_id,
            par2_file_id,
            described_length,
            path: state.working_dir.join(current_filename),
            is_complete: file.is_complete(),
        })
    }

    /// Bind one pipeline file to exactly one parsed recovery-set description.
    ///
    /// A description that answers from two recovery sets is ambiguous even when
    /// each set resolves it uniquely on its own, so neither may claim it.
    pub(crate) fn resolve_par2_file_binding(&self, file_id: NzbFileId) -> Option<Par2FileBinding> {
        let set_ids = self.par2_runtime(file_id.job_id)?.ordered_set_ids();
        let mut binding = None;
        for set_id in set_ids {
            let Some(candidate) = self.resolve_par2_file_binding_in_set(file_id, set_id) else {
                continue;
            };
            if binding.is_some() {
                return None;
            }
            binding = Some(candidate);
        }
        binding
    }

    /// The one description whose `hash_16k` the file's captured prefix
    /// reproduces, if exactly one does.
    ///
    /// # Why this exists
    ///
    /// Obfuscated posts lie about names and tell the truth about bytes. A set
    /// posted as `a7f3e91c.part01.rar` binds to nothing by name, and a file that
    /// binds to nothing has no description to measure its in-stream block
    /// verdicts against — so the whole dual-CRC grid lapses for it and every
    /// volume is read back from disk at completion. The recovery set already
    /// carries the answer: `hash_16k` is content, and content is the thing the
    /// obfuscation did not touch.
    ///
    /// # The window is the description's, not ours
    ///
    /// A description shorter than [`crate::pipeline::PAR2_HASH_16K_BYTES`]
    /// hashes its whole file with no padding, so each candidate is matched over
    /// `min(desc.length, 16 KiB)` of the prefix — its own window, not a fixed
    /// one. A description whose window the capture does not cover is skipped
    /// rather than guessed at.
    ///
    /// Lengths come from the descriptions only. The NZB's `<segment bytes>` are
    /// yEnc-encoded and would put the window in the wrong place for yEnc and
    /// wildly wrong for uuencode.
    ///
    /// # Fail-closed, on the same terms as the name path
    ///
    /// Zero matches and two matches both return `None`. Two descriptions
    /// sharing a 16 KiB prefix is a real shape — think a set of volumes with
    /// identical headers — and it is exactly the case where binding by content
    /// would be a guess. The file is then unbound, which costs it in-stream
    /// verification and nothing else: it is read at completion like every file
    /// was before the grid existed.
    fn content_bound_par2_file_id(
        &self,
        file_id: NzbFileId,
        set: &Par2FileSet,
    ) -> Option<par2_rs::FileId> {
        let prefix = self.file_prefix_16k.get(&file_id)?;
        if prefix.is_empty() {
            return None;
        }
        let state = self.jobs.get(&file_id.job_id)?;
        let file = state.assembly.file(file_id)?;
        let current_filename = self.current_filename_for_file(file_id.job_id, file);
        let source_filename = self
            .effective_file_identity(file_id.job_id, file_id)
            .map(|identity| identity.source_filename);
        let declared_size = self.file_declared_size.get(&file_id).copied();
        let matches = set
            .files
            .iter()
            .filter(|(_, desc)| {
                let length_contradicts = if file.is_complete() {
                    file.received_bytes() != desc.length
                } else {
                    file.received_bytes() > desc.length
                } || declared_size.is_some_and(|size| size != desc.length);
                if length_contradicts
                    || crate::pipeline::is_split_fragment_of(&current_filename, &desc.filename)
                    || source_filename.as_ref().is_some_and(|source_filename| {
                        crate::pipeline::is_split_fragment_of(source_filename, &desc.filename)
                    })
                {
                    return false;
                }
                let window = (desc.length as usize).min(crate::pipeline::PAR2_HASH_16K_BYTES);
                // A zero-length description has no content to be identified by.
                // A window the capture does not reach cannot be tested without
                // inventing the bytes it is missing.
                window > 0
                    && prefix.len() >= window
                    && par2_rs::checksum::md5(&prefix[..window]) == desc.hash_16k
            })
            .map(|(par2_file_id, _)| *par2_file_id)
            .collect::<Vec<_>>();
        let bound = unique_par2_binding_candidate(&matches)?;
        crate::runtime::perf_probe::record(
            "par2.binding.resolved_by_content",
            std::time::Duration::from_nanos(1),
        );
        Some(bound)
    }

    /// The recovery set's block size for a job, once its PAR2 packets have been
    /// parsed. This is the checkpoint grid the decoder cuts CRC segments on.
    pub(crate) fn par2_block_size(&self, job_id: JobId) -> Option<std::num::NonZeroU64> {
        std::num::NonZeroU64::new(self.par2_set(job_id)?.slice_size)
    }

    /// The block size of the recovery set that currently owns `file_id`.
    ///
    /// An unbound file deliberately records no grid evidence. Binding becomes
    /// available when its name matches a description or when its captured 16
    /// KiB prefix arrives, which is also the first moment a grid claim could be
    /// useful. In particular, we must not cut an earlier article on the served
    /// set's grid and later reinterpret it after the file binds elsewhere.
    pub(crate) fn par2_block_size_for_file(
        &self,
        file_id: NzbFileId,
    ) -> Option<std::num::NonZeroU64> {
        let binding = self.resolve_par2_file_binding(file_id)?;
        std::num::NonZeroU64::new(
            self.par2_set_for(file_id.job_id, binding.recovery_set_id)?
                .slice_size,
        )
    }

    /// Record a decoded article's block-aligned CRC segments against the file it
    /// was placed in.
    ///
    /// `file_offset` and `decoded_len` are the pipeline's own placement, which
    /// is authoritative over the poster's `=ypart begin`. Called on the
    /// durability seam — after the write for this segment returned — so a block
    /// claimed here describes content that is actually on disk.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn note_block_crc_segments(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
        decoded_len: u64,
        part_crc: u32,
        part_crc_verified: bool,
        was_duplicate: bool,
        segments: &[weaver_yenc::Segment],
    ) {
        let Some(block_size) = self.par2_block_size_for_file(file_id) else {
            return;
        };
        self.block_crcs.note_article(
            file_id,
            block_size,
            file_offset,
            decoded_len,
            part_crc,
            part_crc_verified,
            was_duplicate,
            segments,
        );
    }

    /// In-stream block verdicts for a completed file, if the recovery set binds
    /// it and the collector closed any blocks.
    ///
    /// Blocks absent from the result are *unclaimed*: settle-time verification
    /// owns them, and reads and hashes them exactly as it did before.
    pub(crate) fn block_crc_verdicts(
        &self,
        file_id: NzbFileId,
    ) -> Option<std::collections::BTreeMap<u32, crate::pipeline::integrity::BlockVerdict>> {
        let binding = self.resolve_par2_file_binding(file_id)?;
        let set = self.par2_set_for(file_id.job_id, binding.recovery_set_id)?;
        let verdicts = self
            .block_crcs
            .verdicts_against(file_id, set, binding.par2_file_id);
        (!verdicts.is_empty()).then_some(verdicts)
    }

    /// A completed file's PAR2 identity, provable from the in-stream dual-CRC
    /// grid alone.
    ///
    /// `Some` means: the file binds uniquely by name/identity to one PAR2
    /// description, its assembled length equals the described length exactly,
    /// and every described slice closed `Intact` with independent (pCRC
    /// verified) article coverage — the same bar `InStreamCrc32Proof`
    /// enforces per slice, demanded here for all of them. Anything less —
    /// a missing slice, an unclaimed block, `NoReference`, `Damaged`, a
    /// length disagreement, an unverified contribution — returns `None`, and
    /// the caller falls back to digests or the authoritative read.
    pub(crate) fn in_stream_verified_par2_match(
        &self,
        file_id: NzbFileId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Option<(par2_rs::FileId, String)> {
        let binding = self.resolve_par2_file_binding(file_id)?;
        if binding.recovery_set_id != par2_set.recovery_set_id {
            return None;
        }
        let description = par2_set.file_description(&binding.par2_file_id)?;
        if description.length == 0 {
            // A zero-length description has no slices; "every slice intact"
            // would be vacuously true on no evidence at all.
            return None;
        }
        let state = self.jobs.get(&file_id.job_id)?;
        let file = state.assembly.file(file_id)?;
        // `received_bytes` is the decoded length the commits accumulated —
        // what PAR2 describes. `total_bytes()` is the NZB's declared segment
        // sum, which is yEnc-ENCODED (~3% larger on real posts) and must
        // never be compared against a description.
        if !binding.is_complete || file.received_bytes() != description.length {
            return None;
        }
        let verdicts = self.block_crc_verdicts(file_id)?;
        let slice_count = par2_set.slice_count_for_file(description.length);
        let all_slices_independently_intact = (0..slice_count).all(|slice_index| {
            matches!(
                verdicts.get(&slice_index),
                Some(crate::pipeline::integrity::BlockVerdict::Intact {
                    independently_covered: true
                })
            )
        });
        if !all_slices_independently_intact {
            return None;
        }
        Some((
            binding.par2_file_id,
            sanitize_download_filename(&description.filename),
        ))
    }

    /// Whether the dual-CRC grid adjudicated **every** described slice of
    /// **every** described file in a job's recovery set.
    ///
    /// This is the bar an access-backed repair session has to clear before it
    /// may stand in for the read-and-verify pass. That session reads no source
    /// bytes — `analyze()` skips the scan, because the direct volumes are
    /// absent from the directory by construction — so it reports only what its
    /// evidence established. One unclaimed slice and it would call an unread
    /// volume missing, so anything short of total coverage refuses and the
    /// caller falls back to the pass, which can actually read a virtual volume.
    ///
    /// Total coverage here means *clean*:
    /// [`Self::in_stream_verified_par2_match`] demands every slice `Intact`
    /// with independent (pCRC-verified) article coverage at exactly the
    /// described length. A set carrying a `Damaged` block is deliberately not
    /// adjudicated — the grid withholds damaged slices from evidence, so a
    /// session seeded from it would have nothing to say about the very blocks
    /// that matter, and those need the real bytes read.
    pub(crate) fn grid_adjudicated_par2_bindings(
        &self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> bool {
        if par2_set.files.is_empty() {
            // No descriptions is not proof of anything; "every slice intact"
            // would be vacuously true on no evidence at all.
            return false;
        }
        let Some(adjudicated) = self.grid_adjudicated_par2_file_ids(job_id, par2_set) else {
            return false;
        };
        par2_set
            .files
            .iter()
            .all(|(par2_file_id, _)| adjudicated.contains(par2_file_id))
    }

    /// The per-description half of [`Self::grid_adjudicated_par2_bindings`]:
    /// which of a job's PAR2 descriptions the dual-CRC grid proved clean in
    /// stream, at exactly the described length, with independent article
    /// coverage on every slice.
    ///
    /// `None` is ambiguity — two pipeline files claiming one description — and
    /// is not a smaller answer than the empty set: it means the name-to-
    /// description resolution itself cannot be trusted, so no claim derived
    /// from it may be acted on.
    ///
    /// Split out because the two callers want different shapes of the same
    /// question. The access-backed session needs the all-or-nothing answer,
    /// because it reads nothing and one unclaimed slice would have it call an
    /// unread volume missing. The read-and-verify pass needs the per-file
    /// answer: a file this proves clean is one it does not have to read, and
    /// every other file is read exactly as before.
    pub(crate) fn grid_adjudicated_par2_file_ids(
        &self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Option<HashSet<par2_rs::FileId>> {
        let state = self.jobs.get(&job_id)?;
        let file_ids: Vec<NzbFileId> = state.assembly.files().map(|file| file.file_id()).collect();
        let mut adjudicated = HashSet::new();
        for file_id in file_ids {
            if let Some((par2_file_id, _)) = self.in_stream_verified_par2_match(file_id, par2_set)
                && !adjudicated.insert(par2_file_id)
            {
                return None;
            }
        }
        Some(adjudicated)
    }

    /// Every file's in-stream block verdicts for one recovery set, shaped as
    /// PAR2 slice evidence a repair session can be seeded with.
    pub(crate) fn in_stream_slice_evidence_for_set(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> Vec<par2_rs::SliceEvidence> {
        let Some(set) = self.par2_set_for(job_id, set_id) else {
            return Vec::new();
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        let recovery_set_id = set.recovery_set_id;
        let slice_size = set.slice_size;
        let file_ids: Vec<NzbFileId> = state.assembly.files().map(|file| file.file_id()).collect();

        let mut evidence = Vec::new();
        for file_id in file_ids {
            let Some(verdicts) = self.block_crc_verdicts(file_id) else {
                continue;
            };
            let Some(binding) = self.resolve_par2_file_binding(file_id) else {
                continue;
            };
            if binding.recovery_set_id != set_id {
                continue;
            }
            let Some(length) = set
                .file_description(&binding.par2_file_id)
                .map(|desc| desc.length)
            else {
                continue;
            };
            evidence.extend(crate::pipeline::integrity::slice_evidence_from_verdicts(
                recovery_set_id,
                binding.par2_file_id,
                length,
                slice_size,
                &verdicts,
            ));
        }
        evidence
    }

    /// Whether the retained session is in play. Reads the environment in a
    /// real build; a test may force either arm so a differential can assert
    /// the two agree.
    fn stateful_par2_session_gate(&self) -> bool {
        #[cfg(test)]
        if let Some(forced) = self.stateful_par2_session_forced {
            return forced;
        }
        stateful_par2_session_enabled()
    }

    pub(crate) async fn take_or_open_par2_repair_session(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        working_dir: std::path::PathBuf,
        memory_limit: usize,
        progress: Option<par2_rs::ProgressCallback>,
        source_access: Option<std::sync::Arc<dyn par2_rs::FileAccess + Send + Sync>>,
    ) -> Result<Option<(par2_rs::Par2RepairSession, bool)>, String> {
        if !self.stateful_par2_session_gate() {
            return Ok(None);
        }
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id)
            && let Some(set_runtime) = runtime.set_runtime_mut(set_id)
            && let Some(mut session) = set_runtime.session.take()
        {
            {
                // A session is reusable only if it reads sources the way this
                // caller needs them read. Where both want a handle the retained
                // one adopts the new handle — the direct overlay snapshots its
                // coverage, so every pass builds a fresh one and re-pointing is
                // what keeps one session across many of them.
                // A mismatch in *kind* is not adoptable in either direction, so
                // that session is dropped and a fresh one opened below.
                match (&source_access, session.is_access_backed()) {
                    (Some(access), true) => {
                        session.set_source_access(std::sync::Arc::clone(access));
                        set_runtime.session_last_used = Some(Instant::now());
                        return Ok(Some((session, false)));
                    }
                    (None, false) => {
                        set_runtime.session_last_used = Some(Instant::now());
                        return Ok(Some((session, false)));
                    }
                    _ => {}
                }
            }
        }

        let Some((primary_path, recovery_paths)) = self.par2_runtime(job_id).and_then(|runtime| {
            runtime.set_runtime(set_id).and_then(|set_runtime| {
                set_runtime
                    .primary_path
                    .clone()
                    .map(|primary_path| (primary_path, set_runtime.merged_recovery_paths.clone()))
            })
        }) else {
            return Ok(None);
        };

        let session_result = tokio::task::spawn_blocking(move || {
            let mut options = match source_access {
                Some(access) => par2_rs::Par2RepairSessionOptions::with_source_access(
                    working_dir,
                    vec![primary_path],
                    access,
                ),
                None => par2_rs::Par2RepairSessionOptions::new(working_dir, vec![primary_path]),
            };
            options.memory_limit = Some(memory_limit);
            options.progress = progress;
            let mut session = par2_rs::Par2RepairSession::open(options)
                .map_err(|error| format!("failed to open retained PAR2 session: {error}"))?;
            if !recovery_paths.is_empty() {
                session
                    .merge_recovery_paths(recovery_paths)
                    .map_err(|error| {
                        format!("failed to replay retained PAR2 recovery paths: {error}")
                    })?;
            }
            Ok::<_, String>(session)
        })
        .await
        .map_err(|error| format!("retained PAR2 session task panicked: {error}"))??;

        Ok(Some((session_result, true)))
    }

    pub(crate) fn restore_par2_repair_session(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        session: par2_rs::Par2RepairSession,
    ) {
        let set_runtime = self
            .ensure_par2_runtime(job_id)
            .set_runtime_mut(set_id)
            .expect("PAR2 repair sessions belong to a parsed recovery set");
        set_runtime.session = Some(session);
        set_runtime.session_last_used = Some(Instant::now());
        self.enforce_par2_retained_session_budget((job_id, set_id));
    }

    fn enforce_par2_retained_session_budget(&mut self, protected: (JobId, par2_rs::RecoverySetId)) {
        loop {
            let retained_bytes = self
                .par2_runtime
                .values()
                .flat_map(|runtime| runtime.sets.values())
                .filter_map(|set_runtime| set_runtime.session.as_ref())
                .map(par2_rs::Par2RepairSession::estimated_retained_bytes)
                .sum::<usize>();
            if retained_bytes <= PAR2_RETAINED_SESSION_BUDGET_BYTES {
                return;
            }

            let victim = select_par2_session_eviction(
                self.par2_runtime.iter().flat_map(|(job_id, runtime)| {
                    runtime.sets.iter().map(|(set_id, set_runtime)| {
                        (
                            (*job_id, *set_id),
                            set_runtime.session.is_some(),
                            set_runtime.session_last_used,
                        )
                    })
                }),
                protected,
            );
            let Some(victim) = victim else {
                return;
            };
            let runtime = self
                .par2_runtime
                .get_mut(&victim.0)
                .expect("PAR2 session eviction target exists");
            let set_runtime = runtime
                .set_runtime_mut(victim.1)
                .expect("PAR2 session eviction target owns a recovery set");
            set_runtime.session = None;
            set_runtime.session_last_used = None;
            set_runtime.session_evidence_file_ids.clear();
            info!(
                job_id = victim.0.0,
                recovery_set_id = %victim.1,
                retained_bytes,
                budget_bytes = PAR2_RETAINED_SESSION_BUDGET_BYTES,
                "evicted retained PAR2 session; it will be reopened and reanalyzed if needed"
            );
        }
    }

    /// A decoded write can replace bytes that were previously committed to a
    /// retained repair session. Drop source locations before that write is
    /// allowed to become observable; parsed PAR2 packets remain reusable.
    pub(crate) fn invalidate_par2_session_for_file_write(&mut self, file_id: NzbFileId) {
        let Some(runtime) = self.par2_runtime.get_mut(&file_id.job_id) else {
            return;
        };
        runtime.completed_checksums.remove(&file_id);
        for set_runtime in runtime.sets.values_mut() {
            set_runtime.session_evidence_file_ids.clear();
            if let Some(session) = set_runtime.session.as_mut() {
                session.invalidate_all_sources();
            }
        }
    }

    /// Identity changes can rename or rebind a path without changing the
    /// downloaded bytes. A retained location must nevertheless be discarded:
    /// repair always derives a fresh location from the current identity.
    pub(crate) fn invalidate_par2_session_for_identity_rebind(&mut self, job_id: JobId) {
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id) {
            for set_runtime in runtime.sets.values_mut() {
                set_runtime.session_evidence_file_ids.clear();
                if let Some(session) = set_runtime.session.as_mut() {
                    session.invalidate_all_sources();
                }
            }
        }
    }

    pub(crate) fn note_recovery_count_from_yenc_name(
        &mut self,
        job_id: JobId,
        file_index: u32,
        yenc_name: &str,
    ) {
        if yenc_name.is_empty() {
            return;
        }

        if let weaver_model::files::FileRole::Par2 {
            is_index,
            recovery_block_count,
        } = weaver_model::files::FileRole::from_filename(yenc_name)
        {
            let blocks = if is_index { 0 } else { recovery_block_count };
            let runtime = self.ensure_par2_runtime(job_id);
            let entry = runtime.files.entry(file_index).or_default();
            entry.filename = yenc_name.to_string();
            entry.recovery_blocks = blocks;
        }
    }

    fn recovery_packet_size(&self, job_id: JobId, set_id: par2_rs::RecoverySetId) -> Option<u64> {
        let par2_set = self.par2_set_for(job_id, set_id)?;
        Some(par2_recovery_packet_size(par2_set.slice_size))
    }

    fn recovery_metadata_overhead_bytes(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> Option<u64> {
        let packet_size = self.recovery_packet_size(job_id, set_id)?;
        let state = self.jobs.get(&job_id)?;

        let mut overheads = Vec::new();

        if let Some(runtime) = self.par2_runtime(job_id) {
            for (&file_index, file) in &runtime.files {
                // A salvaged volume's block count covers only the packets that
                // survived it, while its declared byte total covers the whole
                // file — so subtracting one from the other reports the missing
                // packets as metadata overhead. That outlier would drag the
                // median this returns, and with it every size-derived estimate.
                if file.salvaged {
                    continue;
                }
                // Another set's volumes are sized against another set's slice
                // size, and mixing them into this median mis-calibrates every
                // size-derived estimate the served set makes.
                if !self.recovery_file_serves_set(job_id, file_index, set_id) {
                    continue;
                }
                let blocks = file.recovery_blocks;
                let Some(total_bytes) = recovery_file_bytes(&state.spec, file_index) else {
                    continue;
                };
                let Some(block_bytes) = packet_size.checked_mul(blocks as u64) else {
                    continue;
                };
                if total_bytes >= block_bytes {
                    overheads.push(total_bytes - block_bytes);
                }
            }
        }

        if overheads.is_empty() {
            for (file_index, file) in state.spec.files.iter().enumerate() {
                if matches!(
                    file.role,
                    weaver_model::files::FileRole::Par2 { is_index: true, .. }
                ) && self.recovery_file_serves_set(job_id, file_index as u32, set_id)
                    && let Some(total_bytes) = recovery_file_bytes(&state.spec, file_index as u32)
                {
                    overheads.push(total_bytes);
                }
            }
        }

        if overheads.is_empty() {
            return None;
        }

        overheads.sort_unstable();
        Some(overheads[overheads.len() / 2])
    }

    fn recovery_block_count_for(
        &self,
        job_id: JobId,
        file_index: u32,
        set_id: par2_rs::RecoverySetId,
    ) -> Option<(u32, RecoveryCountSource)> {
        if let Some(blocks) = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&file_index))
            .and_then(|file| {
                file.recovery_blocks_by_set
                    .get(&set_id)
                    .copied()
                    .or(Some(file.recovery_blocks))
            })
        {
            // Zero from the runtime is not a count — entries are created for
            // bookkeeping (identity binding, metadata promotion) with the
            // default of 0 long before any packet is parsed, and treating that
            // as exact silently disqualifies the volume from targeted
            // promotion (`candidate.blocks > 0`). A recovery volume with
            // genuinely zero packets does not exist; only an index is exactly
            // zero, and the role branch below says so. So a zero falls through
            // to the role-derived count.
            if blocks > 0 {
                return Some((blocks, RecoveryCountSource::Exact));
            }
        }

        let state = self.jobs.get(&job_id)?;
        let role = recovery_file_role(&state.spec, file_index)?;

        if matches!(
            role,
            weaver_model::files::FileRole::Par2 { is_index: true, .. }
        ) {
            return Some((0, RecoveryCountSource::Exact));
        }

        // A standard `.volNNN+CCC.par2` name carries the exact packet count.
        // Prefer it before estimating from the encoded file size: the latter
        // includes set metadata and can otherwise under- or over-select the
        // first recovery wave.
        if let weaver_model::files::FileRole::Par2 {
            is_index: false,
            recovery_block_count,
        } = role
        {
            return Some((recovery_block_count, RecoveryCountSource::FilenameFallback));
        }

        if let (Some(packet_size), Some(overhead), Some(total_bytes)) = (
            self.recovery_packet_size(job_id, set_id),
            self.recovery_metadata_overhead_bytes(job_id, set_id),
            recovery_file_bytes(&state.spec, file_index),
        ) {
            let delta = total_bytes.saturating_sub(overhead);
            let estimated = if delta == 0 {
                0
            } else {
                ((delta + (packet_size / 2)) / packet_size) as u32
            };
            return Some((estimated, RecoveryCountSource::Calibrated));
        }

        None
    }

    /// If the completed file is a PAR2 index, read it from disk, parse it,
    /// retain the Par2FileSet for repair, and adopt canonical filenames as
    /// authoritative file identity when available.
    pub(crate) async fn try_load_par2_metadata(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (filename, file_path, is_par2, is_index) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                return;
            };

            let is_par2 = matches!(file_asm.role(), weaver_model::files::FileRole::Par2 { .. });
            let is_index = matches!(
                file_asm.role(),
                weaver_model::files::FileRole::Par2 { is_index: true, .. }
            );
            let filename = self.current_filename_for_file(job_id, file_asm);
            let file_path = state.working_dir.join(&filename);
            (filename, file_path, is_par2, is_index)
        };

        if !is_par2 || !is_index {
            return;
        }
        let parse_path = file_path.clone();
        let parsed = match tokio::task::spawn_blocking(move || {
            let scanned = par2_rs::scan_packets_from_path_with_set_ids(&parse_path)?;
            let mut groups: Vec<ParsedPar2Set> = Vec::new();
            for scanned_packet in scanned {
                if let Some(group) = groups
                    .iter_mut()
                    .find(|group| group.set_id == scanned_packet.recovery_set_id)
                {
                    group.packets.push(scanned_packet.packet);
                } else {
                    groups.push(ParsedPar2Set {
                        set_id: scanned_packet.recovery_set_id,
                        packets: vec![scanned_packet.packet],
                    });
                }
            }
            Ok::<_, par2_rs::Par2Error>(groups)
        })
        .await
        {
            Ok(Ok(parsed)) => parsed,
            Ok(Err(e)) => {
                warn!(filename = %filename, error = %e, "failed to parse PAR2 index");
                return;
            }
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to join PAR2 index parse task");
                return;
            }
        };

        let observed_set_ids = parsed.iter().map(|group| group.set_id).collect::<Vec<_>>();
        self.note_foreign_recovery_set_sightings(job_id, file_id.file_index, &observed_set_ids);

        for group in parsed {
            let set_id = group.set_id;
            let already_installed = self
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.set_runtime(set_id))
                .is_some_and(|set_runtime| set_runtime.set.is_some());
            let par2_set = if already_installed {
                let merge = self
                    .ensure_par2_runtime(job_id)
                    .set_runtime_mut(set_id)
                    .and_then(|set_runtime| set_runtime.set.as_mut())
                    .map(|set| Arc::make_mut(set).merge_packets(group.packets));
                match merge {
                    Some(Ok(result)) => info!(
                        job_id = job_id.0,
                        filename = %filename,
                        recovery_set_id = %set_id,
                        recovery_blocks_merged = result.new_recovery_slices,
                        "merged PAR2 metadata into an existing recovery set"
                    ),
                    Some(Err(error)) => {
                        warn!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            error = %error,
                            "failed to merge PAR2 metadata into an existing recovery set"
                        );
                        continue;
                    }
                    None => continue,
                }
                self.par2_set_for(job_id, set_id).cloned()
            } else {
                match par2_rs::Par2FileSet::from_packets(group.packets) {
                    Ok(set) => Some(Arc::new(set)),
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            error = %error,
                            "failed to build PAR2 recovery set from metadata"
                        );
                        None
                    }
                }
            };
            let Some(par2_set) = par2_set else {
                continue;
            };

            if let Err(error) = self
                .apply_par2_authoritative_identity(job_id, par2_set.as_ref())
                .await
            {
                warn!(
                    job_id = job_id.0,
                    recovery_set_id = %set_id,
                    error = %error,
                    "failed to apply authoritative PAR2 file identity"
                );
            }

            self.record_par2_set_summary(job_id, par2_set.as_ref(), &filename, file_id.file_index);
            let set_runtime = self.ensure_par2_runtime(job_id).ensure_set_runtime(set_id);
            set_runtime.primary_path = Some(file_path.clone());
            if set_runtime.set.is_none() {
                set_runtime.set = Some(par2_set);
            }
        }

        {
            let recovery_blocks = match observed_set_ids.as_slice() {
                [set_id] => self
                    .par2_set_for(job_id, *set_id)
                    .map_or(0, |set| set.recovery_block_count()),
                _ => 0,
            };
            let runtime = self.ensure_par2_runtime(job_id);
            let entry = runtime.files.entry(file_id.file_index).or_default();
            entry.filename = filename.clone();
            entry.recovery_blocks = recovery_blocks;
        }

        // A newly parsed index can expose a set that was not part of an
        // earlier aggregate.  Recompute before choosing the compatibility view
        // so that the old set keeps its settled verdict while the new one is
        // queued for its own pass.
        self.mark_par2_verified(job_id).await;

        // The compatibility view still starts from deterministic metadata
        // selection.  The completion gate replaces it with the earliest
        // unsettled set before every pass, so this cannot re-judge a set that
        // already settled when a later index appears.
        let selected = (!self.par2_verified.contains(&job_id))
            .then(|| self.select_primary_recovery_set(job_id))
            .flatten();
        if let Some(set_id) = selected
            && self.par2_served_set_id(job_id) != Some(set_id)
        {
            self.install_primary_recovery_set(job_id, set_id).await;
        }
        for set_id in observed_set_ids {
            self.install_recovery_set(job_id, set_id).await;
        }
        self.warn_unservable_recovery_sets_once(job_id);

        let _ = self
            .event_tx
            .send(PipelineEvent::Par2MetadataLoaded { job_id });
    }

    /// Adopt a parsed recovery set as the one this job serves, discarding every
    /// piece of state that spoke for the set it replaces.
    ///
    /// Nothing derived from the previous set survives the swap: its merged
    /// volumes describe a different recovery equation, the retained session was
    /// opened over its index, and the in-stream block grid measured files
    /// against its descriptions. Volumes already on disk are re-read so the
    /// incoming set starts with the recovery it is actually entitled to — the
    /// repairer takes its slices from this set, not from a directory scan.
    async fn install_primary_recovery_set(
        &mut self,
        job_id: JobId,
        new_set_id: par2_rs::RecoverySetId,
    ) {
        let replaced = self.par2_served_set_id(job_id);
        {
            let runtime = self.ensure_par2_runtime(job_id);
            if let Some(replaced) = replaced.filter(|replaced| *replaced != new_set_id)
                && let Some(set_runtime) = runtime.set_runtime_mut(replaced)
            {
                set_runtime.merged_recovery_paths.clear();
                set_runtime.session = None;
                set_runtime.session_last_used = None;
                set_runtime.session_evidence_file_ids.clear();
            }
            runtime.served = Some(new_set_id);
            runtime.unserved_sets_warned = false;
        }

        if let Some(previous) = replaced.filter(|previous| *previous != new_set_id) {
            warn!(
                job_id = job_id.0,
                replaced = %previous,
                "the recovery set this job serves changed; recovery state for the previous set was discarded"
            );
            self.block_crcs.forget_job(job_id);
        }

        self.install_recovery_set(job_id, new_set_id).await;
    }

    /// Replay completed recovery volumes after a set's index becomes usable.
    async fn install_recovery_set(&mut self, job_id: JobId, set_id: par2_rs::RecoverySetId) {
        if self.par2_set_for(job_id, set_id).is_none() {
            return;
        }

        let volumes: Vec<NzbFileId> = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .filter(|file| {
                        file.is_complete()
                            && matches!(
                                file.role(),
                                weaver_model::files::FileRole::Par2 {
                                    is_index: false,
                                    ..
                                }
                            )
                            && self.recovery_file_serves_set(
                                job_id,
                                file.file_id().file_index,
                                set_id,
                            )
                    })
                    .map(|file| file.file_id())
                    .collect()
            })
            .unwrap_or_default();
        for file_id in volumes {
            self.try_merge_par2_recovery(job_id, file_id).await;
        }
    }

    /// Record what a parsed set describes, so it can be weighed against the
    /// others and its files recognized later.
    fn record_par2_set_summary(
        &mut self,
        job_id: JobId,
        par2_set: &Par2FileSet,
        index_filename: &str,
        index_file_index: u32,
    ) {
        let described_bytes = par2_set
            .recovery_file_ids
            .iter()
            .filter_map(|file_id| par2_set.files.get(file_id))
            .map(|desc| desc.length)
            .sum();
        let described_filenames = par2_set
            .recovery_file_ids
            .iter()
            .filter_map(|file_id| par2_set.files.get(file_id))
            .map(|desc| sanitize_download_filename(&desc.filename))
            .collect();
        let base_name = par2_set_base_name(index_filename);
        let set_id = par2_set.recovery_set_id;

        let runtime = self.ensure_par2_runtime(job_id);
        let newly_known = runtime.set_runtime(set_id).is_none();
        let summary = &mut runtime.ensure_set_runtime(set_id).summary;
        summary.describes = true;
        summary.index_filename = index_filename.to_string();
        summary.index_file_index = index_file_index;
        summary.base_name = base_name;
        summary.described_filenames = described_filenames;
        summary.described_bytes = described_bytes;
        summary.volume_file_indices.insert(index_file_index);
        if newly_known {
            runtime.unserved_sets_warned = false;
        }
    }

    /// Note every recovery set a PAR2 file's packets turned out to speak for.
    ///
    /// A set met only this way has no descriptions and can never be served — it
    /// is recorded so its volumes are attributed away from the served set and
    /// so the job can name it.
    fn note_foreign_recovery_set_sightings(
        &mut self,
        job_id: JobId,
        file_index: u32,
        observed: &[par2_rs::RecoverySetId],
    ) {
        let runtime = self.ensure_par2_runtime(job_id);
        let mut newly_known = false;
        for set_id in observed {
            if runtime.set_runtime(*set_id).is_none() {
                newly_known = true;
            }
            runtime
                .ensure_set_runtime(*set_id)
                .summary
                .volume_file_indices
                .insert(file_index);
        }
        if newly_known {
            runtime.unserved_sets_warned = false;
        }

        // A file whose packets all answer to one set is that set's, whatever it
        // is named. A file carrying more than one is not attributable at all,
        // and is left to be grouped by name.
        let learned = match observed {
            [only] => Some(*only),
            _ => None,
        };
        let entry = runtime.files.entry(file_index).or_default();
        entry.recovery_set_packets_read = !observed.is_empty();
        entry.recovery_set_id = learned;
    }

    /// The recovery set worth serving: the one protecting the most payload,
    /// ties broken by position in the posting.
    ///
    /// Both keys are properties of the posting rather than of this run, so a
    /// job that is restarted, replayed from disk, or whose files arrive in a
    /// different order reaches the same answer every time. A set known only
    /// through somebody else's packets describes nothing and is not eligible.
    fn select_primary_recovery_set(&self, job_id: JobId) -> Option<par2_rs::RecoverySetId> {
        let runtime = self.par2_runtime(job_id)?;
        runtime
            .sets
            .iter()
            .filter(|(_, set_runtime)| set_runtime.summary.describes)
            .max_by(|(_, left), (_, right)| {
                left.summary
                    .described_bytes
                    .cmp(&right.summary.described_bytes)
                    .then_with(|| {
                        right
                            .summary
                            .index_file_index
                            .cmp(&left.summary.index_file_index)
                    })
            })
            .map(|(set_id, _)| *set_id)
    }

    /// Say once, loudly, that this posting carries recovery sets without an
    /// index and therefore without a possible verification pass.
    ///
    /// The files those sets describe are still delivered; what is lost is the
    /// repair they were entitled to, and that is worth exactly one line naming
    /// every set and every file it covers. Latched because the completion gate
    /// is entered many times per job and nothing about this changes between
    /// entries.
    pub(in crate::pipeline) fn warn_unservable_recovery_sets_once(&mut self, job_id: JobId) {
        let Some(runtime) = self.par2_runtime(job_id) else {
            return;
        };
        if runtime.unserved_sets_warned {
            return;
        }

        let mut unservable: Vec<String> = runtime
            .ordered_set_ids()
            .into_iter()
            .filter_map(|set_id| {
                runtime
                    .set_runtime(set_id)
                    .filter(|set_runtime| !set_runtime.summary.describes)
                    .map(|set_runtime| (set_id, set_runtime))
            })
            .map(|(set_id, set_runtime)| {
                let summary = &set_runtime.summary;
                let name = if summary.index_filename.is_empty() {
                    set_id.to_string()
                } else {
                    summary.index_filename.clone()
                };
                if summary.described_filenames.is_empty() {
                    format!("{name} (no index of it was posted)")
                } else {
                    format!("{name} covering {}", summary.described_filenames.join(", "))
                }
            })
            .collect();
        if unservable.is_empty() {
            return;
        }
        unservable.sort();
        warn!(
            job_id = job_id.0,
            "this posting carries {} recovery set(s) with no posted index: they cannot verify \
             or repair the files they cover — {}",
            runtime.sets.len(),
            unservable.join("; ")
        );

        if let Some(runtime) = self.par2_runtime.get_mut(&job_id) {
            runtime.unserved_sets_warned = true;
        }
        #[cfg(test)]
        {
            self.par2_unserved_set_warnings += 1;
        }
    }

    /// Whether a PAR2 file's recovery blocks belong to one recovery set.
    ///
    /// Attribution is by packets where they have been read, and by name only
    /// where packets have never been read. A job that has met fewer than two
    /// sets skips the filename question entirely: there is nothing to confuse
    /// its volumes with, and an obfuscated posting names nothing recognizably.
    fn recovery_file_serves_set(
        &self,
        job_id: JobId,
        file_index: u32,
        set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let Some(runtime) = self.par2_runtime(job_id) else {
            return true;
        };
        if let Some(file) = runtime.files.get(&file_index) {
            // Blocks this file demonstrably gave the set outrank any attribution
            // question: they are merged, the repairer counts them, and capacity
            // that pretended otherwise could refuse a repair the job can afford.
            if file.recovery_blocks_by_set.contains_key(&set_id) {
                return true;
            }
            if let Some(learned) = file.recovery_set_id {
                return learned == set_id;
            }
            if file.recovery_set_packets_read {
                return false;
            }
        }
        if runtime.sets.len() < 2 {
            return true;
        }
        let Some(set_base) = runtime
            .set_runtime(set_id)
            .and_then(|set_runtime| set_runtime.summary.base_name.as_deref())
        else {
            return true;
        };
        let filename = runtime
            .files
            .get(&file_index)
            .map(|file| file.filename.as_str())
            .filter(|name| !name.is_empty())
            .or_else(|| {
                self.jobs
                    .get(&job_id)
                    .and_then(|state| state.spec.files.get(file_index as usize))
                    .map(|file| file.filename.as_str())
            })
            .unwrap_or_default();
        match par2_set_base_name(filename) {
            Some(base) => base == set_base,
            // A name the convention does not cover is no evidence either way,
            // and refusing it would cost the served set a volume that may well
            // be its own.
            None => true,
        }
    }

    /// When a PAR2 recovery volume completes, parse it and merge recovery
    /// slices into the retained Par2FileSet (avoids re-reading at repair time).
    pub(crate) async fn try_merge_par2_recovery(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (filename, file_path, is_par2_volume) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                return;
            };

            let is_par2_volume = matches!(
                file_asm.role(),
                weaver_model::files::FileRole::Par2 {
                    is_index: false,
                    ..
                }
            );
            let filename = self.current_filename_for_file(job_id, file_asm);
            let file_path = state.working_dir.join(&filename);
            (filename, file_path, is_par2_volume)
        };
        if !is_par2_volume {
            return;
        }

        let parse_path = file_path.clone();
        let groups = match tokio::task::spawn_blocking(move || {
            par2_rs::scan_packets_from_path_with_set_ids(&parse_path).map(|packets| {
                let mut groups: Vec<(par2_rs::RecoverySetId, Vec<par2_rs::Packet>)> = Vec::new();
                for scanned_packet in packets {
                    if let Some((_, group)) = groups
                        .iter_mut()
                        .find(|(set_id, _)| *set_id == scanned_packet.recovery_set_id)
                    {
                        group.push(scanned_packet.packet);
                    } else {
                        groups.push((scanned_packet.recovery_set_id, vec![scanned_packet.packet]));
                    }
                }
                groups
            })
        })
        .await
        {
            Ok(Ok(scanned)) => scanned,
            Ok(Err(e)) => {
                warn!(filename = %filename, error = %e, "failed to parse PAR2 recovery volume");
                return;
            }
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to join PAR2 recovery parse task");
                return;
            }
        };
        let observed_set_ids = groups.iter().map(|(set_id, _)| *set_id).collect::<Vec<_>>();
        self.note_foreign_recovery_set_sightings(job_id, file_id.file_index, &observed_set_ids);
        let single_set_id = match observed_set_ids.as_slice() {
            [set_id] => Some(*set_id),
            _ => None,
        };
        let mut bootstrapped_set_ids = Vec::new();

        for (set_id, packet_list) in groups {
            let mut packet_list = Some(packet_list);
            let bootstrapped_recovery_blocks = if self.par2_set_for(job_id, set_id).is_none() {
                match par2_rs::Par2FileSet::from_packets(
                    packet_list
                        .take()
                        .expect("a PAR2 packet group is consumed once"),
                ) {
                    Ok(set) => {
                        let recovery_blocks = set.recovery_block_count();
                        let par2_set = Arc::new(set);
                        if let Err(error) = self
                            .apply_par2_authoritative_identity(job_id, par2_set.as_ref())
                            .await
                        {
                            warn!(
                                job_id = job_id.0,
                                filename = %filename,
                                recovery_set_id = %set_id,
                                error = %error,
                                "failed to apply authoritative PAR2 identity from recovery volume"
                            );
                        }
                        self.record_par2_set_summary(
                            job_id,
                            par2_set.as_ref(),
                            &filename,
                            file_id.file_index,
                        );
                        let set_runtime =
                            self.ensure_par2_runtime(job_id).ensure_set_runtime(set_id);
                        set_runtime.primary_path = Some(file_path.clone());
                        set_runtime.set = Some(par2_set);
                        bootstrapped_set_ids.push(set_id);
                        Some(recovery_blocks)
                    }
                    Err(error) => {
                        // Recovery-slice-only volumes cannot describe a usable
                        // set. Without Main and file-description packets there
                        // is no safe identity or repair input to install.
                        warn!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            error = %error,
                            "recovery volume does not contain enough metadata to establish a PAR2 set"
                        );
                        continue;
                    }
                }
            } else {
                None
            };
            let path_was_new = self
                .ensure_par2_runtime(job_id)
                .set_runtime_mut(set_id)
                .expect("parsed PAR2 recovery set exists")
                .merged_recovery_paths
                .insert(file_path.clone());
            if path_was_new
                && let Some(mut session) = self
                    .par2_runtime
                    .get_mut(&job_id)
                    .and_then(|runtime| runtime.set_runtime_mut(set_id))
                    .and_then(|set_runtime| set_runtime.session.take())
            {
                let session_path = file_path.clone();
                match tokio::task::spawn_blocking(move || {
                    let result = session.merge_recovery_paths([session_path]);
                    (session, result)
                })
                .await
                {
                    Ok((session, Ok(_))) => {
                        self.restore_par2_repair_session(job_id, set_id, session)
                    }
                    Ok((session, Err(error))) => {
                        self.restore_par2_repair_session(job_id, set_id, session);
                        warn!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            error = %error,
                            "failed to merge recovery path into retained PAR2 session"
                        );
                    }
                    Err(error) => warn!(
                        job_id = job_id.0,
                        filename = %filename,
                        recovery_set_id = %set_id,
                        error = %error,
                        "retained PAR2 recovery merge task panicked; session was evicted"
                    ),
                }
            }

            let (new_recovery_blocks, total_recovery) = if let Some(recovery_blocks) =
                bootstrapped_recovery_blocks
            {
                (recovery_blocks, recovery_blocks)
            } else {
                let merge_result = {
                    let par2_set = Arc::make_mut(
                        self.ensure_par2_runtime(job_id)
                            .set_runtime_mut(set_id)
                            .and_then(|set_runtime| set_runtime.set.as_mut())
                            .expect("parsed PAR2 recovery set exists"),
                    );
                    let merge = par2_set.merge_packets(
                        packet_list
                            .take()
                            .expect("unbootstrapped packets remain available to merge"),
                    );
                    let total_recovery = par2_set.recovery_block_count();
                    (merge, total_recovery)
                };
                match merge_result {
                    (Ok(result), total_recovery) => (result.new_recovery_slices, total_recovery),
                    (Err(error), _) => {
                        warn!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            error = %error,
                            "failed to merge PAR2 recovery volume"
                        );
                        continue;
                    }
                }
            };
            if new_recovery_blocks > 0 {
                if single_set_id != Some(set_id) {
                    // Only a file that answers to more than one set needs this.
                    // A single-set volume is fully described by
                    // `recovery_blocks`, which the salvage path accumulates —
                    // shadowing it here would report a partial count for a
                    // volume that was salvaged and later completed.
                    let entry = self
                        .ensure_par2_runtime(job_id)
                        .files
                        .entry(file_id.file_index)
                        .or_default();
                    let by_set = entry.recovery_blocks_by_set.entry(set_id).or_insert(0);
                    *by_set = (*by_set).max(new_recovery_blocks);
                }
                if single_set_id == Some(set_id) {
                    let promoted = self
                        .par2_runtime(job_id)
                        .and_then(|runtime| runtime.files.get(&file_id.file_index))
                        .is_some_and(|file| file.promoted);
                    let runtime = self.ensure_par2_runtime(job_id);
                    let entry = runtime.files.entry(file_id.file_index).or_default();
                    entry.filename = filename.clone();
                    entry.recovery_blocks = if entry.salvaged {
                        entry.recovery_blocks.saturating_add(new_recovery_blocks)
                    } else {
                        new_recovery_blocks
                    };
                    entry.salvaged = false;
                    entry.salvage_attempted = false;
                    entry.promoted = promoted;
                }
                info!(
                    job_id = job_id.0,
                    filename = %filename,
                    recovery_set_id = %set_id,
                    recovery_blocks_merged = new_recovery_blocks,
                    total_recovery,
                    "merged PAR2 recovery volume"
                );
            }
        }

        let bootstrapped_any = !bootstrapped_set_ids.is_empty();
        for set_id in bootstrapped_set_ids {
            Box::pin(self.install_recovery_set(job_id, set_id)).await;
        }
        if bootstrapped_any {
            if self.par2_served_set_id(job_id).is_none()
                && !self.par2_verified.contains(&job_id)
                && let Some(set_id) = self.select_primary_recovery_set(job_id)
            {
                Box::pin(self.install_primary_recovery_set(job_id, set_id)).await;
            }
            self.warn_unservable_recovery_sets_once(job_id);
        }
    }

    /// Read back the recovery packets that survived on every PAR2 volume of
    /// this job that can no longer complete.
    ///
    /// Recovery otherwise merges only on file *completion*, so a volume one
    /// article short of fifty contributed **zero** blocks to the arithmetic
    /// that decides whether a job is repairable — with its intact packets
    /// sitting on disk the whole time. Both reference downloaders read such a
    /// volume packet by packet instead of writing it off, and the PAR2 format
    /// is what makes that safe: every packet carries its own MD5, and the
    /// scanner resynchronises on the packet magic, so a hole costs the packets
    /// it lands on and nothing else.
    ///
    /// Only packets that validate are merged. An unvalidated merge would be
    /// worse than no merge at all: the set keys recovery slices by exponent and
    /// ignores repeats, so a packet read out of a hole would occupy its
    /// exponent permanently and a later good copy of the same block could never
    /// replace it.
    pub(in crate::pipeline) async fn salvage_partial_promoted_recovery_volumes(
        &mut self,
        job_id: JobId,
    ) {
        let candidate_file_indices: Vec<u32> = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| {
                        (!file.salvage_attempted).then_some(file_index)
                    })
                    .collect()
            })
            .unwrap_or_default();

        let candidates: Vec<(u32, par2_rs::RecoverySetId)> = candidate_file_indices
            .into_iter()
            .flat_map(|file_index| {
                self.recovery_sets_for_unread_file(job_id, file_index)
                    .into_iter()
                    .map(move |set_id| (file_index, set_id))
            })
            .collect();

        for (file_index, expected_set_id) in candidates {
            if !self.recovery_volume_is_stranded(job_id, file_index) {
                continue;
            }
            self.salvage_stranded_recovery_volume(job_id, file_index, expected_set_id)
                .await;
        }
    }

    /// Return the parsed recovery sets an unread volume can belong to.
    ///
    /// A packet observation settles attribution, including the deliberate
    /// no-set result for a file that carries several recovery sets. Until then,
    /// the same filename fallback used by recovery arithmetic identifies the
    /// set to scan. A set without parsed metadata is never a candidate.
    fn recovery_sets_for_unread_file(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> Vec<par2_rs::RecoverySetId> {
        let Some(runtime) = self.par2_runtime(job_id) else {
            return Vec::new();
        };
        let Some(file) = runtime.files.get(&file_index) else {
            return Vec::new();
        };
        if let Some(set_id) = file.recovery_set_id {
            return runtime
                .set_runtime(set_id)
                .is_some_and(|set_runtime| set_runtime.set.is_some())
                .then_some(set_id)
                .into_iter()
                .collect();
        }
        if file.recovery_set_packets_read {
            return Vec::new();
        }

        runtime
            .sets
            .iter()
            .filter_map(|(set_id, set_runtime)| {
                (set_runtime.set.is_some()
                    && self.recovery_file_serves_set(job_id, file_index, *set_id))
                .then_some(*set_id)
            })
            .collect()
    }

    /// Whether this file is a PAR2 recovery volume that has bytes on disk, will
    /// never complete, and has nothing left in flight that could change that.
    fn recovery_volume_is_stranded(&self, job_id: JobId, file_index: u32) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(file) = state.assembly.file(NzbFileId { job_id, file_index }) else {
            return false;
        };
        if !matches!(
            file.role(),
            weaver_model::files::FileRole::Par2 {
                is_index: false,
                ..
            }
        ) {
            return false;
        }
        if file.is_complete() {
            return false;
        }
        // Bytes have to exist to be read. A volume is normally reached here
        // because it was promoted, but a job whose NZB carries no index
        // downloads its smallest volume eagerly without promoting anything, and
        // that volume can strand in exactly the same way.
        let promoted = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&file_index))
            .is_some_and(|entry| entry.promoted);
        if !promoted && file.received_bytes() == 0 {
            return false;
        }
        !self.promoted_recovery_file_has_pending_work(job_id, file_index)
    }

    async fn salvage_stranded_recovery_volume(
        &mut self,
        job_id: JobId,
        file_index: u32,
        expected_set_id: par2_rs::RecoverySetId,
    ) {
        let file_id = NzbFileId { job_id, file_index };
        let Some((filename, file_path)) = self.jobs.get(&job_id).and_then(|state| {
            let file_asm = state.assembly.file(file_id)?;
            let filename = self.current_filename_for_file(job_id, file_asm);
            let file_path = state.working_dir.join(&filename);
            Some((filename, file_path))
        }) else {
            return;
        };

        {
            let runtime = self.ensure_par2_runtime(job_id);
            let entry = runtime.files.entry(file_index).or_default();
            entry.filename = filename.clone();
            entry.salvage_attempted = true;
        }
        #[cfg(test)]
        {
            self.par2_recovery_salvage_scans += 1;
        }

        let scan_path = file_path.clone();
        let packet_list = match tokio::task::spawn_blocking(move || {
            par2_rs::scan_packets_from_path_with_set_ids(&scan_path).map(|packets| {
                packets
                    .into_iter()
                    .filter(|scanned| scanned.recovery_set_id == expected_set_id)
                    .filter_map(|scanned| match scanned.packet {
                        par2_rs::Packet::RecoverySlice(recovery) => {
                            // Metadata packets were hashed by the scan itself;
                            // recovery payloads are deliberately skipped there,
                            // so this is where a payload sitting in a hole is
                            // told apart from one that arrived.
                            let exponent = recovery.exponent;
                            match recovery
                                .data
                                .validate_packet_hash(expected_set_id.as_bytes(), exponent)
                            {
                                Ok(true) => Some(par2_rs::Packet::RecoverySlice(recovery)),
                                _ => None,
                            }
                        }
                        metadata => Some(metadata),
                    })
                    .collect::<Vec<_>>()
            })
        })
        .await
        {
            Ok(Ok(packet_list)) => packet_list,
            Ok(Err(error)) => {
                warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %error,
                    "failed to read back a PAR2 recovery volume that cannot complete"
                );
                return;
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %error,
                    "failed to join PAR2 recovery read-back task"
                );
                return;
            }
        };

        let salvaged_blocks = packet_list
            .iter()
            .filter(|packet| matches!(packet, par2_rs::Packet::RecoverySlice(_)))
            .count() as u32;
        if salvaged_blocks == 0 {
            info!(
                job_id = job_id.0,
                filename = %filename,
                "no PAR2 recovery packets survived on a volume that cannot complete"
            );
            return;
        }

        // The retained session is fed the same way the completed-file path and
        // the restore path feed it — by path, which it scans itself. It holds
        // scan-order packets and validates them lazily when a repair selects
        // them; the set above holds only packets that already validated. The
        // asymmetry is deliberate: the set is what weaver's own recovery
        // arithmetic reads, and that has to be truthful.
        self.ensure_par2_runtime(job_id)
            .set_runtime_mut(expected_set_id)
            .expect("PAR2 recovery volumes merge into their parsed recovery set")
            .merged_recovery_paths
            .insert(file_path.clone());
        if let Some(mut session) = self
            .par2_runtime
            .get_mut(&job_id)
            .and_then(|runtime| runtime.set_runtime_mut(expected_set_id))
            .and_then(|set_runtime| set_runtime.session.take())
        {
            let session_path = file_path.clone();
            match tokio::task::spawn_blocking(move || {
                let result = session.merge_recovery_paths([session_path]);
                (session, result)
            })
            .await
            {
                Ok((session, Ok(_))) => {
                    self.restore_par2_repair_session(job_id, expected_set_id, session)
                }
                Ok((session, Err(error))) => {
                    self.restore_par2_repair_session(job_id, expected_set_id, session);
                    warn!(
                        job_id = job_id.0,
                        filename = %filename,
                        error = %error,
                        "failed to merge a read-back recovery volume into the retained PAR2 session"
                    );
                }
                Err(error) => warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %error,
                    "retained PAR2 read-back merge task panicked; session was evicted"
                ),
            }
        }

        let merge_result = {
            let Some(set) = self
                .ensure_par2_runtime(job_id)
                .set_runtime_mut(expected_set_id)
                .and_then(|set_runtime| set_runtime.set.as_mut())
            else {
                return;
            };
            let par2_set = Arc::make_mut(set);
            let merge = par2_set.merge_packets(packet_list);
            let total_recovery = par2_set.recovery_block_count();
            (merge, total_recovery)
        };
        match merge_result {
            (Ok(merge), total_recovery) => {
                // What the merge accepted, not what the scan found. An exponent
                // already held by the set is not new recovery, and counting the
                // scan would credit this volume with a block the arithmetic
                // already had.
                let salvaged_blocks = merge.new_recovery_slices;
                {
                    let runtime = self.ensure_par2_runtime(job_id);
                    let entry = runtime.files.entry(file_index).or_default();
                    entry.recovery_blocks = salvaged_blocks;
                    entry.salvaged = salvaged_blocks > 0;
                    entry.recovery_set_id = Some(expected_set_id);
                }
                info!(
                    job_id = job_id.0,
                    filename = %filename,
                    salvaged_blocks,
                    total_recovery,
                    "read back recovery blocks from a PAR2 volume that cannot complete"
                );
            }
            (Err(error), _) => {
                warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %error,
                    "failed to merge read-back PAR2 recovery packets"
                );
            }
        }
    }

    /// Forget that a recovery volume was ever read back short.
    ///
    /// Called where the file re-opens for download, so a volume that does
    /// arrive after all merges through the ordinary completion path and reports
    /// its whole block count.
    pub(in crate::pipeline) fn clear_par2_salvage_state_for_file(&mut self, file_id: NzbFileId) {
        if let Some(entry) = self
            .par2_runtime
            .get_mut(&file_id.job_id)
            .and_then(|runtime| runtime.files.get_mut(&file_id.file_index))
        {
            entry.salvaged = false;
            entry.salvage_attempted = false;
        }
    }

    fn recovery_candidate_for(
        &self,
        job_id: JobId,
        file_index: u32,
        set_id: par2_rs::RecoverySetId,
    ) -> Option<RecoveryCandidate> {
        let state = self.jobs.get(&job_id)?;
        let total_bytes = recovery_file_bytes(&state.spec, file_index)?;
        let (blocks, source) = self.recovery_block_count_for(job_id, file_index, set_id)?;
        Some(RecoveryCandidate {
            file_index,
            blocks,
            total_bytes,
            source,
        })
    }

    pub(crate) fn is_promoted_recovery_file(&self, job_id: JobId, file_index: u32) -> bool {
        self.par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&file_index))
            .is_some_and(|file| file.promoted)
    }

    fn promoted_recovery_file_is_complete(&self, job_id: JobId, file_index: u32) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        state
            .assembly
            .file(NzbFileId { job_id, file_index })
            .is_some_and(|file| file.is_complete())
    }

    pub(crate) fn promoted_recovery_file_has_unavailable_segment(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> bool {
        self.unavailable_promoted_recovery_segments
            .iter()
            .any(|segment_id| {
                segment_id.file_id.job_id == job_id && segment_id.file_id.file_index == file_index
            })
    }

    pub(crate) fn mark_promoted_recovery_segment_unavailable(&mut self, segment_id: SegmentId) {
        if !self.is_promoted_recovery_file(segment_id.file_id.job_id, segment_id.file_id.file_index)
        {
            return;
        }
        if self
            .unavailable_promoted_recovery_segments
            .insert(segment_id)
        {
            warn!(
                segment = %segment_id,
                "promoted PAR2 recovery segment became unavailable"
            );
            self.schedule_job_completion_check(segment_id.file_id.job_id);
        }
    }

    pub(crate) fn promoted_recovery_file_has_pending_work(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> bool {
        let file_id = NzbFileId { job_id, file_index };
        let queued_download = self.jobs.get(&job_id).is_some_and(|state| {
            state
                .download_queue
                .count_matching(|work| work.segment_id.file_id == file_id)
                > 0
        });
        let active_download = self.active_downloads_by_file.contains_key(&file_id);
        let delayed_retry = self
            .pending_retries_by_segment
            .keys()
            .any(|segment_id| segment_id.file_id == file_id);
        let pending_decode = self
            .pending_decode
            .iter()
            .any(|work| work.segment_id.file_id == file_id);
        let active_decode = self.active_decodes_by_file.contains_key(&file_id);
        let write_buffered = self
            .write_buffers
            .get(&file_id)
            .is_some_and(|buffer| buffer.buffered_len() > 0);

        queued_download
            || active_download
            || delayed_retry
            || pending_decode
            || active_decode
            || write_buffered
    }

    fn loaded_recovery_file_indices(&self, job_id: JobId) -> HashSet<u32> {
        let Some(state) = self.jobs.get(&job_id) else {
            return HashSet::new();
        };
        let mut file_indices = HashSet::new();

        for file in state.assembly.files() {
            if file.is_complete()
                && matches!(
                    file.role(),
                    weaver_model::files::FileRole::Par2 {
                        is_index: false,
                        ..
                    }
                )
            {
                file_indices.insert(file.file_id().file_index);
            }
        }

        file_indices
    }

    fn targeted_recovery_file_indices(&self, job_id: JobId) -> HashSet<u32> {
        self.par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| {
                        if !file.promoted
                            || self.promoted_recovery_file_is_complete(job_id, file_index)
                            || self
                                .promoted_recovery_file_has_unavailable_segment(job_id, file_index)
                        {
                            return None;
                        }
                        self.promoted_recovery_file_has_pending_work(job_id, file_index)
                            .then_some(file_index)
                    })
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default()
    }

    /// Volumes that will never complete but whose surviving recovery packets
    /// were read back off disk and merged.
    ///
    /// They are counted apart from the two sets above because both of those
    /// bypass the runtime entry for exactly this file: the loaded set requires
    /// a complete assembly, and the targeted set requires work still in flight.
    fn salvaged_recovery_file_indices(&self, job_id: JobId) -> HashSet<u32> {
        self.par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| file.salvaged.then_some(file_index))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn total_recovery_block_capacity(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> u32 {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };

        state
            .spec
            .files
            .iter()
            .enumerate()
            .map(|(file_index, _)| file_index as u32)
            .filter(|file_index| self.recovery_file_serves_set(job_id, *file_index, set_id))
            .filter_map(|file_index| self.recovery_block_count_for(job_id, file_index, set_id))
            .map(|(blocks, _)| blocks)
            .sum()
    }

    pub(crate) fn recovery_blocks_available_or_targeted(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> u32 {
        let mut file_indices = self.loaded_recovery_file_indices(job_id);
        file_indices.extend(self.targeted_recovery_file_indices(job_id));
        file_indices.extend(self.salvaged_recovery_file_indices(job_id));
        file_indices
            .into_iter()
            .filter(|file_index| self.recovery_file_serves_set(job_id, *file_index, set_id))
            .filter_map(|file_index| self.recovery_block_count_for(job_id, file_index, set_id))
            .map(|(blocks, _)| blocks)
            .sum()
    }

    /// Which PAR2 files this job could still promote in order to obtain
    /// metadata, and which of them have already had their turn.
    ///
    /// Index files rank ahead of any other PAR2 file, and within each tier the
    /// posting's own order decides — both properties of the posting rather than
    /// of this run, so a restart reaches the same answer.
    fn par2_metadata_candidates(&self, job_id: JobId) -> (Vec<u32>, Vec<u32>) {
        let Some(state) = self.jobs.get(&job_id) else {
            return (Vec::new(), Vec::new());
        };
        let has_available_segment = |file_index: u32| {
            state
                .spec
                .files
                .get(file_index as usize)
                .is_some_and(|file| {
                    file.segments.iter().any(|segment| {
                        !self
                            .unavailable_promoted_recovery_segments
                            .contains(&SegmentId {
                                file_id: NzbFileId { job_id, file_index },
                                segment_number: segment.ordinal,
                            })
                    })
                })
        };
        let tier = |want_index: bool| {
            let mut candidates = state
                .spec
                .files
                .iter()
                .enumerate()
                .filter_map(|(file_index, file)| {
                    let is_index = matches!(
                        file.role,
                        weaver_model::files::FileRole::Par2 { is_index: true, .. }
                    );
                    let is_par2 = matches!(file.role, weaver_model::files::FileRole::Par2 { .. });
                    let wanted = if want_index { is_index } else { is_par2 };
                    wanted.then_some(file_index as u32)
                })
                .collect::<Vec<_>>();
            candidates.sort_unstable();
            candidates
        };

        // A promoted candidate is finished once it either arrived — whatever it
        // turned out to contain — or can no longer arrive at all. Anything else
        // is still on its way, and the job is waiting rather than stuck.
        let outstanding = |file_index: u32| {
            self.is_promoted_recovery_file(job_id, file_index)
                && !self.promoted_recovery_file_is_complete(job_id, file_index)
                && has_available_segment(file_index)
        };

        let mut untried = Vec::new();
        let mut still_arriving = Vec::new();
        for file_index in tier(true).into_iter().chain(tier(false)) {
            if untried.contains(&file_index) || still_arriving.contains(&file_index) {
                continue;
            }
            if outstanding(file_index) {
                still_arriving.push(file_index);
            } else if !self.is_promoted_recovery_file(job_id, file_index)
                && has_available_segment(file_index)
            {
                untried.push(file_index);
            }
        }
        (untried, still_arriving)
    }

    /// Put this job's next PAR2 metadata candidate on the wire, if there is one
    /// left to try.
    ///
    /// `true` means metadata is on its way and the caller should wait; `false`
    /// means nothing can produce it and the caller owns the terminal failure.
    ///
    /// Promotion is idempotent per file. A candidate that has already been
    /// promoted is never promoted again: while its segments can still arrive
    /// the job simply waits, and once they have arrived — or provably cannot —
    /// the next untried candidate takes its turn. Re-pushing an already
    /// promoted index is what turns "waiting for metadata" into a job that
    /// re-enqueues the same file on every completion-gate entry forever,
    /// because a file that completed without yielding a usable set looks
    /// exactly like one that was never tried.
    pub(crate) fn promote_par2_metadata(&mut self, job_id: JobId) -> bool {
        if self.par2_set(job_id).is_some() {
            return false;
        }

        let (untried, still_arriving) = self.par2_metadata_candidates(job_id);
        // Something promoted is still on the wire. Nothing to enqueue — and no
        // reason to spend a second candidate's articles while the first can
        // still answer — but the job is waiting for something real, so it must
        // not be failed either.
        if !still_arriving.is_empty() {
            return true;
        }
        if untried.is_empty() {
            let already_tried: Vec<u32> = self
                .par2_runtime(job_id)
                .map(|runtime| {
                    let mut tried = runtime
                        .files
                        .iter()
                        .filter(|(_, file)| file.promoted)
                        .map(|(file_index, _)| *file_index)
                        .collect::<Vec<_>>();
                    tried.sort_unstable();
                    tried
                })
                .unwrap_or_default();
            let runtime = self.ensure_par2_runtime(job_id);
            if !runtime.metadata_exhausted_warned {
                runtime.metadata_exhausted_warned = true;
                warn!(
                    job_id = job_id.0,
                    promoted_candidates = ?already_tried,
                    "every PAR2 metadata candidate this job could promote has settled without \
                     yielding a usable recovery set"
                );
            }
            return false;
        }

        let queued = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return false;
            };
            state.recovery_queue.drain_all()
        };

        let mut work_by_file: HashMap<u32, Vec<DownloadWork>> = HashMap::new();
        for work in queued {
            work_by_file
                .entry(work.segment_id.file_id.file_index)
                .or_default()
                .push(work);
        }

        // The classification above already applied the index-first ordering and
        // excluded anything promoted or unreachable, so the pick is just the
        // first of what is left.
        let selected_file = untried.first().copied();

        let Some(selected_file) = selected_file else {
            if let Some(state) = self.jobs.get_mut(&job_id) {
                for (_, works) in work_by_file {
                    for work in works {
                        state.recovery_queue.push(work);
                    }
                }
            }
            return false;
        };

        let (filename, promoted_segments) = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return false;
            };
            let filename = state
                .spec
                .files
                .get(selected_file as usize)
                .map(|file| file.filename.clone())
                .unwrap_or_default();
            let mut promoted_segments = 0usize;
            if let Some(file_spec) = state.spec.files.get(selected_file as usize) {
                for segment_spec in &file_spec.segments {
                    let segment_id = SegmentId {
                        file_id: NzbFileId {
                            job_id,
                            file_index: selected_file,
                        },
                        segment_number: segment_spec.ordinal,
                    };
                    if self
                        .unavailable_promoted_recovery_segments
                        .contains(&segment_id)
                    {
                        continue;
                    }
                    state.download_queue.push(DownloadWork {
                        segment_id,
                        message_id: crate::jobs::ids::MessageId::new(&segment_spec.message_id),
                        groups: file_spec.groups.clone(),
                        priority: PROMOTED_RECOVERY_PRIORITY,
                        byte_estimate: segment_spec.bytes,
                        retry_count: 0,
                        is_recovery: true,
                        exclude_servers: Vec::new(),
                        avoid_server: None,
                    });
                    promoted_segments += 1;
                }
            }
            for (file_index, works) in work_by_file {
                if file_index != selected_file {
                    for work in works {
                        state.recovery_queue.push(work);
                    }
                }
            }
            (filename, promoted_segments)
        };

        {
            let runtime = self.ensure_par2_runtime(job_id);
            let entry = runtime.files.entry(selected_file).or_default();
            entry.filename = filename.clone();
            entry.recovery_blocks = 0;
            entry.promoted = true;
        }

        info!(
            job_id = job_id.0,
            file_index = selected_file,
            filename = %filename,
            promoted_segments,
            "promoted PAR2 metadata file"
        );
        self.update_queue_metrics();
        true
    }

    /// Promote the smallest byte set of recovery files needed to cover the requested block count.
    ///
    /// Returns the number of recovery blocks newly promoted by this call.
    pub(crate) fn promote_recovery_targeted(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        blocks_needed: u32,
    ) -> u32 {
        let already_available_blocks = self.recovery_blocks_available_or_targeted(job_id, set_id);
        let remaining_needed = blocks_needed.saturating_sub(already_available_blocks);
        if remaining_needed == 0 {
            return 0;
        }

        // Candidates come from every pool an un-promoted recovery segment can
        // sit in, not just the parked one. Parking is progressive — segments
        // move to `recovery_queue` at build, on retry, on health re-routing —
        // so at any given promotion pass some volumes' work is still in the
        // ordinary download queue. Selecting only from the parked pool made a
        // wave promote whatever happened to be parked (job 10020: 15 of 30
        // available blocks, with the one volume that could cover the damage
        // invisible), and every later pass promoted nothing while the job
        // waited for blocks it had never asked for.
        let promoted_files_now: std::collections::HashSet<u32> = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| file.promoted.then_some(file_index))
                    .collect()
            })
            .unwrap_or_default();
        let queued = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return 0;
            };
            let mut pool = state.recovery_queue.drain_all();
            pool.extend(state.download_queue.extract_matching(|work| {
                work.is_recovery
                    && !promoted_files_now.contains(&work.segment_id.file_id.file_index)
            }));
            pool
        };

        let mut work_by_file: HashMap<u32, Vec<DownloadWork>> = HashMap::new();
        for work in queued {
            work_by_file
                .entry(work.segment_id.file_id.file_index)
                .or_default()
                .push(work);
        }

        let mut candidates = Vec::new();
        for file_index in work_by_file.keys().copied() {
            if self
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&file_index))
                .is_some_and(|file| file.promoted)
            {
                continue;
            }
            // Fetching another set's volume spends bandwidth on blocks that
            // cannot enter this repair's equation. Its work stays parked below
            // rather than being dropped.
            if !self.recovery_file_serves_set(job_id, file_index, set_id) {
                continue;
            }
            if let Some(candidate) = self.recovery_candidate_for(job_id, file_index, set_id)
                && candidate.blocks > 0
            {
                candidates.push(candidate);
            }
        }

        let selected: HashSet<u32> = select_recovery_file_indices(&candidates, remaining_needed)
            .into_iter()
            .collect();

        let source_map: HashMap<u32, RecoveryCountSource> = candidates
            .iter()
            .map(|candidate| (candidate.file_index, candidate.source))
            .collect();
        let block_map: HashMap<u32, u32> = candidates
            .iter()
            .map(|candidate| (candidate.file_index, candidate.blocks))
            .collect();

        let (promoted_file_indices, promoted_blocks, promoted_segments, sources) = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return 0;
            };

            let mut promoted_file_indices = Vec::new();
            let mut promoted_blocks = 0u32;
            let mut promoted_segments = 0usize;
            let mut promoted_sources = Vec::new();

            for (file_index, mut works) in work_by_file {
                if selected.contains(&file_index) {
                    for mut work in works.drain(..) {
                        work.priority = PROMOTED_RECOVERY_PRIORITY;
                        state.download_queue.push(work);
                        promoted_segments += 1;
                    }
                    promoted_file_indices.push(file_index);
                    promoted_blocks = promoted_blocks
                        .saturating_add(block_map.get(&file_index).copied().unwrap_or(0));
                    if let Some(source) = source_map.get(&file_index).copied() {
                        promoted_sources.push((file_index, source));
                    }
                } else {
                    for work in works.drain(..) {
                        state.recovery_queue.push(work);
                    }
                }
            }

            (
                promoted_file_indices,
                promoted_blocks,
                promoted_segments,
                promoted_sources,
            )
        };

        if !promoted_file_indices.is_empty() {
            let filenames: HashMap<u32, String> = self
                .jobs
                .get(&job_id)
                .map(|state| {
                    promoted_file_indices
                        .iter()
                        .filter_map(|file_index| {
                            state
                                .spec
                                .files
                                .get(*file_index as usize)
                                .map(|file| (*file_index, file.filename.clone()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            for file_index in &promoted_file_indices {
                let (filename, recovery_blocks) = {
                    let runtime = self.ensure_par2_runtime(job_id);
                    let entry = runtime.files.entry(*file_index).or_default();
                    if let Some(filename) = filenames.get(file_index) {
                        entry.filename = filename.clone();
                    }
                    entry.recovery_blocks = block_map.get(file_index).copied().unwrap_or(0);
                    entry.promoted = true;
                    (entry.filename.clone(), entry.recovery_blocks)
                };
                let _ = (filename, recovery_blocks);
            }
            info!(
                job_id = job_id.0,
                blocks_needed,
                already_available_blocks,
                promoted_blocks,
                promoted_segments,
                promoted_files = ?promoted_file_indices,
                promoted_sources = ?sources,
                "promoted targeted recovery files"
            );
            self.update_queue_metrics();
        } else {
            debug!(
                job_id = job_id.0,
                blocks_needed,
                already_available_blocks,
                "no additional recovery files available to promote"
            );
        }

        promoted_blocks
    }

    pub(crate) fn reapply_promoted_recovery_queue(&mut self, job_id: JobId) -> usize {
        let promoted: HashSet<u32> = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| file.promoted.then_some(file_index))
                    .collect()
            })
            .unwrap_or_default();
        if promoted.is_empty() {
            return 0;
        }

        let Some(state) = self.jobs.get_mut(&job_id) else {
            return 0;
        };

        let queued = state.recovery_queue.drain_all();
        let mut moved_segments = 0usize;
        let mut moved_files = HashSet::new();
        for mut work in queued {
            let file_index = work.segment_id.file_id.file_index;
            if promoted.contains(&file_index) {
                work.priority = PROMOTED_RECOVERY_PRIORITY;
                state.download_queue.push(work);
                moved_segments += 1;
                moved_files.insert(file_index);
            } else {
                state.recovery_queue.push(work);
            }
        }

        if moved_segments > 0 {
            info!(
                job_id = job_id.0,
                moved_segments,
                moved_files = ?moved_files,
                "reapplied promoted PAR2 recovery queue state after restore"
            );
            self.update_queue_metrics();
        }

        moved_segments
    }

    /// List all jobs.
    pub(crate) fn list_jobs(&self) -> Vec<JobInfo> {
        let mut list = Vec::with_capacity(self.jobs.len() + self.finished_jobs.len());
        let mut seen = HashSet::with_capacity(self.jobs.len());

        let mut push_state = |state: &JobState| {
            let total = state.spec.total_bytes;
            let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
                state.assembly.optional_recovery_bytes();
            let health = health_milli(total, state.failed_bytes);
            let (mut download_state, post_state, run_state) =
                crate::jobs::model::runtime_lanes_from_status_snapshot(&state.status);
            if matches!(download_state, crate::jobs::model::DownloadState::Complete)
                && self.job_has_pending_download_pipeline_work(state.job_id)
            {
                download_state = crate::jobs::model::DownloadState::Downloading;
            }
            let remaining_par_files = state
                .assembly
                .files()
                .filter(|file| {
                    matches!(
                        file.role(),
                        weaver_model::files::FileRole::Par2 {
                            is_index: false,
                            ..
                        }
                    ) && !file.is_complete()
                })
                .count() as u32;
            let download_wait = self.download_wait_by_job.get(&state.job_id);
            list.push(JobInfo {
                job_id: state.job_id,
                job_hash: Some(state.job_hash),
                name: state.spec.name.clone(),
                error: if let JobStatus::Failed { error } = &state.status {
                    Some(error.clone())
                } else {
                    None
                },
                download_wait_reason: download_wait.map(|wait| wait.reason.to_owned()),
                download_retry_at_epoch_ms: download_wait.and_then(|wait| wait.retry_at_epoch_ms),
                status: state.status.clone(),
                download_state,
                post_state,
                run_state,
                progress: Self::effective_progress(state),
                total_bytes: total,
                downloaded_bytes: Self::effective_downloaded_bytes(state),
                optional_recovery_bytes,
                optional_recovery_downloaded_bytes,
                phase_progress: self
                    .phase_progress_snapshots
                    .get(&state.job_id)
                    .cloned()
                    .unwrap_or_default(),
                failed_bytes: state.failed_bytes,
                health,
                total_files: state.assembly.total_file_count() as u32,
                completed_files: state.assembly.complete_file_count() as u32,
                remaining_par_files,
                password: state.spec.password.clone(),
                category: state.spec.category.clone(),
                metadata: state.spec.metadata.clone(),
                output_dir: Some(state.working_dir.display().to_string()),
                created_at_epoch_ms: state.created_at_epoch_ms,
            });
        };

        for job_id in &self.job_order {
            let Some(state) = self.jobs.get(job_id) else {
                continue;
            };
            if is_terminal_status(&state.status) || !seen.insert(*job_id) {
                continue;
            }
            push_state(state);
        }

        let mut unordered: Vec<&JobState> = self
            .jobs
            .values()
            .filter(|state| !is_terminal_status(&state.status) && !seen.contains(&state.job_id))
            .collect();
        unordered.sort_by(|left, right| {
            left.created_at_epoch_ms
                .total_cmp(&right.created_at_epoch_ms)
        });
        for state in unordered {
            push_state(state);
        }

        list.extend(self.finished_jobs.iter().cloned());
        list
    }
}

#[cfg(test)]
mod tests;
