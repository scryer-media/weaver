use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::*;
use crate::jobs::record::{ActiveFileIdentity, FileIdentitySource};
use crate::runtime::fs as runtime_fs;
use weaver_model::files::{
    allocate_unique_download_filename, forget_reserved_download_filename,
    reserve_download_filename, sanitize_download_filename,
};

pub(crate) mod live;

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

fn select_par2_session_eviction<I>(sessions: I, protected_job_id: JobId) -> Option<JobId>
where
    I: IntoIterator<Item = (JobId, bool, Option<Instant>)>,
{
    let mut protected_available = false;
    let mut oldest_unprotected: Option<(JobId, Option<Instant>)> = None;
    for (job_id, has_session, last_used) in sessions {
        if !has_session {
            continue;
        }
        if job_id == protected_job_id {
            protected_available = true;
            continue;
        }
        if oldest_unprotected
            .as_ref()
            .is_none_or(|(_, oldest)| last_used < *oldest)
        {
            oldest_unprotected = Some((job_id, last_used));
        }
    }
    oldest_unprotected
        .map(|(job_id, _)| job_id)
        .or_else(|| protected_available.then_some(protected_job_id))
}

/// Where one live-PAR2 read-back gets its bytes.
///
/// A conventional file is re-read from disk; a direct set's source volume has
/// no file, so it is read through the hybrid virtual-volume provider. Both are
/// owned values so the read can move onto the blocking pool.
enum LiveReadSource {
    OnDisk(std::path::PathBuf),
    Virtual(
        u32,
        crate::pipeline::direct_store::provider::HybridVolumeProvider,
    ),
}

/// One file's length check for the live short-circuit.
///
/// `scan_placement` rejects a file whose length disagrees with its description,
/// and the short-circuit has to reach the same verdict without running it. For
/// a direct volume the length is the provider's, not a `stat`'s.
enum LiveLengthCheck {
    OnDisk {
        path: std::path::PathBuf,
        expected: u64,
    },
    Virtual {
        volume_index: u32,
        actual: u64,
        expected: u64,
    },
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

fn unique_live_par2_candidate(candidates: &[par2_rs::FileId]) -> Option<par2_rs::FileId> {
    let [candidate] = candidates else {
        return None;
    };
    Some(*candidate)
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
            .and_then(|runtime| runtime.set.as_ref())
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

    pub(crate) fn resolve_live_par2_binding(
        &self,
        file_id: NzbFileId,
    ) -> Option<(par2_rs::FileId, u64, std::path::PathBuf, bool)> {
        let set = self.par2_set(file_id.job_id)?;
        let state = self.jobs.get(&file_id.job_id)?;
        let file = state.assembly.file(file_id)?;
        let current_filename = self.current_filename_for_file(file_id.job_id, file);
        // Sanitized on the way in, because they are matched against sanitized
        // descriptions below. Comparing a raw posted name to a sanitized one
        // silently loses the binding — and with it live verification for that
        // file — for every name that needed sanitizing at all.
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
        let candidates = set
            .files
            .iter()
            .filter_map(|(par2_file_id, desc)| {
                names
                    .contains(&sanitize_download_filename(&desc.filename))
                    .then_some(*par2_file_id)
            })
            .collect::<Vec<_>>();
        let par2_file_id = unique_live_par2_candidate(&candidates)?;
        // The binding length is what PAR2 *describes*, never the
        // NZB's declared total: `<segment bytes=…>` is yEnc-**encoded** size,
        // around 1.03x the decoded bytes, so a declared total can never equal
        // `desc.length` for a real post. `bind` refuses on that inequality, so
        // passing the declared total silently refused every binding — pending
        // spans then never drained and nothing was ever verified in stream.
        // The meaningful length check is decoded-vs-described and lives in
        // `live_par2_clean_verification_shape`, which compares
        // `file.received_bytes()` against this same `desc.length`.
        let described_length = set.file_description(&par2_file_id)?.length;
        Some((
            par2_file_id,
            described_length,
            state.working_dir.join(current_filename),
            file.is_complete(),
        ))
    }

    fn bind_live_par2_file(&mut self, file_id: NzbFileId) {
        let Some((par2_file_id, length, path, complete)) = self.resolve_live_par2_binding(file_id)
        else {
            return;
        };
        if self.live_par2.bind(file_id, par2_file_id, length, path) && complete {
            // The *received* length, never the described one. This is the
            // fail-safe for a file whose decoded bytes disagree with what PAR2
            // describes, and since `bind` was handed `desc.length` as the
            // binding length, passing it again here would compare a value to
            // itself: `rejected` could never fire and `completed_bytes` would
            // always look right.
            let received_bytes = self
                .jobs
                .get(&file_id.job_id)
                .and_then(|state| state.assembly.file(file_id))
                .map(|file| file.received_bytes());
            if let Some(received_bytes) = received_bytes {
                self.live_par2.note_file_complete(file_id, received_bytes);
            }
        }
    }

    pub(crate) fn activate_live_par2(&mut self, job_id: JobId, packets: &[par2_rs::Packet]) {
        self.live_par2.activate(job_id, packets);
        let file_ids = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .map(|file| file.file_id())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for file_id in file_ids {
            self.bind_live_par2_file(file_id);
        }
    }

    /// Feed bytes after normal assembly made them durable.  We only retain
    /// lightweight range shape until metadata and identity are both known.
    pub(crate) fn note_live_par2_segment(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
        data: &crate::pipeline::DecodedChunk,
    ) {
        if !self.live_par2.enabled()
            || (!self.job_spec_has_par2_file(file_id.job_id)
                && self.par2_set(file_id.job_id).is_none())
        {
            return;
        }
        self.bind_live_par2_file(file_id);
        self.live_par2.note_segment(file_id, file_offset, data);
    }

    /// The recovery set's block size for a job, once its PAR2 packets have been
    /// parsed. This is the checkpoint grid the decoder cuts CRC segments on.
    pub(crate) fn par2_block_size(&self, job_id: JobId) -> Option<std::num::NonZeroU64> {
        std::num::NonZeroU64::new(self.par2_set(job_id)?.slice_size)
    }

    /// Record a decoded article's block-aligned CRC segments against the file it
    /// was placed in.
    ///
    /// `file_offset` and `decoded_len` are the pipeline's own placement, which
    /// is authoritative over the poster's `=ypart begin`. Called after the bytes
    /// are durable, on the same seam as live PAR2 verification, so a block
    /// claimed here describes content that is actually on disk.
    pub(crate) fn note_block_crc_segments(
        &mut self,
        file_id: NzbFileId,
        file_offset: u64,
        decoded_len: u64,
        part_crc: u32,
        segments: &[weaver_yenc::Segment],
    ) {
        let Some(block_size) = self.par2_block_size(file_id.job_id) else {
            return;
        };
        self.block_crcs.note_article(
            file_id,
            block_size,
            file_offset,
            decoded_len,
            part_crc,
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
        let set = self.par2_set(file_id.job_id)?;
        let (par2_file_id, ..) = self.resolve_live_par2_binding(file_id)?;
        let verdicts = self.block_crcs.verdicts_against(file_id, set, par2_file_id);
        (!verdicts.is_empty()).then_some(verdicts)
    }

    pub(crate) fn note_live_par2_file_complete(&mut self, file_id: NzbFileId, received_bytes: u64) {
        self.bind_live_par2_file(file_id);
        self.live_par2.note_file_complete(file_id, received_bytes);
    }

    /// Complete the bounded disk backfill/settle phase.  Any I/O error or
    /// short read simply leaves live verification incomplete and the existing
    /// authoritative pass remains responsible for the job.
    pub(crate) async fn run_live_par2_reads(&mut self, job_id: JobId) {
        let reads = self.live_par2.take_reads(job_id);
        if reads.is_empty() {
            return;
        }
        // The adopted engine settles a read by opening the binding's path,
        // which is right for every conventional file and impossible for a
        // direct set: suppression leaves its source volumes with no file at
        // all, so a path read is dropped and every span stays `Pending`
        // forever. Ask direct-store first — the hybrid provider serves the same
        // bytes in the same source-volume coordinate space, assembled from the
        // envelope plus the routed member partials, and for an encrypted set
        // the overlay re-derives the posted cipher on the way out. A range
        // landing on a hole comes back short, exactly as a truncated file
        // would, and those blocks correctly stay `Pending` for the
        // authoritative pass.
        let reads = reads
            .into_iter()
            .filter_map(|read| {
                let source = match self.direct_virtual_volume(read.file_id) {
                    Some((volume_index, _, provider)) => {
                        LiveReadSource::Virtual(volume_index, provider)
                    }
                    None => LiveReadSource::OnDisk(self.live_par2.path_for_read(read)?),
                };
                Some((read, source))
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        let results = match tokio::task::spawn_blocking(move || {
            reads
                .into_iter()
                .map(|(read, source)| {
                    let result = match source {
                        LiveReadSource::OnDisk(path) => {
                            live::read_range_best_effort(&path, read.offset, read.len)
                        }
                        LiveReadSource::Virtual(volume_index, provider) => {
                            live::read_virtual_range_best_effort(
                                &provider,
                                volume_index,
                                read.offset,
                                read.len,
                            )
                        }
                    };
                    (read, result)
                })
                .collect::<Vec<_>>()
        })
        .await
        {
            Ok(results) => results,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "live PAR2 disk-read task panicked");
                return;
            }
        };
        crate::runtime::perf_probe::record("verify.live_par2.disk_reads", started.elapsed());
        for (read, result) in results {
            match result {
                Ok(bytes) => self.live_par2.apply_read(read, &bytes),
                Err(error) => debug!(
                    job_id = job_id.0,
                    file_id = %read.file_id,
                    offset = read.offset,
                    len = read.len,
                    error = %error,
                    "live PAR2 read unavailable; falling back to authoritative verification"
                ),
            }
        }
    }

    pub(crate) async fn settle_live_par2_job(&mut self, job_id: JobId) {
        let claimed = self.in_stream_claimed_slices(job_id);
        self.live_par2.schedule_settle_reads(job_id, &claimed);
        self.run_live_par2_reads(job_id).await;
        let metrics = self.live_par2.metrics();
        debug!(
            job_id = job_id.0,
            partial_bytes = self.live_par2.partial_bytes(),
            strong_slices = metrics.strongly_verified_slices,
            metadata_range_overflows = metrics.metadata_range_overflows,
            backfill_reads = metrics.backfill_reads,
            settle_reads = metrics.settle_reads,
            disk_read_bytes = metrics.disk_read_bytes,
            disk_read_budget_exhausted = metrics.disk_read_budget_exhausted,
            blocks_claimed_in_stream = self.block_crcs.blocks_derived(),
            articles_without_usable_segments = self.block_crcs.rebased_articles(),
            "live PAR2 settle diagnostics"
        );
    }

    /// `(file, slice)` pairs whose bytes in-stream block verification found
    /// intact against the recovery set's IFSC CRC32.
    ///
    /// A block found *damaged* is deliberately not listed: settle-time
    /// verification still reads it, so the authoritative pass sees the same
    /// evidence it always did about bytes that are actually wrong. Only the
    /// intact case is a read the download path already paid for.
    pub(crate) fn in_stream_claimed_slices(
        &self,
        job_id: JobId,
    ) -> std::collections::HashSet<(NzbFileId, u32)> {
        let mut claimed = std::collections::HashSet::new();
        let Some(state) = self.jobs.get(&job_id) else {
            return claimed;
        };
        let file_ids: Vec<NzbFileId> = state.assembly.files().map(|file| file.file_id()).collect();
        for file_id in file_ids {
            let Some(verdicts) = self.block_crc_verdicts(file_id) else {
                continue;
            };
            for (block_index, verdict) in verdicts {
                if verdict == crate::pipeline::integrity::BlockVerdict::Intact {
                    claimed.insert((file_id, block_index));
                }
            }
        }
        claimed
    }

    pub(crate) fn live_par2_strong_evidence(
        &mut self,
        job_id: JobId,
    ) -> Vec<(std::path::PathBuf, par2_rs::SliceEvidence)> {
        self.live_par2.strong_evidence(job_id)
    }

    pub(crate) fn live_par2_complete_bindings(
        &self,
        job_id: JobId,
    ) -> Option<HashMap<NzbFileId, par2_rs::FileId>> {
        self.live_par2.complete_bindings_if_strong(job_id)
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
        working_dir: std::path::PathBuf,
        memory_limit: usize,
        progress: Option<par2_rs::ProgressCallback>,
        source_access: Option<std::sync::Arc<dyn par2_rs::FileAccess + Send + Sync>>,
    ) -> Result<Option<(par2_rs::Par2RepairSession, bool)>, String> {
        if !self.stateful_par2_session_gate() {
            return Ok(None);
        }
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id)
            && let Some(mut session) = runtime.session.take()
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
                        runtime.session_last_used = Some(Instant::now());
                        return Ok(Some((session, false)));
                    }
                    (None, false) => {
                        runtime.session_last_used = Some(Instant::now());
                        return Ok(Some((session, false)));
                    }
                    _ => {}
                }
            }
        }

        let Some((primary_path, recovery_paths)) = self.par2_runtime(job_id).and_then(|runtime| {
            runtime
                .primary_path
                .clone()
                .map(|primary_path| (primary_path, runtime.merged_recovery_paths.clone()))
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
        session: par2_rs::Par2RepairSession,
    ) {
        let runtime = self.ensure_par2_runtime(job_id);
        runtime.session = Some(session);
        runtime.session_last_used = Some(Instant::now());
        self.enforce_par2_retained_session_budget(job_id);
    }

    fn enforce_par2_retained_session_budget(&mut self, protected_job_id: JobId) {
        loop {
            let retained_bytes = self
                .par2_runtime
                .values()
                .filter_map(|runtime| runtime.session.as_ref())
                .map(par2_rs::Par2RepairSession::estimated_retained_bytes)
                .sum::<usize>();
            if retained_bytes <= PAR2_RETAINED_SESSION_BUDGET_BYTES {
                return;
            }

            let victim = select_par2_session_eviction(
                self.par2_runtime.iter().map(|(job_id, runtime)| {
                    (
                        *job_id,
                        runtime.session.is_some(),
                        runtime.session_last_used,
                    )
                }),
                protected_job_id,
            );
            let Some(victim) = victim else {
                return;
            };
            let runtime = self
                .par2_runtime
                .get_mut(&victim)
                .expect("PAR2 session eviction target exists");
            runtime.session = None;
            runtime.session_last_used = None;
            runtime.session_evidence_file_ids.clear();
            info!(
                job_id = victim.0,
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
        runtime.session_evidence_file_ids.clear();
        if let Some(session) = runtime.session.as_mut() {
            session.invalidate_all_sources();
        }
    }

    /// Identity changes can rename or rebind a path without changing the
    /// downloaded bytes. A retained location must nevertheless be discarded:
    /// repair always derives a fresh location from the current identity.
    pub(crate) fn invalidate_par2_session_for_identity_rebind(&mut self, job_id: JobId) {
        self.live_par2.invalidate_job(job_id);
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id) {
            runtime.session_evidence_file_ids.clear();
            if let Some(session) = runtime.session.as_mut() {
                session.invalidate_all_sources();
            }
        }
    }
    /// performed while reading the files.
    pub(crate) async fn live_par2_clean_verification(
        &self,
        job_id: JobId,
    ) -> Option<(par2_rs::VerificationResult, par2_rs::PlacementPlan)> {
        let (verification, placement_plan, length_checks) =
            self.live_par2_clean_verification_shape(job_id)?;
        if !Self::live_par2_lengths_match(job_id, length_checks).await {
            return None;
        }
        Some((verification, placement_plan))
    }
    async fn live_par2_lengths_match(job_id: JobId, checks: Vec<LiveLengthCheck>) -> bool {
        let mut on_disk = Vec::with_capacity(checks.len());
        for check in checks {
            match check {
                LiveLengthCheck::OnDisk { path, expected } => on_disk.push((path, expected)),
                LiveLengthCheck::Virtual {
                    volume_index,
                    actual,
                    expected,
                } => {
                    if actual != expected {
                        debug!(
                            job_id = job_id.0,
                            volume_index,
                            actual,
                            expected,
                            "live PAR2 short-circuit refused — a direct volume's virtual length \
                             disagrees with the PAR2 description"
                        );
                        return false;
                    }
                }
            }
        }
        if on_disk.is_empty() {
            return true;
        }
        let mismatch = tokio::task::spawn_blocking(move || {
            on_disk.into_iter().find(|(path, expected)| {
                !std::fs::metadata(path).is_ok_and(|metadata| metadata.len() == *expected)
            })
        })
        .await;
        match mismatch {
            Ok(None) => true,
            Ok(Some((path, expected))) => {
                debug!(
                    job_id = job_id.0,
                    path = %path.display(),
                    expected,
                    "live PAR2 short-circuit refused — on-disk length disagrees with the PAR2 description"
                );
                false
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "live PAR2 length-check task panicked; falling through to the full pass"
                );
                false
            }
        }
    }
    fn live_par2_clean_verification_shape(
        &self,
        job_id: JobId,
    ) -> Option<(
        par2_rs::VerificationResult,
        par2_rs::PlacementPlan,
        Vec<LiveLengthCheck>,
    )> {
        let par2_set = self.par2_set(job_id)?;
        if par2_set.recovery_file_ids.is_empty() {
            return None;
        }
        // The adopted engine states this as NzbFileId -> par2 FileId, keyed
        // the other way round, and gates on `Crc32AndMd5` slice strength
        // rather than a bare "ok" — strictly stronger than the map this
        // short-circuit used to read. Inverted here so the rest of the shape
        // check is unchanged.
        let verified: HashMap<par2_rs::FileId, NzbFileId> = self
            .live_par2
            .complete_bindings_if_strong(job_id)?
            .into_iter()
            .map(|(nzb_file_id, par2_file_id)| (par2_file_id, nzb_file_id))
            .collect();
        let state = self.jobs.get(&job_id)?;

        let mut files = Vec::with_capacity(par2_set.recovery_file_ids.len());
        let mut length_checks = Vec::with_capacity(par2_set.recovery_file_ids.len());
        let mut claimed_files = HashSet::new();
        for par2_file_id in &par2_set.recovery_file_ids {
            let file_id = verified.get(par2_file_id).copied()?;
            if !claimed_files.insert(file_id) {
                return None;
            }
            let file = state.assembly.file(file_id)?;
            let desc = par2_set.file_description(par2_file_id)?;
            // `received_bytes` is the decoded length — the space PAR2
            // describes. The NZB's declared total is yEnc-encoded size and
            // never equals it.
            if !file.is_complete() || file.received_bytes() != desc.length {
                return None;
            }
            let correct_filename = sanitize_download_filename(&desc.filename);
            let current_filename =
                sanitize_download_filename(&self.current_filename_for_file(job_id, file));
            if current_filename != correct_filename {
                return None;
            }
            length_checks.push(match self.direct_virtual_volume(file_id) {
                Some((volume_index, len, _)) => LiveLengthCheck::Virtual {
                    volume_index,
                    actual: len,
                    expected: desc.length,
                },
                None => LiveLengthCheck::OnDisk {
                    path: state.working_dir.join(&current_filename),
                    expected: desc.length,
                },
            });

            let slice_count = par2_set.slice_count_for_file(desc.length) as usize;
            files.push(par2_rs::verify::FileVerification {
                file_id: *par2_file_id,
                filename: correct_filename,
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: vec![true; slice_count],
                missing_slice_count: 0,
            });
        }

        Some((
            par2_rs::VerificationResult {
                files,
                recovery_blocks_available: par2_set.recovery_block_count(),
                total_missing_blocks: 0,
                repairable: par2_rs::verify::Repairability::NotNeeded,
            },
            par2_rs::PlacementPlan {
                exact: par2_set.recovery_file_ids.clone(),
                swaps: Vec::new(),
                renames: Vec::new(),
                unresolved: Vec::new(),
                conflicts: Vec::new(),
            },
            length_checks,
        ))
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

    fn recovery_packet_size(&self, job_id: JobId) -> Option<u64> {
        let par2_set = self.par2_set(job_id)?;
        Some(par2_recovery_packet_size(par2_set.slice_size))
    }

    fn recovery_metadata_overhead_bytes(&self, job_id: JobId) -> Option<u64> {
        let packet_size = self.recovery_packet_size(job_id)?;
        let state = self.jobs.get(&job_id)?;

        let mut overheads = Vec::new();

        if let Some(runtime) = self.par2_runtime(job_id) {
            for (&file_index, file) in &runtime.files {
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
                ) && let Some(total_bytes) = recovery_file_bytes(&state.spec, file_index as u32)
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
    ) -> Option<(u32, RecoveryCountSource)> {
        if let Some(blocks) = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&file_index))
            .map(|file| file.recovery_blocks)
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
            self.recovery_packet_size(job_id),
            self.recovery_metadata_overhead_bytes(job_id),
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

        if !is_par2 {
            return;
        }
        if !is_index && self.par2_set(job_id).is_some() {
            return;
        }

        let parse_path = file_path.clone();
        let (par2_set, packets) = match tokio::task::spawn_blocking(move || {
            let packets = par2_rs::scan_packets_from_path(&parse_path)?
                .into_iter()
                .map(|(packet, _)| packet)
                .collect::<Vec<_>>();
            let set = par2_rs::Par2FileSet::from_packets(packets.clone())?;
            Ok::<_, par2_rs::Par2Error>((set, packets))
        })
        .await
        {
            Ok(Ok(set)) => set,
            Ok(Err(e)) => {
                warn!(filename = %filename, error = %e, "failed to parse PAR2 index");
                return;
            }
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to join PAR2 index parse task");
                return;
            }
        };

        if let Err(error) = self
            .apply_par2_authoritative_identity(job_id, &par2_set)
            .await
        {
            warn!(
                job_id = job_id.0,
                error = %error,
                "failed to apply authoritative PAR2 file identity"
            );
        }

        let slice_size = par2_set.slice_size;
        let recovery_block_count = par2_set.recovery_block_count();

        {
            let runtime = self.ensure_par2_runtime(job_id);
            let entry = runtime.files.entry(file_id.file_index).or_default();
            entry.filename = filename.clone();
            entry.recovery_blocks = recovery_block_count;
        }

        info!(
            job_id = job_id.0,
            filename = %filename,
            slice_size,
            recovery_blocks = recovery_block_count,
            "PAR2 metadata loaded"
        );

        {
            let runtime = self.ensure_par2_runtime(job_id);
            runtime.primary_path = Some(file_path);
            runtime.set = Some(Arc::new(par2_set));
        }
        self.activate_live_par2(job_id, &packets);
        self.run_live_par2_reads(job_id).await;

        let _ = self
            .event_tx
            .send(PipelineEvent::Par2MetadataLoaded { job_id });
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

        let Some(expected_set_id) = self.par2_set(job_id).map(|set| set.recovery_set_id) else {
            return;
        };

        let parse_path = file_path.clone();
        let packet_list = match tokio::task::spawn_blocking(move || {
            par2_rs::scan_packets_from_path_with_set_ids(&parse_path).map(|packets| {
                packets
                    .into_iter()
                    .filter_map(|packet| {
                        (packet.recovery_set_id == expected_set_id).then_some(packet.packet)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .await
        {
            Ok(Ok(packet_list)) => packet_list,
            Ok(Err(e)) => {
                warn!(filename = %filename, error = %e, "failed to parse PAR2 recovery volume");
                return;
            }
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to join PAR2 recovery parse task");
                return;
            }
        };

        self.ensure_par2_runtime(job_id)
            .merged_recovery_paths
            .insert(file_path.clone());
        self.live_par2.merge_packets(job_id, &packet_list);
        if let Some(mut session) = self
            .par2_runtime
            .get_mut(&job_id)
            .and_then(|runtime| runtime.session.take())
        {
            let session_path = file_path.clone();
            match tokio::task::spawn_blocking(move || {
                let result = session.merge_recovery_paths([session_path]);
                (session, result)
            })
            .await
            {
                Ok((session, Ok(_))) => self.restore_par2_repair_session(job_id, session),
                Ok((session, Err(error))) => {
                    self.restore_par2_repair_session(job_id, session);
                    warn!(
                        job_id = job_id.0,
                        filename = %filename,
                        error = %error,
                        "failed to merge recovery path into retained PAR2 session"
                    );
                }
                Err(error) => warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %error,
                    "retained PAR2 recovery merge task panicked; session was evicted"
                ),
            }
        }

        let merge_result = {
            let par2_set = Arc::make_mut(self.ensure_par2_runtime(job_id).set.as_mut().unwrap());
            let merge = par2_set.merge_packets(packet_list);
            let total_recovery = par2_set.recovery_block_count();
            (merge, total_recovery)
        };
        match merge_result {
            (Ok(result), total_recovery) if result.new_recovery_slices > 0 => {
                let promoted = self
                    .par2_runtime(job_id)
                    .and_then(|runtime| runtime.files.get(&file_id.file_index))
                    .is_some_and(|file| file.promoted);
                {
                    let runtime = self.ensure_par2_runtime(job_id);
                    let entry = runtime.files.entry(file_id.file_index).or_default();
                    entry.filename = filename.clone();
                    entry.recovery_blocks = result.new_recovery_slices;
                    entry.promoted = promoted;
                }
                info!(
                    job_id = job_id.0,
                    filename = %filename,
                    recovery_blocks_merged = result.new_recovery_slices,
                    total_recovery,
                    "merged PAR2 recovery volume"
                );
            }
            (Err(e), _) => {
                warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %e,
                    "failed to merge PAR2 recovery volume"
                );
            }
            _ => {}
        }
    }

    fn recovery_candidate_for(&self, job_id: JobId, file_index: u32) -> Option<RecoveryCandidate> {
        let state = self.jobs.get(&job_id)?;
        let total_bytes = recovery_file_bytes(&state.spec, file_index)?;
        let (blocks, source) = self.recovery_block_count_for(job_id, file_index)?;
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

    pub(crate) fn total_recovery_block_capacity(&self, job_id: JobId) -> u32 {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };

        state
            .spec
            .files
            .iter()
            .enumerate()
            .filter_map(|(file_index, _)| self.recovery_block_count_for(job_id, file_index as u32))
            .map(|(blocks, _)| blocks)
            .sum()
    }

    pub(crate) fn recovery_blocks_available_or_targeted(&self, job_id: JobId) -> u32 {
        let mut file_indices = self.loaded_recovery_file_indices(job_id);
        file_indices.extend(self.targeted_recovery_file_indices(job_id));
        file_indices
            .into_iter()
            .filter_map(|file_index| self.recovery_block_count_for(job_id, file_index))
            .map(|(blocks, _)| blocks)
            .sum()
    }

    pub(crate) fn promote_par2_metadata(&mut self, job_id: JobId) -> bool {
        if self.par2_set(job_id).is_some() {
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

        let selected_file = self.jobs.get(&job_id).and_then(|state| {
            let candidate_available = |file_index: u32| {
                state
                    .spec
                    .files
                    .get(file_index as usize)
                    .is_some_and(|file| {
                        file.segments.iter().any(|segment| {
                            let segment_id = SegmentId {
                                file_id: NzbFileId { job_id, file_index },
                                segment_number: segment.ordinal,
                            };
                            !self
                                .unavailable_promoted_recovery_segments
                                .contains(&segment_id)
                        })
                    })
            };

            let mut index_candidates = state
                .spec
                .files
                .iter()
                .enumerate()
                .filter_map(|(file_index, file)| {
                    matches!(
                        file.role,
                        weaver_model::files::FileRole::Par2 { is_index: true, .. }
                    )
                    .then_some(file_index as u32)
                })
                .filter(|file_index| candidate_available(*file_index))
                .collect::<Vec<_>>();
            index_candidates.sort_unstable();
            if let Some(file_index) = index_candidates.into_iter().next() {
                return Some(file_index);
            }

            let mut par2_candidates = state
                .spec
                .files
                .iter()
                .enumerate()
                .filter_map(|(file_index, file)| {
                    matches!(file.role, weaver_model::files::FileRole::Par2 { .. })
                        .then_some(file_index as u32)
                })
                .filter(|file_index| candidate_available(*file_index))
                .collect::<Vec<_>>();
            par2_candidates.sort_unstable();
            par2_candidates.into_iter().next()
        });

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
    pub(crate) fn promote_recovery_targeted(&mut self, job_id: JobId, blocks_needed: u32) -> u32 {
        let already_available_blocks = self.recovery_blocks_available_or_targeted(job_id);
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
            if let Some(candidate) = self.recovery_candidate_for(job_id, file_index)
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
