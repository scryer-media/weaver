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
const PAR2_PACKET_HEADER_BYTES: usize = 64;
const PAR2_MAGIC: &[u8; 8] = b"PAR2\0PKT";
const PAR2_RECOVERY_PACKET_OVERHEAD: u64 = 68; // 64-byte header + 4-byte exponent
const PAR2_RETAINED_SESSION_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const STATEFUL_PAR2_SESSION_ENV: &str = "WEAVER_STATEFUL_PAR2_SESSION";
const PAR2_METADATA_PREFIX_CAP_BYTES: usize = PAR2_HASH_16K_BYTES;

fn par2_prefix_set_ids(prefix: &[u8]) -> Vec<par2_rs::RecoverySetId> {
    let budget = par2_rs::PacketScanBudget::new(par2_rs::PacketScanLimits::default());
    let mut set_ids = Vec::new();
    let mut sink =
        |_: par2_rs::Packet, _: u64, set_id: par2_rs::RecoverySetId| -> par2_rs::Result<()> {
            if !set_ids.contains(&set_id) {
                set_ids.push(set_id);
            }
            Ok(())
        };
    if par2_rs::scan_packets_bounded(prefix, 0, &budget, &mut sink).is_err() {
        return Vec::new();
    }
    set_ids.sort_by_key(|set_id| *set_id.as_bytes());
    set_ids
}

/// A cheap, non-authoritative admission check for a file whose NZB name did
/// not identify it as PAR2. Packet hashes remain the authority: this only
/// decides whether a completed file is worth passing to the authenticated
/// packet scanner.
fn par2_header_looks_valid(prefix: &[u8], known_file_len: Option<u64>) -> bool {
    if prefix.len() < PAR2_PACKET_HEADER_BYTES || &prefix[..8] != PAR2_MAGIC {
        return false;
    }
    let Ok(packet_len) = prefix[8..16].try_into().map(u64::from_le_bytes) else {
        return false;
    };
    packet_len >= PAR2_PACKET_HEADER_BYTES as u64
        && packet_len.is_multiple_of(PAR2_PACKET_ALIGNMENT)
        && known_file_len.is_none_or(|file_len| packet_len <= file_len)
}

/// Whether the retained repair session is in play, from the raw env value.
///
/// On by default, and the default is what makes evidence reachable at all: the
/// one-shot repairer has no seat for evidence in the crate's published API, so
/// a job that routes there re-reads every described file no matter how much the
/// in-stream grid already proved. Only the retained session can be seeded, so
/// only the retained session can make a damaged job read its damaged files and
/// nothing else. The variable remains as an off switch.
fn parse_stateful_par2_session_enabled(raw: Option<&str>) -> bool {
    !matches!(
        raw.map(str::trim),
        Some("0")
            | Some("false")
            | Some("FALSE")
            | Some("no")
            | Some("NO")
            | Some("off")
            | Some("OFF")
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

/// Scan a completed PAR2 carrier into per-set packet groups. The packet scan
/// authenticates metadata, but intentionally defers recovery-payload hashes;
/// validate those here before any caller can merge or count a slice.
fn scan_completed_par2_packet_groups(path: &Path) -> par2_rs::Result<Vec<ParsedPar2Set>> {
    let scanned = par2_rs::scan_packets_from_path_with_set_ids(path)?;
    let mut groups: Vec<ParsedPar2Set> = Vec::new();
    for scanned_packet in scanned {
        let group = if let Some(group) = groups
            .iter_mut()
            .find(|group| group.set_id == scanned_packet.recovery_set_id)
        {
            group
        } else {
            groups.push(ParsedPar2Set {
                set_id: scanned_packet.recovery_set_id,
                packets: Vec::new(),
            });
            groups.last_mut().expect("just pushed a PAR2 packet group")
        };
        let packet_is_valid = match &scanned_packet.packet {
            par2_rs::Packet::RecoverySlice(recovery) => {
                recovery
                    .data
                    .validate_packet_hash(
                        scanned_packet.recovery_set_id.as_bytes(),
                        recovery.exponent,
                    )
                    .ok()
                    == Some(true)
            }
            _ => true,
        };
        if packet_is_valid {
            group.packets.push(scanned_packet.packet);
        }
    }
    Ok(groups)
}

/// Everything a packet merge can add to a set: descriptions, slice checksums,
/// recovery slices and the creator. Two identical readings across a merge mean
/// the merge inserted nothing, so anything already built from the set — a
/// retained repair session above all — still describes it exactly.
fn par2_set_merge_shape(set: &Par2FileSet) -> (usize, usize, usize, bool) {
    (
        set.files.len(),
        set.slice_checksums.len(),
        set.recovery_slices.len(),
        set.creator.is_some(),
    )
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

/// How many recovery blocks a PAR2 file has proven it carries for one set.
///
/// A file that answers to several sets keeps a count per set, because no single
/// number describes it; one that answers to a single set is described by its own
/// validated total. Neither figure is ever derived from a name or a byte size —
/// that is what makes it safe for the arithmetic that decides whether a repair
/// can go ahead.
fn validated_recovery_blocks_for_set(
    file: &Par2FileRuntime,
    set_id: par2_rs::RecoverySetId,
) -> u32 {
    file.recovery_blocks_by_set
        .get(&set_id)
        .copied()
        .unwrap_or(file.validated_recovery_blocks)
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

/// The lowest byte offset covered by a block the recovery set found damaged.
///
/// Split out so the damage floor and the intact prefix can be read from one
/// verdict map rather than two.
fn damage_floor_from_verdicts(
    verdicts: &std::collections::BTreeMap<u32, crate::pipeline::integrity::BlockVerdict>,
    slice_size: u64,
) -> Option<u64> {
    verdicts
        .iter()
        .filter(|(_, verdict)| matches!(verdict, crate::pipeline::integrity::BlockVerdict::Damaged))
        .filter_map(|(&index, _)| u64::from(index).checked_mul(slice_size))
        .min()
}

/// The contiguous run of Intact blocks from block zero, in bytes.
///
/// Only blocks the grid actually claimed *and* found Intact count, and only
/// while they are consecutive from zero: a block the grid never claimed stops
/// the run, because unverified is not the same as intact. An arithmetic
/// overflow ends the run for the same reason — a prefix that cannot be
/// represented has not been proved.
fn intact_prefix_from_verdicts(
    verdicts: &std::collections::BTreeMap<u32, crate::pipeline::integrity::BlockVerdict>,
    slice_size: u64,
) -> u64 {
    let mut prefix = 0u64;
    for index in 0.. {
        match verdicts.get(&index) {
            Some(crate::pipeline::integrity::BlockVerdict::Intact { .. }) => {
                match prefix.checked_add(slice_size) {
                    Some(next) => prefix = next,
                    None => break,
                }
            }
            _ => break,
        }
    }
    prefix
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
        // RAR topologies only. This map is the last fallback in the search for a
        // file's *old RAR set name*, and everything downstream of that name —
        // staleness, the retirement sweep at the end of this function — speaks
        // only RAR. A 7z volume that found its own 7z set here was answering a
        // question about RAR with a fact about 7z, and the set it named was then
        // swept away as a stale RAR set: every PAR2 registration silently
        // deleted every non-RAR topology in the job, and only the next file
        // completion rebuilding it hid that.
        let old_set_by_topology_filename: HashMap<String, String> = state
            .assembly
            .archive_topologies()
            .iter()
            .filter(|(_, topology)| {
                matches!(
                    topology.archive_type,
                    crate::jobs::assembly::ArchiveType::Rar
                )
            })
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
                // The one rename that fires *during* the download pass, so it
                // is the only PAR2 site that can move a part while a chase is
                // still reading it rather than after.
                if let Some(name) = old_path.file_name().and_then(|name| name.to_str()) {
                    let name = name.to_string();
                    self.taint_direct_unpack_for_file(job_id, &name);
                }
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

            // Staleness is bookkeeping *about a rebind*, so it belongs after the
            // check for whether one happened. It used to run above the `continue`
            // and therefore fired on every identity application, including the
            // overwhelming majority that come out byte-identical and change
            // nothing — which meant the retirement sweep at the end of this
            // function ran against sets no rebind had touched, with `rebound`
            // still zero and nothing logged.
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
            // A set the rebind left stale retires here when nothing holds it;
            // when something does, the extraction path rules on it later.
            let _ = self.clear_archive_set_if_unreferenced_and_idle(job_id, &set_name);
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
        }) || self
            .par2_runtime(job_id)
            .is_some_and(|runtime| runtime.files.values().any(|file| file.signature_candidate))
    }

    fn is_par2_signature_eligible_role(role: &weaver_model::files::FileRole) -> bool {
        matches!(
            role,
            weaver_model::files::FileRole::Unknown | weaver_model::files::FileRole::Standalone
        )
    }

    /// Records a structurally plausible PAR2 header from an obfuscated file.
    ///
    /// This is deliberately only admission evidence. A later whole-file packet
    /// scan must authenticate every packet before metadata, recovery capacity,
    /// or a canonical name becomes authoritative.
    pub(crate) fn note_par2_metadata_signature(
        &mut self,
        file_id: NzbFileId,
        known_file_len: Option<u64>,
    ) {
        let Some((filename, role, prefix)) = self.jobs.get(&file_id.job_id).and_then(|state| {
            state.assembly.file(file_id).map(|file| {
                (
                    self.current_filename_for_file(file_id.job_id, file),
                    file.role().clone(),
                    self.file_prefix_16k.get(&file_id).cloned(),
                )
            })
        }) else {
            return;
        };
        let Some(prefix) = prefix else {
            return;
        };
        if !Self::is_par2_signature_eligible_role(&role)
            || !par2_header_looks_valid(&prefix, known_file_len)
        {
            return;
        }

        let set_ids = par2_prefix_set_ids(&prefix);
        if !set_ids.is_empty() {
            self.note_foreign_recovery_set_sightings(file_id.job_id, file_id.file_index, &set_ids);
        }
        let entry = self
            .ensure_par2_runtime(file_id.job_id)
            .files
            .entry(file_id.file_index)
            .or_default();
        if entry.signature_candidate {
            return;
        }
        entry.filename = filename;
        entry.signature_candidate = true;
        entry.discovery = if set_ids.is_empty() {
            Par2DiscoveryState::ProbeInconclusive
        } else {
            Par2DiscoveryState::PrefixProbed { set_ids }
        };
    }

    /// Restored jobs have no in-memory decode prefix. Probe only the fixed
    /// PAR2 header for their already-complete eligible files, from the normal
    /// completion path rather than startup recovery.
    pub(crate) async fn probe_restored_par2_headers(&mut self, job_id: JobId) {
        let candidates = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .filter(|file| {
                        file.is_complete()
                            && Self::is_par2_signature_eligible_role(file.role())
                            && !self.file_prefix_16k.contains_key(&file.file_id())
                    })
                    .map(|file| {
                        (
                            file.file_id(),
                            state
                                .working_dir
                                .join(self.current_filename_for_file(job_id, file)),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if candidates.is_empty() {
            return;
        }

        let scanned = match tokio::task::spawn_blocking(move || {
            use std::io::Read;

            candidates
                .into_iter()
                .filter_map(|(file_id, path)| {
                    let mut file = std::fs::File::open(&path).ok()?;
                    let file_len = file.metadata().ok()?.len();
                    let mut prefix = vec![0; PAR2_PACKET_HEADER_BYTES];
                    let read = file.read(&mut prefix).ok()?;
                    prefix.truncate(read);
                    Some((file_id, prefix, file_len))
                })
                .collect::<Vec<_>>()
        })
        .await
        {
            Ok(scanned) => scanned,
            Err(error) => {
                warn!(job_id = job_id.0, %error, "failed to join restored PAR2 header probe");
                return;
            }
        };

        for (file_id, prefix, file_len) in scanned {
            self.file_prefix_16k.entry(file_id).or_insert(prefix);
            self.note_par2_metadata_signature(file_id, Some(file_len));
            if self
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&file_id.file_index))
                .is_some_and(|file| file.signature_candidate)
            {
                self.try_load_par2_metadata(job_id, file_id).await;
            }
        }
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

    /// Admit newly parsed PAR2 slice sizes and publish one immutable snapshot
    /// for future leases. Existing grids never disappear mid-job: an older
    /// batch may still carry evidence for them, while a newly learned grid is
    /// safe only for later batches.
    pub(crate) fn refresh_par2_checkpoint_plan(&mut self, job_id: JobId) {
        let Some(runtime) = self.par2_runtime(job_id) else {
            return;
        };
        if runtime.admitted_checkpoint_sizes.len() > weaver_yenc::MAX_CHECKPOINT_GRIDS {
            return;
        }
        let mut sizes = runtime.admitted_checkpoint_sizes.clone();
        for slice_size in runtime
            .sets
            .values()
            .filter_map(|set_runtime| set_runtime.set.as_deref())
            .map(|set| set.slice_size)
        {
            sizes.insert(slice_size);
            if sizes.len() > weaver_yenc::MAX_CHECKPOINT_GRIDS {
                break;
            }
        }
        if sizes == runtime.admitted_checkpoint_sizes {
            return;
        }
        let build = weaver_yenc::CheckpointPlan::from_slice_sizes(sizes.iter().copied());
        if crate::runtime::perf_probe::enabled() {
            let shape = match &build.plan {
                weaver_yenc::CheckpointPlan::None => "par2.checkpoint_plan.build.none",
                weaver_yenc::CheckpointPlan::Single(_) => "par2.checkpoint_plan.build.single",
                weaver_yenc::CheckpointPlan::Multi(_) => "par2.checkpoint_plan.build.multi",
            };
            crate::runtime::perf_probe::record_value(shape, 1);
            crate::runtime::perf_probe::record_value(
                "par2.checkpoint_plan.grid_count",
                build.plan.grid_count() as u64,
            );
            if let Some(reason) = build.degradation {
                let label = match reason {
                    weaver_yenc::CheckpointPlanDegradation::TooManyGrids => {
                        "par2.checkpoint_plan.degraded.too_many_grids"
                    }
                    weaver_yenc::CheckpointPlanDegradation::InvalidSliceSize => {
                        "par2.checkpoint_plan.degraded.invalid_slice_size"
                    }
                };
                crate::runtime::perf_probe::record_value(label, 1);
            }
        }
        let degraded = build.degradation.is_some();
        let runtime = self.ensure_par2_runtime(job_id);
        runtime.admitted_checkpoint_sizes = sizes;
        runtime.checkpoint_plan = Some(build.plan);
        if degraded {
            crate::runtime::perf_probe::record(
                "par2.checkpoint_plan.degraded",
                std::time::Duration::from_nanos(1),
            );
        }
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
        #[cfg(test)]
        self.par2_binding_resolver_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

    fn resolve_par2_md5_substitution_binding(
        &self,
        file_id: NzbFileId,
    ) -> Option<crate::pipeline::Par2Md5SubstitutionBinding> {
        let binding = self.resolve_par2_file_binding(file_id)?;
        self.par2_set_for(file_id.job_id, binding.recovery_set_id)
            .is_some_and(|set| {
                !set.slice_checksums.is_empty()
                    && set.file_description(&binding.par2_file_id).is_some()
            })
            .then_some(crate::pipeline::Par2Md5SubstitutionBinding {
                recovery_set_id: binding.recovery_set_id,
                par2_file_id: binding.par2_file_id,
            })
    }

    /// Rebuild the positive-only MD5-substitution cache after a bounded
    /// metadata or identity transition. Articles only read this result.
    pub(crate) fn refresh_par2_md5_substitution_bindings(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let file_ids = state
            .assembly
            .files()
            .map(|file| file.file_id())
            .collect::<Vec<_>>();
        let bindings = file_ids
            .into_iter()
            .filter_map(|file_id| {
                self.resolve_par2_md5_substitution_binding(file_id)
                    .map(|binding| (file_id, binding))
            })
            .collect();
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id) {
            runtime.md5_substitution_bindings = bindings;
        }
    }

    pub(crate) fn refresh_par2_md5_substitution_binding(&mut self, file_id: NzbFileId) {
        let binding = self.resolve_par2_md5_substitution_binding(file_id);
        let Some(runtime) = self.par2_runtime.get_mut(&file_id.job_id) else {
            return;
        };
        match binding {
            Some(binding) => {
                runtime.md5_substitution_bindings.insert(file_id, binding);
            }
            None => {
                runtime.md5_substitution_bindings.remove(&file_id);
            }
        }
    }

    pub(crate) fn par2_md5_substitution_is_cached(&self, file_id: NzbFileId) -> bool {
        let Some(binding) = self
            .par2_runtime(file_id.job_id)
            .and_then(|runtime| runtime.md5_substitution_bindings.get(&file_id))
        else {
            return false;
        };
        self.par2_set_for(file_id.job_id, binding.recovery_set_id)
            .is_some_and(|set| {
                !set.slice_checksums.is_empty()
                    && set.file_description(&binding.par2_file_id).is_some()
            })
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
    #[cfg(test)]
    pub(crate) fn par2_block_size(&self, job_id: JobId) -> Option<std::num::NonZeroU64> {
        std::num::NonZeroU64::new(self.par2_set(job_id)?.slice_size)
    }

    /// The common-refinement checkpoint geometry for every parsed set known
    /// when a batch is leased. Served-set selection is a UI/repair view and
    /// must not remove geometry needed by another file in the same job.
    pub(crate) fn par2_checkpoint_plan(&self, job_id: JobId) -> weaver_yenc::CheckpointPlan {
        self.par2_runtime(job_id)
            .and_then(|runtime| runtime.checkpoint_plan.clone())
            .unwrap_or(weaver_yenc::CheckpointPlan::None)
    }

    /// The block size of the recovery set that currently owns `file_id`.
    ///
    /// An unbound file deliberately records no grid evidence. Binding becomes
    /// available when its name matches a description or when its captured 16
    /// KiB prefix arrives, which is also the first moment a grid claim could be
    /// useful. In particular, we must not cut an earlier article on the served
    /// set's grid and later reinterpret it after the file binds elsewhere.
    #[cfg(test)]
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
    #[cfg(test)]
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
        self.note_block_crc_segments_for_plan(
            file_id,
            &weaver_yenc::CheckpointPlan::Single(block_size),
            file_offset,
            decoded_len,
            part_crc,
            part_crc_verified,
            was_duplicate,
            segments,
        );
    }

    /// Record an article using exactly the checkpoint geometry that its decoder
    /// applied. Evidence is offered independently of binding, so article
    /// commits do not scan recovery-set metadata or hash a prefix.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn note_block_crc_segments_for_plan(
        &mut self,
        file_id: NzbFileId,
        checkpoint_plan: &weaver_yenc::CheckpointPlan,
        file_offset: u64,
        decoded_len: u64,
        part_crc: u32,
        part_crc_verified: bool,
        was_duplicate: bool,
        segments: &[weaver_yenc::Segment],
    ) {
        if checkpoint_plan.is_none() {
            return;
        }
        self.block_crcs.note_article_on_grids(
            file_id,
            checkpoint_plan.sizes(),
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

    /// Both in-stream facts a chased part needs, from a single verdict build.
    ///
    /// The damage floor and the intact prefix are read together on every commit
    /// of a gated part, and each derives from the same `verdicts_against` map.
    /// Asking for them separately built that map twice per commit; this builds
    /// it once. `None` means the file has no binding or no parsed set, which is
    /// the same "nothing vouches for this" the individual accessors report.
    ///
    /// The build itself remains O(closed blocks) per call, and the map is
    /// rebuilt rather than cached because the grid is still accumulating — a
    /// cached answer is exactly the stale one the gate must not act on. So the
    /// honest bound over a gated part's life is O(articles x blocks). It is
    /// paid only by sets already known to carry damage.
    pub(crate) fn in_stream_chase_evidence(
        &self,
        file_id: NzbFileId,
    ) -> Option<(Option<u64>, u64)> {
        let binding = self.resolve_par2_file_binding(file_id)?;
        let set = self.par2_set_for(file_id.job_id, binding.recovery_set_id)?;
        let slice = set.slice_size;
        let verdicts = self
            .block_crcs
            .verdicts_against(file_id, set, binding.par2_file_id);
        Some((
            damage_floor_from_verdicts(&verdicts, slice),
            intact_prefix_from_verdicts(&verdicts, slice),
        ))
    }

    /// How many bytes from the start of a file the recovery set has positively
    /// vouched for, as a contiguous run.
    ///
    /// Only blocks the grid actually claimed *and* found Intact count, and only
    /// while they are consecutive from block zero. A block the grid never
    /// claimed — `NoReference`, or simply absent because no article closed it —
    /// stops the run: unverified is not the same as intact, and this number is
    /// used to decide that repair cannot touch what the decoder already read.
    ///
    /// The basis is CRC32, from PAR2's IFSC. A block whose CRC32 matches but
    /// whose MD5 does not would be rewritten by repair below this line, and the
    /// chase would keep a decode of the pre-repair bytes. Downstream 7z entry
    /// CRCs catch that for entries that carry one; entries without a CRC carry
    /// the residual. Stated rather than defended against: closing it means
    /// hashing the prefix, which is the read this whole path exists to avoid.
    pub(crate) fn in_stream_intact_prefix(&self, file_id: NzbFileId) -> Option<u64> {
        let binding = self.resolve_par2_file_binding(file_id)?;
        let set = self.par2_set_for(file_id.job_id, binding.recovery_set_id)?;
        let slice = set.slice_size;
        Some(intact_prefix_from_verdicts(
            &self
                .block_crcs
                .verdicts_against(file_id, set, binding.par2_file_id),
            slice,
        ))
    }

    /// A completed file's PAR2 identity, provable from the in-stream dual-CRC
    /// grid alone.
    ///
    /// `Some` means: the file binds uniquely by name/identity within this
    /// recovery set, its assembled length equals the described length exactly,
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
        let binding = self.resolve_par2_file_binding_in_set(file_id, par2_set.recovery_set_id)?;
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
        let verdicts = self
            .block_crcs
            .verdicts_against(file_id, par2_set, binding.par2_file_id);
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

    /// Why [`Self::in_stream_verified_par2_match`] would refuse this file —
    /// `None` when it would claim. Diagnostics only: the answer names the
    /// FIRST failing rung of the same ladder the predicate walks, so a
    /// production log can say why a healthy-looking job still paid a read
    /// instead of leaving it a mystery gap in the timeline.
    pub(crate) fn in_stream_par2_claim_shortfall(
        &self,
        file_id: NzbFileId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Option<&'static str> {
        let Some(binding) =
            self.resolve_par2_file_binding_in_set(file_id, par2_set.recovery_set_id)
        else {
            return Some("no_binding");
        };
        let Some(description) = par2_set.file_description(&binding.par2_file_id) else {
            return Some("no_description");
        };
        if description.length == 0 {
            return Some("zero_length_description");
        }
        let Some(file) = self
            .jobs
            .get(&file_id.job_id)
            .and_then(|state| state.assembly.file(file_id))
        else {
            return Some("no_assembly_file");
        };
        if !binding.is_complete {
            return Some("binding_incomplete");
        }
        if file.received_bytes() != description.length {
            return Some("length_mismatch");
        }
        let verdicts = self
            .block_crcs
            .verdicts_against(file_id, par2_set, binding.par2_file_id);
        let slice_count = par2_set.slice_count_for_file(description.length);
        let mut unverdicted = false;
        let mut dependent = false;
        for slice_index in 0..slice_count {
            match verdicts.get(&slice_index) {
                Some(crate::pipeline::integrity::BlockVerdict::Intact {
                    independently_covered: true,
                }) => {}
                Some(crate::pipeline::integrity::BlockVerdict::Intact {
                    independently_covered: false,
                }) => dependent = true,
                Some(crate::pipeline::integrity::BlockVerdict::Damaged) => {
                    return Some("slices_damaged");
                }
                _ => unverdicted = true,
            }
        }
        if unverdicted {
            return Some("slices_unverdicted");
        }
        if dependent {
            return Some("slices_dependent_coverage");
        }
        None
    }

    /// Per-slice grid evidence for one bound file, in the shape
    /// [`par2_rs::VerifyOptions`]'s `proven_slices` takes: one entry per slice
    /// of the described file, `true` only for a slice the grid proved `Intact`
    /// with independent (pCRC-verified) article coverage — the same bar
    /// [`Self::in_stream_verified_par2_match`] demands of every slice, applied
    /// per slice. The eligibility rungs are the match predicate's own: a
    /// binding, a non-empty description, a complete binding at exactly the
    /// described length. `None` when a rung fails or when no slice is proven —
    /// to the verifier, absent and all-`false` are the same statement.
    ///
    /// This is what lets a file the grid could *not* claim whole — one damaged
    /// or unverdicted slice vetoes the whole-file claim — still hand the
    /// verify pass everything the grid did prove, so the pass reads only the
    /// slices in question instead of the whole file.
    pub(crate) fn in_stream_proven_slices(
        &self,
        file_id: NzbFileId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Option<(par2_rs::FileId, Vec<bool>)> {
        let binding = self.resolve_par2_file_binding_in_set(file_id, par2_set.recovery_set_id)?;
        let description = par2_set.file_description(&binding.par2_file_id)?;
        if description.length == 0 {
            return None;
        }
        let state = self.jobs.get(&file_id.job_id)?;
        let file = state.assembly.file(file_id)?;
        if !binding.is_complete || file.received_bytes() != description.length {
            return None;
        }
        let verdicts = self
            .block_crcs
            .verdicts_against(file_id, par2_set, binding.par2_file_id);
        let slice_count = par2_set.slice_count_for_file(description.length);
        let proven: Vec<bool> = (0..slice_count)
            .map(|slice_index| {
                matches!(
                    verdicts.get(&slice_index),
                    Some(crate::pipeline::integrity::BlockVerdict::Intact {
                        independently_covered: true
                    })
                )
            })
            .collect();
        if proven.iter().any(|slice_proven| *slice_proven) {
            Some((binding.par2_file_id, proven))
        } else {
            None
        }
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
        self.in_stream_slice_evidence_by_file(job_id, set_id)
            .into_iter()
            .flat_map(|(_, evidence)| evidence)
            .collect()
    }

    /// [`Self::in_stream_slice_evidence_for_set`] keyed by the path each file
    /// actually occupies, for a session that finds its sources in the directory
    /// rather than through a handle.
    ///
    /// A path-backed session refuses the `FileId`-keyed seat outright, so the
    /// conventional pass has to name a path — and the only path that names the
    /// bytes is the file's *effective* identity. A file the deobfuscation or
    /// reconciliation passes renamed is seeded under the name it now carries,
    /// because that is the name the session will open; seeding the NZB's
    /// original name would attach every verdict to a path that no longer
    /// exists, and the file would be read in full after all.
    pub(crate) fn in_stream_slice_evidence_paths_for_set(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> Vec<(std::path::PathBuf, Vec<par2_rs::SliceEvidence>)> {
        let Some(working_dir) = self
            .jobs
            .get(&job_id)
            .map(|state| state.working_dir.clone())
        else {
            return Vec::new();
        };
        self.in_stream_slice_evidence_by_file(job_id, set_id)
            .into_iter()
            .filter_map(|(file_id, evidence)| {
                if evidence.is_empty() {
                    return None;
                }
                let current_filename = self
                    .effective_file_identity(job_id, file_id)
                    .map(|identity| identity.current_filename)
                    .or_else(|| {
                        self.jobs
                            .get(&job_id)?
                            .assembly
                            .file(file_id)
                            .map(|file| file.filename().to_string())
                    })?;
                Some((working_dir.join(current_filename), evidence))
            })
            .collect()
    }

    /// The per-file grouping both evidence shapes are built from.
    fn in_stream_slice_evidence_by_file(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> Vec<(NzbFileId, Vec<par2_rs::SliceEvidence>)> {
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
            let file_evidence = crate::pipeline::integrity::slice_evidence_from_verdicts(
                recovery_set_id,
                binding.par2_file_id,
                length,
                slice_size,
                &verdicts,
            );
            if !file_evidence.is_empty() {
                evidence.push((file_id, file_evidence));
            }
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

    /// The files under this job's working directory that the named recovery
    /// set's extra scan must not read.
    ///
    /// An extra candidate is a file the set does not describe, rolling-scanned
    /// window by window on the chance that it holds a copy of some slice.
    /// Finding a renamed or concatenated source that way is the entire point,
    /// so anything this cannot positively place elsewhere stays discoverable —
    /// an obfuscated file with no binding at all is exactly what extras exist
    /// for, and excluding it would be excluding the answer.
    ///
    /// Two things can be positively placed elsewhere:
    ///
    ///  - A file that binds to a *different* recovery set. The binding
    ///    resolver refuses a name two sets both answer to, so a binding that
    ///    names another set is unambiguous: those bytes are that set's
    ///    payload, and a slice of this set cannot be inside them at any offset.
    ///  - A complete volume of a RAR set other than the one this recovery set
    ///    describes. Applied only when at least one file bound to this set
    ///    carries a RAR classification, because that is what says which
    ///    archive set the recovery set is for; with nothing to compare against,
    ///    every volume stays discoverable. Incomplete volumes are left alone: a
    ///    file still being written is not yet the archive its name claims, and
    ///    its bytes may still be rearranged.
    ///
    /// Without this, a directory holding two recovery sets makes each set read
    /// the other set's whole payload, once per scanning pass, and match nothing
    /// both times.
    pub(crate) fn par2_extra_scan_exclusions(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> Vec<PathBuf> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        let file_ids: Vec<NzbFileId> = state.assembly.files().map(|file| file.file_id()).collect();
        let mut excluded: Vec<PathBuf> = Vec::new();
        let mut own_paths: HashSet<PathBuf> = HashSet::new();
        let mut own_rar_sets: HashSet<String> = HashSet::new();
        let mut unbound_rar: Vec<(NzbFileId, String)> = Vec::new();
        for file_id in file_ids {
            let rar_set_name = self
                .effective_file_identity(job_id, file_id)
                .and_then(|identity| identity.classification)
                .and_then(|classification| {
                    matches!(
                        classification.kind,
                        crate::jobs::assembly::DetectedArchiveKind::Rar
                    )
                    .then_some(classification.set_name)
                });
            match self.resolve_par2_file_binding(file_id) {
                Some(binding) if binding.recovery_set_id == set_id => {
                    own_paths.insert(binding.path);
                    if let Some(set_name) = rar_set_name {
                        own_rar_sets.insert(set_name);
                    }
                }
                Some(binding) => excluded.push(binding.path),
                None => {
                    if let Some(set_name) = rar_set_name {
                        unbound_rar.push((file_id, set_name));
                    }
                }
            }
        }
        if !own_rar_sets.is_empty() {
            for (file_id, set_name) in unbound_rar {
                if own_rar_sets.contains(&set_name) {
                    continue;
                }
                let Some(state) = self.jobs.get(&job_id) else {
                    break;
                };
                let Some(file) = state.assembly.file(file_id) else {
                    continue;
                };
                if !file.is_complete() {
                    continue;
                }
                excluded.push(
                    state
                        .working_dir
                        .join(self.current_filename_for_file(job_id, file)),
                );
            }
        }
        // A path this set itself resolves to is never an exclusion, whatever
        // else claimed it: the set's own sources are scanned as canonical
        // candidates and are not extras in the first place.
        excluded.retain(|path| !own_paths.contains(path));
        excluded.sort();
        excluded.dedup();
        excluded
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
        // Which of this job's files belong to somebody else, computed here so
        // every consumer of a retained session gets the same answer: a reused
        // session is re-pointed at the current list, and a fresh one opens with
        // it already applied.
        let exclude_paths = self.par2_extra_scan_exclusions(job_id, set_id);
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
                        session.set_exclude_paths(exclude_paths);
                        set_runtime.session_last_used = Some(Instant::now());
                        return Ok(Some((session, false)));
                    }
                    (None, false) => {
                        session.set_exclude_paths(exclude_paths);
                        set_runtime.session_last_used = Some(Instant::now());
                        return Ok(Some((session, false)));
                    }
                    _ => {}
                }
            }
        }

        let Some(par2_set) = self.par2_runtime(job_id).and_then(|runtime| {
            runtime
                .set_runtime(set_id)
                .and_then(|set_runtime| set_runtime.set.as_deref())
                .cloned()
        }) else {
            return Ok(None);
        };
        let cancellation = self.par2_cancellation_token(job_id);

        let session_result = tokio::task::spawn_blocking(move || {
            let mut options = match source_access {
                Some(access) => {
                    par2_rs::Par2RepairSessionOptions::from_set(working_dir, par2_set, access)
                }
                None => {
                    let mut options =
                        par2_rs::Par2RepairSessionOptions::new(working_dir, Vec::new());
                    options.file_set = Some(par2_set);
                    options
                }
            };
            options.memory_limit = Some(memory_limit);
            options.cancel = Some(cancellation);
            options.progress = progress;
            // Files this job holds that belong to a *different* recovery set,
            // or to a different archive set than the one this recovery set
            // describes. Their bytes cannot contain this set's slices at any
            // offset, so rolling-scanning them as extra candidates is a
            // whole-payload read that can only ever find nothing.
            options.exclude_paths = exclude_paths;
            // The in-stream grid seeds this session with per-slice verdicts, so
            // the analysis need not re-read what those verdicts already account
            // for. Every seeded verdict is admitted only on dual-alignment
            // evidence, and the crate re-stats each path before honouring a
            // skip, so a file that moved under us is read in full instead.
            options.trust_seeded_evidence_for_scan = true;
            par2_rs::Par2RepairSession::open(options)
                .map_err(|error| format!("failed to open retained PAR2 session: {error}"))
        })
        .await
        .map_err(|error| format!("retained PAR2 session task panicked: {error}"))??;

        Ok(Some((session_result, true)))
    }

    pub(crate) fn par2_cancellation_token(&mut self, job_id: JobId) -> par2_rs::CancellationToken {
        if let Some(cancellation) = self.par2_cancellations.get(&job_id) {
            return cancellation.clone();
        }
        let cancellation = par2_rs::CancellationToken::new();
        let callback_cancellation = cancellation.clone();
        self.shared_state.register_job_cancellation(
            job_id,
            std::sync::Arc::new(move || callback_cancellation.cancel()),
        );
        self.par2_cancellations.insert(job_id, cancellation.clone());
        cancellation
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

    /// Teach the retained session about a recovery volume that just landed,
    /// instead of throwing the session away for being a set behind.
    ///
    /// Only for a set holding a parked damaged-path verdict — the one shape
    /// where the session is carrying an analysis the job is about to repair on,
    /// and where losing it means reading every damaged file a second time.
    /// Everything else keeps the eviction: it is free, and a session rebuilt
    /// from the merged set is always correct.
    ///
    /// par2-rs draws the same distinction internally. Recovery-only packets
    /// leave the source scan standing, so the repair's analysis re-uses it and
    /// reads nothing; a volume that turns out to carry *new* descriptions
    /// rebuilds the source map and costs the scan anyway, which is the right
    /// answer because the protected file set itself changed.
    ///
    /// Returns whether the session may stand. `false` means the caller must
    /// evict, which is also what every refusal below leaves behind: the session
    /// has already been taken out of the runtime by then.
    async fn merge_recovery_into_retained_par2_session(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        volume_path: std::path::PathBuf,
    ) -> bool {
        if !self.has_pending_par2_repair(job_id, set_id) {
            return false;
        }
        let Some(set_runtime) = self
            .par2_runtime
            .get_mut(&job_id)
            .and_then(|runtime| runtime.set_runtime_mut(set_id))
        else {
            return false;
        };
        let Some(session) = set_runtime.session.take() else {
            return false;
        };
        // An access-backed session serves volumes that have no path, and this
        // merge names one. A direct set reaching here is demoted before any
        // repair anyway, so there is nothing to preserve.
        if session.is_access_backed() {
            return false;
        }
        let merged = tokio::task::spawn_blocking(move || {
            let mut session = session;
            let result = session.merge_recovery_paths([volume_path]);
            (session, result)
        })
        .await;
        match merged {
            Ok((session, Ok(result))) => {
                debug!(
                    job_id = job_id.0,
                    recovery_set_id = %set_id,
                    recovery_blocks_merged = result.new_recovery_slices,
                    "merged a landed recovery volume into the retained PAR2 session"
                );
                #[cfg(test)]
                {
                    self.par2_session_recovery_merges += 1;
                }
                self.restore_par2_repair_session(job_id, set_id, session);
                true
            }
            Ok((_, Err(error))) => {
                debug!(
                    job_id = job_id.0,
                    recovery_set_id = %set_id,
                    error = %error,
                    "retained PAR2 session refused a landed recovery volume; rebuilding it"
                );
                false
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    recovery_set_id = %set_id,
                    error = %error,
                    "retained PAR2 session merge task panicked; rebuilding the session"
                );
                false
            }
        }
    }

    /// The retained session owns a snapshot of the validated set. Packet
    /// arrivals update that set first, then make the snapshot stale.
    pub(in crate::pipeline) fn evict_par2_repair_session(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) {
        let Some(set_runtime) = self
            .par2_runtime
            .get_mut(&job_id)
            .and_then(|runtime| runtime.set_runtime_mut(set_id))
        else {
            return;
        };
        set_runtime.session = None;
        set_runtime.session_last_used = None;
        set_runtime.session_evidence_file_ids.clear();
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
        // A damaged-path analysis reading right now is reading the bytes this
        // write replaces, so its verdict would name a file state that no longer
        // exists. Drop the ticket; the completion check submits a fresh read.
        self.forget_par2_analysis_work(file_id.job_id);
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
        // Same reason a retained location is discarded here: an analysis in
        // flight was handed the paths the old identities produced, and the
        // verdict it brings back would decide a repair against names that have
        // since moved. Forgetting it makes the completion check re-read.
        self.forget_par2_analysis_work(job_id);
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
                // What the file proved, when it has proved anything. A volume
                // whose packets were read answers this exactly; one that has
                // only been named or sized answers it as well as it can.
                let blocks = if file.validated_recovery_blocks > 0 {
                    file.validated_recovery_blocks
                } else {
                    file.recovery_blocks
                };
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
        if let Some(file) = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&file_index))
        {
            if let Some(&validated) = file.recovery_blocks_by_set.get(&set_id) {
                return Some((validated, RecoveryCountSource::Exact));
            }
            // A completed scan answered the capacity question even when it
            // found no usable recovery.  Filename counts are predictions for
            // unread volumes only; after this point zero is the safe answer.
            if file.recovery_capacity_accounted {
                return Some((
                    validated_recovery_blocks_for_set(file, set_id),
                    RecoveryCountSource::Exact,
                ));
            }
            let validated = validated_recovery_blocks_for_set(file, set_id);
            if validated > 0 {
                return Some((validated, RecoveryCountSource::Exact));
            }
            // Nothing of this file has been read yet, so what it says about
            // itself is the best answer available — and it is an estimate, said
            // so, rather than a count the repair arithmetic may bank on.
            if file.recovery_blocks > 0 {
                return Some((file.recovery_blocks, RecoveryCountSource::FilenameFallback));
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
}

mod promotion;
mod recovery_sets;

#[cfg(test)]
mod tests;
