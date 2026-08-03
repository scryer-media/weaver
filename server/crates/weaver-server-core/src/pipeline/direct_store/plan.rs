//! Set admission and destination naming (plan 135, D1/D3/D6).
//!
//! A direct set is admitted from the **job spec alone**, before a byte lands:
//! every candidate volume is an NZB file whose role already says it is a RAR
//! volume, so the volume-to-file mapping the coverage barrier needs exists
//! without waiting for anything to complete. Everything the *layout* decides —
//! members, eligibility, extents — happens later, in the router.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use weaver_model::files::{FileRole, archive_base_name};

use crate::jobs::model::JobSpec;

/// Bytes of the envelope file reserved per source volume.
///
/// The envelope file is addressed deterministically — slot base plus the run's
/// offset — rather than by append order, so the same bytes land at the same
/// place after a restart. A volume whose envelope does not fit its slot is not
/// the "headers only" shape phase 4 admits, and demotes.
pub(crate) const ENVELOPE_SLOT_BYTES: u64 = 64 * 1024;

/// Half a slot: the head of a volume (signature, main and file headers) takes
/// the low half and its trailer (service blocks, end-of-archive record) the
/// high half, so neither can grow into the other.
pub(crate) const ENVELOPE_SLOT_HALF: u64 = ENVELOPE_SLOT_BYTES / 2;

/// Why a candidate set was not admitted. Reported as a metric so
/// `sets == direct + materialized` stays checkable (D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionRefusal {
    /// Two NZB files claim the same volume index.
    DuplicateVolume,
    /// The volume indices are not `0..n`.
    VolumeGap,
    /// The set has no volumes at all.
    Empty,
    /// The job carries PAR2 recovery files.
    ///
    /// Every PAR2 path — the live short-circuit, the quick verify and the full
    /// analyze — reads the *volume files* the set's descriptions name, and a
    /// direct set has none: the analyze runs over zero volume files and reports
    /// "not repairable", so a par2-bearing job that routed directly could never
    /// complete. Phase 5's hybrid provider, which answers those reads from the
    /// envelope plus the routed member bytes, is what unlocks this; until then
    /// the job stays conventional end to end.
    Par2Present,
}

impl AdmissionRefusal {
    pub(crate) fn metric(self) -> &'static str {
        match self {
            Self::DuplicateVolume => "duplicate_volume",
            Self::VolumeGap => "volume_gap",
            Self::Empty => "empty_set",
            Self::Par2Present => "par2_present",
        }
    }
}

/// One admitted archive set: its identity, its volume-to-file mapping and the
/// working directory its destinations are relative to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectSetPlan {
    pub(crate) set_name: String,
    /// Volume index to NZB file index. Dense from zero.
    pub(crate) volumes: BTreeMap<u32, u32>,
    /// NZB file index to volume index — the direction the decode seam asks in.
    pub(crate) files: HashMap<u32, u32>,
    pub(crate) working_dir: PathBuf,
}

impl DirectSetPlan {
    /// Every RAR set the spec declares, admitted or refused.
    pub(crate) fn discover(
        spec: &JobSpec,
        working_dir: &Path,
    ) -> (Vec<Self>, Vec<(String, AdmissionRefusal)>) {
        let mut candidates: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
        for (file_index, file) in spec.files.iter().enumerate() {
            let FileRole::RarVolume { volume_number } = file.role else {
                continue;
            };
            let Some(set_name) = archive_base_name(&file.filename, &file.role) else {
                continue;
            };
            candidates
                .entry(set_name)
                .or_default()
                .push((volume_number, file_index as u32));
        }

        // A whole-job refusal, decided before any per-set shape is looked at:
        // PAR2 covers the *volumes*, and repair reads files direct routing
        // never creates. See [`AdmissionRefusal::Par2Present`].
        let par2_present = spec
            .files
            .iter()
            .any(|file| matches!(file.role, FileRole::Par2 { .. }));

        let mut admitted = Vec::new();
        let mut refused = Vec::new();
        for (set_name, mut entries) in candidates {
            if par2_present {
                refused.push((set_name, AdmissionRefusal::Par2Present));
                continue;
            }
            entries.sort_unstable();
            if entries.is_empty() {
                refused.push((set_name, AdmissionRefusal::Empty));
                continue;
            }
            let mut volumes = BTreeMap::new();
            let mut duplicate = false;
            for (volume_index, file_index) in &entries {
                if volumes.insert(*volume_index, *file_index).is_some() {
                    duplicate = true;
                }
            }
            if duplicate {
                refused.push((set_name, AdmissionRefusal::DuplicateVolume));
                continue;
            }
            if volumes
                .keys()
                .enumerate()
                .any(|(position, volume)| position as u32 != *volume)
            {
                refused.push((set_name, AdmissionRefusal::VolumeGap));
                continue;
            }
            let files = volumes
                .iter()
                .map(|(volume, file)| (*file, *volume))
                .collect();
            admitted.push(Self {
                set_name,
                volumes,
                files,
                working_dir: working_dir.to_path_buf(),
            });
        }
        (admitted, refused)
    }

    pub(crate) fn volume_for_file(&self, file_index: u32) -> Option<u32> {
        self.files.get(&file_index).copied()
    }

    /// The plan facts a checkpoint is validated against at restart. Reached
    /// only from the restart reader, which phase 4 left unwired (see
    /// `restart`'s module docs).
    #[allow(dead_code)]
    pub(crate) fn expected_volume_files(&self) -> HashMap<u32, u32> {
        self.volumes
            .iter()
            .map(|(volume, file)| (*volume, *file))
            .collect()
    }

    /// Working-directory-relative envelope file for the set.
    pub(crate) fn envelope_relative_path(&self) -> String {
        format!(
            "{}.direct-envelope",
            crate::jobs::working_dir::sanitize_dirname(&self.set_name)
        )
    }

    pub(crate) fn envelope_path(&self) -> PathBuf {
        self.working_dir.join(self.envelope_relative_path())
    }

    /// Deterministic envelope offset for one run of a volume's non-member bytes.
    ///
    /// `None` when the run does not fit its half-slot, which for phase 4's shape
    /// means the set carries more than headers and must demote.
    pub(crate) fn envelope_offset(
        &self,
        volume_index: u32,
        physical_offset: u64,
        len: u64,
        tail_base: u64,
    ) -> Option<u64> {
        let slot = (volume_index as u64).checked_mul(ENVELOPE_SLOT_BYTES)?;
        let (half_base, inside) = if tail_base > 0 && physical_offset >= tail_base {
            (ENVELOPE_SLOT_HALF, physical_offset - tail_base)
        } else {
            (0, physical_offset)
        };
        if inside.checked_add(len)? > ENVELOPE_SLOT_HALF {
            return None;
        }
        slot.checked_add(half_base)?.checked_add(inside)
    }

    /// Working-directory-relative `.direct.partial` for one member.
    ///
    /// Gated by the same validator RAR extraction gates member paths with, so a
    /// hostile name cannot escape the working directory here either.
    pub(crate) fn member_partial_path(&self, member_name: &str) -> Result<String, ()> {
        let safe = crate::pipeline::extraction::validate_sanitized_rar_member_path(member_name)
            .map_err(|_| ())?;
        let safe = safe.to_string_lossy().replace('\\', "/");
        if safe.is_empty() {
            return Err(());
        }
        Ok(format!("{safe}.direct.partial"))
    }

    /// Final destination for a member, resolved exactly as the incremental
    /// extractor resolves it (D3: reuse, don't invent).
    pub(crate) fn member_output_path(&self, member_name: &str) -> Result<PathBuf, ()> {
        let safe = crate::pipeline::extraction::validate_sanitized_rar_member_path(member_name)
            .map_err(|_| ())?;
        let safe = safe.to_string_lossy().replace('\\', "/");
        Ok(crate::pipeline::Pipeline::member_output_paths(&self.working_dir, &safe).0)
    }

    /// Digest of the layout plan the coverage is produced against (D6).
    ///
    /// Deliberately **stable as volumes arrive**: it binds the set identity, the
    /// volume-to-file mapping and the member destinations, and excludes the
    /// per-part physical extents. Those grow monotonically while the set
    /// downloads, so digesting them would invalidate the checkpoint at every
    /// barrier and make a restart redownload from zero. Excluding them is safe
    /// because a member's logical offsets are prefix sums that a later volume
    /// only ever extends — the library guarantees no offset moves while the
    /// member still routes — and any change to the facts a claimed extent
    /// depends on shows up as a different member name or unpacked size.
    pub(crate) fn digest(&self, members: &[(u32, String, u64)]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"weaver.direct_store.plan.v1\0");
        hasher.update(self.set_name.as_bytes());
        hasher.update(&[0]);
        hasher.update(&(self.volumes.len() as u64).to_le_bytes());
        for (volume, file) in &self.volumes {
            hasher.update(&volume.to_le_bytes());
            hasher.update(&file.to_le_bytes());
        }
        let mut members = members.to_vec();
        members.sort_by_key(|(index, _, _)| *index);
        hasher.update(&(members.len() as u64).to_le_bytes());
        for (index, name, unpacked_size) in &members {
            hasher.update(&index.to_le_bytes());
            hasher.update(name.as_bytes());
            hasher.update(&[0]);
            hasher.update(&unpacked_size.to_le_bytes());
        }
        *hasher.finalize().as_bytes()
    }
}
