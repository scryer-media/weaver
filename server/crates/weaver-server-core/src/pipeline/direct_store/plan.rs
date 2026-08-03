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

/// Appends `suffix` to the final component of a working-directory-relative
/// path, shortening the component's stem so the result stays inside
/// [`weaver_model::files::DOWNLOAD_FILENAME_MAX_BYTES`].
fn with_suffix(relative: &str, suffix: &str) -> String {
    match relative.rsplit_once('/') {
        Some((parent, name)) => format!(
            "{parent}/{}",
            weaver_model::files::path_component_with_suffix(name, suffix)
        ),
        None => weaver_model::files::path_component_with_suffix(relative, suffix),
    }
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

    /// Working-directory-relative envelope file for **one source volume**
    /// (envelope v2, plan 135 D3's "sparse envelope files", plural).
    ///
    /// Phase 4 packed every volume's envelope into fixed 64 KiB half-slots of a
    /// single per-set file. That ceiling is what demoted every `-rr` and `-qo`
    /// set, and it was a phase-4 narrowing, not a design. Envelope v2 gives each
    /// volume its own file holding every non-member byte **at its true physical
    /// offset**, with holes where member data was routed away:
    ///
    /// - unbounded by construction — recovery records, quick-open blocks and
    ///   ineligible members' packed ranges fit because the file *is* the volume;
    /// - restart-stable by construction — an offset is physical, never
    ///   append-order, so the same byte lands in the same place every time;
    /// - and it is the natural backing store for the hybrid virtual-volume
    ///   provider, which overlays member partials onto exactly this image.
    ///
    /// Zero-padded so a lexical listing of a 2 000-volume set sorts in volume
    /// order.
    pub(crate) fn envelope_relative_path(&self, volume_index: u32) -> String {
        // Clamped as one component rather than formatted (nit): the suffix is 18
        // bytes and a set name is an NZB-supplied string, so a long one produced
        // a filename the filesystem refuses and the set demoted on its first
        // routed byte — with `DestinationWriteFailed`, which says nothing about
        // the name being the cause. `path_component_with_suffix` shortens the
        // *stem* and keeps the suffix whole, so the volume number and the
        // extension survive.
        weaver_model::files::path_component_with_suffix(
            &crate::jobs::working_dir::sanitize_dirname(&self.set_name),
            &format!(".vol{volume_index:05}.envelope"),
        )
    }

    pub(crate) fn envelope_path(&self, volume_index: u32) -> PathBuf {
        self.working_dir
            .join(self.envelope_relative_path(volume_index))
    }

    /// Every envelope file the set can own, in volume order.
    pub(crate) fn envelope_paths(&self) -> Vec<PathBuf> {
        self.volumes
            .keys()
            .map(|volume_index| self.envelope_path(*volume_index))
            .collect()
    }

    /// A raw header name resolved the way the incremental extractor resolves it
    /// (D3: reuse, don't invent) — `weaver_unrar::sanitize_path` first, then the
    /// validator that refuses anything escaping the directory it is joined onto.
    ///
    /// Both steps matter, and phase 4 only had the second: `sanitize_path`
    /// normalizes separators and strips a drive or root prefix, so a name the
    /// extractor would have written to `Silver.Horizon/S01E01.mkv` and one that
    /// direct routing would otherwise have refused now resolve identically. With
    /// several members per set that difference is reachable, where a
    /// single-member set could only ever have demoted on it.
    fn resolve_member_path(member_name: &str) -> Result<String, ()> {
        let sanitized = weaver_unrar::sanitize_path(member_name);
        let safe = crate::pipeline::extraction::validate_sanitized_rar_member_path(&sanitized)
            .map_err(|_| ())?;
        let safe = safe.to_string_lossy().replace('\\', "/");
        if safe.is_empty() {
            return Err(());
        }
        Ok(safe)
    }

    /// The key the extractor decides two members collide on: the sanitized path,
    /// case-folded, exactly as `ensure_unique_sanitized_rar_member_paths` folds
    /// it. Two members sharing one is an archive today's extractor refuses
    /// outright, so a direct set carrying one demotes rather than inventing
    /// overwrite semantics that nothing downstream shares.
    pub(crate) fn member_collision_key(member_name: &str) -> Result<String, ()> {
        Self::resolve_member_path(member_name).map(|safe| safe.to_ascii_lowercase())
    }

    /// Working-directory-relative `.direct.partial` for one member.
    ///
    /// Only the **last** component is clamped, and only the stem inside it: a
    /// member stored inside a directory keeps its directory, and the
    /// `.direct.partial` suffix keeps its whole length so nothing downstream has
    /// to guess whether a truncated name still names a partial (nit).
    pub(crate) fn member_partial_path(&self, member_name: &str) -> Result<String, ()> {
        Self::resolve_member_path(member_name).map(|safe| with_suffix(&safe, ".direct.partial"))
    }

    /// Final destination for a member.
    pub(crate) fn member_output_path(&self, member_name: &str) -> Result<PathBuf, ()> {
        let safe = Self::resolve_member_path(member_name)?;
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
    ///
    /// Members are bound **by name**, sorted, with no index: a multi-member set
    /// discovers its members in whatever order its volumes arrive, and weaver's
    /// per-member destination key is an in-run counter, so digesting the index
    /// would make the same set produce different digests on different runs. The
    /// name is the layout's own key and the only thing a destination path is
    /// derived from.
    pub(crate) fn digest(&self, members: &[(String, u64)]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"weaver.direct_store.plan.v2\0");
        hasher.update(self.set_name.as_bytes());
        hasher.update(&[0]);
        hasher.update(&(self.volumes.len() as u64).to_le_bytes());
        for (volume, file) in &self.volumes {
            hasher.update(&volume.to_le_bytes());
            hasher.update(&file.to_le_bytes());
        }
        let mut members = members.to_vec();
        members.sort();
        hasher.update(&(members.len() as u64).to_le_bytes());
        for (name, unpacked_size) in &members {
            hasher.update(name.as_bytes());
            hasher.update(&[0]);
            hasher.update(&unpacked_size.to_le_bytes());
        }
        *hasher.finalize().as_bytes()
    }
}
