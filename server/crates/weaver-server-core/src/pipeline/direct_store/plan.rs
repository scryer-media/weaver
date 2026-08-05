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
}

/// Extension of a damaged volume's repair scratch (plan 135, D8). Matched as a
/// suffix by the restart sweep, exactly as `.envelope` is.
pub(crate) const REPAIR_SUFFIX: &str = ".repair";

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

        // Phase 4 refused the whole job here when its spec carried a PAR2 file:
        // every PAR2 path reads the *volume files* the descriptions name, and a
        // direct set has none. Wave 2's `par2_access` adapter answers those
        // reads out of the envelope plus the routed member bytes, so the
        // refusal is gone — a par2-bearing set routes, and its finalization
        // waits for the job's verification to conclude (see the module docs).
        let mut admitted = Vec::new();
        let mut refused = Vec::new();
        for (set_name, mut entries) in candidates {
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

    /// The plan facts a checkpoint is validated against at restart.
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
    /// The set's stable per-job discriminator: its lowest NZB file index.
    ///
    /// Unique across a job by construction — a file belongs to exactly one set —
    /// and derived from the spec, so it is the same on every restart. Every
    /// internal path this plan derives carries it, because every one of those
    /// namespaces has the same collision problem the holds-scratch comment below
    /// describes: `sanitize_dirname` is many-to-one, `path_component_with_suffix`
    /// clamps, and member names are shared freely between archives, so *any*
    /// path derived from names alone can be reached by two sets of one job.
    /// Envelopes, repair scratch and member partials all found that out the slow
    /// way (post-completion review, 2026-08-04): two sets sharing a partial
    /// interleave their writes into one file while each router's in-memory
    /// gates pass over its own buffers — silent mixed bytes on a PAR2-less set,
    /// which is exactly the set direct-store exists for.
    fn set_discriminator(&self) -> u32 {
        self.volumes.values().min().copied().unwrap_or_default()
    }

    pub(crate) fn envelope_relative_path(&self, volume_index: u32) -> String {
        // Clamped as one component rather than formatted (nit): the suffix is 18
        // bytes and a set name is an NZB-supplied string, so a long one produced
        // a filename the filesystem refuses and the set demoted on its first
        // routed byte — with `DestinationWriteFailed`, which says nothing about
        // the name being the cause. `path_component_with_suffix` shortens the
        // *stem* and keeps the suffix whole, so the volume number and the
        // extension survive. The discriminator rides the suffix for the same
        // reason it does on the holds scratch: the clamp can never shorten it
        // away, and two sets whose names sanitize identically stay two files.
        weaver_model::files::path_component_with_suffix(
            &crate::jobs::working_dir::sanitize_dirname(&self.set_name),
            &format!(".f{}.vol{volume_index:05}.envelope", self.set_discriminator()),
        )
    }

    pub(crate) fn envelope_path(&self, volume_index: u32) -> PathBuf {
        self.working_dir
            .join(self.envelope_relative_path(volume_index))
    }

    /// Working-directory-relative holds scratch file for this set (D2).
    ///
    /// One file per set, at the top level, named from the set so the restart
    /// sweep can recognise it by prefix and so two sets of one job never share a
    /// region index. Append-only and write-once while the set is open, deleted
    /// at finalization or demotion.
    ///
    /// # The disambiguator is not decoration
    ///
    /// `sanitize_dirname` is many-to-one and `path_component_with_suffix` clamps
    /// long names, so two sets of one job can reach the same stem — `A/B` and
    /// `A_B`, or two long names differing past the clamp. Sharing one scratch
    /// file between two sets is silent corruption of the worst kind: the file is
    /// append-only with an *in-memory* region index per set, so each set hands
    /// out offsets the other is also writing at, and a paged hold reads back as
    /// the other set's bytes. The set's lowest NZB file index disambiguates it —
    /// unique across a job by construction (a file belongs to one set) and
    /// derived from the spec, so it is the same on every restart. It goes in the
    /// *suffix* argument so the clamp shortens the stem around it and can never
    /// shorten it away.
    pub(crate) fn holds_scratch_relative_path(&self) -> String {
        weaver_model::files::path_component_with_suffix(
            &format!(
                "{}{}",
                crate::pipeline::direct_store::restart::HOLDS_SCRATCH_PREFIX,
                crate::jobs::working_dir::sanitize_dirname(&self.set_name)
            ),
            &format!(".f{}", self.set_discriminator()),
        )
    }

    pub(crate) fn holds_scratch_path(&self) -> PathBuf {
        self.working_dir.join(self.holds_scratch_relative_path())
    }

    /// Working-directory-relative **repair scratch** for one source volume
    /// (plan 135, D8's *repair while still direct*).
    ///
    /// A PAR2 repair needs a file to write recovered slices into, and a virtual
    /// volume is not one. Phase 6 materializes *only the damaged volumes* into
    /// these files, repairs them there, routes the repaired spans back through
    /// the router, and deletes them again — clean volumes stay virtual and no
    /// direct output is deleted.
    ///
    /// Deliberately **not** the volume's own `.partNN.rar` name: that path is
    /// what a demotion materializes into, and a leftover half-repaired file
    /// sitting there would be read as a downloaded volume by every conventional
    /// path. This suffix is swept at restart alongside envelopes and partials.
    pub(crate) fn repair_relative_path(&self, volume_index: u32) -> String {
        weaver_model::files::path_component_with_suffix(
            &crate::jobs::working_dir::sanitize_dirname(&self.set_name),
            &format!(
                ".f{}.vol{volume_index:05}{REPAIR_SUFFIX}",
                self.set_discriminator()
            ),
        )
    }

    pub(crate) fn repair_path(&self, volume_index: u32) -> PathBuf {
        self.working_dir
            .join(self.repair_relative_path(volume_index))
    }

    /// Every repair scratch file the set can own, in volume order.
    pub(crate) fn repair_paths(&self) -> Vec<PathBuf> {
        self.volumes
            .keys()
            .map(|volume_index| self.repair_path(*volume_index))
            .collect()
    }

    /// Every envelope file the set can own, in volume order.
    pub(crate) fn envelope_paths(&self) -> Vec<PathBuf> {
        self.volumes
            .keys()
            .map(|volume_index| self.envelope_path(*volume_index))
            .collect()
    }

    /// A raw header name resolved the way the incremental extractor resolves it
    /// (D3: reuse, don't invent) — `unrar_rs::sanitize_path` first, then the
    /// validator that refuses anything escaping the directory it is joined onto.
    ///
    /// Both steps matter, and phase 4 only had the second: `sanitize_path`
    /// normalizes separators and strips a drive or root prefix, so a name the
    /// extractor would have written to `Silver.Horizon/S01E01.mkv` and one that
    /// direct routing would otherwise have refused now resolve identically. With
    /// several members per set that difference is reachable, where a
    /// single-member set could only ever have demoted on it.
    fn resolve_member_path(member_name: &str) -> Result<String, ()> {
        let sanitized = unrar_rs::sanitize_path(member_name);
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
    ///
    /// Carries the set discriminator, and here it is most load-bearing of all:
    /// this is the one derived path with **no set component at all**, so two
    /// sets of one job that both contain a `movie.mkv` reached the *same*
    /// partial with nothing colliding but the archives' own contents. The final
    /// destination stays undiscriminated on purpose — it is the user-visible
    /// name, and two sets claiming it resolve by rename order exactly as two
    /// conventionally-extracted archives resolve by extraction order.
    pub(crate) fn member_partial_path(&self, member_name: &str) -> Result<String, ()> {
        let suffix = format!(".f{}.direct.partial", self.set_discriminator());
        Self::resolve_member_path(member_name).map(|safe| with_suffix(&safe, &suffix))
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
        // v3: the derived-path shape gained the set discriminator. The digest
        // binds names rather than paths, so a shape change is invisible to it —
        // and a v2 row would restore claims against partials that now have
        // different names. Bumping the domain string refuses every older row
        // into the ordinary redownload; no v2 row ever shipped in a release.
        hasher.update(b"weaver.direct_store.plan.v3\0");
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
