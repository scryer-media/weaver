// Continuation of the `impl Pipeline` block from `direct_store/wiring.rs`.
//
// Volumes a name-admitted set has but the post never carried. The recovery
// set describes every volume of the archive whether or not it was posted, so
// once its descriptions are parsed a volume missing from the post has a name
// and a length. It gets a reserved file of its own, the same reserved output a
// PAR3 repair creates for a file no NZB article carries, and from then on it is
// one of the set's volumes like any other: one whose every byte is missing,
// which the set's in-place repair rebuilds from recovery blocks.

use super::*;
use crate::jobs::assembly::FileAssembly;
use crate::jobs::record::{ActiveFileIdentity, FileIdentitySource};
use crate::jobs::repair_outputs::RepairOutput;
use weaver_model::files::{FileRole, archive_base_name, sanitize_download_filename};

// One recovery-set description of a volume the set does not hold.
struct DescribedVolume {
    volume_index: u32,
    filename: String,
    length: u64,
}

// Where a description places a volume: its set's base name and its number.
fn described_place(filename: &str, format: super::super::plan::SetFormat) -> Option<(String, u32)> {
    let role = FileRole::from_filename(filename);
    let number = match (format, &role) {
        (super::super::plan::SetFormat::Rar, FileRole::RarVolume { volume_number }) => {
            *volume_number
        }
        (super::super::plan::SetFormat::SevenZip, FileRole::SevenZipSplit { number }) => *number,
        _ => return None,
    };
    Some((archive_base_name(filename, &role)?, number))
}

impl Pipeline {
    // Gives each live name-admitted set of the job the volumes its recovery set
    // describes and the post does not carry.
    //
    // The set's posted volumes say how the recovery set names this archive:
    // each one binds to a description, every binding has to agree on one base
    // name, and each description has to number its volume as the posted name
    // does. A description under that base name that no job file answers to is
    // a volume of this set the post left out, at the number it gives. Posted
    // names that number the volumes otherwise than the recovery set are not
    // names to place a missing volume by, so that and anything else less
    // certain leaves the set as it is, and a hole it cannot fill demotes it
    // when the job settles.
    pub(crate) fn bind_unposted_direct_volumes(&mut self, job_id: JobId) {
        let candidates: Vec<(usize, Vec<DescribedVolume>)> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| {
                !set.is_demoted() && !set.is_finalized() && set.plan().identity.is_none()
            })
            .filter_map(|(set_index, set)| {
                let described = self.described_unposted_volumes(job_id, set);
                (!described.is_empty()).then_some((set_index, described))
            })
            .collect();
        for (set_index, described) in candidates {
            for volume in described {
                let Some(file_index) = self.reserve_unposted_volume_file(job_id, &volume) else {
                    continue;
                };
                let bound = self
                    .direct_store
                    .set_mut(job_id, set_index)
                    .is_some_and(|set| {
                        if !(set.note_unposted_volume(volume.volume_index)
                            && set.bind_unposted_volume(volume.volume_index, file_index))
                        {
                            return false;
                        }
                        // No article will state this volume's length, and a
                        // container whose first part was left out reads its
                        // part size from that statement. The description's
                        // length is the one the rebuilt volume will have; one
                        // the geometry disagrees with demotes the set.
                        let _ = set.note_declared_volume_size(volume.volume_index, volume.length);
                        true
                    });
                if bound {
                    info!(
                        job_id = job_id.0,
                        set_index,
                        volume_index = volume.volume_index,
                        file_index,
                        filename = %volume.filename,
                        "a volume the post never carried joined its direct set from the \
                         recovery set's description"
                    );
                }
            }
            // Every volume between the posted ones and the highest one
            // described belongs to the set, described or not, so the set
            // cannot read as whole while any of them is missing.
            if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
                let highest = set
                    .plan()
                    .volumes
                    .keys()
                    .chain(set.plan().unposted.iter())
                    .max()
                    .copied()
                    .unwrap_or_default();
                for volume_index in 0..highest {
                    set.note_unposted_volume(volume_index);
                }
            }
        }
    }

    // The descriptions of volumes `set` does not hold, by the set's own
    // volume index.
    fn described_unposted_volumes(&self, job_id: JobId, set: &DirectSet) -> Vec<DescribedVolume> {
        let plan = set.plan();
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        // The naming every posted volume's description agrees on.
        let mut anchor: Option<(par2_rs::RecoverySetId, String)> = None;
        for (volume_index, file_index) in &plan.volumes {
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            let Some(binding) = self.resolve_par2_file_binding(file_id) else {
                continue;
            };
            let Some(description) = self
                .par2_set_for(job_id, binding.recovery_set_id)
                .and_then(|par2_set| par2_set.file_description(&binding.par2_file_id))
            else {
                continue;
            };
            let name = sanitize_download_filename(&description.filename);
            let Some((base, number)) = described_place(&name, plan.format) else {
                return Vec::new();
            };
            if number != *volume_index {
                return Vec::new();
            }
            let place = (binding.recovery_set_id, base);
            match &anchor {
                None => anchor = Some(place),
                Some(existing) if *existing == place => {}
                Some(_) => return Vec::new(),
            }
        }
        let Some((recovery_set_id, base)) = anchor else {
            return Vec::new();
        };
        let Some(par2_set) = self.par2_set_for(job_id, recovery_set_id) else {
            return Vec::new();
        };
        // Descriptions some job file already answers to are posted, under
        // whatever name, and never the set's to create. A file whose name
        // says nothing and that has not bound yet may answer to any of them,
        // so while one is left nothing is placed.
        let mut claimed: HashSet<par2_rs::FileId> = HashSet::new();
        for file in state
            .assembly
            .files()
            .filter(|file| !file.is_repair_output())
        {
            match self.resolve_par2_file_binding_in_set(file.file_id(), recovery_set_id) {
                Some(binding) => {
                    claimed.insert(binding.par2_file_id);
                }
                None if matches!(
                    self.classified_role_for_file(job_id, file),
                    FileRole::Unknown
                ) =>
                {
                    return Vec::new();
                }
                None => {}
            }
        }
        let mut described: BTreeMap<u32, Option<DescribedVolume>> = BTreeMap::new();
        for (par2_file_id, description) in &par2_set.files {
            let name = sanitize_download_filename(&description.filename);
            let Some((described_base, volume_index)) = described_place(&name, plan.format) else {
                continue;
            };
            if described_base != base || claimed.contains(par2_file_id) {
                continue;
            }
            if plan.volumes.contains_key(&volume_index) {
                continue;
            }
            // Two descriptions for one volume say nothing about which is it.
            described
                .entry(volume_index)
                .and_modify(|claim| *claim = None)
                .or_insert(Some(DescribedVolume {
                    volume_index,
                    filename: name,
                    length: description.length,
                }));
        }
        described.into_values().flatten().collect()
    }

    // The job file a described volume is rebuilt into: the reserved output
    // already standing for that name, or a new one.
    fn reserve_unposted_volume_file(
        &mut self,
        job_id: JobId,
        volume: &DescribedVolume,
    ) -> Option<u32> {
        let state = self.jobs.get(&job_id)?;
        if let Some(existing) = state
            .assembly
            .files()
            .find(|file| self.current_filename_for_file(job_id, file) == volume.filename)
        {
            // A posted file under that name would have bound; one that did
            // not is not this volume's to take over.
            return existing
                .is_repair_output()
                .then(|| existing.file_id().file_index);
        }
        let next = state
            .assembly
            .files()
            .map(|file| u64::from(file.file_id().file_index) + 1)
            .chain(
                state
                    .file_identities
                    .keys()
                    .map(|index| u64::from(*index) + 1),
            )
            .chain(std::iter::once(state.spec.files.len() as u64))
            .max()
            .unwrap_or(0);
        let record = RepairOutput {
            file_index: u32::try_from(next).ok()?,
            filename: volume.filename.clone(),
            expected_length: volume.length,
        };
        if record.validate().is_err() {
            return None;
        }
        // A name absent from the NZB is new ownership, never permission to
        // take over a file already in the working directory.
        match record
            .destination(&state.working_dir)
            .map(|path| std::fs::symlink_metadata(path))
        {
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return None,
        }
        if let Err(error) = self
            .db
            .reserve_repair_outputs(job_id, std::slice::from_ref(&record))
        {
            warn!(
                job_id = job_id.0,
                filename = %record.filename,
                %error,
                "could not reserve a file for a volume the post never carried"
            );
            return None;
        }
        let file_id = NzbFileId {
            job_id,
            file_index: record.file_index,
        };
        self.jobs
            .get_mut(&job_id)?
            .assembly
            .add_file(FileAssembly::repair_output(
                file_id,
                record.filename.clone(),
            ));
        let identity = ActiveFileIdentity {
            file_index: record.file_index,
            source_filename: record.filename.clone(),
            current_filename: record.filename.clone(),
            canonical_filename: Some(record.filename.clone()),
            classification: Pipeline::canonical_archive_identity_from_filename(&record.filename),
            classification_source: FileIdentitySource::Par2,
        };
        if let Err(error) = self.set_file_identity(job_id, identity) {
            warn!(
                job_id = job_id.0,
                filename = %record.filename,
                %error,
                "could not record the identity of a volume the post never carried"
            );
        }
        Some(record.file_index)
    }
}
