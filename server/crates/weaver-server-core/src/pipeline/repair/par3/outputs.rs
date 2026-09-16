//! Reserve logical output identities before a native repair writes them.

use super::*;
use crate::jobs::{assembly::FileAssembly, repair_outputs::RepairOutput};

impl Pipeline {
    /// The assessed layout of one set, when the runtime is willing to show it.
    fn par3_view(
        &self,
        job: JobId,
        set: par3_rs::InputSetId,
    ) -> Option<&assessment::AssessmentView> {
        self.par3_runtime
            .as_ref()?
            .assessments(job)
            .find(|(id, _)| *id == set)
            .map(|(_, view)| view)
    }

    /// The first name in an assessed set that weaver refuses to create, with
    /// the rule that refused it.
    ///
    /// The check runs against the whole resolved relative path, so a directory
    /// component is refused on the same rules as the file's own last
    /// component. A refusal is never repaired by rewriting the name: a set
    /// that asks for a name weaver will not create is a defect in the set.
    pub(super) fn par3_unsafe_output_path(
        &self,
        job: JobId,
        set: par3_rs::InputSetId,
    ) -> Option<(String, paths::UnsafePath)> {
        let view = self.par3_view(job, set)?;
        view.files.iter().find_map(|file| {
            paths::check_install_path(&file.path)
                .err()
                .map(|reason| (file.path.clone(), reason))
        })
    }

    /// Bytes the working directory is short of what installing this set needs,
    /// or `None` when it has room.
    ///
    /// Only the files the repair will actually write count. A file whose
    /// protected data is already verified at its expected length is never
    /// rebuilt, so its bytes are neither needed nor freed, and counting them
    /// would refuse a large set on a disk that comfortably holds the handful
    /// of damaged files in it.
    ///
    /// Of those that will be written, the allowance is every output length
    /// plus one more copy of the largest, because the engine writes each
    /// reconstruction to a temporary file beside its destination and renames
    /// it into place. A damaged output already on disk is still counted in
    /// full: its bytes are not free until that rename lands.
    pub(super) async fn par3_output_space_shortfall(
        &mut self,
        job: JobId,
        set: par3_rs::InputSetId,
    ) -> Option<outcome::Par3Outcome> {
        let view = self.par3_view(job, set)?;
        // A layout that does not describe every assessed file is not a plan
        // this can measure. Output reservation refuses it on its own terms.
        if view.files.len() != view.output_lengths.len() {
            return None;
        }
        let rebuilt = || {
            view.files
                .iter()
                .zip(&view.output_lengths)
                .filter(|(file, _)| !file.complete)
                .map(|(_, length)| *length)
        };
        let total = rebuilt().try_fold(0u64, |bytes, length| bytes.checked_add(length));
        let staging = rebuilt().max().unwrap_or(0);
        let need = total.and_then(|total| total.checked_add(staging))?;
        if need == 0 {
            return None;
        }
        let path = self.jobs.get(&job)?.working_dir.clone();
        let reserve = self.direct_store.settings().holds_disk_reserve_bytes;
        let space =
            tokio::task::spawn_blocking(move || crate::operations::disk::probe_disk_space(&path))
                .await;
        // A probe weaver cannot take is not evidence of a shortfall. Planning
        // proceeds and the write itself reports whatever the filesystem says.
        let Ok(Ok(space)) = space else {
            return None;
        };
        let usable = space.available_bytes.saturating_sub(reserve);
        (need > usable).then(|| outcome::Par3Outcome::NoOutputSpace {
            need,
            available: usable,
            shortfall: need - usable,
        })
    }

    pub(super) fn prepare_par3_outputs(
        &mut self,
        job: JobId,
        set: par3_rs::InputSetId,
    ) -> Result<(), String> {
        let state = self.jobs.get(&job).ok_or("missing output job")?;
        let view = self
            .par3_runtime
            .as_ref()
            .ok_or("missing PAR3 runtime")?
            .assessments(job)
            .find(|(id, _)| *id == set)
            .map(|(_, view)| view)
            .ok_or("missing output assessment")?;
        if view.files.len() != view.output_lengths.len() {
            return Err("incomplete output layout".into());
        }
        let _reservation = assessment::ViewReservation::acquire(
            view.files.iter().map(|file| 256 + file.path.len()).sum(),
        )
        .map_err(|error| error.to_string())?;
        let known = self
            .db
            .load_repair_outputs(job)
            .map_err(|error| error.to_string())?;
        let mut next = state
            .assembly
            .files()
            .map(|file| u64::from(file.file_id().file_index) + 1)
            .chain(
                state
                    .file_identities
                    .keys()
                    .map(|index| u64::from(*index) + 1),
            )
            .max()
            .unwrap_or(0);
        let mut outputs = Vec::new();
        for (file, &length) in view.files.iter().zip(&view.output_lengths) {
            if let Some(bound) = state
                .assembly
                .files()
                .find(|bound| self.current_filename_for_file(job, bound) == file.path)
            {
                if bound.is_repair_output()
                    && !known.iter().any(|record| {
                        record.file_index == bound.file_id().file_index
                            && record.filename == file.path
                            && record.expected_length == length
                    })
                {
                    return Err("changed reconstructed output description".into());
                }
                continue;
            }
            if outputs
                .iter()
                .any(|record: &RepairOutput| record.filename == file.path)
            {
                continue;
            }
            let record = RepairOutput {
                file_index: u32::try_from(next)
                    .map_err(|_| "reconstructed output identity exhausted")?,
                filename: file.path.clone(),
                expected_length: length,
            };
            record.validate().map_err(|error| error.to_string())?;
            // A path absent from the NZB is new ownership, not permission to
            // overwrite a pre-existing file or link in the working directory.
            match std::fs::symlink_metadata(record.destination(&state.working_dir)?) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.to_string()),
                Ok(_) => {
                    return Err(format!(
                        "unclaimed output already exists: {}",
                        record.filename
                    ));
                }
            }
            next += 1;
            outputs.push(record);
        }
        if outputs.is_empty() {
            return Ok(());
        }
        self.db
            .reserve_repair_outputs(job, &outputs)
            .map_err(|error| error.to_string())?;
        for output in outputs {
            let id = NzbFileId {
                job_id: job,
                file_index: output.file_index,
            };
            self.jobs
                .get_mut(&job)
                .expect("live job")
                .assembly
                .add_file(FileAssembly::repair_output(id, output.filename.clone()));
            self.set_file_identity(job, crate::jobs::repair_outputs::identity(&output))?;
        }
        Ok(())
    }
}
