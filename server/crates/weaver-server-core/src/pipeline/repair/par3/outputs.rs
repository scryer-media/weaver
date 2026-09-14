//! Reserve logical output identities before a native repair writes them.

use super::*;
use crate::jobs::{assembly::FileAssembly, repair_outputs::RepairOutput};

impl Pipeline {
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
