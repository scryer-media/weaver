//! Reserve logical output identities before a native repair writes them.

use super::*;
use crate::jobs::{assembly::FileAssembly, repair_outputs::RepairOutput};
use par3_rs::session_repair::InstalledFile;

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
    /// Of those that will be written, the allowance is every output length:
    /// the engine stages each reconstruction in full beside its destination
    /// before it renames any of them into place, and a damaged file already on
    /// disk is not free space until that rename lands, so the probe has
    /// already left it out. A rename adds nothing. Only an embedded carrier's
    /// self-repair also keeps a scratch tree beside its staged archive, and
    /// only that path is allowed one more copy of its output.
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
        let staging = if view.embedded_source.is_some() {
            rebuilt().max().unwrap_or(0)
        } else {
            0
        };
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

    /// A direct set holding a volume the repair is about to rebuild under
    /// another name, when the repair will create an output no posted file
    /// answers to.
    ///
    /// Such a volume is a posted file nothing could name. Its bytes sit in the
    /// set's destinations, where neither the rebuilt archive nor the check
    /// that retires the posted copy can reach them, and the set can never be
    /// made whole from a file the repair writes beside it. The set writes
    /// itself out first.
    pub(super) fn par3_direct_set_behind_unposted_output(
        &self,
        job: JobId,
        set: par3_rs::InputSetId,
    ) -> Option<usize> {
        let view = self.par3_view(job, set)?;
        if !view.files.iter().any(|file| file.source.is_none()) {
            return None;
        }
        let state = self.jobs.get(&job)?;
        let runtime = self.par3_runtime.as_ref()?;
        self.direct_store.sets_for(job).iter().position(|direct| {
            !direct.is_demoted()
                && !direct.is_finalized()
                && state.assembly.files().any(|file| {
                    direct
                        .plan()
                        .volume_for_file(file.file_id().file_index)
                        .is_some()
                        && !file.is_complete()
                        && {
                            let name = self.current_filename_for_file(job, file);
                            !runtime
                                .assessments(job)
                                .any(|(_, view)| view.files.iter().any(|file| file.path == name))
                        }
                })
        })
    }

    /// Find the posted files a repair's new outputs were rebuilt from.
    ///
    /// A posted file that cannot be named — its name says nothing and it lost
    /// bytes, so no whole-image identity can be read from it — is rebuilt
    /// under the name the set describes, and the posted copy is left beside
    /// the output. The copy is superseded when the bytes that did arrive are
    /// the output's own bytes at the same offsets, and those of no other
    /// output. The output is a verified image on disk, so that is a direct
    /// comparison and nothing is hashed.
    ///
    /// A posted file of which nothing arrived has no bytes to compare. It is
    /// accounted for by count instead: every output the repair had to create
    /// stands for one posted file that could not be named, so once the
    /// comparison has claimed what it can, the outputs left over are owed
    /// exactly that many posted files. When no more empty files than that
    /// remain, each is one of them. When more remain, nothing says which, and
    /// none is claimed.
    ///
    /// The comparison is deliberately small. It runs once per repair that
    /// created an output no posted file answered to, only over incomplete
    /// files no set describes, and reads at most [`SUPERSEDED_WINDOWS`]
    /// windows of [`SUPERSEDED_WINDOW_BYTES`] from each.
    pub(super) async fn note_par3_superseded_sources(
        &mut self,
        job: JobId,
        installed: &[InstalledFile],
    ) {
        let Some(state) = self.jobs.get(&job) else {
            return;
        };
        let outputs: Vec<PathBuf> = state
            .assembly
            .files()
            .filter(|file| file.is_repair_output() && file.is_complete())
            .map(|file| {
                state
                    .working_dir
                    .join(self.current_filename_for_file(job, file))
            })
            .filter(|path| installed.iter().any(|output| output.path == *path))
            .collect();
        if outputs.is_empty() {
            return;
        }
        let described = |name: &str| {
            self.par3_runtime.as_ref().is_some_and(|runtime| {
                runtime
                    .assessments(job)
                    .any(|(_, view)| view.files.iter().any(|file| file.path == name))
            })
        };
        let candidates: Vec<_> = state
            .assembly
            .files()
            .filter(|file| {
                !file.is_complete()
                    && !file.is_repair_output()
                    && !file.role().is_recovery()
                    && !self.recovery_superseded_source(job, file.file_id())
            })
            .filter_map(|file| {
                let name = self.current_filename_for_file(job, file);
                if described(&name) {
                    return None;
                }
                // Where bytes arrived: the articles assembly placed, or for a
                // file a demotion wrote out, the ranges that handback kept.
                let mut windows: Vec<(u64, usize)> = (0..file.total_segments())
                    .filter(|segment| file.has_segment(*segment))
                    .filter_map(|segment| file.placement_of(segment))
                    .map(|(offset, len)| (offset, len as usize))
                    .filter(|(_, len)| *len != 0)
                    .take(SUPERSEDED_WINDOWS)
                    .collect();
                if windows.is_empty() {
                    windows = self
                        .par3_runtime
                        .as_ref()
                        .and_then(|runtime| {
                            runtime
                                .materialized_ranges(
                                    job,
                                    SourceId(u64::from(file.file_id().file_index)),
                                )
                                .ok()
                                .flatten()
                        })
                        .unwrap_or_default()
                        .iter()
                        .map(|range| {
                            let len =
                                usize::try_from(range.end - range.start).unwrap_or(usize::MAX);
                            (range.start, len)
                        })
                        .filter(|(_, len)| *len != 0)
                        .take(SUPERSEDED_WINDOWS)
                        .collect();
                }
                for (_, len) in &mut windows {
                    *len = (*len).min(SUPERSEDED_WINDOW_BYTES);
                }
                Some((file.file_id(), state.working_dir.join(name), windows))
            })
            .collect();
        if candidates.is_empty() {
            return;
        }
        let probe = tokio::task::spawn_blocking(move || {
            let mut claimed = vec![false; outputs.len()];
            let mut superseded = Vec::new();
            let mut empty = Vec::new();
            for (file, path, windows) in candidates {
                if windows.is_empty() {
                    empty.push(file);
                    continue;
                }
                let mut matches = (0..outputs.len())
                    .filter(|output| holds_windows_of(&path, &outputs[*output], &windows));
                if let (Some(output), None) = (matches.next(), matches.next()) {
                    claimed[output] = true;
                    superseded.push(file);
                }
            }
            let owed = claimed.iter().filter(|claimed| !**claimed).count();
            if empty.len() <= owed {
                superseded.append(&mut empty);
            }
            superseded
        })
        .await;
        let Ok(superseded) = probe else {
            return;
        };
        if superseded.is_empty() {
            return;
        }
        tracing::info!(
            job_id = job.0,
            superseded = superseded.len(),
            "posted files accounted for by outputs the recovery set rebuilt"
        );
        self.recovery_unposted_outputs
            .entry(job)
            .or_default()
            .superseded
            .extend(superseded);
    }
}

/// Windows of a posted copy compared against a rebuilt output.
const SUPERSEDED_WINDOWS: usize = 3;
/// Bytes compared from the front of each window.
const SUPERSEDED_WINDOW_BYTES: usize = 64 * 1024;

/// Whether every window of `copy` holds the bytes `output` has at the same
/// offset, with something other than zeros in at least one of them.
fn holds_windows_of(
    copy: &std::path::Path,
    output: &std::path::Path,
    windows: &[(u64, usize)],
) -> bool {
    use std::io::{Read, Seek, SeekFrom};

    let (Ok(mut copy), Ok(mut output)) = (std::fs::File::open(copy), std::fs::File::open(output))
    else {
        return false;
    };
    let mut ours = Vec::new();
    let mut theirs = Vec::new();
    let mut evidence = false;
    for &(offset, len) in windows {
        ours.resize(len, 0);
        theirs.resize(len, 0);
        for (file, bytes) in [(&mut copy, &mut ours), (&mut output, &mut theirs)] {
            if file.seek(SeekFrom::Start(offset)).is_err() || file.read_exact(bytes).is_err() {
                return false;
            }
        }
        if ours != theirs {
            return false;
        }
        evidence |= ours.iter().any(|byte| *byte != 0);
    }
    evidence
}
