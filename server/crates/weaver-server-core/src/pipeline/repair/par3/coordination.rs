//! Cross-format repair ordering and source-evidence handoff.
//!
//! Engine-native verdicts remain separate; shared bytes are freshly verified
//! after the other engine repairs them. PAR2-only jobs allocate no handoff state.

use super::*;
use par3_rs::session::RepairStatus;

impl Pipeline {
    /// A native PAR2 write retires PAR3 evidence for its write set before the
    /// filesystem changes. Clean siblings keep their retained PAR3 evidence.
    pub(in crate::pipeline) fn fence_par3_before_par2_repair(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        verification: Option<&par2_rs::VerificationResult>,
    ) -> Result<(), String> {
        if !self
            .par3_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.contains_job(job_id))
        {
            return Ok(());
        }
        let (files, _reservation) = self
            .par3_bound_par2_files(job_id, set_id, |id| {
                verification.is_none_or(|verification| {
                    verification.files.iter().any(|file| {
                        file.file_id == id
                            && !matches!(file.status, par2_rs::verify::FileStatus::Complete)
                    })
                })
            })
            .map_err(|error| error.to_string())?;
        if verification.is_some()
            && files.iter().any(|id| {
                self.par3_runtime.as_ref().is_some_and(|runtime| {
                    runtime.source_verified(job_id, SourceId(u64::from(id.file_index)))
                })
            })
        {
            return Err("conflicting PAR2 and PAR3 source verdicts; refusing to overwrite verified PAR3 data".into());
        }
        for id in files {
            self.invalidate_par3_source_write(id);
        }
        Ok(())
    }

    /// A failed PAR2 ladder may hand the job to admitted PAR3 work. This does
    /// not clear the PAR2 failure or turn a PAR3 proof into a PAR2 checksum.
    pub(in crate::pipeline) fn par3_has_work_after_par2_failure(&self, job_id: JobId) -> bool {
        self.par3_runtime.as_ref().is_some_and(|runtime| {
            runtime.contains_job(job_id)
                && self.par2_runtime(job_id).is_none_or(|par2| {
                    par2.sets
                        .values()
                        .all(|set| set.failure.is_none() || set.alternate_repair.is_some())
                })
                && (runtime.has_work(job_id)
                    || runtime.authenticated_set_count(job_id) == 0
                    || runtime
                        .assessments(job_id)
                        .any(|(_, view)| view.status != RepairStatus::Complete))
        })
    }

    /// An unavailable PAR2 index has no authenticated source descriptions to
    /// settle. It may be omitted only when current PAR3 evidence covers every
    /// payload; an unrelated verified set cannot excuse unverified bytes.
    pub(in crate::pipeline) fn par3_verifies_all_payloads(&self, job_id: JobId) -> bool {
        let Some(runtime) = self.par3_runtime.as_ref() else {
            return false;
        };
        runtime.verified(job_id)
            && self.jobs.get(&job_id).is_some_and(|state| {
                state.assembly.files().all(|file| {
                    matches!(file.role(), FileRole::Par2 { .. } | FileRole::Par3 { .. })
                        || runtime
                            .source_verified(job_id, SourceId(u64::from(file.file_id().file_index)))
                })
            })
    }

    pub(super) fn rearm_par2_after_par3_installations(
        &mut self,
        job_id: JobId,
        files: &[(NzbFileId, String)],
    ) {
        let Some(runtime) = self.par2_runtime.get_mut(&job_id) else {
            return;
        };
        for set in runtime.sets.values_mut() {
            if set.set.as_ref().is_some_and(|native| {
                native
                    .files
                    .values()
                    .any(|description| files.iter().any(|(_, name)| name == &description.filename))
            }) {
                set.settled = false;
                set.failure = None;
                set.alternate_repair = None;
                set.pending_repair = None;
                set.post_verdict_reconcile_attempts = 0;
            }
        }
        self.par2_verified.remove(&job_id);
    }

    pub(super) fn par3_damage_overlaps_settled_par2(
        &self,
        job_id: JobId,
        view: &assessment::AssessmentView,
    ) -> bool {
        if self.par2_bypassed.contains(&job_id) {
            return false;
        }
        view.files.iter().filter(|file| !file.complete).any(|file| {
            let Some(source) = file.source else {
                return false;
            };
            let Ok(file_index) = u32::try_from(source.0) else {
                return false;
            };
            self.par2_runtime(job_id).is_some_and(|runtime| {
                runtime.sets.iter().any(|(set_id, set)| {
                    set.settled
                        && set.failure.is_none()
                        && self
                            .resolve_par2_file_binding_in_set(
                                NzbFileId { job_id, file_index },
                                *set_id,
                            )
                            .is_some()
                })
            })
        })
    }

    /// PAR2's installed bytes are candidates for fresh PAR3 verification. A
    /// PAR2 MD5 verdict cannot become a PAR3 fingerprint or fill an old hole.
    pub(in crate::pipeline) fn refresh_par3_after_par2_repair(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        rewritten: &std::collections::HashSet<par2_rs::FileId>,
    ) -> EngineResult<()> {
        if !self
            .par3_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.contains_job(job_id))
        {
            return Ok(());
        }
        let (files, _reservation) =
            self.par3_bound_par2_files(job_id, set_id, |id| rewritten.contains(&id))?;
        for id in files {
            if self.par3_virtual_volume(id).is_some() {
                self.enqueue_par3_file(job_id, id)?;
            } else {
                self.enqueue_par3_installed_file(job_id, id)?;
            }
        }
        self.par3_runtime
            .as_mut()
            .expect("admitted runtime")
            .dispatch()
    }

    /// Cross-format handoff lists share the bounded PAR3 host pool. They are
    /// allocated only for a job already admitted to the PAR3 coordinator.
    fn par3_bound_par2_files(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        include: impl Fn(par2_rs::FileId) -> bool,
    ) -> EngineResult<(Vec<NzbFileId>, assessment::ViewReservation)> {
        let state = self
            .jobs
            .get(&job_id)
            .ok_or(EngineError::InvalidState("missing repair job"))?;
        let count = state.assembly.files().count();
        let cost = count
            .checked_mul(std::mem::size_of::<NzbFileId>())
            .ok_or(EngineError::ResourceLimit("PAR3 handoff files"))?;
        let reservation = assessment::ViewReservation::acquire(cost)?;
        let mut files = Vec::with_capacity(count);
        for file in state.assembly.files() {
            let id = file.file_id();
            if let Some(binding) = self.resolve_par2_file_binding_in_set(id, set_id)
                && include(binding.par2_file_id)
            {
                files.push(id);
            }
        }
        Ok((files, reservation))
    }
}
