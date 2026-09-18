use super::*;

impl Pipeline {
    pub(in crate::pipeline::download) fn status_allows_download_dispatch(
        status: &JobStatus,
    ) -> bool {
        matches!(
            status,
            JobStatus::Queued
                | JobStatus::Downloading
                | JobStatus::Checking
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::Repairing
                | JobStatus::QueuedExtract
                | JobStatus::Extracting
        )
    }

    pub(in crate::pipeline::download::worker) fn job_has_dispatchable_work(
        &mut self,
        job_id: JobId,
    ) -> bool {
        let uu_capped = !self.uu_files.is_empty() && self.uu_spool_dispatch_capped();
        let direct_admission = self.direct_store_admission(job_id, &[]);
        let sweep_held = self.demotion_sweep_held_file_indices(job_id);
        let checkpoint = self.checkpoint_admission(job_id);
        self.jobs.get(&job_id).is_some_and(|state| {
            if !Self::status_allows_download_dispatch(&state.status) {
                return false;
            }
            // A retained-byte cap is local to its set, so raw queue occupancy
            // would overstate what this job can hand out right now.
            if !direct_admission.is_empty() || sweep_held.is_some() || checkpoint.enforced {
                return state
                    .download_queue
                    .peek_first_matching(|work| {
                        direct_admission.iter().all(|set| set.allows(work))
                            && checkpoint.decision(work, &[]).allows()
                            && sweep_held.as_deref().is_none_or(|held| {
                                !held.contains(&work.segment_id.file_id.file_index)
                            })
                            && (!uu_capped
                                || self
                                    .uu_files
                                    .get(&work.segment_id.file_id)
                                    .is_none_or(|uu| {
                                        uu.next_index == work.segment_id.segment_number
                                    }))
                    })
                    .is_some();
            }
            if !uu_capped {
                return !state.download_queue.is_empty();
            }
            // A blocked UU head must not hide another encoding deeper in
            // either heap. Reuse per-file counts instead of scanning articles
            // or allocating a cursor map for this availability check.
            let queued_uu: usize = self
                .uu_files
                .keys()
                .filter(|file_id| file_id.job_id == job_id)
                .map(|file_id| state.download_queue.queued_count_for_file(*file_id) as usize)
                .sum();
            state.download_queue.len() > queued_uu
                || state
                    .download_queue
                    .peek_next_matching(|work| {
                        self.uu_files
                            .get(&work.segment_id.file_id)
                            .is_none_or(|uu| uu.next_index == work.segment_id.segment_number)
                    })
                    .is_some()
        })
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn job_has_dispatchable_work_for_test(
        &mut self,
        job_id: JobId,
    ) -> bool {
        self.job_has_dispatchable_work(job_id)
    }

    pub(in crate::pipeline::download) fn job_dispatch_priority(state: &JobState) -> u8 {
        state
            .spec
            .metadata
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("priority"))
            .map(|(_, value)| {
                if value.eq_ignore_ascii_case("high") {
                    0
                } else if value.eq_ignore_ascii_case("low") {
                    2
                } else {
                    1
                }
            })
            .unwrap_or(1)
    }
}
