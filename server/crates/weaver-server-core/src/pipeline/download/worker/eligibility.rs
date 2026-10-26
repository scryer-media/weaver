// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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

    #[cfg(test)]
    pub(in crate::pipeline::download::worker) fn job_has_dispatchable_work(
        &mut self,
        job_id: JobId,
    ) -> bool {
        let uu_capped = !self.uu_files.is_empty() && self.uu_spool_dispatch_capped();
        let direct_admission = self.direct_store_admission(job_id);
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

    // The gate the dispatcher stops this job at while it holds no lane: no
    // server, byte pressure, a direct-store hold or the restart checkpoint.
    // None while a lane holds the job or none of its queued work is gated, so
    // a job only waiting its turn behind another reads as plain queued.
    pub(in crate::pipeline) fn job_dispatch_hold_reason(
        &self,
        job_id: JobId,
    ) -> Option<&'static str> {
        let state = self.jobs.get(&job_id)?;
        if state.download_queue.is_empty()
            || !Self::status_allows_download_dispatch(&state.status)
            || self.job_has_current_download_activity(job_id)
        {
            return None;
        }
        // In the order the dispatch pass meets them.
        if crate::DownloadPressureState::from_code(
            self.metrics.download_pressure_state.load(Ordering::Relaxed),
        ) == crate::DownloadPressureState::Hard
        {
            return Some("byte_pressure");
        }
        if self.nntp.pool().server_count() == 0 {
            return Some("no_server");
        }
        let direct_admission = self.direct_store_admission(job_id);
        let checkpoint = self.checkpoint_admission(job_id);
        if direct_admission.is_empty() && !checkpoint.enforced {
            return None;
        }
        let mut reason = None;
        let open = state.download_queue.peek_first_matching(|work| {
            if !direct_admission.iter().all(|set| set.allows(work)) {
                reason.get_or_insert("direct_store");
                false
            } else if !checkpoint.decision(work, &[]).allows() {
                reason.get_or_insert("checkpoint");
                false
            } else {
                true
            }
        });
        if open.is_some() { None } else { reason }
    }

    // A hold lands or lifts, and a job gains or loses its lanes, with no job
    // event of its own, so each dispatch pass re-reports the jobs whose queue
    // row no longer says what the dispatcher sees.
    pub(in crate::pipeline) fn report_changed_dispatch_holds(&self) {
        let current: Vec<(JobId, Option<&'static str>, bool)> = self
            .job_order
            .iter()
            .map(|job_id| {
                let reason = self
                    .download_wait_by_job
                    .get(job_id)
                    .map(|wait| wait.reason)
                    .or_else(|| self.job_dispatch_hold_reason(*job_id));
                (
                    *job_id,
                    reason,
                    self.job_has_current_download_activity(*job_id),
                )
            })
            .collect();
        let stale: Vec<JobId> = self.shared_state.queue.read(|inner| {
            current
                .iter()
                .filter(|(job_id, reason, active)| {
                    inner
                        .jobs
                        .iter()
                        .find(|row| row.id == *job_id)
                        .is_some_and(|row| {
                            row.report.job.download_wait_reason.as_deref() != *reason
                                || row.report.download_active != *active
                        })
                })
                .map(|(job_id, _, _)| *job_id)
                .collect()
        });
        for job_id in stale {
            self.report_job(job_id);
        }
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
