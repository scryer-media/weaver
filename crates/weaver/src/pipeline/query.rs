use std::collections::{HashMap, HashSet};

use super::*;

const PROMOTED_RECOVERY_PRIORITY: u32 = 2;

fn recovery_file_block_count(spec: &JobSpec, file_index: u32) -> Option<u32> {
    match spec.files.get(file_index as usize)?.role {
        weaver_core::classify::FileRole::Par2 {
            is_index: false,
            recovery_block_count,
        } => Some(recovery_block_count),
        _ => None,
    }
}

fn select_recovery_file_indices(candidates: &[(u32, u32)], remaining_needed: u32) -> Vec<u32> {
    if remaining_needed == 0 {
        return Vec::new();
    }

    let mut ordered = candidates.to_vec();
    ordered.sort_by_key(|(file_index, block_count)| (*block_count, *file_index));

    let mut selected = Vec::new();
    let mut covered = 0u32;
    for (file_index, block_count) in ordered {
        if covered >= remaining_needed {
            break;
        }
        selected.push(file_index);
        covered = covered.saturating_add(block_count);
    }

    selected
}

impl Pipeline {
    /// Set a job's status.
    pub(super) fn set_job_status(
        &mut self,
        job_id: JobId,
        status: JobStatus,
    ) -> Result<(), weaver_scheduler::SchedulerError> {
        match self.jobs.get_mut(&job_id) {
            Some(state) => {
                state.status = status;
                Ok(())
            }
            None => Err(weaver_scheduler::SchedulerError::JobNotFound(job_id)),
        }
    }

    pub(super) fn total_recovery_block_capacity(&self, job_id: JobId) -> u32 {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };

        state
            .spec
            .files
            .iter()
            .enumerate()
            .filter_map(|(file_index, _)| recovery_file_block_count(&state.spec, file_index as u32))
            .sum()
    }

    pub(super) fn recovery_blocks_available_or_targeted(&self, job_id: JobId) -> u32 {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };

        let mut file_indices = self
            .promoted_recovery_files
            .get(&job_id)
            .cloned()
            .unwrap_or_default();

        for file in state.assembly.files() {
            if file.is_complete()
                && matches!(
                    file.role(),
                    weaver_core::classify::FileRole::Par2 {
                        is_index: false,
                        ..
                    }
                )
            {
                file_indices.insert(file.file_id().file_index);
            }
        }

        file_indices
            .into_iter()
            .filter_map(|file_index| recovery_file_block_count(&state.spec, file_index))
            .sum()
    }

    /// Promote the smallest recovery files needed to cover the requested block count.
    ///
    /// Returns the number of recovery blocks newly promoted by this call.
    pub(super) fn promote_recovery_targeted(&mut self, job_id: JobId, blocks_needed: u32) -> u32 {
        let already_available_blocks = self.recovery_blocks_available_or_targeted(job_id);
        let remaining_needed = blocks_needed.saturating_sub(already_available_blocks);
        if remaining_needed == 0 {
            return 0;
        }

        let spec_block_counts: HashMap<u32, u32> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return 0;
            };
            state
                .spec
                .files
                .iter()
                .enumerate()
                .filter_map(|(file_index, _)| {
                    recovery_file_block_count(&state.spec, file_index as u32)
                        .map(|blocks| (file_index as u32, blocks))
                })
                .collect()
        };

        let (promoted_file_indices, promoted_blocks, promoted_segments) = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return 0;
            };

            let queued = state.recovery_queue.drain_all();
            let mut work_by_file: HashMap<u32, Vec<DownloadWork>> = HashMap::new();
            for work in queued {
                work_by_file
                    .entry(work.segment_id.file_id.file_index)
                    .or_default()
                    .push(work);
            }

            let candidates: Vec<(u32, u32)> = work_by_file
                .keys()
                .filter_map(|file_index| {
                    spec_block_counts
                        .get(file_index)
                        .copied()
                        .map(|blocks| (*file_index, blocks))
                })
                .collect();
            let selected: HashSet<u32> =
                select_recovery_file_indices(&candidates, remaining_needed)
                    .into_iter()
                    .collect();

            let mut promoted_file_indices = Vec::new();
            let mut promoted_blocks = 0u32;
            let mut promoted_segments = 0usize;

            for (file_index, mut works) in work_by_file {
                if selected.contains(&file_index) {
                    for mut work in works.drain(..) {
                        work.priority = PROMOTED_RECOVERY_PRIORITY;
                        state.download_queue.push(work);
                        promoted_segments += 1;
                    }
                    promoted_file_indices.push(file_index);
                    promoted_blocks = promoted_blocks
                        .saturating_add(spec_block_counts.get(&file_index).copied().unwrap_or(0));
                } else {
                    for work in works.drain(..) {
                        state.recovery_queue.push(work);
                    }
                }
            }

            (promoted_file_indices, promoted_blocks, promoted_segments)
        };

        if !promoted_file_indices.is_empty() {
            self.promoted_recovery_files
                .entry(job_id)
                .or_default()
                .extend(promoted_file_indices.iter().copied());
            info!(
                job_id = job_id.0,
                blocks_needed,
                already_available_blocks,
                promoted_blocks,
                promoted_segments,
                promoted_files = ?promoted_file_indices,
                "promoted targeted recovery files"
            );
            self.update_queue_metrics();
        } else {
            debug!(
                job_id = job_id.0,
                blocks_needed,
                already_available_blocks,
                "no additional recovery files available to promote"
            );
        }

        promoted_blocks
    }

    /// Promote all recovery files for a job from its recovery queue to its
    /// primary queue. Used as an emergency fallback when targeted promotion
    /// cannot determine a trustworthy lower bound.
    #[allow(dead_code)]
    pub(super) fn promote_recovery(&mut self, job_id: JobId) {
        let total_capacity = self.total_recovery_block_capacity(job_id);
        let promoted = self.promote_recovery_targeted(job_id, total_capacity);
        if promoted > 0 {
            info!(
                job_id = job_id.0,
                promoted_blocks = promoted,
                "promoted all recovery files to primary queue"
            );
        }
    }

    /// List all jobs.
    pub(super) fn list_jobs(&self) -> Vec<JobInfo> {
        let mut list: Vec<JobInfo> = self
            .jobs
            .values()
            .map(|state| {
                let total = state.spec.total_bytes;
                let health = if total == 0 {
                    1000
                } else {
                    ((total.saturating_sub(state.failed_bytes)) * 1000 / total) as u32
                };
                JobInfo {
                    job_id: state.job_id,
                    name: state.spec.name.clone(),
                    error: if let JobStatus::Failed { error } = &state.status {
                        Some(error.clone())
                    } else {
                        None
                    },
                    status: state.status.clone(),
                    progress: state.assembly.progress(),
                    total_bytes: total,
                    downloaded_bytes: state.downloaded_bytes,
                    failed_bytes: state.failed_bytes,
                    health,
                    password: state.spec.password.clone(),
                    category: state.spec.category.clone(),
                    metadata: state.spec.metadata.clone(),
                    output_dir: Some(state.working_dir.display().to_string()),
                    created_at_epoch_ms: state.created_at_epoch_ms,
                }
            })
            .collect();
        list.extend(self.finished_jobs.iter().cloned());
        list
    }
}

#[cfg(test)]
mod tests {
    use super::select_recovery_file_indices;

    #[test]
    fn targeted_selection_prefers_smallest_files() {
        let selected = select_recovery_file_indices(&[(9, 8), (4, 2), (2, 2), (7, 4)], 5);
        assert_eq!(selected, vec![2, 4, 7]);
    }

    #[test]
    fn targeted_selection_returns_empty_when_covered() {
        let selected = select_recovery_file_indices(&[(1, 2), (2, 4)], 0);
        assert!(selected.is_empty());
    }
}
