use std::collections::{HashMap, HashSet};

use super::*;

const PROMOTED_RECOVERY_PRIORITY: u32 = 2;
const PAR2_PACKET_ALIGNMENT: u64 = 4;
const PAR2_RECOVERY_PACKET_OVERHEAD: u64 = 68; // 64-byte header + 4-byte exponent

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryCountSource {
    Exact,
    Calibrated,
    FilenameFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveryCandidate {
    file_index: u32,
    blocks: u32,
    total_bytes: u64,
    source: RecoveryCountSource,
}

fn par2_recovery_packet_size(slice_size: u64) -> u64 {
    let raw = slice_size.saturating_add(PAR2_RECOVERY_PACKET_OVERHEAD);
    let rem = raw % PAR2_PACKET_ALIGNMENT;
    if rem == 0 {
        raw
    } else {
        raw + (PAR2_PACKET_ALIGNMENT - rem)
    }
}

fn recovery_file_bytes(spec: &JobSpec, file_index: u32) -> Option<u64> {
    let file = spec.files.get(file_index as usize)?;
    Some(
        file.segments
            .iter()
            .map(|segment| segment.bytes as u64)
            .sum(),
    )
}

fn recovery_file_role(spec: &JobSpec, file_index: u32) -> Option<weaver_core::classify::FileRole> {
    spec.files
        .get(file_index as usize)
        .map(|file| file.role.clone())
}

fn compare_selection(
    lhs: (u64, usize, u32, &[u32]),
    rhs: (u64, usize, u32, &[u32]),
) -> std::cmp::Ordering {
    lhs.0
        .cmp(&rhs.0)
        .then_with(|| lhs.1.cmp(&rhs.1))
        .then_with(|| lhs.2.cmp(&rhs.2))
        .then_with(|| lhs.3.cmp(rhs.3))
}

fn select_recovery_file_indices(
    candidates: &[RecoveryCandidate],
    remaining_needed: u32,
) -> Vec<u32> {
    if remaining_needed == 0 || candidates.is_empty() {
        return Vec::new();
    }

    let mut ordered = candidates.to_vec();
    ordered.sort_by_key(|candidate| (candidate.total_bytes, candidate.file_index));

    if ordered.len() > 24 {
        let mut selected = Vec::new();
        let mut covered = 0u32;
        for candidate in ordered {
            if covered >= remaining_needed {
                break;
            }
            selected.push(candidate.file_index);
            covered = covered.saturating_add(candidate.blocks);
        }
        return selected;
    }

    let mut best: Option<(u64, usize, u32, Vec<u32>)> = None;
    let total_masks = 1u128 << ordered.len();
    for mask in 1u128..total_masks {
        let mut covered = 0u32;
        let mut total_bytes = 0u64;
        let mut file_indices = Vec::new();

        for (idx, candidate) in ordered.iter().enumerate() {
            if (mask & (1u128 << idx)) == 0 {
                continue;
            }
            covered = covered.saturating_add(candidate.blocks);
            total_bytes = total_bytes.saturating_add(candidate.total_bytes);
            file_indices.push(candidate.file_index);
        }

        if covered < remaining_needed {
            continue;
        }

        let overshoot = covered - remaining_needed;
        let current = (total_bytes, file_indices.len(), overshoot, file_indices);
        let replace = best.as_ref().is_none_or(|existing| {
            compare_selection(
                (current.0, current.1, current.2, current.3.as_slice()),
                (existing.0, existing.1, existing.2, existing.3.as_slice()),
            ) == std::cmp::Ordering::Less
        });
        if replace {
            best = Some(current);
        }
    }

    best.map(|(_, _, _, file_indices)| file_indices)
        .unwrap_or_else(|| {
            ordered
                .into_iter()
                .map(|candidate| candidate.file_index)
                .collect()
        })
}

impl Pipeline {
    pub(super) fn note_recovery_block_count(
        &mut self,
        job_id: JobId,
        file_index: u32,
        blocks: u32,
    ) {
        self.recovery_block_counts
            .entry(job_id)
            .or_default()
            .insert(file_index, blocks);
    }

    pub(super) fn note_recovery_count_from_yenc_name(
        &mut self,
        job_id: JobId,
        file_index: u32,
        yenc_name: &str,
    ) {
        if yenc_name.is_empty() {
            return;
        }

        match weaver_core::classify::FileRole::from_filename(yenc_name) {
            weaver_core::classify::FileRole::Par2 {
                is_index,
                recovery_block_count,
            } => {
                let blocks = if is_index { 0 } else { recovery_block_count };
                self.note_recovery_block_count(job_id, file_index, blocks);
            }
            _ => {}
        }
    }

    fn recovery_packet_size(&self, job_id: JobId) -> Option<u64> {
        let par2_set = self.par2_sets.get(&job_id)?;
        Some(par2_recovery_packet_size(par2_set.slice_size))
    }

    fn recovery_metadata_overhead_bytes(&self, job_id: JobId) -> Option<u64> {
        let packet_size = self.recovery_packet_size(job_id)?;
        let state = self.jobs.get(&job_id)?;

        let mut overheads = Vec::new();

        if let Some(exact) = self.recovery_block_counts.get(&job_id) {
            for (&file_index, &blocks) in exact {
                let Some(total_bytes) = recovery_file_bytes(&state.spec, file_index) else {
                    continue;
                };
                let Some(block_bytes) = packet_size.checked_mul(blocks as u64) else {
                    continue;
                };
                if total_bytes >= block_bytes {
                    overheads.push(total_bytes - block_bytes);
                }
            }
        }

        if overheads.is_empty() {
            for (file_index, file) in state.spec.files.iter().enumerate() {
                if matches!(
                    file.role,
                    weaver_core::classify::FileRole::Par2 { is_index: true, .. }
                ) && let Some(total_bytes) = recovery_file_bytes(&state.spec, file_index as u32)
                {
                    overheads.push(total_bytes);
                }
            }
        }

        if overheads.is_empty() {
            return None;
        }

        overheads.sort_unstable();
        Some(overheads[overheads.len() / 2])
    }

    fn recovery_block_count_for(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> Option<(u32, RecoveryCountSource)> {
        if let Some(blocks) = self
            .recovery_block_counts
            .get(&job_id)
            .and_then(|counts| counts.get(&file_index))
            .copied()
        {
            return Some((blocks, RecoveryCountSource::Exact));
        }

        let state = self.jobs.get(&job_id)?;
        let role = recovery_file_role(&state.spec, file_index)?;

        if matches!(
            role,
            weaver_core::classify::FileRole::Par2 { is_index: true, .. }
        ) {
            return Some((0, RecoveryCountSource::Exact));
        }

        if let (Some(packet_size), Some(overhead), Some(total_bytes)) = (
            self.recovery_packet_size(job_id),
            self.recovery_metadata_overhead_bytes(job_id),
            recovery_file_bytes(&state.spec, file_index),
        ) {
            let delta = total_bytes.saturating_sub(overhead);
            let estimated = if delta == 0 {
                0
            } else {
                ((delta + (packet_size / 2)) / packet_size) as u32
            };
            return Some((estimated, RecoveryCountSource::Calibrated));
        }

        match role {
            weaver_core::classify::FileRole::Par2 {
                is_index,
                recovery_block_count,
            } => Some((
                if is_index { 0 } else { recovery_block_count },
                RecoveryCountSource::FilenameFallback,
            )),
            _ => None,
        }
    }

    fn recovery_candidate_for(&self, job_id: JobId, file_index: u32) -> Option<RecoveryCandidate> {
        let state = self.jobs.get(&job_id)?;
        let total_bytes = recovery_file_bytes(&state.spec, file_index)?;
        let (blocks, source) = self.recovery_block_count_for(job_id, file_index)?;
        Some(RecoveryCandidate {
            file_index,
            blocks,
            total_bytes,
            source,
        })
    }

    fn available_recovery_file_indices(&self, job_id: JobId) -> HashSet<u32> {
        let Some(state) = self.jobs.get(&job_id) else {
            return HashSet::new();
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
    }

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
            .filter_map(|(file_index, _)| self.recovery_block_count_for(job_id, file_index as u32))
            .map(|(blocks, _)| blocks)
            .sum()
    }

    pub(super) fn recovery_blocks_available_or_targeted(&self, job_id: JobId) -> u32 {
        self.available_recovery_file_indices(job_id)
            .into_iter()
            .filter_map(|file_index| self.recovery_block_count_for(job_id, file_index))
            .map(|(blocks, _)| blocks)
            .sum()
    }

    /// Promote the smallest byte set of recovery files needed to cover the requested block count.
    ///
    /// Returns the number of recovery blocks newly promoted by this call.
    pub(super) fn promote_recovery_targeted(&mut self, job_id: JobId, blocks_needed: u32) -> u32 {
        let already_available_blocks = self.recovery_blocks_available_or_targeted(job_id);
        let remaining_needed = blocks_needed.saturating_sub(already_available_blocks);
        if remaining_needed == 0 {
            return 0;
        }

        let queued = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return 0;
            };
            state.recovery_queue.drain_all()
        };

        let mut work_by_file: HashMap<u32, Vec<DownloadWork>> = HashMap::new();
        for work in queued {
            work_by_file
                .entry(work.segment_id.file_id.file_index)
                .or_default()
                .push(work);
        }

        let mut candidates = Vec::new();
        for file_index in work_by_file.keys().copied() {
            if self
                .promoted_recovery_files
                .get(&job_id)
                .is_some_and(|files| files.contains(&file_index))
            {
                continue;
            }
            if let Some(candidate) = self.recovery_candidate_for(job_id, file_index)
                && candidate.blocks > 0
            {
                candidates.push(candidate);
            }
        }

        let selected: HashSet<u32> = select_recovery_file_indices(&candidates, remaining_needed)
            .into_iter()
            .collect();

        let source_map: HashMap<u32, RecoveryCountSource> = candidates
            .iter()
            .map(|candidate| (candidate.file_index, candidate.source))
            .collect();
        let block_map: HashMap<u32, u32> = candidates
            .iter()
            .map(|candidate| (candidate.file_index, candidate.blocks))
            .collect();

        let (promoted_file_indices, promoted_blocks, promoted_segments, sources) = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return 0;
            };

            let mut promoted_file_indices = Vec::new();
            let mut promoted_blocks = 0u32;
            let mut promoted_segments = 0usize;
            let mut promoted_sources = Vec::new();

            for (file_index, mut works) in work_by_file {
                if selected.contains(&file_index) {
                    for mut work in works.drain(..) {
                        work.priority = PROMOTED_RECOVERY_PRIORITY;
                        state.download_queue.push(work);
                        promoted_segments += 1;
                    }
                    promoted_file_indices.push(file_index);
                    promoted_blocks = promoted_blocks
                        .saturating_add(block_map.get(&file_index).copied().unwrap_or(0));
                    if let Some(source) = source_map.get(&file_index).copied() {
                        promoted_sources.push((file_index, source));
                    }
                } else {
                    for work in works.drain(..) {
                        state.recovery_queue.push(work);
                    }
                }
            }

            (
                promoted_file_indices,
                promoted_blocks,
                promoted_segments,
                promoted_sources,
            )
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
                promoted_sources = ?sources,
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
            .filter(|state| !is_terminal_status(&state.status))
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
    use super::{
        RecoveryCandidate, RecoveryCountSource, is_terminal_status, par2_recovery_packet_size,
        select_recovery_file_indices,
    };
    use weaver_scheduler::JobStatus;

    #[test]
    fn targeted_selection_prefers_minimum_bytes_then_file_count() {
        let selected = select_recovery_file_indices(
            &[
                RecoveryCandidate {
                    file_index: 1,
                    blocks: 1,
                    total_bytes: 10,
                    source: RecoveryCountSource::Exact,
                },
                RecoveryCandidate {
                    file_index: 2,
                    blocks: 2,
                    total_bytes: 20,
                    source: RecoveryCountSource::Exact,
                },
                RecoveryCandidate {
                    file_index: 3,
                    blocks: 4,
                    total_bytes: 40,
                    source: RecoveryCountSource::Exact,
                },
                RecoveryCandidate {
                    file_index: 4,
                    blocks: 8,
                    total_bytes: 80,
                    source: RecoveryCountSource::Exact,
                },
                RecoveryCandidate {
                    file_index: 5,
                    blocks: 16,
                    total_bytes: 160,
                    source: RecoveryCountSource::Exact,
                },
            ],
            20,
        );
        assert_eq!(selected, vec![3, 5]);
    }

    #[test]
    fn targeted_selection_returns_empty_when_covered() {
        let selected = select_recovery_file_indices(&[], 0);
        assert!(selected.is_empty());
    }

    #[test]
    fn recovery_packet_size_rounds_to_alignment() {
        assert_eq!(par2_recovery_packet_size(8), 76);
        assert_eq!(par2_recovery_packet_size(9), 80);
    }

    #[test]
    fn terminal_status_detection_matches_history_contract() {
        assert!(is_terminal_status(&JobStatus::Complete));
        assert!(is_terminal_status(&JobStatus::Failed {
            error: "boom".to_string(),
        }));
        assert!(!is_terminal_status(&JobStatus::Downloading));
        assert!(!is_terminal_status(&JobStatus::Paused));
    }
}
