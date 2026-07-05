use super::*;
use crate::jobs::assembly::{ArchiveMember, DetectedArchiveKind};
use crate::jobs::record::FileIdentitySource;
use std::collections::{BTreeSet, HashMap, HashSet};

const RAR_UNLOCK_PRIORITY: u32 = 3;
const RAR_UNLOCK_PROTECTED_PRIORITY: u32 = RAR_UNLOCK_PRIORITY - 1;
const RAR_UNLOCK_NON_SOLID_VOLUME_CAP: usize = 8;
const RAR_UNLOCK_SOLID_VOLUME_CAP: usize = 4;

#[derive(Debug, Clone, Copy)]
struct RarUnlockVolumeFile {
    file_id: NzbFileId,
    queued: bool,
    active: bool,
}

#[derive(Debug)]
struct RarUnlockPriorityPlan {
    updates: HashMap<NzbFileId, (u32, Option<u32>)>,
    boosted_files: HashSet<NzbFileId>,
}

#[derive(Debug)]
struct RarUnlockMemberCandidate<'a> {
    member: &'a ArchiveMember,
    queued_missing_volumes: Vec<u32>,
    incomplete_volume_count: usize,
}

impl Pipeline {
    pub(in crate::pipeline) fn mark_rar_unlock_priorities_dirty(&mut self, job_id: JobId) {
        self.rar_unlock_priority_dirty_jobs.insert(job_id);
    }

    pub(in crate::pipeline) fn clear_rar_unlock_priorities(&mut self, job_id: JobId) {
        self.rar_unlock_priority_dirty_jobs.remove(&job_id);
        self.rar_unlock_boosted_files.remove(&job_id);
    }

    pub(in crate::pipeline) fn rar_unlock_requeued_work_is_relevant(
        &self,
        work: &DownloadWork,
    ) -> bool {
        if work.is_recovery {
            return false;
        }
        let job_id = work.segment_id.file_id.job_id;
        let file_id = work.segment_id.file_id;
        if self
            .rar_unlock_boosted_files
            .get(&job_id)
            .is_some_and(|files| files.contains(&file_id))
        {
            return true;
        }

        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(file) = state.assembly.file(file_id) else {
            return false;
        };
        if matches!(
            self.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::RarVolume { .. }
        ) {
            return true;
        }

        self.effective_file_identity(job_id, file_id)
            .and_then(|identity| identity.classification)
            .is_some_and(|classification| {
                classification.kind == DetectedArchiveKind::Rar
                    && classification.volume_index.is_some()
            })
    }

    pub(in crate::pipeline) fn apply_rar_unlock_priorities_if_dirty(&mut self, job_id: JobId) {
        if !self.rar_unlock_priority_dirty_jobs.remove(&job_id) {
            return;
        }

        let Some(plan) = self.compute_rar_unlock_priority_plan(job_id) else {
            self.rar_unlock_boosted_files.remove(&job_id);
            return;
        };
        let updates = plan.updates;
        let boosted_files = plan.boosted_files;
        let changed = self
            .jobs
            .get_mut(&job_id)
            .map(|state| {
                state
                    .download_queue
                    .reprioritize_matching_with_rank(|work| {
                        if work.priority <= RAR_UNLOCK_PROTECTED_PRIORITY {
                            return None;
                        }
                        updates.get(&work.segment_id.file_id).copied()
                    })
            })
            .unwrap_or(0);

        if boosted_files.is_empty() {
            self.rar_unlock_boosted_files.remove(&job_id);
        } else {
            self.rar_unlock_boosted_files.insert(job_id, boosted_files);
        }

        if changed > 0 {
            debug!(
                job_id = job_id.0,
                changed, "RAR unlock download priorities updated"
            );
        }
    }

    fn compute_rar_unlock_priority_plan(&self, job_id: JobId) -> Option<RarUnlockPriorityPlan> {
        let state = self.jobs.get(&job_id)?;
        let previous_boosted = self
            .rar_unlock_boosted_files
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let mut normal_priority_by_file = HashMap::<NzbFileId, u32>::new();
        let mut volume_files = HashMap::<(String, u32), RarUnlockVolumeFile>::new();
        let mut completed_volume_files = HashSet::<(String, u32)>::new();
        let mut ambiguous_volumes = HashSet::<(String, u32)>::new();

        for file in state.assembly.files() {
            let file_id = file.file_id();
            let normal_priority = self
                .classified_role_for_file(job_id, file)
                .download_priority();
            normal_priority_by_file.insert(file_id, normal_priority);

            let Some(identity) = self.effective_file_identity(job_id, file_id) else {
                continue;
            };
            let Some(classification) = identity.classification.as_ref() else {
                continue;
            };
            if classification.kind != DetectedArchiveKind::Rar {
                continue;
            }
            let Some(volume_index) = classification.volume_index else {
                continue;
            };
            let trusted = identity.classification_source == FileIdentitySource::Par2
                || self.rar_unlock_topology_agrees_with_identity(job_id, &identity);
            if !trusted {
                continue;
            }

            let key = (classification.set_name.clone(), volume_index);
            if file.is_complete() {
                completed_volume_files.insert(key);
                continue;
            }
            let queued = state
                .download_queue
                .count_matching(|work| work.segment_id.file_id == file_id)
                > 0;
            let active = self.active_downloads_by_file.contains_key(&file_id);
            if !queued && !active {
                continue;
            }
            if volume_files
                .insert(
                    key.clone(),
                    RarUnlockVolumeFile {
                        file_id,
                        queued,
                        active,
                    },
                )
                .is_some()
            {
                ambiguous_volumes.insert(key);
            }
        }

        for key in ambiguous_volumes {
            volume_files.remove(&key);
            completed_volume_files.remove(&key);
        }

        let selected = self.select_rar_unlock_files(job_id, &volume_files, &completed_volume_files);
        let mut updates = HashMap::new();
        let mut boosted_files = HashSet::new();

        for (rank, file_id) in selected.into_iter().enumerate() {
            let Some(normal_priority) = normal_priority_by_file.get(&file_id).copied() else {
                continue;
            };
            // min() keeps protected-tier files (e.g. volume 0) at their base
            // priority while still correcting queue entries pushed before the
            // file was classified; the reprioritize closure never touches
            // entries already at or below the protected tier.
            let target_priority = normal_priority.min(RAR_UNLOCK_PRIORITY);
            updates.insert(file_id, (target_priority, Some(rank as u32)));
            boosted_files.insert(file_id);
        }

        for file_id in previous_boosted.difference(&boosted_files) {
            if let Some(priority) = normal_priority_by_file.get(file_id).copied() {
                updates.insert(*file_id, (priority, None));
            }
        }

        Some(RarUnlockPriorityPlan {
            updates,
            boosted_files,
        })
    }

    fn rar_unlock_topology_agrees_with_identity(
        &self,
        job_id: JobId,
        identity: &crate::jobs::record::ActiveFileIdentity,
    ) -> bool {
        let Some(classification) = identity.classification.as_ref() else {
            return false;
        };
        if classification.kind != DetectedArchiveKind::Rar {
            return false;
        }
        let Some(volume_index) = classification.volume_index else {
            return false;
        };

        let topology_volume = self
            .rar_sets
            .get(&(job_id, classification.set_name.clone()))
            .and_then(|state| state.plan.as_ref())
            .and_then(|plan| {
                plan.topology
                    .volume_map
                    .get(&identity.current_filename)
                    .copied()
                    .or_else(|| {
                        identity
                            .canonical_filename
                            .as_ref()
                            .and_then(|name| plan.topology.volume_map.get(name).copied())
                    })
            })
            .or_else(|| {
                self.jobs
                    .get(&job_id)
                    .and_then(|state| {
                        state
                            .assembly
                            .archive_topology_for(&classification.set_name)
                    })
                    .and_then(|topology| {
                        topology
                            .volume_map
                            .get(&identity.current_filename)
                            .copied()
                            .or_else(|| {
                                identity
                                    .canonical_filename
                                    .as_ref()
                                    .and_then(|name| topology.volume_map.get(name).copied())
                            })
                    })
            });

        topology_volume == Some(volume_index)
    }

    fn select_rar_unlock_files(
        &self,
        job_id: JobId,
        volume_files: &HashMap<(String, u32), RarUnlockVolumeFile>,
        completed_volume_files: &HashSet<(String, u32)>,
    ) -> Vec<NzbFileId> {
        let mut selected = Vec::new();
        let mut seen_files = HashSet::new();
        let mut solid_selected = 0usize;
        let mut sets = self
            .rar_sets
            .iter()
            .filter_map(|((set_job_id, set_name), state)| {
                (*set_job_id == job_id)
                    .then(|| state.plan.as_ref().map(|plan| (set_name, state, plan)))
                    .flatten()
            })
            .collect::<Vec<_>>();
        sets.sort_by_key(|(set_name, _, _)| (*set_name).clone());
        let planned_sets: HashSet<&str> = sets
            .iter()
            .map(|(set_name, _, _)| set_name.as_str())
            .collect();

        for (set_name, set_state, plan) in sets {
            if plan.is_solid {
                let remaining = RAR_UNLOCK_SOLID_VOLUME_CAP.saturating_sub(solid_selected);
                if remaining == 0 {
                    continue;
                }
                let added = self.select_solid_rar_unlock_files(
                    job_id,
                    set_name,
                    set_state,
                    volume_files,
                    completed_volume_files,
                    remaining,
                );
                for file_id in added {
                    if seen_files.insert(file_id) {
                        selected.push(file_id);
                        solid_selected += 1;
                    }
                }
                continue;
            }

            let remaining = RAR_UNLOCK_NON_SOLID_VOLUME_CAP.saturating_sub(selected.len());
            if remaining == 0 {
                break;
            }
            let added = self.select_non_solid_rar_unlock_files(
                job_id,
                set_name,
                set_state,
                volume_files,
                completed_volume_files,
                remaining,
            );
            for file_id in added {
                if seen_files.insert(file_id) {
                    selected.push(file_id);
                }
            }
        }

        // Sets whose volumes are classified through trusted identities (in
        // practice PAR2 metadata) but which have no computed plan yet. A plan
        // needs volume 0 or cached headers before it can exist, so pull the
        // sequential volume prefix forward; without this the download order
        // is volume-blind exactly while incremental extraction is waiting
        // for its first volume.
        let bootstrap_sets: BTreeSet<&str> = volume_files
            .keys()
            .map(|(set_name, _)| set_name.as_str())
            .filter(|set_name| !planned_sets.contains(set_name))
            .collect();
        for set_name in bootstrap_sets {
            let remaining = RAR_UNLOCK_NON_SOLID_VOLUME_CAP.saturating_sub(selected.len());
            if remaining == 0 {
                break;
            }
            let added = Self::select_bootstrap_rar_unlock_files(
                set_name,
                volume_files,
                completed_volume_files,
                remaining.min(RAR_UNLOCK_SOLID_VOLUME_CAP),
            );
            for file_id in added {
                if seen_files.insert(file_id) {
                    selected.push(file_id);
                }
            }
        }

        selected
    }

    /// Sequential-prefix selection for a set with no computed plan: walk
    /// volumes from 0 upward, skip completed and in-flight ones, and boost
    /// queued volumes until the known prefix breaks or the cap is reached.
    /// Volume 0 leads because it carries the archive headers that unlock the
    /// first real plan.
    fn select_bootstrap_rar_unlock_files(
        set_name: &str,
        volume_files: &HashMap<(String, u32), RarUnlockVolumeFile>,
        completed_volume_files: &HashSet<(String, u32)>,
        cap: usize,
    ) -> Vec<NzbFileId> {
        let Some(max_volume) = volume_files
            .keys()
            .chain(completed_volume_files.iter())
            .filter_map(|(name, volume)| (name.as_str() == set_name).then_some(*volume))
            .max()
        else {
            return Vec::new();
        };

        let mut selected = Vec::new();
        for volume in 0..=max_volume {
            if selected.len() >= cap {
                break;
            }
            let key = (set_name.to_string(), volume);
            if completed_volume_files.contains(&key) {
                continue;
            }
            let Some(file) = volume_files.get(&key) else {
                // A volume with nothing queued or active breaks the prefix;
                // volumes past the hole cannot produce headers or extend a
                // solid run.
                break;
            };
            if file.queued {
                selected.push(file.file_id);
            }
        }
        selected
    }

    fn select_non_solid_rar_unlock_files(
        &self,
        job_id: JobId,
        set_name: &str,
        set_state: &RarSetState,
        volume_files: &HashMap<(String, u32), RarUnlockVolumeFile>,
        completed_volume_files: &HashSet<(String, u32)>,
        cap: usize,
    ) -> Vec<NzbFileId> {
        let Some(plan) = set_state.plan.as_ref() else {
            return Vec::new();
        };
        let mut candidates = self.rar_unlock_member_candidates(
            job_id,
            set_name,
            set_state,
            volume_files,
            completed_volume_files,
        );
        candidates.sort_by(|left, right| {
            left.queued_missing_volumes
                .len()
                .cmp(&right.queued_missing_volumes.len())
                .then_with(|| {
                    left.incomplete_volume_count
                        .cmp(&right.incomplete_volume_count)
                })
                .then_with(|| {
                    (left.member.last_volume - left.member.first_volume)
                        .cmp(&(right.member.last_volume - right.member.first_volume))
                })
                .then_with(|| left.member.last_volume.cmp(&right.member.last_volume))
                .then_with(|| left.member.unpacked_size.cmp(&right.member.unpacked_size))
                .then_with(|| left.member.name.cmp(&right.member.name))
        });

        let mut selected = Vec::new();
        let mut seen_files = HashSet::new();
        for candidate in candidates {
            let remaining = cap.saturating_sub(selected.len());
            if remaining == 0 {
                break;
            }
            for volume in candidate.queued_missing_volumes.into_iter().take(remaining) {
                if let Some(file) = volume_files.get(&(set_name.to_string(), volume))
                    && seen_files.insert(file.file_id)
                {
                    selected.push(file.file_id);
                }
            }
        }

        debug_assert!(
            !plan.is_solid,
            "non-solid RAR unlock selector must not be used for solid plans"
        );
        selected
    }

    fn select_solid_rar_unlock_files(
        &self,
        job_id: JobId,
        set_name: &str,
        set_state: &RarSetState,
        volume_files: &HashMap<(String, u32), RarUnlockVolumeFile>,
        completed_volume_files: &HashSet<(String, u32)>,
        cap: usize,
    ) -> Vec<NzbFileId> {
        let mut candidates = self.rar_unlock_member_candidates(
            job_id,
            set_name,
            set_state,
            volume_files,
            completed_volume_files,
        );
        candidates.sort_by(|left, right| {
            left.member
                .first_volume
                .cmp(&right.member.first_volume)
                .then_with(|| left.member.last_volume.cmp(&right.member.last_volume))
                .then_with(|| left.member.name.cmp(&right.member.name))
        });
        let Some(candidate) = candidates.into_iter().next() else {
            return Vec::new();
        };

        candidate
            .queued_missing_volumes
            .into_iter()
            .take(cap)
            .filter_map(|volume| volume_files.get(&(set_name.to_string(), volume)))
            .map(|file| file.file_id)
            .collect()
    }

    fn rar_unlock_member_candidates<'a>(
        &'a self,
        job_id: JobId,
        set_name: &str,
        set_state: &'a RarSetState,
        volume_files: &HashMap<(String, u32), RarUnlockVolumeFile>,
        completed_volume_files: &HashSet<(String, u32)>,
    ) -> Vec<RarUnlockMemberCandidate<'a>> {
        let Some(plan) = set_state.plan.as_ref() else {
            return Vec::new();
        };
        let extracted = self.extracted_members.get(&job_id);
        let failed = self.failed_extractions.get(&job_id);

        plan.topology
            .members
            .iter()
            .filter_map(|member| {
                if extracted.is_some_and(|members| members.contains(&member.name))
                    || failed.is_some_and(|members| members.contains(&member.name))
                    || set_state.in_flight_members.contains(&member.name)
                {
                    return None;
                }
                let mut queued_missing_volumes = Vec::new();
                let mut incomplete_volume_count = 0usize;
                for volume in member.first_volume..=member.last_volume {
                    let volume_key = (set_name.to_string(), volume);
                    if plan.topology.complete_volumes.contains(&volume)
                        || completed_volume_files.contains(&volume_key)
                    {
                        continue;
                    }
                    let file = volume_files.get(&volume_key)?;
                    if !file.active && !file.queued {
                        return None;
                    }
                    incomplete_volume_count += 1;
                    if file.queued {
                        queued_missing_volumes.push(volume);
                    }
                }
                (!queued_missing_volumes.is_empty()).then_some(RarUnlockMemberCandidate {
                    member,
                    queued_missing_volumes,
                    incomplete_volume_count,
                })
            })
            .collect()
    }
}
