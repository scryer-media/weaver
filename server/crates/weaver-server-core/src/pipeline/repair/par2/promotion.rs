//! Continuation of the `impl Pipeline` block from `repair/par2.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl Pipeline {
    pub(super) fn recovery_candidate_for(
        &self,
        job_id: JobId,
        file_index: u32,
        set_id: par2_rs::RecoverySetId,
    ) -> Option<RecoveryCandidate> {
        let state = self.jobs.get(&job_id)?;
        let total_bytes = recovery_file_bytes(&state.spec, file_index)?;
        let (blocks, source) = self.recovery_block_count_for(job_id, file_index, set_id)?;
        Some(RecoveryCandidate {
            file_index,
            blocks,
            total_bytes,
            source,
        })
    }

    pub(crate) fn is_promoted_recovery_file(&self, job_id: JobId, file_index: u32) -> bool {
        self.par3_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.is_promoted(job_id, file_index))
            || self
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&file_index))
                .is_some_and(|file| file.promoted)
    }

    pub(crate) fn segment_is_completion_critical(&self, segment_id: SegmentId) -> bool {
        self.par3_runtime.as_ref().is_some_and(|runtime| {
            runtime.article_promoted(
                segment_id.file_id.job_id,
                segment_id.file_id.file_index,
                segment_id.segment_number,
            )
        }) || self
            .par2_runtime(segment_id.file_id.job_id)
            .and_then(|runtime| runtime.files.get(&segment_id.file_id.file_index))
            .is_some_and(|file| {
                file.promoted
                    || match &file.discovery {
                        Par2DiscoveryState::PrefixProbeQueued => file
                            .discovery_probe_ordinals
                            .iter()
                            .max()
                            .is_some_and(|ordinal| *ordinal == segment_id.segment_number),
                        Par2DiscoveryState::MetadataCarrierQueued { .. } => {
                            file.metadata_carrier_completion_critical
                        }
                        _ => false,
                    }
            })
    }

    pub(super) fn promoted_recovery_file_is_complete(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        state
            .assembly
            .file(NzbFileId { job_id, file_index })
            .is_some_and(|file| file.is_complete())
    }

    pub(crate) fn promoted_recovery_file_has_unavailable_segment(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> bool {
        self.unavailable_promoted_recovery_segments
            .iter()
            .any(|segment_id| {
                segment_id.file_id.job_id == job_id && segment_id.file_id.file_index == file_index
            })
    }

    pub(crate) fn mark_promoted_recovery_segment_unavailable(&mut self, segment_id: SegmentId) {
        let discovery_work = self
            .par2_runtime(segment_id.file_id.job_id)
            .and_then(|runtime| runtime.files.get(&segment_id.file_id.file_index))
            .is_some_and(|file| file.discovery.work_is_queued());
        if !self.is_promoted_recovery_file(segment_id.file_id.job_id, segment_id.file_id.file_index)
            && !discovery_work
        {
            return;
        }
        if self
            .unavailable_promoted_recovery_segments
            .insert(segment_id)
        {
            warn!(
                segment = %segment_id,
                "promoted PAR2 recovery segment became unavailable"
            );
            self.refresh_par2_metadata_discovery(segment_id.file_id.job_id);
            self.schedule_job_completion_check(segment_id.file_id.job_id);
        }
    }

    pub(crate) fn promoted_recovery_file_has_pending_work(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> bool {
        let file_id = NzbFileId { job_id, file_index };
        let queued_download = self.jobs.get(&job_id).is_some_and(|state| {
            state
                .download_queue
                .count_matching(|work| work.segment_id.file_id == file_id)
                > 0
        });
        let active_download = self.active_downloads_by_file.contains_key(&file_id);
        let delayed_retry = self
            .pending_retries_by_segment
            .keys()
            .any(|segment_id| segment_id.file_id == file_id);
        let pending_decode = self
            .pending_decode
            .iter()
            .any(|work| work.segment_id.file_id == file_id);
        let active_decode = self.active_decodes_by_file.contains_key(&file_id);
        let write_buffered = self
            .write_buffers
            .get(&file_id)
            .is_some_and(|buffer| buffer.buffered_len() > 0);

        queued_download
            || active_download
            || delayed_retry
            || pending_decode
            || active_decode
            || write_buffered
    }

    pub(super) fn file_is_completion_critical(&self, file_id: NzbFileId) -> bool {
        self.par2_runtime(file_id.job_id)
            .and_then(|runtime| runtime.files.get(&file_id.file_index))
            .is_some_and(|file| {
                file.promoted
                    || matches!(file.discovery, Par2DiscoveryState::PrefixProbeQueued)
                    || matches!(
                        file.discovery,
                        Par2DiscoveryState::MetadataCarrierQueued { .. }
                    ) && file.metadata_carrier_completion_critical
            })
    }

    pub(super) fn jobs_fetching_repair_data(&self) -> HashSet<JobId> {
        let mut fetching = self
            .jobs
            .iter()
            .filter_map(|(job_id, state)| {
                state
                    .download_queue
                    .has_completion_critical_work()
                    .then_some(*job_id)
            })
            .collect::<HashSet<_>>();
        for segment_id in self.pending_retries_by_segment.keys() {
            if self.segment_is_completion_critical(*segment_id) {
                fetching.insert(segment_id.file_id.job_id);
            }
        }
        for work in &self.pending_decode {
            if self.segment_is_completion_critical(work.segment_id) {
                fetching.insert(work.segment_id.file_id.job_id);
            }
        }
        for file_id in self.active_downloads_by_file.keys() {
            if self.file_is_completion_critical(*file_id) {
                fetching.insert(file_id.job_id);
            }
        }
        for file_id in self.active_decodes_by_file.keys() {
            if self.file_is_completion_critical(*file_id) {
                fetching.insert(file_id.job_id);
            }
        }
        fetching
    }

    pub(super) fn loaded_recovery_file_indices(&self, job_id: JobId) -> HashSet<u32> {
        let Some(state) = self.jobs.get(&job_id) else {
            return HashSet::new();
        };
        let mut file_indices = HashSet::new();

        for file in state.assembly.files() {
            if file.is_complete()
                && matches!(
                    file.role(),
                    weaver_model::files::FileRole::Par2 {
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

    pub(super) fn targeted_recovery_file_indices(&self, job_id: JobId) -> HashSet<u32> {
        self.par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| {
                        if !file.promoted
                            || self.promoted_recovery_file_is_complete(job_id, file_index)
                            || self
                                .promoted_recovery_file_has_unavailable_segment(job_id, file_index)
                        {
                            return None;
                        }
                        self.promoted_recovery_file_has_pending_work(job_id, file_index)
                            .then_some(file_index)
                    })
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default()
    }

    /// Volumes that will never complete but whose surviving recovery packets
    /// were read back off disk and merged.
    ///
    /// They are counted apart from the two sets above because both of those
    /// bypass the runtime entry for exactly this file: the loaded set requires
    /// a complete assembly, and the targeted set requires work still in flight.
    pub(super) fn salvaged_recovery_file_indices(&self, job_id: JobId) -> HashSet<u32> {
        self.par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| file.salvaged.then_some(file_index))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn total_recovery_block_capacity(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> u32 {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };

        state
            .spec
            .files
            .iter()
            .enumerate()
            .filter(|(file_index, file)| {
                matches!(
                    file.role,
                    weaver_model::files::FileRole::Par2 {
                        is_index: false,
                        ..
                    }
                ) || self
                    .par2_runtime(job_id)
                    .and_then(|runtime| runtime.files.get(&(*file_index as u32)))
                    .is_some_and(|file| file.recovery_set_packets_read || file.recovery_blocks > 0)
            })
            .map(|(file_index, _)| file_index as u32)
            .filter(|file_index| self.recovery_file_serves_set(job_id, *file_index, set_id))
            .filter_map(|file_index| self.recovery_block_count_for(job_id, file_index, set_id))
            .map(|(blocks, _)| blocks)
            .sum()
    }

    /// How much recovery this repair can count on: what has arrived, what is on
    /// its way, and what was read back off a volume that will never arrive.
    ///
    /// The three groups are counted through [`Self::recovery_block_count_for`],
    /// which answers with a validated count wherever there is one and only falls
    /// back to what a file advertises while nothing of it has been read. That
    /// ordering is what keeps a volume stranded holding three of its
    /// twenty-four blocks from being credited with the other twenty-one — a
    /// promise nothing can keep, which reads here as a shortfall already
    /// covered, so no further recovery is promoted and the job waits for an
    /// arrival that has no source. A volume still on its way has proved nothing
    /// yet and rightly contributes what it advertises: that is what "targeted"
    /// means.
    pub(crate) fn recovery_blocks_available_or_targeted(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> u32 {
        let mut file_indices = self.loaded_recovery_file_indices(job_id);
        file_indices.extend(self.targeted_recovery_file_indices(job_id));
        file_indices.extend(self.salvaged_recovery_file_indices(job_id));
        file_indices
            .into_iter()
            .filter(|file_index| self.recovery_file_serves_set(job_id, *file_index, set_id))
            .filter_map(|file_index| self.recovery_block_count_for(job_id, file_index, set_id))
            .map(|(blocks, _)| blocks)
            .sum()
    }

    pub(in crate::pipeline) fn par2_metadata_candidate_indices(
        &self,
        job_id: JobId,
    ) -> Vec<(u32, bool, u64)> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        let signature_candidates = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| {
                        file.signature_candidate.then_some(file_index)
                    })
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        state
            .spec
            .files
            .iter()
            .enumerate()
            .filter_map(|(file_index, file)| {
                let file_index = file_index as u32;
                match file.role {
                    weaver_model::files::FileRole::Par2 { is_index, .. } => Some((
                        file_index,
                        is_index,
                        file.segments
                            .iter()
                            .map(|segment| segment.bytes as u64)
                            .sum(),
                    )),
                    _ if signature_candidates.contains(&file_index) => Some((
                        file_index,
                        false,
                        file.segments
                            .iter()
                            .map(|segment| segment.bytes as u64)
                            .sum(),
                    )),
                    _ => None,
                }
            })
            .collect()
    }

    pub(in crate::pipeline) fn par2_discovery_state_for_candidate(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> Par2DiscoveryState {
        let Some(runtime) = self.par2_runtime(job_id) else {
            return Par2DiscoveryState::Unseen;
        };
        if let Some(file) = runtime.files.get(&file_index) {
            return file.discovery.clone();
        }
        // Restored and test-built runtimes can install parsed set state without
        // replaying the file-complete callback. The set summary is durable
        // proof that this exact candidate already parsed; do not requeue it.
        let mut set_ids = runtime
            .sets
            .iter()
            .filter_map(|(set_id, set_runtime)| {
                (set_runtime.set.is_some() && set_runtime.summary.index_file_index == file_index)
                    .then_some(*set_id)
            })
            .collect::<Vec<_>>();
        set_ids.sort_by_key(|set_id| *set_id.as_bytes());
        if set_ids.is_empty() {
            Par2DiscoveryState::Unseen
        } else {
            Par2DiscoveryState::Parsed { set_ids }
        }
    }

    /// Move finished probe/carrier attempts to a state that can make the next
    /// deterministic discovery decision. The prefix scanner validates packet
    /// headers and hashes through par2-rs before any SetID becomes authority.
    pub(super) fn refresh_par2_metadata_discovery(&mut self, job_id: JobId) {
        let candidates = self.par2_metadata_candidate_indices(job_id);
        for (file_index, _is_index, _) in candidates {
            let discovery = self
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&file_index))
                .map(|file| file.discovery.clone())
                .unwrap_or_default();
            if !discovery.work_is_queued()
                || self.promoted_recovery_file_has_pending_work(job_id, file_index)
            {
                continue;
            }

            match discovery {
                Par2DiscoveryState::PrefixProbeQueued => {
                    let file_id = NzbFileId { job_id, file_index };
                    let prefix = self.file_prefix_16k.get(&file_id);
                    let set_ids = prefix
                        .map(|prefix| par2_prefix_set_ids(prefix))
                        .unwrap_or_default();
                    let assembly = self
                        .jobs
                        .get(&job_id)
                        .and_then(|state| state.assembly.file(file_id));
                    let probe_may_arrive =
                        self.par2_runtime(job_id)
                            .and_then(|runtime| runtime.files.get(&file_index))
                            .is_some_and(|file| {
                                file.discovery_probe_ordinals.iter().any(|ordinal| {
                                    !self.unavailable_promoted_recovery_segments.contains(
                                        &SegmentId {
                                            file_id,
                                            segment_number: *ordinal,
                                        },
                                    ) && !assembly.is_some_and(|file| file.has_segment(*ordinal))
                                })
                            });
                    if set_ids.is_empty()
                        && prefix.is_none_or(Vec::is_empty)
                        && !self.promoted_recovery_file_is_complete(job_id, file_index)
                        && probe_may_arrive
                    {
                        continue;
                    }
                    if set_ids.is_empty() {
                        self.ensure_par2_runtime(job_id)
                            .files
                            .entry(file_index)
                            .or_default()
                            .discovery = Par2DiscoveryState::ProbeInconclusive;
                    } else {
                        self.note_foreign_recovery_set_sightings(job_id, file_index, &set_ids);
                        self.ensure_par2_runtime(job_id)
                            .files
                            .entry(file_index)
                            .or_default()
                            .discovery = Par2DiscoveryState::PrefixProbed { set_ids };
                    }
                }
                Par2DiscoveryState::MetadataCarrierQueued {
                    target_set_id,
                    set_ids,
                } => {
                    let carrier_may_arrive = self.jobs.get(&job_id).is_some_and(|state| {
                        let file_id = NzbFileId { job_id, file_index };
                        let has_segment = |ordinal| {
                            state
                                .assembly
                                .file(file_id)
                                .is_some_and(|file| file.has_segment(ordinal))
                        };
                        state
                            .spec
                            .files
                            .get(file_index as usize)
                            .is_some_and(|file| {
                                file.segments.iter().any(|segment| {
                                    !self.unavailable_promoted_recovery_segments.contains(
                                        &SegmentId {
                                            file_id,
                                            segment_number: segment.ordinal,
                                        },
                                    ) && !has_segment(segment.ordinal)
                                })
                            })
                    });
                    if !self.promoted_recovery_file_is_complete(job_id, file_index)
                        && carrier_may_arrive
                    {
                        continue;
                    }
                    if let Some(target_set_id) = target_set_id {
                        self.ensure_par2_runtime(job_id)
                            .files
                            .entry(file_index)
                            .or_default()
                            .metadata_targets_attempted
                            .insert(target_set_id);
                    }
                    self.ensure_par2_runtime(job_id)
                        .files
                        .entry(file_index)
                        .or_default()
                        .metadata_targets_attempted
                        .extend(set_ids.iter().copied());
                    self.ensure_par2_runtime(job_id)
                        .files
                        .entry(file_index)
                        .or_default()
                        .discovery = Par2DiscoveryState::Exhausted { set_ids };
                }
                _ => {}
            }
        }
    }

    /// Select the next bounded discovery action as
    /// `(file_index, prefix_only, target_set_id)`.
    pub(in crate::pipeline) fn next_par2_metadata_action(
        &self,
        job_id: JobId,
    ) -> Option<(u32, bool, Option<par2_rs::RecoverySetId>)> {
        let candidates = self.par2_metadata_candidate_indices(job_id);
        let discovery_for =
            |file_index: u32| self.par2_discovery_state_for_candidate(job_id, file_index);
        let collection_key_for = |file_index: u32| {
            self.jobs
                .get(&job_id)
                .and_then(|state| state.spec.files.get(file_index as usize))
                .and_then(|file| par2_set_base_name(&file.filename))
                .map(|base_name| format!("name:{base_name}"))
                // Obfuscated names do not identify siblings. Keep those
                // carriers separate until authenticated metadata does.
                .unwrap_or_else(|| format!("file:{file_index}"))
        };
        let mut collections = HashMap::<String, Vec<(u32, bool, u64)>>::new();
        for candidate in candidates {
            collections
                .entry(collection_key_for(candidate.0))
                .or_default()
                .push(candidate);
        }

        let metadata_is_installed = |set_id| {
            self.par2_runtime(job_id)
                .and_then(|runtime| runtime.set_runtime(set_id))
                .is_some_and(|set_runtime| set_runtime.set.is_some())
        };
        let collection_is_resolved = |members: &Vec<(u32, bool, u64)>| {
            let observed_set_ids = members
                .iter()
                .flat_map(|(file_index, _, _)| {
                    discovery_for(*file_index).observed_set_ids().to_vec()
                })
                .collect::<HashSet<_>>();
            !observed_set_ids.is_empty()
                && observed_set_ids
                    .iter()
                    .all(|set_id| metadata_is_installed(*set_id))
        };

        let mut actions = Vec::new();
        for members in collections.values() {
            if collection_is_resolved(members) {
                continue;
            }

            // An explicit index is the cheapest authoritative bootstrap. One
            // carrier at a time prevents a collection from draining every
            // sibling before its first result is parsed.
            if let Some((file_index, _, total_bytes)) = members
                .iter()
                .filter(|(file_index, is_index, _)| {
                    *is_index && matches!(discovery_for(*file_index), Par2DiscoveryState::Unseen)
                })
                .min_by_key(|(file_index, _, total_bytes)| (*total_bytes, *file_index))
            {
                actions.push((0_u8, *total_bytes, *file_index, false, None));
                continue;
            }

            // A prefix that has authenticated a SetID selects only the
            // cheapest carrier for that unresolved set. Other recovery
            // volumes remain cold unless this attempt fails.
            let metadata_carrier = members
                .iter()
                .flat_map(|(file_index, _, total_bytes)| {
                    let discovery = discovery_for(*file_index);
                    discovery
                        .observed_set_ids()
                        .to_vec()
                        .into_iter()
                        .filter_map(move |set_id| {
                            let attempted = self
                                .par2_runtime(job_id)
                                .and_then(|runtime| runtime.files.get(file_index))
                                .is_some_and(|file| {
                                    file.metadata_targets_attempted.contains(&set_id)
                                });
                            (!metadata_is_installed(set_id) && !attempted).then_some((
                                *total_bytes,
                                *file_index,
                                set_id,
                            ))
                        })
                })
                .min_by_key(|(total_bytes, file_index, set_id)| {
                    (*total_bytes, *file_index, *set_id.as_bytes())
                });
            if let Some((total_bytes, file_index, set_id)) = metadata_carrier {
                actions.push((1, total_bytes, file_index, false, Some(set_id)));
                continue;
            }

            // A single indexless candidate gets the bounded prefix path.
            // Only after it is exhausted can a sibling take its place.
            if let Some((file_index, _, total_bytes)) = members
                .iter()
                .filter(|(file_index, is_index, _)| {
                    !*is_index
                        && matches!(
                            discovery_for(*file_index),
                            Par2DiscoveryState::ProbeInconclusive
                        )
                })
                .min_by_key(|(file_index, _, total_bytes)| (*total_bytes, *file_index))
            {
                let prefix_len = self
                    .file_prefix_16k
                    .get(&NzbFileId {
                        job_id,
                        file_index: *file_index,
                    })
                    .map_or(0, Vec::len);
                actions.push((
                    2,
                    *total_bytes,
                    *file_index,
                    prefix_len < PAR2_METADATA_PREFIX_CAP_BYTES,
                    None,
                ));
                continue;
            }

            if let Some((file_index, _, total_bytes)) = members
                .iter()
                .filter(|(file_index, is_index, _)| {
                    !*is_index && matches!(discovery_for(*file_index), Par2DiscoveryState::Unseen)
                })
                .min_by_key(|(file_index, _, total_bytes)| (*total_bytes, *file_index))
            {
                actions.push((3, *total_bytes, *file_index, true, None));
            }
        }

        actions
            .into_iter()
            .min_by_key(|(stage, total_bytes, file_index, _, _)| {
                (*stage, *file_index, *total_bytes)
            })
            .map(|(_, _, file_index, prefix_only, target_set_id)| {
                (file_index, prefix_only, target_set_id)
            })
    }

    pub(super) fn queue_par2_metadata_action(
        &mut self,
        job_id: JobId,
        file_index: u32,
        prefix_only: bool,
        target_set_id: Option<par2_rs::RecoverySetId>,
    ) -> bool {
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state
                .download_queue
                .extract_matching(|work| work.segment_id.file_id.file_index == file_index);
            state
                .recovery_queue
                .extract_matching(|work| work.segment_id.file_id.file_index == file_index);
        } else {
            return false;
        }

        let already_probed = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&file_index))
            .map(|file| file.discovery_probe_ordinals.clone())
            .unwrap_or_default();
        let unavailable = &self.unavailable_promoted_recovery_segments;
        let (filename, work) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return false;
            };
            let Some(file) = state.spec.files.get(file_index as usize) else {
                return false;
            };
            let (priority, is_recovery) = if matches!(
                file.role,
                weaver_model::files::FileRole::Par2 { is_index: true, .. }
            ) {
                (file.role.download_priority(), false)
            } else {
                (PROMOTED_RECOVERY_PRIORITY, true)
            };
            let mut segments = file
                .segments
                .iter()
                .filter(|segment| {
                    let segment_id = SegmentId {
                        file_id: NzbFileId { job_id, file_index },
                        segment_number: segment.ordinal,
                    };
                    !unavailable.contains(&segment_id) && !already_probed.contains(&segment.ordinal)
                })
                .collect::<Vec<_>>();
            segments.sort_by_key(|segment| segment.ordinal);
            if prefix_only {
                let assembly = state.assembly.file(NzbFileId { job_id, file_index });
                // The capture can grow only from byte zero. If filtering left
                // an article above the lowest missing ordinal, the hole below
                // it is terminal and this optional carrier is exhausted.
                let frontier = file
                    .segments
                    .iter()
                    .map(|segment| segment.ordinal)
                    .filter(|ordinal| assembly.is_none_or(|file| !file.has_segment(*ordinal)))
                    .min();
                segments.truncate(1);
                segments.retain(|segment| Some(segment.ordinal) == frontier);
            }
            let work = segments
                .into_iter()
                .map(|segment| DownloadWork {
                    segment_id: SegmentId {
                        file_id: NzbFileId { job_id, file_index },
                        segment_number: segment.ordinal,
                    },
                    message_id: crate::jobs::ids::MessageId::new(&segment.message_id),
                    groups: std::sync::Arc::from(file.groups.as_slice()),
                    priority,
                    byte_estimate: segment.bytes,
                    retry_count: 0,
                    is_recovery,
                    completion_critical: true,
                    exclude_servers: Vec::new(),
                    avoid_server: None,
                })
                .collect::<Vec<_>>();
            (file.filename.clone(), work)
        };

        let promoted_segments = work.len();
        let probe_ordinal = prefix_only
            .then(|| work.first().map(|work| work.segment_id.segment_number))
            .flatten();
        if let Some(state) = self.jobs.get_mut(&job_id) {
            for work in work {
                state.download_queue.push(work);
            }
        }

        let runtime = self.ensure_par2_runtime(job_id);
        let file = runtime.files.entry(file_index).or_default();
        file.filename = filename.clone();
        file.recovery_blocks = 0;
        if prefix_only {
            if let Some(ordinal) = probe_ordinal {
                file.discovery_probe_ordinals.insert(ordinal);
            }
            file.metadata_carrier_completion_critical = false;
            file.discovery = Par2DiscoveryState::PrefixProbeQueued;
        } else {
            file.promoted = true;
            file.metadata_carrier_completion_critical = true;
            let set_ids = file.discovery.observed_set_ids().to_vec();
            file.discovery = Par2DiscoveryState::MetadataCarrierQueued {
                target_set_id,
                set_ids,
            };
        }

        if promoted_segments == 0 {
            let set_ids = file.discovery.observed_set_ids().to_vec();
            if let Par2DiscoveryState::MetadataCarrierQueued {
                target_set_id: Some(target_set_id),
                ..
            } = &file.discovery
            {
                file.metadata_targets_attempted.insert(*target_set_id);
            }
            file.metadata_targets_attempted
                .extend(set_ids.iter().copied());
            file.discovery = Par2DiscoveryState::Exhausted { set_ids };
            return false;
        }
        info!(
            job_id = job_id.0,
            file_index,
            filename = %filename,
            promoted_segments,
            prefix_only,
            target_set_id = ?target_set_id,
            "queued PAR2 metadata discovery work"
        );
        self.update_queue_metrics();
        true
    }

    pub(super) fn rearm_prefix_probe_from_recovery_queue(&mut self, job_id: JobId) -> bool {
        let mut probe = None;
        for (file_index, _, _) in self.par2_metadata_candidate_indices(job_id) {
            let Some(file) = self
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&file_index))
            else {
                continue;
            };
            if !matches!(&file.discovery, Par2DiscoveryState::PrefixProbeQueued)
                || file.promoted
                || self.promoted_recovery_file_has_pending_work(job_id, file_index)
            {
                continue;
            }
            // Prefix probing chooses the lowest untried ordinal, so the most
            // recently recorded ordinal is the one this queued state owns.
            let Some(ordinal) = file.discovery_probe_ordinals.iter().max().copied() else {
                continue;
            };
            let segment_id = SegmentId {
                file_id: NzbFileId { job_id, file_index },
                segment_number: ordinal,
            };
            if self
                .unavailable_promoted_recovery_segments
                .contains(&segment_id)
            {
                continue;
            }
            probe = Some(segment_id);
            break;
        }

        let Some(segment_id) = probe else {
            return false;
        };
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return false;
        };
        let mut queued = state
            .recovery_queue
            .extract_matching(|work| work.segment_id == segment_id);
        let Some(mut work) = queued.pop() else {
            return false;
        };
        for duplicate in queued {
            state.recovery_queue.push(duplicate);
        }
        work.completion_critical = true;
        state.download_queue.push(work);
        self.update_queue_metrics();
        true
    }

    /// Put the next finite metadata probe or set-specific carrier on the wire.
    /// Discovery continues even after one usable set exists.
    pub(crate) fn promote_par2_metadata(&mut self, job_id: JobId) -> bool {
        if self.rearm_prefix_probe_from_recovery_queue(job_id) {
            return true;
        }
        self.refresh_par2_metadata_discovery(job_id);
        if self
            .par2_metadata_candidate_indices(job_id)
            .iter()
            .any(|(file_index, _, _)| {
                self.par2_runtime(job_id)
                    .and_then(|runtime| runtime.files.get(file_index))
                    .is_some_and(|file| file.discovery.work_is_queued())
            })
        {
            return true;
        }

        while let Some((file_index, prefix_only, target_set_id)) =
            self.next_par2_metadata_action(job_id)
        {
            if self.queue_par2_metadata_action(job_id, file_index, prefix_only, target_set_id) {
                return true;
            }
        }

        let exhausted = self
            .par2_runtime(job_id)
            .map(|runtime| {
                let mut exhausted = runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| {
                        file.discovery
                            .candidate_probe_is_terminal()
                            .then_some(file_index)
                    })
                    .collect::<Vec<_>>();
                exhausted.sort_unstable();
                exhausted
            })
            .unwrap_or_default();
        let should_warn = {
            let runtime = self.ensure_par2_runtime(job_id);
            let should_warn = !runtime.metadata_exhausted_warned;
            runtime.metadata_exhausted_warned = true;
            should_warn
        };
        if should_warn {
            warn!(
                job_id = job_id.0,
                exhausted_candidates = ?exhausted,
                "PAR2 metadata discovery exhausted every declared candidate"
            );
        }
        self.warn_unservable_recovery_sets_once(job_id);
        false
    }

    /// Promote the smallest byte set of recovery files needed to cover the requested block count.
    ///
    /// Returns the number of recovery blocks newly promoted by this call.
    pub(crate) fn promote_recovery_targeted(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        blocks_needed: u32,
    ) -> u32 {
        let already_available_blocks = self.recovery_blocks_available_or_targeted(job_id, set_id);
        let remaining_needed = blocks_needed.saturating_sub(already_available_blocks);
        if remaining_needed == 0 {
            return 0;
        }

        // Candidates come from every pool an un-promoted recovery segment can
        // sit in, not just the parked one. Parking is progressive — segments
        // move to `recovery_queue` at build, on retry, on health re-routing —
        // so at any given promotion pass some volumes' work is still in the
        // ordinary download queue. Selecting only from the parked pool made a
        // wave promote whatever happened to be parked (job 10020: 15 of 30
        // available blocks, with the one volume that could cover the damage
        // invisible), and every later pass promoted nothing while the job
        // waited for blocks it had never asked for.
        let promoted_files_now: std::collections::HashSet<u32> = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| file.promoted.then_some(file_index))
                    .collect()
            })
            .unwrap_or_default();
        let queued = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return 0;
            };
            let mut pool = state.recovery_queue.drain_all();
            pool.extend(state.download_queue.extract_matching(|work| {
                work.is_recovery
                    && !promoted_files_now.contains(&work.segment_id.file_id.file_index)
            }));
            pool
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
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&file_index))
                .is_some_and(|file| file.promoted)
            {
                continue;
            }
            // Fetching another set's volume spends bandwidth on blocks that
            // cannot enter this repair's equation. Its work stays parked below
            // rather than being dropped.
            if !self.recovery_file_serves_set(job_id, file_index, set_id)
                && !self.unread_recovery_file_is_named_for_set(job_id, file_index, set_id)
            {
                continue;
            }
            if let Some(candidate) = self.recovery_candidate_for(job_id, file_index, set_id)
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
                        work.completion_critical = true;
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
            let filenames: HashMap<u32, String> = self
                .jobs
                .get(&job_id)
                .map(|state| {
                    promoted_file_indices
                        .iter()
                        .filter_map(|file_index| {
                            state
                                .spec
                                .files
                                .get(*file_index as usize)
                                .map(|file| (*file_index, file.filename.clone()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            for file_index in &promoted_file_indices {
                let (filename, recovery_blocks) = {
                    let runtime = self.ensure_par2_runtime(job_id);
                    let entry = runtime.files.entry(*file_index).or_default();
                    if let Some(filename) = filenames.get(file_index) {
                        entry.filename = filename.clone();
                    }
                    entry.recovery_blocks = block_map.get(file_index).copied().unwrap_or(0);
                    entry.promoted = true;
                    (entry.filename.clone(), entry.recovery_blocks)
                };
                let _ = (filename, recovery_blocks);
            }
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

    pub(crate) fn reapply_promoted_recovery_queue(&mut self, job_id: JobId) -> usize {
        let promoted: HashSet<u32> = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| file.promoted.then_some(file_index))
                    .collect()
            })
            .unwrap_or_default();
        if promoted.is_empty() {
            return 0;
        }

        let Some(state) = self.jobs.get_mut(&job_id) else {
            return 0;
        };

        let queued = state.recovery_queue.drain_all();
        let mut moved_segments = 0usize;
        let mut moved_files = HashSet::new();
        for mut work in queued {
            let file_index = work.segment_id.file_id.file_index;
            if promoted.contains(&file_index) {
                work.priority = PROMOTED_RECOVERY_PRIORITY;
                work.completion_critical = true;
                state.download_queue.push(work);
                moved_segments += 1;
                moved_files.insert(file_index);
            } else {
                state.recovery_queue.push(work);
            }
        }

        if moved_segments > 0 {
            info!(
                job_id = job_id.0,
                moved_segments,
                moved_files = ?moved_files,
                "reapplied promoted PAR2 recovery queue state after restore"
            );
            self.update_queue_metrics();
        }

        moved_segments
    }

    /// List all jobs.
    pub(crate) fn list_jobs(&self) -> Vec<JobInfo> {
        let mut list = Vec::with_capacity(self.jobs.len() + self.finished_jobs.len());
        let mut seen = HashSet::with_capacity(self.jobs.len());
        let jobs_fetching_repair_data = self.jobs_fetching_repair_data();

        let mut push_state = |state: &JobState| {
            let total = state.spec.total_bytes;
            let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
                state.assembly.optional_recovery_bytes();
            let health = health_milli(total, state.failed_bytes);
            let native_verifying = self.show_par3_verification_wait(state.job_id);
            let status = if native_verifying {
                JobStatus::Verifying
            } else {
                state.status.clone()
            };
            let (mut download_state, post_state, run_state) =
                crate::jobs::model::runtime_lanes_from_status_snapshot(&status);
            let has_current_download_activity =
                self.job_has_current_download_activity(state.job_id);
            if matches!(download_state, crate::jobs::model::DownloadState::Complete)
                && has_current_download_activity
            {
                download_state = crate::jobs::model::DownloadState::Downloading;
            } else if matches!(status, JobStatus::Downloading) && !has_current_download_activity {
                download_state = crate::jobs::model::DownloadState::Queued;
            }
            let remaining_par_files = state
                .assembly
                .files()
                .filter(|file| {
                    matches!(
                        file.role(),
                        weaver_model::files::FileRole::Par2 {
                            is_index: false,
                            ..
                        }
                    ) && !file.is_complete()
                })
                .count() as u32;
            let download_wait = self.download_wait_by_job.get(&state.job_id);
            let propagation_retry_at = self
                .propagation_ready_at
                .get(&state.job_id)
                .filter(|(deadline, _)| *deadline > Instant::now())
                .filter(|_| matches!(state.status, JobStatus::Queued | JobStatus::Downloading))
                .map(|(_, retry_at_epoch_ms)| *retry_at_epoch_ms as f64);
            if propagation_retry_at.is_some() {
                download_state = crate::jobs::model::DownloadState::Queued;
            }
            list.push(JobInfo {
                job_id: state.job_id,
                job_hash: Some(state.job_hash),
                name: state.spec.name.clone(),
                error: if let JobStatus::Failed { error } = &state.status {
                    Some(error.clone())
                } else {
                    None
                },
                download_wait_reason: propagation_retry_at
                    .map(|_| crate::jobs::handle::PROPAGATION_WAIT_REASON.to_owned())
                    .or_else(|| download_wait.map(|wait| wait.reason.to_owned())),
                download_retry_at_epoch_ms: propagation_retry_at
                    .or_else(|| download_wait.and_then(|wait| wait.retry_at_epoch_ms)),
                status,
                download_state,
                finalizing_download: !native_verifying
                    && self.jobs_finalizing_download.contains(&state.job_id),
                fetching_repair_data: !native_verifying
                    && jobs_fetching_repair_data.contains(&state.job_id),
                post_state,
                run_state,
                progress: Self::effective_progress(state),
                total_bytes: total,
                downloaded_bytes: Self::effective_downloaded_bytes(state),
                optional_recovery_bytes,
                optional_recovery_downloaded_bytes,
                phase_progress: self
                    .phase_progress_snapshots
                    .get(&state.job_id)
                    .cloned()
                    .unwrap_or_default(),
                failed_bytes: state.failed_bytes,
                health,
                terminal_discards: Vec::new(),
                total_files: state.assembly.total_file_count() as u32,
                completed_files: state.assembly.complete_file_count() as u32,
                remaining_par_files,
                password: state.spec.password.clone(),
                category: state.spec.category.clone(),
                metadata: state.spec.metadata.clone(),
                output_dir: Some(state.working_dir.display().to_string()),
                created_at_epoch_ms: state.created_at_epoch_ms,
            });
        };

        for job_id in &self.job_order {
            let Some(state) = self.jobs.get(job_id) else {
                continue;
            };
            if is_terminal_status(&state.status) || !seen.insert(*job_id) {
                continue;
            }
            push_state(state);
        }

        let mut unordered: Vec<&JobState> = self
            .jobs
            .values()
            .filter(|state| !is_terminal_status(&state.status) && !seen.contains(&state.job_id))
            .collect();
        unordered.sort_by(|left, right| {
            left.created_at_epoch_ms
                .total_cmp(&right.created_at_epoch_ms)
        });
        for state in unordered {
            push_state(state);
        }

        list.extend(self.finished_jobs.iter().cloned());
        list
    }
}
