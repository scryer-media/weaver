//! Continuation of the `impl Pipeline` block from `repair/par2.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl Pipeline {
    /// Read a completed PAR2 metadata candidate from disk. Recovery volumes
    /// repeat enough critical packets to be valid metadata carriers, so an
    /// indexless posting must not require an index-looking filename here.
    pub(crate) async fn try_load_par2_metadata(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (filename, file_path) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                return;
            };
            let declared_par2 =
                matches!(file_asm.role(), weaver_model::files::FileRole::Par2 { .. });
            let signature_candidate = self
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&file_id.file_index))
                .is_some_and(|file| file.signature_candidate);
            if !(declared_par2 || signature_candidate) || !file_asm.is_complete() {
                return;
            }
            let filename = self.current_filename_for_file(job_id, file_asm);
            let file_path = state.working_dir.join(&filename);
            (filename, file_path)
        };
        let parse_path = file_path.clone();
        let budget = self.par2_scan_budget(job_id);
        let parsed = match tokio::task::spawn_blocking(move || {
            scan_completed_par2_packet_groups(&parse_path, &budget)
        })
        .await
        {
            Ok(Ok(parsed)) => parsed,
            Ok(Err(error @ par2_rs::Par2Error::ResourceLimitExceeded { .. })) => {
                self.fail_job(job_id, error.to_string());
                return;
            }
            Ok(Err(e)) => {
                warn!(filename = %filename, error = %e, "failed to parse PAR2 metadata candidate");
                let entry = self
                    .ensure_par2_runtime(job_id)
                    .files
                    .entry(file_id.file_index)
                    .or_default();
                let set_ids = entry.discovery.observed_set_ids().to_vec();
                if let Par2DiscoveryState::MetadataCarrierQueued {
                    target_set_id: Some(target_set_id),
                    ..
                } = &entry.discovery
                {
                    entry.metadata_targets_attempted.insert(*target_set_id);
                }
                entry
                    .metadata_targets_attempted
                    .extend(set_ids.iter().copied());
                entry.recovery_capacity_accounted = true;
                entry.discovery = Par2DiscoveryState::Exhausted { set_ids };
                return;
            }
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to join PAR2 metadata parse task");
                let entry = self
                    .ensure_par2_runtime(job_id)
                    .files
                    .entry(file_id.file_index)
                    .or_default();
                let set_ids = entry.discovery.observed_set_ids().to_vec();
                if let Par2DiscoveryState::MetadataCarrierQueued {
                    target_set_id: Some(target_set_id),
                    ..
                } = &entry.discovery
                {
                    entry.metadata_targets_attempted.insert(*target_set_id);
                }
                entry
                    .metadata_targets_attempted
                    .extend(set_ids.iter().copied());
                entry.recovery_capacity_accounted = true;
                entry.discovery = Par2DiscoveryState::Exhausted { set_ids };
                return;
            }
        };

        let observed_set_ids = parsed.iter().map(|group| group.set_id).collect::<Vec<_>>();
        if !self.admit_par2_set_ids(job_id, &observed_set_ids) {
            return;
        }
        self.note_foreign_recovery_set_sightings(job_id, file_id.file_index, &observed_set_ids);
        let mut accepted_recovery_blocks = HashMap::new();

        for group in parsed {
            let set_id = group.set_id;
            let already_installed = self
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.set_runtime(set_id))
                .is_some_and(|set_runtime| set_runtime.set.is_some());
            let (par2_set, new_recovery_blocks) = if already_installed {
                let merge = self
                    .ensure_par2_runtime(job_id)
                    .set_runtime_mut(set_id)
                    .and_then(|set_runtime| set_runtime.set.as_mut())
                    .map(|set| Arc::make_mut(set).merge_packets(group.packets));
                match merge {
                    Some(Ok(result)) => {
                        info!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            recovery_blocks_merged = result.new_recovery_slices,
                            "merged PAR2 metadata into an existing recovery set"
                        );
                        (
                            self.par2_set_for(job_id, set_id).cloned(),
                            result.new_recovery_slices,
                        )
                    }
                    Some(Err(error)) => {
                        if matches!(error, par2_rs::Par2Error::ResourceLimitExceeded { .. }) {
                            self.fail_job(job_id, error.to_string());
                            return;
                        }
                        warn!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            error = %error,
                            "failed to merge PAR2 metadata into an existing recovery set"
                        );
                        continue;
                    }
                    None => continue,
                }
            } else {
                match par2_rs::Par2FileSet::from_packets(group.packets) {
                    Ok(set) => {
                        let recovery_blocks = set.recovery_block_count();
                        (Some(Arc::new(set)), recovery_blocks)
                    }
                    Err(error @ par2_rs::Par2Error::ResourceLimitExceeded { .. }) => {
                        self.fail_job(job_id, error.to_string());
                        return;
                    }
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            error = %error,
                            "failed to build PAR2 recovery set from metadata"
                        );
                        (None, 0)
                    }
                }
            };
            let Some(par2_set) = par2_set else {
                continue;
            };
            if !self.admit_par2_geometry(job_id, &par2_set) {
                return;
            }
            accepted_recovery_blocks.insert(set_id, new_recovery_blocks);

            if let Err(error) = self
                .apply_par2_authoritative_identity(job_id, par2_set.as_ref())
                .await
            {
                warn!(
                    job_id = job_id.0,
                    recovery_set_id = %set_id,
                    error = %error,
                    "failed to apply authoritative PAR2 file identity"
                );
            }

            self.record_par2_set_summary(job_id, par2_set.as_ref(), &filename, file_id.file_index);
            let set_runtime = self.ensure_par2_runtime(job_id).ensure_set_runtime(set_id);
            if set_runtime.set.is_none() {
                set_runtime.set = Some(par2_set);
            }
            self.refresh_par2_checkpoint_plan(job_id);
            // The session snapshot is now a set behind. A set waiting to repair
            // gets the volume handed to its session instead, which keeps the
            // analysis it is about to repair on; every other set rebuilds, as
            // it always did.
            if !self
                .merge_recovery_into_retained_par2_session(job_id, set_id, file_path.clone())
                .await
            {
                self.evict_par2_repair_session(job_id, set_id);
            }
        }

        {
            let runtime = self.ensure_par2_runtime(job_id);
            let entry = runtime.files.entry(file_id.file_index).or_default();
            entry.filename = filename.clone();
            for set_id in &observed_set_ids {
                let accepted = accepted_recovery_blocks.remove(set_id).unwrap_or(0);
                let blocks = entry.recovery_blocks_by_set.entry(*set_id).or_insert(0);
                *blocks = blocks.saturating_add(accepted);
                if observed_set_ids.as_slice() == [*set_id] {
                    entry.validated_recovery_blocks = *blocks;
                }
            }
            entry.recovery_blocks = observed_set_ids
                .first()
                .and_then(|set_id| entry.recovery_blocks_by_set.get(set_id))
                .copied()
                .unwrap_or(0);
            entry.recovery_capacity_accounted = true;
            if let Par2DiscoveryState::MetadataCarrierQueued {
                target_set_id: Some(target_set_id),
                ..
            } = &entry.discovery
            {
                entry.metadata_targets_attempted.insert(*target_set_id);
            }
            entry
                .metadata_targets_attempted
                .extend(observed_set_ids.iter().copied());
            let mut set_ids = entry.discovery.observed_set_ids().to_vec();
            set_ids.extend(observed_set_ids.iter().copied());
            set_ids.sort_by_key(|set_id| *set_id.as_bytes());
            set_ids.dedup();
            entry
                .metadata_targets_attempted
                .extend(set_ids.iter().copied());
            entry.discovery = if observed_set_ids.is_empty() {
                Par2DiscoveryState::Exhausted { set_ids }
            } else {
                Par2DiscoveryState::Parsed { set_ids }
            };
        }

        // A newly parsed index can expose a set that was not part of an
        // earlier aggregate.  Recompute before choosing the compatibility view
        // so that the old set keeps its settled verdict while the new one is
        // queued for its own pass.
        self.mark_par2_verified(job_id).await;

        // The compatibility view still starts from deterministic metadata
        // selection.  The completion gate replaces it with the earliest
        // unsettled set before every pass, so this cannot re-judge a set that
        // already settled when a later index appears.
        let selected = (!self.par2_verified.contains(&job_id))
            .then(|| self.select_primary_recovery_set(job_id))
            .flatten();
        if let Some(set_id) = selected
            && self.par2_served_set_id(job_id) != Some(set_id)
        {
            self.install_primary_recovery_set(job_id, set_id).await;
        }
        for set_id in observed_set_ids.iter().copied() {
            self.install_recovery_set(job_id, set_id).await;
        }
        self.refresh_par2_md5_substitution_bindings(job_id);

        // The descriptions just parsed are the only place an obfuscated post's
        // real volume names exist, so this is the moment direct-store can arm
        // admission for the sets the spec's filenames could not name.
        self.arm_direct_identity_admission(job_id).await;

        let _ = self
            .event_tx
            .send(PipelineEvent::Par2MetadataLoaded { job_id });
    }

    /// Select the compatibility view used by legacy single-set helpers.
    /// Per-set sessions and byte-derived grid evidence survive this view change;
    /// only an actual identity or byte mutation may retire them.
    pub(super) async fn install_primary_recovery_set(
        &mut self,
        job_id: JobId,
        new_set_id: par2_rs::RecoverySetId,
    ) {
        {
            let runtime = self.ensure_par2_runtime(job_id);
            runtime.served = Some(new_set_id);
            runtime.unserved_sets_warned = false;
        }

        self.install_recovery_set(job_id, new_set_id).await;
    }

    /// Replay completed recovery volumes after a set's index becomes usable.
    pub(super) async fn install_recovery_set(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) {
        if self.par2_set_for(job_id, set_id).is_none() {
            return;
        }

        let volumes: Vec<NzbFileId> = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .filter(|file| {
                        file.is_complete()
                            && matches!(
                                file.role(),
                                weaver_model::files::FileRole::Par2 {
                                    is_index: false,
                                    ..
                                }
                            )
                            && self.recovery_file_serves_set(
                                job_id,
                                file.file_id().file_index,
                                set_id,
                            )
                    })
                    .map(|file| file.file_id())
                    .collect()
            })
            .unwrap_or_default();
        for file_id in volumes {
            self.try_merge_par2_recovery(job_id, file_id).await;
        }
    }

    /// Record what a parsed set describes, so it can be weighed against the
    /// others and its files recognized later.
    pub(super) fn record_par2_set_summary(
        &mut self,
        job_id: JobId,
        par2_set: &Par2FileSet,
        index_filename: &str,
        index_file_index: u32,
    ) {
        let described_bytes = par2_set
            .recovery_file_ids
            .iter()
            .filter_map(|file_id| par2_set.files.get(file_id))
            .map(|desc| desc.length)
            .sum();
        let described_filenames = par2_set
            .recovery_file_ids
            .iter()
            .filter_map(|file_id| par2_set.files.get(file_id))
            .map(|desc| sanitize_download_filename(&desc.filename))
            .collect();
        let base_name = par2_set_base_name(index_filename);
        let set_id = par2_set.recovery_set_id;
        let candidate_is_index = self.jobs.get(&job_id).is_some_and(|state| {
            matches!(
                state
                    .spec
                    .files
                    .get(index_file_index as usize)
                    .map(|file| &file.role),
                Some(weaver_model::files::FileRole::Par2 { is_index: true, .. })
            )
        });
        let current_index_file_index = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.set_runtime(set_id))
            .filter(|set_runtime| set_runtime.summary.describes)
            .map(|set_runtime| set_runtime.summary.index_file_index);
        let current_is_index = current_index_file_index.is_some_and(|file_index| {
            self.jobs.get(&job_id).is_some_and(|state| {
                matches!(
                    state
                        .spec
                        .files
                        .get(file_index as usize)
                        .map(|file| &file.role),
                    Some(weaver_model::files::FileRole::Par2 { is_index: true, .. })
                )
            })
        });

        let runtime = self.ensure_par2_runtime(job_id);
        let newly_known = runtime.set_runtime(set_id).is_none();
        let summary = &mut runtime.ensure_set_runtime(set_id).summary;
        let replaces_summary = !summary.describes
            || (candidate_is_index && !current_is_index)
            || (candidate_is_index == current_is_index
                && index_file_index < summary.index_file_index);
        if replaces_summary {
            summary.index_filename = index_filename.to_string();
            summary.index_file_index = index_file_index;
            summary.base_name = base_name;
            summary.described_filenames = described_filenames;
            summary.described_bytes = described_bytes;
        }
        summary.describes = true;
        summary.volume_file_indices.insert(index_file_index);
        if newly_known {
            runtime.unserved_sets_warned = false;
        }
    }

    /// Note every recovery set a PAR2 file's packets turned out to speak for.
    ///
    /// A set met only this way has no descriptions and can never be served — it
    /// is recorded so its volumes are attributed away from the served set and
    /// so the job can name it.
    pub(super) fn note_foreign_recovery_set_sightings(
        &mut self,
        job_id: JobId,
        file_index: u32,
        observed: &[par2_rs::RecoverySetId],
    ) {
        let runtime = self.ensure_par2_runtime(job_id);
        let mut newly_known = false;
        for set_id in observed {
            if runtime.set_runtime(*set_id).is_none() {
                newly_known = true;
            }
            runtime
                .ensure_set_runtime(*set_id)
                .summary
                .volume_file_indices
                .insert(file_index);
        }
        if newly_known {
            runtime.unserved_sets_warned = false;
        }

        // A file whose packets all answer to one set is that set's, whatever it
        // is named. A file carrying more than one is not attributable at all,
        // and is left to be grouped by name.
        let learned = match observed {
            [only] => Some(*only),
            _ => None,
        };
        let entry = runtime.files.entry(file_index).or_default();
        entry.recovery_set_packets_read = !observed.is_empty();
        entry.recovery_set_id = learned;
    }

    /// The recovery set worth serving: the one protecting the most payload,
    /// ties broken by position in the posting.
    ///
    /// Both keys are properties of the posting rather than of this run, so a
    /// job that is restarted, replayed from disk, or whose files arrive in a
    /// different order reaches the same answer every time. A set known only
    /// through somebody else's packets describes nothing and is not eligible.
    pub(super) fn select_primary_recovery_set(
        &self,
        job_id: JobId,
    ) -> Option<par2_rs::RecoverySetId> {
        let runtime = self.par2_runtime(job_id)?;
        runtime
            .sets
            .iter()
            .filter(|(_, set_runtime)| set_runtime.summary.describes)
            .max_by(|(_, left), (_, right)| {
                left.summary
                    .described_bytes
                    .cmp(&right.summary.described_bytes)
                    .then_with(|| {
                        right
                            .summary
                            .index_file_index
                            .cmp(&left.summary.index_file_index)
                    })
            })
            .map(|(set_id, _)| *set_id)
    }

    /// Say once, after bounded discovery is exhausted, that this posting
    /// carries recovery sets without enough critical metadata for a pass.
    ///
    /// The files those sets describe are still delivered; what is lost is the
    /// repair they were entitled to, and that is worth exactly one line naming
    /// every set and every file it covers. The caller invokes this only after
    /// every observed carrier has had its turn; the latch then avoids repeats.
    pub(in crate::pipeline) fn warn_unservable_recovery_sets_once(&mut self, job_id: JobId) {
        let Some(runtime) = self.par2_runtime(job_id) else {
            return;
        };
        if runtime.unserved_sets_warned {
            return;
        }

        let mut unservable: Vec<String> = runtime
            .ordered_set_ids()
            .into_iter()
            .filter_map(|set_id| {
                runtime
                    .set_runtime(set_id)
                    .filter(|set_runtime| !set_runtime.summary.describes)
                    .map(|set_runtime| (set_id, set_runtime))
            })
            .map(|(set_id, set_runtime)| {
                let summary = &set_runtime.summary;
                let name = if summary.index_filename.is_empty() {
                    set_id.to_string()
                } else {
                    summary.index_filename.clone()
                };
                if summary.described_filenames.is_empty() {
                    format!("{name} (critical metadata unavailable)")
                } else {
                    format!("{name} covering {}", summary.described_filenames.join(", "))
                }
            })
            .collect();
        if unservable.is_empty() {
            return;
        }
        unservable.sort();
        warn!(
            job_id = job_id.0,
            "this posting carries {} recovery set(s) whose metadata carriers were exhausted: \
             they cannot verify or repair the files they cover — {}",
            runtime.sets.len(),
            unservable.join("; ")
        );

        if let Some(runtime) = self.par2_runtime.get_mut(&job_id) {
            runtime.unserved_sets_warned = true;
        }
        #[cfg(test)]
        {
            self.par2_unserved_set_warnings += 1;
        }
    }

    /// Whether a PAR2 file's recovery blocks belong to one recovery set.
    ///
    /// Attribution is by validated packets. A job that has met fewer than two
    /// sets needs no attribution; once two sets exist, an unread filename is no
    /// evidence at all.
    pub(super) fn recovery_file_serves_set(
        &self,
        job_id: JobId,
        file_index: u32,
        set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let Some(runtime) = self.par2_runtime(job_id) else {
            return true;
        };
        if let Some(file) = runtime.files.get(&file_index) {
            // Blocks this file demonstrably gave the set outrank any attribution
            // question: they are merged, the repairer counts them, and capacity
            // that pretended otherwise could refuse a repair the job can afford.
            if file.recovery_blocks_by_set.contains_key(&set_id) {
                return true;
            }
            if let Some(learned) = file.recovery_set_id {
                return learned == set_id;
            }
            let observed = file.discovery.observed_set_ids();
            if !observed.is_empty() {
                return observed.contains(&set_id);
            }
            if file.recovery_set_packets_read {
                return false;
            }
        }
        if runtime.sets.len() < 2 {
            return true;
        }
        false
    }

    /// Whether an unread conventional recovery volume is named for a parsed set.
    ///
    /// This is a download-selection hint only. Packet evidence remains the sole
    /// authority for recovery ownership and capacity.
    pub(super) fn unread_recovery_file_is_named_for_set(
        &self,
        job_id: JobId,
        file_index: u32,
        set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(file) = state.spec.files.get(file_index as usize) else {
            return false;
        };
        if !matches!(
            file.role,
            weaver_model::files::FileRole::Par2 {
                is_index: false,
                ..
            }
        ) {
            return false;
        }
        let Some(runtime) = self.par2_runtime(job_id) else {
            return false;
        };
        if runtime.files.get(&file_index).is_some_and(|entry| {
            entry.recovery_set_packets_read
                || entry.recovery_set_id.is_some()
                || !entry.discovery.observed_set_ids().is_empty()
                || !entry.recovery_blocks_by_set.is_empty()
        }) {
            return false;
        }
        let Some(summary) = runtime
            .set_runtime(set_id)
            .filter(|set| set.summary.describes)
        else {
            return false;
        };
        let Some(base_name) = summary.summary.base_name.as_deref() else {
            return false;
        };
        par2_set_base_name(&file.filename).as_deref() == Some(base_name)
    }

    /// When a PAR2 recovery volume completes, parse it and merge recovery
    /// slices into the retained Par2FileSet (avoids re-reading at repair time).
    pub(crate) async fn try_merge_par2_recovery(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (filename, file_path, is_par2_volume, is_complete) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                return;
            };

            let is_par2_volume = matches!(
                file_asm.role(),
                weaver_model::files::FileRole::Par2 {
                    is_index: false,
                    ..
                }
            );
            let filename = self.current_filename_for_file(job_id, file_asm);
            let file_path = state.working_dir.join(&filename);
            (filename, file_path, is_par2_volume, file_asm.is_complete())
        };
        if !is_par2_volume || !is_complete {
            return;
        }

        let parse_path = file_path.clone();
        let budget = self.par2_scan_budget(job_id);
        let groups = match tokio::task::spawn_blocking(move || {
            scan_completed_par2_packet_groups(&parse_path, &budget)
        })
        .await
        {
            Ok(Ok(scanned)) => scanned,
            Ok(Err(error @ par2_rs::Par2Error::ResourceLimitExceeded { .. })) => {
                self.fail_job(job_id, error.to_string());
                return;
            }
            Ok(Err(e)) => {
                warn!(filename = %filename, error = %e, "failed to parse PAR2 recovery volume");
                self.ensure_par2_runtime(job_id)
                    .files
                    .entry(file_id.file_index)
                    .or_default()
                    .recovery_capacity_accounted = true;
                return;
            }
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to join PAR2 recovery parse task");
                self.ensure_par2_runtime(job_id)
                    .files
                    .entry(file_id.file_index)
                    .or_default()
                    .recovery_capacity_accounted = true;
                return;
            }
        };
        let observed_set_ids = groups.iter().map(|group| group.set_id).collect::<Vec<_>>();
        if !self.admit_par2_set_ids(job_id, &observed_set_ids) {
            return;
        }
        self.note_foreign_recovery_set_sightings(job_id, file_id.file_index, &observed_set_ids);
        {
            let entry = self
                .ensure_par2_runtime(job_id)
                .files
                .entry(file_id.file_index)
                .or_default();
            entry.filename = filename.clone();
            entry.recovery_capacity_accounted = true;
            for set_id in &observed_set_ids {
                entry.recovery_blocks_by_set.entry(*set_id).or_insert(0);
            }
        }
        let single_set_id = match observed_set_ids.as_slice() {
            [set_id] => Some(*set_id),
            _ => None,
        };
        let mut bootstrapped_set_ids = Vec::new();

        for ParsedPar2Set {
            set_id,
            packets: packet_list,
        } in groups
        {
            let mut packet_list = Some(packet_list);
            let bootstrapped_recovery_blocks = if self.par2_set_for(job_id, set_id).is_none() {
                match par2_rs::Par2FileSet::from_packets(
                    packet_list
                        .take()
                        .expect("a PAR2 packet group is consumed once"),
                ) {
                    Ok(set) => {
                        if !self.admit_par2_geometry(job_id, &set) {
                            return;
                        }
                        let recovery_blocks = set.recovery_block_count();
                        let par2_set = Arc::new(set);
                        if let Err(error) = self
                            .apply_par2_authoritative_identity(job_id, par2_set.as_ref())
                            .await
                        {
                            warn!(
                                job_id = job_id.0,
                                filename = %filename,
                                recovery_set_id = %set_id,
                                error = %error,
                                "failed to apply authoritative PAR2 identity from recovery volume"
                            );
                        }
                        self.record_par2_set_summary(
                            job_id,
                            par2_set.as_ref(),
                            &filename,
                            file_id.file_index,
                        );
                        let set_runtime =
                            self.ensure_par2_runtime(job_id).ensure_set_runtime(set_id);
                        set_runtime.set = Some(par2_set);
                        self.refresh_par2_checkpoint_plan(job_id);
                        bootstrapped_set_ids.push(set_id);
                        Some(recovery_blocks)
                    }
                    Err(error @ par2_rs::Par2Error::ResourceLimitExceeded { .. }) => {
                        self.fail_job(job_id, error.to_string());
                        return;
                    }
                    Err(error) => {
                        // Recovery-slice-only volumes cannot describe a usable
                        // set. Without Main and file-description packets there
                        // is no safe identity or repair input to install.
                        warn!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            error = %error,
                            "recovery volume does not contain enough metadata to establish a PAR2 set"
                        );
                        continue;
                    }
                }
            } else {
                None
            };
            // A bootstrapped set is new, so nothing can have been built from
            // it yet; a merge that inserts nothing leaves everything built from
            // the set current.
            let mut set_changed = true;
            let (new_recovery_blocks, total_recovery) = if let Some(recovery_blocks) =
                bootstrapped_recovery_blocks
            {
                (recovery_blocks, recovery_blocks)
            } else {
                let merge_result = {
                    let par2_set = Arc::make_mut(
                        self.ensure_par2_runtime(job_id)
                            .set_runtime_mut(set_id)
                            .and_then(|set_runtime| set_runtime.set.as_mut())
                            .expect("parsed PAR2 recovery set exists"),
                    );
                    let before = par2_set_merge_shape(par2_set);
                    let merge = par2_set.merge_packets(
                        packet_list
                            .take()
                            .expect("unbootstrapped packets remain available to merge"),
                    );
                    let total_recovery = par2_set.recovery_block_count();
                    set_changed = par2_set_merge_shape(par2_set) != before;
                    (merge, total_recovery)
                };
                match merge_result {
                    (Ok(result), total_recovery) => (result.new_recovery_slices, total_recovery),
                    (Err(error @ par2_rs::Par2Error::ResourceLimitExceeded { .. }), _) => {
                        self.fail_job(job_id, error.to_string());
                        return;
                    }
                    (Err(error), _) => {
                        warn!(
                            job_id = job_id.0,
                            filename = %filename,
                            recovery_set_id = %set_id,
                            error = %error,
                            "failed to merge PAR2 recovery volume"
                        );
                        continue;
                    }
                }
            };
            if let Some(set) = self.par2_set_for(job_id, set_id).cloned()
                && !self.admit_par2_geometry(job_id, &set)
            {
                return;
            }
            // Volume replays re-offer packets the set already holds: every
            // file-complete pass parses a volume twice, once here and once as
            // metadata, so the arrival that hands a waiting set's session its
            // new recovery reaches this line with nothing left to give. Holding
            // the session through a merge that inserted nothing is what lets
            // that hand-off survive to the repair; every other set rebuilds on
            // any arrival, as it always did.
            if set_changed || !self.has_pending_par2_repair(job_id, set_id) {
                self.evict_par2_repair_session(job_id, set_id);
            }
            let promoted = self
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&file_id.file_index))
                .is_some_and(|file| file.promoted);
            let entry = self
                .ensure_par2_runtime(job_id)
                .files
                .entry(file_id.file_index)
                .or_default();
            let blocks = entry.recovery_blocks_by_set.entry(set_id).or_insert(0);
            *blocks = blocks.saturating_add(new_recovery_blocks);
            if single_set_id == Some(set_id) {
                entry.validated_recovery_blocks = *blocks;
                entry.recovery_blocks = *blocks;
                entry.salvaged = false;
                entry.salvaged_at_received_bytes = None;
                entry.promoted = promoted;
            }
            if new_recovery_blocks > 0 {
                info!(
                    job_id = job_id.0,
                    filename = %filename,
                    recovery_set_id = %set_id,
                    recovery_blocks_merged = new_recovery_blocks,
                    total_recovery,
                    "merged PAR2 recovery volume"
                );
            }
        }

        let bootstrapped_any = !bootstrapped_set_ids.is_empty();
        for set_id in bootstrapped_set_ids {
            Box::pin(self.install_recovery_set(job_id, set_id)).await;
        }
        if bootstrapped_any {
            self.refresh_par2_md5_substitution_bindings(job_id);
            if self.par2_served_set_id(job_id).is_none()
                && !self.par2_verified.contains(&job_id)
                && let Some(set_id) = self.select_primary_recovery_set(job_id)
            {
                Box::pin(self.install_primary_recovery_set(job_id, set_id)).await;
            }
            self.warn_unservable_recovery_sets_once(job_id);
        }
    }

    /// Read back the recovery packets that survived on every PAR2 volume of
    /// this job that can no longer complete.
    ///
    /// Recovery otherwise merges only on file *completion*, so a volume one
    /// article short of fifty contributed **zero** blocks to the arithmetic
    /// that decides whether a job is repairable — with its intact packets
    /// sitting on disk the whole time. Both reference downloaders read such a
    /// volume packet by packet instead of writing it off, and the PAR2 format
    /// is what makes that safe: every packet carries its own MD5, and the
    /// scanner resynchronises on the packet magic, so a hole costs the packets
    /// it lands on and nothing else.
    ///
    /// Only packets that validate are merged. An unvalidated merge would be
    /// worse than no merge at all: the set keys recovery slices by exponent and
    /// ignores repeats, so a packet read out of a hole would occupy its
    /// exponent permanently and a later good copy of the same block could never
    /// replace it.
    pub(in crate::pipeline) async fn salvage_partial_promoted_recovery_volumes(
        &mut self,
        job_id: JobId,
    ) {
        // A volume is read back once per generation of its bytes. Re-reading one
        // that has not moved is a slow path run on a hot loop; never re-reading
        // one that has taken more articles since is how a volume salvaged early
        // and short keeps reporting the short count for the rest of the job.
        let candidate_file_indices: Vec<u32> = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(
                        |(&file_index, file)| match file.salvaged_at_received_bytes {
                            None => Some(file_index),
                            Some(read_at_bytes) => (self
                                .recovery_file_received_bytes(job_id, file_index)
                                > read_at_bytes)
                                .then_some(file_index),
                        },
                    )
                    .collect()
            })
            .unwrap_or_default();

        let candidates: Vec<(u32, par2_rs::RecoverySetId)> = candidate_file_indices
            .into_iter()
            .flat_map(|file_index| {
                self.recovery_sets_for_unread_file(job_id, file_index)
                    .into_iter()
                    .map(move |set_id| (file_index, set_id))
            })
            .collect();

        for (file_index, expected_set_id) in candidates {
            if !self.recovery_volume_is_stranded(job_id, file_index) {
                continue;
            }
            self.salvage_stranded_recovery_volume(job_id, file_index, expected_set_id)
                .await;
        }
    }

    /// Return the parsed recovery sets an unread volume can belong to.
    ///
    /// A packet observation settles attribution, including the deliberate
    /// no-set result for a file that carries several recovery sets. Until then,
    /// the same filename fallback used by recovery arithmetic identifies the
    /// set to scan. A set without parsed metadata is never a candidate.
    pub(super) fn recovery_sets_for_unread_file(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> Vec<par2_rs::RecoverySetId> {
        let Some(runtime) = self.par2_runtime(job_id) else {
            return Vec::new();
        };
        let Some(file) = runtime.files.get(&file_index) else {
            return Vec::new();
        };
        if let Some(set_id) = file.recovery_set_id {
            return runtime
                .set_runtime(set_id)
                .is_some_and(|set_runtime| set_runtime.set.is_some())
                .then_some(set_id)
                .into_iter()
                .collect();
        }
        if file.recovery_set_packets_read {
            return Vec::new();
        }

        runtime
            .sets
            .iter()
            .filter_map(|(set_id, set_runtime)| {
                (set_runtime.set.is_some()
                    && self.recovery_file_serves_set(job_id, file_index, *set_id))
                .then_some(*set_id)
            })
            .collect()
    }

    /// How many bytes of this file have been committed to disk so far.
    pub(super) fn recovery_file_received_bytes(&self, job_id: JobId, file_index: u32) -> u64 {
        self.jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(NzbFileId { job_id, file_index }))
            .map(|file| file.received_bytes())
            .unwrap_or(0)
    }

    /// Whether any of this file's work is parked in the recovery queue.
    ///
    /// Parked work is not in flight, but it is not lost either: the completion
    /// gate moves a promoted file's parked segments back onto the download queue
    /// on its way in. A volume in that state is waiting, not stranded.
    pub(super) fn recovery_file_has_parked_segments(&self, job_id: JobId, file_index: u32) -> bool {
        let file_id = NzbFileId { job_id, file_index };
        self.jobs.get(&job_id).is_some_and(|state| {
            state
                .recovery_queue
                .count_matching(|work| work.segment_id.file_id == file_id)
                > 0
        })
    }

    /// Whether this file is a PAR2 recovery volume that has bytes on disk, will
    /// never complete, and has nothing left in flight that could change that.
    pub(super) fn recovery_volume_is_stranded(&self, job_id: JobId, file_index: u32) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(file) = state.assembly.file(NzbFileId { job_id, file_index }) else {
            return false;
        };
        if !matches!(
            file.role(),
            weaver_model::files::FileRole::Par2 {
                is_index: false,
                ..
            }
        ) {
            return false;
        }
        if file.is_complete() {
            return false;
        }
        // Bytes have to exist to be read. A volume is normally reached here
        // because it was promoted, but a job whose NZB carries no index
        // downloads its smallest volume eagerly without promoting anything, and
        // that volume can strand in exactly the same way.
        let promoted = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&file_index))
            .is_some_and(|entry| entry.promoted);
        if !promoted && file.received_bytes() == 0 {
            return false;
        }
        // A segment the servers have run out of answers for is the one fact that
        // settles this on its own: the volume cannot complete, whatever else the
        // job still has moving.
        if self.promoted_recovery_file_has_unavailable_segment(job_id, file_index) {
            return true;
        }
        // Otherwise "cannot complete" is a claim about the whole pipeline, not
        // about this instant. Work parked for later, or anything of this job's
        // still moving, means the articles that would finish this volume may yet
        // arrive — and a volume read back while they were merely resting is one
        // that reports the short count it happened to see.
        !self.promoted_recovery_file_has_pending_work(job_id, file_index)
            && !self.recovery_file_has_parked_segments(job_id, file_index)
            && !self.job_has_pending_download_pipeline_work(job_id)
    }

    pub(super) async fn salvage_stranded_recovery_volume(
        &mut self,
        job_id: JobId,
        file_index: u32,
        expected_set_id: par2_rs::RecoverySetId,
    ) {
        let file_id = NzbFileId { job_id, file_index };
        let Some((filename, file_path, received_bytes)) =
            self.jobs.get(&job_id).and_then(|state| {
                let file_asm = state.assembly.file(file_id)?;
                let filename = self.current_filename_for_file(job_id, file_asm);
                let file_path = state.working_dir.join(&filename);
                Some((filename, file_path, file_asm.received_bytes()))
            })
        else {
            return;
        };

        {
            let runtime = self.ensure_par2_runtime(job_id);
            let entry = runtime.files.entry(file_index).or_default();
            entry.filename = filename.clone();
        }
        #[cfg(test)]
        {
            self.par2_recovery_salvage_scans += 1;
        }

        let scan_path = file_path.clone();
        let budget = self.par2_scan_budget(job_id);
        let packet_list = match tokio::task::spawn_blocking(move || {
            scan_job_par2_packets(&scan_path, &budget).map(|packets| {
                packets
                    .into_iter()
                    .filter(|scanned| scanned.recovery_set_id == expected_set_id)
                    .filter_map(|scanned| match scanned.packet {
                        par2_rs::Packet::RecoverySlice(recovery) => {
                            // Metadata packets were hashed by the scan itself;
                            // recovery payloads are deliberately skipped there,
                            // so this is where a payload sitting in a hole is
                            // told apart from one that arrived.
                            let exponent = recovery.exponent;
                            match recovery
                                .data
                                .validate_packet_hash(expected_set_id.as_bytes(), exponent)
                            {
                                Ok(true) => Some(par2_rs::Packet::RecoverySlice(recovery)),
                                _ => None,
                            }
                        }
                        metadata => Some(metadata),
                    })
                    .collect::<Vec<_>>()
            })
        })
        .await
        {
            Ok(Ok(packet_list)) => {
                // The bytes that were on disk have now been looked at, whatever
                // they turned out to hold. A read that never got that far leaves
                // no mark, so the next articles to land bring the volume back
                // here rather than writing it off on a file that was not there.
                let runtime = self.ensure_par2_runtime(job_id);
                let entry = runtime.files.entry(file_index).or_default();
                entry.salvaged_at_received_bytes = Some(received_bytes);
                packet_list
            }
            Ok(Err(error @ par2_rs::Par2Error::ResourceLimitExceeded { .. })) => {
                self.fail_job(job_id, error.to_string());
                return;
            }
            Ok(Err(error)) => {
                warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %error,
                    "failed to read back a PAR2 recovery volume that cannot complete"
                );
                return;
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %error,
                    "failed to join PAR2 recovery read-back task"
                );
                return;
            }
        };

        let salvaged_blocks = packet_list
            .iter()
            .filter(|packet| matches!(packet, par2_rs::Packet::RecoverySlice(_)))
            .count() as u32;
        if salvaged_blocks == 0 {
            info!(
                job_id = job_id.0,
                filename = %filename,
                "no PAR2 recovery packets survived on a volume that cannot complete"
            );
            return;
        }

        let merge_result = {
            let Some(set) = self
                .ensure_par2_runtime(job_id)
                .set_runtime_mut(expected_set_id)
                .and_then(|set_runtime| set_runtime.set.as_mut())
            else {
                return;
            };
            let par2_set = Arc::make_mut(set);
            let merge = par2_set.merge_packets(packet_list);
            let total_recovery = par2_set.recovery_block_count();
            (merge, total_recovery)
        };
        if let Some(set) = self.par2_set_for(job_id, expected_set_id).cloned()
            && !self.admit_par2_geometry(job_id, &set)
        {
            return;
        }
        match merge_result {
            (Ok(merge), total_recovery) => {
                self.evict_par2_repair_session(job_id, expected_set_id);
                // What the merge accepted, not what the scan found. An exponent
                // already held by the set is not new recovery, and counting the
                // scan would credit this volume with a block the arithmetic
                // already had.
                let salvaged_blocks = merge.new_recovery_slices;
                {
                    let runtime = self.ensure_par2_runtime(job_id);
                    let entry = runtime.files.entry(file_index).or_default();
                    // A second read-back of the same volume reports only what
                    // the first one did not already merge, so the file's total
                    // is the sum of its read-backs rather than the last of them.
                    let blocks = entry
                        .recovery_blocks_by_set
                        .entry(expected_set_id)
                        .or_insert(0);
                    *blocks = blocks.saturating_add(salvaged_blocks);
                    entry.validated_recovery_blocks = *blocks;
                    entry.recovery_capacity_accounted = true;
                    entry.salvaged = *blocks > 0;
                    entry.recovery_set_id = Some(expected_set_id);
                }
                info!(
                    job_id = job_id.0,
                    filename = %filename,
                    salvaged_blocks,
                    total_recovery,
                    "read back recovery blocks from a PAR2 volume that cannot complete"
                );
            }
            (Err(error @ par2_rs::Par2Error::ResourceLimitExceeded { .. }), _) => {
                self.fail_job(job_id, error.to_string());
            }
            (Err(error), _) => {
                warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %error,
                    "failed to merge read-back PAR2 recovery packets"
                );
            }
        }
    }

    /// Forget that a recovery volume was ever read back short.
    ///
    /// Called where the file re-opens for download, so a volume that does
    /// arrive after all merges through the ordinary completion path and reports
    /// its whole block count.
    pub(in crate::pipeline) fn clear_par2_salvage_state_for_file(&mut self, file_id: NzbFileId) {
        if let Some(entry) = self
            .par2_runtime
            .get_mut(&file_id.job_id)
            .and_then(|runtime| runtime.files.get_mut(&file_id.file_index))
        {
            entry.salvaged = false;
            entry.salvaged_at_received_bytes = None;
        }
    }
}
