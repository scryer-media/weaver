use super::*;

impl Pipeline {
    fn is_recoverable_full_set_extraction_error(error: &str) -> bool {
        let lower = error.to_ascii_lowercase();
        lower.contains("checksum") || lower.contains("crc mismatch")
    }

    fn rar_set_worker_limit(plan: &crate::pipeline::archive::rar_state::RarDerivedPlan) -> usize {
        if plan.is_solid { 1 } else { 2 }
    }

    pub(crate) async fn try_rar_extraction(&mut self, job_id: JobId) {
        self.try_batch_extraction(job_id).await;
    }

    async fn try_batch_extraction(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        match &state.status {
            JobStatus::Downloading | JobStatus::Verifying | JobStatus::Extracting => {}
            _ => return,
        }

        let active_workers = self
            .rar_sets
            .values()
            .map(|state| state.active_workers)
            .sum::<usize>();
        let job_active_workers = self
            .rar_sets
            .iter()
            .filter(|((jid, _), _)| *jid == job_id)
            .map(|(_, state)| state.active_workers)
            .sum::<usize>();
        let available_slots = self
            .tuner
            .max_concurrent_extractions()
            .saturating_sub(active_workers);
        if available_slots == 0 {
            if job_active_workers == 0
                && self
                    .jobs
                    .get(&job_id)
                    .is_some_and(|state| matches!(state.status, JobStatus::Extracting))
            {
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::QueuedExtract,
                    Some("queued_extract"),
                );
            }
            return;
        }

        let mut ready_sets: Vec<(String, Vec<String>)> = self
            .rar_sets
            .iter()
            .filter(|((jid, _), _)| *jid == job_id)
            .filter_map(|((_, set_name), set_state)| {
                let plan = set_state.plan.as_ref()?;
                let worker_limit = Self::rar_set_worker_limit(plan);
                let free_workers = worker_limit.saturating_sub(set_state.active_workers);
                if free_workers == 0 || plan.ready_members.is_empty() {
                    return None;
                }
                let mut seen_members = HashSet::new();
                let mut members = Vec::new();
                for ready_member in &plan.ready_members {
                    if !seen_members.insert(ready_member.name.clone()) {
                        continue;
                    }
                    if set_state.in_flight_members.contains(&ready_member.name) {
                        continue;
                    }
                    members.push(ready_member.name.clone());
                    if members.len() >= free_workers {
                        break;
                    }
                }
                (!members.is_empty()).then_some((set_name.clone(), members))
            })
            .collect();
        ready_sets.sort_by(|a, b| a.0.cmp(&b.0));
        if ready_sets.is_empty() {
            return;
        }
        if !self.maybe_start_extraction(job_id).await {
            return;
        }

        let mut scheduled_slots = 0usize;
        for (set_name, ready_members) in ready_sets {
            for member_name in ready_members {
                if scheduled_slots >= available_slots {
                    return;
                }

                let members_to_extract = vec![member_name.clone()];
                let volume_paths_map = self.volume_paths_for_rar_set(job_id, &set_name);
                if volume_paths_map.is_empty() {
                    continue;
                }
                let cached_headers = self.load_rar_snapshot(job_id, &set_name);

                let password = self.jobs.get(&job_id).unwrap().spec.password.clone();

                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    known_volumes = volume_paths_map.len(),
                    cached_headers = cached_headers.is_some(),
                    "RAR incremental extraction member ready"
                );

                if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                    set_state.active_workers += 1;
                    set_state.in_flight_members.insert(member_name.clone());
                    set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
                    if let Some(plan) = set_state.plan.as_mut() {
                        plan.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
                    }
                }
                scheduled_slots += 1;

                let output_dir = self.extraction_staging_dir(job_id);
                let event_tx = self.event_tx.clone();
                let db = self.db.clone();
                let attempted = members_to_extract.clone();
                let extract_done_tx = self.extract_done_tx.clone();
                let set_name_owned = set_name.clone();
                let set_name_for_task = set_name.clone();
                let set_name_for_archive = set_name.clone();
                let pp_pool = self.pp_pool.clone();
                tokio::task::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        pp_pool.install(move || {
                            let mut archive = Self::open_rar_archive_from_snapshot_or_disk(
                                &set_name_for_archive,
                                volume_paths_map.clone(),
                                password.clone(),
                                cached_headers,
                            )?;

                            let options = weaver_rar::ExtractOptions {
                                verify: true,
                                password: password.clone(),
                            };
                            let mut outcome = BatchExtractionOutcome {
                                extracted: Vec::new(),
                                failed: Vec::new(),
                            };

                            for member_name in &members_to_extract {
                                let Some(idx) = archive.find_member_sanitized(member_name) else {
                                    outcome.failed.push((
                                        member_name.clone(),
                                        "member not found in archive".to_string(),
                                    ));
                                    continue;
                                };

                                match Self::extract_rar_member_to_output(
                                    &mut archive,
                                    RarExtractionContext {
                                        volume_paths: &volume_paths_map,
                                        db: &db,
                                        event_tx: &event_tx,
                                        job_id,
                                        set_name: &set_name_for_task,
                                        output_dir: &output_dir,
                                        options: &options,
                                    },
                                    idx,
                                ) {
                                    Ok((extracted_name, bytes_written, total_bytes)) => {
                                        let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                                            job_id,
                                            member: extracted_name.clone(),
                                            bytes_written,
                                            total_bytes,
                                        });
                                        let _ = event_tx.send(
                                            PipelineEvent::ExtractionMemberFinished {
                                                job_id,
                                                set_name: set_name_for_task.clone(),
                                                member: extracted_name.clone(),
                                            },
                                        );
                                        outcome.extracted.push(extracted_name);
                                    }
                                    Err(error) => {
                                        let _ =
                                            event_tx.send(PipelineEvent::ExtractionMemberFailed {
                                                job_id,
                                                set_name: set_name_for_task.clone(),
                                                member: member_name.clone(),
                                                error: error.clone(),
                                            });
                                        outcome.failed.push((member_name.clone(), error));
                                    }
                                }
                            }

                            Ok(outcome)
                        })
                    })
                    .await;

                    let result = match result {
                        Ok(result) => result,
                        Err(error) => Err(format!("extraction task panicked: {error}")),
                    };
                    let _ = extract_done_tx
                        .send(ExtractionDone::Batch {
                            job_id,
                            set_name: set_name_owned,
                            attempted,
                            result,
                        })
                        .await;
                });
            }
        }
    }

    fn member_volume_span(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> Option<(u32, u32)> {
        let state = self.jobs.get(&job_id)?;
        let topo = state.assembly.archive_topology_for(set_name)?;
        let member = topo.members.iter().find(|m| m.name == member_name)?;
        Some((member.first_volume, member.last_volume))
    }

    pub(crate) fn clear_failed_extraction_member(&mut self, job_id: JobId, member_name: &str) {
        let mut remove_entry = false;
        if let Some(failed) = self.failed_extractions.get_mut(&job_id) {
            failed.remove(member_name);
            remove_entry = failed.is_empty();
        }
        if remove_entry {
            self.failed_extractions.remove(&job_id);
        }
        if let Err(error) = self.db.remove_failed_extraction(job_id, member_name) {
            error!(
                job_id = job_id.0,
                member = %member_name,
                error = %error,
                "failed to clear persisted failed extraction member"
            );
        }
    }

    fn suspect_par2_file_ids_for_member(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> Vec<weaver_par2::FileId> {
        let Some(par2_set) = self.par2_set(job_id) else {
            return Vec::new();
        };
        let Some((first_volume, last_volume)) =
            self.member_volume_span(job_id, set_name, member_name)
        else {
            return Vec::new();
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        let Some(topo) = state.assembly.archive_topology_for(set_name) else {
            return Vec::new();
        };

        let filename_to_file_id: HashMap<&str, weaver_par2::FileId> = par2_set
            .recovery_file_ids
            .iter()
            .filter_map(|file_id| {
                par2_set
                    .file_description(file_id)
                    .map(|desc| (desc.filename.as_str(), *file_id))
            })
            .collect();

        let mut file_ids = Vec::new();
        let target_first = first_volume.saturating_sub(1);
        let target_last = last_volume.saturating_add(1);
        for (filename, &volume_number) in &topo.volume_map {
            if (target_first..=target_last).contains(&volume_number)
                && let Some(file_id) = filename_to_file_id.get(filename.as_str())
            {
                file_ids.push(*file_id);
            }
        }
        file_ids.sort_unstable_by_key(|id| *id.as_bytes());
        file_ids.dedup();
        file_ids
    }

    async fn promote_recovery_for_failed_member(
        &mut self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) {
        let file_ids = self.suspect_par2_file_ids_for_member(job_id, set_name, member_name);
        if file_ids.is_empty() {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                member = %member_name,
                "no PAR2 file ids available for lower-bound verification"
            );
            return;
        }

        let (working_dir, par2_set) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(par2_set) = self.par2_set(job_id).cloned() else {
                return;
            };
            (state.working_dir.clone(), par2_set)
        };

        let pp_pool = self.pp_pool.clone();
        let lower_bound = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || -> Result<u32, String> {
                let plan = weaver_par2::scan_placement(&working_dir, &par2_set)
                    .map_err(|e| format!("placement scan failed: {e}"))?;
                let selected: HashSet<weaver_par2::FileId> = file_ids.iter().copied().collect();
                if plan
                    .conflicts
                    .iter()
                    .any(|file_id| selected.contains(file_id))
                {
                    return Err("placement conflicts in suspect files".to_string());
                }

                let access =
                    weaver_par2::PlacementFileAccess::from_plan(working_dir, &par2_set, &plan);
                let verification =
                    weaver_par2::verify_selected_file_ids(&par2_set, &access, &file_ids);
                Ok(verification.total_missing_blocks)
            })
        })
        .await;

        match lower_bound {
            Ok(Ok(blocks_needed)) if blocks_needed > 0 => {
                let promoted = self.promote_recovery_targeted(job_id, blocks_needed);
                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    blocks_needed,
                    promoted_blocks = promoted,
                    "targeted recovery promotion from lower-bound verify"
                );
            }
            Ok(Ok(_)) => {
                debug!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    "lower-bound verify found no missing slices"
                );
            }
            Ok(Err(error)) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    error = %error,
                    "skipping lower-bound targeted promotion"
                );
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    error = %error,
                    "lower-bound verification task panicked"
                );
            }
        }
    }

    pub(crate) async fn handle_extraction_done(&mut self, done: ExtractionDone) {
        match done {
            ExtractionDone::Batch {
                job_id,
                set_name,
                attempted,
                result,
            } => {
                if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                    set_state.active_workers = set_state.active_workers.saturating_sub(1);
                    for member in &attempted {
                        set_state.in_flight_members.remove(member);
                    }
                }

                match result {
                    Ok(outcome) => {
                        for name in &outcome.extracted {
                            info!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                member = %name,
                                "RAR batch member extracted"
                            );
                            self.extracted_members
                                .entry(job_id)
                                .or_default()
                                .insert(name.clone());
                            if let Some(state) = self.jobs.get(&job_id) {
                                let output_path = state.working_dir.join(name);
                                if let Err(error) =
                                    self.db.add_extracted_member(job_id, name, &output_path)
                                {
                                    error!(
                                        job_id = job_id.0,
                                        set_name = %set_name,
                                        member = %name,
                                        error = %error,
                                        "failed to persist extracted member"
                                    );
                                }
                            }
                            self.clear_failed_extraction_member(job_id, name);
                        }
                        for (member, error) in &outcome.failed {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                member = %member,
                                error = %error,
                                "RAR batch member failed"
                            );
                            self.set_failed_extraction_member(job_id, member);
                            self.promote_recovery_for_failed_member(job_id, &set_name, member)
                                .await;
                        }
                    }
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            error = %error,
                            "RAR batch extraction worker failed"
                        );
                        for member in &attempted {
                            self.set_failed_extraction_member(job_id, member);
                            self.promote_recovery_for_failed_member(job_id, &set_name, member)
                                .await;
                        }
                    }
                }

                if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error,
                        "failed to refresh RAR set state after batch extraction"
                    );
                }
                self.try_delete_volumes(job_id, &set_name);
                let all_downloaded = self.jobs.get(&job_id).is_some_and(|state| {
                    state.assembly.complete_data_file_count() >= state.assembly.data_file_count()
                });
                if all_downloaded {
                    self.check_job_completion(job_id).await;
                } else {
                    self.try_rar_extraction(job_id).await;
                    if !self.has_active_rar_workers(job_id)
                        && self
                            .jobs
                            .get(&job_id)
                            .is_some_and(|state| matches!(state.status, JobStatus::Extracting))
                    {
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                    }
                }
            }
            ExtractionDone::FullSet {
                job_id,
                set_name,
                result,
            } => match result {
                Ok(outcome) => {
                    if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                        set_state.active_workers = 0;
                        set_state.in_flight_members.clear();
                    }
                    for member in &outcome.extracted {
                        self.extracted_members
                            .entry(job_id)
                            .or_default()
                            .insert(member.clone());
                        if let Some(state) = self.jobs.get(&job_id) {
                            let output_path = state.working_dir.join(member);
                            let _ = self.db.add_extracted_member(job_id, member, &output_path);
                        }
                        self.clear_failed_extraction_member(job_id, member);
                    }

                    if outcome.failed.is_empty() {
                        info!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            members = outcome.extracted.len(),
                            "set extraction complete"
                        );
                        if let Some(sets) = self.inflight_extractions.get_mut(&job_id) {
                            sets.remove(&set_name);
                        }
                        self.extracted_archives
                            .entry(job_id)
                            .or_default()
                            .insert(set_name.clone());
                    } else {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            succeeded = outcome.extracted.len(),
                            failed = ?outcome.failed,
                            "set extraction completed with failures"
                        );
                        for member in &outcome.failed {
                            if let Some(members) = self.extracted_members.get_mut(&job_id) {
                                members.remove(member);
                            }
                            self.set_failed_extraction_member(job_id, member);
                            self.promote_recovery_for_failed_member(job_id, &set_name, member)
                                .await;
                        }
                        if let Some(sets) = self.inflight_extractions.get_mut(&job_id) {
                            sets.remove(&set_name);
                        }
                    }
                    if self.rar_sets.contains_key(&(job_id, set_name.clone())) {
                        let _ = self.recompute_rar_set_state(job_id, &set_name).await;
                    }
                    self.try_delete_volumes(job_id, &set_name);
                    self.check_job_completion(job_id).await;
                }
                Err(e) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %e,
                        "set extraction failed"
                    );
                    if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                        set_state.active_workers = 0;
                        set_state.in_flight_members.clear();
                    }
                    if let Some(sets) = self.inflight_extractions.get_mut(&job_id) {
                        sets.remove(&set_name);
                    }
                    if Self::is_recoverable_full_set_extraction_error(&e) {
                        self.set_failed_extraction_member(job_id, &set_name);
                        self.check_job_completion(job_id).await;
                        return;
                    }
                    let _ = self.event_tx.send(PipelineEvent::ExtractionFailed {
                        job_id,
                        error: e.clone(),
                    });
                    self.fail_job(job_id, e);
                }
            },
        }
    }
}
