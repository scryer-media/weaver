use super::*;

struct ReadyRarExtraction {
    set_name: String,
    members: Vec<String>,
    volume_paths_map: std::collections::BTreeMap<u32, PathBuf>,
    cached_headers: Option<Vec<u8>>,
    password: Option<String>,
    is_solid: bool,
}

impl Pipeline {
    fn is_recoverable_full_set_extraction_error(error: &str) -> bool {
        let lower = error.to_ascii_lowercase();
        lower.contains("checksum") || lower.contains("crc mismatch")
    }

    fn rar_set_worker_limit(plan: &crate::pipeline::archive::rar_state::RarDerivedPlan) -> usize {
        if plan.is_solid { 1 } else { 2 }
    }

    pub(crate) fn rar_member_can_start_extraction(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> bool {
        self.rar_sets
            .get(&(job_id, set_name.to_string()))
            .and_then(|state| state.plan.as_ref())
            .is_some_and(|plan| {
                plan.ready_members
                    .iter()
                    .any(|ready_member| ready_member.name == member_name)
            })
    }

    pub(crate) fn rar_volume_paths_need_header_refresh(
        &self,
        job_id: JobId,
        set_name: &str,
        volume_paths: &std::collections::BTreeMap<u32, PathBuf>,
    ) -> bool {
        self.rar_sets
            .get(&(job_id, set_name.to_string()))
            .is_some_and(|state| {
                volume_paths
                    .keys()
                    .any(|volume| state.verified_suspect_volumes.contains(volume))
            })
    }

    pub(in crate::pipeline) fn volume_paths_for_rar_members(
        &self,
        job_id: JobId,
        set_name: &str,
        members: &[String],
        volume_paths: &std::collections::BTreeMap<u32, PathBuf>,
        has_cached_headers: bool,
        is_solid: bool,
    ) -> std::collections::BTreeMap<u32, PathBuf> {
        if is_solid || !has_cached_headers {
            return volume_paths.clone();
        }

        let mut selected = std::collections::BTreeMap::new();
        for member in members {
            let Some((first_volume, last_volume)) =
                self.member_volume_span(job_id, set_name, member)
            else {
                continue;
            };
            for volume in first_volume..=last_volume {
                if let Some(path) = volume_paths.get(&volume) {
                    selected.insert(volume, path.clone());
                }
            }
        }

        if selected.is_empty() {
            volume_paths.clone()
        } else {
            selected
        }
    }

    fn revalidate_rar_members_from_registered_volumes(
        set_name: &str,
        candidates: &[String],
        volume_paths: &std::collections::BTreeMap<u32, PathBuf>,
        cached_headers: Option<Vec<u8>>,
        password: Option<String>,
        refresh_provided_volumes: bool,
    ) -> Result<HashSet<String>, String> {
        let selected = candidates
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let archive = Self::open_rar_archive_from_snapshot_or_disk(
            set_name,
            volume_paths.clone(),
            password,
            cached_headers,
            refresh_provided_volumes,
        )?;
        let mut grouped = HashMap::<String, (usize, bool)>::new();
        for state in archive.planner_member_states() {
            if !selected.contains(state.name.as_str()) {
                continue;
            }
            let entry = grouped.entry(state.name).or_default();
            entry.0 += 1;
            entry.1 |= state.extractable;
        }

        Ok(candidates
            .iter()
            .filter(|member| {
                grouped
                    .get(member.as_str())
                    .is_some_and(|(_, extractable)| *extractable)
            })
            .cloned()
            .collect())
    }

    fn reconcile_waiting_members_for_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
        members: &[String],
    ) {
        let waiting_on_volumes = self
            .rar_sets
            .get(&(job_id, set_name.to_string()))
            .and_then(|state| state.plan.as_ref())
            .map(|plan| plan.waiting_on_volumes.clone())
            .unwrap_or_default();

        for member in members {
            let key = (job_id, set_name.to_string(), member.clone());
            let next_wait_volume =
                self.member_volume_span(job_id, set_name, member)
                    .and_then(|(first, last)| {
                        (first..=last)
                            .find(|volume| waiting_on_volumes.contains(volume))
                            .map(|volume| volume as usize)
                    });

            match next_wait_volume {
                Some(volume_index) => {
                    let previous = self.rar_waiting_members.insert(key.clone(), volume_index);
                    if previous != Some(volume_index) {
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::ExtractionMemberWaitingStarted {
                                job_id,
                                set_name: set_name.to_string(),
                                member: member.clone(),
                                volume_index,
                            });
                    }
                }
                None => {
                    if let Some(volume_index) = self.rar_waiting_members.remove(&key) {
                        let _ =
                            self.event_tx
                                .send(PipelineEvent::ExtractionMemberWaitingFinished {
                                    job_id,
                                    set_name: set_name.to_string(),
                                    member: member.clone(),
                                    volume_index,
                                });
                    }
                }
            }
        }
    }

    pub(crate) async fn try_rar_extraction(&mut self, job_id: JobId) {
        self.try_batch_extraction(job_id).await;
    }

    async fn try_batch_extraction(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        if matches!(
            state.status,
            JobStatus::Paused
                | JobStatus::Checking
                | JobStatus::Moving
                | JobStatus::Complete
                | JobStatus::Failed { .. }
                | JobStatus::Verifying
                | JobStatus::Repairing
                | JobStatus::QueuedRepair
        ) {
            return;
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

        let mut candidate_sets: Vec<(String, Vec<String>, bool)> = self
            .rar_sets
            .iter()
            .filter(|((jid, _), _)| *jid == job_id)
            .filter_map(|((_, set_name), set_state)| {
                let plan = set_state.plan.as_ref()?;
                let is_solid = plan.is_solid;
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
                    if !self.rar_member_can_start_extraction(job_id, set_name, &ready_member.name) {
                        continue;
                    }
                    members.push(ready_member.name.clone());
                    if members.len() >= free_workers {
                        break;
                    }
                }
                (!members.is_empty()).then_some((set_name.clone(), members, is_solid))
            })
            .collect();
        candidate_sets.sort_by(|a, b| a.0.cmp(&b.0));
        if candidate_sets.is_empty() {
            return;
        }

        let mut ready_sets = Vec::new();
        for (set_name, candidate_members, is_solid) in candidate_sets {
            let volume_paths_map = self.volume_paths_for_rar_set(job_id, &set_name);
            if volume_paths_map.is_empty() {
                continue;
            }
            let cached_headers = self.load_rar_snapshot(job_id, &set_name);
            let Some(password) = self
                .jobs
                .get(&job_id)
                .map(|state| state.spec.password.clone())
            else {
                return;
            };
            let validation_volume_paths = self.volume_paths_for_rar_members(
                job_id,
                &set_name,
                &candidate_members,
                &volume_paths_map,
                cached_headers.is_some(),
                is_solid,
            );
            let refresh_validation_headers = self.rar_volume_paths_need_header_refresh(
                job_id,
                &set_name,
                &validation_volume_paths,
            );
            let extractable = match Self::revalidate_rar_members_from_registered_volumes(
                &set_name,
                &candidate_members,
                &validation_volume_paths,
                cached_headers.clone(),
                password.clone(),
                refresh_validation_headers,
            ) {
                Ok(extractable) => extractable,
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %error,
                        "skipping RAR extraction scheduling after readiness revalidation failed"
                    );
                    if let Err(recompute_error) =
                        self.recompute_rar_set_state(job_id, &set_name).await
                    {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            error = %recompute_error,
                            "failed to refresh RAR state after readiness revalidation failed"
                        );
                    }
                    self.reconcile_waiting_members_for_set(job_id, &set_name, &candidate_members);
                    continue;
                }
            };
            let mut extractable_members = extractable.iter().cloned().collect::<Vec<_>>();
            extractable_members.sort();

            let rejected_members = candidate_members
                .iter()
                .filter(|member| !extractable.contains(member.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                candidate_members = ?candidate_members,
                extractable_members = ?extractable_members,
                rejected_members = ?rejected_members,
                validation_volumes = ?validation_volume_paths.keys().copied().collect::<Vec<_>>(),
                cached_headers = cached_headers.is_some(),
                refresh_validation_headers,
                is_solid,
                "RAR extraction scheduling revalidated members"
            );
            if !rejected_members.is_empty() {
                if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %error,
                        "failed to refresh RAR state after readiness revalidation rejected members"
                    );
                }
                self.reconcile_waiting_members_for_set(job_id, &set_name, &rejected_members);
            }

            let members = candidate_members
                .into_iter()
                .filter(|member| extractable.contains(member))
                .collect::<Vec<_>>();
            if members.is_empty() {
                continue;
            }
            ready_sets.push(ReadyRarExtraction {
                set_name,
                members,
                volume_paths_map,
                cached_headers,
                password,
                is_solid,
            });
        }

        if ready_sets.is_empty() {
            return;
        }

        if !self.maybe_start_extraction(job_id).await {
            return;
        }

        let mut scheduled_slots = 0usize;
        for ready_set in ready_sets {
            let ReadyRarExtraction {
                set_name,
                members: ready_members,
                volume_paths_map,
                cached_headers,
                password,
                is_solid,
            } = ready_set;
            for member_name in ready_members {
                if scheduled_slots >= available_slots {
                    return;
                }

                let members_to_extract = vec![member_name.clone()];
                let volume_paths_for_member = self.volume_paths_for_rar_members(
                    job_id,
                    &set_name,
                    &members_to_extract,
                    &volume_paths_map,
                    cached_headers.is_some(),
                    is_solid,
                );
                let refresh_member_headers = self.rar_volume_paths_need_header_refresh(
                    job_id,
                    &set_name,
                    &volume_paths_for_member,
                );
                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    known_volumes = volume_paths_for_member.len(),
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
                if let Some(volume_index) = self.rar_waiting_members.remove(&(
                    job_id,
                    set_name.clone(),
                    member_name.clone(),
                )) {
                    let _ = self
                        .event_tx
                        .send(PipelineEvent::ExtractionMemberWaitingFinished {
                            job_id,
                            set_name: set_name.clone(),
                            member: member_name.clone(),
                            volume_index,
                        });
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
                let volume_paths_for_task = volume_paths_for_member;
                let cached_headers_for_task = cached_headers.clone();
                let password_for_task = password.clone();
                let pp_pool = self.pp_pool.clone();
                tokio::task::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        pp_pool.install(move || {
                            let mut archive = Self::open_rar_archive_from_snapshot_or_disk(
                                &set_name_for_archive,
                                volume_paths_for_task.clone(),
                                password_for_task.clone(),
                                cached_headers_for_task,
                                refresh_member_headers,
                            )?;

                            let options = weaver_rar::ExtractOptions {
                                verify: true,
                                password: password_for_task.clone(),
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
                                        volume_paths: &volume_paths_for_task,
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
                        info!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            attempted = ?attempted,
                            extracted = ?outcome.extracted,
                            failed = ?outcome.failed,
                            "RAR batch extraction completed"
                        );
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
                            if self.jobs.contains_key(&job_id) {
                                let output_root = self
                                    .jobs
                                    .get(&job_id)
                                    .and_then(|state| state.staging_dir.clone())
                                    .unwrap_or_else(|| {
                                        self.deterministic_extraction_staging_dir(job_id)
                                    });
                                let output_path = output_root.join(name);
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
                            attempted = ?attempted,
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

                self.purge_empty_rar_set_if_idle(job_id, &set_name);
                if self.rar_sets.contains_key(&(job_id, set_name.clone()))
                    && let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await
                {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error,
                        "failed to refresh RAR set state after batch extraction"
                    );
                }
                if self.rar_sets.contains_key(&(job_id, set_name.clone())) {
                    self.reconcile_waiting_members_for_set(job_id, &set_name, &attempted);
                }
                self.reconcile_job_progress(job_id).await;
                if self.rar_sets.contains_key(&(job_id, set_name.clone())) {
                    self.try_delete_volumes(job_id, &set_name);
                }
                let all_downloaded = self.jobs.get(&job_id).is_some_and(|state| {
                    state.assembly.complete_data_file_count() >= state.assembly.data_file_count()
                });
                if all_downloaded {
                    self.check_job_completion(job_id).await;
                } else {
                    self.try_rar_extraction(job_id).await;
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
                        if self.jobs.contains_key(&job_id) {
                            let output_root = self
                                .jobs
                                .get(&job_id)
                                .and_then(|state| state.staging_dir.clone())
                                .unwrap_or_else(|| {
                                    self.deterministic_extraction_staging_dir(job_id)
                                });
                            let output_path = output_root.join(member);
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
                    self.purge_empty_rar_set_if_idle(job_id, &set_name);
                    if self.rar_sets.contains_key(&(job_id, set_name.clone())) {
                        let _ = self.recompute_rar_set_state(job_id, &set_name).await;
                    }
                    self.reconcile_job_progress(job_id).await;
                    if self.rar_sets.contains_key(&(job_id, set_name.clone())) {
                        self.try_delete_volumes(job_id, &set_name);
                    }
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
                    self.purge_empty_rar_set_if_idle(job_id, &set_name);
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
