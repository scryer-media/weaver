use super::*;
use crate::pipeline::{RAR_CAPACITY_RETRY_DELAY, RarCapacityRetry, RarCapacityRetryKind};

struct ReadyRarExtraction {
    set_name: String,
    members: Vec<String>,
    volume_paths_map: std::collections::BTreeMap<u32, PathBuf>,
    cached_headers: Option<Vec<u8>>,
    shared_kdf_cache: std::sync::Arc<weaver_unrar::crypto::KdfCache>,
    password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
    is_solid: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RarExtractionSettle {
    Idle,
    ActiveWorkersRemain,
    RefreshLaunched { allows_extraction: bool },
}

impl Pipeline {
    fn is_recoverable_full_set_extraction_error(error: &str) -> bool {
        let lower = error.to_ascii_lowercase();
        lower.contains("checksum") || lower.contains("crc mismatch")
    }

    async fn normalize_extraction_output_tree(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), String> {
        let output_root = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.staging_dir.clone())
            .unwrap_or_else(|| self.deterministic_extraction_staging_dir(job_id));
        let output_root_for_error = output_root.clone();
        tokio::task::spawn_blocking(move || {
            crate::runtime::fs::normalize_extracted_tree(&output_root)
        })
        .await
        .map_err(|error| {
            format!(
                "extraction output permission normalization task panicked for set '{set_name}': {error}"
            )
        })?
        .map_err(|error| {
            format!(
                "failed to normalize extraction output permissions for set '{set_name}' at {}: {error}",
                output_root_for_error.display()
            )
        })
    }

    fn rar_set_worker_limit(plan: &crate::pipeline::archive::rar_state::RarDerivedPlan) -> usize {
        if plan.is_solid { 1 } else { 2 }
    }

    fn is_stale_topology_batch_extraction_error(error: &str) -> bool {
        let lower = error.to_ascii_lowercase();
        error.contains("member not found in archive")
            || lower.contains("not registered")
            || lower.contains("unavailable")
            || lower.contains("no on-disk rar volumes")
    }

    fn is_capacity_pressure_batch_extraction_error(error: &str) -> bool {
        crate::pipeline::capacity::is_fd_capacity_error_message(error)
    }

    pub(in crate::pipeline) fn schedule_rar_capacity_retry(
        &mut self,
        job_id: JobId,
        set_name: &str,
        kind: RarCapacityRetryKind,
    ) {
        let set_name = set_name.to_string();
        let key = (job_id, set_name.clone(), kind);
        if !self.pending_rar_capacity_retries.insert(key) {
            return;
        }

        let retry_tx = self.rar_capacity_retry_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(RAR_CAPACITY_RETRY_DELAY).await;
            let _ = retry_tx
                .send(RarCapacityRetry {
                    job_id,
                    set_name,
                    kind,
                })
                .await;
        });
    }

    pub(in crate::pipeline) async fn handle_rar_capacity_retry(&mut self, retry: RarCapacityRetry) {
        let key = (retry.job_id, retry.set_name.clone(), retry.kind);
        self.pending_rar_capacity_retries.remove(&key);

        if !self.jobs.contains_key(&retry.job_id)
            || !self
                .rar_sets
                .contains_key(&(retry.job_id, retry.set_name.clone()))
        {
            return;
        }

        match retry.kind {
            RarCapacityRetryKind::Refresh => {
                self.launch_queued_rar_capacity_refresh(retry.job_id, &retry.set_name);
            }
            RarCapacityRetryKind::Extraction => {
                self.try_rar_extraction(retry.job_id).await;
            }
            RarCapacityRetryKind::FullSetExtraction => {
                self.check_job_completion(retry.job_id).await;
            }
        }
    }

    pub(crate) fn rar_member_can_start_extraction(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> bool {
        if self
            .extracted_members
            .get(&job_id)
            .is_some_and(|members| members.contains(member_name))
        {
            return false;
        }
        let Some(set_state) = self.rar_sets.get(&(job_id, set_name.to_string())) else {
            return false;
        };
        set_state.plan.as_ref().is_some_and(|plan| {
            plan.ready_members
                .iter()
                .any(|ready_member| ready_member.name == member_name)
        })
    }

    pub(crate) fn rar_ready_member_is_startable_for_batch_extraction(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> bool {
        if self
            .inflight_extractions
            .get(&job_id)
            .is_some_and(|sets| sets.contains(set_name))
        {
            return false;
        }
        if self.pending_rar_capacity_retries.contains(&(
            job_id,
            set_name.to_string(),
            RarCapacityRetryKind::Extraction,
        )) {
            return false;
        }
        if self.pending_rar_capacity_retries.contains(&(
            job_id,
            set_name.to_string(),
            RarCapacityRetryKind::Refresh,
        )) {
            return false;
        }
        if self
            .rar_member_refresh_request(job_id, set_name, member_name)
            .is_some()
        {
            return false;
        }
        let Some(set_state) = self.rar_sets.get(&(job_id, set_name.to_string())) else {
            return false;
        };
        let Some(plan) = set_state.plan.as_ref() else {
            return false;
        };
        if set_state.active_workers >= Self::rar_set_worker_limit(plan) {
            return false;
        }
        if set_state.in_flight_members.contains(member_name) {
            return false;
        }
        if plan
            .member_dependencies
            .get(member_name)
            .is_some_and(|dependency| {
                !self
                    .extracted_members
                    .get(&job_id)
                    .is_some_and(|extracted| extracted.contains(&dependency.source_member))
            })
        {
            return false;
        }
        self.rar_member_can_start_extraction(job_id, set_name, member_name)
    }

    pub(in crate::pipeline) fn rar_member_refresh_request(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> Option<RarRefreshRequest> {
        let (first_volume, last_volume) = self.member_volume_span(job_id, set_name, member_name)?;
        let state = self
            .rar_refresh_state
            .get(&(job_id, set_name.to_string()))?;
        let target_completed_volume = state.latest_completed_volume.max(last_volume);
        if let Some(last_error) = state.last_error.as_ref() {
            return Some(RarRefreshRequest {
                target_completed_volume,
                reason: if last_error.is_capacity_pressure() {
                    RefreshReason::CoverageExpansion
                } else {
                    RefreshReason::ValidationFailure
                },
            });
        }
        if state.structure_dirty {
            return Some(RarRefreshRequest {
                target_completed_volume,
                reason: RefreshReason::IdentityRebind,
            });
        }
        if !(first_volume..=last_volume).all(|volume| state.refreshed_volumes.contains(&volume)) {
            return Some(RarRefreshRequest {
                target_completed_volume,
                reason: RefreshReason::CoverageExpansion,
            });
        }
        state
            .in_flight
            .into_iter()
            .chain(state.queued)
            .filter(|request| request.reason > RefreshReason::CoverageExpansion)
            .max_by_key(|request| request.reason)
    }

    pub(crate) fn rar_volume_paths_need_header_refresh(
        &self,
        job_id: JobId,
        set_name: &str,
        volume_paths: &std::collections::BTreeMap<u32, PathBuf>,
    ) -> bool {
        if self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0
        {
            return false;
        }

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
            if let Some(dependency) = self
                .rar_sets
                .get(&(job_id, set_name.to_string()))
                .and_then(|set| set.plan.as_ref())
                .and_then(|plan| plan.member_dependencies.get(member))
            {
                for volume in dependency.source_first_volume..=dependency.source_last_volume {
                    if let Some(path) = volume_paths.get(&volume) {
                        selected.insert(volume, path.clone());
                    }
                }
            }
        }

        if selected.is_empty() {
            volume_paths.clone()
        } else {
            selected
        }
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

        let generic_full_set_inflight = self
            .inflight_extractions
            .get(&job_id)
            .cloned()
            .unwrap_or_default();

        let mut candidate_sets: Vec<(String, Vec<String>, bool)> = self
            .rar_sets
            .iter()
            .filter(|((jid, _), _)| *jid == job_id)
            .filter(|((_, set_name), _)| !generic_full_set_inflight.contains(set_name))
            .filter(|((_, set_name), _)| {
                !self.pending_rar_capacity_retries.contains(&(
                    job_id,
                    (*set_name).clone(),
                    RarCapacityRetryKind::Extraction,
                ))
            })
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
                    if self
                        .extracted_members
                        .get(&job_id)
                        .is_some_and(|extracted| extracted.contains(&ready_member.name))
                    {
                        continue;
                    }
                    if plan
                        .member_dependencies
                        .get(&ready_member.name)
                        .is_some_and(|dependency| {
                            !self
                                .extracted_members
                                .get(&job_id)
                                .is_some_and(|extracted| {
                                    extracted.contains(&dependency.source_member)
                                })
                        })
                    {
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
            let mut gated_members = Vec::new();
            let mut blocked_members = Vec::new();
            let refresh_retry_pending = self.pending_rar_capacity_retries.contains(&(
                job_id,
                set_name.clone(),
                RarCapacityRetryKind::Refresh,
            ));
            for member in candidate_members {
                if refresh_retry_pending {
                    blocked_members.push(member);
                    continue;
                }
                if let Some(request) = self.rar_member_refresh_request(job_id, &set_name, &member) {
                    self.enqueue_rar_set_refresh(
                        job_id,
                        &set_name,
                        request.target_completed_volume,
                        request.reason,
                    );
                    blocked_members.push(member);
                } else if self
                    .rar_ready_member_is_startable_for_batch_extraction(job_id, &set_name, &member)
                {
                    gated_members.push(member);
                }
            }
            if !blocked_members.is_empty() {
                self.reconcile_waiting_members_for_set(job_id, &set_name, &blocked_members);
            }
            if gated_members.is_empty() {
                continue;
            }
            let volume_paths_map = self.volume_paths_for_rar_set(job_id, &set_name);
            if volume_paths_map.is_empty() {
                continue;
            }
            let cached_headers = self.load_rar_snapshot(job_id, &set_name);
            if !self.jobs.contains_key(&job_id) {
                return;
            }
            let password_candidates = self.archive_password_candidates_for_set(job_id, &set_name);
            let shared_kdf_cache = self
                .rar_sets
                .get(&(job_id, set_name.clone()))
                .map(|state| state.shared_kdf_cache.clone())
                .unwrap_or_else(|| std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()));
            ready_sets.push(ReadyRarExtraction {
                set_name,
                members: gated_members,
                volume_paths_map,
                cached_headers,
                shared_kdf_cache,
                password_candidates,
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
                shared_kdf_cache,
                password_candidates,
                is_solid,
            } = ready_set;
            for member_name in ready_members {
                if scheduled_slots >= available_slots {
                    return;
                }
                if !self.rar_member_can_start_extraction(job_id, &set_name, &member_name) {
                    continue;
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
                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    known_volumes = volume_paths_for_member.len(),
                    cached_headers = cached_headers.is_some(),
                    "RAR incremental extraction member ready"
                );

                let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) else {
                    continue;
                };
                if set_state.in_flight_members.contains(&member_name) {
                    continue;
                }
                set_state.active_workers += 1;
                set_state.in_flight_members.insert(member_name.clone());
                set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
                if let Some(plan) = set_state.plan.as_mut() {
                    plan.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
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
                let attempted = members_to_extract.clone();
                let extract_done_tx = self.extract_done_tx.clone();
                let phase_counters = self.phase_begin(job_id, JobPhase::Extracting, None);
                let already_extracted_for_totals = self
                    .extracted_members
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                self.phase_reserve_topology_extraction_totals(
                    job_id,
                    &set_name,
                    members_to_extract.iter().map(String::as_str),
                    &already_extracted_for_totals,
                    &phase_counters,
                );
                let set_name_owned = set_name.clone();
                let set_name_for_task = set_name.clone();
                let set_name_for_archive = set_name.clone();
                let volume_paths_for_task = volume_paths_for_member;
                let cached_headers_for_task = cached_headers.clone();
                let password_candidates_for_task = password_candidates.clone();
                let shared_kdf_cache_for_task = shared_kdf_cache.clone();
                let pp_pool = self.pp_pool.clone();
                tokio::task::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        pp_pool.install(move || {
                            let selection =
                                Self::open_rar_archive_for_extraction_with_password_candidates(
                                    RarExtractionOpenRequest {
                                        set_name: &set_name_for_archive,
                                        volume_paths: volume_paths_for_task.clone(),
                                        password_candidates: password_candidates_for_task.clone(),
                                        cached_headers: cached_headers_for_task,
                                        shared_kdf_cache: shared_kdf_cache_for_task,
                                        open_mode: RarArchiveOpenMode::AttachOnly,
                                        requested_members: &members_to_extract,
                                        already_extracted: None,
                                    },
                                )?;
                            let mut archive = selection.archive;
                            let selected_password = selection.password;
                            let archive_password_required = archive.metadata().is_encrypted;

                            let options = weaver_unrar::ExtractOptions {
                                verify: true,
                                password: selected_password.clone(),
                                restore_owners: false,
                            };
                            let mut outcome = BatchExtractionOutcome {
                                extracted: Vec::new(),
                                failed: Vec::new(),
                                selected_password: selection.validated_password,
                                phase_completed_bytes: 0,
                            };

                            for member_name in &members_to_extract {
                                let Some(idx) = archive.find_member_sanitized(member_name) else {
                                    outcome.failed.push((
                                        member_name.clone(),
                                        "member not found in archive".to_string(),
                                    ));
                                    continue;
                                };

                                let member_password_required = archive_password_required
                                    || archive
                                        .member_info(idx)
                                        .is_some_and(|member| member.is_encrypted);

                                match Self::extract_rar_member_to_output(
                                    &mut archive,
                                    RarExtractionContext {
                                        volume_paths: &volume_paths_for_task,
                                        event_tx: &event_tx,
                                        job_id,
                                        set_name: &set_name_for_task,
                                        output_dir: &output_dir,
                                        options: &options,
                                        phase_attempt: Some(Arc::new(PhaseAttemptCounters::new(
                                            Arc::clone(&phase_counters),
                                        ))),
                                    },
                                    idx,
                                ) {
                                    Ok((extracted_name, bytes_written, total_bytes)) => {
                                        if outcome.selected_password.is_none()
                                            && member_password_required
                                        {
                                            outcome.selected_password = selected_password.clone();
                                        }
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
                                        outcome.phase_completed_bytes = outcome
                                            .phase_completed_bytes
                                            .saturating_add(bytes_written);
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

        if !self.job_has_pending_download_pipeline_work(job_id) {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                member = %member_name,
                "skipping lower-bound targeted promotion because the download pipeline is already exhausted"
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

        #[cfg(test)]
        {
            self.par2_lower_bound_preflight_calls += 1;
        }

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

    #[cfg(test)]
    pub(crate) async fn test_promote_recovery_for_failed_member(
        &mut self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) {
        self.promote_recovery_for_failed_member(job_id, set_name, member_name)
            .await;
    }

    async fn settle_rar_set_after_extraction_worker(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> RarExtractionSettle {
        let key = (job_id, set_name.to_string());
        let Some(set_state) = self.rar_sets.get(&key) else {
            return RarExtractionSettle::Idle;
        };
        if set_state.active_workers > 0 || !set_state.in_flight_members.is_empty() {
            return RarExtractionSettle::ActiveWorkersRemain;
        }

        // UnRAR keeps volume readers owned by the extraction call and switches
        // volumes from inside that call (ExtractCurrentFile -> MergeArchive).
        // Once the worker has returned, no reader is live, so we can refresh
        // extracted/failed ownership and run the conservative delete audit.
        if let Err(error) = self.recompute_rar_set_state(job_id, set_name).await {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                error = %error,
                "failed to recompute RAR set after extraction worker completed"
            );
            return RarExtractionSettle::Idle;
        }
        let mut follow_up = None;
        if let Some(refresh_state) = self.rar_refresh_state.get_mut(&key)
            && refresh_state.in_flight.is_none()
        {
            if refresh_state
                .queued
                .as_ref()
                .is_some_and(|request| request.reason == RefreshReason::PostExtraction)
            {
                refresh_state.queued = None;
            } else if let Some(request) = refresh_state.queued.take() {
                refresh_state.in_flight = Some(request);
                follow_up = Some(request);
            }
        }
        if self.rar_sets.contains_key(&key) {
            self.try_delete_volumes(job_id, set_name);
        }
        if let Some(request) = follow_up {
            let allows_extraction = request.reason == RefreshReason::CoverageExpansion;
            self.spawn_rar_refresh(job_id, set_name.to_string(), request);
            RarExtractionSettle::RefreshLaunched { allows_extraction }
        } else {
            RarExtractionSettle::Idle
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
                let current_attempted =
                    if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                        let current = attempted
                            .iter()
                            .filter(|member| set_state.in_flight_members.contains(*member))
                            .cloned()
                            .collect::<HashSet<_>>();
                        set_state.active_workers = set_state.active_workers.saturating_sub(1);
                        for member in &attempted {
                            set_state.in_flight_members.remove(member);
                        }
                        set_state.extraction_generation =
                            set_state.extraction_generation.saturating_add(1);
                        current
                    } else {
                        HashSet::new()
                    };
                let already_extracted = self
                    .extracted_members
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                    debug_assert!(
                        attempted
                            .iter()
                            .all(|member| !set_state.in_flight_members.contains(member))
                    );
                }
                self.mark_rar_unlock_priorities_dirty(job_id);

                if current_attempted.is_empty() {
                    if let Ok(outcome) = &result {
                        self.phase_subtract_completed_bytes(
                            job_id,
                            JobPhase::Extracting,
                            outcome.phase_completed_bytes,
                        );
                    }
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        attempted = ?attempted,
                        result = if result.is_ok() { "success" } else { "error" },
                        "ignoring stale RAR batch extraction completion for non-current members"
                    );
                    return;
                }

                let mut capacity_retry = false;
                match result {
                    Ok(outcome) => {
                        if let Err(error) = self
                            .normalize_extraction_output_tree(job_id, &set_name)
                            .await
                        {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                error = %error,
                                "failed to normalize extraction output permissions"
                            );
                            self.fail_job(job_id, error);
                            return;
                        }
                        let password_candidates =
                            self.archive_password_candidates_for_set(job_id, &set_name);
                        self.remember_archive_password_winner(
                            job_id,
                            &set_name,
                            outcome.selected_password.as_deref(),
                            &password_candidates,
                        );
                        info!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            attempted = ?attempted,
                            extracted = ?outcome.extracted,
                            failed = ?outcome.failed,
                            "RAR batch extraction completed"
                        );
                        let mut extracted_to_persist: Vec<(String, std::path::PathBuf)> =
                            Vec::with_capacity(outcome.extracted.len());
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
                                extracted_to_persist.push((name.clone(), output_root.join(name)));
                            }
                            self.clear_failed_extraction_member(job_id, name);
                        }
                        if !extracted_to_persist.is_empty()
                            && let Err(error) = self
                                .db_blocking(move |db| {
                                    db.add_extracted_members(job_id, &extracted_to_persist)
                                })
                                .await
                        {
                            error!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                error = %error,
                                "failed to persist extracted members"
                            );
                        }
                        for (member, error) in &outcome.failed {
                            if already_extracted.contains(member)
                                || !current_attempted.contains(member)
                            {
                                warn!(
                                    job_id = job_id.0,
                                    set_name = %set_name,
                                    member = %member,
                                    error = %error,
                                    "ignoring stale RAR batch member failure for non-current or already extracted member"
                                );
                                continue;
                            }
                            if Self::is_stale_topology_batch_extraction_error(error) {
                                warn!(
                                    job_id = job_id.0,
                                    set_name = %set_name,
                                    member = %member,
                                    error = %error,
                                    "RAR batch member hit stale topology; queueing validation refresh"
                                );
                            } else if Self::is_capacity_pressure_batch_extraction_error(error) {
                                warn!(
                                    job_id = job_id.0,
                                    set_name = %set_name,
                                    member = %member,
                                    error = %error,
                                    "RAR batch member hit FD capacity pressure; keeping member waiting"
                                );
                            } else {
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
                        let refresh_retry_members = outcome
                            .failed
                            .iter()
                            .filter_map(|(member, error)| {
                                (current_attempted.contains(member)
                                    && !already_extracted.contains(member)
                                    && Self::is_stale_topology_batch_extraction_error(error))
                                .then_some(member.clone())
                            })
                            .collect::<Vec<_>>();
                        let capacity_retry_members = outcome
                            .failed
                            .iter()
                            .filter_map(|(member, error)| {
                                (current_attempted.contains(member)
                                    && !already_extracted.contains(member)
                                    && Self::is_capacity_pressure_batch_extraction_error(error))
                                .then_some(member.clone())
                            })
                            .collect::<Vec<_>>();
                        if !refresh_retry_members.is_empty() {
                            self.enqueue_rar_set_refresh(
                                job_id,
                                &set_name,
                                self.latest_completed_rar_volume(job_id, &set_name),
                                RefreshReason::ValidationFailure,
                            );
                            self.reconcile_waiting_members_for_set(
                                job_id,
                                &set_name,
                                &refresh_retry_members,
                            );
                        }
                        if !capacity_retry_members.is_empty() {
                            capacity_retry = true;
                            self.reconcile_waiting_members_for_set(
                                job_id,
                                &set_name,
                                &capacity_retry_members,
                            );
                        }
                    }
                    Err(error) => {
                        let current_failed_members = attempted
                            .iter()
                            .filter(|member| {
                                current_attempted.contains(*member)
                                    && !already_extracted.contains(*member)
                            })
                            .cloned()
                            .collect::<Vec<_>>();
                        if current_failed_members.is_empty() {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                attempted = ?attempted,
                                error = %error,
                                "ignoring stale RAR batch extraction failure for non-current or already extracted members"
                            );
                        } else if Self::is_stale_topology_batch_extraction_error(&error) {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                attempted = ?attempted,
                                error = %error,
                                "RAR batch extraction worker hit stale topology; queueing validation refresh"
                            );
                            self.enqueue_rar_set_refresh(
                                job_id,
                                &set_name,
                                self.latest_completed_rar_volume(job_id, &set_name),
                                RefreshReason::ValidationFailure,
                            );
                            self.reconcile_waiting_members_for_set(
                                job_id,
                                &set_name,
                                &current_failed_members,
                            );
                        } else if Self::is_capacity_pressure_batch_extraction_error(&error) {
                            capacity_retry = true;
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                attempted = ?attempted,
                                error = %error,
                                "RAR batch extraction worker hit FD capacity pressure; keeping members waiting"
                            );
                            self.reconcile_waiting_members_for_set(
                                job_id,
                                &set_name,
                                &current_failed_members,
                            );
                        } else {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                attempted = ?attempted,
                                error = %error,
                                "RAR batch extraction worker failed"
                            );
                            for member in &current_failed_members {
                                self.set_failed_extraction_member(job_id, member);
                                self.promote_recovery_for_failed_member(job_id, &set_name, member)
                                    .await;
                            }
                        }
                    }
                }

                self.purge_empty_rar_set_if_idle(job_id, &set_name);
                let settle_result = self
                    .settle_rar_set_after_extraction_worker(job_id, &set_name)
                    .await;
                if self.rar_sets.contains_key(&(job_id, set_name.clone())) {
                    let extracted_members = self.extracted_members.get(&job_id);
                    let waiting_reconcile_members = attempted
                        .iter()
                        .filter(|member| {
                            extracted_members.is_none_or(|extracted| !extracted.contains(*member))
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    self.reconcile_waiting_members_for_set(
                        job_id,
                        &set_name,
                        &waiting_reconcile_members,
                    );
                }
                self.reconcile_job_progress(job_id).await;
                let all_downloaded = self.jobs.get(&job_id).is_some_and(|state| {
                    state.assembly.complete_data_file_count() >= state.assembly.data_file_count()
                });
                if capacity_retry {
                    self.schedule_rar_capacity_retry(
                        job_id,
                        &set_name,
                        RarCapacityRetryKind::Extraction,
                    );
                } else {
                    match settle_result {
                        RarExtractionSettle::ActiveWorkersRemain => {
                            self.try_rar_extraction(job_id).await;
                        }
                        RarExtractionSettle::RefreshLaunched {
                            allows_extraction: true,
                        } => {
                            self.try_rar_extraction(job_id).await;
                        }
                        RarExtractionSettle::RefreshLaunched {
                            allows_extraction: false,
                        } => {}
                        RarExtractionSettle::Idle if all_downloaded => {
                            self.check_job_completion(job_id).await;
                        }
                        RarExtractionSettle::Idle => {
                            self.try_rar_extraction(job_id).await;
                        }
                    }
                }
            }
            ExtractionDone::FullSet {
                job_id,
                set_name,
                result,
            } => match result {
                Ok(outcome) => {
                    if let Err(error) = self
                        .normalize_extraction_output_tree(job_id, &set_name)
                        .await
                    {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            error = %error,
                            "failed to normalize extraction output permissions"
                        );
                        self.fail_job(job_id, error);
                        return;
                    }
                    let password_candidates =
                        self.archive_password_candidates_for_set(job_id, &set_name);
                    self.remember_archive_password_winner(
                        job_id,
                        &set_name,
                        outcome.selected_password.as_deref(),
                        &password_candidates,
                    );
                    if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                        set_state.active_workers = 0;
                        set_state.in_flight_members.clear();
                        set_state.extraction_generation =
                            set_state.extraction_generation.saturating_add(1);
                    }
                    self.mark_rar_unlock_priorities_dirty(job_id);
                    let mut extracted_to_persist: Vec<(String, std::path::PathBuf)> =
                        Vec::with_capacity(outcome.extracted.len());
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
                            extracted_to_persist.push((member.clone(), output_root.join(member)));
                        }
                        self.clear_failed_extraction_member(job_id, member);
                    }
                    if !extracted_to_persist.is_empty()
                        && let Err(error) = self
                            .db_blocking(move |db| {
                                db.add_extracted_members(job_id, &extracted_to_persist)
                            })
                            .await
                    {
                        error!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            error = %error,
                            "failed to persist extracted members"
                        );
                    }

                    let full_set_capacity_pressure = outcome
                        .failed
                        .iter()
                        .any(|(_, error)| Self::is_capacity_pressure_batch_extraction_error(error));
                    if full_set_capacity_pressure {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            succeeded = outcome.extracted.len(),
                            failed = ?outcome.failed,
                            "RAR full-set extraction hit FD capacity pressure; retrying without repair promotion"
                        );
                        if let Some(sets) = self.inflight_extractions.get_mut(&job_id) {
                            sets.remove(&set_name);
                        }
                        self.phase_end_extracting_if_idle(job_id);
                        if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone()))
                        {
                            let fallback_phase =
                                crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet;
                            set_state.active_workers = 0;
                            set_state.in_flight_members.clear();
                            set_state.phase = fallback_phase;
                            if let Some(plan) = set_state.plan.as_mut() {
                                plan.phase = fallback_phase;
                            }
                        }
                        self.schedule_rar_capacity_retry(
                            job_id,
                            &set_name,
                            RarCapacityRetryKind::FullSetExtraction,
                        );
                        return;
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
                        self.phase_end_extracting_if_idle(job_id);
                        for (member, _error) in &outcome.failed {
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
                    let settle_result = self
                        .settle_rar_set_after_extraction_worker(job_id, &set_name)
                        .await;
                    self.reconcile_job_progress(job_id).await;
                    match settle_result {
                        RarExtractionSettle::Idle => {
                            self.check_job_completion(job_id).await;
                        }
                        RarExtractionSettle::ActiveWorkersRemain => {
                            self.try_rar_extraction(job_id).await;
                        }
                        RarExtractionSettle::RefreshLaunched {
                            allows_extraction: true,
                        } => {
                            self.try_rar_extraction(job_id).await;
                        }
                        RarExtractionSettle::RefreshLaunched {
                            allows_extraction: false,
                        } => {}
                    }
                }
                Err(e) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %e,
                        "set extraction failed"
                    );
                    let rar_capacity_pressure =
                        self.rar_sets.contains_key(&(job_id, set_name.clone()))
                            && Self::is_capacity_pressure_batch_extraction_error(&e);
                    if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                        set_state.active_workers = 0;
                        set_state.in_flight_members.clear();
                        set_state.extraction_generation =
                            set_state.extraction_generation.saturating_add(1);
                        if rar_capacity_pressure {
                            let fallback_phase =
                                crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet;
                            set_state.phase = fallback_phase;
                            if let Some(plan) = set_state.plan.as_mut() {
                                plan.phase = fallback_phase;
                            }
                        }
                    }
                    self.mark_rar_unlock_priorities_dirty(job_id);
                    if let Some(sets) = self.inflight_extractions.get_mut(&job_id) {
                        sets.remove(&set_name);
                    }
                    if rar_capacity_pressure {
                        self.phase_end_extracting_if_idle(job_id);
                        self.schedule_rar_capacity_retry(
                            job_id,
                            &set_name,
                            RarCapacityRetryKind::FullSetExtraction,
                        );
                        return;
                    }
                    self.purge_empty_rar_set_if_idle(job_id, &set_name);
                    if Self::is_recoverable_full_set_extraction_error(&e) {
                        self.phase_end_extracting_if_idle(job_id);
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
