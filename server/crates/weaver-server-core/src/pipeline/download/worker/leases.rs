use super::*;

impl Pipeline {
    /// Effective excludes for one lease: the segment's failure ledger plus job
    /// retention excludes, plus the transport-rotation `avoid_server` hint.
    /// The hint only ever shapes selection here — exhaustion accounting reads
    /// the ledger alone, so a transient timeout can never help declare an
    /// article missing.
    fn lease_effective_exclude_servers(
        &mut self,
        job_id: JobId,
        compatibility: &DownloadBatchCompatibility,
    ) -> Vec<usize> {
        let mut effective = self.effective_exclude_servers(job_id, &compatibility.exclude_servers);
        if let Some(avoid) = compatibility.avoid_server
            && !effective.contains(&avoid)
        {
            effective.push(avoid);
        }
        effective
    }

    pub(in crate::pipeline::download::worker) fn reserve_download_work_for_dispatch(
        &mut self,
        job_id: JobId,
        work: DownloadWork,
        stop_on_cap_block: bool,
    ) -> Result<Option<DownloadWork>, DispatchAttempt> {
        if !self.primary_download_within_restart_durable_lead(job_id, &work) {
            let (projected_before_flush, limit) = self
                .restart_durable_lead_block(job_id, &work)
                .expect("blocked durable lead must include projected bytes");
            self.flush_file_progress_batch("download.file_progress.flush.restart_durable_lead");
            if let Some((projected_after_flush, _)) = self.restart_durable_lead_block(job_id, &work)
            {
                let backlog = self.download_pipeline_backlog_for_job(job_id);
                if !backlog.has_durable_catch_up_work() {
                    debug!(
                        job_id = job_id.0,
                        segment = ?work.segment_id,
                        projected_before_flush,
                        projected_after_flush,
                        limit,
                        "dispatch continuing: restart durable lead exceeded but download pipeline is idle"
                    );
                } else {
                    self.metrics
                        .download_restart_durable_lead_blocked_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.download_restart_durable_lead_retry_after.insert(
                        job_id,
                        Instant::now() + DOWNLOAD_RESTART_DURABLE_LEAD_RETRY_DELAY,
                    );
                    debug!(
                        job_id = job_id.0,
                        segment = ?work.segment_id,
                        projected_before_flush,
                        projected_after_flush,
                        limit,
                        active_downloads = backlog.active_downloads,
                        active_connections = backlog.active_connections,
                        active_decodes = backlog.active_decodes,
                        delayed_retries = backlog.delayed_retries,
                        released_results = backlog.released_results,
                        pending_decodes = backlog.pending_decodes,
                        buffered_write_segments = backlog.buffered_write_segments,
                        buffered_write_bytes = backlog.buffered_write_bytes,
                        "dispatch delayed: restart durable lead"
                    );
                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        state.download_queue.push(work);
                    }
                    self.update_queue_metrics();
                    return Ok(None);
                }
            }
        }
        self.download_restart_durable_lead_retry_after
            .remove(&job_id);

        let reservation_estimate = Self::bandwidth_reservation_estimate(work.byte_estimate);
        match self.reserve_bandwidth_for_dispatch(work.segment_id, reservation_estimate) {
            Ok(true) => Ok(Some(work)),
            Ok(false) => {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(work);
                }
                // reserve_bandwidth_for_dispatch marked the cap runtime parked
                // and published the IspCap block state; the mark is sticky, so
                // later usage flushes keep presenting the cap block instead of
                // reverting to None while remaining allowance is nonzero.
                self.update_queue_metrics();
                if stop_on_cap_block {
                    Err(DispatchAttempt::StopAll)
                } else {
                    Ok(None)
                }
            }
            Err(error) => {
                error!(error = %error, "failed to reserve ISP bandwidth for dispatch");
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(work);
                }
                self.update_queue_metrics();
                if stop_on_cap_block {
                    Err(DispatchAttempt::StopAll)
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub(in crate::pipeline::download::worker) fn pop_download_work_for_batch(
        &mut self,
        job_id: JobId,
        selector: Option<DownloadBatchSelector<'_>>,
    ) -> Option<DownloadWork> {
        self.jobs.get_mut(&job_id).and_then(|state| {
            let Some(selector) = selector else {
                return state.download_queue.pop();
            };
            if selector.is_refill() {
                // A refill must not be turned away by whatever happens to sit
                // at the head: the head is exactly what it wants, and when the
                // head is unservable (a differing exclude set) the work behind
                // it still is. Scanning past the head is the slow path only in
                // the degenerate case where the whole queue carries exclusions;
                // ordinary work matches on the first pop.
                state
                    .download_queue
                    .pop_first_matching(|work| selector.matches(work))
            } else {
                state
                    .download_queue
                    .pop_next_matching(|work| selector.matches(work))
            }
        })
    }

    /// Hold payload work only while declared PAR2 indexes are unresolved.
    ///
    /// Checkpoint cuts are fixed when a batch is leased, so an index already
    /// present in the primary queue must publish its grid first. Indexless
    /// recovery discovery stays completion-bounded instead of turning every
    /// optional volume into a pre-download barrier.
    fn par2_metadata_bootstrap_files(&mut self, job_id: JobId) -> Option<Vec<u32>> {
        if self.par2_bypassed.contains(&job_id)
            || self
                .jobs
                .get(&job_id)
                .is_none_or(|state| state.par2_bytes == 0)
        {
            return None;
        }
        if self
            .par2_runtime(job_id)
            .is_some_and(|runtime| runtime.explicit_index_bootstrap_closed)
        {
            return None;
        }

        let files = self
            .par2_metadata_candidate_indices(job_id)
            .into_iter()
            .filter(|(_, is_index, _)| *is_index)
            .filter_map(|(file_index, _, _)| {
                (!self
                    .par2_discovery_state_for_candidate(job_id, file_index)
                    .candidate_probe_is_terminal())
                .then_some(file_index)
            })
            .collect::<Vec<_>>();
        if files.is_empty() {
            self.ensure_par2_runtime(job_id)
                .explicit_index_bootstrap_closed = true;
            None
        } else {
            Some(files)
        }
    }

    /// While bootstrap is active, lease only tracked explicit-index work.
    fn par2_metadata_bootstrap_claims_work(&mut self, job_id: JobId, work: &DownloadWork) {
        let file_index = work.segment_id.file_id.file_index;
        if !matches!(
            self.par2_discovery_state_for_candidate(job_id, file_index),
            Par2DiscoveryState::Unseen
        ) {
            return;
        }
        let filename = self.jobs.get(&job_id).and_then(|state| {
            let file = state.spec.files.get(file_index as usize)?;
            matches!(
                file.role,
                weaver_model::files::FileRole::Par2 { is_index: true, .. }
            )
            .then(|| file.filename.clone())
        });
        let Some(filename) = filename else {
            return;
        };
        let file = self
            .ensure_par2_runtime(job_id)
            .files
            .entry(file_index)
            .or_default();
        file.filename = filename;
        file.metadata_carrier_completion_critical = false;
        file.discovery = Par2DiscoveryState::MetadataCarrierQueued {
            target_set_id: None,
            set_ids: Vec::new(),
        };
    }

    fn pop_download_work_for_par2_bootstrap(
        &mut self,
        job_id: JobId,
        bootstrap_files: Option<&[u32]>,
        selector: Option<DownloadBatchSelector<'_>>,
        selection: DownloadWorkSelection,
        uu_cursor_ordinals: Option<&HashMap<NzbFileId, u32>>,
    ) -> Option<DownloadWork> {
        if bootstrap_files.is_none() {
            if let Some(uu_cursor_ordinals) = uu_cursor_ordinals {
                return self.jobs.get_mut(&job_id).and_then(|state| {
                    let matches = |work: &DownloadWork| {
                        selector.is_none_or(|selector| selector.matches(work))
                            && selection.matches(work)
                            && Self::uu_work_closes_cursor(uu_cursor_ordinals, work)
                    };
                    match selection {
                        DownloadWorkSelection::Any => {
                            state.download_queue.pop_first_matching(matches)
                        }
                        DownloadWorkSelection::CompletionCritical => state
                            .download_queue
                            .pop_first_matching_in_class(true, matches),
                        DownloadWorkSelection::NonCritical => state
                            .download_queue
                            .pop_first_matching_in_class(false, matches),
                    }
                });
            }
            if selection == DownloadWorkSelection::Any {
                return self.pop_download_work_for_batch(job_id, selector);
            }
            let completion_critical = selection == DownloadWorkSelection::CompletionCritical;
            let refill = selector.is_some_and(|selector| selector.is_refill());
            return self.jobs.get_mut(&job_id).and_then(|state| {
                let matches =
                    |work: &DownloadWork| selector.is_none_or(|selector| selector.matches(work));
                if refill {
                    // See `pop_download_work_for_batch`: a refill takes the
                    // head of its class, and looks past it rather than parking.
                    state
                        .download_queue
                        .pop_first_matching_in_class(completion_critical, matches)
                } else {
                    state
                        .download_queue
                        .pop_next_matching_in_class(completion_critical, matches)
                }
            });
        }
        self.jobs.get_mut(&job_id).and_then(|state| {
            let matches = |work: &DownloadWork| {
                bootstrap_files
                    .is_none_or(|files| files.contains(&work.segment_id.file_id.file_index))
                    && selector.is_none_or(|selector| selector.matches(work))
                    && selection.matches(work)
                    && uu_cursor_ordinals
                        .is_none_or(|cursors| Self::uu_work_closes_cursor(cursors, work))
            };
            match selection {
                DownloadWorkSelection::Any => state.download_queue.pop_first_matching(matches),
                DownloadWorkSelection::CompletionCritical => state
                    .download_queue
                    .pop_first_matching_in_class(true, matches),
                DownloadWorkSelection::NonCritical => state
                    .download_queue
                    .pop_first_matching_in_class(false, matches),
            }
        })
    }

    pub(in crate::pipeline::download::worker) fn try_lease_initial_download_batch(
        &mut self,
        job_id: JobId,
        pressure: DownloadPressure,
        selection: DownloadWorkSelection,
    ) -> Result<Option<DownloadBatchLease>, DispatchAttempt> {
        let par2_metadata_bootstrap_files = self.par2_metadata_bootstrap_files(job_id);
        let uu_cursor_ordinals = pressure
            .uu_spool_admission_capped
            .then(|| self.uu_spool_cursor_ordinals());
        let Some(first) = self.pop_download_work_for_par2_bootstrap(
            job_id,
            par2_metadata_bootstrap_files.as_deref(),
            None,
            selection,
            uu_cursor_ordinals.as_ref(),
        ) else {
            return Ok(None);
        };
        if par2_metadata_bootstrap_files.is_some() {
            self.par2_metadata_bootstrap_claims_work(job_id, &first);
        }
        if !first.is_recovery && !self.normal_download_connection_capacity_available() {
            if let Some(state) = self.jobs.get_mut(&job_id) {
                state.download_queue.push(first);
            }
            return Ok(None);
        }
        let Some(first) = self.reserve_download_work_for_dispatch(job_id, first, true)? else {
            return Ok(None);
        };

        let lane_mode = self.choose_download_lane_mode(job_id, first.is_recovery, pressure);
        let compatibility = DownloadBatchCompatibility::from_work(&first);
        Ok(Some(self.finish_download_batch_lease(
            lane_mode,
            compatibility,
            first,
            pressure,
            DownloadBatchRule::Initial,
            par2_metadata_bootstrap_files.as_deref(),
        )))
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn try_lease_initial_download_batch_for_test(
        &mut self,
        job_id: JobId,
        pressure: DownloadPressure,
    ) -> Option<DownloadBatchLease> {
        match self.try_lease_initial_download_batch(job_id, pressure, DownloadWorkSelection::Any) {
            Ok(lease) => lease,
            Err(_) => panic!("test lease must not hit a dispatch policy stop"),
        }
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn try_lease_refill_download_batch_for_test(
        &mut self,
        job_id: JobId,
        compatibility: DownloadBatchCompatibility,
        pressure: DownloadPressure,
    ) -> Option<DownloadBatchLease> {
        match self.try_lease_refill_download_batch(job_id, 0, compatibility, pressure) {
            Ok(lease) => lease,
            Err(_) => panic!("test lease must not hit a dispatch policy stop"),
        }
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn try_lease_ip_replacement_trial_batch_for_test(
        &mut self,
        job_id: JobId,
        server_idx: usize,
    ) -> Option<DownloadBatchLease> {
        match self.try_lease_ip_replacement_trial_batch(job_id, server_idx) {
            Ok(lease) => lease,
            Err(_) => panic!("test lease must not hit a dispatch policy stop"),
        }
    }

    /// Soft pressure must not strip an established lane of its proven pipeline
    /// depth on refill: sequential-mode batches add a full round-trip per body,
    /// which costs more than the backlog it protects. The reduced refill runway
    /// (see `download_lane_lease_work_limit`) is the soft-pressure throttle.
    ///
    /// Two stages, so a live connection is never sent away while its job has
    /// work it could serve:
    ///
    /// 1. Take the head of the lane's class under the refill rule, which no
    ///    longer asks about priority or groups.
    /// 2. If nothing there matched — the queue head carries a different
    ///    exclude set — re-open the lease around the first queued work this
    ///    lane's own server is allowed to fetch, with that work's own
    ///    compatibility. The lane carries the new compatibility forward, so
    ///    the excludes each result reports stay the ones its article was
    ///    leased under.
    pub(in crate::pipeline::download::worker) fn try_lease_refill_download_batch(
        &mut self,
        job_id: JobId,
        server_idx: usize,
        compatibility: DownloadBatchCompatibility,
        pressure: DownloadPressure,
    ) -> Result<Option<DownloadBatchLease>, DispatchAttempt> {
        let par2_metadata_bootstrap_files = self.par2_metadata_bootstrap_files(job_id);
        let uu_cursor_ordinals = pressure
            .uu_spool_admission_capped
            .then(|| self.uu_spool_cursor_ordinals());
        let lane_mode = self.choose_download_lane_mode(
            job_id,
            compatibility.is_recovery,
            Self::refill_mode_pressure(pressure),
        );
        let selection = if compatibility.completion_critical {
            DownloadWorkSelection::CompletionCritical
        } else {
            DownloadWorkSelection::NonCritical
        };
        let mut compatibility = compatibility;
        let rule = DownloadBatchRule::Refill {
            match_groups: self.server_needs_group_prologue(server_idx),
        };
        let first = match self.pop_download_work_for_par2_bootstrap(
            job_id,
            par2_metadata_bootstrap_files.as_deref(),
            Some(DownloadBatchSelector::new(&compatibility, rule)),
            selection,
            uu_cursor_ordinals.as_ref(),
        ) {
            Some(first) => first,
            None => {
                let Some(first) = self.pop_refill_work_servable_by_lane(
                    job_id,
                    server_idx,
                    &compatibility,
                    rule,
                    par2_metadata_bootstrap_files.as_deref(),
                    selection,
                    uu_cursor_ordinals.as_ref(),
                ) else {
                    return Ok(None);
                };
                compatibility = DownloadBatchCompatibility::from_work(&first);
                first
            }
        };
        if par2_metadata_bootstrap_files.is_some() {
            self.par2_metadata_bootstrap_claims_work(job_id, &first);
        }
        let Some(first) = self.reserve_download_work_for_dispatch(job_id, first, false)? else {
            return Ok(None);
        };

        Ok(Some(self.finish_download_batch_lease(
            lane_mode,
            compatibility,
            first,
            pressure,
            rule,
            par2_metadata_bootstrap_files.as_deref(),
        )))
    }

    /// Whether `server_idx` has proven it refuses a message-id fetch without
    /// a selected group, so a lane on it was opened with `GROUP` and may only
    /// be refilled from that group.
    fn server_needs_group_prologue(&self, server_idx: usize) -> bool {
        self.nntp
            .pool()
            .server_configs()
            .get(server_idx)
            .is_some_and(|config| {
                weaver_nntp::server_caps::requires_group_selection(&config.host, config.port)
            })
    }

    /// Stage two of a refill: the first queued work of this lane's class that
    /// `server_idx` is actually allowed to fetch.
    ///
    /// The class and the recovery flag still hold — a lane is counted under
    /// both for its whole life — but the exclude set is allowed to differ,
    /// because a differing exclude set is a statement about *other* servers.
    /// The lane's own server must be clear of the work's failure exclusions,
    /// its rotation hint, and the job's retention exclusions; otherwise this
    /// lane genuinely cannot serve it and the queue is left alone. The group
    /// question is the rule's, exactly as in stage one.
    #[allow(clippy::too_many_arguments)]
    fn pop_refill_work_servable_by_lane(
        &mut self,
        job_id: JobId,
        server_idx: usize,
        compatibility: &DownloadBatchCompatibility,
        rule: DownloadBatchRule,
        bootstrap_files: Option<&[u32]>,
        selection: DownloadWorkSelection,
        uu_cursor_ordinals: Option<&HashMap<NzbFileId, u32>>,
    ) -> Option<DownloadWork> {
        let retention_excludes = self.job_retention_excludes(job_id);
        if retention_excludes.contains(&server_idx) {
            return None;
        }
        let is_recovery = compatibility.is_recovery;
        let completion_critical = compatibility.completion_critical;
        let match_groups = matches!(rule, DownloadBatchRule::Refill { match_groups: true });
        let groups = compatibility.groups.clone();
        self.jobs.get_mut(&job_id).and_then(|state| {
            let matches = |work: &DownloadWork| {
                work.is_recovery == is_recovery
                    && work.completion_critical == completion_critical
                    && !work.exclude_servers.contains(&server_idx)
                    && work.avoid_server != Some(server_idx)
                    && (!match_groups
                        || std::sync::Arc::ptr_eq(&work.groups, &groups)
                        || work.groups == groups)
                    && bootstrap_files
                        .is_none_or(|files| files.contains(&work.segment_id.file_id.file_index))
                    && selection.matches(work)
                    && uu_cursor_ordinals
                        .is_none_or(|cursors| Self::uu_work_closes_cursor(cursors, work))
            };
            match selection {
                DownloadWorkSelection::Any => state.download_queue.pop_first_matching(matches),
                DownloadWorkSelection::CompletionCritical => state
                    .download_queue
                    .pop_first_matching_in_class(true, matches),
                DownloadWorkSelection::NonCritical => state
                    .download_queue
                    .pop_first_matching_in_class(false, matches),
            }
        })
    }

    pub(in crate::pipeline::download::worker) fn try_lease_ip_replacement_trial_batch(
        &mut self,
        job_id: JobId,
        server_idx: usize,
    ) -> Result<Option<DownloadBatchLease>, DispatchAttempt> {
        if self.refresh_download_pressure().uu_spool_admission_capped {
            return Ok(None);
        }
        let par2_metadata_bootstrap_files = self.par2_metadata_bootstrap_files(job_id);
        let Some(first) = self.pop_download_work_for_par2_bootstrap(
            job_id,
            par2_metadata_bootstrap_files.as_deref(),
            None,
            DownloadWorkSelection::NonCritical,
            None,
        ) else {
            return Ok(None);
        };
        if par2_metadata_bootstrap_files.is_some() {
            self.par2_metadata_bootstrap_claims_work(job_id, &first);
        }
        // A segment that just transport-failed on this server must not be its
        // IP-replacement probe either — and its avoid hint would land in the
        // lease's effective excludes, fighting the trial's own target.
        if first.is_recovery
            || first.exclude_servers.contains(&server_idx)
            || first.avoid_server == Some(server_idx)
        {
            if let Some(state) = self.jobs.get_mut(&job_id) {
                state.download_queue.push(first);
            }
            return Ok(None);
        }

        let Some(first) = self.reserve_download_work_for_dispatch(job_id, first, true)? else {
            return Ok(None);
        };
        let compatibility = DownloadBatchCompatibility::from_work(&first);
        let effective_exclude_servers =
            self.lease_effective_exclude_servers(job_id, &compatibility);
        if compatibility.is_recovery {
            let lease = DownloadBatchLease {
                job_id,
                runtime_generation: self.pool_generation,
                lane_mode: DownloadLaneMode::Sequential,
                spillover_loan_kind: None,
                server_modes: Vec::new(),
                compatibility,
                effective_exclude_servers,
                checkpoint_plan: self.par2_checkpoint_plan(job_id),
                pressure_clear: false,
                works: vec![first],
            };
            self.rollback_download_batch_lease(lease);
            return Ok(None);
        }

        let mut works = vec![first];
        while works.len() < IP_REPLACEMENT_TRIAL_SAMPLES {
            let Some(next) = self.pop_download_work_for_par2_bootstrap(
                job_id,
                par2_metadata_bootstrap_files.as_deref(),
                Some(DownloadBatchSelector::initial(&compatibility)),
                DownloadWorkSelection::NonCritical,
                None,
            ) else {
                break;
            };
            if par2_metadata_bootstrap_files.is_some() {
                self.par2_metadata_bootstrap_claims_work(job_id, &next);
            }
            if next.is_recovery {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(next);
                }
                break;
            }
            match self.reserve_download_work_for_dispatch(job_id, next, false) {
                Ok(Some(next)) => works.push(next),
                Ok(None) | Err(DispatchAttempt::StopAll) | Err(DispatchAttempt::NoWork) => break,
                Err(DispatchAttempt::Dispatched) => unreachable!("reserve helper never dispatches"),
            }
        }

        let lease = DownloadBatchLease {
            job_id,
            runtime_generation: self.pool_generation,
            lane_mode: DownloadLaneMode::Sequential,
            spillover_loan_kind: None,
            server_modes: Vec::new(),
            compatibility,
            effective_exclude_servers,
            checkpoint_plan: self.par2_checkpoint_plan(job_id),
            pressure_clear: false,
            works,
        };
        if lease.works.len() < IP_REPLACEMENT_TRIAL_SAMPLES {
            self.rollback_download_batch_lease(lease);
            return Ok(None);
        }

        Ok(Some(lease))
    }

    pub(in crate::pipeline::download::worker) fn finish_download_batch_lease(
        &mut self,
        lane_mode: DownloadLaneMode,
        compatibility: DownloadBatchCompatibility,
        first: DownloadWork,
        pressure: DownloadPressure,
        rule: DownloadBatchRule,
        par2_metadata_bootstrap_files: Option<&[u32]>,
    ) -> DownloadBatchLease {
        let job_id = first.segment_id.file_id.job_id;
        let refill = rule.is_refill();
        // Rate reservations are activated after the lease is finalized. Keep
        // limited leases single-work so every subsequent BODY refill observes
        // the updated token balance instead of pre-leasing past the limit.
        let work_limit =
            if self.rate_limiter.is_limited() {
                1
            } else {
                let runway = self.download_lane_lease_work_limit(
                    job_id,
                    lane_mode,
                    pressure,
                    refill,
                    first.byte_estimate,
                );
                if par2_metadata_bootstrap_files.is_some() {
                    runway
                } else {
                    // Runway sizing decides how much work a lane may hold; the
                    // fair share decides how much of the job's remainder one lane
                    // may take, so every lane of the job finishes within about an
                    // article of the others instead of one lane draining the tail
                    // alone.
                    runway.min(self.download_lane_fair_share_work_limit(
                        job_id,
                        compatibility.completion_critical,
                    ))
                }
            };
        let cap_for_restart_durable_lead = self.should_cap_lease_for_restart_durable_lead(job_id);
        let mut leased_undurable_bytes = if first.is_recovery {
            0
        } else {
            first.byte_estimate as u64
        };
        let mut works = vec![first];
        let selection = if compatibility.completion_critical {
            DownloadWorkSelection::CompletionCritical
        } else {
            DownloadWorkSelection::NonCritical
        };
        // The rule the lease was opened under has to hold for the whole batch:
        // a refill that took the head under the refill rule and then filled
        // under the initial rule would stop at the first priority change, which
        // is precisely the boundary it exists to cross.
        let selector = DownloadBatchSelector::new(&compatibility, rule);
        while works.len() < work_limit {
            let Some(next) = self.pop_download_work_for_par2_bootstrap(
                job_id,
                par2_metadata_bootstrap_files,
                Some(selector),
                selection,
                None,
            ) else {
                break;
            };
            if par2_metadata_bootstrap_files.is_some() {
                self.par2_metadata_bootstrap_claims_work(job_id, &next);
            }
            if cap_for_restart_durable_lead
                && self
                    .restart_durable_lead_block_with_extra(job_id, &next, leased_undurable_bytes)
                    .is_some()
            {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(next);
                }
                break;
            }
            match self.reserve_download_work_for_dispatch(job_id, next, false) {
                Ok(Some(next)) => {
                    if !next.is_recovery {
                        leased_undurable_bytes =
                            leased_undurable_bytes.saturating_add(next.byte_estimate as u64);
                    }
                    works.push(next);
                }
                Ok(None) | Err(DispatchAttempt::StopAll) | Err(DispatchAttempt::NoWork) => break,
                Err(DispatchAttempt::Dispatched) => unreachable!("reserve helper never dispatches"),
            }
        }

        let server_modes_pressure = if refill {
            Self::refill_mode_pressure(pressure)
        } else {
            pressure
        };
        let server_modes = self.download_lane_server_modes(
            job_id,
            compatibility.is_recovery,
            server_modes_pressure,
        );
        let effective_exclude_servers =
            self.lease_effective_exclude_servers(job_id, &compatibility);
        DownloadBatchLease {
            job_id,
            runtime_generation: self.pool_generation,
            lane_mode,
            spillover_loan_kind: None,
            server_modes,
            compatibility,
            effective_exclude_servers,
            checkpoint_plan: self.par2_checkpoint_plan(job_id),
            pressure_clear: pressure.state == DownloadPressureState::Clear,
            works,
        }
    }

    /// How many lanes this job's remaining work will actually be spread over.
    ///
    /// The count has to anticipate the lanes dispatch is about to start, not
    /// only the ones already running. At a job's first wave, and at every
    /// promotion — where every lane of the job is parked on `NoWork` and the
    /// live count is zero — sizing a lease by the running lanes alone hands
    /// the whole set to the first lane and leaves the rest parked. That is
    /// how one connection ended up fetching an entire promoted recovery set
    /// while seven lanes idled.
    ///
    /// Completion-critical work has no lane cap (see
    /// `dispatch_completion_critical_work`) and the hot job fills every free
    /// connection in its own phase, so both may count the free capacity
    /// dispatch is about to hand them. Any other job only keeps the lanes it
    /// already holds.
    ///
    /// The two counts are combined with `max`, never added: a lane that is
    /// already running holds a share of the remainder, and counting it
    /// alongside a still-free connection would divide the same remainder
    /// twice and shrink mid-job leases for no benefit.
    fn download_lane_fair_share_lanes(&self, job_id: JobId, completion_critical: bool) -> usize {
        let active = self
            .active_download_connections_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0);
        if !completion_critical && self.hot_dispatch_job != Some(job_id) {
            return active.max(1);
        }
        let capacity = self
            .effective_download_connection_capacity(self.tuner.params().max_concurrent_downloads);
        // A lane being dispatched has not been counted as active yet, so the
        // free capacity already includes this lease's own connection.
        let free = capacity.saturating_sub(self.active_download_connections);
        active.max(free).max(1)
    }

    /// Remaining articles this lease's class may still take for `job_id`.
    ///
    /// O(1) — a heap length, never a queue scan: leases are cut on every
    /// refill.
    fn job_remaining_leasable_work(&self, job_id: JobId, completion_critical: bool) -> usize {
        self.jobs
            .get(&job_id)
            .map(|state| state.download_queue.len_in_class(completion_critical))
            .unwrap_or(0)
    }

    /// The tail bound on a lease: one lane's fair share of what the job has
    /// left.
    ///
    /// Runway sizing alone leaves no end-of-job rebalancing, so the lane that
    /// happens to lease last drains its whole batch alone while every other
    /// lane of the job has already finished. The spread is invisible at zero
    /// latency and grows with the round trip; at 100 ms it was seconds of a
    /// job's tail spent on one connection.
    ///
    /// Shrinking a lease never reorders the queue, so the volume-frontier
    /// ordering `hot_lease_work_limit` protects is untouched: a lane still
    /// takes the head of the queue, just less of it. The bound only bites
    /// once the remaining work no longer fills every lane's runway, and going
    /// below a lane's pipeline depth there costs no round trip — the work
    /// that would have deepened one lane's batch is in another lane's batch,
    /// in flight at the same time.
    ///
    /// PAR2 index bootstrap is exempt: that window leases only the declared
    /// explicit indexes, a bounded barrier set that is claimed in one batch so
    /// the grid publishes before payload leases cut their checkpoints. It is
    /// not a tail, and splitting it would reopen the barrier, not shorten it.
    fn download_lane_fair_share_work_limit(
        &self,
        job_id: JobId,
        completion_critical: bool,
    ) -> usize {
        let remaining = self.job_remaining_leasable_work(job_id, completion_critical);
        let lanes = self.download_lane_fair_share_lanes(job_id, completion_critical);
        // +1: the lease's first work item is already out of the queue and is
        // part of this lane's share.
        remaining.saturating_add(1).div_ceil(lanes).max(1)
    }

    pub(in crate::pipeline::download::worker) fn download_lane_lease_work_limit(
        &mut self,
        job_id: JobId,
        lane_mode: DownloadLaneMode,
        pressure: DownloadPressure,
        refill: bool,
        article_bytes: u32,
    ) -> usize {
        if pressure.uu_spool_admission_capped {
            return 1;
        }
        if self.hot_dispatch_job == Some(job_id) {
            match pressure.state {
                DownloadPressureState::Clear => {
                    return self.hot_lease_work_limit(job_id, lane_mode, article_bytes);
                }
                // Refills keep an established lane on its full runway under soft
                // pressure; hard pressure is the flow control (those requests are
                // deferred until the backlog drains, see
                // handle_download_lane_refill_request). Initial dispatch under soft
                // pressure stays a minimal probe because it adds a new connection
                // to an already-loaded pipeline.
                DownloadPressureState::Soft if refill => {
                    return self.hot_lease_work_limit(job_id, lane_mode, article_bytes);
                }
                DownloadPressureState::Soft | DownloadPressureState::Hard => {}
            }
        }
        lane_mode.max_depth()
    }

    /// Hot-lane lease size in articles, bounded by a time-based runway.
    ///
    /// A flat article count sizes the in-flight window by bandwidth: at full
    /// caps, `lanes x 64` articles are committed the moment a job starts,
    /// which on a slow link leases several RAR volumes' worth of work at once
    /// and spreads bandwidth evenly across them, so no volume finishes early.
    /// Incremental extraction needs the earliest volumes to finish first, so
    /// cap each lease near HOT_LEASE_TARGET_RUNWAY_SECS of the lane's
    /// measured throughput: fast lanes keep full batches, slow lanes cycle
    /// back to the queue head, which base priorities and unlock boosts keep
    /// pointed at the volume frontier.
    pub(in crate::pipeline) fn hot_lease_work_limit(
        &mut self,
        job_id: JobId,
        lane_mode: DownloadLaneMode,
        article_bytes: u32,
    ) -> usize {
        let full = HOT_CLEAR_PRESSURE_LANE_LEASE_WORK_LIMIT.max(lane_mode.max_depth());
        if article_bytes == 0 {
            return full;
        }
        let speed_bps = self.hot_dispatch_speed_bps(Instant::now());
        if speed_bps == 0 {
            // No measured throughput yet (fresh hot job or a stall): the
            // first dispatch wave must not lease several volumes' worth of
            // articles blind — on a slow link those leases take minutes to
            // drain before any runway discipline applies. A quarter batch
            // bounds cold-start refill churn until the window fills.
            return HOT_LEASE_COLD_START_WORK_LIMIT
                .max(lane_mode.max_depth())
                .min(full);
        }
        // The throughput window tracks hot-job primary bytes, so divide by the
        // hot job's own lanes; the global count only stands in before the
        // per-job entry exists.
        let lanes = self
            .active_download_connections_by_job
            .get(&job_id)
            .copied()
            .filter(|count| *count > 0)
            .unwrap_or_else(|| self.active_download_connections.max(1)) as u64;
        let runway_bytes = (speed_bps / lanes).saturating_mul(HOT_LEASE_TARGET_RUNWAY_SECS);
        let articles = (runway_bytes / u64::from(article_bytes)) as usize;
        articles.clamp(lane_mode.max_depth(), full)
    }

    pub(in crate::pipeline::download) fn actual_download_lane_mode(
        lease_mode: DownloadLaneMode,
        server_modes: &[(usize, DownloadLaneMode)],
        server_idx: usize,
        supports_pipelining: bool,
    ) -> DownloadLaneMode {
        let server_mode = server_modes
            .iter()
            .find_map(|(idx, mode)| (*idx == server_idx).then_some(*mode))
            .unwrap_or(DownloadLaneMode::Sequential);
        if !supports_pipelining || server_mode == DownloadLaneMode::Sequential {
            return DownloadLaneMode::Sequential;
        }
        if server_mode.max_depth() <= lease_mode.max_depth() {
            server_mode
        } else {
            lease_mode
        }
    }

    pub(in crate::pipeline::download::worker) fn activate_download_batch_lease(
        &mut self,
        lease: &DownloadBatchLease,
        activation_items: &[(SegmentId, NzbFileId, u64)],
        starts_connection: bool,
    ) {
        self.activate_download_batch(
            lease.job_id,
            DownloadBatchClass::from(&lease.compatibility),
            lease.lane_mode,
            lease.works.len(),
            activation_items,
            starts_connection,
        );
    }

    pub(in crate::pipeline::download::worker) fn activate_download_batch(
        &mut self,
        job_id: JobId,
        batch_class: DownloadBatchClass,
        lane_mode: DownloadLaneMode,
        work_count: usize,
        activation_items: &[(SegmentId, NzbFileId, u64)],
        starts_connection: bool,
    ) {
        if work_count == 0 {
            return;
        }

        self.active_downloads += work_count;
        self.metrics
            .download_lane_lease_items_total
            .fetch_add(work_count as u64, Ordering::Relaxed);
        if starts_connection {
            if self.hot_dispatch_job == Some(job_id) {
                let now = Instant::now();
                let speed = self.hot_dispatch_speed_bps(now);
                self.hot_dispatch_expansion_window
                    .record(now, HotExpansionKind::LaneStart, speed);
            }
            self.active_download_connections += 1;
            self.note_download_lane_started(lane_mode);
            *self
                .active_download_connections_by_job
                .entry(job_id)
                .or_default() += 1;
            if batch_class.completion_critical {
                self.active_completion_critical_connections += 1;
                *self
                    .active_completion_critical_connections_by_job
                    .entry(job_id)
                    .or_default() += 1;
            }
        }
        if batch_class.is_recovery {
            self.active_recovery += work_count;
        }
        *self.active_downloads_by_job.entry(job_id).or_default() += work_count;
        for (segment_id, file_id, estimate) in activation_items {
            *self.active_downloads_by_file.entry(*file_id).or_default() += 1;
            self.reserve_rate_limit_for_dispatch(*segment_id, *estimate);
        }
        self.mark_download_pass_started(job_id);
        self.publish_active_stage_metrics();
    }
}
