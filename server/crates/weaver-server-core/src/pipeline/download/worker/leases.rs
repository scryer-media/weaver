use super::direct_store::DirectStoreAdmission;
use super::pressure::CheckpointAdmission;
use super::*;

/// One sampled answer to "may this server fetch this article of this job?",
/// reusable across a whole queue scan. Built by
/// [`Pipeline::servable_work_filter`], which is the only place the clauses
/// are written down.
pub(in crate::pipeline::download) struct ServableWorkFilter<'a> {
    server_idx: usize,
    /// Set when `server_idx` is a backfill server: the fill servers that are
    /// available right now. A backfill server only takes an article every one
    /// of those cannot fetch — by the job's retention, the article's own
    /// exclusions, or its rotation hint — which is what keeps backfill traffic
    /// to what the fill tier has already given up on.
    fill_servers: Option<Vec<usize>>,
    retention_excludes: Arc<Vec<usize>>,
    bootstrap_files: Option<&'a [u32]>,
    uu_cursor_ordinals: Option<&'a HashMap<NzbFileId, u32>>,
    leased: &'a [DownloadWork],
    direct_admission: Vec<DirectStoreAdmission>,
    sweep_held: Option<Vec<u32>>,
    checkpoint: CheckpointAdmission,
    /// Set when at least one article was refused *only* by the restart
    /// checkpoint, so a caller that came away empty can tell "held for the
    /// checkpoint" from "nothing here for this server" and schedule the
    /// recheck the checkpoint needs.
    checkpoint_blocked: std::cell::Cell<bool>,
}

impl ServableWorkFilter<'_> {
    pub(in crate::pipeline::download) fn allows(&self, work: &DownloadWork) -> bool {
        self.direct_admission.iter().all(|set| set.allows(work))
            && self
                .sweep_held
                .as_deref()
                .is_none_or(|held| !held.contains(&work.segment_id.file_id.file_index))
            && !work.exclude_servers.contains(&self.server_idx)
            && work.avoid_server != Some(self.server_idx)
            && self.fill_servers.as_deref().is_none_or(|fill| {
                fill.iter().all(|server| {
                    self.retention_excludes.contains(server)
                        || work.exclude_servers.contains(server)
                        || work.avoid_server == Some(*server)
                })
            })
            && self
                .bootstrap_files
                .is_none_or(|files| files.contains(&work.segment_id.file_id.file_index))
            && self
                .uu_cursor_ordinals
                .is_none_or(|cursors| Pipeline::uu_work_closes_cursor(cursors, work))
            && {
                let allowed = self.checkpoint.decision(work, self.leased).allows();
                if !allowed {
                    self.checkpoint_blocked.set(true);
                }
                allowed
            }
    }

    pub(in crate::pipeline::download) fn checkpoint_blocked(&self) -> bool {
        self.checkpoint_blocked.get()
    }
}

impl Pipeline {
    pub(in crate::pipeline::download::worker) fn reserve_download_work_for_dispatch(
        &mut self,
        job_id: JobId,
        work: DownloadWork,
        stop_on_cap_block: bool,
    ) -> Result<Option<DownloadWork>, DispatchAttempt> {
        if !self.primary_download_within_restart_durable_lead(job_id, &work) {
            self.flush_file_progress_batch("download.file_progress.flush.restart_durable_lead");
            if !self.primary_download_within_restart_durable_lead(job_id, &work) {
                self.note_checkpoint_dispatch_block(job_id);
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(work);
                }
                self.update_queue_metrics();
                return Ok(None);
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

    /// Hold payload work only while declared PAR2 indexes are unresolved.
    ///
    /// Checkpoint cuts are fixed when a batch is leased, so an index already
    /// present in the primary queue must publish its grid first. Indexless
    /// recovery discovery stays completion-bounded instead of turning every
    /// optional volume into a pre-download barrier.
    pub(in crate::pipeline::download) fn par2_metadata_bootstrap_files(
        &mut self,
        job_id: JobId,
    ) -> Option<Vec<u32>> {
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
    pub(in crate::pipeline::download) fn par2_metadata_bootstrap_claims_work(
        &mut self,
        job_id: JobId,
        work: &DownloadWork,
    ) {
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

    /// The single definition of "this server may fetch this queued article
    /// right now", for one job.
    ///
    /// Both selection paths ask the same question, and they must not be able
    /// to answer it differently: one of them pops work onto a live connection
    /// and the other decides whether a connection should be given work at
    /// all, so a drift between them shows up as either an idle link or an
    /// article handed to a server that cannot serve it. Everything the answer
    /// depends on — retention, per-set disk admission, a demotion sweep's
    /// held files, the restart checkpoint, the PAR2 index bootstrap, the UU
    /// spool cursor, the work's own exclusions and rotation hint — is sampled
    /// once here, so a whole scan of a queue costs one sample rather than one
    /// per article.
    ///
    /// `None` means retention already rules this server out for the whole
    /// job; there is nothing to scan.
    ///
    /// `leased` is the work already taken in the batch being built, so the
    /// byte-budget clauses (per-set disk admission and the restart
    /// checkpoint's undurable lead) see the batch's own projection rather
    /// than only what the actor has already committed.
    pub(in crate::pipeline::download) fn servable_work_filter<'a>(
        &mut self,
        job_id: JobId,
        server_idx: usize,
        bootstrap_files: Option<&'a [u32]>,
        uu_cursor_ordinals: Option<&'a HashMap<NzbFileId, u32>>,
        leased: &'a [DownloadWork],
    ) -> Option<ServableWorkFilter<'a>> {
        let retention_excludes = self.job_retention_excludes(job_id);
        if retention_excludes.contains(&server_idx) {
            return None;
        }
        Some(ServableWorkFilter {
            server_idx,
            fill_servers: self.backfill_fill_gate(server_idx),
            retention_excludes,
            bootstrap_files,
            uu_cursor_ordinals,
            leased,
            direct_admission: self.direct_store_admission(job_id, leased),
            sweep_held: self.demotion_sweep_held_file_indices(job_id),
            checkpoint: self.checkpoint_admission(job_id),
            checkpoint_blocked: std::cell::Cell::new(false),
        })
    }

    /// The fill servers a backfill server must see exhausted before it takes
    /// an article; `None` for a fill server, and for a backfill server once
    /// the pool has already unlocked the backfill tier for everyone.
    fn backfill_fill_gate(&self, server_idx: usize) -> Option<Vec<usize>> {
        let flags = self.nntp.pool().server_backfill_flags();
        if !flags.get(server_idx).copied().unwrap_or(false) {
            return None;
        }
        match self.nntp.blocking_body_server_order(&[]) {
            Some(order) => {
                if order.iter().any(|server| flags[server.0]) {
                    // The fill tier is exhausted or auth-disabled for the
                    // whole pool; backfill serves everything.
                    return None;
                }
                Some(order.into_iter().map(|server| server.0).collect())
            }
            // The ranking is contended: gate on every fill server, which is
            // the strict answer and costs at most one pass.
            None => Some((0..flags.len()).filter(|idx| !flags[*idx]).collect()),
        }
    }

    /// The UU spool cursors a selection pass must respect, sampled once.
    ///
    /// Only a capped spool constrains selection; below the cap every encoding
    /// dispatches freely and the map is not worth building.
    pub(in crate::pipeline::download) fn selection_uu_cursor_ordinals(
        &self,
        pressure: DownloadPressure,
    ) -> Option<HashMap<NzbFileId, u32>> {
        pressure
            .uu_spool_admission_capped
            .then(|| self.uu_spool_cursor_ordinals())
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
        if let Some(segment_id) = self.checkpoint_progress_article_for_lease(lease) {
            self.checkpoint_progress_articles
                .insert(lease.job_id, (lease.lane_id, segment_id));
        }
        self.book_download_lane_owner(lease, starts_connection);
        self.activate_download_batch(
            lease.job_id,
            lease.works.iter().filter(|work| work.is_recovery).count(),
            lease.completion_critical,
            lease.lane_mode,
            activation_items,
            starts_connection,
        );
    }

    pub(in crate::pipeline) fn activate_download_batch(
        &mut self,
        job_id: JobId,
        recovery_count: usize,
        completion_critical: bool,
        lane_mode: DownloadLaneMode,
        activation_items: &[(SegmentId, NzbFileId, u64)],
        starts_connection: bool,
    ) {
        let work_count = activation_items.len();
        if work_count == 0 {
            return;
        }

        self.active_downloads += work_count;
        self.metrics
            .download_lane_lease_items_total
            .fetch_add(work_count as u64, Ordering::Relaxed);
        if starts_connection {
            self.active_download_connections += 1;
            self.note_download_lane_started(lane_mode);
            *self
                .active_download_connections_by_job
                .entry(job_id)
                .or_default() += 1;
            if completion_critical {
                self.book_completion_critical_connection(job_id);
            }
        }
        self.active_recovery += recovery_count;
        *self.active_downloads_by_job.entry(job_id).or_default() += work_count;
        for (segment_id, file_id, estimate) in activation_items {
            *self.active_downloads_by_file.entry(*file_id).or_default() += 1;
            self.reserve_rate_limit_for_dispatch(*segment_id, *estimate);
        }
        self.mark_download_pass_started(job_id);
        self.publish_active_stage_metrics();
    }
}
