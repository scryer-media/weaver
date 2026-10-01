use super::direct_store::DirectStoreAdmission;
use super::pressure::{CheckpointAdmission, CheckpointLease};
use super::*;
use crate::operations::metrics::SchedulerBlockClause;

/// The clauses that refused a job's articles during one scan, so the
/// scheduler can say what kept a blocked job from the asking server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::pipeline::download) struct BlockedBy(u16);

impl BlockedBy {
    pub(in crate::pipeline::download) fn only(clause: SchedulerBlockClause) -> Self {
        Self::default().with(clause)
    }

    fn with(self, clause: SchedulerBlockClause) -> Self {
        Self(self.0 | 1 << clause.index())
    }

    pub(in crate::pipeline::download) fn clauses(
        self,
    ) -> impl Iterator<Item = SchedulerBlockClause> {
        SchedulerBlockClause::ALL
            .into_iter()
            .filter(move |clause| self.0 & 1 << clause.index() != 0)
    }
}

/// One sampled answer to "may this server fetch this article of this job?",
/// reusable across a whole queue scan and across every article of one
/// handout. Built by [`Pipeline::servable_work_filter`], which is the only
/// place the clauses are written down; [`ServableWorkFilter::note_taken`]
/// charges each article the handout takes to the byte-budget clauses.
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
    direct_admission: Vec<DirectStoreAdmission>,
    sweep_held: Option<Vec<u32>>,
    checkpoint: CheckpointAdmission,
    /// What the handout being built has taken, as the checkpoint weighs it.
    leased: CheckpointLease,
    /// Set when at least one article was refused *only* by the restart
    /// checkpoint, so a caller that came away empty can tell "held for the
    /// checkpoint" from "nothing here for this server" and schedule the
    /// recheck the checkpoint needs.
    checkpoint_blocked: std::cell::Cell<bool>,
    /// Articles refused so far, and the clauses that refused them; counters
    /// only, never consulted by the answer.
    skipped: std::cell::Cell<u64>,
    refused_by: std::cell::Cell<BlockedBy>,
}

/// What [`Pipeline::servable_work_filter`] found for one job and one server.
pub(in crate::pipeline::download) enum ServableWork<'a> {
    /// Retention rules this server out for the whole job.
    RetentionExcluded,
    /// Every queued file is refused by a clause decided per file, so no
    /// article can pass and there is nothing to scan. Carries the clauses
    /// that refused them.
    NoQueuedFilePasses(BlockedBy),
    /// Some article may pass; scan the queue with this filter.
    Scan(ServableWorkFilter<'a>),
}

impl ServableWorkFilter<'_> {
    pub(in crate::pipeline::download) fn allows(&self, work: &DownloadWork) -> bool {
        let Some(clause) = self.refusal(work) else {
            return true;
        };
        self.skipped.set(self.skipped.get() + 1);
        self.refused_by.set(self.refused_by.get().with(clause));
        false
    }

    /// The first clause that refuses `work`, in a fixed order; the restart
    /// checkpoint is asked last and only of an article every other clause
    /// admits.
    fn refusal(&self, work: &DownloadWork) -> Option<SchedulerBlockClause> {
        let file_index = work.segment_id.file_id.file_index;
        if !self.direct_admission.iter().all(|set| set.allows(work)) {
            return Some(SchedulerBlockClause::DirectStore);
        }
        if self
            .sweep_held
            .as_deref()
            .is_some_and(|held| held.contains(&file_index))
        {
            return Some(SchedulerBlockClause::SweepHeld);
        }
        if work.exclude_servers.contains(&self.server_idx)
            || work.avoid_server == Some(self.server_idx)
        {
            return Some(SchedulerBlockClause::ServerExclusion);
        }
        if !self.fill_servers.as_deref().is_none_or(|fill| {
            fill.iter().all(|server| {
                self.retention_excludes.contains(server)
                    || work.exclude_servers.contains(server)
                    || work.avoid_server == Some(*server)
            })
        }) {
            return Some(SchedulerBlockClause::Backfill);
        }
        if self
            .bootstrap_files
            .is_some_and(|files| !files.contains(&file_index))
        {
            return Some(SchedulerBlockClause::Par2Bootstrap);
        }
        if self
            .uu_cursor_ordinals
            .is_some_and(|cursors| !Pipeline::uu_work_closes_cursor(cursors, work))
        {
            return Some(SchedulerBlockClause::UuCursor);
        }
        if !self
            .checkpoint
            .decision_with_lease(work, self.leased)
            .allows()
        {
            self.checkpoint_blocked.set(true);
            return Some(SchedulerBlockClause::Checkpoint);
        }
        None
    }

    pub(in crate::pipeline::download) fn checkpoint_blocked(&self) -> bool {
        self.checkpoint_blocked.get()
    }

    /// Articles this filter has refused, over every scan it drove.
    pub(in crate::pipeline::download) fn skipped(&self) -> u64 {
        self.skipped.get()
    }

    /// The clauses that refused them.
    pub(in crate::pipeline::download) fn refused_by(&self) -> BlockedBy {
        self.refused_by.get()
    }

    /// Charge an article the handout has just taken, so every later article
    /// of the same handout is admitted against the lease as it now stands:
    /// the per-set disk budgets and header probes, and the restart
    /// checkpoint's projected lead.
    pub(in crate::pipeline::download) fn note_taken(&mut self, work: &DownloadWork) {
        for set in &mut self.direct_admission {
            set.note_leased(work);
        }
        self.leased.add(work);
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
    /// once here, so a whole handout costs one sample rather than one per
    /// article.
    ///
    /// Before the queue is scanned, the clauses that can refuse a whole file
    /// are put to every queued file at once (see
    /// [`Self::queued_file_may_pass`]); when none passes, the answer is
    /// [`ServableWork::NoQueuedFilePasses`] and no scan is owed. Only the
    /// per-set header-probe peeks run ahead of that answer, because a file
    /// holding a probe is never refused by its set's budget.
    ///
    /// The filter starts from an empty lease. A caller cutting a batch passes
    /// each article it takes to [`ServableWorkFilter::note_taken`], so the
    /// byte-budget clauses (per-set disk admission and the restart
    /// checkpoint's undurable lead) see the batch's own projection rather
    /// than only what the actor has already committed.
    pub(in crate::pipeline::download) fn servable_work_filter<'a>(
        &mut self,
        job_id: JobId,
        server_idx: usize,
        bootstrap_files: Option<&'a [u32]>,
        uu_cursor_ordinals: Option<&'a HashMap<NzbFileId, u32>>,
    ) -> ServableWork<'a> {
        let retention_excludes = self.job_retention_excludes(job_id);
        if retention_excludes.contains(&server_idx) {
            return ServableWork::RetentionExcluded;
        }
        let sweep_held = self.demotion_sweep_held_file_indices(job_id);
        let direct_admission = self.direct_store_admission(job_id);
        if let Err(blocked) = self.queued_file_may_pass(
            job_id,
            &direct_admission,
            sweep_held.as_deref(),
            bootstrap_files,
        ) {
            return ServableWork::NoQueuedFilePasses(blocked);
        }
        ServableWork::Scan(ServableWorkFilter {
            server_idx,
            fill_servers: self.backfill_fill_gate(server_idx),
            retention_excludes,
            bootstrap_files,
            uu_cursor_ordinals,
            direct_admission,
            sweep_held,
            checkpoint: self.checkpoint_admission(job_id),
            leased: CheckpointLease::default(),
            checkpoint_blocked: std::cell::Cell::new(false),
            skipped: std::cell::Cell::new(0),
            refused_by: std::cell::Cell::new(BlockedBy::default()),
        })
    }

    /// Whether any queued file of the job could get an article past the
    /// clauses that refuse whole files — a direct-store set's disk budget, a
    /// demotion sweep's holds and the PAR2 index bootstrap — answered from
    /// the queue's per-file counts and smallest queued estimates in
    /// O(files · sets), without looking at an article.
    ///
    /// Only an `Err` is acted on, so this may answer `Ok` freely and must
    /// never answer `Err` while some article would pass. The `Err` names the
    /// clauses that refused the queued files. A set's budget refuses a file
    /// only when even the file's smallest queued article does not fit and no
    /// probe of the set lies in that file
    /// (see [`DirectStoreAdmission::may_admit_from_file`]). The other clauses
    /// cannot refuse a whole file this way: the UU cursor admits the article
    /// at its ordinal; exclusions, rotation hints and the backfill gate are
    /// per article; and the checkpoint's refusals must be observed article by
    /// article for the recheck it owes. Retention is decided for the whole
    /// job before this is asked.
    fn queued_file_may_pass(
        &self,
        job_id: JobId,
        direct_admission: &[DirectStoreAdmission],
        sweep_held: Option<&[u32]>,
        bootstrap_files: Option<&[u32]>,
    ) -> Result<(), BlockedBy> {
        if direct_admission.is_empty() && sweep_held.is_none() && bootstrap_files.is_none() {
            return Ok(());
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(());
        };
        let queue = &state.download_queue;
        let mut counted = 0usize;
        let mut blocked = BlockedBy::default();
        for file_index in (0..state.spec.files.len()).filter_map(|index| u32::try_from(index).ok())
        {
            let file_id = NzbFileId { job_id, file_index };
            let queued = queue.queued_count_for_file(file_id) as usize;
            if queued == 0 {
                continue;
            }
            counted += queued;
            let smallest = queue.min_queued_byte_estimate_for_file(file_id);
            if smallest.is_some_and(|smallest| {
                !direct_admission
                    .iter()
                    .all(|set| set.may_admit_from_file(file_index, smallest))
            }) {
                blocked = blocked.with(SchedulerBlockClause::DirectStore);
            } else if sweep_held.is_some_and(|held| held.contains(&file_index)) {
                blocked = blocked.with(SchedulerBlockClause::SweepHeld);
            } else if bootstrap_files.is_some_and(|files| !files.contains(&file_index)) {
                blocked = blocked.with(SchedulerBlockClause::Par2Bootstrap);
            } else {
                return Ok(());
            }
        }
        // Articles of a file the spec does not list were not looked at; let
        // the scan judge them.
        if counted != queue.len() {
            return Ok(());
        }
        Err(blocked)
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
