// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::pressure::{CheckpointAdmission, CheckpointLease};
use super::*;
use crate::operations::metrics::SchedulerBlockClause;

// The clauses that refused a job's articles during one scan, so the
// scheduler can say what kept a blocked job from the asking server.
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

// One sampled answer to "may this server fetch this article of this job?",
// reusable across a whole queue scan and across every article of one
// handout. Built by [`Pipeline::servable_work_filter`], which is the only
// place the clauses are written down; [`ServableWorkFilter::note_taken`]
// charges each article the handout takes to the byte-budget clauses.
pub(in crate::pipeline::download) struct ServableWorkFilter<'a> {
    server_idx: usize,
    // The pass's one sample of the pool's provider policy, shared by every
    // filter built in that pass.
    provider_policy: Arc<weaver_nntp::provider_policy::ProviderPolicy>,
    // The fill servers a hinted article could go to instead of its hinted
    // one: the dispatch attempt's available fill servers, or a fresh ranking
    // when the handout is not part of one. The rotation hint is a
    // preference: it refuses the hinted server only while one of these the
    // article may use is available, so an article whose only alternative is
    // down is still served by the server it failed on.
    hint_alternatives: Vec<usize>,
    retention_excludes: Arc<Vec<usize>>,
    bootstrap_files: Option<&'a [u32]>,
    uu_cursor_ordinals: Option<&'a HashMap<NzbFileId, u32>>,
    checkpoint: CheckpointAdmission,
    // What the handout being built has taken, as the checkpoint weighs it.
    leased: CheckpointLease,
    // Set when at least one article was refused *only* by the restart
    // checkpoint, so a caller that came away empty can tell "held for the
    // checkpoint" from "nothing here for this server" and schedule the
    // recheck the checkpoint needs.
    checkpoint_blocked: std::cell::Cell<bool>,
    // Articles refused so far, and the clauses that refused them; counters
    // only, never consulted by the answer.
    skipped: std::cell::Cell<u64>,
    refused_by: std::cell::Cell<BlockedBy>,
}

// What [`Pipeline::servable_work_filter`] found for one job and one server.
pub(in crate::pipeline::download) enum ServableWork<'a> {
    // Retention rules this server out for the whole job.
    RetentionExcluded,
    // Every queued file is refused by a clause decided per file, so no
    // article can pass and there is nothing to scan. Carries the clauses
    // that refused them.
    NoQueuedFilePasses(BlockedBy),
    // Some article may pass; scan the queue with this filter.
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

    // The first clause that refuses `work`, in a fixed order; the restart
    // checkpoint is asked last and only of an article every other clause
    // admits.
    fn refusal(&self, work: &DownloadWork) -> Option<SchedulerBlockClause> {
        let file_index = work.segment_id.file_id.file_index;
        if work.exclude_servers.contains(&self.server_idx)
            || (work.avoid_server == Some(self.server_idx) && self.hint_has_alternative(work))
        {
            return Some(SchedulerBlockClause::ServerExclusion);
        }
        use weaver_nntp::provider_policy::ArticleEligibility;
        match self.provider_policy.eligibility(
            self.server_idx,
            &work.exclude_servers,
            &self.retention_excludes,
            Pipeline::bandwidth_reservation_estimate(work.byte_estimate),
        ) {
            ArticleEligibility::Eligible(_) => {}
            ArticleEligibility::BackfillProtected => return Some(SchedulerBlockClause::Backfill),
            _ => return Some(SchedulerBlockClause::ProviderPriority),
        }
        if self
            .bootstrap_files
            .is_some_and(|files| !files.contains(&file_index))
        {
            return Some(SchedulerBlockClause::RecoveryMetadataBootstrap);
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

    // Whether a server other than this one could take `work` now: available,
    // not excluded by the article, not ruled out by the job's retention.
    fn hint_has_alternative(&self, work: &DownloadWork) -> bool {
        self.hint_alternatives.iter().any(|server| {
            *server != self.server_idx
                && !work.exclude_servers.contains(server)
                && !self.retention_excludes.contains(server)
                && matches!(
                    self.provider_policy.eligibility(
                        *server,
                        &work.exclude_servers,
                        &self.retention_excludes,
                        Pipeline::bandwidth_reservation_estimate(work.byte_estimate)
                    ),
                    weaver_nntp::provider_policy::ArticleEligibility::Eligible(_)
                )
        })
    }

    pub(in crate::pipeline::download) fn checkpoint_blocked(&self) -> bool {
        self.checkpoint_blocked.get()
    }

    // Articles this filter has refused, over every scan it drove.
    pub(in crate::pipeline::download) fn skipped(&self) -> u64 {
        self.skipped.get()
    }

    // The clauses that refused them.
    pub(in crate::pipeline::download) fn refused_by(&self) -> BlockedBy {
        self.refused_by.get()
    }

    // Charge an article the handout has just taken, so every later article
    // of the same handout is admitted against the restart checkpoint's
    // projected lead as it now stands.
    pub(in crate::pipeline::download) fn note_taken(&mut self, work: &DownloadWork) {
        self.leased.add(work);
    }
}

impl Pipeline {
    pub(in crate::pipeline::download::worker) fn reserve_download_work_for_dispatch(
        &mut self,
        job_id: JobId,
        work: DownloadWork,
    ) -> Result<Option<DownloadWork>, DispatchAttempt> {
        if !self.primary_download_within_restart_durable_lead(job_id, &work) {
            self.advance_durable_download_floor(job_id);
            if !self.primary_download_within_restart_durable_lead(job_id, &work) {
                self.note_checkpoint_dispatch_block(job_id);
                self.enqueue_download_work(work);
                self.update_queue_metrics();
                return Ok(None);
            }
        }
        self.download_restart_durable_lead_retry_after
            .remove(&job_id);

        Ok(Some(work))
    }

    // Whether work going back on the queue waits in the recovery queue
    // until something asks for it: what the job's root holds parked. An
    // article an object deferred while it was on the wire goes back parked,
    // whatever server failed it, and the root's moves are the one way out.
    pub(in crate::pipeline) fn requeue_parks(&self, work: &DownloadWork) -> bool {
        self.graph_article_parked(work.segment_id).unwrap_or(false)
    }

    // Hold payload work only while declared recovery indexes are unresolved.
    //
    // Checkpoint cuts are fixed when a batch is leased, so an index already
    // present in the primary queue must publish its grid first. Indexless
    // recovery discovery stays completion-bounded instead of turning every
    // optional volume into a pre-download barrier.
    pub(in crate::pipeline::download) fn recovery_metadata_bootstrap_files(
        &mut self,
        job_id: JobId,
    ) -> Option<Vec<u32>> {
        // The job's root namespace holds its own payload for the indexes the
        // post declares, as long as it says.
        self.graph_indexes_to_come(job_id)
    }

    // The recovery indexes the post declares that a graph job's root namespace
    // holds its payload for. Root members are numbered as the posted files
    // they are.
    fn graph_indexes_to_come(&self, job_id: JobId) -> Option<Vec<u32>> {
        let (graph, state) = (self.graph_jobs.get(&job_id)?, self.jobs.get(&job_id)?);
        let declared = state
            .spec
            .files
            .iter()
            .zip(0u32..)
            .filter_map(|(file, index)| {
                crate::pipeline::graph::metadata::RecoveryFormat::declared_index(&file.role)
                    .map(|format| (weaver_graph::MemberId(index), format))
            });
        graph
            .indexes_to_come(declared)
            .map(|held| held.into_iter().map(|member| member.0).collect())
    }

    // The single definition of "this server may fetch this queued article
    // right now", for one job.
    //
    // Both selection paths ask the same question, and they must not be able
    // to answer it differently: one of them pops work onto a live connection
    // and the other decides whether a connection should be given work at
    // all, so a drift between them shows up as either an idle link or an
    // article handed to a server that cannot serve it. Everything the answer
    // depends on — retention, the restart checkpoint, the PAR2 index
    // bootstrap, the UU spool cursor, the work's own exclusions and rotation
    // hint — is sampled once here, so a whole handout costs one sample rather
    // than one per article.
    //
    // Before the queue is scanned, the clauses that can refuse a whole file
    // are put to every queued file at once (see
    // [`Self::queued_file_may_pass`]); when none passes, the answer is
    // [`ServableWork::NoQueuedFilePasses`] and no scan is owed.
    //
    // The filter starts from an empty lease. A caller cutting a batch passes
    // each article it takes to [`ServableWorkFilter::note_taken`], so the
    // restart checkpoint's undurable lead sees the batch's own projection
    // rather than only what the actor has already committed.
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
        if let Err(blocked) = self.queued_file_may_pass(job_id, bootstrap_files) {
            return ServableWork::NoQueuedFilePasses(blocked);
        }
        ServableWork::Scan(ServableWorkFilter {
            server_idx,
            provider_policy: match &self.download_pass_provider_policy {
                Some(pass) => pass.clone(),
                None => Arc::new(self.nntp.pool().provider_policy()),
            },
            hint_alternatives: self.rotation_hint_alternatives(job_id),
            retention_excludes,
            bootstrap_files,
            uu_cursor_ordinals,
            checkpoint: self.checkpoint_admission(job_id),
            leased: CheckpointLease::default(),
            checkpoint_blocked: std::cell::Cell::new(false),
            skipped: std::cell::Cell::new(0),
            refused_by: std::cell::Cell::new(BlockedBy::default()),
        })
    }

    // Whether any queued file of the job could get an article past the
    // clause that refuses whole files, the PAR2 index bootstrap, answered
    // from the queue's per-file counts without looking at an article.
    //
    // Only an `Err` is acted on, so this may answer `Ok` freely and must
    // never answer `Err` while some article would pass. The other clauses
    // cannot refuse a whole file this way: the UU cursor admits the article
    // at its ordinal; exclusions, rotation hints and the backfill gate are
    // per article; and the checkpoint's refusals must be observed article by
    // article for the recheck it owes. Retention is decided for the whole
    // job before this is asked.
    fn queued_file_may_pass(
        &self,
        job_id: JobId,
        bootstrap_files: Option<&[u32]>,
    ) -> Result<(), BlockedBy> {
        let Some(bootstrap_files) = bootstrap_files else {
            return Ok(());
        };
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
            if bootstrap_files.contains(&file_index) {
                return Ok(());
            }
            blocked = blocked.with(SchedulerBlockClause::RecoveryMetadataBootstrap);
        }
        // Articles of a file the spec does not list were not looked at; let
        // the scan judge them.
        if counted != queue.len() {
            return Ok(());
        }
        Err(blocked)
    }

    // The fill servers a hinted article of `job_id` could be sent to instead
    // of its hinted one. Inside a dispatch attempt this is the attempt's own
    // ranking; outside one the pool is asked, but only when the job's queue
    // holds steered work, since nothing else reads the answer. A contended
    // ranking answers with every fill server, which keeps the hint, the
    // strict answer, for at most one pass.
    fn rotation_hint_alternatives(&self, job_id: JobId) -> Vec<usize> {
        if let Some(fill) = &self.download_pass_fill_servers {
            return fill.clone();
        }
        if self
            .jobs
            .get(&job_id)
            .is_none_or(|state| state.download_queue.excluded_work_count() == 0)
        {
            return Vec::new();
        }
        let flags = self.nntp.pool().server_backfill_flags();
        let is_fill = |idx: &usize| !flags.get(*idx).copied().unwrap_or(false);
        match self.nntp.blocking_body_server_order(&[]) {
            Some(order) => order
                .into_iter()
                .map(|server| server.0)
                .filter(is_fill)
                .collect(),
            None => (0..flags.len()).filter(is_fill).collect(),
        }
    }

    // The UU spool cursors a selection pass must respect, sampled once.
    //
    // Only a capped spool constrains selection; below the cap every encoding
    // dispatches freely and the map is not worth building.
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

    pub(in crate::pipeline) fn activate_download_batch_lease(
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
            lease.completion_critical,
            lease.lane_mode,
            activation_items,
            starts_connection,
        );
    }

    pub(in crate::pipeline) fn activate_download_batch(
        &mut self,
        job_id: JobId,
        completion_critical: bool,
        lane_mode: DownloadLaneMode,
        activation_items: &[(SegmentId, NzbFileId, u64)],
        starts_connection: bool,
    ) {
        let work_count = activation_items.len();
        if work_count == 0 {
            return;
        }

        // The in-flight article gauges were raised when the lane's record
        // took these articles (see `hold_lane_works`).
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
        for (segment_id, _, estimate) in activation_items {
            self.reserve_rate_limit_for_dispatch(*segment_id, *estimate);
        }
        self.mark_download_pass_started(job_id);
        self.publish_active_stage_metrics();
    }
}
