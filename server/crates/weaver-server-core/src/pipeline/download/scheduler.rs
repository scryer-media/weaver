//! One global answer to "what should this server fetch next?".
//!
//! # The rule
//!
//! A link that is paid for by the month is only worth what it carries, so the
//! first obligation is that a connection is never sent away empty while any
//! job the user has running still holds an article that connection's server
//! is allowed to fetch. The only things allowed to leave a slot idle are the
//! ones that are about the whole link rather than about any one job: a global
//! pause, hard byte pressure, an exhausted download quota, the rate
//! limiter, and an NNTP pool handover still draining its old sockets.
//!
//! The second obligation pulls the other way and is just as firm: a
//! downloading job should finish, not creep. Spreading a link's articles over
//! every queued job at once finishes none of them and leaves the user
//! watching ten bars crawl. So work is handed out from *one* job — the hot
//! job — and only when that job cannot serve the asking server does the next
//! one get a turn.
//!
//! Put together:
//!
//! 1. **Hot job.** The hot job is the first eligible job in
//!    `(dispatch priority, submission order)`, recomputed on every call.
//!    Eligible means the job's status allows downloading and its queue is not
//!    empty. While the hot job can yield anything at all to the asking
//!    server, every article handed to that server comes from it.
//! 2. **Blocked is per server, per call.** A job is blocked on a server when
//!    it yields nothing to that server *right now*: retention rules the
//!    server out, every queued article excludes it or points its rotation
//!    hint at it, per-set disk admission holds the file, a demotion sweep
//!    holds it, the restart checkpoint holds it, the UU spool cursor has not
//!    reached it, the PAR2 index bootstrap is still claiming the queue, the
//!    post is still propagating, or the queue is simply empty of anything
//!    else. Nothing here is remembered between calls.
//! 3. **Spill goes to exactly one job.** When the hot job is blocked, the
//!    walk continues down the same order and stops at the first job that
//!    yields. Obligation 1 outranks "the next one only": if the next job is
//!    blocked too, the walk keeps going. A spill job that already has
//!    articles out on this server keeps it while it can still serve it, so
//!    an earlier job that unblocks waits for that ring to drain rather than
//!    opening a second spill beside it; only the hot job outranks a spill in
//!    flight. Because the walk is in order it can never start a job behind
//!    one that still has servable work either.
//! 4. **A handout never spans jobs.** If the chosen job yields fewer articles
//!    than were asked for, that is the handout.
//! 5. **Completion-critical work orders a job internally, never globally.**
//!    Inside the chosen job its completion-critical heap is drained ahead of
//!    its ordinary heap. No other job's critical work displaces the hot job's
//!    ordinary work.
//! 6. **Soft byte pressure** narrows the field to the hot job alone and
//!    clamps the handout to a single article; there is no spill while memory
//!    is draining.
//! 7. **A lane holds its share of a job, not a fixed runway.** An article
//!    handed to a lane has left the queue, and that lane fetches what it holds
//!    one after another on one socket. A runway sized per lane alone lets the
//!    first lanes to ask reserve a small job whole: the job's queue reads
//!    empty while most of it has yet to be fetched, rule 3 sends every other
//!    connection on to the next job, and the link ends up spread over jobs it
//!    was supposed to finish one at a time. So what a lane may hold of a job
//!    is that job's unfetched articles — queued, plus those out on lanes —
//!    divided over the connections the link has, never less than a full pipe
//!    plus the article behind it. And a lane opens a job it holds nothing of
//!    only once its whole pipe is below that same floor: a lane still full of
//!    one job does not pre-lease the next one, whether the walk reached it
//!    because the first job's queue ran dry or because this lane's share of it
//!    is out. A lane held by either rule is not blocked and does not spill: it
//!    is [`Handout::Saturated`], and asks again once it has fetched down below
//!    the count the rule named. A large job's share is far beyond any runway,
//!    so nothing changes for it until its queue empties, when the boundary
//!    rule takes over.
//!
//! There is nothing else: no newsgroup dimension, no equal-priority rule, no
//! requirement that one handout be all recovery or all payload, and no loans
//! between jobs.
//!
//! # What the caller still owes
//!
//! [`Pipeline::next_works`] is queue-side only. It performs the bookkeeping
//! that belongs to *taking work out of a queue*:
//!
//! * PAR2 index-bootstrap claims, for each article popped while a bootstrap
//!   is in force;
//! * the checkpoint recheck note, when a job came away empty only because the
//!   restart checkpoint held its articles.
//!
//! Everything lane-side is the caller's: recording the lane's owner,
//! connection and lane gauges, download quota reservations, activation of the
//! work it was handed, and returning unused work to the queue.

use super::worker::DownloadPressure;
use super::worker::{BlockedBy, ServableWork, ServableWorkFilter};
use super::*;
use crate::operations::metrics::SchedulerBlockClause;

/// What a server gets when it asks for work.
pub(in crate::pipeline) enum Handout {
    /// Articles to fetch, all from one job, in the order they should go out.
    Works(Vec<DownloadWork>),
    /// No job can serve this server right now.
    Idle,
    /// A whole-link gate is shut; this is not about any job's queue.
    Yield(YieldReason),
    /// The job this lane would be served from has work for it, but the lane
    /// already holds its share of that job, or a full pipe of some other. It
    /// is busy, not idle: answer it again once the wake's count holds.
    Saturated(SaturationWake),
}

/// What a saturated lane is waiting to fetch down to before it is asked
/// about again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) struct SaturationWake {
    /// The job whose articles are counted; every job's when `None`.
    pub(in crate::pipeline) of: Option<JobId>,
    /// Answer the lane again once it holds fewer than this many of them.
    pub(in crate::pipeline) below: usize,
}

/// The asking lane, so what it already holds of a job can be set against
/// its share of that job.
#[derive(Debug, Clone, Copy)]
pub(in crate::pipeline) struct LaneShare {
    pub(in crate::pipeline) lane_id: u64,
    /// The pipeline depth the lane runs at.
    pub(in crate::pipeline) depth: usize,
}

/// The only reasons a slot may be left empty while work is queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum YieldReason {
    Paused,
    HardPressure,
    RateLimited,
    HandoffDraining,
}

/// What one job yields to one lane.
enum ShareTaken {
    Works(Vec<DownloadWork>),
    Saturated(SaturationWake),
    /// Nothing for this server, refused by these clauses.
    Blocked(BlockedBy),
}

/// Which class of handout a counter should record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandoutKind {
    Hot,
    Spill,
}

impl Pipeline {
    /// The next articles for `server_idx`, at most `want` of them.
    ///
    /// `spill_in_flight` names the job that already has articles out on this
    /// server behind the hot one, if any. While the hot job is blocked and
    /// that job can still serve the server, it keeps it: a second spill job
    /// never opens beside one in flight. `pressure` is the pressure sample the
    /// caller already took for this pass.
    #[cfg(test)]
    pub(in crate::pipeline) fn next_works(
        &mut self,
        server_idx: usize,
        want: usize,
        spill_in_flight: Option<JobId>,
        pressure: DownloadPressure,
    ) -> Handout {
        self.next_works_for_lane(server_idx, want, None, spill_in_flight, pressure)
    }

    /// [`Self::next_works`] for a lane whose holdings are known, so the
    /// handout can be held to the lane's share of the job it comes from.
    /// `None` hands out `want` regardless, as a caller with no lane does.
    pub(in crate::pipeline) fn next_works_for_lane(
        &mut self,
        server_idx: usize,
        want: usize,
        lane: Option<LaneShare>,
        spill_in_flight: Option<JobId>,
        pressure: DownloadPressure,
    ) -> Handout {
        if let Some(reason) = self.download_scheduler_link_gate(pressure) {
            return Handout::Yield(reason);
        }

        // Soft pressure keeps the hot job moving and nothing else, one
        // article at a time, until the decode and write backlogs drain.
        let soft_pressure = pressure.state == DownloadPressureState::Soft;
        let want = if soft_pressure { want.min(1) } else { want };
        if want == 0 {
            return Handout::Idle;
        }

        let eligible = self.download_scheduler_eligible_jobs();
        let Some((hot_job, spill_candidates)) = eligible.split_first() else {
            return Handout::Idle;
        };

        match self.take_lane_share(*hot_job, server_idx, want, lane, pressure) {
            ShareTaken::Works(works) => return self.record_handout(HandoutKind::Hot, works),
            ShareTaken::Saturated(wake) => return Handout::Saturated(wake),
            ShareTaken::Blocked(blocked) => self.note_hot_blocked(blocked),
        }

        if soft_pressure {
            // A deliberate gate, not an unexplained idle slot: the guard
            // counter deliberately does not fire here.
            return Handout::Idle;
        }

        if let Some(in_flight) = spill_in_flight
            && spill_candidates.contains(&in_flight)
        {
            match self.take_lane_share(in_flight, server_idx, want, lane, pressure) {
                ShareTaken::Works(works) => {
                    return self.record_handout(HandoutKind::Spill, works);
                }
                ShareTaken::Saturated(wake) => return Handout::Saturated(wake),
                ShareTaken::Blocked(_) => {}
            }
        }

        for job_id in spill_candidates {
            match self.take_lane_share(*job_id, server_idx, want, lane, pressure) {
                ShareTaken::Works(works) => {
                    return self.record_handout(HandoutKind::Spill, works);
                }
                ShareTaken::Saturated(wake) => return Handout::Saturated(wake),
                ShareTaken::Blocked(_) => {}
            }
        }

        self.note_scheduler_idle(server_idx, &eligible, pressure);
        Handout::Idle
    }

    /// The gates that are about the link rather than about any job's queue.
    fn download_scheduler_link_gate(&mut self, pressure: DownloadPressure) -> Option<YieldReason> {
        if self.global_paused || self.shared_state.schedule_replay_paused() {
            return Some(YieldReason::Paused);
        }
        if self.rate_limiter.should_wait() {
            return Some(YieldReason::RateLimited);
        }
        if self.nntp_handoff_draining {
            return Some(YieldReason::HandoffDraining);
        }
        if pressure.state == DownloadPressureState::Hard {
            return Some(YieldReason::HardPressure);
        }
        None
    }

    /// Every job that could be downloaded from, in the order the hot job and
    /// then each spill candidate are drawn from: dispatch priority first,
    /// submission order within a priority.
    pub(in crate::pipeline) fn download_scheduler_eligible_jobs(&self) -> Vec<JobId> {
        let mut eligible = self
            .job_order
            .iter()
            .enumerate()
            .filter_map(|(index, job_id)| {
                let state = self.jobs.get(job_id)?;
                if state.download_queue.is_empty()
                    || !Self::status_allows_download_dispatch(&state.status)
                    || self.held_for_added_scripts(*job_id)
                {
                    return None;
                }
                // A job whose own health probe asked every lane for a pickup
                // and got none stands down for one handout. The probe rides
                // the download lanes, so a job holding all of them starves the
                // batch that is trying to decide whether its release exists at
                // all — and the busier it is, the longer it holds them. No
                // lane is reserved: the job is simply not offered work while
                // the batch is waiting, so the next lane to come free takes
                // the batch.
                if self.owned_download_lane_pool.probe_is_starved_by(job_id.0) {
                    return None;
                }
                Some((Self::job_dispatch_priority(state), index, *job_id))
            })
            .collect::<Vec<_>>();
        eligible.sort_unstable();
        eligible.into_iter().map(|(_, _, job_id)| job_id).collect()
    }

    /// How many of one job's articles a single lane may hold at once.
    ///
    /// The job's unfetched articles — queued, plus those already out on
    /// lanes — divided over every connection the link may run, rounded up,
    /// so a job with fewer articles than the lanes' combined runway is spread
    /// over all of them instead of being reserved by the first few to ask.
    /// Measured against the whole rather than the queue alone so the share is
    /// the same for the last lane to ask as for the first. Never less than
    /// `depth + 1`: a full pipe, and the article that keeps it full while the
    /// next ask is answered.
    pub(in crate::pipeline) fn lane_share_of_job(&self, job_id: JobId, depth: usize) -> usize {
        let queued = self
            .jobs
            .get(&job_id)
            .map_or(0, |state| state.download_queue.len());
        let out_on_lanes = self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0);
        let connections = self.tuner.params().max_concurrent_downloads.max(1);
        queued
            .saturating_add(out_on_lanes)
            .div_ceil(connections)
            .max(depth.max(1) + 1)
    }

    /// One job's answer to one lane: work within the lane's share, a lane
    /// already at its share, or a job that is blocked on this server.
    fn take_lane_share(
        &mut self,
        job_id: JobId,
        server_idx: usize,
        want: usize,
        lane: Option<LaneShare>,
        pressure: DownloadPressure,
    ) -> ShareTaken {
        let Some(lane) = lane else {
            return match self.take_servable_works(job_id, server_idx, want, pressure) {
                Ok(works) => ShareTaken::Works(works),
                Err(blocked) => ShareTaken::Blocked(blocked),
            };
        };
        let share = self.lane_share_of_job(job_id, lane.depth);
        // Only what the lane holds *of this job* counts against its share of
        // it. Articles of another job it is still carrying say nothing about
        // how this one is spread over the link.
        let holds = self.download_lane_holdings_of_job(lane.lane_id, job_id);
        let full_pipe = lane.depth.max(1) + 1;
        let wake = if holds == 0 && self.download_lane_holdings(lane.lane_id) >= full_pipe {
            // A lane still full of another job does not open this one. The
            // share above is what keeps one job spread over the link; this
            // is what keeps a job that has left the queue — every article of
            // it out on lanes — from pulling the next job onto every lane
            // that carries a piece of it.
            Some(SaturationWake {
                of: None,
                below: full_pipe,
            })
        } else if holds >= share {
            Some(SaturationWake {
                of: Some(job_id),
                below: share,
            })
        } else {
            None
        };
        if let Some(wake) = wake {
            // Only a job that would actually have served this lane may hold it:
            // one that is blocked here must let the walk go on, or a lane at
            // its share of a job it cannot fetch from would shut out the rest.
            return match self.job_servable_work_for_server(job_id, server_idx, pressure) {
                Ok(()) => ShareTaken::Saturated(wake),
                Err(blocked) => ShareTaken::Blocked(blocked),
            };
        }
        let room = share - holds;
        match self.take_servable_works(job_id, server_idx, want.min(room), pressure) {
            Ok(works) => ShareTaken::Works(works),
            Err(blocked) => ShareTaken::Blocked(blocked),
        }
    }

    /// Take up to `want` articles of one job that `server_idx` may fetch.
    ///
    /// `Ok` is never empty; `Err` means this job is blocked on this server
    /// for this call, and names the clauses that refused it. The job's
    /// completion-critical heap leads its ordinary one, which is what the
    /// queue's own "first matching" scan already does.
    fn take_servable_works(
        &mut self,
        job_id: JobId,
        server_idx: usize,
        want: usize,
        pressure: DownloadPressure,
    ) -> Result<Vec<DownloadWork>, BlockedBy> {
        // Too young to fetch: asking now produces not-founds indistinguishable
        // from articles that were never posted.
        if self.propagation_hold_until(job_id).is_some() {
            return Err(BlockedBy::only(SchedulerBlockClause::Propagation));
        }
        // An archive whose unlock order changed re-ranks its queue before
        // anything is taken from it.
        self.apply_rar_unlock_priorities_if_dirty(job_id);
        let bootstrap_files = self.par2_metadata_bootstrap_files(job_id);
        let uu_cursor_ordinals = self.selection_uu_cursor_ordinals(pressure);

        // Sampled once for the whole handout. Each article taken is charged
        // to the filter, so the byte-budget clauses see what this handout has
        // already taken, exactly as a batch lease does.
        let mut filter = match self.servable_work_filter(
            job_id,
            server_idx,
            bootstrap_files.as_deref(),
            uu_cursor_ordinals.as_ref(),
        ) {
            ServableWork::Scan(filter) => filter,
            ServableWork::NoQueuedFilePasses(blocked) => return Err(blocked),
            ServableWork::RetentionExcluded => {
                // Retention rules this server out for the job. When it rules
                // every server out, no lane will ever take the queue: retire
                // it as missing now rather than leave the job waiting.
                let server_count = self.nntp.pool().server_count();
                let retention = self.job_retention_excludes(job_id);
                if Self::unavailable_server_count_from_excludes(server_count, &[], &retention)
                    >= server_count
                {
                    self.retire_unservable_queued_work(job_id);
                }
                return Err(BlockedBy::only(SchedulerBlockClause::Retention));
            }
        };
        let mut taken: Vec<DownloadWork> = Vec::new();
        while taken.len() < want {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                break;
            };
            let popped = state
                .download_queue
                .pop_first_matching(|work| filter.allows(work));
            let Some(work) = popped else {
                self.metrics
                    .download_scheduler_scan_no_match_total
                    .fetch_add(1, Ordering::Relaxed);
                break;
            };
            filter.note_taken(&work);
            if bootstrap_files.is_some() {
                self.par2_metadata_bootstrap_claims_work(job_id, &work);
            }
            taken.push(work);
        }

        self.note_scan_skips(&filter);
        if taken.is_empty() {
            if filter.checkpoint_blocked() {
                self.note_checkpoint_dispatch_block(job_id);
            }
            return Err(filter.refused_by());
        }
        Ok(taken)
    }

    fn note_scan_skips(&self, filter: &ServableWorkFilter<'_>) {
        let skipped = filter.skipped();
        if skipped != 0 {
            self.metrics
                .download_scheduler_scan_items_skipped_total
                .fetch_add(skipped, Ordering::Relaxed);
        }
    }

    /// Count what kept the hot job from the asking server, once per clause
    /// that refused it.
    fn note_hot_blocked(&self, blocked: BlockedBy) {
        for clause in blocked.clauses() {
            self.metrics.download_scheduler_hot_blocked_total[clause.index()]
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_handout(&mut self, kind: HandoutKind, works: Vec<DownloadWork>) -> Handout {
        let counter = match kind {
            HandoutKind::Hot => &self.metrics.download_scheduler_handouts_total_hot,
            HandoutKind::Spill => &self.metrics.download_scheduler_handouts_total_spill,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        Handout::Works(works)
    }

    /// Prove that an idle answer was honest.
    ///
    /// The walk above already visited every eligible job and took nothing, so
    /// in a release build that walk *is* the proof and the counter stays at
    /// zero. A debug build pays for a second, independent pass: it re-asks
    /// each eligible job whether it holds an article this server could fetch,
    /// which catches a walk that skipped a job it should have offered work
    /// to. The counter therefore reads the same in both builds unless the
    /// scheduler is wrong, and only a debug build can notice that it is.
    fn note_scheduler_idle(
        &mut self,
        server_idx: usize,
        eligible: &[JobId],
        pressure: DownloadPressure,
    ) {
        if !cfg!(debug_assertions) {
            return;
        }
        let servable = eligible
            .iter()
            .any(|job_id| self.job_has_servable_work_for_server(*job_id, server_idx, pressure));
        if servable {
            self.metrics
                .download_scheduler_idle_with_servable_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Whether any eligible job holds an article some server other than
    /// `server_idx` may fetch: the sign that a lane idle on `server_idx` is
    /// holding a slot a dial elsewhere could use.
    pub(in crate::pipeline) fn servable_work_on_other_server(
        &mut self,
        server_idx: usize,
        pressure: DownloadPressure,
    ) -> bool {
        let eligible = self.download_scheduler_eligible_jobs();
        if eligible.is_empty() {
            return false;
        }
        let server_count = self.nntp.pool().server_count();
        (0..server_count)
            .filter(|other| *other != server_idx)
            .any(|other| {
                eligible
                    .iter()
                    .any(|job_id| self.job_has_servable_work_for_server(*job_id, other, pressure))
            })
    }

    /// Whether one job holds an article `server_idx` may fetch, without
    /// taking it. The read-only twin of [`Self::take_servable_works`].
    fn job_has_servable_work_for_server(
        &mut self,
        job_id: JobId,
        server_idx: usize,
        pressure: DownloadPressure,
    ) -> bool {
        self.job_servable_work_for_server(job_id, server_idx, pressure)
            .is_ok()
    }

    /// [`Self::job_has_servable_work_for_server`], naming the clauses that
    /// refused the job when it holds nothing for the server.
    fn job_servable_work_for_server(
        &mut self,
        job_id: JobId,
        server_idx: usize,
        pressure: DownloadPressure,
    ) -> Result<(), BlockedBy> {
        if self.propagation_hold_until(job_id).is_some() {
            return Err(BlockedBy::only(SchedulerBlockClause::Propagation));
        }
        let bootstrap_files = self.par2_metadata_bootstrap_files(job_id);
        let uu_cursor_ordinals = self.selection_uu_cursor_ordinals(pressure);
        let filter = match self.servable_work_filter(
            job_id,
            server_idx,
            bootstrap_files.as_deref(),
            uu_cursor_ordinals.as_ref(),
        ) {
            ServableWork::Scan(filter) => filter,
            ServableWork::NoQueuedFilePasses(blocked) => return Err(blocked),
            ServableWork::RetentionExcluded => {
                return Err(BlockedBy::only(SchedulerBlockClause::Retention));
            }
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return Err(BlockedBy::default());
        };
        let found = state
            .download_queue
            .peek_first_matching(|work| filter.allows(work))
            .is_some();
        self.note_scan_skips(&filter);
        if found {
            return Ok(());
        }
        self.metrics
            .download_scheduler_scan_no_match_total
            .fetch_add(1, Ordering::Relaxed);
        Err(filter.refused_by())
    }
}
