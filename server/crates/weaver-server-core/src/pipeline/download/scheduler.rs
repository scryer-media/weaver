//! One global answer to "what should this server fetch next?".
//!
//! # The rule
//!
//! A link that is paid for by the month is only worth what it carries, so the
//! first obligation is that a connection is never sent away empty while any
//! job the user has running still holds an article that connection's server
//! is allowed to fetch. The only things allowed to leave a slot idle are the
//! ones that are about the whole link rather than about any one job: a global
//! pause, hard byte pressure, an exhausted bandwidth-cap window, the rate
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
//! connection and lane gauges, ISP bandwidth reservations, activation of the
//! work it was handed, and returning unused work to the queue.

use super::worker::DownloadPressure;
use super::*;

/// What a server gets when it asks for work.
pub(in crate::pipeline) enum Handout {
    /// Articles to fetch, all from one job, in the order they should go out.
    Works(Vec<DownloadWork>),
    /// No job can serve this server right now.
    Idle,
    /// A whole-link gate is shut; this is not about any job's queue.
    Yield(YieldReason),
}

/// The only reasons a slot may be left empty while work is queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum YieldReason {
    Paused,
    HardPressure,
    BandwidthCapExhausted,
    RateLimited,
    HandoffDraining,
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
    pub(in crate::pipeline) fn next_works(
        &mut self,
        server_idx: usize,
        want: usize,
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

        let works = self.take_servable_works(*hot_job, server_idx, want, pressure);
        if !works.is_empty() {
            return self.record_handout(HandoutKind::Hot, works);
        }

        if soft_pressure {
            // A deliberate gate, not an unexplained idle slot: the guard
            // counter deliberately does not fire here.
            return Handout::Idle;
        }

        if let Some(in_flight) = spill_in_flight
            && spill_candidates.contains(&in_flight)
        {
            let works = self.take_servable_works(in_flight, server_idx, want, pressure);
            if !works.is_empty() {
                return self.record_handout(HandoutKind::Spill, works);
            }
        }

        for job_id in spill_candidates {
            let works = self.take_servable_works(*job_id, server_idx, want, pressure);
            if works.is_empty() {
                continue;
            }
            return self.record_handout(HandoutKind::Spill, works);
        }

        self.note_scheduler_idle(server_idx, &eligible, pressure);
        Handout::Idle
    }

    /// The gates that are about the link rather than about any job's queue.
    fn download_scheduler_link_gate(&mut self, pressure: DownloadPressure) -> Option<YieldReason> {
        if self.global_paused {
            return Some(YieldReason::Paused);
        }
        if self.rate_limiter.should_wait() {
            return Some(YieldReason::RateLimited);
        }
        if self.nntp_handoff_draining {
            return Some(YieldReason::HandoffDraining);
        }
        if let Err(error) = self.refresh_bandwidth_cap_window() {
            // The cap window could not be read, so the allowance is unknown.
            // Treating unknown as spent is the only safe direction: it costs a
            // pass, where fetching past a real cap costs the user money.
            error!(error = %error, "failed to refresh ISP bandwidth cap state");
            return Some(YieldReason::BandwidthCapExhausted);
        }
        if self.bandwidth_cap.cap_enabled() && self.bandwidth_cap.remaining_bytes() == 0 {
            return Some(YieldReason::BandwidthCapExhausted);
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
                {
                    return None;
                }
                Some((Self::job_dispatch_priority(state), index, *job_id))
            })
            .collect::<Vec<_>>();
        eligible.sort_unstable();
        eligible.into_iter().map(|(_, _, job_id)| job_id).collect()
    }

    /// Take up to `want` articles of one job that `server_idx` may fetch.
    ///
    /// Empty means this job is blocked on this server for this call. The
    /// job's completion-critical heap leads its ordinary one, which is what
    /// the queue's own "first matching" scan already does.
    fn take_servable_works(
        &mut self,
        job_id: JobId,
        server_idx: usize,
        want: usize,
        pressure: DownloadPressure,
    ) -> Vec<DownloadWork> {
        // Too young to fetch: asking now produces not-founds indistinguishable
        // from articles that were never posted.
        if self.propagation_hold_until(job_id).is_some() {
            return Vec::new();
        }
        // An archive whose unlock order changed re-ranks its queue before
        // anything is taken from it.
        self.apply_rar_unlock_priorities_if_dirty(job_id);
        let bootstrap_files = self.par2_metadata_bootstrap_files(job_id);
        let uu_cursor_ordinals = self.selection_uu_cursor_ordinals(pressure);

        let mut taken: Vec<DownloadWork> = Vec::new();
        let mut checkpoint_blocked = false;
        while taken.len() < want {
            // Rebuilt per article so the byte-budget clauses see what this
            // handout has already taken, exactly as a batch lease does.
            let Some(filter) = self.servable_work_filter(
                job_id,
                server_idx,
                bootstrap_files.as_deref(),
                uu_cursor_ordinals.as_ref(),
                &taken,
            ) else {
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
                break;
            };
            let popped = self.jobs.get_mut(&job_id).and_then(|state| {
                state
                    .download_queue
                    .pop_first_matching(|work| filter.allows(work))
            });
            checkpoint_blocked |= filter.checkpoint_blocked();
            let Some(work) = popped else {
                break;
            };
            if bootstrap_files.is_some() {
                self.par2_metadata_bootstrap_claims_work(job_id, &work);
            }
            taken.push(work);
        }

        if taken.is_empty() && checkpoint_blocked {
            self.note_checkpoint_dispatch_block(job_id);
        }
        taken
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
        if self.propagation_hold_until(job_id).is_some() {
            return false;
        }
        let bootstrap_files = self.par2_metadata_bootstrap_files(job_id);
        let uu_cursor_ordinals = self.selection_uu_cursor_ordinals(pressure);
        let Some(filter) = self.servable_work_filter(
            job_id,
            server_idx,
            bootstrap_files.as_deref(),
            uu_cursor_ordinals.as_ref(),
            &[],
        ) else {
            return false;
        };
        self.jobs.get(&job_id).is_some_and(|state| {
            state
                .download_queue
                .peek_first_matching(|work| filter.allows(work))
                .is_some()
        })
    }
}
