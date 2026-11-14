// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::*;

// How long a job's computed retention exclusions stay fresh. Retention is
// day-granular; the TTL only exists to pick up server-config edits and
// day-boundary crossings without recomputing per lease.
const JOB_RETENTION_EXCLUDES_TTL: Duration = Duration::from_secs(60);

impl Pipeline {
    // Newest known post date across the job's files. `None` when the NZB
    // carried no usable dates — such jobs are never retention-skipped.
    pub(crate) fn job_posted_at_epoch(spec: &JobSpec) -> Option<u64> {
        spec.files
            .iter()
            .filter_map(|file| file.posted_at_epoch)
            .max()
    }

    // When this job's articles become old enough to fetch, or `None` if they
    // already are.
    //
    // `Some` means dispatch holds off. Queue snapshots expose the reason and
    // deadline so clients can distinguish propagation from active downloading.
    // Pause, resume and delete retain their ordinary lifecycle semantics.
    //
    // The answer is computed once per job and cached. It is derived from wall
    // time (the NZB's date is an epoch second) but stored as an [`Instant`],
    // so a system-clock adjustment after the job was admitted cannot move a
    // deadline the pipeline has already committed to — the same reason the
    // restart-lead retry beside it stores one.
    //
    // A job with no parseable dates is never deferred: `job_posted_at_epoch`
    // is `None` and the question has no answer, which is different from
    // answering "wait". Missing dates on *some* files contribute nothing —
    // the anchor is the newest date the NZB does carry, because that is the
    // article most likely still in flight.
    pub(in crate::pipeline) fn propagation_hold_until(&mut self, job_id: JobId) -> Option<Instant> {
        // A hold that ran out goes once seen, whatever the delay is now, so
        // it never calls for another walk.
        if self
            .propagation_ready_at
            .get(&job_id)
            .is_some_and(|(ready_at, _)| *ready_at <= Instant::now())
        {
            self.propagation_ready_at.remove(&job_id);
            self.emit_job_event(PipelineEvent::PhaseProgressUpdated { job_id });
            return None;
        }
        let delay = self.propagation_delay;
        #[cfg(test)]
        let delay = self.propagation_delay_forced.unwrap_or(delay);
        if delay.is_zero() {
            return None;
        }
        if let Some((ready_at, _)) = self.propagation_ready_at.get(&job_id).copied() {
            return Some(ready_at);
        }

        let posted_at = self
            .jobs
            .get(&job_id)
            .and_then(|state| Self::job_posted_at_epoch(&state.spec))
            .or_else(|| {
                self.shared_state.queue.read(|inner| {
                    let row = inner.job(job_id)?;
                    row.admission
                        .as_ref()
                        .map(|job| &job.spec)
                        .or_else(|| row.intents.restore.as_ref().map(|job| &job.spec))
                        .and_then(Self::job_posted_at_epoch)
                })
            })?;
        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        // A post dated in the future is treated as posted now rather than
        // deferred by however far the clock disagrees: saturating here means a
        // skewed poster's date can cost at most `delay`, never hours.
        let age = Duration::from_secs(now_epoch.saturating_sub(posted_at));
        let remaining = delay.saturating_sub(age);
        if remaining.is_zero() {
            return None;
        }

        let ready_at = Instant::now() + remaining;
        let retry_at_epoch_ms =
            chrono::Utc::now().timestamp_millis() + remaining.as_millis() as i64;
        self.propagation_ready_at
            .insert(job_id, (ready_at, retry_at_epoch_ms));
        self.emit_job_event(PipelineEvent::PhaseProgressUpdated { job_id });
        // Once per job, because the gate is consulted on every dispatch pass.
        info!(
            job_id = job_id.0,
            posted_at_epoch = posted_at,
            eligible_in_secs = remaining.as_secs(),
            delay_secs = delay.as_secs(),
            "deferring download start: the post is still propagating"
        );
        crate::runtime::perf_probe::record(
            "download.propagation.deferred",
            std::time::Duration::from_nanos(1),
        );
        Some(ready_at)
    }

    // How long until the earliest deferred job becomes eligible, for the run
    // loop's sleep. `None` when nothing is deferred.
    pub(crate) fn next_propagation_delay(&self) -> Option<Duration> {
        let now = Instant::now();
        self.propagation_ready_at
            .values()
            .map(|(ready_at, _)| *ready_at)
            .min()
            .map(|ready_at| ready_at.saturating_duration_since(now))
    }

    // Pool server indices whose retention window is shorter than this job's
    // post age. These servers are skipped for the job's articles without a
    // network attempt, carry no health penalty, and count toward
    // per-article exhaustion.
    //
    // Computing them reads the newest post date across the job's files, so
    // the answer is kept for a minute whoever asks: a pass that reads it
    // for every watched job, every turn, walks a job's files at most once a
    // minute, however many files the job has.
    pub(in crate::pipeline) fn job_retention_excludes(
        &self,
        job_id: JobId,
    ) -> weaver_nntp::ServerSet {
        let now = Instant::now();
        let mut cache = self
            .job_retention_exclude_cache
            .lock()
            .expect("retention exclude cache poisoned");
        if let Some((computed_at, excludes)) = cache.get(&job_id)
            && now.duration_since(*computed_at) < JOB_RETENTION_EXCLUDES_TTL
        {
            return *excludes;
        }
        let excludes = self.compute_job_retention_excludes(job_id);
        cache.insert(job_id, (now, excludes));
        excludes
    }

    pub(in crate::pipeline) fn compute_job_retention_excludes(
        &self,
        job_id: JobId,
    ) -> weaver_nntp::ServerSet {
        let retention_days = self.nntp.pool().server_retention_days();
        if retention_days.iter().all(|days| *days == 0) {
            return weaver_nntp::ServerSet::EMPTY;
        }
        let Some(posted_at) = self
            .jobs
            .get(&job_id)
            .and_then(|state| Self::job_posted_at_epoch(&state.spec))
        else {
            return weaver_nntp::ServerSet::EMPTY;
        };
        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        let age_secs = now_epoch.saturating_sub(posted_at);
        retention_days
            .iter()
            .enumerate()
            .filter(|(_, days)| **days > 0 && age_secs > u64::from(**days) * 86_400)
            .map(|(idx, _)| idx)
            .collect()
    }

    // Union of a work item's failure exclusions and the job's retention
    // exclusions — the effective exclude set for server ordering. Failure
    // excludes stay per-article on the work item; retention excludes stay
    // job-derived so a server-config change applies without rewriting
    // queued work.
    pub(in crate::pipeline) fn effective_exclude_servers(
        &mut self,
        job_id: JobId,
        failure_excludes: weaver_nntp::ServerSet,
    ) -> weaver_nntp::ServerSet {
        failure_excludes.union(self.job_retention_excludes(job_id))
    }

    // Number of distinct, currently-valid pool indices unavailable to this
    // article (see [`Self::server_may_serve`]). Indices outside the current
    // pool are ignored — a server config rebuild can shrink or reorder the
    // pool, and stale indices must not inflate exhaustion math into spurious
    // "article missing" verdicts.
    pub(in crate::pipeline) fn unavailable_server_count(
        &mut self,
        job_id: JobId,
        failure_excludes: weaver_nntp::ServerSet,
    ) -> usize {
        let retention = self.job_retention_excludes(job_id);
        Self::unavailable_server_count_from_excludes(self.nntp.pool(), failure_excludes, retention)
    }

    pub(in crate::pipeline) fn unavailable_server_count_from_excludes(
        pool: &weaver_nntp::NntpPool,
        failure_excludes: weaver_nntp::ServerSet,
        retention_excludes: weaver_nntp::ServerSet,
    ) -> usize {
        (0..pool.server_count())
            .filter(|idx| !Self::server_may_serve(pool, *idx, failure_excludes, retention_excludes))
            .count()
    }

    // Whether pool server `idx` may serve an article carrying
    // `failure_excludes`, in a job whose retention rules out
    // `retention_excludes`. The one answer every "is this article servable"
    // question reads: the server must not be excluded by the article, must
    // hold the post within its retention, and must not be refusing this
    // pool's credentials. A refusal is a verdict, like the other two; a
    // quarantine for transport or capacity is a wait and keeps the server.
    // A backfill server may serve the article only once every fill server
    // is excluded for it or out of its retention: backfill fills what the
    // fill servers lack, and a fill server refusing the credentials does not
    // lack the article, so it never opens backfill.
    pub(in crate::pipeline) fn server_may_serve(
        pool: &weaver_nntp::NntpPool,
        idx: usize,
        failure_excludes: weaver_nntp::ServerSet,
        retention_excludes: weaver_nntp::ServerSet,
    ) -> bool {
        Self::server_may_serve_when(
            pool,
            idx,
            failure_excludes,
            retention_excludes,
            pool.refuses_credentials(idx),
        )
    }

    // [`Self::server_may_serve`] with whether the server refuses this pool's
    // credentials already read, so a caller asking for many articles reads
    // the pool's refusals once.
    pub(in crate::pipeline) fn server_may_serve_when(
        pool: &weaver_nntp::NntpPool,
        idx: usize,
        failure_excludes: weaver_nntp::ServerSet,
        retention_excludes: weaver_nntp::ServerSet,
        refuses_credentials: bool,
    ) -> bool {
        !failure_excludes.contains(idx)
            && !retention_excludes.contains(idx)
            && !refuses_credentials
            && (!pool.is_backfill(idx)
                || pool.backfill_open_for(failure_excludes, retention_excludes))
    }

    // The pool's login refusals, read once for every job a pass asks about,
    // and outside any queue lock: the fill servers whose refusal is still a
    // wait and, only while there is one, the servers whose refusal is the
    // verdict. Empty in the common case, at one atomic read per server.
    pub(in crate::pipeline) fn login_refusal_view(&self) -> LoginRefusalView {
        let pool = self.nntp.pool();
        let waiting = pool.login_refusal_waits();
        if waiting.is_empty() {
            return LoginRefusalView::default();
        }
        LoginRefusalView {
            waiting,
            refusing: (0..pool.server_count())
                .filter(|idx| pool.refuses_credentials(*idx))
                .collect(),
        }
    }

    // A fill server's run of login refusals has lasted its give-up time
    // and the server refuses now: the one sweep every path to the verdict
    // leads to, whether the hour itself came up or a login was refused
    // past it. The verdict is on every job whose work waited for the
    // server, not only the one whose article a probe carried, so each job
    // dispatch would draw from is asked whether any of its queued work is
    // now work no server may serve, and that work is booked missing. A
    // paused job, or one being cancelled, is left alone: dispatch weighs
    // its work when it is drawn from again. Dispatch then runs again, which
    // restates the rows that said they waited.
    //
    // The verdict is marked told, and its one line said, only after the
    // sweep, and only for a server that refused from before the sweep to
    // after it. Otherwise it stays untold and the sweep runs again when
    // the server next refuses.
    //
    // Nothing runs while no verdict is untold, at one atomic read per
    // server. Asking costs one question per distinct set of exclusions a
    // job's queues carry; a queue is walked only to take out what is
    // booked. Called at the verdict instant the loop waits for and after a
    // refused login, never on a poll.
    pub(in crate::pipeline) fn tell_login_refusal_verdicts(&mut self) {
        let pool = Arc::clone(self.nntp.pool());
        let Some(untold) = pool.login_refusal_verdicts_untold() else {
            return;
        };
        self.download_dispatch_wake = true;
        let unservable: Vec<JobId> = self
            .download_scheduler_eligible_jobs()
            .into_iter()
            .filter(|job_id| self.job_has_unservable_queued_work(*job_id))
            .collect();
        for job_id in unservable {
            self.retire_unservable_queued_work(job_id);
        }
        pool.mark_login_refusal_verdicts_told(&untold);
    }

    // Whether any article `job_id` has queued, for download or as parked
    // recovery, is one no server may fetch: what
    // [`Self::retire_unservable_queued_work`] would book. Asked once per
    // distinct set of exclusions, never per article.
    fn job_has_unservable_queued_work(&self, job_id: JobId) -> bool {
        let pool = self.nntp.pool();
        let server_count = pool.server_count();
        let Some(active) = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.articles.active())
        else {
            return false;
        };
        if server_count == 0 {
            return false;
        }
        let retention = self.job_retention_excludes(job_id);
        [&active.download_queue, &active.recovery_queue]
            .into_iter()
            .any(|queue| {
                (queue.len() > queue.excluded_work_count())
                    .then_some(weaver_nntp::ServerSet::EMPTY)
                    .into_iter()
                    .chain(queue.steered_exclusion_sets())
                    .any(|excludes| {
                        Self::unavailable_server_count_from_excludes(pool, excludes, retention)
                            >= server_count
                    })
            })
    }

    // The servers an article carrying `failure_excludes`, in a job whose
    // retention rules out `retention_excludes`, waits on for the login: some
    // server may serve it ([`Self::server_may_serve`]) and every server that
    // may is one whose refusal is still a wait. None when any other server
    // may serve it, or none may.
    pub(in crate::pipeline) fn login_refusal_waited_by(
        &self,
        view: LoginRefusalView,
        failure_excludes: weaver_nntp::ServerSet,
        retention_excludes: weaver_nntp::ServerSet,
    ) -> Option<weaver_nntp::ServerSet> {
        Self::login_refusal_waited_by_in(
            self.nntp.pool(),
            view,
            failure_excludes,
            retention_excludes,
        )
    }

    // [`Self::login_refusal_waited_by`] against a pool the caller holds.
    pub(in crate::pipeline) fn login_refusal_waited_by_in(
        pool: &weaver_nntp::NntpPool,
        view: LoginRefusalView,
        failure_excludes: weaver_nntp::ServerSet,
        retention_excludes: weaver_nntp::ServerSet,
    ) -> Option<weaver_nntp::ServerSet> {
        let may_serve: weaver_nntp::ServerSet = (0..pool.server_count())
            .filter(|idx| {
                Self::server_may_serve_when(
                    pool,
                    *idx,
                    failure_excludes,
                    retention_excludes,
                    view.refusing.contains(*idx),
                )
            })
            .collect();
        (!may_serve.is_empty() && may_serve.difference(view.waiting).is_empty())
            .then_some(may_serve)
    }

    // The one answer to "does this job wait on a server refusing the login":
    // the servers its queued download work waits on for the login
    // ([`Self::login_refusal_waited_by`]), and whether all of that work
    // does. Parked optional recovery dispatches nothing and is not asked
    // about; recovery a round promoted sits in the download queue and is.
    // With `parked`, the article the job parked for its refused-login retry
    // is asked about too, by the exclusions its recorded wait keeps, so an
    // article refused at a probe names the same servers parked as queued.
    //
    // Nothing runs while no server's refusal is a wait. Otherwise the
    // question is asked once for the work with no exclusion and once per
    // distinct set of exclusions the steered work carries, which the queue
    // counts as it goes: the cost is bounded by the combinations of servers
    // tried, never by the articles queued. The servers named are those of
    // every waiting set together, so the answer does not depend on which
    // set is read first.
    pub(in crate::pipeline) fn job_login_refusal_wait(
        &self,
        job_id: JobId,
        view: LoginRefusalView,
        parked: Option<weaver_nntp::ServerSet>,
    ) -> Option<LoginRefusalWait> {
        if view.waiting.is_empty() {
            return None;
        }
        let queue = self.jobs.get(&job_id)?.articles.download_queue()?;
        let retention = self.job_retention_excludes(job_id);
        let unsteered =
            (queue.len() > queue.excluded_work_count()).then_some(weaver_nntp::ServerSet::EMPTY);
        let mut servers = weaver_nntp::ServerSet::EMPTY;
        let mut every = true;
        for excludes in unsteered
            .into_iter()
            .chain(queue.steered_exclusion_sets())
            .chain(parked)
        {
            #[cfg(test)]
            self.login_refusal_sets_examined
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match self.login_refusal_waited_by(view, excludes, retention) {
                Some(waited) => servers = servers.union(waited),
                None => every = false,
            }
        }
        (!servers.is_empty()).then_some(LoginRefusalWait { servers, every })
    }

    // Whether an article carrying `failure_excludes` has no server left
    // that may fetch it ([`Self::server_may_serve`]).
    // An unconfigured pool answers no: nothing can be booked against
    // servers that do not exist yet.
    pub(in crate::pipeline) fn no_server_can_serve(
        &mut self,
        job_id: JobId,
        failure_excludes: weaver_nntp::ServerSet,
    ) -> bool {
        let server_count = self.nntp.pool().server_count();
        server_count > 0 && self.unavailable_server_count(job_id, failure_excludes) >= server_count
    }

    // Book one article as missing because no server may fetch it. The
    // article is not necessarily gone from Usenet, but nothing configured
    // can prove otherwise, and leaving it queued would hold the job at
    // downloading forever with nothing in flight.
    //
    // When a server refusing this pool's credentials is among those that
    // would otherwise serve it, the refusal is why: the job keeps which
    // server refused, and its failure for want of articles says so.
    pub(in crate::pipeline) fn book_unservable_work(&mut self, work: &DownloadWork) {
        self.book_unservable_segment(work.segment_id, work.exclude_servers);
    }

    pub(in crate::pipeline) fn book_unservable_segment(
        &mut self,
        segment_id: SegmentId,
        exclude_servers: weaver_nntp::ServerSet,
    ) {
        self.book_unservable_segment_logged(segment_id, exclude_servers, true);
    }

    // Books the article and returns the servers refusing credentials that
    // would otherwise have served it. A sweep books many articles in one
    // pass and reports them once itself, so its per-article line is debug.
    fn book_unservable_segment_logged(
        &mut self,
        segment_id: SegmentId,
        exclude_servers: weaver_nntp::ServerSet,
        warn_per_article: bool,
    ) -> weaver_nntp::ServerSet {
        #[cfg(test)]
        {
            let (depth, deepest) = &mut self.unservable_booking_depth;
            *depth += 1;
            *deepest = (*deepest).max(*depth);
        }
        let refused_by =
            self.book_unservable_segment_inner(segment_id, exclude_servers, warn_per_article);
        #[cfg(test)]
        {
            self.unservable_booking_depth.0 -= 1;
        }
        refused_by
    }

    fn book_unservable_segment_inner(
        &mut self,
        segment_id: SegmentId,
        exclude_servers: weaver_nntp::ServerSet,
        warn_per_article: bool,
    ) -> weaver_nntp::ServerSet {
        // The id in use failed on every server it may use. When the NZB gave
        // the part another id, the article moves to it instead of being
        // booked. An id no server may serve either is spent the same way, one
        // turn of this loop per id, until an id is queued or none is left.
        let job_id = segment_id.file_id.job_id;
        let mut unservable: Option<DownloadWork> = None;
        while let Some(work) = self.spend_message_id_for_work(segment_id) {
            match self.enqueue_servable_download_work(work) {
                Some(work) => unservable = Some(work),
                None => {
                    self.update_queue_metrics();
                    if unservable.is_some() {
                        self.finish_spent_message_ids(job_id);
                    }
                    return weaver_nntp::ServerSet::EMPTY;
                }
            }
        }
        let Some(work) = unservable else {
            return self.book_unservable_article(segment_id, exclude_servers, warn_per_article);
        };
        // The last id is booked as the article's own requeue is: with the
        // exclusions that work carried, warned per article.
        self.book_unservable_article(segment_id, work.exclude_servers, true);
        self.finish_spent_message_ids(job_id);
        weaver_nntp::ServerSet::EMPTY
    }

    // What follows an article that spent ids no server could serve, once it
    // is queued or booked: the queue metrics and the download pass are
    // brought up to date. Both answer the same however often they are asked
    // in a row, so once covers every id spent.
    fn finish_spent_message_ids(&mut self, job_id: JobId) {
        self.update_queue_metrics();
        self.maybe_finish_download_pass(job_id);
        self.update_queue_metrics();
    }

    // Books the article missing, every id spent, and returns the servers
    // refusing credentials that would otherwise have served it.
    fn book_unservable_article(
        &mut self,
        segment_id: SegmentId,
        exclude_servers: weaver_nntp::ServerSet,
        warn_per_article: bool,
    ) -> weaver_nntp::ServerSet {
        let job_id = segment_id.file_id.job_id;
        let refused_by = self.login_refused_unasked(job_id, exclude_servers);
        if warn_per_article {
            warn!(
                segment = %segment_id,
                exclude_servers = ?exclude_servers,
                refusing_credentials = ?refused_by,
                "no server may fetch this article; booking it as missing so repair can proceed"
            );
        } else {
            debug!(
                segment = %segment_id,
                exclude_servers = ?exclude_servers,
                refusing_credentials = ?refused_by,
                "no server may fetch this article; booking it as missing so repair can proceed"
            );
        }
        // The job lacks the article whether or not a server refused: its
        // cause is the missing articles, as when every server asked said
        // the article is not there, so its next duplicate is tried. The job
        // keeps which server refused, and a failure for want of articles
        // names it.
        if !refused_by.is_empty() {
            self.semantic_terminal_causes.insert(
                job_id,
                crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth,
            );
            self.note_login_refusals(job_id, refused_by);
        }
        self.metrics
            .articles_not_found
            .fetch_add(1, Ordering::Relaxed);
        self.send_segment_event(|| PipelineEvent::ArticleNotFound { segment_id });
        // A server that refuses the credentials never said the article is
        // gone, and the state says so; the post's health counts it missing
        // all the same, since no server can bring it.
        let state = if refused_by.is_empty() {
            SegmentTerminalState::Missing
        } else {
            SegmentTerminalState::Unservable
        };
        if self.book_terminal_segment(segment_id, state) {
            self.note_gap_servers_support_fact(job_id, exclude_servers);
        }
        refused_by
    }

    // The servers refusing the login that an article of `job_id` carrying
    // `exclude_servers` was never asked of: neither excluded for the article
    // nor out of the post's retention. One pass over the server list, from
    // what the pool and the job already hold.
    pub(in crate::pipeline) fn login_refused_unasked(
        &mut self,
        job_id: JobId,
        exclude_servers: weaver_nntp::ServerSet,
    ) -> weaver_nntp::ServerSet {
        let retention = self.job_retention_excludes(job_id);
        let pool = self.nntp.pool();
        (0..pool.server_count())
            .filter(|idx| {
                !exclude_servers.contains(*idx)
                    && !retention.contains(*idx)
                    && pool.refuses_credentials(*idx)
            })
            .collect()
    }

    // An article was booked past `refused_by`, every other server having
    // answered not-found for it or being out of the post's retention: the
    // job keeps which servers refused, by identity (stable
    // id, host, port and account) so the set outlives a change of the server
    // list as the tried servers of an article do. Nothing else changes
    // here: the job's cause is whatever its failures record.
    pub(in crate::pipeline) fn note_login_refusals(
        &mut self,
        job_id: JobId,
        refused_by: weaver_nntp::ServerSet,
    ) {
        if refused_by.is_empty() {
            return;
        }
        let pool = self.nntp.pool();
        let configs = pool.server_configs();
        let held = self.login_refusals.entry(job_id).or_default();
        for server in refused_by.iter() {
            let (Some(stable), Some(config)) = (
                pool.stable_server_id(weaver_nntp::pool::ServerId(server)),
                configs.get(server),
            ) else {
                continue;
            };
            // Compared in place: a server already held costs no allocation.
            if !held.iter().any(|known| {
                known.stable_id == stable
                    && known.host == config.host
                    && known.port == config.port
                    && known.username == config.username
            }) {
                held.push(weaver_nntp::pool::ServerIdentity {
                    stable_id: stable,
                    host: config.host.clone(),
                    port: config.port,
                    username: config.username.clone(),
                });
            }
        }
    }

    // The words a job failing for want of articles gains when a server
    // refusing the login left articles of it unasked for, naming each such
    // server still in the server list, in the list's order. A server since
    // removed is not named, and with none left there are no words.
    pub(in crate::pipeline) fn login_refusal_words(&self, job_id: JobId) -> Option<String> {
        let refused_by = self.login_refusals.get(&job_id)?;
        let names = self
            .nntp
            .pool()
            .server_identities()
            .into_iter()
            .filter(|server| refused_by.contains(server))
            .map(|server| format!("{}:{}", server.host, server.port))
            .collect::<Vec<_>>();
        let rest = "never asked for the articles no other server had";
        match names.as_slice() {
            [] => None,
            [name] => Some(format!("server {name} refuses the login and was {rest}")),
            names => Some(format!(
                "servers {} refuse the login and were {rest}",
                names.join(", ")
            )),
        }
    }

    // Book as missing every queued article of `job_id` that no server may
    // fetch. Returns how many were retired.
    pub(in crate::pipeline) fn retire_unservable_queued_work(&mut self, job_id: JobId) -> usize {
        let pool = Arc::clone(self.nntp.pool());
        let server_count = pool.server_count();
        if server_count == 0 {
            return 0;
        }
        let retention = self.job_retention_excludes(job_id);
        let unservable = |work: &QueuedWork<'_>| {
            Self::unavailable_server_count_from_excludes(&pool, work.exclude_servers, retention)
                >= server_count
        };
        let Some(active) = self
            .jobs
            .get_mut(&job_id)
            .and_then(|state| state.articles.active_mut())
        else {
            return 0;
        };
        let mut retired = active.download_queue.extract_matching(unservable);
        retired.extend(active.recovery_queue.extract_matching(unservable));
        if retired.is_empty() {
            return 0;
        }
        let mut exclude_sets = std::collections::BTreeSet::new();
        let mut refusing = weaver_nntp::ServerSet::EMPTY;
        for work in &retired {
            exclude_sets.insert(work.exclude_servers);
            refusing = refusing.union(self.book_unservable_segment_logged(
                work.segment_id,
                work.exclude_servers,
                false,
            ));
        }
        warn!(
            job_id = job_id.0,
            booked = retired.len(),
            exclude_sets = ?exclude_sets,
            refusing_credentials = ?refusing,
            "no server may fetch these queued articles; booking them as missing so repair can proceed"
        );
        self.update_queue_metrics();
        self.maybe_finish_download_pass(job_id);
        retired.len()
    }

    pub(in crate::pipeline) fn clear_job_retention_excludes(&mut self, job_id: JobId) {
        self.job_retention_exclude_cache
            .get_mut()
            .expect("retention exclude cache poisoned")
            .remove(&job_id);
    }

    pub(in crate::pipeline) fn clear_retention_exclude_cache(&mut self) {
        self.job_retention_exclude_cache
            .get_mut()
            .expect("retention exclude cache poisoned")
            .clear();
    }
}

// The pool's login refusals as one pass reads them; see
// [`Pipeline::login_refusal_view`].
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::pipeline) struct LoginRefusalView {
    // Fill servers whose refusal is still a wait.
    pub(in crate::pipeline) waiting: weaver_nntp::ServerSet,
    // Servers whose refusal is the verdict.
    pub(in crate::pipeline) refusing: weaver_nntp::ServerSet,
}

// A job's wait on servers refusing the login; see
// [`Pipeline::job_login_refusal_wait`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::pipeline) struct LoginRefusalWait {
    // The servers the waiting work waits on.
    pub(in crate::pipeline) servers: weaver_nntp::ServerSet,
    // Whether all the job's queued download work waits on them, so nothing
    // of it could be dispatched now whatever else is wrong.
    pub(in crate::pipeline) every: bool,
}
