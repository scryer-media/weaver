// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

impl Pipeline {
    pub(in crate::pipeline::download) fn status_allows_download_dispatch(
        status: &JobStatus,
    ) -> bool {
        matches!(
            status,
            JobStatus::Queued
                | JobStatus::Downloading
                | JobStatus::Checking
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::Repairing
                | JobStatus::QueuedExtract
                | JobStatus::Extracting
        )
    }

    // Whether the job holds work dispatch can hand out: articles in its
    // download queue, in a phase that dispatches. Recovery a seek round has
    // promoted is moved into the download queue, so it counts; recovery still
    // parked in the recovery queue dispatches nothing until a round promotes
    // it, so it does not. The dispatcher picks its jobs by this, and every
    // stall warning counts by it too.
    pub fn has_dispatchable_work(state: &JobState) -> bool {
        state.articles.has_queued_download_work()
            && Self::status_allows_download_dispatch(&state.status)
    }

    // The articles `has_dispatchable_work` counts, across the jobs holding a
    // slot: only those can dispatch. A job held for a full disk dispatches
    // nothing, so none of its articles count.
    pub(in crate::pipeline::download) fn dispatchable_queue_depth(&self) -> usize {
        self.nzb_admitted
            .iter()
            .filter(|job_id| !self.held_for_full_disk(**job_id))
            .filter_map(|job_id| self.jobs.get(job_id))
            .filter(|state| Self::has_dispatchable_work(state))
            .map(|state| state.articles.queued_download_len())
            .sum()
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline::download::worker) fn job_has_dispatchable_work(
        &mut self,
        job_id: JobId,
    ) -> bool {
        let uu_capped = !self.uu_files.is_empty() && self.uu_spool_dispatch_capped();
        let checkpoint = self.checkpoint_admission(job_id);
        self.jobs.get(&job_id).is_some_and(|state| {
            if !Self::status_allows_download_dispatch(&state.status) {
                return false;
            }
            // A job whose articles are not built yet has every one to hand out
            // once they are.
            let Some(queue) = state.articles.download_queue() else {
                return true;
            };
            // The checkpoint holds articles one by one, so raw queue occupancy
            // would overstate what this job can hand out right now.
            if checkpoint.enforced {
                return queue
                    .peek_first_matching(|work| {
                        checkpoint.decision(work, &[]).allows()
                            && (!uu_capped
                                || self
                                    .uu_files
                                    .get(&work.segment_id.file_id)
                                    .is_none_or(|uu| {
                                        uu.next_index.get() == work.segment_id.segment_number
                                    }))
                    })
                    .is_some();
            }
            if !uu_capped {
                return !queue.is_empty();
            }
            // A blocked UU head must not hide another encoding deeper in
            // either heap. Reuse per-file counts instead of scanning articles
            // or allocating a cursor map for this availability check.
            let queued_uu: usize = self
                .uu_files
                .keys()
                .filter(|file_id| file_id.job_id == job_id)
                .map(|file_id| queue.queued_count_for_file(*file_id) as usize)
                .sum();
            queue.len() > queued_uu
                || queue
                    .peek_next_matching(|work| {
                        self.uu_files
                            .get(&work.segment_id.file_id)
                            .is_none_or(|uu| uu.next_index.get() == work.segment_id.segment_number)
                    })
                    .is_some()
        })
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn job_has_dispatchable_work_for_test(
        &mut self,
        job_id: JobId,
    ) -> bool {
        self.job_has_dispatchable_work(job_id)
    }

    // The gate the dispatcher stops this job at while it holds no lane: no
    // server, byte pressure, a direct-store hold or the restart checkpoint.
    // None while a lane holds the job or none of its queued work is gated, so
    // a job only waiting its turn behind another reads as plain queued.
    #[cfg(any(test, feature = "test-support"))]
    pub fn job_dispatch_hold_reason(&self, job_id: JobId) -> Option<&'static str> {
        self.jobs
            .get(&job_id)
            .and_then(|state| self.hold_reason_of(job_id, state))
    }

    fn hold_reason_of(&self, job_id: JobId, state: &JobState) -> Option<&'static str> {
        if !Self::has_dispatchable_work(state) || self.job_has_current_download_activity(job_id) {
            return None;
        }
        // In the order the dispatch pass meets them.
        if crate::DownloadPressureState::from_code(
            self.metrics.download_pressure_state.load(Ordering::Relaxed),
        ) == crate::DownloadPressureState::Hard
        {
            return Some("byte_pressure");
        }
        if self.nntp.pool().server_count() == 0 {
            return Some("no_server");
        }
        // The job's own checkpoint holds it whatever the servers do; only
        // work the checkpoint lets through can be waiting on a server.
        self.checkpoint_hold_of(job_id, state)
    }

    // The restart checkpoint holding every queued article the job could
    // dispatch, if it does.
    fn checkpoint_hold_of(&self, job_id: JobId, state: &JobState) -> Option<&'static str> {
        let checkpoint = self.checkpoint_admission(job_id);
        if !checkpoint.enforced {
            return None;
        }
        let queue = state.articles.download_queue()?;
        // A checkpoint holding every payload article admits only recovery,
        // and the queue counts its recovery articles, so a queue of payload
        // alone is held without reading an article.
        if checkpoint.holds_every_payload_article() && queue.recovery_work_count() == 0 {
            return Some("checkpoint");
        }
        let open = queue.peek_first_matching(|work| {
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            self.dispatch_hold_items_examined
                .fetch_add(1, Ordering::Relaxed);
            checkpoint.decision(work, &[]).allows()
        });
        if open.is_some() {
            None
        } else {
            Some("checkpoint")
        }
    }

    // The dispatcher's word on a job: the wait it recorded, else the gate
    // holding it, else a wait for room on a disk, which holds the job's
    // extraction and repair while its fetches go on, else a wait on a
    // server refusing the login. A job the dispatcher holds is waiting for
    // that first; the disk is the reason only when nothing else explains
    // the wait. Only an installed job can wait for room, and the jobs the
    // loop found waiting are a set of its own: a job out of the queue window
    // is answered by the one lookup it always was.
    //
    // A fill server refusing the login is waited for until its refusal is
    // the verdict, and the job says so, naming the servers its work waits
    // on ([`Self::job_login_refusal_wait`]), for the whole wait:
    // `login_refused:<host:port>[, <host:port>...]`. It comes after the
    // gates and the disk, which hold the job whatever the servers do, and
    // stands in for the one-second wait a refused probe records ("NNTP
    // authentication blocked"), which is the same wait seen from one
    // article: the parked article is asked by its own exclusions, so the
    // two name the same servers and do not take turns. Every other recorded
    // wait keeps its place ahead of all of it. The job says so whether or
    // not other work of it is being fetched meanwhile, parked or queued
    // alike, so the reason does not come and go with each probe or each
    // article another server sends. `login_refusals` is the pass's one read
    // of the pool's refusals; while it is empty nothing of this runs.
    pub fn dispatch_wait_reason(
        &self,
        job_id: JobId,
        login_refusals: LoginRefusalView,
    ) -> Option<std::borrow::Cow<'static, str>> {
        use std::borrow::Cow;
        let mut login = None;
        if let Some(wait) = self.download_wait_by_job.get(&job_id) {
            if wait.reason != crate::pipeline::AUTH_WAIT_REASON {
                return Some(Cow::Borrowed(wait.reason));
            }
            login = self
                .job_login_refusal_wait(job_id, login_refusals, Some(wait.exclude_servers))
                .map(|wait| wait.servers);
            if login.is_none() {
                return Some(Cow::Borrowed(wait.reason));
            }
        }
        let state = self.jobs.get(&job_id)?;
        if let Some(reason) = self.hold_reason_of(job_id, state) {
            return Some(Cow::Borrowed(reason));
        }
        if (!self.disk_waiting.is_empty() && self.disk_waiting.contains(&job_id))
            || self.held_for_full_disk(job_id)
        {
            return Some(Cow::Borrowed(crate::jobs::handle::DISK_SPACE_WAIT_REASON));
        }
        if login.is_none()
            && !login_refusals.waiting.is_empty()
            && Self::has_dispatchable_work(state)
        {
            login = self
                .job_login_refusal_wait(job_id, login_refusals, None)
                .map(|wait| wait.servers);
        }
        login.map(|servers| Cow::Owned(self.login_refused_wait_words(servers)))
    }

    // `login_refused:` and the servers named, as `host:port` (`[host]:port`
    // for an IPv6 address, whose own colons would run into the port), read
    // from the configuration of those servers alone.
    pub fn login_refused_wait_words(&self, servers: weaver_nntp::ServerSet) -> String {
        let configs = self.nntp.pool().server_configs();
        let mut words = String::from(crate::jobs::handle::LOGIN_REFUSED_WAIT_REASON);
        for (n, server) in servers
            .iter()
            .filter_map(|idx| configs.get(idx))
            .enumerate()
        {
            use std::fmt::Write as _;
            words.push_str(if n == 0 { ":" } else { ", " });
            let _ = if server.host.contains(':') && !server.host.starts_with('[') {
                write!(words, "[{}]:{}", server.host, server.port)
            } else {
                write!(words, "{}:{}", server.host, server.port)
            };
        }
        words
    }

    // A hold lands or lifts, and a job gains or loses its lanes, with no job
    // event of its own, so each dispatch pass re-reports the jobs whose queue
    // row no longer says what the dispatcher sees. Only a job holding a slot,
    // a recorded wait or download activity can move that way, so the pass
    // looks at those and at the ones the last pass looked at, which catches a
    // job the moment it leaves them. A job without a slot reads its slot
    // wait, which the admission pass restates.
    pub(in crate::pipeline) fn report_changed_dispatch_holds(&mut self) {
        let mut watched: Vec<JobId> = self
            .nzb_admitted
            .iter()
            .chain(self.download_wait_by_job.keys())
            .chain(self.active_download_connections_by_job.keys())
            .chain(self.articles_in_flight_by_job.keys())
            .chain(self.active_decodes_by_job.jobs())
            .chain(self.pending_released_download_results_by_job.keys())
            .copied()
            .collect();
        watched.sort_unstable();
        watched.dedup();
        let previous = std::mem::take(&mut self.dispatch_hold_watch);
        let mut ids = watched.clone();
        ids.extend(
            previous
                .into_iter()
                .filter(|id| watched.binary_search(id).is_err()),
        );
        self.dispatch_hold_watch = watched;
        // One read of the pool's login refusals for the pass, outside the
        // queue lock.
        let login_refusals = self.login_refusal_view();
        let stale: Vec<JobId> = self.shared_state.queue.read(|inner| {
            ids.into_iter()
                .filter(|job_id| {
                    self.jobs.contains_key(job_id)
                        && inner.job(*job_id).is_some_and(|row| {
                            row.report.job.download_wait_reason.as_deref()
                                != self.stored_wait_reason(row, login_refusals).as_deref()
                                || row.report.download_active
                                    != self.job_has_current_download_activity(*job_id)
                        })
                })
                .collect()
        });
        for job_id in stale {
            self.report_job(job_id);
        }
    }

    pub(in crate::pipeline::download) fn job_dispatch_priority(state: &JobState) -> u8 {
        crate::pipeline::queue::order::metadata_priority(&state.spec.metadata)
    }
}
