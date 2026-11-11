// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A queue far longer than its slots keeps only a window of it installed: the
// jobs next in dispatch order, and every job doing something its queue row
// cannot carry. Every other job is its row and its database rows, and is
// installed again through the restore path when it enters the window.

use super::*;
use crate::jobs::handle::RestoreJobRequest;
use crate::pipeline::queue::{Change, JobRow, QueueInner};

// How many jobs the window holds in dispatch order.
pub(crate) const QUEUE_WINDOW: usize = 100;

// The window for a slot limit: a limit above the window widens it by as many
// jobs again, so the jobs that wait behind every slot are still installed.
pub(crate) fn window_size(max_concurrent_nzbs: usize) -> usize {
    if max_concurrent_nzbs > QUEUE_WINDOW {
        max_concurrent_nzbs + QUEUE_WINDOW
    } else {
        QUEUE_WINDOW
    }
}

// A job that never started: queued, or paused to go back to queued.
pub(crate) fn waits_its_turn(status: &JobStatus, paused_resume_status: Option<&JobStatus>) -> bool {
    match status {
        JobStatus::Queued => true,
        JobStatus::Paused => paused_resume_status.is_none_or(|resume| *resume == JobStatus::Queued),
        _ => false,
    }
}

// The row's priority in dispatch order, with an edit not yet applied in force.
fn row_priority(row: &JobRow, globals: &crate::pipeline::queue::Globals) -> u8 {
    if row.intents.update.is_some() {
        crate::pipeline::nzb_admission::metadata_priority(&row.info(globals).metadata)
    } else {
        crate::pipeline::nzb_admission::metadata_priority(&row.report.job.metadata)
    }
}

// What a window install reads of a job that is not installed: its active row
// and its stored NZB outline, as a restart reads it.
pub(crate) struct ParkedLoads {
    db: crate::Database,
    markers: Vec<(JobId, Result<tokio::sync::oneshot::Receiver<()>, String>)>,
}

impl ParkedLoads {
    // Each job read back, in the order named, from its row and outline,
    // spread over the cores. A job with no outline yet reads its stored NZB
    // once for one.
    pub(crate) async fn read(self) -> Vec<(JobId, Result<Option<RestoreJobRequest>, String>)> {
        let Self { db, markers } = self;
        if markers.is_empty() {
            return Vec::new();
        }
        let mut ids = Vec::with_capacity(markers.len());
        let mut waited = HashMap::new();
        for (id, marker) in markers {
            ids.push(id);
            let reached = match marker {
                Ok(marker) => marker
                    .await
                    .map_err(|_| "database writer queue closed".to_string()),
                Err(error) => Err(error),
            };
            if let Err(error) = reached {
                waited.insert(id, error);
            }
        }
        tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;
            ids.into_par_iter()
                .map(|id| match waited.get(&id) {
                    Some(error) => (id, Err(error.clone())),
                    None => (id, load_parked_job(&db, id)),
                })
                .collect()
        })
        .await
        .unwrap_or_default()
    }
}

fn load_parked_job(
    db: &crate::Database,
    job_id: JobId,
) -> Result<Option<RestoreJobRequest>, String> {
    let Some(recovered) = db
        .load_active_job_row(job_id)
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    let outline = db
        .load_job_outlines(Some(job_id))
        .map_err(|error| error.to_string())?
        .remove(&job_id);
    let (spec, summaries) = crate::operations::read_stored_files(db, job_id, &recovered, outline)
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    let mut request = crate::operations::restore_request(job_id, recovered, spec, summaries);
    if let Some(read) = request.restart.as_mut() {
        read.reentry = true;
    }
    Ok(Some(request))
}

impl Pipeline {
    // The window's one predicate: whether the row's job is installed. A job
    // is installed when it ranks among the window's first jobs in dispatch
    // order (`ranked`, from `queue_window_ranks`), or when it does something
    // its row cannot carry.
    pub(crate) fn in_queue_window(&self, ranked: &HashSet<JobId>, row: &JobRow) -> bool {
        ranked.contains(&row.id) || self.window_activity(row)
    }

    // The first jobs in dispatch order, priority then queue position, of the
    // rows not cancelled, retiring or blocked.
    pub(crate) fn queue_window_ranks(&self, inner: &QueueInner) -> HashSet<JobId> {
        let globals = &inner.globals;
        let size = window_size(
            globals
                .max_concurrent_nzbs
                .unwrap_or(crate::settings::model::DEFAULT_MAX_CONCURRENT_NZBS)
                .get() as usize,
        );
        let mut ranked: Vec<(u8, usize, JobId)> = inner
            .jobs
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                row.intents.cancelled.is_none()
                    && !row.report.retiring
                    && !self.blocked_restores.contains_key(&row.id)
                    && !self.window_unreadable.contains(&row.id)
            })
            .map(|(position, row)| (row_priority(row, globals), position, row.id))
            .collect();
        if ranked.len() > size {
            ranked.select_nth_unstable(size - 1);
            ranked.truncate(size);
        }
        ranked.into_iter().map(|(_, _, id)| id).collect()
    }

    // Whether the row's job does something its row cannot carry: it holds a
    // slot, an intent waits for it, it arrives to run again, its arrival's
    // scripts hold it, it restores mid-run, or, installed, it ever started.
    fn window_activity(&self, row: &JobRow) -> bool {
        let id = row.id;
        if self.nzb_admitted.contains(&id)
            || self.window_due.contains(&id)
            || self.added_script_holds.contains(&id)
            || Self::intent_in_flight(row)
        {
            return true;
        }
        if let Some(add) = &row.admission {
            return add.restart.is_some();
        }
        if let Some(request) = &row.intents.restore {
            return !waits_its_turn(&request.status, request.paused_resume_status.as_ref());
        }
        self.jobs
            .get(&id)
            .is_some_and(|state| !self.resting(id, state))
    }

    // An intent the actor still has to act on. A resume waiting for its slot
    // is acted on once the slot is taken, which installs the job.
    fn intent_in_flight(row: &JobRow) -> bool {
        let intents = &row.intents;
        let seen = row.report.seen_intent_seq;
        let waits_for_slot = intents.paused == Some(false) && intents.pause_seq > seen;
        intents.cancelled.is_some()
            || intents.cancel_post_processing
            || ((intents.seq != seen || intents.update.is_some() || intents.rename_to.is_some())
                && !waits_for_slot)
    }

    // An installed job waiting its turn with nothing in hand or in flight.
    fn resting(&self, id: JobId, state: &crate::jobs::model::JobState) -> bool {
        matches!(state.articles, crate::jobs::model::JobArticles::Queued(_))
            && waits_its_turn(&state.status, state.paused_resume_status.as_ref())
            && state.download_state == crate::DownloadState::Queued
            && state.post_state == crate::PostState::Idle
            && state.downloaded_bytes == 0
            && state.failed_bytes == 0
            && !self.graph_jobs.contains_key(&id)
            && !self.retiring_graph_jobs.contains_key(&id)
            && !self.blocked_restores.contains_key(&id)
            && !self.nzb_reprocess_pending.contains(&id)
            && !self.jobs_finalizing_download.contains(&id)
            && !self.propagation_ready_at.contains_key(&id)
            && !self.deferred_moves.contains(&id)
            && !self.queue_script_waiters.contains(&id)
            && !self.inflight_moves.contains(&id)
            && !self.inflight_terminal_post_processing.contains(&id)
            && !self.nzb_work_in_flight(id)
    }

    // Count a job out of the window: its articles still count toward the
    // queue's depth.
    pub(crate) fn park_window_job(&mut self, id: JobId, articles: u64) {
        if let Some(previous) = self.window_parked.insert(id, articles) {
            self.window_parked_articles -= previous;
        }
        self.window_parked_articles += articles;
    }

    fn unpark_window_job(&mut self, id: JobId) -> bool {
        match self.window_parked.remove(&id) {
            Some(articles) => {
                self.window_parked_articles -= articles;
                true
            }
            None => false,
        }
    }

    pub(crate) fn window_parked(&self, id: JobId) -> bool {
        self.window_parked.contains_key(&id)
    }

    // The parked jobs named, to be read from the database off the actor.
    // Each job's queued writes land first, so an edit it was given while
    // parked is in the row read: the marker at the end of each job's lane is
    // queued here, before any is awaited.
    pub(crate) fn parked_loads(&self, ids: Vec<JobId>) -> ParkedLoads {
        let markers = ids
            .into_iter()
            .map(|id| {
                let marker = self
                    .db
                    .queue_job_writes_marker(id)
                    .map_err(|error| error.to_string());
                (id, marker)
            })
            .collect();
        ParkedLoads {
            db: self.db.clone(),
            markers,
        }
    }

    // A parked job read back for its install leaves the parked count; one
    // that cannot be read stays parked, out of the window, until a restart.
    pub(crate) fn take_parked_load<T>(
        &mut self,
        id: JobId,
        loaded: Result<Option<T>, String>,
    ) -> Option<T> {
        match loaded {
            Ok(Some(request)) => {
                self.unpark_window_job(id);
                Some(request)
            }
            Ok(None) => {
                self.unpark_window_job(id);
                self.window_due.remove(&id);
                None
            }
            Err(error) => {
                error!(job_id = id.0, %error, "failed to read a queued job entering the window");
                self.window_unreadable.insert(id);
                self.window_due.remove(&id);
                None
            }
        }
    }

    // After an install that entered the window: what the job's scripts asked
    // for while it was out of it is in force again.
    pub(crate) fn settle_window_entry(&mut self, id: JobId) {
        self.window_due.remove(&id);
        self.apply_queue_script_effects(id);
        if let Some(values) = self.active_runtime_values(id) {
            self.window_stored.insert(id, values);
        }
    }

    // Drop every installed job outside the window, keeping its row. What it
    // holds that its active row does not is written first.
    pub(crate) fn evict_outside_window(&mut self, ranked: &HashSet<JobId>) {
        let evicted: Vec<JobId> = self.shared_state.queue.read(|inner| {
            self.job_order
                .iter()
                .copied()
                .filter(|id| {
                    inner
                        .job(*id)
                        .is_some_and(|row| !self.in_queue_window(ranked, row))
                })
                .collect()
        });
        if evicted.is_empty() {
            return;
        }
        for id in &evicted {
            let id = *id;
            let values = self.active_runtime_values(id);
            if values.is_some() && self.window_stored.get(&id) != values.as_ref() {
                self.persist_active_runtime(id);
            }
            self.window_stored.remove(&id);
            self.release_job_articles(id);
            let Some(state) = self.jobs.remove(&id) else {
                continue;
            };
            self.park_window_job(id, state.articles.queued_download_len() as u64);
            self.job_last_download_activity.remove(&id);
            self.remove_pending_completion_check(id);
            self.download_wait_by_job.remove(&id);
            self.queue_scripts_completed.remove(&id);
            self.post_probes.forget(id);
            self.clear_job_progress_floor_runtime(id);
        }
        let evicted_set: HashSet<JobId> = evicted.into_iter().collect();
        self.job_order.retain(|id| !evicted_set.contains(id));
        self.update_queue_metrics();
        self.shared_state.queue.write(|inner| {
            for id in &evicted_set {
                if let Some(row) = inner.job_mut(*id) {
                    let reason = self.stored_wait_reason(row);
                    row.report.job.download_wait_reason = reason.map(str::to_owned);
                }
            }
            Change::changed(())
        });
    }

    // Forget the parked jobs whose rows are gone.
    pub(crate) fn forget_gone_parked(&self, inner: &QueueInner) -> Vec<JobId> {
        self.window_parked
            .keys()
            .copied()
            .filter(|id| !inner.contains_job(*id))
            .collect()
    }

    pub(crate) fn drop_parked(&mut self, gone: Vec<JobId>) {
        for id in gone {
            self.unpark_window_job(id);
            self.window_due.remove(&id);
            self.window_unreadable.remove(&id);
        }
    }
}
