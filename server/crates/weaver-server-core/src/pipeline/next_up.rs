// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The jobs next up, their article lists read ahead.
//
// A job waiting its turn holds no article list. The few jobs that take the
// next slots, in dispatch order, have theirs read from their stored NZBs off
// the pipeline thread as they come up, so a job that gets a slot builds its
// articles from lists already in hand. A job that falls out of the next few
// without starting drops its lists. The lists read ahead are checked against
// the job's files as any read is, and a job whose lists do not match, or did
// not read, reads them again as it activates.

use super::*;
use crate::jobs::model::{JobArticles, SegmentSpec};

// How many jobs past the slots have their lists read ahead.
pub(crate) const NEXT_UP_DEPTH: usize = 10;

pub(crate) struct NextUp {
    pub(crate) depth: usize,
    entries: HashMap<JobId, Entry>,
    next_read: u64,
}

enum Entry {
    // A read is out, told by its number.
    Reading(u64),
    Read(Vec<Vec<SegmentSpec>>),
    // The read failed: the job reads its lists as it activates.
    Unread,
}

impl Default for NextUp {
    fn default() -> Self {
        Self {
            depth: NEXT_UP_DEPTH,
            entries: HashMap::new(),
            next_read: 0,
        }
    }
}

impl NextUp {
    // The lists read ahead for the job, taken.
    pub(crate) fn take(&mut self, job_id: JobId) -> Option<Vec<Vec<SegmentSpec>>> {
        match self.entries.remove(&job_id)? {
            Entry::Read(lists) => Some(lists),
            Entry::Reading(_) | Entry::Unread => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn held(&self) -> Vec<JobId> {
        let mut held: Vec<JobId> = self
            .entries
            .iter()
            .filter(|(_, entry)| matches!(entry, Entry::Read(_)))
            .map(|(id, _)| *id)
            .collect();
        held.sort_unstable();
        held
    }

    #[cfg(test)]
    pub(crate) fn reading(&self) -> bool {
        self.entries
            .values()
            .any(|entry| matches!(entry, Entry::Reading(_)))
    }
}

impl Pipeline {
    // The jobs next up: installed jobs that never started, waiting for a
    // slot, not paused, in dispatch order, as many as the depth.
    pub(in crate::pipeline) fn next_up_jobs(&self) -> Vec<JobId> {
        let depth = self.next_up.depth;
        if depth == 0 {
            return Vec::new();
        }
        self.shared_state.queue.read(|inner| {
            if inner.globals.shutting_down {
                return Vec::new();
            }
            let mut waiting = inner.dispatch_keys(|row| {
                row.intents.cancelled.is_none()
                    && !row.report.retiring
                    && !self.nzb_admitted.contains(&row.id)
                    && self.jobs.get(&row.id).is_some_and(|state| {
                        matches!(state.articles, JobArticles::Queued(_))
                            && state.status == JobStatus::Queued
                            && state.run_state != crate::jobs::model::RunState::Paused
                    })
            });
            crate::pipeline::queue::order::keep_first(&mut waiting, depth);
            waiting.into_iter().map(|key| key.job_id()).collect()
        })
    }

    // Read ahead the lists of the jobs now next up, and drop those of the
    // jobs that are not. A job that took a slot keeps its lists until it
    // activates. Each read waits for the writes queued for its job, then
    // streams its stored NZB on a blocking thread.
    pub(crate) fn refresh_next_up(&mut self) {
        let wanted: HashSet<JobId> = self.next_up_jobs().into_iter().collect();
        let admitted = &self.nzb_admitted;
        let jobs = &self.jobs;
        self.next_up.entries.retain(|id, _| {
            wanted.contains(id)
                || (admitted.contains(id)
                    && jobs
                        .get(id)
                        .is_some_and(|state| matches!(state.articles, JobArticles::Queued(_))))
        });
        for job_id in wanted {
            if self.next_up.entries.contains_key(&job_id) {
                continue;
            }
            self.next_up.next_read += 1;
            let read = self.next_up.next_read;
            self.next_up.entries.insert(job_id, Entry::Reading(read));
            let (written_tx, written_rx) = tokio::sync::oneshot::channel();
            if let Err(error) = self
                .db
                .try_queue_job_write(job_id, "next_up_read", move |_| {
                    let _ = written_tx.send(());
                    Ok(())
                })
            {
                warn!(job_id = job_id.0, error = %error, "could not queue the job's read ahead behind its writes");
            }
            let db = self.db.clone();
            let replies = self.db_reply_tx.clone();
            tokio::spawn(async move {
                let _ = written_rx.await;
                let lists = tokio::task::spawn_blocking(move || {
                    super::activation::read_stored_lists(&db, job_id).map(|(_, read)| read.lists)
                })
                .await
                .unwrap_or_else(|error| Err(format!("read ahead worker failed: {error}")));
                let _ = replies.send(crate::pipeline::db_reply::DbReply::NextUp {
                    job_id,
                    read,
                    lists,
                });
            });
        }
    }

    // A read ahead came back. Only the job's current read is kept.
    pub(crate) fn next_up_read(
        &mut self,
        job_id: JobId,
        read: u64,
        lists: Result<Vec<Vec<SegmentSpec>>, String>,
    ) {
        let Some(entry) = self.next_up.entries.get_mut(&job_id) else {
            return;
        };
        if !matches!(entry, Entry::Reading(current) if *current == read) {
            return;
        }
        *entry = match lists {
            Ok(lists) => Entry::Read(lists),
            Err(error) => {
                debug!(job_id = job_id.0, %error, "could not read the job's lists ahead");
                Entry::Unread
            }
        };
    }
}
