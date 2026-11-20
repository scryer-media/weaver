// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The queue's dispatch order, the one every part of the pipeline that ranks
// jobs takes its order from: the queue window, slot admission, the jobs next
// up, and the download scheduler's hot job and spills.
//
// A job ranks by its priority (high, normal, low), then by where its row
// stands in the queue. The operator's moves, priority edits, adds, removals
// and completions all change one of those two, so each reaches every ranking
// the same way. Each consumer chooses which jobs it ranks; none orders them
// any other way.

use super::{Globals, JobRow, QueueInner};
use crate::jobs::JobId;

// A job's place in dispatch order. Keys compare by priority, then position,
// then id; positions are unique, so the id only makes the order total.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DispatchKey {
    priority: u8,
    position: usize,
    job_id: JobId,
}

impl DispatchKey {
    pub fn new(priority: u8, position: usize, job_id: JobId) -> Self {
        Self {
            priority,
            position,
            job_id,
        }
    }

    // The key of the row standing at `position`, its priority read with any
    // edit not yet applied in force.
    pub(crate) fn of_row(row: &JobRow, position: usize, globals: &Globals) -> Self {
        Self::new(row_priority(row, globals), position, row.id)
    }

    pub fn job_id(self) -> JobId {
        self.job_id
    }
}

// A job's priority from its metadata: high 0, normal 1, low 2.
pub fn metadata_priority(metadata: &[(String, String)]) -> u8 {
    metadata
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("priority"))
        .map_or(1, |(_, value)| {
            if value.eq_ignore_ascii_case("high") {
                0
            } else if value.eq_ignore_ascii_case("low") {
                2
            } else {
                1
            }
        })
}

// The row's priority, with an edit not yet applied in force. Only a pending
// edit can move it off the reported metadata.
pub(crate) fn row_priority(row: &JobRow, globals: &Globals) -> u8 {
    if row.intents.update.is_some() {
        metadata_priority(&row.info(globals).metadata)
    } else {
        metadata_priority(&row.report.job.metadata)
    }
}

// Keeps the first `n` keys in dispatch order, in no particular order, by a
// selection rather than a sort.
pub fn keep_first(keys: &mut Vec<DispatchKey>, n: usize) {
    if n == 0 {
        keys.clear();
    } else if keys.len() > n {
        keys.select_nth_unstable(n - 1);
        keys.truncate(n);
    }
}

impl QueueInner {
    // The key of every row `keep` accepts, in one walk over the rows.
    pub(crate) fn dispatch_keys(&self, mut keep: impl FnMut(&JobRow) -> bool) -> Vec<DispatchKey> {
        self.jobs
            .iter()
            .enumerate()
            .filter(|(_, row)| keep(row))
            .map(|(position, row)| DispatchKey::of_row(row, position, &self.globals))
            .collect()
    }

    // Where a job stands for dispatch: its row's position, or, for a job
    // installed without a row, after every row in install order.
    pub(crate) fn dispatch_position(&self, id: JobId, installed: &[JobId]) -> Option<usize> {
        self.job_position(id).or_else(|| {
            installed
                .iter()
                .position(|installed| *installed == id)
                .map(|index| self.jobs.len() + index)
        })
    }
}
