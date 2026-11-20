// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

mod layout;
#[cfg(feature = "pipeline-test-fixtures")]
pub(super) mod units {
    pub mod layout {
        pub use super::super::layout::*;
    }
    pub mod decode {
        pub use super::super::*;
    }
}
mod worker;
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(in crate::pipeline) use worker::SegmentWriteError;

// In-flight decode task count per job, with their sum kept beside them so the
// stage gauge reads one number instead of walking every job on each decode.
// Every change goes through these methods, so the sum moves with the counts.
#[derive(Default)]
pub struct ActiveDecodes {
    by_job: HashMap<JobId, usize>,
    total: usize,
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    walks: std::sync::atomic::AtomicU64,
}

impl ActiveDecodes {
    pub(in crate::pipeline) fn get(&self, job_id: &JobId) -> Option<&usize> {
        self.by_job.get(job_id)
    }

    pub fn is_empty(&self) -> bool {
        self.by_job.is_empty()
    }

    // Decode tasks in flight across every job.
    pub fn total(&self) -> usize {
        self.total
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn started(&mut self, job_id: JobId) {
        *self.by_job.entry(job_id).or_default() += 1;
        self.total += 1;
    }

    pub fn finished(&mut self, job_id: JobId) {
        if let Some(active) = self.by_job.get_mut(&job_id) {
            if *active > 0 {
                *active -= 1;
                self.total -= 1;
            }
            if *active == 0 {
                self.by_job.remove(&job_id);
            }
        }
    }

    pub fn remove(&mut self, job_id: &JobId) -> Option<usize> {
        let removed = self.by_job.remove(job_id);
        self.total -= removed.unwrap_or(0);
        removed
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn contains_key(&self, job_id: &JobId) -> bool {
        self.by_job.contains_key(job_id)
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn insert(&mut self, job_id: JobId, count: usize) -> Option<usize> {
        let previous = self.remove(&job_id);
        self.by_job.insert(job_id, count);
        self.total += count;
        previous
    }

    // The jobs with a decode in flight.
    pub(in crate::pipeline) fn jobs(&self) -> impl Iterator<Item = &JobId> {
        self.by_job.keys()
    }

    // Every job's count, one walk of the map.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn values(&self) -> impl Iterator<Item = &usize> {
        self.walks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.by_job.values()
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn walks(&self) -> u64 {
        self.walks.load(std::sync::atomic::Ordering::Relaxed)
    }
}

use layout::{
    AuthoritativeLayoutError, ExpectedSegmentLayout, YencLayoutMismatch, declared_part_end,
    expected_segment_layout, format_authoritative_layout_error, format_withdrawn_placement,
    format_yenc_layout_conflict, format_yenc_layout_mismatch, placement_within_nzb_envelope,
    validate_yenc_layout,
};
