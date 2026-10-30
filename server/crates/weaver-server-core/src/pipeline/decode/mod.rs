// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

mod layout;
mod worker;

// In-flight decode task count per job, with their sum kept beside them so the
// stage gauge reads one number instead of walking every job on each decode.
// Every change goes through these methods, so the sum moves with the counts.
#[derive(Default)]
pub(crate) struct ActiveDecodes {
    by_job: HashMap<JobId, usize>,
    total: usize,
    #[cfg(test)]
    walks: std::sync::atomic::AtomicU64,
}

impl ActiveDecodes {
    pub(in crate::pipeline) fn get(&self, job_id: &JobId) -> Option<&usize> {
        self.by_job.get(job_id)
    }

    pub(in crate::pipeline) fn is_empty(&self) -> bool {
        self.by_job.is_empty()
    }

    // Decode tasks in flight across every job.
    pub(in crate::pipeline) fn total(&self) -> usize {
        self.total
    }

    pub(in crate::pipeline) fn started(&mut self, job_id: JobId) {
        *self.by_job.entry(job_id).or_default() += 1;
        self.total += 1;
    }

    pub(in crate::pipeline) fn finished(&mut self, job_id: JobId) {
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

    pub(in crate::pipeline) fn remove(&mut self, job_id: &JobId) -> Option<usize> {
        let removed = self.by_job.remove(job_id);
        self.total -= removed.unwrap_or(0);
        removed
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn contains_key(&self, job_id: &JobId) -> bool {
        self.by_job.contains_key(job_id)
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn insert(&mut self, job_id: JobId, count: usize) -> Option<usize> {
        let previous = self.remove(&job_id);
        self.by_job.insert(job_id, count);
        self.total += count;
        previous
    }

    // Every job's count, one walk of the map.
    #[cfg(test)]
    pub(in crate::pipeline) fn values(&self) -> impl Iterator<Item = &usize> {
        self.walks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.by_job.values()
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn walks(&self) -> u64 {
        self.walks.load(std::sync::atomic::Ordering::Relaxed)
    }
}

use layout::{
    AuthoritativeLayoutError, ExpectedSegmentLayout, YencLayoutMismatch, declared_part_end,
    expected_segment_layout, format_authoritative_layout_error, format_yenc_layout_mismatch,
    placement_within_nzb_envelope, validate_yenc_layout,
};

#[cfg(test)]
mod active_decodes_tests {
    use super::*;

    // The kept sum is the sum of the per-job counts after every kind of change.
    #[test]
    fn the_kept_sum_follows_every_change_to_the_counts() {
        let mut active = ActiveDecodes::default();
        let check = |active: &ActiveDecodes| {
            assert_eq!(active.total(), active.values().sum::<usize>());
        };
        active.started(JobId(1));
        active.started(JobId(1));
        active.started(JobId(2));
        check(&active);
        assert_eq!(active.total(), 3);
        active.finished(JobId(1));
        active.finished(JobId(3));
        check(&active);
        assert_eq!(active.total(), 2);
        active.finished(JobId(2));
        assert!(!active.contains_key(&JobId(2)));
        assert_eq!(active.remove(&JobId(1)), Some(1));
        assert_eq!(active.remove(&JobId(1)), None);
        check(&active);
        assert!(active.is_empty());
        assert_eq!(active.insert(JobId(4), 2), None);
        assert_eq!(active.insert(JobId(4), 5), Some(2));
        active.insert(JobId(5), 0);
        active.finished(JobId(5));
        check(&active);
        assert_eq!(active.total(), 5);
        assert!(!active.contains_key(&JobId(5)));
    }
}
