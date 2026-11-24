// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::database::HistoryDatabase;
use super::record::JobHistoryRow;
use std::collections::{BTreeMap, VecDeque};

const JOB_HISTORY_CACHE_CAPACITY: usize = 1024;

#[derive(Default)]
pub struct JobHistoryCache {
    rows: BTreeMap<u64, JobHistoryRow>,
    insertion_order: VecDeque<u64>,
    // Bumped on every invalidation (`remove`/`clear`). A cache-populate site
    // that captured a generation *before* its DB read may only insert when the
    // generation still matches; otherwise an intervening delete has moved the
    // cache forward and the row it read is stale, so the insert is dropped.
    generation: u64,
}

impl JobHistoryCache {
    pub fn get(&self, job_id: u64) -> Option<JobHistoryRow> {
        self.rows.get(&job_id).cloned()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn insert(&mut self, row: JobHistoryRow) {
        if !self.rows.contains_key(&row.job_id) {
            self.insertion_order.push_back(row.job_id);
        }
        self.rows.insert(row.job_id, row);
        while self.rows.len() > JOB_HISTORY_CACHE_CAPACITY {
            let Some(job_id) = self.insertion_order.pop_front() else {
                break;
            };
            self.rows.remove(&job_id);
        }
    }

    // Insert a row read from the database only if no invalidation happened
    // since `observed_generation` was captured. Guards the read-then-cache
    // race with a concurrent delete: a reader that fetched the row just before
    // a delete would otherwise re-populate it after the delete's invalidation.
    pub fn insert_if_generation(&mut self, row: JobHistoryRow, observed_generation: u64) {
        if self.generation == observed_generation {
            self.insert(row);
        }
    }

    pub fn remove(&mut self, job_id: u64) {
        self.rows.remove(&job_id);
        self.insertion_order
            .retain(|cached_id| *cached_id != job_id);
        self.generation = self.generation.wrapping_add(1);
    }
}

pub trait HistoryCacheQueries {
    fn get_cached_job_history(&self, job_id: u64) -> Option<JobHistoryRow>;
    fn job_history_cache_generation(&self) -> u64;
    fn cache_job_history_at(&self, row: JobHistoryRow, observed_generation: u64);
    fn invalidate_job_history_cache(&self, job_id: u64);
    fn invalidate_job_history_cache_for(&self, job_ids: &[u64]);
}

impl<D: HistoryDatabase> HistoryCacheQueries for D {
    fn get_cached_job_history(&self, job_id: u64) -> Option<JobHistoryRow> {
        self.job_history_cache()
            .lock()
            .expect("job history cache poisoned")
            .get(job_id)
    }

    // Snapshot the cache invalidation generation before a DB read whose result
    // will be cached. Pair with [`Self::cache_job_history_at`] to insert only
    // if no invalidation raced the read.
    fn job_history_cache_generation(&self) -> u64 {
        self.job_history_cache()
            .lock()
            .expect("job history cache poisoned")
            .generation()
    }

    // Cache a freshly-read row only if the cache generation still matches
    // `observed_generation` (captured before the read). Drops the insert when a
    // concurrent delete has invalidated the cache in between, preventing a
    // deleted job from being resurrected in the cache.
    fn cache_job_history_at(&self, row: JobHistoryRow, observed_generation: u64) {
        self.job_history_cache()
            .lock()
            .expect("job history cache poisoned")
            .insert_if_generation(row, observed_generation);
    }

    fn invalidate_job_history_cache(&self, job_id: u64) {
        self.invalidate_job_history_cache_for(&[job_id]);
    }

    fn invalidate_job_history_cache_for(&self, job_ids: &[u64]) {
        let mut cache = self
            .job_history_cache()
            .lock()
            .expect("job history cache poisoned");
        for job_id in job_ids {
            cache.remove(*job_id);
        }
    }
}
