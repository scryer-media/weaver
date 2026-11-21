// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The writer's side of the job history cache: the ordered writer reads and
// fills the cache the composed database shares with its readers.

pub use weaver_server_history::history::cache::*;

use super::record::JobHistoryRow;
use crate::persistence::DatabaseWriterExecutor;

pub(crate) trait HistoryWriterCacheQueries {
    fn job_history_cache_generation(&self) -> u64;
    fn cache_job_history_at(&self, row: JobHistoryRow, observed_generation: u64);
}

impl HistoryWriterCacheQueries for DatabaseWriterExecutor {
    fn job_history_cache_generation(&self) -> u64 {
        self.job_history_cache
            .lock()
            .expect("job history cache poisoned")
            .generation()
    }

    fn cache_job_history_at(&self, row: JobHistoryRow, observed_generation: u64) {
        self.job_history_cache
            .lock()
            .expect("job history cache poisoned")
            .insert_if_generation(row, observed_generation);
    }
}
