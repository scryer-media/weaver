// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{insert_active_job, new_direct_pipeline, standalone_job_spec};
use crate::{JobId, Pipeline};
use std::time::{Duration, Instant};
use tempfile::TempDir;

// Seconds since the epoch, as the NZB's `date` attribute carries it.
pub fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

/// An idle pipeline containing posts with the dates supplied by a dispatch test.
pub struct PropagationFixture(Pipeline);

impl PropagationFixture {
    pub async fn new(temp: &TempDir, delay: Duration) -> Self {
        let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
        pipeline.propagation_delay = delay;
        Self(pipeline)
    }

    pub async fn add_post(&mut self, id: JobId, name: &str, files: &[(&str, Option<u64>)]) {
        let sizes: Vec<_> = files
            .iter()
            .map(|(file, _)| (file.to_string(), 512))
            .collect();
        let mut spec = standalone_job_spec(name, &sizes);
        for (file, (_, posted_at)) in spec.files.iter_mut().zip(files) {
            file.posted_at_epoch = *posted_at;
        }
        insert_active_job(&mut self.0, id, spec).await;
    }

    pub fn propagation_hold_until(&mut self, id: JobId) -> Option<Instant> {
        self.0.propagation_hold_until(id)
    }

    pub fn has_cached_hold(&self, id: JobId) -> bool {
        self.0.propagation_ready_at.contains_key(&id)
    }

    pub fn cached_hold_count(&self) -> usize {
        self.0.propagation_ready_at.len()
    }

    pub fn next_propagation_delay(&self) -> Option<Duration> {
        self.0.next_propagation_delay()
    }
}
