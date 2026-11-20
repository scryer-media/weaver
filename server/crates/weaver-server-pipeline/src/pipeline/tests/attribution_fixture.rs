// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::{
    JobsServerAttributionPersistenceInternalQueries, server_attribution::JobServerAttribution,
};
pub fn note_article(pipeline: &mut Pipeline, id: JobId, server: u32, bytes: u64) {
    pipeline
        .jobs
        .get_mut(&id)
        .unwrap()
        .server_attribution
        .note_article(server, bytes);
    pipeline.dirty_server_attribution.insert(id);
}
pub fn checkpoint_at(pipeline: &mut Pipeline, at: Instant) {
    pipeline.server_attribution_checkpoint_at = at;
    pipeline.checkpoint_server_attribution_if_due();
}
pub fn dirty_jobs(pipeline: &Pipeline) -> &std::collections::HashSet<JobId> {
    &pipeline.dirty_server_attribution
}
pub fn flush(pipeline: &mut Pipeline) {
    pipeline.flush_server_attribution();
}
pub fn clear_floor(pipeline: &mut Pipeline, id: JobId) {
    pipeline.clear_job_progress_floor_runtime(id);
}
pub fn attribution(pipeline: &Pipeline, id: JobId) -> &JobServerAttribution {
    &pipeline.jobs[&id].server_attribution
}
pub fn stored(pipeline: &Pipeline, id: JobId) -> JobServerAttribution {
    pipeline.db.load_active_server_attribution(id).unwrap()
}
pub fn save(pipeline: &Pipeline, id: JobId, json: String) {
    pipeline
        .db
        .save_active_server_attribution(vec![(id, json)])
        .unwrap();
}
