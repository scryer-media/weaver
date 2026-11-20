// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The harvest as the pair these tests assert on: the candidates, and whether
// the job remembers the half it read from its row.
pub fn harvest(pipeline: &Pipeline, job_id: JobId) -> (Vec<ArchivePasswordCandidate>, bool) {
    let candidates = pipeline.archive_password_candidates_for_job(job_id);
    let remembered = pipeline
        .jobs
        .get(&job_id)
        .is_some_and(|state| state.nzb_password_candidates.get().is_some());
    (candidates, remembered)
}

// The job as one restored from its row holds it: nothing of its NZB's half
// in memory, so the first harvest reads the row.
pub fn forget_harvest(pipeline: &mut Pipeline, job_id: JobId) {
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .nzb_password_candidates
        .take();
}

pub async fn add_job(
    pipeline: &mut Pipeline,
    id: JobId,
    spec: JobSpec,
    path: PathBuf,
    nzb: Vec<u8>,
    options: crate::jobs::AddJobOptions,
) -> Result<(), crate::SchedulerError> {
    pipeline.add_job(id, spec, path, nzb, options).await
}

pub fn queued_articles(pipeline: &Pipeline, id: JobId) -> usize {
    pipeline.jobs[&id].articles.queued_download_len()
}

pub async fn reprocess_job(
    pipeline: &mut Pipeline,
    id: JobId,
) -> Result<(), crate::SchedulerError> {
    pipeline.reprocess_job(id).await
}

pub async fn redownload_job_with_password(
    pipeline: &mut Pipeline,
    id: JobId,
    password: Option<String>,
) -> Result<(), crate::SchedulerError> {
    pipeline.redownload_job_with_password(id, password).await
}

pub async fn reprocess_job_with_password(
    pipeline: &mut Pipeline,
    id: JobId,
    password: Option<String>,
) -> Result<(), crate::SchedulerError> {
    pipeline.reprocess_job_with_password(id, password).await
}

pub async fn reprocess_job_with_no_memory_limit(
    pipeline: &mut Pipeline,
    id: JobId,
    password: Option<String>,
) -> Result<(), crate::SchedulerError> {
    pipeline
        .reprocess_job_with_no_memory_limit(id, password)
        .await
}
