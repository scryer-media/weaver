// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a submission is added with, and how one fails: shared by the
// submission paths in the pipeline and the post-processing scan below it.
use weaver_server_jobs::jobs::duplicate::{
    CallerScopedIdempotency, DuplicateDecision, DuplicateMode, SemanticDuplicate, SubmissionOrigin,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionOptions {
    pub add_paused: bool,
    pub duplicate_mode: DuplicateMode,
    pub semantic_duplicate: Option<SemanticDuplicate>,
    pub origin: SubmissionOrigin,
    pub idempotency: Option<CallerScopedIdempotency>,
    // Archive passwords supplied beside the job's own: a calling integration's
    // (`integration_archive_password_candidates`), then those weaver read from
    // its own NZB fetch (`header_archive_password_candidates`), in that order.
    pub supplied_password_candidates: Vec<weaver_server_jobs::jobs::ArchivePasswordCandidate>,
    // The tags the job is added with. A job a duplicate check parks, or a
    // replay of one already added, takes none from this submit.
    pub tags: Vec<String>,
}

impl Default for SubmissionOptions {
    fn default() -> Self {
        Self {
            add_paused: false,
            duplicate_mode: DuplicateMode::Enforce,
            semantic_duplicate: None,
            origin: SubmissionOrigin::Api,
            idempotency: None,
            supplied_password_candidates: Vec::new(),
            tags: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubmitNzbError {
    #[error("NZB parse error: {0}")]
    Parse(#[from] weaver_nzb::NzbError),
    #[error("NZB contains no files")]
    Empty,
    #[error("failed to save NZB: {0}")]
    Save(std::io::Error),
    #[error("failed to read uploaded NZB: {0}")]
    Upload(std::io::Error),
    #[error("scheduler error: {0}")]
    Scheduler(#[from] weaver_server_jobs::jobs::error::SchedulerError),
    #[error("state error: {0}")]
    State(#[from] weaver_server_db::persistence::StateError),
    #[error("NZB fetch failed: {0}")]
    Fetch(String),
    #[error("NZB response was not valid XML")]
    NotXml,
    #[error("invalid category: {0}")]
    InvalidCategory(#[from] weaver_server_settings::categories::CategoryValidationError),
    #[error("duplicate submission blocked")]
    DuplicateBlocked { decision: DuplicateDecision },
    #[error("idempotency key is already bound to job {job_id}")]
    IdempotencyConflict { job_id: u64 },
}

// The next job number a submission takes. Start-up recovery sets it past the
// highest stored job; every submission path draws from it.
static NEXT_API_JOB_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(10_000);

pub fn init_job_counter(start: u64) {
    NEXT_API_JOB_ID.store(start.max(10_000), std::sync::atomic::Ordering::Relaxed);
}

pub fn next_submission_job_id() -> weaver_server_base::jobs::ids::JobId {
    weaver_server_base::jobs::ids::JobId(
        NEXT_API_JOB_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    )
}
