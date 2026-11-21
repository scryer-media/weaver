// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Intake above the pipeline: the pipeline's ingest, plus the
// submission paths, which hand jobs to the scheduler.
pub use weaver_server_composition::ingest::*;

mod submission;

pub use submission::{
    DuplicateBackfillReport, FetchedNzb, StagedSubmissionPreparation, SubmissionDuplicateOutcome,
    SubmittedJob, SubmittedJobSummary, fetch_nzb_from_url, materialize_semantic_promotion,
    nzb_to_submission_spec, reconcile_duplicate_fingerprint_backfill,
    reconcile_semantic_promotions, resolve_submission_category,
    run_duplicate_fingerprint_backfill_batch, submit_nzb_bytes, submit_nzb_bytes_with_options,
    submit_staged_nzb_zstd, submit_staged_nzb_zstd_with_options,
    submit_staged_parsed_nzb_with_options, submit_staged_prepared_nzb_with_options,
    submit_uploaded_nzb_reader, submit_uploaded_nzb_reader_with_options,
};
