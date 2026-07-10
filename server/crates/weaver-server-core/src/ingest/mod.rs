mod import;
pub mod metadata;
pub mod naming;
mod persisted_nzb;
mod submission;

pub use import::{
    ImportError, import_nzb, normalize_archive_password_candidate, nzb_password_candidates,
    nzb_to_spec,
};
pub use metadata::{
    ORIGINAL_TITLE_METADATA_KEY, append_original_title_metadata, original_release_title,
};
pub use naming::derive_release_name;
pub use persisted_nzb::{
    PersistedNzbError, compress_nzb_bytes, decode_persisted_nzb_bytes, hash_persisted_nzb_bytes,
    load_persisted_nzb_storage_bytes, parse_persisted_nzb_bytes,
    persist_decoded_nzb_reader_to_zstd,
};
pub use submission::{
    CategoryResolutionMode, DuplicateBackfillReport, SubmissionDuplicateOutcome, SubmissionOptions,
    SubmitNzbError, SubmittedJob, fetch_nzb_from_url, init_job_counter,
    materialize_semantic_promotion, next_submission_job_id, nzb_to_submission_spec,
    reconcile_duplicate_fingerprint_backfill, reconcile_semantic_promotions,
    resolve_submission_category, run_duplicate_fingerprint_backfill_batch, submit_nzb_bytes,
    submit_nzb_bytes_with_options, submit_staged_nzb_zstd, submit_staged_nzb_zstd_with_options,
    submit_uploaded_nzb_reader, submit_uploaded_nzb_reader_with_options,
};
