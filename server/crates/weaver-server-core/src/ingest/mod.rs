mod import;
pub mod metadata;
pub mod naming;
mod persisted_nzb;
pub mod release_parser;
mod submission;

pub use import::{ImportError, import_nzb, nzb_to_spec};
pub use metadata::{
    ORIGINAL_TITLE_METADATA_KEY, append_original_title_metadata, original_release_title,
};
pub use naming::derive_release_name;
pub use persisted_nzb::{
    PersistedNzbError, PersistedNzbReader, cleanup_orphaned_persisted_nzbs, hash_persisted_nzb,
    hash_persisted_nzb_or_empty, open_persisted_nzb_reader, parse_persisted_nzb,
    persist_decoded_nzb_reader, remove_persisted_nzb_if_exists, write_compressed_nzb,
};
pub use release_parser::parse_job_release;
pub use submission::{
    SubmitNzbError, SubmittedJob, fetch_nzb_from_url, init_job_counter, next_submission_job_id,
    nzb_storage_dir, nzb_to_submission_spec, resolve_submission_category, submit_nzb_bytes,
    submit_uploaded_nzb_reader,
};
