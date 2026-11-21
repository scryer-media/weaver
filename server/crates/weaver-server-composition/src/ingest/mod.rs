// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub use persisted_nzb::NZB_PARSES;
pub use weaver_server_ingest::ingest::classify;
pub use weaver_server_ingest::ingest::import;
pub use weaver_server_ingest::ingest::metadata;
pub use weaver_server_ingest::ingest::naming;
pub use weaver_server_ingest::ingest::outline;
pub use weaver_server_ingest::ingest::persisted_nzb;
use weaver_server_ingest::ingest::submission_options;
mod torrent_fetch;
use weaver_server_ingest::ingest::xz;

pub use classify::{Kind, classify, declared_protocol};
pub use import::{
    ImportError, dnzb_header_passwords, header_archive_password_candidates, import_nzb,
    integration_archive_password_candidates, normalize_archive_password_candidate,
    nzb_password_candidates, nzb_to_spec,
};
pub use metadata::full_file_name_never_kept;
pub use metadata::{
    FULL_FILE_NAMES_METADATA_KEY, FileRename, FullFileName, ORIGINAL_TITLE_METADATA_KEY,
    RENAMED_TITLE_METADATA_KEY, append_full_file_names_metadata, append_original_title_metadata,
    full_file_names, full_file_names_record, original_release_title, parse_full_file_names,
    record_file_renames, renamed_title, set_renamed_title,
};
pub use naming::{completed_folder_name, derive_release_name, strip_nzb_source_suffix};
pub use persisted_nzb::{
    PersistedNzbError, PreparedPersistedNzb, compress_nzb_bytes, decode_persisted_nzb_bytes,
    hash_persisted_nzb_bytes, load_persisted_nzb_storage_bytes, parse_and_hash_persisted_nzb_bytes,
    parse_persisted_nzb_bytes, persist_decoded_nzb_reader_to_zstd,
};
pub use submission_options::{
    SubmissionOptions, SubmitNzbError, init_job_counter, next_submission_job_id,
};
pub use torrent_fetch::fetch_torrent_url;
pub use xz::{XZ_DECODER_MEMORY_LIMIT_BYTES, xz_multistream_decoder};

// Opens a decoder for bzip2 input that reads every stream in it, as Python's
// `BZ2File` does for an `.nzb.bz2`.
pub fn bzip2_multistream_decoder<R: std::io::Read>(reader: R) -> bzip2::read::MultiBzDecoder<R> {
    bzip2::read::MultiBzDecoder::new(reader)
}
