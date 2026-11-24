// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_base::ingest::metadata;
pub use weaver_server_base::ingest::naming;
pub mod persisted_nzb;
pub mod xz;
pub use metadata::{ORIGINAL_TITLE_METADATA_KEY, append_original_title_metadata};
pub use naming::strip_nzb_source_suffix;
pub mod classify;
pub mod import;
pub mod outline;
pub mod outline_persistence;
pub mod submission_options;
pub use import::*;
pub use metadata::append_full_file_names_metadata;
pub use metadata::{renamed_title, set_renamed_title};
pub use naming::derive_release_name;
pub use persisted_nzb::*;
pub use xz::{XZ_DECODER_MEMORY_LIMIT_BYTES, xz_multistream_decoder};
