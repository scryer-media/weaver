// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod analysis;
pub mod content_sniff;
pub mod delivery_rename;
pub mod deobfuscate;
pub mod error;
pub mod parser;
pub mod types;

pub use content_sniff::{SNIFF_BYTES, sniff_extension};
pub use delivery_rename::{
    DeliveredFile, PlannedRename, lacks_extension, looks_obfuscated_for_delivery,
    plan_content_extensions, plan_renames, select_rename_candidate,
};
pub use deobfuscate::{
    contains_protected_media_structure, extract_filename, is_obfuscated,
    is_protected_media_structure,
};
pub use error::NzbError;
pub use parser::{
    FileParseDiagnostics, ParseDiagnostics, parse_nzb, parse_nzb_reader, parse_nzb_with_diagnostics,
};
pub use types::{Nzb, NzbFile, NzbMeta, NzbSegment};
