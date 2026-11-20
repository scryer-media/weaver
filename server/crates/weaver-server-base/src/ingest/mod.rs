// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod metadata;
pub mod naming;
pub use metadata::{ORIGINAL_TITLE_METADATA_KEY, append_original_title_metadata};
pub use naming::strip_nzb_source_suffix;
