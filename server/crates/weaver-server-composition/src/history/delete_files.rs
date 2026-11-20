// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to its domain crate; the tests open the composed database, so they
// stay here under the same module path.
pub use weaver_server_torrent_store::history::delete_files::*;

#[cfg(test)]
use crate::JobId;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
mod tests;
