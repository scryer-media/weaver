// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to the network runtime; the tests stay here under the same module
// path.
pub use weaver_server_net_runtime::ingest::torrent_fetch::*;

#[cfg(test)]
use crate::proxies::TorrentScope;

#[cfg(test)]
mod tests;
