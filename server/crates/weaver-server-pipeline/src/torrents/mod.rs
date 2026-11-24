// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The torrents as the pipeline sees them: the composition crate's torrents,
// plus the class an unavailable server seats on the scheduler.
pub use weaver_server_composition::torrents::*;

pub mod availability;
