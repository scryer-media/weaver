// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to the jobs crate, below the jobs queries that read these keys; the
// tests stay here under the same module path.
pub use weaver_server_jobs::jobs::attributes::*;

#[cfg(test)]
mod tests;
