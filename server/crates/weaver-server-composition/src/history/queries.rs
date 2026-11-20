// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job history queries live in the history crate; their tests open the
// composed database, so they stay here under the same module path.
pub use weaver_server_history::history::job_history_queries::*;

#[cfg(test)]
mod order_index_tests;
