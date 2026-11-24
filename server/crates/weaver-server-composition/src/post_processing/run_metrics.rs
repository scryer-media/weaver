// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-postproc; the tests open the composed database, so
// they stay here under the same module path.
pub use weaver_server_postproc::post_processing::run_metrics::*;

#[cfg(test)]
use super::model::{ScriptAdapter, ScriptResult};

#[cfg(test)]
mod tests;
