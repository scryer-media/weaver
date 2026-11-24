// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-postproc; the tests open the composed database, so
// they stay here under the same module path.
pub use weaver_server_postproc::post_processing::settings::*;

#[cfg(test)]
use super::model::{PostProcessingSettings, ScriptResult, UnwantedExtensionAction};
#[cfg(test)]
use crate::persistence::Database;
#[cfg(test)]
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
#[cfg(test)]
use std::path::PathBuf;

#[cfg(test)]
mod tests;
