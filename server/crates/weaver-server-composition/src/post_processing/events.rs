// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-postproc; the tests open the composed database, so
// they stay here under the same module path.
pub use weaver_server_postproc::post_processing::events::*;

#[cfg(test)]
use super::directives::Directive;
#[cfg(test)]
use super::instances::InstanceTrigger;
#[cfg(test)]
use super::model::{PostProcessingSettings, QueueEvent, ScriptEventLabel, ScriptStatus};
#[cfg(test)]
use super::runner::JobExecutionContext;
#[cfg(test)]
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
#[cfg(test)]
use crate::persistence::{Database, StateError};
#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use tokio::sync::watch;

#[cfg(test)]
#[path = "event_tests.rs"]
mod tests;
