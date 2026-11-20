// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-settings; the tests open the composed database.
pub use weaver_server_settings::diagnostic_names::*;

#[cfg(test)]
use crate::persistence::Database;
#[cfg(test)]
use crate::persistence::sql_runtime::SqlRuntime;
#[cfg(test)]
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;
