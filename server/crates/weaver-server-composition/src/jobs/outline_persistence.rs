// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to its domain crate; the tests open the composed database, so they
// stay here under the same module path.
pub use weaver_server_ingest::ingest::outline_persistence::*;

#[cfg(test)]
use crate::ingest::outline::NzbOutline;
#[cfg(test)]
use crate::persistence::Database;
#[cfg(test)]
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, max_rows_for_engine};
#[cfg(test)]
use weaver_server_base::jobs::ids::JobId;
#[cfg(test)]
use weaver_server_jobs::jobs::outline_persistence::{decode_outline_files, encode_outline_files};

#[cfg(test)]
mod tests;
