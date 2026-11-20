// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The deepest a graph job's namespaces may nest: the ten-deep chain the
// one-engine plan delivers, with headroom. A child bound below it is
// refused, and the job fails naming depth.
pub const MAX_GRAPH_NESTING_DEPTH: u32 = 16;

pub mod check;
pub(crate) mod claims;
pub mod deobfuscate;
pub mod output;
pub use output::claim_complete_destination_path;
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(in crate::pipeline) use output::{
    CLAIM_FULL, DESTINATION_ROOM, FIXTURE_MOVE_HOOKS, FIXTURE_SPENT_HOOKS, PARTIAL_STAYS,
    member_crc32_by_path,
};
pub(crate) use output::{RestoredPostProcessing, release_failed_delivery};
mod status;
pub(crate) use status::ActiveRuntimeValues;
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(crate) use status::RUNTIME_WRITES;
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(crate) use status::{MOVING_ROW_REFUSED, MOVING_ROWS_WRITTEN};
