// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The deepest a graph job's namespaces may nest: the ten-deep chain the
// one-engine plan delivers, with headroom. A child bound below it is
// refused, and the job fails naming depth.
pub(in crate::pipeline) const MAX_GRAPH_NESTING_DEPTH: u32 = 16;

pub(crate) mod check;
pub(crate) mod claims;
pub(crate) mod deobfuscate;
mod output;
pub(crate) use output::claim_complete_destination_path;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub(in crate::pipeline) use output::{
    CLAIM_FULL, DESTINATION_ROOM, FIXTURE_MOVE_HOOKS, FIXTURE_SPENT_HOOKS, PARTIAL_STAYS,
    member_crc32_by_path,
};
pub(crate) use output::{RestoredPostProcessing, release_failed_delivery};
mod status;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub(crate) use status::RUNTIME_WRITES;
pub(crate) use status::{ActiveRuntimeValues, StoredRuntimeValues};
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub(crate) use status::{MOVING_ROW_REFUSED, MOVING_ROWS_WRITTEN};
