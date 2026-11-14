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
#[cfg(test)]
pub(in crate::pipeline) use output::member_crc32_by_path;
#[cfg(test)]
pub(in crate::pipeline) use output::{
    BEFORE_FIRST_MOVE, BEFORE_SPENT_REMOVAL, CLAIM_FULL, DESTINATION_ROOM, PARTIAL_STAYS,
};
pub(crate) use output::{RestoredPostProcessing, release_failed_delivery};
mod status;
pub(crate) use status::{ActiveRuntimeValues, StoredRuntimeValues};
#[cfg(test)]
pub(crate) use status::{MOVING_ROW_REFUSED, MOVING_ROWS_WRITTEN, RUNTIME_WRITES};
