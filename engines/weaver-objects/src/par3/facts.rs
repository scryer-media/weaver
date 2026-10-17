// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The PAR3 object keeps no facts yet: the trait's own body says so, and its
// restore refuses, so a restart reads its volumes again. Once its wave
// lands, its facts are the kept packet fronts of each volume (or the ranges
// they were read from), the described paths and the member each is bound
// to with the match that bound it, the blocks known damaged, the recovery
// blocks on hand by volume range, and the last repair's outcome.

use weaver_graph::{Facts, Object, RestoreError};

pub(crate) fn restore(facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
    Err(RestoreError::Unsupported { kind: facts.kind })
}
