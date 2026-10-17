// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The PAR2 object keeps no facts yet: the trait's own body says so, and its
// restore refuses, so a restart reads its volumes again. Once repair at
// depth lands, its facts are each set's main and file description packets
// (or the volume ranges they were read from), which members it covers and
// by which match (file id, name, or first 16 KiB), each member's length,
// the slice checksum grid as far as it was folded, the recovery slices on
// hand by volume range, and the last repair's outcome.

use weaver_graph::{Facts, Object, RestoreError};

pub(crate) fn restore(facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
    Err(RestoreError::Unsupported { kind: facts.kind })
}
