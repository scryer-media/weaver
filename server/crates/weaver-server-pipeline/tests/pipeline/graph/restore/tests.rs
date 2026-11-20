// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::VolumeIndex;

use super::{GraphError, GraphRestoreError, RestoreFailure};
use crate::pipeline::graph::root::source::Inconsistent;

// A record the rebuild finds contradicting itself would fault again on
// every start: the job starts afresh, as from a checkpoint this build
// does not restore. A rebuild that failed writing is still held.
#[test]
fn a_rebuild_that_finds_its_record_inconsistent_starts_afresh() {
    let inconsistent = GraphError::Inconsistent(Inconsistent {
        volume: VolumeIndex(0),
        start: 27,
        end: 28,
    });
    assert!(GraphRestoreError::Rebuild(RestoreFailure::Step(inconsistent)).not_this_builds());
    let write = GraphError::Write(
        crate::pipeline::graph::namespace::WriteError::UnknownMember {
            node: weaver_graph::NodeId::ROOT,
            member: weaver_graph::MemberId(9),
        },
    );
    assert!(!GraphRestoreError::Rebuild(RestoreFailure::Step(write)).not_this_builds());
}
