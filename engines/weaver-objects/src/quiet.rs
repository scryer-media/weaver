// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a format object that cannot finish reads in the tree's record of its
// members once the job is quiet. Each object asks in its own order, beside
// what only it knows: a set's first volume, a password it is still asking
// for.

use weaver_graph::{Standing, Why};

// A member still damaged that nothing repaired: what it holds is short.
pub(crate) fn damaged(standing: &dyn Standing) -> Option<Why> {
    standing.damaged().then_some(Why::Unrepairable)
}

// A member named and never placed, or none named at all: lost with the
// object's own bytes when those were lost, since what it never placed lay
// in them, and otherwise never mapped.
pub(crate) fn unplaced(standing: &dyn Standing) -> Option<Why> {
    standing.unplaced().then(|| match standing.lost() {
        true => Why::Unrepairable,
        false => Why::Unmapped,
    })
}
