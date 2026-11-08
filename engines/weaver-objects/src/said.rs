// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a format object has said of its members' bytes, taken back when
// those bytes are to come again.

use weaver_graph::{Coverage, Placement};

// Each member `placements` reach forgets what was said of the bytes placed
// in it and is open until they are said again. `said` is where the object
// keeps its coverage of a member and whether that member is closed.
pub(crate) fn unposted<T>(
    placements: &[Placement],
    members: &mut [T],
    said: fn(&mut T) -> (&mut Coverage, &mut bool),
) {
    for placement in placements {
        if let Placement::Member { member, range } = placement
            && !range.is_empty()
            && let Some(out) = members.get_mut(member.0 as usize)
        {
            let (coverage, closed) = said(out);
            coverage.erase(*range);
            *closed = false;
        }
    }
}
