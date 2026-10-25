// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a format object that cannot finish reads in the tree's record of its
// members once the job is quiet. Each object asks in its own order, beside
// what only it knows: a set's first volume, a password it is still asking
// for.

use weaver_graph::{Beside, Standing, VolumeIndex, Why};

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

// Where a member's facts put it in a set: the one place that fits it, or
// more than one place and nothing that chooses between them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placed {
    At(VolumeIndex),
    Unordered,
}

impl Placed {
    // The places that fit a member, in any order: none places it nowhere.
    pub(crate) fn of(mut fits: impl Iterator<Item = VolumeIndex>) -> Option<Self> {
        let first = fits.next()?;
        Some(match fits.next() {
            None => Self::At(first),
            Some(_) => Self::Unordered,
        })
    }

    // The volume a claim answers. A member the set needs and cannot order
    // stays unclaimed while the set stands, since a name may yet order it;
    // a set that was refused takes it at `past`, past every volume it holds,
    // so it goes where the set went.
    pub(crate) fn claimed(self, gone: bool, past: Option<VolumeIndex>) -> Option<VolumeIndex> {
        match self {
            Self::At(volume) => Some(volume),
            Self::Unordered => past.filter(|_| gone),
        }
    }

    // The index past every volume `last` and those before it hold.
    pub(crate) fn past(last: Option<u32>) -> Option<VolumeIndex> {
        last.map_or(Some(0), |last| last.checked_add(1))
            .map(VolumeIndex)
    }
}

// A member beside the set that the set needs and nothing orders: the set
// cannot say where it goes, so it reads none of its bytes.
pub(crate) fn unordered(
    standing: &dyn Standing,
    placed: impl Fn(&Beside<'_>) -> Option<Placed>,
) -> Option<Why> {
    standing
        .beside()
        .iter()
        .any(|beside| placed(beside) == Some(Placed::Unordered))
        .then_some(Why::OrderUnknown)
}
