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
// every place that does, held or not, when more than one fits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Placed {
    At(VolumeIndex),
    Among(Vec<VolumeIndex>),
}

impl Placed {
    // The places that fit a member, in any order: none places it nowhere.
    pub(crate) fn of(fits: impl Iterator<Item = VolumeIndex>) -> Option<Self> {
        let fits: Vec<VolumeIndex> = fits.collect();
        match fits[..] {
            [] => None,
            [only] => Some(Self::At(only)),
            _ => Some(Self::Among(fits)),
        }
    }

    // The places a claim answers, for the tree to judge which the member
    // takes. A set that was refused takes a member it cannot order at
    // `past`, past every volume it holds, so it goes where the set went.
    pub(crate) fn claimed(self, gone: bool, past: Option<VolumeIndex>) -> Vec<VolumeIndex> {
        match self {
            Self::At(volume) => vec![volume],
            Self::Among(_) if gone => past.into_iter().collect(),
            Self::Among(places) => places,
        }
    }

    // The index past every volume `last` and those before it hold.
    pub(crate) fn past(last: Option<u32>) -> Option<VolumeIndex> {
        last.map_or(Some(0), |last| last.checked_add(1))
            .map(VolumeIndex)
    }
}

// A member beside the set that the set needs and nothing orders: more than
// one place it fits is free. A place held is not its own: the tree weighed
// it against the holder and left it beside. The set cannot say where it
// goes, so it reads none of its bytes.
pub(crate) fn unordered(
    standing: &dyn Standing,
    placed: impl Fn(&Beside<'_>) -> Option<Placed>,
    held: impl Fn(VolumeIndex) -> bool,
) -> Option<Why> {
    standing
        .beside()
        .iter()
        .any(|beside| match placed(beside) {
            Some(Placed::Among(places)) => {
                places.into_iter().filter(|place| !held(*place)).count() > 1
            }
            _ => false,
        })
        .then_some(Why::OrderUnknown)
}
