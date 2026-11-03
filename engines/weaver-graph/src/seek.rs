// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Asking up. An object asks its parent for bytes it needs out of order. The
// parent translates the request into its own volumes and asks its parent,
// until the root answers. A parent that cannot reorder answers for itself.

use crate::volume::{ByteRange, VolumeIndex};

// Why the bytes are wanted. Recovery seeks are promoted at the root and, when
// they cannot be lifted, waited for rather than given up on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Purpose {
    Data,
    Recovery,
}

// How badly the asker wants it, relative to the other bids into the root's
// window. Lower is sooner. A RAR object extracting a member bids its next
// volume at 1, the one after at 2, and so on.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Weight(pub u32);

impl Weight {
    pub const FIRST: Weight = Weight(0);
}

// Whether the asker wants the bytes now or says they are not wanted yet. A
// deferred seek travels the same walk as any other and is translated the
// same way; the root parks the units that carry it, so nothing fetches them
// until a later seek of the same walk wants them now. It bids for nothing
// and its answer is only how many units it parked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum When {
    Now,
    Deferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Seek {
    pub volume: VolumeIndex,
    pub range: ByteRange,
    pub purpose: Purpose,
    pub weight: Weight,
    pub when: When,
}

impl Seek {
    pub fn data(volume: VolumeIndex, range: ByteRange, weight: Weight) -> Self {
        Self {
            volume,
            range,
            purpose: Purpose::Data,
            weight,
            when: When::Now,
        }
    }

    pub fn recovery(volume: VolumeIndex, range: ByteRange, weight: Weight) -> Self {
        Self {
            volume,
            range,
            purpose: Purpose::Recovery,
            weight,
            when: When::Now,
        }
    }

    // Recovery bytes the asker knows it holds and does not want yet.
    pub fn deferred(volume: VolumeIndex, range: ByteRange) -> Self {
        Self {
            volume,
            range,
            purpose: Purpose::Recovery,
            weight: Weight::FIRST,
            when: When::Deferred,
        }
    }

    // What an ask that cannot be reordered answers: an asker that wants the
    // bytes now waits for the normal order; one that deferred them parked
    // nothing.
    pub fn unseekable(self) -> Pull {
        match self.when {
            When::Now => Pull::Unseekable,
            When::Deferred => Pull::Moved(0),
        }
    }

    // What an ask answers whose bytes a store above has not located yet: an
    // asker that wants them now waits and asks again; one that deferred
    // them parked nothing.
    pub fn unlocated(self) -> Pull {
        match self.when {
            When::Now => Pull::Unlocated,
            When::Deferred => Pull::Moved(0),
        }
    }
}

// The root's answer, carried back down unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pull {
    // The units that carry the range are now ahead of the normal order; the
    // count is how many were moved. Zero means they were already ahead.
    Moved(u64),
    // The range is gone from every source. The bytes will never arrive.
    Lost,
    // Nothing above can reorder. The bytes arrive when the normal order
    // reaches them. For data the asker gives up streaming and asks for an
    // engine; for recovery it waits.
    Unseekable,
    // A store above has not read the header that places the range yet. The
    // asker waits: its standing want is lifted again every round, and is
    // answered once the store has read that header.
    Unlocated,
}

impl Pull {
    pub fn fold(self, other: Pull) -> Pull {
        // A seek that spans several parent ranges is as good as its worst
        // part: one lost range loses the seek, one unseekable range leaves
        // the asker waiting for the normal order, and one not located yet
        // leaves it asking again.
        match (self, other) {
            (Pull::Lost, _) | (_, Pull::Lost) => Pull::Lost,
            (Pull::Unseekable, _) | (_, Pull::Unseekable) => Pull::Unseekable,
            (Pull::Unlocated, _) | (_, Pull::Unlocated) => Pull::Unlocated,
            (Pull::Moved(left), Pull::Moved(right)) => Pull::Moved(left + right),
        }
    }
}

// The parent's face toward a child. The namespace implements it for every
// bound object by translating the child's volume into the member it is, and
// the member's range into the parent object's own volumes.
pub trait SeekUp {
    fn seek(&mut self, seek: Seek) -> Pull;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seek_over_several_ranges_is_as_good_as_its_worst_part() {
        assert_eq!(Pull::Moved(2).fold(Pull::Moved(3)), Pull::Moved(5));
        assert_eq!(Pull::Moved(2).fold(Pull::Unseekable), Pull::Unseekable);
        assert_eq!(Pull::Unseekable.fold(Pull::Lost), Pull::Lost);
        assert_eq!(Pull::Moved(2).fold(Pull::Unlocated), Pull::Unlocated);
        assert_eq!(Pull::Unlocated.fold(Pull::Unseekable), Pull::Unseekable);
    }
}
