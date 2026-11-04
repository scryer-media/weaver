// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-04.

use std::ops::Deref;

use crate::constants::SEQUENCE_HALF_SPACE;

#[derive(PartialEq, Eq, Clone, Copy, Default, Hash)]
pub struct SeqNr(pub u16);

impl SeqNr {
    pub fn forward_distance_from(self, base: Self) -> u16 {
        self.0.wrapping_sub(base.0)
    }

    // Exactly half the sequence space has no unambiguous direction.
    pub fn signed_offset_from(self, base: Self) -> Option<isize> {
        let distance = self.forward_distance_from(base);
        (distance != SEQUENCE_HALF_SPACE).then_some((distance as i16) as isize)
    }

    pub fn is_after(self, other: Self) -> bool {
        let distance = self.forward_distance_from(other);
        distance != 0 && distance < SEQUENCE_HALF_SPACE
    }

    pub fn is_before(self, other: Self) -> bool {
        other.is_after(self)
    }

    pub fn is_at_or_after(self, other: Self) -> bool {
        self == other || self.is_after(other)
    }

    pub fn is_at_or_before(self, other: Self) -> bool {
        self == other || self.is_before(other)
    }
}

impl Deref for SeqNr {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u16> for SeqNr {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for SeqNr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for SeqNr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Add<u16> for SeqNr {
    type Output = SeqNr;

    fn add(self, rhs: u16) -> Self::Output {
        Self(self.0.wrapping_add(rhs))
    }
}

impl std::ops::Sub<u16> for SeqNr {
    type Output = SeqNr;

    fn sub(self, rhs: u16) -> Self::Output {
        Self(self.0.wrapping_sub(rhs))
    }
}

impl std::ops::AddAssign<u16> for SeqNr {
    fn add_assign(&mut self, rhs: u16) {
        *self = *self + rhs;
    }
}

impl std::ops::SubAssign<u16> for SeqNr {
    fn sub_assign(&mut self, rhs: u16) {
        *self = *self - rhs;
    }
}

#[cfg(test)]
mod tests {
    use super::SeqNr;

    #[test]
    fn offsets_follow_the_shorter_path_across_wrap() {
        for (new, old, offset) in [
            (2, 1, 1),
            (1, 1, 0),
            (0, 1, -1),
            (0, 65535, 1),
            (1063, 65500, 1099),
            (65500, 1063, -1099),
            (32767, 0, 32767),
            (0, 32767, -32767),
        ] {
            let (new, old) = (SeqNr(new), SeqNr(old));
            assert_eq!(new.signed_offset_from(old), Some(offset));
            assert_eq!(new.is_after(old), offset > 0);
            assert_eq!(new.is_before(old), offset < 0);
            assert_eq!(new.is_at_or_after(old), offset >= 0);
            assert_eq!(new.is_at_or_before(old), offset <= 0);
        }
    }

    #[test]
    fn half_a_sequence_space_is_unordered_in_both_directions() {
        for (left, right) in [(0, 32768), (32768, 0), (65535, 32767), (32767, 65535)] {
            let (left, right) = (SeqNr(left), SeqNr(right));
            assert_eq!(left.signed_offset_from(right), None);
            assert!(!left.is_before(right));
            assert!(!left.is_after(right));
            assert!(!left.is_at_or_before(right));
            assert!(!left.is_at_or_after(right));
        }
    }
}
