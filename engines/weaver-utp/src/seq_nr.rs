// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.

use std::{cmp::Ordering, ops::Deref};

use crate::{constants::WRAP_TOLERANCE, utils::seq_nr_offset};

#[derive(PartialEq, Eq, Clone, Copy, Default, Hash)]
pub struct SeqNr(pub u16);

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

impl std::ops::Sub<SeqNr> for SeqNr {
    type Output = isize;

    fn sub(self, rhs: SeqNr) -> Self::Output {
        seq_nr_offset(self.0, rhs.0, WRAP_TOLERANCE)
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

impl std::cmp::PartialOrd for SeqNr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for SeqNr {
    fn cmp(&self, other: &Self) -> Ordering {
        let offset = *self - *other;
        offset.cmp(&0)
    }
}
