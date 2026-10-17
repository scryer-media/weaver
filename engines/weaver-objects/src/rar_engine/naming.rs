// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a RAR set numbers its volumes in their names. Two schemes exist:
// `name.part1.rar`, `name.part2.rar`, … counted from one, and the old
// `name.rar`, `name.r00`, `name.r01`, … where `.r99` runs on to `.s00`.
// Case never matters; RAR writers on every host have used both.

use crate::engine::SetName;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scheme {
    Part,
    Old,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeName {
    stem: String,
    scheme: Scheme,
    // The volume this name is, counted from zero.
    index: u32,
}

impl SetName for VolumeName {
    fn index(&self) -> u32 {
        self.index
    }

    fn same_set(&self, other: &VolumeName) -> bool {
        self.stem == other.stem && self.scheme == other.scheme
    }
}

pub(super) fn parse(name: &str) -> Option<VolumeName> {
    let lower = name.to_ascii_lowercase();
    if let Some(base) = lower.strip_suffix(".rar") {
        if let Some((stem, digits)) = base.rsplit_once(".part")
            && let Some(number) = number(digits)
        {
            return Some(VolumeName {
                stem: stem.to_owned(),
                scheme: Scheme::Part,
                index: number.checked_sub(1)?,
            });
        }
        return Some(VolumeName {
            stem: base.to_owned(),
            scheme: Scheme::Old,
            index: 0,
        });
    }
    let (stem, extension) = lower.rsplit_once('.')?;
    let (letter, digits) = extension.split_at_checked(1)?;
    let letter = letter.as_bytes()[0];
    if !(b'r'..=b'z').contains(&letter) || digits.len() != 2 {
        return None;
    }
    let index = u32::from(letter - b'r') * 100 + number(digits)? + 1;
    Some(VolumeName {
        stem: stem.to_owned(),
        scheme: Scheme::Old,
        index,
    })
}

fn number(digits: &str) -> Option<u32> {
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}
