// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a 7z set numbers its volumes in their names: `name.7z.001`,
// `name.7z.002`, … counted from one, three digits until a set outgrows
// them. A lone `name.7z` is a set of one and names no siblings. Case never
// matters.

use crate::engine::SetName;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VolumeName {
    stem: String,
    // The volume this name is, counted from zero.
    index: u32,
}

impl SetName for VolumeName {
    fn index(&self) -> u32 {
        self.index
    }

    fn same_set(&self, other: &VolumeName) -> bool {
        self.stem == other.stem
    }
}

pub(super) fn parse(name: &str) -> Option<VolumeName> {
    let lower = name.to_ascii_lowercase();
    let (base, digits) = lower.rsplit_once('.')?;
    let stem = base.strip_suffix(".7z")?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let number: u32 = digits.parse().ok()?;
    Some(VolumeName {
        stem: stem.to_owned(),
        index: number.checked_sub(1)?,
    })
}
