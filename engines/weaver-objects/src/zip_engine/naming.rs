// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a spanned zip names its volumes: `name.z01`, `name.z02`, … counted
// from one, and then `name.zip`, which is the last volume and carries no
// number. A lone `name.zip` is a set of one. Case never matters.
//
// The last volume's number is how many came before it, which its name does
// not say. It is given the volume after every numbered one bound so far, and
// a numbered name that would land at or after it is not the set's: the root
// samples its members in name order, and `.z01` … `.z99` sort before `.zip`.

use std::collections::BTreeMap;

use crate::engine::SetName;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeName {
    stem: String,
    // The volume this name is, counted from zero; `None` for the last.
    number: Option<u32>,
}

impl VolumeName {
    pub(crate) fn is_last(&self) -> bool {
        self.number.is_none()
    }
}

impl SetName for VolumeName {
    // The last volume's index is its slot; on its own it has none, and the
    // engine object asks for the slot.
    fn index(&self) -> u32 {
        self.number.unwrap_or(u32::MAX)
    }

    fn same_set(&self, other: &VolumeName) -> bool {
        self.stem == other.stem
    }

    fn slot(&self, bound: &BTreeMap<u32, VolumeName>) -> Option<u32> {
        let last = bound
            .iter()
            .find(|(_, name)| name.is_last())
            .map(|(index, _)| *index);
        match self.number {
            None => last
                .is_none()
                .then(|| bound.keys().next_back().map_or(0, |index| index + 1)),
            Some(index) => (!bound.contains_key(&index) && last.is_none_or(|last| index < last))
                .then_some(index),
        }
    }
}

pub(crate) fn parse(name: &str) -> Option<VolumeName> {
    let lower = name.to_ascii_lowercase();
    let (stem, extension) = lower.rsplit_once('.')?;
    if stem.is_empty() {
        return None;
    }
    let number = match extension.strip_prefix('z')? {
        "ip" => None,
        digits if digits.len() >= 2 && digits.bytes().all(|byte| byte.is_ascii_digit()) => {
            Some(digits.parse::<u32>().ok()?.checked_sub(1)?)
        }
        _ => return None,
    };
    Some(VolumeName {
        stem: stem.to_owned(),
        number,
    })
}
