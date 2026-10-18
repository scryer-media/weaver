// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a spanned zip names its volumes: `name.z01`, `name.z02`, … counted
// from one, and then `name.zip`, which is the last volume and carries no
// number. A lone `name.zip` is a set of one. Case never matters.
//
// The last volume's number is how many came before it, which its name does
// not say, and which volumes posted out of order cannot say either. So it is
// the symbolic last volume, after every numbered one whatever their count,
// and the store learns its disk from the end record it carries.

use std::collections::BTreeMap;

use crate::engine::SetName;

// The volume `name.zip` binds as in a spanned set: after every other.
pub(crate) const LAST: u32 = u32::MAX;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    fn index(&self) -> u32 {
        self.number.unwrap_or(LAST)
    }

    fn same_set(&self, other: &VolumeName) -> bool {
        self.stem == other.stem
    }

    // A `name.zip` bound first, as a volume of its own, is a set of one:
    // nothing joins it.
    fn slot(&self, bound: &BTreeMap<u32, VolumeName>) -> Option<u32> {
        let alone = bound
            .iter()
            .any(|(index, name)| name.is_last() && *index != LAST);
        let index = self.index();
        (!alone && !bound.contains_key(&index)).then_some(index)
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
