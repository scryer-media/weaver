// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a RAR set numbers its volumes in their names: `name.part1.rar`,
// `name.part2.rar`, … counted from one, or the old `name.rar`, `name.r00`,
// `name.r01`, … with `.sNN` numbered the same way. Case never matters.

use crate::engine::SetName;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum Scheme {
    Part,
    Old,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

pub(crate) fn parse(name: &str) -> Option<VolumeName> {
    let lower = name.to_ascii_lowercase();
    // The last four bytes are read as an extension, which must not split a
    // character.
    if lower.len() >= 4 && !lower.is_char_boundary(lower.len() - 4) {
        return None;
    }
    let index = parse_rar_volume(&lower)?;
    let (stem, scheme) = match lower.strip_suffix(".rar") {
        Some(stem) => match stem.rfind(".part") {
            Some(part_pos) if stem[part_pos + 5..].parse::<u32>().is_ok() => {
                (&stem[..part_pos], Scheme::Part)
            }
            _ => (stem, Scheme::Old),
        },
        None => (&lower[..lower.len() - 4], Scheme::Old),
    };
    Some(VolumeName {
        stem: stem.to_owned(),
        scheme,
        index,
    })
}

fn parse_rar_volume(lower: &str) -> Option<u32> {
    if let Some(stem) = lower.strip_suffix(".rar") {
        if let Some(part_pos) = stem.rfind(".part") {
            let num_str = &stem[part_pos + 5..];
            if let Ok(n) = num_str.parse::<u32>() {
                return Some(n.saturating_sub(1));
            }
        }
        return Some(0);
    }
    if lower.len() >= 4 {
        let ext = &lower[lower.len() - 4..];
        if (ext.starts_with(".r") || ext.starts_with(".s"))
            && let Ok(n) = ext[2..].parse::<u32>()
        {
            return Some(n + 1);
        }
    }
    None
}
