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

impl VolumeName {
    // The set's name, lower-cased, without its numbering.
    pub(crate) fn stem(&self) -> &str {
        &self.stem
    }
}

// The name volume `index` has in the set `reference` is a volume of, in the
// reference's own numbering, width and case.
pub(crate) fn sibling(reference: &str, index: u32) -> Option<String> {
    let parsed = parse(reference)?;
    let base = reference.get(..reference.len() - 4)?;
    match parsed.scheme {
        Scheme::Part => {
            let digits = base.len() - base.rfind(|ch: char| !ch.is_ascii_digit())? - 1;
            let width = digits;
            Some(format!(
                "{}{:0width$}{}",
                &base[..base.len() - digits],
                index + 1,
                &reference[base.len()..]
            ))
        }
        Scheme::Old if index == 0 => Some(format!("{base}.rar")),
        // `parse` reads two digits after the letter, so `.r99` is the last
        // old name it numbers.
        Scheme::Old => (index <= 100).then(|| format!("{base}.r{:02}", index - 1)),
    }
}

// The set a recovery volume's name covers, as far as the name alone says:
// `name.part1.rev` covers the volumes of `name`, and an old-numbered set's
// `name1.rev` does too, its number written straight after the set's name.
// Where the set's own name ends in digits the name alone cannot tell, so
// `covers` settles it against the data volumes' names.
pub(crate) fn recovery_stem(name: &str) -> Option<String> {
    let lower = name.to_ascii_lowercase();
    let stem = lower.strip_suffix(".rev")?;
    let stem = match stem.rfind(".part") {
        Some(at) if stem[at + 5..].parse::<u32>().is_ok() => &stem[..at],
        _ => stem.trim_end_matches(|ch: char| ch.is_ascii_digit()),
    };
    (!stem.is_empty()).then(|| stem.to_owned())
}

// Whether the recovery volume `name` is one of the set called `stem`: the
// set's name, then `.part` and a number, or the number alone.
pub(crate) fn recovery_covers(name: &str, stem: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    let Some(rest) = lower
        .strip_suffix(".rev")
        .and_then(|whole| whole.strip_prefix(stem))
    else {
        return false;
    };
    let number = rest.strip_prefix(".part").unwrap_or(rest);
    !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sibling_keeps_the_numbering_width_and_case() {
        assert_eq!(
            sibling("Archive.part1.rar", 1).as_deref(),
            Some("Archive.part2.rar")
        );
        assert_eq!(sibling("a.part01.RAR", 9).as_deref(), Some("a.part10.RAR"));
        assert_eq!(sibling("a.r00", 0).as_deref(), Some("a.rar"));
        assert_eq!(sibling("a.rar", 1).as_deref(), Some("a.r00"));
        assert_eq!(sibling("a.rar", 101), None);
        for name in ["a.part3.rar", "a.r05", "a.rar"] {
            let index = parse(name).unwrap().index();
            assert_eq!(sibling(name, index).as_deref(), Some(name));
        }
    }

    #[test]
    fn a_recovery_volume_names_its_set() {
        assert_eq!(
            recovery_stem("Archive.part1.rev").as_deref(),
            Some("archive")
        );
        assert_eq!(recovery_stem("archive1.rev").as_deref(), Some("archive"));
        assert_eq!(recovery_stem("archive.rev").as_deref(), Some("archive"));
        assert_eq!(recovery_stem("archive.rar"), None);
        assert!(recovery_covers("Archive.part1.rev", "archive"));
        assert!(recovery_covers("archive1.rev", "archive"));
        assert!(recovery_covers("set21.rev", "set2"));
        assert!(recovery_covers("set21.rev", "set"));
        assert!(!recovery_covers("archive.rev", "archive"));
        assert!(!recovery_covers("archive1.rev", "archive1"));
        assert!(!recovery_covers("other1.rev", "archive"));
        assert_eq!(
            parse("archive.part2.rar").unwrap().stem(),
            parse("archive.r00").unwrap().stem()
        );
    }
}
