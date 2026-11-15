// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a RAR set numbers its volumes in their names: `name.part1.rar`,
// `name.part2.rar`, … counted from one, or the old `name.rar`, `name.r00`,
// … `name.r99`, then `name.s00` … `name.s99`, `name.t00` and on, a hundred
// to a letter. Case never matters.

use std::ops::RangeInclusive;

use crate::engine::SetName;

// The letters the old numbering runs through. It stops before `z`, whose
// `.zNN` names are a split zip's.
const OLD_LETTERS: RangeInclusive<u8> = b'r'..=b'y';

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
        let ext = &lower.as_bytes()[lower.len() - 4..];
        if ext[0] == b'.'
            && OLD_LETTERS.contains(&ext[1])
            && let Ok(n) = lower[lower.len() - 2..].parse::<u32>()
        {
            return Some(u32::from(ext[1] - OLD_LETTERS.start()) * 100 + n + 1);
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
        // `parse` reads two digits after the letter, a hundred names to each
        // letter it numbers.
        Scheme::Old => {
            let letter = u32::from(*OLD_LETTERS.start()) + (index - 1) / 100;
            let letter = u8::try_from(letter)
                .ok()
                .filter(|letter| OLD_LETTERS.contains(letter))?;
            Some(format!("{base}.{}{:02}", letter as char, (index - 1) % 100))
        }
    }
}

// The set a recovery volume's name covers, as far as the name alone says:
// `name.part1.rev` covers the volumes of `name`, and an old-numbered set's
// `name1.rev` does too, its number written straight after the set's name.
// RAR 3.0 named them `name5_3_1.rev` (see `old_style`). Where the set's own
// name ends in digits the name alone cannot tell, so `covers` settles it
// against the data volumes' names.
pub(crate) fn recovery_stem(name: &str) -> Option<String> {
    let lower = name.to_ascii_lowercase();
    let stem = lower.strip_suffix(".rev")?;
    let stem = match (old_style(stem), stem.rfind(".part")) {
        (Some(set), _) => set.strip_suffix(".part").unwrap_or(set),
        (None, Some(at)) if stem[at + 5..].parse::<u32>().is_ok() => &stem[..at],
        _ => stem.trim_end_matches(|ch: char| ch.is_ascii_digit()),
    };
    (!stem.is_empty()).then(|| stem.to_owned())
}

// Whether the recovery volume `name` is one of the set called `stem`: the
// set's name, then `.part` and a number, or the number alone, or RAR 3.0's
// three numbers, straight after the name or after an underscore.
pub(crate) fn recovery_covers(name: &str, stem: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    let Some(rest) = lower
        .strip_suffix(".rev")
        .and_then(|whole| whole.strip_prefix(stem))
    else {
        return false;
    };
    let number = rest.strip_prefix(".part").unwrap_or(rest);
    let numbered = !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit());
    numbered || old_style(number).is_some_and(|before| before.is_empty() || before == "_")
}

// A RAR 3.0 recovery volume's name ends, before `.rev`, in three numbers:
// `<data>_<recovery>_<position>`, the set's data volumes, its recovery
// volumes, and this one's place among them. Each is 1 to 255, with
// `data + recovery` and `data + position - 1` at most 255, as RAR reads
// them. The data count is the digits straight before the second-to-last
// underscore. What comes before those digits is returned: the set's name,
// with any underscore it ends in. `None` when the name is not of that form.
fn old_style(stem: &str) -> Option<&str> {
    let mut parts = stem.rsplitn(3, '_');
    let position = rev_count(parts.next()?)?;
    let recovery = rev_count(parts.next()?)?;
    let head = parts.next()?;
    let at = head.trim_end_matches(|ch: char| ch.is_ascii_digit()).len();
    let data = rev_count(&head[at..])?;
    (data + recovery <= 255 && data + position - 1 <= 255).then(|| &head[..at])
}

fn rev_count(digits: &str) -> Option<u32> {
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits
        .parse::<u32>()
        .ok()
        .filter(|count| (1..=255).contains(count))
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
        for name in ["a.part3.rar", "a.r05", "a.rar", "a.r99", "a.s00", "a.y99"] {
            let index = parse(name).unwrap().index();
            assert_eq!(sibling(name, index).as_deref(), Some(name));
        }
    }

    #[test]
    fn the_old_numbering_runs_a_hundred_to_a_letter_from_r() {
        for (name, index) in [
            ("a.rar", 0),
            ("a.r00", 1),
            ("a.r99", 100),
            ("a.s00", 101),
            ("A.S01", 102),
            ("a.s99", 200),
            ("a.t00", 201),
            ("a.y99", 800),
        ] {
            assert_eq!(parse(name).unwrap().index(), index, "{name}");
            assert_eq!(
                sibling("a.rar", index).as_deref(),
                Some(name.to_ascii_lowercase().as_str())
            );
        }
        assert_eq!(sibling("a.rar", 801), None);
        assert_eq!(parse("a.z01"), None);
        assert_eq!(parse("a.q00"), None);
        assert!(parse("a.s00").unwrap().same_set(&parse("a.r00").unwrap()));
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

    // RAR 3.0 wrote `<set><data>_<recovery>_<position>.rev`.
    #[test]
    fn a_rar_3_0_recovery_volume_names_its_set_by_three_numbers() {
        assert_eq!(recovery_stem("Arc5_3_1.rev").as_deref(), Some("arc"));
        assert_eq!(recovery_stem("arc5_3_3.rev").as_deref(), Some("arc"));
        assert_eq!(recovery_stem("arc_5_3_1.rev").as_deref(), Some("arc_"));
        assert_eq!(recovery_stem("arc.part5_3_1.rev").as_deref(), Some("arc"));
        assert!(recovery_covers("Arc5_3_1.rev", "arc"));
        assert!(recovery_covers("arc_5_3_1.rev", "arc"));
        assert!(recovery_covers("arc_5_3_1.rev", "arc_"));
        assert!(recovery_covers("arc.part5_3_1.rev", "arc"));
        assert!(!recovery_covers("arc5_3_1.rev", "ar"));
        assert!(!recovery_covers("arc__5_3_1.rev", "arc"));
        assert!(!recovery_covers("arcx5_3_1.rev", "arc"));
        assert!(!recovery_covers("arc5_3.rev", "arc"));
        assert!(!recovery_covers("arc5_3_1_.rev", "arc"));
    }

    // Each number is 1 to 255, `data + recovery` and `data + position - 1`
    // at most 255.
    #[test]
    fn a_rar_3_0_recovery_name_holds_to_the_limits_rar_reads() {
        for good in [
            "a1_1_1.rev",
            "a250_5_6.rev",
            "a1_254_255.rev",
            "a005_3_1.rev",
        ] {
            assert!(recovery_covers(good, "a"), "{good}");
        }
        for bad in [
            "a0_3_1.rev",
            "a5_0_1.rev",
            "a5_3_0.rev",
            "a256_1_1.rev",
            "a200_60_1.rev",
            "a250_5_7.rev",
            "a5_3_99999999999.rev",
        ] {
            assert!(!recovery_covers(bad, "a"), "{bad}");
        }
        // Out of range, the name is no RAR 3.0 one; it is read as the newer
        // scheme, whose number is the digits at its end.
        assert_eq!(recovery_stem("a0_3_1.rev").as_deref(), Some("a0_3_"));
    }

    // A set whose own name ends in digits or underscores keeps them: only
    // the three numbers are the recovery volume's. Where the digits before
    // the first number could be the set's, the data volumes' names settle
    // it, as they do for the newer scheme.
    #[test]
    fn a_rar_3_0_recovery_name_takes_nothing_of_the_set_name() {
        assert!(recovery_covers("show25_3_1.rev", "show2"));
        assert!(recovery_covers("show25_3_1.rev", "show"));
        assert!(!recovery_covers("show25_3_1.rev", "show25"));
        assert!(recovery_covers("show_2019_5_3_1.rev", "show_2019"));
        assert!(recovery_covers("show_2019_5_3_1.rev", "show_2019_"));
        assert!(!recovery_covers("show_2019_5_3_1.rev", "show_"));
        assert_eq!(
            recovery_stem("show_2019_5_3_1.rev").as_deref(),
            Some("show_2019_")
        );
        assert!(recovery_covers("set__5_3_1.rev", "set_"));
        assert!(!recovery_covers("set__5_3_1.rev", "set"));
        // The newer scheme of a set named like the three numbers' tail is
        // still the newer scheme.
        assert!(recovery_covers("show_1_20191.rev", "show_1_2019"));
        assert_eq!(
            recovery_stem("show_2019_1.rev").as_deref(),
            Some("show_2019_")
        );
    }
}
