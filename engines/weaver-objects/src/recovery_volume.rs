// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A PAR volume as a recovery object fetches it: what its name says it
// carries, and the ranges of it still to come; and what the object has been
// shown of its namespace's damage.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::{ByteRange, Coverage, MemberId, Namespace, ObjectId};

// The recovery blocks a volume's name says it carries: `<stem>.vol` and the
// format's `extension` around `<start>+<count>`, so `name.vol07+08.par2`
// carries eight. A PAR2 volume may instead be named `<start>-<end>`, its
// first block and the one past its last, so `name.vol07-15.par2` carries
// eight too; one whose end is not past its start names none.
pub(crate) fn named_blocks(name: &str, extension: &str) -> Option<u64> {
    let lower = name.to_ascii_lowercase();
    let (_, tail) = lower.strip_suffix(extension)?.rsplit_once(".vol")?;
    suffix_blocks(tail, extension)
}

// The blocks what follows `.vol` names, or `None` when it names none.
fn suffix_blocks(tail: &str, extension: &str) -> Option<u64> {
    if let Some((start, count)) = tail.split_once('+') {
        start.parse::<u64>().ok()?;
        return count.parse().ok();
    }
    if extension != ".par2" {
        return None;
    }
    let (start, end) = tail.split_once('-')?;
    let number = |text: &str| {
        (!text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| text.parse::<u64>().ok())
            .flatten()
    };
    number(end)?
        .checked_sub(number(start)?)
        .filter(|count| *count > 0)
}

// The stem a PAR file's name gives its set: the name less the format's
// `extension` and any `.vol` suffix that names blocks.
fn stem<'a>(lower: &'a str, extension: &str) -> Option<&'a str> {
    let bare = lower.strip_suffix(extension)?;
    Some(match bare.rsplit_once(".vol") {
        Some((stem, tail)) if suffix_blocks(tail, extension).is_some() => stem,
        _ => bare,
    })
}

// Whether `name` is a recovery volume of the set whose file `held` is: a
// `.vol` name of the same stem.
pub(crate) fn same_set(held: &str, name: &str, extension: &str) -> bool {
    let (held, name) = (held.to_ascii_lowercase(), name.to_ascii_lowercase());
    named_blocks(&name, extension).is_some()
        && stem(&held, extension).is_some_and(|held| stem(&name, extension) == Some(held))
}

// Each range of a volume not yet in hand, the last open-ended: the volume's
// length is the parent's to know, and the root clamps an open end to it.
pub(crate) fn gaps(arrived: &Coverage, mut each: impl FnMut(ByteRange)) {
    let mut cursor = 0;
    for run in arrived.runs() {
        if run.range.start > cursor {
            each(ByteRange::new(cursor, run.range.start));
        }
        cursor = run.range.end;
    }
    each(ByteRange::new(cursor, u64::MAX));
}

// What a recovery object has been shown of its namespace's damage: the
// ranges the parent refused, kept for every member bound or not so a set
// bound later counts them; and, as the last bind saw the namespace, the
// members that are the object's own volumes and every other member holding
// damage.
#[derive(Debug, Default)]
pub(crate) struct Shown {
    pub(crate) refusals: BTreeMap<MemberId, Vec<ByteRange>>,
    own: BTreeSet<MemberId>,
    damaged: BTreeSet<MemberId>,
}

// What a refusal adds to what the object was shown.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    // Kept already.
    Known,
    // New, on one of the object's own volumes.
    Own,
    // New, on a member the object's sets may cover.
    Damage,
}

impl Shown {
    // The namespace as object `id`'s bind sees it: whether it shows damage
    // it did not before.
    pub(crate) fn bind(&mut self, namespace: &Namespace, id: ObjectId) -> bool {
        let (own, others): (Vec<_>, Vec<_>) = namespace
            .members()
            .partition(|member| member.bound_to == Some(id));
        self.own = own.iter().map(|member| member.id).collect();
        let damaged: BTreeSet<MemberId> = others
            .iter()
            .filter(|member| !member.coverage.is_clean())
            .map(|member| member.id)
            .collect();
        let newly = !damaged.is_subset(&self.damaged);
        self.damaged = damaged;
        newly
    }

    pub(crate) fn refused(&mut self, member: MemberId, range: ByteRange) -> Refusal {
        let ranges = self.refusals.entry(member).or_default();
        if ranges.contains(&range) {
            return Refusal::Known;
        }
        ranges.push(range);
        match self.own.contains(&member) {
            true => Refusal::Own,
            false => Refusal::Damage,
        }
    }

    // Whether the namespace shows damage `covers` leaves out: a member,
    // none of the object's own volumes, held damaged or refused.
    pub(crate) fn undescribed(&self, covers: impl Fn(MemberId) -> bool) -> bool {
        let refused = self
            .refusals
            .iter()
            .filter(|(member, ranges)| !ranges.is_empty() && !self.own.contains(member))
            .map(|(member, _)| member);
        self.damaged
            .iter()
            .chain(refused)
            .any(|member| !covers(*member))
    }
}

#[cfg(test)]
mod tests {
    use super::{named_blocks, same_set, stem};

    #[test]
    fn a_volume_name_says_how_many_blocks_it_carries() {
        assert_eq!(named_blocks("set.vol07+08.par2", ".par2"), Some(8));
        assert_eq!(named_blocks("SET.VOL000+128.PAR2", ".par2"), Some(128));
        assert_eq!(named_blocks("set.vol0+4.par3", ".par3"), Some(4));
        assert_eq!(named_blocks("set.vol07+08.par2", ".par3"), None);
        assert_eq!(named_blocks("set.par2", ".par2"), None);
        assert_eq!(named_blocks("set.vol1.par2", ".par2"), None);
        assert_eq!(named_blocks("set.volx+4.par2", ".par2"), None);
    }

    // A volume named by its first block and the one past its last carries
    // the blocks between, at any width of number.
    #[test]
    fn a_volume_named_by_its_block_range_carries_the_blocks_between() {
        let main = [
            ("000", "001", 1),
            ("001", "003", 2),
            ("003", "007", 4),
            ("007", "015", 8),
            ("015", "031", 16),
            ("031", "063", 32),
            ("063", "105", 42),
        ];
        for (start, end, blocks) in main {
            let name = format!("fixture.release.vol{start}-{end}.par2");
            assert_eq!(named_blocks(&name, ".par2"), Some(blocks), "{name}");
        }
        let sample = [
            ("00", "01", 1),
            ("01", "03", 2),
            ("03", "07", 4),
            ("07", "15", 8),
            ("15", "31", 16),
            ("31", "58", 27),
        ];
        for (start, end, blocks) in sample {
            let name = format!("fixture.release-sample.vol{start}-{end}.par2");
            assert_eq!(named_blocks(&name, ".par2"), Some(blocks), "{name}");
        }
        assert_eq!(named_blocks("SET.VOL7-15.PAR2", ".par2"), Some(8));
        assert_eq!(named_blocks("set.vol0007-00015.par2", ".par2"), Some(8));
        assert_eq!(
            named_blocks("set.vol1-18446744073709551615.par2", ".par2"),
            Some(u64::MAX - 1)
        );
    }

    // A name that is no volume of either form names no blocks, and neither
    // panics nor wraps.
    #[test]
    fn a_malformed_range_names_no_blocks() {
        for name in [
            "set.vol15-07.par2",
            "set.vol07-07.par2",
            "set.vol-07.par2",
            "set.vol07-.par2",
            "set.vol-.par2",
            "set.vol07-+15.par2",
            "set.vol+07-15.par2",
            "set.vol07-15-31.par2",
            "set.volx-15.par2",
            "set.vol07-1x.par2",
            "set.vol 7-15.par2",
            "set.vol0-99999999999999999999999.par2",
            "set.vol99999999999999999999999-1.par2",
            "set-vol07-15.par2",
            "set.vol07-15.par",
        ] {
            assert_eq!(named_blocks(name, ".par2"), None, "{name}");
        }
        // The range form is a PAR2 name: a PAR3 volume names its count.
        assert_eq!(named_blocks("set.vol07-15.par3", ".par3"), None);
    }

    // A stem holding dashes and dots keeps them; only the `.vol` suffix that
    // names blocks is taken off.
    #[test]
    fn a_stem_keeps_its_own_dashes_and_dots() {
        let stem = |name: &str| stem(name, ".par2").map(str::to_owned);
        let owned = |text: &str| Some(text.to_owned());
        assert_eq!(
            stem("fixture.release-sample.vol00-01.par2"),
            owned("fixture.release-sample")
        );
        assert_eq!(
            stem("fixture.release.vol063-105.par2"),
            owned("fixture.release")
        );
        assert_eq!(
            stem("fixture.release-sample.par2"),
            owned("fixture.release-sample")
        );
        assert_eq!(
            stem("fixture.re-lease.v2.vol3+4.par2"),
            owned("fixture.re-lease.v2")
        );
        assert_eq!(
            stem("fixture.volume-sample.par2"),
            owned("fixture.volume-sample")
        );
        assert_eq!(
            stem("fixture.release.vol15-07.par2"),
            owned("fixture.release.vol15-07")
        );
    }

    // Two sets whose stems differ only by a dashed suffix each claim their
    // own volumes, of either form, and not the other's.
    #[test]
    fn prefix_related_stems_claim_only_their_own_volumes() {
        let main = "fixture.release.par2";
        let sample = "fixture.release-sample.par2";
        for name in [
            "fixture.release.vol000-001.par2",
            "fixture.release.vol063-105.par2",
            "FIXTURE.RELEASE.VOL007+008.PAR2",
        ] {
            assert!(same_set(main, name, ".par2"), "{name}");
            assert!(!same_set(sample, name, ".par2"), "{name}");
        }
        for name in [
            "fixture.release-sample.vol00-01.par2",
            "fixture.release-sample.vol31-58.par2",
            "fixture.release-sample.vol07+08.par2",
        ] {
            assert!(same_set(sample, name, ".par2"), "{name}");
            assert!(!same_set(main, name, ".par2"), "{name}");
        }
        // A volume already held continues its set by its stem.
        assert!(same_set(
            "fixture.release.vol000-001.par2",
            "fixture.release.vol001-003.par2",
            ".par2"
        ));
        assert!(!same_set(
            "fixture.release.vol000-001.par2",
            "fixture.release-sample.vol01-03.par2",
            ".par2"
        ));
        // A name naming no blocks is never claimed by its name.
        assert!(!same_set(main, "fixture.release.vol15-07.par2", ".par2"));
        assert!(!same_set(main, "fixture.release.par2", ".par2"));
    }
}
