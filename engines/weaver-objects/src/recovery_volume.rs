// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A PAR volume as a recovery object fetches it: what its name says it
// carries, and the ranges of it still to come; and what the object has been
// shown of its namespace's damage.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::{ByteRange, Coverage, MemberId, Namespace, ObjectId};

// The recovery blocks a volume's name says it carries, as every PAR creator
// names a volume: `<stem>.vol<start>+<count>` and the format's `extension`,
// so `name.vol07+08.par2` carries eight.
pub(crate) fn named_blocks(name: &str, extension: &str) -> Option<u64> {
    let lower = name.to_ascii_lowercase();
    let (_, tail) = lower.strip_suffix(extension)?.rsplit_once(".vol")?;
    let (start, count) = tail.split_once('+')?;
    start.parse::<u64>().ok()?;
    count.parse().ok()
}

// The stem a PAR file's name gives its set: the name less the format's
// `extension` and any `.vol<start>+<count>`.
fn stem<'a>(lower: &'a str, extension: &str) -> Option<&'a str> {
    let bare = lower.strip_suffix(extension)?;
    Some(match bare.rsplit_once(".vol") {
        Some((stem, tail))
            if tail.split_once('+').is_some_and(|(start, count)| {
                start.parse::<u64>().is_ok() && count.parse::<u64>().is_ok()
            }) =>
        {
            stem
        }
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
    use super::named_blocks;

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
}
