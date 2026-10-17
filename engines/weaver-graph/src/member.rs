// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A namespace is the list of members an object produces. The root's members
// are the files of the job; a RAR object's are the files inside the RAR.
// Each member is a leaf, or is bound to the object that owns it. The
// namespace records what has arrived for each member, by range and by
// provenance, and allocates the name each one is written under.

use std::collections::BTreeMap;

use crate::deliver::Provenance;
use crate::names::NameLedger;
use crate::object::ObjectId;
use crate::volume::ByteRange;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemberId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: MemberId,
    // What the producer called it. Changes at most once, when a recovery
    // object recognises an obfuscated member and renames it.
    pub name: String,
    // The name it is written under, allocated by the namespace.
    pub output_name: String,
    pub length: Option<u64>,
    // The object that owns it, or `None` for a leaf. Decided once, at the
    // member boundary, by the sniffer; revised only by a refusal, which
    // unbinds it, or a retraction, which starts it over.
    pub bound_to: Option<ObjectId>,
    pub coverage: Coverage,
}

impl Member {
    pub fn is_leaf(&self) -> bool {
        self.bound_to.is_none()
    }

    // Complete means every byte of a known length has arrived, in any
    // provenance. Whether it is also clean is `coverage.is_clean()`.
    pub fn is_complete(&self) -> bool {
        self.length
            .is_some_and(|length| self.coverage.covered(ByteRange::new(0, length)))
    }
}

// One contiguous run of bytes that arrived, with how they arrived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Run {
    pub range: ByteRange,
    pub provenance: Provenance,
}

// What has arrived for one member: disjoint runs in offset order, adjacent
// runs of one provenance merged. Writing a range replaces whatever it
// overlapped, which is what a repair does and what a replay must do.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    runs: BTreeMap<u64, Run>,
}

impl Coverage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn runs(&self) -> impl Iterator<Item = &Run> {
        self.runs.values()
    }

    pub fn bytes_covered(&self) -> u64 {
        self.runs.values().map(|run| run.range.len()).sum()
    }

    pub fn covered(&self, range: ByteRange) -> bool {
        if range.is_empty() {
            return true;
        }
        let mut cursor = range.start;
        for run in self.runs.range(..range.end).map(|(_, run)| run) {
            if run.range.end <= cursor {
                continue;
            }
            if run.range.start > cursor {
                return false;
            }
            cursor = run.range.end;
            if cursor >= range.end {
                return true;
            }
        }
        false
    }

    // Whether every byte in `range` arrived with a verified provenance.
    pub fn verified(&self, range: ByteRange) -> bool {
        self.covered(range)
            && self
                .overlapping(range)
                .all(|run| run.provenance.is_verified())
    }

    pub fn is_clean(&self) -> bool {
        self.runs.values().all(|run| run.provenance.is_verified())
    }

    pub fn damaged_ranges(&self) -> impl Iterator<Item = ByteRange> + '_ {
        self.runs
            .values()
            .filter(|run| run.provenance == Provenance::Damaged)
            .map(|run| run.range)
    }

    pub fn overlapping(&self, range: ByteRange) -> impl Iterator<Item = &Run> {
        self.runs
            .values()
            .filter(move |run| run.range.intersect(range).is_some())
    }

    // Record that `range` arrived with `provenance`, replacing any earlier
    // record of those bytes.
    pub fn write(&mut self, range: ByteRange, provenance: Provenance) {
        if range.is_empty() {
            return;
        }
        self.cut(range);
        let mut run = Run { range, provenance };
        // Merge with a same-provenance neighbour on either side.
        if let Some((&start, &before)) = self.runs.range(..run.range.start).next_back()
            && before.range.end == run.range.start
            && before.provenance == provenance
        {
            self.runs.remove(&start);
            run.range.start = before.range.start;
        }
        if let Some(&after) = self.runs.get(&run.range.end)
            && after.provenance == provenance
        {
            self.runs.remove(&after.range.start);
            run.range.end = after.range.end;
        }
        self.runs.insert(run.range.start, run);
    }

    // Forget `range`: the bytes are gone, for example after a retraction.
    pub fn erase(&mut self, range: ByteRange) {
        self.cut(range);
    }

    pub fn clear(&mut self) {
        self.runs.clear();
    }

    // Remove `range` from every run it overlaps, keeping the parts outside it.
    fn cut(&mut self, range: ByteRange) {
        let touched: Vec<Run> = self
            .runs
            .range(..range.end)
            .map(|(_, run)| *run)
            .filter(|run| run.range.end > range.start)
            .collect();
        for run in touched {
            self.runs.remove(&run.range.start);
            if run.range.start < range.start {
                let left = Run {
                    range: ByteRange::new(run.range.start, range.start),
                    ..run
                };
                self.runs.insert(left.range.start, left);
            }
            if run.range.end > range.end {
                let right = Run {
                    range: ByteRange::new(range.end, run.range.end),
                    ..run
                };
                self.runs.insert(right.range.start, right);
            }
        }
    }
}

// The members of one object, and the names they are written under.
#[derive(Debug, Default)]
pub struct Namespace {
    members: BTreeMap<MemberId, Member>,
    names: NameLedger,
    next: u32,
    // The front of each member, up to FIRST_BYTES: what a recovery object
    // recognises an obfuscated member by. Kept apart from the members so a
    // member record stays a plain value.
    first_bytes: BTreeMap<MemberId, Vec<u8>>,
}

// How much of a member's front the namespace keeps. PAR2 identifies a file
// by the MD5 of its first 16 KiB, and nothing needs more.
pub const FIRST_BYTES: usize = 16 * 1024;

impl Namespace {
    pub fn new() -> Self {
        Self::default()
    }

    // A member the producer announced. Its output name is allocated here
    // and nowhere else.
    pub fn announce(&mut self, name: &str, length: Option<u64>) -> MemberId {
        let id = MemberId(self.next);
        self.next += 1;
        let output_name = self.names.allocate(id, name);
        self.members.insert(
            id,
            Member {
                id,
                name: name.to_owned(),
                output_name,
                length,
                bound_to: None,
                coverage: Coverage::new(),
            },
        );
        id
    }

    // Restore a member with the identity and name it had before. The next
    // announced member comes after every restored one.
    pub fn restore(&mut self, member: Member) {
        self.next = self.next.max(member.id.0 + 1);
        self.names.allocate(member.id, &member.output_name);
        self.members.insert(member.id, member);
    }

    pub fn get(&self, member: MemberId) -> Option<&Member> {
        self.members.get(&member)
    }

    pub fn get_mut(&mut self, member: MemberId) -> Option<&mut Member> {
        self.members.get_mut(&member)
    }

    pub fn members(&self) -> impl Iterator<Item = &Member> {
        self.members.values()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn bind(&mut self, member: MemberId, object: ObjectId) {
        if let Some(found) = self.members.get_mut(&member) {
            found.bound_to = Some(object);
        }
    }

    pub fn unbind(&mut self, member: MemberId) {
        if let Some(found) = self.members.get_mut(&member) {
            found.bound_to = None;
        }
    }

    // The one identity revision after the first bytes: a recovery object
    // recognised the member. The output name follows.
    pub fn rename(&mut self, member: MemberId, name: &str) -> Option<String> {
        let output_name = self.names.allocate(member, name);
        let found = self.members.get_mut(&member)?;
        found.name = name.to_owned();
        found.output_name = output_name.clone();
        Some(output_name)
    }

    // The member starts over: its bytes are forgotten and it is unbound. Its
    // output name is kept, so what comes back lands where it was.
    pub fn retract(&mut self, member: MemberId) {
        if let Some(found) = self.members.get_mut(&member) {
            found.coverage.clear();
            found.bound_to = None;
            found.length = None;
        }
        self.first_bytes.remove(&member);
    }

    // The member is gone for good, with its name.
    pub fn remove(&mut self, member: MemberId) -> Option<Member> {
        self.names.release(member);
        self.first_bytes.remove(&member);
        self.members.remove(&member)
    }

    // Keep the part of a delivered range that lies in the member's front.
    // Only bytes contiguous from offset zero count, because a front with a
    // hole in it identifies nothing; bytes over what is kept replace it, the
    // way a repair replaces a damaged first article.
    pub fn keep_first_bytes(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        if !self.members.contains_key(&member) {
            return;
        }
        let front = self.first_bytes.entry(member).or_default();
        let Ok(start) = usize::try_from(offset) else {
            return;
        };
        if start > front.len() || start >= FIRST_BYTES {
            return;
        }
        let end = start.saturating_add(bytes.len()).min(FIRST_BYTES);
        if end > front.len() {
            front.resize(end, 0);
        }
        front[start..end].copy_from_slice(&bytes[..end - start]);
    }

    // The member's front as far as it arrived, at most FIRST_BYTES.
    pub fn first_bytes(&self, member: MemberId) -> &[u8] {
        self.first_bytes.get(&member).map_or(&[], Vec::as_slice)
    }

    pub fn find_by_name(&self, name: &str) -> Option<&Member> {
        self.members.values().find(|member| member.name == name)
    }

    // Whether every byte that arrived for every member arrived verified.
    // Coverage is the one record of damage; completion reads it here.
    pub fn is_clean(&self) -> bool {
        self.members
            .values()
            .all(|member| member.coverage.is_clean())
    }

    // Every damaged range of every member, in member order.
    pub fn damaged_ranges(&self) -> impl Iterator<Item = (MemberId, ByteRange)> + '_ {
        self.members.values().flat_map(|member| {
            member
                .coverage
                .damaged_ranges()
                .map(move |range| (member.id, range))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runs(coverage: &Coverage) -> Vec<(u64, u64, Provenance)> {
        coverage
            .runs()
            .map(|run| (run.range.start, run.range.end, run.provenance))
            .collect()
    }

    #[test]
    fn adjacent_runs_of_one_provenance_merge() {
        let mut coverage = Coverage::new();
        coverage.write(ByteRange::new(0, 10), Provenance::Fresh);
        coverage.write(ByteRange::new(20, 30), Provenance::Fresh);
        coverage.write(ByteRange::new(10, 20), Provenance::Fresh);
        assert_eq!(runs(&coverage), vec![(0, 30, Provenance::Fresh)]);
        assert!(coverage.covered(ByteRange::new(0, 30)));
        assert!(!coverage.covered(ByteRange::new(0, 31)));
    }

    #[test]
    fn a_repair_replaces_the_damaged_middle_of_a_run() {
        let mut coverage = Coverage::new();
        coverage.write(ByteRange::new(0, 30), Provenance::Damaged);
        coverage.write(ByteRange::new(10, 20), Provenance::Repaired);
        assert_eq!(
            runs(&coverage),
            vec![
                (0, 10, Provenance::Damaged),
                (10, 20, Provenance::Repaired),
                (20, 30, Provenance::Damaged),
            ]
        );
        assert!(coverage.covered(ByteRange::new(0, 30)));
        assert!(!coverage.verified(ByteRange::new(0, 30)));
        assert!(coverage.verified(ByteRange::new(10, 20)));
        assert_eq!(coverage.damaged_ranges().count(), 2);
    }

    #[test]
    fn a_gap_is_not_covered_and_erase_opens_one() {
        let mut coverage = Coverage::new();
        coverage.write(ByteRange::new(0, 10), Provenance::Fresh);
        coverage.write(ByteRange::new(15, 20), Provenance::Fresh);
        assert!(!coverage.covered(ByteRange::new(5, 16)));
        coverage.erase(ByteRange::new(2, 4));
        assert_eq!(
            runs(&coverage),
            vec![
                (0, 2, Provenance::Fresh),
                (4, 10, Provenance::Fresh),
                (15, 20, Provenance::Fresh)
            ]
        );
    }

    #[test]
    fn announced_members_get_distinct_output_names_and_a_rename_follows() {
        let mut namespace = Namespace::new();
        let first = namespace.announce("subs.srt", Some(10));
        let second = namespace.announce("subs.srt", None);
        assert_eq!(namespace.get(first).unwrap().output_name, "subs.srt");
        assert_eq!(namespace.get(second).unwrap().output_name, "subs (1).srt");
        assert_eq!(
            namespace.rename(second, "other.srt").as_deref(),
            Some("other.srt")
        );
        assert_eq!(namespace.get(second).unwrap().name, "other.srt");
    }

    #[test]
    fn a_member_is_complete_when_its_known_length_is_covered() {
        let mut namespace = Namespace::new();
        let member = namespace.announce("a.bin", Some(4));
        namespace
            .get_mut(member)
            .unwrap()
            .coverage
            .write(ByteRange::new(0, 4), Provenance::Fresh);
        assert!(namespace.get(member).unwrap().is_complete());
        let restored = namespace.remove(member).unwrap();
        namespace.restore(restored);
        assert_eq!(namespace.announce("b.bin", None), MemberId(1));
    }
}
