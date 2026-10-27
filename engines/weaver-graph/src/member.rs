// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A namespace is the list of members an object produces. The root's members
// are the files of the job; a RAR object's are the files inside the RAR.
// Each member is a leaf, or is bound to the object that owns it. The
// namespace records what has arrived for each member, by range and by
// provenance, and the name the tree's ledger gave each one to be written
// under.

use std::collections::{BTreeMap, BTreeSet};

use crate::deliver::Provenance;
use crate::object::ObjectId;
use crate::recovery::Recovery;
use crate::volume::ByteRange;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemberId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: MemberId,
    // What the producer called it. Changes when a recovery object
    // recognises an obfuscated member and renames it, or when its producer,
    // having started it over, announces it again under another name.
    pub name: String,
    // The name it is written under: the wanted name until the tree's one
    // ledger places it, and what that ledger gave it after.
    pub output_name: String,
    pub length: Option<u64>,
    // The object that owns it, or `None` for a leaf. Decided once, at the
    // member boundary, by the sniffer; revised only by a refusal, which
    // unbinds it, or a retraction, which starts it over.
    pub bound_to: Option<ObjectId>,
    pub coverage: Coverage,
    // Nothing more will arrive for it: everything that could has.
    pub closed: bool,
    // The CRC32 its object's headers state for its bytes, when they do.
    pub crc32: Option<u32>,
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

    // Closed whole: nothing more will arrive, every byte did, and every one
    // of them is good. Such a member is delivered whatever its producer or
    // its siblings do after.
    pub fn is_whole(&self) -> bool {
        self.closed && self.is_complete() && self.coverage.is_clean()
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

    // Whether any byte in `range` is held as damage: what clean bytes over
    // it would mend.
    pub fn damaged(&self, range: ByteRange) -> bool {
        self.overlapping(range)
            .any(|run| run.provenance == Provenance::Damaged)
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
    next: u32,
    // The front of each member, up to FIRST_BYTES: what a recovery object
    // recognises an obfuscated member by. Kept apart from the members so a
    // member record stays a plain value.
    first_bytes: BTreeMap<MemberId, Front>,
    // Further postings of a member, each by the member it is: a volume
    // posted again under a name of its own. A posting keeps its own record
    // of what arrived under its name and whether it ended; everywhere else
    // it is the member, and the namespace lists it nowhere.
    copies: BTreeMap<MemberId, MemberId>,
    // The members posted more than once whose own posting ended. Such a
    // member closes once its own posting and every further one have.
    ended: BTreeSet<MemberId>,
}

// What arrived of a member's first FIRST_BYTES, and how: bytes past a
// hole are kept, so the front reaches as far as they do once it fills, and
// damage is a hole with bytes in it, filled by the repair that replaces it.
#[derive(Debug, Default)]
struct Front {
    bytes: Vec<u8>,
    arrived: Coverage,
}

// How much of a member's front the namespace keeps. PAR2 identifies a file
// by the MD5 of its first 16 KiB, and nothing needs more.
pub const FIRST_BYTES: usize = 16 * 1024;

impl Namespace {
    pub fn new() -> Self {
        Self::default()
    }

    // A member the producer announced. Its output name is the tree's to
    // give; until it does, the member wants the name it was announced by.
    pub fn announce(&mut self, name: &str, length: Option<u64>) -> MemberId {
        let id = MemberId(self.next);
        self.next += 1;
        self.members.insert(
            id,
            Member {
                id,
                name: name.to_owned(),
                output_name: name.to_owned(),
                length,
                bound_to: None,
                coverage: Coverage::new(),
                closed: false,
                crc32: None,
            },
        );
        id
    }

    // Restore a member with the identity and names it had before. The next
    // announced member comes after every restored one; the tree's ledger
    // takes the names back with `Tree::rebuild_names`.
    pub fn restore(&mut self, member: Member) {
        self.next = self.next.max(member.id.0 + 1);
        self.members.insert(member.id, member);
    }

    pub fn get(&self, member: MemberId) -> Option<&Member> {
        self.members.get(&member)
    }

    pub fn get_mut(&mut self, member: MemberId) -> Option<&mut Member> {
        self.members.get_mut(&member)
    }

    // The members, each once: a further posting of one is not listed.
    pub fn members(&self) -> impl Iterator<Item = &Member> {
        self.members
            .values()
            .filter(|member| !self.copies.contains_key(&member.id))
    }

    // Every record, the members' and each further posting's own, which a
    // checkpoint keeps.
    pub fn records(&self) -> impl Iterator<Item = &Member> {
        self.members.values()
    }

    pub fn len(&self) -> usize {
        self.members.len() - self.copies.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // The member a posting is: itself, or the member it was folded into.
    pub fn resolve(&self, posting: MemberId) -> MemberId {
        self.copies.get(&posting).copied().unwrap_or(posting)
    }

    // The member `copy` was folded into, when it was.
    pub fn copy_of(&self, copy: MemberId) -> Option<MemberId> {
        self.copies.get(&copy).copied()
    }

    // The further postings of the member, by their own records.
    pub fn copies_of(&self, member: MemberId) -> impl Iterator<Item = &Member> + '_ {
        self.copies
            .iter()
            .filter(move |(_, of)| **of == member)
            .filter_map(|(copy, _)| self.members.get(copy))
    }

    // Every posting of the member: its own record, then its copies'.
    pub fn postings(&self, member: MemberId) -> impl Iterator<Item = &Member> + '_ {
        self.members
            .get(&member)
            .into_iter()
            .chain(self.copies_of(member))
    }

    // Whether `copy` can fold into `into` as another posting of it: their
    // fronts agree wherever both brought them clean, and both did somewhere,
    // since a front that never came, or came damaged, says nothing. A
    // posting that has further postings of its own does not fold, nor does
    // one into itself.
    pub fn folds(&self, copy: MemberId, into: MemberId) -> bool {
        let (Some(one), Some(other)) = (self.members.get(&copy), self.members.get(&into)) else {
            return false;
        };
        if copy == into
            || self.copies.contains_key(&copy)
            || self.copies.contains_key(&into)
            || self.copies.values().any(|of| *of == copy)
        {
            return false;
        }
        let (front, held) = (self.first_bytes(copy), self.first_bytes(into));
        let common = ByteRange::new(0, front.len().min(held.len()) as u64);
        let mut clean = Coverage::new();
        clean_within(&one.coverage, common, &mut clean);
        let mut agreed = 0;
        for run in other.coverage.overlapping(common) {
            if !run.provenance.is_verified() {
                continue;
            }
            for mine in clean.overlapping(run.range) {
                let Some(shared) = mine.range.intersect(run.range) else {
                    continue;
                };
                let (start, end) = (shared.start as usize, shared.end as usize);
                if front[start..end] != held[start..end] {
                    return false;
                }
                agreed += end - start;
            }
        }
        agreed > 0
    }

    // `copy` is another posting of `into`: from now on its bytes are
    // `into`'s, and it keeps only its own record of what arrived under its
    // name. A member that closed before stays closed: what the copy brings
    // after mends it, as a repair would.
    pub fn fold(&mut self, copy: MemberId, into: MemberId) -> bool {
        if !self.folds(copy, into) {
            return false;
        }
        if self.members.get(&into).is_some_and(|record| record.closed) {
            self.ended.insert(into);
        }
        self.copies.insert(copy, into);
        true
    }

    // A fold a checkpoint recorded. Whether the member's own posting had
    // ended is not kept: the restored job closes again every volume with
    // nothing left to fetch, which ends that posting again.
    pub fn restore_copy(&mut self, copy: MemberId, into: MemberId) -> bool {
        let folds = copy != into
            && self.members.contains_key(&copy)
            && self.members.contains_key(&into)
            && !self.copies.contains_key(&into);
        if folds {
            self.copies.insert(copy, into);
        }
        folds
    }

    // The posting brings nothing more. The member it is closes once every
    // posting of it has: true when that is now.
    pub fn end(&mut self, posting: MemberId) -> bool {
        let member = self.resolve(posting);
        match self.members.get_mut(&posting) {
            Some(copy) if posting != member => copy.closed = true,
            Some(_) => {
                self.ended.insert(member);
            }
            None => return false,
        }
        let every = self.ended.contains(&member) && self.copies_of(member).all(|copy| copy.closed);
        if self.copies_of(member).next().is_none() {
            self.ended.remove(&member);
        }
        let Some(record) = self
            .members
            .get_mut(&member)
            .filter(|record| !record.closed)
        else {
            return false;
        };
        record.closed = every;
        every
    }

    // Bytes of `posting` over `range` are not in hand: a restart could not
    // read them back, and they come again. Neither the posting nor the
    // member it is holds them, and the member is open until they come.
    // Returns the member.
    pub fn uncover(&mut self, posting: MemberId, range: ByteRange) -> MemberId {
        let member = self.resolve(posting);
        for id in [posting, member] {
            if let Some(record) = self.members.get_mut(&id) {
                record.coverage.erase(range);
                record.closed = false;
            }
        }
        member
    }

    // Bytes of `posting` arrived over `range` with `provenance`. The
    // posting records them, and the member it is takes them except where
    // they are damaged and another posting brought those bytes clean, or
    // its own record already answers them: a clean byte replaces a damaged
    // one or a hole, whichever posting brings it, a damaged byte never
    // replaces a clean one another posting brought, and damage never takes
    // back a repair. Returns the member and the parts of `range` it took,
    // in order.
    pub fn arrive(
        &mut self,
        posting: MemberId,
        range: ByteRange,
        provenance: Provenance,
    ) -> (MemberId, Vec<ByteRange>) {
        let member = self.resolve(posting);
        let taken = if provenance.is_verified() {
            vec![range]
        } else {
            gaps(range, &self.standing(posting, member, range, provenance))
        };
        if posting != member
            && let Some(copy) = self.members.get_mut(&posting)
        {
            copy.coverage.write(range, provenance);
        }
        if let Some(record) = self.members.get_mut(&member) {
            for part in &taken {
                record.coverage.write(*part, provenance);
            }
        }
        (member, taken)
    }

    // What of `range` stands against bytes of `posting` arriving with
    // `provenance`, which is not verified: what another posting brought
    // clean, and what the member's own record answers.
    fn standing(
        &self,
        posting: MemberId,
        member: MemberId,
        range: ByteRange,
        provenance: Provenance,
    ) -> Coverage {
        let mut standing = self.clean_elsewhere(posting, member, range);
        if let Some(record) = self.members.get(&member) {
            for run in record.coverage.overlapping(range) {
                if run.provenance.answers(provenance)
                    && let Some(piece) = run.range.intersect(range)
                {
                    standing.write(piece, Provenance::Fresh);
                }
            }
        }
        standing
    }

    // What of `range` a posting of the member other than `posting` brought
    // clean. A further posting's own record says it of itself; the
    // member's own posting is whatever the member holds clean that
    // `posting` did not bring.
    fn clean_elsewhere(&self, posting: MemberId, member: MemberId, range: ByteRange) -> Coverage {
        let mut clean = Coverage::new();
        for copy in self.copies_of(member).filter(|copy| copy.id != posting) {
            clean_within(&copy.coverage, range, &mut clean);
        }
        if posting != member
            && let (Some(record), Some(own)) =
                (self.members.get(&member), self.members.get(&posting))
        {
            let mut held = Coverage::new();
            clean_within(&record.coverage, range, &mut held);
            let mut brought = Coverage::new();
            clean_within(&own.coverage, range, &mut brought);
            for run in brought.runs() {
                held.erase(run.range);
            }
            for run in held.runs() {
                clean.write(run.range, Provenance::Fresh);
            }
        }
        clean
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
    // recognised the member. The name it now wants goes up to the tree,
    // whose ledger moves its output name, in `Tree::rename`.
    pub fn rename(&mut self, member: MemberId, name: &str) -> Option<&Member> {
        let found = self.members.get_mut(&member)?;
        found.name = name.to_owned();
        Some(found)
    }

    // A recovery object recognised the member as a file it describes: it
    // takes the description's name and length. A member none of whose bytes
    // decoded reached only the post's encoded bound, and what it reached
    // past the length is not it.
    pub fn recognise(&mut self, member: MemberId, name: &str, length: u64) -> Option<&Member> {
        let found = self.members.get_mut(&member)?;
        found.name = name.to_owned();
        let reached = found.coverage.runs().last().map_or(0, |run| run.range.end);
        if reached > length {
            found.coverage.erase(ByteRange::new(length, reached));
        }
        found.length = Some(length);
        Some(found)
    }

    // The member starts over: its bytes are forgotten and it is unbound. Its
    // output name is kept, so what comes back lands where it was, until its
    // producer announces it again under another name.
    pub fn retract(&mut self, member: MemberId) {
        if let Some(found) = self.members.get_mut(&member) {
            found.coverage.clear();
            found.bound_to = None;
            found.length = None;
            found.closed = false;
            found.crc32 = None;
        }
        self.ended.remove(&member);
        self.first_bytes.remove(&member);
    }

    // The member is gone for good, and its further postings with it. Its
    // name is the tree's to release.
    pub fn remove(&mut self, member: MemberId) -> Option<Member> {
        let copies: Vec<MemberId> = self.copies_of(member).map(|copy| copy.id).collect();
        for copy in copies {
            self.copies.remove(&copy);
            self.first_bytes.remove(&copy);
            self.members.remove(&copy);
        }
        self.copies.remove(&member);
        self.ended.remove(&member);
        self.first_bytes.remove(&member);
        self.members.remove(&member)
    }

    // Keep the part of a delivered range that lies in the member's first
    // FIRST_BYTES, with how it arrived. Bytes over what is kept replace it,
    // the way a repair replaces a damaged first article.
    pub fn keep_first_bytes(
        &mut self,
        member: MemberId,
        offset: u64,
        bytes: &[u8],
        provenance: Provenance,
    ) {
        if !self.members.contains_key(&member) {
            return;
        }
        let Ok(start) = usize::try_from(offset) else {
            return;
        };
        if start >= FIRST_BYTES || bytes.is_empty() {
            return;
        }
        let end = start.saturating_add(bytes.len()).min(FIRST_BYTES);
        let front = self.first_bytes.entry(member).or_default();
        if end > front.bytes.len() {
            front.bytes.resize(end, 0);
        }
        front.bytes[start..end].copy_from_slice(&bytes[..end - start]);
        front
            .arrived
            .write(ByteRange::new(start as u64, end as u64), provenance);
    }

    // The member's front: what arrived contiguous from offset zero, at most
    // FIRST_BYTES, because a front with a hole in it identifies nothing,
    // and damage says nothing of what the member is.
    pub fn first_bytes(&self, member: MemberId) -> &[u8] {
        let Some(front) = self.first_bytes.get(&member) else {
            return &[];
        };
        let mut reached = 0;
        for run in front.arrived.runs() {
            if run.range.start != reached || run.provenance == Provenance::Damaged {
                break;
            }
            reached = run.range.end;
        }
        &front.bytes[..usize::try_from(reached).unwrap_or(0).min(front.bytes.len())]
    }

    // The bytes the namespace keeps in memory for the record `member`: the
    // record with its names and the runs of what arrived, and its front with
    // the runs of where that arrived. Zero for a record it does not hold.
    pub fn resident(&self, member: MemberId) -> u64 {
        let Some(record) = self.members.get(&member) else {
            return 0;
        };
        let run = std::mem::size_of::<(u64, Run)>();
        let mut bytes = std::mem::size_of::<Member>()
            + record.name.len()
            + record.output_name.len()
            + record.coverage.runs.len() * run;
        if let Some(front) = self.first_bytes.get(&member) {
            bytes +=
                std::mem::size_of::<Front>() + front.bytes.len() + front.arrived.runs.len() * run;
        }
        bytes as u64
    }

    pub fn find_by_name(&self, name: &str) -> Option<&Member> {
        self.members().find(|member| member.name == name)
    }

    // Whether every byte that arrived for every member arrived verified.
    // Coverage is the one record of damage; completion reads it here.
    pub fn is_clean(&self) -> bool {
        self.members().all(|member| member.coverage.is_clean())
    }

    // What of the member's damaged `range` a recovery object that found the
    // member whole or made it so proves: the part its checks reach, over
    // the member's length, or up to the range's end while that length is
    // not known.
    pub fn proven(
        &self,
        member: MemberId,
        range: ByteRange,
        recovery: &dyn Recovery,
    ) -> Option<ByteRange> {
        let length = self.get(member)?.length.unwrap_or(range.end);
        range.intersect(recovery.proves(member, length))
    }

    // Every damaged range of every member, in member order.
    pub fn damaged_ranges(&self) -> impl Iterator<Item = (MemberId, ByteRange)> + '_ {
        self.members().flat_map(|member| {
            member
                .coverage
                .damaged_ranges()
                .map(move |range| (member.id, range))
        })
    }
}

// Record in `into` what of `range` arrived clean in `coverage`.
fn clean_within(coverage: &Coverage, range: ByteRange, into: &mut Coverage) {
    for run in coverage.overlapping(range) {
        if run.provenance.is_verified()
            && let Some(piece) = run.range.intersect(range)
        {
            into.write(piece, Provenance::Fresh);
        }
    }
}

// The parts of `range` that `covered` does not reach, in order.
fn gaps(range: ByteRange, covered: &Coverage) -> Vec<ByteRange> {
    let mut out = Vec::new();
    let mut at = range.start;
    for run in covered.overlapping(range) {
        if run.range.start > at {
            out.push(ByteRange::new(at, run.range.start));
        }
        at = at.max(run.range.end);
    }
    if at < range.end {
        out.push(ByteRange::new(at, range.end));
    }
    out
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

    // Damage arriving over a member whose middle a repair rebuilt: the
    // member takes it only at the damaged ends, and the repair stands.
    #[test]
    fn damage_never_takes_back_a_repair() {
        let mut namespace = Namespace::new();
        let member = namespace.announce("ember.bin", Some(30));
        namespace.arrive(member, ByteRange::new(0, 30), Provenance::Damaged);
        namespace.arrive(member, ByteRange::new(10, 20), Provenance::Repaired);
        let (_, taken) = namespace.arrive(member, ByteRange::new(0, 30), Provenance::Damaged);
        assert_eq!(taken, vec![ByteRange::new(0, 10), ByteRange::new(20, 30)]);
        assert_eq!(
            runs(&namespace.get(member).unwrap().coverage),
            vec![
                (0, 10, Provenance::Damaged),
                (10, 20, Provenance::Repaired),
                (20, 30, Provenance::Damaged),
            ]
        );
        let (_, taken) = namespace.arrive(member, ByteRange::new(5, 25), Provenance::Fresh);
        assert_eq!(taken, vec![ByteRange::new(5, 25)]);
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
    fn a_rename_changes_the_wanted_name_and_leaves_the_output_name_to_the_tree() {
        let mut namespace = Namespace::new();
        let member = namespace.announce("3f9a.bin", Some(10));
        assert_eq!(namespace.get(member).unwrap().output_name, "3f9a.bin");
        assert!(namespace.rename(member, "other.srt").is_some());
        let record = namespace.get(member).unwrap();
        assert_eq!(
            (record.name.as_str(), record.output_name.as_str()),
            ("other.srt", "3f9a.bin")
        );
    }

    #[test]
    fn a_member_is_charged_its_record_and_the_front_kept_for_it() {
        let mut namespace = Namespace::new();
        let member = namespace.announce("ember.bin", Some(2 * FIRST_BYTES as u64));
        let run = std::mem::size_of::<(u64, Run)>() as u64;
        let record = std::mem::size_of::<Member>() as u64 + 2 * "ember.bin".len() as u64;
        assert_eq!(namespace.resident(member), record);
        let bytes = vec![7u8; 2 * FIRST_BYTES];
        namespace
            .get_mut(member)
            .unwrap()
            .coverage
            .write(ByteRange::new(0, bytes.len() as u64), Provenance::Fresh);
        namespace.keep_first_bytes(member, 0, &bytes, Provenance::Fresh);
        let front = std::mem::size_of::<Front>() as u64 + FIRST_BYTES as u64 + run;
        assert_eq!(namespace.resident(member), record + run + front);
        namespace.retract(member);
        assert_eq!(namespace.resident(member), record);
        namespace.remove(member);
        assert_eq!(namespace.resident(member), 0);
    }

    // A lost first article arrives as damage, zero filled: it is no front,
    // and what arrived clean past it waits for the repair that fills it.
    #[test]
    fn damage_at_the_front_identifies_nothing_until_a_repair_fills_it() {
        let mut namespace = Namespace::new();
        let member = namespace.announce("ember.bin", Some(600));
        namespace.keep_first_bytes(member, 0, &[0; 200], Provenance::Damaged);
        namespace.keep_first_bytes(member, 200, &[7; 400], Provenance::Fresh);
        assert_eq!(namespace.first_bytes(member), &[]);
        namespace.keep_first_bytes(member, 0, &[9; 200], Provenance::Repaired);
        assert_eq!(namespace.first_bytes(member).len(), 600);
        assert_eq!(namespace.first_bytes(member)[199..201], [9, 7]);
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
