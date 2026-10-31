// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A namespace is the list of members an object produces. The root's members
// are the files of the job; a RAR object's are the files inside the RAR.
// Each member is a leaf, or is bound to the object that owns it. The
// namespace records what has arrived for each member, by range and by
// provenance, and the name the tree's ledger gave each one to be written
// under.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::deliver::Provenance;
use crate::evidence::Algorithm;
use crate::marks::Marked;
use crate::object::{Kind, ObjectId};
use crate::recovery::Recovery;
use crate::sniff::{Sniff, signed, sniff};
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
    // Its front came: clean bytes from offset zero were once in hand, so
    // more than its name placed it, whether or not the front is kept now.
    // Only a retraction, which starts the member over, takes it back.
    pub fronted: bool,
    // The kind whose reading of it was refuted: by a later piece's own
    // bytes against the claim its set's names made, or by the name a
    // recovery set gave it. That kind is no answer to who owns it, now or
    // after a restart: its bytes say (`said`). Only a retraction takes it
    // back.
    pub refuted: Option<Kind>,
}

impl Member {
    // What the member is, as founding asks it: by its name and its front,
    // except that the kind whose reading of it was refuted is no answer,
    // and then its bytes alone say. The one question every path asks that
    // decides who owns a member.
    pub fn said(&self, front: &[u8]) -> Sniff {
        match sniff(&self.name, front, self.length) {
            Sniff::Object(kind) if Some(kind) == self.refuted => signed(front, self.length),
            said => said,
        }
    }

    // The kind of digest its stated check is of: what a level that judges
    // the member folds as its bytes pass. A member that states no check
    // needs none, and nothing is folded for it.
    pub fn check(&self) -> Option<Algorithm> {
        self.crc32.map(|_| Algorithm::Crc32)
    }

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
            .any(|run| run.provenance.is_damaged())
    }

    pub fn damaged_ranges(&self) -> impl Iterator<Item = ByteRange> + '_ {
        self.runs
            .values()
            .filter(|run| run.provenance.is_damaged())
            .map(|run| run.range)
    }

    // The runs `range` reaches, found from the one at or before its start,
    // so a member's every arrival reads only the runs it lands on.
    pub fn overlapping(&self, range: ByteRange) -> impl Iterator<Item = &Run> {
        let from = self
            .runs
            .range(..=range.start)
            .next_back()
            .map_or(range.start, |(start, _)| *start);
        self.runs
            .range(from..range.end.max(from))
            .map(|(_, run)| run)
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

// The members of one object, and the names they are written under. Every
// way to change a member's record marks it, as does every change to what
// the tree keeps of it beside the record, so a snapshot reads again only the
// members marked since it read last.
#[derive(Debug, Default)]
pub struct Namespace {
    members: Marked<MemberId, Member>,
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
    // Which write left the front as it is: drawn from one sequence every
    // namespace shares, so no two writes anywhere carry the same stamp. A
    // recovery object that moves between namespaces, or outlives one, never
    // mistakes another front for one it read.
    stamp: u64,
}

// The stamps fronts are written under. Zero is no front.
static FRONT_WRITES: AtomicU64 = AtomicU64::new(1);

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
                fronted: false,
                refuted: None,
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

    // The member's rows changed by what the tree keeps of it beside its
    // record: its owner, whether it waits on one, what it was given up as.
    pub fn mark(&mut self, member: MemberId) {
        self.members.mark(member);
    }

    // The generation of the members' marks: a change to one marks it under
    // a later one.
    pub fn generation(&self) -> u64 {
        self.members.marks().at()
    }

    // The members marked since generation `at`, one gone since among them.
    pub fn changed_since(&self, at: u64) -> impl Iterator<Item = MemberId> + '_ {
        self.members.marks().since(at)
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
    // `into`'s, and its own record holds what `into` took of it, which is
    // nothing yet. What it recorded while it was a member of its own comes
    // back, each run with how it arrived: those bytes go down again as
    // `into`'s, and where `into` holds them clean already they were never
    // the copy's to give. A member that closed before stays closed: what
    // the copy brings after mends it, as a repair would.
    pub fn fold(&mut self, copy: MemberId, into: MemberId) -> Option<Coverage> {
        if !self.folds(copy, into) {
            return None;
        }
        if self.members.get(&into).is_some_and(|record| record.closed) {
            self.ended.insert(into);
        }
        self.copies.insert(copy, into);
        self.members
            .get_mut(&copy)
            .map(|record| std::mem::take(&mut record.coverage))
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
            self.members.mark(copy);
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
    // another posting brought those bytes first, or its own record already
    // answers them: the first copy, clean or refuted, stands against every
    // other posting's copy but a repair, a repair stands against everything
    // but a repair, and anything replaces other damage or a hole. What it takes
    // lands as it came, except under a checksum's verdict, which stands
    // over the bytes it judged until a repair. Returns the member and the
    // parts of `range` it took, in order, each with what it landed as.
    pub fn arrive(
        &mut self,
        posting: MemberId,
        range: ByteRange,
        provenance: Provenance,
    ) -> (MemberId, Vec<(ByteRange, Provenance)>) {
        let (member, taken) = self.taking(posting, range, provenance);
        // A further posting records what the member took of it: bytes the
        // first copy answered were never its, and say nothing of it.
        let postings: &[MemberId] = if posting == member {
            &[member]
        } else {
            &[posting, member]
        };
        for id in postings {
            if let Some(record) = self.members.get_mut(id) {
                for &(part, landed) in &taken {
                    record.coverage.write(part, landed);
                }
            }
        }
        (member, taken)
    }

    // The same acceptance decision used by arrive, before evidence is fed:
    // the parts of `range` the member takes, each with what it lands as.
    pub fn taking(
        &self,
        posting: MemberId,
        range: ByteRange,
        provenance: Provenance,
    ) -> (MemberId, Vec<(ByteRange, Provenance)>) {
        let member = self.resolve(posting);
        let held = self.members.get(&member).map(|record| &record.coverage);
        let mut taken: Vec<(ByteRange, Provenance)> = Vec::new();
        let mut take = |piece: ByteRange, landed: Provenance| match taken.last_mut() {
            Some((last, was)) if last.end == piece.start && *was == landed => last.end = piece.end,
            _ => taken.push((piece, landed)),
        };
        for part in gaps(range, &self.standing(posting, member, range, provenance)) {
            let mut at = part.start;
            for run in held.into_iter().flat_map(|held| held.overlapping(part)) {
                let Some(piece) = run.range.intersect(part) else {
                    continue;
                };
                if piece.start > at {
                    take(ByteRange::new(at, piece.start), provenance);
                }
                take(piece, run.provenance.landing(provenance));
                at = piece.end;
            }
            if at < part.end {
                take(ByteRange::new(at, part.end), provenance);
            }
        }
        (member, taken)
    }

    // What of `range` stands against bytes of `posting` arriving with
    // `provenance`: what another posting brought first, against any copy
    // but a repair, since the first copy stands, and what the member's own
    // record answers.
    fn standing(
        &self,
        posting: MemberId,
        member: MemberId,
        range: ByteRange,
        provenance: Provenance,
    ) -> Coverage {
        let mut standing = match provenance {
            Provenance::Repaired => Coverage::new(),
            Provenance::Fresh | Provenance::Damaged | Provenance::Refuted => {
                self.first_elsewhere(posting, member, range)
            }
        };
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
    // first. A further posting's own record says what it brought clean; the
    // member's own posting is whatever the member holds that `posting` did
    // not bring, but damage: a copy a checksum refuted is still the first
    // copy, and only a repair displaces it.
    fn first_elsewhere(&self, posting: MemberId, member: MemberId, range: ByteRange) -> Coverage {
        let mut clean = Coverage::new();
        for copy in self.copies_of(member).filter(|copy| copy.id != posting) {
            clean_within(&copy.coverage, range, &mut clean);
        }
        if posting != member
            && let (Some(record), Some(own)) =
                (self.members.get(&member), self.members.get(&posting))
        {
            let mut held = Coverage::new();
            for run in record.coverage.overlapping(range) {
                if run.provenance != Provenance::Damaged
                    && let Some(piece) = run.range.intersect(range)
                {
                    held.write(piece, Provenance::Fresh);
                }
            }
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

    // A member bound under its own name is a file its recovery set describes
    // as `size` long; `verified` says the set's blocks verify, as it ruled
    // it clean or repaired it. The one place a recovery set settles a
    // member's length: verified, the member is the length the set
    // describes, shorter or longer than what arrived, as par2 rebuilds a
    // file of another size at the size it describes. Before that, a member
    // that reached less and holds damage takes the described length, so
    // the tail it lacks is damage to repair. True when its length changed.
    pub fn take_described(&mut self, member: MemberId, size: u64, verified: bool) -> bool {
        let Some(found) = self.members.get(&member) else {
            return false;
        };
        let differs = found.length.is_some_and(|length| length != size);
        let short = found.length.is_some_and(|length| length < size) && !found.coverage.is_clean();
        if (verified && differs) || short {
            let name = found.name.clone();
            self.recognise(member, &name, size);
            return true;
        }
        false
    }

    // A recovery object repaired the member and describes it as `size`
    // long, but it is booked longer, with no verified byte past that size:
    // what lies past was booked for articles that never came, and the
    // member keeps its name and takes the described size. True when it did.
    pub fn trim_booked(&mut self, member: MemberId, size: u64) -> bool {
        let Some(found) = self.members.get(&member) else {
            return false;
        };
        let booked = found.length.is_some_and(|length| length > size)
            && !found
                .coverage
                .overlapping(ByteRange::new(size, u64::MAX))
                .any(|run| run.provenance.is_verified());
        if booked {
            let name = found.name.clone();
            self.recognise(member, &name, size);
        }
        booked
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
            found.fronted = false;
            found.refuted = None;
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
        front.stamp = FRONT_WRITES.fetch_add(1, Ordering::Relaxed);
        if !self.first_bytes(member).is_empty()
            && let Some(record) = self.members.get_mut(&member)
        {
            record.fronted = true;
        }
    }

    // The member's front: what arrived contiguous from offset zero, at most
    // FIRST_BYTES, because a front with a hole in it identifies nothing,
    // and damage says nothing of what the member is.
    pub fn first_bytes(&self, member: MemberId) -> &[u8] {
        self.stamped_front(member).1
    }

    // The member's front with the stamp of the write that left it so: the
    // same stamp is the same front, and zero is none. Fronts are not kept
    // across a restart, so a restored front comes again under a new stamp.
    pub fn stamped_front(&self, member: MemberId) -> (u64, &[u8]) {
        let Some(front) = self.first_bytes.get(&member) else {
            return (0, &[]);
        };
        let mut reached = 0;
        for run in front.arrived.runs() {
            if run.range.start != reached || run.provenance.is_damaged() {
                break;
            }
            reached = run.range.end;
        }
        let kept = usize::try_from(reached).unwrap_or(0).min(front.bytes.len());
        (front.stamp, &front.bytes[..kept])
    }

    // The member's front came once and is not in hand: a restart kept the
    // record of it, not its bytes. `Some(false)` while it comes again,
    // `Some(true)` once its first byte is lost, `None` when its front is in
    // hand or never came.
    pub fn front_away(&self, member: MemberId) -> Option<bool> {
        let record = self.members.get(&member)?;
        (record.fronted && self.first_bytes(member).is_empty())
            .then(|| record.coverage.damaged(ByteRange::new(0, 1)))
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

    fn landed(pieces: &[(u64, u64, Provenance)]) -> Vec<(ByteRange, Provenance)> {
        pieces
            .iter()
            .map(|&(start, end, provenance)| (ByteRange::new(start, end), provenance))
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

    // A member posted short of the length its set describes takes that
    // length when it holds damage or the set's blocks verify, and keeps its
    // own when it reached less clean and nothing verified it. One booked
    // longer, or longer by bytes that arrived, takes it once verified.
    #[test]
    fn a_described_member_takes_its_length_when_short_and_damaged_or_verified() {
        let short = |damaged: bool, rebuilt: bool| {
            let mut namespace = Namespace::new();
            let member = namespace.announce("ember.bin", Some(20));
            let provenance = match damaged {
                true => Provenance::Damaged,
                false => Provenance::Fresh,
            };
            namespace.arrive(member, ByteRange::new(0, 20), provenance);
            let changed = namespace.take_described(member, 30, rebuilt);
            (changed, namespace.get(member).unwrap().length)
        };
        assert_eq!(short(false, false), (false, Some(20)));
        assert_eq!(short(true, false), (true, Some(30)));
        assert_eq!(short(false, true), (true, Some(30)));
        let booked = |rebuilt: bool| {
            let mut namespace = Namespace::new();
            let member = namespace.announce("ember.bin", Some(40));
            namespace.arrive(member, ByteRange::new(0, 30), Provenance::Repaired);
            let changed = namespace.take_described(member, 30, rebuilt);
            (changed, namespace.get(member).unwrap().length)
        };
        assert_eq!(booked(false), (false, Some(40)));
        assert_eq!(booked(true), (true, Some(30)));
        let longer = |verified: bool| {
            let mut namespace = Namespace::new();
            let member = namespace.announce("ember.bin", Some(40));
            namespace.arrive(member, ByteRange::new(0, 40), Provenance::Fresh);
            let changed = namespace.take_described(member, 30, verified);
            let record = namespace.get(member).unwrap();
            (
                changed,
                record.length,
                record.coverage.covered(ByteRange::new(30, 40)),
            )
        };
        assert_eq!(longer(false), (false, Some(40), true));
        assert_eq!(longer(true), (true, Some(30), false));
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
        assert_eq!(
            taken,
            landed(&[(0, 10, Provenance::Damaged), (20, 30, Provenance::Damaged)])
        );
        assert_eq!(
            runs(&namespace.get(member).unwrap().coverage),
            vec![
                (0, 10, Provenance::Damaged),
                (10, 20, Provenance::Repaired),
                (20, 30, Provenance::Damaged),
            ]
        );
        let (_, taken) = namespace.arrive(member, ByteRange::new(5, 25), Provenance::Fresh);
        assert_eq!(
            taken,
            landed(&[(5, 10, Provenance::Fresh), (20, 25, Provenance::Fresh)])
        );
    }

    #[test]
    fn a_repair_replaces_damaged_and_verified_bytes_alike() {
        let mut namespace = Namespace::new();
        let member = namespace.announce("ember.bin", Some(30));
        namespace.arrive(member, ByteRange::new(0, 10), Provenance::Fresh);
        namespace.arrive(member, ByteRange::new(10, 20), Provenance::Damaged);
        let (_, taken) = namespace.arrive(member, ByteRange::new(5, 15), Provenance::Repaired);
        assert_eq!(taken, landed(&[(5, 15, Provenance::Repaired)]));
        let (_, taken) = namespace.arrive(member, ByteRange::new(0, 30), Provenance::Repaired);
        assert_eq!(taken, landed(&[(0, 30, Provenance::Repaired)]));
        assert_eq!(
            runs(&namespace.get(member).unwrap().coverage),
            vec![(0, 30, Provenance::Repaired)]
        );
    }

    // A member and a further posting of it, folded: whichever posting brings
    // clean bytes first, they stand against the other, and each posting's
    // own bytes coming again are taken again.
    fn posted_twice() -> (Namespace, MemberId, MemberId) {
        let mut namespace = Namespace::new();
        let member = namespace.announce("ember.bin", Some(40));
        let copy = namespace.announce("ember.copy", Some(40));
        for posting in [member, copy] {
            namespace.keep_first_bytes(posting, 0, &[7; 40], Provenance::Fresh);
            namespace.arrive(posting, ByteRange::new(30, 40), Provenance::Fresh);
        }
        let before = namespace.fold(copy, member).expect("the fronts agree");
        assert_eq!(runs(&before), vec![(30, 40, Provenance::Fresh)]);
        assert!(
            namespace
                .get(copy)
                .unwrap()
                .coverage
                .runs()
                .next()
                .is_none()
        );
        (namespace, member, copy)
    }

    // A posting folded after bytes of it came while it was a member of its
    // own: they go down again as the member's, and where the member holds
    // them clean already the first copy stands, since the member never took
    // them from the copy.
    #[test]
    fn a_folded_postings_earlier_bytes_meet_the_first_copy() {
        let (mut namespace, member, copy) = posted_twice();
        let (_, taken) = namespace.arrive(copy, ByteRange::new(30, 40), Provenance::Fresh);
        assert!(taken.is_empty(), "{taken:?}");
        let (_, taken) = namespace.arrive(copy, ByteRange::new(0, 10), Provenance::Fresh);
        assert_eq!(taken, landed(&[(0, 10, Provenance::Fresh)]));
        assert_eq!(
            runs(&namespace.get(member).unwrap().coverage),
            vec![(0, 10, Provenance::Fresh), (30, 40, Provenance::Fresh)]
        );
    }

    #[test]
    fn the_first_clean_copy_stands_until_a_repair_or_damage_displaces_it() {
        let (mut namespace, member, copy) = posted_twice();
        namespace.arrive(member, ByteRange::new(0, 20), Provenance::Fresh);
        let (_, taken) = namespace.arrive(copy, ByteRange::new(10, 30), Provenance::Fresh);
        assert_eq!(taken, landed(&[(20, 30, Provenance::Fresh)]));
        let (_, taken) = namespace.arrive(copy, ByteRange::new(0, 10), Provenance::Fresh);
        assert!(taken.is_empty(), "{taken:?}");
        let (_, taken) = namespace.arrive(member, ByteRange::new(20, 30), Provenance::Fresh);
        assert!(taken.is_empty(), "the copy brought them first: {taken:?}");
        let (_, taken) = namespace.arrive(copy, ByteRange::new(20, 30), Provenance::Fresh);
        assert_eq!(
            taken,
            landed(&[(20, 30, Provenance::Fresh)]),
            "its own bytes again"
        );
        let (_, taken) = namespace.arrive(copy, ByteRange::new(0, 10), Provenance::Damaged);
        assert!(taken.is_empty(), "{taken:?}");
    }

    // A first copy that was wrong, then a recovery set's repair of it: the
    // repair takes the range, and a further posting's clean copy after it
    // takes nothing.
    #[test]
    fn a_recovery_proof_displaces_the_first_copy() {
        let (mut namespace, member, copy) = posted_twice();
        namespace.arrive(member, ByteRange::new(0, 30), Provenance::Fresh);
        let (_, taken) = namespace.arrive(member, ByteRange::new(5, 15), Provenance::Repaired);
        assert_eq!(taken, landed(&[(5, 15, Provenance::Repaired)]));
        let (_, taken) = namespace.arrive(copy, ByteRange::new(0, 30), Provenance::Fresh);
        assert!(taken.is_empty(), "{taken:?}");
        assert_eq!(
            runs(&namespace.get(member).unwrap().coverage),
            vec![
                (0, 5, Provenance::Fresh),
                (5, 15, Provenance::Repaired),
                (15, 40, Provenance::Fresh),
            ]
        );
    }

    // A first copy a checksum refuted is still the first copy: a further
    // posting's clean copy of those bytes is not written over it, and only
    // a repair displaces it.
    #[test]
    fn a_further_copy_does_not_displace_a_refuted_first_copy() {
        let mut namespace = Namespace::new();
        let member = namespace.announce("ember.bin", Some(100));
        let copy = namespace.announce("ember.copy", Some(100));
        for posting in [member, copy] {
            namespace.keep_first_bytes(posting, 0, &[7; 10], Provenance::Fresh);
            namespace.arrive(posting, ByteRange::new(0, 10), Provenance::Fresh);
        }
        assert!(namespace.fold(copy, member).is_some());
        namespace.arrive(member, ByteRange::new(10, 100), Provenance::Fresh);
        namespace.arrive(member, ByteRange::new(10, 90), Provenance::Refuted);
        let (_, taken) = namespace.arrive(copy, ByteRange::new(10, 90), Provenance::Fresh);
        assert!(
            taken.is_empty(),
            "the further copy was written over the refuted first copy: {taken:?}"
        );
        assert_eq!(
            runs(&namespace.get(member).unwrap().coverage),
            vec![
                (0, 10, Provenance::Fresh),
                (10, 90, Provenance::Refuted),
                (90, 100, Provenance::Fresh),
            ]
        );
        let (_, taken) = namespace.arrive(member, ByteRange::new(10, 90), Provenance::Repaired);
        assert_eq!(taken, landed(&[(10, 90, Provenance::Repaired)]));
        assert_eq!(
            runs(&namespace.get(member).unwrap().coverage),
            vec![
                (0, 10, Provenance::Fresh),
                (10, 90, Provenance::Repaired),
                (90, 100, Provenance::Fresh),
            ]
        );
    }

    // Bytes a checksum refuted: the same posting's bytes arriving again
    // land under the verdict, as do bytes it judged that come after it, and
    // only a repair displaces it.
    #[test]
    fn a_fresh_copy_of_its_own_posting_does_not_lift_a_checksum_verdict() {
        let mut namespace = Namespace::new();
        let member = namespace.announce("ember.bin", Some(100));
        namespace.arrive(member, ByteRange::new(0, 60), Provenance::Fresh);
        namespace.arrive(member, ByteRange::new(10, 90), Provenance::Refuted);
        let (_, taken) = namespace.arrive(member, ByteRange::new(60, 100), Provenance::Fresh);
        assert_eq!(
            taken,
            landed(&[(60, 90, Provenance::Refuted), (90, 100, Provenance::Fresh)]),
            "bytes the verdict was said of before they came"
        );
        let (_, taken) = namespace.arrive(member, ByteRange::new(0, 100), Provenance::Fresh);
        assert_eq!(
            taken,
            landed(&[
                (0, 10, Provenance::Fresh),
                (10, 90, Provenance::Refuted),
                (90, 100, Provenance::Fresh),
            ])
        );
        assert_eq!(
            runs(&namespace.get(member).unwrap().coverage),
            vec![
                (0, 10, Provenance::Fresh),
                (10, 90, Provenance::Refuted),
                (90, 100, Provenance::Fresh),
            ]
        );
        assert!(
            namespace
                .get(member)
                .unwrap()
                .coverage
                .damaged(ByteRange::new(10, 90))
        );
        let (_, taken) = namespace.arrive(member, ByteRange::new(10, 90), Provenance::Repaired);
        assert_eq!(taken, landed(&[(10, 90, Provenance::Repaired)]));
        assert_eq!(
            runs(&namespace.get(member).unwrap().coverage),
            vec![
                (0, 10, Provenance::Fresh),
                (10, 90, Provenance::Repaired),
                (90, 100, Provenance::Fresh),
            ]
        );
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
