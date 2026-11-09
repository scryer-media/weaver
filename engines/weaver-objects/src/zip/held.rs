// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Ciphertext held for its keys to run over away from the loop. A part of a
// member whose keys are open and proved, that starts where they reach, or
// where the part held before it ends, needs nothing read to be deciphered:
// it waits here, and the owner lends the object out to run the keys over it
// on the post-processing pool. Each part is deciphered as it would have been
// on arrival, in the order it came. Whatever else reaches the object runs
// what is held first, so nothing is heard out of order. The owner takes no
// checkpoint while the object is out, so held ciphertext reaches the facts
// only when a checkpoint finds it in the tree; keys are not durable, and a
// part a restart left with no proved keys waits for a delivery, whose
// placements can be read.
//
// What is held here is bounded: past `CAP` a part is parked, neither held
// nor deciphered. The object places it as held, so its owner keeps it in
// its counted holds, which go to disk past their budget, and hands it back
// once the keys run over what is held here and reach it. A part that
// starts where what is held ends is cut at the cap: what fits is held and
// the rest parks. One that starts past it, behind a gap, parks whole where
// the gap and it would pass the cap.

use serde::{Deserialize, Serialize};
use weaver_graph::{ByteRange, Emit, Evidenced, MemberId, Placement, Provenance};

use super::Zip;

// What reads an owner's placements.
type Read<'a> = &'a mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool;

// The most ciphertext held for the keys at once, across the object's
// members: 64 MiB. A job with no slot to run them in holds no more than
// this here; the rest waits with its owner, counted.
pub(super) const CAP: u64 = 64 * 1024 * 1024;

fn cap() -> u64 {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(cap) = super::counting::cap() {
        return cap;
    }
    CAP
}

// What became of a part offered to be held.
pub(super) enum Hold {
    Held,
    // Past the cap: its owner holds it, as the object places it.
    Parked,
    // Held up to the cap, this many bytes; the rest parks.
    Cut(usize),
    // It cannot wait behind what is held.
    Declined,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Held {
    pub(super) member: u32,
    pub(super) offset: u64,
    bytes: Vec<u8>,
    provenance: Provenance,
}

impl Zip {
    // Hold the part of `member` at `offset` when its keys can run over it
    // with nothing read: they are open and proved, no replay waits, nothing
    // of it was said, and it starts where the keys, or the member's last
    // held part, reach. Kept bytes where the keys reach are said from what
    // was kept, ahead of anything held; kept bytes past it, under a part
    // held behind another, are said once what is held is. A part that
    // parks is neither held nor said.
    pub(super) fn hold(
        &mut self,
        member: MemberId,
        offset: u64,
        bytes: &Evidenced,
        provenance: Provenance,
    ) -> Hold {
        let part = ByteRange::at(offset, bytes.len() as u64);
        let Some(reaches) = self.reaches_held(member, part) else {
            return Hold::Declined;
        };
        // Room left under the cap, where the part would start.
        let room = cap().saturating_sub(self.queued());
        if offset != reaches {
            return match self.parks_after(reaches, part) {
                true => Hold::Parked,
                false => Hold::Declined,
            };
        }
        if room == 0 {
            return Hold::Parked;
        }
        let taken = room.min(part.len()) as usize;
        self.held.push(Held {
            member: member.0,
            offset,
            bytes: bytes[..taken].to_vec(),
            provenance,
        });
        #[cfg(any(test, feature = "test-support"))]
        super::counting::queued(self.queued());
        match taken < bytes.len() {
            true => Hold::Cut(taken),
            false => Hold::Held,
        }
    }

    // Whether `part` of `member` parks: it waits behind what is held, and
    // what is held, with everything from where it ends through the part,
    // would pass the cap. Its owner holds it; the object places it so.
    pub(super) fn parks(&self, member: MemberId, part: ByteRange) -> bool {
        self.reaches_held(member, part)
            .is_some_and(|reaches| self.parks_after(reaches, part))
    }

    // A part at the end of what is held parks only where the cap is
    // reached already; one past it parks where the cap would be passed by
    // the time the keys reached its end.
    // Where in `range` of `member` what parks starts: at or past the end
    // of what is held for the keys.
    pub(super) fn parks_from(&self, member: MemberId, range: ByteRange) -> Option<u64> {
        let end = self.held_end(member)?;
        let at = range.start.max(end);
        (at < range.end && self.parks(member, ByteRange::new(at, range.end))).then_some(at)
    }

    fn parks_after(&self, reaches: u64, part: ByteRange) -> bool {
        let queued = self.queued();
        match part.start == reaches {
            true => queued >= cap(),
            false => queued > 0 && queued.saturating_add(part.end - reaches) > cap(),
        }
    }

    // Where the keys, or what `member` holds, reach, when a part of it at
    // or past there could wait behind what is held: its keys are open and
    // proved, no replay waits, nothing of it was said, and nothing is kept
    // where the keys reach.
    fn reaches_held(&self, member: MemberId, part: ByteRange) -> Option<u64> {
        let reaches = self.held_end(member)?;
        let said = self.members[member.0 as usize]
            .coverage
            .overlapping(part)
            .next()
            .is_some();
        (!part.is_empty() && part.start >= reaches && !said).then_some(reaches)
    }

    // Where the keys, or what `member` holds, reach, when anything can
    // wait behind what is held: its keys are open and proved, no replay
    // waits, and nothing is kept where the keys reach.
    fn held_end(&self, member: MemberId) -> Option<u64> {
        let entry = self.map.as_ref()?.entry(member)?;
        let (data, cipher) = (entry.data?, entry.cipher.as_ref()?);
        let open = cipher.open.as_ref()?;
        if !open.checked || cipher.retry || !cipher.pending.is_empty() {
            return None;
        }
        if !self.kept.from(data + open.reaches()).is_empty() {
            return None;
        }
        Some(
            self.held
                .iter()
                .rev()
                .find(|held| held.member == member.0)
                .map_or(open.reaches(), |held| held.offset + held.bytes.len() as u64),
        )
    }

    // The ciphertext held, across every member.
    pub(super) fn queued(&self) -> u64 {
        self.held.iter().map(|held| held.bytes.len() as u64).sum()
    }

    // Whether each part held is of a member the map gives, inside its
    // entry.
    pub(super) fn held_sound(&self) -> bool {
        self.held.iter().all(|held| {
            self.map
                .as_ref()
                .and_then(|map| map.entry(MemberId(held.member)))
                .is_some_and(|entry| {
                    held.offset
                        .checked_add(held.bytes.len() as u64)
                        .is_some_and(|end| end <= entry.length)
                })
        })
    }

    // Where what `member` holds for its keys ends, while it holds any.
    pub(super) fn queued_end(&self, member: MemberId) -> Option<u64> {
        self.holding(member)
            .then(|| self.held_end(member))
            .flatten()
    }

    pub(super) fn holding(&self, member: MemberId) -> bool {
        self.held.iter().any(|held| held.member == member.0)
    }

    pub(super) fn holds_any(&self) -> bool {
        !self.held.is_empty()
    }

    // Before anything else reaches the object, what it holds is run.
    pub(super) fn run_held_first(&mut self, out: &mut Vec<Emit>) {
        if self.holds_any() {
            self.run_held(None, out);
        }
    }

    // Whether some member's keys are proved and reach the first part it
    // holds: that part, and what the member holds after it, can be run with
    // nothing read.
    pub(super) fn held_ready(&self) -> bool {
        let mut seen = std::collections::BTreeSet::new();
        self.held
            .iter()
            .filter(|held| seen.insert(held.member))
            .any(|held| self.ready(MemberId(held.member), held.offset))
    }

    fn ready(&self, member: MemberId, offset: u64) -> bool {
        self.map
            .as_ref()
            .and_then(|map| map.entry(member))
            .and_then(|entry| entry.cipher.as_ref())
            .and_then(|cipher| cipher.open.as_ref())
            .is_some_and(|open| open.checked && open.reaches() == offset)
    }

    // Decipher everything held, in the order it came, through `read` where
    // the owner's placements can be read, or with nothing read away from
    // the tree. Away from the tree a member's parts are deciphered only
    // where its keys are proved and reach them, as they did when they were
    // held; a member a restart or a forgotten range left otherwise needs a
    // read, and what it holds waits for one. A part nothing read reaches
    // yet stays, with every part of its member after it, for the keys to
    // come back to it: nothing held is lost.
    pub(super) fn run_held(&mut self, mut read: Option<Read<'_>>, out: &mut Vec<Emit>) {
        let mut away = |_: &Placement, _: &mut Vec<u8>| false;
        let mut waiting = std::collections::BTreeSet::new();
        let mut kept = Vec::new();
        for held in std::mem::take(&mut self.held) {
            let member = MemberId(held.member);
            if waiting.contains(&held.member)
                || (read.is_none() && !self.ready(member, held.offset))
            {
                waiting.insert(held.member);
                kept.push(held);
                continue;
            }
            let read: Read<'_> = match &mut read {
                Some(read) => &mut **read,
                None => &mut away,
            };
            let Held {
                offset,
                bytes,
                provenance,
                ..
            } = held;
            let bytes = Evidenced::from(bytes);
            if !self.decrypt_record(member, offset, bytes.clone(), provenance, read, out) {
                waiting.insert(member.0);
                kept.push(Held {
                    member: member.0,
                    offset,
                    bytes: bytes.to_vec(),
                    provenance,
                });
            }
        }
        self.held = kept;
    }
}
