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

use serde::{Deserialize, Serialize};
use weaver_graph::{ByteRange, Emit, Evidenced, MemberId, Placement, Provenance};

use super::Zip;

// What reads an owner's placements.
type Read<'a> = &'a mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool;

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
    // held behind another, are said once what is held is.
    pub(super) fn hold(
        &mut self,
        member: MemberId,
        offset: u64,
        bytes: &Evidenced,
        provenance: Provenance,
    ) -> bool {
        let len = bytes.len() as u64;
        let Some(entry) = self.map.as_ref().and_then(|map| map.entry(member)) else {
            return false;
        };
        let (Some(data), Some(cipher)) = (entry.data, &entry.cipher) else {
            return false;
        };
        let Some(open) = &cipher.open else {
            return false;
        };
        if len == 0 || !open.checked || cipher.retry || !cipher.pending.is_empty() {
            return false;
        }
        let reaches = self
            .held
            .iter()
            .rev()
            .find(|held| held.member == member.0)
            .map_or(open.reaches(), |held| held.offset + held.bytes.len() as u64);
        if offset != reaches
            || self.members[member.0 as usize]
                .coverage
                .overlapping(ByteRange::at(offset, len))
                .next()
                .is_some()
            || !self.kept.from(data + open.reaches()).is_empty()
        {
            return false;
        }
        self.held.push(Held {
            member: member.0,
            offset,
            bytes: bytes.to_vec(),
            provenance,
        });
        true
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
    // read, and what it holds waits for one.
    pub(super) fn run_held(&mut self, mut read: Option<Read<'_>>, out: &mut Vec<Emit>) {
        let mut away = |_: &Placement, _: &mut Vec<u8>| false;
        let mut waiting = std::collections::BTreeSet::new();
        let mut kept = Vec::new();
        for held in std::mem::take(&mut self.held) {
            let member = MemberId(held.member);
            if read.is_none()
                && (waiting.contains(&held.member) || !self.ready(member, held.offset))
            {
                waiting.insert(held.member);
                kept.push(held);
                continue;
            }
            let read: Read<'_> = match &mut read {
                Some(read) => &mut **read,
                None => &mut away,
            };
            self.decrypt_record(
                member,
                held.offset,
                Evidenced::from(held.bytes),
                held.provenance,
                read,
                out,
            );
        }
        self.held = kept;
    }
}
