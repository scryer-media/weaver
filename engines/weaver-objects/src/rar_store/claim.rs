// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which sibling members are this set's volumes, and where in the set each
// one is: the position the set gives it, never the order it arrived in.
// Every volume of a RAR set starts with the signature, so a member without
// one is never claimed. A volume is placed by what states its position: the
// position its own headers give, else its name in the set's numbering, as in
// a set numbered by its names alone. A posted name is a label a recovery set
// may change later, so the headers outrank it, and a name they contradict
// places nothing. A volume that neither places goes after the held volume
// whose last member it continues. The
// store's first volume is placed the same way, with nothing held yet, and a
// front that places it nowhere founds the store with it first. That claim is
// provisional: a volume whose own headers, read to its close, say it is not
// where the store holds it is given back, with what was read of it, and the
// volumes beside it claim it again by that parse alone. The
// RAR engine a store gives its set to places the rest of it by the same
// headers.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use unrar_rs::{ArchiveFormat, KdfCache, RarArchive, RarVolumeFacts, RarVolumeMemberFacts};
use weaver_graph::VolumeIndex;

use super::{RarStore, Volume};
use crate::engine::SetName;
use crate::rar_engine::naming;

// A volume of the set already held, as far as it places the next: what its
// headers state, once they are read, and whether a volume follows it, once
// that is known.
pub(crate) struct Held<'a> {
    pub(crate) facts: Option<&'a RarVolumeFacts>,
    pub(crate) successor: Option<bool>,
}

// What a member's front states that places it in a set: its headers, when
// they say it is a volume of one.
pub(crate) fn front_facts(
    first_bytes: &[u8],
    password: Option<&str>,
    kdf: Arc<KdfCache>,
) -> Option<RarVolumeFacts> {
    let facts = RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
        Cursor::new(first_bytes.to_vec()),
        password,
        kdf,
    )
    .ok()?
    .facts;
    facts.is_volume.then_some(facts)
}

// The most a volume given back may carry of what was read of it: the ceiling
// a walk shows of a front before it is no RAR volume. A crafted volume whose
// headers run past it is refused rather than carried.
pub(super) const CARRIED: u64 = 4 * 1024 * 1024;

// Whether a volume's headers say it is not at `index`: they state another
// position, or it opens on a member begun before it, so it is no set's
// first.
pub(crate) fn misplaced(index: VolumeIndex, facts: &RarVolumeFacts) -> bool {
    let this = Nameless { facts };
    this.position().is_some_and(|stated| stated != index.0)
        || (index.0 == 0 && this.continued_member().is_some())
}

// One volume's headers, as far as they place it in its set.
struct Nameless<'a> {
    facts: &'a RarVolumeFacts,
}

impl Nameless<'_> {
    // The position the headers state. A RAR5 volume numbers itself in its
    // main header, all but the first, which states no number; a RAR4
    // volume numbers itself only in its end record, and short of one is
    // its set's first when it opens on a member it does not continue, as
    // RAR reads it.
    fn position(&self) -> Option<u32> {
        match (self.facts.volume_number, self.facts.archive_format()) {
            (Some(stated), _) => Some(stated),
            (None, ArchiveFormat::Rar5) => self.facts.is_volume.then_some(0),
            (None, _) => self
                .ordered_members()
                .first()
                .is_some_and(|member| !member.split_before)
                .then_some(0),
        }
    }

    fn ordered_members(&self) -> Vec<&RarVolumeMemberFacts> {
        let mut members: Vec<_> = self.facts.members.iter().collect();
        members.sort_by_key(|member| member.order);
        members
    }

    // The member this volume opens on, when it continues one begun earlier.
    fn continued_member(&self) -> Option<&RarVolumeMemberFacts> {
        self.ordered_members()
            .first()
            .copied()
            .filter(|member| member.split_before)
    }

    // The member this volume ends on, when it carries on into the next one.
    fn continuing_member(&self) -> Option<&RarVolumeMemberFacts> {
        self.ordered_members()
            .last()
            .copied()
            .filter(|member| member.split_after)
    }

    // Whether this volume can follow `previous` at index `index`, judged only
    // by what both volumes' headers state.
    // Whether both volumes are of one kind of set: the same format, and
    // both enciphered or neither.
    fn kin(&self, other: &Nameless<'_>) -> bool {
        self.facts.format == other.facts.format
            && self.facts.is_encrypted == other.facts.is_encrypted
    }

    fn follows(&self, previous: &Nameless<'_>, index: u32) -> bool {
        if !self.kin(previous) || self.position().is_some_and(|stated| stated != index) {
            return false;
        }
        match (previous.continuing_member(), self.continued_member()) {
            (Some(tail), Some(head)) => continues(tail, head),
            // A boundary that falls between two members links only on a
            // stated number; nothing else in the headers ties the volumes.
            (None, None) => self.position() == Some(index),
            _ => false,
        }
    }

    // Whether `next` can come straight after this volume by what this one's
    // headers say of its end: a member this volume carries on is the one
    // `next` opens on. A front that ends on no carried member may stop
    // short of its last header, so it says nothing against `next`.
    fn runs_into(&self, next: &Nameless<'_>) -> bool {
        self.continuing_member().is_none_or(|tail| {
            next.continued_member()
                .is_some_and(|head| continues(tail, head))
        })
    }
}

// Whether `head`, the member a volume opens on, is `tail`, the member the
// volume before it carries on.
fn continues(tail: &RarVolumeMemberFacts, head: &RarVolumeMemberFacts) -> bool {
    head.name == tail.name
        && match (head.unpacked_size, tail.unpacked_size) {
            (Some(head_size), Some(tail_size)) => head_size == tail_size,
            _ => true,
        }
}

impl RarStore {
    pub(super) fn claim_volume(&self, name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        let format = unrar_rs::signature::read_signature(&mut Cursor::new(first_bytes)).ok()?;
        self.claim_as(name, format, self.front_facts(first_bytes).as_ref())
    }

    // A volume a store beside this one gave back, placed by the headers that
    // store read of it: nothing of the member is read again.
    pub(super) fn reclaim_volume(&self, name: &str, given: &Volume) -> Option<VolumeIndex> {
        let facts = given
            .walk
            .facts
            .as_ref()
            .or(given.front.as_ref())
            .filter(|facts| facts.is_volume)?;
        self.claim_as(name, facts.archive_format(), Some(facts))
    }

    fn claim_as(
        &self,
        name: &str,
        format: ArchiveFormat,
        facts: Option<&RarVolumeFacts>,
    ) -> Option<VolumeIndex> {
        if self.volumes.values().any(|ours| {
            ours.walk.in_set() == Some(false)
                || ours
                    .walk
                    .facts
                    .as_ref()
                    .is_some_and(|facts| facts.archive_format() != format)
        }) {
            return None;
        }
        // The set's numbering is the one the bound names share; before any
        // is bound, the one this member's own name gives.
        let wanted = naming::parse(name);
        let unnamed = self.volumes.values().all(|volume| volume.name.is_none());
        let named = wanted.as_ref().is_some_and(|wanted| {
            unnamed
                || self
                    .volumes
                    .values()
                    .filter_map(|volume| volume.name.as_deref().and_then(naming::parse))
                    .any(|bound| bound.same_set(wanted))
        });
        // A position is what states it: the position the headers give,
        // else the name in the set's numbering. Only a volume that neither
        // places is placed by the members that link it to those held, which
        // never fix a position on their own: one member may span every
        // volume, and then it links each to all the others. Nothing places
        // a volume where its own headers say it is not, a name least of all.
        let stated = facts.is_some_and(states_position);
        let named = wanted
            .filter(|_| named)
            .map(|wanted| VolumeIndex(wanted.index()))
            .filter(|index| !facts.is_some_and(|facts| misplaced(*index, facts)));
        let index = match (facts.and_then(|facts| self.claim_by_headers(facts)), named) {
            (Some(index), _) if stated => index,
            (_, Some(named)) => named,
            (linked, None) => linked?,
        };
        let taken = self
            .volumes
            .get(&index)
            .is_some_and(|volume| volume.name.is_some());
        (!taken).then_some(index)
    }

    // What a front states, read with the store's own keys.
    pub(super) fn front_facts(&self, first_bytes: &[u8]) -> Option<RarVolumeFacts> {
        let password = self.header_password();
        front_facts(
            first_bytes,
            password.as_deref().map(String::as_str),
            Arc::clone(self.keys.kdf()),
        )
    }

    // The position this volume's headers give it beside the volumes held:
    // what each held volume's walk read, or until it has read anything,
    // what its front stated when it was bound. A volume held where its own
    // headers do not place it places nothing beside it.
    fn claim_by_headers(&self, facts: &RarVolumeFacts) -> Option<VolumeIndex> {
        let held = self
            .volumes
            .iter()
            .filter(|(_, volume)| volume.name.is_some())
            .filter_map(|(index, volume)| {
                let facts = volume.walk.facts.as_ref().or(volume.front.as_ref());
                let held = Held {
                    facts,
                    successor: volume.walk.successor(),
                };
                (!facts.is_some_and(|facts| misplaced(*index, facts))).then_some((index.0, held))
            })
            .collect();
        place(facts, &held).map(VolumeIndex)
    }
}

// Whether a volume's headers state its position: a number, or the first
// volume's mark. One that states none is placed by its name in the set's
// numbering before it is placed by the members that link it to its
// neighbours.
pub(crate) fn states_position(facts: &RarVolumeFacts) -> bool {
    Nameless { facts }.position().is_some()
}

// The position a volume's headers give it beside the volumes held, by
// index. One that states its number is at that number, when the held
// volumes beside it agree: the one before carries on into it, the one after
// is of the same kind, states the next number and opens on the member it
// carries on, so a volume never joins a set it does not continue. One that
// states none, a nameless RAR4 front, goes in the one free place beside the
// held volumes where both its neighbours link to it: after the one whose
// last member it continues, before the one that continues its last member.
pub(crate) fn place(facts: &RarVolumeFacts, held: &BTreeMap<u32, Held<'_>>) -> Option<u32> {
    let this = Nameless { facts };
    let joins = |previous: &Held<'_>, index: u32| {
        previous.successor != Some(false)
            && previous
                .facts
                .is_some_and(|facts| this.follows(&Nameless { facts }, index))
    };
    if let Some(stated) = this.position() {
        let before = stated
            .checked_sub(1)
            .and_then(|index| held.get(&index))
            .is_none_or(|previous| joins(previous, stated));
        let after = held.get(&(stated + 1)).is_none_or(|next| {
            next.facts.is_none_or(|facts| {
                let next = Nameless { facts };
                next.kin(&this)
                    && next.position().is_none_or(|next| next == stated + 1)
                    && this.runs_into(&next)
            })
        });
        return (before && after).then_some(stated);
    }
    let precedes = |next: &Held<'_>, index: u32| {
        next.facts
            .is_some_and(|facts| Nameless { facts }.follows(&this, index))
    };
    let fits = |slot: u32| {
        let before = slot.checked_sub(1).and_then(|index| held.get(&index));
        let after = held.get(&(slot + 1));
        (before.is_some() || after.is_some())
            && before.is_none_or(|previous| joins(previous, slot))
            && after.is_none_or(|next| precedes(next, slot + 1))
    };
    let mut candidates: Vec<u32> = held
        .keys()
        .flat_map(|index| [index.checked_sub(1), Some(index + 1)])
        .flatten()
        .filter(|slot| !held.contains_key(slot) && fits(*slot))
        .collect();
    candidates.sort_unstable();
    candidates.dedup();
    match candidates.as_slice() {
        [only] => Some(*only),
        _ => None,
    }
}
