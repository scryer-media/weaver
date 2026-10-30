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
// whose last member it continues. A number every set states alike joins a
// set only where a held volume beside it judges it, or where the volume's
// name is not another set's, and no set takes a volume past the one that
// says no volume follows it. The
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
use crate::quiet::Placed;
use crate::rar_engine::naming;

// A volume of the set already held, as far as it places the next: what its
// headers state, once they are read, and whether a volume follows it, once
// that is known.
#[derive(Clone, Copy)]
pub(crate) struct Held<'a> {
    pub(crate) facts: Option<&'a RarVolumeFacts>,
    pub(crate) successor: Option<bool>,
}

// The volumes held, by index, as far as they place the next: looked up
// where they are kept, never gathered for one claim.
pub(crate) trait Holds {
    fn at(&self, index: u32) -> Option<Held<'_>>;
    fn indices(&self) -> impl Iterator<Item = u32> + '_;
}

impl Holds for BTreeMap<u32, Held<'_>> {
    fn at(&self, index: u32) -> Option<Held<'_>> {
        self.get(&index).copied()
    }

    fn indices(&self) -> impl Iterator<Item = u32> + '_ {
        self.keys().copied()
    }
}

// The store's own volumes, as they place the next: what each one's walk
// read, or until it has read its main header, what its front stated when it
// was bound: a walk short of the main header states nothing, as one that
// read nothing does. A volume not bound yet, or held where its own headers
// do not place it, places nothing beside it.
struct Bound<'a>(&'a BTreeMap<VolumeIndex, Volume>);

fn held(index: VolumeIndex, volume: &Volume) -> Option<Held<'_>> {
    volume.name.as_ref()?;
    let facts = volume.stated();
    (!facts.is_some_and(|facts| misplaced(index, facts))).then_some(Held {
        facts,
        successor: volume.walk.successor(),
    })
}

impl Holds for Bound<'_> {
    fn at(&self, index: u32) -> Option<Held<'_>> {
        let index = VolumeIndex(index);
        self.0.get(&index).and_then(|volume| held(index, volume))
    }

    fn indices(&self) -> impl Iterator<Item = u32> + '_ {
        self.0
            .iter()
            .filter(|(index, volume)| held(**index, volume).is_some())
            .map(|(index, _)| index.0)
    }
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
                .first_member()
                .is_some_and(|member| !member.split_before)
                .then_some(0),
        }
    }

    // The member the headers list first, and last, in their own order.
    fn first_member(&self) -> Option<&RarVolumeMemberFacts> {
        self.facts.members.iter().min_by_key(|member| member.order)
    }

    fn last_member(&self) -> Option<&RarVolumeMemberFacts> {
        self.facts.members.iter().max_by_key(|member| member.order)
    }

    // Whether the walk read no file header of this volume yet, only the
    // main header that states its number: its members say nothing yet,
    // and the number is all that places it.
    fn unread(&self) -> bool {
        self.facts.members.is_empty() && self.position().is_some()
    }

    // The member this volume opens on, when it continues one begun earlier.
    fn continued_member(&self) -> Option<&RarVolumeMemberFacts> {
        self.first_member().filter(|member| member.split_before)
    }

    // The member this volume ends on, when it carries on into the next one.
    fn continuing_member(&self) -> Option<&RarVolumeMemberFacts> {
        self.last_member().filter(|member| member.split_after)
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
            // A volume whose file headers are not read yet links on its
            // stated number alone, as a boundary between members does.
            _ => (previous.unread() || self.unread()) && self.position() == Some(index),
        }
    }

    // Whether `next` can come straight after this volume by what this one's
    // headers say of its end: a member this volume carries on is the one
    // `next` opens on. A front that ends on no carried member may stop
    // short of its last header, so it says nothing against `next`, nor
    // does a `next` whose file headers are not read yet.
    fn runs_into(&self, next: &Nameless<'_>) -> bool {
        self.continuing_member().is_none_or(|tail| {
            next.unread()
                || next
                    .continued_member()
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
    pub(super) fn claim_volume(&self, name: &str, first_bytes: &[u8]) -> Option<Placed> {
        let format = unrar_rs::signature::read_signature(&mut Cursor::new(first_bytes)).ok()?;
        self.claim_as(name, format, self.front_facts(first_bytes).as_ref())
    }

    // A transferable volume is judged by its parsed headers, or by the
    // retained front and name while those headers say nothing. Both are
    // the same evidence an unowned member offers, without reading it again.
    pub(super) fn reclaim_volume(
        &self,
        name: &str,
        front: &[u8],
        given: &Volume,
    ) -> Option<Placed> {
        self.reclaim_front(name, front, given.stated())
    }

    pub(super) fn reclaim_front(
        &self,
        name: &str,
        front: &[u8],
        facts: Option<&RarVolumeFacts>,
    ) -> Option<Placed> {
        match facts {
            Some(facts) if facts.is_volume => {
                self.claim_as(name, facts.archive_format(), Some(facts))
            }
            Some(_) => None,
            None => self.claim_volume(name, front),
        }
    }

    // One past every volume the set holds.
    pub(super) fn past(&self) -> Option<VolumeIndex> {
        Placed::past(self.volumes.keys().next_back().map(|volume| volume.0))
    }

    fn claim_as(
        &self,
        name: &str,
        format: ArchiveFormat,
        facts: Option<&RarVolumeFacts>,
    ) -> Option<Placed> {
        if self.holds_other_than(format) {
            return None;
        }
        // The set's numbering is the one the bound names share; before any
        // is bound, the one this member's own name gives. Each bound name
        // was read once, when it was bound.
        let wanted = naming::parse(name);
        let mut numbered = self
            .volumes
            .values()
            .filter_map(|volume| volume.numbered.as_ref())
            .peekable();
        let bound = numbered.peek().is_some();
        let ours = wanted
            .as_ref()
            .map(|wanted| numbered.any(|bound| bound.same_set(wanted)));
        let named = wanted.is_some() && (!bound || ours == Some(true));
        // The name gives the volume to another set: it and the bound names
        // number sets, and not the same one.
        let elsewhere = ours == Some(false) && bound;
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
        // A number is one every set states alike, so a position the headers
        // state is the volume's only where a held volume beside it judged
        // it, or where nothing gives the volume to another set.
        let (headers, judged) = facts.map_or((None, false), |facts| self.claim_by_headers(facts));
        let headers = headers.filter(|_| judged || !elsewhere);
        // A new numbering can refine an established header link, but an
        // unrelated name cannot invent membership beside opaque labels.
        let named = named.filter(|_| bound || self.volumes.is_empty() || headers.is_some());
        // A position held already is answered all the same: the volume
        // posted again is that volume.
        let placed = match (headers, named) {
            (Some(placed), _) if stated => Some(placed),
            (_, Some(named)) => Some(Placed::At(named)),
            (linked, None) => linked,
        };
        // A set ends at the volume that says no volume follows it.
        placed.filter(|placed| match placed {
            Placed::At(at) => self.last().is_none_or(|last| *at <= last),
            Placed::Among(_) => true,
        })
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

    // The position this volume's headers give it beside the volumes held,
    // and whether a held volume beside it judged that position.
    fn claim_by_headers(&self, facts: &RarVolumeFacts) -> (Option<Placed>, bool) {
        let held = Bound(&self.volumes);
        (place(facts, &held), judged(facts, &held))
    }
}

// Whether the held volumes judged where a volume's headers place it: the
// position they state carries the same member identity, or a known member
// continues between this volume and a neighbor. The ordinal alone is shared
// by every set and cannot override a name belonging to another set. Whether
// a repeated volume is byte-identical remains the tree's comparison.
pub(crate) fn judged(facts: &RarVolumeFacts, held: &impl Holds) -> bool {
    let this = Nameless { facts };
    this.position().is_none_or(|stated| {
        let same =
            held.at(stated)
                .and_then(|held| held.facts)
                .is_some_and(|facts| {
                    let other = Nameless { facts };
                    this.kin(&other)
                        && this.first_member().zip(other.first_member()).is_some_and(
                            |(one, two)| {
                                continues(one, two)
                                    && one.split_before == two.split_before
                                    && one.split_after == two.split_after
                            },
                        )
                });
        let before = stated
            .checked_sub(1)
            .and_then(|at| held.at(at))
            .and_then(|held| held.facts)
            .is_some_and(|facts| {
                let previous = Nameless { facts };
                this.kin(&previous)
                    && previous
                        .continuing_member()
                        .zip(this.continued_member())
                        .is_some_and(|(tail, head)| continues(tail, head))
            });
        let after = stated
            .checked_add(1)
            .and_then(|at| held.at(at))
            .and_then(|held| held.facts)
            .is_some_and(|facts| {
                let next = Nameless { facts };
                this.kin(&next)
                    && this
                        .continuing_member()
                        .zip(next.continued_member())
                        .is_some_and(|(tail, head)| continues(tail, head))
            });
        same || before || after
    })
}

// Whether a volume's headers state its position: a number, or the first
// volume's mark. One that states none is placed by its name in the set's
// numbering before it is placed by the members that link it to its
// neighbours.
pub(crate) fn states_position(facts: &RarVolumeFacts) -> bool {
    Nameless { facts }.position().is_some()
}

// Whether a held volume is at `index` by more than the members linking it
// to its neighbours: its headers state its position, its name gives it in
// the set's numbering, or its walk has read its headers to their end, where
// a stated number would show. Links alone never fix a position, since one
// member spanning every volume links each free place beside the held ones,
// so a volume they placed is mapped only once its own end is read.
// A holder at `index` yields its place to a claimant whose headers state
// a position, the claim that named the place, when links alone hold it
// there. A claimant that only links never moves it.
pub(super) fn yields(
    index: VolumeIndex,
    holder: &Volume,
    claimant: Option<&RarVolumeFacts>,
) -> bool {
    !settled(index, holder) && claimant.is_some_and(states_position)
}

// Whether a volume is settled at `index`: read to its end, or placed there
// by a stated position or by its name. Nothing settles a volume where its
// own headers say it is not, a name least of all.
pub(super) fn settled(index: VolumeIndex, volume: &Volume) -> bool {
    let facts = volume.stated();
    volume.walk.ended
        || (!facts.is_some_and(|facts| misplaced(index, facts))
            && (facts.is_some_and(states_position)
                || volume
                    .numbered
                    .as_ref()
                    .is_some_and(|name| name.index() == index.0)))
}

// The position a volume's headers give it beside the volumes held, by
// index. One that states its number is at that number, when the held
// volumes beside it agree: the one before carries on into it, the one after
// is of the same kind, states the next number and opens on the member it
// carries on, so a volume never joins a set it does not continue. One that
// states none, a nameless RAR4 front, goes in the one free place beside the
// held volumes where both its neighbours link to it: after the one whose
// last member it continues, before the one that continues its last member.
// Where more than one free place takes it, as when one member spans every
// volume and links each to all the others, its front orders it nowhere: it
// takes the lowest, held there by links alone until its walk reads its end,
// where a RAR4 volume states its number. Where no free place takes it, it
// is the one held place its neighbours link it to, posted again; whether it
// is that volume its bytes say, which the tree compares.
pub(crate) fn place(facts: &RarVolumeFacts, held: &impl Holds) -> Option<Placed> {
    let this = Nameless { facts };
    let joins = |previous: &Held<'_>, index: u32| {
        previous.successor != Some(false)
            && previous
                .facts
                .is_some_and(|facts| this.follows(&Nameless { facts }, index))
    };
    // A stated position is judged by what the neighbours' headers say, and
    // a neighbour none of whose headers were read yet says nothing against
    // it, on either side. A position with no place after it is none a set
    // has.
    if let Some(stated) = this.position() {
        let following = stated.checked_add(1)?;
        let before = stated
            .checked_sub(1)
            .and_then(|index| held.at(index))
            .is_none_or(|previous| previous.facts.is_none() || joins(&previous, stated));
        let after = held.at(following).is_none_or(|next| {
            next.facts.is_none_or(|facts| {
                let next = Nameless { facts };
                next.kin(&this)
                    && next.position().is_none_or(|next| next == following)
                    && this.runs_into(&next)
            })
        });
        return (before && after).then_some(Placed::At(VolumeIndex(stated)));
    }
    let precedes = |next: &Held<'_>, index: u32| {
        next.facts
            .is_some_and(|facts| Nameless { facts }.follows(&this, index))
    };
    // A place the volume's own headers rule out is no place for it, however
    // its neighbours link to it: a volume that opens on a continued member
    // is never its set's first.
    let fits = |slot: u32| {
        let before = slot.checked_sub(1).and_then(|index| held.at(index));
        let after = slot
            .checked_add(1)
            .and_then(|index| held.at(index).map(|next| (next, index)));
        !misplaced(VolumeIndex(slot), facts)
            && (before.is_some() || after.is_some())
            && before.is_none_or(|previous| joins(&previous, slot))
            && after.is_none_or(|(next, index)| precedes(&next, index))
    };
    let lowest = held
        .indices()
        .flat_map(|index| [index.checked_sub(1), index.checked_add(1)])
        .flatten()
        .filter(|slot| held.at(*slot).is_none() && fits(*slot))
        .min();
    if let Some(lowest) = lowest {
        return Some(Placed::At(VolumeIndex(lowest)));
    }
    let mut again = held.indices().filter(|slot| fits(*slot));
    match (again.next(), again.next()) {
        (Some(only), None) => Some(Placed::At(VolumeIndex(only))),
        _ => None,
    }
}
