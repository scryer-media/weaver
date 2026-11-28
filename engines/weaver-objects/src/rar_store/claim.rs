// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which sibling members are this set's volumes, and where in the set each
// one is: the position the set gives it, never the order it arrived in.
// Every volume of a RAR set starts with the signature, so a member whose
// front lacks one is never claimed; one whose front will never come is
// claimed by its name alone. A volume is placed by what states its position: the
// position its own headers give, else its name in the set's numbering, as in
// a set numbered by its names alone. A posted name is a label a recovery set
// may change later, so the headers outrank it, and a name they contradict
// places nothing. A volume that neither places goes after the held volume
// whose last member it continues. A number every set states alike joins a
// set only where a held volume beside it judges it, or where the volume's
// name is not another set's, and no set takes a volume past the one that
// says no volume follows it. An archive whose headers say it is no
// volume is a set of one, whatever its name numbers: it founds its own
// store, and one held among a set's volumes goes back at its close. The
// store's first volume is placed the same way, with nothing held yet, and a
// front that places it nowhere founds the store with it first. That claim is
// provisional: a volume whose own headers, read to its close, say it is not
// where the store holds it is given back, with what was read of it, and the
// volumes beside it claim it again by that parse alone. The
// RAR engine a store gives its set to places the rest of it by the same
// headers.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use unrar_rs::{ArchiveFormat, KdfCache, RarArchive, RarVolumeFacts, RarVolumeMemberFacts};
use weaver_graph::{Claim, VolumeIndex};

use crate::engine::Format;
use crate::rar_engine::Rar;

use super::{RarStore, Volume};
use crate::beside::Beside;
use crate::engine::SetName;
use crate::rar_engine::naming;

// A volume of the set already held, as far as it places the next: what its
// headers state, once they are read, whether a volume follows it, once
// that is known, and whether its headers were read to their end. A front
// is never read to its end.
#[derive(Clone, Copy)]
pub(crate) struct Held<'a> {
    pub(crate) facts: Option<&'a RarVolumeFacts>,
    pub(crate) successor: Option<bool>,
    pub(crate) ended: bool,
}

// The volumes held, by index, as far as they place the next: looked up
// where they are kept, never gathered for one claim.
pub(crate) trait Holds {
    fn at(&self, index: u32) -> Option<Held<'_>>;
}

// The places a volume ordered by its links alone may take, as the owner of
// the volumes held files them (`Beside`): never a walk over every volume
// held for one placement.
pub(crate) trait Places: Holds {
    // The free places beside a held volume, or more: `place` keeps only a
    // free one that fits.
    fn beside(&self) -> impl Iterator<Item = u32> + '_;
    // Every place held.
    fn held(&self) -> impl Iterator<Item = u32> + '_;
}

impl Holds for BTreeMap<u32, Held<'_>> {
    fn at(&self, index: u32) -> Option<Held<'_>> {
        self.get(&index).copied()
    }
}

// A walk over every volume held, for each placement: what `place` read
// before the owners filed the places beside the held ones. Kept for the test
// that compares the two.
#[cfg(test)]
impl Places for BTreeMap<u32, Held<'_>> {
    fn beside(&self) -> impl Iterator<Item = u32> + '_ {
        self.keys()
            .flat_map(|index| [index.checked_sub(1), index.checked_add(1)])
            .flatten()
    }

    fn held(&self) -> impl Iterator<Item = u32> + '_ {
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
    (!volume.misplaced(index)).then_some(Held {
        facts: volume.stated(),
        successor: volume.walk.successor(),
        ended: volume.walk.ended,
    })
}

// Whether the store holds a volume at `index` as it places the next: what
// the store files in its `Beside` wherever that may change.
pub(super) fn holds(index: VolumeIndex, volume: &Volume) -> bool {
    held(index, volume).is_some()
}

impl Holds for Bound<'_> {
    fn at(&self, index: u32) -> Option<Held<'_>> {
        let index = VolumeIndex(index);
        self.0.get(&index).and_then(|volume| held(index, volume))
    }
}

// The store's volumes with the places it filed beside them, for a
// placement by links.
struct Filed<'a> {
    bound: Bound<'a>,
    beside: &'a Beside,
}

impl Holds for Filed<'_> {
    fn at(&self, index: u32) -> Option<Held<'_>> {
        self.bound.at(index)
    }
}

impl Places for Filed<'_> {
    fn beside(&self) -> impl Iterator<Item = u32> + '_ {
        self.beside.free().inspect(|_| {
            #[cfg(test)]
            super::counts::bump(|counts| counts.beside += 1);
        })
    }

    fn held(&self) -> impl Iterator<Item = u32> + '_ {
        self.beside.held().inspect(|_| {
            #[cfg(test)]
            super::counts::bump(|counts| counts.beside += 1);
        })
    }
}

// What a member's front states that places it in a set: its headers, when
// they say whether it is a volume of one. A clear volume flag says so only
// past the main header (`read_main`): one an archive that is no volume
// states places it a set of its own at once, so no store's numbering takes
// it meanwhile, and none takes a volume beside it.
pub(crate) fn front_facts(
    first_bytes: &[u8],
    password: Option<&str>,
    kdf: Arc<KdfCache>,
) -> Option<RarVolumeFacts> {
    front_walk(first_bytes, password, kdf).map(|(facts, _)| facts)
}

// What `front_facts` reads, and whether its headers were read to their end:
// the walk stopped on the end record, or on the Quick Open cache, not where
// the bytes ran out.
pub(crate) fn front_walk(
    first_bytes: &[u8],
    password: Option<&str>,
    kdf: Arc<KdfCache>,
) -> Option<(RarVolumeFacts, bool)> {
    walk(first_bytes, password, kdf).filter(|(facts, _)| facts.is_volume || read_main(facts))
}

// Whether headers read off a volume were read past its main header: they
// list a file or service header, which in either format only follows the
// main header. Short of it, a walk reports the volume flag clear, which
// then says nothing.
pub(crate) fn read_main(facts: &RarVolumeFacts) -> bool {
    !facts.members.is_empty() || !facts.services.is_empty()
}

// The format whose signature a front opens on, at its first byte. A volume
// opens on its signature; a member that holds one further in is no volume,
// whatever it carries: a ReScene `.srr` opens on its own header block and
// holds copies of a set's volume headers, and a self-extracting `.exe` is
// never probed. The read is never `read_signature`'s, which scans past the
// first byte for a self-extractor's stub.
fn opens_rar(first_bytes: &[u8]) -> Option<ArchiveFormat> {
    unrar_rs::signature::detect_format(first_bytes).ok()
}

// What a front's headers state, of a volume or of an archive that is none:
// nothing for a front that does not open on a signature (`opens_rar`).
pub(crate) fn headers(
    first_bytes: &[u8],
    password: Option<&str>,
    kdf: Arc<KdfCache>,
) -> Option<RarVolumeFacts> {
    walk(first_bytes, password, kdf).map(|(facts, _)| facts)
}

// The headers, and whether the walk read them to their end.
fn walk(
    first_bytes: &[u8],
    password: Option<&str>,
    kdf: Arc<KdfCache>,
) -> Option<(RarVolumeFacts, bool)> {
    opens_rar(first_bytes)?;
    let walk = RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
        Cursor::new(first_bytes.to_vec()),
        password,
        kdf,
    )
    .ok()?;
    Some((walk.facts, walk.short_at.is_none()))
}

// The most a volume given back may carry of what was read of it: the ceiling
// a walk shows of a front before it is no RAR volume. A crafted volume whose
// headers run past it is refused rather than carried.
pub(super) const CARRIED: u64 = 4 * 1024 * 1024;

// Whether two volumes' headers are of one kind of set (`Nameless::kin`).
pub(crate) fn kin(facts: &RarVolumeFacts, other: &RarVolumeFacts) -> bool {
    Nameless { facts }.kin(&Nameless { facts: other })
}

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
    // RAR reads it. A RAR5 archive that is no volume is its own set's
    // first and only one, as a RAR4 one that continues nothing is.
    fn position(&self) -> Option<u32> {
        match (self.facts.volume_number, self.facts.archive_format()) {
            (Some(stated), _) => Some(stated),
            (None, ArchiveFormat::Rar5) => Some(0),
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

    // `ended` when the headers of `previous` were read to their end.
    fn follows(&self, previous: &Nameless<'_>, ended: bool, index: u32) -> bool {
        if !self.kin(previous) || self.position().is_some_and(|stated| stated != index) {
            return false;
        }
        match (previous.continuing_member(), self.continued_member()) {
            (Some(tail), Some(head)) => continues(tail, head),
            // A boundary that falls between two members links only on a
            // stated number; nothing else in the headers ties the volumes.
            (None, None) => self.position() == Some(index),
            // A volume whose file headers are not read yet links on its
            // stated number alone, as a boundary between members does. So
            // does one after a volume whose headers stop short of their end
            // on a member that does not carry on: its last header is not
            // read yet, and it says nothing against the next, as
            // `runs_into` reads it.
            _ => {
                let short = !ended && previous.continuing_member().is_none();
                (previous.unread() || self.unread() || short) && self.position() == Some(index)
            }
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

// Whether the headers put `first` before `then`, wherever either is held: a
// member `first` carries on is one `then` continues, and one of the two holds
// an end of it, its first part or its last. Two volumes that each only carry
// that member on are alike by their headers, and this says nothing of them
// (`order`).
pub(super) fn before(first: &RarVolumeFacts, then: &RarVolumeFacts) -> bool {
    let (first, then) = (Nameless { facts: first }, Nameless { facts: then });
    first.kin(&then)
        && first
            .continuing_member()
            .zip(then.continued_member())
            .is_some_and(|(tail, head)| {
                continues(tail, head) && !(tail.split_before && head.split_after)
            })
}

// Whether the headers put the volume held at `index + 1` before the one held
// at `index`, both held on trust: their names swapped the two, and the store
// orders them by their headers rather than give either back.
pub(super) fn inverted(index: u32, volumes: &BTreeMap<VolumeIndex, Volume>) -> bool {
    let trusted = |at: u32| {
        volumes
            .get(&VolumeIndex(at))
            .and_then(Volume::stated)
            .filter(|facts| !states_position(facts))
    };
    index
        .checked_add(1)
        .and_then(|next| trusted(index).zip(trusted(next)))
        .is_some_and(|(left, right)| before(right, left))
}

// Whether `head`, the member a volume opens on, is `tail`, the member the
// volume before it carries on, as far as the claim judges which set a
// volume is of: the two name the same member. The size each part states
// is no test of it. unrar extracts a member to the size its first part
// states and reads the later parts by place without looking at theirs
// (CmdExtract::ExtractCurrentFile and MergeArchive), so packers that
// misstate it in the middle parts make sets unrar reads whole; the CRC the
// last part states is what proves the member, and the layout checks it.
// What keeps two sets posted under one stem apart is the name, a number a
// volume's headers state, and a set's first volume opening on a new member.
// The store orders volumes by the same name (`member`, `order`).
fn continues(tail: &RarVolumeMemberFacts, head: &RarVolumeMemberFacts) -> bool {
    member(head) == member(tail)
}

// The name a part gives its member: what `continues` compares, and what
// the store orders volumes by (`order`).
pub(super) fn member(part: &RarVolumeMemberFacts) -> &str {
    part.name.as_str()
}

// Where a volume's headers and name place it in a set: the place, the
// place when what gave it states the volume's position, its headers or its
// name in the set's numbering, rather than links alone, and whether the
// volume is not yet judged.
#[derive(Default)]
pub(super) struct Placing {
    placed: Option<VolumeIndex>,
    stated: Option<VolumeIndex>,
    not_yet: bool,
    // The headers put the volume before the one that says no volume follows
    // it, which is held on trust: that volume's place does not end the set
    // ahead of it.
    before_last: bool,
}

impl Placing {
    // The claim the place makes. A set ends at the volume that says no
    // volume follows it.
    fn claim(self, store: &RarStore) -> Claim {
        let places: Vec<VolumeIndex> = self
            .placed
            .filter(|at| self.before_last || store.last().is_none_or(|last| *at <= last))
            .into_iter()
            .collect();
        if self.not_yet {
            Claim::NotYet(places)
        } else {
            Claim::Places(places)
        }
    }
}

impl RarStore {
    // A volume's claim by its front, with the bytes past it the RAR engine
    // asked for (`Claim::Wants`), when they came, read into a copy of its
    // facts first: `told`. The store asks for none itself.
    pub(super) fn claim_told(&self, name: &str, first_bytes: &[u8], tail: &[u8]) -> Claim {
        let Some(format) = opens_rar(first_bytes) else {
            return Claim::default();
        };
        let facts = told(self.front_facts(first_bytes).map(Cow::Owned), tail);
        self.placing(name, Some(format), facts.as_deref())
            .claim(self)
    }

    // A member offered by the bytes past its hole, its front never to
    // come: nothing of it says it is a volume but its name, so only its
    // name in the set's numbering places it, as in a set numbered by its
    // names alone. A name no set's numbering places, or one the bound
    // names do not share, places nothing.
    pub(super) fn claim_past_hole(&self, name: &str) -> Claim {
        self.placing(name, None, None).claim(self)
    }

    fn placing_volume(&self, name: &str, first_bytes: &[u8]) -> Placing {
        let Some(format) = opens_rar(first_bytes) else {
            return Placing::default();
        };
        self.placing(name, Some(format), self.front_facts(first_bytes).as_ref())
    }

    // A transferable volume is judged by its parsed headers, or by its
    // front where that read further (`given_stated`), and by the retained
    // front and name while those headers say nothing. Both are the same
    // evidence an unowned member offers, and are answered as it is, not yet
    // included. A volume whose end
    // record says its set ends with it is taken nowhere a volume held here
    // lies past: held there, that volume would go back at once, as one past
    // the last does, only to be taken again.
    pub(super) fn reclaim_volume(
        &self,
        name: &str,
        (front, tail): (&[u8], &[u8]),
        given: &Volume,
    ) -> Claim {
        let ends = given.walk.is_last();
        // A volume the set's headers put somewhere its name does not is
        // claimed there, while nothing held beside it says otherwise.
        let ordered = given.ordered().filter(|at| {
            given
                .stated()
                .is_some_and(|facts| !breaks(facts, at.0, &Bound(&self.volumes)))
        });
        let mut placing = match ordered {
            Some(at) => Placing {
                placed: Some(at),
                stated: Some(at),
                ..Placing::default()
            },
            None => {
                let stated = told(self.given_stated(given, front), tail);
                self.reclaim_placing(name, front, stated.as_deref())
            }
        };
        placing.placed = placing.placed.filter(|at| {
            !ends
                || self
                    .volumes
                    .range((std::ops::Bound::Excluded(*at), std::ops::Bound::Unbounded))
                    .next()
                    .is_none()
        });
        placing.claim(self)
    }

    pub(super) fn reclaim_front(
        &self,
        name: &str,
        (front, tail): (&[u8], &[u8]),
        facts: Option<&RarVolumeFacts>,
    ) -> Claim {
        let facts = told(facts.map(Cow::Borrowed), tail);
        self.reclaim_placing(name, front, facts.as_deref())
            .claim(self)
    }

    fn reclaim_placing(&self, name: &str, front: &[u8], facts: Option<&RarVolumeFacts>) -> Placing {
        let mut placing = self.placing_front(name, front, facts);
        // A volume another store holds is taken nowhere it cannot sit
        // beside a volume held here: what its holder links it to outranks
        // a name or number every set states alike.
        let held = Bound(&self.volumes);
        placing.placed = placing
            .placed
            .filter(|at| !facts.is_some_and(|facts| breaks(facts, at.0, &held)));
        placing
    }

    fn placing_front(&self, name: &str, front: &[u8], facts: Option<&RarVolumeFacts>) -> Placing {
        match facts {
            Some(facts) => self.placing(name, Some(facts.archive_format()), Some(facts)),
            None => self.placing_volume(name, front),
        }
    }

    // Whether the volume at `index` yields to the member whose name and
    // front, or what was parsed of it, give that place: links alone hold it
    // there, and the member states it, by its headers or by its name in the
    // set's numbering. A claimant that only links never moves it. Nor does
    // a name or number keep a holder there that cannot sit beside a held
    // neighbour, when the claimant that states the place can: that holder
    // is another set's, however it came first.
    pub(super) fn yields(
        &self,
        index: VolumeIndex,
        holder: &Volume,
        name: &str,
        front: &[u8],
        given: Option<&Volume>,
    ) -> bool {
        let read;
        let facts = match given {
            Some(given) => {
                read = self.given_stated(given, front);
                read.as_deref()
            }
            None => {
                read = self.front_facts(front).map(Cow::Owned);
                read.as_deref()
            }
        };
        let placing = self.placing_front(name, front, facts);
        let held = Bound(&self.volumes);
        let stated = placing.stated == Some(index)
            && (!settled(index, holder)
                || facts
                    .zip(holder.stated())
                    .is_some_and(|(claimant, holder)| {
                        breaks(holder, index.0, &held) && !breaks(claimant, index.0, &held)
                    }));
        stated
            || (placing.stated.is_none()
                && placing.placed == Some(index)
                && on_links(index, holder)
                && self::held(index, holder).is_some_and(|holding| {
                    let claimant = Held {
                        facts,
                        successor: given.and_then(|given| given.walk.successor()),
                        ended: given.is_some_and(|given| given.walk.ended),
                    };
                    let filed = Filed {
                        bound: Bound(&self.volumes),
                        beside: &self.beside,
                    };
                    makes_way(index.0, holding, claimant, &filed)
                }))
    }

    // The place a volume's headers and name give it in this set. Headers
    // that give it a place its name gives another set are judged by the
    // volumes beside it, by its file headers; a front that has none of
    // those yet is not yet judged, and its place is what it gives now. A
    // member with no front states no format.
    fn placing(
        &self,
        name: &str,
        format: Option<ArchiveFormat>,
        facts: Option<&RarVolumeFacts>,
    ) -> Placing {
        if format.is_some_and(|format| self.holds_other_than(format)) {
            return Placing::default();
        }
        // An archive whose headers say it is no volume is a set of one,
        // whatever its name numbers: it is the first and only volume of a
        // store that holds nothing else, and no store holding a volume
        // takes it.
        if facts.is_some_and(|facts| !facts.is_volume) {
            let alone = self.volumes.is_empty().then_some(VolumeIndex(0));
            return Placing {
                placed: alone,
                stated: alone,
                ..Placing::default()
            };
        }
        // The set's numbering is the one the bound names share; before any
        // is bound, the one this member's own name gives. Each bound name
        // was read once, when it was bound.
        let wanted = naming::parse(name);
        let bound = self.numberings.any();
        let ours = wanted.as_ref().map(|wanted| self.numberings.holds(wanted));
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
        let numbered = wanted
            .filter(|_| named)
            .map(|wanted| VolumeIndex(wanted.index()));
        // A name in the set's numbering that the headers contradict: the
        // two disagree, so beside a held volume whose headers could link it
        // the stated position is the volume's only where that neighbour
        // does. A stray posted under a volume's name then takes no place
        // the set's own volume should, whichever came first.
        let misnamed =
            numbered.is_some_and(|index| facts.is_some_and(|facts| misplaced(index, facts)));
        let contradicted = bound && misnamed && facts.is_some_and(|facts| self.beside(facts));
        let named = numbered.filter(|_| !misnamed);
        // A number is one every set states alike, so a position the headers
        // state is the volume's only where a held volume beside it judged
        // it, or where nothing gives the volume to another set.
        let (headers, judged) = facts.map_or((None, false), |facts| self.claim_by_headers(facts));
        let unread = facts.is_some_and(|facts| Nameless { facts }.unread());
        let not_yet = headers.is_some() && !judged && elsewhere && unread;
        let headers = headers.filter(|_| judged || !(elsewhere || contradicted));
        // A new numbering can refine an established header link, but an
        // unrelated name cannot invent membership beside opaque labels.
        let named = named.filter(|_| bound || self.volumes.is_empty() || headers.is_some());
        // A position held already is answered all the same: the volume
        // posted again is that volume. Headers that state a position a held
        // volume beside it cannot sit beside place it nowhere here, its name
        // least of all: a first volume that does not run into the second
        // held is another set's, whichever came first. A neighbour whose
        // front stops short says nothing against it, and the name places it.
        let ruled_out = facts.is_some_and(|facts| {
            Nameless { facts }
                .position()
                .is_some_and(|stated| breaks(facts, stated, &Bound(&self.volumes)))
        });
        let (placed, stated) = match (headers, named) {
            (Some(placed), _) if stated => (Some(placed), Some(placed)),
            (None, Some(_)) if stated && ruled_out => (None, None),
            (_, Some(named)) => (Some(named), Some(named)),
            (linked, None) => (linked, None),
        };
        Placing {
            placed,
            stated,
            not_yet,
            before_last: facts.is_some_and(|facts| self.before_last(facts)),
        }
    }

    // Whether the headers put a volume before the one held on trust that
    // says no volume follows it.
    pub(super) fn before_last(&self, facts: &RarVolumeFacts) -> bool {
        self.last()
            .and_then(|last| self.volumes.get(&last))
            .and_then(Volume::stated)
            .is_some_and(|last| !states_position(last) && before(facts, last))
    }

    // What a volume given back states as it is offered again: whichever
    // reading of its headers went further, the walk it carries or its front
    // as it stands now. The front went further when it lists more member
    // headers than the walk's facts hold, or reached the end record the walk
    // did not. A walk that ended read every header, so its front is not
    // read at all.
    fn given_stated<'a>(&self, given: &'a Volume, front: &[u8]) -> Option<Cow<'a, RarVolumeFacts>> {
        let carried = given.stated();
        if given.walk.ended {
            return carried.map(Cow::Borrowed);
        }
        let password = self.header_password();
        let read = front_walk(
            front,
            password.as_deref().map(String::as_str),
            Arc::clone(self.keys.kdf()),
        );
        let held = carried.map_or(0, |facts| facts.members.len());
        match read {
            Some((facts, ended)) if ended || facts.members.len() > held => Some(Cow::Owned(facts)),
            _ => carried.map(Cow::Borrowed),
        }
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

    // Whether a held volume whose headers were read sits beside the
    // position this volume's headers state.
    fn beside(&self, facts: &RarVolumeFacts) -> bool {
        let held = Bound(&self.volumes);
        Nameless { facts }.position().is_some_and(|stated| {
            [stated.checked_sub(1), stated.checked_add(1)]
                .into_iter()
                .flatten()
                .any(|at| held.at(at).is_some_and(|held| held.facts.is_some()))
        })
    }

    // The position this volume's headers give it beside the volumes held,
    // and whether a held volume beside it judged that position.
    fn claim_by_headers(&self, facts: &RarVolumeFacts) -> (Option<VolumeIndex>, bool) {
        let held = Filed {
            bound: Bound(&self.volumes),
            beside: &self.beside,
        };
        (place(facts, &held), judged(facts, &held))
    }
}

// The facts a claim is judged by, with the bytes past the front the RAR
// engine asked for (`Claim::Wants`) read into a copy of them, as the RAR engine reads a
// bound volume's tail (`Rar::told`). The facts as they are without any.
fn told<'a>(
    facts: Option<Cow<'a, RarVolumeFacts>>,
    tail: &[u8],
) -> Option<Cow<'a, RarVolumeFacts>> {
    match tail.is_empty() {
        true => facts,
        false => facts.map(|facts| {
            let mut told = facts.into_owned();
            Rar::told(&mut told, tail, None);
            Cow::Owned(told)
        }),
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

// How a volume at `index` meets the held volumes beside it, the one before
// and the one after: whether one member runs across that boundary, the
// same kind of set on both sides, or nothing where no neighbour is held
// whose headers were read.
fn meets(facts: &RarVolumeFacts, index: u32, held: &impl Holds) -> [Option<bool>; 2] {
    let this = Nameless { facts };
    let across = |tail: &Nameless<'_>, head: &Nameless<'_>| {
        tail.kin(head)
            && tail
                .continuing_member()
                .zip(head.continued_member())
                .is_some_and(|(tail, head)| continues(tail, head))
    };
    let neighbour = |at: Option<u32>| {
        at.and_then(|at| held.at(at))
            .and_then(|held| held.facts)
            .map(|facts| Nameless { facts })
    };
    [
        neighbour(index.checked_sub(1)).map(|previous| across(&previous, &this)),
        neighbour(index.checked_add(1)).map(|next| across(&this, &next)),
    ]
}

// Whether a volume at `index` cannot sit beside the held volume before it
// or after it, by what the headers at that boundary state: the sets differ
// in kind, a member carried on is not the one the next volume opens on, or
// a volume opens on a continued member that the one before, read to its
// end, does not carry on. A front not read that far says nothing.
fn breaks(facts: &RarVolumeFacts, index: u32, held: &impl Holds) -> bool {
    let this = Nameless { facts };
    let before = index
        .checked_sub(1)
        .and_then(|at| held.at(at))
        .is_some_and(|previous| {
            previous.facts.is_some_and(|facts| {
                apart(&Nameless { facts }, previous.successor.is_some(), &this)
            })
        });
    let after = index
        .checked_add(1)
        .and_then(|at| held.at(at))
        .is_some_and(|next| {
            next.facts
                .is_some_and(|facts| apart(&this, false, &Nameless { facts }))
        });
    before || after
}

// Whether `head` cannot follow `tail`, by what both state at the boundary
// between them; `ended` when `tail` was read to its end.
fn apart(tail: &Nameless<'_>, ended: bool, head: &Nameless<'_>) -> bool {
    !tail.kin(head)
        || match (tail.continuing_member(), head.first_member()) {
            (Some(tail), Some(head)) => !(head.split_before && continues(tail, head)),
            (None, Some(head)) => head.split_before && ended && tail.last_member().is_some(),
            (_, None) => false,
        }
}

// The places beside the volume at `index` its headers run into: the place
// after it when a member carries on past its end and a volume follows it
// before any last one, and the place before it when its first member
// carries on from a volume before it.
pub(super) fn beside(
    index: VolumeIndex,
    volume: &Volume,
    last: Option<VolumeIndex>,
) -> [Option<VolumeIndex>; 2] {
    let Some(facts) = volume.stated() else {
        return [None, None];
    };
    let this = Nameless { facts };
    let after = (volume.walk.successor() == Some(true)
        && this.continuing_member().is_some()
        && last.is_none_or(|last| index < last))
    .then(|| index.0.checked_add(1).map(VolumeIndex))
    .flatten();
    let before = this
        .first_member()
        .is_some_and(|member| member.split_before)
        .then(|| index.0.checked_sub(1).map(VolumeIndex))
        .flatten();
    [after, before]
}

// The held volumes beside the one at `index` that cannot sit beside it,
// where one of the two is anchored and the other is not: a member runs
// across between it and its other neighbour, or it states it is its set's
// first, at the first place. The one not anchored is no volume of this set
// there, whichever came first; where both or neither are anchored, what the
// store holds does not say which, and it keeps both.
pub(super) fn misfits(
    index: VolumeIndex,
    volumes: &BTreeMap<VolumeIndex, Volume>,
) -> Vec<VolumeIndex> {
    let held = Bound(volumes);
    let anchored = |at: u32| {
        held.at(at)
            .and_then(|held| held.facts)
            .is_some_and(|facts| {
                (at == 0 && Nameless { facts }.position() == Some(0))
                    || meets(facts, at, &held).contains(&Some(true))
            })
    };
    let Some(this) = held.at(index.0) else {
        return Vec::new();
    };
    let Some(facts) = this.facts else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for at in [index.0.checked_sub(1), index.0.checked_add(1)]
        .into_iter()
        .flatten()
    {
        let Some(other) = held.at(at) else {
            continue;
        };
        let Some(theirs) = other.facts else {
            continue;
        };
        // Two held on trust in the order opposite to the one their headers
        // prove are the store's to order, not to give back.
        if inverted(at.min(index.0), volumes) {
            continue;
        }
        let broken = if at < index.0 {
            apart(
                &Nameless { facts: theirs },
                other.successor.is_some(),
                &Nameless { facts },
            )
        } else {
            apart(
                &Nameless { facts },
                this.successor.is_some(),
                &Nameless { facts: theirs },
            )
        };
        if !broken {
            continue;
        }
        match (anchored(index.0), anchored(at)) {
            (true, false) => out.push(VolumeIndex(at)),
            (false, true) => out.push(index),
            _ => {}
        }
    }
    out
}

// Whether the volume held at `index` is tied there: its headers state that
// position, or its name gives it in the set's numbering, and a member runs
// across from the held volume before it, or on into the one after. Such a
// volume is its set's at that place, so no other store takes it, whichever
// of them, or of a stray, came first. One held by links alone ties nothing:
// one member spanning every volume links each place to all the others.
pub(super) fn tied(index: VolumeIndex, volumes: &BTreeMap<VolumeIndex, Volume>) -> bool {
    let held = Bound(volumes);
    let Some(facts) = held.at(index.0).and_then(|this| this.facts) else {
        return false;
    };
    let numbered = volumes.get(&index).is_some_and(|volume| {
        volume.ordered() == Some(index)
            || volume
                .numbered
                .as_ref()
                .is_some_and(|name| name.index() == index.0)
    });
    if !numbered && (Nameless { facts }).position() != Some(index.0) {
        return false;
    }
    meets(facts, index.0, &held).contains(&Some(true))
}

// Whether a volume at `index` is tied to a held volume beside it: one
// member runs across from the one before, or on into the one after.
pub(crate) fn linked(facts: &RarVolumeFacts, index: u32, held: &impl Holds) -> bool {
    meets(facts, index, held).contains(&Some(true))
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
// so a volume they placed is mapped only once its own end is read, and a
// holder links alone hold yields to a claimant that states the place.
//
// Whether a volume is settled at `index`: read to its end, or placed there
// by a stated position or by its name. Nothing settles a volume where its
// own headers say it is not, a name least of all. An end read that states
// no number, as a RAR4 volume's short of a numbered end record, says
// nothing of where the volume is: one its own name numbers at another place
// is held where links alone put it, and a claimant that states the place
// takes it. Links do not order the middle volumes of one member, which each
// carry it on alike, so the first of them to come takes the lowest free
// place, whichever it is.
pub(super) fn settled(index: VolumeIndex, volume: &Volume) -> bool {
    let facts = volume.stated();
    // A name the volume's own headers refute numbers it nowhere, as in `placing`.
    let named = volume
        .numbered
        .as_ref()
        .filter(|name| !volume.misplaced(VolumeIndex(name.index())))
        .map(|name| name.index() == index.0);
    !volume.misplaced(index)
        && ((volume.walk.ended && named != Some(false))
            || facts.is_some_and(states_position)
            || volume.ordered() == Some(index)
            || named == Some(true))
}

// Whether only its links hold a volume at `index`: its headers state no
// position, its name does not number it there, and the set's order did not
// put it there. A walk read to an end that states no number is no more.
fn on_links(index: VolumeIndex, volume: &Volume) -> bool {
    !volume.stated().is_some_and(states_position)
        && volume
            .numbered
            .as_ref()
            .is_none_or(|name| name.index() != index.0)
        && volume.ordered() != Some(index)
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
pub(crate) fn place(facts: &RarVolumeFacts, held: &impl Places) -> Option<VolumeIndex> {
    let this = Nameless { facts };
    let joins = |previous: &Held<'_>, index: u32| {
        previous.successor != Some(false)
            && previous
                .facts
                .is_some_and(|facts| this.follows(&Nameless { facts }, previous.ended, index))
    };
    // A stated position is judged by what the neighbours' headers say, and
    // a neighbour none of whose headers were read yet says nothing against
    // it, on either side. A position with no place after it is none a set
    // has, and one the volume's own headers rule out places it nowhere, as
    // they hold it nowhere settled: a first volume that opens on a member
    // begun before it.
    if let Some(stated) = this.position() {
        if misplaced(VolumeIndex(stated), facts) {
            return None;
        }
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
        return (before && after).then_some(VolumeIndex(stated));
    }
    // The volume placed is no held one: what it says against the next is
    // what its headers state, as before.
    let precedes = |next: &Held<'_>, index: u32| {
        next.facts
            .is_some_and(|facts| Nameless { facts }.follows(&this, true, index))
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
    // A free place fits only beside a held volume, so the places the owner
    // filed beside the held ones are every candidate. A held place is asked
    // only once no free one fits.
    let lowest = held
        .beside()
        .filter(|slot| held.at(*slot).is_none() && fits(*slot))
        .min();
    if let Some(lowest) = lowest {
        return Some(VolumeIndex(lowest));
    }
    let mut again = held.held().filter(|slot| fits(*slot));
    match (again.next(), again.next()) {
        (Some(only), None) => Some(VolumeIndex(only)),
        _ => None,
    }
}

// Whether `holder`, held at `index` by its links alone, makes way there for
// `claimant`, which links alone place there and at no free place: the holder
// says no volume follows it, the claimant does not, and the holder, with the
// claimant at `index`, fits a free place beside the volumes held. A set's
// last volume posted first is held after the first volume, where its links
// fit while the middle has not come; the middle that comes then fits only
// there, and the last goes after it.
//
// It never runs back: a claimant that makes a holder move does not say it
// ends the set, so nothing moves it by this rule, and the holder, which
// does, moves nothing. Each move places a claimant that stays, so a set of
// `n` volumes moves at most `n` times.
pub(crate) fn makes_way(
    index: u32,
    holder: Held<'_>,
    claimant: Held<'_>,
    held: &impl Places,
) -> bool {
    let (Some(facts), Some(_)) = (holder.facts, claimant.facts) else {
        return false;
    };
    let instead = Instead {
        held,
        index,
        claimant,
    };
    holder.successor == Some(false)
        && claimant.successor != Some(false)
        && !states_position(facts)
        && place(facts, &instead).is_some_and(|at| held.at(at.0).is_none())
}

// The volumes held, with `claimant` at `index` in place of its holder.
struct Instead<'a, P> {
    held: &'a P,
    index: u32,
    claimant: Held<'a>,
}

impl<P: Places> Holds for Instead<'_, P> {
    fn at(&self, index: u32) -> Option<Held<'_>> {
        match index == self.index {
            true => Some(self.claimant),
            false => self.held.at(index),
        }
    }
}

// The places held stay the ones held: `index` is held by the claimant.
impl<P: Places> Places for Instead<'_, P> {
    fn beside(&self) -> impl Iterator<Item = u32> + '_ {
        self.held.beside()
    }

    fn held(&self) -> impl Iterator<Item = u32> + '_ {
        self.held.held()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use unrar_rs::{KdfCache, RarVolumeFacts};
    use weaver_graph::VolumeIndex;

    use super::{Held, headers, place};
    use crate::rar_store::writer::{Item, Options, Version, write};
    use crate::testing::payload;

    fn read(bytes: &[u8]) -> RarVolumeFacts {
        headers(bytes, None, Arc::new(KdfCache::new())).expect("headers")
    }

    // Four volumes: the first file runs from the first into the second, a
    // small one lies whole in the second, and the third runs on from the
    // second into the third and fourth.
    fn set(version: Version) -> Vec<Vec<u8>> {
        let mut options = Options::new(version);
        options.split = Some(6_000);
        options.numbered = true;
        let set = write(
            &[
                Item::File("alpha.bin", payload(1, 10_000)),
                Item::File("beta.bin", payload(2, 300)),
                Item::File("gamma.bin", payload(3, 10_000)),
            ],
            &options,
        );
        assert_eq!(set.len(), 4, "{version:?}");
        set
    }

    // The second volume's front read as far as the small file's data: the
    // member it last read does not carry on, and the header of the one
    // that does is not read yet.
    fn short_second(set: &[Vec<u8>]) -> RarVolumeFacts {
        let whole = read(&set[1]);
        let small = whole
            .members
            .iter()
            .find(|member| member.name == "beta.bin")
            .expect("the small file");
        let short = read(&set[1][..(small.data_offset + small.data_size) as usize]);
        assert!(short.members.iter().all(|member| !member.split_after));
        assert!(short.members.iter().any(|member| member.split_before));
        short
    }

    fn holding<'a>(
        first: &'a RarVolumeFacts,
        second: &'a RarVolumeFacts,
        ended: bool,
    ) -> BTreeMap<u32, Held<'a>> {
        BTreeMap::from([
            (
                0,
                Held {
                    facts: Some(first),
                    successor: Some(true),
                    ended: true,
                },
            ),
            (
                1,
                Held {
                    facts: Some(second),
                    successor: None,
                    ended,
                },
            ),
        ])
    }

    // A volume that states the next position follows a held volume whose
    // headers stop short of their end on a member that does not carry on:
    // that volume's last header is not read yet, and it says nothing
    // against the next. Read to its end on that member, it does.
    #[test]
    fn a_stated_position_follows_a_volume_whose_headers_stop_short() {
        for version in [Version::Rar5, Version::Rar4] {
            let set = set(version);
            let first = read(&set[0]);
            let second = short_second(&set);
            let third = read(&set[2]);
            assert_eq!(third.volume_number, Some(2), "{version:?}");
            let short = holding(&first, &second, false);
            assert_eq!(place(&third, &short), Some(VolumeIndex(2)), "{version:?}");
            let ended = holding(&first, &second, true);
            assert_eq!(place(&third, &ended), None, "{version:?}");
        }
    }

    // A volume that states no position gets nothing from a neighbour that
    // stops short: a RAR4 front short of its numbered end record is held
    // nowhere beside it by that neighbour alone.
    #[test]
    fn a_volume_stating_no_position_gets_nothing_from_a_neighbour_that_stops_short() {
        let set = set(Version::Rar4);
        let first = read(&set[0]);
        let second = short_second(&set);
        let mut third = read(&set[2]);
        third.volume_number = None;
        let short = holding(&first, &second, false);
        assert_eq!(place(&third, &short), None);
    }
}
