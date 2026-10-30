// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The engine object, shared by every engine format: conventional
// extraction of a set that sits on disk as files, whole or still growing.
// The object itself keeps almost nothing: the set's names, and what its
// volumes' fronts state of their places, so it can claim the set's other
// volumes, which volumes it has, and a password the job offered. How far
// the run has read is the owner's, which hands it in to bid for the
// volumes the run reaches next.
// Every byte of its volumes is envelope, kept by the owner; the run reads
// them back. A format supplies only how its volumes are named, what their
// fronts say of their places, and what a run of it is.

mod joined;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::ops::Bound;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Engine, Extent, Extraction, Facts, Kind, Located, MemberId,
    Object, Placement, Pull, Seek, Standing, VolumeIndex, Weight, Why,
};

pub use crate::quiet::Placed;
pub(crate) use joined::Joined;

// What one engine format knows: its kind, how its set names its volumes,
// what a volume's front states of its place, and the run that extracts it.
pub trait Format: Send + Sync {
    const KIND: Kind;
    type Name: SetName;
    // What a volume's front states of its place in its set, for a set whose
    // names say nothing of it. Serde, because it is an engine's fact.
    type Front: Serialize + DeserializeOwned + Send + Sync;
    fn parse(name: &str) -> Option<Self::Name>;
    // A volume's front, as far as it places the volume: `None` for a format
    // whose volumes state nothing of their places.
    fn front(_first_bytes: &[u8]) -> Option<Self::Front> {
        None
    }
    // Whether the front states the volume's place itself, rather than only
    // linking it to the volumes beside it.
    fn states_place(_front: &Self::Front) -> bool {
        false
    }
    // Whether the front says the volume is not at `index`.
    fn misplaced(_front: &Self::Front, _index: u32) -> bool {
        false
    }
    // Where the volumes bound place a volume with `front`, when its front
    // states one, and `length`, once known: each bound volume by index,
    // with its front and its length.
    fn place(
        _bound: &BTreeMap<u32, Holding<'_, Self::Front>>,
        _front: Option<&Self::Front>,
        _length: Option<u64>,
    ) -> Option<Placed> {
        None
    }

    // Whether a placement also judges membership across conflicting set
    // names. Formats whose headers only state a position must distinguish
    // that from evidence linking the candidate to the bound volumes.
    fn judged(
        _bound: &BTreeMap<u32, Holding<'_, Self::Front>>,
        _front: Option<&Self::Front>,
        _length: Option<u64>,
    ) -> bool {
        true
    }
    fn run(password: Option<String>) -> Box<dyn Extraction>;

    fn reserve() -> u64 {
        0
    }
}

// A volume bound, as far as it places the others: what its front states,
// and its length once known.
pub struct Holding<'a, T> {
    pub front: Option<&'a T>,
    pub length: Option<u64>,
}

// Engine volumes hold no parsed output. Only their existing placement
// evidence travels when a corrected name gives one back for reoffering.
pub(crate) struct CarriedVolume<T> {
    pub(crate) front: Option<T>,
    length: Option<u64>,
}

// A volume's name as its set numbers it.
// Serde, because the names bound are an engine's facts.
pub trait SetName: Send + Sync + Sized + Serialize + DeserializeOwned {
    // The volume this name is, counted from zero.
    fn index(&self) -> u32;
    fn same_set(&self, other: &Self) -> bool;

    // The volume this name becomes beside the volumes of its set already
    // bound, by index, held or not: a volume posted again is that volume.
    // A name is its own number; a set whose last volume carries none says
    // otherwise.
    fn slot(&self, _bound: &BTreeMap<u32, Self>) -> Option<u32> {
        Some(self.index())
    }
}

// Its facts are the volumes it has, by name and front. Never the password,
// which the owner offers again.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct EngineObject<F: Format> {
    #[serde(skip)]
    format: PhantomData<F>,
    // The names of the volumes bound that name a volume of a set, which
    // every other volume's must share.
    names: BTreeMap<u32, F::Name>,
    // What the fronts of the volumes bound state of their places.
    #[serde(default)]
    fronts: BTreeMap<u32, F::Front>,
    // The lengths of the volumes bound, where a format's lengths place
    // its volumes.
    #[serde(default)]
    lengths: BTreeMap<u32, u64>,
    bound: BTreeSet<u32>,
    #[serde(skip)]
    password: Option<String>,
    // The set was refused: what it claims after goes where the set went.
    #[serde(skip)]
    gone: bool,
}

impl<F: Format> Durable for EngineObject<F> {
    // 3: a directory is a member, numbered in the walk with the files, so the
    // members recorded under an older run carry other ids.
    const VERSION: u16 = 3;

    fn impossible(&self) -> Option<&'static str> {
        let known = self
            .names
            .keys()
            .chain(self.fronts.keys())
            .chain(self.lengths.keys());
        let named = known.into_iter().all(|volume| self.bound.contains(volume));
        (!named).then_some("a volume named that is not bound")
    }
}

impl<F: Format> Default for EngineObject<F> {
    fn default() -> Self {
        Self {
            format: PhantomData,
            names: BTreeMap::new(),
            fronts: BTreeMap::new(),
            lengths: BTreeMap::new(),
            bound: BTreeSet::new(),
            password: None,
            gone: false,
        }
    }
}

impl<F: Format> EngineObject<F> {
    pub fn new() -> Self {
        Self::default()
    }

    // A volume's place is what states it, as the RAR store places one: the
    // position its front gives, beside what the fronts of the volumes bound
    // state; else its name in the set's own numbering where the front does
    // not say the volume is not there; and only a volume that places
    // neither by what links it to the volumes bound, its front or its
    // length, which guess wrong across a volume not yet bound. A posted
    // name is a label, which a recovery set may change later, so the front
    // outranks it. A place bound already is answered all the same: the
    // volume posted again is that volume.
    fn placed(&self, name: &str, first_bytes: &[u8], length: Option<u64>) -> Option<Placed> {
        self.placed_front(name, F::front(first_bytes).as_ref(), length)
    }

    fn placed_front(
        &self,
        name: &str,
        front: Option<&F::Front>,
        length: Option<u64>,
    ) -> Option<Placed> {
        let bound = self
            .bound
            .iter()
            .map(|volume| {
                let holding = Holding {
                    front: self.fronts.get(volume),
                    length: self.lengths.get(volume).copied(),
                };
                (*volume, holding)
            })
            .collect();
        let stated = front.is_some_and(F::states_place);
        let wanted = F::parse(name);
        let elsewhere = wanted.as_ref().is_some_and(|candidate| {
            self.names
                .values()
                .next()
                .is_some_and(|first| !first.same_set(candidate))
        });
        let placed = F::place(&bound, front, length)
            .filter(|_| !elsewhere || F::judged(&bound, front, length));
        let named = wanted
            .filter(|candidate| {
                self.names
                    .values()
                    .next()
                    .is_some_and(|first| first.same_set(candidate))
            })
            .and_then(|candidate| candidate.slot(&self.names))
            .filter(|index| !front.is_some_and(|front| F::misplaced(front, *index)));
        match (placed, named) {
            (Some(placed), _) if stated => Some(placed),
            (_, Some(named)) => Some(Placed::At(VolumeIndex(named))),
            (linked, None) => linked,
        }
    }
}

impl<F: Format + 'static> Object for EngineObject<F> {
    fn kind(&self) -> Kind {
        F::KIND
    }

    // The run reads the volumes from the owner's files; the bytes on their
    // way there say nothing the run does not read itself, but a volume's
    // front. A volume can be bound before its front comes, as a restored
    // volume fetched again is: what its front states of its place is the
    // engine's when it does.
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _out: &mut Vec<Emit>,
    ) {
        let volume = delivery.volume.0;
        if delivery.offset == 0
            && self.bound.contains(&volume)
            && let Some(front) = F::front(delivery.bytes)
        {
            self.fronts.entry(volume).or_insert(front);
        }
    }

    fn volume_closed(&mut self, _volume: VolumeIndex, _out: &mut Vec<Emit>) {}

    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    // Nothing more will reach the run. A member it left damaged, a password
    // it is still asking for, a volume beside the set that nothing orders,
    // a member it named and never closed: the set cannot be read whole,
    // and its volumes go back as they stand.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        let why = crate::quiet::damaged(standing)
            .or_else(|| standing.asking().then_some(Why::EncryptedHeader))
            .or_else(|| {
                crate::quiet::unordered(standing, |beside| {
                    self.placed(beside.name, beside.front, beside.length)
                })
            })
            .or_else(|| crate::quiet::unplaced(standing));
        out.extend(why.map(Emit::Refuse));
    }

    // It wants nothing of itself: what it bids for follows its run.
    fn wants(&self, _out: &mut Vec<Seek>) {}

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // The fronts it copies out are in its facts, which a restart keeps, and
    // its run reads the volumes themselves: it says nothing of the posted bytes.
    fn unposted(&mut self, _volume: VolumeIndex, _range: ByteRange) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn offer_password(&mut self, secret: &str, _out: &mut Vec<Emit>) {
        self.password = Some(secret.to_owned());
    }

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, front: &[u8], length: Option<u64>) {
        let first = self.names.values().next();
        if let Some(parsed) = F::parse(name)
            && first.is_none_or(|first| first.same_set(&parsed))
        {
            self.names.insert(volume.0, parsed);
        }
        if let Some(front) = F::front(front) {
            self.fronts.insert(volume.0, front);
        }
        if let Some(length) = length {
            self.lengths.insert(volume.0, length);
        }
        self.bound.insert(volume.0);
    }

    fn refused(&mut self) {
        self.gone = true;
    }

    // The name a recovery set gave the volume is the set's numbering now.
    fn renamed(&mut self, volume: VolumeIndex, name: &str, out: &mut Vec<Emit>) {
        self.names.remove(&volume.0);
        let first = self.names.values().next();
        let parsed = F::parse(name);
        let elsewhere = parsed
            .as_ref()
            .is_some_and(|parsed| first.is_some_and(|first| !first.same_set(parsed)));
        if !elsewhere && let Some(parsed) = parsed {
            self.names.insert(volume.0, parsed);
        }
        let corrected = self.placed_front(
            name,
            self.fronts.get(&volume.0),
            self.lengths.get(&volume.0).copied(),
        );
        if matches!(corrected, Some(Placed::At(at)) if at != volume)
            || (elsewhere && corrected.is_none())
        {
            out.push(Emit::GiveBack(volume));
        }
    }

    fn give_back(&mut self, volume: VolumeIndex) -> Option<weaver_graph::Carried> {
        self.bound.remove(&volume.0).then(|| {
            self.names.remove(&volume.0);
            Box::new(CarriedVolume {
                front: self.fronts.remove(&volume.0),
                length: self.lengths.remove(&volume.0),
            }) as weaver_graph::Carried
        })
    }

    fn reclaim(
        &self,
        name: &str,
        front: &[u8],
        length: Option<u64>,
        carried: &(dyn std::any::Any + Send + Sync),
    ) -> Option<VolumeIndex> {
        let carried = carried.downcast_ref::<CarriedVolume<F::Front>>()?;
        let parsed = F::front(front);
        self.placed_front(
            name,
            carried.front.as_ref().or(parsed.as_ref()),
            carried.length.or(length),
        )?
        .claimed(self.gone, Placed::past(self.bound.last().copied()))
    }

    fn adopt(&mut self, volume: VolumeIndex, name: &str, carried: weaver_graph::Carried) {
        let Ok(carried) = carried.downcast::<CarriedVolume<F::Front>>() else {
            return;
        };
        self.bound(volume, name, &[], carried.length);
        if let Some(front) = carried.front {
            self.fronts.insert(volume.0, front);
        }
    }

    // The place the volume's facts give it. One that fits more than one
    // place is not claimed while the set stands: a name a recovery set
    // gives it later may order it.
    fn claim(
        &mut self,
        name: &str,
        first_bytes: &[u8],
        length: Option<u64>,
    ) -> Option<VolumeIndex> {
        let past = Placed::past(self.bound.last().copied());
        self.placed(name, first_bytes, length)?
            .claimed(self.gone, past)
    }

    fn reads_in_order(&self) -> bool {
        true
    }

    fn engine(&self) -> Option<&dyn Engine> {
        Some(self)
    }

    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        Some(self)
    }

    fn facts(&self) -> Facts {
        Facts::of(F::KIND, self)
    }
}

impl<F: Format + 'static> Engine for EngineObject<F> {
    fn start(&mut self) -> Box<dyn Extraction> {
        F::run(self.password.clone())
    }

    fn reserve(&self) -> Option<u64> {
        Some(F::reserve())
    }

    // The volumes after the one the run is on, the nearest first: weighed
    // by how many bound volumes lie between, since an index orders the
    // volumes and need not be dense.
    fn bid(&self, reached: VolumeIndex, out: &mut Vec<Seek>) {
        let after = (Bound::Excluded(reached.0), Bound::Unbounded);
        for (distance, &volume) in (1..).zip(self.bound.range(after)) {
            out.push(Seek::data(
                VolumeIndex(volume),
                ByteRange::new(0, u64::MAX),
                Weight(distance),
            ));
        }
    }
}
