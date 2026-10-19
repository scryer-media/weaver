// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The engine object, shared by every engine format: conventional
// extraction of a set that sits on disk as files, whole or still growing.
// The object itself keeps almost nothing: the set's names, and what its
// volumes' fronts state of their places, so it can claim the set's other
// volumes, which volumes it has, a password the job offered, and how far
// the run has read, so it can bid for the volumes the run reaches next.
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
    ByteRange, Delivery, Durable, Emit, Engine, Extent, Extraction, Facts, Kind, MemberId, Object,
    Placement, Pull, Seek, Standing, VolumeIndex, Weight, Why,
};

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
    // The index the fronts of the volumes bound give a volume whose front
    // is `front`: each bound volume by index, with its front when it states
    // one.
    fn place(_bound: &BTreeMap<u32, Option<&Self::Front>>, _front: &Self::Front) -> Option<u32> {
        None
    }
    fn run(password: Option<String>) -> Box<dyn Extraction>;
}

// A volume's name as its set numbers it.
// Serde, because the names bound are an engine's facts.
pub trait SetName: Send + Sync + Sized + Serialize + DeserializeOwned {
    // The volume this name is, counted from zero.
    fn index(&self) -> u32;
    fn same_set(&self, other: &Self) -> bool;

    // The volume this name becomes beside the volumes of its set already
    // bound, by index, or `None` when its place is taken. A name is its own
    // number; a set whose last volume carries none says otherwise.
    fn slot(&self, bound: &BTreeMap<u32, Self>) -> Option<u32> {
        let index = self.index();
        (!bound.contains_key(&index)).then_some(index)
    }
}

// Its facts are the volumes it has, by name and front. Never the password, which the owner
// offers again, and never how far its run read, which is a run's live
// progress and no durable byte: a restored engine is one whose run has not
// started, and the run it starts tells it again where it is.
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
    bound: BTreeSet<u32>,
    #[serde(skip)]
    password: Option<String>,
    // The furthest volume the run has read.
    #[serde(skip)]
    reached: Option<VolumeIndex>,
    // The run asked for a password and no offer has answered it since.
    #[serde(skip)]
    asking: bool,
}

impl<F: Format> Durable for EngineObject<F> {
    const VERSION: u16 = 2;

    fn impossible(&self) -> Option<&'static str> {
        let known = self.names.keys().chain(self.fronts.keys());
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
            bound: BTreeSet::new(),
            password: None,
            reached: None,
            asking: false,
        }
    }
}

impl<F: Format> EngineObject<F> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<F: Format + 'static> Object for EngineObject<F> {
    fn kind(&self) -> Kind {
        F::KIND
    }

    // The run reads the volumes from the owner's files; the bytes on their
    // way there say nothing the run does not read itself.
    fn deliver(
        &mut self,
        _delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _out: &mut Vec<Emit>,
    ) {
    }

    fn volume_closed(&mut self, _volume: VolumeIndex, _out: &mut Vec<Emit>) {}

    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    // Nothing more will reach the run. A member it left damaged, a password
    // it is still asking for, a member it named and never closed: the set
    // cannot be read whole, and its volumes go back as they stand.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        let why = crate::quiet::damaged(standing)
            .or_else(|| self.asking.then_some(Why::EncryptedHeader))
            .or_else(|| crate::quiet::unplaced(standing));
        out.extend(why.map(Emit::Refuse));
    }

    // The volumes after the one the run is on, the nearest first: weighed
    // by how many bound volumes lie between, since an index orders the
    // volumes and need not be dense.
    fn wants(&mut self, out: &mut Vec<Seek>) {
        let Some(reached) = self.reached else {
            return;
        };
        let after = (Bound::Excluded(reached.0), Bound::Unbounded);
        for (distance, &volume) in (1..).zip(self.bound.range(after)) {
            out.push(Seek::data(
                VolumeIndex(volume),
                ByteRange::new(0, u64::MAX),
                Weight(distance),
            ));
        }
    }

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) {}

    fn offer_password(&mut self, secret: &str, _out: &mut Vec<Emit>) {
        self.password = Some(secret.to_owned());
        self.asking = false;
    }

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, front: &[u8]) {
        let first = self.names.values().next();
        if let Some(parsed) = F::parse(name)
            && first.is_none_or(|first| first.same_set(&parsed))
        {
            self.names.insert(volume.0, parsed);
        }
        if let Some(front) = F::front(front) {
            self.fronts.insert(volume.0, front);
        }
        self.bound.insert(volume.0);
    }

    // The name a recovery set gave the volume is the set's numbering now.
    fn renamed(&mut self, volume: VolumeIndex, name: &str) {
        self.names.remove(&volume.0);
        let first = self.names.values().next();
        if let Some(parsed) = F::parse(name)
            && first.is_none_or(|first| first.same_set(&parsed))
        {
            self.names.insert(volume.0, parsed);
        }
    }

    // A volume's place is what states it, as the RAR store places one: the
    // number its front gives, beside what the fronts of the volumes bound
    // state; else its name in the set's own numbering; and only a volume
    // that states neither by the fronts it links to beside it, which guess
    // wrong across a volume not yet bound. A posted name is a label, which
    // a recovery set may change later, so a stated number outranks it.
    fn claim(&mut self, name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        let bound = self
            .bound
            .iter()
            .map(|volume| (*volume, self.fronts.get(volume)))
            .collect();
        let front = F::front(first_bytes);
        let stated = front.as_ref().is_some_and(F::states_place);
        let placed = front.and_then(|front| F::place(&bound, &front));
        let named = F::parse(name)
            .filter(|candidate| {
                self.names
                    .values()
                    .next()
                    .is_some_and(|first| first.same_set(candidate))
            })
            .and_then(|candidate| candidate.slot(&self.names));
        let index = match (placed, named) {
            (Some(index), _) if stated => index,
            (_, Some(named)) => named,
            (linked, None) => linked?,
        };
        (!self.bound.contains(&index)).then_some(VolumeIndex(index))
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
    fn start(&self) -> Box<dyn Extraction> {
        F::run(self.password.clone())
    }

    fn reached(&mut self, volume: VolumeIndex, _offset: u64) {
        if self.reached.is_none_or(|reached| volume > reached) {
            self.reached = Some(volume);
        }
    }

    fn asked(&mut self) {
        self.asking = true;
    }
}
