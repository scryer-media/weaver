// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The engine object, shared by every engine format: conventional
// extraction of a set that sits on disk as files, whole or still growing.
// The object itself keeps almost nothing: the set's name, so it can claim
// the set's other volumes, which volumes it has, a password the job
// offered, and how far the run has read, so it can bid for the volumes the
// run reaches next. Every byte of its volumes is envelope, kept by the
// owner; the run reads them back. A format supplies only how its volumes
// are named and what a run of it is.

mod joined;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Engine, Extent, Extraction, Facts, Kind, MemberId, Object,
    Placement, Pull, Seek, VolumeIndex, Weight,
};

pub(crate) use joined::Joined;

// What one engine format knows: its kind, how its set names its volumes,
// and the run that extracts it.
pub trait Format: Send + Sync {
    const KIND: Kind;
    type Name: SetName;
    fn parse(name: &str) -> Option<Self::Name>;
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

// Its facts are the volumes it has and how far its run read. Never the
// password, which the owner offers again, and never its bids, which a
// restart makes again: a restored engine is one whose run has not started.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct EngineObject<F: Format> {
    #[serde(skip)]
    format: PhantomData<F>,
    // The names of the volumes bound that name a volume of a set, which
    // every other volume's must share.
    names: BTreeMap<u32, F::Name>,
    bound: BTreeSet<u32>,
    #[serde(skip)]
    password: Option<String>,
    // The furthest volume the run has read.
    reached: Option<VolumeIndex>,
}

impl<F: Format> Durable for EngineObject<F> {
    const VERSION: u16 = 1;

    fn impossible(&self) -> Option<&'static str> {
        let named = self.names.keys().all(|volume| self.bound.contains(volume));
        (!named).then_some("a volume named that is not bound")
    }
}

impl<F: Format> Default for EngineObject<F> {
    fn default() -> Self {
        Self {
            format: PhantomData,
            names: BTreeMap::new(),
            bound: BTreeSet::new(),
            password: None,
            reached: None,
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
    fn deliver(&mut self, _delivery: Delivery<'_>, _out: &mut Vec<Emit>) {}

    fn volume_closed(&mut self, _volume: VolumeIndex, _out: &mut Vec<Emit>) {}

    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    // The volumes after the one the run is on, the nearest first.
    fn wants(&mut self, out: &mut Vec<Seek>) {
        let Some(reached) = self.reached else {
            return;
        };
        for &volume in self.bound.range(reached.0 + 1..) {
            out.push(Seek::data(
                VolumeIndex(volume),
                ByteRange::new(0, u64::MAX),
                Weight(volume - reached.0),
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
    }

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str) {
        let first = self.names.values().next();
        if let Some(parsed) = F::parse(name)
            && first.is_none_or(|first| first.same_set(&parsed))
        {
            self.names.insert(volume.0, parsed);
        }
        self.bound.insert(volume.0);
    }

    fn claim(&mut self, name: &str, _first_bytes: &[u8]) -> Option<VolumeIndex> {
        let candidate = F::parse(name)?;
        let first = self.names.values().next()?;
        let index = first
            .same_set(&candidate)
            .then(|| candidate.slot(&self.names))??;
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
}
