// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The 7z engine: conventional extraction of a 7z file on disk, whole or
// still growing, through sevenz-turbo. The run is its own; the object is the
// shared engine object. A 7z posted in numbered pieces is the split store's,
// which joins them into the one file this engine reads.

mod run;

#[cfg(test)]
use weaver_graph::{ByteRange, Emit, Engine, MemberId, Object, Placement};
use weaver_graph::{Extraction, Kind, VolumeIndex};

use std::collections::BTreeMap;

use crate::engine::{EngineObject, Format, Holding, SetName};

pub struct SevenZip;

// A 7z is one file, so no name numbers a volume of it.
#[derive(serde::Serialize, serde::Deserialize)]
pub enum Unnumbered {}

impl SetName for Unnumbered {
    fn index(&self) -> u32 {
        match *self {}
    }

    fn same_set(&self, _other: &Unnumbered) -> bool {
        match *self {}
    }
}

// A front states a place only as the signature header, which measures the
// container: the one place it is.
impl Format for SevenZip {
    const KIND: Kind = Kind::SevenZipEngine;
    type Name = Unnumbered;
    type Front = u64;

    fn parse(_name: &str) -> Option<Unnumbered> {
        None
    }

    fn front(first_bytes: &[u8]) -> Option<u64> {
        crate::sevenz_store::measured(first_bytes)
    }

    fn states_place(_front: &u64) -> bool {
        true
    }

    fn misplaced(_front: &u64, index: u32) -> bool {
        index != 0
    }

    // A copy of the file is the file when its signature header measures the
    // same container.
    fn place(
        bound: &BTreeMap<u32, Holding<'_, u64>>,
        front: Option<&u64>,
        _length: Option<u64>,
    ) -> Option<VolumeIndex> {
        let first = bound.get(&0)?;
        (first.front == Some(front?)).then_some(VolumeIndex(0))
    }

    fn run(password: Option<String>) -> Box<dyn Extraction> {
        Box::new(run::Run::new(password))
    }
}

pub type SevenZipEngine = EngineObject<SevenZip>;

#[cfg(test)]
mod tests;
