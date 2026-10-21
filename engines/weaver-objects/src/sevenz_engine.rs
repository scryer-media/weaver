// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The 7z engine: conventional extraction of a 7z set on disk, whole or
// still growing, through sevenz-turbo. The run and the naming are its own;
// the object is the shared engine object.

pub mod naming;
mod run;

#[cfg(test)]
use weaver_graph::{ByteRange, Emit, Engine, MemberId, Object, Placement, VolumeIndex};
use weaver_graph::{Extraction, Kind};

use std::collections::BTreeMap;

use crate::engine::{EngineObject, Format};

pub struct SevenZip;

// A front states a place only as the first piece's signature header, which
// measures the container: the one place it is.
impl Format for SevenZip {
    const KIND: Kind = Kind::SevenZipEngine;
    type Name = naming::VolumeName;
    type Front = u64;

    fn parse(name: &str) -> Option<naming::VolumeName> {
        naming::parse(name)
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

    fn place(bound: &BTreeMap<u32, Option<&u64>>, front: &u64) -> Option<u32> {
        bound
            .get(&0)
            .copied()
            .flatten()
            .is_some_and(|first| first == front)
            .then_some(0)
    }

    fn run(password: Option<String>) -> Box<dyn Extraction> {
        Box::new(run::Run::new(password))
    }
}

pub type SevenZipEngine = EngineObject<SevenZip>;

#[cfg(test)]
mod tests;
