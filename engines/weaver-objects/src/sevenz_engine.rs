// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The 7z engine: conventional extraction of a 7z set on disk, whole or
// still growing, through sevenz-turbo. The run and the naming are its own;
// the object is the shared engine object.

mod geometry;
pub mod naming;
mod run;

#[cfg(test)]
use weaver_graph::{ByteRange, Emit, Engine, MemberId, Object, Placement};
use weaver_graph::{Extraction, Kind, VolumeIndex};

use std::collections::BTreeMap;

use crate::engine::{EngineObject, Format, Holding, Placed};

pub struct SevenZip;

// A front states a place only as the first piece's signature header, which
// measures the container: the one place it is. Every other piece is placed
// by its name, else by its length.
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

    // A piece with no front of its own is placed by its length, as the
    // store places it, among the places the first piece measures, held or
    // not: the tree judges a held place by fronts.
    fn place(
        bound: &BTreeMap<u32, Holding<'_, u64>>,
        front: Option<&u64>,
        length: Option<u64>,
    ) -> Option<Placed> {
        let first = bound.get(&0)?;
        match front {
            Some(front) => (first.front == Some(front)).then_some(Placed::At(VolumeIndex(0))),
            None => crate::sevenz_store::fits(first.length?, *first.front?, length?),
        }
    }

    fn run(password: Option<String>) -> Box<dyn Extraction> {
        Box::new(run::Run::new(password))
    }
}

pub type SevenZipEngine = EngineObject<SevenZip>;

#[cfg(test)]
mod tests;
