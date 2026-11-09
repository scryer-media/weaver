// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The RAR engine: conventional extraction of a RAR set on disk, whole or
// still growing, through unrar-rs. The run and the naming are its own, and
// its volumes are placed by their headers as the RAR store's are; the object
// is the shared engine object.

pub mod naming;
mod run;

use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(test)]
use weaver_graph::{ByteRange, Emit, Engine, Object, Placement};

use unrar_rs::{KdfCache, RarVolumeFacts};
use weaver_graph::{Extraction, Kind, VolumeIndex};

use crate::engine::{EngineObject, Format, Holding};
use crate::rar_store::claim::{self, Held};

pub struct Rar;

impl Format for Rar {
    const KIND: Kind = Kind::RarEngine;
    type Name = naming::VolumeName;
    type Front = RarVolumeFacts;

    fn parse(name: &str) -> Option<naming::VolumeName> {
        naming::parse(name)
    }

    // Headers enciphered under a password state nothing here: the engine
    // is offered the password after it is bound.
    fn front(first_bytes: &[u8]) -> Option<RarVolumeFacts> {
        claim::front_facts(first_bytes, None, Arc::new(KdfCache::new()))
    }

    fn states_place(front: &RarVolumeFacts) -> bool {
        claim::states_position(front)
    }

    fn misplaced(front: &RarVolumeFacts, index: u32) -> bool {
        claim::misplaced(VolumeIndex(index), front)
    }

    // A front states nothing of whether a volume follows it: that is in the
    // end record, past the front of all but the smallest volume.
    fn place(
        bound: &BTreeMap<u32, Holding<'_, RarVolumeFacts>>,
        front: Option<&RarVolumeFacts>,
        _length: Option<u64>,
    ) -> Option<VolumeIndex> {
        claim::place(front?, &held(bound))
    }

    fn judged(
        bound: &BTreeMap<u32, Holding<'_, RarVolumeFacts>>,
        front: Option<&RarVolumeFacts>,
        _length: Option<u64>,
    ) -> bool {
        front.is_some_and(|front| claim::judged(front, &held(bound)))
    }

    fn run(password: Option<String>) -> Box<dyn Extraction> {
        Box::new(run::Run::new(password))
    }

    fn window(first_bytes: &[u8]) -> Option<u64> {
        window(&Self::front(first_bytes)?)
    }
}

pub type RarEngine = EngineObject<Rar>;

// The widest window a decode of the compressed members a front's headers
// name will hold: the dictionary each header declares, or the member's whole
// unpacked size where that is less. A solid archive keeps one window across
// its members, whose sum no single front states, so it holds the whole
// dictionary. A RAR3 PPMd model's size is in the packed data, not a header,
// and members whose headers lie past the front, or under a password, state
// nothing here.
pub(crate) fn window(facts: &RarVolumeFacts) -> Option<u64> {
    facts
        .members
        .iter()
        .filter(|member| !member.is_directory && member.compression_method != 0)
        .filter(|member| member.dict_size != 0)
        .map(|member| {
            if facts.is_solid || member.compression_solid {
                member.dict_size
            } else {
                member
                    .unpacked_size
                    .map_or(member.dict_size, |size| size.min(member.dict_size))
            }
        })
        .max()
}

fn held<'a>(bound: &BTreeMap<u32, Holding<'a, RarVolumeFacts>>) -> BTreeMap<u32, Held<'a>> {
    bound
        .iter()
        .filter_map(|(index, holding)| {
            let facts = holding.front;
            (!facts.is_some_and(|facts| claim::misplaced(VolumeIndex(*index), facts))).then_some((
                *index,
                Held {
                    facts,
                    successor: None,
                },
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests;
