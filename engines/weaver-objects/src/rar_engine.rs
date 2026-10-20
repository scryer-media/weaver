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

use crate::engine::{EngineObject, Format};
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
        bound: &BTreeMap<u32, Option<&RarVolumeFacts>>,
        front: &RarVolumeFacts,
    ) -> Option<u32> {
        let held = bound
            .iter()
            .map(|(index, facts)| {
                let held = Held {
                    facts: *facts,
                    successor: None,
                };
                (*index, held)
            })
            .collect();
        claim::place(front, &held)
    }

    fn run(password: Option<String>) -> Box<dyn Extraction> {
        Box::new(run::Run::new(password))
    }
}

pub type RarEngine = EngineObject<Rar>;

#[cfg(test)]
mod tests;
