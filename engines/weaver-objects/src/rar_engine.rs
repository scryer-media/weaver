// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The RAR engine: conventional extraction of a RAR set on disk, whole or
// still growing, through unrar-rs. The run and the naming are its own, and
// its volumes are placed by their headers as the RAR store's are; the object
// is the shared engine object.

pub mod naming;
mod run;

use std::sync::Arc;
#[cfg(test)]
use weaver_graph::{Emit, Engine, Object, Placement};

use unrar_rs::{ArchiveFormat, KdfCache, RarVolumeFacts};
use weaver_graph::{ByteRange, Extraction, Kind, VolumeIndex};

use crate::engine::{EngineObject, Format, Holdings};
use crate::rar_store::claim::{self, Held, Holds, Places};

pub struct Rar;

impl Format for Rar {
    const KIND: Kind = Kind::RarEngine;
    type Name = naming::VolumeName;
    type Front = RarVolumeFacts;

    fn parse(name: &str) -> Option<naming::VolumeName> {
        naming::parse(name)
    }

    // Headers enciphered under a password state nothing here: the engine
    // is offered the password after it is bound. An archive that is no
    // volume is no front of a set the engine holds.
    fn front(first_bytes: &[u8]) -> Option<RarVolumeFacts> {
        claim::front_facts(first_bytes, None, Arc::new(KdfCache::new()))
            .filter(|facts| facts.is_volume)
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
        bound: &Holdings<'_, RarVolumeFacts>,
        front: Option<&RarVolumeFacts>,
        _length: Option<u64>,
    ) -> Option<VolumeIndex> {
        claim::place(front?, &Kept(bound))
    }

    fn judged(
        bound: &Holdings<'_, RarVolumeFacts>,
        front: Option<&RarVolumeFacts>,
        _length: Option<u64>,
    ) -> bool {
        front.is_some_and(|front| claim::judged(front, &Kept(bound)))
    }

    fn linked(bound: &Holdings<'_, RarVolumeFacts>, front: &RarVolumeFacts, index: u32) -> bool {
        claim::linked(front, index, &Kept(bound))
    }

    // A RAR4 volume numbers itself only in its end record, the last header
    // of the volume, past the data its front's headers name. A RAR5 volume
    // numbers itself in its main header, and a RAR 1.4 one nowhere; headers
    // enciphered past the main header say nothing here.
    fn tail(front: &RarVolumeFacts, length: u64) -> Option<ByteRange> {
        let numbered = front.archive_format() == ArchiveFormat::Rar4
            && !front.is_encrypted
            && !claim::states_position(front);
        let start = crate::rar_store::data_end(front).max(length.saturating_sub(END_MOST));
        (numbered && start < length).then(|| ByteRange::new(start, length))
    }

    fn told(front: &mut RarVolumeFacts, tail: &[u8]) {
        if let Some(number) = end_number(tail) {
            front.volume_number = Some(number);
        }
    }

    fn run(password: Option<String>) -> Box<dyn Extraction> {
        Box::new(run::Run::new(password))
    }

    // A lone archive is no volume and places nothing, but its headers state
    // its window all the same.
    fn window(first_bytes: &[u8]) -> Option<u64> {
        window(&claim::headers(
            first_bytes,
            None,
            Arc::new(KdfCache::new()),
        )?)
    }

    // A run's decoder borrows a dictionary or a PPMd arena over its window
    // from the run's lender: `crate::lent::rar`.
    fn lends() -> bool {
        true
    }
}

pub type RarEngine = EngineObject<Rar>;

// The most a RAR4 end record takes: its seven bytes, the CRC32 of the
// volume before it, its number and the seven it reserves after (UnRAR
// `arcread.cpp`, `HEAD_ENDARC`).
const END_MOST: u64 = 20;
const END: u8 = 0x7b;
const END_CRC: u16 = 0x0002;
const END_NUMBER: u16 = 0x0008;

// The number a RAR4 end record that ends `tail` states, under its own
// CRC16: `None` where no such record ends it, or it states no number.
fn end_number(tail: &[u8]) -> Option<u32> {
    let le = |at: &[u8]| u16::from_le_bytes([at[0], at[1]]);
    (7..=END_MOST as usize)
        .filter_map(|size| tail.len().checked_sub(size).map(|at| &tail[at..]))
        .find(|block| {
            block[2] == END
                && usize::from(le(&block[5..7])) == block.len()
                && crate::grid::crc32(&block[2..]) as u16 == le(&block[..2])
        })
        .and_then(|block| {
            let flags = le(&block[3..5]);
            let at = 7 + if flags & END_CRC != 0 { 4 } else { 0 };
            (flags & END_NUMBER != 0)
                .then(|| block.get(at..at + 2).map(le))
                .flatten()
                .map(u32::from)
        })
}

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

// The engine's volumes, as they place the next, read by index: a volume
// held where its own headers do not place it places nothing beside it.
struct Kept<'a>(&'a Holdings<'a, RarVolumeFacts>);

fn held<'a>(index: u32, facts: Option<&'a RarVolumeFacts>) -> Option<Held<'a>> {
    (!facts.is_some_and(|facts| claim::misplaced(VolumeIndex(index), facts))).then_some(Held {
        facts,
        successor: None,
    })
}

impl Holds for Kept<'_> {
    fn at(&self, index: u32) -> Option<Held<'_>> {
        held(index, self.0.get(index)?.front)
    }
}

// The engine files a volume as held where `held` holds it: bound, and not
// where its front says it is not (`EngineObject::held`).
impl Places for Kept<'_> {
    fn beside(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.beside()
    }

    fn held(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.held()
    }
}

#[cfg(test)]
mod tests;
