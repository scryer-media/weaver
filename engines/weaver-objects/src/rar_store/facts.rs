// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The RAR store's facts: each volume as its owner bound it (its index is
// how a nameless volume was chained), the header bytes it kept, and what
// unrar-rs read from them (members and their extents, the solid flag, the
// end record, the recovery record's range); each member as it was
// announced, its pieces across the volumes and what was said of it; and
// what was said once. An encrypted member records its key tuple as the
// header states it (salt, KDF count, IV, check value), never a key: it
// comes back locked and opens again on the password the server offers.
//
// Restore walks the kept header bytes again and requires them to read as
// recorded, so the layout is rebuilt rather than trusted. A member's CRC
// runs and an open cipher's seeds are rebuilt by the server's replay of
// the placed ranges, which every restart does; the cipher blocks it held,
// parked or partial, are on no disk, so its chain is kept, never its key. What came out of the cipher
// is the member's coverage, which the cipher reads; the one thing coverage
// cannot say, the plaintext past the payload, is the member's padding.

use std::collections::BTreeSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use unrar_rs::{
    MemberKeying, RarVolumeFacts, RarVolumeHeaderEncryption, RarVolumeMemberEncryptionFacts,
};
use weaver_graph::{ByteRange, Coverage, Durable, Facts, Kind, Object, RestoreError, VolumeIndex};

use super::walk::Block;
use super::{Entry, Lock, Out, Piece, RarStore, Volume};
use crate::crypt::{Chain, Padding};
use crate::kept::Kept;
use crate::runs::CrcRuns;

#[derive(Serialize, Deserialize)]
pub(super) struct StoreFacts {
    volumes: Vec<VolumeFacts>,
    pub(super) entries: Vec<EntryFacts>,
    asked: bool,
    damage_said: BTreeSet<(VolumeIndex, u64, u64, u32)>,
    recovery_said: BTreeSet<(VolumeIndex, u64)>,
    gaps_said: BTreeSet<VolumeIndex>,
    gone: bool,
}

#[derive(Serialize, Deserialize)]
struct VolumeFacts {
    index: VolumeIndex,
    name: Option<String>,
    length: Option<u64>,
    closed: bool,
    kept: Kept,
    headers: Option<RarVolumeFacts>,
    ended: bool,
    locked: Option<RarVolumeHeaderEncryption>,
    // What the front stated when it was bound, which can precede the walk's
    // first header read.
    front: Option<RarVolumeFacts>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct EntryFacts {
    name: String,
    length: Option<u64>,
    crc: Option<u32>,
    uses_mac: bool,
    pieces: Vec<PieceFacts>,
    complete: bool,
    pub(super) keying: Option<KeyTuple>,
    coverage: Coverage,
    closed: bool,
    padding: Padding,
    // An open cipher's blocks, never its key.
    chain: Option<Chain>,
}

#[derive(Serialize, Deserialize)]
struct PieceFacts {
    volume: VolumeIndex,
    data: ByteRange,
    at: u64,
    crc: Option<u32>,
    uses_mac: bool,
    after: bool,
}

// The header's own statement of how a member is keyed.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum KeyTuple {
    Rar5(RarVolumeMemberEncryptionFacts),
    Rar4(Option<[u8; 8]>),
}

impl From<MemberKeying> for KeyTuple {
    fn from(keying: MemberKeying) -> Self {
        match keying {
            MemberKeying::Rar5(facts) => Self::Rar5(facts),
            MemberKeying::Rar4 { salt } => Self::Rar4(salt),
        }
    }
}

impl Durable for StoreFacts {
    // 2: the front each volume stated when it was bound.
    // 3: a kept cipher block says whether an arrival started inside it.
    const VERSION: u16 = 3;

    fn impossible(&self) -> Option<&'static str> {
        let indices: BTreeSet<_> = self.volumes.iter().map(|volume| volume.index).collect();
        if indices.len() != self.volumes.len() {
            return Some("a volume recorded twice");
        }
        if !self.volumes.iter().all(|volume| volume.kept.sound()) {
            return Some("kept header bytes that overlap");
        }
        let placed = self.entries.iter().all(|entry| {
            entry.pieces.windows(2).all(|pair| pair[0].at < pair[1].at)
                && entry
                    .pieces
                    .iter()
                    .all(|piece| indices.contains(&piece.volume))
        });
        (!placed).then_some("a member piece out of order or in no volume")
    }
}

pub(super) fn of(store: &RarStore) -> Facts {
    let volumes = store
        .volumes
        .iter()
        .map(|(index, volume)| VolumeFacts {
            index: *index,
            name: volume.name.clone(),
            length: volume.length,
            closed: volume.closed,
            kept: volume.kept.clone(),
            headers: volume.walk.facts.clone(),
            ended: volume.walk.ended,
            locked: volume.walk.locked,
            front: volume.front.clone(),
        })
        .collect();
    let entries = store
        .entries
        .iter()
        .map(|entry| EntryFacts {
            name: entry.name.clone(),
            length: entry.length,
            crc: entry.crc,
            uses_mac: entry.uses_mac,
            pieces: entry
                .pieces
                .iter()
                .map(|piece| PieceFacts {
                    volume: piece.volume,
                    data: piece.data,
                    at: piece.at,
                    crc: piece.crc,
                    uses_mac: piece.uses_mac,
                    after: piece.after,
                })
                .collect(),
            complete: entry.complete,
            keying: keying(entry),
            coverage: entry.out.coverage.clone(),
            closed: entry.out.closed,
            padding: entry.out.padding.clone(),
            chain: match &entry.lock {
                Lock::Open(cbc) => Some(cbc.chain()),
                Lock::Sealed(chain) => Some(Chain::clone(chain)),
                Lock::Plain | Lock::Locked => None,
            },
        })
        .collect();
    let facts = StoreFacts {
        volumes,
        entries,
        asked: store.asked,
        damage_said: store.damage_said.iter().copied().collect(),
        recovery_said: store.recovery_said.clone(),
        gaps_said: store.gaps_said.clone(),
        gone: store.gone,
    };
    Facts::of(Kind::RarStore, &facts)
}

fn keying(entry: &Entry) -> Option<KeyTuple> {
    entry.store.map(|store| store.keying().into())
}

pub(crate) fn restore(facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
    let kept: StoreFacts = facts.read(Kind::RarStore)?;
    let mut store = RarStore::new();
    for found in kept.volumes {
        let mut volume = Volume::new();
        if let Some(name) = &found.name {
            volume.name_as(name);
        }
        volume.length = found.length;
        volume.closed = found.closed;
        volume.kept = found.kept;
        volume.front = found.front;
        volume.walk.blocks = found.headers.as_ref().map_or_else(Vec::new, |headers| {
            headers
                .members
                .iter()
                .map(|member| Block {
                    data: ByteRange::at(member.data_offset, member.data_size),
                })
                .collect()
        });
        volume.walk.facts = found.headers;
        volume.walk.ended = found.ended;
        volume.walk.locked = found.locked;
        store.volumes.insert(found.index, volume);
        // The walk reads the kept headers again at the look below.
        store.file(found.index);
        store.touch(found.index);
    }
    let keyings: Vec<_> = kept.entries.iter().map(|entry| entry.keying).collect();
    store.entries = kept.entries.into_iter().map(entry).collect();
    store.remap();
    store.damage_said = kept.damage_said.into_iter().collect();
    store.recovery_said = kept.recovery_said;
    store.gaps_said = kept.gaps_said;
    store.gone = kept.gone;
    // The walk reads the kept headers again; what it says now was said
    // before the checkpoint, so it goes nowhere.
    store.settle(None, &mut Vec::new());
    // Nor is a question it asked while no password was on hand yet: the
    // server offers the winning one again, and one never asked is asked
    // by the next look.
    store.asked = kept.asked;
    let agrees = store.entries.len() == keyings.len()
        && store
            .entries
            .iter()
            .zip(&keyings)
            .all(|(entry, recorded)| keying(entry) == *recorded);
    if !agrees && !store.gone {
        return Err(RestoreError::Impossible {
            kind: Kind::RarStore,
            reason: "kept headers that no longer read as recorded",
        });
    }
    Ok(Box::new(store))
}

fn entry(facts: EntryFacts) -> Entry {
    Entry {
        name: facts.name,
        length: facts.length,
        crc: facts.crc,
        uses_mac: facts.uses_mac,
        store: None,
        pieces: facts
            .pieces
            .into_iter()
            .map(|piece| Piece {
                volume: piece.volume,
                data: piece.data,
                at: piece.at,
                crc: piece.crc,
                uses_mac: piece.uses_mac,
                after: piece.after,
            })
            .collect(),
        complete: facts.complete,
        lock: match (facts.keying, facts.chain) {
            (Some(_), Some(chain)) => Lock::Sealed(Box::new(chain)),
            (Some(_), None) => Lock::Locked,
            (None, _) => Lock::Plain,
        },
        hash_key: None,
        trial: None,
        out: Arc::new(Out {
            coverage: facts.coverage,
            closed: facts.closed,
            padding: facts.padding,
        }),
        plain_runs: CrcRuns::default(),
        parts: Default::default(),
    }
}
