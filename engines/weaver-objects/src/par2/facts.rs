// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The PAR2 object's facts: each volume's walk at its last packet boundary
// and what has arrived, been lost or closed there; each set's metadata
// packets and their hashes, its recovery packets by exponent and where they
// lie, the members it binds by file and its last repair; the slice grid and
// the members' lengths; the meter, the refusal, the outcome and the limits.
//
// How a member was matched (described name, folded name, or the content of
// its first 16 KiB) is not state the object keeps: the next bind matches
// again from the namespace, which keeps every member's first bytes, and
// only a content match renames. Recovery payloads stay in the volumes and
// are read back when a repair spends them; a packet streamed when the
// checkpoint was taken, and the bytes held past the walk's front, come back
// with the server's replay of the volumes, so nothing of a hash in flight is
// recorded.

use std::collections::{BTreeMap, BTreeSet};

use par2_rs::packet::{CreatorPacket, FileDescriptionPacket, IfscPacket, MainPacket};
use par2_rs::{FileId, Packet, RecoverySetId, SliceChecksum};
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{
    ByteRange, Coverage, Durable, Facts, Kind, MemberId, Object, ObjectId, RestoreError, Sealed,
    VolumeIndex,
};

use super::Par2;
use super::repair::SetRepair;
use super::sets::{Location, SetState, Volume};
use super::walker::{Meter, Walker};
use crate::grid::Grid;

#[derive(Serialize, Deserialize)]
pub(super) struct Par2Facts {
    id: ObjectId,
    pub(super) volumes: Vec<VolumeFacts>,
    // In first-seen order.
    pub(super) sets: Vec<SetFacts>,
    grid: Grid,
    lengths: BTreeMap<MemberId, u64>,
    outcome: Option<RepairOutcome>,
    meter: Meter,
    refused: bool,
    output_limit: u64,
    memory_limit: usize,
}

#[derive(Serialize, Deserialize)]
pub(super) struct VolumeFacts {
    front: u64,
    walk_lost: Vec<ByteRange>,
    arrived: Coverage,
    lost: Vec<ByteRange>,
    closed: bool,
    named_blocks: Option<u64>,
    walked_blocks: u64,
    sets: BTreeSet<[u8; 16]>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct SetFacts {
    key: [u8; 16],
    first_seen: (VolumeIndex, u64),
    packets: Sealed<Packets>,
    hashes: BTreeSet<[u8; 16]>,
    recovery: BTreeMap<u32, Location>,
    pub(super) members: Vec<([u8; 16], MemberId)>,
    last: Option<SetRepair>,
}

// The metadata packets the walk keeps, field for field, so the set's index
// is rebuilt by par2-rs from the same packets.
#[derive(Serialize, Deserialize)]
enum PacketFacts {
    Main {
        set: [u8; 16],
        slice_size: u64,
        recovery: Vec<[u8; 16]>,
        other: Vec<[u8; 16]>,
    },
    File {
        file: [u8; 16],
        hash_full: [u8; 16],
        hash_16k: [u8; 16],
        length: u64,
        name: String,
        par2_name: String,
    },
    Checksums {
        file: [u8; 16],
        slices: Vec<(u32, [u8; 16])>,
    },
    Creator(String),
}

// A set's metadata packets, as the set keeps them. They encode as their
// facts, so the set's index is rebuilt by par2-rs from the same packets.
#[derive(Clone, Debug, Default)]
pub(super) struct Packets(pub(super) Vec<Packet>);

impl Serialize for Packets {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().filter_map(PacketFacts::of))
    }
}

impl<'de> Deserialize<'de> for Packets {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let kept = Vec::<PacketFacts>::deserialize(deserializer)?;
        Ok(Self(kept.into_iter().map(PacketFacts::packet).collect()))
    }
}

fn ids(files: &[FileId]) -> Vec<[u8; 16]> {
    files.iter().map(|file| *file.as_bytes()).collect()
}

fn files(ids: Vec<[u8; 16]>) -> Vec<FileId> {
    ids.into_iter().map(FileId::from_bytes).collect()
}

impl PacketFacts {
    // Only metadata reaches a set; anything else has no place in one.
    fn of(packet: &Packet) -> Option<Self> {
        Some(match packet {
            Packet::Main(main) => Self::Main {
                set: *main.recovery_set_id.as_bytes(),
                slice_size: main.slice_size,
                recovery: ids(&main.recovery_file_ids),
                other: ids(&main.non_recovery_file_ids),
            },
            Packet::FileDescription(file) => Self::File {
                file: *file.file_id.as_bytes(),
                hash_full: file.hash_full,
                hash_16k: file.hash_16k,
                length: file.file_length,
                name: file.filename.clone(),
                par2_name: file.par2_name.clone(),
            },
            Packet::InputFileSliceChecksum(ifsc) => Self::Checksums {
                file: *ifsc.file_id.as_bytes(),
                slices: ifsc
                    .checksums
                    .iter()
                    .map(|slice| (slice.crc32, slice.md5))
                    .collect(),
            },
            Packet::Creator(creator) => Self::Creator(creator.creator_id.clone()),
            Packet::RecoverySlice(_) | Packet::Unknown { .. } => return None,
        })
    }

    fn packet(self) -> Packet {
        match self {
            Self::Main {
                set,
                slice_size,
                recovery,
                other,
            } => Packet::Main(MainPacket {
                recovery_set_id: RecoverySetId::from_bytes(set),
                slice_size,
                recovery_file_ids: files(recovery),
                non_recovery_file_ids: files(other),
            }),
            Self::File {
                file,
                hash_full,
                hash_16k,
                length,
                name,
                par2_name,
            } => Packet::FileDescription(FileDescriptionPacket {
                file_id: FileId::from_bytes(file),
                hash_full,
                hash_16k,
                file_length: length,
                filename: name,
                par2_name,
            }),
            Self::Checksums { file, slices } => Packet::InputFileSliceChecksum(IfscPacket {
                file_id: FileId::from_bytes(file),
                checksums: slices
                    .into_iter()
                    .map(|(crc32, md5)| SliceChecksum { crc32, md5 })
                    .collect(),
            }),
            Self::Creator(creator_id) => Packet::Creator(CreatorPacket { creator_id }),
        }
    }
}

impl Durable for Par2Facts {
    // 2: each set's packets sealed, nested as the bytes they encoded to.
    // 3: sealed parts kept apart from the facts' bytes, and the grid
    // shelved by chunk of blocks.
    const VERSION: u16 = 3;

    fn impossible(&self) -> Option<&'static str> {
        let keys: BTreeSet<_> = self.sets.iter().map(|set| set.key).collect();
        if keys.len() != self.sets.len() {
            return Some("a recovery set recorded twice");
        }
        if self.refused && !self.sets.is_empty() {
            return Some("a refused object that kept its sets");
        }
        let count = self.volumes.len() as u32;
        let placed = self.sets.iter().all(|set| {
            set.first_seen.0.0 < count
                && set
                    .recovery
                    .values()
                    .all(|at| at.volume.0 < count && !at.range.is_empty())
        });
        (!placed).then_some("a packet in no volume")
    }
}

pub(super) fn of(par2: &Par2) -> Facts {
    let volumes = par2
        .volumes
        .iter()
        .map(|volume| VolumeFacts {
            front: volume.walker.front(),
            walk_lost: volume.walker.lost().to_vec(),
            arrived: volume.arrived.clone(),
            lost: volume.lost.clone(),
            closed: volume.closed,
            named_blocks: volume.named_blocks,
            walked_blocks: volume.walked_blocks,
            sets: volume.sets.clone(),
        })
        .collect();
    let sets = par2
        .ordered()
        .into_iter()
        .map(|key| {
            let state = &par2.sets[&key];
            SetFacts {
                key,
                first_seen: state.first_seen,
                packets: state.packets().clone(),
                hashes: state.hashes().clone(),
                recovery: state.recovery.clone(),
                members: state
                    .members()
                    .iter()
                    .map(|(file, member)| (*file.as_bytes(), *member))
                    .collect(),
                last: state.last,
            }
        })
        .collect();
    let facts = Par2Facts {
        id: par2.id,
        volumes,
        sets,
        grid: par2.grid.clone(),
        lengths: par2.lengths.clone(),
        outcome: par2.outcome,
        meter: par2.meter.clone(),
        refused: par2.refused,
        output_limit: par2.output_limit,
        memory_limit: par2.memory_limit,
    };
    Facts::of(Kind::Par2, &facts)
}

pub(super) fn par2(facts: &Facts) -> Result<Par2, RestoreError> {
    let kept: Par2Facts = facts.read(Kind::Par2)?;
    let mut par2 = Par2::new(kept.id);
    par2.volumes = kept
        .volumes
        .into_iter()
        .map(|volume| Volume {
            walker: Walker::resumed(volume.front, volume.walk_lost),
            arrived: volume.arrived,
            lost: volume.lost,
            closed: volume.closed,
            named_blocks: volume.named_blocks,
            walked_blocks: volume.walked_blocks,
            sets: volume.sets,
        })
        .collect();
    for set in kept.sets {
        let mut state =
            SetState::restored(set.first_seen, set.packets, set.hashes, kept.output_limit);
        state.recovery = set.recovery;
        state.bind(
            set.members
                .into_iter()
                .map(|(file, member)| (FileId::from_bytes(file), member))
                .collect(),
        );
        state.last = set.last;
        par2.sets.insert(set.key, state);
    }
    par2.grid = kept.grid;
    par2.lengths = kept.lengths;
    par2.outcome = kept.outcome;
    par2.meter = kept.meter;
    par2.refused = kept.refused;
    par2.output_limit = kept.output_limit;
    par2.memory_limit = kept.memory_limit;
    for key in par2.ordered() {
        par2.fold_set(key);
    }
    Ok(par2)
}

pub(crate) fn restore(facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
    Ok(Box::new(par2(facts)?))
}
