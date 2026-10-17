// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The PAR3 object. Same shape as the PAR2 one: built over the first .par3
// member, it claims the others as further volumes, and the namespace keeps
// every volume's bytes in the member's own file.
//
// It declines the segment feed. A PAR3 block is checked by a BLAKE3
// fingerprint, not a CRC32, so the segment checksums fold into nothing it
// can compare; it reads members back to verify them instead. It keeps the
// front of each volume to learn early which members it describes, and reads
// every arrived run of each volume back when it repairs. Only packets that
// lie wholly in a volume's readable ranges count: one a hole cuts is
// dropped, and the scan carries on past it.

pub(crate) mod facts;
mod rebuild;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use bytes::Bytes;
use par3_rs::{MAGIC, Packet, Par3Set};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Extent, Kind, MemberId, Namespace, Object, ObjectId,
    Placement, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex, Weight,
};

use crate::par2::combine;

// How much of each volume's front is kept to read the index from. An index
// file is metadata only; a recovery volume repeats it ahead of its blocks.
const FRONT_CAP: usize = 1024 * 1024;
// What one job may carry.
const MAX_CARRIERS: usize = 4096;
const MAX_SETS: usize = 256;

#[derive(Debug, Default)]
struct Volume {
    front: Vec<u8>,
    arrived: Coverage,
    closed: bool,
    // Nothing more of the volume will come.
    lost: bool,
    asked: bool,
}

pub struct Par3 {
    id: ObjectId,
    volumes: Vec<Volume>,
    // Described paths and the member each is bound to.
    bound: BTreeMap<String, MemberId>,
    // Damaged blocks the last repair found while it waited.
    needed: u64,
    // What the last repair came to, for the members bound then.
    outcome: Option<RepairOutcome>,
}

impl Par3 {
    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            volumes: vec![Volume::default()],
            bound: BTreeMap::new(),
            needed: 0,
            outcome: None,
        }
    }

    fn volume(&mut self, index: VolumeIndex) -> &mut Volume {
        let at = index.0 as usize;
        if self.volumes.len() <= at {
            self.volumes.resize_with(at + 1, Volume::default);
        }
        &mut self.volumes[at]
    }

    // The sets as far as the kept fronts describe them.
    fn front_sets(&self) -> Vec<Par3Set> {
        sets(
            self.volumes
                .iter()
                .map(|volume| (volume.front.as_slice(), &volume.arrived)),
        )
    }

    fn coming(&self) -> bool {
        self.volumes
            .iter()
            .any(|volume| !volume.closed && !volume.lost)
    }
}

// Every set the volumes' packets describe, from the packets that lie wholly
// in a readable range; a volume that fails to scan contributes nothing, and
// a job of more sets than the limit has none.
fn sets<'a>(volumes: impl Iterator<Item = (&'a [u8], &'a Coverage)>) -> Vec<Par3Set> {
    let mut packets: Vec<Packet> = Vec::new();
    for (bytes, readable) in volumes {
        let Ok(found) = par3_rs::scan_packets(bytes) else {
            continue;
        };
        packets.extend(
            found
                .into_iter()
                .filter(|(at, packet)| readable.covered(ByteRange::at(*at, packet.len())))
                .map(|(_, packet)| packet),
        );
    }
    let sets = Par3Set::from_packets(packets).unwrap_or_default();
    if sets.len() > MAX_SETS {
        return Vec::new();
    }
    sets
}

impl Object for Par3 {
    fn kind(&self) -> Kind {
        Kind::Par3
    }

    fn deliver(&mut self, delivery: Delivery<'_>, _out: &mut Vec<Emit>) {
        let volume = self.volume(delivery.volume);
        volume.arrived.write(delivery.range(), delivery.provenance);
        let Ok(offset) = usize::try_from(delivery.offset) else {
            return;
        };
        // Bytes past a hole are kept past zeroes, which no packet hash
        // passes.
        if offset >= FRONT_CAP {
            return;
        }
        let end = offset.saturating_add(delivery.bytes.len()).min(FRONT_CAP);
        if end > volume.front.len() {
            volume.front.resize(end, 0);
        }
        volume.front[offset..end].copy_from_slice(&delivery.bytes[..end - offset]);
    }

    fn volume_closed(&mut self, volume: VolumeIndex, _out: &mut Vec<Emit>) {
        self.volume(volume).closed = true;
    }

    // A loss that runs to the end of the volume ends it; any other leaves a
    // hole in what arrived.
    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, _out: &mut Vec<Emit>) {
        if range.end == u64::MAX {
            self.volume(volume).lost = true;
        }
    }

    fn wants(&mut self, out: &mut Vec<Seek>) {
        Recovery::wants(self, out);
    }

    fn pulled(&mut self, seek: Seek, pull: Pull, out: &mut Vec<Emit>) {
        if pull == Pull::Lost {
            self.volume_lost(seek.volume, seek.range, out);
        }
    }

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) {}

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn claim(&mut self, _name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        if first_bytes.get(..MAGIC.len()) != Some(MAGIC.as_slice())
            || self.volumes.len() >= MAX_CARRIERS
        {
            return None;
        }
        self.volumes.push(Volume::default());
        Some(VolumeIndex((self.volumes.len() - 1) as u32))
    }

    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }

    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }
}

impl Recovery for Par3 {
    // By path or bare name, and only a unique match binds.
    fn bind(&mut self, namespace: &mut Namespace, _renamed: &mut Vec<MemberId>) {
        let mut bound = BTreeMap::new();
        for set in self.front_sets() {
            for file in set.files() {
                let mut matches = namespace.members().filter(|member| {
                    member.bound_to != Some(self.id)
                        && (member.name == file.path() || member.name == file.name())
                });
                if let (Some(member), None) = (matches.next(), matches.next()) {
                    bound.insert(file.path().to_owned(), member.id);
                }
            }
        }
        if bound != self.bound {
            self.outcome = None;
        }
        self.bound = bound;
    }

    fn covers(&self, member: MemberId) -> bool {
        self.bound.values().any(|bound| *bound == member)
    }

    fn feed(&mut self, _member: MemberId, _segments: &[Segment]) {}

    // Nothing is known before a readback.
    fn verdicts(&self, _member: MemberId, _out: &mut Vec<SliceVerdict>) {}

    fn wants(&mut self, out: &mut Vec<Seek>) {
        if self.needed == 0 {
            return;
        }
        for (index, volume) in self.volumes.iter_mut().enumerate() {
            if volume.closed || volume.lost || volume.asked {
                continue;
            }
            volume.asked = true;
            let start = volume.arrived.runs().next().map_or(0, |run| {
                if run.range.start == 0 {
                    run.range.end
                } else {
                    0
                }
            });
            out.push(Seek::recovery(
                VolumeIndex(index as u32),
                ByteRange::new(start, u64::MAX),
                Weight::FIRST,
            ));
        }
    }

    fn repair(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        // Each volume as it arrived, and the runs of it that read back.
        let mut volumes = Vec::with_capacity(self.volumes.len());
        for (index, volume) in self.volumes.iter().enumerate() {
            let end = volume.arrived.runs().map(|run| run.range.end).max();
            let mut bytes = vec![0; usize::try_from(end.unwrap_or(0)).unwrap_or(0)];
            let mut read = Vec::new();
            let mut kept = Coverage::new();
            for run in volume.arrived.runs() {
                read.clear();
                if access.read_volume(VolumeIndex(index as u32), run.range, &mut read)
                    && let Some(into) =
                        bytes.get_mut(run.range.start as usize..run.range.end as usize)
                    && into.len() == read.len()
                {
                    into.copy_from_slice(&read);
                    kept.write(run.range, run.provenance);
                }
            }
            volumes.push((bytes, kept));
        }
        let coming = self.coming();
        let mut outcome = RepairOutcome::Clean;
        let mut waiting = 0;
        for set in sets(volumes.iter().map(|(bytes, kept)| (bytes.as_slice(), kept))) {
            let result = rebuild::repair_set(&set, &self.bound, coming, access);
            if result.outcome == RepairOutcome::Waiting {
                waiting += result.lost;
            }
            outcome = combine(outcome, result.outcome);
        }
        self.needed = waiting;
        self.outcome = Some(outcome);
        outcome
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        self.outcome
    }

    // What the kept fronts carry, past any hole in them.
    fn blocks_reachable(&self) -> u64 {
        self.front_sets()
            .iter()
            .map(|set| rebuild::usable_recovery(set).len() as u64)
            .sum()
    }
}
