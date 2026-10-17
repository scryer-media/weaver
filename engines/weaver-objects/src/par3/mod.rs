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
// whole volumes back when it repairs.

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

#[derive(Debug, Default)]
struct Volume {
    front: Vec<u8>,
    arrived: Coverage,
    closed: bool,
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
        sets(self.volumes.iter().map(|volume| volume.front.as_slice()))
    }

    fn coming(&self) -> bool {
        self.volumes
            .iter()
            .any(|volume| !volume.closed && !volume.lost)
    }
}

// Every set the volumes' packets describe; a volume that fails to scan
// contributes nothing.
fn sets<'a>(volumes: impl Iterator<Item = &'a [u8]>) -> Vec<Par3Set> {
    let packets: Vec<Packet> = volumes
        .filter_map(|bytes| par3_rs::scan_packets(bytes).ok())
        .flatten()
        .map(|(_, packet)| packet)
        .collect();
    Par3Set::from_packets(packets).unwrap_or_default()
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
        if offset > volume.front.len() || offset >= FRONT_CAP {
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

    fn volume_lost(&mut self, volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {
        self.volume(volume).lost = true;
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
        if first_bytes.get(..MAGIC.len()) != Some(MAGIC.as_slice()) {
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
        let mut volumes = Vec::with_capacity(self.volumes.len());
        for (index, volume) in self.volumes.iter().enumerate() {
            let end = volume
                .arrived
                .runs()
                .map(|run| run.range.end)
                .max()
                .unwrap_or(0);
            let mut bytes = Vec::new();
            if access.read_volume(
                VolumeIndex(index as u32),
                ByteRange::new(0, end),
                &mut bytes,
            ) {
                volumes.push(bytes);
            }
        }
        let coming = self.coming();
        let mut outcome = RepairOutcome::Clean;
        let mut waiting = 0;
        for set in sets(volumes.iter().map(Vec::as_slice)) {
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

    // What the kept fronts of volumes still reachable carry.
    fn blocks_reachable(&self) -> u64 {
        sets(
            self.volumes
                .iter()
                .filter(|volume| !volume.lost)
                .map(|volume| volume.front.as_slice()),
        )
        .iter()
        .map(|set| rebuild::usable_recovery(set).len() as u64)
        .sum()
    }
}
