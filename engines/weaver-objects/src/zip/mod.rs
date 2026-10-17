// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The zip store: a zip, whole or spanned over `.z01` … `.zip`, streamed into
// its members as its bytes arrive, in any order, with no engine and no file
// of its own. It bids for the directory at the container's tail and reads
// the entries from it; a stored entry's data is its member's bytes once the
// entry's local header says where they start. When nothing above can bring
// the tail forward, it walks the local headers from the front instead.
// Anything compressed, enciphered or of unknown extent goes to an engine,
// before any member is said whenever the directory can say so.
//
// The object is pure: no I/O, no threads, no files. Bytes it cannot place yet
// are held by its owner, who hands them back once the map places them.

mod directory;
mod geometry;
mod map;
mod place;
mod record;
pub(crate) mod records;
mod walk;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashSet};

use weaver_graph::{
    ByteRange, Delivery, Emit, Extent, Kind, MemberId, Object, Placement, Pull, Seek, VolumeIndex,
    Weight, Why,
};

use crate::engine::{Format, SetName};
use crate::grid::{Grid, crc32};
use crate::kept::Kept;
use crate::zip_engine::{ZipFormat, naming::VolumeName};
use geometry::Geometry;
use map::{Map, Out};

// The one envelope: container coordinates, for every byte that belongs to no
// member.
const ENVELOPE: u32 = 0;

#[derive(Default)]
pub struct Zip {
    // The names bound that name a volume of a set, and every volume bound.
    names: BTreeMap<u32, VolumeName>,
    bound: BTreeSet<u32>,
    geometry: Geometry,
    kept: Kept,
    map: Option<Map>,
    // Reading the local headers from the front, not the directory.
    walking: bool,
    // Container ranges the directory read needs: the tail, the zip64
    // records, the directory itself.
    directory: Vec<ByteRange>,
    // The bytes the map was built on, by range and checksum.
    basis: Vec<(ByteRange, u32)>,
    members: Vec<Out>,
    grid: Grid,
    // Container ranges wanted ahead of the normal order, and the volume
    // ranges already bid for them.
    bids: Vec<(ByteRange, Weight)>,
    bid: BTreeSet<(u32, u64, u64)>,
    damage_said: HashSet<(u64, u64, u32)>,
    // Something changed since the object last looked.
    stale: bool,
    // The object gave its container up; it says nothing more.
    gone: bool,
}

impl Zip {
    pub fn new() -> Self {
        Self::default()
    }

    // The volume the directory ends: the one named last, or the only one
    // when the first names no set.
    fn last(&self) -> Option<VolumeIndex> {
        match self.names.iter().find(|(_, name)| name.is_last()) {
            Some((index, _)) => Some(VolumeIndex(*index)),
            None => {
                (self.bound.contains(&0) && !self.names.contains_key(&0)).then_some(VolumeIndex(0))
            }
        }
    }

    fn total(&self) -> Option<u64> {
        self.geometry.total(self.last()?)
    }

    fn global(&self, volume: VolumeIndex, range: ByteRange) -> Option<ByteRange> {
        let base = self.geometry.base(volume)?;
        Some(range.shift(base as i64))
    }

    fn to_volumes(&self, range: ByteRange, out: &mut Vec<(VolumeIndex, ByteRange)>) {
        self.geometry.to_volumes(range, out);
    }

    fn give_up(&mut self, emit: Emit, out: &mut Vec<Emit>) {
        self.gone = true;
        out.push(emit);
    }

    // Keep a range, and bid for it unless it comes in the normal order.
    fn want(&mut self, range: ByteRange, bid: bool) {
        self.kept.want(range);
        if bid && !range.is_empty() && !self.bids.iter().any(|(wanted, _)| *wanted == range) {
            self.bids.push((range, Weight::FIRST));
        }
    }

    // A range failed to parse: say so once for each version of its bytes,
    // then wait for a repair to deliver it again.
    fn say_damaged(&mut self, range: ByteRange, out: &mut Vec<Emit>) {
        let crc = self.kept.read(range).map_or(0, crc32);
        if !self.damage_said.insert((range.start, range.end, crc)) {
            return;
        }
        let mut pieces = Vec::new();
        self.to_volumes(range, &mut pieces);
        out.extend(
            pieces
                .into_iter()
                .map(|(volume, range)| Emit::Damaged { volume, range }),
        );
    }

    // Look again at everything, after anything changed.
    fn settle(&mut self, out: &mut Vec<Emit>) {
        if self.gone || !std::mem::take(&mut self.stale) {
            return;
        }
        if self.walking {
            self.walk(out);
        } else if self.map.is_none() {
            self.read_directory(out);
        }
        if !self.walking && self.map.is_some() && !self.gone {
            self.read_locals(out);
        }
        if let (Some(end), Some(total)) = (self.map.as_ref().and_then(|map| map.end), self.total())
        {
            self.kept.want(ByteRange::new(end.min(total), total));
        }
    }

    // Read the local headers from the front from now on: nothing above can
    // bring the directory forward, or it will never arrive.
    fn start_walking(&mut self, out: &mut Vec<Emit>) {
        if self.walking || self.map.is_some() {
            return;
        }
        self.walking = true;
        self.stale = true;
        self.settle(out);
    }

    // Repaired bytes changed what the map was built on: everything said about
    // the members was built on wrong bytes.
    fn rebuild_if_changed(&mut self, range: ByteRange, out: &mut Vec<Emit>) {
        if self.map.is_none()
            || !self
                .basis
                .iter()
                .any(|(basis, _)| basis.intersect(range).is_some())
        {
            return;
        }
        let same = self.basis.iter().all(|(basis, crc)| {
            self.kept
                .read(*basis)
                .is_some_and(|bytes| crc32(bytes) == *crc)
        });
        if same {
            return;
        }
        for id in 0..self.members.len() {
            out.push(Emit::Retract(MemberId(id as u32)));
        }
        self.map = None;
        self.basis.clear();
        self.members.clear();
        self.grid = Grid::default();
    }

    // Whether the range is one the directory read needs, before the map.
    fn is_directory(&self, range: ByteRange) -> bool {
        self.map.is_none()
            && self
                .directory
                .iter()
                .any(|wanted| wanted.intersect(range).is_some())
    }
}

impl Object for Zip {
    fn kind(&self) -> Kind {
        Kind::Zip
    }

    fn deliver(&mut self, delivery: Delivery<'_>, out: &mut Vec<Emit>) {
        if self.gone {
            return;
        }
        // A volume with no place yet has its bytes held until it has one.
        let Some(range) = self.global(delivery.volume, delivery.range()) else {
            return;
        };
        if range.is_empty() {
            return;
        }
        // Absorbing can make the object want more of this very delivery: the
        // tail names the directory, the directory names the local headers.
        loop {
            let generation = self.kept.generation();
            if self.kept.absorb(range, delivery.bytes, delivery.provenance) {
                self.stale = true;
                self.rebuild_if_changed(range, out);
            }
            self.settle(out);
            if self.gone {
                return;
            }
            if self.kept.generation() == generation {
                break;
            }
        }
        self.route(range, delivery.bytes, delivery.provenance, out);
    }

    fn volume_closed(&mut self, _volume: VolumeIndex, out: &mut Vec<Emit>) {
        self.stale = true;
        self.settle(out);
    }

    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>) {
        if self.gone {
            return;
        }
        let Some(range) = self.global(volume, range) else {
            return;
        };
        if self.is_directory(range) {
            return self.start_walking(out);
        }
        // A header that will never arrive leaves its entry's bytes nowhere.
        if self.lost_header(range) {
            self.give_up(Emit::Refuse(Why::Unrepairable), out);
        }
    }

    fn wants(&mut self, out: &mut Vec<Seek>) {
        if self.gone {
            return;
        }
        let mut pieces = Vec::new();
        for (range, weight) in self.bids.clone() {
            pieces.clear();
            self.to_volumes(range, &mut pieces);
            for (volume, range) in &pieces {
                // Only volumes bound so far can be bid: a seek into one not
                // yet claimed would be answered as unseekable.
                if self.bound.contains(&volume.0)
                    && self.bid.insert((volume.0, range.start, range.end))
                {
                    out.push(Seek::data(*volume, *range, weight));
                }
            }
        }
    }

    fn pulled(&mut self, seek: Seek, pull: Pull, out: &mut Vec<Emit>) {
        if self.gone || matches!(pull, Pull::Moved(_)) {
            return;
        }
        let Some(range) = self.global(seek.volume, seek.range) else {
            return;
        };
        // The directory is at the tail and nothing above brings it forward,
        // or it is gone: the local headers say what they can from the front.
        if self.is_directory(range) {
            self.start_walking(out);
        }
    }

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        self.place(volume, range, out);
    }

    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) {
        self.member_extent(member, range, out);
    }

    fn sized(&mut self, volume: VolumeIndex, length: u64, out: &mut Vec<Emit>) {
        if self.geometry.set(volume, length) {
            self.stale = true;
            self.settle(out);
        }
    }

    fn envelope(&self, envelope: u32, range: ByteRange) -> Option<bytes::Bytes> {
        if envelope != ENVELOPE {
            return None;
        }
        self.kept.read(range).map(bytes::Bytes::copy_from_slice)
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str) {
        let first = self.names.values().next();
        if let Some(parsed) = ZipFormat::parse(name)
            && first.is_none_or(|first| first.same_set(&parsed))
        {
            self.names.insert(volume.0, parsed);
        }
        self.bound.insert(volume.0);
        self.stale = true;
    }

    fn claim(&mut self, name: &str, _first_bytes: &[u8]) -> Option<VolumeIndex> {
        let candidate = ZipFormat::parse(name)?;
        let first = self.names.get(&0)?;
        let index = first
            .same_set(&candidate)
            .then(|| candidate.slot(&self.names))??;
        (!self.bound.contains(&index)).then_some(VolumeIndex(index))
    }
}

// Why the store cannot stream an entry, if it cannot. Nothing to stream is
// never a reason.
fn refusal(header: &records::Header) -> Option<Why> {
    if header.shape != records::Shape::File || header.length == 0 {
        return None;
    }
    if header.encrypted() {
        Some(Why::Encrypted)
    } else if !header.stored() {
        Some(Why::Compressed)
    } else {
        (header.compressed != header.length).then_some(Why::Unsupported)
    }
}
