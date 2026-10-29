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

mod carried;
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

use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Extent, Facts, Give, Kind, Located, MemberId, Object,
    Placement, Pull, Seek, Standing, Take, VolumeIndex, Weight, Why,
};

use crate::engine::{Format, SetName};
use crate::grid::{Grid, crc32};
use crate::kept::Kept;
use crate::zip_engine::ZipFormat;
use crate::zip_engine::naming::{LAST, VolumeName};
use geometry::Geometry;
use map::{Map, Out};

// The one envelope: container coordinates, for every byte that belongs to no
// member.
const ENVELOPE: u32 = 0;

// Its facts are all of it but its bids, which a restart makes again: the
// directory or how far the walk got, the bytes it kept and what it said.
#[derive(Default, Serialize, Deserialize)]
pub struct Zip {
    // The names bound that name a volume of a set, and every volume bound.
    names: BTreeMap<u32, VolumeName>,
    bound: BTreeSet<u32>,
    // The disk the symbolic last volume is, once its end record said, and
    // its length until then: the geometry is by disk.
    last_disk: Option<u32>,
    last_length: Option<u64>,
    // The symbolic last volume opened with a local header while it was the
    // only volume bound: it is a whole archive, disk 0, and streams from its
    // front. A numbered volume bound beside it says otherwise.
    #[serde(default)]
    opens_alone: bool,
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
    // Container ranges wanted ahead of the normal order.
    bids: Vec<(ByteRange, Weight)>,
    damage_said: HashSet<(u64, u64, u32)>,
    // Whether the container's tail was looked at for a recovery set.
    #[serde(default)]
    recovery_said: bool,
    // Something changed since the object last looked.
    stale: bool,
    // The object gave its container up; it says nothing more.
    gone: bool,
}

impl Durable for Zip {
    // 4: the grid shelved by chunk of blocks, kept apart from the bytes.
    // 5: a sealed value encodes as nothing, numbered by its owner's walk.
    // 6: every entry keeps one record's header until the other agrees.
    const VERSION: u16 = 6;

    fn parts(&self, take: &mut Take) -> Result<(), String> {
        self.grid.parts(take)
    }

    fn restore_parts(&mut self, give: &mut Give) -> Result<(), String> {
        self.grid.restore_parts(give)
    }

    // A member said is an entry of the map, and every entry's member is one.
    fn impossible(&self) -> Option<&'static str> {
        let mapped = self.map.as_ref().map_or(self.members.is_empty(), |map| {
            map.members.len() == self.members.len()
                && map.members.iter().all(|entry| *entry < map.entries.len())
                && map
                    .entries
                    .iter()
                    .filter_map(|entry| entry.member)
                    .all(|member| map.members.get(member.0 as usize).is_some())
        });
        (!mapped || !self.kept.sound()).then_some("members no map gives")
    }
}

impl Zip {
    pub fn new() -> Self {
        Self::default()
    }

    // The disk the directory ends: the one named last, or the only one
    // when the first names no set.
    fn last(&self) -> Option<VolumeIndex> {
        match self.names.iter().find(|(_, name)| name.is_last()) {
            Some((&LAST, _)) => self.disk(VolumeIndex(LAST)),
            Some((index, _)) => Some(VolumeIndex(*index)),
            None => {
                (self.bound.contains(&0) && !self.names.contains_key(&0)).then_some(VolumeIndex(0))
            }
        }
    }

    fn total(&self) -> Option<u64> {
        self.geometry.total(self.last()?)
    }

    // The disk a bound volume is: its own number, or for the symbolic last
    // the disk its end record said, or disk 0 when it opened alone.
    fn disk(&self, volume: VolumeIndex) -> Option<VolumeIndex> {
        match volume.0 {
            LAST => self
                .last_disk
                .or(self.opens_alone.then_some(0))
                .map(VolumeIndex),
            _ => Some(volume),
        }
    }

    // The bound volume a disk is.
    fn volume(&self, disk: VolumeIndex) -> VolumeIndex {
        match self.disk(VolumeIndex(LAST)) {
            Some(last) if last == disk && self.bound.contains(&LAST) => VolumeIndex(LAST),
            _ => disk,
        }
    }

    // Where the symbolic last volume's disk is decided, from what it has
    // delivered. Alone and opening with a local header, it is a whole
    // archive at disk 0, so it streams from its front; otherwise its end
    // record says. A numbered volume bound beside one that opened alone is
    // a spanned set whose last disk starts at an entry, which the store
    // has mapped as disk 0: it gives the set to the engine, which reads the
    // bound volumes in their order.
    fn decide_last(
        &mut self,
        delivery: &Delivery<'_>,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if self.opens_alone {
            if self.bound.iter().any(|volume| *volume != LAST) {
                self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            }
            return;
        }
        if delivery.volume.0 != LAST || self.last_disk.is_some() {
            return;
        }
        let alone = self.bound.iter().all(|volume| *volume == LAST);
        let signature = ByteRange::new(0, 4);
        if alone
            && read_through(signature, delivery, read)
                .is_some_and(|front| front == records::LOCAL.to_le_bytes())
        {
            self.opens_alone = true;
            if let Some(length) = self.last_length {
                self.geometry.set(VolumeIndex(0), length);
            }
            self.stale = true;
            return;
        }
        self.resolve_last(delivery, read, out);
    }

    fn global(&self, volume: VolumeIndex, range: ByteRange) -> Option<ByteRange> {
        let base = self.geometry.base(self.disk(volume)?)?;
        Some(range.shift(base as i64))
    }

    fn to_volumes(&self, range: ByteRange, out: &mut Vec<(VolumeIndex, ByteRange)>) {
        let start = out.len();
        self.geometry.to_volumes(range, out);
        for (volume, _) in &mut out[start..] {
            *volume = self.volume(*volume);
        }
    }

    // Learn the symbolic last volume's disk from the end record in its own
    // tail. Every byte of it is held by the owner until then, since it has
    // no place in the container, so the tail is read back through the owner
    // where this delivery does not carry it.
    fn resolve_last(
        &mut self,
        delivery: &Delivery<'_>,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let Some(length) = self.last_length else {
            return;
        };
        let tail = ByteRange::new(
            length.saturating_sub(records::TAIL + records::LOCATOR_LEN),
            length,
        );
        let Some(bytes) = read_through(tail, delivery, read) else {
            return;
        };
        let Some(end) = records::find_end(&bytes) else {
            return;
        };
        // A saturated disk number is in the zip64 locator, as the count of
        // disks, just ahead of the end record.
        let disk = match end.this_disk {
            disk if disk != u32::from(u16::MAX) => Some(disk),
            _ => usize::try_from(end.at)
                .ok()
                .and_then(|at| at.checked_sub(records::LOCATOR_LEN as usize))
                .and_then(|at| {
                    records::locator(&bytes[at..])?;
                    records::u32_at(&bytes, at + 16)?.checked_sub(1)
                }),
        };
        // A numbered volume at or past the last disk is not of this set.
        let Some(disk) = disk.filter(|disk| {
            self.bound
                .iter()
                .all(|volume| *volume == LAST || volume < disk)
        }) else {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        };
        self.last_disk = Some(disk);
        self.geometry.set(VolumeIndex(disk), length);
        self.stale = true;
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
        self.find_recovery(out);
        if self.walking {
            self.walk(out);
            if !self.gone
                && self
                    .map
                    .as_ref()
                    .is_some_and(|map| map.end.is_some() && !map.checked)
            {
                self.read_directory(out);
            }
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

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if self.gone {
            return;
        }
        self.decide_last(&delivery, read, out);
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

    // Nothing more will arrive. A member still damaged, or one named and
    // never placed, leaves the archive unread.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        if self.gone {
            return;
        }
        if let Some(why) =
            crate::quiet::damaged(standing).or_else(|| crate::quiet::unplaced(standing))
        {
            self.give_up(Emit::Refuse(why), out);
        }
    }

    fn wants(&self, out: &mut Vec<Seek>) {
        if self.gone {
            return;
        }
        let mut pieces = Vec::new();
        for (range, weight) in &self.bids {
            // A range in hand is wanted no more.
            if self.kept.read(*range).is_some() {
                continue;
            }
            pieces.clear();
            self.to_volumes(*range, &mut pieces);
            for (volume, range) in &pieces {
                // Only volumes bound so far can be bid: a seek into one not
                // yet claimed would be answered as unseekable.
                if self.bound.contains(&volume.0) {
                    out.push(Seek::data(*volume, *range, *weight));
                }
            }
        }
    }

    // A range brought forward, or one not located yet and asked for again
    // next round, is waited for.
    fn pulled(&mut self, seek: Seek, pull: Pull, out: &mut Vec<Emit>) {
        if self.gone || matches!(pull, Pull::Moved(_) | Pull::Unlocated) {
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

    // Each member the bytes reached forgets what was said of them and is
    // open until they are said again.
    fn unposted(&mut self, volume: VolumeIndex, range: ByteRange) {
        let mut placements = Vec::new();
        self.locate(volume, range, &mut placements);
        crate::said::unposted(&placements, &mut self.members, |out| {
            (&mut out.coverage, &mut out.closed)
        });
    }

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        self.place(volume, range, out);
    }

    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located {
        self.member_extent(member, range, out)
    }

    fn sized(&mut self, volume: VolumeIndex, length: u64, out: &mut Vec<Emit>) {
        let Some(disk) = self.disk(volume) else {
            self.last_length = Some(length);
            return;
        };
        if self.geometry.set(disk, length) {
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

    fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8], _length: Option<u64>) {
        let first = self.names.values().next();
        if let Some(parsed) = ZipFormat::parse(name)
            && first.is_none_or(|first| first.same_set(&parsed))
        {
            self.names.insert(volume.0, parsed);
        }
        self.bound.insert(volume.0);
        self.stale = true;
    }

    // The name a recovery set gave the volume is the set's numbering now.
    fn renamed(&mut self, volume: VolumeIndex, name: &str) {
        self.names.remove(&volume.0);
        let first = self.names.values().next();
        if let Some(parsed) = ZipFormat::parse(name)
            && first.is_none_or(|first| first.same_set(&parsed))
        {
            self.names.insert(volume.0, parsed);
        }
    }

    fn claim(
        &mut self,
        name: &str,
        _first_bytes: &[u8],
        _length: Option<u64>,
    ) -> Option<VolumeIndex> {
        let candidate = ZipFormat::parse(name)?;
        // The member that founds the store binds at its own name's index,
        // the `.zip` at the symbolic last.
        if self.bound.is_empty() {
            return Some(VolumeIndex(candidate.index()));
        }
        // A store founded by its `.zip` has no first volume's name yet.
        let first = self.names.values().next()?;
        let index = first
            .same_set(&candidate)
            .then(|| candidate.slot(&self.names))??;
        let before_last = index == LAST || self.last_disk.is_none_or(|last| index < last);
        before_last.then_some(VolumeIndex(index))
    }

    fn facts(&self) -> Facts {
        Facts::of(Kind::Zip, self)
    }
}

// `range` of the delivery's volume: the delivery's own bytes where it
// carries them, the rest read through the owner.
fn read_through(
    range: ByteRange,
    delivery: &Delivery<'_>,
    read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
) -> Option<Vec<u8>> {
    let carried = delivery.range().intersect(range);
    let mut bytes = Vec::new();
    let mut read_held = |range: ByteRange, bytes: &mut Vec<u8>| {
        range.is_empty() || read(&Placement::Held { range }, bytes)
    };
    match carried {
        Some(carried) => {
            if !read_held(ByteRange::new(range.start, carried.start), &mut bytes) {
                return None;
            }
            let from = (carried.start - delivery.offset) as usize;
            bytes.extend_from_slice(&delivery.bytes[from..from + carried.len() as usize]);
            read_held(ByteRange::new(carried.end, range.end), &mut bytes).then_some(bytes)
        }
        None => read_held(range, &mut bytes).then_some(bytes),
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
