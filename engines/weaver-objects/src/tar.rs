// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tar object: a tar volume streamed into its entries as its bytes
// arrive. A tar is a walk: each entry's headers say how long its data is,
// and the next entry starts after that data's padding, so the object reads
// its headers in order from the front and never seeks. An entry's data maps
// straight through to its member; headers, padding, the two zero blocks that
// end the archive and whatever follows them are envelope, kept by the object
// so it can hand them back. Tar carries no checksum over data, so nothing is
// verified; a header's own checksum is the only damage it can see.
//
// The object is pure: no I/O, no threads, no files. Bytes past the header
// the walk waits on are held by its owner, who hands them back once the walk
// reaches them.

mod header;

#[cfg(test)]
mod tests;

use std::collections::HashSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Coverage, Delivery, Durable, Emit, Extent, Facts, Kind, Located, MemberId, Object,
    Placement, Provenance, Pull, Seek, Standing, VolumeIndex, Why,
};

use crate::grid::crc32;
use crate::kept::Kept;
use header::{Body, Read};

// The one envelope, in the volume's own coordinates.
const ENVELOPE: u32 = 0;
const VOLUME: VolumeIndex = VolumeIndex(0);

// The most of an entry's data that may be envelope: a link's or a
// directory's data is kept in memory to be handed back.
const ENVELOPE_LIMIT: u64 = 1 << 20;

// One entry as the walk read it.
#[derive(Serialize, Deserialize)]
struct Record {
    start: u64,
    data: ByteRange,
    // Past the data's padding: where the next entry starts.
    end: u64,
    member: Option<MemberId>,
    // The data is the member's, not envelope.
    carries: bool,
    // Of the header bytes the entry was read from, so a repair that changes
    // them is told from one that delivers them again.
    crc: u32,
}

impl Record {
    fn headers(&self) -> ByteRange {
        ByteRange::new(self.start, self.data.start)
    }

    // The entry's bytes in volume order, each with the member it is.
    fn pieces(&self) -> [(ByteRange, Option<MemberId>); 3] {
        [
            (self.headers(), None),
            (self.data, self.member.filter(|_| self.carries)),
            (ByteRange::new(self.data.end, self.end), None),
        ]
    }
}

// What has been said for one member.
#[derive(Serialize, Deserialize)]
struct Out {
    // Where its data lies in the volume; empty for a directory.
    data: ByteRange,
    coverage: Coverage,
    closed: bool,
}

// The two zero blocks, with the checksum of what they were read as.
#[derive(Serialize, Deserialize)]
struct End {
    range: ByteRange,
    crc: u32,
}

// Its facts are the whole of it: the walk so far, the headers it kept and
// what it said.
#[derive(Default, Serialize, Deserialize)]
pub struct Tar {
    kept: Kept,
    records: Vec<Record>,
    members: Vec<Out>,
    // Where the next entry starts.
    cursor: u64,
    // The bytes from the cursor the walk waits on.
    ahead: Option<ByteRange>,
    end: Option<End>,
    length: Option<u64>,
    reached: u64,
    lost: Vec<ByteRange>,
    damage_said: HashSet<(u64, u64, u32)>,
    gone: bool,
}

impl Durable for Tar {
    const VERSION: u16 = 1;

    // Entries follow one another from the front, each inside its own span,
    // up to the cursor, and name only members said.
    fn impossible(&self) -> Option<&'static str> {
        let mut at = 0;
        let walked = self.records.iter().all(|record| {
            let fits = record.start == at
                && record.start <= record.data.start
                && record.data.end <= record.end
                && record
                    .member
                    .is_none_or(|member| (member.0 as usize) < self.members.len());
            at = record.end;
            fits
        });
        (!walked || at > self.cursor || !self.kept.sound()).then_some("a walk no tar gives")
    }
}

impl Tar {
    pub fn new() -> Self {
        Self::default()
    }

    fn give_up(&mut self, emit: Emit, out: &mut Vec<Emit>) {
        self.gone = true;
        out.push(emit);
    }

    fn crc_of(&self, range: ByteRange) -> u32 {
        self.kept.read(range).map_or(0, crc32)
    }

    // Read entries from the cursor for as long as their headers are in.
    fn walk(&mut self, out: &mut Vec<Emit>) {
        while self.end.is_none() && !self.gone {
            match header::read(&self.kept, self.cursor) {
                Read::Wait(range) => return self.wait(range, out),
                Read::Damaged(block) => {
                    self.wait(ByteRange::new(self.cursor, block.end), out);
                    return self.say_damaged(block, out);
                }
                Read::End(range) => {
                    self.ahead = None;
                    self.end = Some(End {
                        range,
                        crc: self.crc_of(range),
                    });
                    return self.want_tail();
                }
                Read::Entry(entry) => self.record(entry, out),
                Read::Unsupported => return self.give_up(Emit::Refuse(Why::Unsupported), out),
            }
        }
    }

    fn wait(&mut self, range: ByteRange, out: &mut Vec<Emit>) {
        if self.lost.iter().any(|lost| lost.intersect(range).is_some()) {
            return self.give_up(Emit::Refuse(Why::Unrepairable), out);
        }
        self.ahead = Some(range);
        self.kept.want(range);
    }

    // Said once for each version of the header's bytes; the walk then waits
    // for a repair to deliver it again.
    fn say_damaged(&mut self, range: ByteRange, out: &mut Vec<Emit>) {
        if self
            .damage_said
            .insert((range.start, range.end, self.crc_of(range)))
        {
            out.push(Emit::Damaged {
                volume: VOLUME,
                range,
            });
        }
    }

    fn record(&mut self, entry: header::Entry, out: &mut Vec<Emit>) {
        let bounds = entry.data.checked_add(entry.size).and_then(|data_end| {
            let end = entry.data.checked_add(header::padded(entry.size))?;
            Some((ByteRange::new(entry.data, data_end), end))
        });
        let Some((data, end)) = bounds else {
            return self.give_up(Emit::Refuse(Why::Unsupported), out);
        };
        let carries = entry.body == Body::File;
        if !carries && data.len() > ENVELOPE_LIMIT {
            return self.give_up(Emit::Refuse(Why::Unsupported), out);
        }
        if !carries {
            self.kept.want(data);
        }
        self.kept.want(ByteRange::new(data.end, end));
        let headers = ByteRange::new(self.cursor, entry.data);
        let member = (entry.body != Body::Skipped).then(|| self.announce(entry, data, out));
        self.records.push(Record {
            start: self.cursor,
            data,
            end,
            member,
            carries,
            crc: self.crc_of(headers),
        });
        self.cursor = end;
        self.ahead = None;
    }

    fn announce(&mut self, entry: header::Entry, data: ByteRange, out: &mut Vec<Emit>) -> MemberId {
        let member = MemberId(self.members.len() as u32);
        let (name, data) = match entry.body {
            Body::Directory if !entry.name.ends_with('/') => {
                (format!("{}/", entry.name), ByteRange::at(data.start, 0))
            }
            Body::Directory => (entry.name, ByteRange::at(data.start, 0)),
            _ => (entry.name, data),
        };
        out.push(Emit::Member {
            member,
            name,
            length: Some(data.len()),
        });
        let closed = data.is_empty();
        if closed {
            out.push(Emit::MemberClosed(member));
        }
        self.members.push(Out {
            data,
            coverage: Coverage::new(),
            closed,
        });
        member
    }

    // Once the end is read and the volume's length known, everything after
    // the end is envelope too.
    fn want_tail(&mut self) {
        if let (Some(end), Some(length)) = (&self.end, self.length)
            && length > end.range.end
        {
            self.kept.want(ByteRange::new(end.range.end, length));
        }
    }

    // Repaired bytes changed headers the walk already read: everything said
    // from that entry on was built on wrong bytes, so it is retracted and the
    // walk starts again there.
    fn rewind_if_changed(&mut self, range: ByteRange, out: &mut Vec<Emit>) {
        let first = self
            .records
            .partition_point(|record| record.data.start <= range.start);
        let changed = self.records[first..]
            .iter()
            .take_while(|record| record.start < range.end)
            .position(|record| self.crc_of(record.headers()) != record.crc);
        if let Some(index) = changed {
            return self.rewind(first + index, out);
        }
        if let Some(end) = &self.end
            && end.range.intersect(range).is_some()
            && self.crc_of(end.range) != end.crc
        {
            self.end = None;
        }
    }

    fn rewind(&mut self, index: usize, out: &mut Vec<Emit>) {
        let undone = self.records.split_off(index);
        self.cursor = undone[0].start;
        out.extend(
            undone
                .iter()
                .filter_map(|record| record.member)
                .map(Emit::Retract),
        );
        let kept = self
            .records
            .iter()
            .filter(|record| record.member.is_some())
            .count();
        self.members.truncate(kept);
        self.end = None;
        self.ahead = None;
    }

    // Data bytes to the members they belong to, as they arrive.
    fn route(
        &mut self,
        range: ByteRange,
        bytes: &[u8],
        provenance: Provenance,
        out: &mut Vec<Emit>,
    ) {
        let first = self
            .records
            .partition_point(|record| record.data.end <= range.start);
        for record in &self.records[first..] {
            if record.data.start >= range.end {
                break;
            }
            let (Some(member), true) = (record.member, record.carries) else {
                continue;
            };
            let Some(part) = record.data.intersect(range) else {
                continue;
            };
            let at = (part.start - range.start) as usize;
            let offset = part.start - record.data.start;
            out.push(Emit::Bytes {
                member,
                offset,
                bytes: Bytes::copy_from_slice(&bytes[at..at + part.len() as usize]),
                provenance,
            });
            let said = &mut self.members[member.0 as usize];
            said.coverage
                .write(ByteRange::at(offset, part.len()), provenance);
            if !said.closed && said.coverage.covered(ByteRange::new(0, record.data.len())) {
                said.closed = true;
                out.push(Emit::MemberClosed(member));
            }
        }
    }

    // How far past the walk the bytes are envelope: what the walk waits on,
    // or, once it has ended, the end blocks and, with the volume's length
    // known, everything after them.
    fn kept_until(&self) -> u64 {
        match (&self.end, self.length) {
            (Some(_), Some(_)) => u64::MAX,
            (Some(end), None) => end.range.end,
            (None, _) => self.ahead.map_or(self.cursor, |ahead| ahead.end),
        }
    }

    // What the walk still needs when nothing more can arrive: the tail of an
    // entry the volume ends inside, or of the header the walk waits on.
    fn missing_tail(&self, length: u64) -> Option<ByteRange> {
        if let Some(last) = self.records.last()
            && last.end > length
        {
            return (last.data.end > length).then(|| ByteRange::new(length, last.data.end));
        }
        let ahead = self.ahead.filter(|ahead| ahead.end > length)?;
        // A single zero block where the volume ends ends the archive too;
        // so does a volume that ends where an entry would start.
        if self.cursor >= length || header::is_zero_block(&self.kept, self.cursor) {
            return None;
        }
        Some(ByteRange::new(length, ahead.end))
    }
}

impl Object for Tar {
    fn kind(&self) -> Kind {
        Kind::Tar
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if delivery.volume != VOLUME || delivery.bytes.is_empty() {
            return;
        }
        let range = delivery.range();
        self.reached = self.reached.max(range.end);
        // Reading a header can make the walk want more of this very delivery:
        // the next header, an extension's data.
        loop {
            let generation = self.kept.generation();
            if self.kept.absorb(range, delivery.bytes, delivery.provenance) {
                self.rewind_if_changed(range, out);
            }
            self.walk(out);
            if self.gone {
                return;
            }
            if self.kept.generation() == generation {
                break;
            }
        }
        self.route(range, delivery.bytes, delivery.provenance, out);
    }

    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        if volume != VOLUME || self.end.is_some() {
            return;
        }
        let length = self.length.unwrap_or(self.reached);
        if let Some(range) = self.missing_tail(length)
            && self.damage_said.insert((range.start, range.end, 0))
        {
            out.push(Emit::Damaged {
                volume: VOLUME,
                range,
            });
        }
    }

    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>) {
        if volume != VOLUME {
            return;
        }
        self.lost.push(range);
        if self.end.is_none()
            && self
                .ahead
                .is_some_and(|ahead| ahead.intersect(range).is_some())
        {
            self.give_up(Emit::Refuse(Why::Unrepairable), out);
        }
    }

    // Nothing more will arrive. A member still damaged, or one named and
    // never placed, leaves the archive unread.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        if let Some(why) =
            crate::quiet::damaged(standing).or_else(|| crate::quiet::unplaced(standing))
        {
            self.give_up(Emit::Refuse(why), out);
        }
    }

    // A tar is read in order; it never asks for anything ahead of it.
    fn wants(&self, _out: &mut Vec<Seek>) {}

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

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
        if volume != VOLUME {
            return out.push(Placement::Held { range });
        }
        let mut at = range.start;
        let first = self.records.partition_point(|record| record.end <= at);
        for record in &self.records[first..] {
            if at >= range.end {
                break;
            }
            for (piece, member) in record.pieces() {
                let Some(part) = piece.intersect(ByteRange::new(at, range.end)) else {
                    continue;
                };
                out.push(match member {
                    Some(member) => Placement::Member {
                        member,
                        range: ByteRange::new(
                            part.start - record.data.start,
                            part.end - record.data.start,
                        ),
                    },
                    None => Placement::Envelope {
                        envelope: ENVELOPE,
                        range: part,
                    },
                });
                at = part.end;
            }
        }
        if at >= range.end {
            return;
        }
        let split = self.kept_until().clamp(at, range.end);
        if split > at {
            out.push(Placement::Envelope {
                envelope: ENVELOPE,
                range: ByteRange::new(at, split),
            });
        }
        if split < range.end {
            out.push(Placement::Held {
                range: ByteRange::new(split, range.end),
            });
        }
    }

    // Every member is an entry's raw data, so every range of one lies
    // somewhere: not located yet until the walk reads the entry's header,
    // and never for a range past the data that header says it has.
    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located {
        let Some(said) = self.members.get(member.0 as usize) else {
            return Located::NotYet;
        };
        let wanted = ByteRange::new(
            said.data.start.saturating_add(range.start),
            said.data.start.saturating_add(range.end),
        );
        let Some(range) = said.data.intersect(wanted) else {
            return Located::Never;
        };
        out.push(Extent {
            volume: VOLUME,
            range,
        });
        Located::Whole
    }

    fn sized(&mut self, volume: VolumeIndex, length: u64, _out: &mut Vec<Emit>) {
        if volume != VOLUME || self.length.is_some() {
            return;
        }
        self.length = Some(length);
        self.want_tail();
    }

    fn envelope(&self, envelope: u32, range: ByteRange) -> Option<Bytes> {
        if envelope != ENVELOPE {
            return None;
        }
        if range.is_empty() {
            return Some(Bytes::new());
        }
        self.kept.read(range).map(Bytes::copy_from_slice)
    }

    fn reads_in_order(&self) -> bool {
        true
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::Tar, self)
    }
}
