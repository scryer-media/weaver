// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Reading the directory: the end record from the container's tail, the
// zip64 records it points at, then the directory, whose entries are every
// member, said in directory order. One entry the store cannot stream sends
// the whole container to an engine before any member is said. Each entry
// then waits for its local header, which must say what the directory says,
// and whose own name and extra lengths say where a stored entry's data
// starts. A walk that reaches the directory reads it too, and holds every
// header it walked against it.

use std::collections::HashSet;

use weaver_graph::{ByteRange, Emit, Extent, MemberId, VolumeIndex, Why};

use super::map::{Entry, Map, Out};
use super::records::{self, Central, END64_LEN, LOCAL_LEN, LOCATOR_LEN, Shape, TAIL};
use super::{Zip, refusal};
use crate::grid::crc32;
use crate::kept::Kept;
use crate::zip_engine::{MAX_DIRECTORY_BYTES, MAX_DIRECTORY_ENTRIES};

impl Zip {
    // Keep a range the directory read needs, and bid for it unless a walk
    // reads it in the normal order; its bytes once they are all in.
    fn need(&mut self, range: ByteRange) -> Option<Vec<u8>> {
        if !self.directory.contains(&range) {
            self.directory.push(range);
        }
        self.want(range, !self.walking);
        self.kept.read(range).map(<[u8]>::to_vec)
    }

    pub(super) fn read_directory(&mut self, out: &mut Vec<Emit>) {
        let Some(total) = self.total() else {
            return;
        };
        let tail = ByteRange::new(total.saturating_sub(TAIL), total);
        let Some(bytes) = self.need(tail) else {
            return;
        };
        let Some(found) = records::find_end(&bytes) else {
            return self.say_damaged(tail, out);
        };
        let at = tail.start + found.at;
        let mut end = found;
        // Where the records after the directory start.
        let mut after = at;
        let mut read = vec![tail];
        if found.wide {
            let range = ByteRange::new(at.saturating_sub(LOCATOR_LEN), at);
            let Some(bytes) = self.need(range) else {
                return;
            };
            let Some((disk, offset)) = records::locator(&bytes) else {
                return self.say_damaged(range, out);
            };
            let Some(start) = self
                .geometry
                .global(VolumeIndex(disk), offset)
                .filter(|start| {
                    start
                        .checked_add(END64_LEN)
                        .is_some_and(|end| end <= range.start)
                })
            else {
                return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            };
            let record = ByteRange::at(start, END64_LEN);
            let Some(bytes) = self.need(record) else {
                return;
            };
            let Some(wide) = records::end64(&bytes, found) else {
                return self.say_damaged(record, out);
            };
            end = wide;
            after = start;
            read.extend([range, record]);
        }
        if end.size > MAX_DIRECTORY_BYTES || end.entries > MAX_DIRECTORY_ENTRIES {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        }
        // A directory the volumes bound cannot hold is not one this store
        // reads: prepended bytes, or a set whose last volume is misplaced.
        let start = self
            .last()
            .filter(|last| last.0 == end.this_disk)
            .and_then(|_| {
                self.geometry
                    .global(VolumeIndex(end.directory_disk), end.offset)
            })
            .filter(|start| start.checked_add(end.size).is_some_and(|end| end <= after));
        let Some(start) = start else {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        };
        let range = ByteRange::at(start, end.size);
        let Some(bytes) = self.need(range) else {
            return;
        };
        let Some(centrals) = records::directory(&bytes, end.entries) else {
            return self.say_damaged(range, out);
        };
        read.push(range);
        let basis = read
            .into_iter()
            .filter_map(|range| self.kept.read(range).map(|bytes| (range, crc32(bytes))));
        // Two entries of one name are no archive the zip library reads.
        let mut names = HashSet::with_capacity(centrals.len());
        if !centrals
            .iter()
            .all(|central| names.insert(central.header.raw.as_slice()))
        {
            return self.give_up(Emit::Refuse(Why::InvalidArchive), out);
        }
        // A map held against its directory whose footing changed is read
        // again from the directory, a walk's included.
        match self.walking && !self.rereading() {
            true => {
                self.basis.extend(basis);
                self.check_walk(&centrals, start, out);
            }
            false => {
                self.basis = basis.collect();
                self.build(&centrals, start, out);
            }
        }
    }

    // A map held against its directory, with none of the bytes it stood on
    // left: one of them changed, and the directory is read again.
    pub(super) fn rereading(&self) -> bool {
        self.basis.is_empty() && self.map.as_ref().is_some_and(|map| map.checked)
    }

    // The walk's entries against the directory, as the zip library's stream
    // reader holds them: as many, each header where the directory puts it
    // and saying what the directory says, the directory just past the last.
    fn check_walk(&mut self, centrals: &[Central], start: u64, out: &mut Vec<Emit>) {
        let headers: Option<Vec<(u64, &Central)>> = centrals
            .iter()
            .map(|central| {
                self.geometry
                    .global(VolumeIndex(central.disk), central.offset)
                    .map(|header| (header, central))
            })
            .collect();
        let Some(map) = &mut self.map else {
            return;
        };
        let agrees = headers.is_some_and(|mut headers| {
            headers.sort_by_key(|(header, _)| *header);
            map.end == Some(start)
                && headers.len() == map.entries.len()
                && map
                    .entries
                    .iter()
                    .zip(&headers)
                    .all(|(entry, (header, central))| {
                        entry.header == *header
                            && entry
                                .expected
                                .as_ref()
                                .is_some_and(|local| records::agree(local, &central.header))
                    })
        });
        if !agrees {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        }
        for entry in &mut map.entries {
            entry.expected = None;
        }
        map.checked = true;
    }

    fn build(&mut self, centrals: &[Central], start: u64, out: &mut Vec<Emit>) {
        if let Some(why) = centrals.iter().find_map(|central| refusal(&central.header)) {
            return self.give_up(Emit::NeedsEngine(why), out);
        }
        let headers: Option<Vec<u64>> = centrals
            .iter()
            .map(|central| {
                self.geometry
                    .global(VolumeIndex(central.disk), central.offset)
                    .filter(|header| *header < start)
            })
            .collect();
        let Some(headers) = headers else {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        };
        let mut order: Vec<usize> = (0..centrals.len()).collect();
        order.sort_by_key(|index| headers[*index]);
        if order
            .windows(2)
            .any(|pair| headers[pair[0]] == headers[pair[1]])
        {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        }
        let mut map = Map {
            end: Some(start),
            frontier: start,
            checked: true,
            ..Map::default()
        };
        // Members are numbered in directory order, which is the order the
        // namespace numbers them in.
        let mut ids = vec![None; centrals.len()];
        let mut count = 0;
        for (index, central) in centrals.iter().enumerate() {
            if central.header.shape == Shape::Other {
                continue;
            }
            ids[index] = Some((MemberId(count), Said::of(&central.header).length));
            count += 1;
        }
        // The entries by header, with every member pointing at its own.
        map.members = vec![0; count as usize];
        for index in order {
            let (member, length) =
                ids[index].map_or((None, 0), |(member, length)| (Some(member), length));
            if let Some(member) = member {
                map.members[member.0 as usize] = map.entries.len();
            }
            let header = &centrals[index].header;
            let said = Said::of(header);
            map.entries.push(Entry {
                header: headers[index],
                member,
                name: said.name,
                checksum: said.checksum,
                length,
                crc: header.crc,
                data: None,
                streams: length > 0,
                expected: Some(header.clone()),
            });
        }
        // Every byte that is no member's is kept, so the posted volumes can
        // be rebuilt from the members and the envelope.
        let first = map.entries.first().map_or(start, |entry| entry.header);
        self.kept.want(ByteRange::new(0, first));
        for (index, entry) in map.entries.iter().enumerate() {
            let stretch = ByteRange::new(entry.header, map.stretch_end(index));
            match entry.streams {
                true => self.kept.want(ByteRange::new(
                    stretch.start,
                    (stretch.start + LOCAL_LEN).min(stretch.end),
                )),
                false => self.kept.want(stretch),
            }
        }
        if self.map.is_none() {
            // Every member is said now, in id order.
            for central in centrals
                .iter()
                .filter(|central| central.header.shape != Shape::Other)
            {
                let member = MemberId(self.members.len() as u32);
                self.announce(member, &central.header, out);
            }
            self.map = Some(map);
            return;
        }
        // A directory read again over a map in hand: what of its local
        // headers is in hand places its entries now, and the store moves
        // what the old map routed.
        for index in 0..map.entries.len() {
            if let Local::Agreed { range, crc, data } = local(&mut self.kept, &map, index) {
                let entry = &mut map.entries[index];
                entry.expected = None;
                entry.data = data;
                self.basis.push((range, crc));
            }
        }
        self.remap(map, out);
    }

    // Say a member; one with nothing to stream is closed at once. Its length.
    pub(super) fn announce(
        &mut self,
        member: MemberId,
        header: &records::Header,
        out: &mut Vec<Emit>,
    ) -> u64 {
        let said = Said::of(header);
        let length = said.length;
        let mut out_said = Out::default();
        say(member, said, &mut out_said, out);
        self.members.push(out_said);
        length
    }

    // Each entry's local header, once it is in whole: it must say what the
    // directory says, and a stored entry's says where its data starts.
    pub(super) fn read_locals(&mut self, out: &mut Vec<Emit>) {
        let Some(map) = &self.map else {
            return;
        };
        let waiting: Vec<usize> = map
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.expected.is_some())
            .map(|(index, _)| index)
            .collect();
        for index in waiting {
            let Some(map) = &self.map else {
                return;
            };
            let (range, crc, data) = match local(&mut self.kept, map, index) {
                Local::Waiting => continue,
                Local::Damaged(range) => {
                    self.say_damaged(range, out);
                    continue;
                }
                Local::Unsupported => {
                    return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
                }
                Local::Agreed { range, crc, data } => (range, crc, data),
            };
            self.basis.push((range, crc));
            let Some(map) = &mut self.map else {
                return;
            };
            let stretch_end = map.stretch_end(index);
            let entry = &mut map.entries[index];
            entry.expected = None;
            let Some(data) = data else {
                continue;
            };
            entry.data = Some(data);
            let (member, length) = (entry.member, entry.length);
            self.kept.want(ByteRange::new(data + length, stretch_end));
            if let Some(member) = member {
                self.emit_kept(member, out);
            }
        }
    }

    // A directory read again over the map in hand gives this one. The
    // members it places as the old map did keep what they said; every other
    // one moves. The bytes the old map placed in a moving member, and those
    // the new map places in one, are asked for again: they read back through
    // the old map, which stands until the next input, and arrive under the
    // new one.
    fn remap(&mut self, new: Map, out: &mut Vec<Emit>) {
        let Some(old) = self.map.as_ref() else {
            return;
        };
        let mut extents = Vec::new();
        for id in moved(old, &new) {
            let member = MemberId(id as u32);
            self.extent_in(old, member, ByteRange::new(0, u64::MAX), &mut extents);
            self.extent_in(&new, member, ByteRange::new(0, u64::MAX), &mut extents);
        }
        self.next = Some(new);
        if extents.is_empty() {
            return self.install(out);
        }
        extents.sort_by_key(|extent| (extent.volume, extent.range.start));
        let mut ranges: Vec<(VolumeIndex, ByteRange)> = Vec::new();
        for Extent { volume, range } in extents {
            match ranges.last_mut() {
                Some((last, merged)) if *last == volume && merged.end >= range.start => {
                    merged.end = merged.end.max(range.end);
                }
                _ => ranges.push((volume, range)),
            }
        }
        out.extend(
            ranges
                .into_iter()
                .map(|(volume, range)| Emit::Again { volume, range }),
        );
    }

    // The map a directory read again gave takes over: each moving member
    // starts over under it, every other member stands as it was, and the
    // old map is dropped. The map is the directory's from now on, a walk's
    // included.
    pub(super) fn install(&mut self, out: &mut Vec<Emit>) {
        let Some(new) = self.next.take() else {
            return;
        };
        let Some(old) = self.map.take() else {
            return;
        };
        let moved = moved(&old, &new);
        for &id in moved.iter().filter(|id| **id < old.members.len()) {
            out.push(Emit::Retract(MemberId(id as u32)));
            self.grid.forget(MemberId(id as u32));
        }
        let mut was = std::mem::take(&mut self.members);
        self.members = (0..new.members.len())
            .map(|id| match moved.binary_search(&id) {
                Ok(_) => Out::default(),
                Err(_) => std::mem::take(&mut was[id]),
            })
            .collect();
        for &id in moved.iter().filter(|id| **id < new.members.len()) {
            let member = MemberId(id as u32);
            let Some(entry) = new.entry(member) else {
                continue;
            };
            let said = Said {
                name: entry.name.clone(),
                length: entry.length,
                checksum: entry.checksum,
            };
            say(member, said, &mut self.members[id], out);
        }
        self.map = Some(new);
        self.walking = false;
        self.stale = true;
    }

    // Whether a lost range takes a header the map still waits for.
    pub(super) fn lost_header(&self, range: ByteRange) -> bool {
        let Some(map) = &self.map else {
            return false;
        };
        // The whole header once its fixed part says how long it is.
        let waits = |header: u64| {
            let len = self
                .kept
                .read(ByteRange::at(header, LOCAL_LEN))
                .and_then(records::local_len)
                .unwrap_or(LOCAL_LEN);
            ByteRange::at(header, len).intersect(range).is_some()
        };
        let walked = self.walking && map.end.is_none() && waits(map.frontier);
        walked
            || map
                .entries
                .iter()
                .any(|entry| entry.streams && entry.data.is_none() && waits(entry.header))
    }
}

// What a member is said as.
pub(super) struct Said {
    pub(super) name: String,
    pub(super) length: u64,
    pub(super) checksum: Option<u32>,
}

impl Said {
    pub(super) fn of(header: &records::Header) -> Self {
        let directory = header.shape == Shape::Directory;
        Self {
            name: records::member_name(header.name.clone(), header.shape),
            length: if directory { 0 } else { header.length },
            // The header's CRC32 is the member's own unless a key went
            // into it.
            checksum: (!directory && !header.encrypted()).then_some(header.crc),
        }
    }
}

fn say(member: MemberId, said: Said, out_said: &mut Out, out: &mut Vec<Emit>) {
    out.push(Emit::Member {
        member,
        name: said.name,
        length: Some(said.length),
    });
    if let Some(crc32) = said.checksum {
        out.push(Emit::Checksum { member, crc32 });
    }
    if said.length == 0 {
        out_said.closed = true;
        out.push(Emit::MemberClosed(member));
    }
}

// What an entry's local header says, as far as it is in hand.
enum Local {
    Waiting,
    Damaged(ByteRange),
    // It runs into the next header, or says other than the directory.
    Unsupported,
    // It agrees with the directory: its range and checksum, and where the
    // data starts, for an entry whose data streams.
    Agreed {
        range: ByteRange,
        crc: u32,
        data: Option<u64>,
    },
}

fn local(kept: &mut Kept, map: &Map, index: usize) -> Local {
    let entry = &map.entries[index];
    let Some(expected) = &entry.expected else {
        return Local::Waiting;
    };
    let streams = entry.streams.then_some(entry.length);
    let (header, stretch_end) = (entry.header, map.stretch_end(index));
    let fixed = ByteRange::new(header, (header + LOCAL_LEN).min(stretch_end));
    let Some(bytes) = kept.read(fixed) else {
        return Local::Waiting;
    };
    let Some(len) = records::local_len(bytes) else {
        return Local::Damaged(fixed);
    };
    let data = header + len;
    // A header, or data, that runs into the next header is no layout this
    // store reads.
    if data + streams.unwrap_or(0) > stretch_end {
        return Local::Unsupported;
    }
    let range = ByteRange::new(header, data);
    kept.want(range);
    let Some(bytes) = kept.read(range) else {
        return Local::Waiting;
    };
    let Some(local) = records::local(bytes) else {
        return Local::Damaged(range);
    };
    if !records::agree(&local, expected) {
        return Local::Unsupported;
    }
    Local::Agreed {
        range,
        crc: crc32(bytes),
        data: streams.map(|_| data),
    }
}

// The members two maps place differently, by id, ascending: said as
// another member, or at other bytes of the container, or at bytes the new
// map has not placed yet.
fn moved(old: &Map, new: &Map) -> Vec<usize> {
    (0..old.members.len().max(new.members.len()))
        .filter(|&id| {
            let member = MemberId(id as u32);
            match (old.entry(member), new.entry(member)) {
                (Some(was), Some(is)) => {
                    was.name != is.name
                        || was.length != is.length
                        || was.checksum != is.checksum
                        || was.crc != is.crc
                        || was.streams != is.streams
                        || was.header != is.header
                        || was.span() != is.span()
                }
                _ => true,
            }
        })
        .collect()
}
