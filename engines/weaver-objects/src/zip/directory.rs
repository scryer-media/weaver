// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Reading the directory: the end record from the container's tail, the
// zip64 records it points at, then the directory, whose entries are every
// member, said in directory order. One entry the store cannot stream sends
// the whole container to an engine before any member is said. Each stored
// entry's data then waits for its local header, whose own name and extra
// lengths say where the data starts.

use weaver_graph::{ByteRange, Emit, MemberId, VolumeIndex, Why};

use super::map::{Entry, Map, Out};
use super::records::{self, Central, END64_LEN, LOCAL_LEN, LOCATOR_LEN, Shape, TAIL};
use super::{Zip, refusal};
use crate::grid::crc32;

impl Zip {
    // Keep and bid for a range the directory read needs; its bytes once they
    // are all in.
    fn need(&mut self, range: ByteRange) -> Option<Vec<u8>> {
        if !self.directory.contains(&range) {
            self.directory.push(range);
        }
        self.want(range, true);
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
        self.basis = read
            .into_iter()
            .filter_map(|range| self.kept.read(range).map(|bytes| (range, crc32(bytes))))
            .collect();
        self.build(&centrals, start, out);
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
            ..Map::default()
        };
        // Every member is said now, in directory order, which is the order
        // the namespace numbers them in.
        let mut ids = vec![None; centrals.len()];
        for (index, central) in centrals.iter().enumerate() {
            if central.header.shape == Shape::Other {
                continue;
            }
            let member = MemberId(self.members.len() as u32);
            ids[index] = Some((member, self.announce(member, &central.header, out)));
        }
        // The entries by header, with every member pointing at its own.
        map.members = vec![0; self.members.len()];
        for index in order {
            let (member, length) =
                ids[index].map_or((None, 0), |(member, length)| (Some(member), length));
            if let Some(member) = member {
                map.members[member.0 as usize] = map.entries.len();
            }
            map.entries.push(Entry {
                header: headers[index],
                member,
                length,
                crc: centrals[index].header.crc,
                data: None,
                streams: length > 0,
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
        self.map = Some(map);
    }

    // Say a member; one with nothing to stream is closed at once. Its length.
    pub(super) fn announce(
        &mut self,
        member: MemberId,
        header: &records::Header,
        out: &mut Vec<Emit>,
    ) -> u64 {
        let directory = header.shape == Shape::Directory;
        let name = records::member_name(header.name.clone(), header.shape);
        let length = if directory { 0 } else { header.length };
        out.push(Emit::Member {
            member,
            name,
            length: Some(length),
        });
        let mut said = Out::default();
        if length == 0 {
            said.closed = true;
            out.push(Emit::MemberClosed(member));
        }
        self.members.push(said);
        length
    }

    // Each stored entry's local header, once it is in: where its data starts.
    pub(super) fn read_locals(&mut self, out: &mut Vec<Emit>) {
        let Some(map) = &self.map else {
            return;
        };
        let waiting: Vec<(usize, u64, u64, u64)> = map
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.streams && entry.data.is_none())
            .map(|(index, entry)| (index, entry.header, entry.length, map.stretch_end(index)))
            .collect();
        for (index, header, length, stretch_end) in waiting {
            let range = ByteRange::new(header, (header + LOCAL_LEN).min(stretch_end));
            let Some(bytes) = self.kept.read(range) else {
                continue;
            };
            let Some(len) = records::local_len(bytes) else {
                self.say_damaged(range, out);
                continue;
            };
            let crc = crc32(bytes);
            let data = header + len;
            // Data that runs into the next header is no layout this store
            // reads.
            if data + length > stretch_end {
                return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            }
            self.basis.push((range, crc));
            self.kept.want(ByteRange::new(header, data));
            self.kept.want(ByteRange::new(data + length, stretch_end));
            let Some(map) = &mut self.map else {
                return;
            };
            map.entries[index].data = Some(data);
            if let Some(member) = map.entries[index].member {
                self.emit_kept(member, out);
            }
        }
    }

    // Whether a lost range takes a header the map still waits for.
    pub(super) fn lost_header(&self, range: ByteRange) -> bool {
        let Some(map) = &self.map else {
            return false;
        };
        let waits = |header: u64| ByteRange::at(header, LOCAL_LEN).intersect(range).is_some();
        let walked = self.walking && map.end.is_none() && waits(map.frontier);
        walked
            || map
                .entries
                .iter()
                .any(|entry| entry.streams && entry.data.is_none() && waits(entry.header))
    }
}
