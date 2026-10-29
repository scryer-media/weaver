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

use weaver_graph::{ByteRange, Emit, MemberId, VolumeIndex, Why};

use super::map::{Entry, Map, Out};
use super::records::{self, Central, END64_LEN, LOCAL_LEN, LOCATOR_LEN, Shape, TAIL};
use super::{Zip, refusal};
use crate::grid::crc32;
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
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        }
        match self.walking {
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
                expected: Some(centrals[index].header.clone()),
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
        // The header's CRC32 is the member's own unless a key went into it.
        if !directory && !header.encrypted() {
            out.push(Emit::Checksum {
                member,
                crc32: header.crc,
            });
        }
        let mut said = Out::default();
        if length == 0 {
            said.closed = true;
            out.push(Emit::MemberClosed(member));
        }
        self.members.push(said);
        length
    }

    // Each entry's local header, once it is in whole: it must say what the
    // directory says, and a stored entry's says where its data starts.
    pub(super) fn read_locals(&mut self, out: &mut Vec<Emit>) {
        let Some(map) = &self.map else {
            return;
        };
        // Each with the length of its data, when the data streams.
        let waiting: Vec<(usize, u64, Option<u64>, u64)> = map
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.expected.is_some())
            .map(|(index, entry)| {
                let streams = entry.streams.then_some(entry.length);
                (index, entry.header, streams, map.stretch_end(index))
            })
            .collect();
        for (index, header, streams, stretch_end) in waiting {
            let fixed = ByteRange::new(header, (header + LOCAL_LEN).min(stretch_end));
            let Some(bytes) = self.kept.read(fixed) else {
                continue;
            };
            let Some(len) = records::local_len(bytes) else {
                self.say_damaged(fixed, out);
                continue;
            };
            let data = header + len;
            // A header, or data, that runs into the next header is no layout
            // this store reads.
            if data + streams.unwrap_or(0) > stretch_end {
                return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            }
            let range = ByteRange::new(header, data);
            self.kept.want(range);
            let Some(bytes) = self.kept.read(range) else {
                continue;
            };
            let Some(local) = records::local(bytes) else {
                self.say_damaged(range, out);
                continue;
            };
            let crc = crc32(bytes);
            let Some(map) = &mut self.map else {
                return;
            };
            let entry = &mut map.entries[index];
            if !entry
                .expected
                .take()
                .is_some_and(|central| records::agree(&local, &central))
            {
                return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            }
            self.basis.push((range, crc));
            let Some(length) = streams else {
                continue;
            };
            entry.data = Some(data);
            let member = entry.member;
            self.kept.want(ByteRange::new(data + length, stretch_end));
            if let Some(member) = member {
                self.emit_kept(member, out);
            }
        }
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
