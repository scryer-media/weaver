// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Walking the local headers from the front, when the directory cannot be
// had first. Each header says its entry's name and sizes, so a stored entry
// streams as soon as its header is in; the walk stops at the directory,
// which every header walked must then agree with. A header whose sizes
// follow the data cannot say where its entry ends, and a local header
// carries no file mode, so here a link reads as a file.

use weaver_graph::{ByteRange, Emit, MemberId, Why};

use super::directory::Said;
use super::map::{Entry, Map};
use super::records::{self, LOCAL_LEN, Next};
use super::{Zip, refusal};
use crate::grid::crc32;

// The bytes that say what a header is.
const SIGNATURE_LEN: u64 = 4;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct Descriptor {
    at: u64,
    member: MemberId,
    crc: u32,
    compressed: u64,
    length: u64,
    wide: bool,
}

impl Zip {
    // Keep a range the walk reads in the normal order; its bytes once in.
    fn read_ahead(&mut self, range: ByteRange) -> Option<Vec<u8>> {
        self.want(range, false);
        self.kept.read(range).map(<[u8]>::to_vec)
    }

    pub(super) fn walk(&mut self, out: &mut Vec<Emit>) {
        loop {
            if !self.read_descriptor(out) {
                return;
            }
            let map = self.map.get_or_insert_with(Map::default);
            if map.end.is_some() {
                return;
            }
            let (cursor, first) = (map.frontier, map.entries.is_empty());
            let signature = ByteRange::at(cursor, SIGNATURE_LEN);
            let Some(bytes) = self.read_ahead(signature) else {
                return;
            };
            // A spanned set opens with a marker ahead of its first header.
            if first && cursor == 0 && records::marker(&bytes) == Some(SIGNATURE_LEN) {
                self.advance(SIGNATURE_LEN);
                continue;
            }
            match records::next(&bytes) {
                Some(Next::Local) => {}
                Some(Next::Directory) => {
                    if let Some(map) = &mut self.map {
                        map.end = Some(cursor);
                    }
                    return;
                }
                _ => return self.say_damaged(signature, out),
            }
            let fixed = ByteRange::at(cursor, LOCAL_LEN);
            let Some(bytes) = self.read_ahead(fixed) else {
                return;
            };
            let Some(len) = records::local_len(&bytes) else {
                return self.say_damaged(fixed, out);
            };
            let range = ByteRange::at(cursor, len);
            let Some(bytes) = self.read_ahead(range) else {
                return;
            };
            let Some(header) = records::local(&bytes) else {
                return self.say_damaged(range, out);
            };
            if let Some(why) = walk_refusal(&header) {
                return self.give_up(Emit::NeedsEngine(why), out);
            }
            self.basis.push((range, crc32(&bytes)));
            let member = MemberId(self.members.len() as u32);
            let said = Said::of(&header);
            let length = self.announce(member, &header, out);
            let data = cursor + len;
            let compressed = header.compressed;
            // A directory holds nothing to decrypt: its encryption header,
            // if a writer gave it one, is the envelope's.
            let cipher = (header.encrypted() && header.shape != records::Shape::Directory)
                .then(|| super::crypto::Cipher::new(&header));
            let prefix = if cipher.is_some() {
                super::crypto::HEADER
            } else {
                0
            };
            self.want(ByteRange::at(data, prefix), false);
            let Some(map) = &mut self.map else {
                return;
            };
            map.members.push(map.entries.len());
            map.entries.push(Entry {
                header: cursor,
                member: Some(member),
                name: said.name,
                checksum: said.checksum,
                length,
                crc: header.crc,
                crc_known: !header.deferred() || header.crc != 0,
                data: Some(data + prefix),
                cipher,
                streams: length > 0 || prefix > 0,
                expected: Some(header.clone()),
            });
            map.frontier = data + compressed;
            if header.deferred() {
                map.descriptor = Some(Descriptor {
                    at: map.frontier,
                    member,
                    crc: header.crc,
                    compressed,
                    length,
                    wide: records::local_zip64(&bytes),
                });
            }
            // What follows the data, a directory entry's included, is the
            // envelope's.
            if length == 0 {
                self.want(ByteRange::new(data, data + compressed), false);
            }
            self.emit_kept(member, out);
        }
    }

    fn advance(&mut self, by: u64) {
        if let Some(map) = &mut self.map {
            map.frontier += by;
        }
    }

    fn read_descriptor(&mut self, out: &mut Vec<Emit>) -> bool {
        let Some(descriptor) = self.map.as_ref().and_then(|map| map.descriptor.clone()) else {
            return true;
        };
        let body = if descriptor.wide { 20 } else { 12 };
        let mut range = ByteRange::at(descriptor.at, body);
        let Some(mut bytes) = self.read_ahead(range) else {
            return false;
        };
        let matches = |bytes: &[u8], at: usize| {
            let crc = records::u32_at(bytes, at)?;
            let (compressed, length) = if descriptor.wide {
                (
                    records::u64_at(bytes, at + 4),
                    records::u64_at(bytes, at + 12),
                )
            } else {
                (
                    records::u32_at(bytes, at + 4).map(u64::from),
                    records::u32_at(bytes, at + 8).map(u64::from),
                )
            };
            ((descriptor.crc == 0 || crc == descriptor.crc)
                && compressed == Some(descriptor.compressed)
                && length == Some(descriptor.length))
            .then_some(crc)
        };
        // The signature is optional, and its value is also a possible
        // CRC. Match the unsigned fields before interpreting that word.
        let crc = match matches(&bytes, 0) {
            Some(crc) => crc,
            None if records::u32_at(&bytes, 0) == Some(0x0807_4b50) => {
                range = ByteRange::at(descriptor.at, body + 4);
                let Some(signed) = self.read_ahead(range) else {
                    return false;
                };
                bytes = signed;
                let Some(crc) = matches(&bytes, 4) else {
                    self.say_damaged(range, out);
                    return false;
                };
                crc
            }
            None => {
                self.say_damaged(range, out);
                return false;
            }
        };
        self.basis.push((range, crc32(&bytes)));
        if let Some(map) = &mut self.map {
            map.frontier = range.end;
            map.descriptor = None;
            let index = map.members[descriptor.member.0 as usize];
            map.entries[index].crc = crc;
            map.entries[index].crc_known = true;
        }
        self.record(
            descriptor.member,
            descriptor.length,
            Vec::new().into(),
            weaver_graph::Provenance::Fresh,
            out,
        );
        true
    }
}

// Why the walk cannot stream an entry, if it cannot. One whose extent the
// header does not say stops the walk whatever it holds.
fn walk_refusal(header: &records::Header) -> Option<Why> {
    if header.deferred() && (!header.encrypted() || header.compressed == 0) {
        return Some(Why::UnboundedEntry);
    }
    refusal(header)
}
