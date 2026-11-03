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

impl Zip {
    // Keep a range the walk reads in the normal order; its bytes once in.
    fn read_ahead(&mut self, range: ByteRange) -> Option<Vec<u8>> {
        self.want(range, false);
        self.kept.read(range).map(<[u8]>::to_vec)
    }

    pub(super) fn walk(&mut self, out: &mut Vec<Emit>) {
        loop {
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
                data: Some(data),
                streams: length > 0,
                expected: Some(header),
            });
            map.frontier = data + compressed;
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
}

// Why the walk cannot stream an entry, if it cannot. One whose extent the
// header does not say stops the walk whatever it holds.
fn walk_refusal(header: &records::Header) -> Option<Why> {
    if header.deferred() {
        return Some(Why::UnboundedEntry);
    }
    refusal(header)
}
