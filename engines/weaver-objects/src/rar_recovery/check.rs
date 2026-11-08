// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// The volume checks below follow unrar-rs 0.15.0 `recovery.rs`
// (`rar5_data_volume_status`, `Rar3Walk::step`, `rar3_volume_walk`),
// Apache-2.0, itself developed from RARLAB's unRAR source (`recvol5.cpp`,
// `recvol3.cpp`), whose licence asks this paragraph be kept:
//
// UnRAR source code may be used in any software to handle RAR archives
// without limitations free of charge, but cannot be used to develop RAR
// (WinRAR) compatible archiver and to re-create RAR compression algorithm,
// which is proprietary. Distribution of modified UnRAR source code in
// separate form or as a part of other software is permitted, provided that
// full text of this paragraph, starting from "UnRAR source code" words, is
// included in license, or in documentation if license is not available, and
// in source code comments of resulting package.

// Whether a data volume of a recovery set is whole, by the checksum RAR
// keeps for it: a RAR5 set's table gives each volume's size and the CRC32
// of all its bytes, and a RAR3 set's recovery volumes give none, so a RAR
// 1.5 to 4.x volume is held to what its own headers state, the
// end-of-archive header's CRC32 of every byte before it among them.
//
// Both are CRC32s over the front of the volume, so the grid's fold of the
// articles' own CRC32s over the whole volume proves or refutes one without
// a read: the RAR4 one through the few bytes from its end header on, which
// its header walk reads anyway. Only a volume the fold cannot speak for is
// read back.

use weaver_graph::{ByteRange, SliceVerdict};

use crate::grid::{self, Folded};

// How much is read at once, so a volume never sits in memory whole.
const CHUNK: u64 = 1 << 20;

const RAR4_SIGN: &[u8] = b"Rar!\x1a\x07\x00";
const HEADER_MIN: u64 = 7;
const MAIN: u8 = 0x73;
const FILE: u8 = 0x74;
const SERVICE: u8 = 0x7a;
const END: u8 = 0x7b;
// The main header's flag for headers enciphered past it.
const SEALED: u16 = 0x0080;
// A file or service header's flag for a packed size past 32 bits.
const LARGE: u16 = 0x0100;
// Any header's flag for a data area after it.
const DATA: u16 = 0x8000;
// The end-of-archive header's flag for the CRC32 of the bytes before it.
const END_CRC: u16 = 0x0002;

// Reads a range of a volume's bytes onto the end of a buffer: its member
// through the namespace, or the file a restore rebuilt it in.
pub(super) type Reader<'a> = dyn FnMut(ByteRange, &mut Vec<u8>) -> bool + 'a;

// One data volume, read by range into one buffer the ruling lends, beside
// the grid's fold of all its bytes when the volume arrived whole.
pub(super) struct Volume<'a> {
    read: &'a mut Reader<'a>,
    length: u64,
    fold: Option<Folded>,
    buffer: &'a mut Vec<u8>,
}

impl<'a> Volume<'a> {
    pub(super) fn new(
        read: &'a mut Reader<'a>,
        length: u64,
        fold: Option<Folded>,
        buffer: &'a mut Vec<u8>,
    ) -> Self {
        Self {
            read,
            length,
            fold,
            buffer,
        }
    }

    // The volume's `len` bytes at `at` into the buffer.
    fn read(&mut self, at: u64, len: u64) -> bool {
        let Some(end) = at.checked_add(len).filter(|end| *end <= self.length) else {
            return false;
        };
        self.buffer.clear();
        (self.read)(ByteRange::new(at, end), self.buffer) && self.buffer.len() as u64 == len
    }

    // The CRC32 of the volume's first `end` bytes, read back.
    fn crc32(&mut self, end: u64) -> Option<u32> {
        let mut crc = grid::crc32(&[]);
        let mut at = 0;
        while at < end {
            let len = CHUNK.min(end - at);
            if !self.read(at, len) {
                return None;
            }
            crc = grid::crc32_combine(crc, grid::crc32(self.buffer), len);
            at += len;
        }
        Some(crc)
    }

    // Whether the volume's first `end` bytes have the CRC32 `stated`. The
    // fold proves the whole volume, so the bytes past `end` are read, and
    // the CRC32 the volume has when the front has `stated` is what the fold
    // must give: the front's CRC32 is the one that, followed by those bytes,
    // gives the fold. The front is read back only when the fold says
    // nothing.
    fn holds(&mut self, end: u64, stated: u32) -> bool {
        let tail = self.length - end;
        let whole = if tail == 0 {
            stated
        } else if self.read(end, tail) {
            grid::crc32_combine(stated, grid::crc32(self.buffer), tail)
        } else {
            return false;
        };
        match grid::verdict(self.fold, self.length, 0, whole) {
            SliceVerdict::Intact => true,
            SliceVerdict::Damaged => false,
            SliceVerdict::Unknown => self.crc32(end) == Some(stated),
        }
    }

    // A RAR5 set's table row for it: its size, then the CRC32 of all its
    // bytes, as unRAR checks a data volume before it rebuilds a set.
    pub(super) fn matches(&mut self, size: u64, crc: u32) -> bool {
        self.length == size && self.holds(size, crc)
    }

    // A RAR 1.5 to 4.x volume's own headers: the signature, every header
    // through to the end-of-archive header under its CRC16, and the CRC32
    // of the bytes before that header, when it states one. One whose
    // headers are enciphered shows nothing past its main header and stands
    // on that, as unRAR takes it. `None` when the volume is no such one.
    pub(super) fn sound_rar4(&mut self) -> Option<bool> {
        if !self.read(0, RAR4_SIGN.len() as u64) || self.buffer.as_slice() != RAR4_SIGN {
            return None;
        }
        let mut at = RAR4_SIGN.len() as u64;
        let mut first = true;
        Some(loop {
            if !self.read(at, HEADER_MIN) {
                break false;
            }
            let flags = le_u16(&self.buffer[3..5]);
            let size = u64::from(le_u16(&self.buffer[5..7]));
            if size < HEADER_MIN || !self.read(at, size) {
                break false;
            }
            let header = self.buffer.as_slice();
            if grid::crc32(&header[2..]) as u16 != le_u16(&header[..2]) {
                break false;
            }
            let kind = header[2];
            if first {
                if kind != MAIN {
                    break false;
                }
                if flags & SEALED != 0 {
                    break true;
                }
                first = false;
            }
            if kind == END {
                if flags & END_CRC == 0 {
                    break true;
                }
                let Some(stated) = header.get(7..11).map(le_u32) else {
                    break false;
                };
                break self.holds(at, stated);
            }
            // A file or service header's packed size, else the data area
            // any header may declare.
            let packed = matches!(kind, FILE | SERVICE);
            let low = match header.get(7..11) {
                Some(low) if packed || flags & DATA != 0 => u64::from(le_u32(low)),
                _ => 0,
            };
            let high = match header.get(32..36) {
                Some(high) if packed && flags & LARGE != 0 => u64::from(le_u32(high)),
                _ => 0,
            };
            match at.checked_add(size + (high << 32 | low)) {
                Some(next) if next <= self.length => at = next,
                _ => break false,
            }
        })
    }
}

fn le_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes([bytes[0], bytes[1]])
}

fn le_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}
