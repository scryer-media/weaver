// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// The recovery volume header and footer layouts below follow unrar-rs
// 0.11.2 `recovery.rs` (`read_rar5_rev_header`, `parse_rar5_rev_body`,
// `read_rar3_rev_header`), Apache-2.0, itself developed from RARLAB's unRAR
// source, whose licence asks this paragraph be kept:
//
// UnRAR source code may be used in any software to handle RAR archives
// without limitations free of charge, but cannot be used to develop RAR
// (WinRAR) compatible archiver and to re-create RAR compression algorithm,
// which is proprietary. Distribution of modified UnRAR source code in
// separate form or as a part of other software is permitted, provided that
// full text of this paragraph, starting from "UnRAR source code" words, is
// included in license, or in documentation if license is not available, and
// in source code comments of resulting package.

// What a RAR recovery volume says of the set it rebuilds: how many data and
// recovery volumes it has, and in RAR5, each data volume's size. The object
// reads it to name the volumes no member carries; the restore checks every
// checksum itself.

use crate::grid;

const REV5_SIGN: &[u8; 8] = b"Rar!\x1aRev";
const REV5_PREFIX_LEN: usize = 16;
const MAX_RAR5_HEADER: usize = 0x10_0000;
const MAX_RAR5_VOLUMES: usize = 65_535;
pub(super) const RAR3_FOOTER: usize = 7;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct Header {
    pub(super) data_count: u32,
    pub(super) rec_count: u32,
    // Each data volume's size, as a RAR5 volume's table gives it. A RAR3
    // volume is the width of the widest data volume.
    pub(super) sizes: Vec<u64>,
}

pub(super) enum Read {
    Header(Header),
    // The front holds no complete header yet: this many bytes would.
    Short(usize),
    Not,
}

// A RAR5 recovery volume's header, from the contiguous front of the volume.
pub(super) fn rar5(front: &[u8]) -> Read {
    if front.len() < REV5_PREFIX_LEN {
        return if REV5_SIGN.starts_with(&front[..front.len().min(REV5_SIGN.len())]) {
            Read::Short(REV5_PREFIX_LEN)
        } else {
            Read::Not
        };
    }
    if &front[..REV5_SIGN.len()] != REV5_SIGN {
        return Read::Not;
    }
    let block_crc = le_u32(&front[8..12]);
    let header_size = le_u32(&front[12..16]) as usize;
    if !(6..=MAX_RAR5_HEADER).contains(&header_size) {
        return Read::Not;
    }
    let Some(raw) = front.get(REV5_PREFIX_LEN..REV5_PREFIX_LEN + header_size) else {
        return Read::Short(REV5_PREFIX_LEN + header_size);
    };
    if grid::crc32(&front[12..REV5_PREFIX_LEN + header_size]) != block_crc {
        return Read::Not;
    }
    body(raw).map_or(Read::Not, Read::Header)
}

fn body(raw: &[u8]) -> Option<Header> {
    let mut reader = Reader { raw, at: 0 };
    if reader.take(1)? != [1] {
        return None;
    }
    let data_count = usize::from(le_u16(reader.take(2)?));
    let rec_count = usize::from(le_u16(reader.take(2)?));
    let rec_num = usize::from(le_u16(reader.take(2)?));
    let total = data_count + rec_count;
    if data_count == 0 || rec_count == 0 || total > MAX_RAR5_VOLUMES || rec_num >= total {
        return None;
    }
    let _rev_crc = reader.take(4)?;
    // A recovery volume after the first may carry no table; the set's sizes
    // come from one that does.
    let mut sizes = Vec::new();
    if raw.len().saturating_sub(reader.at) >= data_count * 12 {
        for _ in 0..data_count {
            sizes.push(le_u64(reader.take(8)?));
            let _crc = reader.take(4)?;
        }
    }
    Some(Header {
        data_count: u32::try_from(data_count).ok()?,
        rec_count: u32::try_from(rec_count).ok()?,
        sizes,
    })
}

struct Reader<'a> {
    raw: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let field = self.raw.get(self.at..self.at + len)?;
        self.at += len;
        Some(field)
    }
}

// A RAR3 recovery volume's footer, its last seven bytes: the counts less
// one and the CRC32 of everything before the CRC, which the restore checks.
// Every data volume but the last is as long as the recovery volume: RAR
// reserves the footer's seven bytes at the end of each, so the parity of
// those zeros gives way to the footer and the restore writes zeros back.
pub(super) fn rar3(footer: &[u8; RAR3_FOOTER], length: u64) -> Option<Header> {
    let data_count = u32::from(footer[0]) + 1;
    let rec_count = u32::from(footer[1]) + 1;
    let position = u32::from(footer[2]) + 1;
    if data_count + rec_count > 255 || position > rec_count {
        return None;
    }
    let width = length;
    Some(Header {
        data_count,
        rec_count,
        sizes: vec![width; data_count as usize],
    })
}

fn le_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes([bytes[0], bytes[1]])
}

fn le_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn le_u64(bytes: &[u8]) -> u64 {
    let mut word = [0; 8];
    word.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(word)
}
