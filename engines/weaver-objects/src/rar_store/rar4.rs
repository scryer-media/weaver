// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One RAR4 block from its plaintext bytes. Every block is the low half of a
// CRC32 over the rest of it, a type, flags and a size; a long block's data
// size follows, and a file block's fields after that.

use super::header::{File, FileCrypt, Header, Main, Read, What};
use super::reader::Reader;
use crate::grid::crc32;

// The smallest block: CRC, type, flags, size.
pub(super) const MIN: u64 = 7;

const MAIN: u8 = 0x73;
const FILE: u8 = 0x74;
const PROTECT: u8 = 0x78;
const SERVICE: u8 = 0x7a;
const END: u8 = 0x7b;

const LONG_BLOCK: u16 = 0x8000;

pub(super) fn length(bytes: &[u8]) -> Read {
    let mut reader = Reader::new(bytes);
    let size = reader.take(5).and_then(|_| reader.u16());
    match size {
        None => Read::Short(MIN),
        Some(size) if u64::from(size) < MIN => Read::Bad(MIN),
        Some(size) => Read::Short(u64::from(size)),
    }
}

pub(super) fn read(bytes: &[u8]) -> Read {
    let total = match length(bytes) {
        Read::Short(total) => total,
        other => return other,
    };
    if (bytes.len() as u64) < total {
        return Read::Short(total);
    }
    let bytes = &bytes[..total as usize];
    let crc = u16::from_le_bytes([bytes[0], bytes[1]]);
    if crc32(&bytes[2..]) as u16 != crc {
        return Read::Bad(total);
    }
    match block(bytes) {
        Some((data, what)) => Read::Header(Header {
            len: total,
            data,
            what,
        }),
        None => Read::Broken,
    }
}

fn block(bytes: &[u8]) -> Option<(u64, What)> {
    let mut reader = Reader::new(bytes);
    reader.take(2)?;
    let kind = reader.u8()?;
    let flags = reader.u16()?;
    reader.u16()?;
    match kind {
        MAIN => Some((
            0,
            What::Main(Main {
                volume: flags & 0x1 != 0,
                // RAR4 numbers its volumes only in the end block.
                number: None,
                solid: flags & 0x8 != 0,
                encrypted: flags & 0x80 != 0,
            }),
        )),
        FILE | SERVICE => {
            let (packed, file) = file(&mut reader, flags)?;
            let what = match (kind, file.name.as_str()) {
                (FILE, _) => What::File(file),
                (_, "RR") => What::Recovery,
                _ => What::Other,
            };
            Some((packed, what))
        }
        _ => {
            let data = if flags & LONG_BLOCK != 0 {
                u64::from(reader.u32()?)
            } else {
                0
            };
            let what = match kind {
                PROTECT => What::Recovery,
                END => What::End {
                    more: flags & 0x1 != 0,
                },
                _ => What::Other,
            };
            Some((data, what))
        }
    }
}

fn file(reader: &mut Reader<'_>, flags: u16) -> Option<(u64, File)> {
    let packed = reader.u32()?;
    let unpacked = reader.u32()?;
    let _host = reader.u8()?;
    let crc = reader.u32()?;
    let _time = reader.u32()?;
    let version = reader.u8()?;
    let method = reader.u8()?;
    let name_len = usize::from(reader.u16()?);
    let _attributes = reader.u32()?;
    let (packed, unpacked) = if flags & 0x100 != 0 {
        let high_packed = reader.u32()?;
        let high_unpacked = reader.u32()?;
        (
            u64::from(high_packed) << 32 | u64::from(packed),
            u64::from(high_unpacked) << 32 | u64::from(unpacked),
        )
    } else {
        (u64::from(packed), u64::from(unpacked))
    };
    let raw = reader.take(name_len)?;
    let salt: Option<[u8; 8]> = if flags & 0x400 != 0 {
        Some(reader.array()?)
    } else {
        None
    };
    let crypt = (flags & 0x4 != 0).then(|| match salt {
        // AES-128 arrived with RAR 2.9; anything older is a cipher of its own.
        Some(salt) if version >= 29 => FileCrypt::Rar4 { salt },
        _ => FileCrypt::Other,
    });
    Some((
        packed,
        File {
            name: name(raw, flags & 0x200 != 0),
            dir: flags & 0xe0 == 0xe0,
            length: Some(unpacked),
            crc: Some(crc),
            before: flags & 0x1 != 0,
            after: flags & 0x2 != 0,
            stored: method == 0x30 && flags & 0x10 == 0,
            crypt,
        },
    ))
}

// A RAR4 name: plain bytes, or with the Unicode flag either UTF-8 or the
// plain name, a zero, and RAR's own compact encoding of the wide name.
fn name(raw: &[u8], unicode: bool) -> String {
    match raw.iter().position(|byte| *byte == 0) {
        Some(zero) if unicode => wide(&raw[..zero], &raw[zero + 1..]),
        _ => String::from_utf8_lossy(raw).into_owned(),
    }
}

// The wide name from the plain one and its encoding: a high byte, then two
// bits per character saying how each is made, MSB first.
fn wide(plain: &[u8], encoded: &[u8]) -> String {
    let mut out: Vec<u16> = Vec::new();
    let mut reader = Reader::new(encoded);
    let high = u16::from(reader.u8().unwrap_or(0));
    let mut flags = 0u8;
    let mut bits = 0;
    while reader.position() < encoded.len() {
        if bits == 0 {
            let Some(byte) = reader.u8() else {
                break;
            };
            flags = byte;
            bits = 8;
        }
        let made = match flags >> 6 {
            0 => reader.u8().map(|low| out.push(u16::from(low))),
            1 => reader.u8().map(|low| out.push(u16::from(low) | high << 8)),
            2 => reader.u16().map(|wide| out.push(wide)),
            _ => reader.u8().map(|length| {
                let (count, correction) = if length & 0x80 != 0 {
                    (usize::from(length & 0x7f) + 2, reader.u8())
                } else {
                    (usize::from(length) + 2, None)
                };
                for _ in 0..count {
                    let Some(byte) = plain.get(out.len()) else {
                        break;
                    };
                    out.push(match correction {
                        Some(correction) => u16::from(byte.wrapping_add(correction)) | high << 8,
                        None => u16::from(*byte),
                    });
                }
            }),
        };
        if made.is_none() {
            break;
        }
        flags <<= 2;
        bits -= 2;
    }
    String::from_utf16_lossy(&out)
}
