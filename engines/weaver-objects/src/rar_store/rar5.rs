// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One RAR5 header from its plaintext bytes. Every header is a CRC32 over the
// rest of it, a vint size, then a vint type and flags, optional extra and
// data sizes, the type's own fields, and an extra area at its end.

use super::header::{File, FileCrypt, Header, HeaderCrypt, Main, Read, What, check_value};
use super::reader::Reader;
use crate::grid::crc32;

// The smallest header: the CRC, a one-byte size, a type and flags.
pub(super) const MIN: u64 = 7;

// The format's own bound on a header, which also bounds what a damaged size
// field can make the store keep.
const MAX: u64 = 2 * 1024 * 1024;

const MAIN: u64 = 1;
const FILE: u64 = 2;
const SERVICE: u64 = 3;
const CRYPT: u64 = 4;
const END: u64 = 5;

const HAS_EXTRA: u64 = 0x1;
const HAS_DATA: u64 = 0x2;
const SPLIT_BEFORE: u64 = 0x8;
const SPLIT_AFTER: u64 = 0x10;

// The header's length from its first bytes, once they are in.
pub(super) fn length(bytes: &[u8]) -> Read {
    let mut reader = Reader::new(bytes);
    let size = reader.take(4).and_then(|_| {
        let start = reader.position();
        let size = reader.vint()?;
        Some((size, reader.position() - start))
    });
    match size {
        // The size vint is three bytes at most, so seven bytes always hold it.
        None if (bytes.len() as u64) < MIN => Read::Short(MIN),
        None => Read::Bad(MIN),
        Some((size, width)) if !(2..=MAX).contains(&size) || width > 3 => Read::Bad(MIN),
        Some((size, width)) => Read::Short(4 + width as u64 + size),
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
    let crc = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if crc32(&bytes[4..]) != crc {
        return Read::Bad(total);
    }
    let mut reader = Reader::new(&bytes[4..]);
    let parsed = reader
        .vint()
        .and_then(|_| body(&bytes[4 + reader.position()..]));
    match parsed {
        Some((data, what)) => Read::Header(Header {
            len: total,
            data,
            what,
        }),
        None => Read::Broken,
    }
}

fn body(bytes: &[u8]) -> Option<(u64, What)> {
    let mut reader = Reader::new(bytes);
    let kind = reader.vint()?;
    let flags = reader.vint()?;
    let extra = if flags & HAS_EXTRA != 0 {
        reader.vint()?
    } else {
        0
    };
    let data = if flags & HAS_DATA != 0 {
        reader.vint()?
    } else {
        0
    };
    let fields_end = bytes.len().checked_sub(usize::try_from(extra).ok()?)?;
    let start = reader.position();
    let fields = bytes.get(start..fields_end)?;
    let extra = &bytes[fields_end..];
    let what = match kind {
        MAIN => What::Main(main(fields)?),
        FILE | SERVICE => {
            let file = file(fields, extra, flags)?;
            match (kind, file.name.as_str()) {
                (FILE, _) => What::File(file),
                (_, "RR") => What::Recovery,
                _ => What::Other,
            }
        }
        CRYPT => What::Crypt(crypt(fields)?),
        END => What::End {
            more: Reader::new(fields).vint()? & 0x1 != 0,
        },
        _ => What::Other,
    };
    Some((data, what))
}

fn main(fields: &[u8]) -> Option<Main> {
    let mut reader = Reader::new(fields);
    let flags = reader.vint()?;
    let number = if flags & 0x2 != 0 {
        Some(u32::try_from(reader.vint()?).ok()?)
    } else {
        None
    };
    Some(Main {
        volume: flags & 0x1 != 0,
        // The first volume carries no number: it is volume zero.
        number: number.or((flags & 0x1 != 0).then_some(0)),
        solid: flags & 0x4 != 0,
        encrypted: false,
    })
}

fn file(fields: &[u8], extra: &[u8], flags: u64) -> Option<File> {
    let mut reader = Reader::new(fields);
    let file_flags = reader.vint()?;
    let unpacked = reader.vint()?;
    let _attributes = reader.vint()?;
    if file_flags & 0x2 != 0 {
        reader.u32()?;
    }
    let crc = if file_flags & 0x4 != 0 {
        Some(reader.u32()?)
    } else {
        None
    };
    let compression = reader.vint()?;
    let _host = reader.vint()?;
    let name_len = usize::try_from(reader.vint()?).ok()?;
    let name = String::from_utf8_lossy(reader.take(name_len)?).into_owned();
    let method = (compression >> 7) & 0x7;
    let solid = compression & 0x40 != 0;
    Some(File {
        name,
        dir: file_flags & 0x1 != 0,
        length: (file_flags & 0x8 == 0).then_some(unpacked),
        crc,
        before: flags & SPLIT_BEFORE != 0,
        after: flags & SPLIT_AFTER != 0,
        stored: method == 0 && !solid,
        crypt: file_crypt(extra)?,
    })
}

// The file encryption record among the extra records, if there is one.
fn file_crypt(extra: &[u8]) -> Option<Option<FileCrypt>> {
    let mut reader = Reader::new(extra);
    while reader.position() < extra.len() {
        let size = usize::try_from(reader.vint()?).ok()?;
        let record = reader.take(size)?;
        let mut record = Reader::new(record);
        if record.vint()? != 0x1 {
            continue;
        }
        if record.vint()? != 0 {
            return Some(Some(FileCrypt::Other));
        }
        let flags = record.vint()?;
        let count = record.u8()?;
        let salt = record.array()?;
        let iv = record.array()?;
        let check = if flags & 0x1 != 0 {
            check_value(record.array()?)
        } else {
            None
        };
        return Some(Some(FileCrypt::Rar5 {
            count,
            salt,
            iv,
            check,
            hash_key: flags & 0x2 != 0,
        }));
    }
    Some(None)
}

fn crypt(fields: &[u8]) -> Option<HeaderCrypt> {
    let mut reader = Reader::new(fields);
    if reader.vint()? != 0 {
        return None;
    }
    let flags = reader.vint()?;
    let count = reader.u8()?;
    let salt = reader.array()?;
    let check = if flags & 0x1 != 0 {
        check_value(reader.array()?)
    } else {
        None
    };
    Some(HeaderCrypt { count, salt, check })
}
