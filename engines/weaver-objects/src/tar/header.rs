// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One tar entry's headers, read from where it starts: the 512-byte header,
// and ahead of it any extension headers that rename or resize it, a pax
// record set or a GNU long name, each with its data padded to 512. Every
// dialect is read the same way: ustar with its prefix, GNU, pax, and the
// pre-POSIX header with no magic at all.
//
// Only a regular entry is a file. A GNU sparse entry, a multi-volume
// continuation, a contiguous file and any type tar does not name are not
// extracted: their stored data is envelope. A sparse entry's map may run on
// into extension blocks after its header, and its data starts past them.

use weaver_graph::ByteRange;

use crate::kept::Kept;

pub(super) const BLOCK: u64 = 512;

// The most an extension's data may hold: it is kept in memory to be read.
const EXTENSION_LIMIT: u64 = 1 << 20;

const NAME: std::ops::Range<usize> = 0..100;
const SIZE: std::ops::Range<usize> = 124..136;
const CHECKSUM: std::ops::Range<usize> = 148..156;
const TYPEFLAG: usize = 156;
const MAGIC: std::ops::Range<usize> = 257..263;
const PREFIX: std::ops::Range<usize> = 345..500;
const GNU_MAGIC: std::ops::Range<usize> = 257..265;
// Whether another block of the sparse map follows: in the header, and at the
// end of every extension block.
const SPARSE_EXTENDED: usize = 482;
const EXTENSION_EXTENDED: usize = 504;

// What the entry's data is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Body {
    File,
    Directory,
    // A link, a device, a fifo, a volume label, a sparse or continued
    // piece, a type tar does not name, or a lone zero block: not a file
    // body, so its data, if any, is envelope.
    Skipped,
}

#[derive(Debug)]
pub(super) struct Entry {
    // Where the headers the entry was read from end and its data begins.
    pub data: u64,
    pub name: String,
    pub size: u64,
    pub body: Body,
}

#[derive(Debug)]
pub(super) enum Read {
    // These bytes, from the entry's start, are needed first.
    Wait(ByteRange),
    // This header's checksum failed.
    Damaged(ByteRange),
    // The two zero blocks that end the archive.
    End(ByteRange),
    Entry(Entry),
    // A size field nothing can read, or an extension too large to keep.
    Unsupported,
}

// What extension headers said of the entry they precede.
#[derive(Default)]
struct Extended {
    path: Option<String>,
    long_name: Option<String>,
    size: Option<u64>,
}

pub(super) fn read(kept: &Kept, start: u64) -> Read {
    let mut at = start;
    let mut extended = Extended::default();
    loop {
        let block = ByteRange::at(at, BLOCK);
        let Some(header) = kept.read(block) else {
            return Read::Wait(ByteRange::new(start, block.end));
        };
        if is_zero(header) {
            return zero(kept, start, at);
        }
        if !checksum_matches(header) {
            return Read::Damaged(block);
        }
        let Some(size) = number(&header[SIZE]) else {
            return Read::Unsupported;
        };
        let typeflag = header[TYPEFLAG];
        if typeflag == b'S' {
            return sparse(kept, start, header, block.end, extended, size);
        }
        if !matches!(typeflag, b'x' | b'g' | b'L' | b'K') {
            return Read::Entry(entry(header, block.end, extended, size));
        }
        if size > EXTENSION_LIMIT {
            return Read::Unsupported;
        }
        let data = ByteRange::at(block.end, size);
        let end = block.end + padded(size);
        let Some(bytes) = read_range(kept, data) else {
            return Read::Wait(ByteRange::new(start, end));
        };
        match typeflag {
            b'x' => pax(bytes, &mut extended),
            b'L' => extended.long_name = Some(text(bytes)),
            // A global pax set and a GNU long link name say nothing of a
            // file body's name or size.
            _ => {}
        }
        at = end;
    }
}

// A zero block ends the archive when another follows it; alone, it is
// padding between entries. One inside an extension chain breaks the chain.
fn zero(kept: &Kept, start: u64, at: u64) -> Read {
    let block = ByteRange::at(at, BLOCK);
    if at != start {
        return Read::Damaged(block);
    }
    let next = ByteRange::at(block.end, BLOCK);
    match kept.read(next) {
        None => Read::Wait(ByteRange::new(start, next.end)),
        Some(bytes) if is_zero(bytes) => Read::End(ByteRange::new(start, next.end)),
        Some(_) => Read::Entry(Entry {
            data: block.end,
            name: String::new(),
            size: 0,
            body: Body::Skipped,
        }),
    }
}

// A GNU sparse entry, read past the extension blocks its map runs on into.
// Only a GNU header carries the map; a sparse type on any other is refused.
fn sparse(
    kept: &Kept,
    start: u64,
    header: &[u8],
    mut data: u64,
    extended: Extended,
    size: u64,
) -> Read {
    if &header[GNU_MAGIC] != b"ustar  \0" {
        return Read::Unsupported;
    }
    let mut more = header[SPARSE_EXTENDED] == 1;
    while more {
        let block = ByteRange::at(data, BLOCK);
        let Some(extension) = kept.read(block) else {
            return Read::Wait(ByteRange::new(start, block.end));
        };
        more = extension[EXTENSION_EXTENDED] == 1;
        data = block.end;
    }
    Read::Entry(entry(header, data, extended, size))
}

pub(super) fn is_zero_block(kept: &Kept, at: u64) -> bool {
    kept.read(ByteRange::at(at, BLOCK)).is_some_and(is_zero)
}

fn entry(header: &[u8], data: u64, extended: Extended, size: u64) -> Entry {
    let name = extended
        .path
        .or(extended.long_name)
        .unwrap_or_else(|| ustar_name(header));
    let typeflag = header[TYPEFLAG];
    let body = match typeflag {
        b'5' => Body::Directory,
        // A pre-POSIX header marks a directory by its trailing slash alone.
        b'0' | 0 if name.ends_with('/') => Body::Directory,
        b'0' | 0 => Body::File,
        _ => Body::Skipped,
    };
    Entry {
        data,
        name,
        size: extended.size.unwrap_or(size),
        body,
    }
}

// The name field, with POSIX ustar's prefix before it. GNU's "ustar  " keeps
// other fields where the prefix would be, so only "ustar\0" has one.
fn ustar_name(header: &[u8]) -> String {
    let name = text(&header[NAME]);
    if &header[MAGIC] != b"ustar\0" {
        return name;
    }
    let prefix = text(&header[PREFIX]);
    if prefix.is_empty() {
        name
    } else {
        format!("{prefix}/{name}")
    }
}

// pax records: "<length> <key>=<value>\n", the length counting the whole
// record.
fn pax(mut records: &[u8], extended: &mut Extended) {
    while let Some(space) = records.iter().position(|&byte| byte == b' ') {
        let Some(length) = std::str::from_utf8(&records[..space])
            .ok()
            .and_then(|length| length.parse::<usize>().ok())
            .filter(|length| *length > space && *length <= records.len())
        else {
            return;
        };
        let record = &records[space + 1..length];
        let record = record.strip_suffix(b"\n").unwrap_or(record);
        if let Some(equals) = record.iter().position(|&byte| byte == b'=') {
            let value = &record[equals + 1..];
            match &record[..equals] {
                b"path" => extended.path = Some(String::from_utf8_lossy(value).into_owned()),
                b"size" => {
                    extended.size = std::str::from_utf8(value)
                        .ok()
                        .and_then(|size| size.parse().ok());
                }
                _ => {}
            }
        }
        records = &records[length..];
    }
}

// The bytes of a range that may be empty.
fn read_range(kept: &Kept, range: ByteRange) -> Option<&[u8]> {
    if range.is_empty() {
        return Some(&[]);
    }
    kept.read(range)
}

pub(super) fn padded(size: u64) -> u64 {
    size.div_ceil(BLOCK) * BLOCK
}

fn is_zero(block: &[u8]) -> bool {
    block.iter().all(|&byte| byte == 0)
}

fn text(field: &[u8]) -> String {
    let end = field
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

// A numeric field: octal text, space or NUL padded, or GNU's base-256 with
// the top bit of the first byte set.
fn number(field: &[u8]) -> Option<u64> {
    if let Some((&first, rest)) = field.split_first()
        && first & 0x80 != 0
    {
        // A negative base-256 number is no size.
        if first & 0x40 != 0 {
            return None;
        }
        return rest
            .iter()
            .try_fold(u64::from(first & 0x3F), |value, &byte| {
                value.checked_mul(256)?.checked_add(u64::from(byte))
            });
    }
    let digits = field
        .iter()
        .skip_while(|&&byte| byte == b' ' || byte == 0)
        .take_while(|&&byte| byte != b' ' && byte != 0);
    // An empty field is zero, which old writers leave for an empty file.
    digits.fold(Some(0u64), |value, &digit| {
        (b'0'..=b'7').contains(&digit).then_some(())?;
        value?.checked_mul(8)?.checked_add(u64::from(digit - b'0'))
    })
}

// The checksum field holds the sum of the header's bytes with the field
// itself read as spaces; some old writers summed them as signed bytes.
fn checksum_matches(header: &[u8]) -> bool {
    let Some(stored) = number(&header[CHECKSUM]).and_then(|stored| i64::try_from(stored).ok())
    else {
        return false;
    };
    let spaces = CHECKSUM.len() as i64 * i64::from(b' ');
    let outside = |byte: &(usize, &u8)| !CHECKSUM.contains(&byte.0);
    let unsigned: i64 = header
        .iter()
        .enumerate()
        .filter(outside)
        .map(|(_, &byte)| i64::from(byte))
        .sum();
    let signed: i64 = header
        .iter()
        .enumerate()
        .filter(outside)
        .map(|(_, &byte)| i64::from(byte as i8))
        .sum();
    stored == unsigned + spaces || stored == signed + spaces
}
