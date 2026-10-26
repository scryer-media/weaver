// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The zip records, read from bytes: the end of central directory record and
// its zip64 locator and record, the central directory's entries, and the
// local header in front of each entry's data. Every field is little-endian
// at a fixed place; a record that does not fit what it says of itself is
// `None`, which the reader treats as damage. Nothing here decides what to do
// with an entry; it only says what the bytes are.

pub(crate) const LOCAL: u32 = 0x0403_4b50;
pub(crate) const CENTRAL: u32 = 0x0201_4b50;
pub(crate) const END: u32 = 0x0605_4b50;
pub(crate) const END64: u32 = 0x0606_4b50;
pub(crate) const LOCATOR: u32 = 0x0706_4b50;
// What opens the first volume of a spanned set, ahead of its first header.
const SPANNED: [u32; 2] = [0x0807_4b50, 0x3030_4b50];

pub(crate) const LOCAL_LEN: u64 = 30;
pub(crate) const CENTRAL_LEN: usize = 46;
pub(crate) const END_LEN: u64 = 22;
pub(crate) const LOCATOR_LEN: u64 = 20;
pub(crate) const END64_LEN: u64 = 56;
// The end record and the longest comment it can carry.
pub(crate) const TAIL: u64 = END_LEN + u16::MAX as u64;

const ZIP64: u16 = 0x0001;
const AES: u16 = 0x9901;
const AES_METHOD: u16 = 99;
const UTF8: u16 = 1 << 11;
const ENCRYPTED: u16 = 1;
const DESCRIPTOR: u16 = 1 << 3;
const UNIX: u8 = 3;
const KIND: u32 = 0o170_000;
const FILE: u32 = 0o100_000;
const DIRECTORY: u32 = 0o040_000;

pub(crate) fn u16_at(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}

pub(crate) fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

pub(crate) fn u64_at(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

// The length of the spanned marker at the very front of a set, or zero.
pub(crate) fn marker(front: &[u8]) -> Option<u64> {
    let signature = u32_at(front, 0)?;
    Some(if SPANNED.contains(&signature) { 4 } else { 0 })
}

// Where the directory is, from the end record and, for a zip64 archive, its
// record, which overrides every field the end record saturated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct End {
    // Where the end record starts within the bytes it was found in.
    pub(crate) at: u64,
    pub(crate) this_disk: u32,
    pub(crate) directory_disk: u32,
    pub(crate) entries: u64,
    pub(crate) size: u64,
    pub(crate) offset: u64,
    // A field is saturated, so a zip64 record says the real values.
    pub(crate) wide: bool,
}

// The end record in a volume's tail: the last signature whose comment runs
// exactly to the end, so a signature inside a comment or inside stored data
// is not taken for it.
pub(crate) fn find_end(tail: &[u8]) -> Option<End> {
    end_among(tail, 0..tail.len())
}

// The end record among the signatures that start at `starts` in the tail,
// for a search that reads the tail backward and looks only at what it has
// just read.
pub(crate) fn end_among(tail: &[u8], starts: std::ops::Range<usize>) -> Option<End> {
    let last = tail.len().checked_sub(END_LEN as usize)?;
    starts.filter(|at| *at <= last).rev().find_map(|at| {
        let comment = u16_at(tail, at + 20)? as usize;
        (u32_at(tail, at)? == END && at + END_LEN as usize + comment == tail.len())
            .then(|| end_at(tail, at))
            .flatten()
    })
}

fn end_at(tail: &[u8], at: usize) -> Option<End> {
    let this_disk = u16_at(tail, at + 4)?;
    let directory_disk = u16_at(tail, at + 6)?;
    let entries = u16_at(tail, at + 10)?;
    let size = u32_at(tail, at + 12)?;
    let offset = u32_at(tail, at + 16)?;
    Some(End {
        at: at as u64,
        this_disk: u32::from(this_disk),
        directory_disk: u32::from(directory_disk),
        entries: u64::from(entries),
        size: u64::from(size),
        offset: u64::from(offset),
        wide: this_disk == u16::MAX
            || directory_disk == u16::MAX
            || entries == u16::MAX
            || size == u32::MAX
            || offset == u32::MAX,
    })
}

// The zip64 locator in the 20 bytes before the end record: the disk and
// offset of the zip64 record. `None` when there is none.
pub(crate) fn locator(bytes: &[u8]) -> Option<(u32, u64)> {
    (u32_at(bytes, 0)? == LOCATOR).then(|| Some((u32_at(bytes, 4)?, u64_at(bytes, 8)?)))?
}

// The zip64 record's fields over the end record's.
pub(crate) fn end64(bytes: &[u8], end: End) -> Option<End> {
    (u32_at(bytes, 0)? == END64).then_some(())?;
    Some(End {
        this_disk: u32_at(bytes, 16)?,
        directory_disk: u32_at(bytes, 20)?,
        entries: u64_at(bytes, 32)?,
        size: u64_at(bytes, 40)?,
        offset: u64_at(bytes, 48)?,
        ..end
    })
}

// What an entry is, for the reader that streams it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shape {
    File,
    Directory,
    // A link, a device or a pipe: not a file body.
    Other,
}

// An entry as the central directory, or a local header, describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) name: String,
    pub(crate) flags: u16,
    pub(crate) method: u16,
    pub(crate) crc: u32,
    pub(crate) compressed: u64,
    pub(crate) length: u64,
    pub(crate) aes: bool,
    pub(crate) shape: Shape,
}

impl Header {
    pub(crate) fn encrypted(&self) -> bool {
        self.flags & ENCRYPTED != 0 || self.aes || self.method == AES_METHOD
    }

    pub(crate) fn stored(&self) -> bool {
        self.method == 0
    }

    // The sizes follow the data instead of preceding it.
    pub(crate) fn deferred(&self) -> bool {
        self.flags & DESCRIPTOR != 0
    }
}

// A central directory entry, with where its disk and header offset sit in
// the directory's bytes, so a reader can rewrite them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Central {
    pub(crate) header: Header,
    pub(crate) disk: u32,
    pub(crate) offset: u64,
    // Where, in the directory's bytes, the disk and the offset are, and
    // whether each is the 8- or 4-byte zip64 field rather than the narrow one.
    pub(crate) disk_at: (usize, bool),
    pub(crate) offset_at: (usize, bool),
}

// Every entry of a central directory that holds exactly `entries` of them
// and nothing else.
pub(crate) fn directory(bytes: &[u8], entries: u64) -> Option<Vec<Central>> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while at < bytes.len() {
        let (entry, next) = central(bytes, at)?;
        out.push(entry);
        at = next;
    }
    (out.len() as u64 == entries).then_some(out)
}

fn central(bytes: &[u8], at: usize) -> Option<(Central, usize)> {
    (u32_at(bytes, at)? == CENTRAL).then_some(())?;
    let made_by = *bytes.get(at + 5)?;
    let flags = u16_at(bytes, at + 8)?;
    let method = u16_at(bytes, at + 10)?;
    let crc = u32_at(bytes, at + 16)?;
    let compressed = u32_at(bytes, at + 20)?;
    let length = u32_at(bytes, at + 24)?;
    let name_len = u16_at(bytes, at + 28)? as usize;
    let extra_len = u16_at(bytes, at + 30)? as usize;
    let comment_len = u16_at(bytes, at + 32)? as usize;
    let disk = u16_at(bytes, at + 34)?;
    let external = u32_at(bytes, at + 38)?;
    let offset = u32_at(bytes, at + 42)?;
    let name_at = at + CENTRAL_LEN;
    let extra_at = name_at + name_len;
    let next = extra_at + extra_len + comment_len;
    let name = bytes.get(name_at..extra_at)?;
    let extra = bytes.get(extra_at..extra_at + extra_len)?;
    bytes.get(..next)?;

    let mut wide = Wide::new(extra, extra_at);
    let length = wide.take(u64::from(length), length == u32::MAX)?;
    let compressed = wide.take(u64::from(compressed), compressed == u32::MAX)?;
    let offset_at = wide.at(42 + at, offset == u32::MAX);
    let offset = wide.take(u64::from(offset), offset == u32::MAX)?;
    let disk_at = wide.at(34 + at, disk == u16::MAX);
    let disk = wide.take_disk(u32::from(disk), disk == u16::MAX)?;

    let mode = (made_by == UNIX).then_some(external >> 16);
    let name = decode(name, flags);
    let shape = match mode.map(|mode| mode & KIND) {
        Some(DIRECTORY) => Shape::Directory,
        Some(kind) if kind != 0 && kind != FILE => Shape::Other,
        _ if name.ends_with(['/', '\\']) => Shape::Directory,
        _ => Shape::File,
    };
    Some((
        Central {
            header: Header {
                name,
                flags,
                method,
                crc,
                compressed,
                length,
                aes: field(extra, AES).is_some(),
                shape,
            },
            disk,
            offset,
            disk_at,
            offset_at,
        },
        next,
    ))
}

// The local header's fixed part: the lengths of the name and extra field
// that follow it.
pub(crate) fn local_len(bytes: &[u8]) -> Option<u64> {
    (u32_at(bytes, 0)? == LOCAL).then_some(())?;
    let name_len = u16_at(bytes, 26)?;
    let extra_len = u16_at(bytes, 28)?;
    Some(LOCAL_LEN + u64::from(name_len) + u64::from(extra_len))
}

// A whole local header, name and extra field included.
pub(crate) fn local(bytes: &[u8]) -> Option<Header> {
    let len = local_len(bytes)? as usize;
    bytes.get(..len)?;
    let flags = u16_at(bytes, 6)?;
    let name_len = u16_at(bytes, 26)? as usize;
    let extra_at = LOCAL_LEN as usize + name_len;
    let name = &bytes[LOCAL_LEN as usize..extra_at];
    let extra = &bytes[extra_at..len];
    let compressed = u32_at(bytes, 18)?;
    let length = u32_at(bytes, 22)?;
    // A local zip64 field carries both sizes when either is saturated.
    let wide = compressed == u32::MAX || length == u32::MAX;
    let mut sizes = Wide::new(extra, extra_at);
    let length = sizes.take(u64::from(length), wide)?;
    let compressed = sizes.take(u64::from(compressed), wide)?;
    let name = decode(name, flags);
    let shape = if name.ends_with(['/', '\\']) {
        Shape::Directory
    } else {
        Shape::File
    };
    Some(Header {
        name,
        flags,
        method: u16_at(bytes, 8)?,
        crc: u32_at(bytes, 14)?,
        compressed,
        length,
        aes: field(extra, AES).is_some(),
        shape,
    })
}

// What a header starting at a place in a set is: a local header, the start
// of the directory or the records after it, or something else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Next {
    Local,
    Directory,
    Unknown,
}

pub(crate) fn next(bytes: &[u8]) -> Option<Next> {
    Some(match u32_at(bytes, 0)? {
        LOCAL => Next::Local,
        CENTRAL | END | END64 => Next::Directory,
        _ => Next::Unknown,
    })
}

// A name as the header gives it: UTF-8 when the header says so or the bytes
// are, otherwise read byte for byte.
fn decode(name: &[u8], flags: u16) -> String {
    match std::str::from_utf8(name) {
        Ok(text) => text.to_owned(),
        Err(_) if flags & UTF8 != 0 => String::from_utf8_lossy(name).into_owned(),
        Err(_) => name.iter().map(|byte| char::from(*byte)).collect(),
    }
}

// The body of an extra field by its id, and where it starts.
fn field(extra: &[u8], id: u16) -> Option<(usize, &[u8])> {
    let mut at = 0;
    while at + 4 <= extra.len() {
        let kind = u16_at(extra, at)?;
        let len = u16_at(extra, at + 2)? as usize;
        let body = extra.get(at + 4..at + 4 + len)?;
        if kind == id {
            return Some((at + 4, body));
        }
        at += 4 + len;
    }
    None
}

// The zip64 extra field, read in its fixed order: each saturated field of
// the header, and only those, in turn.
struct Wide<'a> {
    body: Option<(usize, &'a [u8])>,
    // Where the extra field starts in the record's bytes.
    base: usize,
    cursor: usize,
}

impl<'a> Wide<'a> {
    fn new(extra: &'a [u8], base: usize) -> Self {
        Self {
            body: field(extra, ZIP64),
            base,
            cursor: 0,
        }
    }

    // Where the next field would be read: in the zip64 field when the
    // header's is saturated, at `narrow` otherwise.
    fn at(&self, narrow: usize, saturated: bool) -> (usize, bool) {
        match (saturated, self.body) {
            (true, Some((start, _))) => (self.base + start + self.cursor, true),
            _ => (narrow, false),
        }
    }

    fn take(&mut self, narrow: u64, saturated: bool) -> Option<u64> {
        if !saturated {
            return Some(narrow);
        }
        let (_, body) = self.body?;
        let value = u64_at(body, self.cursor)?;
        self.cursor += 8;
        Some(value)
    }

    fn take_disk(&mut self, narrow: u32, saturated: bool) -> Option<u32> {
        if !saturated {
            return Some(narrow);
        }
        let (_, body) = self.body?;
        let value = u32_at(body, self.cursor)?;
        self.cursor += 4;
        Some(value)
    }
}
