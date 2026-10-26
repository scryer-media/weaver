// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The set as the zip library reads it: one seekable file, the volumes end
// to end. The run reads the archive's records itself, each exactly where the
// end record puts it, and holds them: its directory, rewritten for the joined
// bytes, and a fresh end record after it, with zip64 records where a field no
// longer fits. A spanned set's directory gives every entry a disk and an
// offset within that disk, and the library reads only single-disk archives,
// so every entry is put on disk zero at its place in the whole. The library
// learns the archive from those records alone and reads only the entries'
// own bytes from the set, each when the walk reaches it: a byte lost in one
// entry fails that entry, not the opening of the archive.

use std::io::{self, Read, Seek, SeekFrom};

use ::zip::ZipArchive;
use ::zip::read::{ArchiveOffset, Config};
use ::zip::result::ZipResult;
use weaver_graph::Why;

use crate::engine::Joined;
use crate::zip::records::{self, END, END_LEN, END64, END64_LEN, End, LOCATOR, LOCATOR_LEN, TAIL};

// Why the set is not an image: the owner failed a read, which it already
// knows, or the shape is one no image is made of.
pub(super) enum Unopened {
    Failed,
    Refused(Why),
}

impl From<io::Error> for Unopened {
    fn from(_: io::Error) -> Self {
        Unopened::Failed
    }
}

pub(super) struct Image {
    joined: Joined,
    // The joined bytes read as they lie, up to the rewritten directory.
    prefix: u64,
    rest: Vec<u8>,
    position: u64,
}

impl Image {
    // Once every volume has its length: the directory is at the tail, and
    // every place in a spanned set is the length of the volumes before it.
    pub(super) fn open(joined: Joined) -> Result<Image, Unopened> {
        let lengths = joined.closed_lens()?;
        let total: u64 = lengths.iter().sum();
        let whole = |joined| Image {
            joined,
            prefix: total,
            rest: Vec::new(),
            position: 0,
        };
        // With no end record the library says what is wrong.
        let Some((start, found)) = find_end(&joined, total)? else {
            return Ok(whole(joined));
        };
        let base = |disk: u32| -> Option<u64> {
            lengths
                .get(..disk as usize)
                .filter(|_| (disk as usize) < lengths.len())
                .map(|before| before.iter().sum())
        };
        // Where the records after the directory start.
        let mut after = start + found.at;
        let end = match found.wide {
            true => {
                let at = after
                    .checked_sub(LOCATOR_LEN)
                    .ok_or(Unopened::Refused(Why::Unrepairable))?;
                let mut locator = [0u8; LOCATOR_LEN as usize];
                joined.read_exact(at, &mut locator)?;
                let (disk, offset) =
                    records::locator(&locator).ok_or(Unopened::Refused(Why::Unrepairable))?;
                let at = base(disk)
                    .and_then(|base| base.checked_add(offset))
                    .ok_or(Unopened::Refused(Why::Unsupported))?;
                let mut record = [0u8; END64_LEN as usize];
                joined.read_exact(at, &mut record)?;
                after = at;
                records::end64(&record, found).ok_or(Unopened::Refused(Why::Unrepairable))?
            }
            false => found,
        };
        // The end record is on the last disk; a set that disagrees is not
        // the set bound.
        if end.this_disk as usize + 1 != lengths.len() {
            return Err(Unopened::Refused(Why::Unsupported));
        }
        // A directory runs no further than the records after it.
        let directory = base(end.directory_disk)
            .and_then(|base| base.checked_add(end.offset))
            .filter(|directory| {
                directory
                    .checked_add(end.size)
                    .is_some_and(|end| end <= after)
            });
        let rest = match directory {
            Some(directory) => {
                let mut bytes = vec![0u8; end.size as usize];
                joined.read_exact(directory, &mut bytes)?;
                rewrite(&mut bytes, &end, directory, &base).map(|rest| (directory, rest))
            }
            None => None,
        };
        match rest {
            Some((prefix, rest)) => Ok(Image {
                joined,
                prefix,
                rest,
                position: 0,
            }),
            // A volume whose directory is not where its end record puts it
            // has bytes in front of the archive: the library finds it there.
            None if lengths.len() == 1 => Ok(whole(joined)),
            None => Err(Unopened::Refused(Why::Unsupported)),
        }
    }

    // The archive, as the library reads it. Where the run holds the records,
    // the library learns the archive from them alone, at their places in
    // the whole and with nothing in front of them, so it reads no entry's
    // bytes to open it; it then reads each entry from the set. Otherwise it
    // reads the set, finds the archive in it, or says what is wrong.
    pub(super) fn archive(self) -> ZipResult<ZipArchive<Image>> {
        if self.rest.is_empty() {
            return ZipArchive::new(self);
        }
        let records = Records {
            prefix: self.prefix,
            rest: &self.rest,
            position: 0,
        };
        let at_zero = Config {
            archive_offset: ArchiveOffset::Known(0),
        };
        let metadata = ZipArchive::with_config(at_zero, records)?.metadata();
        // SAFETY: the metadata is the image's. The records it was read from
        // are the image's own bytes at the same places, its whole length,
        // and only the bytes in front of the directory, which the library
        // did not read the archive from, stand as zeros there.
        Ok(unsafe { ZipArchive::unsafe_new_with_metadata(self, metadata) })
    }

    fn len(&self) -> u64 {
        self.prefix + self.rest.len() as u64
    }
}

// The end record, searched for from the set's end backward as far as the
// longest comment it can carry, a record's length read at a time: the search
// reads no further back than a record's length past the record it finds,
// into the directory or the zip64 locator ahead of it, which the open reads
// next. Its place in the set and the record, or `None` when there is none.
fn find_end(joined: &Joined, total: u64) -> Result<Option<(u64, End)>, Unopened> {
    let floor = total.saturating_sub(TAIL);
    let mut tail = vec![0u8; (total - floor) as usize];
    let mut start = total;
    while start > floor {
        let from = start.saturating_sub(END_LEN).max(floor);
        let read = (from - floor) as usize..(start - floor) as usize;
        joined.read_exact(from, &mut tail[read.clone()])?;
        start = from;
        let held = &tail[read.start..];
        if let Some(found) = records::end_among(held, 0..read.len()) {
            return Ok(Some((start, found)));
        }
    }
    Ok(None)
}

// The image as the library opens it: the records the run holds, at their
// places, and zeros in front of them.
struct Records<'a> {
    prefix: u64,
    rest: &'a [u8],
    position: u64,
}

impl Read for Records<'_> {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        let len = self.prefix + self.rest.len() as u64;
        let wanted = usize::try_from(len.saturating_sub(self.position))
            .map_or(into.len(), |rest| rest.min(into.len()));
        let into = &mut into[..wanted];
        for (at, byte) in (self.position..).zip(into.iter_mut()) {
            *byte = at
                .checked_sub(self.prefix)
                .and_then(|at| usize::try_from(at).ok())
                .map_or(0, |at| self.rest[at]);
        }
        self.position += wanted as u64;
        Ok(wanted)
    }
}

impl Seek for Records<'_> {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let len = self.prefix + self.rest.len() as u64;
        self.position = seek(self.position, len, to)?;
        Ok(self.position)
    }
}

// Where a seek from `position` in `len` bytes lands.
fn seek(position: u64, len: u64, to: SeekFrom) -> io::Result<u64> {
    match to {
        SeekFrom::Start(at) => Some(at),
        SeekFrom::End(by) => len.checked_add_signed(by),
        SeekFrom::Current(by) => position.checked_add_signed(by),
    }
    .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))
}

// The directory on disk zero at its place in the whole, followed by the
// records that end it. `None` when an entry's place does not fit its field.
fn rewrite(
    directory: &mut [u8],
    end: &End,
    start: u64,
    base: &dyn Fn(u32) -> Option<u64>,
) -> Option<Vec<u8>> {
    let entries = records::directory(directory, end.entries)?;
    for entry in &entries {
        let offset = base(entry.disk)?.checked_add(entry.offset)?;
        put(directory, entry.offset_at, offset, 4)?;
        put(directory, entry.disk_at, 0, 2)?;
    }
    let size = directory.len() as u64;
    let wide = end.entries >= u64::from(u16::MAX)
        || size >= u64::from(u32::MAX)
        || start >= u64::from(u32::MAX);
    let mut rest = directory.to_vec();
    if wide {
        let at = start + size;
        rest.extend(END64.to_le_bytes());
        rest.extend((END64_LEN - 12).to_le_bytes());
        rest.extend([45, 0, 45, 0]);
        rest.extend([0u8; 8]);
        rest.extend(end.entries.to_le_bytes());
        rest.extend(end.entries.to_le_bytes());
        rest.extend(size.to_le_bytes());
        rest.extend(start.to_le_bytes());
        rest.extend(LOCATOR.to_le_bytes());
        rest.extend(0u32.to_le_bytes());
        rest.extend(at.to_le_bytes());
        rest.extend(1u32.to_le_bytes());
    }
    let narrow16 = |value: u64| {
        u16::try_from(value)
            .ok()
            .filter(|_| !wide)
            .unwrap_or(u16::MAX)
    };
    let narrow32 = |value: u64| {
        u32::try_from(value)
            .ok()
            .filter(|_| !wide)
            .unwrap_or(u32::MAX)
    };
    rest.extend(END.to_le_bytes());
    rest.extend([0u8; 4]);
    rest.extend(narrow16(end.entries).to_le_bytes());
    rest.extend(narrow16(end.entries).to_le_bytes());
    rest.extend(narrow32(size).to_le_bytes());
    rest.extend(narrow32(start).to_le_bytes());
    rest.extend(0u16.to_le_bytes());
    Some(rest)
}

// Write a field where the directory has it: the narrow one, whose all-ones
// mark means the zip64 field holds the value, or that zip64 field, twice
// as wide.
fn put(directory: &mut [u8], (at, wide): (usize, bool), value: u64, narrow: usize) -> Option<()> {
    let width = if wide { narrow * 2 } else { narrow };
    let fits = match (wide, width) {
        (_, 8) => true,
        (true, _) => value >> (width * 8) == 0,
        (false, _) => value < (1 << (width * 8)) - 1,
    };
    fits.then_some(())?;
    directory
        .get_mut(at..at + width)?
        .copy_from_slice(&value.to_le_bytes()[..width]);
    Some(())
}

impl Read for Image {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        let read = if self.position < self.prefix {
            let wanted = usize::try_from(self.prefix - self.position)
                .map_or(into.len(), |rest| rest.min(into.len()));
            self.joined.read(self.position, &mut into[..wanted])?
        } else {
            let at = usize::try_from(self.position - self.prefix)
                .map_or(self.rest.len(), |at| at.min(self.rest.len()));
            let read = (self.rest.len() - at).min(into.len());
            into[..read].copy_from_slice(&self.rest[at..at + read]);
            read
        };
        self.position += read as u64;
        Ok(read)
    }
}

impl Seek for Image {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        self.position = seek(self.position, self.len(), to)?;
        Ok(self.position)
    }
}
