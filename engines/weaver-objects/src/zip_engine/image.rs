// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The set as the zip library reads it: one seekable file. A zip in one
// volume is that volume. A spanned set is its volumes end to end, but its
// directory gives every entry a disk and an offset within that disk, and
// the library reads only single-disk archives, so the directory is
// rewritten for the joined bytes: every entry on disk zero at its place in
// the whole, then a fresh end record, with zip64 records where a field no
// longer fits. The entries' own bytes are read where they lie.

use std::io::{self, Read, Seek, SeekFrom};

use weaver_graph::{VolumeIndex, Why};

use crate::engine::Joined;
use crate::zip::records::{self, END, END64, END64_LEN, End, LOCATOR, LOCATOR_LEN, TAIL};

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
        let lengths = lengths(&joined)?;
        let total: u64 = lengths.iter().sum();
        let whole = |joined| Image {
            joined,
            prefix: total,
            rest: Vec::new(),
            position: 0,
        };
        let start = total.saturating_sub(TAIL);
        let mut tail = vec![0u8; (total - start) as usize];
        joined.read_exact(start, &mut tail)?;
        // With no end record the library says what is wrong.
        let Some(found) = records::find_end(&tail) else {
            return Ok(whole(joined));
        };
        let base = |disk: u32| -> Option<u64> {
            lengths
                .get(..disk as usize)
                .filter(|_| (disk as usize) < lengths.len())
                .map(|before| before.iter().sum())
        };
        let end = match found.wide {
            true => {
                let at = (start + found.at)
                    .checked_sub(LOCATOR_LEN)
                    .ok_or(Unopened::Refused(Why::Unrepairable))?;
                let mut locator = [0u8; LOCATOR_LEN as usize];
                joined.read_exact(at, &mut locator)?;
                let (disk, offset) =
                    records::locator(&locator).ok_or(Unopened::Refused(Why::Unrepairable))?;
                let at = base(disk).ok_or(Unopened::Refused(Why::Unsupported))? + offset;
                let mut record = [0u8; END64_LEN as usize];
                joined.read_exact(at, &mut record)?;
                records::end64(&record, found).ok_or(Unopened::Refused(Why::Unrepairable))?
            }
            false => found,
        };
        // The end record is on the last disk; a set that disagrees is not
        // the set bound.
        if end.this_disk as usize + 1 != lengths.len() {
            return Err(Unopened::Refused(Why::Unsupported));
        }
        if end.this_disk == 0 {
            return Ok(whole(joined));
        }
        let directory =
            base(end.directory_disk).ok_or(Unopened::Refused(Why::Unsupported))? + end.offset;
        let mut bytes = vec![0u8; end.size as usize];
        joined.read_exact(directory, &mut bytes)?;
        let rest = rewrite(&mut bytes, &end, directory, &base)
            .ok_or(Unopened::Refused(Why::Unsupported))?;
        Ok(Image {
            joined,
            prefix: directory,
            rest,
            position: 0,
        })
    }

    fn len(&self) -> u64 {
        self.prefix + self.rest.len() as u64
    }
}

// Every volume's length, waiting for each to close, and for the set to stop
// growing while the run waited.
fn lengths(joined: &Joined) -> io::Result<Vec<u64>> {
    loop {
        let count = joined.volumes().count();
        let lengths = (0..count)
            .map(|volume| joined.closed_len(VolumeIndex(volume as u32)))
            .collect::<io::Result<Vec<u64>>>()?;
        if joined.volumes().count() == count {
            return Ok(lengths);
        }
    }
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
        let offset = base(entry.disk)? + entry.offset;
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
        let position = match to {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::End(by) => self.len().checked_add_signed(by),
            SeekFrom::Current(by) => self.position.checked_add_signed(by),
        };
        self.position = position.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok(self.position)
    }
}
