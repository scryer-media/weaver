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
use std::sync::Arc;

use ::zip::ZipArchive;
use ::zip::read::{ArchiveOffset, Config, ZipFileEntry};
use ::zip::result::{ZipError, ZipResult};
use weaver_graph::{VolumeIndex, Why};

use super::{MAX_DIRECTORY_BYTES, MAX_DIRECTORY_ENTRIES};
use crate::engine::Joined;
use crate::zip::records::{
    self, END, END_LEN, END64, END64_LEN, End, Grow, LOCATOR, LOCATOR_LEN, TAIL,
};

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

// The set's disks as its end record lays them out, end to end: each one's
// volume and the length the run reads it at, the length its member declared
// until it closes. Disk `n` of a set before its last is the volume numbered
// `n`; the last is the volume that ends the set, at its own number or after
// every numbered one. A read waits for its own bytes only, so the run reads
// the set while it grows.
#[derive(Clone)]
pub(super) struct Disks {
    joined: Joined,
    disks: Arc<[(VolumeIndex, u64)]>,
}

impl Disks {
    fn new(joined: &Joined, disks: Vec<(VolumeIndex, u64)>) -> Self {
        Self {
            joined: joined.clone(),
            disks: disks.into(),
        }
    }

    // Where a disk starts: the length of the disks before it.
    fn base(&self, disk: u32) -> Option<u64> {
        self.disks
            .get(..disk as usize)
            .filter(|_| (disk as usize) < self.disks.len())
            .map(|before| before.iter().map(|(_, length)| length).sum())
    }

    // Bytes at `offset` of the joined disks; short only at their end. A disk
    // whose bytes end short of the length read for it was declared longer
    // than it is: the read fails as the owner's, not as damage, and the run
    // is started over on the length it closes at.
    fn read(&self, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        if into.is_empty() {
            return Ok(0);
        }
        let mut start = 0u64;
        for &(volume, length) in self.disks.iter() {
            let local = offset - start;
            if local < length {
                let wanted =
                    usize::try_from(length - local).map_or(into.len(), |rest| rest.min(into.len()));
                return match self
                    .joined
                    .read_volume(volume, local, &mut into[..wanted])?
                {
                    0 => {
                        self.joined.fail();
                        Err(io::ErrorKind::UnexpectedEof.into())
                    }
                    read => Ok(read),
                };
            }
            start += length;
        }
        Ok(0)
    }

    // Every byte of `into` from `offset`, or an error at the set's end.
    fn read_exact(&self, offset: u64, into: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        while filled < into.len() {
            let read = self.read(offset + filled as u64, &mut into[filled..])?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            filled += read;
        }
        Ok(())
    }

    // Whether every disk closed at the length the run read it at, waiting
    // for each to close: a verdict rests on those lengths, so it is said only
    // once they are final. A disk that closed at another fails the run as
    // the owner's, which starts it over on the length it closed at.
    pub(super) fn settled(&self) -> bool {
        for &(volume, length) in self.disks.iter() {
            match self.joined.closed_len(volume) {
                Ok(closed) if closed == length => {}
                Ok(_) => {
                    self.joined.fail();
                    return false;
                }
                Err(_) => return false,
            }
        }
        true
    }
}

pub(super) struct Image {
    disks: Disks,
    // The joined bytes read as they lie, up to the rewritten directory.
    prefix: u64,
    rest: Vec<u8>,
    position: u64,
}

impl Image {
    // From the set's tail, as soon as it lands: the end record is in the
    // last volume's own tail, where its declared length puts it, and says
    // how many disks come before it; every place in a spanned set is the
    // length of the disks before it. No volume needs to close first.
    pub(super) fn open(joined: Joined, budget: &weaver_graph::Budget) -> Result<Image, Unopened> {
        let (last, length, start, found) = loop {
            let bound = joined.bound()?;
            let Some(&last) = bound.last() else {
                return Err(Unopened::Refused(Why::Unrepairable));
            };
            let length = joined.sized_len(last)?;
            let tail = Disks::new(&joined, vec![(last, length)]);
            if let Some((start, found)) = find_end(&tail, length)? {
                break (last, length, start, found);
            }
            // No end record where the declared length puts the tail. The
            // length may be wrong, which only its close tells; or a volume
            // after this one may still join and end the set.
            if !tail.settled() {
                return Err(Unopened::Failed);
            }
            if joined.next(Some(last))?.is_none() {
                return Err(Unopened::Refused(Why::Unrepairable));
            }
        };
        let tail = Disks::new(&joined, vec![(last, length)]);
        let refused = |why: Why, disks: &Disks| match disks.settled() {
            true => Unopened::Refused(why),
            false => Unopened::Failed,
        };
        // The disk the end record is on, which is how many come before it.
        // A saturated one is in the zip64 locator, as the count of disks,
        // just ahead of the end record.
        let disk = match found.this_disk {
            disk if disk != u32::from(u16::MAX) => Some(disk),
            _ => {
                let at = (start + found.at).checked_sub(LOCATOR_LEN);
                let mut locator = [0u8; LOCATOR_LEN as usize];
                match at {
                    Some(at) => {
                        tail.read_exact(at, &mut locator)?;
                        records::locator(&locator)
                            .and_then(|_| records::u32_at(&locator, 16)?.checked_sub(1))
                    }
                    None => None,
                }
            }
        };
        let Some(disk) = disk else {
            return Err(refused(Why::Unrepairable, &tail));
        };
        // The set bound is the one the end record ends: the volume that ends
        // it is past every disk before it, and every other volume bound is
        // one of them.
        let bound = joined.bound()?;
        if last.0 < disk
            || bound
                .iter()
                .any(|volume| *volume != last && volume.0 >= disk)
        {
            return Err(refused(Why::Unsupported, &tail));
        }
        let mut lengths = Vec::new();
        for number in 0..disk {
            let volume = VolumeIndex(number);
            lengths.push((volume, joined.sized_len(volume)?));
        }
        lengths.push((last, length));
        let disks = Disks::new(&joined, lengths);
        match Self::shaped(disks.clone(), disk, start, found, budget) {
            Err(Unopened::Refused(why)) => Err(refused(why, &disks)),
            shaped => shaped,
        }
    }

    // The image of the disks, the end record found at `start` of the last.
    fn shaped(
        disks: Disks,
        last: u32,
        start: u64,
        found: End,
        budget: &weaver_graph::Budget,
    ) -> Result<Image, Unopened> {
        let base = |disk: u32| disks.base(disk);
        // Where the records after the directory start.
        let mut after = base(last)
            .and_then(|base| base.checked_add(start + found.at))
            .ok_or(Unopened::Refused(Why::Unrepairable))?;
        let end = match found.wide {
            true => {
                let at = after
                    .checked_sub(LOCATOR_LEN)
                    .ok_or(Unopened::Refused(Why::Unrepairable))?;
                let mut locator = [0u8; LOCATOR_LEN as usize];
                disks.read_exact(at, &mut locator)?;
                let (disk, offset) =
                    records::locator(&locator).ok_or(Unopened::Refused(Why::Unrepairable))?;
                let at = base(disk)
                    .and_then(|base| base.checked_add(offset))
                    .ok_or(Unopened::Refused(Why::Unsupported))?;
                let mut record = [0u8; END64_LEN as usize];
                disks.read_exact(at, &mut record)?;
                after = at;
                records::end64(&record, found).ok_or(Unopened::Refused(Why::Unrepairable))?
            }
            false => found,
        };
        // The end record is on the last disk; a set that disagrees is not
        // the set bound.
        if end.this_disk != last {
            return Err(Unopened::Refused(Why::Unsupported));
        }
        let directory_limit = budget
            .memory_or(MAX_DIRECTORY_BYTES * 8)
            .checked_div(8)
            .unwrap_or(0)
            .min(MAX_DIRECTORY_BYTES);
        if end.size > directory_limit || end.entries > MAX_DIRECTORY_ENTRIES {
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
        let mut rest = match directory {
            Some(directory) => {
                let mut bytes = vec![0u8; end.size as usize];
                disks.read_exact(directory, &mut bytes)?;
                rewrite(&bytes, &end, directory, &base).map(|rest| (directory, rest))
            }
            None => None,
        };
        // A single-volume archive may have a prepended executable. Its
        // directory still ends before the end records; shift the offsets
        // only after validating that bounded directory at its actual place.
        if rest.is_none()
            && last == 0
            && end.directory_disk == 0
            && let Some((directory, shift)) = after.checked_sub(end.size).and_then(|directory| {
                directory
                    .checked_sub(end.offset)
                    .filter(|shift| *shift > 0)
                    .map(|shift| (directory, shift))
            })
        {
            let mut bytes = vec![0u8; end.size as usize];
            disks.read_exact(directory, &mut bytes)?;
            rest = rewrite(&bytes, &end, directory, &|disk| {
                (disk == 0).then_some(shift)
            })
            .map(|rest| (directory, rest));
        }
        match rest {
            Some((prefix, rest)) => Ok(Image {
                disks,
                prefix,
                rest,
                position: 0,
            }),
            None => Err(Unopened::Refused(Why::Unsupported)),
        }
    }

    // The disks the image reads, for the run to settle before a verdict.
    pub(super) fn disks(&self) -> Disks {
        self.disks.clone()
    }

    // The archive, as the library reads it. Where the run holds the records,
    // the library learns the archive from them alone, at their places in
    // the whole and with nothing in front of them, so it reads no entry's
    // bytes to open it; it then reads each entry from the set.
    pub(super) fn archive(self) -> ZipResult<ZipArchive<Image>> {
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

    // Identity, encoding and checksum still agree; only the two local size
    // claims may be stale. The directory supplies the extraction bounds.
    pub(super) fn check_header(&self, entry: &ZipFileEntry<'_>) -> ZipResult<()> {
        let invalid = || ZipError::InvalidArchive("conflicting ZIP entry headers".into());
        let at = entry
            .central_header_start()
            .checked_sub(self.prefix)
            .and_then(|at| usize::try_from(at).ok())
            .ok_or_else(invalid)?;
        let (central, _) = records::central(&self.rest, at).ok_or_else(invalid)?;
        let mut bytes = vec![0; records::LOCAL_LEN as usize];
        self.disks.read_exact(entry.header_start(), &mut bytes)?;
        let len = records::local_len(&bytes).ok_or_else(invalid)?;
        let end = entry.header_start().checked_add(len).ok_or_else(invalid)?;
        if end > self.prefix {
            return Err(invalid());
        }
        bytes.resize(len as usize, 0);
        self.disks.read_exact(
            entry.header_start() + records::LOCAL_LEN,
            &mut bytes[records::LOCAL_LEN as usize..],
        )?;
        let mut local = records::local(&bytes).ok_or_else(invalid)?;
        local.compressed = central.header.compressed;
        local.length = central.header.length;
        if !records::agree(&local, &central.header) {
            return Err(invalid());
        }
        Ok(())
    }

    fn len(&self) -> u64 {
        self.prefix + self.rest.len() as u64
    }
}

// The end record, searched for from the last disk's end backward as far as
// the longest comment it can carry, a record's length read at a time: the
// search reads no further back than a record's length past the record it
// finds, into the directory or the zip64 locator ahead of it, which the open
// reads next. Its place in the disk and the record, or `None` when there is
// none.
fn find_end(tail: &Disks, total: u64) -> Result<Option<(u64, End)>, Unopened> {
    let floor = total.saturating_sub(TAIL);
    let mut held = vec![0u8; (total - floor) as usize];
    let mut start = total;
    while start > floor {
        let from = start.saturating_sub(END_LEN).max(floor);
        let read = (from - floor) as usize..(start - floor) as usize;
        tail.read_exact(from, &mut held[read.clone()])?;
        start = from;
        let held = &held[read.start..];
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
// records that end it. An entry whose place in the whole no longer fits its
// narrow field gets it in its zip64 field. `None` when an entry cannot.
pub(super) fn rewrite(
    directory: &[u8],
    end: &End,
    start: u64,
    base: &dyn Fn(u32) -> Option<u64>,
) -> Option<Vec<u8>> {
    let entries = records::directory(directory, end.entries)?;
    let mut rest = Vec::with_capacity(directory.len());
    for entry in &entries {
        let offset = base(entry.disk)?.checked_add(entry.offset)?;
        let mut bytes = directory.get(entry.span.clone())?.to_vec();
        let within = |(at, wide): (usize, bool)| (at - entry.span.start, wide);
        put(&mut bytes, within(entry.disk_at), 0, 2)?;
        let (at, wide) = within(entry.offset_at);
        if wide || offset < u64::from(u32::MAX) {
            put(&mut bytes, (at, wide), offset, 4)?;
        } else {
            widen(&mut bytes, entry.grow, entry.span.start, offset)?;
        }
        rest.extend(bytes);
    }
    let size = rest.len() as u64;
    let wide = end.entries >= u64::from(u16::MAX)
        || size >= u64::from(u32::MAX)
        || start >= u64::from(u32::MAX);
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

// An entry's offset moved into its zip64 field, at the place the field's
// order gives it, with the narrow field's all-ones mark saying so. An entry
// with no zip64 field gains one at the end of its extra field. `entry` is
// where the entry's bytes start in the directory `grow` was read from.
fn widen(bytes: &mut Vec<u8>, grow: Grow, entry: usize, offset: u64) -> Option<()> {
    const EXTRA_LEN: usize = 30;
    const OFFSET: usize = 42;
    let mut added = Vec::with_capacity(12);
    match grow.field {
        Some(field) => {
            let len = field - entry + 2;
            let grown = records::u16_at(bytes, len)?.checked_add(8)?;
            bytes[len..len + 2].copy_from_slice(&grown.to_le_bytes());
        }
        None => {
            added.extend(records::ZIP64.to_le_bytes());
            added.extend(8u16.to_le_bytes());
        }
    }
    added.extend(offset.to_le_bytes());
    let extra = records::u16_at(bytes, EXTRA_LEN)?.checked_add(added.len() as u16)?;
    bytes[EXTRA_LEN..EXTRA_LEN + 2].copy_from_slice(&extra.to_le_bytes());
    bytes[OFFSET..OFFSET + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let at = grow.at - entry;
    bytes.splice(at..at, added);
    Some(())
}

impl Read for Image {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        let read = if self.position < self.prefix {
            let wanted = usize::try_from(self.prefix - self.position)
                .map_or(into.len(), |rest| rest.min(into.len()));
            self.disks.read(self.position, &mut into[..wanted])?
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
