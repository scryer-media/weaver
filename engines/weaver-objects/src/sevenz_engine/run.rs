// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One run of the 7z engine. sevenz-turbo reads the set as one positional
// source, the volumes joined end to end, and walks its entries in archive
// order, each decoded straight into the sink with its CRC checked as it
// goes. A 7z names its members only in the end header at its tail, so the
// run's first reads wait for the tail to land; reads wait for bytes still
// on their way, so the run never reads ahead of the download.

use std::borrow::Cow;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use sevenz_turbo::{ArchiveEntry, ArchiveReader, EntryRead, Error, Password, ReadAt};
use weaver_graph::{Budget, Emit, Extraction, MemberId, Provenance, Sink, Volumes};

use crate::engine::Joined;
use crate::verdict;

pub(super) struct Run {
    password: Option<String>,
}

impl Run {
    pub(super) fn new(password: Option<String>) -> Self {
        Self { password }
    }
}

impl Extraction for Run {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>, budget: Budget) {
        let volumes = match super::geometry::resolve(volumes) {
            Ok(volumes) => volumes,
            // As with Joined's decoder reads, the owner already knows why
            // its source failed or stopped. That is no format verdict.
            Err(super::geometry::Failure::Source(error)) => {
                drop(error);
                return;
            }
            Err(super::geometry::Failure::Layout) => {
                sink(Emit::Refuse(weaver_graph::Why::InvalidArchive));
                return;
            }
        };
        let input = Joined::new(volumes);
        let failed = input.failure();
        let password = self
            .password
            .as_deref()
            .map_or_else(Password::empty, Password::new);
        let mut walk = Walk {
            sink,
            next: 0,
            open: None,
            stopped: false,
        };
        // The decoder holds no more than the run was granted, and decodes on
        // as many threads.
        let limits = crate::sevenz_store::limits(&budget);
        let threads = u32::try_from(budget.threads()).unwrap_or(u32::MAX);
        let walked = ArchiveReader::from_read_at(input, password, limits).and_then(|mut reader| {
            reader.set_verify_checksums(true);
            reader.set_threads(threads);
            reader.for_each_entries_direct(|entry, read| walk.entry(entry, read))
        });
        let Err(error) = walked else {
            return;
        };
        // The owner stopped the run or failed a read, so it already knows why.
        if walk.stopped || failed.load(Ordering::Acquire) {
            return;
        }
        // A member left open was built on bytes the run cannot vouch for.
        if let Some(open) = walk.open.take()
            && !(walk.sink)(Emit::Retract(open))
        {
            return;
        }
        (walk.sink)(verdict::sevenz(&error));
    }
}

struct Walk<'s, 'a> {
    sink: &'s mut Sink<'a>,
    // The id the next member announced gets: members are numbered in the
    // order the walk meets them, so every run of the same set numbers them
    // alike.
    next: u32,
    open: Option<MemberId>,
    stopped: bool,
}

impl Walk<'_, '_> {
    fn entry(&mut self, entry: &ArchiveEntry, read: &mut dyn EntryRead) -> Result<bool, Error> {
        // A directory is a member with nothing in it, named with a trailing
        // slash as the store names it. Links and devices are not file
        // bodies, so none is a member and the files around them stand.
        // sevenz-turbo drains what is left unread.
        let mut name = entry.name().to_owned();
        let length = if is_directory(entry) {
            if !name.ends_with(['/', '\\']) {
                name.push('/');
            }
            0
        } else if is_file_body(entry) {
            entry.size()
        } else {
            return Ok(true);
        };
        let member = MemberId(self.next);
        self.next += 1;
        self.say(Emit::Member {
            member,
            name,
            length: Some(length),
        })?;
        if length > 0 && entry.has_crc {
            self.say(Emit::Checksum {
                member,
                crc32: entry.crc as u32,
            })?;
        }
        self.open = Some(member);
        let mut out = Out {
            sink: &mut *self.sink,
            member,
            offset: 0,
            stopped: false,
        };
        let written = read.write_rest(&mut out);
        if out.stopped {
            self.stopped = true;
            return Err(stopped());
        }
        written?;
        self.open = None;
        self.say(Emit::MemberClosed(member))?;
        Ok(true)
    }

    fn say(&mut self, emit: Emit) -> Result<(), Error> {
        if (self.sink)(emit) {
            Ok(())
        } else {
            self.stopped = true;
            Err(stopped())
        }
    }
}

fn stopped() -> Error {
    Error::Other(Cow::Borrowed("the owner stopped the run"))
}

fn is_directory(entry: &ArchiveEntry) -> bool {
    entry.is_directory() && !entry.is_anti_item() && !entry.is_symlink()
}

fn is_file_body(entry: &ArchiveEntry) -> bool {
    const KIND: u32 = 0o170_000;
    const FILE: u32 = 0o100_000;
    !entry.is_directory()
        && !entry.is_anti_item()
        && !entry.is_symlink()
        && entry
            .unix_mode()
            .is_none_or(|mode| matches!(mode & KIND, 0 | FILE))
}

// The joined volumes, as sevenz-turbo reads them.
impl ReadAt for Joined {
    fn read_at(&self, into: &mut [u8], offset: u64) -> io::Result<usize> {
        self.read(offset, into)
    }

    // The signature header says where the end header lies and how long it
    // is, and a 7z ends with it: the set's length while it is still
    // growing, and the reason the run waits for the tail before anything
    // else. Bytes after the end header, a trailing recovery set, are not the
    // archive's.
    fn size(&self) -> io::Result<u64> {
        let mut header = [0u8; 32];
        self.read_exact(0, &mut header)?;
        let field = |at: usize| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&header[at..at + 8]);
            u64::from_le_bytes(bytes)
        };
        32u64
            .checked_add(field(12))
            .and_then(|end| end.checked_add(field(20)))
            .ok_or_else(|| io::ErrorKind::InvalidData.into())
    }
}

// The decoder's output, handed to the sink as it is written.
struct Out<'s, 'a> {
    sink: &'s mut Sink<'a>,
    member: MemberId,
    offset: u64,
    stopped: bool,
}

impl Write for Out<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let heard = (self.sink)(Emit::Bytes {
            member: self.member,
            offset: self.offset,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        });
        if !heard {
            self.stopped = true;
            return Err(io::Error::other("the owner stopped the run"));
        }
        self.offset += bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
