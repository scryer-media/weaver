// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One run of the 7z engine. sevenz-turbo reads the file as one positional
// source and decodes its entries with their CRCs checked as it goes, the
// folders of a non-solid archive side by side on as many workers as the
// budget's threads and memory allow, each entry's bytes handed to the sink
// as they come. A 7z names its members only in the end header at its tail,
// so the run's first reads wait for the tail to land; reads wait for bytes
// still on their way, so the run never reads ahead of the download.
//
// Members are numbered, announced and closed in archive order whichever
// folder finishes first: numbered and announced from the end header, and a
// member closed only once every member before it is, so a run that fails
// partway closes what an entry-by-entry walk would have closed and no more.

use std::borrow::Cow;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use sevenz_turbo::{ArchiveEntry, ArchiveReader, Error, Password, ReadAt, UnorderedEntry};
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
        let input = Joined::new(volumes);
        let failed = input.failure();
        let password = self
            .password
            .as_deref()
            .map_or_else(Password::empty, Password::new);
        let mut walk = Walk {
            sink,
            ids: Vec::new(),
            members: Vec::new(),
            announced: 0,
            ended: Vec::new(),
            closed: 0,
            stopped: false,
        };
        // The decoder holds no more than the run was granted, and decodes on
        // as many threads.
        let limits = crate::sevenz_store::limits(&budget);
        let threads = u32::try_from(budget.threads()).unwrap_or(u32::MAX);
        let walked = ArchiveReader::from_read_at(input, password, limits).and_then(|mut reader| {
            reader.set_verify_checksums(true);
            reader.set_threads(threads);
            walk.number(&reader);
            reader.for_each_entries_unordered(|event| walk.event(event))
        });
        let Err(error) = walked else {
            return;
        };
        // The owner stopped the run or failed a read, so it already knows why.
        if walk.stopped || failed.load(Ordering::Acquire) {
            return;
        }
        // A member announced and not closed was built on bytes the run
        // cannot vouch for, or on none yet.
        for open in walk.closed..walk.announced {
            if !(walk.sink)(Emit::Retract(MemberId(open))) {
                return;
            }
        }
        (walk.sink)(verdict::sevenz(&error));
    }
}

// A member as the end header names it.
struct Named {
    name: String,
    length: u64,
    crc32: Option<u32>,
}

struct Walk<'s, 'a> {
    sink: &'s mut Sink<'a>,
    // Each entry's member, by the entry's place in the end header; none for
    // an entry that is no member. Members are numbered in archive order, so
    // every run of the same set numbers them alike.
    ids: Vec<Option<u32>>,
    members: Vec<Named>,
    // Members below this are announced.
    announced: u32,
    // Whether each member's every byte is handed over and checked.
    ended: Vec<bool>,
    // Members below this are closed.
    closed: u32,
    stopped: bool,
}

impl Walk<'_, '_> {
    // A directory is a member with nothing in it, named with a trailing
    // slash as the store names it. Links and devices are not file bodies, so
    // none is a member and the files around them stand.
    fn number<R: io::Read + io::Seek>(&mut self, reader: &ArchiveReader<R>) {
        let files = &reader.archive().files;
        self.ids = vec![None; files.len()];
        for index in reader.entry_order() {
            let entry = &files[index];
            let mut name = entry.name().to_owned();
            let length = if is_directory(entry) {
                if !name.ends_with(['/', '\\']) {
                    name.push('/');
                }
                0
            } else if is_file_body(entry) {
                entry.size()
            } else {
                continue;
            };
            let crc32 = (length > 0 && entry.has_crc).then_some(entry.crc as u32);
            self.ids[index] = Some(self.members.len() as u32);
            self.members.push(Named {
                name,
                length,
                crc32,
            });
        }
        self.ended = vec![false; self.members.len()];
    }

    fn event(&mut self, event: UnorderedEntry<'_>) -> Result<bool, Error> {
        match event {
            UnorderedEntry::Begin { file_index, .. } => {
                if let Some(id) = self.id(file_index) {
                    self.announce_through(id)?;
                }
            }
            // sevenz-turbo hands over a link's body too; it is no member's.
            UnorderedEntry::Data {
                file_index,
                offset,
                bytes,
            } => {
                if let Some(id) = self.id(file_index)
                    && !bytes.is_empty()
                {
                    self.say(Emit::Bytes {
                        member: MemberId(id),
                        offset,
                        bytes: Bytes::copy_from_slice(bytes).into(),
                        provenance: Provenance::Fresh,
                    })?;
                }
            }
            UnorderedEntry::End { file_index } => {
                if let Some(id) = self.id(file_index) {
                    self.ended[id as usize] = true;
                    while self.closed < self.announced && self.ended[self.closed as usize] {
                        let member = MemberId(self.closed);
                        self.closed += 1;
                        self.say(Emit::MemberClosed(member))?;
                    }
                }
            }
            _ => {}
        }
        Ok(true)
    }

    fn id(&self, file_index: usize) -> Option<u32> {
        self.ids.get(file_index).copied().flatten()
    }

    // Announces every member up to `id` not yet announced, in order, each
    // with the checksum its header gives.
    fn announce_through(&mut self, id: u32) -> Result<(), Error> {
        while self.announced <= id {
            let member = MemberId(self.announced);
            let named = &self.members[self.announced as usize];
            let (name, length, crc32) = (named.name.clone(), named.length, named.crc32);
            self.announced += 1;
            self.say(Emit::Member {
                member,
                name,
                length: Some(length),
            })?;
            if let Some(crc32) = crc32 {
                self.say(Emit::Checksum { member, crc32 })?;
            }
        }
        Ok(())
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

// The file, as sevenz-turbo reads it.
impl ReadAt for Joined {
    fn read_at(&self, into: &mut [u8], offset: u64) -> io::Result<usize> {
        self.read(offset, into)
    }

    // The signature header says where the end header lies and how long it
    // is, and a 7z ends with it: the file's length while it is still
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
