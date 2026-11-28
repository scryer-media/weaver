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
//
// The same run decodes the coded folders of a set the 7z store streams the
// rest of: only those folders, one after another, and only the store's
// members in them, numbered as the store numbers them. Everything else, the
// limits, the lender, the threads, the checks and what a failure says, is
// this one run's.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use sevenz_turbo::{
    Archive, ArchiveEntry, ArchiveReader, EntryRead, Error, Password, ReadAt, ReadAtCursor,
    UnorderedEntry,
};
use weaver_graph::{Budget, Emit, Extraction, MemberId, Provenance, Sink, Volumes, Why};

use crate::engine::Joined;
use crate::verdict;

pub(crate) struct Run {
    password: Option<String>,
    // Whether something other than this run proved the password: a failure
    // that would ask for one is then damage (`verdict::sevenz_under`).
    proven: bool,
    scope: Scope,
}

// What a run decodes, and which members it makes.
pub(crate) enum Scope {
    // Every folder; every file body and directory is a member, numbered in
    // archive order.
    Whole,
    // These folders alone, in this order; the members are these entries,
    // each numbered by its place in the end header, as the store numbers
    // them. Any other entry of the folders is decoded past and no member.
    Folders {
        folders: Vec<usize>,
        members: HashSet<usize>,
    },
}

impl Run {
    pub(crate) fn new(password: Option<String>) -> Self {
        Self::of(password, false, Scope::Whole)
    }

    pub(crate) fn of(password: Option<String>, proven: bool, scope: Scope) -> Self {
        Self {
            password,
            proven,
            scope,
        }
    }
}

impl Extraction for Run {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>, budget: Budget) {
        let input = Arc::new(Joined::new(volumes));
        let failed = input.failure();
        let password = self
            .password
            .as_deref()
            .map_or_else(Password::empty, Password::new);
        let mut walk = Walk {
            sink,
            ids: Vec::new(),
            first: Vec::new(),
            members: Vec::new(),
            announced: 0,
            ended: Vec::new(),
            closed: 0,
            stopped: false,
        };
        let mut in_ram = None;
        let walked = open(&input, password, &budget, &mut in_ram).and_then(|mut reader| {
            reader.set_verify_checksums(true);
            walk.number(&reader, &self.scope);
            match &self.scope {
                Scope::Whole => reader.for_each_entries_unordered(|event| walk.event(event)),
                Scope::Folders { folders, .. } => walk.folders(&mut reader, folders),
            }
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
            let member = walk.members[open as usize].id;
            if !(walk.sink)(Emit::Retract(member)) {
                return;
            }
        }
        // A window the scratch had no room for asks for the decode in RAM:
        // all of what it holds there on one thread, so one start in RAM is
        // enough, not just the window refused. What it was allowed there is
        // the run's memory, which it keeps in RAM what fits.
        let said = match verdict::sevenz_under(&error, self.proven) {
            Emit::Refuse(Why::TooLarge {
                needed: Some(needed),
                allowed: None,
            }) => Emit::Refuse(Why::TooLarge {
                needed: Some(needed.max(in_ram.unwrap_or(0))),
                allowed: budget.memory_bytes,
            }),
            said => said,
        };
        if (walk.sink)(Emit::Failed(verdict::failed(&said_of(&error)))) {
            (walk.sink)(said);
        }
    }
}

// The error as the job's log may show it: a file it could not open names
// the file, so only the failure's kind is told.
fn said_of(error: &sevenz_turbo::Error) -> String {
    match error {
        sevenz_turbo::Error::FileOpen(error, _) => format!("file open failed: {}", error.kind()),
        error => error.to_string(),
    }
}

// A member as the end header names it.
struct Named {
    id: MemberId,
    name: String,
    length: u64,
    crc32: Option<u32>,
}

// The reader over the run's input. The decoder holds no more than the run
// was granted, and decodes on as many threads. With a lender, what the run's
// memory holds stays in RAM (`crate::lent::sevenz_of`), and the decode runs
// on fewer threads where what they would borrow at once is more than the
// lender has room for. `in_ram` is then what a decode holds in RAM on one
// thread with nothing lent.
fn open(
    input: &Arc<Joined>,
    password: Password,
    budget: &Budget,
    in_ram: &mut Option<u64>,
) -> Result<ArchiveReader<ReadAtCursor<Joined>>, Error> {
    let limits = crate::sevenz_store::limits(budget);
    let most = u32::try_from(budget.threads()).unwrap_or(u32::MAX);
    let mut cursor = ReadAtCursor::new(Arc::clone(input));
    let archive = Archive::read_with_limits(&mut cursor, &password, &limits)?;
    let (limits, threads) = if budget.lender.is_some() {
        let limits = crate::lent::sevenz_of(&archive, budget);
        *in_ram = crate::lent::sevenz_in_ram(&archive, &limits);
        let threads = crate::lent::sevenz_threads(&archive, &limits, budget, most);
        (limits, threads)
    } else {
        (limits, most)
    };
    let mut reader = ArchiveReader::from_archive_with_limits(archive, cursor, password, limits)?;
    reader.set_positional_source(Arc::clone(input));
    reader.set_threads(threads);
    Ok(reader)
}

struct Walk<'s, 'a> {
    sink: &'s mut Sink<'a>,
    // Each entry's place among the run's members, by the entry's place in
    // the end header; none for an entry that is no member. Members are
    // ordered as the walk ends them, in archive order, and numbered as the
    // scope says, so every run of the same set numbers them alike.
    ids: Vec<Option<u32>>,
    // Each folder's first entry in the end header, for a walk by folder.
    first: Vec<usize>,
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
    // none is a member and the files around them stand. A walk by folder
    // makes only the entries its scope names, in the folders' order.
    fn number<R: io::Read + io::Seek>(&mut self, reader: &ArchiveReader<R>, scope: &Scope) {
        let files = &reader.archive().files;
        self.ids = vec![None; files.len()];
        let order = match scope {
            Scope::Whole => reader.entry_order(),
            Scope::Folders { folders, members } => {
                let blocks = &reader.archive().stream_map.file_block_index;
                self.first = reader.archive().stream_map.block_first_file_index.clone();
                let rank: HashMap<usize, usize> = folders
                    .iter()
                    .enumerate()
                    .map(|(rank, folder)| (*folder, rank))
                    .collect();
                let mut order: Vec<(usize, usize)> = members
                    .iter()
                    .filter(|index| **index < files.len())
                    .filter_map(|index| {
                        let folder = blocks.get(*index).copied().flatten()?;
                        Some((*rank.get(&folder)?, *index))
                    })
                    .collect();
                order.sort_unstable();
                order.into_iter().map(|(_, index)| index).collect()
            }
        };
        for index in order {
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
            let id = match scope {
                Scope::Whole => MemberId(self.members.len() as u32),
                Scope::Folders { .. } => MemberId(index as u32),
            };
            self.ids[index] = Some(self.members.len() as u32);
            self.members.push(Named {
                id,
                name,
                length,
                crc32,
            });
        }
        self.ended = vec![false; self.members.len()];
    }

    // The folders, one after another, each walked entry by entry as the
    // whole set's ordered walk walks a folder it decodes alone.
    fn folders<R: io::Read + io::Seek>(
        &mut self,
        reader: &mut ArchiveReader<R>,
        folders: &[usize],
    ) -> Result<(), Error> {
        for &folder in folders {
            let mut next = *self.first.get(folder).ok_or(Error::Other(Cow::Borrowed(
                "a folder the archive does not map",
            )))?;
            reader
                .block_decoder(folder)?
                .for_each_entries_direct(&mut |entry, read| {
                    let file_index = next;
                    next += 1;
                    self.entry(file_index, entry, read)?;
                    Ok(true)
                })?;
        }
        Ok(())
    }

    // One entry of a folder walked alone: begun, its bytes handed over in
    // the pieces the decoder made, and ended.
    fn entry(
        &mut self,
        file_index: usize,
        entry: &ArchiveEntry,
        read: &mut dyn EntryRead,
    ) -> Result<(), Error> {
        self.event(UnorderedEntry::Begin { file_index, entry })?;
        let mut out = Pieces {
            walk: self,
            file_index,
            offset: 0,
            error: None,
        };
        let written = read.write_rest(&mut out);
        if let Some(error) = out.error.take() {
            return Err(error);
        }
        written?;
        self.event(UnorderedEntry::End { file_index })?;
        Ok(())
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
                        member: self.members[id as usize].id,
                        offset,
                        bytes: Bytes::copy_from_slice(bytes).into(),
                        provenance: Provenance::Fresh,
                    })?;
                }
            }
            UnorderedEntry::End { file_index } => {
                if let Some(id) = self.id(file_index) {
                    self.ended[id as usize] = true;
                    // sevenz-turbo ends an entry with a CRC only once that CRC
                    // matched, so the member is said checked as it closes and
                    // the namespace does not check it again. The end can come
                    // after later entries of its folder began or ended.
                    while self.closed < self.announced && self.ended[self.closed as usize] {
                        let member = self.members[self.closed as usize].id;
                        let crc32 = self.members[self.closed as usize].crc32;
                        self.closed += 1;
                        if let Some(crc32) = crc32 {
                            self.say(Emit::Checked { member, crc32 })?;
                        }
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

    // Announces every member up to `id` not yet announced, in order.
    fn announce_through(&mut self, id: u32) -> Result<(), Error> {
        while self.announced <= id {
            let named = &self.members[self.announced as usize];
            let member = named.id;
            let (name, length) = (named.name.clone(), named.length);
            self.announced += 1;
            self.say(Emit::Member {
                member,
                name,
                length: Some(length),
            })?;
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

// An entry's bytes as the decoder writes them on, each piece handed to the
// walk as the unordered walk hands it.
struct Pieces<'w, 's, 'a> {
    walk: &'w mut Walk<'s, 'a>,
    file_index: usize,
    offset: u64,
    error: Option<Error>,
}

impl io::Write for Pieces<'_, '_, '_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let event = UnorderedEntry::Data {
            file_index: self.file_index,
            offset: self.offset,
            bytes,
        };
        if let Err(error) = self.walk.event(event) {
            self.error = Some(error);
            return Err(io::Error::other("the walk stopped"));
        }
        self.offset += bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
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
