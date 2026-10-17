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
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use sevenz_turbo::{
    ArchiveEntry, ArchiveLimits, ArchiveReader, BlockErrorKind, EntryRead, Error, Limit, Password,
    ReadAt,
};
use weaver_graph::{Emit, Extraction, MemberId, Provenance, Sink, VolumeIndex, Volumes, Why};

pub(super) struct Run {
    password: Option<String>,
}

impl Run {
    pub(super) fn new(password: Option<String>) -> Self {
        Self { password }
    }
}

impl Extraction for Run {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>) {
        let input = Joined {
            volumes,
            failed: Arc::new(AtomicBool::new(false)),
        };
        let failed = Arc::clone(&input.failed);
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
        let walked = ArchiveReader::from_read_at(input, password, ArchiveLimits::default())
            .and_then(|mut reader| {
                reader.set_verify_checksums(true);
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
        (walk.sink)(verdict(&error));
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
        // The namespace has no directories, and no links or devices: a
        // directory is implied by the paths of the members inside it, and
        // the others are not file bodies, so none is a member and the files
        // around them stand. sevenz-turbo drains what is left unread.
        if !is_file_body(entry) {
            return Ok(true);
        }
        let member = MemberId(self.next);
        self.next += 1;
        self.say(Emit::Member {
            member,
            name: entry.name().to_owned(),
            length: Some(entry.size()),
        })?;
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

// What a 7z error means for the job. sevenz-turbo already calls a checksum
// failure in encrypted content a possibly bad password, so a wrong key asks
// for another before it is called damage.
fn verdict(error: &Error) -> Emit {
    match error {
        Error::PasswordRequired
        | Error::MaybeBadPassword(_)
        | Error::BlockDecode {
            kind: BlockErrorKind::Password,
            ..
        } => Emit::NeedsPassword,
        Error::LimitExceeded { what, .. } if *what != Limit::ArchiveBytes => {
            Emit::Refuse(Why::Unsupported)
        }
        Error::BadSignature(_)
        | Error::UnsupportedVersion { .. }
        | Error::ExternalUnsupported
        | Error::UnsupportedCompressionMethod(_)
        | Error::Unsupported(_)
        | Error::MaxMemLimited { .. }
        | Error::MemoryLimited { .. }
        | Error::EndHeaderTooLarge { .. }
        | Error::BlockDecode {
            kind: BlockErrorKind::UnsupportedMethod,
            ..
        } => Emit::Refuse(Why::Unsupported),
        _ => Emit::Refuse(Why::Unrepairable),
    }
}

// The owner's volumes as one source, end to end. A volume still growing has
// no length yet, so a read that may lie in it is made there: it waits for
// the bytes, or for the volume to close short of them, and then the read
// carries on into the next. A read the owner fails is remembered, so the
// error sevenz-turbo makes of it ends the run silently instead of being
// taken for damage.
struct Joined {
    volumes: Arc<dyn Volumes>,
    failed: Arc<AtomicBool>,
}

impl Joined {
    fn read(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        self.volumes
            .read_at(volume, offset, into)
            .inspect_err(|_| self.failed.store(true, Ordering::Release))
    }
}

impl ReadAt for Joined {
    fn read_at(&self, into: &mut [u8], offset: u64) -> io::Result<usize> {
        if into.is_empty() {
            return Ok(0);
        }
        let mut start = 0u64;
        for volume in 0..self.volumes.count() {
            let volume = VolumeIndex(
                u32::try_from(volume).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
            );
            let local = offset - start;
            let len = match self.volumes.len(volume) {
                Some(len) => len,
                None => {
                    let read = self.read(volume, local, into)?;
                    if read > 0 {
                        return Ok(read);
                    }
                    self.volumes
                        .len(volume)
                        .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?
                }
            };
            if local < len {
                let wanted =
                    usize::try_from(len - local).map_or(into.len(), |rest| rest.min(into.len()));
                return self.read(volume, local, &mut into[..wanted]);
            }
            start += len;
        }
        Ok(0)
    }

    // The signature header says where the end header lies and how long it
    // is, and a 7z ends with it: the set's length while it is still
    // growing, and the reason the run waits for the tail before anything
    // else. Bytes after the end header, a trailing recovery set, are not the
    // archive's.
    fn size(&self) -> io::Result<u64> {
        let mut header = [0u8; 32];
        let mut filled = 0;
        while filled < header.len() {
            let read = self.read_at(&mut header[filled..], filled as u64)?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            filled += read;
        }
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
            bytes: Bytes::copy_from_slice(bytes),
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
