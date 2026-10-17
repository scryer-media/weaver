// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One run of the RAR engine. unrar-rs walks the set's headers a file header
// at a time, volume after volume, and each member is decoded as soon as its
// header is read, straight into the sink. Reads wait for bytes still on
// their way, so the run keeps pace with a download without ever reading
// ahead of it, and a solid set keeps its dictionary across members.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use unrar_rs::{RarArchive, RarError, ReadSeek, VolumeProvider, VolumeProviderError};
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
        let mut walk = Walk {
            input: Input {
                volumes,
                failed: Arc::new(AtomicBool::new(false)),
            },
            sink,
            next: 0,
            open: None,
        };
        let Err(Stop::Say(last)) = walk.archive(self.password.as_deref()) else {
            return;
        };
        // A member left open was built on bytes the run cannot vouch for.
        if let Some(open) = walk.open.take()
            && !(walk.sink)(Emit::Retract(open.member))
        {
            return;
        }
        (walk.sink)(last);
    }
}

// How a run ends early: silently, because the owner stopped it or failed a
// read and so already knows why, or saying why.
enum Stop {
    Silent,
    Say(Emit),
}

struct Open {
    member: MemberId,
    encrypted: bool,
}

struct Walk<'s, 'a> {
    input: Input,
    sink: &'s mut Sink<'a>,
    // The id the next member announced gets: members are numbered in
    // archive order, so every run of the same set numbers them alike.
    next: u32,
    open: Option<Open>,
}

impl Walk<'_, '_> {
    fn archive(&mut self, password: Option<&str>) -> Result<(), Stop> {
        let mut archive =
            RarArchive::open_prefix(self.input.reader(0), password, NonZeroUsize::MIN)
                .map_err(|error| self.fail(error))?;
        // Bytes may catch up with a large member as it decodes; adaptive
        // decoding picks inline or batched work from what it measures.
        archive.set_decode_mode(unrar_rs::DecodeMode::Adaptive);
        archive.set_verify(true);
        let mut index = 0;
        let mut volume = 0;
        while volume < self.input.volumes.count() {
            let mut headers = NonZeroUsize::MIN;
            loop {
                let found = archive
                    .extend_volume_prefix(volume, Box::new(self.input.reader(volume)), headers)
                    .map_err(|error| self.fail(error))?;
                while index < archive.len() {
                    self.member(&mut archive, index, password.is_some())?;
                    index += 1;
                }
                if found < headers.get() {
                    break;
                }
                headers = headers
                    .checked_add(1)
                    .ok_or(Stop::Say(Emit::Refuse(Why::Unsupported)))?;
            }
            volume += 1;
        }
        Ok(())
    }

    fn member(
        &mut self,
        archive: &mut RarArchive,
        index: usize,
        has_password: bool,
    ) -> Result<(), Stop> {
        let Some(info) = archive.member_info(index) else {
            return Err(self.fail(RarError::MemberIndexOutOfRange {
                index,
                len: archive.len(),
            }));
        };
        // The namespace has no directories, and no links: a directory is
        // implied by the paths of the members inside it, and a link is not a
        // file body, so neither is a member and the files around them stand.
        if info.is_directory || info.is_symlink || info.is_hardlink || info.is_file_copy {
            return Ok(());
        }
        if info.is_encrypted && !has_password {
            return Err(Stop::Say(Emit::NeedsPassword));
        }
        let member = MemberId(self.next);
        self.next += 1;
        self.say(Emit::Member {
            member,
            name: unrar_rs::sanitize_path(&info.raw_name),
            length: info.unpacked_size,
        })?;
        self.open = Some(Open {
            member,
            encrypted: info.is_encrypted,
        });
        let mut out = Out {
            sink: &mut *self.sink,
            member,
            offset: 0,
            stopped: false,
        };
        let decoded = archive
            .by_index_via(index, &self.input)
            .and_then(|entry| entry.copy_to(&mut out));
        match decoded {
            Ok(_) => {
                self.open = None;
                self.say(Emit::MemberClosed(member))
            }
            Err(_) if out.stopped => Err(Stop::Silent),
            Err(error) => Err(self.fail(error)),
        }
    }

    fn say(&mut self, emit: Emit) -> Result<(), Stop> {
        if (self.sink)(emit) {
            Ok(())
        } else {
            Err(Stop::Silent)
        }
    }

    fn fail(&self, error: RarError) -> Stop {
        if self.input.failed.load(Ordering::Acquire) {
            return Stop::Silent;
        }
        let encrypted = self.open.as_ref().is_some_and(|open| open.encrypted);
        Stop::Say(verdict(&error, encrypted))
    }
}

// What a RAR error means for the job. A wrong key decrypts to bytes whose
// checksum fails, so on an encrypted member a checksum failure asks for
// another password before it is called damage.
fn verdict(error: &RarError, encrypted: bool) -> Emit {
    match error {
        RarError::EncryptedArchive
        | RarError::EncryptedMember { .. }
        | RarError::InvalidPassword
        | RarError::WrongPassword { .. } => Emit::NeedsPassword,
        RarError::DataCrcMismatch { .. } | RarError::Blake2Mismatch { .. } if encrypted => {
            Emit::NeedsPassword
        }
        RarError::UnsafeLinkTarget { .. } | RarError::UnsupportedLinkType { .. } => {
            Emit::Refuse(Why::NotAFile)
        }
        RarError::InvalidSignature
        | RarError::UnsupportedFormat { .. }
        | RarError::UnsupportedCompression { .. }
        | RarError::UnsupportedEncryption { .. }
        | RarError::UnsupportedEncryptionKdf { .. }
        | RarError::UnsupportedFilter { .. }
        | RarError::DictionaryTooLarge { .. }
        | RarError::ResourceLimit { .. }
        | RarError::VolumeProviderRequired { .. }
        | RarError::SolidOrderViolation { .. } => Emit::Refuse(Why::Unsupported),
        _ => Emit::Refuse(Why::Unrepairable),
    }
}

// The owner's volumes as unrar-rs reads them: one reader per open, each with
// its own cursor. A read the owner fails is remembered, so the error unrar-rs
// makes of it ends the run silently instead of being taken for damage.
struct Input {
    volumes: Arc<dyn Volumes>,
    failed: Arc<AtomicBool>,
}

impl Input {
    fn reader(&self, volume: usize) -> Reader {
        Reader {
            volumes: Arc::clone(&self.volumes),
            failed: Arc::clone(&self.failed),
            volume: VolumeIndex(u32::try_from(volume).unwrap_or(u32::MAX)),
            position: 0,
        }
    }
}

impl VolumeProvider for Input {
    fn get_volume(&self, index: usize) -> Result<Box<dyn ReadSeek>, VolumeProviderError> {
        if index >= self.volumes.count() {
            return Err(VolumeProviderError::Unavailable {
                volume: index,
                reason: "the set has no such volume".into(),
            });
        }
        Ok(Box::new(self.reader(index)))
    }
}

struct Reader {
    volumes: Arc<dyn Volumes>,
    failed: Arc<AtomicBool>,
    volume: VolumeIndex,
    position: u64,
}

impl Read for Reader {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        if into.is_empty() {
            return Ok(0);
        }
        match self.volumes.read_at(self.volume, self.position, into) {
            Ok(read) => {
                self.position += read as u64;
                Ok(read)
            }
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                Err(error)
            }
        }
    }
}

impl Seek for Reader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::Current(by) => self.position.checked_add_signed(by),
            // A growing volume has no end yet; unrar-rs reads a prefix
            // without one.
            SeekFrom::End(by) => self
                .volumes
                .len(self.volume)
                .ok_or_else(|| io::Error::from(io::ErrorKind::Unsupported))?
                .checked_add_signed(by),
        };
        self.position = position.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok(self.position)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("the owner stopped the run")]
struct Stopped;

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
            return Err(io::Error::other(Stopped));
        }
        self.offset += bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
