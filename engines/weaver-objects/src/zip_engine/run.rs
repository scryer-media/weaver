// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One run of the zip engine. The zip library reads the directory first, at
// the set's tail, so the run's first reads wait for the tail to land, where
// the last volume's declared length puts it; it then walks the entries in
// directory order, each decoded straight into the sink with its CRC checked
// as it goes. Reads wait for bytes still on their way, so the run inflates
// as the download brings each entry, never ahead of it. A verdict waits for
// the volumes to close, as the lengths it rests on are final only then;
// an ask for a password does not.

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ::zip::read::ZipReadOptions;
use bytes::Bytes;
use weaver_graph::{Emit, Extraction, MemberId, Provenance, Sink, Volumes};

use super::image::{Image, Unopened};
use crate::engine::Joined;
use crate::verdict;
use crate::zip::records::{self, Shape};

pub(super) struct Run {
    password: Option<String>,
}

// Bytes a member's output gathers before the sink hears them. The zip
// library's readers hand back a few kilobytes at a time; each piece the sink
// hears costs a message, a write and the bookkeeping behind both, so pieces
// are gathered into a quarter megabyte first, at the cost of one copy.
pub(super) const EMIT_BYTES: usize = 256 * 1024;

impl Run {
    pub(super) fn new(password: Option<String>) -> Self {
        Self { password }
    }
}

impl Extraction for Run {
    fn run(
        self: Box<Self>,
        volumes: Arc<dyn Volumes>,
        sink: &mut Sink<'_>,
        budget: weaver_graph::Budget,
    ) {
        let joined = Joined::new(volumes);
        let failed = joined.failure();
        let image = match Image::open(joined, &budget) {
            Ok(image) => image,
            Err(Unopened::Refused(why)) if !failed.load(Ordering::Acquire) => {
                sink(Emit::Refuse(why));
                return;
            }
            Err(_) => return,
        };
        let disks = image.disks();
        let mut walk = Walk {
            sink,
            next: 0,
            open: None,
        };
        // `None`: the owner stopped the run, so it already knows why.
        let Err(Some(verdict)) = walk.all(image, self.password.as_deref()) else {
            return;
        };
        // An owner that failed a read already knows why too, as it does
        // when a volume closed at another length than the run read.
        if failed.load(Ordering::Acquire) {
            return;
        }
        // An ask for a password rests on no volume's length, and the tree
        // rules on it only once the job is quiet, so it is said at once: the
        // run the offer starts then decrypts alongside the download.
        if verdict != Emit::NeedsPassword && !disks.settled() {
            return;
        }
        // A member left open was built on bytes the run cannot vouch for.
        if let Some(open) = walk.open.take()
            && !(walk.sink)(Emit::Retract(open))
        {
            return;
        }
        (walk.sink)(verdict);
    }
}

struct Walk<'s, 'a> {
    sink: &'s mut Sink<'a>,
    // The id the next member announced gets: members are numbered in the
    // order the walk meets them, so every run of the same set numbers them
    // alike.
    next: u32,
    open: Option<MemberId>,
}

impl Walk<'_, '_> {
    fn all(&mut self, image: Image, password: Option<&str>) -> Result<(), Option<Emit>> {
        let archive = image
            .archive()
            .map_err(|error| Some(verdict::zip(&error)))?;
        let directory = archive.metadata();
        if directory.has_duplicate_names() {
            return Err(Some(Emit::Refuse(weaver_graph::Why::InvalidArchive)));
        }
        let mut image = archive.into_inner();
        for index in 0..directory.len() {
            let entry = directory
                .entry(index)
                .map_err(|error| Some(verdict::zip(&error)))?;
            image
                .check_header(&entry)
                .map_err(|error| Some(verdict::zip(&error)))?;
            // The directory bounds the read and the decoded output. A stale
            // local size does not discard a member whose CRC still checks.
            let options = ZipReadOptions::new().password(password.map(str::as_bytes));
            let mut file = entry
                .with_reader(&mut image, options)
                .map_err(|error| Some(verdict::zip(&error)))?;
            let name = file
                .name()
                .map_err(|error| Some(verdict::zip(&error)))?
                .into_owned();
            // A directory is a member with nothing in it, named as the store
            // names it. Links and devices are not file bodies, so none is a
            // member and the files around them stand.
            let shape = records::shape(&name, file.unix_mode());
            let length = match shape {
                Shape::File => file.size(),
                Shape::Directory => 0,
                Shape::Other => continue,
            };
            let name = records::member_name(name, shape);
            let member = MemberId(self.next);
            self.next += 1;
            self.say(Emit::Member {
                member,
                name,
                length: Some(length),
            })?;
            self.open = Some(member);
            let encrypted = file.encrypted();
            if shape == Shape::File && !encrypted {
                self.say(Emit::Checksum {
                    member,
                    crc32: file.crc32(),
                })?;
            }
            let out = Out {
                sink: &mut *self.sink,
                member,
                offset: 0,
                stopped: false,
            };
            let mut out = io::BufWriter::with_capacity(EMIT_BYTES, out);
            let copied = io::copy(&mut file, &mut out).and_then(|_| out.flush());
            // What is still gathered when the copy failed is not said.
            let (out, _) = out.into_parts();
            if out.stopped {
                return Err(None);
            }
            copied.map_err(|error| Some(verdict::zip_read(&error, encrypted)))?;
            self.open = None;
            self.say(Emit::MemberClosed(member))?;
        }
        Ok(())
    }

    fn say(&mut self, emit: Emit) -> Result<(), Option<Emit>> {
        (self.sink)(emit).then_some(()).ok_or(None)
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
