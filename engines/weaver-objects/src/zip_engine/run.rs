// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One run of the zip engine. The zip library reads the directory first, at
// the set's tail, so the run waits for every volume's length before it
// reads anything, then walks the entries in directory order, each decoded
// straight into the sink with its CRC checked as it goes.

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ::zip::ZipArchive;
use bytes::Bytes;
use weaver_graph::{Emit, Extraction, MemberId, Provenance, Sink, Volumes};

use super::image::{Image, Unopened};
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
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>) {
        let joined = Joined::new(volumes);
        let failed = joined.failure();
        let image = match Image::open(joined) {
            Ok(image) => image,
            Err(Unopened::Refused(why)) if !failed.load(Ordering::Acquire) => {
                sink(Emit::Refuse(why));
                return;
            }
            Err(_) => return,
        };
        let mut walk = Walk {
            sink,
            next: 0,
            open: None,
        };
        // `None`: the owner stopped the run, so it already knows why.
        let Err(Some(verdict)) = walk.all(image, self.password.as_deref()) else {
            return;
        };
        // An owner that failed a read already knows why too.
        if failed.load(Ordering::Acquire) {
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
        let mut archive = ZipArchive::new(image).map_err(|error| Some(verdict::zip(&error)))?;
        for index in 0..archive.len() {
            let file = match password {
                Some(password) => archive.by_index_decrypt(index, password.as_bytes()),
                None => archive.by_index(index),
            };
            let mut file = file.map_err(|error| Some(verdict::zip(&error)))?;
            // The namespace has no directories, and no links or devices: a
            // directory is implied by the paths of the members inside it, and
            // the others are not file bodies.
            if !is_file_body(file.is_dir(), file.unix_mode()) {
                continue;
            }
            let member = MemberId(self.next);
            self.next += 1;
            self.say(Emit::Member {
                member,
                name: file.name().to_owned(),
                length: Some(file.size()),
            })?;
            self.open = Some(member);
            let encrypted = file.encrypted();
            let mut out = Out {
                sink: &mut *self.sink,
                member,
                offset: 0,
                stopped: false,
            };
            let copied = io::copy(&mut file, &mut out);
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

fn is_file_body(directory: bool, mode: Option<u32>) -> bool {
    const KIND: u32 = 0o170_000;
    const FILE: u32 = 0o100_000;
    !directory && mode.is_none_or(|mode| matches!(mode & KIND, 0 | FILE))
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
