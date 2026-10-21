// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The volume a record sits in, read by range through the namespace the way
// unrar-rs reads a file. Each run of bytes that arrived, as the namespace
// says, is read as it is; bytes that never arrived read as zeros, which
// fail the record's checksums, so the record finds them damaged and
// rebuilds them.

use std::io::{self, Read, Seek, SeekFrom};

use weaver_graph::recovery::MemberAccess;
use weaver_graph::{ByteRange, MemberId};

pub(super) struct Volume<'a> {
    access: &'a mut dyn MemberAccess,
    member: MemberId,
    length: u64,
    at: u64,
    buffer: Vec<u8>,
}

impl<'a> Volume<'a> {
    pub(super) fn new(access: &'a mut dyn MemberAccess, member: MemberId, length: u64) -> Self {
        Self {
            access,
            member,
            length,
            at: 0,
            buffer: Vec::new(),
        }
    }

    pub(super) fn write_repaired(&mut self, offset: u64, bytes: &[u8]) {
        self.access.write_repaired(self.member, offset, bytes);
    }

    pub(super) fn found_damaged(&mut self, range: ByteRange) {
        self.access.found_damaged(self.member, range);
    }

    // `range` onto the buffer: what arrived of it as read, the rest zeros.
    fn read_range(&mut self, range: ByteRange) {
        let from = self.buffer.len();
        let mut runs = Vec::new();
        self.access.arrived(self.member, range, &mut runs);
        for run in runs {
            let start = from + (run.start - range.start) as usize;
            self.buffer.resize(start, 0);
            if !self.access.read(self.member, run, &mut self.buffer)
                || (self.buffer.len() - start) as u64 != run.len()
            {
                self.buffer.truncate(start);
            }
        }
        self.buffer.resize(from + range.len() as usize, 0);
    }
}

impl Read for Volume<'_> {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        let end = self.at.saturating_add(into.len() as u64).min(self.length);
        if self.at >= end {
            return Ok(0);
        }
        self.buffer.clear();
        self.read_range(ByteRange::new(self.at, end));
        let read = self.buffer.len();
        into[..read].copy_from_slice(&self.buffer);
        self.at = end;
        Ok(read)
    }
}

impl Seek for Volume<'_> {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let at = match to {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::End(delta) => self.length.checked_add_signed(delta),
            SeekFrom::Current(delta) => self.at.checked_add_signed(delta),
        };
        self.at = at.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok(self.at)
    }
}
