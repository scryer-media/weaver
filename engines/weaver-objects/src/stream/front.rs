// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stream's volume as a decoder reads it: from the front, waiting for bytes
// still on their way. It seeks too, for a format that reads its own
// structure back from its tail; the end is known only once the volume
// closes, so a seek from the end waits for that.

use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use weaver_graph::Volumes;

use crate::engine::Joined;

pub struct Front {
    input: Joined,
    at: u64,
}

impl Front {
    pub(crate) fn new(volumes: Arc<dyn Volumes>) -> Self {
        Self {
            input: Joined::new(volumes),
            at: 0,
        }
    }

    // Whether the owner failed a read: the run was cancelled or the bytes
    // will never come, which the owner already knows.
    pub(crate) fn failure(&self) -> Arc<AtomicBool> {
        self.input.failure()
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.input.closed_lens()?.iter().sum())
    }
}

impl Read for Front {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        let read = self.input.read(self.at, into)?;
        self.at += read as u64;
        Ok(read)
    }
}

impl Seek for Front {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let at = match to {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::Current(by) => self.at.checked_add_signed(by),
            SeekFrom::End(by) => self.len()?.checked_add_signed(by),
        };
        self.at = at.ok_or(io::ErrorKind::InvalidInput)?;
        Ok(self.at)
    }
}
