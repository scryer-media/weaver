// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stream's volume as a decoder reads it: from the front, waiting for bytes
// still on their way. It seeks too, for a format that locates damage by
// reading its own structure back; the end is known only once the volume
// closes, so a seek from the end waits for that.

use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use weaver_graph::{ByteRange, VolumeIndex, Volumes};

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

    // Where a range of the joined stream lies in its volumes, a piece per
    // volume it crosses, waiting for each volume it reaches to close.
    pub(crate) fn place(&self, range: ByteRange) -> io::Result<Vec<(VolumeIndex, ByteRange)>> {
        let mut pieces = Vec::new();
        let mut start = 0u64;
        let mut at = None;
        while start < range.end
            && let Some(volume) = self.input.next(at)?
        {
            at = Some(volume);
            let end = start + self.input.closed_len(volume)?;
            let (from, to) = (range.start.max(start), range.end.min(end));
            if from < to {
                pieces.push((volume, ByteRange::new(from - start, to - start)));
            }
            start = end;
        }
        Ok(pieces)
    }

    // Read on past the lost span a failed read stopped at, when that is why
    // it failed: returns the span's length, which the joined bytes leave as
    // a gap where the span's bytes would have been.
    pub(crate) fn bridge(&mut self, error: &io::Error) -> Option<u64> {
        let len = self.input.bridge(error)?;
        self.at += len;
        Some(len)
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
