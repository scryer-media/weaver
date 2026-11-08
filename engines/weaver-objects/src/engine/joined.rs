// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The owner's volumes as one source, end to end, for every engine whose
// library reads a set as one positional input. A volume still growing has no
// length yet, so a read that may lie in it is made there: it waits for the
// bytes, or for the volume to close short of them, and then the read carries
// on into the next. A read the owner fails is remembered, so the error the
// library makes of it ends the run silently instead of being taken for
// damage.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use weaver_graph::{VolumeIndex, Volumes};

pub(crate) struct Joined {
    volumes: Arc<dyn Volumes>,
    failed: Arc<AtomicBool>,
}

impl Joined {
    pub(crate) fn new(volumes: Arc<dyn Volumes>) -> Self {
        Self {
            volumes,
            failed: Arc::new(AtomicBool::new(false)),
        }
    }

    // Whether the owner failed a read, which the run reads once the library
    // has given up, after the source has moved into it.
    pub(crate) fn failure(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.failed)
    }

    fn read_volume(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        self.volumes
            .read_at(volume, offset, into)
            .inspect_err(|_| self.failed.store(true, Ordering::Release))
    }

    // The volume after `after` in the set's order, as the owner says.
    pub(crate) fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
        self.volumes
            .next(after)
            .inspect_err(|_| self.failed.store(true, Ordering::Release))
    }

    // Every volume's length in the set's order, waiting for each to close.
    pub(crate) fn closed_lens(&self) -> io::Result<Vec<u64>> {
        let mut lens = Vec::new();
        let mut at = None;
        while let Some(volume) = self.next(at)? {
            lens.push(self.closed_len(volume)?);
            at = Some(volume);
        }
        Ok(lens)
    }

    // Bytes at `offset` of the joined set; short only at its end.
    pub(crate) fn read(&self, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        if into.is_empty() {
            return Ok(0);
        }
        let mut start = 0u64;
        let mut at = None;
        while let Some(volume) = self.next(at)? {
            at = Some(volume);
            let local = offset - start;
            let len = match self.volumes.len(volume) {
                Some(len) => len,
                None => {
                    let read = self.read_volume(volume, local, into)?;
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
                return self.read_volume(volume, local, &mut into[..wanted]);
            }
            start += len;
        }
        Ok(0)
    }

    // Every byte of `into` from `offset`, or an error at the set's end.
    pub(crate) fn read_exact(&self, offset: u64, into: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        while filled < into.len() {
            let read = self.read(offset + filled as u64, &mut into[filled..])?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            filled += read;
        }
        Ok(())
    }

    // The volume's length, waiting for it to close: a read past any end
    // there can be returns only once the volume has one.
    pub(crate) fn closed_len(&self, volume: VolumeIndex) -> io::Result<u64> {
        loop {
            if let Some(len) = self.volumes.len(volume) {
                return Ok(len);
            }
            self.read_volume(volume, u64::MAX - 1, &mut [0u8; 1])?;
        }
    }
}
