// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A volume's bytes in hand as the reader the header walk reads. Each run sits
// at its own offset and a hole reads as a clean end, so the walk stops where
// the bytes stop and never sees a byte that did not arrive.

use std::io::{Read, Seek, SeekFrom};

use bytes::Bytes;
use weaver_graph::Provenance;

use crate::kept::Kept;

pub(super) struct SparseImage {
    runs: Vec<(u64, Bytes)>,
    position: u64,
}

// Whether the walk may read a run: one that arrived damaged once the volume
// is closed, one that arrived clean at once, and a loss never, since no
// bytes of the volume are there, whatever a copy since sent back left.
pub(super) fn readable(provenance: Provenance, closed: bool) -> bool {
    provenance.arrived() && (closed || !provenance.is_damaged())
}

impl SparseImage {
    // The runs a walk may trust, as `readable` says.
    pub(super) fn of(kept: &Kept, closed: bool) -> Self {
        Self {
            runs: kept
                .runs_from(0)
                .filter(|(_, _, provenance)| readable(*provenance, closed))
                .map(|(range, bytes, _)| (range.start, Bytes::copy_from_slice(bytes)))
                .collect(),
            position: 0,
        }
    }
}

impl Read for SparseImage {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let position = self.position;
        let index = self.runs.partition_point(|(start, _)| *start <= position);
        let Some(index) = index.checked_sub(1) else {
            return Ok(0);
        };
        let (start, run) = &self.runs[index];
        let inside = position - start;
        if inside >= run.len() as u64 {
            return Ok(0);
        }
        let taken = (run.len() as u64 - inside).min(out.len() as u64) as usize;
        out[..taken].copy_from_slice(&run[inside as usize..inside as usize + taken]);
        self.position = position.saturating_add(taken as u64);
        Ok(taken)
    }
}

impl Seek for SparseImage {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        self.position = match from {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => self.position.saturating_add_signed(offset),
            // The image has no end: its last run ends wherever the last
            // delivery reached, which is not the volume's length.
            SeekFrom::End(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "a sparse volume image has no end to seek from",
                ));
            }
        };
        Ok(self.position)
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        Ok(self.position)
    }
}
