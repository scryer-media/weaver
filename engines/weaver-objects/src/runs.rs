// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The member gate a store reads: (offset, len, crc32) runs over one
// contiguous space, composed on demand, and compared with the CRC the
// archive's header wrote for the member or the part, folded with the hash key
// when the header keys it.
//
// The runs are kept exactly as they were fed, never merged, so any sub-range
// that starts and ends on a run boundary can be composed. A run that
// overlaps one already recorded is a duplicate and is ignored: the same bytes
// must not advance the composition twice. A repaired run is the opposite: the
// bytes changed, so it replaces every run it overlaps, and the parts of those
// runs outside it are covered by nothing until they are fed again. A range the
// runs do not tile exactly has no reference value, which is a refusal, never
// a pass. The bytes held are counted as runs come and go, so a whole is
// composed once, when every byte of it is in, and not at every delivery.

use unrar_rs::convert_crc32_to_mac;
use weaver_graph::{ByteRange, Provenance, SliceVerdict};

use crate::grid::crc32_combine;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CrcRuns {
    runs: Vec<(u64, u64, u32)>,
    held: u64,
}

impl CrcRuns {
    // Record delivered bytes: a repair rewrites what it covers, anything
    // else is a new run or a duplicate.
    pub(crate) fn feed(&mut self, range: ByteRange, crc: u32, provenance: Provenance) {
        match provenance {
            Provenance::Repaired => {
                self.overwrite(range.start, range.len(), crc);
            }
            Provenance::Fresh | Provenance::Damaged => self.insert(range.start, range.len(), crc),
        }
    }

    // Inserts one run. Overlapping an existing run is ignored.
    pub(crate) fn insert(&mut self, start: u64, len: u64, crc: u32) {
        if len == 0 {
            return;
        }
        let end = start.saturating_add(len);
        let position = self
            .runs
            .partition_point(|(run_start, _, _)| *run_start < start);
        if position < self.runs.len() {
            let (next_start, _, _) = self.runs[position];
            if next_start < end {
                return;
            }
        }
        if let Some(index) = position.checked_sub(1) {
            let (previous_start, previous_len, _) = self.runs[index];
            if previous_start.saturating_add(previous_len) > start {
                return;
            }
        }
        self.runs.insert(position, (start, len, crc));
        self.held += len;
    }

    // The bytes the runs hold between them.
    pub(crate) fn held(&self) -> u64 {
        self.held
    }

    // Replaces every run overlapping `[start, start + len)` with a single run
    // for the rewritten span, and returns the sub-ranges of the discarded runs
    // that fall outside it: the stale gaps.
    pub(crate) fn overwrite(&mut self, start: u64, len: u64, crc: u32) -> Vec<(u64, u64)> {
        if len == 0 {
            return Vec::new();
        }
        let end = start.saturating_add(len);
        let mut gaps = Vec::new();
        let held = &mut self.held;
        self.runs.retain(|&(run_start, run_len, _)| {
            let run_end = run_start.saturating_add(run_len);
            if run_end <= start || run_start >= end {
                return true;
            }
            *held -= run_len;
            if run_start < start {
                gaps.push((run_start, start));
            }
            if run_end > end {
                gaps.push((end, run_end));
            }
            false
        });
        let position = self
            .runs
            .partition_point(|(run_start, _, _)| *run_start < start);
        self.runs.insert(position, (start, len, crc));
        self.held += len;
        gaps
    }

    // The composed value for `[start, start + len)`, when the runs fed in tile
    // it exactly: the first starts at `start`, each one abuts the next, and the
    // last ends at `start + len`.
    pub(crate) fn compose(&self, start: u64, len: u64) -> Option<u32> {
        if len == 0 {
            return None;
        }
        let end = start.checked_add(len)?;
        let mut index = self
            .runs
            .partition_point(|(run_start, _, _)| *run_start < start);
        let mut cursor = start;
        // CRC32 of no bytes is zero, and combining it with the first run is the
        // identity.
        let mut composed = 0u32;
        while cursor < end {
            let (run_start, run_len, run_crc) = *self.runs.get(index)?;
            if run_start != cursor {
                return None;
            }
            let run_end = run_start.checked_add(run_len)?;
            if run_end > end {
                return None;
            }
            composed = crc32_combine(composed, run_crc, run_len);
            cursor = run_end;
            index += 1;
        }
        Some(composed)
    }
}

// The composed CRC32 as the header compares it: folded with the KDF hash key
// when the header keys it. `None` is a header claiming a keyed checksum for a
// member with no hash key, a contradiction no value answers.
pub(crate) fn fold_member_crc(
    composed: u32,
    uses_mac: bool,
    hash_key: Option<&[u8; 32]>,
) -> Option<u32> {
    match (uses_mac, hash_key) {
        (true, Some(hash_key)) => Some(convert_crc32_to_mac(composed, hash_key)),
        (false, _) => Some(composed),
        (true, None) => None,
    }
}

// What the runs say of `[0, length)` against the CRC a header wrote. The
// composed value comes back beside a damaged verdict, so a blame can be said
// once per version of the bytes.
pub(crate) fn gate(
    runs: &CrcRuns,
    length: u64,
    expected: u32,
    uses_mac: bool,
    hash_key: Option<&[u8; 32]>,
) -> (SliceVerdict, u32) {
    if runs.held() != length {
        return (SliceVerdict::Unknown, 0);
    }
    let Some(composed) = runs.compose(0, length) else {
        return (SliceVerdict::Unknown, 0);
    };
    match fold_member_crc(composed, uses_mac, hash_key) {
        Some(folded) if folded == expected => (SliceVerdict::Intact, composed),
        Some(_) => (SliceVerdict::Damaged, composed),
        None => (SliceVerdict::Unknown, composed),
    }
}

#[cfg(test)]
mod tests;
