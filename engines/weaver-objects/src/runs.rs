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
//
// Each run keeps whether its bytes are verified. Damaged bytes are recorded
// so the count of what is in stays true, but they have no reference value:
// a range holding any of them composes to nothing until clean bytes replace
// them, a repair's or another posting's, so no verdict is ever said over
// bytes known to be bad.

use unrar_rs::convert_crc32_to_mac;
use weaver_graph::{ByteRange, Provenance, SliceVerdict};

use crate::grid::crc32_combine;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CrcRuns {
    runs: Vec<Run>,
    held: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Run {
    start: u64,
    len: u64,
    crc: u32,
    verified: bool,
}

impl Run {
    fn end(&self) -> u64 {
        self.start.saturating_add(self.len)
    }
}

impl CrcRuns {
    // Record delivered bytes: a repair rewrites what it covers, and so do
    // clean bytes over damaged ones, whoever brings them; anything else is
    // a new run or a duplicate.
    pub(crate) fn feed(&mut self, range: ByteRange, crc: u32, provenance: Provenance) {
        let mends = provenance.is_verified()
            && self
                .runs
                .iter()
                .any(|run| !run.verified && run.start < range.end && range.start < run.end());
        match provenance {
            Provenance::Repaired => {
                self.overwrite(range.start, range.len(), crc);
            }
            Provenance::Fresh if mends => {
                self.overwrite(range.start, range.len(), crc);
            }
            Provenance::Fresh | Provenance::Damaged => {
                self.insert(Run {
                    start: range.start,
                    len: range.len(),
                    crc,
                    verified: provenance.is_verified(),
                });
            }
        }
    }

    // Inserts one run. Overlapping an existing run is ignored.
    fn insert(&mut self, run: Run) {
        if run.len == 0 {
            return;
        }
        let position = self.runs.partition_point(|next| next.start < run.start);
        if self
            .runs
            .get(position)
            .is_some_and(|next| next.start < run.end())
        {
            return;
        }
        if position
            .checked_sub(1)
            .is_some_and(|previous| self.runs[previous].end() > run.start)
        {
            return;
        }
        self.runs.insert(position, run);
        self.held += run.len;
    }

    // The bytes the runs hold between them.
    pub(crate) fn held(&self) -> u64 {
        self.held
    }

    // Replaces every run overlapping `[start, start + len)` with a single,
    // verified run for the rewritten span, and returns the sub-ranges of the
    // discarded runs that fall outside it: the stale gaps.
    pub(crate) fn overwrite(&mut self, start: u64, len: u64, crc: u32) -> Vec<(u64, u64)> {
        if len == 0 {
            return Vec::new();
        }
        let end = start.saturating_add(len);
        let mut gaps = Vec::new();
        let held = &mut self.held;
        self.runs.retain(|run| {
            if run.end() <= start || run.start >= end {
                return true;
            }
            *held -= run.len;
            if run.start < start {
                gaps.push((run.start, start));
            }
            if run.end() > end {
                gaps.push((end, run.end()));
            }
            false
        });
        let position = self.runs.partition_point(|run| run.start < start);
        self.runs.insert(
            position,
            Run {
                start,
                len,
                crc,
                verified: true,
            },
        );
        self.held += len;
        gaps
    }

    // The composed value for `[start, start + len)`, when verified runs tile
    // it exactly: the first starts at `start`, each one abuts the next, and the
    // last ends at `start + len`.
    pub(crate) fn compose(&self, start: u64, len: u64) -> Option<u32> {
        if len == 0 {
            return None;
        }
        let end = start.checked_add(len)?;
        let mut index = self.runs.partition_point(|run| run.start < start);
        let mut cursor = start;
        // CRC32 of no bytes is zero, and combining it with the first run is the
        // identity.
        let mut composed = 0u32;
        while cursor < end {
            let run = self.runs.get(index)?;
            if run.start != cursor || !run.verified {
                return None;
            }
            let run_end = run.start.checked_add(run.len)?;
            if run_end > end {
                return None;
            }
            composed = crc32_combine(composed, run.crc, run.len);
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

// What the runs say of `[0, length)` against the CRC a header wrote: unknown
// while any byte of it is missing or unverified. The composed value comes
// back beside a damaged verdict, so a blame can be said once per version of
// the bytes.
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
