// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The CRC grid: slice verdicts from the segment checksums the namespace
// hands a recovery object as bytes pass, without reading a member back.
//
// A segment is a range of a member and the CRC32 of its bytes. Segments that
// fall inside one block of a grid are folded, as they arrive, into runs; a
// block whose runs tile it exactly has a CRC, and that CRC is compared with
// the one the recovery set wrote for the slice. A segment that crosses a
// block boundary cannot be split from its CRC alone, so it only clears what
// it overlaps: the block is then unproven and is read back if it matters.
// Everything is positional, so a range written again, by a replay or a
// repair, replaces what was recorded for it and the verdict is recomputed.
//
// A grid whose block is the whole member gives the second arm, the
// whole-file CRC32, from the same machinery: segments of any alignment fold
// into it.

use std::collections::BTreeMap;

use weaver_graph::{ByteRange, MemberId, Segment, SliceVerdict};

// The old engine's bounds, kept as the object's own: a run is folded eagerly
// inside its block, so ordered traffic needs one per grid, and these only
// bite on pathological interleavings.
const MAX_PENDING_RUNS_PER_FILE: usize = 4096;
const MAX_DERIVED_BLOCKS_PER_FILE: usize = 16_384;
const MAX_PENDING_RUNS_PER_JOB: usize = 16_384;
const MAX_DERIVED_BLOCKS_PER_JOB: usize = 65_536;

// The block size of the grid whose one block is the whole member.
pub(crate) const WHOLE: u64 = u64::MAX;

pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
}

// The CRC32 of `first` followed by `len_second` bytes whose CRC32 is `second`.
pub(crate) fn crc32_combine(first: u32, second: u32, len_second: u64) -> u32 {
    crc_fast::checksum_combine(
        crc_fast::CrcAlgorithm::Crc32IsoHdlc,
        u64::from(first),
        u64::from(second),
        len_second,
    ) as u32
}

// CRC32 of `len` zero bytes, by doubling. PAR2 checksums a short final slice
// zero-padded to the block size, so a CRC over the real bytes is padded the
// same way before the two are compared; doubling keeps that logarithmic.
pub(crate) fn crc32_of_zeros(len: u64) -> u32 {
    let mut unit = (1u64, crc32(&[0]));
    let mut total: Option<(u64, u32)> = None;
    let mut remaining = len;
    while remaining > 0 {
        if remaining & 1 == 1 {
            total = Some(match total {
                Some((have, crc)) => (have + unit.0, crc32_combine(crc, unit.1, unit.0)),
                None => (unit.0, unit.1),
            });
        }
        remaining >>= 1;
        if remaining > 0 {
            unit = (unit.0 * 2, crc32_combine(unit.1, unit.1, unit.0));
        }
    }
    total.map_or_else(|| crc32(&[]), |(_, crc)| crc)
}

// A contiguous run inside one block, folded from the segments that made it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Run {
    range: ByteRange,
    crc32: u32,
    // Every segment in the run passed an independent checksum upstream.
    verified: bool,
}

// What a block of a member's grid says once its runs are folded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Folded {
    pub(crate) crc32: u32,
    pub(crate) verified: bool,
}

// Fold runs that tile `range` exactly. `None` on a gap, an overlap, or a run
// outside the range: an unproven block, never a confident wrong answer.
fn fold_tiling<'a>(runs: impl Iterator<Item = &'a Run>, range: ByteRange) -> Option<Folded> {
    let mut cursor = range.start;
    let mut folded: Option<Folded> = None;
    for run in runs {
        if run.range.start != cursor || run.range.is_empty() || run.range.end > range.end {
            return None;
        }
        folded = Some(match folded {
            Some(prefix) => Folded {
                crc32: crc32_combine(prefix.crc32, run.crc32, run.range.len()),
                verified: prefix.verified && run.verified,
            },
            None => Folded {
                crc32: run.crc32,
                verified: run.verified,
            },
        });
        cursor = run.range.end;
    }
    (cursor == range.end).then_some(folded).flatten()
}

// What one block holds against the bounds: a block closed by one run over
// its full extent counts as derived, anything else as pending runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Load {
    pending: usize,
    derived: usize,
}

impl Load {
    fn of(block: &BTreeMap<u64, Run>, bounds: ByteRange) -> Self {
        match block.values().next() {
            Some(run) if block.len() == 1 && run.range == bounds => Load {
                pending: 0,
                derived: 1,
            },
            _ => Load {
                pending: block.len(),
                derived: 0,
            },
        }
    }

    fn add(&mut self, other: Load) {
        self.pending += other.pending;
        self.derived += other.derived;
    }

    fn sub(&mut self, other: Load) {
        self.pending = self.pending.saturating_sub(other.pending);
        self.derived = self.derived.saturating_sub(other.derived);
    }

    fn within(self, pending: usize, derived: usize) -> bool {
        self.pending <= pending && self.derived <= derived
    }
}

#[derive(Debug, Default)]
struct FileGrid {
    // Runs per block index, keyed by their start.
    blocks: BTreeMap<u64, BTreeMap<u64, Run>>,
    load: Load,
}

// Every grid of every member one recovery object watches, under one budget.
#[derive(Debug, Default)]
pub(crate) struct Grid {
    files: BTreeMap<(MemberId, u64), FileGrid>,
    load: Load,
}

impl Grid {
    // Record a segment on the member's grid of `block_size`.
    pub(crate) fn offer(&mut self, member: MemberId, block_size: u64, segment: &Segment) {
        if block_size == 0 || segment.range.is_empty() {
            return;
        }
        let first = segment.range.start / block_size;
        let last = (segment.range.end - 1) / block_size;
        let file = self.files.entry((member, block_size)).or_default();
        // Whatever the segment overlaps is superseded by it.
        for index in first..=last {
            let bounds = block_bounds(index, block_size);
            if let Some(block) = file.blocks.get_mut(&index) {
                let before = Load::of(block, bounds);
                block.retain(|_, run| run.range.intersect(segment.range).is_none());
                let after = Load::of(block, bounds);
                file.load.sub(before);
                file.load.add(after);
                self.load.sub(before);
                self.load.add(after);
                if block.is_empty() {
                    file.blocks.remove(&index);
                }
            }
        }
        if first != last {
            return;
        }
        let bounds = block_bounds(first, block_size);
        let block = file.blocks.entry(first).or_default();
        let before = Load::of(block, bounds);
        insert_merged(
            block,
            Run {
                range: segment.range,
                crc32: segment.crc32,
                verified: segment.verified,
            },
        );
        let after = Load::of(block, bounds);
        let mut file_load = file.load;
        file_load.sub(before);
        file_load.add(after);
        let mut job_load = self.load;
        job_load.sub(before);
        job_load.add(after);
        if file_load.within(MAX_PENDING_RUNS_PER_FILE, MAX_DERIVED_BLOCKS_PER_FILE)
            && job_load.within(MAX_PENDING_RUNS_PER_JOB, MAX_DERIVED_BLOCKS_PER_JOB)
        {
            file.load = file_load;
            self.load = job_load;
            return;
        }
        // Over budget: the block is forgotten, so its slice is read back.
        file.blocks.remove(&first);
        file.load.sub(before);
        self.load.sub(before);
    }

    // The fold of the member's block `index` on the grid of `block_size`,
    // over `range`, which is the block's extent clipped to the length the
    // caller compares against.
    pub(crate) fn fold(
        &self,
        member: MemberId,
        block_size: u64,
        index: u64,
        range: ByteRange,
    ) -> Option<Folded> {
        let block = self.files.get(&(member, block_size))?.blocks.get(&index)?;
        fold_tiling(block.values(), range)
    }
}

// Compare a fold over a slice's real bytes with the CRC the set wrote for
// the slice, zero-padded to the block size the way PAR2 checksums it.
pub(crate) fn verdict(
    folded: Option<Folded>,
    real: u64,
    block_size: u64,
    expected: u32,
) -> SliceVerdict {
    let Some(folded) = folded else {
        return SliceVerdict::Unknown;
    };
    let padding = block_size.saturating_sub(real);
    let padded = if padding == 0 {
        folded.crc32
    } else {
        crc32_combine(folded.crc32, crc32_of_zeros(padding), padding)
    };
    match (padded == expected, folded.verified) {
        (false, _) => SliceVerdict::Damaged,
        (true, true) => SliceVerdict::Intact,
        (true, false) => SliceVerdict::Unknown,
    }
}

fn block_bounds(index: u64, block_size: u64) -> ByteRange {
    let start = index.saturating_mul(block_size);
    ByteRange::new(start, start.saturating_add(block_size))
}

// Insert a run into its block, folding it with a neighbour it abuts and
// agrees with on verification. An unverified run stays apart, so the
// segment that later replaces it supersedes it alone.
fn insert_merged(block: &mut BTreeMap<u64, Run>, mut run: Run) {
    if let Some((&start, &before)) = block.range(..run.range.start).next_back()
        && before.range.end == run.range.start
        && before.verified == run.verified
    {
        block.remove(&start);
        run = Run {
            range: ByteRange::new(before.range.start, run.range.end),
            crc32: crc32_combine(before.crc32, run.crc32, run.range.len()),
            verified: run.verified,
        };
    }
    if let Some(after) = block.get(&run.range.end).copied()
        && after.verified == run.verified
    {
        block.remove(&after.range.start);
        run = Run {
            range: ByteRange::new(run.range.start, after.range.end),
            crc32: crc32_combine(run.crc32, after.crc32, after.range.len()),
            verified: run.verified,
        };
    }
    block.insert(run.range.start, run);
}

#[cfg(test)]
mod tests;
