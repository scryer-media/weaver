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

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use weaver_graph::{ByteRange, Give, MemberId, Segment, Shelf, SliceVerdict, Take};

// A run is folded eagerly inside its block, so ordered traffic needs one per
// grid, and the pending bounds only bite on pathological interleavings.
// They bound the runs a grid holds past what ordered traffic leaves: a
// whole member's grid holding one run from its start, as every member read
// in order ends with, counts against none of them.
pub(crate) const MAX_PENDING_RUNS_PER_FILE: usize = 4096;
const MAX_PENDING_RUNS_PER_JOB: usize = 16_384;
// A block closed by one run over its full extent is what every slice read
// in order ends as, so these bound what a job's sets may describe, not an
// interleaving: one member may hold every slice a PAR2 set may have, and
// one object every slice a set may track, across all its sets. A closed
// block is kept as its CRC32 and two bits.
pub(crate) const MAX_DERIVED_BLOCKS_PER_FILE: usize = 32_768;
const MAX_DERIVED_BLOCKS_PER_JOB: usize = 1_048_576;

// The block size of the grid whose one block is the whole member.
pub(crate) const WHOLE: u64 = u64::MAX;

// Hashed through the graph's own algorithm, so every CRC32 a store computes
// is one a test can count.
pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    weaver_graph::Algorithm::Crc32.hash(bytes).value as u32
}

// The CRC32 of `first` followed by `len_second` bytes whose CRC32 is `second`.
pub(crate) fn crc32_combine(first: u32, second: u32, len_second: u64) -> u32 {
    crc_turbo::checksum_combine(
        crc_turbo::CrcAlgorithm::Crc32IsoHdlc,
        u64::from(first),
        u64::from(second),
        len_second,
    ) as u32
}

// CRC32 of `len` zero bytes, by doubling. PAR2 checksums a short final slice
// zero-padded to the block size, so a CRC over the real bytes is padded the
// same way before the two are compared.
pub(crate) fn crc32_of_zeros(len: u64) -> u32 {
    crc_turbo::checksum_zeros(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, len) as u32
}

// A contiguous run inside one block, folded from the segments that made it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
// its full extent counts as derived, the one block of a whole member's grid
// holding one run from its start counts as nothing, and anything else
// counts as pending runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Load {
    pending: usize,
    derived: usize,
}

impl Load {
    fn of(block: &BTreeMap<u64, Run>, index: u64, block_size: u64) -> Self {
        match block.values().next() {
            Some(run) if block.len() == 1 && block_size == WHOLE && run.range.start == 0 => {
                Load::default()
            }
            Some(run) if block.len() == 1 && run.range == block_bounds(index, block_size) => Load {
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

// Blocks per chunk of a member's grid: a segment changes the chunk of the
// block it lands in, so a checkpoint writes that chunk again and not the
// member's whole grid.
const BLOCKS_PER_CHUNK: u64 = 16;

type Block = BTreeMap<u64, Run>;

// One chunk of a member's grid. A block closed by one run over its full
// extent, as every slice read in order ends, is kept as a bit, its CRC32
// and whether it was verified: its range is its place. Every other block
// keeps its runs, keyed by their start.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Chunk {
    // One bit per block of the chunk, by its place in it.
    closed: u16,
    verified: u16,
    // The closed blocks' CRC32s, in place order.
    crcs: Vec<u32>,
    open: BTreeMap<u64, Block>,
}

impl Chunk {
    // The block's place in its chunk. `None` past the last chunk, which
    // blocks of many places share: they stay open.
    fn place(index: u64) -> Option<u32> {
        (index / BLOCKS_PER_CHUNK < u64::from(u32::MAX))
            .then_some((index % BLOCKS_PER_CHUNK) as u32)
    }

    fn is_empty(&self) -> bool {
        self.closed == 0 && self.open.is_empty()
    }

    // Where a closed block's CRC32 lies among the chunk's.
    fn at(&self, place: u32) -> usize {
        (self.closed & ((1u16 << place) - 1)).count_ones() as usize
    }

    // The one run of a closed block.
    fn closed(&self, index: u64, block_size: u64) -> Option<Run> {
        let place = Self::place(index)?;
        (self.closed >> place & 1 == 1).then(|| Run {
            range: block_bounds(index, block_size),
            crc32: self.crcs[self.at(place)],
            verified: self.verified >> place & 1 == 1,
        })
    }

    // The block's runs, taken out of the chunk.
    fn take(&mut self, index: u64, block_size: u64) -> Block {
        let Some(run) = self.closed(index, block_size) else {
            return self.open.remove(&index).unwrap_or_default();
        };
        if let Some(place) = Self::place(index) {
            self.crcs.remove(self.at(place));
            self.closed &= !(1 << place);
            self.verified &= !(1 << place);
        }
        Block::from([(run.range.start, run)])
    }

    // The block's runs, put back: nothing when it has none.
    fn put(&mut self, index: u64, block_size: u64, block: Block) {
        let whole = block
            .values()
            .next()
            .filter(|run| block.len() == 1 && run.range == block_bounds(index, block_size));
        match (whole, Self::place(index)) {
            (Some(run), Some(place)) => {
                self.crcs.insert(self.at(place), run.crc32);
                self.closed |= 1 << place;
                self.verified |= u16::from(run.verified) << place;
            }
            _ if block.is_empty() => {}
            _ => {
                self.open.insert(index, block);
            }
        }
    }

    // Every block of chunk `number`, in index order.
    fn each(&self, number: u32, block_size: u64, mut each: impl FnMut(u64, &Block)) {
        let first = u64::from(number) * BLOCKS_PER_CHUNK;
        let closed = (0..BLOCKS_PER_CHUNK)
            .map(|place| first + place)
            .filter_map(|index| Some((index, self.closed(index, block_size)?)))
            .map(|(index, run)| (index, Block::from([(run.range.start, run)])));
        let mut blocks: Vec<(u64, std::borrow::Cow<'_, Block>)> = closed
            .map(|(index, block)| (index, std::borrow::Cow::Owned(block)))
            .chain(
                self.open
                    .iter()
                    .map(|(index, block)| (*index, std::borrow::Cow::Borrowed(block))),
            )
            .collect();
        blocks.sort_unstable_by_key(|(index, _)| *index);
        for (index, block) in &blocks {
            each(*index, block);
        }
    }
}

// Four values in a row. A chunk written before closed blocks were kept
// apart is a map of its blocks, and reads as one with every block open.
impl Serialize for Chunk {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut row = serializer.serialize_tuple(4)?;
        row.serialize_element(&self.closed)?;
        row.serialize_element(&self.verified)?;
        row.serialize_element(&self.crcs)?;
        row.serialize_element(&self.open)?;
        row.end()
    }
}

impl<'de> Deserialize<'de> for Chunk {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Read;

        impl<'de> Visitor<'de> for Read {
            type Value = Chunk;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a chunk of a checksum grid")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut row: A) -> Result<Chunk, A::Error> {
                let short = |at| serde::de::Error::invalid_length(at, &"four values");
                let chunk = Chunk {
                    closed: row.next_element()?.ok_or_else(|| short(0))?,
                    verified: row.next_element()?.ok_or_else(|| short(1))?,
                    crcs: row.next_element()?.ok_or_else(|| short(2))?,
                    open: row.next_element()?.ok_or_else(|| short(3))?,
                };
                if chunk.crcs.len() != chunk.closed.count_ones() as usize
                    || chunk.verified & !chunk.closed != 0
                {
                    return Err(serde::de::Error::custom("a chunk's closed blocks disagree"));
                }
                Ok(chunk)
            }

            fn visit_map<A: MapAccess<'de>>(self, mut blocks: A) -> Result<Chunk, A::Error> {
                let mut chunk = Chunk::default();
                while let Some((index, block)) = blocks.next_entry::<u64, Block>()? {
                    chunk.open.insert(index, block);
                }
                Ok(chunk)
            }
        }

        deserializer.deserialize_any(Read)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct FileGrid {
    // The blocks, shelved by chunk.
    blocks: Shelf<Chunk>,
    load: Load,
    // A whole member's grid went over a budget and its runs were forgotten,
    // so a fold was thrown away: a note, kept until the member is
    // forgotten. The grid goes on recording as any other. Never set on a
    // grid of smaller blocks. Absent from facts written before it was kept,
    // which read as not dropped.
    #[serde(default)]
    dropped: bool,
}

impl FileGrid {
    fn chunk(index: u64) -> u32 {
        // Past the last chunk only on a grid of more blocks than any file
        // has; they share it, which costs bytes, never a verdict.
        u32::try_from(index / BLOCKS_PER_CHUNK).unwrap_or(u32::MAX)
    }

    // What the block's runs fold to over `range`.
    fn fold(&self, index: u64, block_size: u64, range: ByteRange) -> Option<Folded> {
        let chunk = self.blocks.get(Self::chunk(index))?;
        match chunk.closed(index, block_size) {
            Some(run) => fold_tiling(std::iter::once(&run), range),
            None => fold_tiling(chunk.open.get(&index)?.values(), range),
        }
    }

    // Whether the block holds a run `range` overlaps.
    fn overlaps(&self, index: u64, block_size: u64, range: ByteRange) -> bool {
        self.blocks.get(Self::chunk(index)).is_some_and(|chunk| {
            match chunk.closed(index, block_size) {
                Some(run) => run.range.intersect(range).is_some(),
                None => chunk
                    .open
                    .get(&index)
                    .is_some_and(|block| overlapping(block, range).next().is_some()),
            }
        })
    }

    // Change one block, made empty first if the grid has none; an emptied
    // block is dropped.
    fn change<R>(
        &mut self,
        index: u64,
        block_size: u64,
        change: impl FnOnce(&mut Block) -> R,
    ) -> R {
        self.blocks
            .change(Self::chunk(index), Chunk::is_empty, |chunk| {
                let mut block = chunk.take(index, block_size);
                let out = change(&mut block);
                chunk.put(index, block_size, block);
                out
            })
    }

    // Every block, in index order.
    fn each(&self, block_size: u64, mut each: impl FnMut(u64, &Block)) {
        for (number, chunk) in self.blocks.chunks() {
            chunk.each(number, block_size, &mut each);
        }
    }

    // The block's runs, as they would be were none kept apart.
    #[cfg(test)]
    fn block(&self, index: u64, block_size: u64) -> Option<Block> {
        let chunk = self.blocks.get(Self::chunk(index))?;
        match chunk.closed(index, block_size) {
            Some(run) => Some(Block::from([(run.range.start, run)])),
            None => chunk.open.get(&index).cloned(),
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.blocks.chunks().next().is_none()
    }
}

// Every grid of every member one recovery object watches, under one budget.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Grid {
    files: BTreeMap<(MemberId, u64), FileGrid>,
    load: Load,
}

impl Grid {
    // Each member's shelf of blocks, in the order the grid encodes them.
    pub(crate) fn parts(&self, take: &mut Take) -> Result<(), String> {
        self.files
            .values()
            .try_for_each(|file| take.shelf(&file.blocks))
    }

    // The loads are counted again from the blocks given back, once per
    // block, so loads facts were written with stand for nothing.
    pub(crate) fn restore_parts(&mut self, give: &mut Give) -> Result<(), String> {
        self.load = Load::default();
        for (&(_, block_size), file) in &mut self.files {
            give.shelf(&mut file.blocks)?;
            let mut load = Load::default();
            file.each(block_size, |index, block| {
                load.add(Load::of(block, index, block_size));
            });
            file.load = load;
            self.load.add(file.load);
        }
        Ok(())
    }

    // Record a segment on the member's grid of `block_size`.
    pub(crate) fn offer(&mut self, member: MemberId, block_size: u64, segment: &Segment) {
        self.offer_all(member, block_size, std::slice::from_ref(segment));
    }

    // Record the segments of one delivery. Whatever any of them overlaps is
    // superseded before one is recorded, so a piece never joins a run the
    // next piece of the same delivery replaces.
    pub(crate) fn offer_all(&mut self, member: MemberId, block_size: u64, segments: &[Segment]) {
        if block_size == 0 {
            return;
        }
        for segment in segments {
            self.supersede(member, block_size, segment);
        }
        for segment in segments {
            self.record(member, block_size, segment);
        }
    }

    // Whatever the segment overlaps is superseded by it.
    fn supersede(&mut self, member: MemberId, block_size: u64, segment: &Segment) {
        if segment.range.is_empty() {
            return;
        }
        let first = segment.range.start / block_size;
        let last = (segment.range.end - 1) / block_size;
        let file = self.files.entry((member, block_size)).or_default();
        for index in first..=last {
            if file.overlaps(index, block_size, segment.range) {
                let (before, after) = file.change(index, block_size, |block| {
                    let before = Load::of(block, index, block_size);
                    remove_overlapping(block, segment.range);
                    (before, Load::of(block, index, block_size))
                });
                file.load.sub(before);
                file.load.add(after);
                self.load.sub(before);
                self.load.add(after);
            }
        }
    }

    fn record(&mut self, member: MemberId, block_size: u64, segment: &Segment) {
        if segment.range.is_empty() {
            return;
        }
        let first = segment.range.start / block_size;
        if first != (segment.range.end - 1) / block_size {
            return;
        }
        let file = self.files.entry((member, block_size)).or_default();
        let (before, after) = file.change(first, block_size, |block| {
            let before = Load::of(block, first, block_size);
            // A run the segment overlaps can only be one this delivery just
            // recorded, as the delivery superseded its whole range first. Two
            // pieces of one delivery that disagree on a stretch prove nothing
            // of it: both go, and the stretch is unproven.
            if remove_overlapping(block, segment.range) == 0 {
                insert_merged(
                    block,
                    Run {
                        range: segment.range,
                        crc32: segment.crc32,
                        verified: segment.verified,
                    },
                );
            }
            (before, Load::of(block, first, block_size))
        });
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
        // Over budget: the block is forgotten, so its slice is read back, and
        // later segments are recorded on it again. A whole member's grid
        // keeps a note that its fold was thrown away.
        file.change(first, block_size, Block::clear);
        file.load.sub(before);
        self.load.sub(before);
        if block_size == WHOLE {
            file.dropped = true;
        }
    }

    // Whether the member's whole grid went over a budget since the member
    // was last forgotten, so a fold of it was thrown away; it records again
    // all the same. Never for a grid of smaller blocks.
    pub(crate) fn dropped(&self, member: MemberId, block_size: u64) -> bool {
        block_size == WHOLE
            && self
                .files
                .get(&(member, block_size))
                .is_some_and(|file| file.dropped)
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
        self.files
            .get(&(member, block_size))?
            .fold(index, block_size, range)
    }

    // A replay can retain a ruling only where the retained evidence proves
    // the same accepted bytes. Missing or coalesced-away evidence is unknown.
    pub(crate) fn matches(&self, member: MemberId, segments: &[Segment]) -> bool {
        self.matches_on(member, WHOLE, segments)
    }

    pub(crate) fn matches_on(&self, member: MemberId, block: u64, segments: &[Segment]) -> bool {
        let mut at = 0;
        while let Some(first) = segments.get(at) {
            let index = first.range.start / block;
            let mut range = first.range;
            let mut crc = first.crc32;
            let mut verified = first.verified;
            at += 1;
            while let Some(segment) = segments
                .get(at)
                .filter(|segment| segment.range.start / block == index)
            {
                if range.end != segment.range.start {
                    return false;
                }
                crc = crc32_combine(crc, segment.crc32, segment.range.len());
                verified &= segment.verified;
                range.end = segment.range.end;
                at += 1;
            }
            if range.is_empty()
                || (range.end - 1) / block != index
                || self
                    .fold(member, block, index, range)
                    .is_none_or(|was| was.crc32 != crc || was.verified != verified)
            {
                return false;
            }
        }
        true
    }

    // The block sizes of the grids kept for the member, smallest first.
    pub(crate) fn sizes(&self, member: MemberId) -> Vec<u64> {
        self.files
            .range((member, 0)..=(member, u64::MAX))
            .map(|(key, _)| key.1)
            .collect()
    }

    pub(crate) fn forget(&mut self, member: MemberId) {
        let keys: Vec<_> = self
            .files
            .range((member, 0)..=(member, u64::MAX))
            .map(|(key, _)| *key)
            .collect();
        for key in keys {
            if let Some(file) = self.files.remove(&key) {
                self.load.sub(file.load);
            }
        }
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

// The runs of `block` that `range` overlaps, in offset order. A block's runs
// are disjoint, as the grid keeps them: a segment supersedes what it
// overlaps before it is recorded, and one that overlaps a run recorded
// since is not recorded. So no run before the one at or before the range's
// start reaches it, and a probe reads only the runs the range spans.
fn overlapping(block: &Block, range: ByteRange) -> impl Iterator<Item = &Run> {
    let from = block
        .range(..=range.start)
        .next_back()
        .map_or(range.start, |(start, _)| *start);
    block
        .range(from..range.end.max(from))
        .map(|(_, run)| run)
        .filter(move |run| run.range.intersect(range).is_some())
}

// Remove the runs of `block` that `range` overlaps, and say how many went.
fn remove_overlapping(block: &mut Block, range: ByteRange) -> usize {
    let mut removed = 0;
    loop {
        let next = overlapping(block, range).next();
        let Some(start) = next.map(|run| run.range.start) else {
            return removed;
        };
        block.remove(&start);
        removed += 1;
    }
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
