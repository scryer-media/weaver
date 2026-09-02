//! Segment-CRC checkpointing: one CRC pass over decoded article bytes that
//! also yields PAR2 block CRC32s.
//!
//! The decode path already computes a CRC32 over every decoded byte to verify
//! the yEnc `pcrc32`. That pass covers exactly the bytes PAR2 blocks are made
//! of, just cut at article boundaries instead of block boundaries. Checkpointing
//! the pass at block boundaries makes one pass serve both integrity families:
//!
//! - the article `pcrc32` is the in-order combine-fold of the article's own
//!   segments, and
//! - a block CRC32 is the combine-fold of the segments tiling that block, which
//!   may span several articles.
//!
//! Segment assembly across articles happens in the evidence collector above the
//! decoder; this module provides the checkpointing pass ([`SegmentedCrc32`]),
//! the record type ([`Segment`]) and the composition primitives
//! ([`combine_contiguous`], [`crate::crc::crc32_combine`]).
//!
//! # Intended call pattern
//!
//! The caller declares boundaries in *absolute output offsets* — the file
//! offset the article's decoded bytes land at, plus the PAR2 block size — and
//! then feeds decoded bytes in whatever chunks the driver produces. Checkpoint
//! placement is a function of file offsets alone, so it is invariant to chunk
//! and window boundaries:
//!
//! ```rust
//! use std::num::NonZeroU64;
//! use weaver_yenc::segment::{CheckpointPlan, SegmentedCrc32, combine_contiguous};
//!
//! // Article starts 3 MiB into the file; PAR2 block size is 1 MiB.
//! let mut pass = SegmentedCrc32::new(
//!     3 << 20,
//!     CheckpointPlan::Single(NonZeroU64::new(1 << 20).expect("non-zero")),
//! );
//! pass.update(&[b'a'; 700 * 1024]); // fed in arbitrary chunks by the driver
//! pass.update(&[b'b'; 700 * 1024]);
//! let segments = pass.finish();
//!
//! // Article pcrc32 == the in-order fold of the article's segments.
//! let article = combine_contiguous(&segments).expect("contiguous");
//! assert_eq!(article.file_offset, 3 << 20);
//! assert_eq!(article.len, 1_400 * 1024);
//! ```
//!
//! Before PAR2 geometry is known, construct with `CheckpointPlan::None`: the
//! pass emits one segment for the whole article. Such segments still compose
//! whenever they happen to tile a block; blocks they do not tile fall back to
//! settle-time read-back. An article is never delayed or re-decoded to obtain
//! checkpoints.

use std::num::NonZeroU64;
use std::sync::Arc;

use crate::crc::{Crc32, Crc32Combine, crc32_combine};

/// Maximum number of distinct PAR2 slice grids admitted to one checkpoint
/// plan. Keeping this small bounds both boundary selection and the work a
/// decoded segment can offer to downstream collectors.
pub const MAX_CHECKPOINT_GRIDS: usize = 32;

/// Hard cap for checkpoint segment storage for one article. The count below is
/// deliberately derived from the representation so a future `Segment` layout
/// change cannot silently raise the allocation bound.
pub const MAX_CHECKPOINT_SEGMENT_BYTES: usize = 64 * 1024;
pub const MAX_CHECKPOINT_SEGMENTS: usize =
    MAX_CHECKPOINT_SEGMENT_BYTES / std::mem::size_of::<Segment>();

/// Maximum downstream grid offers represented by one decoded article.
pub const MAX_CHECKPOINT_OFFER_WORK: usize = 65_536;

/// An immutable snapshot of every PAR2 slice geometry known when an article
/// began decoding. It describes cuts only; file/set ownership remains a
/// durable-commit decision.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum CheckpointPlan {
    #[default]
    None,
    Single(NonZeroU64),
    Multi(Arc<[NonZeroU64]>),
}

/// Why a plan was safely degraded to [`CheckpointPlan::None`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointPlanDegradation {
    TooManyGrids,
    InvalidSliceSize,
}

/// Why a checkpointing pass safely discarded fine-grained segments for one
/// article. The decoded bytes and article CRC remain valid; downstream PAR2
/// evidence must simply read back any block the coarse segment cannot tile.
///
/// This deliberately records only a small, stable classification. It crosses
/// the yEnc/server boundary so the server can profile defensive fallbacks once
/// per completed article without adding instrumentation to the cut loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointCollapseReason {
    SegmentCount,
    SegmentStorage,
    OfferWork,
    BoundaryOverflow,
    MissingBoundary,
    NonAdvancingBoundary,
    OffsetOverflow,
}

/// Result of normalizing untrusted parsed slice sizes into a safe plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointPlanBuild {
    pub plan: CheckpointPlan,
    pub degradation: Option<CheckpointPlanDegradation>,
}

impl CheckpointPlan {
    /// Normalize, sort, and deduplicate slice sizes without ever growing a
    /// temporary list beyond the admitted-grid cap. An oversized distinct set
    /// degrades as a whole rather than keeping an order-dependent prefix.
    pub fn from_sizes<I>(sizes: I) -> CheckpointPlanBuild
    where
        I: IntoIterator<Item = NonZeroU64>,
    {
        let mut normalized = Vec::with_capacity(MAX_CHECKPOINT_GRIDS.min(8));
        for size in sizes {
            match normalized.binary_search(&size) {
                Ok(_) => {}
                Err(index) if normalized.len() < MAX_CHECKPOINT_GRIDS => {
                    normalized.insert(index, size);
                }
                Err(_) => {
                    return CheckpointPlanBuild {
                        plan: Self::None,
                        degradation: Some(CheckpointPlanDegradation::TooManyGrids),
                    };
                }
            }
        }

        Self::from_normalized(normalized)
    }

    fn from_normalized(normalized: Vec<NonZeroU64>) -> CheckpointPlanBuild {
        let plan = match normalized.as_slice() {
            [] => Self::None,
            [size] => Self::Single(*size),
            _ => Self::Multi(normalized.into()),
        };
        CheckpointPlanBuild {
            plan,
            degradation: None,
        }
    }

    /// Normalize raw PAR2 slice sizes. A zero value is malformed metadata, so
    /// it disables checkpointing for the whole snapshot instead of being
    /// silently filtered out and changing the plan's meaning.
    pub fn from_slice_sizes<I>(slice_sizes: I) -> CheckpointPlanBuild
    where
        I: IntoIterator<Item = u64>,
    {
        let mut normalized = Vec::with_capacity(MAX_CHECKPOINT_GRIDS.min(8));
        for raw_size in slice_sizes {
            let Some(size) = NonZeroU64::new(raw_size) else {
                return CheckpointPlanBuild {
                    plan: Self::None,
                    degradation: Some(CheckpointPlanDegradation::InvalidSliceSize),
                };
            };
            match normalized.binary_search(&size) {
                Ok(_) => {}
                Err(index) if normalized.len() < MAX_CHECKPOINT_GRIDS => {
                    normalized.insert(index, size);
                }
                Err(_) => {
                    return CheckpointPlanBuild {
                        plan: Self::None,
                        degradation: Some(CheckpointPlanDegradation::TooManyGrids),
                    };
                }
            }
        }
        Self::from_normalized(normalized)
    }

    #[inline]
    pub fn sizes(&self) -> &[NonZeroU64] {
        match self {
            Self::None => &[],
            Self::Single(size) => std::slice::from_ref(size),
            Self::Multi(sizes) => sizes,
        }
    }

    #[inline]
    pub fn grid_count(&self) -> usize {
        self.sizes().len()
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

/// A contiguous run of decoded output bytes and its standalone CRC32.
///
/// `crc32` is a complete CRC32 over `[file_offset, file_offset + len)` with the
/// standard init and final xor — not a running prefix value — so segments
/// compose in any order-preserving tiling via [`combine_contiguous`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    /// Absolute offset of the first byte within the reconstructed file.
    pub file_offset: u64,
    /// Number of bytes covered.
    pub len: u64,
    /// CRC32 (ISO-HDLC) over exactly those bytes.
    pub crc32: u32,
}

impl Segment {
    /// Absolute offset one past the last byte covered.
    #[inline]
    pub fn end_offset(&self) -> u64 {
        self.file_offset + self.len
    }
}

/// A CRC32 pass over decoded output bytes that checkpoints at caller-declared
/// block boundaries.
///
/// Feeding is chunk-agnostic: `update` may be called with any split of the
/// article's decoded bytes and produces the same segments, because boundaries
/// are derived from absolute file offsets rather than from call boundaries.
#[derive(Debug, Clone)]
pub struct SegmentedCrc32 {
    crc: Crc32,
    /// File offset of the first byte fed to this pass.
    base_offset: u64,
    /// File offset of the first byte of the segment currently open.
    open_offset: u64,
    /// File offset one past the last byte fed.
    cursor: u64,
    /// Bytes remaining until the next fixed-stride checkpoint.
    until_boundary: Option<u64>,
    checkpoint_plan: CheckpointPlan,
    /// Next boundary for each admitted grid. The plan cap makes this fixed.
    next_boundaries: [u64; MAX_CHECKPOINT_GRIDS],
    next_boundary_count: usize,
    segments: Vec<Segment>,
    /// In-order combine-fold of every closed segment, i.e. the CRC32 over
    /// `[base_offset, open_offset)`. Maintained at each cut so the whole-pass
    /// CRC is available in O(1) without re-folding the segment list.
    closed_crc: u32,
    /// Bytes covered by closed segments. Kept independently of absolute file
    /// offsets so an address overflow can fail closed without corrupting the
    /// article CRC calculation.
    closed_len: u64,
    /// Total decoded bytes in this article.
    total_len: u64,
    /// Fine-grained evidence was discarded after a defensive limit or offset
    /// arithmetic failure. The final result is one coarse article segment.
    collapsed: bool,
    collapse_reason: Option<CheckpointCollapseReason>,
    /// The combine operator for the last cut's segment length. Segments on a
    /// checkpoint stride all have the stride's length, so the operator is
    /// built once per stride rather than once per cut.
    combine_memo: Option<(u64, Crc32Combine)>,
}

impl SegmentedCrc32 {
    /// Start a pass whose first byte lands at `file_offset` in the
    /// reconstructed file, checkpointing according to an immutable geometry
    /// snapshot.
    pub fn new(file_offset: u64, mut checkpoint_plan: CheckpointPlan) -> Self {
        let oversized_multi = matches!(
            &checkpoint_plan,
            CheckpointPlan::Multi(sizes) if sizes.len() > MAX_CHECKPOINT_GRIDS
        );
        if oversized_multi {
            checkpoint_plan = CheckpointPlan::None;
        }
        let (until_boundary, next_boundaries, next_boundary_count, collapse_reason) =
            match &checkpoint_plan {
                CheckpointPlan::None => (
                    None,
                    [0; MAX_CHECKPOINT_GRIDS],
                    0,
                    oversized_multi.then_some(CheckpointCollapseReason::OfferWork),
                ),
                CheckpointPlan::Single(size) => (
                    Some(bytes_to_next_boundary(file_offset, *size)),
                    [0; MAX_CHECKPOINT_GRIDS],
                    0,
                    None,
                ),
                CheckpointPlan::Multi(sizes) => {
                    let mut boundaries = [0; MAX_CHECKPOINT_GRIDS];
                    let mut valid = true;
                    for (index, &size) in sizes.iter().enumerate() {
                        let Some(boundary) = next_boundary_after(file_offset, size) else {
                            valid = false;
                            break;
                        };
                        boundaries[index] = boundary;
                    }
                    if valid {
                        (None, boundaries, sizes.len(), None)
                    } else {
                        (
                            None,
                            [0; MAX_CHECKPOINT_GRIDS],
                            0,
                            Some(CheckpointCollapseReason::BoundaryOverflow),
                        )
                    }
                }
            };
        Self {
            crc: Crc32::new(),
            base_offset: file_offset,
            open_offset: file_offset,
            cursor: file_offset,
            until_boundary,
            checkpoint_plan,
            next_boundaries,
            next_boundary_count,
            segments: Vec::new(),
            closed_crc: 0,
            closed_len: 0,
            total_len: 0,
            collapsed: collapse_reason.is_some(),
            collapse_reason,
            combine_memo: None,
        }
    }

    /// The combine operator for a closed segment of `len` bytes, reusing the
    /// last one when the length repeats.
    #[inline]
    fn combine_for(&mut self, len: u64) -> Crc32Combine {
        match self.combine_memo {
            Some((memo_len, op)) if memo_len == len => op,
            _ => {
                let op = Crc32Combine::new(len);
                self.combine_memo = Some((len, op));
                op
            }
        }
    }

    /// File offset of the first byte this pass covers.
    #[inline]
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// File offset one past the last byte fed so far.
    #[inline]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Bytes fed so far.
    #[inline]
    pub fn len(&self) -> u64 {
        self.total_len
    }

    /// Whether no bytes have been fed yet.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    /// Immutable geometry this article used for checkpointing.
    #[inline]
    pub fn checkpoint_plan(&self) -> &CheckpointPlan {
        &self.checkpoint_plan
    }

    /// Why this article was reduced to one coarse segment, if it was.
    #[inline]
    pub fn collapse_reason(&self) -> Option<CheckpointCollapseReason> {
        self.collapse_reason
    }

    /// CRC32 over every byte fed so far — the article `pcrc32` as of the
    /// cursor, identical to what an unsegmented [`Crc32`] over the same bytes
    /// would report.
    ///
    /// Closed segments are folded in as they are cut, so this is one combine
    /// over the open segment rather than a re-fold of the whole list.
    #[inline]
    pub fn current_crc(&self) -> u32 {
        crc32_combine(
            self.closed_crc,
            self.crc.current(),
            self.total_len - self.closed_len,
        )
    }

    /// Segments closed so far. The segment currently open is not included;
    /// call [`Self::finish`] to close it.
    #[inline]
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// Feed decoded output bytes, closing a segment at every checkpoint-plan
    /// boundary crossed.
    pub fn update(&mut self, mut data: &[u8]) {
        if self.collapsed || self.checkpoint_plan.is_none() {
            self.consume(data);
            return;
        }

        match &self.checkpoint_plan {
            CheckpointPlan::Single(_) => self.update_single(&mut data),
            CheckpointPlan::Multi(_) => self.update_multi(&mut data),
            CheckpointPlan::None => unreachable!("the no-plan fast path returned above"),
        }
    }

    /// Close the segment currently open at the current cursor.
    ///
    /// Automatic block-boundary checkpoints call this; callers with boundaries
    /// that are not a fixed stride can drive it directly. A no-op when no bytes
    /// have been fed since the previous checkpoint — zero-length segments are
    /// never emitted.
    pub fn checkpoint(&mut self) {
        if !self.collapsed && self.total_len > self.closed_len {
            self.cut();
        }
    }

    /// Close the open segment and return every segment in file order.
    pub fn finish(mut self) -> Vec<Segment> {
        self.checkpoint();
        self.finish_segments()
    }

    /// Close the open segment and return both the CRC32 over everything fed
    /// (the article `pcrc32`) and the segments in file order.
    ///
    /// This is the decoder-facing finish: the CRC is the value an unsegmented
    /// pass would have produced, so checkpointing cannot move an article's
    /// verdict, and the segments are the block-aligned evidence.
    pub fn finish_article(self) -> (u32, Vec<Segment>) {
        let (crc, segments, _) = self.finish_article_with_reason();
        (crc, segments)
    }

    /// Like [`Self::finish_article`], retaining the defensive-collapse
    /// classification for once-per-article observability.
    pub fn finish_article_with_reason(
        self,
    ) -> (u32, Vec<Segment>, Option<CheckpointCollapseReason>) {
        let (crc, segments, _, reason) = self.finish_article_with_reason_and_plan();
        (crc, segments, reason)
    }

    /// Like [`Self::finish_article_with_reason`], returning the immutable
    /// plan by move for the transport result.
    pub fn finish_article_with_reason_and_plan(
        mut self,
    ) -> (
        u32,
        Vec<Segment>,
        CheckpointPlan,
        Option<CheckpointCollapseReason>,
    ) {
        self.checkpoint();
        // Every closed segment has been folded into `closed_crc`, and
        // `checkpoint` just closed the last open one, so this is the whole-pass
        // CRC without re-folding the list.
        let crc = self.current_crc();
        let segments = self.finish_segments();
        (crc, segments, self.checkpoint_plan, self.collapse_reason)
    }

    fn cut(&mut self) {
        if let Some(reason) = self.budget_collapse_reason() {
            self.collapse(reason);
            return;
        }
        if self.segments.len() == self.segments.capacity() {
            let target = self
                .segments
                .capacity()
                .saturating_mul(2)
                .clamp(4, MAX_CHECKPOINT_SEGMENTS);
            if self
                .segments
                .try_reserve_exact(target - self.segments.len())
                .is_err()
                || self.segments.capacity() > MAX_CHECKPOINT_SEGMENTS
            {
                self.collapse(CheckpointCollapseReason::SegmentStorage);
                return;
            }
        }
        let len = self.total_len - self.closed_len;
        let crc32 = self.crc.checkpoint();
        self.segments.push(Segment {
            file_offset: self.open_offset,
            len,
            crc32,
        });
        self.closed_crc = self.combine_for(len).combine(self.closed_crc, crc32);
        self.closed_len = self.total_len;
        self.open_offset = self.cursor;
        if let CheckpointPlan::Single(size) = self.checkpoint_plan {
            self.until_boundary = Some(bytes_to_next_boundary(self.cursor, size));
        }
    }

    fn update_single(&mut self, data: &mut &[u8]) {
        let mut remaining = self
            .until_boundary
            .expect("single checkpoint plans always have a boundary");
        while !data.is_empty() {
            let take = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(data.len());
            self.consume(&data[..take]);
            *data = &data[take..];
            if self.collapsed {
                self.consume(data);
                return;
            }
            remaining -= take as u64;
            if remaining == 0 {
                self.cut();
                if self.collapsed {
                    self.consume(data);
                    return;
                }
                remaining = self
                    .until_boundary
                    .expect("single checkpoint plans always have a boundary");
            }
        }
        self.until_boundary = Some(remaining);
    }

    fn update_multi(&mut self, data: &mut &[u8]) {
        while !data.is_empty() {
            let Some(next_boundary) = self.next_boundaries[..self.next_boundary_count]
                .iter()
                .copied()
                .min()
            else {
                self.collapse(CheckpointCollapseReason::MissingBoundary);
                self.consume(data);
                return;
            };
            let remaining = next_boundary.saturating_sub(self.cursor);
            if remaining == 0 {
                self.collapse(CheckpointCollapseReason::NonAdvancingBoundary);
                self.consume(data);
                return;
            }
            let take = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(data.len());
            self.consume(&data[..take]);
            *data = &data[take..];
            if self.collapsed {
                self.consume(data);
                return;
            }
            if self.cursor == next_boundary {
                self.cut();
                if self.collapsed || !self.advance_multi_boundaries(next_boundary) {
                    self.collapse(CheckpointCollapseReason::BoundaryOverflow);
                    self.consume(data);
                    return;
                }
            }
        }
    }

    fn consume(&mut self, data: &[u8]) {
        self.crc.update(data);
        let len = data.len() as u64;
        self.total_len = self.total_len.saturating_add(len);
        match self.cursor.checked_add(len) {
            Some(cursor) => self.cursor = cursor,
            None => {
                self.cursor = u64::MAX;
                self.collapse(CheckpointCollapseReason::OffsetOverflow);
            }
        }
    }

    fn advance_multi_boundaries(&mut self, emitted_cut: u64) -> bool {
        let CheckpointPlan::Multi(sizes) = &self.checkpoint_plan else {
            return false;
        };
        for (boundary, &size) in self.next_boundaries[..self.next_boundary_count]
            .iter_mut()
            .zip(sizes.iter())
        {
            if *boundary == emitted_cut {
                let Some(next) = boundary.checked_add(size.get()) else {
                    return false;
                };
                *boundary = next;
            }
        }
        true
    }

    fn budget_collapse_reason(&self) -> Option<CheckpointCollapseReason> {
        let segment_count = self.segments.len().saturating_add(1);
        if segment_count > MAX_CHECKPOINT_SEGMENTS {
            Some(CheckpointCollapseReason::SegmentCount)
        } else if segment_count.saturating_mul(self.checkpoint_plan.grid_count())
            > MAX_CHECKPOINT_OFFER_WORK
        {
            Some(CheckpointCollapseReason::OfferWork)
        } else {
            None
        }
    }

    fn collapse(&mut self, reason: CheckpointCollapseReason) {
        self.collapsed = true;
        self.collapse_reason.get_or_insert(reason);
        self.until_boundary = None;
        self.next_boundary_count = 0;
        self.segments = Vec::new();
    }

    fn finish_segments(&mut self) -> Vec<Segment> {
        if self.collapsed {
            return (self.total_len > 0)
                .then_some(Segment {
                    file_offset: self.base_offset,
                    len: self.total_len,
                    crc32: self.current_crc(),
                })
                .into_iter()
                .collect();
        }
        std::mem::take(&mut self.segments)
    }
}

impl Default for SegmentedCrc32 {
    /// A pass over an article whose file offset is not yet known and with no
    /// block grid: one segment based at offset 0, i.e. exactly an unsegmented
    /// [`Crc32`] plus a byte counter.
    fn default() -> Self {
        Self::new(0, CheckpointPlan::None)
    }
}

/// Bytes from `offset` to the next multiple of `block_size` strictly after it.
#[inline]
fn bytes_to_next_boundary(offset: u64, block_size: NonZeroU64) -> u64 {
    let size = block_size.get();
    size - offset % size
}

fn next_boundary_after(offset: u64, block_size: NonZeroU64) -> Option<u64> {
    offset.checked_add(bytes_to_next_boundary(offset, block_size))
}

/// Fold segments that tile a contiguous file range into the single segment
/// covering that range.
///
/// Returns `None` if `segments` is empty or does not form a gapless,
/// non-overlapping, ascending tiling — a caller assembling a block from
/// several articles' segments uses that to detect an unclaimed block rather
/// than publishing a CRC derived from a broken tiling.
pub fn combine_contiguous(segments: &[Segment]) -> Option<Segment> {
    let (first, rest) = segments.split_first()?;
    let mut folded = *first;
    let mut memo: Option<(u64, Crc32Combine)> = None;
    for segment in rest {
        if segment.file_offset != folded.end_offset() {
            return None;
        }
        let op = match memo {
            Some((len, op)) if len == segment.len => op,
            _ => {
                let op = Crc32Combine::new(segment.len);
                memo = Some((segment.len, op));
                op
            }
        };
        folded.crc32 = op.combine(folded.crc32, segment.crc32);
        folded.len += segment.len;
    }
    Some(folded)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic xorshift64* stream: every test input and every random
    /// matrix below is a pure function of its seed.
    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next_u64() % bound
        }

        fn range(&mut self, low: u64, high: u64) -> u64 {
            low + self.below(high - low + 1)
        }
    }

    fn random_bytes(seed: u64, len: usize) -> Vec<u8> {
        let mut rng = Rng(seed | 1);
        (0..len).map(|_| (rng.next_u64() >> 33) as u8).collect()
    }

    fn direct(data: &[u8]) -> u32 {
        crc_fast::crc32_iso_hdlc(data)
    }

    fn single_plan(block_size: Option<NonZeroU64>) -> CheckpointPlan {
        block_size.map_or(CheckpointPlan::None, CheckpointPlan::Single)
    }

    /// Run one article's bytes through the checkpointing pass with a chunking
    /// schedule, returning the emitted segments.
    fn run_pass(
        data: &[u8],
        file_offset: u64,
        block_size: Option<NonZeroU64>,
        chunks: &[usize],
    ) -> Vec<Segment> {
        let mut pass = SegmentedCrc32::new(file_offset, single_plan(block_size));
        let mut cursor = 0usize;
        let mut idx = 0usize;
        while cursor < data.len() {
            let want = if chunks.is_empty() {
                data.len()
            } else {
                chunks[idx % chunks.len()].max(1)
            };
            let end = (cursor + want).min(data.len());
            pass.update(&data[cursor..end]);
            cursor = end;
            idx += 1;
        }
        pass.finish()
    }

    fn run_plan(
        data: &[u8],
        file_offset: u64,
        checkpoint_plan: CheckpointPlan,
        chunks: &[usize],
    ) -> Vec<Segment> {
        let mut pass = SegmentedCrc32::new(file_offset, checkpoint_plan);
        let mut cursor = 0usize;
        let mut chunk_index = 0usize;
        while cursor < data.len() {
            let want = chunks
                .get(chunk_index % chunks.len().max(1))
                .copied()
                .unwrap_or(data.len())
                .max(1);
            let end = (cursor + want).min(data.len());
            pass.update(&data[cursor..end]);
            cursor = end;
            chunk_index += 1;
        }
        pass.finish()
    }

    fn assert_full_blocks_match_direct(
        data: &[u8],
        file_offset: u64,
        segments: &[Segment],
        block_size: NonZeroU64,
    ) {
        let article_end = file_offset + data.len() as u64;
        let size = block_size.get();
        let mut block_start = file_offset.div_ceil(size) * size;
        while block_start
            .checked_add(size)
            .is_some_and(|end| end <= article_end)
        {
            let block_end = block_start + size;
            let block_segments: Vec<_> = segments
                .iter()
                .copied()
                .filter(|segment| {
                    segment.file_offset >= block_start && segment.end_offset() <= block_end
                })
                .collect();
            let combined = combine_contiguous(&block_segments).expect("refinement tiles block");
            assert_eq!(combined.file_offset, block_start);
            assert_eq!(combined.len, size);
            let start = usize::try_from(block_start - file_offset).expect("fixture-sized");
            let end = start + usize::try_from(size).expect("fixture-sized");
            assert_eq!(combined.crc32, direct(&data[start..end]));
            block_start += size;
        }
    }

    #[test]
    fn empty_pass_emits_no_segments() {
        let pass = SegmentedCrc32::new(0, single_plan(NonZeroU64::new(1024)));
        assert_eq!(pass.finish(), Vec::new());
    }

    #[test]
    fn empty_pass_reports_the_empty_crc() {
        let pass = SegmentedCrc32::new(4096, single_plan(NonZeroU64::new(1024)));
        assert!(pass.is_empty());
        assert_eq!(pass.current_crc(), direct(&[]));
        assert_eq!(pass.finish_article(), (direct(&[]), Vec::new()));
    }

    /// The incrementally folded whole-pass CRC — what the decoder reports as the
    /// article `pcrc32` — must equal the CRC of everything fed at every prefix,
    /// whatever the block grid. This is what makes checkpointing invisible to
    /// the article verdict: a segmented pass and an unsegmented one agree byte
    /// for byte, at every point, not just at the end.
    #[test]
    fn running_and_final_article_crc_equal_an_unsegmented_pass() {
        let data = random_bytes(0x9ec0, 20_000);
        for block_size in [1u64, 7, 255, 256, 257, 1024, 4096, 19_999, 20_000, 65_536] {
            let block_size = NonZeroU64::new(block_size).expect("non-zero");
            for offset in [0u64, 1, 255, 1000, 4096] {
                for schedule in [vec![1usize], vec![255, 1, 256], vec![4096, 7], vec![20_000]] {
                    let mut pass = SegmentedCrc32::new(offset, CheckpointPlan::Single(block_size));
                    let mut fed = 0usize;
                    let mut idx = 0usize;
                    while fed < data.len() {
                        let end = (fed + schedule[idx % schedule.len()]).min(data.len());
                        pass.update(&data[fed..end]);
                        fed = end;
                        idx += 1;
                        assert_eq!(
                            pass.current_crc(),
                            direct(&data[..fed]),
                            "block {block_size} offset {offset} prefix {fed}"
                        );
                        assert_eq!(pass.len(), fed as u64);
                        assert_eq!(pass.cursor(), offset + fed as u64);
                    }
                    assert_eq!(pass.base_offset(), offset);
                    assert_eq!(pass.checkpoint_plan(), &CheckpointPlan::Single(block_size));

                    let (article_crc, segments) = pass.finish_article();
                    assert_eq!(article_crc, direct(&data));
                    // And the folded form agrees with the incremental one, so
                    // the O(1) accumulator cannot drift from the segment list
                    // it is supposed to summarise.
                    assert_eq!(
                        combine_contiguous(&segments).expect("segments tile").crc32,
                        article_crc,
                        "block {block_size} offset {offset}"
                    );
                }
            }
        }
    }

    #[test]
    fn segments_tile_the_article_at_block_boundaries() {
        let data = random_bytes(0xa11ce, 5000);
        // Article starts 100 bytes into a 1024-byte block grid: the first
        // segment is short, the rest are block-sized until the tail.
        let segments = run_pass(&data, 1024 * 3 + 100, NonZeroU64::new(1024), &[97]);

        let offsets: Vec<(u64, u64)> = segments.iter().map(|s| (s.file_offset, s.len)).collect();
        assert_eq!(
            offsets,
            vec![
                (3172, 924),
                (4096, 1024),
                (5120, 1024),
                (6144, 1024),
                (7168, 1004),
            ]
        );
        for segment in &segments {
            let start = (segment.file_offset - 3172) as usize;
            let end = start + segment.len as usize;
            assert_eq!(segment.crc32, direct(&data[start..end]));
        }
    }

    #[test]
    fn no_block_size_emits_one_segment_per_article() {
        let data = random_bytes(0xb0b, 4096);
        let segments = run_pass(&data, 777, None, &[13, 900, 1]);
        assert_eq!(
            segments,
            vec![Segment {
                file_offset: 777,
                len: 4096,
                crc32: direct(&data),
            }]
        );
    }

    #[test]
    fn combine_contiguous_rejects_gaps_and_overlaps() {
        let a = Segment {
            file_offset: 0,
            len: 10,
            crc32: 0x1111_1111,
        };
        let gap = Segment {
            file_offset: 11,
            len: 10,
            crc32: 0x2222_2222,
        };
        let overlap = Segment {
            file_offset: 9,
            len: 10,
            crc32: 0x3333_3333,
        };
        assert_eq!(combine_contiguous(&[]), None);
        assert_eq!(combine_contiguous(&[a]), Some(a));
        assert_eq!(combine_contiguous(&[a, gap]), None);
        assert_eq!(combine_contiguous(&[a, overlap]), None);
    }

    #[test]
    fn zero_length_combine_is_the_identity() {
        let data = random_bytes(0xfeed, 512);
        let crc = direct(&data);
        assert_eq!(crc32_combine(crc, direct(&[]), 0), crc);
        assert_eq!(crc32_combine(direct(&[]), crc, data.len() as u64), crc);
    }

    /// Gate 1 + gate 2, exhaustively over a seeded random matrix: random file
    /// bytes x random block sizes x random article tilings (articles that
    /// straddle 0..3 boundaries, first/last block, short final block) x random
    /// chunk schedules.
    ///
    /// Every article's derived pcrc32 must equal its direct whole-article CRC,
    /// and every block's CRC derived from the segments tiling it must equal the
    /// direct CRC over that block's bytes.
    #[test]
    fn derived_block_and_article_crcs_match_direct_over_random_matrix() {
        let mut cases = 0usize;
        let mut blocks_checked = 0usize;
        let mut articles_checked = 0usize;
        let mut straddle_histogram = [0usize; 8];

        for seed in 0..64u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            // Block sizes deliberately include values far below and above the
            // article sizes so articles straddle zero, one, two and three-plus
            // boundaries within the same matrix.
            let block_size = match seed % 8 {
                0 => 1,
                1 => 7,
                2 => 64,
                3 => 256,
                4 => 1000,
                5 => 1024,
                6 => rng.range(2, 4096),
                _ => rng.range(2, 300),
            };
            let block_size = NonZeroU64::new(block_size).expect("non-zero");

            let file_len = rng.range(1, 20_000) as usize;
            let file = random_bytes(seed ^ 0xdead_beef, file_len);

            // Random article tiling of the whole file, so the first and last
            // blocks and the short final block are always covered.
            let mut article_bounds = vec![0usize];
            while *article_bounds.last().expect("seeded") < file_len {
                let start = *article_bounds.last().expect("seeded");
                let step = rng.range(1, 3000) as usize;
                article_bounds.push((start + step).min(file_len));
            }

            // Chunk schedule: how the driver hands decoded bytes to the pass.
            let chunks: Vec<usize> = (0..rng.range(1, 5))
                .map(|_| rng.range(1, 700) as usize)
                .collect();

            // Some articles decode before the block size is known.
            let pre_blocksize = rng.below(4) == 0;

            let mut all_segments: Vec<Segment> = Vec::new();
            for window in article_bounds.windows(2) {
                let (start, end) = (window[0], window[1]);
                let offset = start as u64;
                let article = &file[start..end];
                let known = !(pre_blocksize && rng.below(3) == 0);
                let segments = run_pass(
                    article,
                    offset,
                    known.then_some(block_size),
                    chunks.as_slice(),
                );

                // Gate 2: derived article pcrc32 == direct whole-article CRC.
                let derived = combine_contiguous(&segments).expect("article segments tile");
                assert_eq!(
                    derived.crc32,
                    direct(article),
                    "seed {seed} article [{start},{end}) block {block_size} derived pcrc32"
                );
                assert_eq!(derived.file_offset, offset);
                assert_eq!(derived.len, (end - start) as u64);

                if known {
                    let boundaries = segments.len() - 1;
                    straddle_histogram[boundaries.min(7)] += 1;
                }
                all_segments.extend_from_slice(&segments);
                articles_checked += 1;
            }

            // Segments must tile the file end to end before any block is
            // derived from them.
            let whole = combine_contiguous(&all_segments).expect("file segments tile");
            assert_eq!(whole.crc32, direct(&file), "seed {seed} whole-file fold");

            // Gate 1: every block whose segments tile it exactly.
            let size = block_size.get();
            let block_count = (file_len as u64).div_ceil(size);
            for block in 0..block_count {
                let start = block * size;
                let end = (start + size).min(file_len as u64);
                let tiling: Vec<Segment> = all_segments
                    .iter()
                    .copied()
                    .filter(|s| s.file_offset >= start && s.end_offset() <= end)
                    .collect();
                let Some(folded) = combine_contiguous(&tiling) else {
                    continue;
                };
                if folded.file_offset != start || folded.end_offset() != end {
                    // Boundary orphan: a pre-block-size article's segment
                    // crosses into this block, so it has no in-stream verdict.
                    continue;
                }
                assert_eq!(
                    folded.crc32,
                    direct(&file[start as usize..end as usize]),
                    "seed {seed} block {block} [{start},{end}) block size {block_size}"
                );
                blocks_checked += 1;
            }
            cases += 1;
        }

        // Non-vacuity: the matrix actually exercised the straddle classes the
        // gate names, rather than passing because nothing was checked. Printed
        // as well as asserted so a run's coverage is legible in the log instead
        // of having to be inferred from a green tick.
        eprintln!(
            "derived_block_and_article_crcs: {cases} cases, {articles_checked} articles, \
             {blocks_checked} blocks, articles by boundaries straddled {straddle_histogram:?}"
        );
        assert_eq!(cases, 64);
        assert!(
            articles_checked > 200,
            "articles checked {articles_checked}"
        );
        assert!(blocks_checked > 500, "blocks checked {blocks_checked}");
        for straddled in 0..4usize {
            assert!(
                straddle_histogram[straddled] > 0,
                "no article straddling {straddled} boundaries; histogram {straddle_histogram:?}"
            );
        }
    }

    /// Checkpoint placement is a function of file offsets only: the same
    /// article fed with wildly different chunk schedules must produce
    /// byte-identical segment records.
    #[test]
    fn segments_are_invariant_to_chunking() {
        for seed in 0..24u64 {
            let mut rng = Rng(seed | 1);
            let len = rng.range(1, 9000) as usize;
            let data = random_bytes(seed ^ 0x5eed, len);
            let offset = rng.range(0, 5000);
            let block_size = NonZeroU64::new(rng.range(1, 2048));

            let reference = run_pass(&data, offset, block_size, &[]);
            for schedule in [
                vec![1usize],
                vec![2],
                vec![3, 1, 255, 256, 257],
                vec![64],
                vec![255],
                vec![256],
                vec![1000, 1],
                vec![4096, 7],
                vec![len.max(1)],
            ] {
                assert_eq!(
                    run_pass(&data, offset, block_size, &schedule),
                    reference,
                    "seed {seed} schedule {schedule:?}"
                );
            }
        }
    }

    /// Every split point of a small article, at every offset within a small
    /// block grid — the dense counterpart to the random sweep above.
    #[test]
    fn every_split_point_and_offset_agrees_with_direct() {
        let data = random_bytes(0xc0ffee, 300);
        for block_size in [1u64, 2, 3, 7, 16, 64, 128, 299, 300, 301, 4096] {
            let block_size = NonZeroU64::new(block_size).expect("non-zero");
            for offset in [0u64, 1, 5, 63, 64, 65, 127, 300, 4095] {
                let reference = run_pass(&data, offset, Some(block_size), &[]);
                assert_eq!(
                    combine_contiguous(&reference).expect("tiles").crc32,
                    direct(&data),
                    "block {block_size} offset {offset}"
                );
                for split in 0..=data.len() {
                    let mut pass = SegmentedCrc32::new(offset, CheckpointPlan::Single(block_size));
                    pass.update(&data[..split]);
                    pass.update(&data[split..]);
                    assert_eq!(
                        pass.finish(),
                        reference,
                        "block {block_size} offset {offset} split {split}"
                    );
                }
            }
        }
    }

    /// A checkpoint must be able to cut a segment while the x86 folded-streak
    /// path is carrying state, and while it is not. Sizes straddle the
    /// `VPCLMUL_MIN_UPDATE` threshold in both directions so the cut lands on a
    /// pending folded streak, on a digest-path streak, and on the hand-off.
    #[test]
    fn checkpoint_cuts_across_folded_streak_states() {
        let data = random_bytes(0x51ea4, 64 * 1024);
        let schedules: [&[usize]; 6] = [
            &[255, 1, 256, 300, 4096, 7],
            &[4096, 4096, 1],
            &[256, 256, 256],
            &[1, 1, 1, 4096, 1],
            &[300, 255, 256, 257],
            &[8192, 3, 8192],
        ];
        for block_size in [1u64, 255, 256, 257, 1024, 4096, 12_345] {
            let block_size = NonZeroU64::new(block_size).expect("non-zero");
            for schedule in schedules {
                let total: usize = schedule.iter().sum::<usize>().min(data.len());
                let article = &data[..total];
                let segments = run_pass(article, 0, Some(block_size), schedule);
                for segment in &segments {
                    let start = segment.file_offset as usize;
                    let end = segment.end_offset() as usize;
                    assert_eq!(
                        segment.crc32,
                        direct(&article[start..end]),
                        "block {block_size} schedule {schedule:?} segment [{start},{end})"
                    );
                }
                assert_eq!(
                    combine_contiguous(&segments).expect("tiles").crc32,
                    direct(article),
                    "block {block_size} schedule {schedule:?}"
                );
            }
        }
    }

    #[test]
    fn checkpoint_plan_normalizes_and_bounds_distinct_grids() {
        let single = CheckpointPlan::from_sizes([
            NonZeroU64::new(96).expect("non-zero"),
            NonZeroU64::new(96).expect("non-zero"),
        ]);
        assert_eq!(single.degradation, None);
        assert_eq!(
            single.plan,
            CheckpointPlan::Single(NonZeroU64::new(96).unwrap())
        );

        let multi = CheckpointPlan::from_sizes([
            NonZeroU64::new(96).expect("non-zero"),
            NonZeroU64::new(64).expect("non-zero"),
            NonZeroU64::new(96).expect("non-zero"),
        ]);
        assert_eq!(
            multi.plan.sizes(),
            &[NonZeroU64::new(64).unwrap(), NonZeroU64::new(96).unwrap()]
        );
        assert!(matches!(multi.plan, CheckpointPlan::Multi(_)));

        let oversized = CheckpointPlan::from_sizes(
            (1..=u64::try_from(MAX_CHECKPOINT_GRIDS + 1).unwrap())
                .map(|size| NonZeroU64::new(size).unwrap()),
        );
        assert_eq!(oversized.plan, CheckpointPlan::None);
        assert_eq!(
            oversized.degradation,
            Some(CheckpointPlanDegradation::TooManyGrids)
        );
    }

    #[test]
    fn multi_plan_common_refinement_tiles_every_grid_across_chunking() {
        let grids = [
            NonZeroU64::new(64).unwrap(),
            NonZeroU64::new(96).unwrap(),
            NonZeroU64::new(160).unwrap(),
        ];
        let plan = CheckpointPlan::from_sizes(grids).plan;
        let data = random_bytes(0xc0_1100, 8_192);
        let file_offset = 37;
        let reference = run_plan(&data, file_offset, plan.clone(), &[data.len()]);
        for schedule in [&[1usize][..], &[7, 257, 3, 1024][..], &[4_096, 13][..]] {
            let actual = run_plan(&data, file_offset, plan.clone(), schedule);
            assert_eq!(actual, reference, "schedule {schedule:?}");
        }
        for grid in grids {
            assert_full_blocks_match_direct(&data, file_offset, &reference, grid);
        }
    }

    #[test]
    fn single_plan_matches_fixed_stride_output() {
        let data = random_bytes(0x51_061e, 10_003);
        let block_size = NonZeroU64::new(257).unwrap();
        let legacy = run_pass(&data, 19, Some(block_size), &[5, 997, 3, 1_024]);
        let single = run_plan(
            &data,
            19,
            CheckpointPlan::Single(block_size),
            &[5, 997, 3, 1_024],
        );
        assert_eq!(single, legacy);
    }

    #[test]
    fn checkpoint_budget_collapses_to_one_coarse_article_segment() {
        let data = random_bytes(0x00b0_d6e7, MAX_CHECKPOINT_SEGMENTS + 1);
        let segments = run_plan(
            &data,
            0,
            CheckpointPlan::Single(NonZeroU64::new(1).unwrap()),
            &[data.len()],
        );
        assert_eq!(
            segments,
            vec![Segment {
                file_offset: 0,
                len: data.len() as u64,
                crc32: direct(&data),
            }]
        );

        let work_plan = CheckpointPlan::from_sizes(
            (1..=u64::try_from(MAX_CHECKPOINT_GRIDS).unwrap())
                .map(|size| NonZeroU64::new(size).unwrap()),
        )
        .plan;
        let work_data = random_bytes(
            0x000f_fee0,
            MAX_CHECKPOINT_OFFER_WORK / MAX_CHECKPOINT_GRIDS + 1,
        );
        let work_segments = run_plan(&work_data, 0, work_plan, &[work_data.len()]);
        assert_eq!(work_segments.len(), 1);
        assert_eq!(work_segments[0].len, work_data.len() as u64);
        assert_eq!(work_segments[0].crc32, direct(&work_data));
    }

    #[test]
    fn public_oversized_multi_plan_collapses_before_the_cut_loop() {
        let plan = CheckpointPlan::Multi(Arc::from(
            (1..=u64::try_from(MAX_CHECKPOINT_GRIDS + 1).unwrap())
                .map(|size| NonZeroU64::new(size).unwrap())
                .collect::<Vec<_>>(),
        ));
        let data = random_bytes(0x00c0_ffee, 4_096);
        let mut pass = SegmentedCrc32::new(0, plan);

        // Construction rejects the direct public variant before `update`, so
        // the article cannot pay a per-byte multi-grid boundary scan.
        assert_eq!(
            pass.collapse_reason(),
            Some(CheckpointCollapseReason::OfferWork)
        );
        assert_eq!(pass.checkpoint_plan(), &CheckpointPlan::None);
        pass.update(&data);
        assert!(pass.segments().is_empty());

        let (crc, segments, returned_plan, reason) = pass.finish_article_with_reason_and_plan();
        assert_eq!(crc, direct(&data));
        assert_eq!(returned_plan, CheckpointPlan::None);
        assert_eq!(reason, Some(CheckpointCollapseReason::OfferWork));
        assert_eq!(
            segments,
            vec![Segment {
                file_offset: 0,
                len: data.len() as u64,
                crc32: direct(&data),
            }]
        );
    }

    #[test]
    fn checkpoint_collapse_reports_the_budget_that_forced_coarse_evidence() {
        let data = random_bytes(0x0b5e_cafe, MAX_CHECKPOINT_SEGMENTS + 1);
        let mut segment_limited =
            SegmentedCrc32::new(0, CheckpointPlan::Single(NonZeroU64::new(1).unwrap()));
        segment_limited.update(&data[..MAX_CHECKPOINT_SEGMENTS]);
        assert!(
            segment_limited.segments.capacity() * std::mem::size_of::<Segment>()
                <= MAX_CHECKPOINT_SEGMENT_BYTES
        );
        assert_eq!(segment_limited.collapse_reason(), None);
        segment_limited.update(&data[MAX_CHECKPOINT_SEGMENTS..]);
        let (_, segments, reason) = segment_limited.finish_article_with_reason();
        assert_eq!(reason, Some(CheckpointCollapseReason::SegmentCount));
        assert_eq!(segments.len(), 1);

        let work_plan = CheckpointPlan::from_sizes(
            (1..=u64::try_from(MAX_CHECKPOINT_GRIDS).unwrap())
                .map(|size| NonZeroU64::new(size).unwrap()),
        )
        .plan;
        let work_data = random_bytes(
            0x000f_fee1,
            MAX_CHECKPOINT_OFFER_WORK / MAX_CHECKPOINT_GRIDS + 1,
        );
        let mut work_limited = SegmentedCrc32::new(0, work_plan);
        work_limited.update(&work_data);
        let (_, segments, reason) = work_limited.finish_article_with_reason();
        assert_eq!(reason, Some(CheckpointCollapseReason::OfferWork));
        assert_eq!(segments.len(), 1);
    }

    #[test]
    fn address_overflow_degrades_without_panicking_or_wrapping() {
        let segments = run_plan(
            b"ab",
            u64::MAX - 1,
            CheckpointPlan::Single(NonZeroU64::new(1).unwrap()),
            &[1],
        );
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].file_offset, u64::MAX - 1);
        assert_eq!(segments[0].len, 2);
        assert_eq!(segments[0].crc32, direct(b"ab"));
    }

    /// The checkpoint primitive on the raw CRC state: a cut must restart from
    /// the CRC init state, not carry the closed segment's value forward.
    #[test]
    fn crc_checkpoint_restarts_from_init_state() {
        let data = random_bytes(0x1234_5678, 40_000);
        for cut in [0usize, 1, 255, 256, 257, 1024, 9999, 40_000] {
            let mut crc = Crc32::new();
            crc.update(&data[..cut]);
            assert_eq!(crc.checkpoint(), direct(&data[..cut]), "cut {cut}");
            crc.update(&data[cut..]);
            assert_eq!(crc.finalize(), direct(&data[cut..]), "tail after cut {cut}");
        }
    }
}
