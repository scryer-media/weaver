//! In-stream PAR2 block verification from the decode pass's CRC segments.
//!
//! # Verification policy
//!
//! Payload integrity on the download path is established by two independent,
//! differently aligned CRC32 passes over the same bytes, both produced by the
//! single CRC pass the decoder already runs:
//!
//! - the yEnc `pcrc32`, aligned to article boundaries and compared against the
//!   value the poster wrote into `=yend`; and
//! - the PAR2 IFSC block CRC32, aligned to the recovery set's block grid and
//!   compared against the value the recovery set's creator wrote into the IFSC
//!   packet.
//!
//! The two grids are cut by unrelated parties at unrelated offsets, so a span
//! that satisfies both was seen intact by both. Spans that satisfy only one —
//! blocks straddling an article that arrived before the block size was known,
//! blocks whose segments never tiled — get no in-stream verdict and are
//! *unclaimed*: they fall through to settle-time verification, which reads them
//! back and hashes them exactly as before, MD5 included. Repair, when it runs,
//! recomputes everything from scratch regardless of what was claimed here.
//!
//! CRC32 is not a cryptographic hash and a determined poster could forge one.
//! That is not a property this policy relies on or claims: NNTP articles carry
//! no authenticity of any kind, so a poster who wants to serve chosen bytes
//! simply serves them and writes matching checksums in the headers they also
//! author. What in-stream verification detects is corruption — truncation,
//! substitution and transport damage — which is what the checksums exist for.
//!
//! # Mechanism
//!
//! The decoder emits [`weaver_yenc::Segment`] records cut at block boundaries
//! (see `weaver_yenc::segment`). This module assembles the segments tiling each
//! block — which may come from several articles — into that block's CRC32, and
//! compares it against the recovery set's IFSC entry. Segment composition uses
//! [`par2_rs::checksum::Crc32CombineOp`], the same combine the pipeline's
//! part-CRC to file-CRC composition already runs, so block CRCs and file CRCs
//! are derived by one implementation.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;

use weaver_yenc::Segment;

use crate::jobs::ids::NzbFileId;

/// Maximum incomplete per-grid runs retained for one file.
///
/// A run is eagerly folded inside one PAR2 block, so normal ordered traffic
/// needs at most one entry per grid even when a fine checkpoint plan produces
/// many cuts in the block.
const MAX_PENDING_RUNS_PER_FILE: usize = 4096;
/// Maximum closed block claims retained for one file across all PAR2 grids.
const MAX_DERIVED_BLOCKS_PER_FILE: usize = 16_384;
/// Maximum incomplete per-grid runs retained for one job.
const MAX_PENDING_RUNS_PER_JOB: usize = 16_384;
/// Maximum closed block claims retained for one job across all files/grids.
const MAX_DERIVED_BLOCKS_PER_JOB: usize = 65_536;

/// What in-stream verification concluded about one PAR2 block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockVerdict {
    /// The derived block CRC32 matched the recovery set's IFSC entry.
    ///
    /// `independently_covered` is true only when every article that
    /// contributed bytes to this block also verified its declared yEnc
    /// `pcrc32` — the second, article-aligned grid. A block assembled from
    /// articles without declared (or with unverified) part CRCs still has a
    /// correct derived CRC32, but it was seen intact by one grid, not two,
    /// and must not mint an [`par2_rs::InStreamCrc32Proof`] claiming
    /// independent coverage.
    Intact { independently_covered: bool },
    /// The derived block CRC32 contradicted the IFSC entry: these bytes are
    /// damaged, and no read-back is needed to know it.
    Damaged,
    /// A block CRC was derived but the recovery set has no IFSC entry to
    /// compare it against, so nothing was verified.
    NoReference,
}

/// CRC32 of `len` zero bytes, by repeated doubling of the combine operator.
///
/// PAR2 computes the IFSC checksum of a short final slice over the slice
/// zero-padded up to the full block size, so a derived CRC over the file's real
/// bytes has to be padded the same way before the two can be compared. Doing it
/// by combining rather than by hashing keeps the cost logarithmic in the
/// padding length instead of linear.
pub(crate) fn crc32_of_zeros(len: u64) -> u32 {
    if len == 0 {
        return par2_rs::checksum::crc32(&[]);
    }
    // CRC32 of a single zero byte, then square the run length while consuming
    // the bits of `len`.
    let mut unit_len = 1u64;
    let mut unit_crc = par2_rs::checksum::crc32(&[0u8]);
    let mut acc: Option<(u32, u64)> = None;
    let mut remaining = len;
    while remaining > 0 {
        if remaining & 1 == 1 {
            acc = Some(match acc {
                Some((crc, acc_len)) => (
                    par2_rs::checksum::Crc32CombineOp::new(unit_len).combine(crc, unit_crc),
                    acc_len + unit_len,
                ),
                None => (unit_crc, unit_len),
            });
        }
        remaining >>= 1;
        if remaining > 0 {
            unit_crc = par2_rs::checksum::Crc32CombineOp::new(unit_len).combine(unit_crc, unit_crc);
            unit_len *= 2;
        }
    }
    acc.expect("len > 0 sets at least one bit").0
}

/// Fold segments that tile a contiguous range into that range's CRC32.
///
/// Returns `None` when the segments do not form a gapless, non-overlapping,
/// ascending tiling of `[start, end)` — the unclaimed-block signal. A
/// zero-length segment is never synthesised to bridge a gap: the composition
/// operator's zero-length case is the identity on its first argument, which
/// would silently absorb the second and turn a broken tiling into a confident
/// wrong answer.
fn fold_tiling(segments: &[Segment], start: u64, end: u64) -> Option<u32> {
    let mut cursor = start;
    let mut crc: Option<u32> = None;
    for segment in segments {
        if segment.file_offset != cursor || segment.len == 0 {
            return None;
        }
        crc = Some(match crc {
            Some(prefix) => {
                par2_rs::checksum::Crc32CombineOp::new(segment.len).combine(prefix, segment.crc32)
            }
            None => segment.crc32,
        });
        cursor = segment.end_offset();
        if cursor > end {
            return None;
        }
    }
    (cursor == end).then_some(crc).flatten()
}

/// One contiguous, in-block run plus the attestation of every article that
/// contributed to it.
#[derive(Debug, Clone, Copy)]
struct PendingRun {
    file_offset: u64,
    len: u64,
    crc32: u32,
    independently_covered: bool,
}

impl PendingRun {
    fn end_offset(self) -> Option<u64> {
        self.file_offset.checked_add(self.len)
    }
}

/// One closed block: the derived CRC32 plus whether every contributing
/// article carried a verified `pcrc32` (see [`BlockVerdict::Intact`]).
#[derive(Debug, Clone, Copy)]
struct DerivedBlock {
    crc32: u32,
    independently_covered: bool,
}

/// The change in retained incomplete runs after adding one observation.
#[derive(Debug, Clone, Copy, Default)]
struct PendingMutation {
    added: usize,
    removed: usize,
    adjacent_merges: usize,
}

/// One target PAR2 grid's incomplete and closed evidence for a file.
#[derive(Debug)]
struct GridAccumulator {
    block_size: NonZeroU64,
    /// Eagerly combined, non-overlapping runs, keyed by their file offset.
    ///
    /// A PAR2 block may accumulate several islands while articles arrive out
    /// of order. Keeping every island is required to derive the block when a
    /// later article bridges them; only adjacency lets us combine CRCs.
    pending: BTreeMap<u64, PendingRun>,
    /// Derived CRC32 (+ attestation) per zero-based block index.
    derived: BTreeMap<u32, DerivedBlock>,
}

impl GridAccumulator {
    fn new(block_size: NonZeroU64) -> Self {
        Self {
            block_size,
            pending: BTreeMap::new(),
            derived: BTreeMap::new(),
        }
    }

    fn block_bounds(&self, block_index: u32, file_len: Option<u64>) -> Option<(u64, u64)> {
        let size = self.block_size.get();
        let start = u64::from(block_index).checked_mul(size)?;
        let end = start.checked_add(size)?;
        match file_len {
            Some(file_len) if end > file_len => (start < file_len).then_some((start, file_len)),
            Some(_) | None => Some((start, end)),
        }
    }

    /// Add one segment when it stays inside this grid's one target block.
    /// Segments crossing a grid boundary cannot be split from their CRC alone,
    /// so they remain deliberately unclaimed for this grid.
    fn offer_segment(
        &mut self,
        segment: Segment,
        independently_covered: bool,
    ) -> Option<PendingMutation> {
        let end = segment.file_offset.checked_add(segment.len)?;
        if segment.len == 0 {
            return None;
        }
        let size = self.block_size.get();
        let block_index = u32::try_from(segment.file_offset / size).ok()?;
        if end.saturating_sub(1) / size != u64::from(block_index) {
            return None;
        }
        let block_start = u64::from(block_index).checked_mul(size)?;
        let block_end = block_start.checked_add(size)?;
        if self.derived.contains_key(&block_index) {
            return Some(PendingMutation::default());
        }

        let mut incoming = PendingRun {
            file_offset: segment.file_offset,
            len: segment.len,
            crc32: segment.crc32,
            independently_covered,
        };
        let mut removed = 0;
        let mut adjacent_merges = 0;
        let mut overlaps = Vec::new();
        for (&offset, existing) in self.pending.range(..end).rev() {
            let Some(existing_end) = existing.end_offset() else {
                overlaps.push(offset);
                continue;
            };
            if existing_end <= incoming.file_offset {
                break;
            }
            if existing.file_offset == incoming.file_offset && existing.len == incoming.len {
                // Replaying an identical observation cannot add coverage. A
                // missing pCRC still denies independent attestation.
                let existing = self.pending.get_mut(&offset)?;
                existing.independently_covered &= incoming.independently_covered;
                return Some(PendingMutation::default());
            }
            overlaps.push(offset);
        }
        for offset in overlaps {
            self.pending.remove(&offset);
            removed += 1;
        }

        if let Some((&offset, previous)) = self.pending.range(..incoming.file_offset).next_back()
            && previous.file_offset >= block_start
            && previous.end_offset() == Some(incoming.file_offset)
        {
            let previous = self.pending.remove(&offset)?;
            incoming.crc32 = par2_rs::checksum::Crc32CombineOp::new(incoming.len)
                .combine(previous.crc32, incoming.crc32);
            incoming.file_offset = previous.file_offset;
            incoming.len = previous.len.checked_add(incoming.len)?;
            incoming.independently_covered &= previous.independently_covered;
            removed += 1;
            adjacent_merges += 1;
        }
        while incoming.end_offset()? < block_end {
            let Some(next) = self.pending.get(&incoming.end_offset()?).copied() else {
                break;
            };
            if next.end_offset()? > block_end {
                break;
            }
            self.pending.remove(&next.file_offset);
            incoming.crc32 = par2_rs::checksum::Crc32CombineOp::new(next.len)
                .combine(incoming.crc32, next.crc32);
            incoming.len = incoming.len.checked_add(next.len)?;
            incoming.independently_covered &= next.independently_covered;
            removed += 1;
            adjacent_merges += 1;
        }

        self.pending.insert(incoming.file_offset, incoming);
        Some(PendingMutation {
            added: 1,
            removed,
            adjacent_merges,
        })
    }

    /// Retire one exact in-block run into a derived block, if it closes now.
    fn take_closed_block(
        &mut self,
        block_index: u32,
        file_len: Option<u64>,
    ) -> Option<DerivedBlock> {
        if self.derived.contains_key(&block_index) {
            return None;
        }
        let (start, end) = self.block_bounds(block_index, file_len)?;
        let run = *self.pending.get(&start)?;
        (run.file_offset == start && run.len == end - start).then(|| {
            self.pending.remove(&start);
            DerivedBlock {
                crc32: run.crc32,
                independently_covered: run.independently_covered,
            }
        })
    }

    fn clear_pending(&mut self) -> usize {
        let count = self.pending.len();
        self.pending.clear();
        count
    }

    /// Close the ordinary one-segment block without first round-tripping it
    /// through the pending-run map. Existing partial evidence deliberately
    /// takes the general path, which reconciles its accounting and attestation.
    fn take_direct_block(
        &mut self,
        segment: Segment,
        independently_covered: bool,
        file_len: Option<u64>,
    ) -> Option<(u32, DerivedBlock)> {
        let block_index = u32::try_from(segment.file_offset / self.block_size.get()).ok()?;
        let (start, end) = self.block_bounds(block_index, file_len)?;
        (segment.file_offset == start
            && segment.len == end.checked_sub(start)?
            && !self.derived.contains_key(&block_index)
            && self.pending.range(start..end).next().is_none())
        .then_some((
            block_index,
            DerivedBlock {
                crc32: segment.crc32,
                independently_covered,
            },
        ))
    }
}

/// Per-file evidence for every grid the decoded batch actually knew about.
#[derive(Debug, Default)]
struct FileBlockCrcs {
    grids: BTreeMap<u64, GridAccumulator>,
    file_len: Option<u64>,
    pending_entries: usize,
    derived_entries: usize,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
struct EntryCounts {
    pending: usize,
    derived: usize,
}

/// Completion-side accounting for one article's collector work. The counters
/// are plain integers in the collector, then emitted only when the existing
/// hot-path profiler is enabled; normal article processing takes no metric
/// locks or clock reads.
#[derive(Debug, Default)]
struct CollectorArticleAccounting {
    adjacent_merges: u64,
    pending_budget_pressure_events: u64,
    pending_runs_dropped: u64,
    derived_budget_drops: u64,
    derived_blocks_preserved: u64,
}

fn record_collector_article_observability(
    entry: &FileBlockCrcs,
    job: EntryCounts,
    accounting: &CollectorArticleAccounting,
) {
    if !crate::runtime::perf_probe::enabled() {
        return;
    }

    crate::runtime::perf_probe::record_value(
        "par2.collector.pending_runs.file",
        entry.pending_entries as u64,
    );
    crate::runtime::perf_probe::record_value("par2.collector.pending_runs.job", job.pending as u64);
    for grid in entry.grids.values() {
        crate::runtime::perf_probe::record_value(
            "par2.collector.pending_runs.per_grid",
            grid.pending.len() as u64,
        );
        crate::runtime::perf_probe::record_value(
            "par2.collector.derived_blocks.per_grid",
            grid.derived.len() as u64,
        );
    }
    if accounting.adjacent_merges > 0 {
        crate::runtime::perf_probe::record_value(
            "par2.collector.pending_runs.adjacent_merges",
            accounting.adjacent_merges,
        );
    }
    if accounting.pending_budget_pressure_events > 0 {
        crate::runtime::perf_probe::record_value(
            "par2.collector.pending_budget_pressure.events",
            accounting.pending_budget_pressure_events,
        );
        crate::runtime::perf_probe::record_value(
            "par2.collector.pending_budget_pressure.runs_dropped",
            accounting.pending_runs_dropped,
        );
        crate::runtime::perf_probe::record_value(
            "par2.collector.pending_budget_pressure.derived_preserved",
            accounting.derived_blocks_preserved,
        );
    }
    if accounting.derived_budget_drops > 0 {
        crate::runtime::perf_probe::record_value(
            "par2.collector.derived_budget_pressure.blocks_dropped",
            accounting.derived_budget_drops,
        );
    }
}

impl FileBlockCrcs {
    fn grid_mut(&mut self, block_size: NonZeroU64) -> &mut GridAccumulator {
        self.grids
            .entry(block_size.get())
            .or_insert_with(|| GridAccumulator::new(block_size))
    }
}

/// Turn one file's in-stream block verdicts into PAR2 slice evidence a repair
/// session can be seeded with.
///
/// Only *intact* verdicts become evidence, and the omission is load-bearing.
/// Seeding a contradiction invalidates the *source* the verdict names, and a
/// source served by a handle is named only by file identity — so one damaged
/// block would retire that file's other seeds along with it. A damaged block is
/// instead left unclaimed, exactly as a block with no verdict is: settle-time
/// verification reads it back, and the authoritative pass sees the evidence it
/// always did about bytes that are actually wrong.
///
/// The attestation attached to each verdict is what makes a CRC32-only verdict
/// admissible at all. It asserts that the block CRC32 covered the block's whole
/// extent, over bytes already made durable, and that the same span carries the
/// article-aligned yEnc `pcrc32` on an unrelated grid. It does not assert slice
/// identity: repair still re-derives CRC32 and MD5 over every byte it consumes.
pub(crate) fn slice_evidence_from_verdicts(
    recovery_set_id: par2_rs::RecoverySetId,
    par2_file_id: par2_rs::FileId,
    file_length: u64,
    block_size: u64,
    verdicts: &BTreeMap<u32, BlockVerdict>,
) -> Vec<par2_rs::SliceEvidence> {
    if block_size == 0 {
        return Vec::new();
    }
    verdicts
        .iter()
        .filter_map(|(&block_index, verdict)| {
            let BlockVerdict::Intact {
                independently_covered,
            } = *verdict
            else {
                return None;
            };
            // Real bytes the block covers: a whole block, or the short
            // remainder for the final one. PAR2's zero padding is not part of
            // the file, so it is not part of the covered length.
            let start = u64::from(block_index).checked_mul(block_size)?;
            let covered = file_length.checked_sub(start)?.min(block_size);
            // `try_new` rejects a proof without independent coverage, so a
            // block assembled from articles that never verified a declared
            // `pcrc32` mints no evidence and stays with settle-time
            // verification — exactly like an unclaimed block.
            let proof =
                par2_rs::InStreamCrc32Proof::try_new(covered, true, true, independently_covered)
                    .ok()?;
            Some(par2_rs::SliceEvidence::from_in_stream_crc32(
                recovery_set_id,
                par2_file_id,
                block_index,
                true,
                proof,
            ))
        })
        .collect()
}

/// Assembles PAR2 block CRC32s per file from the decode pass's segments.
#[derive(Debug, Default)]
pub(crate) struct BlockCrcCollector {
    files: HashMap<NzbFileId, FileBlockCrcs>,
    /// Exact entry counts across each job. These are maintained on every
    /// insert/removal so limits never need a map walk in the article path.
    job_entries: HashMap<crate::jobs::ids::JobId, EntryCounts>,
    /// Blocks closed with a derived CRC, for observability.
    blocks_derived: u64,
    /// Articles whose segments did not describe the range the pipeline placed
    /// them at, and were reduced to a single whole-article record.
    rebased_articles: u64,
}

impl BlockCrcCollector {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record one decoded article's segments against the file it belongs to.
    ///
    /// `file_offset` and `len` are the pipeline's own placement of the article,
    /// which is authoritative over the poster's `=ypart begin`; `part_crc` is
    /// the article's verified CRC32. Segments that do not tile exactly the range
    /// the pipeline placed are discarded in favour of one whole-article record,
    /// which is always true even when the poster's offsets are not — the article
    /// then composes only where its own boundaries tile a block, which is the
    /// same position an article decoded before the block size was known is in.
    // One argument per fact of the wire observation (placement, length, pCRC
    // value + whether it was verified, whether the assembly already held this
    // ordinal, checkpoint segments); bundling them into a struct would only
    // rename the tuple.
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub(crate) fn note_article(
        &mut self,
        file_id: NzbFileId,
        block_size: NonZeroU64,
        file_offset: u64,
        len: u64,
        part_crc: u32,
        part_crc_verified: bool,
        was_duplicate: bool,
        segments: &[Segment],
    ) {
        self.note_article_on_grids(
            file_id,
            std::slice::from_ref(&block_size),
            file_offset,
            len,
            part_crc,
            part_crc_verified,
            was_duplicate,
            segments,
        );
    }

    /// Offer one decoded article to every grid in the immutable checkpoint-plan
    /// snapshot captured before that article was decoded. A grid learned later
    /// cannot consume these segments; claiming it would imply cuts the decoder
    /// never emitted.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn note_article_on_grids(
        &mut self,
        file_id: NzbFileId,
        grids: &[NonZeroU64],
        file_offset: u64,
        len: u64,
        part_crc: u32,
        part_crc_verified: bool,
        was_duplicate: bool,
        segments: &[Segment],
    ) {
        if len == 0 {
            return;
        }
        let Some(end) = file_offset.checked_add(len) else {
            return;
        };
        if grids.is_empty() {
            return;
        }

        let article_matches_placement = fold_tiling(segments, file_offset, end) == Some(part_crc);
        let whole_article = [Segment {
            file_offset,
            len,
            crc32: part_crc,
        }];
        let accepted: &[Segment] = if article_matches_placement {
            segments
        } else {
            self.rebased_articles = self.rebased_articles.saturating_add(1);
            &whole_article
        };

        let mut derived = 0u64;
        let mut accounting = CollectorArticleAccounting::default();
        let job_id = file_id.job_id;
        let (files, job_entries) = (&mut self.files, &mut self.job_entries);
        let entry = files.entry(file_id).or_default();
        let job = job_entries.entry(job_id).or_default();

        if was_duplicate {
            for grid in entry.grids.values_mut() {
                let size = grid.block_size.get();
                let first = u32::try_from(file_offset / size).unwrap_or(u32::MAX);
                let last = u32::try_from((end - 1) / size).unwrap_or(u32::MAX);
                let stale_pending: Vec<_> = grid
                    .pending
                    .iter()
                    .filter_map(|(&index, run)| {
                        (run.end_offset()
                            .is_some_and(|run_end| run_end > file_offset)
                            && run.file_offset < end)
                            .then_some(index)
                    })
                    .collect();
                for index in stale_pending {
                    if grid.pending.remove(&index).is_some() {
                        entry.pending_entries = entry.pending_entries.saturating_sub(1);
                        job.pending = job.pending.saturating_sub(1);
                    }
                }
                let stale_derived: Vec<_> = grid
                    .derived
                    .range(first..=last)
                    .map(|(&index, _)| index)
                    .collect();
                for index in stale_derived {
                    if grid.derived.remove(&index).is_some() {
                        entry.derived_entries = entry.derived_entries.saturating_sub(1);
                        job.derived = job.derived.saturating_sub(1);
                    }
                }
            }
        }

        for &block_size in grids {
            for segment in accepted {
                derived = derived.saturating_add(Self::offer_segment(
                    entry,
                    job,
                    block_size,
                    *segment,
                    part_crc_verified,
                    &mut accounting,
                ));
            }
        }
        record_collector_article_observability(entry, *job, &accounting);
        self.blocks_derived = self.blocks_derived.saturating_add(derived);
    }

    fn offer_segment(
        entry: &mut FileBlockCrcs,
        job: &mut EntryCounts,
        block_size: NonZeroU64,
        segment: Segment,
        independently_covered: bool,
        accounting: &mut CollectorArticleAccounting,
    ) -> u64 {
        let end = match segment.file_offset.checked_add(segment.len) {
            Some(end) if segment.len > 0 => end,
            _ => return 0,
        };
        let size = block_size.get();
        let Some(block_index) = u32::try_from(segment.file_offset / size).ok() else {
            return 0;
        };
        if end.saturating_sub(1) / size != u64::from(block_index) {
            return 0;
        }

        let file_len = entry.file_len;
        if let Some((block_index, closed)) =
            entry
                .grid_mut(block_size)
                .take_direct_block(segment, independently_covered, file_len)
        {
            if entry.derived_entries >= MAX_DERIVED_BLOCKS_PER_FILE
                || job.derived >= MAX_DERIVED_BLOCKS_PER_JOB
            {
                accounting.derived_budget_drops = accounting.derived_budget_drops.saturating_add(1);
                accounting.derived_blocks_preserved = accounting
                    .derived_blocks_preserved
                    .saturating_add(entry.derived_entries as u64);
                return 0;
            }
            entry
                .grid_mut(block_size)
                .derived
                .insert(block_index, closed);
            entry.derived_entries = entry.derived_entries.saturating_add(1);
            job.derived = job.derived.saturating_add(1);
            return 1;
        }

        if entry.pending_entries >= MAX_PENDING_RUNS_PER_FILE
            || job.pending >= MAX_PENDING_RUNS_PER_JOB
        {
            accounting.pending_budget_pressure_events =
                accounting.pending_budget_pressure_events.saturating_add(1);
            accounting.derived_blocks_preserved = accounting
                .derived_blocks_preserved
                .saturating_add(entry.derived_entries as u64);
            let cleared = entry.grid_mut(block_size).clear_pending();
            accounting.pending_runs_dropped = accounting
                .pending_runs_dropped
                .saturating_add(cleared as u64);
            entry.pending_entries = entry.pending_entries.saturating_sub(cleared);
            job.pending = job.pending.saturating_sub(cleared);
        }
        if entry.pending_entries >= MAX_PENDING_RUNS_PER_FILE
            || job.pending >= MAX_PENDING_RUNS_PER_JOB
        {
            return 0;
        }

        let mutation = entry
            .grid_mut(block_size)
            .offer_segment(segment, independently_covered);
        let Some(mutation) = mutation else {
            return 0;
        };
        accounting.adjacent_merges = accounting
            .adjacent_merges
            .saturating_add(mutation.adjacent_merges as u64);
        entry.pending_entries = entry
            .pending_entries
            .saturating_add(mutation.added)
            .saturating_sub(mutation.removed);
        job.pending = job
            .pending
            .saturating_add(mutation.added)
            .saturating_sub(mutation.removed);

        let file_len = entry.file_len;
        let closed = entry
            .grid_mut(block_size)
            .take_closed_block(block_index, file_len);
        let Some(closed) = closed else {
            return 0;
        };
        entry.pending_entries = entry.pending_entries.saturating_sub(1);
        job.pending = job.pending.saturating_sub(1);
        if entry.derived_entries >= MAX_DERIVED_BLOCKS_PER_FILE
            || job.derived >= MAX_DERIVED_BLOCKS_PER_JOB
        {
            accounting.derived_budget_drops = accounting.derived_budget_drops.saturating_add(1);
            accounting.derived_blocks_preserved = accounting
                .derived_blocks_preserved
                .saturating_add(entry.derived_entries as u64);
            return 0;
        }
        entry
            .grid_mut(block_size)
            .derived
            .insert(block_index, closed);
        entry.derived_entries = entry.derived_entries.saturating_add(1);
        job.derived = job.derived.saturating_add(1);
        1
    }

    /// Declare the file's final length, which is what lets the short final block
    /// close. Repeating the same length is idempotent; a conflicting extent
    /// invalidates the file's retained evidence so the read paths can decide.
    pub(crate) fn note_file_len(&mut self, file_id: NzbFileId, file_len: u64) {
        let known_len = self.files.get(&file_id).and_then(|entry| entry.file_len);
        if file_len == 0 || known_len.is_some_and(|known| known != file_len) {
            self.forget_file(file_id);
            return;
        }
        if known_len == Some(file_len) {
            return;
        }
        let (files, job_entries) = (&mut self.files, &mut self.job_entries);
        let Some(entry) = files.get_mut(&file_id) else {
            return;
        };
        entry.file_len = Some(file_len);
        let Some(job) = job_entries.get_mut(&file_id.job_id) else {
            return;
        };
        let last_blocks: Vec<_> = entry
            .grids
            .iter()
            .filter_map(|(&size, _)| {
                u32::try_from((file_len - 1) / size)
                    .ok()
                    .map(|last| (size, last))
            })
            .collect();
        let mut derived = 0u64;
        let mut accounting = CollectorArticleAccounting::default();
        for (size, block_index) in last_blocks {
            let Some(block_size) = NonZeroU64::new(size) else {
                continue;
            };
            let file_len = entry.file_len;
            let closed = entry
                .grid_mut(block_size)
                .take_closed_block(block_index, file_len);
            let Some(closed) = closed else {
                continue;
            };
            entry.pending_entries = entry.pending_entries.saturating_sub(1);
            job.pending = job.pending.saturating_sub(1);
            if entry.derived_entries >= MAX_DERIVED_BLOCKS_PER_FILE
                || job.derived >= MAX_DERIVED_BLOCKS_PER_JOB
            {
                accounting.derived_budget_drops = accounting.derived_budget_drops.saturating_add(1);
                accounting.derived_blocks_preserved = accounting
                    .derived_blocks_preserved
                    .saturating_add(entry.derived_entries as u64);
                continue;
            }
            entry
                .grid_mut(block_size)
                .derived
                .insert(block_index, closed);
            entry.derived_entries = entry.derived_entries.saturating_add(1);
            job.derived = job.derived.saturating_add(1);
            derived = derived.saturating_add(1);
        }
        record_collector_article_observability(entry, *job, &accounting);
        self.blocks_derived = self.blocks_derived.saturating_add(derived);
    }

    /// Derived CRC32 for one block, if in-stream evidence closed it.
    ///
    /// Production code compares verdicts, not raw CRCs; this is how the tests
    /// pin a derived value against a direct hash of the block's bytes.
    #[cfg(test)]
    pub(crate) fn derived_block_crc(&self, file_id: NzbFileId, block_index: u32) -> Option<u32> {
        self.files
            .get(&file_id)?
            .grids
            .values()
            .next()?
            .derived
            .get(&block_index)
            .map(|block| block.crc32)
    }

    /// Every block this file has an in-stream CRC for, ascending.
    #[cfg(test)]
    pub(crate) fn derived_blocks(&self, file_id: NzbFileId) -> impl Iterator<Item = (u32, u32)> {
        self.files.get(&file_id).into_iter().flat_map(|entry| {
            entry
                .grids
                .values()
                .next()
                .into_iter()
                .flat_map(|grid| grid.derived.iter())
                .map(|(index, block)| (*index, block.crc32))
        })
    }

    #[cfg(test)]
    fn entry_counts_for_job(&self, job_id: crate::jobs::ids::JobId) -> EntryCounts {
        self.job_entries.get(&job_id).copied().unwrap_or_default()
    }

    /// Compare this file's derived block CRC32s against a recovery set's IFSC
    /// entries, block by block.
    ///
    /// The short final block is zero-padded to the full block size before the
    /// comparison, because that is what PAR2 checksums. Blocks with no derived
    /// CRC are absent from the result: they are unclaimed, and settle-time
    /// verification owns them.
    pub(crate) fn verdicts_against(
        &self,
        file_id: NzbFileId,
        par2_set: &par2_rs::Par2FileSet,
        par2_file_id: par2_rs::FileId,
    ) -> BTreeMap<u32, BlockVerdict> {
        let mut verdicts = BTreeMap::new();
        let Some(entry) = self.files.get(&file_id) else {
            return verdicts;
        };
        let Some(grid) = entry.grids.get(&par2_set.slice_size) else {
            // A batch that did not know this grid never produced a composable
            // segment belt for it.
            return verdicts;
        };
        let Some(description) = par2_set.file_description(&par2_file_id) else {
            // Length congruence below is what licenses the final short
            // block's zero-padded comparison; without the description there
            // is no length to be congruent with.
            return verdicts;
        };
        let checksums = par2_set.file_checksums(&par2_file_id);
        let block_size = grid.block_size.get();
        for (&block_index, derived) in &grid.derived {
            let Some((start, end)) = grid.block_bounds(block_index, entry.file_len) else {
                continue;
            };
            // Length congruence, per block. A full-size block within the
            // described extent compares 1:1 with its IFSC entry no matter
            // what length the collector believes. The short final block is
            // different: its verdict zero-pads from the extent the collector
            // closed it at, which is a statement about the described file
            // only when that extent IS the described length. A collector
            // whose length disagrees with the description must not have its
            // final block compared on the wrong padding basis — that block
            // stays unclaimed and the read paths own it.
            if end - start == block_size {
                if end > description.length {
                    // Beyond the described extent entirely: not a slice of
                    // this description. NoReference (below) covers the case
                    // where the checksum table simply ends; skipping here
                    // avoids ever comparing bytes the description disclaims.
                    continue;
                }
            } else if entry.file_len != Some(description.length) || end != description.length {
                continue;
            }
            let Some(expected) = checksums
                .and_then(|checksums| checksums.get(block_index as usize))
                .map(|checksum| checksum.crc32)
            else {
                verdicts.insert(block_index, BlockVerdict::NoReference);
                continue;
            };
            let padding = block_size - (end - start);
            let padded = if padding == 0 {
                derived.crc32
            } else {
                par2_rs::checksum::Crc32CombineOp::new(padding)
                    .combine(derived.crc32, crc32_of_zeros(padding))
            };
            verdicts.insert(
                block_index,
                if padded == expected {
                    BlockVerdict::Intact {
                        independently_covered: derived.independently_covered,
                    }
                } else {
                    BlockVerdict::Damaged
                },
            );
        }
        verdicts
    }

    /// Drop everything retained for every file of a job.
    pub(crate) fn forget_job(&mut self, job_id: crate::jobs::ids::JobId) {
        self.files.retain(|file_id, _| file_id.job_id != job_id);
        self.job_entries.remove(&job_id);
    }

    /// Drop everything retained for **one** file.
    ///
    /// The narrow twin of [`Self::forget_job`], for the transitions that retire
    /// a single file's evidence while the rest of the job's stands: a direct
    /// set's source volumes handed back to the conventional download path, whose
    /// bytes are about to be written again from scratch. Whatever the direct
    /// phase derived described a virtual volume assembled from member partials
    /// and envelopes; the conventional feeds that follow describe a real file,
    /// and merging the two would let evidence from one image adjudicate blocks
    /// of another.
    pub(crate) fn forget_file(&mut self, file_id: NzbFileId) {
        let Some(entry) = self.files.remove(&file_id) else {
            return;
        };
        let Some(job) = self.job_entries.get_mut(&file_id.job_id) else {
            return;
        };
        job.pending = job.pending.saturating_sub(entry.pending_entries);
        job.derived = job.derived.saturating_sub(entry.derived_entries);
        if job.pending == 0 && job.derived == 0 {
            self.job_entries.remove(&file_id.job_id);
        }
    }

    pub(crate) fn blocks_derived(&self) -> u64 {
        self.blocks_derived
    }

    pub(crate) fn rebased_articles(&self) -> u64 {
        self.rebased_articles
    }
}

#[cfg(test)]
mod tests;
