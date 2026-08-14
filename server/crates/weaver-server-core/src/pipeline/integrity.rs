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

/// How many segments a file may hold for blocks that are not yet complete.
///
/// Segments are retired as soon as the block they belong to is closed, so this
/// only bounds genuinely out-of-order arrival. A file that exceeds it stops
/// claiming blocks and falls back to settle-time verification rather than
/// growing without limit.
const MAX_PENDING_SEGMENTS_PER_FILE: usize = 4096;

/// What in-stream verification concluded about one PAR2 block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockVerdict {
    /// The derived block CRC32 matched the recovery set's IFSC entry.
    Intact,
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
fn crc32_of_zeros(len: u64) -> u32 {
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

/// Per-file assembly of block CRC32s from decoded-article segments.
#[derive(Debug)]
struct FileBlockCrcs {
    block_size: NonZeroU64,
    /// Segments belonging to blocks that are not closed yet, keyed by file
    /// offset so a duplicate arrival replaces rather than duplicates.
    pending: BTreeMap<u64, Segment>,
    /// Derived CRC32 per zero-based block index.
    derived: BTreeMap<u32, u32>,
    /// Set once the file's length is known, which is what makes the short final
    /// block closable.
    file_len: Option<u64>,
    /// Pending grew past its bound; this file stops claiming blocks.
    overflowed: bool,
}

impl FileBlockCrcs {
    fn new(block_size: NonZeroU64) -> Self {
        Self {
            block_size,
            pending: BTreeMap::new(),
            derived: BTreeMap::new(),
            file_len: None,
            overflowed: false,
        }
    }

    fn block_bounds(&self, block_index: u32) -> Option<(u64, u64)> {
        let size = self.block_size.get();
        let start = u64::from(block_index).checked_mul(size)?;
        let end = start.checked_add(size)?;
        match self.file_len {
            Some(file_len) if end > file_len => (start < file_len).then_some((start, file_len)),
            Some(_) | None => Some((start, end)),
        }
    }

    /// Close every block the newly inserted segments completed, retiring their
    /// segments from `pending`.
    fn close_blocks(&mut self, first_block: u32, last_block: u32) {
        let size = self.block_size.get();
        for block_index in first_block..=last_block {
            if self.derived.contains_key(&block_index) {
                continue;
            }
            let Some((start, end)) = self.block_bounds(block_index) else {
                continue;
            };
            // The final block of a file whose length is not known yet cannot be
            // closed: its extent is undecided, so a tiling that reaches the last
            // arrived byte may or may not be the whole block.
            if self.file_len.is_none() && end - start < size {
                continue;
            }
            let tiling: Vec<Segment> = self
                .pending
                .range(start..end)
                .map(|(_, segment)| *segment)
                .collect();
            let Some(crc32) = fold_tiling(&tiling, start, end) else {
                continue;
            };
            self.derived.insert(block_index, crc32);
            for segment in tiling {
                self.pending.remove(&segment.file_offset);
            }
        }
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
        .filter(|(_, verdict)| **verdict == BlockVerdict::Intact)
        .filter_map(|(&block_index, _)| {
            // Real bytes the block covers: a whole block, or the short
            // remainder for the final one. PAR2's zero padding is not part of
            // the file, so it is not part of the covered length.
            let start = u64::from(block_index).checked_mul(block_size)?;
            let covered = file_length.checked_sub(start)?.min(block_size);
            let proof = par2_rs::InStreamCrc32Proof::try_new(covered, true, true, true).ok()?;
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
    pub(crate) fn note_article(
        &mut self,
        file_id: NzbFileId,
        block_size: NonZeroU64,
        file_offset: u64,
        len: u64,
        part_crc: u32,
        segments: &[Segment],
    ) {
        if len == 0 {
            return;
        }
        let end = file_offset.saturating_add(len);
        let entry = self
            .files
            .entry(file_id)
            .or_insert_with(|| FileBlockCrcs::new(block_size));
        if entry.block_size != block_size {
            // The recovery set's grid changed under us (a second set bound to
            // the same file). Everything derived on the old grid is meaningless.
            *entry = FileBlockCrcs::new(block_size);
        }
        if entry.overflowed {
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

        for segment in accepted {
            entry.pending.insert(segment.file_offset, *segment);
        }
        if entry.pending.len() > MAX_PENDING_SEGMENTS_PER_FILE {
            entry.overflowed = true;
            entry.pending.clear();
            return;
        }

        let size = block_size.get();
        let first_block = u32::try_from(file_offset / size).unwrap_or(u32::MAX);
        let last_block = u32::try_from((end - 1) / size).unwrap_or(u32::MAX);
        let before = entry.derived.len();
        entry.close_blocks(first_block, last_block);
        self.blocks_derived = self
            .blocks_derived
            .saturating_add((entry.derived.len() - before) as u64);
    }

    /// Declare the file's final length, which is what lets the short final block
    /// close. Idempotent.
    pub(crate) fn note_file_len(&mut self, file_id: NzbFileId, file_len: u64) {
        let Some(entry) = self.files.get_mut(&file_id) else {
            return;
        };
        if entry.file_len == Some(file_len) || entry.overflowed {
            return;
        }
        entry.file_len = Some(file_len);
        let size = entry.block_size.get();
        let last_block = u32::try_from(file_len.saturating_sub(1) / size).unwrap_or(u32::MAX);
        let before = entry.derived.len();
        entry.close_blocks(0, last_block);
        self.blocks_derived = self
            .blocks_derived
            .saturating_add((entry.derived.len() - before) as u64);
    }

    /// Derived CRC32 for one block, if in-stream evidence closed it.
    ///
    /// Production code compares verdicts, not raw CRCs; this is how the tests
    /// pin a derived value against a direct hash of the block's bytes.
    #[cfg(test)]
    pub(crate) fn derived_block_crc(&self, file_id: NzbFileId, block_index: u32) -> Option<u32> {
        self.files.get(&file_id)?.derived.get(&block_index).copied()
    }

    /// Every block this file has an in-stream CRC for, ascending.
    #[cfg(test)]
    pub(crate) fn derived_blocks(&self, file_id: NzbFileId) -> impl Iterator<Item = (u32, u32)> {
        self.files
            .get(&file_id)
            .into_iter()
            .flat_map(|entry| entry.derived.iter().map(|(index, crc)| (*index, *crc)))
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
        if par2_set.slice_size != entry.block_size.get() {
            // A verdict derived on one grid says nothing about another.
            return verdicts;
        }
        let checksums = par2_set.file_checksums(&par2_file_id);
        let block_size = entry.block_size.get();
        for (&block_index, &derived) in &entry.derived {
            let Some((start, end)) = entry.block_bounds(block_index) else {
                continue;
            };
            let Some(expected) = checksums
                .and_then(|checksums| checksums.get(block_index as usize))
                .map(|checksum| checksum.crc32)
            else {
                verdicts.insert(block_index, BlockVerdict::NoReference);
                continue;
            };
            let padding = block_size - (end - start);
            let padded = if padding == 0 {
                derived
            } else {
                par2_rs::checksum::Crc32CombineOp::new(padding)
                    .combine(derived, crc32_of_zeros(padding))
            };
            verdicts.insert(
                block_index,
                if padded == expected {
                    BlockVerdict::Intact
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
