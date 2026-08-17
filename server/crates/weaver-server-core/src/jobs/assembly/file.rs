use crate::jobs::ids::NzbFileId;
use bitvec::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use weaver_model::files::FileRole;

use super::error::AssemblyError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DetectedArchiveKind {
    Rar,
    SevenZipSingle,
    SevenZipSplit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectedArchiveIdentity {
    pub kind: DetectedArchiveKind,
    pub set_name: String,
    pub volume_index: Option<u32>,
}

impl DetectedArchiveIdentity {
    pub fn effective_role(&self) -> FileRole {
        match self.kind {
            DetectedArchiveKind::Rar => FileRole::RarVolume {
                volume_number: self.volume_index.unwrap_or(0),
            },
            DetectedArchiveKind::SevenZipSingle => FileRole::SevenZipArchive,
            DetectedArchiveKind::SevenZipSplit => FileRole::SevenZipSplit {
                number: self.volume_index.unwrap_or(0),
            },
        }
    }
}

impl DetectedArchiveKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Rar => "rar",
            Self::SevenZipSingle => "seven_zip_single",
            Self::SevenZipSplit => "seven_zip_split",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "rar" => Some(Self::Rar),
            "seven_zip_single" => Some(Self::SevenZipSingle),
            "seven_zip_split" => Some(Self::SevenZipSplit),
            _ => None,
        }
    }
}

/// Tracks the assembly state of a single NZB file.
pub struct FileAssembly {
    file_id: NzbFileId,
    filename: String,
    declared_role: FileRole,
    total_segments: u32,
    total_bytes: u64,
    /// Cumulative byte offsets: cumulative_offsets[i] = sum of segment_sizes[0..i].
    cumulative_offsets: Vec<u64>,

    /// Bitset tracking which segments (0-indexed) have been received.
    received: BitVec,
    /// Running byte count of received data.
    received_bytes: u64,
    /// Where each arrived segment was placed, keyed by ordinal.
    ///
    /// The NZB cannot supply decoded offsets (its sizes are yEnc-encoded), so
    /// placement comes from the article's own header. Recording it lets a later
    /// article be refused when it would sit outside the gap its ordinal owns,
    /// which is what stops a hostile server writing over bytes it already
    /// served correctly.
    ///
    /// Ordered by ordinal so the check is two range probes rather than a scan:
    /// this runs on the orchestrator thread for every decoded article, and a
    /// linear pass would cost O(segments) each time — hundreds of microseconds
    /// per article, and hundreds of KiB of memory traffic, on a large file.
    placements: BTreeMap<u32, (u64, u32)>,
    /// A repeated article leaves no reliable proof that all writes had a
    /// single, unambiguous source. Keep fast PAR2 evidence conservative.
    has_duplicate_segments: bool,
}

/// Result of committing a segment to assembly.
#[derive(Debug)]
pub struct CommitResult {
    /// Whether the file is now complete (all segments received).
    pub file_complete: bool,
    /// Whether this was a duplicate segment (already received).
    pub was_duplicate: bool,
}

impl FileAssembly {
    /// Create a new FileAssembly for tracking.
    /// segment_sizes: expected byte size for each segment (0-indexed).
    pub fn new(
        file_id: NzbFileId,
        filename: String,
        role: FileRole,
        segment_sizes: Vec<u32>,
    ) -> Self {
        let total_segments = segment_sizes.len() as u32;
        let total_bytes = segment_sizes.iter().map(|&s| s as u64).sum();

        let mut cumulative_offsets = Vec::with_capacity(total_segments as usize + 1);
        cumulative_offsets.push(0);
        let mut acc = 0u64;
        for &size in &segment_sizes {
            acc += size as u64;
            cumulative_offsets.push(acc);
        }

        Self {
            file_id,
            filename,
            declared_role: role,
            total_segments,
            total_bytes,
            cumulative_offsets,
            received: bitvec![0; total_segments as usize],
            received_bytes: 0,
            placements: BTreeMap::new(),
            has_duplicate_segments: false,
        }
    }

    /// The neighbouring segment this placement would run into, if any.
    ///
    /// Segments tile the file in ordinal order, so a placement is legitimate
    /// exactly when it starts at or after the nearest arrived lower ordinal
    /// ends, and ends at or before the nearest arrived higher ordinal starts.
    /// Every accepted placement therefore stays disjoint from all the others by
    /// induction, without comparing against any but its two neighbours.
    pub fn placement_conflict(&self, segment_number: u32, offset: u64, len: u32) -> Option<u32> {
        let end = offset.saturating_add(u64::from(len));
        if let Some((previous, (previous_offset, previous_len))) =
            self.placements.range(..segment_number).next_back()
            && offset < previous_offset.saturating_add(u64::from(*previous_len))
        {
            return Some(*previous);
        }
        if let Some((next, (next_offset, _))) = self
            .placements
            .range(segment_number.saturating_add(1)..)
            .next()
            && end > *next_offset
        {
            return Some(*next);
        }
        None
    }

    /// Record where a segment was placed. Re-recording the same ordinal is the
    /// ordinary duplicate/retry case and simply overwrites.
    pub fn record_placement(&mut self, segment_number: u32, offset: u64, len: u32) {
        self.placements.insert(segment_number, (offset, len));
    }

    /// Record that a segment has been received and decoded.
    pub fn commit_segment(
        &mut self,
        segment_number: u32,
        decoded_size: u32,
    ) -> Result<CommitResult, AssemblyError> {
        if segment_number >= self.total_segments {
            return Err(AssemblyError::SegmentOutOfRange {
                segment_number,
                total_segments: self.total_segments,
            });
        }

        // Check for duplicate.
        if self.received[segment_number as usize] {
            self.has_duplicate_segments = true;
            return Ok(CommitResult {
                file_complete: self.is_complete(),
                was_duplicate: true,
            });
        }

        // NOTE: decoded_size is never compared against the NZB-declared
        // segment size on purpose — declared sizes are yEnc-ENCODED (~3%
        // larger than decoded on every real post), so such a comparison is
        // not a corruption signal. Gap-free, overlap-free assembly is proven
        // from the recorded placements instead; see
        // `contiguous_placements_proven`.
        self.received.set(segment_number as usize, true);
        self.received_bytes += decoded_size as u64;

        Ok(CommitResult {
            file_complete: self.is_complete(),
            was_duplicate: false,
        })
    }

    pub fn reset(&mut self) {
        self.received.fill(false);
        self.received_bytes = 0;
        self.placements.clear();
        self.has_duplicate_segments = false;
    }

    pub fn mark_complete(&mut self) {
        self.received.fill(true);
        self.received_bytes = self.total_bytes;
    }

    /// Whether one specific segment has been received.
    ///
    /// Out-of-range segment numbers read as not received rather than panicking:
    /// callers iterate a spec, which can disagree with the assembly only if the
    /// job was rebuilt underneath them.
    pub fn has_segment(&self, segment_number: u32) -> bool {
        self.received
            .get(segment_number as usize)
            .is_some_and(|received| *received)
    }

    /// How many segments are still missing.
    pub fn missing_count(&self) -> u32 {
        self.total_segments - self.received.count_ones() as u32
    }

    /// Completion fraction (0.0 to 1.0).
    pub fn progress(&self) -> f64 {
        if self.total_segments == 0 {
            return 1.0;
        }
        self.received.count_ones() as f64 / self.total_segments as f64
    }

    /// Whether all segments have been received.
    pub fn is_complete(&self) -> bool {
        self.received.count_ones() == self.total_segments as usize
    }

    /// The file's role.
    pub fn role(&self) -> &FileRole {
        &self.declared_role
    }

    pub fn declared_role(&self) -> &FileRole {
        &self.declared_role
    }

    pub fn effective_role(&self) -> FileRole {
        self.declared_role.clone()
    }

    pub fn archive_set_name(&self) -> Option<String> {
        weaver_model::files::archive_base_name(&self.filename, &self.declared_role)
    }

    /// The filename.
    pub fn filename(&self) -> &str {
        &self.filename
    }

    /// The file id.
    pub fn file_id(&self) -> NzbFileId {
        self.file_id
    }

    /// The byte offset within the target file where a given segment's data should be written.
    /// Segments are sequential: segment 0 starts at offset 0, segment 1 at segment_sizes[0], etc.
    pub fn segment_offset(&self, segment_number: u32) -> u64 {
        self.cumulative_offsets[segment_number as usize]
    }

    /// The trusted zero-based byte range for a segment.
    pub fn segment_bounds(&self, segment_number: u32) -> Option<(u64, u64)> {
        let index = segment_number as usize;
        Some((
            *self.cumulative_offsets.get(index)?,
            *self.cumulative_offsets.get(index.checked_add(1)?)?,
        ))
    }

    /// Total expected bytes for the file.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Total number of segments.
    pub fn total_segments(&self) -> u32 {
        self.total_segments
    }

    /// Received bytes so far.
    pub fn received_bytes(&self) -> u64 {
        self.received_bytes
    }

    /// Whether the assembled file observed any duplicate article.
    pub fn has_duplicate_segments(&self) -> bool {
        self.has_duplicate_segments
    }

    /// Whether the recorded placements prove a gap-free, overlap-free
    /// decoded tiling of `[0, received_bytes())`.
    ///
    /// Placements are recorded from each accepted article's own bounded
    /// header before its write, and `placement_conflict` refuses overlaps on
    /// the way in, so a complete file whose placements start at zero, abut
    /// exactly in ordinal order, and sum to the decoded total was assembled
    /// with no gap and no overlap. Files completed by verification or repair
    /// rather than by decode have no such observations and prove nothing
    /// here — deliberately: this proof licenses whole-file CRC evidence, and
    /// only the decode path measured what it wrote.
    pub fn contiguous_placements_proven(&self) -> bool {
        if !self.is_complete() || self.placements.len() != self.total_segments as usize {
            return false;
        }
        let mut cursor = 0u64;
        for (offset, len) in self.placements.values() {
            if *offset != cursor {
                return false;
            }
            cursor = cursor.saturating_add(u64::from(*len));
        }
        cursor == self.received_bytes
    }
}

#[cfg(test)]
mod tests;
