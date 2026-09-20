use crate::jobs::ids::NzbFileId;
use bitvec::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
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
    /// Durable reconstruction proves coverage, but supplies no decoded CRC atoms.
    reconstructed_placements: BTreeMap<u32, (u64, u32)>,
    /// A repeated article leaves no reliable proof that all writes had a
    /// single, unambiguous source. Keep fast PAR2 evidence conservative.
    has_duplicate_segments: bool,
    /// Conservative restart ceiling after a damaged write. No damaged state is
    /// persisted: a restart refetches from this ordinal until file verification
    /// or repair establishes completion.
    retained_damage_floor: Option<u64>,
    damaged_segments: BTreeSet<u32>,
    /// Existing durable prefix evidence, clipped whenever resumed bytes are rewritten.
    restored_prefix_end: u64,
    final_part_verified: bool,
    geometry_requires_verification: bool,
    // Outputs without NZB articles have independent availability and contribute
    // no declared or received download bytes.
    repair_output_ready: Option<bool>,
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
            reconstructed_placements: BTreeMap::new(),
            has_duplicate_segments: false,
            retained_damage_floor: None,
            damaged_segments: BTreeSet::new(),
            restored_prefix_end: 0,
            final_part_verified: false,
            geometry_requires_verification: false,
            repair_output_ready: None,
        }
    }

    /// A planned output reconstructed without any NZB articles.
    pub(crate) fn repair_output(file_id: NzbFileId, filename: String) -> Self {
        let role = FileRole::from_filename(&filename);
        let mut file = Self::new(file_id, filename, role, Vec::new());
        file.repair_output_ready = Some(false);
        file
    }

    pub(crate) fn is_repair_output(&self) -> bool {
        self.repair_output_ready.is_some()
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
        for placements in [&self.placements, &self.reconstructed_placements] {
            if let Some((previous, (previous_offset, previous_len))) =
                placements.range(..segment_number).next_back()
                && offset < previous_offset.saturating_add(u64::from(*previous_len))
            {
                return Some(*previous);
            }
            if let Some((next, (next_offset, _))) =
                placements.range(segment_number.saturating_add(1)..).next()
                && end > *next_offset
            {
                return Some(*next);
            }
        }
        None
    }

    /// Record where a segment was placed. Re-recording the same ordinal is the
    /// ordinary duplicate/retry case and simply overwrites.
    pub fn record_placement(&mut self, segment_number: u32, offset: u64, len: u32) {
        self.restored_prefix_end = self.restored_prefix_end.min(offset);
        self.reconstructed_placements.remove(&segment_number);
        self.placements.insert(segment_number, (offset, len));
    }

    pub(crate) fn record_reconstructed_placement(&mut self, ordinal: u32, offset: u64, len: u32) {
        self.reconstructed_placements.insert(ordinal, (offset, len));
    }

    /// Where an ordinal was placed, if it has arrived.
    ///
    /// Sequential assembly needs this to re-place a duplicate at the offset its
    /// first copy already occupies, rather than deriving an offset the cursor
    /// has since moved past.
    pub fn placement_of(&self, segment_number: u32) -> Option<(u64, u32)> {
        self.placements.get(&segment_number).copied()
    }

    pub(crate) fn note_retained_damage(&mut self, segment_number: u32) {
        self.damaged_segments.insert(segment_number);
        let floor = self
            .placement_of(segment_number)
            .map_or(self.segment_offset(segment_number), |(offset, _)| {
                offset.min(self.segment_offset(segment_number))
            });
        self.retained_damage_floor = Some(
            self.retained_damage_floor
                .map_or(floor, |old| old.min(floor)),
        );
    }

    pub(crate) fn clear_retained_damage(&mut self, segment_number: u32) -> bool {
        self.damaged_segments.remove(&segment_number)
    }

    pub(crate) fn has_retained_damage(&self) -> bool {
        !self.damaged_segments.is_empty()
    }

    pub(crate) fn retained_damage_floor(&self) -> Option<u64> {
        self.retained_damage_floor
    }

    pub(crate) fn segment_has_retained_damage(&self, segment_number: u32) -> bool {
        self.damaged_segments.contains(&segment_number)
    }

    pub(crate) fn requires_file_verification(&self) -> bool {
        self.has_retained_damage() || self.geometry_requires_verification
    }

    pub(crate) fn require_geometry_verification(&mut self) {
        self.geometry_requires_verification = true;
    }

    pub(crate) fn clear_geometry_verification(&mut self) {
        self.geometry_requires_verification = false;
    }

    pub(crate) fn note_restored_prefix(&mut self, end: u64) {
        self.restored_prefix_end = end;
    }

    pub(crate) fn note_part_verification(&mut self, ordinal: u32, verified: bool) {
        if ordinal.checked_add(1) == Some(self.total_segments) {
            self.final_part_verified = verified;
        }
    }

    pub(crate) fn final_part_verified(&self) -> bool {
        self.final_part_verified
    }

    /// Actual coverage, including the untouched restored prefix. NZB encoded
    /// sizes and progress totals are never used as decoded file lengths.
    pub(crate) fn decoded_coverage_end(&self) -> Option<u64> {
        if !self.is_complete() || self.has_retained_damage() {
            return None;
        }
        let final_ordinal = self.total_segments.checked_sub(1)?;
        self.placements
            .get(&final_ordinal)
            .or_else(|| self.reconstructed_placements.get(&final_ordinal))?;
        let mut cursor = self.restored_prefix_end;
        let mut decoded = self.placements.iter().peekable();
        let mut rebuilt = self.reconstructed_placements.iter().peekable();
        while decoded.peek().is_some() || rebuilt.peek().is_some() {
            let (_, (offset, len)) = if rebuilt.peek().is_none_or(|(rebuilt_ordinal, _)| {
                decoded
                    .peek()
                    .is_some_and(|(ordinal, _)| ordinal < rebuilt_ordinal)
            }) {
                decoded.next()?
            } else {
                rebuilt.next()?
            };
            if *offset != cursor {
                return None;
            }
            cursor = cursor.checked_add(u64::from(*len))?;
        }
        Some(cursor)
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
        if let Some(ready) = &mut self.repair_output_ready {
            *ready = false;
        }
        self.received.fill(false);
        self.received_bytes = 0;
        self.placements.clear();
        self.reconstructed_placements.clear();
        self.has_duplicate_segments = false;
        self.retained_damage_floor = None;
        self.damaged_segments.clear();
        self.restored_prefix_end = 0;
        self.final_part_verified = false;
        self.geometry_requires_verification = false;
    }

    /// Declare the file fully received.
    ///
    /// UNITS: `received_bytes` normally accumulates DECODED bytes, one segment
    /// at a time, while `total_bytes` is the sum of the NZB's DECLARED segment
    /// sizes — which are encoded. The two are deliberately made equal here, so
    /// that a file completed by verification or repair reports 100% rather than
    /// the ~97% a yEnc file's decoded total would otherwise show against its
    /// declared total.
    ///
    /// That gap is far wider for uuencode: it encodes at roughly 1.38x, so a
    /// uuencode file's decoded total is about 72% of its declared total. The
    /// decision is to keep this behaviour unchanged for both encodings —
    /// completion is a statement about *segments*, and every ordinal has been
    /// accounted for. `received_bytes` after this call is a progress figure in
    /// declared units, not a measurement of the bytes on disk, and nothing may
    /// use it as one. The bytes actually written are the sum of the recorded
    /// placements; `contiguous_placements_proven` is what reasons about those.
    pub fn mark_complete(&mut self) {
        self.damaged_segments.clear();
        self.retained_damage_floor = None;
        self.geometry_requires_verification = false;
        if let Some(ready) = &mut self.repair_output_ready {
            *ready = true;
        }
        self.received.fill(true);
        self.received_bytes = self.total_bytes;
    }

    /// Complete an independently verified PAR3 image while preserving its
    /// authenticated decoded length for virtual source readers. The ordinary
    /// PAR2 completion/progress policy continues to use `mark_complete`.
    pub(crate) fn mark_complete_decoded(&mut self, decoded_len: u64) {
        self.mark_complete();
        self.received_bytes = decoded_len;
    }

    /// Withdraw a segment the assembly already holds, because the bytes it
    /// delivered are now known not to be trustworthy.
    ///
    /// A whole-file CRC32 recovery that cannot re-fetch a doubted segment ends
    /// with that segment's bytes still on disk. Leaving its ordinal marked
    /// received would let the file read as complete and its failure be booked
    /// as nothing, since delivery is a terminal state held in this bitmap. The
    /// ordinal goes back to missing so the segment can be booked as damage and
    /// repair decides the file's fate. Returns whether anything was withdrawn.
    pub fn retract_segment(&mut self, segment_number: u32) -> bool {
        if !self.has_segment(segment_number) {
            return false;
        }
        self.received.set(segment_number as usize, false);
        let placement = self
            .placements
            .remove(&segment_number)
            .or_else(|| self.reconstructed_placements.remove(&segment_number));
        if let Some((_, len)) = placement {
            self.received_bytes = self.received_bytes.saturating_sub(len as u64);
        }
        if let Some(ready) = &mut self.repair_output_ready {
            *ready = false;
        }
        true
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
        if let Some(ready) = self.repair_output_ready {
            return if ready { 1.0 } else { 0.0 };
        }
        if self.total_segments == 0 {
            return 1.0;
        }
        self.received.count_ones() as f64 / self.total_segments as f64
    }

    /// Whether all segments have been received.
    pub fn is_complete(&self) -> bool {
        if let Some(ready) = self.repair_output_ready {
            return ready;
        }
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
