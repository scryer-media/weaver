use bitvec::prelude::*;
use weaver_core::checksum::{FileHashState, SliceChecksumState};
use weaver_core::classify::FileRole;
use weaver_core::id::NzbFileId;

use crate::error::AssemblyError;

/// Tracks the assembly state of a single NZB file.
pub struct FileAssembly {
    file_id: NzbFileId,
    filename: String,
    role: FileRole,
    total_segments: u32,
    total_bytes: u64,
    segment_sizes: Vec<u32>,

    /// Bitset tracking which segments (0-indexed) have been received.
    received: BitVec,
    /// Running byte count of received data.
    received_bytes: u64,

    /// Incremental PAR2 slice verification state.
    /// Populated when PAR2 metadata becomes available.
    slice_states: Option<Vec<IncrementalSliceState>>,

    /// Full-file MD5 hash (can only be computed if segments arrive in order).
    /// None if out-of-order segments have been received.
    file_hash: Option<FileHashState>,
    /// Track the next expected sequential segment for in-order hash feeding.
    next_sequential_segment: u32,
}

/// Tracks checksum state for a single PAR2 slice.
pub struct IncrementalSliceState {
    pub slice_index: u32,
    /// Byte range in the target file this slice covers.
    pub byte_start: u64,
    pub byte_end: u64,
    /// Expected checksum from PAR2 IFSC packet.
    pub expected_crc32: u32,
    pub expected_md5: [u8; 16],
    /// Streaming checksum state (fed with raw data when available).
    pub checksum: SliceChecksumState,
    /// How many bytes of this slice have been fed.
    pub bytes_received: u64,
    /// Verification result: None = incomplete, Some(true) = valid, Some(false) = damaged.
    pub verified: Option<bool>,
    /// Accumulated CRC32 built from per-segment CRCs using crc32_combine.
    /// Used for early damage detection without keeping decoded data alive.
    /// None if any segment within this slice spanned a boundary (not combinable).
    pub accumulated_crc: Option<u32>,
    /// Bytes covered by the accumulated CRC (may differ from bytes_received
    /// if some segments couldn't be combined due to boundary spanning).
    pub crc_bytes_covered: u64,
}

/// Result of committing a segment to assembly.
#[derive(Debug)]
pub struct CommitResult {
    /// Whether the file is now complete (all segments received).
    pub file_complete: bool,
    /// Slices that were verified as a result of this commit.
    pub newly_verified_slices: Vec<(u32, bool)>,
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

        Self {
            file_id,
            filename,
            role,
            total_segments,
            total_bytes,
            received: bitvec![0; total_segments as usize],
            received_bytes: 0,
            segment_sizes,
            slice_states: None,
            file_hash: Some(FileHashState::new()),
            next_sequential_segment: 0,
        }
    }

    /// Returns true if PAR2 slice verification states have been attached.
    /// When false, `commit_segment_meta` can be used instead of `commit_segment`
    /// to avoid keeping the decoded data buffer alive through assembly update.
    pub fn has_slice_states(&self) -> bool {
        self.slice_states
            .as_ref()
            .is_some_and(|s| !s.is_empty())
    }

    /// Metadata-only commit: updates segment tracking (marks received, byte counts,
    /// completeness) without feeding data into PAR2 slice checksums.
    ///
    /// Use this when `has_slice_states()` returns false (before PAR2 index is parsed)
    /// so the caller can drop the decoded data buffer sooner.
    ///
    /// The returned `CommitResult` always has empty `newly_verified_slices`.
    pub fn commit_segment_meta(
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
            return Ok(CommitResult {
                file_complete: self.is_complete(),
                newly_verified_slices: Vec::new(),
                was_duplicate: true,
            });
        }

        // Mark as received.
        self.received.set(segment_number as usize, true);
        self.received_bytes += decoded_size as u64;

        // Without data we cannot maintain the streaming file hash, so abandon it.
        self.file_hash = None;

        Ok(CommitResult {
            file_complete: self.is_complete(),
            newly_verified_slices: Vec::new(),
            was_duplicate: false,
        })
    }

    /// Accumulate a segment's CRC32 into overlapping slices without needing
    /// the raw data. Uses `crc32_combine` to build per-slice CRCs from
    /// per-segment CRCs. Only works for segments that fall entirely within
    /// a single slice; boundary-spanning segments poison the slice's accumulator.
    ///
    /// Returns early damage detection results: slices where the accumulated CRC
    /// covers the full slice but doesn't match the expected CRC32.
    pub fn accumulate_segment_crc(
        &mut self,
        segment_number: u32,
        segment_crc32: u32,
        decoded_size: u32,
    ) -> Vec<(u32, bool)> {
        let slice_states = match self.slice_states.as_mut() {
            Some(states) => states,
            None => return Vec::new(),
        };

        // Compute segment byte range.
        let seg_start: u64 = self.segment_sizes[..segment_number as usize]
            .iter()
            .map(|&s| s as u64)
            .sum();
        let seg_end = seg_start + decoded_size as u64;

        let mut early_results = Vec::new();

        for state in slice_states.iter_mut() {
            if state.verified.is_some() {
                continue;
            }

            let overlap_start = seg_start.max(state.byte_start);
            let overlap_end = seg_end.min(state.byte_end);

            if overlap_start >= overlap_end {
                continue;
            }

            // Check if segment falls entirely within this slice.
            let fully_contained = seg_start >= state.byte_start && seg_end <= state.byte_end;

            if fully_contained {
                if let Some(acc) = state.accumulated_crc {
                    // Combine: CRC(accumulated || segment) = combine(acc, seg_crc, seg_len)
                    state.accumulated_crc = Some(weaver_core::checksum::crc32_combine(
                        acc,
                        segment_crc32,
                        decoded_size as u64,
                    ));
                    state.crc_bytes_covered += decoded_size as u64;
                }
            } else {
                // Segment spans slice boundary — can't combine CRC.
                state.accumulated_crc = None;
            }

            // Early damage detection: when CRC covers the full slice, check it.
            let slice_size = state.byte_end - state.byte_start;
            if state.crc_bytes_covered >= slice_size
                && let Some(acc_crc) = state.accumulated_crc {
                    // CRC-only check (MD5 deferred to full verification).
                    let crc_matches = acc_crc == state.expected_crc32;
                    if !crc_matches {
                        // Early damage: CRC mismatch means this slice is corrupt.
                        // Don't mark as verified yet — full MD5 verify deferred.
                        early_results.push((state.slice_index, false));
                    }
                    // If CRC matches, we can't be certain (CRC32 collisions possible),
                    // so we don't mark as verified. Full MD5 check at file completion.
                }
        }

        early_results
    }

    /// Record that a segment has been received and decoded.
    /// data: the decoded segment bytes.
    /// Returns CommitResult indicating what changed.
    pub fn commit_segment(
        &mut self,
        segment_number: u32,
        data: &[u8],
    ) -> Result<CommitResult, AssemblyError> {
        if segment_number >= self.total_segments {
            return Err(AssemblyError::SegmentOutOfRange {
                segment_number,
                total_segments: self.total_segments,
            });
        }

        // Check for duplicate.
        if self.received[segment_number as usize] {
            return Ok(CommitResult {
                file_complete: self.is_complete(),
                newly_verified_slices: Vec::new(),
                was_duplicate: true,
            });
        }

        // Mark as received.
        self.received.set(segment_number as usize, true);
        self.received_bytes += data.len() as u64;

        // Update file hash if segments are arriving in order.
        if segment_number == self.next_sequential_segment {
            if let Some(ref mut hash) = self.file_hash {
                hash.update(data);
            }
            self.next_sequential_segment += 1;
        } else {
            // Out-of-order arrival; we can no longer compute a streaming file hash.
            self.file_hash = None;
        }

        // Feed data into any overlapping PAR2 slice checksums.
        let newly_verified = self.feed_slice_checksums(segment_number, data);

        Ok(CommitResult {
            file_complete: self.is_complete(),
            newly_verified_slices: newly_verified,
            was_duplicate: false,
        })
    }

    /// Feed segment data into overlapping PAR2 slice checksum states.
    /// Returns any slices that became fully verified as a result.
    fn feed_slice_checksums(
        &mut self,
        segment_number: u32,
        data: &[u8],
    ) -> Vec<(u32, bool)> {
        let slice_states = match self.slice_states.as_mut() {
            Some(states) => states,
            None => return Vec::new(),
        };

        // Compute segment byte range without borrowing self again.
        let seg_start: u64 = self.segment_sizes[..segment_number as usize]
            .iter()
            .map(|&s| s as u64)
            .sum();
        let seg_end = seg_start + data.len() as u64;

        // Compute the nominal slice size from the first slice (for padding calculation).
        let nominal_slice_size = slice_states
            .first()
            .map(|s| s.byte_end - s.byte_start)
            .unwrap_or(0);

        let total_bytes = self.total_bytes;
        let mut newly_verified = Vec::new();

        for state in slice_states.iter_mut() {
            // Skip already-verified slices.
            if state.verified.is_some() {
                continue;
            }

            // Check overlap between segment [seg_start, seg_end) and slice [byte_start, byte_end).
            let overlap_start = seg_start.max(state.byte_start);
            let overlap_end = seg_end.min(state.byte_end);

            if overlap_start >= overlap_end {
                continue;
            }

            // Extract the overlapping portion from data.
            let data_offset = (overlap_start - seg_start) as usize;
            let data_len = (overlap_end - overlap_start) as usize;
            let overlap_data = &data[data_offset..data_offset + data_len];

            state.checksum.update(overlap_data);
            state.bytes_received += data_len as u64;

            // Check if this slice is now complete.
            let slice_size = state.byte_end - state.byte_start;
            if state.bytes_received >= slice_size {
                // Determine if this is the last slice (may need zero-padding per PAR2 spec).
                let pad_to = if state.byte_end == total_bytes
                    && state.bytes_received < nominal_slice_size
                {
                    Some(nominal_slice_size)
                } else {
                    None
                };

                // We need to finalize, so swap in a fresh state to take ownership.
                let finished = std::mem::replace(&mut state.checksum, SliceChecksumState::new());
                let (crc32, md5) = finished.finalize(pad_to);

                let valid = crc32 == state.expected_crc32 && md5 == state.expected_md5;
                state.verified = Some(valid);
                newly_verified.push((state.slice_index, valid));
            }
        }

        newly_verified
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
        &self.role
    }

    /// The filename.
    pub fn filename(&self) -> &str {
        &self.filename
    }

    /// The file id.
    pub fn file_id(&self) -> NzbFileId {
        self.file_id
    }

    /// Attach PAR2 verification metadata.
    /// Called when the PAR2 index file is downloaded and parsed.
    /// slice_size: bytes per PAR2 slice.
    /// checksums: (crc32, md5) per slice.
    pub fn attach_par2_metadata(&mut self, slice_size: u64, checksums: &[(u32, [u8; 16])]) {
        let mut states = Vec::with_capacity(checksums.len());

        for (i, &(expected_crc32, expected_md5)) in checksums.iter().enumerate() {
            let byte_start = i as u64 * slice_size;
            let byte_end = ((i as u64 + 1) * slice_size).min(self.total_bytes);

            states.push(IncrementalSliceState {
                slice_index: i as u32,
                byte_start,
                byte_end,
                expected_crc32,
                expected_md5,
                checksum: SliceChecksumState::new(),
                bytes_received: 0,
                verified: None,
                accumulated_crc: Some(0), // CRC32 identity (empty data)
                crc_bytes_covered: 0,
            });
        }

        self.slice_states = Some(states);
    }

    /// Get verification results for all slices.
    /// Returns None if PAR2 metadata not attached.
    pub fn slice_verification_results(&self) -> Option<Vec<(u32, Option<bool>)>> {
        self.slice_states.as_ref().map(|states| {
            states
                .iter()
                .map(|s| (s.slice_index, s.verified))
                .collect()
        })
    }

    /// Count of damaged slices found so far.
    pub fn damaged_slice_count(&self) -> u32 {
        self.slice_states
            .as_ref()
            .map(|states| {
                states
                    .iter()
                    .filter(|s| s.verified == Some(false))
                    .count() as u32
            })
            .unwrap_or(0)
    }

    /// Count of verified-good slices.
    pub fn verified_slice_count(&self) -> u32 {
        self.slice_states
            .as_ref()
            .map(|states| {
                states
                    .iter()
                    .filter(|s| s.verified == Some(true))
                    .count() as u32
            })
            .unwrap_or(0)
    }

    /// Returns unverified slice descriptors: (slice_index, byte_start, byte_end, expected_crc32, expected_md5).
    /// These are slices where `verified` is still `None` after all segments have been committed —
    /// typically because their segments arrived before PAR2 metadata was loaded.
    pub fn unverified_slices(&self) -> Vec<(u32, u64, u64, u32, [u8; 16])> {
        let Some(states) = self.slice_states.as_ref() else {
            return Vec::new();
        };
        states
            .iter()
            .filter(|s| s.verified.is_none())
            .map(|s| (s.slice_index, s.byte_start, s.byte_end, s.expected_crc32, s.expected_md5))
            .collect()
    }

    /// Mark a slice as verified after disk-based re-verification.
    pub fn mark_slice_verified(&mut self, slice_index: u32, valid: bool) {
        if let Some(states) = self.slice_states.as_mut()
            && let Some(state) = states.iter_mut().find(|s| s.slice_index == slice_index) {
                state.verified = Some(valid);
            }
    }

    /// The byte offset within the target file where a given segment's data should be written.
    /// Segments are sequential: segment 0 starts at offset 0, segment 1 at segment_sizes[0], etc.
    pub fn segment_offset(&self, segment_number: u32) -> u64 {
        self.segment_sizes[..segment_number as usize]
            .iter()
            .map(|&s| s as u64)
            .sum()
    }

    /// Finalize and return the file's MD5 hash.
    /// Returns None if segments arrived out of order (streaming hash was abandoned).
    pub fn finalize_hash(&mut self) -> Option<[u8; 16]> {
        self.file_hash.take().map(|h| h.finalize())
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
}

#[cfg(test)]
mod tests {
    use weaver_core::classify::FileRole;
    use weaver_core::id::{JobId, NzbFileId};

    use super::*;

    fn make_assembly(segment_sizes: Vec<u32>) -> FileAssembly {
        let file_id = NzbFileId {
            job_id: JobId(1),
            file_index: 0,
        };
        FileAssembly::new(file_id, "test.rar".into(), FileRole::RarVolume { volume_number: 0 }, segment_sizes)
    }

    #[test]
    fn crc_accumulate_single_slice_detects_damage() {
        // 2 segments of 500 bytes each = 1000 bytes total.
        // 1 slice covering the whole file (slice_size = 1000).
        let mut asm = make_assembly(vec![500, 500]);

        // Compute the expected CRC for known data.
        let data_a = vec![0xAA; 500];
        let data_b = vec![0xBB; 500];
        let crc_a = weaver_core::checksum::crc32(&data_a);
        let crc_b = weaver_core::checksum::crc32(&data_b);

        // Compute the full-slice CRC (A || B).
        let mut full_data = data_a.clone();
        full_data.extend_from_slice(&data_b);
        let full_crc = weaver_core::checksum::crc32(&full_data);

        // MD5 doesn't matter for CRC-only early detection — use dummy.
        let full_md5 = [0u8; 16];

        // Attach PAR2 metadata with correct checksums.
        asm.attach_par2_metadata(1000, &[(full_crc, full_md5)]);

        // Accumulate segment 0 CRC — no early result yet (slice not fully covered).
        let early = asm.accumulate_segment_crc(0, crc_a, 500);
        assert!(early.is_empty());

        // Accumulate segment 1 CRC — slice now fully covered, CRC should match.
        let early = asm.accumulate_segment_crc(1, crc_b, 500);
        assert!(early.is_empty()); // Match = no damage reported

        // Now test with a wrong CRC for segment 1.
        let mut asm2 = make_assembly(vec![500, 500]);
        asm2.attach_par2_metadata(1000, &[(full_crc, full_md5)]);

        let early = asm2.accumulate_segment_crc(0, crc_a, 500);
        assert!(early.is_empty());

        // Wrong CRC for segment 1.
        let early = asm2.accumulate_segment_crc(1, 0xDEADBEEF, 500);
        assert_eq!(early.len(), 1);
        assert_eq!(early[0], (0, false)); // Slice 0 damaged
    }

    #[test]
    fn crc_accumulate_boundary_spanning_poisons() {
        // 2 segments of 600 bytes each = 1200 bytes total.
        // 2 slices of 768 bytes each (second covers 768..1200).
        let mut asm = make_assembly(vec![600, 600]);

        // Dummy checksums (we're testing the poisoning, not matching).
        asm.attach_par2_metadata(768, &[(0, [0; 16]), (0, [0; 16])]);

        let seg0_crc = weaver_core::checksum::crc32(&[0u8; 600]);

        // Segment 0 (bytes 0..600) falls within slice 0 (0..768) — no spanning.
        let early = asm.accumulate_segment_crc(0, seg0_crc, 600);
        assert!(early.is_empty());
        // Slice 0 should still have accumulated_crc.
        let states = asm.slice_states.as_ref().unwrap();
        assert!(states[0].accumulated_crc.is_some());

        // Segment 1 (bytes 600..1200) spans slice 0 (0..768) and slice 1 (768..1200).
        let seg1_crc = weaver_core::checksum::crc32(&[0u8; 600]);
        let _early = asm.accumulate_segment_crc(1, seg1_crc, 600);

        // Both slices should have poisoned accumulated_crc.
        let states = asm.slice_states.as_ref().unwrap();
        assert!(states[0].accumulated_crc.is_none()); // Poisoned by spanning segment
        assert!(states[1].accumulated_crc.is_none()); // Poisoned by spanning segment
    }
}
