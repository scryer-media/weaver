use bitvec::prelude::*;
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
    /// Cumulative byte offsets: cumulative_offsets[i] = sum of segment_sizes[0..i].
    cumulative_offsets: Vec<u64>,

    /// Bitset tracking which segments (0-indexed) have been received.
    received: BitVec,
    /// Running byte count of received data.
    received_bytes: u64,
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
            role,
            total_segments,
            total_bytes,
            cumulative_offsets,
            received: bitvec![0; total_segments as usize],
            received_bytes: 0,
        }
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
            return Ok(CommitResult {
                file_complete: self.is_complete(),
                was_duplicate: true,
            });
        }

        // Mark as received.
        self.received.set(segment_number as usize, true);
        self.received_bytes += decoded_size as u64;

        Ok(CommitResult {
            file_complete: self.is_complete(),
            was_duplicate: false,
        })
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

    /// The byte offset within the target file where a given segment's data should be written.
    /// Segments are sequential: segment 0 starts at offset 0, segment 1 at segment_sizes[0], etc.
    pub fn segment_offset(&self, segment_number: u32) -> u64 {
        self.cumulative_offsets[segment_number as usize]
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
        FileAssembly::new(
            file_id,
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            segment_sizes,
        )
    }

    #[test]
    fn commit_in_order() {
        let mut asm = make_assembly(vec![500, 500]);
        let r0 = asm.commit_segment(0, 500).unwrap();
        assert!(!r0.file_complete);
        assert!(!r0.was_duplicate);
        let r1 = asm.commit_segment(1, 500).unwrap();
        assert!(r1.file_complete);
    }

    #[test]
    fn commit_out_of_order() {
        let mut asm = make_assembly(vec![500, 500, 500]);
        let r2 = asm.commit_segment(2, 500).unwrap();
        assert!(!r2.file_complete);
        let r0 = asm.commit_segment(0, 500).unwrap();
        assert!(!r0.file_complete);
        let r1 = asm.commit_segment(1, 500).unwrap();
        assert!(r1.file_complete);
    }

    #[test]
    fn duplicate_segment() {
        let mut asm = make_assembly(vec![500]);
        let r0 = asm.commit_segment(0, 500).unwrap();
        assert!(!r0.was_duplicate);
        let r1 = asm.commit_segment(0, 500).unwrap();
        assert!(r1.was_duplicate);
    }

    #[test]
    fn segment_out_of_range() {
        let mut asm = make_assembly(vec![500]);
        assert!(asm.commit_segment(1, 500).is_err());
    }
}
