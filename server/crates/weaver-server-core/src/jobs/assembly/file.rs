use crate::jobs::ids::NzbFileId;
use bitvec::prelude::*;
use serde::{Deserialize, Serialize};
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

    pub fn reset(&mut self) {
        self.received.fill(false);
        self.received_bytes = 0;
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
mod tests;
