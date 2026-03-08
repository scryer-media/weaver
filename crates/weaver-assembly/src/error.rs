use weaver_core::id::NzbFileId;

/// Errors that can occur during file and job assembly.
#[derive(Debug, thiserror::Error)]
pub enum AssemblyError {
    #[error("segment {segment_number} out of range (total: {total_segments})")]
    SegmentOutOfRange {
        segment_number: u32,
        total_segments: u32,
    },

    #[error("duplicate segment {segment_number} for file {file_id}")]
    DuplicateSegment {
        file_id: NzbFileId,
        segment_number: u32,
    },

    #[error("file {file_id} not found in job assembly")]
    FileNotFound { file_id: NzbFileId },
}
