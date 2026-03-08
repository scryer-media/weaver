use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use weaver_core::id::{JobId, NzbFileId, SegmentId};

/// A single journal entry representing a state transition.
/// Serialized via MessagePack (rmp-serde) for compactness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalEntry {
    // -- Job lifecycle --
    JobCreated {
        job_id: JobId,
        nzb_hash: [u8; 32],
        nzb_path: PathBuf,
        output_dir: PathBuf,
        created_at: u64,
        #[serde(default)]
        category: Option<String>,
        #[serde(default)]
        metadata: Vec<(String, String)>,
    },
    JobStatusChanged {
        job_id: JobId,
        status: PersistedJobStatus,
        timestamp: u64,
    },

    // -- Segment-level progress --
    SegmentCommitted {
        segment_id: SegmentId,
        file_offset: u64,
        decoded_size: u32,
        crc32: u32,
    },

    // -- File-level progress --
    FileComplete {
        file_id: NzbFileId,
        filename: String,
        md5: [u8; 16],
    },

    // -- Verification --
    FileVerified {
        file_id: NzbFileId,
        status: PersistedVerifyStatus,
    },

    // -- PAR2 metadata --
    Par2MetadataLoaded {
        job_id: JobId,
        slice_size: u64,
        recovery_block_count: u32,
    },

    // -- Extraction --
    MemberExtracted {
        job_id: JobId,
        member_name: String,
        output_path: PathBuf,
    },
    ExtractionComplete {
        job_id: JobId,
    },

    // -- Compaction marker --
    Checkpoint {
        job_id: JobId,
        timestamp: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedJobStatus {
    Downloading,
    Verifying,
    Repairing,
    Extracting,
    Complete,
    Failed { error: String },
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedVerifyStatus {
    Intact,
    Damaged { bad_slices: u32, total_slices: u32 },
    Missing,
}
