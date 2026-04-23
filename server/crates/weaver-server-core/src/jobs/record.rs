use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::jobs::assembly::DetectedArchiveIdentity;
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};

#[derive(Debug, Clone)]
pub struct ActiveJob {
    pub job_id: JobId,
    pub nzb_hash: [u8; 32],
    pub nzb_path: PathBuf,
    pub output_dir: PathBuf,
    pub created_at: u64,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct CommittedSegment {
    pub job_id: JobId,
    pub file_index: u32,
    pub segment_number: u32,
    pub file_offset: u64,
    pub decoded_size: u32,
    pub crc32: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileIdentitySource {
    Declared,
    Probe,
    Par2,
    Nested,
}

impl FileIdentitySource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Declared => "declared",
            Self::Probe => "probe",
            Self::Par2 => "par2",
            Self::Nested => "nested",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "declared" => Some(Self::Declared),
            "probe" => Some(Self::Probe),
            "par2" => Some(Self::Par2),
            "nested" => Some(Self::Nested),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveFileIdentity {
    pub file_index: u32,
    pub source_filename: String,
    pub current_filename: String,
    pub canonical_filename: Option<String>,
    pub classification: Option<DetectedArchiveIdentity>,
    pub classification_source: FileIdentitySource,
}

#[derive(Debug)]
pub struct RecoveredJob {
    pub job_id: JobId,
    pub nzb_path: PathBuf,
    pub output_dir: PathBuf,
    pub committed_segments: HashSet<SegmentId>,
    pub file_progress: HashMap<u32, u64>,
    pub detected_archives: HashMap<u32, DetectedArchiveIdentity>,
    pub file_identities: HashMap<u32, ActiveFileIdentity>,
    pub complete_files: HashSet<NzbFileId>,
    pub extracted_members: HashSet<String>,
    pub status: String,
    pub download_state: Option<String>,
    pub post_state: Option<String>,
    pub run_state: Option<String>,
    pub error: Option<String>,
    pub created_at: u64,
    pub queued_repair_at_epoch_ms: Option<f64>,
    pub queued_extract_at_epoch_ms: Option<f64>,
    pub paused_resume_status: Option<String>,
    pub paused_resume_download_state: Option<String>,
    pub paused_resume_post_state: Option<String>,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivePar2File {
    pub file_index: u32,
    pub filename: String,
    pub recovery_block_count: u32,
    pub promoted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveFileProgress {
    pub job_id: JobId,
    pub file_index: u32,
    pub contiguous_bytes_written: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveExtractionChunk {
    pub job_id: JobId,
    pub set_name: String,
    pub member_name: String,
    pub volume_index: u32,
    pub bytes_written: u64,
    pub temp_path: String,
    pub start_offset: u64,
    pub end_offset: u64,
}

pub type RarVolumeFactsBySet = HashMap<String, Vec<(u32, Vec<u8>)>>;

#[derive(Debug, Clone)]
pub struct ExtractionChunk {
    pub member_name: String,
    pub volume_index: u32,
    pub bytes_written: u64,
    pub temp_path: String,
    pub start_offset: u64,
    pub end_offset: u64,
    pub verified: bool,
    pub appended: bool,
}
