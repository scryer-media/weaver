use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// The RAR archive format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveFormat {
    Rar14,
    Rar4,
    Rar5,
}

impl ArchiveFormat {
    pub fn is_rar4_family(self) -> bool {
        matches!(self, Self::Rar14 | Self::Rar4)
    }
}

/// Unpacked data integrity value stored in archive headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataHash {
    /// RAR 1.4 stores UnRAR's legacy 16-bit Checksum14, not CRC32.
    Rar14(u16),
    /// RAR 1.5+ stores CRC32.
    Crc32(u32),
}

impl DataHash {
    pub fn crc32(self) -> Option<u32> {
        match self {
            Self::Crc32(crc) => Some(crc),
            Self::Rar14(_) => None,
        }
    }
}

/// Host operating system that created the archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostOs {
    Windows,
    Unix,
    Unknown(u64),
}

impl From<u64> for HostOs {
    fn from(value: u64) -> Self {
        match value {
            0 => HostOs::Windows,
            1 => HostOs::Unix,
            other => HostOs::Unknown(other),
        }
    }
}

/// Compression method used for a file entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMethod {
    /// No compression (store / method 0).
    Store,
    /// Fastest compression (method 1).
    Fastest,
    /// Fast compression (method 2).
    Fast,
    /// Normal compression (method 3).
    Normal,
    /// Good compression (method 4).
    Good,
    /// Best compression (method 5).
    Best,
    /// Unknown method value.
    Unknown(u8),
}

impl CompressionMethod {
    /// Create from the 3-bit method code in compression info.
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => CompressionMethod::Store,
            1 => CompressionMethod::Fastest,
            2 => CompressionMethod::Fast,
            3 => CompressionMethod::Normal,
            4 => CompressionMethod::Good,
            5 => CompressionMethod::Best,
            other => CompressionMethod::Unknown(other),
        }
    }

    /// Return the numeric method code.
    pub fn code(&self) -> u8 {
        match self {
            CompressionMethod::Store => 0,
            CompressionMethod::Fastest => 1,
            CompressionMethod::Fast => 2,
            CompressionMethod::Normal => 3,
            CompressionMethod::Good => 4,
            CompressionMethod::Best => 5,
            CompressionMethod::Unknown(c) => *c,
        }
    }
}

/// Decoded compression information from the bit-packed vint field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionInfo {
    /// Archive format (RAR4 vs RAR5) — determines which decompressor to use.
    pub format: ArchiveFormat,
    /// Algorithm version (currently 0).
    pub version: u8,
    /// Solid flag: continue dictionary from previous file.
    pub solid: bool,
    /// Compression method.
    pub method: CompressionMethod,
    /// Dictionary size in bytes.
    pub dict_size: u64,
}

/// File hash type stored in extra records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileHash {
    Blake2sp([u8; 32]),
}

/// Span of volumes containing a member's data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeSpan {
    pub first_volume: usize,
    pub last_volume: usize,
}

impl VolumeSpan {
    pub fn single(volume: usize) -> Self {
        Self {
            first_volume: volume,
            last_volume: volume,
        }
    }

    /// Returns true if the member spans multiple volumes.
    pub fn is_split(&self) -> bool {
        self.first_volume != self.last_volume
    }
}

/// Unix ownership metadata from a RAR5 owner extra field.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UnixOwnerInfo {
    pub user_name: Option<String>,
    pub group_name: Option<String>,
    pub uid: Option<u64>,
    pub gid: Option<u64>,
}

/// Metadata for a single file/directory in the archive.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    /// The sanitized file name (safe for extraction).
    pub name: String,
    /// The original unsanitized name from the archive header.
    pub raw_name: String,
    pub unpacked_size: Option<u64>,
    pub compressed_size: u64,
    pub is_directory: bool,
    pub crc32: Option<u32>,
    pub mtime: Option<SystemTime>,
    pub ctime: Option<SystemTime>,
    pub atime: Option<SystemTime>,
    /// RAR5 file-version extra record, when present.
    pub version: Option<u64>,
    pub host_os: HostOs,
    pub compression: CompressionInfo,
    pub is_encrypted: bool,
    pub hash: Option<FileHash>,
    pub attributes: FileAttributes,
    pub owner: Option<UnixOwnerInfo>,
    pub volumes: VolumeSpan,
    pub is_symlink: bool,
    pub is_hardlink: bool,
    pub is_file_copy: bool,
    pub link_target: Option<String>,
}

/// Topology-oriented member info, including unresolved continuation entries.
#[derive(Debug, Clone)]
pub struct TopologyMemberInfo {
    /// The sanitized file name if known. Continuation-only entries may be empty.
    pub name: String,
    pub unpacked_size: Option<u64>,
    pub is_directory: bool,
    pub volumes: VolumeSpan,
    /// True when the starting header is still missing and this entry only
    /// represents later continuation segments.
    pub missing_start: bool,
}

/// Header-only metadata for the archive.
#[derive(Debug, Clone)]
pub struct ArchiveMetadata {
    pub format: ArchiveFormat,
    pub is_solid: bool,
    pub is_encrypted: bool,
    pub has_recovery_record: bool,
    pub recovery_records: Vec<RecoveryRecordInfo>,
    pub is_locked: bool,
    pub has_authenticity_verification: bool,
    pub has_locator: bool,
    pub quick_open_offset: Option<u64>,
    pub recovery_record_offset: Option<u64>,
    pub original_name: Option<String>,
    pub original_creation_time: Option<SystemTime>,
    pub volume_count: Option<usize>,
    pub members: Vec<MemberInfo>,
}

/// Metadata for embedded recovery records discovered in archive headers.
///
/// This describes RAR4 protect headers and RAR5 `RR` service records. It does
/// not imply that embedded recovery-record self-repair is implemented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRecordInfo {
    pub format: ArchiveFormat,
    pub kind: RecoveryRecordKind,
    pub offset: u64,
    pub data_offset: u64,
    pub data_size: u64,
    pub protected_size: Option<u64>,
    pub recovery_sectors: Option<u16>,
    pub total_blocks: Option<u32>,
    pub recovery_percent: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryRecordKind {
    Rar4Protect,
    Rar5Service,
}

/// File attributes (OS-dependent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileAttributes(pub u64);

impl FileAttributes {
    /// Windows read-only attribute.
    pub fn is_readonly(&self) -> bool {
        self.0 & 0x1 != 0
    }

    /// Windows hidden attribute.
    pub fn is_hidden(&self) -> bool {
        self.0 & 0x2 != 0
    }

    /// Windows system attribute.
    pub fn is_system(&self) -> bool {
        self.0 & 0x4 != 0
    }

    /// Windows directory attribute.
    pub fn is_directory_attr(&self) -> bool {
        self.0 & 0x10 != 0
    }

    /// Unix permission mode (low 9 bits of attributes for Unix).
    pub fn unix_mode(&self) -> u32 {
        (self.0 & 0xFFFF) as u32
    }
}
