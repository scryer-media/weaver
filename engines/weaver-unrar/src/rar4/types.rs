//! RAR4-specific types.

/// RAR4 header type codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rar4HeaderType {
    /// Marker/signature header (0x72).
    Mark,
    /// Archive header (0x73).
    Archive,
    /// File header (0x74).
    File,
    /// Comment header (0x75).
    Comment,
    /// Old authenticity verification header (HEAD3_AV / 0x76).
    Extra,
    /// Sub-block header (0x77).
    Sub,
    /// Recovery record (0x78).
    Recovery,
    /// Old authenticity signature header (HEAD3_SIGN / 0x79).
    Sign,
    /// New-style sub-header (0x7A) — comments, NTFS timestamps, ACL, streams.
    NewSub,
    /// End of archive header (0x7B).
    EndArchive,
    /// Unknown header type.
    Unknown(u8),
}

impl From<u8> for Rar4HeaderType {
    fn from(val: u8) -> Self {
        match val {
            0x72 => Self::Mark,
            0x73 => Self::Archive,
            0x74 => Self::File,
            0x75 => Self::Comment,
            0x76 => Self::Extra,
            0x77 => Self::Sub,
            0x78 => Self::Recovery,
            0x79 => Self::Sign,
            0x7A => Self::NewSub,
            0x7B => Self::EndArchive,
            other => Self::Unknown(other),
        }
    }
}

/// RAR4 archive header flags.
pub mod archive_flags {
    /// Archive is part of a multi-volume set.
    pub const VOLUME: u16 = 0x0001;
    /// Archive has a comment.
    pub const COMMENT: u16 = 0x0002;
    /// Archive is locked.
    pub const LOCK: u16 = 0x0004;
    /// Archive is solid.
    pub const SOLID: u16 = 0x0008;
    /// RAR 1.4 archive comment is packed.
    pub const PACK_COMMENT: u16 = 0x0010;
    /// New volume naming scheme (volname.partN.rar).
    pub const NEW_NUMBERING: u16 = 0x0010;
    /// Archive header has authentication info.
    pub const AUTH: u16 = 0x0020;
    /// Recovery record present.
    pub const RECOVERY: u16 = 0x0040;
    /// Block header has encrypted data.
    pub const ENCRYPTED_HEADERS: u16 = 0x0080;
    /// First volume of a set.
    pub const FIRST_VOLUME: u16 = 0x0100;
}

/// RAR4 file header flags.
pub mod file_flags {
    /// File continues from previous volume.
    pub const SPLIT_BEFORE: u16 = 0x0001;
    /// File continues in next volume.
    pub const SPLIT_AFTER: u16 = 0x0002;
    /// Password-encrypted.
    pub const ENCRYPTED: u16 = 0x0004;
    /// File comment present.
    pub const COMMENT: u16 = 0x0008;
    /// Solid flag (uses previous files' data for dictionary).
    pub const SOLID: u16 = 0x0010;
    /// Dictionary-size bits in RAR4 file headers.
    pub const WINDOW_MASK: u16 = 0x00e0;
    /// Special WINDOW_MASK value used by RAR4 to mark directory entries.
    pub const DIRECTORY: u16 = 0x00e0;
    /// High-size fields present (unpacked/packed sizes are 64-bit).
    pub const LARGE: u16 = 0x0100;
    /// Unicode filename present.
    pub const UNICODE: u16 = 0x0200;
    /// Salt present for encryption.
    pub const SALT: u16 = 0x0400;
    /// File version number present.
    pub const VERSION: u16 = 0x0800;
    /// Extended time fields present.
    pub const EXT_TIME: u16 = 0x1000;
}

/// RAR4 common header flags (apply to all header types).
pub mod common_flags {
    /// Data area present after header.
    pub const HAS_DATA: u16 = 0x8000;
    /// Skip header if unknown type.
    pub const SKIP_IF_UNKNOWN: u16 = 0x4000;
}

/// RAR4 compression method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rar4Method {
    /// Store (no compression).
    Store, // 0x30
    Fastest, // 0x31
    Fast,    // 0x32
    Normal,  // 0x33
    Good,    // 0x34
    Best,    // 0x35
    Unknown(u8),
}

impl From<u8> for Rar4Method {
    fn from(val: u8) -> Self {
        match val {
            0x30 => Self::Store,
            0x31 => Self::Fastest,
            0x32 => Self::Fast,
            0x33 => Self::Normal,
            0x34 => Self::Good,
            0x35 => Self::Best,
            other => Self::Unknown(other),
        }
    }
}

/// RAR4 host OS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rar4HostOs {
    MsDos,
    Os2,
    Windows,
    Unix,
    MacOs,
    BeOs,
    Unknown(u8),
}

impl From<u8> for Rar4HostOs {
    fn from(val: u8) -> Self {
        match val {
            0 => Self::MsDos,
            1 => Self::Os2,
            2 => Self::Windows,
            3 => Self::Unix,
            4 => Self::MacOs,
            5 => Self::BeOs,
            other => Self::Unknown(other),
        }
    }
}

/// Parsed RAR4 archive header.
#[derive(Debug, Clone)]
pub struct Rar4ArchiveHeader {
    pub flags: u16,
    pub is_volume: bool,
    pub is_solid: bool,
    pub is_encrypted: bool,
    pub has_recovery_record: bool,
    pub is_locked: bool,
    pub has_authenticity_verification: bool,
    pub is_first_volume: bool,
    pub high_pos_av: u16,
    pub pos_av: u32,
    pub is_signed: bool,
    /// Old-style archive comment is embedded in the main header.
    pub comment_in_header: bool,
    /// True if using `.partNNNN.rar` naming, false for old `.rNN` naming.
    pub new_naming: bool,
}

/// RAR4 embedded recovery record (`HEAD3_PROTECT`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rar4RecoveryRecord {
    pub header_offset: u64,
    pub data_offset: u64,
    pub data_size: u64,
    pub version: u8,
    pub recovery_sectors: u16,
    pub total_blocks: u32,
    pub mark: [u8; 8],
}

/// Old-style RAR4/RAR2.9 archive comment header (`HEAD3_CMT`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rar4CommentHeader {
    pub header_offset: u64,
    pub data_offset: u64,
    pub packed_size: u64,
    pub unpacked_size: u16,
    pub unpack_version: u8,
    pub method: Rar4Method,
    /// RAR stores only the low 16 bits of the comment data CRC.
    pub crc16: u16,
}

/// Old RAR 2.9 service subtype (`HEAD3_OLDSERVICE` / 0x77).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rar4OldServiceData {
    UnixOwner,
    NtAcl {
        unpacked_size: u32,
        unpack_version: u8,
        method: Rar4Method,
        crc32: u32,
    },
    Stream {
        unpacked_size: u32,
        unpack_version: u8,
        method: Rar4Method,
        crc32: u32,
        stream_name: String,
        stream_name_raw: Vec<u8>,
    },
    Unknown,
}

/// Old-style RAR4/RAR2.9 service header (`HEAD3_OLDSERVICE`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rar4OldServiceHeader {
    pub header_offset: u64,
    pub data_offset: u64,
    pub data_size: u64,
    pub subtype: u16,
    pub level: u8,
    pub data: Rar4OldServiceData,
}

/// Parsed RAR4 file header.
#[derive(Debug, Clone)]
pub struct Rar4FileHeader {
    /// True when parsed from a RAR 1.4 `ReadHeader14` header.
    pub is_rar14: bool,
    pub flags: u16,
    pub packed_size: u64,
    pub unpacked_size: Option<u64>,
    pub host_os: Rar4HostOs,
    pub crc32: u32,
    pub mtime: u32, // DOS datetime
    pub mtime_precise: Option<std::time::SystemTime>,
    pub ctime: Option<std::time::SystemTime>,
    pub atime: Option<std::time::SystemTime>,
    pub version: Option<u64>,
    pub unpack_version: u8,
    pub method: Rar4Method,
    pub dict_size: u64,
    pub name: String,
    /// Raw RAR4 header name bytes, capped and zero-filled before Unicode/OEM
    /// conversion.
    pub name_raw: Option<Vec<u8>>,
    pub is_directory: bool,
    pub is_unix_symlink: bool,
    pub is_encrypted: bool,
    pub encryption_method: Option<Rar4EncryptionMethod>,
    pub is_solid: bool,
    /// For RAR4 service headers, `LHD_SOLID` means this subblock has a parent file.
    pub is_subblock: bool,
    pub split_before: bool,
    pub split_after: bool,
    /// Old-style file comment is embedded in this file header.
    pub comment_in_header: bool,
    /// Byte offset of the compressed data in the stream.
    pub data_offset: u64,
    /// Salt for encrypted files (8 bytes, if present).
    pub salt: Option<[u8; 8]>,
    /// File attributes.
    pub attributes: u32,
    /// Optional service subheader bytes stored after the service name.
    pub service_subdata: Option<Vec<u8>>,
    /// Unix owner metadata supplied by a following `UOW` service record.
    pub owner: Option<crate::types::UnixOwnerInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rar4EncryptionMethod {
    Rar13,
    Rar15,
    Rar20,
    Rar30,
}

impl Rar4EncryptionMethod {
    pub fn for_unpack_version(unpack_version: u8) -> Self {
        match unpack_version {
            13 => Self::Rar13,
            15 => Self::Rar15,
            20 | 26 => Self::Rar20,
            _ => Self::Rar30,
        }
    }

    pub fn uses_salt(self) -> bool {
        matches!(self, Self::Rar30)
    }
}

/// RAR4 end-of-archive header flags.
pub mod end_flags {
    /// More volumes follow.
    pub const NEXT_VOLUME: u16 = 0x0001;
    /// Data CRC and volume number are present after the header.
    pub const DATA_CRC: u16 = 0x0002;
    /// Volume number is present.
    pub const VOLUME_NUMBER: u16 = 0x0004;
    /// Recovery record space is present at the end of the volume.
    pub const REV_SPACE: u16 = 0x0008;
}

/// Parsed RAR4 end-of-archive header.
#[derive(Debug, Clone)]
pub struct Rar4EndHeader {
    pub flags: u16,
    pub more_volumes: bool,
    pub has_data_crc: bool,
    pub rev_space: bool,
    pub data_crc: Option<u32>,
    /// Volume number (0-based), if present in the ENDARC header.
    pub volume_number: Option<u16>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_type_from() {
        assert_eq!(Rar4HeaderType::from(0x72), Rar4HeaderType::Mark);
        assert_eq!(Rar4HeaderType::from(0x73), Rar4HeaderType::Archive);
        assert_eq!(Rar4HeaderType::from(0x74), Rar4HeaderType::File);
        assert_eq!(Rar4HeaderType::from(0x76), Rar4HeaderType::Extra);
        assert_eq!(Rar4HeaderType::from(0x79), Rar4HeaderType::Sign);
        assert_eq!(Rar4HeaderType::from(0x7B), Rar4HeaderType::EndArchive);
        assert!(matches!(
            Rar4HeaderType::from(0xFF),
            Rar4HeaderType::Unknown(0xFF)
        ));
    }

    #[test]
    fn test_method_from() {
        assert_eq!(Rar4Method::from(0x30), Rar4Method::Store);
        assert_eq!(Rar4Method::from(0x33), Rar4Method::Normal);
    }

    #[test]
    fn test_host_os_from() {
        assert_eq!(Rar4HostOs::from(2), Rar4HostOs::Windows);
        assert_eq!(Rar4HostOs::from(3), Rar4HostOs::Unix);
    }
}
