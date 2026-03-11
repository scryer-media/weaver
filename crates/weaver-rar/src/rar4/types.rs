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
    /// Extra info header (0x76).
    Extra,
    /// Sub-block header (0x77).
    Sub,
    /// Recovery record (0x78).
    Recovery,
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
    pub is_first_volume: bool,
    /// True if using `.partNNNN.rar` naming, false for old `.rNN` naming.
    pub new_naming: bool,
}

/// Parsed RAR4 file header.
#[derive(Debug, Clone)]
pub struct Rar4FileHeader {
    pub flags: u16,
    pub packed_size: u64,
    pub unpacked_size: u64,
    pub host_os: Rar4HostOs,
    pub crc32: u32,
    pub mtime: u32, // DOS datetime
    pub method: Rar4Method,
    pub name: String,
    pub is_directory: bool,
    pub is_encrypted: bool,
    pub is_solid: bool,
    pub split_before: bool,
    pub split_after: bool,
    /// Byte offset of the compressed data in the stream.
    pub data_offset: u64,
    /// Salt for encrypted files (8 bytes, if present).
    pub salt: Option<[u8; 8]>,
    /// File attributes.
    pub attributes: u32,
}

/// RAR4 end-of-archive header flags.
pub mod end_flags {
    /// More volumes follow.
    pub const NEXT_VOLUME: u16 = 0x0001;
    /// Data CRC and volume number are present after the header.
    pub const DATA_CRC: u16 = 0x0002;
    /// Volume number is present.
    pub const VOLUME_NUMBER: u16 = 0x0004;
}

/// Parsed RAR4 end-of-archive header.
#[derive(Debug, Clone)]
pub struct Rar4EndHeader {
    pub flags: u16,
    pub more_volumes: bool,
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
