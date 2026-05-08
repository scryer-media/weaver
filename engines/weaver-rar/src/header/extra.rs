//! Extra area record parsing for file/service headers.
//!
//! Extra area record types:
//! - 0x01: File encryption
//! - 0x02: File hash (BLAKE2sp)
//! - 0x03: File time (high-precision)
//! - 0x04: File version
//! - 0x05: Redirection (symlinks, hardlinks)
//! - 0x06: Unix owner (uid/gid)
//! - 0x07: Service data
//!
//! Each record: size (vint), type (vint), data (size - type_vint_len bytes).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::crypto::CRYPT5_KDF_LG2_COUNT_MAX;
use crate::error::RarResult;
use crate::types::FileHash;
use crate::vint;

/// Extra area record types.
pub mod record_type {
    pub const FILE_ENCRYPTION: u64 = 0x01;
    pub const FILE_HASH: u64 = 0x02;
    pub const FILE_TIME: u64 = 0x03;
    pub const FILE_VERSION: u64 = 0x04;
    pub const REDIRECTION: u64 = 0x05;
    pub const UNIX_OWNER: u64 = 0x06;
    pub const SERVICE_DATA: u64 = 0x07;
}

const FHEXTRA_HTIME_UNIXTIME: u64 = 0x01;
const FHEXTRA_HTIME_MTIME: u64 = 0x02;
const FHEXTRA_HTIME_CTIME: u64 = 0x04;
const FHEXTRA_HTIME_ATIME: u64 = 0x08;
const FHEXTRA_HTIME_UNIX_NS: u64 = 0x10;

const FHEXTRA_CRYPT_PSWCHECK: u64 = 0x01;
const FHEXTRA_REDIR_DIR: u64 = 0x01;

const WINDOWS_TICKS_PER_SECOND: u64 = 10_000_000;
const WINDOWS_TO_UNIX_SECONDS: u64 = 11_644_473_600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtraAreaOwner {
    File,
    Service,
}

/// A parsed extra area record.
#[derive(Debug, Clone)]
pub enum ExtraRecord {
    /// File encryption parameters.
    FileEncryption {
        version: u64,
        enc_flags: u64,
        kdf_count: u8,
        salt: [u8; 16],
        iv: [u8; 16],
        check_data: Option<[u8; 12]>,
    },
    /// BLAKE2sp hash of the file data.
    FileHash(FileHash),
    /// High-precision file times.
    FileTime {
        mtime: Option<SystemTime>,
        ctime: Option<SystemTime>,
        atime: Option<SystemTime>,
    },
    /// File version.
    FileVersion { version: u64 },
    /// Redirection (symlink, hardlink, junction).
    Redirection {
        redir_type: u64,
        target: String,
        target_is_directory: bool,
    },
    /// Unix owner info.
    UnixOwner { flags: u64 },
    /// Service subdata payload.
    ServiceData(Vec<u8>),
    /// Unknown record type.
    Unknown { record_type: u64 },
}

fn system_time_from_unix(seconds: u32, nanos: u32) -> Option<SystemTime> {
    UNIX_EPOCH.checked_add(Duration::new(seconds as u64, nanos))
}

fn system_time_from_windows(filetime: u64) -> Option<SystemTime> {
    let seconds = filetime / WINDOWS_TICKS_PER_SECOND;
    let nanos = ((filetime % WINDOWS_TICKS_PER_SECOND) * 100) as u32;

    if seconds >= WINDOWS_TO_UNIX_SECONDS {
        UNIX_EPOCH.checked_add(Duration::new(seconds - WINDOWS_TO_UNIX_SECONDS, nanos))
    } else {
        UNIX_EPOCH.checked_sub(Duration::new(WINDOWS_TO_UNIX_SECONDS - seconds, nanos))
    }
}

fn read_u32_le(data: &[u8], pos: &mut usize) -> Option<u32> {
    let end = pos.checked_add(4)?;
    let bytes = data.get(*pos..end)?;
    *pos = end;
    Some(u32::from_le_bytes(bytes.try_into().ok()?))
}

fn read_u64_le(data: &[u8], pos: &mut usize) -> Option<u64> {
    let end = pos.checked_add(8)?;
    let bytes = data.get(*pos..end)?;
    *pos = end;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}

fn validated_password_check(check_data: [u8; 12]) -> Option<[u8; 12]> {
    let digest = Sha256::digest(&check_data[..8]);
    (digest[..4] == check_data[8..12]).then_some(check_data)
}

/// Parse all extra area records from the raw extra area bytes.
pub fn parse_extra_area(data: &[u8], owner: ExtraAreaOwner) -> RarResult<Vec<ExtraRecord>> {
    let mut records = Vec::new();
    let mut pos = 0usize;

    while pos < data.len() {
        let (record_size, size_n) = vint::read_vint(&data[pos..])?;
        pos += size_n;

        if record_size == 0 {
            break;
        }

        let mut record_end = pos + record_size as usize;
        if record_end > data.len() {
            break;
        }

        let mut record_data = &data[pos..record_end];
        let (record_type, type_n) = match vint::read_vint(record_data) {
            Ok(value) => value,
            Err(_) => break,
        };

        // RAR 5.21 and earlier stored service subdata with a size one byte too
        // small. It is always the last extra record, so absorb that trailing
        // byte to preserve the payload without desynchronizing later headers.
        if owner == ExtraAreaOwner::Service
            && record_type == record_type::SERVICE_DATA
            && record_end + 1 == data.len()
        {
            record_end += 1;
            record_data = &data[pos..record_end];
        }

        let record_body = &record_data[type_n..];
        let record = match record_type {
            record_type::FILE_ENCRYPTION => parse_file_encryption(record_body, owner),
            record_type::FILE_HASH => parse_file_hash(record_body),
            record_type::FILE_TIME => parse_file_time(record_body),
            record_type::FILE_VERSION => parse_file_version(record_body),
            record_type::REDIRECTION => parse_redirection(record_body),
            record_type::UNIX_OWNER => ExtraRecord::UnixOwner {
                flags: if record_body.is_empty() {
                    0
                } else {
                    vint::read_vint(record_body)
                        .map(|(value, _)| value)
                        .unwrap_or(0)
                },
            },
            record_type::SERVICE_DATA => ExtraRecord::ServiceData(record_body.to_vec()),
            other => ExtraRecord::Unknown { record_type: other },
        };

        records.push(record);
        pos = record_end;
    }

    Ok(records)
}

fn parse_file_hash(data: &[u8]) -> ExtraRecord {
    if data.is_empty() {
        return ExtraRecord::Unknown {
            record_type: record_type::FILE_HASH,
        };
    }

    let (hash_type, n) = match vint::read_vint(data) {
        Ok(value) => value,
        Err(_) => {
            return ExtraRecord::Unknown {
                record_type: record_type::FILE_HASH,
            };
        }
    };

    if hash_type == 0 && data.len() >= n + 32 {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&data[n..n + 32]);
        ExtraRecord::FileHash(FileHash::Blake2sp(hash))
    } else {
        ExtraRecord::Unknown {
            record_type: record_type::FILE_HASH,
        }
    }
}

fn parse_file_time(data: &[u8]) -> ExtraRecord {
    let (flags, mut pos) = match vint::read_vint(data) {
        Ok(value) => value,
        Err(_) => {
            return ExtraRecord::FileTime {
                mtime: None,
                ctime: None,
                atime: None,
            };
        }
    };

    let unix_time = flags & FHEXTRA_HTIME_UNIXTIME != 0;
    let mut mtime = None;
    let mut ctime = None;
    let mut atime = None;

    if flags & FHEXTRA_HTIME_MTIME != 0 {
        mtime = if unix_time {
            read_u32_le(data, &mut pos).and_then(|secs| system_time_from_unix(secs, 0))
        } else {
            read_u64_le(data, &mut pos).and_then(system_time_from_windows)
        };
    }
    if flags & FHEXTRA_HTIME_CTIME != 0 {
        ctime = if unix_time {
            read_u32_le(data, &mut pos).and_then(|secs| system_time_from_unix(secs, 0))
        } else {
            read_u64_le(data, &mut pos).and_then(system_time_from_windows)
        };
    }
    if flags & FHEXTRA_HTIME_ATIME != 0 {
        atime = if unix_time {
            read_u32_le(data, &mut pos).and_then(|secs| system_time_from_unix(secs, 0))
        } else {
            read_u64_le(data, &mut pos).and_then(system_time_from_windows)
        };
    }

    if unix_time && flags & FHEXTRA_HTIME_UNIX_NS != 0 {
        if flags & FHEXTRA_HTIME_MTIME != 0
            && let Some(ns) = read_u32_le(data, &mut pos).map(|value| value & 0x3fff_ffff)
            && ns < 1_000_000_000
            && let Some(time) = mtime
        {
            mtime = time.checked_add(Duration::from_nanos(ns as u64));
        }
        if flags & FHEXTRA_HTIME_CTIME != 0
            && let Some(ns) = read_u32_le(data, &mut pos).map(|value| value & 0x3fff_ffff)
            && ns < 1_000_000_000
            && let Some(time) = ctime
        {
            ctime = time.checked_add(Duration::from_nanos(ns as u64));
        }
        if flags & FHEXTRA_HTIME_ATIME != 0
            && let Some(ns) = read_u32_le(data, &mut pos).map(|value| value & 0x3fff_ffff)
            && ns < 1_000_000_000
            && let Some(time) = atime
        {
            atime = time.checked_add(Duration::from_nanos(ns as u64));
        }
    }

    ExtraRecord::FileTime {
        mtime,
        ctime,
        atime,
    }
}

fn parse_file_version(data: &[u8]) -> ExtraRecord {
    let (_, n) = match vint::read_vint(data) {
        Ok(value) => value,
        Err(_) => return ExtraRecord::FileVersion { version: 0 },
    };

    let version = if data.len() > n {
        vint::read_vint(&data[n..])
            .map(|(value, _)| value)
            .unwrap_or(0)
    } else {
        0
    };

    ExtraRecord::FileVersion { version }
}

fn parse_redirection(data: &[u8]) -> ExtraRecord {
    let (redir_type, n) = match vint::read_vint(data) {
        Ok(value) => value,
        Err(_) => {
            return ExtraRecord::Redirection {
                redir_type: 0,
                target: String::new(),
                target_is_directory: false,
            };
        }
    };

    let rest = &data[n..];
    let (flags, m) = match vint::read_vint(rest) {
        Ok(value) => value,
        Err(_) => {
            return ExtraRecord::Redirection {
                redir_type,
                target: String::new(),
                target_is_directory: false,
            };
        }
    };
    let rest = &rest[m..];

    let (name_len, k) = match vint::read_vint(rest) {
        Ok(value) => value,
        Err(_) => {
            return ExtraRecord::Redirection {
                redir_type,
                target: String::new(),
                target_is_directory: flags & FHEXTRA_REDIR_DIR != 0,
            };
        }
    };

    let name_start = k;
    let name_end = name_start + name_len as usize;
    let target = if name_end <= rest.len() {
        String::from_utf8_lossy(&rest[name_start..name_end]).into_owned()
    } else {
        String::new()
    };

    ExtraRecord::Redirection {
        redir_type,
        target,
        target_is_directory: flags & FHEXTRA_REDIR_DIR != 0,
    }
}

fn parse_file_encryption(data: &[u8], owner: ExtraAreaOwner) -> ExtraRecord {
    let (version, n) = match vint::read_vint(data) {
        Ok(value) => value,
        Err(_) => {
            return ExtraRecord::FileEncryption {
                version: 0,
                enc_flags: 0,
                kdf_count: 0,
                salt: [0; 16],
                iv: [0; 16],
                check_data: None,
            };
        }
    };

    let rest = &data[n..];
    let (enc_flags, n) = match vint::read_vint(rest) {
        Ok(value) => value,
        Err(_) => {
            return ExtraRecord::FileEncryption {
                version,
                enc_flags: 0,
                kdf_count: 0,
                salt: [0; 16],
                iv: [0; 16],
                check_data: None,
            };
        }
    };

    let rest = &rest[n..];
    if rest.len() < 33 {
        return ExtraRecord::FileEncryption {
            version,
            enc_flags,
            kdf_count: 0,
            salt: [0; 16],
            iv: [0; 16],
            check_data: None,
        };
    }

    let kdf_count = rest[0];
    let mut salt = [0u8; 16];
    salt.copy_from_slice(&rest[1..17]);
    let mut iv = [0u8; 16];
    iv.copy_from_slice(&rest[17..33]);

    let check_data = if kdf_count <= CRYPT5_KDF_LG2_COUNT_MAX
        && enc_flags & FHEXTRA_CRYPT_PSWCHECK != 0
        && rest.len() >= 45
    {
        let mut check_data = [0u8; 12];
        check_data.copy_from_slice(&rest[33..45]);
        let validated = validated_password_check(check_data);
        match (owner, validated) {
            (ExtraAreaOwner::Service, Some(data)) if data[..8] == [0u8; 8] => None,
            (_, value) => value,
        }
    } else {
        None
    };

    ExtraRecord::FileEncryption {
        version,
        enc_flags,
        kdf_count,
        salt,
        iv,
        check_data,
    }
}

/// Check if any extra record indicates file-level encryption.
pub fn is_encrypted(records: &[ExtraRecord]) -> bool {
    records
        .iter()
        .any(|record| matches!(record, ExtraRecord::FileEncryption { .. }))
}

/// Extract BLAKE2sp hash from extra records, if present.
pub fn blake2_hash(records: &[ExtraRecord]) -> Option<FileHash> {
    records.iter().find_map(|record| {
        if let ExtraRecord::FileHash(hash) = record {
            Some(hash.clone())
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vint::encode_vint;

    fn build_extra_record(record_type: u64, body: &[u8]) -> Vec<u8> {
        let type_bytes = encode_vint(record_type);
        let record_size = type_bytes.len() + body.len();
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(record_size as u64));
        data.extend_from_slice(&type_bytes);
        data.extend_from_slice(body);
        data
    }

    #[test]
    fn test_parse_encryption_record() {
        let password_check = [0x55; 8];
        let checksum: [u8; 4] = Sha256::digest(password_check)[..4].try_into().unwrap();

        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(FHEXTRA_CRYPT_PSWCHECK));
        body.push(15);
        body.extend_from_slice(&[0xAA; 16]);
        body.extend_from_slice(&[0xCC; 16]);
        body.extend_from_slice(&password_check);
        body.extend_from_slice(&checksum);

        let data = build_extra_record(record_type::FILE_ENCRYPTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            ExtraRecord::FileEncryption {
                kdf_count,
                check_data,
                ..
            } => {
                assert_eq!(*kdf_count, 15);
                assert!(check_data.is_some());
            }
            other => panic!("expected FileEncryption, got {other:?}"),
        }
        assert!(is_encrypted(&records));
    }

    #[test]
    fn test_parse_high_precision_unix_mtime() {
        let flags = FHEXTRA_HTIME_UNIXTIME | FHEXTRA_HTIME_UNIX_NS | FHEXTRA_HTIME_MTIME;
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(flags));
        body.extend_from_slice(&1_700_000_000u32.to_le_bytes());
        body.extend_from_slice(&123_456_789u32.to_le_bytes());

        let data = build_extra_record(record_type::FILE_TIME, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::FileTime { mtime, .. } => {
                let duration = mtime.unwrap().duration_since(UNIX_EPOCH).unwrap();
                assert_eq!(duration.as_secs(), 1_700_000_000);
                assert_eq!(duration.subsec_nanos(), 123_456_789);
            }
            other => panic!("expected FileTime, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_redirection_directory_flag() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(FHEXTRA_REDIR_DIR));
        body.extend_from_slice(&encode_vint(4));
        body.extend_from_slice(b"dest");

        let data = build_extra_record(record_type::REDIRECTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::Redirection {
                target,
                target_is_directory,
                ..
            } => {
                assert_eq!(target, "dest");
                assert!(*target_is_directory);
            }
            other => panic!("expected Redirection, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_service_subdata_521_quirk() {
        let mut data = build_extra_record(record_type::SERVICE_DATA, b"abc");
        data.pop();
        data.push(b'c');

        let records = parse_extra_area(&data, ExtraAreaOwner::Service).unwrap();
        match &records[0] {
            ExtraRecord::ServiceData(payload) => assert_eq!(payload, b"abc"),
            other => panic!("expected ServiceData, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_blake2_hash() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&[0x42; 32]);
        let data = build_extra_record(record_type::FILE_HASH, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::FileHash(FileHash::Blake2sp(hash)) => {
                assert_eq!(hash, &[0x42; 32]);
            }
            other => panic!("expected FileHash, got {other:?}"),
        }
    }
}
