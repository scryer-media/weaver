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
use crate::types::{FileHash, UnixOwnerInfo};

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
const MAXPATHSIZE: u64 = 0x10000;
const UNIX_OWNER_NAME_MAX: usize = 255;
const OWNER_USER_NAME: u64 = 0x01;
const OWNER_GROUP_NAME: u64 = 0x02;
const OWNER_USER_UID: u64 = 0x04;
const OWNER_GROUP_GID: u64 = 0x08;

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
        target_raw: Vec<u8>,
        target_is_directory: bool,
    },
    /// Unix owner info.
    UnixOwner { owner: UnixOwnerInfo },
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

fn read_raw_u32_lossy(data: &[u8], pos: &mut usize) -> u32 {
    let Some(end) = pos.checked_add(4) else {
        return 0;
    };
    if end <= data.len() {
        let value = u32::from_le_bytes(data[*pos..end].try_into().unwrap_or([0; 4]));
        *pos = end;
        value
    } else {
        0
    }
}

fn read_raw_u64_lossy(data: &[u8], pos: &mut usize) -> u64 {
    let low = read_raw_u32_lossy(data, pos);
    let high = read_raw_u32_lossy(data, pos);
    u64::from(low) | (u64::from(high) << 32)
}

fn validated_password_check(check_data: [u8; 12]) -> Option<[u8; 12]> {
    let digest = Sha256::digest(&check_data[..8]);
    (digest[..4] == check_data[8..12]).then_some(check_data)
}

/// Parse all extra area records from the raw extra area bytes.
pub fn parse_extra_area(data: &[u8], owner: ExtraAreaOwner) -> RarResult<Vec<ExtraRecord>> {
    let mut records = Vec::new();
    let mut pos = 0usize;

    while data.len().saturating_sub(pos) >= 2 {
        let (record_size, size_n) = read_raw_vint_lossy(&data[pos..]);
        pos += size_n;

        if record_size == 0 || pos == data.len() {
            break;
        }

        let Ok(record_size) = usize::try_from(record_size) else {
            break;
        };
        let Some(mut record_end) = pos.checked_add(record_size) else {
            break;
        };
        if record_end > data.len() {
            break;
        }

        let mut record_data = &data[pos..record_end];
        let (record_type, type_n) = read_raw_vint_lossy(record_data);

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
            record_type::UNIX_OWNER => parse_unix_owner(record_body),
            record_type::SERVICE_DATA => ExtraRecord::ServiceData(record_body.to_vec()),
            other => ExtraRecord::Unknown { record_type: other },
        };

        records.push(record);
        pos = record_end;
    }

    Ok(records)
}

fn parse_file_hash(data: &[u8]) -> ExtraRecord {
    let (hash_type, n) = read_raw_vint_lossy(data);

    if hash_type == 0 {
        let mut hash = [0u8; 32];
        if data.len() > n {
            let hash_len = (data.len() - n).min(hash.len());
            hash[..hash_len].copy_from_slice(&data[n..n + hash_len]);
        }
        ExtraRecord::FileHash(FileHash::Blake2sp(hash))
    } else {
        ExtraRecord::Unknown {
            record_type: record_type::FILE_HASH,
        }
    }
}

fn read_raw_vint_lossy(data: &[u8]) -> (u64, usize) {
    let mut result = 0u64;
    let mut read = 0usize;
    for shift in (0..64).step_by(7) {
        let Some(&byte) = data.get(read) else {
            return (0, read);
        };
        read += 1;
        result += u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return (result, read);
        }
    }
    (0, read)
}

fn parse_file_time(data: &[u8]) -> ExtraRecord {
    if data.len() < 5 {
        return ExtraRecord::FileTime {
            mtime: None,
            ctime: None,
            atime: None,
        };
    }

    let (flags, mut pos) = read_raw_vint_lossy(data);

    let unix_time = flags & FHEXTRA_HTIME_UNIXTIME != 0;
    let mut mtime = None;
    let mut ctime = None;
    let mut atime = None;

    if flags & FHEXTRA_HTIME_MTIME != 0 {
        mtime = if unix_time {
            system_time_from_unix(read_raw_u32_lossy(data, &mut pos), 0)
        } else {
            system_time_from_windows(read_raw_u64_lossy(data, &mut pos))
        };
    }
    if flags & FHEXTRA_HTIME_CTIME != 0 {
        ctime = if unix_time {
            system_time_from_unix(read_raw_u32_lossy(data, &mut pos), 0)
        } else {
            system_time_from_windows(read_raw_u64_lossy(data, &mut pos))
        };
    }
    if flags & FHEXTRA_HTIME_ATIME != 0 {
        atime = if unix_time {
            system_time_from_unix(read_raw_u32_lossy(data, &mut pos), 0)
        } else {
            system_time_from_windows(read_raw_u64_lossy(data, &mut pos))
        };
    }

    if unix_time && flags & FHEXTRA_HTIME_UNIX_NS != 0 {
        if flags & FHEXTRA_HTIME_MTIME != 0 {
            let ns = read_raw_u32_lossy(data, &mut pos) & 0x3fff_ffff;
            if ns < 1_000_000_000
                && let Some(time) = mtime
            {
                mtime = time.checked_add(Duration::from_nanos(ns as u64));
            }
        }
        if flags & FHEXTRA_HTIME_CTIME != 0 {
            let ns = read_raw_u32_lossy(data, &mut pos) & 0x3fff_ffff;
            if ns < 1_000_000_000
                && let Some(time) = ctime
            {
                ctime = time.checked_add(Duration::from_nanos(ns as u64));
            }
        }
        if flags & FHEXTRA_HTIME_ATIME != 0 {
            let ns = read_raw_u32_lossy(data, &mut pos) & 0x3fff_ffff;
            if ns < 1_000_000_000
                && let Some(time) = atime
            {
                atime = time.checked_add(Duration::from_nanos(ns as u64));
            }
        }
    }

    ExtraRecord::FileTime {
        mtime,
        ctime,
        atime,
    }
}

fn parse_file_version(data: &[u8]) -> ExtraRecord {
    if data.is_empty() {
        return ExtraRecord::FileVersion { version: 0 };
    }

    let (_, n) = read_raw_vint_lossy(data);
    let (version, _) = read_raw_vint_lossy(data.get(n..).unwrap_or(&[]));

    ExtraRecord::FileVersion { version }
}

fn parse_redirection(data: &[u8]) -> ExtraRecord {
    let (redir_type, n) = read_raw_vint_lossy(data);
    let rest = &data[n..];
    let (flags, m) = read_raw_vint_lossy(rest);
    let rest = &rest[m..];
    let (name_len, k) = read_raw_vint_lossy(rest);

    let name_start = k;
    if name_len == 0 || name_len >= MAXPATHSIZE {
        return invalid_redirection_record();
    }
    let Ok(name_len) = usize::try_from(name_len) else {
        return invalid_redirection_record();
    };
    let mut target_bytes = vec![0u8; name_len];
    if name_start < rest.len() {
        let copy_len = (rest.len() - name_start).min(name_len);
        target_bytes[..copy_len].copy_from_slice(&rest[name_start..name_start + copy_len]);
    }
    let (target, target_raw) =
        crate::header::common::decode_utf8_prefix_until_nul_with_unrar_bytes(&target_bytes);

    ExtraRecord::Redirection {
        redir_type,
        target,
        target_raw,
        target_is_directory: flags & FHEXTRA_REDIR_DIR != 0,
    }
}

fn invalid_redirection_record() -> ExtraRecord {
    ExtraRecord::Unknown {
        record_type: record_type::REDIRECTION,
    }
}

fn parse_unix_owner(data: &[u8]) -> ExtraRecord {
    let (flags, mut pos) = read_raw_vint_lossy(data);

    let mut owner = UnixOwnerInfo::default();

    if flags & OWNER_USER_NAME != 0
        && let Some(name) = read_unix_owner_name(data, &mut pos)
    {
        owner.user_name = Some(String::from_utf8_lossy(&name).into_owned());
        owner.user_name_raw = Some(name);
    }

    if flags & OWNER_GROUP_NAME != 0
        && let Some(name) = read_unix_owner_name(data, &mut pos)
    {
        owner.group_name = Some(String::from_utf8_lossy(&name).into_owned());
        owner.group_name_raw = Some(name);
    }

    if flags & OWNER_USER_UID != 0 {
        let (uid, n) = read_raw_vint_lossy(data.get(pos..).unwrap_or(&[]));
        owner.uid = Some(uid);
        pos = pos.saturating_add(n).min(data.len());
    }

    if flags & OWNER_GROUP_GID != 0 {
        let (gid, _) = read_raw_vint_lossy(data.get(pos..).unwrap_or(&[]));
        owner.gid = Some(gid);
    }

    ExtraRecord::UnixOwner { owner }
}

fn read_unix_owner_name(data: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let (name_len, n) = read_raw_vint_lossy(data.get(*pos..).unwrap_or(&[]));
    *pos = (*pos).saturating_add(n).min(data.len());
    let copy_len = usize::try_from(name_len)
        .unwrap_or(usize::MAX)
        .min(UNIX_OWNER_NAME_MAX);
    let mut bytes = vec![0; copy_len];
    let available = data.get(*pos..).unwrap_or(&[]);
    let copied = available.len().min(copy_len);
    bytes[..copied].copy_from_slice(&available[..copied]);
    *pos = (*pos).saturating_add(copied).min(data.len());
    let nul_end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    Some(bytes[..nul_end].to_vec())
}

fn parse_file_encryption(data: &[u8], owner: ExtraAreaOwner) -> ExtraRecord {
    let (version, n) = read_raw_vint_lossy(data);
    let rest = &data[n..];
    let (enc_flags, n) = read_raw_vint_lossy(rest);
    let rest = &rest[n..];
    let kdf_count = rest.first().copied().unwrap_or(0);
    let mut salt = [0u8; 16];
    if rest.len() > 1 {
        let salt_len = (rest.len() - 1).min(salt.len());
        salt[..salt_len].copy_from_slice(&rest[1..1 + salt_len]);
    }
    let mut iv = [0u8; 16];
    if rest.len() > 17 {
        let iv_len = (rest.len() - 17).min(iv.len());
        iv[..iv_len].copy_from_slice(&rest[17..17 + iv_len]);
    }

    let check_data =
        if kdf_count <= CRYPT5_KDF_LG2_COUNT_MAX && enc_flags & FHEXTRA_CRYPT_PSWCHECK != 0 {
            let mut check_data = [0u8; 12];
            if rest.len() > 33 {
                let check_len = (rest.len() - 33).min(check_data.len());
                check_data[..check_len].copy_from_slice(&rest[33..33 + check_len]);
            }
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
                version,
                kdf_count,
                check_data,
                ..
            } => {
                assert_eq!(*version, 0);
                assert_eq!(*kdf_count, 15);
                assert!(check_data.is_some());
            }
            other => panic!("expected FileEncryption, got {other:?}"),
        }
        assert!(is_encrypted(&records));
    }

    #[test]
    fn test_parse_future_encryption_version_stays_encrypted() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(0));
        body.push(15);
        body.extend_from_slice(&[0xAA; 16]);
        body.extend_from_slice(&[0xCC; 16]);

        let data = build_extra_record(record_type::FILE_ENCRYPTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileEncryption { version, .. } => assert_eq!(*version, 1),
            other => panic!("expected FileEncryption, got {other:?}"),
        }
        assert!(is_encrypted(&records));
    }

    #[test]
    fn test_parse_encryption_reads_oversized_kdf_before_missing_salt() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(0));
        body.push(CRYPT5_KDF_LG2_COUNT_MAX + 1);

        let data = build_extra_record(record_type::FILE_ENCRYPTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileEncryption {
                kdf_count,
                salt,
                iv,
                ..
            } => {
                assert_eq!(*kdf_count, CRYPT5_KDF_LG2_COUNT_MAX + 1);
                assert_eq!(*salt, [0; 16]);
                assert_eq!(*iv, [0; 16]);
            }
            other => panic!("expected FileEncryption, got {other:?}"),
        }
        assert!(is_encrypted(&records));
    }

    #[test]
    fn test_parse_encryption_zero_fills_short_salt_and_iv_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(0));
        body.push(4);
        body.extend_from_slice(&[1, 2, 3]);

        let data = build_extra_record(record_type::FILE_ENCRYPTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileEncryption {
                kdf_count,
                salt,
                iv,
                ..
            } => {
                let mut expected_salt = [0u8; 16];
                expected_salt[..3].copy_from_slice(&[1, 2, 3]);
                assert_eq!(*kdf_count, 4);
                assert_eq!(*salt, expected_salt);
                assert_eq!(*iv, [0; 16]);
            }
            other => panic!("expected FileEncryption, got {other:?}"),
        }
        assert!(is_encrypted(&records));
    }

    #[test]
    fn test_parse_encryption_truncated_version_vint_defaults_like_unrar() {
        let data = build_extra_record(record_type::FILE_ENCRYPTION, &[0x80]);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileEncryption {
                version,
                enc_flags,
                kdf_count,
                salt,
                iv,
                check_data,
            } => {
                assert_eq!(*version, 0);
                assert_eq!(*enc_flags, 0);
                assert_eq!(*kdf_count, 0);
                assert_eq!(*salt, [0; 16]);
                assert_eq!(*iv, [0; 16]);
                assert!(check_data.is_none());
            }
            other => panic!("expected FileEncryption, got {other:?}"),
        }
        assert!(is_encrypted(&records));
    }

    #[test]
    fn test_parse_encryption_truncated_flags_vint_defaults_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(0));
        body.push(0x80);

        let data = build_extra_record(record_type::FILE_ENCRYPTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileEncryption {
                version,
                enc_flags,
                kdf_count,
                salt,
                iv,
                check_data,
            } => {
                assert_eq!(*version, 0);
                assert_eq!(*enc_flags, 0);
                assert_eq!(*kdf_count, 0);
                assert_eq!(*salt, [0; 16]);
                assert_eq!(*iv, [0; 16]);
                assert!(check_data.is_none());
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
    fn test_parse_file_time_ignores_field_shorter_than_unrar_minimum() {
        let flags = FHEXTRA_HTIME_UNIXTIME | FHEXTRA_HTIME_MTIME;
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(flags));
        body.extend_from_slice(&[1, 0, 0]);

        let data = build_extra_record(record_type::FILE_TIME, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileTime {
                mtime,
                ctime,
                atime,
            } => {
                assert!(mtime.is_none());
                assert!(ctime.is_none());
                assert!(atime.is_none());
            }
            other => panic!("expected FileTime, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_file_time_missing_flagged_unix_value_defaults_to_epoch_like_unrar() {
        let flags = FHEXTRA_HTIME_UNIXTIME | FHEXTRA_HTIME_MTIME | FHEXTRA_HTIME_CTIME;
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(flags));
        body.extend_from_slice(&123u32.to_le_bytes());

        let data = build_extra_record(record_type::FILE_TIME, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileTime { mtime, ctime, .. } => {
                assert_eq!(
                    mtime.unwrap().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    123
                );
                assert_eq!(*ctime, Some(UNIX_EPOCH));
            }
            other => panic!("expected FileTime, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_file_version_reads_version_after_flags() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(0x42));
        body.extend_from_slice(&encode_vint(7));

        let data = build_extra_record(record_type::FILE_VERSION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::FileVersion { version } => assert_eq!(*version, 7),
            other => panic!("expected FileVersion, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_file_version_truncated_flags_vint_defaults_to_zero_like_unrar() {
        let data = build_extra_record(record_type::FILE_VERSION, &[0x80]);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::FileVersion { version } => assert_eq!(*version, 0),
            other => panic!("expected FileVersion, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_file_version_truncated_version_vint_defaults_to_zero_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(0));
        body.push(0x80);

        let data = build_extra_record(record_type::FILE_VERSION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::FileVersion { version } => assert_eq!(*version, 0),
            other => panic!("expected FileVersion, got {other:?}"),
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
                target_raw,
                target_is_directory,
                ..
            } => {
                assert_eq!(target, "dest");
                assert_eq!(target_raw, b"dest");
                assert!(*target_is_directory);
            }
            other => panic!("expected Redirection, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_redirection_ignores_empty_target_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(0));

        let data = build_extra_record(record_type::REDIRECTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        assert!(matches!(
            records[0],
            ExtraRecord::Unknown {
                record_type: record_type::REDIRECTION
            }
        ));
    }

    #[test]
    fn test_parse_redirection_ignores_maxpathsize_target_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(MAXPATHSIZE));

        let data = build_extra_record(record_type::REDIRECTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        assert!(matches!(
            records[0],
            ExtraRecord::Unknown {
                record_type: record_type::REDIRECTION
            }
        ));
    }

    #[test]
    fn test_parse_redirection_zero_fills_and_truncates_short_target_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(4));
        body.extend_from_slice(b"xy");

        let data = build_extra_record(record_type::REDIRECTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::Redirection {
                target, target_raw, ..
            } => {
                assert_eq!(target, "xy");
                assert_eq!(target_raw, b"xy");
            }
            other => panic!("expected Redirection, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_redirection_truncates_embedded_nul_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(7));
        body.extend_from_slice(b"ab\0cdef");

        let data = build_extra_record(record_type::REDIRECTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::Redirection {
                target, target_raw, ..
            } => {
                assert_eq!(target, "ab");
                assert_eq!(target_raw, b"ab");
            }
            other => panic!("expected Redirection, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_redirection_invalid_utf8_target_keeps_prefix_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(11));
        body.extend_from_slice(b"ab/\xffignored");

        let data = build_extra_record(record_type::REDIRECTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::Redirection {
                target, target_raw, ..
            } => {
                assert_eq!(target, "ab/");
                assert_eq!(target_raw, b"ab/");
            }
            other => panic!("expected Redirection, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_redirection_overlong_utf8_target_decodes_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(6));
        body.extend_from_slice(b"ab\xc0\xafcd");

        let data = build_extra_record(record_type::REDIRECTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::Redirection {
                target, target_raw, ..
            } => {
                assert_eq!(target, "ab/cd");
                assert_eq!(target_raw, b"ab/cd");
            }
            other => panic!("expected Redirection, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_redirection_surrogate_target_preserves_unrar_bytes() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(5));
        body.extend_from_slice(b"a\xed\xa0\x80b");

        let data = build_extra_record(record_type::REDIRECTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::Redirection {
                target, target_raw, ..
            } => {
                assert_eq!(target, "a\u{fffd}b");
                assert_eq!(target_raw, b"a\xed\xa0\x80b");
            }
            other => panic!("expected Redirection, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_redirection_huge_target_len_does_not_panic() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(1));
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(u64::MAX));

        let data = build_extra_record(record_type::REDIRECTION, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        assert!(matches!(
            records[0],
            ExtraRecord::Unknown {
                record_type: record_type::REDIRECTION
            }
        ));
    }

    #[test]
    fn test_parse_extra_area_huge_record_size_does_not_panic() {
        let mut data = encode_vint(u64::MAX);
        data.push(record_type::REDIRECTION as u8);

        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_parse_extra_area_truncated_size_vint_stops_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(3));
        let mut data = build_extra_record(record_type::FILE_VERSION, &body);
        data.push(0x80);

        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            ExtraRecord::FileVersion { version } => assert_eq!(*version, 3),
            other => panic!("expected FileVersion, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unix_owner_names_are_capped_like_unrar() {
        let long_name = vec![b'a'; UNIX_OWNER_NAME_MAX + 10];
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(OWNER_USER_NAME));
        body.extend_from_slice(&encode_vint(long_name.len() as u64));
        body.extend_from_slice(&long_name);

        let data = build_extra_record(record_type::UNIX_OWNER, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::UnixOwner { owner } => {
                let name = owner.user_name.as_deref().unwrap();
                assert_eq!(name.len(), UNIX_OWNER_NAME_MAX);
                assert!(name.bytes().all(|byte| byte == b'a'));
            }
            other => panic!("expected UnixOwner, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unix_owner_name_truncates_at_nul_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(OWNER_USER_NAME));
        body.extend_from_slice(&encode_vint(9));
        body.extend_from_slice(b"alice\0zzz");

        let data = build_extra_record(record_type::UNIX_OWNER, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::UnixOwner { owner } => {
                assert_eq!(owner.user_name.as_deref(), Some("alice"));
                assert_eq!(owner.user_name_raw.as_deref(), Some(&b"alice"[..]));
            }
            other => panic!("expected UnixOwner, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unix_owner_preserves_non_utf8_raw_bytes_for_lookup_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(OWNER_USER_NAME | OWNER_GROUP_NAME));
        body.extend_from_slice(&encode_vint(6));
        body.extend_from_slice(b"al\xffice");
        body.extend_from_slice(&encode_vint(5));
        body.extend_from_slice(b"gr\xf0up");

        let data = build_extra_record(record_type::UNIX_OWNER, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::UnixOwner { owner } => {
                assert_eq!(owner.user_name_raw.as_deref(), Some(&b"al\xffice"[..]));
                assert_eq!(owner.group_name_raw.as_deref(), Some(&b"gr\xf0up"[..]));
                assert_eq!(owner.user_name.as_deref(), Some("al\u{fffd}ice"));
                assert_eq!(owner.group_name.as_deref(), Some("gr\u{fffd}up"));
            }
            other => panic!("expected UnixOwner, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unix_owner_huge_name_length_does_not_panic() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(OWNER_USER_NAME));
        body.extend_from_slice(&encode_vint(u64::MAX));
        body.extend_from_slice(&vec![b'a'; UNIX_OWNER_NAME_MAX]);

        let data = build_extra_record(record_type::UNIX_OWNER, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::UnixOwner { owner } => {
                assert_eq!(
                    owner.user_name.as_deref().unwrap().len(),
                    UNIX_OWNER_NAME_MAX
                );
            }
            other => panic!("expected UnixOwner, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unix_owner_zero_fills_short_name_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(OWNER_USER_NAME));
        body.extend_from_slice(&encode_vint(5));
        body.extend_from_slice(b"ab");

        let data = build_extra_record(record_type::UNIX_OWNER, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::UnixOwner { owner } => {
                assert_eq!(owner.user_name.as_deref(), Some("ab"));
            }
            other => panic!("expected UnixOwner, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unix_owner_truncated_name_length_vint_defaults_to_empty_like_unrar() {
        let body = vec![OWNER_USER_NAME as u8, 0x80];

        let data = build_extra_record(record_type::UNIX_OWNER, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::UnixOwner { owner } => {
                assert_eq!(owner.user_name.as_deref(), Some(""));
            }
            other => panic!("expected UnixOwner, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unix_owner_numeric_ids_default_to_zero_on_missing_vint_like_unrar() {
        let body = encode_vint(OWNER_USER_UID | OWNER_GROUP_GID);

        let data = build_extra_record(record_type::UNIX_OWNER, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();
        match &records[0] {
            ExtraRecord::UnixOwner { owner } => {
                assert_eq!(owner.uid, Some(0));
                assert_eq!(owner.gid, Some(0));
            }
            other => panic!("expected UnixOwner, got {other:?}"),
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

    #[test]
    fn test_parse_blake2_hash_zero_fills_short_digest_like_unrar() {
        let body = [0, 0x42, 0x43, 0x44];
        let data = build_extra_record(record_type::FILE_HASH, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileHash(FileHash::Blake2sp(hash)) => {
                let mut expected = [0u8; 32];
                expected[..3].copy_from_slice(&[0x42, 0x43, 0x44]);
                assert_eq!(*hash, expected);
            }
            other => panic!("expected FileHash, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_blake2_hash_empty_body_defaults_to_zero_hash_like_unrar() {
        let data = build_extra_record(record_type::FILE_HASH, &[]);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileHash(FileHash::Blake2sp(hash)) => assert_eq!(*hash, [0; 32]),
            other => panic!("expected FileHash, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_blake2_hash_truncated_type_vint_defaults_to_zero_hash_like_unrar() {
        let body = [0x80, 0x80];
        let data = build_extra_record(record_type::FILE_HASH, &body);
        let records = parse_extra_area(&data, ExtraAreaOwner::File).unwrap();

        match &records[0] {
            ExtraRecord::FileHash(FileHash::Blake2sp(hash)) => assert_eq!(*hash, [0; 32]),
            other => panic!("expected FileHash, got {other:?}"),
        }
    }
}
