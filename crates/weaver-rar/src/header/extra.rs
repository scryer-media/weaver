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
    /// High-precision file time record (raw, not fully decoded).
    FileTime {
        flags: u64,
    },
    /// File version.
    FileVersion {
        version: u64,
    },
    /// Redirection (symlink, hardlink, junction).
    Redirection {
        redir_type: u64,
        target: String,
    },
    /// Unix owner info.
    UnixOwner {
        flags: u64,
    },
    /// Service data.
    ServiceData,
    /// Unknown record type.
    Unknown {
        record_type: u64,
    },
}

/// Parse all extra area records from the raw extra area bytes.
pub fn parse_extra_area(data: &[u8]) -> RarResult<Vec<ExtraRecord>> {
    let mut records = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let (record_size, n) = vint::read_vint(&data[pos..])?;
        pos += n;

        if record_size == 0 {
            break;
        }

        let record_end = pos + record_size as usize;
        if record_end > data.len() {
            break; // Truncated record, stop parsing
        }

        let record_data = &data[pos..record_end];
        let (record_type, type_n) = vint::read_vint(record_data)?;
        let record_body = &record_data[type_n..];

        let record = match record_type {
            record_type::FILE_ENCRYPTION => parse_file_encryption(record_body),
            record_type::FILE_HASH => parse_file_hash(record_body),
            record_type::FILE_TIME => parse_file_time(record_body),
            record_type::FILE_VERSION => parse_file_version(record_body),
            record_type::REDIRECTION => parse_redirection(record_body),
            record_type::UNIX_OWNER => ExtraRecord::UnixOwner {
                flags: if record_body.is_empty() {
                    0
                } else {
                    vint::read_vint(record_body).map(|(v, _)| v).unwrap_or(0)
                },
            },
            record_type::SERVICE_DATA => ExtraRecord::ServiceData,
            other => ExtraRecord::Unknown {
                record_type: other,
            },
        };

        records.push(record);
        pos = record_end;
    }

    Ok(records)
}

fn parse_file_hash(data: &[u8]) -> ExtraRecord {
    // Hash type (vint): 0 = BLAKE2sp
    if data.is_empty() {
        return ExtraRecord::Unknown { record_type: 0x02 };
    }

    let (hash_type, n) = match vint::read_vint(data) {
        Ok(v) => v,
        Err(_) => return ExtraRecord::Unknown { record_type: 0x02 },
    };

    if hash_type == 0 && data.len() >= n + 32 {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&data[n..n + 32]);
        ExtraRecord::FileHash(FileHash::Blake2sp(hash))
    } else {
        ExtraRecord::Unknown { record_type: 0x02 }
    }
}

fn parse_file_time(data: &[u8]) -> ExtraRecord {
    let flags = if data.is_empty() {
        0
    } else {
        vint::read_vint(data).map(|(v, _)| v).unwrap_or(0)
    };
    ExtraRecord::FileTime { flags }
}

fn parse_file_version(data: &[u8]) -> ExtraRecord {
    // Flags (vint), then version (vint)
    if data.is_empty() {
        return ExtraRecord::FileVersion { version: 0 };
    }
    let (_, n) = match vint::read_vint(data) {
        Ok(v) => v,
        Err(_) => return ExtraRecord::FileVersion { version: 0 },
    };
    let version = if data.len() > n {
        vint::read_vint(&data[n..]).map(|(v, _)| v).unwrap_or(0)
    } else {
        0
    };
    ExtraRecord::FileVersion { version }
}

fn parse_redirection(data: &[u8]) -> ExtraRecord {
    if data.is_empty() {
        return ExtraRecord::Redirection {
            redir_type: 0,
            target: String::new(),
        };
    }
    let (redir_type, n) = match vint::read_vint(data) {
        Ok(v) => v,
        Err(_) => {
            return ExtraRecord::Redirection {
                redir_type: 0,
                target: String::new(),
            }
        }
    };

    let rest = &data[n..];
    // flags (vint)
    let (_, m) = match vint::read_vint(rest) {
        Ok(v) => v,
        Err(_) => {
            return ExtraRecord::Redirection {
                redir_type,
                target: String::new(),
            }
        }
    };
    let rest = &rest[m..];

    // name length (vint) + name
    let (name_len, k) = match vint::read_vint(rest) {
        Ok(v) => v,
        Err(_) => {
            return ExtraRecord::Redirection {
                redir_type,
                target: String::new(),
            }
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
    }
}

fn parse_file_encryption(data: &[u8]) -> ExtraRecord {
    // version (vint)
    let (version, n) = match vint::read_vint(data) {
        Ok(v) => v,
        Err(_) => {
            return ExtraRecord::FileEncryption {
                version: 0,
                enc_flags: 0,
                kdf_count: 0,
                salt: [0; 16],
                iv: [0; 16],
                check_data: None,
            }
        }
    };
    let rest = &data[n..];

    // enc_flags (vint)
    let (enc_flags, n) = match vint::read_vint(rest) {
        Ok(v) => v,
        Err(_) => {
            return ExtraRecord::FileEncryption {
                version,
                enc_flags: 0,
                kdf_count: 0,
                salt: [0; 16],
                iv: [0; 16],
                check_data: None,
            }
        }
    };
    let rest = &rest[n..];

    // kdf_count (1 byte) + salt (16 bytes) + iv (16 bytes) = 33 bytes minimum
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

    let check_data = if enc_flags & 0x0001 != 0 && rest.len() >= 33 + 12 {
        let mut cd = [0u8; 12];
        cd.copy_from_slice(&rest[33..45]);
        Some(cd)
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
        .any(|r| matches!(r, ExtraRecord::FileEncryption { .. }))
}

/// Extract BLAKE2sp hash from extra records, if present.
pub fn blake2_hash(records: &[ExtraRecord]) -> Option<FileHash> {
    records.iter().find_map(|r| {
        if let ExtraRecord::FileHash(hash) = r {
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
        let data = build_extra_record(record_type::FILE_ENCRYPTION, &[0; 20]);
        let records = parse_extra_area(&data).unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0], ExtraRecord::FileEncryption { .. }));
        assert!(is_encrypted(&records));
    }

    #[test]
    fn test_parse_blake2_hash() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(0)); // hash type = BLAKE2sp
        body.extend_from_slice(&[0x42; 32]); // 32-byte hash
        let data = build_extra_record(record_type::FILE_HASH, &body);
        let records = parse_extra_area(&data).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            ExtraRecord::FileHash(FileHash::Blake2sp(hash)) => {
                assert_eq!(hash, &[0x42; 32]);
            }
            other => panic!("expected FileHash, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_multiple_records() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_extra_record(record_type::FILE_TIME, &[0x00]));
        data.extend_from_slice(&build_extra_record(record_type::FILE_ENCRYPTION, &[0; 10]));
        let records = parse_extra_area(&data).unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(records[0], ExtraRecord::FileTime { .. }));
        assert!(matches!(records[1], ExtraRecord::FileEncryption { .. }));
    }

    #[test]
    fn test_empty_extra_area() {
        let records = parse_extra_area(&[]).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_unknown_record_type() {
        let data = build_extra_record(0xFF, &[0x01, 0x02]);
        let records = parse_extra_area(&data).unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(
            records[0],
            ExtraRecord::Unknown {
                record_type: 0xFF
            }
        ));
    }
}
