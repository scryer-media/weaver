//! Archive encryption header (type 4) parsing.
//!
//! When present, all subsequent headers are AES-256-CBC encrypted. This module
//! parses and validates the encryption parameters; archive-level header
//! decryption is performed by the header reader once a password is available.
//!
//! Fields:
//! - Encryption version (vint, 0 = AES-256)
//! - Encryption flags (vint, 0x0001 = password check data present)
//! - KDF count (1 byte, log2 of PBKDF2 iterations)
//! - Salt (16 bytes)
//! - Password check data (12 bytes, if flag 0x0001)

use sha2::{Digest, Sha256};

use crate::crypto::CRYPT5_KDF_LG2_COUNT_MAX;
use crate::error::{RarError, RarResult};
use crate::header::common::{self, RawHeader};

const CRYPT_VERSION: u64 = 0;

fn read_vint_lossy(data: &[u8]) -> (u64, usize) {
    let mut result = 0u64;
    let mut read = 0usize;
    for shift in (0..64).step_by(7) {
        let Some(&byte) = data.get(read) else {
            return (0, read);
        };
        read += 1;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return (result, read);
        }
    }
    (0, read)
}

fn read_byte_lossy(data: &[u8], pos: &mut usize) -> u8 {
    let value = data.get(*pos).copied().unwrap_or(0);
    if *pos < data.len() {
        *pos += 1;
    }
    value
}

fn read_bytes_lossy<const N: usize>(data: &[u8], pos: &mut usize) -> [u8; N] {
    let mut bytes = [0u8; N];
    let available = data.get(*pos..).unwrap_or(&[]);
    let copied = available.len().min(N);
    bytes[..copied].copy_from_slice(&available[..copied]);
    *pos = (*pos).saturating_add(copied).min(data.len());
    bytes
}

/// Parsed encryption header (detection only).
#[derive(Debug, Clone)]
pub struct EncryptionHeader {
    /// Encryption version (0 = AES-256).
    pub version: u64,
    /// Encryption flags.
    pub enc_flags: u64,
    /// KDF iteration count (log2 of PBKDF2 iterations).
    pub kdf_count: u8,
    /// Salt (16 bytes).
    pub salt: [u8; 16],
    /// Whether password check data is present in the raw header.
    pub has_password_check: bool,
    /// Password check data with a valid checksum, if present.
    pub check_data: Option<[u8; 12]>,
}

/// Parse an encryption header from a raw header.
pub fn parse(raw: &RawHeader) -> RarResult<EncryptionHeader> {
    let offset = common::type_specific_offset(raw)?;
    let body = &raw.body[offset..];

    let mut pos = 0;

    let (version, n) = read_vint_lossy(&body[pos..]);
    pos += n;
    if version > CRYPT_VERSION {
        return Err(RarError::UnsupportedEncryption { version });
    }

    let (enc_flags, n) = read_vint_lossy(&body[pos..]);
    pos += n;

    let kdf_count = read_byte_lossy(body, &mut pos);
    if kdf_count > CRYPT5_KDF_LG2_COUNT_MAX {
        return Err(RarError::UnsupportedEncryptionKdf {
            count: kdf_count,
            max: CRYPT5_KDF_LG2_COUNT_MAX,
        });
    }

    let salt = read_bytes_lossy::<16>(body, &mut pos);
    let has_password_check = enc_flags & 0x0001 != 0;
    let check_data = if has_password_check {
        let check_data = read_bytes_lossy::<12>(body, &mut pos);
        let digest = Sha256::digest(&check_data[..8]);
        if digest[..4] == check_data[8..12] {
            Some(check_data)
        } else {
            None
        }
    } else {
        None
    };

    Ok(EncryptionHeader {
        version,
        enc_flags,
        kdf_count,
        salt,
        has_password_check,
        check_data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::common::read_raw_header;
    use crate::vint::encode_vint;

    fn build_raw_encryption_header(type_body: &[u8]) -> Vec<u8> {
        let header_type = 4u64;
        let header_flags = 0u64;

        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(header_type));
        body.extend_from_slice(&encode_vint(header_flags));
        body.extend_from_slice(type_body);

        let header_size = body.len() as u64;
        let header_size_bytes = encode_vint(header_size);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header_size_bytes);
        hasher.update(&body);
        let crc = hasher.finalize();

        let mut data = Vec::new();
        data.extend_from_slice(&crc.to_le_bytes());
        data.extend_from_slice(&header_size_bytes);
        data.extend_from_slice(&body);
        data
    }

    #[test]
    fn test_encryption_header() {
        let password_check = [0xBB; 8];
        let checksum: [u8; 4] = Sha256::digest(password_check)[..4].try_into().unwrap();
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(0)); // version = AES-256
        type_body.extend_from_slice(&encode_vint(0x0001)); // password check present
        type_body.push(15); // kdf_count
        type_body.extend_from_slice(&[0xAA; 16]); // salt
        type_body.extend_from_slice(&password_check);
        type_body.extend_from_slice(&checksum);

        let data = build_raw_encryption_header(&type_body);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let enc = parse(&raw).unwrap();

        assert_eq!(enc.version, 0);
        assert!(enc.has_password_check);
        assert_eq!(enc.kdf_count, 15);
        assert_eq!(enc.salt, [0xAA; 16]);
        assert!(enc.check_data.is_some());
    }

    #[test]
    fn test_encryption_header_truncated_salt_is_padded_like_unrar() {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(0)); // version
        type_body.extend_from_slice(&encode_vint(0)); // flags
        type_body.push(0); // kdf_count
        type_body.extend_from_slice(&[0xAA; 5]);

        let data = build_raw_encryption_header(&type_body);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let enc = parse(&raw).unwrap();

        assert_eq!(enc.version, 0);
        assert_eq!(enc.enc_flags, 0);
        assert_eq!(enc.kdf_count, 0);
        assert_eq!(
            enc.salt,
            [
                0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        assert!(!enc.has_password_check);
        assert_eq!(enc.check_data, None);
    }

    #[test]
    fn test_encryption_header_truncated_password_check_disables_check_like_unrar() {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(0)); // version
        type_body.extend_from_slice(&encode_vint(0x0001)); // password check present
        type_body.push(0); // kdf_count
        type_body.extend_from_slice(&[0xAA; 16]); // salt
        type_body.extend_from_slice(&[0xBB; 3]); // short password check + checksum

        let data = build_raw_encryption_header(&type_body);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let enc = parse(&raw).unwrap();

        assert!(enc.has_password_check);
        assert_eq!(enc.check_data, None);
    }

    #[test]
    fn test_future_encryption_version_is_unsupported_before_rest_of_header() {
        let type_body = encode_vint(CRYPT_VERSION + 1);
        let data = build_raw_encryption_header(&type_body);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();

        let result = parse(&raw);

        assert!(matches!(
            result,
            Err(RarError::UnsupportedEncryption { version }) if version == CRYPT_VERSION + 1
        ));
    }

    #[test]
    fn test_oversized_kdf_count_is_unsupported_before_salt() {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(0));
        type_body.extend_from_slice(&encode_vint(0));
        type_body.push(CRYPT5_KDF_LG2_COUNT_MAX + 1);
        let data = build_raw_encryption_header(&type_body);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();

        let result = parse(&raw);

        assert!(matches!(
            result,
            Err(RarError::UnsupportedEncryptionKdf { count, max })
                if count == CRYPT5_KDF_LG2_COUNT_MAX + 1
                    && max == CRYPT5_KDF_LG2_COUNT_MAX
        ));
    }
}
