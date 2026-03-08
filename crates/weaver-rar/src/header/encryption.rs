//! Archive encryption header (type 4) parsing.
//!
//! When present, all subsequent headers are AES-256-CBC encrypted.
//! We only detect encryption and return an error -- decryption is not supported.
//!
//! Fields:
//! - Encryption version (vint, 0 = AES-256)
//! - Encryption flags (vint, 0x0001 = password check data present)
//! - KDF count (1 byte, log2 of PBKDF2 iterations)
//! - Salt (16 bytes)
//! - Password check data (12 bytes, if flag 0x0001)

use crate::error::RarResult;
use crate::header::common::{self, RawHeader};
use crate::vint;

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
    /// Whether password check data is present.
    pub has_password_check: bool,
}

/// Parse an encryption header from a raw header.
pub fn parse(raw: &RawHeader) -> RarResult<EncryptionHeader> {
    let offset = common::type_specific_offset(raw)?;
    let body = &raw.body[offset..];

    let mut pos = 0;

    let (version, n) = vint::read_vint(&body[pos..])?;
    pos += n;

    let (enc_flags, n) = vint::read_vint(&body[pos..])?;
    pos += n;

    // Ensure body has enough bytes for KDF count (1) + salt (16) = 17 bytes minimum.
    if body.len() < pos + 17 {
        return Err(crate::error::RarError::CorruptArchive {
            detail: "encryption header body too short for KDF + salt".into(),
        });
    }

    let kdf_count = body[pos];
    pos += 1;

    let mut salt = [0u8; 16];
    salt.copy_from_slice(&body[pos..pos + 16]);
    pos += 16;
    let _ = pos;

    let has_password_check = enc_flags & 0x0001 != 0;

    Ok(EncryptionHeader {
        version,
        enc_flags,
        kdf_count,
        salt,
        has_password_check,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::common::read_raw_header;
    use crate::vint::encode_vint;

    #[test]
    fn test_encryption_header() {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(0)); // version = AES-256
        type_body.extend_from_slice(&encode_vint(0x0001)); // password check present
        type_body.push(15); // kdf_count
        type_body.extend_from_slice(&[0xAA; 16]); // salt
        type_body.extend_from_slice(&[0xBB; 12]); // password check data

        let header_type = 4u64;
        let header_flags = 0u64;

        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(header_type));
        body.extend_from_slice(&encode_vint(header_flags));
        body.extend_from_slice(&type_body);

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

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let enc = parse(&raw).unwrap();

        assert_eq!(enc.version, 0);
        assert!(enc.has_password_check);
        assert_eq!(enc.kdf_count, 15);
        assert_eq!(enc.salt, [0xAA; 16]);
    }

    #[test]
    fn test_encryption_header_truncated() {
        // Build an encryption header with body too short for KDF + salt.
        // Only provide version and flags vints, then 5 bytes (need 17).
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(0)); // version
        type_body.extend_from_slice(&encode_vint(0)); // flags
        type_body.extend_from_slice(&[0xAA; 5]); // too short for kdf(1) + salt(16)

        let header_type = 4u64;
        let header_flags = 0u64;

        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(header_type));
        body.extend_from_slice(&encode_vint(header_flags));
        body.extend_from_slice(&type_body);

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

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let result = parse(&raw);
        assert!(
            matches!(result, Err(crate::error::RarError::CorruptArchive { ref detail }) if detail.contains("too short")),
            "expected CorruptArchive error for truncated encryption header, got: {result:?}"
        );
    }
}
