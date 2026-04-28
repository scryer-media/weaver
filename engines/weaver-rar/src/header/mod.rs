//! Header parsing dispatcher for RAR5 archives.
//!
//! Iterates through all headers in a RAR5 archive, dispatching to
//! type-specific parsers.

pub mod common;
pub mod compression_info;
pub mod encryption;
pub mod end_archive;
pub mod extra;
pub mod file;
pub mod main_archive;
pub mod service;

use std::io::{Read, Seek, SeekFrom};

use tracing::{debug, warn};

use crate::error::{RarError, RarResult};
use crate::vint;
use common::{HeaderType, RawHeader};

/// A parsed header from the archive.
#[derive(Debug, Clone)]
pub enum Header {
    MainArchive(main_archive::MainArchiveHeader),
    File(file::FileHeader),
    Service(service::ServiceHeader),
    Encryption(encryption::EncryptionHeader),
    EndArchive(end_archive::EndArchiveHeader),
    /// Unknown header type that was skipped.
    Unknown(u64),
}

/// File-level encryption parameters from extra records.
#[derive(Debug, Clone)]
pub struct FileEncryptionParams {
    pub kdf_count: u8,
    pub salt: [u8; 16],
    pub iv: [u8; 16],
    pub check_data: Option<[u8; 12]>,
    /// When true, CRC32 and BLAKE2 hashes in the header are HMAC-transformed
    /// and cannot be verified without HashKey (from unrar's custom PBKDF2 chain).
    pub use_hash_mac: bool,
}

/// A file header enriched with extra record data.
#[derive(Debug, Clone)]
pub struct ParsedFile {
    pub header: file::FileHeader,
    pub is_encrypted: bool,
    pub file_encryption: Option<FileEncryptionParams>,
    pub hash: Option<crate::types::FileHash>,
    pub redirection: Option<Redirection>,
}

/// Redirection info from extra records (symlinks/hardlinks).
#[derive(Debug, Clone)]
pub struct Redirection {
    pub redir_type: RedirectionType,
    pub target: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedirectionType {
    UnixSymlink,
    WindowsSymlink,
    WindowsJunction,
    Hardlink,
    Unknown(u64),
}

impl From<u64> for RedirectionType {
    fn from(value: u64) -> Self {
        match value {
            1 => RedirectionType::UnixSymlink,
            2 => RedirectionType::WindowsSymlink,
            3 => RedirectionType::WindowsJunction,
            4 => RedirectionType::Hardlink,
            other => RedirectionType::Unknown(other),
        }
    }
}

/// Result of parsing all headers in a single volume.
#[derive(Debug)]
pub struct ParsedHeaders {
    /// The main archive header (always present in a valid archive).
    pub main: Option<main_archive::MainArchiveHeader>,
    /// All file headers found, enriched with extra record data.
    pub files: Vec<ParsedFile>,
    /// All service headers found.
    pub services: Vec<service::ServiceHeader>,
    /// Encryption header, if present.
    pub encryption: Option<encryption::EncryptionHeader>,
    /// End of archive header, if present.
    pub end: Option<end_archive::EndArchiveHeader>,
    /// Whether the archive is encrypted at the header level.
    pub is_encrypted: bool,
}

/// Parse all headers from a RAR5 archive stream.
///
/// The reader should be positioned right after the RAR5 signature (8 bytes in).
/// If a password is provided, archive-level encryption (encrypted headers) will
/// be decrypted. Without a password, encountering an encryption header returns
/// `Err(RarError::EncryptedArchive)`.
pub fn parse_all_headers<R: Read + Seek>(
    reader: &mut R,
    password: Option<&str>,
) -> RarResult<ParsedHeaders> {
    let mut result = ParsedHeaders {
        main: None,
        files: Vec::new(),
        services: Vec::new(),
        encryption: None,
        end: None,
        is_encrypted: false,
    };

    // Parse plaintext headers until we hit an encryption header or end.
    loop {
        let raw = match common::read_raw_header(reader)? {
            Some(raw) => raw,
            None => {
                debug!("reached EOF while reading headers");
                return Ok(result);
            }
        };

        debug!(
            "header type={:?} flags={:#x} size={} data_size={} at offset={}",
            raw.header_type, raw.flags, raw.header_size, raw.data_area_size, raw.offset
        );

        let data_offset = raw.offset + 4 + raw.header_size_vint_len as u64 + raw.header_size;

        match raw.header_type {
            HeaderType::Encryption => {
                let enc = encryption::parse(&raw)?;
                debug!(
                    "encryption header: version={} kdf_count={}",
                    enc.version, enc.kdf_count
                );
                result.encryption = Some(enc.clone());
                result.is_encrypted = true;

                let pwd = match password {
                    Some(p) => p,
                    None => return Err(RarError::EncryptedArchive),
                };

                // Derive key (IV comes per-header from stream, not PBKDF2).
                let (key, _) = crate::crypto::derive_key(pwd, &enc.salt, enc.kdf_count);

                // Parse remaining headers — each has its own IV + padded encryption.
                parse_encrypted_headers(reader, &key, &mut result)?;
                return Ok(result);
            }
            HeaderType::EndArchive => {
                let end = end_archive::parse(&raw)?;
                debug!("end of archive: more_volumes={}", end.more_volumes);
                result.end = Some(end);
                return Ok(result);
            }
            _ => {
                dispatch_header(&raw, data_offset, &mut result)?;
                common::skip_data_area(reader, &raw)?;
            }
        }
    }
}

/// Parse headers from an encrypted archive (after the encryption header).
///
/// Each encrypted header has its own layout:
///   [16-byte IV] [header data padded to 16-byte boundary]
/// The header data is AES-256-CBC encrypted using the archive key and per-header IV.
fn parse_encrypted_headers<R: Read + Seek>(
    reader: &mut R,
    key: &[u8; 32],
    result: &mut ParsedHeaders,
) -> RarResult<()> {
    loop {
        let header_start = reader.stream_position().map_err(RarError::Io)?;

        // Read per-header 16-byte IV.
        let mut iv = [0u8; 16];
        match reader.read_exact(&mut iv) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("reached EOF reading encrypted header IV");
                break;
            }
            Err(e) => return Err(RarError::Io(e)),
        }

        // Read and decrypt first AES block (16 bytes) to determine header size.
        let mut block0 = [0u8; 16];
        reader.read_exact(&mut block0).map_err(RarError::Io)?;

        let mut decryptor = crate::crypto::CbcDecryptor::new(key, &iv);
        let mut dec0 = block0;
        decryptor.decrypt_blocks(&mut dec0);

        // Parse CRC32 (4 bytes LE) and header_size vint from decrypted data.
        let stored_crc = u32::from_le_bytes(dec0[0..4].try_into().unwrap());
        let (header_size, vint_len) = match vint::read_vint(&dec0[4..]) {
            Ok(v) => v,
            Err(_) => {
                debug!(
                    "failed to parse header_size vint in encrypted header — likely end of headers"
                );
                break;
            }
        };

        if header_size == 0 {
            debug!(
                "zero header size in encrypted header at offset {header_start} — likely end of headers"
            );
            break;
        }

        if header_size > common::MAX_HEADER_BODY {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "encrypted header size {} exceeds maximum {}",
                    header_size,
                    common::MAX_HEADER_BODY
                ),
            });
        }

        // Total header bytes = CRC(4) + vint_len + body(header_size).
        let total = 4 + vint_len + header_size as usize;
        let aligned = (total + 15) & !15;

        // Read and decrypt remaining blocks if needed.
        let mut decrypted = Vec::with_capacity(aligned);
        decrypted.extend_from_slice(&dec0);

        if aligned > 16 {
            let remaining = aligned - 16;
            let mut more = vec![0u8; remaining];
            reader.read_exact(&mut more).map_err(RarError::Io)?;
            decryptor.decrypt_blocks(&mut more);
            decrypted.extend_from_slice(&more);
        }

        // Extract the body (header_size bytes after CRC + vint).
        let body_start = 4 + vint_len;
        let body_end = body_start + header_size as usize;
        if body_end > decrypted.len() {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "encrypted header body extends beyond decrypted data ({body_end} > {})",
                    decrypted.len()
                ),
            });
        }
        let body = decrypted[body_start..body_end].to_vec();

        // CRC32 check over header_size vint encoding + body.
        let vint_encoded = common::vint_encode_for_crc(header_size, vint_len);
        let computed_crc = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&vint_encoded);
            hasher.update(&body);
            hasher.finalize()
        };

        if computed_crc != stored_crc {
            debug!(
                "CRC mismatch in encrypted header at offset {} (expected {:#x}, got {:#x}) — likely wrong password",
                header_start, stored_crc, computed_crc
            );
            return Err(RarError::HeaderCrcMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        // Build RawHeader from decrypted data.
        let raw = common::parse_raw_header_from_parts(
            header_start,
            stored_crc,
            header_size,
            vint_len,
            body,
        )?;

        debug!(
            "encrypted header type={:?} flags={:#x} size={} data_size={} at offset={}",
            raw.header_type, raw.flags, raw.header_size, raw.data_area_size, raw.offset
        );

        // Data offset: past the aligned encrypted header block, in the file stream.
        let data_offset = header_start + 16 + aligned as u64;

        match raw.header_type {
            HeaderType::EndArchive => {
                let end = end_archive::parse(&raw)?;
                debug!("end of archive: more_volumes={}", end.more_volumes);
                result.end = Some(end);
                break;
            }
            _ => {
                dispatch_header(&raw, data_offset, result)?;
                // Skip data area (not part of the encrypted header block).
                if raw.data_area_size > 0 {
                    reader
                        .seek(SeekFrom::Current(raw.data_area_size as i64))
                        .map_err(RarError::Io)?;
                }
            }
        }
    }
    Ok(())
}

/// Dispatch a parsed header to the appropriate type-specific handler.
fn dispatch_header(raw: &RawHeader, data_offset: u64, result: &mut ParsedHeaders) -> RarResult<()> {
    match raw.header_type {
        HeaderType::MainArchive => {
            let main = main_archive::parse(raw)?;
            debug!(
                "main archive: volume={} solid={} vol_num={:?}",
                main.is_volume, main.is_solid, main.volume_number
            );
            result.main = Some(main);
        }
        HeaderType::File => {
            let file_hdr = file::parse(raw, data_offset)?;
            let (is_encrypted, file_encryption, hash, redirection) = extract_extra_info(raw);
            debug!(
                "file: name={:?} size={:?} method={:?} encrypted={}",
                file_hdr.name, file_hdr.unpacked_size, file_hdr.compression.method, is_encrypted
            );
            result.files.push(ParsedFile {
                header: file_hdr,
                is_encrypted,
                file_encryption,
                hash,
                redirection,
            });
        }
        HeaderType::Service => {
            let svc = service::parse(raw, data_offset)?;
            debug!("service: name={:?}", svc.service_name());
            result.services.push(svc);
        }
        HeaderType::Unknown(type_val) => {
            warn!("unknown header type {} at offset {}", type_val, raw.offset);
            if raw.flags & common::flags::SKIP_IF_UNKNOWN == 0 {
                return Err(RarError::CorruptArchive {
                    detail: format!("unknown header type {type_val} at offset {}", raw.offset),
                });
            }
        }
        // Encryption and EndArchive are handled by the caller.
        _ => {}
    }
    Ok(())
}

/// Extract encryption status, encryption params, BLAKE2 hash, and redirection info from extra records.
fn extract_extra_info(
    raw: &RawHeader,
) -> (
    bool,
    Option<FileEncryptionParams>,
    Option<crate::types::FileHash>,
    Option<Redirection>,
) {
    let Some(ea_bytes) = common::extra_area_bytes(raw) else {
        return (false, None, None, None);
    };
    let Ok(records) = extra::parse_extra_area(ea_bytes) else {
        return (false, None, None, None);
    };

    let is_encrypted = extra::is_encrypted(&records);
    let hash = extra::blake2_hash(&records);

    let file_encryption = records.iter().find_map(|r| {
        if let extra::ExtraRecord::FileEncryption {
            kdf_count,
            salt,
            iv,
            check_data,
            enc_flags,
            ..
        } = r
        {
            Some(FileEncryptionParams {
                kdf_count: *kdf_count,
                salt: *salt,
                iv: *iv,
                check_data: *check_data,
                use_hash_mac: enc_flags & 0x0002 != 0,
            })
        } else {
            None
        }
    });

    let redirection = records.iter().find_map(|r| {
        if let extra::ExtraRecord::Redirection { redir_type, target } = r {
            Some(Redirection {
                redir_type: RedirectionType::from(*redir_type),
                target: target.clone(),
            })
        } else {
            None
        }
    });

    (is_encrypted, file_encryption, hash, redirection)
}

/// Get extra records for a file header from its raw header's extra area.
pub fn parse_file_extra_records(raw: &RawHeader) -> RarResult<Vec<extra::ExtraRecord>> {
    if let Some(ea_bytes) = common::extra_area_bytes(raw) {
        extra::parse_extra_area(ea_bytes)
    } else {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes::Aes256;
    use aes::cipher::{BlockEncrypt, KeyInit};

    fn encrypt_first_cbc_block(key: &[u8; 32], iv: &[u8; 16], plaintext: [u8; 16]) -> [u8; 16] {
        let cipher = Aes256::new(key.into());
        let mut block = plaintext;
        for (byte, iv_byte) in block.iter_mut().zip(iv.iter()) {
            *byte ^= iv_byte;
        }
        let block_ref = aes::cipher::generic_array::GenericArray::from_mut_slice(&mut block);
        cipher.encrypt_block(block_ref);
        block
    }

    #[test]
    fn encrypted_header_size_above_cap_is_rejected() {
        let oversized = common::MAX_HEADER_BODY + 1;
        let size_vint = vint::encode_vint(oversized);

        let mut decrypted = [0u8; 16];
        decrypted[4..4 + size_vint.len()].copy_from_slice(&size_vint);

        let key = [0u8; 32];
        let iv = [0u8; 16];
        let ciphertext = encrypt_first_cbc_block(&key, &iv, decrypted);

        let mut payload = Vec::with_capacity(32);
        payload.extend_from_slice(&iv);
        payload.extend_from_slice(&ciphertext);

        let mut cursor = std::io::Cursor::new(payload);
        let mut parsed = ParsedHeaders {
            main: None,
            files: Vec::new(),
            services: Vec::new(),
            encryption: None,
            end: None,
            is_encrypted: true,
        };

        let result = parse_encrypted_headers(&mut cursor, &key, &mut parsed);
        assert!(
            matches!(result, Err(RarError::CorruptArchive { ref detail }) if detail.contains("exceeds maximum")),
            "expected oversize encrypted header to be rejected, got: {result:?}"
        );
    }
}
