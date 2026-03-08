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

use std::io::{Read, Seek};

use tracing::{debug, trace, warn};

use crate::error::{RarError, RarResult};
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

    // When archive-level encryption is active, all headers after the encryption
    // header have their bodies encrypted with AES-256-CBC. We store the derived
    // key/IV here once the encryption header is processed.
    let mut decrypt_key: Option<([u8; 32], [u8; 16])> = None;

    loop {
        let mut raw = match common::read_raw_header(reader)? {
            Some(raw) => raw,
            None => {
                debug!("reached EOF while reading headers");
                break;
            }
        };

        // If archive-level encryption is active, decrypt the header body.
        // CRC32 was already validated against the encrypted bytes by read_raw_header.
        if let Some((ref key, ref iv)) = decrypt_key {
            let decrypted = crate::crypto::decrypt_data(key, iv, &raw.body)?;
            // The decrypted body may have AES padding at the end; truncate
            // to the declared header_size if the decrypted buffer is larger.
            if decrypted.len() as u64 > raw.header_size {
                raw.body = decrypted[..raw.header_size as usize].to_vec();
            } else {
                raw.body = decrypted;
            }

            // Re-parse the common fields from the decrypted body so that
            // header_type, flags, extra_area_size, and data_area_size reflect
            // the plaintext values.
            let mut pos = 0;
            let (header_type_val, n) = crate::vint::read_vint(&raw.body[pos..])?;
            pos += n;
            let (header_flags, n) = crate::vint::read_vint(&raw.body[pos..])?;
            pos += n;
            let extra_area_size = if header_flags & common::flags::EXTRA_AREA != 0 {
                let (size, n) = crate::vint::read_vint(&raw.body[pos..])?;
                pos += n;
                size
            } else {
                0
            };
            let data_area_size = if header_flags & common::flags::DATA_AREA != 0 {
                let (size, _n) = crate::vint::read_vint(&raw.body[pos..])?;
                size
            } else {
                0
            };
            raw.header_type = HeaderType::from(header_type_val);
            raw.flags = header_flags;
            raw.extra_area_size = extra_area_size;
            raw.data_area_size = data_area_size;
        }

        trace!(
            "header type={:?} flags={:#x} size={} data_size={} at offset={}",
            raw.header_type,
            raw.flags,
            raw.header_size,
            raw.data_area_size,
            raw.offset
        );

        // The data area starts right after the header bytes in the stream
        let data_offset = raw.offset
            + 4 // CRC32
            + raw.header_size_vint_len as u64
            + raw.header_size;

        match raw.header_type {
            HeaderType::MainArchive => {
                let main = main_archive::parse(&raw)?;
                debug!(
                    "main archive: volume={} solid={} vol_num={:?}",
                    main.is_volume, main.is_solid, main.volume_number
                );
                result.main = Some(main);
            }
            HeaderType::File => {
                let file_hdr = file::parse(&raw, data_offset)?;

                // Extract enriched data from extra records
                let (is_encrypted, file_encryption, hash, redirection) = extract_extra_info(&raw);

                debug!(
                    "file: name={:?} size={:?} method={:?} encrypted={}",
                    file_hdr.name,
                    file_hdr.unpacked_size,
                    file_hdr.compression.method,
                    is_encrypted
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
                let svc = service::parse(&raw, data_offset)?;
                debug!("service: name={:?}", svc.service_name());
                result.services.push(svc);
            }
            HeaderType::Encryption => {
                let enc = encryption::parse(&raw)?;
                debug!(
                    "encryption header: version={} kdf_count={}",
                    enc.version, enc.kdf_count
                );
                result.encryption = Some(enc.clone());
                result.is_encrypted = true;

                if let Some(pwd) = password {
                    // Derive key/IV for decrypting subsequent headers.
                    let (key, iv) = crate::crypto::derive_key(pwd, &enc.salt, enc.kdf_count);

                    // If password check data is available, verify before proceeding.
                    if enc.has_password_check {
                        // Read the 12-byte check data that follows salt in the
                        // encryption header. We need to re-parse it from the raw body.
                        let offset = common::type_specific_offset(&raw)?;
                        let body = &raw.body[offset..];
                        let mut pos = 0;
                        // Skip version vint
                        let (_, n) = crate::vint::read_vint(&body[pos..])?;
                        pos += n;
                        // Skip enc_flags vint
                        let (_, n) = crate::vint::read_vint(&body[pos..])?;
                        pos += n;
                        // Skip kdf_count (1) + salt (16)
                        pos += 17;
                        if body.len() >= pos + 12 {
                            let mut check_data = [0u8; 12];
                            check_data.copy_from_slice(&body[pos..pos + 12]);
                            if !crate::crypto::verify_password_check(
                                pwd,
                                &enc.salt,
                                enc.kdf_count,
                                &check_data,
                            ) {
                                return Err(RarError::InvalidPassword);
                            }
                        }
                    }

                    decrypt_key = Some((key, iv));
                    // Continue parsing — subsequent headers will be decrypted.
                } else {
                    // No password provided; cannot continue past encryption header.
                    return Err(RarError::EncryptedArchive);
                }
            }
            HeaderType::EndArchive => {
                let end = end_archive::parse(&raw)?;
                debug!("end of archive: more_volumes={}", end.more_volumes);
                result.end = Some(end);
                // Don't skip data area for end header, just stop
                break;
            }
            HeaderType::Unknown(type_val) => {
                warn!("unknown header type {} at offset {}", type_val, raw.offset);
                if raw.flags & common::flags::SKIP_IF_UNKNOWN != 0 {
                    // Skip and continue
                } else {
                    return Err(RarError::CorruptArchive {
                        detail: format!("unknown header type {type_val} at offset {}", raw.offset),
                    });
                }
            }
        }

        // Skip data area if present
        common::skip_data_area(reader, &raw)?;
    }

    Ok(result)
}

/// Extract encryption status, encryption params, BLAKE2 hash, and redirection info from extra records.
fn extract_extra_info(raw: &RawHeader) -> (bool, Option<FileEncryptionParams>, Option<crate::types::FileHash>, Option<Redirection>) {
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
            ..
        } = r
        {
            Some(FileEncryptionParams {
                kdf_count: *kdf_count,
                salt: *salt,
                iv: *iv,
                check_data: *check_data,
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
