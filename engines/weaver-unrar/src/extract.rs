//! Member extraction with CRC32 and BLAKE2 verification.
//!
//! Supports both stored (method 0) and LZ-compressed (methods 1-5) extraction.

use std::fmt;
use std::io::{Read, Seek, SeekFrom, Write};

use tempfile::NamedTempFile;

use crate::decompress;
use crate::error::{RarError, RarResult};
use crate::header::file::FileHeader;
use crate::limits::Limits;
use crate::progress::ProgressHandler;
use crate::types::{CompressionMethod, FileHash, MemberInfo};

/// Options for extraction.
pub struct ExtractOptions {
    /// Whether to verify CRC32/BLAKE2 after extraction.
    pub verify: bool,
    /// Password for decrypting encrypted members.
    pub password: Option<String>,
    /// Restore archived Unix owner/group metadata when available.
    ///
    /// This mirrors UnRAR's explicit `-ow` / `ProcessOwners` mode. It remains
    /// disabled by default so extraction does not unexpectedly require owner
    /// privileges or alter ownership on normal user workflows.
    pub restore_owners: bool,
}

impl Default for ExtractOptions {
    fn default() -> Self {
        Self {
            verify: true,
            password: None,
            restore_owners: false,
        }
    }
}

/// Buffer size for copying data during store extraction.
const COPY_BUF_SIZE: usize = 64 * 1024;

/// Batch extraction keeps small members in memory and spills larger outputs to a temp file.
/// Keep the default threshold low so `extract_member()` does not retain large
/// heap buffers by default on archives that are better served by file-backed output.
const DEFAULT_SPOOL_THRESHOLD_BYTES: usize = 1024 * 1024;
const MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES: usize = 512 * 1024 * 1024;

fn spool_threshold_bytes() -> usize {
    std::env::var("WEAVER_RAR_SPOOL_THRESHOLD_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SPOOL_THRESHOLD_BYTES)
}

fn enforce_memory_extract_member_buffer_limit(file_header: &FileHeader) -> RarResult<()> {
    if file_header.data_size > MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES as u64 {
        return Err(RarError::ResourceLimit {
            detail: format!(
                "member {} compressed data size {} exceeds memory extraction limit {}",
                file_header.name, file_header.data_size, MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES
            ),
        });
    }

    if let Some(unpacked_size) = file_header.unpacked_size
        && unpacked_size > MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES as u64
    {
        return Err(RarError::ResourceLimit {
            detail: format!(
                "member {} unpacked size {} exceeds memory extraction limit {}",
                file_header.name, unpacked_size, MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES
            ),
        });
    }

    Ok(())
}

fn enforce_memory_materialization_limit(member_name: &str, len: usize) -> RarResult<()> {
    if len > MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES {
        return Err(RarError::ResourceLimit {
            detail: format!(
                "member {member_name} extracted size {len} exceeds memory materialization limit {}",
                MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES
            ),
        });
    }

    Ok(())
}

fn output_capacity_hint(file_header: &FileHeader, limits: &Limits) -> usize {
    file_header
        .unpacked_size
        .unwrap_or(0)
        .min(limits.max_unpacked_size)
        .min(usize::MAX as u64) as usize
}

fn enforce_member_limits(file_header: &FileHeader, limits: &Limits) -> RarResult<()> {
    if file_header.data_size > limits.max_data_segment {
        return Err(RarError::ResourceLimit {
            detail: format!(
                "member {} compressed data size {} exceeds maximum {}",
                file_header.name, file_header.data_size, limits.max_data_segment
            ),
        });
    }

    if let Some(unpacked_size) = file_header.unpacked_size
        && unpacked_size > limits.max_unpacked_size
    {
        return Err(RarError::ResourceLimit {
            detail: format!(
                "member {} unpacked size {} exceeds maximum {}",
                file_header.name, unpacked_size, limits.max_unpacked_size
            ),
        });
    }

    let dict_size = effective_member_dict_size(file_header);
    if dict_size > limits.max_dict_size {
        return Err(RarError::DictionaryTooLarge {
            size: dict_size,
            max: limits.max_dict_size,
        });
    }

    Ok(())
}

fn effective_member_dict_size(file_header: &FileHeader) -> u64 {
    if file_header.compression.method == CompressionMethod::Store {
        0
    } else if file_header.compression.format.is_rar4_family() {
        crate::decompress::rar4_old::effective_rar4_window_size(file_header.compression.dict_size)
    } else {
        crate::decompress::lz::effective_lz_window_size(file_header.compression.dict_size)
    }
}

pub enum ExtractedMember {
    InMemory(Vec<u8>),
    TempFile { file: NamedTempFile, len: usize },
}

impl ExtractedMember {
    pub fn len(&self) -> usize {
        match self {
            Self::InMemory(data) => data.len(),
            Self::TempFile { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_bytes(&self) -> RarResult<Vec<u8>> {
        match self {
            Self::InMemory(data) => {
                enforce_memory_materialization_limit("in-memory member", data.len())?;
                Ok(data.clone())
            }
            Self::TempFile { file, len } => {
                enforce_memory_materialization_limit("tempfile-backed member", *len)?;
                let mut reopened = file.reopen().map_err(RarError::Io)?;
                reopened.seek(SeekFrom::Start(0)).map_err(RarError::Io)?;
                let mut data = Vec::with_capacity(*len);
                reopened.read_to_end(&mut data).map_err(RarError::Io)?;
                Ok(data)
            }
        }
    }

    pub fn into_bytes(self) -> RarResult<Vec<u8>> {
        match self {
            Self::InMemory(data) => Ok(data),
            Self::TempFile { file, len } => {
                enforce_memory_materialization_limit("tempfile-backed member", len)?;
                let mut reopened = file.reopen().map_err(RarError::Io)?;
                reopened.seek(SeekFrom::Start(0)).map_err(RarError::Io)?;
                let mut data = Vec::with_capacity(len);
                reopened.read_to_end(&mut data).map_err(RarError::Io)?;
                Ok(data)
            }
        }
    }
}

impl fmt::Debug for ExtractedMember {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InMemory(data) => f
                .debug_struct("ExtractedMember")
                .field("storage", &"memory")
                .field("len", &data.len())
                .finish(),
            Self::TempFile { len, .. } => f
                .debug_struct("ExtractedMember")
                .field("storage", &"tempfile")
                .field("len", len)
                .finish(),
        }
    }
}

impl PartialEq<Vec<u8>> for ExtractedMember {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.to_bytes().is_ok_and(|data| data == *other)
    }
}

impl PartialEq<&[u8]> for ExtractedMember {
    fn eq(&self, other: &&[u8]) -> bool {
        self.to_bytes().is_ok_and(|data| data.as_slice() == *other)
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for ExtractedMember {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.to_bytes()
            .is_ok_and(|data| data.as_slice() == other.as_slice())
    }
}

impl PartialEq<ExtractedMember> for Vec<u8> {
    fn eq(&self, other: &ExtractedMember) -> bool {
        other == self
    }
}

pub struct ExtractedMemberSink {
    storage: ExtractedMemberSinkStorage,
    threshold: usize,
    len: usize,
}

enum ExtractedMemberSinkStorage {
    Memory(Vec<u8>),
    TempFile(NamedTempFile),
}

impl ExtractedMemberSink {
    pub fn with_capacity_hint(capacity_hint: usize) -> RarResult<Self> {
        let threshold = spool_threshold_bytes().min(MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES);
        let storage = if capacity_hint > threshold {
            ExtractedMemberSinkStorage::TempFile(NamedTempFile::new().map_err(RarError::Io)?)
        } else {
            ExtractedMemberSinkStorage::Memory(Vec::with_capacity(capacity_hint))
        };

        Ok(Self {
            storage,
            threshold,
            len: 0,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn into_extracted(self) -> RarResult<ExtractedMember> {
        Ok(match self.storage {
            ExtractedMemberSinkStorage::Memory(data) => ExtractedMember::InMemory(data),
            ExtractedMemberSinkStorage::TempFile(file) => ExtractedMember::TempFile {
                file,
                len: self.len,
            },
        })
    }

    fn promote_to_tempfile(&mut self) -> std::io::Result<()> {
        let ExtractedMemberSinkStorage::Memory(data) = std::mem::replace(
            &mut self.storage,
            ExtractedMemberSinkStorage::Memory(Vec::new()),
        ) else {
            return Ok(());
        };

        let mut file = NamedTempFile::new()?;
        if !data.is_empty() {
            file.write_all(&data)?;
        }
        self.storage = ExtractedMemberSinkStorage::TempFile(file);
        Ok(())
    }
}

impl Write for ExtractedMemberSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_all(buf)?;
        Ok(buf.len())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match &mut self.storage {
            ExtractedMemberSinkStorage::Memory(data)
                if data.len().saturating_add(buf.len()) <= self.threshold =>
            {
                data.extend_from_slice(buf);
            }
            ExtractedMemberSinkStorage::Memory(_) => {
                self.promote_to_tempfile()?;
                if let ExtractedMemberSinkStorage::TempFile(file) = &mut self.storage {
                    file.write_all(buf)?;
                }
            }
            ExtractedMemberSinkStorage::TempFile(file) => {
                file.write_all(buf)?;
            }
        }
        self.len = self.len.saturating_add(buf.len());
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.storage {
            ExtractedMemberSinkStorage::Memory(_) => Ok(()),
            ExtractedMemberSinkStorage::TempFile(file) => file.flush(),
        }
    }
}

/// Extract a stored (method 0, uncompressed) file from the archive.
///
/// `reader` must be positioned at the start of the data area.
/// `writer` receives the uncompressed data.
/// `file_header` provides metadata for verification.
///
/// Returns the number of bytes written.
pub fn extract_stored<R, W>(
    reader: &mut R,
    writer: &mut W,
    file_header: &FileHeader,
    options: &ExtractOptions,
    progress: Option<&dyn ProgressHandler>,
    member_info: Option<&MemberInfo>,
) -> RarResult<u64>
where
    R: Read + Seek,
    W: Write,
{
    extract_stored_with_limits(
        reader,
        writer,
        file_header,
        options,
        progress,
        member_info,
        &Limits::default(),
    )
}

/// Extract a stored file using caller-provided resource limits.
pub fn extract_stored_with_limits<R, W>(
    reader: &mut R,
    writer: &mut W,
    file_header: &FileHeader,
    options: &ExtractOptions,
    progress: Option<&dyn ProgressHandler>,
    member_info: Option<&MemberInfo>,
    limits: &Limits,
) -> RarResult<u64>
where
    R: Read + Seek,
    W: Write,
{
    // Reject encrypted members — callers must decrypt before extraction.
    if file_header.is_encrypted {
        return Err(RarError::EncryptedMember {
            member: file_header.name.clone(),
        });
    }

    // Verify this is actually stored (method 0)
    if file_header.compression.method != CompressionMethod::Store {
        return Err(RarError::UnsupportedCompression {
            method: file_header.compression.method.code(),
            version: file_header.compression.version,
        });
    }
    enforce_member_limits(file_header, limits)?;

    // Seek to the data offset
    reader
        .seek(SeekFrom::Start(file_header.data_offset))
        .map_err(RarError::Io)?;

    let data_size = file_header.data_size;
    let mut remaining = data_size;
    let mut total_written: u64 = 0;

    let mut crc_hasher = if options.verify {
        Some(crc32fast::Hasher::new())
    } else {
        None
    };

    let mut buf = vec![0u8; COPY_BUF_SIZE];

    while remaining > 0 {
        let to_read = std::cmp::min(remaining, buf.len() as u64) as usize;
        let n = reader.read(&mut buf[..to_read]).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                RarError::TruncatedData {
                    offset: file_header.data_offset + total_written,
                }
            } else {
                RarError::Io(e)
            }
        })?;

        if n == 0 {
            return Err(RarError::TruncatedData {
                offset: file_header.data_offset + total_written,
            });
        }

        writer.write_all(&buf[..n]).map_err(RarError::Io)?;

        if let Some(ref mut hasher) = crc_hasher {
            hasher.update(&buf[..n]);
        }

        total_written += n as u64;
        remaining -= n as u64;

        if let (Some(p), Some(mi)) = (progress, member_info) {
            p.on_member_progress(mi, total_written);
        }
    }

    // Verify CRC32
    if options.verify
        && let Some(expected_crc) = file_header.data_crc32
    {
        let actual_crc = crc_hasher.unwrap().finalize();
        if actual_crc != expected_crc {
            return Err(RarError::DataCrcMismatch {
                member: file_header.name.clone(),
                expected: expected_crc,
                actual: actual_crc,
            });
        }
    }

    Ok(total_written)
}

/// Extract a member from the archive, handling both stored and compressed data.
///
/// `reader` provides access to the archive data.
/// `file_header` contains the parsed header for this member.
/// `options` controls verification behavior.
/// `progress` and `member_info` enable progress reporting.
/// `hash` is the optional BLAKE2sp hash from extra records.
///
/// Returns the decompressed data as a `Vec<u8>`.
pub fn extract_member<R: Read + Seek>(
    reader: &mut R,
    file_header: &FileHeader,
    options: &ExtractOptions,
    progress: Option<&dyn ProgressHandler>,
    member_info: Option<&MemberInfo>,
    hash: Option<&FileHash>,
) -> RarResult<ExtractedMember> {
    extract_member_with_limits(
        reader,
        file_header,
        options,
        progress,
        member_info,
        hash,
        &Limits::default(),
    )
}

/// Extract a member using caller-provided resource limits.
pub fn extract_member_with_limits<R: Read + Seek>(
    reader: &mut R,
    file_header: &FileHeader,
    options: &ExtractOptions,
    progress: Option<&dyn ProgressHandler>,
    member_info: Option<&MemberInfo>,
    hash: Option<&FileHash>,
    limits: &Limits,
) -> RarResult<ExtractedMember> {
    // Reject encrypted members — callers must decrypt before extraction.
    if file_header.is_encrypted {
        return Err(RarError::EncryptedMember {
            member: file_header.name.clone(),
        });
    }

    enforce_member_limits(file_header, limits)?;

    // Seek to the data area.
    reader
        .seek(SeekFrom::Start(file_header.data_offset))
        .map_err(RarError::Io)?;

    // Determine unpacked size.
    let unpacked_size = file_header.unpacked_size.unwrap_or(0);

    // Report progress start.
    if let (Some(p), Some(mi)) = (progress, member_info) {
        p.on_member_start(mi);
    }

    let output = match file_header.compression.method {
        CompressionMethod::Store => {
            let mut output =
                ExtractedMemberSink::with_capacity_hint(output_capacity_hint(file_header, limits))?;
            extract_stored_with_limits(
                reader,
                &mut output,
                file_header,
                options,
                progress,
                member_info,
                limits,
            )?;
            output.into_extracted()?
        }
        _ => {
            enforce_memory_extract_member_buffer_limit(file_header)?;
            let data_size =
                usize::try_from(file_header.data_size).map_err(|_| RarError::ResourceLimit {
                    detail: format!(
                        "member {} compressed data size {} exceeds platform capacity",
                        file_header.name, file_header.data_size
                    ),
                })?;
            let mut compressed = vec![0u8; data_size];
            if data_size > 0 {
                reader.read_exact(&mut compressed).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        RarError::TruncatedData {
                            offset: file_header.data_offset,
                        }
                    } else {
                        RarError::Io(e)
                    }
                })?;
            }

            // For compressed data, decompress first then verify CRC.
            let decompressed = decompress::decompress_with_limits(
                &compressed,
                unpacked_size,
                &file_header.compression,
                None, // CRC verified post-decompression
                limits,
            )?;

            // Verify CRC32 of decompressed data.
            if options.verify
                && let Some(expected) = file_header.data_crc32
            {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&decompressed);
                let actual = hasher.finalize();
                if actual != expected {
                    return Err(RarError::DataCrcMismatch {
                        member: file_header.name.clone(),
                        expected,
                        actual,
                    });
                }
            }

            ExtractedMember::InMemory(decompressed)
        }
    };

    // Report progress.
    if let (Some(p), Some(mi)) = (progress, member_info) {
        p.on_member_progress(mi, output.len() as u64);
    }

    // Verify BLAKE2sp hash if provided.
    if options.verify
        && let Some(FileHash::Blake2sp(expected)) = hash
        && !verify_blake2_member(&output, expected)?
    {
        return Err(RarError::Blake2Mismatch {
            member: file_header.name.clone(),
        });
    }

    // Report completion.
    if let (Some(p), Some(mi)) = (progress, member_info) {
        p.on_member_complete(mi, &Ok(()));
    }

    Ok(output)
}

/// Verify a BLAKE2sp hash against extracted data.
pub fn verify_blake2(data: &[u8], expected: &[u8; 32]) -> bool {
    crate::crypto::blake2sp_hash(data) == *expected
}

pub fn verify_blake2_member(data: &ExtractedMember, expected: &[u8; 32]) -> RarResult<bool> {
    match data {
        ExtractedMember::InMemory(bytes) => Ok(verify_blake2(bytes, expected)),
        ExtractedMember::TempFile { file, .. } => {
            let mut reopened = file.reopen().map_err(RarError::Io)?;
            reopened.seek(SeekFrom::Start(0)).map_err(RarError::Io)?;
            let mut hasher = crate::crypto::Blake2spHasher::new();
            let mut buf = vec![0u8; COPY_BUF_SIZE];
            loop {
                let n = reopened.read(&mut buf).map_err(RarError::Io)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(hasher.finalize() == *expected)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ArchiveFormat, CompressionInfo, FileAttributes, HostOs};

    #[test]
    fn verify_blake2_uses_blake2sp_reference_vector() {
        let expected = [
            0x05, 0x0d, 0xc5, 0x78, 0x60, 0x37, 0xea, 0x72, 0xcb, 0x9e, 0xd9, 0xd0, 0x32, 0x4a,
            0xfc, 0xab, 0x03, 0xc9, 0x7e, 0xc0, 0x2e, 0x8c, 0x47, 0x36, 0x8f, 0xc5, 0xdf, 0xb4,
            0xcf, 0x49, 0xd8, 0xc9,
        ];
        assert!(verify_blake2(b"foo", &expected));
    }

    fn make_stored_file_header(name: &str, data: &[u8], data_offset: u64) -> FileHeader {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        let crc = hasher.finalize();

        FileHeader {
            name: name.to_string(),
            name_raw: Some(name.as_bytes().to_vec()),
            unpacked_size: Some(data.len() as u64),
            attributes: FileAttributes(0o644),
            mtime: None,
            ctime: None,
            atime: None,
            data_crc32: Some(crc),
            data_hash: Some(crate::types::DataHash::Crc32(crc)),
            compression: CompressionInfo {
                format: crate::types::ArchiveFormat::Rar5,
                version: 0,
                solid: false,
                method: CompressionMethod::Store,
                dict_size: 128 * 1024,
            },
            host_os: HostOs::Unix,
            is_directory: false,
            file_flags: 0x0004, // CRC32_PRESENT
            data_size: data.len() as u64,
            split_before: false,
            split_after: false,
            data_offset,
            is_encrypted: false,
            version: None,
            service_subdata: None,
        }
    }

    #[test]
    fn test_extract_stored_basic() {
        let test_data = b"Hello, RAR world!";
        let fh = make_stored_file_header("test.txt", test_data, 0);

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let mut output = Vec::new();

        let bytes_written = extract_stored(
            &mut reader,
            &mut output,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(bytes_written, test_data.len() as u64);
        assert_eq!(output, test_data);
    }

    #[test]
    fn test_extract_stored_crc_mismatch() {
        let test_data = b"Hello, RAR world!";
        let mut fh = make_stored_file_header("test.txt", test_data, 0);
        fh.data_crc32 = Some(0xDEADBEEF); // Wrong CRC

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let mut output = Vec::new();

        let result = extract_stored(
            &mut reader,
            &mut output,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
        );

        assert!(matches!(result, Err(RarError::DataCrcMismatch { .. })));
    }

    #[test]
    fn test_extract_stored_no_verify() {
        let test_data = b"Hello, RAR world!";
        let mut fh = make_stored_file_header("test.txt", test_data, 0);
        fh.data_crc32 = Some(0xDEADBEEF); // Wrong CRC

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let mut output = Vec::new();

        let result = extract_stored(
            &mut reader,
            &mut output,
            &fh,
            &ExtractOptions {
                verify: false,
                ..Default::default()
            },
            None,
            None,
        );

        assert!(result.is_ok());
        assert_eq!(output, test_data);
    }

    #[test]
    fn test_extract_stored_empty_file() {
        let test_data = b"";
        let fh = make_stored_file_header("empty.txt", test_data, 0);

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let mut output = Vec::new();

        let bytes_written = extract_stored(
            &mut reader,
            &mut output,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(bytes_written, 0);
        assert!(output.is_empty());
    }

    #[test]
    fn test_extract_stored_large() {
        // Test with data larger than the copy buffer
        let test_data: Vec<u8> = (0..=255u8).cycle().take(256 * 1024).collect();
        let fh = make_stored_file_header("large.bin", &test_data, 0);

        let mut reader = std::io::Cursor::new(test_data.clone());
        let mut output = Vec::new();

        let bytes_written = extract_stored(
            &mut reader,
            &mut output,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(bytes_written, test_data.len() as u64);
        assert_eq!(output, test_data);
    }

    #[test]
    fn test_extract_rejects_compressed() {
        let test_data = b"compressed data";
        let mut fh = make_stored_file_header("test.txt", test_data, 0);
        fh.compression.method = CompressionMethod::Normal;

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let mut output = Vec::new();

        let result = extract_stored(
            &mut reader,
            &mut output,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
        );

        assert!(matches!(
            result,
            Err(RarError::UnsupportedCompression { .. })
        ));
    }

    #[test]
    fn test_extract_stored_with_offset() {
        let prefix = b"JUNK DATA BEFORE";
        let test_data = b"actual file content";
        let data_offset = prefix.len() as u64;

        let mut full_data = Vec::new();
        full_data.extend_from_slice(prefix);
        full_data.extend_from_slice(test_data);

        let fh = make_stored_file_header("test.txt", test_data, data_offset);

        let mut reader = std::io::Cursor::new(full_data);
        let mut output = Vec::new();

        let bytes_written = extract_stored(
            &mut reader,
            &mut output,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
        )
        .unwrap();

        assert_eq!(bytes_written, test_data.len() as u64);
        assert_eq!(output, test_data);
    }

    #[test]
    fn extract_member_rejects_data_size_above_limit_before_allocation() {
        let test_data = b"small";
        let mut fh = make_stored_file_header("huge-packed.bin", test_data, 0);
        fh.data_size = Limits::default().max_data_segment + 1;

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let result = extract_member(
            &mut reader,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
            None,
        );

        assert!(matches!(result, Err(RarError::ResourceLimit { .. })));
    }

    #[test]
    fn extract_member_with_limits_uses_custom_data_size_limit() {
        let test_data = b"small";
        let fh = make_stored_file_header("custom-packed.bin", test_data, 0);
        let limits = Limits {
            max_data_segment: 4,
            ..Limits::default()
        };

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let result = extract_member_with_limits(
            &mut reader,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
            None,
            &limits,
        );

        assert!(matches!(result, Err(RarError::ResourceLimit { .. })));
    }

    #[test]
    fn extract_member_rejects_unpacked_size_above_limit() {
        let test_data = b"small";
        let mut fh = make_stored_file_header("huge-unpacked.bin", test_data, 0);
        fh.unpacked_size = Some(Limits::default().max_unpacked_size + 1);

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let result = extract_member(
            &mut reader,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
            None,
        );

        assert!(matches!(result, Err(RarError::ResourceLimit { .. })));
    }

    #[test]
    fn extract_member_rejects_compressed_dictionary_above_limit() {
        let test_data = b"small";
        let mut fh = make_stored_file_header("huge-dict.bin", test_data, 0);
        fh.compression.method = CompressionMethod::Normal;
        fh.compression.dict_size = Limits::default().max_dict_size + 1;

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let result = extract_member(
            &mut reader,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
            None,
        );

        assert!(matches!(result, Err(RarError::DictionaryTooLarge { .. })));
    }

    #[test]
    fn extract_member_with_limits_uses_effective_rar4_dictionary_limit() {
        let test_data = b"small";
        let mut fh = make_stored_file_header("rar4-small-dict.bin", test_data, 0);
        fh.compression.format = ArchiveFormat::Rar4;
        fh.compression.version = 29;
        fh.compression.method = CompressionMethod::Normal;
        fh.compression.dict_size = 128 * 1024;
        let limits = Limits {
            max_dict_size: 128 * 1024,
            ..Limits::default()
        };

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let result = extract_member_with_limits(
            &mut reader,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
            None,
            &limits,
        );

        assert!(matches!(
            result,
            Err(RarError::DictionaryTooLarge {
                size: 262_144,
                max: 131_072
            })
        ));
    }

    #[test]
    fn extract_member_with_limits_uses_effective_rar5_dictionary_limit() {
        let test_data = b"small";
        let mut fh = make_stored_file_header("rar5-small-dict.bin", test_data, 0);
        fh.compression.format = ArchiveFormat::Rar5;
        fh.compression.method = CompressionMethod::Normal;
        fh.compression.dict_size = 128 * 1024;
        let limits = Limits {
            max_dict_size: 128 * 1024,
            ..Limits::default()
        };

        let mut reader = std::io::Cursor::new(test_data.to_vec());
        let result = extract_member_with_limits(
            &mut reader,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
            None,
            &limits,
        );

        assert!(matches!(
            result,
            Err(RarError::DictionaryTooLarge {
                size: 262_144,
                max: 131_072
            })
        ));
    }

    #[test]
    fn stored_member_with_large_unpacked_size_spools_instead_of_allocating() {
        let mut fh = make_stored_file_header("BDMV/STREAM/00042.m2ts", b"", 0);
        fh.unpacked_size = Some(68_325_814_272);

        let mut reader = std::io::Cursor::new(Vec::new());
        let result = extract_member(
            &mut reader,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
            None,
        )
        .unwrap();

        assert!(matches!(result, ExtractedMember::TempFile { len: 0, .. }));
    }

    #[test]
    fn temp_backed_member_refuses_large_memory_materialization() {
        let member = ExtractedMember::TempFile {
            file: tempfile::NamedTempFile::new().unwrap(),
            len: MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES + 1,
        };

        assert!(matches!(
            member.to_bytes(),
            Err(RarError::ResourceLimit { .. })
        ));
        assert!(matches!(
            member.into_bytes(),
            Err(RarError::ResourceLimit { .. })
        ));
    }

    #[test]
    fn default_limits_accept_500_gib_members_and_reject_larger_members() {
        let mut fh = make_stored_file_header("boundary.bin", b"", 0);
        fh.data_size = crate::limits::WEAVER_MAX_MEMBER_DATA_SIZE;
        fh.unpacked_size = Some(crate::limits::WEAVER_MAX_MEMBER_DATA_SIZE);
        enforce_member_limits(&fh, &Limits::default()).unwrap();

        fh.data_size = crate::limits::WEAVER_MAX_MEMBER_DATA_SIZE + 1;
        assert!(matches!(
            enforce_member_limits(&fh, &Limits::default()),
            Err(RarError::ResourceLimit { .. })
        ));

        fh.data_size = 0;
        fh.unpacked_size = Some(crate::limits::WEAVER_MAX_MEMBER_DATA_SIZE + 1);
        assert!(matches!(
            enforce_member_limits(&fh, &Limits::default()),
            Err(RarError::ResourceLimit { .. })
        ));
    }

    #[test]
    fn compressed_memory_extraction_rejects_huge_packed_size_before_allocation() {
        let mut fh = make_stored_file_header("huge-packed-compressed.bin", b"small", 0);
        fh.compression.method = CompressionMethod::Normal;
        fh.data_size = MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES as u64 + 1;
        fh.unpacked_size = Some(1);

        let mut reader = std::io::Cursor::new(Vec::new());
        let result = extract_member(
            &mut reader,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
            None,
        );

        assert!(matches!(result, Err(RarError::ResourceLimit { .. })));
    }

    #[test]
    fn compressed_memory_extraction_rejects_huge_unpacked_size_before_allocation() {
        let mut fh = make_stored_file_header("huge-unpacked-compressed.bin", b"small", 0);
        fh.compression.method = CompressionMethod::Normal;
        fh.unpacked_size = Some(MEMORY_EXTRACT_MEMBER_MAX_BUFFERED_BYTES as u64 + 1);

        let mut reader = std::io::Cursor::new(Vec::new());
        let result = extract_member(
            &mut reader,
            &fh,
            &ExtractOptions::default(),
            None,
            None,
            None,
        );

        assert!(matches!(result, Err(RarError::ResourceLimit { .. })));
    }
}
