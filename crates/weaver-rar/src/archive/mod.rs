//! RarArchive: open, parse headers, list members, metadata.
//!
//! Supports single-volume and multi-volume archives. For multi-volume
//! archives, volumes can be added incrementally as they become available.

mod member;
mod parse;
mod volume;

use std::io::{Read, Seek, SeekFrom, Write};

use crate::decompress::lz::LzDecoder;
use crate::decompress::rar4::Rar4LzDecoder;
use crate::error::{RarError, RarResult};
use crate::extract::{self, ExtractOptions};
use crate::header;
use crate::header::file::FileHeader;
use crate::limits::Limits;
use crate::progress::ProgressHandler;
use crate::signature;
use crate::types::{
    ArchiveFormat, ArchiveMetadata, CompressionMethod, FileHash, MemberInfo, VolumeSpan,
};
use crate::volume::VolumeSet;

/// File-level encryption parameters extracted from the file's extra records.
#[derive(Debug, Clone)]
pub(super) struct FileEncryptionInfo {
    pub(super) kdf_count: u8,
    pub(super) salt: [u8; 16],
    pub(super) iv: [u8; 16],
    pub(super) check_data: Option<[u8; 12]>,
}

/// Internal entry tracking a member and its data segments across volumes.
#[derive(Debug, Clone)]
pub(super) struct MemberEntry {
    /// The parsed file header (from the first volume containing this member).
    pub(super) file_header: FileHeader,
    /// Whether this member is encrypted (file-level encryption).
    pub(super) is_encrypted: bool,
    /// File-level encryption info, if encrypted (RAR5).
    pub(super) file_encryption: Option<FileEncryptionInfo>,
    /// RAR4 encryption salt (8 bytes), if encrypted.
    pub(super) rar4_salt: Option<[u8; 8]>,
    /// Optional BLAKE2sp hash from extra records.
    pub(super) hash: Option<FileHash>,
    /// Redirection info (symlink/hardlink) from extra records.
    pub(super) redirection: Option<header::Redirection>,
    /// Data segments across volumes, in order.
    pub(super) segments: Vec<DataSegment>,
}

/// A contiguous data segment within a single volume.
#[derive(Debug, Clone)]
pub struct DataSegment {
    /// Volume index this segment lives in.
    pub volume_index: usize,
    /// Offset within the volume reader where data begins.
    pub data_offset: u64,
    /// Size of this data segment.
    pub data_size: u64,
}

/// Holder for a volume's reader and parsed metadata.
pub(super) struct VolumeData {
    pub(super) reader: Box<dyn ReadSeek>,
    /// Volume index (0-based).
    #[allow(dead_code)]
    pub(super) index: usize,
}

/// Trait alias for Read + Seek so we can use trait objects.
pub trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

/// A parsed RAR5 archive that can list members and extract files.
///
/// Supports both single-volume and multi-volume archives. For multi-volume
/// archives, use `open()` to parse the first volume, then `add_volume()`
/// to register additional volumes as they become available.
pub struct RarArchive {
    /// Parsed archive metadata.
    pub(super) format: ArchiveFormat,
    /// Whether the archive is solid.
    pub(super) is_solid: bool,
    /// Whether the archive has header-level encryption.
    pub(super) is_encrypted: bool,
    /// Volume topology.
    pub(super) volume_set: VolumeSet,
    /// All member entries (file headers + data segments).
    pub(super) members: Vec<MemberEntry>,
    /// Whether the end of archive was reached.
    #[allow(dead_code)]
    pub(super) end_reached: bool,
    /// Whether more volumes follow.
    pub(super) more_volumes: bool,
    /// Volume readers, indexed by volume number.
    pub(super) volumes: Vec<Option<VolumeData>>,
    /// RAR5 LZ decoder state for solid archive extraction (carries across members).
    pub(super) solid_decoder: Option<LzDecoder>,
    /// RAR4 LZ decoder state for solid archive extraction.
    pub(super) solid_decoder_rar4: Option<Rar4LzDecoder>,
    /// Index of the next member that must be extracted in a solid archive.
    pub(super) solid_next_index: usize,
    /// Resource limits for archive processing.
    pub(super) limits: Limits,
    /// Password for decrypting encrypted archives/members.
    pub(super) password: Option<String>,
}

impl RarArchive {
    /// Open and parse a RAR archive from a reader.
    pub fn open(reader: impl Read + Seek + Send + 'static) -> RarResult<Self> {
        let reader: Box<dyn ReadSeek> = Box::new(reader);
        Self::open_boxed_inner(reader, None)
    }

    /// Open and parse an encrypted RAR archive with a password.
    ///
    /// The password is used both for decrypting archive-level headers and for
    /// decrypting individual encrypted members during extraction.
    pub fn open_with_password(
        reader: impl Read + Seek + Send + 'static,
        password: &str,
    ) -> RarResult<Self> {
        let reader: Box<dyn ReadSeek> = Box::new(reader);
        Self::open_boxed_inner(reader, Some(password))
    }

    /// Set or update the password for decrypting encrypted members.
    ///
    /// This does not re-parse headers; it only affects subsequent extraction
    /// calls. For archive-level encryption (encrypted headers), use
    /// `open_with_password()` instead.
    pub fn set_password(&mut self, password: impl Into<String>) {
        self.password = Some(password.into());
    }

    /// Set resource limits for archive processing.
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Check if a member is extractable (all required volumes are present).
    ///
    /// A member is extractable when:
    /// 1. All known data segments have their volume readers present, AND
    /// 2. The member is not still waiting for continuation segments
    ///    (i.e. the last segment does not have `split_after` set, or
    ///    equivalently, a segment from the volume that terminates this
    ///    member has been registered).
    pub fn is_extractable(&self, name: &str) -> bool {
        if let Some(entry) = self.members.iter().find(|m| m.file_header.name == name) {
            // If the file header still has split_after, we haven't seen the
            // final continuation segment yet.
            if entry.file_header.split_after {
                return false;
            }
            // Check that we have readers for all segments' volumes.
            entry.segments.iter().all(|seg| {
                self.volumes.get(seg.volume_index)
                    .is_some_and(|v| v.is_some())
            })
        } else {
            false
        }
    }

    /// List which volume indices are still needed for a member.
    ///
    /// This includes both volumes whose readers are missing AND the next
    /// expected volume if the member still has continuation segments pending.
    pub fn missing_volumes(&self, name: &str) -> Vec<usize> {
        if let Some(entry) = self.members.iter().find(|m| m.file_header.name == name) {
            let mut missing: Vec<usize> = entry.segments.iter()
                .filter(|seg| {
                    !self.volumes.get(seg.volume_index)
                        .is_some_and(|v| v.is_some())
                })
                .map(|seg| seg.volume_index)
                .collect();

            // If the member is still split (awaiting more segments),
            // report the next volume after the last known segment.
            if entry.file_header.split_after {
                let next_vol = entry.segments.last()
                    .map(|s| s.volume_index + 1)
                    .unwrap_or(1);
                if !missing.contains(&next_vol) {
                    missing.push(next_vol);
                }
            }

            missing
        } else {
            Vec::new()
        }
    }

    /// Get archive metadata without decompression.
    pub fn metadata(&self) -> ArchiveMetadata {
        let members: Vec<MemberInfo> = self
            .members
            .iter()
            .filter(|entry| !entry.file_header.split_before || entry.segments.len() > 1)
            .map(|entry| self.make_member_info(entry))
            .collect();

        ArchiveMetadata {
            format: self.format,
            is_solid: self.is_solid,
            is_encrypted: self.is_encrypted,
            volume_count: self.volume_set.expected_count(),
            members,
        }
    }

    /// List all member names.
    pub fn member_names(&self) -> Vec<&str> {
        self.members
            .iter()
            .map(|m| m.file_header.name.as_str())
            .collect()
    }

    /// Find a member by name, returning its index.
    pub fn find_member(&self, name: &str) -> Option<usize> {
        self.members
            .iter()
            .position(|m| m.file_header.name == name)
    }

    /// Get member info by index.
    pub fn member_info(&self, index: usize) -> Option<MemberInfo> {
        self.members.get(index).map(|entry| self.make_member_info(entry))
    }

    pub(super) fn make_member_info(&self, entry: &MemberEntry) -> MemberInfo {
        let fh = &entry.file_header;
        let first_vol = entry.segments.first().map(|s| s.volume_index).unwrap_or(0);
        let last_vol = entry.segments.last().map(|s| s.volume_index).unwrap_or(first_vol);
        let total_compressed: u64 = entry.segments.iter().map(|s| s.data_size).sum();

        let (is_symlink, is_hardlink, link_target) = match &entry.redirection {
            Some(redir) => {
                let is_sym = matches!(
                    redir.redir_type,
                    header::RedirectionType::UnixSymlink
                        | header::RedirectionType::WindowsSymlink
                        | header::RedirectionType::WindowsJunction
                );
                let is_hard = matches!(redir.redir_type, header::RedirectionType::Hardlink);
                (is_sym, is_hard, Some(redir.target.clone()))
            }
            None => (false, false, None),
        };

        MemberInfo {
            name: crate::path::sanitize_path(&fh.name),
            raw_name: fh.name.clone(),
            unpacked_size: fh.unpacked_size,
            compressed_size: total_compressed,
            is_directory: fh.is_directory,
            crc32: fh.data_crc32,
            mtime: fh.mtime,
            host_os: fh.host_os,
            compression: fh.compression,
            is_encrypted: entry.is_encrypted,
            hash: entry.hash.clone(),
            volumes: VolumeSpan {
                first_volume: first_vol,
                last_volume: last_vol,
            },
            is_symlink,
            is_hardlink,
            link_target,
        }
    }

    /// Returns the archive format.
    pub fn format(&self) -> ArchiveFormat {
        self.format
    }

    /// Returns whether the archive is solid.
    pub fn is_solid(&self) -> bool {
        self.is_solid
    }

    /// Returns whether more volumes follow.
    pub fn more_volumes(&self) -> bool {
        self.more_volumes
    }

    /// Returns a reference to the volume set.
    pub fn volume_set(&self) -> &VolumeSet {
        &self.volume_set
    }

    /// Reset solid decoder state (e.g. if starting a fresh extraction pass).
    pub fn reset_solid_state(&mut self) {
        self.solid_decoder = None;
        self.solid_decoder_rar4 = None;
        self.solid_next_index = 0;
    }

    /// Get the data segments for a member by index.
    ///
    /// Used by streaming extraction to know which volumes/offsets to read.
    pub fn member_segments(&self, index: usize) -> Option<&[DataSegment]> {
        self.members.get(index).map(|e| e.segments.as_slice())
    }

    /// Get the compression method for a member by index.
    pub fn member_compression(&self, index: usize) -> Option<CompressionMethod> {
        self.members.get(index).map(|e| e.file_header.compression.method)
    }

    /// Whether a member is encrypted.
    pub fn member_is_encrypted(&self, index: usize) -> bool {
        self.members.get(index).is_some_and(|e| e.is_encrypted)
    }
}

/// Writer wrapper that computes CRC32 of all data written through it.
pub(super) struct CrcWriter<'a, W: Write> {
    inner: &'a mut W,
    hasher: Option<crc32fast::Hasher>,
}

impl<'a, W: Write> CrcWriter<'a, W> {
    pub(super) fn new(inner: &'a mut W, compute_crc: bool) -> Self {
        Self {
            inner,
            hasher: if compute_crc {
                Some(crc32fast::Hasher::new())
            } else {
                None
            },
        }
    }

    pub(super) fn finalize_crc(&self) -> u32 {
        self.hasher
            .as_ref()
            .map(|h| h.clone().finalize())
            .unwrap_or(0)
    }
}

impl<W: Write> Write for CrcWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        if let Some(ref mut h) = self.hasher {
            h.update(&buf[..n]);
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
