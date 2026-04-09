//! Lightweight volume probing for RAR archives.
//!
//! Reads just enough headers to extract volume metadata (format, volume number,
//! naming scheme, encryption, contained files) without constructing a full
//! `RarArchive`. This is designed for the scheduler's obfuscated filename
//! handling — equivalent to NZBGet's `RarVolume::Read()`.

use std::io::{Read, Seek, SeekFrom};

use crate::error::{RarError, RarResult};
use crate::signature;
use crate::types::ArchiveFormat;

/// A file entry found during probing (minimal info for volume assembly).
#[derive(Debug, Clone)]
pub struct ProbeFile {
    /// Filename stored in the archive.
    pub name: String,
    /// Unpacked file size (if known).
    pub unpacked_size: Option<u64>,
    /// File continues from a previous volume.
    pub split_before: bool,
    /// File continues into the next volume.
    pub split_after: bool,
}

/// Result of probing a single RAR volume.
#[derive(Debug, Clone)]
pub struct VolumeProbe {
    /// Detected archive format.
    pub format: ArchiveFormat,
    /// Volume number (0-based). For RAR5, from main header. For RAR4, from ENDARC.
    pub volume_number: Option<usize>,
    /// Whether this is a multi-volume archive.
    pub is_multi_volume: bool,
    /// Whether a next volume follows this one.
    pub has_next_volume: bool,
    /// RAR4: true = `.partNNNN.rar` naming, false = old `.rNN` naming.
    /// RAR5: always true (RAR5 only supports new naming).
    pub new_naming: bool,
    /// Whether the archive has encrypted headers (cannot list files without password).
    pub is_header_encrypted: bool,
    /// Whether any file entries are encrypted (file-level encryption).
    pub has_encrypted_files: bool,
    /// Whether this is a solid archive.
    pub is_solid: bool,
    /// Whether this appears to be the first volume of a set.
    pub is_first_volume: bool,
    /// Files found in this volume (for volume set assembly via split flag matching).
    pub files: Vec<ProbeFile>,
}

/// Probe a reader for RAR volume metadata.
///
/// This reads the signature and headers but does not read any compressed data.
/// It's much cheaper than `RarArchive::open()` — useful for scanning directories
/// of obfuscated files to identify and order RAR volumes.
///
/// Returns `Err(InvalidSignature)` if the reader doesn't contain a RAR archive.
pub fn probe_volume<R: Read + Seek>(reader: &mut R) -> RarResult<VolumeProbe> {
    reader.seek(SeekFrom::Start(0)).map_err(RarError::Io)?;
    let format = signature::read_signature(reader)?;

    match format {
        ArchiveFormat::Rar5 => probe_rar5(reader),
        ArchiveFormat::Rar4 => probe_rar4(reader),
    }
}

/// Probe a RAR5 volume.
fn probe_rar5<R: Read + Seek>(reader: &mut R) -> RarResult<VolumeProbe> {
    // Parse headers without password (encrypted headers will be detected and reported).
    let parsed = match crate::header::parse_all_headers(reader, None) {
        Ok(p) => p,
        Err(RarError::EncryptedArchive) => {
            return Ok(VolumeProbe {
                format: ArchiveFormat::Rar5,
                volume_number: None,
                is_multi_volume: false,
                has_next_volume: false,
                new_naming: true,
                is_header_encrypted: true,
                has_encrypted_files: false,
                is_solid: false,
                is_first_volume: false,
                files: Vec::new(),
            });
        }
        Err(e) => return Err(e),
    };

    let (is_multi_volume, volume_number, is_solid) = match &parsed.main {
        Some(main) => (
            main.is_volume,
            main.volume_number.map(|v| v as usize),
            main.is_solid,
        ),
        None => (false, None, false),
    };

    let has_next_volume = parsed.end.as_ref().is_some_and(|e| e.more_volumes);

    let has_encrypted_files = parsed.files.iter().any(|f| f.is_encrypted);

    let is_first_volume = if is_multi_volume {
        volume_number == Some(0) || volume_number.is_none()
    } else {
        true
    };

    let files = parsed
        .files
        .iter()
        .map(|pf| ProbeFile {
            name: pf.header.name.clone(),
            unpacked_size: pf.header.unpacked_size,
            split_before: pf.header.split_before,
            split_after: pf.header.split_after,
        })
        .collect();

    Ok(VolumeProbe {
        format: ArchiveFormat::Rar5,
        volume_number,
        is_multi_volume,
        has_next_volume,
        new_naming: true, // RAR5 always uses new naming
        is_header_encrypted: false,
        has_encrypted_files,
        is_solid,
        is_first_volume,
        files,
    })
}

/// Probe a RAR4 volume.
fn probe_rar4<R: Read + Seek>(reader: &mut R) -> RarResult<VolumeProbe> {
    let parsed = match crate::rar4::parse_rar4_headers(reader, None) {
        Ok(p) => p,
        Err(RarError::EncryptedArchive) => {
            return Ok(VolumeProbe {
                format: ArchiveFormat::Rar4,
                volume_number: None,
                is_multi_volume: false,
                has_next_volume: false,
                new_naming: false,
                is_header_encrypted: true,
                has_encrypted_files: false,
                is_solid: false,
                is_first_volume: false,
                files: Vec::new(),
            });
        }
        Err(e) => return Err(e),
    };

    let arch = &parsed.archive_header;
    let is_multi_volume = arch.is_volume;

    // RAR4 volume number comes from the ENDARC header.
    let volume_number = parsed
        .end
        .as_ref()
        .and_then(|e| e.volume_number)
        .map(|v| v as usize);

    let has_next_volume = parsed.end.as_ref().is_some_and(|e| e.more_volumes);
    let has_encrypted_files = parsed.files.iter().any(|f| f.is_encrypted);

    let is_first_volume = if is_multi_volume {
        arch.is_first_volume
            || volume_number == Some(0)
            || (volume_number.is_none() && !parsed.files.iter().any(|f| f.split_before))
    } else {
        true
    };

    let files = parsed
        .files
        .iter()
        .map(|fh| ProbeFile {
            name: fh.name.clone(),
            unpacked_size: Some(fh.unpacked_size),
            split_before: fh.split_before,
            split_after: fh.split_after,
        })
        .collect();

    Ok(VolumeProbe {
        format: ArchiveFormat::Rar4,
        volume_number,
        is_multi_volume,
        has_next_volume,
        new_naming: arch.new_naming,
        is_header_encrypted: false,
        has_encrypted_files,
        is_solid: arch.is_solid,
        is_first_volume,
        files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    const RAR4_SIG: [u8; 7] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00];

    /// Build a minimal RAR4 single-volume archive for probing.
    fn build_probe_rar4(
        arch_flags: u16,
        filename: &str,
        file_flags: u16,
        end_flags: u16,
        end_vol_num: Option<u16>,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&RAR4_SIG);

        // Archive header
        let arch_header_size: u16 = 7 + 6;
        buf.extend_from_slice(&[0x00, 0x00]); // CRC16
        buf.push(0x73);
        buf.extend_from_slice(&arch_flags.to_le_bytes());
        buf.extend_from_slice(&arch_header_size.to_le_bytes());
        buf.extend_from_slice(&[0u8; 6]); // reserved

        // File header (no data area)
        let name_bytes = filename.as_bytes();
        let file_header_size: u16 = 7 + 25 + name_bytes.len() as u16;
        buf.extend_from_slice(&[0x00, 0x00]); // CRC16
        buf.push(0x74);
        buf.extend_from_slice(&file_flags.to_le_bytes());
        buf.extend_from_slice(&file_header_size.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // packed size = 0
        buf.extend_from_slice(&1000u32.to_le_bytes()); // unpacked size
        buf.push(3); // Unix
        buf.extend_from_slice(&0u32.to_le_bytes()); // CRC32
        buf.extend_from_slice(&0u32.to_le_bytes()); // datetime
        buf.push(29); // version
        buf.push(0x30); // Store
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // attrs
        buf.extend_from_slice(name_bytes);

        // End header
        let mut end_extra = Vec::new();
        if end_flags & 0x0002 != 0 {
            // DATA_CRC
            end_extra.extend_from_slice(&0u32.to_le_bytes());
        }
        if let Some(vn) = end_vol_num {
            end_extra.extend_from_slice(&vn.to_le_bytes());
        }
        let end_header_size: u16 = 7 + end_extra.len() as u16;
        buf.extend_from_slice(&[0x00, 0x00]); // CRC16
        buf.push(0x7B);
        buf.extend_from_slice(&end_flags.to_le_bytes());
        buf.extend_from_slice(&end_header_size.to_le_bytes());
        buf.extend_from_slice(&end_extra);

        buf
    }

    #[test]
    fn test_probe_rar4_single_volume() {
        let data = build_probe_rar4(0, "movie.mkv", 0, 0, None);
        let result = probe_volume(&mut Cursor::new(data)).unwrap();
        assert_eq!(result.format, ArchiveFormat::Rar4);
        assert!(!result.is_multi_volume);
        assert!(result.is_first_volume);
        assert!(!result.has_next_volume);
        assert!(!result.new_naming);
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0].name, "movie.mkv");
    }

    #[test]
    fn test_probe_rar4_multi_volume_first() {
        // VOLUME + FIRST_VOLUME + NEW_NUMBERING, with volume number 0 in ENDARC
        let arch_flags = 0x0001 | 0x0100 | 0x0010;
        let end_flags = 0x0001 | 0x0004; // NEXT_VOLUME + VOLUME_NUMBER
        let data = build_probe_rar4(
            arch_flags,
            "movie.mkv",
            0x0002, // SPLIT_AFTER
            end_flags,
            Some(0),
        );
        let result = probe_volume(&mut Cursor::new(data)).unwrap();
        assert!(result.is_multi_volume);
        assert!(result.is_first_volume);
        assert!(result.has_next_volume);
        assert!(result.new_naming);
        assert_eq!(result.volume_number, Some(0));
        assert!(result.files[0].split_after);
        assert!(!result.files[0].split_before);
    }

    #[test]
    fn test_probe_rar4_multi_volume_continuation() {
        // VOLUME only (not first), volume number 2 in ENDARC
        let arch_flags = 0x0001;
        let end_flags = 0x0001 | 0x0004; // NEXT_VOLUME + VOLUME_NUMBER
        let data = build_probe_rar4(
            arch_flags,
            "movie.mkv",
            0x0001, // SPLIT_BEFORE
            end_flags,
            Some(2),
        );
        let result = probe_volume(&mut Cursor::new(data)).unwrap();
        assert!(result.is_multi_volume);
        assert!(!result.is_first_volume);
        assert!(result.has_next_volume);
        assert_eq!(result.volume_number, Some(2));
        assert!(result.files[0].split_before);
    }

    #[test]
    fn test_probe_rar4_last_volume() {
        let arch_flags = 0x0001;
        let end_flags = 0x0004; // VOLUME_NUMBER only (no NEXT_VOLUME)
        let data = build_probe_rar4(
            arch_flags,
            "movie.mkv",
            0x0001, // SPLIT_BEFORE
            end_flags,
            Some(5),
        );
        let result = probe_volume(&mut Cursor::new(data)).unwrap();
        assert!(result.is_multi_volume);
        assert!(!result.has_next_volume);
        assert_eq!(result.volume_number, Some(5));
    }

    #[test]
    fn test_probe_invalid_signature() {
        let data = vec![0u8; 100];
        let result = probe_volume(&mut Cursor::new(data));
        assert!(matches!(result, Err(RarError::InvalidSignature)));
    }
}
