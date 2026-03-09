use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use crate::checksum::{self, SliceChecksumState};
use crate::par2_set::Par2FileSet;
use crate::types::{CancellationToken, FileId, ProgressCallback, ProgressStage, ProgressUpdate};

/// Abstraction for file I/O during verification and repair.
pub trait FileAccess {
    /// Read a range of bytes from a file identified by its FileId.
    fn read_file_range(&self, file_id: &FileId, offset: u64, len: u64) -> io::Result<Vec<u8>>;

    /// Check if a file exists on disk.
    fn file_exists(&self, file_id: &FileId) -> bool;

    /// Get the actual length of a file on disk.
    fn file_length(&self, file_id: &FileId) -> Option<u64>;

    /// Read the entire file content.
    fn read_file(&self, file_id: &FileId) -> io::Result<Vec<u8>>;

    /// Write data to a file at the given offset.
    fn write_file_range(&mut self, file_id: &FileId, offset: u64, data: &[u8]) -> io::Result<()>;
}

/// Simple in-memory file access for testing.
pub struct MemoryFileAccess {
    files: HashMap<FileId, Vec<u8>>,
}

impl MemoryFileAccess {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    pub fn add_file(&mut self, file_id: FileId, data: Vec<u8>) {
        self.files.insert(file_id, data);
    }
}

impl Default for MemoryFileAccess {
    fn default() -> Self {
        Self::new()
    }
}

impl FileAccess for MemoryFileAccess {
    fn read_file_range(&self, file_id: &FileId, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let data = self
            .files
            .get(file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        let offset = offset as usize;
        let end = (offset + len as usize).min(data.len());
        if offset >= data.len() {
            return Ok(Vec::new());
        }
        Ok(data[offset..end].to_vec())
    }

    fn file_exists(&self, file_id: &FileId) -> bool {
        self.files.contains_key(file_id)
    }

    fn file_length(&self, file_id: &FileId) -> Option<u64> {
        self.files.get(file_id).map(|d| d.len() as u64)
    }

    fn read_file(&self, file_id: &FileId) -> io::Result<Vec<u8>> {
        self.files
            .get(file_id)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
    }

    fn write_file_range(&mut self, file_id: &FileId, offset: u64, data: &[u8]) -> io::Result<()> {
        let file_data = self
            .files
            .get_mut(file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        let offset = offset as usize;
        let end = offset + data.len();
        // Extend the file if needed.
        if end > file_data.len() {
            file_data.resize(end, 0);
        }
        file_data[offset..end].copy_from_slice(data);
        Ok(())
    }
}

/// Status of a single file after verification.
#[derive(Debug, Clone)]
pub enum FileStatus {
    /// File is complete: full MD5 matches.
    Complete,
    /// File is damaged: N slices are bad.
    Damaged(u32),
    /// File was not found on disk.
    Missing,
    /// File was found at a different path (identified via 16k hash).
    Renamed(PathBuf),
}

/// Verification result for a single file.
#[derive(Debug, Clone)]
pub struct FileVerification {
    pub file_id: FileId,
    pub filename: String,
    pub status: FileStatus,
    /// Per-slice validity: true = valid, false = damaged/missing.
    pub valid_slices: Vec<bool>,
    /// Number of damaged/missing slices.
    pub missing_slice_count: u32,
}

/// Repairability assessment.
#[derive(Debug, Clone)]
pub enum Repairability {
    /// No repair needed; all files are intact.
    NotNeeded,
    /// Repair is possible with available recovery data.
    Repairable {
        blocks_needed: u32,
        blocks_available: u32,
    },
    /// Insufficient recovery data to repair.
    Insufficient {
        blocks_needed: u32,
        blocks_available: u32,
        deficit: u32,
    },
}

/// Overall verification result.
#[derive(Debug, Clone)]
pub struct VerificationResult {
    pub files: Vec<FileVerification>,
    pub recovery_blocks_available: u32,
    pub total_missing_blocks: u32,
    pub repairable: Repairability,
}

/// Perform a 16KB quick-check on a file.
///
/// Reads the first 16384 bytes (or less if file is shorter) and computes MD5,
/// comparing against the stored `hash_16k` from the File Description packet.
pub fn quick_check_16k(
    par2: &Par2FileSet,
    file_id: &FileId,
    access: &dyn FileAccess,
) -> Option<bool> {
    let desc = par2.file_description(file_id)?;

    if !access.file_exists(file_id) {
        return Some(false);
    }

    let data = access.read_file_range(file_id, 0, 16384).ok()?;
    let hash = checksum::md5(&data);
    Some(hash == desc.hash_16k)
}

/// Perform full-file MD5 verification.
pub fn verify_full_hash(
    par2: &Par2FileSet,
    file_id: &FileId,
    access: &dyn FileAccess,
) -> Option<bool> {
    let desc = par2.file_description(file_id)?;

    let data = access.read_file(file_id).ok()?;
    if data.len() as u64 != desc.length {
        return Some(false);
    }

    let hash = checksum::md5(&data);
    Some(hash == desc.hash_full)
}

/// Perform slice-level verification of a single file.
///
/// Returns a vector of booleans, one per slice: true = valid, false = damaged.
pub fn verify_slices(
    par2: &Par2FileSet,
    file_id: &FileId,
    access: &dyn FileAccess,
) -> Option<Vec<bool>> {
    let _desc = par2.file_description(file_id)?;
    let checksums = par2.file_checksums(file_id)?;
    let slice_size = par2.slice_size;

    if !access.file_exists(file_id) {
        return Some(vec![false; checksums.len()]);
    }

    let file_data = access.read_file(file_id).ok()?;

    let results: Vec<bool> = checksums
        .iter()
        .enumerate()
        .map(|(i, expected)| {
            let offset = i as u64 * slice_size;
            let end = ((offset + slice_size) as usize).min(file_data.len());
            if offset as usize >= file_data.len() {
                return false;
            }

            let slice_data = &file_data[offset as usize..end];
            let mut state = SliceChecksumState::new();
            state.update(slice_data);

            // Pad last slice if needed
            let pad_to = if (slice_data.len() as u64) < slice_size {
                Some(slice_size)
            } else {
                None
            };
            let (crc, md5) = state.finalize(pad_to);

            crc == expected.crc32 && md5 == expected.md5
        })
        .collect();

    Some(results)
}

/// Verify file slices using pre-computed CRC32 values instead of reading from disk.
///
/// This is a CRC-only check (no MD5 verification). Useful when the download layer
/// has already computed CRC32s for each slice during decode, avoiding disk re-reads.
///
/// Returns a vector of booleans per slice (true = CRC matches), or `None` if the
/// file is not in the PAR2 set.
pub fn verify_slices_from_crcs(
    par2: &Par2FileSet,
    file_id: &FileId,
    slice_crcs: &[u32],
) -> Option<Vec<bool>> {
    let checksums = par2.file_checksums(file_id)?;

    let results: Vec<bool> = checksums
        .iter()
        .enumerate()
        .map(|(i, expected)| {
            slice_crcs
                .get(i)
                .map(|&crc| crc == expected.crc32)
                .unwrap_or(false)
        })
        .collect();

    Some(results)
}

/// Options controlling verification behavior.
#[derive(Default)]
pub struct VerifyOptions {
    /// If set, verification will check this token and stop early if cancelled.
    pub cancel: Option<CancellationToken>,
    /// If set, called with progress updates after each file is verified.
    pub progress: Option<ProgressCallback>,
}

/// Verify all files in a PAR2 set.
pub fn verify_all(par2: &Par2FileSet, access: &dyn FileAccess) -> VerificationResult {
    verify_all_with_options(par2, access, &VerifyOptions::default())
}

/// Verify only the selected PAR2 file IDs.
pub fn verify_selected_file_ids(
    par2: &Par2FileSet,
    access: &dyn FileAccess,
    file_ids: &[FileId],
) -> VerificationResult {
    verify_selected_file_ids_with_options(par2, access, file_ids, &VerifyOptions::default())
}

/// Verify all files in a PAR2 set with cancellation and progress support.
pub fn verify_all_with_options(
    par2: &Par2FileSet,
    access: &dyn FileAccess,
    options: &VerifyOptions,
) -> VerificationResult {
    verify_selected_file_ids_with_options(par2, access, &par2.recovery_file_ids, options)
}

/// Verify selected PAR2 file IDs with cancellation and progress support.
pub fn verify_selected_file_ids_with_options(
    par2: &Par2FileSet,
    access: &dyn FileAccess,
    file_ids: &[FileId],
    options: &VerifyOptions,
) -> VerificationResult {
    let mut files = Vec::new();
    let mut total_missing_blocks = 0u32;
    let total_files = file_ids.len() as u32;
    let mut bytes_processed = 0u64;

    for (file_index, file_id) in file_ids.iter().enumerate() {
        // Check cancellation before each file.
        if let Some(ref cancel) = options.cancel
            && cancel.is_cancelled()
        {
            break;
        }

        let desc = match par2.file_description(file_id) {
            Some(d) => d,
            None => continue,
        };

        if !access.file_exists(file_id) {
            let slice_count = par2.slice_count_for_file(desc.length);
            total_missing_blocks += slice_count;
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status: FileStatus::Missing,
                valid_slices: vec![false; slice_count as usize],
                missing_slice_count: slice_count,
            });
            continue;
        }

        // Empty file: PAR2 spec says 0-length files have 0 slices.
        // Just verify the file exists and has the expected length.
        if desc.length == 0 {
            let actual_len = access.file_length(file_id).unwrap_or(1);
            let status = if actual_len == 0 {
                FileStatus::Complete
            } else {
                FileStatus::Damaged(0)
            };
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status,
                valid_slices: vec![],
                missing_slice_count: 0,
            });
            continue;
        }

        // Check for files that should have content but are empty on disk.
        if let Some(actual_len) = access.file_length(file_id)
            && actual_len == 0
            && desc.length > 0
        {
            let slice_count = par2.slice_count_for_file(desc.length);
            total_missing_blocks += slice_count;
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status: FileStatus::Damaged(slice_count),
                valid_slices: vec![false; slice_count as usize],
                missing_slice_count: slice_count,
            });
            continue;
        }

        // If IFSC data is missing for this file, we can't do slice-level verification.
        // Fall back to full-file hash check only. This can happen with truncated PAR2 files.
        if par2.file_checksums(file_id).is_none() {
            let full_ok = verify_full_hash(par2, file_id, access).unwrap_or(false);
            if full_ok {
                let slice_count = par2.slice_count_for_file(desc.length) as usize;
                files.push(FileVerification {
                    file_id: *file_id,
                    filename: desc.filename.clone(),
                    status: FileStatus::Complete,
                    valid_slices: vec![true; slice_count],
                    missing_slice_count: 0,
                });
            } else {
                // Can't determine which slices are bad without IFSC data.
                // Mark all slices as damaged — repair will treat the whole file as needing recovery.
                let slice_count = par2.slice_count_for_file(desc.length);
                total_missing_blocks += slice_count;
                files.push(FileVerification {
                    file_id: *file_id,
                    filename: desc.filename.clone(),
                    status: FileStatus::Damaged(slice_count),
                    valid_slices: vec![false; slice_count as usize],
                    missing_slice_count: slice_count,
                });
            }
            bytes_processed += desc.length;
            if let Some(ref progress) = options.progress {
                progress(ProgressUpdate {
                    stage: ProgressStage::Verifying,
                    current: file_index as u32 + 1,
                    total: total_files,
                    bytes_processed,
                });
            }
            continue;
        }

        // Quick check
        let quick_ok = quick_check_16k(par2, file_id, access).unwrap_or(false);
        if !quick_ok {
            // 16k hash failed; do slice-level check to find which slices are bad
            let valid = verify_slices(par2, file_id, access).unwrap_or_else(|| {
                let n = par2.slice_count_for_file(desc.length) as usize;
                vec![false; n]
            });
            let damaged = valid.iter().filter(|&&v| !v).count() as u32;
            total_missing_blocks += damaged;
            let status = if damaged == 0 {
                FileStatus::Complete
            } else {
                FileStatus::Damaged(damaged)
            };
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status,
                valid_slices: valid.clone(),
                missing_slice_count: damaged,
            });
            continue;
        }

        // Full hash check
        let full_ok = verify_full_hash(par2, file_id, access).unwrap_or(false);
        if full_ok {
            let slice_count = par2.slice_count_for_file(desc.length) as usize;
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status: FileStatus::Complete,
                valid_slices: vec![true; slice_count],
                missing_slice_count: 0,
            });
        } else {
            // Full hash failed but 16k passed; check slice by slice
            let valid = verify_slices(par2, file_id, access).unwrap_or_else(|| {
                let n = par2.slice_count_for_file(desc.length) as usize;
                vec![false; n]
            });
            let damaged = valid.iter().filter(|&&v| !v).count() as u32;
            total_missing_blocks += damaged;
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status: FileStatus::Damaged(damaged),
                valid_slices: valid,
                missing_slice_count: damaged,
            });
        }

        bytes_processed += desc.length;
        if let Some(ref progress) = options.progress {
            progress(ProgressUpdate {
                stage: ProgressStage::Verifying,
                current: file_index as u32 + 1,
                total: total_files,
                bytes_processed,
            });
        }
    }

    let recovery_blocks_available = par2.recovery_block_count();
    let repairable = if total_missing_blocks == 0 {
        Repairability::NotNeeded
    } else if total_missing_blocks <= recovery_blocks_available {
        Repairability::Repairable {
            blocks_needed: total_missing_blocks,
            blocks_available: recovery_blocks_available,
        }
    } else {
        Repairability::Insufficient {
            blocks_needed: total_missing_blocks,
            blocks_available: recovery_blocks_available,
            deficit: total_missing_blocks - recovery_blocks_available,
        }
    };

    VerificationResult {
        files,
        recovery_blocks_available,
        total_missing_blocks,
        repairable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksum;
    use crate::packet::header;
    use crate::par2_set::Par2FileSet;
    use crate::types::SliceChecksum;
    use md5::{Digest, Md5};

    /// Helper to build a complete valid packet (header + body).
    fn make_full_packet(packet_type: &[u8; 16], body: &[u8], recovery_set_id: [u8; 16]) -> Vec<u8> {
        let length = (header::HEADER_SIZE + body.len()) as u64;
        let mut hash_input = Vec::new();
        hash_input.extend_from_slice(&recovery_set_id);
        hash_input.extend_from_slice(packet_type);
        hash_input.extend_from_slice(body);
        let packet_hash: [u8; 16] = Md5::digest(&hash_input).into();

        let mut data = Vec::new();
        data.extend_from_slice(header::MAGIC);
        data.extend_from_slice(&length.to_le_bytes());
        data.extend_from_slice(&packet_hash);
        data.extend_from_slice(&recovery_set_id);
        data.extend_from_slice(packet_type);
        data.extend_from_slice(body);
        data
    }

    /// Build a Par2FileSet and MemoryFileAccess for testing.
    /// Creates a single file with known content and matching checksums.
    fn setup_test_set(
        file_data: &[u8],
        slice_size: u64,
    ) -> (Par2FileSet, MemoryFileAccess, FileId) {
        let file_length = file_data.len() as u64;
        let hash_full = checksum::md5(file_data);
        let hash_16k_data = &file_data[..file_data.len().min(16384)];
        let hash_16k = checksum::md5(hash_16k_data);

        // Compute file_id: MD5(hash_16k || length || filename)
        let filename = b"testfile.dat";
        let mut id_input = Vec::new();
        id_input.extend_from_slice(&hash_16k);
        id_input.extend_from_slice(&file_length.to_le_bytes());
        id_input.extend_from_slice(filename);
        let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();
        let file_id = FileId::from_bytes(file_id_bytes);

        // Build slice checksums
        let num_slices = if file_length == 0 {
            0
        } else {
            ((file_length + slice_size - 1) / slice_size) as usize
        };

        let mut checksums = Vec::new();
        for i in 0..num_slices {
            let offset = i as u64 * slice_size;
            let end = ((offset + slice_size) as usize).min(file_data.len());
            let slice_data = &file_data[offset as usize..end];

            let mut state = SliceChecksumState::new();
            state.update(slice_data);
            let pad_to = if (slice_data.len() as u64) < slice_size {
                Some(slice_size)
            } else {
                None
            };
            let (crc, md5) = state.finalize(pad_to);
            checksums.push(SliceChecksum { crc32: crc, md5 });
        }

        // Build main body and RSID
        let mut main_body = Vec::new();
        main_body.extend_from_slice(&slice_size.to_le_bytes());
        main_body.extend_from_slice(&1u32.to_le_bytes());
        main_body.extend_from_slice(&file_id_bytes);
        let rsid: [u8; 16] = Md5::digest(&main_body).into();

        // File description body
        let mut fd_body = Vec::new();
        fd_body.extend_from_slice(&file_id_bytes);
        fd_body.extend_from_slice(&hash_full);
        fd_body.extend_from_slice(&hash_16k);
        fd_body.extend_from_slice(&file_length.to_le_bytes());
        fd_body.extend_from_slice(filename);
        // Pad to multiple of 4
        while fd_body.len() % 4 != 0 {
            fd_body.push(0);
        }

        // IFSC body
        let mut ifsc_body = Vec::new();
        ifsc_body.extend_from_slice(&file_id_bytes);
        for cs in &checksums {
            ifsc_body.extend_from_slice(&cs.md5);
            ifsc_body.extend_from_slice(&cs.crc32.to_le_bytes());
        }

        // Build stream
        let mut stream = Vec::new();
        stream.extend_from_slice(&make_full_packet(header::TYPE_MAIN, &main_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_FILE_DESC, &fd_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_IFSC, &ifsc_body, rsid));

        let set = Par2FileSet::from_files(&[&stream]).unwrap();

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, file_data.to_vec());

        (set, access, file_id)
    }

    fn setup_test_set_multi(
        files: &[(&[u8], &str)],
        slice_size: u64,
    ) -> (Par2FileSet, MemoryFileAccess, Vec<FileId>) {
        let mut file_ids = Vec::new();
        let mut fd_bodies = Vec::new();
        let mut ifsc_bodies = Vec::new();
        let mut access = MemoryFileAccess::new();

        for &(file_data, filename) in files {
            let file_length = file_data.len() as u64;
            let hash_full = checksum::md5(file_data);
            let hash_16k = checksum::md5(&file_data[..file_data.len().min(16384)]);

            let mut id_input = Vec::new();
            id_input.extend_from_slice(&hash_16k);
            id_input.extend_from_slice(&file_length.to_le_bytes());
            id_input.extend_from_slice(filename.as_bytes());
            let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();
            let file_id = FileId::from_bytes(file_id_bytes);
            file_ids.push(file_id);
            access.add_file(file_id, file_data.to_vec());

            let num_slices = if file_length == 0 {
                0
            } else {
                ((file_length + slice_size - 1) / slice_size) as usize
            };

            let mut checksums = Vec::new();
            for i in 0..num_slices {
                let offset = i as u64 * slice_size;
                let end = ((offset + slice_size) as usize).min(file_data.len());
                let slice_data = &file_data[offset as usize..end];

                let mut state = SliceChecksumState::new();
                state.update(slice_data);
                let pad_to = if (slice_data.len() as u64) < slice_size {
                    Some(slice_size)
                } else {
                    None
                };
                let (crc, md5) = state.finalize(pad_to);
                checksums.push(SliceChecksum { crc32: crc, md5 });
            }

            let mut fd_body = Vec::new();
            fd_body.extend_from_slice(&file_id_bytes);
            fd_body.extend_from_slice(&hash_full);
            fd_body.extend_from_slice(&hash_16k);
            fd_body.extend_from_slice(&file_length.to_le_bytes());
            fd_body.extend_from_slice(filename.as_bytes());
            while fd_body.len() % 4 != 0 {
                fd_body.push(0);
            }
            fd_bodies.push(fd_body);

            let mut ifsc_body = Vec::new();
            ifsc_body.extend_from_slice(&file_id_bytes);
            for cs in &checksums {
                ifsc_body.extend_from_slice(&cs.md5);
                ifsc_body.extend_from_slice(&cs.crc32.to_le_bytes());
            }
            ifsc_bodies.push(ifsc_body);
        }

        let mut main_body = Vec::new();
        main_body.extend_from_slice(&slice_size.to_le_bytes());
        main_body.extend_from_slice(&(file_ids.len() as u32).to_le_bytes());
        for file_id in &file_ids {
            main_body.extend_from_slice(file_id.as_bytes());
        }
        let rsid: [u8; 16] = Md5::digest(&main_body).into();

        let mut stream = Vec::new();
        stream.extend_from_slice(&make_full_packet(header::TYPE_MAIN, &main_body, rsid));
        for fd_body in &fd_bodies {
            stream.extend_from_slice(&make_full_packet(header::TYPE_FILE_DESC, fd_body, rsid));
        }
        for ifsc_body in &ifsc_bodies {
            stream.extend_from_slice(&make_full_packet(header::TYPE_IFSC, ifsc_body, rsid));
        }

        let set = Par2FileSet::from_files(&[&stream]).unwrap();
        (set, access, file_ids)
    }

    #[test]
    fn verify_slices_from_crcs_intact() {
        let file_data = vec![0xABu8; 2048];
        let (set, _access, file_id) = setup_test_set(&file_data, 1024);

        // Compute per-slice CRC32s manually.
        let crc0 = checksum::crc32(&file_data[0..1024]);
        let crc1 = checksum::crc32(&file_data[1024..2048]);

        let result = verify_slices_from_crcs(&set, &file_id, &[crc0, crc1]).unwrap();
        assert_eq!(result, vec![true, true]);
    }

    #[test]
    fn verify_slices_from_crcs_damaged() {
        let file_data = vec![0xABu8; 2048];
        let (set, _access, file_id) = setup_test_set(&file_data, 1024);

        let crc0 = checksum::crc32(&file_data[0..1024]);
        let wrong_crc = 0xDEADBEEF;

        let result = verify_slices_from_crcs(&set, &file_id, &[crc0, wrong_crc]).unwrap();
        assert_eq!(result, vec![true, false]);
    }

    #[test]
    fn verify_slices_from_crcs_with_padding() {
        // 1500 bytes, 1024 slice size -> last slice is 476 bytes, zero-padded to 1024
        let file_data = vec![0xCDu8; 1500];
        let (set, _access, file_id) = setup_test_set(&file_data, 1024);

        let crc0 = checksum::crc32(&file_data[0..1024]);
        // PAR2 checksums include zero-padding on the last slice
        let mut padded_last = file_data[1024..1500].to_vec();
        padded_last.resize(1024, 0);
        let crc1 = checksum::crc32(&padded_last);

        let result = verify_slices_from_crcs(&set, &file_id, &[crc0, crc1]).unwrap();
        assert_eq!(result, vec![true, true]);
    }

    #[test]
    fn verify_intact_file() {
        let file_data = vec![0xABu8; 2048];
        let (set, access, file_id) = setup_test_set(&file_data, 1024);

        // Quick check
        assert_eq!(quick_check_16k(&set, &file_id, &access), Some(true));

        // Full hash
        assert_eq!(verify_full_hash(&set, &file_id, &access), Some(true));

        // Slice verification
        let slices = verify_slices(&set, &file_id, &access).unwrap();
        assert_eq!(slices.len(), 2);
        assert!(slices.iter().all(|&v| v));

        // Full verification
        let result = verify_all(&set, &access);
        assert_eq!(result.files.len(), 1);
        assert!(matches!(result.files[0].status, FileStatus::Complete));
        assert_eq!(result.total_missing_blocks, 0);
        assert!(matches!(result.repairable, Repairability::NotNeeded));
    }

    #[test]
    fn verify_damaged_file() {
        let mut file_data = vec![0xABu8; 2048];
        let (set, mut access, file_id) = setup_test_set(&file_data, 1024);

        // Corrupt the second slice
        file_data[1024] ^= 0xFF;
        file_data[1025] ^= 0xFF;
        access.add_file(file_id, file_data);

        // Slice verification should show second slice damaged
        let slices = verify_slices(&set, &file_id, &access).unwrap();
        assert!(slices[0]); // first slice intact
        assert!(!slices[1]); // second slice damaged

        // Full verification
        let result = verify_all(&set, &access);
        assert_eq!(result.files.len(), 1);
        assert!(matches!(result.files[0].status, FileStatus::Damaged(1)));
        assert_eq!(result.total_missing_blocks, 1);
    }

    #[test]
    fn verify_missing_file() {
        let file_data = vec![0xABu8; 2048];
        let (set, _, _file_id) = setup_test_set(&file_data, 1024);

        // Don't add file to access
        let access = MemoryFileAccess::new();

        let result = verify_all(&set, &access);
        assert_eq!(result.files.len(), 1);
        assert!(matches!(result.files[0].status, FileStatus::Missing));
        assert_eq!(result.total_missing_blocks, 2);
    }

    #[test]
    fn verify_file_with_partial_last_slice() {
        // File is 1500 bytes with 1024-byte slices -> 2 slices, last is 476 bytes
        let file_data = vec![0xCDu8; 1500];
        let (set, access, file_id) = setup_test_set(&file_data, 1024);

        let slices = verify_slices(&set, &file_id, &access).unwrap();
        assert_eq!(slices.len(), 2);
        assert!(slices[0]);
        assert!(slices[1]); // Last partial slice should still verify with padding
    }

    #[test]
    fn quick_check_missing_file() {
        let file_data = vec![0xABu8; 100];
        let (set, _, file_id) = setup_test_set(&file_data, 1024);
        let access = MemoryFileAccess::new();

        assert_eq!(quick_check_16k(&set, &file_id, &access), Some(false));
    }

    #[test]
    fn verify_repairable_assessment() {
        let file_data = vec![0xABu8; 4096];
        let (mut set, mut access, file_id) = setup_test_set(&file_data, 1024);

        // Corrupt data
        let mut corrupted = file_data.clone();
        corrupted[0] ^= 0xFF;
        access.add_file(file_id, corrupted);

        // Add some recovery blocks
        use crate::par2_set::RecoverySlice;
        use bytes::Bytes;
        set.recovery_slices.insert(
            0,
            RecoverySlice {
                exponent: 0,
                data: Bytes::from(vec![0u8; 1024]),
            },
        );
        set.recovery_slices.insert(
            1,
            RecoverySlice {
                exponent: 1,
                data: Bytes::from(vec![0u8; 1024]),
            },
        );

        let result = verify_all(&set, &access);
        // 1 damaged slice, 2 recovery blocks available
        assert!(matches!(
            result.repairable,
            Repairability::Repairable {
                blocks_needed: 1,
                blocks_available: 2
            }
        ));
    }

    #[test]
    fn verify_insufficient_recovery() {
        let file_data = vec![0xABu8; 4096];
        let (set, _, _file_id) = setup_test_set(&file_data, 1024);
        let access = MemoryFileAccess::new(); // file missing = 4 slices needed, 0 available

        let result = verify_all(&set, &access);
        assert!(matches!(
            result.repairable,
            Repairability::Insufficient { .. }
        ));
    }

    #[test]
    fn verify_large_file_multiple_slices() {
        // 10 slices worth of data
        let file_data: Vec<u8> = (0..10240u32).map(|i| (i % 256) as u8).collect();
        let (set, access, file_id) = setup_test_set(&file_data, 1024);

        let slices = verify_slices(&set, &file_id, &access).unwrap();
        assert_eq!(slices.len(), 10);
        assert!(slices.iter().all(|&v| v));
    }

    #[test]
    fn verify_corrupted_first_slice_16k_check_fails() {
        let file_data = vec![0xABu8; 4096];
        let (set, mut access, file_id) = setup_test_set(&file_data, 1024);

        // Corrupt the first byte
        let mut corrupted = file_data.clone();
        corrupted[0] ^= 0xFF;
        access.add_file(file_id, corrupted);

        assert_eq!(quick_check_16k(&set, &file_id, &access), Some(false));
    }

    #[test]
    fn verify_selected_file_ids_only_counts_requested_files() {
        let good = vec![0x11u8; 2048];
        let damaged = vec![0x22u8; 2048];
        let (set, mut access, file_ids) =
            setup_test_set_multi(&[(&good, "good.rar"), (&damaged, "damaged.rar")], 1024);

        let mut corrupted = damaged.clone();
        corrupted[0] ^= 0xFF;
        access.add_file(file_ids[1], corrupted);

        let selected = verify_selected_file_ids(&set, &access, &[file_ids[0]]);
        assert_eq!(selected.files.len(), 1);
        assert_eq!(selected.total_missing_blocks, 0);
        assert!(matches!(selected.repairable, Repairability::NotNeeded));

        let damaged_only = verify_selected_file_ids(&set, &access, &[file_ids[1]]);
        assert_eq!(damaged_only.files.len(), 1);
        assert_eq!(damaged_only.total_missing_blocks, 1);
        assert!(matches!(
            damaged_only.files[0].status,
            FileStatus::Damaged(1)
        ));
    }
}
