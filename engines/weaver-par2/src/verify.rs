use std::collections::HashMap;
use std::io::{self, Read};
use std::path::PathBuf;

use crate::checksum;
use crate::md5_simd;
use crate::par2_set::Par2FileSet;
use crate::types::{
    CancellationToken, FileId, MAX_SLICES_PER_FILE, ProgressCallback, ProgressStage,
    ProgressUpdate, SliceChecksum,
};

const VERIFY_SLICE_CHUNK_BYTES: usize = 64 * 1024;
const VERIFY_SIMD_BATCH_MEMORY_BYTES: usize = 4 * 1024 * 1024;
const VERIFY_SIMD_MAX_LANES: usize = 4;
const QUICK_CHECK_16K_BYTES: usize = 16 * 1024;
const VERIFY_FULL_HASH_CHUNK_BYTES: usize = 1024 * 1024;

/// Abstraction for file I/O during verification and repair.
pub trait FileAccess {
    /// Read a range of bytes from a file identified by its FileId.
    fn read_file_range(&self, file_id: &FileId, offset: u64, len: u64) -> io::Result<Vec<u8>>;

    /// Read bytes into a caller-provided buffer, returning the number of bytes read.
    fn read_file_range_into(
        &self,
        file_id: &FileId,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<usize> {
        let data = self.read_file_range(file_id, offset, dst.len() as u64)?;
        let read_len = data.len().min(dst.len());
        dst[..read_len].copy_from_slice(&data[..read_len]);
        Ok(read_len)
    }

    /// Open a forward-only reader for hot paths that consume a whole file from offset 0.
    fn open_sequential_reader(&self, _file_id: &FileId) -> io::Result<Option<Box<dyn Read>>> {
        Ok(None)
    }

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

    fn read_file_range_into(
        &self,
        file_id: &FileId,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<usize> {
        let data = self
            .files
            .get(file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(0);
        }
        let end = (offset + dst.len()).min(data.len());
        let read_len = end - offset;
        dst[..read_len].copy_from_slice(&data[offset..end]);
        Ok(read_len)
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
    /// Verification could not continue within configured resource limits.
    ResourceLimited { reason: String },
}

/// Overall verification result.
#[derive(Debug, Clone)]
pub struct VerificationResult {
    pub files: Vec<FileVerification>,
    pub recovery_blocks_available: u32,
    pub total_missing_blocks: u32,
    pub repairable: Repairability,
}

impl VerificationResult {
    pub fn needs_repair(&self) -> bool {
        self.total_missing_blocks > 0
            || self
                .files
                .iter()
                .any(|file| !matches!(file.status, FileStatus::Complete))
    }

    pub fn refresh_repairability(&mut self) {
        if let Repairability::ResourceLimited { .. } = self.repairable
            && (self.total_missing_blocks > 0
                || self
                    .files
                    .iter()
                    .any(|file| !matches!(file.status, FileStatus::Complete)))
        {
            return;
        }

        self.repairable = repairability_for_result(
            &self.files,
            self.total_missing_blocks,
            self.recovery_blocks_available,
        );
    }
}

fn repairability_for_result(
    files: &[FileVerification],
    total_missing_blocks: u32,
    recovery_blocks_available: u32,
) -> Repairability {
    if total_missing_blocks == 0
        && files
            .iter()
            .all(|file| matches!(file.status, FileStatus::Complete))
    {
        Repairability::NotNeeded
    } else {
        repairability_for_counts(total_missing_blocks, recovery_blocks_available)
    }
}

fn repairability_for_result_with_resource_limit(
    files: &[FileVerification],
    total_missing_blocks: u32,
    recovery_blocks_available: u32,
    resource_limit_reason: Option<String>,
) -> Repairability {
    match resource_limit_reason {
        Some(reason) => Repairability::ResourceLimited { reason },
        None => repairability_for_result(files, total_missing_blocks, recovery_blocks_available),
    }
}

fn repairability_for_counts(
    total_missing_blocks: u32,
    recovery_blocks_available: u32,
) -> Repairability {
    if total_missing_blocks == 0 {
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
    }
}

fn bounded_slice_count(par2: &Par2FileSet, length: u64) -> Option<usize> {
    let count = usize::try_from(par2.slice_count_for_file(length)).ok()?;
    (count <= MAX_SLICES_PER_FILE).then_some(count)
}

fn resource_limited_verification(file_id: FileId, filename: String) -> FileVerification {
    FileVerification {
        file_id,
        filename,
        status: FileStatus::Damaged(0),
        valid_slices: Vec::new(),
        missing_slice_count: 0,
    }
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
    let mut scratch = [0u8; QUICK_CHECK_16K_BYTES];
    quick_check_16k_with_scratch(par2, file_id, access, &mut scratch)
}

fn quick_check_16k_with_scratch(
    par2: &Par2FileSet,
    file_id: &FileId,
    access: &dyn FileAccess,
    scratch: &mut [u8; QUICK_CHECK_16K_BYTES],
) -> Option<bool> {
    let desc = par2.file_description(file_id)?;

    if !access.file_exists(file_id) {
        return Some(false);
    }

    let read_len = if let Some(mut reader) = access.open_sequential_reader(file_id).ok()? {
        let scratch_len = scratch.len();
        read_from_sequential_reader(&mut *reader, scratch, scratch_len).ok()?
    } else {
        access.read_file_range_into(file_id, 0, scratch).ok()?
    };
    let hash = checksum::md5(&scratch[..read_len]);
    Some(hash == desc.hash_16k)
}

/// Perform full-file MD5 verification.
pub fn verify_full_hash(
    par2: &Par2FileSet,
    file_id: &FileId,
    access: &dyn FileAccess,
) -> Option<bool> {
    let desc = par2.file_description(file_id)?;
    let actual_len = access.file_length(file_id)?;
    if actual_len != desc.length {
        return Some(false);
    }
    verify_full_hash_streaming(desc.hash_full, actual_len, file_id, access)
}

fn verify_quick_and_full_hash(
    par2: &Par2FileSet,
    file_id: &FileId,
    access: &dyn FileAccess,
) -> Option<(bool, bool)> {
    let desc = par2.file_description(file_id)?;
    let actual_len = access.file_length(file_id)?;
    let mut quick_state = checksum::FileHashState::new();
    let mut full_state = None;
    let mut buf = vec![0u8; VERIFY_FULL_HASH_CHUNK_BYTES];
    let mut total_read = 0u64;

    if let Some(mut reader) = access.open_sequential_reader(file_id).ok()? {
        loop {
            let read_len = reader.read(&mut buf).ok()?;
            if read_len == 0 {
                break;
            }
            update_quick_and_full_hash_states(&mut quick_state, &mut full_state, &buf[..read_len]);
            total_read += read_len as u64;
        }
    } else {
        while total_read < actual_len {
            let chunk_len = ((actual_len - total_read) as usize).min(buf.len());
            let read_len = access
                .read_file_range_into(file_id, total_read, &mut buf[..chunk_len])
                .ok()?;
            if read_len == 0 {
                return Some((false, false));
            }
            update_quick_and_full_hash_states(&mut quick_state, &mut full_state, &buf[..read_len]);
            total_read += read_len as u64;
        }
    }

    if total_read != actual_len {
        return Some((false, false));
    }

    let quick_hash = quick_state.finalize();
    let full_hash = full_state.map_or(quick_hash, checksum::FileHashState::finalize);
    Some((
        quick_hash == desc.hash_16k,
        actual_len == desc.length && full_hash == desc.hash_full,
    ))
}

fn verify_full_hash_streaming(
    expected_hash: [u8; 16],
    actual_len: u64,
    file_id: &FileId,
    access: &dyn FileAccess,
) -> Option<bool> {
    let mut state = checksum::FileHashState::new();
    let mut buf = vec![0u8; VERIFY_FULL_HASH_CHUNK_BYTES];

    if let Some(mut reader) = access.open_sequential_reader(file_id).ok()? {
        let mut total_read = 0u64;
        loop {
            let read_len = reader.read(&mut buf).ok()?;
            if read_len == 0 {
                break;
            }
            state.update(&buf[..read_len]);
            total_read += read_len as u64;
        }
        if total_read != actual_len {
            return Some(false);
        }
    } else {
        let mut offset = 0u64;
        while offset < actual_len {
            let chunk_len = ((actual_len - offset) as usize).min(buf.len());
            let read_len = access
                .read_file_range_into(file_id, offset, &mut buf[..chunk_len])
                .ok()?;
            if read_len == 0 {
                return Some(false);
            }
            state.update(&buf[..read_len]);
            offset += read_len as u64;
        }
    }

    Some(state.finalize() == expected_hash)
}

fn update_quick_and_full_hash_states(
    quick_state: &mut checksum::FileHashState,
    full_state: &mut Option<checksum::FileHashState>,
    data: &[u8],
) {
    if data.is_empty() {
        return;
    }
    if let Some(full_state) = full_state.as_mut() {
        full_state.update(data);
        return;
    }

    let quick_remaining = QUICK_CHECK_16K_BYTES.saturating_sub(quick_state.bytes_fed() as usize);
    if data.len() <= quick_remaining {
        quick_state.update(data);
        return;
    }

    quick_state.update(&data[..quick_remaining]);
    let mut cloned = quick_state.clone();
    cloned.update(&data[quick_remaining..]);
    *full_state = Some(cloned);
}

/// Perform slice-level verification of a single file.
///
/// Returns a vector of booleans, one per slice: true = valid, false = damaged.
pub fn verify_slices(
    par2: &Par2FileSet,
    file_id: &FileId,
    access: &dyn FileAccess,
) -> Option<Vec<bool>> {
    let desc = par2.file_description(file_id)?;
    let checksums = par2.file_checksums(file_id)?;
    let slice_size = par2.slice_size;
    let expected_slices = bounded_slice_count(par2, desc.length)?;
    if checksums.len() != expected_slices {
        return Some(vec![false; expected_slices]);
    }

    if !access.file_exists(file_id) {
        return Some(vec![false; expected_slices]);
    }

    if let Some(results) =
        verify_slices_batched_md5(file_id, desc.length, checksums, slice_size, access).ok()?
    {
        return Some(results);
    }

    let mut results = vec![false; expected_slices];
    let mut buf = vec![0u8; VERIFY_SLICE_CHUNK_BYTES];

    for (i, result) in results.iter_mut().enumerate() {
        let offset = i as u64 * slice_size;
        let expected_data_len = desc.length.saturating_sub(offset).min(slice_size);
        let actual = checksum_file_slice_padded(
            file_id,
            offset,
            expected_data_len,
            slice_size,
            access,
            &mut buf,
        )
        .ok()?;
        *result = actual.crc32 == checksums[i].crc32 && actual.md5 == checksums[i].md5;
    }

    Some(results)
}

fn verify_slices_batched_md5(
    file_id: &FileId,
    file_len: u64,
    checksums: &[SliceChecksum],
    slice_size: u64,
    access: &dyn FileAccess,
) -> io::Result<Option<Vec<bool>>> {
    if checksums.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let Ok(slice_size_usize) = usize::try_from(slice_size) else {
        return Ok(None);
    };
    if slice_size_usize == 0 {
        return Ok(None);
    }

    let max_lanes = (VERIFY_SIMD_BATCH_MEMORY_BYTES / slice_size_usize).min(VERIFY_SIMD_MAX_LANES);
    if max_lanes < 2 {
        return Ok(None);
    }

    let mut results = vec![false; checksums.len()];
    let mut buffers = (0..max_lanes)
        .map(|_| vec![0u8; slice_size_usize])
        .collect::<Vec<_>>();

    if let Some(mut reader) = access.open_sequential_reader(file_id)? {
        return verify_slices_batched_md5_from_reader(
            &mut *reader,
            file_len,
            checksums,
            slice_size,
            &mut buffers,
        )
        .map(Some);
    }

    let mut index = 0usize;

    while index < checksums.len() {
        let lanes = max_lanes.min(checksums.len() - index);
        let mut read_lens = Vec::with_capacity(lanes);
        let mut crc32s = Vec::with_capacity(lanes);

        for (lane, buffer) in buffers.iter_mut().take(lanes).enumerate() {
            let slice_index = index + lane;
            let offset = slice_index as u64 * slice_size;
            let expected_data_len = file_len.saturating_sub(offset).min(slice_size);
            let read_len =
                read_file_slice_into(file_id, offset, expected_data_len, access, buffer)?;
            read_lens.push(read_len);
            crc32s.push(checksum::crc32_padded(&buffer[..read_len], slice_size));
        }

        let inputs = buffers
            .iter()
            .take(lanes)
            .zip(read_lens.iter())
            .map(|(buffer, read_len)| &buffer[..*read_len])
            .collect::<Vec<_>>();
        let md5s = md5_simd::md5_multi(&inputs, Some(slice_size));

        for lane in 0..lanes {
            let expected = checksums[index + lane];
            results[index + lane] = crc32s[lane] == expected.crc32 && md5s[lane] == expected.md5;
        }

        index += lanes;
    }

    Ok(Some(results))
}

fn verify_slices_batched_md5_from_reader(
    reader: &mut dyn Read,
    file_len: u64,
    checksums: &[SliceChecksum],
    slice_size: u64,
    buffers: &mut [Vec<u8>],
) -> io::Result<Vec<bool>> {
    let mut results = vec![false; checksums.len()];
    let max_lanes = buffers.len();
    let mut index = 0usize;

    while index < checksums.len() {
        let lanes = max_lanes.min(checksums.len() - index);
        let mut read_lens = Vec::with_capacity(lanes);
        let mut crc32s = Vec::with_capacity(lanes);

        for (lane, buffer) in buffers.iter_mut().take(lanes).enumerate() {
            let slice_index = index + lane;
            let offset = slice_index as u64 * slice_size;
            let expected_data_len = file_len.saturating_sub(offset).min(slice_size) as usize;
            let read_len = read_from_sequential_reader(reader, buffer, expected_data_len)?;
            read_lens.push(read_len);
            crc32s.push(checksum::crc32_padded(&buffer[..read_len], slice_size));
        }

        let inputs = buffers
            .iter()
            .take(lanes)
            .zip(read_lens.iter())
            .map(|(buffer, read_len)| &buffer[..*read_len])
            .collect::<Vec<_>>();
        let md5s = md5_simd::md5_multi(&inputs, Some(slice_size));

        for lane in 0..lanes {
            let expected = checksums[index + lane];
            results[index + lane] = crc32s[lane] == expected.crc32 && md5s[lane] == expected.md5;
        }

        index += lanes;
    }

    Ok(results)
}

fn read_from_sequential_reader(
    reader: &mut dyn Read,
    dst: &mut [u8],
    expected_len: usize,
) -> io::Result<usize> {
    let expected_len = expected_len.min(dst.len());
    let mut read_len = 0usize;
    while read_len < expected_len {
        let n = reader.read(&mut dst[read_len..expected_len])?;
        if n == 0 {
            break;
        }
        read_len += n;
    }
    Ok(read_len)
}

fn read_file_slice_into(
    file_id: &FileId,
    offset: u64,
    expected_data_len: u64,
    access: &dyn FileAccess,
    dst: &mut [u8],
) -> io::Result<usize> {
    let mut consumed = 0u64;

    while consumed < expected_data_len {
        let start = consumed as usize;
        let remaining_capacity = dst.len().saturating_sub(start);
        if remaining_capacity == 0 {
            break;
        }
        let take = (expected_data_len - consumed).min(remaining_capacity as u64) as usize;
        let read_len = access.read_file_range_into(
            file_id,
            offset + consumed,
            &mut dst[start..start + take],
        )?;
        if read_len == 0 {
            break;
        }
        consumed += read_len as u64;
        if read_len < take {
            break;
        }
    }

    Ok(consumed as usize)
}

fn checksum_file_slice_padded(
    file_id: &FileId,
    offset: u64,
    expected_data_len: u64,
    slice_size: u64,
    access: &dyn FileAccess,
    buf: &mut [u8],
) -> io::Result<SliceChecksum> {
    let mut state = checksum::SliceChecksumState::new();
    let mut consumed = 0u64;

    while consumed < expected_data_len {
        let take = (expected_data_len - consumed).min(buf.len() as u64) as usize;
        let read_len = access.read_file_range_into(file_id, offset + consumed, &mut buf[..take])?;
        if read_len == 0 {
            break;
        }
        state.update(&buf[..read_len]);
        consumed += read_len as u64;
        if read_len < take {
            break;
        }
    }

    let (crc32, md5) = state.finalize(Some(slice_size));
    Ok(SliceChecksum { crc32, md5 })
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

/// Verify selected PAR2 file IDs with one rayon task per file. Each file
/// runs the serial single-file pipeline unchanged (no cancellation or
/// progress on this path), so results are identical to
/// [`verify_selected_file_ids`] with the set-level repairability assessed
/// once over the combined outcome.
pub fn verify_selected_file_ids_parallel(
    par2: &Par2FileSet,
    access: &(dyn FileAccess + Sync),
    file_ids: &[FileId],
) -> VerificationResult {
    use rayon::prelude::*;
    let partials: Vec<VerificationResult> = file_ids
        .par_iter()
        .map(|file_id| verify_selected_file_ids(par2, access, std::slice::from_ref(file_id)))
        .collect();

    let mut files = Vec::with_capacity(file_ids.len());
    let mut total_missing_blocks = 0u32;
    let mut resource_limit_reason = None;
    for partial in partials {
        total_missing_blocks = total_missing_blocks.saturating_add(partial.total_missing_blocks);
        if let Repairability::ResourceLimited { reason } = &partial.repairable {
            resource_limit_reason.get_or_insert_with(|| reason.clone());
        }
        files.extend(partial.files);
    }

    let recovery_blocks_available = par2.recovery_block_count();
    let repairable = repairability_for_result_with_resource_limit(
        &files,
        total_missing_blocks,
        recovery_blocks_available,
        resource_limit_reason,
    );
    VerificationResult {
        files,
        recovery_blocks_available,
        total_missing_blocks,
        repairable,
    }
}

/// Overlay `updated` per-file verifications onto `base` (matched by file
/// id), recomputing the totals and set-level repairability. Files absent
/// from `updated` keep their `base` entry — the caller asserts their state
/// on disk has not changed since `base` was computed.
pub fn merge_verification_results(
    par2: &Par2FileSet,
    base: &VerificationResult,
    updated: VerificationResult,
) -> VerificationResult {
    let mut updated_by_id: HashMap<FileId, FileVerification> = updated
        .files
        .into_iter()
        .map(|file| (file.file_id, file))
        .collect();
    let files: Vec<FileVerification> = base
        .files
        .iter()
        .map(|file| {
            updated_by_id
                .remove(&file.file_id)
                .unwrap_or_else(|| file.clone())
        })
        .collect();

    let mut total_missing_blocks = 0u32;
    for file in &files {
        total_missing_blocks = total_missing_blocks.saturating_add(file.missing_slice_count);
    }
    let resource_limit_reason = match &updated.repairable {
        Repairability::ResourceLimited { reason } => Some(reason.clone()),
        _ => match &base.repairable {
            Repairability::ResourceLimited { reason } => Some(reason.clone()),
            _ => None,
        },
    };

    let recovery_blocks_available = par2.recovery_block_count();
    let repairable = repairability_for_result_with_resource_limit(
        &files,
        total_missing_blocks,
        recovery_blocks_available,
        resource_limit_reason,
    );
    VerificationResult {
        files,
        recovery_blocks_available,
        total_missing_blocks,
        repairable,
    }
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
    let mut resource_limit_reason = None;
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
        let Some(slice_count) = bounded_slice_count(par2, desc.length) else {
            resource_limit_reason.get_or_insert_with(|| {
                format!("file {} exceeds verifier slice limits", desc.filename)
            });
            files.push(resource_limited_verification(
                *file_id,
                desc.filename.clone(),
            ));
            continue;
        };
        let slice_count_u32 = slice_count as u32;

        if !access.file_exists(file_id) {
            total_missing_blocks = total_missing_blocks.saturating_add(slice_count_u32);
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status: FileStatus::Missing,
                valid_slices: vec![false; slice_count],
                missing_slice_count: slice_count_u32,
            });
            continue;
        }

        let Some(actual_len) = access.file_length(file_id) else {
            total_missing_blocks = total_missing_blocks.saturating_add(slice_count_u32);
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status: FileStatus::Damaged(slice_count_u32),
                valid_slices: vec![false; slice_count],
                missing_slice_count: slice_count_u32,
            });
            continue;
        };

        // Empty file: PAR2 spec says 0-length files have 0 slices.
        // Just verify the file exists and has the expected length.
        if desc.length == 0 {
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
        if actual_len == 0 && desc.length > 0 {
            total_missing_blocks = total_missing_blocks.saturating_add(slice_count_u32);
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status: FileStatus::Damaged(slice_count_u32),
                valid_slices: vec![false; slice_count],
                missing_slice_count: slice_count_u32,
            });
            continue;
        }

        // If IFSC data is missing for this file, we can't do slice-level verification.
        // Fall back to full-file hash check only. This can happen with truncated PAR2 files.
        if par2.file_checksums(file_id).is_none() {
            let full_ok = verify_full_hash(par2, file_id, access).unwrap_or(false);
            if full_ok {
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
                total_missing_blocks = total_missing_blocks.saturating_add(slice_count_u32);
                files.push(FileVerification {
                    file_id: *file_id,
                    filename: desc.filename.clone(),
                    status: FileStatus::Damaged(slice_count_u32),
                    valid_slices: vec![false; slice_count],
                    missing_slice_count: slice_count_u32,
                });
            }
            bytes_processed += desc.length;
            if let Some(ref progress) = options.progress {
                progress(ProgressUpdate {
                    stage: ProgressStage::Verifying,
                    current: file_index as u32 + 1,
                    total: total_files,
                    bytes_processed,
                    total_bytes: None,
                });
            }
            continue;
        }

        if actual_len != desc.length {
            let valid =
                verify_slices(par2, file_id, access).unwrap_or_else(|| vec![false; slice_count]);
            let damaged = valid.iter().filter(|&&v| !v).count() as u32;
            total_missing_blocks = total_missing_blocks.saturating_add(damaged);
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status: FileStatus::Damaged(damaged),
                valid_slices: valid,
                missing_slice_count: damaged,
            });
            continue;
        }

        let (quick_ok, full_ok) =
            verify_quick_and_full_hash(par2, file_id, access).unwrap_or((false, false));
        if !quick_ok {
            // 16k hash failed; do slice-level check to find which slices are bad
            let valid =
                verify_slices(par2, file_id, access).unwrap_or_else(|| vec![false; slice_count]);
            let damaged = valid.iter().filter(|&&v| !v).count() as u32;
            total_missing_blocks = total_missing_blocks.saturating_add(damaged);
            let status = if damaged == 0 {
                // Slice checks can identify usable blocks, but only a full
                // length+MD5 match is allowed to mark a file complete.
                FileStatus::Damaged(0)
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

        if full_ok {
            files.push(FileVerification {
                file_id: *file_id,
                filename: desc.filename.clone(),
                status: FileStatus::Complete,
                valid_slices: vec![true; slice_count],
                missing_slice_count: 0,
            });
        } else {
            // Full hash failed but 16k passed; check slice by slice
            let valid =
                verify_slices(par2, file_id, access).unwrap_or_else(|| vec![false; slice_count]);
            let damaged = valid.iter().filter(|&&v| !v).count() as u32;
            total_missing_blocks = total_missing_blocks.saturating_add(damaged);
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
                total_bytes: None,
            });
        }
    }

    let recovery_blocks_available = par2.recovery_block_count();
    let repairable = repairability_for_result_with_resource_limit(
        &files,
        total_missing_blocks,
        recovery_blocks_available,
        resource_limit_reason,
    );

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
    use crate::checksum::{self, SliceChecksumState};
    use crate::packet::header;
    use crate::par2_set::Par2FileSet;
    use crate::types::SliceChecksum;
    use md5::{Digest, Md5};
    use std::collections::HashMap;
    use std::io::{self, Cursor};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

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
            file_length.div_ceil(slice_size) as usize
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
                file_length.div_ceil(slice_size) as usize
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

    fn setup_oversized_file_set() -> Par2FileSet {
        let slice_size = 4u64;
        let file_length = (MAX_SLICES_PER_FILE as u64 + 1) * slice_size;
        let filename = b"huge.dat";
        let hash_full = [0u8; 16];
        let hash_16k = [0u8; 16];

        let mut id_input = Vec::new();
        id_input.extend_from_slice(&hash_16k);
        id_input.extend_from_slice(&file_length.to_le_bytes());
        id_input.extend_from_slice(filename);
        let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();

        let mut main_body = Vec::new();
        main_body.extend_from_slice(&slice_size.to_le_bytes());
        main_body.extend_from_slice(&1u32.to_le_bytes());
        main_body.extend_from_slice(&file_id_bytes);
        let rsid: [u8; 16] = Md5::digest(&main_body).into();

        let mut fd_body = Vec::new();
        fd_body.extend_from_slice(&file_id_bytes);
        fd_body.extend_from_slice(&hash_full);
        fd_body.extend_from_slice(&hash_16k);
        fd_body.extend_from_slice(&file_length.to_le_bytes());
        fd_body.extend_from_slice(filename);
        while fd_body.len() % 4 != 0 {
            fd_body.push(0);
        }

        let mut stream = Vec::new();
        stream.extend_from_slice(&make_full_packet(header::TYPE_MAIN, &main_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_FILE_DESC, &fd_body, rsid));

        Par2FileSet::from_files(&[&stream]).unwrap()
    }

    struct ReadIntoOnlyAccess {
        files: HashMap<FileId, Vec<u8>>,
    }

    impl FileAccess for ReadIntoOnlyAccess {
        fn read_file_range(
            &self,
            _file_id: &FileId,
            _offset: u64,
            _len: u64,
        ) -> io::Result<Vec<u8>> {
            panic!("read_file_range should not be used by quick_check_16k")
        }

        fn read_file_range_into(
            &self,
            file_id: &FileId,
            offset: u64,
            dst: &mut [u8],
        ) -> io::Result<usize> {
            let data = self
                .files
                .get(file_id)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
            let offset = offset as usize;
            if offset >= data.len() {
                return Ok(0);
            }
            let end = (offset + dst.len()).min(data.len());
            let read_len = end - offset;
            dst[..read_len].copy_from_slice(&data[offset..end]);
            Ok(read_len)
        }

        fn file_exists(&self, file_id: &FileId) -> bool {
            self.files.contains_key(file_id)
        }

        fn file_length(&self, file_id: &FileId) -> Option<u64> {
            self.files.get(file_id).map(|data| data.len() as u64)
        }

        fn read_file(&self, file_id: &FileId) -> io::Result<Vec<u8>> {
            self.files
                .get(file_id)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
        }

        fn write_file_range(
            &mut self,
            _file_id: &FileId,
            _offset: u64,
            _data: &[u8],
        ) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "test access is read-only",
            ))
        }
    }

    struct SequentialOnlyAccess {
        files: HashMap<FileId, Vec<u8>>,
    }

    impl FileAccess for SequentialOnlyAccess {
        fn read_file_range(
            &self,
            _file_id: &FileId,
            _offset: u64,
            _len: u64,
        ) -> io::Result<Vec<u8>> {
            panic!("read_file_range should not be used when a sequential reader is available")
        }

        fn read_file_range_into(
            &self,
            _file_id: &FileId,
            _offset: u64,
            _dst: &mut [u8],
        ) -> io::Result<usize> {
            panic!("read_file_range_into should not be used when a sequential reader is available")
        }

        fn open_sequential_reader(&self, file_id: &FileId) -> io::Result<Option<Box<dyn Read>>> {
            let data = self
                .files
                .get(file_id)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
            Ok(Some(Box::new(Cursor::new(data.clone()))))
        }

        fn file_exists(&self, file_id: &FileId) -> bool {
            self.files.contains_key(file_id)
        }

        fn file_length(&self, file_id: &FileId) -> Option<u64> {
            self.files.get(file_id).map(|data| data.len() as u64)
        }

        fn read_file(&self, file_id: &FileId) -> io::Result<Vec<u8>> {
            self.files
                .get(file_id)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
        }

        fn write_file_range(
            &mut self,
            _file_id: &FileId,
            _offset: u64,
            _data: &[u8],
        ) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "test access is read-only",
            ))
        }
    }

    struct CountingSequentialAccess {
        files: HashMap<FileId, Vec<u8>>,
        open_calls: Arc<AtomicUsize>,
    }

    impl FileAccess for CountingSequentialAccess {
        fn read_file_range(
            &self,
            _file_id: &FileId,
            _offset: u64,
            _len: u64,
        ) -> io::Result<Vec<u8>> {
            panic!("read_file_range should not be used when a sequential reader is available")
        }

        fn read_file_range_into(
            &self,
            _file_id: &FileId,
            _offset: u64,
            _dst: &mut [u8],
        ) -> io::Result<usize> {
            panic!("read_file_range_into should not be used when a sequential reader is available")
        }

        fn open_sequential_reader(&self, file_id: &FileId) -> io::Result<Option<Box<dyn Read>>> {
            self.open_calls.fetch_add(1, Ordering::Relaxed);
            let data = self
                .files
                .get(file_id)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
            Ok(Some(Box::new(Cursor::new(data.clone()))))
        }

        fn file_exists(&self, file_id: &FileId) -> bool {
            self.files.contains_key(file_id)
        }

        fn file_length(&self, file_id: &FileId) -> Option<u64> {
            self.files.get(file_id).map(|data| data.len() as u64)
        }

        fn read_file(&self, _file_id: &FileId) -> io::Result<Vec<u8>> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "force streaming path in this test",
            ))
        }

        fn write_file_range(
            &mut self,
            _file_id: &FileId,
            _offset: u64,
            _data: &[u8],
        ) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "test access is read-only",
            ))
        }
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
    fn quick_check_short_file_matches_and_detects_corruption() {
        let file_data = b"short-par2-file".to_vec();
        let (set, mut access, file_id) = setup_test_set(&file_data, 1024);

        assert_eq!(quick_check_16k(&set, &file_id, &access), Some(true));

        let mut corrupted = file_data.clone();
        corrupted[3] ^= 0xFF;
        access.add_file(file_id, corrupted);

        assert_eq!(quick_check_16k(&set, &file_id, &access), Some(false));
    }

    #[test]
    fn quick_check_exact_16k_boundary_matches_and_detects_corruption() {
        let file_data = (0..QUICK_CHECK_16K_BYTES)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        let (set, mut access, file_id) = setup_test_set(&file_data, 4096);

        assert_eq!(quick_check_16k(&set, &file_id, &access), Some(true));

        let mut corrupted = file_data.clone();
        corrupted[QUICK_CHECK_16K_BYTES - 1] ^= 0x55;
        access.add_file(file_id, corrupted);

        assert_eq!(quick_check_16k(&set, &file_id, &access), Some(false));
    }

    #[test]
    fn quick_check_uses_read_into_path() {
        let file_data = vec![0x5Au8; 4096];
        let (set, access, file_id) = setup_test_set(&file_data, 1024);
        let access = ReadIntoOnlyAccess {
            files: access.files,
        };

        assert_eq!(quick_check_16k(&set, &file_id, &access), Some(true));
    }

    #[test]
    fn quick_check_uses_sequential_reader_when_available() {
        let file_data = vec![0x6Bu8; 4096];
        let (set, access, file_id) = setup_test_set(&file_data, 1024);
        let access = SequentialOnlyAccess {
            files: access.files,
        };

        assert_eq!(quick_check_16k(&set, &file_id, &access), Some(true));
    }

    #[test]
    fn verify_full_hash_uses_sequential_reader_when_available() {
        let file_data = (0..(QUICK_CHECK_16K_BYTES + 4096))
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        let (set, access, file_id) = setup_test_set(&file_data, 1024);
        let access = SequentialOnlyAccess {
            files: access.files,
        };

        assert_eq!(verify_full_hash(&set, &file_id, &access), Some(true));
    }

    #[test]
    fn verify_selected_file_ids_uses_single_sequential_pass_for_healthy_file() {
        let file_data = (0..(QUICK_CHECK_16K_BYTES + 4096))
            .map(|i| (i % 241) as u8)
            .collect::<Vec<_>>();
        let (set, access, file_id) = setup_test_set(&file_data, 1024);
        let open_calls = Arc::new(AtomicUsize::new(0));
        let access = CountingSequentialAccess {
            files: access.files,
            open_calls: open_calls.clone(),
        };

        let verification = verify_selected_file_ids(&set, &access, &[file_id]);

        assert_eq!(verification.total_missing_blocks, 0);
        assert!(matches!(verification.files[0].status, FileStatus::Complete));
        assert_eq!(open_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn verify_slices_uses_batched_sequential_reader_when_available() {
        let file_data = (0..8192)
            .map(|i| (i as u8).wrapping_mul(13).wrapping_add(7))
            .collect::<Vec<_>>();
        let (set, access, file_id) = setup_test_set(&file_data, 1024);
        let access = SequentialOnlyAccess {
            files: access.files,
        };

        let slices = verify_slices(&set, &file_id, &access).unwrap();
        assert_eq!(slices, vec![true; 8]);
    }

    #[test]
    fn verifier_falls_back_to_read_into_when_no_sequential_reader_exists() {
        let file_data = (0..8192)
            .map(|i| (i as u8).wrapping_mul(17).wrapping_add(11))
            .collect::<Vec<_>>();
        let (set, access, file_id) = setup_test_set(&file_data, 1024);
        let access = ReadIntoOnlyAccess {
            files: access.files,
        };

        assert_eq!(verify_full_hash(&set, &file_id, &access), Some(true));
        assert_eq!(verify_slices(&set, &file_id, &access), Some(vec![true; 8]));
    }

    #[test]
    fn verify_selected_file_ids_reuses_quick_check_scratch_without_allocating_reads() {
        let file_a = vec![0x11u8; 2048];
        let file_b = (0..(QUICK_CHECK_16K_BYTES + 257))
            .map(|i| (i % 239) as u8)
            .collect::<Vec<_>>();
        let files = [
            (file_a.as_slice(), "alpha.bin"),
            (file_b.as_slice(), "beta.bin"),
        ];
        let (set, access, file_ids) = setup_test_set_multi(&files, 1024);
        let access = ReadIntoOnlyAccess {
            files: access.files,
        };

        let verification = verify_selected_file_ids_with_options(
            &set,
            &access,
            &file_ids,
            &VerifyOptions::default(),
        );

        assert_eq!(verification.files.len(), 2);
        assert!(
            verification
                .files
                .iter()
                .all(|file| matches!(file.status, FileStatus::Complete))
        );
        assert_eq!(verification.total_missing_blocks, 0);
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
                data: Bytes::from(vec![0u8; 1024]).into(),
            },
        );
        set.recovery_slices.insert(
            1,
            RecoverySlice {
                exponent: 1,
                data: Bytes::from(vec![0u8; 1024]).into(),
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
    fn verify_resource_limited_file_does_not_inflate_missing_blocks() {
        let set = setup_oversized_file_set();
        let access = MemoryFileAccess::new();

        let result = verify_all(&set, &access);

        assert_eq!(result.total_missing_blocks, 0);
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0].missing_slice_count, 0);
        assert!(result.files[0].valid_slices.is_empty());
        assert!(matches!(result.files[0].status, FileStatus::Damaged(0)));
        match result.repairable {
            Repairability::ResourceLimited { reason } => {
                assert!(reason.contains("huge.dat"));
            }
            other => panic!("expected resource-limited repairability, got {other:?}"),
        }
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

    #[test]
    fn parallel_selected_verify_matches_serial() {
        let intact = vec![0x11u8; 4096];
        let damaged = vec![0x22u8; 4096];
        let truncated = vec![0x33u8; 4096];
        let (set, mut access, file_ids) = setup_test_set_multi(
            &[
                (&intact, "intact.rar"),
                (&damaged, "damaged.rar"),
                (&truncated, "truncated.rar"),
            ],
            1024,
        );

        let mut corrupted = damaged.clone();
        corrupted[1500] ^= 0xFF;
        corrupted[3000] ^= 0xFF;
        access.add_file(file_ids[1], corrupted);
        access.add_file(file_ids[2], truncated[..2500].to_vec());

        let serial = verify_selected_file_ids(&set, &access, &file_ids);
        let parallel = verify_selected_file_ids_parallel(&set, &access, &file_ids);
        assert!(serial.total_missing_blocks > 0, "fixture must have damage");
        assert_eq!(
            format!("{serial:?}"),
            format!("{parallel:?}"),
            "parallel selected verify must match the serial pipeline exactly"
        );

        // Merging the post-repair way: overlaying a subset onto a base
        // keeps untouched entries and recomputes the totals.
        let base = verify_selected_file_ids(&set, &access, &file_ids);
        let fixed_subset = {
            let mut fixed = MemoryFileAccess::new();
            fixed.add_file(file_ids[0], intact.clone());
            fixed.add_file(file_ids[1], damaged.clone());
            fixed.add_file(file_ids[2], truncated.clone());
            verify_selected_file_ids_parallel(&set, &fixed, &file_ids[1..])
        };
        let merged = merge_verification_results(&set, &base, fixed_subset);
        assert_eq!(merged.files.len(), file_ids.len());
        assert_eq!(merged.total_missing_blocks, 0);
        assert!(
            merged
                .files
                .iter()
                .all(|file| matches!(file.status, FileStatus::Complete)),
            "merged result must show all files complete: {merged:?}"
        );
        assert!(matches!(merged.repairable, Repairability::NotNeeded));
    }

    #[test]
    fn refresh_repairability_recomputes_after_missing_count_changes() {
        let mut result = VerificationResult {
            files: Vec::new(),
            recovery_blocks_available: 40,
            total_missing_blocks: 1380,
            repairable: Repairability::Insufficient {
                blocks_needed: 1380,
                blocks_available: 40,
                deficit: 1340,
            },
        };

        result.total_missing_blocks = 40;
        result.refresh_repairability();

        assert!(matches!(
            result.repairable,
            Repairability::Repairable {
                blocks_needed: 40,
                blocks_available: 40
            }
        ));
    }
}
