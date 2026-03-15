//! PAR2 repair orchestration using Reed-Solomon decoding over GF(2^16).
//!
//! This module implements the full repair pipeline:
//! 1. Analyze verification results to identify missing/damaged slices
//! 2. Build a repair plan (decode matrix from Gaussian elimination)
//! 3. Execute repair by reading recovery data, XOR-ing out known contributions,
//!    and multiplying by the decode matrix to reconstruct missing slices
//! 4. Write repaired data back to files

use rayon::prelude::*;
use tracing::{debug, info, warn};

use crate::error::{Par2Error, Result};
use crate::gf;
use crate::matrix;
use crate::par2_set::Par2FileSet;
use crate::types::{CancellationToken, FileId, ProgressCallback, ProgressStage, ProgressUpdate};
use crate::verify::{FileAccess, Repairability, VerificationResult};

/// A plan for repairing missing/damaged slices.
#[derive(Debug, Clone)]
pub struct RepairPlan {
    /// The missing input slices as (FileId, slice_index) pairs.
    pub missing_slices: Vec<(FileId, u32)>,
    /// Global indices of missing slices in the concatenated input slice ordering.
    pub missing_global_indices: Vec<usize>,
    /// Which recovery block exponents to use (one per missing slice).
    pub recovery_exponents: Vec<u32>,
    /// The inverted decode matrix rows (each row has `missing_slices.len()` entries).
    pub decode_matrix: Vec<Vec<u16>>,
    /// The PAR2 slice size in bytes.
    pub slice_size: u64,
    /// Constants for all input slices (needed for XOR-out step).
    pub constants: Vec<u16>,
    /// Total number of input slices across all files.
    pub total_input_slices: usize,
    /// Mapping from global slice index to (FileId, local_slice_index).
    pub global_to_file: Vec<(FileId, u32)>,
}

/// Plan a repair operation based on verification results.
///
/// Examines the verification result to find missing/damaged slices, selects
/// recovery blocks to use, and builds the decode matrix via Gaussian elimination.
pub fn plan_repair(
    par2_set: &Par2FileSet,
    verification: &VerificationResult,
) -> Result<RepairPlan> {
    // Check repairability.
    match &verification.repairable {
        Repairability::NotNeeded => {
            return Err(Par2Error::ReedSolomonError {
                reason: "no repair needed".to_string(),
            });
        }
        Repairability::Insufficient {
            blocks_needed,
            blocks_available,
            deficit,
        } => {
            return Err(Par2Error::InsufficientRecoveryData {
                needed: *blocks_needed,
                available: *blocks_available,
                deficit: *deficit,
            });
        }
        Repairability::Repairable { .. } => {}
    }

    // Build the global slice index mapping.
    // Global ordering: files in order of recovery_file_ids, slices in order within each file.
    let mut global_to_file: Vec<(FileId, u32)> = Vec::new();
    for file_id in &par2_set.recovery_file_ids {
        if let Some(desc) = par2_set.file_description(file_id) {
            let slice_count = par2_set.slice_count_for_file(desc.length);
            for s in 0..slice_count {
                global_to_file.push((*file_id, s));
            }
        }
    }
    let total_input_slices = global_to_file.len();

    // Identify missing slices (global indices).
    let mut missing_slices: Vec<(FileId, u32)> = Vec::new();
    let mut missing_global_indices: Vec<usize> = Vec::new();

    let mut global_idx = 0usize;
    for file_id in &par2_set.recovery_file_ids {
        let desc = match par2_set.file_description(file_id) {
            Some(d) => d,
            None => continue,
        };
        let slice_count = par2_set.slice_count_for_file(desc.length) as usize;

        // Find this file's verification result.
        let file_verif = verification.files.iter().find(|fv| fv.file_id == *file_id);

        for s in 0..slice_count {
            let is_valid = file_verif
                .map(|fv| fv.valid_slices.get(s).copied().unwrap_or(false))
                .unwrap_or(false);

            if !is_valid {
                missing_slices.push((*file_id, s as u32));
                missing_global_indices.push(global_idx + s);
            }
        }
        global_idx += slice_count;
    }

    let missing_count = missing_slices.len();
    debug!("repair: {missing_count} missing slices identified");

    // Select recovery exponents. Try the first N available; if the decode
    // matrix is singular (bad recovery block), skip that exponent and retry
    // with the next available one.
    let all_exponents: Vec<u32> = par2_set.recovery_slices.keys().copied().collect();

    if all_exponents.len() < missing_count {
        return Err(Par2Error::InsufficientRecoveryData {
            needed: missing_count as u32,
            available: all_exponents.len() as u32,
            deficit: (missing_count - all_exponents.len()) as u32,
        });
    }

    // Compute constants for all input slices.
    let constants = gf::input_slice_constants(total_input_slices);

    // Try building the decode matrix, skipping singular recovery blocks.
    let mut skip_set: Vec<usize> = Vec::new();
    let (recovery_exponents, decode) = loop {
        let selected: Vec<u32> = all_exponents
            .iter()
            .enumerate()
            .filter(|(i, _)| !skip_set.contains(i))
            .map(|(_, &e)| e)
            .take(missing_count)
            .collect();

        if selected.len() < missing_count {
            return Err(Par2Error::InsufficientRecoveryData {
                needed: missing_count as u32,
                available: selected.len() as u32,
                deficit: (missing_count - selected.len()) as u32,
            });
        }

        match matrix::build_decode_matrix(&missing_global_indices, &selected, &constants) {
            Ok(decode) => break (selected, decode),
            Err(Par2Error::ReedSolomonError { .. }) => {
                // Find which exponent to skip: try removing each one from the
                // current selection until we find the culprit, or just skip
                // the last one added (simplest heuristic).
                let skip_idx = all_exponents
                    .iter()
                    .position(|e| *e == *selected.last().unwrap())
                    .unwrap();
                warn!(
                    "recovery exponent {} produced singular matrix, skipping",
                    selected.last().unwrap()
                );
                skip_set.push(skip_idx);
            }
            Err(e) => return Err(e),
        }
    };

    info!(
        "repair plan: {} missing slices, {} recovery blocks selected",
        missing_count,
        recovery_exponents.len()
    );

    Ok(RepairPlan {
        missing_slices,
        missing_global_indices,
        recovery_exponents,
        decode_matrix: decode.data,
        slice_size: par2_set.slice_size,
        constants,
        total_input_slices,
        global_to_file,
    })
}

/// Options controlling repair execution.
#[derive(Default)]
pub struct RepairOptions {
    /// If set, repair will check this token and stop early if cancelled.
    pub cancel: Option<CancellationToken>,
    /// If set, called with progress updates during repair.
    pub progress: Option<ProgressCallback>,
    /// Maximum memory (bytes) to use for repair buffers.
    ///
    /// If `None`, all recovery data is loaded into memory at once.
    /// If `Some(limit)`, recovery data is processed in word-range chunks.
    pub memory_limit: Option<usize>,
}

/// Execute a repair plan, reading recovery data and writing repaired slices.
///
/// The algorithm:
/// 1. For each recovery block, read its data
/// 2. XOR out contributions from all *known* input slices
/// 3. Multiply the adjusted recovery data by the decode matrix to get missing slices
/// 4. Write repaired slices back to files
pub fn execute_repair(
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
    file_access: &mut dyn FileAccess,
) -> Result<()> {
    execute_repair_with_options(plan, par2_set, file_access, &RepairOptions::default())
}

/// Load recovery block data into buffers, one per selected recovery exponent.
/// Each buffer is resized to `plan.slice_size`.
pub fn prepare_recovery_buffers(
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
    options: &RepairOptions,
) -> Result<Vec<Vec<u8>>> {
    let n = plan.missing_slices.len();
    let slice_size = plan.slice_size as usize;

    let mut recovery_data: Vec<Vec<u8>> = Vec::with_capacity(n);
    for (i, &exp) in plan.recovery_exponents.iter().enumerate() {
        if let Some(ref cancel) = options.cancel
            && cancel.is_cancelled()
        {
            return Err(Par2Error::Cancelled);
        }
        let rs = par2_set
            .recovery_slices
            .get(&exp)
            .ok_or_else(|| Par2Error::ReedSolomonError {
                reason: format!("recovery block with exponent {exp} not found"),
            })?;
        let mut data = rs.data.to_vec();
        data.resize(slice_size, 0);
        recovery_data.push(data);

        if let Some(ref progress) = options.progress {
            progress(ProgressUpdate {
                stage: ProgressStage::ReadingRecovery,
                current: i as u32 + 1,
                total: n as u32,
                bytes_processed: (i + 1) as u64 * slice_size as u64,
            });
        }
    }

    Ok(recovery_data)
}

/// XOR-out a single known-good input slice's contribution from all recovery buffers.
///
/// `global_idx` is the slice's position in the global input ordering.
/// `input_data` is the slice data (will be zero-padded to `plan.slice_size` if shorter).
pub fn xor_out_slice(
    recovery_buffers: &mut [Vec<u8>],
    plan: &RepairPlan,
    global_idx: usize,
    input_data: &[u8],
) {
    let slice_size = plan.slice_size as usize;
    assert!(
        slice_size.is_multiple_of(2),
        "PAR2 slice_size must be a multiple of 2"
    );

    // Pad input data to slice_size if needed.
    let padded;
    let data = if input_data.len() < slice_size {
        padded = {
            let mut v = input_data.to_vec();
            v.resize(slice_size, 0);
            v
        };
        &padded[..]
    } else {
        &input_data[..slice_size]
    };

    let constant = plan.constants[global_idx];

    // Parallelize across recovery buffers — each (factor, buffer) pair is independent.
    recovery_buffers
        .par_iter_mut()
        .zip(plan.recovery_exponents.par_iter())
        .for_each(|(recovery, &exp)| {
            let factor = gf::pow(constant, exp);
            if factor != 0 {
                crate::gf_simd::mul_acc_region(factor, data, &mut recovery[..slice_size]);
            }
        });
}

/// Multiply adjusted recovery data by the decode matrix and write repaired slices.
pub fn reconstruct_and_write(
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
    recovery_buffers: Vec<Vec<u8>>,
    file_access: &mut dyn FileAccess,
    options: &RepairOptions,
) -> Result<()> {
    let check_cancel = |options: &RepairOptions| -> Result<()> {
        if let Some(ref cancel) = options.cancel
            && cancel.is_cancelled()
        {
            return Err(Par2Error::Cancelled);
        }
        Ok(())
    };

    let n = plan.missing_slices.len();
    if n == 0 {
        return Ok(());
    }

    let slice_size = plan.slice_size as usize;
    assert!(
        slice_size.is_multiple_of(2),
        "PAR2 slice_size must be a multiple of 2"
    );
    let word_count = slice_size / 2;

    // Step 2: Multiply adjusted recovery data by decode matrix.
    info!("reconstructing {} missing slices", n);

    // Determine chunk size (for memory budget support).
    let base_chunk_size = 4096usize;
    let chunk_size = if let Some(limit) = options.memory_limit {
        let max_chunk_words = if n > 0 {
            limit / (n * 2)
        } else {
            base_chunk_size
        };
        max_chunk_words.max(1).min(word_count)
    } else {
        base_chunk_size
    };

    let mut repaired_slices: Vec<Vec<u8>> = vec![vec![0u8; slice_size]; n];

    let word_chunks: Vec<usize> = (0..word_count).step_by(chunk_size).collect();
    let total_chunks = word_chunks.len() as u32;

    check_cancel(options)?;

    let chunk_results: Vec<Vec<Vec<u8>>> = word_chunks
        .par_iter()
        .map(|&chunk_start| {
            let chunk_end = (chunk_start + chunk_size).min(word_count);
            let chunk_len = chunk_end - chunk_start;
            let byte_start = chunk_start * 2;
            let byte_len = chunk_len * 2;

            let mut chunk_output: Vec<Vec<u8>> = vec![vec![0u8; byte_len]; n];

            // Transposed loop: iterate recovery buffers in the outer loop so each
            // buffer is loaded into cache once and reused for all N output slices.
            // Uses multi-region kernel to read src once per SIMD chunk across all
            // destinations. GF addition is commutative — accumulation order doesn't
            // matter.
            for (r, recovery) in recovery_buffers.iter().enumerate() {
                let src = &recovery[byte_start..byte_start + byte_len];
                let mut pairs: Vec<crate::gf_simd::FactorDst<'_>> = chunk_output
                    .iter_mut()
                    .enumerate()
                    .filter_map(|(j, chunk_out)| {
                        let factor = plan.decode_matrix[j][r];
                        if factor != 0 {
                            Some(crate::gf_simd::FactorDst {
                                factor,
                                dst: chunk_out.as_mut_slice(),
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                if !pairs.is_empty() {
                    crate::gf_simd::mul_acc_multi_region(&mut pairs, src);
                }
            }

            chunk_output
        })
        .collect();

    check_cancel(options)?;

    // Reassemble chunks into repaired slices.
    for (ci, &chunk_start) in word_chunks.iter().enumerate() {
        let chunk_end = (chunk_start + chunk_size).min(word_count);
        let chunk_len = chunk_end - chunk_start;
        let byte_start = chunk_start * 2;

        for j in 0..n {
            repaired_slices[j][byte_start..byte_start + chunk_len * 2]
                .copy_from_slice(&chunk_results[ci][j]);
        }

        if let Some(ref progress) = options.progress {
            progress(ProgressUpdate {
                stage: ProgressStage::Repairing,
                current: ci as u32 + 1,
                total: total_chunks,
                bytes_processed: (ci + 1) as u64 * chunk_size as u64 * 2,
            });
        }
    }

    // Step 3: Write repaired slices back to files.
    info!("writing repaired slices to files");

    for (j, (file_id, local_slice)) in plan.missing_slices.iter().enumerate() {
        check_cancel(options)?;

        let offset = *local_slice as u64 * plan.slice_size;

        let desc =
            par2_set
                .file_description(file_id)
                .ok_or_else(|| Par2Error::ReedSolomonError {
                    reason: format!("file description not found for {file_id}"),
                })?;
        let file_end = desc.length;
        let slice_end = offset + plan.slice_size;
        let write_len = if slice_end > file_end {
            (file_end - offset) as usize
        } else {
            slice_size
        };

        file_access
            .write_file_range(file_id, offset, &repaired_slices[j][..write_len])
            .map_err(|e| Par2Error::RepairWriteFailed {
                filename: desc.filename.clone(),
                offset,
                source: e,
            })?;

        debug!(
            "repaired slice {local_slice} of file {} ({write_len} bytes at offset {offset})",
            desc.filename
        );

        if let Some(ref progress) = options.progress {
            progress(ProgressUpdate {
                stage: ProgressStage::WritingRepaired,
                current: j as u32 + 1,
                total: n as u32,
                bytes_processed: (j + 1) as u64 * slice_size as u64,
            });
        }
    }

    info!("repair complete: {} slices restored", n);
    Ok(())
}

/// Execute a repair plan with cancellation, progress, and memory budget support.
pub fn execute_repair_with_options(
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
    file_access: &mut dyn FileAccess,
    options: &RepairOptions,
) -> Result<()> {
    let n = plan.missing_slices.len();
    if n == 0 {
        return Ok(());
    }

    let slice_size = plan.slice_size as usize;
    assert!(
        slice_size.is_multiple_of(2),
        "PAR2 slice_size must be a multiple of 2"
    );

    let missing_set: std::collections::HashSet<usize> =
        plan.missing_global_indices.iter().copied().collect();

    // Step 1: Read recovery slice data.
    info!("reading recovery blocks and computing adjusted syndromes");
    let mut recovery_buffers = prepare_recovery_buffers(plan, par2_set, options)?;

    // XOR out known input slice contributions from each recovery block.
    for global_idx in 0..plan.total_input_slices {
        if missing_set.contains(&global_idx) {
            continue;
        }

        // Check cancellation every 64 input slices.
        if global_idx % 64 == 0
            && let Some(ref cancel) = options.cancel
            && cancel.is_cancelled()
        {
            return Err(Par2Error::Cancelled);
        }

        let (file_id, local_slice) = plan.global_to_file[global_idx];
        let offset = local_slice as u64 * plan.slice_size;
        let mut input_data = file_access
            .read_file_range(&file_id, offset, plan.slice_size)
            .map_err(Par2Error::Io)?;
        input_data.resize(slice_size, 0);

        xor_out_slice(&mut recovery_buffers, plan, global_idx, &input_data);
    }

    // Step 2 + 3: Multiply and write.
    reconstruct_and_write(plan, par2_set, recovery_buffers, file_access, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksum::{self, SliceChecksumState};
    use crate::packet::header;
    use crate::par2_set::{Par2FileSet, RecoverySlice};
    use crate::types::SliceChecksum;
    use crate::verify::{self, MemoryFileAccess};
    use bytes::Bytes;
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

    /// Create a PAR2 file set with known data and recovery blocks.
    ///
    /// Returns (par2_set, original_file_data, file_id).
    fn setup_repairable_set(
        file_data: &[u8],
        slice_size: u64,
        num_recovery: usize,
    ) -> (Par2FileSet, FileId) {
        let file_length = file_data.len() as u64;
        let hash_full = checksum::md5(file_data);
        let hash_16k_data = &file_data[..file_data.len().min(16384)];
        let hash_16k = checksum::md5(hash_16k_data);

        let filename = b"testfile.dat";
        let mut id_input = Vec::new();
        id_input.extend_from_slice(&hash_16k);
        id_input.extend_from_slice(&file_length.to_le_bytes());
        id_input.extend_from_slice(filename);
        let file_id_bytes: [u8; 16] = Md5::digest(&id_input).into();
        let file_id = FileId::from_bytes(file_id_bytes);

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

        // Build packets
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

        let mut ifsc_body = Vec::new();
        ifsc_body.extend_from_slice(&file_id_bytes);
        for cs in &checksums {
            ifsc_body.extend_from_slice(&cs.md5);
            ifsc_body.extend_from_slice(&cs.crc32.to_le_bytes());
        }

        let mut stream = Vec::new();
        stream.extend_from_slice(&make_full_packet(header::TYPE_MAIN, &main_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_FILE_DESC, &fd_body, rsid));
        stream.extend_from_slice(&make_full_packet(header::TYPE_IFSC, &ifsc_body, rsid));

        let mut set = Par2FileSet::from_files(&[&stream]).unwrap();

        // Generate recovery blocks using the PAR2 encoding formula.
        let constants = gf::input_slice_constants(num_slices);
        let ss = slice_size as usize;
        let word_count = ss / 2;

        // Pad file data to full slices.
        let mut padded = file_data.to_vec();
        padded.resize(num_slices * ss, 0);

        for r in 0..num_recovery {
            let exp = r as u32;
            let mut recovery = vec![0u8; ss];

            for (i, &constant) in constants.iter().enumerate() {
                let factor = gf::pow(constant, exp);
                for w in 0..word_count {
                    let input_word =
                        u16::from_le_bytes([padded[i * ss + w * 2], padded[i * ss + w * 2 + 1]]);
                    let contribution = gf::mul(input_word, factor);
                    let rec_word = u16::from_le_bytes([recovery[w * 2], recovery[w * 2 + 1]]);
                    let new_val = gf::add(rec_word, contribution);
                    let bytes = new_val.to_le_bytes();
                    recovery[w * 2] = bytes[0];
                    recovery[w * 2 + 1] = bytes[1];
                }
            }

            set.recovery_slices.insert(
                exp,
                RecoverySlice {
                    exponent: exp,
                    data: Bytes::from(recovery),
                },
            );
        }

        (set, file_id)
    }

    #[test]
    fn end_to_end_repair_single_damaged_slice() {
        // Create a file with 4 slices of 64 bytes each (256 bytes total).
        let slice_size = 64u64;
        let file_data: Vec<u8> = (0..256u32).map(|i| (i % 256) as u8).collect();
        let (par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 2);

        // Set up access with damaged data (corrupt slice 2).
        let mut damaged = file_data.clone();
        for item in damaged.iter_mut().take(192).skip(128) {
            *item ^= 0xFF;
        }

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, damaged);

        // Verify.
        let result = verify::verify_all(&par2_set, &access);
        assert_eq!(result.total_missing_blocks, 1);
        assert!(matches!(
            result.repairable,
            Repairability::Repairable { .. }
        ));

        // Plan repair.
        let plan = plan_repair(&par2_set, &result).unwrap();
        assert_eq!(plan.missing_slices.len(), 1);
        assert_eq!(plan.missing_slices[0], (file_id, 2));

        // Execute repair.
        execute_repair(&plan, &par2_set, &mut access).unwrap();

        // Verify the repaired data matches original.
        let repaired = access.read_file(&file_id).unwrap();
        assert_eq!(repaired, file_data, "repaired data should match original");
    }

    #[test]
    fn end_to_end_repair_multiple_damaged_slices() {
        let slice_size = 32u64;
        let file_data: Vec<u8> = (0..128u32).map(|i| ((i * 7 + 13) % 256) as u8).collect();
        let (par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 3);

        // Damage slices 0 and 3 (out of 4 total).
        let mut damaged = file_data.clone();
        for item in damaged.iter_mut().take(32) {
            *item = 0;
        }
        for item in damaged.iter_mut().take(128).skip(96) {
            *item = 0;
        }

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, damaged);

        let result = verify::verify_all(&par2_set, &access);
        assert_eq!(result.total_missing_blocks, 2);

        let plan = plan_repair(&par2_set, &result).unwrap();
        assert_eq!(plan.missing_slices.len(), 2);

        execute_repair(&plan, &par2_set, &mut access).unwrap();

        let repaired = access.read_file(&file_id).unwrap();
        assert_eq!(repaired, file_data);
    }

    #[test]
    fn end_to_end_repair_missing_file() {
        // Test repairing a completely missing file.
        let slice_size = 64u64;
        let file_data: Vec<u8> = (0..128u32).map(|i| (i % 256) as u8).collect();
        let (par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 4);

        // File is completely missing -- create with zeros so write_file_range works.
        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, vec![0u8; 128]);

        let result = verify::verify_all(&par2_set, &access);
        assert_eq!(result.total_missing_blocks, 2); // 2 slices, both damaged

        let plan = plan_repair(&par2_set, &result).unwrap();
        assert_eq!(plan.missing_slices.len(), 2);

        execute_repair(&plan, &par2_set, &mut access).unwrap();

        let repaired = access.read_file(&file_id).unwrap();
        assert_eq!(repaired, file_data);
    }

    #[test]
    fn plan_repair_not_needed() {
        let slice_size = 64u64;
        let file_data = vec![0xABu8; 128];
        let (par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 2);

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, file_data);

        let result = verify::verify_all(&par2_set, &access);
        let err = plan_repair(&par2_set, &result).unwrap_err();
        assert!(matches!(err, Par2Error::ReedSolomonError { .. }));
    }

    #[test]
    fn plan_repair_insufficient() {
        let slice_size = 64u64;
        let file_data: Vec<u8> = (0..256u32).map(|i| (i % 256) as u8).collect();
        let (par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 1);

        // Damage 2 slices but only 1 recovery block.
        let mut damaged = file_data.clone();
        for item in damaged.iter_mut().take(64) {
            *item = 0;
        }
        for item in damaged.iter_mut().take(128).skip(64) {
            *item = 0;
        }

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, damaged);

        let result = verify::verify_all(&par2_set, &access);
        let err = plan_repair(&par2_set, &result).unwrap_err();
        assert!(matches!(err, Par2Error::InsufficientRecoveryData { .. }));
    }

    #[test]
    fn repair_with_partial_last_slice() {
        // File size not a multiple of slice_size.
        let slice_size = 64u64;
        // 100 bytes = 2 slices (64 + 36), last slice padded to 64 for RS.
        let file_data: Vec<u8> = (0..100u32).map(|i| ((i * 3 + 5) % 256) as u8).collect();
        let (par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 2);

        // Damage the last slice.
        let mut damaged = file_data.clone();
        for item in damaged.iter_mut().take(100).skip(64) {
            *item = 0;
        }

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, damaged);

        let result = verify::verify_all(&par2_set, &access);
        assert_eq!(result.total_missing_blocks, 1);

        let plan = plan_repair(&par2_set, &result).unwrap();
        execute_repair(&plan, &par2_set, &mut access).unwrap();

        let repaired = access.read_file(&file_id).unwrap();
        assert_eq!(repaired, file_data);
    }
}
