//! PAR2 repair orchestration using Reed-Solomon decoding over GF(2^16).
//!
//! This module implements the full repair pipeline:
//! 1. Analyze verification results to identify missing/damaged slices
//! 2. Build a repair plan (decode matrix from Gaussian elimination)
//! 3. Execute repair by reading recovery data, XOR-ing out known contributions,
//!    and multiplying by the decode matrix to reconstruct missing slices
//! 4. Write repaired data back to files

use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{debug, info, warn};

use crate::error::{Par2Error, Result};
use crate::gf;
use crate::matrix;
use crate::par2_set::Par2FileSet;
use crate::types::{
    CancellationToken, FileId, MAX_SLICES_PER_FILE, MAX_TOTAL_INPUT_SLICES, ProgressCallback,
    ProgressStage, ProgressUpdate,
};
use crate::verify::{FileAccess, Repairability, VerificationResult};

pub(crate) const DEFAULT_REPAIR_MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// The decode matrix is a transient planning workspace whose size is set by
/// the damage (missing^2 + missing*total words), not by streaming buffer
/// tuning. Give it its own budget floor so tight slice-buffer limits do not
/// refuse legitimately repairable sets; an explicitly larger `memory_limit`
/// raises it further. Bounded in practice by the 32768-slice spec cap.
const MATRIX_WORKSPACE_BUDGET_FLOOR: usize = 1024 * 1024 * 1024;
const XOR_OUT_PAR_CHUNK: usize = 16;

/// A plan for repairing missing/damaged slices.
#[derive(Debug, Clone)]
pub struct RepairPlan {
    /// The missing input slices as (FileId, slice_index) pairs.
    pub missing_slices: Vec<(FileId, u32)>,
    /// Global indices of missing slices in the concatenated input slice ordering.
    pub missing_global_indices: Vec<usize>,
    /// Global indices of source slices that are already available and will be read as inputs.
    pub available_input_global_indices: Vec<usize>,
    /// Which recovery block exponents to use (one per missing slice).
    pub recovery_exponents: Vec<u32>,
    /// The inverted decode matrix rows (each row has `missing_slices.len()` entries).
    pub decode_matrix: matrix::Matrix,
    /// Full repair coefficients with columns ordered as
    /// `available_input_global_indices` followed by `recovery_exponents`.
    pub input_factors: matrix::Matrix,
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
    plan_repair_with_memory_limit(par2_set, verification, Some(DEFAULT_REPAIR_MEMORY_LIMIT))
}

/// Plan a repair operation using an explicit repair workspace memory limit.
pub fn plan_repair_with_memory_limit(
    par2_set: &Par2FileSet,
    verification: &VerificationResult,
    memory_limit: Option<usize>,
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
        Repairability::ResourceLimited { reason } => {
            return Err(Par2Error::ResourceLimitExceeded {
                reason: format!("PAR2 verification is resource-limited: {reason}"),
            });
        }
        Repairability::Repairable { .. } => {}
    }

    // Build the global slice index mapping.
    // Global ordering: files in order of recovery_file_ids, slices in order within each file.
    let mut global_to_file: Vec<(FileId, u32)> = Vec::new();
    for file_id in &par2_set.recovery_file_ids {
        if let Some(desc) = par2_set.file_description(file_id) {
            let slice_count =
                usize::try_from(par2_set.slice_count_for_file(desc.length)).map_err(|_| {
                    Par2Error::ResourceLimitExceeded {
                        reason: format!(
                            "file {} has more than {MAX_SLICES_PER_FILE} addressable PAR2 slices",
                            desc.filename
                        ),
                    }
                })?;
            if slice_count > MAX_SLICES_PER_FILE {
                return Err(Par2Error::ResourceLimitExceeded {
                    reason: format!(
                        "file {} has {slice_count} PAR2 slices; max is {MAX_SLICES_PER_FILE}",
                        desc.filename
                    ),
                });
            }
            for s in 0..slice_count {
                global_to_file.push((*file_id, s as u32));
            }
        }
    }
    let total_input_slices = global_to_file.len();
    if total_input_slices > MAX_TOTAL_INPUT_SLICES {
        return Err(Par2Error::ResourceLimitExceeded {
            reason: format!(
                "recovery set has {total_input_slices} input slices; PAR2 supports at most {MAX_TOTAL_INPUT_SLICES}"
            ),
        });
    }

    // Identify missing slices (global indices).
    let mut missing_slices: Vec<(FileId, u32)> = Vec::new();
    let mut missing_global_indices: Vec<usize> = Vec::new();

    let mut global_idx = 0usize;
    for file_id in &par2_set.recovery_file_ids {
        let desc = match par2_set.file_description(file_id) {
            Some(d) => d,
            None => continue,
        };
        let slice_count =
            usize::try_from(par2_set.slice_count_for_file(desc.length)).map_err(|_| {
                Par2Error::ResourceLimitExceeded {
                    reason: format!(
                        "file {} has more than {MAX_SLICES_PER_FILE} addressable PAR2 slices",
                        desc.filename
                    ),
                }
            })?;
        if slice_count > MAX_SLICES_PER_FILE {
            return Err(Par2Error::ResourceLimitExceeded {
                reason: format!(
                    "file {} has {slice_count} PAR2 slices; max is {MAX_SLICES_PER_FILE}",
                    desc.filename
                ),
            });
        }

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
    let mut all_exponents: Vec<u32> = par2_set.recovery_slices.keys().copied().collect();
    all_exponents.sort_unstable();

    if all_exponents.len() < missing_count {
        return Err(Par2Error::InsufficientRecoveryData {
            needed: missing_count as u32,
            available: all_exponents.len() as u32,
            deficit: (missing_count - all_exponents.len()) as u32,
        });
    }
    if let Some(reason) =
        repair_matrix_limit_reason(total_input_slices, missing_count, memory_limit)
    {
        return Err(Par2Error::ResourceLimitExceeded { reason });
    }

    // Compute constants for all input slices.
    let constants = gf::input_slice_constants(total_input_slices);
    let missing_set: HashSet<usize> = missing_global_indices.iter().copied().collect();
    let available_input_global_indices: Vec<usize> = (0..total_input_slices)
        .filter(|global_idx| !missing_set.contains(global_idx))
        .collect();

    // Try building the decode matrix, skipping corrupt or singular recovery
    // blocks. Payload validation is lazy: only selected blocks are hashed,
    // each at most once, so undamaged repairs never pay for unused volumes.
    let mut skip_set: HashSet<usize> = HashSet::new();
    let mut validated_exponents: HashMap<u32, bool> = HashMap::new();
    let (recovery_exponents, input_factors, decode) = loop {
        let selected_indices: Vec<usize> = all_exponents
            .iter()
            .enumerate()
            .filter(|(i, _)| !skip_set.contains(i))
            .map(|(i, _)| i)
            .take(missing_count)
            .collect();
        let selected: Vec<u32> = selected_indices
            .iter()
            .map(|&idx| all_exponents[idx])
            .collect();

        if selected.len() < missing_count {
            return Err(Par2Error::InsufficientRecoveryData {
                needed: missing_count as u32,
                available: selected.len() as u32,
                deficit: (missing_count - selected.len()) as u32,
            });
        }

        let mut corrupt_selection = None;
        for (position, &exponent) in selected.iter().enumerate() {
            let valid = *validated_exponents.entry(exponent).or_insert_with(|| {
                let slice = &par2_set.recovery_slices[&exponent];
                match slice
                    .data
                    .validate_packet_hash(par2_set.recovery_set_id.as_bytes(), exponent)
                {
                    Ok(valid) => {
                        if !valid {
                            warn!(
                                "recovery block exponent {exponent} failed packet hash validation, skipping"
                            );
                        }
                        valid
                    }
                    Err(error) => {
                        warn!("recovery block exponent {exponent} is unreadable ({error}), skipping");
                        false
                    }
                }
            });
            if !valid {
                corrupt_selection = Some(selected_indices[position]);
                break;
            }
        }
        if let Some(skip_idx) = corrupt_selection {
            skip_set.insert(skip_idx);
            continue;
        }

        match matrix::build_repair_matrix_with_bad_row(
            &available_input_global_indices,
            &missing_global_indices,
            &selected,
            &constants,
        ) {
            Ok((input_factors, decode)) => break (selected, input_factors, decode),
            Err(matrix_error) => {
                let mut skip_idx = matrix_error
                    .bad_row
                    .and_then(|row| selected_indices.get(row).copied());
                if skip_idx.is_none() {
                    for candidate_idx in &selected_indices {
                        let trial: Vec<u32> = all_exponents
                            .iter()
                            .enumerate()
                            .filter(|(idx, _)| !skip_set.contains(idx) && idx != candidate_idx)
                            .map(|(_, &exponent)| exponent)
                            .take(missing_count)
                            .collect();
                        if trial.len() < missing_count {
                            continue;
                        }
                        if matrix::build_repair_matrix_with_bad_row(
                            &available_input_global_indices,
                            &missing_global_indices,
                            &trial,
                            &constants,
                        )
                        .is_ok()
                        {
                            skip_idx = Some(*candidate_idx);
                            break;
                        }
                    }
                }
                let skip_idx = skip_idx.unwrap_or_else(|| {
                    *selected_indices
                        .last()
                        .expect("singular repair selection must contain at least one row")
                });
                warn!(
                    "recovery exponent {} produced singular matrix, skipping",
                    all_exponents[skip_idx]
                );
                skip_set.insert(skip_idx);
            }
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
        available_input_global_indices,
        recovery_exponents,
        decode_matrix: decode,
        input_factors,
        slice_size: par2_set.slice_size,
        constants,
        total_input_slices,
        global_to_file,
    })
}

pub(crate) fn repair_matrix_resource_limit_reason(
    par2_set: &Par2FileSet,
    verification: &VerificationResult,
    memory_limit: Option<usize>,
) -> Result<Option<String>> {
    if !matches!(verification.repairable, Repairability::Repairable { .. }) {
        return Ok(None);
    }

    let total_input_slices = total_input_slices_for_set(par2_set)?;
    let missing_count = verification.total_missing_blocks as usize;
    Ok(repair_matrix_limit_reason(
        total_input_slices,
        missing_count,
        memory_limit,
    ))
}

/// Options controlling repair execution.
pub struct RepairOptions {
    /// If set, repair will check this token and stop early if cancelled.
    pub cancel: Option<CancellationToken>,
    /// If set, called with progress updates during repair.
    pub progress: Option<ProgressCallback>,
    /// Maximum transient repair workspace size in bytes.
    ///
    /// Repair chooses the in-memory fast path only when its
    /// estimated working set fits within `limit`; otherwise it switches to the
    /// streamed chunk path and sizes chunk buffers from this budget.
    ///
    /// If `None`, the crate default bounded repair budget is used.
    pub memory_limit: Option<usize>,
}

impl Default for RepairOptions {
    fn default() -> Self {
        Self {
            cancel: None,
            progress: None,
            memory_limit: Some(DEFAULT_REPAIR_MEMORY_LIMIT),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RepairExecutionMode {
    InMemory { chunk_words: usize },
    Streaming { chunk_words: usize, budget: usize },
}

#[derive(Clone, Copy)]
struct FactorIndex {
    factor: u16,
    input_idx: u16,
}

#[derive(Clone, Debug)]
struct RepairWriteTarget {
    file_id: FileId,
    filename: String,
    offset: u64,
    file_end: u64,
}

fn check_cancel(options: &RepairOptions) -> Result<()> {
    if let Some(ref cancel) = options.cancel
        && cancel.is_cancelled()
    {
        return Err(Par2Error::Cancelled);
    }
    Ok(())
}

fn estimated_in_memory_repair_bytes(plan: &RepairPlan) -> usize {
    let slice_size = plan.slice_size as usize;
    // Prepared multiply tables are deduplicated per distinct factor value, so
    // they are bounded by the field size rather than outputs*inputs.
    let prepared_tables = 65_535usize
        .min(
            plan.input_factors
                .rows
                .saturating_mul(plan.input_factors.cols),
        )
        .saturating_mul(std::mem::size_of::<crate::gf_simd::PreparedInputFactor>());
    plan.input_factors
        .cols
        .saturating_mul(slice_size)
        .saturating_add(plan.missing_slices.len().saturating_mul(slice_size))
        .saturating_add(slice_size.saturating_mul(2))
        .saturating_add(prepared_tables)
}

fn estimated_repair_matrix_bytes(total_inputs: usize, missing_rows: usize) -> usize {
    let working_words = missing_rows
        .saturating_mul(missing_rows)
        .saturating_add(missing_rows.saturating_mul(total_inputs));
    working_words.saturating_mul(std::mem::size_of::<u16>())
}

fn repair_memory_limit_bytes(memory_limit: Option<usize>) -> usize {
    memory_limit.unwrap_or(DEFAULT_REPAIR_MEMORY_LIMIT)
}

fn repair_matrix_limit_reason(
    total_input_slices: usize,
    missing_count: usize,
    memory_limit: Option<usize>,
) -> Option<String> {
    if total_input_slices > MAX_TOTAL_INPUT_SLICES {
        return Some(format!(
            "recovery set has {total_input_slices} input slices; PAR2 supports at most {MAX_TOTAL_INPUT_SLICES}"
        ));
    }
    let estimated = estimated_repair_matrix_bytes(total_input_slices, missing_count);
    let limit = repair_memory_limit_bytes(memory_limit).max(MATRIX_WORKSPACE_BUDGET_FLOOR);
    (estimated > limit).then(|| {
        format!(
            "repair matrix for {missing_count} missing slices would require {estimated} bytes, exceeding the {limit} byte matrix workspace budget"
        )
    })
}

fn total_input_slices_for_set(par2_set: &Par2FileSet) -> Result<usize> {
    let mut total = 0usize;
    for file_id in &par2_set.recovery_file_ids {
        let Some(desc) = par2_set.file_description(file_id) else {
            continue;
        };
        let slice_count =
            usize::try_from(par2_set.slice_count_for_file(desc.length)).map_err(|_| {
                Par2Error::ResourceLimitExceeded {
                    reason: format!(
                        "file {} has more than {MAX_SLICES_PER_FILE} addressable PAR2 slices",
                        desc.filename
                    ),
                }
            })?;
        if slice_count > MAX_SLICES_PER_FILE {
            return Err(Par2Error::ResourceLimitExceeded {
                reason: format!(
                    "file {} has {slice_count} PAR2 slices; max is {MAX_SLICES_PER_FILE}",
                    desc.filename
                ),
            });
        }
        total = total.saturating_add(slice_count);
    }
    Ok(total)
}

fn chunk_words_for_budget(word_count: usize, outputs: usize, limit_bytes: usize) -> usize {
    if outputs == 0 {
        return word_count.max(1);
    }

    // Outputs plus both double-buffered source-batch sets share the budget.
    let streaming_buffers = outputs.saturating_add(2 * STREAM_INPUT_BATCH);
    let chunk_bytes = (limit_bytes / streaming_buffers)
        .max(2)
        .min(word_count.saturating_mul(2));
    (chunk_bytes / 2).max(1).min(word_count.max(1))
}

#[cfg(target_arch = "x86_64")]
fn round_div(value: usize, divisor: usize) -> usize {
    (value + (divisor / 2)) / divisor
}

fn method_tuned_chunk_words(slice_size: usize) -> usize {
    let word_count = (slice_size / 2).max(1);

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("gfni") && is_x86_feature_detected!("avx2") {
            let ideal_chunk_bytes = 4 * 1024usize;
            let stride_bytes = 64usize;
            let threads = rayon::current_num_threads().max(1);
            let aligned_slice_bytes =
                slice_size.div_ceil(stride_bytes) * stride_bytes + stride_bytes;
            let target_thread_chunk = aligned_slice_bytes.div_ceil(threads);

            let mut num_chunks = if target_thread_chunk <= ideal_chunk_bytes / 2 {
                round_div(aligned_slice_bytes, ideal_chunk_bytes).max(1)
            } else {
                round_div(target_thread_chunk, ideal_chunk_bytes).max(1) * threads
            };

            if num_chunks == 0 {
                num_chunks = 1;
            }

            let chunk_bytes = aligned_slice_bytes
                .div_ceil(num_chunks)
                .div_ceil(stride_bytes)
                * stride_bytes;
            return (chunk_bytes / 2).max(1).min(word_count);
        }
    }

    word_count
}

/// Repairs whose estimated in-memory footprint fits under this take the
/// in-memory path; everything larger streams. Streaming outperforms the
/// in-memory path on every measured large set (it received all the
/// kernel/batching work), so a generous memory budget must not steer big
/// repairs in-memory — the budget only sizes streaming chunks. Small repairs
/// keep the in-memory path's lower fixed overhead.
const IN_MEMORY_REPAIR_MAX_BYTES: usize = 128 * 1024 * 1024;

fn select_repair_execution_mode(plan: &RepairPlan, options: &RepairOptions) -> RepairExecutionMode {
    let slice_size = plan.slice_size as usize;
    let word_count = (slice_size / 2).max(1);

    let limit = options.memory_limit.unwrap_or(DEFAULT_REPAIR_MEMORY_LIMIT);
    let budget_chunk_words = chunk_words_for_budget(word_count, plan.missing_slices.len(), limit);
    if estimated_in_memory_repair_bytes(plan) <= limit.min(IN_MEMORY_REPAIR_MAX_BYTES) {
        // The in-memory path keeps its historical cache-tuned chunking.
        let method_chunk_words = method_tuned_chunk_words(slice_size);
        RepairExecutionMode::InMemory {
            chunk_words: budget_chunk_words.min(method_chunk_words),
        }
    } else {
        // Streaming chunks are budget-driven only: the outer chunk decides how
        // many times every source is re-read from disk, so it must stay as
        // large as the budget allows. Cache tiling happens inside the batch
        // compute (STREAM_STRIP_BYTES), never in the I/O loop.
        RepairExecutionMode::Streaming {
            chunk_words: budget_chunk_words,
            budget: limit,
        }
    }
}

fn build_write_targets(
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
) -> Result<Vec<RepairWriteTarget>> {
    plan.missing_slices
        .iter()
        .map(|(file_id, local_slice)| {
            let desc =
                par2_set
                    .file_description(file_id)
                    .ok_or_else(|| Par2Error::ReedSolomonError {
                        reason: format!("file description not found for {file_id}"),
                    })?;
            Ok(RepairWriteTarget {
                file_id: *file_id,
                filename: desc.filename.clone(),
                offset: *local_slice as u64 * plan.slice_size,
                file_end: desc.length,
            })
        })
        .collect()
}

fn grouped_input_factors(coefficients: &matrix::Matrix) -> Vec<Vec<FactorIndex>> {
    (0..coefficients.rows)
        .map(|row_idx| {
            coefficients
                .row(row_idx)
                .iter()
                .enumerate()
                .filter_map(|(input_idx, &factor)| {
                    if factor == 0 {
                        None
                    } else {
                        Some(FactorIndex {
                            factor,
                            input_idx: input_idx as u16,
                        })
                    }
                })
                .collect()
        })
        .collect()
}

/// Number of source slices (present inputs + recovery blocks) multiplied into
/// every output per streamed batch. Batching sources lets the batch kernels
/// load and store each output region once per K sources instead of once per
/// source, cutting output memory traffic by ~K. A multiple of the folded
/// kernel's group width so batches split into whole groups.
const STREAM_INPUT_BATCH: usize = 11 * crate::gf_simd::FOLDED_GROUP;

/// Compute tile for the folded streaming kernel. The destination tile must
/// stay L1-resident across every source group of a batch: the kernel loads
/// and stores the destination once per six-source group, so a tile larger
/// than L1 turns those group passes into DRAM traffic — measured as the
/// dominant repair cost at larger tile sizes. 4 KiB keeps the dst tile plus
/// a group's staging column comfortably inside L1/L2 on every supported
/// core, including E-cores with small shared caches.
const COMPUTE_TILE_BYTES: usize = 4 * 1024;

/// Strip size for the generic (non-folded) compute path, whose kernels
/// iterate every source per destination pass and so tolerate larger strips.
const STREAM_STRIP_BYTES: usize = 32 * 1024;

/// 64-byte-aligned backing for the folded staging streams so every 32-byte
/// block sits cache-line aligned.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct StagingCell([u8; 64]);

fn staging_cells_for(bytes: usize) -> Vec<StagingCell> {
    vec![StagingCell([0u8; 64]); bytes.div_ceil(64)]
}

fn staging_bytes(cells: &[StagingCell]) -> &[u8] {
    // Safe: StagingCell is a plain 64-byte array with alignment 64; viewing
    // the contiguous cell storage as bytes narrows alignment only.
    unsafe { std::slice::from_raw_parts(cells.as_ptr() as *const u8, cells.len() * 64) }
}

fn staging_bytes_mut(cells: &mut [StagingCell]) -> &mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(cells.as_mut_ptr() as *mut u8, cells.len() * 64) }
}

/// Lazily prepared multiply tables, one slot per distinct GF(2^16) factor.
/// Entries are boxed so references handed to worker threads stay valid; the
/// memo is fully populated before any parallel compute runs.
struct MemoEntry {
    prepared: crate::gf_simd::PreparedInputFactor,
    /// Affine matrix forms for the folded split-layout kernel; only built
    /// when that path is active.
    affine: Option<crate::gf_simd::AffineMulMatrices>,
}

struct PreparedFactorMemo {
    slots: Vec<Option<Box<MemoEntry>>>,
}

impl PreparedFactorMemo {
    fn from_matrix(matrix: &matrix::Matrix, with_affine: bool) -> Self {
        let mut slots: Vec<Option<Box<MemoEntry>>> = (0..1usize << 16).map(|_| None).collect();
        // Factor 0 backs the padding lanes of partially filled groups.
        let ensure = |factor: u16, slots: &mut Vec<Option<Box<MemoEntry>>>| {
            let slot = &mut slots[factor as usize];
            if slot.is_none() {
                *slot = Some(Box::new(MemoEntry {
                    prepared: crate::gf_simd::prepare_input_factor(factor),
                    affine: with_affine.then(|| crate::gf_simd::precompute_affine_matrices(factor)),
                }));
            }
        };
        ensure(0, &mut slots);
        for output_idx in 0..matrix.rows {
            for source_idx in 0..matrix.cols {
                ensure(matrix.get(output_idx, source_idx), &mut slots);
            }
        }
        Self { slots }
    }

    #[inline]
    fn get(&self, factor: u16) -> &crate::gf_simd::PreparedInputFactor {
        &self.slots[factor as usize]
            .as_deref()
            .expect("factor prepared during memo construction")
            .prepared
    }

    #[inline]
    fn get_affine(&self, factor: u16) -> &crate::gf_simd::AffineMulMatrices {
        self.slots[factor as usize]
            .as_deref()
            .expect("factor prepared during memo construction")
            .affine
            .as_ref()
            .expect("affine matrices built for the folded path")
    }
}

/// One double-buffered set of streamed source chunks. Which sources feed
/// which outputs is read directly from the decode matrix inside the compute
/// tasks — no gather lists are materialized.
struct StreamBatchSet {
    /// Per-source chunk buffers (generic kernel path).
    bufs: Vec<Vec<u8>>,
    /// Folded path: per group of FOLDED_GROUP sources, their split-layout
    /// blocks interleaved at 32-byte granularity into one 64-byte-aligned
    /// stream.
    staging: Vec<Vec<StagingCell>>,
    /// Folded path: read scratch, and each source's sub-block tail bytes
    /// (kept in normal layout, applied scalar).
    scratch: Vec<u8>,
    tails: Vec<[u8; crate::gf_simd::SPLIT_BLOCK_BYTES]>,
    tail_len: usize,
    start: usize,
    len: usize,
}

impl StreamBatchSet {
    fn new(max_byte_len: usize, folded: bool) -> Self {
        let groups = STREAM_INPUT_BATCH / crate::gf_simd::FOLDED_GROUP;
        if folded {
            Self {
                bufs: Vec::new(),
                staging: (0..groups)
                    .map(|_| staging_cells_for(max_byte_len * crate::gf_simd::FOLDED_GROUP))
                    .collect(),
                scratch: vec![0u8; max_byte_len],
                tails: vec![[0u8; crate::gf_simd::SPLIT_BLOCK_BYTES]; STREAM_INPUT_BATCH],
                tail_len: 0,
                start: 0,
                len: 0,
            }
        } else {
            Self {
                bufs: vec![vec![0u8; max_byte_len]; STREAM_INPUT_BATCH],
                staging: Vec::new(),
                scratch: Vec::new(),
                tails: Vec::new(),
                tail_len: 0,
                start: 0,
                len: 0,
            }
        }
    }
}

/// Read one streamed source chunk (a present input slice range or a recovery
/// block range) into `dst`, zero-padding any short tail.
#[allow(clippy::too_many_arguments)]
fn read_stream_source_chunk(
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
    file_access: &mut dyn FileAccess,
    recovery_files: &mut HashMap<PathBuf, File>,
    available_inputs: usize,
    source_idx: usize,
    byte_start: usize,
    dst: &mut [u8],
) -> Result<()> {
    if source_idx < available_inputs {
        let global_idx = plan.available_input_global_indices[source_idx];
        let (file_id, local_slice) = plan.global_to_file[global_idx];
        let offset = local_slice as u64 * plan.slice_size + byte_start as u64;
        let read_len = file_access
            .read_file_range_into(&file_id, offset, dst)
            .map_err(Par2Error::Io)?;
        dst[read_len..].fill(0);
    } else {
        let exp = plan.recovery_exponents[source_idx - available_inputs];
        let rs = par2_set
            .recovery_slices
            .get(&exp)
            .ok_or_else(|| Par2Error::ReedSolomonError {
                reason: format!("recovery block with exponent {exp} not found"),
            })?;
        fill_recovery_chunk(&rs.data, byte_start, dst, recovery_files).map_err(Par2Error::Io)?;
    }
    Ok(())
}

/// Fill one batch set: read the source chunks for `[batch_start,
/// batch_start+len)`.
#[allow(clippy::too_many_arguments)]
fn fill_stream_batch(
    set: &mut StreamBatchSet,
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
    file_access: &mut dyn FileAccess,
    recovery_files: &mut HashMap<PathBuf, File>,
    available_inputs: usize,
    batch_start: usize,
    batch_len: usize,
    byte_start: usize,
    byte_len: usize,
    use_folded: bool,
    options: &RepairOptions,
) -> Result<()> {
    for b in 0..batch_len {
        if b % 64 == 0 {
            check_cancel(options)?;
        }
        read_stream_source_chunk(
            plan,
            par2_set,
            file_access,
            recovery_files,
            available_inputs,
            batch_start + b,
            byte_start,
            if use_folded {
                &mut set.scratch[..byte_len]
            } else {
                &mut set.bufs[b][..byte_len]
            },
        )?;
        if use_folded {
            let group = b / crate::gf_simd::FOLDED_GROUP;
            let lane = b % crate::gf_simd::FOLDED_GROUP;
            crate::gf_simd::split_encode_scatter(
                &set.scratch[..byte_len],
                staging_bytes_mut(&mut set.staging[group]),
                lane,
            );
            let vec_len = byte_len & !(crate::gf_simd::SPLIT_BLOCK_BYTES - 1);
            let tail_len = byte_len - vec_len;
            set.tails[b][..tail_len].copy_from_slice(&set.scratch[vec_len..byte_len]);
            set.tail_len = tail_len;
        }
    }
    set.start = batch_start;
    set.len = batch_len;
    Ok(())
}

/// Multiply one source batch into every output, with factors read straight
/// from the decode matrix inside each task. Strip-parallel when the chunk is
/// large (keeps the batch's source strips cache-resident across all outputs);
/// output-parallel when the chunk itself is small.
fn run_stream_batch_compute(
    output_ptrs: &[usize],
    set: &StreamBatchSet,
    plan: &RepairPlan,
    memo: &PreparedFactorMemo,
    byte_len: usize,
    use_folded: bool,
) {
    let matrix = &plan.input_factors;
    if use_folded {
        let vec_len = byte_len & !(crate::gf_simd::SPLIT_BLOCK_BYTES - 1);
        let groups = set.len.div_ceil(crate::gf_simd::FOLDED_GROUP);
        let group_matrices = |j: usize,
                              g: usize|
         -> [&crate::gf_simd::AffineMulMatrices;
                                  crate::gf_simd::FOLDED_GROUP] {
            std::array::from_fn(|lane| {
                let b = g * crate::gf_simd::FOLDED_GROUP + lane;
                // Padding lanes multiply through zero matrices and add nothing.
                let factor = if b < set.len {
                    matrix.get(j, set.start + b)
                } else {
                    0
                };
                memo.get_affine(factor)
            })
        };

        if vec_len > 0 {
            // Coefficient matrices are resolved ONCE per batch into a flat
            // table (outputs x groups); compute tasks index it instead of
            // walking the decode matrix and memo per tile, which multiplied
            // lookup work by the tile count.
            let mut batch_matrices: Vec<
                [&crate::gf_simd::AffineMulMatrices; crate::gf_simd::FOLDED_GROUP],
            > = Vec::with_capacity(output_ptrs.len() * groups);
            for j in 0..output_ptrs.len() {
                for g in 0..groups {
                    batch_matrices.push(group_matrices(j, g));
                }
            }
            let batch_matrices = &batch_matrices;

            // Reference-style tile nest: tile-outer -> output -> source-group.
            // The 4 KiB destination tile stays L1-resident across every group
            // pass (the kernel reloads dst once per group — an L1 hit at this
            // size), and the tile's staging column (~batch x 4 KiB) stays in
            // L2 across the output sweep. Larger tiles turn the per-group dst
            // reload into DRAM traffic that dominates the whole repair.
            let tiles = vec_len.div_ceil(COMPUTE_TILE_BYTES);
            let threads = rayon::current_num_threads().max(1);
            let outputs_n = output_ptrs.len();
            // One task per tile when tiles alone give enough parallelism;
            // otherwise split the output sweep so every thread has work.
            let blocks_per_tile = if tiles >= threads * 2 {
                1
            } else {
                (threads * 4)
                    .div_ceil(tiles.max(1))
                    .clamp(1, outputs_n.max(1))
            };
            let output_block = outputs_n.div_ceil(blocks_per_tile).max(1);
            let task_count = tiles * blocks_per_tile;
            (0..task_count).into_par_iter().for_each(|task| {
                let tile = task / blocks_per_tile;
                let block = task % blocks_per_tile;
                let s0 = tile * COMPUTE_TILE_BYTES;
                let s1 = (s0 + COMPUTE_TILE_BYTES).min(vec_len);
                let j0 = block * output_block;
                let j1 = (j0 + output_block).min(outputs_n);
                let stride = crate::gf_simd::FOLDED_GROUP;
                let stagings: Vec<&[u8]> = (0..groups)
                    .map(|g| &staging_bytes(&set.staging[g])[s0 * stride..s1 * stride])
                    .collect();
                for (j, output_ptr) in output_ptrs.iter().copied().enumerate().take(j1).skip(j0) {
                    // Safe: (tile, output) ranges have exactly one writer —
                    // tiles partition the region, output blocks partition the
                    // outputs, and batches are processed sequentially.
                    let dst = unsafe {
                        std::slice::from_raw_parts_mut((output_ptr as *mut u8).add(s0), s1 - s0)
                    };
                    crate::gf_simd::mul_acc_folded_batch(
                        dst,
                        &stagings,
                        &batch_matrices[j * groups..(j + 1) * groups],
                    );
                }
            });
        }

        // Sub-block tails stay in normal layout on both sides; apply scalar.
        if set.tail_len > 0 {
            output_ptrs
                .par_iter()
                .copied()
                .enumerate()
                .for_each(|(j, output_ptr)| {
                    let dst = unsafe {
                        std::slice::from_raw_parts_mut(
                            (output_ptr as *mut u8).add(vec_len),
                            set.tail_len,
                        )
                    };
                    for b in 0..set.len {
                        let factor = matrix.get(j, set.start + b);
                        if factor == 0 {
                            continue;
                        }
                        crate::gf_simd::mul_acc_region(factor, &set.tails[b][..set.tail_len], dst);
                    }
                });
        }
        return;
    }

    let strips = byte_len.div_ceil(STREAM_STRIP_BYTES);
    let threads = rayon::current_num_threads().max(1);

    if strips >= threads {
        (0..strips).into_par_iter().for_each(|si| {
            let s0 = si * STREAM_STRIP_BYTES;
            let s1 = (s0 + STREAM_STRIP_BYTES).min(byte_len);
            let mut srcs: Vec<crate::gf_simd::PreparedFactorSrc<'_>> = Vec::with_capacity(set.len);
            for (j, output_ptr) in output_ptrs.iter().copied().enumerate() {
                srcs.clear();
                for b in 0..set.len {
                    let factor = matrix.get(j, set.start + b);
                    if factor == 0 {
                        continue;
                    }
                    srcs.push(crate::gf_simd::PreparedFactorSrc {
                        prepared: memo.get(factor),
                        src: &set.bufs[b][s0..s1],
                    });
                }
                if srcs.is_empty() {
                    continue;
                }
                // Safe: each (strip, output) range has exactly one writer —
                // strips partition the chunk and this task is the only one
                // for strip `si`; batches are processed sequentially.
                let dst = unsafe {
                    std::slice::from_raw_parts_mut((output_ptr as *mut u8).add(s0), s1 - s0)
                };
                crate::gf_simd::mul_acc_input_batch_prepared(dst, &srcs);
            }
        });
    } else {
        output_ptrs
            .par_iter()
            .copied()
            .enumerate()
            .for_each(|(j, output_ptr)| {
                let mut srcs: Vec<crate::gf_simd::PreparedFactorSrc<'_>> =
                    Vec::with_capacity(set.len);
                for b in 0..set.len {
                    let factor = matrix.get(j, set.start + b);
                    if factor == 0 {
                        continue;
                    }
                    srcs.push(crate::gf_simd::PreparedFactorSrc {
                        prepared: memo.get(factor),
                        src: &set.bufs[b][..byte_len],
                    });
                }
                if srcs.is_empty() {
                    return;
                }
                // Safe: one task per output buffer.
                let dst =
                    unsafe { std::slice::from_raw_parts_mut(output_ptr as *mut u8, byte_len) };
                crate::gf_simd::mul_acc_input_batch_prepared(dst, &srcs);
            });
    }
}

fn xor_out_known_data(
    recovery_buffers: &mut [Vec<u8>],
    recovery_factors: &[u16],
    data: &[u8],
    chunk_words: usize,
) {
    assert_eq!(
        recovery_buffers.len(),
        recovery_factors.len(),
        "recovery factor count must match recovery buffer count"
    );
    assert!(
        data.len().is_multiple_of(2),
        "PAR2 slice_size must be a multiple of 2"
    );

    let word_count = data.len() / 2;
    let chunk_words = chunk_words.max(1).min(word_count.max(1));
    let word_chunks: Vec<usize> = (0..word_count.max(1)).step_by(chunk_words).collect();
    let recovery_ptrs: Vec<usize> = recovery_buffers
        .iter_mut()
        .map(|recovery| recovery.as_mut_ptr() as usize)
        .collect();

    word_chunks.par_iter().for_each(|&chunk_start| {
        let chunk_end = (chunk_start + chunk_words).min(word_count);
        let byte_start = chunk_start * 2;
        let byte_len = (chunk_end - chunk_start) * 2;
        let src = &data[byte_start..byte_start + byte_len];

        for factor_start in (0..recovery_factors.len()).step_by(XOR_OUT_PAR_CHUNK) {
            let factor_end = (factor_start + XOR_OUT_PAR_CHUNK).min(recovery_factors.len());
            let mut pairs: Vec<crate::gf_simd::FactorDst<'_>> =
                Vec::with_capacity(factor_end - factor_start);

            for idx in factor_start..factor_end {
                let factor = recovery_factors[idx];
                if factor == 0 {
                    continue;
                }

                let dst = unsafe {
                    let ptr = recovery_ptrs[idx] as *mut u8;
                    std::slice::from_raw_parts_mut(ptr.add(byte_start), byte_len)
                };
                pairs.push(crate::gf_simd::FactorDst { factor, dst });
            }

            if !pairs.is_empty() {
                crate::gf_simd::mul_acc_multi_region(&mut pairs, src);
            }
        }
    });
}

fn read_exact_at_cached(
    files: &mut HashMap<PathBuf, File>,
    path: &Path,
    offset: u64,
    dst: &mut [u8],
) -> io::Result<()> {
    let file = if let Some(file) = files.get_mut(path) {
        file
    } else {
        files.insert(path.to_path_buf(), File::open(path)?);
        files.get_mut(path).expect("cached file should exist")
    };
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(dst)
}

fn fill_recovery_chunk(
    data: &crate::packet::RecoverySliceData,
    start: usize,
    dst: &mut [u8],
    file_cache: &mut HashMap<PathBuf, File>,
) -> io::Result<()> {
    dst.fill(0);

    if let Some(bytes) = data.as_bytes() {
        if start >= bytes.len() {
            return Ok(());
        }
        let end = (start + dst.len()).min(bytes.len());
        let copy_len = end - start;
        dst[..copy_len].copy_from_slice(&bytes[start..end]);
        return Ok(());
    }

    let Some((path, base_offset, len)) = data.file_span() else {
        return Ok(());
    };
    if start >= len {
        return Ok(());
    }

    let read_len = dst.len().min(len - start);
    read_exact_at_cached(
        file_cache,
        path,
        base_offset + start as u64,
        &mut dst[..read_len],
    )
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
        let mut data = rs.data.to_vec().map_err(Par2Error::Io)?;
        data.resize(slice_size, 0);
        recovery_data.push(data);

        if let Some(ref progress) = options.progress {
            progress(ProgressUpdate {
                stage: ProgressStage::ReadingRecovery,
                current: i as u32 + 1,
                total: n as u32,
                bytes_processed: (i + 1) as u64 * slice_size as u64,
                total_bytes: None,
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

    let recovery_factors: Vec<u16> = plan
        .recovery_exponents
        .iter()
        .map(|&exp| gf::pow(plan.constants[global_idx], exp))
        .collect();

    xor_out_known_data(
        recovery_buffers,
        &recovery_factors,
        &data[..slice_size],
        slice_size / 2,
    );
}

/// Multiply adjusted recovery data by the decode matrix and write repaired slices.
pub fn reconstruct_and_write(
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
    recovery_buffers: Vec<Vec<u8>>,
    file_access: &mut dyn FileAccess,
    chunk_words: usize,
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
    let word_count = slice_size / 2;

    // Step 2: Multiply adjusted recovery data by decode matrix.
    info!("reconstructing {} missing slices", n);

    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("gfni") && is_x86_feature_detected!("avx2") {
        return reconstruct_and_write_grouped_inputs(
            plan,
            par2_set,
            recovery_buffers,
            file_access,
            chunk_words,
            options,
        );
    }

    let mut repaired_slices: Vec<Vec<u8>> = vec![vec![0u8; slice_size]; n];

    let total_chunks_usize = word_count.div_ceil(chunk_words);
    let total_chunks = total_chunks_usize.min(u32::MAX as usize) as u32;
    let repair_total_bytes = (word_count as u64).saturating_mul(2);
    let write_total_bytes = (n as u64).saturating_mul(slice_size as u64);
    let operation_total_bytes = repair_total_bytes.saturating_add(write_total_bytes);

    check_cancel(options)?;

    let completed_chunks = AtomicU32::new(0);
    let repaired_ptrs: Vec<usize> = repaired_slices
        .iter_mut()
        .map(|slice| slice.as_mut_ptr() as usize)
        .collect();

    (0..total_chunks_usize)
        .into_par_iter()
        .try_for_each(|chunk_idx| -> Result<()> {
            check_cancel(options)?;

            let chunk_start = chunk_idx * chunk_words;
            let chunk_end = (chunk_start + chunk_words).min(word_count);
            let chunk_len = chunk_end - chunk_start;
            let byte_start = chunk_start * 2;
            let byte_len = chunk_len * 2;

            // Transposed loop: iterate recovery buffers in the outer loop so each
            // buffer is loaded into cache once and reused for all N output slices.
            // Uses multi-region kernel to read src once per SIMD chunk across all
            // destinations. GF addition is commutative — accumulation order doesn't
            // matter.
            for (r, recovery) in recovery_buffers.iter().enumerate() {
                let src = &recovery[byte_start..byte_start + byte_len];
                let mut pairs: Vec<crate::gf_simd::FactorDst<'_>> = (0..n)
                    .filter_map(|j| {
                        let factor = plan.decode_matrix.get(j, r);
                        if factor != 0 {
                            // Safe because each chunk task writes a disjoint byte
                            // range within every repaired slice, and the slices
                            // themselves live in distinct Vec allocations.
                            let dst = unsafe {
                                let ptr = repaired_ptrs[j] as *mut u8;
                                std::slice::from_raw_parts_mut(ptr.add(byte_start), byte_len)
                            };
                            Some(crate::gf_simd::FactorDst { factor, dst })
                        } else {
                            None
                        }
                    })
                    .collect();
                if !pairs.is_empty() {
                    crate::gf_simd::mul_acc_multi_region(&mut pairs, src);
                }
            }

            if let Some(ref progress) = options.progress {
                let current = completed_chunks.fetch_add(1, Ordering::Relaxed) + 1;
                progress(ProgressUpdate {
                    stage: ProgressStage::Repairing,
                    current,
                    total: total_chunks,
                    bytes_processed: (current as u64)
                        .saturating_mul(chunk_words as u64)
                        .saturating_mul(2)
                        .min(repair_total_bytes),
                    total_bytes: Some(operation_total_bytes),
                });
            }

            Ok(())
        })?;

    check_cancel(options)?;

    // Step 3: Write repaired slices back to files.
    info!("writing repaired slices to files");
    let write_targets = build_write_targets(plan, par2_set)?;

    for (j, target) in write_targets.iter().enumerate() {
        check_cancel(options)?;

        let slice_end = target.offset + plan.slice_size;
        let write_len = if slice_end > target.file_end {
            (target.file_end - target.offset) as usize
        } else {
            slice_size
        };

        file_access
            .write_file_range(
                &target.file_id,
                target.offset,
                &repaired_slices[j][..write_len],
            )
            .map_err(|e| Par2Error::RepairWriteFailed {
                filename: target.filename.clone(),
                offset: target.offset,
                source: e,
            })?;

        debug!(
            "repaired slice {} of file {} ({write_len} bytes at offset {})",
            plan.missing_slices[j].1, target.filename, target.offset
        );

        if let Some(ref progress) = options.progress {
            progress(ProgressUpdate {
                stage: ProgressStage::WritingRepaired,
                current: j as u32 + 1,
                total: n as u32,
                bytes_processed: repair_total_bytes
                    .saturating_add((j + 1) as u64 * slice_size as u64),
                total_bytes: Some(operation_total_bytes),
            });
        }
    }

    info!("repair complete: {} slices restored", n);
    Ok(())
}

#[cfg(target_arch = "x86_64")]
fn reconstruct_and_write_grouped_inputs(
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
    recovery_buffers: Vec<Vec<u8>>,
    file_access: &mut dyn FileAccess,
    chunk_words: usize,
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
    let word_count = slice_size / 2;
    let total_chunks_usize = word_count.div_ceil(chunk_words);
    let output_inputs = grouped_input_factors(&plan.decode_matrix);
    // Deduplicate prepared multiply tables per distinct factor value; see the
    // in-memory repair path for rationale.
    let mut factor_slots: HashMap<u16, usize> = HashMap::new();
    let mut prepared_factors: Vec<crate::gf_simd::PreparedInputFactor> = Vec::new();
    let prepared_output_inputs: Vec<Vec<(u16, usize)>> = output_inputs
        .iter()
        .map(|inputs| {
            inputs
                .iter()
                .map(|factor_input| {
                    let slot = *factor_slots.entry(factor_input.factor).or_insert_with(|| {
                        prepared_factors
                            .push(crate::gf_simd::prepare_input_factor(factor_input.factor));
                        prepared_factors.len() - 1
                    });
                    (factor_input.input_idx, slot)
                })
                .collect()
        })
        .collect();

    let mut repaired_slices: Vec<Vec<u8>> = vec![vec![0u8; slice_size]; n];
    let completed_outputs = AtomicU32::new(0);
    let repair_total_bytes = (n as u64).saturating_mul(slice_size as u64);
    let write_total_bytes = (n as u64).saturating_mul(slice_size as u64);
    let operation_total_bytes = repair_total_bytes.saturating_add(write_total_bytes);

    repaired_slices.par_iter_mut().enumerate().try_for_each(
        |(output_idx, repaired)| -> Result<()> {
            check_cancel(options)?;

            let decode_inputs = &prepared_output_inputs[output_idx];
            let mut chunk_inputs = Vec::with_capacity(decode_inputs.len());

            for chunk_idx in 0..total_chunks_usize {
                let chunk_start = chunk_idx * chunk_words;
                let chunk_end = (chunk_start + chunk_words).min(word_count);
                let byte_start = chunk_start * 2;
                let byte_len = (chunk_end - chunk_start) * 2;

                chunk_inputs.clear();
                for (input_idx, factor_slot) in decode_inputs {
                    chunk_inputs.push(crate::gf_simd::PreparedFactorSrc {
                        prepared: &prepared_factors[*factor_slot],
                        src: &recovery_buffers[*input_idx as usize]
                            [byte_start..byte_start + byte_len],
                    });
                }

                crate::gf_simd::mul_acc_input_batch_prepared(
                    &mut repaired[byte_start..byte_start + byte_len],
                    &chunk_inputs,
                );
            }

            if let Some(ref progress) = options.progress {
                let current = completed_outputs.fetch_add(1, Ordering::Relaxed) + 1;
                progress(ProgressUpdate {
                    stage: ProgressStage::Repairing,
                    current,
                    total: n as u32,
                    bytes_processed: current as u64 * slice_size as u64,
                    total_bytes: Some(operation_total_bytes),
                });
            }

            Ok(())
        },
    )?;

    check_cancel(options)?;

    info!("writing repaired slices to files");
    let write_targets = build_write_targets(plan, par2_set)?;

    for (j, target) in write_targets.iter().enumerate() {
        check_cancel(options)?;

        let slice_end = target.offset + plan.slice_size;
        let write_len = if slice_end > target.file_end {
            (target.file_end - target.offset) as usize
        } else {
            slice_size
        };

        file_access
            .write_file_range(
                &target.file_id,
                target.offset,
                &repaired_slices[j][..write_len],
            )
            .map_err(|e| Par2Error::RepairWriteFailed {
                filename: target.filename.clone(),
                offset: target.offset,
                source: e,
            })?;

        if let Some(ref progress) = options.progress {
            progress(ProgressUpdate {
                stage: ProgressStage::WritingRepaired,
                current: j as u32 + 1,
                total: n as u32,
                bytes_processed: repair_total_bytes
                    .saturating_add((j + 1) as u64 * slice_size as u64),
                total_bytes: Some(operation_total_bytes),
            });
        }
    }

    info!("repair complete: {} slices restored", n);
    Ok(())
}

/// Optional Apple-GPU arm of the streaming compute. All platform gating
/// lives here: on non-macOS builds every method is a no-op and the CPU
/// path runs unchanged; on macOS a session engages only when a Metal
/// device is present and the repair is large enough to amortize dispatch
/// (see `weaver_reed_solomon::metal_gf16`). Any GPU error permanently
/// disables the arm and the caller redoes the affected chunk on the CPU.
struct GpuComputeArm {
    #[cfg(target_os = "macos")]
    session: Option<weaver_reed_solomon::metal_gf16::MetalGf16Session>,
}

impl GpuComputeArm {
    #[cfg(target_os = "macos")]
    fn engage(use_folded: bool, outputs: usize, max_byte_len: usize, effective_bytes: u64) -> Self {
        // The folded path is x86-only, so `!use_folded` is the macOS shape;
        // the guard keeps the invariant explicit.
        let session = (!use_folded)
            .then(|| {
                weaver_reed_solomon::metal_gf16::MetalGf16Session::try_new(
                    outputs,
                    max_byte_len,
                    effective_bytes,
                )
            })
            .flatten();
        if let Some(session) = &session {
            info!(
                device = %session.device_name(),
                outputs,
                "metal gf16 tier engaged for streaming repair"
            );
        }
        Self { session }
    }

    #[cfg(not(target_os = "macos"))]
    fn engage(
        _use_folded: bool,
        _outputs: usize,
        _max_byte_len: usize,
        _effective_bytes: u64,
    ) -> Self {
        Self {}
    }

    /// True when the GPU owns this chunk's accumulation.
    fn begin_chunk(&mut self, _byte_len: usize) -> bool {
        #[cfg(target_os = "macos")]
        {
            if let Some(session) = self.session.as_mut() {
                match session.begin_chunk(_byte_len) {
                    Ok(()) => return true,
                    Err(reason) => {
                        warn!(reason, "metal gf16 begin_chunk failed; using CPU path");
                        self.session = None;
                    }
                }
            }
        }
        false
    }

    /// Queue one source batch. `Err` means the GPU arm died mid-chunk and
    /// the caller must redo the chunk on the CPU.
    fn accumulate(
        &mut self,
        _set: &StreamBatchSet,
        _plan: &RepairPlan,
        _byte_len: usize,
    ) -> std::result::Result<(), ()> {
        #[cfg(target_os = "macos")]
        {
            let Some(session) = self.session.as_mut() else {
                return Err(());
            };
            let srcs: Vec<&[u8]> = _set.bufs[.._set.len]
                .iter()
                .map(|buf| &buf[.._byte_len])
                .collect();
            let matrix = &_plan.input_factors;
            let start = _set.start;
            if let Err(reason) = session.accumulate(&srcs, |j, s| matrix.get(j, start + s)) {
                warn!(reason, "metal gf16 accumulate failed; redoing chunk on CPU");
                self.session = None;
                return Err(());
            }
            return Ok(());
        }
        #[allow(unreachable_code)]
        Err(())
    }

    /// Drain the chunk's dispatches into the output rows.
    fn finish_chunk(
        &mut self,
        _rows: &mut [Vec<u8>],
        _byte_len: usize,
    ) -> std::result::Result<(), ()> {
        #[cfg(target_os = "macos")]
        {
            let Some(session) = self.session.as_mut() else {
                return Err(());
            };
            if let Err(reason) = session.finish_chunk(_rows) {
                warn!(
                    reason,
                    "metal gf16 finish_chunk failed; redoing chunk on CPU"
                );
                self.session = None;
                return Err(());
            }
            return Ok(());
        }
        #[allow(unreachable_code)]
        Err(())
    }
}

fn execute_repair_streaming(
    plan: &RepairPlan,
    par2_set: &Par2FileSet,
    file_access: &mut dyn FileAccess,
    options: &RepairOptions,
    chunk_words: usize,
    budget: usize,
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
    let word_count = slice_size / 2;
    let total_chunks_usize = word_count.div_ceil(chunk_words);
    let total_chunks = total_chunks_usize.min(u32::MAX as usize) as u32;
    let operation_total_bytes = (word_count as u64).saturating_mul(2);
    let write_targets = build_write_targets(plan, par2_set)?;
    let mut recovery_files: HashMap<PathBuf, File> = HashMap::new();
    let max_byte_len = chunk_words * 2;
    let mut chunk_output: Vec<Vec<u8>> = vec![vec![0u8; max_byte_len]; n];
    let available_inputs = plan.available_input_global_indices.len();
    let total_sources = available_inputs + plan.recovery_exponents.len();
    // The folded path keeps every buffer in split byte-plane layout end to
    // end: sources are encoded and group-interleaved as they are read,
    // accumulation runs plane-wise with register-resident folded matrices,
    // and outputs decode once per chunk before writing.
    let use_folded = crate::gf_simd::altmap_supported();
    let memo = PreparedFactorMemo::from_matrix(&plan.input_factors, use_folded);
    let mut batch_sets = [
        StreamBatchSet::new(max_byte_len, use_folded),
        StreamBatchSet::new(max_byte_len, use_folded),
    ];
    let effective_bytes = (n as u64)
        .saturating_mul(total_sources as u64)
        .saturating_mul(plan.slice_size);
    let mut gpu = GpuComputeArm::engage(use_folded, n, max_byte_len, effective_bytes);

    info!(
        missing_slices = n,
        chunk_bytes = chunk_words * 2,
        budget_bytes = budget,
        source_batch = STREAM_INPUT_BATCH,
        "repairing with streamed chunk path"
    );

    let mut chunk_idx = 0usize;
    while chunk_idx < total_chunks_usize {
        check_cancel(options)?;

        let chunk_start = chunk_idx * chunk_words;
        let chunk_end = (chunk_start + chunk_words).min(word_count);
        let chunk_len = chunk_end - chunk_start;
        let byte_start = chunk_start * 2;
        let byte_len = chunk_len * 2;
        let output_ptrs: Vec<usize> = chunk_output
            .iter_mut()
            .map(|chunk| {
                chunk[..byte_len].fill(0);
                chunk.as_mut_ptr() as usize
            })
            .collect();

        // Batched source loop with read/compute overlap. CPU path: the
        // batch's multiply runs on the rayon pool while the caller thread
        // fills the other buffer set. GPU path: dispatches are queued
        // asynchronously, so the caller thread fills the next batch while
        // the GPU computes; a GPU failure redoes this whole chunk on the
        // CPU (destinations are re-zeroed at the top of the loop).
        let gpu_chunk = gpu.begin_chunk(byte_len);
        let batch_count = total_sources.div_ceil(STREAM_INPUT_BATCH);
        fill_stream_batch(
            &mut batch_sets[0],
            plan,
            par2_set,
            file_access,
            &mut recovery_files,
            available_inputs,
            0,
            STREAM_INPUT_BATCH.min(total_sources),
            byte_start,
            byte_len,
            use_folded,
            options,
        )?;
        let mut gpu_failed = false;
        for batch_idx in 0..batch_count {
            check_cancel(options)?;
            let (set_a, set_b) = batch_sets.split_at_mut(1);
            let (current, next): (&StreamBatchSet, &mut StreamBatchSet) = if batch_idx % 2 == 0 {
                (&set_a[0], &mut set_b[0])
            } else {
                (&set_b[0], &mut set_a[0])
            };

            let mut read_result: Result<()> = Ok(());
            if gpu_chunk {
                if gpu.accumulate(current, plan, byte_len).is_err() {
                    gpu_failed = true;
                    break;
                }
                if batch_idx + 1 < batch_count {
                    let next_start = (batch_idx + 1) * STREAM_INPUT_BATCH;
                    let next_len = STREAM_INPUT_BATCH.min(total_sources - next_start);
                    read_result = fill_stream_batch(
                        next,
                        plan,
                        par2_set,
                        file_access,
                        &mut recovery_files,
                        available_inputs,
                        next_start,
                        next_len,
                        byte_start,
                        byte_len,
                        use_folded,
                        options,
                    );
                }
            } else {
                rayon::in_place_scope(|scope| {
                    let output_ptrs = &output_ptrs;
                    let memo = &memo;
                    scope.spawn(move |_| {
                        run_stream_batch_compute(
                            output_ptrs,
                            current,
                            plan,
                            memo,
                            byte_len,
                            use_folded,
                        );
                    });
                    if batch_idx + 1 < batch_count {
                        let next_start = (batch_idx + 1) * STREAM_INPUT_BATCH;
                        let next_len = STREAM_INPUT_BATCH.min(total_sources - next_start);
                        read_result = fill_stream_batch(
                            next,
                            plan,
                            par2_set,
                            file_access,
                            &mut recovery_files,
                            available_inputs,
                            next_start,
                            next_len,
                            byte_start,
                            byte_len,
                            use_folded,
                            options,
                        );
                    }
                });
            }
            read_result?;
        }
        if gpu_chunk && !gpu_failed && gpu.finish_chunk(&mut chunk_output, byte_len).is_err() {
            gpu_failed = true;
        }
        if gpu_failed {
            // The arm is disabled; redo this chunk from batch zero on the
            // CPU path.
            continue;
        }

        if use_folded {
            chunk_output.par_iter_mut().for_each(|output| {
                crate::gf_simd::altmap_decode(&mut output[..byte_len]);
            });
        }

        for (j, target) in write_targets.iter().enumerate() {
            let write_offset = target.offset + byte_start as u64;
            let remaining = target.file_end.saturating_sub(write_offset);
            let write_len = remaining.min(byte_len as u64) as usize;
            if write_len == 0 {
                continue;
            }

            file_access
                .write_file_range(&target.file_id, write_offset, &chunk_output[j][..write_len])
                .map_err(|e| Par2Error::RepairWriteFailed {
                    filename: target.filename.clone(),
                    offset: write_offset,
                    source: e,
                })?;
        }

        if let Some(ref progress) = options.progress {
            progress(ProgressUpdate {
                stage: ProgressStage::Repairing,
                current: chunk_idx as u32 + 1,
                total: total_chunks,
                bytes_processed: ((chunk_idx + 1) as u64)
                    .saturating_mul(chunk_words as u64)
                    .saturating_mul(2)
                    .min(operation_total_bytes),
                total_bytes: Some(operation_total_bytes),
            });
        }
        chunk_idx += 1;
    }

    info!("streaming repair complete: {} slices restored", n);
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

    match select_repair_execution_mode(plan, options) {
        RepairExecutionMode::Streaming {
            chunk_words,
            budget,
        } => execute_repair_streaming(plan, par2_set, file_access, options, chunk_words, budget),
        RepairExecutionMode::InMemory { chunk_words } => {
            info!("reconstructing {} missing slices", n);
            let output_inputs = grouped_input_factors(&plan.input_factors);
            let available_inputs = plan.available_input_global_indices.len();
            let total_inputs = available_inputs + plan.recovery_exponents.len();
            let total_inputs_u32 = total_inputs.min(u32::MAX as usize) as u32;
            let read_total_bytes = (total_inputs as u64).saturating_mul(slice_size as u64);
            let repair_total_bytes = (n as u64).saturating_mul(slice_size as u64);
            let write_total_bytes = (n as u64).saturating_mul(slice_size as u64);
            let operation_total_bytes = read_total_bytes
                .saturating_add(repair_total_bytes)
                .saturating_add(write_total_bytes);
            let mut input_buffers: Vec<Vec<u8>> = vec![vec![0u8; slice_size]; total_inputs];
            let mut repaired_slices: Vec<Vec<u8>> = vec![vec![0u8; slice_size]; n];

            for (input_idx, &global_idx) in plan.available_input_global_indices.iter().enumerate() {
                if input_idx % 64 == 0 {
                    check_cancel(options)?;
                }

                let (file_id, local_slice) = plan.global_to_file[global_idx];
                let offset = local_slice as u64 * plan.slice_size;
                let read_len = file_access
                    .read_file_range_into(&file_id, offset, &mut input_buffers[input_idx])
                    .map_err(Par2Error::Io)?;
                input_buffers[input_idx][read_len..].fill(0);

                if let Some(ref progress) = options.progress {
                    progress(ProgressUpdate {
                        stage: ProgressStage::Repairing,
                        current: input_idx as u32 + 1,
                        total: total_inputs_u32,
                        bytes_processed: (input_idx + 1) as u64 * slice_size as u64,
                        total_bytes: Some(operation_total_bytes),
                    });
                }
            }

            for (recovery_idx, &exp) in plan.recovery_exponents.iter().enumerate() {
                check_cancel(options)?;
                let rs = par2_set.recovery_slices.get(&exp).ok_or_else(|| {
                    Par2Error::ReedSolomonError {
                        reason: format!("recovery block with exponent {exp} not found"),
                    }
                })?;
                let recovery_data = rs.data.to_vec().map_err(Par2Error::Io)?;
                let copy_len = recovery_data.len().min(slice_size);
                input_buffers[available_inputs + recovery_idx][..copy_len]
                    .copy_from_slice(&recovery_data[..copy_len]);
                input_buffers[available_inputs + recovery_idx][copy_len..].fill(0);

                if let Some(ref progress) = options.progress {
                    let current = available_inputs + recovery_idx + 1;
                    progress(ProgressUpdate {
                        stage: ProgressStage::Repairing,
                        current: current.min(u32::MAX as usize) as u32,
                        total: total_inputs_u32,
                        bytes_processed: current as u64 * slice_size as u64,
                        total_bytes: Some(operation_total_bytes),
                    });
                }
            }
            // Prepared multiply tables are keyed by factor value: a dense
            // decode matrix repeats factors across (output, input) pairs, and
            // preparing one table per pair multiplies memory (and cache
            // pressure) by outputs*inputs. At most 65535 distinct factors
            // exist, so the deduplicated pool stays a few megabytes.
            let mut factor_slots: HashMap<u16, usize> = HashMap::new();
            let mut prepared_factors: Vec<crate::gf_simd::PreparedInputFactor> = Vec::new();
            let prepared_output_inputs: Vec<Vec<(u16, usize)>> = output_inputs
                .iter()
                .map(|inputs| {
                    inputs
                        .iter()
                        .map(|factor_input| {
                            let slot =
                                *factor_slots.entry(factor_input.factor).or_insert_with(|| {
                                    prepared_factors.push(crate::gf_simd::prepare_input_factor(
                                        factor_input.factor,
                                    ));
                                    prepared_factors.len() - 1
                                });
                            (factor_input.input_idx, slot)
                        })
                        .collect()
                })
                .collect();
            let total_chunks_usize = (slice_size / 2).div_ceil(chunk_words);
            let completed_outputs = AtomicU32::new(0);

            repaired_slices.par_iter_mut().enumerate().try_for_each(
                |(output_idx, repaired)| -> Result<()> {
                    check_cancel(options)?;
                    let decode_inputs = &prepared_output_inputs[output_idx];
                    let mut chunk_inputs = Vec::with_capacity(decode_inputs.len());

                    for chunk_idx in 0..total_chunks_usize {
                        let chunk_start = chunk_idx * chunk_words;
                        let chunk_end = (chunk_start + chunk_words).min(slice_size / 2);
                        let byte_start = chunk_start * 2;
                        let byte_len = (chunk_end - chunk_start) * 2;

                        chunk_inputs.clear();
                        for (input_idx, factor_slot) in decode_inputs {
                            chunk_inputs.push(crate::gf_simd::PreparedFactorSrc {
                                prepared: &prepared_factors[*factor_slot],
                                src: &input_buffers[*input_idx as usize]
                                    [byte_start..byte_start + byte_len],
                            });
                        }

                        crate::gf_simd::mul_acc_input_batch_prepared(
                            &mut repaired[byte_start..byte_start + byte_len],
                            &chunk_inputs,
                        );
                    }

                    if let Some(ref progress) = options.progress {
                        let current = completed_outputs.fetch_add(1, Ordering::Relaxed) + 1;
                        progress(ProgressUpdate {
                            stage: ProgressStage::Repairing,
                            current,
                            total: n as u32,
                            bytes_processed: read_total_bytes
                                .saturating_add(current as u64 * slice_size as u64),
                            total_bytes: Some(operation_total_bytes),
                        });
                    }

                    Ok(())
                },
            )?;

            info!("writing repaired slices to files");
            let write_targets = build_write_targets(plan, par2_set)?;
            for (j, target) in write_targets.iter().enumerate() {
                check_cancel(options)?;

                let slice_end = target.offset + plan.slice_size;
                let write_len = if slice_end > target.file_end {
                    (target.file_end - target.offset) as usize
                } else {
                    slice_size
                };

                file_access
                    .write_file_range(
                        &target.file_id,
                        target.offset,
                        &repaired_slices[j][..write_len],
                    )
                    .map_err(|e| Par2Error::RepairWriteFailed {
                        filename: target.filename.clone(),
                        offset: target.offset,
                        source: e,
                    })?;

                if let Some(ref progress) = options.progress {
                    progress(ProgressUpdate {
                        stage: ProgressStage::WritingRepaired,
                        current: j as u32 + 1,
                        total: n as u32,
                        bytes_processed: read_total_bytes
                            .saturating_add(repair_total_bytes)
                            .saturating_add((j + 1) as u64 * slice_size as u64),
                        total_bytes: Some(operation_total_bytes),
                    });
                }
            }

            info!("repair complete: {} slices restored", n);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksum::{self, SliceChecksumState};
    use crate::packet::header;
    use crate::par2_set::{Par2FileSet, RecoverySlice};
    use crate::types::SliceChecksum;
    use crate::verify::{self, FileStatus, FileVerification, MemoryFileAccess};
    use bytes::Bytes;
    use md5::{Digest, Md5};
    use tempfile::tempdir;

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
                    data: Bytes::from(recovery).into(),
                },
            );
        }

        (set, file_id)
    }

    fn spill_recovery_slices_to_disk(set: &mut Par2FileSet) -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        for (exp, slice) in &mut set.recovery_slices {
            let path = dir.path().join(format!("recovery_{exp}.bin"));
            let bytes = slice.data.to_vec().unwrap();
            std::fs::write(&path, &bytes).unwrap();
            slice.data = crate::packet::RecoverySliceData::file_backed(path, 0, bytes.len());
        }
        dir
    }

    /// Like [`spill_recovery_slices_to_disk`], but records the PAR2 packet
    /// hash the streaming scanner would have captured, enabling lazy payload
    /// validation.
    fn spill_recovery_slices_to_disk_with_hashes(set: &mut Par2FileSet) -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        let rsid = *set.recovery_set_id.as_bytes();
        for (exp, slice) in &mut set.recovery_slices {
            let path = dir.path().join(format!("recovery_{exp}.bin"));
            let bytes = slice.data.to_vec().unwrap();
            std::fs::write(&path, &bytes).unwrap();

            let mut hash_input = Vec::new();
            hash_input.extend_from_slice(&rsid);
            hash_input.extend_from_slice(header::TYPE_RECOVERY);
            hash_input.extend_from_slice(&exp.to_le_bytes());
            hash_input.extend_from_slice(&bytes);
            let packet_hash: [u8; 16] = Md5::digest(&hash_input).into();

            slice.data = crate::packet::RecoverySliceData::file_backed_with_hash(
                path,
                0,
                bytes.len(),
                packet_hash,
            );
        }
        dir
    }

    #[test]
    fn plan_repair_skips_recovery_blocks_with_corrupt_payloads() {
        let slice_size = 64u64;
        let file_data: Vec<u8> = (0..256u32).map(|i| ((i * 11 + 3) % 256) as u8).collect();
        let (mut par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 4);
        let spill_dir = spill_recovery_slices_to_disk_with_hashes(&mut par2_set);

        // Corrupt the payloads of the two lowest exponents on disk. Selection
        // prefers low exponents, so the plan must detect the damage and fall
        // back to the clean blocks.
        for exp in [0u32, 1] {
            let path = spill_dir.path().join(format!("recovery_{exp}.bin"));
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[7] ^= 0xFF;
            std::fs::write(&path, &bytes).unwrap();
        }

        let mut damaged = file_data.clone();
        damaged[..64].fill(0);
        damaged[128..192].fill(0);

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, damaged);

        let result = verify::verify_all(&par2_set, &access);
        assert_eq!(result.total_missing_blocks, 2);

        let plan = plan_repair(&par2_set, &result).unwrap();
        assert!(!plan.recovery_exponents.contains(&0));
        assert!(!plan.recovery_exponents.contains(&1));

        execute_repair(&plan, &par2_set, &mut access).unwrap();
        let repaired = access.read_file(&file_id).unwrap();
        assert_eq!(repaired, file_data);
    }

    #[test]
    fn plan_repair_fails_when_all_recovery_payloads_are_corrupt() {
        let slice_size = 64u64;
        let file_data: Vec<u8> = (0..256u32).map(|i| ((i * 5 + 1) % 256) as u8).collect();
        let (mut par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 2);
        let spill_dir = spill_recovery_slices_to_disk_with_hashes(&mut par2_set);

        for exp in [0u32, 1] {
            let path = spill_dir.path().join(format!("recovery_{exp}.bin"));
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[0] ^= 0x01;
            std::fs::write(&path, &bytes).unwrap();
        }

        let mut damaged = file_data.clone();
        damaged[..64].fill(0);

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, damaged);

        let result = verify::verify_all(&par2_set, &access);
        let err = plan_repair(&par2_set, &result).unwrap_err();
        assert!(matches!(err, Par2Error::InsufficientRecoveryData { .. }));
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
    fn plan_repair_rejects_resource_limited_verification() {
        let slice_size = 64u64;
        let file_data = vec![0xABu8; 128];
        let (par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 2);
        let result = VerificationResult {
            files: vec![FileVerification {
                file_id,
                filename: "testfile.dat".to_string(),
                status: FileStatus::Damaged(0),
                valid_slices: Vec::new(),
                missing_slice_count: 0,
            }],
            recovery_blocks_available: 2,
            total_missing_blocks: 0,
            repairable: Repairability::ResourceLimited {
                reason: "file testfile.dat exceeds verifier slice limits".to_string(),
            },
        };

        let err = plan_repair(&par2_set, &result).unwrap_err();
        assert!(matches!(err, Par2Error::ResourceLimitExceeded { .. }));
    }

    #[test]
    fn matrix_memory_budget_has_floor_but_still_caps() {
        // A tiny slice-buffer limit no longer starves the transient decode
        // matrix: planning small repairs succeeds even at absurd limits.
        assert!(repair_matrix_limit_reason(4, 2, Some(8)).is_none());

        // A matrix that exceeds even the budget floor is still refused...
        let missing = 20_000usize;
        let reason = repair_matrix_limit_reason(32_768, missing, Some(8)).unwrap();
        assert!(reason.contains("matrix workspace budget"));

        // ...unless the caller raises the limit explicitly.
        assert!(repair_matrix_limit_reason(32_768, missing, Some(8 << 30)).is_none());

        // Sets beyond the PAR2 total-slice cap are reported as such.
        let reason = repair_matrix_limit_reason(40_000, 1, None).unwrap();
        assert!(reason.contains("at most"));
    }

    #[test]
    fn plan_repair_succeeds_with_tiny_configured_memory_limit() {
        let slice_size = 64u64;
        let file_data: Vec<u8> = (0..256u32).map(|i| (i % 256) as u8).collect();
        let (par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 3);

        let mut damaged = file_data.clone();
        damaged[..64].fill(0);
        damaged[64..128].fill(0);

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, damaged);

        let result = verify::verify_all(&par2_set, &access);
        assert_eq!(result.total_missing_blocks, 2);

        let plan = plan_repair_with_memory_limit(&par2_set, &result, Some(8)).unwrap();
        assert_eq!(plan.missing_slices.len(), 2);
    }

    #[test]
    fn plan_repair_rejects_sets_over_total_slice_limit() {
        let slice_size = 4u64;
        let slices_per_file = 20_000u64;
        let mut files = HashMap::new();
        let mut recovery_file_ids = Vec::new();
        let mut verifications = Vec::new();
        for index in 0..2u8 {
            let file_id = FileId::from_bytes([index + 1; 16]);
            recovery_file_ids.push(file_id);
            files.insert(
                file_id,
                crate::par2_set::FileDescription {
                    file_id,
                    hash_full: [0; 16],
                    hash_16k: [0; 16],
                    length: slice_size * slices_per_file,
                    par2_name: format!("big{index}.dat"),
                    filename: format!("big{index}.dat"),
                },
            );
            let mut valid_slices = vec![true; slices_per_file as usize];
            if index == 0 {
                valid_slices[0] = false;
            }
            verifications.push(FileVerification {
                file_id,
                filename: format!("big{index}.dat"),
                status: if index == 0 {
                    FileStatus::Damaged(1)
                } else {
                    FileStatus::Complete
                },
                missing_slice_count: u32::from(index == 0),
                valid_slices,
            });
        }

        let mut recovery_slices = std::collections::BTreeMap::new();
        recovery_slices.insert(
            0,
            RecoverySlice {
                exponent: 0,
                data: Bytes::from(vec![0u8; slice_size as usize]).into(),
            },
        );
        let par2_set = Par2FileSet {
            recovery_set_id: crate::types::RecoverySetId::from_bytes([9; 16]),
            slice_size,
            recovery_file_ids,
            non_recovery_file_ids: Vec::new(),
            files,
            slice_checksums: HashMap::new(),
            recovery_slices,
            creator: None,
        };
        let result = VerificationResult {
            files: verifications,
            recovery_blocks_available: 1,
            total_missing_blocks: 1,
            repairable: Repairability::Repairable {
                blocks_needed: 1,
                blocks_available: 1,
            },
        };

        let err = plan_repair(&par2_set, &result).unwrap_err();
        assert!(matches!(err, Par2Error::ResourceLimitExceeded { .. }));
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

    #[test]
    fn repair_with_tiny_memory_limit_still_succeeds() {
        let slice_size = 128u64;
        let file_data: Vec<u8> = (0..384u32).map(|i| ((i * 9 + 17) % 256) as u8).collect();
        let (par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 3);

        let mut damaged = file_data.clone();
        for item in damaged.iter_mut().take(128) {
            *item = 0;
        }
        for item in damaged.iter_mut().take(384).skip(256) {
            *item = 0;
        }

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, damaged);

        let result = verify::verify_all(&par2_set, &access);
        let plan = plan_repair(&par2_set, &result).unwrap();

        execute_repair_with_options(
            &plan,
            &par2_set,
            &mut access,
            &RepairOptions {
                memory_limit: Some(32),
                ..RepairOptions::default()
            },
        )
        .unwrap();

        let repaired = access.read_file(&file_id).unwrap();
        assert_eq!(repaired, file_data);
    }

    #[test]
    fn repair_with_file_backed_recovery_streaming_succeeds() {
        let slice_size = 128u64;
        let file_data: Vec<u8> = (0..384u32).map(|i| ((i * 9 + 17) % 256) as u8).collect();
        let (mut par2_set, file_id) = setup_repairable_set(&file_data, slice_size, 3);
        let _spill_dir = spill_recovery_slices_to_disk(&mut par2_set);

        let mut damaged = file_data.clone();
        for item in damaged.iter_mut().take(128) {
            *item = 0;
        }
        for item in damaged.iter_mut().take(384).skip(256) {
            *item = 0;
        }

        let mut access = MemoryFileAccess::new();
        access.add_file(file_id, damaged);

        let result = verify::verify_all(&par2_set, &access);
        let plan = plan_repair(&par2_set, &result).unwrap();

        execute_repair_with_options(
            &plan,
            &par2_set,
            &mut access,
            &RepairOptions {
                memory_limit: Some(32),
                ..RepairOptions::default()
            },
        )
        .unwrap();

        let repaired = access.read_file(&file_id).unwrap();
        assert_eq!(repaired, file_data);
    }

    fn synthetic_plan(missing_slices: usize, slice_size: u64) -> RepairPlan {
        RepairPlan {
            missing_slices: (0..missing_slices)
                .map(|i| (FileId::from_bytes([i as u8; 16]), i as u32))
                .collect(),
            missing_global_indices: (0..missing_slices).collect(),
            available_input_global_indices: Vec::new(),
            recovery_exponents: (0..missing_slices as u32).collect(),
            decode_matrix: matrix::Matrix {
                rows: missing_slices,
                cols: missing_slices,
                data: vec![1; missing_slices.saturating_mul(missing_slices)],
            },
            input_factors: matrix::Matrix {
                rows: missing_slices,
                cols: missing_slices,
                data: vec![1; missing_slices.saturating_mul(missing_slices)],
            },
            slice_size,
            constants: vec![1; missing_slices],
            total_input_slices: missing_slices,
            global_to_file: (0..missing_slices)
                .map(|i| (FileId::from_bytes([i as u8; 16]), i as u32))
                .collect(),
        }
    }

    #[test]
    fn repair_selector_prefers_streaming_when_budget_is_tight() {
        let plan = synthetic_plan(450, 64 * 1024);
        let mode = select_repair_execution_mode(
            &plan,
            &RepairOptions {
                memory_limit: Some(50 * 1024 * 1024),
                ..RepairOptions::default()
            },
        );
        assert!(matches!(mode, RepairExecutionMode::Streaming { .. }));
    }

    #[test]
    fn repair_selector_keeps_fast_path_when_budget_allows() {
        let plan = synthetic_plan(8, 64 * 1024);
        let mode = select_repair_execution_mode(
            &plan,
            &RepairOptions {
                memory_limit: Some(16 * 1024 * 1024),
                ..RepairOptions::default()
            },
        );
        assert!(matches!(mode, RepairExecutionMode::InMemory { .. }));
    }
}
