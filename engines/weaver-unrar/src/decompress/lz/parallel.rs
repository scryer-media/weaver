//! Block-level parallel Huffman decode for RAR5 LZ decompression.
//!
//! Matches unrar's `RAR_SMP` approach: scan block headers sequentially to
//! resolve Huffman table dependencies, dispatch per-block symbol decoding
//! to rayon worker threads, then apply decoded items to the window serially.
//!
//! The parallelism is in the Huffman decode phase only — window writes,
//! distance cache updates, and filter application remain sequential because
//! they depend on running output state.

use rayon::prelude::*;
use std::sync::Arc;

use super::bitstream::{BitRead, BitReader};
use super::filter::{FilterType, PendingFilter};
use super::huffman::{self, HuffmanTable};
use super::{LzDecoder, NUM_LENGTH_SLOTS};
use crate::error::{RarError, RarResult};

#[derive(Clone)]
struct PreparsedBlocks {
    blocks: Vec<BlockInfo>,
    table_sets: Vec<TableSet>,
    consumed_bytes: usize,
    saw_last_block: bool,
}

/// Minimum number of blocks to justify parallel dispatch.
/// Below this, rayon overhead exceeds the benefit.
const MIN_PARALLEL_BLOCKS: usize = 4;

/// Maximum number of blocks per parallel dispatch. Typical RAR5 blocks carry
/// tens of kilobytes of compressed data, so this covers a multi-megabyte span
/// per fork/join — few enough barriers that worker wake/park churn stays
/// negligible next to decode work.
const MAX_BATCH_BLOCKS: usize = 64;

/// Per-block decoded item buffer size.
/// Matches unrar's `DecodedAllocated = 0x4100`.
const DECODED_ITEMS_CAPACITY: usize = 0x4100;

/// Maximum worker count to consider when sizing parallel decode batches.
/// Mirrors unrar's `MaxUserThreads = Min(Threads, 8)` behavior.
const MAX_PARALLEL_THREADS: usize = 8;

/// Maximum compressed block size (in bits) for parallel decode.
/// Blocks exceeding this fall back to single-threaded inline decode.
/// Matches unrar's LargeBlockSize = 0x20000 bytes = 1,048,576 bits.
const LARGE_BLOCK_BITS: i64 = 0x20000 * 8;

/// Maximum number of pending filters to hold at once.
/// Mirrors unrar's defensive `MAX_UNPACK_FILTERS` bound.
const MAX_PENDING_FILTERS: usize = 8192;

/// Maximum accepted filter block size.
/// Mirrors unrar's `MAX_FILTER_BLOCK_SIZE` bound.
const MAX_FILTER_BLOCK_SIZE: u32 = 0x400000;

// ─── Types ───────────────────────────────────────────────────────────────────

/// A decoded Huffman item — one logical operation extracted from the bitstream.
///
/// Matches unrar's `UnpackDecodedItem`. Everything needed from the bitstream
/// is fully resolved; only sequential state (window, dist_cache, last_length)
/// is deferred to the apply phase.
#[derive(Clone, Copy)]
pub enum DecodedItem {
    /// 1–8 consecutive literal bytes, batched for cache efficiency.
    Literals { bytes: [u8; 8], count: u8 },
    /// Inline match (sym >= 262): length and distance fully resolved.
    /// Distance is 1-based. Length includes distance-based adjustment.
    /// Distance must be u64: RAR7 (`extra_dist`) match distances exceed 4 GiB.
    Match { length: u32, distance: u64 },
    /// Repeat previous match (sym 257). Uses current last_length + dist_cache[0].
    RepeatPrev,
    /// Cache reference (sym 258–261). Distance resolved from cache during apply.
    CacheRef { cache_idx: u8, length: u32 },
    /// Filter marker (sym 256). `block_start_delta` is relative to output_size
    /// at the point this item is applied (NOT an absolute offset).
    Filter {
        filter_type: u8,
        block_start_delta: u64,
        block_length: u32,
        channels: u8,
    },
}

/// Metadata for one LZ block, parsed during the sequential header scan.
#[derive(Clone)]
struct BlockInfo {
    /// Bit offset in the input where this block's compressed data starts
    /// (after header and any table data have been consumed).
    data_bit_offset: usize,
    /// Number of compressed bits in this block's data section.
    data_bits: i64,
    /// Index into the `table_sets` array.
    table_set_index: usize,
    /// Whether this block exceeds the large-block threshold.
    is_large: bool,
}

/// A set of Huffman tables shared by one or more blocks.
#[derive(Clone)]
struct TableSet {
    nc: Arc<HuffmanTable>,
    dc: Arc<HuffmanTable>,
    ldc: Arc<HuffmanTable>,
    rc: Arc<HuffmanTable>,
}

pub(super) fn parallel_enabled() -> bool {
    parallel_enabled_from_disable_env(std::env::var_os("WEAVER_RAR_DISABLE_PARALLEL").as_deref())
}

fn parallel_enabled_from_disable_env(disable_env: Option<&std::ffi::OsStr>) -> bool {
    disable_env.is_none()
}

pub(super) fn is_truncated_input_error(error: &RarError) -> bool {
    match error {
        RarError::InvalidHuffmanTable => true,
        RarError::CorruptArchive { detail } => {
            detail.contains("unexpected end of data")
                || detail.contains("truncated")
                || detail.contains("no bits remaining")
                || detail.contains("need ")
                || detail.contains("cannot skip")
        }
        _ => false,
    }
}

fn parse_next_block(
    reader: &mut BitReader<'_>,
    code_lengths: &mut [u8],
    table_sets: &mut Vec<TableSet>,
    extra_dist: bool,
) -> RarResult<Option<(BlockInfo, bool)>> {
    if !reader.has_bits() {
        return Ok(None);
    }

    reader.align_byte();

    if reader.bits_remaining() < 16 {
        return Ok(None);
    }

    let flags = reader.read_bits(8)? as u8;
    let checksum = reader.read_bits(8)? as u8;

    let extra_bits = (flags & 0x07) as i64 + 1;
    let num_size_bytes = ((flags >> 3) & 0x03) + 1;
    if num_size_bytes > 3 {
        return Err(RarError::CorruptArchive {
            detail: "RAR5 block header: invalid size byte count".into(),
        });
    }

    let is_last = (flags & 0x40) != 0;
    let table_present = (flags & 0x80) != 0;

    let mut block_bytes: i64 = 0;
    let mut xor_sum = 0x5Au8 ^ flags;
    for i in 0..num_size_bytes {
        let b = reader.read_bits(8)? as u8;
        xor_sum ^= b;
        block_bytes |= (b as i64) << (i * 8);
    }

    if xor_sum != checksum {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "RAR5 block header checksum mismatch: expected {:#04x}, got {:#04x}",
                checksum, xor_sum
            ),
        });
    }

    let mut block_bits_remaining = if block_bytes == 0 {
        0
    } else {
        extra_bits + (block_bytes - 1) * 8
    };

    if !table_present && table_sets.is_empty() {
        return Err(RarError::CorruptArchive {
            detail: "RAR5 first block is missing Huffman tables".into(),
        });
    }

    let table_set_index = if table_present {
        let pos_before = reader.position();
        let (nc, dc, ldc, rc) = huffman::read_tables_bitreader(reader, code_lengths, extra_dist)?;
        let bits_used = (reader.position() - pos_before) as i64;
        block_bits_remaining -= bits_used;
        table_sets.push(TableSet {
            nc: Arc::new(nc),
            dc: Arc::new(dc),
            ldc: Arc::new(ldc),
            rc: Arc::new(rc),
        });
        table_sets.len() - 1
    } else {
        table_sets.len() - 1
    };

    let data_bit_offset = reader.position();
    let is_large = block_bits_remaining > LARGE_BLOCK_BITS;

    if block_bits_remaining > 0 {
        reader.skip_bits(block_bits_remaining as u32)?;
    }

    Ok(Some((
        BlockInfo {
            data_bit_offset,
            data_bits: block_bits_remaining,
            table_set_index,
            is_large,
        },
        is_last,
    )))
}

fn preparse_complete_blocks(
    input: &[u8],
    header_limit: usize,
    code_lengths: &mut [u8],
    existing_tables: Option<&TableSet>,
    extra_dist: bool,
) -> RarResult<PreparsedBlocks> {
    let mut reader = BitReader::new(input);
    let estimated_blocks = (input.len() / 0x4000).clamp(8, 512);
    let mut blocks = Vec::with_capacity(estimated_blocks);
    let mut table_sets = Vec::with_capacity(estimated_blocks.min(64));
    let mut working_code_lengths = code_lengths.to_vec();
    let mut consumed_bytes = 0usize;
    let mut saw_last_block = false;

    if let Some(ts) = existing_tables {
        table_sets.push(ts.clone());
    }

    loop {
        // Stop accepting blocks whose header would start at or beyond the
        // caller's limit (volume boundary or unreliable staging tail).
        if reader.position().div_ceil(8) >= header_limit {
            break;
        }

        let checkpoint_code_lengths = working_code_lengths.clone();
        let checkpoint_table_set_len = table_sets.len();

        match parse_next_block(
            &mut reader,
            &mut working_code_lengths,
            &mut table_sets,
            extra_dist,
        ) {
            Ok(Some((block, is_last))) => {
                blocks.push(block);
                consumed_bytes = reader.position().div_ceil(8);
                if is_last {
                    saw_last_block = true;
                    break;
                }
            }
            Ok(None) => {
                working_code_lengths = checkpoint_code_lengths;
                table_sets.truncate(checkpoint_table_set_len);
                break;
            }
            Err(error) if is_truncated_input_error(&error) => {
                working_code_lengths = checkpoint_code_lengths;
                table_sets.truncate(checkpoint_table_set_len);
                break;
            }
            Err(error) => return Err(error),
        }
    }

    if !blocks.is_empty() {
        code_lengths.copy_from_slice(&working_code_lengths);
    }

    Ok(PreparsedBlocks {
        blocks,
        table_sets,
        consumed_bytes,
        saw_last_block,
    })
}

// ─── Phase 1: Sequential block header pre-parsing ────────────────────────────

/// Pre-parse all RAR5 LZ block headers from the input.
///
/// Reads headers sequentially, resolving Huffman table dependencies
/// (blocks with `table_present=false` inherit the previous block's tables).
/// Returns the block metadata and the table sets, plus the final code_lengths
/// state (for solid archive continuity).
fn preparse_blocks(
    input: &[u8],
    code_lengths: &mut [u8],
    existing_tables: Option<&TableSet>,
    extra_dist: bool,
) -> RarResult<(Vec<BlockInfo>, Vec<TableSet>)> {
    let mut reader = BitReader::new(input);
    let mut blocks = Vec::new();
    let mut table_sets: Vec<TableSet> = Vec::new();

    // If we have existing tables from a prior call (solid archive),
    // seed the table_sets so blocks can reference them.
    if let Some(ts) = existing_tables {
        table_sets.push(TableSet {
            nc: ts.nc.clone(),
            dc: ts.dc.clone(),
            ldc: ts.ldc.clone(),
            rc: ts.rc.clone(),
        });
    }

    loop {
        if !reader.has_bits() {
            break;
        }

        // Block header is byte-aligned.
        reader.align_byte();

        if reader.bits_remaining() < 16 {
            break; // Not enough for a minimal header.
        }

        let flags = reader.read_bits(8)? as u8;
        let checksum = reader.read_bits(8)? as u8;

        let extra_bits = (flags & 0x07) as i64 + 1;
        let num_size_bytes = ((flags >> 3) & 0x03) + 1;
        if num_size_bytes > 3 {
            return Err(RarError::CorruptArchive {
                detail: "RAR5 block header: invalid size byte count".into(),
            });
        }

        let is_last = (flags & 0x40) != 0;
        let table_present = (flags & 0x80) != 0;

        let mut block_bytes: i64 = 0;
        let mut xor_sum = 0x5Au8 ^ flags;
        for i in 0..num_size_bytes {
            let b = reader.read_bits(8)? as u8;
            xor_sum ^= b;
            block_bytes |= (b as i64) << (i * 8);
        }

        if xor_sum != checksum {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "RAR5 block header checksum mismatch: expected {:#04x}, got {:#04x}",
                    checksum, xor_sum
                ),
            });
        }

        let mut block_bits_remaining = if block_bytes == 0 {
            0
        } else {
            extra_bits + (block_bytes - 1) * 8
        };

        if !table_present && table_sets.is_empty() {
            return Err(RarError::CorruptArchive {
                detail: "RAR5 first block is missing Huffman tables".into(),
            });
        }

        // Read tables if present — this advances the reader and consumes bits.
        let table_set_index = if table_present {
            let pos_before = reader.position();
            let (nc, dc, ldc, rc) =
                huffman::read_tables_bitreader(&mut reader, code_lengths, extra_dist)?;
            let bits_used = (reader.position() - pos_before) as i64;
            block_bits_remaining -= bits_used;
            table_sets.push(TableSet {
                nc: Arc::new(nc),
                dc: Arc::new(dc),
                ldc: Arc::new(ldc),
                rc: Arc::new(rc),
            });
            table_sets.len() - 1
        } else {
            table_sets.len() - 1
        };

        let data_bit_offset = reader.position();
        let is_large = block_bits_remaining > LARGE_BLOCK_BITS;

        blocks.push(BlockInfo {
            data_bit_offset,
            data_bits: block_bits_remaining,
            table_set_index,
            is_large,
        });

        // Skip past this block's data to reach the next header.
        if block_bits_remaining > 0 {
            reader.skip_bits(block_bits_remaining as u32)?;
        }

        if is_last {
            break;
        }
    }

    Ok((blocks, table_sets))
}

// ─── Phase 2: Per-block Huffman decode (pure, parallelizable) ────────────────

/// Decode Huffman symbols from one block into a DecodedItem buffer.
///
/// This is a pure function — it only reads from `input` and `tables`, and
/// writes to `items`. No mutable shared state.
fn decode_block_symbols(
    input: &[u8],
    block: &BlockInfo,
    tables: &TableSet,
    extra_dist: bool,
    items: &mut Vec<DecodedItem>,
) -> RarResult<()> {
    // Create a BitReader positioned at this block's data.
    let byte_offset = block.data_bit_offset / 8;
    let bit_remainder = block.data_bit_offset % 8;
    let slice = &input[byte_offset..];
    let mut reader = BitReader::new(slice);
    if bit_remainder > 0 {
        reader.skip_bits(bit_remainder as u32)?;
    }

    let block_end_bits = bit_remainder + block.data_bits as usize;

    // Pending literal batch kept in locals: touching items.last_mut() per
    // literal costs a load + discriminant check per byte on the hottest path.
    // `lit_count` is the number of valid bytes (1-based); the stored item
    // count stays 0-indexed like unrar.
    let mut lit_bytes = [0u8; 8];
    let mut lit_count: usize = 0;

    while reader.position() < block_end_bits {
        if !reader.has_bits() {
            break;
        }

        let sym = tables.nc.decode_bitreader(&mut reader)? as u32;

        if sym < 256 {
            lit_bytes[lit_count] = sym as u8;
            lit_count += 1;
            if lit_count == 8 {
                items.push(DecodedItem::Literals {
                    bytes: lit_bytes,
                    count: 7,
                });
                lit_count = 0;
            }
            continue;
        }

        if lit_count > 0 {
            items.push(DecodedItem::Literals {
                bytes: lit_bytes,
                count: (lit_count - 1) as u8,
            });
            lit_count = 0;
        }

        if sym >= 262 {
            // Inline match — fully resolve length and distance from bitstream.
            let length_idx = (sym - 262) as usize;
            let length = slot_to_length(&mut reader, length_idx)?;
            let distance = decode_distance(&mut reader, &tables.dc, &tables.ldc, extra_dist)?;
            let length = adjust_length_for_distance(length, distance);
            items.push(DecodedItem::Match {
                length: length as u32,
                distance: distance as u64,
            });
            continue;
        }

        if sym == 256 {
            // Filter — read descriptor, store raw delta.
            let (filter_type, block_start_delta, block_length, channels) =
                read_filter_descriptor(&mut reader)?;
            items.push(DecodedItem::Filter {
                filter_type,
                block_start_delta,
                block_length,
                channels,
            });
            continue;
        }

        if sym == 257 {
            items.push(DecodedItem::RepeatPrev);
            continue;
        }

        // sym 258..=261: cache reference.
        let cache_idx = (sym - 258) as u8;
        let slot = tables.rc.decode_bitreader(&mut reader)? as usize;
        let length = slot_to_length(&mut reader, slot)?;
        items.push(DecodedItem::CacheRef {
            cache_idx,
            length: length as u32,
        });
    }

    if lit_count > 0 {
        items.push(DecodedItem::Literals {
            bytes: lit_bytes,
            count: (lit_count - 1) as u8,
        });
    }

    Ok(())
}

/// Read a filter descriptor from the bitstream (sym 256 handler).
/// Returns (filter_type_code, block_start_delta, block_length, channels).
fn read_filter_descriptor(reader: &mut BitReader) -> RarResult<(u8, u64, u32, u8)> {
    let block_start_delta = read_filter_data(reader)? as u64;
    let mut block_length = read_filter_data(reader)?;
    let filter_code = reader.read_bits(3)? as u8;

    if block_length > MAX_FILTER_BLOCK_SIZE {
        block_length = 0;
    }

    let channels = if filter_code == 0 {
        (reader.read_bits(5)? + 1) as u8
    } else {
        0
    };

    Ok((filter_code, block_start_delta, block_length, channels))
}

fn read_filter_data(reader: &mut BitReader) -> RarResult<u32> {
    let byte_count = reader.read_bits(2)? as usize + 1;
    let mut data = 0u32;
    for index in 0..byte_count {
        data |= reader.read_bits(8)? << (index * 8);
    }
    Ok(data)
}

// ─── Standalone decode helpers (no &self, usable from parallel context) ──────

fn slot_to_length(reader: &mut BitReader, slot: usize) -> RarResult<usize> {
    if slot >= NUM_LENGTH_SLOTS {
        return Err(RarError::CorruptArchive {
            detail: format!("length slot out of range: {slot}"),
        });
    }
    let (base, extra_bits) = if slot < 8 {
        (2 + slot, 0)
    } else {
        let lbits = slot / 4 - 1;
        (2 + ((4 | (slot & 3)) << lbits), lbits)
    };
    let extra_val = if extra_bits > 0 {
        reader.read_bits(extra_bits as u8)? as usize
    } else {
        0
    };
    Ok(base + extra_val)
}

fn decode_distance(
    reader: &mut BitReader,
    dc: &HuffmanTable,
    ldc: &HuffmanTable,
    extra_dist: bool,
) -> RarResult<usize> {
    let dist_code = dc.decode_bitreader(reader)? as usize;
    let max_dist_code = if extra_dist { 79 } else { 63 };
    if dist_code > max_dist_code {
        return Err(RarError::CorruptArchive {
            detail: format!("distance code out of range: {dist_code}"),
        });
    }

    if dist_code < 4 {
        return Ok(dist_code + 1);
    }

    let num_bits = (dist_code >> 1) - 1;
    let distance = if num_bits >= 4 {
        let high = if num_bits > 4 {
            reader.read_bits64((num_bits - 4) as u8)? << 4
        } else {
            0
        };
        let low = ldc.decode_bitreader(reader)? as u64;
        super::LzDecoder::distance_from_slot_parts(dist_code, num_bits, high, low, usize::BITS)?
    } else {
        let extra = reader.read_bits64(num_bits as u8)?;
        super::LzDecoder::distance_from_slot_parts(dist_code, num_bits, extra, 0, usize::BITS)?
    };

    Ok(distance)
}

fn adjust_length_for_distance(length: usize, distance: usize) -> usize {
    let mut len = length;
    if distance > 0x100 {
        len += 1;
    }
    if distance > 0x2000 {
        len += 1;
    }
    if distance > 0x40000 {
        len += 1;
    }
    len
}

// ─── Phase 3: Sequential item application ────────────────────────────────────

// ─── Phase 4: Parallel dispatch ──────────────────────────────────────────────

/// Decode a batch of (non-large) blocks in parallel using rayon.
fn decoded_item_buffers(
    buffers: &mut Vec<Vec<DecodedItem>>,
    active_len: usize,
) -> &mut [Vec<DecodedItem>] {
    if buffers.len() < active_len {
        buffers.resize_with(active_len, || Vec::with_capacity(DECODED_ITEMS_CAPACITY));
    }

    for buffer in buffers.iter_mut() {
        buffer.clear();
    }

    &mut buffers[..active_len]
}

/// Dedicated bounded pool for RAR block decode, mirroring unrar's
/// `MaxUserThreads = Min(Threads, 8)`. Keeps decode fan-out off the shared
/// global pool and bounds wake/park churn.
fn rar_decode_pool() -> Option<&'static rayon::ThreadPool> {
    use std::sync::OnceLock;

    static POOL: OnceLock<Option<rayon::ThreadPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(MAX_PARALLEL_THREADS);
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("weaver-unrar-dec-{i}"))
            .build()
            .ok()
    })
    .as_ref()
}

/// Decode a batch on whatever rayon context the caller is already in.
fn decode_batch_direct(
    input: &[u8],
    blocks: &[BlockInfo],
    table_sets: &[TableSet],
    extra_dist: bool,
    items: &mut [Vec<DecodedItem>],
) -> RarResult<()> {
    debug_assert_eq!(blocks.len(), items.len());
    blocks
        .par_iter()
        .zip(items.par_iter_mut())
        .try_for_each(|(block, items)| {
            let tables = &table_sets[block.table_set_index];
            decode_block_symbols(input, block, tables, extra_dist, items)
        })
}

fn parallel_decode_batch_into(
    input: &[u8],
    blocks: &[BlockInfo],
    table_sets: &[TableSet],
    extra_dist: bool,
    items: &mut [Vec<DecodedItem>],
) -> RarResult<()> {
    match rar_decode_pool() {
        Some(pool) => {
            pool.install(|| decode_batch_direct(input, blocks, table_sets, extra_dist, items))
        }
        None => decode_batch_direct(input, blocks, table_sets, extra_dist, items),
    }
}

/// A decoded-item buffer set moving through the decode/apply pipeline.
enum PipelineMsg {
    /// A decoded batch, in dispatch order.
    Decoded {
        set: Vec<Vec<DecodedItem>>,
        batch_len: usize,
        result: RarResult<()>,
    },
    /// An unused buffer set returned when the producer shuts down.
    Leftover { set: Vec<Vec<DecodedItem>> },
}

fn parallel_batch_size(thread_count: usize, block_count: usize) -> usize {
    let _ = thread_count;
    block_count.min(MAX_BATCH_BLOCKS)
}

fn next_small_block_batch_end(blocks: &[BlockInfo], start: usize, batch_size: usize) -> usize {
    let max_end = start.saturating_add(batch_size).min(blocks.len());
    let mut end = start;
    while end < max_end && !blocks[end].is_large {
        end += 1;
    }
    end
}

// ─── Public entry point ──────────────────────────────────────────────────────

impl LzDecoder {
    fn take_item_buffer_set(&mut self) -> Vec<Vec<DecodedItem>> {
        self.parallel_item_buffer_sets.pop().unwrap_or_default()
    }

    fn recycle_item_buffer_set(&mut self, set: Vec<Vec<DecodedItem>>) {
        if self.parallel_item_buffer_sets.len() < 2 {
            self.parallel_item_buffer_sets.push(set);
        }
    }

    fn decode_and_apply_parallel_batch<W: std::io::Write>(
        &mut self,
        input: &[u8],
        block_batch: &[BlockInfo],
        table_sets: &[TableSet],
        unpacked_size: u64,
        output_size: &mut u64,
        writer: &mut W,
    ) -> RarResult<()> {
        let mut set = self.take_item_buffer_set();
        let result = {
            let active_buffers = decoded_item_buffers(&mut set, block_batch.len());
            parallel_decode_batch_into(
                input,
                block_batch,
                table_sets,
                self.extra_dist,
                active_buffers,
            )
            .and_then(|()| {
                self.apply_decoded_items_parallel(
                    active_buffers,
                    unpacked_size,
                    output_size,
                    writer,
                )
            })
        };
        self.recycle_item_buffer_set(set);
        result
    }

    /// Decode and apply a run of consecutive small blocks with the decode and
    /// apply phases pipelined: a producer task on the decode pool fans each
    /// batch out via rayon into one of two recycled buffer sets, while this
    /// (calling) thread applies the previous batch's items to the window.
    /// Buffer-set ownership moves through channels, so the writer and decoder
    /// state never leave the calling thread.
    fn decode_and_apply_small_run<W: std::io::Write>(
        &mut self,
        input: &[u8],
        run: &[BlockInfo],
        table_sets: &[TableSet],
        unpacked_size: u64,
        output_size: &mut u64,
        writer: &mut W,
    ) -> RarResult<()> {
        let batch_size = parallel_batch_size(0, run.len());

        // A single batch has nothing to overlap with.
        if run.len() <= batch_size {
            return self.decode_and_apply_parallel_batch(
                input,
                run,
                table_sets,
                unpacked_size,
                output_size,
                writer,
            );
        }

        let Some(pool) = rar_decode_pool() else {
            for batch in run.chunks(batch_size) {
                self.decode_and_apply_parallel_batch(
                    input,
                    batch,
                    table_sets,
                    unpacked_size,
                    output_size,
                    writer,
                )?;
                if *output_size >= unpacked_size {
                    return Ok(());
                }
            }
            return Ok(());
        };

        let extra_dist = self.extra_dist;
        let batches: Vec<&[BlockInfo]> = run.chunks(batch_size).collect();
        let batch_count = batches.len();

        let (done_tx, done_rx) = std::sync::mpsc::channel::<PipelineMsg>();
        let (free_tx, free_rx) = std::sync::mpsc::channel::<Vec<Vec<DecodedItem>>>();
        // Prime the pipeline with two buffer sets: one decoding, one applying.
        for _ in 0..2 {
            let set = self.take_item_buffer_set();
            free_tx.send(set).expect("free receiver alive");
        }

        let mut run_result: RarResult<()> = Ok(());
        let batches_ref = &batches;

        pool.in_place_scope(|scope| {
            scope.spawn(move |_| {
                // Producer: decode batches in dispatch order, blocking on a
                // free buffer set so at most two batches are in flight.
                for batch in batches_ref {
                    let Ok(mut set) = free_rx.recv() else { break };
                    let active = decoded_item_buffers(&mut set, batch.len());
                    let result = decode_batch_direct(input, batch, table_sets, extra_dist, active);
                    let failed = result.is_err();
                    let sent = done_tx.send(PipelineMsg::Decoded {
                        set,
                        batch_len: batch.len(),
                        result,
                    });
                    if sent.is_err() || failed {
                        break;
                    }
                }
                // Hand any unused sets back for recycling.
                while let Ok(set) = free_rx.try_recv() {
                    let _ = done_tx.send(PipelineMsg::Leftover { set });
                }
            });

            // Consumer: apply each decoded batch in order on this thread.
            for _ in 0..batch_count {
                let msg = match done_rx.recv() {
                    Ok(msg) => msg,
                    Err(_) => break,
                };
                let (set, batch_len, result) = match msg {
                    PipelineMsg::Decoded {
                        set,
                        batch_len,
                        result,
                    } => (set, batch_len, result),
                    PipelineMsg::Leftover { set } => {
                        self.recycle_item_buffer_set(set);
                        break;
                    }
                };

                if let Err(error) = result {
                    self.recycle_item_buffer_set(set);
                    run_result = Err(error);
                    break;
                }

                let applied = self.apply_decoded_items_parallel(
                    &set[..batch_len],
                    unpacked_size,
                    output_size,
                    writer,
                );
                match free_tx.send(set) {
                    Ok(()) => {}
                    Err(returned) => self.recycle_item_buffer_set(returned.0),
                }
                if let Err(error) = applied {
                    run_result = Err(error);
                    break;
                }
                if *output_size >= unpacked_size {
                    break;
                }
            }

            // Shut the producer down and recover in-flight buffer sets. The
            // scope waits for the producer, whose sends fail once these ends
            // drop, so this cannot deadlock.
            drop(free_tx);
            while let Ok(msg) = done_rx.recv() {
                match msg {
                    PipelineMsg::Decoded { set, .. } | PipelineMsg::Leftover { set } => {
                        self.recycle_item_buffer_set(set);
                    }
                }
            }
        });

        run_result
    }

    pub(super) fn process_buffered_blocks<W: std::io::Write>(
        &mut self,
        input: &[u8],
        header_limit: usize,
        unpacked_size: u64,
        output_size: &mut u64,
        writer: &mut W,
    ) -> RarResult<usize> {
        let existing_tables = if let (Some(nc), Some(dc), Some(ldc), Some(rc)) = (
            &self.nc_table,
            &self.dc_table,
            &self.ldc_table,
            &self.rc_table,
        ) {
            Some(TableSet {
                nc: Arc::clone(nc),
                dc: Arc::clone(dc),
                ldc: Arc::clone(ldc),
                rc: Arc::clone(rc),
            })
        } else {
            None
        };

        let preparsed = preparse_complete_blocks(
            input,
            header_limit,
            &mut self.code_lengths,
            existing_tables.as_ref(),
            self.extra_dist,
        )?;
        if preparsed.blocks.is_empty() {
            return Ok(0);
        }

        let mut block_index = 0usize;
        while block_index < preparsed.blocks.len() {
            if preparsed.blocks[block_index].is_large {
                let block = &preparsed.blocks[block_index];
                let tables = &preparsed.table_sets[block.table_set_index];
                self.decode_preparsed_block_inline(
                    input,
                    block,
                    tables,
                    unpacked_size,
                    output_size,
                    writer,
                )?;
                block_index += 1;
            } else {
                // Whole run of consecutive small blocks up to the next large
                // block; the pipelined path batches internally.
                let run_end = next_small_block_batch_end(
                    &preparsed.blocks,
                    block_index,
                    preparsed.blocks.len(),
                );
                let run = &preparsed.blocks[block_index..run_end];
                if run.len() >= MIN_PARALLEL_BLOCKS && parallel_enabled() {
                    self.decode_and_apply_small_run(
                        input,
                        run,
                        &preparsed.table_sets,
                        unpacked_size,
                        output_size,
                        writer,
                    )?;
                } else {
                    for block in run {
                        let tables = &preparsed.table_sets[block.table_set_index];
                        self.decode_preparsed_block_inline(
                            input,
                            block,
                            tables,
                            unpacked_size,
                            output_size,
                            writer,
                        )?;
                    }
                }
                block_index = run_end;
            }

            if *output_size >= unpacked_size {
                break;
            }
        }

        if let Some(last_ts) = preparsed.table_sets.last() {
            self.nc_table = Some(Arc::clone(&last_ts.nc));
            self.dc_table = Some(Arc::clone(&last_ts.dc));
            self.ldc_table = Some(Arc::clone(&last_ts.ldc));
            self.rc_table = Some(Arc::clone(&last_ts.rc));
        }
        self.block_bits_remaining = 0;
        self.is_last_block = preparsed.saw_last_block;
        self.flush_filters_and_write(writer)?;

        Ok(preparsed.consumed_bytes)
    }

    fn decode_preparsed_block_inline<W: std::io::Write>(
        &mut self,
        input: &[u8],
        block: &BlockInfo,
        tables: &TableSet,
        unpacked_size: u64,
        output_size: &mut u64,
        writer: &mut W,
    ) -> RarResult<()> {
        self.nc_table = Some(Arc::clone(&tables.nc));
        self.dc_table = Some(Arc::clone(&tables.dc));
        self.ldc_table = Some(Arc::clone(&tables.ldc));
        self.rc_table = Some(Arc::clone(&tables.rc));
        self.block_bits_remaining = block.data_bits;

        let byte_offset = block.data_bit_offset / 8;
        let bit_remainder = block.data_bit_offset % 8;
        let slice = &input[byte_offset..];
        let mut reader = BitReader::new(slice);
        if bit_remainder > 0 {
            reader.skip_bits(bit_remainder as u32)?;
        }

        let flush_threshold = self.flush_threshold();
        while *output_size < unpacked_size && self.block_bits_remaining > 0 {
            *output_size = self.decode_block(
                &mut reader,
                unpacked_size,
                *output_size,
                Some(flush_threshold),
            )?;

            if self.pending_filters.is_empty() {
                if self.window.unflushed_bytes() as usize >= flush_threshold {
                    self.flush_unfiltered_stream_output(writer)?;
                }
            } else {
                self.flush_filters_and_write(writer)?;
                if self.window.unflushed_bytes() as usize > self.window.dict_size() {
                    return Err(RarError::CorruptArchive {
                        detail: "RAR5 pending filters exceeded dictionary window before flush"
                            .into(),
                    });
                }
            }
        }

        Ok(())
    }

    fn apply_decoded_items_parallel<W: std::io::Write>(
        &mut self,
        all_items: &[Vec<DecodedItem>],
        unpacked_size: u64,
        output_size: &mut u64,
        writer: &mut W,
    ) -> RarResult<()> {
        let flush_threshold = self.flush_threshold();
        let mut bytes_since_flush = 0usize;

        for block_items in all_items {
            for item in block_items {
                if *output_size >= unpacked_size {
                    return Ok(());
                }

                let mut produced = 0usize;
                let mut force_sync = false;

                match *item {
                    DecodedItem::Literals { bytes, count } => {
                        let n = (count as usize + 1).min((unpacked_size - *output_size) as usize);
                        self.window.put_literal_batch(&bytes, n);
                        *output_size += n as u64;
                        produced = n;
                    }
                    DecodedItem::Match { length, distance } => {
                        let remaining = (unpacked_size - *output_size) as usize;
                        let full_len = length as usize;
                        let len = full_len.min(remaining);

                        self.insert_old_dist(distance as usize);

                        self.last_length = full_len;
                        self.window
                            .copy_with_visible_len(distance as usize, full_len, len)?;
                        *output_size += len as u64;
                        produced = len;
                    }
                    DecodedItem::RepeatPrev => {
                        if self.last_length != 0 {
                            let distance = self.dist_cache[0];
                            let remaining = (unpacked_size - *output_size) as usize;
                            let len = self.last_length.min(remaining);
                            self.window
                                .copy_with_visible_len(distance, self.last_length, len)?;
                            *output_size += len as u64;
                            produced = len;
                        }
                    }
                    DecodedItem::CacheRef { cache_idx, length } => {
                        let idx = cache_idx as usize;
                        let distance = self.promote_old_dist(idx)?;

                        let remaining = (unpacked_size - *output_size) as usize;
                        let full_len = length as usize;
                        let len = full_len.min(remaining);
                        self.last_length = full_len;
                        self.window.copy_with_visible_len(distance, full_len, len)?;
                        *output_size += len as u64;
                        produced = len;
                    }
                    DecodedItem::Filter {
                        filter_type,
                        block_start_delta,
                        block_length,
                        channels,
                    } => {
                        let ft = FilterType::from_code(filter_type);
                        super::filter::push_pending_filter(
                            &mut self.pending_filters,
                            PendingFilter {
                                filter_type: ft,
                                block_start: self.current_file_base_total
                                    + *output_size
                                    + block_start_delta,
                                block_length: block_length as usize,
                                channels,
                            },
                            MAX_PENDING_FILTERS,
                        );

                        force_sync = true;
                    }
                }

                if force_sync || !self.pending_filters.is_empty() {
                    self.flush_stream_output(writer)?;
                    bytes_since_flush = 0;
                } else if produced != 0 {
                    bytes_since_flush += produced;
                    if bytes_since_flush >= flush_threshold {
                        self.flush_unfiltered_stream_output(writer)?;
                        bytes_since_flush = 0;
                    }
                }
            }

            if !self.pending_filters.is_empty() {
                self.flush_stream_output(writer)?;
                bytes_since_flush = 0;
            }
        }

        Ok(())
    }

    /// Attempt parallel decompression. Returns `None` if the input has too few
    /// blocks to benefit (caller should fall back to single-threaded).
    pub(super) fn try_decompress_parallel<W: std::io::Write>(
        &mut self,
        input: &[u8],
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<Option<u64>> {
        if !parallel_enabled() {
            return Ok(None);
        }

        // Phase 1: pre-parse block headers.
        let existing_tables = if let (Some(nc), Some(dc), Some(ldc), Some(rc)) = (
            &self.nc_table,
            &self.dc_table,
            &self.ldc_table,
            &self.rc_table,
        ) {
            Some(TableSet {
                nc: Arc::clone(nc),
                dc: Arc::clone(dc),
                ldc: Arc::clone(ldc),
                rc: Arc::clone(rc),
            })
        } else {
            None
        };

        let (blocks, table_sets) = preparse_blocks(
            input,
            &mut self.code_lengths,
            existing_tables.as_ref(),
            self.extra_dist,
        )?;

        if blocks.len() < MIN_PARALLEL_BLOCKS {
            return Ok(None); // Too few blocks — fall back to single-threaded.
        }

        let mut output_size = 0u64;
        let mut block_index = 0usize;
        while block_index < blocks.len() {
            if blocks[block_index].is_large {
                let block = &blocks[block_index];
                let tables = &table_sets[block.table_set_index];
                self.decode_preparsed_block_inline(
                    input,
                    block,
                    tables,
                    unpacked_size,
                    &mut output_size,
                    writer,
                )?;
                block_index += 1;
            } else {
                let run_end = next_small_block_batch_end(&blocks, block_index, blocks.len());
                let run = &blocks[block_index..run_end];
                self.decode_and_apply_small_run(
                    input,
                    run,
                    &table_sets,
                    unpacked_size,
                    &mut output_size,
                    writer,
                )?;
                block_index = run_end;
            }

            if output_size >= unpacked_size {
                break;
            }
        }

        // Update decoder state for solid archive continuity.
        if let Some(last_ts) = table_sets.last() {
            self.nc_table = Some(Arc::clone(&last_ts.nc));
            self.dc_table = Some(Arc::clone(&last_ts.dc));
            self.ldc_table = Some(Arc::clone(&last_ts.ldc));
            self.rc_table = Some(Arc::clone(&last_ts.rc));
        }
        self.block_bits_remaining = 0;
        // preparse_blocks stops on is_last, so if we consumed all blocks
        // the last one was the final block.
        self.is_last_block = true;

        // Apply pending filters and flush.
        self.flush_filters_and_write(writer)?;

        Ok(Some(output_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_batch_size_caps_dispatch_at_max_batch_blocks() {
        assert_eq!(parallel_batch_size(32, 128), MAX_BATCH_BLOCKS);
        assert_eq!(parallel_batch_size(64, 1024), MAX_BATCH_BLOCKS);
    }

    #[test]
    fn parallel_batch_size_is_thread_count_independent() {
        assert_eq!(parallel_batch_size(1, 8), 8);
        assert_eq!(parallel_batch_size(0, 8), 8);
    }

    #[test]
    fn parallel_batch_size_never_exceeds_available_blocks() {
        assert_eq!(parallel_batch_size(8, 3), 3);
        assert_eq!(parallel_batch_size(2, 2), 2);
    }

    #[test]
    fn parallel_distance_finalizer_uses_unrar_32bit_overflow_sentinel() {
        let distance = super::LzDecoder::distance_from_slot_parts(62, 30, 0, 0, 32).unwrap();

        assert_eq!(distance, u32::MAX as usize);
    }

    #[test]
    fn decoded_item_matches_unrar_item_size() {
        // unrar's UnpackDecodedItem is 16 bytes; the apply loop streams these,
        // so the u64 distance must not grow the item.
        assert_eq!(std::mem::size_of::<DecodedItem>(), 16);
    }

    #[test]
    fn decode_block_symbols_preserves_rar7_distance_beyond_u32() {
        // Uniform code lengths make canonical codes equal symbol indices:
        // NC len 9 (306 syms), DC len 7 (80 syms, RAR7 DCX), LDC len 4, RC len 6.
        let tables = TableSet {
            nc: Arc::new(HuffmanTable::build(&[9u8; 306]).unwrap()),
            dc: Arc::new(HuffmanTable::build(&[7u8; 80]).unwrap()),
            ldc: Arc::new(HuffmanTable::build(&[4u8; 16]).unwrap()),
            rc: Arc::new(HuffmanTable::build(&[6u8; 44]).unwrap()),
        };

        // Bitstream: NC sym 262 (9 bits) → length slot 0 (len 2, no extra),
        // DC sym 66 (7 bits) → 32 distance bits: 28 high bits = 0 from the
        // stream, low 4 bits from LDC sym 3. distance = (2<<32) + 3 + 1.
        // Packed MSB-first: 100000110 1000010 0^28 0011 = 48 bits.
        let input = [0x83u8, 0x42, 0x00, 0x00, 0x00, 0x03, 0, 0, 0, 0];
        let block = BlockInfo {
            data_bit_offset: 0,
            data_bits: 48,
            table_set_index: 0,
            is_large: false,
        };

        let mut items = Vec::new();
        decode_block_symbols(&input, &block, &tables, true, &mut items).unwrap();

        assert_eq!(items.len(), 1);
        let DecodedItem::Match { length, distance } = items[0] else {
            panic!("expected a match item");
        };
        let expected = (2u64 << 32) + 3 + 1;
        assert!(expected > u32::MAX as u64);
        assert_eq!(distance, expected);
        // Base length 2, +3 from the distance-based length adjustment.
        assert_eq!(length, 5);
    }

    #[test]
    fn parallel_enabled_defaults_on() {
        assert!(parallel_enabled_from_disable_env(None));
    }

    #[test]
    fn parallel_disable_env_turns_off_parallel_path() {
        assert!(!parallel_enabled_from_disable_env(Some(
            std::ffi::OsStr::new("1")
        )));
    }

    #[test]
    fn decoded_item_buffers_reuse_allocations() {
        let mut buffers = Vec::new();
        let first_ptr = {
            let active = decoded_item_buffers(&mut buffers, 2);
            active[0].push(DecodedItem::RepeatPrev);
            active[0].as_ptr()
        };

        let active = decoded_item_buffers(&mut buffers, 2);
        assert_eq!(active.len(), 2);
        assert!(active[0].is_empty());
        assert!(active[0].capacity() >= DECODED_ITEMS_CAPACITY);
        assert_eq!(active[0].as_ptr(), first_ptr);
    }

    #[test]
    fn parallel_apply_keeps_filter_until_future_bytes_arrive() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        let mut output_size = 0u64;
        let mut out = Vec::new();
        let all_items = vec![vec![
            DecodedItem::Filter {
                filter_type: 7,
                block_start_delta: 0,
                block_length: 1,
                channels: 0,
            },
            DecodedItem::Literals {
                bytes: [b'X', 0, 0, 0, 0, 0, 0, 0],
                count: 0,
            },
        ]];

        decoder
            .apply_decoded_items_parallel(&all_items, 1, &mut output_size, &mut out)
            .unwrap();

        assert_eq!(output_size, 1);
        assert!(out.is_empty());
        assert!(decoder.pending_filters.is_empty());
        assert_eq!(decoder.current_file_written_size, 1);
    }

    #[test]
    fn parallel_filter_offsets_include_current_file_base_total() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.current_file_base_total = 1_000;
        let mut output_size = 7u64;
        let mut out = Vec::new();
        let all_items = vec![vec![DecodedItem::Filter {
            filter_type: 1,
            block_start_delta: 5,
            block_length: 4,
            channels: 0,
        }]];

        decoder
            .apply_decoded_items_parallel(&all_items, 20, &mut output_size, &mut out)
            .unwrap();

        assert!(out.is_empty());
        assert_eq!(decoder.pending_filters.len(), 1);
        assert_eq!(
            decoder.pending_filters[0].block_start,
            decoder.current_file_base_total + output_size + 5
        );
    }

    #[test]
    fn next_small_block_batch_end_stops_before_large_block() {
        let blocks = vec![
            BlockInfo {
                data_bit_offset: 0,
                data_bits: 1,
                table_set_index: 0,
                is_large: false,
            },
            BlockInfo {
                data_bit_offset: 1,
                data_bits: 1,
                table_set_index: 0,
                is_large: false,
            },
            BlockInfo {
                data_bit_offset: 2,
                data_bits: 1,
                table_set_index: 0,
                is_large: true,
            },
            BlockInfo {
                data_bit_offset: 3,
                data_bits: 1,
                table_set_index: 0,
                is_large: false,
            },
        ];

        assert_eq!(next_small_block_batch_end(&blocks, 0, 4), 2);
        assert_eq!(next_small_block_batch_end(&blocks, 3, 4), 4);
    }
}
