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

use super::bitstream::BitReader;
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

/// Number of blocks to queue per worker in the parallel RAR5 path.
/// Matches unrar's `UNP_BLOCKS_PER_THREAD` batching strategy.
const BLOCKS_PER_THREAD: usize = 2;

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
    Match { length: u32, distance: u32 },
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
    std::env::var_os("WEAVER_RAR_DISABLE_PARALLEL").is_none()
        && std::env::var_os("WEAVER_RAR_ENABLE_PARALLEL").is_some()
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

    let mut block_bits_remaining = extra_bits + (block_bytes - 1) * 8;

    let table_set_index = if table_present || table_sets.is_empty() {
        let pos_before = reader.position();
            let (nc, dc, ldc, rc) = huffman::read_tables_bitreader(reader, code_lengths)?;
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
    code_lengths: &mut [u8],
    existing_tables: Option<&TableSet>,
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
        let checkpoint_code_lengths = working_code_lengths.clone();
        let checkpoint_table_set_len = table_sets.len();

        match parse_next_block(&mut reader, &mut working_code_lengths, &mut table_sets) {
            Ok(Some((block, is_last))) => {
                blocks.push(block);
                consumed_bytes = (reader.position() + 7) / 8;
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

        let mut block_bits_remaining = extra_bits + (block_bytes - 1) * 8;

        // Read tables if present — this advances the reader and consumes bits.
        let table_set_index = if table_present || table_sets.is_empty() {
            let pos_before = reader.position();
            let (nc, dc, ldc, rc) = huffman::read_tables_bitreader(&mut reader, code_lengths)?;
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

    while reader.position() < block_end_bits {
        if !reader.has_bits() {
            break;
        }

        let sym = tables.nc.decode_bitreader(&mut reader)? as u32;

        if sym < 256 {
            // Literal — try to batch with previous item.
            if let Some(DecodedItem::Literals { bytes, count }) = items.last_mut()
                && (*count as usize) < 7
            {
                *count += 1;
                bytes[*count as usize] = sym as u8;
                continue;
            }
            items.push(DecodedItem::Literals {
                bytes: {
                    let mut b = [0u8; 8];
                    b[0] = sym as u8;
                    b
                },
                count: 0, // 0 means 1 byte (count is 0-indexed like unrar)
            });
            continue;
        }

        if sym >= 262 {
            // Inline match — fully resolve length and distance from bitstream.
            let length_idx = (sym - 262) as usize;
            let length = slot_to_length(&mut reader, length_idx)?;
            let distance = decode_distance(&mut reader, &tables.dc, &tables.ldc)?;
            let length = adjust_length_for_distance(length, distance);
            items.push(DecodedItem::Match {
                length: length as u32,
                distance: distance as u32,
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

    Ok(())
}

/// Read a filter descriptor from the bitstream (sym 256 handler).
/// Returns (filter_type_code, block_start_delta, block_length, channels).
fn read_filter_descriptor(reader: &mut BitReader) -> RarResult<(u8, u64, u32, u8)> {
    let flags = reader.read_bits(8)?;
    let filter_code = (flags & 0x07) as u8;
    let use_counts = (flags >> 3) & 1;
    let block_start_bits = ((flags >> 4) & 0x0F) as u8;

    let _ = FilterType::from_code(filter_code).ok_or(RarError::UnsupportedFilter {
        filter_type: filter_code,
    })?;

    let block_start_delta = if block_start_bits > 0 {
        reader.read_bits(block_start_bits + 4)? as u64
    } else {
        0
    };

    let block_length_bits = reader.read_bits(4)? as u8;
    let block_length = if block_length_bits > 0 {
        reader.read_bits(block_length_bits + 4)?
    } else {
        0
    };

    if block_length > MAX_FILTER_BLOCK_SIZE {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "RAR5 filter block length {} exceeds limit {}",
                block_length, MAX_FILTER_BLOCK_SIZE
            ),
        });
    }

    let channels = if filter_code == 0 && use_counts != 0 {
        // DELTA filter with channel count
        (reader.read_bits(5)? + 1) as u8
    } else if filter_code == 0 {
        1
    } else {
        0
    };

    Ok((filter_code, block_start_delta, block_length, channels))
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
) -> RarResult<usize> {
    let dist_code = dc.decode_bitreader(reader)? as usize;
    if dist_code > 63 {
        return Err(RarError::CorruptArchive {
            detail: format!("distance code out of range: {dist_code}"),
        });
    }

    let distance = if dist_code < 4 {
        dist_code
    } else {
        let num_bits = (dist_code >> 1) - 1;
        let base = (2 | (dist_code & 1)) << num_bits;

        if num_bits >= 4 {
            let high = if num_bits > 4 {
                (reader.read_bits((num_bits - 4) as u8)? as usize) << 4
            } else {
                0
            };
            let low = ldc.decode_bitreader(reader)? as usize;
            base + high + low
        } else {
            base + reader.read_bits(num_bits as u8)? as usize
        }
    };

    Ok(distance + 1)
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
fn parallel_decode_batch(
    input: &[u8],
    blocks: &[BlockInfo],
    table_sets: &[TableSet],
) -> RarResult<Vec<Vec<DecodedItem>>> {
    blocks
        .par_iter()
        .map(|block| {
            let tables = &table_sets[block.table_set_index];
            let mut items = Vec::with_capacity(DECODED_ITEMS_CAPACITY);
            decode_block_symbols(input, block, tables, &mut items)?;
            Ok(items)
        })
        .collect::<RarResult<Vec<_>>>()
}

    fn parallel_batch_size(thread_count: usize, block_count: usize) -> usize {
        thread_count
        .clamp(1, MAX_PARALLEL_THREADS)
        .saturating_mul(BLOCKS_PER_THREAD)
        .max(MIN_PARALLEL_BLOCKS)
        .min(block_count)
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
    pub(super) fn process_buffered_blocks<W: std::io::Write>(
        &mut self,
        input: &[u8],
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

        let preparsed = preparse_complete_blocks(input, &mut self.code_lengths, existing_tables.as_ref())?;
        if preparsed.blocks.is_empty() {
            return Ok(0);
        }

        let batch_size = parallel_batch_size(rayon::current_num_threads(), preparsed.blocks.len());
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
                continue;
            }

            let batch_end = next_small_block_batch_end(&preparsed.blocks, block_index, batch_size);
            let block_batch = &preparsed.blocks[block_index..batch_end];
            if block_batch.len() >= MIN_PARALLEL_BLOCKS && parallel_enabled() {
                let all_items = parallel_decode_batch(input, block_batch, &preparsed.table_sets)?;
                self.apply_decoded_items_parallel(&all_items, unpacked_size, output_size, writer)?;
            } else {
                for block in block_batch {
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
            block_index = batch_end;

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
                    self.window.flush_to_writer(writer).map_err(RarError::Io)?;
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
                        self.window.put_bytes(&bytes[..n]);
                        *output_size += n as u64;
                        produced = n;
                    }
                    DecodedItem::Match { length, distance } => {
                        let remaining = (unpacked_size - *output_size) as usize;
                        let len = (length as usize).min(remaining);

                        self.insert_old_dist(distance as usize);

                        self.last_length = length as usize;
                        self.window.copy(distance as usize, len)?;
                        *output_size += len as u64;
                        produced = len;
                    }
                    DecodedItem::RepeatPrev => {
                        if self.last_length != 0 {
                            let distance = self.dist_cache[0];
                            let remaining = (unpacked_size - *output_size) as usize;
                            let len = self.last_length.min(remaining);
                            self.window.copy(distance, len)?;
                            *output_size += len as u64;
                            produced = len;
                        }
                    }
                    DecodedItem::CacheRef { cache_idx, length } => {
                        let idx = cache_idx as usize;
                        let distance = self.promote_old_dist(idx)?;

                        let remaining = (unpacked_size - *output_size) as usize;
                        let len = (length as usize).min(remaining);
                        self.last_length = length as usize;
                        self.window.copy(distance, len)?;
                        *output_size += len as u64;
                        produced = len;
                    }
                    DecodedItem::Filter {
                        filter_type,
                        block_start_delta,
                        block_length,
                        channels,
                    } => {
                        let ft = FilterType::from_code(filter_type)
                            .ok_or(RarError::UnsupportedFilter { filter_type })?;
                        self.pending_filters.push(PendingFilter {
                            filter_type: ft,
                            block_start: *output_size + block_start_delta,
                            block_length: block_length as usize,
                            channels,
                        });

                        if self.pending_filters.len() > MAX_PENDING_FILTERS {
                            return Err(RarError::CorruptArchive {
                                detail: format!(
                                    "RAR5 pending filter count {} exceeds limit {}",
                                    self.pending_filters.len(),
                                    MAX_PENDING_FILTERS
                                ),
                            });
                        }

                        force_sync = true;
                    }
                }

                if force_sync || !self.pending_filters.is_empty() {
                    self.flush_stream_output(writer)?;
                    bytes_since_flush = 0;
                } else if produced != 0 {
                    bytes_since_flush += produced;
                    if bytes_since_flush >= flush_threshold {
                        self.window.flush_to_writer(writer).map_err(RarError::Io)?;
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

        let (blocks, table_sets) =
            preparse_blocks(input, &mut self.code_lengths, existing_tables.as_ref())?;

        if blocks.len() < MIN_PARALLEL_BLOCKS {
            return Ok(None); // Too few blocks — fall back to single-threaded.
        }

        let batch_size = parallel_batch_size(rayon::current_num_threads(), blocks.len());

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
                let batch_end = next_small_block_batch_end(&blocks, block_index, batch_size);
                let block_batch = &blocks[block_index..batch_end];
                let all_items = parallel_decode_batch(input, block_batch, &table_sets)?;
                self.apply_decoded_items_parallel(
                    &all_items,
                    unpacked_size,
                    &mut output_size,
                    writer,
                )?;
                block_index = batch_end;
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
    fn parallel_batch_size_respects_unrar_thread_cap() {
        assert_eq!(parallel_batch_size(32, 128), 16);
        assert_eq!(parallel_batch_size(64, 128), 16);
    }

    #[test]
    fn parallel_batch_size_preserves_minimum_batch_floor() {
        assert_eq!(parallel_batch_size(1, 8), 4);
        assert_eq!(parallel_batch_size(0, 8), 4);
    }

    #[test]
    fn parallel_batch_size_never_exceeds_available_blocks() {
        assert_eq!(parallel_batch_size(8, 3), 3);
        assert_eq!(parallel_batch_size(2, 2), 2);
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
