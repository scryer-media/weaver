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

use super::bitstream::BitReader;
use super::filter::{FilterType, PendingFilter};
use super::huffman::{self, HuffmanTable};
use super::{LzDecoder, NUM_LENGTH_SLOTS};
use crate::error::{RarError, RarResult};

/// Minimum number of blocks to justify parallel dispatch.
/// Below this, rayon overhead exceeds the benefit.
const MIN_PARALLEL_BLOCKS: usize = 4;

/// Number of blocks to queue per worker in the parallel RAR5 path.
/// Matches unrar's `UNP_BLOCKS_PER_THREAD` batching strategy.
const BLOCKS_PER_THREAD: usize = 2;

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
struct TableSet {
    nc: HuffmanTable,
    dc: HuffmanTable,
    ldc: HuffmanTable,
    rc: HuffmanTable,
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
            let (nc, dc, ldc, rc) = huffman::read_tables(&mut reader, code_lengths)?;
            let bits_used = (reader.position() - pos_before) as i64;
            block_bits_remaining -= bits_used;
            table_sets.push(TableSet { nc, dc, ldc, rc });
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

        let sym = tables.nc.decode(&mut reader)? as u32;

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
        let slot = tables.rc.decode(&mut reader)? as usize;
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
    let dist_code = dc.decode(reader)? as usize;
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
            let low = ldc.decode(reader)? as usize;
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
            let mut items = Vec::with_capacity(16384);
            decode_block_symbols(input, block, tables, &mut items)?;
            Ok(items)
        })
        .collect::<RarResult<Vec<_>>>()
}

// ─── Public entry point ──────────────────────────────────────────────────────

impl LzDecoder {
    fn apply_decoded_items_parallel<W: std::io::Write>(
        &mut self,
        all_items: &[Vec<DecodedItem>],
        unpacked_size: u64,
        output_size: &mut u64,
        writer: &mut W,
    ) -> RarResult<()> {
        let flush_threshold = (self.window.dict_size() / 2).max(1);

        for block_items in all_items {
            for item in block_items {
                if *output_size >= unpacked_size {
                    return Ok(());
                }

                match *item {
                    DecodedItem::Literals { bytes, count } => {
                        let n = (count as usize + 1).min((unpacked_size - *output_size) as usize);
                        for b in &bytes[..n] {
                            self.window.put_byte(*b);
                        }
                        *output_size += n as u64;
                    }
                    DecodedItem::Match { length, distance } => {
                        let remaining = (unpacked_size - *output_size) as usize;
                        let len = (length as usize).min(remaining);

                        self.dist_cache[3] = self.dist_cache[2];
                        self.dist_cache[2] = self.dist_cache[1];
                        self.dist_cache[1] = self.dist_cache[0];
                        self.dist_cache[0] = distance as usize;

                        self.last_length = length as usize;
                        self.window.copy(distance as usize, len)?;
                        *output_size += len as u64;
                    }
                    DecodedItem::RepeatPrev => {
                        if self.last_length != 0 {
                            let distance = self.dist_cache[0];
                            let remaining = (unpacked_size - *output_size) as usize;
                            let len = self.last_length.min(remaining);
                            self.window.copy(distance, len)?;
                            *output_size += len as u64;
                        }
                    }
                    DecodedItem::CacheRef { cache_idx, length } => {
                        let idx = cache_idx as usize;
                        let distance = self.dist_cache[idx];
                        if distance == 0 {
                            return Err(RarError::CorruptArchive {
                                detail: "distance cache entry is zero".into(),
                            });
                        }

                        if idx > 0 {
                            let dist = self.dist_cache[idx];
                            for i in (1..=idx).rev() {
                                self.dist_cache[i] = self.dist_cache[i - 1];
                            }
                            self.dist_cache[0] = dist;
                        }

                        let remaining = (unpacked_size - *output_size) as usize;
                        let len = (length as usize).min(remaining);
                        self.last_length = length as usize;
                        self.window.copy(distance, len)?;
                        *output_size += len as u64;
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
                    }
                }

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
        if std::env::var_os("WEAVER_RAR_DISABLE_PARALLEL").is_some()
            || std::env::var_os("WEAVER_RAR_ENABLE_PARALLEL").is_none()
        {
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
                nc: nc.clone(),
                dc: dc.clone(),
                ldc: ldc.clone(),
                rc: rc.clone(),
            })
        } else {
            None
        };

        let (blocks, table_sets) =
            preparse_blocks(input, &mut self.code_lengths, existing_tables.as_ref())?;

        if blocks.len() < MIN_PARALLEL_BLOCKS {
            return Ok(None); // Too few blocks — fall back to single-threaded.
        }

        // Check for large blocks — if any exist, fall back entirely.
        // (A more sophisticated approach could mix parallel + inline, but
        // large blocks are rare in practice.)
        if blocks.iter().any(|b| b.is_large) {
            return Ok(None);
        }

        let batch_size = (rayon::current_num_threads() * BLOCKS_PER_THREAD)
            .max(MIN_PARALLEL_BLOCKS)
            .min(blocks.len());

        // Phase 2 + 3: decode a bounded batch in parallel, then apply it
        // incrementally before moving to the next batch. This keeps peak
        // memory bounded by a few blocks instead of the entire file.
        let mut output_size = 0u64;
        for block_batch in blocks.chunks(batch_size) {
            let all_items = parallel_decode_batch(input, block_batch, &table_sets)?;
            self.apply_decoded_items_parallel(&all_items, unpacked_size, &mut output_size, writer)?;
            if output_size >= unpacked_size {
                break;
            }
        }

        // Update decoder state for solid archive continuity.
        if let Some(last_ts) = table_sets.last() {
            self.nc_table = Some(last_ts.nc.clone());
            self.dc_table = Some(last_ts.dc.clone());
            self.ldc_table = Some(last_ts.ldc.clone());
            self.rc_table = Some(last_ts.rc.clone());
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
