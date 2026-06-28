//! LZ block decompression orchestrator for RAR5.
//!
//! RAR5 compressed data is organized into blocks, each with a byte-aligned
//! header followed by Huffman-encoded LZ data. Unlike RAR3, RAR5 uses only
//! LZ+Huffman compression (no PPMd blocks).
//!
//! Block header (byte-aligned):
//! - `flags` (1 byte): bit_size[0:2], byte_count[3:4], is_last[6], table_present[7]
//! - `checksum` (1 byte): XOR of flags and all size bytes, must equal 0x5A
//! - `block_size_bytes` (1-3 bytes, LE): high part of block size
//! - Extra bits from bitstream: low part of block size (bit_size+1 bits)
//!
//! Symbol interpretation (NC table, 306 symbols):
//! - 0-255: literal bytes
//! - 256: filter marker
//! - 257: repeat previous match (same length, same distance[0])
//! - 258-261: repeat distance cache references (length from RC table)
//! - 262-305: inline length codes with extra bits (distance from DC/LDC tables)

pub mod bitstream;
pub mod filter;
pub mod huffman;
mod parallel;
pub mod window;

use std::io::Write;
use std::sync::Arc;

use tracing::trace;

use crate::error::{RarError, RarResult};
use crate::limits::{Limits, UNRAR_MIN_LZ_WINDOW_SIZE, UNRAR_UNPACK_MAX_DICT_SIZE};
use crate::types::CompressionInfo;
use bitstream::{BitRead, BitReader, StreamingBitReader};
use filter::{FilterType, PendingFilter};
use huffman::HuffmanTable;
use window::Window;

/// Maximum number of length slots.
const NUM_LENGTH_SLOTS: usize = 44;

/// Number of entries in the last-distance cache.
const DIST_CACHE_SIZE: usize = 4;

/// Maximum number of pending filters to hold before treating the archive as invalid.
/// Mirrors unrar's defensive `MAX_UNPACK_FILTERS` bound.
const MAX_PENDING_FILTERS: usize = 8192;

/// Maximum filter block size accepted from the bitstream.
/// Mirrors unrar's `MAX_FILTER_BLOCK_SIZE` bound.
const MAX_FILTER_BLOCK_SIZE: usize = 0x400000;

/// Maximum number of bytes to accumulate before flushing decoded output.
/// Mirrors unrar's `UNPACK_MAX_WRITE` write border.
const UNPACK_MAX_WRITE: usize = 0x400000;
const STREAMING_PARALLEL_READ_BUFFER_SIZE: usize = 0x400000;
const STREAMING_PARALLEL_MIN_PROCESS_SIZE: usize = 1024;

/// State for the LZ decompressor.
pub struct LzDecoder {
    /// Sliding window / ring buffer.
    window: Window,
    /// Last-distance cache (4 entries for repeat matches).
    dist_cache: [usize; DIST_CACHE_SIZE],
    /// Length of the last match (for symbol 257 repeat).
    last_length: usize,
    /// Current Huffman tables (kept across blocks when table_present is false).
    nc_table: Option<Arc<HuffmanTable>>,
    dc_table: Option<Arc<HuffmanTable>>,
    ldc_table: Option<Arc<HuffmanTable>>,
    rc_table: Option<Arc<HuffmanTable>>,
    /// Persistent code lengths for delta encoding across blocks.
    code_lengths: Vec<u8>,
    /// RAR7 uses larger distance tables and longer high-distance reads.
    extra_dist: bool,
    /// Number of compressed bits remaining in the current block.
    /// When 0, a new block header must be read.
    block_bits_remaining: i64,
    /// Whether the current block is the last one.
    is_last_block: bool,
    /// Filters pending application to ranges of the current output.
    pending_filters: Vec<PendingFilter>,
    /// Absolute output offset where the current file begins in the window.
    current_file_base_total: u64,
    /// Logical UnRAR `WrittenFileSize` for the current file after filters.
    ///
    /// This advances by filter block length even when unsupported filters
    /// suppress the corresponding output bytes.
    current_file_written_size: u64,
    /// Reusable decoded-item buffers for RAR5 multithreaded block decode.
    parallel_item_buffers: Vec<Vec<parallel::DecodedItem>>,
}

impl LzDecoder {
    /// Create a new LZ decoder with the specified dictionary size.
    pub fn new(dict_size: usize, version: u8) -> Self {
        Self::try_new(dict_size, version).expect("RAR5 LZ decoder allocation failed")
    }

    /// Fallibly create a new LZ decoder with the specified dictionary size.
    pub fn try_new(dict_size: usize, version: u8) -> RarResult<Self> {
        let extra_dist = match version {
            0 => false,
            1 => true,
            _ => {
                return Err(RarError::UnsupportedCompression { method: 0, version });
            }
        };
        let total_symbols = huffman::HUFF_NC
            + if extra_dist {
                huffman::HUFF_DCX
            } else {
                huffman::HUFF_DCB
            }
            + huffman::HUFF_LDC
            + huffman::HUFF_RC;
        Ok(Self {
            window: Window::try_new(dict_size)?,
            dist_cache: [usize::MAX; DIST_CACHE_SIZE],
            last_length: 0,
            nc_table: None,
            dc_table: None,
            ldc_table: None,
            rc_table: None,
            code_lengths: vec![0u8; total_symbols],
            extra_dist,
            block_bits_remaining: 0,
            is_last_block: false,
            pending_filters: Vec::new(),
            current_file_base_total: 0,
            current_file_written_size: 0,
            parallel_item_buffers: Vec::new(),
        })
    }

    fn flush_threshold(&self) -> usize {
        self.window.dict_size().clamp(1, UNPACK_MAX_WRITE)
    }

    fn begin_file_decode(&mut self) {
        self.pending_filters.clear();
        self.current_file_base_total = self.window.total_written();
        self.current_file_written_size = 0;
        self.window.mark_flushed(self.current_file_base_total);
    }

    #[inline]
    pub(super) fn insert_old_dist(&mut self, distance: usize) {
        self.dist_cache[3] = self.dist_cache[2];
        self.dist_cache[2] = self.dist_cache[1];
        self.dist_cache[1] = self.dist_cache[0];
        self.dist_cache[0] = distance;
    }

    #[inline]
    pub(super) fn promote_old_dist(&mut self, cache_idx: usize) -> RarResult<usize> {
        let distance = self.dist_cache[cache_idx];

        if cache_idx > 0 {
            for index in (1..=cache_idx).rev() {
                self.dist_cache[index] = self.dist_cache[index - 1];
            }
            self.dist_cache[0] = distance;
        }

        Ok(distance)
    }

    fn advance_staged_prefix(staged_start: &mut usize, staged_len: usize, bit_offset: &mut usize) {
        *staged_start += *bit_offset / 8;
        *bit_offset %= 8;
        if *staged_start >= staged_len {
            *staged_start = 0;
            *bit_offset = 0;
        }
    }

    fn compact_staged_buffer(staged: &mut Vec<u8>, staged_start: &mut usize) {
        if *staged_start == 0 {
            return;
        }

        if *staged_start < staged.len() {
            staged.copy_within(*staged_start.., 0);
            staged.truncate(staged.len() - *staged_start);
        } else {
            staged.clear();
        }
        *staged_start = 0;
    }

    fn flush_unfiltered_stream_output<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
    ) -> RarResult<()> {
        let logical_advance = self
            .window
            .total_written()
            .saturating_sub(self.window.total_flushed());
        self.window.flush_to_writer(writer).map_err(RarError::Io)?;
        self.current_file_written_size += logical_advance;
        Ok(())
    }

    fn flush_stream_output<W: Write>(&mut self, writer: &mut W) -> RarResult<()> {
        if self.pending_filters.is_empty() {
            self.flush_unfiltered_stream_output(writer)?;
        } else {
            self.flush_filters_and_write(writer)?;
            if self.window.unflushed_bytes() as usize > self.window.dict_size() {
                return Err(RarError::CorruptArchive {
                    detail: "RAR5 pending filters exceeded dictionary window before flush".into(),
                });
            }
        }

        Ok(())
    }

    fn try_read_block_header_buffered(&mut self, reader: &mut BitReader<'_>) -> RarResult<bool> {
        let reader_checkpoint = reader.clone();
        let code_lengths_checkpoint = self.code_lengths.clone();
        let nc_table_checkpoint = self.nc_table.clone();
        let dc_table_checkpoint = self.dc_table.clone();
        let ldc_table_checkpoint = self.ldc_table.clone();
        let rc_table_checkpoint = self.rc_table.clone();
        let block_bits_remaining_checkpoint = self.block_bits_remaining;
        let is_last_block_checkpoint = self.is_last_block;

        match self.read_block_header_bitreader(reader) {
            Ok(()) => Ok(true),
            Err(error) if parallel::is_truncated_input_error(&error) => {
                *reader = reader_checkpoint;
                self.code_lengths = code_lengths_checkpoint;
                self.nc_table = nc_table_checkpoint;
                self.dc_table = dc_table_checkpoint;
                self.ldc_table = ldc_table_checkpoint;
                self.rc_table = rc_table_checkpoint;
                self.block_bits_remaining = block_bits_remaining_checkpoint;
                self.is_last_block = is_last_block_checkpoint;
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    fn read_block_header_bitreader(&mut self, reader: &mut BitReader<'_>) -> RarResult<()> {
        reader.align_byte();

        let flags = reader.read_bits(8)? as u8;
        let checksum = reader.read_bits(8)? as u8;

        let extra_bits = (flags & 0x07) as i64 + 1;
        let num_size_bytes = ((flags >> 3) & 0x03) + 1;
        if num_size_bytes > 3 {
            return Err(RarError::CorruptArchive {
                detail: "RAR5 block header: invalid size byte count".into(),
            });
        }

        self.is_last_block = (flags & 0x40) != 0;
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

        self.block_bits_remaining = if block_bytes == 0 {
            0
        } else {
            extra_bits + (block_bytes - 1) * 8
        };

        if !table_present && self.nc_table.is_none() {
            return Err(RarError::CorruptArchive {
                detail: "RAR5 first block is missing Huffman tables".into(),
            });
        }

        if table_present {
            let pos_before = reader.position();
            let (nc, dc, ldc, rc) =
                huffman::read_tables_bitreader(reader, &mut self.code_lengths, self.extra_dist)?;
            let bits_used = (reader.position() - pos_before) as i64;
            self.block_bits_remaining -= bits_used;
            self.nc_table = Some(Arc::new(nc));
            self.dc_table = Some(Arc::new(dc));
            self.ldc_table = Some(Arc::new(ldc));
            self.rc_table = Some(Arc::new(rc));
        }

        Ok(())
    }

    /// Read a RAR5 block header.
    ///
    /// The block header is byte-aligned:
    /// - flags (1 byte): bit_size[0:2], byte_count[3:4], is_last[6], table_present[7]
    /// - checksum (1 byte): must equal 0x5A ^ flags ^ size_byte_0 ^ ...
    /// - size bytes (1-3 bytes, LE): block byte count
    ///
    /// Block size in bits = byte_count * 8 + ((flags & 7) + 1).
    /// byte_count is full data bytes; the low 3 flag bits + 1 give additional valid bits.
    fn read_block_header<R: BitRead>(&mut self, reader: &mut R) -> RarResult<()> {
        // Block header is byte-aligned.
        reader.align_byte()?;

        let flags = reader.read_bits(8)? as u8;
        let checksum = reader.read_bits(8)? as u8;

        let extra_bits = (flags & 0x07) as i64 + 1; // valid bits in last byte (1-8)
        let num_size_bytes = ((flags >> 3) & 0x03) + 1;
        if num_size_bytes > 3 {
            return Err(RarError::CorruptArchive {
                detail: "RAR5 block header: invalid size byte count".into(),
            });
        }

        self.is_last_block = (flags & 0x40) != 0;
        let table_present = (flags & 0x80) != 0;

        // Read block size bytes (little-endian) and validate checksum.
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

        // Block size in bits = (block_bytes - 1) * 8 + extra_bits.
        // The block_bytes value includes the last partial byte; extra_bits gives
        // how many bits are valid in that last byte (1-8).
        self.block_bits_remaining = if block_bytes == 0 {
            0
        } else {
            extra_bits + (block_bytes - 1) * 8
        };

        if !table_present && self.nc_table.is_none() {
            return Err(RarError::CorruptArchive {
                detail: "RAR5 first block is missing Huffman tables".into(),
            });
        }

        if table_present {
            let pos_before = reader.position();
            let (nc, dc, ldc, rc) =
                huffman::read_tables(reader, &mut self.code_lengths, self.extra_dist)?;
            let bits_used = (reader.position() - pos_before) as i64;
            self.block_bits_remaining -= bits_used;
            self.nc_table = Some(Arc::new(nc));
            self.dc_table = Some(Arc::new(dc));
            self.ldc_table = Some(Arc::new(ldc));
            self.rc_table = Some(Arc::new(rc));
        }

        Ok(())
    }

    /// Decompress all LZ-compressed data from the input into the output buffer.
    ///
    /// `input` is the raw compressed data (data area from the file header).
    /// `unpacked_size` is the expected uncompressed size.
    /// Returns the decompressed data.
    pub fn decompress(&mut self, input: &[u8], unpacked_size: u64) -> RarResult<Vec<u8>> {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        let capacity = usize::try_from(unpacked_size).unwrap_or_else(|_| self.window.dict_size());
        let capacity = capacity.min(self.window.dict_size());
        let mut output = Vec::with_capacity(capacity);
        self.decompress_to_writer(input, unpacked_size, &mut output)?;
        Ok(output)
    }

    #[cfg(test)]
    /// Apply pending filters to an in-memory output buffer using UnRAR's write
    /// semantics: invalid filters suppress their covered block, but the logical
    /// `WrittenFileSize` used for later filters still advances by block length.
    fn apply_filters_to_vec(&mut self, output: Vec<u8>, base_offset: u64) -> Vec<u8> {
        if self.pending_filters.is_empty() {
            return output;
        }

        let total = base_offset + output.len() as u64;
        let mut filtered = Vec::with_capacity(output.len());
        let mut written_up_to = base_offset;
        let mut logical_written_size = 0u64;
        let mut pending_filters = std::mem::take(&mut self.pending_filters);
        pending_filters.sort_by_key(|filter| filter.block_start);

        for f in pending_filters {
            if f.block_start < written_up_to || f.block_start > total {
                continue;
            }
            if f.block_start > written_up_to {
                let rel_start = (written_up_to - base_offset) as usize;
                let rel_end = (f.block_start - base_offset) as usize;
                filtered.extend_from_slice(&output[rel_start..rel_end]);
                logical_written_size += f.block_start - written_up_to;
                written_up_to = f.block_start;
            }

            let block_end = f.block_start.saturating_add(f.block_length as u64);
            if block_end > total {
                continue;
            }

            let rel_start = (f.block_start - base_offset) as usize;
            let rel_end = (block_end - base_offset) as usize;
            let mut block = output[rel_start..rel_end].to_vec();
            let file_block_start = logical_written_size;
            match f.filter_type {
                FilterType::Delta => filter::apply_delta(&mut block, f.channels),
                FilterType::E8 => filter::apply_e8(&mut block, file_block_start),
                FilterType::E8E9 => filter::apply_e8e9(&mut block, file_block_start),
                FilterType::Arm => filter::apply_arm(&mut block, file_block_start),
                FilterType::Unsupported(_) => {}
            }
            if f.filter_type.emits_output() {
                filtered.extend_from_slice(&block);
            }
            logical_written_size += f.block_length as u64;
            written_up_to = block_end;
        }

        if written_up_to < total {
            let rel_start = (written_up_to - base_offset) as usize;
            filtered.extend_from_slice(&output[rel_start..]);
            logical_written_size += total - written_up_to;
        }

        self.pending_filters.clear();
        self.current_file_written_size = logical_written_size;
        filtered
    }

    /// Apply distance-based length adjustment per RAR5 spec.
    ///
    /// Longer distances get +1 to the match length at each threshold:
    /// - distance > 256 (0x100): +1
    /// - distance > 8192 (0x2000): +1
    /// - distance > 262144 (0x40000): +1
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

    /// Decode symbols from one LZ block.
    ///
    /// Returns the updated output_size.
    ///
    /// Hot loop optimized to match unrar's Unpack5:
    /// - Precompute block end position — compare against it instead of
    ///   decrementing block_bits_remaining per symbol
    /// - No range checks — ordered `if/else if` with simple comparisons
    ///   matching unrar's frequency order (literal first, match >= 262 second)
    /// - `has_bits()` instead of `bits_remaining() < 1`
    fn decode_block<R: BitRead>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
        mut output_size: u64,
        yield_threshold: Option<usize>,
    ) -> RarResult<u64> {
        // Precompute the bitstream position at which this block ends.
        // This replaces per-symbol block_bits_remaining decrement with a
        // single comparison per iteration (matching unrar's ReadBorder check).
        let block_end_pos = reader.position() as i64 + self.block_bits_remaining;

        while output_size < unpacked_size && (reader.position() as i64) < block_end_pos {
            if !reader.has_bits() {
                break;
            }

            let nc_table = self
                .nc_table
                .as_ref()
                .ok_or_else(|| RarError::CorruptArchive {
                    detail: "RAR5 LZ block is missing literal/length table".into(),
                })?;
            let sym = nc_table.decode(reader)? as u32;
            let mut should_check_yield = false;

            if sym < 256 {
                // Literal byte (most common case — first).
                self.window.put_byte(sym as u8);
                output_size += 1;
                should_check_yield = true;
            } else if sym >= 262 {
                // Inline length-distance pair (second most common).
                let length_idx = (sym - 262) as usize;
                let mut length = Self::slot_to_length(reader, length_idx)?;
                let dc = self
                    .dc_table
                    .as_ref()
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: "RAR5 LZ block is missing distance table".into(),
                    })?;
                let ldc = self
                    .ldc_table
                    .as_ref()
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: "RAR5 LZ block is missing low-distance table".into(),
                    })?;
                let distance = self.decode_distance(reader, dc, ldc)?;
                length = Self::adjust_length_for_distance(length, distance);
                let remaining = (unpacked_size - output_size) as usize;
                let visible_len = length.min(remaining);

                self.insert_old_dist(distance);

                self.last_length = length;
                self.window
                    .copy_with_visible_len(distance, length, visible_len)?;
                output_size += visible_len as u64;
                should_check_yield = true;
            } else if sym == 256 {
                // Filter marker.
                output_size = self.handle_filter(reader, output_size)?;
            } else if sym == 257 {
                // Repeat previous match.
                if self.last_length != 0 {
                    let distance = self.dist_cache[0];
                    let length = self.last_length;
                    let remaining = (unpacked_size - output_size) as usize;
                    let visible_len = length.min(remaining);
                    self.window
                        .copy_with_visible_len(distance, length, visible_len)?;
                    output_size += visible_len as u64;
                    should_check_yield = true;
                }
            } else {
                // sym 258..=261: repeat distance from cache.
                let cache_idx = (sym - 258) as usize;
                let distance = self.promote_old_dist(cache_idx)?;

                let rc = self
                    .rc_table
                    .as_ref()
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: "RAR5 LZ block is missing repeat-length table".into(),
                    })?;
                let length = self.decode_rc_length(reader, rc)?;
                let remaining = (unpacked_size - output_size) as usize;
                let visible_len = length.min(remaining);

                self.last_length = length;
                self.window
                    .copy_with_visible_len(distance, length, visible_len)?;
                output_size += visible_len as u64;
                should_check_yield = true;
            }

            if should_check_yield
                && let Some(threshold) = yield_threshold
                && self.pending_filters.is_empty()
                && self.window.unflushed_bytes() as usize >= threshold
            {
                break;
            }
        }

        // Update block_bits_remaining from final position.
        self.block_bits_remaining = block_end_pos - reader.position() as i64;

        Ok(output_size)
    }

    /// Convert a length slot (0-43) to a match length.
    ///
    /// Uses the same formula as unrar's SlotToLength:
    /// - Slots 0-7: length = 2 + slot, no extra bits
    /// - Slots 8+:  extra_bits = slot/4 - 1
    ///   length = 2 + (4 | (slot & 3)) << extra_bits + read_bits(extra_bits)
    fn slot_to_length<R: BitRead>(reader: &mut R, slot: usize) -> RarResult<usize> {
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

    /// Decode a length from the RC/LenDecoder table (used for symbols 256 and 258-261).
    fn decode_rc_length<R: BitRead>(&self, reader: &mut R, rc: &HuffmanTable) -> RarResult<usize> {
        let slot = rc.decode(reader)? as usize;
        Self::slot_to_length(reader, slot)
    }

    /// Decode a distance value from the DC and LDC (AlignDecoder) tables.
    ///
    /// RAR5 distance decoding:
    /// - dist_code < 4: distance = dist_code + 1
    /// - dist_code >= 4: base + extra bits, where extra bits may be split
    ///   between the bitstream (high) and AlignDecoder/LDC (low 4 bits)
    fn decode_distance<R: BitRead>(
        &self,
        reader: &mut R,
        dc: &HuffmanTable,
        ldc: &HuffmanTable,
    ) -> RarResult<usize> {
        let dist_code = dc.decode(reader)? as usize;
        let max_dist_code = if self.extra_dist { 79 } else { 63 };
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
            // Split: high bits from bitstream, low 4 bits from AlignDecoder (LDC).
            let high = if num_bits > 4 {
                reader.read_bits64((num_bits - 4) as u8)? << 4
            } else {
                0
            };
            let low = ldc.decode(reader)? as u64;
            Self::distance_from_slot_parts(dist_code, num_bits, high, low, usize::BITS)?
        } else {
            // All extra bits from bitstream.
            let extra = reader.read_bits64(num_bits as u8)?;
            Self::distance_from_slot_parts(dist_code, num_bits, extra, 0, usize::BITS)?
        };

        Ok(distance)
    }

    fn distance_from_slot_parts(
        dist_code: usize,
        num_bits: usize,
        high_or_extra: u64,
        low: u64,
        pointer_bits: u32,
    ) -> RarResult<usize> {
        // Current UnRAR maps distances that would overflow 32-bit size_t to
        // size_t(-1), so CopyString treats them as invalid and zero-fills.
        if pointer_bits <= 32 && num_bits >= 30 {
            return Ok(u32::MAX as usize);
        }

        let base = (2u64 | (dist_code as u64 & 1))
            .checked_shl(num_bits as u32)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("RAR5 distance shift {num_bits} overflows"),
            })?;
        let distance = base
            .checked_add(high_or_extra)
            .and_then(|value| value.checked_add(low))
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR5 distance overflows u64".into(),
            })?;
        usize::try_from(distance).map_err(|_| RarError::CorruptArchive {
            detail: format!("RAR5 distance {distance} does not fit in usize"),
        })
    }

    /// Handle a filter marker (symbol 256).
    ///
    /// Reads the full filter descriptor from the bitstream and pushes a
    /// [`PendingFilter`] for later application to the output.
    fn handle_filter<R: BitRead>(&mut self, reader: &mut R, output_size: u64) -> RarResult<u64> {
        let block_start_delta = Self::read_filter_data(reader)? as u64;
        let block_start = self.current_file_base_total + output_size + block_start_delta;
        let mut block_length = Self::read_filter_data(reader)? as usize;
        let filter_code = reader.read_bits(3)? as u8;
        let filter_type = FilterType::from_code(filter_code);

        if block_length > MAX_FILTER_BLOCK_SIZE {
            block_length = 0;
        }

        let channels = if filter_type == FilterType::Delta {
            (reader.read_bits(5)? + 1) as u8
        } else {
            0
        };

        trace!(
            "filter at output offset {}: type={:?}, block_start={}, block_length={}, channels={}",
            output_size, filter_type, block_start, block_length, channels
        );

        filter::push_pending_filter(
            &mut self.pending_filters,
            PendingFilter {
                filter_type,
                block_start,
                block_length,
                channels,
            },
            MAX_PENDING_FILTERS,
        );

        Ok(output_size)
    }

    fn read_filter_data<R: BitRead>(reader: &mut R) -> RarResult<u32> {
        let byte_count = reader.read_bits(2)? as usize + 1;
        let mut data = 0u32;
        for index in 0..byte_count {
            data |= reader.read_bits(8)? << (index * 8);
        }
        Ok(data)
    }

    /// Streaming variant: decompress directly to a writer instead of a Vec.
    ///
    /// Periodically flushes the sliding window to the writer to keep memory
    /// usage bounded to the dictionary size. Filters are applied in-place
    /// to a temporary buffer before flushing.
    pub fn decompress_to_writer<W: Write>(
        &mut self,
        input: &[u8],
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        if unpacked_size == 0 {
            return Ok(0);
        }

        self.begin_file_decode();
        // Try parallel decode if there are enough blocks.
        if let Some(output_size) = self.try_decompress_parallel(input, unpacked_size, writer)? {
            return Ok(output_size);
        }

        // Fall back to single-threaded decode.
        let mut reader = BitReader::new(input);
        let mut output_size: u64 = 0;
        let flush_threshold = self.flush_threshold();

        while output_size < unpacked_size {
            if self.block_bits_remaining <= 0 {
                if reader.bits_remaining() < 16 {
                    break;
                }
                self.read_block_header(&mut reader)?;
            }

            output_size = self.decode_block(
                &mut reader,
                unpacked_size,
                output_size,
                Some(flush_threshold),
            )?;

            // Flush window periodically — but only up to filter boundaries.
            if self.pending_filters.is_empty() {
                self.flush_unfiltered_stream_output(writer)?;
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

        // Apply any remaining filters and flush.
        self.flush_filters_and_write(writer)?;

        Ok(output_size)
    }

    pub fn decompress_reader_to_writer<Rd: std::io::Read, W: Write>(
        &mut self,
        mut input: Rd,
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        if unpacked_size == 0 {
            return Ok(0);
        }

        if !parallel::parallel_enabled() {
            return self.decompress_reader_to_writer_single_thread(input, unpacked_size, writer, 0);
        }

        self.begin_file_decode();
        let mut output_size = 0u64;
        let mut staged = Vec::with_capacity(STREAMING_PARALLEL_READ_BUFFER_SIZE);
        let mut read_buf = vec![0u8; STREAMING_PARALLEL_READ_BUFFER_SIZE];
        let mut reached_eof = false;
        let mut staged_start = 0usize;
        let mut staged_bit_offset = 0usize;

        while output_size < unpacked_size {
            if staged_start > 0
                && (staged_start >= STREAMING_PARALLEL_READ_BUFFER_SIZE / 2
                    || staged.len() == STREAMING_PARALLEL_READ_BUFFER_SIZE)
            {
                Self::compact_staged_buffer(&mut staged, &mut staged_start);
            }

            while !reached_eof && staged.len() - staged_start < STREAMING_PARALLEL_READ_BUFFER_SIZE
            {
                if staged.len() == STREAMING_PARALLEL_READ_BUFFER_SIZE {
                    Self::compact_staged_buffer(&mut staged, &mut staged_start);
                }

                let max_read = STREAMING_PARALLEL_READ_BUFFER_SIZE - (staged.len() - staged_start);
                let read = input
                    .read(&mut read_buf[..max_read])
                    .map_err(RarError::Io)?;
                if read == 0 {
                    reached_eof = true;
                    break;
                }
                staged.extend_from_slice(&read_buf[..read]);
            }

            let staged_slice = &staged[staged_start..];
            if staged_slice.is_empty() {
                break;
            }

            let have_incomplete_block = self.block_bits_remaining > 0 || staged_bit_offset > 0;
            if have_incomplete_block {
                let mut reader = BitReader::new(staged_slice);
                if staged_bit_offset > 0 {
                    reader.skip_bits(staged_bit_offset as u32)?;
                }

                match self.decode_block(
                    &mut reader,
                    unpacked_size,
                    output_size,
                    Some(self.flush_threshold()),
                ) {
                    Ok(new_output_size) => {
                        output_size = new_output_size;
                        self.flush_stream_output(writer)?;
                        staged_bit_offset = reader.position();
                        Self::advance_staged_prefix(
                            &mut staged_start,
                            staged.len(),
                            &mut staged_bit_offset,
                        );
                        continue;
                    }
                    Err(error) if parallel::is_truncated_input_error(&error) && !reached_eof => {
                        staged_bit_offset = reader.position();
                        Self::advance_staged_prefix(
                            &mut staged_start,
                            staged.len(),
                            &mut staged_bit_offset,
                        );
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }

            let consumed = self.process_buffered_blocks(
                staged_slice,
                unpacked_size,
                &mut output_size,
                writer,
            )?;
            if consumed > 0 {
                staged_start += consumed;
                if staged_start >= staged.len() {
                    staged.clear();
                    staged_start = 0;
                }
                staged_bit_offset = 0;
                continue;
            }

            if !reached_eof && staged_slice.len() < STREAMING_PARALLEL_MIN_PROCESS_SIZE {
                continue;
            }

            let mut reader = BitReader::new(staged_slice);
            if !self.try_read_block_header_buffered(&mut reader)? {
                if reached_eof {
                    break;
                }
                continue;
            }

            staged_bit_offset = reader.position();
        }

        self.flush_filters_and_write(writer)?;
        Ok(output_size)
    }

    fn decompress_reader_to_writer_single_thread<Rd: std::io::Read, W: Write>(
        &mut self,
        input: Rd,
        unpacked_size: u64,
        writer: &mut W,
        mut output_size: u64,
    ) -> RarResult<u64> {
        self.begin_file_decode();
        let mut reader = StreamingBitReader::new(input);
        let flush_threshold = self.flush_threshold();

        while output_size < unpacked_size {
            if self.block_bits_remaining <= 0 {
                if reader.bits_remaining() < 16 {
                    break;
                }
                self.read_block_header(&mut reader)?;
            }

            output_size = self.decode_block(
                &mut reader,
                unpacked_size,
                output_size,
                Some(flush_threshold),
            )?;

            if self.pending_filters.is_empty() {
                self.flush_unfiltered_stream_output(writer)?;
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

        self.flush_filters_and_write(writer)?;
        Ok(output_size)
    }

    /// Chunked variant: decompress with output split at compressed byte boundaries.
    ///
    /// `boundaries` lists compressed byte offsets where volume transitions occur,
    /// paired with the new volume index. At each boundary crossing, the current
    /// writer is flushed and a new one is obtained from `writer_factory`.
    ///
    /// Returns a list of `(volume_index, bytes_written)` for each chunk. The first
    /// chunk starts at `first_volume_index`.
    pub fn decompress_to_writer_chunked<F>(
        &mut self,
        input: &[u8],
        unpacked_size: u64,
        first_volume_index: usize,
        boundaries: &[super::VolumeTransition],
        mut writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        self.begin_file_decode();
        let mut reader = BitReader::new(input);
        let mut output_size: u64 = 0;
        let mut boundary_idx = 0;
        let flush_threshold = self.flush_threshold();

        // Track per-chunk output.
        let mut chunks: Vec<(usize, u64)> = Vec::new();
        let mut current_vol = first_volume_index;
        let mut current_writer = writer_factory(current_vol)?;
        let mut chunk_bytes: u64 = 0;

        while output_size < unpacked_size {
            if self.block_bits_remaining <= 0 {
                if reader.bits_remaining() < 16 {
                    break;
                }
                self.read_block_header(&mut reader)?;
            }

            let prev_output = output_size;
            output_size = self.decode_block(
                &mut reader,
                unpacked_size,
                output_size,
                Some(flush_threshold),
            )?;
            let decoded_this_round = output_size - prev_output;

            // Check if we crossed a volume boundary in compressed space.
            let byte_pos = reader.byte_position() as u64;
            if boundary_idx < boundaries.len()
                && byte_pos >= boundaries[boundary_idx].compressed_offset
            {
                // Flush current writer and record chunk.
                self.flush_filters_and_write(&mut *current_writer)?;
                chunk_bytes += decoded_this_round;
                chunks.push((current_vol, chunk_bytes));

                // Switch to new volume's writer.
                current_vol = boundaries[boundary_idx].volume_index;
                boundary_idx += 1;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
            } else {
                chunk_bytes += decoded_this_round;

                // Flush window periodically — but only up to filter boundaries.
                if self.pending_filters.is_empty() {
                    self.flush_unfiltered_stream_output(&mut *current_writer)?;
                } else {
                    self.flush_filters_and_write(&mut *current_writer)?;
                    if self.window.unflushed_bytes() as usize > self.window.dict_size() {
                        return Err(RarError::CorruptArchive {
                            detail: "RAR5 pending filters exceeded dictionary window before flush"
                                .into(),
                        });
                    }
                }
            }
        }

        // Final flush.
        self.flush_filters_and_write(&mut *current_writer)?;
        if chunk_bytes > 0 || chunks.is_empty() {
            chunks.push((current_vol, chunk_bytes));
        }

        Ok(chunks)
    }

    pub fn decompress_reader_to_writer_chunked<Rd: std::io::Read, F>(
        &mut self,
        input: Rd,
        unpacked_size: u64,
        first_volume_index: usize,
        transitions: std::sync::Arc<std::sync::Mutex<Vec<super::VolumeTransition>>>,
        mut writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        self.begin_file_decode();
        let mut reader = StreamingBitReader::new(input);
        let mut output_size: u64 = 0;
        let mut boundary_idx = 0;
        let flush_threshold = self.flush_threshold();

        let mut chunks: Vec<(usize, u64)> = Vec::new();
        let mut current_vol = first_volume_index;
        let mut current_writer = writer_factory(current_vol)?;
        let mut chunk_bytes: u64 = 0;

        while output_size < unpacked_size {
            if self.block_bits_remaining <= 0 {
                if reader.bits_remaining() < 16 {
                    break;
                }
                self.read_block_header(&mut reader)?;
            }

            let prev_output = output_size;
            output_size = self.decode_block(
                &mut reader,
                unpacked_size,
                output_size,
                Some(flush_threshold),
            )?;
            let decoded_this_round = output_size - prev_output;

            let byte_pos = reader.byte_position() as u64;
            let boundary = {
                let guard = transitions.lock().map_err(|_| RarError::CorruptArchive {
                    detail: "RAR5 chunked transition state poisoned".into(),
                })?;
                guard.get(boundary_idx).cloned()
            };

            if let Some(boundary) = boundary
                && byte_pos >= boundary.compressed_offset
            {
                self.flush_filters_and_write(&mut *current_writer)?;
                chunk_bytes += decoded_this_round;
                chunks.push((current_vol, chunk_bytes));

                current_vol = boundary.volume_index;
                boundary_idx += 1;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
            } else {
                chunk_bytes += decoded_this_round;

                if self.pending_filters.is_empty() {
                    self.flush_unfiltered_stream_output(&mut *current_writer)?;
                } else {
                    self.flush_filters_and_write(&mut *current_writer)?;
                    if self.window.unflushed_bytes() as usize > self.window.dict_size() {
                        return Err(RarError::CorruptArchive {
                            detail: "RAR5 pending filters exceeded dictionary window before flush"
                                .into(),
                        });
                    }
                }
            }
        }

        self.flush_filters_and_write(&mut *current_writer)?;
        if chunk_bytes > 0 || chunks.is_empty() {
            chunks.push((current_vol, chunk_bytes));
        }

        Ok(chunks)
    }

    /// Flush pending filters and remaining window data to a writer.
    fn flush_filters_and_write<W: Write + ?Sized>(&mut self, writer: &mut W) -> RarResult<()> {
        if self.pending_filters.is_empty() {
            return self.flush_unfiltered_stream_output(writer);
        }

        let total = self.window.total_written();
        let mut written_up_to = self.window.total_flushed();
        if written_up_to == total {
            self.pending_filters
                .retain(|filter| filter.block_start >= total);
            return Ok(());
        }

        let mut pending_filters = std::mem::take(&mut self.pending_filters);
        pending_filters.sort_by_key(|filter| filter.block_start);
        let mut remaining_filters = Vec::with_capacity(pending_filters.len());

        let mut pending_iter = pending_filters.into_iter();
        while let Some(filter) = pending_iter.next() {
            if filter.block_start < written_up_to {
                return Err(RarError::CorruptArchive {
                    detail: format!(
                        "RAR5 filter block starts before flushed border ({} < {})",
                        filter.block_start, written_up_to
                    ),
                });
            }

            if filter.block_start > total {
                remaining_filters.push(filter);
                remaining_filters.extend(pending_iter);
                break;
            }

            if filter.block_start > written_up_to {
                let prefix_len = (filter.block_start - written_up_to) as usize;
                self.window
                    .flush_visible_until(filter.block_start, writer)
                    .map_err(RarError::Io)?;
                self.current_file_written_size += prefix_len as u64;
                written_up_to = filter.block_start;
            }

            let block_end = filter
                .block_start
                .saturating_add(filter.block_length as u64);
            if block_end <= total {
                let file_block_start = self.current_file_written_size;
                let mut buf = self
                    .window
                    .try_copy_output(filter.block_start, filter.block_length)?;
                match filter.filter_type {
                    FilterType::Delta => filter::apply_delta(&mut buf, filter.channels),
                    FilterType::E8 => filter::apply_e8(&mut buf, file_block_start),
                    FilterType::E8E9 => filter::apply_e8e9(&mut buf, file_block_start),
                    FilterType::Arm => filter::apply_arm(&mut buf, file_block_start),
                    FilterType::Unsupported(_) => {}
                }
                if filter.filter_type.emits_output() {
                    for (offset, len) in self
                        .window
                        .visible_subranges(filter.block_start, filter.block_length)
                    {
                        writer
                            .write_all(&buf[offset..offset + len])
                            .map_err(RarError::Io)?;
                    }
                }
                self.current_file_written_size += filter.block_length as u64;
                written_up_to = block_end;
                self.window.mark_flushed(written_up_to);
            } else {
                remaining_filters.push(filter);
                remaining_filters.extend(pending_iter);
                break;
            }
        }

        if remaining_filters.is_empty() && written_up_to < total {
            let tail_len = (total - written_up_to) as usize;
            self.window
                .flush_visible_until(total, writer)
                .map_err(RarError::Io)?;
            self.current_file_written_size += tail_len as u64;
            written_up_to = total;
        }

        self.pending_filters = remaining_filters;
        self.window.mark_flushed(written_up_to);

        Ok(())
    }

    /// Reset the decoder for a new file (non-solid mode).
    pub fn reset(&mut self) {
        self.window.reset();
        self.dist_cache = [usize::MAX; DIST_CACHE_SIZE];
        self.last_length = 0;
        self.nc_table = None;
        self.dc_table = None;
        self.ldc_table = None;
        self.rc_table = None;
        self.code_lengths.fill(0);
        self.block_bits_remaining = 0;
        self.is_last_block = false;
        self.pending_filters.clear();
        self.current_file_base_total = 0;
        self.current_file_written_size = 0;
    }

    /// Prepare for the next member in a solid archive.
    ///
    /// In solid mode, the sliding window (dictionary) carries over between
    /// files, enabling cross-file back-references. The distance cache and
    /// Huffman tables also persist.
    pub fn prepare_solid_continuation(&mut self) {
        self.block_bits_remaining = 0;
        self.is_last_block = false;
        self.pending_filters.clear();
        self.current_file_base_total = self.window.total_written();
        self.current_file_written_size = 0;
    }
}

/// Decompress LZ-compressed data.
///
/// `input` is the compressed data area.
/// `unpacked_size` is the expected output size.
/// `info` provides compression parameters (dictionary size, solid flag, etc.).
///
/// Returns the decompressed data.
pub fn decompress_lz(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
) -> RarResult<Vec<u8>> {
    decompress_lz_with_max_dict_size(input, unpacked_size, info, Limits::default().max_dict_size)
}

pub(crate) fn decompress_lz_with_max_dict_size(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    max_dict_size: u64,
) -> RarResult<Vec<u8>> {
    let dict_size = checked_lz_dict_size(info, max_dict_size)?;
    let mut decoder = LzDecoder::try_new(dict_size, info.version)?;
    decoder.decompress(input, unpacked_size)
}

/// Streaming variant: decompress LZ data directly to a writer.
///
/// Memory usage is bounded to the default dictionary limit instead of
/// accumulating the full output in memory.
pub fn decompress_lz_to_writer<W: Write>(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    writer: &mut W,
) -> RarResult<u64> {
    decompress_lz_to_writer_with_max_dict_size(
        input,
        unpacked_size,
        info,
        writer,
        Limits::default().max_dict_size,
    )
}

pub(crate) fn decompress_lz_to_writer_with_max_dict_size<W: Write>(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    writer: &mut W,
    max_dict_size: u64,
) -> RarResult<u64> {
    let dict_size = checked_lz_dict_size(info, max_dict_size)?;
    let mut decoder = LzDecoder::try_new(dict_size, info.version)?;
    decoder.decompress_to_writer(input, unpacked_size, writer)
}

pub fn decompress_lz_reader_to_writer<Rd: std::io::Read, W: Write>(
    input: Rd,
    unpacked_size: u64,
    info: &CompressionInfo,
    writer: &mut W,
) -> RarResult<u64> {
    decompress_lz_reader_to_writer_with_max_dict_size(
        input,
        unpacked_size,
        info,
        writer,
        Limits::default().max_dict_size,
    )
}

pub(crate) fn decompress_lz_reader_to_writer_with_max_dict_size<Rd: std::io::Read, W: Write>(
    input: Rd,
    unpacked_size: u64,
    info: &CompressionInfo,
    writer: &mut W,
    max_dict_size: u64,
) -> RarResult<u64> {
    let dict_size = checked_lz_dict_size(info, max_dict_size)?;
    let mut decoder = LzDecoder::try_new(dict_size, info.version)?;
    decoder.decompress_reader_to_writer(input, unpacked_size, writer)
}

pub fn decompress_lz_reader_to_writer_chunked<Rd: std::io::Read, F>(
    input: Rd,
    unpacked_size: u64,
    info: &CompressionInfo,
    first_volume_index: usize,
    transitions: std::sync::Arc<std::sync::Mutex<Vec<super::VolumeTransition>>>,
    writer_factory: F,
) -> RarResult<Vec<(usize, u64)>>
where
    F: FnMut(usize) -> RarResult<Box<dyn Write>>,
{
    decompress_lz_reader_to_writer_chunked_with_max_dict_size(
        input,
        unpacked_size,
        info,
        first_volume_index,
        transitions,
        writer_factory,
        Limits::default().max_dict_size,
    )
}

pub(crate) fn decompress_lz_reader_to_writer_chunked_with_max_dict_size<Rd: std::io::Read, F>(
    input: Rd,
    unpacked_size: u64,
    info: &CompressionInfo,
    first_volume_index: usize,
    transitions: std::sync::Arc<std::sync::Mutex<Vec<super::VolumeTransition>>>,
    writer_factory: F,
    max_dict_size: u64,
) -> RarResult<Vec<(usize, u64)>>
where
    F: FnMut(usize) -> RarResult<Box<dyn Write>>,
{
    let dict_size = checked_lz_dict_size(info, max_dict_size)?;
    let mut decoder = LzDecoder::try_new(dict_size, info.version)?;
    decoder.decompress_reader_to_writer_chunked(
        input,
        unpacked_size,
        first_volume_index,
        transitions,
        writer_factory,
    )
}

pub(crate) fn decompress_lz_to_writer_chunked_with_max_dict_size<F>(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    first_volume_index: usize,
    boundaries: &[super::VolumeTransition],
    writer_factory: F,
    max_dict_size: u64,
) -> RarResult<Vec<(usize, u64)>>
where
    F: FnMut(usize) -> RarResult<Box<dyn Write>>,
{
    let dict_size = checked_lz_dict_size(info, max_dict_size)?;
    let mut decoder = LzDecoder::try_new(dict_size, info.version)?;
    decoder.decompress_to_writer_chunked(
        input,
        unpacked_size,
        first_volume_index,
        boundaries,
        writer_factory,
    )
}

pub(crate) fn effective_lz_window_size(dict_size: u64) -> u64 {
    dict_size.max(UNRAR_MIN_LZ_WINDOW_SIZE)
}

fn checked_lz_dict_size(info: &CompressionInfo, max_dict_size: u64) -> RarResult<usize> {
    let dict_size = effective_lz_window_size(info.dict_size);
    if dict_size > UNRAR_UNPACK_MAX_DICT_SIZE {
        return Err(RarError::DictionaryTooLarge {
            size: dict_size,
            max: UNRAR_UNPACK_MAX_DICT_SIZE,
        });
    }
    if dict_size > max_dict_size {
        return Err(RarError::DictionaryTooLarge {
            size: dict_size,
            max: max_dict_size,
        });
    }

    usize::try_from(dict_size).map_err(|_| RarError::DictionaryTooLarge {
        size: dict_size,
        max: usize::MAX as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_to_length_formula() {
        // Slots 0-7: base = 2 + slot, no extra bits
        let data = [0u8; 8]; // unused, no bits needed for slots 0-7
        for slot in 0..8 {
            let mut reader = BitReader::new(&data);
            let len = LzDecoder::slot_to_length(&mut reader, slot).unwrap();
            assert_eq!(len, 2 + slot, "slot {slot}");
        }
    }

    #[test]
    fn test_slot_to_length_groups_of_4() {
        // Verify extra bits group in 4s (not 3s): slots 8-11 all have 1 extra bit
        let data = [0xFF; 8]; // all 1s for extra bits
        for slot in 8..12 {
            let mut reader = BitReader::new(&data);
            let len = LzDecoder::slot_to_length(&mut reader, slot).unwrap();
            // With 1 extra bit = 1, check base + 1
            let lbits = slot / 4 - 1;
            let base = 2 + ((4 | (slot & 3)) << lbits);
            assert_eq!(lbits, 1, "slots 8-11 should have 1 extra bit");
            assert_eq!(len, base + 1, "slot {slot}"); // extra bit reads 1
        }
    }

    #[test]
    fn test_slot_to_length_max() {
        // Slot 43 with max extra bits should give MAX_LZ_MATCH = 4097
        let data = [0xFF; 8]; // all 1s
        let mut reader = BitReader::new(&data);
        let len = LzDecoder::slot_to_length(&mut reader, 43).unwrap();
        // slot 43: lbits = 43/4-1 = 9, base = 2 + (4|3)<<9 = 2 + 7*512 = 3586
        // extra = 9 bits of 1s = 511, total = 3586 + 511 = 4097
        assert_eq!(len, 4097);
    }

    #[test]
    fn test_decoder_creation() {
        let decoder = LzDecoder::new(128 * 1024, 0);
        assert_eq!(decoder.window.dict_size(), 128 * 1024);
        assert_eq!(decoder.dist_cache, [usize::MAX; 4]);
    }

    #[test]
    fn test_uninitialized_old_dist_uses_unrar_sentinel() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);

        assert_eq!(decoder.promote_old_dist(2).unwrap(), usize::MAX);
        assert_eq!(decoder.dist_cache, [usize::MAX; 4]);
    }

    #[test]
    fn lz_decoder_rejects_future_rar5_unpack_versions_like_unrar() {
        assert!(matches!(
            LzDecoder::try_new(128 * 1024, 2),
            Err(RarError::UnsupportedCompression { version: 2, .. })
        ));
    }

    #[test]
    fn test_effective_lz_window_uses_unrar_minimum() {
        assert_eq!(effective_lz_window_size(128 * 1024), 0x40000);
        assert_eq!(effective_lz_window_size(512 * 1024), 512 * 1024);
    }

    #[test]
    fn distance_slot_parts_keep_32bit_boundary_before_unrar_overflow_sentinel() {
        let distance =
            LzDecoder::distance_from_slot_parts(61, 29, (1u64 << 29) - 16, 15, 32).unwrap();

        assert_eq!(distance, 0x8000_0000);
    }

    #[test]
    fn distance_slot_parts_use_unrar_32bit_overflow_sentinel() {
        let distance = LzDecoder::distance_from_slot_parts(62, 30, 0, 0, 32).unwrap();

        assert_eq!(distance, u32::MAX as usize);
    }

    #[test]
    fn distance_slot_parts_allow_rar7_extended_distance_on_64bit() {
        let distance = LzDecoder::distance_from_slot_parts(79, 38, 0, 0, 64).unwrap();

        assert_eq!(distance, (3u64 << 38) as usize + 1);
    }

    #[test]
    fn test_checked_lz_dict_size_enforces_effective_minimum() {
        let info = CompressionInfo {
            format: crate::types::ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: crate::types::CompressionMethod::Normal,
            dict_size: 128 * 1024,
        };

        let result = checked_lz_dict_size(&info, 128 * 1024);

        assert!(matches!(
            result,
            Err(RarError::DictionaryTooLarge {
                size: 262_144,
                max: 131_072
            })
        ));
    }

    #[test]
    fn test_checked_lz_dict_size_allows_custom_limit_above_default() {
        let info = CompressionInfo {
            format: crate::types::ArchiveFormat::Rar5,
            version: 1,
            solid: false,
            method: crate::types::CompressionMethod::Normal,
            dict_size: 512 * 1024 * 1024,
        };

        let dict_size = checked_lz_dict_size(&info, 512 * 1024 * 1024).unwrap();

        assert_eq!(dict_size, 512 * 1024 * 1024);
    }

    #[test]
    fn test_max_dict_size_enforcement() {
        let info = CompressionInfo {
            format: crate::types::ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: crate::types::CompressionMethod::Normal,
            dict_size: 1024 * 1024 * 1024, // 1 GB
        };
        let result = decompress_lz(&[], 0, &info);
        assert!(matches!(result, Err(RarError::DictionaryTooLarge { .. })));
    }

    #[test]
    fn test_empty_input() {
        let info = CompressionInfo {
            format: crate::types::ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: crate::types::CompressionMethod::Normal,
            dict_size: 128 * 1024,
        };
        let result = decompress_lz(&[], 0, &info);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_decoder_reset() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.dist_cache = [10, 20, 30, 40];
        decoder.block_bits_remaining = 100;
        decoder.reset();
        assert_eq!(decoder.dist_cache, [usize::MAX; 4]);
        assert_eq!(decoder.block_bits_remaining, 0);
        assert!(decoder.nc_table.is_none());
    }

    #[test]
    fn test_prepare_solid_continuation() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.dist_cache = [10, 20, 30, 40];
        decoder.block_bits_remaining = 50;
        // Write some data to the window to simulate previous decompression.
        decoder.window.put_byte(0xAA);
        decoder.window.put_byte(0xBB);

        decoder.prepare_solid_continuation();

        // block state should be reset.
        assert_eq!(decoder.block_bits_remaining, 0);
        // dist_cache should be preserved.
        assert_eq!(decoder.dist_cache, [10, 20, 30, 40]);
        // Window state should be preserved.
        assert_eq!(decoder.window.total_written(), 2);
        assert_eq!(decoder.window.get_byte(1), 0xBB);
        assert_eq!(decoder.window.get_byte(2), 0xAA);
    }

    #[test]
    fn test_flush_filters_stops_at_first_incomplete_block() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.window.put_bytes(b"abcdefghij");
        decoder.pending_filters = vec![
            PendingFilter {
                filter_type: FilterType::E8,
                block_start: 4,
                block_length: 8,
                channels: 0,
            },
            PendingFilter {
                filter_type: FilterType::E8,
                block_start: 8,
                block_length: 1,
                channels: 0,
            },
        ];

        let mut out = Vec::new();
        decoder.flush_filters_and_write(&mut out).unwrap();

        assert_eq!(out, b"abcd");
        assert_eq!(decoder.window.total_flushed(), 4);
        assert_eq!(decoder.pending_filters.len(), 2);
        assert_eq!(decoder.pending_filters[0].block_start, 4);
        assert_eq!(decoder.pending_filters[1].block_start, 8);
    }

    #[test]
    fn test_filterless_flush_advances_hidden_match_tail_like_unrar() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.window.put_bytes(b"ABCD");
        decoder.window.mark_flushed(4);
        decoder.current_file_written_size = 4;
        decoder.window.copy_with_visible_len(4, 4, 2).unwrap();

        let mut out = Vec::new();
        decoder.flush_filters_and_write(&mut out).unwrap();

        assert_eq!(out, b"AB");
        assert_eq!(
            decoder.window.total_flushed(),
            decoder.window.total_written()
        );
        assert_eq!(decoder.current_file_written_size, 8);
    }

    #[test]
    fn test_raw_stream_flush_advances_later_filter_file_offset_like_unrar() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        let mut out = Vec::new();

        decoder.window.put_bytes(b"prefix");
        decoder.flush_stream_output(&mut out).unwrap();
        assert_eq!(out, b"prefix");
        assert_eq!(decoder.window.total_flushed(), 6);
        assert_eq!(decoder.current_file_written_size, 6);

        decoder.window.put_bytes(&[0xe8, 100, 0, 0, 0]);
        decoder.pending_filters = vec![PendingFilter {
            filter_type: FilterType::E8,
            block_start: 6,
            block_length: 5,
            channels: 0,
        }];

        decoder.flush_stream_output(&mut out).unwrap();

        let mut expected_filter_output = vec![0xe8, 100, 0, 0, 0];
        filter::apply_e8(&mut expected_filter_output, 6);
        assert_eq!(&out[..6], b"prefix");
        assert_eq!(&out[6..], expected_filter_output.as_slice());
        assert_ne!(&out[6..], &[0xe8, 99, 0, 0, 0]);
        assert_eq!(decoder.current_file_written_size, 11);
    }

    #[test]
    fn test_filter_block_emits_only_visible_hidden_tail_prefix() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.window.put_bytes(b"ABCD");
        decoder.window.mark_flushed(4);
        decoder.current_file_written_size = 4;
        decoder.window.copy_with_visible_len(4, 4, 2).unwrap();
        decoder.pending_filters = vec![PendingFilter {
            filter_type: FilterType::E8,
            block_start: 4,
            block_length: 4,
            channels: 0,
        }];

        let mut out = Vec::new();
        decoder.flush_filters_and_write(&mut out).unwrap();

        assert_eq!(out, b"AB");
        assert_eq!(
            decoder.window.total_flushed(),
            decoder.window.total_written()
        );
        assert_eq!(decoder.current_file_written_size, 8);
        assert!(decoder.pending_filters.is_empty());
    }

    #[test]
    fn test_flush_filters_keeps_future_filter_when_no_output_is_ready() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.pending_filters = vec![
            PendingFilter {
                filter_type: FilterType::E8,
                block_start: 0,
                block_length: 5,
                channels: 0,
            },
            PendingFilter {
                filter_type: FilterType::E8,
                block_start: 8,
                block_length: 5,
                channels: 0,
            },
        ];

        let mut out = Vec::new();
        decoder.flush_filters_and_write(&mut out).unwrap();

        assert!(out.is_empty());
        assert_eq!(decoder.pending_filters.len(), 2);
        assert_eq!(decoder.pending_filters[0].block_start, 0);
        assert_eq!(decoder.pending_filters[1].block_start, 8);
    }

    #[test]
    fn test_unsupported_filter_skips_block_like_unrar() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.window.put_bytes(b"prefixBLOCKsuffix");
        decoder.pending_filters = vec![PendingFilter {
            filter_type: FilterType::Unsupported(7),
            block_start: 6,
            block_length: 5,
            channels: 0,
        }];

        let mut out = Vec::new();
        decoder.flush_filters_and_write(&mut out).unwrap();

        assert_eq!(out, b"prefixsuffix");
        assert_eq!(
            decoder.window.total_flushed(),
            decoder.window.total_written()
        );
        assert!(decoder.pending_filters.is_empty());
    }

    fn write_test_bits(bits: &mut Vec<u8>, value: u32, count: u8) {
        for shift in (0..count).rev() {
            bits.push(((value >> shift) & 1) as u8);
        }
    }

    fn pack_test_bits(bits: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(bits.len().div_ceil(8));
        for chunk in bits.chunks(8) {
            let mut byte = 0u8;
            for (idx, bit) in chunk.iter().enumerate() {
                byte |= *bit << (7 - idx);
            }
            bytes.push(byte);
        }
        bytes
    }

    fn rar5_filter_descriptor(block_start: u8, block_length: u8, filter_code: u8) -> Vec<u8> {
        let mut bits = Vec::new();
        write_test_bits(&mut bits, 0, 2);
        write_test_bits(&mut bits, u32::from(block_start), 8);
        write_test_bits(&mut bits, 0, 2);
        write_test_bits(&mut bits, u32::from(block_length), 8);
        write_test_bits(&mut bits, u32::from(filter_code), 3);
        pack_test_bits(&bits)
    }

    #[test]
    fn test_filter_descriptor_resets_full_queue_like_unrar() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.pending_filters = vec![
            PendingFilter {
                filter_type: FilterType::E8,
                block_start: 0,
                block_length: 5,
                channels: 0,
            };
            MAX_PENDING_FILTERS
        ];

        let descriptor = rar5_filter_descriptor(3, 5, 1);
        let mut reader = BitReader::new(&descriptor);
        decoder.handle_filter(&mut reader, 10).unwrap();

        assert_eq!(decoder.pending_filters.len(), 1);
        assert_eq!(decoder.pending_filters[0].filter_type, FilterType::E8);
        assert_eq!(decoder.pending_filters[0].block_start, 13);
        assert_eq!(decoder.pending_filters[0].block_length, 5);
    }

    #[test]
    fn test_apply_filters_to_vec_uses_logical_offset_after_suppressed_block() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        let mut output = b"preXXX".to_vec();
        output.extend_from_slice(&[0xE8, 10, 0, 0, 0]);
        output.extend_from_slice(b"tail");
        decoder.pending_filters = vec![
            PendingFilter {
                filter_type: FilterType::Unsupported(7),
                block_start: 3,
                block_length: 3,
                channels: 0,
            },
            PendingFilter {
                filter_type: FilterType::E8,
                block_start: 6,
                block_length: 5,
                channels: 0,
            },
        ];

        let filtered = decoder.apply_filters_to_vec(output, 0);

        let mut expected_e8 = [0xE8, 10, 0, 0, 0];
        filter::apply_e8(&mut expected_e8, 6);
        let mut expected = b"pre".to_vec();
        expected.extend_from_slice(&expected_e8);
        expected.extend_from_slice(b"tail");
        assert_eq!(filtered, expected);
        assert!(decoder.pending_filters.is_empty());
    }

    #[test]
    fn test_flush_filters_uses_logical_offset_after_suppressed_block() {
        let mut decoder = LzDecoder::new(128 * 1024, 0);
        decoder.window.put_bytes(b"preXXX");
        decoder.window.put_bytes(&[0xE8, 10, 0, 0, 0]);
        decoder.window.put_bytes(b"tail");
        decoder.pending_filters = vec![
            PendingFilter {
                filter_type: FilterType::Unsupported(7),
                block_start: 3,
                block_length: 3,
                channels: 0,
            },
            PendingFilter {
                filter_type: FilterType::E8,
                block_start: 6,
                block_length: 5,
                channels: 0,
            },
        ];

        let mut out = Vec::new();
        decoder.flush_filters_and_write(&mut out).unwrap();

        let mut expected_e8 = [0xE8, 10, 0, 0, 0];
        filter::apply_e8(&mut expected_e8, 6);
        let mut expected = b"pre".to_vec();
        expected.extend_from_slice(&expected_e8);
        expected.extend_from_slice(b"tail");
        assert_eq!(out, expected);
        assert_eq!(decoder.current_file_written_size, 15);
        assert!(decoder.pending_filters.is_empty());
    }

    #[test]
    fn test_slot_ranges_contiguous() {
        // Verify the length ranges from SlotToLength are contiguous (no gaps).
        // Each slot covers [base, base + 2^extra_bits). Next slot's base = prev end.
        let data = [0u8; 8];
        let mut prev_end = 3; // slot 0 covers [2,3), slot 1 should start at 3
        for slot in 1..NUM_LENGTH_SLOTS {
            let mut reader = BitReader::new(&data);
            let base = LzDecoder::slot_to_length(&mut reader, slot).unwrap();
            assert_eq!(
                base, prev_end,
                "slot {slot}: base {base} != prev_end {prev_end}"
            );
            let extra_bits = if slot < 8 { 0 } else { slot / 4 - 1 };
            prev_end = base + (1 << extra_bits);
        }
    }

    #[test]
    fn test_all_compression_methods_accepted() {
        for method_code in 1..=5u8 {
            let info = CompressionInfo {
                format: crate::types::ArchiveFormat::Rar5,
                version: 0,
                solid: false,
                method: crate::types::CompressionMethod::from_code(method_code),
                dict_size: 128 * 1024,
            };
            let result = decompress_lz(&[], 0, &info);
            assert!(result.is_ok(), "method {} failed", method_code);
        }
    }
}
