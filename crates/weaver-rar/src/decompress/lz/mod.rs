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
pub mod window;

use std::io::Write;

use tracing::trace;

use crate::error::{RarError, RarResult};
use crate::types::CompressionInfo;
use bitstream::BitReader;
use filter::{FilterType, PendingFilter};
use huffman::HuffmanTable;
use window::Window;

/// Maximum dictionary size we'll allocate (256 MB).
const MAX_DICT_SIZE: u64 = 256 * 1024 * 1024;

/// Maximum number of length slots.
const NUM_LENGTH_SLOTS: usize = 44;

/// Number of entries in the last-distance cache.
const DIST_CACHE_SIZE: usize = 4;

/// State for the LZ decompressor.
pub struct LzDecoder {
    /// Sliding window / ring buffer.
    window: Window,
    /// Last-distance cache (4 entries for repeat matches).
    dist_cache: [usize; DIST_CACHE_SIZE],
    /// Length of the last match (for symbol 257 repeat).
    last_length: usize,
    /// Current Huffman tables (kept across blocks when table_present is false).
    nc_table: Option<HuffmanTable>,
    dc_table: Option<HuffmanTable>,
    ldc_table: Option<HuffmanTable>,
    rc_table: Option<HuffmanTable>,
    /// Persistent code lengths for delta encoding across blocks.
    code_lengths: Vec<u8>,
    /// Number of compressed bits remaining in the current block.
    /// When 0, a new block header must be read.
    block_bits_remaining: i64,
    /// Whether the current block is the last one.
    is_last_block: bool,
    /// Filters pending application to ranges of the current output.
    pending_filters: Vec<PendingFilter>,
}

impl LzDecoder {
    /// Create a new LZ decoder with the specified dictionary size.
    pub fn new(dict_size: usize) -> Self {
        let total_symbols = huffman::HUFF_NC + huffman::HUFF_DC + huffman::HUFF_LDC + huffman::HUFF_RC;
        Self {
            window: Window::new(dict_size),
            dist_cache: [0; DIST_CACHE_SIZE],
            last_length: 0,
            nc_table: None,
            dc_table: None,
            ldc_table: None,
            rc_table: None,
            code_lengths: vec![0u8; total_symbols],
            block_bits_remaining: 0,
            is_last_block: false,
            pending_filters: Vec::new(),
        }
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
    fn read_block_header(&mut self, reader: &mut BitReader) -> RarResult<()> {
        // Block header is byte-aligned.
        reader.align_byte();

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
        self.block_bits_remaining = extra_bits + (block_bytes - 1) * 8;

        // Read new Huffman tables if present (consumes bits from the block).
        if table_present || self.nc_table.is_none() {
            let pos_before = reader.position();
            let (nc, dc, ldc, rc) = huffman::read_tables(reader, &mut self.code_lengths)?;
            let bits_used = (reader.position() - pos_before) as i64;
            self.block_bits_remaining -= bits_used;
            self.nc_table = Some(nc);
            self.dc_table = Some(dc);
            self.ldc_table = Some(ldc);
            self.rc_table = Some(rc);
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

        let mut reader = BitReader::new(input);
        let mut output_size: u64 = 0;

        while output_size < unpacked_size {
            // Read a new block header if we've exhausted the current block.
            if self.block_bits_remaining <= 0 {
                if reader.bits_remaining() < 16 {
                    break;
                }
                self.read_block_header(&mut reader)?;
            }

            // Decode symbols from the current block.
            output_size = self.decode_block(&mut reader, unpacked_size, output_size)?;
        }

        // Extract output from the window.
        let total = self.window.total_written();
        let start = total.saturating_sub(unpacked_size);
        let len = (total - start) as usize;
        let mut output = self.window.copy_output(start, len);

        // Apply pending filters to the output buffer.
        self.apply_filters(&mut output, start);

        Ok(output)
    }

    /// Apply pending filters to an output buffer.
    fn apply_filters(&mut self, output: &mut [u8], base_offset: u64) {
        for f in &self.pending_filters {
            if f.block_start >= base_offset && f.block_length > 0 {
                let rel_start = (f.block_start - base_offset) as usize;
                let rel_end = rel_start + f.block_length;
                if rel_end <= output.len() {
                    let block = &mut output[rel_start..rel_end];
                    match f.filter_type {
                        FilterType::Delta => filter::apply_delta(block, f.channels),
                        FilterType::E8 => filter::apply_e8(block, f.block_start),
                        FilterType::E8E9 => filter::apply_e8e9(block, f.block_start),
                        FilterType::Arm => filter::apply_arm(block, f.block_start),
                    }
                }
            }
        }
        self.pending_filters.clear();
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
    fn decode_block(
        &mut self,
        reader: &mut BitReader,
        unpacked_size: u64,
        mut output_size: u64,
    ) -> RarResult<u64> {
        while output_size < unpacked_size && self.block_bits_remaining > 0 {
            if reader.bits_remaining() < 1 {
                break;
            }

            let pos_before = reader.position();

            let sym = self.nc_table.as_ref().unwrap().decode(reader)? as u32;

            if sym < 256 {
                // Literal byte.
                self.window.put_byte(sym as u8);
                output_size += 1;
                trace!("literal: {:#04x}", sym);
            } else if sym == 256 {
                // Filter marker. Read full filter descriptor and enqueue.
                output_size = self.handle_filter(reader, output_size)?;
            } else if sym == 257 {
                // Repeat previous match (same length, same distance[0]).
                if self.last_length != 0 {
                    let distance = self.dist_cache[0];
                    let mut length = self.last_length;
                    let remaining = (unpacked_size - output_size) as usize;
                    length = length.min(remaining);
                    trace!("repeat last: dist={}, len={}", distance, length);
                    self.window.copy(distance, length)?;
                    output_size += length as u64;
                }
            } else if (258..=261).contains(&sym) {
                // Repeat distance from cache.
                let cache_idx = (sym - 258) as usize;
                let distance = self.dist_cache[cache_idx];
                if distance == 0 {
                    return Err(RarError::CorruptArchive {
                        detail: "distance cache entry is zero".into(),
                    });
                }

                // Read length from RC table.
                // Note: cache references do NOT get distance-based length adjustment.
                let rc = self.rc_table.as_ref().unwrap();
                let mut length = self.decode_rc_length(reader, rc)?;
                let remaining = (unpacked_size - output_size) as usize;
                length = length.min(remaining);

                // Promote this cache entry to front.
                if cache_idx > 0 {
                    let dist = self.dist_cache[cache_idx];
                    for i in (1..=cache_idx).rev() {
                        self.dist_cache[i] = self.dist_cache[i - 1];
                    }
                    self.dist_cache[0] = dist;
                }

                self.last_length = length;
                trace!("repeat match: dist={}, len={}", distance, length);
                self.window.copy(distance, length)?;
                output_size += length as u64;
            } else if (262..=305).contains(&sym) {
                // Inline length-distance pair.
                let length_idx = (sym - 262) as usize;
                let mut length = self.decode_inline_length(reader, length_idx)?;
                let dc = self.dc_table.as_ref().unwrap();
                let ldc = self.ldc_table.as_ref().unwrap();
                let distance = self.decode_distance(reader, dc, ldc)?;
                length = Self::adjust_length_for_distance(length, distance);
                let remaining = (unpacked_size - output_size) as usize;
                length = length.min(remaining);

                // Update distance cache.
                self.dist_cache[3] = self.dist_cache[2];
                self.dist_cache[2] = self.dist_cache[1];
                self.dist_cache[1] = self.dist_cache[0];
                self.dist_cache[0] = distance;

                self.last_length = length;
                trace!("match: dist={}, len={}", distance, length);
                self.window.copy(distance, length)?;
                output_size += length as u64;
            } else {
                return Err(RarError::CorruptArchive {
                    detail: format!("invalid NC symbol: {sym}"),
                });
            }

            let bits_consumed = (reader.position() - pos_before) as i64;
            self.block_bits_remaining -= bits_consumed;
        }

        Ok(output_size)
    }

    /// Convert a length slot (0-43) to a match length.
    ///
    /// Uses the same formula as unrar's SlotToLength:
    /// - Slots 0-7: length = 2 + slot, no extra bits
    /// - Slots 8+:  extra_bits = slot/4 - 1
    ///   length = 2 + (4 | (slot & 3)) << extra_bits + read_bits(extra_bits)
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

    /// Decode an inline length value from a length slot index (0-43).
    fn decode_inline_length(&self, reader: &mut BitReader, idx: usize) -> RarResult<usize> {
        Self::slot_to_length(reader, idx)
    }

    /// Decode a length from the RC/LenDecoder table (used for symbols 256 and 258-261).
    fn decode_rc_length(
        &self,
        reader: &mut BitReader,
        rc: &HuffmanTable,
    ) -> RarResult<usize> {
        let slot = rc.decode(reader)? as usize;
        Self::slot_to_length(reader, slot)
    }

    /// Decode a distance value from the DC and LDC (AlignDecoder) tables.
    ///
    /// RAR5 distance decoding:
    /// - dist_code < 4: distance = dist_code + 1
    /// - dist_code >= 4: base + extra bits, where extra bits may be split
    ///   between the bitstream (high) and AlignDecoder/LDC (low 4 bits)
    fn decode_distance(
        &self,
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
                // Split: high bits from bitstream, low 4 bits from AlignDecoder (LDC).
                let high = if num_bits > 4 {
                    (reader.read_bits((num_bits - 4) as u8)? as usize) << 4
                } else {
                    0
                };
                let low = ldc.decode(reader)? as usize;
                base + high + low
            } else {
                // All extra bits from bitstream.
                base + reader.read_bits(num_bits as u8)? as usize
            }
        };

        // Distance is 1-based for the window copy.
        Ok(distance + 1)
    }

    /// Handle a filter marker (symbol 256).
    ///
    /// Reads the full filter descriptor from the bitstream and pushes a
    /// [`PendingFilter`] for later application to the output.
    fn handle_filter(
        &mut self,
        reader: &mut BitReader,
        output_size: u64,
    ) -> RarResult<u64> {
        // Read filter flags byte.
        let flags = reader.read_bits(8)?;
        let filter_code = (flags & 0x07) as u8; // bits 0-2: filter type
        let use_counts = (flags >> 3) & 1; // bit 3: channel count follows (DELTA)
        let block_start_bits = ((flags >> 4) & 0x0F) as u8; // bits 4-7: size of block_start field

        let filter_type = FilterType::from_code(filter_code).ok_or(
            RarError::UnsupportedFilter {
                filter_type: filter_code,
            },
        )?;

        // Read block start delta (relative to current output position).
        let block_start_delta = if block_start_bits > 0 {
            reader.read_bits(block_start_bits + 4)? as u64
        } else {
            0
        };
        let block_start = output_size + block_start_delta;

        // Read block length (always present).
        let block_length_bits = reader.read_bits(4)? as u8;
        let block_length = if block_length_bits > 0 {
            reader.read_bits(block_length_bits + 4)? as usize
        } else {
            0
        };

        // For DELTA filter, read channel count.
        let channels = if filter_type == FilterType::Delta && use_counts != 0 {
            (reader.read_bits(5)? + 1) as u8
        } else if filter_type == FilterType::Delta {
            1
        } else {
            0
        };

        trace!(
            "filter at output offset {}: type={:?}, block_start={}, block_length={}, channels={}",
            output_size,
            filter_type,
            block_start,
            block_length,
            channels
        );

        self.pending_filters.push(PendingFilter {
            filter_type,
            block_start,
            block_length,
            channels,
        });

        Ok(output_size)
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

        let mut reader = BitReader::new(input);
        let mut output_size: u64 = 0;

        while output_size < unpacked_size {
            if self.block_bits_remaining <= 0 {
                if reader.bits_remaining() < 16 {
                    break;
                }
                self.read_block_header(&mut reader)?;
            }

            output_size = self.decode_block(&mut reader, unpacked_size, output_size)?;

            // Flush window periodically — but only up to filter boundaries.
            if self.pending_filters.is_empty() {
                self.window.flush_to_writer(writer).map_err(RarError::Io)?;
            }
        }

        // Apply any remaining filters and flush.
        self.flush_filters_and_write(writer)?;

        Ok(output_size)
    }

    /// Flush pending filters and remaining window data to a writer.
    fn flush_filters_and_write<W: Write>(
        &mut self,
        writer: &mut W,
    ) -> RarResult<()> {
        if self.pending_filters.is_empty() {
            self.window.flush_to_writer(writer).map_err(RarError::Io)?;
            return Ok(());
        }

        let total = self.window.total_written();
        let unflushed = self.window.unflushed_bytes();
        if unflushed == 0 {
            self.pending_filters.clear();
            return Ok(());
        }

        let flushed_total = total - unflushed;
        let mut buf = self.window.copy_output(flushed_total, unflushed as usize);

        self.apply_filters(&mut buf, flushed_total);

        writer.write_all(&buf).map_err(RarError::Io)?;
        self.window.mark_flushed(total);

        Ok(())
    }

    /// Reset the decoder for a new file (non-solid mode).
    pub fn reset(&mut self) {
        self.window.reset();
        self.dist_cache = [0; DIST_CACHE_SIZE];
        self.last_length = 0;
        self.nc_table = None;
        self.dc_table = None;
        self.ldc_table = None;
        self.rc_table = None;
        self.code_lengths.fill(0);
        self.block_bits_remaining = 0;
        self.is_last_block = false;
        self.pending_filters.clear();
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
    if info.dict_size > MAX_DICT_SIZE {
        return Err(RarError::DictionaryTooLarge {
            size: info.dict_size,
            max: MAX_DICT_SIZE,
        });
    }

    let dict_size = info.dict_size as usize;
    let mut decoder = LzDecoder::new(dict_size);
    decoder.decompress(input, unpacked_size)
}

/// Streaming variant: decompress LZ data directly to a writer.
///
/// Memory usage is bounded to the dictionary size (max 256 MB) instead of
/// accumulating the full output in memory.
pub fn decompress_lz_to_writer<W: Write>(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    writer: &mut W,
) -> RarResult<u64> {
    if info.dict_size > MAX_DICT_SIZE {
        return Err(RarError::DictionaryTooLarge {
            size: info.dict_size,
            max: MAX_DICT_SIZE,
        });
    }

    let dict_size = info.dict_size as usize;
    let mut decoder = LzDecoder::new(dict_size);
    decoder.decompress_to_writer(input, unpacked_size, writer)
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
        let decoder = LzDecoder::new(128 * 1024);
        assert_eq!(decoder.window.dict_size(), 128 * 1024);
        assert_eq!(decoder.dist_cache, [0; 4]);
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
        let mut decoder = LzDecoder::new(128 * 1024);
        decoder.dist_cache = [10, 20, 30, 40];
        decoder.block_bits_remaining = 100;
        decoder.reset();
        assert_eq!(decoder.dist_cache, [0; 4]);
        assert_eq!(decoder.block_bits_remaining, 0);
        assert!(decoder.nc_table.is_none());
    }

    #[test]
    fn test_prepare_solid_continuation() {
        let mut decoder = LzDecoder::new(128 * 1024);
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
