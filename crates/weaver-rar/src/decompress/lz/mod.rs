//! LZ block decompression orchestrator for RAR5.
//!
//! Reads compressed data as a bitstream of LZ blocks. Each block contains
//! Huffman-encoded symbols that represent either literal bytes or
//! length-distance pairs referencing previous output.
//!
//! Block structure:
//! - First bit of the compressed stream selects algorithm: 0 = LZ, 1 = PPMd
//! - `keepOldTable` flag (1 bit): if 0, read new Huffman tables
//! - Huffman-encoded symbol stream until block end
//!
//! Symbol interpretation (NC table, 306 symbols):
//! - 0-255: literal bytes
//! - 256: filter marker
//! - 257: last-distance match with length from next symbol
//! - 258-262: repeat distance cache references
//! - 263-305: length codes with extra bits

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

/// Length base values for length codes 263-305 (indices 0-42).
/// Length code N (symbol 263+N) has base length LENGTH_BASE[N] and
/// LENGTH_EXTRA_BITS[N] additional bits to read.
///
/// These values are derived from the RAR5 spec and libarchive reference.
static LENGTH_BASE: [u32; 43] = [
    2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 20, 24, 28, 36, 44, 52, 68, 84, 100, 132, 164, 196,
    260, 324, 388, 516, 644, 772, 1028, 1284, 1540, 2052, 2564, 3076, 4100, 5124, 6148, 8196,
    10244, 12292, 16388,
];

static LENGTH_EXTRA_BITS: [u8; 43] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8,
    8, 8, 9, 9, 9, 10, 10, 10, 11, 11, 11, 12, 12,
];

/// Distance base values for distance codes 0-63.
static DISTANCE_BASE: [u32; 64] = [
    0, 1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536,
    2048, 3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768, 49152, 65536, 98304, 131072,
    196608, 262144, 393216, 524288, 786432, 1048576, 1572864, 2097152, 3145728, 4194304, 6291456,
    8388608, 12582912, 16777216, 25165824, 33554432, 50331648, 67108864, 100663296, 134217728,
    201326592, 268435456, 402653184, 536870912, 805306368, 1073741824, 1610612736, 2147483648,
    3221225472,
];

static DISTANCE_EXTRA_BITS: [u8; 64] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12,
    13, 13, 14, 14, 15, 15, 16, 16, 17, 17, 18, 18, 19, 19, 20, 20, 21, 21, 22, 22, 23, 23, 24,
    24, 25, 25, 26, 26, 27, 27, 28, 28, 29, 29, 30, 30,
];

/// Number of entries in the last-distance cache.
const DIST_CACHE_SIZE: usize = 4;

/// State for the LZ decompressor.
pub struct LzDecoder {
    /// Sliding window / ring buffer.
    window: Window,
    /// Last-distance cache (4 entries for repeat matches).
    dist_cache: [usize; DIST_CACHE_SIZE],
    /// Current Huffman tables (kept across blocks when keepOldTable is set).
    nc_table: Option<HuffmanTable>,
    dc_table: Option<HuffmanTable>,
    ldc_table: Option<HuffmanTable>,
    rc_table: Option<HuffmanTable>,
    /// Whether we've read the initial block type bit.
    block_started: bool,
    /// Filters pending application to ranges of the current output.
    pending_filters: Vec<PendingFilter>,
    /// PPMd decoder for PPMd blocks (lazy-initialized on first PPMd block).
    ppmd_decoder: Option<super::ppmd::PpmdDecoder>,
}

impl LzDecoder {
    /// Create a new LZ decoder with the specified dictionary size.
    pub fn new(dict_size: usize) -> Self {
        Self {
            window: Window::new(dict_size),
            dist_cache: [0; DIST_CACHE_SIZE],
            nc_table: None,
            dc_table: None,
            ldc_table: None,
            rc_table: None,
            block_started: false,
            pending_filters: Vec::new(),
            ppmd_decoder: None,
        }
    }

    /// Decompress all LZ-compressed data from the input into the output buffer.
    ///
    /// `input` is the raw compressed data (data area from the file header).
    /// `unpacked_size` is the expected uncompressed size.
    /// Returns the decompressed data.
    pub fn decompress(&mut self, input: &[u8], unpacked_size: u64) -> RarResult<Vec<u8>> {
        let mut reader = BitReader::new(input);
        let mut output_size: u64 = 0;

        while output_size < unpacked_size {
            if reader.bits_remaining() < 2 {
                break;
            }

            // Read block type bit if this is the first block.
            if !self.block_started {
                let block_type = reader.read_bit()?;
                if block_type == 1 {
                    // PPMd block: hand off remaining data to PPMd decoder.
                    let ppmd = self.ppmd_decoder.get_or_insert_with(super::ppmd::PpmdDecoder::new);
                    let remaining_bytes = reader.remaining_bytes();
                    let ppmd_remaining = unpacked_size - output_size;
                    let mut ppmd_output = Vec::new();
                    ppmd.decode_block(remaining_bytes, ppmd_remaining, &mut ppmd_output)?;

                    // Write PPMd output to the window (for potential LZ backreferences
                    // in subsequent blocks) and accumulate output.
                    for &b in &ppmd_output {
                        self.window.put_byte(b);
                    }

                    // PPMd consumed the rest of this block's data.
                    self.block_started = false;
                    break;
                }
                self.block_started = true;
            }

            // Read keepOldTable flag.
            let keep_old = reader.read_bit()?;

            if keep_old == 0 || self.nc_table.is_none() {
                // Read new Huffman tables.
                let (nc, dc, ldc, rc) = huffman::read_tables(&mut reader)?;
                self.nc_table = Some(nc);
                self.dc_table = Some(dc);
                self.ldc_table = Some(ldc);
                self.rc_table = Some(rc);
            }

            // Decode symbols until block end or unpacked_size reached.
            output_size = self.decode_block(&mut reader, unpacked_size, output_size)?;

            // After a block ends, the next block needs its type bit again.
            // In RAR5, blocks within the same compressed stream share the
            // initial block-type bit (all LZ or all PPMd). The keepOldTable
            // flag is per-block. According to the spec, the block type bit
            // only appears once at the very start.
        }

        // Extract output from the window.
        let total = self.window.total_written();
        let start = total.saturating_sub(unpacked_size);
        let len = (total - start) as usize;
        let mut output = self.window.copy_output(start, len);

        // Apply pending filters to the output buffer.
        for f in &self.pending_filters {
            if f.block_start >= start && f.block_length > 0 {
                let rel_start = (f.block_start - start) as usize;
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

        Ok(output)
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
        while output_size < unpacked_size {
            if reader.bits_remaining() < 1 {
                break;
            }

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
                // End of block marker - return to the outer loop
                // which will read the next block's keepOldTable bit.
                break;
            } else if sym >= 258 && sym <= 262 {
                // Repeat distance from cache.
                // sym 258 = cache[0], 259 = cache[1], etc.
                let cache_idx = (sym - 258) as usize;
                if cache_idx >= DIST_CACHE_SIZE {
                    return Err(RarError::CorruptArchive {
                        detail: format!("invalid distance cache index: {cache_idx}"),
                    });
                }

                let distance = self.dist_cache[cache_idx];
                if distance == 0 {
                    return Err(RarError::CorruptArchive {
                        detail: "distance cache entry is zero".into(),
                    });
                }

                // Read length from rc table.
                let rc = self.rc_table.as_ref().unwrap();
                let length = self.decode_repeat_length(reader, rc)?;

                // Promote this cache entry to front.
                if cache_idx > 0 {
                    let dist = self.dist_cache[cache_idx];
                    for i in (1..=cache_idx).rev() {
                        self.dist_cache[i] = self.dist_cache[i - 1];
                    }
                    self.dist_cache[0] = dist;
                }

                trace!("repeat match: dist={}, len={}", distance, length);
                self.window.copy(distance, length)?;
                output_size += length as u64;
            } else if sym >= 263 && sym <= 305 {
                // Length-distance pair.
                let length_idx = (sym - 263) as usize;
                let length = self.decode_length(reader, length_idx)?;
                let dc = self.dc_table.as_ref().unwrap();
                let ldc = self.ldc_table.as_ref().unwrap();
                let distance = self.decode_distance(reader, dc, ldc, length)?;

                // Update distance cache.
                self.dist_cache[3] = self.dist_cache[2];
                self.dist_cache[2] = self.dist_cache[1];
                self.dist_cache[1] = self.dist_cache[0];
                self.dist_cache[0] = distance;

                trace!("match: dist={}, len={}", distance, length);
                self.window.copy(distance, length)?;
                output_size += length as u64;
            } else {
                return Err(RarError::CorruptArchive {
                    detail: format!("invalid NC symbol: {sym}"),
                });
            }
        }

        Ok(output_size)
    }

    /// Decode a length value from a length code index (0-42).
    fn decode_length(&self, reader: &mut BitReader, idx: usize) -> RarResult<usize> {
        if idx >= LENGTH_BASE.len() {
            return Err(RarError::CorruptArchive {
                detail: format!("length code index out of range: {idx}"),
            });
        }
        let base = LENGTH_BASE[idx] as usize;
        let extra = LENGTH_EXTRA_BITS[idx];
        let extra_val = if extra > 0 {
            reader.read_bits(extra)? as usize
        } else {
            0
        };
        Ok(base + extra_val)
    }

    /// Decode a repeat-match length from the RC table.
    fn decode_repeat_length(
        &self,
        reader: &mut BitReader,
        rc: &HuffmanTable,
    ) -> RarResult<usize> {
        let sym = rc.decode(reader)? as usize;
        if sym >= LENGTH_BASE.len() {
            // For RC symbols that exceed our table, treat as base length.
            return Ok(sym + 2);
        }
        let base = LENGTH_BASE[sym] as usize;
        let extra = LENGTH_EXTRA_BITS[sym];
        let extra_val = if extra > 0 {
            reader.read_bits(extra)? as usize
        } else {
            0
        };
        Ok(base + extra_val)
    }

    /// Decode a distance value from the DC and LDC tables.
    fn decode_distance(
        &self,
        reader: &mut BitReader,
        dc: &HuffmanTable,
        ldc: &HuffmanTable,
        length: usize,
    ) -> RarResult<usize> {
        let dist_code = dc.decode(reader)? as usize;
        if dist_code >= DISTANCE_BASE.len() {
            return Err(RarError::CorruptArchive {
                detail: format!("distance code out of range: {dist_code}"),
            });
        }

        let base = DISTANCE_BASE[dist_code] as usize;
        let extra_bits = DISTANCE_EXTRA_BITS[dist_code];

        let distance = if extra_bits > 0 {
            // For short distances (distance code < 4), the lower bits come
            // from the LDC table. For larger distances, extra bits come
            // from the bitstream directly.
            if dist_code < 4 && length == 2 {
                // Use LDC table for lower bits.
                let low = ldc.decode(reader)? as usize;
                base + low
            } else {
                let extra = reader.read_bits(extra_bits)? as usize;
                base + extra
            }
        } else {
            base
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
        let flags = reader.read_bits(8)? as u32;
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
        let mut reader = BitReader::new(input);
        let mut output_size: u64 = 0;

        while output_size < unpacked_size {
            if reader.bits_remaining() < 2 {
                break;
            }

            if !self.block_started {
                let block_type = reader.read_bit()?;
                if block_type == 1 {
                    // PPMd block — decode and write directly.
                    let ppmd = self.ppmd_decoder.get_or_insert_with(super::ppmd::PpmdDecoder::new);
                    let remaining_bytes = reader.remaining_bytes();
                    let ppmd_remaining = unpacked_size - output_size;
                    let mut ppmd_output = Vec::new();
                    ppmd.decode_block(remaining_bytes, ppmd_remaining, &mut ppmd_output)?;

                    for &b in &ppmd_output {
                        self.window.put_byte(b);
                    }
                    self.window.flush_to_writer(writer).map_err(RarError::Io)?;

                    self.block_started = false;
                    break;
                }
                self.block_started = true;
            }

            let keep_old = reader.read_bit()?;
            if keep_old == 0 || self.nc_table.is_none() {
                let (nc, dc, ldc, rc) = huffman::read_tables(&mut reader)?;
                self.nc_table = Some(nc);
                self.dc_table = Some(dc);
                self.ldc_table = Some(ldc);
                self.rc_table = Some(rc);
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
            // No filters — just flush remaining window data.
            self.window.flush_to_writer(writer).map_err(RarError::Io)?;
            return Ok(());
        }

        // When filters are present, we need the full output to apply them
        // (filters reference absolute positions). Extract unflushed data,
        // apply filters, and write.
        let total = self.window.total_written();

        // We need to extract unflushed data from the window for filter application.
        let unflushed = self.window.unflushed_bytes();
        if unflushed == 0 {
            self.pending_filters.clear();
            return Ok(());
        }

        // The absolute position of the first unflushed byte.
        let flushed_total = total - unflushed;

        // Extract unflushed data from window.
        let mut buf = self.window.copy_output(flushed_total, unflushed as usize);

        // Apply filters to the buffer (adjusting offsets relative to the buffer start).
        for f in &self.pending_filters {
            if f.block_start >= flushed_total && f.block_length > 0 {
                let rel_start = (f.block_start - flushed_total) as usize;
                let rel_end = rel_start + f.block_length;
                if rel_end <= buf.len() {
                    let block = &mut buf[rel_start..rel_end];
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

        writer.write_all(&buf).map_err(RarError::Io)?;
        // Mark as flushed so we don't double-write.
        self.window.mark_flushed(total);

        Ok(())
    }

    /// Reset the decoder for a new file (non-solid mode).
    ///
    /// Clears the window, distance cache, Huffman tables, and block state.
    pub fn reset(&mut self) {
        self.window.reset();
        self.dist_cache = [0; DIST_CACHE_SIZE];
        self.nc_table = None;
        self.dc_table = None;
        self.ldc_table = None;
        self.rc_table = None;
        self.block_started = false;
        self.pending_filters.clear();
        self.ppmd_decoder = None;
    }

    /// Prepare for the next member in a solid archive.
    ///
    /// In solid mode, the sliding window (dictionary) carries over between
    /// files, enabling cross-file back-references. The distance cache and
    /// Huffman tables also persist. Only the block_started flag is reset
    /// because each member has its own compressed bitstream.
    pub fn prepare_solid_continuation(&mut self) {
        self.block_started = false;
        self.pending_filters.clear();
        // PPMd state carries over in solid mode (model retains learned contexts).
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
    // Validate dictionary size.
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
    fn test_length_base_table() {
        // Verify first few entries make sense.
        assert_eq!(LENGTH_BASE[0], 2); // code 263: length 2
        assert_eq!(LENGTH_BASE[1], 3); // code 264: length 3
        assert_eq!(LENGTH_EXTRA_BITS[0], 0); // no extra bits for length 2
    }

    #[test]
    fn test_distance_base_table() {
        assert_eq!(DISTANCE_BASE[0], 0);
        assert_eq!(DISTANCE_BASE[1], 1);
        assert_eq!(DISTANCE_BASE[2], 2);
        assert_eq!(DISTANCE_BASE[3], 3);
        assert_eq!(DISTANCE_BASE[4], 4);
        assert_eq!(DISTANCE_EXTRA_BITS[0], 0);
        assert_eq!(DISTANCE_EXTRA_BITS[4], 1);
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
    fn test_ppmd_block_dispatched() {
        // First bit = 1 indicates PPMd block. With empty remaining data,
        // decode_block returns Ok(0) and the decompressor finishes with empty output.
        let data = [0x80]; // MSB first: bit 1 = PPMd
        let info = CompressionInfo {
            version: 0,
            solid: false,
            method: crate::types::CompressionMethod::Normal,
            dict_size: 128 * 1024,
        };
        let result = decompress_lz(&data, 100, &info);
        // With no PPMd data, we get an empty result (no error since block is empty).
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_decoder_reset() {
        let mut decoder = LzDecoder::new(128 * 1024);
        decoder.dist_cache = [10, 20, 30, 40];
        decoder.block_started = true;
        decoder.reset();
        assert_eq!(decoder.dist_cache, [0; 4]);
        assert!(!decoder.block_started);
        assert!(decoder.nc_table.is_none());
    }

    #[test]
    fn test_prepare_solid_continuation() {
        let mut decoder = LzDecoder::new(128 * 1024);
        decoder.dist_cache = [10, 20, 30, 40];
        decoder.block_started = true;
        // Write some data to the window to simulate previous decompression.
        decoder.window.put_byte(0xAA);
        decoder.window.put_byte(0xBB);

        decoder.prepare_solid_continuation();

        // block_started should be reset.
        assert!(!decoder.block_started);
        // dist_cache should be preserved.
        assert_eq!(decoder.dist_cache, [10, 20, 30, 40]);
        // Window state should be preserved.
        assert_eq!(decoder.window.total_written(), 2);
        assert_eq!(decoder.window.get_byte(1), 0xBB);
        assert_eq!(decoder.window.get_byte(2), 0xAA);
    }

    #[test]
    fn test_length_extra_bits_monotonic() {
        // Extra bits should be non-decreasing
        for i in 1..LENGTH_EXTRA_BITS.len() {
            assert!(
                LENGTH_EXTRA_BITS[i] >= LENGTH_EXTRA_BITS[i - 1],
                "LENGTH_EXTRA_BITS[{}] = {} < LENGTH_EXTRA_BITS[{}] = {}",
                i, LENGTH_EXTRA_BITS[i], i - 1, LENGTH_EXTRA_BITS[i - 1]
            );
        }
    }

    #[test]
    fn test_distance_extra_bits_monotonic() {
        // Extra bits should be non-decreasing
        for i in 1..DISTANCE_EXTRA_BITS.len() {
            assert!(
                DISTANCE_EXTRA_BITS[i] >= DISTANCE_EXTRA_BITS[i - 1],
                "DISTANCE_EXTRA_BITS[{}] = {} < DISTANCE_EXTRA_BITS[{}] = {}",
                i, DISTANCE_EXTRA_BITS[i], i - 1, DISTANCE_EXTRA_BITS[i - 1]
            );
        }
    }

    #[test]
    fn test_length_base_monotonic() {
        // Length base values should be strictly increasing
        for i in 1..LENGTH_BASE.len() {
            assert!(
                LENGTH_BASE[i] > LENGTH_BASE[i - 1],
                "LENGTH_BASE[{}] = {} <= LENGTH_BASE[{}] = {}",
                i, LENGTH_BASE[i], i - 1, LENGTH_BASE[i - 1]
            );
        }
    }

    #[test]
    fn test_distance_base_monotonic() {
        // Distance base values should be strictly increasing
        for i in 1..DISTANCE_BASE.len() {
            assert!(
                DISTANCE_BASE[i] > DISTANCE_BASE[i - 1],
                "DISTANCE_BASE[{}] = {} <= DISTANCE_BASE[{}] = {}",
                i, DISTANCE_BASE[i], i - 1, DISTANCE_BASE[i - 1]
            );
        }
    }

    #[test]
    fn test_all_compression_methods_accepted() {
        // Methods 1-5 should all be accepted (they all use LZ)
        for method_code in 1..=5u8 {
            let info = CompressionInfo {
                version: 0,
                solid: false,
                method: crate::types::CompressionMethod::from_code(method_code),
                dict_size: 128 * 1024,
            };
            // Empty input with 0 unpack size should succeed
            let result = decompress_lz(&[], 0, &info);
            assert!(result.is_ok(), "method {} failed", method_code);
        }
    }
}
