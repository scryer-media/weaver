//! RAR4 (v29) LZ decompressor.
//!
//! Implements the Unpack29 algorithm used by RAR 3.x/4.x archives.
//!
//! Symbol interpretation (LD table, 299 symbols):
//! - 0-255: literal bytes
//! - 256: end of block
//! - 257: VM filter code (read and skip)
//! - 258: repeat previous match (last_length, dist_cache[0])
//! - 259-262: repeat distance cache references (new length from RD table)
//! - 263-270: short distance matches (length=2)
//! - 271-298: inline length codes with extra bits (distance from DD/LDD tables)

use std::io::Write;

use tracing::{trace, warn};

use super::lz::bitstream::BitReader;
use super::lz::huffman::HuffmanTable;
use super::lz::window::Window;
use super::ppmd::model::Model;
use super::ppmd::range::RangeDecoder;
use crate::error::{RarError, RarResult};

/// RAR4 Huffman table sizes.
const NC: usize = 299; // Literal/Length codes
const DC: usize = 60; // Distance codes
const LDC: usize = 17; // Low distance codes
const RC: usize = 28; // Repeat/Length codes
const BC: usize = 20; // Bit length codes

/// Total symbols across all tables (for delta encoding persistence).
const HUFF_TABLE_SIZE: usize = NC + DC + LDC + RC;

/// Length decode base values (28 entries).
const LDECODE: [u16; 28] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112, 128,
    160, 192, 224,
];

/// Length extra bits (28 entries).
const LBITS: [u8; 28] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5,
];

/// Short distance decode base values (8 entries, for symbols 263-270).
const SDDECODE: [u16; 8] = [0, 4, 8, 16, 32, 64, 128, 192];

/// Short distance extra bits.
const SDBITS: [u8; 8] = [2, 2, 3, 4, 5, 6, 6, 6];

/// DBitLengthCounts for building DDecode/DBits tables.
const DBIT_LENGTH_COUNTS: [usize; 19] = [4, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 14, 0, 12];

/// Low distance repeat count.
const LOW_DIST_REP_COUNT: usize = 16;

/// Maximum dictionary size (256 MB).
const MAX_DICT_SIZE: u64 = 256 * 1024 * 1024;

/// Block type: LZ or PPMd.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BlockType {
    Lz,
    Ppm,
}

/// Distance decode base values (computed from DBIT_LENGTH_COUNTS).
fn build_ddecode_tables() -> ([u32; DC], [u8; DC]) {
    let mut ddecode = [0u32; DC];
    let mut dbits = [0u8; DC];
    let mut dist: u32 = 0;
    let mut slot: usize = 0;
    for (bit_length, &count) in (0u8..).zip(DBIT_LENGTH_COUNTS.iter()) {
        for _ in 0..count {
            if slot < DC {
                ddecode[slot] = dist;
                dbits[slot] = bit_length;
                dist += 1u32 << bit_length;
                slot += 1;
            }
        }
    }
    (ddecode, dbits)
}

/// State for the RAR4 LZ decompressor.
pub struct Rar4LzDecoder {
    /// Sliding window / ring buffer.
    window: Window,
    /// Last-distance cache (4 entries for repeat matches).
    dist_cache: [usize; 4],
    /// Last match length (for symbol 258 repeat).
    last_length: usize,
    /// Huffman tables.
    ld_table: Option<HuffmanTable>,
    dd_table: Option<HuffmanTable>,
    ldd_table: Option<HuffmanTable>,
    rd_table: Option<HuffmanTable>,
    /// Code lengths for delta encoding persistence across blocks.
    code_lengths: Vec<u8>,
    /// Distance decode tables (built once).
    ddecode: [u32; DC],
    dbits: [u8; DC],
    /// Low distance state.
    low_dist_rep_count: usize,
    prev_low_dist: usize,
    /// Current block type (LZ or PPMd).
    block_type: BlockType,
    /// PPMd model (persists across PPMd blocks within a file).
    ppm_model: Option<Model>,
    /// PPMd escape character (default 2).
    ppm_esc_char: u8,
    /// Equivalent to unrar's TablesRead3 flag.
    tables_read: bool,
}

impl Rar4LzDecoder {
    /// Create a new RAR4 LZ decoder with the specified dictionary size.
    pub fn new(dict_size: usize) -> Self {
        let (ddecode, dbits) = build_ddecode_tables();
        Self {
            window: Window::new(dict_size),
            dist_cache: [0; 4],
            last_length: 0,
            ld_table: None,
            dd_table: None,
            ldd_table: None,
            rd_table: None,
            code_lengths: vec![0u8; HUFF_TABLE_SIZE],
            ddecode,
            dbits,
            low_dist_rep_count: 0,
            prev_low_dist: 0,
            block_type: BlockType::Lz,
            ppm_model: None,
            ppm_esc_char: 2,
            tables_read: false,
        }
    }

    /// Decompress RAR4 LZ data, returning the decompressed output.
    pub fn decompress(&mut self, input: &[u8], unpacked_size: u64) -> RarResult<Vec<u8>> {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        let mut reader = BitReader::new(input);

        if !self.tables_read {
            self.read_tables(&mut reader)?;
        }

        let mut output_size: u64 = 0;

        while output_size < unpacked_size {
            if reader.bits_remaining() < 1 {
                break;
            }

            match self.block_type {
                BlockType::Lz => {
                    output_size =
                        self.decode_lz_symbols(&mut reader, unpacked_size, output_size)?;
                }
                BlockType::Ppm => {
                    output_size =
                        self.decode_ppm_symbols(&mut reader, unpacked_size, output_size)?;
                }
            }
        }

        let total = output_size.min(unpacked_size) as usize;
        let start = self.window.total_written() - total as u64;
        Ok(self.window.copy_output(start, total))
    }

    /// Decode a RAR4 solid member only to advance decoder state.
    pub fn replay(&mut self, input: &[u8], unpacked_size: u64) -> RarResult<u64> {
        if unpacked_size == 0 {
            return Ok(0);
        }

        let mut reader = BitReader::new(input);

        if !self.tables_read {
            self.read_tables(&mut reader)?;
        }

        let mut output_size: u64 = 0;

        while output_size < unpacked_size {
            if reader.bits_remaining() < 1 {
                break;
            }

            match self.block_type {
                BlockType::Lz => {
                    output_size =
                        self.decode_lz_symbols(&mut reader, unpacked_size, output_size)?;
                }
                BlockType::Ppm => {
                    output_size =
                        self.decode_ppm_symbols(&mut reader, unpacked_size, output_size)?;
                }
            }
        }

        Ok(output_size.min(unpacked_size))
    }

    /// Decompress RAR4 LZ data directly to a writer.
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

        if !self.tables_read {
            self.read_tables(&mut reader)?;
        }

        let mut output_size: u64 = 0;
        let flush_threshold = self.window.dict_size() / 2;
        let decode_chunk = flush_threshold.max(1) as u64;

        while output_size < unpacked_size {
            if reader.bits_remaining() < 1 {
                break;
            }

            let target_output = output_size.saturating_add(decode_chunk).min(unpacked_size);

            match self.block_type {
                BlockType::Lz => {
                    output_size =
                        self.decode_lz_symbols(&mut reader, target_output, output_size)?;
                }
                BlockType::Ppm => {
                    output_size =
                        self.decode_ppm_symbols(&mut reader, target_output, output_size)?;
                }
            }

            if self.window.unflushed_bytes() as usize >= flush_threshold {
                self.window.flush_to_writer(writer).map_err(RarError::Io)?;
            }
        }

        self.window.flush_to_writer(writer).map_err(RarError::Io)?;
        Ok(output_size)
    }

    /// Chunked variant: decompress with output split at compressed byte boundaries.
    ///
    /// Same as `decompress_to_writer` but switches output writers when the
    /// compressed byte position crosses volume boundaries.
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

        let mut reader = BitReader::new(input);
        if !self.tables_read {
            self.read_tables(&mut reader)?;
        }

        let mut output_size: u64 = 0;
        let flush_threshold = self.window.dict_size() / 2;
        let decode_chunk = flush_threshold.max(1) as u64;
        let mut boundary_idx = 0;

        let mut chunks: Vec<(usize, u64)> = Vec::new();
        let mut current_vol = first_volume_index;
        let mut current_writer = writer_factory(current_vol)?;
        let mut chunk_bytes: u64 = 0;

        while output_size < unpacked_size {
            if reader.bits_remaining() < 1 {
                break;
            }

            let prev_output = output_size;
            let target_output = output_size.saturating_add(decode_chunk).min(unpacked_size);
            match self.block_type {
                BlockType::Lz => {
                    output_size =
                        self.decode_lz_symbols(&mut reader, target_output, output_size)?;
                }
                BlockType::Ppm => {
                    output_size =
                        self.decode_ppm_symbols(&mut reader, target_output, output_size)?;
                }
            }
            let decoded_this_round = output_size - prev_output;

            let byte_pos = reader.byte_position() as u64;
            if boundary_idx < boundaries.len()
                && byte_pos >= boundaries[boundary_idx].compressed_offset
            {
                self.window
                    .flush_to_writer(&mut *current_writer)
                    .map_err(RarError::Io)?;
                chunk_bytes += decoded_this_round;
                chunks.push((current_vol, chunk_bytes));

                current_vol = boundaries[boundary_idx].volume_index;
                boundary_idx += 1;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
            } else {
                chunk_bytes += decoded_this_round;
                if self.window.unflushed_bytes() as usize >= flush_threshold {
                    self.window
                        .flush_to_writer(&mut *current_writer)
                        .map_err(RarError::Io)?;
                }
            }
        }

        self.window
            .flush_to_writer(&mut *current_writer)
            .map_err(RarError::Io)?;
        if chunk_bytes > 0 || chunks.is_empty() {
            chunks.push((current_vol, chunk_bytes));
        }

        Ok(chunks)
    }

    /// Read Huffman tables from the bitstream (ReadTables30 equivalent).
    ///
    /// Byte-aligns first, then reads:
    /// 1. Block type flag (PPM not supported, LZ only)
    /// 2. Table inheritance flag
    /// 3. BC code lengths (20 x 4-bit, with 15+zero_count special case)
    /// 4. Main code lengths using BC table (delta encoded)
    /// 5. Builds LD, DD, LDD, RD tables
    fn read_tables(&mut self, reader: &mut BitReader) -> RarResult<()> {
        // Align to byte boundary.
        reader.align_byte();

        if reader.bits_remaining() < 2 {
            return Err(RarError::CorruptArchive {
                detail: "RAR4: not enough data for table header".into(),
            });
        }

        // Bit 0: PPM flag (1=PPM block, 0=LZ block).
        let ppm_flag = reader.read_bits(1)?;
        if ppm_flag != 0 {
            self.block_type = BlockType::Ppm;
            return self.init_ppm(reader);
        }

        self.block_type = BlockType::Lz;

        // Bit 1: inherit previous tables (1=keep, 0=reset).
        let inherit = reader.read_bits(1)?;
        if inherit == 0 {
            self.code_lengths.fill(0);
        }

        // Reset low distance state on new tables.
        self.prev_low_dist = 0;
        self.low_dist_rep_count = 0;

        // Read BC table: 20 x 4-bit lengths.
        let mut bc_lengths = [0u8; BC];
        let mut i = 0;
        while i < BC {
            if reader.bits_remaining() < 4 {
                return Err(RarError::CorruptArchive {
                    detail: "RAR4: truncated BC table".into(),
                });
            }
            let length = reader.read_bits(4)? as u8;
            if length == 15 {
                let zero_count = reader.read_bits(4)? as usize;
                if zero_count == 0 {
                    // Literal 15.
                    bc_lengths[i] = 15;
                } else {
                    // Zero fill: zero_count + 2 entries.
                    let fill = zero_count + 2;
                    for _ in 0..fill {
                        if i < BC {
                            bc_lengths[i] = 0;
                            i += 1;
                        }
                    }
                    continue; // i already advanced
                }
            } else {
                bc_lengths[i] = length;
            }
            i += 1;
        }
        let bc_table = HuffmanTable::build(&bc_lengths)?;

        // Read main code lengths using BC table (delta encoded).
        let mut i = 0;
        while i < HUFF_TABLE_SIZE {
            if reader.bits_remaining() < 1 {
                break;
            }
            let number = bc_table.decode(reader)? as usize;
            if number < 16 {
                // Delta: add to previous value mod 16.
                self.code_lengths[i] = ((self.code_lengths[i] as usize + number) & 0xF) as u8;
                i += 1;
            } else if number < 18 {
                // Repeat previous value.
                let count = if number == 16 {
                    reader.read_bits(3)? as usize + 3
                } else {
                    reader.read_bits(7)? as usize + 11
                };
                if i == 0 {
                    return Err(RarError::CorruptArchive {
                        detail: "RAR4: repeat-previous at position 0".into(),
                    });
                }
                let prev = self.code_lengths[i - 1];
                for _ in 0..count {
                    if i >= HUFF_TABLE_SIZE {
                        break;
                    }
                    self.code_lengths[i] = prev;
                    i += 1;
                }
            } else {
                // Zero fill.
                let count = if number == 18 {
                    reader.read_bits(3)? as usize + 3
                } else {
                    reader.read_bits(7)? as usize + 11
                };
                for _ in 0..count {
                    if i >= HUFF_TABLE_SIZE {
                        break;
                    }
                    self.code_lengths[i] = 0;
                    i += 1;
                }
            }
        }

        // Build the four main tables.
        let mut offset = 0;
        self.ld_table = Some(HuffmanTable::build(
            &self.code_lengths[offset..offset + NC],
        )?);
        offset += NC;
        self.dd_table = Some(HuffmanTable::build(
            &self.code_lengths[offset..offset + DC],
        )?);
        offset += DC;
        self.ldd_table = Some(HuffmanTable::build(
            &self.code_lengths[offset..offset + LDC],
        )?);
        offset += LDC;
        self.rd_table = Some(HuffmanTable::build(
            &self.code_lengths[offset..offset + RC],
        )?);

        self.tables_read = true;
        Ok(())
    }

    /// Main decode loop — processes symbols until unpacked_size is reached
    /// or end-of-block.
    fn decode_lz_symbols(
        &mut self,
        reader: &mut BitReader,
        unpacked_size: u64,
        mut output_size: u64,
    ) -> RarResult<u64> {
        while output_size < unpacked_size {
            if !reader.has_bits() {
                break;
            }

            let number = self.ld_table.as_ref().unwrap().decode(reader)? as usize;

            if number < 256 {
                // Literal byte (most common — first).
                self.window.put_byte(number as u8);
                output_size += 1;
                continue;
            }

            if number >= 271 {
                // Regular match: decode length then distance.
                let length_idx = number - 271;
                if length_idx >= LDECODE.len() {
                    return Err(RarError::CorruptArchive {
                        detail: format!("RAR4: length index out of range: {length_idx}"),
                    });
                }
                let mut length = LDECODE[length_idx] as usize + 3;
                let lbits = LBITS[length_idx];
                if lbits > 0 {
                    length += reader.read_bits(lbits)? as usize;
                }

                let distance = self.decode_distance(reader)?;

                // Distance-based length adjustment.
                if distance >= 0x2000 {
                    length += 1;
                    if distance >= 0x40000 {
                        length += 1;
                    }
                }

                self.insert_old_dist(distance);
                self.last_length = length;
                let remaining = (unpacked_size - output_size) as usize;
                let copy_len = length.min(remaining);
                self.window.copy(distance, copy_len)?;
                output_size += copy_len as u64;
                continue;
            }

            if number == 256 {
                // End of block.
                let continue_decompressing = self.read_end_of_block(reader)?;
                if !continue_decompressing {
                    break;
                }
                continue;
            }

            if number == 257 {
                // VM filter code — read and skip.
                self.skip_vm_code(reader)?;
                continue;
            }

            if number == 258 {
                // Repeat previous match.
                if self.last_length != 0 {
                    let distance = self.dist_cache[0];
                    let remaining = (unpacked_size - output_size) as usize;
                    let copy_len = self.last_length.min(remaining);
                    if distance > 0 {
                        self.window.copy(distance, copy_len)?;
                        output_size += copy_len as u64;
                    }
                }
                continue;
            }

            if number < 263 {
                // Repeat distance from cache (259-262).
                let cache_idx = number - 259;
                let distance = self.dist_cache[cache_idx];
                if distance == 0 {
                    return Err(RarError::CorruptArchive {
                        detail: "RAR4: distance cache entry is zero".into(),
                    });
                }

                // Rotate cache.
                for j in (1..=cache_idx).rev() {
                    self.dist_cache[j] = self.dist_cache[j - 1];
                }
                self.dist_cache[0] = distance;

                // Decode length from RD table.
                let length_number = self.rd_table.as_ref().unwrap().decode(reader)? as usize;
                if length_number >= LDECODE.len() {
                    return Err(RarError::CorruptArchive {
                        detail: format!("RAR4: RD length index out of range: {length_number}"),
                    });
                }
                let mut length = LDECODE[length_number] as usize + 2; // +2 for cache refs
                let lbits = LBITS[length_number];
                if lbits > 0 {
                    length += reader.read_bits(lbits)? as usize;
                }

                self.last_length = length;
                let remaining = (unpacked_size - output_size) as usize;
                let copy_len = length.min(remaining);
                self.window.copy(distance, copy_len)?;
                output_size += copy_len as u64;
                continue;
            }

            if number < 272 {
                // Short match (263-270): length=2, decode short distance.
                let sd_idx = number - 263;
                let mut distance = SDDECODE[sd_idx] as usize + 1;
                let sd_bits = SDBITS[sd_idx];
                if sd_bits > 0 {
                    distance += reader.read_bits(sd_bits)? as usize;
                }

                self.insert_old_dist(distance);
                self.last_length = 2;
                let remaining = (unpacked_size - output_size) as usize;
                let copy_len = 2usize.min(remaining);
                self.window.copy(distance, copy_len)?;
                output_size += copy_len as u64;
                continue;
            }

            return Err(RarError::CorruptArchive {
                detail: format!("RAR4: invalid symbol: {number}"),
            });
        }

        Ok(output_size)
    }

    /// Decode a full distance value from DD and LDD tables.
    fn decode_distance(&mut self, reader: &mut BitReader) -> RarResult<usize> {
        let dist_number = self.dd_table.as_ref().unwrap().decode(reader)? as usize;
        if dist_number >= DC {
            return Err(RarError::CorruptArchive {
                detail: format!("RAR4: distance code out of range: {dist_number}"),
            });
        }

        let mut distance = self.ddecode[dist_number] as usize + 1;
        let bits = self.dbits[dist_number];

        if bits > 0 {
            if dist_number > 9 {
                // Complex case: high bits from bitstream, low 4 bits from LDD table.
                if bits > 4 {
                    let high_bits = bits - 4;
                    distance += (reader.read_bits(high_bits)? as usize) << 4;
                }

                if self.low_dist_rep_count > 0 {
                    self.low_dist_rep_count -= 1;
                    distance += self.prev_low_dist;
                } else {
                    let low_dist = self.ldd_table.as_ref().unwrap().decode(reader)? as usize;
                    if low_dist == 16 {
                        // Repeat previous low distance.
                        self.low_dist_rep_count = LOW_DIST_REP_COUNT - 1;
                        distance += self.prev_low_dist;
                    } else {
                        distance += low_dist;
                        self.prev_low_dist = low_dist;
                    }
                }
            } else {
                // Simple case: just read extra bits directly.
                distance += reader.read_bits(bits)? as usize;
            }
        }

        Ok(distance)
    }

    /// Insert a new distance into the old distance cache (rotate right).
    fn insert_old_dist(&mut self, distance: usize) {
        self.dist_cache[3] = self.dist_cache[2];
        self.dist_cache[2] = self.dist_cache[1];
        self.dist_cache[1] = self.dist_cache[0];
        self.dist_cache[0] = distance;
    }

    /// Handle end-of-block marker (symbol 256).
    ///
    /// Returns true if decompression should continue (new tables read),
    /// false if this is the end of file.
    fn read_end_of_block(&mut self, reader: &mut BitReader) -> RarResult<bool> {
        if reader.bits_remaining() < 1 {
            return Ok(false);
        }

        let bit = reader.read_bits(1)?;
        if bit != 0 {
            // "1" — no new file, new table immediately.
            self.tables_read = false;
            self.read_tables(reader)?;
            return Ok(true);
        }

        // "0x" — new file.
        if reader.bits_remaining() < 1 {
            return Ok(false);
        }
        let new_table_bit = reader.read_bits(1)?;
        self.tables_read = new_table_bit == 0;
        Ok(false)
    }

    /// Read and skip VM filter code (symbol 257).
    ///
    /// We don't implement the RarVM, but we must consume the data from the
    /// bitstream to stay synchronized.
    fn skip_vm_code(&self, reader: &mut BitReader) -> RarResult<()> {
        let first_byte = reader.read_bits(8)? as u8;
        let mut length = (first_byte & 7) as usize + 1;
        if length == 7 {
            length = reader.read_bits(8)? as usize + 7;
        } else if length == 8 {
            length = reader.read_bits(16)? as usize;
        }

        if length == 0 {
            return Err(RarError::CorruptArchive {
                detail: "RAR4: VM code length is 0".into(),
            });
        }

        // Skip the VM code bytes.
        for _ in 0..length {
            if reader.bits_remaining() < 8 {
                return Err(RarError::CorruptArchive {
                    detail: "RAR4: truncated VM code".into(),
                });
            }
            reader.read_bits(8)?;
        }

        warn!(
            "RAR4: VM filter skipped ({length} bytes) — output may be incorrect for filtered data"
        );
        Ok(())
    }

    /// Initialize PPMd block (DecodeInit equivalent from unrar).
    ///
    /// The PPM flag bit has already been consumed. The remaining 7 bits of that
    /// byte plus subsequent bytes form the DecodeInit header:
    /// - Bits 0-4: max order value
    /// - Bit 5: reset flag (reinitialize model)
    /// - Bit 6: new escape character flag
    /// - If reset: next byte = allocator size in MB
    /// - If bit 6: next byte = new escape character
    /// - Then the range coder reads its init bytes from the stream
    fn init_ppm(&mut self, reader: &mut BitReader) -> RarResult<()> {
        // The PPM flag (bit 7) was consumed as 1 bit. Read remaining 7 bits
        // to reconstruct the MaxOrder byte (bit 7 is always 1 but unused).
        if reader.bits_remaining() < 7 {
            return Err(RarError::CorruptArchive {
                detail: "RAR4: truncated PPMd init header".into(),
            });
        }
        let max_order_byte = reader.read_bits(7)? as u8;

        let reset = (max_order_byte & 0x20) != 0;
        let new_esc = (max_order_byte & 0x40) != 0;

        let max_mb = if reset {
            reader.read_bits(8)? as usize
        } else {
            if self.ppm_model.is_none() {
                return Err(RarError::CorruptArchive {
                    detail: "RAR4: PPMd block without model (no reset)".into(),
                });
            }
            0
        };

        if new_esc {
            self.ppm_esc_char = reader.read_bits(8)? as u8;
        }

        if reset {
            let mut order = (max_order_byte & 0x1F) as usize + 1;
            if order > 16 {
                order = 16 + (order - 16) * 3;
            }
            if order < 2 {
                return Err(RarError::CorruptArchive {
                    detail: "RAR4: PPMd order too small".into(),
                });
            }
            let alloc_size = (max_mb + 1) * 1024 * 1024;
            trace!("RAR4 PPMd init: order={order}, alloc={alloc_size}");
            self.ppm_model = Some(Model::new(order, alloc_size));
        }

        Ok(())
    }

    /// Decode PPMd symbols until block switch, EOF, or output complete.
    ///
    /// Creates a RangeDecoder from the remaining bitstream bytes, decodes
    /// symbols via the PPMd model, and handles escape sequences.
    fn decode_ppm_symbols(
        &mut self,
        reader: &mut BitReader,
        unpacked_size: u64,
        mut output_size: u64,
    ) -> RarResult<u64> {
        let remaining = reader.remaining_bytes();
        if remaining.len() < 4 {
            return Ok(output_size);
        }

        let mut rc = RangeDecoder::new(remaining)?;

        while output_size < unpacked_size {
            let model = match self.ppm_model.as_mut() {
                Some(m) => m,
                None => {
                    self.block_type = BlockType::Lz;
                    break;
                }
            };

            let ch = model.decode_char(&mut rc);
            if ch == -1 {
                // Corrupt PPM data — switch to LZ mode.
                self.ppm_model = None;
                self.block_type = BlockType::Lz;
                break;
            }

            let ch = ch as u8;
            if ch == self.ppm_esc_char {
                // Escape sequence — decode the command byte.
                let model = self.ppm_model.as_mut().unwrap();
                let next_ch = model.decode_char(&mut rc);
                if next_ch == -1 {
                    self.ppm_model = None;
                    self.block_type = BlockType::Lz;
                    break;
                }

                match next_ch {
                    0 => {
                        // End of PPM block — switch via ReadTables.
                        let consumed = rc.position();
                        reader.skip_bits((consumed * 8) as u32)?;
                        self.read_tables(reader)?;
                        return Ok(output_size);
                    }
                    2 => {
                        // End of file in PPM mode.
                        break;
                    }
                    3 => {
                        // VM filter in PPM mode — read and discard.
                        self.skip_vm_code_ppm(&mut rc)?;
                    }
                    4 => {
                        // LZ match inside PPM: 3 bytes distance (big-endian) + 1 byte length.
                        let mut distance: u32 = 0;
                        let mut length: u32 = 0;
                        let mut failed = false;
                        let model = self.ppm_model.as_mut().unwrap();
                        for i in 0..4 {
                            let b = model.decode_char(&mut rc);
                            if b == -1 {
                                failed = true;
                                break;
                            }
                            if i == 3 {
                                length = b as u32;
                            } else {
                                distance = (distance << 8) | (b as u32 & 0xFF);
                            }
                        }
                        if failed {
                            self.ppm_model = None;
                            self.block_type = BlockType::Lz;
                            break;
                        }
                        let copy_len = (length + 32) as usize;
                        let copy_dist = (distance + 2) as usize;
                        let remaining_out = (unpacked_size - output_size) as usize;
                        let actual = copy_len.min(remaining_out);
                        self.window.copy(copy_dist, actual)?;
                        output_size += actual as u64;
                    }
                    5 => {
                        // RLE match inside PPM: 1 byte length, distance=1.
                        let model = self.ppm_model.as_mut().unwrap();
                        let len_byte = model.decode_char(&mut rc);
                        if len_byte == -1 {
                            self.ppm_model = None;
                            self.block_type = BlockType::Lz;
                            break;
                        }
                        let copy_len = (len_byte as usize) + 4;
                        let remaining_out = (unpacked_size - output_size) as usize;
                        let actual = copy_len.min(remaining_out);
                        self.window.copy(1, actual)?;
                        output_size += actual as u64;
                    }
                    _ => {
                        // NextCh == 1 (or any other): literal escape character.
                        // The original `ch` (== ppm_esc_char) is the output byte.
                        self.window.put_byte(ch);
                        output_size += 1;
                    }
                }
            } else {
                // Regular literal byte.
                self.window.put_byte(ch);
                output_size += 1;
            }
        }

        // Advance the bitreader past all bytes consumed by the range decoder.
        let consumed = rc.position();
        reader.skip_bits((consumed * 8) as u32)?;

        Ok(output_size)
    }

    /// Read and skip VM filter code in PPM mode.
    ///
    /// Same structure as skip_vm_code but reads bytes via PPMd model
    /// instead of from the bitstream.
    fn skip_vm_code_ppm(&mut self, rc: &mut RangeDecoder) -> RarResult<()> {
        let model = self
            .ppm_model
            .as_mut()
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR4: PPMd model missing during VM skip".into(),
            })?;

        let first_byte = model.decode_char(rc);
        if first_byte == -1 {
            return Err(RarError::CorruptArchive {
                detail: "RAR4: PPMd corrupt during VM filter read".into(),
            });
        }
        let first_byte = first_byte as u8;

        let mut length = (first_byte & 7) as usize + 1;
        if length == 7 {
            let b = model.decode_char(rc);
            if b == -1 {
                return Err(RarError::CorruptArchive {
                    detail: "RAR4: PPMd corrupt during VM filter length".into(),
                });
            }
            length = (b as usize) + 7;
        } else if length == 8 {
            let b1 = model.decode_char(rc);
            let b2 = model.decode_char(rc);
            if b1 == -1 || b2 == -1 {
                return Err(RarError::CorruptArchive {
                    detail: "RAR4: PPMd corrupt during VM filter length".into(),
                });
            }
            length = (b1 as usize) * 256 + (b2 as usize);
        }

        // Consume the VM code bytes through the PPMd model.
        for _ in 0..length {
            let model = self.ppm_model.as_mut().unwrap();
            if model.decode_char(rc) == -1 {
                return Err(RarError::CorruptArchive {
                    detail: "RAR4: PPMd corrupt during VM filter data".into(),
                });
            }
        }

        warn!(
            "RAR4: PPMd VM filter skipped ({length} bytes) — output may be incorrect for filtered data"
        );
        Ok(())
    }

    /// Prepare for solid continuation (keep window state, reset block state).
    pub fn prepare_solid_continuation(&mut self) {
        // Tables are kept (TablesRead3 stays true in unrar).
        // Window state is preserved.
        // Reset low distance state.
        self.low_dist_rep_count = 0;
        self.prev_low_dist = 0;
    }

    /// Reset the decoder for a new non-solid file.
    pub fn reset(&mut self) {
        self.dist_cache = [0; 4];
        self.last_length = 0;
        self.ld_table = None;
        self.dd_table = None;
        self.ldd_table = None;
        self.rd_table = None;
        self.code_lengths.fill(0);
        self.low_dist_rep_count = 0;
        self.prev_low_dist = 0;
        self.block_type = BlockType::Lz;
        self.ppm_model = None;
        self.ppm_esc_char = 2;
        self.tables_read = false;
        self.window.reset();
    }
}

/// Decompress RAR4 LZ data.
pub fn decompress_rar4_lz(input: &[u8], unpacked_size: u64, dict_size: u64) -> RarResult<Vec<u8>> {
    if dict_size > MAX_DICT_SIZE {
        return Err(RarError::DictionaryTooLarge {
            size: dict_size,
            max: MAX_DICT_SIZE,
        });
    }

    let mut decoder = Rar4LzDecoder::new(dict_size as usize);
    decoder.decompress(input, unpacked_size)
}

/// Streaming variant: decompress RAR4 LZ data directly to a writer.
pub fn decompress_rar4_lz_to_writer<W: Write>(
    input: &[u8],
    unpacked_size: u64,
    dict_size: u64,
    writer: &mut W,
) -> RarResult<u64> {
    if dict_size > MAX_DICT_SIZE {
        return Err(RarError::DictionaryTooLarge {
            size: dict_size,
            max: MAX_DICT_SIZE,
        });
    }

    let mut decoder = Rar4LzDecoder::new(dict_size as usize);
    decoder.decompress_to_writer(input, unpacked_size, writer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ddecode_table_construction() {
        let (ddecode, dbits) = build_ddecode_tables();
        // First 4 entries: distance 0,1,2,3 with 0 extra bits.
        assert_eq!(ddecode[0], 0);
        assert_eq!(ddecode[1], 1);
        assert_eq!(ddecode[2], 2);
        assert_eq!(ddecode[3], 3);
        assert_eq!(dbits[0], 0);
        assert_eq!(dbits[3], 0);

        // Entries 4-5: distance 4,6 with 1 extra bit.
        assert_eq!(ddecode[4], 4);
        assert_eq!(ddecode[5], 6);
        assert_eq!(dbits[4], 1);
        assert_eq!(dbits[5], 1);

        // Entries 6-7: distance 8,12 with 2 extra bits.
        assert_eq!(ddecode[6], 8);
        assert_eq!(ddecode[7], 12);
        assert_eq!(dbits[6], 2);
    }

    #[test]
    fn test_ddecode_table_coverage() {
        let (ddecode, dbits) = build_ddecode_tables();
        // Verify entries are monotonically increasing.
        for i in 1..DC {
            if dbits[i] > 0 || ddecode[i] > 0 {
                assert!(
                    ddecode[i] >= ddecode[i - 1],
                    "ddecode not monotonic at {i}: {} < {}",
                    ddecode[i],
                    ddecode[i - 1]
                );
            }
        }
    }

    #[test]
    fn test_decoder_creation() {
        let decoder = Rar4LzDecoder::new(4 * 1024 * 1024);
        assert_eq!(decoder.window.dict_size(), 4 * 1024 * 1024);
        assert_eq!(decoder.dist_cache, [0; 4]);
        assert_eq!(decoder.last_length, 0);
    }

    #[test]
    fn test_insert_old_dist() {
        let mut decoder = Rar4LzDecoder::new(1024);
        decoder.insert_old_dist(100);
        assert_eq!(decoder.dist_cache, [100, 0, 0, 0]);
        decoder.insert_old_dist(200);
        assert_eq!(decoder.dist_cache, [200, 100, 0, 0]);
        decoder.insert_old_dist(300);
        assert_eq!(decoder.dist_cache, [300, 200, 100, 0]);
        decoder.insert_old_dist(400);
        assert_eq!(decoder.dist_cache, [400, 300, 200, 100]);
        decoder.insert_old_dist(500);
        assert_eq!(decoder.dist_cache, [500, 400, 300, 200]);
    }

    #[test]
    fn test_empty_input() {
        let result = decompress_rar4_lz(&[], 0, 4 * 1024 * 1024);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_dict_size_enforcement() {
        let result = decompress_rar4_lz(&[], 0, 1024 * 1024 * 1024);
        assert!(matches!(result, Err(RarError::DictionaryTooLarge { .. })));
    }

    #[test]
    fn test_length_tables() {
        // Verify length decode tables produce contiguous ranges.
        // Slot 0: base=0+3=3, extra=0, covers [3,4)
        // Slot 1: base=1+3=4, extra=0, covers [4,5)
        // ...
        let mut prev_end = LDECODE[0] as u32 + 3 + (1 << LBITS[0]); // end of slot 0
        for i in 1..LDECODE.len() {
            let base = LDECODE[i] as u32 + 3;
            assert_eq!(
                base, prev_end,
                "slot {i}: base {base} != prev_end {prev_end}"
            );
            prev_end = base + (1 << LBITS[i]);
        }
    }

    #[test]
    fn test_length_tables_cache_ref() {
        // Cache refs use +2 instead of +3.
        let mut prev_end = LDECODE[0] as u32 + 2 + (1 << LBITS[0]); // end of slot 0
        for i in 1..LDECODE.len() {
            let base = LDECODE[i] as u32 + 2;
            assert_eq!(
                base, prev_end,
                "slot {i}: base {base} != prev_end {prev_end}"
            );
            prev_end = base + (1 << LBITS[i]);
        }
    }
}
