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

use tracing::trace;

use super::lz::bitstream::{BitRead, BitReader, StreamingBitReader};
use super::lz::huffman::HuffmanTable;
use super::lz::window::Window;
use super::ppmd::model::Model;
use super::ppmd::range::{BitReadRangeDecoder, RangeCode};
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
const VM_MEM_SIZE: usize = 0x40000;
const MAX3_UNPACK_FILTERS: usize = 8192;
const MAX3_UNPACK_CHANNELS: u32 = 1024;

/// Maximum number of bytes to accumulate before flushing decoded output.
/// Mirrors unrar's `UNPACK_MAX_WRITE` write border.
const UNPACK_MAX_WRITE: usize = 0x400000;

/// Block type: LZ or PPMd.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BlockType {
    Lz,
    Ppm,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rar4StandardFilter {
    E8,
    E8E9,
    Itanium,
    Delta,
    Rgb,
    Audio,
}

#[derive(Clone, Debug)]
struct Rar4VmFilterDefinition {
    filter_type: Rar4StandardFilter,
    last_block_length: usize,
}

#[derive(Clone, Debug)]
struct Rar4PendingVmFilter {
    filter_type: Rar4StandardFilter,
    block_start_total: u64,
    block_length: usize,
    init_regs: [u32; 7],
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
    /// Reusable VM filter definitions within the current filter scope.
    vm_filters: Vec<Rar4VmFilterDefinition>,
    /// Pending filters waiting for their output block to become flushable.
    pending_vm_filters: Vec<Rar4PendingVmFilter>,
    /// Last referenced VM filter slot.
    last_vm_filter: usize,
    /// Absolute window position where the current file started.
    current_file_base_total: u64,
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
            vm_filters: Vec::new(),
            pending_vm_filters: Vec::new(),
            last_vm_filter: 0,
            current_file_base_total: 0,
        }
    }

    fn flush_threshold(&self) -> usize {
        self.window.dict_size().clamp(1, UNPACK_MAX_WRITE)
    }

    fn begin_file_decode(&mut self) {
        self.pending_vm_filters.clear();
        self.current_file_base_total = self.window.total_written();
        self.window.mark_flushed(self.current_file_base_total);
    }

    fn reset_vm_filter_state(&mut self) {
        self.pending_vm_filters.clear();
        self.vm_filters.clear();
        self.last_vm_filter = 0;
    }

    fn read_vm_data(reader: &mut BitReader<'_>) -> RarResult<u32> {
        let data = reader.peek_bits(16)?;
        match data & 0xC000 {
            0 => {
                reader.consume_bits(6)?;
                Ok((data >> 10) & 0x0F)
            }
            0x4000 => {
                if (data & 0x3C00) == 0 {
                    reader.consume_bits(14)?;
                    Ok(0xFFFF_FF00 | ((data >> 2) & 0xFF))
                } else {
                    reader.consume_bits(10)?;
                    Ok((data >> 6) & 0xFF)
                }
            }
            0x8000 => {
                reader.consume_bits(2)?;
                reader.read_bits(16)
            }
            _ => {
                reader.consume_bits(2)?;
                let high = reader.read_bits(16)?;
                let low = reader.read_bits(16)?;
                Ok((high << 16) | low)
            }
        }
    }

    fn decode_vm_code_length<F>(first_byte: u8, mut read_byte: F) -> RarResult<usize>
    where
        F: FnMut() -> RarResult<u8>,
    {
        let mut length = (first_byte & 7) as usize + 1;
        if length == 7 {
            length = read_byte()? as usize + 7;
        } else if length == 8 {
            let high = read_byte()? as usize;
            let low = read_byte()? as usize;
            length = (high << 8) | low;
        }

        if length == 0 {
            return Err(RarError::CorruptArchive {
                detail: "RAR4: VM code length is 0".into(),
            });
        }

        Ok(length)
    }

    fn standard_vm_filter(code: &[u8]) -> Option<Rar4StandardFilter> {
        if code.is_empty() {
            return None;
        }

        let xor_sum = code[1..].iter().fold(0u8, |acc, byte| acc ^ byte);
        if xor_sum != code[0] {
            return None;
        }

        match (code.len(), crc32fast::hash(code)) {
            (53, 0xAD57_6887) => Some(Rar4StandardFilter::E8),
            (57, 0x3CD7_E57E) => Some(Rar4StandardFilter::E8E9),
            (120, 0x3769_893F) => Some(Rar4StandardFilter::Itanium),
            (29, 0x0E06_077D) => Some(Rar4StandardFilter::Delta),
            (149, 0x1C2C_5DC8) => Some(Rar4StandardFilter::Rgb),
            (216, 0xBC85_E701) => Some(Rar4StandardFilter::Audio),
            _ => None,
        }
    }

    fn vm_u32_to_usize(value: u32, field: &str) -> RarResult<usize> {
        usize::try_from(value).map_err(|_| RarError::CorruptArchive {
            detail: format!("RAR4: {field} value {value} does not fit usize"),
        })
    }

    fn add_vm_code(&mut self, first_byte: u8, code: &[u8], output_size: u64) -> RarResult<()> {
        let mut vm_reader = BitReader::new(code);

        let filter_pos = if (first_byte & 0x80) != 0 {
            let value = Self::vm_u32_to_usize(Self::read_vm_data(&mut vm_reader)?, "filter slot")?;
            if value == 0 {
                self.reset_vm_filter_state();
                0
            } else {
                value - 1
            }
        } else {
            self.last_vm_filter
        };

        if filter_pos > self.vm_filters.len() || filter_pos > MAX3_UNPACK_FILTERS {
            return Err(RarError::CorruptArchive {
                detail: format!("RAR4: VM filter slot {filter_pos} is out of range"),
            });
        }

        let new_filter = filter_pos == self.vm_filters.len();
        if new_filter {
            self.vm_filters.push(Rar4VmFilterDefinition {
                filter_type: Rar4StandardFilter::E8,
                last_block_length: 0,
            });
        }
        let mut block_start =
            Self::vm_u32_to_usize(Self::read_vm_data(&mut vm_reader)?, "block start")?;
        if (first_byte & 0x40) != 0 {
            block_start = block_start.saturating_add(258);
        }

        let block_length = if (first_byte & 0x20) != 0 {
            let length =
                Self::vm_u32_to_usize(Self::read_vm_data(&mut vm_reader)?, "block length")?;
            if let Some(existing) = self.vm_filters.get_mut(filter_pos) {
                existing.last_block_length = length;
            }
            length
        } else if let Some(existing) = self.vm_filters.get(filter_pos) {
            existing.last_block_length
        } else {
            0
        };

        let mut init_regs = [0u32; 7];
        init_regs[4] = block_length as u32;
        if (first_byte & 0x10) != 0 {
            let init_mask = vm_reader.read_bits(7)? as u8;
            for (index, register) in init_regs.iter_mut().enumerate() {
                if (init_mask & (1 << index)) != 0 {
                    *register = Self::read_vm_data(&mut vm_reader)?;
                }
            }
        }

        let filter_type = if new_filter {
            let code_size =
                Self::vm_u32_to_usize(Self::read_vm_data(&mut vm_reader)?, "VM code size")?;
            if code_size == 0 || code_size >= 0x10000 {
                return Err(RarError::CorruptArchive {
                    detail: format!("RAR4: invalid VM code size {code_size}"),
                });
            }

            let mut vm_code = Vec::with_capacity(code_size);
            for _ in 0..code_size {
                vm_code.push(vm_reader.read_bits(8)? as u8);
            }

            let filter_type =
                Self::standard_vm_filter(&vm_code).ok_or_else(|| RarError::CorruptArchive {
                    detail: "RAR4: unsupported custom VM filter program".into(),
                })?;

            self.vm_filters[filter_pos].filter_type = filter_type;
            filter_type
        } else {
            self.vm_filters[filter_pos].filter_type
        };

        self.last_vm_filter = filter_pos;
        self.pending_vm_filters.push(Rar4PendingVmFilter {
            filter_type,
            block_start_total: self.current_file_base_total + output_size + block_start as u64,
            block_length,
            init_regs,
        });
        Ok(())
    }

    fn itanium_get_bits(data: &[u8], bit_pos: u32, bit_count: u32) -> u32 {
        let in_addr = (bit_pos / 8) as usize;
        let in_bit = bit_pos & 7;
        let mut bit_field = data[in_addr] as u32;
        bit_field |= (data[in_addr + 1] as u32) << 8;
        bit_field |= (data[in_addr + 2] as u32) << 16;
        bit_field |= (data[in_addr + 3] as u32) << 24;
        (bit_field >> in_bit) & (0xFFFF_FFFFu32 >> (32 - bit_count))
    }

    fn itanium_set_bits(data: &mut [u8], bit_field: u32, bit_pos: u32, bit_count: u32) {
        let in_addr = (bit_pos / 8) as usize;
        let in_bit = bit_pos & 7;
        let mut and_mask = !(0xFFFF_FFFFu32 >> (32 - bit_count) << in_bit);
        let mut bit_field = bit_field << in_bit;

        for offset in 0..4 {
            data[in_addr + offset] &= and_mask as u8;
            data[in_addr + offset] |= bit_field as u8;
            and_mask = (and_mask >> 8) | 0xFF00_0000;
            bit_field >>= 8;
        }
    }

    fn execute_standard_filter(
        filter: &Rar4PendingVmFilter,
        file_base_total: u64,
        data: &mut Vec<u8>,
    ) -> RarResult<()> {
        let data_size = data.len();
        let file_offset = (filter.block_start_total - file_base_total) as u32;

        match filter.filter_type {
            Rar4StandardFilter::E8 | Rar4StandardFilter::E8E9 => {
                if data_size < 4 || data_size > VM_MEM_SIZE {
                    return Err(RarError::CorruptArchive {
                        detail: format!("RAR4: invalid E8 filter block length {data_size}"),
                    });
                }

                let cmp_byte2 = if filter.filter_type == Rar4StandardFilter::E8E9 {
                    0xE9
                } else {
                    0xE8
                };
                let file_size = 0x0100_0000u32;
                let mut cur_pos = 0usize;
                while cur_pos + 4 < data_size {
                    let cur_byte = data[cur_pos];
                    cur_pos += 1;
                    if cur_byte == 0xE8 || cur_byte == cmp_byte2 {
                        let offset = file_offset.wrapping_add(cur_pos as u32);
                        let addr = u32::from_le_bytes([
                            data[cur_pos],
                            data[cur_pos + 1],
                            data[cur_pos + 2],
                            data[cur_pos + 3],
                        ]);
                        if (addr & 0x8000_0000) != 0 {
                            if ((addr.wrapping_add(offset)) & 0x8000_0000) == 0 {
                                let bytes = addr.wrapping_add(file_size).to_le_bytes();
                                data[cur_pos..cur_pos + 4].copy_from_slice(&bytes);
                            }
                        } else if ((addr.wrapping_sub(file_size)) & 0x8000_0000) != 0 {
                            let bytes = addr.wrapping_sub(offset).to_le_bytes();
                            data[cur_pos..cur_pos + 4].copy_from_slice(&bytes);
                        }
                        cur_pos += 4;
                    }
                }
            }
            Rar4StandardFilter::Itanium => {
                if data_size < 21 || data_size > VM_MEM_SIZE {
                    return Err(RarError::CorruptArchive {
                        detail: format!("RAR4: invalid Itanium filter block length {data_size}"),
                    });
                }

                let masks = [4u8, 4, 6, 6, 0, 0, 7, 7, 4, 4, 0, 0, 4, 4, 0, 0];
                let mut file_offset = file_offset >> 4;
                let mut cur_pos = 0usize;
                while cur_pos + 21 < data_size {
                    let byte = (data[cur_pos] & 0x1F) as i32 - 0x10;
                    if byte >= 0 {
                        let cmd_mask = masks[byte as usize];
                        if cmd_mask != 0 {
                            for slot in 0..=2u32 {
                                if (cmd_mask & (1 << slot)) != 0 {
                                    let start_pos = slot * 41 + 5;
                                    let op_type = Self::itanium_get_bits(data, start_pos + 37, 4);
                                    if op_type == 5 {
                                        let offset =
                                            Self::itanium_get_bits(data, start_pos + 13, 20);
                                        Self::itanium_set_bits(
                                            data,
                                            offset.wrapping_sub(file_offset) & 0x0F_FFFF,
                                            start_pos + 13,
                                            20,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    cur_pos += 16;
                    file_offset = file_offset.wrapping_add(1);
                }
            }
            Rar4StandardFilter::Delta => {
                let channels = filter.init_regs[0];
                if data_size > VM_MEM_SIZE / 2 || channels == 0 || channels > MAX3_UNPACK_CHANNELS {
                    return Err(RarError::CorruptArchive {
                        detail: format!(
                            "RAR4: invalid DELTA filter params size={data_size} channels={channels}"
                        ),
                    });
                }

                let mut dest = vec![0u8; data_size];
                let mut src_pos = 0usize;
                for channel in 0..channels as usize {
                    let mut prev_byte = 0u8;
                    let mut dest_pos = channel;
                    while dest_pos < data_size {
                        prev_byte = prev_byte.wrapping_sub(data[src_pos]);
                        dest[dest_pos] = prev_byte;
                        src_pos += 1;
                        dest_pos += channels as usize;
                    }
                }
                *data = dest;
            }
            Rar4StandardFilter::Rgb => {
                let width =
                    filter.init_regs[0]
                        .checked_sub(3)
                        .ok_or_else(|| RarError::CorruptArchive {
                            detail: "RAR4: RGB filter width underflow".into(),
                        })? as usize;
                let pos_r = filter.init_regs[1] as usize;
                if data_size < 3 || data_size > VM_MEM_SIZE / 2 || width > data_size || pos_r > 2 {
                    return Err(RarError::CorruptArchive {
                        detail: format!(
                            "RAR4: invalid RGB filter params size={data_size} width={width} pos_r={pos_r}"
                        ),
                    });
                }

                let mut dest = vec![0u8; data_size];
                let channels = 3usize;
                let mut src_pos = 0usize;
                for channel in 0..channels {
                    let mut prev_byte = 0u8;
                    let mut index = channel;
                    while index < data_size {
                        let predicted = if index >= width + 3 {
                            let upper = dest[index - width];
                            let upper_left = dest[index - width - 3];
                            let mut predicted =
                                prev_byte.wrapping_add(upper).wrapping_sub(upper_left);
                            let pa = (predicted as i32 - prev_byte as i32).abs();
                            let pb = (predicted as i32 - upper as i32).abs();
                            let pc = (predicted as i32 - upper_left as i32).abs();
                            if pa <= pb && pa <= pc {
                                predicted = prev_byte;
                            } else if pb <= pc {
                                predicted = upper;
                            } else {
                                predicted = upper_left;
                            }
                            predicted
                        } else {
                            prev_byte
                        };

                        let value = predicted.wrapping_sub(data[src_pos]);
                        dest[index] = value;
                        prev_byte = value;
                        src_pos += 1;
                        index += channels;
                    }
                }

                let border = data_size.saturating_sub(2);
                let mut index = pos_r;
                while index < border {
                    let g = dest[index + 1];
                    dest[index] = dest[index].wrapping_add(g);
                    dest[index + 2] = dest[index + 2].wrapping_add(g);
                    index += 3;
                }

                *data = dest;
            }
            Rar4StandardFilter::Audio => {
                let channels = filter.init_regs[0] as usize;
                if data_size > VM_MEM_SIZE / 2 || channels == 0 || channels > 128 {
                    return Err(RarError::CorruptArchive {
                        detail: format!(
                            "RAR4: invalid AUDIO filter params size={data_size} channels={channels}"
                        ),
                    });
                }

                let mut dest = vec![0u8; data_size];
                let mut src_pos = 0usize;
                for channel in 0..channels {
                    let mut prev_byte = 0u32;
                    let mut prev_delta = 0i32;
                    let mut dif = [0i32; 7];
                    let (mut d1, mut d2) = (0i32, 0i32);
                    let (mut k1, mut k2, mut k3) = (0i32, 0i32, 0i32);
                    let mut byte_count = 0usize;
                    let mut index = channel;

                    while index < data_size {
                        let d3 = d2;
                        d2 = prev_delta - d1;
                        d1 = prev_delta;

                        let mut predicted =
                            (8 * prev_byte as i32 + k1 * d1 + k2 * d2 + k3 * d3) >> 3;
                        predicted &= 0xFF;

                        let cur_byte = data[src_pos];
                        src_pos += 1;

                        let decoded = (predicted as u8).wrapping_sub(cur_byte);
                        dest[index] = decoded;
                        prev_delta = decoded as i8 as i32 - prev_byte as i8 as i32;
                        prev_byte = decoded as u32;

                        let d = ((cur_byte as i8 as i32) << 3).abs();
                        dif[0] += d;
                        dif[1] += (d - d1).abs();
                        dif[2] += (d + d1).abs();
                        dif[3] += (d - d2).abs();
                        dif[4] += (d + d2).abs();
                        dif[5] += (d - d3).abs();
                        dif[6] += (d + d3).abs();

                        if (byte_count & 0x1F) == 0 {
                            let mut min_dif = dif[0];
                            let mut min_index = 0usize;
                            dif[0] = 0;
                            for (candidate, value) in dif.iter_mut().enumerate().skip(1) {
                                if *value < min_dif {
                                    min_dif = *value;
                                    min_index = candidate;
                                }
                                *value = 0;
                            }
                            match min_index {
                                1 if k1 >= -16 => k1 -= 1,
                                2 if k1 < 16 => k1 += 1,
                                3 if k2 >= -16 => k2 -= 1,
                                4 if k2 < 16 => k2 += 1,
                                5 if k3 >= -16 => k3 -= 1,
                                6 if k3 < 16 => k3 += 1,
                                _ => {}
                            }
                        }

                        byte_count += 1;
                        index += channels;
                    }
                }

                *data = dest;
            }
        }

        Ok(())
    }

    fn flush_ready_output_to_writer<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        final_flush: bool,
    ) -> RarResult<()> {
        loop {
            let written_border = self.window.total_flushed();
            let total_written = self.window.total_written();

            if self.pending_vm_filters.is_empty() {
                if total_written > written_border {
                    self.window
                        .write_range_to_writer(
                            written_border,
                            (total_written - written_border) as usize,
                            writer,
                        )
                        .map_err(RarError::Io)?;
                    self.window.mark_flushed(total_written);
                }
                return Ok(());
            }

            let next_start = self.pending_vm_filters[0].block_start_total;
            if next_start < written_border {
                return Err(RarError::CorruptArchive {
                    detail: format!(
                        "RAR4: pending VM filter starts before flushed border ({next_start} < {written_border})"
                    ),
                });
            }

            let raw_len = next_start.saturating_sub(written_border);
            if raw_len > 0 {
                self.window
                    .write_range_to_writer(written_border, raw_len as usize, writer)
                    .map_err(RarError::Io)?;
                self.window.mark_flushed(next_start);
                continue;
            }

            let block_length = self.pending_vm_filters[0].block_length as u64;
            let block_end =
                next_start
                    .checked_add(block_length)
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: "RAR4: VM filter block end overflow".into(),
                    })?;
            if block_end > total_written {
                if final_flush {
                    return Err(RarError::CorruptArchive {
                        detail: format!(
                            "RAR4: VM filter block [{next_start}, {block_end}) is incomplete at EOF"
                        ),
                    });
                }
                return Ok(());
            }

            let mut data = self
                .window
                .copy_output(next_start, self.pending_vm_filters[0].block_length);
            let mut chain_len = 1usize;
            while chain_len < self.pending_vm_filters.len() {
                let next = &self.pending_vm_filters[chain_len];
                if next.block_start_total != next_start
                    || next.block_length != self.pending_vm_filters[0].block_length
                {
                    break;
                }
                chain_len += 1;
            }

            for filter in self.pending_vm_filters.iter().take(chain_len) {
                Self::execute_standard_filter(filter, self.current_file_base_total, &mut data)?;
            }

            writer.write_all(&data).map_err(RarError::Io)?;
            self.window.mark_flushed(block_end);
            self.pending_vm_filters.drain(..chain_len);
        }
    }

    /// Decompress RAR4 LZ data, returning the decompressed output.
    pub fn decompress(&mut self, input: &[u8], unpacked_size: u64) -> RarResult<Vec<u8>> {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        let mut reader = BitReader::new(input);
        self.decompress_with_reader(&mut reader, unpacked_size)
    }

    fn decompress_with_reader<R: BitRead>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
    ) -> RarResult<Vec<u8>> {
        let mut output = Vec::with_capacity(unpacked_size.min(1024 * 1024) as usize);
        self.decompress_to_writer_with_reader(reader, unpacked_size, &mut output)?;
        Ok(output)
    }

    /// Decode a RAR4 solid member only to advance decoder state.
    pub fn replay(&mut self, input: &[u8], unpacked_size: u64) -> RarResult<u64> {
        if unpacked_size == 0 {
            return Ok(0);
        }

        let mut reader = BitReader::new(input);

        self.replay_with_reader(&mut reader, unpacked_size)
    }

    fn replay_with_reader<R: BitRead>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
    ) -> RarResult<u64> {
        let mut sink = std::io::sink();
        self.decompress_to_writer_with_reader(reader, unpacked_size, &mut sink)
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

        self.decompress_to_writer_with_reader(&mut reader, unpacked_size, writer)
    }

    pub fn decompress_reader_to_writer<Rd: std::io::Read, W: Write>(
        &mut self,
        input: Rd,
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        if unpacked_size == 0 {
            return Ok(0);
        }

        let mut reader = StreamingBitReader::new(input);
        self.decompress_to_writer_with_reader(&mut reader, unpacked_size, writer)
    }

    fn decompress_to_writer_with_reader<R: BitRead, W: Write>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        if unpacked_size == 0 {
            return Ok(0);
        }

        if !self.tables_read {
            self.read_tables(reader)?;
        }

        self.begin_file_decode();
        let mut output_size: u64 = 0;
        let flush_threshold = self.flush_threshold();
        let decode_chunk = flush_threshold as u64;

        while output_size < unpacked_size {
            if reader.bits_remaining() < 1 {
                break;
            }

            let target_output = output_size.saturating_add(decode_chunk).min(unpacked_size);

            match self.block_type {
                BlockType::Lz => {
                    output_size = self.decode_lz_symbols(reader, target_output, output_size)?;
                }
                BlockType::Ppm => {
                    output_size = self.decode_ppm_symbols(reader, target_output, output_size)?;
                }
            }

            if self.window.unflushed_bytes() as usize >= flush_threshold {
                self.flush_ready_output_to_writer(writer, false)?;
            }
        }

        self.flush_ready_output_to_writer(writer, true)?;
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
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        let mut reader = BitReader::new(input);

        self.decompress_to_writer_chunked_with_reader(
            &mut reader,
            unpacked_size,
            first_volume_index,
            boundaries,
            writer_factory,
        )
    }

    pub fn decompress_reader_to_writer_chunked<Rd: std::io::Read, F>(
        &mut self,
        input: Rd,
        unpacked_size: u64,
        first_volume_index: usize,
        shared_transitions: std::sync::Arc<std::sync::Mutex<Vec<super::VolumeTransition>>>,
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        let mut reader = StreamingBitReader::new(input);
        self.decompress_to_writer_chunked_with_shared_transitions(
            &mut reader,
            unpacked_size,
            first_volume_index,
            shared_transitions,
            writer_factory,
        )
    }

    fn decompress_to_writer_chunked_with_reader<R: BitRead, F>(
        &mut self,
        reader: &mut R,
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

        if !self.tables_read {
            self.read_tables(reader)?;
        }

        self.begin_file_decode();
        let mut output_size: u64 = 0;
        let flush_threshold = self.flush_threshold();
        let decode_chunk = flush_threshold as u64;
        let mut boundary_idx = 0;

        let mut chunks: Vec<(usize, u64)> = Vec::new();
        let mut current_vol = first_volume_index;
        let mut current_writer = writer_factory(current_vol)?;
        let mut chunk_bytes: u64 = 0;
        let mut pending_boundary_volume = None;

        while output_size < unpacked_size {
            if reader.bits_remaining() < 1 {
                break;
            }

            let prev_output = output_size;
            let target_output = output_size.saturating_add(decode_chunk).min(unpacked_size);
            match self.block_type {
                BlockType::Lz => {
                    output_size = self.decode_lz_symbols(reader, target_output, output_size)?;
                }
                BlockType::Ppm => {
                    output_size = self.decode_ppm_symbols(reader, target_output, output_size)?;
                }
            }
            let decoded_this_round = output_size - prev_output;
            chunk_bytes += decoded_this_round;

            let byte_pos = reader.byte_position() as u64;
            if pending_boundary_volume.is_none()
                && boundary_idx < boundaries.len()
                && byte_pos >= boundaries[boundary_idx].compressed_offset
            {
                pending_boundary_volume = Some(boundaries[boundary_idx].volume_index);
                boundary_idx += 1;
            }

            if pending_boundary_volume.is_some() || self.window.unflushed_bytes() as usize >= flush_threshold {
                self.flush_ready_output_to_writer(&mut *current_writer, false)?;
                if self.window.unflushed_bytes() as usize > self.window.dict_size() {
                    return Err(RarError::CorruptArchive {
                        detail:
                            "RAR4 pending VM filters exceeded dictionary window before flush"
                                .into(),
                    });
                }
            }

            // VM filters can span a compressed-volume boundary, so only hand
            // output to the next writer after the current chunk is fully
            // materialized through the filter queue.
            if let Some(next_vol) = pending_boundary_volume
                && self.window.total_flushed() == self.window.total_written()
            {
                chunks.push((current_vol, chunk_bytes));
                current_vol = next_vol;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
                pending_boundary_volume = None;
            }
        }

        self.flush_ready_output_to_writer(&mut *current_writer, true)?;
        if chunk_bytes > 0 || chunks.is_empty() {
            chunks.push((current_vol, chunk_bytes));
        }

        Ok(chunks)
    }

    fn decompress_to_writer_chunked_with_shared_transitions<R: BitRead, F>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
        first_volume_index: usize,
        shared_transitions: std::sync::Arc<std::sync::Mutex<Vec<super::VolumeTransition>>>,
        mut writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        if !self.tables_read {
            self.read_tables(reader)?;
        }

        self.begin_file_decode();
        let mut output_size: u64 = 0;
        let flush_threshold = self.flush_threshold();
        let decode_chunk = flush_threshold as u64;
        let mut boundary_idx = 0;

        let mut chunks: Vec<(usize, u64)> = Vec::new();
        let mut current_vol = first_volume_index;
        let mut current_writer = writer_factory(current_vol)?;
        let mut chunk_bytes: u64 = 0;
        let mut pending_boundary_volume = None;

        while output_size < unpacked_size {
            if reader.bits_remaining() < 1 {
                break;
            }

            let prev_output = output_size;
            let target_output = output_size.saturating_add(decode_chunk).min(unpacked_size);
            match self.block_type {
                BlockType::Lz => {
                    output_size = self.decode_lz_symbols(reader, target_output, output_size)?;
                }
                BlockType::Ppm => {
                    output_size = self.decode_ppm_symbols(reader, target_output, output_size)?;
                }
            }
            let decoded_this_round = output_size - prev_output;
            chunk_bytes += decoded_this_round;

            let byte_pos = reader.byte_position() as u64;
            let next_boundary = {
                let guard = shared_transitions.lock().unwrap();
                guard.get(boundary_idx).cloned()
            };

            if pending_boundary_volume.is_none()
                && let Some(boundary) = next_boundary
                && byte_pos >= boundary.compressed_offset
            {
                pending_boundary_volume = Some(boundary.volume_index);
                boundary_idx += 1;
            }

            if pending_boundary_volume.is_some() || self.window.unflushed_bytes() as usize >= flush_threshold {
                self.flush_ready_output_to_writer(&mut *current_writer, false)?;
                if self.window.unflushed_bytes() as usize > self.window.dict_size() {
                    return Err(RarError::CorruptArchive {
                        detail:
                            "RAR4 pending VM filters exceeded dictionary window before flush"
                                .into(),
                    });
                }
            }

            if let Some(next_vol) = pending_boundary_volume
                && self.window.total_flushed() == self.window.total_written()
            {
                chunks.push((current_vol, chunk_bytes));
                current_vol = next_vol;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
                pending_boundary_volume = None;
            }
        }

        self.flush_ready_output_to_writer(&mut *current_writer, true)?;
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
    fn read_tables<R: BitRead>(&mut self, reader: &mut R) -> RarResult<()> {
        // Align to byte boundary.
        reader.align_byte()?;

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
    fn decode_lz_symbols<R: BitRead>(
        &mut self,
        reader: &mut R,
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
                self.read_vm_code(reader, output_size)?;
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
    fn decode_distance<R: BitRead>(&mut self, reader: &mut R) -> RarResult<usize> {
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
    fn read_end_of_block<R: BitRead>(&mut self, reader: &mut R) -> RarResult<bool> {
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

    /// Read VM filter code (symbol 257) and queue a standard filter block.
    fn read_vm_code<R: BitRead>(&mut self, reader: &mut R, output_size: u64) -> RarResult<()> {
        let first_byte = reader.read_bits(8)? as u8;
        let length = Self::decode_vm_code_length(first_byte, || Ok(reader.read_bits(8)? as u8))?;
        let mut code = Vec::with_capacity(length);
        for _ in 0..length {
            if reader.bits_remaining() < 8 {
                return Err(RarError::CorruptArchive {
                    detail: "RAR4: truncated VM code".into(),
                });
            }
            code.push(reader.read_bits(8)? as u8);
        }
        self.add_vm_code(first_byte, &code, output_size)
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
    fn init_ppm<R: BitRead>(&mut self, reader: &mut R) -> RarResult<()> {
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
    fn decode_ppm_symbols<R: BitRead>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
        mut output_size: u64,
    ) -> RarResult<u64> {
        if reader.bits_remaining() < 32 {
            return Ok(output_size);
        }

        let mut switch_to_lz_tables = false;

        {
            let mut rc = BitReadRangeDecoder::new(reader)?;

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
                            switch_to_lz_tables = true;
                            break;
                        }
                        2 => {
                            break;
                        }
                        3 => {
                            self.read_vm_code_ppm(&mut rc, output_size)?;
                        }
                        4 => {
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
                            self.window.put_byte(ch);
                            output_size += 1;
                        }
                    }
                } else {
                    self.window.put_byte(ch);
                    output_size += 1;
                }
            }
        }

        if switch_to_lz_tables {
            self.read_tables(reader)?;
        }

        Ok(output_size)
    }

    /// Read VM filter code in PPM mode and queue a standard filter block.
    fn read_vm_code_ppm<R: RangeCode>(&mut self, rc: &mut R, output_size: u64) -> RarResult<()> {
        let read_model_byte = |this: &mut Self, rc: &mut R| -> RarResult<u8> {
            let model = this
                .ppm_model
                .as_mut()
                .ok_or_else(|| RarError::CorruptArchive {
                    detail: "RAR4: PPMd model missing during VM filter read".into(),
                })?;
            let byte = model.decode_char(rc);
            if byte == -1 {
                return Err(RarError::CorruptArchive {
                    detail: "RAR4: PPMd corrupt during VM filter read".into(),
                });
            }
            Ok(byte as u8)
        };

        let first_byte = read_model_byte(self, rc)?;
        let length = Self::decode_vm_code_length(first_byte, || read_model_byte(self, rc))?;
        let mut code = Vec::with_capacity(length);
        for _ in 0..length {
            code.push(read_model_byte(self, rc)?);
        }

        self.add_vm_code(first_byte, &code, output_size)
    }

    /// Prepare for solid continuation (keep window state, reset block state).
    pub fn prepare_solid_continuation(&mut self) {
        // Tables are kept (TablesRead3 stays true in unrar).
        // Window state is preserved.
        // Reset low distance state.
        self.low_dist_rep_count = 0;
        self.prev_low_dist = 0;
        self.pending_vm_filters.clear();
        self.current_file_base_total = self.window.total_written();
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
        self.vm_filters.clear();
        self.pending_vm_filters.clear();
        self.last_vm_filter = 0;
        self.current_file_base_total = 0;
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

pub fn decompress_rar4_lz_reader_to_writer<R: std::io::Read, W: Write>(
    input: R,
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
    let mut reader = StreamingBitReader::new(input);
    decoder.decompress_to_writer_with_reader(&mut reader, unpacked_size, writer)
}

pub fn decompress_rar4_lz_reader_to_writer_chunked<R: std::io::Read, F>(
    input: R,
    unpacked_size: u64,
    dict_size: u64,
    first_volume_index: usize,
    shared_transitions: std::sync::Arc<std::sync::Mutex<Vec<super::VolumeTransition>>>,
    writer_factory: F,
) -> RarResult<Vec<(usize, u64)>>
where
    F: FnMut(usize) -> RarResult<Box<dyn Write>>,
{
    if dict_size > MAX_DICT_SIZE {
        return Err(RarError::DictionaryTooLarge {
            size: dict_size,
            max: MAX_DICT_SIZE,
        });
    }

    let mut decoder = Rar4LzDecoder::new(dict_size as usize);
    let mut reader = StreamingBitReader::new(input);
    decoder.decompress_to_writer_chunked_with_shared_transitions(
        &mut reader,
        unpacked_size,
        first_volume_index,
        shared_transitions,
        writer_factory,
    )
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

    #[test]
    fn test_execute_standard_delta_filter() {
        let filter = Rar4PendingVmFilter {
            filter_type: Rar4StandardFilter::Delta,
            block_start_total: 0,
            block_length: 4,
            init_regs: [1, 0, 0, 0, 4, 0, 0],
        };
        let mut data = vec![1u8, 2, 3, 4];
        Rar4LzDecoder::execute_standard_filter(&filter, 0, &mut data).unwrap();
        assert_eq!(data, vec![255, 253, 250, 246]);
    }

    #[test]
    fn test_flush_ready_output_to_writer_applies_e8_filter() {
        let mut decoder = Rar4LzDecoder::new(1024);
        decoder.begin_file_decode();
        decoder.window.put_bytes(&[0xAA, 0xBB, 0xE8, 100, 0, 0, 0]);
        decoder.pending_vm_filters.push(Rar4PendingVmFilter {
            filter_type: Rar4StandardFilter::E8,
            block_start_total: 2,
            block_length: 5,
            init_regs: [0; 7],
        });

        let mut out = Vec::new();
        decoder
            .flush_ready_output_to_writer(&mut out, true)
            .unwrap();
        assert_eq!(out, vec![0xAA, 0xBB, 0xE8, 97, 0, 0, 0]);
        assert_eq!(
            decoder.window.total_flushed(),
            decoder.window.total_written()
        );
    }

    #[test]
    fn test_chunked_flush_applies_e8_filter() {
        let mut decoder = Rar4LzDecoder::new(1024);
        decoder.begin_file_decode();
        decoder.window.put_bytes(&[0xAA, 0xBB, 0xE8, 100, 0, 0, 0]);
        decoder.pending_vm_filters.push(Rar4PendingVmFilter {
            filter_type: Rar4StandardFilter::E8,
            block_start_total: 2,
            block_length: 5,
            init_regs: [0; 7],
        });

        let mut out = Vec::new();
        decoder
            .flush_ready_output_to_writer(&mut out, false)
            .unwrap();
        assert_eq!(out, vec![0xAA, 0xBB, 0xE8, 97, 0, 0, 0]);
        assert!(decoder.pending_vm_filters.is_empty());
        assert_eq!(
            decoder.window.total_flushed(),
            decoder.window.total_written()
        );
    }

    #[test]
    fn test_vm_filter_reset_clears_pending_state() {
        let mut decoder = Rar4LzDecoder::new(1024);
        decoder.begin_file_decode();
        decoder.vm_filters.push(Rar4VmFilterDefinition {
            filter_type: Rar4StandardFilter::Delta,
            last_block_length: 8,
        });
        decoder.last_vm_filter = 3;
        decoder.pending_vm_filters.push(Rar4PendingVmFilter {
            filter_type: Rar4StandardFilter::Delta,
            block_start_total: 12,
            block_length: 8,
            init_regs: [1, 0, 0, 0, 8, 0, 0],
        });

        decoder.reset_vm_filter_state();

        assert!(decoder.pending_vm_filters.is_empty());
        assert!(decoder.vm_filters.is_empty());
        assert_eq!(decoder.last_vm_filter, 0);
    }

    #[test]
    fn test_new_vm_filter_without_length_defaults_to_zero_block_length() {
        let mut decoder = Rar4LzDecoder::new(1024);
        decoder.begin_file_decode();

        let vm_code = [0x04, 0x00, 0x00, 0xD8, 0xD2];
        decoder.add_vm_code(0x81, &vm_code, 0).unwrap();

        assert_eq!(decoder.vm_filters.len(), 1);
        assert_eq!(decoder.vm_filters[0].last_block_length, 0);
        assert_eq!(decoder.pending_vm_filters.len(), 1);
        assert_eq!(decoder.pending_vm_filters[0].block_length, 0);
    }

    #[test]
    fn test_chunked_final_flush_applies_pending_vm_filter() {
        let mut decoder = Rar4LzDecoder::new(1024);
        decoder.tables_read = true;
        decoder.begin_file_decode();
        decoder.window.put_bytes(&[0xAA, 0xBB, 0xE8, 100, 0, 0, 0]);
        decoder.pending_vm_filters.push(Rar4PendingVmFilter {
            filter_type: Rar4StandardFilter::E8,
            block_start_total: 2,
            block_length: 5,
            init_regs: [0; 7],
        });

        let chunks = decoder
            .decompress_to_writer_chunked(&[], 7, 0, &[], |_volume_index| {
                Ok(Box::new(Vec::<u8>::new()))
            })
            .unwrap();

        assert_eq!(chunks, vec![(0, 7)]);
        assert!(decoder.pending_vm_filters.is_empty());
        assert_eq!(
            decoder.window.total_flushed(),
            decoder.window.total_written()
        );
    }
}
