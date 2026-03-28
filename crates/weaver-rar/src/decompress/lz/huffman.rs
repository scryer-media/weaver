//! Huffman table construction and decoding for RAR LZ decompression.
//!
//! RAR5 uses 5 canonical Huffman tables:
//! - `HUFF_BC = 20` — bit-length bootstrap codes (used to decode other tables)
//! - `HUFF_NC = 306` — literal/length codes (256 literals + 50 length codes)
//! - `HUFF_DC = 64` — distance codes
//! - `HUFF_LDC = 16` — lower distance bits codes
//! - `HUFF_RC = 44` — repeating/cached distance codes
//!
//! Decoding uses the same sorted-threshold scheme as unrar:
//! - Quick table (9-bit lookup) for the most common short codes
//! - Linear scan of 15 left-aligned thresholds for longer codes
//! - No tree pointers — just flat arrays (cache-friendly)

use crate::error::{RarError, RarResult};

use super::bitstream::{BitRead, BitReader};

/// Number of bit-length bootstrap codes.
pub const HUFF_BC: usize = 20;
/// Number of literal/length codes (256 literals + 50 length codes).
pub const HUFF_NC: usize = 306;
/// Number of distance codes.
pub const HUFF_DC: usize = 64;
/// Number of lower distance bit codes.
pub const HUFF_LDC: usize = 16;
/// Number of repeating/cached distance codes.
pub const HUFF_RC: usize = 44;

/// Maximum Huffman code length in bits.
const MAX_CODE_LENGTH: usize = 15;

/// Maximum quick decode bits (matches unrar MAX_QUICK_DECODE_BITS = 9).
const MAX_QUICK_BITS: u8 = 9;

/// Maximum number of symbols across supported Huffman tables.
const MAX_NUM_SYMBOLS: usize = HUFF_NC;

/// Maximum quick decode table size.
const MAX_QUICK_ENTRIES: usize = 1 << MAX_QUICK_BITS;

/// A Huffman decoding table using unrar's sorted-threshold scheme.
#[derive(Clone)]
///
/// Instead of a binary tree, uses left-aligned upper-limit codes per bit
/// length (`decode_len`) and a flat symbol array (`decode_num`). The hot
/// path does a single 16-bit peek + quick table lookup. The slow path
/// scans at most 15 threshold values (fits in one cache line).
pub struct HuffmanTable {
    /// Left-aligned upper limit codes for each bit length.
    /// `decode_len[i]` is the left-aligned (16-bit) upper limit code for
    /// bit length `i`. Codes < decode_len[i] have bit length <= i.
    decode_len: [u32; 16],

    /// Start position in decode_num for each bit length.
    decode_pos: [u32; 16],

    /// Maps position in code list to alphabet symbol.
    decode_num: [u16; MAX_NUM_SYMBOLS],

    /// Quick lookup: bit length for each quick-decodable prefix.
    quick_len: [u8; MAX_QUICK_ENTRIES],

    /// Quick lookup: alphabet symbol for each quick-decodable prefix.
    quick_num: [u16; MAX_QUICK_ENTRIES],

    /// Number of bits used for quick decode (6 for small tables, 9 for NC).
    quick_bits: u8,

    /// Maximum code length in the table.
    max_length: u8,

    /// Number of symbols in this table.
    num_symbols: usize,
}

impl HuffmanTable {
    /// Build a Huffman table from an array of bit-lengths.
    ///
    /// `bit_lengths[i]` is the code length for symbol `i`.
    /// A length of 0 means the symbol is not present.
    ///
    /// Matches unrar's `MakeDecodeTables`.
    pub fn build(bit_lengths: &[u8]) -> RarResult<Self> {
        let num_symbols = bit_lengths.len();
        if num_symbols > MAX_NUM_SYMBOLS {
            return Err(RarError::InvalidHuffmanTable);
        }

        let quick_bits = quick_bits_for_size(num_symbols);

        let mut length_count = [0u32; 16];
        let mut max_length: u8 = 0;
        for &bl in bit_lengths {
            if bl > 0 {
                if bl as usize > MAX_CODE_LENGTH {
                    return Err(RarError::InvalidHuffmanTable);
                }
                length_count[bl as usize] += 1;
                if bl > max_length {
                    max_length = bl;
                }
            }
        }
        length_count[0] = 0;

        if max_length == 0 {
            return Ok(Self {
                decode_len: [0; 16],
                decode_pos: [0; 16],
                decode_num: [0; MAX_NUM_SYMBOLS],
                quick_len: [0; MAX_QUICK_ENTRIES],
                quick_num: [0; MAX_QUICK_ENTRIES],
                quick_bits,
                max_length: 0,
                num_symbols,
            });
        }

        let mut decode_len = [0u32; 16];
        let mut decode_pos = [0u32; 16];
        let mut upper_limit: u32 = 0;
        for i in 1..16 {
            upper_limit += length_count[i];
            let left_aligned = upper_limit << (16 - i);
            upper_limit *= 2;
            decode_len[i] = left_aligned;
            decode_pos[i] = decode_pos[i - 1] + length_count[i - 1];
        }

        let mut decode_num = [0u16; MAX_NUM_SYMBOLS];
        let mut copy_pos = decode_pos;
        for (sym, &bl) in bit_lengths.iter().enumerate() {
            let bl = bl & 0xF;
            if bl != 0 {
                let pos = copy_pos[bl as usize] as usize;
                if pos < num_symbols {
                    decode_num[pos] = sym as u16;
                }
                copy_pos[bl as usize] += 1;
            }
        }

        let mut quick_len_table = [0u8; MAX_QUICK_ENTRIES];
        let mut quick_num_table = [0u16; MAX_QUICK_ENTRIES];
        let quick_data_size = 1usize << quick_bits;
        let mut cur_bit_length: usize = 1;
        for code in 0..quick_data_size {
            let bit_field = (code as u32) << (16 - quick_bits);
            while cur_bit_length < decode_len.len() && bit_field >= decode_len[cur_bit_length] {
                cur_bit_length += 1;
            }

            // Prefixes that run off the end do not correspond to a valid quick-table
            // decode. Leave the entry empty so decode falls back to the slow path.
            if cur_bit_length >= decode_pos.len() {
                continue;
            }

            quick_len_table[code] = cur_bit_length as u8;

            let dist = bit_field.wrapping_sub(decode_len[cur_bit_length - 1]);
            let dist = dist >> (16 - cur_bit_length);
            let pos = (decode_pos[cur_bit_length] + dist) as usize;
            if pos < num_symbols {
                quick_num_table[code] = decode_num[pos];
            }
        }

        Ok(Self {
            decode_len,
            decode_pos,
            decode_num,
            quick_len: quick_len_table,
            quick_num: quick_num_table,
            quick_bits,
            max_length,
            num_symbols,
        })
    }

    /// Decode the next symbol from the bitstream.
    ///
    /// Matches unrar's `DecodeNumber`: peek 16 left-aligned bits, try quick
    /// table, fall back to linear threshold scan.
    #[inline(always)]
    pub fn decode<R: BitRead>(&self, reader: &mut R) -> RarResult<u16> {
        if self.max_length == 0 {
            return Err(RarError::InvalidHuffmanTable);
        }

        // Peek 16 left-aligned bits (or fewer if near end of stream), matching
        // unrar's `getbits() & 0xfffe` hot path.
        let bit_field = reader.peek_16_left_aligned()?;

        // Quick path: if bit_field is below the threshold for quick_bits length.
        let quick_bits = self.quick_bits as usize;
        if bit_field < self.decode_len[quick_bits] {
            let code = (bit_field >> (16 - self.quick_bits)) as usize;
            let len = self.quick_len[code];
            if len != 0 {
                reader.consume_bits(len)?;
                return Ok(self.quick_num[code]);
            }
        }

        // Slow path: linear scan of thresholds to find bit length.
        let mut bits = self.max_length as usize;
        for i in (quick_bits + 1)..bits {
            if bit_field < self.decode_len[i] {
                bits = i;
                break;
            }
        }

        reader.consume_bits(bits as u8)?;

        // Calculate position in decode_num.
        let dist = bit_field.wrapping_sub(self.decode_len[bits - 1]);
        let dist = dist >> (16 - bits);
        let pos = (self.decode_pos[bits] + dist) as usize;

        // Safety check for corrupt data (matching unrar: Pos=0 if out of bounds).
        let pos = if pos >= self.num_symbols { 0 } else { pos };

        Ok(self.decode_num[pos])
    }

    /// Slice-reader specialization of `DecodeNumber`, organized to follow
    /// unrar's `getbits()` / `addbits()` control flow as directly as possible.
    #[inline(always)]
    pub fn decode_bitreader(&self, reader: &mut BitReader<'_>) -> RarResult<u16> {
        if self.max_length == 0 {
            return Err(RarError::InvalidHuffmanTable);
        }

        let bit_field = reader.getbits()?;
        let quick_bits = self.quick_bits as usize;

        if bit_field < self.decode_len[quick_bits] {
            let code = (bit_field >> (16 - self.quick_bits)) as usize;
            let bits = self.quick_len[code];
            if bits != 0 {
                reader.addbits(bits)?;
                return Ok(self.quick_num[code]);
            }
        }

        let mut bits = 15usize;
        for i in (quick_bits + 1)..15 {
            if bit_field < self.decode_len[i] {
                bits = i;
                break;
            }
        }

        reader.addbits(bits as u8)?;

        let dist = bit_field.wrapping_sub(self.decode_len[bits - 1]);
        let dist = dist >> (16 - bits);
        let pos = (self.decode_pos[bits] + dist) as usize;
        let pos = if pos >= self.num_symbols { 0 } else { pos };

        Ok(self.decode_num[pos])
    }

    /// Returns the number of symbols this table can decode.
    pub fn num_symbols(&self) -> usize {
        self.num_symbols
    }

    /// Returns the maximum code length.
    pub fn max_length(&self) -> u8 {
        self.max_length
    }
}

#[inline(always)]
fn quick_bits_for_size(num_symbols: usize) -> u8 {
    match num_symbols {
        306 | 299 | 298 => MAX_QUICK_BITS,
        _ if MAX_QUICK_BITS > 3 => MAX_QUICK_BITS - 3,
        _ => 0,
    }
}

fn build_tables_from_combined_lengths(
    all_lengths: &[u8],
) -> RarResult<(HuffmanTable, HuffmanTable, HuffmanTable, HuffmanTable)> {
    let mut offset = 0;
    let nc_table = HuffmanTable::build(&all_lengths[offset..offset + HUFF_NC])?;
    offset += HUFF_NC;
    let dc_table = HuffmanTable::build(&all_lengths[offset..offset + HUFF_DC])?;
    offset += HUFF_DC;
    let ldc_table = HuffmanTable::build(&all_lengths[offset..offset + HUFF_LDC])?;
    offset += HUFF_LDC;
    let rc_table = HuffmanTable::build(&all_lengths[offset..offset + HUFF_RC])?;

    Ok((nc_table, dc_table, ldc_table, rc_table))
}

/// Read the 5 Huffman tables from the bitstream (called at the start of an LZ block
/// when table_present is set).
///
/// `prev_lengths` carries code lengths across blocks for delta encoding.
/// On first call, pass a zero-initialized vec of length `HUFF_NC + HUFF_DC + HUFF_LDC + HUFF_RC`.
///
/// Returns (nc_table, dc_table, ldc_table, rc_table).
pub fn read_tables<R: BitRead>(
    reader: &mut R,
    prev_lengths: &mut [u8],
) -> RarResult<(HuffmanTable, HuffmanTable, HuffmanTable, HuffmanTable)> {
    let mut bc_lengths = [0u8; HUFF_BC];
    let mut i = 0;
    while i < HUFF_BC {
        let length = reader.read_bits(4)? as u8;
        if length == 15 {
            let mut zero_count = reader.read_bits(4)? as usize;
            if zero_count == 0 {
                bc_lengths[i] = 15;
            } else {
                zero_count += 2;
                while zero_count > 0 && i < HUFF_BC {
                    bc_lengths[i] = 0;
                    i += 1;
                    zero_count -= 1;
                }
                continue;
            }
        } else {
            bc_lengths[i] = length;
        }
        i += 1;
    }
    let bc_table = HuffmanTable::build(&bc_lengths)?;

    let total_symbols = HUFF_NC + HUFF_DC + HUFF_LDC + HUFF_RC;
    let all_lengths = prev_lengths;
    let mut i = 0;

    while i < total_symbols {
        if reader.bits_remaining() == 0 {
            break;
        }

        let sym = bc_table.decode(reader)? as usize;

        if sym < 16 {
            all_lengths[i] = sym as u8;
            i += 1;
        } else if sym < 18 {
            let count = if sym == 16 {
                reader.read_bits(3)? as usize + 3
            } else {
                reader.read_bits(7)? as usize + 11
            };
            let value = if i == 0 { 0 } else { all_lengths[i - 1] };
            for _ in 0..count {
                if i >= total_symbols {
                    break;
                }
                all_lengths[i] = value;
                i += 1;
            }
        } else {
            let count = if sym == 18 {
                reader.read_bits(3)? as usize + 3
            } else {
                reader.read_bits(7)? as usize + 11
            };
            for _ in 0..count {
                if i >= total_symbols {
                    break;
                }
                all_lengths[i] = 0;
                i += 1;
            }
        }
    }

    build_tables_from_combined_lengths(all_lengths)
}

pub fn read_tables_bitreader(
    reader: &mut BitReader<'_>,
    prev_lengths: &mut [u8],
) -> RarResult<(HuffmanTable, HuffmanTable, HuffmanTable, HuffmanTable)> {
    let mut bc_lengths = [0u8; HUFF_BC];
    let mut i = 0usize;
    while i < HUFF_BC {
        let length = (reader.getbits()? >> 12) as u8;
        reader.addbits(4)?;
        if length == 15 {
            let mut zero_count = (reader.getbits()? >> 12) as usize;
            reader.addbits(4)?;
            if zero_count == 0 {
                bc_lengths[i] = 15;
            } else {
                zero_count += 2;
                while zero_count > 0 && i < HUFF_BC {
                    bc_lengths[i] = 0;
                    i += 1;
                    zero_count -= 1;
                }
                continue;
            }
        } else {
            bc_lengths[i] = length;
        }
        i += 1;
    }

    let bc_table = HuffmanTable::build(&bc_lengths)?;
    let total_symbols = HUFF_NC + HUFF_DC + HUFF_LDC + HUFF_RC;
    let all_lengths = prev_lengths;
    let mut i = 0usize;

    while i < total_symbols {
        if !reader.has_bits() {
            break;
        }

        let number = bc_table.decode_bitreader(reader)? as usize;
        if number < 16 {
            all_lengths[i] = number as u8;
            i += 1;
        } else if number < 18 {
            let count = if number == 16 {
                ((reader.getbits()? >> 13) as usize) + 3
            } else {
                ((reader.getbits()? >> 9) as usize) + 11
            };
            reader.addbits(if number == 16 { 3 } else { 7 })?;
            let value = if i == 0 { 0 } else { all_lengths[i - 1] };
            for _ in 0..count {
                if i >= total_symbols {
                    break;
                }
                all_lengths[i] = value;
                i += 1;
            }
        } else {
            let count = if number == 18 {
                ((reader.getbits()? >> 13) as usize) + 3
            } else {
                ((reader.getbits()? >> 9) as usize) + 11
            };
            reader.addbits(if number == 18 { 3 } else { 7 })?;
            for _ in 0..count {
                if i >= total_symbols {
                    break;
                }
                all_lengths[i] = 0;
                i += 1;
            }
        }
    }

    build_tables_from_combined_lengths(all_lengths)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pack_bits(bit_string: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut current = 0u8;
        let mut count = 0u8;

        for bit in bit_string.bytes() {
            current <<= 1;
            if bit == b'1' {
                current |= 1;
            }
            count += 1;
            if count == 8 {
                bytes.push(current);
                current = 0;
                count = 0;
            }
        }

        if count > 0 {
            current <<= 8 - count;
            bytes.push(current);
        }

        bytes
    }

    #[test]
    fn test_build_empty_table() {
        let lengths = [0u8; 10];
        let table = HuffmanTable::build(&lengths).unwrap();
        assert_eq!(table.max_length, 0);
        assert_eq!(table.num_symbols(), 10);
    }

    #[test]
    fn test_single_symbol() {
        // One symbol with length 1: code 0
        let mut lengths = [0u8; 4];
        lengths[0] = 1;
        let table = HuffmanTable::build(&lengths).unwrap();
        assert_eq!(table.max_length, 1);

        // Bit 0 should decode to symbol 0
        let data = [0x00]; // 00000000
        let mut reader = BitReader::new(&data);
        let sym = table.decode(&mut reader).unwrap();
        assert_eq!(sym, 0);
    }

    #[test]
    fn test_single_symbol_invalid_quick_prefixes_remain_empty() {
        let mut lengths = [0u8; 4];
        lengths[0] = 1;
        let table = HuffmanTable::build(&lengths).unwrap();

        let quick_bits = table.quick_bits as usize;
        let quick_limit = table.decode_len[quick_bits];
        for code in 0..(1usize << quick_bits) {
            let bit_field = (code as u32) << (16 - quick_bits);
            if bit_field >= quick_limit {
                assert_eq!(table.quick_len[code], 0);
            }
        }
    }

    #[test]
    fn test_two_symbols() {
        // Two symbols: symbol 0 = length 1 (code 0), symbol 1 = length 1 (code 1)
        let mut lengths = [0u8; 4];
        lengths[0] = 1;
        lengths[1] = 1;
        let table = HuffmanTable::build(&lengths).unwrap();

        // 0b10 = symbol 1, then symbol 0
        let data = [0b10000000];
        let mut reader = BitReader::new(&data);
        assert_eq!(table.decode(&mut reader).unwrap(), 1);
        assert_eq!(table.decode(&mut reader).unwrap(), 0);
    }

    #[test]
    fn test_canonical_three_symbols() {
        // Symbol A(0): length 1 -> code 0
        // Symbol B(1): length 2 -> code 10
        // Symbol C(2): length 2 -> code 11
        let mut lengths = [0u8; 3];
        lengths[0] = 1; // A
        lengths[1] = 2; // B
        lengths[2] = 2; // C

        let table = HuffmanTable::build(&lengths).unwrap();

        // Encode: A(0) B(10) C(11) A(0) = 0_10_11_0_0 = 0b01011000
        let data = [0b01011000];
        let mut reader = BitReader::new(&data);
        assert_eq!(table.decode(&mut reader).unwrap(), 0); // A
        assert_eq!(table.decode(&mut reader).unwrap(), 1); // B
        assert_eq!(table.decode(&mut reader).unwrap(), 2); // C
        assert_eq!(table.decode(&mut reader).unwrap(), 0); // A
    }

    #[test]
    fn test_decode_deeper_tree() {
        // Build a table with varied lengths to test tree walk.
        // sym 0: len 1 -> code 0
        // sym 1: len 3 -> code 100
        // sym 2: len 3 -> code 101
        // sym 3: len 3 -> code 110
        // sym 4: len 3 -> code 111
        let mut lengths = [0u8; 5];
        lengths[0] = 1;
        lengths[1] = 3;
        lengths[2] = 3;
        lengths[3] = 3;
        lengths[4] = 3;

        let table = HuffmanTable::build(&lengths).unwrap();

        // Encode: sym0(0) sym4(111) sym2(101)
        // = 0_111_101_? = 0b01111010
        let data = [0b01111010];
        let mut reader = BitReader::new(&data);
        assert_eq!(table.decode(&mut reader).unwrap(), 0);
        assert_eq!(table.decode(&mut reader).unwrap(), 4);
        assert_eq!(table.decode(&mut reader).unwrap(), 2);
    }

    #[test]
    fn test_build_invalid_length() {
        let mut lengths = [0u8; 4];
        lengths[0] = 20; // too long
        assert!(HuffmanTable::build(&lengths).is_err());
    }

    #[test]
    fn test_bc_table_constants() {
        assert_eq!(HUFF_BC, 20);
        assert_eq!(HUFF_NC, 306);
        assert_eq!(HUFF_DC, 64);
        assert_eq!(HUFF_LDC, 16);
        assert_eq!(HUFF_RC, 44);
    }

    #[test]
    fn test_total_symbol_count() {
        // Verify the total matches what read_tables expects
        assert_eq!(HUFF_NC + HUFF_DC + HUFF_LDC + HUFF_RC, 430);
    }

    #[test]
    fn test_empty_table_decode_fails() {
        // Decoding from an empty table should fail
        let lengths = [0u8; 8];
        let table = HuffmanTable::build(&lengths).unwrap();
        let data = [0xFF];
        let mut reader = BitReader::new(&data);
        assert!(table.decode(&mut reader).is_err());
    }

    #[test]
    fn test_large_table() {
        // Build a table with many symbols where all lengths fit in quick bits
        let mut lengths = [0u8; 256];
        for item in lengths.iter_mut().take(128) {
            *item = 8;
        }
        for item in lengths.iter_mut().take(192).skip(128) {
            *item = 9;
        }
        for item in lengths.iter_mut().skip(192) {
            *item = 10;
        }

        let table = HuffmanTable::build(&lengths).unwrap();
        assert_eq!(table.num_symbols(), 256);
        assert_eq!(table.max_length, 10);
    }

    #[test]
    fn test_table_with_tree_walk() {
        // Build a table where some codes exceed quick bits
        // 4 symbols: lengths 1, 2, 3, 11
        // This forces one symbol through the slow path
        let mut lengths = [0u8; 4];
        lengths[0] = 1; // code: 0
        lengths[1] = 3; // code: 100
        lengths[2] = 3; // code: 101
        lengths[3] = 11; // long code requiring slow path

        let table = HuffmanTable::build(&lengths).unwrap();
        assert_eq!(table.max_length, 11);

        // Verify symbol 0 decodes correctly (length 1, code 0)
        let data = [0x00]; // 00000000
        let mut reader = BitReader::new(&data);
        assert_eq!(table.decode(&mut reader).unwrap(), 0);
    }

    #[test]
    fn test_sequential_decode() {
        // Build table: sym 0=len 2, sym 1=len 2, sym 2=len 2, sym 3=len 2
        // Codes: 00, 01, 10, 11
        let lengths = [2u8; 4];
        let table = HuffmanTable::build(&lengths).unwrap();

        // Encode: sym0(00) sym1(01) sym2(10) sym3(11) = 0b00011011
        let data = [0b00011011];
        let mut reader = BitReader::new(&data);
        assert_eq!(table.decode(&mut reader).unwrap(), 0);
        assert_eq!(table.decode(&mut reader).unwrap(), 1);
        assert_eq!(table.decode(&mut reader).unwrap(), 2);
        assert_eq!(table.decode(&mut reader).unwrap(), 3);
        assert!(reader.is_empty());
    }

    #[test]
    fn test_bc_table_sym14_vs_sym19() {
        // Exact BC table from our test fixture.
        let bc_lengths: [u8; 20] = [6, 0, 5, 4, 3, 4, 4, 4, 3, 4, 6, 4, 4, 4, 4, 0, 5, 6, 6, 4];
        let table = HuffmanTable::build(&bc_lengths).unwrap();

        // sym 14: code=12=1100 (len 4), sym 19: code=13=1101 (len 4)
        // Code 1101 → sym 19
        let data_19 = [0b11010000]; // 1101 0000
        let mut r = BitReader::new(&data_19);
        let sym = table.decode(&mut r).unwrap();
        assert_eq!(sym, 19, "code 1101 should be sym 19, got {}", sym);
        assert_eq!(r.position(), 4);

        // Code 1100 → sym 14
        let data_14 = [0b11000000]; // 1100 0000
        let mut r = BitReader::new(&data_14);
        let sym = table.decode(&mut r).unwrap();
        assert_eq!(sym, 14, "code 1100 should be sym 14, got {}", sym);
        assert_eq!(r.position(), 4);
    }

    #[test]
    fn test_decode_bitreader_matches_generic_decode() {
        let mut lengths = [0u8; 5];
        lengths[0] = 1;
        lengths[1] = 3;
        lengths[2] = 3;
        lengths[3] = 3;
        lengths[4] = 3;

        let table = HuffmanTable::build(&lengths).unwrap();
        let data = [0b01111010, 0b00000000];
        let mut generic = BitReader::new(&data);
        let mut ported = BitReader::new(&data);

        for _ in 0..3 {
            assert_eq!(
                table.decode(&mut generic).unwrap(),
                table.decode_bitreader(&mut ported).unwrap()
            );
            assert_eq!(generic.position(), ported.position());
        }
    }

    #[test]
    fn test_read_tables_bitreader_matches_generic() {
        let mut bits = String::new();

        for _ in 0..18 {
            bits.push_str("0000");
        }
        bits.push_str("0001");
        bits.push_str("0001");

        for _ in 0..3 {
            bits.push('1');
            bits.push_str("1111111");
        }
        for _ in 0..2 {
            bits.push('0');
            bits.push_str("101");
        }

        let data = pack_bits(&bits);
        let mut prev_generic = vec![0u8; HUFF_NC + HUFF_DC + HUFF_LDC + HUFF_RC];
        let mut prev_ported = prev_generic.clone();
        let mut generic = BitReader::new(&data);
        let mut ported = BitReader::new(&data);

        let (generic_nc, generic_dc, generic_ldc, generic_rc) =
            read_tables(&mut generic, &mut prev_generic).unwrap();
        let (ported_nc, ported_dc, ported_ldc, ported_rc) =
            read_tables_bitreader(&mut ported, &mut prev_ported).unwrap();

        assert_eq!(generic.position(), ported.position());
        assert_eq!(prev_generic, prev_ported);
        assert_eq!(generic_nc.max_length(), ported_nc.max_length());
        assert_eq!(generic_dc.max_length(), ported_dc.max_length());
        assert_eq!(generic_ldc.max_length(), ported_ldc.max_length());
        assert_eq!(generic_rc.max_length(), ported_rc.max_length());
    }
}
