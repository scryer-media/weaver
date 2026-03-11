//! Huffman table construction and decoding for RAR5 LZ decompression.
//!
//! RAR5 uses 5 canonical Huffman tables:
//! - `HUFF_BC = 20` — bit-length bootstrap codes (used to decode other tables)
//! - `HUFF_NC = 306` — literal/length codes (256 literals + 50 length codes)
//! - `HUFF_DC = 64` — distance codes
//! - `HUFF_LDC = 16` — lower distance bits codes
//! - `HUFF_RC = 44` — repeating/cached distance codes
//!
//! Table construction from libarchive reference (BSD-licensed constants):
//! Tables are built from arrays of bit-lengths using canonical Huffman coding.
//! Zero-run encoding: when a bit-length value is 15, the next 4 bits encode
//! a run of zeros.

use crate::error::{RarError, RarResult};

use super::bitstream::BitReader;

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

/// Lookup table size for fast decoding (2^MAX_CODE_LENGTH would be too large;
/// we use a smaller table for quick lookup and fall back to tree walk).
/// For tables up to 306 entries, a 10-bit quick table is a good tradeoff.
const QUICK_BITS: u8 = 10;
const QUICK_TABLE_SIZE: usize = 1 << QUICK_BITS;

/// A single entry in the quick-lookup table.
#[derive(Clone, Copy, Default)]
struct QuickEntry {
    /// The decoded symbol, or 0xFFFF if this entry requires tree walk.
    symbol: u16,
    /// Number of bits consumed for this symbol.
    length: u8,
}

/// A Huffman decoding table built from an array of bit-lengths.
pub struct HuffmanTable {
    /// Quick lookup table for codes up to QUICK_BITS long.
    quick: Vec<QuickEntry>,
    /// For codes longer than QUICK_BITS: decode_tree[left/right child].
    /// Each node stores either a symbol (leaf) or an index to child nodes.
    /// Organized as pairs: tree[i*2] = left child, tree[i*2+1] = right child.
    /// Value >= 0 means symbol; value < 0 means -(child_pair_index).
    decode_tree: Vec<i32>,
    /// Number of tree node pairs allocated.
    #[allow(dead_code)]
    tree_nodes: usize,
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
    pub fn build(bit_lengths: &[u8]) -> RarResult<Self> {
        let num_symbols = bit_lengths.len();

        // Count codes of each length.
        let mut bl_count = [0u32; MAX_CODE_LENGTH + 1];
        let mut max_length: u8 = 0;
        for &bl in bit_lengths {
            if bl > 0 {
                if bl as usize > MAX_CODE_LENGTH {
                    return Err(RarError::InvalidHuffmanTable);
                }
                bl_count[bl as usize] += 1;
                if bl > max_length {
                    max_length = bl;
                }
            }
        }

        // If no symbols have nonzero length, the table is empty.
        if max_length == 0 {
            return Ok(Self {
                quick: vec![QuickEntry::default(); QUICK_TABLE_SIZE],
                decode_tree: Vec::new(),
                tree_nodes: 0,
                max_length: 0,
                num_symbols,
            });
        }

        // Compute the starting code for each length (canonical Huffman).
        let mut next_code = [0u32; MAX_CODE_LENGTH + 1];
        {
            let mut code = 0u32;
            for bits in 1..=MAX_CODE_LENGTH {
                code = (code + bl_count[bits - 1]) << 1;
                next_code[bits] = code;
            }
        }

        // Assign codes to symbols.
        let mut symbol_codes = vec![(0u32, 0u8); num_symbols]; // (code, length)
        for (sym, &bl) in bit_lengths.iter().enumerate() {
            if bl > 0 {
                symbol_codes[sym] = (next_code[bl as usize], bl);
                next_code[bl as usize] += 1;
            }
        }

        // Build quick lookup table.
        let mut quick = vec![
            QuickEntry {
                symbol: 0xFFFF,
                length: 0,
            };
            QUICK_TABLE_SIZE
        ];

        // Pre-allocate tree for codes longer than QUICK_BITS.
        // Each unique QUICK_BITS prefix gets its own subtree.
        // Tree is stored as pairs: tree[idx*2] = left, tree[idx*2+1] = right.
        // Positive value = symbol (leaf). Negative value = -(child pair index).
        // Zero = uninitialized node.
        let mut decode_tree: Vec<i32> = Vec::new();
        let mut tree_nodes: usize = 0; // pairs allocated

        for (sym, &(code, length)) in symbol_codes.iter().enumerate() {
            if length == 0 {
                continue;
            }

            if length <= QUICK_BITS {
                // Fill all quick table entries that share this prefix.
                let pad_bits = QUICK_BITS - length;
                let base_index = (code as usize) << pad_bits;
                let count = 1usize << pad_bits;
                for j in 0..count {
                    let idx = base_index + j;
                    if idx < QUICK_TABLE_SIZE {
                        quick[idx] = QuickEntry {
                            symbol: sym as u16,
                            length,
                        };
                    }
                }
            } else {
                // Codes longer than QUICK_BITS: use tree.
                // The QUICK_BITS prefix selects a tree root via the quick table.
                let prefix = (code >> (length - QUICK_BITS)) as usize;

                // Get or create a tree root for this prefix.
                let tree_root = if prefix < QUICK_TABLE_SIZE && quick[prefix].length > QUICK_BITS {
                    // Already has a tree root.
                    quick[prefix].symbol as usize
                } else {
                    // Allocate a new tree root pair.
                    let root = tree_nodes;
                    tree_nodes += 1;
                    let needed = root * 2 + 2;
                    if needed > decode_tree.len() {
                        decode_tree.resize(needed, 0);
                    }
                    if prefix < QUICK_TABLE_SIZE {
                        quick[prefix] = QuickEntry {
                            symbol: root as u16,
                            length: QUICK_BITS + 1, // sentinel: tree walk required
                        };
                    }
                    root
                };

                // Walk the tree for bits after QUICK_BITS.
                let suffix_len = length - QUICK_BITS;
                let mut node_idx = tree_root;

                for bit_idx in (0..suffix_len).rev() {
                    let bit = ((code >> bit_idx) & 1) as usize;
                    let slot = node_idx * 2 + bit;

                    // Ensure tree is large enough.
                    if slot >= decode_tree.len() {
                        decode_tree.resize(slot + 2, 0);
                    }

                    if bit_idx == 0 {
                        // Leaf: store symbol (encoded as sym + 1 so 0 stays "empty").
                        decode_tree[slot] = (sym as i32) + 1;
                    } else if decode_tree[slot] == 0 {
                        // Allocate a new child node pair.
                        let new_node = tree_nodes;
                        tree_nodes += 1;
                        let needed = new_node * 2 + 2;
                        if needed > decode_tree.len() {
                            decode_tree.resize(needed, 0);
                        }
                        decode_tree[slot] = -(new_node as i32) - 1;
                        node_idx = new_node;
                    } else {
                        // Existing internal node.
                        node_idx = (-(decode_tree[slot] + 1)) as usize;
                    }
                }
            }
        }

        Ok(Self {
            quick,
            decode_tree,
            tree_nodes,
            max_length,
            num_symbols,
        })
    }

    /// Decode the next symbol from the bitstream.
    pub fn decode(&self, reader: &mut BitReader) -> RarResult<u16> {
        if self.max_length == 0 {
            return Err(RarError::InvalidHuffmanTable);
        }

        let bits_avail = reader.bits_remaining();
        if bits_avail == 0 {
            return Err(RarError::CorruptArchive {
                detail: "huffman: no bits remaining".into(),
            });
        }

        // Try quick lookup first.
        let peek_len = (QUICK_BITS as usize)
            .min(self.max_length as usize)
            .min(bits_avail) as u8;
        let peeked = reader.peek_bits(peek_len)?;
        let index = if peek_len < QUICK_BITS {
            (peeked as usize) << (QUICK_BITS - peek_len)
        } else {
            peeked as usize
        };

        let entry = &self.quick[index];
        if entry.length > 0 && entry.length <= QUICK_BITS {
            // Direct hit in quick table.
            if (entry.length as usize) <= bits_avail {
                reader.skip_bits(entry.length as u32)?;
                return Ok(entry.symbol);
            }
        } else if entry.length > QUICK_BITS {
            // Tree walk required. Skip the QUICK_BITS prefix.
            reader.skip_bits(peek_len as u32)?;
            let mut node_idx = entry.symbol as usize; // tree root for this prefix

            let remaining_bits = self.max_length - QUICK_BITS;
            for _ in 0..remaining_bits {
                if reader.bits_remaining() == 0 {
                    return Err(RarError::CorruptArchive {
                        detail: "huffman: truncated code".into(),
                    });
                }
                let bit = reader.read_bit()? as usize;
                let slot = node_idx * 2 + bit;
                if slot >= self.decode_tree.len() {
                    return Err(RarError::CorruptArchive {
                        detail: "huffman: invalid code in tree".into(),
                    });
                }

                let val = self.decode_tree[slot];
                if val > 0 {
                    // Leaf: symbol = val - 1.
                    return Ok((val - 1) as u16);
                } else if val < 0 {
                    // Internal node: follow to child.
                    node_idx = (-(val + 1)) as usize;
                } else {
                    // Uninitialized node — invalid code.
                    return Err(RarError::CorruptArchive {
                        detail: "huffman: invalid code (uninitialized tree node)".into(),
                    });
                }
            }

            return Err(RarError::CorruptArchive {
                detail: "huffman: code exceeds max length".into(),
            });
        }

        Err(RarError::CorruptArchive {
            detail: "huffman: invalid code".into(),
        })
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

/// Read the 5 Huffman tables from the bitstream (called at the start of an LZ block
/// when table_present is set).
///
/// `prev_lengths` carries code lengths across blocks for delta encoding.
/// On first call, pass a zero-initialized vec of length `HUFF_NC + HUFF_DC + HUFF_LDC + HUFF_RC`.
///
/// Returns (nc_table, dc_table, ldc_table, rc_table).
pub fn read_tables(
    reader: &mut BitReader,
    prev_lengths: &mut [u8],
) -> RarResult<(HuffmanTable, HuffmanTable, HuffmanTable, HuffmanTable)> {
    // Step 1: Read 20 x 4-bit lengths for the BC (bit-length code) table.
    // Special encoding: if a 4-bit value is 0xF, read 4 more bits as skip count.
    // If count > 0, skip (count+1) entries (all zeros). If count == 0, value is 15.
    let mut bc_lengths = [0u8; HUFF_BC];
    let mut i = 0;
    while i < HUFF_BC {
        let n = reader.read_bits(4)? as u8;
        if n == 0x0F {
            let cnt = reader.read_bits(4)? as usize;
            if cnt > 0 {
                // Skip cnt+2 entries (zero-fill), matching 7-zip's zeroCount += 2.
                i += cnt + 2;
                continue;
            }
            // cnt == 0: value is literally 15.
        }
        if i < HUFF_BC {
            bc_lengths[i] = n;
        }
        i += 1;
    }
    let bc_table = HuffmanTable::build(&bc_lengths)?;

    // Step 2: Use BC table to decode lengths for NC, DC, LDC, RC tables.
    // Symbols 0-15: direct bit length values.
    // Symbols 16-19 are RLE codes:
    //   16: repeat previous, count = readBits(3) + 3  (3-10)
    //   17: repeat previous, count = readBits(7) + 11 (11-138)
    //   18: zero fill,       count = readBits(3) + 3  (3-10)
    //   19: zero fill,       count = readBits(7) + 11 (11-138)
    let total_symbols = HUFF_NC + HUFF_DC + HUFF_LDC + HUFF_RC;
    let all_lengths = prev_lengths;
    let mut i = 0;

    while i < total_symbols {
        if reader.bits_remaining() == 0 {
            break;
        }

        let sym = bc_table.decode(reader)? as usize;

        if sym < 16 {
            // Direct bit length value.
            all_lengths[i] = sym as u8;
            i += 1;
        } else {
            // RLE code: determine count and fill value.
            let count = match sym {
                16 | 18 => 3 + reader.read_bits(3)? as usize, // short: 3-10
                _ => 11 + reader.read_bits(7)? as usize,      // long: 11-138 (17, 19)
            };
            let value = if sym < 18 {
                // Symbols 16-17: repeat previous value.
                if i == 0 {
                    return Err(RarError::CorruptArchive {
                        detail: "huffman: repeat code at start of table".into(),
                    });
                }
                all_lengths[i - 1]
            } else {
                // Symbols 18-19: fill with zeros.
                0
            };
            for _ in 0..count {
                if i >= total_symbols {
                    break;
                }
                all_lengths[i] = value;
                i += 1;
            }
        }
    }

    // Split the combined lengths into individual tables.
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

#[cfg(test)]
mod tests {
    use super::*;

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
        // Build a table with many symbols where all lengths fit in QUICK_BITS
        let mut lengths = [0u8; 256];
        // 128 symbols at length 8, 64 at length 9, 64 at length 10
        // All fit within QUICK_BITS (10), so no tree walk needed
        for i in 0..128 {
            lengths[i] = 8;
        }
        for i in 128..192 {
            lengths[i] = 9;
        }
        for i in 192..256 {
            lengths[i] = 10;
        }

        let table = HuffmanTable::build(&lengths).unwrap();
        assert_eq!(table.num_symbols(), 256);
        assert_eq!(table.max_length, 10);
    }

    #[test]
    fn test_table_with_tree_walk() {
        // Build a table where some codes exceed QUICK_BITS (10)
        // 4 symbols: lengths 1, 2, 3, 11
        // This forces one symbol through the tree walk path
        let mut lengths = [0u8; 4];
        lengths[0] = 1; // code: 0
        lengths[1] = 3; // code: 100
        lengths[2] = 3; // code: 101
        lengths[3] = 11; // long code requiring tree walk

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
}
