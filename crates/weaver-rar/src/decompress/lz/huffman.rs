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

        // Pre-allocate a tree for longer codes.
        // Worst case: each code longer than QUICK_BITS needs nodes.
        let initial_tree_size = (num_symbols * 4).max(64);
        let mut decode_tree = vec![0i32; initial_tree_size];
        // Node 0 is the root pair.
        let mut tree_nodes: usize = 1; // pairs allocated

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
                // For codes longer than QUICK_BITS, store in the tree.
                // The first QUICK_BITS direct us to a tree entry, then we
                // walk bit by bit.
                let prefix = (code >> (length - QUICK_BITS)) as usize;
                if prefix < QUICK_TABLE_SIZE {
                    // Mark the quick entry as requiring tree walk.
                    // Store the tree root node index negated.
                    if quick[prefix].symbol == 0xFFFF && quick[prefix].length == 0 {
                        // First time: create a tree root for this prefix.
                        quick[prefix].length = QUICK_BITS + 1; // sentinel: means "use tree"
                    }
                }

                // Walk the tree from the root for bits after QUICK_BITS.
                let mut node_idx: usize = 0; // root pair
                for bit_idx in (0..(length - QUICK_BITS)).rev() {
                    let bit = ((code >> bit_idx) & 1) as usize;
                    let slot = node_idx.checked_mul(2).unwrap_or(usize::MAX)
                        .checked_add(bit).unwrap_or(usize::MAX);

                    // Ensure tree is large enough.
                    while slot >= decode_tree.len() {
                        let new_len = decode_tree.len().saturating_mul(2).max(slot.saturating_add(2));
                        decode_tree.resize(new_len, 0);
                    }

                    if bit_idx == 0 {
                        // Leaf: store symbol.
                        decode_tree[slot] = sym as i32;
                    } else if decode_tree[slot] <= 0 && bit_idx > 0 {
                        if decode_tree[slot] == 0 {
                            // Allocate a new node pair.
                            let new_node = tree_nodes;
                            tree_nodes += 1;
                            let needed = new_node.checked_mul(2)
                                .unwrap_or(usize::MAX)
                                .checked_add(2).unwrap_or(usize::MAX);
                            while needed > decode_tree.len() {
                                let new_len = decode_tree.len().saturating_mul(2).max(needed);
                                decode_tree.resize(new_len, 0);
                            }
                            decode_tree[slot] = -(new_node as i32);
                        }
                        node_idx = (-decode_tree[slot]) as usize;
                    } else {
                        node_idx = (-decode_tree[slot]) as usize;
                    }
                }
            }
        }

        decode_tree.truncate(tree_nodes * 2);

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

        // Try quick lookup first.
        let peek_count = QUICK_BITS.min(self.max_length);
        let bits_avail = reader.bits_remaining();
        if bits_avail == 0 {
            return Err(RarError::CorruptArchive {
                detail: "huffman: no bits remaining".into(),
            });
        }

        let peek_len = peek_count.min(bits_avail as u8);
        let peeked = reader.peek_bits(peek_len)?;
        // Pad to QUICK_BITS for table lookup.
        let index = if peek_len < QUICK_BITS {
            (peeked as usize) << (QUICK_BITS - peek_len)
        } else {
            peeked as usize
        };

        if index < QUICK_TABLE_SIZE {
            let entry = &self.quick[index];
            if entry.symbol != 0xFFFF && entry.length > 0 && entry.length <= QUICK_BITS {
                if (entry.length as usize) <= bits_avail {
                    reader.skip_bits(entry.length as u32)?;
                    return Ok(entry.symbol);
                }
            }
        }

        // Fall back to tree walk for longer codes.
        // We need to read bit by bit past the quick prefix.
        if self.tree_nodes <= 1 && self.max_length <= QUICK_BITS {
            // No tree nodes and max_length fits in quick table -- symbol not found.
            return Err(RarError::CorruptArchive {
                detail: "huffman: invalid code".into(),
            });
        }

        // Skip the QUICK_BITS we already peeked, then walk the tree.
        let mut node_idx: usize = 0;
        reader.skip_bits(peek_len as u32)?;

        // Walk remaining bits.
        let remaining_bits = self.max_length - peek_len;
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
            if val < 0 {
                // Internal node: follow to child.
                node_idx = (-val) as usize;
            } else {
                // Leaf: return symbol.
                return Ok(val as u16);
            }
        }

        Err(RarError::CorruptArchive {
            detail: "huffman: code exceeds max length".into(),
        })
    }

    /// Returns the number of symbols this table can decode.
    pub fn num_symbols(&self) -> usize {
        self.num_symbols
    }
}

/// Read the 5 Huffman tables from the bitstream (called at the start of an LZ block
/// when keepOldTable is false).
///
/// Returns (nc_table, dc_table, ldc_table, rc_table).
pub fn read_tables(reader: &mut BitReader) -> RarResult<(HuffmanTable, HuffmanTable, HuffmanTable, HuffmanTable)> {
    // Step 1: Read 20 x 4-bit lengths for the BC (bit-length code) table.
    let mut bc_lengths = [0u8; HUFF_BC];
    for bl in &mut bc_lengths {
        *bl = reader.read_bits(4)? as u8;
    }
    let bc_table = HuffmanTable::build(&bc_lengths)?;

    // Step 2: Use BC table to decode lengths for NC, DC, LDC, RC tables.
    let total_symbols = HUFF_NC + HUFF_DC + HUFF_LDC + HUFF_RC;
    let mut all_lengths = vec![0u8; total_symbols];
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
        } else if sym == 16 {
            // Repeat previous length 3 + next 2 bits times.
            if i == 0 {
                return Err(RarError::CorruptArchive {
                    detail: "huffman: repeat code at start of table".into(),
                });
            }
            let count = 3 + reader.read_bits(2)? as usize;
            let prev = all_lengths[i - 1];
            for _ in 0..count {
                if i >= total_symbols {
                    break;
                }
                all_lengths[i] = prev;
                i += 1;
            }
        } else if sym == 17 {
            // Run of 3 + next 3 bits zeros.
            let count = 3 + reader.read_bits(3)? as usize;
            for _ in 0..count {
                if i >= total_symbols {
                    break;
                }
                all_lengths[i] = 0;
                i += 1;
            }
        } else if sym == 18 {
            // Run of 11 + next 7 bits zeros.
            let count = 11 + reader.read_bits(7)? as usize;
            for _ in 0..count {
                if i >= total_symbols {
                    break;
                }
                all_lengths[i] = 0;
                i += 1;
            }
        } else {
            return Err(RarError::CorruptArchive {
                detail: format!("huffman: invalid BC symbol {sym}"),
            });
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
        lengths[0] = 1;  // code: 0
        lengths[1] = 3;  // code: 100
        lengths[2] = 3;  // code: 101
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
}
