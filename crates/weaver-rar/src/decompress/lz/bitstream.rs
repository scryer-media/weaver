//! Bit-level reader over a byte slice.
//!
//! Reads individual bits and multi-bit values from a compressed data stream,
//! tracking byte position and bit offset within the current byte.

use crate::error::{RarError, RarResult};

/// Bit-level reader that wraps a byte slice.
///
/// Bits are read MSB-first within each byte, which matches RAR5's bitstream
/// convention. The reader tracks a byte position and a bit offset (0-7)
/// within the current byte.
pub struct BitReader<'a> {
    /// Source data.
    data: &'a [u8],
    /// Current byte position in the data.
    byte_pos: usize,
    /// Bit offset within the current byte (0 = MSB, 7 = LSB).
    bit_pos: u8,
}

impl<'a> BitReader<'a> {
    /// Create a new BitReader over the given byte slice.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    /// Returns the total number of bits remaining.
    pub fn bits_remaining(&self) -> usize {
        if self.byte_pos >= self.data.len() {
            return 0;
        }
        (self.data.len() - self.byte_pos) * 8 - self.bit_pos as usize
    }

    /// Returns true if there are no more bits to read.
    pub fn is_empty(&self) -> bool {
        self.bits_remaining() == 0
    }

    /// Returns the current position in bits from the start.
    pub fn position(&self) -> usize {
        self.byte_pos * 8 + self.bit_pos as usize
    }

    /// Returns the current byte position (rounded down from bit position).
    pub fn byte_position(&self) -> usize {
        self.byte_pos
    }

    /// Read a single bit. Returns 0 or 1.
    pub fn read_bit(&mut self) -> RarResult<u32> {
        if self.byte_pos >= self.data.len() {
            return Err(RarError::CorruptArchive {
                detail: "bitstream: unexpected end of data".into(),
            });
        }
        let byte = self.data[self.byte_pos];
        // MSB-first: bit_pos 0 reads the highest bit
        let bit = (byte >> (7 - self.bit_pos)) & 1;
        self.bit_pos += 1;
        if self.bit_pos >= 8 {
            self.bit_pos = 0;
            self.byte_pos += 1;
        }
        Ok(bit as u32)
    }

    /// Read N bits (up to 32), MSB-first.
    ///
    /// Returns the bits packed into the lower N bits of a u32.
    pub fn read_bits(&mut self, count: u8) -> RarResult<u32> {
        debug_assert!(count <= 32);
        if count == 0 {
            return Ok(0);
        }
        if self.bits_remaining() < count as usize {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "bitstream: need {} bits but only {} remaining",
                    count,
                    self.bits_remaining()
                ),
            });
        }

        let mut result: u32 = 0;
        let mut remaining = count;

        while remaining > 0 {
            let byte = self.data[self.byte_pos];
            // How many bits are available in the current byte
            let avail = 8 - self.bit_pos;
            let take = remaining.min(avail);

            // Extract `take` bits starting from bit_pos, MSB-first
            // Shift byte right to align the bits we want at the LSB side,
            // then mask off `take` bits.
            let shift = avail - take;
            let mask = if take >= 8 { 0xFF } else { (1u8 << take) - 1 };
            let bits = ((byte >> shift) & mask) as u32;

            result = (result << take) | bits;
            remaining -= take;
            self.bit_pos += take;
            if self.bit_pos >= 8 {
                self.bit_pos = 0;
                self.byte_pos += 1;
            }
        }

        Ok(result)
    }

    /// Peek at the next N bits without advancing the position (up to 32 bits).
    pub fn peek_bits(&self, count: u8) -> RarResult<u32> {
        debug_assert!(count <= 32);
        if count == 0 {
            return Ok(0);
        }
        if self.bits_remaining() < count as usize {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "bitstream: need {} bits to peek but only {} remaining",
                    count,
                    self.bits_remaining()
                ),
            });
        }

        // Temporarily clone state for peeking
        let mut byte_pos = self.byte_pos;
        let mut bit_pos = self.bit_pos;
        let mut result: u32 = 0;
        let mut remaining = count;

        while remaining > 0 {
            let byte = self.data[byte_pos];
            let avail = 8 - bit_pos;
            let take = remaining.min(avail);
            let shift = avail - take;
            let mask = if take >= 8 { 0xFF } else { (1u8 << take) - 1 };
            let bits = ((byte >> shift) & mask) as u32;
            result = (result << take) | bits;
            remaining -= take;
            bit_pos += take;
            if bit_pos >= 8 {
                bit_pos = 0;
                byte_pos += 1;
            }
        }

        Ok(result)
    }

    /// Skip N bits.
    pub fn skip_bits(&mut self, count: u32) -> RarResult<()> {
        if self.bits_remaining() < count as usize {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "bitstream: cannot skip {} bits, only {} remaining",
                    count,
                    self.bits_remaining()
                ),
            });
        }
        let total_bits = self.bit_pos as u32 + count;
        self.byte_pos += (total_bits / 8) as usize;
        self.bit_pos = (total_bits % 8) as u8;
        Ok(())
    }

    /// Align to the next byte boundary by skipping remaining bits in the current byte.
    pub fn align_byte(&mut self) {
        if self.bit_pos != 0 {
            self.bit_pos = 0;
            self.byte_pos += 1;
        }
    }

    /// Read a full byte (8 bits) as u8. Must be byte-aligned or it reads
    /// across the boundary.
    pub fn read_byte(&mut self) -> RarResult<u8> {
        Ok(self.read_bits(8)? as u8)
    }

    /// Read a 32-bit little-endian value from the bitstream.
    /// This reads 4 bytes, where each byte is read MSB-first in the bitstream.
    pub fn read_u32_le(&mut self) -> RarResult<u32> {
        let b0 = self.read_bits(8)?;
        let b1 = self.read_bits(8)?;
        let b2 = self.read_bits(8)?;
        let b3 = self.read_bits(8)?;
        Ok(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24))
    }

    /// Return the remaining unconsumed bytes (aligned to next byte boundary).
    ///
    /// If the reader is mid-byte, the partial byte is skipped.
    pub fn remaining_bytes(&self) -> &'a [u8] {
        let start = if self.bit_pos == 0 {
            self.byte_pos
        } else {
            self.byte_pos + 1
        };
        if start >= self.data.len() {
            &[]
        } else {
            &self.data[start..]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_single_bits() {
        // 0xA5 = 1010_0101
        let data = [0xA5];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bit().unwrap(), 1);
        assert_eq!(reader.read_bit().unwrap(), 0);
        assert_eq!(reader.read_bit().unwrap(), 1);
        assert_eq!(reader.read_bit().unwrap(), 0);
        assert_eq!(reader.read_bit().unwrap(), 0);
        assert_eq!(reader.read_bit().unwrap(), 1);
        assert_eq!(reader.read_bit().unwrap(), 0);
        assert_eq!(reader.read_bit().unwrap(), 1);
        assert!(reader.is_empty());
    }

    #[test]
    fn test_read_bits_multi() {
        // 0xFF 0x00 = 11111111 00000000
        let data = [0xFF, 0x00];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bits(4).unwrap(), 0xF);
        assert_eq!(reader.read_bits(4).unwrap(), 0xF);
        assert_eq!(reader.read_bits(8).unwrap(), 0x00);
    }

    #[test]
    fn test_read_bits_crossing_boundary() {
        // 0b10110011 0b01100101
        let data = [0xB3, 0x65];
        let mut reader = BitReader::new(&data);
        // Read 5 bits: 10110 = 22
        assert_eq!(reader.read_bits(5).unwrap(), 0b10110);
        // Read 6 bits across boundary: 011_011 = 0b011011 = 27
        assert_eq!(reader.read_bits(6).unwrap(), 0b011011);
        // Read remaining 5 bits: 00101 = 5
        assert_eq!(reader.read_bits(5).unwrap(), 0b00101);
        assert!(reader.is_empty());
    }

    #[test]
    fn test_peek_does_not_advance() {
        let data = [0xAB]; // 10101011
        let reader = BitReader::new(&data);
        let peeked = reader.peek_bits(4).unwrap();
        assert_eq!(peeked, 0b1010);
        // Position should not change
        assert_eq!(reader.position(), 0);
        assert_eq!(reader.bits_remaining(), 8);
    }

    #[test]
    fn test_skip_bits() {
        let data = [0xFF, 0x00]; // 11111111 00000000
        let mut reader = BitReader::new(&data);
        reader.skip_bits(4).unwrap();
        assert_eq!(reader.read_bits(4).unwrap(), 0xF);
        reader.skip_bits(4).unwrap();
        assert_eq!(reader.read_bits(4).unwrap(), 0x0);
    }

    #[test]
    fn test_align_byte() {
        let data = [0xFF, 0xAA]; // 11111111 10101010
        let mut reader = BitReader::new(&data);
        reader.read_bits(3).unwrap(); // read 3 bits
        reader.align_byte(); // skip to byte boundary
        assert_eq!(reader.position(), 8);
        assert_eq!(reader.read_bits(8).unwrap(), 0xAA);
    }

    #[test]
    fn test_align_byte_already_aligned() {
        let data = [0xFF, 0xAA];
        let mut reader = BitReader::new(&data);
        reader.align_byte(); // already aligned, should be no-op
        assert_eq!(reader.position(), 0);
    }

    #[test]
    fn test_read_past_end() {
        let data = [0xFF];
        let mut reader = BitReader::new(&data);
        reader.read_bits(8).unwrap();
        assert!(reader.read_bit().is_err());
    }

    #[test]
    fn test_read_bits_zero() {
        let data = [0xFF];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bits(0).unwrap(), 0);
        assert_eq!(reader.position(), 0);
    }

    #[test]
    fn test_bits_remaining() {
        let data = [0xFF, 0x00, 0xAA];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.bits_remaining(), 24);
        reader.read_bits(5).unwrap();
        assert_eq!(reader.bits_remaining(), 19);
        reader.read_bits(8).unwrap();
        assert_eq!(reader.bits_remaining(), 11);
    }

    #[test]
    fn test_empty_data() {
        let data: &[u8] = &[];
        let reader = BitReader::new(data);
        assert!(reader.is_empty());
        assert_eq!(reader.bits_remaining(), 0);
    }

    #[test]
    fn test_read_u32_le() {
        // Bytes 0x01 0x02 0x03 0x04 as LE u32 = 0x04030201
        let data = [0x01, 0x02, 0x03, 0x04];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_u32_le().unwrap(), 0x04030201);
    }

    #[test]
    fn test_read_32_bits_at_once() {
        // 0xDEADBEEF in MSB-first bitstream
        let data = [0xDE, 0xAD, 0xBE, 0xEF];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bits(32).unwrap(), 0xDEADBEEF);
    }

    #[test]
    fn test_skip_then_read() {
        let data = [0b11001010, 0b11110000];
        let mut reader = BitReader::new(&data);
        reader.skip_bits(4).unwrap();
        // Remaining in first byte: 1010, then 11110000
        assert_eq!(reader.read_bits(8).unwrap(), 0b1010_1111);
    }

    #[test]
    fn test_peek_then_read_consistency() {
        let data = [0xAB, 0xCD, 0xEF];
        let mut reader = BitReader::new(&data);
        // Peek and read should return the same value
        let peeked = reader.peek_bits(12).unwrap();
        let read = reader.read_bits(12).unwrap();
        assert_eq!(peeked, read);
        // Position should have advanced by 12 bits
        assert_eq!(reader.position(), 12);
    }

    #[test]
    fn test_read_byte_unaligned() {
        // Read 3 bits, then a full byte crossing byte boundary
        let data = [0b11100110, 0b10100000];
        let mut reader = BitReader::new(&data);
        let first3 = reader.read_bits(3).unwrap();
        assert_eq!(first3, 0b111); // first 3 bits of 0b11100110
        let byte = reader.read_byte().unwrap();
        // Next 8 bits: 00110_101 = 0b00110101 = 0x35
        assert_eq!(byte, 0b00110101);
    }

    #[test]
    fn test_skip_to_exact_end() {
        let data = [0xFF, 0x00];
        let mut reader = BitReader::new(&data);
        reader.skip_bits(16).unwrap();
        assert!(reader.is_empty());
        assert_eq!(reader.bits_remaining(), 0);
    }

    #[test]
    fn test_skip_past_end_fails() {
        let data = [0xFF];
        let mut reader = BitReader::new(&data);
        assert!(reader.skip_bits(9).is_err());
    }

    #[test]
    fn test_read_bits_insufficient_fails() {
        let data = [0xFF];
        let mut reader = BitReader::new(&data);
        reader.read_bits(4).unwrap();
        // Only 4 bits left, asking for 5
        assert!(reader.read_bits(5).is_err());
    }
}
