//! Bit-level reader over a byte slice using a 64-bit accumulator.
//!
//! Reads individual bits and multi-bit values from a compressed data stream.
//! Uses a 64-bit register that is bulk-refilled from the source, eliminating
//! per-bit bounds checks in the hot path. This matches the approach used by
//! production decompressors (zlib, zstd, brotli).
//!
//! Bits are read MSB-first within each byte, matching RAR's bitstream convention.

use std::io::Read;

use crate::error::{RarError, RarResult};

// Keep the streaming reader buffer modest. Large RAR5 members already need the
// dictionary window; additional multi-megabyte input staging is pure overhead.
const STREAMING_INPUT_BUFFER_SIZE: usize = 0x80000;

pub trait BitRead {
    fn bits_remaining(&mut self) -> usize;
    fn has_bits(&mut self) -> bool;
    fn position(&self) -> usize;
    fn byte_position(&self) -> usize {
        self.position() / 8
    }
    fn read_bits(&mut self, count: u8) -> RarResult<u32>;
    fn peek_16_left_aligned(&mut self) -> RarResult<u32>;
    fn consume_bits(&mut self, count: u8) -> RarResult<()>;
    fn skip_bits(&mut self, count: u32) -> RarResult<()>;
    fn align_byte(&mut self) -> RarResult<()>;
}

/// Bit-level reader with a 64-bit accumulator for fast bit extraction.
///
/// The accumulator holds up to 56 bits (7 bytes) of pre-loaded data.
/// Bits are stored left-aligned in the u64: the next bit to read is
/// always at bit 63 (MSB). `peek_bits(n)` is a single right-shift,
/// and `skip_bits(n)` is a left-shift + counter decrement.
#[derive(Clone)]
pub struct BitReader<'a> {
    /// Source data.
    data: &'a [u8],
    /// Next byte to load from data into the accumulator.
    byte_pos: usize,
    /// Left-aligned accumulator. Next bit to read is at bit 63.
    acc: u64,
    /// Number of valid bits currently in the accumulator (0..=64).
    acc_bits: u8,
}

impl<'a> BitReader<'a> {
    /// Create a new BitReader over the given byte slice.
    pub fn new(data: &'a [u8]) -> Self {
        let mut reader = Self {
            data,
            byte_pos: 0,
            acc: 0,
            acc_bits: 0,
        };
        reader.refill();
        reader
    }

    /// Refill the accumulator from the source data.
    ///
    /// Loads bytes until we have at least 56 bits (or the source is exhausted).
    /// This is designed to be called infrequently — typically every ~4-7 bytes
    /// consumed, not on every bit read.
    #[inline]
    fn refill(&mut self) {
        // Fast path: load 8 bytes at once if available.
        // We can always fit more bytes when acc_bits <= 56.
        while self.acc_bits <= 56 && self.byte_pos < self.data.len() {
            // Place the new byte into the correct position (left-aligned,
            // just below the existing valid bits).
            self.acc |= (self.data[self.byte_pos] as u64) << (56 - self.acc_bits);
            self.byte_pos += 1;
            self.acc_bits += 8;
        }
    }

    /// Returns the total number of bits remaining (accumulator + unread source).
    #[inline]
    pub fn bits_remaining(&self) -> usize {
        self.acc_bits as usize + (self.data.len() - self.byte_pos) * 8
    }

    /// Returns true if there are no more bits to read.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.acc_bits == 0 && self.byte_pos >= self.data.len()
    }

    /// Returns true if the accumulator has at least one bit ready.
    /// Cheaper than `bits_remaining()` — no multiplication.
    #[inline]
    pub fn has_bits(&self) -> bool {
        self.acc_bits > 0 || self.byte_pos < self.data.len()
    }

    /// Returns the current position in bits from the start.
    #[inline]
    pub fn position(&self) -> usize {
        // Total bits in source minus bits we haven't consumed yet.
        self.data.len() * 8 - self.bits_remaining()
    }

    /// Returns the current byte position (rounded down from bit position).
    #[inline]
    pub fn byte_position(&self) -> usize {
        self.position() / 8
    }

    /// Read a single bit. Returns 0 or 1.
    #[inline]
    pub fn read_bit(&mut self) -> RarResult<u32> {
        if self.acc_bits == 0 {
            return Err(RarError::CorruptArchive {
                detail: "bitstream: unexpected end of data".into(),
            });
        }
        let bit = (self.acc >> 63) as u32;
        self.acc <<= 1;
        self.acc_bits -= 1;
        self.refill();
        Ok(bit)
    }

    /// Read N bits (up to 32), MSB-first.
    ///
    /// Returns the bits packed into the lower N bits of a u32.
    #[inline]
    pub fn read_bits(&mut self, count: u8) -> RarResult<u32> {
        debug_assert!(count <= 32);
        if count == 0 {
            return Ok(0);
        }
        if (self.acc_bits as usize) < count as usize && self.bits_remaining() < count as usize {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "bitstream: need {} bits but only {} remaining",
                    count,
                    self.bits_remaining()
                ),
            });
        }
        // If accumulator doesn't have enough, refill first.
        // This shouldn't normally happen since refill keeps acc_bits >= 56,
        // but handles edge cases near end of stream.
        if self.acc_bits < count {
            self.refill();
            if self.acc_bits < count {
                return Err(RarError::CorruptArchive {
                    detail: format!(
                        "bitstream: need {} bits but only {} remaining",
                        count,
                        self.bits_remaining()
                    ),
                });
            }
        }

        // Extract top `count` bits from accumulator.
        let result = (self.acc >> (64 - count as u32)) as u32;
        self.acc <<= count as u32;
        self.acc_bits -= count;
        self.refill();
        Ok(result)
    }

    /// Peek at the next N bits without advancing the position (up to 32 bits).
    #[inline]
    pub fn peek_bits(&self, count: u8) -> RarResult<u32> {
        debug_assert!(count <= 32);
        if count == 0 {
            return Ok(0);
        }
        if (self.acc_bits as usize) < count as usize {
            // Near end of stream — not enough in accumulator.
            // We can't refill (self is &self), so fall back to checking total.
            if self.bits_remaining() < count as usize {
                return Err(RarError::CorruptArchive {
                    detail: format!(
                        "bitstream: need {} bits to peek but only {} remaining",
                        count,
                        self.bits_remaining()
                    ),
                });
            }
            // We have enough bits total but not in the accumulator.
            // This can happen when acc_bits < count at end of stream.
            // Build the result by combining acc + next source bytes.
            let mut result = if self.acc_bits > 0 {
                (self.acc >> (64 - self.acc_bits as u32)) as u32
            } else {
                0
            };
            let have = self.acc_bits;
            let need = count - have;
            let mut bp = self.byte_pos;
            let mut remaining = need;
            while remaining > 0 && bp < self.data.len() {
                let byte = self.data[bp] as u32;
                bp += 1;
                if remaining >= 8 {
                    result = (result << 8) | byte;
                    remaining -= 8;
                } else {
                    result = (result << remaining) | (byte >> (8 - remaining));
                    remaining = 0;
                }
            }
            return Ok(result);
        }
        // Fast path: just shift the accumulator.
        Ok((self.acc >> (64 - count as u32)) as u32)
    }

    /// Peek up to 16 bits and return them left-aligned in a 16-bit field.
    ///
    /// This matches unrar's hot `getbits() & 0xfffe` use in Huffman decode.
    #[inline]
    pub fn peek_16_left_aligned(&self) -> RarResult<u32> {
        if self.acc_bits >= 16 {
            return Ok(((self.acc >> 48) as u32) & 0xfffe);
        }

        let bits_avail = self.bits_remaining();
        if bits_avail == 0 {
            return Err(RarError::CorruptArchive {
                detail: "bitstream: unexpected end of data".into(),
            });
        }

        let peek_count = 16.min(bits_avail) as u8;
        let raw = self.peek_bits(peek_count)?;
        Ok(if peek_count < 16 {
            (raw << (16 - peek_count)) & 0xfffe
        } else {
            raw & 0xfffe
        })
    }

    /// Consume a small number of bits using the accumulator fast path when possible.
    #[inline]
    pub fn consume_bits(&mut self, count: u8) -> RarResult<()> {
        debug_assert!(count <= 32);
        if count == 0 {
            return Ok(());
        }

        if self.acc_bits >= count {
            self.acc <<= count as u32;
            self.acc_bits -= count;
            self.refill();
            return Ok(());
        }

        self.skip_bits(count as u32)
    }

    /// Skip N bits.
    #[inline]
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
        let mut remaining = count;
        // Consume from accumulator first.
        if remaining <= self.acc_bits as u32 {
            if remaining < 64 {
                self.acc <<= remaining;
            } else {
                self.acc = 0;
            }
            self.acc_bits -= remaining as u8;
            self.refill();
            return Ok(());
        }
        // Drain accumulator.
        remaining -= self.acc_bits as u32;
        self.acc = 0;
        self.acc_bits = 0;
        // Skip full bytes directly.
        let skip_bytes = (remaining / 8) as usize;
        self.byte_pos += skip_bytes;
        remaining -= (skip_bytes * 8) as u32;
        // Refill and skip remaining bits.
        self.refill();
        if remaining > 0 {
            self.acc <<= remaining;
            self.acc_bits -= remaining as u8;
            self.refill();
        }
        Ok(())
    }

    /// Align to the next byte boundary by skipping remaining bits in the current byte.
    pub fn align_byte(&mut self) {
        let bit_offset = self.position() % 8;
        if bit_offset != 0 {
            let skip = 8 - bit_offset;
            // We know we have at least `skip` bits (we're mid-byte).
            if skip as u8 <= self.acc_bits {
                self.acc <<= skip as u32;
                self.acc_bits -= skip as u8;
                self.refill();
            }
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
        // Calculate the source byte position accounting for bits in the accumulator.
        // byte_pos is where we'd next load from, but we have acc_bits still buffered.
        // The "consumed" position in the source is: byte_pos - ceil(acc_bits / 8)
        // But for remaining_bytes we want to return from the next byte boundary
        // after the current bit position.
        let bit_position = self.position();
        let start = if bit_position.is_multiple_of(8) {
            bit_position / 8
        } else {
            bit_position / 8 + 1
        };
        if start >= self.data.len() {
            &[]
        } else {
            &self.data[start..]
        }
    }
}

impl BitRead for BitReader<'_> {
    fn bits_remaining(&mut self) -> usize {
        BitReader::bits_remaining(self)
    }

    fn has_bits(&mut self) -> bool {
        BitReader::has_bits(self)
    }

    fn position(&self) -> usize {
        BitReader::position(self)
    }

    fn read_bits(&mut self, count: u8) -> RarResult<u32> {
        BitReader::read_bits(self, count)
    }

    fn peek_16_left_aligned(&mut self) -> RarResult<u32> {
        BitReader::peek_16_left_aligned(self)
    }

    fn consume_bits(&mut self, count: u8) -> RarResult<()> {
        BitReader::consume_bits(self, count)
    }

    fn skip_bits(&mut self, count: u32) -> RarResult<()> {
        BitReader::skip_bits(self, count)
    }

    fn align_byte(&mut self) -> RarResult<()> {
        BitReader::align_byte(self);
        Ok(())
    }
}

pub struct StreamingBitReader<R: Read> {
    inner: R,
    buf: Box<[u8]>,
    buf_pos: usize,
    buf_len: usize,
    acc: u64,
    acc_bits: u8,
    eof: bool,
    bit_pos: usize,
}

impl<R: Read> StreamingBitReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: vec![0u8; STREAMING_INPUT_BUFFER_SIZE].into_boxed_slice(),
            buf_pos: 0,
            buf_len: 0,
            acc: 0,
            acc_bits: 0,
            eof: false,
            bit_pos: 0,
        }
    }

    fn fill_buffer(&mut self) -> RarResult<()> {
        if self.buf_pos < self.buf_len || self.eof {
            return Ok(());
        }

        let n = self.inner.read(&mut self.buf).map_err(RarError::Io)?;
        self.buf_pos = 0;
        self.buf_len = n;
        if n == 0 {
            self.eof = true;
        }
        Ok(())
    }

    fn refill(&mut self) -> RarResult<()> {
        while self.acc_bits <= 56 {
            self.fill_buffer()?;
            if self.buf_pos >= self.buf_len {
                break;
            }
            self.acc |= (self.buf[self.buf_pos] as u64) << (56 - self.acc_bits);
            self.buf_pos += 1;
            self.acc_bits += 8;
        }
        Ok(())
    }

    fn peek_bits(&mut self, count: u8) -> RarResult<u32> {
        debug_assert!(count <= 32);
        if count == 0 {
            return Ok(0);
        }

        self.refill()?;

        let available = self.acc_bits as usize + (self.buf_len - self.buf_pos) * 8;
        if available < count as usize && self.eof {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "bitstream: need {} bits to peek but only {} remaining",
                    count, available
                ),
            });
        }

        if self.acc_bits >= count {
            return Ok((self.acc >> (64 - count as u32)) as u32);
        }

        let mut result = if self.acc_bits > 0 {
            (self.acc >> (64 - self.acc_bits as u32)) as u32
        } else {
            0
        };
        let mut need = count - self.acc_bits;
        let mut pos = self.buf_pos;

        while need > 0 {
            if pos >= self.buf_len {
                if self.eof {
                    break;
                }
                return Err(RarError::CorruptArchive {
                    detail: "bitstream: truncated buffered read".into(),
                });
            }
            let byte = self.buf[pos] as u32;
            pos += 1;
            if need >= 8 {
                result = (result << 8) | byte;
                need -= 8;
            } else {
                result = (result << need) | (byte >> (8 - need));
                need = 0;
            }
        }

        Ok(result)
    }
}

impl<R: Read> BitRead for StreamingBitReader<R> {
    fn bits_remaining(&mut self) -> usize {
        let _ = self.refill();
        let buffered = self.acc_bits as usize + (self.buf_len.saturating_sub(self.buf_pos)) * 8;
        if self.eof {
            buffered
        } else {
            buffered.saturating_add(64)
        }
    }

    fn has_bits(&mut self) -> bool {
        let _ = self.refill();
        self.acc_bits > 0 || self.buf_pos < self.buf_len
    }

    fn position(&self) -> usize {
        self.bit_pos
    }

    fn read_bits(&mut self, count: u8) -> RarResult<u32> {
        debug_assert!(count <= 32);
        if count == 0 {
            return Ok(0);
        }

        self.refill()?;
        let available = self.acc_bits as usize + (self.buf_len - self.buf_pos) * 8;
        if available < count as usize && self.eof {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "bitstream: need {} bits but only {} remaining",
                    count, available
                ),
            });
        }

        if self.acc_bits < count {
            self.refill()?;
            let available = self.acc_bits as usize + (self.buf_len - self.buf_pos) * 8;
            if available < count as usize && self.eof {
                return Err(RarError::CorruptArchive {
                    detail: format!(
                        "bitstream: need {} bits but only {} remaining",
                        count, available
                    ),
                });
            }
        }

        let result = self.peek_bits(count)?;
        self.consume_bits(count)?;
        Ok(result)
    }

    fn peek_16_left_aligned(&mut self) -> RarResult<u32> {
        self.refill()?;
        let bits_avail = self.acc_bits as usize + (self.buf_len - self.buf_pos) * 8;
        if bits_avail == 0 {
            return Err(RarError::CorruptArchive {
                detail: "bitstream: unexpected end of data".into(),
            });
        }
        let peek_count = 16.min(bits_avail) as u8;
        let raw = self.peek_bits(peek_count)?;
        Ok(if peek_count < 16 {
            (raw << (16 - peek_count)) & 0xfffe
        } else {
            raw & 0xfffe
        })
    }

    fn consume_bits(&mut self, count: u8) -> RarResult<()> {
        debug_assert!(count <= 32);
        if count == 0 {
            return Ok(());
        }

        self.refill()?;
        let available = self.acc_bits as usize + (self.buf_len - self.buf_pos) * 8;
        if available < count as usize && self.eof {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "bitstream: cannot consume {} bits, only {} remaining",
                    count, available
                ),
            });
        }

        if self.acc_bits < count {
            self.refill()?;
        }
        if self.acc_bits < count {
            return Err(RarError::CorruptArchive {
                detail: "bitstream: truncated code".into(),
            });
        }

        self.acc <<= count as u32;
        self.acc_bits -= count;
        self.bit_pos += count as usize;
        self.refill()?;
        Ok(())
    }

    fn skip_bits(&mut self, count: u32) -> RarResult<()> {
        let mut remaining = count;
        while remaining > 0 {
            let step = remaining.min(32) as u8;
            self.consume_bits(step)?;
            remaining -= step as u32;
        }
        Ok(())
    }

    fn align_byte(&mut self) -> RarResult<()> {
        let bit_offset = self.position() % 8;
        if bit_offset != 0 {
            self.consume_bits((8 - bit_offset) as u8)?;
        }
        Ok(())
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
