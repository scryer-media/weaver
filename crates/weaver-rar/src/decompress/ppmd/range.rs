//! Range (arithmetic) decoder for PPMd.
//!
//! PPMd uses a range coder (a variant of arithmetic coding) to encode symbols.
//! The range coder maintains a `low` value and a `range` value. As symbols are
//! decoded, the range is narrowed and new bytes are shifted in from the input.
//!
//! Reference: Dmitry Shkarin's public-domain PPMd code, 7-zip (public domain).

use crate::error::{RarError, RarResult};

/// Scale factor for frequency counts — range is normalized to [0, SCALE).
pub const SCALE: u32 = 1 << 15; // 32768

/// Range decoder state.
pub struct RangeDecoder<'a> {
    input: &'a [u8],
    pos: usize,
    low: u32,
    code: u32,
    range: u32,
}

impl<'a> RangeDecoder<'a> {
    /// Initialize the range decoder from a byte stream.
    ///
    /// Reads the first 4 bytes to initialize the `code` register.
    pub fn new(input: &'a [u8]) -> RarResult<Self> {
        if input.len() < 4 {
            return Err(RarError::CorruptArchive {
                detail: "PPMd input too short for range decoder init".into(),
            });
        }

        let code = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);

        Ok(Self {
            input,
            pos: 4,
            low: 0,
            code,
            range: u32::MAX,
        })
    }

    /// Read one byte from the input stream.
    fn read_byte(&mut self) -> u8 {
        if self.pos < self.input.len() {
            let b = self.input[self.pos];
            self.pos += 1;
            b
        } else {
            0 // past end — return 0 (matches reference implementations)
        }
    }

    /// Normalize the range decoder state.
    ///
    /// When `range` drops below 2^24, shift in new bytes to keep precision.
    #[inline]
    fn normalize(&mut self) {
        while self.range < (1 << 24) {
            self.code = (self.code << 8) | self.read_byte() as u32;
            self.range <<= 8;
            self.low <<= 8;
        }
    }

    /// Get the current count value within [0, scale).
    ///
    /// Returns `(code - low) / (range / scale)`, which indicates where the
    /// current code point falls within the probability range.
    #[inline]
    pub fn get_current_count(&self, scale: u32) -> u32 {
        (self.code.wrapping_sub(self.low)) / (self.range / scale)
    }

    /// Get the current "threshold" for binary symbol decisions.
    /// Used by SEE: returns (code - low) / (range / scale).
    #[inline]
    pub fn get_threshold(&self, scale: u32) -> u32 {
        self.get_current_count(scale)
    }

    /// Decode a symbol given cumulative frequency bounds.
    ///
    /// After determining which symbol the current code falls into (using
    /// `get_current_count`), call this with:
    /// - `cum_freq`: cumulative frequency of symbols before this one
    /// - `freq`: frequency of this symbol
    /// - `scale`: total frequency scale
    pub fn decode(&mut self, cum_freq: u32, freq: u32, scale: u32) {
        let r = self.range / scale;
        self.low = self.low.wrapping_add(cum_freq.wrapping_mul(r));
        self.range = freq.wrapping_mul(r);
        self.normalize();
    }

    /// Decode a binary symbol (used in SEE path).
    ///
    /// `freq0` is the frequency of symbol 0 out of `scale`.
    /// Returns true if symbol 0 was selected.
    pub fn decode_binary(&mut self, freq0: u32, scale: u32) -> bool {
        let threshold = self.get_threshold(scale);
        if threshold < freq0 {
            self.decode(0, freq0, scale);
            true
        } else {
            self.decode(freq0, scale - freq0, scale);
            false
        }
    }

    /// Return how many input bytes have been consumed.
    pub fn position(&self) -> usize {
        self.pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_short_input() {
        let result = RangeDecoder::new(&[0, 1, 2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_new_init() {
        let input = [0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF];
        let rd = RangeDecoder::new(&input).unwrap();
        assert_eq!(rd.code, 0);
        assert_eq!(rd.range, u32::MAX);
        assert_eq!(rd.pos, 4);
    }

    #[test]
    fn test_get_current_count() {
        let input = [0x40, 0x00, 0x00, 0x00];
        let rd = RangeDecoder::new(&input).unwrap();
        // code = 0x40000000, range = 0xFFFFFFFF
        // get_current_count(256) = 0x40000000 / (0xFFFFFFFF / 256) = ~64
        let count = rd.get_current_count(256);
        assert_eq!(count, 64);
    }

    #[test]
    fn test_decode_binary() {
        // code = 0, so threshold = 0 which is < any freq0 > 0
        let input = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut rd = RangeDecoder::new(&input).unwrap();
        assert!(rd.decode_binary(128, 256)); // symbol 0 selected
    }
}
