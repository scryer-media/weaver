//! Variable-length integer (vint) decoder for RAR5.
//!
//! RAR5 uses vints to encode integers up to 64 bits. Each byte contributes
//! 7 data bits (bits 0-6); bit 7 is a continuation flag (1 = more bytes follow).
//! Maximum encoding length is 10 bytes.

use std::io::Read;

use crate::error::{RarError, RarResult};

/// Maximum number of bytes a vint can occupy.
const VINT_MAX_BYTES: usize = 10;

/// Read a vint from a byte slice, returning (value, bytes_consumed).
pub fn read_vint(data: &[u8]) -> RarResult<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    for (i, &byte) in data.iter().enumerate().take(VINT_MAX_BYTES) {
        let value_bits = (byte & 0x7F) as u64;
        result |= value_bits << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            // No continuation bit - done
            return Ok((result, i + 1));
        }

        if i == VINT_MAX_BYTES - 1 {
            // We've consumed the maximum bytes and still have continuation
            return Err(RarError::InvalidVint { offset: 0 });
        }
    }

    // Not enough data
    Err(RarError::TruncatedHeader { offset: 0 })
}

/// Read a vint from a reader, returning (value, bytes_consumed).
pub fn read_vint_from<R: Read>(reader: &mut R) -> RarResult<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    let mut buf = [0u8; 1];

    for i in 0..VINT_MAX_BYTES {
        reader.read_exact(&mut buf).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                RarError::TruncatedHeader { offset: 0 }
            } else {
                RarError::Io(e)
            }
        })?;

        let byte = buf[0];
        let value_bits = (byte & 0x7F) as u64;
        result |= value_bits << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }

        if i == VINT_MAX_BYTES - 1 {
            return Err(RarError::InvalidVint { offset: 0 });
        }
    }

    Err(RarError::TruncatedHeader { offset: 0 })
}

/// Encode a u64 as a vint, returning the bytes.
#[cfg(test)]
pub fn encode_vint(mut value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if value == 0 {
            break;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vint_zero() {
        let data = [0x00];
        let (val, consumed) = read_vint(&data).unwrap();
        assert_eq!(val, 0);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_vint_single_byte() {
        // Value 127 = 0x7F (max single byte)
        let data = [0x7F];
        let (val, consumed) = read_vint(&data).unwrap();
        assert_eq!(val, 127);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_vint_two_bytes() {
        // Value 128: low 7 bits = 0 with continuation, then 1
        let data = [0x80, 0x01];
        let (val, consumed) = read_vint(&data).unwrap();
        assert_eq!(val, 128);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_vint_two_bytes_value_300() {
        // 300 = 0b100101100
        // low 7 bits: 0101100 = 0x2C, with continuation = 0xAC
        // next 7 bits: 0000010 = 0x02
        let data = [0xAC, 0x02];
        let (val, consumed) = read_vint(&data).unwrap();
        assert_eq!(val, 300);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_vint_roundtrip() {
        let test_values: &[u64] = &[
            0,
            1,
            127,
            128,
            255,
            256,
            300,
            16383,
            16384,
            65535,
            1_000_000,
            u32::MAX as u64,
            u64::MAX,
        ];

        for &value in test_values {
            let encoded = encode_vint(value);
            let (decoded, consumed) = read_vint(&encoded).unwrap();
            assert_eq!(decoded, value, "roundtrip failed for {value}");
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn test_vint_max_u64() {
        let encoded = encode_vint(u64::MAX);
        assert!(encoded.len() <= 10);
        let (decoded, _) = read_vint(&encoded).unwrap();
        assert_eq!(decoded, u64::MAX);
    }

    #[test]
    fn test_vint_with_trailing_data() {
        // vint 1 followed by extra bytes
        let data = [0x01, 0xFF, 0xFF];
        let (val, consumed) = read_vint(&data).unwrap();
        assert_eq!(val, 1);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_vint_truncated() {
        // Continuation bit set but no more data
        let data = [0x80];
        let result = read_vint(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_vint_from_reader() {
        let data = [0xAC, 0x02];
        let mut cursor = std::io::Cursor::new(&data);
        let (val, consumed) = read_vint_from(&mut cursor).unwrap();
        assert_eq!(val, 300);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_vint_padding_with_0x80() {
        // 0x80 bytes are padding (value 0 with continuation)
        // 0x80, 0x80, 0x01 = value at shift 14: 1 << 14 = 16384
        let data = [0x80, 0x80, 0x01];
        let (val, consumed) = read_vint(&data).unwrap();
        assert_eq!(val, 16384);
        assert_eq!(consumed, 3);
    }

    #[test]
    fn test_encode_vint_specific() {
        assert_eq!(encode_vint(0), vec![0x00]);
        assert_eq!(encode_vint(1), vec![0x01]);
        assert_eq!(encode_vint(127), vec![0x7F]);
        assert_eq!(encode_vint(128), vec![0x80, 0x01]);
        assert_eq!(encode_vint(300), vec![0xAC, 0x02]);
    }
}
