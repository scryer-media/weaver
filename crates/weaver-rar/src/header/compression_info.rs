//! Bit-packed compression info decoder.
//!
//! Compression information is stored as a vint with bit fields:
//! - Bits 0-5: Version (currently 0)
//! - Bit 6: Solid flag
//! - Bits 7-9: Method (0=store, 1-5=compression levels)
//! - Bits 10-13: Dictionary size code

use crate::types::{CompressionInfo, CompressionMethod};

/// Decode compression info from the raw vint value.
pub fn decode_compression_info(raw: u64) -> CompressionInfo {
    let version = (raw & 0x3F) as u8; // bits 0-5
    let solid = (raw >> 6) & 1 != 0; // bit 6
    let method_code = ((raw >> 7) & 0x07) as u8; // bits 7-9
    let dict_code = ((raw >> 10) & 0x0F) as u8; // bits 10-13

    let method = CompressionMethod::from_code(method_code);

    // Dictionary size: 128KB << dict_code
    let dict_size: u64 = 128 * 1024 * (1u64 << dict_code);

    CompressionInfo {
        version,
        solid,
        method,
        dict_size,
    }
}

/// Encode compression info to a raw vint value (for testing).
#[cfg(test)]
pub fn encode_compression_info(info: &CompressionInfo) -> u64 {
    let mut raw: u64 = 0;
    raw |= (info.version as u64) & 0x3F;
    if info.solid {
        raw |= 1 << 6;
    }
    raw |= (info.method.code() as u64 & 0x07) << 7;

    // Reverse dict_size to code: dict_size = 128KB << code
    let base = 128u64 * 1024;
    let mut code = 0u8;
    if info.dict_size >= base {
        let mut s = info.dict_size / base;
        while s > 1 {
            s >>= 1;
            code += 1;
        }
    }
    raw |= (code as u64 & 0x0F) << 10;
    raw
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_no_solid() {
        let raw = 0u64; // version=0, solid=false, method=0(store), dict_code=0(128KB)
        let info = decode_compression_info(raw);
        assert_eq!(info.version, 0);
        assert!(!info.solid);
        assert_eq!(info.method, CompressionMethod::Store);
        assert_eq!(info.dict_size, 128 * 1024);
    }

    #[test]
    fn test_normal_compression() {
        // method=3 (normal), dict_code=5 (128KB << 5 = 4MB)
        let raw = (3u64 << 7) | (5u64 << 10);
        let info = decode_compression_info(raw);
        assert_eq!(info.version, 0);
        assert!(!info.solid);
        assert_eq!(info.method, CompressionMethod::Normal);
        assert_eq!(info.dict_size, 128 * 1024 * 32); // 4MB
    }

    #[test]
    fn test_solid_flag() {
        let raw = 1u64 << 6;
        let info = decode_compression_info(raw);
        assert!(info.solid);
    }

    #[test]
    fn test_version_field() {
        let raw = 5u64; // version=5
        let info = decode_compression_info(raw);
        assert_eq!(info.version, 5);
    }

    #[test]
    fn test_dict_size_codes() {
        for code in 0..15u8 {
            let raw = (code as u64) << 10;
            let info = decode_compression_info(raw);
            let expected = 128u64 * 1024 * (1u64 << code);
            assert_eq!(info.dict_size, expected, "dict code {code}");
        }
    }

    #[test]
    fn test_roundtrip() {
        let info = CompressionInfo {
            version: 0,
            solid: true,
            method: CompressionMethod::Best,
            dict_size: 128 * 1024 * 64, // 8MB = code 6
        };
        let raw = encode_compression_info(&info);
        let decoded = decode_compression_info(raw);
        assert_eq!(decoded.version, info.version);
        assert_eq!(decoded.solid, info.solid);
        assert_eq!(decoded.method, info.method);
        assert_eq!(decoded.dict_size, info.dict_size);
    }

    #[test]
    fn test_all_methods() {
        for method_code in 0..6u8 {
            let raw = (method_code as u64) << 7;
            let info = decode_compression_info(raw);
            assert_eq!(info.method.code(), method_code);
        }
    }
}
