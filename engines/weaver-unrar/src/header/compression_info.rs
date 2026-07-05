//! Bit-packed compression info decoder.
//!
//! Compression information is stored as a vint with bit fields:
//! - Bits 0-5: Compression algorithm selector (`0 = RAR5`, `1 = RAR7`)
//! - Bit 6: Solid flag
//! - Bits 7-9: Method (0=store, 1-5=compression levels)
//! - Bits 10-13: RAR5 dictionary size code
//! - Bits 10-14: RAR7 dictionary size code
//! - Bits 15-19: RAR7 fractional dictionary size in 1/32 units
//! - Bit 20: `FCI_RAR5_COMPAT`

use crate::error::{RarError, RarResult};
use crate::limits::RAR_UNPACK_MAX_DICT_SIZE;
use crate::types::{CompressionInfo, CompressionMethod};

const RAR5_DICT_BASE: u64 = 128 * 1024;
const FCI_RAR5_COMPAT: u64 = 0x0010_0000;

fn rar5_dict_size(dict_code: u8) -> RarResult<u64> {
    RAR5_DICT_BASE
        .checked_shl(dict_code as u32)
        .ok_or_else(|| RarError::CorruptArchive {
            detail: format!("RAR5 dictionary code {dict_code} overflowed"),
        })
}

fn rar7_dict_size(raw: u64) -> RarResult<u64> {
    let dict_code = ((raw >> 10) & 0x1F) as u8;
    let fraction = (raw >> 15) & 0x1F;
    let base = rar5_dict_size(dict_code)?;
    Ok(base + (base / 32) * fraction)
}

/// Decode compression info from the raw vint value.
pub fn decode_compression_info(raw: u64) -> RarResult<CompressionInfo> {
    decode_compression_info_for_file(raw, false)
}

pub(crate) fn decode_compression_info_for_file(
    raw: u64,
    is_directory: bool,
) -> RarResult<CompressionInfo> {
    let raw_version = (raw & 0x3F) as u8; // bits 0-5
    let solid = (raw >> 6) & 1 != 0; // bit 6
    let method_code = ((raw >> 7) & 0x07) as u8; // bits 7-9

    let (version, dict_size) = match raw_version {
        0 if is_directory => (0, 0),
        0 => (0, rar5_dict_size(((raw >> 10) & 0x0F) as u8)?),
        1 if is_directory => (1, 0),
        1 => {
            let version = if raw & FCI_RAR5_COMPAT != 0 { 0 } else { 1 };
            (version, rar7_dict_size(raw)?)
        }
        other if method_code == 0 => (other, 0),
        other => {
            return Err(RarError::UnsupportedCompression {
                method: method_code,
                version: other,
            });
        }
    };
    if dict_size > RAR_UNPACK_MAX_DICT_SIZE {
        return Err(RarError::DictionaryTooLarge {
            size: dict_size,
            max: RAR_UNPACK_MAX_DICT_SIZE,
        });
    }

    Ok(CompressionInfo {
        format: crate::types::ArchiveFormat::Rar5,
        version,
        solid,
        method: CompressionMethod::from_code(method_code),
        dict_size,
    })
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
        let info = decode_compression_info(raw).unwrap();
        assert_eq!(info.version, 0);
        assert!(!info.solid);
        assert_eq!(info.method, CompressionMethod::Store);
        assert_eq!(info.dict_size, 128 * 1024);
    }

    #[test]
    fn test_normal_compression() {
        // method=3 (normal), dict_code=5 (128KB << 5 = 4MB)
        let raw = (3u64 << 7) | (5u64 << 10);
        let info = decode_compression_info(raw).unwrap();
        assert_eq!(info.version, 0);
        assert!(!info.solid);
        assert_eq!(info.method, CompressionMethod::Normal);
        assert_eq!(info.dict_size, 128 * 1024 * 32); // 4MB
    }

    #[test]
    fn test_solid_flag() {
        let raw = 1u64 << 6;
        let info = decode_compression_info(raw).unwrap();
        assert!(info.solid);
    }

    #[test]
    fn test_version_field() {
        let raw = 5u64 | (3u64 << 7); // version=5, method=normal
        let err = decode_compression_info(raw).unwrap_err();
        assert!(matches!(
            err,
            crate::error::RarError::UnsupportedCompression { version: 5, .. }
        ));
    }

    #[test]
    fn test_unknown_version_store_is_allowed_like_rar_behavior() {
        let raw = 5u64; // version=5, method=store
        let info = decode_compression_info(raw).unwrap();
        assert_eq!(info.version, 5);
        assert_eq!(info.method, CompressionMethod::Store);
        assert_eq!(info.dict_size, 0);
    }

    #[test]
    fn test_dict_size_codes() {
        for code in 0..15u8 {
            let raw = (code as u64) << 10;
            let info = decode_compression_info(raw).unwrap();
            let expected = 128u64 * 1024 * (1u64 << code);
            assert_eq!(info.dict_size, expected, "dict code {code}");
        }
    }

    #[test]
    fn test_roundtrip() {
        let info = CompressionInfo {
            format: crate::types::ArchiveFormat::Rar5,
            version: 0,
            solid: true,
            method: CompressionMethod::Best,
            dict_size: 128 * 1024 * 64, // 8MB = code 6
        };
        let raw = encode_compression_info(&info);
        let decoded = decode_compression_info(raw).unwrap();
        assert_eq!(decoded.version, info.version);
        assert_eq!(decoded.solid, info.solid);
        assert_eq!(decoded.method, info.method);
        assert_eq!(decoded.dict_size, info.dict_size);
    }

    #[test]
    fn test_all_methods() {
        for method_code in 0..6u8 {
            let raw = (method_code as u64) << 7;
            let info = decode_compression_info(raw).unwrap();
            assert_eq!(info.method.code(), method_code);
        }
    }

    #[test]
    fn test_rar7_fractional_dict_size() {
        let raw = 1u64 | (5u64 << 10) | (3u64 << 15);
        let info = decode_compression_info(raw).unwrap();
        let base = 128u64 * 1024 * (1u64 << 5);
        assert_eq!(info.version, 1);
        assert_eq!(info.dict_size, base + (base / 32) * 3);
    }

    #[test]
    fn test_rar7_compat_uses_rar5_algorithm_version() {
        let raw = 1u64 | FCI_RAR5_COMPAT | (4u64 << 10) | (7u64 << 15);
        let info = decode_compression_info(raw).unwrap();
        let base = 128u64 * 1024 * (1u64 << 4);
        assert_eq!(info.version, 0);
        assert_eq!(info.dict_size, base + (base / 32) * 7);
    }

    #[test]
    fn test_rar7_dictionary_above_rar_unpack_max_is_rejected() {
        let raw = 1u64 | (20u64 << 10);
        let err = decode_compression_info(raw).unwrap_err();
        assert!(matches!(
            err,
            RarError::DictionaryTooLarge {
                size,
                max: RAR_UNPACK_MAX_DICT_SIZE
            } if size > RAR_UNPACK_MAX_DICT_SIZE
        ));
    }

    #[test]
    fn test_directory_ignores_dictionary_size_like_rar_behavior() {
        let raw = 1u64 | FCI_RAR5_COMPAT | (20u64 << 10);
        let info = decode_compression_info_for_file(raw, true).unwrap();
        assert_eq!(info.version, 1);
        assert_eq!(info.method, CompressionMethod::Store);
        assert_eq!(info.dict_size, 0);
    }
}
