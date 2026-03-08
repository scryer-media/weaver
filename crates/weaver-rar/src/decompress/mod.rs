//! Decompression dispatcher for RAR5.
//!
//! Routes compressed data to the appropriate decompressor based on the
//! compression method (Store vs LZ).

pub mod lz;
pub mod ppmd;
pub mod store;

use std::io::Write;

use crate::error::{RarError, RarResult};
use crate::types::{CompressionInfo, CompressionMethod};

/// Decompress data from a RAR5 file entry.
///
/// `input` is the raw compressed data area.
/// `unpacked_size` is the expected uncompressed size.
/// `info` provides compression parameters.
/// `expected_crc` is the CRC32 from the file header (for store verification).
///
/// Returns the decompressed data.
pub fn decompress(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    expected_crc: Option<u32>,
) -> RarResult<Vec<u8>> {
    match info.method {
        CompressionMethod::Store => {
            // Method 0: no compression, direct copy with CRC check.
            let mut output = Vec::with_capacity(input.len());
            store::decompress_store(input, &mut output, expected_crc)?;
            Ok(output)
        }
        CompressionMethod::Fastest
        | CompressionMethod::Fast
        | CompressionMethod::Normal
        | CompressionMethod::Good
        | CompressionMethod::Best => {
            // Methods 1-5: LZ compression.
            lz::decompress_lz(input, unpacked_size, info)
        }
        CompressionMethod::Unknown(code) => Err(RarError::UnsupportedCompression {
            method: code,
            version: info.version,
        }),
    }
}

/// Streaming variant: decompress directly to a writer.
///
/// For Store method, writes input directly with CRC verification.
/// For LZ methods, flushes the sliding window periodically.
/// Memory usage is bounded to dict_size instead of output size.
pub fn decompress_to_writer<W: Write>(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    expected_crc: Option<u32>,
    writer: &mut W,
) -> RarResult<u64> {
    match info.method {
        CompressionMethod::Store => {
            let written = store::decompress_store_to_writer(input, writer, expected_crc)?;
            Ok(written)
        }
        CompressionMethod::Fastest
        | CompressionMethod::Fast
        | CompressionMethod::Normal
        | CompressionMethod::Good
        | CompressionMethod::Best => lz::decompress_lz_to_writer(input, unpacked_size, info, writer),
        CompressionMethod::Unknown(code) => Err(RarError::UnsupportedCompression {
            method: code,
            version: info.version,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dispatch_store() {
        let data = b"Hello, dispatcher!";
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        let crc = hasher.finalize();

        let info = CompressionInfo {
            version: 0,
            solid: false,
            method: CompressionMethod::Store,
            dict_size: 128 * 1024,
        };

        let result = decompress(data, data.len() as u64, &info, Some(crc)).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_dispatch_unknown_method() {
        let info = CompressionInfo {
            version: 0,
            solid: false,
            method: CompressionMethod::Unknown(7),
            dict_size: 128 * 1024,
        };

        let result = decompress(&[], 0, &info, None);
        assert!(matches!(
            result,
            Err(RarError::UnsupportedCompression { method: 7, .. })
        ));
    }

    #[test]
    fn test_dispatch_lz_empty() {
        let info = CompressionInfo {
            version: 0,
            solid: false,
            method: CompressionMethod::Normal,
            dict_size: 128 * 1024,
        };

        let result = decompress(&[], 0, &info, None).unwrap();
        assert!(result.is_empty());
    }
}
