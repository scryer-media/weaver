//! Decompression dispatcher for RAR4 and RAR5.
//!
//! Routes compressed data to the appropriate decompressor based on the
//! archive format and compression method.

pub mod lz;
pub mod ppmd;
pub mod rar4;
pub mod store;

use std::io::Write;

use crate::error::{RarError, RarResult};
use crate::types::{ArchiveFormat, CompressionInfo, CompressionMethod};

/// A volume transition boundary used for chunked decompression.
///
/// Represents the compressed byte offset at which the reader switched to a new
/// volume. Used by `decompress_to_writer_chunked` to split decompressed output.
#[derive(Debug, Clone)]
pub struct VolumeTransition {
    /// The new volume index after the transition.
    pub volume_index: usize,
    /// Compressed byte offset at which the transition occurred.
    pub compressed_offset: u64,
}

/// Decompress data from a RAR file entry.
///
/// `input` is the raw compressed data area.
/// `unpacked_size` is the expected uncompressed size.
/// `info` provides compression parameters (including format for RAR4 vs RAR5).
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
        | CompressionMethod::Best => match info.format {
            ArchiveFormat::Rar4 => rar4::decompress_rar4_lz(input, unpacked_size, info.dict_size),
            ArchiveFormat::Rar5 => lz::decompress_lz(input, unpacked_size, info),
        },
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
        | CompressionMethod::Best => match info.format {
            ArchiveFormat::Rar4 => {
                rar4::decompress_rar4_lz_to_writer(input, unpacked_size, info.dict_size, writer)
            }
            ArchiveFormat::Rar5 => lz::decompress_lz_to_writer(input, unpacked_size, info, writer),
        },
        CompressionMethod::Unknown(code) => Err(RarError::UnsupportedCompression {
            method: code,
            version: info.version,
        }),
    }
}

/// Chunked variant: decompress with output split at compressed byte boundaries.
///
/// At each volume boundary crossing (from `VolumeTrackingReader`), the current
/// writer is flushed and `writer_factory` is called to get a new writer for
/// the next volume's chunk. Returns `(volume_index, bytes_written)` per chunk.
///
/// Only supports LZ methods (Store is handled directly in the extraction path).
pub fn decompress_to_writer_chunked<F>(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    first_volume_index: usize,
    boundaries: &[VolumeTransition],
    writer_factory: F,
) -> RarResult<Vec<(usize, u64)>>
where
    F: FnMut(usize) -> RarResult<Box<dyn Write>>,
{
    match info.method {
        CompressionMethod::Store => Err(RarError::CorruptArchive {
            detail: "chunked decompression not needed for Store mode".into(),
        }),
        CompressionMethod::Fastest
        | CompressionMethod::Fast
        | CompressionMethod::Normal
        | CompressionMethod::Good
        | CompressionMethod::Best => match info.format {
            ArchiveFormat::Rar4 => {
                let mut decoder = rar4::Rar4LzDecoder::new(info.dict_size as usize);
                decoder.decompress_to_writer_chunked(
                    input,
                    unpacked_size,
                    first_volume_index,
                    boundaries,
                    writer_factory,
                )
            }
            ArchiveFormat::Rar5 => {
                let mut decoder = lz::LzDecoder::new(info.dict_size as usize);
                decoder.decompress_to_writer_chunked(
                    input,
                    unpacked_size,
                    first_volume_index,
                    boundaries,
                    writer_factory,
                )
            }
        },
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
            format: ArchiveFormat::Rar5,
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
            format: ArchiveFormat::Rar5,
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
            format: ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: CompressionMethod::Normal,
            dict_size: 128 * 1024,
        };

        let result = decompress(&[], 0, &info, None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_dispatch_rar4_lz_empty() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar4,
            version: 29,
            solid: false,
            method: CompressionMethod::Normal,
            dict_size: 4 * 1024 * 1024,
        };

        let result = decompress(&[], 0, &info, None).unwrap();
        assert!(result.is_empty());
    }
}
