//! Decompression dispatcher for RAR4 and RAR5.
//!
//! Routes compressed data to the appropriate decompressor based on the
//! archive format and compression method.

pub mod lz;
pub mod ppmd;
pub mod rar4;
pub(crate) mod rar4_old;
pub mod store;

use std::io::Write;

use crate::error::{RarError, RarResult};
use crate::limits::Limits;
use crate::types::{ArchiveFormat, CompressionInfo, CompressionMethod};

/// Maximum initial allocation for the Vec-returning decompression helper.
const DIRECT_OUTPUT_INITIAL_CAPACITY_LIMIT: usize = 1024 * 1024;

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
    decompress_with_limits(input, unpacked_size, info, expected_crc, &Limits::default())
}

/// Decompress data using caller-provided resource limits.
pub fn decompress_with_limits(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    expected_crc: Option<u32>,
    limits: &Limits,
) -> RarResult<Vec<u8>> {
    enforce_direct_input_limit(input.len(), limits)?;
    enforce_direct_output_limit(unpacked_size, limits)?;
    enforce_supported_rar4_compression(info)?;

    match info.method {
        CompressionMethod::Store => {
            enforce_direct_store_output_limit(input.len(), limits)?;
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
            enforce_direct_dict_limit(info, limits)?;
            let capacity = direct_output_initial_capacity(input.len(), unpacked_size);
            let mut output = Vec::with_capacity(capacity);
            decompress_to_writer_with_max_dict_size(
                input,
                unpacked_size,
                info,
                expected_crc,
                &mut output,
                limits.max_dict_size,
            )?;
            Ok(output)
        }
        CompressionMethod::Unknown(code) => Err(RarError::UnsupportedCompression {
            method: code,
            version: info.version,
        }),
    }
}

fn enforce_direct_input_limit(input_len: usize, limits: &Limits) -> RarResult<()> {
    let input_len = input_len as u64;
    let max_data_segment = limits.max_data_segment;
    if input_len > max_data_segment {
        return Err(RarError::ResourceLimit {
            detail: format!(
                "direct decompression input size {input_len} exceeds limit {max_data_segment}"
            ),
        });
    }

    Ok(())
}

fn enforce_direct_output_limit(unpacked_size: u64, limits: &Limits) -> RarResult<()> {
    let max_unpacked_size = limits.max_unpacked_size;
    if unpacked_size > max_unpacked_size {
        return Err(RarError::ResourceLimit {
            detail: format!(
                "direct decompression output size {unpacked_size} exceeds limit {max_unpacked_size}"
            ),
        });
    }

    Ok(())
}

fn enforce_direct_store_output_limit(input_len: usize, limits: &Limits) -> RarResult<()> {
    let output_len = input_len as u64;
    let max_unpacked_size = limits.max_unpacked_size;
    if output_len > max_unpacked_size {
        return Err(RarError::ResourceLimit {
            detail: format!(
                "direct stored output size {output_len} exceeds limit {max_unpacked_size}"
            ),
        });
    }

    Ok(())
}

fn enforce_direct_dict_limit(info: &CompressionInfo, limits: &Limits) -> RarResult<()> {
    let dict_size = effective_direct_dict_size(info);
    if dict_size > limits.max_dict_size {
        return Err(RarError::DictionaryTooLarge {
            size: dict_size,
            max: limits.max_dict_size,
        });
    }

    Ok(())
}

fn effective_direct_dict_size(info: &CompressionInfo) -> u64 {
    if info.method == CompressionMethod::Store {
        0
    } else if info.format.is_rar4_family() {
        rar4_old::effective_rar4_window_size(info.dict_size)
    } else {
        lz::effective_lz_window_size(info.dict_size)
    }
}

fn enforce_supported_rar4_compression(info: &CompressionInfo) -> RarResult<()> {
    if info.format.is_rar4_family() && info.method != CompressionMethod::Store {
        return rar4_old::ensure_supported_rar4_version(info.version, info.method.code());
    }

    Ok(())
}

fn direct_output_initial_capacity(input_len: usize, unpacked_size: u64) -> usize {
    let unpacked_hint =
        usize::try_from(unpacked_size).unwrap_or(DIRECT_OUTPUT_INITIAL_CAPACITY_LIMIT);

    input_len
        .min(unpacked_hint)
        .min(DIRECT_OUTPUT_INITIAL_CAPACITY_LIMIT)
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
    decompress_to_writer_with_max_dict_size(
        input,
        unpacked_size,
        info,
        expected_crc,
        writer,
        Limits::default().max_dict_size,
    )
}

fn decompress_to_writer_with_max_dict_size<W: Write>(
    input: &[u8],
    unpacked_size: u64,
    info: &CompressionInfo,
    expected_crc: Option<u32>,
    writer: &mut W,
    max_dict_size: u64,
) -> RarResult<u64> {
    enforce_supported_rar4_compression(info)?;

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
            ArchiveFormat::Rar14 | ArchiveFormat::Rar4 => rar4_old::decompress_rar4_to_writer(
                input,
                unpacked_size,
                info.version,
                info.method.code(),
                info.dict_size,
                writer,
            ),
            ArchiveFormat::Rar5 => lz::decompress_lz_to_writer_with_max_dict_size(
                input,
                unpacked_size,
                info,
                writer,
                max_dict_size,
            ),
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
    enforce_supported_rar4_compression(info)?;

    match info.method {
        CompressionMethod::Store => Err(RarError::CorruptArchive {
            detail: "chunked decompression not needed for Store mode".into(),
        }),
        CompressionMethod::Fastest
        | CompressionMethod::Fast
        | CompressionMethod::Normal
        | CompressionMethod::Good
        | CompressionMethod::Best => match info.format {
            ArchiveFormat::Rar14 | ArchiveFormat::Rar4 => {
                rar4_old::decompress_rar4_to_writer_chunked(
                    input,
                    unpacked_size,
                    info.version,
                    info.method.code(),
                    info.dict_size,
                    first_volume_index,
                    boundaries,
                    writer_factory,
                )
            }
            ArchiveFormat::Rar5 => lz::decompress_lz_to_writer_chunked_with_max_dict_size(
                input,
                unpacked_size,
                info,
                first_volume_index,
                boundaries,
                writer_factory,
                Limits::default().max_dict_size,
            ),
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
    fn test_dispatch_lz_rejects_direct_output_above_limit_before_allocating() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: CompressionMethod::Normal,
            dict_size: 128 * 1024,
        };

        let result = decompress(&[0], Limits::default().max_unpacked_size + 1, &info, None);

        assert!(matches!(result, Err(RarError::ResourceLimit { .. })));
    }

    #[test]
    fn test_dispatch_uses_custom_direct_output_limit() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: CompressionMethod::Store,
            dict_size: 128 * 1024,
        };
        let limits = Limits {
            max_unpacked_size: 0,
            ..Limits::default()
        };

        let result = decompress_with_limits(&[0], 1, &info, None, &limits);

        assert!(matches!(result, Err(RarError::ResourceLimit { .. })));
    }

    #[test]
    fn test_dispatch_uses_custom_direct_input_limit_for_store() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: CompressionMethod::Store,
            dict_size: 128 * 1024,
        };
        let limits = Limits {
            max_data_segment: 0,
            ..Limits::default()
        };

        let result = decompress_with_limits(&[0], 1, &info, None, &limits);

        assert!(matches!(result, Err(RarError::ResourceLimit { .. })));
    }

    #[test]
    fn test_dispatch_uses_custom_store_output_limit() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: CompressionMethod::Store,
            dict_size: 128 * 1024,
        };
        let limits = Limits {
            max_unpacked_size: 0,
            ..Limits::default()
        };

        let result = decompress_with_limits(&[0], 0, &info, None, &limits);

        assert!(matches!(result, Err(RarError::ResourceLimit { .. })));
    }

    #[test]
    fn test_dispatch_uses_custom_direct_dictionary_limit() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: CompressionMethod::Normal,
            dict_size: 256 * 1024,
        };
        let limits = Limits {
            max_dict_size: 128 * 1024,
            ..Limits::default()
        };

        let result = decompress_with_limits(&[], 0, &info, None, &limits);

        assert!(matches!(
            result,
            Err(RarError::DictionaryTooLarge {
                size: 262_144,
                max: 131_072
            })
        ));
    }

    #[test]
    fn test_dispatch_uses_effective_rar4_dictionary_limit() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar4,
            version: 29,
            solid: false,
            method: CompressionMethod::Normal,
            dict_size: 128 * 1024,
        };
        let limits = Limits {
            max_dict_size: 128 * 1024,
            ..Limits::default()
        };

        let result = decompress_with_limits(&[], 0, &info, None, &limits);

        assert!(matches!(
            result,
            Err(RarError::DictionaryTooLarge {
                size: 262_144,
                max: 131_072
            })
        ));
    }

    #[test]
    fn test_dispatch_uses_effective_rar5_dictionary_limit() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar5,
            version: 0,
            solid: false,
            method: CompressionMethod::Normal,
            dict_size: 128 * 1024,
        };
        let limits = Limits {
            max_dict_size: 128 * 1024,
            ..Limits::default()
        };

        let result = decompress_with_limits(&[], 0, &info, None, &limits);

        assert!(matches!(
            result,
            Err(RarError::DictionaryTooLarge {
                size: 262_144,
                max: 131_072
            })
        ));
    }

    #[test]
    fn test_direct_output_initial_capacity_is_bounded() {
        assert_eq!(
            direct_output_initial_capacity(usize::MAX, u64::MAX),
            DIRECT_OUTPUT_INITIAL_CAPACITY_LIMIT
        );
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

    #[test]
    fn test_dispatch_accepts_old_rar4_store() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar4,
            version: 20,
            solid: false,
            method: CompressionMethod::Store,
            dict_size: 0,
        };

        let result = decompress(b"stored", 6, &info, None).unwrap();

        assert_eq!(result, b"stored");
    }

    #[test]
    fn test_dispatch_accepts_rar20_and_reaches_old_decoder() {
        let info = CompressionInfo {
            format: ArchiveFormat::Rar4,
            version: 20,
            solid: false,
            method: CompressionMethod::Normal,
            dict_size: 4 * 1024 * 1024,
        };

        let result = decompress(&[], 1, &info, None);

        assert!(matches!(result, Err(RarError::CorruptArchive { .. })));
    }
}
