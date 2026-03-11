//! Store (method 0) decompression -- simple passthrough with CRC32 verification.
//!
//! Stored data is uncompressed; we just copy bytes from input to output
//! and optionally verify the CRC32.

use std::io::Write;

use crate::error::{RarError, RarResult};

/// Decompress stored (method 0) data by copying it directly to output.
///
/// If `expected_crc` is `Some`, the CRC32 of the output is verified.
/// Returns the number of bytes written.
pub fn decompress_store(
    input: &[u8],
    output: &mut Vec<u8>,
    expected_crc: Option<u32>,
) -> RarResult<()> {
    output.extend_from_slice(input);

    if let Some(expected) = expected_crc {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(input);
        let actual = hasher.finalize();
        if actual != expected {
            return Err(RarError::DataCrcMismatch {
                member: String::new(),
                expected,
                actual,
            });
        }
    }

    Ok(())
}

/// Streaming variant: write stored data directly to a writer with incremental CRC.
///
/// Unlike `decompress_store`, this does not accumulate output in memory.
/// The writer should be a BufWriter for best performance.
pub fn decompress_store_to_writer<W: Write>(
    input: &[u8],
    writer: &mut W,
    expected_crc: Option<u32>,
) -> RarResult<u64> {
    let mut hasher = expected_crc.map(|_| crc32fast::Hasher::new());

    // Write in chunks to avoid holding the full input pinned.
    const CHUNK: usize = 256 * 1024;
    let mut written = 0u64;
    for chunk in input.chunks(CHUNK) {
        writer.write_all(chunk).map_err(RarError::Io)?;
        if let Some(ref mut h) = hasher {
            h.update(chunk);
        }
        written += chunk.len() as u64;
    }

    if let (Some(expected), Some(h)) = (expected_crc, hasher) {
        let actual = h.finalize();
        if actual != expected {
            return Err(RarError::DataCrcMismatch {
                member: String::new(),
                expected,
                actual,
            });
        }
    }

    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_basic() {
        let input = b"Hello, world!";
        let mut output = Vec::new();
        decompress_store(input, &mut output, None).unwrap();
        assert_eq!(output, input);
    }

    #[test]
    fn test_store_empty() {
        let input = b"";
        let mut output = Vec::new();
        decompress_store(input, &mut output, None).unwrap();
        assert!(output.is_empty());
    }

    #[test]
    fn test_store_crc_valid() {
        let input = b"test data for CRC";
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(input);
        let crc = hasher.finalize();

        let mut output = Vec::new();
        decompress_store(input, &mut output, Some(crc)).unwrap();
        assert_eq!(output, input);
    }

    #[test]
    fn test_store_crc_mismatch() {
        let input = b"test data";
        let mut output = Vec::new();
        let result = decompress_store(input, &mut output, Some(0xDEADBEEF));
        assert!(matches!(result, Err(RarError::DataCrcMismatch { .. })));
    }

    #[test]
    fn test_store_large() {
        let input: Vec<u8> = (0..=255u8).cycle().take(256 * 1024).collect();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&input);
        let crc = hasher.finalize();

        let mut output = Vec::new();
        decompress_store(&input, &mut output, Some(crc)).unwrap();
        assert_eq!(output, input);
    }
}
