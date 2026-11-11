// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Article-level yEnc encoding (`=ybegin` / `=ypart` / `=yend` around a body)
// over the body encoder in `kernel`.

pub mod kernel;

pub use kernel::{
    EncodeError, EncoderTier, MAX_COLUMN, MAX_LINE_SIZE, encode_body, encode_body_append,
    encode_body_with_tier, encode_max_len,
};

use crate::crc::Crc32;
use crate::error::YencError;

impl From<EncodeError> for YencError {
    fn from(err: EncodeError) -> Self {
        match err {
            EncodeError::DestinationTooSmall { needed, available } => {
                YencError::BufferTooSmall { needed, available }
            }
            EncodeError::LineSizeOutOfRange { .. } => YencError::InvalidHeader {
                field: "line".to_string(),
                reason: err.to_string(),
            },
            EncodeError::ColumnOutOfRange { .. } | EncodeError::TierUnavailable { .. } => {
                YencError::InvalidHeader {
                    field: "body".to_string(),
                    reason: err.to_string(),
                }
            }
        }
    }
}

// Encode `input` bytes into a complete single-part yEnc article, appending to `output`.
//
// Produces `=ybegin` header, encoded data lines, and `=yend` trailer with CRC32.
pub fn encode(
    input: &[u8],
    output: &mut Vec<u8>,
    line_length: usize,
    filename: &str,
) -> Result<(), YencError> {
    check_line_length(line_length)?;

    // Compute CRC32 of the input data.
    let mut crc = Crc32::new();
    crc.update(input);
    let crc_val = crc.finalize();

    // Write =ybegin header.
    let header = format!(
        "=ybegin line={} size={} name={}\r\n",
        line_length,
        input.len(),
        filename
    );
    output.extend_from_slice(header.as_bytes());

    // Encode the data.
    encode_data(input, output, line_length)?;

    // Write =yend trailer.
    let trailer = format!("\r\n=yend size={} crc32={:08x}\r\n", input.len(), crc_val);
    output.extend_from_slice(trailer.as_bytes());

    Ok(())
}

// Encode `input` bytes into a complete multi-part yEnc article, appending to `output`.
#[allow(clippy::too_many_arguments)]
pub fn encode_part(
    input: &[u8],
    output: &mut Vec<u8>,
    line_length: usize,
    filename: &str,
    part: u32,
    total: u32,
    begin: u64,
    end: u64,
    file_size: u64,
) -> Result<(), YencError> {
    check_line_length(line_length)?;

    // Compute CRC32 of this part's data.
    let mut crc = Crc32::new();
    crc.update(input);
    let part_crc = crc.finalize();

    // Write =ybegin header.
    let header = format!(
        "=ybegin part={} total={} line={} size={} name={}\r\n",
        part, total, line_length, file_size, filename
    );
    output.extend_from_slice(header.as_bytes());

    // Write =ypart header.
    let ypart = format!("=ypart begin={} end={}\r\n", begin, end);
    output.extend_from_slice(ypart.as_bytes());

    // Encode the data.
    encode_data(input, output, line_length)?;

    // Write =yend trailer.
    let trailer = format!(
        "\r\n=yend size={} part={} pcrc32={:08x}\r\n",
        input.len(),
        part,
        part_crc
    );
    output.extend_from_slice(trailer.as_bytes());

    Ok(())
}

// Encode raw data bytes into yEnc body lines (no headers), appending to
// `output`: rapidyenc's body encoding, one call, ending the article.
fn encode_data(input: &[u8], output: &mut Vec<u8>, line_length: usize) -> Result<(), YencError> {
    let mut column = 0usize;
    encode_body_append(line_length, &mut column, input, output, true)?;
    Ok(())
}

// Refuse a line length the body encoder would refuse, before any header is
// written.
fn check_line_length(line_length: usize) -> Result<(), YencError> {
    if (1..=MAX_LINE_SIZE).contains(&line_length) {
        Ok(())
    } else {
        Err(EncodeError::LineSizeOutOfRange {
            line_size: line_length,
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode;
    use crate::types::CrcVerification;

    #[test]
    fn encode_simple() {
        let input = b"Hello, World!";
        let mut output = Vec::new();
        encode(input, &mut output, 128, "test.bin").unwrap();

        let article = String::from_utf8_lossy(&output);
        assert!(article.starts_with("=ybegin "));
        assert!(article.contains("name=test.bin"));
        assert!(article.contains("=yend "));
    }

    #[test]
    fn round_trip_simple() {
        let original = b"Hello, yEnc World! This is a test.";
        let mut encoded = Vec::new();
        encode(original, &mut encoded, 128, "test.bin").unwrap();

        let mut decoded = vec![0u8; 1024];
        let result = decode::decode(&encoded, &mut decoded).unwrap();

        assert_eq!(result.bytes_written, original.len());
        assert_eq!(&decoded[..result.bytes_written], original.as_slice());
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    #[test]
    fn round_trip_all_byte_values() {
        let original: Vec<u8> = (0..=255).collect();
        let mut encoded = Vec::new();
        encode(&original, &mut encoded, 128, "allbytes.bin").unwrap();

        let mut decoded = vec![0u8; 1024];
        let result = decode::decode(&encoded, &mut decoded).unwrap();

        assert_eq!(result.bytes_written, 256);
        assert_eq!(&decoded[..256], &original[..]);
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    #[test]
    fn round_trip_all_zeros() {
        let original = vec![0u8; 512];
        let mut encoded = Vec::new();
        encode(&original, &mut encoded, 128, "zeros.bin").unwrap();

        let mut decoded = vec![0u8; 1024];
        let result = decode::decode(&encoded, &mut decoded).unwrap();

        assert_eq!(result.bytes_written, 512);
        assert_eq!(&decoded[..512], &original[..]);
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    #[test]
    fn round_trip_all_escape_bytes() {
        // Bytes whose encoded forms are all critical characters.
        // 214 -> NUL, 224 -> LF, 227 -> CR, 19 -> '='
        let original: Vec<u8> = vec![214, 224, 227, 19]
            .into_iter()
            .cycle()
            .take(200)
            .collect();

        let mut encoded = Vec::new();
        encode(&original, &mut encoded, 128, "escapes.bin").unwrap();

        let mut decoded = vec![0u8; 1024];
        let result = decode::decode(&encoded, &mut decoded).unwrap();

        assert_eq!(result.bytes_written, original.len());
        assert_eq!(&decoded[..result.bytes_written], &original[..]);
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    #[test]
    fn round_trip_large_data() {
        // 64KB of pseudo-random data.
        let original: Vec<u8> = (0..65536).map(|i| (i * 7 + 13) as u8).collect();
        let mut encoded = Vec::new();
        encode(&original, &mut encoded, 128, "large.bin").unwrap();

        let mut decoded = vec![0u8; 128 * 1024];
        let result = decode::decode(&encoded, &mut decoded).unwrap();

        assert_eq!(result.bytes_written, original.len());
        assert_eq!(&decoded[..result.bytes_written], &original[..]);
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    #[test]
    fn round_trip_line_length_256() {
        let original: Vec<u8> = (0..1000).map(|i| i as u8).collect();
        let mut encoded = Vec::new();
        encode(&original, &mut encoded, 256, "wide.bin").unwrap();

        let mut decoded = vec![0u8; 2048];
        let result = decode::decode(&encoded, &mut decoded).unwrap();

        assert_eq!(result.bytes_written, original.len());
        assert_eq!(&decoded[..result.bytes_written], &original[..]);
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    #[test]
    fn round_trip_multipart() {
        let full_data: Vec<u8> = (0..1000).map(|i| i as u8).collect();
        let part1_data = &full_data[..500];
        let part2_data = &full_data[500..];

        let mut encoded1 = Vec::new();
        encode_part(
            part1_data,
            &mut encoded1,
            128,
            "multi.bin",
            1,
            2,
            1,
            500,
            1000,
        )
        .unwrap();

        let mut encoded2 = Vec::new();
        encode_part(
            part2_data,
            &mut encoded2,
            128,
            "multi.bin",
            2,
            2,
            501,
            1000,
            1000,
        )
        .unwrap();

        // Decode part 1.
        let mut decoded1 = vec![0u8; 1024];
        let result1 = decode::decode(&encoded1, &mut decoded1).unwrap();
        assert_eq!(result1.bytes_written, 500);
        assert_eq!(&decoded1[..500], part1_data);
        assert_eq!(result1.metadata.part, Some(1));
        assert_eq!(result1.metadata.total, Some(2));
        assert_eq!(result1.metadata.begin, Some(1));
        assert_eq!(result1.metadata.end, Some(500));
        // For multi-part, CRC is checked against pcrc32, and our encoder writes
        // one -- so this really is a verification, not an absent-CRC pass.
        assert_eq!(result1.crc_status, CrcVerification::Verified);

        // Decode part 2.
        let mut decoded2 = vec![0u8; 1024];
        let result2 = decode::decode(&encoded2, &mut decoded2).unwrap();
        assert_eq!(result2.bytes_written, 500);
        assert_eq!(&decoded2[..500], part2_data);
        assert_eq!(result2.metadata.part, Some(2));
    }

    #[test]
    fn round_trip_filename_with_spaces() {
        let original = b"file content";
        let mut encoded = Vec::new();
        encode(original, &mut encoded, 128, "my cool file.bin").unwrap();

        let mut decoded = vec![0u8; 1024];
        let result = decode::decode(&encoded, &mut decoded).unwrap();
        assert_eq!(result.metadata.name, "my cool file.bin");
        assert_eq!(&decoded[..result.bytes_written], original.as_slice());
    }

    #[test]
    fn round_trip_empty() {
        let original = b"";
        let mut encoded = Vec::new();
        encode(original, &mut encoded, 128, "empty.bin").unwrap();

        let mut decoded = vec![0u8; crate::decode::max_decoded_len(encoded.len())];
        let result = decode::decode(&encoded, &mut decoded).unwrap();
        assert_eq!(result.bytes_written, 0);
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    #[test]
    fn encode_line_breaks_present() {
        // With line_length=10, a sufficiently long input should produce line breaks.
        let original: Vec<u8> = (0..50).map(|i| (i + 65) as u8).collect(); // ASCII letters
        let mut encoded = Vec::new();
        encode(&original, &mut encoded, 10, "lines.bin").unwrap();

        // The encoded data (between header and trailer) should contain CRLF.
        let article_str = String::from_utf8_lossy(&encoded);
        let lines: Vec<&str> = article_str.split("\r\n").collect();
        // Should have multiple lines (header, data lines, trailer).
        assert!(lines.len() > 3);

        // Verify round-trip still works.
        let mut decoded = vec![0u8; 1024];
        let result = decode::decode(&encoded, &mut decoded).unwrap();
        assert_eq!(result.bytes_written, original.len());
        assert_eq!(&decoded[..result.bytes_written], &original[..]);
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    #[test]
    fn round_trip_single_byte() {
        for b in 0..=255u8 {
            let original = [b];
            let mut encoded = Vec::new();
            encode(&original, &mut encoded, 128, "byte.bin").unwrap();

            let mut decoded = vec![0u8; crate::decode::max_decoded_len(encoded.len())];
            let result = decode::decode(&encoded, &mut decoded).unwrap();
            assert_eq!(result.bytes_written, 1, "failed for byte {}", b);
            assert_eq!(decoded[0], b, "mismatch for byte {}", b);
            assert_eq!(
                result.crc_status,
                CrcVerification::Verified,
                "CRC not verified for byte {}",
                b
            );
        }
    }

    #[test]
    fn zero_line_length_is_refused_before_any_output() {
        let mut output = b"keep".to_vec();
        assert!(matches!(
            encode(b"data", &mut output, 0, "x.bin"),
            Err(YencError::InvalidHeader { .. })
        ));
        assert!(encode_part(b"data", &mut output, 0, "x.bin", 1, 1, 1, 4, 4).is_err());
        assert_eq!(output, b"keep");
    }

    // The body between header and trailer is exactly the body encoder's
    // output for one call ending the article.
    #[test]
    fn article_body_is_the_body_encoder_output() {
        let input: Vec<u8> = (0..5000u32).map(|i| (i * 131 + 7) as u8).collect();
        let mut article = Vec::new();
        encode(&input, &mut article, 128, "b.bin").unwrap();
        let mut body = Vec::new();
        let mut column = 0;
        encode_body_append(128, &mut column, &input, &mut body, true).unwrap();
        let header_end = article.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
        assert_eq!(&article[header_end..header_end + body.len()], &body[..]);
        assert!(article[header_end + body.len()..].starts_with(b"\r\n=yend "));
    }
}
