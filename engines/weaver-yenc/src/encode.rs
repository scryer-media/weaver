use crate::crc::Crc32;
use crate::error::YencError;

/// Encode `input` bytes into a complete single-part yEnc article, appending to `output`.
///
/// Produces `=ybegin` header, encoded data lines, and `=yend` trailer with CRC32.
pub fn encode(
    input: &[u8],
    output: &mut Vec<u8>,
    line_length: usize,
    filename: &str,
) -> Result<(), YencError> {
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
    encode_data(input, output, line_length);

    // Write =yend trailer.
    let trailer = format!("\r\n=yend size={} crc32={:08x}\r\n", input.len(), crc_val);
    output.extend_from_slice(trailer.as_bytes());

    Ok(())
}

/// Encode `input` bytes into a complete multi-part yEnc article, appending to `output`.
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
    encode_data(input, output, line_length);

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

/// Encode raw data bytes into yEnc format (no headers), appending to `output`.
/// Inserts CRLF line breaks at approximately `line_length` encoded characters.
fn encode_data(input: &[u8], output: &mut Vec<u8>, line_length: usize) {
    let mut col = 0usize;

    for (i, &byte) in input.iter().enumerate() {
        let encoded = byte.wrapping_add(42);

        let needs_escape = matches!(encoded, 0x00 | 0x0A | 0x0D | 0x3D)
            || (encoded == b'\t' && (col == 0 || i == input.len() - 1))
            || (encoded == b' ' && (col == 0 || i == input.len() - 1))
            || (encoded == b'.' && col == 0);

        if needs_escape {
            // Check if we need a line break before the 2-byte escape.
            if col + 2 > line_length && line_length > 0 {
                output.extend_from_slice(b"\r\n");
                col = 0;
            }
            output.push(b'=');
            output.push(encoded.wrapping_add(64));
            col += 2;
        } else {
            if col >= line_length && line_length > 0 {
                output.extend_from_slice(b"\r\n");
                col = 0;
                // After line break, check if this byte needs escaping at line start.
                if encoded == b'.' || encoded == b'\t' || encoded == b' ' {
                    output.push(b'=');
                    output.push(encoded.wrapping_add(64));
                    col += 2;
                    continue;
                }
            }
            output.push(encoded);
            col += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode;

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
        assert!(result.crc_valid);
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
        assert!(result.crc_valid);
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
        assert!(result.crc_valid);
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
        assert!(result.crc_valid);
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
        assert!(result.crc_valid);
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
        assert!(result.crc_valid);
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
        // For multi-part, CRC is checked against pcrc32.
        // Our encoder writes pcrc32 so crc_valid should be checked.

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

        let mut decoded = vec![0u8; 64];
        let result = decode::decode(&encoded, &mut decoded).unwrap();
        assert_eq!(result.bytes_written, 0);
        assert!(result.crc_valid);
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
        assert!(result.crc_valid);
    }

    #[test]
    fn round_trip_single_byte() {
        for b in 0..=255u8 {
            let original = [b];
            let mut encoded = Vec::new();
            encode(&original, &mut encoded, 128, "byte.bin").unwrap();

            let mut decoded = vec![0u8; 64];
            let result = decode::decode(&encoded, &mut decoded).unwrap();
            assert_eq!(result.bytes_written, 1, "failed for byte {}", b);
            assert_eq!(decoded[0], b, "mismatch for byte {}", b);
            assert!(result.crc_valid, "CRC invalid for byte {}", b);
        }
    }
}
