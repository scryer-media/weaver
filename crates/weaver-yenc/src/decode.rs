use crate::crc::Crc32;
use crate::error::YencError;
use crate::header;
use crate::types::DecodeResult;

/// Options for decoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecodeOptions {
    /// If true, perform NNTP dot-unstuffing (strip leading dot from lines
    /// starting with `..`). Default: false (assumes transport layer handled it).
    pub dot_unstuffing: bool,
}

/// Decode a complete yEnc article (headers + data + trailer) from `input` into `output`.
///
/// Returns metadata and CRC verification results. The caller provides the output
/// buffer, which must be large enough to hold the decoded data.
pub fn decode(input: &[u8], output: &mut [u8]) -> Result<DecodeResult, YencError> {
    decode_with_options(input, output, DecodeOptions::default())
}

/// Decode a complete yEnc article with custom options.
pub fn decode_with_options(
    input: &[u8],
    output: &mut [u8],
    options: DecodeOptions,
) -> Result<DecodeResult, YencError> {
    let parsed = header::parse_headers(input)?;

    let data_end = parsed.data_end.max(parsed.data_start);
    let data = &input[parsed.data_start..data_end];

    let mut crc = Crc32::new();
    let bytes_written = decode_body(data, output, &mut crc, options)?;

    let part_crc = crc.finalize();

    // Extract expected CRC values from =yend.
    let (expected_part_crc, expected_file_crc, yend_size) = match &parsed.yend {
        Some(yend) => (yend.pcrc32, yend.crc32, yend.size),
        None => (None, None, None),
    };

    // Validate decoded size against =yend size (like NZBGet's dsInvalidSize).
    if let Some(expected_size) = yend_size
        && bytes_written as u64 != expected_size
    {
        return Err(YencError::SizeMismatch {
            expected: expected_size,
            actual: bytes_written as u64,
        });
    }

    // For single-part articles, also validate =ybegin size vs =yend size.
    if parsed.metadata.part.is_none()
        && let Some(expected_size) = yend_size
        && parsed.metadata.size != expected_size
    {
        return Err(YencError::SizeMismatch {
            expected: parsed.metadata.size,
            actual: expected_size,
        });
    }

    // For single-part articles, the `crc32` field in =yend is the part CRC.
    // For multi-part articles, `pcrc32` is the part CRC.
    let expected_crc_to_check = if parsed.metadata.part.is_some() {
        expected_part_crc
    } else {
        // For single-part, crc32 is the file CRC which equals the part CRC.
        expected_file_crc
    };

    let crc_valid = match expected_crc_to_check {
        Some(expected) => part_crc == expected,
        None => true, // No expected CRC means we can't validate, treat as valid.
    };

    Ok(DecodeResult {
        metadata: parsed.metadata,
        bytes_written,
        part_crc,
        expected_part_crc,
        expected_file_crc,
        crc_valid,
        has_trailer: parsed.yend.is_some(),
    })
}

/// Decode raw yEnc-encoded data (no headers/trailers) from `input` into `output`.
///
/// Updates the CRC hasher with decoded bytes. Returns the number of bytes written.
pub fn decode_body(
    input: &[u8],
    output: &mut [u8],
    crc: &mut Crc32,
    options: DecodeOptions,
) -> Result<usize, YencError> {
    let mut src = 0usize;
    let mut dst = 0usize;
    let mut at_line_start = true;

    // CRC is updated once at the end for better hardware utilization
    // (crc32fast's PCLMULQDQ path needs >= 128 bytes).
    let crc_pos = 0usize;

    while src < input.len() {
        let byte = input[src];

        // NNTP dot-unstuffing: if at the start of a line and byte is '.'.
        if options.dot_unstuffing && at_line_start && byte == b'.' {
            src += 1;
            if src >= input.len() {
                break;
            }
            // If next is also '.', this is a dot-stuffed line -- skip the first dot.
            // If next is '\r' or '\n', this is the NNTP terminator -- stop.
            let next = input[src];
            if next == b'\r' || next == b'\n' {
                // NNTP article terminator.
                break;
            }
            // If next is '.', we consumed the stuffed dot, continue normally.
            // If next is something else, the dot was data (shouldn't happen in
            // well-formed NNTP, but handle gracefully).
            if next != b'.' {
                // The dot was actual encoded data.
                if dst >= output.len() {
                    return Err(YencError::BufferTooSmall {
                        needed: dst + 1,
                        available: output.len(),
                    });
                }
                output[dst] = b'.'.wrapping_sub(42);
                dst += 1;
            }
            at_line_start = false;
            continue;
        }

        match byte {
            b'\r' | b'\n' => {
                at_line_start = byte == b'\n' || (byte == b'\r');
                src += 1;
                if byte == b'\n' {
                    at_line_start = true;
                }
            }
            b'=' => {
                at_line_start = false;
                src += 1;
                if src >= input.len() {
                    return Err(YencError::MalformedEscape(src - 1));
                }
                let escaped = input[src];
                if dst >= output.len() {
                    return Err(YencError::BufferTooSmall {
                        needed: dst + 1,
                        available: output.len(),
                    });
                }
                // Decode: (escaped_byte - 64 - 42) mod 256 = escaped_byte - 106
                output[dst] = escaped.wrapping_sub(106);
                dst += 1;
                src += 1;
            }
            _ => {
                at_line_start = false;
                // Use SIMD fast path for runs of normal bytes.
                let (consumed, written) = crate::simd::decode_normal_run(input, src, output, dst);
                if written == 0 {
                    // Should not happen since byte is not a special char,
                    // but handle output buffer being full.
                    if dst >= output.len() {
                        return Err(YencError::BufferTooSmall {
                            needed: dst + 1,
                            available: output.len(),
                        });
                    }
                    output[dst] = byte.wrapping_sub(42);
                    dst += 1;
                    src += 1;
                } else {
                    src += consumed;
                    dst += written;
                }
            }
        }
    }

    // Final CRC update for any remaining decoded bytes.
    if dst > crc_pos {
        crc.update(&output[crc_pos..dst]);
    }

    Ok(dst)
}

/// Persistent state for streaming yEnc decode across chunk boundaries.
///
/// Tracks whether the previous chunk ended mid-escape sequence or at a line
/// boundary, so the next chunk continues correctly.
#[derive(Debug, Clone)]
pub struct DecodeState {
    /// If true, the previous chunk ended with an `=` escape character.
    /// The next byte in the following chunk is the escaped value.
    pub escape_pending: bool,
    /// If true, we're at the start of a line (for dot-unstuffing).
    pub at_line_start: bool,
    /// If true, the previous chunk ended with a dot at line start (dot-unstuffing).
    /// The next byte determines whether it's a terminator, stuffed dot, or data.
    pub dot_pending: bool,
    /// Running CRC32 state.
    pub(crate) crc: crc32fast::Hasher,
    /// Total bytes decoded so far across all chunks.
    pub bytes_decoded: u64,
}

impl DecodeState {
    /// Create a new decode state for streaming decode.
    pub fn new() -> Self {
        Self {
            escape_pending: false,
            at_line_start: true,
            dot_pending: false,
            crc: crc32fast::Hasher::new(),
            bytes_decoded: 0,
        }
    }

    /// Finalize and return the CRC32 of all decoded data. Consumes the state.
    pub fn finalize_crc(self) -> u32 {
        self.crc.finalize()
    }

    /// Get the current CRC32 value without consuming the state.
    /// (clones the hasher internally)
    pub fn current_crc(&self) -> u32 {
        self.crc.clone().finalize()
    }
}

impl Default for DecodeState {
    fn default() -> Self {
        Self::new()
    }
}

/// Decode a chunk of raw yEnc body data, updating persistent state.
///
/// Unlike `decode_body`, this function can be called repeatedly with
/// successive chunks of the same article body. State (escape sequences,
/// line position, CRC) carries over between calls.
///
/// Returns the number of bytes written to `output`.
pub fn decode_chunk(
    input: &[u8],
    output: &mut [u8],
    state: &mut DecodeState,
    options: DecodeOptions,
) -> Result<usize, YencError> {
    let mut src = 0usize;
    let mut dst = 0usize;

    // CRC is updated once at the end (or on early return) for better
    // hardware utilization (crc32fast's PCLMULQDQ path needs >= 128 bytes).
    let crc_pos = 0usize;

    // Handle a pending escape from the previous chunk.
    if state.escape_pending {
        if input.is_empty() {
            return Ok(0);
        }
        state.escape_pending = false;
        let escaped = input[src];
        if dst >= output.len() {
            return Err(YencError::BufferTooSmall {
                needed: dst + 1,
                available: output.len(),
            });
        }
        output[dst] = escaped.wrapping_sub(106);
        dst += 1;
        src += 1;
        state.at_line_start = false;
    }

    // Handle a pending dot from previous chunk (dot at line start, chunk ended).
    // Now we can see the next byte to determine the correct action.
    if state.dot_pending {
        state.dot_pending = false;
        if src >= input.len() {
            // Still can't resolve — emit dot as data (best effort at EOF).
            if dst >= output.len() {
                return Err(YencError::BufferTooSmall {
                    needed: dst + 1,
                    available: output.len(),
                });
            }
            output[dst] = b'.'.wrapping_sub(42);
            dst += 1;
            state.at_line_start = false;
        } else {
            let next = input[src];
            if next == b'\r' || next == b'\n' {
                // NNTP terminator (.\r\n or .\n) — stop decoding.
                // Update CRC and return.
                if dst > crc_pos {
                    state.crc.update(&output[crc_pos..dst]);
                }
                state.bytes_decoded += dst as u64;
                return Ok(dst);
            } else if next == b'.' {
                // Dot-stuffed line: skip the stuffed dot, continue with data.
                src += 1;
                state.at_line_start = false;
            } else {
                // Dot was actual encoded data.
                if dst >= output.len() {
                    return Err(YencError::BufferTooSmall {
                        needed: dst + 1,
                        available: output.len(),
                    });
                }
                output[dst] = b'.'.wrapping_sub(42);
                dst += 1;
                state.at_line_start = false;
            }
        }
    }

    while src < input.len() {
        let byte = input[src];

        // NNTP dot-unstuffing: if at the start of a line and byte is '.'.
        if options.dot_unstuffing && state.at_line_start && byte == b'.' {
            src += 1;
            if src >= input.len() {
                // Chunk ended right after a dot at line start.
                // Defer to next chunk — it will resolve via dot_pending.
                state.dot_pending = true;
                state.at_line_start = false;
                break;
            }
            let next = input[src];
            if next == b'\r' || next == b'\n' {
                // NNTP article terminator.
                break;
            }
            if next != b'.' {
                // The dot was actual encoded data.
                if dst >= output.len() {
                    return Err(YencError::BufferTooSmall {
                        needed: dst + 1,
                        available: output.len(),
                    });
                }
                output[dst] = b'.'.wrapping_sub(42);
                dst += 1;
            }
            state.at_line_start = false;
            continue;
        }

        match byte {
            b'\r' | b'\n' => {
                src += 1;
                if byte == b'\n' {
                    state.at_line_start = true;
                }
                // \r also sets at_line_start for compatibility with the
                // existing decode_body behavior.
                if byte == b'\r' {
                    state.at_line_start = true;
                }
            }
            b'=' => {
                state.at_line_start = false;
                src += 1;
                if src >= input.len() {
                    // Chunk ended with '=' — the escaped byte is in the next chunk.
                    state.escape_pending = true;
                    break;
                }
                let escaped = input[src];
                if dst >= output.len() {
                    return Err(YencError::BufferTooSmall {
                        needed: dst + 1,
                        available: output.len(),
                    });
                }
                output[dst] = escaped.wrapping_sub(106);
                dst += 1;
                src += 1;
            }
            _ => {
                state.at_line_start = false;
                if dst >= output.len() {
                    return Err(YencError::BufferTooSmall {
                        needed: dst + 1,
                        available: output.len(),
                    });
                }
                output[dst] = byte.wrapping_sub(42);
                dst += 1;
                src += 1;
            }
        }
    }

    // Final CRC update for any remaining decoded bytes in this chunk.
    if dst > crc_pos {
        state.crc.update(&output[crc_pos..dst]);
    }

    state.bytes_decoded += dst as u64;
    Ok(dst)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: encode a single byte using yEnc rules (for test construction).
    fn yenc_encode_byte(b: u8) -> Vec<u8> {
        let encoded = b.wrapping_add(42);
        match encoded {
            0x00 | 0x0A | 0x0D | 0x3D => {
                vec![b'=', encoded.wrapping_add(64)]
            }
            _ => vec![encoded],
        }
    }

    /// Helper: encode a byte slice to raw yEnc data (no headers).
    fn encode_raw(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for &b in data {
            out.extend(yenc_encode_byte(b));
        }
        out
    }

    #[test]
    fn decode_body_simple() {
        // Encode "Hello" and decode it back.
        let original = b"Hello";
        let encoded = encode_raw(original);

        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let n = decode_body(&encoded, &mut output, &mut crc, DecodeOptions::default()).unwrap();
        assert_eq!(n, original.len());
        assert_eq!(&output[..n], original);
    }

    #[test]
    fn decode_body_all_zeros() {
        // All zero bytes -- each encodes to '*' (0x2A).
        let original = vec![0u8; 256];
        let encoded = encode_raw(&original);

        let mut output = vec![0u8; 512];
        let mut crc = Crc32::new();
        let n = decode_body(&encoded, &mut output, &mut crc, DecodeOptions::default()).unwrap();
        assert_eq!(n, 256);
        assert_eq!(&output[..n], &original[..]);
    }

    #[test]
    fn decode_body_all_critical_chars() {
        // Bytes that encode to critical characters.
        // NUL (0x00) is encoded by input byte (256 - 42) = 214 = 0xD6
        // LF  (0x0A) is encoded by input byte (0x0A - 42) mod 256 = 224 = 0xE0
        // ... wait, let's think differently.
        // encoded = (input + 42) mod 256
        // For encoded to be 0x00 (NUL): input = (0 - 42) mod 256 = 214
        // For encoded to be 0x0A (LF):  input = (10 - 42) mod 256 = 224
        // For encoded to be 0x0D (CR):  input = (13 - 42) mod 256 = 227
        // For encoded to be 0x3D (=):   input = (61 - 42) mod 256 = 19
        let critical_inputs: Vec<u8> = vec![214, 224, 227, 19];
        let encoded = encode_raw(&critical_inputs);

        // All should be escaped: =X format.
        // Verify escapes are present.
        assert!(encoded.contains(&b'='));

        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let n = decode_body(&encoded, &mut output, &mut crc, DecodeOptions::default()).unwrap();
        assert_eq!(n, critical_inputs.len());
        assert_eq!(&output[..n], &critical_inputs[..]);
    }

    #[test]
    fn decode_body_sequential_bytes() {
        // All 256 byte values.
        let original: Vec<u8> = (0..=255).collect();
        let encoded = encode_raw(&original);

        let mut output = vec![0u8; 512];
        let mut crc = Crc32::new();
        let n = decode_body(&encoded, &mut output, &mut crc, DecodeOptions::default()).unwrap();
        assert_eq!(n, 256);
        assert_eq!(&output[..n], &original[..]);
    }

    #[test]
    fn decode_body_with_line_breaks() {
        // Encoded data with CRLF line breaks interspersed.
        let original = b"ABCD";
        let mut encoded = Vec::new();
        encoded.extend(yenc_encode_byte(b'A'));
        encoded.extend(yenc_encode_byte(b'B'));
        encoded.extend_from_slice(b"\r\n");
        encoded.extend(yenc_encode_byte(b'C'));
        encoded.extend(yenc_encode_byte(b'D'));
        encoded.extend_from_slice(b"\r\n");

        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let n = decode_body(&encoded, &mut output, &mut crc, DecodeOptions::default()).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&output[..n], original.as_slice());
    }

    #[test]
    fn decode_body_bare_lf() {
        // Bare LF line endings.
        let original = b"AB";
        let mut encoded = Vec::new();
        encoded.extend(yenc_encode_byte(b'A'));
        encoded.push(b'\n');
        encoded.extend(yenc_encode_byte(b'B'));

        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let n = decode_body(&encoded, &mut output, &mut crc, DecodeOptions::default()).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&output[..n], original.as_slice());
    }

    #[test]
    fn decode_body_malformed_escape_at_end() {
        let encoded = b"=";
        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let result = decode_body(encoded, &mut output, &mut crc, DecodeOptions::default());
        assert!(matches!(result, Err(YencError::MalformedEscape(_))));
    }

    #[test]
    fn decode_body_buffer_too_small() {
        let original = b"Hello, World!";
        let encoded = encode_raw(original);

        let mut output = vec![0u8; 4]; // too small
        let mut crc = Crc32::new();
        let result = decode_body(&encoded, &mut output, &mut crc, DecodeOptions::default());
        assert!(matches!(result, Err(YencError::BufferTooSmall { .. })));
    }

    #[test]
    fn decode_body_streaming_crc() {
        let original = b"Hello, World!";
        let encoded = encode_raw(original);

        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let n = decode_body(&encoded, &mut output, &mut crc, DecodeOptions::default()).unwrap();

        let computed = crc.finalize();

        // Verify against independently computed CRC.
        let mut expected_crc = Crc32::new();
        expected_crc.update(original);
        assert_eq!(computed, expected_crc.finalize());
        assert_eq!(n, original.len());
    }

    #[test]
    fn decode_body_dot_unstuffing() {
        // Line starting with ".." should have first dot stripped.
        let _original = b".hello";
        let mut encoded = Vec::new();
        // Dot-stuffed: line starts with ".." then the encoded data.
        encoded.extend_from_slice(b"..");
        for &b in b"hello" {
            encoded.extend(yenc_encode_byte(b));
        }

        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let opts = DecodeOptions {
            dot_unstuffing: true,
        };
        let _n = decode_body(&encoded, &mut output, &mut crc, opts).unwrap();
        // The first '.' was the stuffed dot (stripped), the second '.' is data
        // that gets decoded as (0x2E - 42) = 0x04.
        // Actually wait -- dot-unstuffing strips the first dot. The second dot
        // IS the encoded dot. So decoded = (0x2E - 42) wrapping = 4.
        // But original is ".hello" which is [0x2E, ...].
        // Hmm, the issue is dot-unstuffing is about NNTP transport, not about
        // the encoded data content. Let me reconsider.

        // In NNTP, if a line starts with ".", it gets an extra "." prepended.
        // So "..hello_encoded" after unstuffing becomes ".hello_encoded"
        // where ".hello_encoded" is the actual yEnc data.
        // The "." is then an encoded byte: decoded = (0x2E - 42) = 4.
        // So this test checks that dot-unstuffing correctly removes the leading dot.

        // Let's make a simpler test: encode data that doesn't start with dot.
        // The NNTP layer added a dot.

        // Actually let me redo this test properly.
        // Original decoded data: [4, ...] (encoded as 0x2E which is '.')
        // yEnc encoded: b".hello_enc"  (the '.' is the encoded byte for value 4)
        // NNTP dot-stuffed: b"..hello_enc"
        // After our dot-unstuffing: b".hello_enc" -> decode '.' as byte 4.

        // So result should be: byte 4 followed by decoded "hello".
        let expected_first = b'.'.wrapping_sub(42); // 4
        assert_eq!(output[0], expected_first);
    }

    #[test]
    fn decode_body_dot_unstuffing_nntp_terminator() {
        // A line with just "." signals end of NNTP body.
        let mut encoded = Vec::new();
        encoded.extend(yenc_encode_byte(b'A'));
        encoded.extend_from_slice(b"\r\n.\r\n");
        encoded.extend(yenc_encode_byte(b'B')); // should not be decoded

        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let opts = DecodeOptions {
            dot_unstuffing: true,
        };
        let n = decode_body(&encoded, &mut output, &mut crc, opts).unwrap();
        assert_eq!(n, 1);
        assert_eq!(output[0], b'A');
    }

    #[test]
    fn decode_full_single_part() {
        let original = b"Hello, yEnc World!";
        let encoded_data = encode_raw(original);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(original);
        let crc_val = hasher.finalize();
        let crc_hex = format!("{:08x}", crc_val);

        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded_data);
        article.extend_from_slice(
            format!("\r\n=yend size={} crc32={}\r\n", original.len(), crc_hex).as_bytes(),
        );

        let mut output = vec![0u8; 1024];
        let result = decode(&article, &mut output).unwrap();

        assert_eq!(result.bytes_written, original.len());
        assert_eq!(&output[..result.bytes_written], original.as_slice());
        assert!(result.crc_valid);
        assert_eq!(result.metadata.name, "test.bin");
        assert_eq!(result.metadata.size, original.len() as u64);
    }

    #[test]
    fn decode_full_crc_mismatch_still_decodes() {
        let original = b"Test data";
        let encoded_data = encode_raw(original);

        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded_data);
        article.extend_from_slice(b"\r\n=yend size=9 crc32=DEADBEEF\r\n");

        let mut output = vec![0u8; 1024];
        let result = decode(&article, &mut output).unwrap();

        // Data should still be decoded.
        assert_eq!(result.bytes_written, original.len());
        assert_eq!(&output[..result.bytes_written], original.as_slice());
        // But CRC should be invalid.
        assert!(!result.crc_valid);
    }

    #[test]
    fn decode_full_missing_trailer() {
        let original = b"No trailer";
        let encoded_data = encode_raw(original);

        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded_data);
        article.extend_from_slice(b"\r\n");

        let mut output = vec![0u8; 1024];
        let result = decode(&article, &mut output).unwrap();

        assert_eq!(result.bytes_written, original.len());
        assert_eq!(&output[..result.bytes_written], original.as_slice());
        // No CRC to validate.
        assert!(result.crc_valid);
    }

    #[test]
    fn decode_empty_data() {
        let article = b"=ybegin line=128 size=0 name=empty.bin\r\n\
                         =yend size=0\r\n";
        let mut output = vec![0u8; 64];
        let result = decode(article, &mut output).unwrap();
        assert_eq!(result.bytes_written, 0);
    }

    #[test]
    fn decode_body_escaped_at_line_boundary() {
        // Escape sequence right before CRLF.
        let original = vec![214u8]; // encodes to =@ (NUL escape)
        let encoded = encode_raw(&original);

        let mut with_crlf = encoded.clone();
        with_crlf.extend_from_slice(b"\r\n");

        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let n = decode_body(&with_crlf, &mut output, &mut crc, DecodeOptions::default()).unwrap();
        assert_eq!(n, 1);
        assert_eq!(output[0], 214);
    }

    // ── decode_chunk tests ──────────────────────────────────────────────

    #[test]
    fn decode_chunk_simple() {
        // Decode full body in one chunk, compare to decode_body.
        let original = b"Hello, World!";
        let encoded = encode_raw(original);

        // One-shot via decode_body.
        let mut out_body = vec![0u8; 64];
        let mut crc_body = Crc32::new();
        let n_body = decode_body(
            &encoded,
            &mut out_body,
            &mut crc_body,
            DecodeOptions::default(),
        )
        .unwrap();
        let crc_body_val = crc_body.finalize();

        // One-shot via decode_chunk.
        let mut out_chunk = [0u8; 64];
        let mut state = DecodeState::new();
        let n_chunk = decode_chunk(
            &encoded,
            &mut out_chunk,
            &mut state,
            DecodeOptions::default(),
        )
        .unwrap();
        let crc_chunk_val = state.finalize_crc();

        assert_eq!(n_body, n_chunk);
        assert_eq!(&out_body[..n_body], &out_chunk[..n_chunk]);
        assert_eq!(crc_body_val, crc_chunk_val);
    }

    #[test]
    fn decode_chunk_split_escape() {
        // Split input in the middle of an =X escape sequence.
        // Use a byte that requires escaping: 214 encodes to =@ (NUL).
        let data = vec![214u8, 1u8, 2u8];
        let enc = encode_raw(&data); // Should be: =, @, +, ,

        // Split right after '=' (the escape char), before the escaped byte.
        let eq_pos = enc.iter().position(|&b| b == b'=').unwrap();
        let chunk1 = &enc[..=eq_pos]; // includes '='
        let chunk2 = &enc[eq_pos + 1..]; // starts with escaped byte

        let mut output = vec![0u8; 64];
        let mut state = DecodeState::new();
        let opts = DecodeOptions::default();

        let n1 = decode_chunk(chunk1, &mut output, &mut state, opts).unwrap();
        assert!(
            state.escape_pending,
            "escape should be pending after chunk ending with ="
        );

        let n2 = decode_chunk(chunk2, &mut output[n1..], &mut state, opts).unwrap();
        assert!(!state.escape_pending);

        let total = n1 + n2;
        assert_eq!(total, data.len());
        assert_eq!(&output[..total], &data[..]);

        // CRC must match one-shot.
        let mut crc_expected = Crc32::new();
        crc_expected.update(&data);
        assert_eq!(state.finalize_crc(), crc_expected.finalize());
    }

    #[test]
    fn decode_chunk_split_line_boundary() {
        // Split at a CRLF boundary.
        let original = b"ABCD";
        let mut encoded = Vec::new();
        encoded.extend(yenc_encode_byte(b'A'));
        encoded.extend(yenc_encode_byte(b'B'));
        encoded.extend_from_slice(b"\r\n");
        encoded.extend(yenc_encode_byte(b'C'));
        encoded.extend(yenc_encode_byte(b'D'));

        // Split right after CRLF.
        let crlf_end = encoded
            .array_windows()
            .position(|w: &[u8; 2]| w == b"\r\n")
            .unwrap()
            + 2;
        let chunk1 = &encoded[..crlf_end];
        let chunk2 = &encoded[crlf_end..];

        let mut output = vec![0u8; 64];
        let mut state = DecodeState::new();
        let opts = DecodeOptions::default();

        let n1 = decode_chunk(chunk1, &mut output, &mut state, opts).unwrap();
        assert!(state.at_line_start, "should be at line start after CRLF");

        let n2 = decode_chunk(chunk2, &mut output[n1..], &mut state, opts).unwrap();

        let total = n1 + n2;
        assert_eq!(total, 4);
        assert_eq!(&output[..total], original.as_slice());
    }

    #[test]
    fn decode_chunk_multi_chunk_crc() {
        // Decode in many small chunks, verify CRC matches one-shot.
        let original: Vec<u8> = (0..=255).collect();
        let encoded = encode_raw(&original);

        // One-shot CRC.
        let mut out_body = vec![0u8; 512];
        let mut crc_body = Crc32::new();
        let n_body = decode_body(
            &encoded,
            &mut out_body,
            &mut crc_body,
            DecodeOptions::default(),
        )
        .unwrap();
        let expected_crc = crc_body.finalize();

        // Chunked decode (chunks of 7 bytes for odd splitting).
        let mut out_chunk = vec![0u8; 512];
        let mut state = DecodeState::new();
        let opts = DecodeOptions::default();
        let mut total = 0usize;

        for chunk in encoded.chunks(7) {
            let n = decode_chunk(chunk, &mut out_chunk[total..], &mut state, opts).unwrap();
            total += n;
        }

        assert_eq!(total, n_body);
        assert_eq!(&out_chunk[..total], &out_body[..n_body]);
        assert_eq!(state.finalize_crc(), expected_crc);
    }

    #[test]
    fn decode_chunk_single_byte_chunks() {
        // Feed one byte at a time, verify result matches one-shot.
        let original = b"Hello, yEnc!";
        let encoded = encode_raw(original);

        // One-shot.
        let mut out_body = vec![0u8; 64];
        let mut crc_body = Crc32::new();
        let n_body = decode_body(
            &encoded,
            &mut out_body,
            &mut crc_body,
            DecodeOptions::default(),
        )
        .unwrap();
        let expected_crc = crc_body.finalize();

        // Single-byte chunks.
        let mut out_chunk = [0u8; 64];
        let mut state = DecodeState::new();
        let opts = DecodeOptions::default();
        let mut total = 0usize;

        for &byte in &encoded {
            let n = decode_chunk(&[byte], &mut out_chunk[total..], &mut state, opts).unwrap();
            total += n;
        }

        assert_eq!(total, n_body);
        assert_eq!(&out_chunk[..total], &out_body[..n_body]);
        assert_eq!(state.bytes_decoded, total as u64);
        assert_eq!(state.finalize_crc(), expected_crc);
    }

    #[test]
    fn decode_chunk_dot_unstuffing_across_chunks() {
        // Dot-unstuffing with chunk boundary at line start.
        // Scenario: chunk1 ends with \r\n, chunk2 starts with ".." (dot-stuffed line).
        let mut encoded = Vec::new();
        encoded.extend(yenc_encode_byte(b'A'));
        encoded.extend_from_slice(b"\r\n");
        // After CRLF, the line starts with ".." (NNTP dot-stuffed).
        // The real encoded byte is '.' = 0x2E, which decodes to (0x2E - 42) = 4.
        encoded.extend_from_slice(b"..");
        encoded.extend(yenc_encode_byte(b'B'));

        let crlf_end = encoded
            .array_windows()
            .position(|w: &[u8; 2]| w == b"\r\n")
            .unwrap()
            + 2;
        let chunk1 = &encoded[..crlf_end];
        let chunk2 = &encoded[crlf_end..];

        let opts = DecodeOptions {
            dot_unstuffing: true,
        };

        let mut output = vec![0u8; 64];
        let mut state = DecodeState::new();

        let n1 = decode_chunk(chunk1, &mut output, &mut state, opts).unwrap();
        assert!(state.at_line_start, "should be at line start after CRLF");

        let n2 = decode_chunk(chunk2, &mut output[n1..], &mut state, opts).unwrap();

        let total = n1 + n2;
        // Expected: 'A', then dot-unstuffed '.' decoded as byte 4, then 'B'.
        assert_eq!(total, 3);
        assert_eq!(output[0], b'A');
        assert_eq!(output[1], b'.'.wrapping_sub(42)); // 4
        assert_eq!(output[2], b'B');
    }

    // ── Size validation tests ─────────────────────────────────────────

    #[test]
    fn decode_full_size_mismatch_detected() {
        // =yend size differs from actual decoded bytes.
        let original = b"Hello";
        let encoded_data = encode_raw(original);

        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded_data);
        // Lie about size in =yend.
        article.extend_from_slice(b"\r\n=yend size=999\r\n");

        let mut output = vec![0u8; 1024];
        let result = decode(&article, &mut output);
        assert!(matches!(
            result,
            Err(YencError::SizeMismatch {
                expected: 999,
                actual: 5
            })
        ));
    }

    #[test]
    fn decode_full_single_part_begin_end_size_mismatch() {
        // Single-part: =ybegin size != =yend size.
        let original = b"Hello";
        let encoded_data = encode_raw(original);

        let mut article = Vec::new();
        article.extend_from_slice(b"=ybegin line=128 size=100 name=test.bin\r\n");
        article.extend_from_slice(&encoded_data);
        article.extend_from_slice(format!("\r\n=yend size={}\r\n", original.len()).as_bytes());

        let mut output = vec![0u8; 1024];
        let result = decode(&article, &mut output);
        // =ybegin size=100 != =yend size=5
        assert!(matches!(
            result,
            Err(YencError::SizeMismatch {
                expected: 100,
                actual: 5
            })
        ));
    }

    #[test]
    fn decode_full_has_trailer_flag() {
        let original = b"test";
        let encoded_data = encode_raw(original);

        // With trailer.
        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded_data);
        article.extend_from_slice(format!("\r\n=yend size={}\r\n", original.len()).as_bytes());

        let mut output = vec![0u8; 1024];
        let result = decode(&article, &mut output).unwrap();
        assert!(result.has_trailer);

        // Without trailer.
        let mut article2 = Vec::new();
        article2.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).as_bytes(),
        );
        article2.extend_from_slice(&encoded_data);

        let mut output2 = vec![0u8; 1024];
        let result2 = decode(&article2, &mut output2).unwrap();
        assert!(!result2.has_trailer);
    }

    // ── Dot-pending state machine tests ───────────────────────────────

    #[test]
    fn decode_chunk_dot_pending_terminator() {
        // Chunk1 ends with \r\n followed by '.', chunk2 starts with \r\n (terminator).
        let opts = DecodeOptions {
            dot_unstuffing: true,
        };
        let mut output = vec![0u8; 64];
        let mut state = DecodeState::new();

        // Chunk 1: encoded 'A' + \r\n + '.'
        let mut chunk1 = Vec::new();
        chunk1.extend(yenc_encode_byte(b'A'));
        chunk1.extend_from_slice(b"\r\n.");

        let n1 = decode_chunk(&chunk1, &mut output, &mut state, opts).unwrap();
        assert_eq!(n1, 1); // Only 'A' decoded.
        assert!(state.dot_pending, "dot should be pending");

        // Chunk 2: \r\n — this completes the NNTP terminator.
        let n2 = decode_chunk(b"\r\n", &mut output[n1..], &mut state, opts).unwrap();
        assert_eq!(n2, 0); // Terminator, no data.
        assert_eq!(output[0], b'A');
    }

    #[test]
    fn decode_chunk_dot_pending_stuffed() {
        // Chunk1 ends with \r\n + '.', chunk2 starts with '.' (dot-stuffed).
        let opts = DecodeOptions {
            dot_unstuffing: true,
        };
        let mut output = vec![0u8; 64];
        let mut state = DecodeState::new();

        let mut chunk1 = Vec::new();
        chunk1.extend(yenc_encode_byte(b'A'));
        chunk1.extend_from_slice(b"\r\n.");

        let n1 = decode_chunk(&chunk1, &mut output, &mut state, opts).unwrap();
        assert!(state.dot_pending);

        // Chunk 2: '.' + encoded 'B' — the first dot was stuffed, second is data.
        let mut chunk2 = Vec::new();
        chunk2.push(b'.');
        chunk2.extend(yenc_encode_byte(b'B'));

        let n2 = decode_chunk(&chunk2, &mut output[n1..], &mut state, opts).unwrap();
        assert!(!state.dot_pending);

        let total = n1 + n2;
        assert_eq!(total, 2); // 'A' and 'B'
        assert_eq!(output[0], b'A');
        assert_eq!(output[1], b'B');
    }

    #[test]
    fn decode_chunk_dot_pending_data() {
        // Chunk1 ends with \r\n + '.', chunk2 starts with a normal byte.
        // The dot was actual encoded data (shouldn't normally happen in NNTP,
        // but we handle it gracefully).
        let opts = DecodeOptions {
            dot_unstuffing: true,
        };
        let mut output = vec![0u8; 64];
        let mut state = DecodeState::new();

        let mut chunk1 = Vec::new();
        chunk1.extend(yenc_encode_byte(b'A'));
        chunk1.extend_from_slice(b"\r\n.");

        let n1 = decode_chunk(&chunk1, &mut output, &mut state, opts).unwrap();
        assert!(state.dot_pending);

        // Chunk 2: starts with 'k' — dot was data.
        let mut chunk2 = Vec::new();
        chunk2.extend(yenc_encode_byte(b'B'));

        let n2 = decode_chunk(&chunk2, &mut output[n1..], &mut state, opts).unwrap();

        let total = n1 + n2;
        assert_eq!(total, 3); // 'A', decoded dot, 'B'
        assert_eq!(output[0], b'A');
        assert_eq!(output[1], b'.'.wrapping_sub(42)); // dot decoded as data
        assert_eq!(output[2], b'B');
    }
}
