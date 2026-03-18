//! Incremental yEnc decoder for streaming article bodies.
//!
//! Accepts arbitrary byte chunks of pre-unstuffed NNTP body payload and
//! produces decoded output incrementally. Header lines (`=ybegin`, `=ypart`),
//! payload data, and trailer lines (`=yend`) are all consumed incrementally —
//! any of them may be split across feeds.
//!
//! This decoder has **no NNTP knowledge** — it receives clean payload bytes
//! with dot-unstuffing already handled by the transport layer.

use crate::error::YencError;
use crate::header;
use crate::types::{DecodeResult, YencMetadata};

const YEND_PREFIX: &[u8] = b"=yend ";

/// Incremental streaming yEnc decoder.
pub struct StreamingYencDecoder {
    phase: ParsePhase,
    /// Carry buffer for partial header/trailer lines across feeds.
    carry: Vec<u8>,
    /// Parsed metadata from `=ybegin` (and `=ypart`).
    metadata: Option<YencMetadata>,
    /// Parsed `=yend` trailer fields.
    yend: Option<header::YendFields>,
    /// Payload decode state (escapes, line position, CRC).
    decode_state: crate::decode::DecodeState,
    /// Whether the previous feed ended at a line start (for detecting `=yend`).
    at_line_start: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsePhase {
    /// Accumulating `=ybegin` (and optional `=ypart`) header lines.
    Header,
    /// Decoding payload data.
    Payload,
    /// Accumulating `=yend` trailer line.
    Trailer,
    /// Complete — `finish()` can be called.
    Done,
}

/// Result returned by `finish()` after the complete article has been fed.
pub struct StreamingDecodeResult {
    pub metadata: YencMetadata,
    pub bytes_written: usize,
    pub part_crc: u32,
    pub expected_part_crc: Option<u32>,
    pub expected_file_crc: Option<u32>,
    pub crc_valid: bool,
    pub has_trailer: bool,
}

impl StreamingYencDecoder {
    /// Create a new streaming decoder.
    pub fn new() -> Self {
        Self {
            phase: ParsePhase::Header,
            carry: Vec::with_capacity(512),
            metadata: None,
            yend: None,
            decode_state: crate::decode::DecodeState::new(),
            at_line_start: true,
        }
    }

    /// Feed a chunk of pre-unstuffed body payload. Returns bytes written
    /// to `output`. Header lines, payload data, and trailer lines are all
    /// consumed incrementally — any of them may be split across feeds.
    pub fn feed(&mut self, input: &[u8], output: &mut [u8]) -> Result<usize, YencError> {
        match self.phase {
            ParsePhase::Header => self.feed_header(input, output),
            ParsePhase::Payload => self.feed_payload(input, output),
            ParsePhase::Trailer => {
                self.feed_trailer(input)?;
                Ok(0)
            }
            ParsePhase::Done => Ok(0),
        }
    }

    /// Assert that a complete article was seen and validate CRC/size.
    /// Does NOT parse trailer bytes — those were consumed by `feed()`.
    pub fn finish(self) -> Result<StreamingDecodeResult, YencError> {
        let metadata = self.metadata.ok_or(YencError::MissingHeader)?;
        let bytes_written = self.decode_state.bytes_decoded as usize;
        let part_crc = self.decode_state.finalize_crc();

        let (expected_part_crc, expected_file_crc, yend_size) = match &self.yend {
            Some(yend) => (yend.pcrc32, yend.crc32, yend.size),
            None => (None, None, None),
        };

        // Validate decoded size against =yend size.
        if let Some(expected_size) = yend_size
            && bytes_written as u64 != expected_size
        {
            return Err(YencError::SizeMismatch {
                expected: expected_size,
                actual: bytes_written as u64,
            });
        }

        // For single-part articles, validate =ybegin size vs =yend size.
        if metadata.part.is_none()
            && let Some(expected_size) = yend_size
            && metadata.size != expected_size
        {
            return Err(YencError::SizeMismatch {
                expected: metadata.size,
                actual: expected_size,
            });
        }

        // CRC validation: same logic as decode.rs.
        let expected_crc_to_check = if metadata.part.is_some() {
            expected_part_crc
        } else {
            expected_file_crc
        };

        let crc_valid = match expected_crc_to_check {
            Some(expected) => part_crc == expected,
            None => true,
        };

        Ok(StreamingDecodeResult {
            metadata,
            bytes_written,
            part_crc,
            expected_part_crc,
            expected_file_crc,
            crc_valid,
            has_trailer: self.yend.is_some(),
        })
    }

    /// Accumulate header lines until we have a complete `=ybegin` (and
    /// optional `=ypart`). Returns decoded payload bytes if the input
    /// contains data past the headers.
    fn feed_header(&mut self, input: &[u8], output: &mut [u8]) -> Result<usize, YencError> {
        self.carry.extend_from_slice(input);

        // Don't attempt to parse until we have at least one complete line
        // after `=ybegin`. A partial header line would cause parse_headers
        // to find `=ybegin` but fail on missing fields.
        let ybegin_pos = self.carry.windows(8).position(|w| w == b"=ybegin ");
        if ybegin_pos.is_none() {
            // No =ybegin marker yet — need more data.
            if self.carry.len() > 8192 {
                return Err(YencError::MissingHeader);
            }
            return Ok(0);
        }

        // We have =ybegin. Check if the header line(s) are complete.
        // For single-part: need at least one \n after =ybegin.
        // For multi-part: need =ybegin line + =ypart line (two \n's).
        let after_ybegin = ybegin_pos.unwrap();
        let newlines_after: usize = self.carry[after_ybegin..]
            .iter()
            .filter(|&&b| b == b'\n')
            .count();

        // Check if this is multi-part (has "part=" in the =ybegin line).
        let is_multipart = self.carry[after_ybegin..].windows(5).any(|w| w == b"part=");

        let needed_newlines = if is_multipart { 2 } else { 1 };
        if newlines_after < needed_newlines {
            // Header line(s) not yet complete.
            if self.carry.len() > 8192 {
                return Err(YencError::MissingHeader);
            }
            return Ok(0);
        }

        // Try to parse headers from accumulated data.
        match header::parse_headers(&self.carry) {
            Ok(parsed) => {
                self.metadata = Some(parsed.metadata);
                let remaining = if parsed.data_start < self.carry.len() {
                    self.carry[parsed.data_start..].to_vec()
                } else {
                    Vec::new()
                };
                self.carry.clear();
                self.phase = ParsePhase::Payload;
                self.at_line_start = true;
                if !remaining.is_empty() {
                    self.feed_payload(&remaining, output)
                } else {
                    Ok(0)
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Decode payload bytes, watching for `=yend` at line starts.
    ///
    /// When `at_line_start` is true, bytes are checked against the
    /// `=yend ` prefix before being decoded. If `=yend` is split
    /// across feeds, partial matches are buffered in `carry`.
    fn feed_payload(&mut self, input: &[u8], output: &mut [u8]) -> Result<usize, YencError> {
        let opts = crate::decode::DecodeOptions {
            dot_unstuffing: false,
        };
        let mut total_written = 0usize;
        let mut src = 0usize;

        // If we have a pending partial =yend match from the previous feed,
        // try to resolve it with the new input.
        if !self.carry.is_empty() {
            let needed = YEND_PREFIX.len() - self.carry.len();
            let available = input.len().min(needed);
            self.carry.extend_from_slice(&input[..available]);
            src = available;

            if self.carry.len() >= YEND_PREFIX.len() {
                if self.carry.starts_with(YEND_PREFIX) {
                    // It IS =yend — transition to trailer.
                    self.phase = ParsePhase::Trailer;
                    // carry already has the start of the trailer line.
                    // Append the rest of the input.
                    self.carry.extend_from_slice(&input[src..]);
                    self.feed_trailer_from_carry()?;
                    return Ok(total_written);
                } else {
                    // False alarm — decode the buffered bytes as payload.
                    let buffered = std::mem::take(&mut self.carry);
                    let n = crate::decode::decode_chunk(
                        &buffered,
                        &mut output[total_written..],
                        &mut self.decode_state,
                        opts,
                    )?;
                    total_written += n;
                    self.at_line_start = buffered.last() == Some(&b'\n');
                }
            } else {
                // Still not enough data to determine — wait for more.
                return Ok(total_written);
            }
        }

        while src < input.len() {
            // At a line start, check for =yend prefix.
            if self.at_line_start && input[src] == b'=' {
                let remaining = &input[src..];
                if remaining.len() >= YEND_PREFIX.len() {
                    if remaining.starts_with(YEND_PREFIX) {
                        // Found =yend — transition to trailer.
                        self.phase = ParsePhase::Trailer;
                        self.carry.clear();
                        self.carry.extend_from_slice(remaining);
                        self.feed_trailer_from_carry()?;
                        return Ok(total_written);
                    }
                    // Not =yend, decode normally.
                } else {
                    // Partial match possible — buffer and wait.
                    self.carry.clear();
                    self.carry.extend_from_slice(remaining);
                    return Ok(total_written);
                }
            }

            // Find the next line boundary or end of input, decode up to there.
            let chunk_end = memchr::memchr(b'\n', &input[src..])
                .map(|pos| src + pos + 1)
                .unwrap_or(input.len());
            let chunk = &input[src..chunk_end];
            let n = crate::decode::decode_chunk(
                chunk,
                &mut output[total_written..],
                &mut self.decode_state,
                opts,
            )?;
            total_written += n;
            self.at_line_start = chunk.last() == Some(&b'\n');
            src = chunk_end;
        }

        Ok(total_written)
    }

    /// Feed the carry buffer (which contains partial or full =yend line)
    /// into the trailer parser.
    fn feed_trailer_from_carry(&mut self) -> Result<(), YencError> {
        let data = std::mem::take(&mut self.carry);
        self.feed_trailer(&data)
    }

    /// Accumulate the `=yend` trailer line. When the line is complete,
    /// parse it and transition to Done.
    fn feed_trailer(&mut self, input: &[u8]) -> Result<(), YencError> {
        self.carry.extend_from_slice(input);

        // Check if we have a complete line (ends with \n).
        if let Some(lf_pos) = memchr::memchr(b'\n', &self.carry) {
            let line_end = if lf_pos > 0 && self.carry[lf_pos - 1] == b'\r' {
                lf_pos - 1
            } else {
                lf_pos
            };
            let line = &self.carry[..line_end];

            // Parse the =yend line.
            if let Some(content) = line
                .strip_prefix(b"=yend ")
                .or_else(|| line.strip_prefix(b"=yend\t"))
            {
                let content_str = String::from_utf8_lossy(content);
                let fields = parse_yend_fields(&content_str);
                self.yend = Some(fields);
            }
            self.phase = ParsePhase::Done;
            self.carry.clear();
        }
        Ok(())
    }
}

impl Default for StreamingYencDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a `StreamingDecodeResult` into the standard `DecodeResult` type.
impl From<StreamingDecodeResult> for DecodeResult {
    fn from(r: StreamingDecodeResult) -> Self {
        DecodeResult {
            metadata: r.metadata,
            bytes_written: r.bytes_written,
            part_crc: r.part_crc,
            expected_part_crc: r.expected_part_crc,
            expected_file_crc: r.expected_file_crc,
            crc_valid: r.crc_valid,
            has_trailer: r.has_trailer,
        }
    }
}

/// Parse `=yend` fields from the content after `=yend `.
fn parse_yend_fields(content: &str) -> header::YendFields {
    let fields: Vec<(String, String)> = parse_kv_fields(content);
    header::YendFields {
        size: find_field_u64(&fields, "size"),
        part: find_field_u64(&fields, "part").map(|v| v as u32),
        pcrc32: find_field_hex(&fields, "pcrc32"),
        crc32: find_field_hex(&fields, "crc32"),
    }
}

fn parse_kv_fields(line: &str) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    let mut remaining = line;
    loop {
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }
        if let Some(eq_pos) = remaining.find('=') {
            let key = remaining[..eq_pos].trim().to_lowercase();
            let after_eq = &remaining[eq_pos + 1..];
            let value_end = after_eq.find(' ').unwrap_or(after_eq.len());
            let value = &after_eq[..value_end];
            fields.push((key, value.to_string()));
            remaining = &after_eq[value_end..];
        } else {
            break;
        }
    }
    fields
}

fn find_field_u64(fields: &[(String, String)], name: &str) -> Option<u64> {
    fields
        .iter()
        .find(|(k, _)| k == name)
        .and_then(|(_, v)| v.trim().parse().ok())
}

fn find_field_hex(fields: &[(String, String)], name: &str) -> Option<u32> {
    fields
        .iter()
        .find(|(k, _)| k == name)
        .and_then(|(_, v)| u32::from_str_radix(v.trim(), 16).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: encode a byte slice to raw yEnc data (no headers).
    fn encode_raw(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for &b in data {
            let encoded = b.wrapping_add(42);
            match encoded {
                0x00 | 0x0A | 0x0D | 0x3D => {
                    out.push(b'=');
                    out.push(encoded.wrapping_add(64));
                }
                _ => out.push(encoded),
            }
        }
        out
    }

    /// Build a complete yEnc single-part article.
    fn build_article(data: &[u8]) -> Vec<u8> {
        let encoded = encode_raw(data);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        let crc = hasher.finalize();
        let crc_hex = format!("{crc:08x}");

        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", data.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded);
        article.extend_from_slice(
            format!("\r\n=yend size={} crc32={crc_hex}\r\n", data.len()).as_bytes(),
        );
        article
    }

    /// Build a complete yEnc multi-part article.
    fn build_multipart_article(data: &[u8], part: u32, total: u32, begin: u64) -> Vec<u8> {
        let encoded = encode_raw(data);
        let end = begin + data.len() as u64 - 1;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        let pcrc = hasher.finalize();
        let pcrc_hex = format!("{pcrc:08x}");

        let file_size = total as u64 * data.len() as u64; // approximate

        let mut article = Vec::new();
        article.extend_from_slice(
            format!(
                "=ybegin part={part} total={total} line=128 size={file_size} name=test.bin\r\n"
            )
            .as_bytes(),
        );
        article.extend_from_slice(format!("=ypart begin={begin} end={end}\r\n").as_bytes());
        article.extend_from_slice(&encoded);
        article.extend_from_slice(
            format!(
                "\r\n=yend size={} part={part} pcrc32={pcrc_hex}\r\n",
                data.len()
            )
            .as_bytes(),
        );
        article
    }

    /// Decode an article using the streaming decoder in one feed.
    fn decode_streaming_one_shot(article: &[u8]) -> (Vec<u8>, StreamingDecodeResult) {
        let mut decoder = StreamingYencDecoder::new();
        let mut output = vec![0u8; article.len()];
        let written = decoder.feed(article, &mut output).unwrap();
        output.truncate(written);
        let result = decoder.finish().unwrap();
        (output, result)
    }

    /// Decode an article using the streaming decoder in chunks of `chunk_size`.
    fn decode_streaming_chunked(
        article: &[u8],
        chunk_size: usize,
    ) -> (Vec<u8>, StreamingDecodeResult) {
        let mut decoder = StreamingYencDecoder::new();
        let mut output = vec![0u8; article.len()];
        let mut total_written = 0;
        for chunk in article.chunks(chunk_size) {
            let written = decoder.feed(chunk, &mut output[total_written..]).unwrap();
            total_written += written;
        }
        output.truncate(total_written);
        let result = decoder.finish().unwrap();
        (output, result)
    }

    /// Decode an article using the reference `decode` function.
    fn decode_reference(article: &[u8]) -> (Vec<u8>, DecodeResult) {
        let mut output = vec![0u8; article.len()];
        let result = crate::decode::decode(article, &mut output).unwrap();
        output.truncate(result.bytes_written);
        (output, result)
    }

    #[test]
    fn one_shot_matches_reference() {
        let data = b"Hello, streaming yEnc World!";
        let article = build_article(data);

        let (ref_out, ref_result) = decode_reference(&article);
        let (stream_out, stream_result) = decode_streaming_one_shot(&article);

        assert_eq!(ref_out, stream_out);
        assert_eq!(ref_result.bytes_written, stream_result.bytes_written);
        assert_eq!(ref_result.part_crc, stream_result.part_crc);
        assert_eq!(ref_result.crc_valid, stream_result.crc_valid);
        assert!(stream_result.crc_valid);
    }

    #[test]
    fn chunked_matches_reference() {
        let data: Vec<u8> = (0..=255).cycle().take(1000).collect();
        let article = build_article(&data);

        let (ref_out, ref_result) = decode_reference(&article);

        // Test various chunk sizes including pathological ones.
        for chunk_size in [1, 3, 7, 13, 64, 128, 500, 4096] {
            let (stream_out, stream_result) = decode_streaming_chunked(&article, chunk_size);
            assert_eq!(
                ref_out, stream_out,
                "output mismatch at chunk_size={chunk_size}"
            );
            assert_eq!(
                ref_result.part_crc, stream_result.part_crc,
                "CRC mismatch at chunk_size={chunk_size}"
            );
            assert!(
                stream_result.crc_valid,
                "CRC invalid at chunk_size={chunk_size}"
            );
        }
    }

    #[test]
    fn header_split_across_feeds() {
        let data = b"test data";
        let article = build_article(data);

        // Split in the middle of "=ybegin".
        let (stream_out, stream_result) = decode_streaming_chunked(&article, 5);
        let (ref_out, _) = decode_reference(&article);
        assert_eq!(ref_out, stream_out);
        assert!(stream_result.crc_valid);
    }

    #[test]
    fn trailer_split_across_feeds() {
        let data = b"trailer split test";
        let article = build_article(data);

        // Use chunk size that will likely split the =yend line.
        let (stream_out, stream_result) = decode_streaming_chunked(&article, 11);
        let (ref_out, _) = decode_reference(&article);
        assert_eq!(ref_out, stream_out);
        assert!(stream_result.crc_valid);
    }

    #[test]
    fn escape_split_across_feeds() {
        // Use bytes that encode with escape sequences.
        let data: Vec<u8> = vec![214, 224, 227, 19]; // critical bytes
        let article = build_article(&data);

        for chunk_size in [1, 2, 3] {
            let (stream_out, stream_result) = decode_streaming_chunked(&article, chunk_size);
            let (ref_out, _) = decode_reference(&article);
            assert_eq!(
                ref_out, stream_out,
                "escape split mismatch at chunk_size={chunk_size}"
            );
            assert!(stream_result.crc_valid);
        }
    }

    #[test]
    fn multipart_article() {
        let data = b"part one data here";
        let article = build_multipart_article(data, 1, 10, 1);

        let (stream_out, stream_result) = decode_streaming_one_shot(&article);
        assert_eq!(stream_out, data);
        assert!(stream_result.crc_valid);
        assert_eq!(stream_result.metadata.part, Some(1));
        assert_eq!(stream_result.metadata.total, Some(10));
        assert_eq!(stream_result.metadata.begin, Some(1));
    }

    #[test]
    fn missing_trailer_still_works() {
        let data = b"no trailer";
        let encoded = encode_raw(data);
        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", data.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded);

        let (stream_out, stream_result) = decode_streaming_one_shot(&article);
        assert_eq!(stream_out, data);
        assert!(!stream_result.has_trailer);
        assert!(stream_result.crc_valid); // no CRC to validate
    }

    #[test]
    fn crc_mismatch_detected() {
        let data = b"crc test";
        let encoded = encode_raw(data);
        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", data.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded);
        article.extend_from_slice(
            format!("\r\n=yend size={} crc32=DEADBEEF\r\n", data.len()).as_bytes(),
        );

        let (_stream_out, stream_result) = decode_streaming_one_shot(&article);
        assert!(!stream_result.crc_valid);
    }

    #[test]
    fn size_mismatch_detected() {
        let data = b"size test";
        let encoded = encode_raw(data);
        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", data.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded);
        article.extend_from_slice(b"\r\n=yend size=999\r\n");

        let mut decoder = StreamingYencDecoder::new();
        let mut output = vec![0u8; 1024];
        let _ = decoder.feed(&article, &mut output);
        let result = decoder.finish();
        assert!(matches!(result, Err(YencError::SizeMismatch { .. })));
    }

    #[test]
    fn empty_data() {
        let article = b"=ybegin line=128 size=0 name=empty.bin\r\n=yend size=0\r\n";
        let (stream_out, stream_result) = decode_streaming_one_shot(article);
        assert!(stream_out.is_empty());
        assert_eq!(stream_result.bytes_written, 0);
    }

    #[test]
    fn large_article_chunked() {
        // Larger data to exercise SIMD paths.
        let data: Vec<u8> = (0..=255).cycle().take(100_000).collect();
        let article = build_article(&data);

        let (ref_out, ref_result) = decode_reference(&article);
        let (stream_out, stream_result) = decode_streaming_chunked(&article, 65536);

        assert_eq!(ref_out, stream_out);
        assert_eq!(ref_result.part_crc, stream_result.part_crc);
        assert!(stream_result.crc_valid);
    }
}
