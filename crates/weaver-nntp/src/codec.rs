use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::NntpError;

/// Maximum size for a single response line (generous limit).
const MAX_LINE_LENGTH: usize = 16 * 1024;

/// Maximum size for a multi-line response body (256 MB).
const MAX_MULTILINE_LENGTH: usize = 256 * 1024 * 1024;

/// A chunk from streaming multiline decode.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamChunk {
    /// A chunk of decoded data. More may follow.
    Data(BytesMut),
    /// The multiline block has ended (terminator found).
    End,
}

/// Frames produced by the NNTP codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NntpFrame {
    /// A single response line (without the trailing CRLF).
    Line(String),
    /// A complete multi-line data block (dot-unstuffed, without terminating ".\r\n").
    MultiLineData(BytesMut),
}

/// NNTP codec that operates in two modes:
///
/// 1. **Line mode** (default): reads CRLF-terminated lines, yields `NntpFrame::Line`.
/// 2. **Multi-line mode**: reads until the `.\r\n` terminator on its own line,
///    performs dot-unstuffing, yields `NntpFrame::MultiLineData`.
///
/// The caller switches modes via `set_multiline(true)` after receiving a status
/// code that indicates a multi-line response follows.
pub struct NntpCodec {
    multiline: bool,
    streaming_multiline: bool,
}

impl NntpCodec {
    /// Create a new codec in line mode.
    pub fn new() -> Self {
        NntpCodec {
            multiline: false,
            streaming_multiline: false,
        }
    }

    /// Switch to multi-line mode for reading the next data block.
    pub fn set_multiline(&mut self, multiline: bool) {
        self.multiline = multiline;
    }

    /// Whether the codec is currently in multi-line mode.
    pub fn is_multiline(&self) -> bool {
        self.multiline
    }

    /// Set the codec into streaming multiline mode.
    pub fn set_streaming_multiline(&mut self, streaming: bool) {
        self.streaming_multiline = streaming;
    }

    /// Decode the next chunk in streaming multiline mode.
    ///
    /// Yields complete lines (dot-unstuffed) as `Data` chunks.
    /// Yields `End` when the `.\r\n` terminator is found.
    pub fn decode_streaming_chunk(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<StreamChunk>, NntpError> {
        if src.is_empty() {
            return Ok(None);
        }

        // Check for empty body: buffer starts with ".\r\n" or ".\n"
        if src.len() >= 2 && src[0] == b'.' && src[1] == b'\n' {
            src.advance(2);
            self.streaming_multiline = false;
            return Ok(Some(StreamChunk::End));
        }
        if src.len() >= 3 && src[0] == b'.' && src[1] == b'\r' && src[2] == b'\n' {
            src.advance(3);
            self.streaming_multiline = false;
            return Ok(Some(StreamChunk::End));
        }

        // Look for line-ending + "." + line-ending terminator within the buffer.
        if let Some((pos, le_len, dot_term_len)) = find_line_dot_line(src) {
            // Data is everything up to and including the line ending before the dot.
            let data_end = pos + le_len;
            let raw = src.split_to(data_end);
            // Consume the ".\r\n" or ".\n"
            src.advance(dot_term_len);
            let unstuffed = dot_unstuff(&raw);
            self.streaming_multiline = false;
            return Ok(Some(StreamChunk::Data(unstuffed)));
        }

        // No terminator found. Yield complete lines up to the last line ending,
        // leaving any partial line in the buffer for the next read.
        if let Some((last_pos, term_len)) = rfind_line_ending(src) {
            let split_at = last_pos + term_len;
            let raw = src.split_to(split_at);
            let unstuffed = dot_unstuff(&raw);
            Ok(Some(StreamChunk::Data(unstuffed)))
        } else {
            // No complete line yet — need more data.
            Ok(None)
        }
    }

    /// Decode a single line terminated by `\r\n` or bare `\n`.
    fn decode_line(&self, src: &mut BytesMut) -> Result<Option<NntpFrame>, NntpError> {
        if let Some((pos, term_len)) = find_line_ending(src) {
            if pos > MAX_LINE_LENGTH {
                return Err(NntpError::MalformedResponse(
                    "response line too long".into(),
                ));
            }
            let line_bytes = src.split_to(pos);
            // Skip the line terminator (\r\n or \n)
            src.advance(term_len);
            // Convert to string — treat as lossy UTF-8 per plan (some servers send non-UTF-8)
            let line = String::from_utf8_lossy(&line_bytes).into_owned();
            Ok(Some(NntpFrame::Line(line)))
        } else {
            // Not enough data yet
            if src.len() > MAX_LINE_LENGTH {
                return Err(NntpError::MalformedResponse(
                    "response line too long".into(),
                ));
            }
            Ok(None)
        }
    }

    /// Decode a multi-line data block terminated by `\r\n.\r\n` (or bare `\n` variants).
    fn decode_multiline(&self, src: &mut BytesMut) -> Result<Option<NntpFrame>, NntpError> {
        // Look for the termination sequence: a line containing only "." followed
        // by a line ending. Handles \r\n.\r\n, \n.\n, \n.\r\n, \r\n.\n variants.
        if let Some((end_pos, dot_term_len)) = find_multiline_terminator(src) {
            if end_pos > MAX_MULTILINE_LENGTH {
                return Err(NntpError::MalformedResponse(
                    "multi-line response too large".into(),
                ));
            }

            // Extract the raw data up to (but not including) the terminator line.
            let raw = &src[..end_pos];

            // Dot-unstuff the data.
            let unstuffed = dot_unstuff(raw);

            // Advance past the data + the dot terminator (".\r\n" or ".\n")
            src.advance(end_pos + dot_term_len);

            Ok(Some(NntpFrame::MultiLineData(unstuffed)))
        } else {
            if src.len() > MAX_MULTILINE_LENGTH {
                return Err(NntpError::MalformedResponse(
                    "multi-line response too large".into(),
                ));
            }
            Ok(None)
        }
    }
}

impl Default for NntpCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for NntpCodec {
    type Item = NntpFrame;
    type Error = NntpError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.multiline {
            let result = self.decode_multiline(src)?;
            if result.is_some() {
                // Automatically switch back to line mode after yielding multi-line data.
                self.multiline = false;
            }
            Ok(result)
        } else {
            self.decode_line(src)
        }
    }
}

impl Encoder<bytes::Bytes> for NntpCodec {
    type Error = NntpError;

    fn encode(&mut self, item: bytes::Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item);
        Ok(())
    }
}

/// Find the position of the first line terminator (`\r\n` or bare `\n`) in the buffer.
///
/// Returns `(position, terminator_length)` where `terminator_length` is 2 for `\r\n`
/// and 1 for bare `\n`. Position points to the start of the line content (i.e., the
/// byte offset of the `\r` in `\r\n` or the `\n` for bare `\n`).
fn find_line_ending(buf: &[u8]) -> Option<(usize, usize)> {
    // Search for any \n — it's either bare \n or the \n in \r\n.
    let nl_pos = memchr::memchr(b'\n', buf)?;
    if nl_pos > 0 && buf[nl_pos - 1] == b'\r' {
        Some((nl_pos - 1, 2)) // \r\n
    } else {
        Some((nl_pos, 1)) // bare \n
    }
}

/// Find the `\n.\n` or `\n.\r\n` or `\r\n.\r\n` or `\r\n.\n` terminator in the buffer.
///
/// Returns `(pos, line_ending_len, term_dot_len)` where:
/// - `pos` is the index of the `\r` or `\n` that starts the line ending before the dot
/// - `line_ending_len` is the length of that line ending (1 for `\n`, 2 for `\r\n`)
/// - `term_dot_len` is the total length of `.<terminator>` after the line ending (2 for `.\n`, 3 for `.\r\n`)
fn find_line_dot_line(buf: &[u8]) -> Option<(usize, usize, usize)> {
    // Search for every \n that could precede a dot-terminator.
    let mut start = 0;
    while start < buf.len() {
        let nl_pos = match memchr::memchr(b'\n', &buf[start..]) {
            Some(p) => start + p,
            None => return None,
        };
        // Check if \n is followed by '.' and then another line ending
        if nl_pos + 1 < buf.len() && buf[nl_pos + 1] == b'.' {
            // Check what follows the dot
            if nl_pos + 2 < buf.len() {
                if buf[nl_pos + 2] == b'\n' {
                    // \n.\n — the line ending before is \r\n or \n
                    let (pos, le_len) = if nl_pos > 0 && buf[nl_pos - 1] == b'\r' {
                        (nl_pos - 1, 2) // \r\n.\n
                    } else {
                        (nl_pos, 1) // \n.\n
                    };
                    return Some((pos, le_len, 2)); // dot_term = ".\n"
                } else if buf[nl_pos + 2] == b'\r' && nl_pos + 3 < buf.len() && buf[nl_pos + 3] == b'\n' {
                    let (pos, le_len) = if nl_pos > 0 && buf[nl_pos - 1] == b'\r' {
                        (nl_pos - 1, 2) // \r\n.\r\n
                    } else {
                        (nl_pos, 1) // \n.\r\n
                    };
                    return Some((pos, le_len, 3)); // dot_term = ".\r\n"
                }
            }
        }
        start = nl_pos + 1;
    }
    None
}

/// Find the last line ending (`\r\n` or bare `\n`) in the buffer.
///
/// Returns `(position, terminator_length)` where position is the index of `\r`
/// (for `\r\n`) or `\n` (for bare `\n`).
fn rfind_line_ending(buf: &[u8]) -> Option<(usize, usize)> {
    let nl_pos = memchr::memrchr(b'\n', buf)?;
    if nl_pos > 0 && buf[nl_pos - 1] == b'\r' {
        Some((nl_pos - 1, 2))
    } else {
        Some((nl_pos, 1))
    }
}

/// Find the position within `buf` where the multi-line terminator begins.
///
/// The terminator is a line consisting of just "." followed by a line ending.
/// This appears as `\r\n.\r\n`, `\n.\n`, `\n.\r\n`, or `\r\n.\n` in the stream,
/// or at the very start of the buffer if the data block is empty (`.\r\n` or `.\n`).
///
/// Returns `(data_end, dot_term_len)` where `buf[..data_end]` is the data before
/// the terminator and `dot_term_len` is the length of the `.\r\n` or `.\n` to skip.
fn find_multiline_terminator(buf: &[u8]) -> Option<(usize, usize)> {
    // Check for empty data block: starts with ".\r\n" or ".\n"
    if buf.len() >= 2 && buf[0] == b'.' && buf[1] == b'\n' {
        return Some((0, 2)); // ".\n"
    }
    if buf.len() >= 3 && buf[0] == b'.' && buf[1] == b'\r' && buf[2] == b'\n' {
        return Some((0, 3)); // ".\r\n"
    }
    // Look for line-ending + "." + line-ending
    let (pos, le_len, dot_term_len) = find_line_dot_line(buf)?;
    // Data includes everything up to and including the line ending before the dot.
    // pos is where the line ending starts, le_len is its length.
    Some((pos + le_len, dot_term_len))
}

/// Perform NNTP dot-unstuffing on raw multi-line data.
///
/// Per RFC 3977 Section 3.1.1: if a line begins with a dot, the first dot is
/// removed (it was added by the server to avoid confusion with the terminator).
pub fn dot_unstuff(data: &[u8]) -> BytesMut {
    let mut result = BytesMut::with_capacity(data.len());
    let mut i = 0;
    let mut line_start = true;

    while i < data.len() {
        if line_start && i < data.len() && data[i] == b'.' {
            // Skip the stuffed dot at the beginning of the line.
            i += 1;
            line_start = false;
            continue;
        }

        if data[i] == b'\n' {
            result.extend_from_slice(&[b'\n']);
            line_start = true;
            i += 1;
        } else {
            result.extend_from_slice(&[data[i]]);
            line_start = false;
            i += 1;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn decode_single_line() {
        let mut codec = NntpCodec::new();
        let mut buf = BytesMut::from("200 Welcome\r\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, NntpFrame::Line("200 Welcome".into()));
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_partial_line() {
        let mut codec = NntpCodec::new();
        let mut buf = BytesMut::from("200 Welco");
        assert!(codec.decode(&mut buf).unwrap().is_none());
        // Add the rest
        buf.extend_from_slice(b"me\r\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, NntpFrame::Line("200 Welcome".into()));
    }

    #[test]
    fn decode_multiple_lines() {
        let mut codec = NntpCodec::new();
        let mut buf = BytesMut::from("200 OK\r\n281 Auth OK\r\n");

        let frame1 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame1, NntpFrame::Line("200 OK".into()));

        let frame2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame2, NntpFrame::Line("281 Auth OK".into()));

        assert!(buf.is_empty());
    }

    #[test]
    fn decode_multiline_simple() {
        let mut codec = NntpCodec::new();
        codec.set_multiline(true);

        let mut buf = BytesMut::from("line one\r\nline two\r\n.\r\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();

        match frame {
            NntpFrame::MultiLineData(data) => {
                assert_eq!(&data[..], b"line one\r\nline two\r\n");
            }
            _ => panic!("expected MultiLineData"),
        }
        assert!(buf.is_empty());
        // Should auto-switch back to line mode
        assert!(!codec.is_multiline());
    }

    #[test]
    fn decode_multiline_empty() {
        let mut codec = NntpCodec::new();
        codec.set_multiline(true);

        let mut buf = BytesMut::from(".\r\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();

        match frame {
            NntpFrame::MultiLineData(data) => {
                assert!(data.is_empty());
            }
            _ => panic!("expected MultiLineData"),
        }
    }

    #[test]
    fn decode_multiline_dot_unstuffing() {
        let mut codec = NntpCodec::new();
        codec.set_multiline(true);

        // Server sends "..hello" (dot-stuffed) -> should become ".hello"
        let mut buf = BytesMut::from("..hello\r\nnormal\r\n.\r\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();

        match frame {
            NntpFrame::MultiLineData(data) => {
                assert_eq!(&data[..], b".hello\r\nnormal\r\n");
            }
            _ => panic!("expected MultiLineData"),
        }
    }

    #[test]
    fn decode_multiline_partial() {
        let mut codec = NntpCodec::new();
        codec.set_multiline(true);

        let mut buf = BytesMut::from("data here\r\n");
        assert!(codec.decode(&mut buf).unwrap().is_none());

        buf.extend_from_slice(b"more data\r\n.\r\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();

        match frame {
            NntpFrame::MultiLineData(data) => {
                assert_eq!(&data[..], b"data here\r\nmore data\r\n");
            }
            _ => panic!("expected MultiLineData"),
        }
    }

    #[test]
    fn dot_unstuff_identity_no_dots() {
        let input = b"hello\r\nworld\r\n";
        let result = dot_unstuff(input);
        assert_eq!(&result[..], input);
    }

    #[test]
    fn dot_unstuff_leading_dots() {
        let input = b"..stuffed\r\n...two dots\r\n";
        let result = dot_unstuff(input);
        assert_eq!(&result[..], b".stuffed\r\n..two dots\r\n");
    }

    #[test]
    fn dot_unstuff_dot_in_middle_of_line() {
        // Dots not at the start of a line should be preserved.
        let input = b"no.dots.here\r\n";
        let result = dot_unstuff(input);
        assert_eq!(&result[..], b"no.dots.here\r\n");
    }

    #[test]
    fn encode_command() {
        let mut codec = NntpCodec::new();
        let mut buf = BytesMut::new();
        let cmd = Bytes::from("BODY <test@example.com>\r\n");
        codec.encode(cmd, &mut buf).unwrap();
        assert_eq!(&buf[..], b"BODY <test@example.com>\r\n");
    }

    #[test]
    fn streaming_decode_simple_body() {
        let mut codec = NntpCodec::new();
        codec.set_streaming_multiline(true);

        let mut buf = BytesMut::from("line one\r\nline two\r\n.\r\n");
        let chunk = codec.decode_streaming_chunk(&mut buf).unwrap().unwrap();
        match chunk {
            StreamChunk::Data(data) => {
                assert_eq!(&data[..], b"line one\r\nline two\r\n");
            }
            StreamChunk::End => panic!("expected Data, got End"),
        }
        // Next call should yield End — but actually End was returned inline
        // because \r\n.\r\n was found, so streaming mode should be off.
        assert!(!codec.streaming_multiline);
        assert!(buf.is_empty());
    }

    #[test]
    fn streaming_decode_empty_body() {
        let mut codec = NntpCodec::new();
        codec.set_streaming_multiline(true);

        let mut buf = BytesMut::from(".\r\n");
        let chunk = codec.decode_streaming_chunk(&mut buf).unwrap().unwrap();
        assert_eq!(chunk, StreamChunk::End);
        assert!(!codec.streaming_multiline);
        assert!(buf.is_empty());
    }

    #[test]
    fn streaming_decode_dot_unstuffing() {
        let mut codec = NntpCodec::new();
        codec.set_streaming_multiline(true);

        // "..hello" is dot-stuffed, should become ".hello"
        let mut buf = BytesMut::from("..hello\r\nnormal\r\n.\r\n");
        let chunk = codec.decode_streaming_chunk(&mut buf).unwrap().unwrap();
        match chunk {
            StreamChunk::Data(data) => {
                assert_eq!(&data[..], b".hello\r\nnormal\r\n");
            }
            StreamChunk::End => panic!("expected Data"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn streaming_decode_partial_data() {
        let mut codec = NntpCodec::new();
        codec.set_streaming_multiline(true);

        // First: partial data with no complete line
        let mut buf = BytesMut::from("partial");
        let result = codec.decode_streaming_chunk(&mut buf).unwrap();
        assert!(result.is_none());
        assert!(codec.streaming_multiline); // still streaming

        // Add a complete line but no terminator
        buf.extend_from_slice(b" line\r\nmore ");
        let chunk = codec.decode_streaming_chunk(&mut buf).unwrap().unwrap();
        match chunk {
            StreamChunk::Data(data) => {
                assert_eq!(&data[..], b"partial line\r\n");
            }
            StreamChunk::End => panic!("expected Data"),
        }
        // "more " should remain in the buffer
        assert_eq!(&buf[..], b"more ");
        assert!(codec.streaming_multiline); // still streaming

        // Now finish with terminator
        buf.extend_from_slice(b"data\r\n.\r\n");
        let chunk = codec.decode_streaming_chunk(&mut buf).unwrap().unwrap();
        match chunk {
            StreamChunk::Data(data) => {
                assert_eq!(&data[..], b"more data\r\n");
            }
            StreamChunk::End => panic!("expected Data"),
        }
        assert!(!codec.streaming_multiline);
        assert!(buf.is_empty());
    }

    #[test]
    fn line_then_multiline_sequence() {
        let mut codec = NntpCodec::new();
        let mut buf = BytesMut::from("222 body follows\r\nthe body data\r\n.\r\n");

        // First: read the status line
        let line = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(line, NntpFrame::Line("222 body follows".into()));

        // Switch to multiline for the body
        codec.set_multiline(true);
        let body = codec.decode(&mut buf).unwrap().unwrap();
        match body {
            NntpFrame::MultiLineData(data) => {
                assert_eq!(&data[..], b"the body data\r\n");
            }
            _ => panic!("expected MultiLineData"),
        }
    }

    // --- Bare \n tests ---

    #[test]
    fn decode_single_line_bare_lf() {
        let mut codec = NntpCodec::new();
        let mut buf = BytesMut::from("200 Welcome\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, NntpFrame::Line("200 Welcome".into()));
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_multiline_bare_lf() {
        let mut codec = NntpCodec::new();
        codec.set_multiline(true);

        let mut buf = BytesMut::from("line one\nline two\n.\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();

        match frame {
            NntpFrame::MultiLineData(data) => {
                assert_eq!(&data[..], b"line one\nline two\n");
            }
            _ => panic!("expected MultiLineData"),
        }
        assert!(buf.is_empty());
        assert!(!codec.is_multiline());
    }

    #[test]
    fn decode_multiline_empty_bare_lf() {
        let mut codec = NntpCodec::new();
        codec.set_multiline(true);

        let mut buf = BytesMut::from(".\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();

        match frame {
            NntpFrame::MultiLineData(data) => {
                assert!(data.is_empty());
            }
            _ => panic!("expected MultiLineData"),
        }
    }

    #[test]
    fn streaming_decode_bare_lf() {
        let mut codec = NntpCodec::new();
        codec.set_streaming_multiline(true);

        let mut buf = BytesMut::from("line one\nline two\n.\n");
        let chunk = codec.decode_streaming_chunk(&mut buf).unwrap().unwrap();
        match chunk {
            StreamChunk::Data(data) => {
                assert_eq!(&data[..], b"line one\nline two\n");
            }
            StreamChunk::End => panic!("expected Data, got End"),
        }
        assert!(!codec.streaming_multiline);
        assert!(buf.is_empty());
    }

    #[test]
    fn streaming_decode_empty_body_bare_lf() {
        let mut codec = NntpCodec::new();
        codec.set_streaming_multiline(true);

        let mut buf = BytesMut::from(".\n");
        let chunk = codec.decode_streaming_chunk(&mut buf).unwrap().unwrap();
        assert_eq!(chunk, StreamChunk::End);
        assert!(!codec.streaming_multiline);
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_mixed_crlf_and_bare_lf() {
        // Status line with \r\n, body lines with mixed terminators, terminator with \n
        let mut codec = NntpCodec::new();
        let mut buf = BytesMut::from("222 body follows\r\nfirst line\nsecond line\r\n.\n");

        let line = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(line, NntpFrame::Line("222 body follows".into()));

        codec.set_multiline(true);
        let body = codec.decode(&mut buf).unwrap().unwrap();
        match body {
            NntpFrame::MultiLineData(data) => {
                assert_eq!(&data[..], b"first line\nsecond line\r\n");
            }
            _ => panic!("expected MultiLineData"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_mixed_terminators_multiline() {
        // Body uses \n, but terminator uses \r\n
        let mut codec = NntpCodec::new();
        codec.set_multiline(true);

        let mut buf = BytesMut::from("line one\nline two\n.\r\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();

        match frame {
            NntpFrame::MultiLineData(data) => {
                assert_eq!(&data[..], b"line one\nline two\n");
            }
            _ => panic!("expected MultiLineData"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn streaming_decode_mixed_terminators() {
        let mut codec = NntpCodec::new();
        codec.set_streaming_multiline(true);

        // Mix of \r\n and \n lines, terminator with bare \n
        let mut buf = BytesMut::from("first\r\nsecond\nthird\r\n.\n");
        let chunk = codec.decode_streaming_chunk(&mut buf).unwrap().unwrap();
        match chunk {
            StreamChunk::Data(data) => {
                assert_eq!(&data[..], b"first\r\nsecond\nthird\r\n");
            }
            StreamChunk::End => panic!("expected Data, got End"),
        }
        assert!(!codec.streaming_multiline);
        assert!(buf.is_empty());
    }
}
