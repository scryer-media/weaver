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

#[derive(Debug)]
struct MultilineScan {
    data_end: usize,
    terminator_len: usize,
    copied: Option<BytesMut>,
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
        let Some(scan) = scan_multiline_chunk(src, true) else {
            return Ok(None);
        };

        let chunk = take_multiline_output(src, scan);
        if chunk.terminator_len > 0 {
            self.streaming_multiline = false;
        }
        if chunk.data.is_empty() && chunk.terminator_len > 0 {
            return Ok(Some(StreamChunk::End));
        }
        Ok(Some(StreamChunk::Data(chunk.data)))
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
        match scan_multiline_chunk(src, false) {
            Some(scan) => {
                if scan.data_end > MAX_MULTILINE_LENGTH {
                    return Err(NntpError::MalformedResponse(
                        "multi-line response too large".into(),
                    ));
                }
                let chunk = take_multiline_output(src, scan);
                Ok(Some(NntpFrame::MultiLineData(chunk.data)))
            }
            None => {
                if src.len() > MAX_MULTILINE_LENGTH {
                    return Err(NntpError::MalformedResponse(
                        "multi-line response too large".into(),
                    ));
                }
                Ok(None)
            }
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

fn scan_multiline_chunk(buf: &[u8], allow_partial: bool) -> Option<MultilineScan> {
    if buf.is_empty() {
        return None;
    }

    let mut cursor = 0usize;
    let mut last_complete_end = 0usize;
    let mut copy_cursor = 0usize;
    let mut copied: Option<BytesMut> = None;

    while cursor < buf.len() {
        let Some(line_rel_end) = memchr::memchr(b'\n', &buf[cursor..]) else {
            break;
        };
        let line_end = cursor + line_rel_end;
        let content_end = if line_end > cursor && buf[line_end - 1] == b'\r' {
            line_end - 1
        } else {
            line_end
        };
        let line_total_end = line_end + 1;

        if cursor < content_end && buf[cursor] == b'.' {
            // A line consisting of just "." is the multiline terminator.
            if content_end == cursor + 1 {
                if let Some(ref mut output) = copied
                    && copy_cursor < cursor
                {
                    output.extend_from_slice(&buf[copy_cursor..cursor]);
                }
                return Some(MultilineScan {
                    data_end: cursor,
                    terminator_len: line_total_end - cursor,
                    copied,
                });
            }

            let output = copied.get_or_insert_with(|| {
                let mut output = BytesMut::with_capacity(buf.len());
                if copy_cursor < cursor {
                    output.extend_from_slice(&buf[copy_cursor..cursor]);
                }
                output
            });
            output.extend_from_slice(&buf[cursor + 1..line_total_end]);
            copy_cursor = line_total_end;
        }

        cursor = line_total_end;
        last_complete_end = cursor;
    }

    if allow_partial && last_complete_end > 0 {
        if let Some(ref mut output) = copied
            && copy_cursor < last_complete_end
        {
            output.extend_from_slice(&buf[copy_cursor..last_complete_end]);
        }
        return Some(MultilineScan {
            data_end: last_complete_end,
            terminator_len: 0,
            copied,
        });
    }

    None
}

struct TakenMultiline {
    data: BytesMut,
    terminator_len: usize,
}

fn take_multiline_output(src: &mut BytesMut, scan: MultilineScan) -> TakenMultiline {
    let data = if let Some(copied) = scan.copied {
        src.advance(scan.data_end + scan.terminator_len);
        copied
    } else {
        let data = src.split_to(scan.data_end);
        if scan.terminator_len > 0 {
            src.advance(scan.terminator_len);
        }
        data
    };

    TakenMultiline {
        data,
        terminator_len: scan.terminator_len,
    }
}

/// Perform NNTP dot-unstuffing on raw multi-line data.
///
/// Per RFC 3977 Section 3.1.1: if a line begins with a dot, the first dot is
/// removed (it was added by the server to avoid confusion with the terminator).
pub fn dot_unstuff(data: &[u8]) -> BytesMut {
    let mut i = 0usize;
    let mut line_start = true;
    let mut used_copy = false;
    let mut segment_start = 0usize;
    let mut segments = Vec::new();

    while i < data.len() {
        if line_start && data[i] == b'.' {
            used_copy = true;
            if segment_start < i {
                segments.push((segment_start, i));
            }
            segment_start = i + 1;
            line_start = false;
            i += 1;
            continue;
        }
        if data[i] == b'\n' {
            line_start = true;
        } else {
            line_start = false;
        }
        i += 1;
    }

    if !used_copy {
        return BytesMut::from(data);
    }

    if segment_start < data.len() {
        segments.push((segment_start, data.len()));
    }

    let mut output = BytesMut::with_capacity(data.len());
    for (start, end) in segments {
        output.extend_from_slice(&data[start..end]);
    }
    output
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
