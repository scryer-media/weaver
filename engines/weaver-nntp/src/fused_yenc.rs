use std::time::Duration;

use bytes::{Buf, BytesMut};
use thiserror::Error;
use weaver_yenc::{
    DecodeResult, DecodeState, RapidyencDecodeEnd, YencError, YencMetadata,
    decode_body_chunk_until_control, finish_streaming_result, header,
};

use crate::error::NntpError;
use crate::response::parse_response;
use crate::types::Response;

const MAX_CONTROL_LINE: usize = 16 * 1024;
const MAX_ARTICLE_RESERVE: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum FusedYencError {
    #[error(transparent)]
    Nntp(#[from] NntpError),
    #[error(transparent)]
    Yenc(#[from] YencError),
}

impl From<FusedYencError> for NntpError {
    fn from(err: FusedYencError) -> Self {
        match err {
            FusedYencError::Nntp(err) => err,
            FusedYencError::Yenc(err) => NntpError::MalformedResponse(err.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, FusedYencError>;

#[derive(Debug)]
pub struct FusedYencArticle {
    pub response: Response,
    pub chunks: Vec<Box<[u8]>>,
    pub result: DecodeResult,
    pub stats: FusedYencArticleStats,
}

impl FusedYencArticle {
    pub fn to_data(&self) -> Vec<u8> {
        let len = self.chunks.iter().map(|chunk| chunk.len()).sum();
        let mut data = Vec::with_capacity(len);
        for chunk in &self.chunks {
            data.extend_from_slice(chunk.as_ref());
        }
        data
    }

    pub fn into_data(self) -> Vec<u8> {
        let len = self.chunks.iter().map(|chunk| chunk.len()).sum();
        let mut data = Vec::with_capacity(len);
        for chunk in self.chunks {
            data.extend_from_slice(chunk.as_ref());
        }
        data
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FusedYencArticleStats {
    pub input_bytes_consumed: u64,
    pub encoded_bytes_consumed: u64,
    pub decoded_bytes_written: u64,
    pub crc_actual: u32,
    pub crc_expected: Option<u32>,
    pub yenc_size_expected: Option<u64>,
    pub yenc_size_actual: u64,
    pub read_calls: u64,
    pub read_bytes: u64,
    pub input_chunks: u64,
    pub decode_calls: u64,
    pub crc_update_calls: u64,
    pub output_batches: u64,
    pub yenc_control_hits: u64,
    pub nntp_terminator_hits: u64,
    pub nntp_terminator_bytes: u64,
    pub leftover_bytes_after_terminator: u64,
    pub buffer_compactions: u64,
    pub read_poll_cpu: Duration,
    pub fused_decode_cpu: Duration,
    pub output_callback_cpu: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FusedArticleState {
    ResponseLine,
    YencHeader,
    Body,
    YEndLine,
    NntpTerminator,
    Done,
}

/// In-memory prototype for a fused NNTP BODY + yEnc article decoder.
///
/// The decoder consumes bytes directly from a caller-owned `BytesMut` and leaves
/// any bytes after the NNTP multiline terminator untouched for the next
/// response.
#[derive(Debug)]
pub struct FusedYencArticleDecoder {
    state: FusedArticleState,
    response: Option<Response>,
    line_buf: Vec<u8>,
    metadata: Option<YencMetadata>,
    yend_line: Option<Vec<u8>>,
    decode_state: DecodeState,
    output: Vec<u8>,
    output_chunks: Vec<Box<[u8]>>,
    output_reserved: bool,
    stats: FusedYencArticleStats,
}

impl FusedYencArticleDecoder {
    pub fn new() -> Self {
        Self {
            state: FusedArticleState::ResponseLine,
            response: None,
            line_buf: Vec::with_capacity(256),
            metadata: None,
            yend_line: None,
            decode_state: DecodeState::new(),
            output: Vec::new(),
            output_chunks: Vec::new(),
            output_reserved: false,
            stats: FusedYencArticleStats::default(),
        }
    }

    pub fn from_body_response(response: Response) -> Result<Self> {
        let mut decoder = Self::new();
        decoder.accept_response(response)?;
        Ok(decoder)
    }

    pub fn decode_available(&mut self, src: &mut BytesMut) -> Result<Option<FusedYencArticle>> {
        self.stats.input_chunks += 1;

        loop {
            match self.state {
                FusedArticleState::ResponseLine => {
                    if !self.process_response_line(src)? {
                        return Ok(None);
                    }
                }
                FusedArticleState::YencHeader => {
                    if !self.process_yenc_header(src)? {
                        return Ok(None);
                    }
                }
                FusedArticleState::Body => {
                    if src.is_empty() {
                        return Ok(None);
                    }
                    if let Some(article) = self.process_body(src)? {
                        return Ok(Some(article));
                    }
                }
                FusedArticleState::YEndLine => {
                    if !self.process_yend_line(src)? {
                        return Ok(None);
                    }
                }
                FusedArticleState::NntpTerminator => {
                    if !self.process_nntp_terminator(src)? {
                        return Ok(None);
                    }
                    self.stats.leftover_bytes_after_terminator = src.len() as u64;
                    return self.finish_article().map(Some);
                }
                FusedArticleState::Done => return Ok(None),
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.state == FusedArticleState::Done
    }

    pub(crate) fn drain_output_chunks(&mut self) -> Vec<Box<[u8]>> {
        std::mem::take(&mut self.output_chunks)
    }

    fn process_response_line(&mut self, src: &mut BytesMut) -> Result<bool> {
        if !self.consume_line_into_buffer(src)? {
            return Ok(false);
        }

        let line = trim_line_ending(&self.line_buf);
        let line = std::str::from_utf8(line).map_err(|err| {
            NntpError::MalformedResponse(format!("invalid UTF-8 response line: {err}"))
        })?;
        let response = parse_response(line)?;
        self.accept_response(response)?;
        self.line_buf.clear();
        Ok(true)
    }

    fn accept_response(&mut self, response: Response) -> Result<()> {
        if response.code.raw() != 222 {
            let error = if response.code.is_error() {
                NntpError::from_status(response.code, &response.message)
            } else {
                NntpError::unexpected(response.code, response.message)
            };
            return Err(error.into());
        }

        self.response = Some(response);
        self.state = FusedArticleState::YencHeader;
        Ok(())
    }

    fn process_yenc_header(&mut self, src: &mut BytesMut) -> Result<bool> {
        if !self.consume_line_into_buffer(src)? {
            return Ok(false);
        }

        if let Some(metadata) = self.metadata.as_mut() {
            if metadata.part.is_none() || metadata.begin.is_some() || metadata.end.is_some() {
                return Err(YencError::InvalidHeader {
                    field: "=ypart".to_string(),
                    reason: "unexpected yEnc header line".to_string(),
                }
                .into());
            }
            header::apply_ypart_line(&self.line_buf, metadata)?;
            self.line_buf.clear();
            self.reserve_output_if_known();
            self.state = FusedArticleState::Body;
            return Ok(true);
        }

        let metadata = header::parse_ybegin_line(&self.line_buf)?;
        self.line_buf.clear();
        let needs_ypart = metadata.part.is_some();
        self.metadata = Some(metadata);
        if !needs_ypart {
            self.reserve_output_if_known();
            self.state = FusedArticleState::Body;
        }
        Ok(true)
    }

    fn process_body(&mut self, src: &mut BytesMut) -> Result<Option<FusedYencArticle>> {
        self.stats.decode_calls += 1;
        let progress =
            decode_body_chunk_until_control(&mut self.decode_state, src, &mut self.output)?;
        self.advance_src(src, progress.source_consumed);

        match progress.end {
            RapidyencDecodeEnd::None => Ok(None),
            RapidyencDecodeEnd::Control => {
                self.stats.yenc_control_hits += 1;
                self.line_buf.clear();
                self.line_buf.extend_from_slice(b"=y");
                self.state = FusedArticleState::YEndLine;
                Ok(None)
            }
            RapidyencDecodeEnd::Article => Err(NntpError::MalformedResponse(
                "NNTP terminator before yEnc trailer".to_string(),
            )
            .into()),
        }
    }

    fn process_yend_line(&mut self, src: &mut BytesMut) -> Result<bool> {
        if !self.consume_line_into_buffer(src)? {
            return Ok(false);
        }

        if self.line_buf.starts_with(b"=yend ") {
            self.yend_line = Some(std::mem::take(&mut self.line_buf));
            self.state = FusedArticleState::NntpTerminator;
            return Ok(true);
        }

        if self.line_buf.iter().all(|b| matches!(b, b'\r' | b'\n')) {
            self.line_buf.clear();
            return Ok(true);
        }

        Err(YencError::InvalidHeader {
            field: "=yend".to_string(),
            reason: "unexpected trailing line after yEnc body".to_string(),
        }
        .into())
    }

    fn process_nntp_terminator(&mut self, src: &mut BytesMut) -> Result<bool> {
        if !self.consume_line_into_buffer(src)? {
            return Ok(false);
        }

        if self.line_buf == b".\r\n" || self.line_buf == b".\n" {
            self.stats.nntp_terminator_hits += 1;
            self.stats.nntp_terminator_bytes += self.line_buf.len() as u64;
            self.line_buf.clear();
            return Ok(true);
        }

        Err(NntpError::MalformedMultilineTerminator.into())
    }

    fn finish_article(&mut self) -> Result<FusedYencArticle> {
        let response = self.response.take().ok_or_else(|| {
            NntpError::MalformedResponse("missing BODY response line".to_string())
        })?;
        let metadata = self.metadata.take().ok_or(YencError::MissingHeader)?;
        let yend_line = self.yend_line.take().ok_or(YencError::MissingTrailer)?;

        let yend = header::parse_yend_line(&yend_line)?;
        let crc_update_calls = self.decode_state.crc_update_calls;

        self.flush_output();

        let result =
            finish_streaming_result(metadata, Some(yend), std::mem::take(&mut self.decode_state))?;
        let chunks = std::mem::take(&mut self.output_chunks);

        let mut stats = self.stats.clone();
        stats.decoded_bytes_written = result.bytes_written as u64;
        stats.crc_actual = result.part_crc;
        stats.crc_expected = if result.metadata.part.is_some() {
            result.expected_part_crc
        } else {
            result.expected_file_crc
        };
        stats.yenc_size_expected = expected_decoded_size(&result.metadata);
        stats.yenc_size_actual = result.bytes_written as u64;
        stats.crc_update_calls = crc_update_calls;
        stats.output_batches = chunks.len() as u64;
        stats.input_bytes_consumed = stats.encoded_bytes_consumed;

        self.state = FusedArticleState::Done;

        Ok(FusedYencArticle {
            response,
            chunks,
            result,
            stats,
        })
    }

    fn consume_line_into_buffer(&mut self, src: &mut BytesMut) -> Result<bool> {
        if src.is_empty() {
            return Ok(false);
        }

        let Some(lf_index) = memchr::memchr(b'\n', src) else {
            self.line_buf.extend_from_slice(src);
            self.advance_src(src, src.len());
            self.check_control_line_len()?;
            return Ok(false);
        };

        let consumed = lf_index + 1;
        self.line_buf.extend_from_slice(&src[..consumed]);
        self.advance_src(src, consumed);
        self.check_control_line_len()?;
        Ok(true)
    }

    fn check_control_line_len(&self) -> Result<()> {
        if self.line_buf.len() > MAX_CONTROL_LINE {
            return Err(NntpError::MalformedResponse("control line too large".to_string()).into());
        }
        Ok(())
    }

    fn reserve_output_if_known(&mut self) {
        if self.output_reserved {
            return;
        }
        self.output_reserved = true;

        let Some(metadata) = self.metadata.as_ref() else {
            return;
        };
        let Some(expected) = expected_decoded_size(metadata) else {
            return;
        };
        let Ok(expected) = usize::try_from(expected) else {
            return;
        };
        if expected == 0 || expected > MAX_ARTICLE_RESERVE || expected <= self.output.capacity() {
            return;
        }

        self.output.reserve_exact(expected - self.output.capacity());
    }

    fn flush_output(&mut self) {
        if self.output.is_empty() {
            return;
        }
        self.output_chunks
            .push(std::mem::take(&mut self.output).into_boxed_slice());
    }

    fn advance_src(&mut self, src: &mut BytesMut, count: usize) {
        if count == 0 {
            return;
        }
        src.advance(count);
        self.stats.encoded_bytes_consumed += count as u64;
        self.stats.input_bytes_consumed = self.stats.encoded_bytes_consumed;
    }
}

impl Default for FusedYencArticleDecoder {
    fn default() -> Self {
        Self::new()
    }
}

fn trim_line_ending(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    if end > 0 && line[end - 1] == b'\n' {
        end -= 1;
        if end > 0 && line[end - 1] == b'\r' {
            end -= 1;
        }
    }
    &line[..end]
}

fn expected_decoded_size(metadata: &YencMetadata) -> Option<u64> {
    match (metadata.begin, metadata.end) {
        (Some(begin), Some(end)) if end >= begin => Some(end - begin + 1),
        (Some(_), Some(_)) => None,
        _ if metadata.part.is_none() => Some(metadata.size),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{NntpCodec, NntpFrame, StreamChunk};
    use tokio_util::codec::Decoder;
    use weaver_yenc::{DecodedArticle, StreamingArticleDecoder, encode, encode_part};

    fn transcript(article: &[u8], leftover: &[u8]) -> Vec<u8> {
        let mut bytes = b"222 <test@local> body follows\r\n".to_vec();
        bytes.extend_from_slice(article);
        bytes.extend_from_slice(b".\r\n");
        bytes.extend_from_slice(leftover);
        bytes
    }

    fn dot_stuff_lines(input: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(input.len());
        let mut at_line_start = true;
        for &byte in input {
            if at_line_start && byte == b'.' {
                output.push(b'.');
            }
            output.push(byte);
            at_line_start = byte == b'\n';
        }
        output
    }

    fn decode_current(transcript: &[u8]) -> (DecodedArticle, Vec<u8>) {
        let mut codec = NntpCodec::new();
        let mut src = BytesMut::from(transcript);

        match codec.decode(&mut src).unwrap().unwrap() {
            NntpFrame::Line(line) => {
                let response = parse_response(&line).unwrap();
                assert_eq!(response.code.raw(), 222);
            }
            other => panic!("expected BODY response line, got {other:?}"),
        }

        codec.set_streaming_multiline(true);
        codec.set_raw_multiline(true);

        let mut decoder = StreamingArticleDecoder::new();
        let mut output = Vec::new();
        loop {
            match codec.decode_streaming_raw_chunk(&mut src).unwrap().unwrap() {
                StreamChunk::Data(data) => decoder.feed_chunk(&data, &mut output).unwrap(),
                StreamChunk::End => break,
            }
        }

        (decoder.finish(output).unwrap(), src.to_vec())
    }

    fn decode_fused_with_chunks(
        transcript: &[u8],
        chunks: &[usize],
    ) -> (FusedYencArticle, Vec<u8>) {
        let mut decoder = FusedYencArticleDecoder::new();
        let mut src = BytesMut::new();
        let mut offset = 0;
        let mut article = None;

        for &chunk_len in chunks {
            let end = (offset + chunk_len).min(transcript.len());
            if end > offset {
                src.extend_from_slice(&transcript[offset..end]);
                offset = end;
            }

            if article.is_none() {
                article = decoder.decode_available(&mut src).unwrap();
            }
        }

        if offset < transcript.len() {
            src.extend_from_slice(&transcript[offset..]);
            if article.is_none() {
                article = decoder.decode_available(&mut src).unwrap();
            }
        }

        (article.expect("fused decoder did not finish"), src.to_vec())
    }

    fn assert_same_article(expected: &DecodedArticle, actual: &FusedYencArticle) {
        assert_eq!(actual.response.code.raw(), 222);
        let actual_data: Vec<u8> = actual
            .chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();
        assert_eq!(expected.data, actual_data);
        assert_eq!(expected.result.bytes_written, actual.result.bytes_written);
        assert_eq!(expected.result.part_crc, actual.result.part_crc);
        assert_eq!(
            expected.result.expected_part_crc,
            actual.result.expected_part_crc
        );
        assert_eq!(
            expected.result.expected_file_crc,
            actual.result.expected_file_crc
        );
        assert_eq!(expected.result.has_trailer, actual.result.has_trailer);

        let expected_meta = &expected.result.metadata;
        let actual_meta = &actual.result.metadata;
        assert_eq!(expected_meta.name, actual_meta.name);
        assert_eq!(expected_meta.size, actual_meta.size);
        assert_eq!(expected_meta.line_length, actual_meta.line_length);
        assert_eq!(expected_meta.part, actual_meta.part);
        assert_eq!(expected_meta.total, actual_meta.total);
        assert_eq!(expected_meta.begin, actual_meta.begin);
        assert_eq!(expected_meta.end, actual_meta.end);
    }

    #[test]
    fn fused_decodes_complete_transcript_and_leaves_pipelined_bytes() {
        let original = b"hello fused decoder";
        let mut article = Vec::new();
        encode(original, &mut article, 128, "test.bin").unwrap();

        let leftover = b"223 <next@local> article follows\r\n";
        let transcript = transcript(&article, leftover);
        let (expected, expected_leftover) = decode_current(&transcript);
        let (actual, actual_leftover) = decode_fused_with_chunks(&transcript, &[transcript.len()]);

        assert_same_article(&expected, &actual);
        assert_eq!(expected_leftover, leftover);
        assert_eq!(actual_leftover, leftover);
        assert_eq!(
            actual.stats.encoded_bytes_consumed as usize,
            transcript.len() - leftover.len()
        );
        assert_eq!(
            actual.stats.input_bytes_consumed,
            actual.stats.encoded_bytes_consumed
        );
        assert!(actual.stats.crc_update_calls > 0);
        assert_eq!(actual.stats.nntp_terminator_hits, 1);
        assert_eq!(actual.stats.nntp_terminator_bytes, b".\r\n".len() as u64);
        assert_eq!(
            actual.stats.leftover_bytes_after_terminator,
            leftover.len() as u64
        );
    }

    #[test]
    fn fused_matches_current_path_for_every_single_split_point() {
        let original = b"\x04AB=\r\n\x04CD yEnc split edges";
        let mut article = Vec::new();
        encode(original, &mut article, 8, "split.bin").unwrap();
        let article = dot_stuff_lines(&article);

        let leftover = b"223 next response\r\n";
        let transcript = transcript(&article, leftover);
        let (expected, expected_leftover) = decode_current(&transcript);

        for split in 0..=transcript.len() {
            let (actual, actual_leftover) =
                decode_fused_with_chunks(&transcript, &[split, transcript.len() - split]);
            assert_same_article(&expected, &actual);
            assert_eq!(expected_leftover, actual_leftover, "split at {split}");
        }
    }

    #[test]
    fn fused_matches_current_path_for_one_byte_chunks() {
        let original = b"\x04one byte chunks with = escapes \0\r\n";
        let mut article = Vec::new();
        encode(original, &mut article, 6, "bytes.bin").unwrap();
        let article = dot_stuff_lines(&article);

        let leftover = b"224 overview follows\r\n";
        let transcript = transcript(&article, leftover);
        let chunks = vec![1; transcript.len()];
        let (expected, expected_leftover) = decode_current(&transcript);
        let (actual, actual_leftover) = decode_fused_with_chunks(&transcript, &chunks);

        assert_same_article(&expected, &actual);
        assert_eq!(expected_leftover, actual_leftover);
    }

    #[test]
    fn fused_matches_current_path_for_large_chunk_patterns() {
        let mut original = Vec::with_capacity(384 * 1024);
        const ESCAPE_HEAVY: [u8; 7] = [214, 224, 227, 19, b'A', b'B', b'C'];
        for idx in 0..(384 * 1024) {
            if idx % 128 == 0 {
                original.push(4);
            } else {
                original.push(ESCAPE_HEAVY[idx % ESCAPE_HEAVY.len()]);
            }
        }

        let mut article = Vec::new();
        encode(&original, &mut article, 128, "large.bin").unwrap();
        let article = dot_stuff_lines(&article);

        let leftover = b"223 <large-next@local> article follows\r\n";
        let transcript = transcript(&article, leftover);
        let (expected, expected_leftover) = decode_current(&transcript);

        for chunk_len in [2usize, 3, 257, 4 * 1024, 64 * 1024, 256 * 1024] {
            let chunks = vec![chunk_len; transcript.len().div_ceil(chunk_len)];
            let (actual, actual_leftover) = decode_fused_with_chunks(&transcript, &chunks);
            assert_same_article(&expected, &actual);
            assert_eq!(
                expected_leftover, actual_leftover,
                "chunk length {chunk_len}"
            );
        }
    }

    #[test]
    fn fused_matches_current_path_for_multipart_article() {
        let original = b"multipart fused body";
        let mut article = Vec::new();
        encode_part(
            original,
            &mut article,
            128,
            "part.bin",
            1,
            2,
            1,
            original.len() as u64,
            1024,
        )
        .unwrap();

        let leftover = b"221 head follows\r\n";
        let transcript = transcript(&article, leftover);
        let (expected, expected_leftover) = decode_current(&transcript);
        let (actual, actual_leftover) = decode_fused_with_chunks(&transcript, &[3, 5, 7, 11, 13]);

        assert_same_article(&expected, &actual);
        assert_eq!(expected_leftover, actual_leftover);
    }

    #[test]
    fn fused_rejects_non_body_response() {
        let mut src = BytesMut::from("430 no such article\r\n".as_bytes());
        let mut decoder = FusedYencArticleDecoder::new();
        let err = decoder.decode_available(&mut src).unwrap_err();

        assert!(matches!(
            err,
            FusedYencError::Nntp(NntpError::ArticleNotFound)
        ));
    }

    #[test]
    fn fused_rejects_malformed_nntp_terminator() {
        let original = b"bad terminator";
        let mut article = Vec::new();
        encode(original, &mut article, 128, "bad.bin").unwrap();

        let mut bytes = b"222 <test@local> body follows\r\n".to_vec();
        bytes.extend_from_slice(&article);
        bytes.extend_from_slice(b"..\r\n");

        let mut src = BytesMut::from(bytes.as_slice());
        let mut decoder = FusedYencArticleDecoder::new();
        let err = decoder.decode_available(&mut src).unwrap_err();

        assert!(matches!(
            err,
            FusedYencError::Nntp(NntpError::MalformedMultilineTerminator)
        ));
    }
}
