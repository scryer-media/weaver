use std::time::Duration;

use bytes::{Buf, BytesMut};
use thiserror::Error;
use weaver_yenc::{
    DecodeResult, DecodeState, RapidyencDecodeEnd, YencError, YencMetadata,
    decode_body_chunk_until_control, finish_streaming_result, header,
};

use crate::error::NntpError;
use crate::response::parse_response;
use crate::tls::TransportReadStats;
use crate::types::Response;

const MAX_CONTROL_LINE: usize = 16 * 1024;
const MAX_ARTICLE_RESERVE: usize = 16 * 1024 * 1024;
const OUTPUT_BATCH_TARGET: usize = 512 * 1024;
/// How many bytes of leading junk may precede `=ybegin` before the article is
/// declared header-less. Bounded so a 222 response whose body never contains a
/// yEnc header cannot make the header scan run for the whole article.
const MAX_HEADER_SCAN_BYTES: usize = 64 * 1024;

#[cfg(unix)]
fn thread_cpu_time() -> Option<Duration> {
    let mut timespec = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, timespec.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let timespec = unsafe { timespec.assume_init() };
    let seconds = u64::try_from(timespec.tv_sec).ok()?;
    let nanos = u32::try_from(timespec.tv_nsec).ok()?.min(999_999_999);
    Some(Duration::new(seconds, nanos))
}

#[cfg(windows)]
fn thread_cpu_time() -> Option<Duration> {
    use windows_sys::Win32::System::Threading::{GetCurrentThread, GetThreadTimes};

    const ZERO: windows_sys::Win32::Foundation::FILETIME =
        windows_sys::Win32::Foundation::FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
    let mut times = [ZERO; 4];
    let [creation, exit, kernel, user] = &mut times;
    // SAFETY: the pseudo-handle is always valid for the current thread and
    // all four out-pointers reference live FILETIME slots.
    let rc = unsafe { GetThreadTimes(GetCurrentThread(), creation, exit, kernel, user) };
    if rc == 0 {
        return None;
    }
    // FILETIME counts 100 ns ticks; kernel+user matches the unix
    // CLOCK_THREAD_CPUTIME_ID semantics. Granularity is the scheduler tick
    // (~15.6 ms), which is fine for the aggregated deltas reported here.
    let ticks = |filetime: windows_sys::Win32::Foundation::FILETIME| {
        ((filetime.dwHighDateTime as u64) << 32) | filetime.dwLowDateTime as u64
    };
    let total = ticks(times[2]).saturating_add(ticks(times[3]));
    Some(Duration::from_nanos(total.saturating_mul(100)))
}

#[cfg(not(any(unix, windows)))]
fn thread_cpu_time() -> Option<Duration> {
    None
}

fn add_cpu_delta(total: &mut Duration, started: Option<Duration>) {
    let Some(started) = started else {
        return;
    };
    let Some(current) = thread_cpu_time() else {
        return;
    };
    if let Some(delta) = current.checked_sub(started) {
        *total += delta;
    }
}

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
    pub transport_read: TransportReadStats,
    pub read_poll_cpu: Duration,
    pub fused_decode_cpu: Duration,
    pub response_line_cpu: Duration,
    pub yenc_header_cpu: Duration,
    pub body_decode_cpu: Duration,
    pub yend_line_cpu: Duration,
    pub nntp_terminator_cpu: Duration,
    pub article_finish_cpu: Duration,
    pub output_callback_cpu: Duration,
    /// Deliberate shared server-rate wait, excluded from RTT/timeout metrics.
    pub throttle_wait: Duration,
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
    /// Bytes of non-`=ybegin` lines skipped while scanning for the yEnc header.
    junk_before_ybegin_bytes: usize,
    yend_line: Option<Vec<u8>>,
    decode_state: DecodeState,
    output: Vec<u8>,
    output_chunks: Vec<Box<[u8]>>,
    output_reserved: bool,
    profile_cpu: bool,
    stats: FusedYencArticleStats,
}

impl FusedYencArticleDecoder {
    pub fn new() -> Self {
        Self {
            state: FusedArticleState::ResponseLine,
            response: None,
            line_buf: Vec::with_capacity(256),
            metadata: None,
            junk_before_ybegin_bytes: 0,
            yend_line: None,
            decode_state: DecodeState::new(),
            output: Vec::new(),
            output_chunks: Vec::new(),
            output_reserved: false,
            profile_cpu: false,
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
                    let cpu_started = self.phase_cpu_started();
                    let result = self.process_response_line(src);
                    add_cpu_delta(&mut self.stats.response_line_cpu, cpu_started);
                    if !result? {
                        return Ok(None);
                    }
                }
                FusedArticleState::YencHeader => {
                    let cpu_started = self.phase_cpu_started();
                    let result = self.process_yenc_header(src);
                    add_cpu_delta(&mut self.stats.yenc_header_cpu, cpu_started);
                    if !result? {
                        return Ok(None);
                    }
                }
                FusedArticleState::Body => {
                    if src.is_empty() {
                        return Ok(None);
                    }
                    let cpu_started = self.phase_cpu_started();
                    let result = self.process_body(src);
                    add_cpu_delta(&mut self.stats.body_decode_cpu, cpu_started);
                    if let Some(article) = result? {
                        return Ok(Some(article));
                    }
                }
                FusedArticleState::YEndLine => {
                    let cpu_started = self.phase_cpu_started();
                    let result = self.process_yend_line(src);
                    add_cpu_delta(&mut self.stats.yend_line_cpu, cpu_started);
                    if !result? {
                        return Ok(None);
                    }
                }
                FusedArticleState::NntpTerminator => {
                    let cpu_started = self.phase_cpu_started();
                    let result = self.process_nntp_terminator(src);
                    add_cpu_delta(&mut self.stats.nntp_terminator_cpu, cpu_started);
                    if !result? {
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

    pub fn set_profile_cpu(&mut self, enabled: bool) {
        self.profile_cpu = enabled;
    }

    /// Raw NNTP BODY payload consumed so far, excluding the multiline
    /// terminator. Available even when the next decode call returns an error,
    /// which lets transfer accounting retain partial failed bodies.
    pub(crate) fn body_payload_bytes_consumed(&self) -> u64 {
        self.stats
            .encoded_bytes_consumed
            .saturating_sub(self.stats.nntp_terminator_bytes)
    }

    pub(crate) fn drain_output_chunks(&mut self) -> Vec<Box<[u8]>> {
        std::mem::take(&mut self.output_chunks)
    }

    fn phase_cpu_started(&self) -> Option<Duration> {
        self.profile_cpu.then(thread_cpu_time).flatten()
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

        // SABnzbd and nzbget scan the body for `=ybegin` instead of demanding it
        // on the first line, so leading junk (stray headers, banners, blank
        // lines) does not kill an otherwise decodable article. Skip whole lines
        // until one of them is a real `=ybegin` control line.
        if !header::is_control_line(&self.line_buf, b"=ybegin") {
            // An NNTP multiline terminator ends the article: there is no
            // `=ybegin` to find, so fail now instead of waiting forever for a
            // header that will never arrive.
            if self.line_buf == b".\r\n" || self.line_buf == b".\n" {
                return Err(YencError::MissingHeader.into());
            }
            self.junk_before_ybegin_bytes = self
                .junk_before_ybegin_bytes
                .saturating_add(self.line_buf.len());
            if self.junk_before_ybegin_bytes > MAX_HEADER_SCAN_BYTES {
                return Err(YencError::MissingHeader.into());
            }
            self.line_buf.clear();
            return Ok(true);
        }

        let mut metadata = header::parse_ybegin_line(&self.line_buf)?;
        metadata.defects.junk_before_ybegin = self.junk_before_ybegin_bytes > 0;
        self.line_buf.clear();
        let needs_ypart = metadata.part.is_some();
        self.decode_state
            .set_line_length_hint(Some(metadata.line_length));
        self.metadata = Some(metadata);
        if !needs_ypart {
            self.reserve_output_if_known();
            self.state = FusedArticleState::Body;
        }
        Ok(true)
    }

    fn process_body(&mut self, src: &mut BytesMut) -> Result<Option<FusedYencArticle>> {
        self.flush_ready_output();
        let input_len = self.next_body_input_len(src.len());
        if input_len == 0 {
            return Ok(None);
        }

        self.stats.decode_calls += 1;
        let progress = decode_body_chunk_until_control(
            &mut self.decode_state,
            &src[..input_len],
            &mut self.output,
        )?;
        self.advance_src(src, progress.source_consumed);
        self.flush_ready_output();

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

        if header::is_control_line(&self.line_buf, b"=yend") {
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
        loop {
            if !self.consume_line_into_buffer(src)? {
                return Ok(false);
            }

            if self.line_buf == b".\r\n" || self.line_buf == b".\n" {
                self.stats.nntp_terminator_hits += 1;
                self.stats.nntp_terminator_bytes += self.line_buf.len() as u64;
                self.line_buf.clear();
                return Ok(true);
            }
            // Some providers emit a blank line after the yEnc trailer before
            // the NNTP dot terminator. It carries no article data and is safe
            // to ignore while remaining strict about every non-blank line.
            if self.line_buf.iter().all(|b| matches!(b, b'\r' | b'\n')) {
                self.line_buf.clear();
                continue;
            }

            return Err(NntpError::MalformedMultilineTerminator.into());
        }
    }

    fn finish_article(&mut self) -> Result<FusedYencArticle> {
        let cpu_started = self.phase_cpu_started();
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
        add_cpu_delta(&mut stats.article_finish_cpu, cpu_started);

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
        // Clamp, do not bail: an article whose declared size exceeds the cap is
        // exactly the one that most needs a head start, and skipping the
        // reservation entirely left it growing from zero one doubling at a
        // time. The cap bounds a lying header's damage; it is not a reason to
        // reserve nothing.
        let reserve = expected.min(MAX_ARTICLE_RESERVE).min(OUTPUT_BATCH_TARGET);
        if reserve == 0 || reserve <= self.output.capacity() {
            return;
        }

        self.output.reserve_exact(reserve - self.output.capacity());
    }

    fn flush_output(&mut self) {
        if self.output.is_empty() {
            return;
        }
        self.output_chunks
            .push(std::mem::take(&mut self.output).into_boxed_slice());
    }

    fn flush_ready_output(&mut self) {
        while self.output.len() >= OUTPUT_BATCH_TARGET {
            let full_batch = if self.output.len() == OUTPUT_BATCH_TARGET {
                let next_capacity = self.next_output_capacity();
                std::mem::replace(&mut self.output, Vec::with_capacity(next_capacity))
            } else {
                let remainder = self.output.split_off(OUTPUT_BATCH_TARGET);
                std::mem::replace(&mut self.output, remainder)
            };
            self.output_chunks.push(full_batch.into_boxed_slice());
        }
    }

    fn next_body_input_len(&self, available: usize) -> usize {
        available.min(OUTPUT_BATCH_TARGET.saturating_sub(self.output.len()))
    }

    fn next_output_capacity(&self) -> usize {
        let Some(metadata) = self.metadata.as_ref() else {
            return OUTPUT_BATCH_TARGET;
        };
        let Some(expected) = expected_decoded_size(metadata) else {
            return OUTPUT_BATCH_TARGET;
        };
        let Ok(expected) = usize::try_from(expected) else {
            return OUTPUT_BATCH_TARGET;
        };
        let decoded = usize::try_from(self.decode_state.bytes_decoded).unwrap_or(usize::MAX);
        expected.saturating_sub(decoded).min(OUTPUT_BATCH_TARGET)
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
        // A single-part article's `=ybegin size=` is the decoded length -- but
        // only when the poster actually declared one. The `0` placeholder for a
        // missing or mangled `size=` means "unknown", not "empty article".
        _ if metadata.part.is_none()
            && !metadata.defects.missing_size
            && !metadata.defects.invalid_size =>
        {
            Some(metadata.size)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{NntpCodec, NntpFrame, StreamChunk};
    use tokio_util::codec::Decoder;
    use weaver_yenc::{
        CrcVerification, DecodedArticle, StreamingArticleDecoder, encode, encode_part,
    };

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
        while let StreamChunk::Data(data) =
            codec.decode_streaming_raw_chunk(&mut src).unwrap().unwrap()
        {
            decoder.feed_chunk(&data, &mut output).unwrap();
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
        assert_eq!(expected.result.crc_status, actual.result.crc_status);
        assert_eq!(expected.result.defects, actual.result.defects);

        let expected_meta = &expected.result.metadata;
        let actual_meta = &actual.result.metadata;
        assert_eq!(expected_meta.name, actual_meta.name);
        assert_eq!(expected_meta.size, actual_meta.size);
        assert_eq!(expected_meta.line_length, actual_meta.line_length);
        assert_eq!(expected_meta.part, actual_meta.part);
        assert_eq!(expected_meta.total, actual_meta.total);
        assert_eq!(expected_meta.begin, actual_meta.begin);
        assert_eq!(expected_meta.end, actual_meta.end);
        assert_eq!(expected_meta.defects, actual_meta.defects);
    }

    /// Body of the split-point acceptance guard: the fused decoder must agree
    /// with the streaming path, and must leave the pipelined bytes after the
    /// NNTP terminator untouched, for *every* chunk boundary in the transcript.
    fn assert_fused_matches_at_every_split_point(article: &[u8], leftover: &[u8]) {
        let transcript = transcript(article, leftover);
        let (expected, expected_leftover) = decode_current(&transcript);
        assert_eq!(expected_leftover, leftover);

        for split in 0..=transcript.len() {
            let (actual, actual_leftover) =
                decode_fused_with_chunks(&transcript, &[split, transcript.len() - split]);
            assert_same_article(&expected, &actual);
            assert_eq!(expected_leftover, actual_leftover, "split at {split}");
        }
    }

    /// Payload used by the broken-poster corpus: escape-heavy, dot-stuffable,
    /// and long enough to straddle several encoded lines.
    const BROKEN_POSTER_BODY: &[u8] = b"\x04broken poster body = with escapes\r\n\0\x04";

    /// Build a single-part article with caller-supplied `=ybegin`/`=yend` lines
    /// wrapped around a genuinely valid encoded body, so header damage is the
    /// only variable under test. `yend` receives the true decoded size and CRC.
    fn broken_poster_article(
        prologue: &[u8],
        ybegin: &[u8],
        yend: impl FnOnce(u64, u32) -> Vec<u8>,
    ) -> Vec<u8> {
        let mut encoded = Vec::new();
        encode(BROKEN_POSTER_BODY, &mut encoded, 16, "ignored.bin").unwrap();

        // Keep only the encoded payload between the generated header/trailer.
        let body_start = encoded
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("generated article has a header line")
            + 2;
        let body_end = encoded
            .windows(b"\r\n=yend ".len())
            .rposition(|window| window == b"\r\n=yend ")
            .expect("generated article has a trailer")
            + 2;
        let body = encoded[body_start..body_end].to_vec();

        let mut crc = weaver_yenc::crc::Crc32::new();
        crc.update(BROKEN_POSTER_BODY);

        let mut article = Vec::new();
        article.extend_from_slice(prologue);
        article.extend_from_slice(ybegin);
        article.extend_from_slice(&body);
        article.extend_from_slice(&yend(BROKEN_POSTER_BODY.len() as u64, crc.finalize()));
        dot_stuff_lines(&article)
    }

    /// The well-formed `=yend` for [`broken_poster_article`].
    fn healthy_yend(size: u64, crc: u32) -> Vec<u8> {
        format!("=yend size={size} crc32={crc:08x}\r\n").into_bytes()
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
    fn fused_flushes_decoded_batches_before_article_finish() {
        let mut original = Vec::with_capacity(OUTPUT_BATCH_TARGET + 123);
        for idx in 0..(OUTPUT_BATCH_TARGET + 123) {
            original.push((idx % 251) as u8);
        }

        let mut article = Vec::new();
        encode(&original, &mut article, 128, "batch.bin").unwrap();
        let yend_offset = article
            .windows(b"=yend ".len())
            .position(|window| window == b"=yend ")
            .expect("encoded article has yend trailer");

        let mut prefix = b"222 <test@local> body follows\r\n".to_vec();
        prefix.extend_from_slice(&article[..yend_offset]);

        let mut src = BytesMut::from(prefix.as_slice());
        let mut decoder = FusedYencArticleDecoder::new();
        assert!(decoder.decode_available(&mut src).unwrap().is_none());

        let chunks = decoder.drain_output_chunks();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), OUTPUT_BATCH_TARGET);
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

    // ── Broken-poster corpus, verified at every split point ──────────────
    //
    // Each of these decodes in SABnzbd/nzbget and used to hard-fail in the
    // fused path. The every-split-point sweep is the acceptance guard: the
    // fused decoder must agree with the streaming path *and* leave the next
    // pipelined response's bytes in `src` no matter where the chunk boundary
    // lands.

    /// D6: `=ybegin` is scanned for, not required on the first body line --
    /// including junk that contains `=` and partial `=yb` prefixes.
    #[test]
    fn fused_tolerates_leading_junk_at_every_split_point() {
        let prologue = b"Subject: leftover = header\r\n\
                         =yb\r\n\
                         =ybegi partial\r\n\
                         =ybeginner notes\r\n\
                         \r\n";
        let article = broken_poster_article(
            prologue,
            b"=ybegin line=16 size=38 name=junk.bin\r\n",
            healthy_yend,
        );

        assert_fused_matches_at_every_split_point(&article, b"223 <next@local> follows\r\n");

        let (decoded, _) = decode_fused_with_chunks(
            &transcript(&article, b"223 <next@local> follows\r\n"),
            &[usize::MAX],
        );
        assert_eq!(decoded.to_data(), BROKEN_POSTER_BODY);
        assert!(decoded.result.defects.junk_before_ybegin);
        assert_eq!(decoded.result.crc_status, CrcVerification::Verified);
    }

    /// D7: `line=`/`size=`/`name=` are all optional -- reference decoders do
    /// not even parse `line=`. Every combination the references tolerate.
    #[test]
    fn fused_tolerates_every_missing_ybegin_field_combination_at_every_split_point() {
        for line_field in [None, Some("line=16")] {
            for size_field in [None, Some("size=38")] {
                for name_field in [None, Some("name=missing.bin")] {
                    let mut ybegin = String::from("=ybegin");
                    for field in [line_field, size_field, name_field].into_iter().flatten() {
                        ybegin.push(' ');
                        ybegin.push_str(field);
                    }
                    ybegin.push_str(" \r\n");

                    let article = broken_poster_article(b"", ybegin.as_bytes(), |size, crc| {
                        format!("=yend size={size} crc32={crc:08x}\r\n").into_bytes()
                    });
                    assert_fused_matches_at_every_split_point(&article, b"223 next\r\n");
                }
            }
        }
    }

    /// D7 continued: a `=ybegin` whose numeric fields are unparseable degrades
    /// exactly like one that omitted them.
    #[test]
    fn fused_tolerates_unparseable_ybegin_numbers_at_every_split_point() {
        let article = broken_poster_article(
            b"",
            b"=ybegin line=abc size=-1000 name=neg.bin\r\n",
            healthy_yend,
        );
        assert_fused_matches_at_every_split_point(&article, b"223 next\r\n");
    }

    /// D8: `=ypart end=` past the `=ybegin size=` file size. The part length
    /// (end - begin + 1) is still checked against the decoded byte count.
    #[test]
    fn fused_tolerates_ypart_end_past_declared_size_at_every_split_point() {
        let size = BROKEN_POSTER_BODY.len() as u64;
        let article = broken_poster_article(
            b"",
            // size=10 is far below end=38: a classic broken multi-part poster.
            format!("=ybegin part=1 total=2 line=16 size=10 name=over.bin\r\n=ypart begin=1 end={size}\r\n")
                .as_bytes(),
            |size, crc| format!("=yend size={size} part=1 pcrc32={crc:08x}\r\n").into_bytes(),
        );

        assert_fused_matches_at_every_split_point(&article, b"221 head\r\n");

        let (decoded, _) =
            decode_fused_with_chunks(&transcript(&article, b"221 head\r\n"), &[usize::MAX]);
        assert!(decoded.result.defects.ypart_end_exceeds_size);
        assert_eq!(decoded.to_data(), BROKEN_POSTER_BODY);
    }

    /// The healthy direction (`end` well inside `size`) stays defect-free.
    #[test]
    fn fused_multipart_end_below_declared_size_is_clean_at_every_split_point() {
        let size = BROKEN_POSTER_BODY.len() as u64;
        let article = broken_poster_article(
            b"",
            format!(
                "=ybegin part=1 total=2 line=16 size=100000 name=under.bin\r\n=ypart begin=1 end={size}\r\n"
            )
            .as_bytes(),
            |size, crc| format!("=yend size={size} part=1 pcrc32={crc:08x}\r\n").into_bytes(),
        );

        assert_fused_matches_at_every_split_point(&article, b"221 head\r\n");

        let (decoded, _) =
            decode_fused_with_chunks(&transcript(&article, b"221 head\r\n"), &[usize::MAX]);
        assert!(!decoded.result.defects.any());
        assert_eq!(decoded.result.crc_status, CrcVerification::Verified);
    }

    /// D9: a mangled `crc32=` is treated as absent, leaving the article decoded
    /// but explicitly *unverified* -- never silently "valid".
    #[test]
    fn fused_tolerates_garbage_crc_at_every_split_point() {
        for garbage in ["nothex", "", "DEADBEEFDEADBEEF0", "1234ZZZZ", "0x1234"] {
            let article = broken_poster_article(
                b"",
                b"=ybegin line=16 size=38 name=badcrc.bin\r\n",
                |size, _crc| format!("=yend size={size} crc32={garbage}\r\n").into_bytes(),
            );

            assert_fused_matches_at_every_split_point(&article, b"223 next\r\n");

            let (decoded, _) =
                decode_fused_with_chunks(&transcript(&article, b"223 next\r\n"), &[usize::MAX]);
            assert_eq!(decoded.to_data(), BROKEN_POSTER_BODY, "crc32={garbage:?}");
            assert!(decoded.result.defects.invalid_crc32, "crc32={garbage:?}");
            assert_eq!(
                decoded.result.crc_status,
                CrcVerification::Unverified,
                "crc32={garbage:?} must not read as verified"
            );
            assert_eq!(decoded.result.expected_file_crc, None);
        }
    }

    /// An over-long but parseable CRC keeps its low 32 bits, as sabctools does
    /// deliberately for posters that emit wide hashes -- and still verifies.
    #[test]
    fn fused_truncates_over_long_crc_and_still_verifies() {
        let article = broken_poster_article(
            b"",
            b"=ybegin line=16 size=38 name=widecrc.bin\r\n",
            |size, crc| format!("=yend size={size} crc32=AAAAAAAA{crc:08x}\r\n").into_bytes(),
        );

        assert_fused_matches_at_every_split_point(&article, b"223 next\r\n");

        let (decoded, _) =
            decode_fused_with_chunks(&transcript(&article, b"223 next\r\n"), &[usize::MAX]);
        assert_eq!(decoded.result.crc_status, CrcVerification::Verified);
        assert!(!decoded.result.defects.invalid_crc32);
    }

    /// D10: one byte-wise parser behind every entry point, so tab separators
    /// and mixed-case field names behave identically in the fused path.
    #[test]
    fn fused_tolerates_tab_and_case_field_variants_at_every_split_point() {
        for ybegin in [
            b"=ybegin\tline=16\tsize=38\tname=tabs.bin\r\n".as_slice(),
            b"=ybegin LINE=16 Size=38 NaMe=case.bin\r\n".as_slice(),
            b"=ybegin  line=16   size=38   name=spaces.bin\r\n".as_slice(),
        ] {
            let article = broken_poster_article(b"", ybegin, healthy_yend);
            assert_fused_matches_at_every_split_point(&article, b"223 next\r\n");
        }
    }

    /// A 222 body that never contains `=ybegin` must fail cleanly at the NNTP
    /// terminator instead of waiting forever for a header that is not coming.
    #[test]
    fn fused_reports_missing_header_when_body_has_no_ybegin() {
        let mut bytes = b"222 <test@local> body follows\r\n".to_vec();
        bytes.extend_from_slice(b"not a yenc article at all\r\n");
        bytes.extend_from_slice(b"=yb still not one\r\n");
        bytes.extend_from_slice(b".\r\n");

        let mut src = BytesMut::from(bytes.as_slice());
        let mut decoder = FusedYencArticleDecoder::new();
        let err = decoder.decode_available(&mut src).unwrap_err();

        assert!(matches!(
            err,
            FusedYencError::Yenc(YencError::MissingHeader)
        ));
    }

    // ── B7: the fused path shares the kernel's stop rule ─────────────────

    /// Drive the fused decoder to completion and fail if it does not reject the
    /// transcript, at every split point.
    fn assert_fused_rejects_at_every_split_point(article: &[u8], expected_reason: &str) {
        let transcript = transcript(article, b"223 next\r\n");

        for split in 0..=transcript.len() {
            let mut decoder = FusedYencArticleDecoder::new();
            let mut src = BytesMut::new();
            let mut error = None;

            for chunk in [&transcript[..split], &transcript[split..]] {
                src.extend_from_slice(chunk);
                match decoder.decode_available(&mut src) {
                    Ok(Some(_)) => panic!("split {split}: fused decoder accepted the article"),
                    Ok(None) => {}
                    Err(err) => {
                        error = Some(err);
                        break;
                    }
                }
            }

            let error = error
                .unwrap_or_else(|| panic!("split {split}: fused decoder accepted the article"));
            assert!(
                error.to_string().contains(expected_reason),
                "split {split}: error {error} does not mention {expected_reason:?}"
            );
        }
    }

    /// A `\r\n=y…` line that is not `=yend`: the kernel stops there, so the
    /// article cannot decode past it in any entry point.
    #[test]
    fn fused_rejects_stray_control_line_in_body_at_every_split_point() {
        for stray in [
            b"=yfoo\r\n".as_slice(),
            b"=yend\r\n".as_slice(),
            b"=y\r\n".as_slice(),
        ] {
            let base = broken_poster_article(
                b"",
                b"=ybegin line=16 size=38 name=stray.bin\r\n",
                healthy_yend,
            );
            let yend_at = base
                .windows(b"=yend ".len())
                .rposition(|window| window == b"=yend ")
                .expect("article has a trailer");

            let mut article = base[..yend_at].to_vec();
            article.extend_from_slice(stray);
            article.extend_from_slice(&base[yend_at..]);

            assert_fused_rejects_at_every_split_point(
                &article,
                "unexpected trailing line after yEnc body",
            );
        }
    }

    /// A dot-stuffed trailer (`\r\n.=yend `): the kernel strips the one leading
    /// `.` at line start, so this really is the trailer.
    #[test]
    fn fused_finds_dot_prefixed_trailer_at_every_split_point() {
        let base = broken_poster_article(
            b"",
            b"=ybegin line=16 size=38 name=dotyend.bin\r\n",
            healthy_yend,
        );
        let yend_at = base
            .windows(b"=yend ".len())
            .rposition(|window| window == b"=yend ")
            .expect("article has a trailer");

        let mut article = base[..yend_at].to_vec();
        article.push(b'.');
        article.extend_from_slice(&base[yend_at..]);

        assert_fused_matches_at_every_split_point(&article, b"223 next\r\n");

        let (decoded, _) =
            decode_fused_with_chunks(&transcript(&article, b"223 next\r\n"), &[usize::MAX]);
        assert_eq!(decoded.to_data(), BROKEN_POSTER_BODY);
        assert!(decoded.result.has_trailer);
        assert_eq!(decoded.result.crc_status, CrcVerification::Verified);
    }

    // ── E19: pre-reservation for oversized articles ──────────────────────

    /// An article whose declared size exceeds the 16 MiB cap used to get *no*
    /// pre-reservation at all. It must reserve a batch and grow from there.
    #[test]
    fn fused_reserves_output_for_articles_larger_than_the_cap() {
        for size in [OUTPUT_BATCH_TARGET as u64, MAX_ARTICLE_RESERVE as u64 * 2] {
            let mut bytes = b"222 <test@local> body follows\r\n".to_vec();
            bytes.extend_from_slice(
                format!("=ybegin line=128 size={size} name=huge.bin\r\n").as_bytes(),
            );

            let mut src = BytesMut::from(bytes.as_slice());
            let mut decoder = FusedYencArticleDecoder::new();
            assert!(decoder.decode_available(&mut src).unwrap().is_none());
            assert_eq!(
                decoder.output.capacity(),
                OUTPUT_BATCH_TARGET,
                "size={size} reserved {}",
                decoder.output.capacity()
            );
        }
    }

    // ── E12: decoded batches are delivered as they are produced ──────────

    /// The decoder's batches are its streaming contract: draining them as they
    /// appear must reproduce the buffered article exactly, in order, wherever
    /// the chunk boundary lands.
    #[test]
    fn fused_batches_drain_in_order_at_every_split_point() {
        let original = b"\x04batched body with = escapes \0\r\n";
        let mut article = Vec::new();
        encode(original, &mut article, 8, "batches.bin").unwrap();
        let article = dot_stuff_lines(&article);
        let transcript = transcript(&article, b"223 next\r\n");

        for split in 0..=transcript.len() {
            let mut decoder = FusedYencArticleDecoder::new();
            let mut src = BytesMut::new();
            let mut delivered: Vec<Box<[u8]>> = Vec::new();
            let mut article = None;

            for chunk in [&transcript[..split], &transcript[split..]] {
                src.extend_from_slice(chunk);
                if article.is_none() {
                    article = decoder.decode_available(&mut src).unwrap();
                    // Whatever the decoder finished with belongs at the end of
                    // the delivered sequence, after everything drained earlier.
                    match article.as_mut() {
                        Some(article) => delivered.extend(std::mem::take(&mut article.chunks)),
                        None => delivered.extend(decoder.drain_output_chunks()),
                    }
                }
            }

            let mut article = article.expect("fused decoder did not finish");
            let streamed: Vec<u8> = delivered.iter().flat_map(|c| c.iter().copied()).collect();
            article.chunks = delivered;
            assert_eq!(streamed, original.as_slice(), "split {split}");
            assert_eq!(article.to_data(), streamed, "split {split}");
        }
    }

    /// The same, for an article large enough to cross the batch target: the
    /// batches really do arrive before the article is finished.
    #[test]
    fn fused_delivers_multiple_batches_before_the_article_finishes() {
        let mut original = Vec::with_capacity(2 * OUTPUT_BATCH_TARGET + 123);
        for idx in 0..(2 * OUTPUT_BATCH_TARGET + 123) {
            original.push((idx % 251) as u8);
        }

        let mut article = Vec::new();
        encode(&original, &mut article, 128, "batched.bin").unwrap();
        let transcript = transcript(&article, b"223 next\r\n");

        for chunk_len in [4 * 1024usize, 64 * 1024, 700 * 1024] {
            let mut decoder = FusedYencArticleDecoder::new();
            let mut src = BytesMut::new();
            let mut delivered: Vec<Box<[u8]>> = Vec::new();
            let mut finished = None;
            let mut delivered_before_finish = 0usize;

            for chunk in transcript.chunks(chunk_len) {
                src.extend_from_slice(chunk);
                if finished.is_some() {
                    continue;
                }
                finished = decoder.decode_available(&mut src).unwrap();
                match finished.as_mut() {
                    Some(article) => delivered.extend(std::mem::take(&mut article.chunks)),
                    None => {
                        delivered.extend(decoder.drain_output_chunks());
                        delivered_before_finish = delivered.len();
                    }
                }
            }

            assert!(finished.is_some(), "chunk_len {chunk_len}: never finished");
            // The point of the streaming design: whole batches are available
            // while the article is still arriving, not only at the end.
            assert!(
                delivered_before_finish >= 1
                    && delivered.len() > delivered_before_finish
                    && delivered.len() >= 3,
                "chunk_len {chunk_len}: {delivered_before_finish} of {} batches arrived \
                 before the article finished",
                delivered.len()
            );
            let lens: Vec<usize> = delivered[..delivered.len() - 1]
                .iter()
                .map(|chunk| chunk.len())
                .collect();
            assert!(
                lens.iter().all(|&len| len == OUTPUT_BATCH_TARGET),
                "chunk_len {chunk_len}: ragged batches {lens:?}"
            );
            let streamed: Vec<u8> = delivered.iter().flat_map(|c| c.iter().copied()).collect();
            assert_eq!(streamed, original, "chunk_len {chunk_len}");
        }
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

    #[test]
    fn fused_accepts_blank_line_before_nntp_terminator() {
        let original = b"provider trailing blank";
        let mut article = Vec::new();
        encode(original, &mut article, 128, "blank.bin").unwrap();

        let mut bytes = b"222 <test@local> body follows\r\n".to_vec();
        bytes.extend_from_slice(&article);
        bytes.extend_from_slice(b"\r\n.\r\n");

        let mut src = BytesMut::from(bytes.as_slice());
        let mut decoder = FusedYencArticleDecoder::new();
        let decoded = decoder
            .decode_available(&mut src)
            .unwrap()
            .expect("blank line before terminator remains decodable");
        let payload = decoded
            .chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(payload, original);
    }
}
