use std::num::NonZeroU64;
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
use crate::uu::{self, UuDecoder, UuOutcome};

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

/// What an article's body decoded to, and therefore what evidence it carries.
///
/// The two encodings are not interchangeable products. A yEnc article states
/// where its bytes belong in the file and what they check to; a uuencode article
/// states neither. Keeping them apart in the type means no downstream stage can
/// read a placement or a checksum off a uuencode article by accident.
#[derive(Debug)]
pub enum FusedArticleBody {
    /// yEnc: offsets, per-part CRC, and the block-aligned CRC segments the
    /// dual-CRC grid is fed from.
    Yenc(Box<DecodeResult>),
    /// uuencode: decoded bytes, and a name only if this part carried a header.
    Uu(UuOutcome),
}

impl FusedArticleBody {
    /// The yEnc decode result, or `None` for a uuencode article.
    pub fn yenc(&self) -> Option<&DecodeResult> {
        match self {
            Self::Yenc(result) => Some(result),
            Self::Uu(_) => None,
        }
    }

    /// The uuencode outcome, or `None` for a yEnc article.
    pub fn uu(&self) -> Option<&UuOutcome> {
        match self {
            Self::Uu(outcome) => Some(outcome),
            Self::Yenc(_) => None,
        }
    }
}

#[derive(Debug)]
pub struct FusedYencArticle {
    pub response: Response,
    pub chunks: Vec<Box<[u8]>>,
    pub body: FusedArticleBody,
    pub stats: FusedYencArticleStats,
}

impl FusedYencArticle {
    /// The yEnc decode result, for callers that already know this article is
    /// yEnc — the decoder's own tests, which post yEnc bodies by construction.
    ///
    /// Panics on a uuencode article. Production paths match on
    /// [`Self::body`] instead, so that the two encodings' differing evidence
    /// has to be handled rather than assumed.
    #[cfg(test)]
    pub(crate) fn yenc_result(&self) -> &DecodeResult {
        self.body
            .yenc()
            .expect("article under test decodes as yEnc")
    }

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
    /// A uuencode article claimed the header scan; the rest of the body is fed
    /// line-by-line to the uuencode decoder instead of the yEnc kernels.
    UuBody,
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
    /// Present once the header scan handed the article to uuencode.
    uu: Option<UuDecoder>,
    yend_line: Option<Vec<u8>>,
    decode_state: DecodeState,
    output: Vec<u8>,
    output_chunks: Vec<Box<[u8]>>,
    output_reserved: bool,
    profile_cpu: bool,
    par2_block_size: Option<NonZeroU64>,
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
            uu: None,
            yend_line: None,
            decode_state: DecodeState::new(),
            output: Vec::new(),
            output_chunks: Vec::new(),
            output_reserved: false,
            profile_cpu: false,
            par2_block_size: None,
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
                FusedArticleState::UuBody => {
                    let cpu_started = self.phase_cpu_started();
                    let result = self.process_uu_body(src);
                    add_cpu_delta(&mut self.stats.body_decode_cpu, cpu_started);
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

    /// Checkpoint the decode CRC pass at multiples of the recovery set's PAR2
    /// block size, so the article's [`weaver_yenc::DecodeResult::segments`] fold
    /// into block CRC32s without a second pass over the decoded bytes.
    ///
    /// Set before the article's yEnc header is consumed. `None` (the default)
    /// is the pre-block-size policy: one segment per article, which composes
    /// only where article boundaries happen to tile blocks.
    pub fn set_par2_block_size(&mut self, block_size: Option<NonZeroU64>) {
        self.par2_block_size = block_size;
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
            self.begin_body();
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

            // A line that is not `=ybegin` may still be the start of a
            // uuencode article — including on the very first line, because a
            // continuation part of a multi-part uuencode post carries no header
            // at all and opens straight into data. Offering the line to the
            // uuencode sniffer *before* it counts as junk is what lets such an
            // article be claimed rather than scanned past.
            //
            // yEnc precedence is unaffected: `=ybegin` is matched above, so a
            // yEnc article never reaches this branch and its cost is unchanged.
            if uu::looks_like_uu(&self.line_buf) {
                let mut decoder = UuDecoder::new();
                decoder.push_line(&self.line_buf);
                self.line_buf.clear();
                self.uu = Some(decoder);
                self.state = FusedArticleState::UuBody;
                return Ok(true);
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
            self.begin_body();
        }
        Ok(true)
    }

    /// Enter the body with the CRC pass anchored at this article's place in the
    /// file, so its checkpoints land on PAR2 block boundaries rather than on
    /// wire-chunk boundaries.
    fn begin_body(&mut self) {
        if let Some(metadata) = self.metadata.as_ref() {
            self.decode_state
                .set_segment_plan(metadata.article_file_offset(), self.par2_block_size);
        }
        self.state = FusedArticleState::Body;
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

    /// Feed the rest of the article to the uuencode decoder, a line at a time.
    ///
    /// Returns `true` once the NNTP multiline terminator has been consumed.
    /// Lines after the uuencode `end` are still drained here rather than
    /// rejected: trailers are routine, and the decoder ignores them.
    fn process_uu_body(&mut self, src: &mut BytesMut) -> Result<bool> {
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

            if let Some(decoder) = self.uu.as_mut() {
                decoder.push_line(&self.line_buf);
                if !decoder.output().is_empty() {
                    self.output.extend_from_slice(&decoder.take_output());
                }
            }
            self.line_buf.clear();
            self.flush_ready_output();
        }
    }

    fn finish_article(&mut self) -> Result<FusedYencArticle> {
        if self.uu.is_some() {
            return self.finish_uu_article();
        }

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
            body: FusedArticleBody::Yenc(Box::new(result)),
            stats,
        })
    }

    /// Close out an article the uuencode decoder claimed.
    ///
    /// An article that engaged the sniffer but decoded nothing is not a
    /// uuencode article after all — treating it as one would hand the pipeline
    /// an empty segment for a file that never existed. It fails as a missing
    /// header instead, which is exactly what the article would have done before
    /// uuencode support existed.
    fn finish_uu_article(&mut self) -> Result<FusedYencArticle> {
        let cpu_started = self.phase_cpu_started();
        let response = self.response.take().ok_or_else(|| {
            NntpError::MalformedResponse("missing BODY response line".to_string())
        })?;
        let decoder = self.uu.take().ok_or(YencError::MissingHeader)?;
        let outcome = decoder.outcome();

        if outcome.decoded_len == 0 && !outcome.ended {
            return Err(YencError::MissingHeader.into());
        }

        self.flush_output();
        let chunks = std::mem::take(&mut self.output_chunks);

        let mut stats = self.stats.clone();
        stats.decoded_bytes_written = outcome.decoded_len;
        // uuencode carries no checksum and no declared size, so every field
        // that would report one stays at its "nothing to say" value rather than
        // reporting a zero that could be read as a verified result.
        stats.crc_actual = 0;
        stats.crc_expected = None;
        stats.yenc_size_expected = None;
        stats.yenc_size_actual = outcome.decoded_len;
        stats.output_batches = chunks.len() as u64;
        stats.input_bytes_consumed = stats.encoded_bytes_consumed;
        add_cpu_delta(&mut stats.article_finish_cpu, cpu_started);

        self.state = FusedArticleState::Done;

        Ok(FusedYencArticle {
            response,
            chunks,
            body: FusedArticleBody::Uu(outcome),
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
        decode_current_with_block_size(transcript, None)
    }

    fn decode_current_with_block_size(
        transcript: &[u8],
        par2_block_size: Option<NonZeroU64>,
    ) -> (DecodedArticle, Vec<u8>) {
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
        decoder.set_par2_block_size(par2_block_size);
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
        decode_fused_with_chunks_and_block_size(transcript, chunks, None)
    }

    fn decode_fused_with_chunks_and_block_size(
        transcript: &[u8],
        chunks: &[usize],
        par2_block_size: Option<NonZeroU64>,
    ) -> (FusedYencArticle, Vec<u8>) {
        let mut decoder = FusedYencArticleDecoder::new();
        decoder.set_par2_block_size(par2_block_size);
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
        assert_eq!(
            expected.result.bytes_written,
            actual.yenc_result().bytes_written
        );
        assert_eq!(expected.result.part_crc, actual.yenc_result().part_crc);
        assert_eq!(
            expected.result.expected_part_crc,
            actual.yenc_result().expected_part_crc
        );
        assert_eq!(
            expected.result.expected_file_crc,
            actual.yenc_result().expected_file_crc
        );
        assert_eq!(
            expected.result.has_trailer,
            actual.yenc_result().has_trailer
        );
        assert_eq!(expected.result.crc_status, actual.yenc_result().crc_status);
        assert_eq!(expected.result.defects, actual.yenc_result().defects);
        // Gate 3: checkpoint placement is a function of file offsets, so the
        // two decoders must emit byte-identical segment records however the
        // wire bytes were split -- not merely agree on the article CRC.
        assert_eq!(expected.result.segments, actual.yenc_result().segments);
        assert_eq!(
            weaver_yenc::combine_contiguous(&actual.yenc_result().segments)
                .map_or(0, |folded| folded.crc32),
            actual.yenc_result().part_crc,
            "article pcrc32 must be the fold of its own segments"
        );

        let expected_meta = &expected.result.metadata;
        let actual_meta = &actual.yenc_result().metadata;
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
        // Block sizes small enough that this corpus's articles straddle many
        // boundaries, plus `None` for the pre-block-size policy. Every one of
        // them is swept at every split point, so a checkpoint that moved with a
        // chunk boundary cannot pass.
        let mut multi_segment_cases = 0usize;
        for par2_block_size in [
            None,
            NonZeroU64::new(1),
            NonZeroU64::new(3),
            NonZeroU64::new(7),
            NonZeroU64::new(16),
            NonZeroU64::new(64),
        ] {
            let (expected, expected_leftover) =
                decode_current_with_block_size(&transcript, par2_block_size);
            assert_eq!(expected_leftover, leftover);
            if expected.result.segments.len() > 1 {
                multi_segment_cases += 1;
            }

            for split in 0..=transcript.len() {
                let (actual, actual_leftover) = decode_fused_with_chunks_and_block_size(
                    &transcript,
                    &[split, transcript.len() - split],
                    par2_block_size,
                );
                assert_same_article(&expected, &actual);
                assert_eq!(
                    expected_leftover, actual_leftover,
                    "split at {split} block size {par2_block_size:?}"
                );
            }
        }
        // Non-vacuity: an article that never crossed a boundary would make the
        // segment comparison above a comparison of one whole-article record.
        assert!(
            multi_segment_cases > 0,
            "no block size made this article emit more than one segment"
        );
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

    /// `=ybegin` is scanned for, not required on the first body line --
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
        assert!(decoded.yenc_result().defects.junk_before_ybegin);
        assert_eq!(decoded.yenc_result().crc_status, CrcVerification::Verified);
    }

    /// `line=`/`size=`/`name=` are all optional -- reference decoders do
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

    /// A `=ybegin` whose numeric fields are unparseable degrades
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

    /// `=ypart end=` past the `=ybegin size=` file size. The part length
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
        assert!(decoded.yenc_result().defects.ypart_end_exceeds_size);
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
        assert!(!decoded.yenc_result().defects.any());
        assert_eq!(decoded.yenc_result().crc_status, CrcVerification::Verified);
    }

    /// A mangled `crc32=` is treated as absent, leaving the article decoded
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
            assert!(
                decoded.yenc_result().defects.invalid_crc32,
                "crc32={garbage:?}"
            );
            assert_eq!(
                decoded.yenc_result().crc_status,
                CrcVerification::Unverified,
                "crc32={garbage:?} must not read as verified"
            );
            assert_eq!(decoded.yenc_result().expected_file_crc, None);
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
        assert_eq!(decoded.yenc_result().crc_status, CrcVerification::Verified);
        assert!(!decoded.yenc_result().defects.invalid_crc32);
    }

    /// One byte-wise parser serves every entry point, so tab separators
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
        assert!(decoded.yenc_result().has_trailer);
        assert_eq!(decoded.yenc_result().crc_status, CrcVerification::Verified);
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

    // ---- uuencode sniffing and routing ----

    /// Encode `data` as a uuencode body, optionally with a `begin` header.
    fn uu_body(data: &[u8], name: Option<&str>) -> Vec<u8> {
        let mut body = Vec::new();
        if let Some(name) = name {
            body.extend_from_slice(format!("begin 644 {name}\r\n").as_bytes());
        }
        for line in data.chunks(45) {
            body.push((line.len() as u8) + b' ');
            for group in line.chunks(3) {
                let b0 = group[0];
                let b1 = group.get(1).copied().unwrap_or(0);
                let b2 = group.get(2).copied().unwrap_or(0);
                for sextet in [
                    b0 >> 2,
                    ((b0 << 4) | (b1 >> 4)) & 0x3F,
                    ((b1 << 2) | (b2 >> 6)) & 0x3F,
                    b2 & 0x3F,
                ] {
                    body.push(if sextet == 0 { b'`' } else { sextet + b' ' });
                }
            }
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(b"`\r\nend\r\n");
        body
    }

    fn article_payload(article: &FusedYencArticle) -> Vec<u8> {
        article
            .chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect()
    }

    #[test]
    fn uu_article_decodes_and_reports_a_uu_body() {
        let original: Vec<u8> = (0..5_000u32).map(|i| (i * 31 + 7) as u8).collect();
        let transcript = transcript(&uu_body(&original, Some("silver-horizon.bin")), b"");

        // Split across awkward chunk boundaries: uuencode is line-oriented, so
        // the streaming line assembly has to hold partial lines correctly.
        let (article, leftover) = decode_fused_with_chunks(&transcript, &[7, 1, 3, 500, 64, 4096]);

        assert!(leftover.is_empty());
        assert_eq!(article_payload(&article), original);

        let outcome = article.body.uu().expect("uuencode body");
        assert_eq!(outcome.decoded_len, original.len() as u64);
        assert_eq!(outcome.filename.as_deref(), Some("silver-horizon.bin"));
        assert!(outcome.ended);
        assert!(!outcome.damaged);
        assert!(article.body.yenc().is_none());
    }

    #[test]
    fn uu_continuation_article_engages_on_its_very_first_line() {
        // A continuation part carries no `begin` line at all: the article opens
        // with data, so the sniffer has to claim it on line one.
        let original: Vec<u8> = (0..2_000u32).map(|i| (i * 17 + 3) as u8).collect();
        let mut body = uu_body(&original, None);
        // A continuation part does not end the file either; drop the terminator
        // so this is purely bare data lines.
        body.truncate(body.len() - b"`\r\nend\r\n".len());
        let transcript = transcript(&body, b"");

        let (article, _) = decode_fused_with_chunks(&transcript, &[13, 4096]);

        assert_eq!(article_payload(&article), original);
        let outcome = article.body.uu().expect("uuencode body");
        assert_eq!(outcome.decoded_len, original.len() as u64);
        assert_eq!(outcome.filename, None);
        assert!(outcome.saw_body);
        assert!(!outcome.ended);
    }

    #[test]
    fn uu_article_survives_a_large_preamble_before_begin() {
        // Leading junk is offered to the sniffer line by line; a `begin` that
        // arrives after a chatty preamble still decodes.
        let original: Vec<u8> = (0..900u32).map(|i| (i * 13 + 5) as u8).collect();
        let mut body = Vec::new();
        for index in 0..200 {
            body.extend_from_slice(
                format!("Preamble line {index} from the poster.\r\n").as_bytes(),
            );
        }
        body.extend_from_slice(&uu_body(&original, Some("silver-horizon.bin")));
        let transcript = transcript(&body, b"");

        let (article, _) = decode_fused_with_chunks(&transcript, &[4096, 4096, 4096]);

        assert_eq!(article_payload(&article), original);
        assert_eq!(
            article.body.uu().expect("uuencode body").decoded_len,
            original.len() as u64
        );
    }

    #[test]
    fn junk_without_any_uu_line_still_fails_as_a_missing_header() {
        // The junk cap is unchanged for articles the sniffer does not claim:
        // prose that is neither yEnc nor uuencode must still fail exactly as it
        // did before, rather than being scanned forever.
        let mut body = Vec::new();
        while body.len() <= MAX_HEADER_SCAN_BYTES {
            body.extend_from_slice(b"this line is ordinary prose and encodes nothing at all\r\n");
        }
        let transcript = transcript(&body, b"");

        let mut decoder = FusedYencArticleDecoder::new();
        let mut src = BytesMut::from(&transcript[..]);
        let error = decoder
            .decode_available(&mut src)
            .expect_err("junk beyond the scan cap must fail");
        assert!(
            matches!(error, FusedYencError::Yenc(YencError::MissingHeader)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn article_with_no_binary_data_fails_like_a_missing_header() {
        // A single line can look uuencode-ish and then produce nothing. That is
        // not a uuencode article, and admitting it would hand the pipeline an
        // empty segment for a file that never existed.
        let transcript = transcript(b"begin 644 empty.bin\r\n", b"");

        let mut decoder = FusedYencArticleDecoder::new();
        let mut src = BytesMut::from(&transcript[..]);
        let error = decoder
            .decode_available(&mut src)
            .expect_err("a body-less uuencode article must fail");
        assert!(
            matches!(error, FusedYencError::Yenc(YencError::MissingHeader)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn uu_article_surfaces_damage_without_losing_bytes() {
        let original: Vec<u8> = (0..135u32).map(|i| (i * 7 + 1) as u8).collect();
        let good = uu_body(&original, Some("silver-horizon.bin"));

        // Splice a line that claims 45 bytes but carries two characters.
        let insert_at = good
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("header line ending")
            + 2;
        let mut body = good[..insert_at].to_vec();
        body.extend_from_slice(b"M!!\r\n");
        body.extend_from_slice(&good[insert_at..]);
        let transcript = transcript(&body, b"");

        let (article, _) = decode_fused_with_chunks(&transcript, &[4096]);

        let outcome = article.body.uu().expect("uuencode body");
        assert!(outcome.damaged, "the bad line must be reported");
        // Every good line's bytes are still here — PAR2 judges, not the decoder.
        assert_eq!(article_payload(&article), original);
        assert_eq!(outcome.decoded_len, original.len() as u64);
    }

    #[test]
    fn the_captured_real_world_uu_article_decodes_to_its_published_values() {
        // The only genuine field-captured uuencode article in either reference
        // decoder's suite: a complete NNTP BODY response, dot-stuffed body and
        // all, carrying an SVG. Every other uuencode fixture in this repository
        // was synthesised by an encoder written against the format, so this is
        // the one test that checks the decoder against a real posting nobody
        // designed to make it pass.
        //
        // The three expected values are the ones the reference suite asserts,
        // so a failure here is a real disagreement between the two decoders
        // rather than a disagreement about the fixture. See
        // `testdata/README.md` for provenance and licensing.
        let transcript = include_bytes!("../testdata/uu_logo_full.nntp");

        let mut src = BytesMut::from(&transcript[..]);
        let mut decoder = FusedYencArticleDecoder::new();
        let article = decoder
            .decode_available(&mut src)
            .unwrap()
            .expect("the captured article decodes in one pass");

        let decoded = article_payload(&article);
        let outcome = article.body.uu().expect("uuencode body");

        assert_eq!(outcome.filename.as_deref(), Some("logo-full.svg"));
        assert_eq!(decoded.len(), 2184);
        assert_eq!(outcome.decoded_len, 2184);

        let mut crc = weaver_yenc::crc::Crc32::new();
        crc.update(&decoded);
        assert_eq!(crc.finalize(), 0x6BC2_917D);

        assert!(outcome.ended, "the article carries its terminator");
        assert!(!outcome.damaged, "a real posting must decode cleanly");
        // It really is the SVG the `begin` line named, which is what makes the
        // CRC meaningful rather than a hash of whatever happened to come out.
        assert!(decoded.starts_with(b"<svg "), "decoded an SVG document");
        assert!(decoded.ends_with(b"</svg>"), "and all of it");
    }

    #[test]
    fn yenc_article_is_unaffected_by_the_uu_sniffer() {
        // The sniffer sits on the `=ybegin`-miss branch, so a yEnc article
        // never reaches it and still produces a yEnc body.
        let original: Vec<u8> = (0..4_096u32).map(|i| (i * 11 + 2) as u8).collect();
        let mut article = Vec::new();
        encode(&original, &mut article, 128, "silver-horizon.bin").unwrap();
        let transcript = transcript(&article, b"");

        let (expected, _) = decode_current(&transcript);
        let (actual, leftover) = decode_fused_with_chunks(&transcript, &[64, 4096]);

        assert!(leftover.is_empty());
        assert!(actual.body.uu().is_none(), "yEnc must not report a uu body");
        assert_same_article(&expected, &actual);
    }
}
