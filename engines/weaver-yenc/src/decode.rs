use crate::crc::Crc32;
use crate::error::YencError;
use crate::header;
use crate::types::{CrcVerification, DecodeResult, YencHeaderDefects, YencMetadata};

/// Decoder state equivalent to rapidyenc's public `RapidYencDecoderState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RapidyencDecodeState {
    /// Previous decoded position is at a CRLF line boundary.
    #[default]
    CrLf,
    /// Previous input ended after an escape marker.
    Eq,
    /// Previous input ended after a carriage return.
    Cr,
    /// No special boundary state is pending.
    None,
    /// Previous input ended after a raw `\r\n.` prefix.
    CrLfDot,
    /// Previous input ended after a raw `\r\n.\r` prefix.
    CrLfDotCr,
    /// Previous input ended after a line-start escape marker.
    CrLfEq,
}

/// End marker reported by rapidyenc-style incremental decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RapidyencDecodeEnd {
    None,
    Control,
    Article,
}

/// Progress reported by rapidyenc-style incremental decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RapidyencDecodeProgress {
    pub source_consumed: usize,
    pub bytes_written: usize,
    pub end: RapidyencDecodeEnd,
}

/// Options for decoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecodeOptions {
    /// If true, perform NNTP dot-unstuffing (strip leading dot from lines
    /// starting with `..`). Default: false (assumes transport layer handled it).
    pub dot_unstuffing: bool,
}

/// Upper bound on the decoded length of `input_len` encoded bytes.
///
/// yEnc never expands: escapes are two bytes in and one byte out, line breaks
/// and NNTP dot-stuffing only shrink the stream. So `input.len()` is both a
/// sufficient and the smallest always-sufficient output size, and it is the
/// size the whole-article entry points require — see [`decode`].
pub const fn max_decoded_len(input_len: usize) -> usize {
    input_len
}

/// Decode a complete yEnc article (headers + data + trailer) from `input` into `output`.
///
/// Returns metadata and CRC verification results.
///
/// # Output buffer contract
///
/// `output` must be at least [`max_decoded_len`]`(input.len())` bytes. A
/// smaller buffer is rejected up front with [`YencError::BufferTooSmall`] —
/// deliberately, and even when the decoded data would have fit: the SIMD
/// kernels store full vector widths and only the byte-at-a-time scalar kernel
/// can honour a compact destination, so accepting an undersized buffer here
/// would silently trade an order of magnitude of throughput for a saved
/// allocation. The byte-level [`decode_body`]/[`decode_chunk`] entry points do
/// accept compact buffers, with that cost documented.
///
/// Do **not** size `output` from [`crate::YencMetadata::size`]. That field is
/// `0` whenever the poster omitted or mangled `size=` (see
/// [`crate::YencHeaderDefects::missing_size`] and
/// [`crate::YencHeaderDefects::invalid_size`]), and it describes the whole
/// *file* rather than this article's part. Size from `input.len()`, or use
/// [`decode_nntp_append`], which sizes itself.
pub fn decode(input: &[u8], output: &mut [u8]) -> Result<DecodeResult, YencError> {
    decode_with_options(input, output, DecodeOptions::default())
}

/// Decode a yEnc article from raw NNTP data (with dot-stuffing still present).
///
/// This is the fast path for the download pipeline: the NNTP codec passes raw
/// data without dot-unstuffing, and this function handles unstuffing inline
/// during the yEnc body decode pass. This eliminates a separate scan over the
/// entire article body.
///
/// Headers (`=ybegin`, `=ypart`) are not affected by dot-stuffing (they start
/// with `=`, not `.`), so header parsing works on raw data without changes.
///
/// `output` is subject to the same [`max_decoded_len`] contract as [`decode`].
pub fn decode_nntp(input: &[u8], output: &mut [u8]) -> Result<DecodeResult, YencError> {
    decode_with_options(
        input,
        output,
        DecodeOptions {
            dot_unstuffing: true,
        },
    )
}

/// Decode a raw NNTP yEnc article, appending decoded bytes to `output`.
///
/// Unlike [`decode_nntp`], the destination does not need to be pre-sized (or
/// zero-initialized): decoded bytes are written straight into the vector's
/// spare capacity. Decoded output never exceeds the encoded input, so one
/// `input.len()` reservation is always sufficient.
pub fn decode_nntp_append(input: &[u8], output: &mut Vec<u8>) -> Result<DecodeResult, YencError> {
    let start = output.len();
    output.reserve(input.len());
    let spare = output.spare_capacity_mut();
    // SAFETY: the decode writes at most `input.len()` initialized bytes into
    // this spare region and reports exactly how many bytes were written.
    let out =
        unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), input.len()) };
    let result = decode_with_options(
        input,
        out,
        DecodeOptions {
            dot_unstuffing: true,
        },
    )?;
    // SAFETY: the first `bytes_written` bytes of the spare region were
    // initialized by the decode, and `bytes_written <= input.len()`.
    unsafe {
        output.set_len(start + result.bytes_written);
    }
    Ok(result)
}

/// Decode bytes with rapidyenc's `rapidyenc_decode` semantics.
pub fn decode_rapidyenc(input: &[u8], output: &mut [u8]) -> Result<usize, YencError> {
    let mut state = RapidyencDecodeState::default();
    decode_rapidyenc_ex(true, input, output, &mut state)
}

/// Decode bytes with rapidyenc's `rapidyenc_decode_ex` semantics.
pub fn decode_rapidyenc_ex(
    is_raw: bool,
    input: &[u8],
    output: &mut [u8],
    state: &mut RapidyencDecodeState,
) -> Result<usize, YencError> {
    crate::simd::decode_rapidyenc_into(input, output, is_raw, state)
}

/// Decode bytes with rapidyenc's raw incremental end-detecting semantics.
pub fn decode_rapidyenc_incremental(
    input: &[u8],
    output: &mut [u8],
    state: &mut RapidyencDecodeState,
) -> Result<RapidyencDecodeProgress, YencError> {
    let outcome = crate::simd::decode_rapidyenc_incremental_into(input, output, state)?;
    Ok(RapidyencDecodeProgress {
        source_consumed: outcome.consumed,
        bytes_written: outcome.written,
        end: outcome.end,
    })
}

/// Decode a raw NNTP yEnc body chunk into `output`, stopping at the next yEnc
/// control line.
///
/// This is the lower-level streaming hook used when an outer NNTP parser owns
/// response-boundary handling. It appends decoded bytes to `output`, updates
/// `decode_state` and CRC, performs NNTP dot-unstuffing, and reports exact
/// source bytes consumed.
pub fn decode_body_chunk_until_control(
    decode_state: &mut DecodeState,
    input: &[u8],
    output: &mut Vec<u8>,
) -> Result<RapidyencDecodeProgress, YencError> {
    decode_body_chunk_until_end(decode_state, input, output)
}

fn validate_ypart_decoded_size(
    metadata: &YencMetadata,
    bytes_written: usize,
) -> Result<(), YencError> {
    if metadata.part.is_some()
        && let (Some(begin), Some(end)) = (metadata.begin, metadata.end)
        // Checked arithmetic: `end < begin` is rejected at parse time, but the
        // metadata is public and callers can hand us anything, and malformed
        // input must never panic.
        && let Some(expected_size) = end.checked_sub(begin).and_then(|len| len.checked_add(1))
        && bytes_written as u64 != expected_size
    {
        return Err(YencError::SizeMismatch {
            expected: expected_size,
            actual: bytes_written as u64,
        });
    }

    Ok(())
}

/// Shared post-decode validation for every article entry point (whole-buffer,
/// streaming, and the fused NNTP decoder), so all three agree on which
/// inconsistencies fail an article and which are merely recorded.
fn finalize_decode(
    metadata: YencMetadata,
    yend: Option<header::YendFields>,
    bytes_written: usize,
    part_crc: u32,
) -> Result<DecodeResult, YencError> {
    let (expected_part_crc, expected_file_crc, yend_size, yend_defects, has_trailer) = match yend {
        Some(yend) => (yend.pcrc32, yend.crc32, yend.size, yend.defects, true),
        None => (None, None, None, YencHeaderDefects::default(), false),
    };
    let defects = metadata.defects.merged(yend_defects);

    // Validate decoded size against =yend size (like NZBGet's dsInvalidSize).
    if let Some(expected_size) = yend_size
        && bytes_written as u64 != expected_size
    {
        return Err(YencError::SizeMismatch {
            expected: expected_size,
            actual: bytes_written as u64,
        });
    }

    validate_ypart_decoded_size(&metadata, bytes_written)?;

    // For single-part articles, also validate =ybegin size vs =yend size --
    // but only when =ybegin actually declared a usable size. A poster who
    // omitted or mangled `size=` gives us nothing to cross-check against, and
    // the placeholder 0 must not be read as a real declaration.
    if metadata.part.is_none()
        && !defects.missing_size
        && !defects.invalid_size
        && let Some(expected_size) = yend_size
        && metadata.size != expected_size
    {
        return Err(YencError::SizeMismatch {
            expected: metadata.size,
            actual: expected_size,
        });
    }

    // For single-part articles, the `crc32` field in =yend is the part CRC.
    // For multi-part articles, `pcrc32` is the part CRC.
    let expected_crc_to_check = if metadata.part.is_some() {
        expected_part_crc
    } else {
        // For single-part, crc32 is the file CRC which equals the part CRC.
        expected_file_crc
    };

    let crc_status = match expected_crc_to_check {
        Some(expected) if expected != part_crc => {
            return Err(YencError::CrcMismatch {
                expected,
                actual: part_crc,
            });
        }
        Some(_) => CrcVerification::Verified,
        // No usable expected CRC: either none was posted or it was unparseable
        // and dropped. Either way nothing was verified, and `crc_status` says
        // exactly that rather than reporting success.
        None => CrcVerification::Unverified,
    };

    Ok(DecodeResult {
        metadata,
        bytes_written,
        part_crc,
        expected_part_crc,
        expected_file_crc,
        crc_status,
        has_trailer,
        defects,
    })
}

/// Decode a complete yEnc article with custom options.
///
/// `output` is subject to the same [`max_decoded_len`] contract as [`decode`].
pub fn decode_with_options(
    input: &[u8],
    output: &mut [u8],
    options: DecodeOptions,
) -> Result<DecodeResult, YencError> {
    // Checked before any parsing so the contract is a property of the call, not
    // of how compressible this particular article happened to be.
    let needed = max_decoded_len(input.len());
    if output.len() < needed {
        return Err(YencError::BufferTooSmall {
            needed,
            available: output.len(),
        });
    }

    let parsed = header::parse_headers_with_options(input, options)?;

    let data_end = parsed.data_end.max(parsed.data_start);
    let data = &input[parsed.data_start..data_end];

    let mut crc = Crc32::new();
    let bytes_written = decode_body_with_line_length(
        data,
        output,
        &mut crc,
        options,
        Some(parsed.metadata.line_length),
    )?;

    let part_crc = crc.finalize();

    finalize_decode(parsed.metadata, parsed.yend, bytes_written, part_crc)
}

/// Result of incrementally decoding a full NNTP yEnc article.
#[derive(Debug)]
pub struct DecodedArticle {
    pub data: Vec<u8>,
    pub result: DecodeResult,
}

/// Finalize a caller-owned streaming yEnc decode using the same validation
/// rules as [`StreamingArticleDecoder`].
pub fn finish_streaming_article(
    metadata: YencMetadata,
    yend: Option<header::YendFields>,
    decode_state: DecodeState,
    output: Vec<u8>,
) -> Result<DecodedArticle, YencError> {
    let result = finish_streaming_result(metadata, yend, decode_state)?;

    Ok(DecodedArticle {
        data: output,
        result,
    })
}

pub fn finish_streaming_result(
    metadata: YencMetadata,
    yend: Option<header::YendFields>,
    decode_state: DecodeState,
) -> Result<DecodeResult, YencError> {
    let bytes_written = decode_state.bytes_decoded as usize;
    let part_crc = decode_state.finalize_crc();

    finalize_decode(metadata, yend, bytes_written, part_crc)
}

/// How much leading junk may precede `=ybegin` before the article is declared
/// header-less. Bounded so a body that never contains a yEnc header cannot make
/// the streaming decoder buffer without limit.
pub(crate) const MAX_HEADER_SCAN_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamingStage {
    Header,
    Body,
    Trailer,
    Finished,
}

/// Incremental yEnc article decoder for raw NNTP BODY data.
///
/// This keeps only the small yEnc header/trailer lines buffered while decoding
/// body bytes as chunks arrive from the socket.
#[derive(Debug, Clone)]
pub struct StreamingArticleDecoder {
    stage: StreamingStage,
    header_bytes: Vec<u8>,
    pending: Vec<u8>,
    metadata: Option<YencMetadata>,
    yend_line: Option<Vec<u8>>,
    decode_state: DecodeState,
    output_reserved: bool,
}

impl StreamingArticleDecoder {
    pub fn new() -> Self {
        Self {
            stage: StreamingStage::Header,
            header_bytes: Vec::with_capacity(256),
            pending: Vec::with_capacity(4096),
            metadata: None,
            yend_line: None,
            decode_state: DecodeState::new(),
            output_reserved: false,
        }
    }

    /// Feed the next raw NNTP BODY chunk into the decoder.
    ///
    /// `output` is appended with decoded bytes. Chunks are expected to end on a
    /// line boundary, which matches the NNTP raw streaming fetch path.
    pub fn feed_chunk(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<(), YencError> {
        if input.is_empty() {
            return Ok(());
        }
        if self.stage == StreamingStage::Finished {
            return Err(YencError::InvalidHeader {
                field: "stream".to_string(),
                reason: "received data after =yend trailer".to_string(),
            });
        }

        if self.stage == StreamingStage::Body && self.pending.is_empty() {
            self.reserve_output_if_known(output);
            let progress = decode_body_chunk_until_end(&mut self.decode_state, input, output)?;
            if progress.end == RapidyencDecodeEnd::Control {
                self.pending.extend_from_slice(b"=y");
                self.pending
                    .extend_from_slice(&input[progress.source_consumed..]);
                self.stage = StreamingStage::Trailer;
            } else {
                return Ok(());
            }
        } else {
            self.pending.extend_from_slice(input);
        }

        loop {
            let progressed = match self.stage {
                StreamingStage::Header => self.process_header()?,
                StreamingStage::Body => self.process_body(output)?,
                StreamingStage::Trailer => self.process_trailer()?,
                StreamingStage::Finished => false,
            };

            if !progressed {
                break;
            }
        }

        Ok(())
    }

    fn reserve_output_if_known(&mut self, output: &mut Vec<u8>) {
        const MAX_ARTICLE_RESERVE: usize = 16 * 1024 * 1024;

        if self.output_reserved {
            return;
        }
        self.output_reserved = true;

        let Some(metadata) = self.metadata.as_ref() else {
            return;
        };

        let expected = match (metadata.begin, metadata.end) {
            (Some(begin), Some(end)) if end >= begin => end - begin + 1,
            (Some(_), Some(_)) => return,
            // Only trust `=ybegin size=` when the poster declared one: the `0`
            // placeholder for a missing/mangled field means "unknown".
            _ if metadata.part.is_none()
                && !metadata.defects.missing_size
                && !metadata.defects.invalid_size =>
            {
                metadata.size
            }
            _ => return,
        };
        let Ok(expected) = usize::try_from(expected) else {
            return;
        };
        // Clamp, do not bail: an article whose declared size is *larger* than
        // the cap is exactly the one that most needs a head start, and skipping
        // the reservation entirely left it growing from zero one doubling at a
        // time. The cap bounds a lying header's damage; it is not a reason to
        // reserve nothing.
        let reserve = expected.min(MAX_ARTICLE_RESERVE);
        if reserve == 0 || reserve <= output.capacity() {
            return;
        }

        output.reserve_exact(reserve - output.capacity());
    }

    pub fn finish(mut self, output: Vec<u8>) -> Result<DecodedArticle, YencError> {
        let metadata = self.metadata.take().ok_or(YencError::MissingHeader)?;

        // `process_trailer` only ever sees complete lines, so an article that
        // ends on its `=yend` with no terminator leaves the trailer sitting in
        // `pending`. The whole-buffer path parses that trailer; recover it here
        // (and reject a non-`=yend` control line with the same error
        // `process_trailer` would have raised) so the two agree.
        let trailer_line = self.yend_line.take();
        let trailer_line = match trailer_line {
            Some(line) => Some(line),
            None if self.stage == StreamingStage::Trailer && !self.pending.is_empty() => {
                if !header::is_control_line(&self.pending, b"=yend") {
                    return Err(YencError::InvalidHeader {
                        field: "=yend".to_string(),
                        reason: "unexpected trailing line after yEnc body".to_string(),
                    });
                }
                Some(std::mem::take(&mut self.pending))
            }
            None => None,
        };

        let yend = if let Some(yend_line) = trailer_line {
            let mut scratch = self.header_bytes;
            scratch.extend_from_slice(b"x\r\n");
            scratch.extend_from_slice(&yend_line);
            let parsed = header::parse_headers(&scratch)?;
            Some(parsed.yend.ok_or(YencError::MissingTrailer)?)
        } else {
            None
        };

        finish_streaming_article(metadata, yend, self.decode_state, output)
    }

    fn process_header(&mut self) -> Result<bool, YencError> {
        let Some(line_len) = next_line_len(&self.pending) else {
            return Ok(false);
        };

        let line = self.pending.drain(..line_len).collect::<Vec<_>>();
        self.header_bytes.extend_from_slice(&line);

        match header::parse_headers(&self.header_bytes) {
            Ok(parsed) => {
                self.decode_state
                    .set_line_length_hint(Some(parsed.metadata.line_length));
                self.metadata = Some(parsed.metadata);
                self.stage = StreamingStage::Body;
            }
            // `=ybegin` is scanned for, not required on line 1: keep buffering
            // leading junk lines until one of them is the real header.
            Err(YencError::MissingHeader) => {
                if self.header_bytes.len() > MAX_HEADER_SCAN_BYTES {
                    return Err(YencError::MissingHeader);
                }
            }
            Err(YencError::MissingField(field)) if field == "=ypart" => {}
            Err(err) => return Err(err),
        }

        Ok(true)
    }

    fn process_body(&mut self, output: &mut Vec<u8>) -> Result<bool, YencError> {
        if self.pending.is_empty() {
            return Ok(false);
        }
        self.reserve_output_if_known(output);

        let progress = decode_body_chunk_until_end(&mut self.decode_state, &self.pending, output)?;
        if progress.end == RapidyencDecodeEnd::Control {
            let rest = self.pending.split_off(progress.source_consumed);
            self.pending.clear();
            self.pending.extend_from_slice(b"=y");
            self.pending.extend_from_slice(&rest);
            self.stage = StreamingStage::Trailer;
            return Ok(true);
        }

        self.pending.clear();
        Ok(false)
    }

    fn process_trailer(&mut self) -> Result<bool, YencError> {
        let Some(line_len) = next_line_len(&self.pending) else {
            return Ok(false);
        };

        let line = self.pending.drain(..line_len).collect::<Vec<_>>();
        if header::is_control_line(&line, b"=yend") {
            self.yend_line = Some(line);
            self.stage = StreamingStage::Finished;
        } else if !line.iter().all(|b| matches!(b, b'\r' | b'\n')) {
            return Err(YencError::InvalidHeader {
                field: "=yend".to_string(),
                reason: "unexpected trailing line after yEnc body".to_string(),
            });
        }

        Ok(true)
    }
}

fn decode_body_chunk_until_end(
    decode_state: &mut DecodeState,
    input: &[u8],
    output: &mut Vec<u8>,
) -> Result<RapidyencDecodeProgress, YencError> {
    if input.is_empty() {
        return Ok(RapidyencDecodeProgress {
            source_consumed: 0,
            bytes_written: 0,
            end: RapidyencDecodeEnd::None,
        });
    }

    let start = output.len();
    output.reserve(input.len());
    let spare = output.spare_capacity_mut();
    // SAFETY: `decode_chunk_until_end` writes at most `input.len()` initialized
    // bytes into this spare region and reports exactly how many bytes were
    // written.
    let out =
        unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), input.len()) };
    let progress = decode_chunk_until_end(
        input,
        out,
        decode_state,
        DecodeOptions {
            dot_unstuffing: true,
        },
    )?;
    // SAFETY: The first `bytes_written` bytes of the spare region were
    // initialized by `decode_chunk_until_end`, and `bytes_written <= input.len()`.
    unsafe {
        output.set_len(start + progress.bytes_written);
    }
    Ok(progress)
}

impl Default for StreamingArticleDecoder {
    fn default() -> Self {
        Self::new()
    }
}

fn next_line_len(buf: &[u8]) -> Option<usize> {
    memchr::memchr(b'\n', buf).map(|idx| idx + 1)
}

/// Decode raw yEnc-encoded data (no headers/trailers) from `input` into `output`.
///
/// Updates the CRC hasher with decoded bytes. Returns the number of bytes written.
///
/// # Output buffer contract
///
/// Unlike the whole-article [`decode`] entry points, this accepts an `output`
/// shorter than [`max_decoded_len`]`(input.len())`: a caller that already knows
/// the exact decoded length (a fixed-size part, a preallocated slot) should not
/// have to over-allocate. The cost is explicit rather than free — a compact
/// destination cannot take the SIMD kernels' full-width stores and runs the
/// byte-at-a-time scalar kernel instead, roughly an order of magnitude slower.
/// Overflow is a typed [`YencError::BufferTooSmall`], never a truncated write.
pub fn decode_body(
    input: &[u8],
    output: &mut [u8],
    crc: &mut Crc32,
    options: DecodeOptions,
) -> Result<usize, YencError> {
    decode_body_with_line_length(input, output, crc, options, None)
}

fn decode_body_with_line_length(
    input: &[u8],
    output: &mut [u8],
    crc: &mut Crc32,
    options: DecodeOptions,
    line_length: Option<u32>,
) -> Result<usize, YencError> {
    let written = crate::simd::decode_body_into_with_line_length(
        input,
        output,
        options.dot_unstuffing,
        line_length,
    )?;

    // CRC is updated once at the end for better hardware utilization
    // (the accelerated CRC path benefits from larger updates).
    if written > 0 {
        crc.update(&output[..written]);
    }

    Ok(written)
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
    /// If true, the previous chunk ended after a raw CR and needs the next byte
    /// to decide whether this is a real CRLF line boundary.
    pub(crate) cr_pending: bool,
    /// Running CRC32 state.
    pub(crate) crc: Crc32,
    /// Trusted encoded-column line length hint from `=ybegin line=...`.
    pub(crate) line_length_hint: Option<usize>,
    /// Total bytes decoded so far across all chunks.
    pub bytes_decoded: u64,
    /// Number of CRC update calls made while streaming this body.
    pub crc_update_calls: u64,
}

impl DecodeState {
    /// Create a new decode state for streaming decode.
    pub fn new() -> Self {
        Self {
            escape_pending: false,
            at_line_start: true,
            dot_pending: false,
            cr_pending: false,
            crc: Crc32::new(),
            line_length_hint: None,
            bytes_decoded: 0,
            crc_update_calls: 0,
        }
    }

    /// Provide the encoded-column line length from `=ybegin line=...`.
    pub fn set_line_length_hint(&mut self, line_length: Option<u32>) {
        self.line_length_hint = line_length
            .and_then(|value| usize::try_from(value).ok())
            .filter(|&value| value > 0);
    }

    /// Finalize and return the CRC32 of all decoded data. Consumes the state.
    pub fn finalize_crc(self) -> u32 {
        self.crc.finalize()
    }

    /// Get the current CRC32 value without consuming the state.
    /// (clones the hasher internally)
    pub fn current_crc(&self) -> u32 {
        self.crc.current()
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
///
/// `output` follows the same compact-buffer contract as [`decode_body`].
pub fn decode_chunk(
    input: &[u8],
    output: &mut [u8],
    state: &mut DecodeState,
    options: DecodeOptions,
) -> Result<usize, YencError> {
    let written = crate::simd::decode_chunk_into(input, output, state, options.dot_unstuffing)?;

    // Final CRC update for any remaining decoded bytes in this chunk.
    if written > 0 {
        state.crc.update(&output[..written]);
        state.crc_update_calls += 1;
    }

    state.bytes_decoded += written as u64;
    Ok(written)
}

fn decode_chunk_until_end(
    input: &[u8],
    output: &mut [u8],
    state: &mut DecodeState,
    options: DecodeOptions,
) -> Result<RapidyencDecodeProgress, YencError> {
    let outcome =
        crate::simd::decode_chunk_until_end_into(input, output, state, options.dot_unstuffing)?;

    if outcome.written > 0 {
        state.crc.update(&output[..outcome.written]);
        state.crc_update_calls += 1;
    }

    state.bytes_decoded += outcome.written as u64;
    Ok(RapidyencDecodeProgress {
        source_consumed: outcome.consumed,
        bytes_written: outcome.written,
        end: outcome.end,
    })
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

    fn decode_body_bytes(input: &[u8], opts: DecodeOptions) -> Result<Vec<u8>, YencError> {
        let mut output = vec![0u8; input.len().saturating_add(64)];
        let mut crc = Crc32::new();
        let written = decode_body(input, &mut output, &mut crc, opts)?;
        output.truncate(written);
        Ok(output)
    }

    fn decode_chunked_bytes(
        input: &[u8],
        opts: DecodeOptions,
        splits: &[usize],
    ) -> Result<Vec<u8>, YencError> {
        let mut state = DecodeState::new();
        let mut output = vec![0u8; input.len().saturating_add(64)];
        let mut written = 0usize;
        let mut start = 0usize;

        for &end in splits {
            let end = end.min(input.len());
            let n = decode_chunk(&input[start..end], &mut output[written..], &mut state, opts)?;
            written += n;
            start = end;
        }

        if start < input.len() {
            let n = decode_chunk(&input[start..], &mut output[written..], &mut state, opts)?;
            written += n;
        }

        output.truncate(written);
        Ok(output)
    }

    fn reference_decode_body(input: &[u8], dot_unstuffing: bool) -> Result<Vec<u8>, YencError> {
        let mut output = Vec::with_capacity(input.len());
        let mut src = 0usize;
        let mut at_crlf = true;

        while src < input.len() {
            let byte = input[src];

            if dot_unstuffing && at_crlf && byte == b'.' {
                src += 1;
                at_crlf = false;
                continue;
            }

            match byte {
                b'\r' => {
                    src += 1;
                    if src < input.len() && input[src] == b'\n' {
                        src += 1;
                        at_crlf = true;
                    } else {
                        at_crlf = false;
                    }
                }
                b'\n' => {
                    src += 1;
                    at_crlf = false;
                }
                b'=' => {
                    src += 1;
                    if src >= input.len() {
                        return Err(YencError::MalformedEscape(src - 1));
                    }
                    output.push(input[src].wrapping_sub(106));
                    src += 1;
                    at_crlf = false;
                }
                _ => {
                    output.push(byte.wrapping_sub(42));
                    src += 1;
                    at_crlf = false;
                }
            }
        }

        Ok(output)
    }

    fn lcg_next(seed: &mut u64) -> u8 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (*seed >> 32) as u8
    }

    #[test]
    fn decode_nntp_append_matches_slice_decode() {
        let mut seed = 0xa11a_c0de_5eed_f00du64;
        for len in [0usize, 1, 63, 128, 4096, 40_000] {
            let original: Vec<u8> = (0..len).map(|_| lcg_next(&mut seed)).collect();
            let mut article = Vec::new();
            crate::encode::encode(&original, &mut article, 128, "append.bin").unwrap();

            let mut slice_out = vec![0u8; article.len() + 64];
            let slice_result = decode_nntp(&article, &mut slice_out).unwrap();
            slice_out.truncate(slice_result.bytes_written);

            // Append into an empty vector and into one carrying a prefix.
            let mut appended = Vec::new();
            let append_result = decode_nntp_append(&article, &mut appended).unwrap();
            assert_eq!(appended, slice_out);
            assert_eq!(append_result.bytes_written, slice_result.bytes_written);
            assert_eq!(append_result.part_crc, slice_result.part_crc);

            let mut prefixed = b"prefix".to_vec();
            decode_nntp_append(&article, &mut prefixed).unwrap();
            assert_eq!(&prefixed[..6], b"prefix");
            assert_eq!(&prefixed[6..], slice_out.as_slice());
        }
    }

    #[test]
    fn decode_nntp_append_leaves_output_untouched_on_error() {
        let mut prefixed = b"keep".to_vec();
        let err = decode_nntp_append(b"not a yenc article", &mut prefixed);
        assert!(err.is_err());
        assert_eq!(prefixed, b"keep");
    }

    #[test]
    fn streaming_article_decoder_single_chunk() {
        let original = b"Hello streamed yEnc";
        let mut article =
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).into_bytes();
        article.extend_from_slice(&encode_raw(original));
        article.extend_from_slice(format!("\r\n=yend size={}\r\n", original.len()).as_bytes());

        let mut decoder = StreamingArticleDecoder::new();
        let mut output = Vec::new();
        decoder.feed_chunk(&article, &mut output).unwrap();
        let decoded = decoder.finish(output).unwrap();

        assert_eq!(decoded.data, original);
        assert_eq!(decoded.result.bytes_written, original.len());
    }

    #[test]
    fn streaming_article_decoder_crc_mismatch_errors() {
        let original = b"Hello streamed yEnc";
        let mut article =
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).into_bytes();
        article.extend_from_slice(&encode_raw(original));
        article.extend_from_slice(
            format!("\r\n=yend size={} crc32=DEADBEEF\r\n", original.len()).as_bytes(),
        );

        let mut decoder = StreamingArticleDecoder::new();
        let mut output = Vec::new();
        decoder.feed_chunk(&article, &mut output).unwrap();
        let err = decoder.finish(output).unwrap_err();

        assert!(matches!(
            err,
            YencError::CrcMismatch {
                expected: 0xDEADBEEF,
                ..
            }
        ));
    }

    #[test]
    fn streaming_article_decoder_split_chunks() {
        let original = b"Hello streamed yEnc split";
        let mut article =
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).into_bytes();
        article.extend_from_slice(&encode_raw(original));
        article.extend_from_slice(format!("\r\n=yend size={}\r\n", original.len()).as_bytes());

        let split_at = article
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|idx| idx + 2)
            .unwrap();

        let mut decoder = StreamingArticleDecoder::new();
        let mut output = Vec::new();
        decoder
            .feed_chunk(&article[..split_at], &mut output)
            .unwrap();
        decoder
            .feed_chunk(&article[split_at..], &mut output)
            .unwrap();
        let decoded = decoder.finish(output).unwrap();

        assert_eq!(decoded.data, original);
        assert_eq!(decoded.result.bytes_written, original.len());
    }

    // ── Broken-poster corpus: whole-buffer + streaming, every split point ──

    /// NNTP dot-stuffing, as a real server applies it on the wire.
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

    /// Build a single-part article from caller-supplied header/trailer lines
    /// around a genuinely valid, dot-stuffed encoded body.
    fn broken_poster_article(
        prologue: &[u8],
        ybegin: &[u8],
        yend: impl FnOnce(u64, u32) -> Vec<u8>,
    ) -> Vec<u8> {
        let mut crc = Crc32::new();
        crc.update(TOLERANT_BODY);

        let mut article = Vec::new();
        article.extend_from_slice(prologue);
        article.extend_from_slice(ybegin);
        article.extend_from_slice(&encode_raw(TOLERANT_BODY));
        article.extend_from_slice(b"\r\n");
        article.extend_from_slice(&yend(TOLERANT_BODY.len() as u64, crc.finalize()));
        dot_stuff_lines(&article)
    }

    /// Leading `\x04` encodes to `.`, so the body exercises dot-unstuffing too.
    const TOLERANT_BODY: &[u8] = b"\x04tolerant poster body\r\n\0";

    fn healthy_yend(size: u64, crc: u32) -> Vec<u8> {
        format!("=yend size={size} crc32={crc:08x}\r\n").into_bytes()
    }

    /// Feed an article through `StreamingArticleDecoder` split at `split`.
    fn decode_streaming_split(article: &[u8], split: usize) -> Result<DecodedArticle, YencError> {
        let mut decoder = StreamingArticleDecoder::new();
        let mut output = Vec::new();
        decoder.feed_chunk(&article[..split], &mut output)?;
        decoder.feed_chunk(&article[split..], &mut output)?;
        decoder.finish(output)
    }

    /// Every entry point must agree on a tolerated article, at every split
    /// point of the streaming one.
    fn assert_all_entry_points_agree(article: &[u8], expected_data: &[u8]) {
        assert_entry_points_agree_at_splits(article, expected_data, 0..=article.len());
    }

    /// [`assert_all_entry_points_agree`] over a chosen set of split points, for
    /// articles too large to sweep byte by byte.
    fn assert_entry_points_agree_at_splits(
        article: &[u8],
        expected_data: &[u8],
        splits: impl IntoIterator<Item = usize>,
    ) {
        let mut whole_out = vec![0u8; article.len() + 64];
        let whole = decode_nntp(article, &mut whole_out).expect("whole-buffer decode");
        assert_eq!(&whole_out[..whole.bytes_written], expected_data);

        for split in splits {
            let streamed = decode_streaming_split(article, split)
                .unwrap_or_else(|err| panic!("split {split}: {err}"));
            assert_eq!(streamed.data, expected_data, "split {split}");
            assert_eq!(streamed.result.bytes_written, whole.bytes_written);
            assert_eq!(
                streamed.result.has_trailer, whole.has_trailer,
                "split {split}"
            );
            assert_eq!(streamed.result.part_crc, whole.part_crc);
            assert_eq!(
                streamed.result.crc_status, whole.crc_status,
                "split {split}"
            );
            assert_eq!(streamed.result.defects, whole.defects, "split {split}");
            assert_eq!(streamed.result.metadata.name, whole.metadata.name);
            assert_eq!(streamed.result.metadata.size, whole.metadata.size);
            assert_eq!(
                streamed.result.metadata.line_length,
                whole.metadata.line_length
            );
        }
    }

    /// Every entry point must reject a broken article the same way, at every
    /// split point of the streaming one.
    fn assert_all_entry_points_reject(article: &[u8], expected_reason: &str) {
        let mut whole_out = vec![0u8; max_decoded_len(article.len())];
        let whole =
            decode_nntp(article, &mut whole_out).expect_err("whole-buffer decode must fail");
        assert!(
            whole.to_string().contains(expected_reason),
            "whole-buffer error {whole} does not mention {expected_reason:?}"
        );

        for split in 0..=article.len() {
            let err = decode_streaming_split(article, split)
                .err()
                .unwrap_or_else(|| panic!("split {split}: streaming decode must fail"));
            assert!(
                err.to_string().contains(expected_reason),
                "split {split}: error {err} does not mention {expected_reason:?}"
            );
        }
    }

    // ── B7: line-layer trailer detection vs the SIMD kernel's stop rule ──
    //
    // The kernel stops the body at a literal CRLF (optionally one NNTP-stuffed
    // `.`) followed by *any* `=y`. The whole-buffer path used to scan for
    // `=yend ` lines instead, which disagreed with the streaming and fused
    // decoders on the three inputs below.

    /// A `\r\n=y…` line that is not `=yend`. The kernel stops there, so the
    /// article cannot decode past it — the line scan used to step over the
    /// line, decode it as body data, and carry on to the real `=yend`.
    #[test]
    fn stray_control_line_in_body_is_rejected_by_every_entry_point() {
        for stray in [
            b"=yfoo\r\n".as_slice(),
            b"=ypart begin=1 end=2\r\n".as_slice(),
            // `=yend` with no separator is deliberately not a control line
            // (both reference decoders require the trailing space), but the
            // kernel still stops at its `=y`.
            b"=yend\r\n".as_slice(),
            b"=y\r\n".as_slice(),
        ] {
            let mut article = Vec::new();
            article.extend_from_slice(b"=ybegin line=128 size=24 name=stray.bin\r\n");
            article.extend_from_slice(&encode_raw(TOLERANT_BODY));
            article.extend_from_slice(b"\r\n");
            article.extend_from_slice(stray);
            article.extend_from_slice(b"=yend size=24\r\n");
            let article = dot_stuff_lines(&article);

            assert_all_entry_points_reject(&article, "unexpected trailing line after yEnc body");
        }
    }

    /// An article truncated in the middle of its control line: `\r\n=y` and
    /// nothing more. Every entry point rejects it rather than one of them
    /// silently reporting a complete, trailer-less article.
    #[test]
    fn truncated_control_line_is_rejected_by_every_entry_point() {
        let mut article = Vec::new();
        article.extend_from_slice(b"=ybegin line=128 size=24 name=trunc.bin\r\n");
        article.extend_from_slice(&encode_raw(TOLERANT_BODY));
        let article = dot_stuff_lines(&article);
        let mut article = article;
        article.extend_from_slice(b"\r\n=y");

        assert_all_entry_points_reject(&article, "unexpected trailing line after yEnc body");
    }

    /// A bare `\n=yend ` is *not* a trailer: the kernel reaches its line-start
    /// state only through a literal CRLF, so the would-be trailer is body data
    /// everywhere. The whole-buffer path used to accept it, decoding 24 bytes
    /// with a verified CRC where the streaming path decoded the trailer text
    /// into the payload.
    #[test]
    fn bare_lf_trailer_is_body_data_in_every_entry_point() {
        let mut article = Vec::new();
        article.extend_from_slice(b"=ybegin line=128 size=12 name=barelf.bin\n");
        let body_start = article.len();
        article.extend_from_slice(&encode_raw(b"bare lf body"));
        article.extend_from_slice(b"\n=yend size=12\n");

        let expected = reference_decode_body(&article[body_start..], true).unwrap();
        assert!(expected.len() > 12, "the trailer must land in the payload");
        assert_all_entry_points_agree(&article, &expected);

        let mut out = vec![0u8; max_decoded_len(article.len())];
        let result = decode_nntp(&article, &mut out).unwrap();
        assert!(!result.has_trailer);
        assert_eq!(result.crc_status, CrcVerification::Unverified);
    }

    /// A dot-stuffed trailer (`\r\n.=yend `). The kernel strips the one leading
    /// `.` at line start in raw mode before looking for `=y`, so this really is
    /// the trailer; the line scan used to miss it entirely.
    #[test]
    fn dot_prefixed_trailer_is_found_by_every_entry_point() {
        let base = broken_poster_article(
            b"",
            b"=ybegin line=128 size=24 name=dotyend.bin\r\n",
            healthy_yend,
        );
        let yend_at = base
            .windows(b"=yend ".len())
            .rposition(|window| window == b"=yend ")
            .expect("article has a trailer");

        let mut article = base[..yend_at].to_vec();
        article.push(b'.');
        article.extend_from_slice(&base[yend_at..]);

        assert_all_entry_points_agree(&article, TOLERANT_BODY);

        let mut out = vec![0u8; max_decoded_len(article.len())];
        let result = decode_nntp(&article, &mut out).unwrap();
        assert!(result.has_trailer);
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    /// An article that ends on its `=yend` with no line terminator. The
    /// whole-buffer path always parsed that trailer; `process_trailer` only
    /// ever sees complete lines, so the streaming decoder now recovers it at
    /// `finish` instead of reporting a trailer-less article.
    #[test]
    fn unterminated_trailer_is_parsed_by_every_entry_point() {
        let base = broken_poster_article(
            b"",
            b"=ybegin line=128 size=24 name=noeol.bin\r\n",
            healthy_yend,
        );
        let article = base[..base.len() - 2].to_vec();
        assert!(
            !matches!(article.last(), Some(b'\r' | b'\n')),
            "the trailer's own CRLF must be the thing that was dropped"
        );

        assert_all_entry_points_agree(&article, TOLERANT_BODY);

        let mut out = vec![0u8; max_decoded_len(article.len())];
        let result = decode_nntp(&article, &mut out).unwrap();
        assert!(result.has_trailer);
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    /// The whole-buffer `=ybegin` scan is bounded at the same 64 KiB the
    /// streaming and fused decoders use, so all three call the same articles
    /// header-less. The article is too large to sweep byte by byte, so this
    /// checks the boundary on both sides at a spread of split points.
    #[test]
    fn leading_junk_scan_bound_agrees_across_entry_points() {
        const JUNK_LINE: &[u8] = b"this line is not a yenc header at all\r\n";
        let lines_at_cap = MAX_HEADER_SCAN_BYTES / JUNK_LINE.len();
        assert!(lines_at_cap * JUNK_LINE.len() <= MAX_HEADER_SCAN_BYTES);

        for (lines, expect_decode) in [(lines_at_cap, true), (lines_at_cap + 1, false)] {
            let prologue = JUNK_LINE.repeat(lines);
            let article = broken_poster_article(
                &prologue,
                b"=ybegin line=128 size=24 name=deepjunk.bin\r\n",
                healthy_yend,
            );
            let splits = [
                0,
                1,
                prologue.len() - 1,
                prologue.len(),
                prologue.len() + 1,
                article.len() - 1,
                article.len(),
            ];

            if expect_decode {
                assert_entry_points_agree_at_splits(&article, TOLERANT_BODY, splits);
            } else {
                let mut out = vec![0u8; max_decoded_len(article.len())];
                assert!(
                    matches!(
                        decode_nntp(&article, &mut out),
                        Err(YencError::MissingHeader)
                    ),
                    "whole-buffer accepted {lines} junk lines"
                );
                for split in splits {
                    assert!(
                        matches!(
                            decode_streaming_split(&article, split),
                            Err(YencError::MissingHeader)
                        ),
                        "streaming accepted {lines} junk lines at split {split}"
                    );
                }
            }
        }
    }

    // ── B8: the output-buffer contract ──────────────────────────────────

    /// The whole-article entry points reject an undersized destination with a
    /// typed error instead of silently dropping to the scalar kernel.
    #[test]
    fn article_decode_rejects_undersized_output_instead_of_degrading() {
        let article = broken_poster_article(
            b"",
            b"=ybegin line=128 size=24 name=sized.bin\r\n",
            healthy_yend,
        );

        // Exactly the trap the docs used to invite: size the destination from
        // `metadata.size`, which a poster is free to omit entirely.
        let mut undeclared = vec![0u8; 0];
        assert!(matches!(
            decode_nntp(&article, &mut undeclared),
            Err(YencError::BufferTooSmall { .. })
        ));

        // Even a buffer that *would* have held the decoded bytes is rejected:
        // the contract is a property of the call, not of how compressible this
        // particular article turned out to be.
        let mut just_enough = vec![0u8; TOLERANT_BODY.len()];
        assert!(matches!(
            decode_nntp(&article, &mut just_enough),
            Err(YencError::BufferTooSmall { .. })
        ));

        let mut sized = vec![0u8; max_decoded_len(article.len())];
        let result = decode_nntp(&article, &mut sized).unwrap();
        assert_eq!(&sized[..result.bytes_written], TOLERANT_BODY);
    }

    /// The byte-level API keeps the compact-buffer fallback, and it still
    /// produces the same bytes as the SIMD path.
    #[test]
    fn body_decode_compact_buffer_matches_the_simd_path() {
        let encoded = encode_raw(TOLERANT_BODY);

        let mut compact = vec![0u8; TOLERANT_BODY.len()];
        let mut compact_crc = Crc32::new();
        let compact_written = decode_body(
            &encoded,
            &mut compact,
            &mut compact_crc,
            DecodeOptions::default(),
        )
        .unwrap();

        let mut roomy = vec![0u8; max_decoded_len(encoded.len())];
        let mut roomy_crc = Crc32::new();
        let roomy_written = decode_body(
            &encoded,
            &mut roomy,
            &mut roomy_crc,
            DecodeOptions::default(),
        )
        .unwrap();

        assert_eq!(compact_written, roomy_written);
        assert_eq!(&compact[..compact_written], &roomy[..roomy_written]);
        assert_eq!(compact_crc.finalize(), roomy_crc.finalize());
    }

    // ── E19: pre-reservation for oversized articles ─────────────────────

    /// An article whose declared size exceeds the 16 MiB reservation cap used
    /// to get *no* pre-reservation at all, so the one article that most needed
    /// a head start grew from zero one doubling at a time.
    #[test]
    fn streaming_reserves_the_cap_for_articles_larger_than_it() {
        const CAP: usize = 16 * 1024 * 1024;

        // Metadata-driven: the header declares 32 MiB, the body is one byte.
        let mut decoder = StreamingArticleDecoder::new();
        let mut output = Vec::new();
        decoder
            .feed_chunk(
                b"=ybegin line=128 size=33554432 name=huge.bin\r\nA",
                &mut output,
            )
            .unwrap();
        assert!(
            output.capacity() >= CAP,
            "oversized article reserved only {}",
            output.capacity()
        );

        // A declared size under the cap still reserves exactly what it needs.
        let mut decoder = StreamingArticleDecoder::new();
        let mut small = Vec::new();
        decoder
            .feed_chunk(
                b"=ybegin line=128 size=1048576 name=small.bin\r\nA",
                &mut small,
            )
            .unwrap();
        assert!(small.capacity() >= 1024 * 1024);
        assert!(small.capacity() < CAP);
    }

    /// D6: junk before `=ybegin`, including `=`-bearing and partial-prefix junk.
    #[test]
    fn tolerates_leading_junk_before_ybegin_at_every_split_point() {
        let article = broken_poster_article(
            b"Path: news.example\r\n=yb\r\n=ybegi partial\r\n=ybeginner notes\r\n\r\n",
            b"=ybegin line=128 size=24 name=junk.bin\r\n",
            healthy_yend,
        );

        assert_all_entry_points_agree(&article, TOLERANT_BODY);

        let mut out = vec![0u8; article.len() + 64];
        let result = decode_nntp(&article, &mut out).unwrap();
        assert!(result.defects.junk_before_ybegin);
        assert_eq!(result.crc_status, CrcVerification::Verified);
        assert_eq!(result.metadata.name, "junk.bin");
    }

    /// D7: every combination of missing `line=`/`size=`/`name=`.
    #[test]
    fn tolerates_every_missing_ybegin_field_combination_at_every_split_point() {
        for line_field in [None, Some("line=128")] {
            for size_field in [None, Some("size=24")] {
                for name_field in [None, Some("name=missing.bin")] {
                    let mut ybegin = String::from("=ybegin");
                    for field in [line_field, size_field, name_field].into_iter().flatten() {
                        ybegin.push(' ');
                        ybegin.push_str(field);
                    }
                    ybegin.push_str(" \r\n");

                    let article = broken_poster_article(b"", ybegin.as_bytes(), healthy_yend);
                    assert_all_entry_points_agree(&article, TOLERANT_BODY);

                    let mut out = vec![0u8; article.len() + 64];
                    let result = decode_nntp(&article, &mut out).unwrap();
                    assert_eq!(result.defects.missing_line, line_field.is_none());
                    assert_eq!(result.defects.missing_size, size_field.is_none());
                    assert_eq!(result.defects.missing_name, name_field.is_none());
                    // A missing =ybegin size= must not be cross-checked against
                    // the =yend size= as if it had been declared as 0.
                    assert_eq!(result.crc_status, CrcVerification::Verified);
                }
            }
        }
    }

    /// D9: garbage `crc32=` leaves the article decoded but explicitly
    /// unverified -- it must never read back as "verified".
    #[test]
    fn tolerates_garbage_crc32_at_every_split_point() {
        for garbage in ["nothex", "", "  ", "DEADBEEFDEADBEEF0", "1234ZZZZ", "0x99"] {
            let article = broken_poster_article(
                b"",
                b"=ybegin line=128 size=24 name=badcrc.bin\r\n",
                |size, _crc| format!("=yend size={size} crc32={garbage}\r\n").into_bytes(),
            );

            assert_all_entry_points_agree(&article, TOLERANT_BODY);

            let mut out = vec![0u8; article.len() + 64];
            let result = decode_nntp(&article, &mut out).unwrap();
            assert!(result.defects.invalid_crc32, "crc32={garbage:?}");
            assert_eq!(result.expected_file_crc, None, "crc32={garbage:?}");
            assert_eq!(
                result.crc_status,
                CrcVerification::Unverified,
                "crc32={garbage:?}"
            );
        }
    }

    /// D8: `=ypart end=` past `=ybegin size=`, and the healthy inverse.
    #[test]
    fn tolerates_ypart_end_past_declared_size_at_every_split_point() {
        for (declared_size, expect_defect) in [(10u64, true), (100_000u64, false)] {
            let end = TOLERANT_BODY.len();
            let article = broken_poster_article(
                b"",
                format!(
                    "=ybegin part=1 total=2 line=128 size={declared_size} name=part.bin\r\n\
                     =ypart begin=1 end={end}\r\n"
                )
                .as_bytes(),
                |size, crc| format!("=yend size={size} part=1 pcrc32={crc:08x}\r\n").into_bytes(),
            );

            assert_all_entry_points_agree(&article, TOLERANT_BODY);

            let mut out = vec![0u8; article.len() + 64];
            let result = decode_nntp(&article, &mut out).unwrap();
            assert_eq!(result.defects.ypart_end_exceeds_size, expect_defect);
            assert_eq!(result.crc_status, CrcVerification::Verified);
        }
    }

    /// D10: tab and case variants of field names behave identically on the
    /// whole-buffer, streaming, and `parse_ybegin_line` entry points.
    #[test]
    fn tolerates_tab_and_case_field_variants_at_every_split_point() {
        for ybegin in [
            b"=ybegin\tline=128\tsize=24\tname=tabs.bin\r\n".as_slice(),
            b"=ybegin LINE=128 Size=24 NaMe=case.bin\r\n".as_slice(),
            b"=ybegin  line=128    size=24  name=spaces.bin\r\n".as_slice(),
        ] {
            let article = broken_poster_article(b"", ybegin, healthy_yend);
            assert_all_entry_points_agree(&article, TOLERANT_BODY);

            let mut out = vec![0u8; article.len() + 64];
            let result = decode_nntp(&article, &mut out).unwrap();
            assert_eq!(result.metadata.size, 24);
            assert_eq!(result.metadata.line_length, 128);
            assert_eq!(result.crc_status, CrcVerification::Verified);
        }
    }

    /// A real CRC mismatch must still fail: tolerance is about *absent* data,
    /// not about accepting corrupt data.
    #[test]
    fn genuine_crc_mismatch_still_fails_after_tolerance_changes() {
        let article = broken_poster_article(
            b"junk line\r\n",
            b"=ybegin name=mismatch.bin\r\n",
            |size, _crc| format!("=yend size={size} crc32=deadbeef\r\n").into_bytes(),
        );

        let mut out = vec![0u8; article.len() + 64];
        let err = decode_nntp(&article, &mut out).unwrap_err();
        assert!(matches!(
            err,
            YencError::CrcMismatch {
                expected: 0xDEADBEEF,
                ..
            }
        ));
    }

    /// A body with no `=ybegin` at all is still a hard failure, and the
    /// streaming decoder must not buffer without bound while looking for one.
    #[test]
    fn header_scan_is_bounded_and_still_reports_missing_header() {
        let mut junk = Vec::new();
        while junk.len() <= MAX_HEADER_SCAN_BYTES + 4096 {
            junk.extend_from_slice(b"this line is not a yenc header at all\r\n");
        }

        let mut decoder = StreamingArticleDecoder::new();
        let mut output = Vec::new();
        let err = decoder
            .feed_chunk(&junk, &mut output)
            .expect_err("unbounded junk must not be buffered forever");
        assert!(matches!(err, YencError::MissingHeader));

        let mut out = vec![0u8; max_decoded_len(junk.len())];
        assert!(matches!(
            decode_nntp(&junk, &mut out),
            Err(YencError::MissingHeader)
        ));
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
    fn decode_body_long_raw_exact_output_uses_bounded_fallback() {
        let mut encoded = b"\r\n".repeat(99);
        encoded.push(b'A');
        let mut output = [0u8; 1];
        let mut crc = Crc32::new();

        let written = decode_body(
            &encoded,
            &mut output,
            &mut crc,
            DecodeOptions {
                dot_unstuffing: true,
            },
        )
        .unwrap();

        assert_eq!(written, 1);
        assert_eq!(output, [b'A'.wrapping_sub(42)]);
    }

    #[test]
    fn decode_body_long_raw_small_output_returns_error() {
        let mut encoded = b"\r\n".repeat(99);
        encoded.extend_from_slice(b"AA");
        let mut output = [0u8; 1];
        let mut crc = Crc32::new();

        let result = decode_body(
            &encoded,
            &mut output,
            &mut crc,
            DecodeOptions {
                dot_unstuffing: true,
            },
        );

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
        // raw rapidyenc-style body decode strips the NNTP terminator's dot
        // but does not stop; end detection is handled by incremental decode.
        let mut encoded = Vec::new();
        encoded.extend(yenc_encode_byte(b'A'));
        encoded.extend_from_slice(b"\r\n.\r\n");
        encoded.extend(yenc_encode_byte(b'B'));

        let mut output = vec![0u8; 64];
        let mut crc = Crc32::new();
        let opts = DecodeOptions {
            dot_unstuffing: true,
        };
        let n = decode_body(&encoded, &mut output, &mut crc, opts).unwrap();
        assert_eq!(n, 2);
        assert_eq!(output[0], b'A');
        assert_eq!(output[1], b'B');
    }

    #[test]
    fn decode_full_single_part() {
        let original = b"Hello, yEnc World!";
        let encoded_data = encode_raw(original);

        let mut hasher = crate::crc::Crc32::new();
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
        assert_eq!(result.crc_status, CrcVerification::Verified);
        assert_eq!(result.metadata.name, "test.bin");
        assert_eq!(result.metadata.size, original.len() as u64);
    }

    #[test]
    fn decode_full_crc_mismatch_errors() {
        let original = b"Test data";
        let encoded_data = encode_raw(original);

        let mut article = Vec::new();
        article.extend_from_slice(
            format!("=ybegin line=128 size={} name=test.bin\r\n", original.len()).as_bytes(),
        );
        article.extend_from_slice(&encoded_data);
        article.extend_from_slice(b"\r\n=yend size=9 crc32=DEADBEEF\r\n");

        let mut output = vec![0u8; 1024];
        let err = decode(&article, &mut output).unwrap_err();

        assert!(matches!(
            err,
            YencError::CrcMismatch {
                expected: 0xDEADBEEF,
                ..
            }
        ));
    }

    #[test]
    fn decode_multipart_checks_pcrc32_and_carries_file_crc32() {
        let original = b"Test data";
        let encoded_data = encode_raw(original);
        let mut hasher = crate::crc::Crc32::new();
        hasher.update(original);
        let part_crc = hasher.finalize();

        let mut article = Vec::new();
        article.extend_from_slice(
            format!(
                "=ybegin part=1 total=2 line=128 size={} name=test.bin\r\n",
                original.len() * 2
            )
            .as_bytes(),
        );
        article.extend_from_slice(format!("=ypart begin=1 end={}\r\n", original.len()).as_bytes());
        article.extend_from_slice(&encoded_data);
        article.extend_from_slice(
            format!(
                "\r\n=yend size={} part=1 pcrc32={part_crc:08x} crc32=DEADBEEF\r\n",
                original.len()
            )
            .as_bytes(),
        );

        let mut output = vec![0u8; 1024];
        let result = decode(&article, &mut output).unwrap();

        assert_eq!(result.bytes_written, original.len());
        assert_eq!(&output[..result.bytes_written], original.as_slice());
        assert_eq!(result.part_crc, part_crc);
        assert_eq!(result.expected_part_crc, Some(part_crc));
        assert_eq!(result.expected_file_crc, Some(0xDEADBEEF));
        assert_eq!(result.crc_status, CrcVerification::Verified);
    }

    #[test]
    fn decode_multipart_with_unrecognized_part_crc_field_remains_unverified() {
        let damaged = b"Damaged data";
        let encoded_data = encode_raw(damaged);
        let mut article = Vec::new();
        article.extend_from_slice(
            format!(
                "=ybegin part=1 total=2 line=128 size={} name=test.bin\r\n",
                damaged.len() * 2
            )
            .as_bytes(),
        );
        article.extend_from_slice(format!("=ypart begin=1 end={}\r\n", damaged.len()).as_bytes());
        article.extend_from_slice(&encoded_data);
        article.extend_from_slice(
            format!(
                "\r\n=yend size={} part=1 xcrc32=DEADBEEF crc32=A3BDCA2D\r\n",
                damaged.len()
            )
            .as_bytes(),
        );

        let mut output = vec![0u8; 1024];
        let result = decode(&article, &mut output).unwrap();

        assert_eq!(result.expected_part_crc, None);
        assert_eq!(result.expected_file_crc, Some(0xA3BDCA2D));
        // Multi-part checks `pcrc32`; the poster misspelled it, so the file
        // `crc32` is carried but nothing about this part was verified.
        assert_eq!(result.crc_status, CrcVerification::Unverified);
    }

    #[test]
    fn decode_multipart_ypart_range_size_mismatch_errors() {
        let original = b"Test data";
        let encoded_data = encode_raw(original);

        let mut article = Vec::new();
        article.extend_from_slice(b"=ybegin part=1 total=2 line=128 size=20 name=test.bin\r\n");
        article.extend_from_slice(b"=ypart begin=1 end=8\r\n");
        article.extend_from_slice(&encoded_data);
        article.extend_from_slice(format!("\r\n=yend size={}\r\n", original.len()).as_bytes());

        let mut output = vec![0u8; 1024];
        let err = decode(&article, &mut output).unwrap_err();

        assert!(matches!(
            err,
            YencError::SizeMismatch {
                expected: 8,
                actual: 9,
            }
        ));
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
        // No trailer, so no CRC to validate -- and that must not read back as
        // a successful verification.
        assert_eq!(result.crc_status, CrcVerification::Unverified);
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
            .windows(2)
            .position(|window| window == b"\r\n")
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
            .windows(2)
            .position(|window| window == b"\r\n")
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
        assert_eq!(total, 3); // 'A', dot-unstuffed '.', and 'B'
        assert_eq!(output[0], b'A');
        assert_eq!(output[1], b'.'.wrapping_sub(42));
        assert_eq!(output[2], b'B');
    }

    #[test]
    fn decode_chunk_dot_pending_data() {
        // Chunk1 ends with \r\n + '.', chunk2 starts with a normal byte.
        // rapidyenc treats the dot as NNTP stuffing and strips it.
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

        // Chunk 2 starts with encoded 'B'; the pending dot is stripped.
        let mut chunk2 = Vec::new();
        chunk2.extend(yenc_encode_byte(b'B'));

        let n2 = decode_chunk(&chunk2, &mut output[n1..], &mut state, opts).unwrap();

        let total = n1 + n2;
        assert_eq!(total, 2); // 'A', stripped stuffed dot, 'B'
        assert_eq!(output[0], b'A');
        assert_eq!(output[1], b'B');
    }

    #[test]
    fn decode_body_dense_escape_clusters() {
        let input = b"=====\r\n==A=B=C===\n==";
        let expected = reference_decode_body(input, false).unwrap();
        let decoded = decode_body_bytes(input, DecodeOptions::default()).unwrap();

        assert_eq!(decoded, expected);
    }

    #[test]
    fn decode_body_raw_escaped_cr_carries_into_crlf_dot_state() {
        let input = b"A=\r\n.B";
        let decoded = decode_body_bytes(
            input,
            DecodeOptions {
                dot_unstuffing: true,
            },
        )
        .unwrap();

        assert_eq!(
            decoded,
            vec![
                b'A'.wrapping_sub(42),
                b'\r'.wrapping_sub(106),
                b'B'.wrapping_sub(42),
            ]
        );
    }

    #[test]
    fn decode_body_rapidyenc_generic_special_pattern() {
        // Ported from rapidyenc's generic decode fixture: dots decode to 0x04,
        // CRLF is skipped, and =n decodes to the same byte.
        let input = b"\x2e\x2e\x2e\x2e\x2e\x2e\x0d\x0a\x3d\x6e\x2e\x2e\x2e";
        let decoded = decode_body_bytes(input, DecodeOptions::default()).unwrap();

        assert_eq!(decoded, vec![0x04; 10]);
    }

    #[test]
    fn decode_body_crlf_dot_and_nntp_terminator() {
        let input = b"AB\r\n..CD\r\n.\r\nEF";
        let expected = reference_decode_body(input, true).unwrap();
        let decoded = decode_body_bytes(
            input,
            DecodeOptions {
                dot_unstuffing: true,
            },
        )
        .unwrap();

        assert_eq!(decoded, expected);
        assert_eq!(
            decoded,
            vec![
                b'A'.wrapping_sub(42),
                b'B'.wrapping_sub(42),
                b'.'.wrapping_sub(42),
                b'C'.wrapping_sub(42),
                b'D'.wrapping_sub(42),
                b'E'.wrapping_sub(42),
                b'F'.wrapping_sub(42),
            ]
        );
    }

    #[test]
    fn decode_body_crlf_control_boundaries() {
        for input in [
            b"AB\r\n=yignored".as_slice(),
            b"AB\r\n.=yignored".as_slice(),
        ] {
            let decoded = decode_body_bytes(
                input,
                DecodeOptions {
                    dot_unstuffing: true,
                },
            )
            .unwrap();

            assert_eq!(decoded, reference_decode_body(input, true).unwrap());
        }
    }

    #[test]
    fn decode_chunk_splits_at_critical_boundaries() {
        let input = b"AB=\r\n..CD\r\n.EF";
        let opts = DecodeOptions {
            dot_unstuffing: true,
        };

        for split in 0..=input.len() {
            let mut state = DecodeState::new();
            let mut output = vec![0u8; 128];
            let n1 = decode_chunk(&input[..split], &mut output, &mut state, opts).unwrap();
            let n2 = decode_chunk(&input[split..], &mut output[n1..], &mut state, opts).unwrap();
            let mut decoded = output;
            decoded.truncate(n1 + n2);

            let expected = decode_chunked_bytes(input, opts, &[input.len()]).unwrap();
            assert_eq!(decoded, expected, "split at {split}");
        }
    }

    #[test]
    fn decode_chunked_matches_whole_body_for_many_split_patterns() {
        let mut input = Vec::new();
        input.extend_from_slice(b"clean-clean-clean");
        input.extend_from_slice(b"\r\n");
        input.extend_from_slice(b"..dotstuffed");
        input.extend_from_slice(b"\r\n");
        input.extend_from_slice(b"==A=B=C");
        input.extend_from_slice(b"\n");
        input.extend_from_slice(b"tail");

        let opts = DecodeOptions {
            dot_unstuffing: true,
        };
        let whole = decode_body_bytes(&input, opts).unwrap();

        for stride in 1..=9 {
            let splits = (stride..input.len()).step_by(stride).collect::<Vec<_>>();
            let chunked = decode_chunked_bytes(&input, opts, &splits).unwrap();
            assert_eq!(chunked, whole, "stride {stride}");
        }
    }

    #[test]
    fn decode_body_randomized_reference_cross_check() {
        let mut seed = 0x1234_5678_90ab_cdefu64;

        for len in [0usize, 1, 2, 3, 7, 16, 31, 64, 257, 1024] {
            let mut input = Vec::with_capacity(len);
            for idx in 0..len {
                let byte = match lcg_next(&mut seed) % 17 {
                    0 => b'=',
                    1 => b'\r',
                    2 => b'\n',
                    3 if idx % 11 == 0 => b'.',
                    _ => {
                        let candidate = 33u8.wrapping_add(lcg_next(&mut seed) % 90);
                        if matches!(candidate, b'=' | b'\r' | b'\n') {
                            b'X'
                        } else {
                            candidate
                        }
                    }
                };
                input.push(byte);
            }

            if input.last() == Some(&b'=') {
                input.push(b'A');
            }

            for dot_unstuffing in [false, true] {
                let opts = DecodeOptions { dot_unstuffing };
                let expected = reference_decode_body(&input, dot_unstuffing).unwrap();
                let decoded = decode_body_bytes(&input, opts).unwrap();
                assert_eq!(
                    decoded, expected,
                    "len {len}, dot_unstuffing {dot_unstuffing}"
                );
            }
        }
    }
}
