//! SIMD and lookup-table accelerated yEnc decoding.
//!
//! The decoder keeps the public streaming state in `decode.rs`, but normalizes it
//! here so the same fast path can serve full-body and chunked decode.
//!
//! The SIMD/state-machine design is ported from rapidyenc commit 27f435a
//! (Public Domain/CC0), especially `decoder.cc`, `decoder_common.h`,
//! `decoder_avx2_base.h`, and `decoder_neon64.cc`.

use crate::decode::{DecodeState, RapidyencDecodeEnd, RapidyencDecodeState};
use crate::error::YencError;

#[cfg(target_arch = "x86_64")]
type DecodeRunFn = fn(&[u8], usize, &mut [u8], usize) -> (usize, usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecoderState {
    CrLf,
    Eq,
    Cr,
    None,
    CrLfDot,
    CrLfDotCr,
    CrLfEq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeEnd {
    None,
    Article,
    Control,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KernelState {
    state: DecoderState,
    end: DecodeEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KernelOutcome {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: RapidyencDecodeEnd,
}

#[derive(Debug, Clone, Copy)]
struct DecodeStepMode {
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
}

impl From<DecodeEnd> for RapidyencDecodeEnd {
    fn from(end: DecodeEnd) -> Self {
        match end {
            DecodeEnd::None => Self::None,
            DecodeEnd::Article => Self::Article,
            DecodeEnd::Control => Self::Control,
        }
    }
}

impl KernelState {
    fn body() -> Self {
        Self {
            state: DecoderState::CrLf,
            end: DecodeEnd::None,
        }
    }

    fn from_decode_state(state: &DecodeState) -> Self {
        let decoder_state = if state.cr_pending && state.dot_pending {
            DecoderState::CrLfDotCr
        } else if state.cr_pending {
            DecoderState::Cr
        } else if state.escape_pending && state.at_line_start {
            DecoderState::CrLfEq
        } else if state.escape_pending {
            DecoderState::Eq
        } else if state.dot_pending {
            DecoderState::CrLfDot
        } else if state.at_line_start {
            DecoderState::CrLf
        } else {
            DecoderState::None
        };

        Self {
            state: decoder_state,
            end: DecodeEnd::None,
        }
    }

    fn from_rapidyenc_state(state: RapidyencDecodeState) -> Self {
        let state = match state {
            RapidyencDecodeState::CrLf => DecoderState::CrLf,
            RapidyencDecodeState::Eq => DecoderState::Eq,
            RapidyencDecodeState::Cr => DecoderState::Cr,
            RapidyencDecodeState::None => DecoderState::None,
            RapidyencDecodeState::CrLfDot => DecoderState::CrLfDot,
            RapidyencDecodeState::CrLfDotCr => DecoderState::CrLfDotCr,
            RapidyencDecodeState::CrLfEq => DecoderState::CrLfEq,
        };
        Self {
            state,
            end: DecodeEnd::None,
        }
    }

    fn write_back(self, state: &mut DecodeState) {
        state.escape_pending = matches!(self.state, DecoderState::Eq | DecoderState::CrLfEq);
        state.dot_pending = matches!(self.state, DecoderState::CrLfDot | DecoderState::CrLfDotCr);
        state.cr_pending = matches!(self.state, DecoderState::Cr | DecoderState::CrLfDotCr);
        state.at_line_start = matches!(
            self.state,
            DecoderState::CrLf
                | DecoderState::CrLfDot
                | DecoderState::CrLfDotCr
                | DecoderState::CrLfEq
        );
    }

    fn write_back_rapidyenc(self, state: &mut RapidyencDecodeState) {
        *state = match self.state {
            DecoderState::CrLf => RapidyencDecodeState::CrLf,
            DecoderState::Eq => RapidyencDecodeState::Eq,
            DecoderState::Cr => RapidyencDecodeState::Cr,
            DecoderState::None => RapidyencDecodeState::None,
            DecoderState::CrLfDot => RapidyencDecodeState::CrLfDot,
            DecoderState::CrLfDotCr => RapidyencDecodeState::CrLfDotCr,
            DecoderState::CrLfEq => RapidyencDecodeState::CrLfEq,
        };
    }
}

/// Decode a complete body buffer with the SIMD-capable internal kernel.
pub(crate) fn decode_body_into(
    input: &[u8],
    output: &mut [u8],
    dot_unstuffing: bool,
) -> Result<usize, YencError> {
    let mut state = KernelState::body();
    Ok(decode_kernel(input, output, &mut state, dot_unstuffing, false, false)?.written)
}

/// Decode one streaming body chunk with carry state preserved across calls.
pub(crate) fn decode_chunk_into(
    input: &[u8],
    output: &mut [u8],
    state: &mut DecodeState,
    dot_unstuffing: bool,
) -> Result<usize, YencError> {
    let mut kernel_state = KernelState::from_decode_state(state);
    let written = decode_kernel(
        input,
        output,
        &mut kernel_state,
        dot_unstuffing,
        true,
        false,
    )?
    .written;
    kernel_state.write_back(state);
    Ok(written)
}

pub(crate) fn decode_chunk_until_end_into(
    input: &[u8],
    output: &mut [u8],
    state: &mut DecodeState,
    dot_unstuffing: bool,
) -> Result<KernelOutcome, YencError> {
    let mut kernel_state = KernelState::from_decode_state(state);
    let outcome = decode_kernel(input, output, &mut kernel_state, dot_unstuffing, true, true)?;
    kernel_state.write_back(state);
    Ok(outcome)
}

/// Decode with rapidyenc's `decode_ex` semantics.
pub(crate) fn decode_rapidyenc_into(
    input: &[u8],
    output: &mut [u8],
    is_raw: bool,
    state: &mut RapidyencDecodeState,
) -> Result<usize, YencError> {
    if input.is_empty() {
        return Ok(0);
    }

    let initial_state = match (is_raw, *state) {
        (false, RapidyencDecodeState::Eq) => RapidyencDecodeState::Eq,
        (false, _) => RapidyencDecodeState::None,
        (
            true,
            RapidyencDecodeState::CrLfDot
            | RapidyencDecodeState::CrLfDotCr
            | RapidyencDecodeState::CrLfEq,
        ) => RapidyencDecodeState::None,
        (true, state) => state,
    };
    let suppress_pending_raw_cr =
        is_raw && initial_state == RapidyencDecodeState::Eq && matches!(input, b"\r" | b"\r\n");
    let mut kernel_state = KernelState::from_rapidyenc_state(initial_state);
    let mut written = decode_kernel(input, output, &mut kernel_state, is_raw, true, false)?.written;
    if suppress_pending_raw_cr {
        written = 0;
    }
    if is_raw {
        *state = match kernel_state.state {
            DecoderState::CrLfEq => RapidyencDecodeState::Eq,
            DecoderState::CrLfDot => RapidyencDecodeState::None,
            _ => {
                kernel_state.write_back_rapidyenc(state);
                *state
            }
        };
    } else {
        *state = if kernel_state.state == DecoderState::Eq {
            RapidyencDecodeState::Eq
        } else {
            RapidyencDecodeState::None
        };
    }
    Ok(written)
}

/// Decode with rapidyenc's `decode_incremental` semantics.
pub(crate) fn decode_rapidyenc_incremental_into(
    input: &[u8],
    output: &mut [u8],
    state: &mut RapidyencDecodeState,
) -> Result<KernelOutcome, YencError> {
    let mut kernel_state = KernelState::from_rapidyenc_state(*state);
    let outcome = decode_kernel(input, output, &mut kernel_state, true, true, true)?;
    kernel_state.write_back_rapidyenc(state);
    Ok(outcome)
}

fn decode_kernel(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512vbmi2")
            && is_x86_feature_detected!("avx512vl")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx2")
        {
            return unsafe {
                decode_kernel_avx512_vbmi2(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("avx2") {
            return unsafe {
                decode_kernel_avx2(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("popcnt")
            && is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("ssse3")
            && !x86_prefers_ssse3_over_sse41()
            && !x86_prefers_sse2_over_ssse3()
        {
            return unsafe {
                decode_kernel_avx(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("ssse3")
            && !x86_prefers_ssse3_over_sse41()
            && !x86_prefers_sse2_over_ssse3()
        {
            return unsafe {
                decode_kernel_sse41(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("ssse3") && !x86_prefers_sse2_over_ssse3() {
            return unsafe {
                decode_kernel_ssse3(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("sse2") {
            return unsafe {
                decode_kernel_sse2(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        unsafe {
            decode_kernel_neon(
                input,
                output,
                state,
                dot_unstuffing,
                preserve_pending,
                search_end,
            )
        }
    }

    #[cfg(all(target_arch = "arm", target_feature = "neon"))]
    {
        return unsafe {
            decode_kernel_arm_neon(
                input,
                output,
                state,
                dot_unstuffing,
                preserve_pending,
                search_end,
            )
        };
    }

    #[cfg(not(any(
        target_arch = "aarch64",
        all(target_arch = "arm", target_feature = "neon")
    )))]
    {
        decode_kernel_scalar(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
        )
    }
}

#[cfg(any(
    test,
    not(any(
        target_arch = "aarch64",
        all(target_arch = "arm", target_feature = "neon")
    ))
))]
fn decode_kernel_scalar(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    let mut src = 0usize;
    let mut dst = 0usize;
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };

    while (!search_end || state.end == DecodeEnd::None) && src < input.len() {
        if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
            break;
        }
    }

    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

fn decode_scalar_step(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<bool, YencError> {
    let DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    } = mode;

    loop {
        if *src >= input.len() {
            return Ok(false);
        }

        match state.state {
            DecoderState::Eq => {
                let byte = input[*src];
                write_decoded(output, dst, byte.wrapping_sub(106))?;
                *src += 1;
                state.state = if dot_unstuffing && byte == b'\r' {
                    DecoderState::Cr
                } else {
                    DecoderState::None
                };
                return Ok(true);
            }
            DecoderState::CrLfEq => {
                let byte = input[*src];
                if search_end && dot_unstuffing && byte == b'y' {
                    *src += 1;
                    state.state = DecoderState::None;
                    state.end = DecodeEnd::Control;
                    return Ok(false);
                }
                write_decoded(output, dst, byte.wrapping_sub(106))?;
                *src += 1;
                state.state = if dot_unstuffing && byte == b'\r' {
                    DecoderState::Cr
                } else {
                    DecoderState::None
                };
                return Ok(true);
            }
            DecoderState::Cr => {
                if input[*src] == b'\n' {
                    *src += 1;
                    state.state = DecoderState::CrLf;
                    return Ok(true);
                }
                state.state = DecoderState::None;
            }
            DecoderState::CrLfDot => {
                let byte = input[*src];
                match byte {
                    b'\r' => {
                        *src += 1;
                        state.state = if search_end {
                            DecoderState::CrLfDotCr
                        } else {
                            DecoderState::Cr
                        };
                        return Ok(true);
                    }
                    b'\n' => {
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                    b'=' if dot_unstuffing && search_end => {
                        *src += 1;
                        state.state = DecoderState::CrLfEq;
                        return Ok(true);
                    }
                    b'=' => {
                        *src += 1;
                        if *src >= input.len() {
                            if preserve_pending {
                                state.state = DecoderState::Eq;
                                return Ok(false);
                            }
                            return Err(YencError::MalformedEscape(src.saturating_sub(1)));
                        }
                        let escaped = input[*src];
                        write_decoded(output, dst, escaped.wrapping_sub(106))?;
                        *src += 1;
                        state.state = if dot_unstuffing && escaped == b'\r' {
                            DecoderState::Cr
                        } else {
                            DecoderState::None
                        };
                        return Ok(true);
                    }
                    b'.' => {
                        write_decoded(output, dst, b'.'.wrapping_sub(42))?;
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                    _ => {
                        state.state = DecoderState::None;
                    }
                }
            }
            DecoderState::CrLfDotCr => {
                if input[*src] == b'\n' {
                    *src += 1;
                    state.state = DecoderState::CrLf;
                    if search_end {
                        state.end = DecodeEnd::Article;
                        return Ok(false);
                    }
                    return Ok(true);
                }
                state.state = DecoderState::None;
            }
            DecoderState::CrLf | DecoderState::None => {
                let at_line_start = state.state == DecoderState::CrLf;
                let byte = input[*src];

                if dot_unstuffing && at_line_start && byte == b'.' {
                    *src += 1;
                    state.state = DecoderState::CrLfDot;
                    return Ok(true);
                }

                match byte {
                    b'\r' => {
                        *src += 1;
                        state.state = DecoderState::Cr;
                        return Ok(true);
                    }
                    b'\n' => {
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                    b'=' => {
                        *src += 1;
                        if *src >= input.len() {
                            if preserve_pending {
                                state.state = if dot_unstuffing && at_line_start {
                                    DecoderState::CrLfEq
                                } else {
                                    DecoderState::Eq
                                };
                                return Ok(false);
                            }
                            return Err(YencError::MalformedEscape(src.saturating_sub(1)));
                        }
                        if search_end && dot_unstuffing && at_line_start && input[*src] == b'y' {
                            *src += 1;
                            state.state = DecoderState::None;
                            state.end = DecodeEnd::Control;
                            return Ok(false);
                        }
                        let escaped = input[*src];
                        write_decoded(output, dst, escaped.wrapping_sub(106))?;
                        *src += 1;
                        state.state = if dot_unstuffing && escaped == b'\r' {
                            DecoderState::Cr
                        } else {
                            DecoderState::None
                        };
                        return Ok(true);
                    }
                    _ => {
                        write_decoded(output, dst, byte.wrapping_sub(42))?;
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                }
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn decode_kernel_sse2(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_sse2_block,
        )
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn decode_kernel_ssse3(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_ssse3_block,
        )
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1,ssse3")]
unsafe fn decode_kernel_sse41(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_sse41_block,
        )
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,popcnt,sse4.1,ssse3")]
unsafe fn decode_kernel_avx(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_avx_block,
        )
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn decode_kernel_avx2(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_avx2_block,
        )
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2")]
unsafe fn decode_kernel_avx512_vbmi2(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_avx512_vbmi2_block,
        )
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn try_decode_sse2_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::x86_64::*;

    if input.len().saturating_sub(src) < 64 || output.len().saturating_sub(*dst) < 64 {
        return Ok(None);
    }

    if !matches!(state.state, DecoderState::None | DecoderState::CrLf) {
        return Ok(None);
    }

    let a = unsafe { _mm_loadu_si128(input.as_ptr().add(src) as *const __m128i) };
    let b = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 16) as *const __m128i) };
    let c = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 32) as *const __m128i) };
    let d = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 48) as *const __m128i) };
    let eq = unsafe { sse2_mask64([a, b, c, d], b'=') };
    let cr = unsafe { sse2_mask64([a, b, c, d], b'\r') };
    let lf = unsafe { sse2_mask64([a, b, c, d], b'\n') };
    let dot = unsafe { sse2_mask64([a, b, c, d], b'.') };

    let fixed_eq = fix_eq_mask(eq, eq << 1);
    if fixed_eq & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let escaped = fixed_eq << 1;
    let raw_cr = cr & !escaped;
    let raw_lf = lf & !escaped;
    let raw_breaks = raw_cr | raw_lf;
    // NNTP line boundaries exist in the raw stream even when yEnc escaped the
    // '\r' (the scalar machine re-enters Cr after an escaped CR when
    // dot-unstuffing), so pair detection uses the unmasked '\r' bits.
    let pair_cr = if dot_unstuffing { cr } else { raw_cr };
    let crlf = pair_cr & (lf >> 1);
    let line_start = if state.state == DecoderState::CrLf {
        1
    } else {
        0
    } | (crlf << 2);
    let dot_start = if dot_unstuffing {
        dot & !escaped & line_start
    } else {
        0
    };

    if dot_start & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || line_start_eq != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());

    if skip == 0 {
        unsafe {
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst) as *mut __m128i,
                _mm_add_epi8(a, sub42),
            );
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst + 16) as *mut __m128i,
                _mm_add_epi8(b, sub42),
            );
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst + 32) as *mut __m128i,
                _mm_add_epi8(c, sub42),
            );
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst + 48) as *mut __m128i,
                _mm_add_epi8(d, sub42),
            );
        }
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(Some(64));
    }

    let keep = 64 - skip.count_ones() as usize;
    if output.len().saturating_sub(*dst) < keep {
        return Err(YencError::BufferTooSmall {
            needed: *dst + keep,
            available: output.len(),
        });
    }

    for lane in 0..64usize {
        if skip & (1u64 << lane) == 0 {
            let byte = input[src + lane];
            output[*dst] = if escaped & (1u64 << lane) != 0 {
                byte.wrapping_sub(106)
            } else {
                byte.wrapping_sub(42)
            };
            *dst += 1;
        }
    }

    let mut next_state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
    if dot_unstuffing && (cr & escaped) & (1 << 63) != 0 {
        // Escaped CR at the window's last byte still opens an NNTP line
        // boundary for dot-stuffing purposes.
        next_state = DecoderState::Cr;
    }
    state.state = next_state;
    Ok(Some(64))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn try_decode_ssse3_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::x86_64::*;

    if input.len().saturating_sub(src) < 64 || output.len().saturating_sub(*dst) < 64 {
        return Ok(None);
    }

    if !matches!(state.state, DecoderState::None | DecoderState::CrLf) {
        return Ok(None);
    }

    let a = unsafe { _mm_loadu_si128(input.as_ptr().add(src) as *const __m128i) };
    let b = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 16) as *const __m128i) };
    let c = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 32) as *const __m128i) };
    let d = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 48) as *const __m128i) };
    let eq = unsafe { sse2_mask64([a, b, c, d], b'=') };
    let cr = unsafe { sse2_mask64([a, b, c, d], b'\r') };
    let lf = unsafe { sse2_mask64([a, b, c, d], b'\n') };
    let dot = unsafe { sse2_mask64([a, b, c, d], b'.') };

    let fixed_eq = fix_eq_mask(eq, eq << 1);
    if fixed_eq & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let escaped = fixed_eq << 1;
    let raw_cr = cr & !escaped;
    let raw_lf = lf & !escaped;
    let raw_breaks = raw_cr | raw_lf;
    // NNTP line boundaries exist in the raw stream even when yEnc escaped the
    // '\r' (the scalar machine re-enters Cr after an escaped CR when
    // dot-unstuffing), so pair detection uses the unmasked '\r' bits.
    let pair_cr = if dot_unstuffing { cr } else { raw_cr };
    let crlf = pair_cr & (lf >> 1);
    let line_start = if state.state == DecoderState::CrLf {
        1
    } else {
        0
    } | (crlf << 2);
    let dot_start = if dot_unstuffing {
        dot & !escaped & line_start
    } else {
        0
    };

    if dot_start & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || line_start_eq != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());

    if skip == 0 {
        unsafe {
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst) as *mut __m128i,
                _mm_add_epi8(a, sub42),
            );
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst + 16) as *mut __m128i,
                _mm_add_epi8(b, sub42),
            );
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst + 32) as *mut __m128i,
                _mm_add_epi8(c, sub42),
            );
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst + 48) as *mut __m128i,
                _mm_add_epi8(d, sub42),
            );
        }
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(Some(64));
    }

    let decoded_a = unsafe { ssse3_decode_with_escape_mask(a, (escaped & 0xffff) as u16) };
    let decoded_b = unsafe { ssse3_decode_with_escape_mask(b, ((escaped >> 16) & 0xffff) as u16) };
    let decoded_c = unsafe { ssse3_decode_with_escape_mask(c, ((escaped >> 32) & 0xffff) as u16) };
    let decoded_d = unsafe { ssse3_decode_with_escape_mask(d, ((escaped >> 48) & 0xffff) as u16) };

    unsafe { compact_store_16_ssse3(decoded_a, (skip & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16_ssse3(decoded_b, ((skip >> 16) & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16_ssse3(decoded_c, ((skip >> 32) & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16_ssse3(decoded_d, ((skip >> 48) & 0xffff) as u16, output, dst)? };

    let mut next_state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
    if dot_unstuffing && (cr & escaped) & (1 << 63) != 0 {
        // Escaped CR at the window's last byte still opens an NNTP line
        // boundary for dot-stuffing purposes.
        next_state = DecoderState::Cr;
    }
    state.state = next_state;
    Ok(Some(64))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1,ssse3")]
unsafe fn try_decode_sse41_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::x86_64::*;

    if input.len().saturating_sub(src) < 64 || output.len().saturating_sub(*dst) < 64 {
        return Ok(None);
    }

    if !matches!(state.state, DecoderState::None | DecoderState::CrLf) {
        return Ok(None);
    }

    let a = unsafe { _mm_loadu_si128(input.as_ptr().add(src) as *const __m128i) };
    let b = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 16) as *const __m128i) };
    let c = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 32) as *const __m128i) };
    let d = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 48) as *const __m128i) };
    let eq = unsafe { sse2_mask64([a, b, c, d], b'=') };
    let cr = unsafe { sse2_mask64([a, b, c, d], b'\r') };
    let lf = unsafe { sse2_mask64([a, b, c, d], b'\n') };
    let dot = unsafe { sse2_mask64([a, b, c, d], b'.') };

    let fixed_eq = fix_eq_mask(eq, eq << 1);
    if fixed_eq & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let escaped = fixed_eq << 1;
    let raw_cr = cr & !escaped;
    let raw_lf = lf & !escaped;
    let raw_breaks = raw_cr | raw_lf;
    // NNTP line boundaries exist in the raw stream even when yEnc escaped the
    // '\r' (the scalar machine re-enters Cr after an escaped CR when
    // dot-unstuffing), so pair detection uses the unmasked '\r' bits.
    let pair_cr = if dot_unstuffing { cr } else { raw_cr };
    let crlf = pair_cr & (lf >> 1);
    let line_start = if state.state == DecoderState::CrLf {
        1
    } else {
        0
    } | (crlf << 2);
    let dot_start = if dot_unstuffing {
        dot & !escaped & line_start
    } else {
        0
    };

    if dot_start & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || line_start_eq != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());

    if skip == 0 {
        unsafe {
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst) as *mut __m128i,
                _mm_add_epi8(a, sub42),
            );
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst + 16) as *mut __m128i,
                _mm_add_epi8(b, sub42),
            );
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst + 32) as *mut __m128i,
                _mm_add_epi8(c, sub42),
            );
            _mm_storeu_si128(
                output.as_mut_ptr().add(*dst + 48) as *mut __m128i,
                _mm_add_epi8(d, sub42),
            );
        }
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(Some(64));
    }

    let decoded_a = unsafe { sse41_decode_with_escape_mask(a, (escaped & 0xffff) as u16) };
    let decoded_b = unsafe { sse41_decode_with_escape_mask(b, ((escaped >> 16) & 0xffff) as u16) };
    let decoded_c = unsafe { sse41_decode_with_escape_mask(c, ((escaped >> 32) & 0xffff) as u16) };
    let decoded_d = unsafe { sse41_decode_with_escape_mask(d, ((escaped >> 48) & 0xffff) as u16) };

    unsafe { compact_store_16_ssse3(decoded_a, (skip & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16_ssse3(decoded_b, ((skip >> 16) & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16_ssse3(decoded_c, ((skip >> 32) & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16_ssse3(decoded_d, ((skip >> 48) & 0xffff) as u16, output, dst)? };

    let mut next_state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
    if dot_unstuffing && (cr & escaped) & (1 << 63) != 0 {
        // Escaped CR at the window's last byte still opens an NNTP line
        // boundary for dot-stuffing purposes.
        next_state = DecoderState::Cr;
    }
    state.state = next_state;
    Ok(Some(64))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,popcnt,sse4.1,ssse3")]
unsafe fn try_decode_avx_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
) -> Result<Option<usize>, YencError> {
    // rapidyenc's AVX decoder reuses the SSE decode body with the SSE4/POPCNT
    // ISA level; keep that shape here instead of inventing a separate AVX1 body.
    unsafe { try_decode_sse41_block(input, src, output, dst, state, dot_unstuffing) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn try_decode_avx2_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::x86_64::*;

    if input.len().saturating_sub(src) < 64 || output.len().saturating_sub(*dst) < 64 {
        return Ok(None);
    }

    if !matches!(state.state, DecoderState::None | DecoderState::CrLf) {
        return Ok(None);
    }

    let a = unsafe { _mm256_loadu_si256(input.as_ptr().add(src) as *const __m256i) };
    let b = unsafe { _mm256_loadu_si256(input.as_ptr().add(src + 32) as *const __m256i) };
    let eq = unsafe { avx2_mask64(a, b, b'=') };
    let cr = unsafe { avx2_mask64(a, b, b'\r') };
    let lf = unsafe { avx2_mask64(a, b, b'\n') };
    let dot = unsafe { avx2_mask64(a, b, b'.') };

    let fixed_eq = fix_eq_mask(eq, eq << 1);
    if fixed_eq & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let escaped = fixed_eq << 1;
    let raw_cr = cr & !escaped;
    let raw_lf = lf & !escaped;
    let raw_breaks = raw_cr | raw_lf;
    // NNTP line boundaries exist in the raw stream even when yEnc escaped the
    // '\r' (the scalar machine re-enters Cr after an escaped CR when
    // dot-unstuffing), so pair detection uses the unmasked '\r' bits.
    let pair_cr = if dot_unstuffing { cr } else { raw_cr };
    let crlf = pair_cr & (lf >> 1);
    let line_start = if state.state == DecoderState::CrLf {
        1
    } else {
        0
    } | (crlf << 2);
    let dot_start = if dot_unstuffing {
        dot & !escaped & line_start
    } else {
        0
    };

    if dot_start & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || line_start_eq != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;

    if skip == 0 {
        let sub42 = _mm256_set1_epi8(42i8.wrapping_neg());
        let decoded_a = _mm256_add_epi8(a, sub42);
        let decoded_b = _mm256_add_epi8(b, sub42);
        unsafe {
            _mm256_storeu_si256(output.as_mut_ptr().add(*dst) as *mut __m256i, decoded_a);
            _mm256_storeu_si256(
                output.as_mut_ptr().add(*dst + 32) as *mut __m256i,
                decoded_b,
            );
        }
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(Some(64));
    }

    let (decoded_a, decoded_b) = unsafe { avx2_decode_with_escape_mask(a, b, escaped) };
    unsafe { compact_store_16_avx2(decoded_a, false, (skip & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16_avx2(decoded_a, true, ((skip >> 16) & 0xffff) as u16, output, dst)? };
    unsafe {
        compact_store_16_avx2(
            decoded_b,
            false,
            ((skip >> 32) & 0xffff) as u16,
            output,
            dst,
        )?
    };
    unsafe { compact_store_16_avx2(decoded_b, true, ((skip >> 48) & 0xffff) as u16, output, dst)? };

    let mut next_state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
    if dot_unstuffing && (cr & escaped) & (1 << 63) != 0 {
        // Escaped CR at the window's last byte still opens an NNTP line
        // boundary for dot-stuffing purposes.
        next_state = DecoderState::Cr;
    }
    state.state = next_state;
    Ok(Some(64))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2")]
unsafe fn try_decode_avx512_vbmi2_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::x86_64::*;

    if input.len().saturating_sub(src) < 64 || output.len().saturating_sub(*dst) < 64 {
        return Ok(None);
    }

    if !matches!(state.state, DecoderState::None | DecoderState::CrLf) {
        return Ok(None);
    }

    let a = unsafe { _mm256_loadu_si256(input.as_ptr().add(src) as *const __m256i) };
    let b = unsafe { _mm256_loadu_si256(input.as_ptr().add(src + 32) as *const __m256i) };
    let eq = unsafe { avx2_mask64(a, b, b'=') };
    let cr = unsafe { avx2_mask64(a, b, b'\r') };
    let lf = unsafe { avx2_mask64(a, b, b'\n') };
    let dot = unsafe { avx2_mask64(a, b, b'.') };

    let fixed_eq = fix_eq_mask(eq, eq << 1);
    if fixed_eq & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let escaped = fixed_eq << 1;
    let raw_cr = cr & !escaped;
    let raw_lf = lf & !escaped;
    let raw_breaks = raw_cr | raw_lf;
    // NNTP line boundaries exist in the raw stream even when yEnc escaped the
    // '\r' (the scalar machine re-enters Cr after an escaped CR when
    // dot-unstuffing), so pair detection uses the unmasked '\r' bits.
    let pair_cr = if dot_unstuffing { cr } else { raw_cr };
    let crlf = pair_cr & (lf >> 1);
    let line_start = if state.state == DecoderState::CrLf {
        1
    } else {
        0
    } | (crlf << 2);
    let dot_start = if dot_unstuffing {
        dot & !escaped & line_start
    } else {
        0
    };

    if dot_start & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || line_start_eq != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    let (decoded_a, decoded_b) = unsafe { avx2_decode_with_escape_mask(a, b, escaped) };

    if skip == 0 {
        unsafe {
            _mm256_storeu_si256(output.as_mut_ptr().add(*dst) as *mut __m256i, decoded_a);
            _mm256_storeu_si256(
                output.as_mut_ptr().add(*dst + 32) as *mut __m256i,
                decoded_b,
            );
        }
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(Some(64));
    }

    unsafe { compact_store_32_avx512_vbmi2(decoded_a, (!skip & 0xffff_ffff) as u32, output, dst)? };
    unsafe {
        compact_store_32_avx512_vbmi2(decoded_b, ((!skip >> 32) & 0xffff_ffff) as u32, output, dst)?
    };
    let mut next_state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
    if dot_unstuffing && (cr & escaped) & (1 << 63) != 0 {
        // Escaped CR at the window's last byte still opens an NNTP line
        // boundary for dot-stuffing purposes.
        next_state = DecoderState::Cr;
    }
    state.state = next_state;
    Ok(Some(64))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn sse2_mask64(vectors: [std::arch::x86_64::__m128i; 4], byte: u8) -> u64 {
    use std::arch::x86_64::*;

    let needle = _mm_set1_epi8(byte as i8);
    let a = _mm_movemask_epi8(_mm_cmpeq_epi8(vectors[0], needle)) as u16 as u64;
    let b = _mm_movemask_epi8(_mm_cmpeq_epi8(vectors[1], needle)) as u16 as u64;
    let c = _mm_movemask_epi8(_mm_cmpeq_epi8(vectors[2], needle)) as u16 as u64;
    let d = _mm_movemask_epi8(_mm_cmpeq_epi8(vectors[3], needle)) as u16 as u64;
    a | (b << 16) | (c << 32) | (d << 48)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_mask64(
    a: std::arch::x86_64::__m256i,
    b: std::arch::x86_64::__m256i,
    byte: u8,
) -> u64 {
    use std::arch::x86_64::*;

    let needle = _mm256_set1_epi8(byte as i8);
    let mask_a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, needle)) as u32 as u64;
    let mask_b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, needle)) as u32 as u64;
    mask_a | (mask_b << 32)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f")]
unsafe fn compact_store_32_avx512_vbmi2(
    decoded: std::arch::x86_64::__m256i,
    keep_mask: u32,
    output: &mut [u8],
    dst: &mut usize,
) -> Result<(), YencError> {
    use std::arch::x86_64::*;

    let keep = keep_mask.count_ones() as usize;
    if output.len().saturating_sub(*dst) < 32 {
        return Err(YencError::BufferTooSmall {
            needed: *dst + 32,
            available: output.len(),
        });
    }

    let packed = _mm256_maskz_compress_epi8(keep_mask, decoded);
    unsafe { _mm256_storeu_si256(output.as_mut_ptr().add(*dst) as *mut __m256i, packed) };
    *dst += keep;
    Ok(())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn ssse3_decode_with_escape_mask(
    block: std::arch::x86_64::__m128i,
    escaped: u16,
) -> std::arch::x86_64::__m128i {
    use std::arch::x86_64::*;

    let mut offsets = [42u8.wrapping_neg(); 16];
    for (lane, offset) in offsets.iter_mut().enumerate() {
        if escaped & (1u16 << lane) != 0 {
            *offset = 106u8.wrapping_neg();
        }
    }
    let offsets = unsafe { _mm_loadu_si128(offsets.as_ptr() as *const __m128i) };
    _mm_add_epi8(block, offsets)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn sse41_decode_with_escape_mask(
    block: std::arch::x86_64::__m128i,
    escaped: u16,
) -> std::arch::x86_64::__m128i {
    use std::arch::x86_64::*;

    let mut mask = [0u8; 16];
    for (lane, mask_byte) in mask.iter_mut().enumerate() {
        if escaped & (1u16 << lane) != 0 {
            *mask_byte = 0xff;
        }
    }
    let mask = unsafe { _mm_loadu_si128(mask.as_ptr() as *const __m128i) };
    let normal = _mm_set1_epi8(-42);
    let escaped_offset = _mm_set1_epi8(-106);
    _mm_add_epi8(block, _mm_blendv_epi8(normal, escaped_offset, mask))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
// Ported from rapidyenc's AVX2 maskEq-to-vector offset path.
unsafe fn avx2_decode_with_escape_mask(
    a: std::arch::x86_64::__m256i,
    b: std::arch::x86_64::__m256i,
    escaped: u64,
) -> (std::arch::x86_64::__m256i, std::arch::x86_64::__m256i) {
    use std::arch::x86_64::*;

    let mask_bits = _mm256_broadcastq_epi64(_mm_cvtsi64_si128(escaped as i64));
    let bit_lanes = _mm256_set1_epi64x(0x8040_2010_0804_0201u64 as i64);

    let mask_a = _mm256_shuffle_epi8(
        mask_bits,
        _mm256_set_epi32(
            0x0303_0303,
            0x0303_0303,
            0x0202_0202,
            0x0202_0202,
            0x0101_0101,
            0x0101_0101,
            0x0000_0000,
            0x0000_0000,
        ),
    );
    let mask_b = _mm256_shuffle_epi8(
        mask_bits,
        _mm256_set_epi32(
            0x0707_0707,
            0x0707_0707,
            0x0606_0606,
            0x0606_0606,
            0x0505_0505,
            0x0505_0505,
            0x0404_0404,
            0x0404_0404,
        ),
    );
    let mask_a = _mm256_cmpeq_epi8(_mm256_and_si256(mask_a, bit_lanes), bit_lanes);
    let mask_b = _mm256_cmpeq_epi8(_mm256_and_si256(mask_b, bit_lanes), bit_lanes);
    let normal = _mm256_set1_epi8(-42);
    let escaped_offset = _mm256_set1_epi8(-106);
    let decoded_a = _mm256_add_epi8(a, _mm256_blendv_epi8(normal, escaped_offset, mask_a));
    let decoded_b = _mm256_add_epi8(b, _mm256_blendv_epi8(normal, escaped_offset, mask_b));

    (decoded_a, decoded_b)
}

#[cfg(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    all(target_arch = "arm", target_feature = "neon")
))]
// Ported from rapidyenc's fix_eqMask bit hack for consecutive '=' runs.
fn fix_eq_mask(mask: u64, mask_shift1: u64) -> u64 {
    let start = mask & !mask_shift1;
    let even = 0x5555_5555_5555_5555u64;
    let odd_groups = mask.wrapping_add(start & even);
    (odd_groups ^ even) & mask
}

#[cfg(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    all(target_arch = "arm", target_feature = "neon")
))]
fn final_state_after_block(
    raw_breaks: u64,
    raw_cr: u64,
    crlf_lf: u64,
    output_mask: u64,
) -> DecoderState {
    if raw_breaks == 0 {
        return DecoderState::None;
    }

    let last_break = 63 - raw_breaks.leading_zeros() as usize;
    let later_output_mask = if last_break == 63 {
        0
    } else {
        output_mask & (!0u64 << (last_break + 1))
    };

    if later_output_mask != 0 {
        DecoderState::None
    } else if raw_cr & (1u64 << last_break) != 0 {
        DecoderState::Cr
    } else if crlf_lf & (1u64 << last_break) != 0 {
        DecoderState::CrLf
    } else {
        DecoderState::None
    }
}

#[cfg(target_arch = "aarch64")]
unsafe fn decode_kernel_neon(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    const WIDTH: usize = 64;

    let mut src = 0usize;
    let mut dst = 0usize;
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };

    // Match rapidyenc's long-span shape on AArch64: set up constants once,
    // then stay inside the NEON loop until scalar boundary handling is needed.
    let tail_buffer = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail_buffer);

    if input.len() > WIDTH * 2 {
        let ctx = Neon64Ctx {
            dot_unstuffing,
            search_end,
            constants: unsafe { Neon64Constants::new() },
            table: compact_table_16(),
        };
        while (!search_end || state.end == DecodeEnd::None) && src + WIDTH <= simd_limit {
            if !matches!(state.state, DecoderState::None | DecoderState::CrLf)
                || output.len().saturating_sub(dst) < WIDTH
            {
                if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
                    break;
                }
                continue;
            }

            match unsafe {
                decode_neon64_span_block(input, &mut src, output, &mut dst, state, &ctx)?
            } {
                SpanBlockOutcome::Consumed => {}
                SpanBlockOutcome::ScalarThrough(through) => {
                    // Consume the trigger byte with the scalar state machine
                    // so the next SIMD attempt starts past it instead of
                    // re-analyzing the same window once per scalar step.
                    while src <= through && (!search_end || state.end == DecodeEnd::None) {
                        if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
                            break;
                        }
                    }
                }
            }
        }
    }

    while (!search_end || state.end == DecodeEnd::None) && src < input.len() {
        if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
            break;
        }
    }

    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
unsafe fn decode_kernel_arm_neon(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    unsafe {
        decode_kernel_simd32(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_arm_neon_block,
        )
    }
}

#[cfg(target_arch = "x86_64")]
type DecodeBlock64 = unsafe fn(
    &[u8],
    usize,
    &mut [u8],
    &mut usize,
    &mut KernelState,
    bool,
) -> Result<Option<usize>, YencError>;

#[cfg(target_arch = "x86_64")]
unsafe fn decode_kernel_simd64(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
    block: DecodeBlock64,
) -> Result<KernelOutcome, YencError> {
    const WIDTH: usize = 64;

    let mut src = 0usize;
    let mut dst = 0usize;
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };

    // rapidyenc keeps enough trailing input for scalar epilogue handling so
    // cross-boundary CRLF, dot-stuffing, and end markers remain exact.
    let tail_buffer = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail_buffer);

    if input.len() > WIDTH * 2 {
        while (!search_end || state.end == DecodeEnd::None) && src + WIDTH <= simd_limit {
            if let Some(consumed) =
                unsafe { block(input, src, output, &mut dst, state, dot_unstuffing)? }
            {
                src += consumed;
                continue;
            }

            if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
                break;
            }
        }
    }

    while (!search_end || state.end == DecodeEnd::None) && src < input.len() {
        if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
            break;
        }
    }

    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
type DecodeBlock32 = unsafe fn(
    &[u8],
    usize,
    &mut [u8],
    &mut usize,
    &mut KernelState,
    bool,
) -> Result<Option<usize>, YencError>;

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
unsafe fn decode_kernel_simd32(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
    block: DecodeBlock32,
) -> Result<KernelOutcome, YencError> {
    const WIDTH: usize = 32;

    let mut src = 0usize;
    let mut dst = 0usize;
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };
    let tail_buffer = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail_buffer);

    if input.len() > WIDTH * 2 {
        while (!search_end || state.end == DecodeEnd::None) && src + WIDTH <= simd_limit {
            if let Some(consumed) =
                unsafe { block(input, src, output, &mut dst, state, dot_unstuffing)? }
            {
                src += consumed;
                continue;
            }

            if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
                break;
            }
        }
    }

    while (!search_end || state.end == DecodeEnd::None) && src < input.len() {
        if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
            break;
        }
    }

    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
unsafe fn try_decode_arm_neon_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::arm::*;

    if input.len().saturating_sub(src) < 32 || output.len().saturating_sub(*dst) < 32 {
        return Ok(None);
    }

    if !matches!(state.state, DecoderState::None | DecoderState::CrLf) {
        return Ok(None);
    }

    let a = unsafe { vld1q_u8(input.as_ptr().add(src)) };
    let b = unsafe { vld1q_u8(input.as_ptr().add(src + 16)) };
    let eq_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'=')) };
    let eq_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'=')) };
    let cr_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'\r')) };
    let cr_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'\r')) };
    let lf_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'\n')) };
    let lf_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'\n')) };
    let dot_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'.')) };
    let dot_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'.')) };
    let eq = unsafe { arm_neon_compare_mask32([eq_a, eq_b]) };
    let cr = unsafe { arm_neon_compare_mask32([cr_a, cr_b]) };
    let lf = unsafe { arm_neon_compare_mask32([lf_a, lf_b]) };
    let dot = unsafe { arm_neon_compare_mask32([dot_a, dot_b]) };

    let fixed_eq = fix_eq_mask(eq, eq << 1);
    if fixed_eq & (1u64 << 31) != 0 {
        return Ok(None);
    }

    let escaped = fixed_eq << 1;
    let raw_cr = cr & !escaped;
    let raw_lf = lf & !escaped;
    let raw_breaks = raw_cr | raw_lf;
    // NNTP line boundaries exist in the raw stream even when yEnc escaped the
    // '\r' (the scalar machine re-enters Cr after an escaped CR when
    // dot-unstuffing), so pair detection uses the unmasked '\r' bits.
    let pair_cr = if dot_unstuffing { cr } else { raw_cr };
    let crlf = pair_cr & (lf >> 1);
    let line_start = if state.state == DecoderState::CrLf {
        1
    } else {
        0
    } | (crlf << 2);
    let dot_start = if dot_unstuffing {
        dot & !escaped & line_start
    } else {
        0
    };

    if dot_start & (1u64 << 31) != 0 {
        return Ok(None);
    }

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || line_start_eq != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    let sub42 = unsafe { vdupq_n_u8(42u8.wrapping_neg()) };

    if skip == 0 {
        unsafe {
            vst1q_u8(output.as_mut_ptr().add(*dst), vaddq_u8(a, sub42));
            vst1q_u8(output.as_mut_ptr().add(*dst + 16), vaddq_u8(b, sub42));
        }
        *dst += 32;
        state.state = DecoderState::None;
        return Ok(Some(32));
    }

    let keep = 32 - skip.count_ones() as usize;
    if output.len().saturating_sub(*dst) < keep {
        return Err(YencError::BufferTooSmall {
            needed: *dst + keep,
            available: output.len(),
        });
    }

    let [decoded_a, decoded_b] = unsafe { arm_neon_decode_with_escape_mask([a, b], escaped) };
    unsafe { compact_store_8_arm_neon(vget_low_u8(decoded_a), (skip & 0xff) as u8, output, dst)? };
    unsafe {
        compact_store_8_arm_neon(
            vget_high_u8(decoded_a),
            ((skip >> 8) & 0xff) as u8,
            output,
            dst,
        )?
    };
    unsafe {
        compact_store_8_arm_neon(
            vget_low_u8(decoded_b),
            ((skip >> 16) & 0xff) as u8,
            output,
            dst,
        )?
    };
    unsafe {
        compact_store_8_arm_neon(
            vget_high_u8(decoded_b),
            ((skip >> 24) & 0xff) as u8,
            output,
            dst,
        )?
    };

    let output_mask = (!skip) & 0xffff_ffff;
    state.state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, output_mask);
    Ok(Some(32))
}

#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy)]
struct Neon64Constants {
    eq: std::arch::aarch64::uint8x16_t,
    cr: std::arch::aarch64::uint8x16_t,
    dot: std::arch::aarch64::uint8x16_t,
    // Table-lookup rows marking '\n' (10) and '\r' (13) so one vqtbx1q merges
    // the CR/LF compares into the '=' compare (rapidyenc's specials gate).
    crlf_table: std::arch::aarch64::uint8x16_t,
    bit_weights: std::arch::aarch64::uint8x16_t,
    selector: std::arch::aarch64::uint8x16_t,
    normal_offset: std::arch::aarch64::uint8x16_t,
    escaped_offset: std::arch::aarch64::uint8x16_t,
}

#[cfg(target_arch = "aarch64")]
impl Neon64Constants {
    #[inline(always)]
    unsafe fn new() -> Self {
        use std::arch::aarch64::*;

        Self {
            eq: unsafe { vdupq_n_u8(b'=') },
            cr: unsafe { vdupq_n_u8(b'\r') },
            dot: unsafe { vdupq_n_u8(b'.') },
            crlf_table: unsafe {
                vld1q_u8([0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 255, 0, 0].as_ptr())
            },
            bit_weights: unsafe {
                vld1q_u8([1u8, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128].as_ptr())
            },
            selector: unsafe {
                vld1q_u8([0u8, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1].as_ptr())
            },
            normal_offset: unsafe { vdupq_n_u8(42) },
            escaped_offset: unsafe { vdupq_n_u8(106) },
        }
    }
}

/// Result of one 64-byte SIMD block attempt.
#[cfg(target_arch = "aarch64")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpanBlockOutcome {
    /// The whole 64-byte window was consumed and decoded.
    Consumed,
    /// A control/terminator candidate needs the scalar state machine; the
    /// driver must consume through this absolute source index before
    /// re-entering SIMD so the trigger is behind the next window.
    ScalarThrough(usize),
}

/// Immutable per-kernel-call context for the 64-byte NEON block.
#[cfg(target_arch = "aarch64")]
struct Neon64Ctx<'a> {
    dot_unstuffing: bool,
    search_end: bool,
    constants: Neon64Constants,
    table: &'a [[u8; 16]; 32768],
}

/// Per-16-bit-group keep counts for a 64-bit skip mask, via one SWAR popcount
/// pass (stays in the scalar domain where the mask already lives).
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn per_group_keeps(skip: u64) -> (usize, usize, usize, usize) {
    let x = skip - ((skip >> 1) & 0x5555_5555_5555_5555);
    let x = (x & 0x3333_3333_3333_3333) + ((x >> 2) & 0x3333_3333_3333_3333);
    let x = (x + (x >> 4)) & 0x0f0f_0f0f_0f0f_0f0f;
    let sums = x + (x >> 8);
    (
        16 - (sums & 0x1f) as usize,
        16 - ((sums >> 16) & 0x1f) as usize,
        16 - ((sums >> 32) & 0x1f) as usize,
        16 - ((sums >> 48) & 0x1f) as usize,
    )
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn decode_neon64_span_block(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    ctx: &Neon64Ctx<'_>,
) -> Result<SpanBlockOutcome, YencError> {
    use std::arch::aarch64::*;

    let dot_unstuffing = ctx.dot_unstuffing;
    let search_end = ctx.search_end;
    let constants = &ctx.constants;
    let table = ctx.table;

    let block_src = *src;
    debug_assert!(input.len().saturating_sub(block_src) > 64);
    debug_assert!(output.len().saturating_sub(*dst) >= 64);

    // A line-start dot at the window's first byte is invisible to the
    // specials gate below; resolve its lookahead here. Terminator/control
    // shapes go to the scalar state machine, a plain stuffed dot is recorded
    // for the vector paths.
    let dot0 = dot_unstuffing && state.state == DecoderState::CrLf && input[block_src] == b'.';
    if dot0 {
        let next = input[block_src + 1];
        if next == b'\r' || next == b'\n' || next == b'=' {
            return Ok(SpanBlockOutcome::ScalarThrough(block_src));
        }
    }

    let a = unsafe { vld1q_u8(input.as_ptr().add(block_src)) };
    let b = unsafe { vld1q_u8(input.as_ptr().add(block_src + 16)) };
    let c = unsafe { vld1q_u8(input.as_ptr().add(block_src + 32)) };
    let d = unsafe { vld1q_u8(input.as_ptr().add(block_src + 48)) };
    let eq_a = unsafe { vceqq_u8(a, constants.eq) };
    let eq_b = unsafe { vceqq_u8(b, constants.eq) };
    let eq_c = unsafe { vceqq_u8(c, constants.eq) };
    let eq_d = unsafe { vceqq_u8(d, constants.eq) };
    // One table lookup folds the '\r'/'\n' compares into the '=' compare, so
    // "does this window contain any special byte?" costs a single reduce.
    let cmp_a = unsafe { vqtbx1q_u8(eq_a, constants.crlf_table, a) };
    let cmp_b = unsafe { vqtbx1q_u8(eq_b, constants.crlf_table, b) };
    let cmp_c = unsafe { vqtbx1q_u8(eq_c, constants.crlf_table, c) };
    let cmp_d = unsafe { vqtbx1q_u8(eq_d, constants.crlf_table, d) };
    let any = unsafe { vorrq_u8(vorrq_u8(cmp_a, cmp_b), vorrq_u8(cmp_c, cmp_d)) };
    let has_specials = unsafe { vmaxvq_u8(any) } != 0;

    if !has_specials && !dot0 {
        unsafe {
            vst1q_u8(
                output.as_mut_ptr().add(*dst),
                vsubq_u8(a, constants.normal_offset),
            );
            vst1q_u8(
                output.as_mut_ptr().add(*dst + 16),
                vsubq_u8(b, constants.normal_offset),
            );
            vst1q_u8(
                output.as_mut_ptr().add(*dst + 32),
                vsubq_u8(c, constants.normal_offset),
            );
            vst1q_u8(
                output.as_mut_ptr().add(*dst + 48),
                vsubq_u8(d, constants.normal_offset),
            );
        }
        *src += 64;
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(SpanBlockOutcome::Consumed);
    }

    // Fold the specials mask and the '=' mask in one combined reduction:
    // lane 0 of `merged` holds the specials bits, lane 1 the '=' bits.
    let (mask, eq) = if has_specials {
        let merged = unsafe {
            vpaddq_u8(
                vpaddq_u8(
                    vpaddq_u8(
                        vandq_u8(cmp_a, constants.bit_weights),
                        vandq_u8(cmp_b, constants.bit_weights),
                    ),
                    vpaddq_u8(
                        vandq_u8(cmp_c, constants.bit_weights),
                        vandq_u8(cmp_d, constants.bit_weights),
                    ),
                ),
                vpaddq_u8(
                    vpaddq_u8(
                        vandq_u8(eq_a, constants.bit_weights),
                        vandq_u8(eq_b, constants.bit_weights),
                    ),
                    vpaddq_u8(
                        vandq_u8(eq_c, constants.bit_weights),
                        vandq_u8(eq_d, constants.bit_weights),
                    ),
                ),
            )
        };
        (
            unsafe { vgetq_lane_u64::<0>(vreinterpretq_u64_u8(merged)) },
            unsafe { vgetq_lane_u64::<1>(vreinterpretq_u64_u8(merged)) },
        )
    } else {
        (0, 0)
    };

    let fixed_eq = fix_eq_mask(eq, eq << 1);
    let escaped = fixed_eq << 1;

    let entry_line_start = (state.state == DecoderState::CrLf) as u64;
    let (raw_cr, escaped_cr, raw_breaks, crlf, line_start, dot_start);
    if mask == eq {
        // No line breaks in the window; the only possible line start (and
        // stripped dot) is at bit 0, carried in through the entry state.
        raw_cr = 0;
        escaped_cr = 0;
        raw_breaks = 0;
        crlf = 0;
        line_start = entry_line_start;
        dot_start = dot0 as u64;
    } else {
        // Breaks present: fold '\r' and '.' the same combined way and derive
        // '\n' from the specials mask.
        let cr_a = unsafe { vceqq_u8(a, constants.cr) };
        let cr_b = unsafe { vceqq_u8(b, constants.cr) };
        let cr_c = unsafe { vceqq_u8(c, constants.cr) };
        let cr_d = unsafe { vceqq_u8(d, constants.cr) };
        let dot_a = unsafe { vceqq_u8(a, constants.dot) };
        let dot_b = unsafe { vceqq_u8(b, constants.dot) };
        let dot_c = unsafe { vceqq_u8(c, constants.dot) };
        let dot_d = unsafe { vceqq_u8(d, constants.dot) };
        let merged = unsafe {
            vpaddq_u8(
                vpaddq_u8(
                    vpaddq_u8(
                        vandq_u8(cr_a, constants.bit_weights),
                        vandq_u8(cr_b, constants.bit_weights),
                    ),
                    vpaddq_u8(
                        vandq_u8(cr_c, constants.bit_weights),
                        vandq_u8(cr_d, constants.bit_weights),
                    ),
                ),
                vpaddq_u8(
                    vpaddq_u8(
                        vandq_u8(dot_a, constants.bit_weights),
                        vandq_u8(dot_b, constants.bit_weights),
                    ),
                    vpaddq_u8(
                        vandq_u8(dot_c, constants.bit_weights),
                        vandq_u8(dot_d, constants.bit_weights),
                    ),
                ),
            )
        };
        let cr = unsafe { vgetq_lane_u64::<0>(vreinterpretq_u64_u8(merged)) };
        let dot_mask = unsafe { vgetq_lane_u64::<1>(vreinterpretq_u64_u8(merged)) };
        let lf = mask & !eq & !cr;
        raw_cr = cr & !escaped;
        escaped_cr = cr & escaped;
        let raw_lf = lf & !escaped;
        raw_breaks = raw_cr | raw_lf;
        // NNTP line boundaries exist in the raw stream even when yEnc escaped
        // the '\r' (the scalar machine re-enters Cr after an escaped CR when
        // dot-unstuffing), so pair detection uses the unmasked '\r' bits.
        let pair_cr = if dot_unstuffing { cr } else { raw_cr };
        crlf = pair_cr & (lf >> 1);
        line_start = entry_line_start | (crlf << 2);
        dot_start = if dot_unstuffing {
            (dot_mask & !escaped & line_start) | dot0 as u64
        } else {
            0
        };
    }

    // '=' at a line start is a potential control line ("=y…"). Confirm with a
    // one-byte lookahead and fall back only for a real control line (once per
    // article, at the =yend trailer).
    if search_end && dot_unstuffing {
        let mut line_start_eq = fixed_eq & line_start;
        while line_start_eq != 0 {
            let bit = line_start_eq.trailing_zeros() as usize;
            if input[block_src + bit + 1] == b'y' {
                return Ok(SpanBlockOutcome::ScalarThrough(block_src + bit));
            }
            line_start_eq &= line_start_eq - 1;
        }
    }

    if dot_start != 0 {
        // A line-start dot immediately before a break or '=' needs
        // terminator/control lookahead; hand it to the scalar state machine.
        let hazards = dot_start & ((raw_breaks >> 1) | (eq >> 1));
        if hazards != 0 {
            return Ok(SpanBlockOutcome::ScalarThrough(
                block_src + hazards.trailing_zeros() as usize,
            ));
        }
    }

    let skip = fixed_eq | raw_breaks | dot_start;

    let decoded = if escaped == 0 {
        [
            unsafe { vsubq_u8(a, constants.normal_offset) },
            unsafe { vsubq_u8(b, constants.normal_offset) },
            unsafe { vsubq_u8(c, constants.normal_offset) },
            unsafe { vsubq_u8(d, constants.normal_offset) },
        ]
    } else if eq & (eq << 1) == 0 {
        // No adjacent '=': escaped positions are exactly the '=' compares
        // shifted one lane, so the offset select never leaves the vector
        // domain (rapidyenc's shortcut path).
        let zero = unsafe { vdupq_n_u8(0) };
        let sel_a = unsafe { vextq_u8::<15>(zero, eq_a) };
        let sel_b = unsafe { vextq_u8::<15>(eq_a, eq_b) };
        let sel_c = unsafe { vextq_u8::<15>(eq_b, eq_c) };
        let sel_d = unsafe { vextq_u8::<15>(eq_c, eq_d) };
        [
            unsafe {
                vsubq_u8(
                    a,
                    vbslq_u8(sel_a, constants.escaped_offset, constants.normal_offset),
                )
            },
            unsafe {
                vsubq_u8(
                    b,
                    vbslq_u8(sel_b, constants.escaped_offset, constants.normal_offset),
                )
            },
            unsafe {
                vsubq_u8(
                    c,
                    vbslq_u8(sel_c, constants.escaped_offset, constants.normal_offset),
                )
            },
            unsafe {
                vsubq_u8(
                    d,
                    vbslq_u8(sel_d, constants.escaped_offset, constants.normal_offset),
                )
            },
        ]
    } else {
        // Invalid '=' chains ("==", "==="): expand the chain-resolved mask
        // through the table path.
        unsafe { neon_decode_with_escape_mask([a, b, c, d], escaped, constants) }
    };

    let keeps = per_group_keeps(skip);
    unsafe {
        compact_store_16(
            decoded[0],
            (skip & 0xffff) as u16,
            keeps.0,
            table,
            output,
            dst,
        );
        compact_store_16(
            decoded[1],
            ((skip >> 16) & 0xffff) as u16,
            keeps.1,
            table,
            output,
            dst,
        );
        compact_store_16(
            decoded[2],
            ((skip >> 32) & 0xffff) as u16,
            keeps.2,
            table,
            output,
            dst,
        );
        compact_store_16(
            decoded[3],
            ((skip >> 48) & 0xffff) as u16,
            keeps.3,
            table,
            output,
            dst,
        );
    }

    let mut next_state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
    if fixed_eq & (1 << 63) != 0 {
        // Escape at the window's last byte: its partner is in the next
        // window; carry through the state machine (rapidyenc's escFirst).
        next_state = if dot_unstuffing && line_start & (1 << 63) != 0 {
            DecoderState::CrLfEq
        } else {
            DecoderState::Eq
        };
    } else if dot_start & (1 << 63) != 0 {
        // Line-start dot at the last byte: it is stripped either way; the
        // state machine resolves terminator vs stuffed data on the next byte.
        next_state = DecoderState::CrLfDot;
    } else if dot_unstuffing && escaped_cr & (1 << 63) != 0 {
        // Escaped CR at the last byte still opens an NNTP line boundary.
        next_state = DecoderState::Cr;
    }
    state.state = next_state;
    *src += 64;
    Ok(SpanBlockOutcome::Consumed)
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
// ARMv7 companion to rapidyenc's 32-byte NEON decoder mask path.
unsafe fn arm_neon_compare_mask32(vectors: [std::arch::arm::uint8x16_t; 2]) -> u64 {
    use std::arch::arm::*;

    let mut mask = 0u64;
    for (chunk, vector) in vectors.into_iter().enumerate() {
        let mut lanes = [0u8; 16];
        unsafe { vst1q_u8(lanes.as_mut_ptr(), vector) };
        for (lane, value) in lanes.into_iter().enumerate() {
            if value != 0 {
                mask |= 1u64 << (chunk * 16 + lane);
            }
        }
    }
    mask
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
// ARMv7 companion to rapidyenc NEON's maskEqTemp/vtbl escaped-byte offset path.
unsafe fn arm_neon_decode_with_escape_mask(
    vectors: [std::arch::arm::uint8x16_t; 2],
    escaped: u64,
) -> [std::arch::arm::uint8x16_t; 2] {
    use std::arch::arm::*;

    let mut offset_a = [42u8; 16];
    let mut offset_b = [42u8; 16];
    for lane in 0..16usize {
        if escaped & (1u64 << lane) != 0 {
            offset_a[lane] = 106;
        }
        if escaped & (1u64 << (lane + 16)) != 0 {
            offset_b[lane] = 106;
        }
    }

    let offset_a = unsafe { vld1q_u8(offset_a.as_ptr()) };
    let offset_b = unsafe { vld1q_u8(offset_b.as_ptr()) };
    [unsafe { vsubq_u8(vectors[0], offset_a) }, unsafe {
        vsubq_u8(vectors[1], offset_b)
    }]
}

#[cfg(target_arch = "aarch64")]
// Ported from rapidyenc NEON64's maskEqTemp/vqtbl escaped-byte offset path.
#[inline(always)]
unsafe fn neon_decode_with_escape_mask(
    vectors: [std::arch::aarch64::uint8x16_t; 4],
    escaped: u64,
    constants: &Neon64Constants,
) -> [std::arch::aarch64::uint8x16_t; 4] {
    use std::arch::aarch64::*;

    let mut mask = unsafe { vreinterpretq_u8_u64(vdupq_n_u64(escaped)) };
    let mask_a = unsafe { vtstq_u8(vqtbl1q_u8(mask, constants.selector), constants.bit_weights) };
    mask = unsafe { vextq_u8::<2>(mask, mask) };
    let mask_b = unsafe { vtstq_u8(vqtbl1q_u8(mask, constants.selector), constants.bit_weights) };
    mask = unsafe { vextq_u8::<2>(mask, mask) };
    let mask_c = unsafe { vtstq_u8(vqtbl1q_u8(mask, constants.selector), constants.bit_weights) };
    mask = unsafe { vextq_u8::<2>(mask, mask) };
    let mask_d = unsafe { vtstq_u8(vqtbl1q_u8(mask, constants.selector), constants.bit_weights) };

    [
        unsafe {
            vsubq_u8(
                vectors[0],
                vbslq_u8(mask_a, constants.escaped_offset, constants.normal_offset),
            )
        },
        unsafe {
            vsubq_u8(
                vectors[1],
                vbslq_u8(mask_b, constants.escaped_offset, constants.normal_offset),
            )
        },
        unsafe {
            vsubq_u8(
                vectors[2],
                vbslq_u8(mask_c, constants.escaped_offset, constants.normal_offset),
            )
        },
        unsafe {
            vsubq_u8(
                vectors[3],
                vbslq_u8(mask_d, constants.escaped_offset, constants.normal_offset),
            )
        },
    ]
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
fn compact_table_16() -> &'static [[u8; 16]; 32768] {
    use std::sync::OnceLock;

    static TABLE: OnceLock<Box<[[u8; 16]; 32768]>> = OnceLock::new();
    TABLE
        .get_or_init(|| {
            let mut table = [[0x80u8; 16]; 32768];
            for (mask, row) in table.iter_mut().enumerate() {
                let mut out = 0usize;
                for lane in 0..16usize {
                    if mask & (1 << lane) == 0 {
                        row[out] = lane as u8;
                        out += 1;
                    }
                }
            }
            Box::new(table)
        })
        .as_ref()
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
fn compact_table_8() -> &'static [[u8; 8]; 256] {
    use std::sync::OnceLock;

    static TABLE: OnceLock<Box<[[u8; 8]; 256]>> = OnceLock::new();
    TABLE
        .get_or_init(|| {
            let mut table = [[0xffu8; 8]; 256];
            for (mask, row) in table.iter_mut().enumerate() {
                let mut out = 0usize;
                for lane in 0..8usize {
                    if mask & (1 << lane) == 0 {
                        row[out] = lane as u8;
                        out += 1;
                    }
                }
            }
            Box::new(table)
        })
        .as_ref()
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
unsafe fn compact_store_8_arm_neon(
    decoded: std::arch::arm::uint8x8_t,
    skip_mask: u8,
    output: &mut [u8],
    dst: &mut usize,
) -> Result<(), YencError> {
    use std::arch::arm::*;

    let keep = 8 - skip_mask.count_ones() as usize;
    if output.len().saturating_sub(*dst) < keep {
        return Err(YencError::BufferTooSmall {
            needed: *dst + keep,
            available: output.len(),
        });
    }

    let shuffle = unsafe { vld1_u8(compact_table_8()[skip_mask as usize].as_ptr()) };
    let packed = unsafe { vtbl1_u8(decoded, shuffle) };
    let mut tmp = [0u8; 8];
    unsafe { vst1_u8(tmp.as_mut_ptr(), packed) };
    output[*dst..*dst + keep].copy_from_slice(&tmp[..keep]);
    *dst += keep;
    Ok(())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn compact_store_16_ssse3(
    decoded: std::arch::x86_64::__m128i,
    skip_mask: u16,
    output: &mut [u8],
    dst: &mut usize,
) -> Result<(), YencError> {
    use std::arch::x86_64::*;

    let keep = 16 - skip_mask.count_ones() as usize;
    if output.len().saturating_sub(*dst) < keep {
        return Err(YencError::BufferTooSmall {
            needed: *dst + keep,
            available: output.len(),
        });
    }

    let shuffle = unsafe {
        _mm_loadu_si128(compact_table_16()[(skip_mask & 0x7fff) as usize].as_ptr() as *const __m128i)
    };
    let packed = _mm_shuffle_epi8(decoded, shuffle);
    unsafe { _mm_storeu_si128(output.as_mut_ptr().add(*dst) as *mut __m128i, packed) };
    *dst += keep;
    Ok(())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn compact_store_16_avx2(
    decoded: std::arch::x86_64::__m256i,
    high_lane: bool,
    skip_mask: u16,
    output: &mut [u8],
    dst: &mut usize,
) -> Result<(), YencError> {
    use std::arch::x86_64::*;

    let keep = 16 - skip_mask.count_ones() as usize;
    if output.len().saturating_sub(*dst) < keep {
        return Err(YencError::BufferTooSmall {
            needed: *dst + keep,
            available: output.len(),
        });
    }

    let shuffle = unsafe {
        _mm_loadu_si128(compact_table_16()[(skip_mask & 0x7fff) as usize].as_ptr() as *const __m128i)
    };
    let shuffle = if high_lane {
        _mm256_inserti128_si256(_mm256_setzero_si256(), shuffle, 1)
    } else {
        _mm256_castsi128_si256(shuffle)
    };
    let packed = _mm256_shuffle_epi8(decoded, shuffle);
    let packed_lane = if high_lane {
        _mm256_extracti128_si256::<1>(packed)
    } else {
        _mm256_castsi256_si128(packed)
    };
    unsafe { _mm_storeu_si128(output.as_mut_ptr().add(*dst) as *mut __m128i, packed_lane) };
    *dst += keep;
    Ok(())
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn compact_store_16(
    decoded: std::arch::aarch64::uint8x16_t,
    skip_mask: u16,
    keep: usize,
    table: &[[u8; 16]; 32768],
    output: &mut [u8],
    dst: &mut usize,
) {
    use std::arch::aarch64::*;

    // The caller guarantees 64 spare output bytes per block, so each of the
    // four stores can write a full 16-byte vector; bytes past `keep` are
    // overwritten by the next store.
    debug_assert!(output.len().saturating_sub(*dst) >= 16);
    debug_assert_eq!(keep, 16 - skip_mask.count_ones() as usize);
    let shuffle = unsafe { vld1q_u8(table[(skip_mask & 0x7fff) as usize].as_ptr()) };
    let packed = unsafe { vqtbl1q_u8(decoded, shuffle) };
    unsafe { vst1q_u8(output.as_mut_ptr().add(*dst), packed) };
    *dst += keep;
}

#[inline]
fn write_decoded(output: &mut [u8], dst: &mut usize, byte: u8) -> Result<(), YencError> {
    if *dst >= output.len() {
        return Err(YencError::BufferTooSmall {
            needed: *dst + 1,
            available: output.len(),
        });
    }
    output[*dst] = byte;
    *dst += 1;
    Ok(())
}

/// Decode a run of "normal" yEnc bytes (no special characters) by subtracting 42.
///
/// Scans `input` from position `start`, decoding bytes into `output` at position
/// `dst_start`. Stops at the first special character (`=`, `\r`, `\n`) or end of input.
///
/// Returns `(bytes_consumed_from_input, bytes_written_to_output)`.
#[inline]
pub fn decode_normal_run(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    #[cfg(target_arch = "x86_64")]
    {
        return dispatch_x86_decode_normal_run()(input, start, output, dst_start);
    }

    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { decode_normal_run_neon(input, start, output, dst_start) };
    }

    #[cfg(all(target_arch = "arm", target_feature = "neon"))]
    {
        return unsafe { decode_normal_run_arm_neon(input, start, output, dst_start) };
    }

    #[cfg(target_arch = "riscv64")]
    {
        return decode_normal_run_riscv64(input, start, output, dst_start);
    }

    #[allow(unreachable_code)]
    decode_normal_run_scalar(input, start, output, dst_start)
}

#[cfg(target_arch = "riscv64")]
#[inline]
fn decode_normal_run_riscv64(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    // RVV runtime dispatch is not exposed through stable Rust's portable
    // detection surface yet, so RISC-V stays on the scalar kernel for now.
    decode_normal_run_scalar(input, start, output, dst_start)
}

/// Scalar fallback: process one byte at a time.
#[inline]
fn decode_normal_run_scalar(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    let max_len = input
        .len()
        .saturating_sub(start)
        .min(output.len().saturating_sub(dst_start));
    let input = &input[start..start + max_len];
    let run_len = memchr::memchr3(b'=', b'\r', b'\n', input).unwrap_or(input.len());
    let output = &mut output[dst_start..dst_start + run_len];

    let mut i = 0usize;
    while i + 8 <= run_len {
        output[i] = input[i].wrapping_sub(42);
        output[i + 1] = input[i + 1].wrapping_sub(42);
        output[i + 2] = input[i + 2].wrapping_sub(42);
        output[i + 3] = input[i + 3].wrapping_sub(42);
        output[i + 4] = input[i + 4].wrapping_sub(42);
        output[i + 5] = input[i + 5].wrapping_sub(42);
        output[i + 6] = input[i + 6].wrapping_sub(42);
        output[i + 7] = input[i + 7].wrapping_sub(42);
        i += 8;
    }
    while i < run_len {
        output[i] = input[i].wrapping_sub(42);
        i += 1;
    }

    (run_len, run_len)
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn dispatch_x86_decode_normal_run() -> DecodeRunFn {
    use std::sync::OnceLock;

    static DISPATCH: OnceLock<DecodeRunFn> = OnceLock::new();
    *DISPATCH.get_or_init(|| {
        if is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx2")
        {
            decode_normal_run_avx512_dispatch
        } else if is_x86_feature_detected!("avx2") {
            decode_normal_run_avx2_dispatch
        } else if is_x86_feature_detected!("avx") {
            decode_normal_run_avx_dispatch
        } else if is_x86_feature_detected!("ssse3") && !x86_prefers_sse2_over_ssse3() {
            decode_normal_run_ssse3_dispatch
        } else if is_x86_feature_detected!("sse2") {
            decode_normal_run_sse2_dispatch
        } else {
            decode_normal_run_scalar
        }
    })
}

#[cfg(target_arch = "x86_64")]
fn x86_prefers_sse2_over_ssse3() -> bool {
    let (family, model) = x86_rapidyenc_family_model();
    x86_family_model_prefers_sse2_over_ssse3(family, model)
}

#[cfg(target_arch = "x86_64")]
fn x86_family_model_prefers_sse2_over_ssse3(family: u32, model: u32) -> bool {
    matches!(
        (family, model),
        // Ported from rapidyenc's Bonnell/Silvermont and AMD Bobcat guardrail:
        // these cores have very slow PSHUFB/PBLENDVB, so rapidyenc pretends
        // SSSE3 does not exist and stays on SSE2.
        (
            6,
            0x1c | 0x26 | 0x27 | 0x35 | 0x36 | 0x37 | 0x4a | 0x4c | 0x4d | 0x5a | 0x5d
        ) | (0x5f, 0..=2)
    )
}

#[cfg(target_arch = "x86_64")]
fn x86_prefers_ssse3_over_sse41() -> bool {
    let (family, model) = x86_rapidyenc_family_model();
    x86_family_model_prefers_ssse3_over_sse41(family, model)
}

#[cfg(target_arch = "x86_64")]
fn x86_family_model_prefers_ssse3_over_sse41(family: u32, model: u32) -> bool {
    matches!(
        (family, model),
        // Ported from rapidyenc's Goldmont/Goldmont Plus/Tremont guardrail:
        // keep PSHUFB compaction but avoid the SSE4.1 PBLENDVB path.
        (6, 0x5c | 0x5f | 0x7a | 0x9c)
    )
}

#[cfg(target_arch = "x86_64")]
fn x86_rapidyenc_family_model() -> (u32, u32) {
    use std::arch::x86_64::__cpuid;

    let leaf = __cpuid(1);
    x86_rapidyenc_family_model_from_eax(leaf.eax)
}

#[cfg(target_arch = "x86_64")]
fn x86_rapidyenc_family_model_from_eax(eax: u32) -> (u32, u32) {
    let family = ((eax >> 8) & 0xf) + ((eax >> 16) & 0xff0);
    let model = ((eax >> 4) & 0xf) + ((eax >> 12) & 0xf0);
    (family, model)
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn decode_normal_run_sse2_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_sse2(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn decode_normal_run_ssse3_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_ssse3(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn decode_normal_run_avx_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_avx(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn decode_normal_run_avx2_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_avx2(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn decode_normal_run_avx512_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_avx512(input, start, output, dst_start) }
}

/// SSE2 implementation: process 16 bytes at a time.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn decode_normal_run_sse2(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::x86_64::*;

    let mut src = start;
    let mut dst = dst_start;

    unsafe {
        let special_eq = _mm_set1_epi8(b'=' as i8);
        let special_cr = _mm_set1_epi8(b'\r' as i8);
        let special_lf = _mm_set1_epi8(b'\n' as i8);
        let sub42 = _mm_set1_epi8(42i8.wrapping_neg());

        while src + 16 <= input.len() && dst + 16 <= output.len() {
            let chunk = _mm_loadu_si128(input.as_ptr().add(src) as *const __m128i);

            let eq_mask = _mm_cmpeq_epi8(chunk, special_eq);
            let cr_mask = _mm_cmpeq_epi8(chunk, special_cr);
            let lf_mask = _mm_cmpeq_epi8(chunk, special_lf);
            let any_special = _mm_or_si128(_mm_or_si128(eq_mask, cr_mask), lf_mask);

            let mask = _mm_movemask_epi8(any_special);
            if mask != 0 {
                let count = mask.trailing_zeros() as usize;
                if count > 0 {
                    let decoded = _mm_add_epi8(chunk, sub42);
                    let mut tmp = [0u8; 16];
                    _mm_storeu_si128(tmp.as_mut_ptr() as *mut __m128i, decoded);
                    output[dst..dst + count].copy_from_slice(&tmp[..count]);
                    src += count;
                    dst += count;
                }
                break;
            }

            let decoded = _mm_add_epi8(chunk, sub42);
            _mm_storeu_si128(output.as_mut_ptr().add(dst) as *mut __m128i, decoded);
            src += 16;
            dst += 16;
        }
    }

    let (extra_src, extra_dst) = decode_normal_run_scalar(input, src, output, dst);
    (src - start + extra_src, dst - dst_start + extra_dst)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn decode_normal_run_ssse3(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_sse2(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn decode_normal_run_avx(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_sse2(input, start, output, dst_start) }
}

/// AVX2 implementation: process 32 bytes at a time.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn decode_normal_run_avx2(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::x86_64::*;

    let mut src = start;
    let mut dst = dst_start;

    unsafe {
        let special_eq = _mm256_set1_epi8(b'=' as i8);
        let special_cr = _mm256_set1_epi8(b'\r' as i8);
        let special_lf = _mm256_set1_epi8(b'\n' as i8);
        let sub42 = _mm256_set1_epi8(42i8.wrapping_neg());

        while src + 32 <= input.len() && dst + 32 <= output.len() {
            let chunk = _mm256_loadu_si256(input.as_ptr().add(src) as *const __m256i);

            let eq_mask = _mm256_cmpeq_epi8(chunk, special_eq);
            let cr_mask = _mm256_cmpeq_epi8(chunk, special_cr);
            let lf_mask = _mm256_cmpeq_epi8(chunk, special_lf);
            let any_special = _mm256_or_si256(_mm256_or_si256(eq_mask, cr_mask), lf_mask);

            let mask = _mm256_movemask_epi8(any_special);
            if mask != 0 {
                let count = mask.trailing_zeros() as usize;
                if count > 0 {
                    let decoded = _mm256_add_epi8(chunk, sub42);
                    let mut tmp = [0u8; 32];
                    _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, decoded);
                    output[dst..dst + count].copy_from_slice(&tmp[..count]);
                    src += count;
                    dst += count;
                }
                break;
            }

            let decoded = _mm256_add_epi8(chunk, sub42);
            _mm256_storeu_si256(output.as_mut_ptr().add(dst) as *mut __m256i, decoded);
            src += 32;
            dst += 32;
        }
    }

    let (extra_src, extra_dst) = unsafe { decode_normal_run_sse2(input, src, output, dst) };
    (src - start + extra_src, dst - dst_start + extra_dst)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512f")]
unsafe fn decode_normal_run_avx512(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::x86_64::*;

    let mut src = start;
    let mut dst = dst_start;

    unsafe {
        let special_eq = _mm512_set1_epi8(b'=' as i8);
        let special_cr = _mm512_set1_epi8(b'\r' as i8);
        let special_lf = _mm512_set1_epi8(b'\n' as i8);
        let sub42 = _mm512_set1_epi8(42i8.wrapping_neg());

        while src + 64 <= input.len() && dst + 64 <= output.len() {
            let chunk = _mm512_loadu_si512(input.as_ptr().add(src) as *const __m512i);

            let mask = _mm512_cmpeq_epi8_mask(chunk, special_eq)
                | _mm512_cmpeq_epi8_mask(chunk, special_cr)
                | _mm512_cmpeq_epi8_mask(chunk, special_lf);
            if mask != 0 {
                let count = mask.trailing_zeros() as usize;
                if count > 0 {
                    let decoded = _mm512_add_epi8(chunk, sub42);
                    let mut tmp = [0u8; 64];
                    _mm512_storeu_si512(tmp.as_mut_ptr() as *mut __m512i, decoded);
                    output[dst..dst + count].copy_from_slice(&tmp[..count]);
                    src += count;
                    dst += count;
                }
                break;
            }

            let decoded = _mm512_add_epi8(chunk, sub42);
            _mm512_storeu_si512(output.as_mut_ptr().add(dst) as *mut __m512i, decoded);
            src += 64;
            dst += 64;
        }
    }

    let (extra_src, extra_dst) = unsafe { decode_normal_run_avx2(input, src, output, dst) };
    (src - start + extra_src, dst - dst_start + extra_dst)
}

/// NEON implementation for aarch64: process 16 bytes at a time.
#[cfg(target_arch = "aarch64")]
unsafe fn decode_normal_run_neon(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::aarch64::*;

    let mut src = start;
    let mut dst = dst_start;

    unsafe {
        let special_eq = vdupq_n_u8(b'=');
        let special_cr = vdupq_n_u8(b'\r');
        let special_lf = vdupq_n_u8(b'\n');
        let sub42 = vdupq_n_u8(42u8.wrapping_neg());

        while src + 16 <= input.len() && dst + 16 <= output.len() {
            let chunk = vld1q_u8(input.as_ptr().add(src));

            let eq_mask = vceqq_u8(chunk, special_eq);
            let cr_mask = vceqq_u8(chunk, special_cr);
            let lf_mask = vceqq_u8(chunk, special_lf);
            let any_special = vorrq_u8(vorrq_u8(eq_mask, cr_mask), lf_mask);

            let max_val = vmaxvq_u8(any_special);
            if max_val != 0 {
                let mask64 = vreinterpretq_u64_u8(any_special);
                let low = vgetq_lane_u64(mask64, 0);
                let high = vgetq_lane_u64(mask64, 1);
                let count = if low != 0 {
                    (low.trailing_zeros() / 8) as usize
                } else {
                    8 + (high.trailing_zeros() / 8) as usize
                };

                if count > 0 {
                    let decoded = vaddq_u8(chunk, sub42);
                    let mut tmp = [0u8; 16];
                    vst1q_u8(tmp.as_mut_ptr(), decoded);
                    output[dst..dst + count].copy_from_slice(&tmp[..count]);
                    src += count;
                    dst += count;
                }
                break;
            }

            let decoded = vaddq_u8(chunk, sub42);
            vst1q_u8(output.as_mut_ptr().add(dst), decoded);
            src += 16;
            dst += 16;
        }
    }

    let (extra_src, extra_dst) = decode_normal_run_scalar(input, src, output, dst);
    (src - start + extra_src, dst - dst_start + extra_dst)
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
unsafe fn decode_normal_run_arm_neon(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::arm::*;

    let mut src = start;
    let mut dst = dst_start;

    unsafe {
        let special_eq = vdupq_n_u8(b'=');
        let special_cr = vdupq_n_u8(b'\r');
        let special_lf = vdupq_n_u8(b'\n');
        let sub42 = vdupq_n_u8(42u8.wrapping_neg());

        while src + 16 <= input.len() && dst + 16 <= output.len() {
            let chunk = vld1q_u8(input.as_ptr().add(src));
            let any_special = vorrq_u8(
                vorrq_u8(vceqq_u8(chunk, special_eq), vceqq_u8(chunk, special_cr)),
                vceqq_u8(chunk, special_lf),
            );

            let mut lanes = [0u8; 16];
            vst1q_u8(lanes.as_mut_ptr(), any_special);
            if let Some(count) = lanes.iter().position(|&lane| lane != 0) {
                if count > 0 {
                    let decoded = vaddq_u8(chunk, sub42);
                    let mut tmp = [0u8; 16];
                    vst1q_u8(tmp.as_mut_ptr(), decoded);
                    output[dst..dst + count].copy_from_slice(&tmp[..count]);
                    src += count;
                    dst += count;
                }
                break;
            }

            let decoded = vaddq_u8(chunk, sub42);
            vst1q_u8(output.as_mut_ptr().add(dst), decoded);
            src += 16;
            dst += 16;
        }
    }

    let (extra_src, extra_dst) = decode_normal_run_scalar(input, src, output, dst);
    (src - start + extra_src, dst - dst_start + extra_dst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::DecodeState;

    fn scalar_body_with_state(
        input: &[u8],
        initial_state: DecoderState,
        dot_unstuffing: bool,
    ) -> (Vec<u8>, KernelState) {
        let mut output = vec![0u8; input.len() + 64];
        let mut state = KernelState {
            state: initial_state,
            ..KernelState::body()
        };
        let outcome =
            decode_kernel_scalar(input, &mut output, &mut state, dot_unstuffing, false, true)
                .unwrap();
        output.truncate(outcome.written);
        (output, state)
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn sse2_block_with_state(
        input: &[u8],
        initial_state: DecoderState,
        dot_unstuffing: bool,
    ) -> Option<(Vec<u8>, KernelState)> {
        let mut output = vec![0u8; input.len() + 64];
        let mut dst = 0usize;
        let mut state = KernelState {
            state: initial_state,
            ..KernelState::body()
        };
        let consumed = unsafe {
            try_decode_sse2_block(input, 0, &mut output, &mut dst, &mut state, dot_unstuffing)
                .unwrap()
        }?;
        assert_eq!(consumed, 64);
        output.truncate(dst);
        Some((output, state))
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn ssse3_block_with_state(
        input: &[u8],
        initial_state: DecoderState,
        dot_unstuffing: bool,
    ) -> Option<(Vec<u8>, KernelState)> {
        if !is_x86_feature_detected!("ssse3") {
            return None;
        }

        let mut output = vec![0u8; input.len() + 64];
        let mut dst = 0usize;
        let mut state = KernelState {
            state: initial_state,
            ..KernelState::body()
        };
        let consumed = unsafe {
            try_decode_ssse3_block(input, 0, &mut output, &mut dst, &mut state, dot_unstuffing)
                .unwrap()
        }?;
        assert_eq!(consumed, 64);
        output.truncate(dst);
        Some((output, state))
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn sse41_block_with_state(
        input: &[u8],
        initial_state: DecoderState,
        dot_unstuffing: bool,
    ) -> Option<(Vec<u8>, KernelState)> {
        if !(is_x86_feature_detected!("sse4.1") && is_x86_feature_detected!("ssse3")) {
            return None;
        }

        let mut output = vec![0u8; input.len() + 64];
        let mut dst = 0usize;
        let mut state = KernelState {
            state: initial_state,
            ..KernelState::body()
        };
        let consumed = unsafe {
            try_decode_sse41_block(input, 0, &mut output, &mut dst, &mut state, dot_unstuffing)
                .unwrap()
        }?;
        assert_eq!(consumed, 64);
        output.truncate(dst);
        Some((output, state))
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn avx_block_with_state(
        input: &[u8],
        initial_state: DecoderState,
        dot_unstuffing: bool,
    ) -> Option<(Vec<u8>, KernelState)> {
        if !(is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("popcnt")
            && is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("ssse3"))
        {
            return None;
        }

        let mut output = vec![0u8; input.len() + 64];
        let mut dst = 0usize;
        let mut state = KernelState {
            state: initial_state,
            ..KernelState::body()
        };
        let consumed = unsafe {
            try_decode_avx_block(input, 0, &mut output, &mut dst, &mut state, dot_unstuffing)
                .unwrap()
        }?;
        assert_eq!(consumed, 64);
        output.truncate(dst);
        Some((output, state))
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn avx2_block_with_state(
        input: &[u8],
        initial_state: DecoderState,
        dot_unstuffing: bool,
    ) -> Option<(Vec<u8>, KernelState)> {
        if !is_x86_feature_detected!("avx2") {
            return None;
        }

        let mut output = vec![0u8; input.len() + 64];
        let mut dst = 0usize;
        let mut state = KernelState {
            state: initial_state,
            ..KernelState::body()
        };
        let consumed = unsafe {
            try_decode_avx2_block(input, 0, &mut output, &mut dst, &mut state, dot_unstuffing)
                .unwrap()
        }?;
        assert_eq!(consumed, 64);
        output.truncate(dst);
        Some((output, state))
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn avx512_vbmi2_block_with_state(
        input: &[u8],
        initial_state: DecoderState,
        dot_unstuffing: bool,
    ) -> Option<(Vec<u8>, KernelState)> {
        if !(is_x86_feature_detected!("avx512vbmi2")
            && is_x86_feature_detected!("avx512vl")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx2"))
        {
            return None;
        }

        let mut output = vec![0u8; input.len() + 64];
        let mut dst = 0usize;
        let mut state = KernelState {
            state: initial_state,
            ..KernelState::body()
        };
        let consumed = unsafe {
            try_decode_avx512_vbmi2_block(
                input,
                0,
                &mut output,
                &mut dst,
                &mut state,
                dot_unstuffing,
            )
            .unwrap()
        }?;
        assert_eq!(consumed, 64);
        output.truncate(dst);
        Some((output, state))
    }

    #[cfg(target_arch = "aarch64")]
    unsafe fn neon_block_with_state(
        input: &[u8],
        initial_state: DecoderState,
        dot_unstuffing: bool,
    ) -> Option<(Vec<u8>, KernelState)> {
        // The block reads one byte past the 64-byte window for control-line
        // lookahead; give it the same slack the kernel driver guarantees.
        let mut padded = input.to_vec();
        padded.extend_from_slice(&[b'A'; 8]);
        let mut output = vec![0u8; input.len() + 64];
        let mut dst = 0usize;
        let mut state = KernelState {
            state: initial_state,
            ..KernelState::body()
        };
        let mut src = 0usize;
        let ctx = Neon64Ctx {
            dot_unstuffing,
            search_end: true,
            constants: unsafe { Neon64Constants::new() },
            table: compact_table_16(),
        };
        match unsafe {
            decode_neon64_span_block(&padded, &mut src, &mut output, &mut dst, &mut state, &ctx)
                .unwrap()
        } {
            SpanBlockOutcome::Consumed => {}
            SpanBlockOutcome::ScalarThrough(_) => return None,
        }
        assert_eq!(src, 64);
        output.truncate(dst);
        Some((output, state))
    }

    #[test]
    fn scalar_normal_run_basic() {
        let input = b"hello world";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run_scalar(input, 0, &mut output, 0);
        assert_eq!(consumed, 11);
        assert_eq!(written, 11);
        for i in 0..11 {
            assert_eq!(output[i], input[i].wrapping_sub(42));
        }
    }

    #[test]
    fn scalar_stops_at_equals() {
        let input = b"AB=CD";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run_scalar(input, 0, &mut output, 0);
        assert_eq!(consumed, 2);
        assert_eq!(written, 2);
    }

    #[test]
    fn scalar_stops_at_cr() {
        let input = b"AB\r\nCD";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run_scalar(input, 0, &mut output, 0);
        assert_eq!(consumed, 2);
        assert_eq!(written, 2);
    }

    #[test]
    fn scalar_stops_at_lf() {
        let input = b"AB\nCD";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run_scalar(input, 0, &mut output, 0);
        assert_eq!(consumed, 2);
        assert_eq!(written, 2);
    }

    #[test]
    fn decode_normal_run_dispatches() {
        let input: Vec<u8> = (0..256u16)
            .filter(|&b| b != b'=' as u16 && b != b'\r' as u16 && b != b'\n' as u16)
            .map(|b| b as u8)
            .collect();
        let mut output = vec![0u8; 256];
        let (consumed, written) = decode_normal_run(&input, 0, &mut output, 0);
        assert_eq!(consumed, input.len());
        assert_eq!(written, input.len());
        for i in 0..written {
            assert_eq!(output[i], input[i].wrapping_sub(42));
        }
    }

    #[test]
    fn normal_run_empty() {
        let input = b"";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run(input, 0, &mut output, 0);
        assert_eq!(consumed, 0);
        assert_eq!(written, 0);
    }

    #[test]
    fn normal_run_starts_with_special() {
        let input = b"=AB";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run(input, 0, &mut output, 0);
        assert_eq!(consumed, 0);
        assert_eq!(written, 0);
    }

    #[test]
    fn normal_run_long_input() {
        let input: Vec<u8> = vec![b'A'; 1000];
        let mut output = vec![0u8; 1000];
        let (consumed, written) = decode_normal_run(&input, 0, &mut output, 0);
        assert_eq!(consumed, 1000);
        assert_eq!(written, 1000);
        assert!(output.iter().all(|&b| b == b'A'.wrapping_sub(42)));
    }

    #[test]
    fn body_kernel_handles_escape_crlf_and_dot_unstuffing() {
        let input = b"AB=C\r\n..D";
        let mut output = vec![0u8; 64];
        let written = decode_body_into(input, &mut output, true).unwrap();

        assert_eq!(
            &output[..written],
            &[
                b'A'.wrapping_sub(42),
                b'B'.wrapping_sub(42),
                b'C'.wrapping_sub(106),
                b'.'.wrapping_sub(42),
                b'D'.wrapping_sub(42),
            ]
        );
    }

    #[test]
    fn chunk_kernel_preserves_pending_escape() {
        let mut state = DecodeState::new();
        let mut output = vec![0u8; 64];
        let n1 = decode_chunk_into(b"AB=", &mut output, &mut state, false).unwrap();
        assert_eq!(n1, 2);
        assert!(state.escape_pending);

        let n2 = decode_chunk_into(b"C", &mut output[n1..], &mut state, false).unwrap();
        assert_eq!(n2, 1);
        assert!(!state.escape_pending);
        assert_eq!(output[n1], b'C'.wrapping_sub(106));
    }

    #[test]
    fn chunk_kernel_preserves_line_start_escape_control_boundary() {
        let mut state = DecodeState::new();
        let mut output = vec![0u8; 64];
        let n1 = decode_chunk_into(b"AB\r\n=", &mut output, &mut state, true).unwrap();
        assert_eq!(n1, 2);
        assert!(state.escape_pending);
        assert!(state.at_line_start);

        let n2 = decode_chunk_into(b"yignored", &mut output[n1..], &mut state, true).unwrap();
        assert_eq!(n2, 8);
        assert!(!state.escape_pending);
        assert_eq!(
            &output[..n1 + n2],
            &[
                b'A'.wrapping_sub(42),
                b'B'.wrapping_sub(42),
                b'y'.wrapping_sub(106),
                b'i'.wrapping_sub(42),
                b'g'.wrapping_sub(42),
                b'n'.wrapping_sub(42),
                b'o'.wrapping_sub(42),
                b'r'.wrapping_sub(42),
                b'e'.wrapping_sub(42),
                b'd'.wrapping_sub(42),
            ]
        );
    }

    #[test]
    fn chunk_kernel_preserves_bare_cr_pending() {
        let input = b"AB\r.CD";
        let mut whole = vec![0u8; 64];
        let whole_written = decode_body_into(input, &mut whole, true).unwrap();

        let mut state = DecodeState::new();
        let mut chunked = vec![0u8; 64];
        let n1 = decode_chunk_into(&input[..3], &mut chunked, &mut state, true).unwrap();
        let n2 = decode_chunk_into(&input[3..], &mut chunked[n1..], &mut state, true).unwrap();

        assert_eq!(&chunked[..n1 + n2], &whole[..whole_written]);
    }

    #[test]
    fn chunk_kernel_preserves_dot_cr_pending() {
        let input = b"AB\r\n.\r.CD";
        let mut whole = vec![0u8; 64];
        let whole_written = decode_body_into(input, &mut whole, true).unwrap();

        let mut state = DecodeState::new();
        let mut chunked = vec![0u8; 64];
        let n1 = decode_chunk_into(&input[..6], &mut chunked, &mut state, true).unwrap();
        let n2 = decode_chunk_into(&input[6..], &mut chunked[n1..], &mut state, true).unwrap();

        assert_eq!(&chunked[..n1 + n2], &whole[..whole_written]);
    }

    #[test]
    fn normal_run_does_not_mutate_past_written_prefix() {
        let input = b"AB=DEFGHIJKLMNOPQRST";
        let mut output = [0xaau8; 64];
        let (consumed, written) = decode_normal_run(input, 0, &mut output, 0);

        assert_eq!(consumed, 2);
        assert_eq!(written, 2);
        assert_eq!(
            &output[..2],
            &[b'A'.wrapping_sub(42), b'B'.wrapping_sub(42)]
        );
        assert!(output[2..32].iter().all(|&byte| byte == 0xaa));
    }

    #[test]
    fn body_kernel_reports_trailing_escape() {
        let mut output = vec![0u8; 64];
        let result = decode_body_into(b"AB=", &mut output, false);
        assert!(matches!(result, Err(YencError::MalformedEscape(2))));
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn fix_eq_mask_keeps_alternating_escape_starts() {
        assert_eq!(fix_eq_mask(0b1111, 0b11110), 0b0101);
        assert_eq!(fix_eq_mask(0b1110, 0b11100), 0b1010);
        assert_eq!(fix_eq_mask(0b1010, 0b10100), 0b1010);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn x86_dispatch_guardrails_match_rapidyenc_model_lists() {
        for model in [
            0x1c, 0x26, 0x27, 0x35, 0x36, 0x37, 0x4a, 0x4c, 0x4d, 0x5a, 0x5d,
        ] {
            assert!(x86_family_model_prefers_sse2_over_ssse3(6, model));
        }
        for model in [0, 1, 2] {
            assert!(x86_family_model_prefers_sse2_over_ssse3(0x5f, model));
        }
        for model in [0x5c, 0x5f, 0x7a, 0x9c] {
            assert!(x86_family_model_prefers_ssse3_over_sse41(6, model));
            assert!(!x86_family_model_prefers_sse2_over_ssse3(6, model));
        }
        assert!(!x86_family_model_prefers_sse2_over_ssse3(6, 0x3c));
        assert!(!x86_family_model_prefers_ssse3_over_sse41(6, 0x3c));
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn x86_rapidyenc_family_model_uses_extended_amd_family_bits() {
        let eax = (0xf << 8) | (0x5 << 20);
        assert_eq!(x86_rapidyenc_family_model_from_eax(eax), (0x5f, 0));
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx2_block_compacts_escapes_and_line_breaks() {
        let mut input = [b'A'; 64];
        input[3] = b'=';
        input[4] = b'C';
        input[10] = b'\r';
        input[11] = b'\n';
        input[20] = b'=';
        input[21] = b'=';
        input[22] = b'B';
        input[37] = b'\n';
        input[52] = b'=';
        input[53] = b'Z';

        let Some((avx2, avx_state)) =
            (unsafe { avx2_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(avx2, scalar);
        assert_eq!(avx_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn sse2_block_compacts_escapes_and_line_breaks() {
        let mut input = [b'A'; 64];
        input[3] = b'=';
        input[4] = b'C';
        input[10] = b'\r';
        input[11] = b'\n';
        input[20] = b'=';
        input[21] = b'=';
        input[22] = b'B';
        input[37] = b'\n';
        input[52] = b'=';
        input[53] = b'Z';

        let Some((sse2, sse_state)) =
            (unsafe { sse2_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(sse2, scalar);
        assert_eq!(sse_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn sse2_block_handles_dot_stuffed_line_start() {
        let mut input = [b'A'; 64];
        input[0] = b'.';
        input[1] = b'.';
        input[16] = b'\r';
        input[17] = b'\n';
        input[18] = b'.';
        input[19] = b'.';

        let Some((sse2, sse_state)) =
            (unsafe { sse2_block_with_state(&input, DecoderState::CrLf, true) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

        assert_eq!(sse2, scalar);
        assert_eq!(sse_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn sse2_block_preserves_trailing_cr_state() {
        let mut input = [b'A'; 64];
        input[63] = b'\r';

        let Some((sse2, sse_state)) =
            (unsafe { sse2_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(sse2, scalar);
        assert_eq!(sse_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn ssse3_block_compacts_escapes_and_line_breaks() {
        let mut input = [b'A'; 64];
        input[3] = b'=';
        input[4] = b'C';
        input[10] = b'\r';
        input[11] = b'\n';
        input[20] = b'=';
        input[21] = b'=';
        input[22] = b'B';
        input[37] = b'\n';
        input[52] = b'=';
        input[53] = b'Z';

        let Some((ssse3, ssse3_state)) =
            (unsafe { ssse3_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(ssse3, scalar);
        assert_eq!(ssse3_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn ssse3_block_handles_dot_stuffed_line_start() {
        let mut input = [b'A'; 64];
        input[0] = b'.';
        input[1] = b'.';
        input[16] = b'\r';
        input[17] = b'\n';
        input[18] = b'.';
        input[19] = b'.';

        let Some((ssse3, ssse3_state)) =
            (unsafe { ssse3_block_with_state(&input, DecoderState::CrLf, true) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

        assert_eq!(ssse3, scalar);
        assert_eq!(ssse3_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn ssse3_block_preserves_trailing_cr_state() {
        let mut input = [b'A'; 64];
        input[63] = b'\r';

        let Some((ssse3, ssse3_state)) =
            (unsafe { ssse3_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(ssse3, scalar);
        assert_eq!(ssse3_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn sse41_block_compacts_escapes_and_line_breaks() {
        let mut input = [b'A'; 64];
        input[3] = b'=';
        input[4] = b'C';
        input[10] = b'\r';
        input[11] = b'\n';
        input[20] = b'=';
        input[21] = b'=';
        input[22] = b'B';
        input[37] = b'\n';
        input[52] = b'=';
        input[53] = b'Z';

        let Some((sse41, sse41_state)) =
            (unsafe { sse41_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(sse41, scalar);
        assert_eq!(sse41_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn sse41_block_handles_dot_stuffed_line_start() {
        let mut input = [b'A'; 64];
        input[0] = b'.';
        input[1] = b'.';
        input[16] = b'\r';
        input[17] = b'\n';
        input[18] = b'.';
        input[19] = b'.';

        let Some((sse41, sse41_state)) =
            (unsafe { sse41_block_with_state(&input, DecoderState::CrLf, true) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

        assert_eq!(sse41, scalar);
        assert_eq!(sse41_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn sse41_block_preserves_trailing_cr_state() {
        let mut input = [b'A'; 64];
        input[63] = b'\r';

        let Some((sse41, sse41_state)) =
            (unsafe { sse41_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(sse41, scalar);
        assert_eq!(sse41_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx_block_compacts_escapes_and_line_breaks() {
        let mut input = [b'A'; 64];
        input[3] = b'=';
        input[4] = b'C';
        input[10] = b'\r';
        input[11] = b'\n';
        input[20] = b'=';
        input[21] = b'=';
        input[22] = b'B';
        input[37] = b'\n';
        input[52] = b'=';
        input[53] = b'Z';

        let Some((avx, avx_state)) =
            (unsafe { avx_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(avx, scalar);
        assert_eq!(avx_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx_block_handles_dot_stuffed_line_start() {
        let mut input = [b'A'; 64];
        input[0] = b'.';
        input[1] = b'.';
        input[16] = b'\r';
        input[17] = b'\n';
        input[18] = b'.';
        input[19] = b'.';

        let Some((avx, avx_state)) =
            (unsafe { avx_block_with_state(&input, DecoderState::CrLf, true) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

        assert_eq!(avx, scalar);
        assert_eq!(avx_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx_block_preserves_trailing_cr_state() {
        let mut input = [b'A'; 64];
        input[63] = b'\r';

        let Some((avx, avx_state)) =
            (unsafe { avx_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(avx, scalar);
        assert_eq!(avx_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx2_block_handles_dot_stuffed_line_start() {
        let mut input = [b'A'; 64];
        input[0] = b'.';
        input[1] = b'.';
        input[16] = b'\r';
        input[17] = b'\n';
        input[18] = b'.';
        input[19] = b'.';

        let Some((avx2, avx_state)) =
            (unsafe { avx2_block_with_state(&input, DecoderState::CrLf, true) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

        assert_eq!(avx2, scalar);
        assert_eq!(avx_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx2_block_preserves_trailing_cr_state() {
        let mut input = [b'A'; 64];
        input[63] = b'\r';

        let Some((avx2, avx_state)) =
            (unsafe { avx2_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(avx2, scalar);
        assert_eq!(avx_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx512_vbmi2_block_compacts_escapes_and_line_breaks() {
        let mut input = [b'A'; 64];
        input[3] = b'=';
        input[4] = b'C';
        input[10] = b'\r';
        input[11] = b'\n';
        input[20] = b'=';
        input[21] = b'=';
        input[22] = b'B';
        input[37] = b'\n';
        input[52] = b'=';
        input[53] = b'Z';

        let Some((vbmi2, vbmi2_state)) =
            (unsafe { avx512_vbmi2_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(vbmi2, scalar);
        assert_eq!(vbmi2_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx512_vbmi2_block_handles_dot_stuffed_line_start() {
        let mut input = [b'A'; 64];
        input[0] = b'.';
        input[1] = b'.';
        input[16] = b'\r';
        input[17] = b'\n';
        input[18] = b'.';
        input[19] = b'.';

        let Some((vbmi2, vbmi2_state)) =
            (unsafe { avx512_vbmi2_block_with_state(&input, DecoderState::CrLf, true) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

        assert_eq!(vbmi2, scalar);
        assert_eq!(vbmi2_state, scalar_state);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx512_vbmi2_block_preserves_trailing_cr_state() {
        let mut input = [b'A'; 64];
        input[63] = b'\r';

        let Some((vbmi2, vbmi2_state)) =
            (unsafe { avx512_vbmi2_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(vbmi2, scalar);
        assert_eq!(vbmi2_state, scalar_state);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_block_compacts_escapes_and_line_breaks() {
        let mut input = [b'A'; 64];
        input[3] = b'=';
        input[4] = b'C';
        input[10] = b'\r';
        input[11] = b'\n';
        input[20] = b'=';
        input[21] = b'=';
        input[22] = b'B';
        input[37] = b'\n';
        input[52] = b'=';
        input[53] = b'Z';

        let Some((neon, neon_state)) =
            (unsafe { neon_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(neon, scalar);
        assert_eq!(neon_state, scalar_state);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_block_handles_dot_stuffed_line_start() {
        let mut input = [b'A'; 64];
        input[0] = b'.';
        input[1] = b'.';
        input[16] = b'\r';
        input[17] = b'\n';
        input[18] = b'.';
        input[19] = b'.';

        let Some((neon, neon_state)) =
            (unsafe { neon_block_with_state(&input, DecoderState::CrLf, true) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

        assert_eq!(neon, scalar);
        assert_eq!(neon_state, scalar_state);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_block_preserves_trailing_cr_state() {
        let mut input = [b'A'; 64];
        input[63] = b'\r';

        let Some((neon, neon_state)) =
            (unsafe { neon_block_with_state(&input, DecoderState::None, false) })
        else {
            return;
        };
        let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

        assert_eq!(neon, scalar);
        assert_eq!(neon_state, scalar_state);
    }

    #[test]
    fn scalar_kernel_detects_control_boundaries() {
        let (output, state) = scalar_body_with_state(b"A\r\n=yignored", DecoderState::CrLf, true);
        assert_eq!(output, vec![b'A'.wrapping_sub(42)]);
        assert_eq!(state.end, DecodeEnd::Control);

        let (output, state) = scalar_body_with_state(b"A\r\n.=yignored", DecoderState::CrLf, true);
        assert_eq!(output, vec![b'A'.wrapping_sub(42)]);
        assert_eq!(state.end, DecodeEnd::Control);
    }

    fn lcg(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *seed >> 33
    }

    /// Build an adversarial raw NNTP/yEnc stream: normal runs interleaved
    /// with escapes, chains, malformed escapes, bare breaks, line-start dots
    /// and escapes, and (rarely) control/terminator shapes.
    fn adversarial_stream(seed: &mut u64, len_target: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len_target + 16);
        while out.len() < len_target {
            match lcg(seed) % 20 {
                0..=7 => {
                    let run = (lcg(seed) % 90) as usize + 1;
                    for _ in 0..run {
                        let byte = (lcg(seed) % 256) as u8;
                        if !matches!(byte, b'=' | b'\r' | b'\n') {
                            out.push(byte);
                        }
                    }
                }
                8..=10 => out.extend_from_slice(b"\r\n"),
                11 => {
                    out.push(b'=');
                    out.push((lcg(seed) % 256) as u8);
                }
                12 => out.extend_from_slice(b"=\r"),
                13 => out.extend_from_slice(b"=\n"),
                14 => out.extend_from_slice(b"=="),
                15 => out.extend_from_slice(b"==="),
                16 => out.extend_from_slice(b"\r\n."),
                17 => out.extend_from_slice(b"\r\n.."),
                18 => out.extend_from_slice(b"\r\n=J"),
                19 => match lcg(seed) % 6 {
                    0 => out.extend_from_slice(b"\r"),
                    1 => out.extend_from_slice(b"\n"),
                    2 => out.extend_from_slice(b"\r\r"),
                    3 => out.extend_from_slice(b"\n\n"),
                    4 => out.extend_from_slice(b"\r\n.\r\n"),
                    _ => out.extend_from_slice(b"\r\n=y"),
                },
                _ => unreachable!(),
            }
        }
        // Avoid a trailing escape so non-preserving modes do not error and
        // every mode combination can run over the same stream.
        while matches!(out.last(), Some(b'=')) {
            out.pop();
        }
        out
    }

    #[allow(clippy::type_complexity)]
    fn run_kernel_whole(
        input: &[u8],
        dot_unstuffing: bool,
        preserve_pending: bool,
        search_end: bool,
        scalar: bool,
    ) -> Result<(Vec<u8>, usize, RapidyencDecodeEnd, KernelState), YencError> {
        let mut output = vec![0u8; input.len() + 64];
        let mut state = KernelState::body();
        let outcome = if scalar {
            decode_kernel_scalar(
                input,
                &mut output,
                &mut state,
                dot_unstuffing,
                preserve_pending,
                search_end,
            )?
        } else {
            decode_kernel(
                input,
                &mut output,
                &mut state,
                dot_unstuffing,
                preserve_pending,
                search_end,
            )?
        };
        output.truncate(outcome.written);
        Ok((output, outcome.consumed, outcome.end, state))
    }

    #[allow(clippy::type_complexity)]
    fn run_kernel_chunked(
        input: &[u8],
        chunk_len: usize,
        dot_unstuffing: bool,
        search_end: bool,
        scalar: bool,
    ) -> (Vec<u8>, usize, RapidyencDecodeEnd, KernelState) {
        let mut output = vec![0u8; input.len() + 64];
        let mut state = KernelState::body();
        let mut consumed = 0usize;
        let mut written = 0usize;
        let mut end = RapidyencDecodeEnd::None;
        while consumed < input.len() && end == RapidyencDecodeEnd::None {
            let chunk_end = (consumed + chunk_len).min(input.len());
            let chunk = &input[consumed..chunk_end];
            let outcome = if scalar {
                decode_kernel_scalar(
                    chunk,
                    &mut output[written..],
                    &mut state,
                    dot_unstuffing,
                    true,
                    search_end,
                )
            } else {
                decode_kernel(
                    chunk,
                    &mut output[written..],
                    &mut state,
                    dot_unstuffing,
                    true,
                    search_end,
                )
            }
            .expect("chunked decode with preserve_pending cannot error");
            written += outcome.written;
            end = outcome.end;
            if outcome.consumed == 0 && outcome.end == RapidyencDecodeEnd::None {
                break;
            }
            consumed += outcome.consumed;
        }
        output.truncate(written);
        (output, consumed, end, state)
    }

    #[test]
    fn dispatch_kernel_matches_scalar_on_adversarial_streams() {
        let mut seed = 0x00c0ffee_d15ea5e5u64;
        for round in 0..64 {
            let len = 256 + (lcg(&mut seed) % 4096) as usize;
            let input = adversarial_stream(&mut seed, len);
            for &(dot, preserve, search) in &[
                (true, true, true),
                (true, true, false),
                (true, false, true),
                (false, true, false),
                (false, false, false),
            ] {
                let simd = run_kernel_whole(&input, dot, preserve, search, false);
                let reference = run_kernel_whole(&input, dot, preserve, search, true);
                match (simd, reference) {
                    (Ok(simd), Ok(reference)) => {
                        assert_eq!(
                            simd, reference,
                            "round {round} dot={dot} preserve={preserve} search={search}"
                        );
                    }
                    (Err(simd), Err(reference)) => {
                        assert_eq!(
                            simd.to_string(),
                            reference.to_string(),
                            "round {round} dot={dot} preserve={preserve} search={search}"
                        );
                    }
                    (simd, reference) => panic!(
                        "kernel disagreement round {round} dot={dot} preserve={preserve} \
                         search={search}: simd={simd:?} reference={reference:?}"
                    ),
                }
            }
        }
    }

    #[test]
    fn dispatch_kernel_matches_scalar_across_chunk_splits() {
        let mut seed = 0xfeedface_0badf00du64;
        for round in 0..12 {
            let len = 1024 + (lcg(&mut seed) % 2048) as usize;
            let input = adversarial_stream(&mut seed, len);
            for &chunk_len in &[1usize, 2, 3, 63, 64, 65, 127, 128, 257] {
                for &(dot, search) in &[(true, true), (true, false), (false, false)] {
                    let simd = run_kernel_chunked(&input, chunk_len, dot, search, false);
                    let reference = run_kernel_chunked(&input, chunk_len, dot, search, true);
                    assert_eq!(
                        simd, reference,
                        "round {round} chunk={chunk_len} dot={dot} search={search}"
                    );
                }
            }
        }
    }

    #[test]
    fn dispatch_kernel_handles_boundary_specials_like_scalar() {
        // Deterministic windows that pin the block-boundary carries: escapes,
        // line-start escapes and dots, and escaped CRs at the 64-byte edge,
        // plus control lines and terminators mid-window.
        let mut cases: Vec<Vec<u8>> = Vec::new();

        // '=' as the final byte of the first 64-byte window.
        let mut case = vec![b'A'; 63];
        case.push(b'=');
        case.extend_from_slice(b"J then more data follows here");
        case.extend_from_slice(&[b'B'; 96]);
        cases.push(case);

        // Line-start '=' at the window edge: "\r\n" at 61..63, '=' at 63.
        let mut case = vec![b'A'; 61];
        case.extend_from_slice(b"\r\n=");
        case.extend_from_slice(b"Jrest-of-line");
        case.extend_from_slice(&[b'C'; 96]);
        cases.push(case);

        // Line-start '=y' split across the window edge (control line).
        let mut case = vec![b'A'; 61];
        case.extend_from_slice(b"\r\n=");
        case.extend_from_slice(b"yend size=128");
        case.extend_from_slice(&[b'D'; 96]);
        cases.push(case);

        // Line-start dot at the window edge, stuffed-data continuation.
        let mut case = vec![b'A'; 61];
        case.extend_from_slice(b"\r\n.");
        case.extend_from_slice(b".data-after-stuffed-dot");
        case.extend_from_slice(&[b'E'; 96]);
        cases.push(case);

        // Line-start dot at the window edge, terminator continuation.
        let mut case = vec![b'A'; 61];
        case.extend_from_slice(b"\r\n.");
        case.extend_from_slice(b"\r\nignored-after-terminator");
        case.extend_from_slice(&[b'F'; 96]);
        cases.push(case);

        // Escaped CR at the window edge, dot-stuffed continuation.
        let mut case = vec![b'A'; 62];
        case.extend_from_slice(b"=\r");
        case.extend_from_slice(b"\n.more-data");
        case.extend_from_slice(&[b'G'; 96]);
        cases.push(case);

        // Escaped CR mid-window opening a line boundary ("=\r\n." strips).
        let mut case = vec![b'H'; 32];
        case.extend_from_slice(b"=\r\n.stuffed");
        case.extend_from_slice(&[b'I'; 128]);
        cases.push(case);

        // '=' chains crossing the window edge.
        let mut case = vec![b'J'; 62];
        case.extend_from_slice(b"===");
        case.extend_from_slice(b"KLMNOP");
        case.extend_from_slice(&[b'K'; 96]);
        cases.push(case);

        // Control line "=y" mid-window with a line-start escape before it.
        let mut case = vec![b'L'; 20];
        case.extend_from_slice(b"\r\n=Mescaped-line-start\r\n=yend trailer");
        case.extend_from_slice(&[b'M'; 96]);
        cases.push(case);

        // Plain terminator mid-window.
        let mut case = vec![b'N'; 40];
        case.extend_from_slice(b"\r\n.\r\nleftover");
        case.extend_from_slice(&[b'O'; 96]);
        cases.push(case);

        for (idx, input) in cases.iter().enumerate() {
            for &(dot, preserve, search) in &[
                (true, true, true),
                (true, true, false),
                (true, false, true),
                (false, true, false),
            ] {
                let simd = run_kernel_whole(input, dot, preserve, search, false);
                let reference = run_kernel_whole(input, dot, preserve, search, true);
                match (simd, reference) {
                    (Ok(simd), Ok(reference)) => assert_eq!(
                        simd, reference,
                        "case {idx} dot={dot} preserve={preserve} search={search}"
                    ),
                    (Err(simd), Err(reference)) => assert_eq!(
                        simd.to_string(),
                        reference.to_string(),
                        "case {idx} dot={dot} preserve={preserve} search={search}"
                    ),
                    (simd, reference) => panic!(
                        "kernel disagreement case {idx} dot={dot} preserve={preserve} \
                         search={search}: simd={simd:?} reference={reference:?}"
                    ),
                }
            }
        }
    }
}
