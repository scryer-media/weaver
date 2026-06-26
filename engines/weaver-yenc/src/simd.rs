//! SIMD and lookup-table accelerated yEnc decoding.
//!
//! The decoder keeps the public streaming state in `decode.rs`, but normalizes it
//! here so the same fast path can serve full-body and chunked decode.
//!
//! The SIMD/state-machine design is ported from rapidyenc commit 27f435a
//! (Public Domain/CC0), especially `decoder.cc`, `decoder_common.h`,
//! `decoder_avx2_base.h`, and `decoder_neon64.cc`.

use crate::decode::DecodeState;
use crate::error::YencError;

#[cfg(target_arch = "x86_64")]
type DecodeRunFn = fn(&[u8], usize, &mut [u8], usize) -> (usize, usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecoderState {
    CrLf,
    Eq,
    Cr,
    None,
    CrLfDot,
    CrLfDotCr,
    CrLfEq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeEnd {
    None,
    Article,
    Control,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KernelState {
    state: DecoderState,
    end: DecodeEnd,
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
}

/// Decode a complete body buffer with the SIMD-capable internal kernel.
pub(crate) fn decode_body_into(
    input: &[u8],
    output: &mut [u8],
    dot_unstuffing: bool,
) -> Result<usize, YencError> {
    let mut state = KernelState::body();
    decode_kernel(input, output, &mut state, dot_unstuffing, false)
}

/// Decode one streaming body chunk with carry state preserved across calls.
pub(crate) fn decode_chunk_into(
    input: &[u8],
    output: &mut [u8],
    state: &mut DecodeState,
    dot_unstuffing: bool,
) -> Result<usize, YencError> {
    let mut kernel_state = KernelState::from_decode_state(state);
    let written = decode_kernel(input, output, &mut kernel_state, dot_unstuffing, true)?;
    kernel_state.write_back(state);
    Ok(written)
}

fn decode_kernel(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    streaming: bool,
) -> Result<usize, YencError> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512vbmi2")
            && is_x86_feature_detected!("avx512vl")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx2")
        {
            return unsafe {
                decode_kernel_avx512_vbmi2(input, output, state, dot_unstuffing, streaming)
            };
        }
        if is_x86_feature_detected!("avx2") {
            return unsafe { decode_kernel_avx2(input, output, state, dot_unstuffing, streaming) };
        }
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("popcnt")
            && is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("ssse3")
            && !x86_prefers_ssse3_over_sse41()
            && !x86_prefers_sse2_over_ssse3()
        {
            return unsafe { decode_kernel_avx(input, output, state, dot_unstuffing, streaming) };
        }
        if is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("ssse3")
            && !x86_prefers_ssse3_over_sse41()
            && !x86_prefers_sse2_over_ssse3()
        {
            return unsafe { decode_kernel_sse41(input, output, state, dot_unstuffing, streaming) };
        }
        if is_x86_feature_detected!("ssse3") && !x86_prefers_sse2_over_ssse3() {
            return unsafe { decode_kernel_ssse3(input, output, state, dot_unstuffing, streaming) };
        }
        if is_x86_feature_detected!("sse2") {
            return unsafe { decode_kernel_sse2(input, output, state, dot_unstuffing, streaming) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        unsafe { decode_kernel_neon(input, output, state, dot_unstuffing, streaming) }
    }

    #[cfg(all(target_arch = "arm", target_feature = "neon"))]
    {
        return unsafe { decode_kernel_arm_neon(input, output, state, dot_unstuffing, streaming) };
    }

    #[cfg(not(any(
        target_arch = "aarch64",
        all(target_arch = "arm", target_feature = "neon")
    )))]
    {
        decode_kernel_scalar(input, output, state, dot_unstuffing, streaming)
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
    streaming: bool,
) -> Result<usize, YencError> {
    let mut src = 0usize;
    let mut dst = 0usize;

    while state.end == DecodeEnd::None && src < input.len() {
        if !decode_scalar_step(
            input,
            &mut src,
            output,
            &mut dst,
            state,
            dot_unstuffing,
            streaming,
        )? {
            break;
        }
    }

    Ok(dst)
}

fn decode_scalar_step(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
    streaming: bool,
) -> Result<bool, YencError> {
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
                if dot_unstuffing && byte == b'y' {
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
                        state.state = DecoderState::CrLfDotCr;
                        return Ok(true);
                    }
                    b'\n' => {
                        *src += 1;
                        state.state = DecoderState::CrLf;
                        state.end = DecodeEnd::Article;
                        return Ok(false);
                    }
                    b'=' if dot_unstuffing => {
                        *src += 1;
                        state.state = DecoderState::CrLfEq;
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
                    state.end = DecodeEnd::Article;
                    return Ok(false);
                }
                state.state = DecoderState::CrLf;
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
                        state.state = if at_line_start {
                            DecoderState::CrLf
                        } else {
                            DecoderState::None
                        };
                        return Ok(true);
                    }
                    b'=' => {
                        *src += 1;
                        if *src >= input.len() {
                            if streaming {
                                state.state = if dot_unstuffing && at_line_start {
                                    DecoderState::CrLfEq
                                } else {
                                    DecoderState::Eq
                                };
                                return Ok(false);
                            }
                            return Err(YencError::MalformedEscape(src.saturating_sub(1)));
                        }
                        if dot_unstuffing && at_line_start && input[*src] == b'y' {
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
    streaming: bool,
) -> Result<usize, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            streaming,
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
    streaming: bool,
) -> Result<usize, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            streaming,
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
    streaming: bool,
) -> Result<usize, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            streaming,
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
    streaming: bool,
) -> Result<usize, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            streaming,
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
    streaming: bool,
) -> Result<usize, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            streaming,
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
    streaming: bool,
) -> Result<usize, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            streaming,
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
    let crlf = raw_cr & (raw_lf >> 1);
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

    state.state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
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
    let crlf = raw_cr & (raw_lf >> 1);
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

    state.state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
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
    let crlf = raw_cr & (raw_lf >> 1);
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

    state.state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
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
    let crlf = raw_cr & (raw_lf >> 1);
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

    state.state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
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
    let crlf = raw_cr & (raw_lf >> 1);
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
    state.state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
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
    if output.len().saturating_sub(*dst) < keep {
        return Err(YencError::BufferTooSmall {
            needed: *dst + keep,
            available: output.len(),
        });
    }

    let packed = _mm256_maskz_compress_epi8(keep_mask, decoded);
    let mut tmp = [0u8; 32];
    unsafe { _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, packed) };
    output[*dst..*dst + keep].copy_from_slice(&tmp[..keep]);
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
    streaming: bool,
) -> Result<usize, YencError> {
    unsafe {
        decode_kernel_simd64(
            input,
            output,
            state,
            dot_unstuffing,
            streaming,
            try_decode_neon_block,
        )
    }
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
unsafe fn decode_kernel_arm_neon(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    streaming: bool,
) -> Result<usize, YencError> {
    unsafe {
        decode_kernel_simd32(
            input,
            output,
            state,
            dot_unstuffing,
            streaming,
            try_decode_arm_neon_block,
        )
    }
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
type DecodeBlock64 = unsafe fn(
    &[u8],
    usize,
    &mut [u8],
    &mut usize,
    &mut KernelState,
    bool,
) -> Result<Option<usize>, YencError>;

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
unsafe fn decode_kernel_simd64(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    streaming: bool,
    block: DecodeBlock64,
) -> Result<usize, YencError> {
    const WIDTH: usize = 64;

    let mut src = 0usize;
    let mut dst = 0usize;

    // rapidyenc keeps enough trailing input for scalar epilogue handling so
    // cross-boundary CRLF, dot-stuffing, and end markers remain exact.
    let tail_buffer = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail_buffer);

    if input.len() > WIDTH * 2 {
        while state.end == DecodeEnd::None && src + WIDTH <= simd_limit {
            if let Some(consumed) =
                unsafe { block(input, src, output, &mut dst, state, dot_unstuffing)? }
            {
                src += consumed;
                continue;
            }

            if !decode_scalar_step(
                input,
                &mut src,
                output,
                &mut dst,
                state,
                dot_unstuffing,
                streaming,
            )? {
                break;
            }
        }
    }

    while state.end == DecodeEnd::None && src < input.len() {
        if !decode_scalar_step(
            input,
            &mut src,
            output,
            &mut dst,
            state,
            dot_unstuffing,
            streaming,
        )? {
            break;
        }
    }

    Ok(dst)
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
    streaming: bool,
    block: DecodeBlock32,
) -> Result<usize, YencError> {
    const WIDTH: usize = 32;

    let mut src = 0usize;
    let mut dst = 0usize;
    let tail_buffer = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail_buffer);

    if input.len() > WIDTH * 2 {
        while state.end == DecodeEnd::None && src + WIDTH <= simd_limit {
            if let Some(consumed) =
                unsafe { block(input, src, output, &mut dst, state, dot_unstuffing)? }
            {
                src += consumed;
                continue;
            }

            if !decode_scalar_step(
                input,
                &mut src,
                output,
                &mut dst,
                state,
                dot_unstuffing,
                streaming,
            )? {
                break;
            }
        }
    }

    while state.end == DecodeEnd::None && src < input.len() {
        if !decode_scalar_step(
            input,
            &mut src,
            output,
            &mut dst,
            state,
            dot_unstuffing,
            streaming,
        )? {
            break;
        }
    }

    Ok(dst)
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
    let crlf = raw_cr & (raw_lf >> 1);
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
unsafe fn try_decode_neon_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::aarch64::*;

    if input.len().saturating_sub(src) < 64 || output.len().saturating_sub(*dst) < 64 {
        return Ok(None);
    }

    if !matches!(state.state, DecoderState::None | DecoderState::CrLf) {
        return Ok(None);
    }

    let a = unsafe { vld1q_u8(input.as_ptr().add(src)) };
    let b = unsafe { vld1q_u8(input.as_ptr().add(src + 16)) };
    let c = unsafe { vld1q_u8(input.as_ptr().add(src + 32)) };
    let d = unsafe { vld1q_u8(input.as_ptr().add(src + 48)) };
    let eq_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'=')) };
    let eq_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'=')) };
    let eq_c = unsafe { vceqq_u8(c, vdupq_n_u8(b'=')) };
    let eq_d = unsafe { vceqq_u8(d, vdupq_n_u8(b'=')) };
    let cr_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'\r')) };
    let cr_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'\r')) };
    let cr_c = unsafe { vceqq_u8(c, vdupq_n_u8(b'\r')) };
    let cr_d = unsafe { vceqq_u8(d, vdupq_n_u8(b'\r')) };
    let lf_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'\n')) };
    let lf_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'\n')) };
    let lf_c = unsafe { vceqq_u8(c, vdupq_n_u8(b'\n')) };
    let lf_d = unsafe { vceqq_u8(d, vdupq_n_u8(b'\n')) };
    let dot_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'.')) };
    let dot_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'.')) };
    let dot_c = unsafe { vceqq_u8(c, vdupq_n_u8(b'.')) };
    let dot_d = unsafe { vceqq_u8(d, vdupq_n_u8(b'.')) };
    let eq = unsafe { neon_compare_mask64([eq_a, eq_b, eq_c, eq_d]) };
    let cr = unsafe { neon_compare_mask64([cr_a, cr_b, cr_c, cr_d]) };
    let lf = unsafe { neon_compare_mask64([lf_a, lf_b, lf_c, lf_d]) };
    let dot = unsafe { neon_compare_mask64([dot_a, dot_b, dot_c, dot_d]) };

    let fixed_eq = fix_eq_mask(eq, eq << 1);
    if fixed_eq & (1u64 << 63) != 0 {
        return Ok(None);
    }

    let escaped = fixed_eq << 1;
    let raw_cr = cr & !escaped;
    let raw_lf = lf & !escaped;
    let raw_breaks = raw_cr | raw_lf;
    let crlf = raw_cr & (raw_lf >> 1);
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
    let sub42 = unsafe { vdupq_n_u8(42u8.wrapping_neg()) };

    if skip == 0 {
        unsafe {
            vst1q_u8(output.as_mut_ptr().add(*dst), vaddq_u8(a, sub42));
            vst1q_u8(output.as_mut_ptr().add(*dst + 16), vaddq_u8(b, sub42));
            vst1q_u8(output.as_mut_ptr().add(*dst + 32), vaddq_u8(c, sub42));
            vst1q_u8(output.as_mut_ptr().add(*dst + 48), vaddq_u8(d, sub42));
        }
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(Some(64));
    }

    let [decoded_a, decoded_b, decoded_c, decoded_d] =
        unsafe { neon_decode_with_escape_mask([a, b, c, d], escaped) };

    unsafe { compact_store_16(decoded_a, (skip & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16(decoded_b, ((skip >> 16) & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16(decoded_c, ((skip >> 32) & 0xffff) as u16, output, dst)? };
    unsafe { compact_store_16(decoded_d, ((skip >> 48) & 0xffff) as u16, output, dst)? };

    state.state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
    Ok(Some(64))
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
// Ported from rapidyenc NEON64's pairwise compare-mask packing.
unsafe fn neon_compare_mask64(vectors: [std::arch::aarch64::uint8x16_t; 4]) -> u64 {
    use std::arch::aarch64::*;

    let bit_weights =
        unsafe { vld1q_u8([1u8, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128].as_ptr()) };
    let merge = unsafe {
        vpaddq_u8(
            vpaddq_u8(
                vandq_u8(vectors[0], bit_weights),
                vandq_u8(vectors[1], bit_weights),
            ),
            vpaddq_u8(
                vandq_u8(vectors[2], bit_weights),
                vandq_u8(vectors[3], bit_weights),
            ),
        )
    };
    let packed = unsafe { vpaddq_u8(merge, vdupq_n_u8(0)) };
    unsafe { vgetq_lane_u64::<0>(vreinterpretq_u64_u8(packed)) }
}

#[cfg(target_arch = "aarch64")]
// Ported from rapidyenc NEON64's maskEqTemp/vqtbl escaped-byte offset path.
unsafe fn neon_decode_with_escape_mask(
    vectors: [std::arch::aarch64::uint8x16_t; 4],
    escaped: u64,
) -> [std::arch::aarch64::uint8x16_t; 4] {
    use std::arch::aarch64::*;

    let selector = unsafe { vld1q_u8([0u8, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1].as_ptr()) };
    let bit_weights =
        unsafe { vld1q_u8([1u8, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128].as_ptr()) };
    let normal = unsafe { vdupq_n_u8(42) };
    let escaped_offset = unsafe { vdupq_n_u8(106) };
    let mut mask = unsafe { vreinterpretq_u8_u64(vdupq_n_u64(escaped)) };
    let mask_a = unsafe { vtstq_u8(vqtbl1q_u8(mask, selector), bit_weights) };
    mask = unsafe { vextq_u8::<2>(mask, mask) };
    let mask_b = unsafe { vtstq_u8(vqtbl1q_u8(mask, selector), bit_weights) };
    mask = unsafe { vextq_u8::<2>(mask, mask) };
    let mask_c = unsafe { vtstq_u8(vqtbl1q_u8(mask, selector), bit_weights) };
    mask = unsafe { vextq_u8::<2>(mask, mask) };
    let mask_d = unsafe { vtstq_u8(vqtbl1q_u8(mask, selector), bit_weights) };

    [
        unsafe { vsubq_u8(vectors[0], vbslq_u8(mask_a, escaped_offset, normal)) },
        unsafe { vsubq_u8(vectors[1], vbslq_u8(mask_b, escaped_offset, normal)) },
        unsafe { vsubq_u8(vectors[2], vbslq_u8(mask_c, escaped_offset, normal)) },
        unsafe { vsubq_u8(vectors[3], vbslq_u8(mask_d, escaped_offset, normal)) },
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
    let mut tmp = [0u8; 16];
    unsafe { _mm_storeu_si128(tmp.as_mut_ptr() as *mut __m128i, packed) };
    output[*dst..*dst + keep].copy_from_slice(&tmp[..keep]);
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
    let mut tmp = [0u8; 32];
    unsafe { _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, packed) };
    let offset = if high_lane { 16 } else { 0 };
    output[*dst..*dst + keep].copy_from_slice(&tmp[offset..offset + keep]);
    *dst += keep;
    Ok(())
}

#[cfg(target_arch = "aarch64")]
unsafe fn compact_store_16(
    decoded: std::arch::aarch64::uint8x16_t,
    skip_mask: u16,
    output: &mut [u8],
    dst: &mut usize,
) -> Result<(), YencError> {
    use std::arch::aarch64::*;

    let keep = 16 - skip_mask.count_ones() as usize;
    if output.len().saturating_sub(*dst) < keep {
        return Err(YencError::BufferTooSmall {
            needed: *dst + keep,
            available: output.len(),
        });
    }

    let shuffle = unsafe { vld1q_u8(compact_table_16()[(skip_mask & 0x7fff) as usize].as_ptr()) };
    let packed = unsafe { vqtbl1q_u8(decoded, shuffle) };
    let mut tmp = [0u8; 16];
    unsafe { vst1q_u8(tmp.as_mut_ptr(), packed) };
    output[*dst..*dst + keep].copy_from_slice(&tmp[..keep]);
    *dst += keep;
    Ok(())
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
            end: DecodeEnd::None,
        };
        let written =
            decode_kernel_scalar(input, &mut output, &mut state, dot_unstuffing, false).unwrap();
        output.truncate(written);
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
            end: DecodeEnd::None,
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
            end: DecodeEnd::None,
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
            end: DecodeEnd::None,
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
            end: DecodeEnd::None,
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
            end: DecodeEnd::None,
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
            end: DecodeEnd::None,
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
        let mut output = vec![0u8; input.len() + 64];
        let mut dst = 0usize;
        let mut state = KernelState {
            state: initial_state,
            end: DecodeEnd::None,
        };
        let consumed = unsafe {
            try_decode_neon_block(input, 0, &mut output, &mut dst, &mut state, dot_unstuffing)
                .unwrap()
        }?;
        assert_eq!(consumed, 64);
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
        assert_eq!(n2, 0);
        assert!(!state.escape_pending);
        assert_eq!(
            &output[..n1],
            &[b'A'.wrapping_sub(42), b'B'.wrapping_sub(42)]
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
}
