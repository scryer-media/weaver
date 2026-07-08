use super::*;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
pub(super) unsafe fn decode_kernel_sse2(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    // Hot path: faithful rapidyenc `do_decode_sse` port at ISA_LEVEL_SSE2
    // (FAST_MATCH=false, BLEND_ADD=false). Other combos keep the general kernel.
    if dot_unstuffing
        && !search_end
        && input.len() > 64
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        let mode = DecodeStepMode {
            dot_unstuffing,
            preserve_pending,
            search_end,
        };
        return unsafe { decode_kernel_sse2_raw(input, output, state, mode) };
    }
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
pub(super) unsafe fn decode_kernel_ssse3(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    // Hot path: faithful rapidyenc `do_decode_sse` port at ISA_LEVEL_SSSE3
    // (FAST_MATCH=true, BLEND_ADD=false). Other combos keep the general kernel.
    if dot_unstuffing
        && !search_end
        && input.len() > 64
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        let mode = DecodeStepMode {
            dot_unstuffing,
            preserve_pending,
            search_end,
        };
        return unsafe { decode_kernel_ssse3_raw(input, output, state, mode) };
    }
    unsafe {
        decode_kernel_simd64_ssse3_line_aware(
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
pub(super) unsafe fn decode_kernel_sse41(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    // Hot path: faithful rapidyenc `do_decode_sse` port at ISA_LEVEL_SSE4_POPCNT
    // (FAST_MATCH=true, BLEND_ADD=true). Other combos keep the general kernel.
    if dot_unstuffing
        && !search_end
        && input.len() > 64
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        let mode = DecodeStepMode {
            dot_unstuffing,
            preserve_pending,
            search_end,
        };
        return unsafe { decode_kernel_sse41_raw(input, output, state, mode) };
    }
    unsafe {
        decode_kernel_simd64_ssse3_line_aware(
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
pub(super) unsafe fn decode_kernel_avx(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    // AVX reuses the SSE4.1/POPCNT raw kernel (weaver treats AVX == SSE4.1 for
    // the 128-bit decode body, matching `try_decode_avx_block`).
    if dot_unstuffing
        && !search_end
        && input.len() > 64
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        let mode = DecodeStepMode {
            dot_unstuffing,
            preserve_pending,
            search_end,
        };
        return unsafe { decode_kernel_sse41_raw(input, output, state, mode) };
    }
    unsafe {
        decode_kernel_simd64_ssse3_line_aware(
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
#[target_feature(enable = "sse2")]
pub(super) unsafe fn try_decode_sse2_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
    search_end: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::x86_64::*;

    if input.len().saturating_sub(src) < 64 || output.len().saturating_sub(*dst) < 64 {
        return Ok(None);
    }

    let a = unsafe { _mm_loadu_si128(input.as_ptr().add(src) as *const __m128i) };
    let b = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 16) as *const __m128i) };
    let c = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 32) as *const __m128i) };
    let d = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 48) as *const __m128i) };
    let vectors = [a, b, c, d];
    let Some((esc_first, dot0)) =
        x86_block_entry_flags(input, src, state.state, dot_unstuffing, search_end)
    else {
        return Ok(None);
    };
    let specials = unsafe { sse2_special_mask64(vectors) };
    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());

    if specials == 0 && !dot0 && !esc_first {
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

    let eq = if specials != 0 {
        unsafe { sse2_mask64(vectors, b'=') }
    } else {
        0
    };
    let esc_first = esc_first as u64;
    let fixed_eq = fix_eq_mask(eq, (eq << 1) | esc_first);
    let escaped = (fixed_eq << 1) | esc_first;
    let entry_line_start = (state.state == DecoderState::CrLf) as u64;

    let (cr, raw_cr, raw_breaks, crlf, line_start, dot_start) = if specials == eq {
        (
            0,
            0,
            0,
            0,
            entry_line_start,
            if dot_unstuffing {
                x86_dot_start_mask(input, src, entry_line_start, escaped)
            } else {
                0
            },
        )
    } else {
        let cr = unsafe { sse2_mask64(vectors, b'\r') };
        let lf = specials & !eq & !cr;
        let raw_cr = cr & !escaped;
        let raw_lf = lf & !escaped;
        let raw_breaks = raw_cr | raw_lf;
        // NNTP line boundaries exist in the raw stream even when yEnc escaped
        // the '\r', so pair detection uses the unmasked '\r' bits.
        let pair_cr = if dot_unstuffing { cr } else { raw_cr };
        let crlf = pair_cr & (lf >> 1);
        let line_start = entry_line_start | (crlf << 2);
        let dot_start = if dot_unstuffing {
            x86_dot_start_mask(input, src, line_start, escaped)
        } else {
            0
        };
        (cr, raw_cr, raw_breaks, crlf, line_start, dot_start)
    };

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || (line_start_eq & !(1u64 << 63)) != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    if skip == 0 && escaped == 0 {
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

    state.state = x86_final_state_after_block(
        fixed_eq,
        dot_start,
        raw_breaks,
        raw_cr,
        crlf,
        skip,
        line_start,
        cr,
        escaped,
        dot_unstuffing,
    );
    Ok(Some(64))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
pub(super) unsafe fn try_decode_ssse3_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
    search_end: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::x86_64::*;

    if input.len().saturating_sub(src) < 64 || output.len().saturating_sub(*dst) < 64 {
        return Ok(None);
    }

    let a = unsafe { _mm_loadu_si128(input.as_ptr().add(src) as *const __m128i) };
    let b = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 16) as *const __m128i) };
    let c = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 32) as *const __m128i) };
    let d = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 48) as *const __m128i) };
    let vectors = [a, b, c, d];
    let Some((esc_first, dot0)) =
        x86_block_entry_flags(input, src, state.state, dot_unstuffing, search_end)
    else {
        return Ok(None);
    };
    let specials = unsafe { ssse3_special_mask64(vectors) };
    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());

    if specials == 0 && !dot0 && !esc_first {
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

    let eq = if specials != 0 {
        unsafe { sse2_mask64(vectors, b'=') }
    } else {
        0
    };
    let esc_first = esc_first as u64;
    let fixed_eq = fix_eq_mask(eq, (eq << 1) | esc_first);
    let escaped = (fixed_eq << 1) | esc_first;
    let entry_line_start = (state.state == DecoderState::CrLf) as u64;

    let (cr, raw_cr, raw_breaks, crlf, line_start, dot_start) = if specials == eq {
        (
            0,
            0,
            0,
            0,
            entry_line_start,
            if dot_unstuffing {
                x86_dot_start_mask(input, src, entry_line_start, escaped)
            } else {
                0
            },
        )
    } else {
        let cr = unsafe { sse2_mask64(vectors, b'\r') };
        let lf = specials & !eq & !cr;
        let raw_cr = cr & !escaped;
        let raw_lf = lf & !escaped;
        let raw_breaks = raw_cr | raw_lf;
        // NNTP line boundaries exist in the raw stream even when yEnc escaped
        // the '\r', so pair detection uses the unmasked '\r' bits.
        let pair_cr = if dot_unstuffing { cr } else { raw_cr };
        let crlf = pair_cr & (lf >> 1);
        let line_start = entry_line_start | (crlf << 2);
        let dot_start = if dot_unstuffing {
            x86_dot_start_mask(input, src, line_start, escaped)
        } else {
            0
        };
        (cr, raw_cr, raw_breaks, crlf, line_start, dot_start)
    };

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || (line_start_eq & !(1u64 << 63)) != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    if skip == 0 && escaped == 0 {
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

    unsafe { compact_store_16_ssse3(decoded_a, (skip & 0xffff) as u16, output, dst) };
    unsafe { compact_store_16_ssse3(decoded_b, ((skip >> 16) & 0xffff) as u16, output, dst) };
    unsafe { compact_store_16_ssse3(decoded_c, ((skip >> 32) & 0xffff) as u16, output, dst) };
    unsafe { compact_store_16_ssse3(decoded_d, ((skip >> 48) & 0xffff) as u16, output, dst) };

    state.state = x86_final_state_after_block(
        fixed_eq,
        dot_start,
        raw_breaks,
        raw_cr,
        crlf,
        skip,
        line_start,
        cr,
        escaped,
        dot_unstuffing,
    );
    Ok(Some(64))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1,ssse3")]
pub(super) unsafe fn try_decode_sse41_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
    search_end: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::x86_64::*;

    if input.len().saturating_sub(src) < 64 || output.len().saturating_sub(*dst) < 64 {
        return Ok(None);
    }

    let a = unsafe { _mm_loadu_si128(input.as_ptr().add(src) as *const __m128i) };
    let b = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 16) as *const __m128i) };
    let c = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 32) as *const __m128i) };
    let d = unsafe { _mm_loadu_si128(input.as_ptr().add(src + 48) as *const __m128i) };
    let vectors = [a, b, c, d];
    let Some((esc_first, dot0)) =
        x86_block_entry_flags(input, src, state.state, dot_unstuffing, search_end)
    else {
        return Ok(None);
    };
    let specials = unsafe { ssse3_special_mask64(vectors) };
    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());

    if specials == 0 && !dot0 && !esc_first {
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

    let eq = if specials != 0 {
        unsafe { sse2_mask64(vectors, b'=') }
    } else {
        0
    };
    let esc_first = esc_first as u64;
    let fixed_eq = fix_eq_mask(eq, (eq << 1) | esc_first);
    let escaped = (fixed_eq << 1) | esc_first;
    let entry_line_start = (state.state == DecoderState::CrLf) as u64;

    let (cr, raw_cr, raw_breaks, crlf, line_start, dot_start) = if specials == eq {
        (
            0,
            0,
            0,
            0,
            entry_line_start,
            if dot_unstuffing {
                x86_dot_start_mask(input, src, entry_line_start, escaped)
            } else {
                0
            },
        )
    } else {
        let cr = unsafe { sse2_mask64(vectors, b'\r') };
        let lf = specials & !eq & !cr;
        let raw_cr = cr & !escaped;
        let raw_lf = lf & !escaped;
        let raw_breaks = raw_cr | raw_lf;
        // NNTP line boundaries exist in the raw stream even when yEnc escaped
        // the '\r', so pair detection uses the unmasked '\r' bits.
        let pair_cr = if dot_unstuffing { cr } else { raw_cr };
        let crlf = pair_cr & (lf >> 1);
        let line_start = entry_line_start | (crlf << 2);
        let dot_start = if dot_unstuffing {
            x86_dot_start_mask(input, src, line_start, escaped)
        } else {
            0
        };
        (cr, raw_cr, raw_breaks, crlf, line_start, dot_start)
    };

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || (line_start_eq & !(1u64 << 63)) != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    if skip == 0 && escaped == 0 {
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

    unsafe { compact_store_16_ssse3(decoded_a, (skip & 0xffff) as u16, output, dst) };
    unsafe { compact_store_16_ssse3(decoded_b, ((skip >> 16) & 0xffff) as u16, output, dst) };
    unsafe { compact_store_16_ssse3(decoded_c, ((skip >> 32) & 0xffff) as u16, output, dst) };
    unsafe { compact_store_16_ssse3(decoded_d, ((skip >> 48) & 0xffff) as u16, output, dst) };

    state.state = x86_final_state_after_block(
        fixed_eq,
        dot_start,
        raw_breaks,
        raw_cr,
        crlf,
        skip,
        line_start,
        cr,
        escaped,
        dot_unstuffing,
    );
    Ok(Some(64))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,popcnt,sse4.1,ssse3")]
pub(super) unsafe fn try_decode_avx_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
    search_end: bool,
) -> Result<Option<usize>, YencError> {
    // The AVX decoder reuses the SSE decode body with the SSE4/POPCNT
    // ISA level; keep that shape here instead of inventing a separate AVX1 body.
    unsafe { try_decode_sse41_block(input, src, output, dst, state, dot_unstuffing, search_end) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[inline]
pub(super) unsafe fn sse2_special_mask64(vectors: [std::arch::x86_64::__m128i; 4]) -> u64 {
    use std::arch::x86_64::*;

    let eq = _mm_set1_epi8(b'=' as i8);
    let cr = _mm_set1_epi8(b'\r' as i8);
    let lf = _mm_set1_epi8(b'\n' as i8);
    let mask = |v| {
        _mm_movemask_epi8(_mm_or_si128(
            _mm_or_si128(_mm_cmpeq_epi8(v, eq), _mm_cmpeq_epi8(v, cr)),
            _mm_cmpeq_epi8(v, lf),
        )) as u16 as u64
    };

    mask(vectors[0])
        | (mask(vectors[1]) << 16)
        | (mask(vectors[2]) << 32)
        | (mask(vectors[3]) << 48)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
#[inline]
pub(super) unsafe fn ssse3_special_mask64(vectors: [std::arch::x86_64::__m128i; 4]) -> u64 {
    use std::arch::x86_64::*;

    let table = _mm_set_epi8(
        -1,
        b'=' as i8,
        b'\r' as i8,
        -1,
        -1,
        b'\n' as i8,
        -1,
        -1,
        -1,
        -1,
        -1,
        -1,
        -1,
        -1,
        -1,
        b'.' as i8,
    );
    let clamp = _mm_set1_epi8(b'.' as i8);
    let mask = |v| {
        let cmp = _mm_cmpeq_epi8(v, _mm_shuffle_epi8(table, _mm_min_epu8(v, clamp)));
        _mm_movemask_epi8(cmp) as u16 as u64
    };

    mask(vectors[0])
        | (mask(vectors[1]) << 16)
        | (mask(vectors[2]) << 32)
        | (mask(vectors[3]) << 48)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[inline]
pub(super) unsafe fn sse2_mask64(vectors: [std::arch::x86_64::__m128i; 4], byte: u8) -> u64 {
    use std::arch::x86_64::*;

    let needle = _mm_set1_epi8(byte as i8);
    let a = _mm_movemask_epi8(_mm_cmpeq_epi8(vectors[0], needle)) as u16 as u64;
    let b = _mm_movemask_epi8(_mm_cmpeq_epi8(vectors[1], needle)) as u16 as u64;
    let c = _mm_movemask_epi8(_mm_cmpeq_epi8(vectors[2], needle)) as u16 as u64;
    let d = _mm_movemask_epi8(_mm_cmpeq_epi8(vectors[3], needle)) as u16 as u64;
    a | (b << 16) | (c << 32) | (d << 48)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
pub(super) unsafe fn ssse3_decode_with_escape_mask(
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
pub(super) unsafe fn sse41_decode_with_escape_mask(
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

/// SSSE3 twin of [`decode_kernel_simd64_avx2_line_aware`] for the pre-AVX2
/// tiers in the portable binary.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
pub(super) unsafe fn decode_kernel_simd64_ssse3_line_aware(
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
    let tail_buffer = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail_buffer);

    if input.len() > WIDTH * 2 {
        while (!search_end || state.end == DecodeEnd::None) && src + WIDTH <= simd_limit {
            if state.line_length.is_some()
                && let Some(consumed) = unsafe {
                    try_decode_ssse3_line(
                        input,
                        src,
                        output,
                        &mut dst,
                        state,
                        dot_unstuffing,
                        search_end,
                        simd_limit,
                    )?
                }
            {
                src += consumed;
                continue;
            }

            if let Some(consumed) = unsafe {
                block(
                    input,
                    src,
                    output,
                    &mut dst,
                    state,
                    dot_unstuffing,
                    search_end,
                )?
            } {
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

/// SSSE3 twin of [`try_decode_avx2_line`]: same guards and bail conditions,
/// 4x16-byte vectors instead of 2x32.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
#[allow(clippy::too_many_arguments)]
pub(super) unsafe fn try_decode_ssse3_line(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
    search_end: bool,
    simd_limit: usize,
) -> Result<Option<usize>, YencError> {
    use std::arch::x86_64::*;

    const WIDTH: usize = 64;
    const MAX_LINE_CHUNKS: usize = 16;
    const LAST: u64 = 1u64 << 63;

    let Some(line_length) = state.line_length else {
        return Ok(None);
    };
    if state.state != DecoderState::CrLf
        || line_length < WIDTH
        || line_length % WIDTH != 0
        || line_length / WIDTH > MAX_LINE_CHUNKS
    {
        return Ok(None);
    }

    let line_end = src.saturating_add(line_length);
    let after_crlf = line_end.saturating_add(2);
    if after_crlf > input.len() || after_crlf > simd_limit {
        return Ok(None);
    }
    if input[line_end] != b'\r' || input[line_end + 1] != b'\n' {
        return Ok(None);
    }
    if dot_unstuffing && input[src] == b'.' {
        return Ok(None);
    }
    if search_end && dot_unstuffing && input[src] == b'=' && input[src + 1] == b'y' {
        return Ok(None);
    }
    if input[line_end - 1] == b'=' || output.len().saturating_sub(*dst) < line_length {
        return Ok(None);
    }

    let load4 = |chunk_src: usize| -> [__m128i; 4] {
        unsafe {
            [
                _mm_loadu_si128(input.as_ptr().add(chunk_src) as *const __m128i),
                _mm_loadu_si128(input.as_ptr().add(chunk_src + 16) as *const __m128i),
                _mm_loadu_si128(input.as_ptr().add(chunk_src + 32) as *const __m128i),
                _mm_loadu_si128(input.as_ptr().add(chunk_src + 48) as *const __m128i),
            ]
        }
    };

    // Single pass; the '=' at line_end-1 guard above already excludes a
    // dangling escape at line end, and a raw CR/LF mid-line rewinds the
    // output cursor and hands the line back to the general path.
    let chunks = line_length / WIDTH;
    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());
    let dst_start = *dst;
    let mut esc_first = 0u64;
    for chunk_idx in 0..chunks {
        let vectors = load4(src + chunk_idx * WIDTH);
        let crlf = unsafe { sse2_mask64(vectors, b'\r') | sse2_mask64(vectors, b'\n') };
        if crlf != 0 {
            *dst = dst_start;
            return Ok(None);
        }
        let eq = unsafe { sse2_mask64(vectors, b'=') };
        let fixed_eq = fix_eq_mask(eq, (eq << 1) | esc_first);
        let escaped = (fixed_eq << 1) | esc_first;
        let skip = fixed_eq;

        if skip == 0 && escaped == 0 {
            unsafe {
                _mm_storeu_si128(
                    output.as_mut_ptr().add(*dst) as *mut __m128i,
                    _mm_add_epi8(vectors[0], sub42),
                );
                _mm_storeu_si128(
                    output.as_mut_ptr().add(*dst + 16) as *mut __m128i,
                    _mm_add_epi8(vectors[1], sub42),
                );
                _mm_storeu_si128(
                    output.as_mut_ptr().add(*dst + 32) as *mut __m128i,
                    _mm_add_epi8(vectors[2], sub42),
                );
                _mm_storeu_si128(
                    output.as_mut_ptr().add(*dst + 48) as *mut __m128i,
                    _mm_add_epi8(vectors[3], sub42),
                );
            }
            *dst += WIDTH;
        } else {
            for (group, &vector) in vectors.iter().enumerate() {
                let group_escaped = ((escaped >> (group * 16)) & 0xffff) as u16;
                let decoded = unsafe { ssse3_decode_with_escape_mask(vector, group_escaped) };
                let group_skip = ((skip >> (group * 16)) & 0xffff) as u16;
                unsafe { compact_store_16_ssse3(decoded, group_skip, output, dst) };
            }
        }

        esc_first = (fixed_eq & LAST != 0) as u64;
    }

    debug_assert_eq!(esc_first, 0);
    state.state = DecoderState::CrLf;
    Ok(Some(line_length + 2))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
pub(super) unsafe fn compact_store_16_ssse3(
    decoded: std::arch::x86_64::__m128i,
    skip_mask: u16,
    output: &mut [u8],
    dst: &mut usize,
) {
    use std::arch::x86_64::*;

    // The caller guarantees 64 spare output bytes per block, so each of the
    // four stores can write a full 16-byte vector; bytes past `keep` are
    // overwritten by the next store.
    debug_assert!(output.len().saturating_sub(*dst) >= 16);
    let keep = 16 - skip_mask.count_ones() as usize;
    let shuffle = unsafe {
        _mm_loadu_si128(compact_table_16()[(skip_mask & 0x7fff) as usize].as_ptr() as *const __m128i)
    };
    let packed = _mm_shuffle_epi8(decoded, shuffle);
    unsafe { _mm_storeu_si128(output.as_mut_ptr().add(*dst) as *mut __m128i, packed) };
    *dst += keep;
}

/// SSE2 implementation: process 16 bytes at a time.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
pub(super) unsafe fn decode_normal_run_sse2(
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
pub(super) unsafe fn decode_normal_run_ssse3(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_sse2(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
pub(super) unsafe fn decode_normal_run_avx(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_sse2(input, start, output, dst_start) }
}

// ---------------------------------------------------------------------------
// Faithful 128-bit port of rapidyenc `do_decode_sse<isRaw=true, searchEnd=false>`
// (decoder_sse_base.h), covering four ISA tiers via two compile-time bools:
//   * FAST_MATCH = use_isa >= SSSE3  (pshufb specials + palignr carry)
//   * BLEND_ADD  = use_isa >= SSE4.1 (pblendvb escape offsets)
// giving SSE2=(F,B)=(false,false); SSSE3=(true,false); SSE4.1/AVX=(true,true).
//
// The window is 32 bytes = two `__m128i` lanes A/B, matching the oracle's
// `sizeof(__m128i)*2`; the special mask is a `u32` = `movemaskA | movemaskB<<16`.
// Decoder state lives entirely in registers across windows (`esc_first`,
// `yenc_offset`, and the ISA-split straddle-dot carry `min_mask`/`lf_compare`),
// exactly the oracle's `escFirst`/`yencOffset`/`minMask`/`lfCompare`. The
// portable scalar `u32` bit-math (`fix_eq_mask`, `escaped`, `esc_first`, `skip`)
// is byte-identical to the AVX2/AVX-512 raw ports; only the vector ops change
// per tier. Mirrors the STRUCTURE of `decode_kernel_avx2_raw`.
// ---------------------------------------------------------------------------

/// SSE2 unshuffle table for [`sse_compact_vect`]: row `k` holds `k` leading
/// `0xff` bytes then `0x00`, so a byte-blend removes lane byte `k`. Ports the
/// oracle's `unshufMask` (only the 16 rows a 16-bit lane mask can index).
#[cfg(target_arch = "x86_64")]
fn sse2_unshuf_table() -> &'static [[u8; 16]; 16] {
    use std::sync::OnceLock;

    static TABLE: OnceLock<[[u8; 16]; 16]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table = [[0u8; 16]; 16];
        for (k, row) in table.iter_mut().enumerate() {
            for (j, byte) in row.iter_mut().enumerate() {
                *byte = if j < k { 0xff } else { 0x00 };
            }
        }
        table
    })
}

/// SSE2 vector compaction: remove the lane bytes flagged in the low 16 bits of
/// `mask16`, packing the survivors toward byte 0. Literal port of the oracle's
/// `sse2_compact_vect`; iterates set bits HIGH-to-LOW so removing a higher byte
/// index never invalidates a lower one.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn sse_compact_vect(
    mask16: u32,
    mut data: std::arch::x86_64::__m128i,
) -> std::arch::x86_64::__m128i {
    use std::arch::x86_64::*;

    let table = sse2_unshuf_table();
    let mut m = mask16 & 0xffff;
    while m != 0 {
        let bit = 31 - m.leading_zeros(); // highest set bit, 0..=15
        m ^= 1 << bit;
        let merge = _mm_loadu_si128(table[bit as usize].as_ptr() as *const __m128i);
        data = _mm_or_si128(
            _mm_and_si128(merge, data),
            _mm_andnot_si128(merge, _mm_srli_si128::<1>(data)),
        );
    }
    data
}

/// Escaped-byte offset application, SSE2-only body (loadu + add), used by the
/// collision path across all tiers. Produces `-42` on ordinary bytes and
/// `-106` (= -42-64) on escaped bytes — byte-identical to the oracle's
/// `yencOffset` + `eqAdd` LUT combination. `escaped` bit 0 already carries the
/// pending inter-window escape, so no separate `yenc_offset` byte-0 patch is
/// needed here.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn sse_escape_decode(
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
    let off = _mm_loadu_si128(offsets.as_ptr() as *const __m128i);
    _mm_add_epi8(block, off)
}

/// Generic 128-bit raw-decode body. Inline-only (no `#[target_feature]` of its
/// own) so the tier wrappers supply the ISA and the SSSE3/SSE4.1 intrinsics in
/// the `FAST_MATCH`/`BLEND_ADD` branches compile at the wrapper's feature level
/// while the dead branches are const-folded away for the lower tiers.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn sse_raw_body<const FAST_MATCH: bool, const BLEND_ADD: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    use std::arch::x86_64::*;
    const WIDTH: usize = 32;

    let mut src = 0usize;
    let mut dst = 0usize;
    let tail = WIDTH - 1 + 4; // 35
    let simd_limit = input.len().saturating_sub(tail);

    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());
    let sub106 = _mm_set1_epi8(106i8.wrapping_neg());
    let neg64 = _mm_set1_epi8(-64);
    let dot = _mm_set1_epi8(b'.' as i8);
    let eq_needle = _mm_set1_epi8(b'=' as i8);
    let cr = _mm_set1_epi8(b'\r' as i8);
    let lf = _mm_set1_epi8(b'\n' as i8);
    // Single 16-byte specials LUT (`_mm_shuffle_epi8` is 16-lane): slot i maps
    // `.`\n`\r`= to itself, everything else to -1 (never self-matches).
    let special_lut = _mm_set_epi8(
        -1,
        b'=' as i8,
        b'\r' as i8,
        -1,
        -1,
        b'\n' as i8,
        -1,
        -1,
        -1,
        -1,
        -1,
        -1,
        -1,
        -1,
        -1,
        b'.' as i8,
    );

    // entry state -> escFirst / nextMask (oracle `_do_decode_simd` switch subset).
    let mut esc_first: u64 = (state.state == DecoderState::Eq) as u64;
    let entry_next_mask: u16 = match state.state {
        DecoderState::CrLf if input[0] == b'.' => 1,
        DecoderState::Cr if input.len() >= 2 && input[0] == b'\n' && input[1] == b'.' => 2,
        _ => 0,
    };

    // byte 0 of yenc_offset carries a pending escape (-106 = -42-64).
    let mut yenc_offset = if esc_first != 0 {
        _mm_xor_si128(sub42, _mm_cvtsi32_si128(0x40))
    } else {
        sub42
    };
    // Straddle-dot carry. FAST tiers force a line-start dot to hit LUT slot 0
    // via a zeroed `min_mask` byte; SSE2 folds `.` into the `\n` compare via
    // `lf_compare`. entry_next_mask 1 -> byte 0, 2 -> byte 1.
    let mut min_mask = dot;
    let mut lf_compare = lf;
    if entry_next_mask != 0 {
        if FAST_MATCH {
            let word: i32 = if entry_next_mask == 1 { 0x2e00 } else { 0x002e };
            min_mask = _mm_insert_epi16::<0>(min_mask, word);
        } else {
            let word: i32 = if entry_next_mask == 1 { 0x0a2e } else { 0x2e0a };
            lf_compare = _mm_insert_epi16::<0>(lf_compare, word);
        }
    }

    if input.len() > WIDTH * 2 {
        while src + WIDTH <= simd_limit {
            let o_data_a = _mm_loadu_si128(input.as_ptr().add(src) as *const __m128i);
            let o_data_b = _mm_loadu_si128(input.as_ptr().add(src + 16) as *const __m128i);

            // --- special-char detection -----------------------------------
            let cmp_a;
            let cmp_b;
            let mut cmp_eq_a = _mm_setzero_si128();
            let mut cmp_eq_b = _mm_setzero_si128();
            let mut cmp_cr_a = _mm_setzero_si128();
            let mut cmp_cr_b = _mm_setzero_si128();
            if FAST_MATCH {
                cmp_a = _mm_cmpeq_epi8(
                    o_data_a,
                    _mm_shuffle_epi8(special_lut, _mm_min_epu8(o_data_a, min_mask)),
                );
                cmp_b = _mm_cmpeq_epi8(
                    o_data_b,
                    _mm_shuffle_epi8(special_lut, _mm_min_epu8(o_data_b, dot)),
                );
            } else {
                cmp_eq_a = _mm_cmpeq_epi8(o_data_a, eq_needle);
                cmp_eq_b = _mm_cmpeq_epi8(o_data_b, eq_needle);
                cmp_cr_a = _mm_cmpeq_epi8(o_data_a, cr);
                cmp_cr_b = _mm_cmpeq_epi8(o_data_b, cr);
                cmp_a = _mm_or_si128(
                    _mm_or_si128(_mm_cmpeq_epi8(o_data_a, lf_compare), cmp_cr_a),
                    cmp_eq_a,
                );
                cmp_b = _mm_or_si128(
                    _mm_or_si128(_mm_cmpeq_epi8(o_data_b, lf), cmp_cr_b),
                    cmp_eq_b,
                );
            }

            // Non-BLEND tiers add the carried offset to lane A up front.
            let mut data_a = if !BLEND_ADD {
                _mm_add_epi8(o_data_a, yenc_offset)
            } else {
                _mm_setzero_si128()
            };

            let mut mask: u32 =
                (_mm_movemask_epi8(cmp_a) as u32) | ((_mm_movemask_epi8(cmp_b) as u32) << 16);

            if mask != 0 {
                if FAST_MATCH {
                    cmp_eq_a = _mm_cmpeq_epi8(o_data_a, eq_needle);
                    cmp_eq_b = _mm_cmpeq_epi8(o_data_b, eq_needle);
                }
                let mask_eq: u32 = (_mm_movemask_epi8(cmp_eq_a) as u32)
                    | ((_mm_movemask_epi8(cmp_eq_b) as u32) << 16);

                // --- \r\n. dot-unstuffing (isRaw) --------------------------
                if mask != mask_eq {
                    let tmp2a = _mm_loadu_si128(input.as_ptr().add(src + 2) as *const __m128i);
                    let tmp2b = _mm_loadu_si128(input.as_ptr().add(src + 18) as *const __m128i);
                    if FAST_MATCH {
                        cmp_cr_a = _mm_cmpeq_epi8(o_data_a, cr);
                        cmp_cr_b = _mm_cmpeq_epi8(o_data_b, cr);
                    }
                    let m2cr_a = _mm_and_si128(cmp_cr_a, _mm_cmpeq_epi8(tmp2a, dot));
                    let m2cr_b = _mm_and_si128(cmp_cr_b, _mm_cmpeq_epi8(tmp2b, dot));
                    let partial = _mm_movemask_epi8(_mm_or_si128(m2cr_a, m2cr_b));
                    if partial != 0 {
                        let m1lf_a = _mm_cmpeq_epi8(
                            lf,
                            _mm_loadu_si128(input.as_ptr().add(src + 1) as *const __m128i),
                        );
                        let m1lf_b = _mm_cmpeq_epi8(
                            lf,
                            _mm_loadu_si128(input.as_ptr().add(src + 17) as *const __m128i),
                        );
                        // recompute cmpCr from the aligned window reads
                        cmp_cr_a = _mm_cmpeq_epi8(o_data_a, cr);
                        cmp_cr_b = _mm_cmpeq_epi8(o_data_b, cr);
                        let m1nl_a = _mm_and_si128(m1lf_a, cmp_cr_a);
                        let m1nl_b = _mm_and_si128(m1lf_b, cmp_cr_b);
                        let m2nldot_a = _mm_and_si128(m2cr_a, m1nl_a);
                        let mut m2nldot_b = _mm_and_si128(m2cr_b, m1nl_b);
                        mask |= (_mm_movemask_epi8(m2nldot_a) as u32) << 2;
                        mask |= (_mm_movemask_epi8(m2nldot_b) as u32) << 18; // u32 drops bits >=32
                        m2nldot_b = _mm_srli_si128::<14>(m2nldot_b);
                        if FAST_MATCH {
                            min_mask = _mm_subs_epu8(dot, m2nldot_b);
                        } else {
                            // '.' | '\n' == '.' folds the carry into lf_compare.
                            lf_compare = _mm_or_si128(_mm_and_si128(m2nldot_b, dot), lf);
                        }
                    } else if FAST_MATCH {
                        min_mask = dot;
                    } else {
                        lf_compare = lf;
                    }
                }
                // when mask == mask_eq the carry is intentionally left intact.

                // Non-BLEND tiers add -42 to lane B here.
                let mut data_b = if !BLEND_ADD {
                    _mm_add_epi8(o_data_b, sub42)
                } else {
                    _mm_setzero_si128()
                };

                // --- escape resolution (portable u32 bit-math) ------------
                let esc_first_in = esc_first as u32;
                let eq_shift1 = (mask_eq << 1) | esc_first_in;
                let collision = (mask_eq & eq_shift1) != 0;
                let fixed_eq: u32 = if collision {
                    fix_eq_mask(mask_eq as u64, eq_shift1 as u64) as u32
                } else {
                    mask_eq
                };
                let escaped = (fixed_eq << 1) | esc_first_in;
                esc_first = (fixed_eq >> 31) as u64;
                let skip = mask & !escaped;

                if collision {
                    // Consecutive-`=` run: rebuild each lane from the corrected
                    // escaped mask (== oracle `yencOffset` + `eqAdd`).
                    data_a = sse_escape_decode(o_data_a, (escaped & 0xffff) as u16);
                    data_b = sse_escape_decode(o_data_b, ((escaped >> 16) & 0xffff) as u16);
                    yenc_offset = _mm_xor_si128(
                        sub42,
                        _mm_slli_epi16::<6>(_mm_cvtsi32_si128(esc_first as i32)),
                    );
                } else if BLEND_ADD {
                    // Isolated escapes: select -106 straight from the `=`
                    // compares shifted one byte (lane A via slli, lane B via
                    // the cross-lane palignr from lane A's top byte).
                    data_a = _mm_add_epi8(
                        o_data_a,
                        _mm_blendv_epi8(yenc_offset, sub106, _mm_slli_si128::<1>(cmp_eq_a)),
                    );
                    data_b = _mm_add_epi8(
                        o_data_b,
                        _mm_blendv_epi8(sub42, sub106, _mm_alignr_epi8::<15>(cmp_eq_b, cmp_eq_a)),
                    );
                    yenc_offset = _mm_xor_si128(
                        sub42,
                        _mm_slli_epi16::<6>(_mm_cvtsi32_si128(esc_first as i32)),
                    );
                } else {
                    // SSE2/SSSE3 non-blend: -64 marker shifted onto the byte
                    // after each `=`, carry across the A->B boundary via
                    // palignr (SSSE3) or slli/srli or-merge (SSE2).
                    cmp_eq_a = _mm_and_si128(cmp_eq_a, neg64);
                    cmp_eq_b = _mm_and_si128(cmp_eq_b, neg64);
                    yenc_offset = _mm_add_epi8(sub42, _mm_srli_si128::<15>(cmp_eq_b));
                    if FAST_MATCH {
                        cmp_eq_b = _mm_alignr_epi8::<15>(cmp_eq_b, cmp_eq_a);
                    } else {
                        cmp_eq_b = _mm_or_si128(
                            _mm_slli_si128::<1>(cmp_eq_b),
                            _mm_srli_si128::<15>(cmp_eq_a),
                        );
                    }
                    cmp_eq_a = _mm_slli_si128::<1>(cmp_eq_a);
                    data_a = _mm_add_epi8(data_a, cmp_eq_a);
                    data_b = _mm_add_epi8(data_b, cmp_eq_b);
                }

                // --- compaction (skip == mask & !escaped) -----------------
                if FAST_MATCH {
                    compact_store_16_ssse3(data_a, (skip & 0xffff) as u16, output, &mut dst);
                    compact_store_16_ssse3(
                        data_b,
                        ((skip >> 16) & 0xffff) as u16,
                        output,
                        &mut dst,
                    );
                } else {
                    let packed_a = sse_compact_vect(skip & 0xffff, data_a);
                    _mm_storeu_si128(output.as_mut_ptr().add(dst) as *mut __m128i, packed_a);
                    dst += 16 - (skip & 0xffff).count_ones() as usize;
                    let packed_b = sse_compact_vect(skip >> 16, data_b);
                    _mm_storeu_si128(output.as_mut_ptr().add(dst) as *mut __m128i, packed_b);
                    dst += 16 - (skip >> 16).count_ones() as usize;
                }
            } else {
                // No specials in either lane: bulk decode + store.
                if BLEND_ADD {
                    data_a = _mm_add_epi8(o_data_a, yenc_offset);
                }
                let data_b = _mm_add_epi8(o_data_b, sub42);
                _mm_storeu_si128(output.as_mut_ptr().add(dst) as *mut __m128i, data_a);
                _mm_storeu_si128(output.as_mut_ptr().add(dst + 16) as *mut __m128i, data_b);
                dst += WIDTH;
                esc_first = 0;
                yenc_offset = sub42;
            }
            src += WIDTH;
        }
    }

    // Loop-exit state re-derived from trailing raw bytes (oracle `nextMask` +
    // `escFirst`), identical to the AVX2/AVX-512 raw ports. Only override when
    // the SIMD loop actually advanced: with the `> 64` gate a 65/66-byte input
    // fires the gate but is too short for a 32-byte window (`simd_limit < 32`),
    // so `src` stays 0 and the carried entry state (e.g. `CrLf` with a stuffed
    // dot at byte 0) must survive untouched into the scalar epilogue.
    if src > 0 {
        let out_next_mask: u16 = if src >= 2 && src + 1 < input.len() {
            if input[src - 2] == b'\r' && input[src - 1] == b'\n' && input[src] == b'.' {
                1
            } else if input[src - 1] == b'\r' && input[src] == b'\n' && input[src + 1] == b'.' {
                2
            } else {
                0
            }
        } else {
            0
        };

        state.state = if esc_first != 0 {
            DecoderState::Eq
        } else if out_next_mask == 1 {
            DecoderState::CrLf
        } else if out_next_mask == 2 {
            DecoderState::Cr
        } else {
            DecoderState::None
        };
    }

    while src < input.len() {
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn decode_kernel_sse2_raw(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<false, false>(input, output, state, mode) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn decode_kernel_ssse3_raw(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<true, false>(input, output, state, mode) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1,ssse3")]
unsafe fn decode_kernel_sse41_raw(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<true, true>(input, output, state, mode) }
}
