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
