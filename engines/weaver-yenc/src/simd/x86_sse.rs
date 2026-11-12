// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };

    // Head resolution (search_end only): a terminator/control sequence whose
    // `\r\n` sits in the PREVIOUS chunk is invisible to the flat raw loop, so
    // resolve those entry shapes with the scalar machine first. Gated on the
    // same length as the raw path, so short inputs keep today's routing exactly.
    let mut head_src = 0usize;
    let mut head_dst = 0usize;
    if search_end
        && dot_unstuffing
        && input.len() > 64
        && x86_search_end_head(input, output, state, mode, &mut head_src, &mut head_dst)?
    {
        return Ok(KernelOutcome {
            consumed: head_src,
            written: head_dst,
            end: state.end.into(),
        });
    }

    // Hot path: faithful rapidyenc `do_decode_sse` port at ISA_LEVEL_SSE2
    // (FAST_MATCH=false, BLEND_ADD=false), both `searchEnd` instantiations.
    // Other combos keep the general kernel.
    if dot_unstuffing
        && input.len() - head_src > 64
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        // `head_src`/`head_dst` are 0 unless the head loop ran, so the
        // `::<false>` instantiation always sees the untouched full buffers.
        let outcome = if search_end {
            unsafe {
                decode_kernel_sse2_raw::<true>(
                    &input[head_src..],
                    &mut output[head_dst..],
                    state,
                    mode,
                )
            }
        } else {
            unsafe { decode_kernel_sse2_raw::<false>(input, output, state, mode) }
        };
        return x86_fold_head(outcome, head_src, head_dst);
    }
    let outcome = unsafe {
        decode_kernel_simd64(
            &input[head_src..],
            &mut output[head_dst..],
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_sse2_block,
        )
    };
    x86_fold_head(outcome, head_src, head_dst)
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
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };

    let mut head_src = 0usize;
    let mut head_dst = 0usize;
    if search_end
        && dot_unstuffing
        && input.len() > 64
        && x86_search_end_head(input, output, state, mode, &mut head_src, &mut head_dst)?
    {
        return Ok(KernelOutcome {
            consumed: head_src,
            written: head_dst,
            end: state.end.into(),
        });
    }

    // Hot path: faithful rapidyenc `do_decode_sse` port at ISA_LEVEL_SSSE3
    // (FAST_MATCH=true, BLEND_ADD=false), both `searchEnd` instantiations.
    // Other combos keep the general kernel.
    if dot_unstuffing
        && input.len() - head_src > 64
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
        && sse_compact_table_aligned()
    {
        let outcome = if search_end {
            unsafe {
                decode_kernel_ssse3_raw::<true>(
                    &input[head_src..],
                    &mut output[head_dst..],
                    state,
                    mode,
                )
            }
        } else {
            unsafe { decode_kernel_ssse3_raw::<false>(input, output, state, mode) }
        };
        return x86_fold_head(outcome, head_src, head_dst);
    }
    let outcome = unsafe {
        decode_kernel_simd64_ssse3_line_aware(
            &input[head_src..],
            &mut output[head_dst..],
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_ssse3_block,
        )
    };
    x86_fold_head(outcome, head_src, head_dst)
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
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };

    let mut head_src = 0usize;
    let mut head_dst = 0usize;
    if search_end
        && dot_unstuffing
        && input.len() > 64
        && x86_search_end_head(input, output, state, mode, &mut head_src, &mut head_dst)?
    {
        return Ok(KernelOutcome {
            consumed: head_src,
            written: head_dst,
            end: state.end.into(),
        });
    }

    // Hot path: faithful rapidyenc `do_decode_sse` port at ISA_LEVEL_SSE4_POPCNT
    // (FAST_MATCH=true, BLEND_ADD=true), both `searchEnd` instantiations. Other
    // combos keep the general kernel.
    if dot_unstuffing
        && input.len() - head_src > 64
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
        && sse_compact_table_aligned()
    {
        let outcome = if search_end {
            unsafe {
                decode_kernel_sse41_raw::<true>(
                    &input[head_src..],
                    &mut output[head_dst..],
                    state,
                    mode,
                )
            }
        } else {
            unsafe { decode_kernel_sse41_raw::<false>(input, output, state, mode) }
        };
        return x86_fold_head(outcome, head_src, head_dst);
    }
    let outcome = unsafe {
        decode_kernel_simd64_ssse3_line_aware(
            &input[head_src..],
            &mut output[head_dst..],
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_sse41_block,
        )
    };
    x86_fold_head(outcome, head_src, head_dst)
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
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };

    let mut head_src = 0usize;
    let mut head_dst = 0usize;
    if search_end
        && dot_unstuffing
        && input.len() > 64
        && x86_search_end_head(input, output, state, mode, &mut head_src, &mut head_dst)?
    {
        return Ok(KernelOutcome {
            consumed: head_src,
            written: head_dst,
            end: state.end.into(),
        });
    }

    // Hot path: rapidyenc's AVX kernel (`do_decode_sse` at
    // ISA_LEVEL_SSE4_POPCNT, VEX-encoded), both `searchEnd` instantiations.
    // Other combos keep the general kernel.
    if dot_unstuffing
        && input.len() - head_src > 64
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
        && sse_compact_table_aligned()
    {
        let outcome = if search_end {
            unsafe {
                decode_kernel_avx_raw::<true>(
                    &input[head_src..],
                    &mut output[head_dst..],
                    state,
                    mode,
                )
            }
        } else {
            unsafe { decode_kernel_avx_raw::<false>(input, output, state, mode) }
        };
        return x86_fold_head(outcome, head_src, head_dst);
    }
    let outcome = unsafe {
        decode_kernel_simd64_ssse3_line_aware(
            &input[head_src..],
            &mut output[head_dst..],
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_avx_block,
        )
    };
    x86_fold_head(outcome, head_src, head_dst)
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

    let (eq_cmp, eq) = if specials != 0 {
        unsafe { sse_eq_compares(vectors) }
    } else {
        ([_mm_setzero_si128(); 4], 0)
    };
    let esc_first = esc_first as u64;
    let eq_shift1 = (eq << 1) | esc_first;
    // `fix_eq_mask` is the identity on a mask with no consecutive `=`, so this
    // is value-identical to running it unconditionally.
    let collision = (eq & eq_shift1) != 0;
    let fixed_eq = if collision {
        fix_eq_mask(eq, eq_shift1)
    } else {
        eq
    };
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

    // Isolated escapes (the overwhelmingly common case) come straight off the
    // `=` compares; only a genuine consecutive-`=` run needs the corrected
    // `escaped` mask expanded through the scalar offset array.
    let decoded = if collision {
        [
            unsafe { sse_escape_decode(a, (escaped & 0xffff) as u16) },
            unsafe { sse_escape_decode(b, ((escaped >> 16) & 0xffff) as u16) },
            unsafe { sse_escape_decode(c, ((escaped >> 32) & 0xffff) as u16) },
            unsafe { sse_escape_decode(d, ((escaped >> 48) & 0xffff) as u16) },
        ]
    } else {
        unsafe { sse_decode_isolated_escapes::<false>(vectors, eq_cmp, esc_first != 0) }
    };

    let table = compact_table_16();
    unsafe { compact_store_16_ssse3(decoded[0], (skip & 0xffff) as u16, table, output, dst) };
    unsafe {
        compact_store_16_ssse3(
            decoded[1],
            ((skip >> 16) & 0xffff) as u16,
            table,
            output,
            dst,
        )
    };
    unsafe {
        compact_store_16_ssse3(
            decoded[2],
            ((skip >> 32) & 0xffff) as u16,
            table,
            output,
            dst,
        )
    };
    unsafe {
        compact_store_16_ssse3(
            decoded[3],
            ((skip >> 48) & 0xffff) as u16,
            table,
            output,
            dst,
        )
    };

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

    let (eq_cmp, eq) = if specials != 0 {
        unsafe { sse_eq_compares(vectors) }
    } else {
        ([_mm_setzero_si128(); 4], 0)
    };
    let esc_first = esc_first as u64;
    let eq_shift1 = (eq << 1) | esc_first;
    // `fix_eq_mask` is the identity on a mask with no consecutive `=`, so this
    // is value-identical to running it unconditionally.
    let collision = (eq & eq_shift1) != 0;
    let fixed_eq = if collision {
        fix_eq_mask(eq, eq_shift1)
    } else {
        eq
    };
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

    // Isolated escapes (the overwhelmingly common case) come straight off the
    // `=` compares; only a genuine consecutive-`=` run needs the corrected
    // `escaped` mask expanded through the scalar offset array.
    let decoded = if collision {
        [
            unsafe { sse_escape_decode(a, (escaped & 0xffff) as u16) },
            unsafe { sse_escape_decode(b, ((escaped >> 16) & 0xffff) as u16) },
            unsafe { sse_escape_decode(c, ((escaped >> 32) & 0xffff) as u16) },
            unsafe { sse_escape_decode(d, ((escaped >> 48) & 0xffff) as u16) },
        ]
    } else {
        unsafe { sse_decode_isolated_escapes::<true>(vectors, eq_cmp, esc_first != 0) }
    };

    let table = compact_table_16();
    unsafe { compact_store_16_ssse3(decoded[0], (skip & 0xffff) as u16, table, output, dst) };
    unsafe {
        compact_store_16_ssse3(
            decoded[1],
            ((skip >> 16) & 0xffff) as u16,
            table,
            output,
            dst,
        )
    };
    unsafe {
        compact_store_16_ssse3(
            decoded[2],
            ((skip >> 32) & 0xffff) as u16,
            table,
            output,
            dst,
        )
    };
    unsafe {
        compact_store_16_ssse3(
            decoded[3],
            ((skip >> 48) & 0xffff) as u16,
            table,
            output,
            dst,
        )
    };

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

// The four per-lane `=` compares of a 64-byte block plus the 64-bit `=` mask
// they reduce to. Keeping the compare vectors alive is what lets the escape
// offsets be selected straight off them ([`sse_decode_isolated_escapes`])
// instead of rebuilding a byte mask from `escaped` through memory.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn sse_eq_compares(
    vectors: [std::arch::x86_64::__m128i; 4],
) -> ([std::arch::x86_64::__m128i; 4], u64) {
    use std::arch::x86_64::*;

    let needle = _mm_set1_epi8(b'=' as i8);
    let cmp = [
        _mm_cmpeq_epi8(vectors[0], needle),
        _mm_cmpeq_epi8(vectors[1], needle),
        _mm_cmpeq_epi8(vectors[2], needle),
        _mm_cmpeq_epi8(vectors[3], needle),
    ];
    let mask = (_mm_movemask_epi8(cmp[0]) as u16 as u64)
        | ((_mm_movemask_epi8(cmp[1]) as u16 as u64) << 16)
        | ((_mm_movemask_epi8(cmp[2]) as u16 as u64) << 32)
        | ((_mm_movemask_epi8(cmp[3]) as u16 as u64) << 48);
    (cmp, mask)
}

// Isolated-escape decode for the four 16-byte lanes of a 64-byte block — the
// case `escaped == (eq << 1) | esc_first`, i.e. no consecutive-`=` run. The
// escaped lanes are exactly the `=` compares shifted one byte, so they come
// straight out of the compare vectors (the shape [`sse_raw_body`] already
// uses); lane 0's cross-block carry rides in through `yenc_offset` byte 0 (the
// oracle's `-42-64` trick). No mask→memory round-trip anywhere.
//
// `BLEND_ADD` picks the SSE4.1 `pblendvb` offset select; the SSSE3 tier adds
// the `-64` marker onto `-42` instead. Both need SSSE3 (`palignr`) for the
// lane-to-lane escape carry, so this is never instantiated for plain SSE2.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn sse_decode_isolated_escapes<const BLEND_ADD: bool>(
    vectors: [std::arch::x86_64::__m128i; 4],
    eq_cmp: [std::arch::x86_64::__m128i; 4],
    esc_first: bool,
) -> [std::arch::x86_64::__m128i; 4] {
    use std::arch::x86_64::*;

    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());
    let sub106 = _mm_set1_epi8(106i8.wrapping_neg());
    let neg64 = _mm_set1_epi8(-64);
    // byte 0 becomes -106 when an escape straddled in from the previous block.
    let yenc_offset = if esc_first {
        _mm_xor_si128(sub42, _mm_cvtsi32_si128(0x40))
    } else {
        sub42
    };
    // Lane i's escaped bytes are lane i's `=` compare shifted up one byte, with
    // lane i-1's top byte carried in through palignr.
    let sel = [
        _mm_slli_si128::<1>(eq_cmp[0]),
        _mm_alignr_epi8::<15>(eq_cmp[1], eq_cmp[0]),
        _mm_alignr_epi8::<15>(eq_cmp[2], eq_cmp[1]),
        _mm_alignr_epi8::<15>(eq_cmp[3], eq_cmp[2]),
    ];
    let base = [yenc_offset, sub42, sub42, sub42];
    let mut decoded = [_mm_setzero_si128(); 4];
    for lane in 0..4 {
        decoded[lane] = if BLEND_ADD {
            _mm_add_epi8(
                vectors[lane],
                _mm_blendv_epi8(base[lane], sub106, sel[lane]),
            )
        } else {
            _mm_add_epi8(
                _mm_add_epi8(vectors[lane], base[lane]),
                _mm_and_si128(sel[lane], neg64),
            )
        };
    }
    decoded
}

// Line-aware 64-byte-block driver for the pre-AVX2 tiers in the portable
// binary: consult the caller's line-length hint, try the fast whole-line
// path first, and fall back to the generic 64-byte block decode. Mirrors the
// AVX-512/VBMI2 line-aware kernel structure with SSSE3-width vectors.
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

// Whole-line fast path for the SSSE3 tier: decode one complete yEnc line
// (hint-length plus CRLF) in a single pass when the window holds it, bailing
// to the block path on escapes at boundaries, stuffed dots, or short input.
// Same guards and bail conditions as `try_decode_avx512_vbmi2_line`, with
// 4x16-byte vectors instead of one 512-bit vector.
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
    let table = compact_table_16();
    let dst_start = *dst;
    let mut esc_first = 0u64;
    for chunk_idx in 0..chunks {
        let vectors = load4(src + chunk_idx * WIDTH);
        let crlf = unsafe { sse2_mask64(vectors, b'\r') | sse2_mask64(vectors, b'\n') };
        if crlf != 0 {
            *dst = dst_start;
            return Ok(None);
        }
        let (eq_cmp, eq) = unsafe { sse_eq_compares(vectors) };
        let eq_shift1 = (eq << 1) | esc_first;
        let collision = (eq & eq_shift1) != 0;
        let fixed_eq = if collision {
            fix_eq_mask(eq, eq_shift1)
        } else {
            eq
        };
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
            let decoded = if collision {
                [
                    unsafe { sse_escape_decode(vectors[0], (escaped & 0xffff) as u16) },
                    unsafe { sse_escape_decode(vectors[1], ((escaped >> 16) & 0xffff) as u16) },
                    unsafe { sse_escape_decode(vectors[2], ((escaped >> 32) & 0xffff) as u16) },
                    unsafe { sse_escape_decode(vectors[3], ((escaped >> 48) & 0xffff) as u16) },
                ]
            } else {
                unsafe { sse_decode_isolated_escapes::<false>(vectors, eq_cmp, esc_first != 0) }
            };
            for (group, &vector) in decoded.iter().enumerate() {
                let group_skip = ((skip >> (group * 16)) & 0xffff) as u16;
                unsafe { compact_store_16_ssse3(vector, group_skip, table, output, dst) };
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
    table: &[[u8; 16]; 32768],
    output: &mut [u8],
    dst: &mut usize,
) {
    use std::arch::x86_64::*;

    // The caller guarantees 64 spare output bytes per block, so each of the
    // four stores can write a full 16-byte vector; bytes past `keep` are
    // overwritten by the next store.
    debug_assert!(output.len().saturating_sub(*dst) >= 16);
    let keep = 16 - skip_mask.count_ones() as usize;
    // `table` is hoisted by the caller: the shuffle LUT lives behind a
    // `OnceLock`, and fetching it here ran the atomic acquire load four times
    // per 64-byte block.
    let shuffle =
        unsafe { _mm_loadu_si128(table[(skip_mask & 0x7fff) as usize].as_ptr() as *const __m128i) };
    let packed = _mm_shuffle_epi8(decoded, shuffle);
    unsafe { _mm_storeu_si128(output.as_mut_ptr().add(*dst) as *mut __m128i, packed) };
    *dst += keep;
}

// SSE2 implementation: process 16 bytes at a time.
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

// Escaped-byte offset application for the general kernels' collision path:
// `-42` on ordinary bytes and `-106` (= -42-64) on escaped bytes. `escaped`
// bit 0 already carries the pending inter-window escape.
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

// ---------------------------------------------------------------------------
// Port of rapidyenc's 128-bit raw decoder: `do_decode_sse<isRaw=true,
// searchEnd>` (decoder_sse_base.h:99-697), driven the way `_do_decode_simd`
// drives it (decoder_common.h:33-162). One generic body covers the four ISA
// tiers through the oracle's own switches:
//   * FAST_MATCH = use_isa >= SSSE3 (`pshufb` specials, `palignr` carries)
//   * BLEND_ADD  = use_isa >= SSE4.1 (`pblendvb` escape offsets)
//   * POPCNT     = `use_isa & ISA_FEATURE_POPCNT` (only the AVX tier; the
//     others count kept bytes through `BitsSetTable256inv`, as rapidyenc's
//     SSE2 and SSSE3 kernels do, decoder_sse_base.h:666-669)
// SSE2 = (F,F,F), SSSE3 = (T,F,F), SSE4.1 = (T,T,F), AVX = (T,T,T). rapidyenc
// has no SSE4.1 kernel; that tier is its SSSE3 kernel with the SSE4.1 blend,
// which is exactly what rapidyenc's AVX kernel runs minus POPCNT.
//
// Kept as the oracle has it:
// - the input is head-aligned to 32 bytes by the scalar machine, then every
//   window is two aligned loads (decoder_common.h:40-47, :139-140);
// - one negative induction variable counts up to zero (`for(i = -len; i;
//   i += 32)`, :138), and the output is one running pointer `p`;
// - the span is the oracle's `dLen`: `len - lenBuffer` rounded up to the
//   window, `lenBuffer` 31+4 with the end search and 31+2 without (:49-51,
//   :130-131);
// - the state carried across windows is `escFirst`, `yencOffset` and the
//   straddling-dot carry (`minMask` for SSSE3+, `lfCompare` for SSE2);
// - `\r\n.` is stripped in the loop, the end probes run before the dot merge,
//   the collision test is the oracle's wide `mask & maskEqShift1` (so the
//   common path compacts `mask` itself), and a collision corrects `mask` and
//   adds the `eqAdd` rows (:509-561);
// - the `cmpCr` recomputations read the window again through a volatile load
//   (`force_align_read_128`, :282-283, :461-462) so they do not keep two more
//   vectors alive across the probe.
// ---------------------------------------------------------------------------

// The oracle's lookup tables (`decoder_sse_init`, decoder_sse2.cc:7-26), in
// one static as the oracle keeps them in one `SSELookups` block, so the loop
// addresses all of them from one base register.
#[cfg(target_arch = "x86_64")]
#[repr(C, align(16))]
struct SseLookups {
    // `eqAdd`: for each 8-bit escape mask, `-64` (0xc0) in every byte whose
    // bit is set.
    eq_add: [u64; 256],
    // `unshufMask` rows 0..16 (the BSR rows): row `k` keeps bytes below `k`
    // and takes the byte above for the rest, which deletes byte `k`.
    unshuf_mask: [[u8; 16]; 16],
    // `BitsSetTable256inv`: 8 minus the number of set bits, i.e. the bytes of
    // an 8-lane group that are kept.
    bits_set_inv: [u8; 256],
}

#[cfg(target_arch = "x86_64")]
static SSE_LOOKUPS: SseLookups = SseLookups {
    eq_add: {
        let mut table = [0u64; 256];
        let mut i = 0;
        while i < 256 {
            let mut k = 0;
            while k < 8 {
                if i & (1 << k) != 0 {
                    table[i] |= 0xc0u64 << (k * 8);
                }
                k += 1;
            }
            i += 1;
        }
        table
    },
    unshuf_mask: {
        let mut table = [[0u8; 16]; 16];
        let mut k = 0;
        while k < 16 {
            let mut j = 0;
            while j < 16 {
                if k > j {
                    table[k][j] = 0xff;
                }
                j += 1;
            }
            k += 1;
        }
        table
    },
    bits_set_inv: {
        let mut table = [0u8; 256];
        let mut i = 0;
        while i < 256 {
            table[i] = 8 - (i as u8).count_ones() as u8;
            i += 1;
        }
        table
    },
};

// Whether the shared compaction table is 16-byte aligned, as the FAST_MATCH
// raw loops read its rows with aligned loads (folded into `pshufb` like the
// oracle's `_mm_load_si128` of `compact`). The allocator aligns it in
// practice; an unaligned table keeps the general kernel.
#[cfg(target_arch = "x86_64")]
#[inline]
fn sse_compact_table_aligned() -> bool {
    compact_table_16().as_ptr() as usize & 15 == 0
}

// `fix_eqMask` (decoder_common.h:209-226) at the window's 32-bit width.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn sse_fix_eq_mask(mask: u32, mask_shift1: u32) -> u32 {
    let start = mask & !mask_shift1;
    let even = 0x5555_5555u32;
    let odd_groups = mask.wrapping_add(start & even);
    (odd_groups ^ even) & mask
}

// `LOAD_HALVES(eqAdd + lo, eqAdd + hi)` (decoder_sse_base.h:187-190): two
// `eqAdd` rows as the low and high halves of one vector (`movq` + `movhpd`).
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn sse_eq_add_halves(lo: u32, hi: u32) -> std::arch::x86_64::__m128i {
    use std::arch::x86_64::*;
    let rows = SSE_LOOKUPS.eq_add.as_ptr();
    _mm_castpd_si128(_mm_loadh_pd(
        _mm_castsi128_pd(_mm_loadl_epi64(rows.add(lo as usize) as *const __m128i)),
        rows.add(hi as usize) as *const f64,
    ))
}

// A `pmovmskb` LLVM cannot see through, for the SSE2 special mask. With the
// plain intrinsic, LLVM keeps the SSE2 end-search loop's mask as a vector
// of bits and byte-reverses it through the stack in every window (90
// single-byte stack moves at the x86-64 baseline, 1.7x the oracle's
// instructions). The instruction emitted is the same.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn sse2_movemask_opaque(v: std::arch::x86_64::__m128i) -> u32 {
    let r: u32;
    std::arch::asm!(
        "pmovmskb {r:e}, {v}",
        r = out(reg) r,
        v = in(xmm_reg) v,
        options(pure, nomem, nostack, preserves_flags),
    );
    r
}

// `sse2_compact_vect` (decoder_sse_base.h:73-95), the BSR arm: delete the
// bytes flagged in `mask` (bits 0..16), highest first, so deleting a higher
// byte never moves a lower one.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn sse2_compact_vect(
    mask: u32,
    mut data: std::arch::x86_64::__m128i,
) -> std::arch::x86_64::__m128i {
    // In asm so the carried vector stays in one register: compiled, LLVM
    // shifts it in place and copies the merge result back each turn, a
    // fourth step on the per-byte dependency chain.
    if mask != 0 {
        std::arch::asm!(
            "2:",
            "bsr {b:e}, {m:e}",
            "btr {m:e}, {b:e}",
            "shl {b:e}, 4",
            "movdqa {row}, xmmword ptr [{tab} + {b}]",
            "movdqa {sh}, {d}",
            "psrldq {sh}, 1",
            "pand {d}, {row}",
            "pandn {row}, {sh}",
            "por {d}, {row}",
            "test {m:e}, {m:e}",
            "jnz 2b",
            m = inout(reg) mask => _,
            b = out(reg) _,
            d = inout(xmm_reg) data,
            row = out(xmm_reg) _,
            sh = out(xmm_reg) _,
            tab = in(reg) SSE_LOOKUPS.unshuf_mask.as_ptr(),
            options(pure, readonly, nostack),
        );
    }
    data
}

// Per tier, whether every window decodes and compacts with no branch on its
// special mask.
#[cfg(target_arch = "x86_64")]
const SSE_FULL_WIDTH: [bool; 4] = [true, true, true, true];

// The window loop (decoder_sse_base.h:138-694) over `span` bytes from the
// aligned `sp`, writing from `p`. Out of line per tier, so the loop is
// register-allocated alone, as the oracle's kernel is, without the head,
// tail and exit-state values live across it.
#[cfg(target_arch = "x86_64")]
struct SseLoopExit {
    // Bytes of the span consumed.
    consumed: usize,
    p: *mut u8,
    esc_first: u32,
    // The window's special mask when an end probe aborts it (oracle
    // `len += i; break;`).
    broke: Option<u32>,
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn, clippy::too_many_lines)]
unsafe fn sse_raw_loop<
    const FAST_MATCH: bool,
    const BLEND_ADD: bool,
    const POPCNT: bool,
    const SEARCH_END: bool,
>(
    sp: *const u8,
    span: usize,
    mut p: *mut u8,
    mut esc_first: u32,
    entry_next_mask: u16,
) -> SseLoopExit {
    use std::arch::x86_64::*;
    const WIDTH: usize = 32;
    let mut broke: Option<u32> = None;

    let tier = if !FAST_MATCH {
        0
    } else if !BLEND_ADD {
        1
    } else if !POPCNT {
        2
    } else {
        3
    };
    // Whether every window takes the escape and compaction path, with no
    // branch on whether it has a special byte (a clean window compacts with
    // the identity row): on real articles about half the windows are clean,
    // with no pattern the predictor can learn.
    // Only with the end search, the shape a download connection decodes.
    let full = SEARCH_END && SSE_FULL_WIDTH[tier];
    let sub42 = _mm_set1_epi8(-42);
    let sub106 = _mm_set1_epi8(-42 - 64);
    let neg64 = _mm_set1_epi8(-64);
    let dot = _mm_set1_epi8(b'.' as i8);
    let eq_needle = _mm_set1_epi8(b'=' as i8);
    let cr = _mm_set1_epi8(b'\r' as i8);
    let lf = _mm_set1_epi8(b'\n' as i8);
    let y_needle = _mm_set1_epi8(b'y' as i8);
    let eq_y = _mm_set1_epi16(0x793d); // "=y"
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
    let table = compact_table_16().as_ptr() as *const u8;
    let bits_inv = SSE_LOOKUPS.bits_set_inv.as_ptr();

    // Entry state -> escFirst / nextMask, from the live state after the
    // alignment steps (decoder_common.h:58-127).
    // decoder_sse_base.h:104-106 and :126-133.
    let mut yenc_offset = if esc_first != 0 {
        _mm_xor_si128(sub42, _mm_cvtsi32_si128(0x40))
    } else {
        sub42
    };
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

    // The oracle counts `i` up from `-len` to 0 (:138); counting from 0 to
    // the span is the same walk, and the end of the span then serves the
    // loop test and the exit position alike.
    let end = span as isize;
    let mut i: isize = 0;

    // Load one window and find its special characters
    // (decoder_sse_base.h:143-174): (oDataA, oDataB, cmpEqA, cmpEqB,
    // cmpCrA, cmpCrB, mask). FAST_MATCH leaves the `=` and CR compares for
    // the specials path, as the oracle does.
    macro_rules! classify {
        ($at:expr) => {{
            let o_data_a = _mm_load_si128(sp.offset($at) as *const __m128i);
            let o_data_b = _mm_load_si128(sp.offset($at + 16) as *const __m128i);
            let cmp_a;
            let cmp_b;
            let cmp_eq_a;
            let cmp_eq_b;
            let cmp_cr_a;
            let cmp_cr_b;
            if FAST_MATCH {
                cmp_a = _mm_cmpeq_epi8(
                    o_data_a,
                    _mm_shuffle_epi8(special_lut, _mm_min_epu8(o_data_a, min_mask)),
                );
                cmp_b = _mm_cmpeq_epi8(
                    o_data_b,
                    _mm_shuffle_epi8(special_lut, _mm_min_epu8(o_data_b, dot)),
                );
                cmp_eq_a = _mm_setzero_si128();
                cmp_eq_b = _mm_setzero_si128();
                cmp_cr_a = _mm_setzero_si128();
                cmp_cr_b = _mm_setzero_si128();
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
            let mask: u32 = if FAST_MATCH {
                (_mm_movemask_epi8(cmp_a) as u32) | ((_mm_movemask_epi8(cmp_b) as u32) << 16)
            } else {
                sse2_movemask_opaque(cmp_a) | (sse2_movemask_opaque(cmp_b) << 16)
            };
            (
                o_data_a, o_data_b, cmp_eq_a, cmp_eq_b, cmp_cr_a, cmp_cr_b, mask,
            )
        }};
    }

    // The windows up to `$limit`.
    macro_rules! windows {
        ($limit:expr) => {
            'windows: while i != $limit {
                let (
                    mut o_data_a,
                    mut o_data_b,
                    mut cmp_eq_a,
                    mut cmp_eq_b,
                    mut cmp_cr_a,
                    mut cmp_cr_b,
                    mut mask,
                ) = classify!(i);

                // :177-179
                let mut data_a = if BLEND_ADD {
                    _mm_setzero_si128()
                } else {
                    _mm_add_epi8(o_data_a, yenc_offset)
                };

                if !full && mask == 0 {
                    // No specials (:684-694).
                    if BLEND_ADD {
                        data_a = _mm_add_epi8(o_data_a, yenc_offset);
                    }
                    _mm_storeu_si128(p as *mut __m128i, data_a);
                    _mm_storeu_si128(p.add(16) as *mut __m128i, _mm_add_epi8(o_data_b, sub42));
                    p = p.add(WIDTH);
                    esc_first = 0;
                    yenc_offset = sub42;

                    // Without the end search, the same loop while windows stay
                    // clean, with the carried `escFirst` and `yencOffset` known
                    // to be their resting values; clang versions the oracle's raw
                    // loop this way, and not its end-search loop, which keeps one
                    // latch here. The first window with specials falls through to
                    // the path below.
                    if !SEARCH_END {
                        i += WIDTH as isize;
                        loop {
                            if i == $limit {
                                break 'windows;
                            }
                            (
                                o_data_a, o_data_b, cmp_eq_a, cmp_eq_b, cmp_cr_a, cmp_cr_b, mask,
                            ) = classify!(i);
                            data_a = if BLEND_ADD {
                                _mm_setzero_si128()
                            } else {
                                _mm_add_epi8(o_data_a, sub42)
                            };
                            if mask != 0 {
                                break;
                            }
                            let clean_a = if BLEND_ADD {
                                _mm_add_epi8(o_data_a, sub42)
                            } else {
                                data_a
                            };
                            _mm_storeu_si128(p as *mut __m128i, clean_a);
                            _mm_storeu_si128(
                                p.add(16) as *mut __m128i,
                                _mm_add_epi8(o_data_b, sub42),
                            );
                            p = p.add(WIDTH);
                            i += WIDTH as isize;
                        }
                    }
                }

                if full || mask != 0 {
                    if FAST_MATCH {
                        cmp_eq_a = _mm_cmpeq_epi8(o_data_a, eq_needle);
                        cmp_eq_b = _mm_cmpeq_epi8(o_data_b, eq_needle);
                    }
                    let mut mask_eq: u32 = (_mm_movemask_epi8(cmp_eq_a) as u32)
                        | ((_mm_movemask_epi8(cmp_eq_b) as u32) << 16);

                    // The escapes and compaction (:506-682).
                    macro_rules! finish {
                        () => {{
                            // :506-507
                            let mut data_b = if BLEND_ADD {
                                _mm_setzero_si128()
                            } else {
                                _mm_add_epi8(o_data_b, sub42)
                            };

                            // :509-630
                            let mask_eq_shift1 = (mask_eq << 1).wrapping_add(esc_first);
                            if mask & mask_eq_shift1 != 0 {
                                // `LIKELIHOOD(0.0001, ...)` (:510).
                                std::hint::cold_path();
                                mask_eq = sse_fix_eq_mask(mask_eq, mask_eq_shift1);
                                mask &= !esc_first;
                                esc_first = mask_eq >> 31;
                                // Drop the bytes after each `=` from the special mask, so
                                // `=\r` and `=\n` are kept as data.
                                mask_eq <<= 1;
                                mask &= !mask_eq;
                                if BLEND_ADD {
                                    data_a = _mm_add_epi8(o_data_a, yenc_offset);
                                    data_b = _mm_add_epi8(o_data_b, sub42);
                                }
                                data_a = _mm_add_epi8(
                                    data_a,
                                    sse_eq_add_halves(mask_eq & 0xff, (mask_eq >> 8) & 0xff),
                                );
                                mask_eq >>= 16;
                                data_b = _mm_add_epi8(
                                    data_b,
                                    sse_eq_add_halves(mask_eq & 0xff, (mask_eq >> 8) & 0xff),
                                );
                                yenc_offset = _mm_xor_si128(
                                    sub42,
                                    _mm_slli_epi16::<6>(_mm_cvtsi32_si128(esc_first as i32)),
                                );
                            } else {
                                esc_first = mask_eq >> 31;
                                if BLEND_ADD {
                                    data_a = _mm_add_epi8(
                                        o_data_a,
                                        _mm_blendv_epi8(
                                            yenc_offset,
                                            sub106,
                                            _mm_slli_si128::<1>(cmp_eq_a),
                                        ),
                                    );
                                    data_b = _mm_add_epi8(
                                        o_data_b,
                                        _mm_blendv_epi8(
                                            sub42,
                                            sub106,
                                            _mm_alignr_epi8::<15>(cmp_eq_b, cmp_eq_a),
                                        ),
                                    );
                                    yenc_offset = _mm_xor_si128(
                                        sub42,
                                        _mm_slli_epi16::<6>(_mm_cvtsi32_si128(esc_first as i32)),
                                    );
                                } else {
                                    cmp_eq_a = _mm_and_si128(cmp_eq_a, neg64);
                                    cmp_eq_b = _mm_and_si128(cmp_eq_b, neg64);
                                    yenc_offset =
                                        _mm_add_epi8(sub42, _mm_srli_si128::<15>(cmp_eq_b));
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
                            }

                            // Compaction (:639-682). `p` may step back past the window's
                            // first output byte between the two stores; every store is
                            // still at or after it.
                            if FAST_MATCH {
                                // Aligned row loads, so they fold into `pshufb` as the
                                // oracle's do; the wrappers enter only with an aligned
                                // table (`sse_compact_table_aligned`).
                                data_a = _mm_shuffle_epi8(
                                    data_a,
                                    _mm_load_si128(table.add(((mask & 0x7fff) << 4) as usize)
                                        as *const __m128i),
                                );
                                _mm_storeu_si128(p as *mut __m128i, data_a);
                                data_b = _mm_shuffle_epi8(
                                    data_b,
                                    _mm_load_si128(table.add(((mask >> 12) & 0x7fff0) as usize)
                                        as *const __m128i),
                                );
                                if POPCNT {
                                    p = p.wrapping_sub((mask & 0xffff).count_ones() as usize);
                                    _mm_storeu_si128(p.wrapping_add(16) as *mut __m128i, data_b);
                                    p = p.wrapping_sub((mask & 0xffff_0000).count_ones() as usize);
                                    p = p.wrapping_add(32);
                                } else {
                                    p = p.add(
                                        *bits_inv.add((mask & 0xff) as usize) as usize
                                            + *bits_inv.add(((mask >> 8) & 0xff) as usize) as usize,
                                    );
                                    _mm_storeu_si128(p as *mut __m128i, data_b);
                                    mask >>= 16;
                                    p = p.add(
                                        *bits_inv.add((mask & 0xff) as usize) as usize
                                            + *bits_inv.add(((mask >> 8) & 0xff) as usize) as usize,
                                    );
                                }
                            } else {
                                data_a = sse2_compact_vect(mask & 0xffff, data_a);
                                _mm_storeu_si128(p as *mut __m128i, data_a);
                                p = p.add(
                                    *bits_inv.add((mask & 0xff) as usize) as usize
                                        + *bits_inv.add(((mask >> 8) & 0xff) as usize) as usize,
                                );
                                mask >>= 16;
                                data_b = sse2_compact_vect(mask, data_b);
                                _mm_storeu_si128(p as *mut __m128i, data_b);
                                p = p.add(
                                    *bits_inv.add((mask & 0xff) as usize) as usize
                                        + *bits_inv.add(((mask >> 8) & 0xff) as usize) as usize,
                                );
                            }
                        }};
                    }

                    // `\r\n.` and the end probes (:199-502).
                    if mask != mask_eq {
                        let tmp2a = _mm_loadu_si128(sp.offset(i + 2) as *const __m128i);
                        let tmp2b = _mm_loadu_si128(sp.offset(i + 18) as *const __m128i);
                        // :225-231; SSSE3+ takes lane A's view from the `=`
                        // compares with `palignr` below instead.
                        let mut match2_eq_a = _mm_setzero_si128();
                        let mut match2_eq_b = _mm_setzero_si128();
                        if SEARCH_END {
                            if !FAST_MATCH {
                                match2_eq_a = _mm_cmpeq_epi8(eq_needle, tmp2a);
                            }
                            match2_eq_b = _mm_cmpeq_epi8(eq_needle, tmp2b);
                        }
                        // :244-252
                        if FAST_MATCH {
                            cmp_cr_a = _mm_cmpeq_epi8(o_data_a, cr);
                            cmp_cr_b = _mm_cmpeq_epi8(o_data_b, cr);
                        }
                        let match2_cr_x_dt_a = _mm_and_si128(cmp_cr_a, _mm_cmpeq_epi8(tmp2a, dot));
                        let match2_cr_x_dt_b = _mm_and_si128(cmp_cr_b, _mm_cmpeq_epi8(tmp2b, dot));
                        let partial_kill_dot_found =
                            _mm_movemask_epi8(_mm_or_si128(match2_cr_x_dt_a, match2_cr_x_dt_b));

                        if partial_kill_dot_found != 0 {
                            // `LIKELIHOOD(0.001, ...)` (:255).
                            std::hint::cold_path();
                            // :277-288
                            let match1_lf_a = _mm_cmpeq_epi8(
                                lf,
                                _mm_loadu_si128(sp.offset(i + 1) as *const __m128i),
                            );
                            let match1_lf_b = _mm_cmpeq_epi8(
                                lf,
                                _mm_loadu_si128(sp.offset(i + 17) as *const __m128i),
                            );
                            cmp_cr_a = _mm_cmpeq_epi8(
                                std::ptr::read_volatile(sp.offset(i) as *const __m128i),
                                cr,
                            );
                            cmp_cr_b = _mm_cmpeq_epi8(
                                std::ptr::read_volatile(sp.offset(i + 16) as *const __m128i),
                                cr,
                            );
                            let match1_nl_a = _mm_and_si128(match1_lf_a, cmp_cr_a);
                            let match1_nl_b = _mm_and_si128(match1_lf_b, cmp_cr_b);
                            let match2_nl_dot_a = _mm_and_si128(match2_cr_x_dt_a, match1_nl_a);
                            let mut match2_nl_dot_b = _mm_and_si128(match2_cr_x_dt_b, match1_nl_b);

                            if SEARCH_END {
                                // `\r\n.\r\n`, `\r\n.=y` and `\r\n=y` (:289-377).
                                let tmp3a = _mm_loadu_si128(sp.offset(i + 3) as *const __m128i);
                                let tmp3b = _mm_loadu_si128(sp.offset(i + 19) as *const __m128i);
                                let tmp4a = _mm_loadu_si128(sp.offset(i + 4) as *const __m128i);
                                let tmp4b = _mm_loadu_si128(sp.offset(i + 20) as *const __m128i);
                                let match3_cr_a = _mm_cmpeq_epi8(cr, tmp3a);
                                let match3_cr_b = _mm_cmpeq_epi8(cr, tmp3b);
                                let match4_lf_a = _mm_cmpeq_epi8(tmp4a, lf);
                                let match4_lf_b = _mm_cmpeq_epi8(tmp4b, lf);
                                let mut match4_eq_y_a = _mm_cmpeq_epi16(tmp4a, eq_y);
                                let mut match4_eq_y_b = _mm_cmpeq_epi16(tmp4b, eq_y);
                                // rapidyenc takes lane A's `=` view with `palignr`
                                // (:327-330); a compare on the shifted load is one
                                // instruction too, and rustc's LLVM loses the
                                // all-or-nothing bytes of the `palignr` result and
                                // spends a register shuffle to rebuild them.
                                if FAST_MATCH {
                                    match2_eq_a = _mm_cmpeq_epi8(eq_needle, tmp2a);
                                }
                                let match3_eq_y_a =
                                    _mm_and_si128(match2_eq_a, _mm_cmpeq_epi8(y_needle, tmp3a));
                                let match3_eq_y_b =
                                    _mm_and_si128(match2_eq_b, _mm_cmpeq_epi8(y_needle, tmp3b));
                                match4_eq_y_a = _mm_slli_epi16::<8>(match4_eq_y_a);
                                match4_eq_y_b = _mm_slli_epi16::<8>(match4_eq_y_b);
                                let mut match4_end_a = _mm_or_si128(
                                    _mm_and_si128(match3_cr_a, match4_lf_a),
                                    _mm_or_si128(match4_eq_y_a, _mm_srli_epi16::<8>(match3_eq_y_a)),
                                );
                                let mut match4_end_b = _mm_or_si128(
                                    _mm_and_si128(match3_cr_b, match4_lf_b),
                                    _mm_or_si128(match4_eq_y_b, _mm_srli_epi16::<8>(match3_eq_y_b)),
                                );
                                match4_end_a = _mm_and_si128(match4_end_a, match2_nl_dot_a);
                                match4_end_b = _mm_and_si128(match4_end_b, match2_nl_dot_b);
                                let match3_end_a = _mm_and_si128(match3_eq_y_a, match1_nl_a);
                                let match3_end_b = _mm_and_si128(match3_eq_y_b, match1_nl_b);
                                let match_end = _mm_movemask_epi8(_mm_or_si128(
                                    _mm_or_si128(match4_end_a, match3_end_a),
                                    _mm_or_si128(match4_end_b, match3_end_b),
                                ));
                                if match_end != 0 {
                                    // `LIKELIHOOD(0.001, ...)` (:371).
                                    std::hint::cold_path();
                                    broke = Some(mask);
                                    break;
                                }
                            }
                            // :387-397
                            mask |= (_mm_movemask_epi8(match2_nl_dot_a) as u32) << 2;
                            mask |= (_mm_movemask_epi8(match2_nl_dot_b) as u32) << 18;
                            match2_nl_dot_b = _mm_srli_si128::<14>(match2_nl_dot_b);
                            if FAST_MATCH {
                                min_mask = _mm_subs_epu8(dot, match2_nl_dot_b);
                            } else {
                                // '.' | '\n' == '.'
                                lf_compare = _mm_or_si128(_mm_and_si128(match2_nl_dot_b, dot), lf);
                            }
                        } else if SEARCH_END {
                            // `\r\n=y` without a stuffed dot (:400-494).
                            let match3_y_a = _mm_cmpeq_epi8(
                                y_needle,
                                _mm_loadu_si128(sp.offset(i + 3) as *const __m128i),
                            );
                            let match3_y_b = _mm_cmpeq_epi8(
                                y_needle,
                                _mm_loadu_si128(sp.offset(i + 19) as *const __m128i),
                            );
                            // :427-430; a compare instead of `palignr`, as above.
                            if FAST_MATCH {
                                match2_eq_a = _mm_cmpeq_epi8(eq_needle, tmp2a);
                            }
                            let match3_eq_y_a = _mm_and_si128(match2_eq_a, match3_y_a);
                            let match3_eq_y_b = _mm_and_si128(match2_eq_b, match3_y_b);
                            if _mm_movemask_epi8(_mm_or_si128(match3_eq_y_a, match3_eq_y_b)) != 0 {
                                // `LIKELIHOOD(0.001, ...)` (:436).
                                std::hint::cold_path();
                                let cr_a = _mm_cmpeq_epi8(
                                    std::ptr::read_volatile(sp.offset(i) as *const __m128i),
                                    cr,
                                );
                                let cr_b = _mm_cmpeq_epi8(
                                    std::ptr::read_volatile(sp.offset(i + 16) as *const __m128i),
                                    cr,
                                );
                                let match1_lf_a = _mm_cmpeq_epi8(
                                    lf,
                                    _mm_loadu_si128(sp.offset(i + 1) as *const __m128i),
                                );
                                let match1_lf_b = _mm_cmpeq_epi8(
                                    lf,
                                    _mm_loadu_si128(sp.offset(i + 17) as *const __m128i),
                                );
                                let end_found = _mm_movemask_epi8(_mm_or_si128(
                                    _mm_and_si128(match3_eq_y_a, _mm_and_si128(match1_lf_a, cr_a)),
                                    _mm_and_si128(match3_eq_y_b, _mm_and_si128(match1_lf_b, cr_b)),
                                ));
                                if end_found != 0 {
                                    broke = Some(mask);
                                    break;
                                }
                            }
                            if FAST_MATCH {
                                min_mask = dot;
                            } else {
                                lf_compare = lf;
                            }
                        } else if FAST_MATCH {
                            min_mask = dot;
                        } else {
                            lf_compare = lf;
                        }
                    }

                    finish!();
                }
                i += WIDTH as isize;
            }
        };
    }

    windows!(end);

    SseLoopExit {
        consumed: i as usize,
        p,
        esc_first,
        broke,
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[inline(never)]
unsafe fn sse_raw_loop_sse2<const SEARCH_END: bool>(
    sp: *const u8,
    span: usize,
    p: *mut u8,
    esc_first: u32,
    entry_next_mask: u16,
) -> SseLoopExit {
    unsafe {
        sse_raw_loop::<false, false, false, SEARCH_END>(sp, span, p, esc_first, entry_next_mask)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
#[inline(never)]
unsafe fn sse_raw_loop_ssse3<const SEARCH_END: bool>(
    sp: *const u8,
    span: usize,
    p: *mut u8,
    esc_first: u32,
    entry_next_mask: u16,
) -> SseLoopExit {
    unsafe {
        sse_raw_loop::<true, false, false, SEARCH_END>(sp, span, p, esc_first, entry_next_mask)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1,ssse3")]
#[inline(never)]
unsafe fn sse_raw_loop_sse41<const SEARCH_END: bool>(
    sp: *const u8,
    span: usize,
    p: *mut u8,
    esc_first: u32,
    entry_next_mask: u16,
) -> SseLoopExit {
    unsafe {
        sse_raw_loop::<true, true, false, SEARCH_END>(sp, span, p, esc_first, entry_next_mask)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,popcnt,sse4.1,ssse3")]
#[inline(never)]
unsafe fn sse_raw_loop_avx<const SEARCH_END: bool>(
    sp: *const u8,
    span: usize,
    p: *mut u8,
    esc_first: u32,
    entry_next_mask: u16,
) -> SseLoopExit {
    unsafe { sse_raw_loop::<true, true, true, SEARCH_END>(sp, span, p, esc_first, entry_next_mask) }
}

// The scalar tail after the window loop, rapidyenc's scalar decoder
// (decoder.cc:13-146) inline: plain bytes in a tight run, and the CR, LF and
// `=` cases of the `None`, `Cr` and `CrLf` states decided here, exactly as
// `decode_scalar_step` decides them. Everything else (a line-start dot, a
// line-start `=` under the end search, an escape cut by the end of input, a
// full output, the dot and end states) takes one `decode_scalar_step`.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn sse_scalar_tail(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<(), YencError> {
    while *src < input.len() {
        match state.state {
            DecoderState::None => {
                decode_plain_run(input, src, output, dst);
                let Some(&byte) = input.get(*src) else {
                    break;
                };
                match byte {
                    b'\r' => {
                        *src += 1;
                        state.state = DecoderState::Cr;
                        continue;
                    }
                    b'\n' => {
                        *src += 1;
                        continue;
                    }
                    _ => {
                        // `=`: decode the escape when its byte and an output
                        // slot are there.
                        if let (Some(&escaped), Some(slot)) =
                            (input.get(*src + 1), output.get_mut(*dst))
                        {
                            *slot = escaped.wrapping_sub(106);
                            *src += 2;
                            *dst += 1;
                            if mode.dot_unstuffing && escaped == b'\r' {
                                state.state = DecoderState::Cr;
                            }
                            continue;
                        }
                    }
                }
            }
            DecoderState::Cr => {
                if input[*src] == b'\n' {
                    *src += 1;
                    state.state = DecoderState::CrLf;
                } else {
                    state.state = DecoderState::None;
                }
                continue;
            }
            DecoderState::CrLf => {
                let byte = input[*src];
                match byte {
                    b'\r' => {
                        *src += 1;
                        state.state = DecoderState::Cr;
                        continue;
                    }
                    b'\n' => {
                        *src += 1;
                        state.state = DecoderState::None;
                        continue;
                    }
                    b'.' if mode.dot_unstuffing => {}
                    b'=' => {}
                    _ => {
                        if let Some(slot) = output.get_mut(*dst) {
                            *slot = byte.wrapping_sub(42);
                            *src += 1;
                            *dst += 1;
                            state.state = DecoderState::None;
                            continue;
                        }
                    }
                }
            }
            _ => {}
        }
        if !decode_scalar_step(input, src, output, dst, state, mode)? {
            break;
        }
    }
    Ok(())
}

// The generic body. Inline-only, so each tier wrapper compiles it at its own
// feature level and the switches fold away.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn, clippy::too_many_lines)]
unsafe fn sse_raw_body<
    const FAST_MATCH: bool,
    const BLEND_ADD: bool,
    const POPCNT: bool,
    const SEARCH_END: bool,
>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    const WIDTH: usize = 32;

    let mut src = 0usize;
    let mut dst = 0usize;

    // Head-align the input to the window with the scalar machine, as the
    // oracle does (decoder_common.h:40-47); at most 31 steps. The end search
    // resolves, before every step, the line-start shapes whose `\r\n` lies
    // behind the span and which the window probes cannot see (the oracle's
    // entry switch, :58-126). The tier wrapper has already resolved them at
    // offset 0, so the probe runs after each step, not before the first.
    while (input.as_ptr() as usize + src) & (WIDTH - 1) != 0 {
        if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)?
            || (SEARCH_END && x86_search_end_head(input, output, state, mode, &mut src, &mut dst)?)
        {
            return Ok(KernelOutcome {
                consumed: src,
                written: dst,
                end: state.end.into(),
            });
        }
    }

    // `lenBuffer` (decoder_common.h:49-51): the bytes the last window may read
    // past its own end, `+4` for the end probes and `+2` for the dot probe.
    let len_buffer = WIDTH - 1 + if SEARCH_END { 4 } else { 2 };
    let remaining = input.len() - src;
    // The window's special mask when an end probe aborts it (oracle
    // `len += i; break;`); the state is set after the loop, keeping the
    // loop's registers for the loop.

    if remaining > len_buffer {
        // `dLen` rounded up to the window (decoder_common.h:130-131). The
        // deepest read of the last window ends at `src + span + 4` (`+2`
        // without the end search), which is at most `input.len()`.
        let span = (remaining - len_buffer + (WIDTH - 1)) & !(WIDTH - 1);

        let esc_first: u32 = (state.state == DecoderState::Eq) as u32;
        let entry_next_mask: u16 = match state.state {
            DecoderState::CrLf if input[src] == b'.' => 1,
            DecoderState::Cr if input[src] == b'\n' && input[src + 1] == b'.' => 2,
            _ => 0,
        };
        let sp = input.as_ptr().add(src);
        let p = output.as_mut_ptr().add(dst);
        let exit = if !FAST_MATCH {
            sse_raw_loop_sse2::<SEARCH_END>(sp, span, p, esc_first, entry_next_mask)
        } else if !BLEND_ADD {
            sse_raw_loop_ssse3::<SEARCH_END>(sp, span, p, esc_first, entry_next_mask)
        } else if !POPCNT {
            sse_raw_loop_sse41::<SEARCH_END>(sp, span, p, esc_first, entry_next_mask)
        } else {
            sse_raw_loop_avx::<SEARCH_END>(sp, span, p, esc_first, entry_next_mask)
        };
        let (i, p, esc_first, broke) = (exit.consumed, exit.p, exit.esc_first, exit.broke);

        let out_base = output.as_mut_ptr();
        dst = p.offset_from(out_base) as usize;
        let span_start = src;
        src = span_start + i;

        // Exit state (decoder_common.h:135-138 with `decoder_set_nextMask`,
        // :180-193). A broken window takes it from the no-backtrack rule.
        if let Some(mask) = broke {
            state.state = x86_break_state(input, src, mask as u64, esc_first as u64);
        } else {
            // The oracle keeps `CrLf`/`Cr` only before a `.`; the scalar
            // machine keeps them before any byte, and a line-start `=` must
            // carry `CrLfEq`, not `Eq`. A `\r` at the end is `Cr` whether or
            // not it was escaped (the `Eq` arm maps `=\r` to `Cr`).
            state.state = if esc_first != 0 {
                if src >= 3 && input[src - 3] == b'\r' && input[src - 2] == b'\n' {
                    DecoderState::CrLfEq
                } else {
                    DecoderState::Eq
                }
            } else if input[src - 1] == b'\r' {
                DecoderState::Cr
            } else if input[src - 2] == b'\r' && input[src - 1] == b'\n' {
                DecoderState::CrLf
            } else {
                DecoderState::None
            };
        }
    }

    // End alignment with the scalar machine (decoder_common.h:145-147).
    sse_scalar_tail(input, &mut src, output, &mut dst, state, mode)?;

    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn decode_kernel_sse2_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<false, false, false, SEARCH_END>(input, output, state, mode) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn decode_kernel_ssse3_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<true, false, false, SEARCH_END>(input, output, state, mode) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1,ssse3")]
unsafe fn decode_kernel_sse41_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<true, true, false, SEARCH_END>(input, output, state, mode) }
}

// rapidyenc's AVX kernel: the SSE4.1 body with POPCNT, VEX-encoded
// (decoder_avx.cc, built `-mavx -mpopcnt`, ISA_LEVEL_SSE4_POPCNT).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,popcnt,sse4.1,ssse3")]
unsafe fn decode_kernel_avx_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<true, true, true, SEARCH_END>(input, output, state, mode) }
}

// The window loop's boundaries, every SSE tier against the scalar machine:
// a special byte or sequence at every position of a buffer, at every head
// alignment, from every entry state the port takes, with and without the
// end search.
#[cfg(all(test, target_arch = "x86_64"))]
mod window_boundary_tests {
    use super::*;

    type Kernel = unsafe fn(
        &[u8],
        &mut [u8],
        &mut KernelState,
        bool,
        bool,
        bool,
    ) -> Result<KernelOutcome, YencError>;

    fn tiers() -> Vec<(&'static str, Kernel)> {
        let mut tiers: Vec<(&'static str, Kernel)> = Vec::new();
        if is_x86_feature_detected!("sse2") {
            tiers.push(("sse2", decode_kernel_sse2));
        }
        if is_x86_feature_detected!("ssse3") {
            tiers.push(("ssse3", decode_kernel_ssse3));
        }
        if is_x86_feature_detected!("sse4.1") && is_x86_feature_detected!("ssse3") {
            tiers.push(("sse4.1", decode_kernel_sse41));
        }
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("popcnt")
            && is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("ssse3")
        {
            tiers.push(("avx", decode_kernel_avx));
        } else {
            eprintln!("window_boundary_tests: avx tier SKIPPED (host has no AVX)");
        }
        tiers
    }

    const ENTRY_STATES: [RapidyencDecodeState; 4] = [
        RapidyencDecodeState::None,
        RapidyencDecodeState::Eq,
        RapidyencDecodeState::Cr,
        RapidyencDecodeState::CrLf,
    ];

    // Decodes `input` placed `offset` bytes past a 64-byte boundary with
    // every tier and with the scalar machine, and asserts they agree.
    fn check(tiers: &[(&'static str, Kernel)], input: &[u8], offset: usize, what: &str) {
        let mut backing = vec![0u8; input.len() + 128];
        let base = backing.as_ptr().align_offset(64) + offset;
        backing[base..base + input.len()].copy_from_slice(input);
        let input = &backing[base..base + input.len()];
        for search_end in [false, true] {
            for entry in ENTRY_STATES {
                let mut want_out = vec![0u8; input.len() + 64];
                let mut want_state = KernelState::from_rapidyenc_state(entry);
                let want = decode_kernel_scalar(
                    input,
                    &mut want_out,
                    &mut want_state,
                    true,
                    true,
                    search_end,
                );
                let want = want.map(|o| {
                    want_out.truncate(o.written);
                    (o.consumed, o.written, o.end)
                });
                for &(name, kernel) in tiers {
                    let mut out = vec![0u8; input.len() + 64];
                    let mut state = KernelState::from_rapidyenc_state(entry);
                    // SAFETY: `tiers` lists only kernels whose features this
                    // CPU has.
                    let got =
                        unsafe { kernel(input, &mut out, &mut state, true, true, search_end) };
                    let got = got.map(|o| {
                        out.truncate(o.written);
                        (o.consumed, o.written, o.end)
                    });
                    assert_eq!(
                        (format!("{got:?}"), &out, &state),
                        (format!("{want:?}"), &want_out, &want_state),
                        "{name} {what} offset {offset} len {} entry {entry:?} search_end {search_end}",
                        input.len()
                    );
                }
            }
        }
    }

    // Plain bytes that decode to anything but a special: `a`..`z` cycling.
    fn plain(len: usize) -> Vec<u8> {
        (0..len).map(|i| b'a' + (i % 26) as u8).collect()
    }

    const PATTERNS: [&[u8]; 13] = [
        b"=",
        b"==",
        b"\r",
        b"\n",
        b"\r\n",
        b"\r\n.",
        b"\r\n..",
        b"\r\n=y",
        b"\r\n.\r\n",
        b"\r\n.=y",
        b"=\r\n",
        b"\r\n=yend\r\n",
        b".\r\n=y",
    ];

    // A pattern at every position of buffers around one, two and three
    // windows, at every head alignment: `=`, CR and CRLF at a window's end,
    // `\r\n.` and `\r\n=y` split across windows, and the span's last window.
    #[test]
    fn patterns_at_every_position_and_alignment() {
        let tiers = tiers();
        for len in [65usize, 66, 70, 96, 100, 131] {
            for pattern in PATTERNS {
                for pos in 0..=len - pattern.len() {
                    let mut input = plain(len);
                    input[pos..pos + pattern.len()].copy_from_slice(pattern);
                    for offset in 0..32 {
                        check(&tiers, &input, offset, &format!("{pattern:?} at {pos}"));
                    }
                }
            }
        }
    }

    // Inputs no longer than the port's 64-byte gate, and the first lengths
    // past it, at every alignment: the port must hand them over or decode
    // them exactly as the scalar machine.
    #[test]
    fn short_inputs_at_every_alignment() {
        let tiers = tiers();
        for len in 0..=72usize {
            for pattern in [&b"\r\n.=y"[..], b"=", b"\r\n=y"] {
                let mut input = plain(len);
                if len >= pattern.len() {
                    let pos = len - pattern.len();
                    input[pos..].copy_from_slice(pattern);
                }
                for offset in 0..32 {
                    check(&tiers, &input, offset, "short");
                }
            }
        }
    }

    // A span that ends on a line break, with or without a pending `=`: the
    // window loop must hand the tail the scalar machine's state (`CrLfEq`,
    // `CrLf`, `Cr`) whatever byte follows, or a later end-searching call
    // reads `=y` as data. The fuzz repro is 62 `a` then `\r\n=` at offset 1.
    #[test]
    fn exit_state_after_trailing_line_break() {
        let tiers = tiers();
        let tails: [&[u8]; 8] = [
            b"\r\n=", b"\r\n", b"\r", b"\r\na", b"\ra", b"=\r", b"=\r\n=", b"a=",
        ];
        for tail in tails {
            for len in [65usize, 66, 67, 70, 96, 97, 98, 99] {
                let mut input = vec![b'a'; len];
                input[len - tail.len()..].copy_from_slice(tail);
                for offset in 0..32 {
                    check(&tiers, &input, offset, &format!("tail {tail:?}"));
                }
            }
        }
    }

    // A pattern at every position in the last hundred bytes of the span, so
    // an escape, a CR/LF or a stuffed dot carries into the last windows and
    // the scalar tail exactly as between any two windows.
    #[test]
    fn patterns_near_the_span_end() {
        let tiers = tiers();
        for len in [70usize, 100, 131] {
            for pattern in PATTERNS {
                for pos in len.saturating_sub(100)..len - pattern.len() {
                    let mut input = plain(len);
                    input[pos..pos + pattern.len()].copy_from_slice(pattern);
                    for offset in [0, 13, 31] {
                        check(&tiers, &input, offset, &format!("{pattern:?} at {pos}"));
                    }
                }
            }
        }
    }

    // Seeded random bodies dense in specials, so every window takes the
    // escape, collision, dot and end-probe paths in mixed orders.
    #[test]
    fn dense_random_bodies() {
        let tiers = tiers();
        const ALPHABET: &[u8] = b"==\r\r\n\n..yyabc\x00\xff\x2a\x29\xd6\x20\x09";
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        for round in 0..1500 {
            let len = 60 + (next() % 400) as usize;
            let input: Vec<u8> = (0..len)
                .map(|_| ALPHABET[(next() % ALPHABET.len() as u64) as usize])
                .collect();
            let offset = (next() % 32) as usize;
            check(&tiers, &input, offset, &format!("round {round}"));
        }
    }
}
