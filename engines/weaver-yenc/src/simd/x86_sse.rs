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

    // AVX reuses the SSE4.1/POPCNT raw kernel (weaver treats AVX == SSE4.1 for
    // the 128-bit decode body, matching `try_decode_avx_block`).
    if dot_unstuffing
        && input.len() - head_src > 64
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
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

/// The four per-lane `=` compares of a 64-byte block plus the 64-bit `=` mask
/// they reduce to. Keeping the compare vectors alive is what lets the escape
/// offsets be selected straight off them ([`sse_decode_isolated_escapes`])
/// instead of rebuilding a byte mask from `escaped` through memory.
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

/// Isolated-escape decode for the four 16-byte lanes of a 64-byte block — the
/// case `escaped == (eq << 1) | esc_first`, i.e. no consecutive-`=` run. The
/// escaped lanes are exactly the `=` compares shifted one byte, so they come
/// straight out of the compare vectors (the shape [`sse_raw_body`] already
/// uses); lane 0's cross-block carry rides in through `yenc_offset` byte 0 (the
/// oracle's `-42-64` trick). No mask→memory round-trip anywhere.
///
/// `BLEND_ADD` picks the SSE4.1 `pblendvb` offset select; the SSSE3 tier adds
/// the `-64` marker onto `-42` instead. Both need SSSE3 (`palignr`) for the
/// lane-to-lane escape carry, so this is never instantiated for plain SSE2.
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

/// Line-aware 64-byte-block driver for the pre-AVX2 tiers in the portable
/// binary: consult the caller's line-length hint, try the fast whole-line
/// path first, and fall back to the generic 64-byte block decode. Mirrors the
/// AVX-512/VBMI2 line-aware kernel structure with SSSE3-width vectors.
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

/// Whole-line fast path for the SSSE3 tier: decode one complete yEnc line
/// (hint-length plus CRLF) in a single pass when the window holds it, bailing
/// to the block path on escapes at boundaries, stuffed dots, or short input.
/// Same guards and bail conditions as `try_decode_avx512_vbmi2_line`, with
/// 4x16-byte vectors instead of one 512-bit vector.
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
    table: &[[u8; 16]; 16],
) -> std::arch::x86_64::__m128i {
    use std::arch::x86_64::*;

    // `table` is hoisted by the caller (the `OnceLock` acquire load must not sit
    // inside the per-window compaction).
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
unsafe fn sse_raw_body<const FAST_MATCH: bool, const BLEND_ADD: bool, const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    use std::arch::x86_64::*;
    const WIDTH: usize = 32;

    let mut src = 0usize;
    let mut dst = 0usize;
    // Oracle `lenBuffer` for `isRaw && searchEnd` is `width-1 + 3 + 1`
    // (decoder_common.h:44-46) == this 35, and the widest lookahead here is the
    // lane-B `+4` view ending at `src + WIDTH + 3`; the loop bound
    // (`src + WIDTH <= len - tail`) leaves a further WIDTH bytes of slack.
    let tail = WIDTH - 1 + 4; // 35
    let simd_limit = input.len().saturating_sub(tail);

    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());
    let sub106 = _mm_set1_epi8(106i8.wrapping_neg());
    let neg64 = _mm_set1_epi8(-64);
    let dot = _mm_set1_epi8(b'.' as i8);
    let eq_needle = _mm_set1_epi8(b'=' as i8);
    let cr = _mm_set1_epi8(b'\r' as i8);
    let lf = _mm_set1_epi8(b'\n' as i8);
    let y_needle = _mm_set1_epi8(b'y' as i8);
    let eq_y = _mm_set1_epi16(0x793d); // "=y", u16-aligned
    // Compaction LUTs, hoisted out of the window loop (both live behind a
    // `OnceLock`; the acquire load must not run per window).
    let table = compact_table_16();
    let unshuf = sse2_unshuf_table();
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

    // Set when the SEARCH_END probe aborts a window (oracle `len += i; break;`):
    // the window is left unconsumed and the exit state comes from the
    // no-backtrack rule instead of the trailing-bytes lookback.
    let mut broke = false;

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
                    // `=` at lane+2 (oracle decoder_sse_base.h:224-232). SSSE3+
                    // takes lane A's view from the two `=` compares via palignr
                    // instead of a second unaligned load.
                    let match2_eq_a = if SEARCH_END {
                        if FAST_MATCH {
                            _mm_alignr_epi8::<2>(cmp_eq_b, cmp_eq_a)
                        } else {
                            _mm_cmpeq_epi8(eq_needle, tmp2a)
                        }
                    } else {
                        _mm_setzero_si128()
                    };
                    let match2_eq_b = if SEARCH_END {
                        _mm_cmpeq_epi8(eq_needle, tmp2b)
                    } else {
                        _mm_setzero_si128()
                    };
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

                        // Terminator probe with a stuffed dot in the window
                        // (oracle decoder_sse_base.h:285-372): `\r\n.\r\n`,
                        // `\r\n.=y` and `\r\n=y`. Runs BEFORE the `mask` merge,
                        // so an aborted window reports the pre-merge mask to
                        // the no-backtrack exit rule, exactly like the oracle.
                        if SEARCH_END {
                            let tmp3a =
                                _mm_loadu_si128(input.as_ptr().add(src + 3) as *const __m128i);
                            let tmp3b =
                                _mm_loadu_si128(input.as_ptr().add(src + 19) as *const __m128i);
                            let tmp4a =
                                _mm_loadu_si128(input.as_ptr().add(src + 4) as *const __m128i);
                            let tmp4b =
                                _mm_loadu_si128(input.as_ptr().add(src + 20) as *const __m128i);

                            let m3cr_a = _mm_cmpeq_epi8(cr, tmp3a);
                            let m3cr_b = _mm_cmpeq_epi8(cr, tmp3b);
                            let m4lf_a = _mm_cmpeq_epi8(tmp4a, lf);
                            let m4lf_b = _mm_cmpeq_epi8(tmp4b, lf);
                            // `=y` at lane+3 for ODD lanes: the u16-aligned pair
                            // (lane+3, lane+4) of the `+4` view, kept in the
                            // high byte of its u16 by the `slli` (oracle :354).
                            let m4eqy_a = _mm_slli_epi16::<8>(_mm_cmpeq_epi16(tmp4a, eq_y));
                            let m4eqy_b = _mm_slli_epi16::<8>(_mm_cmpeq_epi16(tmp4b, eq_y));
                            // `=y` at lane+2.
                            let m3eqy_a =
                                _mm_and_si128(match2_eq_a, _mm_cmpeq_epi8(y_needle, tmp3a));
                            let m3eqy_b =
                                _mm_and_si128(match2_eq_b, _mm_cmpeq_epi8(y_needle, tmp3b));
                            // `srli_epi16(m3eqy, 8)` moves each odd lane's
                            // "`=y` at lane+2" down to the even lane below it,
                            // where it reads "`=y` at lane+3" — the even-lane
                            // half of the same predicate (oracle :359-360).
                            let m4end_a = _mm_and_si128(
                                _mm_or_si128(
                                    _mm_and_si128(m3cr_a, m4lf_a),
                                    _mm_or_si128(m4eqy_a, _mm_srli_epi16::<8>(m3eqy_a)),
                                ),
                                m2nldot_a,
                            );
                            let m4end_b = _mm_and_si128(
                                _mm_or_si128(
                                    _mm_and_si128(m3cr_b, m4lf_b),
                                    _mm_or_si128(m4eqy_b, _mm_srli_epi16::<8>(m3eqy_b)),
                                ),
                                m2nldot_b,
                            );
                            // `\r\n=y`.
                            let m3end_a = _mm_and_si128(m3eqy_a, m1nl_a);
                            let m3end_b = _mm_and_si128(m3eqy_b, m1nl_b);
                            let any_end = _mm_movemask_epi8(_mm_or_si128(
                                _mm_or_si128(m4end_a, m3end_a),
                                _mm_or_si128(m4end_b, m3end_b),
                            ));
                            if any_end != 0 {
                                state.state = x86_break_state(input, src, mask as u64, esc_first);
                                broke = true;
                                break;
                            }
                        }

                        mask |= (_mm_movemask_epi8(m2nldot_a) as u32) << 2;
                        mask |= (_mm_movemask_epi8(m2nldot_b) as u32) << 18; // u32 drops bits >=32
                        m2nldot_b = _mm_srli_si128::<14>(m2nldot_b);
                        if FAST_MATCH {
                            min_mask = _mm_subs_epu8(dot, m2nldot_b);
                        } else {
                            // '.' | '\n' == '.' folds the carry into lf_compare.
                            lf_compare = _mm_or_si128(_mm_and_si128(m2nldot_b, dot), lf);
                        }
                    } else {
                        // Terminator probe without a stuffed dot in the window
                        // (oracle decoder_sse_base.h:398-489): only `\r\n=y` is
                        // reachable — any `\r\n.` shape would have set `partial`.
                        if SEARCH_END {
                            let tmp3a =
                                _mm_loadu_si128(input.as_ptr().add(src + 3) as *const __m128i);
                            let tmp3b =
                                _mm_loadu_si128(input.as_ptr().add(src + 19) as *const __m128i);
                            let m3eqy_a =
                                _mm_and_si128(match2_eq_a, _mm_cmpeq_epi8(y_needle, tmp3a));
                            let m3eqy_b =
                                _mm_and_si128(match2_eq_b, _mm_cmpeq_epi8(y_needle, tmp3b));
                            if _mm_movemask_epi8(_mm_or_si128(m3eqy_a, m3eqy_b)) != 0 {
                                let cr_a = _mm_cmpeq_epi8(o_data_a, cr);
                                let cr_b = _mm_cmpeq_epi8(o_data_b, cr);
                                let m1lf_a = _mm_cmpeq_epi8(
                                    lf,
                                    _mm_loadu_si128(input.as_ptr().add(src + 1) as *const __m128i),
                                );
                                let m1lf_b = _mm_cmpeq_epi8(
                                    lf,
                                    _mm_loadu_si128(input.as_ptr().add(src + 17) as *const __m128i),
                                );
                                let end_found = _mm_movemask_epi8(_mm_or_si128(
                                    _mm_and_si128(m3eqy_a, _mm_and_si128(m1lf_a, cr_a)),
                                    _mm_and_si128(m3eqy_b, _mm_and_si128(m1lf_b, cr_b)),
                                ));
                                if end_found != 0 {
                                    state.state =
                                        x86_break_state(input, src, mask as u64, esc_first);
                                    broke = true;
                                    break;
                                }
                            }
                        }
                        // `\r\n` present but no stuffed dot: reset the carry.
                        if FAST_MATCH {
                            min_mask = dot;
                        } else {
                            lf_compare = lf;
                        }
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
                    compact_store_16_ssse3(data_a, (skip & 0xffff) as u16, table, output, &mut dst);
                    compact_store_16_ssse3(
                        data_b,
                        ((skip >> 16) & 0xffff) as u16,
                        table,
                        output,
                        &mut dst,
                    );
                } else {
                    let packed_a = sse_compact_vect(skip & 0xffff, data_a, unshuf);
                    _mm_storeu_si128(output.as_mut_ptr().add(dst) as *mut __m128i, packed_a);
                    dst += 16 - (skip & 0xffff).count_ones() as usize;
                    let packed_b = sse_compact_vect(skip >> 16, data_b, unshuf);
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
    // A SEARCH_END break already set the state from the no-backtrack rule over
    // the unconsumed window, so the (backtracking) lookback must not run.
    if !broke && src > 0 {
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
unsafe fn decode_kernel_sse2_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<false, false, SEARCH_END>(input, output, state, mode) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn decode_kernel_ssse3_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<true, false, SEARCH_END>(input, output, state, mode) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1,ssse3")]
unsafe fn decode_kernel_sse41_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    unsafe { sse_raw_body::<true, true, SEARCH_END>(input, output, state, mode) }
}
