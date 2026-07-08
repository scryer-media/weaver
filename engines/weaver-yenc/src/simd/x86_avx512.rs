use super::*;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2")]
pub(super) unsafe fn decode_kernel_avx512_vbmi2(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    const WIDTH: usize = 64;

    // Hot path: faithful 512-bit port of rapidyenc `do_decode_avx2<…, VBMI2>`
    // (raw dot-unstuffing, no end-search). Applies the AVX2 flat-loop port's
    // register-carried state model at full 512-bit width. Other combos
    // (search_end, non-raw, or an entry state the head-switch doesn't cover)
    // keep the general line-aware kernel below.
    if dot_unstuffing
        && !search_end
        && input.len() > WIDTH * 2
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
        return unsafe { decode_kernel_avx512_raw(input, output, state, mode) };
    }

    unsafe {
        decode_kernel_simd64_vbmi2_line_aware(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
        )
    }
}

/// Faithful 512-bit port of rapidyenc `do_decode_avx2` instantiated at
/// `ISA_LEVEL_VBMI2` (`decoder_vbmi2.cc` → `decoder_avx2_base.h`), the
/// `isRaw=true, searchEnd=false` path. This is the AVX2 flat-loop port
/// [`decode_kernel_avx2_raw`](super::x86_avx2) widened to a single 512-bit
/// window: the two 256-bit lanes collapse to one `__m512i`, `movemask`+combine
/// collapses to a `_mm512_cmpeq_epi8_mask` k-register `u64`, the 2-lane LUT
/// compaction becomes one `_mm512_maskz_compress_epi8`, and escape unescape is
/// a single `_mm512_mask_add_epi8`. The scalar `u64` bit-math (`fix_eq_mask`,
/// `escaped`, `esc_first`, `skip`, entry/exit state) is byte-identical to the
/// AVX2 port, so both tiers share the same correctness envelope.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn decode_kernel_avx512_raw(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    use std::arch::x86_64::*;
    const WIDTH: usize = 64;

    let mut src = 0usize;
    let mut dst = 0usize;
    let tail = WIDTH - 1 + 4;
    let simd_limit = input.len().saturating_sub(tail);

    let sub42 = _mm512_set1_epi8(42i8.wrapping_neg());
    let sub64 = _mm512_set1_epi8(64i8.wrapping_neg());
    let dot = _mm512_set1_epi8(b'.' as i8);
    let eq_needle = _mm512_set1_epi8(b'=' as i8);
    let cr = _mm512_set1_epi8(b'\r' as i8);
    let lf = _mm512_set1_epi8(b'\n' as i8);
    // The oracle's 16-byte specials LUT (`. \n \r =` → self, else -1) replicated
    // across all four 128-bit lanes (`_mm512_shuffle_epi8` is per-lane).
    let special_lut = _mm512_set_epi8(
        -1, b'=' as i8, b'\r' as i8, -1, -1, b'\n' as i8, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        b'.' as i8, -1, b'=' as i8, b'\r' as i8, -1, -1, b'\n' as i8, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, b'.' as i8, -1, b'=' as i8, b'\r' as i8, -1, -1, b'\n' as i8, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, b'.' as i8, -1, b'=' as i8, b'\r' as i8, -1, -1, b'\n' as i8, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, b'.' as i8,
    );

    // entry state → escFirst / nextMask (oracle `_do_decode_simd` switch subset).
    let mut esc_first: u64 = (state.state == DecoderState::Eq) as u64;
    let entry_next_mask: u16 = match state.state {
        DecoderState::CrLf if input[0] == b'.' => 1,
        DecoderState::Cr if input.len() >= 2 && input[0] == b'\n' && input[1] == b'.' => 2,
        _ => 0,
    };

    // byte 0 of yenc_offset carries a pending escape (-106 = -42-64); rebuilt per
    // window via `mask_add`, exactly the oracle's `yencOffset`.
    let mut yenc_offset = _mm512_mask_add_epi8(sub42, esc_first, sub42, sub64);
    // min_mask forces a stuffed dot at a carried line start to flag as special
    // (oracle `minMask`): byte 0/1 zeroed so `min_epu8` maps the dot onto a LUT
    // hit. entry_next_mask 1 ⇒ byte 0, 2 ⇒ byte 1.
    let entry_zero: u64 = match entry_next_mask {
        1 => 1,
        2 => 2,
        _ => 0,
    };
    let mut min_mask = _mm512_maskz_mov_epi8(!entry_zero, dot);

    if input.len() > WIDTH * 2 {
        while src + WIDTH <= simd_limit {
            let v = _mm512_loadu_si512(input.as_ptr().add(src) as *const _);

            let mut mask: u64 = _mm512_cmpeq_epi8_mask(
                v,
                _mm512_shuffle_epi8(special_lut, _mm512_min_epu8(v, min_mask)),
            );

            if mask != 0 {
                let mask_eq: u64 = _mm512_cmpeq_epi8_mask(v, eq_needle);

                if mask != mask_eq {
                    // \r\n. dot-stuffing detection (oracle match2CrXDt / m2nldot).
                    let cr_mask: u64 = _mm512_cmpeq_epi8_mask(v, cr);
                    let tmp2 = _mm512_loadu_si512(input.as_ptr().add(src + 2) as *const _);
                    let m2cr_mask: u64 = _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp2, dot);
                    if m2cr_mask != 0 {
                        let tmp1 = _mm512_loadu_si512(input.as_ptr().add(src + 1) as *const _);
                        let m1nl_mask: u64 = _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp1, lf);
                        let m2nldot_mask = m2cr_mask & m1nl_mask;
                        mask |= m2nldot_mask << 2;
                        // carry a straddling \r\n. (CR at byte 62/63, dot in the
                        // next window) into the next window's min_mask.
                        min_mask = _mm512_maskz_mov_epi8(!(m2nldot_mask >> 62), dot);
                    } else {
                        min_mask = dot;
                    }
                } else {
                    min_mask = dot;
                }

                let esc_first_in = esc_first;
                let eq_shift1 = (mask_eq << 1) | esc_first_in;
                let collision = (mask_eq & eq_shift1) != 0;
                let fixed_eq = if collision {
                    fix_eq_mask(mask_eq, eq_shift1)
                } else {
                    mask_eq
                };
                let escaped = (fixed_eq << 1) | esc_first_in;
                esc_first = fixed_eq >> 63;

                // decode: add the carried offset, then -64 on every escaped byte
                // in 1..63 (byte 0's -64 already rode in via yenc_offset).
                let data = _mm512_add_epi8(v, yenc_offset);
                let decoded = _mm512_mask_add_epi8(data, fixed_eq << 1, data, sub64);

                let skip = mask & !escaped;
                yenc_offset = _mm512_mask_add_epi8(sub42, esc_first, sub42, sub64);

                if skip == 0 {
                    _mm512_storeu_si512(output.as_mut_ptr().add(dst) as *mut _, decoded);
                    dst += WIDTH;
                } else {
                    // The entry gate + tail guarantee ≥64 spare output bytes, so
                    // the full-width compressed store is always in bounds; bytes
                    // past `keep` are overwritten by the next store.
                    let keep = !skip;
                    _mm512_storeu_si512(
                        output.as_mut_ptr().add(dst) as *mut _,
                        _mm512_maskz_compress_epi8(keep, decoded),
                    );
                    dst += keep.count_ones() as usize;
                }
            } else {
                _mm512_storeu_si512(
                    output.as_mut_ptr().add(dst) as *mut _,
                    _mm512_add_epi8(v, yenc_offset),
                );
                dst += WIDTH;
                esc_first = 0;
                yenc_offset = sub42;
            }
            src += WIDTH;
        }
    }

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
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2")]
#[inline]
pub(super) unsafe fn try_decode_avx512_vbmi2_block(
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

    // Full-width 512-bit window: compares land directly in k-registers as the
    // u64 bit masks the scalar logic wants (no movemask/combine), the escape
    // offsets are one masked blend, and compaction is a single vpcompressb.
    let v = unsafe { _mm512_loadu_si512(input.as_ptr().add(src) as *const _) };
    let Some((esc_first, dot0)) =
        x86_block_entry_flags(input, src, state.state, dot_unstuffing, search_end)
    else {
        return Ok(None);
    };

    let eq = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'=' as i8));
    let cr = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\r' as i8));
    let lf = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8));
    let specials = eq | cr | lf;

    let sub42 = _mm512_set1_epi8(42i8.wrapping_neg());
    if specials == 0 && !dot0 && !esc_first {
        unsafe {
            _mm512_storeu_si512(
                output.as_mut_ptr().add(*dst) as *mut _,
                _mm512_add_epi8(v, sub42),
            );
        }
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(Some(64));
    }

    let esc_first = esc_first as u64;
    let fixed_eq = fix_eq_mask(eq, (eq << 1) | esc_first);
    let escaped = (fixed_eq << 1) | esc_first;
    let entry_line_start = (state.state == DecoderState::CrLf) as u64;

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

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || (line_start_eq & !(1u64 << 63)) != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    let sub106 = _mm512_set1_epi8(106i8.wrapping_neg());
    let decoded = _mm512_add_epi8(v, _mm512_mask_mov_epi8(sub42, escaped, sub106));

    if skip == 0 {
        unsafe { _mm512_storeu_si512(output.as_mut_ptr().add(*dst) as *mut _, decoded) };
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(Some(64));
    }

    // The entry check guarantees 64 spare output bytes, so the full-width
    // compressed store is always in bounds; bytes past `keep` are overwritten
    // by the next store.
    let keep = !skip;
    unsafe {
        _mm512_storeu_si512(
            output.as_mut_ptr().add(*dst) as *mut _,
            _mm512_maskz_compress_epi8(keep, decoded),
        );
    }
    *dst += keep.count_ones() as usize;

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

/// AVX-512/VBMI2 twin of [`decode_kernel_simd64_avx2_line_aware`].
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2")]
pub(super) unsafe fn decode_kernel_simd64_vbmi2_line_aware(
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
                    try_decode_avx512_vbmi2_line(
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
                try_decode_avx512_vbmi2_block(
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

/// AVX-512/VBMI2 twin of [`try_decode_avx2_line`]: same guards and bail
/// conditions, one 512-bit vector per 64-byte chunk with k-register masks and
/// full-width vpcompressb compaction.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2")]
#[allow(clippy::too_many_arguments)]
pub(super) unsafe fn try_decode_avx512_vbmi2_line(
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

    // Single pass; the '=' at line_end-1 guard above already excludes a
    // dangling escape at line end, and a raw CR/LF mid-line rewinds the
    // output cursor and hands the line back to the general path.
    let chunks = line_length / WIDTH;
    let sub42 = _mm512_set1_epi8(42i8.wrapping_neg());
    let sub106 = _mm512_set1_epi8(106i8.wrapping_neg());
    let dst_start = *dst;
    let mut esc_first = 0u64;
    for chunk_idx in 0..chunks {
        let v =
            unsafe { _mm512_loadu_si512(input.as_ptr().add(src + chunk_idx * WIDTH) as *const _) };
        let crlf = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\r' as i8))
            | _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8));
        if crlf != 0 {
            *dst = dst_start;
            return Ok(None);
        }
        let eq = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'=' as i8));
        let fixed_eq = fix_eq_mask(eq, (eq << 1) | esc_first);
        let escaped = (fixed_eq << 1) | esc_first;
        let skip = fixed_eq;

        if skip == 0 && escaped == 0 {
            unsafe {
                _mm512_storeu_si512(
                    output.as_mut_ptr().add(*dst) as *mut _,
                    _mm512_add_epi8(v, sub42),
                );
            }
            *dst += WIDTH;
        } else {
            let decoded = _mm512_add_epi8(v, _mm512_mask_mov_epi8(sub42, escaped, sub106));
            let keep = !skip;
            unsafe {
                _mm512_storeu_si512(
                    output.as_mut_ptr().add(*dst) as *mut _,
                    _mm512_maskz_compress_epi8(keep, decoded),
                );
            }
            *dst += keep.count_ones() as usize;
        }

        esc_first = (fixed_eq & LAST != 0) as u64;
    }

    debug_assert_eq!(esc_first, 0);
    state.state = DecoderState::CrLf;
    Ok(Some(line_length + 2))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512f")]
pub(super) unsafe fn decode_normal_run_avx512(
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
