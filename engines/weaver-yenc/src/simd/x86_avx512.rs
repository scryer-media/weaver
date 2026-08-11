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
        && input.len() > WIDTH * 2
        && x86_search_end_head(input, output, state, mode, &mut head_src, &mut head_dst)?
    {
        return Ok(KernelOutcome {
            consumed: head_src,
            written: head_dst,
            end: state.end.into(),
        });
    }

    // Hot path: faithful 512-bit port of rapidyenc `do_decode_avx2<…, VBMI2>`
    // (raw dot-unstuffing), both `searchEnd` instantiations. Applies the AVX2
    // flat-loop port's register-carried state model at full 512-bit width.
    // Other combos (non-raw, or an entry state the head resolution doesn't
    // cover) keep the general line-aware kernel below.
    if dot_unstuffing
        && input.len() - head_src > WIDTH * 2
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        // `head_src`/`head_dst` are 0 unless the head loop ran, so the
        // `::<false>` instantiation always sees the untouched full buffers.
        let outcome = if search_end {
            unsafe {
                decode_kernel_avx512_raw::<true>(
                    &input[head_src..],
                    &mut output[head_dst..],
                    state,
                    mode,
                )
            }
        } else {
            unsafe { decode_kernel_avx512_raw::<false>(input, output, state, mode) }
        };
        return x86_fold_head(outcome, head_src, head_dst);
    }

    let outcome = unsafe {
        decode_kernel_simd64_vbmi2_line_aware(
            &input[head_src..],
            &mut output[head_dst..],
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
        )
    };
    x86_fold_head(outcome, head_src, head_dst)
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
unsafe fn decode_kernel_avx512_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    use std::arch::x86_64::*;
    const WIDTH: usize = 64;

    let mut src = 0usize;
    let mut dst = 0usize;
    // Oracle `lenBuffer` for `isRaw && searchEnd` is `width-1 + 3 + 1`
    // (decoder_common.h:44-46) == this 67; the widest lookahead is the `+4`
    // view, ending at `src + WIDTH + 3`, and the loop bound
    // (`src + WIDTH <= len - tail`) leaves a further WIDTH bytes of slack.
    let tail = WIDTH - 1 + 4;
    let simd_limit = input.len().saturating_sub(tail);

    let sub42 = _mm512_set1_epi8(42i8.wrapping_neg());
    let sub64 = _mm512_set1_epi8(64i8.wrapping_neg());
    let dot = _mm512_set1_epi8(b'.' as i8);
    let eq_needle = _mm512_set1_epi8(b'=' as i8);
    let cr = _mm512_set1_epi8(b'\r' as i8);
    let lf = _mm512_set1_epi8(b'\n' as i8);
    let y_needle = _mm512_set1_epi8(b'y' as i8);
    // The oracle's 16-byte specials LUT (`. \n \r =` → self, else -1) replicated
    // across all four 128-bit lanes (`_mm512_shuffle_epi8` is per-lane).
    let special_lut = _mm512_set_epi8(
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

    // Set when the SEARCH_END probe aborts a window (oracle `len += i; break;`):
    // the window is left unconsumed and the exit state comes from the
    // no-backtrack rule instead of the trailing-bytes lookback.
    let mut broke = false;

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
                    // `=` at lane+2 (oracle `match2EqMask`, the AVX3 mask arm of
                    // decoder_avx2_base.h:148-152 at 512-bit width).
                    let match2_eq: u64 = if SEARCH_END {
                        _mm512_cmpeq_epi8_mask(eq_needle, tmp2)
                    } else {
                        0
                    };
                    let m2cr_mask: u64 = _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp2, dot);
                    if m2cr_mask != 0 {
                        let tmp1 = _mm512_loadu_si512(input.as_ptr().add(src + 1) as *const _);
                        let m1nl_mask: u64 = _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp1, lf);
                        let m2nldot_mask = m2cr_mask & m1nl_mask;

                        // Terminator probe with a stuffed dot in the window
                        // (oracle decoder_avx2_base.h:222-327): `\r\n.\r\n`,
                        // `\r\n.=y` and `\r\n=y`, in k-mask form. Runs BEFORE the
                        // `mask` merge, so an aborted window reports the pre-merge
                        // mask to the no-backtrack exit rule.
                        if SEARCH_END {
                            let tmp3 = _mm512_loadu_si512(input.as_ptr().add(src + 3) as *const _);
                            let tmp4 = _mm512_loadu_si512(input.as_ptr().add(src + 4) as *const _);
                            // "`=y` at lane+2" (oracle match3EqY).
                            let m3eqy: u64 = _mm512_mask_cmpeq_epi8_mask(match2_eq, tmp3, y_needle);
                            // "`=y` at lane+3" (oracle match34EqY). Bit-position
                            // proof: the oracle builds this per parity because a
                            // vector register cannot address bytes — odd lanes
                            // from `cmpeq_epi16(tmpData4, "=y") << 8` (u16 lane j
                            // covers bytes (4+2j, 5+2j) of the window, and the
                            // `slli` parks the hit at byte 2j+1, i.e. lane
                            // k = 2j+1 whose k+3 = 4+2j is even), even lanes from
                            // `match3EqY >> 8` (u16-lane byte 2j+1 -> 2j, i.e.
                            // lane k = 2j takes match3EqY[k+1], whose "=y at
                            // (k+1)+2" IS "=y at k+3"). Both halves therefore
                            // state exactly `data[k+3]=='=' && data[k+4]=='y'`, so
                            // at bit granularity the whole parity dance collapses
                            // to `m3eqy >> 1` — with bit 63 (which would need
                            // match3EqY[64], outside the window) supplied by the
                            // oracle's u16 lane 31 of the `+4` view, i.e. by bytes
                            // 66/67. Comparing the `+3`/`+4` views directly gives
                            // every bit including 63 from those same bytes.
                            let m34eqy: u64 = _mm512_cmpeq_epi8_mask(tmp3, eq_needle)
                                & _mm512_cmpeq_epi8_mask(tmp4, y_needle);
                            debug_assert_eq!(m34eqy & !(1u64 << 63), m3eqy >> 1);
                            let m4nl: u64 =
                                _mm512_cmpeq_epi8_mask(tmp3, cr) & _mm512_cmpeq_epi8_mask(tmp4, lf);
                            // `\r\n.` + (`\r\n` | `=y`) at lane+3, and `\r\n=y`.
                            let m4end = (m4nl | m34eqy) & m2nldot_mask;
                            let m3end = m3eqy & m1nl_mask;
                            if (m4end | m3end) != 0 {
                                state.state = x86_break_state(input, src, mask, esc_first);
                                broke = true;
                                break;
                            }
                        }

                        mask |= m2nldot_mask << 2;
                        // carry a straddling \r\n. (CR at byte 62/63, dot in the
                        // next window) into the next window's min_mask.
                        min_mask = _mm512_maskz_mov_epi8(!(m2nldot_mask >> 62), dot);
                    } else {
                        // Terminator probe without a stuffed dot in the window
                        // (oracle decoder_avx2_base.h:344-421): only `\r\n=y` is
                        // reachable — any `\r\n.` shape would have set m2cr_mask.
                        if SEARCH_END {
                            let tmp3 = _mm512_loadu_si512(input.as_ptr().add(src + 3) as *const _);
                            let m3eqy: u64 = _mm512_mask_cmpeq_epi8_mask(match2_eq, tmp3, y_needle);
                            if m3eqy != 0 {
                                let tmp1 =
                                    _mm512_loadu_si512(input.as_ptr().add(src + 1) as *const _);
                                let m1nl_mask: u64 = _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp1, lf);
                                if (m3eqy & m1nl_mask) != 0 {
                                    state.state = x86_break_state(input, src, mask, esc_first);
                                    broke = true;
                                    break;
                                }
                            }
                        }
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

    // Only re-derive the carried state when the SIMD loop actually consumed at
    // least one window. With no window consumed (len in {129,130} => simd_limit
    // < WIDTH), `src` is still 0 and the entry state MUST survive untouched for
    // the scalar epilogue — otherwise a carried Cr/CrLf line-start (with a
    // pending stuffed dot) would be clobbered to None and mis-decoded.
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

/// Line-aware 64-byte-block driver for the AVX-512/VBMI2 tier: consult the
/// caller's line-length hint, try the whole-line fast path first, and fall
/// back to the generic 64-byte block decode. Mirrors the SSSE3 line-aware
/// kernel structure (`decode_kernel_simd64_ssse3_line_aware`) at 512-bit
/// width.
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

/// Whole-line fast path for the AVX-512/VBMI2 tier: decode one complete yEnc
/// line (hint-length plus CRLF) in a single pass when the window holds it,
/// bailing to the block path on escapes at boundaries, stuffed dots, or short
/// input. Same guards and bail conditions as `try_decode_ssse3_line`, with
/// one 512-bit vector per 64-byte chunk, k-register masks, and full-width
/// vpcompressb compaction.
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
