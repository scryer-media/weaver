use super::*;

/// Faithful port of rapidyenc `do_decode_avx2` (decoder_avx2_base.h), the
/// `isRaw=true, searchEnd=false` instantiation — the realshape decode path.
/// 1:1 translation of the oracle's HOT LOOP: decoder state lives entirely in
/// registers (`esc_first`/`yenc_offset`/`min_mask`/`next_mask`, exactly the
/// oracle's `escFirst`/`yencOffset`/`minMask`/`nextMask`); `\r\n.` dot-stuffing
/// is stripped IN-LOOP via `min_mask` + a `mask` merge (never a scalar bail);
/// no per-window enum dispatch, no `span_end_state` trailing-byte read. The
/// per-window decode math (escape unescape, 2-lane LUT compaction, `fix_eq_mask`)
/// reuses weaver's existing byte-exact helpers (already identical to the oracle).
/// This removes the ~47 µops/window of weaver-specific scaffolding.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn decode_kernel_avx2_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    use std::arch::x86_64::*;
    const WIDTH: usize = 64;

    let mut src = 0usize;
    // Output cursor is a running pointer, exactly the oracle's `p`. Keeping a
    // `(base, offset)` pair instead cost a reload of the spilled base on every
    // window plus a separate offset register; `dst` is materialised once, after
    // the SIMD span, for the scalar epilogue.
    let out_base = output.as_mut_ptr();
    let mut out = out_base;
    // Oracle `lenBuffer` for `isRaw && searchEnd` is `width-1 + 3 + 1`
    // (decoder_common.h:44-46) == this 67; the widest lookahead is the lane-B
    // `+4` view, ending at `src + WIDTH + 3`, and the loop bound
    // (`src + WIDTH <= len - tail`) leaves a further WIDTH bytes of slack.
    let tail = WIDTH - 1 + 4;
    let simd_limit = input.len().saturating_sub(tail);

    let sub42 = _mm256_set1_epi8(42i8.wrapping_neg());
    let dot = _mm256_set1_epi8(b'.' as i8);
    let eq_needle = _mm256_set1_epi8(b'=' as i8);
    let cr = _mm256_set1_epi8(b'\r' as i8);
    let lf = _mm256_set1_epi8(b'\n' as i8);
    let y_needle = _mm256_set1_epi8(b'y' as i8);
    let eq_y = _mm256_set1_epi16(0x793d); // "=y", u16-aligned
    let esc_off = _mm256_set1_epi8(-106);
    let table = compact_table_16().as_ptr() as *const u8;
    let special_lut = _mm256_set_epi8(
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

    // entry state → escFirst / nextMask (oracle _do_decode_simd switch subset).
    let mut esc_first: u64 = (state.state == DecoderState::Eq) as u64;
    let entry_next_mask: u16 = match state.state {
        DecoderState::CrLf if input[0] == b'.' => 1,
        DecoderState::Cr if input.len() >= 2 && input[0] == b'\n' && input[1] == b'.' => 2,
        _ => 0,
    };

    let mut yenc_offset = if esc_first != 0 {
        _mm256_xor_si256(
            sub42,
            _mm256_inserti128_si256(_mm256_setzero_si256(), _mm_cvtsi32_si128(0x40), 0),
        )
    } else {
        sub42
    };
    let mut min_mask = if entry_next_mask != 0 {
        _mm256_set_epi8(
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            b'.' as i8,
            if entry_next_mask == 2 { 0 } else { b'.' as i8 },
            if entry_next_mask == 1 { 0 } else { b'.' as i8 },
        )
    } else {
        dot
    };

    // Set when the SEARCH_END probe aborts a window (oracle `len += i; break;`):
    // the window is left unconsumed and the exit state comes from the
    // no-backtrack rule instead of the trailing-bytes lookback.
    let mut broke = false;

    if input.len() > WIDTH * 2 {
        // Oracle loop shape (`for(i = -len; i; i += 64)`, decoder_avx2_base.h:84):
        // one negative induction variable counting up to zero, so the back edge
        // is `add`+`jne` with no separate bound compare and no second cursor.
        // The previous `while src + WIDTH <= simd_limit` form compiled to
        // lea/sub/cmp/mov/ja — three extra µops on every window. `src` is only
        // read at the SEARCH_END break and after the loop, so it is derived from
        // `i` there instead of maintained per window.
        //
        // `span` is the number of bytes the old bound admitted: iteration `k`
        // ran when `64k + 64 <= simd_limit`, i.e. `k < simd_limit / 64`.
        let span = (simd_limit / WIDTH) * WIDTH;
        let sp = input.as_ptr().add(span);
        let mut i: isize = -(span as isize);
        // Rung 3: the SEARCH_END=false span runs in the hand-written `asm!`
        // loop — register allocation by construction, immune to the
        // rustc-version allocation luck that made every intrinsic-level spill
        // fix regress something else (see avx2_raw_span_loop_asm). It consumes
        // the whole span (exits with i == 0), so the intrinsic loop below is
        // skipped naturally and remains the SEARCH_END=true / non-asm path.
        #[cfg(weaver_yenc_raw_asm)]
        if !SEARCH_END && i != 0 {
            avx2_raw_span_loop_asm(sp, &mut i, &mut out, &mut esc_first, min_mask, yenc_offset);
            debug_assert_eq!(i, 0);
        }
        while i != 0 {
            let a = _mm256_loadu_si256(sp.offset(i) as *const __m256i);
            let b = _mm256_loadu_si256(sp.offset(i + 32) as *const __m256i);

            let cmp_a = _mm256_cmpeq_epi8(
                a,
                _mm256_shuffle_epi8(special_lut, _mm256_min_epu8(a, min_mask)),
            );
            let cmp_b =
                _mm256_cmpeq_epi8(b, _mm256_shuffle_epi8(special_lut, _mm256_min_epu8(b, dot)));
            let mut mask: u64 = ((_mm256_movemask_epi8(cmp_b) as u32 as u64) << 32)
                | (_mm256_movemask_epi8(cmp_a) as u32 as u64);

            if mask != 0 {
                let eq_va = _mm256_cmpeq_epi8(a, eq_needle);
                let eq_vb = _mm256_cmpeq_epi8(b, eq_needle);
                let mask_eq: u64 = ((_mm256_movemask_epi8(eq_vb) as u32 as u64) << 32)
                    | (_mm256_movemask_epi8(eq_va) as u32 as u64);

                if mask != mask_eq {
                    let tmp2a = _mm256_loadu_si256(sp.offset(i + 2) as *const __m256i);
                    let tmp2b = _mm256_loadu_si256(sp.offset(i + 34) as *const __m256i);
                    // `=` at lane+2 (oracle decoder_avx2_base.h:153-156). The
                    // oracle's alignr alternative for this view is its `#if 0`
                    // experiment; the plain loadu view is the live arm.
                    let (match2_eq_a, match2_eq_b) = if SEARCH_END {
                        (
                            _mm256_cmpeq_epi8(eq_needle, tmp2a),
                            _mm256_cmpeq_epi8(eq_needle, tmp2b),
                        )
                    } else {
                        (_mm256_setzero_si256(), _mm256_setzero_si256())
                    };
                    let m2cr_a =
                        _mm256_and_si256(_mm256_cmpeq_epi8(a, cr), _mm256_cmpeq_epi8(tmp2a, dot));
                    let m2cr_b =
                        _mm256_and_si256(_mm256_cmpeq_epi8(b, cr), _mm256_cmpeq_epi8(tmp2b, dot));
                    let partial = _mm256_movemask_epi8(_mm256_or_si256(m2cr_a, m2cr_b));
                    if partial != 0 {
                        let m1lf_a = _mm256_cmpeq_epi8(
                            lf,
                            _mm256_loadu_si256(sp.offset(i + 1) as *const __m256i),
                        );
                        let m1lf_b = _mm256_cmpeq_epi8(
                            lf,
                            _mm256_loadu_si256(sp.offset(i + 33) as *const __m256i),
                        );
                        let m1nl_a = _mm256_and_si256(m1lf_a, _mm256_cmpeq_epi8(a, cr));
                        let m1nl_b = _mm256_and_si256(m1lf_b, _mm256_cmpeq_epi8(b, cr));
                        let m2nldot_a = _mm256_and_si256(m2cr_a, m1nl_a);
                        let m2nldot_b = _mm256_and_si256(m2cr_b, m1nl_b);

                        // Terminator probe with a stuffed dot in the window
                        // (oracle decoder_avx2_base.h:222-327, the non-AVX512
                        // arm): `\r\n.\r\n`, `\r\n.=y` and `\r\n=y`. Runs BEFORE
                        // the `mask` merge, so an aborted window reports the
                        // pre-merge mask to the no-backtrack exit rule.
                        if SEARCH_END {
                            let tmp3a = _mm256_loadu_si256(sp.offset(i + 3) as *const __m256i);
                            let tmp3b = _mm256_loadu_si256(sp.offset(i + 35) as *const __m256i);
                            let tmp4a = _mm256_loadu_si256(sp.offset(i + 4) as *const __m256i);
                            let tmp4b = _mm256_loadu_si256(sp.offset(i + 36) as *const __m256i);

                            let m3cr_a = _mm256_cmpeq_epi8(cr, tmp3a);
                            let m3cr_b = _mm256_cmpeq_epi8(cr, tmp3b);
                            let m4lf_a = _mm256_cmpeq_epi8(tmp4a, lf);
                            let m4lf_b = _mm256_cmpeq_epi8(tmp4b, lf);
                            // `=y` at lane+3 for ODD lanes: the u16-aligned pair
                            // (lane+3, lane+4) of the `+4` view, kept in the high
                            // byte of its u16 by the `slli` (oracle :294-295).
                            let m4eqy_a = _mm256_slli_epi16::<8>(_mm256_cmpeq_epi16(tmp4a, eq_y));
                            let m4eqy_b = _mm256_slli_epi16::<8>(_mm256_cmpeq_epi16(tmp4b, eq_y));
                            // `=y` at lane+2.
                            let m3eqy_a =
                                _mm256_and_si256(match2_eq_a, _mm256_cmpeq_epi8(y_needle, tmp3a));
                            let m3eqy_b =
                                _mm256_and_si256(match2_eq_b, _mm256_cmpeq_epi8(y_needle, tmp3b));
                            // `srli_epi16(m3eqy, 8)` moves each odd lane's "`=y`
                            // at lane+2" down to the even lane below it, where it
                            // reads "`=y` at lane+3" — the even-lane half of the
                            // same predicate (oracle :299-306).
                            let m4end_a = _mm256_and_si256(
                                _mm256_or_si256(
                                    _mm256_and_si256(m3cr_a, m4lf_a),
                                    _mm256_or_si256(m4eqy_a, _mm256_srli_epi16::<8>(m3eqy_a)),
                                ),
                                m2nldot_a,
                            );
                            let m4end_b = _mm256_and_si256(
                                _mm256_or_si256(
                                    _mm256_and_si256(m3cr_b, m4lf_b),
                                    _mm256_or_si256(m4eqy_b, _mm256_srli_epi16::<8>(m3eqy_b)),
                                ),
                                m2nldot_b,
                            );
                            // `\r\n=y`.
                            let m3end_a = _mm256_and_si256(m3eqy_a, m1nl_a);
                            let m3end_b = _mm256_and_si256(m3eqy_b, m1nl_b);
                            let any_end = _mm256_movemask_epi8(_mm256_or_si256(
                                _mm256_or_si256(m4end_a, m3end_a),
                                _mm256_or_si256(m4end_b, m3end_b),
                            ));
                            if any_end != 0 {
                                state.state = x86_break_state(
                                    input,
                                    (span as isize + i) as usize,
                                    mask,
                                    esc_first,
                                );
                                broke = true;
                                break;
                            }
                        }

                        mask |= (_mm256_movemask_epi8(m2nldot_a) as u32 as u64) << 2;
                        mask |= (_mm256_movemask_epi8(m2nldot_b) as u32 as u64) << 34;
                        let shifted = _mm256_zextsi128_si256(_mm_srli_si128::<14>(
                            _mm256_extracti128_si256::<1>(m2nldot_b),
                        ));
                        min_mask = _mm256_subs_epu8(dot, shifted);
                    } else {
                        // Terminator probe without a stuffed dot in the window
                        // (oracle decoder_avx2_base.h:344-421): only `\r\n=y` is
                        // reachable — any `\r\n.` shape would have set `partial`.
                        //
                        // DELIBERATE ASYMMETRY WITH NEON — MEASURED, DO NOT
                        // "FIX" BY SYMMETRY. `neon.rs` replaced this forward
                        // probe with a scalar mask-space candidate test plus a
                        // 1-bit pending carry re-tested at the next loop top
                        // (there the win came from deleting a whole next-window
                        // load + `vext` chain, which x86 never had: the `+2/+3`
                        // views here are plain overlapping loads). x86 keeps the
                        // oracle-parity forward probe.
                        //
                        // 2026-08-13 A/B, both x86 tiers ported and measured on
                        // two boxes (searchend_timing, min-of-2000, taskset-
                        // pinned, interleaved, 5 fixtures; codex i5-1240P avx2
                        // tier 10 rounds, Synology DS1819+ C3538 ssse3 tier 6
                        // rounds). Faithful port (mask-space test + carry),
                        // until_end lane, Δ vs this code:
                        //     realshape  +10.7% (ADL)   +6.3% (Denverton)
                        //     crlf_only   +8.7%         -1.0%
                        //     esc_only    +4.6%         +5.2%
                        // decode_only stayed flat, so the regression is entirely
                        // the searchEnd path. `esc_only` never even reaches this
                        // branch (`mask == mask_eq`), which localizes ~5% of the
                        // Denverton loss to the carry's unconditional loop-top
                        // re-test alone — the serial scalar dependency the
                        // rewrite adds. A carry-free variant (same mask-space
                        // gate, forward probe kept behind it, no loop-top test)
                        // measured neutral: within ±2.5% on every fixture on both
                        // boxes, no beyond-spread win on realshape. Neither was
                        // adopted.
                        if SEARCH_END {
                            let tmp3a = _mm256_loadu_si256(sp.offset(i + 3) as *const __m256i);
                            let tmp3b = _mm256_loadu_si256(sp.offset(i + 35) as *const __m256i);
                            let m3eqy_a =
                                _mm256_and_si256(match2_eq_a, _mm256_cmpeq_epi8(y_needle, tmp3a));
                            let m3eqy_b =
                                _mm256_and_si256(match2_eq_b, _mm256_cmpeq_epi8(y_needle, tmp3b));
                            if _mm256_movemask_epi8(_mm256_or_si256(m3eqy_a, m3eqy_b)) != 0 {
                                let m1lf_a = _mm256_cmpeq_epi8(
                                    lf,
                                    _mm256_loadu_si256(sp.offset(i + 1) as *const __m256i),
                                );
                                let m1lf_b = _mm256_cmpeq_epi8(
                                    lf,
                                    _mm256_loadu_si256(sp.offset(i + 33) as *const __m256i),
                                );
                                let end_found = _mm256_movemask_epi8(_mm256_or_si256(
                                    _mm256_and_si256(
                                        m3eqy_a,
                                        _mm256_and_si256(m1lf_a, _mm256_cmpeq_epi8(a, cr)),
                                    ),
                                    _mm256_and_si256(
                                        m3eqy_b,
                                        _mm256_and_si256(m1lf_b, _mm256_cmpeq_epi8(b, cr)),
                                    ),
                                ));
                                if end_found != 0 {
                                    state.state = x86_break_state(
                                        input,
                                        (span as isize + i) as usize,
                                        mask,
                                        esc_first,
                                    );
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
                // `+` not `|`: `x << 1` always has bit 0 clear and
                // `esc_first_in` is 0 or 1, so the two are identical, but the
                // add folds the shift and the merge into one 3-operand
                // `lea r, [esc + 2*mask_eq]` — the oracle's
                // `(maskEq << 1) + escFirst` (decoder_avx2_base.h:434).
                let eq_shift1 = (mask_eq << 1).wrapping_add(esc_first_in);
                let collision = (mask_eq & eq_shift1) != 0;
                let fixed_eq = if collision {
                    fix_eq_mask(mask_eq, eq_shift1)
                } else {
                    mask_eq
                };
                let escaped = (fixed_eq << 1).wrapping_add(esc_first_in);
                esc_first = fixed_eq >> 63;
                let (decoded_a, decoded_b) = if escaped == 0 {
                    (_mm256_add_epi8(a, yenc_offset), _mm256_add_epi8(b, sub42))
                } else if collision {
                    avx2_decode_with_escape_mask(a, b, escaped)
                } else {
                    let sel_a = _mm256_alignr_epi8::<15>(
                        eq_va,
                        _mm256_inserti128_si256(eq_needle, _mm256_castsi256_si128(eq_va), 1),
                    );
                    let sel_b = _mm256_cmpeq_epi8(
                        _mm256_loadu_si256(sp.offset(i + 31) as *const __m256i),
                        eq_needle,
                    );
                    (
                        _mm256_add_epi8(a, _mm256_blendv_epi8(yenc_offset, esc_off, sel_a)),
                        _mm256_add_epi8(b, _mm256_blendv_epi8(sub42, esc_off, sel_b)),
                    )
                };

                let skip = mask & !escaped;
                yenc_offset = _mm256_xor_si256(
                    sub42,
                    _mm256_zextsi128_si256(_mm_slli_epi16::<6>(_mm_cvtsi32_si128(
                        esc_first as i32,
                    ))),
                );

                out = avx2_compact_store64(decoded_a, decoded_b, skip, table, out);
            } else {
                _mm256_storeu_si256(out as *mut __m256i, _mm256_add_epi8(a, yenc_offset));
                _mm256_storeu_si256(out.add(32) as *mut __m256i, _mm256_add_epi8(b, sub42));
                out = out.add(WIDTH);
                esc_first = 0;
                yenc_offset = sub42;
            }
            i += WIDTH as isize;
        }
        src = (span as isize + i) as usize;
    }

    // `out` advanced by the SIMD span (or not at all); hand the scalar epilogue
    // the equivalent byte offset.
    let mut dst = out.offset_from(out_base) as usize;

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

/// AVX2 decode: a flat span loop carrying the escape/line state in registers,
/// one special-char mask per 64-byte window, a straight `add(-42)` + store on
/// the common window with no specials, and a single 2-lane LUT compaction on
/// windows that contain `= \r \n`. Escape resolution runs through
/// `fix_eq_mask` + `avx2_decode_with_escape_mask`. The rare dot-stuffing
/// (`\r\n.`) and end-marker (`=y`) cases fall back to the scalar decoder for
/// that one window.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[allow(unsafe_op_in_unsafe_fn)]
pub(super) unsafe fn decode_kernel_avx2(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    use std::arch::x86_64::*;
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

    // Hot path: faithful rapidyenc do_decode_avx2 port (raw dot-unstuffing),
    // both `searchEnd` instantiations. The other combos keep the general kernel
    // below.
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
            decode_kernel_avx2_raw::<true>(&input[head_src..], &mut output[head_dst..], state, mode)
        } else {
            decode_kernel_avx2_raw::<false>(input, output, state, mode)
        };
        return x86_fold_head(outcome, head_src, head_dst);
    }

    // The general kernel below indexes `input`/`output` absolutely, so it just
    // resumes at the head-resolved cursors (0 unless the head loop ran).
    let mut src = head_src;
    let mut dst = head_dst;

    // Trailing bytes kept for the scalar epilogue so cross-window CRLF, dot,
    // and escape sequences stay exact.
    let tail = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail);

    if input.len() > WIDTH * 2 {
        let sub42 = _mm256_set1_epi8(42i8.wrapping_neg());
        let eq_needle = _mm256_set1_epi8(b'=' as i8);
        let table = compact_table_16().as_ptr() as *const u8;

        // Carry the decoder state in registers for the length of the span. The
        // hot windows (bulk data, plain line breaks) only ever touch these
        // locals; `state` is written back to memory just at the rare scalar
        // bails below and once when the span loop exits, so the common path
        // pays no `&mut KernelState` round-trip per 64 bytes.
        let mut carry_state = state.state;
        let mut carry_end = state.end;

        'span: while (!search_end || carry_end == DecodeEnd::None) && src + WIDTH <= simd_limit {
            // Resolve any state the vector path can't carry directly (mid
            // escape/CR straddles, a stuffed dot at a line start, a pending
            // `=y`) with the scalar decoder, one step at a time.
            let simple = matches!(carry_state, DecoderState::None | DecoderState::Eq);
            let clean_line_start = carry_state == DecoderState::CrLf
                && !(dot_unstuffing && input[src] == b'.')
                && !(search_end && input[src] == b'=');
            if !(simple || clean_line_start) {
                state.state = carry_state;
                state.end = carry_end;
                let stepped = decode_scalar_step(input, &mut src, output, &mut dst, state, mode)?;
                carry_state = state.state;
                carry_end = state.end;
                if !stepped {
                    break 'span;
                }
                continue;
            }

            let esc_first = (carry_state == DecoderState::Eq) as u64;
            let at_line_start = (carry_state == DecoderState::CrLf) as u64;

            let a = _mm256_loadu_si256(input.as_ptr().add(src) as *const __m256i);
            let b = _mm256_loadu_si256(input.as_ptr().add(src + 32) as *const __m256i);
            let specials = avx2_special_mask64(a, b);

            // Common window: no special bytes, no carried escape → bulk decode.
            if specials == 0 && esc_first == 0 {
                _mm256_storeu_si256(
                    output.as_mut_ptr().add(dst) as *mut __m256i,
                    _mm256_add_epi8(a, sub42),
                );
                _mm256_storeu_si256(
                    output.as_mut_ptr().add(dst + 32) as *mut __m256i,
                    _mm256_add_epi8(b, sub42),
                );
                src += WIDTH;
                dst += WIDTH;
                carry_state = DecoderState::None;
                continue;
            }

            let eq_va = _mm256_cmpeq_epi8(a, eq_needle);
            let eq_vb = _mm256_cmpeq_epi8(b, eq_needle);
            let eq = (_mm256_movemask_epi8(eq_va) as u32 as u64)
                | ((_mm256_movemask_epi8(eq_vb) as u32 as u64) << 32);
            // Isolated escapes (the common case) need neither `fix_eq_mask` nor
            // a mask→vector reconstruction: the escape offsets fall straight out
            // of the `=` compare vectors shifted one byte (see the decode
            // selection below). Only genuine consecutive-`=` collisions
            // (`eq & eq_shift1 != 0`, e.g. `==`, or a `=` right after the
            // carried entry escape) need the bit correction + reverse-movemask.
            let eq_shift1 = (eq << 1) | esc_first;
            let collision = (eq & eq_shift1) != 0;
            let fixed_eq = if collision {
                fix_eq_mask(eq, eq_shift1)
            } else {
                eq
            };
            let escaped = (fixed_eq << 1) | esc_first;
            // Real (unescaped) `\r`/`\n` break positions come straight out of
            // the specials mask; the char after each `=` is escaped data.
            let breaks = (specials & !eq) & !escaped;

            // Body dots need no special handling — they are not in the
            // specials mask, so the heavy path decodes them as ordinary data.
            // Only a *stuffed* dot (a `.` at a line start, i.e. right after an
            // unescaped `\r\n`, or at the entry line start) must be stripped;
            // and a `=y` pair may be a control marker. Both are ~0.2%, so those
            // windows bail to the scalar decoder. The
            // CRLF/line-start masks are computed only when the window actually
            // contains a `.`, keeping the common (bodydot-free) window cheap.
            // A stuffed dot can only exist at a line start (right after an
            // unescaped `\r\n`, or the entry line start). No unescaped break
            // (`breaks`) and no carried line start (`at_line_start`) => no line
            // start in this window => `stuffed_dot` would be 0 anyway, so skip
            // the whole `.` probe (2 vpcmpeqb + vptest) on pure-body/escape
            // windows. Mirrors rapidyenc gating its dot probe on `mask != maskEq`.
            let stuffed_dot = if dot_unstuffing && (breaks != 0 || at_line_start != 0) {
                let dot_needle = _mm256_set1_epi8(b'.' as i8);
                let dcmp_a = _mm256_cmpeq_epi8(a, dot_needle);
                let dcmp_b = _mm256_cmpeq_epi8(b, dot_needle);
                let d_or = _mm256_or_si256(dcmp_a, dcmp_b);
                // One `vptest` over the OR of the two `.` compares replaces two
                // `vpmovmskb` on the dominant dot-free heavy window (crlf_only:
                // every window; realshape: most). `testz == 0` means a `.` is
                // present — only then materialize the bitmask and run the exact
                // line-start path. Byte-exact: `dots` is the same movemask the
                // old `avx2_mask64(a, b, '.')` produced.
                if _mm256_testz_si256(d_or, d_or) == 0 {
                    let dots = (_mm256_movemask_epi8(dcmp_a) as u32 as u64)
                        | ((_mm256_movemask_epi8(dcmp_b) as u32 as u64) << 32);
                    let cr = avx2_mask64(a, b, b'\r');
                    let lf = specials & !eq & !cr;
                    let crlf = cr & (lf >> 1);
                    let line_start = at_line_start | (crlf << 2);
                    dots & line_start & !escaped
                } else {
                    0
                }
            } else {
                0
            };
            let eqy_any = if search_end {
                eq & (avx2_mask64(a, b, b'y') >> 1)
            } else {
                0
            };
            if stuffed_dot != 0 || eqy_any != 0 {
                state.state = carry_state;
                state.end = carry_end;
                let stepped = decode_scalar_step(input, &mut src, output, &mut dst, state, mode)?;
                carry_state = state.state;
                carry_end = state.end;
                if !stepped {
                    break 'span;
                }
                continue;
            }
            // A `=` at a line start (`at_line_start` for byte 0) is a possible
            // control line — hand it to scalar as well.
            if search_end && at_line_start != 0 && eq & 1 != 0 {
                state.state = carry_state;
                state.end = carry_end;
                let stepped = decode_scalar_step(input, &mut src, output, &mut dst, state, mode)?;
                carry_state = state.state;
                carry_end = state.end;
                if !stepped {
                    break 'span;
                }
                continue;
            }

            let skip = fixed_eq | breaks;
            let (decoded_a, decoded_b) = if escaped == 0 {
                (_mm256_add_epi8(a, sub42), _mm256_add_epi8(b, sub42))
            } else if collision {
                // Rare: a consecutive-`=` run — resolve from the corrected mask.
                avx2_decode_with_escape_mask(a, b, escaped)
            } else {
                // Common (isolated escapes): unescape straight from the `=`
                // compare, shifted one byte. Lane A shifts `eq_va` via
                // `vinserti128` (Zen2 lat1/tput0.5) instead of a lane-crossing
                // `vperm2i128` (lat3/tput1); the fill lane is `eq_needle` (0x3D,
                // high bit 0) so byte 0 reads not-escaped, and the carried entry
                // escape is applied via `first_off`. Lane B recomputes the `=`
                // compare on the byte-shifted window load (bytes [31..63),
                // in-bounds) — avoiding a second lane-crossing shuffle entirely.
                // Mirrors rapidyenc decoder_avx2_base.h:511-531.
                let sel_a = _mm256_alignr_epi8::<15>(
                    eq_va,
                    _mm256_inserti128_si256(eq_needle, _mm256_castsi256_si128(eq_va), 1),
                );
                let sel_b = _mm256_cmpeq_epi8(
                    _mm256_loadu_si256(input.as_ptr().add(src + 31) as *const __m256i),
                    eq_needle,
                );
                // esc_first is 0 in ~all windows, so keep the common lane-A
                // base as plain -42 (identical to lane B) and only build the
                // byte-0 = -106 patch when an escape actually carried in.
                let first_off = if esc_first & 1 != 0 {
                    _mm256_xor_si256(
                        sub42,
                        _mm256_inserti128_si256(_mm256_setzero_si256(), _mm_cvtsi32_si128(0x40), 0),
                    )
                } else {
                    sub42
                };
                let esc_off = _mm256_set1_epi8(-106);
                (
                    _mm256_add_epi8(a, _mm256_blendv_epi8(first_off, esc_off, sel_a)),
                    _mm256_add_epi8(b, _mm256_blendv_epi8(sub42, esc_off, sel_b)),
                )
            };

            if skip == 0 {
                _mm256_storeu_si256(output.as_mut_ptr().add(dst) as *mut __m256i, decoded_a);
                _mm256_storeu_si256(output.as_mut_ptr().add(dst + 32) as *mut __m256i, decoded_b);
                dst += WIDTH;
            } else {
                // 2-lane compaction: one 256-bit shuffle folds the
                // low/high 16-byte compaction tables, then each 16-byte lane
                // stores with a popcount-advanced cursor. Shared with the raw
                // kernel so the two can't drift apart.
                let out = avx2_compact_store64(
                    decoded_a,
                    decoded_b,
                    skip,
                    table,
                    output.as_mut_ptr().add(dst),
                );
                dst = out.offset_from(output.as_mut_ptr()) as usize;
            }

            src += WIDTH;
            let win = &input[src - WIDTH..src];
            carry_state = span_end_state(win, fixed_eq, dot_unstuffing);
        }

        // Publish the register-carried state back to `state` for the scalar
        // tail and the returned outcome.
        state.state = carry_state;
        state.end = carry_end;
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

/// 2×2-lane LUT compaction + store for one 64-byte window, in the oracle's
/// exact addressing shape (rapidyenc `decoder_avx2_base.h:556-600`, the
/// `PLATFORM_AMD64` arm). Byte-for-byte identical output to the previous
/// open-coded form; only the index/cursor arithmetic changed:
///
///   * table offsets are computed as **byte** offsets, not element indices, so
///     each lane costs one shift + one AND instead of shift + AND + scale
///     (`(skip >> 12) & 0x7fff0` is `((skip >> 16) & 0x7fff) * 16`);
///   * a single `skip >> 28` is shared between lane 2's table offset AND lane
///     2's popcount, replacing two independent extractions;
///   * popcounts use position-invariant masks (`skip & 0xffff_0000` rather than
///     `(skip >> 16) & 0xffff`) — popcount ignores bit position, so the shift
///     is pure waste;
///   * the four `+16` lane advances fold into one `+64` at the end, and the
///     cursor is a running pointer instead of a `(base, offset)` pair, which
///     also stops the output base from being spilled and reloaded per window.
///
/// Measured on Haswell (E5-2666 v3) the old form cost 29 scalar µops here
/// against the oracle's 27; combined with the loop-induction fix this closes
/// the specials-path µop gap (see `yenc-avx2-lever` notes).
///
/// `out` must have 64 writable bytes; the returned pointer is
/// `out + 64 - popcount(skip)`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[inline]
#[allow(unsafe_op_in_unsafe_fn)]
pub(super) unsafe fn avx2_compact_store64(
    decoded_a: std::arch::x86_64::__m256i,
    decoded_b: std::arch::x86_64::__m256i,
    skip: u64,
    table: *const u8,
    out: *mut u8,
) -> *mut u8 {
    use std::arch::x86_64::*;

    // `wrapping_*`: the cursor legitimately steps backwards past `out` between
    // lane stores (the oracle's `p -= popcnt(...)` then `store [p + 16]`); every
    // dereference below is still at or above `out`.
    let mut p = out;

    let shuf_a = _mm256_inserti128_si256(
        _mm256_castsi128_si256(_mm_loadu_si128(
            table.add(((skip << 4) & 0x7_fff0) as usize) as *const __m128i,
        )),
        _mm_loadu_si128(table.add(((skip >> 12) & 0x7_fff0) as usize) as *const __m128i),
        1,
    );
    let packed_a = _mm256_shuffle_epi8(decoded_a, shuf_a);
    _mm_storeu_si128(p as *mut __m128i, _mm256_castsi256_si128(packed_a));
    p = p.wrapping_sub((skip as u32 & 0xffff).count_ones() as usize);
    _mm_storeu_si128(
        p.wrapping_add(16) as *mut __m128i,
        _mm256_extracti128_si256::<1>(packed_a),
    );
    p = p.wrapping_sub((skip as u32 & 0xffff_0000).count_ones() as usize);

    // `hi` carries bits 28.. of `skip`; `hi & 0x7fff0` is lane 2's byte offset
    // and `hi & 0xffff0` is lane 2's popcount window — both off the one shift.
    let hi = skip >> 28;
    let shuf_b = _mm256_inserti128_si256(
        _mm256_castsi128_si256(_mm_loadu_si128(
            table.add((hi & 0x7_fff0) as usize) as *const __m128i
        )),
        _mm_loadu_si128(table.add(((hi >> 16) & 0x7_fff0) as usize) as *const __m128i),
        1,
    );
    let packed_b = _mm256_shuffle_epi8(decoded_b, shuf_b);
    _mm_storeu_si128(
        p.wrapping_add(32) as *mut __m128i,
        _mm256_castsi256_si128(packed_b),
    );
    p = p.wrapping_sub((hi as u32 & 0xf_fff0).count_ones() as usize);
    _mm_storeu_si128(
        p.wrapping_add(48) as *mut __m128i,
        _mm256_extracti128_si256::<1>(packed_b),
    );
    p = p.wrapping_sub(((hi >> 20) as u32).count_ones() as usize);
    p.wrapping_add(64)
}

/// Byte-replication shuffle indices expanding a broadcast `escaped` u64 into
/// per-byte mask lanes (lane A: source bytes 0..3, lane B: bytes 4..7) — the
/// RIP-relative twins of the `_mm256_set_epi32` constants in
/// [`avx2_decode_with_escape_mask`], laid out little-endian.
#[cfg(target_arch = "x86_64")]
#[repr(C, align(32))]
struct Align32([u8; 32]);
#[cfg(target_arch = "x86_64")]
static AVX2_ESC_IDX_A: Align32 = Align32([
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
]);
#[cfg(target_arch = "x86_64")]
static AVX2_ESC_IDX_B: Align32 = Align32([
    4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
]);
/// One-bit-per-lane selectors within a replicated byte (`0x8040201008040201`),
/// broadcast per-quadword — the RIP twin of `bit_lanes` in
/// [`avx2_decode_with_escape_mask`].
#[cfg(target_arch = "x86_64")]
static AVX2_ESC_BIT_LANES: u64 = 0x8040_2010_0804_0201;

/// Rung 3: the whole `SEARCH_END = false` SIMD span loop of
/// [`decode_kernel_avx2_raw`] as ONE `asm!` block — a transliteration of the
/// r5 live-set probe's emitted loop (`probe-se-false.s`, the arm that measured
/// realshape 0.944 on Alder Lake), not a de-novo schedule. Byte-identical
/// output to the intrinsic loop (enforced by the in-kernel rapidyenc oracle
/// differential + full lib suite — per the Rung 2 lesson, isolated-call
/// differentials are structurally blind to input-register clobbers, so the
/// in-kernel net is the authority).
///
/// Why asm: five rounds of measurement (r5–r8) proved the intrinsic loop's
/// ~2.4 cycles/window of GPR spill traffic (compaction-LUT base reload +
/// output-cursor round-trip) cannot be removed by any cfg/attribute
/// combination without re-rolling the OTHER instantiation's allocation into a
/// +55% cliff — the win and the cliff were the same allocator event. Inside
/// this block there is no allocator: `table`, the cursor, and the whole mask
/// chain are pinned by construction, on every rustc from here on.
///
/// Two deliberate improvements over the probe's emitted code, both verified
/// equivalent: (1) the back edge is `add $64,i / jnz` — the `add` sets ZF at
/// zero, so the probe's separate limit `cmp` is dropped; (2) the
/// isolated-escape fill vector (which the probe reloaded from a stack slot
/// every escape window — its one remaining spill) is just `eq_needle`, already
/// pinned, so this loop has ZERO stack traffic.
///
/// Register contract (matches the probe listing):
/// - carried across windows: `i`, `out`, `esc_first` (GPRs); `yenc_offset`
///   (byte0 = -106 while an escape straddles, else -42) and `min_mask` (byte
///   0/1 clamped while a line-start dot straddles, else all '.') as ymm.
///   Every specials path rewrites `min_mask` before the back edge; the clean
///   path provably cannot see a pending dot (the injected mask bit forces the
///   specials path), so it leaves `min_mask` untouched.
/// - `esc_first`'s register is transiently reused for `escaped` (collision
///   arm) and then `skip` (store head), exactly like the probe's `%rcx`; it is
///   restored to the NEW esc_first before every back edge.
/// - `fives` = `0x5555_5555_5555_5555`, the `fix_eq_mask` run-parity constant.
///
/// Safety: same contract as the intrinsic loop — `sp` is `input + span` with
/// the 67-byte tail reserve behind it (deepest read is `sp + i + 65`), `out`
/// has ≥ 64 writable bytes per window (the compaction cursor steps backwards
/// between lane stores; every store lands at or above the window base).
/// Flags are clobbered (`preserves_flags` deliberately NOT set); the block
/// reads input + LUT and writes output (`nomem`/`readonly` deliberately NOT
/// set); no stack use (`nostack`).
#[cfg(target_arch = "x86_64")]
#[cfg_attr(not(weaver_yenc_raw_asm), allow(dead_code))]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
// `inline(never)`: the loop lives in its own small frame, exactly like the
// probe arm whose emission this block transliterates — measured, not
// aesthetic: inlined into `decode_kernel_avx2`'s large body the identical
// instruction bytes ran 4–8% slower on ADL realshape (r9/r9b/r9c), with
// front-end counters clean; the standalone frame is part of the measured
// shape.
#[inline(never)]
#[allow(unsafe_op_in_unsafe_fn)]
pub(super) unsafe fn avx2_raw_span_loop_asm(
    sp: *const u8,
    i: &mut isize,
    out: &mut *mut u8,
    esc_first: &mut u64,
    min_mask: std::arch::x86_64::__m256i,
    yenc_offset: std::arch::x86_64::__m256i,
) {
    use std::arch::x86_64::*;

    let sub42 = _mm256_set1_epi8(42i8.wrapping_neg());
    let dot = _mm256_set1_epi8(b'.' as i8);
    let eq_needle = _mm256_set1_epi8(b'=' as i8);
    let cr = _mm256_set1_epi8(b'\r' as i8);
    let lf = _mm256_set1_epi8(b'\n' as i8);
    let esc_off = _mm256_set1_epi8(-106);
    let special_lut = _mm256_set_epi8(
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
    let table = compact_table_16().as_ptr() as *const u8;

    let mut i_v = *i;
    let mut out_v = *out;
    let mut ef_v = *esc_first;

    core::arch::asm!(
        "jmp 20f",
        // ---- clean window (mask == 0): bulk add + store ----------------
        ".p2align 4",
        "29:",
        "vpaddb {s0}, {va}, {yov}",
        "vmovdqu ymmword ptr [{out}], {s0}",
        "vpaddb {s0}, {vb}, {s42}",
        "vmovdqu ymmword ptr [{out} + 32], {s0}",
        "vmovdqa {yov}, {s42}",
        "xor {ef:e}, {ef:e}",
        "add {out}, 64",
        "add {i}, 64",
        "jz 30f",
        // ---- loop head: load window, one specials probe per lane -------
        "20:",
        "vmovdqu {va}, ymmword ptr [{sp} + {i}]",
        "vmovdqu {vb}, ymmword ptr [{sp} + {i} + 32]",
        "vpminub {s0}, {va}, {mmv}",
        "vpshufb {s0}, {lut}, {s0}",
        "vpminub {s1}, {vb}, {dot}",
        "vpshufb {s1}, {lut}, {s1}",
        "vpcmpeqb {s1}, {s1}, {vb}",
        "vpmovmskb {t2:e}, {s1}",
        "shl {t2}, 32",
        "vpcmpeqb {s0}, {s0}, {va}",
        "vpmovmskb {mask:e}, {s0}",
        "or {mask}, {t2}",
        "jz 29b",
        // ---- specials window: the `=` masks ----------------------------
        "vpcmpeqb {s2}, {va}, {eqn}",
        "vpcmpeqb {s3}, {vb}, {eqn}",
        "vpmovmskb {t1:e}, {s3}",
        "mov {t2}, {t1}",
        "shl {t2}, 32",
        "vpmovmskb {meq:e}, {s2}",
        "or {meq}, {t2}",
        "cmp {mask}, {meq}",
        "je 26f",
        // ---- CR/LF present: `\r` + `.`-at-+2 probe ---------------------
        "vpcmpeqb {s3}, {va}, {crv}",
        "vpcmpeqb {s0}, {dot}, ymmword ptr [{sp} + {i} + 2]",
        "vpand {s3}, {s3}, {s0}",
        "vpcmpeqb {s1}, {vb}, {crv}",
        "vpcmpeqb {s0}, {dot}, ymmword ptr [{sp} + {i} + 34]",
        "vpand {s1}, {s1}, {s0}",
        "vpor {s0}, {s3}, {s1}",
        "vpmovmskb {t2:e}, {s0}",
        "test {t2:e}, {t2:e}",
        "jz 26f",
        // ---- stuffed dot: merge `\r\n.` into the mask, clamp min_mask --
        "vpcmpeqb {s0}, {lfv}, ymmword ptr [{sp} + {i} + 1]",
        "vpand {s3}, {s3}, {s0}",
        "vpcmpeqb {s0}, {lfv}, ymmword ptr [{sp} + {i} + 33]",
        "vpand {s1}, {s1}, {s0}",
        "vpmovmskb {t2:e}, {s3}",
        "vpmovmskb {t3:e}, {s1}",
        "shl {t3}, 34",
        "lea {t2}, [{t3} + {t2}*4]",
        "or {mask}, {t2}",
        "vextracti128 {s0:x}, {s1}, 1",
        "vpsrldq {s0:x}, {s0:x}, 14",
        "vpsubusb {mmv}, {dot}, {s0}",
        "lea {esc}, [{ef} + {meq}*2]",
        "test {meq}, {esc}",
        "jnz 27f",
        // (falls into 24 — the probe's layout: the dot arm sits directly
        // above the no-collision path)
        // ---- no collision (fixed_eq == mask_eq) ------------------------
        "24:",
        "shr {t1:e}, 31",
        "test {esc}, {esc}",
        "jz 18f",
        // isolated escapes: select offsets from the `=` compares shifted
        // one byte; lane A's byte-0 partner comes from `yenc_offset`.
        "vinserti128 {s0}, {eqn}, {s2:x}, 1",
        "vpalignr {s0}, {s2}, {s0}, 15",
        "vpcmpeqb {s1}, {eqn}, ymmword ptr [{sp} + {i} + 31]",
        "vpblendvb {s3}, {yov}, {eof}, {s0}",
        "vpaddb {va}, {va}, {s3}",
        "vpblendvb {s3}, {s42}, {eof}, {s1}",
        "vpaddb {vb}, {vb}, {s3}",
        "jmp 19f",
        // ---- no stuffed dot / eq-only: reset min_mask, collision test --
        // (after 24, per the probe's layout: the collision-free exit is a
        // backward jump)
        ".p2align 4",
        "26:",
        "vmovdqa {mmv}, {dot}",
        "lea {esc}, [{ef} + {meq}*2]",
        "test {meq}, {esc}",
        "jz 24b",
        // (falls into 27)
        // ---- consecutive-`=` collision: fix_eq_mask bit hack -----------
        "27:",
        "mov {t1}, {meq}",
        "and {t1}, {fives}",
        "andn {t1}, {esc}, {t1}",
        "add {t1}, {meq}",
        "xor {t1}, {fives}",
        "and {t1}, {meq}",
        "lea {esc}, [{t1} + {t1}]",
        "shr {t1}, 63",
        "add {ef}, {esc}",
        "mov {esc}, {ef}",
        "jz 18f",
        // expand the resolved `escaped` mask to per-byte selects
        "vmovq {s0:x}, {ef}",
        "vpbroadcastq {s0}, {s0:x}",
        "vpshufb {s1}, {s0}, ymmword ptr [rip + {ia}]",
        "vpbroadcastq {s3}, qword ptr [rip + {bl}]",
        "vpshufb {s0}, {s0}, ymmword ptr [rip + {ib}]",
        "vpand {s1}, {s1}, {s3}",
        "vpand {s0}, {s0}, {s3}",
        "vpcmpeqb {s1}, {s1}, {s3}",
        "vpblendvb {s1}, {s42}, {eof}, {s1}",
        "vpaddb {va}, {va}, {s1}",
        "vpcmpeqb {s0}, {s0}, {s3}",
        "vpblendvb {s0}, {s42}, {eof}, {s0}",
        "vpaddb {vb}, {vb}, {s0}",
        "jmp 19f",
        // ---- escaped == 0: plain add -----------------------------------
        ".p2align 4",
        "18:",
        "vpaddb {va}, {va}, {yov}",
        "vpaddb {vb}, {vb}, {s42}",
        "xor {esc:e}, {esc:e}",
        // ---- skip mask, next-window yenc_offset, compaction store ------
        "19:",
        "andn {ef}, {esc}, {mask}",
        "vmovd {yov:x}, {t1:e}",
        "vpsllw {yov:x}, {yov:x}, 6",
        "vpxor {yov}, {yov}, {s42}",
        "mov {mask:e}, {ef:e}",
        "shl {mask:e}, 4",
        "and {mask:e}, 0x7fff0",
        "vmovdqu {s0:x}, xmmword ptr [{tab} + {mask}]",
        "mov {mask:e}, {ef:e}",
        "shr {mask:e}, 12",
        "and {mask:e}, 0x7fff0",
        "vinserti128 {s0}, {s0}, xmmword ptr [{tab} + {mask}], 1",
        "vpshufb {s0}, {va}, {s0}",
        "vmovdqu xmmword ptr [{out}], {s0:x}",
        "movzx {mask:e}, {ef:x}",
        "popcnt {mask:e}, {mask:e}",
        "sub {out}, {mask}",
        "vextracti128 xmmword ptr [{out} + 16], {s0}, 1",
        "mov {mask:e}, {ef:e}",
        "and {mask:e}, 0xffff0000",
        "popcnt {mask:e}, {mask:e}",
        "sub {out}, {mask}",
        "mov {mask}, {ef}",
        "shr {mask}, 28",
        "mov {meq:e}, {mask:e}",
        "and {meq:e}, 0x7fff0",
        "vmovdqu {s0:x}, xmmword ptr [{tab} + {meq}]",
        "mov {meq}, {mask}",
        "shr {meq}, 16",
        "and {meq:e}, 0x7fff0",
        "vinserti128 {s0}, {s0}, xmmword ptr [{tab} + {meq}], 1",
        "vpshufb {s0}, {vb}, {s0}",
        "vmovdqu xmmword ptr [{out} + 32], {s0:x}",
        "and {mask:e}, 0xffff0",
        "popcnt {mask:e}, {mask:e}",
        "sub {out}, {mask}",
        "vextracti128 xmmword ptr [{out} + 48], {s0}, 1",
        "shr {ef}, 48",
        "popcnt {ef:e}, {ef:e}",
        "sub {out}, {ef}",
        "mov {ef}, {t1}",
        "add {out}, 64",
        "add {i}, 64",
        "jnz 20b",
        "30:",
        sp = in(reg) sp,
        i = inout(reg) i_v,
        out = inout(reg) out_v,
        ef = inout(reg) ef_v,
        tab = in(reg) table,
        fives = in(reg) 0x5555_5555_5555_5555u64,
        mask = out(reg) _,
        meq = out(reg) _,
        t1 = out(reg) _,
        t2 = out(reg) _,
        t3 = out(reg) _,
        esc = out(reg) _,
        lut = in(ymm_reg) special_lut,
        dot = in(ymm_reg) dot,
        s42 = in(ymm_reg) sub42,
        eqn = in(ymm_reg) eq_needle,
        crv = in(ymm_reg) cr,
        lfv = in(ymm_reg) lf,
        eof = in(ymm_reg) esc_off,
        yov = inout(ymm_reg) yenc_offset => _,
        mmv = inout(ymm_reg) min_mask => _,
        va = out(ymm_reg) _,
        vb = out(ymm_reg) _,
        s0 = out(ymm_reg) _,
        s1 = out(ymm_reg) _,
        s2 = out(ymm_reg) _,
        s3 = out(ymm_reg) _,
        ia = sym AVX2_ESC_IDX_A,
        ib = sym AVX2_ESC_IDX_B,
        bl = sym AVX2_ESC_BIT_LANES,
        options(nostack),
    );

    *i = i_v;
    *out = out_v;
    *esc_first = ef_v;
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[inline]
pub(super) unsafe fn avx2_special_mask64(
    a: std::arch::x86_64::__m256i,
    b: std::arch::x86_64::__m256i,
) -> u64 {
    use std::arch::x86_64::*;

    let table = _mm256_set_epi8(
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
    let clamp = _mm256_set1_epi8(b'.' as i8);
    let mask_a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(
        a,
        _mm256_shuffle_epi8(table, _mm256_min_epu8(a, clamp)),
    )) as u32 as u64;
    let mask_b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(
        b,
        _mm256_shuffle_epi8(table, _mm256_min_epu8(b, clamp)),
    )) as u32 as u64;
    mask_a | (mask_b << 32)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[inline]
pub(super) unsafe fn avx2_mask64(
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
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
// AVX2 escaped-byte offset path (mask expanded to a vector select).
pub(super) unsafe fn avx2_decode_with_escape_mask(
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

/// AVX2 implementation: process 32 bytes at a time.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
pub(super) unsafe fn decode_normal_run_avx2(
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
