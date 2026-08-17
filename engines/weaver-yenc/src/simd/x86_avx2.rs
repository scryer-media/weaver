use super::*;

/// MAINTENANCE CONTRACT (operator-set): this Rust kernel — and the whole
/// intrinsic path selected by `WEAVER_YENC_RAW_ASM=0` — is preserved
/// permanently as the tunable source of truth behind the frozen `asm!`
/// kernels. The workflow for any future tuning or bugfix is: change the
/// Rust here, validate + measure through the `=0` escape hatch, and only
/// then re-transliterate the winning emission into the asm blocks (see
/// `avx2_raw_kernel_oracle` / `avx2_raw_span_setrue_asm` and the
/// yenc-program JOURNAL for the transliteration method). Never let the asm
/// and this Rust drift semantically: the oracle differential suite is the
/// drift detector.
///
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

    // Rung 3 r9g: the SEARCH_END=false span runs the oracle-model asm kernel
    // (aligned single-cursor loop transliterated from rapidyenc's own emission
    // on the measurement host — see avx2_raw_kernel_oracle). Isolated function
    // so the generic body below stays byte-identical for SEARCH_END=true and
    // non-asm builds.
    #[cfg(weaver_yenc_raw_asm)]
    if !SEARCH_END {
        return avx2_raw_kernel_oracle(input, output, state, mode);
    }

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
        // r10: the SEARCH_END=true span runs the frozen-roll asm kernel; on a
        // terminator hit it exits with the window unconsumed (i != 0) and the
        // pre-merge mask, feeding the same no-backtrack break glue the Rust
        // loop used. The Rust loop below remains the =0 escape hatch and the
        // tunable source of truth (see the maintenance contract above).
        #[cfg(weaver_yenc_raw_asm)]
        let mut asm_ran = false;
        #[cfg(weaver_yenc_raw_asm)]
        if SEARCH_END && i != 0 {
            let mut break_mask = 0u64;
            avx2_raw_span_setrue_asm(
                input.as_ptr(),
                &mut i,
                &mut out,
                &mut esc_first,
                &mut break_mask,
                min_mask,
                yenc_offset,
            );
            if i != 0 {
                state.state =
                    x86_break_state(input, (span as isize + i) as usize, break_mask, esc_first);
                broke = true;
            }
            asm_ran = true;
        }
        #[cfg(weaver_yenc_raw_asm)]
        let run_rust_span = !asm_ran;
        #[cfg(not(weaver_yenc_raw_asm))]
        let run_rust_span = true;
        while run_rust_span && i != 0 {
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

/// Single-byte broadcast sources for the SE=true asm kernel's in-block
/// constant rematerialization (mirroring its source emission's rodata
/// broadcasts) plus the 32-byte specials LUT for restoring the table
/// register after the dot-arm uses it as scratch.
#[cfg(target_arch = "x86_64")]
static YB_DOT: u8 = 0x2e;
#[cfg(target_arch = "x86_64")]
static YB_EQ: u8 = 0x3d;
#[cfg(target_arch = "x86_64")]
static YB_CR: u8 = 0x0d;
#[cfg(target_arch = "x86_64")]
static YB_LF: u8 = 0x0a;
#[cfg(target_arch = "x86_64")]
static YB_SUB42: u8 = 0xd6;
#[cfg(target_arch = "x86_64")]
static YB_ESC: u8 = 0x96;
#[cfg(target_arch = "x86_64")]
static YB_Y: u8 = 0x79;
#[cfg(target_arch = "x86_64")]
static YW_EQY: u16 = 0x793d;
/// The specials LUT rows as bytes (index by min(byte, '.')): '.'->'.',
/// '\n'->'\n', '\r'->'\r', '='->'=' — everything else maps to 0xff (no
/// match). Identical per 16-byte lane; matches `special_lut` in the
/// kernels.
#[cfg(target_arch = "x86_64")]
static AVX2_SPECIAL_LUT: Align32 = Align32([
    0x2e, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0a, 0xff, 0xff, 0x0d, 0x3d, 0xff,
    0x2e, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0a, 0xff, 0xff, 0x0d, 0x3d, 0xff,
]);

/// Rung 3 r9g: the whole `SEARCH_END = false` kernel in the ORACLE'S OWN
/// shape — a transliteration of rapidyenc's compiled `do_decode_avx2`
/// (`isRaw=true, searchEnd=false`) as emitted by the measurement host's gcc
/// (extracted from the same-run harness binary; archived as
/// `yenc-program/oracle-adl.txt`). That emission IS the code the parity
/// ratio is measured against, so matching it is parity by construction.
///
/// The oracle's structure, faithfully kept:
/// - the input cursor is HEAD-ALIGNED to 64 bytes by a scalar prelude (in
///   Rust below), then the loop runs pure ALIGNED loads off one mid-window
///   cursor (`c` points 32 bytes in; lanes live at `[c-32]` and `[c]`) —
///   no cacheline-split window loads, and every memory operand is
///   base+disp8 (no index register, so load+op µops stay micro-fused);
/// - specials take a BACKWARD branch to a block laid above the loop head;
///   the store falls through into the next window's loads; the clean path
///   is the head's fallthrough with its own back edge;
/// - `skip == mask` on the common path (escaped bytes are never
///   special-table matches except `=\r`/`=\n`, which the collision path
///   handles by correcting `mask` with the resolved escape mask);
/// - the collision predicate is the oracle's WIDER `mask & eq_shift1`;
/// - `escFirst` is recomputed at the join (`meq >> 63`) and the
///   `yenc_offset` rebuild is interleaved into the store head;
/// - the CR/LF/dot needles are rematerialized inside their rare blocks
///   (3 rename-free µops) instead of pinning two more ymm constants;
/// - `min_mask` doubles as scratch in the CRLF probe and is rewritten on
///   every specials path before the back edge (dot-clamp or plain dot).
///
/// Deliberate deviations, each strictly smaller: LUT rows load via
/// `vmovdqu` (the heap table only guarantees byte alignment; unaligned
/// loads are same-speed on aligned rows), the collision expansion reads the
/// escape-select constants from this module's RIP statics and reuses the
/// pinned `sub42`/`esc_off` registers instead of reloading them from
/// rodata.
///
/// Safety: the caller guarantees the raw-path contract (`dot_unstuffing`,
/// entry state in {None,Eq,Cr,CrLf}); the scalar prelude/epilogue share the
/// kernel's usual bounds; the span keeps the 67-byte tail reserve, and the
/// deepest lookahead reads `c + 2 + 31 < span end + reserve`. Flags are
/// clobbered; the block reads input + LUT and writes output; no stack use.
#[cfg(target_arch = "x86_64")]
#[cfg_attr(not(weaver_yenc_raw_asm), allow(dead_code))]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn avx2_raw_kernel_oracle(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    use std::arch::x86_64::*;
    const WIDTH: usize = 64;

    let mut src = 0usize;
    let mut dst = 0usize;

    // Head-align the input cursor to a 64-byte boundary with the scalar
    // machine — the oracle's own prelude. At most 63 steps.
    while src < input.len() && (input.as_ptr() as usize + src) & (WIDTH - 1) != 0 {
        if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
            return Ok(KernelOutcome {
                consumed: src,
                written: dst,
                end: state.end.into(),
            });
        }
    }

    let tail = WIDTH - 1 + 4;
    let simd_limit = input.len().saturating_sub(tail);
    let span = (simd_limit.saturating_sub(src) / WIDTH) * WIDTH;

    if span > 0 {
        // entry state -> escFirst / minMask (oracle _do_decode_simd switch),
        // computed AFTER the alignment steps from the live state.
        let mut esc_first: u64 = (state.state == DecoderState::Eq) as u64;
        let entry_next_mask: u16 = match state.state {
            DecoderState::CrLf if input[src] == b'.' => 1,
            DecoderState::Cr
                if src + 1 < input.len() && input[src] == b'\n' && input[src + 1] == b'.' =>
            {
                2
            }
            _ => 0,
        };

        let sub42 = _mm256_set1_epi8(42i8.wrapping_neg());
        let dot = _mm256_set1_epi8(b'.' as i8);
        let eq_needle = _mm256_set1_epi8(b'=' as i8);
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
        let yenc_offset = if esc_first != 0 {
            _mm256_xor_si256(
                sub42,
                _mm256_inserti128_si256(_mm256_setzero_si256(), _mm_cvtsi32_si128(0x40), 0),
            )
        } else {
            sub42
        };
        let min_mask = if entry_next_mask != 0 {
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
        let table = compact_table_16().as_ptr() as *const u8;

        // Mid-window cursor and the oracle's negated end for the add-based
        // loop check (`mask := negend + c` is zero exactly at the last
        // window's end).
        let c0 = input.as_ptr().add(src + 32);
        let negend = -((input.as_ptr() as usize + src + span + 32) as isize);
        let c_v = c0;
        let mut out_v = output.as_mut_ptr().add(dst);
        let mut ef_v = esc_first;

        core::arch::asm!(
            "jmp 20f",
            // ---- specials (backward target of the head): `=` masks -------
            ".p2align 4",
            "21:",
            "vpcmpeqb {s0}, {hi}, {eqn}",
            "vpcmpeqb {s2}, {lo}, {eqn}",
            "vpmovmskb {t1:e}, {s0}",
            "vpmovmskb {meq:e}, {s2}",
            "shl {t1}, 32",
            "or {meq}, {t1}",
            "cmp {mask}, {meq}",
            "jne 23f",
            // ---- join: escape select (oracle 6c1b8) ----------------------
            "22:",
            "lea {t1}, [{ef} + {meq}*2]",
            "test {mask}, {t1}",
            "jnz 26f",
            // weaver's measured edge the oracle lacks (+11.7% on CRLF-heavy
            // content, r2 trade pricing): when the window itself carries no
            // `=` (meq == 0), skip the whole escape select — `yov` already
            // holds the carried-escape byte-0 offset, so the plain adds are
            // exact even when an escape straddled in. (The eq_shift1==0 form
            // of this gate failed the differential net; the meq==0 form
            // passes it and fires on strictly more windows.)
            "test {meq}, {meq}",
            "jz 28f",
            "vinserti128 {s0}, {eqn}, {s2:x}, 1",
            "mov {ef}, {meq}",
            "vpalignr {s2}, {s2}, {s0}, 15",
            "vpcmpeqb {s0}, {eqn}, ymmword ptr [{c} - 1]",
            "shr {ef}, 63",
            "vpblendvb {s2}, {yov}, {eof}, {s2}",
            "vpaddb {s2}, {lo}, {s2}",
            "vpblendvb {s0}, {s42}, {eof}, {s0}",
            "vpaddb {s0}, {hi}, {s0}",
            // ---- store (falls into the head; oracle 6c1f3) ---------------
            "24:",
            "mov {meq:e}, {mask:e}",
            "vmovd {yov:x}, {ef:e}",
            "add {c}, 64",
            "and {meq:e}, 0x7fff",
            "vpsllw {yov:x}, {yov:x}, 6",
            "shl {meq:e}, 4",
            "vmovdqu {s1:x}, xmmword ptr [{tab} + {meq}]",
            "mov {meq}, {mask}",
            "vpxor {yov}, {yov}, {s42}",
            "shr {meq}, 12",
            "and {meq:e}, 0x7fff0",
            "vinserti128 {s1}, {s1}, xmmword ptr [{tab} + {meq}], 1",
            "popcnt {t1:x}, {mask:x}",
            "movzx {t1:e}, {t1:x}",
            "vpshufb {s1}, {s2}, {s1}",
            "vmovdqu xmmword ptr [{out}], {s1:x}",
            "sub {out}, {t1}",
            "mov {meq:e}, {mask:e}",
            "xor {meq:x}, {meq:x}",
            "vextracti128 xmmword ptr [{out} + 16], {s1}, 1",
            "popcnt {meq:e}, {meq:e}",
            "sub {out}, {meq}",
            "mov {meq}, {mask}",
            "shr {meq}, 28",
            "mov {t1}, {meq}",
            "and {meq:e}, 0xffff0",
            "and {t1:e}, 0x7fff0",
            "popcnt {meq:e}, {meq:e}",
            "vmovdqu {s1:x}, xmmword ptr [{tab} + {t1}]",
            "mov {t1}, {mask}",
            "shr {mask}, 48",
            "shr {t1}, 44",
            "popcnt {mask:e}, {mask:e}",
            "and {t1:e}, 0x7fff0",
            "vinserti128 {s1}, {s1}, xmmword ptr [{tab} + {t1}], 1",
            "vpshufb {s1}, {s0}, {s1}",
            "vmovdqu xmmword ptr [{out} + 32], {s1:x}",
            "sub {out}, {meq}",
            "vextracti128 xmmword ptr [{out} + 48], {s1}, 1",
            "sub {out}, {mask}",
            "mov {mask}, {ne}",
            "add {out}, 64",
            "add {mask}, {c}",
            "jz 30f",
            // ---- loop head: aligned lane loads, one specials probe -------
            "20:",
            "vmovdqa {hi}, ymmword ptr [{c}]",
            "vmovdqa {lo}, ymmword ptr [{c} - 32]",
            "vpminub {s1}, {hi}, {dot}",
            "vpminub {s0}, {mmv}, {lo}",
            "vpshufb {s1}, {lut}, {s1}",
            "vpshufb {s0}, {lut}, {s0}",
            "vpcmpeqb {s1}, {s1}, {hi}",
            "vpcmpeqb {s0}, {s0}, {lo}",
            "vpmovmskb {meq:e}, {s1}",
            "vpmovmskb {mask:e}, {s0}",
            "shl {meq}, 32",
            "or {mask}, {meq}",
            "jnz 21b",
            // clean window: the head's fallthrough. First iteration decodes
            // with the carried `yov` (straddled-escape byte 0) and resets the
            // carry, then falls into the pipelined clean streak.
            "vpaddb {s1}, {yov}, {lo}",
            "vpaddb {s0}, {hi}, {s42}",
            "vmovdqa {yov}, {s42}",
            "xor {ef:e}, {ef:e}",
            "jmp 42f",
            // ---- pipelined clean streak: decode N, then load N+1 BEFORE
            // storing N (the July 2-window result: breaking the serial
            // load->store->load chain is worth ~+15% on pure-clean content;
            // here it costs the heavy path nothing — specials exit to 21).
            // In-streak invariants: ef == 0, yov == sub42, mmv == dot (a
            // pending dot forces the specials path, so a clean window can
            // never carry one).
            ".p2align 4",
            "41:",
            "vpaddb {s1}, {lo}, {s42}",
            "vpaddb {s0}, {hi}, {s42}",
            "42:",
            // speculative next-window loads: the span keeps a 67-byte tail
            // reserve, so reading one window past the last is in bounds.
            "vmovdqa {hi}, ymmword ptr [{c} + 64]",
            "vmovdqa {lo}, ymmword ptr [{c} + 32]",
            "vmovdqu ymmword ptr [{out}], {s1}",
            "vmovdqu ymmword ptr [{out} + 32], {s0}",
            "add {c}, 64",
            "add {out}, 64",
            "mov {mask}, {ne}",
            "add {mask}, {c}",
            "jz 30f",
            "vpminub {s1}, {hi}, {dot}",
            "vpminub {s0}, {mmv}, {lo}",
            "vpshufb {s1}, {lut}, {s1}",
            "vpshufb {s0}, {lut}, {s0}",
            "vpcmpeqb {s1}, {s1}, {hi}",
            "vpcmpeqb {s0}, {s0}, {lo}",
            "vpmovmskb {meq:e}, {s1}",
            "vpmovmskb {mask:e}, {s0}",
            "shl {meq}, 32",
            "or {mask}, {meq}",
            "jz 41b",
            "jmp 21b",
            // ---- CR/LF present: remat CR, `.`-at-+2 probe (oracle 6c358) -
            ".p2align 4",
            "23:",
            "mov {t1:e}, 0x0d0d0d0d",
            "vmovd {s0:x}, {t1:e}",
            "vpbroadcastd {s0}, {s0:x}",
            "vpcmpeqb {s1}, {dot}, ymmword ptr [{c} - 30]",
            "vpcmpeqb {mmv}, {lo}, {s0}",
            "vpcmpeqb {s3}, {hi}, {s0}",
            "vpand {s1}, {s1}, {mmv}",
            "vpcmpeqb {mmv}, {dot}, ymmword ptr [{c} + 2]",
            "vpand {mmv}, {s3}, {mmv}",
            "vpor {s4}, {s1}, {mmv}",
            "vpmovmskb {t1:e}, {s4}",
            "test {t1:e}, {t1:e}",
            "jnz 25f",
            "vmovdqa {mmv}, {dot}",
            "jmp 22b",
            // ---- stuffed dot: merge `\r\n.`, clamp min_mask (6c428) ------
            "25:",
            "mov {t1:e}, 0x0a0a0a0a",
            "vmovdqa {s4}, ymmword ptr [{c} - 32]",
            "vmovdqa {s5}, ymmword ptr [{c}]",
            "vmovd {s3:x}, {t1:e}",
            "vpbroadcastd {s3}, {s3:x}",
            "vpcmpeqb {s5}, {s5}, {s0}",
            "vpcmpeqb {s4}, {s4}, {s0}",
            "vpcmpeqb {s0}, {s3}, ymmword ptr [{c} + 1]",
            "vpand {s5}, {s5}, {s0}",
            "vpcmpeqb {s3}, {s3}, ymmword ptr [{c} - 31]",
            "vpand {s4}, {s4}, {s1}",
            "vpand {mmv}, {mmv}, {s5}",
            "vpand {s4}, {s4}, {s3}",
            "vpmovmskb {t2:e}, {mmv}",
            "vpmovmskb {t1:e}, {s4}",
            "shl {t2}, 34",
            "shl {t1}, 2",
            "or {t1}, {t2}",
            "or {mask}, {t1}",
            "vextracti128 {s0:x}, {mmv}, 1",
            "vpsrldq {s0:x}, {s0:x}, 14",
            "vpsubusb {mmv}, {dot}, {s0}",
            "jmp 22b",
            // ---- escaped == 0: plain adds (weaver's shortcut) ------------
            "28:",
            "vpaddb {s2}, {lo}, {yov}",
            "vpaddb {s0}, {hi}, {s42}",
            "xor {ef:e}, {ef:e}",
            "jmp 24b",
            // ---- consecutive-`=` collision (oracle 6c498) ----------------
            "26:",
            "not {t1}",
            "and {t1}, {meq}",
            "mov {t2}, {t1}",
            "movabs {t1}, 0x5555555555555555",
            "and {t2}, {t1}",
            "add {t2}, {meq}",
            "xor {t1}, {t2}",
            "and {meq}, {t1}",
            "lea {t1}, [{meq} + {meq}]",
            "vmovq {s0:x}, {t1}",
            "or {ef}, {t1}",
            "vpbroadcastq {s0}, {s0:x}",
            "not {ef}",
            "vpshufb {s2}, {s0}, ymmword ptr [rip + {ia}]",
            "and {mask}, {ef}",
            "mov {ef}, {meq}",
            "vpshufb {s0}, {s0}, ymmword ptr [rip + {ib}]",
            "vpbroadcastq {s3}, qword ptr [rip + {bl}]",
            "shr {ef}, 63",
            "vpand {s2}, {s2}, {s3}",
            "vpand {s0}, {s0}, {s3}",
            "vpcmpeqb {s2}, {s2}, {s3}",
            "vpblendvb {s2}, {yov}, {eof}, {s2}",
            "vpaddb {s2}, {lo}, {s2}",
            "vpcmpeqb {s0}, {s0}, {s3}",
            "vpblendvb {s0}, {s42}, {eof}, {s0}",
            "vpaddb {s0}, {hi}, {s0}",
            "jmp 24b",
            "30:",
            c = inout(reg) c_v => _,
            ne = in(reg) negend,
            out = inout(reg) out_v,
            ef = inout(reg) ef_v,
            tab = in(reg) table,
            mask = out(reg) _,
            meq = out(reg) _,
            t1 = out(reg) _,
            t2 = out(reg) _,
            lut = in(ymm_reg) special_lut,
            dot = in(ymm_reg) dot,
            s42 = in(ymm_reg) sub42,
            eqn = in(ymm_reg) eq_needle,
            eof = in(ymm_reg) esc_off,
            yov = inout(ymm_reg) yenc_offset => _,
            mmv = inout(ymm_reg) min_mask => _,
            hi = out(ymm_reg) _,
            lo = out(ymm_reg) _,
            s0 = out(ymm_reg) _,
            s1 = out(ymm_reg) _,
            s2 = out(ymm_reg) _,
            s3 = out(ymm_reg) _,
            s4 = out(ymm_reg) _,
            s5 = out(ymm_reg) _,
            ia = sym AVX2_ESC_IDX_A,
            ib = sym AVX2_ESC_IDX_B,
            bl = sym AVX2_ESC_BIT_LANES,
            options(nostack),
        );

        esc_first = ef_v;
        dst = out_v.offset_from(output.as_mut_ptr()) as usize;
        src += span;

        // Exit state from the trailing bytes — identical to the generic
        // kernel's lookback; runs only when the SIMD span consumed windows.
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

/// r10: the `SEARCH_END = true` span loop as one `asm!` block — a
/// transliteration of WEAVER'S OWN emission of this loop from the build
/// whose fused-lane timing measured well on BOTH Alder Lake and Zen2
/// (archived as `yenc-program/setrue-adl.txt`). Unlike the SE=false kernel
/// (which copies the oracle, since the oracle was faster there), weaver's
/// fused searchEnd path BEATS the oracle's — the problem was only that its
/// register allocation re-rolled every build (±15% swings on the fused
/// clean/dots lanes). Freezing the good roll ends that permanently.
///
/// Deviations from the source emission, each strictly smaller:
/// - its two ymm stack spills around the dot arm (`yenc_offset`, `eq_va`)
///   become rematerializations (3 ops from `esc_first` / 1 compare from
///   the lane) — `options(nostack)` requires it and remat is cheaper;
/// - its two loop-invariant stack reloads (the `sub42` xor-base and the
///   alignr fill) come from the pinned `{s42}`/`{eqn}` registers;
/// - its rodata constant-restore storms are mirrored via this module's
///   `sym` statics (byte-broadcast sources + the 32-byte specials LUT).
///
/// Break protocol (the terminator probe hit an end candidate): the block
/// exits with `i != 0` (the window UNCONSUMED), `{mask}` holding the
/// pre-merge specials mask and `{ef}` the pre-window carry — exactly the
/// values the existing Rust `x86_break_state` glue consumes. `i == 0`
/// means the span ran to completion.
///
/// Safety: same contract as the Rust span loop it replaces (the caller's
/// span keeps the 67-byte tail reserve; the deepest view reads
/// `c24 + 0`, i.e. `window + 68 - 32`… all views are the Rust loop's own
/// offsets). Flags clobbered; reads input + LUT, writes output; no stack.
#[cfg(target_arch = "x86_64")]
#[cfg_attr(not(weaver_yenc_raw_asm), allow(dead_code))]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(clippy::too_many_arguments)]
unsafe fn avx2_raw_span_setrue_asm(
    input_base: *const u8,
    i: &mut isize,
    out: &mut *mut u8,
    esc_first: &mut u64,
    break_mask: &mut u64,
    min_mask: std::arch::x86_64::__m256i,
    yenc_offset: std::arch::x86_64::__m256i,
) {
    use std::arch::x86_64::*;

    let sub42 = _mm256_set1_epi8(42i8.wrapping_neg());
    let dot = _mm256_set1_epi8(b'.' as i8);
    let eq_needle = _mm256_set1_epi8(b'=' as i8);
    let cr = _mm256_set1_epi8(b'\r' as i8);
    let special_lut = _mm256_load_si256(AVX2_SPECIAL_LUT.0.as_ptr() as *const __m256i);
    let table = compact_table_16().as_ptr() as *const u8;

    let c24 = input_base.add(0x24);
    let mut i_v = *i;
    let mut out_v = *out;
    let mut ef_v = *esc_first;
    let mut mask_v: u64;

    core::arch::asm!(
        "jmp 20f",
        // ---- escaped == 0: plain adds (falls into the store) ------------
        ".p2align 4",
        "18:",
        "vpaddb {s6}, {yov}, {la}",
        "vpaddb {la}, {s42}, {lb}",
        "xor {esc:e}, {esc:e}",
        // ---- store: skip via andn into the ef reg, yov rebuild ----------
        "19:",
        "andn {ef}, {esc}, {mask}",
        "vmovd {s0:x}, {efn:e}",
        "vpsllw {s0:x}, {s0:x}, 6",
        "vpxor {yov}, {s0}, {s42}",
        "mov {mask:e}, {ef:e}",
        "shl {mask:e}, 4",
        "and {mask:e}, 0x7fff0",
        "vmovdqu {s0:x}, xmmword ptr [{tab} + {mask}]",
        "mov {mask:e}, {ef:e}",
        "shr {mask:e}, 12",
        "and {mask:e}, 0x7fff0",
        "vinserti128 {s0}, {s0}, xmmword ptr [{tab} + {mask}], 1",
        "vpshufb {s0}, {s6}, {s0}",
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
        "mov {esc:e}, {mask:e}",
        "and {esc:e}, 0x7fff0",
        "vmovdqu {s0:x}, xmmword ptr [{tab} + {esc}]",
        "mov {esc}, {ef}",
        "shr {esc}, 44",
        "and {esc:e}, 0x7fff0",
        "vinserti128 {s0}, {s0}, xmmword ptr [{tab} + {esc}], 1",
        "vpshufb {s0}, {la}, {s0}",
        "vmovdqu xmmword ptr [{out} + 32], {s0:x}",
        "and {mask:e}, 0xffff0",
        "popcnt {mask:e}, {mask:e}",
        "sub {out}, {mask}",
        "vextracti128 xmmword ptr [{out} + 48], {s0}, 1",
        "shr {ef}, 48",
        "popcnt {ef:e}, {ef:e}",
        "sub {out}, {ef}",
        "mov {ef}, {efn}",
        "add {out}, 64",
        "add {c}, 64",
        "add {i}, 64",
        "jz 30f",
        // ---- loop head --------------------------------------------------
        "20:",
        "vmovdqu {la}, ymmword ptr [{c} - 36]",
        "vmovdqu {lb}, ymmword ptr [{c} - 4]",
        "vpminub {s6}, {mmv}, {la}",
        "vpshufb {s6}, {lut}, {s6}",
        "vpminub {s7}, {dot}, {lb}",
        "vpshufb {s7}, {lut}, {s7}",
        "vpcmpeqb {s7}, {s7}, {lb}",
        "vpmovmskb {mask:e}, {s7}",
        "shl {mask}, 32",
        "vpcmpeqb {s6}, {s6}, {la}",
        "vpmovmskb {esc:e}, {s6}",
        "or {mask}, {esc}",
        "jz 29f",
        // ---- specials: eq masks ----------------------------------------
        "vpcmpeqb {eqa}, {eqn}, {la}",
        "vpcmpeqb {s6}, {eqn}, {lb}",
        "vpmovmskb {efn:e}, {s6}",
        "mov {t}, {efn}",
        "shl {t}, 32",
        "vpmovmskb {meq:e}, {eqa}",
        "or {meq}, {t}",
        "cmp {mask}, {meq}",
        "jne 23f",
        // eq-only: reset min_mask, collision test, fall into the join
        "vmovdqa {mmv}, {dot}",
        "lea {esc}, [{ef} + {meq}*2]",
        "test {meq}, {esc}",
        "jnz 27f",
        // ---- join -------------------------------------------------------
        "22:",
        "shr {efn:e}, 31",
        "test {esc}, {esc}",
        "jz 18b",
        // isolated escapes
        "vinserti128 {s0}, {eqn}, {eqa:x}, 1",
        "vpalignr {s0}, {eqa}, {s0}, 15",
        "vpcmpeqb {s1}, {eqn}, ymmword ptr [{c} - 5]",
        "vpbroadcastb {s7}, byte ptr [rip + {esc_b}]",
        "vpblendvb {s0}, {yov}, {s7}, {s0}",
        "vpaddb {s6}, {s0}, {la}",
        "vpblendvb {s0}, {s42}, {s7}, {s1}",
        "vpaddb {la}, {s0}, {lb}",
        "jmp 19b",
        // ---- clean window ----------------------------------------------
        ".p2align 4",
        "29:",
        "vpaddb {s0}, {yov}, {la}",
        "vmovdqu ymmword ptr [{out}], {s0}",
        "vpaddb {s0}, {s42}, {lb}",
        "vmovdqu ymmword ptr [{out} + 32], {s0}",
        "vmovdqa {yov}, {s42}",
        "xor {ef:e}, {ef:e}",
        "add {out}, 64",
        "add {c}, 64",
        "add {i}, 64",
        "jnz 20b",
        "jmp 30f",
        // ---- CR/LF present ---------------------------------------------
        "23:",
        "vmovdqu {s12}, ymmword ptr [{c} - 34]",
        "vmovdqu {s7}, ymmword ptr [{c} - 2]",
        "vmovdqa {s0}, {eqn}",
        "vpcmpeqb {eqn}, {crv}, {la}",
        "vpcmpeqb {s6}, {dot}, {s12}",
        "vpand {s6}, {s6}, {eqn}",
        "vpcmpeqb {mmv}, {crv}, {lb}",
        "vmovdqa {s2}, {crv}",
        "vpcmpeqb {crv}, {dot}, {s7}",
        "vpand {crv}, {crv}, {mmv}",
        "vpor {s42}, {s6}, {crv}",
        "vpmovmskb {t:e}, {s42}",
        "vpcmpeqb {s7}, {s0}, {s7}",
        "vpcmpeqb {s12}, {s0}, {s12}",
        "test {t:e}, {t:e}",
        "je 28f",
        // ---- stuffed dot + full terminator probe ------------------------
        "25:",
        "vmovdqu {s42}, ymmword ptr [{c} - 33]",
        "vmovdqu {dot}, ymmword ptr [{c} - 32]",
        "vmovdqu {s1}, ymmword ptr [{c} - 1]",
        "vmovdqu {yov}, ymmword ptr [{c}]",
        "vpcmpeqb {lut}, {s2}, {s42}",
        "vpcmpeqb {s2}, {s2}, {s1}",
        "vpbroadcastb {eqa}, byte ptr [rip + {lf_b}]",
        "vpcmpeqb {s0}, {dot}, {eqa}",
        "vpand {s0}, {s0}, {lut}",
        "vpcmpeqb {lut}, {yov}, {eqa}",
        "vpand {s2}, {s2}, {lut}",
        "vpbroadcastb {eqa}, byte ptr [rip + {y_b}]",
        "vpcmpeqb {lut}, {s42}, {eqa}",
        "vpand {lut}, {lut}, {s12}",
        "vpcmpeqb {s1}, {s1}, {eqa}",
        "vpand {s1}, {s1}, {s7}",
        "vpbroadcastb {s7}, byte ptr [rip + {lf_b}]",
        "vpcmpeqb {s7}, {s7}, ymmword ptr [{c} - 35]",
        "vpand {s42}, {s7}, {eqn}",
        "vpbroadcastw {eqa}, word ptr [rip + {eqy_w}]",
        "vpcmpeqw {dot}, {dot}, {eqa}",
        "vpsllw {dot}, {dot}, 8",
        "vpor {s0}, {s0}, {dot}",
        "vpsrlw {dot}, {lut}, 8",
        "vpor {s0}, {s0}, {dot}",
        "vpand {dot}, {s42}, {s6}",
        "vpand {s0}, {s0}, {dot}",
        "vpcmpeqw {dot}, {yov}, {eqa}",
        "vpsllw {dot}, {dot}, 8",
        "vpor {s2}, {s2}, {dot}",
        "vpsrlw {dot}, {s1}, 8",
        "vpor {s2}, {s2}, {dot}",
        "vpand {lut}, {lut}, {s42}",
        "vpbroadcastb {dot}, byte ptr [rip + {lf_b}]",
        "vpcmpeqb {s12}, {dot}, ymmword ptr [{c} - 3]",
        "vpand {dot}, {s12}, {mmv}",
        "vpand {s1}, {s1}, {dot}",
        "vpor {s1}, {s1}, {lut}",
        "vpor {s0}, {s0}, {s1}",
        "vpand {eqn}, {dot}, {crv}",
        "vpand {s1}, {s2}, {eqn}",
        "vpor {s0}, {s0}, {s1}",
        "vpmovmskb {t:e}, {s0}",
        "test {t:e}, {t:e}",
        "jnz 60f",
        "vpand {s0}, {s7}, {s6}",
        "vpmovmskb {esc:e}, {s0}",
        "vpand {s0}, {s12}, {crv}",
        "vpmovmskb {t:e}, {s0}",
        "shl {t}, 34",
        "lea {esc}, [{t} + {esc}*4]",
        "or {mask}, {esc}",
        "vextracti128 {s0:x}, {eqn}, 1",
        "vpsrldq {s0:x}, {s0:x}, 14",
        "vpbroadcastb {dot}, byte ptr [rip + {dot_b}]",
        "vpsubusb {mmv}, {dot}, {s0}",
        // constant restore (the roll's storm, via statics) + remats
        "vmovdqa {lut}, ymmword ptr [rip + {lut_s}]",
        "vpbroadcastb {s42}, byte ptr [rip + {sub_b}]",
        "vpbroadcastb {eqn}, byte ptr [rip + {eq_b}]",
        "vpbroadcastb {crv}, byte ptr [rip + {cr_b}]",
        "vmovd {yov:x}, {ef:e}",
        "vpsllw {yov:x}, {yov:x}, 6",
        "vpxor {yov}, {yov}, {s42}",
        "vpcmpeqb {eqa}, {eqn}, {la}",
        "lea {esc}, [{ef} + {meq}*2]",
        "test {meq}, {esc}",
        "jnz 27f",
        "jmp 22b",
        // ---- CRLF but no stuffed dot: bare =y probe ---------------------
        "28:",
        "vpbroadcastb {s1}, byte ptr [rip + {y_b}]",
        "vpcmpeqb {s0}, {s1}, ymmword ptr [{c} - 33]",
        "vpand {s6}, {s0}, {s12}",
        "vpcmpeqb {s0}, {s1}, ymmword ptr [{c} - 1]",
        "vpand {s7}, {s0}, {s7}",
        "vpor {s0}, {s7}, {s6}",
        "vpmovmskb {t:e}, {s0}",
        "test {t:e}, {t:e}",
        "je 41f",
        "vpbroadcastb {s1}, byte ptr [rip + {lf_b}]",
        "vpcmpeqb {s0}, {s1}, ymmword ptr [{c} - 35]",
        "vpand {s0}, {s0}, {eqn}",
        "vpand {s0}, {s0}, {s6}",
        "vpcmpeqb {s1}, {s1}, ymmword ptr [{c} - 3]",
        "vpand {s1}, {s1}, {mmv}",
        "vpand {s1}, {s1}, {s7}",
        "vpor {s0}, {s0}, {s1}",
        "vpmovmskb {t:e}, {s0}",
        "vmovdqa {mmv}, {dot}",
        "vpbroadcastb {s42}, byte ptr [rip + {sub_b}]",
        "vpbroadcastb {eqn}, byte ptr [rip + {eq_b}]",
        "vpbroadcastb {crv}, byte ptr [rip + {cr_b}]",
        "vmovdqa {lut}, ymmword ptr [rip + {lut_s}]",
        "vmovd {yov:x}, {ef:e}",
        "vpsllw {yov:x}, {yov:x}, 6",
        "vpxor {yov}, {yov}, {s42}",
        "vpcmpeqb {eqa}, {eqn}, {la}",
        "test {t:e}, {t:e}",
        "jnz 60f",
        "lea {esc}, [{ef} + {meq}*2]",
        "test {meq}, {esc}",
        "je 22b",
        "jmp 27f",
        "41:",
        "vmovdqa {mmv}, {dot}",
        "vpbroadcastb {s42}, byte ptr [rip + {sub_b}]",
        "vpbroadcastb {eqn}, byte ptr [rip + {eq_b}]",
        "vpbroadcastb {crv}, byte ptr [rip + {cr_b}]",
        "vmovdqa {lut}, ymmword ptr [rip + {lut_s}]",
        "vmovd {yov:x}, {ef:e}",
        "vpsllw {yov:x}, {yov:x}, 6",
        "vpxor {yov}, {yov}, {s42}",
        "vpcmpeqb {eqa}, {eqn}, {la}",
        "lea {esc}, [{ef} + {meq}*2]",
        "test {meq}, {esc}",
        "jnz 27f",
        "jmp 22b",
        // ---- consecutive-`=` collision ---------------------------------
        "27:",
        "mov {efn}, {meq}",
        "and {efn}, {fives}",
        "andn {efn}, {esc}, {efn}",
        "add {efn}, {meq}",
        "xor {efn}, {fives}",
        "and {meq}, {efn}",
        "lea {esc}, [{meq} + {meq}]",
        "mov {efn}, {meq}",
        "shr {efn}, 63",
        "add {esc}, {ef}",
        "jz 18b",
        "vmovq {s0:x}, {esc}",
        "vpermq {s0}, {s0}, 0x44",
        "vpshufb {s1}, {s0}, ymmword ptr [rip + {ia}]",
        "vpbroadcastq {s2}, qword ptr [rip + {bl}]",
        "vpshufb {s0}, {s0}, ymmword ptr [rip + {ib}]",
        "vpand {s1}, {s1}, {s2}",
        "vpand {s0}, {s0}, {s2}",
        "vpcmpeqb {s1}, {s1}, {s2}",
        "vpbroadcastb {s7}, byte ptr [rip + {esc_b}]",
        "vpblendvb {s1}, {s42}, {s7}, {s1}",
        "vpaddb {s6}, {s1}, {la}",
        "vpcmpeqb {s0}, {s0}, {s2}",
        "vpblendvb {s0}, {s42}, {s7}, {s0}",
        "vpaddb {la}, {s0}, {lb}",
        "jmp 19b",
        // ---- terminator hit: break with the window unconsumed ----------
        "60:",
        "30:",
        c = inout(reg) c24 => _,
        i = inout(reg) i_v,
        out = inout(reg) out_v,
        ef = inout(reg) ef_v,
        efn = out(reg) _,
        tab = in(reg) table,
        fives = in(reg) 0x5555_5555_5555_5555u64,
        mask = out(reg) mask_v,
        meq = out(reg) _,
        esc = out(reg) _,
        t = out(reg) _,
        lut = inout(ymm_reg) special_lut => _,
        dot = inout(ymm_reg) dot => _,
        s42 = inout(ymm_reg) sub42 => _,
        eqn = inout(ymm_reg) eq_needle => _,
        crv = inout(ymm_reg) cr => _,
        mmv = inout(ymm_reg) min_mask => _,
        yov = inout(ymm_reg) yenc_offset => _,
        eqa = out(ymm_reg) _,
        la = out(ymm_reg) _,
        lb = out(ymm_reg) _,
        s0 = out(ymm_reg) _,
        s1 = out(ymm_reg) _,
        s2 = out(ymm_reg) _,
        s6 = out(ymm_reg) _,
        s7 = out(ymm_reg) _,
        s12 = out(ymm_reg) _,
        ia = sym AVX2_ESC_IDX_A,
        ib = sym AVX2_ESC_IDX_B,
        bl = sym AVX2_ESC_BIT_LANES,
        lut_s = sym AVX2_SPECIAL_LUT,
        dot_b = sym YB_DOT,
        eq_b = sym YB_EQ,
        cr_b = sym YB_CR,
        lf_b = sym YB_LF,
        sub_b = sym YB_SUB42,
        esc_b = sym YB_ESC,
        y_b = sym YB_Y,
        eqy_w = sym YW_EQY,
        options(nostack),
    );

    *i = i_v;
    *out = out_v;
    *esc_first = ef_v;
    *break_mask = mask_v;
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
