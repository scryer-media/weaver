// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The asm loop's escape select for this CPU, chosen once per process. Intel
// (family 6) takes the and-add form: `vpblendvb ymm` is two uops there from
// Haswell on and three on Golden Cove, against two for `vpand` + `vpaddb`;
// measured 1.5-5% faster on Sapphire Rapids. AMD keeps rapidyenc's
// `vpblendvb`, one uop on Zen 2 to Zen 4; the and-add form measured 1-2%
// slower on Zen 4.
fn avx2_and_add_select() -> bool {
    #[cfg(any(fuzzing, test))]
    if let Some(on) = super::fuzzing::and_add_select_override() {
        return on;
    }
    static PICK: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *PICK.get_or_init(|| x86_cpu_family_model().0 == 6)
}

// The window loop prefetches this far ahead of the cursor, on spans of at
// least PREFETCH_MIN_SPAN bytes.
const PREFETCH_AHEAD: usize = 2048;
const PREFETCH_MIN_SPAN: usize = 16 * PREFETCH_AHEAD;

// Maintenance contract: this Rust kernel and the intrinsic path selected by
// `WEAVER_YENC_RAW_ASM=0` are the tunable source of truth behind the frozen
// `asm!` kernel. For future tuning or bug fixes, change this implementation,
// validate and measure it through the `=0` escape hatch, and only then update
// `avx2_raw_kernel_oracle` (both instantiations) from the winning emission.
// The oracle differential suite detects semantic drift between the Rust and
// assembly implementations.
//
// Faithful port of rapidyenc `do_decode_avx2` (decoder_avx2_base.h), the
// `isRaw=true, searchEnd=false` instantiation — the realshape decode path.
// 1:1 translation of the oracle's HOT LOOP: decoder state lives entirely in
// registers (`esc_first`/`yenc_offset`/`min_mask`/`next_mask`, exactly the
// oracle's `escFirst`/`yencOffset`/`minMask`/`nextMask`); `\r\n.` dot-stuffing
// is stripped IN-LOOP via `min_mask` + a `mask` merge (never a scalar bail);
// no per-window enum dispatch, no `span_end_state` trailing-byte read. The
// per-window decode math (escape unescape, 2-lane LUT compaction, `fix_eq_mask`)
// reuses weaver's existing byte-exact helpers (already identical to the oracle).
// This removes the ~47 µops/window of weaver-specific scaffolding.
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

    // Both instantiations run the oracle-model asm kernel (aligned
    // single-cursor loop transliterated from rapidyenc's own emission on the
    // measurement host — see avx2_raw_kernel_oracle); the body below is the
    // `WEAVER_YENC_RAW_ASM=0` build's kernel.
    if avx2_raw_asm() {
        return if avx2_and_add_select() {
            avx2_raw_kernel_oracle::<SEARCH_END, true>(input, output, state, mode, input.len())
        } else {
            avx2_raw_kernel_oracle::<SEARCH_END, false>(input, output, state, mode, input.len())
        };
    }

    let mut src = 0usize;
    // Output cursor is a running pointer, exactly the oracle's `p`. Keeping a
    // `(base, offset)` pair instead cost a reload of the spilled base on every
    // window plus a separate offset register; `dst` is materialised once, after
    // the SIMD span, for the scalar epilogue.
    let out_base = output.as_mut_ptr();
    let mut out = out_base;
    // The widest lookahead with the end search is the lane-B `+4` view,
    // ending at `src + WIDTH + 3`; without the end search the
    // deepest is the `+2` view, ending at `src + WIDTH + 1`. The loop bound
    // (`src + WIDTH <= len - tail`) keeps exactly that, as rapidyenc's
    // rounded-up `dLen` does (decoder_common.h:130-131).
    let tail = if SEARCH_END { 4 } else { 2 };
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
    let dst = out.offset_from(out_base) as usize;

    // Only re-derive the carried state when the SIMD loop actually consumed at
    // least one window. With no window consumed (len in {129,130} => simd_limit
    // < WIDTH), `src` is still 0 and the entry state MUST survive untouched for
    // the scalar epilogue — otherwise a carried Cr/CrLf line-start (with a
    // pending stuffed dot) would be clobbered to None and mis-decoded.
    // A SEARCH_END break already set the state from the no-backtrack rule over
    // the unconsumed window, so the (backtracking) lookback must not run.
    if !broke && src > 0 {
        // The scalar machine's state after the span (at least one window
        // consumed, so `src >= 64`): `Cr`/`CrLf` before any byte, and a
        // line-start `=` carries `CrLfEq`, as `avx2_exit_state` reads them.
        state.state = avx2_exit_state(input, src, esc_first != 0);
    }

    // The tail (decoder_common.h:145-147).
    avx2_tail(input, output, state, mode, src, dst, input.len())
}

// One step of the scalar machine for the heads and tails, with the common
// bytes inline: a plain byte, CR or LF in the `None` state, `=` with its
// escaped byte in reach, the LF of a CR LF, and a plain byte at a line
// start. These are the cases rapidyenc's scalar loop takes inline
// (decoder.cc:52-70); each is exactly what `decode_scalar_step` does with
// that byte and state, and none of them but `=` reads past its byte, so they
// run up to the last input byte. Every other case, and a full output, goes
// to `decode_scalar_step`.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn avx2_tail_step(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<bool, YencError> {
    let s = *src;
    let d = *dst;
    if s < input.len() && d < output.len() {
        let byte = input[s];
        match state.state {
            DecoderState::None => match byte {
                b'\r' => {
                    *src = s + 1;
                    state.state = DecoderState::Cr;
                    return Ok(true);
                }
                b'\n' => {
                    *src = s + 1;
                    return Ok(true);
                }
                b'=' if s + 1 < input.len() => {
                    let escaped = input[s + 1];
                    output[d] = escaped.wrapping_sub(106);
                    *src = s + 2;
                    *dst = d + 1;
                    if mode.dot_unstuffing && escaped == b'\r' {
                        state.state = DecoderState::Cr;
                    }
                    return Ok(true);
                }
                b'=' => {}
                _ => {
                    output[d] = byte.wrapping_sub(42);
                    *src = s + 1;
                    *dst = d + 1;
                    return Ok(true);
                }
            },
            DecoderState::Cr if byte == b'\n' => {
                *src = s + 1;
                state.state = DecoderState::CrLf;
                return Ok(true);
            }
            DecoderState::CrLf
                if !matches!(byte, b'=' | b'\r' | b'\n')
                    && !(mode.dot_unstuffing && byte == b'.') =>
            {
                output[d] = byte.wrapping_sub(42);
                *src = s + 1;
                *dst = d + 1;
                state.state = DecoderState::None;
                return Ok(true);
            }
            _ => {}
        }
    }
    decode_scalar_step(input, src, output, dst, state, mode)
}

// The scalar machine's state at `at` (the cursor after a window), raw
// dot-unstuffing, read off the bytes before it: `CrLfEq` or `Eq` when the
// window's last `=` escapes the byte at `at` (a line-start `=` is `CrLfEq`),
// `Cr` after a CR (an escaped CR also arms it), `CrLf` after CR LF, else
// `None`. Whatever byte follows: a later call may stop at a line-start `=y`.
// Needs `2 <= at`.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn avx2_exit_state(input: &[u8], at: usize, esc_out: bool) -> DecoderState {
    if esc_out {
        if at >= 3 && input[at - 3] == b'\r' && input[at - 2] == b'\n' {
            DecoderState::CrLfEq
        } else {
            DecoderState::Eq
        }
    } else if input[at - 1] == b'\r' {
        DecoderState::Cr
    } else if input[at - 2] == b'\r' && input[at - 1] == b'\n' {
        DecoderState::CrLf
    } else {
        DecoderState::None
    }
}

// One window over the bytes `input[w + o..w + e]` (`o < e <= 64`), entered in
// `entry` (`None`, `Eq`, `Cr` or `CrLf`): the span's head and tail, which
// rapidyenc decodes byte by byte (decoder_common.h:40-47, 145-147), decoded
// from one unaligned 64-byte load at `w` with the bytes outside the range
// dropped. Writes the decoded bytes at `output[dst..]` and returns their
// count and whether the range ends on an unescaped `=` (the next byte is
// escaped); the caller takes the exit state from `avx2_exit_state`.
//
// Each byte decodes as the scalar machine decodes it, read off the bytes
// alone (raw dot-unstuffing):
// - escapes: an `=` escapes the next byte unless it is escaped itself
//   (`fix_eq_mask`); `Eq` escapes the first byte. An escaped byte is
//   `byte - 106` and always kept.
// - an unescaped `=`, CR or LF is dropped.
// - a `.` is dropped exactly when the two bytes before it are CR LF (an
//   escaped CR counts, since `Eq` then CR leaves `Cr`; the LF cannot be
//   escaped, as the byte before it is the CR). Before the range those two
//   bytes are `entry`'s: `CrLf` stands for CR LF, `Cr` for CR.
// - everything else is `byte - 42`.
// With the end search, a line start followed by `=` or `.` with its CR in
// the range or one byte after it (`\r\n=y`, `\r\n.=y`, `\r\n.\r\n` and the
// shapes `x86_search_end_head` resolves), or an entry at a line start, is
// left to the scalar machine: the function returns `None` and writes
// nothing. Needs `w + 66 <= input.len()` and `dst <= w + o`.
#[cfg(target_arch = "x86_64")]
// Inlined into its AVX2 callers (no `target_feature` of its own, so it may
// be `inline(always)`); called only from AVX2 code.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn, clippy::too_many_arguments)]
unsafe fn avx2_edge_window(
    input: &[u8],
    w: usize,
    o: u32,
    e: u32,
    entry: DecoderState,
    search_end: bool,
    output: &mut [u8],
    dst: usize,
) -> Option<(usize, bool)> {
    use std::arch::x86_64::*;
    debug_assert!(o < e && e <= 64 && w + 66 <= input.len() && dst <= w + o as usize);
    debug_assert!(matches!(
        entry,
        DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
    ));
    let p = input.as_ptr().add(w);
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let range = (u64::MAX << o) & (u64::MAX >> (64 - e));
    let cr_w = avx2_mask64(a, b, b'\r');
    let lf_w = avx2_mask64(a, b, b'\n');
    let eq_w = avx2_mask64(a, b, b'=');
    let dot_w = avx2_mask64(a, b, b'.');
    if search_end {
        if matches!(entry, DecoderState::Cr | DecoderState::CrLf) {
            return None;
        }
        let at64 = *input.get_unchecked(w + 64);
        let at65 = *input.get_unchecked(w + 65);
        let lf_x = lf_w as u128 | ((at64 == b'\n') as u128) << 64;
        let ed_x = (eq_w | dot_w) as u128
            | (matches!(at64, b'=' | b'.') as u128) << 64
            | (matches!(at65, b'=' | b'.') as u128) << 65;
        if (cr_w & range) as u128 & (lf_x >> 1) & (ed_x >> 2) != 0 {
            return None;
        }
    }
    let esc_in = (entry == DecoderState::Eq) as u64;
    let eq = eq_w & range & !(esc_in << o);
    let unescaped = fix_eq_mask(eq, eq << 1);
    let escaped = ((unescaped << 1) | (esc_in << o)) & range;
    let cr = cr_w & range;
    let lf = lf_w & range;
    let crlf_in = (entry == DecoderState::CrLf) as u64;
    let cr_in = (entry == DecoderState::Cr) as u64;
    let lf_before = (lf << 1) | (crlf_in << o);
    let cr_two_before = (cr << 2) | (crlf_in << o) | ((cr_in << o) << 1);
    let dropped = unescaped | ((cr | lf) & !escaped) | (dot_w & range & lf_before & cr_two_before);
    let skip = !range | dropped;
    let (decoded_a, decoded_b) = avx2_decode_with_escape_mask(a, b, escaped);
    let n = 64 - skip.count_ones() as usize;
    let table = compact_table_16().as_ptr() as *const u8;
    // The store is 64 bytes wide; it stays below `input.len()`, as every
    // span store does, or goes through a stack copy.
    if dst + 64 <= input.len().min(output.len()) {
        avx2_compact_store64(
            decoded_a,
            decoded_b,
            skip,
            table,
            output.as_mut_ptr().add(dst),
        );
    } else {
        let mut buf = std::mem::MaybeUninit::<[u8; 64]>::uninit();
        let buf = buf.as_mut_ptr() as *mut u8;
        avx2_compact_store64(decoded_a, decoded_b, skip, table, buf);
        debug_assert!(dst + n <= output.len());
        std::ptr::copy_nonoverlapping(buf, output.as_mut_ptr().add(dst), n);
    }
    Some((n, (unescaped >> (e - 1)) & 1 != 0))
}

// The head up to the first 64-byte boundary `to` as one edge window, when
// the state and the range allow it. Returns whether it ran.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[inline]
#[allow(unsafe_op_in_unsafe_fn, clippy::manual_range_contains)]
unsafe fn avx2_head_window(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    search_end: bool,
    src: &mut usize,
    dst: &mut usize,
    to: usize,
) -> bool {
    if to < *src + 2
        || to < 3
        || to > 64
        || input.len() < 66
        || !matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        return false;
    }
    match avx2_edge_window(
        input,
        0,
        *src as u32,
        to as u32,
        state.state,
        search_end,
        output,
        *dst,
    ) {
        Some((n, esc_out)) => {
            *src = to;
            *dst += n;
            state.state = avx2_exit_state(input, to, esc_out);
            true
        }
        None => false,
    }
}

// The tail after the span (decoder_common.h:145-147). Up to `limit` =
// `input.len()`: the bytes to the span's reserve as one edge window, then
// the rest through the scalar kernel's main loop; a bounded call (`limit`
// short of the end) keeps the stepwise tail, which stops at `limit`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[inline]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn avx2_tail(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
    mut src: usize,
    mut dst: usize,
    limit: usize,
) -> Result<KernelOutcome, YencError> {
    if limit == input.len() {
        let reserve = if mode.search_end { 4 } else { 2 };
        let c = input.len().saturating_sub(reserve);
        // Without the end search: the bytes to the reserve as one edge
        // window, then the last two one step each. With it the tail holds
        // the trailer, and the scalar kernel takes it whole, as rapidyenc's
        // own scalar tail does (an edge window there costs more than it
        // saves).
        if !mode.search_end
            && c >= src + 2
            && c - src <= 64
            && c >= 64
            && matches!(
                state.state,
                DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
            )
        {
            let w = c - 64;
            if let Some((n, esc_out)) = avx2_edge_window(
                input,
                w,
                (src - w) as u32,
                64,
                state.state,
                false,
                output,
                dst,
            ) {
                src = c;
                dst += n;
                state.state = avx2_exit_state(input, c, esc_out);
                while src < limit {
                    if !avx2_tail_step(input, &mut src, output, &mut dst, state, mode)? {
                        break;
                    }
                }
                return Ok(KernelOutcome {
                    consumed: src,
                    written: dst,
                    end: state.end.into(),
                });
            }
        }
        if input.len() - src > reserve {
            return x86_fold_head(
                decode_kernel_scalar(
                    &input[src..],
                    &mut output[dst..],
                    state,
                    mode.dot_unstuffing,
                    mode.preserve_pending,
                    mode.search_end,
                ),
                src,
                dst,
            );
        }
    }
    while src < limit {
        if state.state == DecoderState::None {
            decode_plain_run(&input[..limit], &mut src, output, &mut dst);
            if src >= limit {
                break;
            }
        }
        if !avx2_tail_step(input, &mut src, output, &mut dst, state, mode)? {
            break;
        }
    }
    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

// AVX2 decode: a flat span loop carrying the escape/line state in registers,
// one special-char mask per 64-byte window, a straight `add(-42)` + store on
// the common window with no specials, and a single 2-lane LUT compaction on
// windows that contain `= \r \n`. Escape resolution runs through
// `fix_eq_mask` + `avx2_decode_with_escape_mask`. The rare dot-stuffing
// (`\r\n.`) and end-marker (`=y`) cases fall back to the scalar decoder for
// that one window.
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
    if search_end && dot_unstuffing && input.len() > WIDTH * 2 {
        // Head-align the span to a 64-byte boundary with the scalar machine
        // (at most 63 steps), as the SE=false oracle kernel and rapidyenc's own
        // driver do, so no window load splits a cache line. The entry shapes
        // the flat loop cannot see are re-resolved after every step.
        loop {
            if x86_search_end_head(input, output, state, mode, &mut head_src, &mut head_dst)? {
                return Ok(KernelOutcome {
                    consumed: head_src,
                    written: head_dst,
                    end: state.end.into(),
                });
            }
            if (input.as_ptr() as usize + head_src).is_multiple_of(WIDTH) {
                break;
            }
            // Plain bytes up to the boundary in one run, as rapidyenc's
            // scalar head does; a special byte goes through the machine.
            let to = (head_src + (input.as_ptr() as usize + head_src).wrapping_neg() % WIDTH)
                .min(input.len());
            if avx2_head_window(input, output, state, true, &mut head_src, &mut head_dst, to) {
                continue;
            }
            if state.state == DecoderState::None {
                let before = head_src;
                decode_plain_run(&input[..to], &mut head_src, output, &mut head_dst);
                if head_src != before {
                    continue;
                }
            }
            if !avx2_tail_step(input, &mut head_src, output, &mut head_dst, state, mode)? {
                break;
            }
        }
        if state.end != DecodeEnd::None || head_src >= input.len() {
            return Ok(KernelOutcome {
                consumed: head_src,
                written: head_dst,
                end: state.end.into(),
            });
        }
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
        // Without the end search, or when the head loop consumed nothing,
        // the raw kernel's outcome is already absolute: return it as it is,
        // not rebuilt through the fold.
        if !search_end {
            return decode_kernel_avx2_raw::<false>(input, output, state, mode);
        }
        if head_src == 0 && head_dst == 0 {
            return decode_kernel_avx2_raw::<true>(input, output, state, mode);
        }
        let outcome = decode_kernel_avx2_raw::<true>(
            &input[head_src..],
            &mut output[head_dst..],
            state,
            mode,
        );
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
        if !avx2_tail_step(input, &mut src, output, &mut dst, state, mode)? {
            break;
        }
    }

    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

// Raw end-searching decode of `input` that stops at `limit` instead of the
// end of `input`, for a caller that folds the CRC behind each stretch while
// the output is still in cache.
//
// The bytes after `limit` stay visible: the SIMD span keeps its lookahead
// reserve inside `input`, so an end probe in the last window sees exactly
// what an unbounded call would, and the span ends on `limit` itself when
// `limit` is 64-byte aligned with the reserve still ahead of it. That skips
// the scalar tail an unbounded call on the stretch alone would pay. The
// decode can run past `limit` only to finish an escape or reach an end
// marker. `None` when the asm kernel is not built in.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[allow(unsafe_op_in_unsafe_fn)]
pub(super) unsafe fn decode_raw_bounded_avx2(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    limit: usize,
) -> Option<Result<KernelOutcome, YencError>> {
    if !avx2_raw_asm() {
        return None;
    }
    let mode = DecodeStepMode {
        dot_unstuffing: true,
        preserve_pending: true,
        search_end: true,
    };
    let mut src = 0usize;
    let mut dst = 0usize;
    // The oracle enters in None/Eq/Cr/CrLf only; a stretch that ended inside
    // a dot or line-start escape sequence finishes it with the scalar machine.
    let head = (|| {
        while !matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        ) {
            if src >= limit
                || x86_search_end_head(input, output, state, mode, &mut src, &mut dst)?
                || !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)?
            {
                return Ok(true);
            }
        }
        Ok(src >= limit)
    })();
    Some(match head {
        Err(err) => Err(err),
        Ok(true) => Ok(KernelOutcome {
            consumed: src,
            written: dst,
            end: state.end.into(),
        }),
        Ok(false) => x86_fold_head(
            if avx2_and_add_select() {
                avx2_raw_kernel_oracle::<true, true>(
                    &input[src..],
                    &mut output[dst..],
                    state,
                    mode,
                    limit - src,
                )
            } else {
                avx2_raw_kernel_oracle::<true, false>(
                    &input[src..],
                    &mut output[dst..],
                    state,
                    mode,
                    limit - src,
                )
            },
            src,
            dst,
        ),
    })
}

// 2×2-lane LUT compaction + store for one 64-byte window, in the oracle's
// exact addressing shape (rapidyenc `decoder_avx2_base.h:556-600`, the
// `PLATFORM_AMD64` arm). Byte-for-byte identical output to the previous
// open-coded form; only the index/cursor arithmetic changed:
//
//   * table offsets are computed as **byte** offsets, not element indices, so
//     each lane costs one shift + one AND instead of shift + AND + scale
//     (`(skip >> 12) & 0x7fff0` is `((skip >> 16) & 0x7fff) * 16`);
//   * a single `skip >> 28` is shared between lane 2's table offset AND lane
//     2's popcount, replacing two independent extractions;
//   * popcounts use position-invariant masks (`skip & 0xffff_0000` rather than
//     `(skip >> 16) & 0xffff`) — popcount ignores bit position, so the shift
//     is pure waste;
//   * the four `+16` lane advances fold into one `+64` at the end, and the
//     cursor is a running pointer instead of a `(base, offset)` pair, which
//     also stops the output base from being spilled and reloaded per window.
//
// Measured on Haswell (E5-2666 v3) the old form cost 29 scalar µops here
// against the oracle's 27; combined with the loop-induction fix this closes
// the specials-path µop gap (see `yenc-avx2-lever` notes).
//
// `out` must have 64 writable bytes; the returned pointer is
// `out + 64 - popcount(skip)`.
// Inlined into its AVX2 callers (no `target_feature` of its own, so it may
// be `inline(always)`); called only from AVX2 code.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
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

// Byte-replication shuffle indices expanding a broadcast `escaped` u64 into
// per-byte mask lanes (lane A: source bytes 0..3, lane B: bytes 4..7) — the
// RIP-relative twins of the `_mm256_set_epi32` constants in
// [`avx2_decode_with_escape_mask`], laid out little-endian.
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
// One-bit-per-lane selectors within a replicated byte (`0x8040201008040201`),
// broadcast per-quadword — the RIP twin of `bit_lanes` in
// [`avx2_decode_with_escape_mask`].
#[cfg(target_arch = "x86_64")]
static AVX2_ESC_BIT_LANES: u64 = 0x8040_2010_0804_0201;

// Broadcast sources for the end-search probes' needle rematerialization in
// the asm kernel, which has no spare vector register to pin them.
#[cfg(target_arch = "x86_64")]
static YB_CR: u8 = 0x0d;
#[cfg(target_arch = "x86_64")]
static YB_LF: u8 = 0x0a;
#[cfg(target_arch = "x86_64")]
static YB_Y: u8 = 0x79;
#[cfg(target_arch = "x86_64")]
static YW_EQY: u16 = 0x793d;

// The whole raw kernel in the oracle's own shape — a transliteration of
// rapidyenc's compiled `do_decode_avx2` (`isRaw=true`) as emitted by GCC,
// for both `searchEnd` instantiations. The differential suite verifies
// byte-for-byte parity with the reference implementation.
//
// `SEARCH_END = true` adds the oracle's end probes and nothing else: the
// non-dot arm tests `\r\n=y` (`=y` at +2/+3 first, the CR/LF pair only on
// a hit, in an out-of-line block), and the dot arm builds `\r\n.\r\n` /
// `\r\n.=y` / `\r\n=y` from the pre-merge needle masks before the dot
// merge. A hit leaves the window unconsumed and hands the pre-merge mask
// and pre-window `escFirst` to [`x86_break_state`], so the scalar epilogue
// resumes at the window head with no backtrack.
//
// The oracle's structure, faithfully kept:
// - the input cursor is HEAD-ALIGNED to 64 bytes by a scalar prelude (in
//   Rust below), then the loop runs pure ALIGNED loads off one mid-window
//   cursor (`c` points 32 bytes in; lanes live at `[c-32]` and `[c]`) —
//   no cacheline-split window loads, and every memory operand is
//   base+disp8 (no index register, so load+op µops stay micro-fused);
// - specials take a BACKWARD branch to a block laid above the loop head;
//   the store falls through into the next window's loads; the clean path
//   is the head's fallthrough with its own back edge;
// - `skip == mask` on the common path (escaped bytes are never
//   special-table matches except `=\r`/`=\n`, which the collision path
//   handles by correcting `mask` with the resolved escape mask);
// - the collision predicate is the oracle's WIDER `mask & eq_shift1`;
// - `escFirst` is recomputed at the join (`meq >> 63`) and the
//   `yenc_offset` rebuild is interleaved into the store head;
// - the CR/LF/dot needles are rematerialized inside their rare blocks
//   (3 rename-free µops) instead of pinning two more ymm constants;
// - `min_mask` doubles as scratch in the CRLF probe and is rewritten on
//   every specials path before the back edge (dot-clamp or plain dot).
//
// Deliberate deviations, each strictly smaller: LUT rows load via
// `vmovdqu` (the heap table only guarantees byte alignment; unaligned
// loads are same-speed on aligned rows), the collision expansion reads the
// escape-select constants from this module's RIP statics and reuses the
// pinned `sub42`/`esc_off` registers instead of reloading them from
// rodata.
//
// Safety: the caller guarantees the raw-path contract (`dot_unstuffing`,
// entry state in {None,Eq,Cr,CrLf}); the scalar prelude/epilogue share the
// kernel's usual bounds; the span leaves 2 bytes (4 with the end search)
// after its last window, and no window's deepest lookahead (`+2`, or `+4`
// with the end search) reads past them. Flags are
// clobbered; the block reads input + LUT and writes output; no stack use.
#[cfg(target_arch = "x86_64")]
#[cfg_attr(not(weaver_yenc_raw_asm), allow(dead_code))]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn avx2_raw_kernel_oracle<const SEARCH_END: bool, const AND_ADD: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
    limit: usize,
) -> Result<KernelOutcome, YencError> {
    use std::arch::x86_64::*;
    const WIDTH: usize = 64;

    let mut src = 0usize;
    let mut dst = 0usize;

    // Head-align the input cursor to a 64-byte boundary with the scalar
    // machine — the oracle's own prelude. At most 63 steps. The end search
    // also resolves, before every step, the line-start entry shapes whose
    // `\r\n` sits behind the span and which the window probe cannot see.
    loop {
        if SEARCH_END && x86_search_end_head(input, output, state, mode, &mut src, &mut dst)? {
            return Ok(KernelOutcome {
                consumed: src,
                written: dst,
                end: state.end.into(),
            });
        }
        if src >= limit || (input.as_ptr() as usize + src) & (WIDTH - 1) == 0 {
            break;
        }
        // Plain bytes up to the boundary in one run, as rapidyenc's scalar
        // head does; a special byte goes through the machine.
        let to = (src + (input.as_ptr() as usize + src).wrapping_neg() % WIDTH).min(limit);
        if avx2_head_window(input, output, state, SEARCH_END, &mut src, &mut dst, to) {
            continue;
        }
        if state.state == DecoderState::None {
            let before = src;
            decode_plain_run(&input[..to], &mut src, output, &mut dst);
            if src != before {
                continue;
            }
        }
        if !avx2_tail_step(input, &mut src, output, &mut dst, state, mode)? {
            return Ok(KernelOutcome {
                consumed: src,
                written: dst,
                end: state.end.into(),
            });
        }
    }

    // A window's deepest read ends `reserve` bytes past it: the `+2` view in
    // the raw loop, the `+4` view with the end search. That is rapidyenc's
    // `lenBuffer` (decoder_common.h:49-51) less the 63 its round-up of `dLen`
    // (decoder_common.h:130-131) takes back, so the span covers the same
    // windows rapidyenc's does. The reserve stays inside `input` even when
    // `limit` stops the span early, so a bounded call probes the bytes past
    // `limit` exactly as an unbounded one does.
    let reserve = if SEARCH_END { 4 } else { 2 };
    let simd_limit = input.len().saturating_sub(reserve).min(limit);
    let span = (simd_limit.saturating_sub(src) / WIDTH) * WIDTH;
    // Software prefetch distance for the window loop: 2 KiB ahead on a span
    // long enough for it to land inside the input, otherwise the window just
    // loaded (a no-op prefetch). Chosen once per call.
    let prefetch_ahead: usize = if span >= PREFETCH_MIN_SPAN {
        PREFETCH_AHEAD
    } else {
        0
    };

    if span > 0 {
        // entry state -> escFirst / minMask (oracle _do_decode_simd switch),
        // computed AFTER the alignment steps from the live state.
        // A head window may leave `CrLfEq` (a line-start `=` before the
        // boundary); without an end search, or past the head's `=y` check,
        // it escapes the next byte exactly as `Eq` does.
        let mut esc_first: u64 =
            matches!(state.state, DecoderState::Eq | DecoderState::CrLfEq) as u64;
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
        let cr_needle = _mm256_set1_epi8(b'\r' as i8);
        // The escaped-byte offset: the whole -106 for `vpblendvb`, the -64 the
        // and-add form adds on top of the -42 offset.
        let esc_off = _mm256_set1_epi8(if AND_ADD { -64 } else { -106 });
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
        let c_out: *const u8;
        let mask_out: u64;

        // One template for both instantiations: the end search adds its
        // terminator probes to the two CR/LF arms (`nondot`, `dotarm`) and
        // their rare out-of-line block (`outline`), plus the operands those
        // use.
        macro_rules! oracle_span {
            ([$($pf:literal),*], [$($cr23:literal),*], [$($cr25:literal),*], [$($sel_join:literal),*], [$($sel_coll:literal),*], [$($nondot:literal),*], [$($dotarm:literal),*], [$($outline:literal),*], $($extra:tt)*) => {
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
                "vinserti128 {s0}, {eqn}, {s2:x}, 1",
                "mov {ef}, {meq}",
                "vpalignr {s2}, {s2}, {s0}, 15",
                "vpcmpeqb {s0}, {eqn}, ymmword ptr [{c} - 1]",
                "shr {ef}, 63",
                $($sel_join,)*
                // ---- store (falls into the head; oracle 6c1f3) ---------------
                "24:",
                "mov {meq:e}, {mask:e}",
                "vmovd {yov:x}, {ef:e}",
                "add {c}, 64",
                $($pf,)*
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
                // clean window, rapidyenc's (decoder_avx2_base.h:602-612):
                // byte 0 takes the carried offset, the carry resets, both
                // halves store whole, and the loop goes on.
                "vpaddb {s1}, {yov}, {lo}",
                "vpaddb {s0}, {hi}, {s42}",
                "vmovdqa {yov}, {s42}",
                "xor {ef:e}, {ef:e}",
                "add {c}, 64",
                "vmovdqu ymmword ptr [{out}], {s1}",
                "vmovdqu ymmword ptr [{out} + 32], {s0}",
                "add {out}, 64",
                "mov {mask}, {ne}",
                "add {mask}, {c}",
                "jnz 20b",
                "jmp 30f",
                // ---- CR/LF present: remat CR, `.`-at-+2 probe (oracle 6c358) -
                ".p2align 4",
                "23:",
                $($cr23,)*
                "vpcmpeqb {s1}, {dot}, ymmword ptr [{c} - 30]",
                "vpand {s1}, {s1}, {mmv}",
                "vpcmpeqb {mmv}, {dot}, ymmword ptr [{c} + 2]",
                "vpand {mmv}, {s3}, {mmv}",
                "vpor {s4}, {s1}, {mmv}",
                "vpmovmskb {t1:e}, {s4}",
                "test {t1:e}, {t1:e}",
                "jnz 25f",
                $($nondot,)*
                "vmovdqa {mmv}, {dot}",
                "jmp 22b",
                // ---- stuffed dot: merge `\r\n.`, clamp min_mask (6c428) ------
                "25:",
                "mov {t1:e}, 0x0a0a0a0a",
                "vmovdqa {s4}, ymmword ptr [{c} - 32]",
                "vmovdqa {s5}, ymmword ptr [{c}]",
                "vmovd {s3:x}, {t1:e}",
                "vpbroadcastd {s3}, {s3:x}",
                $($cr25,)*
                "vpcmpeqb {s0}, {s3}, ymmword ptr [{c} + 1]",
                "vpand {s5}, {s5}, {s0}",
                "vpcmpeqb {s3}, {s3}, ymmword ptr [{c} - 31]",
                "vpand {s4}, {s4}, {s1}",
                "vpand {mmv}, {mmv}, {s5}",
                "vpand {s4}, {s4}, {s3}",
                $($dotarm,)*
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
                $($sel_coll,)*
                "jmp 24b",
                $($outline,)*
                "30:",
                c = inout(reg) c_v => c_out,
                ne = in(reg) negend,
                out = inout(reg) out_v,
                ef = inout(reg) ef_v,
                tab = in(reg) table,
                pd = in(reg) prefetch_ahead,
                mask = out(reg) mask_out,
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
                    $($extra)*
                    options(nostack),
                )
            };
        }
        // The escape select, as the CPU family wants it (`avx2_and_add_select`):
        // `{s2}`/`{s0}` hold the escaped-byte masks of the low and high
        // halves; each half becomes `data + (escaped ? -106 : offset)`, the
        // offset being `{yov}` (byte 0 carries a straddled escape) for the
        // low half and -42 for the high half.
        macro_rules! with_select {
            ($sel_join:tt, $sel_coll:tt, $pf:tt) => {
        if SEARCH_END {
            oracle_span!(
                $pf,
                // The end search uses all 16 ymm registers: CR is
                // rematerialised in {s0}, which its arms read.
                [
                    "mov {t1:e}, 0x0d0d0d0d",
                    "vmovd {s0:x}, {t1:e}",
                    "vpbroadcastd {s0}, {s0:x}",
                    "vpcmpeqb {mmv}, {lo}, {s0}",
                    "vpcmpeqb {s3}, {hi}, {s0}"
                ],
                ["vpcmpeqb {s5}, {s5}, {s0}", "vpcmpeqb {s4}, {s4}, {s0}"],
                $sel_join,
                $sel_coll,
                [
                    // `\r\n=y` is the only end reachable without a stuffed
                    // dot (rapidyenc decoder_avx2_base.h, the `partialEndFound`
                    // arm): `=` at +2 and `y` at +3 first, the CR/LF anchor
                    // only on a candidate. {s0} still holds the CR needle.
                    "vpcmpeqb {s1}, {eqn}, ymmword ptr [{c} - 30]",
                    "vpcmpeqb {s4}, {eqn}, ymmword ptr [{c} + 2]",
                    "vpbroadcastb {s5}, byte ptr [rip + {y_b}]",
                    "vpcmpeqb {s3}, {s5}, ymmword ptr [{c} - 29]",
                    "vpand {s1}, {s1}, {s3}",
                    "vpcmpeqb {s3}, {s5}, ymmword ptr [{c} + 3]",
                    "vpand {s4}, {s4}, {s3}",
                    "vpor {s3}, {s1}, {s4}",
                    "vptest {s3}, {s3}",
                    "jnz 50f",
                    "51:"
                ],
                [
                    // Stuffed dot present: `\r\n.\r\n`, `\r\n.=y` and
                    // `\r\n=y`, before the dot merge so a hit exits with the
                    // pre-merge mask. Live: {s4} = `\r\n.` (A), {mmv} =
                    // `\r\n.` (B), {s5} = `\r\n` (B), {s3} = LF at +1 (A).
                    "vpbroadcastb {s0}, byte ptr [rip + {cr_b}]",
                    "vpcmpeqb {s1}, {s0}, {lo}",
                    "vpand {s3}, {s3}, {s1}",
                    "vpcmpeqb {s1}, {eqn}, ymmword ptr [{c} - 30]",
                    "vpbroadcastb {s6}, byte ptr [rip + {y_b}]",
                    "vpcmpeqb {s6}, {s6}, ymmword ptr [{c} - 29]",
                    "vpand {s1}, {s1}, {s6}",
                    "vpand {s3}, {s3}, {s1}",
                    "vpsrlw {s1}, {s1}, 8",
                    "vpbroadcastw {s6}, word ptr [rip + {eqy_w}]",
                    "vpcmpeqw {s6}, {s6}, ymmword ptr [{c} - 28]",
                    "vpsllw {s6}, {s6}, 8",
                    "vpor {s1}, {s1}, {s6}",
                    "vpcmpeqb {s6}, {s0}, ymmword ptr [{c} - 29]",
                    "vpbroadcastb {s0}, byte ptr [rip + {lf_b}]",
                    "vpcmpeqb {s0}, {s0}, ymmword ptr [{c} - 28]",
                    "vpand {s6}, {s6}, {s0}",
                    "vpor {s1}, {s1}, {s6}",
                    "vpand {s1}, {s1}, {s4}",
                    "vpor {s3}, {s3}, {s1}",
                    "vpcmpeqb {s1}, {eqn}, ymmword ptr [{c} + 2]",
                    "vpbroadcastb {s6}, byte ptr [rip + {y_b}]",
                    "vpcmpeqb {s6}, {s6}, ymmword ptr [{c} + 3]",
                    "vpand {s1}, {s1}, {s6}",
                    "vpand {s5}, {s5}, {s1}",
                    "vpor {s3}, {s3}, {s5}",
                    "vpsrlw {s1}, {s1}, 8",
                    "vpbroadcastw {s6}, word ptr [rip + {eqy_w}]",
                    "vpcmpeqw {s6}, {s6}, ymmword ptr [{c} + 4]",
                    "vpsllw {s6}, {s6}, 8",
                    "vpor {s1}, {s1}, {s6}",
                    "vpbroadcastb {s6}, byte ptr [rip + {lf_b}]",
                    "vpcmpeqb {s5}, {s6}, ymmword ptr [{c} + 4]",
                    "vpbroadcastb {s6}, byte ptr [rip + {cr_b}]",
                    "vpcmpeqb {s6}, {s6}, ymmword ptr [{c} + 3]",
                    "vpand {s6}, {s6}, {s5}",
                    "vpor {s1}, {s1}, {s6}",
                    "vpand {s1}, {s1}, {mmv}",
                    "vpor {s3}, {s3}, {s1}",
                    "vptest {s3}, {s3}",
                    "jnz 30f"
                ],
                [
                    // `=y` candidate without a stuffed dot: anchor on CR at
                    // +0 and LF at +1. A hit leaves the window unconsumed.
                    "50:",
                    "vpbroadcastb {s5}, byte ptr [rip + {lf_b}]",
                    "vpcmpeqb {s3}, {s5}, ymmword ptr [{c} - 31]",
                    "vpand {s1}, {s1}, {s3}",
                    "vpcmpeqb {s3}, {s0}, {lo}",
                    "vpand {s1}, {s1}, {s3}",
                    "vpcmpeqb {s3}, {s5}, ymmword ptr [{c} + 1]",
                    "vpand {s4}, {s4}, {s3}",
                    "vpcmpeqb {s3}, {s0}, {hi}",
                    "vpand {s4}, {s4}, {s3}",
                    "vpor {s1}, {s1}, {s4}",
                    "vptest {s1}, {s1}",
                    "jz 51b",
                    "jmp 30f"
                ],
                s6 = out(ymm_reg) _,
                cr_b = sym YB_CR,
                lf_b = sym YB_LF,
                y_b = sym YB_Y,
                eqy_w = sym YW_EQY,
            );
        } else {
            // Without it the CR needle has a register of its own
            // (rapidyenc's loop keeps it in one too).
            oracle_span!(
                $pf,
                ["vpcmpeqb {mmv}, {lo}, {crn}", "vpcmpeqb {s3}, {hi}, {crn}"],
                ["vpcmpeqb {s5}, {s5}, {crn}", "vpcmpeqb {s4}, {s4}, {crn}"],
                $sel_join,
                $sel_coll,
                [],
                [],
                [],
                crn = in(ymm_reg) cr_needle,
            );
        }
            };
        }
        // The prefetch is in the loop only on a span long enough for it
        // (`prefetch_ahead` nonzero); a short span runs the loop without it.
        macro_rules! with_pf {
            ($sel_join:tt, $sel_coll:tt) => {
                if prefetch_ahead != 0 {
                    with_select!($sel_join, $sel_coll, ["prefetcht0 byte ptr [{c} + {pd}]"]);
                } else {
                    with_select!($sel_join, $sel_coll, ["/* {pd} */"]);
                }
            };
        }
        if AND_ADD {
            // `vpand` keeps -64 where escaped, then two adds: the escaped
            // byte gets -64 + offset = -106 (the alignr fill and the
            // collision masks are clear at byte 0, so a carried escape there
            // is `{yov}`'s alone).
            with_pf!(
                [
                    "vpand {s2}, {s2}, {eof}",
                    "vpaddb {s2}, {lo}, {s2}",
                    "vpaddb {s2}, {s2}, {yov}",
                    "vpand {s0}, {s0}, {eof}",
                    "vpaddb {s0}, {hi}, {s0}",
                    "vpaddb {s0}, {s0}, {s42}"
                ],
                [
                    "vpand {s2}, {s2}, {eof}",
                    "vpaddb {s2}, {lo}, {s2}",
                    "vpaddb {s2}, {s2}, {yov}",
                    "vpcmpeqb {s0}, {s0}, {s3}",
                    "vpand {s0}, {s0}, {eof}",
                    "vpaddb {s0}, {hi}, {s0}",
                    "vpaddb {s0}, {s0}, {s42}"
                ]
            );
        } else {
            // rapidyenc's select (decoder_avx2_base.h:491-533): `vpblendvb`
            // between the offset and -106.
            with_pf!(
                [
                    "vpblendvb {s2}, {yov}, {eof}, {s2}",
                    "vpaddb {s2}, {lo}, {s2}",
                    "vpblendvb {s0}, {s42}, {eof}, {s0}",
                    "vpaddb {s0}, {hi}, {s0}"
                ],
                [
                    "vpblendvb {s2}, {yov}, {eof}, {s2}",
                    "vpaddb {s2}, {lo}, {s2}",
                    "vpcmpeqb {s0}, {s0}, {s3}",
                    "vpblendvb {s0}, {s42}, {eof}, {s0}",
                    "vpaddb {s0}, {hi}, {s0}"
                ]
            );
        }

        esc_first = ef_v;
        dst = out_v.offset_from(output.as_mut_ptr()) as usize;
        // The span ends with the cursor one half-window past the last window;
        // an end-search hit leaves it on the unconsumed window instead, with
        // the pre-merge specials mask and the pre-window escape carry — the
        // no-backtrack exit rule's inputs.
        let stopped = c_out.offset_from(input.as_ptr()) as usize - 32;
        if SEARCH_END && stopped != src + span {
            src = stopped;
            state.state = x86_break_state(input, src, mask_out, esc_first);
            return x86_fold_head(
                decode_kernel_scalar(
                    &input[src..],
                    &mut output[dst..],
                    state,
                    mode.dot_unstuffing,
                    mode.preserve_pending,
                    mode.search_end,
                ),
                src,
                dst,
            );
        }
        src += span;

        // The scalar machine's state after the span (at least one window
        // consumed, so `src >= 64`): `Cr`/`CrLf` before any byte, and a
        // line-start `=` carries `CrLfEq`, as `avx2_exit_state` reads them.
        state.state = avx2_exit_state(input, src, esc_first != 0);
    }

    // The tail, as above, up to `limit`.
    avx2_tail(input, output, state, mode, src, dst, limit)
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

// AVX2 implementation: process 32 bytes at a time.
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

#[cfg(test)]
mod span_end_tests {
    use super::*;

    // A byte mix dense in the bytes the window loop and its probes treat
    // specially (`=`, CR, LF, `.`, `y`), so every window boundary, the last
    // window of the span and the scalar tail after it all see escapes,
    // stuffed dots, line breaks and terminators.
    fn article(seed: u64, len: usize) -> Vec<u8> {
        const MIX: &[u8] = b"==\r\n\r\n..yy=y\r\n.=\rAbc*~\x00\xff\x3c\x3e\x2a\x80";
        let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                if x.is_multiple_of(3) {
                    MIX[(x >> 8) as usize % MIX.len()]
                } else {
                    (x >> 16) as u8
                }
            })
            .collect()
    }

    // Every input length across the span end (the last window, the reserve
    // and the scalar tail), at every 64-byte phase of the input start, in
    // every raw dot-unstuffing mode the AVX2 kernel serves, against the
    // scalar kernel: same bytes, cursors, end and carried state.
    #[test]
    fn avx2_span_end_matches_scalar() {
        if !is_x86_feature_detected!("avx2") {
            eprintln!("no avx2; skipping");
            return;
        }
        let mut checked = 0usize;
        for len in 129..=420usize {
            for seed in 0..4u64 {
                let body = article(seed * 1000 + len as u64, len);
                // 64-aligned backing store, so `phase` sets the input's
                // offset from a window boundary.
                let mut backing = vec![0u8; len + 128];
                let base = backing.as_ptr().align_offset(64);
                let phase = (seed as usize * 17 + len) % 64;
                let at = base + phase;
                backing[at..at + len].copy_from_slice(&body);
                let input = &backing[at..at + len];
                for search_end in [false, true] {
                    for initial in [
                        DecoderState::CrLf,
                        DecoderState::None,
                        DecoderState::Eq,
                        DecoderState::Cr,
                    ] {
                        let start = KernelState {
                            state: initial,
                            ..KernelState::body()
                        };
                        let mut want = vec![0u8; len + 64];
                        let mut want_state = start;
                        let want_outcome = scalar::decode_kernel_scalar(
                            input,
                            &mut want,
                            &mut want_state,
                            true,
                            true,
                            search_end,
                        );
                        let ctx = format!(
                            "len {len} seed {seed} phase {phase} end {search_end} {initial:?}"
                        );
                        let check = |name: &str, got: &[u8], got_state: KernelState, got_outcome: Result<KernelOutcome, YencError>| {
                            match (&want_outcome, got_outcome) {
                                (Ok(w), Ok(g)) => {
                                    assert_eq!(g, *w, "{name} outcome: {ctx}");
                                    assert_eq!(got[..g.written], want[..w.written], "{name} bytes: {ctx}");
                                    assert_eq!(got_state, want_state, "{name} state: {ctx}");
                                }
                                (Err(_), Err(_)) => {}
                                (w, g) => panic!("{name} result differs: {ctx}: scalar {w:?}, avx2 {g:?}"),
                            }
                        };
                        let mut got = vec![0u8; len + 64];
                        let mut got_state = start;
                        let got_outcome = unsafe {
                            decode_kernel_avx2(
                                input,
                                &mut got,
                                &mut got_state,
                                true,
                                true,
                                search_end,
                            )
                        };
                        check("kernel", &got, got_state, got_outcome);
                        // Both escape selects, whichever this CPU picks.
                        #[cfg(weaver_yenc_raw_asm)]
                        for and_add in [false, true] {
                            let mode = DecodeStepMode {
                                dot_unstuffing: true,
                                preserve_pending: true,
                                search_end,
                            };
                            let mut got = vec![0u8; len + 64];
                            let mut got_state = start;
                            let got_outcome = unsafe {
                                match (search_end, and_add) {
                                    (false, false) => avx2_raw_kernel_oracle::<false, false>(
                                        input,
                                        &mut got,
                                        &mut got_state,
                                        mode,
                                        len,
                                    ),
                                    (false, true) => avx2_raw_kernel_oracle::<false, true>(
                                        input,
                                        &mut got,
                                        &mut got_state,
                                        mode,
                                        len,
                                    ),
                                    (true, false) => avx2_raw_kernel_oracle::<true, false>(
                                        input,
                                        &mut got,
                                        &mut got_state,
                                        mode,
                                        len,
                                    ),
                                    (true, true) => avx2_raw_kernel_oracle::<true, true>(
                                        input,
                                        &mut got,
                                        &mut got_state,
                                        mode,
                                        len,
                                    ),
                                }
                            };
                            check(
                                if and_add { "and-add" } else { "blendv" },
                                &got,
                                got_state,
                                got_outcome,
                            );
                        }
                        checked += 1;
                    }
                }
            }
        }
        assert_eq!(checked, (420 - 129 + 1) * 4 * 2 * 4);
    }

    const ALL_STATES: [DecoderState; 7] = [
        DecoderState::CrLf,
        DecoderState::Eq,
        DecoderState::Cr,
        DecoderState::None,
        DecoderState::CrLfDot,
        DecoderState::CrLfDotCr,
        DecoderState::CrLfEq,
    ];

    // `input` decoded by `decode_kernel_avx2` (the asm or the Rust raw
    // kernel, as built) and, with the asm built and a raw-path entry, by
    // both oracle instantiations, against the scalar kernel: same result,
    // bytes, consumed, written, end and carried state.
    fn check_against_scalar(input: &[u8], entry: DecoderState, mode: DecodeStepMode, ctx: &str) {
        let start = KernelState {
            state: entry,
            ..KernelState::body()
        };
        let len = input.len();
        let mut want = vec![0u8; len + 64];
        let mut want_state = start;
        let want_outcome = scalar::decode_kernel_scalar(
            input,
            &mut want,
            &mut want_state,
            mode.dot_unstuffing,
            mode.preserve_pending,
            mode.search_end,
        );
        let check = |name: &str,
                     got: &[u8],
                     got_state: KernelState,
                     got_outcome: Result<KernelOutcome, YencError>| {
            match (&want_outcome, got_outcome) {
                (Ok(w), Ok(g)) => {
                    assert_eq!(g, *w, "{name} outcome: {ctx}");
                    assert_eq!(got[..g.written], want[..w.written], "{name} bytes: {ctx}");
                    assert_eq!(got_state, want_state, "{name} state: {ctx}");
                }
                (Err(w), Err(g)) => {
                    assert_eq!(format!("{w:?}"), format!("{g:?}"), "{name} error: {ctx}")
                }
                (w, g) => panic!("{name} result differs: {ctx}: scalar {w:?}, avx2 {g:?}"),
            }
        };
        let mut got = vec![0u8; len + 64];
        let mut got_state = start;
        let got_outcome = unsafe {
            decode_kernel_avx2(
                input,
                &mut got,
                &mut got_state,
                mode.dot_unstuffing,
                mode.preserve_pending,
                mode.search_end,
            )
        };
        check("kernel", &got, got_state, got_outcome);
        #[cfg(weaver_yenc_raw_asm)]
        if mode.dot_unstuffing
            && matches!(
                entry,
                DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
            )
        {
            for and_add in [false, true] {
                let mut got = vec![0u8; len + 64];
                let mut got_state = start;
                let got_outcome = unsafe {
                    match (mode.search_end, and_add) {
                        (false, false) => avx2_raw_kernel_oracle::<false, false>(
                            input,
                            &mut got,
                            &mut got_state,
                            mode,
                            len,
                        ),
                        (false, true) => avx2_raw_kernel_oracle::<false, true>(
                            input,
                            &mut got,
                            &mut got_state,
                            mode,
                            len,
                        ),
                        (true, false) => avx2_raw_kernel_oracle::<true, false>(
                            input,
                            &mut got,
                            &mut got_state,
                            mode,
                            len,
                        ),
                        (true, true) => avx2_raw_kernel_oracle::<true, true>(
                            input,
                            &mut got,
                            &mut got_state,
                            mode,
                            len,
                        ),
                    }
                };
                check(
                    if and_add { "and-add" } else { "blendv" },
                    &got,
                    got_state,
                    got_outcome,
                );
            }
        }
    }

    // `body` copied to `head` bytes past a 64-byte boundary.
    fn at_head(backing: &mut Vec<u8>, head: usize, body: &[u8]) -> (usize, usize) {
        backing.clear();
        backing.resize(body.len() + 128, 0);
        let at = backing.as_ptr().align_offset(64) + head;
        backing[at..at + body.len()].copy_from_slice(body);
        (at, at + body.len())
    }

    // A window ending in `=\r\n` before a stuffed line-start `.`, raw mode:
    // the escaped CR still ends the line (`Eq` then CR leaves `Cr`), so the
    // window carries `CrLf` and the `.` is dropped. The general kernel's
    // `span_end_state` required an unescaped CR and decoded the dot as data
    // (entry `CrLfDot` reaches that kernel): 135 bytes written, not 134.
    #[test]
    fn span_end_state_arms_crlf_after_an_escaped_cr() {
        let mut win = [b'a'; 64];
        win[61..64].copy_from_slice(b"=\r\n");
        let fixed_eq = 1u64 << 61;
        assert_eq!(
            super::super::x86_common::span_end_state(&win, fixed_eq, true),
            DecoderState::CrLf
        );
        assert_eq!(
            super::super::x86_common::span_end_state(&win, fixed_eq, false),
            DecoderState::None
        );
    }

    #[test]
    fn avx2_escaped_cr_before_a_stuffed_dot_matches_scalar() {
        if !is_x86_feature_detected!("avx2") {
            eprintln!("no avx2; skipping");
            return;
        }
        let mut backing = Vec::new();
        let mut body = vec![b'a'; 62];
        body.extend_from_slice(b"=\r\n.b");
        body.extend_from_slice(&[b'a'; 70]);
        let mut written = None;
        for head in 0..64 {
            for pp in [false, true] {
                let mode = DecodeStepMode {
                    dot_unstuffing: true,
                    preserve_pending: pp,
                    search_end: false,
                };
                let (a, b) = at_head(&mut backing, head, &body);
                for entry in ALL_STATES {
                    check_against_scalar(
                        &backing[a..b],
                        entry,
                        mode,
                        &format!("head {head} pp {pp} {entry:?}"),
                    );
                }
                // The repro's own shape: entry `CrLfDot` at head 0.
                if head == 0 && !pp {
                    let mut out = vec![0u8; body.len() + 64];
                    let mut st = KernelState {
                        state: DecoderState::CrLfDot,
                        ..KernelState::body()
                    };
                    let o = unsafe {
                        decode_kernel_avx2(&backing[a..b], &mut out, &mut st, true, false, false)
                    };
                    written = o.ok().map(|o| o.written);
                }
            }
        }
        assert_eq!(written, Some(134));
    }

    // Each line-state-sensitive trailing sequence at every window offset
    // (and in the span, the last window, the reserve and the tail), every
    // entry state, every mode, against the scalar kernel. Covers the carried
    // state at the raw kernels' exits: `Cr`/`CrLf` before any byte, and
    // `CrLfEq` for a line-start `=` left pending.
    #[test]
    fn avx2_trailing_line_states_match_scalar() {
        if !is_x86_feature_detected!("avx2") {
            eprintln!("no avx2; skipping");
            return;
        }
        const TRAILS: [&[u8]; 6] = [b"\r", b"\r\n", b"\r\n=", b"=\r\n", b"=\r\n.", b"\r\n.\r\n="];
        let mut backing = Vec::new();
        let mut checked = 0usize;
        for trail in TRAILS {
            for k in 0..=200usize {
                let mut body = vec![b'K'; k];
                body.extend_from_slice(trail);
                for head in [0usize, 1, 31, 63] {
                    let (a, b) = at_head(&mut backing, head, &body);
                    for dot_unstuffing in [true, false] {
                        for preserve_pending in [true, false] {
                            for search_end in [false, true] {
                                let mode = DecodeStepMode {
                                    dot_unstuffing,
                                    preserve_pending,
                                    search_end,
                                };
                                for entry in ALL_STATES {
                                    let ctx = format!(
                                        "trail {trail:?} k {k} head {head} {mode:?} {entry:?}"
                                    );
                                    check_against_scalar(&backing[a..b], entry, mode, &ctx);
                                    checked += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(checked, 6 * 201 * 4 * 8 * 7);
    }
}
