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
unsafe fn decode_kernel_avx2_raw(
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

    let sub42 = _mm256_set1_epi8(42i8.wrapping_neg());
    let dot = _mm256_set1_epi8(b'.' as i8);
    let eq_needle = _mm256_set1_epi8(b'=' as i8);
    let cr = _mm256_set1_epi8(b'\r' as i8);
    let lf = _mm256_set1_epi8(b'\n' as i8);
    let esc_off = _mm256_set1_epi8(-106);
    let table = compact_table_16();
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

    if input.len() > WIDTH * 2 {
        while src + WIDTH <= simd_limit {
            let a = _mm256_loadu_si256(input.as_ptr().add(src) as *const __m256i);
            let b = _mm256_loadu_si256(input.as_ptr().add(src + 32) as *const __m256i);

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
                    let tmp2a = _mm256_loadu_si256(input.as_ptr().add(src + 2) as *const __m256i);
                    let tmp2b = _mm256_loadu_si256(input.as_ptr().add(src + 34) as *const __m256i);
                    let m2cr_a =
                        _mm256_and_si256(_mm256_cmpeq_epi8(a, cr), _mm256_cmpeq_epi8(tmp2a, dot));
                    let m2cr_b =
                        _mm256_and_si256(_mm256_cmpeq_epi8(b, cr), _mm256_cmpeq_epi8(tmp2b, dot));
                    let partial = _mm256_movemask_epi8(_mm256_or_si256(m2cr_a, m2cr_b));
                    if partial != 0 {
                        let m1lf_a = _mm256_cmpeq_epi8(
                            lf,
                            _mm256_loadu_si256(input.as_ptr().add(src + 1) as *const __m256i),
                        );
                        let m1lf_b = _mm256_cmpeq_epi8(
                            lf,
                            _mm256_loadu_si256(input.as_ptr().add(src + 33) as *const __m256i),
                        );
                        let m1nl_a = _mm256_and_si256(m1lf_a, _mm256_cmpeq_epi8(a, cr));
                        let m1nl_b = _mm256_and_si256(m1lf_b, _mm256_cmpeq_epi8(b, cr));
                        let m2nldot_a = _mm256_and_si256(m2cr_a, m1nl_a);
                        let m2nldot_b = _mm256_and_si256(m2cr_b, m1nl_b);
                        mask |= (_mm256_movemask_epi8(m2nldot_a) as u32 as u64) << 2;
                        mask |= (_mm256_movemask_epi8(m2nldot_b) as u32 as u64) << 34;
                        let shifted = _mm256_zextsi128_si256(_mm_srli_si128::<14>(
                            _mm256_extracti128_si256::<1>(m2nldot_b),
                        ));
                        min_mask = _mm256_subs_epu8(dot, shifted);
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
                        _mm256_loadu_si256(input.as_ptr().add(src + 31) as *const __m256i),
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

                let shuf_a = _mm256_inserti128_si256(
                    _mm256_castsi128_si256(_mm_loadu_si128(
                        table[(skip & 0x7fff) as usize].as_ptr() as *const __m128i,
                    )),
                    _mm_loadu_si128(
                        table[((skip >> 16) & 0x7fff) as usize].as_ptr() as *const __m128i
                    ),
                    1,
                );
                let packed_a = _mm256_shuffle_epi8(decoded_a, shuf_a);
                _mm_storeu_si128(
                    output.as_mut_ptr().add(dst) as *mut __m128i,
                    _mm256_castsi256_si128(packed_a),
                );
                dst += 16 - (skip & 0xffff).count_ones() as usize;
                _mm_storeu_si128(
                    output.as_mut_ptr().add(dst) as *mut __m128i,
                    _mm256_extracti128_si256::<1>(packed_a),
                );
                dst += 16 - ((skip >> 16) & 0xffff).count_ones() as usize;
                let shuf_b = _mm256_inserti128_si256(
                    _mm256_castsi128_si256(_mm_loadu_si128(
                        table[((skip >> 32) & 0x7fff) as usize].as_ptr() as *const __m128i,
                    )),
                    _mm_loadu_si128(
                        table[((skip >> 48) & 0x7fff) as usize].as_ptr() as *const __m128i
                    ),
                    1,
                );
                let packed_b = _mm256_shuffle_epi8(decoded_b, shuf_b);
                _mm_storeu_si128(
                    output.as_mut_ptr().add(dst) as *mut __m128i,
                    _mm256_castsi256_si128(packed_b),
                );
                dst += 16 - ((skip >> 32) & 0xffff).count_ones() as usize;
                _mm_storeu_si128(
                    output.as_mut_ptr().add(dst) as *mut __m128i,
                    _mm256_extracti128_si256::<1>(packed_b),
                );
                dst += 16 - ((skip >> 48) & 0xffff).count_ones() as usize;
            } else {
                _mm256_storeu_si256(
                    output.as_mut_ptr().add(dst) as *mut __m256i,
                    _mm256_add_epi8(a, yenc_offset),
                );
                _mm256_storeu_si256(
                    output.as_mut_ptr().add(dst + 32) as *mut __m256i,
                    _mm256_add_epi8(b, sub42),
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

    // Hot path: faithful rapidyenc do_decode_avx2 port (raw dot-unstuffing, no
    // end-search). The other combos keep the general kernel below.
    if dot_unstuffing
        && !search_end
        && input.len() > WIDTH * 2
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        return decode_kernel_avx2_raw(input, output, state, mode);
    }

    let mut src = 0usize;
    let mut dst = 0usize;

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
        let table = compact_table_16();

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
                // stores with a popcount-advanced cursor.
                let shuf_a = _mm256_inserti128_si256(
                    _mm256_castsi128_si256(_mm_loadu_si128(
                        table[(skip & 0x7fff) as usize].as_ptr() as *const __m128i,
                    )),
                    _mm_loadu_si128(
                        table[((skip >> 16) & 0x7fff) as usize].as_ptr() as *const __m128i
                    ),
                    1,
                );
                let packed_a = _mm256_shuffle_epi8(decoded_a, shuf_a);
                _mm_storeu_si128(
                    output.as_mut_ptr().add(dst) as *mut __m128i,
                    _mm256_castsi256_si128(packed_a),
                );
                dst += 16 - (skip & 0xffff).count_ones() as usize;
                _mm_storeu_si128(
                    output.as_mut_ptr().add(dst) as *mut __m128i,
                    _mm256_extracti128_si256(packed_a, 1),
                );
                dst += 16 - ((skip >> 16) & 0xffff).count_ones() as usize;

                let shuf_b = _mm256_inserti128_si256(
                    _mm256_castsi128_si256(_mm_loadu_si128(
                        table[((skip >> 32) & 0x7fff) as usize].as_ptr() as *const __m128i,
                    )),
                    _mm_loadu_si128(
                        table[((skip >> 48) & 0x7fff) as usize].as_ptr() as *const __m128i
                    ),
                    1,
                );
                let packed_b = _mm256_shuffle_epi8(decoded_b, shuf_b);
                _mm_storeu_si128(
                    output.as_mut_ptr().add(dst) as *mut __m128i,
                    _mm256_castsi256_si128(packed_b),
                );
                dst += 16 - ((skip >> 32) & 0xffff).count_ones() as usize;
                _mm_storeu_si128(
                    output.as_mut_ptr().add(dst) as *mut __m128i,
                    _mm256_extracti128_si256(packed_b, 1),
                );
                dst += 16 - ((skip >> 48) & 0xffff).count_ones() as usize;
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
