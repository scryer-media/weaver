use super::*;

#[cfg(target_arch = "aarch64")]
pub(super) unsafe fn decode_kernel_neon(
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

    // Hot path: faithful port of rapidyenc `do_decode_neon<isRaw=true,
    // searchEnd=false>` (decoder_neon64.cc) — the flat, register-carried decode
    // loop, mirroring `decode_kernel_avx2_raw` / `decode_kernel_avx512_raw`.
    // Other combos (search_end, non-raw, or an entry state the head-switch
    // doesn't cover) keep the general scaffolding driver below.
    if dot_unstuffing
        && !search_end
        && input.len() > WIDTH * 2
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        return unsafe { decode_kernel_neon64_raw(input, output, state, mode) };
    }

    // Long-span shape on AArch64: set up constants once,
    // then stay inside the NEON loop until scalar boundary handling is needed.
    let tail_buffer = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail_buffer);

    if input.len() > WIDTH * 2 {
        let ctx = Neon64Ctx {
            dot_unstuffing,
            search_end,
            constants: unsafe { Neon64Constants::new() },
            table: compact_table_16(),
        };
        while (!search_end || state.end == DecodeEnd::None) && src + WIDTH <= simd_limit {
            if !matches!(
                state.state,
                DecoderState::None | DecoderState::CrLf | DecoderState::Eq | DecoderState::CrLfEq
            ) || output.len().saturating_sub(dst) < WIDTH
            {
                if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
                    break;
                }
                continue;
            }

            if state.line_length.is_some()
                && let Some(consumed) = unsafe {
                    try_decode_neon64_line(input, src, output, &mut dst, state, &ctx, simd_limit)?
                }
            {
                src += consumed;
                continue;
            }

            match unsafe {
                decode_neon64_span_block(input, &mut src, output, &mut dst, state, &ctx)?
            } {
                SpanBlockOutcome::Consumed => {}
                SpanBlockOutcome::ScalarThrough(through) => {
                    // Consume the trigger byte with the scalar state machine
                    // so the next SIMD attempt starts past it instead of
                    // re-analyzing the same window once per scalar step.
                    while src <= through && (!search_end || state.end == DecodeEnd::None) {
                        if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
                            break;
                        }
                    }
                }
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

/// Faithful port of rapidyenc `do_decode_neon<isRaw=true, searchEnd=false>`
/// (decoder_neon64.cc): the flat, register-carried decode loop over 64-byte
/// windows (4× `uint8x16`). Structurally a 1:1 clone of
/// [`decode_kernel_avx2_raw`](super::x86_avx2) / `decode_kernel_avx512_raw`,
/// differing only in the vector ops. The scalar `u64` bit-math (`fix_eq_mask`,
/// `escaped`, `esc_first`, `skip`, entry/exit state) is byte-identical to those
/// tiers, so all three share the same correctness envelope.
///
/// Register-carried state (oracle → here):
/// - `escFirst` → `esc_first: u64`
/// - `yencOffset` → `yenc_offset: uint8x16_t` (byte0 = 106 on a carried escape,
///   else 42; lanes 1..15 = 42)
/// - `nextMask`/`minMask` → `next_mask_mix: uint8x16_t`. Unlike AVX2/VBMI2
///   (which clamp via `min_epu8` + a `min_mask`), NEON keeps `.` OUT of the
///   specials LUT and injects a line-start dot by OR-ing `next_mask_mix` into
///   `cmp_a` after the `vqtbx1q` merge (oracle line 96). It is consumed exactly
///   once per window and recomputed (or zeroed) inside the `\r\n.` sub-branch.
#[cfg(target_arch = "aarch64")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn decode_kernel_neon64_raw(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    use std::arch::aarch64::*;
    const WIDTH: usize = 64;

    let mut src = 0usize;
    let mut dst = 0usize;
    // +2 dot lookahead loads read up to src+65 on the last window; the tail
    // budget (identical to AVX2/VBMI2 raw) keeps them in bounds.
    let tail = WIDTH - 1 + 4;
    let simd_limit = input.len().saturating_sub(tail);

    let constants = Neon64Constants::new();
    let table = compact_table_16();
    let zero = vdupq_n_u8(0);
    let normal_offset = constants.normal_offset; // dup(42)
    let escaped_offset = constants.escaped_offset; // dup(106)

    // Entry state → escFirst / nextMask (oracle `_do_decode_simd` head switch).
    let mut esc_first: u64 = (state.state == DecoderState::Eq) as u64;
    let entry_next_mask: u16 = match state.state {
        DecoderState::CrLf if input[0] == b'.' => 1,
        DecoderState::Cr if input.len() >= 2 && input[0] == b'\n' && input[1] == b'.' => 2,
        _ => 0,
    };

    // yenc_offset: byte0 = 106 (=42+64) if a `=` straddled in from the previous
    // window, else 42; lanes 1..15 always 42 (oracle line 58).
    let mut yenc_offset = if esc_first != 0 {
        vsetq_lane_u8::<0>(42 + 64, normal_offset)
    } else {
        normal_offset
    };
    // next_mask_mix: a carried line-start dot. Values 1/2 survive the
    // `& bit_weights` reduction at lanes 0/1 (oracle lines 53-57).
    let mut next_mask_mix = match entry_next_mask {
        1 => vsetq_lane_u8::<0>(1, zero),
        2 => vsetq_lane_u8::<1>(2, zero),
        _ => zero,
    };

    if input.len() > WIDTH * 2 {
        while src + WIDTH <= simd_limit {
            let a = vld1q_u8(input.as_ptr().add(src));
            let b = vld1q_u8(input.as_ptr().add(src + 16));
            let c = vld1q_u8(input.as_ptr().add(src + 32));
            let d = vld1q_u8(input.as_ptr().add(src + 48));

            let eq_a = vceqq_u8(a, constants.eq);
            let eq_b = vceqq_u8(b, constants.eq);
            let eq_c = vceqq_u8(c, constants.eq);
            let eq_d = vceqq_u8(d, constants.eq);

            // Fold the CR/LF compares into the `=` compare via one table lookup
            // (oracle lines 71-95). `.` is deliberately absent from the table;
            // stuffed dots enter the mask only via `next_mask_mix` (below) or
            // the scalar `mask |= kill_dots << 2`.
            let mut cmp_a = vqtbx1q_u8(eq_a, constants.crlf_table, a);
            let cmp_b = vqtbx1q_u8(eq_b, constants.crlf_table, b);
            let cmp_c = vqtbx1q_u8(eq_c, constants.crlf_table, c);
            let cmp_d = vqtbx1q_u8(eq_d, constants.crlf_table, d);
            // Inject the carried/straddled line-start dot (oracle line 96). This
            // is the NEON replacement for the AVX2 `min_mask` clamp.
            cmp_a = vorrq_u8(cmp_a, next_mask_mix);

            let any = vorrq_u8(vorrq_u8(cmp_a, cmp_b), vorrq_u8(cmp_c, cmp_d));
            if vmaxvq_u8(any) != 0 {
                // Fused bit-weight reduction: lane 0 → specials mask, lane 1 →
                // `=` mask (oracle lines 102-125).
                let merged = vpaddq_u8(
                    vpaddq_u8(
                        vpaddq_u8(
                            vandq_u8(cmp_a, constants.bit_weights),
                            vandq_u8(cmp_b, constants.bit_weights),
                        ),
                        vpaddq_u8(
                            vandq_u8(cmp_c, constants.bit_weights),
                            vandq_u8(cmp_d, constants.bit_weights),
                        ),
                    ),
                    vpaddq_u8(
                        vpaddq_u8(
                            vandq_u8(eq_a, constants.bit_weights),
                            vandq_u8(eq_b, constants.bit_weights),
                        ),
                        vpaddq_u8(
                            vandq_u8(eq_c, constants.bit_weights),
                            vandq_u8(eq_d, constants.bit_weights),
                        ),
                    ),
                );
                let mut mask = vgetq_lane_u64::<0>(vreinterpretq_u64_u8(merged));
                let mask_eq = vgetq_lane_u64::<1>(vreinterpretq_u64_u8(merged));

                // Handle `\r\n.` dot-stuffing (oracle lines 129-289, isRaw path).
                // A nonzero `next_mask_mix` always forces `mask != mask_eq` (it
                // adds a dot bit the `=` mask lacks), so this branch is where the
                // carry is consumed and reset — the invariant that keeps a stale
                // carry from double-stripping a dot.
                if mask != mask_eq {
                    let tmp2 = vld1q_u8(input.as_ptr().add(src + 50));
                    let cr_a = vceqq_u8(a, constants.cr);
                    let cr_b = vceqq_u8(b, constants.cr);
                    let cr_c = vceqq_u8(c, constants.cr);
                    let cr_d = vceqq_u8(d, constants.cr);
                    let m2cr_a = vandq_u8(cr_a, vceqq_u8(vextq_u8::<2>(a, b), constants.dot));
                    let m2cr_b = vandq_u8(cr_b, vceqq_u8(vextq_u8::<2>(b, c), constants.dot));
                    let m2cr_c = vandq_u8(cr_c, vceqq_u8(vextq_u8::<2>(c, d), constants.dot));
                    let m2cr_d = vandq_u8(cr_d, vceqq_u8(tmp2, constants.dot));
                    let m2cr_any = vorrq_u8(vorrq_u8(m2cr_a, m2cr_b), vorrq_u8(m2cr_c, m2cr_d));
                    if vmaxvq_u8(m2cr_any) != 0 {
                        let lf_a = vceqq_u8(vextq_u8::<1>(a, b), constants.lf);
                        let lf_b = vceqq_u8(vextq_u8::<1>(b, c), constants.lf);
                        let lf_c = vceqq_u8(vextq_u8::<1>(c, d), constants.lf);
                        let lf_d = vceqq_u8(vld1q_u8(input.as_ptr().add(src + 49)), constants.lf);
                        let m2nldot_a = vandq_u8(m2cr_a, lf_a);
                        let m2nldot_b = vandq_u8(m2cr_b, lf_b);
                        let m2nldot_c = vandq_u8(m2cr_c, lf_c);
                        let m2nldot_d = vandq_u8(m2cr_d, lf_d);
                        // Reduce the `\r\n.` matches to a u64 and strip the
                        // stuffed dot (which sits 2 bytes after the `\r`).
                        let kill_dots = neon64_compare_mask64(
                            [m2nldot_a, m2nldot_b, m2nldot_c, m2nldot_d],
                            constants.bit_weights,
                        );
                        mask |= kill_dots << 2;
                        // Carry a straddling dot (CR at byte 62/63) to the next
                        // window's byte 0/1 (oracle line 252).
                        next_mask_mix = vextq_u8::<14>(m2nldot_d, zero);
                    } else {
                        // `\r\n` present but no stuffed dot: reset the carry.
                        next_mask_mix = zero;
                    }
                }
                // If `mask == mask_eq`, `next_mask_mix` was already zero (a
                // nonzero carry would have forced `mask != mask_eq`), so leaving
                // it untouched matches the oracle.

                // Portable scalar escape bit-math — byte-identical to the AVX2 /
                // VBMI2 raw ports.
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

                let decoded = if escaped == 0 {
                    // No escaped bytes in this window: plain offset subtract.
                    // Lane A uses `yenc_offset` (identical to `normal_offset`
                    // here, since escaped==0 ⇒ esc_first_in==0).
                    [
                        vsubq_u8(a, yenc_offset),
                        vsubq_u8(b, normal_offset),
                        vsubq_u8(c, normal_offset),
                        vsubq_u8(d, normal_offset),
                    ]
                } else if collision {
                    // Consecutive `=` run: expand the chain-resolved `escaped`
                    // mask (which already carries esc_first at bit 0) to a
                    // per-lane select (oracle lines 315-354).
                    neon_decode_with_escape_mask([a, b, c, d], escaped, &constants)
                } else {
                    // Isolated escapes: the escaped lanes are exactly the `=`
                    // compares shifted one byte. Lane A uses the oracle's
                    // `vext(dup42, cmpEqA, 15)` + `yenc_offset` 42-bit-trick so a
                    // carried escape at byte 0 is applied via `yenc_offset`
                    // (oracle lines 360-391).
                    let sel_a = vextq_u8::<15>(normal_offset, eq_a);
                    let sel_b = vextq_u8::<15>(eq_a, eq_b);
                    let sel_c = vextq_u8::<15>(eq_b, eq_c);
                    let sel_d = vextq_u8::<15>(eq_c, eq_d);
                    [
                        vsubq_u8(a, vbslq_u8(sel_a, escaped_offset, yenc_offset)),
                        vsubq_u8(b, vbslq_u8(sel_b, escaped_offset, normal_offset)),
                        vsubq_u8(c, vbslq_u8(sel_c, escaped_offset, normal_offset)),
                        vsubq_u8(d, vbslq_u8(sel_d, escaped_offset, normal_offset)),
                    ]
                };

                let skip = mask & !escaped;
                // Rebuild yenc_offset byte0 for the next window's carried escape
                // (oracle line 393).
                yenc_offset = vsetq_lane_u8::<0>(((esc_first as u8) << 6) | 42, normal_offset);

                if skip == 0 {
                    vst1q_u8(output.as_mut_ptr().add(dst), decoded[0]);
                    vst1q_u8(output.as_mut_ptr().add(dst + 16), decoded[1]);
                    vst1q_u8(output.as_mut_ptr().add(dst + 32), decoded[2]);
                    vst1q_u8(output.as_mut_ptr().add(dst + 48), decoded[3]);
                    dst += WIDTH;
                } else {
                    // Four independent LUT-compaction stores; the entry gate +
                    // tail guarantee ≥64 spare output bytes so each 16-byte
                    // store can overwrite ahead.
                    let keeps = per_group_keeps(skip);
                    compact_store_16(
                        decoded[0],
                        (skip & 0xffff) as u16,
                        keeps.0,
                        table,
                        output,
                        &mut dst,
                    );
                    compact_store_16(
                        decoded[1],
                        ((skip >> 16) & 0xffff) as u16,
                        keeps.1,
                        table,
                        output,
                        &mut dst,
                    );
                    compact_store_16(
                        decoded[2],
                        ((skip >> 32) & 0xffff) as u16,
                        keeps.2,
                        table,
                        output,
                        &mut dst,
                    );
                    compact_store_16(
                        decoded[3],
                        ((skip >> 48) & 0xffff) as u16,
                        keeps.3,
                        table,
                        output,
                        &mut dst,
                    );
                }
            } else {
                // No specials (and no carried dot): bulk decode, subtract the
                // carried offset on lane A and 42 on the rest (oracle lines
                // 423-431).
                vst1q_u8(output.as_mut_ptr().add(dst), vsubq_u8(a, yenc_offset));
                vst1q_u8(
                    output.as_mut_ptr().add(dst + 16),
                    vsubq_u8(b, normal_offset),
                );
                vst1q_u8(
                    output.as_mut_ptr().add(dst + 32),
                    vsubq_u8(c, normal_offset),
                );
                vst1q_u8(
                    output.as_mut_ptr().add(dst + 48),
                    vsubq_u8(d, normal_offset),
                );
                dst += WIDTH;
                esc_first = 0;
                yenc_offset = normal_offset;
            }
            src += WIDTH;
        }
    }

    // Derive the exit state from the trailing bytes (oracle-equivalent to the
    // AVX2/VBMI2 raw ports' out_next_mask lookback) — but ONLY when the SIMD
    // loop consumed at least one window. With no window consumed (len in
    // {129,130} => simd_limit < WIDTH), `src` is still 0 and the entry state
    // MUST survive untouched for the scalar epilogue, else a carried Cr/CrLf
    // line-start (pending stuffed dot) is clobbered to None and mis-decoded.
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

#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy)]
pub(super) struct Neon64Constants {
    eq: std::arch::aarch64::uint8x16_t,
    cr: std::arch::aarch64::uint8x16_t,
    lf: std::arch::aarch64::uint8x16_t,
    dot: std::arch::aarch64::uint8x16_t,
    // Table-lookup rows marking '\n' (10) and '\r' (13) so one vqtbx1q merges
    // the CR/LF compares into the '=' compare (the specials gate).
    crlf_table: std::arch::aarch64::uint8x16_t,
    bit_weights: std::arch::aarch64::uint8x16_t,
    selector: std::arch::aarch64::uint8x16_t,
    normal_offset: std::arch::aarch64::uint8x16_t,
    escaped_offset: std::arch::aarch64::uint8x16_t,
}

#[cfg(target_arch = "aarch64")]
impl Neon64Constants {
    #[inline(always)]
    pub(super) unsafe fn new() -> Self {
        use std::arch::aarch64::*;

        Self {
            eq: unsafe { vdupq_n_u8(b'=') },
            cr: unsafe { vdupq_n_u8(b'\r') },
            lf: unsafe { vdupq_n_u8(b'\n') },
            dot: unsafe { vdupq_n_u8(b'.') },
            crlf_table: unsafe {
                vld1q_u8([0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 255, 0, 0].as_ptr())
            },
            bit_weights: unsafe {
                vld1q_u8([1u8, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128].as_ptr())
            },
            selector: unsafe {
                vld1q_u8([0u8, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1].as_ptr())
            },
            normal_offset: unsafe { vdupq_n_u8(42) },
            escaped_offset: unsafe { vdupq_n_u8(106) },
        }
    }
}

/// Result of one 64-byte SIMD block attempt.
#[cfg(target_arch = "aarch64")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SpanBlockOutcome {
    /// The whole 64-byte window was consumed and decoded.
    Consumed,
    /// A control/terminator candidate needs the scalar state machine; the
    /// driver must consume through this absolute source index before
    /// re-entering SIMD so the trigger is behind the next window.
    ScalarThrough(usize),
}

/// Immutable per-kernel-call context for the 64-byte NEON block.
#[cfg(target_arch = "aarch64")]
pub(super) struct Neon64Ctx<'a> {
    pub(super) dot_unstuffing: bool,
    pub(super) search_end: bool,
    pub(super) constants: Neon64Constants,
    pub(super) table: &'a [[u8; 16]; 32768],
}

/// Per-16-bit-group keep counts for a 64-bit skip mask, via one SWAR popcount
/// pass (stays in the scalar domain where the mask already lives).
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(super) fn per_group_keeps(skip: u64) -> (usize, usize, usize, usize) {
    let x = skip - ((skip >> 1) & 0x5555_5555_5555_5555);
    let x = (x & 0x3333_3333_3333_3333) + ((x >> 2) & 0x3333_3333_3333_3333);
    let x = (x + (x >> 4)) & 0x0f0f_0f0f_0f0f_0f0f;
    let sums = x + (x >> 8);
    (
        16 - (sums & 0x1f) as usize,
        16 - ((sums >> 16) & 0x1f) as usize,
        16 - ((sums >> 32) & 0x1f) as usize,
        16 - ((sums >> 48) & 0x1f) as usize,
    )
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(super) unsafe fn neon64_compare_mask64(
    vectors: [std::arch::aarch64::uint8x16_t; 4],
    bit_weights: std::arch::aarch64::uint8x16_t,
) -> u64 {
    use std::arch::aarch64::*;

    let merged = unsafe {
        vpaddq_u8(
            vpaddq_u8(
                vpaddq_u8(
                    vandq_u8(vectors[0], bit_weights),
                    vandq_u8(vectors[1], bit_weights),
                ),
                vpaddq_u8(
                    vandq_u8(vectors[2], bit_weights),
                    vandq_u8(vectors[3], bit_weights),
                ),
            ),
            vdupq_n_u8(0),
        )
    };
    unsafe { vgetq_lane_u64::<0>(vreinterpretq_u64_u8(merged)) }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(super) unsafe fn try_decode_neon64_line(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    ctx: &Neon64Ctx<'_>,
    simd_limit: usize,
) -> Result<Option<usize>, YencError> {
    use std::arch::aarch64::*;

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
    if ctx.dot_unstuffing && input[src] == b'.' {
        return Ok(None);
    }
    if ctx.search_end && ctx.dot_unstuffing && input[src] == b'=' && input[src + 1] == b'y' {
        return Ok(None);
    }
    if input[line_end - 1] == b'=' || output.len().saturating_sub(*dst) < line_length {
        return Ok(None);
    }

    // Single pass; the '=' at line_end-1 guard above already excludes a
    // dangling escape at line end, and a raw CR/LF mid-line rewinds the
    // output cursor and hands the line back to the general path.
    let constants = &ctx.constants;
    let chunks = line_length / WIDTH;
    let dst_start = *dst;
    let mut esc_first = 0u64;
    for chunk_idx in 0..chunks {
        let chunk_src = src + chunk_idx * WIDTH;
        let vectors = [
            unsafe { vld1q_u8(input.as_ptr().add(chunk_src)) },
            unsafe { vld1q_u8(input.as_ptr().add(chunk_src + 16)) },
            unsafe { vld1q_u8(input.as_ptr().add(chunk_src + 32)) },
            unsafe { vld1q_u8(input.as_ptr().add(chunk_src + 48)) },
        ];
        let eq_vecs = [
            unsafe { vceqq_u8(vectors[0], constants.eq) },
            unsafe { vceqq_u8(vectors[1], constants.eq) },
            unsafe { vceqq_u8(vectors[2], constants.eq) },
            unsafe { vceqq_u8(vectors[3], constants.eq) },
        ];
        let crlf = [
            unsafe {
                vorrq_u8(
                    vceqq_u8(vectors[0], constants.cr),
                    vceqq_u8(vectors[0], constants.lf),
                )
            },
            unsafe {
                vorrq_u8(
                    vceqq_u8(vectors[1], constants.cr),
                    vceqq_u8(vectors[1], constants.lf),
                )
            },
            unsafe {
                vorrq_u8(
                    vceqq_u8(vectors[2], constants.cr),
                    vceqq_u8(vectors[2], constants.lf),
                )
            },
            unsafe {
                vorrq_u8(
                    vceqq_u8(vectors[3], constants.cr),
                    vceqq_u8(vectors[3], constants.lf),
                )
            },
        ];
        if unsafe { neon64_compare_mask64(crlf, constants.bit_weights) } != 0 {
            *dst = dst_start;
            return Ok(None);
        }
        let eq = unsafe { neon64_compare_mask64(eq_vecs, constants.bit_weights) };
        let fixed_eq = fix_eq_mask(eq, (eq << 1) | esc_first);
        let escaped = (fixed_eq << 1) | esc_first;
        let skip = fixed_eq;

        if skip == 0 && escaped == 0 {
            unsafe {
                vst1q_u8(
                    output.as_mut_ptr().add(*dst),
                    vsubq_u8(vectors[0], constants.normal_offset),
                );
                vst1q_u8(
                    output.as_mut_ptr().add(*dst + 16),
                    vsubq_u8(vectors[1], constants.normal_offset),
                );
                vst1q_u8(
                    output.as_mut_ptr().add(*dst + 32),
                    vsubq_u8(vectors[2], constants.normal_offset),
                );
                vst1q_u8(
                    output.as_mut_ptr().add(*dst + 48),
                    vsubq_u8(vectors[3], constants.normal_offset),
                );
            }
            *dst += WIDTH;
        } else {
            let decoded = if esc_first == 0 && eq & (eq << 1) == 0 {
                // No adjacent '=' and no carried-in escape: escaped positions
                // are exactly the '=' compares shifted one lane, so the
                // offset select never leaves the vector domain (same shortcut
                // as the span block).
                let zero = unsafe { vdupq_n_u8(0) };
                let sel_a = unsafe { vextq_u8::<15>(zero, eq_vecs[0]) };
                let sel_b = unsafe { vextq_u8::<15>(eq_vecs[0], eq_vecs[1]) };
                let sel_c = unsafe { vextq_u8::<15>(eq_vecs[1], eq_vecs[2]) };
                let sel_d = unsafe { vextq_u8::<15>(eq_vecs[2], eq_vecs[3]) };
                [
                    unsafe {
                        vsubq_u8(
                            vectors[0],
                            vbslq_u8(sel_a, constants.escaped_offset, constants.normal_offset),
                        )
                    },
                    unsafe {
                        vsubq_u8(
                            vectors[1],
                            vbslq_u8(sel_b, constants.escaped_offset, constants.normal_offset),
                        )
                    },
                    unsafe {
                        vsubq_u8(
                            vectors[2],
                            vbslq_u8(sel_c, constants.escaped_offset, constants.normal_offset),
                        )
                    },
                    unsafe {
                        vsubq_u8(
                            vectors[3],
                            vbslq_u8(sel_d, constants.escaped_offset, constants.normal_offset),
                        )
                    },
                ]
            } else {
                unsafe { neon_decode_with_escape_mask(vectors, escaped, constants) }
            };
            let keeps = per_group_keeps(skip);
            unsafe {
                compact_store_16(
                    decoded[0],
                    (skip & 0xffff) as u16,
                    keeps.0,
                    ctx.table,
                    output,
                    dst,
                );
                compact_store_16(
                    decoded[1],
                    ((skip >> 16) & 0xffff) as u16,
                    keeps.1,
                    ctx.table,
                    output,
                    dst,
                );
                compact_store_16(
                    decoded[2],
                    ((skip >> 32) & 0xffff) as u16,
                    keeps.2,
                    ctx.table,
                    output,
                    dst,
                );
                compact_store_16(
                    decoded[3],
                    ((skip >> 48) & 0xffff) as u16,
                    keeps.3,
                    ctx.table,
                    output,
                    dst,
                );
            }
        }

        esc_first = (fixed_eq & LAST != 0) as u64;
    }

    debug_assert_eq!(esc_first, 0);
    state.state = DecoderState::CrLf;
    Ok(Some(line_length + 2))
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(super) unsafe fn decode_neon64_span_block(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    ctx: &Neon64Ctx<'_>,
) -> Result<SpanBlockOutcome, YencError> {
    use std::arch::aarch64::*;

    let dot_unstuffing = ctx.dot_unstuffing;
    let search_end = ctx.search_end;
    let constants = &ctx.constants;
    let table = ctx.table;

    let block_src = *src;
    debug_assert!(input.len().saturating_sub(block_src) > 64);
    debug_assert!(output.len().saturating_sub(*dst) >= 64);

    // Escape carried in from the previous window's trailing '=' (the escFirst
    // bit): bit 0 of this window is the escaped partner byte.
    let esc_first = matches!(state.state, DecoderState::Eq | DecoderState::CrLfEq);
    if esc_first
        && search_end
        && dot_unstuffing
        && state.state == DecoderState::CrLfEq
        && input[block_src] == b'y'
    {
        // Line-start "=y…" control candidate; the scalar machine resolves it.
        return Ok(SpanBlockOutcome::ScalarThrough(block_src));
    }

    // A line-start dot at the window's first byte is invisible to the
    // specials gate below; resolve its lookahead here. Terminator/control
    // shapes go to the scalar state machine, a plain stuffed dot is recorded
    // for the vector paths.
    let dot0 = dot_unstuffing && state.state == DecoderState::CrLf && input[block_src] == b'.';
    if dot0 {
        let next = input[block_src + 1];
        if next == b'\r' || next == b'\n' || next == b'=' {
            return Ok(SpanBlockOutcome::ScalarThrough(block_src));
        }
    }

    let a = unsafe { vld1q_u8(input.as_ptr().add(block_src)) };
    let b = unsafe { vld1q_u8(input.as_ptr().add(block_src + 16)) };
    let c = unsafe { vld1q_u8(input.as_ptr().add(block_src + 32)) };
    let d = unsafe { vld1q_u8(input.as_ptr().add(block_src + 48)) };
    let eq_a = unsafe { vceqq_u8(a, constants.eq) };
    let eq_b = unsafe { vceqq_u8(b, constants.eq) };
    let eq_c = unsafe { vceqq_u8(c, constants.eq) };
    let eq_d = unsafe { vceqq_u8(d, constants.eq) };
    // One table lookup folds the '\r'/'\n' compares into the '=' compare, so
    // "does this window contain any special byte?" costs a single reduce.
    let cmp_a = unsafe { vqtbx1q_u8(eq_a, constants.crlf_table, a) };
    let cmp_b = unsafe { vqtbx1q_u8(eq_b, constants.crlf_table, b) };
    let cmp_c = unsafe { vqtbx1q_u8(eq_c, constants.crlf_table, c) };
    let cmp_d = unsafe { vqtbx1q_u8(eq_d, constants.crlf_table, d) };
    let any = unsafe { vorrq_u8(vorrq_u8(cmp_a, cmp_b), vorrq_u8(cmp_c, cmp_d)) };
    let has_specials = unsafe { vmaxvq_u8(any) } != 0;

    if !has_specials && !dot0 && !esc_first {
        unsafe {
            vst1q_u8(
                output.as_mut_ptr().add(*dst),
                vsubq_u8(a, constants.normal_offset),
            );
            vst1q_u8(
                output.as_mut_ptr().add(*dst + 16),
                vsubq_u8(b, constants.normal_offset),
            );
            vst1q_u8(
                output.as_mut_ptr().add(*dst + 32),
                vsubq_u8(c, constants.normal_offset),
            );
            vst1q_u8(
                output.as_mut_ptr().add(*dst + 48),
                vsubq_u8(d, constants.normal_offset),
            );
        }
        *src += 64;
        *dst += 64;
        state.state = DecoderState::None;
        return Ok(SpanBlockOutcome::Consumed);
    }

    // Fold the specials mask and the '=' mask in one combined reduction:
    // lane 0 of `merged` holds the specials bits, lane 1 the '=' bits.
    let (mask, eq) = if has_specials {
        let merged = unsafe {
            vpaddq_u8(
                vpaddq_u8(
                    vpaddq_u8(
                        vandq_u8(cmp_a, constants.bit_weights),
                        vandq_u8(cmp_b, constants.bit_weights),
                    ),
                    vpaddq_u8(
                        vandq_u8(cmp_c, constants.bit_weights),
                        vandq_u8(cmp_d, constants.bit_weights),
                    ),
                ),
                vpaddq_u8(
                    vpaddq_u8(
                        vandq_u8(eq_a, constants.bit_weights),
                        vandq_u8(eq_b, constants.bit_weights),
                    ),
                    vpaddq_u8(
                        vandq_u8(eq_c, constants.bit_weights),
                        vandq_u8(eq_d, constants.bit_weights),
                    ),
                ),
            )
        };
        (
            unsafe { vgetq_lane_u64::<0>(vreinterpretq_u64_u8(merged)) },
            unsafe { vgetq_lane_u64::<1>(vreinterpretq_u64_u8(merged)) },
        )
    } else {
        (0, 0)
    };

    let esc_first = esc_first as u64;
    let fixed_eq = fix_eq_mask(eq, (eq << 1) | esc_first);
    let escaped = (fixed_eq << 1) | esc_first;

    let entry_line_start = (state.state == DecoderState::CrLf) as u64;
    let (raw_cr, escaped_cr, raw_breaks, crlf, line_start, dot_start);
    if mask == eq {
        // No line breaks in the window; the only possible line start (and
        // stripped dot) is at bit 0, carried in through the entry state.
        raw_cr = 0;
        escaped_cr = 0;
        raw_breaks = 0;
        crlf = 0;
        line_start = entry_line_start;
        dot_start = dot0 as u64;
    } else {
        // Breaks present: fold '\r' and '.' the same combined way and derive
        // '\n' from the specials mask.
        let cr_a = unsafe { vceqq_u8(a, constants.cr) };
        let cr_b = unsafe { vceqq_u8(b, constants.cr) };
        let cr_c = unsafe { vceqq_u8(c, constants.cr) };
        let cr_d = unsafe { vceqq_u8(d, constants.cr) };
        let dot_a = unsafe { vceqq_u8(a, constants.dot) };
        let dot_b = unsafe { vceqq_u8(b, constants.dot) };
        let dot_c = unsafe { vceqq_u8(c, constants.dot) };
        let dot_d = unsafe { vceqq_u8(d, constants.dot) };
        let merged = unsafe {
            vpaddq_u8(
                vpaddq_u8(
                    vpaddq_u8(
                        vandq_u8(cr_a, constants.bit_weights),
                        vandq_u8(cr_b, constants.bit_weights),
                    ),
                    vpaddq_u8(
                        vandq_u8(cr_c, constants.bit_weights),
                        vandq_u8(cr_d, constants.bit_weights),
                    ),
                ),
                vpaddq_u8(
                    vpaddq_u8(
                        vandq_u8(dot_a, constants.bit_weights),
                        vandq_u8(dot_b, constants.bit_weights),
                    ),
                    vpaddq_u8(
                        vandq_u8(dot_c, constants.bit_weights),
                        vandq_u8(dot_d, constants.bit_weights),
                    ),
                ),
            )
        };
        let cr = unsafe { vgetq_lane_u64::<0>(vreinterpretq_u64_u8(merged)) };
        let dot_mask = unsafe { vgetq_lane_u64::<1>(vreinterpretq_u64_u8(merged)) };
        let lf = mask & !eq & !cr;
        raw_cr = cr & !escaped;
        escaped_cr = cr & escaped;
        let raw_lf = lf & !escaped;
        raw_breaks = raw_cr | raw_lf;
        // NNTP line boundaries exist in the raw stream even when yEnc escaped
        // the '\r' (the scalar machine re-enters Cr after an escaped CR when
        // dot-unstuffing), so pair detection uses the unmasked '\r' bits.
        let pair_cr = if dot_unstuffing { cr } else { raw_cr };
        crlf = pair_cr & (lf >> 1);
        line_start = entry_line_start | (crlf << 2);
        dot_start = if dot_unstuffing {
            (dot_mask & !escaped & line_start) | dot0 as u64
        } else {
            0
        };
    }

    // '=' at a line start is a potential control line ("=y…"). Confirm with a
    // one-byte lookahead and fall back only for a real control line (once per
    // article, at the =yend trailer).
    if search_end && dot_unstuffing {
        let mut line_start_eq = fixed_eq & line_start;
        while line_start_eq != 0 {
            let bit = line_start_eq.trailing_zeros() as usize;
            if input[block_src + bit + 1] == b'y' {
                return Ok(SpanBlockOutcome::ScalarThrough(block_src + bit));
            }
            line_start_eq &= line_start_eq - 1;
        }
    }

    if dot_start != 0 {
        // A line-start dot immediately before a break or '=' needs
        // terminator/control lookahead; hand it to the scalar state machine.
        let hazards = dot_start & ((raw_breaks >> 1) | (eq >> 1));
        if hazards != 0 {
            return Ok(SpanBlockOutcome::ScalarThrough(
                block_src + hazards.trailing_zeros() as usize,
            ));
        }
    }

    let skip = fixed_eq | raw_breaks | dot_start;

    let decoded = if escaped == 0 {
        [
            unsafe { vsubq_u8(a, constants.normal_offset) },
            unsafe { vsubq_u8(b, constants.normal_offset) },
            unsafe { vsubq_u8(c, constants.normal_offset) },
            unsafe { vsubq_u8(d, constants.normal_offset) },
        ]
    } else if esc_first == 0 && eq & (eq << 1) == 0 {
        // No adjacent '=' and no carried-in escape: escaped positions are
        // exactly the '=' compares shifted one lane, so the offset select
        // never leaves the vector domain (the shortcut path).
        let zero = unsafe { vdupq_n_u8(0) };
        let sel_a = unsafe { vextq_u8::<15>(zero, eq_a) };
        let sel_b = unsafe { vextq_u8::<15>(eq_a, eq_b) };
        let sel_c = unsafe { vextq_u8::<15>(eq_b, eq_c) };
        let sel_d = unsafe { vextq_u8::<15>(eq_c, eq_d) };
        [
            unsafe {
                vsubq_u8(
                    a,
                    vbslq_u8(sel_a, constants.escaped_offset, constants.normal_offset),
                )
            },
            unsafe {
                vsubq_u8(
                    b,
                    vbslq_u8(sel_b, constants.escaped_offset, constants.normal_offset),
                )
            },
            unsafe {
                vsubq_u8(
                    c,
                    vbslq_u8(sel_c, constants.escaped_offset, constants.normal_offset),
                )
            },
            unsafe {
                vsubq_u8(
                    d,
                    vbslq_u8(sel_d, constants.escaped_offset, constants.normal_offset),
                )
            },
        ]
    } else {
        // Invalid '=' chains ("==", "==="): expand the chain-resolved mask
        // through the table path.
        unsafe { neon_decode_with_escape_mask([a, b, c, d], escaped, constants) }
    };

    let keeps = per_group_keeps(skip);
    unsafe {
        compact_store_16(
            decoded[0],
            (skip & 0xffff) as u16,
            keeps.0,
            table,
            output,
            dst,
        );
        compact_store_16(
            decoded[1],
            ((skip >> 16) & 0xffff) as u16,
            keeps.1,
            table,
            output,
            dst,
        );
        compact_store_16(
            decoded[2],
            ((skip >> 32) & 0xffff) as u16,
            keeps.2,
            table,
            output,
            dst,
        );
        compact_store_16(
            decoded[3],
            ((skip >> 48) & 0xffff) as u16,
            keeps.3,
            table,
            output,
            dst,
        );
    }

    let mut next_state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
    if fixed_eq & (1 << 63) != 0 {
        // Escape at the window's last byte: its partner is in the next
        // window; carry through the state machine (the escFirst bit).
        next_state = if dot_unstuffing && line_start & (1 << 63) != 0 {
            DecoderState::CrLfEq
        } else {
            DecoderState::Eq
        };
    } else if dot_start & (1 << 63) != 0 {
        // Line-start dot at the last byte: it is stripped either way; the
        // state machine resolves terminator vs stuffed data on the next byte.
        next_state = DecoderState::CrLfDot;
    } else if dot_unstuffing && escaped_cr & (1 << 63) != 0 {
        // Escaped CR at the last byte still opens an NNTP line boundary.
        next_state = DecoderState::Cr;
    }
    state.state = next_state;
    *src += 64;
    Ok(SpanBlockOutcome::Consumed)
}

#[cfg(target_arch = "aarch64")]
// NEON64 maskEqTemp/vqtbl escaped-byte offset path.
#[inline(always)]
pub(super) unsafe fn neon_decode_with_escape_mask(
    vectors: [std::arch::aarch64::uint8x16_t; 4],
    escaped: u64,
    constants: &Neon64Constants,
) -> [std::arch::aarch64::uint8x16_t; 4] {
    use std::arch::aarch64::*;

    let mut mask = unsafe { vreinterpretq_u8_u64(vdupq_n_u64(escaped)) };
    let mask_a = unsafe { vtstq_u8(vqtbl1q_u8(mask, constants.selector), constants.bit_weights) };
    mask = unsafe { vextq_u8::<2>(mask, mask) };
    let mask_b = unsafe { vtstq_u8(vqtbl1q_u8(mask, constants.selector), constants.bit_weights) };
    mask = unsafe { vextq_u8::<2>(mask, mask) };
    let mask_c = unsafe { vtstq_u8(vqtbl1q_u8(mask, constants.selector), constants.bit_weights) };
    mask = unsafe { vextq_u8::<2>(mask, mask) };
    let mask_d = unsafe { vtstq_u8(vqtbl1q_u8(mask, constants.selector), constants.bit_weights) };

    [
        unsafe {
            vsubq_u8(
                vectors[0],
                vbslq_u8(mask_a, constants.escaped_offset, constants.normal_offset),
            )
        },
        unsafe {
            vsubq_u8(
                vectors[1],
                vbslq_u8(mask_b, constants.escaped_offset, constants.normal_offset),
            )
        },
        unsafe {
            vsubq_u8(
                vectors[2],
                vbslq_u8(mask_c, constants.escaped_offset, constants.normal_offset),
            )
        },
        unsafe {
            vsubq_u8(
                vectors[3],
                vbslq_u8(mask_d, constants.escaped_offset, constants.normal_offset),
            )
        },
    ]
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(super) unsafe fn compact_store_16(
    decoded: std::arch::aarch64::uint8x16_t,
    skip_mask: u16,
    keep: usize,
    table: &[[u8; 16]; 32768],
    output: &mut [u8],
    dst: &mut usize,
) {
    use std::arch::aarch64::*;

    // The caller guarantees 64 spare output bytes per block, so each of the
    // four stores can write a full 16-byte vector; bytes past `keep` are
    // overwritten by the next store.
    debug_assert!(output.len().saturating_sub(*dst) >= 16);
    debug_assert_eq!(keep, 16 - skip_mask.count_ones() as usize);
    let shuffle = unsafe { vld1q_u8(table[(skip_mask & 0x7fff) as usize].as_ptr()) };
    let packed = unsafe { vqtbl1q_u8(decoded, shuffle) };
    unsafe { vst1q_u8(output.as_mut_ptr().add(*dst), packed) };
    *dst += keep;
}

/// NEON implementation for aarch64: process 16 bytes at a time.
#[cfg(target_arch = "aarch64")]
pub(super) unsafe fn decode_normal_run_neon(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::aarch64::*;

    let mut src = start;
    let mut dst = dst_start;

    unsafe {
        let special_eq = vdupq_n_u8(b'=');
        let special_cr = vdupq_n_u8(b'\r');
        let special_lf = vdupq_n_u8(b'\n');
        let sub42 = vdupq_n_u8(42u8.wrapping_neg());

        while src + 16 <= input.len() && dst + 16 <= output.len() {
            let chunk = vld1q_u8(input.as_ptr().add(src));

            let eq_mask = vceqq_u8(chunk, special_eq);
            let cr_mask = vceqq_u8(chunk, special_cr);
            let lf_mask = vceqq_u8(chunk, special_lf);
            let any_special = vorrq_u8(vorrq_u8(eq_mask, cr_mask), lf_mask);

            let max_val = vmaxvq_u8(any_special);
            if max_val != 0 {
                let mask64 = vreinterpretq_u64_u8(any_special);
                let low = vgetq_lane_u64(mask64, 0);
                let high = vgetq_lane_u64(mask64, 1);
                let count = if low != 0 {
                    (low.trailing_zeros() / 8) as usize
                } else {
                    8 + (high.trailing_zeros() / 8) as usize
                };

                if count > 0 {
                    let decoded = vaddq_u8(chunk, sub42);
                    let mut tmp = [0u8; 16];
                    vst1q_u8(tmp.as_mut_ptr(), decoded);
                    output[dst..dst + count].copy_from_slice(&tmp[..count]);
                    src += count;
                    dst += count;
                }
                break;
            }

            let decoded = vaddq_u8(chunk, sub42);
            vst1q_u8(output.as_mut_ptr().add(dst), decoded);
            src += 16;
            dst += 16;
        }
    }

    let (extra_src, extra_dst) = decode_normal_run_scalar(input, src, output, dst);
    (src - start + extra_src, dst - dst_start + extra_dst)
}
