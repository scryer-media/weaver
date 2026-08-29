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

    // Head resolution (search_end only), the analogue of the oracle's
    // `_do_decode_simd` entry switch (decoder_common.h:52-121). A
    // terminator/control sequence that straddles the chunk entry has its
    // `\r\n` in the PREVIOUS chunk, so the flat loop below — which only ever
    // matches raw bytes inside its own window — cannot see it. Resolve those
    // entry shapes with the verified scalar machine first; it runs once per
    // chunk and costs a handful of steps at most.
    if search_end && dot_unstuffing {
        while src < input.len() && state.end == DecodeEnd::None {
            let byte = input[src];
            let needs_scalar = match state.state {
                DecoderState::CrLfDot | DecoderState::CrLfDotCr | DecoderState::CrLfEq => true,
                // `\r\n` + `.`(`\r`|`\n`|`=`) → terminator/control candidate;
                // `\r\n=` → `=y` control candidate.
                DecoderState::CrLf => {
                    (byte == b'.' && matches!(input.get(src + 1), Some(b'\r' | b'\n' | b'=')))
                        || byte == b'='
                }
                // `\r` + `\n`(`.`|`=`) re-enters the two cases above one byte in.
                DecoderState::Cr => {
                    byte == b'\n' && matches!(input.get(src + 1), Some(b'.' | b'='))
                }
                _ => false,
            };
            if !needs_scalar {
                break;
            }
            if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
                break;
            }
        }
        if state.end != DecodeEnd::None {
            return Ok(KernelOutcome {
                consumed: src,
                written: dst,
                end: state.end.into(),
            });
        }
    }

    // Hot path: faithful port of rapidyenc `do_decode_neon<isRaw=true>`
    // (decoder_neon64.cc) — the flat, register-carried decode loop, mirroring
    // `decode_kernel_avx2_raw` / `decode_kernel_avx512_raw`. Both `searchEnd`
    // instantiations live there; only non-raw, short, or (search_end aside) an
    // entry state the head resolution doesn't cover keep the general
    // scaffolding driver below.
    if dot_unstuffing
        && input.len() - src > WIDTH * 2
        && matches!(
            state.state,
            DecoderState::None | DecoderState::Eq | DecoderState::Cr | DecoderState::CrLf
        )
    {
        // `src`/`dst` are always 0 here unless the search_end head loop ran, so
        // the `::<false>` instantiation sees the untouched full buffers.
        let outcome = if search_end {
            unsafe {
                decode_kernel_neon64_raw::<true>(&input[src..], &mut output[dst..], state, mode)
            }
        } else {
            unsafe { decode_kernel_neon64_raw::<false>(input, output, state, mode) }
        }
        .map_err(|err| match err {
            // Offsets reported by the sliced kernel are chunk-relative.
            YencError::MalformedEscape(at) => YencError::MalformedEscape(at + src),
            other => other,
        })?;
        return Ok(KernelOutcome {
            consumed: src + outcome.consumed,
            written: dst + outcome.written,
            end: outcome.end,
        });
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

/// Faithful port of rapidyenc `do_decode_neon<isRaw=true, searchEnd=SEARCH_END>`
/// (decoder_neon64.cc): the flat, register-carried decode loop over 64-byte
/// windows (4× `uint8x16`). Structurally a 1:1 clone of
/// [`decode_kernel_avx2_raw`](super::x86_avx2) / `decode_kernel_avx512_raw`,
/// differing only in the vector ops. The scalar `u64` bit-math (`fix_eq_mask`,
/// `escaped`, `esc_first`, `skip`, entry/exit state) is byte-identical to those
/// tiers, so all three share the same correctness envelope.
///
/// Register-carried state (oracle → here):
/// - `escFirst` → `esc_first: u64`
/// - `yencOffset` (byte0 = 106 on a carried escape, else 42; lanes 1..15 = 42)
///   → NOT carried. It is a pure function of `esc_first`, so the two arms that
///   need it derive it at the point of use and the clean window touches only
///   the `dup(42)` constant (see the note in the body — the carried form is a
///   measured +56% cliff in the `SEARCH_END = true` instantiation).
/// - `nextMask`/`minMask` → `next_mask_mix: uint8x16_t`. Unlike AVX2/VBMI2
///   (which clamp via `min_epu8` + a `min_mask`), NEON keeps `.` OUT of the
///   specials LUT and injects a line-start dot by OR-ing `next_mask_mix` into
///   `cmp_a` after the `vqtbx1q` merge (oracle line 96). It is consumed exactly
///   once per window and recomputed (or zeroed) inside the `\r\n.` sub-branch.
///
/// With `SEARCH_END`, the `\r\n.` sub-branch additionally runs the oracle's
/// terminator probe (decoder_neon64.cc:126-290) for `\r\n.\r\n`, `\r\n.=y` and
/// `\r\n=y`. On a hit the window is NOT consumed: the exit state is set from
/// the no-backtrack `decoder_set_nextMask` rule (decoder_common.h:129-132,
/// :190-199) and the scalar drain re-scans the window as the authority on the
/// exact `end`/`consumed` split — the oracle's `len += i; break;` epilogue.
#[cfg(target_arch = "aarch64")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn decode_kernel_neon64_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    use std::arch::aarch64::*;
    const WIDTH: usize = 64;

    // Both are materialised from `i` / the running output pointer once the SIMD
    // loop has run; the loop itself never touches them.
    let mut src: usize;
    let mut dst: usize;
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

    // The oracle's `yencOffset` (byte0 = 106 = 42+64 while a `=` straddled in
    // from the previous window, else 42; lanes 1..15 always 42, oracle line 58)
    // is deliberately NOT a loop-carried vector here. It is a pure function of
    // the carried scalar `esc_first`, so the windows that need it — an escape
    // carried into a specials-free window, or an isolated escape resolved this
    // window — derive it where they use it, and the common clean window touches
    // only the `normal_offset` constant. Measured reason (round 3): the carried
    // vector sits exactly at the `SEARCH_END = true` instantiation's register
    // budget; any change that adds one live vector evicts it into a
    // `str q`/`ldr q` pair on the clean window's dependency path (+56% on
    // `clean until_end`). A value that is never carried cannot be evicted; the
    // per-window always-derive form was also measured (+5.3% on clean, from
    // rebuilding it on every window) and is what the gate below avoids.
    //
    // next_mask_mix: a carried line-start dot. Values 1/2 survive the
    // `& bit_weights` reduction at lanes 0/1 (oracle lines 53-57).
    let mut next_mask_mix = match entry_next_mask {
        1 => vsetq_lane_u8::<0>(1, zero),
        2 => vsetq_lane_u8::<1>(2, zero),
        _ => zero,
    };

    // Set when the SEARCH_END probe aborts a window (oracle `len += i; break;`):
    // the window is left unconsumed and the exit state comes from the
    // no-backtrack rule instead of the trailing-bytes lookback.
    let mut broke = false;

    // Carry-forward end-candidate state (SEARCH_END only). A `\r\n=y` control
    // sequence whose `\r` sits in the last three bytes of a window cannot be
    // decided from that window's own 64 bytes. Rather than peek into the next
    // window (the load this rewrite exists to delete), classify the tail here
    // and re-test it against the next window's first bytes at the top of the
    // following iteration, before that window is emitted:
    //   window ended `\r\n=` → needs `y`    → resumes in CrLfEq
    //   window ended `\r\n`  → needs `=y`   → resumes in CrLf
    //   window ended `\r`    → needs `\n=y` → resumes in Cr
    //
    // The classification is deliberately byte-literal, matching the vector
    // probe it replaces: an `=`-escaped `\r` still opens a line here, because
    // `Eq` + `\r` transitions to `Cr` (scalar.rs; rapidyenc does the same), so
    // `=\r\n=y` IS a terminator. Vetoing escaped tail bytes was tried and the
    // C-oracle differential caught it. The other two shapes cannot involve an
    // escaped byte at all: `\n` at 63 escaped forces `=` at 62 (so byte 62 is
    // not `\r`), and `=` at 63 escaped forces `=` at 62 (not `\n`).
    //
    // The tag is a single bit at the position of the opening `\r` — bit
    // 61/62/63 — which lets the loop-top dispatch be `cbz` + two `tbnz`
    // instead of three equality compares LLVM would hoist above the zero test.
    // (A packed (mask, expected-word) carry compared against one unaligned
    // `u32` load of the next window's head was also tried: it re-introduces a
    // scalar load from `input` into the loop body, which has to be ordered
    // against the output stores, and measured worse — realshape tax 1.194x ->
    // 1.307x.)
    //
    // Invariant: the carry is consumed-and-cleared at the top of every
    // iteration, so the classification below always writes into a zero.
    const PEND_EQ: u64 = 1 << 61; // `\r\n=` seen; needs `y`
    const PEND_CRLF: u64 = 1 << 62; // `\r\n` seen; needs `=y`
    const PEND_CR: u64 = 1 << 63; // `\r` seen; needs `\n=y`
    let mut pending_tail: u64 = 0;

    // Negative induction + running pointers — the oracle's loop shape
    // (`for(i = -len; i; i += 64)`, decoder_neon64.cc:62), and the NEON twin of
    // the AVX2 raw kernel's `span`/`sp`/`i` rewrite.
    //
    // The index form (`src`/`dst` counters plus `base + index` per access) costs
    // seven extra instructions on EVERY window — two address adds, `dst += 64`,
    // `src + 64`, `src + 128`, the cursor copy, and a `cmp` the counter form
    // folds into its `subs`. That is the whole clean-path deficit; measured on
    // three microarchitectures it tracks 7 instructions divided by the core's
    // decode width.
    //
    // `span` is the exact byte count covered by whole windows, so the last
    // window still has the full 67-byte tail reserve behind it. DO NOT round
    // `span` up: one window over happens to be benign because the reserve is
    // exactly one window of slack, two windows over decodes wrong.
    let span = if input.len() > WIDTH * 2 {
        (simd_limit / WIDTH) * WIDTH
    } else {
        0
    };
    let sp = input.as_ptr().add(span);
    let out_base = output.as_mut_ptr();
    let mut out = out_base;
    let mut i: isize = -(span as isize);

    // On Neoverse-N1 the SEARCH_END = false span runs the frozen assembly
    // kernel (see `n1_span` at the end of this file); it consumes the whole
    // span (`i` -> 0), so the Rust loop below no-ops at runtime and the shared
    // exit glue takes over unchanged. Every other core — Apple silicon in
    // particular — keeps the Rust loop.
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    if !SEARCH_END && i != 0 && n1_span::engaged() {
        n1_span::span(
            sp,
            &mut i,
            &mut out,
            &mut esc_first,
            entry_next_mask,
            table.as_ptr() as *const u8,
        );
    }

    // The SEARCH_END = true twin. Kind protocol: 0 span done; 1
    // terminator break (no-backtrack state from the exported mask); 2/3/4
    // pending-tail resume (Cr/CrLf/CrLfEq); 5 = a rare dot-window terminator
    // candidate — the window is left unconsumed and the Rust loop below
    // reprocesses it with its full probe, then finishes the span.
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    if SEARCH_END && i != 0 && n1_span::engaged() {
        let mut mask_out: u64 = 0;
        let mut kind: u64 = 0;
        let mut nmm_buf = [0u8; 16];
        n1_span_se::span_se(
            sp,
            &mut i,
            &mut out,
            &mut esc_first,
            &mut pending_tail,
            &mut mask_out,
            &mut kind,
            entry_next_mask,
            &mut nmm_buf,
            table.as_ptr() as *const u8,
        );
        match kind {
            1 => {
                state.state =
                    neon64_break_state(input, (span as isize + i) as usize, mask_out, esc_first);
                broke = true;
            }
            2 => {
                state.state = DecoderState::Cr;
                broke = true;
            }
            3 => {
                state.state = DecoderState::CrLf;
                broke = true;
            }
            4 => {
                state.state = DecoderState::CrLfEq;
                broke = true;
            }
            5 => next_mask_mix = vld1q_u8(nmm_buf.as_ptr()),
            _ => {}
        }
    }

    if !broke {
        while i != 0 {
            // Straddle resolution for the previous window's tail classification.
            // `src + 2 < input.len()` holds by the loop bound (src + 131 <=
            // input.len()). Everything before `src` is already consumed and
            // emitted, and every byte of the pending prefix (`\r`, `\n`, `=`)
            // is a mask bit — i.e. skipped, never written — so breaking here
            // leaves a consistent (src, dst, state) triple for the scalar
            // drain, exactly like the in-window no-backtrack break.
            if SEARCH_END && pending_tail != 0 {
                // Bit-test dispatch (`tbnz`), not equality against the three
                // tags: equality lets LLVM hoist the compares above the
                // zero test and pay them on every window.
                let resumed = if pending_tail & PEND_CR != 0 {
                    (*sp.offset(i) == b'\n'
                        && *sp.offset(i + 1) == b'='
                        && *sp.offset(i + 2) == b'y')
                        .then_some(DecoderState::Cr)
                } else if pending_tail & PEND_CRLF != 0 {
                    (*sp.offset(i) == b'=' && *sp.offset(i + 1) == b'y')
                        .then_some(DecoderState::CrLf)
                } else {
                    (*sp.offset(i) == b'y').then_some(DecoderState::CrLfEq)
                };
                if let Some(resumed) = resumed {
                    state.state = resumed;
                    broke = true;
                    break;
                }
                pending_tail = 0;
            }

            // Anchor every in-window address on the window base. Deriving the
            // +49..+52 lookaheads straight off `sp.offset(i)` lets LLVM's
            // induction-variable rewrite pick the *lookahead* as the loop's
            // base register, which leaves the four window loads at offsets
            // -0x32/-0x22/-0x12/-0x2 — not multiples of 16, so `ldp` cannot
            // form and the window costs four `ldur` instead of two `ldp`.
            let win = sp.offset(i);
            let data = vld1q_u8_x4(win);
            let (a, b, c, d) = (data.0, data.1, data.2, data.3);

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
            let any_bits = neon64_any_bits(any);
            // Common window: no specials AND no escape carried in. Only the
            // `normal_offset` constant is touched, so no vector value is
            // loop-carried through this path (see the `yencOffset` note above).
            // The `| esc_first` is one scalar `orr` off the vector dependency
            // chain; `esc_first` is already 0 here, so it needs no reset.
            if (any_bits | esc_first) == 0 {
                vst1q_u8_x4(
                    out,
                    uint8x16x4_t(
                        vsubq_u8(a, normal_offset),
                        vsubq_u8(b, normal_offset),
                        vsubq_u8(c, normal_offset),
                        vsubq_u8(d, normal_offset),
                    ),
                );
                out = out.add(WIDTH);
            } else if any_bits != 0 {
                // Fused bit-weight reduction: lane 0 → specials mask, lane 1 →
                // `=` mask (oracle lines 102-125).
                let merged = neon64_addp(
                    neon64_addp(
                        neon64_addp(
                            vandq_u8(cmp_a, constants.bit_weights),
                            vandq_u8(cmp_b, constants.bit_weights),
                        ),
                        neon64_addp(
                            vandq_u8(cmp_c, constants.bit_weights),
                            vandq_u8(cmp_d, constants.bit_weights),
                        ),
                    ),
                    neon64_addp(
                        neon64_addp(
                            vandq_u8(eq_a, constants.bit_weights),
                            vandq_u8(eq_b, constants.bit_weights),
                        ),
                        neon64_addp(
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
                    // The +2 dot lookahead. The oracle (and this port until now)
                    // derived it under SEARCH_END from a load of the WHOLE next
                    // window, because the terminator probe needed that window
                    // anyway (oracle lines 130-137). With the probe moved into
                    // mask space the next-window load has no unconditional
                    // user, so both instantiations now take the same
                    // overlapping 16-byte load: `vld1q(src+50)` is
                    // bit-identical to `vext::<2>(d, next_data)` and costs one
                    // instruction instead of a load plus an `ext`.
                    // In bounds: the loop bound gives `src + 131 <= len`, and
                    // the deepest such view below reads `src + 67`.
                    let tmp2 = vld1q_u8(win.add(50));
                    let cr_a = vceqq_u8(a, constants.cr);
                    let cr_b = vceqq_u8(b, constants.cr);
                    let cr_c = vceqq_u8(c, constants.cr);
                    let cr_d = vceqq_u8(d, constants.cr);
                    let m2cr_a = vandq_u8(cr_a, vceqq_u8(vextq_u8::<2>(a, b), constants.dot));
                    let m2cr_b = vandq_u8(cr_b, vceqq_u8(vextq_u8::<2>(b, c), constants.dot));
                    let m2cr_c = vandq_u8(cr_c, vceqq_u8(vextq_u8::<2>(c, d), constants.dot));
                    let m2cr_d = vandq_u8(cr_d, vceqq_u8(tmp2, constants.dot));
                    let m2cr_any = vorrq_u8(vorrq_u8(m2cr_a, m2cr_b), vorrq_u8(m2cr_c, m2cr_d));
                    if neon64_any(m2cr_any) {
                        let lf_a = vceqq_u8(vextq_u8::<1>(a, b), constants.lf);
                        let lf_b = vceqq_u8(vextq_u8::<1>(b, c), constants.lf);
                        let lf_c = vceqq_u8(vextq_u8::<1>(c, d), constants.lf);
                        let lf_d = vceqq_u8(vld1q_u8(win.add(49)), constants.lf);
                        let m2nldot_a = vandq_u8(m2cr_a, lf_a);
                        let m2nldot_b = vandq_u8(m2cr_b, lf_b);
                        let m2nldot_c = vandq_u8(m2cr_c, lf_c);
                        let m2nldot_d = vandq_u8(m2cr_d, lf_d);

                        // Terminator probe with a stuffed dot in the window
                        // (oracle lines 174-236): `\r\n.\r\n`, `\r\n.=y` and
                        // `\r\n=y`.
                        if SEARCH_END {
                            let y = vdupq_n_u8(b'y');
                            let eq_y = vdupq_n_u16(0x793d);

                            // `\r\n` at lane i.
                            let m1nl_a = vandq_u8(lf_a, cr_a);
                            let m1nl_b = vandq_u8(lf_b, cr_b);
                            let m1nl_c = vandq_u8(lf_c, cr_c);
                            let m1nl_d = vandq_u8(lf_d, cr_d);

                            // `vext::<3|4>(d, next_data)` written as the
                            // equivalent overlapping loads (bytes 51..66 /
                            // 52..67), so no next-window register is needed.
                            let tmp3 = vld1q_u8(win.add(51));
                            let tmp4 = vld1q_u8(win.add(52));

                            // `\r\n` at lane i+3 (closing an article terminator).
                            let m4nl_a = vextq_u8::<3>(m1nl_a, m1nl_b);
                            let m4nl_b = vextq_u8::<3>(m1nl_b, m1nl_c);
                            let m4nl_c = vextq_u8::<3>(m1nl_c, m1nl_d);
                            let m4nl_d = vandq_u8(
                                vceqq_u8(tmp3, constants.cr),
                                vceqq_u8(tmp4, constants.lf),
                            );

                            // `=y` at lane i+4, u16-aligned (even lanes only).
                            let m4eqy_a = vreinterpretq_u8_u16(vceqq_u16(
                                vreinterpretq_u16_u8(vextq_u8::<4>(a, b)),
                                eq_y,
                            ));
                            let m4eqy_b = vreinterpretq_u8_u16(vceqq_u16(
                                vreinterpretq_u16_u8(vextq_u8::<4>(b, c)),
                                eq_y,
                            ));
                            let m4eqy_c = vreinterpretq_u8_u16(vceqq_u16(
                                vreinterpretq_u16_u8(vextq_u8::<4>(c, d)),
                                eq_y,
                            ));
                            let m4eqy_d =
                                vreinterpretq_u8_u16(vceqq_u16(vreinterpretq_u16_u8(tmp4), eq_y));

                            // `=y` at lane i+2.
                            let m2eq_a = vextq_u8::<2>(eq_a, eq_b);
                            let m2eq_b = vextq_u8::<2>(eq_b, eq_c);
                            let m2eq_c = vextq_u8::<2>(eq_c, eq_d);
                            let m2eq_d = vceqq_u8(tmp2, constants.eq);
                            let m3eqy_a = vandq_u8(vceqq_u8(vextq_u8::<3>(a, b), y), m2eq_a);
                            let m3eqy_b = vandq_u8(vceqq_u8(vextq_u8::<3>(b, c), y), m2eq_b);
                            let m3eqy_c = vandq_u8(vceqq_u8(vextq_u8::<3>(c, d), y), m2eq_c);
                            let m3eqy_d = vandq_u8(vceqq_u8(tmp3, y), m2eq_d);

                            // `vsri`(m4eqy, m3eqy, 8) reads "`=y` at lane i+3"
                            // at every lane: odd lanes take the u16-aligned
                            // i+4 match, even lanes take the i+2 match of lane
                            // i+1 shifted down a byte (oracle lines 218-221).
                            let end4_a = vandq_u8(
                                vorrq_u8(m4nl_a, neon64_sri8(m4eqy_a, m3eqy_a)),
                                m2nldot_a,
                            );
                            let end4_b = vandq_u8(
                                vorrq_u8(m4nl_b, neon64_sri8(m4eqy_b, m3eqy_b)),
                                m2nldot_b,
                            );
                            let end4_c = vandq_u8(
                                vorrq_u8(m4nl_c, neon64_sri8(m4eqy_c, m3eqy_c)),
                                m2nldot_c,
                            );
                            let end4_d = vandq_u8(
                                vorrq_u8(m4nl_d, neon64_sri8(m4eqy_d, m3eqy_d)),
                                m2nldot_d,
                            );
                            // `\r\n=y`.
                            let end3_a = vandq_u8(m3eqy_a, m1nl_a);
                            let end3_b = vandq_u8(m3eqy_b, m1nl_b);
                            let end3_c = vandq_u8(m3eqy_c, m1nl_c);
                            let end3_d = vandq_u8(m3eqy_d, m1nl_d);

                            let any_end = vorrq_u8(
                                vorrq_u8(vorrq_u8(end4_a, end4_b), vorrq_u8(end4_c, end4_d)),
                                vorrq_u8(vorrq_u8(end3_a, end3_b), vorrq_u8(end3_c, end3_d)),
                            );
                            if neon64_any(any_end) {
                                state.state = neon64_break_state(
                                    input,
                                    (span as isize + i) as usize,
                                    mask,
                                    esc_first,
                                );
                                broke = true;
                                break;
                            }
                        }

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
                        // Terminator probe without a stuffed dot in the window
                        // (oracle lines 253-287): only `\r\n=y` is reachable —
                        // any `\r\n.` shape would have set `m2cr_any`.
                        //
                        // This is the hot branch: on real bodies it runs on
                        // roughly every second window (any window carrying a
                        // line break), while a terminator occurs once per
                        // article. The oracle's shape — build the `=`/`y`/`\n`
                        // views by `vext`-shifting compare vectors, then reduce
                        // — pays ~24 vector instructions on every one of those
                        // windows to answer a question that is almost always
                        // "no". Answer it first in scalar mask space instead,
                        // for the price of a handful of ALU ops, and only run
                        // the vector resolution when a candidate exists.
                        //
                        // `mask` is the specials mask (`=` | `\r` | `\n` | a
                        // carried line-start dot at bits 0/1) and `mask_eq` the
                        // `=` mask, both bit j <-> byte j, with `mask ⊇
                        // mask_eq`. A trailer can only begin where `\r\n` is
                        // immediately followed by `=`, so bit i of
                        //     mask & (mask >> 1) & (mask_eq >> 2)
                        // is a superset of "an end sequence starts at byte i".
                        // The `\r\n.\r\n` / `\r\n.=y` shapes need a `\r` two
                        // bytes ahead of a `.`, which is exactly `m2cr_any` —
                        // false on this branch — so no dot term is required.
                        //
                        // The two shifted terms are OR'd with their vacated top
                        // bits so the same expression also flags the shapes that
                        // run off the end of the window — bit 62 = two adjacent
                        // specials at 62/63, bit 63 = a special at 63 — which is
                        // where the tail carry is classified. One test, one
                        // branch, covering both.
                        //
                        // Being a superset is the point: it also fires on
                        // `\n\r=`, `\r\r=`, `\n\n=` and on a plain line-start
                        // escape, so a hit falls through to the unchanged vector
                        // resolution, which on a false candidate resolves to "no
                        // end" and resumes decoding exactly as before.
                        let cand = if SEARCH_END {
                            mask & ((mask >> 1) | 1 << 63) & ((mask_eq >> 2) | 3 << 62)
                        } else {
                            0
                        };
                        if cand != 0 {
                            // Bits 0..=61: an end sequence may start inside this
                            // window. `cand << 2` drops the two straddle bits.
                            if cand << 2 != 0 {
                                let y = vdupq_n_u8(b'y');
                                let m2eq_a = vextq_u8::<2>(eq_a, eq_b);
                                let m2eq_b = vextq_u8::<2>(eq_b, eq_c);
                                let m2eq_c = vextq_u8::<2>(eq_c, eq_d);
                                let m2eq_d = vceqq_u8(tmp2, constants.eq);
                                let m3eqy_a = vandq_u8(m2eq_a, vceqq_u8(vextq_u8::<3>(a, b), y));
                                let m3eqy_b = vandq_u8(m2eq_b, vceqq_u8(vextq_u8::<3>(b, c), y));
                                let m3eqy_c = vandq_u8(m2eq_c, vceqq_u8(vextq_u8::<3>(c, d), y));
                                let m3eqy_d = vandq_u8(m2eq_d, vceqq_u8(vld1q_u8(win.add(51)), y));
                                let any_eqy = vorrq_u8(
                                    vorrq_u8(m3eqy_a, m3eqy_b),
                                    vorrq_u8(m3eqy_c, m3eqy_d),
                                );
                                if neon64_any(any_eqy) {
                                    let lf_a = vceqq_u8(vextq_u8::<1>(a, b), constants.lf);
                                    let lf_b = vceqq_u8(vextq_u8::<1>(b, c), constants.lf);
                                    let lf_c = vceqq_u8(vextq_u8::<1>(c, d), constants.lf);
                                    let lf_d = vceqq_u8(vld1q_u8(win.add(49)), constants.lf);
                                    let match_end = vorrq_u8(
                                        vorrq_u8(
                                            vandq_u8(m3eqy_a, vandq_u8(lf_a, cr_a)),
                                            vandq_u8(m3eqy_b, vandq_u8(lf_b, cr_b)),
                                        ),
                                        vorrq_u8(
                                            vandq_u8(m3eqy_c, vandq_u8(lf_c, cr_c)),
                                            vandq_u8(m3eqy_d, vandq_u8(lf_d, cr_d)),
                                        ),
                                    );
                                    if neon64_any(match_end) {
                                        state.state = neon64_break_state(
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

                            // Byte-exact tail classification for the carry. The
                            // three shapes need `\r` at 61 / 62 / 63, each of
                            // which sets the corresponding bit of `cand`, so
                            // this only runs when one is possible. The tag is
                            // the `escaped` bit that would falsify it; the veto
                            // itself happens once `escaped` is resolved, below.
                            if cand >> 61 != 0 {
                                let b61 = *win.add(61);
                                let b62 = *win.add(62);
                                let b63 = *win.add(63);
                                pending_tail = if b61 == b'\r' && b62 == b'\n' && b63 == b'=' {
                                    PEND_EQ
                                } else if b62 == b'\r' && b63 == b'\n' {
                                    PEND_CRLF
                                } else if b63 == b'\r' {
                                    PEND_CR
                                } else {
                                    0
                                };
                            }
                        }
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
                    // `escaped == 0` implies `esc_first_in == 0` (bit 0 of
                    // `escaped` IS `esc_first_in`), so lane A takes the plain
                    // constant too — no derived vector on this arm, which is
                    // every CRLF-only specials window.
                    [
                        vsubq_u8(a, normal_offset),
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
                    // `vext(dup42, cmpEqA, 15)` + `yencOffset` 42-bit-trick so a
                    // carried escape at byte 0 is applied via `yenc_offset`
                    // (oracle lines 360-391) — derived here from the carried-in
                    // scalar (byte 0 = 106 when an escape carried in, else 42)
                    // instead of rebuilt at window exit and carried: the same
                    // `lsl`/`orr`/`ins` the exit rebuild spent, now off the
                    // loop-carried set and only on the arm that needs it.
                    let yenc_offset =
                        vsetq_lane_u8::<0>(((esc_first_in as u8) << 6) | 42, normal_offset);
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

                if skip == 0 {
                    vst1q_u8_x4(
                        out,
                        uint8x16x4_t(decoded[0], decoded[1], decoded[2], decoded[3]),
                    );
                    out = out.add(WIDTH);
                } else {
                    // Four independent LUT-compaction stores; the entry gate +
                    // tail guarantee ≥64 spare output bytes so each 16-byte
                    // store can overwrite ahead.
                    let keeps = per_group_keeps(skip);
                    out = compact_store_16_at(
                        decoded[0],
                        (skip & 0xffff) as u16,
                        keeps.0,
                        table,
                        out,
                    );
                    out = compact_store_16_at(
                        decoded[1],
                        ((skip >> 16) & 0xffff) as u16,
                        keeps.1,
                        table,
                        out,
                    );
                    out = compact_store_16_at(
                        decoded[2],
                        ((skip >> 32) & 0xffff) as u16,
                        keeps.2,
                        table,
                        out,
                    );
                    out = compact_store_16_at(
                        decoded[3],
                        ((skip >> 48) & 0xffff) as u16,
                        keeps.3,
                        table,
                        out,
                    );
                }
            } else {
                // No specials (and no carried dot) but an escape carried in from
                // the previous window's trailing `=` (`esc_first == 1`): byte 0
                // alone decodes with 106, the rest is bulk data, retired with the
                // same single `st1 {v.16b - v.16b}, [x], #64` as the clean window
                // (the oracle's `_vst1q_u8_x4`, decoder_neon64.cc:424-427; with
                // the running `out` pointer this assembles to the
                // post-incrementing form). Rare — a `=`-terminated window
                // followed by a specials-free one — so the byte-0 offset is
                // built here rather than taxing the common clean window with it.
                let first_off = vsetq_lane_u8::<0>(42 + 64, normal_offset);
                vst1q_u8_x4(
                    out,
                    uint8x16x4_t(
                        vsubq_u8(a, first_off),
                        vsubq_u8(b, normal_offset),
                        vsubq_u8(c, normal_offset),
                        vsubq_u8(d, normal_offset),
                    ),
                );
                out = out.add(WIDTH);
                esc_first = 0;
            }
            i += WIDTH as isize;
        }
    }

    // Materialise the index pair the (cold) epilogue below works in. `i` is in
    // `-span..=0`, so both are in range and the cast cannot wrap.
    src = (span as isize + i) as usize;
    dst = out.offset_from(out_base) as usize;

    // The loop ran out of windows with a tail classification still pending, so
    // the straddle test above never got its next iteration. Resolve it here,
    // against the first bytes of the scalar region. The lookback below only
    // reconstructs a *dot-stuffing* line start (`out_next_mask`), so without
    // this a `\r\n=y` split across the last window boundary would reach the
    // scalar drain as `None` + `=y` and decode as a plain escape.
    if SEARCH_END && !broke && pending_tail != 0 {
        let at = |k: usize, want: u8| input.get(src + k) == Some(&want);
        let hit = match pending_tail {
            PEND_EQ => at(0, b'y'),
            PEND_CRLF => at(0, b'=') && at(1, b'y'),
            _ => at(0, b'\n') && at(1, b'=') && at(2, b'y'),
        };
        if hit {
            state.state = match pending_tail {
                PEND_EQ => DecoderState::CrLfEq,
                PEND_CRLF => DecoderState::CrLf,
                _ => DecoderState::Cr,
            };
            broke = true;
        }
    }

    // Derive the exit state from the trailing bytes (oracle-equivalent to the
    // AVX2/VBMI2 raw ports' out_next_mask lookback) — but ONLY when the SIMD
    // loop consumed at least one window. With no window consumed (len in
    // {129,130} => simd_limit < WIDTH), `src` is still 0 and the entry state
    // MUST survive untouched for the scalar epilogue, else a carried Cr/CrLf
    // line-start (pending stuffed dot) is clobbered to None and mis-decoded.
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

/// "Is any lane of `v` nonzero?" — the oracle's `neon_vect_is_nonzero`
/// (decoder_neon64.cc:33-35), byte for byte.
///
/// The obvious spelling, `vmaxvq_u8(v) != 0`, lowers to `umaxv` — a full
/// cross-lane horizontal max whose latency dominates the dependent branch. The
/// oracle instead narrows the 2×u64 view saturating to 2×u32 (`uqxtn`, which is
/// nonzero iff some source lane was) and reads the resulting 64-bit D register
/// out with one `fmov`. Same predicate, materially cheaper: saturating
/// narrowing keeps a nonzero u64 half nonzero (it clamps to `u32::MAX`, never
/// to 0), so the packed pair is zero exactly when `v` is.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon64_any(v: std::arch::aarch64::uint8x16_t) -> bool {
    unsafe { neon64_any_bits(v) != 0 }
}

/// The scalar behind [`neon64_any`]: the `uqxtn`-narrowed 64-bit view of `v`,
/// nonzero iff some lane of `v` is. Exposed so a caller can fold another
/// scalar predicate into the same zero test (`(bits | x) == 0`) with one `orr`
/// instead of a second compare-and-branch.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon64_any_bits(v: std::arch::aarch64::uint8x16_t) -> u64 {
    use std::arch::aarch64::*;

    unsafe { vget_lane_u64::<0>(vreinterpret_u64_u32(vqmovn_u64(vreinterpretq_u64_u8(v)))) }
}

/// The oracle's `vpaddq_u8` (decoder_neon64.cc:102-125, 240-250), which clang
/// emits as a single `addp.16b`.
///
/// Rust's `core::arch::aarch64::vpaddq_u8` is *generic IR* (two shufflevectors
/// plus an add), not the `llvm.aarch64.neon.addp` intrinsic; the AArch64 backend
/// pattern-matches `add(uzp1(a,b), uzp2(a,b))` back into `addp`. Every use in
/// this file feeds it `cmp & bit_weights` operands, whose set bits are provably
/// disjoint, so InstCombine rewrites the `add` into an `or disjoint` — and the
/// backend has no `or(uzp1, uzp2)` pattern. The result is a 3-instruction
/// `uzp1`/`uzp2`/`orr` expansion of every pairwise add: 21 instructions where
/// the oracle emits 7, on every window that contains a special character.
/// Verified in the emitted asm for both `SEARCH_END` instantiations.
///
/// Spelling it as `asm!` restores the oracle's instruction exactly. `pure` +
/// `nomem` keeps it CSE-able and hoistable, so scheduling is unaffected.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon64_addp(
    a: std::arch::aarch64::uint8x16_t,
    b: std::arch::aarch64::uint8x16_t,
) -> std::arch::aarch64::uint8x16_t {
    let out: std::arch::aarch64::uint8x16_t;
    unsafe {
        std::arch::asm!(
            "addp {out:v}.16b, {a:v}.16b, {b:v}.16b",
            out = lateout(vreg) out,
            a = in(vreg) a,
            b = in(vreg) b,
            options(pure, nomem, nostack, preserves_flags),
        );
    }
    out
}

/// `vsriq_n_u16::<8>` on byte vectors: per 16-bit lane, keep `hi`'s high byte
/// and take `lo`'s high byte as the low byte (oracle decoder_neon64.cc:220).
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon64_sri8(
    hi: std::arch::aarch64::uint8x16_t,
    lo: std::arch::aarch64::uint8x16_t,
) -> std::arch::aarch64::uint8x16_t {
    use std::arch::aarch64::*;

    unsafe {
        vreinterpretq_u8_u16(vsriq_n_u16::<8>(
            vreinterpretq_u16_u8(hi),
            vreinterpretq_u16_u8(lo),
        ))
    }
}

/// Exit state for a window the SEARCH_END probe aborted, via the oracle's
/// no-backtrack `decoder_set_nextMask` (decoder_common.h:190-199) plus the
/// driver's `escFirst`-wins mapping (decoder_common.h:129-132). `esc_first` is
/// the PRE-window carry: the aborted window is never consumed, so its own
/// escape bookkeeping has not run yet.
///
/// `src + 1 < input.len()` is guaranteed by the loop bound (`src + 131 <=
/// input.len()`).
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn neon64_break_state(input: &[u8], src: usize, mask: u64, esc_first: u64) -> DecoderState {
    if esc_first != 0 {
        return DecoderState::Eq;
    }
    if input[src] == b'.' {
        if mask & 1 != 0 {
            return DecoderState::CrLf;
        }
    } else if input[src + 1] == b'.' && mask & 2 != 0 {
        return DecoderState::Cr;
    }
    DecoderState::None
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
        neon64_addp(
            neon64_addp(
                neon64_addp(
                    vandq_u8(vectors[0], bit_weights),
                    vandq_u8(vectors[1], bit_weights),
                ),
                neon64_addp(
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
            neon64_addp(
                neon64_addp(
                    neon64_addp(
                        vandq_u8(cmp_a, constants.bit_weights),
                        vandq_u8(cmp_b, constants.bit_weights),
                    ),
                    neon64_addp(
                        vandq_u8(cmp_c, constants.bit_weights),
                        vandq_u8(cmp_d, constants.bit_weights),
                    ),
                ),
                neon64_addp(
                    neon64_addp(
                        vandq_u8(eq_a, constants.bit_weights),
                        vandq_u8(eq_b, constants.bit_weights),
                    ),
                    neon64_addp(
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
            neon64_addp(
                neon64_addp(
                    neon64_addp(
                        vandq_u8(cr_a, constants.bit_weights),
                        vandq_u8(cr_b, constants.bit_weights),
                    ),
                    neon64_addp(
                        vandq_u8(cr_c, constants.bit_weights),
                        vandq_u8(cr_d, constants.bit_weights),
                    ),
                ),
                neon64_addp(
                    neon64_addp(
                        vandq_u8(dot_a, constants.bit_weights),
                        vandq_u8(dot_b, constants.bit_weights),
                    ),
                    neon64_addp(
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

/// [`compact_store_16`] against a running output pointer instead of a
/// `(&mut [u8], &mut usize)` pair, returning the advanced cursor.
///
/// Same store, same LUT row, same overwrite-ahead contract — the caller still
/// guarantees 64 spare output bytes per window. The pair form makes LLVM
/// re-derive `base + index` for every one of the four lane stores; the pointer
/// form carries one register through, which is the shape the oracle uses
/// (`p += counts & 0xff`, decoder_neon64.cc:396-419).
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub(super) unsafe fn compact_store_16_at(
    decoded: std::arch::aarch64::uint8x16_t,
    skip_mask: u16,
    keep: usize,
    table: &[[u8; 16]; 32768],
    out: *mut u8,
) -> *mut u8 {
    use std::arch::aarch64::*;

    debug_assert_eq!(keep, 16 - skip_mask.count_ones() as usize);
    let shuffle = unsafe { vld1q_u8(table[(skip_mask & 0x7fff) as usize].as_ptr()) };
    let packed = unsafe { vqtbl1q_u8(decoded, shuffle) };
    unsafe { vst1q_u8(out, packed) };
    unsafe { out.add(keep) }
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

// ---------------------------------------------------------------------------
// Neoverse-N1 frozen span kernel (SEARCH_END = false).
//
// Maintenance contract — mirror of the AVX2 raw kernels' contract:
// `decode_kernel_neon64_raw` above remains the SOURCE OF TRUTH and the escape
// hatch (WEAVER_YENC_N1_ASM=0, or any non-N1 core — Apple silicon never enters
// here). This block freezes the emission for issue-width-bound Neoverse-N1,
// where the Rust loop's LLVM allocation pays +20% instructions and +44-86%
// branches versus the same algorithm hand-scheduled in local perf-counter
// measurements. Tune the Rust loop, measure via the escape hatch, then
// update this block if the Rust loop wins.
//
// The body is an instruction-for-instruction transliteration of rapidyenc
// 27f435a's compiled do_decode_simd<isRaw=1, searchEnd=0, 64, do_decode_neon>
// AArch64 emission, register-for-register, with these deviations:
//   1. the dot-arm's stp/ldp d12/d13 stack spills -> spare v14/v15 (nostack);
//   2. the collision block's bit-lane + 0x2a/0x6a reloads -> the already-
//      resident v17/v18/v25 (the reloads exist in the oracle only because gcc
//      spilled them around the cold block);
//   3. rodata pools -> pointer operands on Rust statics; the compact-store
//      LUT is weaver's own `compact_table_16` (identical 32768x16 layout,
//      mask<<4 indexing);
//   4. entry-state vectors v19/v26 are built by the Rust prologue from the
//      carried scalars instead of the oracle's per-state rodata forms.
// ---------------------------------------------------------------------------
#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
pub(super) mod n1_span {
    #[repr(C, align(16))]
    pub struct A16(pub [u8; 16]);
    // tbx class table: CR (13) and LF (10) -> 0xff; every byte >= 16 keeps the
    // `=` compare result (tbx semantics). `.` deliberately absent (raw-mode
    // dots enter via the carried v26 / the dot-arm only).
    pub static N1_TBX_CRLF: A16 = A16([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0, 0, 0xff, 0, 0]);
    pub static N1_BIT_LANES: A16 = A16([1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128]);
    // Byte-broadcast for the collision expansion: mask bytes 0/1 to lanes;
    // bytes 2..7 reached by ext-shifted copies of the mask register.
    pub static N1_BCAST01: A16 = A16([0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1]);

    /// Engage on Neoverse-N1 (MIDR implementer 0x41 part 0xd0c) only, with a
    /// runtime escape hatch. Apple silicon compiles this module out entirely
    /// (target_os gate), so the M5-winning Rust path is untouched there.
    pub fn engaged() -> bool {
        static ENGAGED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENGAGED.get_or_init(|| {
            if std::env::var_os("WEAVER_YENC_N1_ASM").is_some_and(|v| v == "0") {
                return false;
            }
            std::fs::read_to_string("/sys/devices/system/cpu/cpu0/regs/identification/midr_el1")
                .ok()
                .and_then(|s| u64::from_str_radix(s.trim().trim_start_matches("0x"), 16).ok())
                .is_some_and(|midr| ((midr >> 24) & 0xff) == 0x41 && ((midr >> 4) & 0xfff) == 0xd0c)
        })
    }

    /// Decode the whole SIMD span (`*i` negative, steps of 64 to 0). On exit
    /// `*i == 0`, `*out` is the advanced output cursor and `*esc_first` the
    /// carried trailing-`=` flag. The caller's tail reserve covers every
    /// lookahead this block performs (deepest read: window + 67).
    #[allow(unused_assignments)]
    pub unsafe fn span(
        sp: *const u8,
        i: &mut isize,
        out: &mut *mut u8,
        esc_first: &mut u64,
        entry_next_mask: u16,
        table: *const u8,
    ) {
        use std::arch::aarch64::*;
        let mut cur = unsafe { sp.offset(*i) };
        let mut i_v = *i;
        let mut out_v = *out;
        let mut ef = *esc_first;
        // v19: lane-A subtrahend vector, byte 0 = 42 | carry<<6 (oracle's
        // yencOffset trick); v26: carried line-start dot marker.
        let mut v19_init = [0x2au8; 16];
        v19_init[0] = 0x2a | ((ef as u8) << 6);
        let v19_q = unsafe { vld1q_u8(v19_init.as_ptr()) };
        let zero = unsafe { vdupq_n_u8(0) };
        let v26_q = match entry_next_mask {
            1 => unsafe { vsetq_lane_u8::<0>(1, zero) },
            2 => unsafe { vsetq_lane_u8::<1>(2, zero) },
            _ => zero,
        };
        unsafe {
            core::arch::asm!(
                // ---- prologue: pin the constant file (oracle 390ec-39118) --
                "ldr q3, [{tbx}]",
                "ldr q17, [{bits}]",
                "movi v16.16b, #0x3d",
                "movi v18.16b, #0x2a",
                "movi v24.16b, #0xd6",
                "movi v25.16b, #0x6a",
                "mov w15, #0x2a",
                "b 2f",
                // ---- clean window (oracle 39120-39144) ---------------------
                "1:",
                "sub v20.16b, v4.16b, v19.16b",
                "add {cur}, {cur}, #0x40",
                "add v21.16b, v24.16b, v5.16b",
                "adds {i}, {i}, #0x40",
                "add v22.16b, v24.16b, v6.16b",
                "mov {ef}, #0x0",
                "add v23.16b, v24.16b, v7.16b",
                "movi v19.16b, #0x2a",
                "st1 {{v20.16b-v23.16b}}, [{out}], #64",
                "b.eq 9f",
                // ---- loop top (oracle 39148) -------------------------------
                "2:",
                "ld1 {{v4.16b-v7.16b}}, [{cur}]",
                "cmeq v2.16b, v5.16b, v16.16b",
                "cmeq v1.16b, v6.16b, v16.16b",
                "cmeq v28.16b, v7.16b, v16.16b",
                "cmeq v27.16b, v4.16b, v16.16b",
                "mov v8.16b, v2.16b",
                "mov v30.16b, v1.16b",
                "mov v31.16b, v28.16b",
                "mov v29.16b, v27.16b",
                "tbx v8.16b, {{v3.16b}}, v5.16b",
                "tbx v30.16b, {{v3.16b}}, v6.16b",
                "tbx v31.16b, {{v3.16b}}, v7.16b",
                "tbx v29.16b, {{v3.16b}}, v4.16b",
                "orr v0.16b, v8.16b, v30.16b",
                "orr v29.16b, v26.16b, v29.16b",
                "orr v0.16b, v0.16b, v31.16b",
                "orr v0.16b, v0.16b, v29.16b",
                "uqxtn v0.2s, v0.2d",
                "fmov x0, d0",
                "cbz x0, 1b",
                // ---- specials: bit-weight reduce (oracle 39198-391d8) ------
                "and v0.16b, v8.16b, v17.16b",
                "and v29.16b, v29.16b, v17.16b",
                "and v8.16b, v31.16b, v17.16b",
                "and v30.16b, v30.16b, v17.16b",
                "and v31.16b, v27.16b, v17.16b",
                "and v11.16b, v2.16b, v17.16b",
                "and v9.16b, v1.16b, v17.16b",
                "and v10.16b, v28.16b, v17.16b",
                "addp v30.16b, v30.16b, v8.16b",
                "addp v29.16b, v29.16b, v0.16b",
                "addp v31.16b, v31.16b, v11.16b",
                "addp v8.16b, v9.16b, v10.16b",
                "addp v0.16b, v29.16b, v30.16b",
                "addp v30.16b, v31.16b, v8.16b",
                "addp v0.16b, v0.16b, v30.16b",
                "mov x5, v0.d[1]",
                "fmov x4, d0",
                "cmp x4, x5",
                "b.ne 5f",
                // ---- all specials are '=' (oracle 391e4-39224) -------------
                "3:",
                "orr x0, {ef}, x5, lsl #1",
                "tst x0, x4",
                "b.ne 7f",
                "ext v28.16b, v1.16b, v28.16b, #15",
                "lsr x5, x5, #63",
                "ext v1.16b, v2.16b, v1.16b, #15",
                "and {ef}, x5, #0xff",
                "ext v2.16b, v27.16b, v2.16b, #15",
                "ext v27.16b, v18.16b, v27.16b, #15",
                "bsl v28.16b, v25.16b, v18.16b",
                "bsl v1.16b, v25.16b, v18.16b",
                "bsl v2.16b, v25.16b, v18.16b",
                "bsl v27.16b, v25.16b, v19.16b",
                "sub v28.16b, v7.16b, v28.16b",
                "sub v1.16b, v6.16b, v1.16b",
                "sub v2.16b, v5.16b, v2.16b",
                "sub v4.16b, v4.16b, v27.16b",
                // ---- compaction store (oracle 39228-392b4) -----------------
                "4:",
                "ubfiz x1, x4, #4, #15",
                "cnt v0.8b, v0.8b",
                "ubfx x3, x4, #16, #15",
                "ubfx x0, x4, #32, #15",
                "ubfx x5, x4, #48, #15",
                "orr w10, w15, {ef:w}, lsl #6",
                "ldr q5, [{lut}, x1]",
                "lsl x3, x3, #4",
                "fmov x1, d0",
                "lsl x0, x0, #4",
                "lsl x5, x5, #4",
                "mov v19.b[0], w10",
                "tbl v4.16b, {{v4.16b}}, v5.16b",
                "add {cur}, {cur}, #0x40",
                "adds {i}, {i}, #0x40",
                "sub x4, {k8}, x1",
                "str q4, [{out}]",
                "add x4, x4, x4, lsr #8",
                "and x17, x4, #0xff",
                "ldr q0, [{lut}, x3]",
                "add x16, {out}, w4, uxtb",
                "ubfx x11, x4, #16, #8",
                "ubfx x1, x4, #32, #8",
                "add x10, x16, x11",
                "ubfx x4, x4, #48, #8",
                "tbl v2.16b, {{v2.16b}}, v0.16b",
                "add x3, x10, x1",
                "str q2, [{out}, x17]",
                "add {out}, x3, x4",
                "ldr q0, [{lut}, x0]",
                "tbl v1.16b, {{v1.16b}}, v0.16b",
                "str q1, [x16, x11]",
                "ldr q0, [{lut}, x5]",
                "tbl v28.16b, {{v28.16b}}, v0.16b",
                "str q28, [x10, x1]",
                "b.ne 2b",
                "b 9f",
                // ---- dot-arm: `\r` (+2 `.`) probe (oracle 393d8-394ac) -----
                "5:",
                "movi v26.16b, #0x2e",
                "movi v9.16b, #0xd",
                "ext v11.16b, v4.16b, v5.16b, #2",
                "ext v10.16b, v5.16b, v6.16b, #2",
                "ext v29.16b, v6.16b, v7.16b, #2",
                "cmeq v8.16b, v4.16b, v9.16b",
                "cmeq v30.16b, v5.16b, v9.16b",
                "cmeq v11.16b, v11.16b, v26.16b",
                "cmeq v10.16b, v10.16b, v26.16b",
                "cmeq v29.16b, v29.16b, v26.16b",
                "and v11.16b, v11.16b, v8.16b",
                "and v10.16b, v10.16b, v30.16b",
                "ldur q30, [{cur}, #50]",
                "cmeq v8.16b, v6.16b, v9.16b",
                "cmeq v9.16b, v7.16b, v9.16b",
                "cmeq v30.16b, v30.16b, v26.16b",
                "movi v26.4s, #0x0",
                "and v29.16b, v29.16b, v8.16b",
                "orr v8.16b, v11.16b, v10.16b",
                "and v9.16b, v30.16b, v9.16b",
                "orr v8.16b, v8.16b, v29.16b",
                "orr v8.16b, v8.16b, v9.16b",
                "uqxtn v8.2s, v8.2d",
                "fmov x0, d8",
                "cbz x0, 3b",
                // stuffed dots present: build m2nldot, kill + carry (39440+)
                "ext v30.16b, v6.16b, v7.16b, #1",
                "ldur q14, [{cur}, #49]",
                "movi v15.16b, #0xa",
                "ext v8.16b, v4.16b, v5.16b, #1",
                "ext v31.16b, v5.16b, v6.16b, #1",
                "cmeq v30.16b, v30.16b, v15.16b",
                "cmeq v8.16b, v8.16b, v15.16b",
                "cmeq v31.16b, v31.16b, v15.16b",
                "cmeq v14.16b, v14.16b, v15.16b",
                "and v8.16b, v8.16b, v17.16b",
                "and v14.16b, v14.16b, v9.16b",
                "and v9.16b, v30.16b, v17.16b",
                "and v30.16b, v31.16b, v17.16b",
                "and v8.16b, v8.16b, v11.16b",
                "ext v26.16b, v14.16b, v26.16b, #14",
                "and v9.16b, v9.16b, v29.16b",
                "and v14.16b, v14.16b, v17.16b",
                "and v10.16b, v30.16b, v10.16b",
                "addp v9.16b, v9.16b, v14.16b",
                "addp v8.16b, v8.16b, v10.16b",
                "addp v8.16b, v8.16b, v9.16b",
                "addp v8.16b, v8.16b, v8.16b",
                "shl v8.2d, v8.2d, #2",
                "fmov x0, d8",
                "orr v0.16b, v0.16b, v8.16b",
                "orr x4, x4, x0",
                "b 3b",
                // ---- consecutive-'=' collision (oracle 3954c-395e4) --------
                "7:",
                "bic x0, x5, x0",
                "and x0, x0, #0x5555555555555555",
                "add x0, x0, x5",
                "eor x0, x0, #0x5555555555555555",
                "and x0, x0, x5",
                "ldr q14, [{bcast}]",
                "orr {ef}, {ef}, x0, lsl #1",
                "lsr x0, x0, #63",
                "fmov d8, {ef}",
                "bic x4, x4, {ef}",
                "and {ef}, x0, #0xff",
                "ext v2.16b, v8.16b, v8.16b, #2",
                "ext v1.16b, v8.16b, v8.16b, #4",
                "ext v28.16b, v8.16b, v8.16b, #6",
                "tbl v27.16b, {{v8.16b}}, v14.16b",
                "tbl v2.16b, {{v2.16b}}, v14.16b",
                "tbl v1.16b, {{v1.16b}}, v14.16b",
                "tbl v28.16b, {{v28.16b}}, v14.16b",
                "cmtst v27.16b, v27.16b, v17.16b",
                "cmtst v2.16b, v2.16b, v17.16b",
                "cmtst v1.16b, v1.16b, v17.16b",
                "cmtst v28.16b, v28.16b, v17.16b",
                "bsl v27.16b, v25.16b, v18.16b",
                "bsl v2.16b, v25.16b, v18.16b",
                "bsl v1.16b, v25.16b, v18.16b",
                "bsl v28.16b, v25.16b, v18.16b",
                "bic v0.16b, v0.16b, v8.16b",
                "sub v2.16b, v5.16b, v2.16b",
                "sub v1.16b, v6.16b, v1.16b",
                "sub v28.16b, v7.16b, v28.16b",
                "sub v4.16b, v4.16b, v27.16b",
                "b 4b",
                "9:",
                cur = inout(reg) cur,
                i = inout(reg) i_v,
                out = inout(reg) out_v,
                ef = inout(reg) ef,
                lut = in(reg) table,
                k8 = in(reg) 0x0808080808080808u64,
                tbx = in(reg) N1_TBX_CRLF.0.as_ptr(),
                bits = in(reg) N1_BIT_LANES.0.as_ptr(),
                bcast = in(reg) N1_BCAST01.0.as_ptr(),
                inout("v19") v19_q => _,
                inout("v26") v26_q => _,
                out("x0") _, out("x1") _, out("x3") _, out("x4") _, out("x5") _,
                out("x10") _, out("x11") _, out("x12") _, out("x15") _,
                out("x16") _, out("x17") _,
                out("v0") _, out("v1") _, out("v2") _, out("v3") _, out("v4") _,
                out("v5") _, out("v6") _, out("v7") _, out("v8") _, out("v9") _,
                out("v10") _, out("v11") _, out("v14") _, out("v15") _,
                out("v16") _, out("v17") _, out("v18") _, out("v20") _,
                out("v21") _, out("v22") _, out("v23") _, out("v24") _,
                out("v25") _, out("v27") _, out("v28") _, out("v29") _,
                out("v30") _, out("v31") _,
                options(nostack),
            );
        }
        *i = i_v;
        *out = out_v;
        *esc_first = ef;
    }
}

#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
pub(super) mod n1_span_se {
    use super::n1_span::{N1_BCAST01, N1_BIT_LANES, N1_TBX_CRLF};

    /// SEARCH_END = true variant of [`super::n1_span::span`]: the same frozen
    /// window body, plus weaver's OWN terminator machinery (NOT the oracle's —
    /// weaver's mask-space candidate probe beats the oracle's per-window
    /// vector probe by ~19% on crlf until_end, so this block freezes weaver's
    /// design, hand-scheduled):
    ///   - loop-top pending-tail dispatch (`cbz` + 2 `tbnz`, tags at bits
    ///     61/62/63 exactly as the Rust loop);
    ///   - no-dot arm: scalar cand = mask & (mask>>1 | 1<<63) & (eq>>2 | 3<<62),
    ///     in-asm vector resolution on hit, in-asm tail classification;
    ///   - dot arm: scalar cand2 over the reduced `\r\n.` bits; any hit exits
    ///     with `kind = 5` and the window unconsumed — the Rust loop reprocesses
    ///     it with its full probe and continues (rare^2: a dot window whose
    ///     specials also alias a terminator shape).
    ///
    /// Exit protocol via `kind`: 0 = span done (i == 0); 1 = terminator break
    /// (mask_out valid, i at the unconsumed window); 2/3/4 = pending-tail
    /// resume hit (Cr / CrLf / CrLfEq); 5 = resolve-window-in-Rust (v26
    /// exported to `nmm_out`, pending clear).
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn span_se(
        sp: *const u8,
        i: &mut isize,
        out: &mut *mut u8,
        esc_first: &mut u64,
        pending_tail: &mut u64,
        mask_out: &mut u64,
        kind: &mut u64,
        entry_next_mask: u16,
        nmm_out: &mut [u8; 16],
        table: *const u8,
    ) {
        use std::arch::aarch64::*;
        let cur = unsafe { sp.offset(*i) };
        let mut i_v = *i;
        let mut out_v = *out;
        let mut ef = *esc_first;
        let mut pend = *pending_tail;
        let mut kind_v: u64 = 0;
        let mut mout: u64 = 0;
        let mut v19_init = [0x2au8; 16];
        v19_init[0] = 0x2a | ((ef as u8) << 6);
        let v19_q = unsafe { vld1q_u8(v19_init.as_ptr()) };
        let zero = unsafe { vdupq_n_u8(0) };
        let v26_q = match entry_next_mask {
            1 => unsafe { vsetq_lane_u8::<0>(1, zero) },
            2 => unsafe { vsetq_lane_u8::<1>(2, zero) },
            _ => zero,
        };
        unsafe {
            core::arch::asm!(
                "ldr q3, [{tbx}]",
                "ldr q17, [{bits}]",
                "movi v16.16b, #0x3d",
                "movi v18.16b, #0x2a",
                "movi v24.16b, #0xd6",
                "movi v25.16b, #0x6a",
                "mov w15, #0x2a",
                "b 2f",
                // ---- clean window --------------------------------------
                "1:",
                "sub v20.16b, v4.16b, v19.16b",
                "add {cur}, {cur}, #0x40",
                "add v21.16b, v24.16b, v5.16b",
                "adds {i}, {i}, #0x40",
                "add v22.16b, v24.16b, v6.16b",
                "mov {ef}, #0x0",
                "add v23.16b, v24.16b, v7.16b",
                "movi v19.16b, #0x2a",
                "st1 {{v20.16b-v23.16b}}, [{out}], #64",
                "b.eq 9f",
                // ---- loop top: pending dispatch, then the window body --
                "2:",
                "cbz {pend}, 20f",
                "ldrb w0, [{cur}]",
                "tbnz {pend}, #63, 21f",
                "tbnz {pend}, #62, 22f",
                "cmp w0, #0x79",
                "b.eq 33f",
                "mov {pend}, xzr",
                "b 20f",
                "21:",
                "cmp w0, #0xa",
                "b.ne 25f",
                "ldrb w1, [{cur}, #1]",
                "cmp w1, #0x3d",
                "b.ne 25f",
                "ldrb w1, [{cur}, #2]",
                "cmp w1, #0x79",
                "b.eq 31f",
                "25:",
                "mov {pend}, xzr",
                "b 20f",
                "22:",
                "cmp w0, #0x3d",
                "b.ne 25b",
                "ldrb w1, [{cur}, #1]",
                "cmp w1, #0x79",
                "b.eq 32f",
                "b 25b",
                "20:",
                "ld1 {{v4.16b-v7.16b}}, [{cur}]",
                "cmeq v2.16b, v5.16b, v16.16b",
                "cmeq v1.16b, v6.16b, v16.16b",
                "cmeq v28.16b, v7.16b, v16.16b",
                "cmeq v27.16b, v4.16b, v16.16b",
                "mov v8.16b, v2.16b",
                "mov v30.16b, v1.16b",
                "mov v31.16b, v28.16b",
                "mov v29.16b, v27.16b",
                "tbx v8.16b, {{v3.16b}}, v5.16b",
                "tbx v30.16b, {{v3.16b}}, v6.16b",
                "tbx v31.16b, {{v3.16b}}, v7.16b",
                "tbx v29.16b, {{v3.16b}}, v4.16b",
                "orr v0.16b, v8.16b, v30.16b",
                "orr v29.16b, v26.16b, v29.16b",
                "orr v0.16b, v0.16b, v31.16b",
                "orr v0.16b, v0.16b, v29.16b",
                "uqxtn v0.2s, v0.2d",
                "fmov x0, d0",
                "cbz x0, 1b",
                "and v0.16b, v8.16b, v17.16b",
                "and v29.16b, v29.16b, v17.16b",
                "and v8.16b, v31.16b, v17.16b",
                "and v30.16b, v30.16b, v17.16b",
                "and v31.16b, v27.16b, v17.16b",
                "and v11.16b, v2.16b, v17.16b",
                "and v9.16b, v1.16b, v17.16b",
                "and v10.16b, v28.16b, v17.16b",
                "addp v30.16b, v30.16b, v8.16b",
                "addp v29.16b, v29.16b, v0.16b",
                "addp v31.16b, v31.16b, v11.16b",
                "addp v8.16b, v9.16b, v10.16b",
                "addp v0.16b, v29.16b, v30.16b",
                "addp v30.16b, v31.16b, v8.16b",
                "addp v0.16b, v0.16b, v30.16b",
                "mov x5, v0.d[1]",
                "fmov x4, d0",
                "cmp x4, x5",
                "b.ne 5f",
                // ---- all-eq path ---------------------------------------
                "3:",
                "orr x0, {ef}, x5, lsl #1",
                "tst x0, x4",
                "b.ne 7f",
                "ext v28.16b, v1.16b, v28.16b, #15",
                "lsr x5, x5, #63",
                "ext v1.16b, v2.16b, v1.16b, #15",
                "and {ef}, x5, #0xff",
                "ext v2.16b, v27.16b, v2.16b, #15",
                "ext v27.16b, v18.16b, v27.16b, #15",
                "bsl v28.16b, v25.16b, v18.16b",
                "bsl v1.16b, v25.16b, v18.16b",
                "bsl v2.16b, v25.16b, v18.16b",
                "bsl v27.16b, v25.16b, v19.16b",
                "sub v28.16b, v7.16b, v28.16b",
                "sub v1.16b, v6.16b, v1.16b",
                "sub v2.16b, v5.16b, v2.16b",
                "sub v4.16b, v4.16b, v27.16b",
                // ---- compaction store ----------------------------------
                "4:",
                "ubfiz x1, x4, #4, #15",
                "cnt v0.8b, v0.8b",
                "ubfx x3, x4, #16, #15",
                "ubfx x0, x4, #32, #15",
                "ubfx x5, x4, #48, #15",
                "orr w10, w15, {ef:w}, lsl #6",
                "ldr q5, [{lut}, x1]",
                "lsl x3, x3, #4",
                "fmov x1, d0",
                "lsl x0, x0, #4",
                "lsl x5, x5, #4",
                "mov v19.b[0], w10",
                "tbl v4.16b, {{v4.16b}}, v5.16b",
                "add {cur}, {cur}, #0x40",
                "adds {i}, {i}, #0x40",
                "sub x4, {k8}, x1",
                "str q4, [{out}]",
                "add x4, x4, x4, lsr #8",
                "and x17, x4, #0xff",
                "ldr q0, [{lut}, x3]",
                "add x16, {out}, w4, uxtb",
                "ubfx x11, x4, #16, #8",
                "ubfx x1, x4, #32, #8",
                "add x10, x16, x11",
                "ubfx x4, x4, #48, #8",
                "tbl v2.16b, {{v2.16b}}, v0.16b",
                "add x3, x10, x1",
                "str q2, [{out}, x17]",
                "add {out}, x3, x4",
                "ldr q0, [{lut}, x0]",
                "tbl v1.16b, {{v1.16b}}, v0.16b",
                "str q1, [x16, x11]",
                "ldr q0, [{lut}, x5]",
                "tbl v28.16b, {{v28.16b}}, v0.16b",
                "str q28, [x10, x1]",
                "b.ne 2b",
                "b 9f",
                // ---- dot-arm: `\r` (+2 `.`) probe -----------------------
                "5:",
                "mov v20.16b, v26.16b",
                "movi v26.16b, #0x2e",
                "movi v9.16b, #0xd",
                "ext v11.16b, v4.16b, v5.16b, #2",
                "ext v10.16b, v5.16b, v6.16b, #2",
                "ext v29.16b, v6.16b, v7.16b, #2",
                "cmeq v8.16b, v4.16b, v9.16b",
                "cmeq v30.16b, v5.16b, v9.16b",
                "cmeq v11.16b, v11.16b, v26.16b",
                "cmeq v10.16b, v10.16b, v26.16b",
                "cmeq v29.16b, v29.16b, v26.16b",
                "and v11.16b, v11.16b, v8.16b",
                "and v10.16b, v10.16b, v30.16b",
                "ldur q30, [{cur}, #50]",
                "cmeq v8.16b, v6.16b, v9.16b",
                "cmeq v9.16b, v7.16b, v9.16b",
                "cmeq v30.16b, v30.16b, v26.16b",
                "and v29.16b, v29.16b, v8.16b",
                "orr v8.16b, v11.16b, v10.16b",
                "and v9.16b, v30.16b, v9.16b",
                "orr v8.16b, v8.16b, v29.16b",
                "orr v8.16b, v8.16b, v9.16b",
                "uqxtn v8.2s, v8.2d",
                "fmov x0, d8",
                "cbz x0, 6f",
                // stuffed dots present: reduce m2nldot FIRST, then cand2
                "ext v30.16b, v6.16b, v7.16b, #1",
                "ldur q14, [{cur}, #49]",
                "movi v15.16b, #0xa",
                "ext v8.16b, v4.16b, v5.16b, #1",
                "ext v31.16b, v5.16b, v6.16b, #1",
                "cmeq v30.16b, v30.16b, v15.16b",
                "cmeq v8.16b, v8.16b, v15.16b",
                "cmeq v31.16b, v31.16b, v15.16b",
                "cmeq v14.16b, v14.16b, v15.16b",
                "and v8.16b, v8.16b, v17.16b",
                "and v14.16b, v14.16b, v9.16b",
                "and v9.16b, v30.16b, v17.16b",
                "and v30.16b, v31.16b, v17.16b",
                "and v8.16b, v8.16b, v11.16b",
                "and v9.16b, v9.16b, v29.16b",
                "and v10.16b, v30.16b, v10.16b",
                "and v30.16b, v14.16b, v17.16b",
                "addp v9.16b, v9.16b, v30.16b",
                "addp v8.16b, v8.16b, v10.16b",
                "addp v8.16b, v8.16b, v9.16b",
                "addp v8.16b, v8.16b, v8.16b",
                "fmov x0, d8",
                // cand2 = (dotc & mask>>3) | dotc>>61 | no-dot cand form
                "lsr x1, x4, #3",
                "and x1, x1, x0",
                "orr x1, x1, x0, lsr #61",
                "lsr x3, x4, #1",
                "orr x3, x3, #0x8000000000000000",
                "and x3, x3, x4",
                "lsr x10, x5, #2",
                "orr x10, x10, #0xc000000000000000",
                "and x3, x3, x10",
                "orr x1, x1, x3",
                "cbnz x1, 36f",
                // no candidate: fold the dot kills + carry, rejoin
                "shl v8.2d, v8.2d, #2",
                "fmov x0, d8",
                "movi v26.4s, #0x0",
                "ext v26.16b, v14.16b, v26.16b, #14",
                "orr v0.16b, v0.16b, v8.16b",
                "orr x4, x4, x0",
                "b 3b",
                // ---- no-dot arm: scalar cand + in-asm resolution --------
                "6:",
                "movi v26.4s, #0x0",
                "lsr x0, x4, #1",
                "orr x0, x0, #0x8000000000000000",
                "and x0, x0, x4",
                "lsr x1, x5, #2",
                "orr x1, x1, #0xc000000000000000",
                "and x0, x0, x1",
                "cbz x0, 3b",
                "lsl x1, x0, #2",
                "cbz x1, 66f",
                // in-window: m3eqy any?
                "movi v20.16b, #0x79",
                "ext v21.16b, v27.16b, v2.16b, #2",
                "ext v22.16b, v2.16b, v1.16b, #2",
                "ext v23.16b, v1.16b, v28.16b, #2",
                "ldur q30, [{cur}, #50]",
                "cmeq v30.16b, v30.16b, v16.16b",
                "ext v8.16b, v4.16b, v5.16b, #3",
                "ext v9.16b, v5.16b, v6.16b, #3",
                "ext v10.16b, v6.16b, v7.16b, #3",
                "ldur q11, [{cur}, #51]",
                "cmeq v8.16b, v8.16b, v20.16b",
                "cmeq v9.16b, v9.16b, v20.16b",
                "cmeq v10.16b, v10.16b, v20.16b",
                "cmeq v11.16b, v11.16b, v20.16b",
                "and v21.16b, v21.16b, v8.16b",
                "and v22.16b, v22.16b, v9.16b",
                "and v23.16b, v23.16b, v10.16b",
                "and v30.16b, v30.16b, v11.16b",
                "orr v8.16b, v21.16b, v22.16b",
                "orr v9.16b, v23.16b, v30.16b",
                "orr v8.16b, v8.16b, v9.16b",
                "uqxtn v8.2s, v8.2d",
                "fmov x1, d8",
                "cbz x1, 66f",
                // full match_end = m3eqy & lf & cr per lane
                "movi v31.16b, #0xd",
                "movi v20.16b, #0xa",
                "cmeq v8.16b, v4.16b, v31.16b",
                "cmeq v9.16b, v5.16b, v31.16b",
                "cmeq v10.16b, v6.16b, v31.16b",
                "cmeq v11.16b, v7.16b, v31.16b",
                "ext v29.16b, v4.16b, v5.16b, #1",
                "cmeq v29.16b, v29.16b, v20.16b",
                "and v8.16b, v8.16b, v29.16b",
                "ext v29.16b, v5.16b, v6.16b, #1",
                "cmeq v29.16b, v29.16b, v20.16b",
                "and v9.16b, v9.16b, v29.16b",
                "ext v29.16b, v6.16b, v7.16b, #1",
                "cmeq v29.16b, v29.16b, v20.16b",
                "and v10.16b, v10.16b, v29.16b",
                "ldur q29, [{cur}, #49]",
                "cmeq v29.16b, v29.16b, v20.16b",
                "and v11.16b, v11.16b, v29.16b",
                "and v21.16b, v21.16b, v8.16b",
                "and v22.16b, v22.16b, v9.16b",
                "and v23.16b, v23.16b, v10.16b",
                "and v30.16b, v30.16b, v11.16b",
                "orr v8.16b, v21.16b, v22.16b",
                "orr v9.16b, v23.16b, v30.16b",
                "orr v8.16b, v8.16b, v9.16b",
                "uqxtn v8.2s, v8.2d",
                "fmov x1, d8",
                "cbnz x1, 35f",
                // tail classification (cand bits 61+)
                "66:",
                "lsr x1, x0, #61",
                "cbz x1, 3b",
                "ldrb w1, [{cur}, #61]",
                "ldrb w3, [{cur}, #62]",
                "ldrb w10, [{cur}, #63]",
                "mov w11, #0x3d",
                "cmp w1, #0xd",
                "ccmp w3, #0xa, #0, eq",
                "ccmp w10, w11, #0, eq",
                "b.ne 67f",
                "mov {pend}, #0x2000000000000000",
                "b 3b",
                "67:",
                "cmp w3, #0xd",
                "ccmp w10, #0xa, #0, eq",
                "b.ne 68f",
                "mov {pend}, #0x4000000000000000",
                "b 3b",
                "68:",
                "cmp w10, #0xd",
                "b.ne 3b",
                "mov {pend}, #0x8000000000000000",
                "b 3b",
                // ---- collision block -----------------------------------
                "7:",
                "bic x0, x5, x0",
                "and x0, x0, #0x5555555555555555",
                "add x0, x0, x5",
                "eor x0, x0, #0x5555555555555555",
                "and x0, x0, x5",
                "ldr q14, [{bcast}]",
                "orr {ef}, {ef}, x0, lsl #1",
                "lsr x0, x0, #63",
                "fmov d8, {ef}",
                "bic x4, x4, {ef}",
                "and {ef}, x0, #0xff",
                "ext v2.16b, v8.16b, v8.16b, #2",
                "ext v1.16b, v8.16b, v8.16b, #4",
                "ext v28.16b, v8.16b, v8.16b, #6",
                "tbl v27.16b, {{v8.16b}}, v14.16b",
                "tbl v2.16b, {{v2.16b}}, v14.16b",
                "tbl v1.16b, {{v1.16b}}, v14.16b",
                "tbl v28.16b, {{v28.16b}}, v14.16b",
                "cmtst v27.16b, v27.16b, v17.16b",
                "cmtst v2.16b, v2.16b, v17.16b",
                "cmtst v1.16b, v1.16b, v17.16b",
                "cmtst v28.16b, v28.16b, v17.16b",
                "bsl v27.16b, v25.16b, v18.16b",
                "bsl v2.16b, v25.16b, v18.16b",
                "bsl v1.16b, v25.16b, v18.16b",
                "bsl v28.16b, v25.16b, v18.16b",
                "bic v0.16b, v0.16b, v8.16b",
                "sub v2.16b, v5.16b, v2.16b",
                "sub v1.16b, v6.16b, v1.16b",
                "sub v28.16b, v7.16b, v28.16b",
                "sub v4.16b, v4.16b, v27.16b",
                "b 4b",
                // ---- exits ---------------------------------------------
                "31:",
                "mov {kind}, #2",
                "b 9f",
                "32:",
                "mov {kind}, #3",
                "b 9f",
                "33:",
                "mov {kind}, #4",
                "b 9f",
                "35:",
                "mov {kind}, #1",
                "mov {mout}, x4",
                "b 9f",
                "36:",
                "mov {kind}, #5",
                "st1 {{v20.16b}}, [{nmm}]",
                "9:",
                cur = inout(reg) cur => _,
                i = inout(reg) i_v,
                out = inout(reg) out_v,
                ef = inout(reg) ef,
                pend = inout(reg) pend,
                kind = inout(reg) kind_v,
                mout = inout(reg) mout,
                lut = in(reg) table,
                k8 = in(reg) 0x0808080808080808u64,
                tbx = in(reg) N1_TBX_CRLF.0.as_ptr(),
                bits = in(reg) N1_BIT_LANES.0.as_ptr(),
                bcast = in(reg) N1_BCAST01.0.as_ptr(),
                nmm = in(reg) nmm_out.as_mut_ptr(),
                inout("v19") v19_q => _,
                inout("v26") v26_q => _,
                out("x0") _, out("x1") _, out("x3") _, out("x4") _, out("x5") _,
                out("x10") _, out("x11") _, out("x12") _, out("x15") _,
                out("x16") _, out("x17") _,
                out("v0") _, out("v1") _, out("v2") _, out("v3") _, out("v4") _,
                out("v5") _, out("v6") _, out("v7") _, out("v8") _, out("v9") _,
                out("v10") _, out("v11") _, out("v14") _, out("v15") _,
                out("v16") _, out("v17") _, out("v18") _, out("v20") _,
                out("v21") _, out("v22") _, out("v23") _, out("v24") _,
                out("v25") _, out("v27") _, out("v28") _, out("v29") _,
                out("v30") _, out("v31") _,
                options(nostack),
            );
        }
        *i = i_v;
        *out = out_v;
        *esc_first = ef;
        *pending_tail = pend;
        *mask_out = mout;
        *kind = kind_v;
    }
}
