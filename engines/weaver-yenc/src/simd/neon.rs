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
