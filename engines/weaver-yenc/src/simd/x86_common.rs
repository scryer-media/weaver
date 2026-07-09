use super::*;

/// End-of-chunk decoder state for a 64-byte window that contained no
/// dot-stuffing (those chunks bail to scalar). Derived from the trailing two
/// input bytes plus the escape mask, matching `final_state_after_block` +
/// `x86_final_state_after_block` for the no-dot case: a decoded window ends
/// mid-line (`None`) unless its final byte is an unescaped `\r` (`Cr`), an
/// unescaped `=` (`Eq`), or the `\n` of a trailing `\r\n` (`CrLf`).
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(super) fn span_end_state(win: &[u8], fixed_eq: u64, dot_unstuffing: bool) -> DecoderState {
    const LAST: u64 = 1u64 << 63;
    // A trailing unescaped '=' means the next byte is escaped. If that '=' sits
    // at a line start (an unescaped `\r\n` at bytes 61..62), the next byte could
    // be the `y` of a `\r\n=y` control marker, so the carried state must be
    // `CrLfEq`, not `Eq` — matching `x86_final_state_after_block`. Without this
    // a `\r\n=y` straddling the 64-byte window boundary is decoded as escaped
    // data instead of stopping at the control.
    if fixed_eq & LAST != 0 {
        let crlf_before_eq =
            dot_unstuffing && win[61] == b'\r' && win[62] == b'\n' && fixed_eq & (1u64 << 60) == 0; // '\r' at 61 not itself escaped
        return if crlf_before_eq {
            DecoderState::CrLfEq
        } else {
            DecoderState::Eq
        };
    }
    let last = win[63];
    let prev = win[62];
    // `fixed_eq` bit 62 set ⇒ byte 63 is the escaped partner of a '=', i.e.
    // data, not a real break.
    let last_escaped = fixed_eq & (LAST >> 1) != 0;
    if last == b'\r' {
        // A trailing '\r' arms `Cr` whether it is a real break (unescaped) or —
        // in raw mode — the escaped byte of a `=\r`: `decode_scalar_step`'s `Eq`
        // arm maps `Eq`+`\r`→`Cr`, and `x86_final_state_after_block` applies the
        // `cr & escaped → Cr` override. Missing this decodes `=\r\n.` wrong when
        // the `\r` lands on the window's last byte.
        if !last_escaped || dot_unstuffing {
            return DecoderState::Cr;
        }
    }
    if !last_escaped && last == b'\n' && prev == b'\r' && fixed_eq & (LAST >> 2) == 0 {
        return DecoderState::CrLf;
    }
    DecoderState::None
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(super) fn x86_block_entry_flags(
    input: &[u8],
    src: usize,
    state: DecoderState,
    dot_unstuffing: bool,
    search_end: bool,
) -> Option<(bool, bool)> {
    if !matches!(
        state,
        DecoderState::None | DecoderState::CrLf | DecoderState::Eq | DecoderState::CrLfEq
    ) {
        return None;
    }

    if state == DecoderState::CrLfEq && search_end && dot_unstuffing && input[src] == b'y' {
        return None;
    }

    let esc_first = matches!(state, DecoderState::Eq | DecoderState::CrLfEq);
    let dot0 = dot_unstuffing && state == DecoderState::CrLf && input[src] == b'.';
    Some((esc_first, dot0))
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(super) fn x86_dot_start_mask(input: &[u8], src: usize, line_start: u64, escaped: u64) -> u64 {
    let mut candidates = line_start & !escaped;
    let mut dot_start = 0u64;
    while candidates != 0 {
        let bit = candidates.trailing_zeros() as usize;
        if input[src + bit] == b'.' {
            dot_start |= 1u64 << bit;
        }
        candidates &= candidates - 1;
    }
    dot_start
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub(super) fn x86_final_state_after_block(
    fixed_eq: u64,
    dot_start: u64,
    raw_breaks: u64,
    raw_cr: u64,
    crlf: u64,
    skip: u64,
    line_start: u64,
    cr: u64,
    escaped: u64,
    dot_unstuffing: bool,
) -> DecoderState {
    const LAST: u64 = 1u64 << 63;

    let mut next_state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, !skip);
    if fixed_eq & LAST != 0 {
        next_state = if dot_unstuffing && line_start & LAST != 0 {
            DecoderState::CrLfEq
        } else {
            DecoderState::Eq
        };
    } else if dot_start & LAST != 0 {
        next_state = DecoderState::CrLfDot;
    } else if dot_unstuffing && (cr & escaped) & LAST != 0 {
        next_state = DecoderState::Cr;
    }
    next_state
}

#[cfg(target_arch = "x86_64")]
pub(super) type DecodeBlock64 = unsafe fn(
    &[u8],
    usize,
    &mut [u8],
    &mut usize,
    &mut KernelState,
    bool,
    bool,
) -> Result<Option<usize>, YencError>;

#[cfg(target_arch = "x86_64")]
pub(super) unsafe fn decode_kernel_simd64(
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

    // Keep enough trailing input for the scalar epilogue so
    // cross-boundary CRLF, dot-stuffing, and end markers remain exact.
    let tail_buffer = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail_buffer);

    if input.len() > WIDTH * 2 {
        while (!search_end || state.end == DecodeEnd::None) && src + WIDTH <= simd_limit {
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

#[cfg(target_arch = "x86_64")]
#[inline]
pub(super) fn dispatch_x86_decode_normal_run() -> DecodeRunFn {
    use std::sync::OnceLock;

    static DISPATCH: OnceLock<DecodeRunFn> = OnceLock::new();
    *DISPATCH.get_or_init(|| {
        if is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx2")
        {
            decode_normal_run_avx512_dispatch
        } else if is_x86_feature_detected!("avx2") {
            decode_normal_run_avx2_dispatch
        } else if is_x86_feature_detected!("avx") {
            decode_normal_run_avx_dispatch
        } else if is_x86_feature_detected!("ssse3") && !x86_prefers_sse2_over_ssse3() {
            decode_normal_run_ssse3_dispatch
        } else if is_x86_feature_detected!("sse2") {
            decode_normal_run_sse2_dispatch
        } else {
            decode_normal_run_scalar
        }
    })
}

#[cfg(target_arch = "x86_64")]
pub(super) fn x86_prefers_sse2_over_ssse3() -> bool {
    let (family, model) = x86_cpu_family_model();
    x86_family_model_prefers_sse2_over_ssse3(family, model)
}

#[cfg(target_arch = "x86_64")]
pub(super) fn x86_family_model_prefers_sse2_over_ssse3(family: u32, model: u32) -> bool {
    matches!(
        (family, model),
        // Bonnell/Silvermont and AMD Bobcat guardrail:
        // these cores have very slow PSHUFB/PBLENDVB, so we pretend
        // SSSE3 does not exist and stays on SSE2.
        (
            6,
            0x1c | 0x26 | 0x27 | 0x35 | 0x36 | 0x37 | 0x4a | 0x4c | 0x4d | 0x5a | 0x5d
        ) | (0x5f, 0..=2)
    )
}

#[cfg(target_arch = "x86_64")]
pub(super) fn x86_prefers_ssse3_over_sse41() -> bool {
    let (family, model) = x86_cpu_family_model();
    x86_family_model_prefers_ssse3_over_sse41(family, model)
}

#[cfg(target_arch = "x86_64")]
pub(super) fn x86_family_model_prefers_ssse3_over_sse41(family: u32, model: u32) -> bool {
    matches!(
        (family, model),
        // Goldmont/Goldmont Plus/Tremont guardrail:
        // keep PSHUFB compaction but avoid the SSE4.1 PBLENDVB path.
        (6, 0x5c | 0x5f | 0x7a | 0x9c)
    )
}

#[cfg(target_arch = "x86_64")]
pub(super) fn x86_cpu_family_model() -> (u32, u32) {
    use std::arch::x86_64::__cpuid;

    let leaf = __cpuid(1);
    x86_cpu_family_model_from_eax(leaf.eax)
}

#[cfg(target_arch = "x86_64")]
pub(super) fn x86_cpu_family_model_from_eax(eax: u32) -> (u32, u32) {
    let family = ((eax >> 8) & 0xf) + ((eax >> 16) & 0xff0);
    let model = ((eax >> 4) & 0xf) + ((eax >> 12) & 0xf0);
    (family, model)
}

#[cfg(target_arch = "x86_64")]
#[inline]
pub(super) fn decode_normal_run_sse2_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_sse2(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[inline]
pub(super) fn decode_normal_run_ssse3_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_ssse3(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[inline]
pub(super) fn decode_normal_run_avx_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_avx(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[inline]
pub(super) fn decode_normal_run_avx2_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_avx2(input, start, output, dst_start) }
}

#[cfg(target_arch = "x86_64")]
#[inline]
pub(super) fn decode_normal_run_avx512_dispatch(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    unsafe { decode_normal_run_avx512(input, start, output, dst_start) }
}
