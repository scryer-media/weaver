// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2,bmi1,bmi2,popcnt,lzcnt")]
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

// Faithful 512-bit port of rapidyenc `do_decode_avx2` instantiated at
// `ISA_LEVEL_VBMI2` (`decoder_vbmi2.cc` → `decoder_avx2_base.h`), the
// `isRaw=true, searchEnd=false` path. This is the AVX2 flat-loop port
// [`decode_kernel_avx2_raw`](super::x86_avx2) widened to a single 512-bit
// window: the two 256-bit lanes collapse to one `__m512i`, `movemask`+combine
// collapses to a `_mm512_cmpeq_epi8_mask` k-register `u64`, the 2-lane LUT
// compaction becomes one `_mm512_maskz_compress_epi8`, and escape unescape is
// a single `_mm512_mask_add_epi8`. The scalar `u64` bit-math (`fix_eq_mask`,
// `escaped`, `esc_first`, `skip`, entry/exit state) is byte-identical to the
// AVX2 port, so both tiers share the same correctness envelope.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2,bmi1,bmi2,popcnt,lzcnt")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn decode_kernel_avx512_raw<const SEARCH_END: bool>(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<KernelOutcome, YencError> {
    const WIDTH: usize = 64;

    let mut src = 0usize;
    let mut dst = 0usize;

    if input.len() > WIDTH * 2 && output.len() >= input.len() {
        // entry state → escFirst / nextMask (oracle `_do_decode_simd` switch
        // subset). min_mask byte 0/1 is zeroed for a carried line start
        // (nextMask 1 ⇒ byte 0, 2 ⇒ byte 1).
        let esc_first: u64 = (state.state == DecoderState::Eq) as u64;
        let entry_zero: u64 = match state.state {
            DecoderState::CrLf if input[0] == b'.' => 1,
            DecoderState::Cr if input[0] == b'\n' && input[1] == b'.' => 2,
            _ => 0,
        };
        let (inp, len, out) = (input.as_ptr(), input.len(), output.as_mut_ptr());
        let exit = if zen4() {
            raw_windows::<SEARCH_END, PREFETCH, true>(inp, len, out, esc_first, entry_zero)
        } else {
            raw_windows::<SEARCH_END, PREFETCH, false>(inp, len, out, esc_first, entry_zero)
        };
        src = exit.src;
        dst = exit.dst;
        if let Some(mask) = exit.break_mask {
            // A SEARCH_END probe aborted the window at `src` (oracle `len += i;
            // break;`): it is left unconsumed and the exit state comes from the
            // no-backtrack rule over it, with the PRE-window escape carry.
            state.state = x86_break_state(input, src, mask, exit.esc_first);
        } else if src > 0 {
            // Only re-derive the carried state when the SIMD loop consumed at
            // least one window; otherwise the entry state must survive
            // untouched for the scalar tail (a carried Cr/CrLf line start).
            // The oracle keeps `CrLf`/`Cr` only before a `.`; the scalar
            // machine keeps them before any byte, and a line-start `=` must
            // carry `CrLfEq`, not `Eq` (`src >= 64` here).
            state.state = if exit.esc_first != 0 {
                if input[src - 3] == b'\r' && input[src - 2] == b'\n' {
                    DecoderState::CrLfEq
                } else {
                    DecoderState::Eq
                }
            } else if input[src - 1] == b'\r' {
                DecoderState::Cr
            } else if input[src - 2] == b'\r' && input[src - 1] == b'\n' {
                DecoderState::CrLf
            } else {
                DecoderState::None
            };
        }
    }

    // Tail (at most the oracle's `lenBuffer` bytes, decoder_common.h:146-147):
    // runs of plain bytes in the `None` state go through `plain_run_avx512`,
    // which decodes them as `decode_scalar_step` would, one window at a time;
    // the common escape and line-break steps go through `tail_step`, which
    // keeps the cursors and state in registers; every other byte goes through
    // the scalar machine.
    let mut st = state.state;
    while src < input.len() {
        if st == DecoderState::None {
            plain_run_avx512(input, &mut src, output, &mut dst);
            if src >= input.len() {
                break;
            }
        }
        if let Some(next) = tail_step(input, &mut src, output, &mut dst, st, mode.dot_unstuffing) {
            st = next;
            continue;
        }
        // Copies, so `src` and `dst` never have their address taken.
        let (mut s, mut d) = (src, dst);
        state.state = st;
        let more = decode_scalar_step(input, &mut s, output, &mut d, state, mode)?;
        (src, dst, st) = (s, d, state.state);
        if !more {
            break;
        }
    }
    state.state = st;

    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

// Decodes the run of plain bytes (not `=`, CR or LF) at `*src` within the
// next 64 input bytes, stopping at the first special byte or at the end of
// either buffer, and advances both cursors past it. Only for the `None`
// state, where `decode_scalar_step` decodes a plain byte as `byte - 42` and
// leaves the state `None`, so the result is that of stepping each byte. The
// masked load and store touch only bytes inside both slices.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2,bmi1,bmi2,popcnt,lzcnt")]
#[inline]
fn plain_run_avx512(input: &[u8], src: &mut usize, output: &mut [u8], dst: &mut usize) {
    use std::arch::x86_64::*;
    let n = input
        .len()
        .saturating_sub(*src)
        .min(output.len().saturating_sub(*dst))
        .min(64);
    if n == 0 {
        return;
    }
    let live: u64 = if n == 64 { !0 } else { (1u64 << n) - 1 };
    // SAFETY: `live` selects bytes `*src..*src + n` of `input` and
    // `*dst..*dst + n` of `output`, all in bounds; masked-off bytes are
    // neither read nor written (AVX-512 masked memory ops suppress them).
    unsafe {
        let v = _mm512_maskz_loadu_epi8(live, input.as_ptr().add(*src) as *const i8);
        let special = (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'=' as i8))
            | _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\r' as i8))
            | _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)))
            & live;
        // Bits below the first special byte, or every live byte.
        let run = if special == 0 {
            live
        } else {
            (special & special.wrapping_neg()) - 1
        };
        _mm512_mask_storeu_epi8(
            output.as_mut_ptr().add(*dst) as *mut i8,
            run,
            _mm512_add_epi8(v, _mm512_set1_epi8(42i8.wrapping_neg())),
        );
        let k = run.count_ones() as usize;
        *src += k;
        *dst += k;
    }
}

// One step of the scalar state machine at `*src` (which must be in bounds)
// for the arms that only move bytes and change state: the new state, or
// `None` when the step is left to `decode_scalar_step`. Each arm does what
// `decode_scalar_step` does in that state for that byte. The arms left to it
// are the ones that can end the article, unstuff a dot, report an error or
// leave a pending escape: `.` or `=` at a line start, `=` as the last byte,
// and any write into a full output.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn tail_step(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    st: DecoderState,
    dot_unstuffing: bool,
) -> Option<DecoderState> {
    let i = *src;
    let byte = input[i];
    let room = *dst < output.len();
    // State after decoding an escaped byte (the `Eq` arm).
    let after_escape = |e: u8| {
        if dot_unstuffing && e == b'\r' {
            DecoderState::Cr
        } else {
            DecoderState::None
        }
    };
    match (st, byte) {
        (DecoderState::None | DecoderState::CrLf, b'\r') => {
            *src = i + 1;
            Some(DecoderState::Cr)
        }
        (DecoderState::None | DecoderState::CrLf, b'\n') => {
            *src = i + 1;
            Some(DecoderState::None)
        }
        (DecoderState::None, b'=') if room && i + 1 < input.len() => {
            let e = input[i + 1];
            output[*dst] = e.wrapping_sub(106);
            *dst += 1;
            *src = i + 2;
            Some(after_escape(e))
        }
        (DecoderState::Eq, _) if room => {
            output[*dst] = byte.wrapping_sub(106);
            *dst += 1;
            *src = i + 1;
            Some(after_escape(byte))
        }
        (DecoderState::Cr, b'\n') => {
            *src = i + 1;
            Some(DecoderState::CrLf)
        }
        // A byte after a lone CR is read again in the `None` state.
        (DecoderState::Cr, _) => Some(DecoderState::None),
        (DecoderState::CrLf, b'.' | b'=') => None,
        (DecoderState::CrLf, _) if room => {
            output[*dst] = byte.wrapping_sub(42);
            *dst += 1;
            *src = i + 1;
            Some(DecoderState::None)
        }
        _ => None,
    }
}

// How far ahead of the window `raw_windows` prefetches its input (0 is
// none). The hardware stream prefetchers stop at every 4 KiB page, so an
// article read from memory waits on each new page without it. One distance
// for every CPU, Zen 4 and Sapphire Rapids alike, issued by every window.
#[cfg(target_arch = "x86_64")]
const PREFETCH: usize = 4096;

// How far ahead of the output cursor `raw_windows` asks for its output lines
// for writing, on Zen 4 in SEARCH_END mode only, so a window's stores find
// their lines already held for writing. The raw mode on Zen 4, and both modes
// on Sapphire Rapids, lost time with it. An article's output goes to memory
// not in cache, so a line asked for must arrive from DRAM before the stores
// reach it: the distance has to cover DRAM latency at the write rate. One
// core writes about 13 bytes a nanosecond here, and DRAM answers in 100-150
// ns, so 2048 bytes (32 lines) is that latency; 512 left the stores waiting
// on the line, and 4096 was slower than 2048 on every real-article row.
#[cfg(target_arch = "x86_64")]
const OUT_PREFETCH: usize = 2048;

// Zen 4 (family 19h with AVX-512; Zen 3 has none) runs 512-bit ops as two
// 256-bit halves, and takes `raw_windows`'s `ZEN4` forms; every other CPU
// keeps the plain loop, which the Zen 4 forms cost 3-5% on Sapphire Rapids.
#[cfg(target_arch = "x86_64")]
fn zen4() -> bool {
    #[cfg(any(fuzzing, test))]
    if let Some(on) = super::fuzzing::zen4_override() {
        return on;
    }
    static ZEN4: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ZEN4.get_or_init(|| x86_cpu_family_model().0 == 0xaf)
}

// 1, 2, ..., 63, 0, 1: from byte 0, `vpermb` indices of the window shifted
// down one byte; from byte 1, two.
#[cfg(target_arch = "x86_64")]
static SHIFT_IDX: [u8; 65] = {
    let mut a = [0u8; 65];
    let mut i = 0;
    while i < 65 {
        a[i] = ((i + 1) % 64) as u8;
        i += 1;
    }
    a
};

// The most windows `end_loop!` runs before `raw_windows` decides its mode
// for the next ones: a chunk with no `=` puts the next in the mode for input
// with no `=` (2 KiB here, a little under 16 lines of 128 bytes).
#[cfg(target_arch = "x86_64")]
const END_CHUNK: usize = 32;

// Where the window loop stopped: byte offsets into input and output, the
// escape carry into the window at `src`, and the pre-merge special mask of
// the window a SEARCH_END probe aborted (None when the loop ran out).
#[cfg(target_arch = "x86_64")]
struct RawExit {
    src: usize,
    dst: usize,
    esc_first: u64,
    break_mask: Option<u64>,
}

#[cfg(target_arch = "x86_64")]
impl RawExit {
    #[inline(always)]
    fn at(
        inp: *const u8,
        sp: *const u8,
        out: *mut u8,
        op: *mut u8,
        esc_first: u64,
        break_mask: Option<u64>,
    ) -> Self {
        RawExit {
            src: sp as usize - inp as usize,
            dst: op as usize - out as usize,
            esc_first,
            break_mask,
        }
    }
}

// The window loop of `decode_kernel_avx512_raw`, out of line so that its
// registers are its own: the input and output cursors, the escape carry and
// the constants stay in registers for the whole loop, with nothing reloaded
// from the stack per window (the inlined form spilled the output base and the
// state pointer and reloaded both every window). Pointer cursors, as the
// oracle's `src+i` / `p` (decoder_avx2_base.h:84, 549-552).
//
// SAFETY (caller): `len > 128`, `inp..inp+len` readable, `out` writable for
// `len` bytes. A window at `sp` reads `sp..sp+WIDTH+LOOKAHEAD`, inside the
// input by the loop bound, and stores 64 bytes at `op <= sp`, inside the
// output since `len(out) >= len`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2,bmi1,bmi2,popcnt,lzcnt")]
#[inline(never)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn raw_windows<const SEARCH_END: bool, const PREFETCH: usize, const ZEN4: bool>(
    inp: *const u8,
    len: usize,
    out: *mut u8,
    mut esc_first: u64,
    entry_zero: u64,
) -> RawExit {
    use std::arch::x86_64::*;
    const WIDTH: usize = 64;

    // Pads (executed once per call) to a 64-byte boundary, so the loop below
    // sits at the same offset within its 64-byte lines however the linker
    // places this function: its speed must not depend on unrelated code.
    std::arch::asm!(".p2align 6", options(nomem, nostack, preserves_flags));

    let sub42 = _mm512_set1_epi8(42i8.wrapping_neg());
    let sub64 = _mm512_set1_epi8(64i8.wrapping_neg());
    let dot = _mm512_set1_epi8(b'.' as i8);
    let eq_needle = _mm512_set1_epi8(b'=' as i8);
    let cr = _mm512_set1_epi8(b'\r' as i8);
    let lf = _mm512_set1_epi8(b'\n' as i8);
    let y_needle = _mm512_set1_epi8(b'y' as i8);
    // Byte i of `idx1` is i+1 and of `idx2` i+2 (mod 64), for `vpermb`.
    let idx1 = _mm512_loadu_si512(SHIFT_IDX.as_ptr() as *const _);
    let idx2 = _mm512_loadu_si512(SHIFT_IDX.as_ptr().add(1) as *const _);
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

    // byte 0 of yenc_offset carries a pending escape (-106 = -42-64); rebuilt per
    // window via `mask_add`, exactly the oracle's `yencOffset`.
    let mut yenc_offset = _mm512_mask_add_epi8(sub42, esc_first, sub42, sub64);
    // min_mask forces a stuffed dot at a carried line start to flag as special
    // (oracle `minMask`).
    let mut min_mask = _mm512_maskz_mov_epi8(!entry_zero, dot);

    // Window span as the oracle's: `lenBuffer` is `width-1 + 3 + 1` with
    // searchEnd, `width-1 + 2` without (decoder_common.h:49-51), and the span
    // `len - lenBuffer` is rounded UP to whole windows (decoder_common.h:130-131),
    // so a window at `sp` runs when `sp + WIDTH + LOOKAHEAD <= inp + len`: the
    // widest view (`+4` with searchEnd, `+2` without) ends at the input's last
    // byte at most. At most `lenBuffer` bytes are left to the scalar tail.
    let lookahead = if SEARCH_END { 4 } else { 2 };
    let last = inp.add(len - (WIDTH + lookahead));
    let mut sp = inp;
    // Nonzero when `end_loop!` starts in its mode for input with no `=`.
    let mut clean = 0usize;
    // The OR of the `=` masks of the windows `end_loop!` ran.
    let mut acc: u64;
    let mut op = out;
    // One window at `sp`: decodes it, stores at `op`, advances both, or
    // returns from `raw_windows` when a SEARCH_END probe aborts it.
    macro_rules! window {
        () => {{
            let v = _mm512_loadu_si512(sp as *const _);

            let mut mask: u64 = _mm512_cmpeq_epi8_mask(
                v,
                _mm512_shuffle_epi8(special_lut, _mm512_min_epu8(v, min_mask)),
            );

            if mask != 0 {
                let mask_eq: u64 = _mm512_cmpeq_epi8_mask(v, eq_needle);

                if mask != mask_eq {
                    // \r\n. dot-stuffing detection (oracle match2CrXDt / m2nldot).
                    let cr_mask: u64 = _mm512_cmpeq_epi8_mask(v, cr);
                    // Without SEARCH_END the arm tests one shape, `\r\n.` from a
                    // `\r` at `i` to `i+2`. With no `\r` in the last two bytes it
                    // ends inside the window and comes from shifts of in-window
                    // masks, so the window reads nothing of the next line: reading
                    // it early stalls on input not yet in cache when every window takes
                    // this arm (a `.` on most lines). Otherwise, and with
                    // SEARCH_END, the arm below reads the shifted views. Zen 4
                    // only: on Sapphire Rapids the shifted views cost less.
                    let in_window = ZEN4 && !SEARCH_END && (cr_mask >> 62) == 0;
                    if in_window {
                        let m2cr_mask = cr_mask & (_mm512_cmpeq_epi8_mask(v, dot) >> 2);
                        if m2cr_mask != 0 {
                            let lf_mask: u64 = _mm512_cmpeq_epi8_mask(v, lf);
                            mask |= (m2cr_mask & (lf_mask >> 1)) << 2;
                        }
                        min_mask = dot;
                    }
                    if !in_window {
                        let tmp2 = _mm512_loadu_si512(sp.add(2) as *const _);
                        // `=` at lane+2 (oracle `match2EqMask`, the AVX3 mask arm of
                        // decoder_avx2_base.h:148-152 at 512-bit width).
                        let match2_eq: u64 = if SEARCH_END {
                            _mm512_cmpeq_epi8_mask(eq_needle, tmp2)
                        } else {
                            0
                        };
                        let m2cr_mask: u64 = _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp2, dot);
                        if m2cr_mask != 0 {
                            let tmp1 = _mm512_loadu_si512(sp.add(1) as *const _);
                            let m1nl_mask: u64 = _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp1, lf);
                            let m2nldot_mask = m2cr_mask & m1nl_mask;

                            // Terminator probe with a stuffed dot in the window
                            // (oracle decoder_avx2_base.h:222-327): `\r\n.\r\n`,
                            // `\r\n.=y` and `\r\n=y`, in k-mask form. Runs BEFORE the
                            // `mask` merge, so an aborted window reports the pre-merge
                            // mask to the no-backtrack exit rule.
                            if SEARCH_END {
                                let tmp3 = _mm512_loadu_si512(sp.add(3) as *const _);
                                let tmp4 = _mm512_loadu_si512(sp.add(4) as *const _);
                                // "`=y` at lane+2" (oracle match3EqY).
                                let m3eqy: u64 =
                                    _mm512_mask_cmpeq_epi8_mask(match2_eq, tmp3, y_needle);
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
                                let m4nl: u64 = _mm512_cmpeq_epi8_mask(tmp3, cr)
                                    & _mm512_cmpeq_epi8_mask(tmp4, lf);
                                // `\r\n.` + (`\r\n` | `=y`) at lane+3, and `\r\n=y`.
                                let m4end = (m4nl | m34eqy) & m2nldot_mask;
                                let m3end = m3eqy & m1nl_mask;
                                if (m4end | m3end) != 0 {
                                    return RawExit::at(inp, sp, out, op, esc_first, Some(mask));
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
                                let tmp3 = _mm512_loadu_si512(sp.add(3) as *const _);
                                let m3eqy: u64 =
                                    _mm512_mask_cmpeq_epi8_mask(match2_eq, tmp3, y_needle);
                                if m3eqy != 0 {
                                    let tmp1 = _mm512_loadu_si512(sp.add(1) as *const _);
                                    let m1nl_mask: u64 =
                                        _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp1, lf);
                                    if (m3eqy & m1nl_mask) != 0 {
                                        return RawExit::at(
                                            inp,
                                            sp,
                                            out,
                                            op,
                                            esc_first,
                                            Some(mask),
                                        );
                                    }
                                }
                            }
                            min_mask = dot;
                        }
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
                    _mm512_storeu_si512(op as *mut _, decoded);
                    op = op.add(WIDTH);
                } else {
                    // The entry gate + tail guarantee ≥64 spare output bytes, so
                    // the full-width compressed store is always in bounds; bytes
                    // past `keep` are overwritten by the next store.
                    let keep = !skip;
                    _mm512_storeu_si512(op as *mut _, _mm512_maskz_compress_epi8(keep, decoded));
                    op = op.add(keep.count_ones() as usize);
                }
            } else {
                _mm512_storeu_si512(op as *mut _, _mm512_add_epi8(v, yenc_offset));
                op = op.add(WIDTH);
                esc_first = 0;
                yenc_offset = sub42;
            }
            sp = sp.add(WIDTH);
        }};
    }
    // The SEARCH_END window without data-dependent branches on the common
    // path. `window!` branches on "any special byte" and on "any CR or LF",
    // and neither outcome follows a pattern on real data: about one window in
    // five has no special byte. On the captured articles those two branches
    // took about 2,100 mispredictions per article on Sapphire Rapids and
    // 3,100 on Zen 4 (perf stat, real-full), and the form below takes under
    // 50 and about 1,150, for 4% and 15% fewer cycles there. Every window
    // decodes and compresses, and the only branch the data drives is the rare
    // one: a `.` two bytes after a CR (a stuffed dot or a terminator) or a
    // `=y` (a control line), which takes `window!`'s probe and dot logic
    // unchanged. Without either, that logic leaves `mask` as it is and
    // `min_mask` all dots, so skipping it changes nothing.
    //
    // The SEARCH_END windows run as loops in inline assembly. Compiled from
    // intrinsics, each window moved five masks to general registers and three
    // back (a mask move costs about 1.4 cycles on Zen 4) and ran the escape
    // and rare-case logic on the copies. Here the rare test and the `==` test
    // set the flags straight from the mask registers (`kortestq`, `ktestq`),
    // and the escaped bytes, the carried one included, take their -64 from
    // one masked add, so no offset vector is rebuilt per window.
    //
    // `end_loop!` runs windows while `sp <= $stop`, in one of two modes. The
    // first decodes every window the same way, with no branch the data
    // drives on the common path, and ORs each window's `=` mask into `acc`.
    // The second (entered when `clean` is nonzero) is for input with no `=`
    // at all, such as long runs of zero bytes, whose line ends fall in a
    // pattern the branch predictor learns: a window with no special byte is
    // stored whole on a branch of its own; one with a `=` drops back to the
    // first mode. A window that is rare (as `window_end_slow!`'s test) or
    // holds a `=` after an escape `=` stops the loop before storing anything,
    // with `sp`, `op`, `esc_first` and `min_mask` as that window found them,
    // and the value is 1; that window then goes through `window_end_slow!`.
    // Otherwise the value is 0. `$pre` runs at the top of every window (the
    // prefetches); `$rare` is the rare test, leaving its result in `ka` and
    // `kc`; `$extra` are the operands those use. The escape carry stays in a
    // mask register (bit 0) from window to window.
    macro_rules! end_loop {
        ([$($pre:literal),*], [$($rare:literal),*], $stop:expr, $($extra:tt)*) => {{
            let slow: u32;
            let _v: __m512i;
            let _t: __m512i;
            std::arch::asm!(
                "test {clean}, {clean}",
                "jnz 7f",
                ".p2align 6",
                "2:",
                $($pre,)*
                "vmovdqu64 {v:z}, zmmword ptr [{sp}]",
                "vpminub {t:z}, {v:z}, {minm:z}",
                "vpshufb {t:z}, {lut:z}, {t:z}",
                // Bytes that are not special.
                "vpcmpub {kns}, {v:z}, {t:z}, 4",
                "3:",
                "vpcmpeqb {ke}, {v:z}, {eqv:z}",
                "vpcmpeqb {kc}, {v:z}, {crv:z}",
                $($rare,)*
                "kortestq {ka}, {kc}",
                "jnz 4f",
                // Escaped bytes: after a `=`, and byte 0 after a carried one.
                "kshiftlq {ka}, {ke}, 1",
                "korq {ka}, {ka}, {esc}",
                "ktestq {ka}, {ke}",
                "jnz 4f",
                "korq {acc}, {acc}, {ke}",
                "kshiftrq {esc}, {ke}, 63",
                "vmovdqa64 {minm:z}, {dotv:z}",
                "vpaddb {t:z}, {v:z}, {sub42:z}",
                "vpaddb {t:z}{{{ka}}}, {t:z}, {sub64:z}",
                // Kept bytes: not special, or escaped.
                "korq {kc}, {kns}, {ka}",
                "vpcompressb {t:z}{{{kc}}}{{z}}, {t:z}",
                "vmovdqu64 zmmword ptr [{op}], {t:z}",
                "kmovq {n}, {kc}",
                "popcnt {n}, {n}",
                "add {op}, {n}",
                "add {sp}, 64",
                "cmp {sp}, {stop}",
                "jbe 2b",
                "jmp 6f",
                // No `=` so far, so no escape is carried.
                ".p2align 4",
                "7:",
                "cmp {sp}, {stop}",
                "ja 6f",
                $($pre,)*
                "vmovdqu64 {v:z}, zmmword ptr [{sp}]",
                "vpminub {t:z}, {v:z}, {minm:z}",
                "vpshufb {t:z}, {lut:z}, {t:z}",
                "vpcmpub {kns}, {v:z}, {t:z}, 4",
                "kortestq {kns}, {kns}",
                "jnc 8f",
                "vpaddb {t:z}, {v:z}, {sub42:z}",
                "vmovdqu64 zmmword ptr [{op}], {t:z}",
                "add {op}, 64",
                "add {sp}, 64",
                "vmovdqa64 {minm:z}, {dotv:z}",
                "jmp 7b",
                "8:",
                "vpcmpeqb {ke}, {v:z}, {eqv:z}",
                "kortestq {ke}, {ke}",
                "jnz 3b",
                "vpcmpeqb {kc}, {v:z}, {crv:z}",
                $($rare,)*
                "kortestq {ka}, {kc}",
                "jnz 4f",
                "vmovdqa64 {minm:z}, {dotv:z}",
                "vpaddb {t:z}, {v:z}, {sub42:z}",
                "vpcompressb {t:z}{{{kns}}}{{z}}, {t:z}",
                "vmovdqu64 zmmword ptr [{op}], {t:z}",
                "kmovq {n}, {kns}",
                "popcnt {n}, {n}",
                "add {op}, {n}",
                "add {sp}, 64",
                "jmp 7b",
                "6:",
                "xor {slow:e}, {slow:e}",
                "jmp 5f",
                "4:",
                "mov {slow:e}, 1",
                "5:",
                slow = out(reg) slow,
                sp = inout(reg) sp,
                op = inout(reg) op,
                stop = in(reg) $stop,
                clean = in(reg) clean,
                n = out(reg) _,
                esc = inout(kreg) esc_first,
                acc = inout(kreg) acc,
                kns = out(kreg) _,
                ke = out(kreg) _,
                kc = out(kreg) _,
                ka = out(kreg) _,
                v = out(zmm_reg) _v,
                t = out(zmm_reg) _t,
                minm = inout(zmm_reg) min_mask,
                lut = in(zmm_reg) special_lut,
                eqv = in(zmm_reg) eq_needle,
                crv = in(zmm_reg) cr,
                dotv = in(zmm_reg) dot,
                sub42 = in(zmm_reg) sub42,
                sub64 = in(zmm_reg) sub64,
                $($extra)*
                options(nostack),
            );
            slow
        }};
    }
    // `end_loop!` for Zen 4, where a mask instruction is dear: the mask
    // forms of `end_loop!` cost more cycles than the compiled window there
    // on a body with no special bytes (perf stat). Here the rare test is two masked
    // compares against permuted copies of the window (byte i+2 and i+1, with
    // `.` and `y` past the end, so a CR in the last two bytes and a `=` in
    // the last are flagged as `window_end_slow!` flags them) and one
    // `kortestq`; the escape logic and `acc` are in general registers, with
    // the carry in `esc_first`; a window moves two masks out and one in.
    macro_rules! end_loop_zen4 {
        ([$($pre:literal),*], $stop:expr, $($extra:tt)*) => {{
            let slow: u32;
            let _v: __m512i;
            let _t: __m512i;
            let _u: __m512i;
            std::arch::asm!(
                "test {clean}, {clean}",
                "jnz 7f",
                ".p2align 6",
                "2:",
                $($pre,)*
                "vmovdqu64 {v:z}, zmmword ptr [{sp}]",
                "vpminub {t:z}, {v:z}, {minm:z}",
                "vpshufb {t:z}, {lut:z}, {t:z}",
                "vpcmpub {kns}, {v:z}, {t:z}, 4",
                "3:",
                "vpcmpeqb {ke}, {v:z}, {eqv:z}",
                "vpcmpeqb {kc}, {v:z}, {crv:z}",
                // A CR with `.` two on (or in the last two bytes).
                "vmovdqa64 {u:z}, {dotv:z}",
                "vpermb {u:z}{{{lo62}}}, {idx2:z}, {v:z}",
                "vpcmpeqb {ka}{{{kc}}}, {u:z}, {dotv:z}",
                // A `=y` (or `=` as the last byte).
                "vmovdqa64 {u:z}, {yv:z}",
                "vpermb {u:z}{{{lo63}}}, {idx1:z}, {v:z}",
                "vpcmpeqb {kc}{{{ke}}}, {u:z}, {yv:z}",
                "kortestq {ka}, {kc}",
                "jnz 4f",
                "kmovq {e}, {ke}",
                // Escaped bytes: after a `=`, and byte 0 after a carried one.
                "lea {x}, [{esc} + 2*{e}]",
                "test {x}, {e}",
                "jnz 4f",
                "or {acc}, {e}",
                "shr {e}, 63",
                "mov {esc}, {e}",
                "kmovq {ka}, {x}",
                "vmovdqa64 {minm:z}, {dotv:z}",
                "vpaddb {t:z}, {v:z}, {sub42:z}",
                "vpaddb {t:z}{{{ka}}}, {t:z}, {sub64:z}",
                "korq {kc}, {kns}, {ka}",
                "vpcompressb {t:z}{{{kc}}}{{z}}, {t:z}",
                "vmovdqu64 zmmword ptr [{op}], {t:z}",
                "kmovq {e}, {kc}",
                "popcnt {e}, {e}",
                "add {op}, {e}",
                "add {sp}, 64",
                "cmp {sp}, {stop}",
                "jbe 2b",
                "jmp 6f",
                // No `=` so far, so no escape is carried.
                ".p2align 4",
                "7:",
                "cmp {sp}, {stop}",
                "ja 6f",
                $($pre,)*
                "vmovdqu64 {v:z}, zmmword ptr [{sp}]",
                "vpminub {t:z}, {v:z}, {minm:z}",
                "vpshufb {t:z}, {lut:z}, {t:z}",
                "vpcmpub {kns}, {v:z}, {t:z}, 4",
                "kortestq {kns}, {kns}",
                "jnc 8f",
                "vpaddb {t:z}, {v:z}, {sub42:z}",
                "vmovdqu64 zmmword ptr [{op}], {t:z}",
                "add {op}, 64",
                "add {sp}, 64",
                "vmovdqa64 {minm:z}, {dotv:z}",
                "jmp 7b",
                "8:",
                "vpcmpeqb {ke}, {v:z}, {eqv:z}",
                "kortestq {ke}, {ke}",
                "jnz 3b",
                // With no `=`, only the CR half of the rare test can hit.
                "vpcmpeqb {kc}, {v:z}, {crv:z}",
                "vmovdqa64 {u:z}, {dotv:z}",
                "vpermb {u:z}{{{lo62}}}, {idx2:z}, {v:z}",
                "vpcmpeqb {ka}{{{kc}}}, {u:z}, {dotv:z}",
                "kortestq {ka}, {ka}",
                "jnz 4f",
                "vmovdqa64 {minm:z}, {dotv:z}",
                "vpaddb {t:z}, {v:z}, {sub42:z}",
                "vpcompressb {t:z}{{{kns}}}{{z}}, {t:z}",
                "vmovdqu64 zmmword ptr [{op}], {t:z}",
                "kmovq {e}, {kns}",
                "popcnt {e}, {e}",
                "add {op}, {e}",
                "add {sp}, 64",
                "jmp 7b",
                "6:",
                "xor {slow:e}, {slow:e}",
                "jmp 5f",
                "4:",
                "mov {slow:e}, 1",
                "5:",
                slow = out(reg) slow,
                sp = inout(reg) sp,
                op = inout(reg) op,
                stop = in(reg) $stop,
                clean = in(reg) clean,
                acc = inout(reg) acc,
                e = out(reg) _,
                x = out(reg) _,
                esc = inout(reg) esc_first,
                kns = out(kreg) _,
                ke = out(kreg) _,
                kc = out(kreg) _,
                ka = out(kreg) _,
                lo62 = in(kreg) (1u64 << 62) - 1,
                lo63 = in(kreg) (1u64 << 63) - 1,
                v = out(zmm_reg) _v,
                t = out(zmm_reg) _t,
                u = out(zmm_reg) _u,
                minm = inout(zmm_reg) min_mask,
                lut = in(zmm_reg) special_lut,
                eqv = in(zmm_reg) eq_needle,
                crv = in(zmm_reg) cr,
                dotv = in(zmm_reg) dot,
                yv = in(zmm_reg) y_needle,
                idx1 = in(zmm_reg) idx1,
                idx2 = in(zmm_reg) idx2,
                sub42 = in(zmm_reg) sub42,
                sub64 = in(zmm_reg) sub64,
                $($extra)*
                options(nostack),
            );
            slow
        }};
    }
    // `end_loop!` for this CPU over at most `END_CHUNK` windows, with or
    // without the prefetches, then the window it stopped at, if any. The
    // next chunk starts in the second mode when this one ran out with no
    // `=` in it; a `=` in any window, or an escape carried out of a window
    // `window_end_slow!` decoded, puts it back in the first.
    macro_rules! end_run {
        ($stop:expr, $pf:literal) => {{
            let stop = (sp as usize + (END_CHUNK - 1) * WIDTH).min($stop as usize);
            acc = 0;
            let slow = match (ZEN4, $pf) {
                (true, true) => end_loop_zen4!(
                    [
                        "prefetcht0 byte ptr [{sp} + {pf}]",
                        "prefetcht0 byte ptr [{op} + {opf}]"
                    ],
                    stop,
                    pf = const PREFETCH,
                    opf = const OUT_PREFETCH,
                ),
                (true, false) => end_loop_zen4!([], stop,),
                // Elsewhere from the shifted views: a CR with a `.` two on,
                // or a `=y` two on.
                (false, true) => end_loop!(
                    ["prefetcht0 byte ptr [{sp} + {pf}]"],
                    [
                        "vpcmpeqb {ka}{{{kc}}}, {dotv:z}, zmmword ptr [{sp} + 2]",
                        "vpcmpeqb {kc}, {eqv:z}, zmmword ptr [{sp} + 2]",
                        "vpcmpeqb {kc}{{{kc}}}, {yv:z}, zmmword ptr [{sp} + 3]"
                    ],
                    stop,
                    pf = const PREFETCH,
                    yv = in(zmm_reg) y_needle,
                ),
                (false, false) => end_loop!(
                    [],
                    [
                        "vpcmpeqb {ka}{{{kc}}}, {dotv:z}, zmmword ptr [{sp} + 2]",
                        "vpcmpeqb {kc}, {eqv:z}, zmmword ptr [{sp} + 2]",
                        "vpcmpeqb {kc}{{{kc}}}, {yv:z}, zmmword ptr [{sp} + 3]"
                    ],
                    stop,
                    yv = in(zmm_reg) y_needle,
                ),
            };
            if slow != 0 {
                window_end_slow!();
                if acc != 0 || esc_first != 0 {
                    clean = 0;
                }
            } else {
                clean = (acc == 0) as usize;
            }
        }};
    }
    macro_rules! window_end_slow {
        () => {{
            let v = _mm512_loadu_si512(sp as *const _);
            let mut mask: u64 = _mm512_cmpeq_epi8_mask(
                v,
                _mm512_shuffle_epi8(special_lut, _mm512_min_epu8(v, min_mask)),
            );
            let mask_eq: u64 = _mm512_cmpeq_epi8_mask(v, eq_needle);
            let cr_mask: u64 = _mm512_cmpeq_epi8_mask(v, cr);
            // Whether the rare logic can do anything. On Zen 4 from the
            // window's own bytes, which spares the two shifted loads (a
            // 512-bit load is two there): a CR with a `.` two on, or a `=y`,
            // inside the window, or a CR in its last two bytes or a `=` in
            // its last, whose partners lie in the next window. Every window
            // the exact test below flags is flagged here too.
            let rare = if ZEN4 {
                let dot_mask: u64 = _mm512_cmpeq_epi8_mask(v, dot);
                let y_mask: u64 = _mm512_cmpeq_epi8_mask(v, y_needle);
                (cr_mask & ((dot_mask >> 2) | (3u64 << 62)))
                    | (mask_eq & ((y_mask >> 1) | (1u64 << 63)))
            } else {
                let tmp2 = _mm512_loadu_si512(sp.add(2) as *const _);
                let tmp3 = _mm512_loadu_si512(sp.add(3) as *const _);
                _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp2, dot)
                    | _mm512_mask_cmpeq_epi8_mask(
                        _mm512_cmpeq_epi8_mask(eq_needle, tmp2),
                        tmp3,
                        y_needle,
                    )
            };
            if rare != 0 {
                let tmp2 = _mm512_loadu_si512(sp.add(2) as *const _);
                let tmp3 = _mm512_loadu_si512(sp.add(3) as *const _);
                let match2_eq: u64 = _mm512_cmpeq_epi8_mask(eq_needle, tmp2);
                let m2cr_mask: u64 = _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp2, dot);
                let m3eqy: u64 = _mm512_mask_cmpeq_epi8_mask(match2_eq, tmp3, y_needle);
                let tmp1 = _mm512_loadu_si512(sp.add(1) as *const _);
                let m1nl_mask: u64 = _mm512_mask_cmpeq_epi8_mask(cr_mask, tmp1, lf);
                if m2cr_mask != 0 {
                    let m2nldot_mask = m2cr_mask & m1nl_mask;
                    // As `window!`'s stuffed-dot probe (oracle
                    // decoder_avx2_base.h:222-327).
                    let tmp4 = _mm512_loadu_si512(sp.add(4) as *const _);
                    let m34eqy: u64 = _mm512_cmpeq_epi8_mask(tmp3, eq_needle)
                        & _mm512_cmpeq_epi8_mask(tmp4, y_needle);
                    let m4nl: u64 =
                        _mm512_cmpeq_epi8_mask(tmp3, cr) & _mm512_cmpeq_epi8_mask(tmp4, lf);
                    let m4end = (m4nl | m34eqy) & m2nldot_mask;
                    let m3end = m3eqy & m1nl_mask;
                    if (m4end | m3end) != 0 {
                        return RawExit::at(inp, sp, out, op, esc_first, Some(mask));
                    }
                    mask |= m2nldot_mask << 2;
                    min_mask = _mm512_maskz_mov_epi8(!(m2nldot_mask >> 62), dot);
                } else {
                    // As `window!`'s probe without a stuffed dot (oracle
                    // decoder_avx2_base.h:344-421).
                    if (m3eqy & m1nl_mask) != 0 {
                        return RawExit::at(inp, sp, out, op, esc_first, Some(mask));
                    }
                    min_mask = dot;
                }
            } else {
                min_mask = dot;
            }

            let esc_first_in = esc_first;
            let eq_shift1 = (mask_eq << 1) | esc_first_in;
            let fixed_eq = if (mask_eq & eq_shift1) != 0 {
                fix_eq_mask(mask_eq, eq_shift1)
            } else {
                mask_eq
            };
            let escaped = (fixed_eq << 1) | esc_first_in;
            esc_first = fixed_eq >> 63;
            // -106 on the escaped bytes (the carried one included), -42 on the rest.
            let data = _mm512_add_epi8(v, sub42);
            let decoded = _mm512_mask_add_epi8(data, escaped, data, sub64);
            // The entry gate + tail guarantee ≥64 spare output bytes.
            let keep = !(mask & !escaped);
            _mm512_storeu_si512(op as *mut _, _mm512_maskz_compress_epi8(keep, decoded));
            op = op.add(keep.count_ones() as usize);
            sp = sp.add(WIDTH);
        }};
    }
    // The input is prefetched `PREFETCH` bytes ahead of the window, never
    // past its last byte. The hardware stream prefetchers stop at every 4 KiB
    // page, so without this an article read from memory waits on each new
    // page; a prefetch only reads, faults on nothing, and changes no result.
    // The last line goes first; the windows whose line `PREFETCH` on is still
    // inside the input then prefetch it (one instruction, no clamp), and the
    // rest prefetch nothing, as everything ahead of them is already asked for.
    if PREFETCH != 0 {
        let pf_last = inp.add(len - 1);
        _mm_prefetch::<_MM_HINT_T0>(pf_last as *const i8);
        if len > PREFETCH {
            let pf_stop = last.min(pf_last.sub(PREFETCH));
            while sp <= pf_stop {
                if SEARCH_END {
                    // `end_loop!` prefetches as the plain loop below does.
                    end_run!(pf_stop, true);
                } else {
                    _mm_prefetch::<_MM_HINT_T0>(sp.add(PREFETCH) as *const i8);
                    window!();
                }
            }
        }
    }
    while sp <= last {
        if SEARCH_END {
            end_run!(last, false);
        } else {
            window!();
        }
    }
    RawExit::at(inp, sp, out, op, esc_first, None)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2,bmi1,bmi2,popcnt,lzcnt")]
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

// Line-aware 64-byte-block driver for the AVX-512/VBMI2 tier: consult the
// caller's line-length hint, try the whole-line fast path first, and fall
// back to the generic 64-byte block decode. Mirrors the SSSE3 line-aware
// kernel structure (`decode_kernel_simd64_ssse3_line_aware`) at 512-bit
// width.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2,bmi1,bmi2,popcnt,lzcnt")]
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

// Whole-line fast path for the AVX-512/VBMI2 tier: decode one complete yEnc
// line (hint-length plus CRLF) in a single pass when the window holds it,
// bailing to the block path on escapes at boundaries, stuffed dots, or short
// input. Same guards and bail conditions as `try_decode_ssse3_line`, with
// one 512-bit vector per 64-byte chunk, k-register masks, and full-width
// vpcompressb compaction.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vl,avx512vbmi2,avx512bw,avx512f,avx2,bmi1,bmi2,popcnt,lzcnt")]
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

#[cfg(all(test, target_arch = "x86_64"))]
mod tests {
    use super::*;

    // The window loop's exit state, against the scalar machine: a body of
    // plain bytes ending in each trailing shape, so the shape lands at every
    // offset of the last window and on every side of the loop's exit, with a
    // few bytes after it. Consumed, written, output, end, state and error must
    // all match, in every mode and from both line-start entry states.
    #[test]
    fn avx512_exit_state_matches_scalar_for_trailing_shapes() {
        if !vbmi2_tier_available() {
            return;
        }
        const SHAPES: [&[u8]; 9] = [
            b"\r", b"\r\n", b"\r\n=", b"=\r\n", b"\r\n.", b"\r\n.=", b"=\r", b"\r\n.\r", b"=",
        ];
        const AFTER: &[u8] = b"y\r\n.ab";
        for len in 129..=129 + 2 * 64 + 8 {
            for shape in SHAPES {
                for after in 0..=AFTER.len() {
                    let body_len = len - shape.len();
                    let mut input = vec![b'a'; body_len];
                    input.extend_from_slice(shape);
                    input.extend_from_slice(&AFTER[..after]);
                    for entry in [DecoderState::CrLf, DecoderState::None] {
                        for search_end in [false, true] {
                            for preserve_pending in [false, true] {
                                let run = |vbmi2: bool| {
                                    let mut output = vec![0u8; input.len() + 64];
                                    let mut state = KernelState {
                                        state: entry,
                                        end: DecodeEnd::None,
                                        line_length: None,
                                    };
                                    let outcome = if vbmi2 {
                                        unsafe {
                                            decode_kernel_avx512_vbmi2(
                                                &input,
                                                &mut output,
                                                &mut state,
                                                true,
                                                preserve_pending,
                                                search_end,
                                            )
                                        }
                                    } else {
                                        decode_kernel_scalar(
                                            &input,
                                            &mut output,
                                            &mut state,
                                            true,
                                            preserve_pending,
                                            search_end,
                                        )
                                    };
                                    let outcome = outcome.map(|o| {
                                        output.truncate(o.written);
                                        (o.consumed, o.written, o.end, output)
                                    });
                                    (format!("{outcome:?}"), state)
                                };
                                assert_eq!(
                                    run(true),
                                    run(false),
                                    "len {len} shape {shape:?} after {after} entry {entry:?} \
                                     search_end {search_end} preserve_pending {preserve_pending}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
