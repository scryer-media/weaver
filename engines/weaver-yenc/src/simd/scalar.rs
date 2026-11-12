// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub(super) fn decode_kernel_scalar(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    let mut src = 0usize;
    let mut dst = 0usize;
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };

    while (!search_end || state.end == DecodeEnd::None) && src < input.len() {
        // The `None` state with room for one output byte per input byte left
        // runs the register-only main loop; everything it leaves (the last two
        // bytes, a pending state, a dot-stuffed line in end mode, a compact
        // output) goes through the state machine one step at a time.
        if state.state == DecoderState::None && output.len() - dst >= input.len() - src {
            // With room for the whole input (the SIMD tiers' contract), a word
            // holding a special byte is stored whole and the bytes past the
            // special one are rewritten later, as the SIMD tiers' full-width
            // stores are; without it, nothing past the decoded bytes is
            // written.
            (src, dst) = if output.len() >= input.len() {
                decode_none_main_loop::<true>(
                    input,
                    src,
                    output,
                    dst,
                    state,
                    dot_unstuffing,
                    search_end,
                )
            } else {
                decode_none_main_loop::<false>(
                    input,
                    src,
                    output,
                    dst,
                    state,
                    dot_unstuffing,
                    search_end,
                )
            };
            if search_end && state.end != DecodeEnd::None {
                break;
            }
        }
        if src >= input.len()
            || !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)?
        {
            break;
        }
    }

    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

// `=`, CR and LF: the bytes the main loop does not decode as `byte - 42`.
// A table, not a range test and bit test: a range test on random data is a
// branch taken a quarter of the time either way, a table load is one branch
// that is taken only at a special byte, like rapidyenc's three compares.
static SPECIAL_BYTE: [bool; 256] = {
    let mut t = [false; 256];
    t[b'\n' as usize] = true;
    t[b'\r' as usize] = true;
    t[b'=' as usize] = true;
    t
};

const LOW_BITS: u64 = 0x0101_0101_0101_0101;
const HIGH_BITS: u64 = 0x8080_8080_8080_8080;

// Nonzero iff some byte of `word` is `=`, CR or LF. Each term is the exact
// zero-byte test (`(v - 1) & !v & 0x80` per byte): it is nonzero iff `v` has
// a zero byte, and no bit is set below the lowest one.
#[inline(always)]
fn special_bytes(word: u64) -> u64 {
    let zero = |v: u64| v.wrapping_sub(LOW_BITS) & !v & HIGH_BITS;
    zero(word ^ (LOW_BITS * b'\n' as u64))
        | zero(word ^ (LOW_BITS * b'\r' as u64))
        | zero(word ^ (LOW_BITS * b'=' as u64))
}

// `byte - 42` (wrapping) in every byte of `word`. With each byte's top bit
// set first, subtracting 42 never borrows into the next byte and leaves the
// low seven bits right; the top bit is then right where the input byte's top
// bit was set and flipped where it was clear.
#[inline(always)]
fn sub_42_per_byte(word: u64) -> u64 {
    ((word | HIGH_BITS).wrapping_sub(LOW_BITS * 42)) ^ (!word & HIGH_BITS)
}

// The scalar main loop, rapidyenc's shape: do_decode_noend_scalar's
// (decoder.cc:52-71) and do_decode_end_scalar's (decoder.cc:225-286) loop
// over all but the last two bytes, with the cursors in registers, one byte
// test per plain byte, and `=`, CR and LF decided by looking up to three
// bytes ahead instead of through a carried state. Entered and left in the
// `None` state; every byte it consumes leaves exactly the output, cursors and
// state that `decode_scalar_step` would, one byte at a time:
//
// - `=x` writes `x - 106`. In raw mode an escaped CR is consumed again as a
//   CR (decoder.cc:66), which is the `Cr` state `Eq` would leave.
// - A CR not followed by LF is dropped (`Cr` then `None`), as is a lone LF.
// - CR LF then a byte `b` that exists: the `CrLf` state differs from `None`
//   only in raw mode, and only for `.` (dot-stuffing) and for `=` (`=y` ends
//   the part in end mode; a trailing `=` stays pending as `CrLfEq`). So the
//   pair is dropped and `b` decoded as in `None`, except: CR LF `.` with a
//   byte after it drops all three (decoder.cc:58; `CrLfDot` decodes that
//   byte as `None` does when not searching for the end); CR LF `=` in end
//   mode checks for `y` (decoder.cc:259-273); and the end-mode dot-stuffed
//   line and a CR LF `.` or `=` at the end of input are left to the state
//   machine.
//
// Needs `output.len() - dst >= input.len() - src`: each input byte decodes to
// at most one output byte, so the writes below never run past `output`.
#[inline(always)]
fn decode_none_main_loop<const WIDE_STORE: bool>(
    input: &[u8],
    mut i: usize,
    output: &mut [u8],
    mut d: usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
    search_end: bool,
) -> (usize, usize) {
    let len = input.len();
    debug_assert!(state.state == DecoderState::None);
    debug_assert!(output.len() - d >= len - i);
    // Every arm consumes at least as many input bytes as it writes, so
    // `output.len() - d >= len - i` holds throughout, and `d < output.len()`
    // at every write (some input is left whenever the loop runs).
    #[inline(always)]
    fn put(output: &mut [u8], d: &mut usize, byte: u8) {
        debug_assert!(*d < output.len());
        // SAFETY: `*d < output.len()` by the invariant above.
        unsafe { *output.get_unchecked_mut(*d) = byte };
        *d += 1;
    }
    let lim = len.saturating_sub(2);
    'main: while i < lim {
        // Eight bytes at a time while no `=`, CR or LF is among them; a clean
        // word moves the cursors by a constant 8, so the next word's load
        // never waits on this one's bytes. A word that holds a special byte
        // has its leading plain bytes decoded, by the whole-word store under
        // `WIDE_STORE` (no per-byte loop whose exit mispredicts, and the
        // special byte taken from the word, not reloaded) and one at a time
        // below otherwise, and the special byte handled as rapidyenc's loop
        // does.
        let c = 'byte: {
            if i + 8 <= lim {
                let word = u64::from_le_bytes(input[i..i + 8].try_into().unwrap());
                let special = special_bytes(word);
                if special == 0 || WIDE_STORE {
                    debug_assert!(d + 8 <= output.len());
                    // SAFETY: `output.len() - d >= len - i >= 8` by the invariant.
                    unsafe { output.get_unchecked_mut(d..d + 8) }
                        .copy_from_slice(&sub_42_per_byte(word).to_le_bytes());
                }
                if special == 0 {
                    i += 8;
                    d += 8;
                    continue 'main;
                }
                if WIDE_STORE {
                    // The first special byte's lane.
                    let lane = special.trailing_zeros() / 8;
                    i += lane as usize;
                    d += lane as usize;
                    break 'byte (word >> (lane * 8)) as u8;
                }
            }
            let c = input[i];
            if !SPECIAL_BYTE[c as usize] {
                put(output, &mut d, c.wrapping_sub(42));
                i += 1;
                continue 'main;
            }
            c
        };
        if c == b'=' {
            // A run of escapes stays here rather than retrying the word test
            // between them (a whole line of them, in the worst case).
            // The cursor always moves two bytes and the rare escaped CR leaves
            // the run by stepping back onto it: a loop exit, which the
            // compiler cannot turn into a select, so the cursor does not wait
            // on the escaped byte's load and the next escape's loads start
            // at once.
            loop {
                let e = input[i + 1];
                put(output, &mut d, e.wrapping_sub(106));
                i += 2;
                if dot_unstuffing && e == b'\r' {
                    i -= 1;
                    break;
                }
                if i >= lim || input[i] != b'=' {
                    break;
                }
            }
            continue;
        }
        if c == b'\n' || input[i + 1] != b'\n' {
            i += 1;
            continue;
        }
        // CR LF then a final `=`: the state machine fails it (or keeps it
        // pending) from `CrLf`, which the error path leaves visible.
        if i + 3 >= len && input[i + 2] == b'=' {
            break;
        }
        if !dot_unstuffing {
            i += 2;
            continue;
        }
        match input[i + 2] {
            b'.' => {
                // Two bytes after the dot, so a trailing `=` after it fails
                // (or stays pending) in the state machine, from `CrLfDot`.
                if search_end || i + 4 >= len {
                    break;
                }
                i += 3;
            }
            b'=' => {
                if i + 3 >= len {
                    break;
                }
                if !search_end {
                    i += 2;
                    continue;
                }
                let e = input[i + 3];
                if e == b'y' {
                    state.end = DecodeEnd::Control;
                    return (i + 4, d);
                }
                put(output, &mut d, e.wrapping_sub(106));
                i += if e == b'\r' { 3 } else { 4 };
            }
            _ => i += 2,
        }
    }
    (i, d)
}

// One step of the scalar state machine. A plain byte (not `=`, CR or LF)
// with no pending state is decoded here, inlined into every caller's loop;
// everything else goes to the out-of-line `decode_scalar_step_slow`, which
// would decode that byte exactly the same way (its `None` arm, `_` case).
// The kernels drain their tails through this loop, so the plain-byte case is
// most of every call's scalar work.
#[inline(always)]
pub(super) fn decode_scalar_step(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<bool, YencError> {
    if state.state == DecoderState::None
        && let (Some(&byte), Some(slot)) = (input.get(*src), output.get_mut(*dst))
        && !matches!(byte, b'=' | b'\r' | b'\n')
    {
        *slot = byte.wrapping_sub(42);
        *src += 1;
        *dst += 1;
        return Ok(true);
    }
    decode_scalar_step_slow(input, src, output, dst, state, mode)
}

// Decodes the run of plain bytes (not `=`, CR or LF) at `*src`, stopping at
// the first special byte or at the end of either buffer, and advances both
// cursors past it. Only for the `None` state, where a plain byte decodes as
// `byte - 42` and leaves the state `None`: each byte is what
// `decode_scalar_step` does with it, without the per-byte state test and
// the two bounds tests (the run length is fixed up front). This is the shape
// of rapidyenc's scalar tail (decoder.cc:13-146) for the common byte.
#[cfg_attr(
    not(any(target_arch = "aarch64", target_arch = "x86_64")),
    allow(dead_code)
)]
#[inline(always)]
pub(super) fn decode_plain_run(input: &[u8], src: &mut usize, output: &mut [u8], dst: &mut usize) {
    // Bit b set <=> byte b is `\n` (10), `\r` (13) or `=` (61).
    const SPECIAL: u64 = 1 << b'\n' | 1 << b'\r' | 1 << b'=';
    let n = input
        .len()
        .saturating_sub(*src)
        .min(output.len().saturating_sub(*dst));
    let inp = &input[*src..*src + n];
    let out = &mut output[*dst..*dst + n];
    let mut k = 0;
    while k < n {
        let byte = inp[k];
        if byte < 64 && (SPECIAL >> byte) & 1 != 0 {
            break;
        }
        out[k] = byte.wrapping_sub(42);
        k += 1;
    }
    *src += k;
    *dst += k;
}

#[inline(never)]
fn decode_scalar_step_slow(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<bool, YencError> {
    let DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    } = mode;

    loop {
        if *src >= input.len() {
            return Ok(false);
        }

        match state.state {
            DecoderState::Eq => {
                let byte = input[*src];
                write_decoded(output, dst, byte.wrapping_sub(106))?;
                *src += 1;
                state.state = if dot_unstuffing && byte == b'\r' {
                    DecoderState::Cr
                } else {
                    DecoderState::None
                };
                return Ok(true);
            }
            DecoderState::CrLfEq => {
                let byte = input[*src];
                if search_end && dot_unstuffing && byte == b'y' {
                    *src += 1;
                    state.state = DecoderState::None;
                    state.end = DecodeEnd::Control;
                    return Ok(false);
                }
                write_decoded(output, dst, byte.wrapping_sub(106))?;
                *src += 1;
                state.state = if dot_unstuffing && byte == b'\r' {
                    DecoderState::Cr
                } else {
                    DecoderState::None
                };
                return Ok(true);
            }
            DecoderState::Cr => {
                if input[*src] == b'\n' {
                    *src += 1;
                    state.state = DecoderState::CrLf;
                    return Ok(true);
                }
                state.state = DecoderState::None;
            }
            DecoderState::CrLfDot => {
                let byte = input[*src];
                match byte {
                    b'\r' => {
                        *src += 1;
                        state.state = if search_end {
                            DecoderState::CrLfDotCr
                        } else {
                            DecoderState::Cr
                        };
                        return Ok(true);
                    }
                    b'\n' => {
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                    b'=' if dot_unstuffing && search_end => {
                        *src += 1;
                        state.state = DecoderState::CrLfEq;
                        return Ok(true);
                    }
                    b'=' => {
                        *src += 1;
                        if *src >= input.len() {
                            if preserve_pending {
                                state.state = DecoderState::Eq;
                                return Ok(false);
                            }
                            return Err(YencError::MalformedEscape(src.saturating_sub(1)));
                        }
                        let escaped = input[*src];
                        write_decoded(output, dst, escaped.wrapping_sub(106))?;
                        *src += 1;
                        state.state = if dot_unstuffing && escaped == b'\r' {
                            DecoderState::Cr
                        } else {
                            DecoderState::None
                        };
                        return Ok(true);
                    }
                    b'.' => {
                        write_decoded(output, dst, b'.'.wrapping_sub(42))?;
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                    _ => {
                        state.state = DecoderState::None;
                    }
                }
            }
            DecoderState::CrLfDotCr => {
                if input[*src] == b'\n' {
                    *src += 1;
                    state.state = DecoderState::CrLf;
                    if search_end {
                        state.end = DecodeEnd::Article;
                        return Ok(false);
                    }
                    return Ok(true);
                }
                state.state = DecoderState::None;
            }
            DecoderState::CrLf | DecoderState::None => {
                let at_line_start = state.state == DecoderState::CrLf;
                let byte = input[*src];

                if dot_unstuffing && at_line_start && byte == b'.' {
                    *src += 1;
                    state.state = DecoderState::CrLfDot;
                    return Ok(true);
                }

                match byte {
                    b'\r' => {
                        *src += 1;
                        state.state = DecoderState::Cr;
                        return Ok(true);
                    }
                    b'\n' => {
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                    b'=' => {
                        *src += 1;
                        if *src >= input.len() {
                            if preserve_pending {
                                state.state = if dot_unstuffing && at_line_start {
                                    DecoderState::CrLfEq
                                } else {
                                    DecoderState::Eq
                                };
                                return Ok(false);
                            }
                            return Err(YencError::MalformedEscape(src.saturating_sub(1)));
                        }
                        if search_end && dot_unstuffing && at_line_start && input[*src] == b'y' {
                            *src += 1;
                            state.state = DecoderState::None;
                            state.end = DecodeEnd::Control;
                            return Ok(false);
                        }
                        let escaped = input[*src];
                        write_decoded(output, dst, escaped.wrapping_sub(106))?;
                        *src += 1;
                        state.state = if dot_unstuffing && escaped == b'\r' {
                            DecoderState::Cr
                        } else {
                            DecoderState::None
                        };
                        return Ok(true);
                    }
                    _ => {
                        write_decoded(output, dst, byte.wrapping_sub(42))?;
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                }
            }
        }
    }
}

#[cfg(target_arch = "riscv64")]
#[inline]
pub(super) fn decode_normal_run_riscv64(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    // RVV runtime dispatch is not exposed through stable Rust's portable
    // detection surface yet, so RISC-V stays on the scalar kernel for now.
    decode_normal_run_scalar(input, start, output, dst_start)
}

// Scalar fallback: process one byte at a time.
#[inline]
pub(super) fn decode_normal_run_scalar(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    let max_len = input
        .len()
        .saturating_sub(start)
        .min(output.len().saturating_sub(dst_start));
    let input = &input[start..start + max_len];
    let run_len = memchr::memchr3(b'=', b'\r', b'\n', input).unwrap_or(input.len());
    let output = &mut output[dst_start..dst_start + run_len];

    let mut i = 0usize;
    while i + 8 <= run_len {
        output[i] = input[i].wrapping_sub(42);
        output[i + 1] = input[i + 1].wrapping_sub(42);
        output[i + 2] = input[i + 2].wrapping_sub(42);
        output[i + 3] = input[i + 3].wrapping_sub(42);
        output[i + 4] = input[i + 4].wrapping_sub(42);
        output[i + 5] = input[i + 5].wrapping_sub(42);
        output[i + 6] = input[i + 6].wrapping_sub(42);
        output[i + 7] = input[i + 7].wrapping_sub(42);
        i += 8;
    }
    while i < run_len {
        output[i] = input[i].wrapping_sub(42);
        i += 1;
    }

    (run_len, run_len)
}

#[cfg(test)]
mod plain_run_tests {
    use super::*;

    // Every byte value, alone and inside a run: the run stops exactly at the
    // first `=`, CR or LF and decodes the rest as `byte - 42`.
    #[test]
    fn plain_run_stops_at_each_special_and_decodes_the_rest() {
        for b in 0..=255u8 {
            let input = [b'a', b'b', b, b'c'];
            let mut output = [0u8; 4];
            let (mut src, mut dst) = (0, 0);
            decode_plain_run(&input, &mut src, &mut output, &mut dst);
            let special = matches!(b, b'=' | b'\r' | b'\n');
            let want = if special { 2 } else { 4 };
            assert_eq!((src, dst), (want, want), "byte {b}");
            for k in 0..want {
                assert_eq!(output[k], input[k].wrapping_sub(42), "byte {b} lane {k}");
            }
        }
    }

    #[test]
    fn plain_run_stops_at_the_shorter_buffer_and_at_the_cursor() {
        let input = [b'x'; 10];
        let mut output = [0u8; 6];
        let (mut src, mut dst) = (3, 2);
        decode_plain_run(&input, &mut src, &mut output, &mut dst);
        assert_eq!((src, dst), (7, 6));
        assert_eq!(output, [0, 0, b'N', b'N', b'N', b'N']);

        let (mut src, mut dst) = (10, 0);
        decode_plain_run(&input, &mut src, &mut output, &mut dst);
        assert_eq!((src, dst), (10, 0));
    }
}

#[cfg(test)]
mod main_loop_tests {
    use super::*;

    // The scalar kernel before its main loop: the state machine, one
    // `decode_scalar_step` at a time. The main loop must leave the same
    // output, cursors, state, end and error on every input.
    fn step_only_kernel(
        input: &[u8],
        output: &mut [u8],
        state: &mut KernelState,
        mode: DecodeStepMode,
    ) -> Result<KernelOutcome, YencError> {
        let (mut src, mut dst) = (0, 0);
        while (!mode.search_end || state.end == DecodeEnd::None) && src < input.len() {
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

    fn next(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *seed >> 33
    }

    // Bodies dense in every byte the loop treats specially, in runs long
    // enough to reach the eight-byte path and short enough to end anywhere.
    fn adversarial(seed: &mut u64) -> Vec<u8> {
        const PIECES: &[&[u8]] = &[
            b"=",
            b"\r",
            b"\n",
            b"\r\n",
            b"\r\n.",
            b"\r\n..",
            b"\r\n.\r\n",
            b"\r\n=y",
            b"\r\n.=y",
            b"\r\n=\r",
            b"=\r",
            b"=\r\n.",
            b"=\n",
            b"==",
            b"=y",
            b".",
            b"y",
            b"\x00",
            b"\xff",
            b"\x8a",
            b"abcdefgh",
            b"0123456789abcdefghij",
            b"\r\r\n",
            b"\n\r\n.",
        ];
        let len = (next(seed) % 160) as usize;
        let mut body = Vec::with_capacity(len + 24);
        while body.len() < len {
            if next(seed).is_multiple_of(3) {
                body.push(next(seed) as u8);
            } else {
                body.extend_from_slice(PIECES[(next(seed) as usize) % PIECES.len()]);
            }
        }
        body
    }

    const STATES: [DecoderState; 7] = [
        DecoderState::CrLf,
        DecoderState::Eq,
        DecoderState::Cr,
        DecoderState::None,
        DecoderState::CrLfDot,
        DecoderState::CrLfDotCr,
        DecoderState::CrLfEq,
    ];

    #[test]
    fn main_loop_matches_the_state_machine_on_adversarial_bodies() {
        let mut seed = 0x5ca1_ab1e_d00d_f00d;
        for _ in 0..40_000 {
            let body = adversarial(&mut seed);
            let start = STATES[(next(&mut seed) as usize) % STATES.len()];
            let mode = DecodeStepMode {
                dot_unstuffing: next(&mut seed) & 1 == 1,
                preserve_pending: next(&mut seed) & 1 == 1,
                search_end: next(&mut seed) & 1 == 1,
            };
            // A roomy output, and compact ones down to empty: the compact
            // kernel must agree byte for byte or fail the same way.
            let short = (next(&mut seed) as usize) % (body.len() + 1);
            for cap in [body.len() + 16, body.len(), body.len() - short] {
                let fresh = || KernelState {
                    state: start,
                    end: DecodeEnd::None,
                    line_length: None,
                };
                let mut want_out = vec![0xabu8; cap];
                let mut want_state = fresh();
                let want = step_only_kernel(&body, &mut want_out, &mut want_state, mode);
                let mut got_out = vec![0xabu8; cap];
                let mut got_state = fresh();
                let got = decode_kernel_scalar(
                    &body,
                    &mut got_out,
                    &mut got_state,
                    mode.dot_unstuffing,
                    mode.preserve_pending,
                    mode.search_end,
                );
                let ctx = format!("body {body:?} start {start:?} mode {mode:?} cap {cap}");
                assert_eq!(format!("{got:?}"), format!("{want:?}"), "{ctx}");
                assert_eq!(got_state, want_state, "{ctx}");
                if cap < body.len() {
                    // A compact output: nothing past the decoded bytes is
                    // written, as before.
                    assert_eq!(got_out, want_out, "{ctx}");
                } else {
                    // A roomy one: the decoded bytes match, and nothing at or
                    // past `input.len()` is written (the SIMD tiers'
                    // full-width store contract).
                    if let Ok(outcome) = &want {
                        assert_eq!(
                            got_out[..outcome.written],
                            want_out[..outcome.written],
                            "{ctx}"
                        );
                    }
                    assert!(got_out[body.len()..].iter().all(|&b| b == 0xab), "{ctx}");
                }
            }
        }
    }
}
