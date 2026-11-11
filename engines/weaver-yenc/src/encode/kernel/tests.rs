// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Kernel-internal checks. The cross-tier and rapidyenc differentials live in
// the crate's integration tests; these pin the pieces a SIMD tier builds on.

use super::scalar::{SIMD_MIN_LINE_SIZE, encode_generic, encode_simd_frame};
use super::tables::{ESCAPE, ESCAPED};
use super::*;

// rapidyenc src/encoder.cc:13-22, written out as its macros define them.
#[test]
fn tables_match_rapidyenc_definitions() {
    for n in 0..256usize {
        let critical = n == 214 || n == 214 + 13 || n == 214 + 10 || n == 61 - 42;
        let expected = if critical { 0 } else { ((n + 42) & 0xff) as u8 };
        assert_eq!(ESCAPE[n], expected, "escapeLUT[{n}]");

        let escaped = critical || n == 214 + 9 || n == 214 + 32 || n == 46 - 42;
        let expected = if escaped {
            [b'=', ((n + 42 + 64) & 0xff) as u8]
        } else {
            [0, 0]
        };
        assert_eq!(ESCAPED[n], expected, "escapedLUT[{n}]");
    }
}

#[test]
fn max_len_matches_rapidyenc_formula() {
    for len in [0usize, 1, 2, 63, 64, 65, 127, 128, 1000, 768_000] {
        for line_size in [1usize, 2, 3, 7, 12, 64, 127, 128, 129, 1000] {
            let mut expected = len * 2 + 2 + 64;
            expected += if line_size == 128 {
                2 * (len >> 6)
            } else {
                2 * ((len * 2) / line_size)
            };
            assert_eq!(encode_max_len(len, line_size), expected);
        }
    }
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// Bytes heavy in the ones whose encoding needs care: NUL, LF, CR, '=', TAB,
// SPACE, '.'.
fn critical_heavy(seed: u64, len: usize) -> Vec<u8> {
    const HOT: [u8; 7] = [214, 224, 227, 19, 223, 246, 4];
    let mut state = seed;
    (0..len)
        .map(|_| {
            let r = splitmix(&mut state);
            if r & 1 == 0 {
                HOT[(r >> 8) as usize % HOT.len()]
            } else {
                (r >> 16) as u8
            }
        })
        .collect()
}

fn run_generic(line_size: usize, column: usize, src: &[u8], is_end: bool) -> (Vec<u8>, usize) {
    let mut out = vec![0u8; encode_max_len(src.len(), line_size)];
    let mut col = column;
    // SAFETY: `out` holds `encode_max_len` bytes; the domain is small values;
    // `encode_generic` needs `len >= 1`, which callers ensure.
    let n = unsafe {
        encode_generic(
            line_size,
            &mut col,
            src.as_ptr(),
            src.len(),
            out.as_mut_ptr(),
            is_end,
        )
    };
    out.truncate(n);
    (out, col)
}

fn run_frame_noop(line_size: usize, column: usize, src: &[u8], is_end: bool) -> (Vec<u8>, usize) {
    let mut out = vec![0u8; encode_max_len(src.len(), line_size)];
    let mut col = column;
    // SAFETY: as `run_generic`; the main loop consumes and writes nothing.
    let n = unsafe {
        encode_simd_frame(
            line_size,
            &mut col,
            src.as_ptr(),
            src.len(),
            out.as_mut_ptr(),
            is_end,
            |_, _, _, _, _| (0, 0),
        )
    };
    out.truncate(n);
    (out, col)
}

// The SIMD frame with a main loop that does nothing must be the scalar
// encoder: rapidyenc's tail in `do_encode_simd` and `do_encode_generic`
// encode the same way. A SIMD tier leans on this when it hands its tail over.
#[test]
fn frame_tail_matches_generic() {
    for line_size in (1..=40).chain([63, 64, 65, 127, 128, 129]) {
        for column in 0..=line_size + 2 {
            for len in 1..=160usize {
                let src = critical_heavy((line_size * 1000 + column) as u64 ^ len as u64, len);
                for is_end in [false, true] {
                    let generic = run_generic(line_size, column, &src, is_end);
                    let frame = run_frame_noop(line_size, column, &src, is_end);
                    assert_eq!(
                        generic, frame,
                        "line_size {line_size} column {column} len {len} is_end {is_end}"
                    );
                }
            }
        }
    }
    assert_eq!(SIMD_MIN_LINE_SIZE, 12);
}

// A main loop that encodes a prefix with the scalar loop (no end rule) and
// hands the rest to the tail must give the scalar result whenever the prefix
// ends mid-line, which is where rapidyenc's kernels stop.
#[test]
fn frame_resumes_tail_after_a_mid_line_prefix() {
    for line_size in [12usize, 16, 64, 128] {
        for len in 1..=600usize {
            let src = critical_heavy(len as u64 * 7 + line_size as u64, len);
            for is_end in [false, true] {
                let whole = run_generic(line_size, 0, &src, is_end);
                for cut in [len / 3, len / 2, len.saturating_sub(5)] {
                    let mut out = vec![0u8; encode_max_len(len, line_size)];
                    let mut col = 0usize;
                    let mut prefix_col = None;
                    // SAFETY: as `run_generic`.
                    let n = unsafe {
                        encode_simd_frame(
                            line_size,
                            &mut col,
                            src.as_ptr(),
                            len,
                            out.as_mut_ptr(),
                            is_end,
                            |ls, col, s, n, d| {
                                if cut == 0 || cut >= n {
                                    return (0, 0);
                                }
                                let w = encode_generic(ls, col, s, cut, d, false);
                                prefix_col = Some(*col);
                                (cut, w)
                            },
                        )
                    };
                    out.truncate(n);
                    // Only mid-line cuts are a valid hand-over; others are
                    // the documented split quirk.
                    if matches!(prefix_col, Some(c) if c != 0 && c < line_size - 1) {
                        assert_eq!(
                            (out, col),
                            whole.clone(),
                            "line_size {line_size} len {len} cut {cut} is_end {is_end}"
                        );
                    }
                }
            }
        }
    }
}
