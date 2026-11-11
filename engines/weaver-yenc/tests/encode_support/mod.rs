// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Shared input generators and comparison drivers for the encoder tests.
//
// `tests/encode_kernel.rs` (always on) drives them tier against tier;
// `tests/rapidyenc_encode_diff.rs` (rapidyenc linked) drives them weaver
// against rapidyenc. Everything is seeded; nothing depends on time or load.

#![allow(dead_code)]

use std::mem::MaybeUninit;

use weaver_yenc::encode::{MAX_COLUMN, MAX_LINE_SIZE};
use weaver_yenc::{EncoderTier, encode_body_with_tier, encode_max_len};

// One encoder under test: (line_size, column in/out, src, is_end) -> bytes.
pub type EncFn<'a> = &'a dyn Fn(usize, &mut usize, &[u8], bool) -> Vec<u8>;

pub fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }
    pub fn next(&mut self) -> u64 {
        splitmix(&mut self.0)
    }
    // Uniform in `0..n` (n > 0); modulo bias is irrelevant here.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    pub fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

// Source bytes that encode to NUL, LF, CR, '=' (always escaped).
pub const ALWAYS: [u8; 4] = [214, 224, 227, 19];
// Source bytes that encode to TAB, SPACE, '.' (escaped at line edges).
pub const EDGE: [u8; 3] = [223, 246, 4];
// All seven.
pub const HOT: [u8; 7] = [214, 224, 227, 19, 223, 246, 4];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dist {
    // Every byte value equally likely.
    Uniform,
    // Half the bytes from `HOT`, the rest uniform.
    CriticalHeavy,
    // Only bytes that are always escaped: the worst case for size.
    AllCritical,
    // Half the bytes from `EDGE`: stresses line-start / line-end rules.
    EdgeHeavy,
    // Mostly bytes that never escape, with a critical one now and then.
    Sparse,
    // Runs of one byte, the byte often hot.
    Runs,
}

pub const DISTS: [Dist; 6] = [
    Dist::Uniform,
    Dist::CriticalHeavy,
    Dist::AllCritical,
    Dist::EdgeHeavy,
    Dist::Sparse,
    Dist::Runs,
];

pub fn gen_input(seed: u64, len: usize, dist: Dist) -> Vec<u8> {
    let mut rng = Rng::new(seed ^ 0x5eed_0000_0000_0000);
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let r = rng.next();
        match dist {
            Dist::Uniform => out.push(r as u8),
            Dist::CriticalHeavy => out.push(if r & 1 == 0 {
                HOT[(r >> 8) as usize % HOT.len()]
            } else {
                (r >> 16) as u8
            }),
            Dist::AllCritical => out.push(ALWAYS[(r >> 8) as usize % ALWAYS.len()]),
            Dist::EdgeHeavy => out.push(if r & 1 == 0 {
                EDGE[(r >> 8) as usize % EDGE.len()]
            } else {
                (r >> 16) as u8
            }),
            Dist::Sparse => out.push(if r.is_multiple_of(61) {
                HOT[(r >> 8) as usize % HOT.len()]
            } else {
                0x30 + ((r >> 16) as u8 % 64)
            }),
            Dist::Runs => {
                let byte = if r & 3 == 0 {
                    (r >> 8) as u8
                } else {
                    HOT[(r >> 8) as usize % HOT.len()]
                };
                let run = 1 + ((r >> 24) % 200) as usize;
                for _ in 0..run.min(len - out.len()) {
                    out.push(byte);
                }
            }
        }
    }
    out
}

pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// Encode on a forced weaver tier into a fresh buffer (no zero-fill).
pub fn weaver_encode(
    tier: EncoderTier,
    line_size: usize,
    column: &mut usize,
    src: &[u8],
    is_end: bool,
) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(encode_max_len(src.len(), line_size));
    let spare: &mut [MaybeUninit<u8>] = out.spare_capacity_mut();
    let n = encode_body_with_tier(tier, line_size, column, src, spare, is_end)
        .unwrap_or_else(|err| panic!("{tier}: {err}"));
    // SAFETY: the encoder initialised the first `n` bytes of the spare
    // capacity, and `n <= capacity`.
    unsafe { out.set_len(n) };
    out
}

// Compare one call of `a` and `b`; panic with the case and the first
// differing offset.
#[allow(clippy::too_many_arguments)]
pub fn compare_call(
    label: &str,
    a: EncFn,
    b: EncFn,
    line_size: usize,
    column: usize,
    src: &[u8],
    is_end: bool,
    desc: &dyn Fn() -> String,
) -> (Vec<u8>, usize) {
    let mut col_a = column;
    let mut col_b = column;
    let out_a = a(line_size, &mut col_a, src, is_end);
    let out_b = b(line_size, &mut col_b, src, is_end);
    if out_a != out_b || col_a != col_b {
        let first = out_a
            .iter()
            .zip(&out_b)
            .position(|(x, y)| x != y)
            .unwrap_or(out_a.len().min(out_b.len()));
        let lo = first.saturating_sub(8);
        panic!(
            "{label}: mismatch at {} (line_size {line_size}, column {column}, len {}, is_end {is_end})\n\
             lengths {} vs {}, final columns {col_a} vs {col_b}, first difference at byte {first}\n\
             a[{lo}..]: {:02x?}\n\
             b[{lo}..]: {:02x?}",
            desc(),
            src.len(),
            out_a.len(),
            out_b.len(),
            &out_a[lo.min(out_a.len())..(first + 8).min(out_a.len())],
            &out_b[lo.min(out_b.len())..(first + 8).min(out_b.len())],
        );
    }
    (out_a, col_a)
}

// Line sizes for the exhaustive sweep: every tiny size, then the sizes
// around each SIMD threshold (12, 16, 64) and the real ones (128, 256).
pub fn exhaustive_line_sizes() -> Vec<usize> {
    let mut sizes: Vec<usize> = (1..=24).collect();
    sizes.extend([
        31, 32, 33, 47, 48, 63, 64, 65, 100, 127, 128, 129, 130, 255, 256,
    ]);
    sizes
}

// Starting columns for one line size: all of them for short lines, the
// edges and the middle for long ones.
pub fn exhaustive_columns(line_size: usize) -> Vec<usize> {
    if line_size <= 40 {
        (0..=line_size + 2).collect()
    } else {
        let mut cols = vec![0, 1, 2, 3, line_size / 2];
        cols.extend(line_size - 3..=line_size + 2);
        cols
    }
}

pub const EXHAUSTIVE_MAX_LEN: usize = 300;

// Every line size x column x length 0..=300 (prefixes of one seeded input
// per distribution) x both `is_end`.
pub fn check_exhaustive(label: &str, line_sizes: &[usize], dists: &[Dist], a: EncFn, b: EncFn) {
    check_exhaustive_from(label, line_sizes, dists, 0, a, b);
}

// As `check_exhaustive`, from length `min_len` (the rapidyenc scalar kernel
// is undefined on empty input).
pub fn check_exhaustive_from(
    label: &str,
    line_sizes: &[usize],
    dists: &[Dist],
    min_len: usize,
    a: EncFn,
    b: EncFn,
) {
    for &dist in dists {
        let data = gen_input(0xe4a0 + dist as u64, EXHAUSTIVE_MAX_LEN, dist);
        for &line_size in line_sizes {
            for column in exhaustive_columns(line_size) {
                for len in min_len..=EXHAUSTIVE_MAX_LEN {
                    for is_end in [false, true] {
                        compare_call(
                            label,
                            a,
                            b,
                            line_size,
                            column,
                            &data[..len],
                            is_end,
                            &|| format!("exhaustive {dist:?}"),
                        );
                    }
                }
            }
        }
    }
}

fn random_line_size(rng: &mut Rng) -> usize {
    const SET: [usize; 12] = [1, 2, 11, 12, 16, 32, 64, 65, 127, 129, 256, 1000];
    match rng.below(4) {
        0 | 1 => 128,
        2 => 1 + rng.below(300),
        _ => SET[rng.below(SET.len())],
    }
}

fn random_len(rng: &mut Rng, min_len: usize) -> usize {
    let len = match rng.below(4) {
        0 => rng.below(64),
        1 => rng.below(1024),
        2 => rng.below(4096),
        _ => rng.below(20_000),
    };
    len.max(min_len)
}

// Seeded random single calls: skewed distributions, line sizes weighted to
// 128, random starting columns, both `is_end`.
pub fn check_random(label: &str, seed: u64, cases: usize, min_len: usize, a: EncFn, b: EncFn) {
    let mut rng = Rng::new(seed);
    for case in 0..cases {
        let dist = DISTS[rng.below(DISTS.len())];
        let len = random_len(&mut rng, min_len);
        let line_size = random_line_size(&mut rng);
        let column = if rng.chance(50) {
            0
        } else {
            rng.below(line_size + 4)
        };
        let is_end = rng.chance(50);
        let data = gen_input(rng.next(), len, dist);
        compare_call(label, a, b, line_size, column, &data, is_end, &|| {
            format!("random case {case} seed {seed:#x} {dist:?}")
        });
    }
}

// Seeded random inputs split into random chunks, `column` carried from call
// to call, `is_end` on the last chunk (and now and then on a middle one).
// Every chunk's bytes, length and column must match.
pub fn check_chunked(label: &str, seed: u64, cases: usize, a: EncFn, b: EncFn) {
    let mut rng = Rng::new(seed);
    for case in 0..cases {
        let dist = DISTS[rng.below(DISTS.len())];
        let len = 1 + rng.below(16_000);
        let line_size = random_line_size(&mut rng);
        let data = gen_input(rng.next(), len, dist);
        let max_chunk = [8usize, 200, 5000][rng.below(3)];
        let mut column = if rng.chance(70) {
            0
        } else {
            rng.below(line_size + 4)
        };
        let mut pos = 0;
        let mut index = 0;
        while pos < len {
            let take = (1 + rng.below(max_chunk)).min(len - pos);
            let last = pos + take == len;
            let is_end = if last { rng.chance(80) } else { rng.chance(3) };
            let (_, col) = compare_call(
                label,
                a,
                b,
                line_size,
                column,
                &data[pos..pos + take],
                is_end,
                &|| format!("chunked case {case} seed {seed:#x} chunk {index} at {pos} {dist:?}"),
            );
            column = col;
            pos += take;
            index += 1;
        }
    }
}

// The domain's edges: the largest line size and column, with small inputs.
pub fn check_extremes(label: &str, min_len: usize, a: EncFn, b: EncFn) {
    let data = gen_input(0xed6e, 400, Dist::CriticalHeavy);
    for line_size in [MAX_LINE_SIZE, MAX_LINE_SIZE - 1, 1 << 20, 4096] {
        for column in [0, 1, 2, line_size - 2, line_size - 1, line_size, MAX_COLUMN] {
            for len in (min_len..=70).chain([200, 400]) {
                for is_end in [false, true] {
                    compare_call(
                        label,
                        a,
                        b,
                        line_size,
                        column,
                        &data[..len],
                        is_end,
                        &|| "extremes".to_string(),
                    );
                }
            }
        }
    }
    for line_size in [1usize, 2, 3, 128] {
        for len in min_len..=70 {
            compare_call(
                label,
                a,
                b,
                line_size,
                MAX_COLUMN,
                &data[..len],
                true,
                &|| "extremes, largest column".to_string(),
            );
        }
    }
}

// Golden vectors: fixed inputs whose rapidyenc output digest is pinned, so
// byte identity holds where rapidyenc is not linked. The constants were
// produced by rapidyenc (`rapidyenc_encode_diff::golden_vectors_match_rapidyenc`
// checks them against the library and prints the table on a mismatch).
pub struct Golden {
    pub seed: u64,
    pub len: usize,
    pub dist: Dist,
    pub line_size: usize,
    pub column: usize,
    pub is_end: bool,
    // 0: one call. Otherwise the input is fed in chunks of this size, column
    // carried, `is_end` on the last.
    pub chunk: usize,
    // Total bytes written, final column, FNV-1a 64 of the concatenated output.
    pub written: usize,
    pub final_column: usize,
    pub digest: u64,
}

// Run one golden case through `enc`: (written, final column, digest).
pub fn run_golden(g: &Golden, enc: EncFn) -> (usize, usize, u64) {
    let data = gen_input(g.seed, g.len, g.dist);
    let mut column = g.column;
    let mut out = Vec::new();
    if g.chunk == 0 {
        out = enc(g.line_size, &mut column, &data, g.is_end);
    } else {
        let mut pos = 0;
        while pos < data.len() {
            let take = g.chunk.min(data.len() - pos);
            let last = pos + take == data.len();
            out.extend(enc(
                g.line_size,
                &mut column,
                &data[pos..pos + take],
                last && g.is_end,
            ));
            pos += take;
        }
    }
    (out.len(), column, fnv1a64(&out))
}

macro_rules! golden {
    ($seed:expr, $len:expr, $dist:ident, $ls:expr, $col:expr, $end:expr, $chunk:expr => $w:expr, $fc:expr, $d:expr) => {
        Golden {
            seed: $seed,
            len: $len,
            dist: Dist::$dist,
            line_size: $ls,
            column: $col,
            is_end: $end,
            chunk: $chunk,
            written: $w,
            final_column: $fc,
            digest: $d,
        }
    };
}

pub const GOLDEN: &[Golden] = &[
    golden!(1, 768_000, Uniform, 128, 0, true, 0 => 792_386, 22, 0xb7584fbdf95e1ea7),
    golden!(2, 65_536, Uniform, 128, 0, true, 0 => 67_590, 110, 0xe63f81ff0113680f),
    golden!(3, 4096, CriticalHeavy, 128, 0, true, 0 => 5347, 8, 0x19597b67746ef99e),
    golden!(4, 4096, AllCritical, 128, 0, true, 0 => 8318, 128, 0x7d71f337ed05b565),
    golden!(5, 4096, EdgeHeavy, 128, 0, true, 0 => 4212, 40, 0x71867c8c7c4045f3),
    golden!(6, 4096, EdgeHeavy, 128, 0, false, 0 => 4215, 43, 0xaec49d807ffa96df),
    golden!(7, 4096, Sparse, 128, 0, true, 0 => 4203, 43, 0x87e535af0dbf81a5),
    golden!(8, 4096, Runs, 128, 0, true, 0 => 5196, 116, 0xae562fdece423c15),
    golden!(9, 3000, CriticalHeavy, 1, 0, true, 0 => 10_525, 2, 0xb5a03136381641f4),
    golden!(10, 3000, CriticalHeavy, 2, 1, true, 0 => 8505, 2, 0xb5e46fe929440e06),
    golden!(11, 3000, EdgeHeavy, 3, 2, true, 0 => 6454, 2, 0xb846bcf0af986b00),
    golden!(12, 3000, CriticalHeavy, 11, 5, true, 0 => 4734, 2, 0x014d80c26445bf36),
    golden!(13, 3000, CriticalHeavy, 12, 11, true, 0 => 4673, 7, 0xbe99fbd044d8cbc2),
    golden!(14, 3000, EdgeHeavy, 16, 15, false, 0 => 3593, 7, 0x644fd2949769f2a0),
    golden!(15, 3000, CriticalHeavy, 64, 63, true, 0 => 4004, 17, 0xfa58aa7832065bfe),
    golden!(16, 3000, EdgeHeavy, 65, 64, true, 0 => 3156, 57, 0x243016684e0d5141),
    golden!(17, 3000, CriticalHeavy, 127, 127, true, 0 => 3958, 73, 0x1b994696394efaf6),
    golden!(18, 3000, Uniform, 128, 200, true, 0 => 3101, 109, 0x112dc576af3f098a),
    golden!(19, 3000, EdgeHeavy, 256, 100, true, 0 => 3059, 60, 0xed6c42d60a5ccfa9),
    golden!(20, 3000, AllCritical, 1000, 999, true, 0 => 6012, 998, 0x9cbe14c28c41b58e),
    golden!(21, 50_000, CriticalHeavy, 128, 0, true, 127 => 65_747, 52, 0xf4ca3e2f6aea58cc),
    golden!(22, 50_000, EdgeHeavy, 128, 0, true, 64 => 51_474, 112, 0x534ec3499f3f57fa),
    golden!(23, 20_000, Uniform, 128, 0, true, 1 => 20_453, 127, 0xcdb2069acd2843ea),
    golden!(24, 20_000, Runs, 61, 3, true, 333 => 29_001, 38, 0x57eb128215b34349),
];

// Print the table with measured values, for refreshing the constants.
pub fn print_golden(measured: &[(usize, usize, u64)]) {
    eprintln!("pub const GOLDEN: &[Golden] = &[");
    for (g, (w, fc, d)) in GOLDEN.iter().zip(measured) {
        eprintln!(
            "    golden!({}, {}, {:?}, {}, {}, {}, {} => {}, {}, {:#x}),",
            g.seed, g.len, g.dist, g.line_size, g.column, g.is_end, g.chunk, w, fc, d
        );
    }
    eprintln!("];");
}
