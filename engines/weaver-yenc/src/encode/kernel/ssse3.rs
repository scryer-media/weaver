// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// SSSE3 tier, and the 128-bit block the AVX tier reuses under VEX.
//
// rapidyenc builds SSSE3 and AVX from one template, `do_encode_sse`
// (src/encoder_sse_base.h:152-724), instantiated at ISA_LEVEL_SSSE3
// (src/encoder_ssse3.cc) and ISA_LEVEL_SSE4_POPCNT compiled with -mavx
// -mpopcnt (src/encoder_avx.cc, CMakeLists.txt:189). Ported here:
//
// * detection: PABSB + PSHUFB of a 16-entry table + PCMPEQB, two vectors
//   (32 bytes) per block (src/encoder_sse_base.h:216-221);
// * expansion with escapes: the `shufMix` table (`SMX`), one PSHUFB + PADDB per 8
//   source bytes, four overlapping stores (:315-337, :370-373).
//
// * at most one escape: one branch-free path for none or one, by index
//   (`expandShufmaskmix`, :479-595), as rapidyenc runs above SSE2 (:162).
//
// The line end is `x86_common`'s.

use std::arch::x86_64::*;

use super::scalar::encode_simd_frame;
use super::x86_common::{Align64, BITS, Block, Dense, SMX, main_loop, popcnt_table};

// The 128-bit block. `POPCNT` picks hardware popcnt (AVX tier) over a byte
// table (SSSE3 tier, which rapidyenc also builds without POPCNT).
pub(super) struct Sse<const POPCNT: bool>;

// rapidyenc's PSHUFB detection table, byte 0 first (src/encoder_sse_base.h:218
// lists it high byte first). LUT[abs(x) & 15] == x exactly for the four
// always-critical source bytes.
#[inline(always)]
pub(super) unsafe fn detect_lut() -> __m128i {
    let k = |c: u8| c.wrapping_sub(42) as i8;
    unsafe {
        _mm_setr_epi8(
            k(b'\n'),
            k(0),
            k(b' '),
            k(b'='),
            k(0),
            k(b'\r'),
            k(0),
            k(0),
            k(b'\n'),
            k(b'\t'),
            k(0),
            k(b'='),
            k(b'.'),
            k(b'\r'),
            k(0),
            k(0),
        )
    }
}

// Escape mask of one 16-byte vector.
#[inline(always)]
pub(super) unsafe fn crit16(lut: __m128i, d: __m128i) -> u64 {
    unsafe {
        _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_shuffle_epi8(lut, _mm_abs_epi8(d)), d)) as u32 as u64
    }
}

// A dense block's line-end state: the four PSHUFB controls and the output
// offsets of groups 1 to 3.
#[derive(Clone, Copy)]
pub(super) struct DenseX {
    c: [__m128i; 4],
    o: [usize; 3],
}

// Store the 16 source bytes of `d`, escaped by `m` (16 bits), at `p` with
// two shufMix expansions; return the width, the low group's width and the
// two controls. `TW` reads each group's width from its shufMix entry, as
// rapidyenc reads BitsSetTable256plus8 beside shufMix (encoder_sse_base.h:
// 355-361); otherwise `POPCNT` picks hardware popcnt over the byte table,
// as rapidyenc's Zen tunes do (same lines).
#[inline(always)]
pub(super) unsafe fn expand16x<const POPCNT: bool, const TW: bool>(
    d: __m128i,
    m: u64,
    p: *mut u8,
) -> (usize, usize, __m128i, __m128i) {
    unsafe {
        let m0 = (m & 0xff) as usize;
        let m1 = ((m >> 8) & 0xff) as usize;
        let (e0, e1) = (&SMX[m0], &SMX[m1]);
        let c0 = _mm_loadu_si128(e0[0].as_ptr().cast());
        let c1 = _mm_loadu_si128(e1[1].as_ptr().cast());
        let a = _mm_add_epi8(
            _mm_shuffle_epi8(d, c0),
            _mm_loadu_si128(e0[2].as_ptr().cast()),
        );
        let b = _mm_add_epi8(
            _mm_shuffle_epi8(d, c1),
            _mm_loadu_si128(e1[2].as_ptr().cast()),
        );
        let (w0, w1) = if TW {
            (e0[3][0] as usize, e1[3][0] as usize)
        } else if POPCNT {
            (
                8 + (m0 as u32).count_ones() as usize,
                8 + (m1 as u32).count_ones() as usize,
            )
        } else {
            (8 + BITS[m0] as usize, 8 + BITS[m1] as usize)
        };
        _mm_storeu_si128(p.cast(), a);
        _mm_storeu_si128(p.add(w0).cast(), b);
        (w0 + w1, w0, c0, c1)
    }
}

// Two `expand16x` stores of a 32-byte block, low then high vector.
#[inline(always)]
pub(super) unsafe fn dense32<const POPCNT: bool, const TW: bool>(
    lo: __m128i,
    hi: __m128i,
    m: u64,
    p: *mut u8,
) -> (usize, DenseX) {
    unsafe {
        let (wa, w0, c0, c1) = expand16x::<POPCNT, TW>(lo, m & 0xffff, p);
        let (wb, w2, c2, c3) = expand16x::<POPCNT, TW>(hi, m >> 16, p.add(wa));
        (
            wa + wb,
            DenseX {
                c: [c0, c1, c2, c3],
                o: [w0, wa, wa + w2],
            },
        )
    }
}

// `out_eq32` of the block from its controls: each control's PMOVMSKB is its
// group's '=' positions (rapidyenc src/encoder_sse_base.h:390-392, :401-403).
#[inline(always)]
pub(super) unsafe fn dense_eq32(d: DenseX) -> u64 {
    unsafe {
        let mm = |c: __m128i| _mm_movemask_epi8(c) as u32 as u64;
        mm(d.c[0]) | (mm(d.c[1]) << d.o[0]) | (mm(d.c[2]) << d.o[1]) | (mm(d.c[3]) << d.o[2])
    }
}

// rapidyenc `expandShufmaskmix` (src/encoder_sse_base.h:479-595): a 32-byte
// block with at most one escape, at index `j` (32: none), as two PSHUFB of
// the vectors at `s` and `s + 15` plus a table add, without a branch on
// whether the escape exists. Entry `j`: the two PSHUFB controls (an escape
// slot gets 0x80, so the add makes it '='), then the two adds (42, 106 on
// the escaped byte). The block's last output byte lands past the 32 the
// vectors hold when there is an escape, so it is written on its own.
// 64-byte aligned, one cache line per entry (`Align64`, x86_common.rs).
pub(super) static ONE: Align64<[[[u8; 16]; 4]; 33]> = Align64(build_one());

const fn build_one() -> [[[u8; 16]; 4]; 33] {
    let mut t = [[[0u8; 16]; 4]; 33];
    let mut j = 0;
    while j < 33 {
        let mut i = 0;
        while i < 16 {
            // Low vector: source bytes 0..16.
            let (ca, ma) = if j < 16 {
                if i < j {
                    (i as u8, 42)
                } else if i == j {
                    (0x80, b'=')
                } else if i == j + 1 {
                    ((i - 1) as u8, 42 + 64)
                } else {
                    ((i - 1) as u8, 42)
                }
            } else {
                (i as u8, 42)
            };
            // High vector, from the 16 bytes at source 15.
            let (cb, mb) = if j < 16 {
                (i as u8, if i == 0 && j == 15 { 42 + 64 } else { 42 })
            } else {
                let jj = j - 16;
                if i < jj {
                    ((i + 1) as u8, 42)
                } else if i == jj {
                    (0x80, b'=')
                } else if i == jj + 1 {
                    (i as u8, 42 + 64)
                } else {
                    (i as u8, 42)
                }
            };
            t[j][0][i] = ca;
            t[j][1][i] = cb;
            t[j][2][i] = ma;
            t[j][3][i] = mb;
            i += 1;
        }
        j += 1;
    }
    t
}

// SAFETY: 32 bytes readable at `s`, 33 writable at `p`; `m` has
// at most one bit, below 32.
#[inline(always)]
pub(super) unsafe fn one32(a: __m128i, s: *const u8, m: u64, p: *mut u8) -> usize {
    unsafe {
        let j = (m as u32).trailing_zeros() as usize;
        let e = &ONE[j];
        let b = _mm_loadu_si128(s.add(15).cast());
        let x = _mm_add_epi8(
            _mm_shuffle_epi8(a, _mm_loadu_si128(e[0].as_ptr().cast())),
            _mm_loadu_si128(e[2].as_ptr().cast()),
        );
        let y = _mm_add_epi8(
            _mm_shuffle_epi8(b, _mm_loadu_si128(e[1].as_ptr().cast())),
            _mm_loadu_si128(e[3].as_ptr().cast()),
        );
        _mm_storeu_si128(p.cast(), x);
        _mm_storeu_si128(p.add(16).cast(), y);
        let w = 32 + (m != 0) as usize;
        p.add(w - 1)
            .write((*s.add(31)).wrapping_add(42 + 64 * (m >> 31) as u8));
        w
    }
}

impl<const POPCNT: bool> Block for Sse<POPCNT> {
    const N: usize = 32;
    const READ_AHEAD: usize = 32;
    type V = (__m128i, __m128i);

    #[inline(always)]
    unsafe fn load(s: *const u8) -> Self::V {
        unsafe { (_mm_loadu_si128(s.cast()), _mm_loadu_si128(s.add(16).cast())) }
    }

    #[inline(always)]
    unsafe fn crit_mask(v: Self::V) -> u64 {
        unsafe {
            let lut = detect_lut();
            crit16(lut, v.0) | (crit16(lut, v.1) << 16)
        }
    }

    #[inline(always)]
    unsafe fn store(v: Self::V, s: *const u8, m: u64, p: *mut u8) -> usize {
        unsafe {
            if m & m.wrapping_sub(1) == 0 {
                return one32(v.0, s, m, p);
            }
            Self::store_many(v, s, m, p)
        }
    }

    #[inline(always)]
    unsafe fn store_one(v: Self::V, s: *const u8, m: u64, p: *mut u8) -> usize {
        unsafe { one32(v.0, s, m, p) }
    }

    #[inline(always)]
    unsafe fn store_many(v: Self::V, _s: *const u8, m: u64, p: *mut u8) -> usize {
        unsafe { dense32::<POPCNT, true>(v.0, v.1, m, p).0 }
    }

    // The rest of the break block is the block's own output from column
    // `w1 = f + 1 + (j <= f)` on, taken from the two output registers by
    // PSHUFB (the NEON tier's in-register remainder, neon.rs:879-889, with
    // TBL's out-of-range zero made by a saturating add of 0x70), so no second
    // load or encode waits on where the line ended. The block's last output
    // byte is written on its own, as `one32` does.
    #[inline(always)]
    unsafe fn break_one(
        v: Self::V,
        s: *const u8,
        m: u64,
        p: *mut u8,
        f: usize,
        q: *mut u8,
        edge: impl FnOnce(),
    ) {
        unsafe {
            let j = (m as u32).trailing_zeros() as usize;
            let e = &ONE[j];
            let b = _mm_loadu_si128(s.add(15).cast());
            let x = _mm_add_epi8(
                _mm_shuffle_epi8(v.0, _mm_loadu_si128(e[0].as_ptr().cast())),
                _mm_loadu_si128(e[2].as_ptr().cast()),
            );
            let y = _mm_add_epi8(
                _mm_shuffle_epi8(b, _mm_loadu_si128(e[1].as_ptr().cast())),
                _mm_loadu_si128(e[3].as_ptr().cast()),
            );
            let lb = (*s.add(31)).wrapping_add(42 + 64 * (m >> 31) as u8);
            _mm_storeu_si128(p.cast(), x);
            _mm_storeu_si128(p.add(16).cast(), y);
            let w = 32 + (m != 0) as usize;
            p.add(w - 1).write(lb);
            edge();
            let w1 = f + 1 + (j <= f) as usize;
            let idx = _mm_add_epi8(
                _mm_set1_epi8(w1 as i8),
                _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
            );
            let hi = _mm_set1_epi8(0x70);
            let r1 = _mm_or_si128(
                _mm_shuffle_epi8(x, _mm_adds_epu8(idx, hi)),
                _mm_shuffle_epi8(y, _mm_adds_epu8(_mm_sub_epi8(idx, _mm_set1_epi8(16)), hi)),
            );
            let r2 = _mm_shuffle_epi8(y, _mm_adds_epu8(idx, hi));
            _mm_storeu_si128(q.cast(), r1);
            _mm_storeu_si128(q.add(16).cast(), r2);
            // A line whose next one starts at the block's last byte has no
            // rest (`w == w1`).
            if w > w1 {
                q.add(w - w1 - 1).write(lb);
            }
        }
    }

    #[inline(always)]
    fn popcnt(x: u64) -> usize {
        if POPCNT {
            x.count_ones() as usize
        } else {
            popcnt_table(x)
        }
    }

    const GRID: bool = true;
    // The AVX tier (`Sse<true>`, avx.rs) decides once per two blocks, as
    // rapidyenc's 64-byte iteration (src/encoder_avx_base.h:128-131, :163):
    // on Sapphire Rapids and Zen 4 that wins every cell but `random-4k`,
    // which stays above rapidyenc. The SSSE3 tier keeps one block per
    // decision; two cost it most cells on Zen 4.
    const GRID2: bool = POPCNT;
    // The SSSE3 tier only: with rapidyenc's unlikely arms out of line its
    // sparse block falls through to one taken back edge, and it gained on
    // Sapphire Rapids and Zen 4 (random +0.07 to +0.09). The AVX and AVX2
    // tiers lost up to 0.07 with it, so they keep LLVM's own layout.
    const COLD: bool = !POPCNT;
}

impl<const POPCNT: bool> Dense for Sse<POPCNT> {
    type D = DenseX;

    #[inline(always)]
    unsafe fn store_dense(v: Self::V, m: u64, p: *mut u8) -> (usize, DenseX) {
        unsafe { dense32::<POPCNT, true>(v.0, v.1, m, p) }
    }

    #[inline(always)]
    unsafe fn dense_eq(d: DenseX) -> u64 {
        unsafe { dense_eq32(d) }
    }
}

// SAFETY: the `RawEncodeFn` contract; the CPU has SSSE3.
#[target_feature(enable = "ssse3")]
pub(super) unsafe fn encode_raw(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize {
    unsafe {
        encode_simd_frame(
            line_size,
            column,
            src,
            len,
            dst,
            is_end,
            |ls, col, s, n, d| main_loop::<Sse<false>>(ls, col, s, n, d),
        )
    }
}
