// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// AVX2 tier. rapidyenc's AVX2 kernel is `do_encode_avx2<ISA_LEVEL_AVX2>`
// (src/encoder_avx_base.h:61-582, built with -mavx2 -mpopcnt -mbmi -mbmi2
// -mlzcnt, CMakeLists.txt:191): 64 bytes per iteration as two YMM vectors,
// detection by VPABSB + VPSHUFB + VPCMPEQB (:133-146), a lzcnt-indexed
// merge table for at most one escape per vector (:380-413) and, for two or
// more, a 2 MB table of 65536 32-byte VPSHUFB controls (:12, :216-244). It
// uses no PDEP.
//
// Here: one YMM per block (32 bytes), the same detection; none or one escape
// is the SSSE3 tier's branch-free `one32`, more expand per 8 bytes through the 8 KB `shufMix` tables
// (the SSSE3 ones, VEX-encoded), which stay in L1 where rapidyenc's 2 MB
// table does not. No PDEP either (it is microcoded on Zen 1 and 2).

use std::arch::x86_64::*;

use super::scalar::encode_simd_frame;
use super::ssse3::{DenseX, ONE, dense32, dense_eq32};
use super::x86_common::{Block, Dense, main_loop};

pub(super) struct Avx2;

// The SSSE3 tier's `one32` (rapidyenc `expandShufmaskmix`) as one 256-bit
// PSHUFB over the source vectors at `s` and `s + 15`: entry `j`'s two
// controls and two adds sit next to each other in `ONE`.
//
// The low lane is the block's own vector `v` (the bytes at `s`); only the
// 16 bytes at `s + 15` are loaded for the high lane.
//
// SAFETY: 47 bytes readable at `s`, 33 writable at `p`; `v` holds the 32
// bytes at `s`; `m` has at most one bit, below 32.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn one32y(v: __m256i, s: *const u8, m: u64, p: *mut u8) -> usize {
    unsafe {
        let (y, lb) = enc32y(v, s, m);
        _mm256_storeu_si256(p.cast(), y);
        let w = 32 + (m != 0) as usize;
        p.add(w - 1).write(lb);
        w
    }
}

// `one32y`'s output register and the block's last output byte.
//
// SAFETY: as `one32y`.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn enc32y(v: __m256i, s: *const u8, m: u64) -> (__m256i, u8) {
    unsafe {
        let j = (m as u32).trailing_zeros() as usize;
        let e = ONE[j].as_ptr().cast::<u8>();
        // LLVM splits a plain 16-byte load here into two 8-byte loads and an
        // unpack (it overlaps `v`); one VINSERTI128 from memory, as the old
        // form compiled to, is written out.
        let x: __m256i;
        std::arch::asm!(
            "vinserti128 {x}, {v}, xmmword ptr [{a}], 1",
            x = lateout(ymm_reg) x,
            v = in(ymm_reg) v,
            a = in(reg) s.add(15),
            options(pure, readonly, nostack, preserves_flags),
        );
        let y = _mm256_add_epi8(
            _mm256_shuffle_epi8(x, _mm256_loadu_si256(e.cast())),
            _mm256_loadu_si256(e.add(32).cast()),
        );
        (y, (*s.add(31)).wrapping_add(42 + 64 * (m >> 31) as u8))
    }
}

// The rest of a break block from output column `w1` on, out of the block's
// own output register `y` and last byte `lb`, stored at `q` (the NEON tier's
// in-register remainder, neon.rs:879-889, as the SSSE3 tier's `break_one`
// does it per 128-bit lane, with TBL's out-of-range zero made by a
// saturating add of 0x70). No second load or encode. The last byte goes at
// the rest's end, or past it into the overrun the next store covers when
// the rest is empty (`w == w1`), so its address is a select, not a branch.
//
// SAFETY: 32 bytes writable at `q`, `w1 <= w <= 33`.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn rest32y(y: __m256i, lb: u8, w: usize, w1: usize, q: *mut u8) {
    unsafe {
        let lo = _mm256_castsi256_si128(y);
        let hi = _mm256_extracti128_si256::<1>(y);
        let idx = _mm_add_epi8(
            _mm_set1_epi8(w1 as i8),
            _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
        );
        let top = _mm_set1_epi8(0x70);
        let r1 = _mm_or_si128(
            _mm_shuffle_epi8(lo, _mm_adds_epu8(idx, top)),
            _mm_shuffle_epi8(hi, _mm_adds_epu8(_mm_sub_epi8(idx, _mm_set1_epi8(16)), top)),
        );
        let r2 = _mm_shuffle_epi8(hi, _mm_adds_epu8(idx, top));
        _mm_storeu_si128(q.cast(), r1);
        _mm_storeu_si128(q.add(16).cast(), r2);
        let at = if w > w1 { w - w1 - 1 } else { 31 };
        q.add(at).write(lb);
    }
}

impl Block for Avx2 {
    const N: usize = 32;
    const READ_AHEAD: usize = 32;
    type V = __m256i;

    #[inline(always)]
    unsafe fn load(s: *const u8) -> Self::V {
        unsafe { _mm256_loadu_si256(s.cast()) }
    }

    #[inline(always)]
    unsafe fn crit_mask(v: Self::V) -> u64 {
        unsafe {
            let lut = super::ssse3::detect_lut();
            let lut = _mm256_broadcastsi128_si256(lut);
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(
                _mm256_shuffle_epi8(lut, _mm256_abs_epi8(v)),
                v,
            )) as u32 as u64
        }
    }

    #[inline(always)]
    unsafe fn store(v: Self::V, s: *const u8, m: u64, p: *mut u8) -> usize {
        unsafe {
            if m & m.wrapping_sub(1) == 0 {
                return one32y(v, s, m, p);
            }
            Self::store_many(v, s, m, p)
        }
    }

    #[inline(always)]
    unsafe fn store_one(v: Self::V, s: *const u8, m: u64, p: *mut u8) -> usize {
        unsafe { one32y(v, s, m, p) }
    }

    #[inline(always)]
    unsafe fn store_many(v: Self::V, _s: *const u8, m: u64, p: *mut u8) -> usize {
        unsafe {
            let lo = _mm256_castsi256_si128(v);
            let hi = _mm256_extracti128_si256::<1>(v);
            dense32::<true, false>(lo, hi, m, p).0
        }
    }

    // The break block stored whole, then the edge, then the rest from the
    // output register (`rest32y`).
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
            let (y, lb) = enc32y(v, s, m);
            _mm256_storeu_si256(p.cast(), y);
            let w = 32 + (m != 0) as usize;
            p.add(w - 1).write(lb);
            edge();
            let j = (m as u32).trailing_zeros() as usize;
            rest32y(y, lb, w, f + 1 + (j <= f) as usize, q);
        }
    }

    #[inline(always)]
    fn popcnt(x: u64) -> usize {
        x.count_ones() as usize
    }

    const GRID: bool = true;
}

// The dense widths come from POPCNT, not from the shufMix entry: on this
// tier the entry's width measured slower on Zen 4 (rapidyenc's Zen tunes
// count with popcnt too, src/encoder_sse_base.h:355-361).
impl Dense for Avx2 {
    type D = DenseX;

    #[inline(always)]
    unsafe fn store_dense(v: Self::V, m: u64, p: *mut u8) -> (usize, DenseX) {
        unsafe {
            let lo = _mm256_castsi256_si128(v);
            let hi = _mm256_extracti128_si256::<1>(v);
            dense32::<true, false>(lo, hi, m, p)
        }
    }

    #[inline(always)]
    unsafe fn dense_eq(d: DenseX) -> u64 {
        unsafe { dense_eq32(d) }
    }
}

// SAFETY: the `RawEncodeFn` contract; the CPU has AVX2, BMI1, BMI2, POPCNT,
// LZCNT.
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
pub(super) unsafe fn encode_raw(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize {
    unsafe {
        encode_simd_frame(line_size, column, src, len, dst, is_end, |ls, col, s, n, d| {
            main_loop::<Avx2>(ls, col, s, n, d)
        })
    }
}
