// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// AVX-512 VBMI2 tier, 512-bit.
//
// rapidyenc's VBMI2 kernel is 256-bit: `do_encode_avx2<ISA_LEVEL_VBMI2>`
// (src/encoder_vbmi2.cc includes encoder_avx_base.h unless
// YENC_DISABLE_AVX256), two YMM per 64-byte iteration. It adds 42 and ORs 64
// into escaped bytes with VPTERNLOGD (src/encoder_avx_base.h:150-153), then
// for at most one escape per vector expands with VPEXPANDB under ~mask
// (:349-358); for more it builds the VPEXPANDB masks with PDEP/PEXT
// (:183-207) and four 32-byte expands. Line ends overflow and revert
// (:257-340), and the next line restarts from the reverted source position,
// so every line puts load -> compare -> revert on one serial chain.
//
// Here, the decoder's 512-bit shape (`simd::x86_avx512`, whose VPCOMPRESSB
// this mirrors with VPEXPANDB): one whole-span loop, one ZMM per 64-byte
// block, one VPCMPEQB into a k-mask, a masked add for the +64, two 64-byte
// VPEXPANDB, each under a PDEP/PEXT-built mask for 32 source bytes, stored
// without a bounds check. At a line end the same masks give the source byte
// at the last column; the last byte and CRLF go out as one 4-byte store, and
// the rest of the block, already expanded in the two registers, is stored
// again shifted past the CRLF with masked stores. The next line's first byte
// goes out from one table load that also gives its width (the NEON tier's
// `START_ENC`, neon.rs:520, :844-845), so a TAB, SPACE or '.' there costs
// no compare. The source stride stays 64: the next load never waits on the
// line end. Only when the block ends at the line end or its rest would reach
// a second line end does the loop restart, past the next line's first byte,
// which goes out from the same table first (rapidyenc's line start,
// src/encoder_avx_base.h:116-127). So no block carries a line-start escape
// the compare does not find, and the block has no state for one.
//
// Two loops share all of this and differ only at a line end: `encode_raw`
// takes the escape widths there from the tables, `encode_raw_zen4` from
// compares of the block (`block_zen4`). `encode_fn` picks one per
// process.

use std::arch::x86_64::*;
use std::sync::OnceLock;

use super::RawEncodeFn;
use super::scalar::encode_simd_frame;
use super::tables::{DOT, ESCAPED};

// Blocks without escapes take a one-test path (the decoder's `mask == 0`
// store) only while the data is clean: on random data a block is clean about
// a third of the time, a branch no predictor learns on a stream of distinct
// articles (Zen 4 learns it only on a repeated buffer). So blocks run in
// 512-byte chunks without the branch, and a chunk without any escape turns
// the branch on until the first block with one.

const ODD: u64 = 0xaaaa_aaaa_aaaa_aaaa;
const EVEN: u64 = 0x5555_5555_5555_5555;

// rapidyenc `eolLastChar` (src/encoder_avx_base.h:20-24): a line's last byte
// plus CRLF as one little-endian 4-byte store. Width is 3, or 4 when bit 27
// is set (escaped form ends in '\n' = 0x0a in the top byte).
static EOL: [u32; 256] = build_eol();
// A line's first byte as one little-endian 2-byte store (its escape, or its
// encoding then a byte the next store overwrites) in bits 0..16, its width
// (1, or 2 when escaped) in bits 16..24 (neon.rs:516-536), and in bit 31 a 1
// when the escape is one the block's compare does not give (TAB, SPACE,
// '.'): one shift gives it at a line end.
static START: [u32; 256] = build_start();

#[inline(always)]
fn eol(c: u8) -> u32 {
    EOL[c as usize]
}

#[inline(always)]
fn start(c: u8) -> u32 {
    START[c as usize]
}

const fn build_eol() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let c = i as u8;
        let e = ESCAPED[i];
        t[i] = if e[0] != 0 && c != DOT {
            (b'=' as u32) | ((e[1] as u32) << 8) | (0x0a0d << 16)
        } else {
            (c.wrapping_add(42) as u32) | (0x0a0d << 8)
        };
        i += 1;
    }
    t
}

const fn build_start() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let e = ESCAPED[i];
        let c = i as u8;
        let edge = c == b'\t'.wrapping_sub(42) || c == b' '.wrapping_sub(42) || c == DOT;
        t[i] = if e[0] != 0 {
            (e[0] as u32) | ((e[1] as u32) << 8) | ((edge as u32) << 31) | (2 << 16)
        } else {
            (c.wrapping_add(42) as u32) | (1 << 16)
        };
        i += 1;
    }
    t
}

// rapidyenc's 16-entry compare table (src/encoder_avx_base.h:134-136), the
// low nibble of |byte| giving the one critical value that nibble can hold.
#[inline(always)]
unsafe fn detect_lut() -> __m128i {
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

// VPEXPANDB mask for 32 source bytes escaped by `m`: in output order, 1 for
// a data byte, 0 for the '=' before an escaped one (rapidyenc
// src/encoder_avx_base.h:183-185). Exactly `32 + popcnt(m)` bits.
#[inline(always)]
unsafe fn expand_mask(m: u64) -> u64 {
    unsafe { _pext_u64(ODD, _pdep_u64(m, EVEN) | ODD) }
}

// A line's first byte `*s` at `*p`; returns its width.
//
// SAFETY: 1 byte readable at `s`, 2 writable at `p`.
#[inline(always)]
unsafe fn first_byte(s: *const u8, p: *mut u8) -> usize {
    unsafe {
        let fe = start(*s);
        p.cast::<u16>().write_unaligned((fe as u16).to_le());
        ((fe >> 16) & 0xff) as usize
    }
}

// Entry at a line's last column or past it (the tail's rules): write the
// last byte if the line has room for it and CRLF, leaving `col` 0.
//
// SAFETY: 1 byte readable at `*s`, 4 writable at `*p`.
#[inline(always)]
unsafe fn enter(last: usize, col: &mut usize, s: &mut *const u8, p: &mut *mut u8) {
    unsafe {
        if *col >= last {
            if *col == last {
                let e = eol(**s);
                p.cast::<u32>().write_unaligned(e.to_le());
                *p = p.add(3 + (e >> 27) as usize);
                *s = s.add(1);
            } else {
                p.cast::<[u8; 2]>().write_unaligned(*b"\r\n");
                *p = p.add(2);
            }
            *col = 0;
        }
    }
}

// Hoisted constants of the block.
#[derive(Clone, Copy)]
struct Consts {
    lut: __m512i,
    k42: __m512i,
    k64: __m512i,
    eqv: __m512i,
    iota: __m512i,
}

// One 64-byte block at `*s` (vector `v`). Returns true when the next block
// starts at `*s + 64` (`*s` already advanced), false when the stream
// restarts at `*s`.
//
// `*rem` is the output offset of the line's last column from `*p` (the last
// column minus the column; rapidyenc counts the same distance down to 0,
// src/encoder_avx_base.h:69-70). `*acc` gathers the block's escape mask.
//
// SAFETY: 65 bytes readable at `*s`; output inside the budget (see `span`).
//
// The `s0` asm below is a comment: it takes the pointer's value and touches
// no memory, so `nomem` holds.
#[allow(clippy::pointers_in_nomem_asm_block)]
#[inline(always)]
unsafe fn block<const BRANCH: bool>(
    v: __m512i,
    k: Consts,
    last: usize,
    s: &mut *const u8,
    p: &mut *mut u8,
    rem: &mut usize,
    acc: &mut u64,
) -> bool {
    unsafe {
        let m = _mm512_cmpeq_epi8_mask(_mm512_shuffle_epi8(k.lut, _mm512_abs_epi8(v)), v);
        *acc |= m;
        let t = _mm512_add_epi8(v, k.k42);
        let t = _mm512_mask_add_epi8(t, m, t, k.k64);
        let lo = m & 0xffff_ffff;
        let pp = *p;
        let (x1, x2, d1, d2) = if BRANCH && m == 0 {
            _mm512_storeu_si512(pp.cast(), t);
            (t, _mm512_alignr_epi64::<4>(t, t), 0xffff_ffff, 0xffff_ffff)
        } else {
            let d1 = expand_mask(lo);
            let d2 = expand_mask(m >> 32);
            let x1 = _mm512_mask_expand_epi8(k.eqv, d1, t);
            let x2 = _mm512_mask_expand_epi8(
                k.eqv,
                d2,
                _mm512_castsi256_si512(_mm512_extracti64x4_epi64::<1>(t)),
            );
            _mm512_storeu_si512(pp.cast(), x1);
            _mm512_storeu_si512(pp.add(32 + lo.count_ones() as usize).cast(), x2);
            (x1, x2, d1, d2)
        };
        let w1 = 32 + lo.count_ones() as usize;
        let w = 64 + m.count_ones() as usize;
        let r = *rem;
        if w <= r {
            *p = pp.add(w);
            *rem = r - w;
            *s = s.add(64);
            return true;
        }
        // The line end takes `*s + 64` from `*s` anew: LLVM otherwise keeps
        // the fast path's copy alive across it and spills it, and the reload
        // puts a store forward on the next load's address.
        let mut s0 = *s;
        std::arch::asm!("/* {0} */", inout(reg) s0, options(pure, nomem, nostack, preserves_flags));
        *s = s0;
        // Output offset `r` is the line's last column. `kk` counts the data
        // bytes below it; byte `kk` starts at `kk + escapes below kk`, which
        // is `r - 1` when its escape straddles the column.
        let kk = _bzhi_u64(d1, r as u32).count_ones() as usize
            + _bzhi_u64(d2, r.saturating_sub(w1) as u32).count_ones() as usize;
        let st = r - kk - _bzhi_u64(m, kk as u32).count_ones() as usize;
        let e = eol(*s.add(kk)) >> (8 * st);
        pp.add(r).cast::<u32>().write_unaligned(e.to_le());
        let q = pp.add(r + 3 + (e >> 27) as usize);
        let nk = kk + 1;
        // Byte `nk`, the next line's first, starts at output offset `off`.
        // Its bytes, width and `fx` (1 for an escape the compare did not
        // give) come from `start`. The rest is `wb` bytes.
        let off = r + 1 + ((m >> kk) & 1) as usize - st;
        let fe = start(*s.add(nk));
        let fx = (fe >> 31) as usize;
        let wb = w.wrapping_sub(off).wrapping_add(fx);
        if nk < 64 && wb <= last {
            q.cast::<u16>().write_unaligned((fe as u16).to_le());
            // The rest, the output bytes at or past `off + fx` of each
            // register, from `q + 2 * fx` on.
            // The store masks are byte compares of the lane index against
            // `o` (below 128, unsigned) and `o - w1` (-64..64, signed): two
            // vector ops each, no mask arithmetic on the integer side.
            let o = off + fx;
            let sh = q.add(fx).sub(off);
            let m1 = _mm512_cmpge_epu8_mask(k.iota, _mm512_set1_epi8(o as i8));
            let m2 = _mm512_cmpge_epi8_mask(k.iota, _mm512_set1_epi8(o.wrapping_sub(w1) as i8));
            _mm512_mask_storeu_epi8(sh.cast(), m1, x1);
            _mm512_mask_storeu_epi8(sh.add(w1).cast(), m2, x2);
            *p = q.add(wb);
            *rem = last - wb;
            *s = s.add(64);
            return true;
        }
        // A line that ends on the block's last byte (every line of zeros or
        // all-critical data) keeps the stride when the next block's first
        // byte needs no escape the compare misses: that block starts the
        // line.
        if nk == 64 && fx == 0 {
            *p = q;
            *rem = last;
            *s = s.add(64);
            return true;
        }
        // Otherwise the next line's first byte goes out from the table and
        // the stream restarts past it. A line start on the block's end
        // restarts one byte past the grid at a constant offset, so the next
        // load waits on a predicted branch, not on the line end's popcounts.
        // (The empty asm keeps it a branch: LLVM otherwise folds the two
        // arms into a CMOV of `nk`.)
        let fl = ((fe >> 16) & 0xff) as usize;
        q.cast::<u16>().write_unaligned((fe as u16).to_le());
        *p = q.add(fl);
        *rem = last - fl;
        if nk == 64 {
            std::arch::asm!("", options(nomem, nostack, preserves_flags));
            *s = s.add(65);
        } else {
            *s = s.add(nk + 1);
        }
        false
    }
}

// `block` with the Zen 4 line end: positions and escape widths at the line
// end come from compares of the block, not from the EOL and START loads,
// so neither load feeds the address of the shifted rest's masked stores.
// Same contract as `block`.
#[allow(clippy::pointers_in_nomem_asm_block)]
#[inline(always)]
unsafe fn block_zen4<const BRANCH: bool>(
    v: __m512i,
    k: Consts,
    last: usize,
    s: &mut *const u8,
    p: &mut *mut u8,
    rem: &mut usize,
    acc: &mut u64,
) -> bool {
    unsafe {
        let m = _mm512_cmpeq_epi8_mask(_mm512_shuffle_epi8(k.lut, _mm512_abs_epi8(v)), v);
        *acc |= m;
        let t = _mm512_add_epi8(v, k.k42);
        let t = _mm512_mask_add_epi8(t, m, t, k.k64);
        let lo = m & 0xffff_ffff;
        let pp = *p;
        let (x1, x2, d1, d2) = if BRANCH && m == 0 {
            _mm512_storeu_si512(pp.cast(), t);
            (t, _mm512_alignr_epi64::<4>(t, t), 0xffff_ffff, 0xffff_ffff)
        } else {
            let d1 = expand_mask(lo);
            let d2 = expand_mask(m >> 32);
            let x1 = _mm512_mask_expand_epi8(k.eqv, d1, t);
            let x2 = _mm512_mask_expand_epi8(
                k.eqv,
                d2,
                _mm512_castsi256_si512(_mm512_extracti64x4_epi64::<1>(t)),
            );
            _mm512_storeu_si512(pp.cast(), x1);
            _mm512_storeu_si512(pp.add(32 + lo.count_ones() as usize).cast(), x2);
            (x1, x2, d1, d2)
        };
        let w1 = 32 + lo.count_ones() as usize;
        let w = 64 + m.count_ones() as usize;
        let r = *rem;
        if w <= r {
            *p = pp.add(w);
            *rem = r - w;
            *s = s.add(64);
            return true;
        }
        // The line end takes `*s + 64` from `*s` anew: LLVM otherwise keeps
        // the fast path's copy alive across it and spills it, and the reload
        // puts a store forward on the next load's address.
        let mut s0 = *s;
        std::arch::asm!("/* {0} */", inout(reg) s0, options(pure, nomem, nostack, preserves_flags));
        *s = s0;
        // Output offset `r` is the line's last column. `kk` counts the data
        // bytes below it; byte `kk` starts at `kk + escapes below kk`, which
        // is `r - 1` when its escape straddles the column.
        let kk = _bzhi_u64(d1, r as u32).count_ones() as usize
            + _bzhi_u64(d2, r.saturating_sub(w1) as u32).count_ones() as usize;
        let st = r - kk - _bzhi_u64(m, kk as u32).count_ones() as usize;
        // Positions come from the block's masks, as rapidyenc's line end
        // takes them from `eqMask` alone (src/encoder_avx_base.h:300-323);
        // the table loads give only the bytes stored. `ts` marks TAB and
        // SPACE (escaped at a line's end), `ed` those and '.' (escaped at
        // its start); `m` holds the bytes escaped everywhere.
        let ts = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\t'.wrapping_sub(42) as i8))
            | _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b' '.wrapping_sub(42) as i8));
        let ed = ts | _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(DOT as i8));
        let e = eol(*s.add(kk)) >> (8 * st);
        pp.add(r).cast::<u32>().write_unaligned(e.to_le());
        // `e >> 27` is 1 for an escaped last byte unless its '=' went out
        // with the block (`st`, which implies the escape).
        let el = (((m | ts) >> kk) & 1) as usize;
        let q = pp.add(r + 3 + el - st);
        let nk = kk + 1;
        // Byte `nk`, the next line's first, starts at output offset `off`.
        // Its bytes come from `start`; `fx` (1 for an escape the compare did
        // not give) from `ed` while it is in the block. The rest is `wb`
        // bytes.
        let off = r + 1 + ((m >> kk) & 1) as usize - st;
        let fe = start(*s.add(nk));
        if nk < 64 {
            let fx = ((ed >> nk) & 1) as usize;
            let wb = w.wrapping_sub(off).wrapping_add(fx);
            if wb <= last {
                q.cast::<u16>().write_unaligned((fe as u16).to_le());
                // The rest, the output bytes at or past `off + fx` of each
                // register, from `q + 2 * fx` on.
                // The store masks are byte compares of the lane index against
                // `o` (below 128, unsigned) and `o - w1` (-64..64, signed): two
                // vector ops each, no mask arithmetic on the integer side.
                let o = off + fx;
                let sh = q.add(fx).sub(off);
                let m1 = _mm512_cmpge_epu8_mask(k.iota, _mm512_set1_epi8(o as i8));
                let m2 = _mm512_cmpge_epi8_mask(k.iota, _mm512_set1_epi8(o.wrapping_sub(w1) as i8));
                _mm512_mask_storeu_epi8(sh.cast(), m1, x1);
                _mm512_mask_storeu_epi8(sh.add(w1).cast(), m2, x2);
                *p = q.add(wb);
                *rem = last - wb;
                *s = s.add(64);
                return true;
            }
        }
        let fx = (fe >> 31) as usize;
        // A line that ends on the block's last byte (every line of zeros or
        // all-critical data) keeps the stride when the next block's first
        // byte needs no escape the compare misses: that block starts the
        // line.
        if nk == 64 && fx == 0 {
            *p = q;
            *rem = last;
            *s = s.add(64);
            return true;
        }
        // Otherwise the next line's first byte goes out from the table and
        // the stream restarts past it. A line start on the block's end
        // restarts one byte past the grid at a constant offset, so the next
        // load waits on a predicted branch, not on the line end's popcounts.
        // (The empty asm keeps it a branch: LLVM otherwise folds the two
        // arms into a CMOV of `nk`.)
        let fl = ((fe >> 16) & 0xff) as usize;
        q.cast::<u16>().write_unaligned((fe as u16).to_le());
        *p = q.add(fl);
        *rem = last - fl;
        if nk == 64 {
            std::arch::asm!("", options(nomem, nostack, preserves_flags));
            *s = s.add(65);
        } else {
            *s = s.add(nk + 1);
        }
        false
    }
}

// `block` or `block_zen4`, picked at compile time.
//
// SAFETY: as `block`.
#[inline(always)]
unsafe fn step<const BRANCH: bool, const ZEN4: bool>(
    v: __m512i,
    k: Consts,
    last: usize,
    s: &mut *const u8,
    p: &mut *mut u8,
    rem: &mut usize,
    acc: &mut u64,
) -> bool {
    unsafe {
        if ZEN4 {
            block_zen4::<BRANCH>(v, k, last, s, p, rem, acc)
        } else {
            block::<BRANCH>(v, k, last, s, p, rem, acc)
        }
    }
}

// SAFETY: the `RawEncodeFn` contract, `line_size >= 12`; the CPU has the
// tier's features.
#[inline(always)]
unsafe fn span<const ZEN4: bool>(
    ls: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
) -> (usize, usize) {
    // With the next vector loaded ahead an iteration reads below `s + 128`,
    // or below `s + 129` when it restarts at most 65 bytes on; the last
    // blocks load their own and read below `s + 65`. The scalar tail gets
    // fewer than 65 bytes.
    const NEED_A: usize = 129;
    const NEED_B: usize = 65;
    if len < NEED_A {
        return (0, 0);
    }
    // SAFETY, whole body: every read is below `s + need` for the phase's
    // `need` (each loop runs only while `s` is at most its limit; the entry
    // reads at most 2 bytes); output stays inside the `encode_max_len`
    // budget: at most 2 bytes per consumed byte plus 2 per line, plus vector
    // overrun of at most 132 bytes while at least 65 input bytes, so 130 + 66
    // budget bytes, remain.
    unsafe {
        let end = src.add(len);
        let limit_a = end.sub(NEED_A);
        let limit_b = end.sub(NEED_B);
        let mut s = src;
        let mut p = dst;
        let mut col = *column;
        let last = ls - 1;
        enter(last, &mut col, &mut s, &mut p);
        // A line start whose byte needs an escape the compare misses goes
        // out from the table here (rapidyenc src/encoder_avx_base.h:116-127).
        // Any other line start stays in the first block, as a line ending on
        // a block's last byte does, so the loads keep the source's grid.
        if col == 0 && start(*s) >> 31 != 0 {
            col = first_byte(s, p);
            p = p.add(col);
            s = s.add(1);
        }
        let mut rem = last - col;
        let k = Consts {
            lut: _mm512_broadcast_i32x4(detect_lut()),
            k42: _mm512_set1_epi8(42),
            k64: _mm512_set1_epi8(64),
            eqv: _mm512_set1_epi8(b'=' as i8),
            iota: _mm512_set_epi8(
                63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43,
                42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22,
                21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0,
            ),
        };
        // The next block's vector is loaded before this block's stores, so
        // the load never waits behind them (Zen 4 counts a store-to-load
        // interlock on every one otherwise).
        let mut v = _mm512_loadu_si512(s.cast());
        let mut acc = 0u64;
        while s <= limit_a {
            // A chunk of blocks without the clean-block branch; when none
            // had an escape, blocks take it until the first that has one.
            let ce = if (limit_a as usize) - (s as usize) > 512 {
                s.add(512)
            } else {
                limit_a
            };
            acc = 0;
            while s <= ce {
                let vn = _mm512_loadu_si512(s.add(64).cast());
                v = if step::<false, ZEN4>(v, k, last, &mut s, &mut p, &mut rem, &mut acc) {
                    vn
                } else {
                    _mm512_loadu_si512(s.cast())
                };
            }
            if acc != 0 {
                continue;
            }
            while s <= limit_a {
                let vn = _mm512_loadu_si512(s.add(64).cast());
                v = if step::<true, ZEN4>(v, k, last, &mut s, &mut p, &mut rem, &mut acc) {
                    vn
                } else {
                    _mm512_loadu_si512(s.cast())
                };
                if acc != 0 {
                    break;
                }
            }
        }
        while s <= limit_b {
            let v = _mm512_loadu_si512(s.cast());
            step::<false, ZEN4>(v, k, last, &mut s, &mut p, &mut rem, &mut acc);
        }
        *column = last - rem;
        (s as usize - src as usize, p as usize - dst as usize)
    }
}

// SAFETY: the `RawEncodeFn` contract; the CPU has the listed features.
#[target_feature(enable = "avx512vbmi2,avx512vl,avx512bw,avx512f,avx2,bmi1,bmi2,popcnt,lzcnt")]
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
            |ls, col, s, n, d| span::<false>(ls, col, s, n, d),
        )
    }
}

// `encode_raw` with the Zen 4 line end.
//
// SAFETY: the `RawEncodeFn` contract; the CPU has the listed features.
// The tests' reference for `encode_asm_zen4`.
#[target_feature(enable = "avx512vbmi2,avx512vl,avx512bw,avx512f,avx2,bmi1,bmi2,popcnt,lzcnt")]
pub(super) unsafe fn encode_raw_zen4(
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
            |ls, col, s, n, d| span::<true>(ls, col, s, n, d),
        )
    }
}

// The object-format lines of the assembly loop below. The loop itself is the
// same bytes on every x86_64 target: numeric local labels (temporary in ELF,
// Mach-O and COFF alike, where a named `.L` label would become a Mach-O
// symbol and split the code into linker atoms), `sym` table operands, and
// SysV argument registers through `extern "sysv64"`. Its spill slots sit in
// the SysV red zone below `rsp`; Windows x64 has no red zone, so there the
// function moves `rsp` down past them after its pushes and back before its
// pops (`A0_RZ`), and elsewhere `rz` is 0 and no instruction is added.
#[cfg(not(any(windows, target_vendor = "apple")))]
macro_rules! a0_rodata {
    () => {
        ".pushsection .rodata.weaver_yenc_a0,\"a\",@progbits"
    };
}
#[cfg(not(any(windows, target_vendor = "apple")))]
macro_rules! a0_text {
    () => {
        ".popsection
.pushsection .text.weaver_yenc_encode_zen4,\"ax\",@progbits
.p2align 6
.globl weaver_yenc_encode_zen4
.hidden weaver_yenc_encode_zen4
.type weaver_yenc_encode_zen4,@function
weaver_yenc_encode_zen4:"
    };
}
#[cfg(not(any(windows, target_vendor = "apple")))]
macro_rules! a0_end {
    () => {
        ".size weaver_yenc_encode_zen4, .-weaver_yenc_encode_zen4
.popsection"
    };
}
#[cfg(target_vendor = "apple")]
macro_rules! a0_rodata {
    () => {
        ".pushsection __TEXT,__const"
    };
}
#[cfg(target_vendor = "apple")]
macro_rules! a0_text {
    () => {
        ".popsection
.pushsection __TEXT,__text,regular,pure_instructions
.p2align 6
.globl _weaver_yenc_encode_zen4
.private_extern _weaver_yenc_encode_zen4
_weaver_yenc_encode_zen4:"
    };
}
#[cfg(target_vendor = "apple")]
macro_rules! a0_end {
    () => {
        ".popsection"
    };
}
#[cfg(windows)]
macro_rules! a0_rodata {
    () => {
        ".pushsection .rdata,\"dr\""
    };
}
#[cfg(windows)]
macro_rules! a0_text {
    () => {
        ".popsection
.pushsection .text,\"xr\"
.p2align 6
.globl weaver_yenc_encode_zen4
.def weaver_yenc_encode_zen4
.scl 2
.type 32
.endef
weaver_yenc_encode_zen4:"
    };
}
#[cfg(windows)]
macro_rules! a0_end {
    () => {
        ".popsection"
    };
}
#[cfg(windows)]
macro_rules! a0_reserve {
    () => {
        "    sub rsp,{rz}"
    };
}
#[cfg(windows)]
macro_rules! a0_release {
    () => {
        "    add rsp,{rz}"
    };
}
#[cfg(not(windows))]
macro_rules! a0_reserve {
    () => {
        ""
    };
}
#[cfg(not(windows))]
macro_rules! a0_release {
    () => {
        ""
    };
}
// The red-zone bytes the loop uses below `rsp` (down to `rsp - 0x50`).
const A0_RZ: usize = if cfg!(windows) { 0x50 } else { 0 };

std::arch::global_asm!(
    a0_rodata!(),
    r#"
.p2align 6
2000:
.byte 224,214,246,19,214,227,214,214,224,223,214,19,4,227,214,214,224,214,246,19,214,227,214,214,224,223,214,19,4,227,214,214,224,214,246,19,214,227,214,214,224,223,214,19,4,227,214,214,224,214,246,19,214,227,214,214,224,223,214,19,4,227,214,214
.p2align 6
2001:
.byte 42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42
.p2align 6
2002:
.byte 61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61,61
.p2align 6
2003:
.byte 223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223
.p2align 6
2004:
.byte 246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246
.p2align 6
2005:
.byte 4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4
.p2align 6
2006:
.byte 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64
.p2align 6
2007:
.byte 106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106,106
"#,
    a0_text!(),
    r#"
    test rcx,rcx
je 2037f
    push rbp
    push r15
    push r14
    push r13
    push r12
    push rbx
"#,
    a0_reserve!(),
    r#"
    cmp rdi,0xc
    mov QWORD PTR [rsp+{rz}-0x40],rdx
jae 2038f
    mov r14,QWORD PTR [rsi]
    test r14,r14
    mov QWORD PTR [rsp+{rz}-0x30],rsi
    mov ebp,r9d
je 2040f
    mov rax,r8
    xor r15d,r15d
    mov r13,r8
    cmp r15,rcx
jae 2041f
2008:
    mov rbx,rcx
    lea r9,[rdi-0x1]
lea    r10,[rip+{escape}+0]
lea    r11,[rip+{escaped}+0]
2009:
    lea rcx,[r15+0x9]
    cmp rcx,rbx
    setae cl
    mov rdx,r14
    sub rdx,rdi
    cmp rdx,0xfffffffffffffff7
    setge dl
    or dl,cl
jne 2027f
    mov r12,rbx
    mov r8,r15
.byte 0x66,0x66,0x66,0x66,0x66,0x2e,0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* data16 data16 data16 data16 cs nop WORD PTR [rax+rax*1+0x0] */
2010:
    mov rsi,rax
    mov r15,QWORD PTR [rsp+{rz}-0x40]
    movzx eax,BYTE PTR [r15+r8*1]
    movzx ecx,BYTE PTR [rax+r10*1]
    test cl,cl
je 2011f
    mov BYTE PTR [rsi],cl
    mov eax,0x1
jmp 2012f
.byte 0x90  /* nop */
2011:
    movzx eax,WORD PTR [r11+rax*2]
    mov WORD PTR [rsi],ax
    mov eax,0x2
2012:
    add rax,rsi
    movzx ecx,BYTE PTR [r15+r8*1+0x1]
    movzx edx,BYTE PTR [rcx+r10*1]
    test dl,dl
je 2013f
    mov BYTE PTR [rax],dl
    mov ecx,0x1
jmp 2014f
.byte 0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    DWORD PTR [rax+rax*1+0x0] */
2013:
    movzx ecx,WORD PTR [r11+rcx*2]
    mov WORD PTR [rax],cx
    mov ecx,0x2
2014:
    add rax,rcx
    movzx ecx,BYTE PTR [r15+r8*1+0x2]
    movzx edx,BYTE PTR [rcx+r10*1]
    test dl,dl
je 2015f
    mov BYTE PTR [rax],dl
    mov ecx,0x1
jmp 2016f
.byte 0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    DWORD PTR [rax+rax*1+0x0] */
2015:
    movzx ecx,WORD PTR [r11+rcx*2]
    mov WORD PTR [rax],cx
    mov ecx,0x2
2016:
    add rax,rcx
    movzx ecx,BYTE PTR [r15+r8*1+0x3]
    movzx edx,BYTE PTR [rcx+r10*1]
    test dl,dl
je 2017f
    mov BYTE PTR [rax],dl
    mov ecx,0x1
jmp 2018f
.byte 0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    DWORD PTR [rax+rax*1+0x0] */
2017:
    movzx ecx,WORD PTR [r11+rcx*2]
    mov WORD PTR [rax],cx
    mov ecx,0x2
2018:
    add rax,rcx
    movzx ecx,BYTE PTR [r15+r8*1+0x4]
    movzx edx,BYTE PTR [rcx+r10*1]
    test dl,dl
je 2019f
    mov BYTE PTR [rax],dl
    mov ecx,0x1
jmp 2020f
.byte 0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    DWORD PTR [rax+rax*1+0x0] */
2019:
    movzx ecx,WORD PTR [r11+rcx*2]
    mov WORD PTR [rax],cx
    mov ecx,0x2
2020:
    add rax,rcx
    movzx ecx,BYTE PTR [r15+r8*1+0x5]
    movzx edx,BYTE PTR [rcx+r10*1]
    test dl,dl
je 2021f
    mov BYTE PTR [rax],dl
    mov ecx,0x1
jmp 2022f
.byte 0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    DWORD PTR [rax+rax*1+0x0] */
2021:
    movzx ecx,WORD PTR [r11+rcx*2]
    mov WORD PTR [rax],cx
    mov ecx,0x2
2022:
    add rax,rcx
    movzx ecx,BYTE PTR [r15+r8*1+0x6]
    movzx edx,BYTE PTR [rcx+r10*1]
    test dl,dl
je 2023f
    mov BYTE PTR [rax],dl
    mov ecx,0x1
jmp 2024f
.byte 0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    DWORD PTR [rax+rax*1+0x0] */
2023:
    movzx ecx,WORD PTR [r11+rcx*2]
    mov WORD PTR [rax],cx
    mov ecx,0x2
2024:
    add rax,rcx
    movzx ecx,BYTE PTR [r15+r8*1+0x7]
    movzx edx,BYTE PTR [rcx+r10*1]
    test dl,dl
je 2025f
    mov BYTE PTR [rax],dl
    mov ecx,0x1
jmp 2026f
.byte 0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    DWORD PTR [rax+rax*1+0x0] */
2025:
    movzx ecx,WORD PTR [r11+rcx*2]
    mov WORD PTR [rax],cx
    mov ecx,0x2
2026:
    add rax,rcx
    lea r15,[r8+0x8]
    sub r14,rsi
    add r14,rax
    add r8,0x11
    cmp r8,r12
jae 2028f
    mov rcx,r14
    sub rcx,rdi
    mov r8,r15
    cmp rcx,0xfffffffffffffff7
jl 2010b
jmp 2028f
2027:
    xor esi,esi
2028:
    test rsi,rsi
    sete cl
    cmp r14,r9
    setl dl
    or dl,cl
    mov r12,rsi
    sub r12,rax
    lea r8,[r15-0x8]
    test dl,dl
    mov ecx,0x0
    cmovne r12,rcx
    cmovne r8,r15
    cmove rax,rsi
    add r12,r14
jmp 2031f
.byte 0x66,0x0f,0x1f,0x44,0x00,0x00  /* nop    WORD PTR [rax+rax*1+0x0] */
2029:
    movzx ecx,WORD PTR [r11+rcx*2]
    mov WORD PTR [rax],cx
    mov ecx,0x2
2030:
    inc r8
    add rax,rcx
    add r12,rcx
    cmp r8,rbx
jae 2042f
2031:
    cmp r12,r9
jge 2032f
    mov rcx,QWORD PTR [rsp+{rz}-0x40]
    movzx ecx,BYTE PTR [rcx+r8*1]
    movzx edx,BYTE PTR [rcx+r10*1]
    test dl,dl
je 2029b
    mov BYTE PTR [rax],dl
    mov ecx,0x1
jmp 2030b
2032:
    cmp r12,rdi
    mov rsi,QWORD PTR [rsp+{rz}-0x40]
jge 2034f
    movzx ecx,BYTE PTR [rsi+r8*1]
    inc r8
    cmp ecx,0x4
je 2033f
    movzx edx,BYTE PTR [r11+rcx*2]
    test dl,dl
je 2033f
    movzx ecx,BYTE PTR [r11+rcx*2+0x1]
    mov BYTE PTR [rax],dl
    mov BYTE PTR [rax+0x1],cl
    add rax,0x2
jmp 2034f
2033:
    add cl,0x2a
    mov BYTE PTR [rax],cl
    inc rax
2034:
    cmp r8,rbx
jae 2042f
    movzx esi,BYTE PTR [rsi+r8*1]
    mov WORD PTR [rax],0xa0d
    movzx ecx,BYTE PTR [r11+rsi*2]
    test cl,cl
je 2035f
    movzx edx,BYTE PTR [r11+rsi*2+0x1]
    mov BYTE PTR [rax+0x3],dl
    mov r14d,0x2
    mov edx,0x4
jmp 2036f
2035:
    add sil,0x2a
    mov r14d,0x1
    mov edx,0x3
    mov ecx,esi
2036:
    inc r8
    add rdx,rax
    mov BYTE PTR [rax+0x2],cl
    mov rax,rdx
    mov r15,r8
    mov r12,r14
    cmp r8,rbx
jb 2009b
jmp 2042f
2037:
    xor eax,eax
    ret
2038:
    mov r12,QWORD PTR [rsi]
    cmp rcx,0x81
    mov QWORD PTR [rsp+{rz}-0x38],rcx
jae 2039f
    xor eax,eax
    xor r10d,r10d
    add rax,r8
    test r12,r12
je 2053f
jmp 2054f
2039:
    lea rax,[rdx+rcx*1]
    add rax,0xffffffffffffff7f
    mov QWORD PTR [rsp+{rz}-0x48],rax
    lea rax,[rdi-0x1]
    mov QWORD PTR [rsp+{rz}-0x50],rax
    cmp r12,rax
    mov QWORD PTR [rsp+{rz}-0x8],r8
    mov QWORD PTR [rsp+{rz}-0x30],rsi
    mov DWORD PTR [rsp+{rz}-0x24],r9d
jae 2044f
    mov r10,rdx
    mov r15,r8
    test r12,r12
    mov r9,QWORD PTR [rsp+{rz}-0x48]
jne 2050f
jmp 2048f
2040:
    movzx eax,BYTE PTR [rdx]
lea    rdx,[rip+{escaped}+0]
    movzx esi,BYTE PTR [rdx+rax*2]
    test sil,sil
je 2045f
    movzx eax,BYTE PTR [rdx+rax*2+0x1]
    mov BYTE PTR [r8],sil
    mov BYTE PTR [r8+0x1],al
    mov r13,r8
    lea rax,[r8+0x2]
    mov r15d,0x1
    mov r14d,0x2
    cmp r15,rcx
jb 2008b
2041:
    mov r12,r14
2042:
    test bpl,bpl
je 2043f
    movzx ecx,BYTE PTR [rax-0x1]
    cmp ecx,0x20
    mov r8,r13
    mov rsi,QWORD PTR [rsp+{rz}-0x30]
jne 2093f
jmp 2094f
2043:
    mov r8,r13
    mov rsi,QWORD PTR [rsp+{rz}-0x30]
jmp 2095f
2044:
jne 2046f
    movzx eax,BYTE PTR [rdx]
lea    rcx,[rip+{eol}+0]
    mov eax,DWORD PTR [rcx+rax*4]
    mov DWORD PTR [r8],eax
    shr eax,0x1b
    lea r15,[r8+rax*1]
    add r15,0x3
    lea r10,[rdx+0x1]
jmp 2047f
2045:
    add al,0x2a
    mov BYTE PTR [r8],al
    mov r13,r8
    lea rax,[r8+0x1]
    mov r14d,0x1
    mov r15d,0x1
    cmp r15,rcx
jb 2008b
jmp 2041b
2046:
    mov WORD PTR [r8],0xa0d
    mov r10,rdx
    lea r15,[r8+0x2]
2047:
    mov r9,QWORD PTR [rsp+{rz}-0x48]
2048:
    movzx eax,BYTE PTR [r10]
lea    rcx,[rip+{start}+0]
    mov eax,DWORD PTR [rcx+rax*4]
    test eax,eax
js 2049f
    xor r12d,r12d
jmp 2050f
2049:
    mov WORD PTR [r15],ax
    shr eax,0x10
    movzx r12d,al
    add r15,r12
    inc r10
2050:
    mov QWORD PTR [rsp+{rz}-0x10],rdi
    mov rax,QWORD PTR [rsp+{rz}-0x38]
    mov rcx,QWORD PTR [rsp+{rz}-0x40]
    add rax,rcx
    add rax,0xffffffffffffffbf
    mov QWORD PTR [rsp+{rz}-0x20],rax
    mov r11,QWORD PTR [rsp+{rz}-0x50]
    sub r11,r12
    cmp r10,r9
jbe 2066f
    movabs r12,0xaaaaaaaaaaaaaaaa
2051:
    cmp r10,QWORD PTR [rsp+{rz}-0x20]
jbe 2083f
    mov rsi,r11
    mov rax,r15
2052:
    mov r12,QWORD PTR [rsp+{rz}-0x50]
    sub r12,rsi
    mov rsi,QWORD PTR [rsp+{rz}-0x30]
    mov QWORD PTR [rsi],r12
    mov rdx,QWORD PTR [rsp+{rz}-0x40]
    sub r10,rdx
    mov r8,QWORD PTR [rsp+{rz}-0x8]
    sub rax,r8
    mov r9d,DWORD PTR [rsp+{rz}-0x24]
    mov rdi,QWORD PTR [rsp+{rz}-0x10]
    add rax,r8
    test r12,r12
jne 2054f
2053:
    cmp r10,QWORD PTR [rsp+{rz}-0x38]
jae 2054f
    movzx ecx,BYTE PTR [rdx+r10*1]
    inc r10
lea    r11,[rip+{escaped}+0]
    movzx edx,BYTE PTR [r11+rcx*2]
    test dl,dl
je 2090f
    movzx ecx,BYTE PTR [r11+rcx*2+0x1]
    mov BYTE PTR [rax],dl
    mov BYTE PTR [rax+0x1],cl
    add rax,0x2
    mov r12d,0x2
2054:
    cmp r10,QWORD PTR [rsp+{rz}-0x38]
jae 2092f
2055:
    mov r13,rdi
    lea r14,[rdi-0x1]
lea    rbx,[rip+{escape}+0]
lea    r15,[rip+{escaped}+0]
jmp 2058f
.byte 0x0f,0x1f,0x40,0x00  /* nop    DWORD PTR [rax+0x0] */
2056:
    movzx ecx,BYTE PTR [rcx+rbx*1]
    test cl,cl
je 2060f
    mov BYTE PTR [rax],cl
    inc rax
    inc r12
2057:
    mov r10,r11
    cmp r11,QWORD PTR [rsp+{rz}-0x38]
jae 2092f
2058:
    mov rcx,QWORD PTR [rsp+{rz}-0x40]
    movzx edx,BYTE PTR [rcx+r10*1]
    movzx ecx,dl
    lea r11,[r10+0x1]
    cmp r12,r14
jl 2056b
    cmp r12,r13
jge 2059f
    cmp dl,0x4
je 2061f
    movzx edi,BYTE PTR [r15+rcx*2]
    test dil,dil
je 2061f
    movzx ecx,BYTE PTR [r15+rcx*2+0x1]
    mov BYTE PTR [rax+0x1],cl
    mov ecx,0x2
jmp 2062f
.byte 0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    DWORD PTR [rax+rax*1+0x0] */
2059:
    mov rcx,rax
jmp 2063f
2060:
    add dl,0x6a
    mov BYTE PTR [rax],0x3d
    mov BYTE PTR [rax+0x1],dl
    add rax,0x2
    add r12,0x2
jmp 2057b
2061:
    add dl,0x2a
    mov ecx,0x1
    mov edi,edx
2062:
    add rcx,rax
    mov BYTE PTR [rax],dil
    cmp r11,QWORD PTR [rsp+{rz}-0x38]
je 2091f
    mov rax,QWORD PTR [rsp+{rz}-0x40]
    movzx edx,BYTE PTR [rax+r10*1+0x1]
    add r10,0x2
    mov r11,r10
2063:
    mov WORD PTR [rcx],0xa0d
    movzx eax,dl
    movzx edx,BYTE PTR [r15+rax*2]
    test dl,dl
je 2064f
    movzx eax,BYTE PTR [r15+rax*2+0x1]
    mov BYTE PTR [rcx+0x2],dl
    mov BYTE PTR [rcx+0x3],al
    add rcx,0x4
    mov r12d,0x2
jmp 2065f
2064:
    add al,0x2a
    mov BYTE PTR [rcx+0x2],al
    add rcx,0x3
    mov r12d,0x1
2065:
    mov rax,rcx
jmp 2057b
2066:
    vmovdqu64 zmm7,ZMMWORD PTR [r10]
vmovdqa64 zmm0,ZMMWORD PTR [rip+2000b]
vmovdqa64 zmm1,ZMMWORD PTR [rip+2001b]
vmovdqa64 zmm2,ZMMWORD PTR [rip+2002b]
vmovdqa64 zmm3,ZMMWORD PTR [rip+2003b]
vmovdqa64 zmm4,ZMMWORD PTR [rip+2004b]
vmovdqa64 zmm5,ZMMWORD PTR [rip+2005b]
vmovdqa64 zmm6,ZMMWORD PTR [rip+2006b]
    movabs r12,0xaaaaaaaaaaaaaaaa
jmp 2069f
.byte 0x0f,0x1f,0x00  /* nop    DWORD PTR [rax] */
2067:
    mov r9,QWORD PTR [rsp+{rz}-0x48]
2068:
    cmp r10,r9
ja 2051b
2069:
    mov rax,r9
    sub rax,r10
    mov rcx,r9
    lea r9,[r10+0x200]
    cmp rax,0x201
    cmovb r9,rcx
    xor esi,esi
    mov QWORD PTR [rsp+{rz}-0x18],r9
jmp 2072f
.byte 0x66,0x66,0x66,0x66,0x66,0x66,0x2e,0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* data16 data16 data16 data16 data16 cs nop WORD PTR [rax+rax*1+0x0] */
2070:
    add r10,0x40
    add r15,r8
    mov r11,r13
2071:
    vmovdqa64 zmm7,zmm8
    or rsi,rax
    cmp r10,r9
ja 2077f
2072:
    vmovdqu64 zmm8,ZMMWORD PTR [r10+0x40]
    vpabsb zmm9,zmm7
    vpshufb zmm9,zmm0,zmm9
    vpcmpeqb k1,zmm9,zmm7
vpblendmb zmm9{{k1}},zmm1,ZMMWORD PTR [rip+2007b]
    kmovq rax,k1
    vpaddb zmm10,zmm9,zmm7
    movabs rdi,0x5555555555555555
    pdep rcx,rax,rdi
    or rcx,r12
    pext rdx,r12,rcx
    mov rcx,rax
    shr rcx,0x20
    pdep rcx,rcx,rdi
    or rcx,r12
    pext rcx,r12,rcx
    kmovq k2,rdx
    vmovdqa64 zmm9,zmm2
    vpexpandb zmm9{{k2}},zmm10
    vextracti64x4 ymm11,zmm10,0x1
    kmovq k2,rcx
    vmovdqa64 zmm10,zmm2
    vpexpandb zmm10{{k2}},zmm11
    vmovdqu64 ZMMWORD PTR [r15],zmm9
    popcnt edi,eax
    vmovdqu64 ZMMWORD PTR [r15+rdi*1+0x20],zmm10
    popcnt r8,rax
    add r8,0x40
    mov r13,r11
    sub r13,r8
jae 2070b
    add rdi,0x20
    bzhi rdx,rdx,r11
    popcnt rdx,rdx
    mov r13,r11
    sub r13,rdi
    mov ebp,0x0
    cmovae rbp,r13
    bzhi rcx,rcx,rbp
    popcnt rbp,rcx
    lea rcx,[rdx+rbp*1]
    bzhi r13,rax,rcx
    vpcmpeqb k0,zmm7,zmm3
    vpcmpeqb k2,zmm7,zmm4
    korq k0,k0,k2
    movzx ebx,BYTE PTR [r10+rcx*1]
    popcnt r12,r13
    lea r13,[r12+rcx*1]
    mov r14d,r11d
    sub r14d,r13d
    shl r14d,0x3
lea    r9,[rip+{eol}+0]
    shrx ebx,DWORD PTR [r9+rbx*4],r14d
    mov DWORD PTR [r15+r11*1],ebx
    korq k1,k0,k1
    kmovq r11,k1
    mov ebx,ecx
    and ebx,0x3f
    lea r14,[r12+rcx*1]
    add r14,0x3
    bt r11,rbx
    adc r15,r14
    bt rax,rbx
    adc r13,0x1
    lea r11,[rdx+rbp*1]
    inc r11
    movzx edx,BYTE PTR [r10+rcx*1+0x1]
lea    r9,[rip+{start}+0]
    mov edx,DWORD PTR [r9+rdx*4]
    cmp rcx,0x3f
jae 2073f
    vpcmpeqb k1,zmm7,zmm5
    korq k0,k1,k0
    kmovq rbx,k0
    shrx rbp,rbx,r11
    and ebp,0x1
    sub r8,r13
    add r8,rbp
    mov r11,QWORD PTR [rsp+{rz}-0x50]
    sub r11,r8
    movabs r12,0xaaaaaaaaaaaaaaaa
    mov r9,QWORD PTR [rsp+{rz}-0x18]
jb 2074f
    mov WORD PTR [r15],dx
    mov rcx,r13
    add rcx,rbp
    add rbp,r15
    sub rbp,r13
    vpbroadcastb zmm7,ecx
    vpcmpltub k1,zmm7,zmm6
    sub ecx,edi
    vpbroadcastb zmm7,ecx
    vpcmpgtb k2,zmm6,zmm7
    vmovdqu8 ZMMWORD PTR [rbp+0x0]{{k1}},zmm9
    vmovdqu8 ZMMWORD PTR [rbp+rdi*1+0x0]{{k2}},zmm10
    add r15,r8
    add r10,0x40
jmp 2071b
.byte 0x66,0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    WORD PTR [rax+rax*1+0x0] */
2073:
    cmp r11d,0x40
    movabs r12,0xaaaaaaaaaaaaaaaa
    mov r9,QWORD PTR [rsp+{rz}-0x18]
jne 2074f
    test edx,edx
js 2076f
    add r10,0x40
    mov r11,QWORD PTR [rsp+{rz}-0x50]
jmp 2071b
.byte 0x66,0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* nop    WORD PTR [rax+rax*1+0x0] */
2074:
    add r10,rcx
    mov WORD PTR [r15],dx
    add r10,0x2
2075:
    shr edx,0x10
    movzx ecx,dl
    add r15,rcx
    mov r11,QWORD PTR [rsp+{rz}-0x50]
    sub r11,rcx
    vmovdqu64 zmm7,ZMMWORD PTR [r10]
    or rsi,rax
    cmp r10,r9
jbe 2072b
jmp 2077f
2076:
    mov WORD PTR [r15],dx
    add r10,0x41
jmp 2075b
.byte 0x66,0x0f,0x1f,0x44,0x00,0x00  /* nop    WORD PTR [rax+rax*1+0x0] */
2077:
    test rsi,rsi
jne 2067b
    mov r9,QWORD PTR [rsp+{rz}-0x48]
    mov rbp,QWORD PTR [rsp+{rz}-0x50]
    cmp r10,r9
ja 2068b
jmp 2080f
.p2align 4
2078:
    add r15,0x40
    sub r11,0x40
2079:
    add r10,0x40
    vmovdqa64 zmm7,zmm8
    cmp r10,r9
ja 2068b
2080:
    vmovdqu64 zmm8,ZMMWORD PTR [r10+0x40]
    vpabsb zmm9,zmm7
    vpshufb zmm9,zmm0,zmm9
    vpcmpeqb k1,zmm9,zmm7
    vpaddb zmm10,zmm7,zmm1
    kortestq k1,k1
jne 2068b
    vmovdqu64 ZMMWORD PTR [r15],zmm10
    cmp r11,0x40
jae 2078b
    movzx eax,BYTE PTR [r10+r11*1]
lea    rbx,[rip+{eol}+0]
    mov eax,DWORD PTR [rbx+rax*4]
    mov DWORD PTR [r15+r11*1],eax
    shr eax,0x1b
    lea rcx,[r15+r11*1+0x3]
    add rcx,rax
    movzx eax,BYTE PTR [r10+r11*1+0x1]
lea    rbx,[rip+{start}+0]
    mov eax,DWORD PTR [rbx+rax*4]
    cmp r11,0x3f
jae 2081f
    mov edx,eax
    shr edx,0x1f
    mov edi,0x3f
    sub rdi,r11
    add rdi,rdx
    cmp rdi,rbp
ja 2082f
    mov WORD PTR [rcx],ax
    lea rbx,[rcx+rdx*1-0x1]
    sub rbx,r11
    lea r8,[r11+rdx*1+0x1]
    vpbroadcastb zmm11,r8d
    vpcmpnleub k2,zmm6,zmm11
    vmovdqu8 ZMMWORD PTR [rbx]{{k2}},zmm10
    lea r15,[rcx+rdi*1]
    mov r11,rbp
    sub r11,rdi
jmp 2079b
2081:
    test eax,eax
js 2082f
    mov r15,rcx
    mov r11,rbp
jmp 2079b
2082:
    mov r15,rcx
    add r10,r11
    mov WORD PTR [r15],ax
    add r10,0x2
    shr eax,0x10
    movzx eax,al
    add r15,rax
    mov r11,rbp
    sub r11,rax
    vmovdqu64 zmm7,ZMMWORD PTR [r10]
jmp 2068b
2083:
vmovdqa64 zmm0,ZMMWORD PTR [rip+2000b]
vmovdqa64 zmm1,ZMMWORD PTR [rip+2001b]
    movabs rbx,0x5555555555555555
vmovdqa64 zmm2,ZMMWORD PTR [rip+2002b]
vmovdqa64 zmm3,ZMMWORD PTR [rip+2003b]
vmovdqa64 zmm4,ZMMWORD PTR [rip+2004b]
vmovdqa64 zmm5,ZMMWORD PTR [rip+2005b]
vmovdqa64 zmm6,ZMMWORD PTR [rip+2006b]
    mov rsi,r11
jmp 2086f
.byte 0x0f,0x1f,0x40,0x00  /* nop    DWORD PTR [rax+0x0] */
2084:
    add r15,r8
    add r10,0x40
    mov rax,r15
2085:
    mov r15,rax
    mov r11,rsi
    cmp r10,QWORD PTR [rsp+{rz}-0x20]
ja 2052b
2086:
    vmovdqu64 zmm9,ZMMWORD PTR [r10]
    vpabsb zmm7,zmm9
    vpshufb zmm7,zmm0,zmm7
    vpcmpeqb k1,zmm7,zmm9
vpblendmb zmm7{{k1}},zmm1,ZMMWORD PTR [rip+2007b]
    kmovq rcx,k1
    vpaddb zmm8,zmm7,zmm9
    pdep rax,rcx,rbx
    or rax,r12
    pext rdx,r12,rax
    mov rax,rcx
    shr rax,0x20
    pdep rax,rax,rbx
    or rax,r12
    pext rax,r12,rax
    kmovq k2,rdx
    vmovdqa64 zmm7,zmm2
    vpexpandb zmm7{{k2}},zmm8
    vextracti64x4 ymm10,zmm8,0x1
    kmovq k2,rax
    vmovdqa64 zmm8,zmm2
    vpexpandb zmm8{{k2}},zmm10
    vmovdqu64 ZMMWORD PTR [r15],zmm7
    popcnt edi,ecx
    vmovdqu64 ZMMWORD PTR [r15+rdi*1+0x20],zmm8
    popcnt r8,rcx
    add r8,0x40
    sub rsi,r8
jae 2084b
    add rdi,0x20
    bzhi rdx,rdx,r11
    popcnt r14,rdx
    mov rdx,r11
    sub rdx,rdi
    mov esi,0x0
    cmovb rdx,rsi
    bzhi rax,rax,rdx
    popcnt r9,rax
    lea rbp,[r9+r14*1]
    bzhi rax,rcx,rbp
    vpcmpeqb k0,zmm9,zmm3
    vpcmpeqb k2,zmm9,zmm4
    korq k0,k0,k2
    movzx r13d,BYTE PTR [r10+rbp*1]
    popcnt rax,rax
    lea rdx,[rax+rbp*1]
    mov r12d,r11d
    sub r12d,edx
    shl r12d,0x3
lea    rsi,[rip+{eol}+0]
    shrx esi,DWORD PTR [rsi+r13*4],r12d
    korq k1,k1,k0
    kmovq r12,k1
    mov r13d,ebp
    and r13d,0x3f
    bt r12,r13
    lea rax,[rax+rbp*1+0x3]
    adc rax,r15
    mov DWORD PTR [r15+r11*1],esi
    bt rcx,r13
    adc rdx,0x1
    lea rsi,[r9+r14*1]
    inc rsi
    movzx ecx,BYTE PTR [r10+rbp*1+0x1]
lea    r9,[rip+{start}+0]
    mov ecx,DWORD PTR [r9+rcx*4]
    cmp rbp,0x3f
jae 2087f
    vpcmpeqb k1,zmm9,zmm5
    korq k0,k1,k0
    kmovq r9,k0
    shrx r9,r9,rsi
    and r9d,0x1
    sub r8,rdx
    add r8,r9
    mov rsi,QWORD PTR [rsp+{rz}-0x50]
    sub rsi,r8
    movabs r12,0xaaaaaaaaaaaaaaaa
jb 2088f
    mov WORD PTR [rax],cx
    lea rcx,[rdx+r9*1]
    add r9,rax
    sub r9,rdx
    vpbroadcastb zmm9,ecx
    vpcmpltub k1,zmm9,zmm6
    sub ecx,edi
    vpbroadcastb zmm9,ecx
    vpcmpgtb k2,zmm6,zmm9
    vmovdqu8 ZMMWORD PTR [r9]{{k1}},zmm7
    vmovdqu8 ZMMWORD PTR [r9+rdi*1]{{k2}},zmm8
    add rax,r8
    add r10,0x40
jmp 2085b
.byte 0x66,0x0f,0x1f,0x44,0x00,0x00  /* nop    WORD PTR [rax+rax*1+0x0] */
2087:
    cmp esi,0x40
    movabs r12,0xaaaaaaaaaaaaaaaa
jne 2088f
    test ecx,ecx
js 2089f
    add r10,0x40
    mov rsi,QWORD PTR [rsp+{rz}-0x50]
jmp 2085b
.byte 0x66,0x66,0x66,0x66,0x66,0x66,0x2e,0x0f,0x1f,0x84,0x00,0x00,0x00,0x00,0x00  /* data16 data16 data16 data16 data16 cs nop WORD PTR [rax+rax*1+0x0] */
2088:
    add r10,rbp
    mov edx,ecx
    shr edx,0x10
    movzx edx,dl
    mov WORD PTR [rax],cx
    add rax,rdx
    mov rsi,QWORD PTR [rsp+{rz}-0x50]
    sub rsi,rdx
    add r10,0x2
jmp 2085b
2089:
    mov edx,ecx
    shr edx,0x10
    movzx edx,dl
    mov WORD PTR [rax],cx
    add rax,rdx
    mov rsi,QWORD PTR [rsp+{rz}-0x50]
    sub rsi,rdx
    add r10,0x41
jmp 2085b
2090:
    add cl,0x2a
    mov BYTE PTR [rax],cl
    inc rax
    mov r12d,0x1
    cmp r10,QWORD PTR [rsp+{rz}-0x38]
jb 2055b
jmp 2092f
2091:
    mov rax,rcx
2092:
    test r9b,r9b
je 2095f
    movzx ecx,BYTE PTR [rax-0x1]
    cmp ecx,0x20
je 2094f
2093:
    cmp ecx,0x9
jne 2095f
2094:
    mov BYTE PTR [rax-0x1],0x3d
    add cl,0x40
    mov BYTE PTR [rax],cl
    inc rax
    inc r12
2095:
    mov QWORD PTR [rsi],r12
    sub rax,r8
"#,
    a0_release!(),
    r#"
    pop rbx
    pop r12
    pop r13
    pop r14
    pop r15
    pop rbp
    vzeroupper
    ret
"#,
    a0_end!(),
    rz = const A0_RZ,
    eol = sym EOL,
    start = sym START,
    escape = sym super::tables::ESCAPE,
    escaped = sym super::tables::ESCAPED,
);

// `encode_raw_zen4` as LLVM emitted it for Linux (64-byte function
// alignment), pinned as assembly so its schedule cannot drift with the
// compiler, with one change: the clean loop a chunk without escapes turns on
// is a lean one (line end straight from the `EOL` and `START` tables, no
// expand, no mask chain), and its first block with an escape goes back to a
// new chunk, so long runs with nothing to escape skip the expand and the
// mask chain. It builds on every x86_64 target: it takes the
// SysV argument registers and clobbers what SysV lets a callee clobber, so
// on Windows the caller saves RSI, RDI and XMM6-XMM15 around the call.
unsafe extern "sysv64" {
    fn weaver_yenc_encode_zen4(
        line_size: usize,
        column: &mut usize,
        src: *const u8,
        len: usize,
        dst: *mut u8,
        is_end: bool,
    ) -> usize;
}
unsafe fn encode_asm_zen4(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize {
    unsafe { weaver_yenc_encode_zen4(line_size, column, src, len, dst, is_end) }
}

// The tier's loop for this CPU, chosen once per process: the Zen 4 line end
// on AMD family 19h, the table line end everywhere else. The Zen 4 line end
// measured faster only on Zen 4 and slower on
// Sapphire Rapids; Zen 5 (family 1Ah) is unmeasured, so it keeps the table
// line end. Family 19h with AVX-512 VBMI2 is Zen 4 alone (Zen 3 has no
// AVX-512).
pub(super) fn encode_fn() -> RawEncodeFn {
    static PICK: OnceLock<RawEncodeFn> = OnceLock::new();
    *PICK.get_or_init(|| {
        // The assembly loop is picked on Linux only. Its Windows stack frame
        // (`A0_RZ`) and its Mach-O build have not yet run on any CPU, so
        // there it stays held back, exercised by the tests, until it has.
        let zen4 = if cfg!(target_os = "linux") {
            encode_asm_zen4 as RawEncodeFn
        } else {
            encode_raw_zen4 as RawEncodeFn
        };
        if is_zen4_family(cpu_family()) {
            zen4
        } else {
            encode_raw as RawEncodeFn
        }
    })
}

// The display family as the decoder computes it (src/simd/x86_common.rs:
// 355-367; rapidyenc src/platform.cc:112): base family plus extended family
// shifted left 4, so AMD family 19h (base 0xf, extended 0xa) is 0xaf.
fn cpu_family() -> u32 {
    // SAFETY: CPUID leaf 1 exists on every x86_64 CPU.
    #[allow(unused_unsafe)]
    let eax = unsafe { __cpuid(1) }.eax;
    ((eax >> 8) & 0xf) + ((eax >> 16) & 0xff0)
}

fn is_zen4_family(family: u32) -> bool {
    family == 0xaf
}

#[cfg(test)]
mod tests {
    use super::super::{encode_max_len, scalar};
    use super::*;

    #[test]
    fn zen4_family_is_amd_family_19h_only() {
        // CPUID leaf 1 EAX: Zen 4 (family 19h, model 61h), Zen 5 (family
        // 1Ah, model 44h), Sapphire Rapids (family 6, model 8Fh).
        let fam = |eax: u32| ((eax >> 8) & 0xf) + ((eax >> 16) & 0xff0);
        assert!(is_zen4_family(fam(0x00a6_0f12)));
        assert!(!is_zen4_family(fam(0x00b4_0f40)));
        assert!(!is_zen4_family(fam(0x0008_06f8)));
    }

    // One input through every loop against the scalar encoder: same bytes,
    // length and column, and nothing written past `encode_max_len` (a guard
    // of 256 bytes after it must stay untouched).
    fn check(loops: &[(&str, RawEncodeFn)], src: &[u8], ls: usize, col0: usize, what: &str) {
        let len = src.len();
        let cap = encode_max_len(len, ls);
        for is_end in [false, true] {
            let mut want = vec![0u8; cap];
            let mut wcol = col0;
            // SAFETY: `want` holds `encode_max_len` bytes; the domain is in
            // range; scalar needs no feature.
            let wn = unsafe {
                scalar::encode_raw(ls, &mut wcol, src.as_ptr(), len, want.as_mut_ptr(), is_end)
            };
            for &(name, f) in loops {
                let mut got = vec![0xa5u8; cap + 256];
                let mut gcol = col0;
                // SAFETY: as above (the guard is extra); the tier's features
                // were checked.
                let gn = unsafe { f(ls, &mut gcol, src.as_ptr(), len, got.as_mut_ptr(), is_end) };
                assert!(
                    gn == wn && gcol == wcol && got[..gn] == want[..wn],
                    "{name}: {what} len {len} line {ls} col {col0} end {is_end}"
                );
                assert!(
                    got[cap..].iter().all(|&b| b == 0xa5),
                    "{name}: wrote past the budget: {what} len {len} line {ls} col {col0} end {is_end}"
                );
            }
        }
    }

    // Every loop (the table loop, the Zen 4 Rust loop and, on Linux, its
    // assembly form) gives the scalar encoder's bytes and column on any host
    // with the tier's features, whichever one `encode_fn` picks here: seeded
    // inputs of the fixtures' kinds (random, every byte critical, zeros, half
    // line-edge bytes, clean with line-edge bytes, clean runs between random
    // ones, escapes on each block's first and last byte), lengths below,
    // at and past one block and the loops' limits, several line sizes and
    // start columns, so every block kind meets a line end at every offset,
    // and the Zen 4 loops enter their clean loop and leave it again. A sweep
    // of every start column puts the line end on every byte of a block,
    // including its last.
    #[test]
    fn both_loops_match_scalar() {
        use super::super::EncoderTier;
        if !EncoderTier::Avx512Vbmi2.cpu_supported() {
            return;
        }
        let loops: &[(&str, RawEncodeFn)] = &[
            ("table", encode_raw),
            ("zen4", encode_raw_zen4),
            ("zen4-asm", encode_asm_zen4),
        ];
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        const CRIT: [u8; 4] = [214, 224, 227, 19];
        const EDGE: [u8; 3] = [223, 246, 4];
        let mut make = |kind: usize, len: usize| -> Vec<u8> {
            (0..len)
                .map(|i| {
                    let r = next();
                    match kind {
                        0 => r as u8,
                        1 => CRIT[(r >> 8) as usize % 4],
                        2 => 0,
                        3 if r & 1 == 0 => EDGE[(r >> 8) as usize % 3],
                        3 => (r >> 16) as u8,
                        // Clean blocks (no byte the compare finds) with
                        // line-edge bytes, for the clean line end.
                        4 if r & 3 == 0 => EDGE[(r >> 8) as usize % 3],
                        4 => 0,
                        // 1500 bytes as kind 4, then 700 random.
                        5 if i % 2200 < 1500 && r & 3 == 0 => EDGE[(r >> 8) as usize % 3],
                        5 if i % 2200 < 1500 => 0,
                        5 => r as u8,
                        // Escapes on each block's first and last byte, a
                        // line-edge byte now and then, zeros between.
                        6 if i % 64 == 0 || i % 64 == 63 => CRIT[(r >> 8) as usize % 4],
                        6 if r & 7 == 0 => EDGE[(r >> 8) as usize % 3],
                        _ => 0,
                    }
                })
                .collect()
        };
        let lens = [
            0usize, 1, 63, 64, 65, 128, 129, 130, 192, 193, 200, 257, 1000, 4096, 9001,
        ];
        for kind in 0..7 {
            for &len in &lens {
                let src = make(kind, len);
                for &ls in &[12usize, 64, 66, 70, 127, 128, 129, 200, 997] {
                    for &col0 in &[0usize, 1, ls / 2, ls - 1, ls, ls + 3] {
                        check(loops, &src, ls, col0, &format!("kind {kind}"));
                    }
                }
            }
        }
        for kind in [1usize, 2, 4, 6] {
            let src = make(kind, 1000);
            for &ls in &[64usize, 65, 128, 129] {
                for col0 in 0..=ls + 1 {
                    check(loops, &src, ls, col0, &format!("sweep kind {kind}"));
                }
            }
        }
    }

    // The Zen 4 loops' clean loop entered and left at every block of a
    // chunk: zeros (clean, so the first chunk turns the clean loop on) with
    // an escaped byte on the first, middle or last byte of block `b`, then a
    // second one 9 blocks on, so the loop leaves the clean loop, runs a chunk
    // and enters it again; line ends fall inside the clean runs (line sizes
    // below and above a block), and line-edge bytes sit on some line ends.
    // An every-byte-critical tail ends the input, for the escape loop's line
    // end after a clean run.
    #[test]
    fn clean_loop_entry_and_exit_at_every_block() {
        use super::super::EncoderTier;
        if !EncoderTier::Avx512Vbmi2.cpu_supported() {
            return;
        }
        let loops: &[(&str, RawEncodeFn)] = &[
            ("table", encode_raw),
            ("zen4", encode_raw_zen4),
            ("zen4-asm", encode_asm_zen4),
        ];
        for b in 0..20usize {
            for o in [0usize, 31, 63] {
                let mut src = vec![0u8; 64 * 48 + 300];
                src[64 * b + o] = 214;
                src[64 * (b + 9) + (63 - o)] = 19;
                for i in (5..64 * 40).step_by(97) {
                    src[i] = [223u8, 246, 4][i % 3];
                }
                let n = src.len();
                for (i, c) in src[n - 300..].iter_mut().enumerate() {
                    *c = [214u8, 224, 227, 19][i % 4];
                }
                for &ls in &[12usize, 64, 100, 128, 129, 997] {
                    for &col0 in &[0usize, 1, ls / 2, ls - 1] {
                        check(
                            loops,
                            &src,
                            ls,
                            col0,
                            &format!("clean block {b} offset {o}"),
                        );
                    }
                }
            }
        }
    }
}
