// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The line loop every x86 tier shares, and its tables.
//
// rapidyenc's x86 kernels (src/encoder_sse_base.h, src/encoder_avx_base.h)
// encode a vector, let the column run past the line end, then walk back
// (`col >= 0` ... `eqMask >>= shiftAmt` ... `p -= col`), write the last char
// and CRLF with `eolLastChar`, and restart the vector stream at the next
// line's first byte, its first-char escape folded into the next mask
// (`eolFirstMask`, src/encoder_sse_base.h:675).
//
// This loop has the same shape. A block of `N` source bytes with escape mask
// `m` encodes to exactly `N + popcnt(m)` bytes, stored without a bounds
// check (the next store overwrites what runs past). When that reaches the
// line's last column, `Block::locate` gives the source byte at that column
// and whether an escape straddles it; one 4-byte `EOL` store writes the last
// byte and CRLF (shifted one byte when an escape straddles), the next line's
// first byte is written there when only the first-char rule escapes it, and
// the stream restarts after it (or keeps its stride when the line ended on
// the block's last byte).
//
// Output is byte-identical to rapidyenc (and to the scalar encoder): the line
// rules are those of `do_encode_generic` (src/encoder.cc:33-151).

use super::tables::{DOT, ESCAPED};

// One tier's block primitive.
pub(super) trait Block {
    // Source bytes per block.
    const N: usize;
    // Bytes `store` may read from its source pointer (at least `N`).
    const READ_AHEAD: usize;
    type V: Copy;
    // Find the line end of a block without escapes by a branch (SSE2, as
    // rapidyenc prefers branching below SSSE3, src/encoder_sse_base.h:162).
    const BRANCH_NOESC: bool = false;
    // Load the next block's vector before this block's stores.
    const PIPELINE: bool = true;
    // Keep the block grid fixed across line ends (see `step_grid`).
    const GRID: bool = false;
    // On the grid, decide once per two blocks (see `step_grid2`).
    const GRID2: bool = false;
    // Lay out the arms rapidyenc marks unlikely (`LIKELIHOOD`, src/common.h:
    // 358-366) out of line, as its gcc build does: the dense arm, a block
    // that reaches the line end, the line's first byte (see each site).
    const COLD: bool = false;

    // Load `N` source bytes.
    unsafe fn load(s: *const u8) -> Self::V;
    // Bit `j` set when source byte `j` encodes to NUL, LF, CR or '='.
    unsafe fn crit_mask(v: Self::V) -> u64;
    // Write the encoding of the `N` bytes at `s` (whose vector is `v`) at `p`,
    // escaping exactly the bytes in `m`; return the encoded width. May store
    // up to `2 * N` bytes (SSE2: up to 96) past `p` and read `READ_AHEAD`
    // bytes from `s`. `m` may hold bits only below `N`.
    unsafe fn store(v: Self::V, s: *const u8, m: u64, p: *mut u8) -> usize;
    // Population count, the tier's fastest way.
    fn popcnt(x: u64) -> usize;
    // `store` for a block known to hold at most one escape, and for one known
    // to hold two or more: `step_grid` branches once on the mask and each arm
    // runs its own store and line end (the NEON tier's two arms,
    // neon.rs:874-906), so no mask test is live across a store.
    #[inline(always)]
    unsafe fn store_one(v: Self::V, s: *const u8, m: u64, p: *mut u8) -> usize {
        unsafe { Self::store(v, s, m, p) }
    }
    #[inline(always)]
    unsafe fn store_many(v: Self::V, s: *const u8, m: u64, p: *mut u8) -> usize {
        unsafe { Self::store(v, s, m, p) }
    }
    // A break block with at most one escape: store it at `p` as `store_one`,
    // run `edge` (the line end and the next line's first byte, written over
    // the block's output), then store the encoding of source bytes `f + 1..N`
    // at `q`. Default: a second load at `s + f + 1`; 64 bytes are readable at
    // `s + f + 1`.
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
            Self::store_one(v, s, m, p);
            edge();
            let rs = s.add(f + 1);
            Self::store_one(Self::load(rs), rs, m >> (f + 1), q);
        }
    }
    // For a block escaped by `m` whose output covers offset `r` (`r` below
    // its width): `(k, st)`, `k` the source index of the byte whose encoding
    // covers offset `r`, `st` 1 when that byte's '=' sits at `r - 1` (an
    // escape straddles the line's last column). Default: at most one escape
    // by its index, more through the 8-byte `EQ16` groups (`N <= 32`).
    #[inline(always)]
    unsafe fn locate(m: u64, r: usize) -> (usize, usize) {
        if m & m.wrapping_sub(1) == 0 {
            locate_one(m, r)
        } else {
            let eq = out_eq32(m);
            (r - Self::popcnt(eq & low64(r)), (((eq << 1) >> r) & 1) as usize)
        }
    }
}

// The dense store of a grid tier, and what its line end needs. rapidyenc's
// slow arm finds a line end's '=' positions from the PSHUFB controls it
// loaded for the stores, still in vector registers (`_mm_movemask_epi8` of
// `shuf1A`, `shuf2A`, src/encoder_sse_base.h:389-404), so no byte index of
// the mask, nor a width taken from them, is held across the stores.
pub(super) trait Dense: Block {
    type D: Copy;
    // `store_many`, with its line-end state.
    unsafe fn store_dense(v: Self::V, m: u64, p: *mut u8) -> (usize, Self::D);
    // The block's output-space '=' positions (`out_eq32` of its mask).
    unsafe fn dense_eq(d: Self::D) -> u64;
}

// `locate` for at most one escape: the byte at index `j` takes offsets `j`
// and `j + 1`, so offset `r` is source byte `r - (r > j)`, and it straddles
// when `r == j + 1`. No escape: `j` is 64 and `k == r`.
#[inline(always)]
pub(super) fn locate_one(m: u64, r: usize) -> (usize, usize) {
    let j = m.trailing_zeros() as usize;
    (r - (r > j) as usize, (r == j + 1) as usize)
}

// Output-space '=' positions of a 32-byte block escaped by `m`.
#[inline(always)]
fn out_eq32(m: u64) -> u64 {
    let mut eq = 0u64;
    let mut off = 0usize;
    let mut g = 0;
    while g < 4 {
        let bits = ((m >> (8 * g)) & 0xff) as usize;
        eq |= (EQ16[bits] as u64) << off;
        off += 8 + BITS[bits] as usize;
        g += 1;
    }
    eq
}

// ---- tables ---------------------------------------------------------------

// A table on a 64-byte boundary, so a 64-byte entry is one cache line and no
// 16- or 32-byte row load splits a line. rapidyenc allocates its tables
// aligned (`ALIGN_ALLOC(lookups, sizeof(*lookups), 16)`,
// src/encoder_sse_base.h:31; 32 for AVX2, src/encoder_avx_base.h:32) and
// reads them with aligned loads (`_mm_load_si128`, :312-334). A plain byte
// array sits wherever the linker puts it (`SMX` landed 4 bytes past a
// 16-byte boundary in one build), so split loads came and went with the
// build's layout.
#[repr(C, align(64))]
pub(super) struct Align64<T>(pub(super) T);

impl<T> core::ops::Deref for Align64<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &T {
        &self.0
    }
}

// rapidyenc `shufMix[i].shuf` (src/encoder_sse_base.h:32-47): the PSHUFB
// control expanding 8 source bytes with escape mask `i`; an escape position
// gets a high-bit index (zero) that `MIX` turns into '='. Positions past the
// group's width hold junk; the next store overwrites them. Then the same for
// the upper 8 bytes of a 16-byte vector (rapidyenc ORs 8 in), then rapidyenc
// `shufMix[i].mix` (src/encoder_sse_base.h:51-58): added after the shuffle,
// it is '=' at escape positions, 42 at plain data, 42+64 at escaped data, so
// the raw source byte becomes its encoding. One 64-byte entry per mask, as
// rapidyenc keeps `shuf` and `mix` side by side in one `shufMix` entry: the
// dense path needs one table base register, not three.
pub(super) static SMX: Align64<[[[u8; 16]; 4]; 256]> = Align64(build_smx());

const fn build_smx() -> [[[u8; 16]; 4]; 256] {
    let shuf = build_shuf(0);
    let hi = build_shuf(8);
    let mix = build_mix();
    let mut t = [[[0u8; 16]; 4]; 256];
    let mut i = 0;
    while i < 256 {
        t[i][0] = shuf[i];
        t[i][1] = hi[i];
        t[i][2] = mix[i];
        t[i][3][0] = 8 + (i as u32).count_ones() as u8;
        i += 1;
    }
    t
}
// Output-space '=' positions of an 8-byte group (rapidyenc's `expandMask`
// marks the data positions instead).
pub(super) static EQ16: [u16; 256] = build_eq16();
// popcnt of a byte, for tiers without POPCNT (rapidyenc
// `BitsSetTable256plus8` without the +8).
pub(super) static BITS: [u8; 256] = build_bits();
// rapidyenc `eolLastChar` (src/encoder_sse_base.h:61): a line's last byte
// plus CRLF as one little-endian 4-byte store. Width is 3, or 4 when bit 27
// is set (escaped form ends in '\n' = 0x0a in the top byte).
pub(super) static EOL: [u32; 256] = build_eol();
// A line's first byte as one little-endian 2-byte store: its escape, or its
// encoding then a byte the next store overwrites.
pub(super) static FIRST: [u16; 256] = build_first();
// `FIRST` with the byte's width (1, or 2 when escaped) in bits 16..: one load
// gives a line start's bytes and width (the NEON tier's `START_ENC`,
// neon.rs:520, :844-845), so the width needs no mask of the block.
pub(super) static START: [u32; 256] = build_start();

const fn build_start() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        t[i] = FIRST[i] as u32 | ((1 + (ESCAPED[i][0] != 0) as u32) << 16);
        i += 1;
    }
    t
}

const fn build_first() -> [u16; 256] {
    let mut t = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let e = ESCAPED[i];
        t[i] = if e[0] != 0 {
            (e[0] as u16) | ((e[1] as u16) << 8)
        } else {
            (i as u8).wrapping_add(42) as u16
        };
        i += 1;
    }
    t
}

const fn build_shuf(add: u8) -> [[u8; 16]; 256] {
    let mut t = [[0u8; 16]; 256];
    let mut i = 0;
    while i < 256 {
        let mut p = 0;
        let mut j = 0;
        // Positions past the group's width take index 0x40 (no high bit, as
        // rapidyenc's `8+p+0x40`, src/encoder_sse_base.h:46-47), so the
        // control's PMOVMSKB is exactly the group's '=' positions.
        let mut row = [0x40u8; 16];
        while j < 8 {
            if i & (1 << j) != 0 {
                row[j + p] = 0x80;
                p += 1;
            }
            row[j + p] = j as u8 + add;
            j += 1;
        }
        t[i] = row;
        i += 1;
    }
    t
}

const fn build_mix() -> [[u8; 16]; 256] {
    let mut t = [[0u8; 16]; 256];
    let mut i = 0;
    while i < 256 {
        let mut p = 0;
        let mut j = 0;
        let mut row = [0u8; 16];
        while j < 8 {
            if i & (1 << j) != 0 {
                row[j + p] = b'=';
                p += 1;
                row[j + p] = 42 + 64;
            } else {
                row[j + p] = 42;
            }
            j += 1;
        }
        t[i] = row;
        i += 1;
    }
    t
}

const fn build_eq16() -> [u16; 256] {
    let mut t = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut p = 0;
        let mut j = 0;
        let mut bits = 0u16;
        while j < 8 {
            if i & (1 << j) != 0 {
                bits |= 1 << (j + p);
                p += 1;
            }
            j += 1;
        }
        t[i] = bits;
        i += 1;
    }
    t
}

const fn build_bits() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 256 {
        t[i] = (i as u32).count_ones() as u8;
        i += 1;
    }
    t
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

// Popcount for tiers without POPCNT (SWAR, branch-free).
#[inline(always)]
pub(super) fn popcnt_table(x: u64) -> usize {
    let x = x - ((x >> 1) & 0x5555_5555_5555_5555);
    let x = (x & 0x3333_3333_3333_3333) + ((x >> 2) & 0x3333_3333_3333_3333);
    let x = (x + (x >> 4)) & 0x0f0f_0f0f_0f0f_0f0f;
    (x.wrapping_mul(0x0101_0101_0101_0101) >> 56) as usize
}

#[inline(always)]
pub(super) fn low64(n: usize) -> u64 {
    if n >= 64 { !0 } else { (1u64 << n) - 1 }
}

// 1 when a line's first byte `c` must be escaped but is not one of the four
// bytes the vector compare finds: TAB, SPACE, '.'.
#[inline(always)]
pub(super) fn first_only_bit(c: u8) -> u64 {
    (c == b'\t'.wrapping_sub(42) || c == b' '.wrapping_sub(42) || c == DOT) as u64
}

// Entry at a line's last column or past it (the tail's rules): write the
// last byte if the line has room for it and CRLF, leaving `col` 0.
//
// SAFETY: 1 byte readable at `*s`, 4 writable at `*p`.
#[inline(always)]
pub(super) unsafe fn enter(last: usize, col: &mut usize, s: &mut *const u8, p: &mut *mut u8) {
    unsafe {
        if *col >= last {
            if *col == last {
                let e = EOL[**s as usize];
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

// ---- the loop -------------------------------------------------------------

// One block at `s` (vector `v`): store it, and at the line's last column
// write the last byte and CRLF and the next line's first byte when only the
// first-char rule escapes it. `room` is the line's room left (the last
// column minus the column). Returns true when the next block starts at
// `s + N` (`*s` already advanced), false when the stream restarts at `*s`.
//
// SAFETY: `READ_AHEAD` and `N + 2` bytes readable at `*s`; output inside
// the budget (see `main_loop`).
#[inline(always)]
unsafe fn step<B: Block>(
    v: B::V,
    last: usize,
    s: &mut *const u8,
    p: &mut *mut u8,
    room: &mut isize,
) -> bool {
    unsafe {
        let m;
        let w;
        let nr;
        // A block without escapes stores, steps and continues with constant
        // widths, as rapidyenc's SSE2 `noesc` path
        // (src/encoder_sse_base.h:474-484).
        if B::BRANCH_NOESC && B::crit_mask(v) == 0 {
            m = 0;
            B::store(v, *s, 0, *p);
            w = B::N;
            nr = *room - B::N as isize;
            if nr >= 0 {
                *p = p.add(B::N);
                *room = nr;
                *s = s.add(B::N);
                return true;
            }
        } else {
            m = B::crit_mask(v);
            w = B::store(v, *s, m, *p);
            // The room left goes negative when the block reaches the line's
            // last column: one subtract sets the flag the branch reads
            // (rapidyenc counts its column up to zero the same way).
            nr = *room - w as isize;
            if nr >= 0 {
                *p = p.add(w);
                *room = nr;
                *s = s.add(B::N);
                return true;
            }
        }
        let r = (nr + w as isize) as usize;
        // Output offset `r` is the line's last column. Without escapes it is
        // source byte `r`, found by a branch rather than from the mask, so
        // the next load's address does not wait on this block's compare
        // (rapidyenc's SSE2 `noesc` line end, src/encoder_sse_base.h:474-476,
        // :617-619).
        let (k, st) = if B::BRANCH_NOESC && m == 0 {
            (r, 0)
        } else {
            B::locate(m, r)
        };
        let e = EOL[*s.add(k) as usize] >> (8 * st);
        p.add(r).cast::<u32>().write_unaligned(e.to_le());
        let mut q = p.add(r + 3 + (e >> 27) as usize);
        let mut nk = k + 1;
        let mut rm = last;
        // The next line's first byte: the compare escapes NUL, LF, CR and
        // '='; TAB, SPACE and '.' (rare) are written here, so no block mask
        // carries a first-char bit.
        let c = *s.add(nk);
        if first_only_bit(c) != 0 {
            q.cast::<[u8; 2]>().write_unaligned(ESCAPED[c as usize]);
            q = q.add(2);
            nk += 1;
            rm = last - 2;
        }
        *p = q;
        *room = rm as isize;
        // A line that ends on the block's last byte continues at the next
        // block, whose vector is already loaded; the branch keeps the stride
        // fixed there too.
        if nk == B::N {
            *s = s.add(B::N);
            true
        } else {
            *s = s.add(nk);
            false
        }
    }
}

// The grid loop, generated twice (`cold`, `plain`): `$cold` puts
// `std::hint::cold_path` on the arms rapidyenc marks unlikely. It must be a
// literal: rustc reads `cold_path` from the generic MIR, where a branch on
// `B::COLD` is not folded yet, so the hint would sit behind it and be lost
// (the SSSE3 loop compiled with the hints off).
macro_rules! grid_loop {
    ($cold:literal) => {
    // `step` with the block grid fixed across line ends, the NEON tier's line
    // start (neon.rs:719-913): the break block is stored whole, the last byte and
    // CRLF go over it at the line's last column, the next line's first byte is a
    // table store whose width comes from the block's own masks (no branch, no
    // load on the column chain), and the rest of the block is stored after it.
    // The next block is still `N` on, so no load address waits on where the line
    // ended. A line that ends on the block's last byte keeps `step`'s handling.
    //
    // SAFETY: `2 * N + 64` bytes readable at `*s`; the line holds `2 * N + 2`
    // columns; output as `step`.
    #[inline(always)]
    unsafe fn step_grid<B: Dense>(
        v: B::V,
        last: usize,
        s: &mut *const u8,
        p: &mut *mut u8,
        room: &mut isize,
    ) -> bool {
        unsafe {
            let m = B::crit_mask(v);
            if m & m.wrapping_sub(1) == 0 {
                grid_one::<B>(v, m, last, s, p, room)
            } else {
                // rapidyenc `LIKELIHOOD(0.089, manyBitsSet)`
                // (src/encoder_sse_base.h:281): the sparse arm falls through.
                if $cold {
                    std::hint::cold_path();
                }
                grid_many::<B>(v, m, last, s, p, room)
            }
        }
    }

    // Two blocks per decision, rapidyenc's 64-byte iteration
    // (src/encoder_avx_base.h:128-131, :163): one test sends both blocks down
    // the sparse path when neither has two escapes, and one width test covers
    // both, so a line takes about half the branches `step_grid` takes and the
    // clean path stores two blocks per branch pair. A line end inside the pair
    // is the block it falls in, through `grid_one`; the other block cannot end a
    // line too (the line holds `2 * N + 8` columns or more).
    //
    // SAFETY: `3 * N + 64` bytes readable at `*s`; otherwise as `step_grid`.
    #[inline(always)]
    unsafe fn step_grid2<B: Dense>(
        va: B::V,
        vb: B::V,
        last: usize,
        s: &mut *const u8,
        p: &mut *mut u8,
        room: &mut isize,
    ) -> bool {
        unsafe {
            let ma = B::crit_mask(va);
            let mb = B::crit_mask(vb);
            // Either block dense: both take the general expansion, with no second
            // sparse test, as rapidyenc's dense arm covers both vectors
            // (src/encoder_avx_base.h:163-255).
            if (ma & ma.wrapping_sub(1)) | (mb & mb.wrapping_sub(1)) != 0 {
                // rapidyenc `LIKELIHOOD(0.170, ...)` (src/encoder_avx_base.h:163).
                if $cold {
                    std::hint::cold_path();
                }
                if !grid_many::<B>(va, ma, last, s, p, room) {
                    return false;
                }
                return grid_many::<B>(vb, mb, last, s, p, room);
            }
            let wa = B::N + (ma != 0) as usize;
            let wb = B::N + (mb != 0) as usize;
            let nr = *room - (wa + wb) as isize;
            if nr >= 0 {
                B::store_one(va, *s, ma, *p);
                B::store_one(vb, s.add(B::N), mb, p.add(wa));
                *p = p.add(wa + wb);
                *room = nr;
                *s = s.add(2 * B::N);
                return true;
            }
            // rapidyenc `LIKELIHOOD(0.3, col >= 0)` (src/encoder_sse_base.h:604).
            if $cold {
                std::hint::cold_path();
            }
            if *room - wa as isize >= 0 {
                B::store_one(va, *s, ma, *p);
                *p = p.add(wa);
                *room -= wa as isize;
                *s = s.add(B::N);
                return grid_one::<B>(vb, mb, last, s, p, room);
            }
            if !grid_one::<B>(va, ma, last, s, p, room) {
                return false;
            }
            B::store_one(vb, *s, mb, *p);
            *p = p.add(wb);
            *room -= wb as isize;
            *s = s.add(B::N);
            true
        }
    }

    // A line that ends on the block's last byte, after its last byte and CRLF
    // (`q` past them): the next line starts at the next block, and its first
    // byte is escaped by a branch, as `step`.
    #[inline(always)]
    unsafe fn grid_edge_n<B: Block>(
        q: *mut u8,
        last: usize,
        s: &mut *const u8,
        p: &mut *mut u8,
        room: &mut isize,
    ) -> bool {
        unsafe {
            let c = *s.add(B::N);
            if first_only_bit(c) != 0 {
                // rapidyenc `LIKELIHOOD(0.0273, escapedLUT[c] != 0)` for a line's
                // first byte (src/encoder_sse_base.h:198).
                if $cold {
                    std::hint::cold_path();
                }
                q.cast::<[u8; 2]>().write_unaligned(ESCAPED[c as usize]);
                *p = q.add(2);
                *room = (last - 2) as isize;
                *s = s.add(B::N + 1);
                return false;
            }
            *p = q;
            *room = last as isize;
            *s = s.add(B::N);
            true
        }
    }

    // At most one escape (the common case), at index `j` (64 when none): the
    // block's width is `N + (m != 0)`, and every width of a break block comes
    // from compares on `j`, so nothing on the line's column and cursor chain
    // waits on a popcount (the NEON tier's sparse break, neon.rs:808-814,
    // :882-889).
    #[inline(always)]
    unsafe fn grid_one<B: Block>(
        v: B::V,
        m: u64,
        last: usize,
        s: &mut *const u8,
        p: &mut *mut u8,
        room: &mut isize,
    ) -> bool {
        unsafe {
            let w = B::N + (m != 0) as usize;
            let r = *room;
            let nr = r - w as isize;
            if nr >= 0 {
                B::store_one(v, *s, m, *p);
                *p = p.add(w);
                *room = nr;
                *s = s.add(B::N);
                return true;
            }
            // rapidyenc `LIKELIHOOD(0.3, col >= 0)` (src/encoder_sse_base.h:604).
            if $cold {
                std::hint::cold_path();
            }
            let r = r as usize;
            let (k, st) = locate_one(m, r);
            let j = m.trailing_zeros() as usize;
            let e = EOL[*s.add(k) as usize] >> (8 * st);
            let q = p.add(r + 3 + (e >> 27) as usize);
            let f = k + 1;
            let pp = *p;
            if f == B::N {
                B::store_one(v, *s, m, pp);
                pp.add(r).cast::<u32>().write_unaligned(e.to_le());
                return grid_edge_n::<B>(q, last, s, p, room);
            }
            let fe = START[*s.add(f) as usize];
            let fb = (fe >> 16) as usize - 1;
            // The rest's width: the block's width less its first `w1` output
            // columns (neon.rs:882, :889 counts the same).
            let rw = w - (f + 1 + (j <= f) as usize);
            let q2 = q.add(1 + fb);
            B::break_one(v, *s, m, pp, f, q2, || {
                pp.add(r).cast::<u32>().write_unaligned(e.to_le());
                q.cast::<u16>().write_unaligned((fe as u16).to_le());
            });
            *p = q2.add(rw);
            *room = (last - 1 - fb - rw) as isize;
            *s = s.add(B::N);
            true
        }
    }

    // Two or more escapes: the widths come from popcounts, and the rest of a
    // break block is stored again from one load at the next line's second byte
    // with the block's mask shifted down.
    #[inline(always)]
    unsafe fn grid_many<B: Dense>(
        v: B::V,
        m: u64,
        last: usize,
        s: &mut *const u8,
        p: &mut *mut u8,
        room: &mut isize,
    ) -> bool {
        unsafe {
            let (w, d) = B::store_dense(v, m, *p);
            let nr = *room - w as isize;
            if nr >= 0 {
                *p = p.add(w);
                *room = nr;
                *s = s.add(B::N);
                return true;
            }
            // rapidyenc `LIKELIHOOD(0.3, col >= 0)` (src/encoder_sse_base.h:378).
            if $cold {
                std::hint::cold_path();
            }
            let r = (nr + w as isize) as usize;
            let eq = B::dense_eq(d);
            let (k, st) = (r - B::popcnt(eq & low64(r)), (((eq << 1) >> r) & 1) as usize);
            let e = EOL[*s.add(k) as usize] >> (8 * st);
            p.add(r).cast::<u32>().write_unaligned(e.to_le());
            let q = p.add(r + 3 + (e >> 27) as usize);
            let f = k + 1;
            if f == B::N {
                return grid_edge_n::<B>(q, last, s, p, room);
            }
            let fe = START[*s.add(f) as usize];
            let fb = (fe >> 16) as usize - 1;
            q.cast::<u16>().write_unaligned((fe as u16).to_le());
            let rs = s.add(f + 1);
            let rm = m >> (f + 1);
            let q = q.add(1 + fb);
            // The rest's width is its store's own (a table width, or none-or-one
            // by a compare), not a second popcount of the mask: the store
            // encodes `N` source bytes, the `f + 1` past the block unescaped.
            let rw = B::store(B::load(rs), rs, rm, q) - (f + 1);
            *p = q.add(rw);
            *room = (last - 1 - fb - rw) as isize;
            *s = s.add(B::N);
            true
        }
    }

    // Main loop for the frame (`scalar::encode_simd_frame`): encodes a prefix of
    // the input, updates `*column`, returns (consumed, written).
    //
    // SAFETY: the `RawEncodeFn` contract, `line_size >= 12`, and the CPU has the
    // features `B` uses.
    #[inline(always)]
    pub(super) unsafe fn main_loop<B: Dense>(
        ls: usize,
        column: &mut usize,
        src: *const u8,
        len: usize,
        dst: *mut u8,
    ) -> (usize, usize) {
        // While the next block's vector is loaded ahead, an iteration reads up
        // to `s + 2N + 1` (a restart one byte past the next block); the last
        // blocks load their own vector and read `READ_AHEAD` and the byte at
        // `s + N`. The scalar tail gets fewer than `need_b` bytes.
        let need_a = if 2 * B::N + 1 > B::READ_AHEAD { 2 * B::N + 1 } else { B::READ_AHEAD };
        let need_b = if B::N + 2 > B::READ_AHEAD { B::N + 2 } else { B::READ_AHEAD };
        if len < need_a {
            return (0, 0);
        }
        // SAFETY, whole body: every read is below `s + need <= src + len` for
        // the phase's `need` (each loop runs only while `s` is at most its
        // limit); output stays inside the `encode_max_len` budget: at most 2
        // bytes per consumed byte plus 2 per line, plus vector overrun of at
        // most 96 bytes while at least `need_b` (>= 34) input bytes, so 68 + 66
        // budget bytes, remain.
        unsafe {
            let end = src.add(len);
            let limit_a = end.sub(need_a);
            let limit_b = end.sub(need_b);
            let mut s = src;
            let mut p = dst;
            let mut col = *column;
            let last = ls - 1;
            enter(last, &mut col, &mut s, &mut p);
            if col == 0 {
                let c = *s;
                if first_only_bit(c) != 0 {
                    p.cast::<[u8; 2]>().write_unaligned(ESCAPED[c as usize]);
                    p = p.add(2);
                    s = s.add(1);
                    col = 2;
                }
            }
            let mut room = (last - col) as isize;
            // The next block's vector is loaded before this block's stores, so
            // the load never waits behind them. (Loading each block at the loop
            // head, as rapidyenc does at src/encoder_avx_base.h:129-130, cost
            // Zen 4 6 to 12% on every AVX2 and AVX cell.)
            let need_g = 2 * B::N + 64;
            let need_g2 = 3 * B::N + 64;
            if B::GRID2 && B::PIPELINE && ls >= 2 * B::N + 8 && len >= need_g2 {
                let limit_g2 = end.sub(need_g2);
                let mut va = B::load(s);
                let mut vb = B::load(s.add(B::N));
                while s <= limit_g2 {
                    let na = B::load(s.add(2 * B::N));
                    let nb = B::load(s.add(3 * B::N));
                    if step_grid2::<B>(va, vb, last, &mut s, &mut p, &mut room) {
                        va = na;
                        vb = nb;
                    } else {
                        va = B::load(s);
                        vb = B::load(s.add(B::N));
                    }
                }
            }
            if B::GRID && B::PIPELINE && ls >= 2 * B::N + 8 && len >= need_g {
                let limit_g = end.sub(need_g);
                let mut v = B::load(s);
                while s <= limit_g {
                    let vn = B::load(s.add(B::N));
                    v = if step_grid::<B>(v, last, &mut s, &mut p, &mut room) {
                        vn
                    } else {
                        if $cold {
                            std::hint::cold_path();
                        }
                        B::load(s)
                    };
                }
            }
            // A grid tier runs only the grid loop and the tail loop below: one
            // hot loop in the function keeps its column, cursor and source in
            // registers (with the pipelined `step` loop beside it, LLVM kept the
            // column on the stack).
            if B::PIPELINE && !B::GRID {
                let mut v = B::load(s);
                while s <= limit_a {
                    let vn = B::load(s.add(B::N));
                    v = if step::<B>(v, last, &mut s, &mut p, &mut room) {
                        vn
                    } else {
                        B::load(s)
                    };
                }
            }
            while s <= limit_b {
                step::<B>(B::load(s), last, &mut s, &mut p, &mut room);
            }
            *column = last - room as usize;
            (s as usize - src as usize, p as usize - dst as usize)
        }
    }
    };
}

mod cold {
    use super::*;
    grid_loop!(true);
}

mod plain {
    use super::*;
    grid_loop!(false);
}

// Main loop for the frame (`scalar::encode_simd_frame`): the grid loop
// copy `B::COLD` picks.
//
// SAFETY: as `cold::main_loop`.
#[inline(always)]
pub(super) unsafe fn main_loop<B: Dense>(
    ls: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
) -> (usize, usize) {
    unsafe {
        if B::COLD {
            cold::main_loop::<B>(ls, column, src, len, dst)
        } else {
            plain::main_loop::<B>(ls, column, src, len, dst)
        }
    }
}

#[cfg(all(test, target_arch = "x86_64"))]
mod dense_tests {
    use std::arch::x86_64::*;

    use super::super::ssse3::{dense_eq32, dense32};
    use super::out_eq32;

    // The dense store's width and '=' mask, from the PSHUFB controls, equal
    // the mask-derived ones the line end used before (`32 + popcnt`,
    // `out_eq32`), for every escape mask of each byte group and a fixed
    // pseudo-random sweep of whole masks. The line end's output pointer and
    // the rest's offsets follow from these two values only.
    #[test]
    fn dense_width_and_eq_match_the_mask() {
        if !is_x86_feature_detected!("ssse3") || !is_x86_feature_detected!("popcnt") {
            return;
        }
        let mut masks: Vec<u64> = Vec::new();
        for b in 0..256u64 {
            for g in 0..4 {
                masks.push(b << (8 * g));
                masks.push((b << (8 * g)) | (0xffff_ffff ^ (0xff << (8 * g))));
            }
        }
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            masks.push(x & 0xffff_ffff);
        }
        let src = [0u8; 32];
        let mut out = [0u8; 96];
        // SAFETY: SSSE3 and POPCNT were detected; the stores write at most
        // 64 bytes from `out`'s start, which holds 96.
        unsafe {
            let lo = _mm_loadu_si128(src.as_ptr().cast());
            let hi = _mm_loadu_si128(src.as_ptr().add(16).cast());
            for &m in &masks {
                let want_w = 32 + m.count_ones() as usize;
                let want_eq = out_eq32(m);
                for (w, d) in [
                    dense32::<false, true>(lo, hi, m, out.as_mut_ptr()),
                    dense32::<true, true>(lo, hi, m, out.as_mut_ptr()),
                    dense32::<true, false>(lo, hi, m, out.as_mut_ptr()),
                    dense32::<false, false>(lo, hi, m, out.as_mut_ptr()),
                ] {
                    assert_eq!(w, want_w, "width, mask {m:#x}");
                    assert_eq!(dense_eq32(d), want_eq, "eq, mask {m:#x}");
                }
            }
        }
    }

    // Each grid loop (SSSE3, AVX, AVX2) gives the scalar encoder's bytes and
    // column: seeded inputs of the fixtures' kinds (random, every byte
    // critical, half line-edge bytes), several line sizes, start columns and
    // lengths, so every block kind meets a line end at every offset.
    #[test]
    fn every_grid_loop_matches_scalar() {
        use super::super::{RawEncodeFn, avx, avx2, encode_max_len, scalar, ssse3};
        use std::arch::is_x86_feature_detected as has;
        let mut loops: Vec<(&str, RawEncodeFn)> = Vec::new();
        if has!("ssse3") {
            loops.push(("ssse3", ssse3::encode_raw));
        }
        if has!("avx") && has!("popcnt") && has!("sse4.1") && has!("ssse3") {
            loops.push(("avx", avx::encode_raw));
        }
        if has!("avx2") && has!("bmi1") && has!("bmi2") && has!("popcnt") && has!("lzcnt") {
            loops.push(("avx2", avx2::encode_raw));
        }
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        const CRIT: [u8; 4] = [214, 224, 227, 19];
        const EDGE: [u8; 3] = [223, 246, 4];
        for kind in 0..3 {
            for &len in &[0usize, 1, 31, 64, 65, 200, 1000, 4096, 9001] {
                let src: Vec<u8> = (0..len)
                    .map(|_| {
                        let r = next();
                        match kind {
                            0 => r as u8,
                            1 => CRIT[(r >> 8) as usize % 4],
                            _ if r & 1 == 0 => EDGE[(r >> 8) as usize % 3],
                            _ => (r >> 16) as u8,
                        }
                    })
                    .collect();
                for &ls in &[12usize, 64, 66, 70, 127, 128, 129, 200, 997] {
                    for &col0 in &[0usize, 1, ls / 2, ls - 1, ls, ls + 3] {
                        for is_end in [false, true] {
                            let cap = encode_max_len(len, ls);
                            let mut want = vec![0u8; cap];
                            let mut wcol = col0;
                            // SAFETY: `want` holds `encode_max_len` bytes; the
                            // domain is in range; scalar needs no feature.
                            let wn = unsafe {
                                scalar::encode_raw(ls, &mut wcol, src.as_ptr(), len, want.as_mut_ptr(), is_end)
                            };
                            for &(name, f) in &loops {
                                let mut got = vec![0u8; cap];
                                let mut gcol = col0;
                                // SAFETY: as above; the loop's features were
                                // detected when it was listed.
                                let gn = unsafe { f(ls, &mut gcol, src.as_ptr(), len, got.as_mut_ptr(), is_end) };
                                assert!(
                                    gn == wn && gcol == wcol && got[..gn] == want[..wn],
                                    "{name}: kind {kind} len {len} line {ls} col {col0} end {is_end}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
