// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// NEON encode tier (aarch64).
//
// `port_main_loop` is a faithful port of rapidyenc `do_encode_neon`
// (src/encoder_neon.cc:264-521 at upstream d223ecc, aarch64 branches only),
// with `encode_eol_handle_pre` (src/encoder_neon.cc:58-259) and the
// `shufLUT` / `expandLUT` tables (src/encoder_neon.cc:524-547). It is kept
// compiled as the parity reference for the tuned kernel.

use std::arch::aarch64::*;

use super::scalar;
use super::tables::ESCAPED;

// rapidyenc `shufLUT` (src/encoder_neon.cc:528-546): for an 8-bit escape
// mask, the `tbx` indices that expand 8 bytes, '=' (61, out of range so `tbx`
// keeps it) before each escaped one. Unused tail entries are `8+p+0x80`.
static SHUF_LUT: [[u8; 16]; 256] = build_shuf_lut();
// rapidyenc `expandLUT`: bit set at each output position holding an '='.
static EXPAND_LUT: [u16; 256] = build_expand_lut();

const fn build_shuf_lut() -> [[u8; 16]; 256] {
    let mut lut = [[0u8; 16]; 256];
    let mut i = 0;
    while i < 256 {
        let mut k = i;
        let mut p = 0;
        let mut j = 0;
        while j < 8 {
            if k & 1 != 0 {
                lut[i][j + p] = b'=';
                p += 1;
            }
            lut[i][j + p] = j as u8;
            k >>= 1;
            j += 1;
        }
        while p < 8 {
            lut[i][8 + p] = (8 + p + 0x80) as u8;
            p += 1;
        }
        i += 1;
    }
    lut
}

const fn build_expand_lut() -> [u16; 256] {
    let mut lut = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut k = i;
        let mut p = 0;
        let mut expand = 0u16;
        let mut j = 0;
        while j < 8 {
            if k & 1 != 0 {
                expand |= 1 << (j + p);
                p += 1;
            }
            k >>= 1;
            j += 1;
        }
        lut[i] = expand;
        i += 1;
    }
    lut
}

static BIT_WEIGHTS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
//                                  \0                    \n      \r
static CRIT_TBL: [u8; 16] = [255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 255, 0, 0];

// NEON encode: rapidyenc's frame around the main loop.
//
// # Safety
//
// The `RawEncodeFn` contract in `mod.rs`.
#[target_feature(enable = "neon")]
pub(super) unsafe fn encode_raw(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize {
    // SAFETY: forwarded contract.
    unsafe {
        scalar::encode_simd_frame(
            line_size,
            column,
            src,
            len,
            dst,
            is_end,
            |ls, col, s, n, d| main_loop(ls, col, s, n, d),
        )
    }
}

// The rapidyenc-port tier entry, for differential tests against the tuned
// kernel.
#[allow(dead_code)]
#[target_feature(enable = "neon")]
pub(super) unsafe fn encode_raw_port(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize {
    // SAFETY: forwarded contract.
    unsafe {
        scalar::encode_simd_frame(
            line_size,
            column,
            src,
            len,
            dst,
            is_end,
            |ls, col, s, n, d| port_main_loop(ls, col, s, n, d),
        )
    }
}

#[inline(always)]
unsafe fn ld(a: &[u8; 16]) -> uint8x16_t {
    unsafe { vld1q_u8(a.as_ptr()) }
}

// rapidyenc's `vpaddq_u8` / `vpadd_u8`, as the single `addp` clang emits.
// Rust's intrinsic is generic IR; with the disjoint-bit operands used here
// LLVM turns the add into an `or` and the backend then emits `uzp1`/`uzp2`/
// `orr` (three instructions) for each one. Verified in the bench binary.
#[inline(always)]
unsafe fn addp16(a: uint8x16_t, b: uint8x16_t) -> uint8x16_t {
    let out: uint8x16_t;
    unsafe {
        std::arch::asm!(
            "addp {o:v}.16b, {a:v}.16b, {b:v}.16b",
            o = lateout(vreg) out, a = in(vreg) a, b = in(vreg) b,
            options(pure, nomem, nostack, preserves_flags),
        );
    }
    out
}

#[inline(always)]
unsafe fn addp8(a: uint8x8_t, b: uint8x8_t) -> uint8x8_t {
    let out: uint8x8_t;
    unsafe {
        std::arch::asm!(
            "addp {o:v}.8b, {a:v}.8b, {b:v}.8b",
            o = lateout(vreg) out, a = in(vreg) a, b = in(vreg) b,
            options(pure, nomem, nostack, preserves_flags),
        );
    }
    out
}

#[inline(always)]
fn clz_n(v: u64) -> isize {
    v.leading_zeros() as isize
}

// rapidyenc `encode_eol_handle_pre`, aarch64 branch (src/encoder_neon.cc:58-259).
#[inline(always)]
unsafe fn eol_handle_pre(
    es: *const u8,
    i: &mut isize,
    p: &mut *mut u8,
    col: &mut isize,
    line_size_offset: isize,
) {
    unsafe {
        let o_data_a = vld1q_u8(es.offset(*i));
        let o_data_b = vld1q_u8(es.offset(*i + 16));
        let mut data_a = o_data_a;
        let mut data_b;
        const T0: [i8; 16] = [
            -42,
            -128,
            -128,
            -42,
            (b'\t' as i8).wrapping_sub(42),
            (b'\n' as i8).wrapping_sub(42),
            (b'\r' as i8).wrapping_sub(42),
            (b'\t' as i8).wrapping_sub(42),
            (b'\n' as i8).wrapping_sub(42),
            (b'\r' as i8).wrapping_sub(42),
            -128,
            -128,
            -42,
            -128,
            -128,
            -128,
        ];
        const T1: [i8; 16] = [
            (b' ' as i8).wrapping_sub(42),
            (b'\n' as i8).wrapping_sub(42),
            (b'\r' as i8).wrapping_sub(42),
            (b' ' as i8).wrapping_sub(42),
            -128,
            -128,
            -128,
            -128,
            -128,
            -128,
            (b'.' as i8).wrapping_sub(42),
            -128,
            -128,
            -128,
            (b'=' as i8).wrapping_sub(42),
            -128,
        ];
        const HADD: [i8; 16] = [
            42, 48, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
        ];
        let tbl = int8x16x2_t(vld1q_s8(T0.as_ptr()), vld1q_s8(T1.as_ptr()));
        let idx = vreinterpretq_u8_s8(vhaddq_s8(
            vreinterpretq_s8_u8(data_a),
            vld1q_s8(HADD.as_ptr()),
        ));
        let mut cmp_a = vreinterpretq_u8_s8(vqtbx2q_s8(
            vdupq_n_s8((b'=' as i8).wrapping_sub(42)),
            tbl,
            idx,
        ));
        cmp_a = vceqq_u8(cmp_a, data_a);

        data_b = vaddq_u8(o_data_b, vdupq_n_u8(42));
        let cmp_b = vqtbx1q_u8(
            vceqq_u8(o_data_b, vdupq_n_u8(b'='.wrapping_sub(42))),
            ld(&CRIT_TBL),
            data_b,
        );
        data_a = vaddq_u8(data_a, vbslq_u8(cmp_a, vdupq_n_u8(64 + 42), vdupq_n_u8(42)));
        data_b = vorrq_u8(data_b, vandq_u8(cmp_b, vdupq_n_u8(64)));

        let cmp_a_masked = vandq_u8(cmp_a, ld(&BIT_WEIGHTS));
        let cmp_b_masked = vandq_u8(cmp_b, ld(&BIT_WEIGHTS));
        let mut cmp_merge = addp16(cmp_a_masked, cmp_b_masked);
        cmp_merge = addp16(cmp_merge, cmp_merge);
        let mut mask = vgetq_lane_u64::<0>(vreinterpretq_u64_u8(cmp_merge));

        // write out first char + newline
        let mut first_char = vgetq_lane_u8::<0>(data_a) as u32;
        if mask & 1 != 0 {
            first_char <<= 8;
            first_char |= 0x0a0d_003d;
            p.cast::<u32>().write_unaligned(first_char);
            *p = p.add(4);
            mask ^= 1;
            const FIRST: [u8; 16] = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
            cmp_merge = vbicq_u8(cmp_merge, ld(&FIRST));
        } else {
            first_char |= 0x0a0d00;
            p.cast::<u32>().write_unaligned(first_char);
            *p = p.add(3);
        }

        if mask & mask.wrapping_sub(1) != 0 {
            mask |= mask >> 8;
            let cmp_packed = addp8(vget_low_u8(cmp_merge), vget_low_u8(cmp_merge));
            let m1 = (mask & 0xff) as usize;
            let m2 = ((mask >> 16) & 0xff) as usize;
            let m3 = ((mask >> 32) & 0xff) as usize;
            let m4 = ((mask >> 48) & 0xff) as usize;
            let shuf1 = ld(&SHUF_LUT[m1]);
            let shuf2 = ld(&SHUF_LUT[m2]);
            let shuf3 = ld(&SHUF_LUT[m3]);
            let shuf4 = ld(&SHUF_LUT[m4]);
            let mut data1a = vqtbx1q_u8(shuf1, data_a, shuf1);
            let data2a = vqtbx1q_u8(shuf2, vextq_u8::<8>(data_a, data_a), shuf2);
            let data1b = vqtbx1q_u8(shuf3, data_b, shuf3);
            let data2b = vqtbx1q_u8(shuf4, vextq_u8::<8>(data_b, data_b), shuf4);
            data1a = vextq_u8::<1>(data1a, data1a);

            let mut counts = vget_lane_u32::<0>(vreinterpret_u32_u8(vcnt_u8(cmp_packed)));
            counts = counts.wrapping_add(0x0808_0807);
            let l1 = (counts & 0xff) as usize;
            let l2 = ((counts >> 8) & 0xff) as usize;
            let l3 = ((counts >> 16) & 0xff) as usize;
            let l4 = ((counts >> 24) & 0xff) as usize;
            let total = (counts.wrapping_mul(0x0101_0101) >> 24) as isize;

            vst1q_u8(*p, data1a);
            *p = p.add(l1);
            vst1q_u8(*p, data2a);
            *p = p.add(l2);
            vst1q_u8(*p, data1b);
            *p = p.add(l3);
            vst1q_u8(*p, data2b);
            *p = p.add(l4);
            *col = total + 1 + line_size_offset - 32;
        } else {
            let bit_index = clz_n(mask);
            let v_clz = vdupq_n_u8((bit_index & !64) as u8);
            const BA: [u8; 16] = [
                63, 62, 61, 60, 51, 50, 49, 48, 47, 46, 45, 44, 35, 34, 33, 32,
            ];
            const BB: [u8; 16] = [31, 30, 29, 28, 19, 18, 17, 16, 15, 14, 13, 12, 3, 2, 1, 0];
            let blend_a = vcgtq_u8(ld(&BA), v_clz);
            let blend_b = vcgtq_u8(ld(&BB), v_clz);
            let mut a_sh = vbslq_u8(cmp_a, vdupq_n_u8(b'='), data_a);
            let mut b_sh = vbslq_u8(cmp_b, vdupq_n_u8(b'='), data_b);
            a_sh = vextq_u8::<1>(a_sh, b_sh);
            b_sh = vextq_u8::<1>(b_sh, b_sh);
            data_a = vbslq_u8(blend_a, a_sh, data_a);
            data_b = vbslq_u8(blend_b, b_sh, data_b);
            vst1q_u8(*p, data_a);
            vst1q_u8(p.add(16), data_b);
            let nz = (mask != 0) as usize;
            *p = p.add(31 + nz);
            *col = line_size_offset + nz as isize;
        }
        *i += 32;
    }
}

// rapidyenc `do_encode_neon` (src/encoder_neon.cc:264-521), aarch64.
// Returns (consumed, written).
#[inline(always)]
unsafe fn port_main_loop(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
) -> (usize, usize) {
    const INPUT_OFFSET: isize = 16 * 4 - 1;
    if len as isize <= INPUT_OFFSET || line_size < 64 {
        return (0, 0);
    }
    let ls = line_size as isize;
    // SAFETY, whole body: rapidyenc's own bounds. The loop runs while at
    // least 64 input bytes remain past `i`; every load reads at most 32
    // bytes past a position the loop has not passed, and reverts only move
    // back. Writes advance at most 2 bytes per input byte plus line breaks,
    // with at most 32 bytes stored past the cursor, inside `encode_max_len`.
    unsafe {
        let mut p = dst;
        let mut i: isize = -(len as isize);
        let line_size_offset: isize = -ls + 32;
        let mut col: isize = *column as isize - ls + 1;

        i += INPUT_OFFSET;
        let es = src.add(len).offset(-INPUT_OFFSET);

        if col == -ls + 1 {
            let c = *es.offset(i);
            i += 1;
            let e = ESCAPED[c as usize];
            if e[0] != 0 {
                p.cast::<[u8; 2]>().write_unaligned(e);
                p = p.add(2);
                col += 2;
            } else {
                *p = c.wrapping_add(42);
                p = p.add(1);
                col += 1;
            }
        }
        if col >= 0 {
            if col == 0 {
                eol_handle_pre(es, &mut i, &mut p, &mut col, line_size_offset);
            } else {
                let c = *es.offset(i);
                i += 1;
                let e = ESCAPED[c as usize];
                if e[0] != 0 {
                    let v = u32::from_le_bytes([b'\r', b'\n', e[0], e[1]]);
                    p.cast::<u32>().write_unaligned(v);
                    p = p.add(4);
                    col = 2 - ls + 1;
                } else {
                    let v = u32::from_le_bytes([b'\r', b'\n', c.wrapping_add(42), 0]);
                    p.cast::<u32>().write_unaligned(v);
                    p = p.add(3);
                    col = 2 - ls;
                }
            }
        }
        let weights = ld(&BIT_WEIGHTS);
        let crit = ld(&CRIT_TBL);
        while i < 0 {
            let mut data_a = vld1q_u8(es.offset(i));
            let mut data_b = vld1q_u8(es.offset(i + 16));
            i += 32;
            let cmp_eq_a = vceqq_u8(data_a, vdupq_n_u8(b'='.wrapping_sub(42)));
            let cmp_eq_b = vceqq_u8(data_b, vdupq_n_u8(b'='.wrapping_sub(42)));
            data_a = vaddq_u8(data_a, vdupq_n_u8(42));
            data_b = vaddq_u8(data_b, vdupq_n_u8(42));
            let cmp_a = vqtbx1q_u8(cmp_eq_a, crit, data_a);
            let cmp_b = vqtbx1q_u8(cmp_eq_b, crit, data_b);
            data_a = vorrq_u8(data_a, vandq_u8(cmp_a, vdupq_n_u8(64)));
            data_b = vorrq_u8(data_b, vandq_u8(cmp_b, vdupq_n_u8(64)));

            let cmp_a_masked = vandq_u8(cmp_a, weights);
            let cmp_b_masked = vandq_u8(cmp_b, weights);
            let mut cmp_merge = addp16(cmp_a_masked, cmp_b_masked);
            cmp_merge = addp16(cmp_merge, cmp_merge);
            let mut mask = vgetq_lane_u64::<0>(vreinterpretq_u64_u8(cmp_merge));
            if mask & mask.wrapping_sub(1) != 0 {
                mask |= mask >> 8;
                let cmp_packed = addp8(vget_low_u8(cmp_merge), vget_low_u8(cmp_merge));
                let m1 = (mask & 0xff) as usize;
                let m2 = ((mask >> 16) & 0xff) as usize;
                let m3 = ((mask >> 32) & 0xff) as usize;
                let m4 = ((mask >> 48) & 0xff) as usize;
                let shuf1 = ld(&SHUF_LUT[m1]);
                let shuf2 = ld(&SHUF_LUT[m2]);
                let shuf3 = ld(&SHUF_LUT[m3]);
                let shuf4 = ld(&SHUF_LUT[m4]);
                let data1a = vqtbx1q_u8(shuf1, data_a, shuf1);
                let data2a = vqtbx1q_u8(shuf2, vextq_u8::<8>(data_a, data_a), shuf2);
                let data1b = vqtbx1q_u8(shuf3, data_b, shuf3);
                let data2b = vqtbx1q_u8(shuf4, vextq_u8::<8>(data_b, data_b), shuf4);

                let mut counts = vget_lane_u32::<0>(vreinterpret_u32_u8(vcnt_u8(cmp_packed)));
                counts = counts.wrapping_add(0x0808_0808);
                let l1 = (counts & 0xff) as isize;
                let l2 = ((counts >> 8) & 0xff) as isize;
                let l3 = ((counts >> 16) & 0xff) as isize;
                let l4 = ((counts >> 24) & 0xff) as isize;
                let total = (counts.wrapping_mul(0x0101_0101) >> 24) as isize;

                vst1q_u8(p, data1a);
                p = p.offset(l1);
                vst1q_u8(p, data2a);
                p = p.offset(l2);
                vst1q_u8(p, data1b);
                p = p.offset(l3);
                vst1q_u8(p, data2b);
                p = p.offset(l4);
                col += total;

                if col >= 0 {
                    // we overflowed - find correct position to revert back to
                    let mut revert = col;
                    let len2nd_half = l3 + l4;
                    let shift_amt = len2nd_half - col - 1;
                    let eq_mask_half: u32 = if shift_amt < 0 {
                        let v = ((EXPAND_LUT[m2] as u32).wrapping_shl(l1 as u32))
                            | EXPAND_LUT[m1] as u32;
                        i += len2nd_half - 16;
                        v.wrapping_shr((total - col - 1) as u32)
                    } else {
                        let v = ((EXPAND_LUT[m4] as u32).wrapping_shl(l3 as u32))
                            | EXPAND_LUT[m3] as u32;
                        v.wrapping_shr(shift_amt as u32)
                    };
                    revert += (eq_mask_half & 1) as isize;
                    i += eq_mask_half.count_ones() as isize;
                    p = p.offset(-revert);
                    i -= revert;
                    eol_handle_pre(es, &mut i, &mut p, &mut col, line_size_offset);
                }
            } else {
                let mut bit_index = clz_n(mask);
                let v_clz = vdupq_n_u8((bit_index & !64) as u8);
                const BA: [u8; 16] = [
                    63, 62, 61, 60, 51, 50, 49, 48, 47, 46, 45, 44, 35, 34, 33, 32,
                ];
                const BB: [u8; 16] = [31, 30, 29, 28, 19, 18, 17, 16, 15, 14, 13, 12, 3, 2, 1, 0];
                let blend_a = vcgeq_u8(ld(&BA), v_clz);
                let blend_b = vcgeq_u8(ld(&BB), v_clz);
                let a_sh = vextq_u8::<15>(data_a, data_a);
                let b_sh = vextq_u8::<15>(data_a, data_b);
                let mut out_a = vbslq_u8(cmp_a, vdupq_n_u8(b'='), data_a);
                let mut out_b = vbslq_u8(cmp_b, vdupq_n_u8(b'='), data_b);
                out_a = vbslq_u8(blend_a, out_a, a_sh);
                out_b = vbslq_u8(blend_b, out_b, b_sh);
                vst1q_u8(p, out_a);
                vst1q_u8(p.add(16), out_b);
                p = p.add(32);
                *p = vgetq_lane_u8::<15>(data_b);
                let nz = (mask != 0) as isize;
                p = p.offset(nz);
                col += nz + 32;

                if col >= 0 {
                    bit_index -= ((bit_index + 4) >> 4) << 3;
                    bit_index += 1;
                    if col == bit_index {
                        // this is an escape character, so line will need to overflow
                        p = p.sub(1);
                    } else {
                        i += (col > bit_index) as isize;
                    }
                    p = p.offset(-col);
                    i -= col;
                    eol_handle_pre(es, &mut i, &mut p, &mut col, line_size_offset);
                }
            }
        }

        *column = (col + ls - 1) as usize;
        let remaining = -(i - INPUT_OFFSET);
        (len - remaining as usize, p.offset_from(dst) as usize)
    }
}

// ---- tuned kernel -----------------------------------------------------------
//
// Same output as the port (both are rapidyenc's), different call structure:
//
// * Input is walked on a fixed 32-byte grid. rapidyenc rewinds the input to
//   the start of each new line (`revert`, src/encoder_neon.cc:445-470 and
//   497-514), so every load of the next line waits on the arithmetic that
//   found where the line ended. Here a line break never moves the grid: the
//   bytes after the break in the same block (the remainder) are written from
//   the block's own blend output with two table lookups, or, when the block
//   has two or more escapes, re-encoded from a second load. The next block
//   is still 32 bytes on. No block load address depends on data.
// * The break block is planned exactly (where the last byte of the line
//   falls, given the escapes before it) instead of encoded, overflowed and
//   undone. Nothing on the line's column and cursor chain waits on a
//   popcount when the block has at most one escape.
// * Blocks with zero or one escape take rapidyenc's branch-free blend
//   (src/encoder_neon.cc:472-495); two or more take its table expansion
//   (src/encoder_neon.cc:367-443). There is no separate no-escape branch:
//   on random data it goes either way about 40% of the time, and on
//   Neoverse V2 its misses cost more than the blend.

// End-of-line encoding of a source byte (rapidyenc escapes TAB, SPACE and
// the always-critical bytes there, not '.'): low 16 bits the bytes in output
// order, bits 16.. the length.
static END_ENC: [u32; 256] = build_edge_enc(false);
// Start-of-line encoding: as `END_ENC`, '.' escaped too.
static START_ENC: [u32; 256] = build_edge_enc(true);

const fn build_edge_enc(start: bool) -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut n = 0;
    while n < 256 {
        let b = n as u8;
        let e = ESCAPED[n];
        let esc = e[0] != 0 && (start || b != super::tables::DOT);
        t[n] = if esc {
            (e[0] as u32) | ((e[1] as u32) << 8) | (2 << 16)
        } else {
            (b.wrapping_add(42) as u32) | (1 << 16)
        };
        n += 1;
    }
    t
}

// Constants of the hot loop, built once per call.
#[derive(Clone, Copy)]
struct K {
    crit: uint8x16_t,
    weights: uint8x16_t,
    iota: uint8x16_t,
}

// One 16-byte vector classified for the middle of a line: (`data + 42`; the
// escape mask). `esc64` adds the escaped bytes' +64 where a path needs it.
#[inline(always)]
unsafe fn classify(d: uint8x16_t, k: &K) -> (uint8x16_t, uint8x16_t) {
    unsafe {
        let eq = vceqq_u8(d, vdupq_n_u8(b'='.wrapping_sub(42)));
        let d42 = vaddq_u8(d, vdupq_n_u8(42));
        let esc = vqtbx1q_u8(eq, k.crit, d42);
        (d42, esc)
    }
}

// `d42 + 64` on the escaped bytes: `(0xff + 2) >> 2 == 64`, one `ursra`.
#[inline(always)]
unsafe fn esc64(d42: uint8x16_t, esc: uint8x16_t) -> uint8x16_t {
    unsafe { vrsraq_n_u8::<2>(d42, esc) }
}

// rapidyenc's 64-bit escape mask of a 32-byte block: element `e` of A at bit
// `8*(e/4) + e%8`, of B 32 above.
#[inline(always)]
unsafe fn mask64(esc_a: uint8x16_t, esc_b: uint8x16_t, k: &K) -> (u64, uint8x16_t) {
    unsafe {
        let merge = addp16(vandq_u8(esc_a, k.weights), vandq_u8(esc_b, k.weights));
        let merge = addp16(merge, merge);
        (vgetq_lane_u64::<0>(vreinterpretq_u64_u8(merge)), merge)
    }
}

// That mask as one bit per byte (bit j = byte j of the block).
#[inline(always)]
fn compress(mask: u64) -> u32 {
    let m = mask | (mask >> 8);
    let x = m & 0x00ff_00ff_00ff_00ff;
    let x = (x | (x >> 8)) & 0x0000_ffff_0000_ffff;
    (x | (x >> 16)) as u32
}

// Encode a 32-byte block as middle bytes at `p`; returns its width. Stores
// up to 33 bytes (blend) or 64 (table) at `p`; only the first `width` count.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
unsafe fn expand32(
    p: *mut u8,
    de_a: uint8x16_t,
    de_b: uint8x16_t,
    esc_a: uint8x16_t,
    esc_b: uint8x16_t,
    mask: u64,
    merge: uint8x16_t,
) -> usize {
    unsafe {
        if mask & mask.wrapping_sub(1) != 0 {
            let m = mask | (mask >> 8);
            let packed = addp8(vget_low_u8(merge), vget_low_u8(merge));
            let s1 = ld(&SHUF_LUT[(m & 0xff) as usize]);
            let s2 = ld(&SHUF_LUT[((m >> 16) & 0xff) as usize]);
            let s3 = ld(&SHUF_LUT[((m >> 32) & 0xff) as usize]);
            let s4 = ld(&SHUF_LUT[((m >> 48) & 0xff) as usize]);
            let o1 = vqtbx1q_u8(s1, de_a, s1);
            let o2 = vqtbx1q_u8(s2, vextq_u8::<8>(de_a, de_a), s2);
            let o3 = vqtbx1q_u8(s3, de_b, s3);
            let o4 = vqtbx1q_u8(s4, vextq_u8::<8>(de_b, de_b), s4);
            let counts =
                vget_lane_u32::<0>(vreinterpret_u32_u8(vcnt_u8(packed))).wrapping_add(0x0808_0808);
            let l1 = (counts & 0xff) as usize;
            let l2 = ((counts >> 8) & 0xff) as usize;
            let l3 = ((counts >> 16) & 0xff) as usize;
            let total = (counts.wrapping_mul(0x0101_0101) >> 24) as usize;
            vst1q_u8(p, o1);
            let q = p.add(l1);
            vst1q_u8(q, o2);
            let q = q.add(l2);
            vst1q_u8(q, o3);
            vst1q_u8(q.add(l3), o4);
            total
        } else {
            let (out_a, out_b) = blend32(de_a, de_b, esc_a, esc_b, mask);
            // Byte 32 is `de_b[15]`: store `de_b` at 17 first and let the
            // blend overwrite 17..32, one store instead of a lane move + store.
            vst1q_u8(p.add(17), de_b);
            vst1q_u8(p, out_a);
            vst1q_u8(p.add(16), out_b);
            32 + (mask != 0) as usize
        }
    }
}

// rapidyenc's blend selectors (`vcgeq_u8(BA/BB, clz & ~64)`,
// src/encoder_neon.cc:476-481) for every `clz` of the mask, 0..=64: one
// load instead of a dup and two compares.
static BLEND_MASK: [[[u8; 16]; 2]; 65] = build_blend_mask();

const fn build_blend_mask() -> [[[u8; 16]; 2]; 65] {
    const BA: [u8; 16] = [
        63, 62, 61, 60, 51, 50, 49, 48, 47, 46, 45, 44, 35, 34, 33, 32,
    ];
    const BB: [u8; 16] = [31, 30, 29, 28, 19, 18, 17, 16, 15, 14, 13, 12, 3, 2, 1, 0];
    let mut t = [[[0u8; 16]; 2]; 65];
    let mut z = 0;
    while z <= 64 {
        let v = (z & !64) as u8;
        let mut j = 0;
        while j < 16 {
            t[z][0][j] = if BA[j] >= v { 0xff } else { 0 };
            t[z][1][j] = if BB[j] >= v { 0xff } else { 0 };
            j += 1;
        }
        z += 1;
    }
    t
}

// rapidyenc's branch-free blend for a block with at most one escape
// (src/encoder_neon.cc:472-495): output bytes 0..32; byte 32 is `de_b[15]`.
#[inline(always)]
unsafe fn blend32(
    de_a: uint8x16_t,
    de_b: uint8x16_t,
    esc_a: uint8x16_t,
    esc_b: uint8x16_t,
    mask: u64,
) -> (uint8x16_t, uint8x16_t) {
    unsafe {
        let t = &BLEND_MASK[mask.leading_zeros() as usize];
        let blend_a = ld(&t[0]);
        let blend_b = ld(&t[1]);
        let a_sh = vextq_u8::<15>(de_a, de_a);
        let b_sh = vextq_u8::<15>(de_a, de_b);
        // Shift first, then the '=': the escape is the block's only one and
        // sits inside the kept part, so the order does not matter, and the
        // escape masks stay intact (no register copies before `bsl`).
        let t_a = vbslq_u8(blend_a, de_a, a_sh);
        let t_b = vbslq_u8(blend_b, de_b, b_sh);
        (
            vbslq_u8(esc_a, vdupq_n_u8(b'='), t_a),
            vbslq_u8(esc_b, vdupq_n_u8(b'='), t_b),
        )
    }
}

// One step of rapidyenc's scalar tail (src/encoder_common.h:50-87) at a
// state that is not column 0: consumes one byte, or two at a line end.
#[inline(always)]
unsafe fn scalar_step(ls: isize, c: &mut isize, src: *const u8, i: &mut usize, p: &mut *mut u8) {
    unsafe {
        let mut b = *src.add(*i);
        *i += 1;
        if *c < ls - 1 {
            let e = super::tables::ESCAPE[b as usize];
            if e == 0 {
                p.cast::<[u8; 2]>()
                    .write_unaligned([b'=', b.wrapping_add(42 + 64)]);
                *p = p.add(2);
                *c += 2;
            } else {
                **p = e;
                *p = p.add(1);
                *c += 1;
            }
        } else {
            if *c < ls {
                let e = END_ENC[b as usize];
                p.cast::<u16>().write_unaligned(e as u16);
                *p = p.add((e >> 16) as usize);
                b = *src.add(*i);
                *i += 1;
            }
            let e = START_ENC[b as usize];
            p.cast::<u32>()
                .write_unaligned(0x0a0d | ((e & 0xffff) << 16));
            *p = p.add(2 + (e >> 16) as usize);
            *c = (e >> 16) as isize;
        }
    }
}

// The tuned main loop. Returns (consumed, written).
#[inline(always)]
unsafe fn main_loop(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
) -> (usize, usize) {
    // A line of at least 64 holds the 30-byte remainder of a break block
    // (at most 62 columns with its first byte) without a second break.
    if line_size < 64 || len < 64 + 3 {
        return (0, 0);
    }
    let ls = line_size as isize;
    let limit = ls - 1;
    // SAFETY, whole body: a block at `s` runs only while 64 input bytes
    // remain at `s`; it reads `s..s+32`, a remainder `s+F+1..s+F+33` with
    // `F <= 31`, the scalar fallback at most `s+33`. Output: each step stores at most about 134 bytes
    // past its cursor, and the 64 input bytes still left at `s` have a
    // budget of at least 2 * 64 + 64 = 192 bytes.
    unsafe {
        let k = K {
            crit: ld(&CRIT_TBL),
            weights: ld(&BIT_WEIGHTS),
            iota: ld(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
        };
        let mut p = dst;
        let mut a = 0usize;
        let mut c = *column as isize;
        // Entry: settle into the middle of a line.
        if c == 0 {
            let e = START_ENC[*src as usize];
            p.cast::<u16>().write_unaligned(e as u16);
            p = p.add((e >> 16) as usize);
            c = (e >> 16) as isize;
            a = 1;
        }
        if c >= limit {
            scalar_step(ls, &mut c, src, &mut a, &mut p);
        }

        // Running source pointer, the decoder's loop shape: no base + index
        // adds per block. `last` is the last block start with 64 bytes left.
        let last = src.add(len - 64);
        let mut s = src.add(a);
        while s <= last {
            let (de_a, esc_a) = classify(vld1q_u8(s), &k);
            let (de_b, esc_b) = classify(vld1q_u8(s.add(16)), &k);
            let (mask, merge) = mask64(esc_a, esc_b, &k);
            let de_a = esc64(de_a, esc_a);
            let de_b = esc64(de_b, esc_b);
            // The block's width, computed once for the line test and the
            // cursor advance.
            let w = 32
                + if mask & mask.wrapping_sub(1) != 0 {
                    mask.count_ones() as isize
                } else {
                    (mask != 0) as isize
                };
            if c + w <= limit {
                expand32(p, de_a, de_b, esc_a, esc_b, mask, merge);
                p = p.offset(w);
                c += w;
                s = s.add(32);
                continue;
            }

            // The line ends in this block.
            let r = limit - c;
            // At most one escape: `j` is its byte (32 when none), read off
            // the mask's lowest set bit (byte `4g + i` of the block sits at
            // bit `8g + i` or `8g + 4 + i`).
            let sparse = mask & mask.wrapping_sub(1) == 0;
            let tz = mask.trailing_zeros();
            let j = ((tz >> 3) << 2) | (tz & 3);
            if !(0..=63).contains(&r) {
                let mut i = s.offset_from(src) as usize;
                let end = i + 32;
                while i < end {
                    scalar_step(ls, &mut c, src, &mut i, &mut p);
                }
                s = src.add(i);
                continue;
            }
            // Bit `o` of `eo` is set when output column `c + o` of the block
            // holds an '='. A block that does not fit has `w(32) > r`, so the
            // line's middle ends inside it: K middle bytes, the K-th ending
            // at or past column `limit`. K counts the bytes that start before
            // column `limit`; a byte starts at every output column but the
            // one after an '='.
            // `below`: columns before `limit - 1`; `top`: column `limit - 1`.
            let (kk, has_last) = if sparse {
                // At most one '=', at column `j`: compares, no popcount on the
                // line's latency chain. (`j < r - 1`: in `below`; `j == r - 1`:
                // the `top` column.)
                let r = r as u32;
                (r - (j + 2 <= r) as u32, j + 1 != r)
            } else {
                let m32 = compress(mask);
                let below = ((1u64 << r) - 1) >> 1;
                let top = (1u64 << r) >> 1;
                let counts =
                    vget_lane_u32::<0>(vreinterpret_u32_u8(vcnt_u8(vcreate_u8(m32 as u64))))
                        .wrapping_add(0x0808_0808);
                let l1 = counts & 0xff;
                let l12 = l1 + ((counts >> 8) & 0xff);
                let l123 = l12 + ((counts >> 16) & 0xff);
                let eo = EXPAND_LUT[(m32 & 0xff) as usize] as u64
                    | (EXPAND_LUT[((m32 >> 8) & 0xff) as usize] as u64) << l1
                    | (EXPAND_LUT[((m32 >> 16) & 0xff) as usize] as u64) << l12
                    | (EXPAND_LUT[(m32 >> 24) as usize] as u64) << l123;
                (r as u32 - (eo & below).count_ones(), eo & top == 0)
            };
            let wk = r as u32 + !has_last as u32;
            let f = kk + has_last as u32;
            // Last byte, CRLF, first byte of the next line, at `p + wk`;
            // returns the cursor after them and the first byte's width. Each
            // arm below runs it, so neither carries the other's registers.
            let edge = |p: *mut u8| -> (*mut u8, u64) {
                let q = p.add(wk as usize);
                let (lw, ll) = if has_last {
                    let e = END_ENC[*s.add(kk as usize) as usize];
                    ((e & 0xffff) as u64, (e >> 16) as u64)
                } else {
                    (0, 0)
                };
                let fe = START_ENC[*s.add(f as usize) as usize];
                let fl = (fe >> 16) as u64;
                let word = lw | (0x0a0d << (8 * ll)) | (((fe & 0xffff) as u64) << (8 * (ll + 2)));
                q.cast::<u64>().write_unaligned(word);
                (q.add((ll + 2 + fl) as usize), fl)
            };
            if f > 31 {
                if !sparse {
                    // Escape-dense data keeps the grid: on the M5, moving it
                    // costs all-critical about 4%.
                    let mut i = s.offset_from(src) as usize;
                    let end = i + 32;
                    while i < end {
                        scalar_step(ls, &mut c, src, &mut i, &mut p);
                    }
                    s = src.add(i);
                    continue;
                }
                // The next line starts past the block (about 3% of line
                // ends on random data): the whole block is this line's, then
                // the edge, and the grid moves on past the next line's first
                // byte (`F == 32`, inside the 64 bytes left). A branch, so
                // the common path's load address stays off the data chain.
                expand32(p, de_a, de_b, esc_a, esc_b, mask, merge);
                let (q, fl) = edge(p);
                p = q;
                c = fl as isize;
                s = s.add(f as usize + 1);
                continue;
            }
            let (q, fl, rem_w) = if sparse {
                let (out_a, out_b) = blend32(de_a, de_b, esc_a, esc_b, mask);
                vst1q_u8(p, out_a);
                vst1q_u8(p.add(16), out_b);
                let (q, fl) = edge(p);
                // The rest of the block is the block's own blend output from
                // column `w(F+1)` on: two table lookups on the registers, no
                // second load and encode. Byte 32 of the output is `de_b[15]`.
                let w1 = f + 1 + (j <= f) as u32;
                let x = vextq_u8::<15>(de_b, de_b);
                let idx = vaddq_u8(vdupq_n_u8(w1 as u8), k.iota);
                let r1 = vqtbl3q_u8(uint8x16x3_t(out_a, out_b, x), idx);
                let r2 = vqtbl2q_u8(uint8x16x2_t(out_b, x), idx);
                vst1q_u8(q, r1);
                vst1q_u8(q.add(16), r2);
                (q, fl, 31 - f + (j > f && j != 32) as u32)
            } else {
                expand32(p, de_a, de_b, esc_a, esc_b, mask, merge);
                let (q, fl) = edge(p);
                // The rest of the block: its encoded bytes and escape mask
                // shifted down by `F + 1` in registers, then expanded. Lanes
                // past the block read as 0 (no escape); their output is past
                // `rem_w` and is overwritten.
                let idx = vaddq_u8(vdupq_n_u8((f + 1) as u8), k.iota);
                let idx_b = vaddq_u8(idx, vdupq_n_u8(16));
                let de = uint8x16x2_t(de_a, de_b);
                let es = uint8x16x2_t(esc_a, esc_b);
                let (re_a, re_b) = (vqtbl2q_u8(de, idx), vqtbl2q_u8(de, idx_b));
                let (resc_a, resc_b) = (vqtbl2q_u8(es, idx), vqtbl2q_u8(es, idx_b));
                let (rmask, rmerge) = mask64(resc_a, resc_b, &k);
                expand32(q, re_a, re_b, resc_a, resc_b, rmask, rmerge);
                (
                    q,
                    fl,
                    31 - f + (compress(mask) as u64 >> (f + 1)).count_ones(),
                )
            };
            p = q.add(rem_w as usize);
            c = fl as isize + rem_w as isize;
            s = s.add(32);
        }
        *column = c as usize;
        (s.offset_from(src) as usize, p.offset_from(dst) as usize)
    }
}
