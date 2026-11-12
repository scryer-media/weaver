// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// AVX tier: the SSSE3 block (`ssse3::Sse`) with hardware POPCNT, compiled
// with AVX so every 128-bit op is VEX-encoded. rapidyenc's AVX kernel is the
// same: `do_encode_sse<ISA_LEVEL_SSE4_POPCNT>` built with -mavx -mpopcnt
// (src/encoder_avx.cc, CMakeLists.txt:189); it uses no 256-bit ops.

use super::scalar::encode_simd_frame;
use super::ssse3::Sse;
use super::x86_common::main_loop;

// SAFETY: the `RawEncodeFn` contract; the CPU has AVX, POPCNT, SSE4.1, SSSE3.
#[target_feature(enable = "avx,popcnt,sse4.1,ssse3")]
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
            |ls, col, s, n, d| main_loop::<Sse<true>>(ls, col, s, n, d),
        )
    }
}
