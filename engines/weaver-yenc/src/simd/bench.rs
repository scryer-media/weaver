// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Decode one buffer with a chosen tier kernel, for decode-bench. Compiled
// only with `--cfg weaver_yenc_bench` (decode-bench/.cargo/config.toml), so
// it is no part of the crate's API or of any production build.
//
// Each call is what `decode_rapidyenc` (`search_end` false) or
// `decode_rapidyenc_incremental` from the CRLF state (`search_end` true)
// runs, with the tier fixed instead of dispatched.

use super::*;

pub use super::SelectedDecoderTier as Tier;

pub const TIERS: [Tier; 8] = [
    Tier::Scalar,
    Tier::Neon,
    Tier::Sse2,
    Tier::Ssse3,
    Tier::Sse41,
    Tier::Avx,
    Tier::Avx2,
    Tier::Avx512Vbmi2,
];

// Whether this CPU has every feature the tier's kernel is compiled with.
pub fn runs(tier: Tier) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::is_x86_feature_detected as has;
        match tier {
            Tier::Scalar => true,
            Tier::Neon => false,
            Tier::Sse2 => has!("sse2"),
            Tier::Ssse3 => has!("ssse3"),
            Tier::Sse41 => has!("sse4.1") && has!("ssse3"),
            Tier::Avx => has!("avx") && has!("popcnt") && has!("sse4.1") && has!("ssse3"),
            Tier::Avx2 => {
                has!("avx2") && has!("bmi1") && has!("bmi2") && has!("popcnt") && has!("lzcnt")
            }
            Tier::Avx512Vbmi2 => vbmi2_tier_available(),
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        matches!(tier, Tier::Scalar | Tier::Neon)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        tier == Tier::Scalar
    }
}

// One decode call, resolved per tier before timing: `decode(src, dst,
// search_end)` returns `(written, consumed | end << 62)` in two registers,
// with end 0 none, 1 control, 2 article, as rapidyenc's YencDecoderEnd, and
// `(usize::MAX, usize::MAX)` for an error. The call does no feature check and no
// bounds check, and reads the kernel's outcome field by field.
//
// SAFETY contract: the CPU runs the tier (`resolve` checks it once) and
// `dst` holds at least `src.len()` bytes.
pub type DecodeFn = unsafe fn(&[u8], &mut [u8], bool) -> (usize, usize);

// Splits a `DecodeFn` answer into (consumed, written, end).
pub fn unpack(out: (usize, usize)) -> (usize, usize, i32) {
    if out == (usize::MAX, usize::MAX) {
        return (usize::MAX, usize::MAX, -1);
    }
    (out.1 & (usize::MAX >> 2), out.0, (out.1 >> 62) as i32)
}

// The tier's `DecodeFn`, or None when this CPU lacks a feature it needs.
pub fn resolve(tier: Tier) -> Option<DecodeFn> {
    if !runs(tier) {
        return None;
    }
    Some(match tier {
        #[cfg(target_arch = "x86_64")]
        Tier::Sse2 => sse2,
        #[cfg(target_arch = "x86_64")]
        Tier::Ssse3 => ssse3,
        #[cfg(target_arch = "x86_64")]
        Tier::Sse41 => sse41,
        #[cfg(target_arch = "x86_64")]
        Tier::Avx => avx,
        #[cfg(target_arch = "x86_64")]
        Tier::Avx2 => avx2,
        #[cfg(target_arch = "x86_64")]
        Tier::Avx512Vbmi2 => avx512_vbmi2,
        #[cfg(target_arch = "aarch64")]
        Tier::Neon => neon,
        _ => scalar,
    })
}

#[inline(always)]
fn pack(out: Result<KernelOutcome, YencError>) -> (usize, usize) {
    match out {
        Ok(o) => {
            let end = match o.end {
                RapidyencDecodeEnd::None => 0usize,
                RapidyencDecodeEnd::Control => 1,
                RapidyencDecodeEnd::Article => 2,
            };
            (o.written, o.consumed | end << 62)
        }
        Err(_) => (usize::MAX, usize::MAX),
    }
}

macro_rules! tier_fn {
    ($name:ident, $kernel:ident) => {
        #[allow(unused_unsafe)]
        unsafe fn $name(src: &[u8], dst: &mut [u8], search_end: bool) -> (usize, usize) {
            // What `decode_rapidyenc_incremental` starts from (CRLF state).
            let mut state = KernelState::from_rapidyenc_state(RapidyencDecodeState::CrLf);
            // SAFETY: the `DecodeFn` contract.
            pack(unsafe { $kernel(src, dst, &mut state, true, true, search_end) })
        }
    };
}

// The decoder state the fused NNTP decoder carries from one body chunk to the
// next (`decode_chunk_until_end_into`): it starts each article's body at CRLF
// with the `=ybegin line=` hint, and each call resets the end as
// `KernelState::from_decode_state` does.
pub struct ChunkState(KernelState);

impl ChunkState {
    pub fn body(line_length: Option<u32>) -> Self {
        Self(KernelState::body_with_line_length(line_length))
    }
}

// One chunk call of the fused decoder's body decode (dot-unstuffing, pending
// bytes kept, end search), with the tier fixed. Answers as `DecodeFn` does.
//
// SAFETY contract: as `DecodeFn`.
pub type ChunkFn = unsafe fn(&[u8], &mut [u8], &mut ChunkState) -> (usize, usize);

// The tier's `ChunkFn`, or None when this CPU lacks a feature it needs.
pub fn resolve_chunk(tier: Tier) -> Option<ChunkFn> {
    if !runs(tier) {
        return None;
    }
    Some(match tier {
        #[cfg(target_arch = "x86_64")]
        Tier::Sse2 => chunk_sse2,
        #[cfg(target_arch = "x86_64")]
        Tier::Ssse3 => chunk_ssse3,
        #[cfg(target_arch = "x86_64")]
        Tier::Sse41 => chunk_sse41,
        #[cfg(target_arch = "x86_64")]
        Tier::Avx => chunk_avx,
        #[cfg(target_arch = "x86_64")]
        Tier::Avx2 => chunk_avx2,
        #[cfg(target_arch = "x86_64")]
        Tier::Avx512Vbmi2 => chunk_avx512_vbmi2,
        #[cfg(target_arch = "aarch64")]
        Tier::Neon => chunk_neon,
        _ => chunk_scalar,
    })
}

macro_rules! chunk_fn {
    ($name:ident, $kernel:ident) => {
        #[allow(unused_unsafe)]
        unsafe fn $name(src: &[u8], dst: &mut [u8], state: &mut ChunkState) -> (usize, usize) {
            state.0.end = DecodeEnd::None;
            // SAFETY: the `ChunkFn` contract.
            pack(unsafe { $kernel(src, dst, &mut state.0, true, true, true) })
        }
    };
}

#[cfg(target_arch = "x86_64")]
chunk_fn!(chunk_sse2, decode_kernel_sse2);
#[cfg(target_arch = "x86_64")]
chunk_fn!(chunk_ssse3, decode_kernel_ssse3);
#[cfg(target_arch = "x86_64")]
chunk_fn!(chunk_sse41, decode_kernel_sse41);
#[cfg(target_arch = "x86_64")]
chunk_fn!(chunk_avx, decode_kernel_avx);
#[cfg(target_arch = "x86_64")]
chunk_fn!(chunk_avx2, decode_kernel_avx2);
#[cfg(target_arch = "x86_64")]
chunk_fn!(chunk_avx512_vbmi2, decode_kernel_avx512_vbmi2);
#[cfg(target_arch = "aarch64")]
chunk_fn!(chunk_neon, decode_kernel_neon);
chunk_fn!(chunk_scalar, decode_kernel_scalar);

#[cfg(target_arch = "x86_64")]
tier_fn!(sse2, decode_kernel_sse2);
#[cfg(target_arch = "x86_64")]
tier_fn!(ssse3, decode_kernel_ssse3);
#[cfg(target_arch = "x86_64")]
tier_fn!(sse41, decode_kernel_sse41);
#[cfg(target_arch = "x86_64")]
tier_fn!(avx, decode_kernel_avx);
#[cfg(target_arch = "x86_64")]
tier_fn!(avx2, decode_kernel_avx2);
#[cfg(target_arch = "x86_64")]
tier_fn!(avx512_vbmi2, decode_kernel_avx512_vbmi2);
#[cfg(target_arch = "aarch64")]
tier_fn!(neon, decode_kernel_neon);
tier_fn!(scalar, decode_kernel_scalar);
