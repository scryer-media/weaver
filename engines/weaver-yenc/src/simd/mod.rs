//! SIMD and lookup-table accelerated yEnc decoding.
//!
//! The decoder keeps the public streaming state in `decode.rs`, but normalizes it
//! here so the same fast path can serve full-body and chunked decode.
//!
//! Line-aware SIMD yEnc decode, validated tier-by-tier against a scalar reference
//! (Public Domain/CC0), especially `decoder.cc`, `decoder_common.h`,
//! `decoder_avx2_base.h`, and `decoder_neon64.cc`.

use crate::decode::{DecodeState, RapidyencDecodeEnd, RapidyencDecodeState};
use crate::error::YencError;

#[cfg(target_arch = "x86_64")]
type DecodeRunFn = fn(&[u8], usize, &mut [u8], usize) -> (usize, usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecoderState {
    CrLf,
    Eq,
    Cr,
    None,
    CrLfDot,
    CrLfDotCr,
    CrLfEq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeEnd {
    None,
    Article,
    Control,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KernelState {
    state: DecoderState,
    end: DecodeEnd,
    line_length: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KernelOutcome {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: RapidyencDecodeEnd,
}

#[derive(Debug, Clone, Copy)]
struct DecodeStepMode {
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
}

impl From<DecodeEnd> for RapidyencDecodeEnd {
    fn from(end: DecodeEnd) -> Self {
        match end {
            DecodeEnd::None => Self::None,
            DecodeEnd::Article => Self::Article,
            DecodeEnd::Control => Self::Control,
        }
    }
}

impl KernelState {
    #[cfg(test)]
    fn body() -> Self {
        Self::body_with_line_length(None)
    }

    fn body_with_line_length(line_length: Option<u32>) -> Self {
        Self {
            state: DecoderState::CrLf,
            end: DecodeEnd::None,
            line_length: sanitize_line_length(line_length),
        }
    }

    fn from_decode_state(state: &DecodeState) -> Self {
        let decoder_state = if state.cr_pending && state.dot_pending {
            DecoderState::CrLfDotCr
        } else if state.cr_pending {
            DecoderState::Cr
        } else if state.escape_pending && state.at_line_start {
            DecoderState::CrLfEq
        } else if state.escape_pending {
            DecoderState::Eq
        } else if state.dot_pending {
            DecoderState::CrLfDot
        } else if state.at_line_start {
            DecoderState::CrLf
        } else {
            DecoderState::None
        };

        Self {
            state: decoder_state,
            end: DecodeEnd::None,
            line_length: state.line_length_hint,
        }
    }

    fn from_rapidyenc_state(state: RapidyencDecodeState) -> Self {
        let state = match state {
            RapidyencDecodeState::CrLf => DecoderState::CrLf,
            RapidyencDecodeState::Eq => DecoderState::Eq,
            RapidyencDecodeState::Cr => DecoderState::Cr,
            RapidyencDecodeState::None => DecoderState::None,
            RapidyencDecodeState::CrLfDot => DecoderState::CrLfDot,
            RapidyencDecodeState::CrLfDotCr => DecoderState::CrLfDotCr,
            RapidyencDecodeState::CrLfEq => DecoderState::CrLfEq,
        };
        Self {
            state,
            end: DecodeEnd::None,
            line_length: None,
        }
    }

    fn write_back(self, state: &mut DecodeState) {
        state.escape_pending = matches!(self.state, DecoderState::Eq | DecoderState::CrLfEq);
        state.dot_pending = matches!(self.state, DecoderState::CrLfDot | DecoderState::CrLfDotCr);
        state.cr_pending = matches!(self.state, DecoderState::Cr | DecoderState::CrLfDotCr);
        state.at_line_start = matches!(
            self.state,
            DecoderState::CrLf
                | DecoderState::CrLfDot
                | DecoderState::CrLfDotCr
                | DecoderState::CrLfEq
        );
        state.line_length_hint = self.line_length;
    }

    fn write_back_rapidyenc(self, state: &mut RapidyencDecodeState) {
        *state = match self.state {
            DecoderState::CrLf => RapidyencDecodeState::CrLf,
            DecoderState::Eq => RapidyencDecodeState::Eq,
            DecoderState::Cr => RapidyencDecodeState::Cr,
            DecoderState::None => RapidyencDecodeState::None,
            DecoderState::CrLfDot => RapidyencDecodeState::CrLfDot,
            DecoderState::CrLfDotCr => RapidyencDecodeState::CrLfDotCr,
            DecoderState::CrLfEq => RapidyencDecodeState::CrLfEq,
        };
    }
}

#[inline]
fn sanitize_line_length(line_length: Option<u32>) -> Option<usize> {
    line_length
        .and_then(|value| usize::try_from(value).ok())
        .filter(|&value| value > 0)
}

/// Decode a complete body buffer with the SIMD-capable internal kernel.
#[cfg(test)]
pub(crate) fn decode_body_into(
    input: &[u8],
    output: &mut [u8],
    dot_unstuffing: bool,
) -> Result<usize, YencError> {
    decode_body_into_with_line_length(input, output, dot_unstuffing, None)
}

pub(crate) fn decode_body_into_with_line_length(
    input: &[u8],
    output: &mut [u8],
    dot_unstuffing: bool,
    line_length: Option<u32>,
) -> Result<usize, YencError> {
    let mut state = KernelState::body_with_line_length(line_length);
    Ok(decode_kernel(input, output, &mut state, dot_unstuffing, false, false)?.written)
}

/// Decode one streaming body chunk with carry state preserved across calls.
pub(crate) fn decode_chunk_into(
    input: &[u8],
    output: &mut [u8],
    state: &mut DecodeState,
    dot_unstuffing: bool,
) -> Result<usize, YencError> {
    let mut kernel_state = KernelState::from_decode_state(state);
    let written = decode_kernel(
        input,
        output,
        &mut kernel_state,
        dot_unstuffing,
        true,
        false,
    )?
    .written;
    kernel_state.write_back(state);
    Ok(written)
}

pub(crate) fn decode_chunk_until_end_into(
    input: &[u8],
    output: &mut [u8],
    state: &mut DecodeState,
    dot_unstuffing: bool,
) -> Result<KernelOutcome, YencError> {
    let mut kernel_state = KernelState::from_decode_state(state);
    let outcome = decode_kernel(input, output, &mut kernel_state, dot_unstuffing, true, true)?;
    kernel_state.write_back(state);
    Ok(outcome)
}

/// Decode a chunk carrying an explicit decoder state; `is_raw` toggles dot-unstuffing.
pub(crate) fn decode_rapidyenc_into(
    input: &[u8],
    output: &mut [u8],
    is_raw: bool,
    state: &mut RapidyencDecodeState,
) -> Result<usize, YencError> {
    if input.is_empty() {
        return Ok(0);
    }

    let initial_state = match (is_raw, *state) {
        (false, RapidyencDecodeState::Eq) => RapidyencDecodeState::Eq,
        (false, _) => RapidyencDecodeState::None,
        (
            true,
            RapidyencDecodeState::CrLfDot
            | RapidyencDecodeState::CrLfDotCr
            | RapidyencDecodeState::CrLfEq,
        ) => RapidyencDecodeState::None,
        (true, state) => state,
    };
    let suppress_pending_raw_cr =
        is_raw && initial_state == RapidyencDecodeState::Eq && matches!(input, b"\r" | b"\r\n");
    let mut kernel_state = KernelState::from_rapidyenc_state(initial_state);
    let mut written = decode_kernel(input, output, &mut kernel_state, is_raw, true, false)?.written;
    if suppress_pending_raw_cr {
        written = 0;
    }
    if is_raw {
        *state = match kernel_state.state {
            DecoderState::CrLfEq => RapidyencDecodeState::Eq,
            DecoderState::CrLfDot => RapidyencDecodeState::None,
            _ => {
                kernel_state.write_back_rapidyenc(state);
                *state
            }
        };
    } else {
        *state = if kernel_state.state == DecoderState::Eq {
            RapidyencDecodeState::Eq
        } else {
            RapidyencDecodeState::None
        };
    }
    Ok(written)
}

/// Incremental decode that stops and reports at a yEnc/NNTP end marker.
pub(crate) fn decode_rapidyenc_incremental_into(
    input: &[u8],
    output: &mut [u8],
    state: &mut RapidyencDecodeState,
) -> Result<KernelOutcome, YencError> {
    let mut kernel_state = KernelState::from_rapidyenc_state(*state);
    let outcome = decode_kernel(input, output, &mut kernel_state, true, true, true)?;
    kernel_state.write_back_rapidyenc(state);
    Ok(outcome)
}

fn decode_kernel(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512vbmi2")
            && is_x86_feature_detected!("avx512vl")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx2")
        {
            return unsafe {
                decode_kernel_avx512_vbmi2(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("avx2") {
            return unsafe {
                decode_kernel_avx2(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("popcnt")
            && is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("ssse3")
            && !x86_prefers_ssse3_over_sse41()
            && !x86_prefers_sse2_over_ssse3()
        {
            return unsafe {
                decode_kernel_avx(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("ssse3")
            && !x86_prefers_ssse3_over_sse41()
            && !x86_prefers_sse2_over_ssse3()
        {
            return unsafe {
                decode_kernel_sse41(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("ssse3") && !x86_prefers_sse2_over_ssse3() {
            return unsafe {
                decode_kernel_ssse3(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
        if is_x86_feature_detected!("sse2") {
            return unsafe {
                decode_kernel_sse2(
                    input,
                    output,
                    state,
                    dot_unstuffing,
                    preserve_pending,
                    search_end,
                )
            };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        unsafe {
            decode_kernel_neon(
                input,
                output,
                state,
                dot_unstuffing,
                preserve_pending,
                search_end,
            )
        }
    }

    #[cfg(not(target_arch = "aarch64"))]
    {
        decode_kernel_scalar(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
        )
    }
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
// Bit hack that resolves consecutive '=' escape runs.
fn fix_eq_mask(mask: u64, mask_shift1: u64) -> u64 {
    let start = mask & !mask_shift1;
    let even = 0x5555_5555_5555_5555u64;
    let odd_groups = mask.wrapping_add(start & even);
    (odd_groups ^ even) & mask
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
fn final_state_after_block(
    raw_breaks: u64,
    raw_cr: u64,
    crlf_lf: u64,
    output_mask: u64,
) -> DecoderState {
    if raw_breaks == 0 {
        return DecoderState::None;
    }

    let last_break = 63 - raw_breaks.leading_zeros() as usize;
    let later_output_mask = if last_break == 63 {
        0
    } else {
        output_mask & (!0u64 << (last_break + 1))
    };

    if later_output_mask != 0 {
        DecoderState::None
    } else if raw_cr & (1u64 << last_break) != 0 {
        DecoderState::Cr
    } else if crlf_lf & (1u64 << last_break) != 0 {
        DecoderState::CrLf
    } else {
        DecoderState::None
    }
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
fn compact_table_16() -> &'static [[u8; 16]; 32768] {
    use std::sync::OnceLock;

    static TABLE: OnceLock<Box<[[u8; 16]; 32768]>> = OnceLock::new();
    TABLE
        .get_or_init(|| {
            let mut table = [[0x80u8; 16]; 32768];
            for (mask, row) in table.iter_mut().enumerate() {
                let mut out = 0usize;
                for lane in 0..16usize {
                    if mask & (1 << lane) == 0 {
                        row[out] = lane as u8;
                        out += 1;
                    }
                }
            }
            Box::new(table)
        })
        .as_ref()
}

#[inline]
fn write_decoded(output: &mut [u8], dst: &mut usize, byte: u8) -> Result<(), YencError> {
    if *dst >= output.len() {
        return Err(YencError::BufferTooSmall {
            needed: *dst + 1,
            available: output.len(),
        });
    }
    output[*dst] = byte;
    *dst += 1;
    Ok(())
}

/// Decode a run of "normal" yEnc bytes (no special characters) by subtracting 42.
///
/// Scans `input` from position `start`, decoding bytes into `output` at position
/// `dst_start`. Stops at the first special character (`=`, `\r`, `\n`) or end of input.
///
/// Returns `(bytes_consumed_from_input, bytes_written_to_output)`.
#[inline]
pub fn decode_normal_run(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    #[cfg(target_arch = "x86_64")]
    {
        return dispatch_x86_decode_normal_run()(input, start, output, dst_start);
    }

    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { decode_normal_run_neon(input, start, output, dst_start) };
    }

    #[cfg(target_arch = "riscv64")]
    {
        return decode_normal_run_riscv64(input, start, output, dst_start);
    }

    #[allow(unreachable_code)]
    decode_normal_run_scalar(input, start, output, dst_start)
}

mod scalar;

#[cfg(target_arch = "x86_64")]
mod x86_avx2;
#[cfg(target_arch = "x86_64")]
mod x86_avx512;
#[cfg(target_arch = "x86_64")]
mod x86_common;
#[cfg(target_arch = "x86_64")]
mod x86_sse;

#[cfg(target_arch = "aarch64")]
mod neon;

#[cfg(test)]
mod tests;

#[cfg(target_arch = "aarch64")]
use neon::*;
use scalar::*;
#[cfg(target_arch = "x86_64")]
use x86_avx2::*;
#[cfg(target_arch = "x86_64")]
use x86_avx512::*;
#[cfg(target_arch = "x86_64")]
use x86_common::*;
#[cfg(target_arch = "x86_64")]
use x86_sse::*;
