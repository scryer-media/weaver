// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Forced-tier decode entry points for the fuzz targets in `fuzz/` and the
// crate's unit tests. Compiled only under `--cfg fuzzing` (what cargo-fuzz
// and ClusterFuzzLite pass) or `cfg(test)`, so it is no part of the crate's
// API or of any production build.
//
// Every entry point routes exactly as `decode_kernel` does, except that the
// tier is the caller's instead of the CPU dispatcher's: an output shorter
// than the input goes to the scalar kernel, as `decode_kernel` sends it.

use std::sync::atomic::{AtomicU8, Ordering};

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
// The same gates as `x86_tier` applies, without its slow-CPU preferences.
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

// 0: the build's choice (`cfg(weaver_yenc_raw_asm)`), 1: the asm kernel,
// 2: the intrinsic kernel. Read by `avx2_raw_asm`.
static AVX2_RAW_ASM: AtomicU8 = AtomicU8::new(0);

// Force the AVX2 raw path to the frozen asm kernel (`Some(true)`), the
// intrinsic kernel (`Some(false)`), or the build's choice (`None`).
pub fn set_avx2_raw_asm(choice: Option<bool>) {
    let v = match choice {
        None => 0,
        Some(true) => 1,
        Some(false) => 2,
    };
    AVX2_RAW_ASM.store(v, Ordering::Relaxed);
}

#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
pub(super) fn avx2_raw_asm_override() -> Option<bool> {
    match AVX2_RAW_ASM.load(Ordering::Relaxed) {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    }
}

// The CPU-family choices, forced the same way: the AVX-512 loop form
// (`zen4` in x86_avx512.rs: `Some(true)` the Zen 4 form, `Some(false)` the
// plain form) and the AVX2 escape select (`avx2_and_add_select` in
// x86_avx2.rs: `Some(true)` the and-add form, `Some(false)` `vpblendvb`).
// `None` is the CPU's own choice. Either form runs on any CPU with the tier.
static ZEN4: AtomicU8 = AtomicU8::new(0);
static AND_ADD: AtomicU8 = AtomicU8::new(0);

fn tri(choice: Option<bool>) -> u8 {
    match choice {
        None => 0,
        Some(true) => 1,
        Some(false) => 2,
    }
}

fn untri(v: u8) -> Option<bool> {
    match v {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    }
}

pub fn set_zen4(choice: Option<bool>) {
    ZEN4.store(tri(choice), Ordering::Relaxed);
}

pub fn set_and_add_select(choice: Option<bool>) {
    AND_ADD.store(tri(choice), Ordering::Relaxed);
}

#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
pub(super) fn zen4_override() -> Option<bool> {
    untri(ZEN4.load(Ordering::Relaxed))
}

#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
pub(super) fn and_add_select_override() -> Option<bool> {
    untri(AND_ADD.load(Ordering::Relaxed))
}

// The decoder's carried state: the line state, the end found, and the line
// length hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct State(KernelState);

const STATES: [DecoderState; 7] = [
    DecoderState::CrLf,
    DecoderState::Eq,
    DecoderState::Cr,
    DecoderState::None,
    DecoderState::CrLfDot,
    DecoderState::CrLfDotCr,
    DecoderState::CrLfEq,
];

impl State {
    // `state` picks one of the seven line states (modulo 7); a hint of 0 is
    // no hint, as `DecodeState::set_line_length_hint` sanitises it.
    pub fn new(state: u8, line_length: Option<u32>) -> Self {
        State(KernelState {
            state: STATES[state as usize % STATES.len()],
            end: DecodeEnd::None,
            line_length: sanitize_line_length(line_length),
        })
    }

    // Whether an end marker was found (search-end mode).
    pub fn ended(&self) -> bool {
        self.0.end != DecodeEnd::None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mode {
    pub dot_unstuffing: bool,
    pub preserve_pending: bool,
    pub search_end: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outcome {
    pub consumed: usize,
    pub written: usize,
    pub end: RapidyencDecodeEnd,
}

impl From<KernelOutcome> for Outcome {
    fn from(o: KernelOutcome) -> Self {
        Outcome {
            consumed: o.consumed,
            written: o.written,
            end: o.end,
        }
    }
}

// The reference: the scalar state machine, one `decode_scalar_step` at a
// time, with no main loop and no SIMD.
pub fn decode_reference(
    input: &[u8],
    output: &mut [u8],
    state: &mut State,
    mode: Mode,
) -> Result<Outcome, YencError> {
    let step_mode = DecodeStepMode {
        dot_unstuffing: mode.dot_unstuffing,
        preserve_pending: mode.preserve_pending,
        search_end: mode.search_end,
    };
    let st = &mut state.0;
    let (mut src, mut dst) = (0, 0);
    while (!mode.search_end || st.end == DecodeEnd::None) && src < input.len() {
        if !decode_scalar_step(input, &mut src, output, &mut dst, st, step_mode)? {
            break;
        }
    }
    Ok(Outcome {
        consumed: src,
        written: dst,
        end: st.end.into(),
    })
}

// `decode_kernel` with the tier forced. Panics if the CPU cannot run it.
pub fn decode_tier(
    tier: Tier,
    input: &[u8],
    output: &mut [u8],
    state: &mut State,
    mode: Mode,
) -> Result<Outcome, YencError> {
    run_tier(tier, input, output, &mut state.0, mode).map(Outcome::from)
}

fn run_tier(
    tier: Tier,
    input: &[u8],
    output: &mut [u8],
    st: &mut KernelState,
    mode: Mode,
) -> Result<KernelOutcome, YencError> {
    assert!(
        runs(tier),
        "tier {} does not run on this CPU",
        tier.as_str()
    );
    let (d, p, s) = (mode.dot_unstuffing, mode.preserve_pending, mode.search_end);
    if output.len() < input.len() {
        return decode_kernel_scalar(input, output, st, d, p, s);
    }
    // SAFETY: `runs` checked every feature the kernel is compiled with, and
    // `output` holds at least `input.len()` bytes, as `decode_kernel` ensures.
    unsafe {
        match tier {
            #[cfg(target_arch = "x86_64")]
            Tier::Sse2 => decode_kernel_sse2(input, output, st, d, p, s),
            #[cfg(target_arch = "x86_64")]
            Tier::Ssse3 => decode_kernel_ssse3(input, output, st, d, p, s),
            #[cfg(target_arch = "x86_64")]
            Tier::Sse41 => decode_kernel_sse41(input, output, st, d, p, s),
            #[cfg(target_arch = "x86_64")]
            Tier::Avx => decode_kernel_avx(input, output, st, d, p, s),
            #[cfg(target_arch = "x86_64")]
            Tier::Avx2 => decode_kernel_avx2(input, output, st, d, p, s),
            #[cfg(target_arch = "x86_64")]
            Tier::Avx512Vbmi2 => decode_kernel_avx512_vbmi2(input, output, st, d, p, s),
            #[cfg(target_arch = "aarch64")]
            Tier::Neon => decode_kernel_neon(input, output, st, d, p, s),
            _ => decode_kernel_scalar(input, output, st, d, p, s),
        }
    }
}

// The fused NNTP decoder's carried state between body chunks: a
// `DecodeState`, converted to the kernel's state and back on every call as
// `decode_chunk_until_end_into` does.
pub struct ChunkState(crate::decode::DecodeState);

impl ChunkState {
    // An article body's start: CRLF, with the `=ybegin line=` hint.
    pub fn body(line_length: Option<u32>) -> Self {
        let mut s = crate::decode::DecodeState::new();
        s.set_line_length_hint(line_length);
        ChunkState(s)
    }

    // The carried line state and hint, with no end (the conversion drops it).
    pub fn line_state(&self) -> State {
        State(KernelState::from_decode_state(&self.0))
    }
}

impl State {
    // This state with the end cleared, to compare with `ChunkState`.
    pub fn without_end(self) -> State {
        State(KernelState {
            end: DecodeEnd::None,
            ..self.0
        })
    }
}

// `decode_chunk_until_end_into` (dot-unstuffing, pending bytes kept, end
// search) with the tier forced.
pub fn chunk_until_end_tier(
    tier: Tier,
    input: &[u8],
    output: &mut [u8],
    state: &mut ChunkState,
) -> Result<Outcome, YencError> {
    let mut ks = KernelState::from_decode_state(&state.0);
    let mode = Mode {
        dot_unstuffing: true,
        preserve_pending: true,
        search_end: true,
    };
    let out = run_tier(tier, input, output, &mut ks, mode)?;
    ks.write_back(&mut state.0);
    Ok(out.into())
}

// `decode_rapidyenc_incremental_into` with the tier forced: rapidyenc's
// `decode_end` semantics, no line hint.
pub fn rapidyenc_incremental_tier(
    tier: Tier,
    input: &[u8],
    output: &mut [u8],
    state: &mut RapidyencDecodeState,
) -> Result<Outcome, YencError> {
    let mut ks = KernelState::from_rapidyenc_state(*state);
    let mode = Mode {
        dot_unstuffing: true,
        preserve_pending: true,
        search_end: true,
    };
    let out = run_tier(tier, input, output, &mut ks, mode)?;
    ks.write_back_rapidyenc(state);
    Ok(out.into())
}

// rapidyenc's own `decode_end` (the C library, linked when the build saw
// `WEAVER_RAPIDYENC_SRC`), carrying `state`. `None` when it is not linked.
// `output` must hold at least `input.len()` bytes.
#[cfg(rapidyenc_linked)]
pub fn rapidyenc_oracle(
    input: &[u8],
    output: &mut [u8],
    state: &mut RapidyencDecodeState,
) -> Option<Outcome> {
    unsafe extern "C" {
        fn weaver_rapidyenc_decode_init();
        fn weaver_rapidyenc_decode_end_state(
            src: *const u8,
            dest: *mut u8,
            len: u64,
            consumed: *mut u64,
            written: *mut u64,
            state: *mut i32,
        ) -> i32;
    }
    static INIT: std::sync::Once = std::sync::Once::new();
    // SAFETY: rapidyenc's one-time dispatch set-up.
    INIT.call_once(|| unsafe { weaver_rapidyenc_decode_init() });
    assert!(output.len() >= input.len());
    let mut st: i32 = match *state {
        RapidyencDecodeState::CrLf => 0,
        RapidyencDecodeState::Eq => 1,
        RapidyencDecodeState::Cr => 2,
        RapidyencDecodeState::None => 3,
        RapidyencDecodeState::CrLfDot => 4,
        RapidyencDecodeState::CrLfDotCr => 5,
        RapidyencDecodeState::CrLfEq => 6,
    };
    let (mut consumed, mut written) = (0u64, 0u64);
    // SAFETY: `input` and `output` are valid for `input.len()` bytes; rapidyenc
    // writes no more than it reads.
    let end = unsafe {
        weaver_rapidyenc_decode_end_state(
            input.as_ptr(),
            output.as_mut_ptr(),
            input.len() as u64,
            &mut consumed,
            &mut written,
            &mut st,
        )
    };
    *state = match st {
        0 => RapidyencDecodeState::CrLf,
        1 => RapidyencDecodeState::Eq,
        2 => RapidyencDecodeState::Cr,
        3 => RapidyencDecodeState::None,
        4 => RapidyencDecodeState::CrLfDot,
        5 => RapidyencDecodeState::CrLfDotCr,
        _ => RapidyencDecodeState::CrLfEq,
    };
    Some(Outcome {
        consumed: consumed as usize,
        written: written as usize,
        end: match end {
            0 => RapidyencDecodeEnd::None,
            1 => RapidyencDecodeEnd::Control,
            _ => RapidyencDecodeEnd::Article,
        },
    })
}

#[cfg(not(rapidyenc_linked))]
pub fn rapidyenc_oracle(
    _input: &[u8],
    _output: &mut [u8],
    _state: &mut RapidyencDecodeState,
) -> Option<Outcome> {
    None
}

// `decode_kernel` itself: the tier the CPU dispatcher picks.
pub fn decode_dispatched(
    input: &[u8],
    output: &mut [u8],
    state: &mut State,
    mode: Mode,
) -> Result<Outcome, YencError> {
    decode_kernel(
        input,
        output,
        &mut state.0,
        mode.dot_unstuffing,
        mode.preserve_pending,
        mode.search_end,
    )
    .map(Outcome::from)
}

// The whole-body raw end-searching decode with CRC folding,
// `decode_raw_body_until_end_crc`, with a chosen stretch length. Returns the
// outcome and the CRC of the decoded bytes.
pub fn raw_body_until_end_crc(
    input: &[u8],
    output: &mut [u8],
    line_length: Option<u32>,
    stretch: usize,
) -> (Result<Outcome, YencError>, u32) {
    let mut crc = crate::crc::Crc32::new();
    let out = decode_raw_body_until_end_crc_in(input, output, line_length, &mut crc, stretch);
    (out.map(Outcome::from), crc.finalize())
}

// The bounded AVX2 stretches of `decode_raw_body_until_end_crc` (the
// `decode_raw_stretches` loop) on any CPU with AVX2, whatever tier the
// dispatcher picks, then the remainder through the AVX2 tier kernel. `None`
// when AVX2 does not run here or the output is compact (the production path
// then takes no stretches either).
pub fn raw_body_until_end_crc_avx2(
    input: &[u8],
    output: &mut [u8],
    line_length: Option<u32>,
    stretch: usize,
) -> Option<(Result<Outcome, YencError>, u32)> {
    #[cfg(target_arch = "x86_64")]
    {
        if !runs(Tier::Avx2) || output.len() < input.len() {
            return None;
        }
        const LOOKAHEAD: usize = 128;
        let mut crc = crate::crc::Crc32::new();
        let mut state = KernelState::body_with_line_length(line_length);
        let (mut consumed, mut written) = (0usize, 0usize);
        let base = input.as_ptr() as usize;
        let res = (|| {
            while consumed.saturating_add(stretch).saturating_add(LOOKAHEAD) <= input.len() {
                let limit = ((base + consumed + stretch) & !63) - base;
                // SAFETY: `runs(Avx2)` checked the features.
                let Some(outcome) = (unsafe {
                    decode_raw_bounded_avx2(
                        &input[consumed..],
                        &mut output[written..],
                        &mut state,
                        limit - consumed,
                    )
                }) else {
                    break;
                };
                let outcome = outcome.map_err(|err| shift_escape_error(err, consumed))?;
                crc.update(&output[written..written + outcome.written]);
                consumed += outcome.consumed;
                written += outcome.written;
                if state.end != DecodeEnd::None || outcome.consumed == 0 {
                    break;
                }
            }
            let tail = if state.end == DecodeEnd::None {
                // SAFETY: as above; the remaining output is at least as long
                // as the remaining input (each stretch wrote no more than it
                // consumed).
                unsafe {
                    decode_kernel_avx2(
                        &input[consumed..],
                        &mut output[written..],
                        &mut state,
                        true,
                        false,
                        true,
                    )
                }
                .map_err(|err| shift_escape_error(err, consumed))?
            } else {
                KernelOutcome {
                    consumed: 0,
                    written: 0,
                    end: state.end.into(),
                }
            };
            crc.update(&output[written..written + tail.written]);
            Ok(Outcome {
                consumed: consumed + tail.consumed,
                written: written + tail.written,
                end: tail.end,
            })
        })();
        Some((res, crc.finalize()))
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = (input, output, line_length, stretch);
        None
    }
}
