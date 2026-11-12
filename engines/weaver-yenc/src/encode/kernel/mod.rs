// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// yEnc body encoder: the kernel layer.
//
// SELF-CONTAINED: this directory uses std only. No `crate::` paths, no
// dependencies. The standalone bench crate (`engines/weaver-yenc/encode-bench`)
// compiles it by `#[path]`, so that a remote host builds only the kernel and
// never the weaver workspace. Keep it that way.
//
// Contract: the output is byte-identical to rapidyenc's
// `rapidyenc_encode_ex(line_size, &column, src, dest, len, is_end)` (upstream
// d223ecc) for every input, every line size and starting column in the
// domain below, both `is_end` values, and any split of the input across
// calls that carries `column` from one call to the next. The oracle tests
// (`tests/rapidyenc_encode_diff.rs`) check exactly that.
//
// Domain (derived from rapidyenc; see `MAX_LINE_SIZE` and `MAX_COLUMN`):
//
//   1 <= line_size <= MAX_LINE_SIZE
//   0 <= column    <= MAX_COLUMN
//
// * `line_size == 0` is undefined in rapidyenc (`rapidyenc_encode_max_length`
//   divides by it). Ours refuses it.
// * rapidyenc takes both as C `int`. A negative column is clamped to 0 by its
//   SIMD wrapper but not by its scalar encoder; ours is unsigned, so it
//   cannot arise.
// * A column at or past `line_size` is accepted, as rapidyenc accepts it: the
//   first input byte starts a new line (a CRLF is written first).
// * Empty input writes nothing and leaves `column` unchanged, `is_end` or
//   not. That is what rapidyenc's SIMD wrapper does on every x86 and ARM
//   build; its scalar encoder on its own reads out of bounds there.
//
// Two rapidyenc behaviours that a caller splitting input should know, both
// reproduced exactly because they are rapidyenc's output:
//
// * A call that ends right after a line's last byte leaves `column` at the
//   position before that byte, so the next call writes one more byte on the
//   same line before breaking it. Split output can carry a line one or two
//   bytes longer than unsplit output. Decoders do not care.
// * `is_end` only escapes a trailing TAB or SPACE in the bytes this call
//   wrote.
//
// Adding a SIMD tier (see `EncoderTier`):
//
//   1. Add one file next to this one, e.g. `neon.rs`, with
//        pub(super) unsafe fn encode_raw(line_size, column, src, len, dst,
//                                        is_end) -> usize
//      marked `#[target_feature(enable = ...)]` with exactly the features
//      `EncoderTier::cpu_supported` checks for that tier. Implement it as
//        scalar::encode_simd_frame(line_size, column, src, len, dst, is_end,
//                                  |ls, col, s, n, d| main_loop(...))
//      so the scalar tail, the `line_size < 12` cut-over and the end rule
//      stay rapidyenc's.
//   2. Declare the module below at its `SLOT` marker.
//   3. Fill the tier's arm in `raw_fn` (another `SLOT` marker).
//
// Nothing else changes: detection, forcing, the always-on differential tests,
// the rapidyenc oracle and the bench all iterate `EncoderTier::available()`.

use std::fmt;
use std::mem::MaybeUninit;
use std::sync::OnceLock;

mod scalar;
mod tables;

#[cfg(target_arch = "x86_64")]
mod avx;
#[cfg(target_arch = "x86_64")]
mod avx2;
#[cfg(target_arch = "x86_64")]
mod avx512vbmi2;
#[cfg(target_arch = "aarch64")]
mod neon;
#[cfg(target_arch = "x86_64")]
mod sse2;
#[cfg(target_arch = "x86_64")]
mod ssse3;
#[cfg(target_arch = "x86_64")]
mod x86_common;

#[cfg(test)]
mod tests;

// Largest accepted line size: `i32::MAX - 1`. rapidyenc holds the column in a
// C `int` and can raise it to `line_size + 1` (a two-byte escape ending the
// line's middle, then the end-of-article escape); one less than `i32::MAX`
// keeps that from overflowing.
pub const MAX_LINE_SIZE: usize = i32::MAX as usize - 1;

// Largest accepted starting column: `i32::MAX - 1`, for the same reason.
pub const MAX_COLUMN: usize = i32::MAX as usize - 1;

// Destination bytes the encoder needs for `len` input bytes at `line_size`.
//
// The same value as rapidyenc's `rapidyenc_encode_max_length(len, line_size)`
// in its default build (with 256-bit vectors, rapidyenc.cc:41-53): every byte
// escaped, two bytes for a leading break or offset, 64 bytes of room for a
// vector store running past the end, and a CRLF for every line the worst
// case can fill. (rapidyenc's `line_size == 128` shortcut is the same number
// as the general formula.)
//
// Panics if `line_size == 0` or the result overflows `usize` (rapidyenc
// divides by zero and wraps there).
pub const fn encode_max_len(len: usize, line_size: usize) -> usize {
    assert!(
        line_size != 0,
        "encode_max_len: line_size must be at least 1"
    );
    let Some(doubled) = len.checked_mul(2) else {
        panic!("encode_max_len: length overflows usize");
    };
    let Some(newlines) = (doubled / line_size).checked_mul(2) else {
        panic!("encode_max_len: length overflows usize");
    };
    match doubled.checked_add(2 + 64) {
        Some(base) => match base.checked_add(newlines) {
            Some(total) => total,
            None => panic!("encode_max_len: length overflows usize"),
        },
        None => panic!("encode_max_len: length overflows usize"),
    }
}

// The encode tiers, one per rapidyenc encode kernel.
//
// Only `Scalar` is implemented today; the others are named so their slots
// exist. A tier is *available* when it is implemented in this build and the
// CPU supports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EncoderTier {
    Scalar,
    Neon,
    Sse2,
    Ssse3,
    Avx,
    Avx2,
    Avx512Vbmi2,
}

impl EncoderTier {
    // Every tier, slowest first.
    pub const ALL: [EncoderTier; 7] = [
        EncoderTier::Scalar,
        EncoderTier::Neon,
        EncoderTier::Sse2,
        EncoderTier::Ssse3,
        EncoderTier::Avx,
        EncoderTier::Avx2,
        EncoderTier::Avx512Vbmi2,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Neon => "neon",
            Self::Sse2 => "sse2",
            Self::Ssse3 => "ssse3",
            Self::Avx => "avx",
            Self::Avx2 => "avx2",
            Self::Avx512Vbmi2 => "avx512-vbmi2",
        }
    }

    // Parse `as_str` output back.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|tier| tier.as_str() == name)
    }

    // rapidyenc's kernel id for the matching encode kernel (the `RYKERN_*`
    // values in rapidyenc.h, also what `rapidyenc_encode_kernel` returns).
    pub const fn rapidyenc_kernel(self) -> i32 {
        match self {
            Self::Scalar => 0,
            Self::Sse2 => 0x100,
            Self::Ssse3 => 0x200,
            Self::Avx => 0x381,
            Self::Avx2 => 0x403,
            Self::Avx512Vbmi2 => 0x603,
            Self::Neon => 0x1000,
        }
    }

    // Whether this CPU has every feature the tier's kernel is compiled with.
    // These are the gates rapidyenc's `cpu_supports_isa` applies for the
    // same kernel, minus its slow-microarchitecture demotions.
    pub fn cpu_supported(self) -> bool {
        match self {
            Self::Scalar => true,
            Self::Neon => {
                #[cfg(target_arch = "aarch64")]
                {
                    std::arch::is_aarch64_feature_detected!("neon")
                }
                #[cfg(not(target_arch = "aarch64"))]
                {
                    false
                }
            }
            Self::Sse2 | Self::Ssse3 | Self::Avx | Self::Avx2 | Self::Avx512Vbmi2 => {
                x86_supported(self)
            }
        }
    }

    // Whether this build has a kernel for the tier.
    pub fn implemented(self) -> bool {
        raw_fn(self).is_some()
    }

    // Implemented here and supported by this CPU: safe to force.
    pub fn is_available(self) -> bool {
        self.implemented() && self.cpu_supported()
    }

    // Every available tier, slowest first. Tests and benches iterate this, so
    // a newly filled slot is picked up without touching them.
    pub fn available() -> Vec<EncoderTier> {
        Self::ALL
            .into_iter()
            .filter(|tier| tier.is_available())
            .collect()
    }

    // The tier `encode_body` dispatches to on this host: the fastest
    // available one. Resolved once per process.
    pub fn detect() -> EncoderTier {
        selected().0
    }
}

impl fmt::Display for EncoderTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(target_arch = "x86_64")]
fn x86_supported(tier: EncoderTier) -> bool {
    use std::arch::is_x86_feature_detected as has;
    match tier {
        EncoderTier::Sse2 => has!("sse2"),
        EncoderTier::Ssse3 => has!("ssse3"),
        EncoderTier::Avx => has!("avx") && has!("popcnt") && has!("sse4.1") && has!("ssse3"),
        EncoderTier::Avx2 => {
            has!("avx2") && has!("bmi1") && has!("bmi2") && has!("popcnt") && has!("lzcnt")
        }
        EncoderTier::Avx512Vbmi2 => {
            has!("avx512vbmi2")
                && has!("avx512vl")
                && has!("avx512bw")
                && has!("avx512f")
                && has!("avx2")
                && has!("bmi1")
                && has!("bmi2")
                && has!("popcnt")
                && has!("lzcnt")
        }
        EncoderTier::Scalar | EncoderTier::Neon => false,
    }
}

#[cfg(not(target_arch = "x86_64"))]
fn x86_supported(_tier: EncoderTier) -> bool {
    false
}

// The raw encoder every tier implements.
//
// # Safety
//
// * `src` is readable for `len` bytes and `dst` is writable for
//   `encode_max_len(len, line_size)` bytes; the two do not overlap.
// * `1 <= line_size <= MAX_LINE_SIZE` and `*column <= MAX_COLUMN`.
// * The CPU has the features of the tier the function belongs to.
//
// Returns the bytes written from `dst`, all initialised, and updates
// `*column`. A kernel may also scribble on bytes past the returned count but
// inside the budget; nothing reads them.
pub(crate) type RawEncodeFn = unsafe fn(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize;

// The dispatch table: one arm per tier. Filling a slot is the only change
// dispatch needs when a kernel lands.
fn raw_fn(tier: EncoderTier) -> Option<RawEncodeFn> {
    match tier {
        EncoderTier::Scalar => Some(scalar::encode_raw as RawEncodeFn),
        #[cfg(target_arch = "aarch64")]
        EncoderTier::Neon => Some(neon::encode_raw as RawEncodeFn),
        #[cfg(not(target_arch = "aarch64"))]
        EncoderTier::Neon => None,
        #[cfg(target_arch = "x86_64")]
        EncoderTier::Sse2 => Some(sse2::encode_raw as RawEncodeFn),
        #[cfg(not(target_arch = "x86_64"))]
        EncoderTier::Sse2 => None,
        #[cfg(target_arch = "x86_64")]
        EncoderTier::Ssse3 => Some(ssse3::encode_raw as RawEncodeFn),
        #[cfg(target_arch = "x86_64")]
        EncoderTier::Avx => Some(avx::encode_raw as RawEncodeFn),
        #[cfg(not(target_arch = "x86_64"))]
        EncoderTier::Ssse3 | EncoderTier::Avx => None,
        #[cfg(target_arch = "x86_64")]
        EncoderTier::Avx2 => Some(avx2::encode_raw as RawEncodeFn),
        #[cfg(target_arch = "x86_64")]
        // The tier's loop for this CPU, chosen once (see `encode_fn`).
        EncoderTier::Avx512Vbmi2 => Some(avx512vbmi2::encode_fn()),
        #[cfg(not(target_arch = "x86_64"))]
        EncoderTier::Avx2 | EncoderTier::Avx512Vbmi2 => None,
    }
}

fn selected() -> (EncoderTier, RawEncodeFn) {
    static SELECTED: OnceLock<(EncoderTier, RawEncodeFn)> = OnceLock::new();
    *SELECTED.get_or_init(|| {
        let tier = EncoderTier::ALL
            .into_iter()
            .rev()
            .find(|tier| tier.is_available())
            .unwrap_or(EncoderTier::Scalar);
        (
            tier,
            raw_fn(tier).expect("an available tier is implemented"),
        )
    })
}

// Why an encode call was refused. Nothing was written and `column` is
// unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    // `line_size` is 0 or above `MAX_LINE_SIZE`.
    LineSizeOutOfRange { line_size: usize },
    // `column` is above `MAX_COLUMN`.
    ColumnOutOfRange { column: usize },
    // The destination is shorter than `encode_max_len(src.len(), line_size)`.
    DestinationTooSmall { needed: usize, available: usize },
    // A forced tier is not implemented in this build or not supported by
    // this CPU.
    TierUnavailable { tier: EncoderTier },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LineSizeOutOfRange { line_size } => {
                write!(f, "line size {line_size} outside 1..={MAX_LINE_SIZE}")
            }
            Self::ColumnOutOfRange { column } => {
                write!(f, "column {column} above {MAX_COLUMN}")
            }
            Self::DestinationTooSmall { needed, available } => {
                write!(
                    f,
                    "encode destination too small: need {needed}, have {available}"
                )
            }
            Self::TierUnavailable { tier } => {
                write!(f, "encoder tier {tier} is not available on this host")
            }
        }
    }
}

impl std::error::Error for EncodeError {}

// yEnc-encode `src` as body bytes into `dst` and return the bytes written.
//
// `column` is the column the output starts at (0 at the start of an article)
// and is updated to where it ends, so a body can be encoded in pieces by
// passing the same `column` to each call. Set `is_end` on the last piece so
// a trailing TAB or SPACE is escaped. Line breaks are CRLF; no header or
// trailer is written.
//
// `dst` may be uninitialised; it must hold at least
// `encode_max_len(src.len(), line_size)` bytes. On return the first `n`
// bytes of `dst` are initialised, where `n` is the returned count.
pub fn encode_body(
    line_size: usize,
    column: &mut usize,
    src: &[u8],
    dst: &mut [MaybeUninit<u8>],
    is_end: bool,
) -> Result<usize, EncodeError> {
    check_domain(line_size, *column)?;
    let (_, raw) = selected();
    // SAFETY: the domain is checked and `raw` is the detected tier, whose
    // CPU features were verified by `is_available`.
    unsafe { encode_checked(raw, line_size, column, src, dst, is_end) }
}

// `encode_body` on a forced tier, for tests and benches. Refuses a tier that
// is not available here.
pub fn encode_body_with_tier(
    tier: EncoderTier,
    line_size: usize,
    column: &mut usize,
    src: &[u8],
    dst: &mut [MaybeUninit<u8>],
    is_end: bool,
) -> Result<usize, EncodeError> {
    check_domain(line_size, *column)?;
    let raw = match raw_fn(tier) {
        Some(raw) if tier.cpu_supported() => raw,
        _ => return Err(EncodeError::TierUnavailable { tier }),
    };
    // SAFETY: the domain is checked and the CPU supports `tier`.
    unsafe { encode_checked(raw, line_size, column, src, dst, is_end) }
}

// `encode_body`, appending to `out` (no zero-fill of the new space).
pub fn encode_body_append(
    line_size: usize,
    column: &mut usize,
    src: &[u8],
    out: &mut Vec<u8>,
    is_end: bool,
) -> Result<usize, EncodeError> {
    check_domain(line_size, *column)?;
    let start = out.len();
    out.reserve(encode_max_len(src.len(), line_size));
    let written = encode_body(line_size, column, src, out.spare_capacity_mut(), is_end)?;
    // SAFETY: `encode_body` initialised the first `written` bytes of the
    // spare capacity, and `written` fits in it.
    unsafe { out.set_len(start + written) };
    Ok(written)
}

fn check_domain(line_size: usize, column: usize) -> Result<(), EncodeError> {
    if !(1..=MAX_LINE_SIZE).contains(&line_size) {
        return Err(EncodeError::LineSizeOutOfRange { line_size });
    }
    if column > MAX_COLUMN {
        return Err(EncodeError::ColumnOutOfRange { column });
    }
    Ok(())
}

// Size check, then the raw call.
//
// # Safety
//
// `line_size` and `*column` are in the domain and the CPU supports the tier
// `raw` belongs to.
unsafe fn encode_checked(
    raw: RawEncodeFn,
    line_size: usize,
    column: &mut usize,
    src: &[u8],
    dst: &mut [MaybeUninit<u8>],
    is_end: bool,
) -> Result<usize, EncodeError> {
    let needed = encode_max_len(src.len(), line_size);
    if dst.len() < needed {
        return Err(EncodeError::DestinationTooSmall {
            needed,
            available: dst.len(),
        });
    }
    // SAFETY: `src` is a live slice of `src.len()` bytes; `dst` is a live,
    // exclusive slice of at least `encode_max_len(src.len(), line_size)`
    // bytes, so the two cannot overlap; the kernels write only initialised
    // bytes, so writing through a `MaybeUninit` slice is sound; the domain
    // and CPU support are the caller's guarantee.
    let written = unsafe {
        raw(
            line_size,
            column,
            src.as_ptr(),
            src.len(),
            dst.as_mut_ptr().cast::<u8>(),
            is_end,
        )
    };
    debug_assert!(written <= needed);
    Ok(written)
}
