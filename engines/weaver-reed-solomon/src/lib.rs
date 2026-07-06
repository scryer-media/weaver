//! Shared Reed-Solomon finite-field kernels for Weaver repair engines.
//!
//! `gf` and `gf_simd` expose the existing GF(2^16) PAR2/RAR5 arithmetic and
//! SIMD multiply-accumulate kernels. RAR-specific coders live in separate
//! modules so PAR2 matrix semantics stay unchanged.

pub mod gf;
pub mod gf_simd;
#[cfg(target_os = "macos")]
pub mod metal_gf16;
pub mod rar3;
pub mod rar5;
