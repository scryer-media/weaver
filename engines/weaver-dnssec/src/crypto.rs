// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The cryptographic backend seam, and the only place a backend is chosen.
//!
//! `algorithm` keeps every DNSSEC policy and shape check and calls these
//! primitives with inputs it has already checked. A backend only hashes and
//! verifies. The `aws-lc-rs` feature selects aws-lc-rs and wins when both
//! features are on; `rust-crypto` alone selects the pure-Rust backend.

#[cfg(not(any(feature = "aws-lc-rs", feature = "rust-crypto")))]
compile_error!(
    "weaver-dnssec needs a crypto backend: enable the `aws-lc-rs` feature (the default) \
     or the `rust-crypto` feature"
);

#[cfg(feature = "aws-lc-rs")]
mod aws_lc;
#[cfg(all(test, feature = "aws-lc-rs", feature = "rust-crypto"))]
mod parity_tests;
#[cfg(all(feature = "rust-crypto", any(test, not(feature = "aws-lc-rs"))))]
mod pure;

#[cfg(feature = "aws-lc-rs")]
pub(crate) use aws_lc::AwsLc as Selected;
#[cfg(all(feature = "rust-crypto", not(feature = "aws-lc-rs")))]
pub(crate) use pure::RustCrypto as Selected;

/// One-shot digests used for DS records and NSEC3 hashing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Digest {
    Sha1,
    Sha256,
    Sha384,
}

/// The digest inside an RSASSA-PKCS1-v1_5 signature.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RsaHash {
    Sha1,
    Sha256,
    Sha512,
}

/// ECDSA curves, each with its own hash: P-256 with SHA-256, P-384 with SHA-384.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Curve {
    P256,
    P384,
}

/// A digest value, at most SHA-384 long.
pub(crate) struct Output {
    bytes: [u8; 48],
    len: usize,
}

impl Output {
    fn new(digest: &[u8]) -> Self {
        let mut bytes = [0; 48];
        let len = digest.len().min(bytes.len());
        bytes[..len].copy_from_slice(&digest[..len]);
        Self { bytes, len }
    }
}

impl AsRef<[u8]> for Output {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// The primitives `algorithm` needs. Each verify returns whether the
/// signature is valid; every malformed input is simply invalid.
pub(crate) trait Backend {
    fn digest(algorithm: Digest, input: &[u8]) -> Output;

    /// RSASSA-PKCS1-v1_5 with a 1024 to 8192 bit modulus. `modulus` and
    /// `exponent` are unsigned big-endian integers without leading zeroes.
    fn verify_rsa(
        hash: RsaHash,
        modulus: &[u8],
        exponent: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> bool;

    /// ECDSA over an uncompressed SEC1 point, with a fixed-size `r || s` signature.
    fn verify_ecdsa(curve: Curve, point: &[u8], message: &[u8], signature: &[u8]) -> bool;

    /// Ed25519 (RFC 8032, pure, no context).
    fn verify_ed25519(key: &[u8], message: &[u8], signature: &[u8]) -> bool;

    /// Raw ML-DSA-44 (FIPS 204) with an empty context and no prehash.
    fn verify_ml_dsa_44(key: &[u8], message: &[u8], signature: &[u8]) -> bool;
}
