// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The pure-Rust backend. Each primitive accepts exactly what the aws-lc-rs
//! backend accepts; the comments name the aws-lc rule each check reproduces.

use ed25519_dalek::Verifier as _;
use ml_dsa::{EncodedSignature, EncodedVerifyingKey, MlDsa44};
use rsa::{BoxedUint, Pkcs1v15Sign};
use sha1::Sha1;
use sha2::{Digest as _, Sha256, Sha384, Sha512};

use super::{Backend, Curve, Digest, Output, RsaHash};

pub(crate) struct RustCrypto;

impl Backend for RustCrypto {
    fn digest(algorithm: Digest, input: &[u8]) -> Output {
        match algorithm {
            Digest::Sha1 => Output::new(&Sha1::digest(input)),
            Digest::Sha256 => Output::new(&Sha256::digest(input)),
            Digest::Sha384 => Output::new(&Sha384::digest(input)),
        }
    }

    fn verify_rsa(
        hash: RsaHash,
        modulus: &[u8],
        exponent: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> bool {
        // aws-lc-rs rejects an empty component or one with a leading zero.
        if modulus.first().is_none_or(|byte| *byte == 0)
            || exponent.first().is_none_or(|byte| *byte == 0)
        {
            return false;
        }
        // `PublicKey::new_with_size_limits` requires an odd modulus in the
        // range and an odd exponent from 3 to 2^33 - 1, below the modulus.
        // aws-lc's public key check requires the same: an odd modulus, and an
        // odd exponent of 2 to 33 bits; its RSA_PKCS1_1024_8192 parameters
        // bound the modulus. Both then require a signature exactly as long as
        // the modulus, below it, and the exact EMSA-PKCS1-v1_5 encoding with
        // a NULL-parameter DigestInfo.
        let Ok(key) = rsa::PublicKey::new_with_size_limits(
            BoxedUint::from_be_slice_vartime(modulus),
            BoxedUint::from_be_slice_vartime(exponent),
            1024..=8192,
        ) else {
            return false;
        };
        let result = match hash {
            RsaHash::Sha1 => key.verify(
                Pkcs1v15Sign::new::<Sha1>(),
                &Sha1::digest(message),
                signature,
            ),
            RsaHash::Sha256 => key.verify(
                Pkcs1v15Sign::new::<Sha256>(),
                &Sha256::digest(message),
                signature,
            ),
            RsaHash::Sha512 => key.verify(
                Pkcs1v15Sign::new::<Sha512>(),
                &Sha512::digest(message),
                signature,
            ),
        };
        result.is_ok()
    }

    fn verify_ecdsa(curve: Curve, point: &[u8], message: &[u8], signature: &[u8]) -> bool {
        // Both backends require a point on the curve with reduced
        // coordinates, and 0 < r, s < n. Neither requires a low s.
        match curve {
            Curve::P256 => {
                let (Ok(key), Ok(signature)) = (
                    p256::ecdsa::VerifyingKey::from_sec1_bytes(point),
                    p256::ecdsa::Signature::from_slice(signature),
                ) else {
                    return false;
                };
                key.verify(message, &signature).is_ok()
            }
            Curve::P384 => {
                let (Ok(key), Ok(signature)) = (
                    p384::ecdsa::VerifyingKey::from_sec1_bytes(point),
                    p384::ecdsa::Signature::from_slice(signature),
                ) else {
                    return false;
                };
                key.verify(message, &signature).is_ok()
            }
        }
    }

    fn verify_ed25519(key: &[u8], message: &[u8], signature: &[u8]) -> bool {
        let (Ok(key), Ok(signature)) =
            (<&[u8; 32]>::try_from(key), <&[u8; 64]>::try_from(signature))
        else {
            return false;
        };
        let Ok(verifying) = ed25519_dalek::VerifyingKey::from_bytes(key) else {
            return false;
        };
        // aws-lc decodes the public key as RFC 8032 section 5.1.3 says: it
        // rejects y >= p, and x = 0 with the sign bit set. curve25519-dalek
        // reduces y and ignores the sign of a zero x, so a key that does not
        // re-encode to its own bytes is one aws-lc rejects.
        if verifying.to_edwards().compress().as_bytes() != key {
            return false;
        }
        // aws-lc requires s < L. Checked here so it holds whatever features
        // ed25519-dalek is built with: `legacy_compatibility` weakens its own
        // check to the top three bits.
        if !scalar_below_order(&signature[32..]) {
            return false;
        }
        // `verify`, not `verify_strict`: like aws-lc it compares R by its
        // encoding and accepts small-order keys and R.
        verifying
            .verify(message, &ed25519_dalek::Signature::from_bytes(signature))
            .is_ok()
    }

    fn verify_ml_dsa_44(key: &[u8], message: &[u8], signature: &[u8]) -> bool {
        let (Ok(key), Ok(signature)) = (
            EncodedVerifyingKey::<MlDsa44>::try_from(key),
            EncodedSignature::<MlDsa44>::try_from(signature),
        ) else {
            return false;
        };
        // Signature decoding rejects a non-canonical hint and a z out of range,
        // as FIPS 204 sigDecode and aws-lc do.
        let Some(signature) = ml_dsa::Signature::<MlDsa44>::decode(&signature) else {
            return false;
        };
        ml_dsa::VerifyingKey::<MlDsa44>::decode(&key).verify_with_context(message, &[], &signature)
    }
}

/// The Ed25519 group order L, little-endian.
const ED25519_ORDER: [u8; 32] = [
    0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9, 0xde, 0x14,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10,
];

/// Whether a 32-byte little-endian scalar is below L.
fn scalar_below_order(scalar: &[u8]) -> bool {
    scalar.len() == 32
        && scalar
            .iter()
            .rev()
            .zip(ED25519_ORDER.iter().rev())
            .find(|(s, l)| s != l)
            .is_some_and(|(s, l)| s < l)
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::Verifier as _;

    use super::{Backend, ED25519_ORDER, RustCrypto, scalar_below_order};

    #[test]
    fn scalar_compare_holds_at_the_order() {
        let mut below = ED25519_ORDER;
        below[0] -= 1;
        let mut above = ED25519_ORDER;
        above[0] += 1;
        // A difference only in the top byte must decide the result too.
        let mut top = [0; 32];
        top[31] = 0x11;
        let mut under_top = [0xff; 32];
        under_top[31] = 0x0f;
        assert!(scalar_below_order(&below));
        assert!(!scalar_below_order(&ED25519_ORDER));
        assert!(!scalar_below_order(&above));
        assert!(!scalar_below_order(&top));
        assert!(scalar_below_order(&under_top));
        assert!(scalar_below_order(&[0; 32]));
        assert!(!scalar_below_order(&[0xff; 32]));
        assert!(!scalar_below_order(&below[..31]));
    }

    /// The identity point with a forged signature that holds for any message:
    /// R is the identity's encoding and s = 0.
    fn identity_case(key: [u8; 32]) -> (ed25519_dalek::VerifyingKey, ed25519_dalek::Signature) {
        let mut signature = [0; 64];
        signature[0] = 1;
        (
            ed25519_dalek::VerifyingKey::from_bytes(&key).unwrap(),
            ed25519_dalek::Signature::from_bytes(&signature),
        )
    }

    #[test]
    fn dalek_alone_accepts_key_encodings_aws_lc_rejects() {
        // y = p + 1, the identity with an unreduced y; and x = -0.
        let mut y_plus_p = [0xff; 32];
        y_plus_p[0] = 0xee;
        y_plus_p[31] = 0x7f;
        let mut negative_zero = [0; 32];
        negative_zero[0] = 1;
        negative_zero[31] = 0x80;
        for key in [y_plus_p, negative_zero] {
            let (verifying, signature) = identity_case(key);
            assert!(verifying.verify(b"any", &signature).is_ok());
            assert!(!RustCrypto::verify_ed25519(
                &key,
                b"any",
                &signature.to_bytes()
            ));
        }
    }

    #[test]
    fn verify_strict_would_reject_a_small_order_key_aws_lc_accepts() {
        let mut identity = [0; 32];
        identity[0] = 1;
        let (verifying, signature) = identity_case(identity);
        assert!(verifying.verify_strict(b"any", &signature).is_err());
        assert!(RustCrypto::verify_ed25519(
            &identity,
            b"any",
            &signature.to_bytes()
        ));
    }
}
