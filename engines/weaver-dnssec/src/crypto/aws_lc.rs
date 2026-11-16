// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The aws-lc-rs backend.

use aws_lc_rs::{digest, signature};

use super::{Backend, Curve, Digest, Output, RsaHash};

pub(crate) struct AwsLc;

impl Backend for AwsLc {
    fn digest(algorithm: Digest, input: &[u8]) -> Output {
        let algorithm = match algorithm {
            Digest::Sha1 => &digest::SHA1_FOR_LEGACY_USE_ONLY,
            Digest::Sha256 => &digest::SHA256,
            Digest::Sha384 => &digest::SHA384,
        };
        Output::new(digest::digest(algorithm, input).as_ref())
    }

    fn verify_rsa(
        hash: RsaHash,
        modulus: &[u8],
        exponent: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> bool {
        let parameters = match hash {
            RsaHash::Sha1 => &signature::RSA_PKCS1_1024_8192_SHA1_FOR_LEGACY_USE_ONLY,
            RsaHash::Sha256 => &signature::RSA_PKCS1_1024_8192_SHA256_FOR_LEGACY_USE_ONLY,
            RsaHash::Sha512 => &signature::RSA_PKCS1_1024_8192_SHA512_FOR_LEGACY_USE_ONLY,
        };
        signature::RsaPublicKeyComponents {
            n: modulus,
            e: exponent,
        }
        .verify(parameters, message, signature)
        .is_ok()
    }

    fn verify_ecdsa(curve: Curve, point: &[u8], message: &[u8], signature: &[u8]) -> bool {
        let algorithm: &'static dyn signature::VerificationAlgorithm = match curve {
            Curve::P256 => &signature::ECDSA_P256_SHA256_FIXED,
            Curve::P384 => &signature::ECDSA_P384_SHA384_FIXED,
        };
        signature::UnparsedPublicKey::new(algorithm, point)
            .verify(message, signature)
            .is_ok()
    }

    fn verify_ed25519(key: &[u8], message: &[u8], signature: &[u8]) -> bool {
        signature::UnparsedPublicKey::new(&signature::ED25519, key)
            .verify(message, signature)
            .is_ok()
    }

    fn verify_ml_dsa_44(key: &[u8], message: &[u8], signature: &[u8]) -> bool {
        signature::UnparsedPublicKey::new(&signature::ML_DSA_44, key)
            .verify(message, signature)
            .is_ok()
    }
}
