// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! DNSSEC cryptographic primitives. Callers apply policy and work budgets first.

use aws_lc_rs::{digest, signature};

use crate::{Name, Policy, rdata::Dnskey};

pub(crate) fn supported_algorithm(algorithm: u8, policy: &Policy) -> bool {
    match algorithm {
        8 | 10 | 13 | 14 | 15 | 16 | 18 => true,
        5 | 7 => policy.sha1_signatures,
        _ => false,
    }
}

pub(crate) fn supported_digest(digest_type: u8, policy: &Policy) -> bool {
    match digest_type {
        2 | 4 => true,
        1 => policy.sha1_ds_digests,
        _ => false,
    }
}

pub(crate) fn digest_length(digest_type: u8) -> Option<usize> {
    match digest_type {
        1 => Some(20),
        2 => Some(32),
        4 => Some(48),
        _ => None,
    }
}

fn dnskey_bytes(key: &Dnskey) -> impl Iterator<Item = u8> + '_ {
    key.flags
        .to_be_bytes()
        .into_iter()
        .chain([key.protocol, key.algorithm])
        .chain(key.public_key.iter().copied())
}

/// RFC 4034 Appendix B's general checksum (algorithm 1 is unsupported).
pub(crate) fn key_tag(key: &Dnskey) -> u16 {
    let mut sum = 0_u32;
    for (position, byte) in dnskey_bytes(key).enumerate() {
        let value = u32::from(byte);
        sum = sum.wrapping_add(if position & 1 == 0 { value << 8 } else { value });
    }
    (sum.wrapping_add((sum >> 16) & 0xffff) & 0xffff) as u16
}

struct RsaKey<'a> {
    exponent: &'a [u8],
    modulus: &'a [u8],
    bits: usize,
}

fn strip_zeroes(bytes: &[u8]) -> &[u8] {
    let mut rest = bytes;
    while let Some((&0, tail)) = rest.split_first() {
        rest = tail;
    }
    rest
}

/// RFC 3110 section 2, with both exponent-length forms and unsigned components.
fn rsa_key(bytes: &[u8]) -> Option<RsaKey<'_>> {
    let (&length, rest) = bytes.split_first()?;
    let (length, rest) = if length == 0 {
        let (length, rest) = rest.split_at_checked(2)?;
        let [high, low] = length else { return None };
        (usize::from(u16::from_be_bytes([*high, *low])), rest)
    } else {
        (usize::from(length), rest)
    };
    let (exponent, modulus) = rest.split_at_checked(length)?;
    let exponent = strip_zeroes(exponent);
    let modulus = strip_zeroes(modulus);
    if exponent.is_empty() {
        return None;
    }
    let (&first, _) = modulus.split_first()?;
    let bits = modulus
        .len()
        .checked_mul(8)?
        .checked_sub(usize::try_from(first.leading_zeros()).ok()?)?;
    Some(RsaKey {
        exponent,
        modulus,
        bits,
    })
}

/// Only a parsed RSA modulus outside the supported range is unusable. A bad
/// shape still reaches the counted verification attempt and fails there.
pub(crate) fn key_usable(key: &Dnskey) -> bool {
    if !matches!(key.algorithm, 5 | 7 | 8 | 10) {
        return true;
    }
    rsa_key(&key.public_key).is_none_or(|key| (1024..=8192).contains(&key.bits))
}

pub(crate) fn verify(key: &Dnskey, message: &[u8], signed: &[u8]) -> bool {
    match key.algorithm {
        5 | 7 | 8 | 10 => {
            let Some(key_parts) = rsa_key(&key.public_key) else {
                return false;
            };
            if !(1024..=8192).contains(&key_parts.bits) {
                return false;
            }
            let parameters = match key.algorithm {
                5 | 7 => &signature::RSA_PKCS1_1024_8192_SHA1_FOR_LEGACY_USE_ONLY,
                8 => &signature::RSA_PKCS1_1024_8192_SHA256_FOR_LEGACY_USE_ONLY,
                10 => &signature::RSA_PKCS1_1024_8192_SHA512_FOR_LEGACY_USE_ONLY,
                _ => return false,
            };
            signature::RsaPublicKeyComponents {
                n: key_parts.modulus,
                e: key_parts.exponent,
            }
            .verify(parameters, message, signed)
            .is_ok()
        }
        13 => verify_ec(
            &key.public_key,
            64,
            &signature::ECDSA_P256_SHA256_FIXED,
            message,
            signed,
        ),
        14 => verify_ec(
            &key.public_key,
            96,
            &signature::ECDSA_P384_SHA384_FIXED,
            message,
            signed,
        ),
        15 => {
            key.public_key.len() == 32
                && signed.len() == 64
                && signature::UnparsedPublicKey::new(&signature::ED25519, &key.public_key)
                    .verify(message, signed)
                    .is_ok()
        }
        16 => verify_ed448(&key.public_key, message, signed),
        18 => {
            // DNSSEC uses raw ML-DSA-44 with empty context and no prehash.
            // Reject DER keys and other parameter sets before the backend.
            key.public_key.len() == 1312
                && signed.len() == 2420
                && signature::UnparsedPublicKey::new(&signature::ML_DSA_44, &key.public_key)
                    .verify(message, signed)
                    .is_ok()
        }
        _ => false,
    }
}

fn verify_ed448(key: &[u8], message: &[u8], signed: &[u8]) -> bool {
    use ed448_goldilocks_plus::{EdwardsPoint, Signature, VerifyingKey};

    let Ok(key_bytes) = <&[u8; 57]>::try_from(key) else {
        return false;
    };
    let Ok(signature_bytes) = <&[u8; 114]>::try_from(signed) else {
        return false;
    };
    // The backend rejects identity, torsion and noncanonical scalars. Its point
    // decoder permits alternate encodings, so require canonical round trips too.
    let Ok(key) = VerifyingKey::from_bytes(key_bytes) else {
        return false;
    };
    if key.to_edwards().compress().as_bytes() != key_bytes {
        return false;
    }
    let Ok(signature) = Signature::from_bytes(signature_bytes) else {
        return false;
    };
    let Some(r) = Option::<EdwardsPoint>::from(signature.r().decompress()) else {
        return false;
    };
    if r.compress() != signature.r() {
        return false;
    }
    // RFC 8080: pure Ed448, with an empty context and no prehash.
    key.verify_raw(&signature, message).is_ok()
}

fn verify_ec(
    key: &[u8],
    size: usize,
    algorithm: &'static dyn signature::VerificationAlgorithm,
    message: &[u8],
    signed: &[u8],
) -> bool {
    if key.len() != size || signed.len() != size {
        return false;
    }
    let mut point = vec![4];
    point.extend_from_slice(key);
    signature::UnparsedPublicKey::new(algorithm, point)
        .verify(message, signed)
        .is_ok()
}

/// The policy is applied by the DS selector; this primitive supports SHA-1 too.
pub(crate) fn ds_digest(owner: &Name, key: &Dnskey, digest_type: u8) -> Option<Vec<u8>> {
    let algorithm = match digest_type {
        1 => &digest::SHA1_FOR_LEGACY_USE_ONLY,
        2 => &digest::SHA256,
        4 => &digest::SHA384,
        _ => return None,
    };
    let mut input = owner.to_lowercase().as_wire().to_vec();
    input.extend(dnskey_bytes(key));
    Some(digest::digest(algorithm, &input).as_ref().to_vec())
}

/// RFC 5155 section 5. The caller enforces iteration and hash budgets.
pub(crate) fn nsec3_hash(owner: &Name, salt: &[u8], iterations: u16) -> Vec<u8> {
    let mut input = owner.to_lowercase().as_wire().to_vec();
    input.extend_from_slice(salt);
    let mut output = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, &input);
    for _ in 0..iterations {
        input.clear();
        input.extend_from_slice(output.as_ref());
        input.extend_from_slice(salt);
        output = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, &input);
    }
    output.as_ref().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ed448_key(seed: u8) -> (ed448_goldilocks_plus::SigningKey, Dnskey) {
        let signing = ed448_goldilocks_plus::SigningKey::try_from(&[seed; 57][..]).unwrap();
        let key = Dnskey {
            flags: 256,
            protocol: 3,
            algorithm: 16,
            public_key: signing.verifying_key().to_bytes().to_vec(),
        };
        (signing, key)
    }

    #[test]
    fn ed448_requires_exact_raw_lengths_and_authenticates_all_inputs() {
        let message = b"DNSSEC Ed448 length and mutation regression";
        let (signing, key) = ed448_key(7);
        let signed = signing.sign_raw(message).to_bytes();
        assert!(verify(&key, message, &signed));
        for length in [0, 1, 56, 58, 64, 114] {
            let mut malformed = key.clone();
            malformed.public_key.resize(length, 0);
            assert!(!verify(&malformed, message, &signed), "key length {length}");
        }
        for length in [0, 1, 57, 64, 113, 115, 228] {
            let mut malformed = signed.to_vec();
            malformed.resize(length, 0);
            assert!(
                !verify(&key, message, &malformed),
                "signature length {length}"
            );
        }
        assert!(!verify(&key, b"changed message", &signed));
        let (_, different_key) = ed448_key(8);
        assert!(!verify(&different_key, message, &signed));
        for index in [0, 28, 56, 57, 85, 113] {
            let mut changed = signed;
            changed[index] ^= 1;
            assert!(!verify(&key, message, &changed), "signature byte {index}");
        }
    }

    #[test]
    fn ed448_rejects_identity_small_order_and_noncanonical_points() {
        let message = b"DNSSEC Ed448 point encoding regression";
        let (signing, key) = ed448_key(9);
        let signed = signing.sign_raw(message).to_bytes();
        let mut identity = [0; 57];
        identity[0] = 1;
        // p - 1 encodes the order-two point (0, -1), where
        // p = 2^448 - 2^224 - 1. y = 0 gives the order-four points.
        let mut order_two = [0xff; 57];
        order_two[0] = 0xfe;
        order_two[28] = 0xfe;
        order_two[56] = 0;
        let mut negative_order_four = [0; 57];
        negative_order_four[56] = 0x80;
        let mut noncanonical_y = order_two;
        noncanonical_y[0] = 0xff; // y = p is not a field encoding.
        for point in [
            identity,
            order_two,
            [0; 57],
            negative_order_four,
            noncanonical_y,
            [0xff; 57],
        ] {
            let mut malformed = key.clone();
            malformed.public_key = point.to_vec();
            assert!(!verify(&malformed, message, &signed));
            let mut malformed = signed;
            malformed[..57].copy_from_slice(&point);
            assert!(!verify(&key, message, &malformed));
        }
        // The backend decoder ignores the seven unused bits. The DNSSEC
        // wrapper must reject these alternate encodings of valid points.
        for bit in 0..7 {
            let mut malformed = key.clone();
            malformed.public_key[56] |= 1 << bit;
            assert!(!verify(&malformed, message, &signed));
            let mut malformed = signed;
            malformed[56] |= 1 << bit;
            assert!(!verify(&key, message, &malformed));
        }
    }

    #[test]
    fn ed448_rejects_noncanonical_signature_scalars() {
        let message = b"DNSSEC Ed448 scalar regression";
        let (signing, key) = ed448_key(10);
        let signed = signing.sign_raw(message).to_bytes();
        let mut group_order = [0; 57];
        group_order[..56].copy_from_slice(&ed448_goldilocks_plus::ORDER.to_le_bytes());
        for scalar in [[0; 57], group_order, [0xff; 57]] {
            let mut malformed = signed;
            malformed[57..].copy_from_slice(&scalar);
            assert!(!verify(&key, message, &malformed));
        }
        let mut malformed = signed;
        malformed[113] = 1;
        assert!(!verify(&key, message, &malformed));
    }

    #[test]
    fn ed448_dnssec_uses_pure_mode_and_empty_context() {
        struct Prehashed;
        impl ed448_goldilocks_plus::PreHash for Prehashed {
            fn fill_bytes(&mut self, output: &mut [u8]) {
                // Independent Python hashlib SHAKE256(message, 64) output.
                output.copy_from_slice(&crate::test_vectors::hex(concat!(
                    "37ef8b414c85bf13f1d2c7f4dda13c132658f1c9de74176a3bf043261e6b621b3",
                    "a7a205e4d545f30d2aa4b8084c60ce22e126a0b237e2821581b85653764d630"
                )));
            }
        }

        let message = b"DNSSEC Ed448 domain separation regression";
        let (signing, key) = ed448_key(11);
        let verifier = signing.verifying_key();
        let pure = signing.sign_raw(message);
        assert!(verify(&key, message, &pure.to_bytes()));
        let empty_context = signing.sign_ctx(&[], message).unwrap();
        assert_eq!(pure, empty_context);
        let context = signing.sign_ctx(b"DNSSEC", message).unwrap();
        assert!(verifier.verify_ctx(&context, b"DNSSEC", message).is_ok());
        assert!(!verify(&key, message, &context.to_bytes()));
        let prehashed = signing.sign_prehashed(None, Prehashed).unwrap();
        assert!(
            verifier
                .verify_prehashed(&prehashed, None, Prehashed)
                .is_ok()
        );
        assert!(!verify(&key, message, &prehashed.to_bytes()));
    }

    #[test]
    fn support_is_explicit_and_sha1_policies_are_independent() {
        for value in 0_u8..=255 {
            let default = Policy::default();
            assert_eq!(
                supported_algorithm(value, &default),
                [8, 10, 13, 14, 15, 16, 18].contains(&value)
            );
            assert_eq!(
                supported_digest(value, &default),
                [1, 2, 4].contains(&value)
            );
            let signatures = Policy {
                sha1_signatures: true,
                ..default
            };
            assert_eq!(
                supported_algorithm(value, &signatures),
                [5, 7, 8, 10, 13, 14, 15, 16, 18].contains(&value)
            );
            assert_eq!(
                supported_digest(value, &signatures),
                supported_digest(value, &default)
            );
            let digests = Policy {
                sha1_ds_digests: false,
                ..default
            };
            assert_eq!(supported_digest(value, &digests), [2, 4].contains(&value));
            assert_eq!(
                supported_algorithm(value, &digests),
                supported_algorithm(value, &default)
            );
        }
    }

    #[test]
    fn rsa_length_forms_leading_zeroes_and_bit_lengths() {
        // RFC 3110 section 2 length fields (rfc3110.txt lines 125–143), plus
        // the handoff 6.5 requirement to tolerate and strip leading zeroes.
        // No signature or successful cryptographic vector is invented here.
        for prefix in [vec![3], vec![0, 0, 3]] {
            let mut bytes = prefix;
            bytes.extend_from_slice(&[0, 1, 1, 0, 0, 0x80]);
            bytes.extend_from_slice(&[0; 127]);
            let key = rsa_key(&bytes).unwrap();
            assert_eq!(key.exponent, [1, 1]);
            assert_eq!(key.modulus.len(), 128);
            assert_eq!(key.bits, 1024);
        }
        assert_eq!(rsa_key(&[1, 3, 1]).unwrap().bits, 1);
        assert_eq!(rsa_key(&[1, 3, 0x7f]).unwrap().bits, 7);
        for bytes in [
            &[][..],
            &[0],
            &[0, 0],
            &[0, 0, 0],
            &[1, 0, 1],
            &[1, 3, 0],
            &[2, 3],
        ] {
            assert!(rsa_key(bytes).is_none(), "{bytes:?}");
        }
    }

    #[test]
    fn malformed_shapes_fail_and_only_parsed_rsa_sizes_are_unusable() {
        let mut key = Dnskey {
            flags: 256,
            protocol: 3,
            algorithm: 8,
            public_key: vec![],
        };
        assert!(key_usable(&key));
        assert!(!verify(&key, &[], &[]));
        for bits in [1023_usize, 1024, 8192, 8193] {
            let octets = bits.div_ceil(8);
            let mut bytes = vec![1, 3];
            bytes.push(1 << ((bits - 1) % 8));
            bytes.extend(std::iter::repeat_n(0, octets - 1));
            key.public_key = bytes;
            assert_eq!(rsa_key(&key.public_key).unwrap().bits, bits);
            assert_eq!(key_usable(&key), (1024..=8192).contains(&bits));
        }
        for algorithm in [13, 14, 15, 16, 18, 255] {
            key.algorithm = algorithm;
            key.public_key.clear();
            assert!(key_usable(&key));
            assert!(!verify(&key, &[], &[]));
        }
    }
}
