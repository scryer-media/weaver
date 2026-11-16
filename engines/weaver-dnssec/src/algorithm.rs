// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! DNSSEC cryptographic primitives. Callers apply policy and work budgets first.

use crate::{
    Name, Policy,
    crypto::{self, Backend, Curve, Digest, RsaHash},
    rdata::Dnskey,
};

pub(crate) fn supported_algorithm(algorithm: u8, policy: &Policy) -> bool {
    match algorithm {
        8 | 10 | 13 | 14 | 15 | 18 => true,
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
    verify_with::<crypto::Selected>(key, message, signed)
}

pub(crate) fn verify_with<B: Backend>(key: &Dnskey, message: &[u8], signed: &[u8]) -> bool {
    match key.algorithm {
        5 | 7 | 8 | 10 => {
            let Some(key_parts) = rsa_key(&key.public_key) else {
                return false;
            };
            if !(1024..=8192).contains(&key_parts.bits) {
                return false;
            }
            let hash = match key.algorithm {
                5 | 7 => RsaHash::Sha1,
                8 => RsaHash::Sha256,
                10 => RsaHash::Sha512,
                _ => return false,
            };
            B::verify_rsa(hash, key_parts.modulus, key_parts.exponent, message, signed)
        }
        13 => verify_ec::<B>(&key.public_key, 64, Curve::P256, message, signed),
        14 => verify_ec::<B>(&key.public_key, 96, Curve::P384, message, signed),
        15 => {
            key.public_key.len() == 32
                && signed.len() == 64
                && B::verify_ed25519(&key.public_key, message, signed)
        }
        18 => {
            // DNSSEC uses raw ML-DSA-44 with empty context and no prehash.
            // Reject DER keys and other parameter sets before the backend.
            key.public_key.len() == 1312
                && signed.len() == 2420
                && B::verify_ml_dsa_44(&key.public_key, message, signed)
        }
        _ => false,
    }
}

fn verify_ec<B: Backend>(
    key: &[u8],
    size: usize,
    curve: Curve,
    message: &[u8],
    signed: &[u8],
) -> bool {
    if key.len() != size || signed.len() != size {
        return false;
    }
    let mut point = vec![4];
    point.extend_from_slice(key);
    B::verify_ecdsa(curve, &point, message, signed)
}

/// The policy is applied by the DS selector; this primitive supports SHA-1 too.
pub(crate) fn ds_digest(owner: &Name, key: &Dnskey, digest_type: u8) -> Option<Vec<u8>> {
    let algorithm = match digest_type {
        1 => Digest::Sha1,
        2 => Digest::Sha256,
        4 => Digest::Sha384,
        _ => return None,
    };
    let mut input = owner.to_lowercase().as_wire().to_vec();
    input.extend(dnskey_bytes(key));
    Some(
        crypto::Selected::digest(algorithm, &input)
            .as_ref()
            .to_vec(),
    )
}

/// RFC 5155 section 5. The caller enforces iteration and hash budgets.
pub(crate) fn nsec3_hash(owner: &Name, salt: &[u8], iterations: u16) -> Vec<u8> {
    let mut input = owner.to_lowercase().as_wire().to_vec();
    input.extend_from_slice(salt);
    let mut output = crypto::Selected::digest(Digest::Sha1, &input);
    for _ in 0..iterations {
        input.clear();
        input.extend_from_slice(output.as_ref());
        input.extend_from_slice(salt);
        output = crypto::Selected::digest(Digest::Sha1, &input);
    }
    output.as_ref().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn support_is_explicit_and_sha1_policies_are_independent() {
        for value in 0_u8..=255 {
            let default = Policy::default();
            assert_eq!(
                supported_algorithm(value, &default),
                [8, 10, 13, 14, 15, 18].contains(&value)
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
                [5, 7, 8, 10, 13, 14, 15, 18].contains(&value)
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
