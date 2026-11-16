// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Both backends must accept and reject exactly the same inputs. Every case
//! runs through `algorithm::verify_with` (policy and shape checks included)
//! under each backend; the results must match, and where a case states an
//! expected result, both must give it.

use aws_lc_rs::signature::{self, KeyPair};

use super::{Backend, Digest, RsaHash, aws_lc::AwsLc, pure::RustCrypto};
use crate::{algorithm, canonical::signed_data, rdata::Dnskey, test_vectors};

const MESSAGE: &[u8] = b"weaver-dnssec backend parity";

/// P-256 and P-384 group orders, and the Ed25519 order L (little-endian).
const P256_N: &str = "ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551";
const P384_N: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffc7634d81f4372ddf581a0db248b0a77aecec196accc52973";
const ED25519_L_LE: &str = "edd3f55c1a631258d69cf7a2def9de1400000000000000000000000000000010";

struct Case {
    name: String,
    key: Dnskey,
    message: Vec<u8>,
    signature: Vec<u8>,
    expected: Option<bool>,
}

fn key(algorithm: u8, public_key: Vec<u8>) -> Dnskey {
    Dnskey {
        flags: 257,
        protocol: 3,
        algorithm,
        public_key,
    }
}

fn case(name: &str, key: Dnskey, message: &[u8], signature: Vec<u8>, expected: bool) -> Case {
    Case {
        name: name.to_owned(),
        key,
        message: message.to_vec(),
        signature,
        expected: Some(expected),
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn rsa_dnskey(exponent: &[u8], modulus: &[u8]) -> Vec<u8> {
    let mut bytes = vec![u8::try_from(exponent.len()).unwrap()];
    bytes.extend_from_slice(exponent);
    bytes.extend_from_slice(modulus);
    bytes
}

/// Big-endian `a - b`, for `a >= b` of equal length.
fn sub_be(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut out = a.to_vec();
    let mut borrow = 0_i16;
    for (out, b) in out.iter_mut().zip(b).rev() {
        let value = i16::from(*out) - i16::from(*b) - borrow;
        borrow = i16::from(value < 0);
        *out = u8::try_from(value.rem_euclid(256)).unwrap();
    }
    assert_eq!(borrow, 0);
    out
}

/// Little-endian `a + b`, of equal length, dropping the carry out.
fn add_le(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut carry = 0_u16;
    a.iter()
        .zip(b)
        .map(|(a, b)| {
            let value = u16::from(*a) + u16::from(*b) + carry;
            carry = value >> 8;
            u8::try_from(value & 0xff).unwrap()
        })
        .collect()
}

/// Fixed keys and signatures: an Ed25519 key from a fixed seed (Ed25519
/// signing is deterministic), and the stored vectors in `parity_vectors.txt`.
fn fixed() -> Vec<Case> {
    let seed: Vec<u8> = (0..32).collect();
    let pair = signature::Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
    let mut cases = vec![case(
        "seeded-ed25519",
        key(15, pair.public_key().as_ref().to_vec()),
        MESSAGE,
        pair.sign(MESSAGE).as_ref().to_vec(),
        true,
    )];
    cases.extend(
        include_str!("parity_vectors.txt")
            .lines()
            .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
            .map(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                let [label, algorithm, public, signed, valid] = fields[..] else {
                    panic!("bad vector line {line}");
                };
                case(
                    label,
                    key(algorithm.parse().unwrap(), test_vectors::hex(public)),
                    MESSAGE,
                    test_vectors::hex(signed),
                    valid == "true",
                )
            }),
    );
    cases
}

fn published() -> Vec<Case> {
    let mut cases = Vec::new();
    for (name, (dnskey, _, set)) in [
        ("rfc6605-p256", test_vectors::p256()),
        ("rfc6605-p384", test_vectors::p384()),
    ] {
        let signed = signed_data(&set, &set.rrsigs[0]).unwrap().bytes;
        cases.push(case(
            name,
            dnskey,
            &signed,
            set.rrsigs[0].signature.clone(),
            true,
        ));
    }
    let sets = test_vectors::parse(include_str!("../ml_dsa_vectors.txt"));
    let dnskey = sets
        .iter()
        .find_map(|set| match &set.records[0].kind {
            crate::rdata::RdataKind::Dnskey(value) => Some(value.clone()),
            _ => None,
        })
        .unwrap();
    let set = sets
        .iter()
        .find(|set| set.rtype == crate::RecordType::MX)
        .unwrap();
    let signed = signed_data(set, &set.rrsigs[0]).unwrap().bytes;
    cases.push(case(
        "draft-mldsa-44",
        dnskey,
        &signed,
        set.rrsigs[0].signature.clone(),
        true,
    ));
    cases
}

fn openssl_rsa() -> Vec<Case> {
    include_str!("parity_rsa_vectors.txt")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .flat_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            let [label, hash, exponent, modulus, signed, valid] = fields[..] else {
                panic!("bad vector line {line}");
            };
            let algorithms: &[u8] = match hash {
                "sha1" => &[5, 7],
                "sha256" => &[8],
                "sha512" => &[10],
                _ => panic!("bad hash {hash}"),
            };
            let dnskey = rsa_dnskey(&test_vectors::hex(exponent), &test_vectors::hex(modulus));
            let signed = test_vectors::hex(signed);
            let valid = valid == "true";
            algorithms
                .iter()
                .map(|alg| {
                    case(
                        &format!("openssl-{label}-alg{alg}"),
                        key(*alg, dnskey.clone()),
                        MESSAGE,
                        signed.clone(),
                        valid,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Corruptions every valid case must fail under both backends, plus
/// algorithm-specific edge shapes whose result both must share.
fn mutations(valid: &Case) -> Vec<Case> {
    let mut out = Vec::new();
    let mut push = |label: &str, key: Dnskey, message: Vec<u8>, signature: Vec<u8>, expected| {
        out.push(Case {
            name: format!("{}/{label}", valid.name),
            key,
            message,
            signature,
            expected,
        });
    };
    let (k, m, s) = (&valid.key, &valid.message, &valid.signature);
    let flip = |bytes: &[u8], index: usize| {
        let mut bytes = bytes.to_vec();
        bytes[index] ^= 1;
        bytes
    };
    let reject = Some(false);
    push("message-byte", k.clone(), flip(m, 0), s.clone(), reject);
    push("message-empty", k.clone(), vec![], s.clone(), reject);
    let mut longer = m.clone();
    longer.push(0);
    push("message-longer", k.clone(), longer, s.clone(), reject);
    push("signature-first", k.clone(), m.clone(), flip(s, 0), reject);
    push(
        "signature-last",
        k.clone(),
        m.clone(),
        flip(s, s.len() - 1),
        reject,
    );
    push(
        "signature-short",
        k.clone(),
        m.clone(),
        s[..s.len() - 1].to_vec(),
        reject,
    );
    let mut padded = vec![0];
    padded.extend_from_slice(s);
    push(
        "signature-leading-zero",
        k.clone(),
        m.clone(),
        padded,
        reject,
    );
    let mut padded = s.clone();
    padded.push(0);
    push(
        "signature-trailing-zero",
        k.clone(),
        m.clone(),
        padded,
        reject,
    );
    push(
        "signature-zero",
        k.clone(),
        m.clone(),
        vec![0; s.len()],
        reject,
    );
    push(
        "signature-ones",
        k.clone(),
        m.clone(),
        vec![0xff; s.len()],
        reject,
    );
    push("signature-empty", k.clone(), m.clone(), vec![], reject);
    let mut changed = k.clone();
    let last = changed.public_key.len() - 1;
    changed.public_key[last] ^= 1;
    push("key-last", changed, m.clone(), s.clone(), reject);
    let mut changed = k.clone();
    changed.public_key.pop();
    push("key-short", changed, m.clone(), s.clone(), reject);

    match k.algorithm {
        5 | 7 | 8 | 10 => {
            let (exponent, modulus) = {
                let length = usize::from(k.public_key[0]);
                (
                    k.public_key[1..=length].to_vec(),
                    k.public_key[length + 1..].to_vec(),
                )
            };
            // These reuse the original signature, so a backend could reject
            // them for the signature alone. `parity_rsa_vectors.txt` holds a
            // genuine signature for e = 1, 2, 65538, 2^32 + 1, 2^64 + 1 and
            // an even modulus, which only the key check rejects.
            for (label, value) in [
                ("e-1", vec![1]),
                ("e-2", vec![2]),
                ("e-3", vec![3]),
                ("e-even", vec![1, 0, 2]),
                ("e-2p32p1", vec![1, 0, 0, 0, 1]),
                ("e-2p33m1", vec![1, 0xff, 0xff, 0xff, 0xff]),
                ("e-2p33p1", vec![2, 0, 0, 0, 1]),
                ("e-2p64p1", vec![1, 0, 0, 0, 0, 0, 0, 0, 1]),
            ] {
                if value == exponent {
                    continue;
                }
                push(
                    label,
                    key(k.algorithm, rsa_dnskey(&value, &modulus)),
                    m.clone(),
                    s.clone(),
                    reject,
                );
            }
            let mut long_form = vec![0, 0, u8::try_from(exponent.len()).unwrap()];
            long_form.extend_from_slice(&exponent);
            long_form.extend_from_slice(&modulus);
            push(
                "e-long-length-form",
                key(k.algorithm, long_form),
                m.clone(),
                s.clone(),
                valid.expected,
            );
            let mut zero_padded = vec![u8::try_from(exponent.len() + 1).unwrap(), 0];
            zero_padded.extend_from_slice(&exponent);
            zero_padded.push(0);
            zero_padded.extend_from_slice(&modulus);
            push(
                "leading-zeroes-stripped",
                key(k.algorithm, zero_padded),
                m.clone(),
                s.clone(),
                valid.expected,
            );
            push(
                "modulus-even",
                key(
                    k.algorithm,
                    rsa_dnskey(&exponent, &flip(&modulus, modulus.len() - 1)),
                ),
                m.clone(),
                s.clone(),
                reject,
            );
            push(
                "signature-is-modulus",
                k.clone(),
                m.clone(),
                modulus.clone(),
                reject,
            );
            let other = if k.algorithm == 10 { 8 } else { 10 };
            push(
                "other-hash",
                key(other, k.public_key.clone()),
                m.clone(),
                s.clone(),
                reject,
            );
        }
        13 | 14 => {
            let (n, size) = if k.algorithm == 13 {
                (test_vectors::hex(P256_N), 32)
            } else {
                (test_vectors::hex(P384_N), 48)
            };
            let (r, sv) = s.split_at(size);
            let join = |r: &[u8], s: &[u8]| [r, s].concat();
            let zero = vec![0; size];
            let one = {
                let mut one = vec![0; size];
                one[size - 1] = 1;
                one
            };
            // No low-s rule in either backend: n - s verifies too.
            push(
                "high-s",
                k.clone(),
                m.clone(),
                join(r, &sub_be(&n, sv)),
                valid.expected,
            );
            push("r-zero", k.clone(), m.clone(), join(&zero, sv), reject);
            push("s-zero", k.clone(), m.clone(), join(r, &zero), reject);
            push("r-n", k.clone(), m.clone(), join(&n, sv), reject);
            push("s-n", k.clone(), m.clone(), join(r, &n), reject);
            push(
                "r-n-minus-1",
                k.clone(),
                m.clone(),
                join(&sub_be(&n, &one), sv),
                reject,
            );
            push(
                "s-n-minus-1",
                k.clone(),
                m.clone(),
                join(r, &sub_be(&n, &one)),
                reject,
            );
            let mut off_curve = k.clone();
            let last = off_curve.public_key.len() - 1;
            off_curve.public_key[last] ^= 2;
            push("point-off-curve", off_curve, m.clone(), s.clone(), reject);
            let mut zero_point = k.clone();
            zero_point.public_key.fill(0);
            push("point-zero", zero_point, m.clone(), s.clone(), reject);
            let other = if k.algorithm == 13 { 14 } else { 13 };
            push(
                "other-curve",
                key(other, k.public_key.clone()),
                m.clone(),
                s.clone(),
                reject,
            );
        }
        15 => {
            let l = test_vectors::hex(ED25519_L_LE);
            let (r, sv) = s.split_at(32);
            // s + L passes a check of the top three bits alone (dalek's
            // `legacy_compatibility`), so only a full s < L compare rejects it.
            let s_plus_l = add_le(sv, &l);
            assert_eq!(
                s_plus_l[31] & 0xe0,
                0,
                "{}: s + L sets a top bit",
                valid.name
            );
            push(
                "s-plus-l",
                k.clone(),
                m.clone(),
                [r, &s_plus_l].concat(),
                reject,
            );
            let mut top = s.clone();
            top[63] |= 0xe0;
            push("s-top-bits", k.clone(), m.clone(), top, reject);
            push("s-l", k.clone(), m.clone(), [r, &l[..]].concat(), reject);
        }
        18 => {
            // ML-DSA-44: c~ is 32 bytes, z is 2304, then 80 hint indices and 4 cuts.
            let hint = 32 + 2304;
            push("c-tilde", k.clone(), m.clone(), flip(s, 5), reject);
            let mut z = s.clone();
            z[32] = 0;
            z[33] = 0;
            z[34] &= 0xfc;
            push("z-out-of-range", k.clone(), m.clone(), z, reject);
            let mut cuts = s.clone();
            cuts.swap(hint + 80, hint + 83);
            if cuts[hint + 80] != s[hint + 80] {
                push(
                    "hint-cuts-not-monotonic",
                    k.clone(),
                    m.clone(),
                    cuts,
                    reject,
                );
            }
            let mut trailing = s.clone();
            trailing[hint + 79] = trailing[hint + 79].wrapping_add(1);
            push(
                "hint-trailing-index",
                k.clone(),
                m.clone(),
                trailing,
                reject,
            );
            let mut order = s.clone();
            if order[hint + 83] >= 2 {
                order.swap(hint, hint + 1);
                push(
                    "hint-indices-not-increasing",
                    k.clone(),
                    m.clone(),
                    order,
                    reject,
                );
            }
            let mut over = s.clone();
            over[hint + 83] = 81;
            push("hint-cut-over-omega", k.clone(), m.clone(), over, reject);
        }
        _ => {}
    }
    out
}

/// Hand-built Ed25519 shapes: the identity is a small-order key that both
/// accept with a forged signature; its non-canonical encodings aws-lc rejects.
fn ed25519_edges() -> Vec<Case> {
    let identity = {
        let mut bytes = vec![0; 32];
        bytes[0] = 1;
        bytes
    };
    // [0]B - [k]O = O, whose encoding is the identity.
    let forged = [identity.clone(), vec![0; 32]].concat();
    let mut y_plus_p = vec![0xff; 32];
    y_plus_p[0] = 0xee;
    y_plus_p[31] = 0x7f;
    let mut negative_zero = identity.clone();
    negative_zero[31] = 0x80;
    vec![
        case(
            "ed25519-identity-key",
            key(15, identity.clone()),
            MESSAGE,
            forged.clone(),
            true,
        ),
        case(
            "ed25519-identity-y-plus-p",
            key(15, y_plus_p.clone()),
            MESSAGE,
            forged.clone(),
            false,
        ),
        case(
            "ed25519-identity-negative-zero",
            key(15, negative_zero),
            MESSAGE,
            forged.clone(),
            false,
        ),
        case(
            "ed25519-non-canonical-r",
            key(15, identity),
            MESSAGE,
            [y_plus_p, vec![0; 32]].concat(),
            false,
        ),
    ]
}

#[test]
fn backends_agree_on_every_signature_case() {
    let mut cases = fixed();
    cases.extend(published());
    cases.extend(openssl_rsa());
    let mutated: Vec<_> = cases
        .iter()
        .filter(|case| case.expected == Some(true))
        .flat_map(mutations)
        .collect();
    // The forged identity-key signature holds for every message, so it
    // gets no mutations.
    cases.extend(ed25519_edges());
    let total = cases.len() + mutated.len();
    let mut failures = Vec::new();
    for case in cases.iter().chain(&mutated) {
        let aws = algorithm::verify_with::<AwsLc>(&case.key, &case.message, &case.signature);
        let pure = algorithm::verify_with::<RustCrypto>(&case.key, &case.message, &case.signature);
        if aws != pure || case.expected.is_some_and(|expected| expected != aws) {
            failures.push(format!(
                "{}: aws-lc-rs {aws}, rust-crypto {pure}, expected {:?}\n  \
                 algorithm {} key {}\n  message {}\n  signature {}",
                case.name,
                case.expected,
                case.key.algorithm,
                to_hex(&case.key.public_key),
                to_hex(&case.message),
                to_hex(&case.signature),
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {total} cases differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
    // Every algorithm family contributed valid cases.
    for alg in [5, 7, 8, 10, 13, 14, 15, 18] {
        assert!(
            cases
                .iter()
                .any(|case| case.key.algorithm == alg && case.expected == Some(true)),
            "no valid case for {alg}"
        );
    }
}

#[test]
fn backends_agree_at_the_rsa_seam() {
    // Shapes `algorithm` filters out before the backend: both backends must
    // still reject them alone.
    for case in openssl_rsa() {
        let length = usize::from(case.key.public_key[0]);
        let exponent = &case.key.public_key[1..=length];
        let modulus = &case.key.public_key[length + 1..];
        let hash = match case.key.algorithm {
            5 | 7 => RsaHash::Sha1,
            8 => RsaHash::Sha256,
            _ => RsaHash::Sha512,
        };
        let run = |exponent: &[u8], modulus: &[u8]| {
            let aws = AwsLc::verify_rsa(hash, modulus, exponent, &case.message, &case.signature);
            let pure =
                RustCrypto::verify_rsa(hash, modulus, exponent, &case.message, &case.signature);
            assert_eq!(aws, pure, "{}", case.name);
            aws
        };
        assert_eq!(
            run(exponent, modulus),
            case.expected.unwrap(),
            "{}",
            case.name
        );
        assert!(!run(&[&[0], exponent].concat(), modulus), "{}", case.name);
        assert!(!run(exponent, &[&[0], modulus].concat()), "{}", case.name);
        assert!(!run(&[], modulus), "{}", case.name);
        assert!(!run(exponent, &[]), "{}", case.name);
    }
}

#[test]
fn backends_agree_on_digests() {
    let input: Vec<u8> = (0..1024_u32).map(|i| (i * 31 + 7) as u8).collect();
    for algorithm in [Digest::Sha1, Digest::Sha256, Digest::Sha384] {
        for length in (0..=200).chain([255, 256, 1000, 1024]) {
            assert_eq!(
                AwsLc::digest(algorithm, &input[..length]).as_ref(),
                RustCrypto::digest(algorithm, &input[..length]).as_ref(),
                "{algorithm:?} over {length} bytes"
            );
        }
    }
}

/// One verify per algorithm per backend, timed. Run by hand, optimised:
/// `cargo nextest run --locked --release -p weaver-dnssec --features rust-crypto,test-support
/// --run-ignored only --no-capture -E 'test(verify_cost)'`.
#[test]
#[ignore = "a measurement, not a check"]
fn verify_cost() {
    let mut cases = fixed();
    cases.extend(openssl_rsa());
    for label in [
        "openssl-rsa2048-sha256-alg8",
        "openssl-rsa8192-sha256-alg8",
        "openssl-p256",
        "openssl-p384",
        "seeded-ed25519",
        "openssl-ml-dsa-44",
    ] {
        let case = cases.iter().find(|case| case.name == label).unwrap();
        let time = |verify: fn(&Dnskey, &[u8], &[u8]) -> bool| {
            let start = std::time::Instant::now();
            let mut runs = 0_u32;
            while runs < 20 || start.elapsed().as_millis() < 300 {
                assert!(verify(
                    std::hint::black_box(&case.key),
                    &case.message,
                    &case.signature
                ));
                runs += 1;
            }
            start.elapsed().as_nanos() / u128::from(runs)
        };
        let aws = time(algorithm::verify_with::<AwsLc>);
        let pure = time(algorithm::verify_with::<RustCrypto>);
        eprintln!("{label}: aws-lc-rs {aws} ns, rust-crypto {pure} ns");
    }
}
