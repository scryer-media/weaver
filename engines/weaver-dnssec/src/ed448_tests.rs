// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Independent RFC 8032 / RFC 8080 vectors; no production signing code is used.

use crate::canonical::signed_data;
use crate::limits::{
    MAX_CLOCK_SKEW_SECONDS, MAX_SIGNATURE_CHECKS, MAX_SIGNATURE_CHECKS_PER_RRSET,
    MIN_CLOCK_SKEW_SECONDS,
};
use crate::rdata::Dnskey;
use crate::rrset::{SignatureLimit, Verified, WorkBudget, verify_rrset};
use crate::wire::Rrset;
use crate::{BogusReason, Name, Policy, RecordType, algorithm, test_vectors};

fn field(vector: &str, field: &str) -> Vec<u8> {
    let section = include_str!("ed448_vectors.txt")
        .split(&format!("[{vector}]\n"))
        .nth(1)
        .unwrap()
        .split("\n\n")
        .next()
        .unwrap();
    test_vectors::hex(
        section
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{field} ")))
            .unwrap(),
    )
}

fn key(public_key: Vec<u8>) -> Dnskey {
    Dnskey {
        flags: 257,
        protocol: 3,
        algorithm: 16,
        public_key,
    }
}

fn example(vector: &str) -> (Dnskey, Rrset) {
    let mut set = test_vectors::parse(concat!(
        "example.com. 3600 IN MX 10 mail.example.com.\n",
        "example.com. 3600 IN RRSIG MX 16 2 3600 ",
        "1440021600 1438207200 0 example.com. AA=="
    ))
    .pop()
    .unwrap();
    assert_eq!(set.rtype, RecordType::MX);
    let key = key(field(vector, "PUBLIC_KEY"));
    set.rrsigs[0].key_tag = algorithm::key_tag(&key);
    set.rrsigs[0].signature = field(vector, "CORRECTED_SIGNATURE");
    (key, set)
}

fn check(set: &Rrset, key: &Dnskey, now: u64) -> (Result<Verified, BogusReason>, usize) {
    let mut budget = WorkBudget::default();
    let result = verify_rrset(
        set,
        &Name::from_text("example.com.").unwrap(),
        std::slice::from_ref(key),
        now,
        Policy::default(),
        &mut budget,
        SignatureLimit::CachedKeys(&mut 0),
    );
    assert_eq!(budget.ds_digests, 0);
    assert_eq!(budget.nsec3_hashes, 0);
    (result, budget.signatures)
}

#[test]
fn rfc8032_empty_and_short_messages_use_pure_ed448_with_empty_context() {
    for vector in ["RFC8032_BLANK", "RFC8032_ONE", "RFC8032_ELEVEN"] {
        let key = key(field(vector, "PUBLIC_KEY"));
        let message = field(vector, "MESSAGE");
        let signature = field(vector, "SIGNATURE");
        assert_eq!(key.public_key.len(), 57);
        assert_eq!(signature.len(), 114);
        assert!(algorithm::verify(&key, &message, &signature), "{vector}");
        let mut changed = message.clone();
        changed.push(0);
        assert!(!algorithm::verify(&key, &changed, &signature));
        let mut changed = signature;
        changed[0] ^= 1;
        assert!(!algorithm::verify(&key, &message, &changed));
    }
    // RFC 8032's context="foo" and Ed448ph signatures are valid signatures
    // for other variants. DNSSEC must reject both over the original message.
    for vector in ["RFC8032_CONTEXT", "RFC8032_PREHASH"] {
        assert!(!algorithm::verify(
            &key(field(vector, "PUBLIC_KEY")),
            &field(vector, "MESSAGE"),
            &field(vector, "SIGNATURE")
        ));
    }
}

#[test]
fn rfc8080_keys_ds_and_independently_corrected_dnssec_signatures_verify() {
    for (vector, tag) in [("RFC8080_1", 9713), ("RFC8080_2", 38353)] {
        let (key, set) = example(vector);
        assert_eq!(algorithm::key_tag(&key), tag);
        assert_eq!(
            algorithm::ds_digest(&set.owner, &key, 2),
            Some(field(vector, "DS_DIGEST"))
        );
        let canonical = signed_data(&set, &set.rrsigs[0]).unwrap().bytes;
        assert_eq!(canonical, field(vector, "CANONICAL"));
        assert!(algorithm::verify(
            &key,
            &canonical,
            &set.rrsigs[0].signature
        ));
        assert_eq!(
            check(&set, &key, 1_439_000_000),
            (
                Ok(Verified {
                    wildcard: None,
                    ttl_bound: 3600
                }),
                1
            )
        );
        // A prehash or a manually prefixed dom4 string changes the pure input.
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA512, &canonical);
        assert!(!algorithm::verify(
            &key,
            digest.as_ref(),
            &set.rrsigs[0].signature
        ));
        let mut prefixed = b"SigEd448\0\0".to_vec();
        prefixed.extend(canonical);
        assert!(!algorithm::verify(
            &key,
            &prefixed,
            &set.rrsigs[0].signature
        ));
    }
}

#[test]
fn rfc8080_published_invalid_label_count_is_rejected_before_crypto() {
    // OpenSSL 3.6.3 independently verifies both published signatures only
    // with Labels=3, although their owner example.com has two labels.
    for vector in ["RFC8080_1", "RFC8080_2"] {
        let (key, mut set) = example(vector);
        set.rrsigs[0].signature = field(vector, "PUBLISHED_SIGNATURE");
        assert_eq!(
            check(&set, &key, 1_439_000_000),
            (Err(BogusReason::SignatureInvalid), 1)
        );
        let mut raw = field(vector, "CANONICAL");
        raw[3] = 3;
        assert!(algorithm::verify(&key, &raw, &set.rrsigs[0].signature));
        set.rrsigs[0].labels = 3;
        assert_eq!(
            check(&set, &key, 1_439_000_000),
            (Err(BogusReason::SignaturesMissing), 0)
        );
    }
}

#[test]
fn corrected_signature_preserves_case_ttl_and_skewed_time_semantics() {
    let (key, mut set) = example("RFC8080_1");
    let original = signed_data(&set, &set.rrsigs[0]).unwrap().bytes;
    set.owner = Name::from_text("EXAMPLE.COM.").unwrap();
    set.rrsigs[0].signer = Name::from_text("ExAmPlE.CoM.").unwrap();
    set.records = test_vectors::parse("example.com. 17 IN MX 10 MaIl.ExAmPlE.CoM.")[0]
        .records
        .clone();
    set.ttl = 17;
    set.rrsigs[0].received_ttl = 11;
    assert_eq!(signed_data(&set, &set.rrsigs[0]).unwrap().bytes, original);
    assert_eq!(
        check(&set, &key, 1_439_000_000),
        (
            Ok(Verified {
                wildcard: None,
                ttl_bound: 11
            }),
            1
        )
    );
    let sig = &set.rrsigs[0];
    let skew = u64::from((sig.expiration - sig.inception) / 10).clamp(
        u64::from(MIN_CLOCK_SKEW_SECONDS),
        u64::from(MAX_CLOCK_SKEW_SECONDS),
    );
    assert_eq!(check(&set, &key, u64::from(sig.inception) - skew).1, 1);
    assert!(
        check(&set, &key, u64::from(sig.expiration) + skew)
            .0
            .is_ok()
    );
    assert_eq!(
        check(&set, &key, u64::from(sig.inception) - skew - 1),
        (Err(BogusReason::SignatureNotYetValid), 1)
    );
    assert_eq!(
        check(&set, &key, u64::from(sig.expiration) + skew + 1),
        (Err(BogusReason::SignatureExpired), 1)
    );
    assert_eq!(check(&set, &key, u64::from(sig.inception)).1, 1);
    assert_eq!(
        check(&set, &key, u64::from(sig.expiration)),
        (
            Ok(Verified {
                wildcard: None,
                ttl_bound: 0
            }),
            1
        )
    );
}

#[test]
fn malformed_ed448_key_and_signature_lengths_consume_failed_checks() {
    let (key, set) = example("RFC8080_1");
    for length in [0, 1, 32, 56, 58, 114] {
        let mut changed_key = key.clone();
        changed_key.public_key.resize(length, 0);
        let mut changed = set.clone();
        changed.rrsigs[0].key_tag = algorithm::key_tag(&changed_key);
        assert_eq!(
            check(&changed, &changed_key, 1_439_000_000),
            (Err(BogusReason::SignatureInvalid), 1),
            "key length {length}"
        );
    }
    for length in [0, 1, 57, 64, 113, 115] {
        let mut changed = set.clone();
        changed.rrsigs[0].signature.resize(length, 0);
        assert_eq!(
            check(&changed, &key, 1_439_000_000),
            (Err(BogusReason::SignatureInvalid), 1),
            "signature length {length}"
        );
    }
    let mut changed = set.clone();
    changed.records[0].canonical[1] ^= 1;
    assert_eq!(
        check(&changed, &key, 1_439_000_000),
        (Err(BogusReason::SignatureInvalid), 1)
    );
}

#[test]
fn distinct_failed_ed448_signatures_obey_exact_existing_budgets() {
    let (key, set) = example("RFC8080_1");
    assert_eq!(MAX_SIGNATURE_CHECKS_PER_RRSET, 20);
    for bad_count in [19, 20] {
        let mut changed = set.clone();
        let valid = changed.rrsigs.pop().unwrap();
        for index in 1..=bad_count {
            let mut bad = valid.clone();
            bad.signature[0] ^= index;
            changed.rrsigs.push(bad);
        }
        changed.rrsigs.push(valid);
        let (result, checks) = check(&changed, &key, 1_439_000_000);
        assert_eq!(checks, 20);
        if bad_count == 19 {
            assert!(result.is_ok());
        } else {
            assert_eq!(result, Err(BogusReason::WorkLimit));
        }
    }
    let mut budget = WorkBudget {
        crypto_operations: MAX_SIGNATURE_CHECKS,
        ..WorkBudget::default()
    };
    assert_eq!(
        verify_rrset(
            &set,
            &set.owner,
            &[key],
            1_439_000_000,
            Policy::default(),
            &mut budget,
            SignatureLimit::CachedKeys(&mut 0)
        ),
        Err(BogusReason::WorkLimit)
    );
    assert_eq!(budget.signatures, 0);
}
