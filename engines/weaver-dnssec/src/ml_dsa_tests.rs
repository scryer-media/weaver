// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Published DNSSEC ML-DSA-44 example and deliberate negative mutations.
//! Source: draft-westerbaan-dnssec-mldsa-04 section 6; no signing code is used.

use crate::canonical::signed_data;
use crate::limits::{
    MAX_CLOCK_SKEW_SECONDS, MAX_SIGNATURE_CHECKS, MAX_SIGNATURE_CHECKS_PER_RRSET,
    MIN_CLOCK_SKEW_SECONDS,
};
use crate::rdata::{Dnskey, Ds, RdataKind};
use crate::rrset::{SignatureLimit, Verified, WorkBudget, verify_rrset};
use crate::wire::Rrset;
use crate::{BogusReason, Name, Policy, RecordType, algorithm, test_vectors};

fn example() -> (Dnskey, Ds, Rrset) {
    let mut key = None;
    let mut ds = None;
    let mut mx = None;
    for set in test_vectors::parse(include_str!("ml_dsa_vectors.txt")) {
        match &set.records[0].kind {
            RdataKind::Dnskey(value) => key = Some(value.clone()),
            RdataKind::Ds(value) => ds = Some(value.clone()),
            _ => {
                assert_eq!(set.rtype, RecordType::MX);
                mx = Some(set);
            }
        }
    }
    (key.unwrap(), ds.unwrap(), mx.unwrap())
}

fn check(set: &Rrset, key: &Dnskey, now: u64) -> (Result<Verified, BogusReason>, usize) {
    let mut budget = WorkBudget::default();
    let mut checks = 0;
    let result = verify_rrset(
        set,
        &Name::from_text("example.com.").unwrap(),
        std::slice::from_ref(key),
        now,
        Policy::default(),
        &mut budget,
        SignatureLimit::CachedKeys(&mut checks),
    );
    assert_eq!(budget.ds_digests, 0);
    assert_eq!(budget.nsec3_hashes, 0);
    (result, budget.signatures)
}

#[test]
fn published_dnskey_ds_and_canonical_mx_signature_verify() {
    let (key, ds, set) = example();
    assert_eq!((key.flags, key.protocol, key.algorithm), (257, 3, 18));
    assert_eq!(key.public_key.len(), 1312);
    assert_eq!((ds.key_tag, ds.algorithm, ds.digest_type), (59829, 18, 2));
    assert_eq!(algorithm::key_tag(&key), ds.key_tag);
    assert_eq!(
        algorithm::ds_digest(&set.owner, &key, ds.digest_type),
        Some(ds.digest)
    );
    let signature = &set.rrsigs[0];
    assert_eq!(signature.signature.len(), 2420);
    let canonical = signed_data(&set, signature).unwrap();
    // Independently serialized published fields, including the MX preference,
    // original TTL and RRSIG header; the signature covers these exact octets.
    assert_eq!(
        canonical.bytes,
        test_vectors::hex(concat!(
            "000f120200000e1055d4fc6055b94ce0e9b5",
            "076578616d706c6503636f6d00",
            "076578616d706c6503636f6d00000f000100000e100014",
            "000a046d61696c076578616d706c6503636f6d00"
        ))
    );
    assert!(algorithm::verify(
        &key,
        &canonical.bytes,
        &signature.signature
    ));
    assert_eq!(
        check(&set, &key, u64::from(signature.inception) + 1),
        (
            Ok(Verified {
                wildcard: None,
                ttl_bound: 3600
            }),
            1
        )
    );

    // A digest or an already-domain-separated message is not the pure input.
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &canonical.bytes);
    assert!(!algorithm::verify(
        &key,
        digest.as_ref(),
        &signature.signature
    ));
    let mut prefixed = vec![0, 0];
    prefixed.extend_from_slice(&canonical.bytes);
    assert!(!algorithm::verify(&key, &prefixed, &signature.signature));
}

#[test]
fn published_signature_survives_dns_case_and_received_ttl_changes() {
    let (key, _, mut set) = example();
    let original = signed_data(&set, &set.rrsigs[0]).unwrap().bytes;
    set.owner = Name::from_text("EXAMPLE.COM.").unwrap();
    set.rrsigs[0].signer = Name::from_text("ExAmPlE.CoM.").unwrap();
    let mixed_mx = test_vectors::parse("example.com. 3600 IN MX 10 MaIl.ExAmPlE.CoM.");
    set.records = mixed_mx[0].records.clone();
    set.ttl = 17;
    set.rrsigs[0].received_ttl = 11;
    assert_eq!(signed_data(&set, &set.rrsigs[0]).unwrap().bytes, original);
    assert_eq!(
        check(&set, &key, u64::from(set.rrsigs[0].inception) + 1),
        (
            Ok(Verified {
                wildcard: None,
                ttl_bound: 11
            }),
            1
        )
    );
}

#[test]
fn message_key_signature_and_signed_header_mutations_are_counted_failures() {
    let (key, _, set) = example();
    let now = u64::from(set.rrsigs[0].inception) + 1;
    let mut changed = set.clone();
    changed.records[0].canonical[1] ^= 1;
    assert_eq!(
        check(&changed, &key, now),
        (Err(BogusReason::SignatureInvalid), 1)
    );
    let mut changed = set.clone();
    changed.rrsigs[0].signature[0] ^= 1;
    assert_eq!(
        check(&changed, &key, now),
        (Err(BogusReason::SignatureInvalid), 1)
    );
    let mut changed = set.clone();
    changed.rrsigs[0].original_ttl ^= 1;
    assert_eq!(
        check(&changed, &key, now),
        (Err(BogusReason::SignatureInvalid), 1)
    );
    let mut changed_key = key;
    changed_key.public_key[0] ^= 1;
    let mut changed = set;
    changed.rrsigs[0].key_tag = algorithm::key_tag(&changed_key);
    assert_eq!(
        check(&changed, &changed_key, now),
        (Err(BogusReason::SignatureInvalid), 1)
    );
}

#[test]
fn malformed_lengths_and_other_parameter_sets_are_counted_failures() {
    let (key, _, set) = example();
    let now = u64::from(set.rrsigs[0].inception) + 1;
    // Include both other FIPS 204 public-key sizes, plus truncation and padding.
    for length in [0, 1, 1311, 1313, 1952, 2592] {
        let mut changed_key = key.clone();
        changed_key.public_key.resize(length, 0);
        assert!(algorithm::key_usable(&changed_key));
        let mut changed = set.clone();
        changed.rrsigs[0].key_tag = algorithm::key_tag(&changed_key);
        assert_eq!(
            check(&changed, &changed_key, now),
            (Err(BogusReason::SignatureInvalid), 1),
            "public-key length {length}"
        );
    }
    // Include the ML-DSA-65 and ML-DSA-87 signature sizes.
    for length in [0, 1, 2419, 2421, 3309, 4627] {
        let mut changed = set.clone();
        changed.rrsigs[0].signature.resize(length, 0);
        assert_eq!(
            check(&changed, &key, now),
            (Err(BogusReason::SignatureInvalid), 1),
            "signature length {length}"
        );
    }
}

#[test]
fn published_signature_time_reasons_need_a_valid_signature_and_bound_ttl() {
    let (key, _, set) = example();
    let sig = &set.rrsigs[0];
    let before = u64::from(sig.inception);
    let after = u64::from(sig.expiration);
    let skew = u64::from((sig.expiration - sig.inception) / 10).clamp(
        u64::from(MIN_CLOCK_SKEW_SECONDS),
        u64::from(MAX_CLOCK_SKEW_SECONDS),
    );
    assert_eq!(check(&set, &key, before - skew).1, 1);
    assert!(check(&set, &key, after + skew).0.is_ok());
    assert_eq!(
        check(&set, &key, before - skew - 1),
        (Err(BogusReason::SignatureNotYetValid), 1)
    );
    assert_eq!(
        check(&set, &key, after + skew + 1),
        (Err(BogusReason::SignatureExpired), 1)
    );
    assert_eq!(check(&set, &key, before).1, 1);
    assert_eq!(
        check(&set, &key, after),
        (
            Ok(Verified {
                wildcard: None,
                ttl_bound: 0
            }),
            1
        )
    );
    assert_eq!(
        check(&set, &key, u64::from(sig.expiration) - 7),
        (
            Ok(Verified {
                wildcard: None,
                ttl_bound: 7
            }),
            1
        )
    );
}

#[test]
fn ml_dsa_uses_existing_signature_budgets_and_counts_malformed_attempts() {
    let (key, _, mut set) = example();
    let now = u64::from(set.rrsigs[0].inception) + 1;
    let valid = set.rrsigs[0].clone();
    set.rrsigs[0].signature.pop();
    set.rrsigs.push(valid);
    let (result, checks) = check(&set, &key, now);
    assert!(result.is_ok());
    assert_eq!(checks, 2);
    let zone = Name::from_text("example.com.").unwrap();
    for (global, per_rrset, attempted) in [
        (MAX_SIGNATURE_CHECKS, 0, 0),
        (0, MAX_SIGNATURE_CHECKS_PER_RRSET, 0),
    ] {
        let mut budget = WorkBudget {
            crypto_operations: global,
            signatures: global,
            ..WorkBudget::default()
        };
        let mut checks = per_rrset;
        assert_eq!(
            verify_rrset(
                &set,
                &zone,
                std::slice::from_ref(&key),
                now,
                Policy::default(),
                &mut budget,
                SignatureLimit::CachedKeys(&mut checks)
            ),
            Err(BogusReason::WorkLimit)
        );
        assert_eq!(budget.crypto_operations, global + attempted);
        assert_eq!(budget.signatures, global + attempted);
        assert_eq!(checks, per_rrset + attempted);
    }
}
