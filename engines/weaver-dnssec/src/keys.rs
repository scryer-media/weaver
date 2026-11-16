// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Authenticate a zone's DNSKEY RRset with its parent's proven DS records.

use std::collections::BTreeMap;

use crate::algorithm;
use crate::limits::{MAX_DNSKEYS, MAX_DS_DIGESTS};
use crate::rdata::{Dnskey, Ds, RdataKind};
use crate::rrset::{SignatureLimit, WorkBudget, verify_rrset};
use crate::wire::Message;
use crate::{BogusReason, Name, Policy, RecordType};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KeyProof {
    Secure { keys: Vec<Dnskey>, ttl_bound: u32 },
    Insecure,
}

/// This runs before requesting DNSKEY, so unsupported delegations need no fetch.
pub(crate) fn filter_ds(ds: &[Ds], policy: Policy) -> Vec<Ds> {
    let mut usable: Vec<_> = ds
        .iter()
        .filter(|entry| {
            algorithm::supported_algorithm(entry.algorithm, &policy)
                && algorithm::supported_digest(entry.digest_type, &policy)
        })
        .cloned()
        .collect();
    if usable
        .iter()
        .any(|entry| matches!(entry.digest_type, 2 | 4))
    {
        usable.retain(|entry| entry.digest_type != 1);
    }
    usable
}

pub(crate) fn prove_keys(
    zone: &Name,
    ds: &[Ds],
    ds_ttl: u32,
    reply: &Message,
    now: u64,
    policy: Policy,
    budget: &mut WorkBudget,
) -> Result<KeyProof, BogusReason> {
    let ds = filter_ds(ds, policy);
    if ds.is_empty() {
        return Ok(KeyProof::Insecure);
    }
    let set = reply
        .answer
        .iter()
        .find(|set| set.owner == *zone && set.rtype == RecordType::DNSKEY)
        .ok_or(BogusReason::KeysNotProven)?;
    if set.records.is_empty() || set.records.len() > MAX_DNSKEYS {
        return Err(BogusReason::KeysNotProven);
    }
    let mut keys = Vec::with_capacity(set.records.len());
    let mut candidates: BTreeMap<(u16, u8), Vec<&Dnskey>> = BTreeMap::new();
    for record in &set.records {
        let RdataKind::Dnskey(key) = &record.kind else {
            return Err(BogusReason::KeysNotProven);
        };
        keys.push(key.clone());
        if key.flags & 0x0100 != 0 && key.flags & 0x0080 == 0 && key.protocol == 3 {
            candidates
                .entry((algorithm::key_tag(key), key.algorithm))
                .or_default()
                .push(key);
        }
    }
    // One synchronous proof may try several DS records for the same key.
    // Memoize exact key bytes (including flags/protocol) so rollover records
    // never repeat a digest or a cryptographic verification.
    let mut digests = BTreeMap::new();
    let mut verifications = BTreeMap::new();
    let mut unusable_match = false;
    let mut usable_match = false;
    // A supported DS with no key in the reply: the set may have been cut.
    let mut unmatched_ds = false;
    let mut out_of_time = None;
    'ds: for entry in ds {
        let Some(matches) = candidates.get(&(entry.key_tag, entry.algorithm)) else {
            unmatched_ds = true;
            continue;
        };
        let mut matched = false;
        for key in matches {
            let identity = (
                key.flags,
                key.protocol,
                key.algorithm,
                key.public_key.clone(),
            );
            let digest = if let Some(digest) = digests.get(&(identity.clone(), entry.digest_type)) {
                digest
            } else {
                if budget.ds_digests >= MAX_DS_DIGESTS {
                    return Err(BogusReason::WorkLimit);
                }
                budget.charge_crypto()?;
                budget.ds_digests += 1;
                digests
                    .entry((identity.clone(), entry.digest_type))
                    .or_insert_with(|| algorithm::ds_digest(zone, key, entry.digest_type))
            };
            if digest.as_ref() != Some(&entry.digest) {
                continue;
            }
            matched = true;
            if !algorithm::key_usable(key) {
                unusable_match = true;
                continue;
            }
            usable_match = true;
            let verified = verifications.entry(identity).or_insert_with(|| {
                verify_rrset(
                    set,
                    zone,
                    core::slice::from_ref(*key),
                    now,
                    policy,
                    budget,
                    SignatureLimit::ExplicitKey,
                )
            });
            match verified {
                Ok(verified) => {
                    return Ok(KeyProof::Secure {
                        keys,
                        ttl_bound: verified.ttl_bound.min(ds_ttl),
                    });
                }
                Err(BogusReason::WorkLimit) => return Err(BogusReason::WorkLimit),
                Err(
                    reason @ (BogusReason::SignatureExpired | BogusReason::SignatureNotYetValid),
                ) => {
                    out_of_time.get_or_insert(*reason);
                    continue 'ds;
                }
                Err(_) => continue 'ds,
            }
        }
        unmatched_ds |= !matched;
    }
    // Insecure only when every supported DS matched a key in the reply and
    // every one of those keys is unusable. A DNSKEY reply cut down to the
    // unusable key leaves another DS unmatched, and that is a failure.
    if unusable_match && !usable_match && !unmatched_ds {
        Ok(KeyProof::Insecure)
    } else if let Some(reason) = out_of_time {
        Err(reason)
    } else if !usable_match {
        // No DS digest matched any key. The caller reports this distinctly
        // only for a configured trust anchor.
        Err(BogusReason::TrustAnchorMismatch)
    } else {
        Err(BogusReason::KeysNotProven)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_vectors;

    #[test]
    fn unsupported_ds_is_insecure_without_keys_or_work() {
        let (_, mut ds, _) = test_vectors::p256();
        ds.algorithm = 253;
        let reply = Message {
            rcode: 0,
            answer: vec![],
            authority: vec![],
            soas: vec![],
            signatures: 0,
            extended_errors: vec![],
        };
        let mut budget = WorkBudget::default();
        assert_eq!(
            prove_keys(
                &Name::root(),
                &[ds],
                3600,
                &reply,
                0,
                Policy::default(),
                &mut budget
            ),
            Ok(KeyProof::Insecure)
        );
        assert_eq!(budget.ds_digests, 0);
        assert_eq!(budget.signatures, 0);
    }

    #[test]
    fn stronger_supported_ds_displaces_sha1_in_received_order() {
        let (_, strong, _) = test_vectors::p256();
        let mut weak = strong.clone();
        weak.digest_type = 1;
        let mut unsupported = strong.clone();
        unsupported.algorithm = 253;
        let policy = Policy {
            sha1_ds_digests: true,
            ..Policy::default()
        };
        assert_eq!(filter_ds(&[weak.clone()], policy), vec![weak.clone()]);
        assert_eq!(
            filter_ds(&[weak.clone(), unsupported, strong.clone()], policy),
            vec![strong]
        );
        assert_eq!(
            filter_ds(&[weak.clone()], Policy::default()),
            vec![weak.clone()]
        );
        assert!(
            filter_ds(
                &[weak],
                Policy {
                    sha1_ds_digests: false,
                    ..Policy::default()
                }
            )
            .is_empty()
        );
    }

    fn rsa_key_proof_fixture() -> (Message, Ds) {
        let field = |name: &str| {
            let value = include_str!("../tests/fixtures/rsa-sha1.txt")
                .lines()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
                .unwrap();
            test_vectors::hex(value)
        };
        let query = crate::Query {
            name: Name::root(),
            rtype: RecordType::DNSKEY,
        };
        let reply = Message::parse(&field("DNSKEY_REPLY"), &query).unwrap();
        let ds = Ds {
            key_tag: u16::from_be_bytes(field("KEY_TAG").try_into().unwrap()),
            algorithm: 8,
            digest_type: 2,
            digest: field("ANCHOR_DIGEST"),
        };
        (reply, ds)
    }

    #[test]
    fn ds_digest_and_signature_share_the_global_crypto_boundary() {
        let (reply, ds) = rsa_key_proof_fixture();
        for used in [198, 199, 200] {
            // The prior work contains all three operation kinds. Neither the
            // DS nor NSEC3 local guard is exhausted at the global boundary.
            let mut budget = WorkBudget {
                crypto_operations: used,
                signatures: used - 48,
                ds_digests: 16,
                nsec3_hashes: 32,
            };
            let result = prove_keys(
                &Name::root(),
                std::slice::from_ref(&ds),
                300,
                &reply,
                1_500_000,
                Policy::default(),
                &mut budget,
            );
            if used == 198 {
                assert!(matches!(result, Ok(KeyProof::Secure { .. })));
                assert_eq!(budget.signatures, 151);
            } else {
                assert_eq!(result, Err(BogusReason::WorkLimit));
                assert_eq!(budget.signatures, used - 48);
            }
            assert_eq!(budget.crypto_operations, 200);
            assert_eq!(budget.ds_digests, if used == 200 { 16 } else { 17 });
            assert_eq!(budget.nsec3_hashes, 32);
            let ds_digests = budget.ds_digests;
            assert_eq!(
                prove_keys(
                    &Name::root(),
                    std::slice::from_ref(&ds),
                    300,
                    &reply,
                    1_500_000,
                    Policy::default(),
                    &mut budget,
                ),
                Err(BogusReason::WorkLimit)
            );
            assert_eq!(budget.crypto_operations, 200);
            assert_eq!(budget.ds_digests, ds_digests);
        }
    }

    #[test]
    fn duplicate_ds_entries_reuse_one_digest_and_one_key_verification() {
        let (reply, ds) = rsa_key_proof_fixture();
        let mut mismatch = ds.clone();
        mismatch.digest[0] ^= 1;
        let mut budget = WorkBudget::default();
        assert!(matches!(
            prove_keys(
                &Name::root(),
                &[mismatch.clone(), mismatch, ds],
                300,
                &reply,
                1_500_000,
                Policy::default(),
                &mut budget,
            ),
            Ok(KeyProof::Secure { .. })
        ));
        assert_eq!(budget.ds_digests, 1);
        assert_eq!(budget.signatures, 1);
        assert_eq!(budget.crypto_operations, 2);
    }
}
