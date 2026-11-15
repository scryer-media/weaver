// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_dnssec::testing::{FakeResolver, KeyAlgorithm, Record, Zone, drive, signature_checks};
use weaver_dnssec::{
    AnchorError, BogusReason, Name, Outcome, Policy, Query, Question, RecordType as T, Step,
    Validator, Verdict,
};

const NOW: u64 = 1_500_000;
const ADDRESS: [u8; 4] = [192, 0, 2, 19];

fn name(text: &str) -> Name {
    Name::from_text(text).unwrap()
}

fn address(verdict: Verdict, owner: &Name, secure: bool) {
    let answer = match (secure, verdict) {
        (true, Verdict::Secure(answer)) | (false, Verdict::Insecure(answer)) => answer,
        (_, verdict) => panic!("unexpected security assessment: {verdict:?}"),
    };
    assert_eq!(
        answer.outcome,
        Outcome::Records {
            name: owner.clone(),
            rdata: vec![ADDRESS.to_vec()],
        }
    );
}

fn counted(
    validator: &Validator,
    resolver: &FakeResolver,
    owner: &Name,
) -> (Verdict, Vec<Query>, usize) {
    let mut validation = validator.begin(Question::new(owner.clone(), T::A).unwrap(), NOW);
    let mut queries = Vec::new();
    loop {
        match validation.step() {
            Step::Need(query) => {
                queries.push(query.clone());
                validation
                    .supply(&query, &resolver.answer(&query).unwrap())
                    .unwrap();
            }
            Step::Done(verdict) => {
                return (verdict, queries, signature_checks(&validation));
            }
        }
    }
}

#[test]
fn default_anchor_policy_excludes_deprecated_algorithms_and_preserves_current_algorithms() {
    let root = Zone::new(Name::root(), KeyAlgorithm::Ed25519).unwrap();
    let anchor = root.anchors().unwrap().remove(0);
    for algorithm in 0..=u8::MAX {
        let mut candidate = anchor.clone();
        candidate.algorithm = algorithm;
        let result = Validator::new(vec![candidate], Policy::default());
        if matches!(algorithm, 8 | 10 | 13 | 14 | 15 | 16 | 18) {
            assert!(result.is_ok(), "algorithm {algorithm}");
        } else {
            assert!(
                matches!(result, Err(AnchorError::NoUsableAnchors)),
                "algorithm {algorithm}"
            );
        }
    }
    for digest_type in 0..=u8::MAX {
        let mut candidate = anchor.clone();
        candidate.digest_type = digest_type;
        candidate.digest = vec![
            0;
            match digest_type {
                1 => 20,
                4 => 48,
                _ => 32,
            }
        ];
        let result = Validator::new(vec![candidate], Policy::default());
        if matches!(digest_type, 1 | 2 | 4) {
            assert!(result.is_ok(), "digest {digest_type}");
        } else {
            assert!(
                matches!(result, Err(AnchorError::NoUsableAnchors)),
                "digest {digest_type}"
            );
        }
    }
}

#[test]
fn authenticated_deprecated_only_delegations_are_insecure_without_child_key_fetches() {
    // RFCs 9905 and 9906 section 2: authenticated unsupported-only
    // delegations are insecure without trying to verify the child's keys.
    for (algorithm, digest_type, digest_length) in [
        (5, 2, 32),
        (7, 2, 32),
        (12, 2, 32),
        (15, 3, 1),
        (15, 3, 31),
        (15, 3, 32),
        (15, 3, 33),
        (12, 3, 32),
    ] {
        let mut root = Zone::new(Name::root(), KeyAlgorithm::Ed25519).unwrap();
        let mut child = Zone::new(name("example"), KeyAlgorithm::Ed25519).unwrap();
        let owner = name("news.example");
        child.add_record(owner.clone(), T::A, ADDRESS.to_vec());
        let mut ds = child.ds_records(2).unwrap().remove(0);
        ds[2] = algorithm;
        ds[3] = digest_type;
        ds.resize(4 + digest_length, 0xff);
        // Mutate before the parent signs: the unsupported DS is authenticated.
        root.delegate(child.apex.clone(), vec![ds]);
        let resolver = FakeResolver::new(vec![root, child]).unwrap();
        let validator =
            Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap();
        let (verdict, queries, _) = counted(&validator, &resolver, &owner);
        address(verdict, &owner, false);
        assert!(queries.contains(&Query {
            name: name("example"),
            rtype: T::DS,
        }));
        assert!(!queries.contains(&Query {
            name: name("example"),
            rtype: T::DNSKEY,
        }));
    }
}

#[test]
fn deprecated_delegations_neither_poison_nor_rescue_accepted_records_in_both_orders() {
    for (reverse, accepted_valid) in [(false, false), (false, true), (true, false), (true, true)] {
        let mut root = Zone::new(Name::root(), KeyAlgorithm::Ed25519).unwrap();
        let mut child = Zone::new(name("example"), KeyAlgorithm::Ed25519).unwrap();
        let owner = name("news.example");
        child.add_record(owner.clone(), T::A, ADDRESS.to_vec());
        let mut accepted = child.ds_records(2).unwrap().remove(0);
        let mut deprecated = Vec::new();
        for algorithm in [5, 7, 12] {
            let mut ds = accepted.clone();
            ds[2] = algorithm;
            deprecated.push(ds);
        }
        let mut deprecated_digest = accepted.clone();
        deprecated_digest[3] = 3;
        if !accepted_valid {
            *accepted.last_mut().unwrap() ^= 1;
        }
        let mut ds = vec![accepted, deprecated_digest];
        ds.extend(deprecated);
        if reverse {
            ds.reverse();
        }
        root.delegate(child.apex.clone(), ds);
        let resolver = FakeResolver::new(vec![root, child]).unwrap();
        let validator =
            Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap();
        let (verdict, queries, _) = counted(&validator, &resolver, &owner);
        if accepted_valid {
            address(verdict, &owner, true);
        } else {
            assert!(matches!(verdict, Verdict::Bogus(failure)
                if failure.reason == BogusReason::KeysNotProven));
        }
        assert!(queries.contains(&Query {
            name: name("example"),
            rtype: T::DNSKEY,
        }));
    }
}

fn key_tag(rdata: &[u8]) -> u16 {
    let sum = rdata.iter().enumerate().fold(0_u32, |sum, (index, byte)| {
        sum + if index % 2 == 0 {
            u32::from(*byte) << 8
        } else {
            u32::from(*byte)
        }
    });
    (sum + ((sum >> 16) & 0xffff)) as u16
}

#[test]
fn malformed_gost_keys_and_signatures_never_add_crypto_work_or_rescue_bad_signatures() {
    for length in [1, 31, 63, 64, 65] {
        for before in [false, true] {
            for accepted_valid in [false, true] {
                let mut root = Zone::new(Name::root(), KeyAlgorithm::Ed25519).unwrap();
                let owner = name("host");
                let warm = name("warm");
                root.add_record(owner.clone(), T::A, ADDRESS.to_vec());
                root.add_record(warm.clone(), T::A, ADDRESS.to_vec());
                let mut deprecated_key = vec![1, 1, 3, 12];
                deprecated_key.extend(vec![0xff; length]);
                let deprecated_tag = key_tag(&deprecated_key);
                // The accepted key signs the DNSKEY RRset including this key.
                root.records.push(Record::new(
                    Name::root(),
                    T::DNSKEY,
                    root.ttl,
                    deprecated_key,
                ));
                let mut resolver = FakeResolver::new(vec![root]).unwrap();
                resolver.tamper(move |query, reply| {
                    if query.rtype != T::A || query.name != name("host") {
                        return;
                    }
                    let accepted = reply
                        .answer
                        .iter_mut()
                        .find(|record| record.rtype == T::RRSIG)
                        .unwrap();
                    let mut deprecated = accepted.clone();
                    deprecated.rdata[2] = 12;
                    deprecated.rdata[16..18].copy_from_slice(&deprecated_tag.to_be_bytes());
                    // The signer is the root (one octet) after the 18-byte prefix.
                    deprecated.rdata.truncate(19);
                    deprecated.rdata.extend(vec![0xff; length]);
                    if !accepted_valid {
                        *accepted.rdata.last_mut().unwrap() ^= 1;
                    }
                    if before {
                        reply.answer.insert(0, deprecated);
                    } else {
                        reply.answer.push(deprecated);
                    }
                });
                let validator =
                    Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default())
                        .unwrap();
                address(
                    drive(
                        &validator,
                        &resolver,
                        Question::new(warm.clone(), T::A).unwrap(),
                        NOW,
                    )
                    .0,
                    &warm,
                    true,
                );
                let (verdict, queries, checks) = counted(&validator, &resolver, &owner);
                if accepted_valid {
                    address(verdict, &owner, true);
                } else {
                    assert!(matches!(verdict, Verdict::Bogus(failure)
                        if failure.reason == BogusReason::SignatureInvalid));
                }
                assert_eq!(
                    queries,
                    vec![Query {
                        name: owner,
                        rtype: T::A
                    }]
                );
                // The cached keys leave exactly the accepted signature attempt.
                assert_eq!(checks, 1, "GOST length {length}, before {before}");
            }
        }
    }
}

#[test]
fn empty_gost_payloads_fail_structural_parsing_before_crypto() {
    for target in [T::DS, T::DNSKEY, T::RRSIG] {
        let mut root = Zone::new(Name::root(), KeyAlgorithm::Ed25519).unwrap();
        let mut child = Zone::new(name("example"), KeyAlgorithm::Ed25519).unwrap();
        let warm = name("warm");
        root.add_record(warm.clone(), T::A, ADDRESS.to_vec());
        root.add_record(name("host"), T::A, ADDRESS.to_vec());
        child.add_record(name("news.example"), T::A, ADDRESS.to_vec());
        if target == T::DS {
            let mut ds = child.ds_records(2).unwrap().remove(0);
            ds[3] = 3;
            ds.truncate(4);
            root.delegate(child.apex.clone(), vec![ds]);
        } else if target == T::DNSKEY {
            root.records.push(Record::new(
                Name::root(),
                T::DNSKEY,
                root.ttl,
                vec![1, 1, 3, 12],
            ));
        }
        let mut resolver = FakeResolver::new(vec![root, child]).unwrap();
        if target == T::RRSIG {
            resolver.tamper(|query, reply| {
                if query.rtype == T::A && query.name == name("host") {
                    let mut deprecated = reply
                        .answer
                        .iter()
                        .find(|record| record.rtype == T::RRSIG)
                        .unwrap()
                        .clone();
                    deprecated.rdata[2] = 12;
                    deprecated.rdata.truncate(19);
                    reply.answer.push(deprecated);
                }
            });
        }
        let validator =
            Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap();
        if target != T::DNSKEY {
            address(
                drive(
                    &validator,
                    &resolver,
                    Question::new(warm.clone(), T::A).unwrap(),
                    NOW,
                )
                .0,
                &warm,
                true,
            );
        }
        let owner = name(if target == T::DS {
            "news.example"
        } else {
            "host"
        });
        let (verdict, queries, checks) = counted(&validator, &resolver, &owner);
        assert!(matches!(verdict, Verdict::Bogus(failure)
            if failure.reason == BogusReason::MalformedReply));
        let expected_query = match target {
            T::DS => Query {
                name: name("example"),
                rtype: T::DS,
            },
            T::DNSKEY => Query {
                name: Name::root(),
                rtype: T::DNSKEY,
            },
            _ => Query {
                name: owner,
                rtype: T::A,
            },
        };
        assert!(queries.contains(&expected_query));
        assert_eq!(checks, 0, "empty payload in {target:?}");
    }
}
