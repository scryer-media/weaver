// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_dnssec::testing::{
    DenialMode, FakeResolver, KeyAlgorithm, Reply, Zone, drive, signature_checks,
};
use weaver_dnssec::{
    BogusReason, Name, Outcome, Policy, Query, Question, RecordType as T, Step, Validator, Verdict,
};

const NOW: u64 = 1_100_000;
const ADDRESS: [u8; 4] = [192, 0, 2, 16];

fn name(text: &str) -> Name {
    Name::from_text(text).unwrap()
}

fn query(owner: &str, rtype: T) -> Query {
    Query {
        name: name(owner),
        rtype,
    }
}

fn tree(root_algorithm: KeyAlgorithm, child_algorithm: KeyAlgorithm) -> FakeResolver {
    let root = Zone::new(Name::root(), root_algorithm).unwrap();
    let mut child = Zone::new(name("example"), child_algorithm).unwrap();
    for owner in ["news.example", "other.example", "*.wild.example"] {
        child.add_record(name(owner), T::A, ADDRESS.to_vec());
    }
    FakeResolver::new(vec![root, child]).unwrap()
}

fn validator(resolver: &FakeResolver) -> Validator {
    Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap()
}

fn run(v: &Validator, resolver: &FakeResolver, owner: &str, rtype: T) -> (Verdict, Vec<Query>) {
    drive(v, resolver, Question::new(name(owner), rtype).unwrap(), NOW)
}

fn secure(verdict: Verdict, expected: Outcome) {
    let Verdict::Secure(answer) = verdict else {
        panic!("expected secure {expected:?}, got {verdict:?}");
    };
    assert_eq!(answer.outcome, expected);
    assert_eq!(answer.ttl, 300);
}

fn address(owner: &str) -> Outcome {
    Outcome::Records {
        name: name(owner),
        rdata: vec![ADDRESS.to_vec()],
    }
}

fn bogus(verdict: Verdict, expected: BogusReason) {
    let Verdict::Bogus(failure) = verdict else {
        panic!("expected bogus {expected:?}, got {verdict:?}");
    };
    assert_eq!(failure.reason, expected);
}

fn counted(v: &Validator, resolver: &FakeResolver) -> (Verdict, usize) {
    let mut validation = v.begin(Question::new(name("news.example"), T::A).unwrap(), NOW);
    loop {
        // Polling the state machine again must never spend another operation.
        let step = validation.step();
        let checks = signature_checks(&validation);
        assert_eq!(validation.step(), step);
        assert_eq!(signature_checks(&validation), checks);
        match step {
            Step::Need(query) => validation
                .supply(&query, &resolver.answer(&query).unwrap())
                .unwrap(),
            Step::Done(verdict) => return (verdict, signature_checks(&validation)),
        }
    }
}

#[test]
fn classical_root_authenticates_ed448_leaf_and_reuses_cached_keys() {
    let resolver = tree(KeyAlgorithm::P256, KeyAlgorithm::Ed448);
    let v = validator(&resolver);
    let (verdict, checks) = counted(&v, &resolver);
    secure(verdict, address("news.example"));
    assert_eq!(checks, 4);
    let (verdict, asked) = run(&v, &resolver, "other.example", T::A);
    secure(verdict, address("other.example"));
    assert_eq!(asked, [query("other.example", T::A)]);
    let (verdict, checks) = counted(&v, &resolver);
    secure(verdict, address("news.example"));
    assert_eq!(checks, 1);
}

#[test]
fn ed448_trust_anchor_authenticates_classical_and_ed448_children() {
    for child in [
        KeyAlgorithm::P256,
        KeyAlgorithm::Ed25519,
        KeyAlgorithm::Ed448,
    ] {
        let resolver = tree(KeyAlgorithm::Ed448, child);
        let (verdict, asked) = run(&validator(&resolver), &resolver, "news.example", T::A);
        secure(verdict, address("news.example"));
        assert_eq!(
            asked,
            [
                query("news.example", T::A),
                query(".", T::DNSKEY),
                query("example", T::DS),
                query("example", T::DNSKEY)
            ]
        );
    }
}

#[test]
fn ed448_data_signatures_and_validity_fail_closed() {
    for case in 0..5 {
        let mut resolver = tree(KeyAlgorithm::P256, KeyAlgorithm::Ed448);
        if case >= 3 {
            let mut signer = resolver.zones()[1].clone();
            // A day past either end is outside any clock tolerance.
            if case == 3 {
                signer.signature_window.expiration = NOW as u32 - 86_401;
            } else {
                signer.signature_window.inception = NOW as u32 + 86_401;
            }
            let q = query("news.example", T::A);
            let reply = Reply {
                answer: signer.signed_rrset(&q.name, T::A).unwrap(),
                ..Reply::default()
            };
            resolver.set_reply(q.clone(), Some(reply.encode(&q).unwrap()));
        } else {
            resolver.tamper(move |q, reply| {
                if q != &query("news.example", T::A) {
                    return;
                }
                if case == 0 {
                    reply
                        .answer
                        .iter_mut()
                        .find(|r| r.rtype == T::A)
                        .unwrap()
                        .rdata[3] ^= 1;
                } else if case == 1 {
                    *reply
                        .answer
                        .iter_mut()
                        .find(|r| r.rtype == T::RRSIG)
                        .unwrap()
                        .rdata
                        .last_mut()
                        .unwrap() ^= 1;
                } else {
                    reply.strip_signatures();
                }
            });
        }
        bogus(
            run(&validator(&resolver), &resolver, "news.example", T::A).0,
            match case {
                0 | 1 => BogusReason::SignatureInvalid,
                2 => BogusReason::NoDnssecRecords,
                3 => BogusReason::SignatureExpired,
                _ => BogusReason::SignatureNotYetValid,
            },
        );
    }
}

#[test]
fn supported_ed448_ds_and_dnskey_tampering_cannot_downgrade_to_insecure() {
    for dnskey in [false, true] {
        let mut resolver = tree(KeyAlgorithm::P256, KeyAlgorithm::Ed448);
        if dnskey {
            resolver.tamper(|q, reply| {
                if q == &query("example", T::DNSKEY) {
                    *reply
                        .answer
                        .iter_mut()
                        .find(|r| r.rtype == T::DNSKEY)
                        .unwrap()
                        .rdata
                        .last_mut()
                        .unwrap() ^= 1;
                }
            });
        } else {
            // The wrong algorithm-16 DS is authentically signed by the parent.
            let mut ds = resolver.zones()[1].ds_records(2).unwrap();
            *ds[0].last_mut().unwrap() ^= 1;
            resolver
                .zone_mut(&Name::root())
                .unwrap()
                .delegate(name("example"), ds);
        }
        let (verdict, asked) = run(&validator(&resolver), &resolver, "news.example", T::A);
        bogus(verdict, BogusReason::KeysNotProven);
        assert_eq!(asked.last(), Some(&query("example", T::DNSKEY)));
    }
}

#[test]
fn ed448_nsec_and_nsec3_authenticate_denial_and_wildcards() {
    for mode in [
        DenialMode::Nsec,
        DenialMode::Nsec3 {
            salt: vec![0xaa, 0xbb],
            iterations: 0,
            opt_out: false,
        },
    ] {
        for (owner, rtype, expected) in [
            ("news.example", T::AAAA, Outcome::NoData),
            ("missing.example", T::A, Outcome::NoSuchName),
            ("host.wild.example", T::A, address("host.wild.example")),
        ] {
            let mut resolver = tree(KeyAlgorithm::P256, KeyAlgorithm::Ed448);
            resolver.zone_mut(&name("example")).unwrap().denial = mode.clone();
            let v = validator(&resolver);
            secure(run(&v, &resolver, owner, rtype).0, expected.clone());
            let (verdict, asked) = run(&v, &resolver, owner, rtype);
            secure(verdict, expected);
            assert_eq!(asked, [query(owner, rtype)]);
            resolver.tamper(|q, reply| {
                if q.rtype == T::DNSKEY || q.rtype == T::DS {
                    return;
                }
                for record in &mut reply.authority {
                    if record.rtype == T::RRSIG {
                        *record.rdata.last_mut().unwrap() ^= 1;
                    }
                }
            });
            bogus(
                run(&validator(&resolver), &resolver, owner, rtype).0,
                if owner == "host.wild.example" {
                    // The address signature is valid, but authentication of
                    // its wildcard expansion requires an intact denial proof.
                    BogusReason::WildcardUnproven
                } else {
                    BogusReason::DenialInvalid
                },
            );
        }
    }
}

#[test]
fn ed448_distinct_failed_signatures_stop_at_twenty_with_cold_or_warm_keys() {
    for warm in [false, true] {
        for failures in [19, 20] {
            let mut resolver = tree(KeyAlgorithm::P256, KeyAlgorithm::Ed448);
            let v = validator(&resolver);
            if warm {
                secure(
                    run(&v, &resolver, "other.example", T::A).0,
                    address("other.example"),
                );
            }
            resolver.tamper(move |q, reply| {
                if q != &query("news.example", T::A) {
                    return;
                }
                let valid = reply
                    .answer
                    .iter()
                    .find(|r| r.rtype == T::RRSIG)
                    .unwrap()
                    .clone();
                reply.answer.retain(|r| r.rtype != T::RRSIG);
                for index in 1..=failures {
                    let mut bad = valid.clone();
                    // Mutate R, preserving a full-width signature and making
                    // every failure distinct so wire deduplication keeps it.
                    let start = 18 + name("example").as_wire().len();
                    bad.rdata[start] ^= index;
                    reply.answer.push(bad);
                }
                reply.answer.push(valid);
            });
            let (verdict, checks) = counted(&v, &resolver);
            assert_eq!(checks, if warm { 20 } else { 23 });
            if failures == 19 {
                secure(verdict, address("news.example"));
            } else {
                bogus(verdict, BogusReason::WorkLimit);
            }
        }
    }
}

#[test]
fn malformed_ed448_signature_lengths_are_rejected_through_wire_validation() {
    for length in [0, 1, 64, 113, 115] {
        let mut resolver = tree(KeyAlgorithm::P256, KeyAlgorithm::Ed448);
        resolver.tamper(move |q, reply| {
            if q != &query("news.example", T::A) {
                return;
            }
            let sig = reply
                .answer
                .iter_mut()
                .find(|r| r.rtype == T::RRSIG)
                .unwrap();
            sig.rdata
                .resize(18 + name("example").as_wire().len() + length, 0);
        });
        let (verdict, checks) = counted(&validator(&resolver), &resolver);
        if length == 0 {
            // An RRSIG with no signature bytes is malformed RDATA, rejected
            // while parsing the first answer before any chain work is done.
            bogus(verdict, BogusReason::MalformedReply);
            assert_eq!(checks, 0);
        } else {
            bogus(verdict, BogusReason::SignatureInvalid);
            assert_eq!(checks, 4, "signature length {length}");
        }
    }
}
