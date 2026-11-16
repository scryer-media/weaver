// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_dnssec::testing::{
    self, FakeResolver, KeyAlgorithm, Record, SignatureWindow, Zone, drive, signature_checks,
};
use weaver_dnssec::{
    BogusReason, Name, Outcome, Policy, Query, Question, RecordType as T, Step, Validator, Verdict,
};

const NOW: u64 = 1_100_000;
const IP: [u8; 4] = [203, 0, 113, 7];

fn n(text: &str) -> Name {
    Name::from_text(text).unwrap()
}

fn zone(text: &str) -> Zone {
    Zone::new(n(text), KeyAlgorithm::P256).unwrap()
}

fn resolver() -> FakeResolver {
    let mut leaf = zone("example.com");
    leaf.add_record(n("news.example.com"), T::A, IP.to_vec());
    FakeResolver::new(vec![zone("."), zone("com"), leaf]).unwrap()
}

fn run(resolver: &FakeResolver, name: &str, now: u64) -> (Verdict, Vec<Query>) {
    let validator =
        Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap();
    drive(
        &validator,
        resolver,
        Question::new(n(name), T::A).unwrap(),
        now,
    )
}

fn run_with_signature_checks(resolver: &FakeResolver, name: &str) -> (Verdict, usize) {
    let validator =
        Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap();
    let mut validation = validator.begin(Question::new(n(name), T::A).unwrap(), NOW);
    validation.set_ask_ahead(false);
    loop {
        match validation.step() {
            Step::Need { awaited: query, .. } => validation
                .supply(&query, &resolver.answer(&query).unwrap())
                .unwrap(),
            Step::Done { verdict, .. } => return (verdict, signature_checks(&validation)),
            step => panic!("unexpected step {step:?}"),
        }
    }
}

fn assert_secure(verdict: Verdict, name: &str) {
    let Verdict::Secure { answer, .. } = verdict else {
        panic!("expected secure, got {verdict:?}")
    };
    assert_eq!(
        *answer.outcome(),
        testing::records(n(name), vec![IP.to_vec()])
    );
}

fn assert_bogus(verdict: Verdict, reason: BogusReason) {
    let Verdict::Bogus { failure, .. } = verdict else {
        panic!("expected bogus {reason:?}, got {verdict:?}")
    };
    assert_eq!(failure.reason(), reason);
}

#[test]
fn unrelated_signatures_are_ignored_in_both_orders_for_answers_and_chain_records() {
    for target in [T::A, T::DNSKEY, T::DS] {
        for before in [false, true] {
            for corrupted in [false, true] {
                let mut resolver = resolver();
                let unrelated = zone("unrelated.test");
                resolver.tamper(move |query, reply| {
                    if query.rtype() != target {
                        return;
                    }
                    let mut extras = Vec::new();
                    for record in reply
                        .answer
                        .iter()
                        .filter(|record| record.rtype != T::RRSIG)
                    {
                        let signed = unrelated.sign_records(vec![record.clone()]).unwrap();
                        let mut signature = signed
                            .into_iter()
                            .find(|record| record.rtype == T::RRSIG)
                            .unwrap();
                        if corrupted {
                            *signature.rdata.last_mut().unwrap() ^= 1;
                        }
                        extras.push(signature);
                    }
                    if before {
                        reply.answer.splice(0..0, extras);
                    } else {
                        reply.answer.extend(extras);
                    }
                });
                let (verdict, queries) = run(&resolver, "news.example.com", NOW);
                assert_secure(verdict, "news.example.com");
                assert!(
                    queries
                        .iter()
                        .all(|query| !query.name().is_at_or_below(&n("unrelated.test")))
                );
            }
        }
    }
}

#[test]
fn unrelated_signatures_are_ignored_in_negative_answers_in_both_orders() {
    for before in [false, true] {
        let mut resolver = resolver();
        let unrelated = zone("unrelated.test");
        resolver.tamper(move |query, reply| {
            if query.rtype() != T::A {
                return;
            }
            let mut extras = Vec::new();
            for record in reply
                .authority
                .iter()
                .filter(|record| record.rtype == T::NSEC)
            {
                extras.extend(
                    unrelated
                        .sign_records(vec![record.clone()])
                        .unwrap()
                        .into_iter()
                        .filter(|record| record.rtype == T::RRSIG),
                );
            }
            if before {
                reply.authority.splice(0..0, extras);
            } else {
                reply.authority.extend(extras);
            }
        });
        let (verdict, _) = run(&resolver, "missing.example.com", NOW);
        let Verdict::Secure { answer, .. } = verdict else {
            panic!("expected secure denial, got {verdict:?}")
        };
        assert_eq!(*answer.outcome(), Outcome::NoSuchName);
    }
}

#[test]
fn multiple_plausible_ancestor_signers_reject_in_both_orders_even_with_a_valid_signature() {
    for before in [false, true] {
        for corrupt in [false, true] {
            let mut resolver = resolver();
            let parent = resolver.zones()[1].clone();
            resolver.tamper(move |query, reply| {
                if query.rtype() != T::A {
                    return;
                }
                let records: Vec<_> = reply
                    .answer
                    .iter()
                    .filter(|record| record.rtype == T::A)
                    .cloned()
                    .collect();
                let mut signature = parent
                    .sign_records(records)
                    .unwrap()
                    .into_iter()
                    .find(|record| record.rtype == T::RRSIG)
                    .unwrap();
                if corrupt {
                    *signature.rdata.last_mut().unwrap() ^= 1;
                }
                if before {
                    reply.answer.insert(0, signature);
                } else {
                    reply.answer.push(signature);
                }
            });
            let (verdict, queries) = run(&resolver, "news.example.com", NOW);
            assert_bogus(verdict, BogusReason::SignerMismatch);
            assert_eq!(queries.len(), 1);
        }
    }
}

#[test]
fn missing_unrelated_wrong_owner_and_bad_signatures_fail_closed() {
    for shape in 0..4 {
        let mut resolver = resolver();
        let unrelated = zone("unrelated.test");
        resolver.tamper(move |query, reply| {
            if query.rtype() != T::A {
                return;
            }
            match shape {
                0 => reply.strip_signatures(),
                1 => {
                    let record = reply
                        .answer
                        .iter()
                        .find(|record| record.rtype == T::A)
                        .unwrap()
                        .clone();
                    reply.answer = unrelated.sign_records(vec![record]).unwrap();
                }
                2 => {
                    for signature in reply
                        .answer
                        .iter_mut()
                        .filter(|record| record.rtype == T::RRSIG)
                    {
                        signature.owner = n("other.example.com");
                    }
                }
                _ => {
                    for signature in reply
                        .answer
                        .iter_mut()
                        .filter(|record| record.rtype == T::RRSIG)
                    {
                        *signature.rdata.last_mut().unwrap() ^= 1;
                    }
                }
            }
        });
        assert_bogus(
            run(&resolver, "news.example.com", NOW).0,
            match shape {
                // No signature is left in the reply at all.
                0 => BogusReason::NoDnssecRecords,
                3 => BogusReason::SignatureInvalid,
                _ => BogusReason::SignaturesMissing,
            },
        );
    }
}

#[test]
fn bad_signature_before_or_after_valid_signature_does_not_mask_success() {
    for before in [false, true] {
        let mut resolver = resolver();
        resolver.tamper(move |query, reply| {
            if query.rtype() != T::A {
                return;
            }
            let mut bad = reply
                .answer
                .iter()
                .find(|record| record.rtype == T::RRSIG)
                .unwrap()
                .clone();
            *bad.rdata.last_mut().unwrap() ^= 1;
            if before {
                reply.answer.insert(0, bad);
            } else {
                reply.answer.push(bad);
            }
        });
        assert_secure(
            run(&resolver, "news.example.com", NOW).0,
            "news.example.com",
        );
    }
}

#[test]
fn signature_dates_allow_a_day_of_skew_and_include_exact_endpoints() {
    // A 1 000 000 s lifetime gets the 86 400 s maximum tolerance.
    for (inception, expiration, now, expected) in [
        (
            1_000_000,
            2_000_000,
            913_599,
            Some(BogusReason::SignatureNotYetValid),
        ),
        (1_000_000, 2_000_000, 913_600, None),
        (1_000_000, 2_000_000, 999_999, None),
        (1_000_000, 2_000_000, 1_000_000, None),
        (1_000_000, 2_000_000, 1_000_001, None),
        (1_000_000, 2_000_000, 1_999_999, None),
        (1_000_000, 2_000_000, 2_000_000, None),
        (1_000_000, 2_000_000, 2_086_400, None),
        (
            1_000_000,
            2_000_000,
            2_086_401,
            Some(BogusReason::SignatureExpired),
        ),
        (
            2_000_000,
            1_000_000,
            NOW,
            Some(BogusReason::SignaturesMissing),
        ),
        (u32::MAX - 100, 1_000, (1_u64 << 32) + 20, None),
    ] {
        let mut resolver = resolver();
        // Keep trust-chain signatures valid; vary only the answer signature.
        let mut signer = resolver.zones()[2].clone();
        signer.signature_window = SignatureWindow {
            inception,
            expiration,
        };
        let signed = signer
            .sign_records(vec![Record::new(
                n("news.example.com"),
                T::A,
                300,
                IP.to_vec(),
            )])
            .unwrap();
        for apex in [".", "com", "example.com"] {
            resolver.zone_mut(&n(apex)).unwrap().signature_window = if now < 3_000_000 {
                SignatureWindow {
                    inception: 0,
                    expiration: 3_000_000,
                }
            } else {
                SignatureWindow {
                    inception: u32::MAX - 1_000,
                    expiration: 3_000,
                }
            };
        }
        resolver.tamper(move |query, reply| {
            if query.rtype() == T::A {
                reply.answer = signed.clone();
            }
        });
        let verdict = run(&resolver, "news.example.com", now).0;
        if let Some(reason) = expected {
            assert_bogus(verdict, reason);
        } else {
            assert_secure(verdict, "news.example.com");
        }
    }
}

fn prepend_twenty_one_invalid_signatures(answer: &mut Vec<Record>, valid: bool) {
    let signature = answer
        .iter()
        .find(|record| record.rtype == T::RRSIG)
        .unwrap()
        .clone();
    answer.retain(|record| record.rtype != T::RRSIG);
    for corruption in 1..=21 {
        let mut bad = signature.clone();
        *bad.rdata.last_mut().unwrap() ^= corruption;
        answer.push(bad);
    }
    if valid {
        answer.push(signature);
    }
    let signatures: Vec<_> = answer
        .iter()
        .filter(|record| record.rtype == T::RRSIG)
        .collect();
    let expected = 21 + usize::from(valid);
    assert_eq!(signatures.len(), expected);
    // Distinct RDATA must survive the wire parser's RRset deduplication.
    assert_eq!(
        signatures
            .iter()
            .map(|signature| &signature.rdata)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        expected,
    );
}

#[test]
fn ds_authenticated_dnskey_accepts_valid_signature_after_twenty_one_failures() {
    for valid in [false, true] {
        let mut resolver = resolver();
        resolver.tamper(move |query, reply| {
            if query.rtype() != T::DNSKEY || *query.name() != n("example.com") {
                return;
            }
            prepend_twenty_one_invalid_signatures(&mut reply.answer, valid);
        });
        let (verdict, checks) = run_with_signature_checks(&resolver, "news.example.com");
        if valid {
            assert_secure(verdict, "news.example.com");
            assert_eq!(checks, 27);
        } else {
            assert_bogus(verdict, BogusReason::KeysNotProven);
            assert_eq!(checks, 25);
        }
    }
}

#[test]
fn repeated_explicit_dnskey_work_still_exhausts_the_shared_crypto_budget() {
    for depth in [7, 8] {
        let mut zones = vec![zone(".")];
        let mut name = String::new();
        for _ in 0..depth {
            name = format!("branch.{name}");
            zones.push(zone(&name));
        }
        let owner = format!("news.{name}");
        zones
            .last_mut()
            .unwrap()
            .add_record(n(&owner), T::A, IP.to_vec());
        let mut resolver = FakeResolver::new(zones).unwrap();
        resolver.tamper(|query, reply| {
            if query.rtype() == T::DNSKEY {
                prepend_twenty_one_invalid_signatures(&mut reply.answer, true);
            }
        });
        let (verdict, checks) = run_with_signature_checks(&resolver, &owner);
        if depth == 7 {
            assert_secure(verdict, &owner);
            assert_eq!(checks, 184);
        } else {
            assert_bogus(verdict, BogusReason::WorkLimit);
            // Nine DS digests share the 200-operation budget with signatures.
            assert_eq!(checks, 191);
        }
    }
}
