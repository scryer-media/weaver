// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_dnssec::testing::{
    self, FakeReply, FakeResolver, KeyAlgorithm, Zone, drive, signature_checks,
};
use weaver_dnssec::{
    BogusReason, Name, Policy, Query, Question, RecordType as T, Step, TrustAnchor, Validator,
    Verdict,
};

const NOW: u64 = 1_500_000;
const ADDRESS: [u8; 4] = [192, 0, 2, 19];

fn name(text: &str) -> Name {
    Name::from_text(text).unwrap()
}

fn secure_address(verdict: Verdict, expected: &Name) {
    let Verdict::Secure { answer, .. } = verdict else {
        panic!("expected secure address, got {verdict:?}");
    };
    assert_eq!(
        *answer.outcome(),
        testing::records(expected.clone(), vec![ADDRESS.to_vec()])
    );
}

fn alias_chain(
    links: usize,
    dname: bool,
    looped: bool,
    unsigned: bool,
) -> (FakeResolver, Name, Name) {
    let root = Zone::new(Name::root(), KeyAlgorithm::Ed25519).unwrap();
    let mut zone = if unsigned {
        Zone::new(name("example"), KeyAlgorithm::Ed25519).unwrap()
    } else {
        root.clone()
    };
    zone.unsigned = unsigned;
    let suffix = if unsigned { ".example" } else { "" };
    for index in 0..links {
        let target = if looped && index + 1 == links {
            0
        } else {
            index + 1
        };
        zone.add_record(
            name(&format!("a{index}{suffix}")),
            if dname { T::DNAME } else { T::CNAME },
            name(&format!("a{target}{suffix}")).as_wire().to_vec(),
        );
    }
    let prefix = if dname { "host." } else { "" };
    let start = name(&format!("{prefix}a0{suffix}"));
    let end = name(&format!("{prefix}a{links}{suffix}"));
    if !looped {
        zone.add_record(end.clone(), T::A, ADDRESS.to_vec());
    }
    let zones = if unsigned {
        vec![root, zone]
    } else {
        vec![zone]
    };
    (FakeResolver::new(zones).unwrap(), start, end)
}

#[test]
fn signed_aliases_are_not_limited_by_the_resolver_cache_extraction_counter() {
    // dnsmasq's ten-link cache extraction counter is not a DNSSEC limit.
    for dname in [false, true] {
        for links in [9, 10, 11] {
            for looped in [false, true] {
                let (resolver, start, end) = alias_chain(links, dname, looped, false);
                let validator =
                    Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default())
                        .unwrap();
                let (verdict, _) = drive(
                    &validator,
                    &resolver,
                    Question::new(start, T::A).unwrap(),
                    NOW,
                );
                if looped {
                    assert!(matches!(verdict, Verdict::Bogus { failure, .. }
                        if failure.reason() == BogusReason::AliasInvalid));
                } else {
                    secure_address(verdict, &end);
                }
            }
        }
    }
}

fn counted_address_validation(
    validator: &Validator,
    resolver: &FakeResolver,
    start: Name,
) -> (Verdict, usize) {
    let mut validation = validator.begin(Question::new(start, T::A).unwrap(), NOW);
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

#[test]
fn signed_alias_chains_share_the_crypto_budget_with_cold_and_warm_keys() {
    for dname in [false, true] {
        for warm in [false, true] {
            let last_success = if warm { 199 } else { 197 };
            for links in [last_success, last_success + 1] {
                let (resolver, start, end) = alias_chain(links, dname, false, false);
                let validator =
                    Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default())
                        .unwrap();
                if warm {
                    secure_address(
                        drive(
                            &validator,
                            &resolver,
                            Question::new(end.clone(), T::A).unwrap(),
                            NOW,
                        )
                        .0,
                        &end,
                    );
                }
                let (verdict, checks) = counted_address_validation(&validator, &resolver, start);
                if links == last_success {
                    secure_address(verdict, &end);
                } else {
                    assert!(matches!(verdict, Verdict::Bogus { failure, .. }
                        if failure.reason() == BogusReason::WorkLimit));
                }
                // A cold root also spends one crypto operation on its DS digest.
                assert_eq!(checks, if warm { 200 } else { 199 });
            }
        }
    }
}

#[test]
fn unsigned_alias_chains_obey_the_local_traversal_bound_and_reject_loops() {
    for dname in [false, true] {
        for links in [199, 200, 201] {
            for looped in [false, true] {
                let (resolver, start, end) = alias_chain(links, dname, looped, true);
                let validator =
                    Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default())
                        .unwrap();
                let (verdict, checks) = counted_address_validation(&validator, &resolver, start);
                if looped || links > 200 {
                    let reason = if looped {
                        BogusReason::AliasInvalid
                    } else {
                        BogusReason::WorkLimit
                    };
                    assert!(
                        matches!(verdict, Verdict::Bogus { failure, .. } if failure.reason() == reason)
                    );
                } else {
                    let Verdict::Insecure { answer, .. } = verdict else {
                        panic!("expected authenticated unsigned delegation, got {verdict:?}");
                    };
                    assert_eq!(
                        *answer.outcome(),
                        testing::records(end, vec![ADDRESS.to_vec()]),
                    );
                }
                // Alias traversal under the proved unsigned cut does no crypto.
                assert_eq!(checks, 2);
            }
        }
    }
}

#[test]
fn signature_time_includes_both_endpoints_with_the_minimum_hour_of_skew() {
    // A 100 s lifetime gets the one-hour minimum tolerance on both ends.
    for (inception, expiration, failure) in [
        (NOW as u32, NOW as u32 + 100, None),
        (NOW as u32 - 100, NOW as u32, None),
        (NOW as u32 + 3_600, NOW as u32 + 3_700, None),
        (NOW as u32 - 3_700, NOW as u32 - 3_600, None),
        (
            NOW as u32 + 3_601,
            NOW as u32 + 3_700,
            Some(BogusReason::SignatureNotYetValid),
        ),
        (
            NOW as u32 - 3_700,
            NOW as u32 - 3_601,
            Some(BogusReason::SignatureExpired),
        ),
    ] {
        let mut root = Zone::new(Name::root(), KeyAlgorithm::Ed25519).unwrap();
        root.add_record(name("host"), T::A, ADDRESS.to_vec());
        let mut resolver = FakeResolver::new(vec![root.clone()]).unwrap();
        root.signature_window.inception = inception;
        root.signature_window.expiration = expiration;
        let query = Query::new(name("host"), T::A);
        let reply = FakeReply {
            answer: root.signed_rrset(query.name(), T::A).unwrap(),
            ..FakeReply::default()
        };
        resolver.set_reply(query.clone(), Some(reply.encode(&query).unwrap()));
        let validator =
            Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap();
        let (verdict, _) = drive(
            &validator,
            &resolver,
            Question::new(query.name().clone(), T::A).unwrap(),
            NOW,
        );
        if let Some(reason) = failure {
            assert!(
                matches!(verdict, Verdict::Bogus { failure, .. } if failure.reason() == reason)
            );
        } else {
            secure_address(verdict, query.name());
        }
    }
}

fn hex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2));
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn sha1_fixture(field: &str) -> Vec<u8> {
    let value = include_str!("fixtures/rsa-sha1.txt")
        .lines()
        .find_map(|line| {
            line.strip_prefix(field)
                .and_then(|rest| rest.strip_prefix(' '))
        })
        .unwrap();
    hex(value)
}

#[test]
fn rsa_sha1_algorithms_five_and_seven_require_explicit_compatibility_policy() {
    let anchor = TrustAnchor::new(
        Name::root(),
        u16::from_be_bytes(sha1_fixture("KEY_TAG").try_into().unwrap()),
        8,
        2,
        sha1_fixture("ANCHOR_DIGEST"),
    );
    for algorithm in [5, 7] {
        for permitted in [true, false] {
            let mut policy = Policy::default();
            if permitted {
                policy.sha1_signatures = true;
            }
            let validator = Validator::new(vec![anchor.clone()], policy).unwrap();
            let owner = name(&format!("a{algorithm}"));
            let mut validation = validator.begin(Question::new(owner.clone(), T::A).unwrap(), NOW);
            validation.set_ask_ahead(false);
            let verdict = loop {
                match validation.step() {
                    Step::Need { awaited: query, .. } => {
                        let reply = match query.rtype() {
                            T::DNSKEY => sha1_fixture("DNSKEY_REPLY"),
                            T::A => sha1_fixture(&format!("A{algorithm}_REPLY")),
                            _ => panic!("unexpected query {query:?}"),
                        };
                        validation.supply(&query, &reply).unwrap();
                    }
                    Step::Done { verdict, .. } => break verdict,
                    step => panic!("unexpected step {step:?}"),
                }
            };
            if permitted {
                secure_address(verdict, &owner);
            } else {
                assert!(
                    matches!(&verdict, Verdict::Bogus { failure, .. }
                    if failure.reason() == BogusReason::SignaturesMissing),
                    "{verdict:?}"
                );
            }
        }
    }
}

#[test]
fn stronger_supported_ds_displaces_sha1_in_either_signed_rrset_order() {
    for (weak_valid, strong_valid, algorithm, digest, accepted) in [
        (true, false, 13, 2, false),
        (true, true, 13, 2, true),
        (false, true, 13, 2, true),
        (true, true, 253, 2, true),
        (true, true, 13, 253, true),
    ] {
        for reverse in [false, true] {
            let mut root = Zone::new(Name::root(), KeyAlgorithm::P256).unwrap();
            let mut leaf = Zone::new(name("example."), KeyAlgorithm::P256).unwrap();
            let owner = name("news.example.");
            leaf.add_record(owner.clone(), T::A, ADDRESS.to_vec());
            let mut weak = leaf.ds_records(1).unwrap().remove(0);
            let mut strong = leaf.ds_records(2).unwrap().remove(0);
            if !weak_valid {
                *weak.last_mut().unwrap() ^= 1;
            }
            if !strong_valid {
                *strong.last_mut().unwrap() ^= 1;
            }
            strong[2] = algorithm;
            strong[3] = digest;
            let mut records = vec![weak, strong];
            if reverse {
                records.reverse();
            }
            // The signature authenticates the mixed DS RRset, including any
            // bad digest: rejection must come from DS selection and matching.
            root.delegate(name("example."), records);
            let resolver = FakeResolver::new(vec![root, leaf]).unwrap();
            let validator =
                Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap();
            let (verdict, queries) = drive(
                &validator,
                &resolver,
                Question::new(owner.clone(), T::A).unwrap(),
                NOW,
            );
            assert!(queries.contains(&Query::new(name("example."), T::DNSKEY)));
            if accepted {
                secure_address(verdict, &owner);
            } else {
                assert!(matches!(verdict, Verdict::Bogus { failure, .. }
                    if failure.reason() == BogusReason::KeysNotProven));
            }
        }
    }
}
