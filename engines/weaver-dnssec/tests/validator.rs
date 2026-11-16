// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_dnssec::testing::{
    self, DenialMode, FakeReply, FakeResolver, KeyAlgorithm, Record, Zone, drive, signature_checks,
};
use weaver_dnssec::{
    BogusReason, Name, Outcome, Policy, Query, Question, RecordType as T, Step, SupplyError,
    UnansweredCause, Validator, Verdict,
};

const NOW: u64 = 1_100_000;
const IP: [u8; 4] = [203, 0, 113, 7];

fn n(text: &str) -> Name {
    Name::from_text(text).unwrap()
}
fn q(text: &str, rtype: T) -> Query {
    Query::new(n(text), rtype)
}
fn question(text: &str, rtype: T) -> Question {
    Question::new(n(text), rtype).unwrap()
}
fn zone(text: &str) -> Zone {
    Zone::new(n(text), KeyAlgorithm::P256).unwrap()
}
fn tree() -> FakeResolver {
    let mut leaf = zone("example.com");
    leaf.add_record(n("news.example.com"), T::A, IP.to_vec());
    leaf.add_record(n("other.example.com"), T::A, IP.to_vec());
    FakeResolver::new(vec![zone("."), zone("com"), leaf]).unwrap()
}
fn validator(resolver: &FakeResolver) -> Validator {
    Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap()
}
fn run(resolver: &FakeResolver, name: &str, rtype: T) -> (Verdict, Vec<Query>) {
    drive(&validator(resolver), resolver, question(name, rtype), NOW)
}
fn secure(verdict: &Verdict, expected: Outcome) {
    let Verdict::Secure { answer, .. } = verdict else {
        panic!("expected secure {expected:?}, got {verdict:?}")
    };
    assert_eq!(*answer.outcome(), expected);
    assert_eq!(answer.ttl(), 300);
}
fn records(name: &str) -> Outcome {
    testing::records(n(name), vec![IP.to_vec()])
}
fn insecure(verdict: &Verdict, expected: Outcome) {
    let Verdict::Insecure { answer, .. } = verdict else {
        panic!("expected insecure {expected:?}, got {verdict:?}")
    };
    assert_eq!(*answer.outcome(), expected);
}
fn bogus(verdict: &Verdict, reason: BogusReason) {
    let Verdict::Bogus { failure, .. } = verdict else {
        panic!("expected bogus {reason:?}, got {verdict:?}")
    };
    assert_eq!(failure.reason(), reason);
}
fn cold_queries() -> Vec<Query> {
    vec![
        q("news.example.com", T::A),
        q(".", T::DNSKEY),
        q("com", T::DS),
        q("com", T::DNSKEY),
        q("example.com", T::DS),
        q("example.com", T::DNSKEY),
    ]
}
fn nsec(owner: &str, next: &str, types: &[T]) -> Record {
    let mut rdata = n(next).as_wire().to_vec();
    let max = types.iter().map(|t| t.get()).max().unwrap_or(0);
    assert!(max < 256);
    if !types.is_empty() {
        let mut bits = vec![0; usize::from(max / 8) + 1];
        for t in types {
            bits[usize::from(t.get() / 8)] |= 0x80 >> (t.get() % 8);
        }
        rdata.extend([0, bits.len() as u8]);
        rdata.extend(bits);
    }
    Record::new(n(owner), T::NSEC, 300, rdata)
}
fn replace_reply(resolver: &mut FakeResolver, query: Query, reply: FakeReply) {
    resolver.set_reply(query.clone(), Some(reply.encode(&query).unwrap()));
}
fn nsec3_mode(opt_out: bool, iterations: u16) -> DenialMode {
    DenialMode::Nsec3 {
        salt: vec![0xaa, 0xbb],
        iterations,
        opt_out,
    }
}
fn negative_resolver(mode: DenialMode) -> FakeResolver {
    let mut resolver = tree();
    resolver.zone_mut(&n("example.com")).unwrap().denial = mode;
    resolver
}

#[test]
fn secure_a_record_three_zones() {
    let resolver = tree();
    let (verdict, queries) = run(&resolver, "news.example.com", T::A);
    secure(&verdict, records("news.example.com"));
    assert_eq!(queries, cold_queries());
    let Verdict::Secure { answer, .. } = verdict else {
        unreachable!()
    };
    assert_eq!(answer.addresses(), vec![std::net::IpAddr::V4(IP.into())]);
}

#[test]
fn second_lookup_uses_the_cache() {
    let resolver = tree();
    let v = validator(&resolver);
    secure(
        &drive(&v, &resolver, question("news.example.com", T::A), NOW).0,
        records("news.example.com"),
    );
    let (verdict, queries) = drive(&v, &resolver, question("other.example.com", T::A), NOW);
    secure(&verdict, records("other.example.com"));
    assert_eq!(queries, vec![q("other.example.com", T::A)]);
}

#[test]
fn cache_entry_expires() {
    let resolver = tree();
    let v = validator(&resolver);
    secure(
        &drive(&v, &resolver, question("news.example.com", T::A), NOW).0,
        records("news.example.com"),
    );
    let (verdict, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW + 301);
    secure(&verdict, records("news.example.com"));
    assert_eq!(queries, cold_queries());
}

#[test]
fn secure_through_empty_non_terminal() {
    let mut leaf = zone("a.b.example.com");
    leaf.add_record(n("news.a.b.example.com"), T::A, IP.to_vec());
    let resolver = FakeResolver::new(vec![zone("."), zone("example.com"), leaf]).unwrap();
    let (verdict, queries) = run(&resolver, "news.a.b.example.com", T::A);
    secure(&verdict, records("news.a.b.example.com"));
    assert_eq!(
        queries,
        vec![
            q("news.a.b.example.com", T::A),
            q(".", T::DNSKEY),
            q("com", T::DS),
            q("example.com", T::DS),
            q("example.com", T::DNSKEY),
            q("b.example.com", T::DS),
            q("a.b.example.com", T::DS),
            q("a.b.example.com", T::DNSKEY)
        ]
    );
}

#[test]
fn secure_zone_two_labels_below_parent() {
    let mut child = zone("a.b");
    child.add_record(n("news.a.b"), T::A, IP.to_vec());
    let resolver = FakeResolver::new(vec![zone("."), child]).unwrap();
    let (verdict, queries) = run(&resolver, "news.a.b", T::A);
    secure(&verdict, records("news.a.b"));
    assert_eq!(
        queries,
        vec![
            q("news.a.b", T::A),
            q(".", T::DNSKEY),
            q("b", T::DS),
            q("a.b", T::DS),
            q("a.b", T::DNSKEY)
        ]
    );
}

fn aliases(dname: bool) -> FakeResolver {
    let mut first = zone("example.com");
    let mut second = zone("example.net");
    if dname {
        first.add_record(
            n("old.example.com"),
            T::DNAME,
            n("example.net").as_wire().to_vec(),
        );
    } else {
        first.add_record(
            n("news.example.com"),
            T::CNAME,
            n("news.example.net").as_wire().to_vec(),
        );
    }
    second.add_record(n("news.example.net"), T::A, IP.to_vec());
    FakeResolver::new(vec![zone("."), first, second]).unwrap()
}

#[test]
fn secure_cname_chain_two_zones() {
    let resolver = aliases(false);
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.net"),
    );
}
#[test]
fn secure_dname_with_unsigned_cname() {
    let resolver = aliases(true);
    secure(
        &run(&resolver, "news.old.example.com", T::A).0,
        records("news.example.net"),
    );
}
#[test]
fn secure_dname_without_cname() {
    let mut resolver = aliases(true);
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.answer.retain(|r| r.rtype != T::CNAME);
        }
    });
    secure(
        &run(&resolver, "news.old.example.com", T::A).0,
        records("news.example.net"),
    );
}
fn alias_chain(verdict: &Verdict) -> Vec<Name> {
    let Some(answer) = verdict.answer() else {
        panic!("expected an answer, got {verdict:?}")
    };
    answer.alias_chain().to_vec()
}
#[test]
fn a_cname_answer_lists_the_question_then_the_target() {
    let resolver = aliases(false);
    let (verdict, _) = run(&resolver, "news.example.com", T::A);
    secure(&verdict, records("news.example.net"));
    assert_eq!(
        alias_chain(&verdict),
        vec![n("news.example.com"), n("news.example.net")]
    );
}
#[test]
fn a_dname_answer_lists_the_question_then_the_synthesised_target() {
    let resolver = aliases(true);
    let (verdict, _) = run(&resolver, "news.old.example.com", T::A);
    secure(&verdict, records("news.example.net"));
    assert_eq!(
        alias_chain(&verdict),
        vec![n("news.old.example.com"), n("news.example.net")]
    );
}
#[test]
fn an_unaliased_answer_lists_only_the_question() {
    let resolver = tree();
    let (verdict, _) = run(&resolver, "news.example.com", T::A);
    assert_eq!(alias_chain(&verdict), vec![n("news.example.com")]);
}

fn wildcard(mode: DenialMode) -> FakeResolver {
    let mut leaf = zone("example.com");
    leaf.denial = mode;
    leaf.add_record(n("*.example.com"), T::A, IP.to_vec());
    FakeResolver::new(vec![zone("."), leaf]).unwrap()
}
#[test]
fn secure_wildcard_with_nsec_proof() {
    let resolver = wildcard(DenialMode::Nsec);
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
    );
}
#[test]
fn secure_wildcard_with_nsec3_proof() {
    let resolver = wildcard(nsec3_mode(false, 0));
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
    );
}
#[test]
fn secure_nodata_nsec() {
    let resolver = negative_resolver(DenialMode::Nsec);
    secure(
        &run(&resolver, "news.example.com", T::AAAA).0,
        Outcome::NoData,
    );
}
#[test]
fn secure_nodata_nsec3() {
    let resolver = negative_resolver(nsec3_mode(false, 0));
    secure(
        &run(&resolver, "news.example.com", T::AAAA).0,
        Outcome::NoData,
    );
}
#[test]
fn secure_nxdomain_nsec() {
    let resolver = negative_resolver(DenialMode::Nsec);
    secure(
        &run(&resolver, "missing.example.com", T::A).0,
        Outcome::NoSuchName,
    );
}
#[test]
fn secure_nxdomain_nsec3() {
    let resolver = negative_resolver(nsec3_mode(false, 0));
    secure(
        &run(&resolver, "missing.example.com", T::A).0,
        Outcome::NoSuchName,
    );
}
#[test]
fn secure_nodata_wildcard() {
    for mode in [DenialMode::Nsec, nsec3_mode(false, 0)] {
        let resolver = wildcard(mode);
        secure(
            &run(&resolver, "news.example.com", T::AAAA).0,
            Outcome::NoData,
        );
    }
}
#[test]
fn secure_nodata_ent() {
    for mode in [DenialMode::Nsec, nsec3_mode(false, 0)] {
        let mut resolver = negative_resolver(mode);
        resolver.zone_mut(&n("example.com")).unwrap().add_record(
            n("leaf.ent.example.com"),
            T::A,
            IP.to_vec(),
        );
        secure(&run(&resolver, "ent.example.com", T::A).0, Outcome::NoData);
    }
}
#[test]
fn nxdomain_rcode_with_nodata_proof_is_nodata() {
    let mut resolver = tree();
    resolver.tamper(|query, reply| {
        if query.rtype() == T::AAAA {
            reply.rcode = 3;
        }
    });
    secure(
        &run(&resolver, "news.example.com", T::AAAA).0,
        Outcome::NoData,
    );
}
#[test]
fn secure_cname_to_missing_name_with_noerror_rcode() {
    let mut resolver = aliases(false);
    resolver
        .zone_mut(&n("example.net"))
        .unwrap()
        .records
        .retain(|r| r.rtype != T::A);
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.rcode = 0;
        }
    });
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        Outcome::NoSuchName,
    );
}
#[test]
fn secure_nodata_after_cname_into_other_zone_with_first_zones_nsec_present() {
    let mut resolver = aliases(false);
    let extra = resolver.zones()[1].denial_records().unwrap();
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::AAAA {
            reply.authority.splice(0..0, extra.clone());
        }
    });
    secure(
        &run(&resolver, "news.example.com", T::AAAA).0,
        Outcome::NoData,
    );
}

#[test]
fn secure_during_key_rollover_two_ds_one_matches() {
    for reverse in [false, true] {
        let mut resolver = tree();
        let mut ds = resolver.zones()[2].ds_records(2).unwrap();
        let mut stale = ds[0].clone();
        stale[0] ^= 1;
        ds.push(stale);
        if reverse {
            ds.reverse();
        }
        resolver
            .zone_mut(&n("com"))
            .unwrap()
            .delegate(n("example.com"), ds);
        secure(
            &run(&resolver, "news.example.com", T::A).0,
            records("news.example.com"),
        );
    }
}

#[test]
fn compressed_rrsig_signer_name_still_verifies() {
    let mut resolver = tree();
    resolver.tamper_bytes(|query, bytes| {
        if query != &q("news.example.com", T::A) {
            return;
        }
        // The answer encoder emits uncompressed records. Point the RRSIG signer
        // back at the example.com suffix in the question (offset 17).
        let mut position = 12 + query.name().as_wire().len() + 4;
        for _ in 0..2 {
            while bytes[position] != 0 {
                position += usize::from(bytes[position]) + 1;
            }
            position += 1;
            let rtype = u16::from_be_bytes([bytes[position], bytes[position + 1]]);
            let length_at = position + 8;
            let length = usize::from(u16::from_be_bytes([bytes[length_at], bytes[length_at + 1]]));
            let start = position + 10;
            if rtype == 46 {
                let signer_start = start + 18;
                let signer_len = n("example.com").as_wire().len();
                bytes.splice(signer_start..signer_start + signer_len, [0xc0, 17]);
                let new_length = u16::try_from(length - signer_len + 2).unwrap();
                bytes[length_at..length_at + 2].copy_from_slice(&new_length.to_be_bytes());
                return;
            }
            position = start + length;
        }
        panic!("test fixture has no RRSIG");
    });
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
    );
}

#[test]
fn positive_path_per_key_type() {
    for algorithm in [
        KeyAlgorithm::P256,
        KeyAlgorithm::Ed25519,
        KeyAlgorithm::Rsa2048,
        KeyAlgorithm::MlDsa44,
    ] {
        let mut leaf = Zone::new(n("example.com"), algorithm).unwrap();
        leaf.add_record(n("news.example.com"), T::A, IP.to_vec());
        let resolver = FakeResolver::new(vec![zone("."), leaf]).unwrap();
        secure(
            &run(&resolver, "news.example.com", T::A).0,
            records("news.example.com"),
        );
    }
}

fn mldsa_tree() -> FakeResolver {
    let mut leaf = Zone::new(n("example"), KeyAlgorithm::MlDsa44).unwrap();
    leaf.add_record(n("news.example"), T::A, IP.to_vec());
    FakeResolver::new(vec![zone("."), leaf]).unwrap()
}

#[test]
fn mldsa_chain_authenticates_keys_once_and_reuses_the_cache() {
    let resolver = mldsa_tree();
    let v = validator(&resolver);
    let mut validation = v.begin(question("news.example", T::A), NOW);
    validation.set_ask_ahead(false);
    let mut queries = Vec::new();
    loop {
        let step = validation.step();
        let checks = signature_checks(&validation);
        assert_eq!(validation.step(), step);
        assert_eq!(signature_checks(&validation), checks);
        match step {
            Step::Need { awaited: query, .. } => {
                queries.push(query.clone());
                validation
                    .supply(&query, &resolver.answer(&query).unwrap())
                    .unwrap();
            }
            Step::Done { verdict, .. } => {
                secure(&verdict, records("news.example"));
                break;
            }
            step => panic!("unexpected step {step:?}"),
        }
    }
    assert_eq!(
        queries,
        [
            q("news.example", T::A),
            q(".", T::DNSKEY),
            q("example", T::DS),
            q("example", T::DNSKEY),
        ]
    );
    assert_eq!(signature_checks(&validation), 4);
    let (verdict, asked) = drive(&v, &resolver, question("news.example", T::A), NOW);
    secure(&verdict, records("news.example"));
    assert_eq!(asked, [q("news.example", T::A)]);
}

#[test]
fn mldsa_trust_anchor_authenticates_a_classical_child() {
    let root = Zone::new(n("."), KeyAlgorithm::MlDsa44).unwrap();
    let mut child = zone("example");
    child.add_record(n("news.example"), T::A, IP.to_vec());
    let resolver = FakeResolver::new(vec![root, child]).unwrap();
    let (verdict, asked) = run(&resolver, "news.example", T::A);
    secure(&verdict, records("news.example"));
    assert_eq!(
        asked,
        [
            q("news.example", T::A),
            q(".", T::DNSKEY),
            q("example", T::DS),
            q("example", T::DNSKEY),
        ]
    );
}

#[test]
fn mldsa_answer_rejects_bad_missing_expired_and_future_signatures() {
    for case in 0..5 {
        let mut resolver = mldsa_tree();
        if case >= 3 {
            // Sign the answer with the wrong time window while leaving the
            // authenticated DNSKEY response valid.
            let mut signer = resolver.zones()[1].clone();
            if case == 3 {
                signer.signature_window.expiration = 1_000_001;
            } else {
                signer.signature_window.inception = 1_500_000;
            }
            replace_reply(
                &mut resolver,
                q("news.example", T::A),
                FakeReply {
                    answer: signer.signed_rrset(&n("news.example"), T::A).unwrap(),
                    ..FakeReply::default()
                },
            );
        } else {
            resolver.tamper(move |query, reply| {
                if query != &q("news.example", T::A) {
                    return;
                }
                if case == 0 {
                    reply.answer[0].rdata[3] ^= 1;
                } else if case == 1 {
                    let signature = reply
                        .answer
                        .iter_mut()
                        .find(|record| record.rtype == T::RRSIG)
                        .unwrap();
                    *signature.rdata.last_mut().unwrap() ^= 1;
                } else {
                    reply.strip_signatures();
                }
            });
        }
        bogus(
            &run(&resolver, "news.example", T::A).0,
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
fn mldsa_dnskey_and_parent_ds_tampering_fail_closed() {
    for tamper_dnskey in [false, true] {
        let mut resolver = mldsa_tree();
        if tamper_dnskey {
            resolver.tamper(|query, reply| {
                if query == &q("example", T::DNSKEY) {
                    let key = reply
                        .answer
                        .iter_mut()
                        .find(|record| record.rtype == T::DNSKEY)
                        .unwrap();
                    *key.rdata.last_mut().unwrap() ^= 1;
                }
            });
        } else {
            // A valid parent signature over a wrong algorithm-18 DS must not
            // downgrade the supported delegation to an insecure answer.
            let mut ds = resolver.zones()[1].ds_records(2).unwrap();
            *ds[0].last_mut().unwrap() ^= 1;
            resolver
                .zone_mut(&n("."))
                .unwrap()
                .delegate(n("example"), ds);
        }
        let (verdict, asked) = run(&resolver, "news.example", T::A);
        bogus(&verdict, BogusReason::KeysNotProven);
        assert_eq!(asked.last(), Some(&q("example", T::DNSKEY)));
    }
}

#[test]
fn mldsa_signed_nsec_and_nsec3_prove_negative_answers_and_reject_tampering() {
    for mode in [DenialMode::Nsec, nsec3_mode(false, 0)] {
        for (owner, rtype, outcome) in [
            ("news.example", T::AAAA, Outcome::NoData),
            ("missing.example", T::A, Outcome::NoSuchName),
        ] {
            let mut resolver = mldsa_tree();
            resolver.zone_mut(&n("example")).unwrap().denial = mode.clone();
            secure(&run(&resolver, owner, rtype).0, outcome);
            resolver.tamper(|query, reply| {
                if query.rtype() == T::DNSKEY || query.rtype() == T::DS {
                    return;
                }
                for record in &mut reply.authority {
                    if record.rtype == T::RRSIG {
                        *record.rdata.last_mut().unwrap() ^= 1;
                    }
                }
            });
            bogus(&run(&resolver, owner, rtype).0, BogusReason::DenialInvalid);
        }
    }
}

#[test]
fn classical_to_mldsa_rollover_requires_the_ds_key_to_sign_dnskeys() {
    for reverse in [false, true] {
        for matching_signer in [false, true] {
            let mut child = zone("example");
            let mldsa = child.add_key(KeyAlgorithm::MlDsa44, 257).unwrap();
            child.add_record(n("news.example"), T::A, IP.to_vec());
            child.record_signers = vec![mldsa];
            child.dnskey_signers = vec![if matching_signer { mldsa } else { 0 }];
            let mut ds = vec![
                child.keys[0].ds_rdata(&child.apex, 2).unwrap(),
                child.keys[mldsa].ds_rdata(&child.apex, 2).unwrap(),
            ];
            // Model retirement of the classical DS while it is still present
            // in the signed rollover set; only the ML-DSA DS remains valid.
            *ds[0].last_mut().unwrap() ^= 1;
            if reverse {
                ds.reverse();
            }
            let mut parent = zone(".");
            parent.delegate(n("example"), ds);
            let resolver = FakeResolver::new(vec![parent, child]).unwrap();
            let (verdict, _) = run(&resolver, "news.example", T::A);
            if matching_signer {
                secure(&verdict, records("news.example"));
            } else {
                bogus(&verdict, BogusReason::KeysNotProven);
            }
        }
    }
}

#[test]
fn mldsa_invalid_signatures_obey_the_rrset_work_budget() {
    for failures in [8_u8, 19, 20] {
        let mut resolver = mldsa_tree();
        resolver.tamper(move |query, reply| {
            if query != &q("news.example", T::A) {
                return;
            }
            let signature = reply.answer.pop().unwrap();
            assert_eq!(signature.rtype, T::RRSIG);
            for bad in 1..=failures {
                let mut invalid = signature.clone();
                *invalid.rdata.last_mut().unwrap() ^= bad;
                reply.answer.push(invalid);
            }
            reply.answer.push(signature);
        });
        let v = validator(&resolver);
        let mut validation = v.begin(question("news.example", T::A), NOW);
        validation.set_ask_ahead(false);
        let verdict = loop {
            match validation.step() {
                Step::Need { awaited: query, .. } => validation
                    .supply(&query, &resolver.answer(&query).unwrap())
                    .unwrap(),
                Step::Done { verdict, .. } => break verdict,
                step => panic!("unexpected step {step:?}"),
            }
        };
        // Twenty failures end the RRset before its valid signature is tried.
        if failures < 20 {
            secure(&verdict, records("news.example"));
            assert_eq!(signature_checks(&validation), 4 + usize::from(failures));
        } else {
            bogus(&verdict, BogusReason::WorkLimit);
            assert_eq!(signature_checks(&validation), 23);
        }
    }
}

fn unsigned(mode: DenialMode) -> FakeResolver {
    let mut parent = zone(".");
    parent.denial = mode;
    let mut child = zone("example");
    child.unsigned = true;
    child.add_record(n("news.example"), T::A, IP.to_vec());
    FakeResolver::new(vec![parent, child]).unwrap()
}
#[test]
fn insecure_unsigned_delegation_nsec() {
    let resolver = unsigned(DenialMode::Nsec);
    insecure(
        &run(&resolver, "news.example", T::A).0,
        records("news.example"),
    );
}
#[test]
fn insecure_unsigned_delegation_nsec3() {
    let resolver = unsigned(nsec3_mode(false, 0));
    insecure(
        &run(&resolver, "news.example", T::A).0,
        records("news.example"),
    );
}
#[test]
fn insecure_unsigned_delegation_nsec3_opt_out() {
    let resolver = unsigned(nsec3_mode(true, 0));
    insecure(
        &run(&resolver, "news.example", T::A).0,
        records("news.example"),
    );
}

fn unusable_ds(kind: u8) -> FakeResolver {
    let mut resolver = tree();
    let mut ds = resolver.zones()[2]
        .ds_records(if kind == 1 { 1 } else { 2 })
        .unwrap();
    if kind == 253 {
        for record in &mut ds {
            record[2] = 253;
        }
    }
    resolver
        .zone_mut(&n("com"))
        .unwrap()
        .delegate(n("example.com"), ds);
    resolver
}
#[test]
fn insecure_only_unsupported_algorithm_ds() {
    let resolver = unusable_ds(253);
    insecure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
    );
}
#[test]
fn secure_only_sha1_ds_default_policy() {
    let resolver = unusable_ds(1);
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
    );
}
#[test]
fn insecure_alg5_zone_with_default_policy() {
    let mut child = zone("example");
    child.keys[0].algorithm_number = 5;
    child.add_record(n("news.example"), T::A, IP.to_vec());
    let resolver = FakeResolver::new(vec![zone("."), child]).unwrap();
    let validator =
        Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap();
    insecure(
        &drive(&validator, &resolver, question("news.example", T::A), NOW).0,
        records("news.example"),
    );
}
#[test]
fn unusable_ds_set_asks_no_dnskey_query() {
    let resolver = unusable_ds(253);
    let (verdict, queries) = run(&resolver, "news.example.com", T::A);
    insecure(&verdict, records("news.example.com"));
    assert_eq!(queries, cold_queries()[..5]);
}
#[test]
fn bogus_nsec3_iterations_over_limit() {
    // A denial only over-limit NSEC3 could give is a failure, never insecure.
    let mut resolver = unsigned(nsec3_mode(false, 151));
    resolver.zone_mut(&n("example")).unwrap().unsigned = false;
    resolver
        .zone_mut(&n("."))
        .unwrap()
        .delegate(n("example"), vec![]);
    let verdict = run(&resolver, "news.example", T::A).0;
    bogus(&verdict, BogusReason::Nsec3Iterations);
}
#[test]
fn secure_cname_into_insecure_zone_is_insecure() {
    let mut resolver = aliases(false);
    resolver.zone_mut(&n("example.net")).unwrap().unsigned = true;
    resolver
        .zone_mut(&n("."))
        .unwrap()
        .delegate(n("example.net"), vec![]);
    insecure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.net"),
    );
}
#[test]
fn insecure_negative_answer_in_unsigned_zone() {
    let resolver = unsigned(DenialMode::Nsec);
    insecure(
        &run(&resolver, "missing.example", T::A).0,
        Outcome::NoSuchName,
    );
}
#[test]
fn insecure_negative_answer_proving_unsigned_delegation_above_name() {
    let mut resolver = unsigned(DenialMode::Nsec);
    let proofs = resolver.zones()[0].denial_records().unwrap();
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::A {
            reply.authority = proofs.clone();
        }
    });
    insecure(
        &run(&resolver, "missing.example", T::A).0,
        Outcome::NoSuchName,
    );
}
#[test]
fn insecure_wildcard_answer_under_opt_out_span() {
    let resolver = wildcard(nsec3_mode(true, 0));
    insecure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
    );
}
#[test]
fn insecure_zone_whose_only_ds_matched_key_is_rsa_512() {
    let mut child = Zone::new(n("example"), KeyAlgorithm::Rsa2048).unwrap();
    // Synthetic unusable RSA key: e=65537 and a 512-bit modulus. The real
    // private key is deliberately irrelevant; this key must never reach crypto.
    child.keys[0].public_key = [vec![3, 1, 0, 1], vec![0xff; 64]].concat();
    child.add_record(n("news.example"), T::A, IP.to_vec());
    let resolver = FakeResolver::new(vec![zone("."), child]).unwrap();
    insecure(
        &run(&resolver, "news.example", T::A).0,
        records("news.example"),
    );
}

#[test]
fn bogus_rrsigs_stripped_from_answer() {
    let mut resolver = tree();
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.strip_signatures();
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::NoDnssecRecords,
    );
}
#[test]
fn unsigned_answer_beside_other_signatures_is_signatures_missing() {
    let mut resolver = tree();
    let other = resolver.zones()[2]
        .signed_rrset(&n("other.example.com"), T::A)
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::A {
            reply.strip_signatures();
            reply.answer.extend(other.clone());
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignaturesMissing,
    );
}
#[test]
fn bogus_rrsigs_stripped_and_ds_reply_stripped() {
    let mut resolver = tree();
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.strip_signatures();
        }
        if query == &q("example.com", T::DS) {
            reply.answer.clear();
            reply.authority.clear();
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::NoDnssecRecords,
    );
}
#[test]
fn bogus_answer_record_changed() {
    let mut resolver = tree();
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.answer[0].rdata[3] ^= 1;
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignatureInvalid,
    );
}
#[test]
fn bogus_extra_record_added_to_rrset() {
    let mut resolver = tree();
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.answer.push(Record::new(
                query.name().clone(),
                T::A,
                300,
                vec![203, 0, 113, 99],
            ));
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignatureInvalid,
    );
}
#[test]
fn bogus_record_removed_from_rrset() {
    let mut resolver = tree();
    resolver.zone_mut(&n("example.com")).unwrap().add_record(
        n("news.example.com"),
        T::A,
        vec![203, 0, 113, 8],
    );
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            let index = reply.answer.iter().position(|r| r.rtype == T::A).unwrap();
            reply.answer.remove(index);
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignatureInvalid,
    );
}
#[test]
fn bogus_signature_expired() {
    let mut resolver = tree();
    resolver
        .zone_mut(&n("example.com"))
        .unwrap()
        .signature_window
        .expiration = 1_000_001;
    // Keep the DNSKEY reply's time window valid so the failure is the answer.
    let mut valid_keys = resolver.zones()[2].clone();
    valid_keys.signature_window.expiration = 2_000_000;
    replace_reply(
        &mut resolver,
        q("example.com", T::DNSKEY),
        FakeReply {
            answer: valid_keys
                .signed_rrset(&n("example.com"), T::DNSKEY)
                .unwrap(),
            ..FakeReply::default()
        },
    );
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignatureExpired,
    );
}
#[test]
fn bogus_signature_not_yet_valid() {
    let mut resolver = tree();
    resolver
        .zone_mut(&n("example.com"))
        .unwrap()
        .signature_window
        .inception = 1_500_000;
    let mut valid_keys = resolver.zones()[2].clone();
    valid_keys.signature_window.inception = 1_000_000;
    replace_reply(
        &mut resolver,
        q("example.com", T::DNSKEY),
        FakeReply {
            answer: valid_keys
                .signed_rrset(&n("example.com"), T::DNSKEY)
                .unwrap(),
            ..FakeReply::default()
        },
    );
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignatureNotYetValid,
    );
}

#[test]
fn bogus_signed_by_sibling_zone() {
    let mut resolver = aliases(false);
    let evil = resolver.zones()[2]
        .sign_records(vec![Record::new(
            n("news.example.com"),
            T::A,
            300,
            IP.to_vec(),
        )])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::A {
            reply.answer = evil.clone();
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignaturesMissing,
    );
}
#[test]
fn bogus_signed_by_child_claimed_as_zone() {
    let mut resolver = tree();
    let attacker = zone("fake.example.com");
    let evil = attacker
        .sign_records(vec![Record::new(
            n("news.fake.example.com"),
            T::A,
            300,
            IP.to_vec(),
        )])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::A {
            reply.answer = evil.clone();
        }
    });
    bogus(
        &run(&resolver, "news.fake.example.com", T::A).0,
        BogusReason::SignerNotAZone,
    );
}
#[test]
fn bogus_ds_signed_by_child() {
    let mut resolver = tree();
    let child = &resolver.zones()[2];
    let evil = child
        .sign_records(vec![Record::new(
            n("example.com"),
            T::DS,
            300,
            child.ds_records(2).unwrap()[0].clone(),
        )])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query == &q("example.com", T::DS) {
            reply.answer = evil.clone();
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignerMismatch,
    );
}
#[test]
fn bogus_dnskey_set_not_signed_by_ds_key() {
    let mut resolver = tree();
    let child = resolver.zone_mut(&n("example.com")).unwrap();
    let other = child.add_key(KeyAlgorithm::P256, 257).unwrap();
    child.dnskey_signers = vec![other];
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::KeysNotProven,
    );
}

fn false_ds_denial(types: &[T], owner: &str) -> FakeResolver {
    let mut resolver = tree();
    let proof = resolver.zones()[1]
        .sign_records(vec![nsec(owner, "com", types)])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query == &q("example.com", T::DS) {
            reply.answer.clear();
            reply.authority = proof.clone();
        }
    });
    resolver
}
#[test]
fn bogus_ds_denial_from_child_side() {
    let resolver = false_ds_denial(&[T::SOA, T::NS], "example.com");
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::DelegationUnproven,
    );
}
#[test]
fn bogus_ds_denial_with_ds_bit_set() {
    let resolver = false_ds_denial(&[T::NS, T::DS], "example.com");
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::DelegationUnproven,
    );
}
#[test]
fn bogus_insecure_claim_using_other_zones_nsec() {
    let mut resolver = tree();
    let proof = resolver.zones()[2]
        .sign_records(vec![nsec("example.com", "example.com", &[T::NS])])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query == &q("example.com", T::DS) {
            reply.answer.clear();
            reply.authority = proof.clone();
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::DelegationUnproven,
    );
}
#[test]
fn bogus_nxdomain_without_wildcard_denial() {
    let mut resolver = tree();
    let proof = resolver.zones()[2]
        .sign_records(vec![nsec("a.example.com", "z.example.com", &[T::A])])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::A {
            reply.authority = proof.clone();
        }
    });
    bogus(
        &run(&resolver, "missing.example.com", T::A).0,
        BogusReason::DenialInvalid,
    );
}
#[test]
fn bogus_nxdomain_proof_with_noerror_rcode() {
    let mut resolver = tree();
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.rcode = 0;
        }
    });
    bogus(
        &run(&resolver, "missing.example.com", T::A).0,
        BogusReason::DenialInvalid,
    );
}
#[test]
fn bogus_nodata_with_type_bit_set() {
    let mut resolver = tree();
    let proof = resolver.zones()[2]
        .sign_records(vec![nsec("news.example.com", "example.com", &[T::A])])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::A {
            reply.answer.clear();
            reply.authority = proof.clone();
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::DenialInvalid,
    );
}
#[test]
fn denial_nsec_keeps_its_signed_wildcard_owner() {
    let mut resolver = tree();
    let mut proof = resolver.zones()[2]
        .sign_records(vec![nsec(
            "*.example.com",
            "z.example.com",
            &[T::TXT, T::RRSIG, T::NSEC],
        )])
        .unwrap();
    for record in &mut proof {
        record.owner = n("news.example.com");
    }
    resolver.tamper(move |query, reply| {
        if query == &q("news.example.com", T::A) {
            reply.answer.clear();
            reply.authority = proof.clone();
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::DenialInvalid,
    );
}

#[test]
fn bogus_negative_answer_no_nsec_in_signed_zone() {
    let mut resolver = tree();
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.authority.clear();
        }
    });
    bogus(
        &run(&resolver, "missing.example.com", T::A).0,
        BogusReason::NoDnssecRecords,
    );
}
#[test]
fn bogus_wildcard_answer_without_closer_name_proof() {
    let mut resolver = wildcard(DenialMode::Nsec);
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.authority.clear();
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::WildcardUnproven,
    );
}
#[test]
fn bogus_wildcard_answer_with_closer_wildcard_nsec() {
    let mut resolver = wildcard(DenialMode::Nsec);
    let leaf = resolver.zone_mut(&n("example.com")).unwrap();
    leaf.add_record(n("*.b.example.com"), T::A, vec![203, 0, 113, 9]);
    leaf.add_record(n("c.example.com"), T::A, IP.to_vec());
    let mut expanded = leaf.signed_rrset(&n("*.example.com"), T::A).unwrap();
    for record in &mut expanded {
        record.owner = n("a.b.example.com");
    }
    let proofs = leaf
        .sign_records(vec![nsec("*.b.example.com", "c.example.com", &[T::A])])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::A {
            reply.answer = expanded.clone();
            reply.authority = proofs.clone();
        }
    });
    bogus(
        &run(&resolver, "a.b.example.com", T::A).0,
        BogusReason::WildcardUnproven,
    );
}
#[test]
fn bogus_ds_stripped_with_unrelated_nsec3_of_low_iteration_parent() {
    let mut resolver = tree();
    let mut parent = resolver.zones()[1].clone();
    parent.denial = nsec3_mode(false, 0);
    let chain = parent.denial_records().unwrap();
    // Give a real signed parent record that exactly matches the parent apex,
    // not the child's hash and not the next-closer proof's covering record.
    let apex_hash = weaver_dnssec::testing::base32hex(&weaver_dnssec::testing::nsec3_hash(
        &n("com"),
        &[0xaa, 0xbb],
        0,
    ));
    let owner = n(&format!("{apex_hash}.com"));
    let proof: Vec<_> = chain.into_iter().filter(|r| r.owner == owner).collect();
    resolver.tamper(move |query, reply| {
        if query == &q("example.com", T::DS) {
            reply.answer.clear();
            reply.authority = proof.clone();
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::DelegationUnproven,
    );
}

#[test]
fn bogus_wildcard_dname() {
    let mut resolver = aliases(true);
    let leaf = resolver.zone_mut(&n("example.com")).unwrap();
    leaf.add_record(
        n("*.example.com"),
        T::DNAME,
        n("example.net").as_wire().to_vec(),
    );
    let mut evil = leaf.signed_rrset(&n("*.example.com"), T::DNAME).unwrap();
    for record in &mut evil {
        record.owner = n("old.example.com");
    }
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::A {
            reply.answer.retain(|r| {
                r.rtype != T::DNAME
                    && !(r.rtype == T::RRSIG && r.rdata[..2] == 39_u16.to_be_bytes())
            });
            reply.answer.splice(0..0, evil.clone());
        }
    });
    bogus(
        &run(&resolver, "news.old.example.com", T::A).0,
        BogusReason::AliasInvalid,
    );
}
#[test]
fn bogus_cname_after_dname_wrong_target() {
    let mut resolver = aliases(true);
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            for record in &mut reply.answer {
                if record.rtype == T::CNAME {
                    record.rdata = n("wrong.example.net").as_wire().to_vec();
                }
            }
        }
    });
    bogus(
        &run(&resolver, "news.old.example.com", T::A).0,
        BogusReason::AliasInvalid,
    );
}
#[test]
fn bogus_cname_loop() {
    let mut resolver = aliases(false);
    resolver
        .zone_mut(&n("example.net"))
        .unwrap()
        .records
        .retain(|r| r.rtype != T::A);
    resolver.zone_mut(&n("example.net")).unwrap().add_record(
        n("news.example.net"),
        T::CNAME,
        n("news.example.com").as_wire().to_vec(),
    );
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::AliasInvalid,
    );
}
#[test]
fn secure_cname_chain_nine_links() {
    let mut leaf = zone("example");
    for link in 0..9 {
        leaf.add_record(
            n(&format!("n{link}.example")),
            T::CNAME,
            n(&format!("n{}.example", link + 1)).as_wire().to_vec(),
        );
    }
    leaf.add_record(n("n9.example"), T::A, IP.to_vec());
    let resolver = FakeResolver::new(vec![zone("."), leaf]).unwrap();
    secure(&run(&resolver, "n0.example", T::A).0, records("n9.example"));
}
fn bad_key_flags(flags: u16) -> FakeResolver {
    let mut resolver = tree();
    let leaf = resolver.zone_mut(&n("example.com")).unwrap();
    let key = leaf.add_key(KeyAlgorithm::P256, flags).unwrap();
    leaf.record_signers = vec![key];
    resolver
}
#[test]
fn bogus_revoked_key_signs() {
    let resolver = bad_key_flags(0x0180);
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::NoMatchingKey,
    );
}
#[test]
fn bogus_non_zone_key_signs() {
    let resolver = bad_key_flags(0);
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::NoMatchingKey,
    );
}
#[test]
fn secure_with_an_unrelated_extra_signer_on_one_rrset() {
    let mut resolver = aliases(false);
    let extra = resolver.zones()[2]
        .sign_records(vec![Record::new(
            n("news.example.com"),
            T::CNAME,
            300,
            n("news.example.net").as_wire().to_vec(),
        )])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query.rtype() == T::A {
            reply
                .answer
                .extend(extra.iter().filter(|r| r.rtype == T::RRSIG).cloned());
        }
    });
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.net"),
    );
}
#[test]
fn additional_section_records_are_never_used() {
    let mut resolver = tree();
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.additional.append(&mut reply.answer);
        }
    });
    // The answer and authority sections hold no signature at all.
    let (verdict, _) = run(&resolver, "news.example.com", T::A);
    bogus(&verdict, BogusReason::NoDnssecRecords);
}

fn unanswered_case(target: Query, cause: UnansweredCause) {
    let mut resolver = tree();
    match cause {
        UnansweredCause::FetchFailed => resolver.set_reply(target.clone(), None),
        UnansweredCause::Truncated => replace_reply(
            &mut resolver,
            target.clone(),
            FakeReply {
                truncated: true,
                ..FakeReply::default()
            },
        ),
        UnansweredCause::Rcode(rcode) => replace_reply(
            &mut resolver,
            target.clone(),
            FakeReply {
                rcode,
                ..FakeReply::default()
            },
        ),
        other => panic!("no fixture for {other:?}"),
    }
    let v = validator(&resolver);
    let (verdict, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW);
    let Verdict::Unanswered { failure, .. } = &verdict else {
        panic!("expected unanswered, got {verdict:?}");
    };
    assert_eq!(*failure.query(), target);
    assert_eq!(failure.cause(), cause);
    assert!(failure.extended_errors().is_empty());
    let sequence = cold_queries();
    let failed_at = sequence.iter().position(|query| query == &target).unwrap();
    assert_eq!(queries, sequence[..=failed_at]);
    let valid = resolver.zones().to_vec();
    let good = FakeResolver::new(valid).unwrap();
    let (retry, asked) = drive(&v, &good, question("news.example.com", T::A), NOW);
    secure(&retry, records("news.example.com"));
    let mut expected = vec![q("news.example.com", T::A)];
    expected.extend_from_slice(&sequence[failed_at.max(1)..]);
    assert_eq!(asked, expected, "the failed step entered the cache");
}
#[test]
fn unanswered_main_fetch_failed() {
    unanswered_case(q("news.example.com", T::A), UnansweredCause::FetchFailed);
}
#[test]
fn unanswered_main_servfail() {
    unanswered_case(q("news.example.com", T::A), UnansweredCause::Rcode(2));
}
#[test]
fn unanswered_ds_refused() {
    unanswered_case(q("example.com", T::DS), UnansweredCause::Rcode(5));
}
#[test]
fn unanswered_ds_fetch_failed() {
    unanswered_case(q("example.com", T::DS), UnansweredCause::FetchFailed);
}
#[test]
fn unanswered_dnskey_servfail() {
    unanswered_case(q("example.com", T::DNSKEY), UnansweredCause::Rcode(2));
}
#[test]
fn unanswered_dnskey_truncated() {
    unanswered_case(q("example.com", T::DNSKEY), UnansweredCause::Truncated);
}

#[test]
fn secure_when_ds_query_at_a_plain_name_gets_servfail() {
    let mut leaf = zone("a.b");
    leaf.add_record(n("news.a.b"), T::A, IP.to_vec());
    let mut resolver = FakeResolver::new(vec![zone("."), leaf]).unwrap();
    replace_reply(
        &mut resolver,
        q("b", T::DS),
        FakeReply {
            rcode: 2,
            ..FakeReply::default()
        },
    );
    secure(&run(&resolver, "news.a.b", T::A).0, records("news.a.b"));
}
#[test]
fn bogus_when_ds_servfail_hides_a_signed_child() {
    let mut resolver = tree();
    replace_reply(
        &mut resolver,
        q("example.com", T::DS),
        FakeReply {
            rcode: 2,
            ..FakeReply::default()
        },
    );
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignerNotAZone,
    );
}
#[test]
fn bogus_when_ds_servfail_and_answer_unsigned() {
    let mut resolver = tree();
    replace_reply(
        &mut resolver,
        q("example.com", T::DS),
        FakeReply {
            rcode: 2,
            ..FakeReply::default()
        },
    );
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.strip_signatures();
        }
    });
    // The walk continues below the hidden cut. The child-side proof for the
    // owner's DS cannot authenticate under com's keys.
    let (verdict, queries) = run(&resolver, "news.example.com", T::A);
    bogus(&verdict, BogusReason::DelegationUnproven);
    assert_eq!(queries.last(), Some(&q("news.example.com", T::DS)));
}
#[test]
fn ds_servfail_is_not_cached() {
    let mut resolver = tree();
    replace_reply(
        &mut resolver,
        q("example.com", T::DS),
        FakeReply {
            rcode: 2,
            ..FakeReply::default()
        },
    );
    let v = validator(&resolver);
    for pass in 0..2 {
        let (verdict, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW);
        bogus(&verdict, BogusReason::SignerNotAZone);
        assert_eq!(
            queries,
            if pass == 0 {
                cold_queries()[..5].to_vec()
            } else {
                vec![q("news.example.com", T::A), q("example.com", T::DS)]
            }
        );
    }
}
#[test]
fn bogus_dnskey_step_is_not_cached() {
    let mut resolver = tree();
    let key_query = q("example.com", T::DNSKEY);
    let good = resolver.answer(&key_query).unwrap();
    replace_reply(&mut resolver, key_query.clone(), FakeReply::default());
    let v = validator(&resolver);
    bogus(
        &drive(&v, &resolver, question("news.example.com", T::A), NOW).0,
        BogusReason::NoDnssecRecords,
    );
    resolver.set_reply(key_query.clone(), Some(good));
    let (verdict, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW);
    secure(&verdict, records("news.example.com"));
    assert_eq!(queries, vec![q("news.example.com", T::A), key_query]);
}

#[test]
fn work_limit_queries() {
    let labels = (0..45).map(|_| "a").collect::<Vec<_>>().join(".");
    let mut root = zone(".");
    // A real root-signed CNAME response to each DS query is a compact,
    // independently authenticated NotACut proof at every label.
    let mut resolver = FakeResolver::new(vec![root.clone()]).unwrap();
    let attacker = zone(&labels);
    let main = q(&format!("news.{labels}"), T::A);
    replace_reply(
        &mut resolver,
        main.clone(),
        FakeReply {
            answer: attacker
                .sign_records(vec![Record::new(
                    main.name().clone(),
                    T::A,
                    300,
                    IP.to_vec(),
                )])
                .unwrap(),
            ..FakeReply::default()
        },
    );
    for count in 1..=45 {
        let owner = n(&labels).suffix(count).unwrap();
        root.add_record(owner.clone(), T::CNAME, n("target").as_wire().to_vec());
    }
    for count in 1..=45 {
        let owner = n(&labels).suffix(count).unwrap();
        replace_reply(
            &mut resolver,
            Query::new(owner.clone(), T::DS),
            FakeReply {
                answer: root.signed_rrset(&owner, T::CNAME).unwrap(),
                ..FakeReply::default()
            },
        );
    }
    let (verdict, queries) = run(&resolver, &format!("news.{labels}"), T::A);
    bogus(&verdict, BogusReason::WorkLimit);
    assert_eq!(queries.len(), 40);
    assert_eq!(&queries[..2], &[main, q(".", T::DNSKEY)]);
}
#[test]
fn work_limit_signatures() {
    let mut root = zone(".");
    for index in 0..10 {
        root.add_record(n(&format!("n{index}")), T::A, IP.to_vec());
    }
    let mut resolver = FakeResolver::new(vec![root]).unwrap();
    // Nineteen failed checks and one success fit the per-RRset budget. The tenth
    // denial RRset exhausts the shared 200-operation budget instead. The root
    // DNSKEY proof also consumes one DS digest operation.
    let signer = &resolver.zones()[0];
    let mut authority = Vec::new();
    for index in 0..10 {
        let set = signer
            .sign_records(vec![nsec(
                &format!("n{index}"),
                &format!("n{}", index + 1),
                &[T::A],
            )])
            .unwrap();
        let signature = set.iter().find(|r| r.rtype == T::RRSIG).unwrap();
        authority.push(set[0].clone());
        for bad in 0..19 {
            let mut sig = signature.clone();
            *sig.rdata.last_mut().unwrap() ^= bad + 1;
            authority.push(sig);
        }
        authority.push(signature.clone());
    }
    replace_reply(
        &mut resolver,
        q("missing", T::A),
        FakeReply {
            rcode: 3,
            authority,
            ..FakeReply::default()
        },
    );
    let v = validator(&resolver);
    let mut validation = v.begin(question("missing", T::A), NOW);
    validation.set_ask_ahead(false);
    let verdict = loop {
        match validation.step() {
            Step::Done { verdict, .. } => break verdict,
            Step::Need { awaited: query, .. } => validation
                .supply(&query, &resolver.answer(&query).unwrap())
                .unwrap(),
            step => panic!("unexpected step {step:?}"),
        }
    };
    bogus(&verdict, BogusReason::WorkLimit);
    assert_eq!(signature_checks(&validation), 199);
}
#[test]
fn step_repeats_need_until_supplied() {
    let resolver = tree();
    let v = validator(&resolver);
    let mut validation = v.begin(question("news.example.com", T::A), NOW);
    validation.set_ask_ahead(false);
    let first = validation.step();
    let Step::Need { awaited, ahead, .. } = &first else {
        panic!("expected a needed query, got {first:?}");
    };
    assert_eq!(*awaited, q("news.example.com", T::A));
    assert!(ahead.is_empty());
    for _ in 0..4 {
        assert_eq!(validation.step(), first);
        assert_eq!(signature_checks(&validation), 0);
    }
}
#[test]
fn step_repeats_done() {
    let resolver = tree();
    let v = validator(&resolver);
    let mut validation = v.begin(question("news.example.com", T::A), NOW);
    validation.set_ask_ahead(false);
    let final_step = loop {
        match validation.step() {
            Step::Need { awaited: query, .. } => validation
                .supply(&query, &resolver.answer(&query).unwrap())
                .unwrap(),
            done @ Step::Done { .. } => break done,
            step => panic!("unexpected step {step:?}"),
        }
    };
    let count = signature_checks(&validation);
    for _ in 0..4 {
        assert_eq!(validation.step(), final_step);
        assert_eq!(signature_checks(&validation), count);
    }
    assert_eq!(
        validation.supply_failure(&q("news.example.com", T::A)),
        Err(SupplyError::UnexpectedQuery)
    );
    assert_eq!(validation.step(), final_step);
}
#[test]
fn supply_for_wrong_query_is_an_error_and_changes_nothing() {
    let resolver = tree();
    let v = validator(&resolver);
    let mut validation = v.begin(question("news.example.com", T::A), NOW);
    validation.set_ask_ahead(false);
    let wrong = q("other.example.com", T::A);
    let bytes = resolver.answer(&wrong).unwrap();
    assert_eq!(
        validation.supply(&wrong, &bytes),
        Err(SupplyError::UnexpectedQuery)
    );
    let expected = validation.step();
    assert_eq!(
        validation.supply(&wrong, &bytes),
        Err(SupplyError::UnexpectedQuery)
    );
    assert_eq!(
        validation.supply_failure(&wrong),
        Err(SupplyError::UnexpectedQuery)
    );
    assert_eq!(validation.step(), expected);
    assert_eq!(signature_checks(&validation), 0);
    let verdict = loop {
        match validation.step() {
            Step::Need { awaited: query, .. } => validation
                .supply(&query, &resolver.answer(&query).unwrap())
                .unwrap(),
            Step::Done { verdict, .. } => break verdict,
            step => panic!("unexpected step {step:?}"),
        }
    };
    secure(&verdict, records("news.example.com"));
}
#[test]
fn no_crypto_repeated_across_steps() {
    let resolver = tree();
    let v = validator(&resolver);
    let mut validation = v.begin(question("news.example.com", T::A), NOW);
    validation.set_ask_ahead(false);
    let mut queries = Vec::new();
    loop {
        let step = validation.step();
        let before = signature_checks(&validation);
        assert_eq!(validation.step(), step);
        assert_eq!(signature_checks(&validation), before);
        match step {
            Step::Need { awaited: query, .. } => {
                queries.push(query.clone());
                validation
                    .supply(&query, &resolver.answer(&query).unwrap())
                    .unwrap();
            }
            Step::Done { verdict, .. } => {
                secure(&verdict, records("news.example.com"));
                break;
            }
            step => panic!("unexpected step {step:?}"),
        }
    }
    assert_eq!(queries, cold_queries());
    // root DNSKEY, com DS/DNSKEY, example.com DS/DNSKEY, final A.
    assert_eq!(signature_checks(&validation), 6);
}
#[test]
fn validator_is_send_sync() {
    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>() {}
    send_sync::<Validator>();
    send::<weaver_dnssec::Validation<'_>>();
}
#[test]
fn byte_flip_sweep() {
    let mut resolver = tree();
    let (true_verdict, queries) = run(&resolver, "news.example.com", T::A);
    secure(&true_verdict, records("news.example.com"));
    assert_eq!(queries, cold_queries());
    let replies: Vec<_> = queries
        .iter()
        .map(|query| resolver.answer(query).unwrap())
        .collect();
    let mut flips = 0;
    for (query, original) in queries.iter().zip(&replies) {
        for index in 0..original.len() {
            let mut bytes = original.clone();
            bytes[index] ^= 1;
            resolver.set_reply(query.clone(), Some(bytes));
            let (verdict, _) = run(&resolver, "news.example.com", T::A);
            if let Verdict::Secure { answer, .. } | Verdict::Insecure { answer, .. } = verdict {
                assert_eq!(
                    *answer.outcome(),
                    records("news.example.com"),
                    "query {query:?}, byte {index}"
                );
            }
            flips += 1;
            resolver.set_reply(query.clone(), Some(original.clone()));
        }
    }
    assert_eq!(flips, replies.iter().map(Vec::len).sum::<usize>());
}

#[test]
fn ancestor_dname_cannot_bypass_a_more_specific_anchor() {
    for synthesized_cname in [false, true] {
        let mut parent = zone("example.com");
        parent.add_record(
            n("old.example.com"),
            T::DNAME,
            n("example.net").as_wire().to_vec(),
        );
        let mut target = zone("example.net");
        target.add_record(n("branch.example.net"), T::A, IP.to_vec());
        target.add_record(n("news.branch.example.net"), T::A, IP.to_vec());
        let mut resolver = FakeResolver::new(vec![zone("."), parent, target]).unwrap();
        if !synthesized_cname {
            resolver.tamper(|query, reply| {
                if query.rtype() == T::A {
                    reply.answer.retain(|record| record.rtype != T::CNAME);
                }
            });
        }
        let island = zone("branch.old.example.com");
        let mut anchors = resolver.zones()[0].anchors().unwrap();
        anchors.extend(island.anchors().unwrap());
        let validator = Validator::new(anchors, Policy::default()).unwrap();
        for owner in ["branch.old.example.com", "news.branch.old.example.com"] {
            let (verdict, asked) = drive(&validator, &resolver, question(owner, T::A), NOW);
            bogus(&verdict, BogusReason::SignerMismatch);
            assert_eq!(asked, [q(owner, T::A)]);
        }
    }
}

#[test]
fn wildcard_expanded_ds_cannot_prove_a_delegation() {
    for unsupported in [false, true] {
        for expanded in [false, true] {
            let mut resolver = tree();
            let mut ds = resolver.zones()[2].ds_records(2).unwrap();
            if unsupported {
                for record in &mut ds {
                    record[2] = 253;
                }
            }
            let owner = n(if expanded { "*.com" } else { "example.com" });
            let mut signed = resolver.zones()[1]
                .sign_records(
                    ds.into_iter()
                        .map(|data| Record::new(owner.clone(), T::DS, 300, data))
                        .collect(),
                )
                .unwrap();
            for record in &mut signed {
                record.owner = n("example.com");
            }
            resolver.tamper(move |query, reply| {
                if query == &q("example.com", T::DS) {
                    reply.answer = signed.clone();
                    reply.authority.clear();
                }
            });
            let (verdict, asked) = run(&resolver, "news.example.com", T::A);
            if expanded {
                bogus(&verdict, BogusReason::DelegationUnproven);
                assert_eq!(asked, cold_queries()[..5]);
            } else if unsupported {
                insecure(&verdict, records("news.example.com"));
                assert_eq!(asked, cold_queries()[..5]);
            } else {
                secure(&verdict, records("news.example.com"));
                assert_eq!(asked, cold_queries());
            }
        }
    }
}
