// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_dnssec::testing::{
    self, DenialMode, FakeReply, FakeResolver, KeyAlgorithm, Record, Zone, base32hex, drive,
    nsec3_hash,
};
use weaver_dnssec::{
    BogusReason, Name, Outcome, Policy, Query, Question, RecordType as T, Validator, Verdict,
};

const NOW: u64 = 1_100_000;

fn n(text: &str) -> Name {
    Name::from_text(text).unwrap()
}

fn query(text: &str, rtype: T) -> Query {
    Query::new(n(text), rtype)
}

fn tree() -> FakeResolver {
    let root = Zone::new(n("."), KeyAlgorithm::P256).unwrap();
    let mut leaf = Zone::new(n("example."), KeyAlgorithm::P256).unwrap();
    for owner in ["news.example.", "other.example.", "*.wild.example."] {
        leaf.add_record(n(owner), T::A, vec![203, 0, 113, 7]);
    }
    FakeResolver::new(vec![root, leaf]).unwrap()
}

fn validator(resolver: &FakeResolver) -> Validator {
    Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap()
}

fn run(validator: &Validator, resolver: &FakeResolver, q: &Query) -> (Verdict, Vec<Query>) {
    drive(
        validator,
        resolver,
        Question::new(q.name().clone(), q.rtype()).unwrap(),
        NOW,
    )
}

fn chain(resolver: &mut FakeResolver, salt: &[u8], iterations: u16) -> Vec<Record> {
    let zone = resolver.zone_mut(&n("example.")).unwrap();
    zone.denial = DenialMode::Nsec3 {
        salt: salt.to_vec(),
        iterations,
        opt_out: false,
    };
    zone.denial_records()
        .unwrap()
        .into_iter()
        .filter(|record| record.rtype == T::NSEC3)
        .collect()
}

fn sign(resolver: &mut FakeResolver, records: Vec<Record>) -> Vec<Record> {
    let zone = resolver.zone_mut(&n("example.")).unwrap();
    records
        .into_iter()
        .flat_map(|record| zone.sign_records(vec![record]).unwrap())
        .collect()
}

fn install(resolver: &mut FakeResolver, q: &Query, records: Vec<Record>) -> FakeReply {
    let mut reply = resolver.reply(q).unwrap();
    reply.authority.retain(|record| {
        !matches!(record.rtype, T::NSEC | T::NSEC3)
            && !(record.rtype == T::RRSIG
                && matches!(record.rdata.get(..2), Some([0, 47] | [0, 50])))
    });
    reply.authority.extend(sign(resolver, records));
    resolver.set_reply(q.clone(), Some(reply.encode(q).unwrap()));
    reply
}

fn hash_owner(owner: &str, salt: &[u8], iterations: u16) -> Name {
    n(&format!(
        "{}.example.",
        base32hex(&nsec3_hash(&n(owner), salt, iterations))
    ))
}

fn secure(verdict: Verdict, expected: Outcome, ttl: u32) {
    let Verdict::Secure { answer, .. } = verdict else {
        panic!("expected secure {expected:?}, got {verdict:?}");
    };
    assert_eq!(answer.outcome(), &expected);
    assert_eq!(answer.ttl(), ttl);
}

fn bogus(verdict: Verdict, expected: BogusReason) {
    let Verdict::Bogus { failure, .. } = verdict else {
        panic!("expected bogus {expected:?}, got {verdict:?}");
    };
    assert_eq!(failure.reason(), expected);
}

#[test]
fn rollover_salts_and_iterations_work_in_either_authority_order() {
    for different_iterations in [false, true] {
        for reverse in [false, true] {
            let mut resolver = tree();
            let mut first = chain(&mut resolver, &[1], 0);
            let (salt, iterations) = if different_iterations {
                (vec![1], 1)
            } else {
                (vec![2], 0)
            };
            let second = chain(&mut resolver, &salt, iterations);
            first.extend(second);
            if reverse {
                first.reverse();
            }
            let v = validator(&resolver);
            for (q, expected) in [
                (query("news.example.", T::AAAA), Outcome::NoData),
                (query("missing.example.", T::A), Outcome::NoSuchName),
                (
                    query("a.wild.example.", T::A),
                    testing::records(n("a.wild.example."), vec![vec![203, 0, 113, 7]]),
                ),
            ] {
                install(&mut resolver, &q, first.clone());
                secure(run(&v, &resolver, &q).0, expected, 300);
            }
        }
    }
}

#[test]
fn incomplete_first_chain_retries_a_complete_chain_without_lowering_its_ttl() {
    for different_iterations in [false, true] {
        for reverse in [false, true] {
            let mut resolver = tree();
            let mut incomplete = chain(&mut resolver, &[1], 0);
            incomplete.retain(|record| record.owner == hash_owner("news.example.", &[1], 0));
            assert_eq!(incomplete.len(), 1);
            incomplete[0].ttl = 1;
            let (salt, iterations) = if different_iterations {
                (vec![1], 1)
            } else {
                (vec![2], 0)
            };
            let mut complete = chain(&mut resolver, &salt, iterations);
            for record in &mut complete {
                record.ttl = 75;
            }
            incomplete.extend(complete);
            if reverse {
                incomplete.reverse();
            }
            let q = query("missing.example.", T::A);
            install(&mut resolver, &q, incomplete);
            secure(
                run(&validator(&resolver), &resolver, &q).0,
                Outcome::NoSuchName,
                75,
            );
        }
    }
}

#[test]
fn mixed_parameter_fragments_do_not_form_a_complete_denial() {
    for different_iterations in [false, true] {
        for reverse in [false, true] {
            let mut resolver = tree();
            let mut records = chain(&mut resolver, &[1], 0);
            let apex = hash_owner("example.", &[1], 0);
            let fragment = records
                .iter_mut()
                .find(|record| record.owner == apex)
                .unwrap();
            // Keep the original hash interval but move the apex fragment to a
            // distinct authenticated chain. Joining these fragments is unsafe.
            if different_iterations {
                fragment.rdata[3] = 1;
            } else {
                fragment.rdata[5] = 2;
            }
            if reverse {
                records.reverse();
            }
            let q = query("missing.example.", T::A);
            install(&mut resolver, &q, records);
            bogus(
                run(&validator(&resolver), &resolver, &q).0,
                BogusReason::DenialInvalid,
            );
        }
    }
}

#[test]
fn unrelated_candidates_do_not_rescue_a_denial_missing_its_wildcard_proof() {
    let mut resolver = tree();
    // Authenticated NSEC intervals deny the requested name but leave the
    // possible wildcard at the apex unproved.
    let mut data = n("z.example.").as_wire().to_vec();
    data.extend([0, 6, 0x40, 0, 0, 0, 0, 3]);
    let only_name = Record::new(n("a.example."), T::NSEC, 300, data);
    let mut incomplete = chain(&mut resolver, &[1], 0);
    incomplete.retain(|record| record.owner == hash_owner("news.example.", &[1], 0));
    incomplete.push(only_name);
    let q = query("missing.example.", T::A);
    install(&mut resolver, &q, incomplete);
    bogus(
        run(&validator(&resolver), &resolver, &q).0,
        BogusReason::DenialInvalid,
    );
}

#[test]
fn nsec3_qname_cover_without_closest_encloser_and_wildcard_proofs_is_bogus() {
    let mut resolver = tree();
    let salt = [0xaa, 0xbb];
    let complete = chain(&mut resolver, &salt, 0);
    let q = query("missing.example.", T::A);
    let hash = nsec3_hash(q.name(), &salt, 0);
    let mut lower = hash.clone();
    let mut upper = hash.clone();
    assert!((1..u8::MAX).contains(hash.last().unwrap()));
    *lower.last_mut().unwrap() -= 1;
    *upper.last_mut().unwrap() += 1;
    assert!(lower < hash && hash < upper);
    // This interval cannot accidentally match the closest encloser, prove its
    // existence, or cover the wildcard whose nonexistence is also required.
    for owner in ["example.", "*.example."] {
        let other = nsec3_hash(&n(owner), &salt, 0);
        assert!(other < lower || other > upper);
    }
    let mut data = vec![1, 0, 0, 0, 2, salt[0], salt[1], 20];
    data.extend(upper);
    data.extend([0, 1, 0x40]);
    let only_name = Record::new(
        n(&format!("{}.example.", base32hex(&lower))),
        T::NSEC3,
        300,
        data,
    );
    let reply = install(&mut resolver, &q, vec![only_name]);
    // answer() supplies the frozen override; reply() rebuilds the zone's
    // original complete proof before overrides are applied.
    assert_eq!(resolver.answer(&q), Some(reply.encode(&q).unwrap()));
    assert_eq!(
        reply
            .authority
            .iter()
            .filter(|rr| rr.rtype == T::NSEC3)
            .count(),
        1
    );
    assert!(reply.authority.iter().all(|rr| rr.rtype != T::NSEC));
    bogus(
        run(&validator(&resolver), &resolver, &q).0,
        BogusReason::DenialInvalid,
    );
    install(&mut resolver, &q, complete);
    secure(
        run(&validator(&resolver), &resolver, &q).0,
        Outcome::NoSuchName,
        300,
    );
}

#[test]
fn iteration_150_is_usable_but_151_is_bogus() {
    for iterations in [150, 151] {
        let mut resolver = tree();
        let records = chain(&mut resolver, &[1], iterations);
        for (q, outcome) in [
            (query("news.example.", T::AAAA), Outcome::NoData),
            (query("missing.example.", T::A), Outcome::NoSuchName),
            (
                query("a.wild.example.", T::A),
                testing::records(n("a.wild.example."), vec![vec![203, 0, 113, 7]]),
            ),
        ] {
            install(&mut resolver, &q, records.clone());
            let verdict = run(&validator(&resolver), &resolver, &q).0;
            if iterations == 150 {
                secure(verdict, outcome, 300);
            } else {
                bogus(verdict, BogusReason::Nsec3Iterations);
            }
        }
    }
}

#[test]
fn high_iteration_candidate_cannot_rescue_bad_proof_and_does_not_poison_good_proof() {
    for reverse in [false, true] {
        let mut resolver = tree();
        let high = chain(&mut resolver, &[2], 151);
        let mut accepted = chain(&mut resolver, &[1], 150);
        for record in &mut accepted {
            record.ttl = 75;
        }
        let mut complete = accepted.clone();
        complete.extend(high.clone());
        if reverse {
            complete.reverse();
        }
        let q = query("missing.example.", T::A);
        install(&mut resolver, &q, complete);
        secure(
            run(&validator(&resolver), &resolver, &q).0,
            Outcome::NoSuchName,
            75,
        );
        accepted.retain(|record| record.owner == hash_owner("news.example.", &[1], 150));
        accepted.extend(high);
        if reverse {
            accepted.reverse();
        }
        install(&mut resolver, &q, accepted);
        bogus(
            run(&validator(&resolver), &resolver, &q).0,
            BogusReason::Nsec3Iterations,
        );
    }
}

#[test]
fn unsupported_hash_groups_are_ignored_without_becoming_proofs() {
    let mut resolver = tree();
    let accepted = chain(&mut resolver, &[1], 0);
    let mut unsupported = chain(&mut resolver, &[2], 0);
    for record in &mut unsupported {
        record.rdata[0] = 2;
    }
    let q = query("missing.example.", T::A);
    install(&mut resolver, &q, unsupported.clone());
    bogus(
        run(&validator(&resolver), &resolver, &q).0,
        BogusReason::DenialInvalid,
    );
    unsupported.extend(accepted);
    install(&mut resolver, &q, unsupported);
    secure(
        run(&validator(&resolver), &resolver, &q).0,
        Outcome::NoSuchName,
        300,
    );
}

#[test]
fn high_iteration_ds_denial_is_bogus_and_leaves_no_insecure_cut() {
    let mut resolver = tree();
    let ds_query = query("example.", T::DS);
    let good_ds = resolver.answer(&ds_query).unwrap();
    let parent = resolver.zone_mut(&n(".")).unwrap();
    let original_ds = parent.delegations[&n("example.")].clone();
    parent.delegate(n("example."), Vec::new());
    parent.denial = DenialMode::Nsec3 {
        salt: vec![1],
        iterations: 151,
        opt_out: false,
    };
    let authority = parent.denial_records().unwrap();
    parent.delegate(n("example."), original_ds);
    let bad = FakeReply {
        authority,
        ..FakeReply::default()
    };
    resolver.set_reply(ds_query.clone(), Some(bad.encode(&ds_query).unwrap()));
    let v = validator(&resolver);
    let answer = |name: &str| testing::records(n(name), vec![vec![203, 0, 113, 7]]);
    bogus(
        run(&v, &resolver, &query("news.example.", T::A)).0,
        BogusReason::Nsec3Iterations,
    );
    // No cut is remembered: the honest DS reply that follows is used.
    resolver.set_reply(ds_query.clone(), Some(good_ds));
    let (verdict, queries) = run(&v, &resolver, &query("other.example.", T::A));
    secure(verdict, answer("other.example."), 300);
    assert!(queries.contains(&ds_query));
    let (verdict, queries) = run(
        &validator(&resolver),
        &resolver,
        &query("other.example.", T::A),
    );
    secure(verdict, answer("other.example."), 300);
    assert!(queries.contains(&ds_query));
    assert!(queries.contains(&query("example.", T::DNSKEY)));
}
