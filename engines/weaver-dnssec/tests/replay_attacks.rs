// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Validly signed records replayed out of place must never make a signed
//! name Insecure. Each attack here once did; each now ends Bogus.

use weaver_dnssec::testing::{self, DenialMode, FakeResolver, KeyAlgorithm, Record, Zone, drive};
use weaver_dnssec::{
    BogusReason, Name, Policy, Query, Question, RecordType as T, Validator, Verdict,
};

const NOW: u64 = 1_100_000;
const IP: [u8; 4] = [203, 0, 113, 7];
const EVIL: [u8; 4] = [6, 6, 6, 6];

fn n(text: &str) -> Name {
    Name::from_text(text).unwrap()
}
fn q(text: &str, rtype: T) -> Query {
    Query::new(n(text), rtype)
}
fn zone(text: &str) -> Zone {
    Zone::new(n(text), KeyAlgorithm::P256).unwrap()
}
fn nsec3(iterations: u16) -> DenialMode {
    DenialMode::Nsec3 {
        salt: vec![0xaa, 0xbb],
        iterations,
        opt_out: false,
    }
}
fn validator(resolver: &FakeResolver) -> Validator {
    Validator::new(resolver.zones()[0].anchors().unwrap(), Policy::default()).unwrap()
}
fn ask_at(v: &Validator, r: &FakeResolver, name: &str, now: u64) -> (Verdict, Vec<Query>) {
    drive(v, r, Question::new(n(name), T::A).unwrap(), now)
}
fn ask(v: &Validator, r: &FakeResolver, name: &str) -> Verdict {
    ask_at(v, r, name, NOW).0
}
fn bogus(verdict: &Verdict, reason: BogusReason, name: &str, rtype: T) {
    let Verdict::Bogus { failure, .. } = verdict else {
        panic!("expected bogus {reason:?}, got {verdict:?}")
    };
    assert_eq!(failure.reason(), reason);
    assert_eq!(failure.name(), &n(name));
    assert_eq!(failure.rtype(), rtype);
}
fn secure(verdict: &Verdict, name: &str) {
    let Verdict::Secure { answer, .. } = verdict else {
        panic!("expected secure {name}, got {verdict:?}")
    };
    assert_eq!(
        *answer.outcome(),
        testing::records(n(name), vec![IP.to_vec()])
    );
}

/// One NSEC3 record and its RRSIG from the zone's chain rebuilt with
/// `iterations`, signed by the zone's current keys: what a replay of an older
/// chain looks like while its signatures are still in their window.
fn old_high_iteration_nsec3(zone: &Zone, iterations: u16) -> Vec<Record> {
    let mut old = zone.clone();
    old.denial = nsec3(iterations);
    let records = old.denial_records().unwrap();
    assert_eq!(records[0].rtype, T::NSEC3);
    assert_eq!(records[1].rtype, T::RRSIG);
    records[..2].to_vec()
}

fn tree() -> FakeResolver {
    let mut com = zone("com");
    com.denial = nsec3(0);
    let mut leaf = zone("example.com");
    leaf.denial = nsec3(0);
    leaf.add_record(n("news.example.com"), T::A, IP.to_vec());
    leaf.add_record(n("other.example.com"), T::A, IP.to_vec());
    FakeResolver::new(vec![zone("."), com, leaf]).unwrap()
}

/// The DS reply for a signed child replaced by one unrelated com NSEC3 with
/// 151 iterations, and a forged unsigned A record beside it.
#[test]
fn unrelated_high_iteration_nsec3_cannot_deny_a_ds() {
    let mut resolver = tree();
    let v = validator(&resolver);
    secure(&ask(&v, &resolver, "news.example.com"), "news.example.com");

    let replay = old_high_iteration_nsec3(&resolver.zones()[1], 151);
    resolver.tamper(move |query, reply| {
        if *query == q("example.com", T::DS) {
            reply.answer.clear();
            reply.authority = replay.clone();
        }
        if *query == q("news.example.com", T::A) {
            reply.answer = vec![Record::new(n("news.example.com"), T::A, 300, EVIL.to_vec())];
            reply.authority.clear();
        }
    });
    bogus(
        &ask(&validator(&resolver), &resolver, "news.example.com"),
        BogusReason::Nsec3Iterations,
        "example.com",
        T::DS,
    );
}

/// The attacked lookup leaves no insecure cut behind: an honest lookup
/// through the same validator afterwards is Secure.
#[test]
fn a_failed_high_iteration_ds_denial_does_not_poison_later_lookups() {
    let mut attacked = tree();
    let honest = FakeResolver::new(attacked.zones().to_vec()).unwrap();
    let replay = old_high_iteration_nsec3(&attacked.zones()[1], 151);
    attacked.tamper(move |query, reply| {
        if *query == q("example.com", T::DS) {
            reply.answer.clear();
            reply.authority = replay.clone();
        }
    });
    let v = validator(&attacked);
    bogus(
        &ask(&v, &attacked, "news.example.com"),
        BogusReason::Nsec3Iterations,
        "example.com",
        T::DS,
    );
    secure(&ask(&v, &honest, "other.example.com"), "other.example.com");
}

/// A genuine wildcard expansion replayed under a name that exists with other
/// data, with one unrelated high-iteration NSEC3 in place of the proof.
#[test]
fn unrelated_high_iteration_nsec3_cannot_carry_a_substituted_wildcard() {
    let mut leaf = zone("example.com");
    leaf.denial = nsec3(0);
    leaf.add_record(n("*.example.com"), T::A, EVIL.to_vec());
    leaf.add_record(n("bank.example.com"), T::A, IP.to_vec());
    let mut resolver = FakeResolver::new(vec![zone("."), leaf]).unwrap();
    let v = validator(&resolver);
    secure(&ask(&v, &resolver, "bank.example.com"), "bank.example.com");

    let mut expanded = resolver
        .reply(&q("nothere.example.com", T::A))
        .unwrap()
        .answer;
    for record in &mut expanded {
        record.owner = n("bank.example.com");
    }
    let replay = old_high_iteration_nsec3(&resolver.zones()[1], 151);
    resolver.tamper(move |query, reply| {
        if *query == q("bank.example.com", T::A) {
            reply.answer = expanded.clone();
            reply.authority = replay.clone();
        }
    });
    bogus(
        &ask(&validator(&resolver), &resolver, "bank.example.com"),
        BogusReason::Nsec3Iterations,
        "bank.example.com",
        T::A,
    );
}

/// Control: the same substitution with the zone's own low-iteration chain
/// stays Bogus, because no genuine record covers an existing name's hash.
#[test]
fn control_wildcard_substitution_with_low_iteration_chain_is_bogus() {
    let mut leaf = zone("example.com");
    leaf.denial = nsec3(0);
    leaf.add_record(n("*.example.com"), T::A, EVIL.to_vec());
    leaf.add_record(n("bank.example.com"), T::A, IP.to_vec());
    let mut resolver = FakeResolver::new(vec![zone("."), leaf]).unwrap();
    let mut expanded = resolver
        .reply(&q("nothere.example.com", T::A))
        .unwrap()
        .answer;
    for record in &mut expanded {
        record.owner = n("bank.example.com");
    }
    let chain = resolver.zones()[1].denial_records().unwrap();
    resolver.tamper(move |query, reply| {
        if *query == q("bank.example.com", T::A) {
            reply.answer = expanded.clone();
            reply.authority = chain.clone();
        }
    });
    let verdict = ask(&validator(&resolver), &resolver, "bank.example.com");
    assert!(matches!(verdict, Verdict::Bogus { .. }), "{verdict:?}");
}

/// The same substitution with the zone's whole chain rebuilt with opt-out:
/// an opt-out span never covers a name that exists, so it stays Bogus.
#[test]
fn an_opt_out_chain_cannot_carry_a_substituted_wildcard() {
    let mut leaf = zone("example.com");
    leaf.denial = nsec3(0);
    leaf.add_record(n("*.example.com"), T::A, EVIL.to_vec());
    leaf.add_record(n("bank.example.com"), T::A, IP.to_vec());
    let mut resolver = FakeResolver::new(vec![zone("."), leaf]).unwrap();
    let mut expanded = resolver
        .reply(&q("nothere.example.com", T::A))
        .unwrap()
        .answer;
    for record in &mut expanded {
        record.owner = n("bank.example.com");
    }
    let mut old = resolver.zones()[1].clone();
    old.denial = DenialMode::Nsec3 {
        salt: vec![0xaa, 0xbb],
        iterations: 0,
        opt_out: true,
    };
    let chain = old.denial_records().unwrap();
    resolver.tamper(move |query, reply| {
        if *query == q("bank.example.com", T::A) {
            reply.answer = expanded.clone();
            reply.authority = chain.clone();
        }
    });
    let verdict = ask(&validator(&resolver), &resolver, "bank.example.com");
    assert!(matches!(verdict, Verdict::Bogus { .. }), "{verdict:?}");
}

/// An existing name answered NXDOMAIN, and NODATA, with one unrelated
/// high-iteration NSEC3.
#[test]
fn unrelated_high_iteration_nsec3_cannot_deny_an_existing_name() {
    for rcode in [3, 0] {
        let mut resolver = tree();
        let replay = old_high_iteration_nsec3(&resolver.zones()[2], 151);
        resolver.tamper(move |query, reply| {
            if *query == q("news.example.com", T::A) {
                reply.answer.clear();
                reply.rcode = rcode;
                reply.authority = replay.clone();
            }
        });
        bogus(
            &ask(&validator(&resolver), &resolver, "news.example.com"),
            BogusReason::Nsec3Iterations,
            "news.example.com",
            T::A,
        );
    }
}

/// A zone with a DS for a usable P-256 key and one for an unusable RSA-512
/// key. The DNSKEY reply is cut down to the RSA-512 key alone.
#[test]
fn a_dnskey_reply_cut_to_an_unusable_key_is_bogus() {
    let mut child = zone("example");
    let rsa = child.add_key(KeyAlgorithm::Rsa2048, 257).unwrap();
    child.keys[rsa].public_key = [vec![3, 1, 0, 1], vec![0xff; 64]].concat();
    child.dnskey_signers = vec![0, rsa];
    child.add_record(n("news.example"), T::A, IP.to_vec());
    let mut resolver = FakeResolver::new(vec![zone("."), child]).unwrap();
    let v = validator(&resolver);
    secure(&ask(&v, &resolver, "news.example"), "news.example");
    let weak = resolver.zones()[1].keys[rsa].dnskey_rdata();
    resolver.tamper(move |query, reply| {
        if *query == q("example", T::DNSKEY) {
            // The signatures stay, so the reply is not a stripped one.
            reply
                .answer
                .retain(|record| record.rtype == T::RRSIG || record.rdata == weak);
            assert_eq!(
                reply
                    .answer
                    .iter()
                    .filter(|record| record.rtype == T::DNSKEY)
                    .count(),
                1
            );
        }
        if *query == q("news.example", T::A) {
            reply.answer = vec![Record::new(n("news.example"), T::A, 300, EVIL.to_vec())];
        }
    });
    bogus(
        &ask(&validator(&resolver), &resolver, "news.example"),
        BogusReason::KeysNotProven,
        "example",
        T::DNSKEY,
    );
}

/// Control: a high-iteration record beside the zone's usable chain proves
/// nothing more, and the DS denial still fails.
#[test]
fn control_mixed_chain_is_bogus() {
    let mut resolver = tree();
    let replay = old_high_iteration_nsec3(&resolver.zones()[1], 151);
    resolver.tamper(move |query, reply| {
        if *query == q("example.com", T::DS) {
            reply.answer.clear();
            let mut authority = replay.clone();
            authority.extend(reply.authority.clone());
            reply.authority = authority;
        }
    });
    let verdict = ask(&validator(&resolver), &resolver, "news.example.com");
    assert!(matches!(verdict, Verdict::Bogus { .. }), "{verdict:?}");
}

/// Trust facts proven at one time are not reused when the caller's clock
/// steps back before that time: the chain is proven again.
#[test]
fn a_clock_stepped_back_does_not_reuse_cached_trust() {
    let resolver = tree();
    let v = validator(&resolver);
    let (verdict, cold) = ask_at(&v, &resolver, "news.example.com", NOW);
    secure(&verdict, "news.example.com");
    let (verdict, warm) = ask_at(&v, &resolver, "news.example.com", NOW + 1);
    secure(&verdict, "news.example.com");
    assert_eq!(warm, [q("news.example.com", T::A)]);
    let (verdict, stepped) = ask_at(&v, &resolver, "news.example.com", NOW - 3_600);
    secure(&verdict, "news.example.com");
    assert_eq!(stepped, cold);
}
