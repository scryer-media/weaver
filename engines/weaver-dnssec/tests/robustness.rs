// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Deterministic robustness sweeps in place of fuzzing. Every reply of each
//! valid exchange is truncated at every length and mutated at seeded single
//! and multiple bytes, then run through the parser and a full validation.
//! Nothing may panic, and a mutated exchange may stay Secure only with the
//! same answer and byte-identical signed records. A second sweep replays
//! validly signed RRsets from one reply of an exchange into another, beside
//! or in place of a section: such a splice is never Insecure, and stays
//! Secure only with the same answer.

use std::collections::BTreeMap;

use weaver_dnssec::testing::{
    DenialMode, FakeResolver, KeyAlgorithm, Record, Reply, Zone, drive, signed_records,
};
use weaver_dnssec::{Name, Policy, Query, Question, RecordType as T, Step, Validator, Verdict};

const NOW: u64 = 1_100_000;
const IP: [u8; 4] = [203, 0, 113, 7];
// Fixed seed: every run makes exactly the same mutations.
const SEED: u64 = 0x5eed_d15c_0ded_2026;
const SINGLE_MUTATIONS: usize = 48;
const MULTI_MUTATIONS: usize = 48;

/// xorshift64*: small, fixed and good enough to spread mutations.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
    fn byte(&mut self) -> u8 {
        // Never zero, so every chosen byte changes.
        (self.next() % 255) as u8 + 1
    }
}

struct Exchange {
    label: String,
    validator: fn() -> Validator,
    question: Query,
    now: u64,
    replies: Vec<(Query, Vec<u8>)>,
}

fn n(text: &str) -> Name {
    Name::from_text(text).unwrap()
}
fn q(text: &str, rtype: T) -> Query {
    Query {
        name: n(text),
        rtype,
    }
}
fn zone(text: &str) -> Zone {
    Zone::new(n(text), KeyAlgorithm::P256).unwrap()
}

fn validate(exchange: &Exchange, replies: &BTreeMap<Query, Vec<u8>>) -> Verdict {
    let validator = (exchange.validator)();
    let question = Question::new(exchange.question.name.clone(), exchange.question.rtype).unwrap();
    let mut validation = validator.begin(question, exchange.now);
    loop {
        match validation.step() {
            Step::Done(verdict) => return verdict,
            Step::Need(query) => match replies.get(&query) {
                Some(bytes) => validation.supply(&query, bytes).unwrap(),
                None => validation.failed(&query).unwrap(),
            },
        }
    }
}

fn sweep(exchange: &Exchange) -> usize {
    let originals: BTreeMap<_, _> = exchange.replies.iter().cloned().collect();
    let baseline = validate(exchange, &originals);
    let Verdict::Secure(expected) = &baseline else {
        panic!("{}: baseline {baseline:?}", exchange.label);
    };
    let mut rng = Rng(SEED ^ exchange.label.len() as u64);
    let mut runs = 0;
    for (query, original) in &exchange.replies {
        let signed = signed_records(original, query).unwrap();
        let mut mutants = Vec::new();
        for length in 0..original.len() {
            mutants.push(original[..length].to_vec());
        }
        for _ in 0..SINGLE_MUTATIONS {
            let mut bytes = original.clone();
            let at = rng.below(bytes.len());
            bytes[at] ^= rng.byte();
            mutants.push(bytes);
        }
        for _ in 0..MULTI_MUTATIONS {
            let mut bytes = original.clone();
            for _ in 0..2 + rng.below(7) {
                let at = rng.below(bytes.len());
                bytes[at] ^= rng.byte();
            }
            mutants.push(bytes);
        }
        for bytes in mutants {
            let mut replies = originals.clone();
            replies.insert(query.clone(), bytes.clone());
            let verdict = validate(exchange, &replies);
            runs += 1;
            match &verdict {
                Verdict::Secure(answer) => {
                    assert_eq!(answer.outcome, expected.outcome, "{}", exchange.label);
                    assert!(answer.ttl <= expected.ttl, "{}", exchange.label);
                    // Every signed RRset left in the mutant is byte-identical
                    // to one in the original. Unsigned strays are not data the
                    // verdict uses, and a dropped set was not needed.
                    let changed = signed_records(&bytes, query).unwrap_or_default();
                    assert!(
                        changed.iter().all(|set| signed.contains(set)),
                        "{}: {query:?} stayed secure with changed signed records\n{answer:?}\nwas {signed:?}\nnow {changed:?}",
                        exchange.label,
                    );
                }
                // A signed chain must never be downgraded by changed bytes.
                Verdict::Insecure(_) => {
                    panic!("{}: {query:?} downgraded to {verdict:?}", exchange.label)
                }
                Verdict::Bogus(_) | Verdict::Unanswered(_) => {}
            }
        }
    }
    runs
}

fn generated(label: &str, resolver: &FakeResolver, question: Query) -> Exchange {
    fn anchored() -> Validator {
        ANCHORS.with(|anchors| Validator::new(anchors.borrow().clone(), Policy::default()).unwrap())
    }
    ANCHORS.with(|anchors| *anchors.borrow_mut() = resolver.zones()[0].anchors().unwrap());
    let (_, queries) = drive(
        &anchored(),
        resolver,
        Question::new(question.name.clone(), question.rtype).unwrap(),
        NOW,
    );
    Exchange {
        label: label.to_string(),
        validator: anchored,
        replies: queries
            .into_iter()
            .map(|query| {
                let bytes = resolver.answer(&query).unwrap();
                (query, bytes)
            })
            .collect(),
        question,
        now: NOW,
    }
}

thread_local! {
    static ANCHORS: std::cell::RefCell<Vec<weaver_dnssec::TrustAnchor>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn tree(denial: DenialMode) -> FakeResolver {
    let mut leaf = zone("example.com");
    leaf.denial = denial;
    leaf.add_record(n("news.example.com"), T::A, IP.to_vec());
    leaf.add_record(n("*.wild.example.com"), T::A, IP.to_vec());
    FakeResolver::new(vec![zone("."), zone("com"), leaf]).unwrap()
}

fn nsec3() -> DenialMode {
    DenialMode::Nsec3 {
        salt: vec![0xaa, 0xbb],
        iterations: 1,
        opt_out: false,
    }
}

#[test]
fn generated_positive_answer_survives_truncation_and_mutation() {
    let exchange = generated(
        "positive",
        &tree(DenialMode::Nsec),
        q("news.example.com", T::A),
    );
    assert_eq!(exchange.replies.len(), 6);
    assert!(sweep(&exchange) > 1_000);
}

#[test]
fn generated_nsec_denial_survives_truncation_and_mutation() {
    let exchange = generated(
        "nsec",
        &tree(DenialMode::Nsec),
        q("missing.example.com", T::A),
    );
    assert!(sweep(&exchange) > 1_000);
}

#[test]
fn generated_nsec3_nodata_and_wildcard_survive_truncation_and_mutation() {
    for (label, name, rtype) in [
        ("nsec3 nodata", "news.example.com", T::AAAA),
        ("nsec3 wildcard", "a.wild.example.com", T::A),
    ] {
        let exchange = generated(label, &tree(nsec3()), q(name, rtype));
        assert!(sweep(&exchange) > 1_000);
    }
}

#[test]
fn generated_alias_chain_survives_truncation_and_mutation() {
    let mut first = zone("example.com");
    first.add_record(
        n("news.example.com"),
        T::CNAME,
        n("news.example.net").as_wire().to_vec(),
    );
    let mut second = zone("example.net");
    second.add_record(n("news.example.net"), T::A, IP.to_vec());
    let resolver = alias_resolver();
    let exchange = generated("alias", &resolver, q("news.example.com", T::A));
    assert!(sweep(&exchange) > 1_000);
}

fn alias_resolver() -> FakeResolver {
    let mut first = zone("example.com");
    first.add_record(
        n("news.example.com"),
        T::CNAME,
        n("news.example.net").as_wire().to_vec(),
    );
    let mut second = zone("example.net");
    second.add_record(n("news.example.net"), T::A, IP.to_vec());
    FakeResolver::new(vec![zone("."), first, second]).unwrap()
}

/// Each signed RRset of a section with the RRSIGs that cover it.
fn signed_sets(records: &[Record]) -> Vec<Vec<Record>> {
    let mut sets = BTreeMap::<(Vec<u8>, u16), Vec<Record>>::new();
    for record in records {
        let covered = if record.rtype == T::RRSIG {
            match record.rdata.get(..2) {
                Some(&[high, low]) => u16::from_be_bytes([high, low]),
                _ => continue,
            }
        } else {
            record.rtype.0
        };
        sets.entry((record.owner.as_wire().to_ascii_lowercase(), covered))
            .or_default()
            .push(record.clone());
    }
    sets.into_values()
        .filter(|set| set.iter().any(|record| record.rtype == T::RRSIG))
        .collect()
}

/// Validly signed records a zone no longer serves: its chain rebuilt with
/// 151 NSEC3 iterations, signed by its current keys. (An older opt-out
/// chain is left out: its spans over names that do not exist make those
/// denials insecure by design.)
fn replayed_chains(resolver: &FakeResolver) -> Vec<Vec<Record>> {
    let mut sets = Vec::new();
    for zone in resolver.zones() {
        let mut old = zone.clone();
        old.denial = DenialMode::Nsec3 {
            salt: vec![0xaa, 0xbb],
            iterations: 151,
            opt_out: false,
        };
        sets.extend(signed_sets(&old.denial_records().unwrap()));
    }
    sets
}

fn splice_sweep(exchange: &Exchange, resolver: &FakeResolver) -> usize {
    let originals: BTreeMap<_, _> = exchange.replies.iter().cloned().collect();
    let baseline = validate(exchange, &originals);
    let Verdict::Secure(expected) = &baseline else {
        panic!("{}: baseline {baseline:?}", exchange.label);
    };
    let replies: Vec<(Query, Reply)> = exchange
        .replies
        .iter()
        .map(|(query, _)| (query.clone(), resolver.reply(query).unwrap()))
        .collect();
    let mut donors = replayed_chains(resolver);
    for (_, reply) in &replies {
        donors.extend(signed_sets(&reply.answer));
        donors.extend(signed_sets(&reply.authority));
    }
    let mut runs = 0;
    for (query, reply) in &replies {
        for set in &donors {
            let mut variants = Vec::new();
            for answer in [true, false] {
                let mut beside = reply.clone();
                let mut instead = reply.clone();
                if answer {
                    beside.answer.extend(set.iter().cloned());
                    instead.answer.clone_from(set);
                } else {
                    beside.authority.extend(set.iter().cloned());
                    instead.authority.clone_from(set);
                }
                variants.push(beside);
                variants.push(instead);
            }
            for variant in variants {
                let mut spliced = originals.clone();
                spliced.insert(query.clone(), variant.encode(query).unwrap());
                let verdict = validate(exchange, &spliced);
                runs += 1;
                match &verdict {
                    Verdict::Secure(answer) => {
                        assert_eq!(
                            answer.outcome, expected.outcome,
                            "{}: {query:?} spliced with {set:?}",
                            exchange.label
                        );
                        assert!(answer.ttl <= expected.ttl, "{}", exchange.label);
                    }
                    Verdict::Insecure(_) => panic!(
                        "{}: {query:?} spliced with {set:?} downgraded to {verdict:?}",
                        exchange.label
                    ),
                    Verdict::Bogus(_) | Verdict::Unanswered(_) => {}
                }
            }
        }
    }
    runs
}

#[test]
fn generated_exchanges_survive_cross_reply_replay() {
    for (label, resolver, name, rtype) in [
        ("positive", tree(DenialMode::Nsec), "news.example.com", T::A),
        ("nsec", tree(DenialMode::Nsec), "missing.example.com", T::A),
        ("nsec3 positive", tree(nsec3()), "news.example.com", T::A),
        ("nsec3 nodata", tree(nsec3()), "news.example.com", T::AAAA),
        ("nsec3 nxdomain", tree(nsec3()), "missing.example.com", T::A),
        ("nsec3 wildcard", tree(nsec3()), "a.wild.example.com", T::A),
        ("alias", alias_resolver(), "news.example.com", T::A),
    ] {
        let exchange = generated(label, &resolver, q(name, rtype));
        assert!(splice_sweep(&exchange, &resolver) > 100, "{label}");
    }
}
