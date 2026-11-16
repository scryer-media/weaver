// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Private names, clock tolerance, stripped replies, insecure reasons, alias
//! follow-ups, bogus hold-down, Extended DNS Errors, parallel queries and
//! negative-answer TTLs, over the in-memory resolver.

use std::collections::BTreeSet;

use weaver_dnssec::testing::{
    self, DenialMode, FakeReply, FakeResolver, KeyAlgorithm, Record, Zone, drive,
};
use weaver_dnssec::{
    BogusReason, InsecureReason, Name, Outcome, Policy, Query, Question, RecordType as T, Source,
    Step, SupplyError, UnansweredCause, Validator, Verdict,
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
fn cold_queries(first: &str) -> Vec<Query> {
    vec![
        q(first, T::A),
        q(".", T::DNSKEY),
        q("com", T::DS),
        q("com", T::DNSKEY),
        q("example.com", T::DS),
        q("example.com", T::DNSKEY),
    ]
}
fn records(name: &str) -> Outcome {
    testing::records(n(name), vec![IP.to_vec()])
}
fn replace_reply(resolver: &mut FakeResolver, query: Query, reply: FakeReply) {
    resolver.set_reply(query.clone(), Some(reply.encode(&query).unwrap()));
}
fn secure(verdict: &Verdict, expected: Outcome) -> u32 {
    let Verdict::Secure { answer, .. } = verdict else {
        panic!("expected secure {expected:?}, got {verdict:?}")
    };
    assert_eq!(*answer.outcome(), expected);
    answer.ttl()
}
fn insecure(verdict: &Verdict, expected: Outcome, reason: InsecureReason) {
    let Verdict::Insecure {
        answer,
        reason: actual,
        ..
    } = verdict
    else {
        panic!("expected insecure {expected:?}, got {verdict:?}")
    };
    assert_eq!(*answer.outcome(), expected);
    assert_eq!(*actual, reason);
}
fn bogus(verdict: &Verdict, reason: BogusReason, name: &str, rtype: T) {
    let Verdict::Bogus { failure, .. } = verdict else {
        panic!("expected bogus {reason:?}, got {verdict:?}")
    };
    assert_eq!(failure.reason(), reason);
    assert_eq!(*failure.name(), n(name));
    assert_eq!(failure.rtype(), rtype);
}
fn nsec3_mode(opt_out: bool, iterations: u16) -> DenialMode {
    DenialMode::Nsec3 {
        salt: vec![0xaa, 0xbb],
        iterations,
        opt_out,
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
fn tamper_address(resolver: &mut FakeResolver) {
    resolver.tamper(|query, reply| {
        if query == &q("news.example.com", T::A) {
            reply.answer[0].rdata[3] ^= 1;
        }
    });
}

// FakeReply::encode ends every message with an 11-byte OPT record whose last two
// bytes are its RDLENGTH.
const OPT_BYTES: usize = 11;
fn remove_opt(bytes: &mut Vec<u8>) {
    bytes.truncate(bytes.len() - OPT_BYTES);
    let count = u16::from_be_bytes([bytes[10], bytes[11]]) - 1;
    bytes[10..12].copy_from_slice(&count.to_be_bytes());
}
fn add_extended_errors(bytes: &mut Vec<u8>, codes: &[u16]) {
    let length_at = bytes.len() - 2;
    let mut options = Vec::new();
    for code in codes {
        options.extend([0, 15, 0, 6]);
        options.extend(code.to_be_bytes());
        options.extend(b"why!");
    }
    bytes[length_at..].copy_from_slice(&(options.len() as u16).to_be_bytes());
    bytes.extend(options);
}

// Hand out every query a step names, awaited and ahead, and answer them
// newest first. Returns the verdict, every query asked and the number of
// rounds.
fn drive_parallel(
    validator: &Validator,
    resolver: &FakeResolver,
    question: Question,
    now: u64,
) -> (Verdict, Vec<Query>, usize) {
    let mut validation = validator.begin(question, now);
    let mut asked = Vec::new();
    let mut rounds = 0;
    loop {
        match validation.step() {
            Step::Done { verdict, .. } => return (verdict, asked, rounds),
            Step::Need { awaited, ahead, .. } => {
                rounds += 1;
                let mut batch = vec![awaited];
                batch.extend(ahead);
                for query in batch.iter().rev() {
                    asked.push(query.clone());
                    match resolver.answer(query) {
                        Some(bytes) => validation.supply(query, &bytes).unwrap(),
                        None => validation.supply_failure(query).unwrap(),
                    }
                }
            }
            step => panic!("unexpected step {step:?}"),
        }
    }
}

// Item 1: built-in negative trust anchors.

#[test]
fn private_names_are_insecure_without_any_chain_query() {
    let mut resolver = tree();
    let printer = q("printer.lan", T::A);
    replace_reply(
        &mut resolver,
        printer.clone(),
        FakeReply {
            answer: vec![Record::new(printer.name().clone(), T::A, 60, IP.to_vec())],
            ..FakeReply::default()
        },
    );
    let missing = q("gone.home.arpa", T::AAAA);
    replace_reply(
        &mut resolver,
        missing.clone(),
        FakeReply {
            rcode: 3,
            ..FakeReply::default()
        },
    );
    for v in [validator(&resolver), Validator::with_root_anchors()] {
        let (verdict, queries) = drive(&v, &resolver, question("printer.lan", T::A), NOW);
        insecure(
            &verdict,
            records("printer.lan"),
            InsecureReason::PrivateName,
        );
        assert_eq!(queries, std::slice::from_ref(&printer));
        let (verdict, queries) = drive(&v, &resolver, question("gone.home.arpa", T::AAAA), NOW);
        insecure(&verdict, Outcome::NoSuchName, InsecureReason::PrivateName);
        assert_eq!(queries, std::slice::from_ref(&missing));
    }
}

#[test]
fn a_signed_alias_into_a_private_name_is_insecure_and_asks_nothing_below_it() {
    let mut resolver = tree();
    resolver.zone_mut(&n("example.com")).unwrap().add_record(
        n("nas.example.com"),
        T::CNAME,
        n("nas.home.arpa").as_wire().to_vec(),
    );
    let mut answer = resolver.zones()[2]
        .signed_rrset(&n("nas.example.com"), T::CNAME)
        .unwrap();
    answer.push(Record::new(n("nas.home.arpa"), T::A, 60, IP.to_vec()));
    replace_reply(
        &mut resolver,
        q("nas.example.com", T::A),
        FakeReply {
            answer,
            ..FakeReply::default()
        },
    );
    let (verdict, queries) = run(&resolver, "nas.example.com", T::A);
    insecure(
        &verdict,
        records("nas.home.arpa"),
        InsecureReason::PrivateName,
    );
    assert_eq!(queries, cold_queries("nas.example.com"));
}

#[test]
fn a_configured_anchor_inside_a_private_zone_is_still_validated() {
    let mut corp = zone("corp");
    corp.add_record(n("host.corp"), T::A, IP.to_vec());
    let mut resolver = FakeResolver::new(vec![corp]).unwrap();
    let (verdict, queries) = run(&resolver, "host.corp", T::A);
    secure(&verdict, records("host.corp"));
    assert_eq!(queries, [q("host.corp", T::A), q("corp", T::DNSKEY)]);
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.answer[0].rdata[3] ^= 1;
        }
    });
    bogus(
        &run(&resolver, "host.corp", T::A).0,
        BogusReason::SignatureInvalid,
        "host.corp",
        T::A,
    );
}

// Item 2: unbound's clock tolerance, and clock reasons for valid signatures only.

#[test]
fn clock_tolerance_is_a_tenth_of_the_lifetime_and_reaches_the_key_set() {
    // A 99 999 s lifetime, expired by one second, is inside its 9 999 s tolerance.
    let mut resolver = tree();
    resolver
        .zone_mut(&n("example.com"))
        .unwrap()
        .signature_window
        .expiration = NOW as u32 - 1;
    assert_eq!(
        secure(
            &run(&resolver, "news.example.com", T::A).0,
            records("news.example.com")
        ),
        0
    );
    // A 90 000 s lifetime expired by 10 000 s is past its 9 000 s tolerance.
    // The zone's DNSKEY set fails first, and says why.
    resolver
        .zone_mut(&n("example.com"))
        .unwrap()
        .signature_window
        .expiration = NOW as u32 - 10_000;
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignatureExpired,
        "example.com",
        T::DNSKEY,
    );
    resolver
        .zone_mut(&n("example.com"))
        .unwrap()
        .signature_window
        .inception = NOW as u32 + 100_000;
    resolver
        .zone_mut(&n("example.com"))
        .unwrap()
        .signature_window
        .expiration = 2_000_000;
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::SignatureNotYetValid,
        "example.com",
        T::DNSKEY,
    );
}

#[test]
fn a_forged_signature_outside_its_window_is_not_a_clock_failure() {
    let mut resolver = tree();
    resolver
        .zone_mut(&n("example.com"))
        .unwrap()
        .signature_window
        .expiration = NOW as u32 - 10_000;
    resolver.tamper(|query, reply| {
        if query == &q("example.com", T::DNSKEY) {
            for record in reply.answer.iter_mut().filter(|r| r.rtype == T::RRSIG) {
                *record.rdata.last_mut().unwrap() ^= 1;
            }
        }
    });
    bogus(
        &run(&resolver, "news.example.com", T::A).0,
        BogusReason::KeysNotProven,
        "example.com",
        T::DNSKEY,
    );
}

// Item 3: replies without DNSSEC records.

#[test]
fn a_reply_without_signatures_in_a_signed_zone_names_the_server() {
    for remove in [false, true] {
        let mut resolver = tree();
        resolver.tamper(|query, reply| {
            if query.rtype() == T::A {
                reply.strip_signatures();
            }
        });
        if remove {
            resolver.tamper_bytes(|query, bytes| {
                if query.rtype() == T::A {
                    remove_opt(bytes);
                }
            });
        }
        bogus(
            &run(&resolver, "news.example.com", T::A).0,
            BogusReason::NoDnssecRecords,
            "news.example.com",
            T::A,
        );
    }
}

#[test]
fn a_reply_without_opt_that_still_carries_signatures_validates() {
    let mut resolver = tree();
    resolver.tamper_bytes(|_, bytes| remove_opt(bytes));
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
    );
}

#[test]
fn a_stripped_reply_in_an_unsigned_zone_stays_insecure() {
    let mut resolver = unsigned(DenialMode::Nsec);
    resolver.tamper_bytes(|query, bytes| {
        if query.rtype() == T::A {
            remove_opt(bytes);
        }
    });
    insecure(
        &run(&resolver, "news.example", T::A).0,
        records("news.example"),
        InsecureReason::UnsignedDelegation,
    );
}

// Item 4: every insecure verdict carries its reason.

#[test]
fn insecure_verdicts_carry_their_reasons() {
    for (mode, reason) in [
        (DenialMode::Nsec, InsecureReason::UnsignedDelegation),
        (nsec3_mode(false, 0), InsecureReason::UnsignedDelegation),
        (nsec3_mode(true, 0), InsecureReason::UnsignedDelegation),
    ] {
        let resolver = unsigned(mode);
        insecure(
            &run(&resolver, "news.example", T::A).0,
            records("news.example"),
            reason,
        );
    }
    // NSEC3 over the iteration limit proves nothing, so it is no reason.
    bogus(
        &run(&unsigned(nsec3_mode(false, 151)), "news.example", T::A).0,
        BogusReason::Nsec3Iterations,
        "example",
        T::DS,
    );
    let mut resolver = tree();
    let mut ds = resolver.zones()[2].ds_records(2).unwrap();
    for record in &mut ds {
        record[2] = 253;
    }
    resolver
        .zone_mut(&n("com"))
        .unwrap()
        .delegate(n("example.com"), ds);
    insecure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
        InsecureReason::UnsupportedAlgorithm,
    );
}

#[test]
fn a_name_no_anchor_covers_is_insecure_without_chain_queries() {
    let resolver = tree();
    let island = zone("example.org");
    let v = Validator::new(island.anchors().unwrap(), Policy::default()).unwrap();
    let (verdict, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW);
    insecure(
        &verdict,
        records("news.example.com"),
        InsecureReason::NoTrustAnchor,
    );
    assert_eq!(queries, [q("news.example.com", T::A)]);
}

#[test]
fn the_cached_insecure_reason_is_reused() {
    let resolver = unsigned(nsec3_mode(false, 0));
    let v = validator(&resolver);
    for pass in 0..2 {
        let (verdict, queries) = drive(&v, &resolver, question("news.example", T::A), NOW);
        insecure(
            &verdict,
            records("news.example"),
            InsecureReason::UnsignedDelegation,
        );
        assert_eq!(queries.len(), if pass == 0 { 3 } else { 1 });
    }
}

// Item 5: the root trust anchor must match a root key.

#[test]
fn root_keys_that_no_anchor_names_are_an_anchor_mismatch() {
    let resolver = tree();
    let stranger = zone(".");
    let v = Validator::new(stranger.anchors().unwrap(), Policy::default()).unwrap();
    let (verdict, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW);
    bogus(&verdict, BogusReason::TrustAnchorMismatch, ".", T::DNSKEY);
    assert_eq!(queries, cold_queries("news.example.com")[..2]);
}

// Item 6: NSEC3 iterations only matter where a denial is needed.

#[test]
fn high_nsec3_iterations_never_touch_a_signed_positive_answer() {
    let mut resolver = tree();
    resolver.zone_mut(&n("example.com")).unwrap().denial = nsec3_mode(false, 500);
    resolver.zone_mut(&n("com")).unwrap().denial = nsec3_mode(false, 500);
    assert_eq!(
        secure(
            &run(&resolver, "news.example.com", T::A).0,
            records("news.example.com")
        ),
        300
    );
    // A denial only high-iteration NSEC3 could give fails, and is never
    // an insecure answer an attacker could replay any one record to reach.
    bogus(
        &run(&resolver, "missing.example.com", T::A).0,
        BogusReason::Nsec3Iterations,
        "missing.example.com",
        T::A,
    );
}

// Item 7: an alias whose target the reply does not hold.

#[test]
fn an_alias_target_missing_from_the_reply_is_asked_for() {
    let mut first = zone("example.com");
    first.add_record(
        n("news.example.com"),
        T::CNAME,
        n("news.example.net").as_wire().to_vec(),
    );
    let mut second = zone("example.net");
    second.add_record(n("news.example.net"), T::A, IP.to_vec());
    let mut resolver = FakeResolver::new(vec![zone("."), first, second]).unwrap();
    resolver.tamper(|query, reply| {
        if query == &q("news.example.com", T::A) {
            reply
                .answer
                .retain(|record| record.owner == n("news.example.com"));
            reply.authority.clear();
        }
    });
    let (verdict, queries) = run(&resolver, "news.example.com", T::A);
    secure(&verdict, records("news.example.net"));
    assert_eq!(
        verdict.answer().unwrap().alias_chain(),
        [n("news.example.com"), n("news.example.net")]
    );
    assert!(queries.contains(&q("news.example.net", T::A)));
    assert_eq!(
        queries.iter().filter(|query| query.rtype() == T::A).count(),
        2
    );
}

#[test]
fn an_alias_loop_across_follow_up_replies_is_refused() {
    let mut first = zone("example.com");
    first.add_record(
        n("news.example.com"),
        T::CNAME,
        n("news.example.net").as_wire().to_vec(),
    );
    let mut second = zone("example.net");
    second.add_record(
        n("news.example.net"),
        T::CNAME,
        n("news.example.com").as_wire().to_vec(),
    );
    let mut resolver = FakeResolver::new(vec![zone("."), first, second]).unwrap();
    let back = resolver.zones()[2]
        .signed_rrset(&n("news.example.net"), T::CNAME)
        .unwrap();
    replace_reply(
        &mut resolver,
        q("news.example.net", T::A),
        FakeReply {
            answer: back,
            ..FakeReply::default()
        },
    );
    resolver.tamper(|query, reply| {
        if query == &q("news.example.com", T::A) {
            reply
                .answer
                .retain(|record| record.owner == n("news.example.com"));
            reply.authority.clear();
        }
    });
    let (verdict, queries) = run(&resolver, "news.example.com", T::A);
    bogus(
        &verdict,
        BogusReason::AliasInvalid,
        "news.example.net",
        T::CNAME,
    );
    assert_eq!(
        queries.iter().filter(|query| query.rtype() == T::A).count(),
        2
    );
}

#[test]
fn an_alias_ending_in_a_proven_denial_is_not_followed() {
    let mut first = zone("example.com");
    first.add_record(
        n("news.example.com"),
        T::CNAME,
        n("gone.example.com").as_wire().to_vec(),
    );
    let resolver = FakeResolver::new(vec![zone("."), first]).unwrap();
    let (verdict, queries) = run(&resolver, "news.example.com", T::A);
    let Verdict::Secure { answer, .. } = verdict else {
        panic!("{verdict:?}");
    };
    assert_eq!(*answer.outcome(), Outcome::NoSuchName);
    assert_eq!(
        answer.alias_chain(),
        [n("news.example.com"), n("gone.example.com")]
    );
    assert_eq!(
        queries.iter().filter(|query| query.rtype() == T::A).count(),
        1
    );
}

// Item 8: a proven failure is held for a minute.

#[test]
fn a_proven_failure_is_held_for_sixty_seconds_without_queries() {
    let mut resolver = tree();
    tamper_address(&mut resolver);
    let v = validator(&resolver);
    let (first, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW);
    bogus(
        &first,
        BogusReason::SignatureInvalid,
        "news.example.com",
        T::A,
    );
    assert_eq!(queries, cold_queries("news.example.com"));
    for now in [NOW, NOW + 59] {
        let (held, queries) = drive(&v, &resolver, question("news.example.com", T::A), now);
        assert_eq!(held, first);
        assert!(queries.is_empty());
    }
    // Another type at the same name is not held.
    let (_, queries) = drive(&v, &resolver, question("news.example.com", T::AAAA), NOW);
    assert_eq!(queries, [q("news.example.com", T::AAAA)]);
    // After a minute the name is validated again, against the shared cache.
    let (again, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW + 60);
    assert_eq!(again, first);
    assert_eq!(queries, [q("news.example.com", T::A)]);
}

fn drive_from(
    validator: &Validator,
    resolver: &FakeResolver,
    source: u64,
    question: Question,
    now: u64,
) -> (Verdict, Vec<Query>) {
    let mut validation = validator.begin_from(Source::new(source), question, now);
    validation.set_ask_ahead(false);
    let mut queries = Vec::new();
    loop {
        match validation.step() {
            Step::Done { verdict, .. } => return (verdict, queries),
            Step::Need { awaited: query, .. } => {
                queries.push(query.clone());
                match resolver.answer(&query) {
                    Some(bytes) => validation.supply(&query, &bytes).unwrap(),
                    None => validation.supply_failure(&query).unwrap(),
                }
            }
            step => panic!("unexpected step {step:?}"),
        }
    }
}

#[test]
fn a_proven_failure_is_held_only_for_the_source_that_gave_it() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    // One signed tree; source 1 changes the address after signing, source 2
    // answers as signed.
    let lying = Arc::new(AtomicBool::new(true));
    let mut resolver = tree();
    let lies = lying.clone();
    resolver.tamper(move |query, reply| {
        if lies.load(Ordering::Relaxed) && query == &q("news.example.com", T::A) {
            reply.answer[0].rdata[3] ^= 1;
        }
    });
    let (tampered, good) = (&resolver, &resolver);
    let v = validator(&resolver);
    let (first, queries) = drive_from(&v, tampered, 1, question("news.example.com", T::A), NOW);
    bogus(
        &first,
        BogusReason::SignatureInvalid,
        "news.example.com",
        T::A,
    );
    assert_eq!(queries, cold_queries("news.example.com"));
    // The same source inside the minute is held and asked nothing.
    let (held, queries) = drive_from(&v, tampered, 1, question("news.example.com", T::A), NOW);
    assert_eq!(held, first);
    assert!(queries.is_empty());
    // Another source is asked, and its good answer validates against the
    // keys the first run already proved.
    lying.store(false, Ordering::Relaxed);
    let (verdict, queries) = drive_from(&v, good, 2, question("news.example.com", T::A), NOW);
    secure(&verdict, records("news.example.com"));
    assert_eq!(queries, [q("news.example.com", T::A)]);
    // The first source is still held.
    lying.store(true, Ordering::Relaxed);
    let (held, queries) = drive_from(
        &v,
        tampered,
        1,
        question("news.example.com", T::A),
        NOW + 59,
    );
    assert_eq!(held, first);
    assert!(queries.is_empty());
}

#[test]
fn begin_is_source_zero() {
    let mut tampered = tree();
    tamper_address(&mut tampered);
    let v = validator(&tampered);
    let (first, _) = drive(&v, &tampered, question("news.example.com", T::A), NOW);
    bogus(
        &first,
        BogusReason::SignatureInvalid,
        "news.example.com",
        T::A,
    );
    let (held, queries) = drive_from(&v, &tampered, 0, question("news.example.com", T::A), NOW);
    assert_eq!(held, first);
    assert!(queries.is_empty());
    let (_, queries) = drive_from(&v, &tampered, 1, question("news.example.com", T::A), NOW);
    assert_eq!(queries, [q("news.example.com", T::A)]);
    // And a hold made from source 0 is what begin finds.
    let v = validator(&tampered);
    let (first, _) = drive_from(&v, &tampered, 0, question("news.example.com", T::A), NOW);
    let (held, queries) = drive(&v, &tampered, question("news.example.com", T::A), NOW);
    assert_eq!(held, first);
    assert!(queries.is_empty());
}

#[test]
fn transport_failures_and_malformed_replies_are_never_held() {
    let target = q("news.example.com", T::A);
    let shapes: [(Option<Vec<u8>>, Option<UnansweredCause>); 3] = [
        (None, Some(UnansweredCause::FetchFailed)),
        (
            Some(
                FakeReply {
                    rcode: 2,
                    ..FakeReply::default()
                }
                .encode(&target)
                .unwrap(),
            ),
            Some(UnansweredCause::Rcode(2)),
        ),
        (Some(vec![0; 5]), None),
    ];
    for (reply, cause) in shapes {
        let mut resolver = tree();
        resolver.set_reply(target.clone(), reply);
        let v = validator(&resolver);
        for _ in 0..2 {
            let (verdict, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW);
            match (&verdict, cause) {
                (Verdict::Unanswered { failure, .. }, Some(cause)) => {
                    assert_eq!(failure.cause(), cause)
                }
                (Verdict::Bogus { failure, .. }, None) => {
                    assert_eq!(failure.reason(), BogusReason::MalformedReply)
                }
                _ => panic!("{verdict:?}"),
            }
            assert_eq!(queries, std::slice::from_ref(&target));
        }
    }
    let mut resolver = tree();
    resolver.tamper(|query, reply| {
        if query.rtype() == T::A {
            reply.strip_signatures();
        }
    });
    let v = validator(&resolver);
    for pass in 0..2 {
        let (verdict, queries) = drive(&v, &resolver, question("news.example.com", T::A), NOW);
        bogus(
            &verdict,
            BogusReason::NoDnssecRecords,
            "news.example.com",
            T::A,
        );
        // The unsigned owner's own DS is asked for too on the cold pass.
        assert_eq!(queries.len(), if pass == 0 { 7 } else { 1 });
    }
}

// Item 9: Extended DNS Errors are reported, never acted on.

#[test]
fn extended_errors_ride_on_failures_and_never_change_a_verdict() {
    let mut resolver = tree();
    resolver.tamper_bytes(|query, bytes| {
        if query == &q("news.example.com", T::A) {
            add_extended_errors(bytes, &[3, 6, 3]);
        }
    });
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
    );
    tamper_address(&mut resolver);
    let Verdict::Bogus { failure, .. } = run(&resolver, "news.example.com", T::A).0 else {
        panic!("a changed address must fail");
    };
    assert_eq!(failure.reason(), BogusReason::SignatureInvalid);
    assert_eq!(failure.extended_errors(), [3, 6]);

    let mut resolver = tree();
    let target = q("news.example.com", T::A);
    let mut servfail = FakeReply {
        rcode: 2,
        ..FakeReply::default()
    }
    .encode(&target)
    .unwrap();
    add_extended_errors(&mut servfail, &[22]);
    resolver.set_reply(target.clone(), Some(servfail));
    let Verdict::Unanswered {
        failure: unanswered,
        ..
    } = run(&resolver, "news.example.com", T::A).0
    else {
        panic!("SERVFAIL must be unanswered");
    };
    assert_eq!(unanswered.cause(), UnansweredCause::Rcode(2));
    assert_eq!(unanswered.extended_errors(), [22]);
}

#[test]
fn a_malformed_option_list_is_ignored() {
    let mut resolver = tree();
    resolver.tamper_bytes(|query, bytes| {
        if query == &q("news.example.com", T::A) {
            // One option claims more data than the OPT record holds.
            let length_at = bytes.len() - 2;
            bytes[length_at..].copy_from_slice(&5_u16.to_be_bytes());
            bytes.extend([0, 15, 0, 9, 0]);
        }
    });
    secure(
        &run(&resolver, "news.example.com", T::A).0,
        records("news.example.com"),
    );
}

// Item 10: several queries at once, answered in any order.

#[test]
fn independent_queries_are_handed_out_together_and_answered_in_any_order() {
    let resolver = tree();
    let sequential = run(&resolver, "news.example.com", T::A);
    let (verdict, asked, rounds) = drive_parallel(
        &validator(&resolver),
        &resolver,
        question("news.example.com", T::A),
        NOW,
    );
    assert_eq!(verdict, sequential.0);
    assert_eq!(
        asked.iter().cloned().collect::<BTreeSet<_>>(),
        sequential.1.into_iter().collect::<BTreeSet<_>>()
    );
    assert_eq!(asked.len(), 6);
    assert_eq!(rounds, 3);
}

#[test]
fn parallel_replies_keep_the_step_contract() {
    let resolver = tree();
    let v = validator(&resolver);
    let mut validation = v.begin(question("news.example.com", T::A), NOW);
    let first = q("news.example.com", T::A);
    let Step::Need {
        awaited,
        ahead: extra,
        ..
    } = validation.step()
    else {
        panic!("the first step needs a query");
    };
    assert_eq!(awaited, first);
    assert_eq!(extra, [q(".", T::DNSKEY)]);
    // A query asked ahead is handed out once.
    assert_eq!(awaited_alone(validation.step()), first);
    // A query nobody handed out is still refused.
    assert_eq!(
        validation.supply(
            &q("com", T::DS),
            &resolver.answer(&q("com", T::DS)).unwrap()
        ),
        Err(SupplyError::UnexpectedQuery)
    );
    // The stepped query stays the one asked for until it is answered.
    validation
        .supply(&extra[0], &resolver.answer(&extra[0]).unwrap())
        .unwrap();
    assert_eq!(
        validation.supply(&extra[0], &resolver.answer(&extra[0]).unwrap()),
        Err(SupplyError::UnexpectedQuery)
    );
    assert_eq!(awaited_alone(validation.step()), first);
    validation
        .supply(&first, &resolver.answer(&first).unwrap())
        .unwrap();
    let Step::Need { awaited, .. } = validation.step() else {
        panic!("the chain still needs a query");
    };
    assert_eq!(awaited, q("com", T::DS));
}

#[test]
fn a_caller_that_ignores_ahead_still_reaches_a_verdict() {
    // Ask-ahead stays at its default, on. The caller sends only the
    // awaited query of each step and never looks at `ahead`.
    let resolver = tree();
    let v = validator(&resolver);
    let mut validation = v.begin(question("news.example.com", T::A), NOW);
    let mut asked = Vec::new();
    let mut handed_out = BTreeSet::new();
    let verdict = loop {
        match validation.step() {
            Step::Done { verdict, .. } => break verdict,
            Step::Need { awaited, ahead, .. } => {
                handed_out.insert(awaited.clone());
                handed_out.extend(ahead);
                asked.push(awaited.clone());
                match resolver.answer(&awaited) {
                    Some(bytes) => validation.supply(&awaited, &bytes).unwrap(),
                    None => validation.supply_failure(&awaited).unwrap(),
                }
            }
            step => panic!("unexpected step {step:?}"),
        }
    };
    secure(&verdict, records("news.example.com"));
    assert_eq!(asked, cold_queries("news.example.com"));
    // Every query handed out ahead came back later as the awaited one.
    assert_eq!(handed_out, asked.into_iter().collect::<BTreeSet<_>>());
}

/// The awaited query of a step that asks nothing ahead.
fn awaited_alone(step: Step) -> Query {
    let Step::Need { awaited, ahead, .. } = step else {
        panic!("expected a needed query, got {step:?}");
    };
    assert!(ahead.is_empty(), "{ahead:?}");
    awaited
}

#[test]
fn a_failed_parallel_query_ends_the_validation_when_it_is_needed() {
    let mut resolver = tree();
    resolver.set_reply(q("example.com", T::DNSKEY), None);
    let (verdict, _, _) = drive_parallel(
        &validator(&resolver),
        &resolver,
        question("news.example.com", T::A),
        NOW,
    );
    let Verdict::Unanswered {
        failure: unanswered,
        ..
    } = verdict
    else {
        panic!("{verdict:?}");
    };
    assert_eq!(*unanswered.query(), q("example.com", T::DNSKEY));
    assert_eq!(unanswered.cause(), UnansweredCause::FetchFailed);
}

#[test]
fn parallel_queries_share_the_forty_query_total() {
    let labels = (0..45).map(|_| "a").collect::<Vec<_>>().join(".");
    let mut root = zone(".");
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
    let (verdict, asked, _) = drive_parallel(
        &validator(&resolver),
        &resolver,
        Question::new(main.name().clone(), T::A).unwrap(),
        NOW,
    );
    let Verdict::Bogus { failure, .. } = verdict else {
        panic!("{verdict:?}");
    };
    assert_eq!(failure.reason(), BogusReason::WorkLimit);
    assert_eq!(asked.len(), 40);
    assert_eq!(asked.iter().collect::<BTreeSet<_>>().len(), 40);
}

// Item 13: a negative answer's TTL comes only from validated records.

#[test]
fn a_forged_negative_soa_fails_and_its_ttl_never_leaks() {
    for mode in [DenialMode::Nsec, nsec3_mode(false, 0)] {
        let mut resolver = tree();
        resolver.zone_mut(&n("example.com")).unwrap().denial = mode;
        let v = validator(&resolver);
        let (verdict, _) = drive(&v, &resolver, question("missing.example.com", T::A), NOW);
        let Verdict::Secure { answer, .. } = verdict else {
            panic!("{verdict:?}");
        };
        assert_eq!(*answer.outcome(), Outcome::NoSuchName);
        assert_eq!(answer.ttl(), 300);
        // The record TTL is not signed: a larger one is bounded by the signature.
        let mut raised = tree();
        raised.zone_mut(&n("example.com")).unwrap().denial = resolver.zones()[2].denial.clone();
        raised.tamper(|query, reply| {
            if query.rtype() == T::A {
                for record in reply.authority.iter_mut() {
                    record.ttl = 1_000_000;
                }
            }
        });
        let Verdict::Secure { answer, .. } = run(&raised, "missing.example.com", T::A).0 else {
            panic!("an unsigned TTL change must not fail the proof");
        };
        assert_eq!(answer.ttl(), 300);
        // The SOA minimum is signed: changing it fails the answer.
        let mut forged = tree();
        forged.zone_mut(&n("example.com")).unwrap().denial = resolver.zones()[2].denial.clone();
        forged.tamper(|query, reply| {
            if query.rtype() == T::A
                && let Some(soa) = reply.authority.iter_mut().find(|r| r.rtype == T::SOA)
            {
                let at = soa.rdata.len() - 1;
                soa.rdata[at] = 1;
            }
        });
        bogus(
            &run(&forged, "missing.example.com", T::A).0,
            BogusReason::SignatureInvalid,
            "example.com",
            T::SOA,
        );
        // So does an SOA that lost its signature.
        let mut unsigned_soa = tree();
        unsigned_soa.zone_mut(&n("example.com")).unwrap().denial =
            resolver.zones()[2].denial.clone();
        unsigned_soa.tamper(|query, reply| {
            if query.rtype() == T::A {
                reply.authority.retain(|record| {
                    !(record.rtype == T::RRSIG && record.rdata.get(..2) == Some(&[0, 6]))
                });
            }
        });
        bogus(
            &run(&unsigned_soa, "missing.example.com", T::A).0,
            BogusReason::SignaturesMissing,
            "example.com",
            T::SOA,
        );
    }
}

// Item 14, found by the robustness sweep: a forged denial record is never
// passed over, even when the rest of the reply proves the denial.

#[test]
fn a_forged_denial_record_beside_a_valid_proof_is_bogus() {
    let mut resolver = tree();
    secure(
        &run(&resolver, "missing.example.com", T::A).0,
        Outcome::NoSuchName,
    );
    resolver.tamper(|query, reply| {
        if query != &q("missing.example.com", T::A) {
            return;
        }
        // A copy of the first NSEC and its signature under another owner:
        // the zone's signer name, with a signature that cannot verify.
        let nsec = reply
            .authority
            .iter()
            .find(|record| record.rtype == T::NSEC)
            .unwrap()
            .clone();
        let signature = reply
            .authority
            .iter()
            .find(|record| {
                record.rtype == T::RRSIG
                    && record.owner == nsec.owner
                    && record.rdata.get(..2) == Some(&[0, 47])
            })
            .unwrap()
            .clone();
        for mut record in [nsec, signature] {
            record.owner = n("zzz.example.com");
            reply.authority.push(record);
        }
    });
    bogus(
        &run(&resolver, "missing.example.com", T::A).0,
        BogusReason::DenialInvalid,
        "zzz.example.com",
        T::NSEC,
    );
}
