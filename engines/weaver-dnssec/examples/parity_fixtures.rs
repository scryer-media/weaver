// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Export generated, frozen packets for a separately operated validator comparison.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use weaver_dnssec::testing::{
    DenialMode, FakeResolver, KeyAlgorithm, Record, Reply, SignatureWindow, Zone, base32hex, drive,
    nsec3_hash,
};
use weaver_dnssec::{Name, Policy, Query, Question, RecordType as T, Validator, Verdict};

const IP: [u8; 4] = [192, 0, 2, 7];

struct Case {
    id: &'static str,
    name: Name,
    rtype: T,
    intended: &'static str,
    resolver: FakeResolver,
}

fn n(text: &str) -> Name {
    Name::from_text(text).unwrap()
}

fn query(text: &str, rtype: T) -> Query {
    Query {
        name: n(text),
        rtype,
    }
}

fn zone(apex: &str, algorithm: KeyAlgorithm, now: u32) -> Zone {
    let mut zone = Zone::new(n(apex), algorithm).unwrap();
    zone.signature_window = SignatureWindow {
        inception: now.checked_sub(86_400).unwrap(),
        expiration: now.checked_add(86_400).unwrap(),
    };
    zone
}

fn tree(algorithm: KeyAlgorithm, now: u32) -> FakeResolver {
    let root = zone(".", KeyAlgorithm::P256, now);
    let mut leaf = zone("example.", algorithm, now);
    leaf.add_record(n("news.example."), T::A, IP.to_vec());
    leaf.add_record(n("other.example."), T::A, IP.to_vec());
    FakeResolver::new(vec![root, leaf]).unwrap()
}

fn case(id: &'static str, intended: &'static str, resolver: FakeResolver) -> Case {
    Case {
        id,
        name: n("news.example."),
        rtype: T::A,
        intended,
        resolver,
    }
}

fn nsec3(salt: &[u8], iterations: u16) -> DenialMode {
    DenialMode::Nsec3 {
        salt: salt.to_vec(),
        iterations,
        opt_out: false,
    }
}

fn negative(id: &'static str, intended: &'static str, mode: DenialMode, now: u32) -> Case {
    let mut resolver = tree(KeyAlgorithm::P256, now);
    resolver.zone_mut(&n("example.")).unwrap().denial = mode;
    let mut case = case(id, intended, resolver);
    case.name = n("missing.example.");
    case
}

fn replace(resolver: &mut FakeResolver, query: Query, reply: Reply) {
    resolver.set_reply(query.clone(), Some(reply.encode(&query).unwrap()));
}

fn mixed_ds_cases(now: u32) -> Vec<Case> {
    let mut cases = Vec::new();
    for (ids, weak_valid, strong_valid, algorithm, digest) in [
        (
            ["ds-bad-sha256-weak-first", "ds-bad-sha256-strong-first"],
            true,
            false,
            13,
            2,
        ),
        (
            ["ds-both-valid-weak-first", "ds-both-valid-strong-first"],
            true,
            true,
            13,
            2,
        ),
        (
            ["ds-bad-sha1-weak-first", "ds-bad-sha1-strong-first"],
            false,
            true,
            13,
            2,
        ),
        (
            [
                "ds-unsupported-algorithm-weak-first",
                "ds-unsupported-algorithm-strong-first",
            ],
            true,
            true,
            253,
            2,
        ),
        (
            [
                "ds-unsupported-digest-weak-first",
                "ds-unsupported-digest-strong-first",
            ],
            true,
            true,
            13,
            253,
        ),
    ] {
        for (reverse, id) in [false, true].into_iter().zip(ids) {
            let mut resolver = tree(KeyAlgorithm::P256, now);
            let leaf = &resolver.zones()[1];
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
            let mut ds = vec![weak, strong];
            if reverse {
                ds.reverse();
            }
            // Mutate the digest before the parent signs the entire DS RRset.
            resolver
                .zone_mut(&Name::root())
                .unwrap()
                .delegate(n("example."), ds);
            cases.push(case(
                id,
                "supported stronger DS displaces SHA-1; unsupported DS does not",
                resolver,
            ));
        }
    }
    cases
}

fn cases(now: u32) -> Vec<Case> {
    let mut cases = vec![
        case(
            "p256-good",
            "ordinary signed positive control",
            tree(KeyAlgorithm::P256, now),
        ),
        case(
            "mldsa44-good",
            "algorithm 18 signed positive control",
            tree(KeyAlgorithm::MlDsa44, now),
        ),
        case(
            "ed448-good",
            "algorithm 16 signed positive control",
            tree(KeyAlgorithm::Ed448, now),
        ),
    ];

    for (algorithm, id) in [
        (KeyAlgorithm::P256, "p256-bad"),
        (KeyAlgorithm::MlDsa44, "mldsa44-bad"),
        (KeyAlgorithm::Ed448, "ed448-bad"),
    ] {
        let mut resolver = tree(algorithm, now);
        resolver.tamper(|query, reply| {
            if query == &self::query("news.example.", T::A) {
                for record in &mut reply.answer {
                    if record.rtype == T::RRSIG {
                        *record.rdata.last_mut().unwrap() ^= 1;
                    }
                }
            }
        });
        cases.push(case(id, "signature tampered after signing", resolver));
    }

    let root = zone(".", KeyAlgorithm::P256, now);
    let mut leaf = zone("example.", KeyAlgorithm::P256, now);
    leaf.unsigned = true;
    leaf.add_record(n("news.example."), T::A, IP.to_vec());
    cases.push(case(
        "unsigned-delegation",
        "authenticated unsigned delegation",
        FakeResolver::new(vec![root, leaf]).unwrap(),
    ));

    cases.push(negative(
        "nxdomain-nsec",
        "signed NSEC NXDOMAIN positive control",
        DenialMode::Nsec,
        now,
    ));
    cases.push(negative(
        "nxdomain-nsec3",
        "signed NSEC3 NXDOMAIN positive control",
        nsec3(&[0xaa, 0xbb], 0),
        now,
    ));

    for expired in [true, false] {
        let mut resolver = tree(KeyAlgorithm::P256, now);
        let mut signer = resolver.zones()[1].clone();
        if expired {
            signer.signature_window.expiration = now - 60;
        } else {
            signer.signature_window.inception = now + 60;
        }
        replace(
            &mut resolver,
            query("news.example.", T::A),
            Reply {
                answer: signer.signed_rrset(&n("news.example."), T::A).unwrap(),
                ..Reply::default()
            },
        );
        cases.push(case(
            if expired {
                "expired-60s"
            } else {
                "not-yet-60s"
            },
            if expired {
                "signature expired 60 seconds; reject"
            } else {
                "signature starts 60 seconds in future; reject"
            },
            resolver,
        ));
    }

    cases.push(negative(
        "nsec3-iterations-151",
        "151 NSEC3 iterations exceed policy and fail closed",
        nsec3(&[0xaa, 0xbb], 151),
        now,
    ));

    let mut resolver = tree(KeyAlgorithm::P256, now);
    let sibling = zone("sibling.", KeyAlgorithm::P256, now);
    let unrelated = sibling
        .sign_records(vec![Record::new(
            n("news.example."),
            T::A,
            300,
            IP.to_vec(),
        )])
        .unwrap();
    resolver.tamper(move |query, reply| {
        if query == &self::query("news.example.", T::A) {
            reply.answer.extend(
                unrelated
                    .iter()
                    .filter(|record| record.rtype == T::RRSIG)
                    .cloned(),
            );
        }
    });
    cases.push(case(
        "unrelated-rrsig",
        "valid signature accompanied by unrelated sibling signer; ignore unrelated signature",
        resolver,
    ));

    for (links, id) in [
        (9, "nine-cname-links"),
        (10, "ten-cname-links"),
        (11, "eleven-cname-links"),
    ] {
        let root = zone(".", KeyAlgorithm::P256, now);
        let mut leaf = zone("example.", KeyAlgorithm::P256, now);
        for link in 0..links {
            leaf.add_record(
                n(&format!("n{link}.example.")),
                T::CNAME,
                n(&format!("n{}.example.", link + 1)).as_wire().to_vec(),
            );
        }
        leaf.add_record(n(&format!("n{links}.example.")), T::A, IP.to_vec());
        let mut alias = case(
            id,
            "signed CNAME traversal is independent of dnsmasq cache extraction limits",
            FakeResolver::new(vec![root, leaf]).unwrap(),
        );
        alias.name = n("n0.example.");
        cases.push(alias);
    }

    for (failures, id) in [
        (8, "ninth-valid-signature"),
        (20, "twenty-first-valid-signature"),
        (21, "twenty-second-valid-signature"),
    ] {
        let mut resolver = tree(KeyAlgorithm::P256, now);
        resolver.tamper(move |query, reply| {
            if query != &self::query("news.example.", T::A) {
                return;
            }
            let signature = reply.answer.pop().unwrap();
            assert_eq!(signature.rtype, T::RRSIG);
            for mutation in 1..=failures {
                let mut invalid = signature.clone();
                *invalid.rdata.last_mut().unwrap() ^= mutation;
                reply.answer.push(invalid);
            }
            reply.answer.push(signature);
        });
        cases.push(case(
            id,
            "valid signature after distinct invalid signatures; allow twenty failures",
            resolver,
        ));
    }

    let mut salts = negative(
        "two-nsec3-salts",
        "two complete independently signed salt chains; accept either complete chain",
        nsec3(&[0xaa, 0xbb], 0),
        now,
    );
    let mut second = salts.resolver.zones()[1].clone();
    let first_chain = second.denial_records().unwrap();
    second.denial = nsec3(&[0xcc, 0xdd], 0);
    let second_chain = second.denial_records().unwrap();
    salts.resolver.tamper(move |query, reply| {
        if query == &self::query("missing.example.", T::A) {
            retain_other_types(&mut reply.authority, T::NSEC3);
            reply.authority.extend(first_chain.clone());
            reply.authority.extend(second_chain.clone());
        }
    });
    cases.push(salts);

    let mut incomplete = negative(
        "missing-wildcard-denial",
        "signed queried-name span lacks wildcard denial; local DenialInvalid",
        DenialMode::Nsec,
        now,
    );
    let mut rdata = n("z.example.").as_wire().to_vec();
    rdata.extend([0, 6, 0x40, 0, 0, 0, 0, 3]);
    let proof = incomplete.resolver.zones()[1]
        .sign_records(vec![Record::new(n("a.example."), T::NSEC, 300, rdata)])
        .unwrap();
    incomplete.resolver.tamper(move |query, reply| {
        if query == &self::query("missing.example.", T::A) {
            retain_other_types(&mut reply.authority, T::NSEC);
            reply.authority.extend(proof.clone());
        }
    });
    cases.push(incomplete);
    let salt = [0xaa, 0xbb];
    let hash = nsec3_hash(&n("missing.example."), &salt, 0);
    let mut lower = hash.clone();
    let mut upper = hash.clone();
    assert!((1..u8::MAX).contains(hash.last().unwrap()));
    *lower.last_mut().unwrap() -= 1;
    *upper.last_mut().unwrap() += 1;
    for owner in ["example.", "*.example."] {
        let other = nsec3_hash(&n(owner), &salt, 0);
        assert!(other < lower || other > upper);
    }
    let mut rdata = vec![1, 0, 0, 0, 2, salt[0], salt[1], 20];
    rdata.extend(upper);
    rdata.extend([0, 1, 0x40]);
    let mut incomplete = negative(
        "nsec3-qname-cover-only",
        "signed QNAME-covering NSEC3 lacks closest-encloser and wildcard proofs; reject",
        nsec3(&salt, 0),
        now,
    );
    let proof = incomplete.resolver.zones()[1]
        .sign_records(vec![Record::new(
            n(&format!("{}.example.", base32hex(&lower))),
            T::NSEC3,
            300,
            rdata,
        )])
        .unwrap();
    incomplete.resolver.tamper(move |query, reply| {
        if query == &self::query("missing.example.", T::A) {
            retain_other_types(&mut reply.authority, T::NSEC);
            retain_other_types(&mut reply.authority, T::NSEC3);
            reply.authority.extend(proof.clone());
        }
    });
    cases.push(incomplete);
    cases.extend(mixed_ds_cases(now));
    cases
}

fn retain_other_types(records: &mut Vec<Record>, rtype: T) {
    records.retain(|record| {
        record.rtype != rtype
            && (record.rtype != T::RRSIG
                || record.rdata.get(..2) != Some(rtype.0.to_be_bytes().as_slice()))
    });
}

fn add_name(names: &mut BTreeSet<Name>, name: Name) {
    let mut current = Some(name);
    while let Some(name) = current {
        current = name.parent();
        names.insert(name);
    }
}

fn candidates(case: &Case) -> BTreeSet<Query> {
    let mut names = BTreeSet::new();
    add_name(&mut names, case.name.clone());
    for zone in case.resolver.zones() {
        add_name(&mut names, zone.apex.clone());
        for record in &zone.records {
            add_name(&mut names, record.owner.clone());
            if matches!(record.rtype, T::CNAME | T::DNAME) {
                add_name(&mut names, Name::from_wire(&record.rdata).unwrap());
            }
        }
        for child in zone.delegations.keys() {
            add_name(&mut names, child.clone());
        }
    }
    names
        .into_iter()
        .flat_map(|name| {
            [T::A, T::AAAA, T::MX, T::DS, T::DNSKEY, case.rtype]
                .into_iter()
                .map(move |rtype| Query {
                    name: name.clone(),
                    rtype,
                })
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

fn json_string(value: &str) -> String {
    let mut output = String::from("\"");
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            c if c < ' ' => write!(&mut output, "\\u{:04x}", u32::from(c)).unwrap(),
            c => output.push(c),
        }
    }
    output.push('"');
    output
}

fn answer_fields(answer: &weaver_dnssec::Answer) -> String {
    let outcome = match &answer.outcome {
        weaver_dnssec::Outcome::NoData => "{\"kind\":\"NoData\"}".to_owned(),
        weaver_dnssec::Outcome::NoSuchName => "{\"kind\":\"NoSuchName\"}".to_owned(),
        weaver_dnssec::Outcome::Records { name, rdata } => {
            let mut values: Vec<_> = rdata.iter().map(|value| json_string(&hex(value))).collect();
            values.sort();
            format!(
                "{{\"kind\":\"Records\",\"name\":{},\"rdata\":[{}]}}",
                json_string(&name.to_string()),
                values.join(",")
            )
        }
    };
    format!(",\"ttl\":{},\"outcome\":{outcome}", answer.ttl)
}

fn export(mut case: Case, now: u64) {
    let mut replies = BTreeMap::new();
    for query in candidates(&case) {
        if let Some(bytes) = case.resolver.answer(&query) {
            replies.insert(query, bytes);
        }
    }
    // Signing may use fresh randomness. Freeze bytes before evaluating the
    // local verdict so both validators receive the same signatures.
    for (query, bytes) in &replies {
        case.resolver.set_reply(query.clone(), Some(bytes.clone()));
    }
    let anchors = case.resolver.zones()[0].anchors().unwrap();
    let validator = Validator::new(anchors, Policy::default()).unwrap();
    let (verdict, asked) = drive(
        &validator,
        &case.resolver,
        Question::new(case.name.clone(), case.rtype).unwrap(),
        now,
    );
    for query in asked {
        assert!(replies.contains_key(&query), "fixture omitted {query:?}");
    }
    let (status, extra) = match verdict {
        Verdict::Secure(answer) => ("Secure", answer_fields(&answer)),
        Verdict::Insecure(answer) => ("Insecure", answer_fields(&answer)),
        Verdict::Bogus(failure) => (
            "Bogus",
            format!(
                ",\"reason\":{}",
                json_string(&format!("{:?}", failure.reason))
            ),
        ),
        Verdict::Unanswered(failure) => (
            "Unanswered",
            format!(
                ",\"reason\":{}",
                json_string(&format!("{:?}", failure.cause))
            ),
        ),
    };
    let anchors = case.resolver.zones()[0]
        .ds_records(2)
        .unwrap()
        .iter()
        .map(|ds| json_string(&hex(ds)))
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "{{\"event\":\"case\",\"id\":{},\"name\":{},\"rtype\":{},\"now\":{},\"verdict\":{}{},\"intended\":{},\"anchors\":[{}]}}",
        json_string(case.id),
        json_string(&case.name.to_string()),
        case.rtype.0,
        now,
        json_string(status),
        extra,
        json_string(case.intended),
        anchors,
    );
    for (query, bytes) in replies {
        println!(
            "{{\"event\":\"reply\",\"case\":{},\"name\":{},\"rtype\":{},\"hex\":{}}}",
            json_string(case.id),
            json_string(&query.name.to_string()),
            query.rtype.0,
            json_string(&hex(&bytes)),
        );
    }
}

fn main() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let timestamp = u32::try_from(now).unwrap();
    for case in cases(timestamp) {
        export(case, now);
    }
}
