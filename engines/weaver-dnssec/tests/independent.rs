// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Public-API checks whose wire messages and signing input are independent of
//! both the validator and the synthetic-zone equipment.

use std::collections::BTreeMap;

use aws_lc_rs::{
    digest,
    signature::{Ed25519KeyPair, KeyPair},
};
use weaver_dnssec::{
    BogusReason, Name, Policy, Query, Question, RecordType, Step, TrustAnchor, Validator, Verdict,
};

const NOW: u64 = 1_500_000;
const ROOT: &[u8] = b"\0";
const CHILD: &[u8] = b"\x07example\0";
const HOST: &[u8] = b"\x01a\x07example\0";
const ADDRESS: &[u8] = &[192, 0, 2, 19];

struct Key {
    pair: Ed25519KeyPair,
    dnskey: Vec<u8>,
    tag: u16,
}

impl Key {
    fn new() -> Self {
        let pair = Ed25519KeyPair::generate().unwrap();
        let mut dnskey = vec![1, 1, 3, 15];
        dnskey.extend(pair.public_key().as_ref());
        let mut sum = 0_u32;
        for chunk in dnskey.chunks(2) {
            sum += (u32::from(chunk[0]) << 8) + u32::from(*chunk.get(1).unwrap_or(&0));
        }
        let tag = (sum + (sum >> 16)) as u16;
        Self { pair, dnskey, tag }
    }

    fn ds(&self, owner: &[u8], digest_type: u8) -> Vec<u8> {
        let algorithm = match digest_type {
            1 => &digest::SHA1_FOR_LEGACY_USE_ONLY,
            2 => &digest::SHA256,
            _ => panic!("test digest type"),
        };
        let mut input = owner.to_vec();
        input.extend(&self.dnskey);
        let mut rdata = self.tag.to_be_bytes().to_vec();
        rdata.extend([15, digest_type]);
        rdata.extend(digest::digest(algorithm, &input).as_ref());
        rdata
    }

    fn anchor(&self) -> TrustAnchor {
        TrustAnchor {
            zone: Name::root(),
            key_tag: self.tag,
            algorithm: 15,
            digest_type: 2,
            digest: self.ds(ROOT, 2)[4..].to_vec(),
        }
    }
}

#[derive(Clone)]
struct Record {
    owner: Vec<u8>,
    kind: u16,
    ttl: u32,
    data: Vec<u8>,
}

impl Record {
    fn append(&self, bytes: &mut Vec<u8>) {
        bytes.extend(&self.owner);
        bytes.extend(self.kind.to_be_bytes());
        bytes.extend(1_u16.to_be_bytes());
        bytes.extend(self.ttl.to_be_bytes());
        bytes.extend(u16::try_from(self.data.len()).unwrap().to_be_bytes());
        bytes.extend(&self.data);
    }
}

// RDATA here are opaque A/DNSKEY/DS values or literal canonical NSEC bytes.
// Names and label counts are caller inputs, so no production name/canonical-data
// encoder can manufacture the bytes this signature expects.
fn signed(
    key: &Key,
    owner: &[u8],
    labels: u8,
    signer: &[u8],
    kind: u16,
    mut values: Vec<Vec<u8>>,
    signature_ttl: u32,
) -> Vec<Record> {
    values.sort();
    values.dedup();
    let records: Vec<_> = values
        .into_iter()
        .map(|data| Record {
            owner: owner.to_vec(),
            kind,
            ttl: 300,
            data,
        })
        .collect();
    let mut prefix = kind.to_be_bytes().to_vec();
    prefix.extend([15, labels]);
    prefix.extend(300_u32.to_be_bytes());
    prefix.extend(2_000_000_u32.to_be_bytes());
    prefix.extend(1_000_000_u32.to_be_bytes());
    prefix.extend(key.tag.to_be_bytes());
    prefix.extend(signer);
    let mut input = prefix.clone();
    for record in &records {
        record.append(&mut input);
    }
    prefix.extend(key.pair.sign(&input).as_ref());
    let mut result = records;
    result.push(Record {
        owner: owner.to_vec(),
        kind: 46,
        ttl: signature_ttl,
        data: prefix,
    });
    result
}

fn query(owner: &[u8], kind: u16) -> Query {
    Query {
        name: Name::from_wire(owner).unwrap(),
        rtype: RecordType(kind),
    }
}

fn message(query: &Query, answer: &[Record]) -> Vec<u8> {
    let mut bytes = vec![0, 0, 0x81, 0x80, 0, 1];
    bytes.extend(u16::try_from(answer.len()).unwrap().to_be_bytes());
    bytes.extend([0, 0, 0, 0]);
    bytes.extend(query.name.as_wire());
    bytes.extend(query.rtype.0.to_be_bytes());
    bytes.extend([0, 1]);
    for record in answer {
        record.append(&mut bytes);
    }
    bytes
}

fn authority_message(query: &Query, records: &[Record]) -> Vec<u8> {
    let mut bytes = message(query, records);
    bytes[6..8].copy_from_slice(&0_u16.to_be_bytes());
    bytes[8..10].copy_from_slice(&u16::try_from(records.len()).unwrap().to_be_bytes());
    bytes
}

fn host_anchor(key: &Key) -> TrustAnchor {
    TrustAnchor {
        zone: Name::from_wire(HOST).unwrap(),
        key_tag: key.tag,
        algorithm: 15,
        digest_type: 2,
        digest: key.ds(HOST, 2)[4..].to_vec(),
    }
}

struct Fixture {
    root: Key,
    child: Key,
    replies: BTreeMap<Query, Vec<u8>>,
}

impl Fixture {
    fn new() -> Self {
        let mut fixture = Self {
            root: Key::new(),
            child: Key::new(),
            replies: BTreeMap::new(),
        };
        fixture.put(
            ROOT,
            48,
            signed(
                &fixture.root,
                ROOT,
                0,
                ROOT,
                48,
                vec![fixture.root.dnskey.clone()],
                300,
            ),
        );
        fixture.put(
            CHILD,
            43,
            signed(
                &fixture.root,
                CHILD,
                1,
                ROOT,
                43,
                vec![fixture.child.ds(CHILD, 2)],
                300,
            ),
        );
        fixture.put(
            CHILD,
            48,
            signed(
                &fixture.child,
                CHILD,
                1,
                CHILD,
                48,
                vec![fixture.child.dnskey.clone()],
                300,
            ),
        );
        fixture.put(
            HOST,
            1,
            signed(
                &fixture.child,
                HOST,
                2,
                CHILD,
                1,
                vec![ADDRESS.to_vec()],
                300,
            ),
        );
        fixture
    }

    fn put(&mut self, owner: &[u8], kind: u16, records: Vec<Record>) {
        let query = query(owner, kind);
        self.replies
            .insert(query.clone(), message(&query, &records));
    }

    fn validator(&self, policy: Policy) -> Validator {
        Validator::new(vec![self.root.anchor()], policy).unwrap()
    }

    fn run(&self, validator: &Validator, now: u64) -> (Verdict, Vec<Query>) {
        self.run_name(validator, now, HOST)
    }

    fn run_name(&self, validator: &Validator, now: u64, owner: &[u8]) -> (Verdict, Vec<Query>) {
        let mut validation = validator.begin(
            Question::new(Name::from_wire(owner).unwrap(), RecordType::A).unwrap(),
            now,
        );
        let mut asked = Vec::new();
        loop {
            match validation.step() {
                Step::Done(verdict) => return (verdict, asked),
                Step::Need(query) => {
                    let bytes = self
                        .replies
                        .get(&query)
                        .unwrap_or_else(|| panic!("unexpected query {query:?}"));
                    asked.push(query.clone());
                    validation.supply(&query, bytes).unwrap();
                }
            }
        }
    }
}

fn secure_ttl(verdict: Verdict) -> u32 {
    let Verdict::Secure(answer) = verdict else {
        panic!("expected secure answer, got {verdict:?}")
    };
    assert_eq!(
        answer.addresses(),
        vec![std::net::IpAddr::V4([192, 0, 2, 19].into())]
    );
    answer.ttl
}

#[test]
fn received_signature_ttl_bounds_positive_answer() {
    let mut fixture = Fixture::new();
    fixture.put(
        HOST,
        1,
        signed(&fixture.child, HOST, 2, CHILD, 1, vec![ADDRESS.to_vec()], 7),
    );
    assert_eq!(
        secure_ttl(fixture.run(&fixture.validator(Policy::default()), NOW).0),
        7
    );
}

#[test]
fn invalid_signature_does_not_lower_the_valid_signatures_ttl() {
    let mut fixture = Fixture::new();
    let mut records = signed(
        &fixture.child,
        HOST,
        2,
        CHILD,
        1,
        vec![ADDRESS.to_vec()],
        300,
    );
    let mut invalid = records.last().unwrap().clone();
    invalid.ttl = 7;
    *invalid.data.last_mut().unwrap() ^= 1;
    records.insert(records.len() - 1, invalid);
    fixture.put(HOST, 1, records);
    assert_eq!(
        secure_ttl(fixture.run(&fixture.validator(Policy::default()), NOW).0),
        300
    );
}

#[test]
fn zero_ttl_signature_authenticates_but_never_caches_the_child_keys() {
    let mut fixture = Fixture::new();
    fixture.put(
        CHILD,
        48,
        signed(
            &fixture.child,
            CHILD,
            1,
            CHILD,
            48,
            vec![fixture.child.dnskey.clone()],
            0,
        ),
    );
    let validator = fixture.validator(Policy::default());
    assert_eq!(secure_ttl(fixture.run(&validator, NOW).0), 300);
    let (verdict, asked) = fixture.run(&validator, NOW);
    assert_eq!(secure_ttl(verdict), 300);
    assert_eq!(asked, [query(HOST, 1), query(CHILD, 48)]);
}

#[test]
fn received_dnskey_signature_ttl_expires_trust_at_the_boundary() {
    let mut fixture = Fixture::new();
    fixture.put(
        CHILD,
        48,
        signed(
            &fixture.child,
            CHILD,
            1,
            CHILD,
            48,
            vec![fixture.child.dnskey.clone()],
            7,
        ),
    );
    let validator = fixture.validator(Policy::default());
    secure_ttl(fixture.run(&validator, NOW).0);
    let (warm, asked) = fixture.run(&validator, NOW + 6);
    secure_ttl(warm);
    assert_eq!(asked, [query(HOST, 1)]);
    let (expired, asked) = fixture.run(&validator, NOW + 7);
    secure_ttl(expired);
    assert_eq!(asked, [query(HOST, 1), query(CHILD, 48)]);
}

#[test]
fn received_ds_signature_ttl_expires_both_delegation_and_child_keys() {
    let mut fixture = Fixture::new();
    fixture.put(
        CHILD,
        43,
        signed(
            &fixture.root,
            CHILD,
            1,
            ROOT,
            43,
            vec![fixture.child.ds(CHILD, 2)],
            7,
        ),
    );
    let validator = fixture.validator(Policy::default());
    secure_ttl(fixture.run(&validator, NOW).0);
    let (expired, asked) = fixture.run(&validator, NOW + 7);
    secure_ttl(expired);
    assert_eq!(asked, [query(HOST, 1), query(CHILD, 43), query(CHILD, 48)]);
}

#[test]
fn sha1_ds_policy_changes_actual_chain_validation() {
    let mut fixture = Fixture::new();
    fixture.put(
        CHILD,
        43,
        signed(
            &fixture.root,
            CHILD,
            1,
            ROOT,
            43,
            vec![fixture.child.ds(CHILD, 1)],
            300,
        ),
    );
    let (default, asked) = fixture.run(&fixture.validator(Policy::default()), NOW);
    assert_eq!(secure_ttl(default), 300);
    assert!(asked.contains(&query(CHILD, 48)));
    let policy = Policy {
        sha1_ds_digests: false,
        ..Policy::default()
    };
    let (disabled, asked) = fixture.run(&fixture.validator(policy), NOW);
    assert!(matches!(disabled, Verdict::Insecure(_)));
    assert!(!asked.contains(&query(CHILD, 48)));
}

#[test]
fn matching_sha1_ds_cannot_rescue_a_mismatching_supported_sha256_ds() {
    let mut fixture = Fixture::new();
    let weak = fixture.child.ds(CHILD, 1);
    let mut strong = fixture.child.ds(CHILD, 2);
    strong[4] ^= 1;
    fixture.put(
        CHILD,
        43,
        signed(&fixture.root, CHILD, 1, ROOT, 43, vec![weak, strong], 300),
    );
    let policy = Policy {
        sha1_ds_digests: true,
        ..Policy::default()
    };
    let (verdict, asked) = fixture.run(&fixture.validator(policy), NOW);
    assert!(
        matches!(&verdict, Verdict::Bogus(bogus) if bogus.reason == BogusReason::KeysNotProven),
        "{verdict:?}"
    );
    assert!(asked.contains(&query(CHILD, 48)));
}

#[test]
fn unsupported_ds_does_not_displace_a_supported_sha1_ds() {
    let mut fixture = Fixture::new();
    let mut unsupported = fixture.child.ds(CHILD, 2);
    unsupported[2] = 253;
    fixture.put(
        CHILD,
        43,
        signed(
            &fixture.root,
            CHILD,
            1,
            ROOT,
            43,
            vec![fixture.child.ds(CHILD, 1), unsupported],
            300,
        ),
    );
    let policy = Policy {
        sha1_ds_digests: true,
        ..Policy::default()
    };
    assert_eq!(
        secure_ttl(fixture.run(&fixture.validator(policy), NOW).0),
        300
    );
}

#[test]
fn actual_two_key_rollover_accepts_either_trust_anchor() {
    let mut fixture = Fixture::new();
    let successor = Key::new();
    // Both keys are present, but only the successor signs the DNSKEY RRset.
    fixture.put(
        ROOT,
        48,
        signed(
            &successor,
            ROOT,
            0,
            ROOT,
            48,
            vec![fixture.root.dnskey.clone(), successor.dnskey.clone()],
            300,
        ),
    );
    for anchors in [
        vec![fixture.root.anchor(), successor.anchor()],
        vec![successor.anchor(), fixture.root.anchor()],
    ] {
        let validator = Validator::new(anchors, Policy::default()).unwrap();
        assert_eq!(secure_ttl(fixture.run(&validator, NOW).0), 300);
    }
    // The old anchor alone cannot authenticate a set signed only by its successor.
    let (verdict, _) = fixture.run(&fixture.validator(Policy::default()), NOW);
    assert!(matches!(verdict, Verdict::Bogus(bogus) if bogus.reason == BogusReason::KeysNotProven));
    // A set holding no key the anchor names is a distinct anchor mismatch.
    fixture.put(
        ROOT,
        48,
        signed(
            &successor,
            ROOT,
            0,
            ROOT,
            48,
            vec![successor.dnskey.clone()],
            300,
        ),
    );
    let (verdict, _) = fixture.run(&fixture.validator(Policy::default()), NOW);
    assert!(
        matches!(&verdict, Verdict::Bogus(bogus) if bogus.reason == BogusReason::TrustAnchorMismatch),
        "{verdict:?}"
    );
}

#[test]
fn cached_ancestor_keys_cannot_override_the_closest_configured_anchor() {
    let mut fixture = Fixture::new();
    fixture.put(
        ROOT,
        1,
        signed(&fixture.root, ROOT, 0, ROOT, 1, vec![ADDRESS.to_vec()], 300),
    );
    let mut stale_ds = fixture.child.ds(CHILD, 2);
    stale_ds[4] ^= 1;
    fixture.put(
        CHILD,
        43,
        signed(&fixture.root, CHILD, 1, ROOT, 43, vec![stale_ds], 300),
    );
    let child_anchor = TrustAnchor {
        zone: Name::from_wire(CHILD).unwrap(),
        key_tag: fixture.child.tag,
        algorithm: 15,
        digest_type: 2,
        digest: fixture.child.ds(CHILD, 2)[4..].to_vec(),
    };
    let anchors = vec![fixture.root.anchor(), child_anchor];
    let cold = Validator::new(anchors.clone(), Policy::default()).unwrap();
    let (verdict, asked) = fixture.run(&cold, NOW);
    assert_eq!(secure_ttl(verdict), 300);
    assert_eq!(asked, [query(HOST, 1), query(CHILD, 48)]);

    let warm = Validator::new(anchors, Policy::default()).unwrap();
    assert_eq!(secure_ttl(fixture.run_name(&warm, NOW, ROOT).0), 300);
    let (verdict, asked) = fixture.run(&warm, NOW);
    assert_eq!(secure_ttl(verdict), 300);
    assert_eq!(asked, [query(HOST, 1), query(CHILD, 48)]);
}

#[test]
fn cached_ancestor_insecurity_cannot_override_the_closest_configured_anchor() {
    for insecure_cut in [false, true] {
        let mut fixture = Fixture::new();
        let island = Key::new();
        fixture.put(
            CHILD,
            1,
            signed(
                &fixture.child,
                CHILD,
                1,
                CHILD,
                1,
                vec![ADDRESS.to_vec()],
                300,
            ),
        );
        fixture.put(
            HOST,
            48,
            signed(&island, HOST, 2, HOST, 48, vec![island.dnskey.clone()], 300),
        );
        fixture.put(
            HOST,
            1,
            signed(&island, HOST, 2, HOST, 1, vec![ADDRESS.to_vec()], 300),
        );
        if insecure_cut {
            // Parent-side NSEC at the delegation: NS present, SOA/DS absent.
            let mut nsec = b"\x01z\0".to_vec();
            nsec.extend([0, 6, 0x20, 0, 0, 0, 0, 3]);
            let records = signed(&fixture.root, CHILD, 1, ROOT, 47, vec![nsec], 300);
            let ds_query = query(CHILD, 43);
            fixture
                .replies
                .insert(ds_query.clone(), authority_message(&ds_query, &records));
        } else {
            let mut unsupported = fixture.child.ds(CHILD, 2);
            unsupported[2] = 253;
            fixture.put(
                CHILD,
                43,
                signed(&fixture.root, CHILD, 1, ROOT, 43, vec![unsupported], 300),
            );
        }
        let validator = Validator::new(
            vec![fixture.root.anchor(), host_anchor(&island)],
            Policy::default(),
        )
        .unwrap();
        assert!(matches!(
            fixture.run_name(&validator, NOW, CHILD).0,
            Verdict::Insecure(_)
        ));
        let (verdict, asked) = fixture.run(&validator, NOW);
        assert_eq!(secure_ttl(verdict), 300);
        assert_eq!(asked, [query(HOST, 1), query(HOST, 48)]);
    }
}

#[test]
fn ancestor_signature_cannot_bypass_a_more_specific_anchor() {
    let fixture = Fixture::new();
    let island = Key::new();
    let validator = Validator::new(
        vec![fixture.root.anchor(), host_anchor(&island)],
        Policy::default(),
    )
    .unwrap();
    let (verdict, asked) = fixture.run(&validator, NOW);
    assert!(
        matches!(verdict, Verdict::Bogus(bogus) if bogus.reason == BogusReason::SignerMismatch)
    );
    assert_eq!(asked, [query(HOST, 1)]);
}

#[test]
fn ancestor_denial_cannot_bypass_a_more_specific_anchor() {
    let mut fixture = Fixture::new();
    let island = Key::new();
    fixture.put(
        HOST,
        48,
        signed(&island, HOST, 2, HOST, 48, vec![island.dnskey.clone()], 300),
    );
    // A valid ancestor-signed NSEC claims no A at the anchored name.
    let mut nsec = b"\x01z\x07example\0".to_vec();
    nsec.extend([0, 6, 0, 0, 0, 0, 0, 3]);
    let records = signed(&fixture.child, HOST, 2, CHILD, 47, vec![nsec], 300);
    let main = query(HOST, 1);
    fixture
        .replies
        .insert(main.clone(), authority_message(&main, &records));
    let validator = Validator::new(
        vec![fixture.root.anchor(), host_anchor(&island)],
        Policy::default(),
    )
    .unwrap();
    let (verdict, asked) = fixture.run(&validator, NOW);
    assert!(
        matches!(&verdict, Verdict::Bogus(bogus) if bogus.reason == BogusReason::DenialMissing),
        "{verdict:?}"
    );
    assert_eq!(asked, [main, query(HOST, 48)]);
}

#[test]
fn malformed_replies_at_every_chain_stage_fail_closed_and_do_not_poison_retry() {
    for target in [
        query(HOST, 1),
        query(ROOT, 48),
        query(CHILD, 43),
        query(CHILD, 48),
    ] {
        for asserted_ad in [false, true] {
            for shape in [
                "empty",
                "short_header",
                "query_packet",
                "wrong_opcode",
                "no_question",
                "two_questions",
                "question_loop",
                "wrong_type",
                "wrong_class",
                "missing_record",
                "trailing_byte",
                "short_rdata",
                "oversized_packet",
            ] {
                let mut fixture = Fixture::new();
                let original = fixture.replies[&target].clone();
                let mut malformed = original.clone();
                if asserted_ad {
                    malformed[3] |= 0x20;
                }
                let question_end = 12 + target.name.as_wire().len();
                match shape {
                    "empty" => malformed.clear(),
                    "short_header" => malformed.truncate(11),
                    "query_packet" => malformed[2] &= !0x80,
                    "wrong_opcode" => malformed[2] |= 0x08,
                    "no_question" => malformed[5] = 0,
                    "two_questions" => malformed[5] = 2,
                    "question_loop" => malformed[12..14].copy_from_slice(&[0xc0, 12]),
                    "wrong_type" => malformed[question_end + 1] ^= 1,
                    "wrong_class" => malformed[question_end + 3] = 3,
                    "missing_record" => malformed[6..8].copy_from_slice(&u16::MAX.to_be_bytes()),
                    "trailing_byte" => malformed.push(0),
                    "short_rdata" => {
                        malformed.pop();
                    }
                    "oversized_packet" => malformed.resize(65_536, 0),
                    _ => unreachable!(),
                }
                fixture.replies.insert(target.clone(), malformed);
                let validator = fixture.validator(Policy::default());
                let (verdict, asked) = fixture.run(&validator, NOW);
                assert!(
                    matches!(verdict, Verdict::Bogus(ref failure)
                        if failure.reason == BogusReason::MalformedReply
                            && failure.name == target.name
                            && failure.rtype == target.rtype),
                    "{target:?}, {shape}, AD={asserted_ad}: {verdict:?}"
                );
                assert_eq!(asked.last(), Some(&target));
                fixture.replies.insert(target.clone(), original);
                let (verdict, retried) = fixture.run(&validator, NOW);
                assert_eq!(secure_ttl(verdict), 300);
                assert!(
                    retried.contains(&target),
                    "malformed reply must not be cached"
                );
            }
        }
    }
}
