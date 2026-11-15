// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn name(text: &str) -> Name {
    Name::from_text(text).unwrap()
}

fn bitmap(types: &[u16]) -> TypeBitmap {
    let mut windows = BTreeMap::<u8, Vec<u8>>::new();
    for &rtype in types {
        let [window, low] = rtype.to_be_bytes();
        let bytes = windows.entry(window).or_default();
        bytes.resize(bytes.len().max(usize::from(low / 8) + 1), 0);
        bytes[usize::from(low / 8)] |= 0x80 >> (low % 8);
    }
    let bytes: Vec<_> = windows
        .into_iter()
        .flat_map(|(window, bytes)| [vec![window, bytes.len() as u8], bytes].concat())
        .collect();
    TypeBitmap::parse(&bytes).unwrap()
}

fn nsec(owner: &str, next: &str, types: &[u16]) -> VerifiedDenial {
    VerifiedDenial {
        owner: name(owner),
        kind: RdataKind::Nsec(Nsec {
            next: name(next),
            types: bitmap(types),
        }),
        ttl_bound: 3600,
    }
}

fn nsec3(owner: &str, next: &str, types: &[u16]) -> VerifiedDenial {
    VerifiedDenial {
        owner: name(&format!("{owner}.example.")),
        kind: RdataKind::Nsec3(Nsec3 {
            hash_algorithm: 1,
            flags: 1,
            iterations: 12,
            salt: vec![0xaa, 0xbb, 0xcc, 0xdd],
            next_hashed_owner: decode_base32hex(next.as_bytes()).unwrap(),
            types: bitmap(types),
        }),
        ttl_bound: 3600,
    }
}

// Literal RFC 4035 Appendix B NSEC owner, next name and bitmap values:
// rfc4035.txt B.2 lines 2387,2395; B.3 line 2443; B.5 line 2511;
// B.6 line 2563; B.7 lines 2627,2643; B.8 line 2683.
fn apex() -> VerifiedDenial {
    nsec("example.", "a.example.", &[2, 6, 15, 46, 47, 48])
}
fn unsigned_cut() -> VerifiedDenial {
    nsec("b.example.", "ns1.example.", &[2, 46, 47])
}
fn ns1() -> VerifiedDenial {
    nsec("ns1.example.", "ns2.example.", &[1, 46, 47])
}
fn wildcard_cover() -> VerifiedDenial {
    nsec("x.y.w.example.", "xx.example.", &[15, 46, 47])
}
fn wildcard_nsec() -> VerifiedDenial {
    nsec("*.w.example.", "x.w.example.", &[15, 46, 47])
}

// Literal RFC 5155 Appendix B data (no signatures are implied by this helper):
// B.1 lines 2229..2231,2250..2251,2261..2262; B.2 2327..2328;
// B.2.1 2383..2384; B.3 2435..2436,2446..2448; B.4 2507..2508;
// B.5 2586..2587,2597..2598,2608..2609; B.6 2663..2665.
fn apex3() -> VerifiedDenial {
    nsec3(
        "0p9mhaveqvm6t7vbl5lop2u3t2rp3tom",
        "2t7b4g4vsa5smi47k61mv5bv1a22bojr",
        &[15, 48, 2, 6, 51, 46],
    )
}
fn ce3() -> VerifiedDenial {
    nsec3(
        "b4um86eghhds6nea196smvmlo4ors995",
        "gjeqe526plbf1g8mklp59enfd789njgi",
        &[15, 46],
    )
}
fn cut3() -> VerifiedDenial {
    nsec3(
        "35mthgpgcu1qg68fab165klnsnk3dpvl",
        "b4um86eghhds6nea196smvmlo4ors995",
        &[2, 43, 46],
    )
}
fn ns13() -> VerifiedDenial {
    nsec3(
        "2t7b4g4vsa5smi47k61mv5bv1a22bojr",
        "2vptu5timamqttgl4luu9kg21e0aor3s",
        &[1, 46],
    )
}
fn ent3() -> VerifiedDenial {
    nsec3(
        "ji6neoaepv8b5o6k4ev33abha8ht9fgc",
        "k8udemvp1j2f7eg6jebps17vp3n8i58h",
        &[],
    )
}
fn w3() -> VerifiedDenial {
    nsec3(
        "k8udemvp1j2f7eg6jebps17vp3n8i58h",
        "kohar7mbb8dc2ce8a9qvl8hon4k53uhi",
        &[],
    )
}
fn wildcard_cover3() -> VerifiedDenial {
    nsec3(
        "q04jkcevqvmu85r014c7dkba38o0ji5r",
        "r53bq7cc2uvmubfu5ocmm6pers9tk9en",
        &[1, 46],
    )
}
fn wildcard3() -> VerifiedDenial {
    nsec3(
        "r53bq7cc2uvmubfu5ocmm6pers9tk9en",
        "t644ebqk9bibcna874givr6joj62mlhv",
        &[15, 46],
    )
}

fn mutate_nsec(record: &mut VerifiedDenial) -> &mut Nsec {
    match &mut record.kind {
        RdataKind::Nsec(nsec) => nsec,
        _ => unreachable!(),
    }
}
fn mutate_nsec3(record: &mut VerifiedDenial) -> &mut Nsec3 {
    match &mut record.kind {
        RdataKind::Nsec3(nsec) => nsec,
        _ => unreachable!(),
    }
}
fn clear_optout(records: &mut [VerifiedDenial]) {
    for record in records {
        mutate_nsec3(record).flags = 0;
    }
}

fn nodata(records: &[VerifiedDenial], q: &str, rtype: RecordType) -> ProofResult {
    no_data(
        records,
        &name("example."),
        &name(q),
        rtype,
        &mut DenialContext::default(),
        &mut WorkBudget::default(),
    )
    .unwrap()
}
fn nx(records: &[VerifiedDenial], q: &str) -> ProofResult {
    name_error(
        records,
        &name("example."),
        &name(q),
        &mut DenialContext::default(),
        &mut WorkBudget::default(),
    )
    .unwrap()
}
fn wild(records: &[VerifiedDenial], q: &str, ce: &str) -> ProofResult {
    wildcard(
        records,
        &name("example."),
        &name(q),
        &name(ce),
        &mut DenialContext::default(),
        &mut WorkBudget::default(),
    )
    .unwrap()
}
fn status(records: &[VerifiedDenial], q: &str) -> Result<StatusResult, BogusReason> {
    delegation_status(
        records,
        &name("example."),
        &name(q),
        &mut DenialContext::default(),
        &mut WorkBudget::default(),
    )
}

#[test]
fn rfc4035_appendix_b_denials() {
    assert_eq!(
        nx(&[unsigned_cut(), apex()], "ml.example.").proof,
        Proof::Proven
    );
    assert_eq!(
        nodata(&[ns1()], "ns1.example.", RecordType::MX).proof,
        Proof::Proven
    );
    assert_eq!(
        wild(&[wildcard_cover()], "a.z.w.example.", "w.example.").proof,
        Proof::Proven
    );
    assert_eq!(
        nodata(
            &[wildcard_cover(), wildcard_nsec()],
            "a.z.w.example.",
            RecordType::AAAA
        )
        .proof,
        Proof::Proven
    );
    assert_eq!(
        status(&[unsigned_cut()], "b.example.").unwrap().status,
        Status::InsecureCut
    );
    assert_eq!(
        status(&[apex()], "example."),
        Err(BogusReason::DelegationUnproven)
    );
}

#[test]
fn rfc5155_appendix_b_preserves_printed_optout_flags() {
    // The handoff calls B.1/B.4/B.5 "Proven", but all printed spans have
    // flag 1. RFC 5155 section 8.7 / handoff 6.8.2 require Insecure here.
    let mut records = vec![apex3(), ce3(), cut3()];
    assert_eq!(nx(&records, "a.c.x.w.example.").proof, Proof::Insecure);
    clear_optout(&mut records);
    assert_eq!(nx(&records, "a.c.x.w.example.").proof, Proof::Proven);
    assert_eq!(
        nodata(&[ns13()], "ns1.example.", RecordType::MX).proof,
        Proof::Proven
    );
    assert_eq!(
        nodata(&[ent3()], "y.w.example.", RecordType::A).proof,
        Proof::Proven
    );
    assert_eq!(
        status(&[cut3(), apex3()], "c.example.").unwrap().status,
        Status::InsecureCut
    );
    assert_eq!(
        wild(&[wildcard_cover3()], "a.z.w.example.", "w.example.").proof,
        Proof::Insecure
    );
    let mut cover = wildcard_cover3();
    mutate_nsec3(&mut cover).flags = 0;
    assert_eq!(
        wild(&[cover], "a.z.w.example.", "w.example.").proof,
        Proof::Proven
    );
    let mut records = vec![w3(), wildcard_cover3(), wildcard3()];
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA).proof,
        Proof::Insecure
    );
    clear_optout(&mut records);
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA).proof,
        Proof::Proven
    );
    assert_eq!(
        status(&[apex3()], "example."),
        Err(BogusReason::DelegationUnproven)
    );
}

#[test]
fn nsec_cover_endpoints_are_strict_and_wrap_is_apex_only() {
    let record = ns1();
    let RdataKind::Nsec(data) = &record.kind else {
        unreachable!()
    };
    let zone = name("example.");
    assert!(nsec_covers(
        &record.owner,
        data,
        &zone,
        &name("ns15.example.")
    ));
    assert!(!nsec_covers(&record.owner, data, &zone, &record.owner));
    assert!(!nsec_covers(&record.owner, data, &zone, &data.next));
    // Deliberate endpoint mutations of the literal record above.
    let last = nsec("xx.example.", "example.", &[1, 46, 47]);
    assert_eq!(wild(&[last], "z.example.", "example.").proof, Proof::Proven);
    let not_apex = nsec("xx.example.", "a.example.", &[1, 46, 47]);
    assert_eq!(
        wild(&[not_apex], "z.example.", "example.").proof,
        Proof::NotProven
    );
    let single = nsec("example.", "example.", &[2, 6, 46, 47]);
    assert_eq!(
        nx(std::slice::from_ref(&single), "ml.example.").proof,
        Proof::Proven
    );
    assert_eq!(nx(&[single], "example.").proof, Proof::NotProven);
}

#[test]
fn nsec_name_error_requires_both_parts_and_excludes_empty_nonterminals() {
    assert_eq!(
        nx(&[unsigned_cut(), apex()], "ml.example.").proof,
        Proof::Proven
    );
    assert_eq!(nx(&[unsigned_cut()], "ml.example.").proof, Proof::NotProven);
    let ent = nsec("x.w.example.", "x.y.w.example.", &[15, 46, 47]);
    assert_eq!(
        nodata(std::slice::from_ref(&ent), "y.w.example.", RecordType::A).proof,
        Proof::Proven
    );
    assert_eq!(nx(&[ent], "y.w.example.").proof, Proof::NotProven);
}

#[test]
fn nsec_delegation_and_dname_cannot_deny_descendants() {
    let unsigned = unsigned_cut();
    assert_eq!(
        nx(std::slice::from_ref(&unsigned), "mc.b.example.").proof,
        Proof::Insecure
    );
    assert_eq!(
        nodata(
            std::slice::from_ref(&unsigned),
            "mc.b.example.",
            RecordType::A
        )
        .proof,
        Proof::Insecure
    );
    assert_eq!(
        nodata(&[unsigned], "b.example.", RecordType::A).proof,
        Proof::NotProven
    );
    let signed = nsec("b.example.", "ns1.example.", &[2, 43, 46, 47]);
    assert_eq!(
        nx(std::slice::from_ref(&signed), "mc.b.example.").proof,
        Proof::NotProven
    );
    assert_eq!(
        nodata(&[signed], "mc.b.example.", RecordType::A).proof,
        Proof::NotProven
    );
    let dname = nsec("b.example.", "ns1.example.", &[39, 46, 47]);
    assert_eq!(
        wild(std::slice::from_ref(&dname), "mc.b.example.", "b.example.").proof,
        Proof::NotProven
    );
    assert_eq!(
        nodata(std::slice::from_ref(&dname), "mc.b.example.", RecordType::A).proof,
        Proof::NotProven
    );
    assert_eq!(
        nodata(&[dname], "b.example.", RecordType::A).proof,
        Proof::Proven
    );
}

#[test]
fn nsec_exact_nodata_requires_absent_type_and_cname() {
    assert_eq!(
        nodata(&[ns1()], "ns1.example.", RecordType::MX).proof,
        Proof::Proven
    );
    assert_eq!(
        nodata(&[ns1()], "ns1.example.", RecordType::A).proof,
        Proof::NotProven
    );
    let mut record = ns1();
    mutate_nsec(&mut record).types = bitmap(&[5, 46, 47]);
    assert_eq!(
        nodata(&[record], "ns1.example.", RecordType::MX).proof,
        Proof::NotProven
    );
}

#[test]
fn nsec_wildcard_requires_exact_closest_encloser_and_usable_wildcard() {
    let mut records = vec![wildcard_cover(), wildcard_nsec()];
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA).proof,
        Proof::Proven
    );
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::MX).proof,
        Proof::NotProven
    );
    records[1].owner = name("*.example.");
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA).proof,
        Proof::NotProven
    );
    records[1] = wildcard_nsec();
    mutate_nsec(&mut records[1]).types = bitmap(&[5, 46, 47]);
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA).proof,
        Proof::NotProven
    );
    mutate_nsec(&mut records[1]).types = bitmap(&[2, 46, 47]);
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA).proof,
        Proof::NotProven
    );
    assert_eq!(
        wild(&[wildcard_cover()], "a.z.w.example.", "w.example.").proof,
        Proof::Proven
    );
    assert_eq!(
        wild(&[wildcard_cover()], "a.z.w.example.", "example.").proof,
        Proof::NotProven
    );
    // A *.b.example cover cannot validate expansion from *.example.
    let attack = nsec("*.b.example.", "ns1.example.", &[15, 46, 47]);
    assert_eq!(
        wild(std::slice::from_ref(&attack), "a.b.example.", "b.example.").proof,
        Proof::Proven
    );
    assert_eq!(
        wild(&[attack], "a.b.example.", "example.").proof,
        Proof::NotProven
    );
}

#[test]
fn unrelated_owners_names_and_next_names_never_prove() {
    let mut unrelated = ns1();
    unrelated.owner = name("ns1.other.");
    assert_eq!(
        nodata(&[unrelated], "ns1.other.", RecordType::MX).proof,
        Proof::NotProven
    );
    let mut unrelated = ns1();
    mutate_nsec(&mut unrelated).next = name("ns2.other.");
    assert_eq!(
        nodata(&[unrelated], "ns1.example.", RecordType::MX).proof,
        Proof::NotProven
    );
    let mut unrelated = ns13();
    unrelated.owner = name("2t7b4g4vsa5smi47k61mv5bv1a22bojr.other.");
    assert_eq!(
        nodata(&[unrelated], "ns1.example.", RecordType::MX).proof,
        Proof::NotProven
    );
    assert_eq!(
        status(&[apex()], "other."),
        Err(BogusReason::DelegationUnproven)
    );
}

#[test]
fn nsec3_preparation_partitions_parameters_and_ignores_bad_shapes() {
    assert_eq!(
        nodata(&[ns13()], "ns1.example.", RecordType::MX).proof,
        Proof::Proven
    );
    for mutation in 0..7 {
        let mut record = ns13();
        match mutation {
            0 => mutate_nsec3(&mut record).flags = 2,
            1 => mutate_nsec3(&mut record).hash_algorithm = 2,
            2 => record.owner = name("2t7b4g4vsa5smi47k61mv5bv1a22boj.example."),
            3 => record.owner = name("zt7b4g4vsa5smi47k61mv5bv1a22bojr.example."),
            4 => {
                mutate_nsec3(&mut record).next_hashed_owner.pop();
            }
            5 => record.owner = name("a.2t7b4g4vsa5smi47k61mv5bv1a22bojr.example."),
            _ => record.owner = name("2t7b4g4vsa5smi47k61mv5bv1a22bojr.other."),
        }
        assert_eq!(
            nodata(&[record], "ns1.example.", RecordType::MX).proof,
            Proof::NotProven,
            "mutation {mutation}"
        );
    }
    let mut other = apex3();
    mutate_nsec3(&mut other).salt.push(0);
    assert_eq!(
        nodata(&[ns13(), other], "ns1.example.", RecordType::MX).proof,
        Proof::Proven
    );
    let mut other = apex3();
    mutate_nsec3(&mut other).iterations = 13;
    assert_eq!(
        nodata(&[ns13(), other], "ns1.example.", RecordType::MX).proof,
        Proof::Proven
    );
    let mut ignored = apex3();
    mutate_nsec3(&mut ignored).flags = 2;
    mutate_nsec3(&mut ignored).salt.push(0);
    assert_eq!(
        nodata(&[ns13(), ignored], "ns1.example.", RecordType::MX).proof,
        Proof::Proven
    );
}

// Encoding exists only to mutate the iteration count of RFC fixture records
// while preserving exact-match relationships. The published hashes below
// independently test the production hash before these synthetic controls.
fn encode(bytes: &[u8]) -> String {
    let mut value = 0_u16;
    let mut bits = 0;
    let alphabet = b"0123456789ABCDEFGHIJKLMNOPQRSTUV";
    let mut output = String::new();
    for &byte in bytes {
        value = (value << 8) | u16::from(byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            output.push(char::from(alphabet[usize::from((value >> bits) & 31)]));
        }
        value &= (1 << bits) - 1;
    }
    assert_eq!(bits, 0);
    output
}

fn hashed_record(owner: &str, next: &str, types: &[u16], iterations: u16) -> VerifiedDenial {
    let salt = [0xaa, 0xbb, 0xcc, 0xdd];
    let owner_hash = algorithm::nsec3_hash(&name(owner), &salt, iterations);
    let next_hash = algorithm::nsec3_hash(&name(next), &salt, iterations);
    let mut record = nsec3(&encode(&owner_hash), &encode(&next_hash), types);
    mutate_nsec3(&mut record).iterations = iterations;
    mutate_nsec3(&mut record).flags = 0;
    record
}

#[test]
fn nsec3_iteration_limit_is_before_hash_and_150_is_accepted() {
    let mut records = vec![ns13()];
    mutate_nsec3(&mut records[0]).iterations = 151;
    let mut budget = WorkBudget::default();
    let mut ctx = DenialContext::default();
    assert_eq!(
        no_data(
            &records,
            &name("example."),
            &name("ns1.example."),
            RecordType::MX,
            &mut ctx,
            &mut budget
        ),
        Ok(ProofResult {
            proof: Proof::IterationsLimited,
            ttl_bound: 0,
        })
    );
    assert_eq!(budget.nsec3_hashes, 0);
    assert_eq!(
        delegation_status(
            &records,
            &name("example."),
            &name("c.example."),
            &mut ctx,
            &mut budget
        ),
        Err(BogusReason::Nsec3Iterations)
    );
    assert_eq!(budget.nsec3_hashes, 0);
    let accepted = hashed_record("ns1.example.", "ns2.example.", &[1, 46], 150);
    assert_eq!(
        nodata(&[accepted], "ns1.example.", RecordType::MX).proof,
        Proof::Proven
    );
}

#[test]
fn nsec3_closest_encloser_includes_apex_but_requires_next_closer_cover() {
    let mut records = vec![apex3(), cut3()];
    clear_optout(&mut records);
    assert_eq!(
        status(&records, "c.example.").unwrap().status,
        Status::NotACut
    );
    assert_eq!(
        status(&records[..1], "c.example."),
        Err(BogusReason::DelegationUnproven)
    );
    assert_eq!(
        status(&records[1..], "c.example."),
        Err(BogusReason::DelegationUnproven)
    );
    assert_eq!(
        status(&[], "c.example."),
        Err(BogusReason::DelegationUnproven)
    );
}

#[test]
fn nsec3_delegation_and_dname_closest_encloser_rules() {
    // RFC B.1's matching x.w.example record is deliberately changed to a
    // delegation or DNAME. The interval and matching hash stay published.
    let mut records = vec![apex3(), ce3(), cut3()];
    clear_optout(&mut records);
    assert_eq!(nx(&records, "a.c.x.w.example.").proof, Proof::Proven);
    mutate_nsec3(&mut records[1]).types = bitmap(&[2, 43, 46]);
    assert_eq!(nx(&records, "a.c.x.w.example.").proof, Proof::NotProven);
    assert_eq!(
        status(&records, "a.c.x.w.example."),
        Err(BogusReason::DelegationUnproven)
    );
    mutate_nsec3(&mut records[1]).types = bitmap(&[2, 46]);
    assert_eq!(nx(&records, "a.c.x.w.example.").proof, Proof::Insecure);
    assert_eq!(
        nodata(&records, "a.c.x.w.example.", RecordType::A).proof,
        Proof::Insecure
    );
    assert_eq!(
        status(&records, "a.c.x.w.example.").unwrap().status,
        Status::InsecureCut
    );
    mutate_nsec3(&mut records[1]).types = bitmap(&[39, 46]);
    assert_eq!(nx(&records, "a.c.x.w.example.").proof, Proof::NotProven);
}

#[test]
fn nsec3_exact_and_wildcard_bitmaps_must_deny_the_type_and_cname() {
    assert_eq!(
        nodata(&[ns13()], "ns1.example.", RecordType::A).proof,
        Proof::NotProven
    );
    let mut exact = ns13();
    mutate_nsec3(&mut exact).types = bitmap(&[5, 46]);
    assert_eq!(
        nodata(&[exact.clone()], "ns1.example.", RecordType::MX).proof,
        Proof::NotProven
    );
    mutate_nsec3(&mut exact).types = bitmap(&[2, 46]);
    assert_eq!(
        nodata(&[exact], "ns1.example.", RecordType::MX).proof,
        Proof::NotProven
    );
    let mut records = vec![w3(), wildcard_cover3(), wildcard3()];
    clear_optout(&mut records);
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA).proof,
        Proof::Proven
    );
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::MX).proof,
        Proof::NotProven
    );
    mutate_nsec3(&mut records[2]).types = bitmap(&[5, 46]);
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA).proof,
        Proof::NotProven
    );
    mutate_nsec3(&mut records[2]).types = bitmap(&[2, 46]);
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA).proof,
        Proof::NotProven
    );
    assert_eq!(
        nodata(&records[..2], "a.z.w.example.", RecordType::AAAA).proof,
        Proof::NotProven
    );
}

#[test]
fn nsec3_name_error_needs_wildcard_denial_and_name_must_not_match() {
    let mut records = vec![apex3(), ce3(), cut3()];
    clear_optout(&mut records);
    assert_eq!(nx(&records, "a.c.x.w.example.").proof, Proof::Proven);
    assert_eq!(
        nx(&records[..2], "a.c.x.w.example.").proof,
        Proof::NotProven
    );
    assert_eq!(nx(&records, "x.w.example.").proof, Proof::NotProven);
    assert_eq!(nx(&[ent3()], "y.w.example.").proof, Proof::NotProven);
}

#[test]
fn nsec3_wildcard_checks_exact_next_closer_and_valid_ce() {
    let mut records = vec![wildcard_cover3()];
    clear_optout(&mut records);
    assert_eq!(
        wild(&records, "a.z.w.example.", "w.example.").proof,
        Proof::Proven
    );
    assert_eq!(
        wild(&records, "a.z.w.example.", "example.").proof,
        Proof::NotProven
    );
    assert_eq!(
        wild(&records, "a.z.w.example.", "a.z.w.example.").proof,
        Proof::NotProven
    );
    assert_eq!(
        wild(&records, "a.z.w.example.", "x.w.example.").proof,
        Proof::NotProven
    );
    assert_eq!(wild(&records, "other.", ".").proof, Proof::NotProven);
    assert_eq!(
        wild(&[], "a.z.w.example.", "w.example.").proof,
        Proof::NotProven
    );
}

#[test]
fn nsec3_wrap_and_single_chain_strict_endpoints() {
    let zone = name("example.");
    // Published final record in RFC 5155 Appendix A lines 2142..2143.
    let last = nsec3(
        "t644ebqk9bibcna874givr6joj62mlhv",
        "0p9mhaveqvm6t7vbl5lop2u3t2rp3tom",
        &[1, 13, 28, 46],
    );
    let prepared = Nsec3s::prepare(std::slice::from_ref(&last), &zone);
    let chain = &prepared.chains[0];
    let owner: [u8; 20] = decode_base32hex(b"t644ebqk9bibcna874givr6joj62mlhv")
        .unwrap()
        .try_into()
        .unwrap();
    let next: [u8; 20] = decode_base32hex(b"0p9mhaveqvm6t7vbl5lop2u3t2rp3tom")
        .unwrap()
        .try_into()
        .unwrap();
    assert!(chain.covering_hash(&[0; 20]).is_some());
    assert!(chain.covering_hash(&[255; 20]).is_some());
    assert!(chain.covering_hash(&owner).is_none());
    assert!(chain.covering_hash(&next).is_none());
    let mut single = apex3();
    mutate_nsec3(&mut single).next_hashed_owner =
        decode_base32hex(b"0p9mhaveqvm6t7vbl5lop2u3t2rp3tom").unwrap();
    let prepared = Nsec3s::prepare(std::slice::from_ref(&single), &zone);
    let chain = &prepared.chains[0];
    assert!(chain.covering_hash(&[0; 20]).is_some());
    assert!(chain.covering_hash(&[255; 20]).is_some());
    assert!(chain.covering_hash(&next).is_none());
}

#[test]
fn ds_step_c_exact_bits_root_exception_and_nsec_other_proofs() {
    assert_eq!(
        status(&[ns1()], "ns1.example.").unwrap().status,
        Status::NotACut
    );
    assert_eq!(
        status(&[unsigned_cut()], "b.example.").unwrap().status,
        Status::InsecureCut
    );
    let mut cut = unsigned_cut();
    mutate_nsec(&mut cut).types = bitmap(&[2, 43, 46, 47]);
    assert_eq!(
        status(&[cut], "b.example."),
        Err(BogusReason::DelegationUnproven)
    );
    let mut cut = unsigned_cut();
    mutate_nsec(&mut cut).types = bitmap(&[2, 6, 46, 47]);
    assert_eq!(
        status(&[cut], "b.example."),
        Err(BogusReason::DelegationUnproven)
    );
    assert_eq!(
        status(&[unsigned_cut(), apex()], "ml.example.")
            .unwrap()
            .status,
        Status::NotACut
    );
    let ent = nsec("x.w.example.", "x.y.w.example.", &[15, 46, 47]);
    assert_eq!(
        status(&[ent], "y.w.example.").unwrap().status,
        Status::NotACut
    );
    assert_eq!(
        status(&[wildcard_cover(), wildcard_nsec()], "a.z.w.example.")
            .unwrap()
            .status,
        Status::NotACut
    );
    let root = nsec(".", ".", &[2, 6, 46, 47]);
    assert_eq!(
        delegation_status(
            &[root],
            &Name::root(),
            &Name::root(),
            &mut DenialContext::default(),
            &mut WorkBudget::default()
        )
        .unwrap()
        .status,
        Status::InsecureCut
    );
    let mut record = ns13();
    mutate_nsec3(&mut record).types = bitmap(&[2, 46]);
    assert_eq!(
        status(&[record.clone()], "ns1.example.").unwrap().status,
        Status::InsecureCut
    );
    mutate_nsec3(&mut record).types = bitmap(&[2, 43, 46]);
    assert_eq!(
        status(&[record], "ns1.example."),
        Err(BogusReason::DelegationUnproven)
    );
}

#[test]
fn hashes_are_memoized_by_name_salt_and_iterations_with_shared_budget() {
    let mut context = DenialContext::default();
    let mut budget = WorkBudget::default();
    // Independent published hash: rfc5155.txt Appendix A line 1925.
    let expected = decode_base32hex(b"2t7b4g4vsa5smi47k61mv5bv1a22bojr").unwrap();
    assert_eq!(
        context
            .hash(
                &name("ns1.example."),
                &[0xaa, 0xbb, 0xcc, 0xdd],
                12,
                &mut budget
            )
            .unwrap()
            .as_slice(),
        expected
    );
    assert_eq!(budget.nsec3_hashes, 1);
    context
        .hash(
            &name("NS1.EXAMPLE."),
            &[0xaa, 0xbb, 0xcc, 0xdd],
            12,
            &mut budget,
        )
        .unwrap();
    assert_eq!(budget.nsec3_hashes, 1);
    context
        .hash(&name("ns1.example."), &[], 12, &mut budget)
        .unwrap();
    context
        .hash(&name("ns1.example."), &[], 13, &mut budget)
        .unwrap();
    assert_eq!(budget.nsec3_hashes, 3);
    for index in 3..MAX_NSEC3_HASHES {
        context
            .hash(&name(&format!("n{index}.example.")), &[], 0, &mut budget)
            .unwrap();
    }
    assert_eq!(budget.nsec3_hashes, MAX_NSEC3_HASHES);
    assert_eq!(
        context.hash(&name("one-more.example."), &[], 0, &mut budget),
        Err(BogusReason::WorkLimit)
    );
    assert_eq!(budget.nsec3_hashes, MAX_NSEC3_HASHES);
    // A memo hit is still usable when the crypto budget is exhausted.
    assert_eq!(
        context
            .hash(
                &name("ns1.example."),
                &[0xaa, 0xbb, 0xcc, 0xdd],
                12,
                &mut budget
            )
            .unwrap()
            .as_slice(),
        expected
    );
}

#[test]
fn proof_ttl_uses_only_participating_records_and_limits_before_work() {
    let mut records = vec![unsigned_cut(), apex(), ns1()];
    records[0].ttl_bound = 100;
    records[1].ttl_bound = 50;
    records[2].ttl_bound = 1;
    assert_eq!(nx(&records, "ml.example."), ProofResult::proven(50));
    assert_eq!(
        nodata(&records, "ns1.example.", RecordType::MX),
        ProofResult::proven(1)
    );
    let mut records = vec![w3(), wildcard_cover3(), wildcard3(), ns13()];
    clear_optout(&mut records);
    records[0].ttl_bound = 100;
    records[1].ttl_bound = 50;
    records[2].ttl_bound = 25;
    records[3].ttl_bound = 1;
    assert_eq!(
        nodata(&records, "a.z.w.example.", RecordType::AAAA),
        ProofResult::proven(25)
    );
    let records = vec![ns13(); MAX_DENIAL_RRSETS + 1];
    let mut budget = WorkBudget::default();
    assert_eq!(
        no_data(
            &records,
            &name("example."),
            &name("ns1.example."),
            RecordType::MX,
            &mut DenialContext::default(),
            &mut budget
        ),
        Err(BogusReason::WorkLimit)
    );
    assert_eq!(budget.nsec3_hashes, 0);
}

#[test]
fn ent_proofs_obey_ancestor_boundaries_and_ignore_outside_owners() {
    // Deliberately mutate the Appendix B.6 endpoints into an ENT interval.
    let ent = nsec("b.example.", "x.mc.b.example.", &[15, 46, 47]);
    assert_eq!(
        nodata(&[ent], "mc.b.example.", RecordType::A).proof,
        Proof::Proven
    );
    let delegated = nsec("b.example.", "x.mc.b.example.", &[2, 43, 46, 47]);
    assert_eq!(
        nodata(&[delegated], "mc.b.example.", RecordType::A).proof,
        Proof::NotProven
    );
    let dname = nsec("b.example.", "x.mc.b.example.", &[39, 46, 47]);
    assert_eq!(
        nodata(&[dname], "mc.b.example.", RecordType::A).proof,
        Proof::NotProven
    );
    let outside = nsec("a.", "x.ns1.example.", &[15, 46, 47]);
    assert_eq!(
        nodata(&[outside], "ns1.example.", RecordType::MX).proof,
        Proof::NotProven
    );
    let mut outside = apex3();
    outside.owner = name("0p9mhaveqvm6t7vbl5lop2u3t2rp3tom.other.");
    mutate_nsec3(&mut outside).iterations = 151;
    assert_eq!(
        nodata(&[outside], "ns1.example.", RecordType::MX).proof,
        Proof::NotProven
    );
}

#[test]
fn exhausted_hash_budget_propagates_through_all_proof_entry_points() {
    let mut budget = WorkBudget {
        nsec3_hashes: MAX_NSEC3_HASHES,
        ..WorkBudget::default()
    };
    let mut context = DenialContext::default();
    let records = [ns13()];
    let zone = name("example.");
    let qname = name("ns1.example.");
    assert_eq!(
        no_data(
            &records,
            &zone,
            &qname,
            RecordType::MX,
            &mut context,
            &mut budget
        ),
        Err(BogusReason::WorkLimit)
    );
    assert_eq!(
        name_error(&records, &zone, &qname, &mut context, &mut budget),
        Err(BogusReason::WorkLimit)
    );
    assert_eq!(
        wildcard(&records, &zone, &qname, &zone, &mut context, &mut budget),
        Err(BogusReason::WorkLimit)
    );
    assert_eq!(
        delegation_status(&records, &zone, &qname, &mut context, &mut budget),
        Err(BogusReason::WorkLimit)
    );
    assert_eq!(budget.nsec3_hashes, MAX_NSEC3_HASHES);
}

#[test]
fn rollover_retries_complete_chains_in_either_record_order_with_shared_hashes() {
    for different_iterations in [false, true] {
        // The first candidate has an authenticated apex but no next-closer
        // interval. Only the complete RFC chain can establish this denial.
        let mut incomplete = apex3();
        if different_iterations {
            mutate_nsec3(&mut incomplete).iterations = 0;
        } else {
            mutate_nsec3(&mut incomplete).salt.clear();
        }
        incomplete.ttl_bound = 1;
        let mut records = vec![incomplete, apex3(), ce3(), cut3()];
        clear_optout(&mut records);
        records[1].ttl_bound = 100;
        records[2].ttl_bound = 80;
        records[3].ttl_bound = 60;
        for reverse in [false, true] {
            if reverse {
                records.reverse();
            }
            let mut ctx = DenialContext::default();
            let mut budget = WorkBudget::default();
            let zone = name("example.");
            let target = name("a.c.x.w.example.");
            assert_eq!(
                name_error(&records, &zone, &target, &mut ctx, &mut budget),
                Ok(ProofResult::proven(60))
            );
            // Each chain hashes each name once, across both the exact and
            // closest-encloser searches; the second invocation is all memo hits.
            let hashes = budget.nsec3_hashes;
            assert!(hashes > 4 && hashes <= 12, "{hashes}");
            assert_eq!(
                name_error(&records, &zone, &target, &mut ctx, &mut budget),
                Ok(ProofResult::proven(60))
            );
            assert_eq!(budget.nsec3_hashes, hashes);
        }
    }
}

#[test]
fn incomplete_chains_cannot_supply_each_others_missing_fragments() {
    for different_iterations in [false, true] {
        let mut fragment = cut3();
        if different_iterations {
            mutate_nsec3(&mut fragment).iterations = 0;
        } else {
            mutate_nsec3(&mut fragment).salt.clear();
        }
        let mut records = vec![apex3(), ce3(), fragment];
        clear_optout(&mut records);
        for _ in 0..2 {
            assert_eq!(nx(&records, "a.c.x.w.example."), ProofResult::missing());
            assert_eq!(
                nodata(&records, "a.c.x.w.example.", RecordType::AAAA),
                ProofResult::missing()
            );
            records.reverse();
        }
    }
}

#[test]
fn rollover_does_not_rescue_invalid_bitmaps_or_missing_wildcard_denial() {
    let mut incomplete = apex3();
    mutate_nsec3(&mut incomplete).salt.clear();
    let mut invalid = ns13();
    mutate_nsec3(&mut invalid).types = bitmap(&[1, 5, 46]);
    assert_eq!(
        nodata(
            &[incomplete.clone(), invalid],
            "ns1.example.",
            RecordType::MX
        ),
        ProofResult::missing()
    );
    let mut records = vec![incomplete, apex3(), ce3()];
    clear_optout(&mut records);
    assert_eq!(nx(&records, "a.c.x.w.example."), ProofResult::missing());
}

#[test]
fn high_iteration_candidates_alone_fail_as_iterations_limited_without_hash_work() {
    let mut high = ns13();
    mutate_nsec3(&mut high).iterations = 151;
    let zone = name("example.");
    let target = name("ns1.example.");
    let mut ctx = DenialContext::default();
    let mut budget = WorkBudget::default();
    assert_eq!(
        name_error(
            std::slice::from_ref(&high),
            &zone,
            &target,
            &mut ctx,
            &mut budget
        ),
        Ok(ProofResult {
            proof: Proof::IterationsLimited,
            ttl_bound: 0,
        })
    );
    assert_eq!(
        wildcard(
            std::slice::from_ref(&high),
            &zone,
            &target,
            &zone,
            &mut ctx,
            &mut budget
        ),
        Ok(ProofResult {
            proof: Proof::IterationsLimited,
            ttl_bound: 0,
        })
    );
    assert_eq!(budget.nsec3_hashes, 0);
    let mut accepted = hashed_record("ns1.example.", "ns2.example.", &[1, 46], 150);
    accepted.ttl_bound = 75;
    high.ttl_bound = 1;
    let mut records = vec![high, accepted];
    for _ in 0..2 {
        assert_eq!(
            no_data(
                &records,
                &zone,
                &target,
                RecordType::MX,
                &mut ctx,
                &mut budget
            ),
            Ok(ProofResult::proven(75))
        );
        assert_eq!(budget.nsec3_hashes, 1);
        records.reverse();
    }
    mutate_nsec3(&mut records[1]).types = bitmap(&[1, 5, 46]);
    assert_eq!(
        no_data(
            &records,
            &zone,
            &target,
            RecordType::MX,
            &mut ctx,
            &mut budget
        ),
        Ok(ProofResult {
            proof: Proof::IterationsLimited,
            ttl_bound: 0,
        })
    );
    assert_eq!(budget.nsec3_hashes, 1);
}

#[test]
fn alternate_chain_search_obeys_the_single_shared_hash_budget() {
    let mut incomplete = apex3();
    mutate_nsec3(&mut incomplete).salt.clear();
    let records = [incomplete, ns13()];
    let mut budget = WorkBudget {
        nsec3_hashes: MAX_NSEC3_HASHES - 1,
        ..WorkBudget::default()
    };
    assert_eq!(
        no_data(
            &records,
            &name("example."),
            &name("ns1.example."),
            RecordType::MX,
            &mut DenialContext::default(),
            &mut budget,
        ),
        Err(BogusReason::WorkLimit)
    );
    assert_eq!(budget.nsec3_hashes, MAX_NSEC3_HASHES);
}

#[test]
fn nsec3_hashes_charge_global_crypto_once_and_memo_hits_remain_free() {
    let mut ctx = DenialContext::default();
    let mut budget = WorkBudget {
        crypto_operations: crate::limits::MAX_SIGNATURE_CHECKS - 1,
        ..WorkBudget::default()
    };
    let zone = name("example.");
    let target = name("ns1.example.");
    let records = [ns13()];
    for _ in 0..2 {
        assert_eq!(
            no_data(
                &records,
                &zone,
                &target,
                RecordType::MX,
                &mut ctx,
                &mut budget
            ),
            Ok(ProofResult::proven(3600))
        );
        assert_eq!(
            budget.crypto_operations,
            crate::limits::MAX_SIGNATURE_CHECKS
        );
        assert_eq!(budget.nsec3_hashes, 1);
    }
    assert_eq!(
        no_data(
            &records,
            &zone,
            &name("ns2.example."),
            RecordType::MX,
            &mut ctx,
            &mut budget,
        ),
        Err(BogusReason::WorkLimit)
    );
    assert_eq!(
        budget.crypto_operations,
        crate::limits::MAX_SIGNATURE_CHECKS
    );
    assert_eq!(budget.nsec3_hashes, 1);
}
