// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::rdata::RdataKind;
use crate::rrset::{SignatureLimit, WorkBudget, verify_rrset};
use crate::{Name, Policy, RecordType};

#[test]
fn rfc4035_appendix_b1_signatures_against_appendix_a_keys() {
    // rfc4035.txt lines 2010–2031 and 2289–2349, copied to the adjacent fixture.
    let sets = crate::test_vectors::parse(include_str!("rfc4035_vectors.txt"));
    let keys: Vec<_> = sets
        .iter()
        .filter(|set| set.rtype == RecordType::DNSKEY)
        .flat_map(|set| &set.records)
        .map(|record| match &record.kind {
            RdataKind::Dnskey(key) => key.clone(),
            _ => panic!("DNSKEY fixture has wrong parsed type"),
        })
        .collect();
    assert_eq!(keys.len(), 2);
    let policy = Policy {
        sha1_signatures: true,
        ..Policy::default()
    };
    let mut outcomes = Vec::new();
    for set in sets.iter().filter(|set| !set.rrsigs.is_empty()) {
        let signature = &set.rrsigs[0];
        let now = (u64::from(signature.inception) + u64::from(signature.expiration)) / 2;
        let outcome = verify_rrset(
            set,
            &Name::from_text("example.").unwrap(),
            &keys,
            now,
            policy,
            &mut WorkBudget::default(),
            SignatureLimit::CachedKeys(&mut 0),
        );
        outcomes.push((set.owner.clone(), set.rtype, outcome));
    }
    assert_eq!(outcomes.len(), 6);
    assert!(
        outcomes.iter().all(|(_, _, result)| result.is_ok()),
        "{outcomes:?}"
    );
}
