// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Replies are read by the DNSSEC crate's one parser on both paths: what the
// lookup makes of replies the reference resolvers read loosely, and of
// those they reject.
use super::ttl::{ASKED, Zone, failure, respond, rr, server, why, wire};
use super::*;
use std::sync::{Arc, Mutex};

// Where the query's question ends.
fn question_end(query: &[u8]) -> usize {
    let mut at = 12;
    while query[at] != 0 {
        at += 1 + query[at] as usize;
    }
    at + 5
}

// The type the query asks for, read from its question: a validating query
// ends with an EDNS record, so its last bytes are not the question's.
fn qtype(query: &[u8]) -> u16 {
    u16_at(query, question_end(query) - 4).unwrap()
}

// The question's name as the query sent it.
fn asked_name(query: &[u8]) -> Vec<u8> {
    query[12..question_end(query) - 4].to_vec()
}

// A reply to `query` with the given sections and no additional section,
// whichever path asked.
fn reply(query: &[u8], rcode: u8, answers: &[Vec<u8>], authority: &[Vec<u8>]) -> Vec<u8> {
    let mut head = query[..question_end(query)].to_vec();
    head[10..12].copy_from_slice(&[0, 0]);
    respond(&head, rcode, answers, authority)
}

// An address record owned by `owner` of the family `query` asks for.
fn address(query: &[u8], owner: &[u8]) -> Vec<u8> {
    match qtype(query) {
        1 => rr(owner, 1, 300, &[192, 0, 2, 7]),
        kind => rr(owner, kind, 300, &[0x20; 16]),
    }
}

// An SOA owned by `owner`, then `trailing` bytes; `numbers` of the twenty
// bytes of its five numbers are kept.
fn soa_at(owner: &str, ttl: u32, minimum: u32, numbers: usize, trailing: usize) -> Vec<u8> {
    let mut data = wire("ns.test");
    data.extend(wire("hostmaster.test"));
    let fields: Vec<u8> = [1u32, 7200, 900, 1_209_600, minimum]
        .iter()
        .flat_map(|field| field.to_be_bytes())
        .collect();
    data.extend_from_slice(&fields[..numbers]);
    data.extend(std::iter::repeat_n(0, trailing));
    rr(&wire(owner), 6, ttl, &data)
}

// The lookup of `host` from `zone`'s single server, taken as sent and then
// validated. The names are under `test.`, a private zone, so the validator
// asks only the question and takes the answer as insecure.
async fn both(zone: &Zone, host: &str) -> [Lookup; 2] {
    let roots = TrustRoots::built_in();
    let mut lookups = Vec::new();
    for trust in [Trust::AsSent, Trust::Validate] {
        let lookup = resolve(zone, roots.wires(), &[server()], trust, host).await;
        lookups.push(lookup.unwrap_or_else(|_| panic!("{trust:?} found no answer")));
    }
    lookups.try_into().ok().unwrap()
}

fn invalid() -> record::LookupError {
    failure(LookupFailure::Other, &format!("192.0.2.53: {INVALID}"))
}

#[tokio::test]
async fn a_reply_of_129_addresses_is_read_whole() {
    let zone = Zone::new(|_, _, query| match qtype(query) {
        1 => {
            let answers: Vec<_> = (0..129u8)
                .map(|n| rr(&ASKED, 1, 60, &[10, 0, 0, n]))
                .collect();
            reply(query, 0, &answers, &[])
        }
        _ => reply(query, 0, &[], &[]),
    });
    let lookup = resolve_name(&zone, &[server()], "fixture.test")
        .await
        .unwrap();
    assert_eq!(lookup.addresses.len(), 129);
}

#[tokio::test]
async fn zero_padding_after_the_reply_is_passed_over_on_both_paths() {
    let zone = Zone::new(|_, _, query| {
        let mut bytes = reply(query, 0, &[address(query, &ASKED)], &[]);
        bytes.extend_from_slice(&[0; 8]);
        bytes
    });
    for (lookup, path) in both(&zone, "fixture.test")
        .await
        .iter()
        .zip(["as sent", "validating"])
    {
        assert_eq!(lookup.addresses.len(), 2, "{path}");
        assert_eq!(lookup.ttl, Ttl::Seconds(300), "{path}");
    }
}

#[tokio::test]
async fn an_alias_to_a_name_with_a_raw_high_byte_is_followed() {
    // gone.test CNAME "\195ab.test": a label byte outside ASCII.
    let target = [&[3, 0xc3, b'a', b'b'][..], &wire("test")].concat();
    let asked = Arc::new(Mutex::new(Vec::new()));
    let seen = asked.clone();
    let alias = target.clone();
    let zone = Zone::new(move |_, _, query| {
        let name = asked_name(query);
        seen.lock().unwrap().push(name.clone());
        if name == alias {
            reply(query, 0, &[address(query, &ASKED)], &[])
        } else {
            reply(query, 0, &[rr(&ASKED, 5, 60, &alias)], &[])
        }
    });
    let lookup = resolve_name(&zone, &[server()], "gone.test").await.unwrap();
    assert_eq!(lookup.addresses.len(), 2);
    assert_eq!(lookup.ttl, Ttl::Seconds(60));
    let asked = asked.lock().unwrap();
    assert_eq!(asked.iter().filter(|name| **name == target).count(), 2);
}

#[tokio::test]
async fn a_label_holding_a_dot_is_not_the_two_label_host() {
    // One label, "a.test", is not the host a.test, of the labels a and test.
    let zone = Zone::new(|_, _, query| {
        let owner = [&[6][..], b"a.test", &[0]].concat();
        reply(query, 0, &[address(query, &owner)], &[])
    });
    let lookup = resolve_name(&zone, &[server()], "a.test").await.unwrap();
    assert!(lookup.addresses.is_empty());
    assert_eq!(lookup.ttl, Ttl::Seconds(0));
}

#[tokio::test]
async fn the_first_soa_received_holds_a_negative_answer_on_both_paths() {
    let child = || soa_at("sub.test", 900, 60, 20, 0);
    let parent = || soa_at("test", 900, 120, 20, 0);
    for (authority, held) in [
        (vec![child(), parent()], 60),
        (vec![parent(), child()], 120),
        // The same SOA twice holds by the lower TTL.
        (vec![parent(), soa_at("test", 10, 120, 20, 0)], 10),
        // Bytes after an SOA's numbers are passed over.
        (vec![soa_at("test", 900, 120, 20, 3)], 120),
        // One short of its numbers holds nothing, and a later one is not
        // taken in its place.
        (vec![soa_at("sub.test", 900, 60, 19, 0), parent()], 0),
    ] {
        let zone = Zone::new({
            let authority = authority.clone();
            move |_, _, query| reply(query, 3, &[], &authority)
        });
        for (lookup, path) in both(&zone, "missing.sub.test")
            .await
            .iter()
            .zip(["as sent", "validating"])
        {
            assert!(lookup.addresses.is_empty(), "{path}");
            assert!(lookup.nonexistent, "{path}");
            assert_eq!(lookup.ttl, Ttl::Seconds(held), "{path} {authority:?}");
        }
    }
}

#[tokio::test]
async fn an_authority_record_cut_short_fails_the_reply() {
    let zone = Zone::new(|_, _, query| {
        let mut soa = soa_at("test", 900, 120, 20, 0);
        soa.truncate(soa.len() - 4);
        reply(query, 3, &[], &[soa])
    });
    assert_eq!(why(&zone, &[server()], "missing.test").await, invalid());
}

#[tokio::test]
async fn noerror_with_an_extended_code_in_the_edns_record_is_an_answer() {
    let zone = Zone::new(|_, _, query| {
        let mut bytes = reply(query, 0, &[address(query, &ASKED)], &[]);
        bytes[10..12].copy_from_slice(&1u16.to_be_bytes());
        // OPT at the root, 1232 bytes, extended RCODE 1: BADVERS were the
        // header's bits combined with it.
        bytes.extend_from_slice(&[0, 0, 41, 4, 208, 1, 0, 0, 0, 0, 0]);
        bytes
    });
    for (lookup, path) in both(&zone, "fixture.test")
        .await
        .iter()
        .zip(["as sent", "validating"])
    {
        assert_eq!(lookup.addresses.len(), 2, "{path}");
    }
}

// A reply whose address record's owner is reached through `hops`
// compression pointers: one from the owner, the rest a chain inside the
// data of a TXT record before it, the first of them to the question's name.
fn chained(query: &[u8], hops: usize) -> Vec<u8> {
    let mut bytes = reply(query, 0, &[], &[]);
    bytes[6..8].copy_from_slice(&2u16.to_be_bytes());
    let data_at = bytes.len() + ASKED.len() + 10;
    let pointer = |at: usize| (0xc000 | at as u16).to_be_bytes();
    let mut chain = pointer(12).to_vec();
    for link in 1..hops - 1 {
        chain.extend(pointer(data_at + 2 * (link - 1)));
    }
    bytes.extend(rr(&ASKED, 16, 60, &chain));
    bytes.extend(address(query, &pointer(data_at + 2 * (hops - 2))));
    bytes
}

#[tokio::test]
async fn a_name_through_40_pointers_is_read_and_one_through_256_is_not() {
    let zone = Zone::new(|_, _, query| chained(query, 40));
    let lookup = resolve_name(&zone, &[server()], "fixture.test")
        .await
        .unwrap();
    assert_eq!(lookup.addresses.len(), 2);
    let zone = Zone::new(|_, _, query| chained(query, 255));
    let lookup = resolve_name(&zone, &[server()], "fixture.test")
        .await
        .unwrap();
    assert_eq!(lookup.addresses.len(), 2);
    let zone = Zone::new(|_, _, query| chained(query, 256));
    assert_eq!(why(&zone, &[server()], "fixture.test").await, invalid());
}

#[tokio::test]
async fn a_reply_under_another_id_is_invalid() {
    let zone = Zone::new(|_, _, query| {
        let mut bytes = reply(query, 0, &[address(query, &ASKED)], &[]);
        bytes[0] ^= 0xff;
        bytes
    });
    assert_eq!(why(&zone, &[server()], "fixture.test").await, invalid());
}
