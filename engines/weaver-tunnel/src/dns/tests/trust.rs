// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::ttl::{ASKED, Zone, failure, respond, rr, server, soa, v4, wire};
use super::*;
use crate::metrics::Resolver;
use crate::resolution::{
    ResolutionCache,
    record::{LookupError, Recording},
};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

// `reply` with the AD bit set.
fn authentic(mut reply: Vec<u8>) -> Vec<u8> {
    reply[3] |= 0x20;
    reply
}

// No address of the asked family, and how long that holds.
fn nodata(query: &[u8]) -> Vec<u8> {
    respond(query, 0, &[], &[soa(600, 60)])
}

fn other() -> DnsServer {
    DnsServer::plain("192.0.2.54".parse().unwrap())
}

async fn ask(zone: &Zone, servers: &[DnsServer], host: &str) -> Result<Lookup, Unanswered> {
    resolve(zone, &WIRES, servers, Trust::RequireAd, host).await
}

// What a test recording says of a lookup of `host` under `trust`.
async fn recorded(
    zone: &Zone,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> record::HostResolution {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            record::asking(host, || record::Asker::servers(servers, trust));
            if let Ok(lookup) = resolve(zone, &WIRES, servers, trust, host).await {
                record::found(host, &lookup.addresses);
            }
        })
        .await;
    recording.finish(host, 80, None).expect("a resolution")
}

fn refused(server: &str) -> LookupError {
    failure(
        LookupFailure::NotAuthenticated,
        &format!("{server}: the answer was not marked authenticated"),
    )
}

#[tokio::test]
async fn a_query_asks_for_the_ad_bit_only_when_answers_must_be_authenticated() {
    for (trust, flags) in [
        (Trust::AsSent, [0x01, 0x00]),
        (Trust::RequireAd, [0x01, 0x20]),
    ] {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let kept = seen.clone();
        let zone = Zone::new(move |_, _, query| {
            kept.lock().unwrap().push([query[2], query[3]]);
            authentic(v4(query, 60))
        });
        resolve(&zone, &WIRES, &[server()], trust, "fixture.test")
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), [flags, flags], "{trust:?}");
    }
}

#[tokio::test]
async fn an_answer_marked_authenticated_is_taken() {
    let zone = Zone::new(|_, qtype, query| match qtype {
        1 => authentic(v4(query, 60)),
        _ => authentic(nodata(query)),
    });
    let lookup = ask(&zone, &[server()], "fixture.test").await.unwrap();
    assert_eq!(lookup.addresses, ["192.0.2.7".parse::<IpAddr>().unwrap()]);
}

#[tokio::test]
async fn an_answer_not_marked_authenticated_is_refused_and_the_next_server_asked() {
    let next = other().address.ip();
    let zone = Zone::new(move |asked, qtype, query| match (asked == next, qtype) {
        (false, _) => v4(query, 60),
        (true, 1) => authentic(respond(
            query,
            0,
            &[rr(&ASKED, 1, 60, &[192, 0, 2, 9])],
            &[],
        )),
        (true, _) => authentic(nodata(query)),
    });
    let lookup = ask(&zone, &[server(), other()], "fixture.test")
        .await
        .unwrap();
    assert_eq!(lookup.addresses, ["192.0.2.9".parse::<IpAddr>().unwrap()]);
    // Taken as sent, the first server's answer stands.
    let lookup = resolve(
        &zone,
        &WIRES,
        &[server(), other()],
        Trust::AsSent,
        "fixture.test",
    )
    .await
    .unwrap();
    assert_eq!(lookup.addresses, ["192.0.2.7".parse::<IpAddr>().unwrap()]);
}

#[tokio::test]
async fn an_alias_walk_with_a_step_not_marked_authenticated_is_refused() {
    let zone = Zone::new(|_, qtype, query| {
        let target = query.windows(6).any(|name| name == b"target");
        match (qtype, target) {
            (1, false) => authentic(respond(
                query,
                0,
                &[rr(&ASKED, 5, 60, &wire("target.test"))],
                &[],
            )),
            // The reply for the alias's target carries no AD bit.
            (1, true) => v4(query, 60),
            _ => authentic(nodata(query)),
        }
    });
    assert!(ask(&zone, &[server()], "alias.test").await.is_err());
    let resolution = recorded(&zone, &[server()], Trust::RequireAd, "alias.test").await;
    assert_eq!(resolution.error, Some(refused("192.0.2.53")));
    assert_eq!(resolution.trust, Some(TrustOutcome::Refused));
}

#[tokio::test]
async fn a_name_said_not_to_exist_counts_only_when_marked_authenticated() {
    let unmarked = Zone::new(|_, _, query| respond(query, 3, &[], &[soa(900, 120)]));
    let unanswered = ask(&unmarked, &[server()], "missing.test")
        .await
        .unwrap_err();
    assert!(unanswered.reached);
    let resolution = recorded(&unmarked, &[server()], Trust::RequireAd, "missing.test").await;
    assert_eq!(resolution.error, Some(refused("192.0.2.53")));

    let marked = Zone::new(|_, _, query| authentic(respond(query, 3, &[], &[soa(900, 120)])));
    let lookup = ask(&marked, &[server()], "missing.test").await.unwrap();
    assert!(lookup.addresses.is_empty());
    assert!(lookup.nonexistent);
    assert_eq!(lookup.ttl, Ttl::Seconds(120));
}

#[tokio::test]
async fn a_refused_lookup_is_asked_again_and_an_authenticated_one_is_held() {
    for (mark, asked_twice) in [(false, true), (true, false)] {
        let queries = Arc::new(AtomicUsize::new(0));
        let counted = queries.clone();
        let zone = Zone::new(move |_, qtype, query| {
            counted.fetch_add(1, Ordering::Relaxed);
            let reply = match qtype {
                1 => v4(query, 300),
                _ => nodata(query),
            };
            if mark { authentic(reply) } else { reply }
        });
        let cache = ResolutionCache::<DialError>::default();
        let mut answers = Vec::new();
        for _ in 0..2 {
            answers.push(
                cache
                    .resolve(Resolver::Routed, "fixture.test", || async {
                        ask(&zone, &[server()], "fixture.test")
                            .await
                            .map_err(|_| crate::dns::unresolved())
                    })
                    .await
                    .is_ok(),
            );
        }
        assert_eq!(answers, [mark, mark]);
        // Two queries a lookup, one for each family.
        let lookups = queries.load(Ordering::Relaxed) / 2;
        assert_eq!(lookups, if asked_twice { 2 } else { 1 }, "marked: {mark}");
    }
}

#[tokio::test]
async fn the_record_says_how_the_answer_was_judged() {
    let marked = Zone::new(|_, qtype, query| match qtype {
        1 => authentic(v4(query, 300)),
        _ => authentic(nodata(query)),
    });
    let resolution = recorded(&marked, &[server()], Trust::RequireAd, "fixture.test").await;
    assert_eq!(resolution.trust, Some(TrustOutcome::Authenticated));
    assert_eq!(resolution.wire, Some(WireKind::Plain));
    assert_eq!(resolution.error, None);

    let unmarked = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => nodata(query),
    });
    let resolution = recorded(&unmarked, &[server()], Trust::RequireAd, "fixture.test").await;
    assert_eq!(resolution.trust, Some(TrustOutcome::Refused));
    assert_eq!(resolution.error, Some(refused("192.0.2.53")));

    let resolution = recorded(&unmarked, &[server()], Trust::AsSent, "fixture.test").await;
    assert_eq!(resolution.trust, Some(TrustOutcome::AsSent));
    assert_eq!(resolution.error, None);
}

#[tokio::test]
async fn an_answer_held_under_require_ad_is_recorded_as_authenticated() {
    let zone = Zone::new(|_, qtype, query| match qtype {
        1 => authentic(v4(query, 300)),
        _ => authentic(nodata(query)),
    });
    let cache = ResolutionCache::<DialError>::default();
    let lookup = || {
        cache.resolve(Resolver::Routed, "fixture.test", || async {
            ask(&zone, &[server()], "fixture.test")
                .await
                .map_err(|_| crate::dns::unresolved())
        })
    };
    lookup().await.unwrap();
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            record::asking("fixture.test", || {
                record::Asker::servers(&[server()], Trust::RequireAd)
            });
            let addresses = lookup().await.unwrap();
            record::found("fixture.test", &addresses);
        })
        .await;
    let resolution = recording.finish("fixture.test", 80, None).unwrap();
    assert!(resolution.cached);
    assert_eq!(resolution.trust, Some(TrustOutcome::Authenticated));
    assert_eq!(resolution.wire, Some(WireKind::Plain));
}
