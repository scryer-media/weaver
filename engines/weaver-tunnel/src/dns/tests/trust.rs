// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::ttl::{ASKED, Zone, failure, respond, rr, server, soa, v4, wire};
use super::*;
use crate::metrics::Resolver;
use crate::resolution::{
    ResolutionCache,
    record::{LookupError, Recording},
};
use std::future::Future;
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

// One lookup of `host` the way a naming makes it: through its answer cache,
// in a test recording of its own, noting what the dial would. `lookup` is
// what the naming's servers come to when the cache asks them.
pub(super) async fn through_cache<F, Fut>(
    cache: &ResolutionCache<DialError>,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
    lookup: F,
) -> record::HostResolution
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Lookup, Unanswered>>,
{
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            record::asking(host, || record::Asker::servers(servers, trust));
            let resolved = cache
                .resolve(Resolver::Routed, host, || async {
                    lookup().await.map_err(|unanswered| {
                        DialError::Destination(std::io::Error::other(unanswered.error))
                    })
                })
                .await
                .and_then(|addresses| {
                    if addresses.is_empty() {
                        Err(crate::dns::unresolved())
                    } else {
                        Ok(addresses)
                    }
                });
            match resolved {
                Ok(addresses) => record::found(host, &addresses),
                Err(error) => record::failed_saying(host, &error),
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
async fn an_answer_marked_authenticated_is_taken() {
    let zone = Zone::new(|_, qtype, query| match qtype {
        1 => authentic(v4(query, 60)),
        _ => authentic(nodata(query)),
    });
    let lookup = ask(&zone, &[server()], "fixture.test").await.unwrap();
    assert_eq!(lookup.addresses, ["192.0.2.7".parse::<IpAddr>().unwrap()]);
}

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
async fn the_record_says_how_the_answer_was_judged() {
    let servers = [server()];
    let marked = Zone::new(|_, qtype, query| match qtype {
        1 => authentic(v4(query, 300)),
        _ => authentic(nodata(query)),
    });
    let unmarked = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => nodata(query),
    });
    for (zone, trust, judged, error) in [
        (&marked, Trust::RequireAd, TrustOutcome::Authenticated, None),
        (
            &unmarked,
            Trust::RequireAd,
            TrustOutcome::Refused,
            Some(refused("192.0.2.53")),
        ),
        (&unmarked, Trust::AsSent, TrustOutcome::AsSent, None),
    ] {
        let cache = ResolutionCache::<DialError>::default();
        let resolution = through_cache(&cache, &servers, trust, "fixture.test", || {
            resolve(zone, &WIRES, &servers, trust, "fixture.test")
        })
        .await;
        assert!(!resolution.cached);
        assert_eq!(resolution.trust, Some(judged), "{trust:?}");
        assert_eq!(resolution.error, error, "{trust:?}");
        // Only an answer that came is said to have come over a wire.
        assert_eq!(resolution.wire, Some(WireKind::Plain));
    }
}

#[tokio::test(start_paused = true)]
async fn an_answer_held_under_require_ad_is_recorded_as_authenticated() {
    let servers = [server()];
    let queries = Arc::new(AtomicUsize::new(0));
    let counted = queries.clone();
    let zone = Zone::new(move |_, qtype, query| {
        counted.fetch_add(1, Ordering::Relaxed);
        match qtype {
            1 => authentic(v4(query, 300)),
            _ => authentic(nodata(query)),
        }
    });
    let cache = ResolutionCache::<DialError>::default();
    let ask = || {
        through_cache(&cache, &servers, Trust::RequireAd, "fixture.test", || {
            resolve(&zone, &WIRES, &servers, Trust::RequireAd, "fixture.test")
        })
    };
    let first = ask().await;
    assert!(!first.cached);
    assert_eq!(first.trust, Some(TrustOutcome::Authenticated));
    let held = ask().await;
    // The second answer is the held one: no query went out for it.
    assert_eq!(queries.load(Ordering::Relaxed), 2);
    assert!(held.cached);
    assert_eq!(held.trust, Some(TrustOutcome::Authenticated));
    assert_eq!(held.addresses, ["192.0.2.7".parse::<IpAddr>().unwrap()]);
    assert_eq!(held.error, None);
}

#[tokio::test(start_paused = true)]
async fn a_miss_refused_as_not_authenticated_is_recorded_as_refused() {
    let servers = [server()];
    let unmarked = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => nodata(query),
    });
    let cache = ResolutionCache::<DialError>::default();
    let resolution = through_cache(&cache, &servers, Trust::RequireAd, "fixture.test", || {
        ask(&unmarked, &servers, "fixture.test")
    })
    .await;
    assert!(!resolution.cached);
    assert_eq!(resolution.trust, Some(TrustOutcome::Refused));
    assert_eq!(resolution.error, Some(refused("192.0.2.53")));
}

#[tokio::test(start_paused = true)]
async fn a_caller_that_joined_a_refused_lookup_is_recorded_as_refused() {
    let servers = [server()];
    let unmarked = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => nodata(query),
    });
    let cache = ResolutionCache::<DialError>::default();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let asker = through_cache(
        &cache,
        &servers,
        Trust::RequireAd,
        "fixture.test",
        || async {
            released.await.unwrap();
            ask(&unmarked, &servers, "fixture.test").await
        },
    );
    let joiner = async {
        // Joins only once the first caller's lookup is out.
        while cache.waiting("fixture.test").is_none() {
            tokio::task::yield_now().await;
        }
        through_cache(
            &cache,
            &servers,
            Trust::RequireAd,
            "fixture.test",
            || async { unreachable!("a caller that joins asks no one") },
        )
        .await
    };
    // Lets the lookup land once the joiner waits on it.
    let releaser = async {
        while cache.waiting("fixture.test") != Some(1) {
            tokio::task::yield_now().await;
        }
        release.send(()).unwrap();
    };
    let (asked, joined, ()) = tokio::join!(asker, joiner, releaser);
    assert_eq!(asked.trust, Some(TrustOutcome::Refused));
    assert!(!joined.cached);
    assert_eq!(joined.trust, Some(TrustOutcome::Refused), "{joined:?}");
    assert_eq!(
        joined.error.map(|error| error.failure),
        Some(LookupFailure::NotAuthenticated)
    );
}

#[tokio::test(start_paused = true)]
async fn a_refused_family_beside_an_authenticated_one_is_left_out_of_the_answer_and_the_cache() {
    let servers = [server()];
    let queries = Arc::new(AtomicUsize::new(0));
    let counted = queries.clone();
    let zone = Zone::new(move |_, qtype, query| {
        counted.fetch_add(1, Ordering::Relaxed);
        match qtype {
            // A, not marked.
            1 => v4(query, 300),
            _ => authentic(respond(
                query,
                0,
                &[rr(
                    &ASKED,
                    28,
                    300,
                    &[0x20, 1, 0xd, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7],
                )],
                &[],
            )),
        }
    });
    let six = ["2001:db8::7".parse::<IpAddr>().unwrap()];
    let cache = ResolutionCache::<DialError>::default();
    for held in [false, true] {
        let resolution = through_cache(&cache, &servers, Trust::RequireAd, "fixture.test", || {
            ask(&zone, &servers, "fixture.test")
        })
        .await;
        assert_eq!(resolution.addresses, six, "held: {held}");
        assert_eq!(resolution.cached, held);
        assert_eq!(resolution.trust, Some(TrustOutcome::Authenticated));
    }
    assert_eq!(queries.load(Ordering::Relaxed), 2);
}
