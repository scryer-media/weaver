// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::ttl::{ASKED, Zone, failure, respond, rr, server, soa, v4, wire};
use super::*;
use crate::metrics::Resolver;
use crate::pipe::{DialPath, Dialed, DialedStream, Naming};
use crate::resolution::{
    ResolutionCache,
    record::{LookupError, Recording},
};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

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

// A proxy hop that carries each query to `carrier`, the server being the
// dial's target. A server `carrier` cannot open fails the dial as the hop.
pub(super) struct Through<'a>(pub(super) &'a dyn Carrier);

// The proxy every test naming asks through.
pub(super) const PROXY: u32 = 7;

#[async_trait::async_trait]
impl Dialer for Through<'_> {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let server = DnsServer {
            address: SocketAddr::new(target.addresses[0], target.port),
            wire: Wire::Plain,
        };
        let stream = self
            .0
            .open(&server)
            .await
            .map_err(|source| DialError::Hop {
                proxy: PROXY,
                source,
            })?;
        Ok(Dialed {
            stream: DialedStream::Tunnel(stream),
            outcome: Default::default(),
            path: DialPath::default(),
            peer: None,
            source: None,
            setup: None,
        })
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "through".into()
    }
}

// How long a test naming's lookup may take, far above any lookup's own.
pub(super) const WINDOW: Duration = Duration::from_secs(600);

// One lookup of `host` the way a dial through a hop makes it, by the
// naming's own path, in a test recording of its own: `carrier` stands for
// the hop, and the lookup has `window` to finish.
pub(super) async fn through_naming_within(
    naming: &Naming,
    carrier: &dyn Carrier,
    host: &str,
    window: Duration,
) -> record::HostResolution {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            let _ = naming.resolve(&Through(carrier), PROXY, host, window).await;
        })
        .await;
    recording.finish(host, 80, None).expect("a resolution")
}

pub(super) async fn through_naming(
    naming: &Naming,
    carrier: &dyn Carrier,
    host: &str,
) -> record::HostResolution {
    through_naming_within(naming, carrier, host, WINDOW).await
}

// The answers a naming that asks DNS servers holds.
pub(super) fn answers(naming: &Naming) -> &ResolutionCache<DialError> {
    match naming {
        Naming::Servers { answers, .. } | Naming::ProxyServers { answers, .. } => answers,
        Naming::Host | Naming::Proxy => unreachable!("a naming that asks no servers"),
    }
}

// The TLS sessions a naming that asks DNS servers resumes.
pub(super) fn sessions(naming: &Naming) -> &Arc<DnsSessions> {
    match naming {
        Naming::Servers { sessions, .. } | Naming::ProxyServers { sessions, .. } => sessions,
        Naming::Host | Naming::Proxy => unreachable!("a naming that asks no servers"),
    }
}

// The trust metric's counts, to take what one test's lookups add.
pub(super) fn counted() -> Vec<(TrustOutcome, u64)> {
    crate::metrics::dns_snapshot().trust
}

// The outcomes the trust metric counted since `before`, one per lookup.
pub(super) fn since(before: &[(TrustOutcome, u64)]) -> Vec<TrustOutcome> {
    counted()
        .into_iter()
        .zip(before)
        .flat_map(|((outcome, now), (_, then))| std::iter::repeat_n(outcome, (now - then) as usize))
        .collect()
}

// Servers that answer each query as `answer` says, or hold the
// connection open and never reply when it gives nothing.
pub(super) struct Scripted(Arc<Script>);

type Script = dyn Fn(IpAddr, u16, &[u8]) -> Option<Vec<u8>> + Send + Sync;

impl Scripted {
    pub(super) fn new(
        answer: impl Fn(IpAddr, u16, &[u8]) -> Option<Vec<u8>> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(answer))
    }
}

#[async_trait::async_trait]
impl Carrier for Scripted {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (client, mut stream) = tokio::io::duplex(4096);
        let answer = self.0.clone();
        let asked = server.address.ip();
        tokio::spawn(async move {
            let size = stream.read_u16().await.unwrap() as usize;
            let mut query = vec![0; size];
            stream.read_exact(&mut query).await.unwrap();
            let Some(reply) = answer(asked, u16_at(&query, size - 4).unwrap(), &query) else {
                return std::future::pending::<()>().await;
            };
            stream.write_u16(reply.len() as u16).await.unwrap();
            stream.write_all(&reply).await.unwrap();
        });
        Ok(Box::new(client))
    }
}

// A hop that cannot be dialled, failing in the words of `said`.
struct Unreachable(&'static str);

#[async_trait::async_trait]
impl Carrier for Unreachable {
    async fn open(&self, _: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        Err(TunnelError::Engine(self.0.into()))
    }
}

// A carrier that holds every dial until `open` is told to let it through.
struct Gated<'a> {
    carrier: &'a dyn Carrier,
    open: tokio::sync::Semaphore,
}

#[async_trait::async_trait]
impl Carrier for Gated<'_> {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        self.open.acquire().await.unwrap().forget();
        self.carrier.open(server).await
    }
}

// Two callers resolving `host` through `naming` at once: the first asks
// through `carrier`, the second joins its lookup. What each recorded, and
// the trust outcomes the metric counted for both.
async fn asked_and_joined(
    naming: &Naming,
    carrier: &dyn Carrier,
    host: &str,
) -> (
    record::HostResolution,
    record::HostResolution,
    Vec<TrustOutcome>,
) {
    let before = counted();
    let gated = Gated {
        carrier,
        open: tokio::sync::Semaphore::new(0),
    };
    let cache = answers(naming);
    let asker = through_naming(naming, &gated, host);
    let joiner = async {
        // Joins only once the first caller's lookup is out.
        while cache.waiting(host).is_none() {
            tokio::task::yield_now().await;
        }
        through_naming(
            naming,
            &Unreachable("a caller that joins asks no one"),
            host,
        )
        .await
    };
    // Lets the lookup through once the joiner waits on it.
    let releaser = async {
        while cache.waiting(host) != Some(1) {
            tokio::task::yield_now().await;
        }
        gated.open.add_permits(64);
    };
    let (asked, joined, ()) = tokio::join!(asker, joiner, releaser);
    (asked, joined, since(&before))
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
    let resolution = recorded(
        &zone,
        &[server(), other()],
        Trust::RequireAd,
        "fixture.test",
    )
    .await;
    assert_eq!(
        resolution.addresses,
        ["192.0.2.9".parse::<IpAddr>().unwrap()]
    );
    // The record names the server that answered, not the first listed.
    assert_eq!(resolution.answered_by, Some(next));
    // Taken as sent, the first server's answer stands.
    let resolution = recorded(&zone, &[server(), other()], Trust::AsSent, "fixture.test").await;
    assert_eq!(
        resolution.addresses,
        ["192.0.2.7".parse::<IpAddr>().unwrap()]
    );
    assert_eq!(resolution.answered_by, Some(server().address.ip()));
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
        let before = counted();
        let naming = Naming::proxy_servers(servers.to_vec(), trust);
        let resolution = through_naming(&naming, zone, "fixture.test").await;
        assert!(!resolution.cached);
        assert_eq!(resolution.trust, Some(judged), "{trust:?}");
        assert_eq!(since(&before), [judged], "{trust:?}");
        assert_eq!(resolution.error, error, "{trust:?}");
        // Only an answer that came is said to have come over a wire.
        assert_eq!(resolution.wire, Some(WireKind::Plain));
    }
}

#[tokio::test(start_paused = true)]
async fn an_answer_held_under_require_ad_is_recorded_as_authenticated() {
    let queries = Arc::new(AtomicUsize::new(0));
    let counted_queries = queries.clone();
    let zone = Zone::new(move |_, qtype, query| {
        counted_queries.fetch_add(1, Ordering::Relaxed);
        match qtype {
            1 => authentic(v4(query, 300)),
            _ => authentic(nodata(query)),
        }
    });
    let naming = Naming::proxy_servers(vec![server()], Trust::RequireAd);
    let before = counted();
    let first = through_naming(&naming, &zone, "fixture.test").await;
    assert!(!first.cached);
    assert_eq!(first.trust, Some(TrustOutcome::Authenticated));
    let held = through_naming(&naming, &zone, "fixture.test").await;
    // The second answer is the held one: no query went out for it, and
    // the metric counts the one lookup.
    assert_eq!(queries.load(Ordering::Relaxed), 2);
    assert_eq!(since(&before), [TrustOutcome::Authenticated]);
    assert!(held.cached);
    assert_eq!(held.trust, Some(TrustOutcome::Authenticated));
    assert_eq!(held.addresses, ["192.0.2.7".parse::<IpAddr>().unwrap()]);
    assert_eq!(held.error, None);
}

#[tokio::test(start_paused = true)]
async fn a_negative_answer_held_under_require_ad_is_recorded_as_authenticated() {
    let zone = Zone::new(|_, _, query| authentic(respond(query, 3, &[], &[soa(900, 120)])));
    let naming = Naming::proxy_servers(vec![server()], Trust::RequireAd);
    let before = counted();
    for held in [false, true] {
        let resolution = through_naming(&naming, &zone, "missing.test").await;
        assert_eq!(resolution.cached, held);
        assert_eq!(resolution.trust, Some(TrustOutcome::Authenticated));
        assert_eq!(
            resolution.error.map(|error| error.failure),
            Some(LookupFailure::NoSuchName)
        );
    }
    assert_eq!(since(&before), [TrustOutcome::Authenticated]);
}

#[tokio::test(start_paused = true)]
async fn a_miss_refused_as_not_authenticated_is_recorded_as_refused() {
    let unmarked = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => nodata(query),
    });
    let naming = Naming::proxy_servers(vec![server()], Trust::RequireAd);
    let before = counted();
    let resolution = through_naming(&naming, &unmarked, "fixture.test").await;
    assert!(!resolution.cached);
    assert_eq!(resolution.trust, Some(TrustOutcome::Refused));
    assert_eq!(since(&before), [TrustOutcome::Refused]);
    assert_eq!(resolution.error, Some(refused("192.0.2.53")));
}

#[tokio::test(start_paused = true)]
async fn a_caller_that_joined_a_refused_lookup_is_recorded_as_refused() {
    let unmarked = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => nodata(query),
    });
    let naming = Naming::proxy_servers(vec![server()], Trust::RequireAd);
    let (asked, joined, counted) = asked_and_joined(&naming, &unmarked, "fixture.test").await;
    assert_eq!(asked.trust, Some(TrustOutcome::Refused));
    assert_eq!(counted, [TrustOutcome::Refused]);
    assert!(!joined.cached);
    assert_eq!(joined.trust, Some(TrustOutcome::Refused), "{joined:?}");
    assert_eq!(joined.error, asked.error);
    assert_eq!(
        joined.error.map(|error| error.failure),
        Some(LookupFailure::NotAuthenticated)
    );
}

#[tokio::test(start_paused = true)]
async fn a_caller_that_joined_an_authenticated_lookup_is_recorded_as_authenticated() {
    let marked = Zone::new(|_, qtype, query| match qtype {
        1 => authentic(v4(query, 300)),
        _ => authentic(nodata(query)),
    });
    let naming = Naming::proxy_servers(vec![server()], Trust::RequireAd);
    let (asked, joined, counted) = asked_and_joined(&naming, &marked, "fixture.test").await;
    assert_eq!(asked.trust, Some(TrustOutcome::Authenticated));
    assert_eq!(counted, [TrustOutcome::Authenticated]);
    assert_eq!(joined.trust, Some(TrustOutcome::Authenticated));
    assert_eq!(joined.addresses, asked.addresses);
    assert_eq!(asked.answered_by, Some(server().address.ip()));
    assert_eq!(joined.answered_by, asked.answered_by);
}

// A family whose query timed out leaves the answer it had unfinished: a
// negative answer from the other family is not an answer, and nothing
// was judged.
#[tokio::test(start_paused = true)]
async fn a_lookup_that_failed_beside_an_authenticated_no_address_is_judged_by_nobody() {
    let zone = Scripted::new(|_, qtype, query| match qtype {
        1 => Some(authentic(nodata(query))),
        _ => None,
    });
    let naming = Naming::proxy_servers(vec![server()], Trust::RequireAd);
    let (asked, joined, counted) = asked_and_joined(&naming, &zone, "fixture.test").await;
    assert_eq!(
        asked.error.as_ref().map(|error| error.failure),
        Some(LookupFailure::NoAddress)
    );
    assert_eq!(asked.trust, None, "{asked:?}");
    assert_eq!(counted, []);
    assert_eq!(joined.error, asked.error);
    assert_eq!(joined.trust, None, "{joined:?}");
    // Nothing was held.
    assert!(!through_naming(&naming, &zone, "fixture.test").await.cached);
}

#[tokio::test(start_paused = true)]
async fn a_lookup_that_failed_after_an_authenticated_nxdomain_is_judged_by_nobody() {
    let first = server().address.ip();
    let zone = Scripted::new(move |asked, _, query| {
        (asked == first).then(|| authentic(respond(query, 3, &[], &[soa(900, 120)])))
    });
    let naming = Naming::proxy_servers(vec![server(), other()], Trust::RequireAd);
    let (asked, joined, counted) = asked_and_joined(&naming, &zone, "missing.test").await;
    assert_eq!(
        asked.error.as_ref().map(|error| error.failure),
        Some(LookupFailure::NoSuchName)
    );
    assert_eq!(asked.trust, None, "{asked:?}");
    assert_eq!(counted, []);
    assert_eq!(joined.error, asked.error);
    assert_eq!(joined.trust, None, "{joined:?}");
}

// A hop that cannot be reached fails the lookup as the hop failed, in
// words that read like a name not found. The caller that joined records
// the failure the lookup gave, not a reading of its words.
#[tokio::test(start_paused = true)]
async fn a_caller_that_joined_a_lookup_no_server_was_reached_for_records_no_reply() {
    let hop = Unreachable(
        "failed to lookup address information: nodename nor servname provided, or not known",
    );
    let naming = Naming::proxy_servers(vec![server()], Trust::RequireAd);
    let (asked, joined, counted) = asked_and_joined(&naming, &hop, "fixture.test").await;
    let error = asked.error.clone().expect("a failure");
    assert_eq!(error.failure, LookupFailure::NoReply, "{error:?}");
    assert_eq!(joined.error, Some(error));
    assert_eq!(asked.trust, None);
    assert_eq!(joined.trust, None);
    assert_eq!(counted, []);
}

#[tokio::test(start_paused = true)]
async fn a_lookup_past_the_naming_window_is_recorded_as_timed_out_and_judged_by_nobody() {
    let silent = Scripted::new(|_, _, _| None);
    let naming = Naming::proxy_servers(vec![server()], Trust::RequireAd);
    let before = counted();
    let resolution =
        through_naming_within(&naming, &silent, "fixture.test", Duration::from_secs(3)).await;
    let error = resolution.error.expect("a failure");
    assert_eq!(error.failure, LookupFailure::TimedOut);
    assert_eq!(
        error.detail,
        format!("{}: fixture.test", crate::pipe::RESOLVE_TIMED_OUT)
    );
    assert_eq!(resolution.trust, None);
    assert_eq!(since(&before), []);
}

#[tokio::test(start_paused = true)]
async fn a_refused_family_beside_an_authenticated_one_is_left_out_of_the_answer_and_the_cache() {
    let queries = Arc::new(AtomicUsize::new(0));
    let counted_queries = queries.clone();
    let zone = Zone::new(move |_, qtype, query| {
        counted_queries.fetch_add(1, Ordering::Relaxed);
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
    let naming = Naming::proxy_servers(vec![server()], Trust::RequireAd);
    for held in [false, true] {
        let resolution = through_naming(&naming, &zone, "fixture.test").await;
        assert_eq!(resolution.addresses, six, "held: {held}");
        assert_eq!(resolution.cached, held);
        assert_eq!(resolution.trust, Some(TrustOutcome::Authenticated));
    }
    assert_eq!(queries.load(Ordering::Relaxed), 2);
}
