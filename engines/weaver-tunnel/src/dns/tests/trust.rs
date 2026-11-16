// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::ttl::{ASKED, Zone, respond, rr, server, soa, v4};
use super::*;
use crate::pipe::{DialPath, Dialed, DialedStream, Naming};
use crate::resolution::{ResolutionCache, record::Recording};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

// No address of the asked family, and how long that holds.
fn nodata(query: &[u8]) -> Vec<u8> {
    respond(query, 0, &[], &[soa(600, 60)])
}

fn other() -> DnsServer {
    DnsServer::plain("192.0.2.54".parse().unwrap())
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
            let roots = TrustRoots::built_in();
            if let Ok(lookup) = resolve(zone, roots.wires(), servers, trust, host).await {
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
    // The hop stands for the carrier, and keeps what the carrier keeps.
    fn keepers(&self) -> Vec<crate::pipe::KeeperRef> {
        self.0.keepers()
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
        Naming::Host | Naming::Proxy | Naming::Refused(_) => {
            unreachable!("a naming that asks no servers")
        }
    }
}

// A naming that asks `servers` through the proxy, as
// `Naming::proxy_servers` gives, its TLS sessions and validator trusting
// `roots`.
pub(super) fn proxy_servers_trusting(
    servers: Vec<DnsServer>,
    trust: Trust,
    roots: &Arc<TrustRoots>,
) -> Naming {
    Naming::proxy_servers_trusting(servers, trust, roots.clone())
}

// The TLS sessions a naming that asks DNS servers resumes.
pub(super) fn sessions(naming: &Naming) -> &Arc<DnsSessions> {
    match naming {
        Naming::Servers { sessions, .. } | Naming::ProxyServers { sessions, .. } => sessions,
        Naming::Host | Naming::Proxy | Naming::Refused(_) => {
            unreachable!("a naming that asks no servers")
        }
    }
}

// How many trust outcomes this test's thread has counted, to take what
// its next lookups add. Each test runs on a runtime of one thread, so no
// other test's lookups are counted here.
pub(super) fn counted() -> usize {
    crate::metrics::TRUST_HERE.with_borrow(Vec::len)
}

// The outcomes this test's lookups counted since `before`, in order.
pub(super) fn since(before: &usize) -> Vec<TrustOutcome> {
    crate::metrics::TRUST_HERE.with_borrow(|judged| judged[*before..].to_vec())
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

#[tokio::test(start_paused = true)]
async fn a_query_taken_as_sent_asks_for_recursion_and_nothing_else() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let kept = seen.clone();
    let zone = Zone::new(move |_, _, query| {
        kept.lock().unwrap().push([query[2], query[3]]);
        v4(query, 60)
    });
    let roots = TrustRoots::built_in();
    resolve(
        &zone,
        roots.wires(),
        &[server()],
        Trust::AsSent,
        "fixture.test",
    )
    .await
    .unwrap();
    assert_eq!(*seen.lock().unwrap(), [[0x01, 0x00], [0x01, 0x00]]);
}

#[tokio::test(start_paused = true)]
async fn an_answer_taken_as_sent_is_the_first_servers() {
    let next = other().address.ip();
    let zone = Zone::new(move |asked, qtype, query| match (asked == next, qtype) {
        (false, _) => v4(query, 60),
        (true, 1) => respond(query, 0, &[rr(&ASKED, 1, 60, &[192, 0, 2, 9])], &[]),
        (true, _) => nodata(query),
    });
    let resolution = recorded(&zone, &[server(), other()], Trust::AsSent, "fixture.test").await;
    assert_eq!(
        resolution.addresses,
        ["192.0.2.7".parse::<IpAddr>().unwrap()]
    );
    assert_eq!(resolution.answered_by, Some(server().address.ip()));
}

#[tokio::test(start_paused = true)]
async fn the_record_says_how_the_answer_was_judged() {
    let zone = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => nodata(query),
    });
    let before = counted();
    let naming = Naming::proxy_servers(vec![server()], Trust::AsSent);
    let resolution = through_naming(&naming, &zone, "fixture.test").await;
    assert!(!resolution.cached);
    assert_eq!(resolution.trust, Some(TrustOutcome::AsSent));
    assert_eq!(since(&before), [TrustOutcome::AsSent]);
    assert_eq!(resolution.error, None);
    // Only an answer that came is said to have come over a wire.
    assert_eq!(resolution.wire, Some(WireKind::Plain));
}

#[tokio::test(start_paused = true)]
async fn a_held_answer_is_recorded_as_judged_when_it_was_asked() {
    let queries = Arc::new(AtomicUsize::new(0));
    let counted_queries = queries.clone();
    let zone = Zone::new(move |_, qtype, query| {
        counted_queries.fetch_add(1, Ordering::Relaxed);
        match qtype {
            1 => v4(query, 300),
            _ => nodata(query),
        }
    });
    let naming = Naming::proxy_servers(vec![server()], Trust::AsSent);
    let before = counted();
    let first = through_naming(&naming, &zone, "fixture.test").await;
    assert!(!first.cached);
    assert_eq!(first.trust, Some(TrustOutcome::AsSent));
    let held = through_naming(&naming, &zone, "fixture.test").await;
    // The second answer is the held one: no query went out for it, and
    // the metric counts the one lookup.
    assert_eq!(queries.load(Ordering::Relaxed), 2);
    assert_eq!(since(&before), [TrustOutcome::AsSent]);
    assert!(held.cached);
    assert_eq!(held.trust, Some(TrustOutcome::AsSent));
    assert_eq!(held.addresses, ["192.0.2.7".parse::<IpAddr>().unwrap()]);
    assert_eq!(held.error, None);
}

#[tokio::test(start_paused = true)]
async fn a_held_negative_answer_is_recorded_as_judged_when_it_was_asked() {
    let zone = Zone::new(|_, _, query| respond(query, 3, &[], &[soa(900, 120)]));
    let naming = Naming::proxy_servers(vec![server()], Trust::AsSent);
    let before = counted();
    for held in [false, true] {
        let resolution = through_naming(&naming, &zone, "missing.test").await;
        assert_eq!(resolution.cached, held);
        assert_eq!(resolution.trust, Some(TrustOutcome::AsSent));
        assert_eq!(
            resolution.error.map(|error| error.failure),
            Some(LookupFailure::NoSuchName)
        );
    }
    assert_eq!(since(&before), [TrustOutcome::AsSent]);
}

#[tokio::test(start_paused = true)]
async fn a_caller_that_joined_a_lookup_is_recorded_as_the_lookup_was_judged() {
    let zone = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => nodata(query),
    });
    let naming = Naming::proxy_servers(vec![server()], Trust::AsSent);
    let (asked, joined, counted) = asked_and_joined(&naming, &zone, "fixture.test").await;
    assert_eq!(asked.trust, Some(TrustOutcome::AsSent));
    assert_eq!(counted, [TrustOutcome::AsSent]);
    assert_eq!(joined.trust, Some(TrustOutcome::AsSent));
    assert_eq!(joined.addresses, asked.addresses);
    assert_eq!(asked.answered_by, Some(server().address.ip()));
    assert_eq!(joined.answered_by, asked.answered_by);
}

// A family whose query timed out leaves the answer it had unfinished: a
// negative answer from the other family is not an answer, and nothing
// was judged.
#[tokio::test(start_paused = true)]
async fn a_lookup_that_failed_beside_a_no_address_answer_is_judged_by_nobody() {
    let zone = Scripted::new(|_, qtype, query| match qtype {
        1 => Some(nodata(query)),
        _ => None,
    });
    let naming = Naming::proxy_servers(vec![server()], Trust::AsSent);
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
async fn a_lookup_that_failed_after_an_nxdomain_is_judged_by_nobody() {
    let first = server().address.ip();
    let zone = Scripted::new(move |asked, _, query| {
        (asked == first).then(|| respond(query, 3, &[], &[soa(900, 120)]))
    });
    let naming = Naming::proxy_servers(vec![server(), other()], Trust::AsSent);
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
    let naming = Naming::proxy_servers(vec![server()], Trust::AsSent);
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
    let naming = Naming::proxy_servers(vec![server()], Trust::AsSent);
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
