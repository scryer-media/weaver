// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved from weaver-server-pipeline proxies/network_torrent.rs: the leg's
// tracker clients and names are the torrents crate's.

use std::net::{IpAddr, SocketAddr};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weaver_torrent::network::{Resolved, Resolver};
use weaver_torrent::{ClientShape, HttpClients, HttpTarget};
use weaver_tunnel::pipe::DialedStream;
use weaver_tunnel::resolution::record::{Recording, ResolvedBy};

use std::collections::HashMap;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::proxies::*;
use crate::torrents::net::{LegKind, LegNames, tracker_client};
use weaver_tunnel::pipe::{DialError, Dialer, Revocable, Target};
use weaver_tunnel::{
    bridge::Bridge,
    pipe::{Dialed, Purpose, Resolution},
};

// A leg whose naming answers `answer` for every name, or hands names on
// to its exit, and whose exit is the loopback tracker at `exit`.
struct Leg {
    answer: Option<IpAddr>,
    exit: SocketAddr,
    asked: Mutex<Vec<String>>,
    dialed: Mutex<Vec<Target>>,
}

#[async_trait::async_trait]
impl Dialer for Leg {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.dialed.lock().unwrap().push(target.clone());
        let stream = tokio::net::TcpStream::connect(self.exit)
            .await
            .map_err(|e| DialError::Skipped(e.to_string()))?;
        Ok(Dialed {
            stream: DialedStream::Socket(stream),
            outcome: Arc::default(),
            path: Default::default(),
            peer: None,
            source: None,
            setup: None,
        })
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.asked.lock().unwrap().push(host.into());
        Ok(self
            .answer
            .map_or(Resolution::Unresolved, |ip| Resolution::Addresses(vec![ip])))
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(60)
    }
    fn describe(&self) -> String {
        "leg".into()
    }
}

fn host_lookups() -> u64 {
    let metrics = weaver_tunnel::metrics::snapshot();
    let resolved: u64 = metrics
        .resolutions
        .iter()
        .filter(|(by, ..)| *by == weaver_tunnel::metrics::Resolver::Host)
        .map(|(_, ok, failed)| ok + failed)
        .sum();
    let held: u64 = metrics
        .cache
        .iter()
        .filter(|(by, ..)| *by == weaver_tunnel::metrics::Resolver::Host)
        .map(|(.., count)| count)
        .sum();
    resolved + held
}

// A tracker that answers one announce.
async fn tracker() -> (SocketAddr, tokio::task::JoinHandle<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut buf = [0u8; 1024];
        while !head.ends_with(b"\r\n\r\n") {
            let read = stream.read(&mut buf).await.unwrap();
            assert!(read > 0, "the request ended early");
            head.extend_from_slice(&buf[..read]);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .unwrap();
        String::from_utf8(head).unwrap()
    });
    (addr, served)
}

// An announce to a tracker by name on a ladder leg, as the session makes
// it: the leg resolves the name by its naming, and the client is held to
// what that gave. Returns what the leg was asked and what it dialled.
async fn announce(answer: Option<IpAddr>) -> (Vec<String>, Vec<Target>) {
    let (exit, served) = tracker().await;
    let leg = Arc::new(Leg {
        answer,
        exit,
        asked: Mutex::default(),
        dialed: Mutex::default(),
    });
    let path = Arc::new(TorrentProvider::new(leg.clone(), Purpose::TorrentHttp));
    let bridge = PathBridge {
        bridge: Bridge::start(
            &tokio::runtime::Handle::current(),
            path.clone(),
            leg.budget(),
            ("weaver".into(), "bridge".into()),
        )
        .unwrap(),
        path,
        keep: None,
    };
    let port = exit.port();
    let addrs: Vec<IpAddr> = match LegNames(leg.clone())
        .resolve("tracker.test", port)
        .await
        .unwrap()
    {
        Resolved::Addresses(found) => found.iter().map(SocketAddr::ip).collect(),
        Resolved::AtExit => Vec::new(),
    };
    let client = tracker_client(&LegKind::Ladder, leg.clone(), Some(bridge.clone()), None)
        .unwrap()
        .client(
            Some(HttpTarget {
                host: "tracker.test",
                port,
                addrs: &addrs,
            }),
            &ClientShape::FETCH,
        )
        .unwrap();
    let response = client
        .get(format!("http://tracker.test:{port}/announce"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let head = served.await.unwrap();
    assert!(
        head.to_ascii_lowercase()
            .contains(&format!("host: tracker.test:{port}\r\n")),
        "{head}"
    );
    bridge.revoke().await;
    let asked = std::mem::take(&mut *leg.asked.lock().unwrap());
    let dialed = std::mem::take(&mut *leg.dialed.lock().unwrap());
    (asked, dialed)
}

// A ladder rung that answers every name with `answer`, and either
// refuses every dial or reaches the loopback tracker at `exit`.
struct OwnNaming {
    answer: Vec<IpAddr>,
    exit: Option<SocketAddr>,
    asked: Mutex<Vec<String>>,
    dialed: Mutex<Vec<Target>>,
}

impl OwnNaming {
    fn new(answer: &[[u8; 4]], exit: Option<SocketAddr>) -> Arc<Self> {
        Arc::new(Self {
            answer: answer.iter().copied().map(IpAddr::from).collect(),
            exit,
            asked: Mutex::default(),
            dialed: Mutex::default(),
        })
    }
}

#[async_trait::async_trait]
impl Dialer for OwnNaming {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.dialed.lock().unwrap().push(target.clone());
        let Some(exit) = self.exit else {
            return Err(DialError::Egress(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "fixture refusal",
            )));
        };
        let stream = tokio::net::TcpStream::connect(exit)
            .await
            .map_err(|e| DialError::Skipped(e.to_string()))?;
        Ok(Dialed {
            stream: DialedStream::Socket(stream),
            outcome: Arc::default(),
            path: Default::default(),
            peer: None,
            source: None,
            setup: None,
        })
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.asked.lock().unwrap().push(host.into());
        Ok(Resolution::Addresses(self.answer.clone()))
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(60)
    }
    fn describe(&self) -> String {
        "own naming".into()
    }
}

// A tracker announced to by name through the bridge of a two-rung
// ladder whose first rung refuses: the name reaches the ladder with the
// first rung's checked answer pinned, and the second rung asks its own
// naming and dials only the address both answers hold.
#[tokio::test]
async fn a_lower_rung_under_the_bridge_dials_only_what_its_own_naming_shares() {
    let (exit, served) = tracker().await;
    let first = OwnNaming::new(&[[127, 0, 0, 1], [127, 0, 0, 2]], None);
    let second = OwnNaming::new(&[[127, 0, 0, 2], [127, 0, 0, 3]], Some(exit));
    let ladder: Arc<dyn Dialer> = Arc::new(weaver_tunnel::pipe::Fallback::new(vec![
        first.clone(),
        second.clone(),
    ]));
    let path = Arc::new(TorrentProvider::new(ladder.clone(), Purpose::TorrentHttp));
    let bridge = PathBridge {
        bridge: Bridge::start(
            &tokio::runtime::Handle::current(),
            path.clone(),
            ladder.budget(),
            ("weaver".into(), "bridge".into()),
        )
        .unwrap(),
        path,
        keep: None,
    };
    let port = exit.port();
    let Resolved::Addresses(found) = LegNames(ladder.clone())
        .resolve("tracker.test", port)
        .await
        .unwrap()
    else {
        panic!("the first rung resolves names");
    };
    let addrs: Vec<IpAddr> = found.iter().map(SocketAddr::ip).collect();
    let client = tracker_client(&LegKind::Ladder, ladder, Some(bridge.clone()), None)
        .unwrap()
        .client(
            Some(HttpTarget {
                host: "tracker.test",
                port,
                addrs: &addrs,
            }),
            &ClientShape::FETCH,
        )
        .unwrap();
    let response = client
        .get(format!("http://tracker.test:{port}/announce"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    served.await.unwrap();
    bridge.revoke().await;

    let [refused] = &first.dialed.lock().unwrap()[..] else {
        panic!("the first rung dials once");
    };
    assert_eq!(refused.host, "tracker.test");
    assert_eq!(refused.addresses, addrs);
    assert_eq!(*first.asked.lock().unwrap(), ["tracker.test"]);
    assert_eq!(*second.asked.lock().unwrap(), ["tracker.test"]);
    let [carried] = &second.dialed.lock().unwrap()[..] else {
        panic!("the second rung dials once");
    };
    assert_eq!(carried.host, "tracker.test");
    assert_eq!(carried.addresses, [IpAddr::from([127, 0, 0, 2])]);
}

// Host, Servers and Proxy-servers naming each answer the leg's lookup
// with addresses: the session checks those and pins them, and the
// bridge is handed the name, which the leg dials at the checked address.
// The host's resolver is never asked.
#[tokio::test]
async fn a_name_the_leg_resolves_is_dialled_at_the_checked_address() {
    let before = host_lookups();
    for naming in ["host", "servers", "proxy servers"] {
        let checked = IpAddr::from([127, 0, 0, 1]);
        let (asked, dialed) = announce(Some(checked)).await;
        assert_eq!(asked, ["tracker.test"], "{naming}");
        let [target] = &dialed[..] else {
            panic!("{naming}: dialled {dialed:?}");
        };
        assert_eq!(target.addresses, [checked], "{naming}");
        assert_eq!(target.host, "tracker.test", "{naming}");
        assert!(matches!(target.purpose, Purpose::TorrentHttp), "{naming}");
    }
    assert_eq!(host_lookups(), before);
}

// Proxy naming hands the name on: the leg is asked once, no address is
// pinned, the bridge is handed the name, and no host lookup is made.
#[tokio::test]
async fn a_name_the_proxy_resolves_is_handed_through_with_no_lookup() {
    let before = host_lookups();
    let (asked, dialed) = announce(None).await;
    assert_eq!(asked, ["tracker.test"]);
    let [target] = &dialed[..] else {
        panic!("dialled {dialed:?}");
    };
    assert_eq!(target.host, "tracker.test");
    assert!(target.addresses.is_empty(), "{:?}", target.addresses);
    assert_eq!(host_lookups(), before);
}

// A runtime whose default torrent scope rides a ladder through a SOCKS5
// proxy, on DNS profile 1, where `who` answers its names. The proxy is
// never dialled: only names are looked up on the leg.
fn ladder_runtime(who: crate::proxies::DnsWho) -> (Arc<NetworkRuntime>, Configuration) {
    let runtime = NetworkRuntime::new(
        crate::Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let consumer = Consumer::Torrent(TorrentScope::Default);
    let mut config = runtime.configuration.read().unwrap().clone();
    config.profiles.insert(
        1,
        ProxyProfile {
            dns_profile_id: 0,
            id: 1,
            name: "socks".into(),
            kind: ProxyKind::Socks5,
            enabled: true,
            host: "127.0.0.1".into(),
            port: 1,
            dns_servers: vec![],
            tunnel_addresses: vec![],
            peer_public_key: None,
            mtu: 1420,
            keepalive_seconds: None,
            timeout_seconds: 1,
            host_key_fingerprint: None,
            natpmp: false,
            forwarded_port: None,
            revision: 1,
            secrets: Default::default(),
        },
    );
    config.dns = Arc::new(dns_with(who));
    config.consumers.insert(consumer.key());
    config.policies.insert(
        consumer.key(),
        RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: SYSTEM_EGRESS_ID,
                weight: 100,
                path: LegPath::Ladder {
                    rungs: vec![Rung::Proxy { id: 1 }],
                    direct_fallback: false,
                },
            }],
            dns_profile_id: Some(1),
            ..Default::default()
        },
    );
    runtime.apply_configuration(config.clone()).unwrap();
    (runtime, config)
}

// The saved DNS with one profile, of id 1, where `who` answers.
fn dns_with(who: crate::proxies::DnsWho) -> crate::proxies::StoredDns {
    let profile = crate::proxies::DnsProfile {
        kind: crate::proxies::DnsProfileKind::Route,
        id: 1,
        name: "Leg".into(),
        who,
        transit: None,
        trust: crate::proxies::DnsTrust::AsSent,
        server_list_id: None,
        system_fallback: false,
    };
    crate::proxies::StoredDns {
        profiles: HashMap::from([(1, profile)]),
        ..Default::default()
    }
}

// What an announce to `host` on a leg does before it sends: the name
// looked up on the leg, recorded, and the client held to what it gave.
async fn reach_on(
    names: &LegNames,
    clients: &dyn HttpClients,
    host: &str,
    port: u16,
) -> (
    Option<weaver_tunnel::resolution::record::HostResolution>,
    Vec<IpAddr>,
) {
    let recording = Arc::new(Recording::default());
    let addrs: Vec<IpAddr> = match recording.scope(names.resolve(host, port)).await.unwrap() {
        Resolved::Addresses(found) => found.iter().map(SocketAddr::ip).collect(),
        Resolved::AtExit => Vec::new(),
    };
    clients
        .client(
            Some(HttpTarget {
                host,
                port,
                addrs: &addrs,
            }),
            &ClientShape::of(weaver_torrent::ClientUse::Tracker { title_case: false }),
        )
        .unwrap();
    (recording.finish(host, port, None), addrs)
}

// Two announces to one tracker host on one leg whose naming is Weaver's:
// the first looks the name up, the second is answered by what the leg's
// naming holds, and both ride the one client built for that host.
#[tokio::test]
async fn two_announces_to_one_host_on_a_leg_resolve_once_and_share_a_client() {
    let (runtime, _) = ladder_runtime(crate::proxies::DnsWho::System);
    let leg = runtime
        .torrent_leg(TorrentScope::Default, 0, 8, Duration::from_secs(30))
        .unwrap();
    let names = LegNames(leg.dialer());
    let clients = crate::torrents::net::LegClients::new(
        || Ok(reqwest::Client::builder().no_proxy()),
        Some(leg.bridge().unwrap()),
        LegKind::Ladder,
        leg.dialer(),
    );
    let (first, first_addrs) = reach_on(&names, &clients, "localhost", 6969).await;
    let (second, second_addrs) = reach_on(&names, &clients, "localhost", 6969).await;
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(first.by, ResolvedBy::Host);
    assert!(!first.cached, "the first announce looks the name up");
    assert!(second.cached, "the second is answered from the held answer");
    assert!(!first_addrs.is_empty());
    assert_eq!(first_addrs, second_addrs);
    assert_eq!(clients.built(), 1);
    runtime.shutdown().await;
}

// The route's DNS profile edited from the host answering to the proxy
// answering keeps the leg the session opened on, and its next announce
// hands the name to the proxy: it reuses neither the answer the old
// naming held nor the client held to that answer's addresses. The
// host's resolver is not asked on the proxied leg.
#[tokio::test]
async fn a_resolver_edit_from_weaver_to_proxy_reuses_no_answer_or_client() {
    let (runtime, mut config) = ladder_runtime(crate::proxies::DnsWho::System);
    let timeout = Duration::from_secs(30);
    let leg = runtime
        .torrent_leg(TorrentScope::Default, 0, 8, timeout)
        .unwrap();
    let clients = crate::torrents::net::LegClients::new(
        || Ok(reqwest::Client::builder().no_proxy()),
        Some(leg.bridge().unwrap()),
        LegKind::Ladder,
        leg.dialer(),
    );
    let (held, _) = reach_on(&LegNames(leg.dialer()), &clients, "localhost", 6969).await;
    assert_eq!(held.unwrap().by, ResolvedBy::Host);
    assert_eq!(clients.built(), 1);

    config.dns = Arc::new(dns_with(crate::proxies::DnsWho::Proxy));
    runtime.apply_configuration(config).unwrap();
    assert!(!leg.stage.is_revoked());
    let again = runtime
        .torrent_leg(TorrentScope::Default, 0, 8, timeout)
        .unwrap();
    assert!(Arc::ptr_eq(&leg.stage, &again.stage));

    let before = host_lookups();
    let (handed, addrs) = reach_on(&LegNames(leg.dialer()), &clients, "localhost", 6969).await;
    let handed = handed.unwrap();
    assert_eq!(handed.by, ResolvedBy::Proxy);
    assert!(!handed.cached);
    assert!(addrs.is_empty(), "{addrs:?}");
    assert_eq!(clients.built(), 2);
    assert_eq!(host_lookups(), before);
    runtime.shutdown().await;
}

// A name the session looks up on its leg, a UDP tracker's or a web
// seed's alike, lands in the resolution record of the task that asked.
#[tokio::test]
async fn a_session_lookup_lands_in_the_resolution_record() {
    let recording = Arc::new(Recording::default());
    let resolved = recording
        .scope(LegNames(crate::torrents::net::system_dialer()).resolve("localhost", 6969))
        .await
        .unwrap();
    let Resolved::Addresses(found) = resolved else {
        panic!("the host resolver handed the name on");
    };
    let noted = recording
        .finish("localhost", 6969, None)
        .expect("the lookup was recorded");
    assert_eq!(noted.by, ResolvedBy::Host);
    // The record keeps the host's order; the leg hands its answer on
    // sorted.
    let mut noted = noted.addresses;
    noted.sort();
    assert_eq!(noted, found.iter().map(SocketAddr::ip).collect::<Vec<_>>());
}

// A member on a stage that has no names refuses every lookup: a name
// only the host knows does not resolve.
#[tokio::test]
async fn a_member_on_a_stage_without_names_refuses_lookups() {
    let system = crate::torrents::net::system_dialer();
    let stage = Revocable::new(Arc::clone(&system));
    let member = Revocable::named(system, member_names(&stage));
    let refused = member.resolve("localhost").await.unwrap_err();
    assert!(
        refused.to_string().contains("names by no DNS profile"),
        "{refused}"
    );
}
