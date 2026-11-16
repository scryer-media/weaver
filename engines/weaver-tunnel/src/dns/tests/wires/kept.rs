// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Encrypted connections a naming keeps between its lookups.
use super::super::trust::{PROXY, WINDOW};
use super::*;
use crate::pipe::{
    DialPath, DnsChoice, Keeper, Names, NamesSlot, SessionHop, SessionSockets, SocketClass,
    SocketShares,
};
use std::time::Duration;

// One server at 192.0.2.53 on each encrypted wire, answering as `answer`
// says: its name, the servers, the authority their certificates come from,
// the server listed, and the connections a cold lookup of both families
// dials there.
fn encrypted(
    answer: impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + Clone + 'static,
) -> Vec<(&'static str, Servers, Authority, DnsServer, usize)> {
    [
        (
            "dot",
            Speaks::Dot { close_after: None },
            &b"dot"[..],
            dot("192.0.2.53", None),
            1,
        ),
        (
            "http/1.1",
            doh_fixture(plain_reply, false),
            b"http/1.1",
            doh("192.0.2.53:443"),
            2,
        ),
        (
            "h2",
            Speaks::H2 {
                respond: Arc::new(h2_message),
                goaway_after: None,
                misstep: H2Misstep::None,
            },
            b"h2",
            doh("192.0.2.53:443"),
            1,
        ),
    ]
    .into_iter()
    .map(|(wire, speaks, alpn, listed, cold)| {
        let authority = Authority::new();
        let servers = Servers::new(speaks, answer.clone())
            .at("192.0.2.53", authority.server(&["192.0.2.53"], alpn));
        (wire, servers, authority, listed, cold)
    })
    .collect()
}

// A naming's own sessions on `wires`, which keep its connections.
fn naming_on(wires: &Wires) -> DnsSessions {
    DnsSessions::of(wires.clone(), Dnssec::new(None))
}

// What a lookup of `host` at `listed` came to, by the naming that keeps its
// connections in `sessions`.
async fn kept_lookup(
    servers: &Servers,
    sessions: &DnsSessions,
    listed: &DnsServer,
    host: &str,
) -> record::HostResolution {
    kept_lookup_of(servers, sessions, std::slice::from_ref(listed), host).await
}

// As `kept_lookup`, with the servers `listed` in order.
async fn kept_lookup_of(
    servers: &dyn Carrier,
    sessions: &DnsSessions,
    listed: &[DnsServer],
    host: &str,
) -> record::HostResolution {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            record::asking(host, || record::Asker::servers(listed, Trust::AsSent));
            let (resolved, _) =
                resolve_judged(servers, sessions, listed, Trust::AsSent, host).await;
            if let Ok(lookup) = resolved {
                record::found(host, &lookup.addresses);
            }
        })
        .await;
    recording.finish(host, 80, None).expect("a resolution")
}

// Two lookups in a row: the first dials as a cold lookup does, and the
// second asks on the connection the first left, dialling nothing.
#[tokio::test(start_paused = true)]
async fn a_second_lookup_asks_on_the_connection_the_first_left() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let wires = authority.wires();
        let sessions = naming_on(&wires);
        let first = kept_lookup(&servers, &sessions, &listed, "a.test").await;
        assert_eq!(first.addresses, seven(), "{wire}");
        assert_eq!(servers.dials().len(), cold, "{wire}");
        assert_eq!(servers.answered().iter().sum::<usize>(), 2, "{wire}");
        // One connection is kept; any other the lookup opened is closed.
        assert_eq!(sessions.kept().len(), 1, "{wire}");
        assert_eq!(servers.open_now(), 1, "{wire}");

        let second = kept_lookup(&servers, &sessions, &listed, "b.test").await;
        assert_eq!(second.addresses, seven(), "{wire}");
        assert_eq!(second.error, None, "{wire}");
        assert_eq!(servers.dials().len(), cold, "{wire}");
        assert_eq!(servers.answered().iter().sum::<usize>(), 4, "{wire}");
        assert_eq!(sessions.kept().len(), 1, "{wire}");
        assert_eq!(servers.open_now(), 1, "{wire}");
    }
}

// A kept connection no lookup takes closes once `IDLE` passes, and not
// before: the server sees it go, and the next lookup dials again.
#[tokio::test(start_paused = true)]
async fn a_kept_connection_closes_once_idle_and_the_next_lookup_dials() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let wires = authority.wires();
        let sessions = naming_on(&wires);
        let first = kept_lookup(&servers, &sessions, &listed, "a.test").await;
        assert_eq!(first.addresses, seven(), "{wire}");
        let left = tokio::time::Instant::now();
        tokio::time::advance(IDLE - Duration::from_millis(1)).await;
        assert_eq!(sessions.kept().len(), 1, "{wire}");
        assert_eq!(servers.open_now(), 1, "{wire}");
        // Every connection the lookup dialled ends on the server's side.
        servers
            .ended
            .acquire_many(cold as u32)
            .await
            .unwrap()
            .forget();
        assert!(left.elapsed() >= IDLE, "{wire}");
        assert_eq!(sessions.kept().len(), 0, "{wire}");
        assert_eq!(servers.open_now(), 0, "{wire}");

        let again = kept_lookup(&servers, &sessions, &listed, "b.test").await;
        assert_eq!(again.addresses, seven(), "{wire}");
        assert_eq!(servers.dials().len(), 2 * cold, "{wire}");
    }
}

// The same wire as `listed`, at 192.0.2.54, where nothing listens: listed
// after it, a lookup asks it only when `listed` was charged a failure.
fn then_nothing(listed: &DnsServer) -> [DnsServer; 2] {
    let mut nothing = listed.clone();
    nothing.address.set_ip("192.0.2.54".parse().unwrap());
    [listed.clone(), nothing]
}

// Whether any lookup went on to the server listed after the first.
fn failed_over(servers: &Servers) -> bool {
    servers
        .dials()
        .iter()
        .any(|dial| dial.ip() == "192.0.2.54".parse::<IpAddr>().unwrap())
}

// The server closed the kept connection: the next lookup finds it gone,
// dials one new connection for it, and answers with no failure charged to
// the server, so the server listed after it is never asked.
#[tokio::test(start_paused = true)]
async fn a_kept_connection_the_server_closed_is_replaced_by_one_dial() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let wires = authority.wires();
        let sessions = naming_on(&wires);
        let listed = then_nothing(&listed);
        kept_lookup_of(&servers, &sessions, &listed, "a.test").await;
        servers.close_all();
        let again = kept_lookup_of(&servers, &sessions, &listed, "b.test").await;
        assert_eq!(again.addresses, seven(), "{wire}");
        assert_eq!(again.error, None, "{wire}");
        assert_eq!(again.answered_by, Some(listed[0].address.ip()), "{wire}");
        assert!(!failed_over(&servers), "{wire}");
        assert_eq!(servers.dials().len(), cold + 1, "{wire}");
    }
}

// The kept connection takes the query and never answers, as one whose NAT
// or tunnel state was dropped: after `REUSED_WAIT`, not the server's time
// limit, the lookup takes it as gone, drops it unread, and asks the same
// queries on one new connection, with no failure charged to the server.
#[tokio::test(start_paused = true)]
async fn a_silent_kept_connection_is_replaced_after_the_reused_wait() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let wires = authority.wires();
        let sessions = naming_on(&wires);
        let listed = then_nothing(&listed);
        kept_lookup_of(&servers, &sessions, &listed, "a.test").await;
        servers.mute_open();
        let started = tokio::time::Instant::now();
        let again = kept_lookup_of(&servers, &sessions, &listed, "b.test").await;
        // The lookup waited out `REUSED_WAIT` on the silent connection
        // before it dialled again.
        assert!(started.elapsed() >= REUSED_WAIT, "{wire}");
        assert_eq!(again.addresses, seven(), "{wire}");
        assert_eq!(again.error, None, "{wire}");
        assert_eq!(again.answered_by, Some(listed[0].address.ip()), "{wire}");
        assert!(!failed_over(&servers), "{wire}");
        assert_eq!(servers.dials().len(), cold + 1, "{wire}");
        // The silent connection is gone from this side; only the new one is
        // open, kept.
        assert_eq!(servers.open_now(), 1, "{wire}");
        assert_eq!(sessions.kept().len(), 1, "{wire}");
    }
}

// The server answers 408 Request Timeout on the kept HTTP/1.1 connection
// while it idles, and closes it, as a server whose idle timeout is shorter
// than ours does: the next lookup takes that as the connection gone, not as
// the server's answer, and asks on one new connection, with no failure
// charged to the server.
#[tokio::test(start_paused = true)]
async fn an_http_1_connection_the_server_timed_out_with_a_408_is_replaced_by_one_dial() {
    let (wire, servers, authority, listed, cold) = encrypted(answers)
        .into_iter()
        .find(|(wire, ..)| *wire == "http/1.1")
        .unwrap();
    let wires = authority.wires();
    let sessions = naming_on(&wires);
    let listed = then_nothing(&listed);
    kept_lookup_of(&servers, &sessions, &listed, "a.test").await;
    assert_eq!(sessions.kept().len(), 1);
    servers.hang_up_open();
    // Both connections the first lookup dialled have ended on the server's
    // side: the one it closed, and the kept one, after its 408.
    servers
        .ended
        .acquire_many(cold as u32)
        .await
        .unwrap()
        .forget();
    let asked_before = servers.answered().iter().sum::<usize>();
    let again = kept_lookup_of(&servers, &sessions, &listed, "b.test").await;
    assert_eq!(again.addresses, seven(), "{wire}");
    assert_eq!(again.error, None, "{wire}");
    assert_eq!(again.answered_by, Some(listed[0].address.ip()), "{wire}");
    assert!(!failed_over(&servers), "{wire}");
    assert_eq!(servers.dials().len(), cold + 1, "{wire}");
    // Both queries were answered on the one new connection.
    assert_eq!(servers.answered().iter().sum::<usize>(), asked_before + 2);
    assert_eq!(servers.answered().last(), Some(&2));
    assert_eq!(sessions.kept().len(), 1, "{wire}");
}

// A DNS over TLS server sends a reply to the first lookup again on the kept
// connection, ahead of the second lookup's own: the second passes it over
// and takes its own answers, on the kept connection.
#[tokio::test(start_paused = true)]
async fn a_late_reply_to_an_earlier_lookup_is_passed_over() {
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let (wires, listed) = (authority.wires(), dot("192.0.2.53", None));
    let sessions = naming_on(&wires);
    let first = kept_lookup(&servers, &sessions, &listed, "a.test").await;
    assert_eq!(first.addresses, seven());
    let replies_to_first = servers.answered().iter().sum::<usize>();
    servers.repeat_from_now();
    let second = kept_lookup(&servers, &sessions, &listed, "b.test").await;
    assert_eq!(second.addresses, seven());
    assert_eq!(second.error, None);
    // Each of the second lookup's two queries went out once, on the kept
    // connection.
    assert_eq!(servers.dials().len(), 1);
    assert_eq!(
        servers.answered().iter().sum::<usize>(),
        replies_to_first + 2
    );
    // A reply came that no query out could take, so the connection may
    // still owe one: it is not kept.
    assert_eq!(sessions.kept().len(), 0);
}

// The kept connection is gone and the dial that would replace it reaches
// nothing: the lookup fails as a cold lookup whose dial reaches nothing
// does, and dials no more than it.
#[tokio::test(start_paused = true)]
async fn a_kept_connection_found_gone_then_a_failed_dial_fails_as_a_cold_lookup() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let (servers, wires) = (servers.unreached_at(cold), authority.wires());
        let sessions = naming_on(&wires);
        kept_lookup(&servers, &sessions, &listed, "a.test").await;
        servers.close_all();
        let failed = kept_lookup(&servers, &sessions, &listed, "b.test").await;
        assert!(failed.addresses.is_empty(), "{wire}");
        assert_eq!(servers.dials().len(), cold + 1, "{wire}");

        let unreached = encrypted(answers)
            .into_iter()
            .find(|(cold_wire, ..)| *cold_wire == wire)
            .unwrap();
        let (_, cold_servers, cold_authority, ..) = unreached;
        let (cold_servers, cold_wires) = (cold_servers.unreached_at(0), cold_authority.wires());
        let cold_lookup =
            kept_lookup(&cold_servers, &naming_on(&cold_wires), &listed, "b.test").await;
        // The same failure; only the dial named differs.
        assert_eq!(
            failed.error.map(|error| error.failure),
            cold_lookup.error.map(|error| error.failure),
            "{wire}"
        );
        assert_eq!(cold_servers.dials().len(), 1, "{wire}");
    }
}

// A query that has its reply owed when the lookup ends leaves its
// connection unkept: a reply sent late can only reach a connection that is
// gone, and the next lookup asks on a new one.
#[tokio::test(start_paused = true)]
async fn a_connection_owing_a_reply_is_never_kept() {
    let silent_on_late = |asked: IpAddr, qtype: u16, query: &[u8]| {
        if query.windows(4).any(|name| name == b"late") {
            return Vec::new();
        }
        answers(asked, qtype, query)
    };
    for (wire, servers, authority, listed, cold) in encrypted(silent_on_late) {
        let wires = authority.wires();
        let sessions = naming_on(&wires);
        let late = kept_lookup(&servers, &sessions, &listed, "late.test").await;
        assert!(late.addresses.is_empty(), "{wire}");
        assert_eq!(sessions.kept().len(), 0, "{wire}");
        assert_eq!(servers.open_now(), 0, "{wire}");
        let dialled = servers.dials().len();
        assert_eq!(dialled, cold, "{wire}");

        let next = kept_lookup(&servers, &sessions, &listed, "fixture.test").await;
        assert_eq!(next.addresses, seven(), "{wire}");
        assert_eq!(next.error, None, "{wire}");
        assert_eq!(servers.dials().len(), dialled + cold, "{wire}");
    }
}

// A DNS over TLS reply with a query's id but another question: the
// connection may still owe that query's own reply, so it is not kept.
#[tokio::test(start_paused = true)]
async fn a_dot_connection_given_a_reply_to_another_question_is_not_kept() {
    let answer = |asked: IpAddr, qtype: u16, query: &[u8]| {
        let mut other = query.to_vec();
        if let Some(at) = other.windows(5).position(|name| name == b"stray") {
            other[at + 4] = b'x';
        }
        answers(asked, qtype, &other)
    };
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answer)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let (wires, listed) = (authority.wires(), dot("192.0.2.53", None));
    let sessions = naming_on(&wires);
    let stray = kept_lookup(&servers, &sessions, &listed, "stray.test").await;
    assert_eq!(stray.error, invalid());
    assert_eq!(sessions.kept().len(), 0);
    assert_eq!(servers.open_now(), 0);
}

// A DNS over TLS talk grown wary, its first connection closed under it
// with a query out, keeps nothing.
#[tokio::test(start_paused = true)]
async fn a_wary_dot_talk_keeps_no_connection() {
    let authority = Authority::new();
    let servers = Servers::new(
        Speaks::Dot {
            close_after: Some(1),
        },
        seven_then_six,
    )
    .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let (wires, listed) = (authority.wires(), dot("192.0.2.53", None));
    let sessions = naming_on(&wires);
    let resolution = kept_lookup(&servers, &sessions, &listed, "fixture.test").await;
    assert_eq!(resolution.addresses, seven_and_six());
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(sessions.kept().len(), 0);
    assert_eq!(servers.open_now(), 0);
}

// An HTTP/2 connection the server sent away is not kept, nor is any
// connection of the talk that grew wary under it.
#[tokio::test(start_paused = true)]
async fn an_http_2_connection_sent_away_is_not_kept() {
    for misstep in [H2Misstep::None, H2Misstep::GracefulAnswers] {
        let (servers, wires) = h2_servers_doing(h2_message, Some(1), misstep, answers);
        let sessions = naming_on(&wires);
        let listed = doh("192.0.2.53:443");
        let resolution = kept_lookup(&servers, &sessions, &listed, "fixture.test").await;
        assert_eq!(resolution.addresses, seven());
        assert_eq!(servers.dials().len(), 2);
        assert_eq!(sessions.kept().len(), 0);
        assert_eq!(servers.open_now(), 0);
    }
}

// An HTTP/2 connection whose streams read more than they may, here an
// endless run of interim responses, is not kept: what it still has to send
// would count against the next lookup's streams.
#[tokio::test(start_paused = true)]
async fn an_http_2_connection_that_overran_its_read_bound_is_not_kept() {
    let (servers, wires) = h2_servers_doing(h2_message, None, H2Misstep::Interims(20_000), answers);
    let sessions = naming_on(&wires);
    let listed = doh("192.0.2.53:443");
    let resolution = kept_lookup(&servers, &sessions, &listed, "fixture.test").await;
    assert_eq!(resolution.error, invalid(), "{resolution:?}");
    assert_eq!(servers.dials().len(), 1);
    assert_eq!(sessions.kept().len(), 0);
    assert_eq!(servers.open_now(), 0);
}

// An HTTP/1.1 connection the server does not keep alive is not kept.
#[tokio::test(start_paused = true)]
async fn an_http_1_connection_not_kept_alive_is_not_kept() {
    let closing = |_: &[u8], reply: Vec<u8>| {
        http(
            "200 OK",
            &[
                "content-type: application/dns-message",
                &format!("content-length: {}", reply.len()),
                "connection: close",
            ],
            &reply,
        )
    };
    let (servers, wires) = doh_servers(closing);
    let sessions = naming_on(&wires);
    let listed = doh("192.0.2.53:443");
    let resolution = kept_lookup(&servers, &sessions, &listed, "fixture.test").await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(sessions.kept().len(), 0);
    assert_eq!(servers.open_now(), 0);
}

// Two namings asking one server keep a connection apiece, each taken only
// by its own naming's lookups; a clone of a naming shares the naming's.
#[tokio::test(start_paused = true)]
async fn two_namings_to_one_server_keep_two_connections() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let roots = authority.roots();
        let one = proxy_servers_trusting(vec![listed.clone()], Trust::AsSent, &roots);
        let other = proxy_servers_trusting(vec![listed.clone()], Trust::AsSent, &roots);
        for (naming, host) in [(&one, "a.test"), (&other, "b.test")] {
            let resolution = through_naming(naming, &servers, host).await;
            assert_eq!(resolution.addresses, seven(), "{wire} {host}");
        }
        assert_eq!(servers.dials().len(), 2 * cold, "{wire}");
        assert_eq!(sessions(&one).kept().len(), 1, "{wire}");
        assert_eq!(sessions(&other).kept().len(), 1, "{wire}");
        assert_eq!(servers.open_now(), 2, "{wire}");
        // Each naming, and a clone of the first, asks on its own kept
        // connection.
        let clone = one.clone();
        for (naming, host) in [(&one, "c.test"), (&other, "d.test"), (&clone, "e.test")] {
            let resolution = through_naming(naming, &servers, host).await;
            assert_eq!(resolution.addresses, seven(), "{wire} {host}");
        }
        assert_eq!(servers.dials().len(), 2 * cold, "{wire}");
        assert_eq!(servers.open_now(), 2, "{wire}");
    }
}

// A naming dropped closes the connections it keeps at once, its idle
// timers with them; one still held by a clone keeps them.
#[tokio::test(start_paused = true)]
async fn dropping_a_naming_closes_its_kept_connections() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let naming = proxy_servers_trusting(vec![listed], Trust::AsSent, &authority.roots());
        let clone = naming.clone();
        through_naming(&naming, &servers, "a.test").await;
        drop(naming);
        assert_eq!(servers.open_now(), 1, "{wire}");
        drop(clone);
        assert_eq!(servers.open_now(), 0, "{wire}");
        servers
            .ended
            .acquire_many(cold as u32)
            .await
            .unwrap()
            .forget();
    }
}

// A route's DNS setting changes: fresh names take the slot, and the old
// names' kept connections close with them. Under unchanged names, a second
// lookup through the same hop asks on the connection kept before.
#[tokio::test(start_paused = true)]
async fn names_replaced_close_the_old_names_kept_connections() {
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let tunnel = [dot("192.0.2.53", None)];
    let choice = DnsChoice::Proxy {
        trust: Trust::AsSent,
        system_fallback: false,
    };
    let names = || Names::with_roots(choice.clone(), None, authority.roots());
    let slot = NamesSlot::new(names());
    let at_hop = |slot: &NamesSlot| slot.get().unwrap().at_hop("hop", Some(&tunnel));
    through_naming(&at_hop(&slot), &servers, "a.test").await;
    assert_eq!(servers.open_now(), 1);
    through_naming(&at_hop(&slot), &servers, "b.test").await;
    assert_eq!(servers.dials().len(), 1);
    // The setting changes: the connection closes as the names go, before
    // anything waits, and the server sees it end.
    slot.set(names());
    assert_eq!(servers.open_now(), 0);
    servers.ended.acquire().await.unwrap().forget();
    through_naming(&at_hop(&slot), &servers, "c.test").await;
    assert_eq!(servers.dials().len(), 2);
}

// A stage instance a naming's lookups go through: its own keeper, and the
// servers it carries the queries to.
struct Instance<'a> {
    servers: &'a Servers,
    keeper: Keeper,
}

impl<'a> Instance<'a> {
    fn of(servers: &'a Servers) -> Self {
        Self {
            servers,
            keeper: Keeper::default(),
        }
    }
}

#[async_trait::async_trait]
impl Carrier for Instance<'_> {
    fn keepers(&self) -> Vec<crate::pipe::KeeperRef> {
        vec![self.keeper.handle()]
    }
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        self.servers.open(server).await
    }
}

// A hop built anew under unchanged names, as a proxy edited makes: the new
// instance never takes the connection kept through the old one, which
// closes as the new one looks for it; the lookup dials on the new hop, and
// keeps its connection there.
#[tokio::test(start_paused = true)]
async fn a_hop_built_anew_never_takes_a_connection_the_old_one_kept() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let naming = proxy_servers_trusting(vec![listed], Trust::AsSent, &authority.roots());
        let old = Instance::of(&servers);
        through_naming(&naming, &old, "a.test").await;
        assert_eq!(servers.open_now(), 1, "{wire}");
        assert_eq!(old.keeper.holding(), 1, "{wire}");

        let new = Instance::of(&servers);
        let resolution = through_naming(&naming, &new, "b.test").await;
        assert_eq!(resolution.addresses, seven(), "{wire}");
        assert_eq!(servers.dials().len(), 2 * cold, "{wire}");
        servers
            .ended
            .acquire_many(cold as u32)
            .await
            .unwrap()
            .forget();
        assert_eq!(old.keeper.holding(), 0, "{wire}");
        assert_eq!(new.keeper.holding(), 1, "{wire}");
        assert_eq!(servers.open_now(), 1, "{wire}");

        // The same new instance asks on what it kept.
        through_naming(&naming, &new, "c.test").await;
        assert_eq!(servers.dials().len(), 2 * cold, "{wire}");
    }
}

// A tunnel provider that carries each dial to `servers`.
struct Forward(Arc<Servers>);

#[async_trait::async_trait]
impl crate::TunnelProvider for Forward {
    async fn dial(
        &self,
        host: &str,
        port: u16,
    ) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        let ip = host
            .parse()
            .map_err(|_| TunnelError::Engine(format!("not an address: {host}")))?;
        let server = DnsServer {
            address: SocketAddr::new(ip, port),
            wire: Wire::Plain,
        };
        self.0.open(&server).await
    }
    fn describe(&self) -> String {
        "forward".into()
    }
}

// The stage beneath a session hop that carries nothing itself.
struct Beneath;

#[async_trait::async_trait]
impl Dialer for Beneath {
    async fn dial(&self, _: &Target) -> Result<crate::pipe::Dialed, DialError> {
        Err(DialError::Skipped("nothing beneath".into()))
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "beneath".into()
    }
}

// A session hop, as the runtime builds one for a saved proxy, whose tunnel
// reaches `servers`, its sockets held to `shares` when given.
fn session_hop(servers: &Arc<Servers>, shares: Option<SocketShares>) -> SessionHop {
    SessionHop {
        sockets: Arc::new(shares.map_or_else(SessionSockets::default, SessionSockets::new)),
        capacity: Default::default(),
        activity: Default::default(),
        id: PROXY,
        provider: Arc::new(Forward(servers.clone())),
        inner: Arc::new(Beneath),
        transport: None,
        path: DialPath::default(),
        endpoint: None,
        timeout: Duration::from_secs(30),
        naming: Naming::Proxy,
        tunnel_dns: Vec::new(),
        resolver: None,
        forwarding: Default::default(),
        failure: Default::default(),
    }
}

// One lookup of `host` by `naming` through `hop`, in a recording of its own.
async fn through_hop(naming: &Naming, hop: &dyn Dialer, host: &str) -> record::HostResolution {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            let _ = naming.resolve(hop, PROXY, host, WINDOW).await;
        })
        .await;
    recording.finish(host, 80, None).expect("a resolution")
}

// A proxy edited: the runtime builds a new session hop and retires the old
// one once nothing else holds it. The connection kept through the old hop
// closes at once, the server sees it end, and the old session retires, held
// up by nothing kept. The next lookup dials on the new hop.
#[tokio::test(start_paused = true)]
async fn a_proxy_edited_closes_what_its_old_hop_kept_and_dials_on_the_new() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let servers = Arc::new(servers);
        let naming = proxy_servers_trusting(vec![listed], Trust::AsSent, &authority.roots());
        let old = session_hop(&servers, None);
        through_hop(&naming, &old, "a.test").await;
        assert_eq!(servers.open_now(), 1, "{wire}");

        let new = session_hop(&servers, None);
        assert!(old.retire_idle().await, "{wire}");
        assert_eq!(servers.open_now(), 0, "{wire}");
        assert_eq!(sessions(&naming).kept().len(), 0, "{wire}");
        servers
            .ended
            .acquire_many(cold as u32)
            .await
            .unwrap()
            .forget();
        drop(old);

        let resolution = through_hop(&naming, &new, "b.test").await;
        assert_eq!(resolution.addresses, seven(), "{wire}");
        assert_eq!(servers.dials().len(), 2 * cold, "{wire}");
        assert_eq!(servers.open_now(), 1, "{wire}");
    }
}

// A proxy turned off retires its hop: what was kept through it closes at
// once. Turned on again, a hop built anew dials afresh.
#[tokio::test(start_paused = true)]
async fn a_proxy_turned_off_closes_what_was_kept_and_on_again_dials_afresh() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let servers = Arc::new(servers);
        let naming = proxy_servers_trusting(vec![listed], Trust::AsSent, &authority.roots());
        let hop = session_hop(&servers, None);
        through_hop(&naming, &hop, "a.test").await;
        assert_eq!(servers.open_now(), 1, "{wire}");

        hop.retire().await;
        drop(hop);
        assert_eq!(servers.open_now(), 0, "{wire}");
        servers
            .ended
            .acquire_many(cold as u32)
            .await
            .unwrap()
            .forget();

        let again = session_hop(&servers, None);
        let resolution = through_hop(&naming, &again, "b.test").await;
        assert_eq!(resolution.addresses, seven(), "{wire}");
        assert_eq!(servers.dials().len(), 2 * cold, "{wire}");
    }
}

// The session's reserve share holds one socket, and a kept connection holds
// it: a claim of the share takes it at once, the kept connection closes,
// and the server sees it end.
#[tokio::test(start_paused = true)]
async fn a_claim_on_a_full_share_closes_a_kept_connection_and_succeeds() {
    let authority = Authority::new();
    let servers = Arc::new(
        Servers::new(Speaks::Dot { close_after: None }, answers)
            .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot")),
    );
    let naming = proxy_servers_trusting(
        vec![dot("192.0.2.53", None)],
        Trust::AsSent,
        &authority.roots(),
    );
    let shares = SocketShares {
        nntp: 1,
        torrent: 1,
        core: 1,
        http: 1,
    };
    let hop = session_hop(&servers, Some(shares));
    through_hop(&naming, &hop, "a.test").await;
    assert_eq!(servers.open_now(), 1);
    assert_eq!(hop.sockets.held(SocketClass::ReserveCore), 1);

    let claim = hop.sockets.take(SocketClass::ReserveCore);
    assert!(matches!(claim, Ok(Some(_))));
    assert_eq!(servers.open_now(), 0);
    assert_eq!(sessions(&naming).kept().len(), 0);
    servers.ended.acquire().await.unwrap().forget();
    drop(claim);

    // The next lookup dials, and keeps its connection again.
    let resolution = through_hop(&naming, &hop, "b.test").await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(hop.sockets.held(SocketClass::ReserveCore), 1);
}

// A SOCKS5 hop stacked on a session hop, as the runtime builds a proxy
// chain: a lookup asked through it keeps its connection with the keepers
// both stages name, the hop's own first.
struct Stacked {
    servers: Arc<Servers>,
    beneath: Arc<SessionHop>,
    above: crate::pipe::TransportHop,
}

impl Stacked {
    fn on(servers: &Arc<Servers>) -> Self {
        let beneath = Arc::new(session_hop(servers, None));
        let above = crate::pipe::TransportHop::new(
            PROXY + 1,
            crate::transport::TransportProxy {
                kind: crate::transport::TransportKind::Socks5,
                host: "192.0.2.80".into(),
                port: 1080,
                username: None,
                password: None,
            },
            beneath.clone(),
            Duration::from_secs(30),
            Naming::Proxy,
            Arc::new(NamesSlot::new(Names::new(DnsChoice::System, None))),
            Default::default(),
        );
        Self {
            servers: servers.clone(),
            beneath,
            above,
        }
    }
}

#[async_trait::async_trait]
impl Carrier for Stacked {
    fn keepers(&self) -> Vec<crate::pipe::KeeperRef> {
        self.above.keepers()
    }
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        self.servers.open(server).await
    }
}

// The hop on top retired, as a proxy turned off: what was kept through it
// closes at once, the server sees it end, and the stage beneath holds
// nothing for it either.
#[tokio::test(start_paused = true)]
async fn a_transport_hop_retired_closes_what_was_kept_through_it() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let servers = Arc::new(servers);
        let naming = proxy_servers_trusting(vec![listed], Trust::AsSent, &authority.roots());
        let stacked = Stacked::on(&servers);
        through_naming(&naming, &stacked, "a.test").await;
        assert_eq!(sessions(&naming).kept().len(), 1, "{wire}");
        assert_eq!(stacked.beneath.sockets.keeper().holding(), 1, "{wire}");

        stacked.above.retire().await;
        assert_eq!(sessions(&naming).kept().len(), 0, "{wire}");
        assert_eq!(servers.open_now(), 0, "{wire}");
        assert_eq!(stacked.beneath.sockets.keeper().holding(), 0, "{wire}");
        servers
            .ended
            .acquire_many(cold as u32)
            .await
            .unwrap()
            .forget();
    }
}

// The session beneath the hop retired, as its own proxy edited: the
// connection kept through the hop above closes at once too, and the hop
// above holds nothing for it.
#[tokio::test(start_paused = true)]
async fn a_stage_beneath_retired_closes_what_was_kept_through_the_hop_above() {
    for (wire, servers, authority, listed, cold) in encrypted(answers) {
        let servers = Arc::new(servers);
        let naming = proxy_servers_trusting(vec![listed], Trust::AsSent, &authority.roots());
        let stacked = Stacked::on(&servers);
        through_naming(&naming, &stacked, "a.test").await;
        assert_eq!(sessions(&naming).kept().len(), 1, "{wire}");

        stacked.beneath.retire().await;
        assert_eq!(sessions(&naming).kept().len(), 0, "{wire}");
        assert_eq!(servers.open_now(), 0, "{wire}");
        servers
            .ended
            .acquire_many(cold as u32)
            .await
            .unwrap()
            .forget();
        // Nothing stays registered with the hop above: through it again, the
        // lookup dials afresh and keeps one connection, held by both.
        through_naming(&naming, &stacked, "b.test").await;
        assert_eq!(servers.dials().len(), 2 * cold, "{wire}");
        assert_eq!(sessions(&naming).kept().len(), 1, "{wire}");
        assert_eq!(stacked.beneath.sockets.keeper().holding(), 1, "{wire}");
    }
}

// A stage dropped after its runtime ended, as at shutdown: what it kept
// closes then, with no runtime to close it on. A runtime started later
// keeps again, and closes what it keeps once idle.
#[test]
fn a_keeper_dropped_outside_a_runtime_closes_what_it_kept() {
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap()
    };
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let (wires, listed) = (authority.wires(), dot("192.0.2.53", None));
    let sessions = naming_on(&wires);
    let stage = Instance::of(&servers);
    runtime().block_on(async {
        let resolution =
            kept_lookup_of(&stage, &sessions, std::slice::from_ref(&listed), "a.test").await;
        assert_eq!(resolution.addresses, seven());
    });
    assert_eq!(sessions.kept().len(), 1);
    assert_eq!(servers.open_now(), 1);
    drop(stage);
    assert_eq!(sessions.kept().len(), 0);
    assert_eq!(servers.open_now(), 0);

    let stage = Instance::of(&servers);
    runtime().block_on(async {
        kept_lookup_of(&stage, &sessions, std::slice::from_ref(&listed), "b.test").await;
        assert_eq!(servers.dials().len(), 2);
        assert_eq!(sessions.kept().len(), 1);
        // The second dial's connection ends on the server's side once idle.
        servers.ended.acquire().await.unwrap().forget();
        assert_eq!(sessions.kept().len(), 0);
    });
}
