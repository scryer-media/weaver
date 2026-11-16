// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The connection one server's queries ride during one lookup. Plain DNS
// opens a stream per query. DNS over TLS (RFC 7858) and over HTTPS (RFC
// 8484) wrap the stream the stage dials in TLS, and keep that one
// connection for the lookup's queries while the server keeps it open.
use super::{Carrier, DnsServer, Failed, Wire, dnssec::Dnssec, https::Connection};
use crate::TunnelStream;
use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{
        CertificateError, ClientConfig, RootCertStore, client::Resumption, crypto::aws_lc_rs,
        pki_types::ServerName,
    },
};

// The largest reply taken on any wire.
pub(super) const REPLY_CAP: usize = 16384;

// TLS sessions one naming keeps for resumption.
const SESSIONS: usize = 32;

// The TLS settings of each encrypted wire.
pub(super) struct Wires {
    dot: Arc<ClientConfig>,
    doh: Arc<ClientConfig>,
}

impl Wires {
    pub(super) fn trusting(roots: RootCertStore) -> Self {
        let roots = Arc::new(roots);
        let config = |alpn: &[&[u8]]| {
            let mut config =
                ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
                    .with_safe_default_protocol_versions()
                    .expect("aws-lc-rs supports Rustls safe default protocol versions")
                    .with_root_certificates(roots.clone())
                    .with_no_client_auth();
            config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
            Arc::new(config)
        };
        Self {
            dot: config(&[b"dot"]),
            // HTTP/2 when the server takes it, else HTTP/1.1.
            doh: config(&[b"h2", b"http/1.1"]),
        }
    }

    // The same settings with a session store of their own. The provider,
    // roots and verifier stay shared; only resumption state is apart.
    pub(super) fn apart(&self) -> Self {
        let fresh = |config: &Arc<ClientConfig>| {
            let mut config = ClientConfig::clone(config);
            config.resumption = Resumption::in_memory_sessions(SESSIONS);
            Arc::new(config)
        };
        Self {
            dot: fresh(&self.dot),
            doh: fresh(&self.doh),
        }
    }
}

// The web PKI roots: the only trust a product build has. Built once; every
// naming takes its own copy through `DnsSessions`.
pub(super) static WIRES: LazyLock<Wires> = LazyLock::new(|| {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Wires::trusting(roots)
});

// The TLS sessions one naming resumes. A ticket from a server asked on one
// route is never offered to it on another, which would tie the two routes
// together in the clear ClientHello. Built on first use, kept with the
// naming's answers, and dropped with them when the hop is rebuilt. The
// naming's DNSSEC validator lives beside them, for the same span.
#[derive(Default)]
pub struct DnsSessions {
    wires: OnceLock<Wires>,
    dnssec: Dnssec,
}

impl DnsSessions {
    pub(super) fn wires(&self) -> &Wires {
        self.wires.get_or_init(|| WIRES.apart())
    }
    pub(super) fn dnssec(&self) -> &Dnssec {
        &self.dnssec
    }
    // Trusts what `wires` trusts, for a test's own authority. Only once: a
    // store set before panics, so two namings sharing one would show.
    #[cfg(test)]
    pub(super) fn trust(&self, wires: &Wires) {
        assert!(self.wires.set(wires.apart()).is_ok(), "sessions set twice");
    }
}

pub(super) type Tls = TlsStream<Box<dyn TunnelStream>>;

// One query's reply, or why it got none.
pub(super) type Reply = Result<Vec<u8>, Failed>;

enum Link {
    Tls(Box<Tls>),
    Https(Box<Connection>),
}

// One server, asked for one lookup.
pub(super) struct Session<'a> {
    provider: &'a dyn Carrier,
    wires: &'a Wires,
    server: &'a DnsServer,
    link: Option<Link>,
    // Each query sent is counted as a DNSSEC query.
    counts: bool,
}

impl<'a> Session<'a> {
    pub(super) fn new(provider: &'a dyn Carrier, wires: &'a Wires, server: &'a DnsServer) -> Self {
        Self {
            provider,
            wires,
            server,
            link: None,
            counts: false,
        }
    }

    pub(super) fn server(&self) -> &DnsServer {
        self.server
    }

    // From now on every query this session sends checks DNSSEC, and is
    // counted when it goes out.
    pub(super) fn counting(&mut self) {
        self.counts = true;
    }

    fn sent(&self, queries: usize) {
        sent(self.counts, queries);
    }

    // Sends `packet` and returns the reply. `reached` is set once the
    // server answers the dial, or a kept connection is used.
    //
    // A query rides the kept connection when there is one, else a new one.
    // A kept connection found dead before any byte of the reply came is
    // replaced once. So one query opens at most two connections, and a
    // server that closes after every reply costs one per query, as plain
    // DNS does.
    pub(super) async fn ask(
        &mut self,
        packet: &[u8],
        reached: &mut bool,
    ) -> Result<Vec<u8>, Failed> {
        if self.server.wire == Wire::Plain {
            let mut stream = self
                .provider
                .open(self.server)
                .await
                .map_err(Failed::Unreached)?;
            *reached = true;
            self.sent(1);
            return framed(&mut stream, packet, plain_failed).await;
        }
        // The link is out of its slot while in use, so a query cut off part
        // way leaves no half-used connection behind.
        if let Some(mut link) = self.link.take() {
            *reached = true;
            self.sent(1);
            match link.exchange(self.server, packet).await {
                Ok(reply) => return Ok(self.keep(link, reply)),
                Err(Failed::Closed) => {}
                Err(failed) => return Err(failed),
            }
        }
        let link = self.connect(reached).await?;
        self.first(link, packet).await
    }

    // The first query on a connection just made. Unlike a kept one, it is
    // not replaced if it closes before the reply.
    async fn first(&mut self, mut link: Link, packet: &[u8]) -> Reply {
        self.sent(1);
        let reply = link.exchange(self.server, packet).await?;
        Ok(self.keep(link, reply))
    }

    // A talk with the server: queries given one at a time, each sent as it
    // is given while others are still out, and each reply handed back as it
    // comes. `reached` is set once the server answers a dial. The talk's
    // connections are its own, and go when it is dropped.
    pub(super) fn talk<'s>(&'s mut self, reached: &'s mut bool) -> Talk<'s, 'a> {
        let flight = match self.server.wire {
            Wire::Plain => Flight::Plain {
                calls: Vec::new(),
                dialled: Arc::new(AtomicBool::new(false)),
            },
            Wire::Tls { .. } => Flight::Tls {
                stream: None,
                answered: 0,
                out: BTreeMap::new(),
                wary: false,
            },
            Wire::Https { .. } => Flight::Https(Box::default()),
        };
        Talk {
            session: self,
            reached,
            waiting: VecDeque::new(),
            ready: VecDeque::new(),
            flight,
        }
    }

    fn keep(&mut self, link: Link, (reply, open): (Vec<u8>, bool)) -> Vec<u8> {
        if open {
            self.link = Some(link);
        }
        reply
    }

    async fn connect(&mut self, reached: &mut bool) -> Result<Link, Failed> {
        dial(self.provider, self.wires, self.server, || *reached = true).await
    }
}

// A new connection to `server` over its encrypted wire. `reached` is called
// once the server answers the dial, before the handshake.
async fn dial(
    provider: &dyn Carrier,
    wires: &Wires,
    server: &DnsServer,
    reached: impl FnOnce(),
) -> Result<Link, Failed> {
    let (config, name) = match &server.wire {
        Wire::Tls { name } => (&wires.dot, name.as_deref()),
        Wire::Https { .. } => (&wires.doh, None),
        Wire::Plain => unreachable!("plain DNS keeps no connection"),
    };
    // The certificate must name the server's address, or the name given.
    let verified = match name {
        Some(name) => {
            ServerName::try_from(name.to_owned()).map_err(|_| Failed::Tls(NOT_A_SERVER_NAME))?
        }
        None => ServerName::IpAddress(server.address.ip().into()),
    };
    let stream = provider.open(server).await.map_err(Failed::Unreached)?;
    reached();
    // A failed handshake or certificate fails this server. It is never
    // asked in plain instead.
    let tls = TlsConnector::from(config.clone())
        .connect(verified, stream)
        .await
        .map_err(|error| Failed::Tls(reason(&error)))?;
    if let Wire::Tls { .. } = server.wire {
        return Ok(Link::Tls(Box::new(tls)));
    }
    Ok(Link::Https(Box::new(Connection::new(tls).await?)))
}

// Why a TLS connection failed, in a few fixed words. Nothing the peer sent,
// such as the names on its certificate, reaches the detail.
const NOT_A_SERVER_NAME: &str = "the configured name is not a server name";
const WRONG_NAME: &str = "certificate not valid for this server";
const UNKNOWN_ISSUER: &str = "unknown issuer";
const OUT_OF_DATE: &str = "certificate expired or not yet valid";
const OTHER_CERTIFICATE: &str = "other certificate error";
const HANDSHAKE: &str = "handshake failed";
const RESET: &str = "connection reset by the server";
const CLOSED: &str = "connection closed by the server";

fn reason(error: &std::io::Error) -> &'static str {
    use tokio_rustls::rustls::Error;
    let Some(error) = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<Error>())
    else {
        return io_reason(error.kind());
    };
    match error {
        Error::InvalidCertificate(
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
        ) => WRONG_NAME,
        Error::InvalidCertificate(CertificateError::UnknownIssuer) => UNKNOWN_ISSUER,
        Error::InvalidCertificate(
            CertificateError::Expired
            | CertificateError::ExpiredContext { .. }
            | CertificateError::NotValidYet
            | CertificateError::NotValidYetContext { .. },
        ) => OUT_OF_DATE,
        Error::InvalidCertificate(_) => OTHER_CERTIFICATE,
        _ => HANDSHAKE,
    }
}

// Why the stream under a TLS connection failed: the server reset it or ended
// it, or else the kind of I/O error by name, never its message.
fn io_reason(kind: std::io::ErrorKind) -> &'static str {
    use std::io::ErrorKind;
    match kind {
        ErrorKind::ConnectionReset => RESET,
        ErrorKind::UnexpectedEof => CLOSED,
        ErrorKind::ConnectionAborted => "I/O error: ConnectionAborted",
        ErrorKind::BrokenPipe => "I/O error: BrokenPipe",
        ErrorKind::NotConnected => "I/O error: NotConnected",
        ErrorKind::TimedOut => "I/O error: TimedOut",
        ErrorKind::Interrupted => "I/O error: Interrupted",
        ErrorKind::WriteZero => "I/O error: WriteZero",
        ErrorKind::InvalidData => "I/O error: InvalidData",
        ErrorKind::InvalidInput => "I/O error: InvalidInput",
        ErrorKind::WouldBlock => "I/O error: WouldBlock",
        ErrorKind::OutOfMemory => "I/O error: OutOfMemory",
        ErrorKind::Other => "I/O error: Other",
        _ => "I/O error: other kind",
    }
}

impl Link {
    // The reply, and whether the connection is still open for the next query.
    async fn exchange(
        &mut self,
        server: &DnsServer,
        packet: &[u8],
    ) -> Result<(Vec<u8>, bool), Failed> {
        match self {
            Self::Tls(stream) => framed(stream.as_mut(), packet, closed)
                .await
                .map(|reply| (reply, true)),
            Self::Https(connection) => {
                let Wire::Https { path } = &server.wire else {
                    unreachable!("an HTTPS link is kept only for an HTTPS server")
                };
                connection.exchange(server, path, packet).await
            }
        }
    }
}

// Counts `queries` sent to check DNSSEC, when `counts`.
fn sent(counts: bool, queries: usize) {
    if counts {
        crate::metrics::record_dnssec_queries(queries as u64);
    }
}

// Plain DNS streams dialled at once for one server. A lookup's DNSSEC check
// can need some forty queries, each its own TCP stream through the route;
// eight in flight keeps one lookup well inside the TCP children a resolver
// serves at once (dnsmasq allows 20, Pi-hole's build 60) while still
// carrying both families' widest round together.
const PLAIN_DIALS: usize = 8;

// The times one query goes out on a kept connection, DNS over TLS or an
// HTTP/2 stream: the first, and up to two more after the connection went
// with it unanswered, or its stream was refused. Queries out together can
// all find their connection gone, and on the next one the server's limit on
// streams at once is not known until its settings come.
const SENDS: u8 = 3;

// A query given to a talk, the lanes that want it, lowest first, whether
// one of them cannot go on without its reply, the lane it goes out in, and
// how many times it has gone out.
struct Asked {
    key: usize,
    lanes: Vec<usize>,
    awaited: bool,
    lane: usize,
    packet: Vec<u8>,
    sends: u8,
}

// Takes from `waiting` the queries whose turn has come, where each lane
// takes turns, and `out` says what a lane has out: None for nothing,
// Some(true) when a query the lane waits on is among it, Some(false) for
// only a query asked ahead. The queries a lane cannot go on without come
// first, then, with `ahead`, those asked ahead, each in the order they were
// given. A query a lane waits on goes in the lowest of its lanes that has
// none out that it waits on, beside a query asked ahead if one is out; one
// asked ahead goes only in a lane with nothing out. So a query two lanes
// want goes in whichever lane is free first, a lane has at most one query
// it waits on and one asked ahead out, and a silent query asked ahead holds
// back nothing a lane waits on, out or not. Without `ahead` nothing asked
// ahead goes until a lane waits on it. Each query goes out once. Each query
// sent has its lane set; the rest stay waiting, in order.
fn turns(
    waiting: &mut VecDeque<Asked>,
    out: impl Fn(usize) -> Option<bool>,
    ahead: bool,
) -> Vec<Asked> {
    let mut going: Vec<Asked> = Vec::new();
    for awaited in [true, false] {
        if !awaited && !ahead {
            break;
        }
        let mut held = VecDeque::new();
        for mut asked in waiting.drain(..) {
            let free = asked.lanes.iter().copied().find(|lane| {
                asked.awaited == awaited
                    && match out(*lane) {
                        None => true,
                        Some(waits) => awaited && !waits,
                    }
                    && going.iter().all(|sent| sent.lane != *lane)
            });
            match free {
                Some(lane) => {
                    asked.lane = lane;
                    going.push(asked);
                }
                None => held.push_back(asked),
            }
        }
        *waiting = held;
    }
    going
}

type PlainCall<'a> = Pin<Box<dyn Future<Output = Reply> + Send + 'a>>;

// One HTTP/1.1 query out, ending with its reply and the connection, when it
// stays open for the next; or with the failure of the dial for it, the
// query not sent.
type OneCall<'a> =
    Pin<Box<dyn Future<Output = Result<(Option<Box<Connection>>, Reply), Failed>> + Send + 'a>>;

// A new connection being made, ending with it or the failure, and whether
// the server answered the dial.
type Dialling<'a> = Pin<Box<dyn Future<Output = (Result<Link, Failed>, bool)> + Send + 'a>>;

// What a talk has out on its wire.
enum Flight<'a> {
    // A stream per query, each with the caller's key. `dialled` is set by
    // the first dial the server answers.
    Plain {
        calls: Vec<(usize, PlainCall<'a>)>,
        dialled: Arc<AtomicBool>,
    },
    // One connection, the queries written on it by id, and the replies read
    // in whatever order they come. `answered` counts the replies the
    // connection has given. `wary` once a connection that answered closed
    // with queries out: from then on one query is out at a time.
    Tls {
        stream: Option<Box<Tls>>,
        answered: usize,
        out: BTreeMap<[u8; 2], Asked>,
        wary: bool,
    },
    Https(Box<Https<'a>>),
}

// What a talk has out over HTTPS. The first connection's handshake says
// which HTTP the server speaks.
#[derive(Default)]
struct Https<'a> {
    // HTTP/2: the one connection, and its streams out by key. `spent` once a
    // stream found the connection gone: it takes no new stream, and goes
    // once none is out. `wary` once a stream was dropped or refused: from
    // then on each lane has at most one stream it waits on out, and one
    // asked ahead, so a server that takes fewer streams than were sent
    // answers each lane in turn, and a stream that stays silent holds up
    // only what waits on it. `old` is a spent connection whose streams still
    // out run on beside `two`, and beside `dialling`, the new connection
    // being made, kept across calls.
    two: Option<Box<Connection>>,
    old: Option<Box<Connection>>,
    dialling: Option<Dialling<'a>>,
    spent: bool,
    wary: bool,
    out: BTreeMap<usize, Asked>,
    // HTTP/1.1, once a connection said so. Each lane asks in turn, one
    // query out at a time, on a connection that is free, or on a new one
    // when none is: so a query that stays silent holds up only its own
    // lane, and the talk opens at most one connection per lane. Each free
    // connection says whether it has answered before. `undialled` once a
    // new connection could not be made: from then on a query waits for one
    // of the talk's connections to come free, and a new one is dialled only
    // when the talk has none out.
    one: bool,
    idle: Vec<(Box<Connection>, bool)>,
    calls: Vec<(Asked, OneCall<'a>)>,
    undialled: bool,
}

// One server's queries for one lookup, sent as they are given and answered
// as they come, so a query that stays silent holds up only what waits on
// its reply. Each key is the caller's own, one per query out at once.
pub(super) struct Talk<'s, 'a> {
    session: &'s mut Session<'a>,
    reached: &'s mut bool,
    // Given, not yet sent.
    waiting: VecDeque<Asked>,
    // Ended, not yet handed back.
    ready: VecDeque<(usize, Reply)>,
    flight: Flight<'a>,
}

impl<'a> Talk<'_, 'a> {
    // Gives the server `packet`, its first two bytes the query's id, to be
    // sent on the next call to `next`. A `lane` is one chain of queries, such
    // as one family's: where queries go out one at a time, HTTP/1.1 and an
    // HTTP/2 server that dropped or refused a stream, each lane has its own
    // turn, so a query that stays silent holds up only its own lane.
    // The lane cannot go on until the reply comes.
    pub(super) fn send(&mut self, key: usize, lane: usize, packet: Vec<u8>) {
        self.give(key, lane, true, packet);
    }

    // As `send`, for a query asked ahead of its need: where a lane takes
    // its turn, a query it waits on goes before this one, and is not held
    // back by this one out. Over HTTP/1.1, and over a wary DNS over TLS
    // talk, it goes only once a lane waits on it, so `next` can end with it
    // still given.
    pub(super) fn ahead(&mut self, key: usize, lane: usize, packet: Vec<u8>) {
        self.give(key, lane, false, packet);
    }

    fn give(&mut self, key: usize, lane: usize, awaited: bool, packet: Vec<u8>) {
        self.waiting.push_back(Asked {
            key,
            lanes: vec![lane],
            awaited,
            lane,
            packet,
            sends: 0,
        });
    }

    // Adds `lane` to the lanes that want the query given under `key`, while
    // it waits to go out, and says whether the lane waits on it: it then
    // goes out in the turn of whichever of them comes first.
    pub(super) fn wanted_by(&mut self, key: usize, lane: usize, awaited: bool) {
        for asked in self.waiting.iter_mut().filter(|asked| asked.key == key) {
            asked.awaited |= awaited;
            if let Err(at) = asked.lanes.binary_search(&lane) {
                asked.lanes.insert(at, lane);
            }
        }
    }

    // Takes back the query given under `key` if it has not gone out yet:
    // nothing waits on it any more. Whether it was taken back.
    pub(super) fn withdraw(&mut self, key: usize) -> bool {
        let before = self.waiting.len();
        self.waiting.retain(|asked| asked.key != key);
        self.waiting.len() < before
    }

    // Sends what was given, and returns the next reply to come, with its
    // query's key: None once nothing is given or out. A server that cannot
    // be reached, or fails TLS, on the talk's first dial ends the talk with
    // that failure; a later dial that fails fails only its own queries.
    pub(super) async fn next(&mut self) -> Result<Option<(usize, Reply)>, Failed> {
        match self.flight {
            Flight::Plain { .. } => self.plain().await,
            Flight::Tls { .. } => self.tls().await,
            Flight::Https(_) => self.https().await,
        }
    }

    // Plain DNS: a stream dialled for each query, at most `PLAIN_DIALS` at
    // once. Until the server has answered a dial only one is dialled, so a
    // route that reaches nothing is dialled once and fails the server. A
    // dial that fails after one was answered fails only its own query.
    async fn plain(&mut self) -> Result<Option<(usize, Reply)>, Failed> {
        let Flight::Plain { calls, dialled } = &mut self.flight else {
            unreachable!("a plain server talks in plain")
        };
        let (provider, server, counts) = (
            self.session.provider,
            self.session.server,
            self.session.counts,
        );
        loop {
            while calls.len() < PLAIN_DIALS && (calls.is_empty() || *self.reached) {
                let Some(asked) = self.waiting.pop_front() else {
                    break;
                };
                let dialled = dialled.clone();
                let call: PlainCall<'a> = Box::pin(async move {
                    let mut stream = provider.open(server).await.map_err(Failed::Unreached)?;
                    dialled.store(true, Ordering::Relaxed);
                    sent(counts, 1);
                    framed(&mut stream, &asked.packet, plain_failed).await
                });
                calls.push((asked.key, call));
            }
            if calls.is_empty() {
                return Ok(None);
            }
            let before = *self.reached;
            let ended = poll_fn(|cx| {
                for at in 0..calls.len() {
                    if let Poll::Ready(reply) = calls[at].1.as_mut().poll(cx) {
                        let (key, _) = calls.swap_remove(at);
                        return Poll::Ready(Some((key, reply)));
                    }
                }
                // The first dial was answered: start the rest.
                if !before && dialled.load(Ordering::Relaxed) {
                    return Poll::Ready(None);
                }
                Poll::Pending
            })
            .await;
            *self.reached |= dialled.load(Ordering::Relaxed);
            match ended {
                None => continue,
                Some((_, Err(failed @ Failed::Unreached(_)))) if !*self.reached => {
                    return Err(failed);
                }
                ended => return Ok(ended),
            }
        }
    }

    // A new connection for the queries waiting. The talk's first dial that
    // fails ends the talk; a later one fails the queries waiting.
    async fn connect(&mut self) -> Result<Option<Link>, Failed> {
        let first = !*self.reached;
        let link = self.session.connect(self.reached).await;
        self.dialled(first, link)
    }

    // What came of a new connection's dial, `first` when the talk had
    // reached the server on none before.
    fn dialled(&mut self, first: bool, link: Result<Link, Failed>) -> Result<Option<Link>, Failed> {
        match link {
            Ok(link) => Ok(Some(link)),
            Err(failed) if first => Err(failed),
            Err(failed) => {
                for asked in self.waiting.drain(..) {
                    self.ready.push_back((asked.key, Err(failed.copy())));
                }
                Ok(None)
            }
        }
    }

    // DNS over TLS: each query written on the connection as it is given,
    // and the replies read one at a time, each matched to its query by id
    // and question. A query waits while another with its id is out. A reply
    // with no query's id is passed over, as plain DNS and HTTPS never see it;
    // one with a query's id but another question fails that query, as it
    // would there. A connection that closes with queries out after it
    // answered something is replaced for them, so a talk opens at most one
    // connection per reply, and one more. From then on the talk is wary:
    // each connection has one query out at a time, and only one a lane
    // waits on, as over HTTP/1.1, so on a server that closes after every
    // reply a query goes out at most twice. Each query is written at most `SENDS` times; one the
    // connection went with that often fails as closed.
    async fn tls(&mut self) -> Result<Option<(usize, Reply)>, Failed> {
        loop {
            if let Some(ended) = self.ready.pop_front() {
                return Ok(Some(ended));
            }
            let Flight::Tls {
                stream, out, wary, ..
            } = &mut self.flight
            else {
                unreachable!("a TLS server talks over TLS")
            };
            // Wary, a query asked ahead goes only once a lane waits on it, so
            // `next` can end with it still given.
            let wary = *wary;
            if out.is_empty() && !self.waiting.iter().any(|asked| asked.awaited || !wary) {
                return Ok(None);
            }
            if stream.is_none() {
                match self.connect().await? {
                    Some(Link::Tls(connection)) => {
                        let Flight::Tls {
                            stream, answered, ..
                        } = &mut self.flight
                        else {
                            unreachable!("a TLS server talks over TLS")
                        };
                        *stream = Some(connection);
                        *answered = 0;
                    }
                    Some(Link::Https(_)) => unreachable!("a TLS server opens a TLS link"),
                    None => {}
                }
                continue;
            }
            let Flight::Tls {
                stream: Some(connection),
                out,
                wary,
                ..
            } = &mut self.flight
            else {
                unreachable!("a TLS server talks over TLS, on a connection made above")
            };
            let mut given = std::mem::take(&mut self.waiting);
            if *wary {
                // One query out at a time, and only one a lane waits on: out
                // ahead, a silent one would hold back the query a lane waits
                // on.
                let next = given
                    .iter()
                    .position(|asked| asked.awaited)
                    .filter(|_| out.is_empty())
                    .and_then(|at| given.remove(at));
                self.waiting = given;
                given = next.into_iter().collect();
            }
            let mut framed = Vec::new();
            let mut sending = 0;
            for mut asked in given {
                let id = asked.packet.get(..2).map_or([0; 2], |id| [id[0], id[1]]);
                if out.contains_key(&id) {
                    self.waiting.push_back(asked);
                    continue;
                }
                framed.extend_from_slice(&(asked.packet.len() as u16).to_be_bytes());
                framed.extend_from_slice(&asked.packet);
                sending += 1;
                asked.sends += 1;
                out.insert(id, asked);
            }
            self.session.sent(sending);
            let wrote = framed.is_empty()
                || (connection.write_all(&framed).await.is_ok()
                    && connection.flush().await.is_ok());
            let read = if wrote {
                reply(connection.as_mut()).await
            } else {
                Err(Failed::Closed)
            };
            let Flight::Tls {
                stream,
                answered,
                out,
                wary,
            } = &mut self.flight
            else {
                unreachable!("a TLS server talks over TLS")
            };
            match read {
                Ok(bytes) => {
                    let Some(asked) = out.remove(&[bytes[0], bytes[1]]) else {
                        continue;
                    };
                    *answered += 1;
                    let reply = if asks(&asked.packet, &bytes) {
                        Ok(bytes)
                    } else {
                        Err(Failed::Invalid)
                    };
                    return Ok(Some((asked.key, reply)));
                }
                Err(Failed::Closed) if *answered > 0 => {
                    *stream = None;
                    *wary |= !out.is_empty();
                    for asked in std::mem::take(out).into_values() {
                        if asked.sends < SENDS {
                            self.waiting.push_back(asked);
                        } else {
                            self.ready.push_back((asked.key, Err(Failed::Closed)));
                        }
                    }
                }
                Err(failed) => {
                    *stream = None;
                    for asked in std::mem::take(out).into_values() {
                        self.ready.push_back((asked.key, Err(failed.copy())));
                    }
                }
            }
        }
    }

    // DNS over HTTPS, over the HTTP the first connection's handshake picks.
    async fn https(&mut self) -> Result<Option<(usize, Reply)>, Failed> {
        loop {
            if let Some(ended) = self.ready.pop_front() {
                return Ok(Some(ended));
            }
            let Flight::Https(https) = &mut self.flight else {
                unreachable!("an HTTPS server talks over HTTPS")
            };
            if https.one {
                return self.one().await;
            }
            if https.two.is_some() || https.old.is_some() {
                if let Some(ended) = self.two().await {
                    return Ok(ended);
                }
                let Flight::Https(https) = &mut self.flight else {
                    unreachable!("an HTTPS server talks over HTTPS")
                };
                if https.two.is_some() || self.waiting.is_empty() {
                    continue;
                }
            }
            if self.waiting.is_empty() {
                return Ok(None);
            }
            // The new connection is made while the old one's streams still
            // out are read: a reply that comes on it meanwhile is handed
            // back at once, and the dial goes on at the next call.
            let (provider, wires, server) = (
                self.session.provider,
                self.session.wires,
                self.session.server,
            );
            let Flight::Https(https) = &mut self.flight else {
                unreachable!("an HTTPS server talks over HTTPS")
            };
            let Https { dialling, old, .. } = &mut **https;
            let dialling = dialling.get_or_insert_with(|| {
                Box::pin(async move {
                    let mut answered = false;
                    let link = dial(provider, wires, server, || answered = true).await;
                    (link, answered)
                })
            });
            let came = match old.as_mut().filter(|old| old.carrying()) {
                Some(old) => tokio::select! {
                    dialled = dialling.as_mut() => Ok(dialled),
                    ended = old.next() => Err(ended),
                },
                None => Ok(dialling.as_mut().await),
            };
            let (link, answered) = match came {
                Ok(dialled) => dialled,
                Err(ended) => {
                    if let Some((key, reply)) = ended
                        && let Some(ended) = self.landed(key, reply, false)
                    {
                        return Ok(ended);
                    }
                    continue;
                }
            };
            let Flight::Https(https) = &mut self.flight else {
                unreachable!("an HTTPS server talks over HTTPS")
            };
            https.dialling = None;
            let first = !*self.reached;
            *self.reached |= answered;
            let link = self.dialled(first, link)?;
            let Flight::Https(https) = &mut self.flight else {
                unreachable!("an HTTPS server talks over HTTPS")
            };
            match link {
                Some(Link::Https(connection)) if connection.multiplexes() => {
                    https.two = Some(connection);
                    https.spent = false;
                }
                Some(Link::Https(connection)) => {
                    https.one = true;
                    https.idle.push((connection, false));
                    // Streams still out on a spent HTTP/2 connection go
                    // again over HTTP/1.1, as a stream that found its
                    // connection gone goes again on a new one.
                    https.old = None;
                    let mut again = VecDeque::new();
                    for asked in std::mem::take(&mut https.out).into_values() {
                        if asked.sends < SENDS {
                            again.push_back(asked);
                        } else {
                            self.ready.push_back((asked.key, Err(Failed::Closed)));
                        }
                    }
                    again.append(&mut self.waiting);
                    self.waiting = again;
                }
                Some(Link::Tls(_)) => unreachable!("an HTTPS server opens an HTTPS link"),
                None => {}
            }
        }
    }

    // HTTP/2: each query goes out on a stream of its own as it is given,
    // beside those still out. A stream the server refused is sent again on
    // the connection; one that found the connection gone, on a new one,
    // opened as soon as a query waits while the old one's streams still out
    // run on beside it: at most two connections at once. Either is sent at
    // most `SENDS` times in all, so a server that refuses or drops every
    // stream ends the query. None when a new connection is wanted or the
    // talk is to look again, and Some(None) once nothing is given or out.
    async fn two(&mut self) -> Option<Option<(usize, Reply)>> {
        let Flight::Https(https) = &mut self.flight else {
            unreachable!("an HTTPS server talks over HTTPS")
        };
        if https
            .old
            .as_ref()
            .is_some_and(|connection| !connection.carrying())
        {
            https.old = None;
        }
        // A spent connection goes once its streams have ended, or drains
        // beside a new one once a query waits for it.
        if let Some(connection) = https.two.as_ref()
            && (https.spent || !connection.open())
        {
            if !connection.carrying() {
                https.two = None;
            } else if https.old.is_none() && !self.waiting.is_empty() {
                https.old = https.two.take();
            }
        }
        if https.two.is_none() && !self.waiting.is_empty() {
            return None;
        }
        let Wire::Https { path } = &self.session.server.wire else {
            unreachable!("an HTTPS link is made only for an HTTPS server")
        };
        if let Some(connection) = https.two.as_mut()
            && connection.open()
            && !https.spent
        {
            let given = if https.wary {
                let out = &https.out;
                let lane_out = |lane| {
                    out.values()
                        .filter(|asked| asked.lane == lane)
                        .map(|asked| asked.awaited)
                        .reduce(|one, other| one || other)
                };
                turns(&mut self.waiting, lane_out, true)
            } else {
                self.waiting.drain(..).collect()
            };
            for mut asked in given {
                match connection.start(self.session.server, path, asked.key, &asked.packet) {
                    Ok(()) => {
                        sent(self.session.counts, 1);
                        asked.sends += 1;
                        https.out.insert(asked.key, asked);
                    }
                    Err(failed) => self.ready.push_back((asked.key, Err(failed))),
                }
            }
        }
        let carrying = |connection: &Option<Box<Connection>>| {
            connection
                .as_ref()
                .is_some_and(|connection| connection.carrying())
        };
        let (key, reply, on_two) = match (carrying(&https.two), carrying(&https.old)) {
            (false, false) => {
                return match self.ready.pop_front() {
                    Some(ended) => Some(Some(ended)),
                    None if self.waiting.is_empty() => Some(None),
                    None => None,
                };
            }
            (true, false) => {
                let (key, reply) = https.two.as_mut()?.next().await?;
                (key, reply, true)
            }
            (false, true) => {
                let (key, reply) = https.old.as_mut()?.next().await?;
                (key, reply, false)
            }
            (true, true) => {
                let (two, old) = (https.two.as_mut()?, https.old.as_mut()?);
                tokio::select! {
                    ended = two.next() => {
                        let (key, reply) = ended?;
                        (key, reply, true)
                    }
                    ended = old.next() => {
                        let (key, reply) = ended?;
                        (key, reply, false)
                    }
                }
            }
        };
        self.landed(key, reply, on_two)
    }

    // The stream under `key` ended with `reply`, on `two` when `on_two`, on
    // `old` otherwise. None when its query goes out again, or is not out.
    fn landed(&mut self, key: usize, reply: Reply, on_two: bool) -> Option<Option<(usize, Reply)>> {
        let Flight::Https(https) = &mut self.flight else {
            unreachable!("an HTTPS server talks over HTTPS")
        };
        if on_two {
            // A stream that found the connection gone may end before this
            // side sees the connection end.
            https.spent |= https.two.as_ref().is_some_and(|two| !two.open());
        }
        let asked = https.out.remove(&key)?;
        match reply {
            Err(Failed::Refused) if asked.sends < SENDS => {
                https.wary = true;
                self.waiting.push_front(asked);
                None
            }
            Err(Failed::Closed) if asked.sends < SENDS => {
                https.wary = true;
                https.spent |= on_two;
                self.waiting.push_front(asked);
                None
            }
            Err(Failed::Closed) => {
                https.spent |= on_two;
                Some(Some((key, Err(Failed::Closed))))
            }
            reply => Some(Some((key, reply))),
        }
    }

    // HTTP/1.1: each lane's next query goes out as soon as the lane has
    // none out, on a free connection or a new one. A query asked ahead goes
    // only once a lane waits on it: out ahead, a silent one would hold its
    // lane's next query back, and a second query out for the lane would
    // take a second connection. A query whose new connection could not be
    // made waits for one the talk has; it fails with the dial only when the
    // talk has no other.
    async fn one(&mut self) -> Result<Option<(usize, Reply)>, Failed> {
        let Flight::Https(https) = &mut self.flight else {
            unreachable!("an HTTPS server talks over HTTPS")
        };
        let (provider, wires, server, counts) = (
            self.session.provider,
            self.session.wires,
            self.session.server,
            self.session.counts,
        );
        loop {
            let calls = &https.calls;
            let lane_out = |lane| {
                calls
                    .iter()
                    .any(|(out, _)| out.lane == lane)
                    .then_some(true)
            };
            let given = turns(&mut self.waiting, lane_out, false);
            let mut held = Vec::new();
            for asked in given {
                let taken = https.idle.pop();
                // No new connection while one could not be made and another
                // is still out: the query waits for that one to come free.
                if taken.is_none() && https.undialled && !https.calls.is_empty() {
                    held.push(asked);
                    continue;
                }
                let call: OneCall<'a> = Box::pin(one_call(
                    provider,
                    wires,
                    server,
                    counts,
                    taken,
                    asked.packet.clone(),
                ));
                https.calls.push((asked, call));
            }
            for asked in held.into_iter().rev() {
                self.waiting.push_front(asked);
            }
            if https.calls.is_empty() {
                return Ok(None);
            }
            let (asked, ended) = poll_fn(|cx| {
                for at in 0..https.calls.len() {
                    if let Poll::Ready(ended) = https.calls[at].1.as_mut().poll(cx) {
                        let (asked, _) = https.calls.swap_remove(at);
                        return Poll::Ready((asked, ended));
                    }
                }
                Poll::Pending
            })
            .await;
            match ended {
                Ok((connection, reply)) => {
                    if let Some(connection) = connection {
                        https.idle.push((connection, true));
                    }
                    return Ok(Some((asked.key, reply)));
                }
                // A dial that failed while another connection is out or
                // free: the query waits for one of those.
                Err(_) if !https.calls.is_empty() || !https.idle.is_empty() => {
                    https.undialled = true;
                    self.waiting.push_front(asked);
                }
                Err(failed) => return Ok(Some((asked.key, Err(failed)))),
            }
        }
    }
}

// One query over HTTP/1.1, on the free connection `taken` when there is one,
// else on a new one. A connection that answered before and is found closed
// before any byte of the reply is replaced once, as `ask` replaces a kept
// one. The connection comes back with the reply while it stays open. A dial
// that fails ends the call with its failure, the query not sent on it.
async fn one_call(
    provider: &dyn Carrier,
    wires: &Wires,
    server: &DnsServer,
    counts: bool,
    mut taken: Option<(Box<Connection>, bool)>,
    packet: Vec<u8>,
) -> Result<(Option<Box<Connection>>, Reply), Failed> {
    let Wire::Https { path } = &server.wire else {
        unreachable!("an HTTPS query goes only to an HTTPS server")
    };
    loop {
        let (mut connection, used) = match taken.take() {
            Some(taken) => taken,
            None => match dial(provider, wires, server, || {}).await? {
                Link::Https(connection) => (connection, false),
                Link::Tls(_) => unreachable!("an HTTPS server opens an HTTPS link"),
            },
        };
        sent(counts, 1);
        match connection.exchange(server, path, &packet).await {
            Ok((reply, open)) => return Ok((open.then_some(connection), Ok(reply))),
            Err(Failed::Closed) if used => {}
            Err(failed) => return Ok((None, Err(failed))),
        }
    }
}

// Whether `reply` asks the question `query` asked: the same question count,
// the same name in any case, as a reply's name is read, and the same type
// and class. With several queries out on one connection, an id alone could
// match a reply to the wrong one.
fn asks(query: &[u8], reply: &[u8]) -> bool {
    let mut end = 12;
    while let Some(&label) = query.get(end) {
        if label == 0 {
            break;
        }
        end += 1 + usize::from(label);
    }
    // The root label.
    end += 1;
    end + 4 <= query.len()
        && query.get(4..6) == reply.get(4..6)
        && reply
            .get(12..end)
            .is_some_and(|name| name.eq_ignore_ascii_case(&query[12..end]))
        && query.get(end..end + 4) == reply.get(end..end + 4)
}

// How an I/O error before the first byte of a reply reads. Over TLS the
// connection is taken as closed, so a kept one is replaced; in plain, the
// error is told by its kind, a reset from another.
fn closed(_: std::io::Error) -> Failed {
    Failed::Closed
}

fn plain_failed(error: std::io::Error) -> Failed {
    Failed::Io(io_reason(error.kind()))
}

// One query over a stream, with the two-byte length prefix of DNS over TCP.
// A stream that ends before the first byte of the reply is closed, and one
// that fails before it fails as `failed` says; one that ends part way cut
// the reply short.
async fn framed<S>(
    stream: &mut S,
    packet: &[u8],
    failed: fn(std::io::Error) -> Failed,
) -> Result<Vec<u8>, Failed>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut framed = Vec::with_capacity(packet.len() + 2);
    framed.extend_from_slice(&(packet.len() as u16).to_be_bytes());
    framed.extend_from_slice(packet);
    stream.write_all(&framed).await.map_err(failed)?;
    stream.flush().await.map_err(failed)?;
    reply_or(stream, failed).await
}

// One reply off a stream, by its two-byte length prefix, within the cap.
async fn reply<S>(stream: &mut S) -> Result<Vec<u8>, Failed>
where
    S: AsyncRead + Unpin + ?Sized,
{
    reply_or(stream, closed).await
}

// As `reply`, with an error before the first byte failing as `failed` says.
async fn reply_or<S>(
    stream: &mut S,
    failed: fn(std::io::Error) -> Failed,
) -> Result<Vec<u8>, Failed>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut prefix = [0; 2];
    match stream.read(&mut prefix[..1]).await {
        Ok(1) => {}
        Ok(_) => return Err(Failed::Closed),
        Err(error) => return Err(failed(error)),
    }
    stream
        .read_exact(&mut prefix[1..])
        .await
        .map_err(|_| Failed::Cut)?;
    let size = u16::from_be_bytes(prefix) as usize;
    if !(12..=REPLY_CAP).contains(&size) {
        return Err(Failed::Invalid);
    }
    let mut bytes = vec![0; size];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|_| Failed::Cut)?;
    Ok(bytes)
}
