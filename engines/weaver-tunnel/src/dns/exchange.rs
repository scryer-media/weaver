// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The connection one server's queries ride during one lookup. Plain DNS
// opens a stream per query. DNS over TLS (RFC 7858) and over HTTPS (RFC
// 8484) wrap the stream the stage dials in TLS, and keep that one
// connection for the lookup's queries while the server keeps it open. A
// lookup on a naming that keeps connections takes the one its last lookup
// to the server left, and leaves its own healthy one for the next.
use super::{Carrier, DnsServer, Exchanged, Wire, list::Keeping};
use crate::TunnelStream;
use crate::pipe::KeeperRef;
use doh::Doh;
use dot::Dot;
#[cfg(test)]
pub(super) use kept::IDLE;
pub(super) use kept::Kept;
use kept::Link;
use plain::PlainTcp;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{
        CertificateError, ClientConfig, RootCertStore, client::Resumption, crypto::aws_lc_rs,
        pki_types::ServerName,
    },
};

mod doh;
mod dot;
mod kept;
mod plain;

static NEXT_ID: AtomicU16 = AtomicU16::new(1);

// The largest reply taken on any wire.
pub(super) const REPLY_CAP: usize = 16384;

// TLS sessions one naming keeps for resumption.
const SESSIONS: usize = 32;

// The TLS settings of each encrypted wire. A clone shares the session store.
#[cfg_attr(test, derive(Clone))]
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

pub(super) type Tls = TlsStream<Box<dyn TunnelStream>>;

// One query's reply, or why it got none.
pub(super) type Reply = Result<Vec<u8>, Exchanged>;

// What a talk's `next` comes to: the next reply to come, with its query's
// key; None once nothing is given or out; or the failure that ends the talk.
type Next = Result<Option<(usize, Reply)>, Exchanged>;

// One server, asked for one lookup.
pub(super) struct Session<'a> {
    provider: &'a dyn Carrier,
    wires: &'a Wires,
    server: &'a DnsServer,
    // The naming's kept connections, which the talk takes its first from
    // and leaves its own to. None for a server asked once, as a probe asks.
    kept: Option<&'a Kept>,
    // The stages a connection to the server rides, the dialling one first:
    // a kept connection is taken only when dialled through that same stage
    // instance, and is left tied to them all.
    rides: Vec<KeeperRef>,
    // Each query sent is counted as a DNSSEC query.
    counts: bool,
}

impl<'a> Session<'a> {
    // A server asked once: nothing is taken from or left to a naming.
    pub(super) fn new(provider: &'a dyn Carrier, wires: &'a Wires, server: &'a DnsServer) -> Self {
        Self {
            provider,
            wires,
            server,
            kept: None,
            rides: Vec::new(),
            counts: false,
        }
    }

    // A server asked for a naming's lookup, on the connection the naming
    // kept for it when there is one, leaving the talk's own healthy one to
    // the naming when it ends. Only a naming's lookup (`list.rs`) has the
    // `Keeping` this takes, so a server test never keeps a connection or
    // reuses one.
    pub(super) fn keeping(
        provider: &'a dyn Carrier,
        keeping: Keeping<'a>,
        server: &'a DnsServer,
    ) -> Self {
        let sessions = keeping.sessions();
        Self {
            kept: Some(sessions.kept()),
            rides: provider.keepers(),
            ..Self::new(provider, sessions.wires(), server)
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

    // The id the next query to the server goes out with, as its wire sends
    // it.
    pub(super) fn id(&self) -> u16 {
        match self.server.wire {
            Wire::Plain => PlainTcp::id(),
            Wire::Tls { .. } => Dot::id(),
            Wire::Https { .. } => Doh::id(),
        }
    }

    // Sends `packet` on a connection of its own and returns the reply,
    // whatever it says. `reached` is set once the server answers the dial.
    // The session asks once, and is used up.
    pub(super) async fn ask(self, packet: &[u8], reached: &mut bool) -> Reply {
        match self.server.wire {
            Wire::Plain => PlainTcp::ask(&self, packet, reached).await,
            Wire::Tls { .. } => Dot::ask(&self, packet, reached).await,
            Wire::Https { .. } => Doh::ask(&self, packet, reached).await,
        }
    }

    // A talk with the server: queries given one at a time, each sent as it
    // is given while others are still out, and each reply handed back as it
    // comes. `reached` is set once the server answers a dial, or a kept
    // connection answers. The talk's connections are its own, and go when
    // it is dropped, but for one it leaves healthy to the naming that keeps
    // connections.
    pub(super) fn talk<'s>(&'s mut self, reached: &'s mut bool) -> Talk<'s, 'a> {
        let kept = self
            .kept
            .and_then(|kept| kept.take(self.server, self.rides.first()));
        let exchange = match self.server.wire {
            Wire::Plain => Wired::Plain(PlainTcp {
                calls: Vec::new(),
                dialled: Arc::new(AtomicBool::new(false)),
            }),
            Wire::Tls { .. } => Wired::Dot(Dot::on(kept)),
            Wire::Https { .. } => Wired::Doh(Box::new(Doh::on(kept))),
        };
        Talk {
            queue: Queue {
                session: self,
                reached,
                waiting: VecDeque::new(),
                ready: VecDeque::new(),
            },
            exchange,
        }
    }
}

// A new TLS connection to `server` over its encrypted wire. `reached` is
// called once the server answers the dial, before the handshake.
async fn handshake(
    provider: &dyn Carrier,
    wires: &Wires,
    server: &DnsServer,
    reached: impl FnOnce(),
) -> Result<Tls, Exchanged> {
    let (config, name) = match &server.wire {
        Wire::Tls { name } => (&wires.dot, name.as_deref()),
        Wire::Https { .. } => (&wires.doh, None),
        Wire::Plain => unreachable!("plain DNS keeps no connection"),
    };
    // The certificate must name the server's address, or the name given.
    let verified = match name {
        Some(name) => {
            ServerName::try_from(name.to_owned()).map_err(|_| Exchanged::Tls(NOT_A_SERVER_NAME))?
        }
        None => ServerName::IpAddress(server.address.ip().into()),
    };
    let stream = provider.open(server).await.map_err(Exchanged::Unreached)?;
    reached();
    // A failed handshake or certificate fails this server. It is never
    // asked in plain instead.
    TlsConnector::from(config.clone())
        .connect(verified, stream)
        .await
        .map_err(|error| Exchanged::Tls(reason(&error)))
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

// Counts `queries` sent to check DNSSEC, when `counts`.
fn sent(counts: bool, queries: usize) {
    if counts {
        crate::metrics::record_dnssec_queries(queries as u64);
    }
}

// How long the first reply on a connection a naming kept is waited for
// before the connection is taken as gone, as one found closed is: one
// redial, no failure charged, the same queries. A connection that died
// without a close (NAT or tunnel state dropped, a peer gone) never says so,
// and would hold the lookup for all of `SERVER_TIMEOUT` and charge the
// server a time-out. On a warm connection, with no dial or handshake first,
// a server answers in a round trip plus its own lookup time; 2 s is far past
// that for a server in use. After the wait the redial has what is left of
// `SERVER_TIMEOUT` for its dial, handshake and reply. A lookup that cannot
// finish in that, as a validated one with a slow chain may not, fails over
// to the next server, as a cold lookup that slow does.
pub(in crate::dns) const REUSED_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

// The times one query goes out on a kept connection, DNS over TLS or an
// HTTP/2 stream: the first, and up to two more after the connection went
// with it unanswered, or its stream was refused. Queries out together can
// all find their connection gone, and on the next one the server's limit on
// streams at once is not known until its settings come.
const SENDS: u8 = 3;

// A query given to a talk, the lanes that want it, lowest first, whether
// one of them cannot go on without its reply, the lane it goes out in, how
// many times it has gone out.
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

// One conversation with one server over one wire, driven by a `Talk`. What
// every wire shares is the talk's `Queue`: the queries given and not yet
// sent, with the lanes that want them, and the replies ended and not yet
// handed back. An exchange takes the queries waiting as its wire lets them
// go, and hands back each reply as it comes.
//
// Each wire is its own type, called through `Wired`, so every call is
// dispatched statically and its future is not boxed.
trait Exchange<'a> {
    // The id a query to the server goes out with.
    fn id() -> u16;

    // Sends `packet` on a connection of its own and returns the reply,
    // whatever it says. `reached` is set once the server answers the dial.
    fn ask(
        session: &Session<'a>,
        packet: &[u8],
        reached: &mut bool,
    ) -> impl Future<Output = Reply> + Send;

    // Sends what the queue holds as the wire lets it go, and returns the
    // next reply to come, with its query's key: None once nothing is given
    // or out. A server that cannot be reached, or fails TLS, on the talk's
    // first dial ends the talk with that failure; a later dial that fails
    // fails only its own queries.
    fn next(&mut self, queue: &mut Queue<'_, 'a>) -> impl Future<Output = Next> + Send;

    // The connection to leave to the naming as the talk ends: one that is
    // healthy and owes no reply, so nothing a later lookup reads on it can
    // belong to this one. None for any other.
    fn leave(&mut self) -> Option<Link>;
}

// The exchange a talk drives, one per wire.
enum Wired<'a> {
    Plain(PlainTcp<'a>),
    Dot(Dot),
    Doh(Box<Doh<'a>>),
}

// What a talk holds for its exchange: the session, whether the server has
// answered a dial, the queries given and not yet sent, and the replies ended
// and not yet handed back.
struct Queue<'s, 'a> {
    session: &'s Session<'a>,
    reached: &'s mut bool,
    // Given, not yet sent.
    waiting: VecDeque<Asked>,
    // Ended, not yet handed back.
    ready: VecDeque<(usize, Reply)>,
}

impl Queue<'_, '_> {
    // What came of a new connection's dial, `first` when the talk had
    // reached the server on none before. The talk's first dial that fails
    // ends the talk; a later one fails the queries waiting.
    fn dialled<C>(
        &mut self,
        first: bool,
        link: Result<C, Exchanged>,
    ) -> Result<Option<C>, Exchanged> {
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
}

// One server's queries for one lookup, sent as they are given and answered
// as they come, so a query that stays silent holds up only what waits on
// its reply. Each key is the caller's own, one per query out at once.
pub(super) struct Talk<'s, 'a> {
    queue: Queue<'s, 'a>,
    exchange: Wired<'a>,
}

impl Talk<'_, '_> {
    // The id the next query given goes out with, as the talk's wire sends
    // it.
    pub(super) fn id(&self) -> u16 {
        match self.exchange {
            Wired::Plain(_) => PlainTcp::id(),
            Wired::Dot(_) => Dot::id(),
            Wired::Doh(_) => Doh::id(),
        }
    }

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
        self.queue.waiting.push_back(Asked {
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
        for asked in self
            .queue
            .waiting
            .iter_mut()
            .filter(|asked| asked.key == key)
        {
            asked.awaited |= awaited;
            if let Err(at) = asked.lanes.binary_search(&lane) {
                asked.lanes.insert(at, lane);
            }
        }
    }

    // Takes back the query given under `key` if it has not gone out yet:
    // nothing waits on it any more. Whether it was taken back.
    pub(super) fn withdraw(&mut self, key: usize) -> bool {
        let before = self.queue.waiting.len();
        self.queue.waiting.retain(|asked| asked.key != key);
        self.queue.waiting.len() < before
    }

    // Sends what was given, and returns the next reply to come, with its
    // query's key: None once nothing is given or out. A server that cannot
    // be reached, or fails TLS, on the talk's first dial ends the talk with
    // that failure; a later dial that fails fails only its own queries.
    pub(super) async fn next(&mut self) -> Next {
        let queue = &mut self.queue;
        match &mut self.exchange {
            Wired::Plain(plain) => plain.next(queue).await,
            Wired::Dot(dot) => dot.next(queue).await,
            Wired::Doh(doh) => doh.next(queue).await,
        }
    }
}

// A talk that ends, done or dropped part way, leaves its healthy connection
// to the naming that keeps connections.
impl Drop for Talk<'_, '_> {
    fn drop(&mut self) {
        let session = self.queue.session;
        let Some(kept) = session.kept else {
            return;
        };
        let left = match &mut self.exchange {
            Wired::Plain(plain) => plain.leave(),
            Wired::Dot(dot) => dot.leave(),
            Wired::Doh(doh) => doh.leave(),
        };
        if let Some(link) = left {
            kept.keep(session.server, link, &session.rides);
        }
    }
}

// How an I/O error before the first byte of a reply reads. Over TLS the
// connection is taken as closed, so a kept one is replaced; in plain, the
// error is told by its kind, a reset from another.
fn closed(_: std::io::Error) -> Exchanged {
    Exchanged::Closed
}

// One query over a stream, with the two-byte length prefix of DNS over TCP.
// A stream that ends before the first byte of the reply is closed, and one
// that fails before it fails as `failed` says; one that ends part way cut
// the reply short.
async fn framed<S>(
    stream: &mut S,
    packet: &[u8],
    failed: fn(std::io::Error) -> Exchanged,
) -> Result<Vec<u8>, Exchanged>
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
async fn reply<S>(stream: &mut S) -> Result<Vec<u8>, Exchanged>
where
    S: AsyncRead + Unpin + ?Sized,
{
    reply_or(stream, closed).await
}

// As `reply`, with an error before the first byte failing as `failed` says.
async fn reply_or<S>(
    stream: &mut S,
    failed: fn(std::io::Error) -> Exchanged,
) -> Result<Vec<u8>, Exchanged>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut prefix = [0; 2];
    match stream.read(&mut prefix[..1]).await {
        Ok(1) => {}
        Ok(_) => return Err(Exchanged::Closed),
        Err(error) => return Err(failed(error)),
    }
    stream
        .read_exact(&mut prefix[1..])
        .await
        .map_err(|_| Exchanged::Cut)?;
    let size = u16::from_be_bytes(prefix) as usize;
    if !(12..=REPLY_CAP).contains(&size) {
        return Err(Exchanged::Invalid);
    }
    let mut bytes = vec![0; size];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|_| Exchanged::Cut)?;
    Ok(bytes)
}
