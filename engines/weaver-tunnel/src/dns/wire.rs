// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The connection one server's queries ride during one lookup. Plain DNS
// opens a stream per query. DNS over TLS (RFC 7858) and over HTTPS (RFC
// 8484) wrap the stream the stage dials in TLS, and keep that one
// connection for the lookup's queries while the server keeps it open.
use super::{Carrier, DnsServer, Failed, Wire, dnssec::Dnssec};
use crate::TunnelStream;
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

enum Link {
    Tls(Box<Tls>),
    Https(Box<super::https::Connection>),
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
            return framed(&mut stream, packet).await;
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
        let mut link = self.connect(reached).await?;
        self.sent(1);
        let reply = link.exchange(self.server, packet).await?;
        Ok(self.keep(link, reply))
    }

    // The reply to each of `packets`, in order, each packet's first two
    // bytes its id. Plain DNS dials a stream per query and asks them all at
    // once. DNS over TLS writes them all on the one kept connection, then
    // reads the replies in whatever order they come, matched by id. Over
    // HTTP/2 the first opens or rides the kept connection and the rest go
    // out on it together, one stream each; a stream that found the
    // connection gone is asked again as `ask` would. HTTP/1.1 asks them in
    // turn on its kept connection. A server that cannot be reached, or fails
    // TLS, fails the whole batch, as does a DNS over TLS reply whose id no
    // query of the batch went out with.
    pub(super) async fn ask_all(
        &mut self,
        packets: &[Vec<u8>],
        reached: &mut bool,
    ) -> Result<Vec<Result<Vec<u8>, Failed>>, Failed> {
        match self.server.wire {
            Wire::Plain => {
                plain_all(self.provider, self.server, self.counts, packets, reached).await
            }
            Wire::Tls { .. } => self.pipelined(packets, reached).await,
            Wire::Https { .. } => self.https_all(packets, reached).await,
        }
    }

    async fn https_all(
        &mut self,
        packets: &[Vec<u8>],
        reached: &mut bool,
    ) -> Result<Vec<Result<Vec<u8>, Failed>>, Failed> {
        let mut replies = Vec::with_capacity(packets.len());
        let mut packets = packets.iter();
        while let Some(packet) = packets.next() {
            let together = packets.len() > 0
                && matches!(&self.link, Some(Link::Https(connection)) if connection.multiplexes());
            if !together {
                replies.push(ends(self.ask(packet, reached).await)?);
                continue;
            }
            let rest: Vec<Vec<u8>> = std::iter::once(packet)
                .chain(packets.by_ref())
                .cloned()
                .collect();
            let Some(Link::Https(mut connection)) = self.link.take() else {
                unreachable!("checked above")
            };
            *reached = true;
            let Wire::Https { path } = &self.server.wire else {
                unreachable!("an HTTPS link is kept only for an HTTPS server")
            };
            self.sent(rest.len());
            let (answered, open) = connection.exchange_all(self.server, path, &rest).await;
            if open {
                self.link = Some(Link::Https(connection));
            }
            for (packet, reply) in rest.iter().zip(answered) {
                let reply = match reply {
                    Err(Failed::Closed) => self.ask(packet, reached).await,
                    reply => reply,
                };
                replies.push(ends(reply)?);
            }
        }
        Ok(replies)
    }

    // DNS over TLS: the batch written at once on the kept connection, or a
    // new one, and the replies read as they come. A connection that closes
    // before every reply came is replaced for the rest, as `ask` replaces
    // it: a kept one always, a new one only after it answered something. So
    // a batch opens at most one connection per query, and one more.
    async fn pipelined(
        &mut self,
        packets: &[Vec<u8>],
        reached: &mut bool,
    ) -> Result<Vec<Result<Vec<u8>, Failed>>, Failed> {
        let mut replies: Vec<Option<Result<Vec<u8>, Failed>>> =
            packets.iter().map(|_| None).collect();
        loop {
            let waiting: Vec<usize> = (0..packets.len())
                .filter(|at| replies[*at].is_none())
                .collect();
            if waiting.is_empty() {
                break;
            }
            let (mut stream, kept) = match self.link.take() {
                Some(Link::Tls(stream)) => {
                    *reached = true;
                    (stream, true)
                }
                Some(Link::Https(_)) => {
                    unreachable!("an HTTPS link is kept only for an HTTPS server")
                }
                None => match self.connect(reached).await? {
                    Link::Tls(stream) => (stream, false),
                    Link::Https(_) => unreachable!("a TLS server opens a TLS link"),
                },
            };
            self.sent(waiting.len());
            let (piped, answered) =
                pipeline(stream.as_mut(), packets, &waiting, &mut replies).await;
            let failed: fn() -> Failed = match piped {
                Piped::Done => {
                    self.link = Some(Link::Tls(stream));
                    break;
                }
                Piped::Closed if kept || answered > 0 => continue,
                Piped::Stranger => return Err(Failed::Invalid),
                Piped::Closed => || Failed::Closed,
                Piped::Cut => || Failed::Cut,
                Piped::Invalid => || Failed::Invalid,
            };
            for reply in replies.iter_mut().filter(|reply| reply.is_none()) {
                *reply = Some(Err(failed()));
            }
        }
        Ok(replies
            .into_iter()
            .map(|reply| reply.unwrap_or(Err(Failed::Invalid)))
            .collect())
    }

    fn keep(&mut self, link: Link, (reply, open): (Vec<u8>, bool)) -> Vec<u8> {
        if open {
            self.link = Some(link);
        }
        reply
    }

    async fn connect(&mut self, reached: &mut bool) -> Result<Link, Failed> {
        let (config, name) = match &self.server.wire {
            Wire::Tls { name } => (&self.wires.dot, name.as_deref()),
            Wire::Https { .. } => (&self.wires.doh, None),
            Wire::Plain => unreachable!("plain DNS keeps no connection"),
        };
        // The certificate must name the server's address, or the name given.
        let verified = match name {
            Some(name) => {
                ServerName::try_from(name.to_owned()).map_err(|_| Failed::Tls(NOT_A_SERVER_NAME))?
            }
            None => ServerName::IpAddress(self.server.address.ip().into()),
        };
        let stream = self
            .provider
            .open(self.server)
            .await
            .map_err(Failed::Unreached)?;
        *reached = true;
        // A failed handshake or certificate fails this server. It is never
        // asked in plain instead.
        let tls = TlsConnector::from(config.clone())
            .connect(verified, stream)
            .await
            .map_err(|error| Failed::Tls(reason(&error)))?;
        if let Wire::Tls { .. } = self.server.wire {
            return Ok(Link::Tls(Box::new(tls)));
        }
        Ok(Link::Https(Box::new(
            super::https::Connection::new(tls).await?,
        )))
    }
}

// Why a TLS connection failed, in a few fixed words. Nothing the peer sent,
// such as the names on its certificate, reaches the detail.
const NOT_A_SERVER_NAME: &str = "the configured name is not a server name";
const WRONG_NAME: &str = "certificate not valid for this server";
const UNKNOWN_ISSUER: &str = "unknown issuer";
const OUT_OF_DATE: &str = "certificate expired or not yet valid";
const OTHER_CERTIFICATE: &str = "other certificate error";
const HANDSHAKE: &str = "handshake failed";
const CLOSED: &str = "connection closed";

fn reason(error: &std::io::Error) -> &'static str {
    use tokio_rustls::rustls::Error;
    let Some(error) = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<Error>())
    else {
        return CLOSED;
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

impl Link {
    // The reply, and whether the connection is still open for the next query.
    async fn exchange(
        &mut self,
        server: &DnsServer,
        packet: &[u8],
    ) -> Result<(Vec<u8>, bool), Failed> {
        match self {
            Self::Tls(stream) => framed(stream.as_mut(), packet)
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

// A batch's failure for the server: one that cannot be reached, or fails
// TLS, would fail every other query the same way.
fn ends(reply: Result<Vec<u8>, Failed>) -> Result<Result<Vec<u8>, Failed>, Failed> {
    match reply {
        Err(failed @ (Failed::Unreached(_) | Failed::Tls(_))) => Err(failed),
        reply => Ok(reply),
    }
}

// Plain DNS: a stream dialled for each of `packets`, all at once, on the
// same route to the same server, and each query asked on its own. `reached`
// is set as soon as any dial is answered, so a batch cut off by the ceiling
// still says so. The first dial that fails ends the batch.
async fn plain_all(
    provider: &dyn Carrier,
    server: &DnsServer,
    counts: bool,
    packets: &[Vec<u8>],
    reached: &mut bool,
) -> Result<Vec<Result<Vec<u8>, Failed>>, Failed> {
    type Call<'a> = Pin<Box<dyn Future<Output = Result<Vec<u8>, Failed>> + Send + 'a>>;
    let dialled = AtomicBool::new(false);
    let dialled = &dialled;
    let mut calls: Vec<Call<'_>> = packets
        .iter()
        .map(|packet| {
            Box::pin(async move {
                let mut stream = provider.open(server).await.map_err(Failed::Unreached)?;
                dialled.store(true, Ordering::Relaxed);
                sent(counts, 1);
                framed(&mut stream, packet).await
            }) as Call<'_>
        })
        .collect();
    let mut replies: Vec<Option<Result<Vec<u8>, Failed>>> = packets.iter().map(|_| None).collect();
    poll_fn(|cx| {
        for (call, reply) in calls.iter_mut().zip(replies.iter_mut()) {
            if reply.is_none()
                && let Poll::Ready(answered) = call.as_mut().poll(cx)
            {
                *reply = Some(answered);
            }
        }
        if dialled.load(Ordering::Relaxed) {
            *reached = true;
        }
        let ended = replies
            .iter()
            .any(|reply| matches!(reply, Some(Err(Failed::Unreached(_)))));
        if ended || replies.iter().all(Option::is_some) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    drop(calls);
    let mut answered = Vec::with_capacity(replies.len());
    for reply in replies {
        answered.push(ends(reply.unwrap_or(Err(Failed::Closed)))?);
    }
    Ok(answered)
}

// How a pipelined batch on one connection ended.
enum Piped {
    // Every reply came.
    Done,
    // The connection closed between replies, or before the batch went out.
    Closed,
    // The connection closed part way through a reply.
    Cut,
    // A reply's length was out of bounds.
    Invalid,
    // A reply came with an id no waiting query went out with.
    Stranger,
}

// Writes the queries at `waiting` together, then reads one reply for each,
// in any order, putting each in its query's slot of `replies` by id. Says
// how it ended, and how many replies came.
async fn pipeline<S>(
    stream: &mut S,
    packets: &[Vec<u8>],
    waiting: &[usize],
    replies: &mut [Option<Result<Vec<u8>, Failed>>],
) -> (Piped, usize)
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut ids = std::collections::BTreeMap::new();
    let mut framed = Vec::new();
    for at in waiting {
        let packet = &packets[*at];
        if let Some(id) = packet.get(..2) {
            ids.insert([id[0], id[1]], *at);
        }
        framed.extend_from_slice(&(packet.len() as u16).to_be_bytes());
        framed.extend_from_slice(packet);
    }
    if stream.write_all(&framed).await.is_err() || stream.flush().await.is_err() {
        return (Piped::Closed, 0);
    }
    let mut answered = 0;
    while answered < waiting.len() {
        let bytes = match reply(stream).await {
            Ok(bytes) => bytes,
            Err(Failed::Closed) => return (Piped::Closed, answered),
            Err(Failed::Cut) => return (Piped::Cut, answered),
            Err(_) => return (Piped::Invalid, answered),
        };
        // Each id is taken once: a second reply to one query is a stranger.
        let Some(at) = ids.remove(&[bytes[0], bytes[1]]) else {
            return (Piped::Stranger, answered);
        };
        replies[at] = Some(Ok(bytes));
        answered += 1;
    }
    (Piped::Done, answered)
}

// One query over a stream, with the two-byte length prefix of DNS over TCP.
// A stream that ends before the first byte of the reply is closed; one that
// ends part way cut the reply short.
async fn framed<S>(stream: &mut S, packet: &[u8]) -> Result<Vec<u8>, Failed>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut framed = Vec::with_capacity(packet.len() + 2);
    framed.extend_from_slice(&(packet.len() as u16).to_be_bytes());
    framed.extend_from_slice(packet);
    stream
        .write_all(&framed)
        .await
        .map_err(|_| Failed::Closed)?;
    stream.flush().await.map_err(|_| Failed::Closed)?;
    reply(stream).await
}

// One reply off a stream, by its two-byte length prefix, within the cap.
async fn reply<S>(stream: &mut S) -> Result<Vec<u8>, Failed>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut prefix = [0; 2];
    match stream.read(&mut prefix[..1]).await {
        Ok(1) => {}
        _ => return Err(Failed::Closed),
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
