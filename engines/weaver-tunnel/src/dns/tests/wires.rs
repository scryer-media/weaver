// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::trust::{counted, proxy_servers_trusting, sessions, since, through_naming};
use super::ttl::{ASKED, Answer, failure, respond, rr, soa, v4, wire};
use super::*;
use crate::dns::roots::Clock;
use crate::pipe::Naming;
use crate::resolution::record::{LookupError, Recording};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        RootCertStore, ServerConfig,
        crypto::aws_lc_rs,
        pki_types::{CertificateDer, PrivatePkcs8KeyDer},
    },
};

mod dnssec;
mod kept;

// A certificate authority the tests trust, or one they do not.
struct Authority {
    certificate: CertificateDer<'static>,
    issuer: Issuer<'static, KeyPair>,
}

impl Authority {
    fn new() -> Self {
        // A name of its own, so a certificate from another authority is
        // never taken for one of this one's.
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(
            rcgen::DnType::CommonName,
            format!("authority {}", NEXT.fetch_add(1, Ordering::Relaxed)),
        );
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap().der().clone();
        Self {
            certificate,
            issuer: Issuer::new(params, key),
        }
    }

    // A root store that holds only this authority.
    fn store(&self) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots.add(self.certificate.clone()).unwrap();
        roots
    }

    // Wires that trust only this authority.
    fn wires(&self) -> Wires {
        Wires::trusting(self.store())
    }

    // Trust roots that trust only this authority for TLS, and otherwise
    // what a product build trusts: the IANA root keys, the default policy
    // and the host clock.
    fn roots(&self) -> Arc<TrustRoots> {
        Arc::new(
            TrustRoots::new(
                self.store(),
                weaver_dnssec::TrustAnchor::iana_root(),
                weaver_dnssec::Policy::default(),
                Clock::Host,
            )
            .unwrap(),
        )
    }

    // A server's TLS settings, with a certificate for `names` (addresses or
    // DNS names) signed by this authority. An empty `alpn` offers none.
    fn server(&self, names: &[&str], alpn: &[u8]) -> Arc<ServerConfig> {
        let params = CertificateParams::new(
            names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let key = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &self.issuer).unwrap();
        let mut config =
            ServerConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![certificate.der().clone()],
                    PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
                )
                .unwrap();
        config.alpn_protocols = [alpn.to_vec()]
            .into_iter()
            .filter(|alpn| !alpn.is_empty())
            .collect();
        Arc::new(config)
    }
}

// A stream that keeps every byte the client sent.
struct Tap {
    inner: DuplexStream,
    seen: Arc<Mutex<Vec<u8>>>,
}

impl AsyncRead for Tap {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if polled.is_ready() {
            let read = buf.filled()[before..].to_vec();
            self.seen.lock().unwrap().extend(read);
        }
        polled
    }
}

impl AsyncWrite for Tap {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// What one HTTP request looked like to the server.
#[derive(Clone, Debug)]
struct Seen {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

// The whole response, as bytes, to a query and the reply `answer` made.
type Respond = dyn Fn(&[u8], Vec<u8>) -> Vec<u8> + Send + Sync;

// How a fixture server speaks once TLS is up.
#[derive(Clone)]
enum Speaks {
    // DNS over TCP framing; the connection closes after `close_after`
    // queries, when set.
    Dot {
        close_after: Option<usize>,
    },
    // HTTP/1.1, by hand. `respond` gets the query and the reply `answer`
    // made of it. With `dribble`, the response goes out a byte at a time.
    // The connection closes after a response that says `connection: close`.
    Doh {
        respond: Arc<Respond>,
        dribble: bool,
    },
    // HTTP/2. `respond` makes the response from the reply. With
    // `goaway_after`, each of the first that many connections meets its
    // first stream with a GOAWAY, answers nothing, and closes; later ones
    // answer. `misstep` is what each stream does first.
    H2 {
        respond: Arc<H2Respond>,
        goaway_after: Option<usize>,
        misstep: H2Misstep,
    },
}

// What an HTTP/2 fixture server does with each request before answering.
#[derive(Clone, Copy)]
enum H2Misstep {
    None,
    // Resets each stream after the first `after` instead of answering.
    Reset { after: usize },
    // Sends this many interim (103) responses first.
    Interims(usize),
    // Tries to push a response first, and logs what came of it.
    Push,
    // Refuses the first stream on each connection unhandled, and answers
    // the rest.
    RefuseFirst,
    // Refuses the second stream on each connection unhandled, as a server
    // over its limit of streams at once does (RFC 9113 §8.7), and answers
    // the rest.
    RefuseSecond,
    // Refuses every stream unhandled.
    RefuseEvery,
    // On the connections `goaway_after` sends away: holds the first stream
    // unanswered and sends the connection away gracefully, keeping that
    // stream, and refuses each stream after it unhandled.
    Graceful,
    // As `Graceful`, but answers the first stream once the next connection
    // is dialled.
    GracefulAnswers,
}

type H2Respond = dyn Fn(Vec<u8>) -> hyper::Response<Vec<u8>> + Send + Sync;

#[derive(Default)]
struct Log {
    // The address and port of each dial.
    dials: Vec<SocketAddr>,
    // Every byte the client sent on each connection, in dial order.
    sent: Vec<Arc<Mutex<Vec<u8>>>>,
    // Queries answered on each connection, in dial order.
    queries: Vec<Arc<AtomicUsize>>,
    requests: Vec<Seen>,
    // What each HTTP/2 push the server tried came to.
    pushes: Vec<Result<(), String>>,
    // HTTP/2 streams refused unhandled.
    refused: usize,
}

// DNS servers, by address, in process: encrypted ones, plain ones, and
// ones that take the connection and never say anything.
struct Servers {
    tls: Vec<(IpAddr, Arc<ServerConfig>)>,
    plain: Vec<IpAddr>,
    silent: Vec<IpAddr>,
    held: Mutex<Vec<DuplexStream>>,
    // Replies left across every connection, when bounded; a query past the
    // last is read and the connection closed without a reply.
    replies: Option<Arc<AtomicUsize>>,
    speaks: Speaks,
    answer: Arc<Answer>,
    log: Arc<Mutex<Log>>,
    // The round trip each dial, and each reply the client waits on, costs
    // on the paused clock, when set; and how many such waits there were.
    rtt: Option<std::time::Duration>,
    waits: Arc<AtomicUsize>,
    // The dial, counted from 0, that finds nothing listening, when set.
    unreached: Option<usize>,
    // The dial, counted from 0, that waits this long before it goes on, or
    // never goes on, when set.
    stalled: Option<(usize, Option<std::time::Duration>)>,
    // From the dial, counted from 0, on: how the server speaks, and its TLS.
    switched: Option<(usize, Speaks, Arc<ServerConfig>)>,
    // DNS over TLS: a query given no reply closes the connection, instead
    // of staying unanswered on it.
    closing: bool,
    // DNS over TLS: each reply goes out after a copy of it under an id no
    // query out has.
    straying: bool,
    // A permit for each dial.
    dialled: Arc<tokio::sync::Semaphore>,
    // Every read on a client's stream fails with this kind, when set, as a
    // stream the server reset does.
    broken: Option<std::io::ErrorKind>,
    // Client streams being dialled or open now, and the most at once.
    open: Arc<AtomicUsize>,
    most_open: Arc<AtomicUsize>,
    // A permit for each HTTP/2 stream held unanswered, and for each
    // connection that has ended on the server's side after its TLS
    // handshake.
    holding: Arc<tokio::sync::Semaphore>,
    ended: Arc<tokio::sync::Semaphore>,
    // The server's side of each TLS connection, to close it at will.
    served: Mutex<Vec<tokio::task::AbortHandle>>,
    // Each connection's switch that makes it take queries and answer none.
    muted: Mutex<Vec<Arc<AtomicBool>>>,
    // Each HTTP/1.1 connection's signal to answer 408 while it waits for a
    // request, and close.
    hung: Mutex<Vec<Arc<tokio::sync::Notify>>>,
    // DNS over TLS, once set: each reply goes after the reply sent before
    // it on its connection, sent again, as a server that repeats itself.
    repeating: Arc<AtomicBool>,
    // What is kept open through these servers, as through the stage a
    // lookup asks them through.
    keeper: crate::pipe::Keeper,
}

// One client stream counted from its dial until it is dropped.
struct Slot(Arc<AtomicUsize>);

impl Slot {
    fn new(open: &Arc<AtomicUsize>, most: &AtomicUsize) -> Self {
        let now = open.fetch_add(1, Ordering::Relaxed) + 1;
        most.fetch_max(now, Ordering::Relaxed);
        Self(open.clone())
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

// A client's end of a connection, with its slot.
struct Counted {
    inner: Box<dyn crate::TunnelStream>,
    _slot: Slot,
}

impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Counted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// A client's end of a connection whose every read fails with `kind`.
struct Broken {
    inner: Box<dyn crate::TunnelStream>,
    kind: std::io::ErrorKind,
}

impl AsyncRead for Broken {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(self.kind.into()))
    }
}

impl AsyncWrite for Broken {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// A client's end of a connection with a round trip's latency: a read that
// follows a write waits one round trip first, so replies the client asked
// for together arrive together.
struct Latency {
    inner: DuplexStream,
    rtt: std::time::Duration,
    wrote: bool,
    wait: Option<Pin<Box<tokio::time::Sleep>>>,
    waits: Arc<AtomicUsize>,
}

impl AsyncRead for Latency {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.wrote && self.wait.is_none() {
            self.wait = Some(Box::pin(tokio::time::sleep(self.rtt)));
            self.waits.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(wait) = self.wait.as_mut() {
            std::task::ready!(wait.as_mut().poll(cx));
            self.wait = None;
            self.wrote = false;
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Latency {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.wrote = true;
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Servers {
    fn new(
        speaks: Speaks,
        answer: impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        Self {
            tls: Vec::new(),
            plain: Vec::new(),
            silent: Vec::new(),
            held: Mutex::default(),
            replies: None,
            speaks,
            answer: Arc::new(answer),
            log: Default::default(),
            rtt: None,
            waits: Arc::default(),
            unreached: None,
            stalled: None,
            switched: None,
            closing: false,
            straying: false,
            dialled: Arc::new(tokio::sync::Semaphore::new(0)),
            broken: None,
            open: Arc::default(),
            most_open: Arc::default(),
            holding: Arc::new(tokio::sync::Semaphore::new(0)),
            ended: Arc::new(tokio::sync::Semaphore::new(0)),
            served: Mutex::default(),
            muted: Mutex::default(),
            hung: Mutex::default(),
            repeating: Arc::default(),
            keeper: crate::pipe::Keeper::default(),
        }
    }
    // From now on each DNS over TLS reply goes after the one before it on
    // its connection, sent again.
    fn repeat_from_now(&self) {
        self.repeating.store(true, Ordering::Relaxed);
    }
    // Every connection open now takes queries from now on and answers none,
    // staying open; a connection dialled later answers.
    fn mute_open(&self) {
        for muted in self.muted.lock().unwrap().iter() {
            muted.store(true, Ordering::Relaxed);
        }
    }
    // Every HTTP/1.1 connection open now, waiting for its next request,
    // answers 408 Request Timeout unasked and closes, as a server does when
    // a connection sits idle past its timeout.
    fn hang_up_open(&self) {
        for hung in self.hung.lock().unwrap().iter() {
            hung.notify_one();
        }
    }
    fn unreached_at(mut self, dial: usize) -> Self {
        self.unreached = Some(dial);
        self
    }
    // `dial` waits `wait` before it goes on, or never with None.
    fn stalled_at(mut self, dial: usize, wait: Option<std::time::Duration>) -> Self {
        self.stalled = Some((dial, wait));
        self
    }
    // From `dial` on, the server speaks `speaks` with TLS `config`.
    fn switched_at(mut self, dial: usize, speaks: Speaks, config: Arc<ServerConfig>) -> Self {
        self.switched = Some((dial, speaks, config));
        self
    }
    fn closing_unanswered(mut self) -> Self {
        self.closing = true;
        self
    }
    fn straying(mut self) -> Self {
        self.straying = true;
        self
    }
    fn broken(mut self, kind: std::io::ErrorKind) -> Self {
        self.broken = Some(kind);
        self
    }
    // The most client streams being dialled or open at once.
    fn most_open(&self) -> usize {
        self.most_open.load(Ordering::Relaxed)
    }
    fn delayed(mut self, rtt: std::time::Duration) -> Self {
        self.rtt = Some(rtt);
        self
    }
    // The round trips waited on so far: one per dial, one per reply waited.
    fn waits(&self) -> usize {
        self.waits.load(Ordering::Relaxed)
    }
    fn at(mut self, ip: &str, config: Arc<ServerConfig>) -> Self {
        self.tls.push((ip.parse().unwrap(), config));
        self
    }
    fn plain_at(mut self, ip: &str) -> Self {
        self.plain.push(ip.parse().unwrap());
        self
    }
    fn silent_at(mut self, ip: &str) -> Self {
        self.silent.push(ip.parse().unwrap());
        self
    }
    fn replying(mut self, replies: usize) -> Self {
        self.replies = Some(Arc::new(AtomicUsize::new(replies)));
        self
    }
    // Every byte the client sent, on every connection.
    fn sent(&self) -> Vec<Vec<u8>> {
        let log = self.log.lock().unwrap();
        log.sent
            .iter()
            .map(|sent| sent.lock().unwrap().clone())
            .collect()
    }
    fn answered(&self) -> Vec<usize> {
        let log = self.log.lock().unwrap();
        log.queries
            .iter()
            .map(|queries| queries.load(Ordering::Relaxed))
            .collect()
    }
    fn dials(&self) -> Vec<SocketAddr> {
        self.log.lock().unwrap().dials.clone()
    }
    // Client streams being dialled or open now.
    fn open_now(&self) -> usize {
        self.open.load(Ordering::Relaxed)
    }
    // The server closes every TLS connection it holds, saying nothing.
    fn close_all(&self) {
        for served in self.served.lock().unwrap().drain(..) {
            served.abort();
        }
    }
}

#[async_trait::async_trait]
impl Carrier for Servers {
    fn keepers(&self) -> Vec<crate::pipe::KeeperRef> {
        vec![self.keeper.handle()]
    }

    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        let (client, inner) = tokio::io::duplex(65536);
        let muted = Arc::new(AtomicBool::new(false));
        self.muted.lock().unwrap().push(muted.clone());
        let hung = Arc::new(tokio::sync::Notify::new());
        self.hung.lock().unwrap().push(hung.clone());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let queries = Arc::new(AtomicUsize::new(0));
        let dial = {
            let mut log = self.log.lock().unwrap();
            log.dials.push(server.address);
            log.sent.push(seen.clone());
            log.queries.push(queries.clone());
            log.dials.len() - 1
        };
        self.dialled.add_permits(1);
        if self.unreached == Some(dial) {
            return Err(TunnelError::Engine(format!("dial {dial} reaches nothing")));
        }
        match self.stalled {
            Some((stalled, Some(wait))) if stalled == dial => tokio::time::sleep(wait).await,
            Some((stalled, None)) if stalled == dial => std::future::pending().await,
            _ => {}
        }
        let slot = Slot::new(&self.open, &self.most_open);
        let client: Box<dyn crate::TunnelStream> = match self.rtt {
            Some(rtt) => {
                self.waits.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(rtt).await;
                Box::new(Latency {
                    inner: client,
                    rtt,
                    wrote: false,
                    wait: None,
                    waits: self.waits.clone(),
                })
            }
            None => Box::new(client),
        };
        let mut client: Box<dyn crate::TunnelStream> = Box::new(Counted {
            inner: client,
            _slot: slot,
        });
        if let Some(kind) = self.broken {
            client = Box::new(Broken {
                inner: client,
                kind,
            });
        }
        let asked = server.address.ip();
        let answer = self.answer.clone();
        let replies = self.replies.clone();
        let (closing, straying, repeating) = (self.closing, self.straying, self.repeating.clone());
        if self.silent.contains(&asked) {
            self.held.lock().unwrap().push(inner);
            return Ok(client);
        }
        if self.plain.contains(&asked) {
            tokio::spawn(async move {
                let mut stream = Tap { inner, seen };
                framed_replies(
                    &mut stream,
                    asked,
                    &*answer,
                    (None, closing, false, &repeating),
                    None,
                    (&queries, &muted),
                )
                .await;
            });
            return Ok(client);
        }
        let Some((_, config)) = self.tls.iter().find(|(ip, _)| *ip == asked) else {
            return Err(TunnelError::Engine(format!("nothing listens at {asked}")));
        };
        let (speaks, config) = match &self.switched {
            Some((from, speaks, config)) if dial >= *from => (speaks.clone(), config),
            _ => (self.speaks.clone(), config),
        };
        let acceptor = TlsAcceptor::from(config.clone());
        let log = self.log.clone();
        let (holding, ended, dialled) = (
            self.holding.clone(),
            self.ended.clone(),
            self.dialled.clone(),
        );
        let served = tokio::spawn(async move {
            let Ok(mut tls) = acceptor.accept(Tap { inner, seen }).await else {
                return;
            };
            let served = async move {
                match speaks {
                    Speaks::Dot { close_after } => {
                        framed_replies(
                            &mut tls,
                            asked,
                            &*answer,
                            (close_after, closing, straying, &repeating),
                            replies.as_deref(),
                            (&queries, &muted),
                        )
                        .await
                    }
                    Speaks::Doh { respond, dribble } => loop {
                        let next = tokio::select! {
                            seen = read_request(&mut tls) => Some(seen),
                            () = hung.notified() => None,
                        };
                        let Some(next) = next else {
                            let timeout = http("408 Request Timeout", &["Connection: close"], b"");
                            let _ = tls.write_all(&timeout).await;
                            let _ = tls.flush().await;
                            let _ = tls.shutdown().await;
                            return;
                        };
                        let Some(seen) = next else {
                            return;
                        };
                        // Past the last reply, the connection closes unanswered
                        // and without saying so before.
                        if replies.as_deref().is_some_and(|left| {
                            left.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                                left.checked_sub(1)
                            })
                            .is_err()
                        }) {
                            return;
                        }
                        queries.fetch_add(1, Ordering::Relaxed);
                        let body = seen.body.clone();
                        log.lock().unwrap().requests.push(seen);
                        let reply = answer(asked, u16_at(&body, body.len() - 4).unwrap(), &body);
                        // No reply made, or the connection muted: the request is
                        // never answered, and the connection stays open.
                        if reply.is_empty() || muted.load(Ordering::Relaxed) {
                            let _held = tls;
                            return std::future::pending().await;
                        }
                        let response = respond(&body, reply);
                        let sent = if dribble {
                            let mut sent = Ok(());
                            for byte in &response {
                                sent = sent.and(tls.write_all(&[*byte]).await);
                                sent = sent.and(tls.flush().await);
                            }
                            sent
                        } else {
                            tls.write_all(&response).await.and(tls.flush().await)
                        };
                        let closes = response
                            .to_ascii_lowercase()
                            .windows(17)
                            .any(|bytes| bytes == b"connection: close");
                        if sent.is_err() || closes {
                            return;
                        }
                    },
                    Speaks::H2 {
                        respond,
                        goaway_after,
                        misstep,
                    } => {
                        serve_h2(
                            tls,
                            asked,
                            answer,
                            (respond, misstep),
                            goaway_after.is_some_and(|connections| dial < connections),
                            (queries, log, holding, dialled),
                            muted,
                        )
                        .await;
                    }
                }
            };
            served.await;
            ended.add_permits(1);
        });
        self.served.lock().unwrap().push(served.abort_handle());
        Ok(client)
    }
}

// An HTTP/2 server on one connection: each request is answered on a task of
// its own while the connection is driven here.
async fn serve_h2<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    asked: IpAddr,
    answer: Arc<Answer>,
    (respond, misstep): (Arc<H2Respond>, H2Misstep),
    goaway: bool,
    (queries, log, holding, dialled): (
        Arc<AtomicUsize>,
        Arc<Mutex<Log>>,
        Arc<tokio::sync::Semaphore>,
        Arc<tokio::sync::Semaphore>,
    ),
    muted: Arc<AtomicBool>,
) {
    let Ok(mut connection) = h2::server::handshake(stream).await else {
        return;
    };
    let mut accepted = 0;
    while let Some(Ok((request, mut send))) = connection.accept().await {
        accepted += 1;
        if goaway && matches!(misstep, H2Misstep::Graceful | H2Misstep::GracefulAnswers) {
            if accepted == 1 {
                connection.graceful_shutdown();
                let holding = holding.clone();
                if matches!(misstep, H2Misstep::GracefulAnswers) {
                    let (dialled, answer, respond) =
                        (dialled.clone(), answer.clone(), respond.clone());
                    tokio::spawn(async move {
                        let mut body = request.into_body();
                        let mut query = Vec::new();
                        while let Some(Ok(chunk)) = body.data().await {
                            let _ = body.flow_control().release_capacity(chunk.len());
                            query.extend_from_slice(&chunk);
                        }
                        holding.add_permits(1);
                        // This connection's dial and the next one's.
                        dialled.acquire_many(2).await.unwrap().forget();
                        let reply = answer(asked, u16_at(&query, query.len() - 4).unwrap(), &query);
                        let (head, bytes) = respond(reply).into_parts();
                        if let Ok(mut stream) =
                            send.send_response(hyper::Response::from_parts(head, ()), false)
                        {
                            let _ = stream.send_data(bytes.into(), true);
                        }
                    });
                    continue;
                }
                tokio::spawn(async move {
                    let _held = (request, send);
                    holding.add_permits(1);
                    std::future::pending::<()>().await
                });
            } else {
                log.lock().unwrap().refused += 1;
                send.send_reset(h2::Reason::REFUSED_STREAM);
            }
            continue;
        }
        if goaway {
            connection.abrupt_shutdown(h2::Reason::NO_ERROR);
            continue;
        }
        let refuse = match misstep {
            H2Misstep::RefuseFirst => accepted == 1,
            H2Misstep::RefuseSecond => accepted == 2,
            H2Misstep::RefuseEvery => true,
            _ => false,
        };
        if refuse {
            log.lock().unwrap().refused += 1;
            send.send_reset(h2::Reason::REFUSED_STREAM);
            continue;
        }
        let (answer, respond, queries, log, holding, muted) = (
            answer.clone(),
            respond.clone(),
            queries.clone(),
            log.clone(),
            holding.clone(),
            muted.clone(),
        );
        tokio::spawn(async move {
            let (parts, mut body) = request.into_parts();
            let mut query = Vec::new();
            while let Some(Ok(chunk)) = body.data().await {
                let _ = body.flow_control().release_capacity(chunk.len());
                query.extend_from_slice(&chunk);
            }
            // The connection went before the query came whole: fewer bytes
            // than the request's length says, or none.
            let length = parts
                .headers
                .get(hyper::header::CONTENT_LENGTH)
                .and_then(|length| length.to_str().ok()?.parse::<usize>().ok());
            if query.is_empty() || length.is_some_and(|length| query.len() != length) {
                return;
            }
            queries.fetch_add(1, Ordering::Relaxed);
            log.lock().unwrap().requests.push(Seen {
                method: parts.method.to_string(),
                target: parts.uri.to_string(),
                headers: parts
                    .headers
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
                    .collect(),
                body: query.clone(),
            });
            match misstep {
                H2Misstep::None
                | H2Misstep::RefuseFirst
                | H2Misstep::RefuseSecond
                | H2Misstep::RefuseEvery
                | H2Misstep::Graceful
                | H2Misstep::GracefulAnswers => {}
                H2Misstep::Reset { after } if accepted > after => {
                    return send.send_reset(h2::Reason::INTERNAL_ERROR);
                }
                H2Misstep::Reset { .. } => {}
                H2Misstep::Interims(count) => {
                    for _ in 0..count {
                        let interim = hyper::Response::builder().status(103).body(()).unwrap();
                        if send.send_informational(interim).is_err() {
                            return;
                        }
                    }
                }
                H2Misstep::Push => {
                    let pushed = hyper::Request::get("https://192.0.2.53/pushed")
                        .body(())
                        .unwrap();
                    let pushed = send
                        .push_request(pushed)
                        .map(drop)
                        .map_err(|error| error.to_string());
                    log.lock().unwrap().pushes.push(pushed);
                }
            }
            let reply = answer(asked, u16_at(&query, query.len() - 4).unwrap(), &query);
            // No reply made, or the connection muted: the stream is held
            // open and never answered.
            if reply.is_empty() || muted.load(Ordering::Relaxed) {
                let _held = send;
                holding.add_permits(1);
                return std::future::pending().await;
            }
            let (head, bytes) = respond(reply).into_parts();
            let Ok(mut stream) = send.send_response(hyper::Response::from_parts(head, ()), false)
            else {
                return;
            };
            let _ = stream.send_data(bytes.into(), true);
        });
    }
}

// `reply` under an id no query out with it has: its own with the top bit
// flipped. A lookup's queries out at once have neighbouring ids, so none
// of them has this one.
fn stray(mut reply: Vec<u8>) -> Vec<u8> {
    reply[0] ^= 0x80;
    reply
}

// DNS over TCP replies to each query read, until the client goes, the
// connection has answered `close_after`, or no `replies` are left. With
// `closing`, a query given no reply closes the connection. With `straying`,
// each reply goes out after a copy of it under a stray id. Once
// `repeating`, each reply goes after the one sent before it, sent again.
// Once `muted`, queries are read and none answered.
async fn framed_replies<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    asked: IpAddr,
    answer: &Answer,
    (close_after, closing, straying, repeating): (Option<usize>, bool, bool, &AtomicBool),
    replies: Option<&AtomicUsize>,
    (queries, muted): (&AtomicUsize, &AtomicBool),
) {
    let mut before: Vec<u8> = Vec::new();
    loop {
        if close_after.is_some_and(|after| queries.load(Ordering::Relaxed) >= after) {
            return;
        }
        let Ok(size) = stream.read_u16().await else {
            return;
        };
        let mut query = vec![0; size as usize];
        if stream.read_exact(&mut query).await.is_err() {
            return;
        }
        if replies.is_some_and(|left| {
            left.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(1)
            })
            .is_err()
        }) {
            return;
        }
        queries.fetch_add(1, Ordering::Relaxed);
        let reply = answer(asked, u16_at(&query, query.len() - 4).unwrap(), &query);
        // No reply made: the query goes unanswered, the connection open, or
        // closed when `closing`.
        if reply.is_empty() && closing {
            return;
        }
        if reply.is_empty() || muted.load(Ordering::Relaxed) {
            continue;
        }
        let mut this = Vec::new();
        if straying {
            this.extend_from_slice(&(reply.len() as u16).to_be_bytes());
            this.extend_from_slice(&stray(reply.clone()));
        }
        this.extend_from_slice(&(reply.len() as u16).to_be_bytes());
        this.extend(reply);
        let mut framed = std::mem::replace(&mut before, this.clone());
        if !repeating.load(Ordering::Relaxed) {
            framed.clear();
        }
        framed.extend_from_slice(&this);
        if stream.write_all(&framed).await.is_err() || stream.flush().await.is_err() {
            return;
        }
    }
}

fn dot(ip: &str, name: Option<&str>) -> DnsServer {
    DnsServer {
        address: SocketAddr::new(ip.parse().unwrap(), 853),
        wire: Wire::Tls {
            name: name.map(str::to_owned),
        },
    }
}

fn doh(address: &str) -> DnsServer {
    DnsServer {
        address: address.parse().unwrap(),
        wire: Wire::Https {
            path: "/dns-query".into(),
        },
    }
}

fn answers(_: IpAddr, qtype: u16, query: &[u8]) -> Vec<u8> {
    match qtype {
        1 => v4(query, 60),
        _ => respond(query, 0, &[], &[soa(600, 60)]),
    }
}

// One request as the fixture server reads it, or None once the client is
// gone.
async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> Option<Seen> {
    let mut buffer = Vec::new();
    let end = loop {
        if let Some(at) = buffer.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break at + 4;
        }
        let mut chunk = [0; 1024];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut headers);
    request.parse(&buffer[..end]).unwrap();
    let headers: Vec<(String, String)> = request
        .headers
        .iter()
        .map(|header| {
            (
                header.name.to_ascii_lowercase(),
                String::from_utf8(header.value.to_vec()).unwrap(),
            )
        })
        .collect();
    let length: usize = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .map(|(_, value)| value.parse().unwrap())
        .unwrap_or(0);
    let mut seen = Seen {
        method: request.method.unwrap().to_owned(),
        target: request.path.unwrap().to_owned(),
        headers,
        body: buffer[end..].to_vec(),
    };
    while seen.body.len() < length {
        let mut chunk = [0; 1024];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        seen.body.extend_from_slice(&chunk[..read]);
    }
    Some(seen)
}

// A response of `status`, with `headers` and then `body`.
fn http(status: &str, headers: &[&str], body: &[u8]) -> Vec<u8> {
    let mut response = format!("HTTP/1.1 {status}\r\n").into_bytes();
    for header in headers {
        response.extend_from_slice(header.as_bytes());
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"\r\n");
    response.extend_from_slice(body);
    response
}

fn dns_message(reply: Vec<u8>) -> Vec<u8> {
    http(
        "200 OK",
        &[
            "content-type: application/dns-message",
            &format!("content-length: {}", reply.len()),
        ],
        &reply,
    )
}

fn plain_reply(_: &[u8], reply: Vec<u8>) -> Vec<u8> {
    dns_message(reply)
}

// `body` in chunks of `size` bytes, ended by a trailer field.
fn chunked(body: &[u8], size: usize) -> Vec<u8> {
    let mut encoded = Vec::new();
    for chunk in body.chunks(size) {
        encoded.extend_from_slice(format!("{:x};note=1\r\n", chunk.len()).as_bytes());
        encoded.extend_from_slice(chunk);
        encoded.extend_from_slice(b"\r\n");
    }
    encoded.extend_from_slice(b"0\r\nx-trailer: dropped\r\n\r\n");
    encoded
}

fn seven() -> Vec<IpAddr> {
    vec!["192.0.2.7".parse().unwrap()]
}

// What a test recording says of a lookup through `servers`.
async fn recorded(
    carrier: &Servers,
    wires: &Wires,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> record::HostResolution {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            record::asking(host, || record::Asker::servers(servers, trust));
            if let Ok(lookup) = resolve(carrier, wires, servers, trust, host).await {
                record::found(host, &lookup.addresses);
            }
        })
        .await;
    recording.finish(host, 80, None).expect("a resolution")
}

fn tls_failed(server: &str, reason: &str) -> Option<LookupError> {
    Some(failure(
        LookupFailure::TlsFailed,
        &format!("{server}: TLS failed: {reason}"),
    ))
}

// The TLS record types in what a client sent.
fn records(sent: &[u8]) -> Vec<u8> {
    let mut kinds = Vec::new();
    let mut at = 0;
    while at + 5 <= sent.len() {
        kinds.push(sent[at]);
        at += 5 + u16::from_be_bytes([sent[at + 3], sent[at + 4]]) as usize;
    }
    kinds
}

#[tokio::test(start_paused = true)]
async fn dot_verifies_a_certificate_for_the_servers_address() {
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let lookup = resolve(
        &servers,
        &authority.wires(),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await
    .unwrap();
    assert_eq!(lookup.addresses, seven());
    assert_eq!(servers.dials(), ["192.0.2.53:853".parse().unwrap()]);
}

#[tokio::test(start_paused = true)]
async fn dot_fails_a_certificate_that_does_not_name_the_address_and_never_asks_in_plain() {
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers).at(
        "192.0.2.53",
        authority.server(&["192.0.2.99", "dns.test"], b"dot"),
    );
    let resolution = recorded(
        &servers,
        &authority.wires(),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(
        resolution.error,
        tls_failed("192.0.2.53", "certificate not valid for this server")
    );
    // One dial, to the TLS port. No second family, nothing on port 53.
    assert_eq!(servers.dials(), ["192.0.2.53:853".parse().unwrap()]);
    // The client sent a ClientHello and at most an alert: no record of
    // data, and no query in the clear.
    let sent = servers.log.lock().unwrap().sent[0].lock().unwrap().clone();
    assert_eq!(sent[5], 1, "the first handshake message is a ClientHello");
    let kinds = records(&sent);
    assert_eq!(kinds[0], 0x16);
    assert!(
        kinds.iter().all(|kind| matches!(kind, 0x14..=0x16)),
        "{kinds:?}"
    );
    assert!(!sent.windows(7).any(|bytes| bytes == b"fixture"));
}

#[tokio::test(start_paused = true)]
async fn dot_with_a_name_verifies_against_the_name_and_sends_it() {
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", authority.server(&["dns.test"], b"dot"));
    let wires = authority.wires();
    let lookup = resolve(
        &servers,
        &wires,
        &[dot("192.0.2.53", Some("dns.test"))],
        Trust::AsSent,
        "fixture.test",
    )
    .await
    .unwrap();
    assert_eq!(lookup.addresses, seven());
    // The name rides in the ClientHello as SNI.
    let sent = servers.log.lock().unwrap().sent[0].lock().unwrap().clone();
    assert!(sent.windows(8).any(|bytes| bytes == b"dns.test"));

    let resolution = recorded(
        &servers,
        &wires,
        &[dot("192.0.2.53", Some("wrong.test"))],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(
        resolution.error,
        tls_failed("192.0.2.53", "certificate not valid for this server")
    );
}

#[tokio::test(start_paused = true)]
async fn dot_fails_a_certificate_from_an_untrusted_issuer() {
    let trusted = Authority::new();
    let stranger = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"dot"));
    let resolution = recorded(
        &servers,
        &trusted.wires(),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(resolution.error, tls_failed("192.0.2.53", "unknown issuer"));
    // No answer came to judge.
    assert_eq!(resolution.trust, None);
}

// The built-in TLS roots from a host store holding `host`, unreadable for
// `errors`. The compiled-in roots always lead them.
fn built_from(host: Vec<CertificateDer<'static>>, errors: &[String]) -> RootCertStore {
    let roots = crate::dns::roots::tls_roots(host, errors);
    assert_eq!(
        &roots.roots[..webpki_roots::TLS_SERVER_ROOTS.len()],
        webpki_roots::TLS_SERVER_ROOTS
    );
    roots
}

#[tokio::test(start_paused = true)]
async fn a_certificate_chained_only_to_a_root_from_the_host_store_is_trusted() {
    let operators = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", operators.server(&["192.0.2.53"], b"dot"));
    let roots = built_from(vec![operators.certificate.clone()], &[]);
    assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len() + 1);
    let lookup = resolve(
        &servers,
        &Wires::trusting(roots),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await
    .unwrap();
    assert_eq!(lookup.addresses, seven());
}

#[tokio::test(start_paused = true)]
async fn a_certificate_chained_to_neither_the_built_in_nor_the_host_roots_is_not_trusted() {
    let operators = Authority::new();
    let stranger = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"dot"));
    let resolution = recorded(
        &servers,
        &Wires::trusting(built_from(vec![operators.certificate.clone()], &[])),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(resolution.error, tls_failed("192.0.2.53", "unknown issuer"));
}

#[tokio::test(start_paused = true)]
async fn an_unparsable_host_certificate_is_skipped_and_the_rest_still_load() {
    let operators = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", operators.server(&["192.0.2.53"], b"dot"));
    let roots = built_from(
        vec![
            CertificateDer::from(vec![1, 2, 3]),
            operators.certificate.clone(),
        ],
        &[],
    );
    assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len() + 1);
    let lookup = resolve(
        &servers,
        &Wires::trusting(roots),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await
    .unwrap();
    assert_eq!(lookup.addresses, seven());
}

#[test]
fn a_host_store_read_that_panics_reads_nothing_and_leaves_the_compiled_in_roots() {
    let (certificates, errors) =
        crate::dns::roots::read_host_store(|| panic!("a store entry would not open"));
    assert!(certificates.is_empty());
    assert_eq!(
        errors,
        ["reading the store panicked: a store entry would not open"]
    );
    let roots = built_from(certificates, &errors);
    assert_eq!(roots.roots, webpki_roots::TLS_SERVER_ROOTS);
}

#[tokio::test(start_paused = true)]
async fn an_unreadable_host_store_leaves_the_compiled_in_roots_working() {
    // Every compiled-in root, and nothing besides.
    let roots = built_from(Vec::new(), &["could not load PEM file".to_string()]);
    assert_eq!(roots.roots, webpki_roots::TLS_SERVER_ROOTS);
    // Lookups still go out over TLS and judge the server's certificate.
    let operators = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", operators.server(&["192.0.2.53"], b"dot"));
    let resolution = recorded(
        &servers,
        &Wires::trusting(roots),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(resolution.error, tls_failed("192.0.2.53", "unknown issuer"));
}

#[tokio::test(start_paused = true)]
async fn a_server_failing_tls_is_passed_over_for_the_next() {
    let trusted = Authority::new();
    let stranger = Authority::new();
    let servers = Servers::new(
        Speaks::Dot { close_after: None },
        |asked, qtype, query| match qtype {
            1 if asked == "192.0.2.54".parse::<IpAddr>().unwrap() => {
                respond(query, 0, &[rr(&ASKED, 1, 60, &[192, 0, 2, 9])], &[])
            }
            _ => answers(asked, qtype, query),
        },
    )
    .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"dot"))
    .at("192.0.2.54", trusted.server(&["192.0.2.54"], b"dot"));
    let resolution = recorded(
        &servers,
        &trusted.wires(),
        &[dot("192.0.2.53", None), dot("192.0.2.54", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(
        resolution.addresses,
        ["192.0.2.9".parse::<IpAddr>().unwrap()]
    );
    assert_eq!(resolution.error, None);
    assert_eq!(resolution.wire, Some(WireKind::Tls));
    assert_eq!(resolution.answered_by, Some("192.0.2.54".parse().unwrap()));
    assert_eq!(
        servers.dials(),
        [
            "192.0.2.53:853".parse::<SocketAddr>().unwrap(),
            "192.0.2.54:853".parse().unwrap()
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_server_failing_https_is_passed_over_and_the_record_names_the_next() {
    let trusted = Authority::new();
    let stranger = Authority::new();
    let servers = Servers::new(doh_fixture(plain_reply, false), answers)
        .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"http/1.1"))
        .at("192.0.2.54", trusted.server(&["192.0.2.54"], b"http/1.1"));
    let resolution = recorded(
        &servers,
        &trusted.wires(),
        &[doh("192.0.2.53:443"), doh("192.0.2.54:443")],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.wire, Some(WireKind::Https));
    assert_eq!(resolution.answered_by, Some("192.0.2.54".parse().unwrap()));
    assert_eq!(resolution.trust, Some(TrustOutcome::AsSent));
}

// Both families and an alias, three queries in all.
fn alias(_: IpAddr, qtype: u16, query: &[u8]) -> Vec<u8> {
    let target = query.windows(6).any(|name| name == b"target");
    match (qtype, target) {
        (1, false) => respond(query, 0, &[rr(&ASKED, 5, 60, &wire("target.test"))], &[]),
        (1, true) => v4(query, 60),
        _ => respond(query, 0, &[], &[soa(600, 60)]),
    }
}

#[tokio::test(start_paused = true)]
async fn dot_asks_both_families_and_an_alias_on_one_connection() {
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, alias)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let lookup = resolve(
        &servers,
        &authority.wires(),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "alias.test",
    )
    .await
    .unwrap();
    assert_eq!(lookup.addresses, seven());
    let log = servers.log.lock().unwrap();
    assert_eq!(log.dials.len(), 1);
    assert_eq!(log.queries[0].load(Ordering::Relaxed), 3);
}

// An alias, its target, and an IPv6 address: three queries.
fn alias_and_six(asked: IpAddr, qtype: u16, query: &[u8]) -> Vec<u8> {
    match qtype {
        28 => respond(query, 0, &[rr(&ASKED, 28, 60, &SIX)], &[]),
        _ => alias(asked, qtype, query),
    }
}

fn seven_then_six(_: IpAddr, qtype: u16, query: &[u8]) -> Vec<u8> {
    match qtype {
        1 => v4(query, 60),
        _ => respond(query, 0, &[rr(&ASKED, 28, 60, &SIX)], &[]),
    }
}

const SIX: [u8; 16] = [0x20, 1, 0xd, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7];

fn seven_and_six() -> Vec<IpAddr> {
    vec!["192.0.2.7".parse().unwrap(), "2001:db8::7".parse().unwrap()]
}

#[tokio::test(start_paused = true)]
async fn a_server_that_closes_after_every_reply_still_answers_every_query() {
    let authority = Authority::new();
    let servers = Servers::new(
        Speaks::Dot {
            close_after: Some(1),
        },
        alias_and_six,
    )
    .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let resolution = recorded(
        &servers,
        &authority.wires(),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "alias.test",
    )
    .await;
    // The alias, its target and the IPv6 address each took a connection.
    assert_eq!(resolution.addresses, seven_and_six());
    assert_eq!(resolution.error, None);
    assert_eq!(servers.dials().len(), 3);
    assert_eq!(servers.answered(), [1, 1, 1]);
}

#[tokio::test(start_paused = true)]
async fn a_kept_connection_found_dead_is_replaced_once_and_no_more() {
    // The first query is answered; every connection after it is taken and
    // closed without a reply.
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, seven_then_six)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"))
        .replying(1);
    let resolution = recorded(
        &servers,
        &authority.wires(),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(resolution.addresses, seven());
    // The IPv6 query found the kept connection dead, opened one more, found
    // that closed too, and stopped there.
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(servers.answered(), [1, 0]);

    let servers = Servers::new(Speaks::Dot { close_after: None }, seven_then_six)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"))
        .replying(0);
    let resolution = recorded(
        &servers,
        &authority.wires(),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    // A new connection that closes before any reply is not retried. Both
    // families went out on it together, so it is the only one.
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::NoReply,
            "192.0.2.53: the connection closed without a reply"
        ))
    );
    assert_eq!(servers.dials().len(), 1);
}

fn doh_fixture(
    respond: impl Fn(&[u8], Vec<u8>) -> Vec<u8> + Send + Sync + 'static,
    dribble: bool,
) -> Speaks {
    Speaks::Doh {
        respond: Arc::new(respond),
        dribble,
    }
}

#[tokio::test(start_paused = true)]
async fn doh_posts_the_query_with_id_zero_to_the_path_at_the_servers_address() {
    let authority = Authority::new();
    let servers = Servers::new(doh_fixture(plain_reply, false), answers)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"http/1.1"))
        .at(
            "2001:db8::53",
            authority.server(&["2001:db8::53"], b"http/1.1"),
        );
    let wires = authority.wires();
    for (server, host) in [
        (doh("192.0.2.53:443"), "192.0.2.53"),
        (doh("[2001:db8::53]:8443"), "[2001:db8::53]:8443"),
    ] {
        let lookup = resolve(&servers, &wires, &[server], Trust::AsSent, "fixture.test")
            .await
            .unwrap();
        assert_eq!(lookup.addresses, seven());
        let requests = std::mem::take(&mut servers.log.lock().unwrap().requests);
        assert_eq!(requests.len(), 2);
        for seen in requests {
            assert_eq!(seen.method, "POST");
            assert_eq!(seen.target, "/dns-query");
            assert_eq!(
                seen.headers,
                [
                    ("host".to_owned(), host.to_owned()),
                    (
                        "content-type".to_owned(),
                        "application/dns-message".to_owned()
                    ),
                    ("accept".to_owned(), "application/dns-message".to_owned()),
                    ("content-length".to_owned(), seen.body.len().to_string()),
                ]
            );
            assert_eq!(seen.body[..2], [0, 0]);
        }
    }
    // HTTP/1.1 asks each family on a connection of its own.
    assert_eq!(servers.dials().len(), 4);
}

fn doh_servers(
    respond: impl Fn(&[u8], Vec<u8>) -> Vec<u8> + Send + Sync + 'static,
) -> (Servers, Wires) {
    doh_servers_speaking(doh_fixture(respond, false))
}

fn doh_servers_speaking(speaks: Speaks) -> (Servers, Wires) {
    doh_servers_answering(speaks, answers)
}

fn doh_servers_answering(
    speaks: Speaks,
    answer: impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> (Servers, Wires) {
    let authority = Authority::new();
    let servers = Servers::new(speaks, answer)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"http/1.1"));
    (servers, authority.wires())
}

// What a lookup through `servers` came to, under `trust`.
async fn doh_lookup(servers: &Servers, wires: &Wires, trust: Trust) -> record::HostResolution {
    recorded(
        servers,
        wires,
        &[doh("192.0.2.53:443")],
        trust,
        "fixture.test",
    )
    .await
}

fn invalid() -> Option<LookupError> {
    Some(failure(
        LookupFailure::Other,
        &format!("192.0.2.53: {INVALID}"),
    ))
}

// Asserts a lookup answered by `respond` is an invalid response.
async fn refused_as_invalid(respond: impl Fn(&[u8], Vec<u8>) -> Vec<u8> + Send + Sync + 'static) {
    let (servers, wires) = doh_servers(respond);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.error, invalid(), "{resolution:?}");
    assert!(resolution.addresses.is_empty());
}

#[tokio::test(start_paused = true)]
async fn doh_fails_a_status_other_than_200() {
    let (servers, wires) = doh_servers(|_, _| {
        http(
            "503 Service Unavailable",
            &["content-type: application/dns-message", "content-length: 0"],
            b"",
        )
    });
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::ServerFailure,
            "192.0.2.53: HTTP 503"
        ))
    );
}

#[tokio::test(start_paused = true)]
async fn doh_takes_a_dns_message_with_parameters_and_refuses_another_type() {
    let (servers, wires) = doh_servers(|_, reply| {
        http(
            "200 OK",
            &[
                "Content-Type: Application/DNS-Message; charset=x",
                &format!("content-length: {}", reply.len()),
            ],
            &reply,
        )
    });
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    for kind in ["text/html", "application/json; x=application/dns-message"] {
        refused_as_invalid(move |_, reply| {
            http(
                "200 OK",
                &[
                    &format!("content-type: {kind}"),
                    &format!("content-length: {}", reply.len()),
                ],
                &reply,
            )
        })
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn doh_fails_a_reply_over_the_size_cap() {
    refused_as_invalid(|_, mut reply| {
        reply.resize(16385, 0);
        dns_message(reply)
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn doh_decodes_a_chunked_body() {
    let respond = |_: &[u8], reply: Vec<u8>| {
        http(
            "200 OK",
            &[
                "content-type: application/dns-message",
                "transfer-encoding: chunked",
            ],
            &chunked(&reply, 7),
        )
    };
    let (servers, wires) = doh_servers_answering(doh_fixture(respond, false), alias);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    // The trailer was read whole, so the first family's connection carried
    // its alias and the alias's target; the other family has its own.
    assert_eq!(servers.answered(), [2, 1]);
}

#[tokio::test(start_paused = true)]
async fn doh_fails_a_chunked_body_over_the_size_cap() {
    refused_as_invalid(|_, mut reply| {
        reply.resize(16385, 0);
        http(
            "200 OK",
            &[
                "content-type: application/dns-message",
                "transfer-encoding: chunked",
            ],
            &chunked(&reply, 4096),
        )
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn doh_fails_a_body_it_cannot_delimit() {
    // Both length headers, neither, and codings other than chunked alone.
    for headers in [
        vec!["transfer-encoding: chunked", "content-length: LENGTH"],
        vec![],
        vec!["transfer-encoding: gzip"],
        vec!["transfer-encoding: gzip, chunked"],
        vec!["content-length: LENGTH", "content-length: LENGTH"],
        vec!["content-length: +LENGTH"],
    ] {
        refused_as_invalid(move |_, reply| {
            let length = reply.len().to_string();
            let mut all = vec!["content-type: application/dns-message".to_owned()];
            all.extend(
                headers
                    .iter()
                    .map(|header| header.replace("LENGTH", &length)),
            );
            let all: Vec<&str> = all.iter().map(String::as_str).collect();
            http("200 OK", &all, &reply)
        })
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn doh_fails_a_head_over_its_cap_or_with_too_many_headers() {
    refused_as_invalid(|_, reply| {
        let padding = format!("x-padding: {}", "p".repeat(8200));
        http(
            "200 OK",
            &[
                &padding,
                "content-type: application/dns-message",
                &format!("content-length: {}", reply.len()),
            ],
            &reply,
        )
    })
    .await;
    refused_as_invalid(|_, reply| {
        let mut headers: Vec<String> = (0..31).map(|n| format!("x-{n}: y")).collect();
        headers.push("content-type: application/dns-message".into());
        headers.push(format!("content-length: {}", reply.len()));
        let headers: Vec<&str> = headers.iter().map(String::as_str).collect();
        http("200 OK", &headers, &reply)
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn doh_passes_over_interim_responses_a_few_times_only() {
    let (servers, wires) = doh_servers(|_, reply| {
        let mut response = http("100 Continue", &[], b"");
        response.extend(dns_message(reply));
        response
    });
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    refused_as_invalid(|_, reply| {
        let mut response = Vec::new();
        for _ in 0..5 {
            response.extend(http("100 Continue", &[], b""));
        }
        response.extend(dns_message(reply));
        response
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn doh_opens_a_new_connection_after_one_that_says_it_closes() {
    let (servers, wires) = doh_servers(|_, reply| {
        http(
            "200 OK",
            &[
                "content-type: application/dns-message",
                "Connection: Close",
                &format!("content-length: {}", reply.len()),
            ],
            &reply,
        )
    });
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.error, None);
    let log = servers.log.lock().unwrap();
    assert_eq!(log.dials.len(), 2);
    let answered: Vec<_> = log
        .queries
        .iter()
        .map(|queries| queries.load(Ordering::Relaxed))
        .collect();
    assert_eq!(answered, [1, 1]);
}

// The first family asks its alias, then the alias's target, on the one
// connection kept alive; the other family asks on a connection of its own.
#[tokio::test(start_paused = true)]
async fn doh_asks_two_queries_on_one_connection_kept_alive() {
    let (servers, wires) = doh_servers_answering(doh_fixture(plain_reply, false), alias);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(servers.answered(), [2, 1]);
}

#[tokio::test(start_paused = true)]
async fn doh_reads_a_response_that_arrives_a_byte_at_a_time() {
    for respond in [plain_reply as fn(&[u8], Vec<u8>) -> Vec<u8>, |_, reply| {
        http(
            "200 OK",
            &[
                "content-type: application/dns-message",
                "transfer-encoding: chunked",
            ],
            &chunked(&reply, 5),
        )
    }] {
        let (servers, wires) = doh_servers_answering(doh_fixture(respond, true), alias);
        let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
        assert_eq!(resolution.addresses, seven());
        // The response was read whole, so its connection was kept.
        assert_eq!(servers.answered(), [2, 1]);
    }
}

#[tokio::test(start_paused = true)]
async fn doh_replies_taken_as_sent_are_recorded_so() {
    let (servers, wires) = doh_servers(plain_reply);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(record::TrustOutcome::AsSent));
    assert_eq!(resolution.wire, Some(WireKind::Https));
    // The query asked for recursion and nothing else.
    let requests = servers.log.lock().unwrap().requests.clone();
    assert_eq!(requests[0].body[2..4], [0x01, 0x00]);
}

// Whether any connection carried the name asked, in the clear.
fn leaked(servers: &Servers) -> bool {
    servers
        .sent()
        .iter()
        .any(|sent| sent.windows(7).any(|bytes| bytes == b"fixture"))
}

#[tokio::test(start_paused = true)]
async fn a_plain_server_after_an_encrypted_one_is_never_asked() {
    let trusted = Authority::new();
    let stranger = Authority::new();
    let plain = DnsServer::plain("192.0.2.54".parse().unwrap());
    let servfail = |asked: IpAddr, qtype, query: &[u8]| match asked.to_string().as_str() {
        "192.0.2.55" => respond(query, 2, &[], &[]),
        _ => answers(asked, qtype, query),
    };
    let servers = Servers::new(Speaks::Dot { close_after: None }, servfail)
        // A certificate the client does not trust.
        .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"dot"))
        // A server that says SERVFAIL.
        .at("192.0.2.55", trusted.server(&["192.0.2.55"], b"dot"))
        // A server that takes the connection and never speaks.
        .silent_at("192.0.2.56")
        .plain_at("192.0.2.54");
    let wires = trusted.wires();
    for (encrypted, failure) in [
        ("192.0.2.53", LookupFailure::TlsFailed),
        ("192.0.2.55", LookupFailure::ServerFailure),
        ("192.0.2.56", LookupFailure::TimedOut),
    ] {
        let listed = [dot(encrypted, None), plain.clone()];
        let resolution = recorded(&servers, &wires, &listed, Trust::AsSent, "fixture.test").await;
        assert!(resolution.addresses.is_empty(), "{encrypted}");
        assert_eq!(
            resolution.error.map(|error| error.failure),
            Some(failure),
            "{encrypted}"
        );
    }
    // Nothing was dialled on port 53, and no query went out in the clear.
    assert!(
        servers.dials().iter().all(|dial| dial.port() == 853),
        "{:?}",
        servers.dials()
    );
    assert!(!leaked(&servers));

    // An encrypted server that cannot be reached at all: the record says
    // why the plain one was not asked.
    let resolution = recorded(
        &servers,
        &wires,
        &[dot("192.0.2.99", None), plain.clone()],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::Other,
            "192.0.2.54: not asked in plain after an encrypted server"
        ))
    );
    assert!(!servers.dials().iter().any(|dial| dial.port() == 53));
    assert!(!leaked(&servers));
}

#[tokio::test(start_paused = true)]
async fn a_plain_server_listed_before_an_encrypted_one_is_asked_first() {
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"))
        .plain_at("192.0.2.54");
    let resolution = recorded(
        &servers,
        &authority.wires(),
        &[
            DnsServer::plain("192.0.2.54".parse().unwrap()),
            dot("192.0.2.53", None),
        ],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.wire, Some(WireKind::Plain));
    assert_eq!(
        servers.dials(),
        [
            "192.0.2.54:53".parse::<SocketAddr>().unwrap(),
            "192.0.2.54:53".parse().unwrap()
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_tls_failure_outranks_a_server_that_timed_out() {
    let trusted = Authority::new();
    let stranger = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"dot"))
        .silent_at("192.0.2.54");
    let resolution = recorded(
        &servers,
        &trusted.wires(),
        &[dot("192.0.2.53", None), dot("192.0.2.54", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(resolution.error, tls_failed("192.0.2.53", "unknown issuer"));
}

#[tokio::test(start_paused = true)]
async fn a_tls_failure_says_a_fixed_reason_and_nothing_the_server_sent() {
    let authority = Authority::new();
    // Thousands of long names, none of them the server's address.
    let names: Vec<String> = (0..1200)
        .map(|n| format!("n{n:04}.a-long-subject-alternative-name.test"))
        .collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", authority.server(&names, b"dot"));
    let resolution = recorded(
        &servers,
        &authority.wires(),
        &[dot("192.0.2.53", None)],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(
        resolution.error,
        tls_failed("192.0.2.53", "certificate not valid for this server")
    );
}

// The extensions of the ClientHello that opens `sent`, by type.
fn hello_extensions(sent: &[u8]) -> Vec<(u16, Vec<u8>)> {
    assert_eq!((sent[0], sent[5]), (0x16, 1), "a ClientHello");
    let size = |at: usize, width: usize| {
        sent[at..at + width]
            .iter()
            .fold(0usize, |size, byte| size << 8 | *byte as usize)
    };
    // The record and handshake headers, the version and the random.
    let mut at = 5 + 4 + 2 + 32;
    at += 1 + size(at, 1);
    at += 2 + size(at, 2);
    at += 1 + size(at, 1);
    let end = at + 2 + size(at, 2);
    at += 2;
    let mut extensions = Vec::new();
    while at < end {
        let kind = size(at, 2) as u16;
        let length = size(at + 2, 2);
        extensions.push((kind, sent[at + 4..at + 4 + length].to_vec()));
        at += 4 + length;
    }
    extensions
}

// Whether a ClientHello offers to resume a session: a pre-shared key (TLS
// 1.3), or a ticket in the session ticket extension (TLS 1.2).
fn offers_a_session(sent: &[u8]) -> bool {
    hello_extensions(sent)
        .iter()
        .any(|(kind, data)| *kind == 41 || (*kind == 35 && !data.is_empty()))
}

#[tokio::test(start_paused = true)]
async fn tls_sessions_resume_within_a_naming_and_never_across_two() {
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let base = authority.wires();
    let (first, second) = (base.apart(), base.apart());
    let listed = [dot("192.0.2.53", None)];
    for wires in [&first, &first, &second] {
        resolve(&servers, wires, &listed, Trust::AsSent, "fixture.test")
            .await
            .unwrap();
    }
    let sent = servers.sent();
    assert_eq!(sent.len(), 3);
    assert!(!offers_a_session(&sent[0]));
    // The same naming asked again resumes its session.
    assert!(offers_a_session(&sent[1]));
    // Another naming offers nothing from the first.
    let other = hello_extensions(&sent[2]);
    assert!(!other.iter().any(|(kind, _)| *kind == 41), "{other:?}");
    assert!(
        other
            .iter()
            .all(|(kind, data)| *kind != 35 || data.is_empty())
    );
}

#[tokio::test(start_paused = true)]
async fn a_naming_records_a_failed_lookup_as_judged_by_nobody() {
    let trusted = Authority::new();
    let stranger = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"dot"))
        .silent_at("192.0.2.54");
    let roots = trusted.roots();
    for (address, failure) in [
        ("192.0.2.53", LookupFailure::TlsFailed),
        ("192.0.2.54", LookupFailure::TimedOut),
    ] {
        let naming = proxy_servers_trusting(vec![dot(address, None)], Trust::AsSent, &roots);
        let before = counted();
        let resolution = through_naming(&naming, &servers, "fixture.test").await;
        assert!(!resolution.cached);
        assert_eq!(resolution.trust, None, "{address}");
        assert_eq!(since(&before), [], "{address}");
        assert_eq!(
            resolution.error.map(|error| error.failure),
            Some(failure),
            "{address}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn doh_fails_a_framing_header_that_is_not_visible_ascii() {
    for (name, value) in [
        ("transfer-encoding", &b"chunked\xff"[..]),
        ("content-length", b"\x7f12"),
        ("connection", b"keep-alive\x01"),
        ("content-type", b"application/dns-message\xe9"),
    ] {
        refused_as_invalid(move |_, reply| {
            let mut response = b"HTTP/1.1 200 OK\r\n".to_vec();
            if name != "content-type" {
                response.extend_from_slice(b"content-type: application/dns-message\r\n");
            }
            response.extend_from_slice(format!("{name}: ").as_bytes());
            response.extend_from_slice(value);
            response.extend_from_slice(b"\r\n");
            if name != "content-length" {
                response
                    .extend_from_slice(format!("content-length: {}\r\n", reply.len()).as_bytes());
            }
            response.extend_from_slice(b"\r\n");
            response.extend(reply);
            response
        })
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn doh_fails_a_switching_protocols_response() {
    let (servers, wires) = doh_servers(|_, reply| {
        let mut response = http("101 Switching Protocols", &["upgrade: dns"], b"");
        response.extend(dns_message(reply));
        response
    });
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::ServerFailure,
            "192.0.2.53: HTTP 101"
        ))
    );
    assert!(resolution.addresses.is_empty());
}

#[tokio::test(start_paused = true)]
async fn doh_opens_a_new_connection_after_bytes_left_over_past_a_response() {
    let (servers, wires) = doh_servers(|_, reply| {
        let mut response = dns_message(reply);
        response.extend_from_slice(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");
        response
    });
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.error, None);
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(servers.answered(), [1, 1]);
}

#[tokio::test(start_paused = true)]
async fn doh_opens_a_new_connection_after_http_1_0_or_a_close_among_other_tokens() {
    for (version, connection) in [("HTTP/1.0", None), ("HTTP/1.1", Some("keep-alive, close"))] {
        let (servers, wires) = doh_servers(move |_, reply| {
            let mut response = format!("{version} 200 OK\r\n").into_bytes();
            let mut headers = vec![
                "content-type: application/dns-message".to_owned(),
                format!("content-length: {}", reply.len()),
            ];
            headers.extend(connection.map(|connection| format!("connection: {connection}")));
            for header in headers {
                response.extend_from_slice(header.as_bytes());
                response.extend_from_slice(b"\r\n");
            }
            response.extend_from_slice(b"\r\n");
            response.extend(reply);
            response
        });
        let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
        assert_eq!(resolution.addresses, seven(), "{version}");
        assert_eq!(servers.dials().len(), 2, "{version}");
        assert_eq!(servers.answered(), [1, 1], "{version}");
    }
}

fn h2_message(reply: Vec<u8>) -> hyper::Response<Vec<u8>> {
    hyper::Response::builder()
        .header("content-type", "application/dns-message")
        .body(reply)
        .unwrap()
}

// One server at 192.0.2.53 that speaks only HTTP/2.
fn h2_servers(
    respond: impl Fn(Vec<u8>) -> hyper::Response<Vec<u8>> + Send + Sync + 'static,
    goaway_after: Option<usize>,
    answer: impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> (Servers, Wires) {
    h2_servers_doing(respond, goaway_after, H2Misstep::None, answer)
}

fn h2_servers_doing(
    respond: impl Fn(Vec<u8>) -> hyper::Response<Vec<u8>> + Send + Sync + 'static,
    goaway_after: Option<usize>,
    misstep: H2Misstep,
    answer: impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> (Servers, Wires) {
    let authority = Authority::new();
    let speaks = Speaks::H2 {
        respond: Arc::new(respond),
        goaway_after,
        misstep,
    };
    let servers =
        Servers::new(speaks, answer).at("192.0.2.53", authority.server(&["192.0.2.53"], b"h2"));
    (servers, authority.wires())
}

#[tokio::test(start_paused = true)]
async fn doh_over_http_2_asks_both_families_and_an_alias_on_one_connection() {
    let (servers, wires) = h2_servers(h2_message, None, alias_and_six);
    let lookup = resolve(
        &servers,
        &wires,
        &[doh("192.0.2.53:443")],
        Trust::AsSent,
        "alias.test",
    )
    .await
    .unwrap();
    assert_eq!(lookup.addresses, seven_and_six());
    assert_eq!(servers.dials().len(), 1);
    assert_eq!(servers.answered(), [3]);
    let requests = servers.log.lock().unwrap().requests.clone();
    for seen in requests {
        assert_eq!(seen.method, "POST");
        assert_eq!(seen.target, "https://192.0.2.53/dns-query");
        assert!(seen.headers.iter().all(|(name, _)| name != "host"));
        assert_eq!(seen.body[..2], [0, 0]);
    }
}

#[tokio::test(start_paused = true)]
async fn doh_speaks_http_1_1_to_a_server_that_offers_only_it_or_no_alpn() {
    for alpn in [&b"http/1.1"[..], b""] {
        let authority = Authority::new();
        let servers = Servers::new(doh_fixture(plain_reply, false), answers)
            .at("192.0.2.53", authority.server(&["192.0.2.53"], alpn));
        let resolution = doh_lookup(&servers, &authority.wires(), Trust::AsSent).await;
        assert_eq!(resolution.addresses, seven(), "{alpn:?}");
        // A connection per family.
        assert_eq!(servers.answered(), [1, 1], "{alpn:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn doh_over_http_2_fails_another_status_another_type_or_an_over_long_body() {
    let (servers, wires) = h2_servers(
        |reply| {
            let mut response = h2_message(reply);
            *response.status_mut() = hyper::StatusCode::INTERNAL_SERVER_ERROR;
            response
        },
        None,
        answers,
    );
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::ServerFailure,
            "192.0.2.53: HTTP 500"
        ))
    );
    for respond in [
        (|reply| {
            hyper::Response::builder()
                .header("content-type", "text/html")
                .body(reply)
                .unwrap()
        }) as fn(Vec<u8>) -> hyper::Response<Vec<u8>>,
        |mut reply| {
            reply.resize(16385, 0);
            h2_message(reply)
        },
    ] {
        let (servers, wires) = h2_servers(respond, None, answers);
        let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
        assert_eq!(resolution.error, invalid(), "{resolution:?}");
        assert!(resolution.addresses.is_empty());
    }
}

// The server's first connection meets its first stream with a GOAWAY and
// answers nothing. Both families went out on it and both are cut off; each
// is asked again on a new connection, as a query that found its connection
// gone is, and the second connection answers both.
#[tokio::test(start_paused = true)]
async fn doh_over_http_2_completes_on_a_new_connection_after_a_goaway() {
    let (servers, wires) = h2_servers(h2_message, Some(1), seven_then_six);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven_and_six());
    assert_eq!(resolution.error, None);
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(servers.answered(), [0, 2]);
}

// The server's first connection keeps its first stream unanswered and sends
// the connection away gracefully, refusing the stream after it. That stream
// is asked again at once on a new connection, while the old one still
// carries the first: its family's reply comes with no time passed, and the
// lookup keeps it when the time limit ends the first.
#[tokio::test(start_paused = true)]
async fn doh_over_http_2_after_a_graceful_goaway_asks_again_on_a_new_connection_at_once() {
    let (servers, wires) =
        h2_servers_doing(h2_message, Some(1), H2Misstep::Graceful, seven_then_six);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses.len(), 1);
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(servers.answered(), [0, 1]);

    let (servers, wires) =
        h2_servers_doing(h2_message, Some(1), H2Misstep::Graceful, seven_then_six);
    let listed = doh("192.0.2.53:443");
    let mut session = Session::new(&servers, &wires, &listed);
    let mut reached = false;
    let mut talk = session.talk(&mut reached);
    talk.send(0, 0, query_packet(0, "fixture.test", 1));
    talk.send(1, 1, query_packet(0, "fixture.test", 28));
    let started = tokio::time::Instant::now();
    let next = talk.next().await;
    assert!(matches!(next, Ok(Some((_, Ok(_))))));
    assert_eq!(started.elapsed(), std::time::Duration::ZERO);
    assert_eq!(servers.dials().len(), 2);
}

// The server's first connection keeps its first stream and sends the
// connection away gracefully, refusing the stream after it, and answers the
// first once the connection that replaces it is dialled. That dial takes a
// second, or never ends: the reply on the old connection comes back with no
// time passed.
#[tokio::test(start_paused = true)]
async fn doh_over_http_2_reads_the_old_connection_while_the_new_one_dials() {
    for wait in [Some(std::time::Duration::from_secs(1)), None] {
        let (servers, wires) = h2_servers_doing(
            h2_message,
            Some(1),
            H2Misstep::GracefulAnswers,
            seven_then_six,
        );
        let servers = servers.stalled_at(1, wait);
        let listed = doh("192.0.2.53:443");
        let mut session = Session::new(&servers, &wires, &listed);
        let mut reached = false;
        let mut talk = session.talk(&mut reached);
        talk.send(0, 0, query_packet(0, "fixture.test", 1));
        talk.send(1, 1, query_packet(0, "fixture.test", 28));
        let started = tokio::time::Instant::now();
        let next = talk.next().await;
        assert!(matches!(next, Ok(Some((0, Ok(_))))), "{wait:?}");
        assert_eq!(started.elapsed(), std::time::Duration::ZERO, "{wait:?}");
        assert_eq!(servers.dials().len(), 2, "{wait:?}");
    }
}

// The server's first connection, over HTTP/2, keeps its first stream and
// sends the connection away gracefully, refusing the stream after it; the
// connection that replaces it speaks only HTTP/1.1. The stream still out on
// the old connection is asked again over HTTP/1.1, as one that found its
// connection gone is, and both families come back.
#[tokio::test(start_paused = true)]
async fn doh_asks_again_over_http_1_1_a_stream_left_on_a_spent_http_2_connection() {
    let authority = Authority::new();
    let speaks = Speaks::H2 {
        respond: Arc::new(h2_message),
        goaway_after: Some(1),
        misstep: H2Misstep::Graceful,
    };
    let servers = Servers::new(speaks, seven_then_six)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"h2"))
        .switched_at(
            1,
            doh_fixture(plain_reply, false),
            authority.server(&["192.0.2.53"], b"http/1.1"),
        );
    let wires = authority.wires();
    let listed = doh("192.0.2.53:443");
    let mut session = Session::new(&servers, &wires, &listed);
    let mut reached = false;
    let mut talk = session.talk(&mut reached);
    talk.send(0, 0, query_packet(0, "fixture.test", 1));
    talk.send(1, 1, query_packet(0, "fixture.test", 28));
    let mut keys = Vec::new();
    while let Ok(Some((key, reply))) = talk.next().await {
        assert!(reply.is_ok(), "{key}");
        keys.push(key);
    }
    keys.sort_unstable();
    assert_eq!(keys, [0, 1]);
    // Nothing answered over HTTP/2; a connection per family over HTTP/1.1.
    assert_eq!(servers.answered(), [0, 1, 1]);
}

// Over HTTP/2 grown wary, where each lane takes turns, a silent query asked
// ahead that is out holds back nothing its lane then waits on: that query
// goes out beside it and comes back with no time passed.
#[tokio::test(start_paused = true)]
async fn doh_over_http_2_grown_wary_sends_a_query_waited_on_beside_one_asked_ahead() {
    let answer = |asked: IpAddr, qtype: u16, query: &[u8]| {
        if query.windows(6).any(|name| name == b"silent") {
            return Vec::new();
        }
        seven_then_six(asked, qtype, query)
    };
    let (servers, wires) = h2_servers_doing(h2_message, None, H2Misstep::RefuseSecond, answer);
    let listed = doh("192.0.2.53:443");
    let mut session = Session::new(&servers, &wires, &listed);
    let mut reached = false;
    let mut talk = session.talk(&mut reached);
    talk.send(0, 0, query_packet(0, "fixture.test", 1));
    talk.send(1, 1, query_packet(0, "fixture.test", 28));
    for _ in 0..2 {
        assert!(matches!(talk.next().await, Ok(Some((_, Ok(_))))));
    }
    // The second stream was refused: the talk is wary.
    assert_eq!(servers.log.lock().unwrap().refused, 1);
    talk.ahead(2, 0, query_packet(0, "silent.ahead.test", 1));
    let holding = servers.holding.clone();
    tokio::select! {
        _ = talk.next() => panic!("the silent query was answered"),
        held = holding.acquire() => held.unwrap().forget(),
    }
    let started = tokio::time::Instant::now();
    talk.send(3, 0, query_packet(0, "fixture.test", 1));
    let next = talk.next().await;
    assert!(matches!(next, Ok(Some((3, Ok(_))))));
    assert_eq!(started.elapsed(), std::time::Duration::ZERO);
}

// The server refuses the second stream on its connection unhandled, as one
// over its limit of streams at once does. A refused stream was never
// handled, so it is sent again on the same connection, and both families
// come back from one dial, whichever family's stream the server took
// second.
#[tokio::test(start_paused = true)]
async fn doh_over_http_2_sends_a_refused_stream_again_on_the_connection() {
    let (servers, wires) =
        h2_servers_doing(h2_message, None, H2Misstep::RefuseSecond, seven_then_six);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven_and_six());
    assert_eq!(resolution.error, None);
    assert_eq!(servers.dials().len(), 1);
    assert_eq!(servers.answered(), [2]);
}

#[tokio::test(start_paused = true)]
async fn doh_over_http_2_replies_taken_as_sent_are_recorded_so() {
    let (servers, wires) = h2_servers(h2_message, None, answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(record::TrustOutcome::AsSent));
    assert_eq!(resolution.wire, Some(WireKind::Https));
}

// The query type of a DNS message: the two bytes after its question name.
fn qtype_of(message: &[u8]) -> u16 {
    let mut at = 12;
    while message[at] != 0 {
        at += 1 + message[at] as usize;
    }
    u16_at(message, at + 1).unwrap()
}

// A response whose header list is over the cap the client set, for the
// families `over` says.
fn padded(over: fn(u16) -> bool) -> impl Fn(Vec<u8>) -> hyper::Response<Vec<u8>> {
    move |reply| {
        let qtype = qtype_of(&reply);
        let mut response = h2_message(reply);
        if over(qtype) {
            let padding = "a".repeat(9000).parse().unwrap();
            response.headers_mut().insert("x-padding", padding);
        }
        response
    }
}

#[tokio::test(start_paused = true)]
async fn doh_over_http_2_fails_a_header_list_over_the_cap_as_invalid_and_dials_no_more() {
    // Only the IPv6 reply is over the cap: it rides the connection beside
    // the IPv4 query, and is not asked again on another.
    let (servers, wires) = h2_servers(padded(|qtype| qtype == 28), None, answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(servers.dials().len(), 1);
    assert_eq!(servers.answered(), [2]);

    let (servers, wires) = h2_servers(padded(|_| true), None, answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.error, invalid(), "{resolution:?}");
    // Both families on one connection, neither redialled.
    assert_eq!(servers.dials().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn doh_over_http_2_a_stream_the_server_resets_fails_without_saying_the_connection_closed() {
    // The IPv6 stream is reset on the connection the IPv4 query used, and
    // is not asked again on another.
    let (servers, wires) =
        h2_servers_doing(h2_message, None, H2Misstep::Reset { after: 1 }, answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(servers.dials().len(), 1);

    let (servers, wires) =
        h2_servers_doing(h2_message, None, H2Misstep::Reset { after: 0 }, answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::NoReply,
            "192.0.2.53: the server reset the query"
        ))
    );
    // Both families' streams were reset on the one connection.
    assert_eq!(servers.dials().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn doh_over_http_2_takes_a_few_interim_responses_and_fails_an_endless_run_of_them() {
    let (servers, wires) = h2_servers_doing(h2_message, None, H2Misstep::Interims(3), answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.error, None);

    // Far more than one exchange may read, each a few bytes.
    let (servers, wires) = h2_servers_doing(h2_message, None, H2Misstep::Interims(20_000), answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.error, invalid(), "{resolution:?}");
    // Both families on one connection, neither redialled.
    assert_eq!(servers.dials().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn doh_over_http_2_accepts_no_server_push() {
    let (servers, wires) = h2_servers_doing(h2_message, None, H2Misstep::Push, answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    // The client's settings forbid a push, so the server could not send one.
    let pushes = servers.log.lock().unwrap().pushes.clone();
    assert_eq!(pushes.len(), 2);
    for pushed in pushes {
        let refused = pushed.expect_err("a push the client forbade");
        assert!(refused.contains("disabled server push"), "{refused}");
    }
}

#[tokio::test(start_paused = true)]
async fn doh_over_http_1_1_a_kept_connection_found_dead_is_replaced_once_and_no_more() {
    // The first query is answered and the connection kept; every query
    // after it is read and the connection closed without a reply.
    let (servers, wires) = doh_servers(plain_reply);
    let servers = servers.replying(1);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    // The IPv6 query found the kept connection dead, opened one more,
    // found that closed too, and stopped there.
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(servers.answered(), [1, 0]);
}

#[tokio::test(start_paused = true)]
async fn a_plain_server_after_an_https_one_is_never_asked() {
    let stranger = Authority::new();
    let servers = Servers::new(doh_fixture(plain_reply, false), answers)
        // A certificate the client does not trust.
        .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"http/1.1"))
        .plain_at("192.0.2.54");
    let listed = [
        doh("192.0.2.53:443"),
        DnsServer::plain("192.0.2.54".parse().unwrap()),
    ];
    let resolution = recorded(
        &servers,
        &Authority::new().wires(),
        &listed,
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error.map(|error| error.failure),
        Some(LookupFailure::TlsFailed)
    );
    assert!(
        servers.dials().iter().all(|dial| dial.port() == 443),
        "{:?}",
        servers.dials()
    );
    assert!(!leaked(&servers));
}

// Through the naming's own path: a naming and its clones share one store of
// TLS sessions, and two namings keep two.
#[tokio::test(start_paused = true)]
async fn tls_sessions_resume_through_a_naming_and_its_clones_and_never_across_two() {
    for (speaks, alpn, listed) in [
        (
            Speaks::Dot { close_after: None },
            &b"dot"[..],
            dot("192.0.2.53", None),
        ),
        (
            doh_fixture(plain_reply, false),
            &b"http/1.1"[..],
            doh("192.0.2.53:443"),
        ),
        (
            Speaks::H2 {
                respond: Arc::new(h2_message),
                goaway_after: None,
                misstep: H2Misstep::None,
            },
            &b"h2"[..],
            doh("192.0.2.53:443"),
        ),
    ] {
        let authority = Authority::new();
        let servers =
            Servers::new(speaks, answers).at("192.0.2.53", authority.server(&["192.0.2.53"], alpn));
        let roots = authority.roots();
        let first = Naming::proxy_servers(vec![listed.clone()], Trust::AsSent);
        let clone = first.clone();
        let second = Naming::proxy_servers(vec![listed.clone()], Trust::AsSent);
        assert!(Arc::ptr_eq(sessions(&first), sessions(&clone)));
        assert!(!Arc::ptr_eq(sessions(&first), sessions(&second)));
        // The same, trusting the test authority: the clone's store is the
        // first's, and the second naming's is its own.
        let first = proxy_servers_trusting(vec![listed.clone()], Trust::AsSent, &roots);
        let clone = first.clone();
        let second = proxy_servers_trusting(vec![listed.clone()], Trust::AsSent, &roots);
        assert!(Arc::ptr_eq(sessions(&first), sessions(&clone)));
        assert!(!Arc::ptr_eq(sessions(&first), sessions(&second)));
        // A name apiece, so none is answered from what the naming holds.
        // Each lookup's first connection is the one held to account: a
        // second one in the same lookup, as HTTP/1.1 opens for the other
        // family, may resume the first's session. Each lookup's connections
        // are closed, the one kept idle past `IDLE`, before the next, so
        // every lookup dials.
        let per_lookup: u32 = if alpn == b"http/1.1" { 2 } else { 1 };
        let mut firsts = Vec::new();
        for (naming, host) in [(&first, "a.test"), (&clone, "b.test"), (&second, "c.test")] {
            firsts.push(servers.dials().len());
            let resolution = through_naming(naming, &servers, host).await;
            assert_eq!(resolution.addresses, seven(), "{listed} {host}");
            servers
                .ended
                .acquire_many(per_lookup)
                .await
                .unwrap()
                .forget();
        }
        let sent = servers.sent();
        assert_eq!(sent.len(), 3 * per_lookup as usize, "{listed}");
        assert!(!offers_a_session(&sent[firsts[0]]), "{listed}");
        // The clone resumes the first naming's session.
        assert!(offers_a_session(&sent[firsts[1]]), "{listed}");
        // The other naming offers nothing from the first.
        assert!(!offers_a_session(&sent[firsts[2]]), "{listed}");
    }
}

// Under a GOAWAY the client grows wary of HTTP/2 and has one stream out per
// family. The first connection meets its first stream with a GOAWAY; on the
// second, one family's stream stays silent, and the other family still
// answers, at its own turn, when the server's time limit ends the silent
// one. Each family silent in turn.
#[tokio::test(start_paused = true)]
async fn doh_over_http_2_grown_wary_keeps_a_family_apart_from_the_others_silent_stream() {
    for (silent, kept) in [(1, vec!["2001:db8::7".parse().unwrap()]), (28, seven())] {
        let (servers, wires) = h2_servers(h2_message, Some(1), move |asked, qtype, query| {
            if qtype == silent {
                return Vec::new();
            }
            seven_then_six(asked, qtype, query)
        });
        let started = tokio::time::Instant::now();
        let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
        assert_eq!(resolution.addresses, kept, "{silent}");
        assert_eq!(servers.dials().len(), 2, "{silent}");
        assert_eq!(started.elapsed(), SERVER_TIMEOUT, "{silent}");
    }
}

// A server that refuses every stream unhandled: each family's query goes
// out three times on the one connection and ends there.
#[tokio::test(start_paused = true)]
async fn doh_over_http_2_a_query_whose_stream_is_refused_every_time_goes_three_times_in_all() {
    let (servers, wires) =
        h2_servers_doing(h2_message, None, H2Misstep::RefuseEvery, seven_then_six);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::NoReply,
            "192.0.2.53: the server reset the query"
        ))
    );
    assert_eq!(servers.dials().len(), 1);
    assert_eq!(servers.log.lock().unwrap().refused, 6);
}

// A server that meets the first stream of every connection with a GOAWAY:
// each family's query goes out on three connections and ends there.
#[tokio::test(start_paused = true)]
async fn doh_over_http_2_a_query_whose_connection_goes_away_every_time_goes_three_times_in_all() {
    let (servers, wires) = h2_servers(h2_message, Some(usize::MAX), seven_then_six);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::NoReply,
            "192.0.2.53: the connection closed without a reply"
        ))
    );
    assert_eq!(servers.dials().len(), 3);
    assert_eq!(servers.answered(), [0, 0, 0]);
}

// A lookup dropped while its HTTP/2 streams are still out ends without a
// panic, and its connection closes.
#[tokio::test(start_paused = true)]
async fn doh_over_http_2_a_lookup_dropped_with_streams_out_closes_its_connection() {
    let (servers, wires) = h2_servers(h2_message, None, |_, _, _| Vec::new());
    let (holding, ended) = (servers.holding.clone(), servers.ended.clone());
    {
        let lookup = doh_lookup(&servers, &wires, Trust::AsSent);
        tokio::pin!(lookup);
        tokio::select! {
            resolution = &mut lookup => panic!("a silent server answered: {resolution:?}"),
            held = holding.acquire_many(2) => held.unwrap().forget(),
        }
    }
    ended.acquire().await.unwrap().forget();
    assert_eq!(servers.dials().len(), 1);
}

// A query for `name` of `qtype`, under `id`.
fn query_packet(id: u16, name: &str, qtype: u16) -> Vec<u8> {
    let mut packet = id.to_be_bytes().to_vec();
    packet.extend_from_slice(&[1, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
    packet.extend(wire(name));
    packet.extend_from_slice(&qtype.to_be_bytes());
    packet.extend_from_slice(&[0, 1]);
    packet
}

// A plain server has at most eight streams dialled or open at once for a
// talk: twelve queries given together go out on twelve streams, the first
// alone, then the rest eight at a time, the first among them while it waits
// on its reply.
#[tokio::test(start_paused = true)]
async fn plain_dns_opens_at_most_eight_streams_at_once() {
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .plain_at("192.0.2.53")
        .delayed(std::time::Duration::from_millis(300));
    let wires = Authority::new().wires();
    let listed = DnsServer::plain("192.0.2.53".parse().unwrap());
    let mut session = Session::new(&servers, &wires, &listed);
    let mut reached = false;
    let mut talk = session.talk(&mut reached);
    for key in 0..12 {
        talk.send(key, key, query_packet(key as u16, "fixture.test", 1));
    }
    let mut keys = Vec::new();
    while let Ok(Some((key, reply))) = talk.next().await {
        assert!(reply.is_ok(), "{key}");
        keys.push(key);
    }
    keys.sort_unstable();
    assert_eq!(keys, (0..12).collect::<Vec<_>>());
    assert_eq!(servers.dials().len(), 12);
    assert_eq!(servers.most_open(), 8);
}

// A plain dial that reaches nothing after the server answered an earlier
// one fails only its own query: the IPv4 address stands.
#[tokio::test(start_paused = true)]
async fn plain_dns_a_dial_that_reaches_nothing_after_one_answered_fails_only_its_query() {
    let servers = Servers::new(Speaks::Dot { close_after: None }, seven_then_six)
        .plain_at("192.0.2.53")
        .unreached_at(1);
    let wires = Authority::new().wires();
    let resolution = recorded(
        &servers,
        &wires,
        &[DnsServer::plain("192.0.2.53".parse().unwrap())],
        Trust::AsSent,
        "fixture.test",
    )
    .await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(servers.answered(), [1, 0]);
}

// A plain stream the server resets before its reply says so, in the words
// the other wires use; one the server closes before its reply says it
// closed without one. Either way the server was reached, and only the
// family's own query failed.
#[tokio::test(start_paused = true)]
async fn plain_dns_tells_a_reset_stream_from_one_the_server_closed() {
    let silent = |_: IpAddr, _: u16, _: &[u8]| Vec::new();
    for (servers, said) in [
        (
            Servers::new(Speaks::Dot { close_after: None }, seven_then_six)
                .broken(std::io::ErrorKind::ConnectionReset),
            "192.0.2.53: connection reset by the server",
        ),
        (
            Servers::new(Speaks::Dot { close_after: None }, silent).closing_unanswered(),
            "192.0.2.53: the connection closed without a reply",
        ),
    ] {
        let servers = servers.plain_at("192.0.2.53");
        let wires = Authority::new().wires();
        let resolution = recorded(
            &servers,
            &wires,
            &[DnsServer::plain("192.0.2.53".parse().unwrap())],
            Trust::AsSent,
            "fixture.test",
        )
        .await;
        assert!(resolution.addresses.is_empty(), "{said}");
        assert_eq!(
            resolution.error,
            Some(failure(LookupFailure::NoReply, said)),
            "{said}"
        );
        // Each family dialled its own stream: the first dial reached the
        // server, so the second family was asked too.
        assert_eq!(servers.dials().len(), 2, "{said}");
    }
}

// A DNS over TLS server answers the first query on each connection it is
// given and closes the connection on a query it never answers. That query
// goes out on each connection after one was answered there, and the third
// time the connection goes with it, it fails as closed: the queries given
// after it are answered on a connection of their own.
#[tokio::test(start_paused = true)]
async fn dot_sends_a_query_its_connection_goes_with_three_times_then_fails_it() {
    let answer = |asked: IpAddr, qtype: u16, query: &[u8]| {
        if query.windows(5).any(|name| name == b"stuck") {
            return Vec::new();
        }
        seven_then_six(asked, qtype, query)
    };
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answer)
        .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"))
        .closing_unanswered();
    let wires = authority.wires();
    let listed = dot("192.0.2.53", None);
    let mut session = Session::new(&servers, &wires, &listed);
    let mut reached = false;
    let mut talk = session.talk(&mut reached);
    // The stuck query's id is the highest, so a query given after it that
    // went out with it on a connection that closed goes again before it.
    talk.send(0, 0, query_packet(1, "fixture.test", 1));
    talk.send(1, 1, query_packet(9, "stuck.test", 1));
    assert!(matches!(talk.next().await, Ok(Some((0, Ok(_))))));
    for key in 2..4 {
        talk.send(key, key, query_packet(key as u16, "fixture.test", 1));
        assert!(matches!(talk.next().await, Ok(Some((got, Ok(_)))) if got == key));
    }
    talk.send(4, 4, query_packet(4, "fixture.test", 1));
    assert!(matches!(
        talk.next().await,
        Ok(Some((1, Err(Exchanged::Closed))))
    ));
    assert!(matches!(talk.next().await, Ok(Some((4, Ok(_))))));
    assert!(matches!(talk.next().await, Ok(None)));
    // The stuck query was read on each of the first three connections, after
    // the one query each answered.
    assert_eq!(servers.answered(), [2, 2, 2, 1]);
}

// A DNS over TLS server that closes after every reply leaves the talk wary,
// one query out at a time. A query asked ahead of its need then goes only
// once a lane waits on it, so `next` ends with it still given; a query a
// lane waits on, given after it, comes back with no time passed, and the
// query asked ahead, which the server does not answer, was not written.
#[tokio::test(start_paused = true)]
async fn dot_grown_wary_sends_a_query_waited_on_and_not_one_asked_ahead() {
    // A permit each time the server reads the query asked ahead.
    let ahead = Arc::new(tokio::sync::Semaphore::new(0));
    let seen = ahead.clone();
    let answer = move |asked: IpAddr, qtype: u16, query: &[u8]| {
        if query.windows(5).any(|name| name == b"ahead") {
            seen.add_permits(1);
            return Vec::new();
        }
        seven_then_six(asked, qtype, query)
    };
    let authority = Authority::new();
    let servers = Servers::new(
        Speaks::Dot {
            close_after: Some(1),
        },
        answer,
    )
    .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let wires = authority.wires();
    let listed = dot("192.0.2.53", None);
    let mut session = Session::new(&servers, &wires, &listed);
    let mut reached = false;
    let mut talk = session.talk(&mut reached);
    // Two queries out on a connection that closes after one reply: the talk
    // grows wary.
    talk.send(0, 0, query_packet(1, "fixture.test", 1));
    talk.send(1, 1, query_packet(2, "fixture.test", 28));
    let mut keys = Vec::new();
    while let Ok(Some((key, reply))) = talk.next().await {
        assert!(reply.is_ok(), "{key}");
        keys.push(key);
    }
    keys.sort_unstable();
    assert_eq!(servers.answered(), [1, 1]);
    talk.ahead(2, 0, query_packet(3, "silent.ahead.test", 1));
    tokio::select! {
        ended = talk.next() => assert!(matches!(ended, Ok(None))),
        _ = ahead.acquire() => panic!("the query asked ahead was written"),
    }
    let started = tokio::time::Instant::now();
    talk.send(3, 0, query_packet(4, "fixture.test", 1));
    let next = talk.next().await;
    assert!(matches!(next, Ok(Some((3, Ok(_))))));
    assert_eq!(started.elapsed(), std::time::Duration::ZERO);
    assert_eq!(ahead.available_permits(), 0);
}

// A DNS over TLS server that keeps its connection open has every query given
// together written at once on one connection: three queries take no longer,
// on a route with a round trip of its own, than one.
#[tokio::test(start_paused = true)]
async fn dot_writes_queries_given_together_at_once_on_one_connection() {
    let mut took = Vec::new();
    for count in [1, 3] {
        let authority = Authority::new();
        let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
            .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"))
            .delayed(std::time::Duration::from_millis(300));
        let wires = authority.wires();
        let listed = dot("192.0.2.53", None);
        let mut session = Session::new(&servers, &wires, &listed);
        let mut reached = false;
        let mut talk = session.talk(&mut reached);
        for key in 0..count {
            talk.send(key, key, query_packet(key as u16, "fixture.test", 1));
        }
        let started = tokio::time::Instant::now();
        let mut keys = Vec::new();
        while let Ok(Some((key, reply))) = talk.next().await {
            assert!(reply.is_ok(), "{count} {key}");
            keys.push(key);
        }
        keys.sort_unstable();
        assert_eq!(keys, (0..count).collect::<Vec<_>>());
        assert_eq!(servers.answered(), [count], "{count}");
        took.push(started.elapsed());
    }
    assert_eq!(took[0], took[1]);
}

// Over HTTP/1.1, where each lane has one query out at a time, a query a lane
// waits on goes before one asked ahead of its need, whatever the order they
// were given in. Lane 1 waits on a silent query; lane 0 asked a silent query
// ahead, then waits, as lane 1 does too, on a query the server answers: that
// one takes lane 0 and comes back with no time passed.
#[tokio::test(start_paused = true)]
async fn doh_over_http_1_1_a_query_waited_on_goes_before_one_asked_ahead() {
    let answer = |asked: IpAddr, qtype: u16, query: &[u8]| {
        if query.windows(6).any(|name| name == b"silent") {
            return Vec::new();
        }
        seven_then_six(asked, qtype, query)
    };
    let (servers, wires) = doh_servers_answering(doh_fixture(plain_reply, false), answer);
    let listed = doh("192.0.2.53:443");
    let mut session = Session::new(&servers, &wires, &listed);
    let mut reached = false;
    let mut talk = session.talk(&mut reached);
    talk.send(0, 1, query_packet(0, "silent.test", 1));
    talk.ahead(1, 0, query_packet(0, "silent.ahead.test", 1));
    talk.send(2, 0, query_packet(0, "fixture.test", 1));
    talk.wanted_by(2, 1, true);
    let started = tokio::time::Instant::now();
    let next = talk.next().await;
    assert!(matches!(next, Ok(Some((2, Ok(_))))));
    assert_eq!(started.elapsed(), std::time::Duration::ZERO);
    // The query asked ahead is still waiting: only the two lanes' queries
    // went out.
    assert_eq!(servers.log.lock().unwrap().requests.len(), 2);
    // A turn passes with lane 0 free. The query asked ahead still does not
    // go: out, a silent one would hold back lane 0's next query, which
    // comes back with no time passed.
    let turn =
        std::future::poll_fn(|cx| Poll::Ready(std::pin::pin!(talk.next()).poll(cx).is_pending()))
            .await;
    assert!(turn);
    talk.send(3, 0, query_packet(0, "fixture.test", 28));
    let next = talk.next().await;
    assert!(matches!(next, Ok(Some((3, Ok(_))))));
    assert_eq!(started.elapsed(), std::time::Duration::ZERO);
    let requests = servers.log.lock().unwrap().requests.clone();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|seen| !seen.body.windows(5).any(|name| name == b"ahead"))
    );
}

// Over HTTP/1.1 the second family's connection reaches nothing: its query
// waits for the first family's connection to come free and is answered on
// it, so both families come back.
#[tokio::test(start_paused = true)]
async fn doh_over_http_1_1_a_query_whose_connection_reaches_nothing_waits_for_the_first() {
    let (servers, wires) = doh_servers_answering(doh_fixture(plain_reply, false), seven_then_six);
    let servers = servers.unreached_at(1);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven_and_six());
    assert_eq!(resolution.error, None);
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(servers.answered(), [2, 0]);
}

// DNS over TLS takes a reply only for the question its id went out with,
// whatever the case of the name: one under the right id that asks another
// name fails that family's query alone.
#[tokio::test(start_paused = true)]
async fn dot_takes_a_reply_for_its_own_question_in_any_case_and_fails_another() {
    for (changed, kept) in [(false, seven_and_six()), (true, seven())] {
        let authority = Authority::new();
        let answer = move |asked: IpAddr, qtype: u16, query: &[u8]| {
            let mut reply = seven_then_six(asked, qtype, query);
            // The name's first label, "fixture", in capitals.
            reply[13..20].make_ascii_uppercase();
            if changed && qtype == 28 {
                // "gixture": another name.
                reply[13] = b'G';
            }
            reply
        };
        let servers = Servers::new(Speaks::Dot { close_after: None }, answer)
            .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
        let resolution = recorded(
            &servers,
            &authority.wires(),
            &[dot("192.0.2.53", None)],
            Trust::AsSent,
            "fixture.test",
        )
        .await;
        assert_eq!(resolution.addresses, kept, "{changed}");
        assert_eq!(servers.dials().len(), 1, "{changed}");
    }
}
