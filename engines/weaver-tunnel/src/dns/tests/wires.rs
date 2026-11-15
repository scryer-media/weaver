// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::trust::{counted, sessions, since, through_naming};
use super::ttl::{ASKED, Answer, failure, respond, rr, soa, v4, wire};
use super::*;
use crate::pipe::Naming;
use crate::resolution::record::{LookupError, Recording};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
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

    // Wires that trust only this authority.
    fn wires(&self) -> Wires {
        let mut roots = RootCertStore::empty();
        roots.add(self.certificate.clone()).unwrap();
        Wires::trusting(roots)
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
    // `goaway_after`, the request after that many is met with a GOAWAY and
    // the connection closed. `misstep` is what each stream does first.
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
        }
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
}

#[async_trait::async_trait]
impl Carrier for Servers {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        let (client, inner) = tokio::io::duplex(65536);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let queries = Arc::new(AtomicUsize::new(0));
        {
            let mut log = self.log.lock().unwrap();
            log.dials.push(server.address);
            log.sent.push(seen.clone());
            log.queries.push(queries.clone());
        }
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
        let asked = server.address.ip();
        let answer = self.answer.clone();
        let replies = self.replies.clone();
        if self.silent.contains(&asked) {
            self.held.lock().unwrap().push(inner);
            return Ok(client);
        }
        if self.plain.contains(&asked) {
            tokio::spawn(async move {
                let mut stream = Tap { inner, seen };
                framed_replies(&mut stream, asked, &*answer, None, None, &queries).await;
            });
            return Ok(client);
        }
        let Some((_, config)) = self.tls.iter().find(|(ip, _)| *ip == asked) else {
            return Err(TunnelError::Engine(format!("nothing listens at {asked}")));
        };
        let acceptor = TlsAcceptor::from(config.clone());
        let speaks = self.speaks.clone();
        let log = self.log.clone();
        tokio::spawn(async move {
            let Ok(mut tls) = acceptor.accept(Tap { inner, seen }).await else {
                return;
            };
            match speaks {
                Speaks::Dot { close_after } => {
                    framed_replies(
                        &mut tls,
                        asked,
                        &*answer,
                        close_after,
                        replies.as_deref(),
                        &queries,
                    )
                    .await
                }
                Speaks::Doh { respond, dribble } => loop {
                    let Some(seen) = read_request(&mut tls).await else {
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
                        goaway_after,
                        queries,
                        log,
                    )
                    .await
                }
            }
        });
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
    goaway_after: Option<usize>,
    queries: Arc<AtomicUsize>,
    log: Arc<Mutex<Log>>,
) {
    let Ok(mut connection) = h2::server::handshake(stream).await else {
        return;
    };
    let mut accepted = 0;
    while let Some(Ok((request, mut send))) = connection.accept().await {
        accepted += 1;
        if goaway_after.is_some_and(|after| accepted > after) {
            connection.abrupt_shutdown(h2::Reason::NO_ERROR);
            continue;
        }
        let (answer, respond, queries, log) = (
            answer.clone(),
            respond.clone(),
            queries.clone(),
            log.clone(),
        );
        tokio::spawn(async move {
            let (parts, mut body) = request.into_parts();
            let mut query = Vec::new();
            while let Some(Ok(chunk)) = body.data().await {
                let _ = body.flow_control().release_capacity(chunk.len());
                query.extend_from_slice(&chunk);
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
                H2Misstep::None => {}
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
            let (head, bytes) = respond(reply).into_parts();
            let Ok(mut stream) = send.send_response(hyper::Response::from_parts(head, ()), false)
            else {
                return;
            };
            let _ = stream.send_data(bytes.into(), true);
        });
    }
}

// DNS over TCP replies to each query read, until the client goes, the
// connection has answered `close_after`, or no `replies` are left.
async fn framed_replies<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    asked: IpAddr,
    answer: &Answer,
    close_after: Option<usize>,
    replies: Option<&AtomicUsize>,
    queries: &AtomicUsize,
) {
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
        let mut framed = (reply.len() as u16).to_be_bytes().to_vec();
        framed.extend(reply);
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
    // A new connection that closes is not retried: one for each family.
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::NoReply,
            "192.0.2.53: the connection closed without a reply"
        ))
    );
    assert_eq!(servers.dials().len(), 2);
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
    assert_eq!(servers.dials().len(), 2);
}

fn doh_servers(
    respond: impl Fn(&[u8], Vec<u8>) -> Vec<u8> + Send + Sync + 'static,
) -> (Servers, Wires) {
    doh_servers_speaking(doh_fixture(respond, false))
}

fn doh_servers_speaking(speaks: Speaks) -> (Servers, Wires) {
    let authority = Authority::new();
    let servers = Servers::new(speaks, answers)
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
    let (servers, wires) = doh_servers(|_, reply| {
        http(
            "200 OK",
            &[
                "content-type: application/dns-message",
                "transfer-encoding: chunked",
            ],
            &chunked(&reply, 7),
        )
    });
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    // The trailer was read whole, so the connection carried both families.
    assert_eq!(servers.dials().len(), 1);
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

#[tokio::test(start_paused = true)]
async fn doh_asks_two_queries_on_one_connection_kept_alive() {
    let (servers, wires) = doh_servers(plain_reply);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    let log = servers.log.lock().unwrap();
    assert_eq!(log.dials.len(), 1);
    assert_eq!(log.queries[0].load(Ordering::Relaxed), 2);
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
        let (servers, wires) = doh_servers_speaking(doh_fixture(respond, true));
        let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
        assert_eq!(resolution.addresses, seven());
        assert_eq!(servers.dials().len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn doh_replies_are_held_to_the_ad_bit() {
    let (servers, wires) = doh_servers(plain_reply);
    let resolution = doh_lookup(&servers, &wires, Trust::RequireAd).await;
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::NotAuthenticated,
            "192.0.2.53: the answer was not marked authenticated"
        ))
    );
    assert_eq!(resolution.trust, Some(record::TrustOutcome::Refused));
    // The query asked for the AD bit.
    let requests = servers.log.lock().unwrap().requests.clone();
    assert_eq!(requests[0].body[2..4], [0x01, 0x20]);

    let (servers, wires) = doh_servers(|_, mut reply| {
        reply[3] |= 0x20;
        dns_message(reply)
    });
    let resolution = doh_lookup(&servers, &wires, Trust::RequireAd).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(record::TrustOutcome::Authenticated));
    assert_eq!(resolution.wire, Some(WireKind::Https));
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
async fn a_naming_records_a_failed_lookup_under_require_ad_as_judged_by_nobody() {
    let trusted = Authority::new();
    let stranger = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answers)
        .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"dot"))
        .silent_at("192.0.2.54");
    let wires = trusted.wires();
    for (address, failure) in [
        ("192.0.2.53", LookupFailure::TlsFailed),
        ("192.0.2.54", LookupFailure::TimedOut),
    ] {
        let naming = Naming::proxy_servers(vec![dot(address, None)], Trust::RequireAd);
        sessions(&naming).trust(&wires);
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
        assert_eq!(servers.dials().len(), 1, "{alpn:?}");
        assert_eq!(servers.answered(), [2], "{alpn:?}");
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

#[tokio::test(start_paused = true)]
async fn doh_over_http_2_completes_on_a_new_connection_after_a_goaway() {
    let (servers, wires) = h2_servers(h2_message, Some(1), seven_then_six);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven_and_six());
    assert_eq!(resolution.error, None);
    assert_eq!(servers.dials().len(), 2);
    assert_eq!(servers.answered(), [1, 1]);
}

#[tokio::test(start_paused = true)]
async fn doh_over_http_2_replies_are_held_to_the_ad_bit() {
    let (servers, wires) = h2_servers(h2_message, None, answers);
    let resolution = doh_lookup(&servers, &wires, Trust::RequireAd).await;
    assert_eq!(
        resolution.error,
        Some(failure(
            LookupFailure::NotAuthenticated,
            "192.0.2.53: the answer was not marked authenticated"
        ))
    );
    assert_eq!(resolution.trust, Some(record::TrustOutcome::Refused));
    assert!(resolution.addresses.is_empty());

    let (servers, wires) = h2_servers(
        |mut reply| {
            reply[3] |= 0x20;
            h2_message(reply)
        },
        None,
        answers,
    );
    let resolution = doh_lookup(&servers, &wires, Trust::RequireAd).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(record::TrustOutcome::Authenticated));
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
    // Only the IPv6 reply is over the cap: it rides the connection the IPv4
    // query left open, and is not asked again on another.
    let (servers, wires) = h2_servers(padded(|qtype| qtype == 28), None, answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(servers.dials().len(), 1);
    assert_eq!(servers.answered(), [2]);

    let (servers, wires) = h2_servers(padded(|_| true), None, answers);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.error, invalid(), "{resolution:?}");
    // One connection for each family, none redialled.
    assert_eq!(servers.dials().len(), 2);
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
    assert_eq!(servers.dials().len(), 2);
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
    assert_eq!(servers.dials().len(), 2);
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
        let wires = authority.wires();
        let first = Naming::proxy_servers(vec![listed.clone()], Trust::AsSent);
        let clone = first.clone();
        let second = Naming::proxy_servers(vec![listed.clone()], Trust::AsSent);
        assert!(Arc::ptr_eq(sessions(&first), sessions(&clone)));
        assert!(!Arc::ptr_eq(sessions(&first), sessions(&second)));
        // A store trusts the test authority once: the clone's is the first's.
        sessions(&first).trust(&wires);
        sessions(&second).trust(&wires);
        // A name apiece, so none is answered from what the naming holds.
        for (naming, host) in [(&first, "a.test"), (&clone, "b.test"), (&second, "c.test")] {
            let resolution = through_naming(naming, &servers, host).await;
            assert_eq!(resolution.addresses, seven(), "{listed} {host}");
        }
        let sent = servers.sent();
        assert_eq!(sent.len(), 3, "{listed}");
        assert!(!offers_a_session(&sent[0]), "{listed}");
        // The clone resumes the first naming's session.
        assert!(offers_a_session(&sent[1]), "{listed}");
        // The other naming offers nothing from the first.
        assert!(!offers_a_session(&sent[2]), "{listed}");
    }
}
