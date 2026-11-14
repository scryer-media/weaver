// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::ttl::{ASKED, Answer, failure, respond, rr, soa, v4, wire};
use super::*;
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

// A certificate authority the tests trust, or one they do not.
struct Authority {
    certificate: CertificateDer<'static>,
    issuer: Issuer<'static, KeyPair>,
}

impl Authority {
    fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
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
    // DNS names) signed by this authority.
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
        config.alpn_protocols = vec![alpn.to_vec()];
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
}

#[derive(Default)]
struct Log {
    // The address and port of each dial.
    dials: Vec<SocketAddr>,
    // Every byte the client sent on each connection, in dial order.
    sent: Vec<Arc<Mutex<Vec<u8>>>>,
    // Queries answered on each connection, in dial order.
    queries: Vec<Arc<AtomicUsize>>,
    requests: Vec<Seen>,
}

// Encrypted DNS servers, by address, in process.
struct Servers {
    tls: Vec<(IpAddr, Arc<ServerConfig>)>,
    speaks: Speaks,
    answer: Arc<Answer>,
    log: Arc<Mutex<Log>>,
}

impl Servers {
    fn new(
        speaks: Speaks,
        answer: impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        Self {
            tls: Vec::new(),
            speaks,
            answer: Arc::new(answer),
            log: Default::default(),
        }
    }
    fn at(mut self, ip: &str, config: Arc<ServerConfig>) -> Self {
        self.tls.push((ip.parse().unwrap(), config));
        self
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
        let asked = server.address.ip();
        let Some((_, config)) = self.tls.iter().find(|(ip, _)| *ip == asked) else {
            return Err(TunnelError::Engine(format!("nothing listens at {asked}")));
        };
        let acceptor = TlsAcceptor::from(config.clone());
        let speaks = self.speaks.clone();
        let answer = self.answer.clone();
        let log = self.log.clone();
        tokio::spawn(async move {
            let Ok(mut tls) = acceptor.accept(Tap { inner, seen }).await else {
                return;
            };
            match speaks {
                Speaks::Dot { close_after } => loop {
                    if close_after.is_some_and(|after| queries.load(Ordering::Relaxed) >= after) {
                        return;
                    }
                    let Ok(size) = tls.read_u16().await else {
                        return;
                    };
                    let mut query = vec![0; size as usize];
                    if tls.read_exact(&mut query).await.is_err() {
                        return;
                    }
                    queries.fetch_add(1, Ordering::Relaxed);
                    let reply = answer(asked, u16_at(&query, query.len() - 4).unwrap(), &query);
                    let mut framed = (reply.len() as u16).to_be_bytes().to_vec();
                    framed.extend(reply);
                    if tls.write_all(&framed).await.is_err() || tls.flush().await.is_err() {
                        return;
                    }
                },
                Speaks::Doh { respond, dribble } => loop {
                    let Some(seen) = read_request(&mut tls).await else {
                        return;
                    };
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
            }
        });
        Ok(Box::new(client))
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

fn tls_failed(error: &Option<LookupError>, server: &str) -> bool {
    error.as_ref().is_some_and(|error| {
        error.failure == LookupFailure::TlsFailed
            && error.detail.starts_with(&format!("{server}: TLS failed: "))
    })
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

#[tokio::test]
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

#[tokio::test]
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
    assert!(
        tls_failed(&resolution.error, "192.0.2.53"),
        "{resolution:?}"
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

#[tokio::test]
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
    assert!(
        tls_failed(&resolution.error, "192.0.2.53"),
        "{resolution:?}"
    );
}

#[tokio::test]
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
    assert!(
        tls_failed(&resolution.error, "192.0.2.53"),
        "{resolution:?}"
    );
    assert_eq!(resolution.trust, Some(record::TrustOutcome::AsSent));
}

#[tokio::test]
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
    assert_eq!(
        servers.dials(),
        [
            "192.0.2.53:853".parse::<SocketAddr>().unwrap(),
            "192.0.2.54:853".parse().unwrap()
        ]
    );
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

#[tokio::test]
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

#[tokio::test]
async fn a_connection_the_server_closes_between_queries_is_opened_once_more() {
    let authority = Authority::new();
    let servers = Servers::new(
        Speaks::Dot {
            close_after: Some(1),
        },
        alias,
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
    // The first connection answers the alias, the second its target; the
    // third query finds the second closed too, and no third is opened.
    assert_eq!(resolution.addresses, seven());
    let log = servers.log.lock().unwrap();
    assert_eq!(log.dials.len(), 2);
    let answered: Vec<_> = log
        .queries
        .iter()
        .map(|queries| queries.load(Ordering::Relaxed))
        .collect();
    assert_eq!(answered, [1, 1]);
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
async fn doh_fails_a_reply_over_the_size_cap() {
    refused_as_invalid(|_, mut reply| {
        reply.resize(16385, 0);
        dns_message(reply)
    })
    .await;
}

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
async fn doh_fails_a_chunk_size_line_too_long() {
    // Sixteen hex digits fit; seventeen do not, nor an extension that runs
    // the line past its cap.
    for line in [
        "0000000000000005\r\n".to_owned(),
        "00000000000000005\r\n".to_owned(),
        format!("5;{}\r\n", "x".repeat(300)),
    ] {
        let digits_fit = line.starts_with("0000000000000005\r");
        let respond = move |_: &[u8], reply: Vec<u8>| {
            let mut body = line.clone().into_bytes();
            body.extend_from_slice(&reply[..5]);
            body.extend_from_slice(b"\r\n");
            body.extend(chunked(&reply[5..], 64));
            http(
                "200 OK",
                &[
                    "content-type: application/dns-message",
                    "transfer-encoding: chunked",
                ],
                &body,
            )
        };
        if digits_fit {
            let (servers, wires) = doh_servers(respond);
            let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
            assert_eq!(resolution.addresses, seven());
        } else {
            refused_as_invalid(respond).await;
        }
    }
}

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
async fn doh_asks_two_queries_on_one_connection_kept_alive() {
    let (servers, wires) = doh_servers(plain_reply);
    let resolution = doh_lookup(&servers, &wires, Trust::AsSent).await;
    assert_eq!(resolution.addresses, seven());
    let log = servers.log.lock().unwrap();
    assert_eq!(log.dials.len(), 1);
    assert_eq!(log.queries[0].load(Ordering::Relaxed), 2);
}

#[tokio::test]
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

#[tokio::test]
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
