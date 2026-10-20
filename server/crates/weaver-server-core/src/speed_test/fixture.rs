// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Loopback sources for speed test tests: an NNTP server that serves yEnc
// articles and can hold its answers, an HTTP(S) server that serves a test
// NZB, other pages and fixed-size blobs, and a server that never answers.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::watch;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

// One article: its message id and the payload its body decodes to.
pub struct FixtureArticle {
    pub message_id: String,
    pub payload: Vec<u8>,
}

impl FixtureArticle {
    // `len` bytes of a fixed pattern.
    pub fn patterned(message_id: &str, len: usize) -> Self {
        Self {
            message_id: message_id.into(),
            payload: (0..len).map(|i| (i * 31 % 251) as u8).collect(),
        }
    }
}

struct NntpShared {
    bodies: HashMap<String, Vec<u8>>,
    // BODY answers sent so far, and the count after which answers wait for
    // `release`.
    served: watch::Sender<usize>,
    hold_after: Option<usize>,
    release: watch::Sender<bool>,
    connections: watch::Sender<usize>,
    started: std::sync::Mutex<usize>,
}

pub struct NntpFixture {
    pub addr: SocketAddr,
    shared: Arc<NntpShared>,
}

impl NntpFixture {
    // Plain NNTP; with `tls`, NNTP over TLS. With `hold_after`, the answers
    // after the first `hold_after` BODY answers wait for `release`.
    pub async fn start(
        articles: Vec<FixtureArticle>,
        hold_after: Option<usize>,
        tls: Option<tokio_rustls::TlsAcceptor>,
    ) -> Self {
        let bodies = articles
            .into_iter()
            .map(|article| {
                let mut encoded = Vec::new();
                weaver_yenc::encode(&article.payload, &mut encoded, 128, "fixture.bin")
                    .expect("yEnc encode");
                (article.message_id, dot_stuff(&encoded))
            })
            .collect();
        let shared = Arc::new(NntpShared {
            bodies,
            served: watch::channel(0).0,
            hold_after,
            release: watch::channel(false).0,
            connections: watch::channel(0).0,
            started: std::sync::Mutex::new(0),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = shared.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accept.connections.send_modify(|count| *count += 1);
                let shared = accept.clone();
                let tls = tls.clone();
                tokio::spawn(async move {
                    let stream: Box<dyn Io> = match tls {
                        Some(acceptor) => match acceptor.accept(stream).await {
                            Ok(stream) => Box::new(stream),
                            Err(_) => return,
                        },
                        None => Box::new(stream),
                    };
                    let _ = serve_nntp(stream, shared).await;
                });
            }
        });
        Self { addr, shared }
    }

    pub fn served(&self) -> watch::Receiver<usize> {
        self.shared.served.subscribe()
    }

    // Sockets accepted so far.
    pub fn connections(&self) -> watch::Receiver<usize> {
        self.shared.connections.subscribe()
    }

    pub fn release(&self) {
        self.shared.release.send_replace(true);
    }

    // The raw (wire) size of one article's body, as a BODY answer carries it
    // between its status line and terminator.
    pub fn raw_size(&self, message_id: &str) -> usize {
        self.shared.bodies[message_id].len()
    }
}

// A test NZB listing `(message id, declared bytes)` segments of one file.
pub fn nzb(segments: &[(&str, u64)]) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n<file poster=\"fixture\" date=\"1700000000\" subject=\"fixture.bin (1/1)\">\n<groups><group>alt.binaries.test</group></groups>\n<segments>\n",
    );
    for (number, (id, bytes)) in segments.iter().enumerate() {
        out.push_str(&format!(
            "<segment bytes=\"{bytes}\" number=\"{}\">{id}</segment>\n",
            number + 1
        ));
    }
    out.push_str("</segments>\n</file>\n</nzb>\n");
    out
}

fn dot_stuff(encoded: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(encoded.len() + 16);
    let mut line_start = true;
    for &byte in encoded {
        if line_start && byte == b'.' {
            out.push(b'.');
        }
        out.push(byte);
        line_start = byte == b'\n';
    }
    if !out.ends_with(b"\r\n") {
        out.extend_from_slice(b"\r\n");
    }
    out
}

async fn serve_nntp(stream: Box<dyn Io>, shared: Arc<NntpShared>) -> std::io::Result<()> {
    let mut stream = BufReader::new(stream);
    stream.write_all(b"200 fixture ready\r\n").await?;
    let mut line = String::new();
    loop {
        line.clear();
        if stream.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let command = line.trim_end();
        let mut words = command.split_whitespace();
        let verb = words.next().unwrap_or("").to_ascii_uppercase();
        match verb.as_str() {
            "CAPABILITIES" => {
                stream
                    .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                    .await?
            }
            "MODE" => stream.write_all(b"200 reader\r\n").await?,
            "AUTHINFO" => match words.next().map(str::to_ascii_uppercase).as_deref() {
                Some("USER") => stream.write_all(b"381 password\r\n").await?,
                _ => stream.write_all(b"281 welcome\r\n").await?,
            },
            "GROUP" => {
                let group = words.next().unwrap_or("");
                stream
                    .write_all(format!("211 0 0 0 {group}\r\n").as_bytes())
                    .await?
            }
            "BODY" => {
                let id = words.next().unwrap_or("");
                let id = id.trim_start_matches('<').trim_end_matches('>');
                let Some(body) = shared.bodies.get(id) else {
                    stream.write_all(b"430 no such article\r\n").await?;
                    continue;
                };
                let position = {
                    let mut started = shared.started.lock().unwrap();
                    *started += 1;
                    *started
                };
                if shared.hold_after.is_some_and(|after| position > after) {
                    let mut release = shared.release.subscribe();
                    let _ = release.wait_for(|released| *released).await;
                }
                stream
                    .write_all(format!("222 0 <{id}>\r\n").as_bytes())
                    .await?;
                stream.write_all(body).await?;
                stream.write_all(b".\r\n").await?;
                stream.flush().await?;
                shared.served.send_modify(|served| *served += 1);
            }
            "DATE" => stream.write_all(b"111 20260101000000\r\n").await?,
            "QUIT" => {
                stream.write_all(b"205 bye\r\n").await?;
                return Ok(());
            }
            _ => stream.write_all(b"500 unknown\r\n").await?,
        }
        stream.flush().await?;
    }
}

// An HTTP/1.1 source: `/blob` answers `blob_len` bytes, each page its own
// body, `/limited` answers HTTP 429 as a rate-limiting provider does, a POST
// to `/upload` takes its body, and anything else 404. With `tls`, HTTPS.
pub struct HttpFixture {
    pub addr: SocketAddr,
    requests: watch::Sender<usize>,
    uploaded: watch::Sender<u64>,
}

impl HttpFixture {
    // `/test.nzb` answers the NZB.
    pub async fn start(
        nzb: String,
        blob_len: usize,
        tls: Option<tokio_rustls::TlsAcceptor>,
    ) -> Self {
        Self::serve(vec![("/test.nzb", nzb.into_bytes())], blob_len, tls).await
    }

    // Each `(path, body)` page answers its body.
    pub async fn serve(
        pages: Vec<(&str, Vec<u8>)>,
        blob_len: usize,
        tls: Option<tokio_rustls::TlsAcceptor>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = watch::channel(0).0;
        let counter = requests.clone();
        let uploaded = watch::channel(0).0;
        let taken = uploaded.clone();
        let pages: Arc<HashMap<String, Vec<u8>>> = Arc::new(
            pages
                .into_iter()
                .map(|(path, body)| (path.to_owned(), body))
                .collect(),
        );
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tls = tls.clone();
                let pages = pages.clone();
                let counter = counter.clone();
                let taken = taken.clone();
                tokio::spawn(async move {
                    let stream: Box<dyn Io> = match tls {
                        Some(acceptor) => match acceptor.accept(stream).await {
                            Ok(stream) => Box::new(stream),
                            Err(_) => return,
                        },
                        None => Box::new(stream),
                    };
                    let _ = serve_http(stream, &pages, blob_len, &counter, &taken).await;
                });
            }
        });
        Self {
            addr,
            requests,
            uploaded,
        }
    }

    pub fn requests(&self) -> watch::Receiver<usize> {
        self.requests.subscribe()
    }

    // Body bytes posted to `/upload` so far.
    pub fn uploaded(&self) -> watch::Receiver<u64> {
        self.uploaded.subscribe()
    }
}

// A LibreSpeed server list naming each `(id, address, ping path)` server
// over HTTPS, with `/blob` as its download endpoint.
pub fn librespeed_list(servers: &[(&str, SocketAddr, Option<&str>)]) -> Vec<u8> {
    let entries: Vec<serde_json::Value> = servers
        .iter()
        .map(|(id, addr, ping)| {
            let mut entry = serde_json::json!({
                "id": id,
                "name": format!("fixture {id}"),
                "server": format!("https://{addr}/"),
                "dlURL": "blob",
                "ulURL": "",
                "getIpURL": "",
                "sponsorName": "",
            });
            if let Some(ping) = ping {
                entry["pingURL"] = serde_json::Value::from(*ping);
            }
            entry
        })
        .collect();
    serde_json::to_vec(&entries).unwrap()
}

// Connections a silent server has taken, and those its peer has closed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SilentCounts {
    pub accepted: usize,
    pub closed: usize,
}

// A TCP server that never answers: it takes every connection and reads and
// drops what it is sent until the peer closes it.
pub struct SilentFixture {
    pub addr: SocketAddr,
    counts: watch::Sender<SilentCounts>,
}

impl SilentFixture {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let counts = watch::channel(SilentCounts::default()).0;
        let accept = counts.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                accept.send_modify(|counts| counts.accepted += 1);
                let counts = accept.clone();
                tokio::spawn(async move {
                    let mut buffer = [0u8; 4096];
                    while let Ok(1..) = stream.read(&mut buffer).await {}
                    counts.send_modify(|counts| counts.closed += 1);
                });
            }
        });
        Self { addr, counts }
    }

    pub fn counts(&self) -> watch::Receiver<SilentCounts> {
        self.counts.subscribe()
    }
}

async fn serve_http(
    stream: Box<dyn Io>,
    pages: &HashMap<String, Vec<u8>>,
    blob_len: usize,
    counter: &watch::Sender<usize>,
    uploaded: &watch::Sender<u64>,
) -> std::io::Result<()> {
    let mut stream = BufReader::new(stream);
    let mut request_line = String::new();
    stream.read_line(&mut request_line).await?;
    let mut header = String::new();
    let mut content_length = 0u64;
    loop {
        header.clear();
        if stream.read_line(&mut header).await? == 0 || header == "\r\n" {
            break;
        }
        if let Some((name, value)) = header.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }
    counter.send_modify(|count| *count += 1);
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");
    let path = path.split('?').next().unwrap_or(path);
    if request_line.starts_with("POST ") && path == "/upload" {
        let mut buffer = vec![0u8; 64 * 1024];
        let mut left = content_length;
        while left > 0 {
            let want = buffer.len().min(left as usize);
            let read = stream.read(&mut buffer[..want]).await?;
            if read == 0 {
                return Ok(());
            }
            left -= read as u64;
            uploaded.send_modify(|bytes| *bytes += read as u64);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await?;
        stream.flush().await?;
        return stream.shutdown().await;
    }
    let body: Vec<u8> = match (path, pages.get(path)) {
        (_, Some(body)) => body.clone(),
        ("/blob", None) => (0..blob_len).map(|i| (i % 253) as u8).collect(),
        ("/limited", None) => {
            stream
                .write_all(
                    b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await?;
            return stream.flush().await;
        }
        _ => {
            stream
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await?;
            return stream.flush().await;
        }
    };
    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    stream.shutdown().await
}

// A TLS acceptor for the loopback certificate, and the certificate's DER
// and PEM to trust.
pub fn loopback_tls() -> (tokio_rustls::TlsAcceptor, Vec<u8>, String) {
    use tokio_rustls::rustls;
    let cert = weaver_tunnel::test_support::tls::loopback_certificate();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![rustls::pki_types::CertificateDer::from(
            cert.cert_der.clone(),
        )],
        rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pkcs8_der.clone().into()),
    )
    .unwrap();
    (
        tokio_rustls::TlsAcceptor::from(Arc::new(config)),
        cert.cert_der,
        cert.cert_pem,
    )
}
