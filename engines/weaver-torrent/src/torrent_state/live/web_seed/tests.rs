// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    sync::atomic::{AtomicBool, Ordering},
};

use tokio::sync::mpsc::unbounded_channel;

use super::*;
use crate::{core::lengths::ChunkInfo, peer_protocol::extended::handshake::ExtendedHandshake};

// What the server was asked.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Head {
    target: String,
    range: Option<(u64, u64)>,
    user_agent: Option<String>,
}

struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
    // How much of the body is sent before the connection closes.
    sent: Option<usize>,
    content_length: bool,
}

impl Reply {
    fn status(status: u16) -> Self {
        Reply {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            sent: None,
            content_length: true,
        }
    }

    fn header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    // `content` as a server that honours ranges serves it.
    fn ranged(content: &[u8], head: &Head) -> Self {
        match head.range {
            Some((first, last)) => Reply::status(206)
                .header(
                    "Content-Range",
                    format!("bytes {first}-{last}/{}", content.len()),
                )
                .body(&content[usize::try_from(first).unwrap()..=usize::try_from(last).unwrap()]),
            None => Reply::status(200).body(content),
        }
    }
}

// An HTTP server on loopback, one request per connection. Its accept
// loop runs on the blocking pool, which holds the paused clock still
// (tokio does not advance it while a blocking task runs): time moves
// only when a test moves it.
struct Server {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<Head>>>,
    stop: Arc<AtomicBool>,
}

type Respond = dyn Fn(&Head) -> Reply + Send + Sync;

impl Server {
    fn start(respond: impl Fn(&Head) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let respond: Arc<Respond> = Arc::new(respond);
        {
            let (seen, stop) = (seen.clone(), stop.clone());
            tokio::task::spawn_blocking(move || {
                for conn in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(conn) = conn else { continue };
                    let (respond, seen) = (respond.clone(), seen.clone());
                    std::thread::spawn(move || serve(conn, &*respond, &seen));
                }
            });
        }
        Self { addr, seen, stop }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn seen(&self) -> Vec<Head> {
        self.seen.lock().clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        crate::test_support::knock(self.addr);
    }
}

fn serve(mut conn: TcpStream, respond: &Respond, seen: &Mutex<Vec<Head>>) {
    let mut buf = Vec::new();
    let mut read = [0u8; 4096];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        match conn.read(&mut read) {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&read[..n]),
        }
    }
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);
    req.parse(&buf).unwrap();
    let range = req
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("range"))
        .map(|h| {
            let v = std::str::from_utf8(h.value).unwrap();
            let (a, b) = v.strip_prefix("bytes=").unwrap().split_once('-').unwrap();
            (a.parse().unwrap(), b.parse().unwrap())
        });
    let user_agent = req
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("user-agent"))
        .map(|h| String::from_utf8(h.value.to_vec()).unwrap());
    let head = Head {
        target: req.path.unwrap().to_owned(),
        range,
        user_agent,
    };
    let reply = respond(&head);
    seen.lock().push(head);
    let mut out = format!("HTTP/1.1 {} X\r\nConnection: close\r\n", reply.status);
    if reply.content_length {
        out += &format!("Content-Length: {}\r\n", reply.body.len());
    }
    for (name, value) in &reply.headers {
        out += &format!("{name}: {value}\r\n");
    }
    out += "\r\n";
    let sent = reply.sent.unwrap_or(reply.body.len());
    let _ = conn.write_all(out.as_bytes());
    let _ = conn.write_all(&reply.body[..sent]);
}

// A peer handler that records what the web seed tells it.
#[derive(Default)]
struct Fake {
    bitfield: Mutex<Option<Vec<u8>>>,
    handshaken: AtomicBool,
    unchoked: AtomicBool,
    pieces: Mutex<Vec<(u32, u32, Vec<u8>)>>,
    arrived: Notify,
}

impl Fake {
    async fn wait_for(&self, n: usize) {
        loop {
            let arrived = self.arrived.notified();
            if self.pieces.lock().len() >= n {
                return;
            }
            arrived.await;
        }
    }

    // The bytes delivered, in torrent order.
    fn bytes(&self, layout: &Layout) -> Vec<u8> {
        let mut pieces = self.pieces.lock().clone();
        pieces.sort_by_key(|(i, b, _)| (*i, *b));
        let mut out = Vec::new();
        for (i, b, data) in pieces {
            assert_eq!(
                layout.offset_of(&Request {
                    index: i,
                    begin: b,
                    length: 0
                }),
                out.len() as u64
            );
            out.extend(data);
        }
        out
    }
}

impl PeerConnectionHandler for &Fake {
    fn should_send_bitfield(&self) -> bool {
        false
    }
    fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
        Ok(0)
    }
    fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
        self.handshaken.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn on_received_message(&self, msg: Message<'_>) -> anyhow::Result<()> {
        match msg {
            Message::Bitfield(b) => *self.bitfield.lock() = Some(b.as_ref().to_vec()),
            Message::Unchoke => self.unchoked.store(true, Ordering::SeqCst),
            Message::Piece(p) => {
                let (b0, b1) = p.data();
                let data = [b0, b1].concat();
                self.pieces.lock().push((p.index, p.begin, data));
                self.arrived.notify_waiters();
            }
            other => panic!("unexpected {other:?}"),
        }
        Ok(())
    }
    fn on_uploaded_bytes(&self, _: u32) {}
    fn read_chunk(&self, _: &ChunkInfo, _: &mut [u8]) -> anyhow::Result<()> {
        anyhow::bail!("a web seed uploads nothing")
    }
}

const PIECE: u32 = 32768;

fn content(len: usize, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap() ^ salt)
        .collect()
}

// Files `(name, len, pad)` of a torrent named `t`, multi-file unless
// there is one.
fn layout(files: &[(&str, u64, bool)]) -> Layout {
    let total = files.iter().map(|f| f.1).sum();
    let mut offset = 0;
    let single = files.len() == 1;
    let files = files
        .iter()
        .map(|&(name, len, pad)| {
            let path = if single {
                name.to_owned()
            } else {
                format!("t/{name}")
            };
            let f = LayoutFile {
                offset,
                len,
                pad,
                path: escape_path(path.as_bytes()),
            };
            offset += len;
            f
        })
        .collect();
    Layout {
        info_hash: Id20::new([7; 20]),
        lengths: Lengths::new(total, PIECE).unwrap(),
        files,
    }
}

// Every block of `pieces`, in order.
fn blocks(layout: &Layout, pieces: std::ops::Range<u32>) -> Vec<Request> {
    pieces
        .flat_map(|p| {
            let p = layout.lengths.validate_piece_index(p).unwrap();
            layout.lengths.iter_chunk_infos(p).map(move |c| Request {
                index: p.get(),
                begin: c.offset,
                length: c.size,
            })
        })
        .collect()
}

// The client the tests' torrent names itself as.
const UA: &str = "Client/1.0";

// Every address may be connected to.
const ALL: &(dyn Fn(SocketAddr) -> bool + Send + Sync) = &|_| true;

// A client looks no name up: it reaches a host only at the addresses it
// was built for.
struct NoNames;

impl reqwest::dns::Resolve for NoNames {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let name = name.as_str().to_owned();
        Box::pin(async move { Err(format!("{name}: no lookups in tests").into()) })
    }
}

fn builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .no_proxy()
        .dns_resolver(Arc::new(NoNames))
}

// The session's own clients.
fn direct() -> Arc<dyn HttpClients> {
    Arc::new(crate::test_support::Loopback::with(builder))
}

// The clients of a network where every address is the server at `at`:
// a client built for an address reaches the server there. Records the
// addresses clients were built for.
struct Wired {
    at: SocketAddr,
    built: Mutex<Vec<IpAddr>>,
}

impl Wired {
    fn new(at: SocketAddr) -> Arc<Self> {
        Arc::new(Self {
            at,
            built: Mutex::default(),
        })
    }

    fn built(&self) -> Vec<IpAddr> {
        self.built.lock().clone()
    }
}

impl HttpClients for Wired {
    fn client(
        &self,
        target: Option<HttpTarget<'_>>,
        shape: &ClientShape,
    ) -> anyhow::Result<reqwest::Client> {
        let at = [self.at.ip()];
        let target = target.map(|t| {
            self.built.lock().extend(t.addrs);
            HttpTarget { addrs: &at, ..t }
        });
        crate::test_support::client(builder(), target, shape)
    }
}

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

// A resolver that knows `names` and no other.
struct Names(HashMap<String, Vec<IpAddr>>);

impl Resolver for Names {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
        let found = self.0.get(host).map(|ips| {
            ips.iter()
                .map(|ip| SocketAddr::new(*ip, port))
                .collect::<Vec<_>>()
        });
        Box::pin(async move {
            found
                .map(crate::core::net::Resolved::Addresses)
                .ok_or_else(|| std::io::ErrorKind::NotFound.into())
        })
    }
}

fn names(names: &[(&str, &str)]) -> Arc<dyn Resolver> {
    let mut map: HashMap<String, Vec<IpAddr>> = HashMap::new();
    for (name, ip) in names {
        map.entry((*name).into())
            .or_default()
            .push(ip.parse().unwrap());
    }
    Arc::new(Names(map))
}

// A seed the operator named: the test servers listen on loopback.
fn one_seed(url: String, kind: WebSeedKind) -> (Arc<WebSeeds>, SeedLink) {
    seed_named(url, kind, NamedBy::Operator)
}

fn seed_named(url: String, kind: WebSeedKind, named_by: NamedBy) -> (Arc<WebSeeds>, SeedLink) {
    let seeds = Arc::new(WebSeeds::new([WebSeed {
        url,
        kind,
        named_by,
    }]));
    let link = seeds.link(0).unwrap();
    (seeds, link)
}

fn entry<R>(seeds: &WebSeeds, f: impl FnOnce(&Entry) -> R) -> R {
    seeds.with_entry(0, |e| f(e)).unwrap()
}

fn lacking(files: &[usize], pieces: &[u32]) -> Lacks {
    Lacks {
        files: files.iter().copied().collect(),
        pieces: pieces.iter().copied().collect(),
    }
}

// The pieces a seed that lacks `lacks` has, one bool each.
fn has(layout: &Layout, lacks: &Lacks) -> Vec<bool> {
    let total = layout.lengths.total_pieces() as usize;
    layout.bitfield(lacks)[..total].iter().by_vals().collect()
}

// Runs the connection on `requests` until `until` blocks arrived, or
// it ends.
async fn drive(
    conn: &WebSeedConnection<'_>,
    fake: &Fake,
    requests: &[Request],
    until: usize,
) -> crate::Result<()> {
    let (tx, rx) = unbounded_channel();
    for r in requests {
        tx.send(WriterRequest::Message(Message::Request(*r)))
            .unwrap();
    }
    tokio::select! {
        r = conn.run(fake, rx) => r,
        () = fake.wait_for(until) => Ok(()),
    }
}

// The seed has every piece and never chokes; requests that follow each
// other are fetched with one range request per file they span, a pad
// file's bytes are zeros asked of no one, and the files are under the
// torrent's name below the seed's URL (BEP 19).
#[tokio::test(start_paused = true)]
async fn a_web_seed_fetches_following_requests_with_a_range_per_file() {
    let (a, b) = (content(40000, 1), content(30000, 2));
    let (sa, sb) = (a.clone(), b.clone());
    let server = Server::start(move |head| match head.target.as_str() {
        "/seed/t/a%20file" => Reply::ranged(&sa, head),
        "/seed/t/b" => Reply::ranged(&sb, head),
        _ => Reply::status(404),
    });
    let layout = layout(&[
        ("a file", 40000, false),
        ("pad", 25536, true),
        ("b", 30000, false),
    ]);
    let (_seeds, link) = one_seed(server.url("/seed"), WebSeedKind::Url);
    let counts = Arc::new(WebSeedStats::default());
    let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA)
        .counting(counts.clone());
    let fake = Fake::default();

    let requests = blocks(&layout, 0..3);
    drive(&conn, &fake, &requests, requests.len())
        .await
        .unwrap();
    // One request per file, and the bytes of both bodies.
    let counted = counts.snapshot();
    assert_eq!(counted.requests[WebSeedResult::Ok as usize], 2);
    assert_eq!(counted.requests.iter().sum::<u64>(), 2);
    assert_eq!(counted.bytes, 70000);
    assert_eq!((counted.waits, counted.failed), (0, 0));

    assert_eq!(fake.bitfield.lock().clone(), Some(vec![0b1110_0000]));
    assert!(fake.unchoked.load(Ordering::SeqCst));
    let mut torrent = a;
    torrent.extend(vec![0; 25536]);
    torrent.extend(b);
    assert_eq!(fake.bytes(&layout), torrent);
    assert_eq!(
        server.seen(),
        vec![
            Head {
                target: "/seed/t/a%20file".into(),
                range: Some((0, 39999)),
                user_agent: Some(UA.into()),
            },
            Head {
                target: "/seed/t/b".into(),
                range: Some((0, 29999)),
                user_agent: Some(UA.into()),
            },
        ]
    );
}

// An error status: the server lacks the file, so the seed is had
// without its pieces from then on, and is left for what its
// Retry-After says, URLSEED_WAIT_RETRY at least once the connection
// fails (libtorrent `web_peer_connection::on_receive`, `handle_error`,
// `disconnect`).
#[tokio::test(start_paused = true)]
async fn an_error_status_loses_the_file_and_the_seed_waits_retry_after() {
    let a = content(40000, 1);
    let server = Server::start(move |head| match head.target.as_str() {
        "/seed/t/a" => Reply::ranged(&a, head),
        "/seed/t/b" => Reply::status(503).header("Retry-After", "120"),
        _ => Reply::status(404),
    });
    let layout = layout(&[
        ("a", 40000, false),
        ("pad", 25536, true),
        ("b", 30000, false),
    ]);
    let (seeds, link) = one_seed(server.url("/seed/"), WebSeedKind::Url);
    let counts = Arc::new(WebSeedStats::default());
    let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA)
        .counting(counts.clone());
    let fake = Fake::default();

    let failed = drive(&conn, &fake, &blocks(&layout, 2..3), 1).await;
    assert!(failed.is_err(), "{failed:?}");
    let counted = counts.snapshot();
    assert_eq!(counted.requests[WebSeedResult::Unavailable as usize], 1);
    assert_eq!((counted.waits, counted.bytes), (1, 0));
    link.failed();
    let now = Instant::now();
    assert_eq!(entry(&seeds, |e| e.lacks.files.clone()), HashSet::from([2]));
    assert_eq!(
        entry(&seeds, |e| e.retry),
        Some(now + Duration::from_secs(120))
    );
    drop(link);

    let interesting = |l: &Lacks| layout.bitfield(l).any();
    assert_eq!(has(&layout, &lacking(&[2], &[])), [true, true, false]);
    let due = |at| seeds.due(at, interesting, false).len();
    assert_eq!(due(now + Duration::from_secs(119)), 0);
    assert_eq!(due(now + Duration::from_secs(120)), 1);

    // Without Retry-After, and with a wait shorter than the failure's,
    // the seed waits URLSEED_WAIT_RETRY.
    for reply in [
        Reply::status(404),
        Reply::status(500).header("Retry-After", "5"),
        Reply::status(500).header("Retry-After", "-5"),
    ] {
        let reply = Mutex::new(Some(reply));
        let server = Server::start(move |_| reply.lock().take().unwrap());
        let (seeds, link) = one_seed(server.url("/f"), WebSeedKind::Url);
        let layout = super::tests::layout(&[("f", 100, false)]);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        assert!(
            drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
                .await
                .is_err()
        );
        link.failed();
        assert_eq!(
            entry(&seeds, |e| e.retry),
            Some(Instant::now() + URLSEED_WAIT_RETRY)
        );
        // The only file is lost: the seed has nothing to give.
        let lacks = entry(&seeds, |e| e.lacks.clone());
        assert!(!layout.bitfield(&lacks).any());
    }
}

// A Retry-After past what libtorrent's `seconds32` holds is held to it,
// rather than overflowing the instant it is added to.
#[tokio::test(start_paused = true)]
async fn a_retry_after_past_seconds32_waits_the_longest_seconds32() {
    for value in [
        "2147483648",
        "18446744073709551615",
        "99999999999999999999999",
    ] {
        let server = Server::start(move |_| Reply::status(503).header("Retry-After", value));
        let (seeds, link) = one_seed(server.url("/f"), WebSeedKind::Url);
        let layout = super::tests::layout(&[("f", 100, false)]);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        assert!(
            drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
                .await
                .is_err()
        );
        link.failed();
        assert_eq!(
            entry(&seeds, |e| e.retry),
            Some(Instant::now() + Duration::from_secs(MAX_RETRY_WAIT_SECS)),
            "{value}"
        );
    }
}

// A redirect is followed, and the file is asked for where it went from
// then on.
#[tokio::test(start_paused = true)]
async fn a_moved_file_is_asked_for_where_it_went() {
    let (a, b) = (content(40000, 3), content(10, 4));
    let (sa, sb) = (a.clone(), b.clone());
    let server = Server::start(move |head| match head.target.as_str() {
        "/seed/t/a" => Reply::status(302).header("Location", "/moved/a"),
        "/moved/a" => Reply::ranged(&sa, head),
        "/seed/t/b" => Reply::ranged(&sb, head),
        _ => Reply::status(404),
    });
    let layout = layout(&[("a", 40000, false), ("b", 10, false)]);
    let (seeds, link) = one_seed(server.url("/seed/"), WebSeedKind::Url);
    let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
    let fake = Fake::default();

    drive(&conn, &fake, &blocks(&layout, 0..1), 2)
        .await
        .unwrap();
    // The move is asked for under the torrent's client, as the seed was.
    let agents: Vec<_> = server.seen().into_iter().map(|h| h.user_agent).collect();
    assert_eq!(agents, [Some(UA.to_owned()), Some(UA.to_owned())]);
    assert_eq!(
        entry(&seeds, |e| e.redirects.clone()),
        HashMap::from([(
            0,
            Redirect {
                url: server.url("/moved/a"),
                // The seed is on loopback, which is no public address:
                // the move is asked for as the seed was named.
                named_by: NamedBy::Operator,
            }
        )])
    );
    let fake = Fake::default();
    let requests = blocks(&layout, 1..2);
    drive(&conn, &fake, &requests, requests.len())
        .await
        .unwrap();
    let targets: Vec<_> = server.seen().into_iter().map(|h| h.target).collect();
    assert_eq!(targets, ["/seed/t/a", "/moved/a", "/moved/a", "/seed/t/b"]);
    assert_eq!(fake.pieces.lock()[0].2, [&a[32768..], &b[..]].concat());
}

// A seed that moves a file to a host with an internationalized label is
// refused at the move before that name is looked up, and the file is
// lost to the seed; a session that allows such names follows it.
#[tokio::test(start_paused = true)]
async fn a_move_to_an_internationalized_name_is_refused() {
    let a = content(100, 7);
    let port = Arc::new(Mutex::new(0u16));
    let location = port.clone();
    let server = Server::start(move |head| match head.target.as_str() {
        "/seed/f" => Reply::status(302).header(
            "Location",
            format!("http://xn--bcher-kva.test:{}/moved/f", location.lock()),
        ),
        _ => Reply::ranged(&a, head),
    });
    *port.lock() = server.addr.port();
    let url = format!("http://seed.test:{}/seed/", server.addr.port());
    let layout = layout(&[("f", 100, false)]);
    let requests = blocks(&layout, 0..1);
    let targets = || {
        server
            .seen()
            .into_iter()
            .map(|h| h.target)
            .collect::<Vec<_>>()
    };
    let resolver = || {
        names(&[
            ("seed.test", "127.0.0.1"),
            ("xn--bcher-kva.test", "127.0.0.2"),
        ])
    };

    let clients = Wired::new(server.addr);
    let (seeds, link) = one_seed(url.clone(), WebSeedKind::Url);
    let conn = WebSeedConnection::new(clients.clone(), resolver(), ALL, &layout, &link, UA);
    assert!(drive(&conn, &Fake::default(), &requests, 1).await.is_err());
    assert_eq!(entry(&seeds, |e| e.lacks.files.clone()), HashSet::from([0]));
    assert!(entry(&seeds, |e| e.redirects.is_empty()));
    assert_eq!(targets(), ["/seed/f"]);
    assert_eq!(clients.built(), [ip("127.0.0.1")]);

    let clients = Wired::new(server.addr);
    let (seeds, link) = one_seed(url, WebSeedKind::Url);
    let conn = WebSeedConnection::new(clients.clone(), resolver(), ALL, &layout, &link, UA)
        .allowing_idna(true);
    drive(&conn, &Fake::default(), &requests, 1).await.unwrap();
    assert!(entry(&seeds, |e| e.lacks.files.is_empty()));
    assert_eq!(targets(), ["/seed/f", "/seed/f", "/moved/f"]);
    assert_eq!(clients.built(), [ip("127.0.0.1"), ip("127.0.0.2")]);
}

// The address a seed is asked at is known before it is asked, and is
// the first its host resolves to that the seed's naming lets it be asked
// at. A seed that came with the torrent is not asked at a loopback,
// private, link-local or unique-local address, by IP or by a name, with
// a query or without, and is disabled; of a name that also resolves to
// a public address it is asked there only. A seed the operator named is
// asked at a private address.
#[tokio::test(start_paused = true)]
async fn a_seed_the_torrent_named_is_asked_at_public_addresses_only() {
    let f = content(100, 5);
    let server = Server::start(move |head| Reply::ranged(&f, head));
    let port = server.addr.port();
    let layout = layout(&[("f", 100, false)]);
    let clients = Wired::new(server.addr);
    for query in ["", "?x=1"] {
        let mut refused = vec![(server.url(&format!("/f{query}")), names(&[]))];
        for private in ["10.0.0.1", "192.168.1.1", "169.254.1.1", "fd00::1"] {
            refused.push((
                format!("http://seed.test:{port}/f{query}"),
                names(&[("seed.test", private)]),
            ));
        }
        for (url, resolver) in refused {
            let (seeds, link) = seed_named(url.clone(), WebSeedKind::Url, NamedBy::Torrent);
            let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
            let failed = drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1).await;
            assert!(failed.is_err(), "{url}");
            assert!(entry(&seeds, |e| e.disabled), "{url}");
        }
    }
    assert!(server.seen().is_empty());
    assert!(clients.built().is_empty());

    let (seeds, link) = seed_named(
        format!("http://seed.test:{port}/f"),
        WebSeedKind::Url,
        NamedBy::Torrent,
    );
    let resolver = names(&[("seed.test", "10.0.0.1"), ("seed.test", "8.8.8.8")]);
    let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
    drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
        .await
        .unwrap();
    assert!(!entry(&seeds, |e| e.disabled));
    assert_eq!(server.seen().len(), 1);
    assert_eq!(clients.built(), [ip("8.8.8.8")]);

    let (seeds, link) = one_seed(format!("http://seed.test:{port}/f"), WebSeedKind::Url);
    let resolver = names(&[("seed.test", "10.0.0.1")]);
    let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
    drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
        .await
        .unwrap();
    assert!(!entry(&seeds, |e| e.disabled));
    assert_eq!(server.seen().len(), 2);
    assert_eq!(clients.built(), [ip("8.8.8.8"), ip("10.0.0.1")]);
}

// A seed on the local network may not carry a query string, whoever
// named it, judged by the address its host resolves to; it is disabled
// and never asked (libtorrent `ssrf_mitigation`, src/torrent.cpp:6860-6886
// and `aux::is_local`, src/ip_helpers.cpp:78-106, at v2.0.15). The query
// the engine adds to ask a BEP 17 seed is not the seed's own. A file
// moved to a local address with a query is lost to the seed.
#[tokio::test(start_paused = true)]
async fn a_seed_on_the_local_network_carries_no_query() {
    let f = content(100, 9);
    let port = Arc::new(Mutex::new(0u16));
    let location = port.clone();
    let server = Server::start(move |head| match head.target.as_str() {
        "/seed/f" => Reply::status(302).header(
            "Location",
            format!("http://moved.test:{}/moved/f?x=1", location.lock()),
        ),
        _ => Reply::ranged(&f, head),
    });
    *port.lock() = server.addr.port();
    let port = server.addr.port();
    let layout = layout(&[("f", 100, false)]);
    let requests = blocks(&layout, 0..1);

    let mut refused = vec![(server.url("/f?"), names(&[]))];
    for local in [
        "10.0.0.1",
        "172.16.0.1",
        "100.64.0.1",
        "127.0.0.1",
        "fe80::1",
    ] {
        refused.push((
            format!("http://seed.test:{port}/f?x=1"),
            names(&[("seed.test", local)]),
        ));
    }
    for (url, resolver) in refused {
        let clients = Wired::new(server.addr);
        let (seeds, link) = one_seed(url.clone(), WebSeedKind::Url);
        let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
        assert!(
            drive(&conn, &Fake::default(), &requests, 1).await.is_err(),
            "{url}"
        );
        assert!(entry(&seeds, |e| e.disabled), "{url}");
        assert!(clients.built().is_empty(), "{url}");
    }
    assert!(server.seen().is_empty());

    // Moved to a local address with a query: the file is lost to the
    // seed, and the new address is never asked.
    let clients = Wired::new(server.addr);
    let resolver = names(&[("seed.test", "10.0.0.1"), ("moved.test", "10.0.0.2")]);
    let (seeds, link) = one_seed(format!("http://seed.test:{port}/seed/"), WebSeedKind::Url);
    let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
    assert!(drive(&conn, &Fake::default(), &requests, 1).await.is_err());
    assert_eq!(entry(&seeds, |e| e.lacks.files.clone()), HashSet::from([0]));
    assert!(!entry(&seeds, |e| e.disabled));
    let targets: Vec<_> = server.seen().into_iter().map(|h| h.target).collect();
    assert_eq!(targets, ["/seed/f"]);
    assert_eq!(clients.built(), [ip("10.0.0.1")]);
}

// A seed at a public address may not move a file to a private one,
// whoever named the seed: the engine follows the redirect itself, and
// the private address is never asked, nor a client built for it; the
// file is lost to the seed. Moved to a public address, the file is
// fetched there and asked for there, at public addresses only, from
// then on.
#[tokio::test(start_paused = true)]
async fn a_public_seed_may_not_move_a_file_to_a_private_address() {
    let a = content(100, 6);
    let port = Arc::new(Mutex::new(0u16));
    let location = port.clone();
    let server = Server::start(move |head| match head.target.as_str() {
        "/seed/f" => Reply::status(302).header(
            "Location",
            format!("http://moved.test:{}/moved/f", location.lock()),
        ),
        _ => Reply::ranged(&a, head),
    });
    *port.lock() = server.addr.port();
    let url = format!("http://seed.test:{}/seed/", server.addr.port());
    let layout = layout(&[("f", 100, false)]);
    let requests = blocks(&layout, 0..1);
    let targets = || {
        server
            .seen()
            .into_iter()
            .map(|h| h.target)
            .collect::<Vec<_>>()
    };

    for (n, named_by) in [NamedBy::Torrent, NamedBy::Operator]
        .into_iter()
        .enumerate()
    {
        let clients = Wired::new(server.addr);
        let resolver = names(&[("seed.test", "8.8.8.8"), ("moved.test", "192.168.1.2")]);
        let (seeds, link) = seed_named(url.clone(), WebSeedKind::Url, named_by);
        let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
        assert!(drive(&conn, &Fake::default(), &requests, 1).await.is_err());
        assert_eq!(entry(&seeds, |e| e.lacks.files.clone()), HashSet::from([0]));
        assert!(!entry(&seeds, |e| e.disabled));
        assert!(entry(&seeds, |e| e.redirects.is_empty()));
        assert_eq!(targets(), vec!["/seed/f"; n + 1]);
        assert_eq!(clients.built(), [ip("8.8.8.8")]);
    }

    let clients = Wired::new(server.addr);
    let resolver = names(&[("seed.test", "8.8.8.8"), ("moved.test", "9.9.9.9")]);
    let (seeds, link) = one_seed(url, WebSeedKind::Url);
    let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
    drive(&conn, &Fake::default(), &requests, 1).await.unwrap();
    assert_eq!(
        entry(&seeds, |e| e.redirects.clone()),
        HashMap::from([(
            0,
            Redirect {
                url: format!("http://moved.test:{}/moved/f", server.addr.port()),
                named_by: NamedBy::Torrent,
            }
        )])
    );
    assert_eq!(targets(), ["/seed/f", "/seed/f", "/seed/f", "/moved/f"]);
    assert_eq!(clients.built(), [ip("8.8.8.8"), ip("9.9.9.9")]);
}

// A request goes to the address its host was checked at, and only
// there: the client looks no name up of its own, so a host it was not
// built for is reached nowhere; a redirect's host is checked and reached
// the same way. The connection's requests to one address share a client.
#[tokio::test(start_paused = true)]
async fn a_request_goes_to_the_address_its_host_was_checked_at() {
    let (a, b) = (content(40000, 7), content(10, 8));
    let (sa, sb) = (a.clone(), b.clone());
    let port = Arc::new(Mutex::new(0u16));
    let location = port.clone();
    let server = Server::start(move |head| match head.target.as_str() {
        "/seed/t/a" => Reply::ranged(&sa, head),
        "/seed/t/b" => Reply::status(307).header(
            "Location",
            format!("http://moved.test:{}/b", location.lock()),
        ),
        "/b" => Reply::ranged(&sb, head),
        _ => Reply::status(404),
    });
    *port.lock() = server.addr.port();
    let url = format!("http://seed.test:{}/seed/", server.addr.port());
    let layout = layout(&[("a", 40000, false), ("b", 10, false)]);
    let resolver = names(&[("seed.test", "127.0.0.1"), ("moved.test", "127.0.0.1")]);
    let (seeds, link) = one_seed(url.clone(), WebSeedKind::Url);
    let conn = WebSeedConnection::new(direct(), resolver, ALL, &layout, &link, UA);
    let fake = Fake::default();
    let requests = blocks(&layout, 0..2);
    drive(&conn, &fake, &requests, requests.len())
        .await
        .unwrap();

    let mut torrent = a;
    torrent.extend(b);
    assert_eq!(fake.bytes(&layout), torrent);
    let mut targets: Vec<_> = server.seen().into_iter().map(|h| h.target).collect();
    targets.sort();
    assert_eq!(targets, ["/b", "/seed/t/a", "/seed/t/b"]);
    assert_eq!(
        entry(&seeds, |e| e.redirects.clone()),
        HashMap::from([(
            1,
            Redirect {
                url: format!("http://moved.test:{}/b", server.addr.port()),
                named_by: NamedBy::Operator,
            }
        )])
    );
    let mut pinned: Vec<_> = conn.pinned.lock().keys().cloned().collect();
    pinned.sort();
    let at = ip("127.0.0.1");
    let port = server.addr.port();
    assert_eq!(
        pinned,
        [
            ("moved.test".into(), port, Some(at)),
            ("seed.test".into(), port, Some(at))
        ]
    );
    // The same factory's client for no target reaches no such host.
    let unpinned = direct().client(None, &ClientShape::FETCH).unwrap();
    assert!(unpinned.get(url).send().await.is_err());
}

// A request follows at most MAX_REDIRECTS redirects; one more fails the
// connection, and the seed and its file are kept for a later one.
#[tokio::test(start_paused = true)]
async fn a_request_follows_at_most_five_redirects() {
    let f = content(100, 9);
    for (served_at, ok) in [(MAX_REDIRECTS, true), (MAX_REDIRECTS + 1, false)] {
        let sf = f.clone();
        let server = Server::start(move |head| {
            let hop: usize = head.target.strip_prefix("/r").unwrap().parse().unwrap();
            if hop == served_at {
                Reply::ranged(&sf, head)
            } else {
                Reply::status(302).header("Location", format!("/r{}", hop + 1))
            }
        });
        let layout = layout(&[("f", 100, false)]);
        let (seeds, link) = one_seed(server.url("/r0"), WebSeedKind::Url);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        let fake = Fake::default();
        let fetched = drive(&conn, &fake, &blocks(&layout, 0..1), 1).await;
        assert_eq!(fetched.is_ok(), ok, "{fetched:?}");
        let hops: Vec<_> = (0..=MAX_REDIRECTS).map(|i| format!("/r{i}")).collect();
        let targets: Vec<_> = server.seen().into_iter().map(|h| h.target).collect();
        assert_eq!(targets, hops);
        assert!(!entry(&seeds, |e| e.disabled));
        assert!(entry(&seeds, |e| e.lacks.files.is_empty()));
        let moved = entry(&seeds, |e| e.redirects.get(&0).map(|r| r.url.clone()));
        assert_eq!(moved, ok.then(|| server.url(&format!("/r{MAX_REDIRECTS}"))));
    }
}

// The session's IP filter holds for the address a seed's name resolves
// to: a blocked address is not asked, and the seed is tried again later.
#[tokio::test(start_paused = true)]
async fn the_ip_filter_holds_for_the_address_a_name_resolves_to() {
    let f = content(100, 10);
    let server = Server::start(move |head| Reply::ranged(&f, head));
    let url = format!("http://seed.test:{}/f", server.addr.port());
    let layout = layout(&[("f", 100, false)]);
    let resolver = || names(&[("seed.test", "127.0.0.1")]);
    let (seeds, link) = one_seed(url, WebSeedKind::Url);

    let blocked = |a: SocketAddr| a.ip() != ip("127.0.0.1");
    let conn = WebSeedConnection::new(direct(), resolver(), &blocked, &layout, &link, UA);
    let failed = drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1).await;
    assert!(failed.is_err());
    assert!(server.seen().is_empty());
    assert!(!entry(&seeds, |e| e.disabled));
    assert!(entry(&seeds, |e| e.lacks.files.is_empty()));

    let conn = WebSeedConnection::new(direct(), resolver(), ALL, &layout, &link, UA);
    drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
        .await
        .unwrap();
    assert_eq!(server.seen().len(), 1);
}

// A seed whose public-looking name the network resolves at its exit is
// asked by name: its client is built for no address, and no address of
// it is checked here, so neither the IP filter nor the seed's naming has
// an address to hold, even for a seed that came with the torrent.
#[tokio::test(start_paused = true)]
async fn a_name_resolved_at_the_exit_is_asked_by_name() {
    struct AtExit;
    impl Resolver for AtExit {
        fn resolve<'a>(
            &'a self,
            _: &'a str,
            _: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<Resolved>> {
            Box::pin(async { Ok(Resolved::AtExit) })
        }
    }
    let f = content(100, 10);
    let server = Server::start(move |head| Reply::ranged(&f, head));
    let url = format!("http://seed.test:{}/f", server.addr.port());
    let layout = layout(&[("f", 100, false)]);
    let (_seeds, link) = seed_named(url, WebSeedKind::Url, NamedBy::Torrent);
    let clients = Wired::new(server.addr);
    let nothing = |_: SocketAddr| false;
    let conn = WebSeedConnection::new(
        clients.clone(),
        Arc::new(AtExit),
        &nothing,
        &layout,
        &link,
        UA,
    );
    drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
        .await
        .unwrap();
    assert_eq!(server.seen().len(), 1);
    assert!(clients.built().is_empty(), "{:?}", clients.built());
    let pinned: Vec<_> = conn.pinned.lock().keys().cloned().collect();
    assert_eq!(pinned, [("seed.test".to_owned(), server.addr.port(), None)]);
}

// Where the network resolves names at its exit, a seed that came with
// the torrent under a plainly local name is not asked, and is disabled;
// a public-looking name is asked, and so is a local name the operator
// named.
#[tokio::test(start_paused = true)]
async fn a_local_name_resolved_at_the_exit_is_asked_only_when_the_operator_named_it() {
    struct AtExit;
    impl Resolver for AtExit {
        fn resolve<'a>(
            &'a self,
            _: &'a str,
            _: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<Resolved>> {
            Box::pin(async { Ok(Resolved::AtExit) })
        }
    }
    async fn ask(host: &str, named_by: NamedBy) -> (crate::Result<()>, usize, bool) {
        let f = content(100, 10);
        let server = Server::start(move |head| Reply::ranged(&f, head));
        let url = format!("http://{host}:{}/f", server.addr.port());
        let layout = layout(&[("f", 100, false)]);
        let (seeds, link) = seed_named(url, WebSeedKind::Url, named_by);
        let conn = WebSeedConnection::new(
            Wired::new(server.addr),
            Arc::new(AtExit),
            ALL,
            &layout,
            &link,
            UA,
        );
        let fetched = drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1).await;
        (fetched, server.seen().len(), entry(&seeds, |e| e.disabled))
    }
    for host in ["nas.lan", "localhost", "NAS", "printer.local."] {
        let (fetched, seen, disabled) = ask(host, NamedBy::Torrent).await;
        assert!(fetched.is_err(), "{host}");
        assert_eq!(seen, 0, "{host}");
        assert!(disabled, "{host}");
    }
    for (host, named_by) in [
        ("tracker.example.org", NamedBy::Torrent),
        ("nas.lan", NamedBy::Operator),
    ] {
        let (fetched, seen, disabled) = ask(host, named_by).await;
        assert!(fetched.is_ok(), "{host}: {fetched:?}");
        assert_eq!(seen, 1, "{host}");
        assert!(!disabled, "{host}");
    }
}

// A body cut short fails the connection; the whole blocks it carried
// are delivered first.
#[tokio::test(start_paused = true)]
async fn a_truncated_body_fails_after_its_whole_blocks() {
    let a = content(PIECE as usize, 4);
    let server = Server::start(move |head| Reply {
        sent: Some(20000),
        ..Reply::ranged(&a, head)
    });
    let layout = layout(&[("a", u64::from(PIECE), false)]);
    let (seeds, link) = one_seed(server.url("/a"), WebSeedKind::Url);
    let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
    let fake = Fake::default();

    let failed = drive(&conn, &fake, &blocks(&layout, 0..1), 2).await;
    assert!(failed.is_err(), "{failed:?}");
    assert_eq!(fake.pieces.lock().len(), 1);
    assert!(entry(&seeds, |e| e.lacks.files.is_empty() && !e.disabled));
}

// A web seed's URL can carry a credential in its user, its path or its
// query. The error a failed connection ends with, which the torrent logs
// and keeps as the peer's error, names the seed by scheme, host and port
// only, whatever failed: an error status, a body cut short, a refused
// connection, a move with no location, the SSRF check (SECURITY.md,
// "Credentials stay with their scope").
#[tokio::test(start_paused = true)]
async fn a_failed_web_seed_names_no_credential() {
    const SECRET: &str = "s3cret";
    let a = content(PIECE as usize, 7);
    let layout = layout(&[("a", u64::from(PIECE), false)]);
    // A port nothing listens on.
    let refused = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap();
    let replies: Vec<Reply> = vec![
        Reply::status(404),
        Reply {
            sent: Some(20000),
            ..Reply::status(206)
                .header(
                    "Content-Range",
                    format!("bytes 0-{}/{}", a.len() - 1, a.len()),
                )
                .body(a.clone())
        },
        Reply::status(302),
        Reply::status(200),
        Reply::status(200),
    ];
    for (case, reply) in replies.into_iter().enumerate() {
        let reply = Mutex::new(Some(reply));
        // Holds the paused clock while the request is out (see Server).
        let server = Server::start(move |_| reply.lock().take().unwrap());
        let at = match case {
            3 => refused,
            _ => server.addr,
        };
        let url = match case {
            // A query to a local address: refused before it is asked.
            4 => format!("http://user:{SECRET}@{at}/{SECRET}/a?pk={SECRET}"),
            _ => format!("http://user:{SECRET}@{at}/{SECRET}/a"),
        };
        let (_seeds, link) = one_seed(url, WebSeedKind::Url);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        let Err(e) = drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 2).await else {
            panic!("case {case}: the connection did not fail");
        };
        let shown = format!("{e:#} {e:?}");
        assert!(!shown.contains(SECRET), "case {case}: {shown}");
        assert!(!shown.contains("user"), "case {case}: {shown}");
        // A request that got no answer is named by its error alone.
        if case != 3 {
            assert!(
                shown.contains(&format!("http://{at}/…")),
                "case {case}: {shown}"
            );
        }
    }
}

// A response must answer the range asked for: one naming none disables
// the seed, one naming another fails the connection (libtorrent
// `get_range`, `web_peer_connection::on_receive`).
#[tokio::test(start_paused = true)]
async fn a_response_must_answer_the_range_asked_for() {
    let a = content(PIECE as usize, 5);
    let layout = layout(&[("a", u64::from(PIECE), false)]);
    for (reply, disabled) in [
        (
            Reply {
                content_length: false,
                ..Reply::status(200).body(a.clone())
            },
            true,
        ),
        (
            Reply::status(206).header("Content-Range", "bytes x-y/z"),
            true,
        ),
        (
            Reply::status(206).header("Content-Range", "bytes 1-32768/32769"),
            false,
        ),
        (Reply::status(200).body(&a[1..]), false),
    ] {
        let reply = Mutex::new(Some(reply));
        let server = Server::start(move |_| reply.lock().take().unwrap());
        let (seeds, link) = one_seed(server.url("/a"), WebSeedKind::Url);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        let fake = Fake::default();
        assert!(
            drive(&conn, &fake, &blocks(&layout, 0..1), 1)
                .await
                .is_err()
        );
        assert_eq!(entry(&seeds, |e| e.disabled), disabled);
        assert!(fake.pieces.lock().is_empty());
    }
    // A whole file answered with 200 is the range from its start.
    let sa = a.clone();
    let server = Server::start(move |_| Reply::status(200).body(sa.clone()));
    let (_seeds, link) = one_seed(server.url("/a"), WebSeedKind::Url);
    let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
    let fake = Fake::default();
    drive(&conn, &fake, &blocks(&layout, 0..1), 2)
        .await
        .unwrap();
    assert_eq!(fake.bytes(&layout), a);
}

// BEP 17: a seed is asked by piece, with the byte ranges of a part of
// one; a 503's body says how long to wait, and any other error removes
// the seed.
#[tokio::test(start_paused = true)]
async fn an_http_seed_is_asked_by_piece() {
    let f = content(40000, 6);
    let sf = f.clone();
    let hash = "%07".repeat(20);
    let server = Server::start(move |head| {
        let query = head.target.strip_prefix("/s.php?").unwrap();
        let param = |k: &str| {
            query
                .split('&')
                .find_map(|kv| kv.strip_prefix(k)?.strip_prefix('='))
                .map(str::to_owned)
        };
        assert_eq!(param("info_hash").unwrap(), hash);
        let piece: usize = param("piece").unwrap().parse().unwrap();
        let start = piece * PIECE as usize;
        let piece_bytes = &sf[start..sf.len().min(start + PIECE as usize)];
        match param("ranges") {
            Some(r) => {
                let (a, b) = r.split_once('-').unwrap();
                let (a, b): (usize, usize) = (a.parse().unwrap(), b.parse().unwrap());
                Reply::status(200).body(&piece_bytes[a..=b])
            }
            None => Reply::status(200).body(piece_bytes),
        }
    });
    let layout = layout(&[("f", 40000, false)]);
    let (seeds, link) = one_seed(server.url("/s.php"), WebSeedKind::Http);
    let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);

    let fake = Fake::default();
    let requests = blocks(&layout, 0..2);
    drive(&conn, &fake, &requests, requests.len())
        .await
        .unwrap();
    assert_eq!(fake.bytes(&layout), f);
    let fake = Fake::default();
    drive(&conn, &fake, &requests[1..2], 1).await.unwrap();
    assert_eq!(fake.pieces.lock()[0].2, f[16384..32768]);
    let mut targets: Vec<_> = server.seen().into_iter().map(|h| h.target).collect();
    targets[..2].sort();
    let hash = "%07".repeat(20);
    assert_eq!(
        targets,
        [
            format!("/s.php?info_hash={hash}&piece=0"),
            format!("/s.php?info_hash={hash}&piece=1"),
            format!("/s.php?info_hash={hash}&piece=0&ranges=16384-32767"),
        ]
    );
    drop(link);
    assert_eq!(seeds.list().len(), 1);

    for (body, wait) in [
        ("90".to_owned(), 90),
        ("0".to_owned(), 60),
        ("soon".to_owned(), 60),
        // A wait past what a seconds32 holds is held to it.
        ("2147483648".to_owned(), MAX_RETRY_WAIT_SECS),
        ("99999999999999999999999".to_owned(), MAX_RETRY_WAIT_SECS),
        // The body is read to MAX_UNAVAILABLE_BODY and no further.
        (" ".repeat(MAX_UNAVAILABLE_BODY - 2) + "90", 90),
        (" ".repeat(MAX_UNAVAILABLE_BODY - 1) + "90", 60),
    ] {
        let server = Server::start(move |_| Reply::status(503).body(body.clone()));
        let (seeds, link) = one_seed(server.url("/s.php"), WebSeedKind::Http);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        assert!(
            drive(&conn, &Fake::default(), &requests[..1], 1)
                .await
                .is_err()
        );
        assert_eq!(
            entry(&seeds, |e| e.retry),
            Some(Instant::now() + Duration::from_secs(wait))
        );
    }
    // A seed whose URL has a query is asked at a global address.
    let server = Server::start(|_| Reply::status(404));
    let url = format!("http://seed.test:{}/s.php?k=v", server.addr.port());
    let (seeds, link) = one_seed(url, WebSeedKind::Http);
    let conn = WebSeedConnection::new(
        Wired::new(server.addr),
        names(&[("seed.test", "8.8.8.8")]),
        ALL,
        &layout,
        &link,
        UA,
    );
    assert!(
        drive(&conn, &Fake::default(), &requests[..1], 1)
            .await
            .is_err()
    );
    assert!(seeds.list().is_empty());
    assert!(server.seen()[0].target.starts_with("/s.php?k=v&info_hash="));
}

// Requests that follow each other in the torrent go in one HTTP
// request, up to URLSEED_MAX_REQUEST_BYTES, within one piece for a BEP
// 17 seed.
#[test]
fn following_requests_are_fetched_together() {
    let piece = 4 * 1024 * 1024;
    let layout = Layout {
        info_hash: Id20::new([0; 20]),
        lengths: Lengths::new(10 * u64::from(piece), piece).unwrap(),
        files: vec![LayoutFile {
            offset: 0,
            len: 10 * u64::from(piece),
            pad: false,
            path: "f".into(),
        }],
    };
    let runs = |kind, requests: Vec<Request>| {
        let seeds = Arc::new(WebSeeds::new([WebSeed {
            url: "http://h/".into(),
            kind,
            named_by: NamedBy::Torrent,
        }]));
        let link = seeds.link(0).unwrap();
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        let mut queue = VecDeque::from(requests);
        std::iter::from_fn(|| conn.take_run(&mut queue))
            .map(|r| (r.offset, r.len))
            .collect::<Vec<_>>()
    };
    let mib = 1024 * 1024;
    let all = blocks(&layout, 0..5);
    assert_eq!(
        runs(WebSeedKind::Url, all.clone()),
        [(0, 16 * mib), (16 * mib, 4 * mib)]
    );
    let mut gap = all.clone();
    gap.remove(3);
    assert_eq!(
        runs(WebSeedKind::Url, gap),
        [
            (0, 3 * 16384),
            (4 * 16384, 16 * mib),
            (16 * mib + 4 * 16384, 4 * mib - 4 * 16384)
        ]
    );
    assert_eq!(
        runs(WebSeedKind::Http, all)
            .into_iter()
            .map(|r| r.1)
            .collect::<Vec<_>>(),
        [4 * mib; 5]
    );
}

// A file's URL: where it moved, the seed's own URL in a torrent of one
// file unless it ends in a slash, its escaped path under the seed's
// URL otherwise (libtorrent `web_peer_connection::web_peer_connection`,
// `write_request`).
#[test]
fn a_files_url() {
    let url_of = |seed: &str, files: &[(&str, u64, bool)], file| {
        let layout = layout(files);
        let seeds = Arc::new(WebSeeds::new([WebSeed {
            url: seed.into(),
            kind: WebSeedKind::Url,
            named_by: NamedBy::Torrent,
        }]));
        let link = seeds.link(0).unwrap();
        WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA)
            .file_target(file)
            .unwrap()
            .url
            .to_string()
    };
    let one = [("f é.iso", 1, false)];
    assert_eq!(url_of("http://h/d/", &one, 0), "http://h/d/f%20%c3%a9.iso");
    assert_eq!(url_of("http://h/d.iso", &one, 0), "http://h/d.iso");
    let two = [("a", 1, false), ("sub/b c", 1, false)];
    assert_eq!(url_of("http://h/d", &two, 1), "http://h/d/t/sub/b%20c");
    assert_eq!(url_of("http://h/d/", &two, 0), "http://h/d/t/a");
}

// The connect policy's view of the seeds (libtorrent
// `torrent::maybe_connect_web_seeds`, `connect_to_url_seed`,
// `connect_web_seed`).
// A seed that sent the whole of a bad piece lying in one file holds
// another copy of that file: it lacks the file from then on, a pad file
// counting for none. One spanning several files it lacks alone. What it
// lacks is kept for its next connections, and a seed left nothing to
// serve is not due (libtorrent
// `web_peer_connection::received_invalid_data`, `on_connected`).
#[test]
fn a_bad_piece_loses_the_seed_its_file_or_itself() {
    let bits = |have: BF, n: usize| have[..n].iter().by_vals().collect::<Vec<_>>();
    let url = || WebSeed {
        url: "http://h/".into(),
        kind: WebSeedKind::Url,
        named_by: NamedBy::Torrent,
    };

    // "a" fills piece 0 and starts piece 1, which a pad file ends.
    let padded = layout(&[
        ("a", 40000, false),
        ("pad", 25536, true),
        ("b", 30000, false),
    ]);
    let piece = |i| padded.lengths.validate_piece_index(i).unwrap();
    let seeds = WebSeeds::new([url()]);
    let have = seeds
        .piece_failed(PeerHandle::WebSeed(0), piece(1), &padded)
        .unwrap();
    assert_eq!(bits(have, 3), [false, false, true]);
    assert_eq!(entry(&seeds, |e| e.lacks.clone()), lacking(&[0], &[]));
    assert_eq!(has(&padded, &lacking(&[0], &[])), [false, false, true]);
    assert_eq!(
        seeds
            .due(Instant::now(), |l| padded.bitfield(l).any(), false)
            .len(),
        1
    );
    // A handle no seed has.
    assert!(
        seeds
            .piece_failed(PeerHandle::WebSeed(1), piece(2), &padded)
            .is_none()
    );

    // Piece 0 spans "x" and "y", piece 1 "y" and "z"; piece 2 is in "z".
    let spans = layout(&[
        ("x", 20000, false),
        ("y", 20000, false),
        ("z", 30000, false),
    ]);
    let piece = |i| spans.lengths.validate_piece_index(i).unwrap();
    let seeds = WebSeeds::new([url()]);
    let have = seeds
        .piece_failed(PeerHandle::WebSeed(0), piece(0), &spans)
        .unwrap();
    assert_eq!(bits(have, 3), [false, true, true]);
    assert_eq!(entry(&seeds, |e| e.lacks.clone()), lacking(&[], &[0]));
    let have = seeds
        .piece_failed(PeerHandle::WebSeed(0), piece(2), &spans)
        .unwrap();
    assert_eq!(bits(have, 3), [false, false, false]);
    assert_eq!(entry(&seeds, |e| e.lacks.clone()), lacking(&[2], &[0]));
    assert!(
        seeds
            .due(Instant::now(), |l| spans.bitfield(l).any(), false)
            .is_empty()
    );

    // The only file of a torrent of one.
    let single = layout(&[("f", 70000, false)]);
    let seeds = WebSeeds::new([url()]);
    let piece = single.lengths.validate_piece_index(1).unwrap();
    let have = seeds
        .piece_failed(PeerHandle::WebSeed(0), piece, &single)
        .unwrap();
    assert!(!have.any());
}

// A seed banned once its connection was opening, before the connection
// runs, tells the torrent nothing: no handshake, no bitfield, no unchoke.
#[tokio::test]
async fn a_connection_to_a_seed_banned_before_it_runs_tells_the_torrent_nothing() {
    let layout = layout(&[("f", 70000, false)]);
    let (seeds, link) = one_seed("http://192.0.2.1/f".into(), WebSeedKind::Url);
    assert!(seeds.ban(link.handle()));
    let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
    let fake = Fake::default();
    let (_tx, rx) = unbounded_channel();
    conn.run(&fake, rx).await.unwrap();
    assert!(!fake.handshaken.load(Ordering::SeqCst));
    assert!(fake.bitfield.lock().is_none());
    assert!(!fake.unchoked.load(Ordering::SeqCst));
    assert!(fake.pieces.lock().is_empty());
}

// The web seed tick finds a seed due, then opens its connection. A seed
// banned between the two is not connected to; one banned once its
// connection is opening has that connection ended, and is due no more.
#[test]
fn a_seed_banned_after_it_was_found_due_is_not_connected_to() {
    let now = Instant::now();
    let any = |_: &Lacks| true;
    let seeds = || {
        Arc::new(WebSeeds::new([WebSeed {
            url: "http://192.0.2.1/f".into(),
            kind: WebSeedKind::Url,
            named_by: NamedBy::Torrent,
        }]))
    };

    let banned_before_link = seeds();
    let due = banned_before_link.due(now, any, false);
    assert_eq!(due.len(), 1);
    assert!(banned_before_link.ban(PeerHandle::WebSeed(due[0].id)));
    assert!(banned_before_link.link(due[0].id).is_none());

    let banned_after_link = seeds();
    let due = banned_after_link.due(now, any, false);
    let link = banned_after_link.link(due[0].id).unwrap();
    assert!(!link.cancel.is_cancelled());
    assert!(banned_after_link.ban(link.handle()));
    assert!(link.cancel.is_cancelled());
    drop(link);
    assert!(banned_after_link.due(now, any, false).is_empty());
    assert!(banned_after_link.link(due[0].id).is_none());
}

#[test]
fn which_seeds_are_due() {
    let now = Instant::now();
    let any = |_: &Lacks| true;
    let seed = |url: &str| WebSeed {
        url: url.into(),
        kind: WebSeedKind::Url,
        named_by: NamedBy::Torrent,
    };

    // At most MAX_WEB_SEED_CONNECTIONS, a connected seed among them.
    let seeds = Arc::new(WebSeeds::new(
        (0..5).map(|i| seed(&format!("http://h{i}.example/"))),
    ));
    let due = seeds.due(now, any, false);
    assert_eq!(due.len(), MAX_WEB_SEED_CONNECTIONS);
    assert!(due.iter().all(|d| d.endpoint.is_none()));
    let _link = seeds.link(due[0].id).unwrap();
    assert!(seeds.link(due[0].id).is_none());
    assert_eq!(
        seeds.due(now, any, false).len(),
        MAX_WEB_SEED_CONNECTIONS - 1
    );
    // A seed with no piece to give is not counted.
    assert_eq!(seeds.due(now, |_| false, false).len(), 0);

    // (url, banned, listed afterwards, disabled afterwards, endpoint)
    let cases = [
        (
            "http://127.0.0.1:8080/f",
            false,
            true,
            false,
            Some("127.0.0.1:8080"),
        ),
        ("https://[::1]/f", false, true, false, Some("[::1]:443")),
        // A query to a local address is refused when the seed is
        // asked, by the address its host resolves to.
        (
            "http://127.0.0.1/f?x=1",
            false,
            true,
            false,
            Some("127.0.0.1:80"),
        ),
        (
            "http://1.2.3.4/f?x=1",
            false,
            true,
            false,
            Some("1.2.3.4:80"),
        ),
        ("http://xn--bcher-kva.example/f", false, true, true, None),
        ("http://bücher.example/f", false, true, true, None),
        ("ftp://h.example/f", false, true, true, None),
        ("http://h.example/f", true, false, true, None),
        ("http://h.example:0/f", false, false, false, None),
        ("not a url", false, false, false, None),
    ];
    for (url, banned, listed, disabled, endpoint) in cases {
        let seeds = WebSeeds::new([seed(url)]);
        if banned {
            assert!(seeds.ban(PeerHandle::WebSeed(0)));
        }
        let due = seeds.due(now, any, false);
        assert_eq!(seeds.list().len(), usize::from(listed), "{url}");
        let present = seeds.with_entry(0, |e| e.disabled);
        assert_eq!(present.unwrap_or(false), disabled, "{url}");
        let endpoint = endpoint.map(|e| e.parse::<SocketAddr>().unwrap());
        assert_eq!(due.first().and_then(|d| d.endpoint), endpoint, "{url}");
        assert_eq!(due.len(), usize::from(listed && !disabled), "{url}");
    }

    // A session that allows internationalized names connects to them.
    for url in [
        "http://xn--bcher-kva.example/f",
        "http://b\u{fc}cher.example/f",
    ] {
        let seeds = WebSeeds::new([seed(url)]);
        assert_eq!(seeds.due(now, any, true).len(), 1, "{url}");
        assert_eq!(seeds.with_entry(0, |e| e.disabled), Some(false), "{url}");
    }

    // A seed's log names its scheme, host and port, never the passkey
    // its URL carries.
    let lines = crate::tests::test_util::LogLines::default();
    {
        let _capture = lines.capture();
        let disabled = WebSeeds::new([seed("ftp://h.example/0123passkey/f?pk=s3cret")]);
        disabled.due(now, any, false);
        let removed = WebSeeds::new([seed("http://h.example:0/0123passkey/f?pk=s3cret")]);
        removed.due(now, any, false);
        let bad = WebSeeds::new([seed("https://u:pw@h.example:8443/0123passkey/?pk=s3cret")]);
        let single = layout(&[("f", 70000, false)]);
        let piece = single.lengths.validate_piece_index(0).unwrap();
        bad.piece_failed(PeerHandle::WebSeed(0), piece, &single)
            .unwrap();
    }
    let text = lines.text();
    for (url, message) in [
        ("ftp://h.example:21/…", "web seed disabled"),
        ("http://h.example:0/…", "web seed removed"),
        ("https://h.example:8443/…", "web seed sent a bad piece"),
    ] {
        assert!(
            text.lines()
                .any(|line| line.contains(url) && line.contains(message)),
            "{url}\n{text}"
        );
    }
    assert!(!text.contains("s3cret"), "{text}");
    assert!(!text.contains("0123passkey"), "{text}");
    assert!(!text.contains("pw@"), "{text}");

    // A seed is listed once; removing it ends its connection.
    let seeds = Arc::new(WebSeeds::new([seed("http://h/"), seed("http://h/")]));
    assert_eq!(seeds.list(), [seed("http://h/")]);
    assert!(!seeds.add(seed("http://h/")));
    let link = seeds.link(0).unwrap();
    assert!(seeds.remove(&seed("http://h/")));
    assert!(link.cancel.is_cancelled());
    assert!(seeds.list().is_empty());
}

// A torrent whose only source is a web seed downloads from it, the
// seed's bytes counted as any peer's; the seed is listed, added and
// removed through the torrent, and a magnet link names it as `ws`.
#[tokio::test(start_paused = true)]
async fn a_torrent_downloads_from_its_web_seed_alone() -> anyhow::Result<()> {
    use crate::{
        AddTorrent, AddTorrentOptions, Session,
        storage::StorageFactory,
        tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
    };

    let t = TestTorrent::new("test_web_seed", 3 * 16384 + 100, 16384).await?;
    let content = t.content.clone();
    let server = Server::start(move |head| {
        if head.target.ends_with("/0.data") {
            Reply::ranged(&content, head)
        } else {
            Reply::status(404)
        }
    });
    let seed = WebSeed {
        url: server.url("/seed/"),
        kind: WebSeedKind::Url,
        named_by: NamedBy::Operator,
    };
    let out = tempfile::TempDir::with_prefix("test_web_seed_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        crate::SessionOptions {
            http_client: Some(crate::test_support::loopback()),
            ..isolated_session_opts(false)
        },
    )
    .await?;
    let storage = MemoryStorageFactory::default();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                web_seeds: vec![seed.clone()],
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_completed().await?;

    assert!(storage.log.file_contents(0) == t.content, "content differs");
    let total = t.content.len() as u64;
    let live = handle.stats().live.context("expected a live torrent")?;
    assert_eq!(live.snapshot.fetched_bytes, total);
    assert_eq!(
        session
            .stats
            .counters
            .fetched_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        total
    );
    assert!(!server.seen().is_empty());
    assert!(server.seen().iter().all(|h| h.target.ends_with("/0.data")));

    assert_eq!(handle.web_seeds(), std::slice::from_ref(&seed));
    assert!(!handle.add_web_seed(seed.clone()));
    let http_seed = WebSeed {
        url: "http://h.example/s.php".into(),
        kind: WebSeedKind::Http,
        named_by: NamedBy::Operator,
    };
    assert!(handle.add_web_seed(http_seed.clone()));
    assert_eq!(handle.magnet().web_seeds, std::slice::from_ref(&seed.url));
    assert!(handle.remove_web_seed(&seed));
    assert!(!handle.remove_web_seed(&seed));
    assert_eq!(handle.web_seeds(), [http_seed]);
    assert!(handle.magnet().web_seeds.is_empty());
    Ok(())
}

// A magnet link's `ws` web seed, its peer and the session that fetches
// its metadata. The peer has the metainfo and none of the content: once
// the metadata is in, the web seed is the content's only source. The seed
// is named by the link, so its host is a public name (`seed.test`, at
// 8.8.8.8) that the session's clients reach the server at.
struct MagnetWithSeed {
    t: crate::tests::embedder_util::TestTorrent,
    server: Server,
    seed: WebSeed,
    magnet: String,
    _peer: Arc<crate::Session>,
    _peer_dir: tempfile::TempDir,
    out: tempfile::TempDir,
    session: Arc<crate::Session>,
}

impl MagnetWithSeed {
    async fn new(name: &str) -> anyhow::Result<Self> {
        use crate::{
            AddTorrent, AddTorrentOptions, Session,
            core::{magnet::Magnet, torrent_metainfo::torrent_from_bytes},
            tests::embedder_util::{TestTorrent, isolated_session_opts},
        };

        let t = TestTorrent::new(name, 3 * 16384 + 100, 16384).await?;
        let content = t.content.clone();
        let server = Server::start(move |head| {
            if head.target.ends_with("/0.data") {
                Reply::ranged(&content, head)
            } else {
                Reply::status(404)
            }
        });
        let peer_dir = tempfile::TempDir::with_prefix("magnet_ws_peer")?;
        let peer =
            Session::new_with_opts(peer_dir.path().into(), isolated_session_opts(true)).await?;
        peer.add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(peer_dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
        let peer_addr = peer.listen_addr().context("the peer is not listening")?;

        let seed = WebSeed {
            url: format!("http://seed.test:{}/seed/", server.addr.port()),
            kind: WebSeedKind::Url,
            named_by: NamedBy::Torrent,
        };
        let mut magnet = Magnet::of(
            torrent_from_bytes(&t.torrent)?.info_hashes,
            None,
            Vec::new(),
        );
        magnet.peers.push(peer_addr);
        magnet.web_seeds.push(seed.url.clone());
        let magnet = magnet.to_string();
        assert!(magnet.contains("&ws="), "{magnet}");

        let out = tempfile::TempDir::with_prefix("magnet_ws_out")?;
        let session = Session::new_with_opts(
            out.path().into(),
            crate::SessionOptions {
                http_client: Some(Wired::new(server.addr)),
                resolver: Some(names(&[("seed.test", "8.8.8.8")])),
                ..isolated_session_opts(false)
            },
        )
        .await?;
        Ok(Self {
            t,
            server,
            seed,
            magnet,
            _peer: peer,
            _peer_dir: peer_dir,
            out,
            session,
        })
    }

    // Adds the torrent with `add` and `opts` and waits for it to complete;
    // checks that the web seed alone served the content.
    async fn downloads(
        &self,
        add: crate::AddTorrent<'static>,
        opts: crate::AddTorrentOptions,
    ) -> anyhow::Result<crate::torrent_state::ManagedTorrentHandle> {
        use crate::{
            AddTorrentOptions, storage::StorageFactory, tests::embedder_util::MemoryStorageFactory,
        };
        let storage = MemoryStorageFactory::default();
        let handle = self
            .session
            .add_torrent(
                add,
                Some(AddTorrentOptions {
                    storage_factory: Some(storage.clone_box()),
                    output_folder: Some(self.out.path().to_str().unwrap().to_owned()),
                    ..opts
                }),
            )
            .await?
            .into_handle()
            .context("expected a handle")?;
        handle.wait_until_completed().await?;
        assert!(
            storage.log.file_contents(0) == self.t.content,
            "content differs"
        );
        let total = self.t.content.len() as u64;
        let live = handle.stats().live.context("expected a live torrent")?;
        assert_eq!(live.snapshot.fetched_bytes, total);
        let seen = self.server.seen();
        assert!(!seen.is_empty());
        assert!(
            seen.iter()
                .all(|h| h.target.starts_with("/seed/") && h.target.ends_with("/0.data")),
            "{seen:?}"
        );
        Ok(handle)
    }
}

// A magnet link's `ws` seed is the torrent's once its metadata is in, and
// is asked for the content (libtorrent `torrent::torrent` puts
// `add_torrent_params::url_seeds` in `m_web_seeds`, which
// `torrent::set_metadata` keeps).
#[tokio::test(flavor = "multi_thread")]
async fn a_magnets_web_seed_serves_the_torrent_once_its_metadata_is_in() -> anyhow::Result<()> {
    let m = MagnetWithSeed::new("magnet_ws_run").await?;
    let handle = m
        .downloads(
            crate::AddTorrent::from_url(m.magnet.clone()),
            Default::default(),
        )
        .await?;
    assert_eq!(handle.web_seeds(), std::slice::from_ref(&m.seed));
    m.session.stop().await;
    Ok(())
}

// A list-only add of a magnet names the link's `ws` seeds beside the
// metainfo, which does not hold them; the torrent added from that
// metainfo and those seeds asks the seed for its content.
#[tokio::test(flavor = "multi_thread")]
async fn a_list_only_magnet_names_its_web_seeds() -> anyhow::Result<()> {
    use crate::{AddTorrent, AddTorrentOptions, AddTorrentResponse};

    let m = MagnetWithSeed::new("magnet_ws_list").await?;
    let listed = match m
        .session
        .add_torrent(
            AddTorrent::from_url(m.magnet.clone()),
            Some(AddTorrentOptions {
                list_only: true,
                ..Default::default()
            }),
        )
        .await?
    {
        AddTorrentResponse::ListOnly(listed) => listed,
        _ => anyhow::bail!("expected a list-only response"),
    };
    assert_eq!(listed.web_seeds, std::slice::from_ref(&m.seed));
    let meta = crate::core::torrent_metainfo::torrent_from_bytes(&listed.torrent_bytes)?;
    assert!(meta.url_list.is_none(), "the metainfo is not rewritten");
    assert!(m.server.seen().is_empty());

    let handle = m
        .downloads(
            AddTorrent::from_bytes(listed.torrent_bytes),
            AddTorrentOptions {
                web_seeds: listed.web_seeds,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(handle.web_seeds(), std::slice::from_ref(&m.seed));
    m.session.stop().await;
    Ok(())
}

// A seed added or removed is found by its URL and kind in one lookup: the
// seeds kept, in their order, and what each add and remove returns, are
// what passing over the list says, over seeded adds and removes.
#[test]
fn seeds_found_by_lookup_are_those_a_pass_finds() {
    let mut rng = 0x6a09_e667_f3bc_c908u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for _ in 0..50 {
        let seeds = WebSeeds::default();
        let mut reference: Vec<WebSeed> = Vec::new();
        for _ in 0..200 {
            let r = next();
            let seed = WebSeed {
                url: format!("http://seed{}.test/", r % 12),
                kind: if r & 0x100 != 0 {
                    WebSeedKind::Url
                } else {
                    WebSeedKind::Http
                },
                named_by: if r & 0x200 != 0 {
                    NamedBy::Torrent
                } else {
                    NamedBy::Operator
                },
            };
            if r & 0x400 != 0 {
                let was = reference.iter().position(|s| same_seed(s, &seed));
                if let Some(at) = was {
                    reference.remove(at);
                }
                assert_eq!(seeds.remove(&seed), was.is_some());
            } else {
                let had = reference.iter().any(|s| same_seed(s, &seed));
                if !had {
                    reference.push(seed.clone());
                }
                assert_eq!(seeds.add(seed), !had);
            }
            assert_eq!(seeds.list(), reference);
        }
    }
}

// The slices found from a binary search for the first file are those a
// pass over every file finds, over seeded layouts with empty and pad files
// and ranges anywhere in the torrent.
#[test]
fn slices_found_by_search_are_those_a_pass_finds() {
    // The pass over every file the search replaced.
    fn every_file(layout: &Layout, offset: u64, len: u64) -> Vec<Slice> {
        let end = offset + len;
        layout
            .files
            .iter()
            .enumerate()
            .filter(|(_, f)| f.len > 0 && f.offset < end && f.offset + f.len > offset)
            .map(|(file, f)| {
                let start = offset.max(f.offset);
                let stop = end.min(f.offset + f.len);
                Slice {
                    file,
                    offset: start - f.offset,
                    len: stop - start,
                }
            })
            .collect()
    }
    let mut rng = 0xbb67_ae85_84ca_a73bu64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for _ in 0..300 {
        let names: Vec<String> = (0..1 + next() % 20).map(|i| format!("f{i}")).collect();
        let files: Vec<(&str, u64, bool)> = names
            .iter()
            .map(|name| {
                let r = next();
                let len = match r % 4 {
                    0 => 0,
                    1 => 1 + (r >> 8) % 100,
                    _ => 1 + (r >> 8) % 100_000,
                };
                (name.as_str(), len, r & 0x80 != 0)
            })
            .collect();
        if files.iter().all(|f| f.1 == 0) {
            continue;
        }
        let layout = layout(&files);
        let total: u64 = files.iter().map(|f| f.1).sum();
        for _ in 0..50 {
            let offset = next() % total;
            let len = 1 + next() % (total - offset);
            assert_eq!(
                layout.slices(offset, len),
                every_file(&layout, offset, len),
                "{files:?} {offset}+{len}"
            );
        }
    }
}
