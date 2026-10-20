// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A loopback stand-in for the test torrent's source: one HTTP server that
// lists and serves a torrent and is its tracker, a session seeding the
// torrent, and a scripted peer that wants its first piece. The tracker
// answers what the test sets and keeps every announce it was sent.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, Mutex};

use librqbit::spawn_utils::BlockingSpawner;
use librqbit::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions, Session,
    SessionOptions, create_torrent,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use super::TestTorrentSource;

// The name the listing gives the torrent.
pub const LISTED: &str = "debian-13.1.0-amd64-netinst.iso.torrent";
pub const PIECE_LEN: u32 = 16 * 1024;
// Three pieces, the last one short.
const CONTENT_LEN: usize = 2 * PIECE_LEN as usize + 7000;

// One announce the tracker was sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Announce {
    pub event: Option<String>,
    pub port: u16,
}

// What the tracker answers.
#[derive(Clone, Debug, Default)]
pub struct Answer {
    pub peers: Vec<SocketAddr>,
    pub seeders: u32,
    pub leechers: u32,
    // Answer this failure reason instead.
    pub failure: Option<String>,
    // Before answering a `started` announce, connect to the announcing
    // peer and trade handshakes with it.
    pub connect_in: bool,
}

struct Shared {
    listing: Vec<u8>,
    torrent: Mutex<Vec<u8>>,
    info_hash: Mutex<[u8; 20]>,
    answer: Mutex<Answer>,
    announces: watch::Sender<Vec<Announce>>,
}

pub struct TorrentFixture {
    pub source: TestTorrentSource,
    shared: Arc<Shared>,
    seeder: Arc<Session>,
    leecher: SocketAddr,
    taken: watch::Receiver<u64>,
    _dir: tempfile::TempDir,
}

impl TorrentFixture {
    // A listing that names the torrent, or, with `listed` false, one that
    // names no netinst torrent.
    pub async fn start(listed: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let name = if listed {
            LISTED
        } else {
            "debian-13.1.0-amd64-DVD-1.iso.torrent"
        };
        let shared = Arc::new(Shared {
            listing: format!("<html><a href=\"../\">..</a><a href=\"{name}\">{name}</a></html>")
                .into_bytes(),
            torrent: Mutex::new(Vec::new()),
            info_hash: Mutex::new([0; 20]),
            answer: Mutex::new(Answer::default()),
            announces: watch::channel(Vec::new()).0,
        });
        let serving = shared.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let shared = serving.clone();
                tokio::spawn(async move {
                    let _ = serve(stream, &shared).await;
                });
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let content = dir.path().join("content");
        std::fs::create_dir(&content).unwrap();
        let file = content.join("netinst.iso");
        std::fs::write(
            &file,
            (0..CONTENT_LEN)
                .map(|i| (i * 7 % 251) as u8)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let created = create_torrent(
            &file,
            CreateTorrentOptions {
                name: None,
                trackers: vec![format!("http://{addr}/announce")],
                piece_length: Some(PIECE_LEN),
            },
            &BlockingSpawner::new(1),
        )
        .await
        .unwrap();
        let bytes = created.as_bytes().unwrap();
        *shared.info_hash.lock().unwrap() = created.info_hash().0;
        *shared.torrent.lock().unwrap() = bytes.to_vec();

        let seeder = session(&dir.path().join("seeder")).await;
        let seeding = seeder
            .add_torrent(
                AddTorrent::from_bytes(bytes.clone()),
                Some(AddTorrentOptions {
                    output_folder: Some(content.display().to_string()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        seeding.wait_until_completed().await.unwrap();
        let (leecher, taken) = leech(created.info_hash().0).await;
        Self {
            source: TestTorrentSource {
                directory: format!("http://{addr}/bt-cd/").parse().unwrap(),
            },
            shared,
            seeder,
            leecher,
            taken,
            _dir: dir,
        }
    }

    pub fn seeder(&self) -> SocketAddr {
        self.seeder.listen_addr().unwrap()
    }

    // A peer that asks whoever dials it for the first piece.
    pub fn leecher(&self) -> SocketAddr {
        self.leecher
    }

    // Block bytes the leecher was sent.
    pub fn taken(&self) -> watch::Receiver<u64> {
        self.taken.clone()
    }

    pub fn answer(&self, answer: Answer) {
        *self.shared.answer.lock().unwrap() = answer;
    }

    pub fn announces(&self) -> watch::Receiver<Vec<Announce>> {
        self.shared.announces.subscribe()
    }

    pub async fn stop(self) {
        self.seeder.stop().await;
    }
}

// A session on loopback alone: no DHT, no trackers, no discovery.
async fn session(dir: &Path) -> Arc<Session> {
    Session::new_with_opts(
        dir.to_path_buf(),
        SessionOptions {
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            persistence: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

async fn serve(stream: TcpStream, shared: &Shared) -> std::io::Result<()> {
    let mut stream = BufReader::new(stream);
    let mut request_line = String::new();
    stream.read_line(&mut request_line).await?;
    let mut header = String::new();
    loop {
        header.clear();
        if stream.read_line(&mut header).await? == 0 || header == "\r\n" {
            break;
        }
    }
    let target = request_line.split_whitespace().nth(1).unwrap_or("/");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let body = match path {
        "/bt-cd/" => shared.listing.clone(),
        "/announce" => announce(query, shared).await,
        _ if path == format!("/bt-cd/{LISTED}") => shared.torrent.lock().unwrap().clone(),
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

// Keep the announce and answer it as the test asked.
async fn announce(query: &str, shared: &Shared) -> Vec<u8> {
    let field = |name: &str| {
        query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.to_owned())
    };
    let announce = Announce {
        event: field("event").filter(|event| !event.is_empty()),
        port: field("port")
            .and_then(|port| port.parse().ok())
            .unwrap_or(0),
    };
    let answer = shared.answer.lock().unwrap().clone();
    if answer.connect_in && announce.event.as_deref() == Some("started") {
        let info_hash = *shared.info_hash.lock().unwrap();
        let _ = trade_handshakes(announce.port, info_hash).await;
    }
    shared
        .announces
        .send_modify(|announces| announces.push(announce));
    if let Some(failure) = answer.failure {
        return format!("d14:failure reason{}:{failure}e", failure.len()).into_bytes();
    }
    let mut peers = Vec::new();
    for peer in &answer.peers {
        if let SocketAddr::V4(peer) = peer {
            peers.extend_from_slice(&peer.ip().octets());
            peers.extend_from_slice(&peer.port().to_be_bytes());
        }
    }
    let mut body = format!(
        "d8:completei{}e10:incompletei{}e8:intervali1800e5:peers{}:",
        answer.seeders,
        answer.leechers,
        peers.len()
    )
    .into_bytes();
    body.extend_from_slice(&peers);
    body.push(b'e');
    body
}

// Connect to the peer on `port` and trade handshakes with it.
async fn trade_handshakes(port: u16, info_hash: [u8; 20]) -> std::io::Result<()> {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
    stream.write_all(&handshake(info_hash)).await?;
    let mut answer = [0u8; 68];
    stream.read_exact(&mut answer).await?;
    Ok(())
}

// A peer that takes connections, says it has nothing and wants the
// torrent, and once the dialer has the first piece and lets it ask, asks for
// that piece's first block. It ignores every other message.
async fn leech(info_hash: [u8; 20]) -> (SocketAddr, watch::Receiver<u64>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (taken, receiver) = watch::channel(0u64);
    let taken = Arc::new(taken);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let taken = taken.clone();
            tokio::spawn(async move {
                let _ = leech_one(stream, info_hash, &taken).await;
            });
        }
    });
    (addr, receiver)
}

async fn leech_one(
    mut stream: TcpStream,
    info_hash: [u8; 20],
    taken: &watch::Sender<u64>,
) -> std::io::Result<()> {
    let mut theirs = [0u8; 68];
    stream.read_exact(&mut theirs).await?;
    stream.write_all(&handshake(info_hash)).await?;
    // A bitfield with no piece, then interested.
    stream.write_all(&[0, 0, 0, 2, 5, 0]).await?;
    stream.write_all(&[0, 0, 0, 1, 2]).await?;
    let (mut unchoked, mut has_first, mut asked) = (false, false, false);
    loop {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).await?;
        let len = u32::from_be_bytes(len) as usize;
        if len == 0 {
            continue;
        }
        let mut message = vec![0u8; len];
        stream.read_exact(&mut message).await?;
        match message[0] {
            1 => unchoked = true,
            4 => has_first |= message[1..5] == [0, 0, 0, 0],
            5 => has_first |= message.get(1).is_some_and(|bits| bits & 0x80 != 0),
            7 => taken.send_modify(|bytes| *bytes += (len - 9) as u64),
            _ => {}
        }
        if unchoked && has_first && !asked {
            asked = true;
            let mut request = vec![0, 0, 0, 13, 6];
            request.extend_from_slice(&0u32.to_be_bytes());
            request.extend_from_slice(&0u32.to_be_bytes());
            request.extend_from_slice(&PIECE_LEN.to_be_bytes());
            stream.write_all(&request).await?;
        }
    }
}

fn handshake(info_hash: [u8; 20]) -> Vec<u8> {
    let mut handshake = Vec::with_capacity(68);
    handshake.push(19);
    handshake.extend_from_slice(b"BitTorrent protocol");
    handshake.extend_from_slice(&[0; 8]);
    handshake.extend_from_slice(&info_hash);
    handshake.extend_from_slice(b"-FX0001-000000000000");
    handshake
}
