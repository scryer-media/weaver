// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use anyhow::Context;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    AddTorrent, AddTorrentOptions, AnnounceOk, BF, CreateTorrentOptions, FilePriority,
    ListenerMode, ListenerOptions, Session, SessionOptions, TrackerAnnounce, create_torrent,
    spawn_utils::BlockingSpawner,
    storage::StorageFactory,
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::{create_default_random_dir_with_torrents, setup_test_logging},
    },
};

/// Tiers of trackers the operator named: the test trackers listen on
/// loopback.
fn named<S: AsRef<str>>(tiers: Vec<Vec<S>>) -> Vec<Vec<crate::tracker::Tracker>> {
    tiers
        .iter()
        .map(|tier| {
            tier.iter()
                .map(|u| crate::tracker::Tracker::operator(url::Url::parse(u.as_ref()).unwrap()))
                .collect()
        })
        .collect()
}

/// The next HTTP request to the tracker, unanswered, and its `event`.
async fn read_announce(listener: &TcpListener) -> anyhow::Result<(TcpStream, Option<String>)> {
    let (conn, query) = read_announce_query(listener).await?;
    let event = param(&query, "event").map(str::to_owned);
    Ok((conn, event))
}

/// One parameter of an announce's query string.
pub(super) fn param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix(key)?.strip_prefix('='))
}

/// The next HTTP request to the tracker, unanswered, and its query string.
pub(super) async fn read_announce_query(
    listener: &TcpListener,
) -> anyhow::Result<(TcpStream, String)> {
    let (mut conn, _) = listener.accept().await?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = conn.read(&mut chunk).await?;
        anyhow::ensure!(n > 0, "connection closed before the request was read");
        buf.extend_from_slice(&chunk[..n]);
    }
    let request = String::from_utf8(buf)?;
    let path = request.split(' ').nth(1).context("no request path")?;
    let query = path.split_once('?').context("no query")?.1.to_owned();
    Ok((conn, query))
}

pub(super) async fn respond(conn: TcpStream, body: &str) -> anyhow::Result<()> {
    respond_bytes(conn, body.as_bytes()).await
}

async fn respond_bytes(mut conn: TcpStream, body: &[u8]) -> anyhow::Result<()> {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    conn.write_all(head.as_bytes()).await?;
    conn.write_all(body).await?;
    Ok(())
}

/// A tracker answer listing one IPv4 peer, compact (BEP 23).
fn answer_with_peer(peer: SocketAddr) -> anyhow::Result<Vec<u8>> {
    let SocketAddr::V4(peer) = peer else {
        anyhow::bail!("{peer} is not IPv4");
    };
    let mut body = b"d8:intervali1800e5:peers6:".to_vec();
    body.extend_from_slice(&peer.ip().octets());
    body.extend_from_slice(&peer.port().to_be_bytes());
    body.push(b'e');
    Ok(body)
}

pub(crate) fn tracker_session_opts() -> anyhow::Result<SessionOptions> {
    Ok(SessionOptions {
        disable_trackers: false,
        http_client: Some(crate::test_support::loopback()),
        ..isolated_session_opts(false)
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resumed_torrent_announces_only_this_runs_bytes() -> anyhow::Result<()> {
    setup_test_logging();
    const PIECE_LENGTH: u32 = 1024;
    const PIECES: usize = 10;
    const HAVE: usize = 4;
    let t = TestTorrent::new(
        "test_resumed_announces",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let (_seeder, seeder_addr) = t.seed().await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);

    // Resumed with the first 40 % verified and every file selected.
    let mut have = BF::from_boxed_slice(vec![0u8; PIECES.div_ceil(8)].into_boxed_slice());
    for p in 0..HAVE {
        have.set(p, true);
    }
    let out = TempDir::with_prefix("test_resumed_announces_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                checkpoint: Some(crate::Checkpoint::pieces(have)),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    let missing = ((PIECES - HAVE) * PIECE_LENGTH as usize).to_string();
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "downloaded"), Some("0"));
    assert_eq!(param(&query, "uploaded"), Some("0"));
    assert_eq!(param(&query, "left"), Some(missing.as_str()));
    // The tracker hands out the seeder: only now can anything be fetched.
    respond_bytes(conn, &answer_with_peer(seeder_addr)?).await?;

    // Finishing during the run sends `completed` at once.
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("completed"));
    assert_eq!(param(&query, "downloaded"), Some(missing.as_str()));
    assert_eq!(param(&query, "left"), Some("0"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;

    // The run's counters, as it ended, go out with `stopped`.
    session.pause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("stopped"));
    assert_eq!(param(&query, "downloaded"), Some(missing.as_str()));
    assert_eq!(param(&query, "left"), Some("0"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

/// The `port` of the `started` announce a session makes for a new torrent.
async fn started_port(opts: SessionOptions) -> anyhow::Result<(String, Option<u16>)> {
    started_port_after(opts, |_| {}).await
}

/// The `port` of the `started` announce a session makes for a new torrent
/// added after `before` has run on the session.
pub(crate) async fn started_port_after(
    opts: SessionOptions,
    before: impl FnOnce(&Session),
) -> anyhow::Result<(String, Option<u16>)> {
    started_port_at(opts, Ipv4Addr::LOCALHOST.into(), before).await
}

/// [`started_port_after`], with the tracker listening on `tracker_ip`.
pub(crate) async fn started_port_at(
    opts: SessionOptions,
    tracker_ip: std::net::IpAddr,
    before: impl FnOnce(&Session),
) -> anyhow::Result<(String, Option<u16>)> {
    let t = TestTorrent::new("test_announced_port", 2048, 1024).await?;
    let tracker = TcpListener::bind((tracker_ip, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let out = TempDir::with_prefix("test_announced_port_out")?;
    let session = Session::new_with_opts(out.path().into(), opts).await?;
    before(&session);
    session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    let port = param(&query, "port").context("no port")?.to_owned();
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok((port, session.listen_addr().map(|a| a.port())))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_announced_port_is_the_one_the_session_listens_on() -> anyhow::Result<()> {
    setup_test_logging();

    // No listener: port 0, the engine's "unreachable", never port 1 or any
    // other made-up one.
    let (port, listening) = started_port(tracker_session_opts()?).await?;
    assert_eq!(listening, None);
    assert_eq!(port, "0");
    assert_ne!(port, "1");

    // A listener: its port.
    let (port, listening) = started_port(SessionOptions {
        listen: Some(ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: (Ipv4Addr::UNSPECIFIED, 0).into(),
            ..Default::default()
        }),
        ..tracker_session_opts()?
    })
    .await?;
    let listening = listening.context("the session listens")?;
    assert_eq!(port, listening.to_string());
    Ok(())
}

/// A transport that forwards a port to the session's peer listener, and
/// tells the listener so with the reports it was made with; every other
/// listener and dial is a plain loopback socket.
struct Forwarding(std::sync::Mutex<Option<crate::upnp::PortMapReceiver>>);

struct ForwardedListener {
    listener: TcpListener,
    reports: std::sync::Mutex<Option<crate::upnp::PortMapReceiver>>,
}

impl crate::network::StreamListener for ForwardedListener {
    fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    fn accept<'a>(
        &'a self,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            SocketAddr,
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        Box::pin(async move {
            let (stream, peer) = self.listener.accept().await?;
            let (read, write) = stream.into_split();
            Ok((
                peer,
                Box::new(read) as crate::network::BoxAsyncReadVectored,
                Box::new(write) as crate::network::BoxAsyncWrite,
            ))
        })
    }

    fn port_mappings(&self) -> Option<crate::upnp::PortMapReceiver> {
        self.reports.lock().unwrap().take()
    }
}

impl Forwarding {
    fn listener(
        reports: Option<crate::upnp::PortMapReceiver>,
        addr: SocketAddr,
    ) -> crate::network::BoxFuture<'static, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        Box::pin(async move {
            let listener =
                TcpListener::bind(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), addr.port())).await?;
            Ok(Box::new(ForwardedListener {
                listener,
                reports: std::sync::Mutex::new(reports),
            }) as Box<dyn crate::network::StreamListener>)
        })
    }
}

impl crate::network::SocketFactory for Forwarding {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        Box::pin(async move { crate::test_support::tcp(addr).await })
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        Self::listener(None, addr)
    }

    fn listen_peers_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        Self::listener(self.0.lock().unwrap().take(), addr)
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<std::sync::Arc<dyn crate::network::DatagramSocket>>,
    > {
        Box::pin(async {
            let socket = crate::test_support::loopback_udp().await;
            Ok(socket)
        })
    }
}

/// A session whose peer listener's transport forwards a port takes it as
/// it takes one its own NAT-PMP mapper got: peers are told it in the
/// extended handshake, but trackers are told the announce port set in the
/// options (libtorrent `session_impl::queue_tracker_request`,
/// session_impl.cpp:1279-1281). When the transport stops forwarding,
/// peers are told the listen port again.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_set_announce_port_is_told_over_a_port_the_transport_forwards() -> anyhow::Result<()>
{
    use crate::upnp::{PortMapped, PortmapProtocol, PortmapTransport};
    setup_test_logging();
    const FORWARDED: u16 = 50002;
    const CONFIGURED: u16 = 6881;
    let (reports, received) = tokio::sync::mpsc::unbounded_channel();
    let (forward, mut forwarded) = tokio::sync::mpsc::unbounded_channel();
    reports.send(PortMapped {
        transport: PortmapTransport::NatPmp,
        protocol: PortmapProtocol::Tcp,
        local_port: FORWARDED,
        external_port: Some(FORWARDED),
        external_ip: None,
        error: None,
    })?;
    let opts = SessionOptions {
        listen: Some(ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: (Ipv4Addr::UNSPECIFIED, 0).into(),
            announce_port: Some(CONFIGURED),
            ..Default::default()
        }),
        socket_factory: std::sync::Arc::new(Forwarding(std::sync::Mutex::new(Some(received)))),
        port_map_events: Some(forward),
        ..tracker_session_opts()?
    };
    let t = TestTorrent::new("test_forwarded_port", 2048, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let out = TempDir::with_prefix("test_forwarded_port_out")?;
    let session = Session::new_with_opts(out.path().into(), opts).await?;
    let report = forwarded.recv().await.context("no forwarded port")?;
    assert_eq!(report.external_port, Some(FORWARDED));
    assert_eq!(session.external_port_of(false, false), Some(FORWARDED));
    assert_eq!(session.announce_port(), Some(CONFIGURED));
    session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "port"), Some(CONFIGURED.to_string().as_str()));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    let listening = session.listen_addr().map(|a| a.port());
    assert_ne!(listening, Some(FORWARDED));

    // A report that the transport stopped forwarding hands peers the
    // listen port back.
    reports.send(PortMapped {
        transport: PortmapTransport::NatPmp,
        protocol: PortmapProtocol::Tcp,
        local_port: 0,
        external_port: None,
        external_ip: None,
        error: Some("the gateway did not answer NAT-PMP".into()),
    })?;
    let report = forwarded.recv().await.context("no failure report")?;
    assert_eq!(report.external_port, None);
    assert_eq!(session.external_port_of(false, false), listening);
    assert_eq!(session.announce_port(), Some(CONFIGURED));
    Ok(())
}

/// The query of the `started` announce a session makes for a torrent that
/// takes its peers from its trackers alone.
async fn trackers_only_started_query(opts: SessionOptions) -> anyhow::Result<String> {
    let t = TestTorrent::new("test_announce_addresses", 2048, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let out = TempDir::with_prefix("test_announce_addresses_out")?;
    let session = Session::new_with_opts(out.path().into(), opts).await?;
    session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                tracker_tiers: Some(named(vec![vec![url]])),
                trackers_only: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(query)
}

fn assert_names_no_address(query: &str) {
    for key in ["ip", "ipv4", "ipv6"] {
        assert_eq!(param(query, key), None, "{key} in {query}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_an_announce_names_no_address_on_a_session_listening_on_every_interface()
-> anyhow::Result<()> {
    setup_test_logging();
    let query = trackers_only_started_query(SessionOptions {
        listen: Some(ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: (Ipv4Addr::UNSPECIFIED, 0).into(),
            ..Default::default()
        }),
        ..tracker_session_opts()?
    })
    .await?;
    assert_names_no_address(&query);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_an_announce_names_no_address_on_a_socket_factory_bound_to_a_source()
-> anyhow::Result<()> {
    setup_test_logging();
    let factory = std::sync::Arc::new(super::e2e_socket_factory::CountingFactory::default());
    let query = trackers_only_started_query(SessionOptions {
        listen: Some(ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            ..Default::default()
        }),
        socket_factory: factory,
        ..tracker_session_opts()?
    })
    .await?;
    assert_names_no_address(&query);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_selection_announces_what_the_whole_torrent_lacks() -> anyhow::Result<()> {
    setup_test_logging();
    const PIECE_LENGTH: u32 = 1024;
    let dir = create_default_random_dir_with_torrents(3, 4 * PIECE_LENGTH as usize, Some("sel"));
    let torrent = create_torrent(
        dir.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(PIECE_LENGTH),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?
    .as_bytes()?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);

    let out = TempDir::with_prefix("test_partial_selection_announces_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;
    let _handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                only_files: Some(vec![1]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    // One of three piece-aligned files selected: `left` is still every byte
    // of the torrent, as libtorrent's `torrent::bytes_left` counts it, so
    // finishing the selection never reads as a seed.
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "downloaded"), Some("0"));
    assert_eq!(
        param(&query, "left"),
        Some((3 * 4 * PIECE_LENGTH).to_string().as_str())
    );
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

// libtorrent `torrent::finished` calls `completed` only `if (is_seed())`,
// and `torrent::files_checked` presets `m_complete_sent` only for a torrent added as
// a seed: a run that began short of a seed with nothing wanted still
// completes once it has every piece.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_run_that_began_with_nothing_wanted_completes_once_it_is_a_seed()
-> anyhow::Result<()> {
    setup_test_logging();
    const PIECE_LENGTH: u32 = 1024;
    const LENGTH: usize = 8 * PIECE_LENGTH as usize;
    let t = TestTorrent::new("test_nothing_wanted_completes", LENGTH, PIECE_LENGTH).await?;
    let (_seeder, seeder_addr) = t.seed().await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);

    let out = TempDir::with_prefix("test_nothing_wanted_completes_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                file_priorities: Some(vec![FilePriority::Ignored]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    // Nothing wanted: the selection is finished as the run begins, the
    // torrent lacks every piece.
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "left"), Some(LENGTH.to_string().as_str()));
    respond_bytes(conn, &answer_with_peer(seeder_addr)?).await?;

    // Wanted now: the torrent becomes a seed and announces `completed` at
    // once, the interval far off.
    handle.set_file_priorities(&[(0, FilePriority::Normal)])?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("completed"));
    assert_eq!(param(&query, "left"), Some("0"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;

    // Once: the next announce is the run's `stopped`.
    session.pause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("stopped"));
    assert_eq!(param(&query, "left"), Some("0"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

// A torrent added as a seed never sends `completed` (libtorrent
// `torrent::files_checked`: "we just added this torrent as a seed").
#[tokio::test(flavor = "multi_thread")]
async fn test_a_torrent_added_as_a_seed_never_completes() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_added_as_seed", 4096, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);

    let session = Session::new_with_opts(t.dir.path().into(), tracker_session_opts()?).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(t.dir.path().to_str().context("utf-8 path")?.to_owned()),
                overwrite: true,
                trackers: Some(vec![url]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "left"), Some("0"));
    let status = handle.tracker_status().context("a live run has a table")?;
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    // The tracker took `started`, so it is owed `stopped`.
    status.subscribe().wait_for(|a| a[0].last.is_some()).await?;

    // The next announce is the run's `stopped`: no `completed` came between.
    session.pause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("stopped"));
    assert_eq!(param(&query, "left"), Some("0"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

// A run that finishes its selection short of a seed sends no `completed`
// (libtorrent `torrent::finished` without `is_seed`).
#[tokio::test(flavor = "multi_thread")]
async fn test_a_finished_selection_short_of_a_seed_never_completes() -> anyhow::Result<()> {
    setup_test_logging();
    const PIECE_LENGTH: u32 = 1024;
    const FILE: usize = 4 * PIECE_LENGTH as usize;
    let dir = create_default_random_dir_with_torrents(2, FILE, Some("short_of_seed"));
    let torrent = create_torrent(
        dir.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(PIECE_LENGTH),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?
    .as_bytes()?;
    let t = TestTorrent {
        dir,
        torrent,
        content: Vec::new(),
    };
    let (_seeder, seeder_addr) = t.seed().await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);

    let out = TempDir::with_prefix("test_short_of_seed_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                file_priorities: Some(vec![FilePriority::Normal, FilePriority::Ignored]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "left"), Some((2 * FILE).to_string().as_str()));
    respond_bytes(conn, &answer_with_peer(seeder_addr)?).await?;
    handle.wait_until_completed().await?;

    // The selection finished, one file still lacked: the next announce is
    // the run's `stopped`, no `completed` before it.
    session.pause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("stopped"));
    assert_eq!(param(&query, "left"), Some(FILE.to_string().as_str()));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tracker_announces_follow_the_live_run() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_tracker_announces", 4096, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);

    let out = TempDir::with_prefix("test_tracker_announces_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            disable_trackers: false,
            http_client: Some(crate::test_support::loopback()),
            ..isolated_session_opts(false)
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url.clone()]),
                paused: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_initialized().await?;
    // Never run: no tracker task, no table.
    assert_eq!(handle.tracker_announces(), Vec::new());

    // First live run: the tracker is listed, then announced to.
    session.unpause(&handle).await?;
    let first = handle.tracker_status().context("a live run has a table")?;
    let listed = first.announces();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].url, url);
    assert_eq!(listed[0].last, None);

    let (conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("started"));
    assert!(handle.tracker_announces()[0].in_flight);
    assert_eq!(session.external_ips(), Vec::<std::net::IpAddr>::new());
    // The answer names our address, 88.89.90.91 (`XYZ[`), compact.
    respond(
        conn,
        "d8:completei3e11:external ip4:XYZ[10:incompletei5e8:intervali1800e5:peers0:e",
    )
    .await?;
    first.subscribe().wait_for(|a| a[0].last.is_some()).await?;
    // The tracker's vote, cast before the row took the answer.
    assert_eq!(
        session.external_ips(),
        [std::net::IpAddr::from(Ipv4Addr::new(88, 89, 90, 91))]
    );
    let answered = handle.tracker_announces();
    assert!(answered[0].next_announce.is_some());
    assert!(!answered[0].in_flight);
    assert_eq!(
        answered[0].last.as_ref().context("answered")?.result,
        Ok(AnnounceOk {
            interval: Duration::from_secs(1800),
            // Named by none, libtorrent's parse_tracker_response default.
            min_interval: Duration::from_secs(30),
            seeders: Some(3),
            leechers: Some(5),
            downloaded: None,
            peers: Vec::new(),
            warning: None,
        })
    );

    // Paused: the task ends and sends `stopped`, whose answer is the row's
    // last outcome; no announce is due while paused.
    session.pause(&handle).await?;
    let (conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("stopped"));
    assert_eq!(handle.tracker_announces()[0].next_announce, None);
    respond(conn, "d8:completei7e8:intervali1800e5:peers0:e").await?;
    first
        .subscribe()
        .wait_for(|a| {
            !a[0].in_flight
                && a[0]
                    .last
                    .as_ref()
                    .is_some_and(|l| l.result.as_ref().is_ok_and(|ok| ok.seeders == Some(7)))
        })
        .await?;
    let answered = handle.tracker_announces();
    assert_eq!(answered[0].next_announce, None);
    assert_eq!(answered[0].fails, 0);

    // Second live run: a new table for the new task, each row keeping the
    // last answer until this run has one.
    session.unpause(&handle).await?;
    let second = handle.tracker_status().context("a live run has a table")?;
    let (_conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("started"));
    assert_eq!(
        handle.tracker_announces(),
        vec![TrackerAnnounce {
            url: url.clone(),
            in_flight: true,
            last: answered[0].last.clone(),
            ..Default::default()
        }]
    );
    assert_eq!(first.announces(), answered);

    // Paused mid-announce: nothing stays in flight, and the last answer
    // stands.
    session.pause(&handle).await?;
    second.subscribe().wait_for(|a| !a[0].in_flight).await?;
    assert_eq!(
        handle.tracker_announces(),
        vec![TrackerAnnounce {
            url,
            last: answered[0].last.clone(),
            ..Default::default()
        }]
    );
    Ok(())
}

// A resumed torrent's tracker reads as it last answered, the `stopped`
// answer of the pause, while `started` is on its way, and takes the
// `started` answer once it lands.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_resumed_tracker_keeps_its_last_answer_until_started_answers() -> anyhow::Result<()>
{
    setup_test_logging();
    let t = TestTorrent::new("test_resumed_tracker_row", 4096, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let out = TempDir::with_prefix("test_resumed_tracker_row_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url.clone()]),
                paused: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_initialized().await?;
    let seeders = |a: &TrackerAnnounce, n: u32| {
        a.last
            .as_ref()
            .is_some_and(|l| l.result.as_ref().is_ok_and(|ok| ok.seeders == Some(n)))
    };

    session.unpause(&handle).await?;
    let first = handle.tracker_status().context("a live run has a table")?;
    let (conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("started"));
    respond(conn, "d8:completei3e8:intervali1800e5:peers0:e").await?;
    first.subscribe().wait_for(|a| seeders(&a[0], 3)).await?;

    // Paused: `stopped` is answered, and the row reads that answer.
    session.pause(&handle).await?;
    let (conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("stopped"));
    respond(conn, "d8:completei7e8:intervali1800e5:peers0:e").await?;
    first
        .subscribe()
        .wait_for(|a| !a[0].in_flight && seeders(&a[0], 7))
        .await?;
    let stopped = handle.tracker_announces()[0].last.clone();

    // Resumed: while `started` waits for its answer, the row is updating
    // and still reads the `stopped` answer.
    session.unpause(&handle).await?;
    let second = handle.tracker_status().context("a live run has a table")?;
    let (conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("started"));
    let row = second
        .subscribe()
        .wait_for(|a| a[0].in_flight)
        .await?
        .clone();
    assert_eq!(row[0].last, stopped);

    // The `started` answer replaces it.
    respond(conn, "d8:completei9e8:intervali1800e5:peers0:e").await?;
    second
        .subscribe()
        .wait_for(|a| !a[0].in_flight && seeders(&a[0], 9))
        .await?;

    session.pause(&handle).await?;
    let (conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("stopped"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

/// A magnet for `torrent`'s hashes, naming nothing else.
fn magnet_of(torrent: &[u8]) -> anyhow::Result<String> {
    let hashes = crate::core::torrent_metainfo::torrent_from_bytes(torrent)?.info_hashes;
    Ok(crate::core::magnet::Magnet::of(hashes, None, Vec::new()).to_string())
}

/// This client under a peer id of `byte` alone, which an announce's query
/// names as is.
fn identity_of(byte: u8) -> crate::Identity {
    crate::Identity {
        peer_id: crate::core::hash_id::Id20::new([byte; 20]),
        ..Default::default()
    }
}

/// Fetches `magnet`'s metadata as a list-only add under `identity`,
/// answering the tracker's `started` with `peer`, which serves it. Returns
/// the fetch's answer and its announce table.
async fn fetch_metadata(
    session: &std::sync::Arc<Session>,
    tracker: &TcpListener,
    url: &str,
    magnet: String,
    identity: crate::Identity,
    peer: SocketAddr,
) -> anyhow::Result<(crate::ListOnlyResponse, crate::TrackerStatus)> {
    let peer_id = String::from_utf8(identity.peer_id.0.to_vec())?;
    let (table_tx, table_rx) = tokio::sync::oneshot::channel();
    let fetch = session.add_torrent(
        AddTorrent::from_url(magnet),
        Some(AddTorrentOptions {
            list_only: true,
            tracker_tiers: Some(named(vec![vec![url]])),
            identity: Some(identity),
            metadata_fetch: Some(table_tx),
            ..Default::default()
        }),
    );
    let tracker = async {
        let (conn, query) = read_announce_query(tracker).await?;
        assert_eq!(param(&query, "event"), Some("started"));
        assert_eq!(param(&query, "peer_id"), Some(peer_id.as_str()));
        respond_bytes(conn, &answer_with_peer(peer)?).await
    };
    let (listed, answered) = tokio::join!(fetch, tracker);
    answered?;
    let crate::AddTorrentResponse::ListOnly(listed) = listed? else {
        anyhow::bail!("a list-only add lists");
    };
    Ok((listed, table_rx.await?.trackers.context("trackers are on")?))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_magnets_fetch_and_its_run_are_one_torrent_to_its_tracker() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_magnet_handover", 4096, 1024).await?;
    let (_seeder, seeder_addr) = t.seed().await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let out = TempDir::with_prefix("test_magnet_handover_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;

    let fetched_as = identity_of(b'f');
    let peer_id = String::from_utf8(fetched_as.peer_id.0.to_vec())?;
    let (mut listed, table) = fetch_metadata(
        &session,
        &tracker,
        &url,
        magnet_of(&t.torrent)?,
        fetched_as.clone(),
        seeder_addr,
    )
    .await?;

    // The torrent added from the fetched metainfo, taking over the fetch's
    // trackers under the identity they heard `started` from.
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(listed.torrent_bytes.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url.clone()]),
                // The fetched metainfo names the tracker as its own; the caller
                // lists it as the operator's, as the fetch did.
                tracker_tiers: Some(named(vec![vec![&url]])),
                identity: Some(fetched_as),
                initial_peers: Some(vec![seeder_addr]),
                tracker_handover: listed.trackers.take(),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_completed().await?;

    // No second `started` and no `stopped` came before this: the run's
    // first announce is `completed`, from the fetch's task.
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("completed"));
    assert_eq!(param(&query, "peer_id"), Some(peer_id.as_str()));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    table
        .subscribe()
        .wait_for(|a| !a[0].in_flight && a[0].last.is_some())
        .await?;
    // One table, the fetch's, read through the run.
    let run_table = handle.tracker_status().context("the run's table")?;
    assert!(run_table.subscribe().same_channel(&table.subscribe()));

    // Paused: one `stopped`, under the same peer id.
    session.pause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("stopped"));
    assert_eq!(param(&query, "peer_id"), Some(peer_id.as_str()));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_magnet_paused_once_its_metadata_is_fetched_sends_stopped_once() -> anyhow::Result<()>
{
    setup_test_logging();
    let t = TestTorrent::new("test_magnet_paused_handover", 4096, 1024).await?;
    let (_seeder, seeder_addr) = t.seed().await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let out = TempDir::with_prefix("test_magnet_paused_handover_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;

    let fetched_as = identity_of(b'p');
    let peer_id = String::from_utf8(fetched_as.peer_id.0.to_vec())?;
    let (mut listed, _table) = fetch_metadata(
        &session,
        &tracker,
        &url,
        magnet_of(&t.torrent)?,
        fetched_as,
        seeder_addr,
    )
    .await?;

    // Added paused: no run takes the fetch's task over, and the add lets go
    // of it, which stops the torrent at its tracker.
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(listed.torrent_bytes.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url.clone()]),
                // The fetched metainfo names the tracker as its own; the caller
                // lists it as the operator's, as the fetch did.
                tracker_tiers: Some(named(vec![vec![&url]])),
                identity: Some(identity_of(b'r')),
                paused: true,
                tracker_handover: listed.trackers.take(),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("stopped"));
    assert_eq!(param(&query, "peer_id"), Some(peer_id.as_str()));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;

    // Its first run is a new presence: `started`, under its own peer id.
    handle.wait_until_initialized().await?;
    session.unpause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "peer_id"), Some("r".repeat(20).as_str()));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_magnet_removed_while_its_metadata_is_fetched_sends_stopped_once()
-> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_magnet_removed_fetch", 4096, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let out = TempDir::with_prefix("test_magnet_removed_fetch_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;

    let (table_tx, table_rx) = tokio::sync::oneshot::channel();
    let fetch = tokio::spawn({
        let session = session.clone();
        let magnet = magnet_of(&t.torrent)?;
        let url = url.clone();
        async move {
            session
                .add_torrent(
                    AddTorrent::from_url(magnet),
                    Some(AddTorrentOptions {
                        list_only: true,
                        tracker_tiers: Some(named(vec![vec![url]])),
                        identity: Some(identity_of(b'x')),
                        metadata_fetch: Some(table_tx),
                        ..Default::default()
                    }),
                )
                .await
                .map(|_| ())
        }
    });
    // The tracker knows no peer: the metadata never comes.
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    // The tracker took `started`: it is owed `stopped`.
    let table = table_rx.await?.trackers.context("trackers are on")?;
    table
        .subscribe()
        .wait_for(|a| !a[0].in_flight && a[0].last.is_some())
        .await?;

    // Removed while fetching: the fetch is dropped, and with it the
    // torrent's one presence at its tracker.
    fetch.abort();
    assert!(fetch.await.is_err_and(|e| e.is_cancelled()));
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("stopped"));
    assert_eq!(param(&query, "peer_id"), Some("x".repeat(20).as_str()));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_magnets_run_starts_from_the_peers_its_fetchs_tracker_gave() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_magnet_handover_peers", 4096, 1024).await?;
    let (_seeder, seeder_addr) = t.seed().await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let out = TempDir::with_prefix("test_magnet_handover_peers_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;

    let fetched_as = identity_of(b'g');
    let peer_id = String::from_utf8(fetched_as.peer_id.0.to_vec())?;
    let (mut listed, _table) = fetch_metadata(
        &session,
        &tracker,
        &url,
        magnet_of(&t.torrent)?,
        fetched_as.clone(),
        seeder_addr,
    )
    .await?;

    // No initial peers and no DHT: the seeder is known to the run only as
    // the peer the tracker gave the fetch, and the tracker is not asked
    // again before the run completes.
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(listed.torrent_bytes.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url.clone()]),
                // The fetched metainfo names the tracker as its own; the caller
                // lists it as the operator's, as the fetch did.
                tracker_tiers: Some(named(vec![vec![&url]])),
                identity: Some(fetched_as),
                tracker_handover: listed.trackers.take(),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_completed().await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("completed"));
    assert_eq!(param(&query, "peer_id"), Some(peer_id.as_str()));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

/// A session that announces to trackers and runs a DHT whose one bootstrap
/// node is `node`, a loopback socket the test holds: whatever the DHT sends
/// lands there and nowhere else.
fn discovery_session_opts<S: crate::network::DatagramSocket + ?Sized>(
    node: &std::sync::Arc<S>,
) -> anyhow::Result<SessionOptions> {
    Ok(SessionOptions {
        dht: Some(crate::DhtSessionConfig {
            bootstrap_addrs: Some(vec![node.bind_addr().to_string()]),
            port: None,
            persistence: None,
        }),
        ipv4_only: true,
        ..tracker_session_opts()?
    })
}

/// The DHT's next get_peers for `info_hash` to `node`, answering nothing
/// else. Returns the request's transaction id and where it came from.
async fn next_get_peers<S: crate::network::DatagramSocket + ?Sized>(
    node: &std::sync::Arc<S>,
    info_hash: crate::core::hash_id::Id20,
) -> anyhow::Result<(crate::bencode::ByteBufOwned, SocketAddr)> {
    use crate::dht::bprotocol::{MessageKind, deserialize_message};
    let mut buf = [0u8; 2048];
    loop {
        let (len, from) = node.recv_from(&mut buf).await?;
        let Ok(message) = deserialize_message::<crate::bencode::ByteBufOwned>(&buf[..len]) else {
            continue;
        };
        if let MessageKind::GetPeersRequest(request) = message.kind
            && request.info_hash == info_hash
        {
            return Ok((message.transaction_id, from));
        }
    }
}

/// Whether the DHT has sent `node` a get_peers for `info_hash` by now.
fn asked_for_peers(
    node: &crate::network::KernelUdp,
    info_hash: crate::core::hash_id::Id20,
) -> bool {
    use crate::dht::bprotocol::{MessageKind, deserialize_message};
    let mut buf = [0u8; 2048];
    while let Ok((len, _)) = node.try_recv_from(&mut buf) {
        if let Ok(message) = deserialize_message::<crate::bencode::ByteBufOwned>(&buf[..len])
            && matches!(message.kind, MessageKind::GetPeersRequest(r) if r.info_hash == info_hash)
        {
            return true;
        }
    }
    false
}

/// Starts fetching `magnet`'s metadata as a list-only add announcing to
/// `tiers`, taking peers from them alone when `trackers_only`. Returns the
/// fetch, running, and its view.
async fn start_fetch(
    session: &std::sync::Arc<Session>,
    magnet: String,
    tiers: Vec<Vec<String>>,
    trackers_only: bool,
) -> anyhow::Result<(
    tokio::task::JoinHandle<anyhow::Result<crate::AddTorrentResponse>>,
    crate::MetadataFetch,
)> {
    let (fetch_tx, fetch_rx) = tokio::sync::oneshot::channel();
    let fetch = tokio::spawn({
        let session = session.clone();
        async move {
            session
                .add_torrent(
                    AddTorrent::from_url(magnet),
                    Some(AddTorrentOptions {
                        list_only: true,
                        tracker_tiers: Some(named(tiers)),
                        trackers_only,
                        metadata_fetch: Some(fetch_tx),
                        ..Default::default()
                    }),
                )
                .await
        }
    });
    Ok((fetch, fetch_rx.await?))
}

fn info_hash_of(torrent: &[u8]) -> anyhow::Result<crate::core::hash_id::Id20> {
    crate::core::torrent_metainfo::torrent_from_bytes(torrent)?
        .info_hashes
        .v1
        .context("a v1 torrent")
}

const REFUSED: &str = "d14:failure reason7:refusede";

#[tokio::test(flavor = "multi_thread")]
async fn test_a_fetch_whose_tracker_names_a_peer_never_asks_the_dht() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_fetch_tracker_peer", 4096, 1024).await?;
    let (_seeder, seeder_addr) = t.seed().await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let node = crate::test_support::loopback_kernel_udp();
    let out = TempDir::with_prefix("test_fetch_tracker_peer_out")?;
    let session = Session::new_with_opts(out.path().into(), discovery_session_opts(&node)?).await?;

    let (fetch, view) =
        start_fetch(&session, magnet_of(&t.torrent)?, vec![vec![url]], false).await?;
    assert_eq!(*view.phase.borrow(), crate::FetchPhase::Trackers);
    let (conn, _) = read_announce_query(&tracker).await?;
    respond_bytes(conn, &answer_with_peer(seeder_addr)?).await?;
    let crate::AddTorrentResponse::ListOnly(_) = fetch.await?? else {
        anyhow::bail!("a list-only add lists");
    };

    // The tracker named the peer that served the metadata: the fetch never
    // went on to the DHT.
    assert_eq!(*view.phase.borrow(), crate::FetchPhase::Trackers);
    assert!(!asked_for_peers(&node, info_hash_of(&t.torrent)?));
    session.cancellation_token().cancel();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_fetch_whose_tracker_fails_goes_on_to_the_dht() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_fetch_tracker_fails", 4096, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let node = crate::test_support::loopback_udp().await;
    let out = TempDir::with_prefix("test_fetch_tracker_fails_out")?;
    let session = Session::new_with_opts(out.path().into(), discovery_session_opts(&node)?).await?;

    let (fetch, mut view) =
        start_fetch(&session, magnet_of(&t.torrent)?, vec![vec![url]], false).await?;
    let (conn, _) = read_announce_query(&tracker).await?;
    respond(conn, REFUSED).await?;
    view.phase
        .wait_for(|phase| *phase == crate::FetchPhase::Discovery)
        .await?;
    assert_eq!(view.phase.borrow().to_string(), "trackers, DHT, LSD");
    next_get_peers(&node, info_hash_of(&t.torrent)?).await?;
    fetch.abort();
    session.cancellation_token().cancel();
    Ok(())
}

/// Accepts the next connection to `peer` and reads its handshake; the
/// connection is left waiting for the answer.
async fn accept_handshake(peer: &TcpListener) -> anyhow::Result<TcpStream> {
    let (mut conn, _) = peer.accept().await?;
    let mut theirs = [0u8; 68];
    conn.read_exact(&mut theirs).await?;
    Ok(conn)
}

/// Answers a handshake for `info_hash` with no extension bit set: the peer
/// cannot serve metadata.
async fn answer_without_extensions(
    mut conn: TcpStream,
    info_hash: crate::core::hash_id::Id20,
) -> anyhow::Result<TcpStream> {
    let mut ours = crate::peer_protocol::Handshake::new(
        info_hash,
        crate::core::hash_id::Id20::new([0x55; 20]),
    );
    ours.reserved = 0;
    let mut out = [0u8; 68];
    let len = ours.serialize_unchecked_len(&mut out);
    conn.write_all(&out[..len]).await?;
    Ok(conn)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_fetch_whose_tracker_names_only_a_peer_without_extensions_goes_on_to_the_dht()
-> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_fetch_tracker_peer_unable", 4096, 1024).await?;
    let info_hash = info_hash_of(&t.torrent)?;
    let peer = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let peer_addr = peer.local_addr()?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let node = crate::test_support::loopback_kernel_udp();
    let out = TempDir::with_prefix("test_fetch_tracker_peer_unable_out")?;
    let session = Session::new_with_opts(out.path().into(), discovery_session_opts(&node)?).await?;

    let (fetch, mut view) =
        start_fetch(&session, magnet_of(&t.torrent)?, vec![vec![url]], false).await?;
    let (conn, _) = read_announce_query(&tracker).await?;
    respond_bytes(conn, &answer_with_peer(peer_addr)?).await?;

    // The tracker named the peer, which is being asked: the fetch stays on
    // its trackers.
    let conn = accept_handshake(&peer).await?;
    assert_eq!(*view.phase.borrow(), crate::FetchPhase::Trackers);
    assert!(!asked_for_peers(&node, info_hash));

    // The peer cannot serve metadata: the trackers named no peer that may,
    // and the fetch goes on to the DHT.
    let _conn = answer_without_extensions(conn, info_hash).await?;
    view.phase
        .wait_for(|phase| *phase == crate::FetchPhase::Discovery)
        .await?;
    next_get_peers(&node, info_hash).await?;
    fetch.abort();
    session.cancellation_token().cancel();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_fetch_without_discovery_whose_tracker_fails_waits() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_fetch_tracker_fails_waits", 4096, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let node = crate::test_support::loopback_kernel_udp();
    let out = TempDir::with_prefix("test_fetch_tracker_fails_waits_out")?;
    let session = Session::new_with_opts(out.path().into(), discovery_session_opts(&node)?).await?;

    let (fetch, mut view) =
        start_fetch(&session, magnet_of(&t.torrent)?, vec![vec![url]], true).await?;
    let (conn, _) = read_announce_query(&tracker).await?;
    respond(conn, REFUSED).await?;
    view.phase
        .wait_for(|phase| *phase == crate::FetchPhase::Waiting)
        .await?;
    assert_eq!(view.phase.borrow().to_string(), "no peers from trackers");
    assert!(!asked_for_peers(&node, info_hash_of(&t.torrent)?));
    fetch.abort();
    session.cancellation_token().cancel();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_fetch_with_no_tracker_and_no_discovery_waits() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_fetch_no_tracker_waits", 4096, 1024).await?;
    let node = crate::test_support::loopback_kernel_udp();
    let out = TempDir::with_prefix("test_fetch_no_tracker_waits_out")?;
    let session = Session::new_with_opts(out.path().into(), discovery_session_opts(&node)?).await?;

    // Added rather than refused: it waits, and says why.
    let (fetch, view) = start_fetch(&session, magnet_of(&t.torrent)?, Vec::new(), true).await?;
    assert_eq!(*view.phase.borrow(), crate::FetchPhase::Waiting);
    assert_eq!(view.phase.borrow().to_string(), "no peers from trackers");
    assert!(!fetch.is_finished());
    assert!(!asked_for_peers(&node, info_hash_of(&t.torrent)?));
    fetch.abort();
    session.cancellation_token().cancel();
    Ok(())
}

/// `torrent` marked private (BEP 27): its info dictionary, the last key of
/// the metainfo, ends with `pieces`, which `private` sorts after.
pub(crate) fn made_private(torrent: &[u8]) -> anyhow::Result<bytes::Bytes> {
    let body = torrent
        .strip_suffix(b"ee")
        .context("the info dictionary closes the metainfo")?;
    let private = [body, b"7:privatei1eee".as_slice()].concat();
    let parsed = crate::core::torrent_metainfo::torrent_from_bytes(&private)?;
    anyhow::ensure!(parsed.info.data.private, "not private");
    Ok(private.into())
}

/// A peer that serves `torrent`'s metadata to the first peer that connects,
/// private or not, and does nothing else. Returns its task and its address.
async fn serve_metadata(
    torrent: &[u8],
) -> anyhow::Result<(tokio::task::JoinHandle<anyhow::Result<()>>, SocketAddr)> {
    use crate::buffers::ByteBuf;
    use crate::peer_protocol::{
        Handshake, Message,
        extended::{
            ExtendedMessage, PeerExtendedMessageIds,
            handshake::ExtendedHandshake,
            ut_metadata::{UtMetadata, UtMetadataData},
        },
    };
    let parsed = crate::core::torrent_metainfo::torrent_from_bytes(torrent)?;
    let info_hash = parsed.info_hashes.v1.context("a v1 torrent")?;
    let info = parsed.info.raw_bytes.as_ref().to_vec();
    let size = u32::try_from(info.len())?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await?;
        let mut theirs = [0u8; 68];
        conn.read_exact(&mut theirs).await?;
        let mut out = vec![0u8; 64 * 1024];
        let len = Handshake::new(info_hash, crate::core::hash_id::Id20::new([0x55; 20]))
            .serialize_unchecked_len(&mut out);
        conn.write_all(&out[..len]).await?;
        let mut ours = ExtendedHandshake::new();
        ours.metadata_size = Some(size);
        let len = Message::Extended(ExtendedMessage::Handshake(ours))
            .serialize(&mut out, &PeerExtendedMessageIds::my)?;
        conn.write_all(&out[..len]).await?;
        loop {
            let frame = conn.read_u32().await?;
            let mut message = frame.to_be_bytes().to_vec();
            message.resize(4 + frame as usize, 0);
            conn.read_exact(&mut message[4..]).await?;
            if let Ok((
                Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Request(piece))),
                _,
            )) = Message::deserialize(&message, &[], None)
            {
                let data = UtMetadataData::from_bytes(piece, size, ByteBuf(&info));
                let len = Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Data(data)))
                    .serialize(&mut out, &PeerExtendedMessageIds::my)?;
                conn.write_all(&out[..len]).await?;
            }
        }
    });
    Ok((task, addr))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_a_private_torrent_found_on_the_dht_takes_peers_from_its_trackers_alone()
-> anyhow::Result<()> {
    use crate::dht::bprotocol::{MessageKind, Response, serialize_message};
    setup_test_logging();
    let mut t = TestTorrent::new("test_fetch_private_after_dht", 4096, 1024).await?;
    t.torrent = made_private(&t.torrent)?;
    let info_hash = info_hash_of(&t.torrent)?;
    let (server, seeder_addr) = serve_metadata(&t.torrent).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let node = crate::test_support::loopback_udp().await;
    let out = TempDir::with_prefix("test_fetch_private_after_dht_out")?;
    let session = Session::new_with_opts(out.path().into(), discovery_session_opts(&node)?).await?;

    // A running add: its fetch is the start of the torrent's run.
    let (fetch_tx, fetch_rx) = tokio::sync::oneshot::channel();
    let add = tokio::spawn({
        let session = session.clone();
        let magnet = magnet_of(&t.torrent)?;
        let url = url.clone();
        async move {
            session
                .add_torrent(
                    AddTorrent::from_url(magnet),
                    Some(AddTorrentOptions {
                        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                        tracker_tiers: Some(named(vec![vec![url]])),
                        metadata_fetch: Some(fetch_tx),
                        ..Default::default()
                    }),
                )
                .await
        }
    });
    let mut view: crate::MetadataFetch = fetch_rx.await?;
    let (conn, _) = read_announce_query(&tracker).await?;
    respond(conn, REFUSED).await?;
    view.phase
        .wait_for(|phase| *phase == crate::FetchPhase::Discovery)
        .await?;

    // The DHT names the seeder, which serves the metadata: private.
    let (transaction_id, dht) = next_get_peers(&node, info_hash).await?;
    let mut answer = Vec::new();
    serialize_message(
        &mut answer,
        transaction_id,
        None,
        None,
        MessageKind::Response(Response {
            id: crate::core::hash_id::Id20::new([0x33; 20]),
            values: Some(vec![seeder_addr.into()]),
            token: Some(b"t".as_slice().into()),
            ..Default::default()
        }),
    )?;
    node.send_to(&answer, dht).await?;
    let handle = add.await??.into_handle().context("expected a handle")?;

    // From its metadata on, it takes peers from its trackers alone.
    assert!(handle.takes_peers_from_trackers_only());
    server.abort();
    session.cancellation_token().cancel();
    Ok(())
}

// An announce tells the tracker the encryption the torrent's connections
// are made under: on a session that prefers it, a torrent that takes the
// session's policy says `supportcrypto`, one that requires it says
// `requirecrypto` too, and one that never encrypts says neither.
#[tokio::test(flavor = "multi_thread")]
async fn test_an_announce_says_the_encryption_the_torrent_connects_under() -> anyhow::Result<()> {
    use crate::{EncryptionPolicy, PeerConnectionOptions};
    setup_test_logging();
    let out = TempDir::with_prefix("test_announce_crypto_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            encryption: EncryptionPolicy::Prefer,
            ..tracker_session_opts()?
        },
    )
    .await?;
    for (name, policy, support, require) in [
        ("announce_crypto_session", None, Some("1"), None),
        (
            "announce_crypto_require",
            Some(EncryptionPolicy::Require),
            Some("1"),
            Some("1"),
        ),
        (
            "announce_crypto_disabled",
            Some(EncryptionPolicy::Disabled),
            None,
            None,
        ),
    ] {
        let t = TestTorrent::new(name, 4096, 1024).await?;
        let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let url = format!("http://{}/announce", tracker.local_addr()?);
        session
            .add_torrent(
                AddTorrent::from_bytes(t.torrent.clone()),
                Some(AddTorrentOptions {
                    storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                    trackers: Some(vec![url]),
                    peer_opts: Some(PeerConnectionOptions {
                        encryption: policy,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            )
            .await?;
        let (conn, query) = read_announce_query(&tracker).await?;
        assert_eq!(param(&query, "event"), Some("started"), "{name}");
        assert_eq!(param(&query, "supportcrypto"), support, "{name}");
        assert_eq!(param(&query, "requirecrypto"), require, "{name}");
        respond(conn, "d8:intervali1800e5:peers0:e").await?;
    }
    session.cancellation_token().cancel();
    Ok(())
}

// The `tracker id` a tracker gave is the torrent's to send back for as long
// as its identity is kept: the `started` of a resumed run carries it, and so
// does the `started` of the torrent added to another session under the same
// identity, as a session reopening adds it.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_tracker_id_outlives_the_run_and_the_session() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_tracker_id_kept", 4096, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let add = || {
        Some(AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            trackers: Some(vec![url.clone()]),
            paused: true,
            ..Default::default()
        })
    };
    let out = TempDir::with_prefix("test_tracker_id_kept_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;
    let handle = session
        .add_torrent(AddTorrent::from_bytes(t.torrent.clone()), add())
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_initialized().await?;

    session.unpause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "trackerid"), None);
    respond(conn, "d8:intervali1800e5:peers0:10:tracker id3:t-1e").await?;
    let status = handle.tracker_status().context("a live run has a table")?;
    status.subscribe().wait_for(|a| a[0].last.is_some()).await?;

    session.pause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("stopped"));
    assert_eq!(param(&query, "trackerid"), Some("t-1"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    status.subscribe().wait_for(|a| !a[0].in_flight).await?;

    // A resumed run: a new tracker task.
    session.unpause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "trackerid"), Some("t-1"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    let status = handle.tracker_status().context("a live run has a table")?;
    status
        .subscribe()
        .wait_for(|a| a[0].last.is_some() && !a[0].in_flight)
        .await?;
    let identity = handle.shared().identity();
    session.pause(&handle).await?;
    let (conn, _) = read_announce_query(&tracker).await?;
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    status.subscribe().wait_for(|a| !a[0].in_flight).await?;

    // Another session, the torrent added under the identity it had.
    let out2 = TempDir::with_prefix("test_tracker_id_kept_out2")?;
    let reopened = Session::new_with_opts(out2.path().into(), tracker_session_opts()?).await?;
    let again = reopened
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                identity: Some(identity),
                paused: false,
                ..add().unwrap()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "trackerid"), Some("t-1"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    let status = again.tracker_status().context("a live run has a table")?;
    status.subscribe().wait_for(|a| a[0].last.is_some()).await?;
    Ok(())
}
