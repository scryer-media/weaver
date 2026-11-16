// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::Arc;

use tempfile::TempDir;

use super::tests::seeding_with;
use super::{AddTorrent, AddTorrentOptions, IpFilter, Session, SessionOptions};

// A session for the incoming-connection rule tests, with nothing that
// reaches out.
async fn quiet_session(dir: &TempDir, opts: SessionOptions) -> std::sync::Arc<Session> {
    Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            ..opts
        },
    )
    .await
    .unwrap()
}

// Writes an IP list holding `range` into `dir`; returns its path.
fn ip_list(dir: &TempDir, name: &str, range: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, format!("test:{range}\n")).unwrap();
    path
}

// Hands the session an incoming connection from `addr` whose peer stays
// open and sends nothing, and polls the check once: the refusal when it
// refuses without reading, `None` while it waits on the peer's handshake.
fn check_once(session: &std::sync::Arc<Session>, addr: &str) -> Option<String> {
    use crate::{peer_info_reader::tests::halves, stream_connect::ConnectionKind};
    use futures::FutureExt;
    let (ours, _theirs) = tokio::io::duplex(1 << 16);
    let (reader, writer) = halves(ours);
    session
        .clone()
        .check_incoming_connection(
            addr.parse().unwrap(),
            ConnectionKind::Tcp,
            false,
            reader,
            writer,
        )
        .now_or_never()
        .map(|checked| format!("{:#}", checked.err().unwrap()))
}

// libtorrent `torrent::start_announcing` runs from `files_checked`: a
// torrent announces to the local network only once its check is done,
// as it does to its trackers and the DHT.
#[tokio::test(start_paused = true)]
async fn a_torrent_announces_on_the_local_network_only_once_checked() -> anyhow::Result<()> {
    use crate::{
        lsd::RecordingSocket,
        storage::StorageFactory,
        tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
    };
    use anyhow::Context;

    let t = TestTorrent::new("test_lsd_after_check", 2 * 16384, 16384).await?;
    let (sent_tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
    let out = TempDir::with_prefix("test_lsd_after_check_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            disable_local_service_discovery: false,
            lsd_socket: Some(Box::new(RecordingSocket(sent_tx))),
            // A loopback listener announces no port unless told one.
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                announce_port: Some(6881),
                ..Default::default()
            }),
            ..isolated_session_opts(false)
        },
    )
    .await?;
    let lsd = session.lsd.clone().context("no LSD")?;
    let checks = session.concurrent_initialize_semaphore.clone();
    let held = checks
        .clone()
        .acquire_many_owned(u32::try_from(checks.available_permits()).unwrap())
        .await?;

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let info_hash = handle.info_hash();
    assert!(!lsd.registered(info_hash), "announced before its check");

    drop(held);
    let announce = sent.recv().await.context("no announce")?;
    assert!(announce.contains(&info_hash.as_string()), "{announce}");
    assert!(lsd.registered(info_hash));

    // A paused torrent is checked, and announces nothing.
    let paused = TestTorrent::new("test_lsd_paused", 2 * 16384, 16384).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(paused.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                paused: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_initialized().await?;
    assert!(handle.is_paused());
    assert!(!lsd.registered(handle.info_hash()));
    // Nor once rechecked: it stays paused.
    session.force_recheck(&handle).await?;
    handle.wait_until_initialized().await?;
    assert!(handle.is_paused());
    assert!(!lsd.registered(handle.info_hash()));
    session.stop().await;
    Ok(())
}

// libtorrent `torrent::lsd_announce` gives `session_impl::listen_port`,
// the listen socket's `tcp_external_port`: the listen port, then the
// port a mapper got once it has one, never the announce port set for
// trackers and the DHT.
#[tokio::test(start_paused = true)]
async fn the_local_network_is_told_the_mapped_port_not_the_set_one() -> anyhow::Result<()> {
    use crate::{
        lsd::RecordingSocket,
        storage::StorageFactory,
        tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        upnp::{PortMapped, PortmapProtocol, PortmapTransport},
    };
    use anyhow::Context;

    fn port_of(announce: &str) -> Option<u16> {
        announce
            .lines()
            .find_map(|line| line.strip_prefix("Port: "))
            .and_then(|port| port.trim().parse().ok())
    }

    let t = TestTorrent::new("test_lsd_mapped_port", 2 * 16384, 16384).await?;
    let (sent_tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
    let out = TempDir::with_prefix("test_lsd_mapped_port_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            disable_local_service_discovery: false,
            lsd_socket: Some(Box::new(RecordingSocket(sent_tx))),
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                announce_port: Some(6881),
                ..Default::default()
            }),
            ..isolated_session_opts(false)
        },
    )
    .await?;
    let listening = session.listen_addr().context("no listener")?.port();
    assert_ne!(listening, 6881);
    let _handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let first = sent.recv().await.context("no announce")?;
    assert_eq!(port_of(&first), Some(listening), "{first}");

    session.announce_port.on_port_mapping(&PortMapped {
        transport: PortmapTransport::NatPmp,
        protocol: PortmapProtocol::Tcp,
        local_port: listening,
        external_port: Some(2000),
        external_ip: None,
        error: None,
    });
    // The resends of the first announce repeat it; the next announce,
    // an interval on, gives the mapped port.
    let mut told = Vec::new();
    while told.last() != Some(&Some(2000)) {
        let announce = sent.recv().await.context("no announce")?;
        told.push(port_of(&announce));
    }
    assert_eq!(
        told,
        [Some(listening), Some(listening), Some(2000)],
        "the resends, then the next announce"
    );
    assert_eq!(session.announce_port(), Some(6881));
    session.stop().await;
    Ok(())
}

// The local discovery is part of the swarm a stopping session leaves:
// once stop returns it listens no more and announces nothing again,
// however long the clock then runs.
#[tokio::test(start_paused = true)]
async fn a_stopped_session_is_done_with_the_local_network() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{
        lsd::ListeningSocket,
        storage::StorageFactory,
        tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
    };
    use anyhow::Context;

    let t = TestTorrent::new("test_lsd_stop", 2 * 16384, 16384).await?;
    let (sent_tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
    let listening = Arc::new(AtomicUsize::new(0));
    let out = TempDir::with_prefix("test_lsd_stop_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            disable_local_service_discovery: false,
            lsd_socket: Some(Box::new(ListeningSocket {
                sent: sent_tx,
                listening: listening.clone(),
            })),
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                announce_port: Some(6881),
                ..Default::default()
            }),
            ..isolated_session_opts(false)
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let announce = sent.recv().await.context("no announce")?;
    assert!(
        announce.contains(&handle.info_hash().as_string()),
        "{announce}"
    );
    assert_eq!(listening.load(Ordering::SeqCst), 1);

    session.stop().await;
    assert_eq!(
        listening.load(Ordering::SeqCst),
        0,
        "the local discovery still listens after stop returned"
    );
    while sent.try_recv().is_ok() {}
    tokio::time::advance(std::time::Duration::from_secs(3600)).await;
    assert!(
        sent.try_recv().is_err(),
        "the local discovery announced after stop returned"
    );
    Ok(())
}

/// Joins that take `outcomes` in turn, and the script they take them
/// from, to see what is left of it.
fn scripted_joins(
    outcomes: Vec<anyhow::Result<Box<dyn crate::lsd::Socket>>>,
) -> (crate::lsd::JoinScript, crate::lsd::Join) {
    let script: crate::lsd::JoinScript = Arc::new(std::sync::Mutex::new(outcomes.into()));
    (script.clone(), crate::lsd::scripted_join(script))
}

/// A session whose local discovery joins as `joins` say, listening on
/// loopback so that its torrents announce a port.
async fn lsd_session(
    out: &TempDir,
    disabled: bool,
    joins: crate::lsd::Join,
) -> anyhow::Result<Arc<Session>> {
    Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            disable_local_service_discovery: disabled,
            lsd_join: Some(joins),
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                announce_port: Some(6881),
                ..Default::default()
            }),
            ..crate::tests::embedder_util::isolated_session_opts(false)
        },
    )
    .await
}

/// A torrent downloads from a seeder it hears of on the local network
/// alone. With `joins_at_open` false, the session opens offline: its
/// local discovery cannot join until the rejoin made after the torrent
/// is added.
async fn download_from_a_local_peer(name: &str, joins_at_open: bool) -> anyhow::Result<()> {
    use crate::{
        lsd::InjectedSocket,
        storage::StorageFactory,
        tests::embedder_util::{MemoryStorageFactory, TestTorrent},
    };
    use anyhow::Context;

    let t = TestTorrent::new(name, 4 * 16384, 16384).await?;
    let (_seeder, seeder) = t.seed().await?;
    let (sent_tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
    let (incoming, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
    let socket: Box<dyn crate::lsd::Socket> = Box::new(InjectedSocket {
        sent: sent_tx,
        incoming: tokio::sync::Mutex::new(incoming_rx),
    });
    let outcomes = if joins_at_open {
        vec![Ok(socket)]
    } else {
        vec![Err(anyhow::anyhow!("no interface up")), Ok(socket)]
    };
    let (_, joins) = scripted_joins(outcomes);
    let out = TempDir::with_prefix(name)?;
    let session = lsd_session(&out, false, joins).await?;
    let lsd = session.lsd.clone().context("no LSD")?;
    assert_eq!(lsd.joined(), joins_at_open);

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let info_hash = handle.info_hash();
    if !joins_at_open {
        session.rejoin_local_discovery()?;
        assert!(lsd.joined());
    }

    // Its first announce shows the torrent listens; a seeder on the
    // host's network then announces it.
    let announce = sent.recv().await.context("no announce")?;
    assert!(announce.contains(&info_hash.as_string()), "{announce}");
    incoming.send((
        format!(
            "BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: {}\r\nInfohash: {}\r\n\r\n\r\n",
            seeder.port(),
            info_hash.as_string()
        ),
        (std::net::Ipv4Addr::LOCALHOST, 6771).into(),
    ))?;
    handle.wait_until_completed().await?;
    session.stop().await;
    Ok(())
}

// A session that opens offline joins local discovery on the first
// rejoin that can, and from then on does what one that joined at open
// does: the torrent added meanwhile announces, and a seeder announced on
// the local network reaches it and serves it whole.
#[tokio::test(flavor = "multi_thread")]
async fn local_discovery_joined_on_a_rejoin_works_as_one_joined_at_open() -> anyhow::Result<()> {
    download_from_a_local_peer("test_lsd_joined_at_open", true).await?;
    download_from_a_local_peer("test_lsd_joined_on_rejoin", false).await
}

// A session with local discovery disabled starts none on a rejoin, and
// does not try to join.
#[tokio::test]
async fn a_rejoin_starts_no_local_discovery_where_it_is_disabled() -> anyhow::Result<()> {
    use crate::lsd::RecordingSocket;

    let (sent_tx, _sent) = tokio::sync::mpsc::unbounded_channel();
    let (script, joins) = scripted_joins(vec![Ok(Box::new(RecordingSocket(sent_tx)))]);
    let out = TempDir::with_prefix("test_lsd_disabled_rejoin")?;
    let session = lsd_session(&out, true, joins).await?;
    session.rejoin_local_discovery()?;
    assert!(session.lsd.is_none());
    assert_eq!(script.lock().unwrap().len(), 1, "a join was tried");
    session.stop().await;
    Ok(())
}

// A rejoin whose join fails reports it and leaves local discovery
// unjoined, as it was; the next rejoin that can join it does.
#[tokio::test(start_paused = true)]
async fn a_failed_rejoin_leaves_local_discovery_for_the_next() -> anyhow::Result<()> {
    use crate::lsd::RecordingSocket;
    use anyhow::Context;

    let (sent_tx, _sent) = tokio::sync::mpsc::unbounded_channel();
    let (_, joins) = scripted_joins(vec![
        Err(anyhow::anyhow!("no interface up")),
        Err(anyhow::anyhow!("still none up")),
        Ok(Box::new(RecordingSocket(sent_tx))),
    ]);
    let out = TempDir::with_prefix("test_lsd_failed_rejoin")?;
    let session = lsd_session(&out, false, joins).await?;
    let lsd = session.lsd.clone().context("no LSD")?;
    assert!(!lsd.joined());
    let error = session
        .rejoin_local_discovery()
        .expect_err("the join failed");
    assert!(format!("{error:#}").contains("still none up"), "{error:#}");
    assert!(!lsd.joined());
    session.rejoin_local_discovery()?;
    assert!(lsd.joined());
    session.stop().await;
    Ok(())
}

// Local discovery joined on a rejoin is part of the swarm a stopping
// session leaves, as one joined at open is: once stop returns it
// listens no more and announces nothing again.
#[tokio::test(start_paused = true)]
async fn a_stopped_session_is_done_with_local_discovery_joined_late() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{
        lsd::ListeningSocket,
        storage::StorageFactory,
        tests::embedder_util::{MemoryStorageFactory, TestTorrent},
    };
    use anyhow::Context;

    let t = TestTorrent::new("test_lsd_late_stop", 2 * 16384, 16384).await?;
    let (sent_tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
    let listening = Arc::new(AtomicUsize::new(0));
    let (_, joins) = scripted_joins(vec![
        Err(anyhow::anyhow!("no interface up")),
        Ok(Box::new(ListeningSocket {
            sent: sent_tx,
            listening: listening.clone(),
        })),
    ]);
    let out = TempDir::with_prefix("test_lsd_late_stop_out")?;
    let session = lsd_session(&out, false, joins).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    session.rejoin_local_discovery()?;
    let announce = sent.recv().await.context("no announce")?;
    assert!(
        announce.contains(&handle.info_hash().as_string()),
        "{announce}"
    );
    while listening.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    assert_eq!(listening.load(Ordering::SeqCst), 1);

    session.stop().await;
    assert_eq!(
        listening.load(Ordering::SeqCst),
        0,
        "the local discovery still listens after stop returned"
    );
    while sent.try_recv().is_ok() {}
    tokio::time::advance(std::time::Duration::from_secs(3600)).await;
    assert!(
        sent.try_recv().is_err(),
        "the local discovery announced after stop returned"
    );
    Ok(())
}

fn blocked_incoming(session: &Session) -> u64 {
    session
        .stats
        .counters
        .blocked_incoming
        .load(std::sync::atomic::Ordering::Relaxed)
}

// An incoming connection passes the rule an outgoing one does: libtorrent
// `session_impl::incoming_connection` refuses an endpoint its one
// `ip_filter` blocks (`peer_blocked_alert::ip_filter`) before reading a
// byte, and the blocklist and the allowlist are that one filter.
#[tokio::test(start_paused = true)]
async fn an_incoming_connection_from_a_filtered_address_is_refused_unread() {
    let dir = TempDir::with_prefix("incoming_filtered").unwrap();
    let session = quiet_session(
        &dir,
        SessionOptions {
            ip_filter: IpFilter {
                blocked: Arc::new(
                    crate::IpRanges::load_from_file(ip_list(
                        &dir,
                        "block.txt",
                        "10.0.0.1-10.0.0.1",
                    ))
                    .await
                    .unwrap(),
                ),
                ..IpFilter::default()
            },
            allowlist_url: Some(
                url::Url::from_file_path(ip_list(&dir, "allow.txt", "10.0.0.0-10.0.0.255"))
                    .unwrap()
                    .to_string(),
            ),
            ..crate::test_support::session_options()
        },
    )
    .await;

    // On the blocklist, then off the allowlist: each refused unread, and
    // counted.
    for (counted, addr) in [(1, "10.0.0.1:6881"), (2, "192.0.2.1:6881")] {
        let refusal = check_once(&session, addr).expect("refused unread");
        assert!(refusal.contains("banned by the IP filter"), "{refusal}");
        assert_eq!(blocked_incoming(&session), counted);
    }

    // Let through, the check waits on the peer's handshake.
    assert_eq!(check_once(&session, "10.0.0.2:6881"), None);
    assert_eq!(blocked_incoming(&session), 2);
    session.stop().await;
}

// An IPv4-only session refuses an IPv6 peer whichever way the connection
// goes. libtorrent's session has no listen socket of a family it does not
// connect over, so `incoming_connection` never sees such a peer and posts
// no `peer_blocked_alert` for it: the refusal is not counted as blocked.
#[tokio::test(start_paused = true)]
async fn an_ipv6_incoming_connection_on_an_ipv4_only_session_is_refused() {
    let dir = TempDir::with_prefix("incoming_family").unwrap();
    let session = quiet_session(
        &dir,
        SessionOptions {
            ipv4_only: true,
            ..crate::test_support::session_options()
        },
    )
    .await;

    let refusal = check_once(&session, "[2001:db8::1]:6881").expect("refused unread");
    assert!(
        refusal.contains("of a family the session does not connect over"),
        "{refusal}"
    );
    assert_eq!(blocked_incoming(&session), 0);
    use crate::session_stats::{Transport, incoming::IncomingEnd};
    let ends = session.stats.incoming.snapshot().ends;
    assert_eq!(
        ends[Transport::Tcp as usize][IncomingEnd::Family as usize],
        1
    );

    assert_eq!(check_once(&session, "192.0.2.1:6881"), None);
    session.stop().await;
}

// Each listener mode has the session run one accept task for each socket
// it binds, under the name it had: TCP's `tcp_listen`, uTP's
// `utp_listen`. A task the session spawns has run once it stops, so the
// tasks polled are the tasks spawned.
#[tokio::test]
async fn each_listener_mode_runs_one_accept_task_per_socket() {
    use crate::core::spawn_utils::polls;
    let listen_tasks = || -> std::collections::BTreeMap<String, u64> {
        polls::counted()
            .into_iter()
            .filter(|(task, _)| task.ends_with("_listen"))
            .collect()
    };
    for (mode, tasks) in [
        (crate::ListenerMode::TcpOnly, &["tcp_listen"][..]),
        (crate::ListenerMode::UtpOnly, &["utp_listen"]),
        (
            crate::ListenerMode::TcpAndUtp,
            &["tcp_listen", "utp_listen"],
        ),
    ] {
        let before = listen_tasks();
        let dir = TempDir::with_prefix("listen_tasks").unwrap();
        let session = quiet_session(
            &dir,
            SessionOptions {
                listen: Some(crate::ListenerOptions {
                    mode,
                    listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                ..crate::test_support::session_options()
            },
        )
        .await;
        session.stop().await;
        let ran: Vec<_> = listen_tasks()
            .into_iter()
            .filter(|(task, polled)| before.get(task) != Some(polled))
            .map(|(task, _)| task)
            .collect();
        assert_eq!(ran, tasks, "{mode:?}");
    }
}

// A connection a peer opens to each of the listener's sockets is counted
// under that socket's kind: a handshake for a torrent the session does
// not hold ends the same over TCP as over uTP, under each its own.
#[tokio::test]
async fn each_listener_socket_counts_its_incoming_connections_under_its_kind() {
    use crate::session_stats::{Transport, incoming::IncomingEnd};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = TempDir::with_prefix("incoming_kinds").unwrap();
    let session = quiet_session(
        &dir,
        SessionOptions {
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpAndUtp,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..crate::test_support::session_options()
        },
    )
    .await;
    let listening = session.listen_addr().unwrap();
    let mut handshake = b"\x13BitTorrent protocol".to_vec();
    handshake.extend_from_slice(&[0; 8]);
    handshake.extend_from_slice(&[7; 20]);
    handshake.extend_from_slice(&[9; 20]);

    // Each connection ends once the session has refused and closed it.
    let (mut read, mut write) = crate::test_support::tcp(listening).await.unwrap();
    write.write_all(&handshake).await.unwrap();
    let _ = read.read_to_end(&mut Vec::new()).await;
    let peer = crate::listen::bind_utp(
        crate::test_support::socket_factory().as_ref(),
        (std::net::Ipv4Addr::LOCALHOST, 0).into(),
        Default::default(),
        crate::udp_options::STREAM_WINDOW,
        None,
    )
    .await
    .unwrap();
    let (mut read, mut write) = peer.connect(listening).await.unwrap().split();
    write.write_all(&handshake).await.unwrap();
    let _ = read.read_to_end(&mut Vec::new()).await;

    let ends = session.stats.incoming.snapshot().ends;
    let refused = |kind: Transport| {
        let mut end = [0; IncomingEnd::COUNT];
        end[IncomingEnd::UnknownTorrent as usize] = 1;
        (kind, ends[kind as usize] == end)
    };
    assert_eq!(
        [refused(Transport::Tcp), refused(Transport::Utp)],
        [(Transport::Tcp, true), (Transport::Utp, true)],
        "{ends:?}"
    );
    session.stop().await;
}

// An address one torrent banned for bad data is banned on every torrent
// of the session, by address whatever its port, as libtorrent bans one
// peer per address (peer_list.cpp:446-459 `ban_peer`, found by address
// in `add_peer` with `allow_multiple_connections_per_ip` off,
// peer_list.cpp:654-664, and refused in `new_connection`,
// peer_list.cpp:700-703). libtorrent bans it on the one torrent
// (torrent.cpp:11531-11542); here the session refuses it everywhere
// while it runs.
#[tokio::test]
async fn an_address_banned_for_bad_data_on_one_torrent_is_banned_on_every_torrent() {
    let (dir, session, a) = seeding_with(10, 0, crate::limits::LimitKind::Unlimited).await;
    let b = crate::create_torrent(
        dir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(32768),
            ..Default::default()
        },
        &crate::spawn_utils::BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap();
    let b = session
        .add_torrent(
            AddTorrent::from_bytes(b),
            Some(AddTorrentOptions {
                output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    b.wait_until_completed().await.unwrap();
    let (a, b) = (a.live().unwrap(), b.live().unwrap());
    let state = |torrent: &crate::torrent_state::TorrentStateLive, addr: &str| {
        torrent
            .per_peer_stats_snapshot(
                crate::torrent_state::peer::stats::snapshot::PeerStatsFilter {
                    state: crate::torrent_state::peer::stats::snapshot::PeerStatsFilterState::All,
                },
            )
            .peers
            .get(addr)
            .map(|peer| peer.state)
    };

    // B knows the liar at one port before A bans it.
    b.add_peer(
        "127.0.0.7:6881".parse().unwrap(),
        crate::type_aliases::PeerSourceFlag::Tracker,
    );
    assert_eq!(state(&b, "127.0.0.7:6881"), Some("queued"));

    a.ban_for_bad_data(
        "127.0.0.7:6881"
            .parse::<std::net::SocketAddr>()
            .unwrap()
            .into(),
    );

    // Nothing below awaits, so no connection is attempted meanwhile.
    // On B the peer it knew is no connect candidate, and the address at
    // another port joins B's list no more, so it is never dialled.
    assert_eq!(state(&b, "127.0.0.7:6881"), Some("dead"));
    b.add_peer(
        "127.0.0.7:7000".parse().unwrap(),
        crate::type_aliases::PeerSourceFlag::Tracker,
    );
    assert_eq!(state(&b, "127.0.0.7:7000"), None);
    // An incoming connection from it, at any port, is refused unread.
    let refusal = check_once(&session, "127.0.0.7:7000").expect("refused unread");
    assert!(refusal.contains("is banned"), "{refusal}");
    // Another address is untouched.
    b.add_peer(
        "127.0.0.8:6881".parse().unwrap(),
        crate::type_aliases::PeerSourceFlag::Tracker,
    );
    assert_eq!(state(&b, "127.0.0.8:6881"), Some("queued"));
    assert_eq!(check_once(&session, "127.0.0.8:6881"), None);
    session.stop().await;
}

/// A web seed at a URL nothing serves.
fn idle_web_seed() -> crate::WebSeed {
    crate::WebSeed {
        url: "http://192.0.2.1/s/".into(),
        kind: crate::WebSeedKind::Url,
        named_by: crate::core::net::NamedBy::Operator,
    }
}

// A web seed is banned on the torrent it sent bad data to, by its entry
// there: another torrent of the session with a seed of the same id and URL
// still uses its own.
#[tokio::test]
async fn a_web_seed_banned_on_one_torrent_is_usable_on_another() {
    let (dir, session, a) = seeding_with(10, 0, crate::limits::LimitKind::Unlimited).await;
    let b = crate::create_torrent(
        dir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(32768),
            ..Default::default()
        },
        &crate::spawn_utils::BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap();
    let b = session
        .add_torrent(
            AddTorrent::from_bytes(b),
            Some(AddTorrentOptions {
                output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    b.wait_until_completed().await.unwrap();
    assert!(a.add_web_seed(idle_web_seed()));
    assert!(b.add_web_seed(idle_web_seed()));

    let seed = crate::peer_handle::PeerHandle::WebSeed(0);
    a.live().unwrap().ban_for_bad_data(seed);

    let due = |torrent: &super::ManagedTorrentHandle| {
        torrent
            .shared
            .web_seeds
            .due(tokio::time::Instant::now(), |_| true, false)
            .len()
    };
    assert!(a.web_seeds().is_empty());
    assert_eq!(due(&a), 0);
    assert_eq!(b.web_seeds(), [idle_web_seed()]);
    assert_eq!(due(&b), 1);
    assert!(b.shared.web_seeds.link(0).is_some());
    session.stop().await;
}

// A web seed's ban outlasts a pause: once resumed, the torrent does not
// connect to it again.
#[tokio::test]
async fn a_banned_web_seed_stays_banned_across_a_pause() {
    let (_dir, session, torrent) = seeding_with(10, 0, crate::limits::LimitKind::Unlimited).await;
    assert!(torrent.add_web_seed(idle_web_seed()));
    torrent
        .live()
        .unwrap()
        .ban_for_bad_data(crate::peer_handle::PeerHandle::WebSeed(0));

    session.pause(&torrent).await.unwrap();
    session.unpause(&torrent).await.unwrap();
    torrent.wait_until_completed().await.unwrap();

    let seeds = &torrent.shared.web_seeds;
    assert!(torrent.web_seeds().is_empty());
    assert!(
        seeds
            .due(tokio::time::Instant::now(), |_| true, false)
            .is_empty()
    );
    assert!(seeds.link(0).is_none());
    session.stop().await;
}

// What a test's TCP want holds: says when the want is let go.
struct WantGone(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for WantGone {
    fn drop(&mut self) {
        if let Some(gone) = self.0.take() {
            let _ = gone.send(());
        }
    }
}

// A look finds the torrent's route crowded out; the receiver resolves once
// the want it left is let go. The look lands before the connect loop's wait,
// and the wait lets the want go on entry, or in it, and leaves none.
fn hold_tcp_want(torrent: &super::ManagedTorrentHandle) -> tokio::sync::oneshot::Receiver<()> {
    let (gone, released) = tokio::sync::oneshot::channel();
    torrent
        .live()
        .unwrap()
        .tcp_wants
        .looked(crate::network::TcpRoom::Crowded(
            crate::network::TcpWant::new(WantGone(Some(gone))),
        ));
    released
}

// A paused torrent dials no one, so it holds no TCP want: the route stops
// counting it as pressing for TCP room.
#[tokio::test]
async fn a_paused_torrent_holds_no_tcp_want() {
    let (_dir, session, torrent) = seeding_with(10, 0, crate::limits::LimitKind::Unlimited).await;
    let mut released = hold_tcp_want(&torrent);
    session.pause(&torrent).await.unwrap();
    assert_eq!(
        released.try_recv(),
        Ok(()),
        "the paused torrent kept its want"
    );
    session.stop().await;
}

// A completed torrent that wants no peers, its upload closed, holds no TCP
// want once its connect loop looks again.
#[tokio::test]
async fn a_completed_torrent_wanting_no_peers_holds_no_tcp_want() {
    let (_dir, session, torrent) = seeding_with(10, 0, crate::limits::LimitKind::Closed).await;
    let released = hold_tcp_want(&torrent);
    torrent.live().unwrap().connections_per_ip_changed();
    released.await.unwrap();
    assert!(!torrent.live().unwrap().tcp_wants.holds());
    session.stop().await;
}

// A completed torrent that still seeds but knows no peer left to dial holds
// no TCP want once its connect loop looks again.
#[tokio::test]
async fn a_completed_torrent_with_no_one_to_dial_holds_no_tcp_want() {
    let (_dir, session, torrent) = seeding_with(10, 0, crate::limits::LimitKind::Unlimited).await;
    let released = hold_tcp_want(&torrent);
    torrent.live().unwrap().connections_per_ip_changed();
    released.await.unwrap();
    assert!(!torrent.live().unwrap().tcp_wants.holds());
    session.stop().await;
}

// At its bound the session forgets its oldest ban for bad data.
#[test]
fn the_session_forgets_its_oldest_ban_for_bad_data_at_its_bound() {
    use crate::torrent_state::live::peers::{MAX_SESSION_BANS, SessionBans};
    let bans = SessionBans::default();
    let ip = |n: usize| std::net::IpAddr::from(u32::try_from(n).unwrap().to_be_bytes());
    for n in 0..MAX_SESSION_BANS {
        assert!(bans.insert(ip(n)));
    }
    assert!(!bans.insert(ip(0)));
    assert!(bans.contains(ip(0)));
    assert!(bans.insert(ip(MAX_SESSION_BANS)));
    assert!(!bans.contains(ip(0)));
    assert!(bans.contains(ip(1)));
    assert!(bans.contains(ip(MAX_SESSION_BANS)));
}

// libtorrent `session_impl::set_ip_filter` and `update_privileged_ports`
// take hold in a running session: an incoming connection is judged by
// the filter as it is when it arrives, its address against the ranges
// and its port against `no_connect_privileged_ports`.
#[tokio::test(start_paused = true)]
async fn a_running_session_judges_incoming_connections_by_its_filter_as_it_changes() {
    let dir = TempDir::with_prefix("incoming_refiltered").unwrap();
    let session = quiet_session(&dir, crate::test_support::session_options()).await;
    assert_eq!(check_once(&session, "10.0.0.1:6881"), None);
    assert_eq!(check_once(&session, "10.0.0.2:80"), None);

    let range = "10.0.0.1".parse().unwrap().."10.0.0.2".parse().unwrap();
    let blocked = Arc::new(crate::IpRanges::new([range], []));
    session.set_ip_filter(IpFilter {
        blocked: Arc::clone(&blocked),
        no_privileged_ports: true,
        trackers: true,
    });
    let refusal = check_once(&session, "10.0.0.1:6881").expect("refused unread");
    assert!(refusal.contains("banned by the IP filter"), "{refusal}");
    let refusal = check_once(&session, "10.0.0.2:80").expect("refused unread");
    assert!(refusal.contains("privileged port"), "{refusal}");
    assert_eq!(check_once(&session, "10.0.0.2:1024"), None);
    assert_eq!(blocked_incoming(&session), 1);
    // Each refusal is counted once under its transport and reason, and a
    // connection still waiting on its handshake under none.
    use crate::session_stats::{Transport, incoming::IncomingEnd};
    let ends = session.stats.incoming.snapshot().ends;
    let tcp = ends[Transport::Tcp as usize];
    assert_eq!(tcp[IncomingEnd::Filtered as usize], 1);
    assert_eq!(tcp[IncomingEnd::PrivilegedPort as usize], 1);
    assert_eq!(ends.as_flattened().iter().sum::<u64>(), 2);

    session.set_ip_filter(IpFilter::default());
    assert_eq!(check_once(&session, "10.0.0.1:6881"), None);
    assert_eq!(check_once(&session, "10.0.0.2:80"), None);
    session.stop().await;
}

// libtorrent `torrent::build_tracker_request` hands an announce the IP
// filter only under `apply_ip_filter_to_trackers`: off, a tracker is
// asked at an address the filter blocks; the family rule holds either
// way.
#[tokio::test(start_paused = true)]
async fn trackers_are_held_to_the_ip_filter_only_while_it_applies_to_them() {
    use crate::tracker::Unreachable;
    let dir = TempDir::with_prefix("tracker_filter").unwrap();
    let session = quiet_session(
        &dir,
        SessionOptions {
            ipv4_only: true,
            ..crate::test_support::session_options()
        },
    )
    .await;
    let reachable = session.reachable();
    let blocked: std::net::IpAddr = "10.0.0.1".parse().unwrap();
    let range = "10.0.0.1".parse().unwrap().."10.0.0.2".parse().unwrap();
    let filter = |trackers| IpFilter {
        blocked: Arc::new(crate::IpRanges::new([range.clone()], [])),
        no_privileged_ports: false,
        trackers,
    };

    session.set_ip_filter(filter(true));
    assert_eq!(reachable(blocked), Err(Unreachable::Filtered));
    session.set_ip_filter(filter(false));
    assert_eq!(reachable(blocked), Ok(()));
    assert_eq!(
        reachable("2001:db8::1".parse().unwrap()),
        Err(Unreachable::Family)
    );
    // Peers stay held to it.
    assert_eq!(session.may_reach(blocked), Err(Unreachable::Filtered));
    session.stop().await;
}

#[test]
fn a_fetch_goes_on_from_its_trackers_once_they_named_no_peer_that_may_serve() {
    use super::FetchPhase::{self, Discovery, Trackers, Waiting};
    use crate::{AnnounceError, AnnounceErrorKind, AnnounceOk, LastAnnounce, TrackerAnnounce};

    let peer = |port| std::net::SocketAddr::from(([10, 0, 0, 1], port));
    let answered = |ports: &[u16]| TrackerAnnounce {
        last: Some(LastAnnounce {
            at: std::time::SystemTime::UNIX_EPOCH,
            result: Ok(AnnounceOk {
                interval: std::time::Duration::from_secs(1800),
                min_interval: std::time::Duration::from_secs(60),
                seeders: None,
                leechers: None,
                downloaded: None,
                peers: ports.iter().copied().map(peer).collect(),
                warning: None,
            }),
        }),
        ..Default::default()
    };
    let failed = || TrackerAnnounce {
        last: Some(LastAnnounce {
            at: std::time::SystemTime::UNIX_EPOCH,
            result: Err(AnnounceError {
                kind: AnnounceErrorKind::Failed,
                message: "refused".to_owned(),
            }),
        }),
        ..Default::default()
    };
    let unasked = TrackerAnnounce::default;

    let none = std::collections::HashSet::new;
    let unable = |ports: &[u16]| {
        ports
            .iter()
            .copied()
            .map(peer)
            .collect::<std::collections::HashSet<_>>()
    };
    for (table, unable, discovery, phase) in [
        (vec![], none(), true, Discovery),
        (vec![], none(), false, Waiting),
        (vec![unasked()], none(), true, Trackers),
        (vec![failed(), unasked()], none(), true, Trackers),
        (vec![failed(), failed()], none(), true, Discovery),
        (vec![failed(), failed()], none(), false, Waiting),
        (vec![answered(&[])], none(), true, Discovery),
        (vec![answered(&[]), unasked()], none(), false, Waiting),
        (vec![answered(&[1])], none(), true, Trackers),
        (vec![failed(), answered(&[1, 2])], none(), false, Trackers),
        // Every peer the answers named was found unable: settled.
        (vec![answered(&[1])], unable(&[1]), true, Discovery),
        (
            vec![answered(&[1]), unasked()],
            unable(&[1]),
            false,
            Waiting,
        ),
        (
            vec![answered(&[1]), answered(&[2])],
            unable(&[1, 2]),
            true,
            Discovery,
        ),
        // One named peer may still serve it.
        (vec![answered(&[1, 2])], unable(&[1]), true, Trackers),
        (
            vec![answered(&[1]), answered(&[2])],
            unable(&[1]),
            true,
            Trackers,
        ),
        // A peer found unable that no answer named settles nothing.
        (vec![unasked()], unable(&[1]), true, Trackers),
    ] {
        assert_eq!(
            FetchPhase::of(&table, &unable, discovery),
            phase,
            "{table:?}, unable {unable:?}, discovery {discovery}"
        );
    }
}

#[test]
fn the_session_caps_utp_syns_without_setting_a_receive_buffer() {
    let mut utp = weaver_utp::SocketOpts::default();
    super::session_utp_opts(&mut utp, 200, None);
    assert!(utp.vsock_rx_bufsize_bytes.is_none());
    assert_eq!(utp.connections_limit.map(|n| n.get()), Some(200));
}
