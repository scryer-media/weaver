// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::Arc;

use tempfile::TempDir;

use super::tests::{seeding, seeding_with};
use super::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, Session, SessionOptions, TorrentIdOrHash,
};

// Connects to the session and sends a handshake for `handle`'s torrent.
// Returns what the session answers before it closes, up to its own
// 68-byte handshake: empty when it refused the connection.
async fn answer(session: &Session, handle: &super::ManagedTorrentHandle) -> Vec<u8> {
    greet(session, handle.info_hash()).await
}

// Connects to the session and sends a handshake for `info_hash`; returns
// what [`answer`] returns.
async fn greet(session: &Session, info_hash: super::Id20) -> Vec<u8> {
    greet_with(session, info_hash, false).await.1
}

// Connects to the session and sends a handshake for `info_hash`,
// offering v2 when `offer_v2` holds; returns the connection and what
// [`answer`] returns.
async fn greet_with(
    session: &Session,
    info_hash: super::Id20,
    offer_v2: bool,
) -> (
    (
        crate::type_aliases::BoxAsyncReadVectored,
        crate::type_aliases::BoxAsyncWrite,
    ),
    Vec<u8>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut read, mut write) = crate::test_support::tcp(session.listen_addr().unwrap())
        .await
        .unwrap();
    let mut handshake = [0u8; 68];
    let len = crate::peer_protocol::Handshake::new(info_hash, super::Id20::new([7; 20]))
        .offering_v2(offer_v2)
        .serialize_unchecked_len(&mut handshake);
    // A refused connection may be closed before or while this is written,
    // and reset after: either way nothing comes back.
    let _ = write.write_all(&handshake[..len]).await;
    let mut answer = Vec::new();
    let _ = (&mut read).take(68).read_to_end(&mut answer).await;
    ((read, write), answer)
}

// Whether a 68-byte handshake answer offers v2.
fn offers_v2(answer: &[u8]) -> bool {
    crate::peer_protocol::Handshake::deserialize(answer)
        .unwrap()
        .0
        .offers_v2()
}

fn is_handshake_for(answer: &[u8], handle: &super::ManagedTorrentHandle) -> bool {
    answer.len() == 68
        && &answer[1..20] == b"BitTorrent protocol"
        && answer[28..48] == handle.info_hash().0
}

// Each incoming connection is kept under its torrent's encryption, the
// session's when the torrent names none: a plaintext one reaches a
// torrent that prefers encryption in a session that requires it, and is
// refused by a torrent that requires it in a session that prefers it.
#[tokio::test]
async fn an_incoming_plaintext_connection_is_judged_by_its_torrents_encryption() {
    use crate::{mse::EncryptionPolicy, peer_connection::PeerConnectionOptions};
    for (session_policy, torrent_policy, kept) in [
        (EncryptionPolicy::Prefer, None, true),
        (
            EncryptionPolicy::Prefer,
            Some(EncryptionPolicy::Require),
            false,
        ),
        (EncryptionPolicy::Require, None, false),
        (
            EncryptionPolicy::Require,
            Some(EncryptionPolicy::Prefer),
            true,
        ),
    ] {
        let dir = crate::tests::test_util::create_default_random_dir_with_torrents(
            1,
            16384,
            Some("torrent_encryption"),
        );
        let torrent = crate::create_torrent(
            dir.path(),
            crate::CreateTorrentOptions {
                piece_length: Some(16384),
                ..Default::default()
            },
            &crate::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .unwrap()
        .as_bytes()
        .unwrap();
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                dht: None,
                persistence: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                encryption: session_policy,
                listen: Some(crate::ListenerOptions {
                    mode: crate::ListenerMode::TcpOnly,
                    listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                ..crate::test_support::session_options()
            },
        )
        .await
        .unwrap();
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent),
                Some(AddTorrentOptions {
                    output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    peer_opts: Some(PeerConnectionOptions {
                        encryption: torrent_policy,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_completed().await.unwrap();
        let answer = answer(&session, &handle).await;
        assert_eq!(
            is_handshake_for(&answer, &handle),
            kept,
            "{session_policy:?} {torrent_policy:?}"
        );
        session.stop().await;
    }
}

#[tokio::test]
async fn an_incoming_connection_past_the_session_cap_and_slack_is_closed() {
    let (_dir, session, handle) = seeding_with(1, 0, crate::limits::LimitKind::Unlimited).await;

    // The one place under the cap is taken and there is no slack: the
    // listener closes the connection it accepts without reading the
    // handshake (libtorrent `session_impl::incoming_connection`).
    let held = session.connection_slots.try_open().unwrap();
    assert!(answer(&session, &handle).await.is_empty());

    // Freed, the same connection is taken and answered.
    drop(held);
    assert!(is_handshake_for(&answer(&session, &handle).await, &handle));

    session.stop().await;
}

// A listener that hands out the connections and errors it is given, in
// order, then waits for ever; it counts the accepts asked of it and says
// when it has handed out an error.
struct Scripted {
    accepts: std::sync::Mutex<std::collections::VecDeque<std::io::Result<tokio::io::DuplexStream>>>,
    asked: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    // A scripted failure waits for this before it fails the accept.
    fail_when: std::sync::Arc<tokio::sync::Notify>,
    errored: std::sync::Arc<tokio::sync::Notify>,
    listener: std::sync::Mutex<Option<Box<dyn crate::network::StreamListener>>>,
}

impl crate::network::StreamListener for std::sync::Arc<Scripted> {
    fn local_addr(&self) -> std::net::SocketAddr {
        (std::net::Ipv4Addr::LOCALHOST, 6881).into()
    }

    fn accept<'a>(
        &'a self,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            std::net::SocketAddr,
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        Box::pin(async move {
            self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let next = self.accepts.lock().unwrap().pop_front();
            if matches!(next, Some(Err(_))) {
                self.fail_when.notified().await;
            }
            match next {
                Some(Ok(stream)) => {
                    let (reader, writer) = crate::peer_info_reader::tests::halves(stream);
                    Ok((
                        (std::net::Ipv4Addr::new(10, 0, 0, 2), 6881).into(),
                        reader,
                        writer,
                    ))
                }
                Some(Err(e)) => {
                    self.errored.notify_one();
                    Err(e)
                }
                None => std::future::pending().await,
            }
        })
    }
}

impl crate::network::SocketFactory for Scripted {
    fn connect_tcp<'a>(
        &'a self,
        _: std::net::SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        Box::pin(async { Err(std::io::ErrorKind::Unsupported.into()) })
    }

    fn listen_tcp<'a>(
        &'a self,
        _: std::net::SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        Box::pin(async {
            self.listener
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| std::io::ErrorKind::AddrInUse.into())
        })
    }

    fn bind_udp<'a>(
        &'a self,
        _: std::net::SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<std::sync::Arc<dyn crate::network::DatagramSocket>>,
    > {
        Box::pin(async {
            let kernel = crate::network::KernelSockets::new(None).map_err(std::io::Error::other)?;
            crate::network::SocketFactory::bind_udp(
                &kernel,
                (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            )
            .await
        })
    }
}

// A failed accept (out of descriptors, say) rests the listener, not the
// handshakes it already took: one taken before the failure is answered
// while the listener rests, before it is asked to accept again. The
// accept fails only once the torrent is complete, and the answer comes
// before the clock reaches the end of the rest.
// libtorrent `session_impl::on_accept_connection` likewise goes on with
// its connections after a failed accept.
#[tokio::test(start_paused = true)]
async fn a_failed_accept_rests_the_listener_but_not_the_handshakes_in_hand() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = crate::tests::test_util::create_default_random_dir_with_torrents(
        1,
        16384,
        Some("failed_accept"),
    );
    let torrent = crate::create_torrent(
        dir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(16384),
            ..Default::default()
        },
        &crate::spawn_utils::BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap();
    let (ours, mut theirs) = tokio::io::duplex(1 << 16);
    let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let errored = std::sync::Arc::new(tokio::sync::Notify::new());
    let fail_when = std::sync::Arc::new(tokio::sync::Notify::new());
    let scripted = std::sync::Arc::new(Scripted {
        accepts: std::sync::Mutex::new(
            [Ok(ours), Err(std::io::Error::from_raw_os_error(24))].into(),
        ),
        asked: asked.clone(),
        fail_when: fail_when.clone(),
        errored: errored.clone(),
        listener: std::sync::Mutex::new(None),
    });
    let factory = std::sync::Arc::new(Scripted {
        accepts: Default::default(),
        asked: Default::default(),
        fail_when: Default::default(),
        errored: Default::default(),
        listener: std::sync::Mutex::new(Some(Box::new(scripted))),
    });
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            socket_factory: factory,
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    let errored_once = errored.notified();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
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
    handle.wait_until_completed().await.unwrap();

    // The connection was taken, then the next accept failed.
    fail_when.notify_one();
    errored_once.await;
    let rest_ends = tokio::time::Instant::now() + super::ACCEPT_BACKOFF;
    assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 2);

    // Its handshake is answered with the listener resting.
    let mut handshake = [0u8; 68];
    let len = crate::peer_protocol::Handshake::new(handle.info_hash(), super::Id20::new([7; 20]))
        .serialize_unchecked_len(&mut handshake);
    theirs.write_all(&handshake[..len]).await.unwrap();
    let mut answer = [0u8; 68];
    theirs.read_exact(&mut answer).await.unwrap();
    assert!(is_handshake_for(&answer, &handle));
    assert!(tokio::time::Instant::now() < rest_ends);
    assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 2);

    session.stop().await;
}

// Two connections are taken, then the listener has no more to accept.
// The first asks for a torrent the session does not have and is closed.
// The second, still in hand when that check failed, is answered all the
// same: a failed check does not stop the listener looking at the others.
// libtorrent runs each incoming connection on its own
// (`session_impl::incoming_connection`), so one that fails holds up no
// other.
#[tokio::test(start_paused = true)]
async fn a_failed_incoming_check_does_not_hold_up_the_checks_in_hand() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = crate::tests::test_util::create_default_random_dir_with_torrents(
        1,
        16384,
        Some("failed_check"),
    );
    let torrent = crate::create_torrent(
        dir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(16384),
            ..Default::default()
        },
        &crate::spawn_utils::BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap();
    let (stray_ours, mut stray) = tokio::io::duplex(1 << 16);
    let (ours, mut theirs) = tokio::io::duplex(1 << 16);
    let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let scripted = std::sync::Arc::new(Scripted {
        accepts: std::sync::Mutex::new([Ok(stray_ours), Ok(ours)].into()),
        asked: asked.clone(),
        fail_when: Default::default(),
        errored: Default::default(),
        listener: std::sync::Mutex::new(None),
    });
    let factory = std::sync::Arc::new(Scripted {
        accepts: Default::default(),
        asked: Default::default(),
        fail_when: Default::default(),
        errored: Default::default(),
        listener: std::sync::Mutex::new(Some(Box::new(scripted))),
    });
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            socket_factory: factory,
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
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
    handle.wait_until_completed().await.unwrap();

    // Both connections are in hand: the listener is on its third accept,
    // which never returns.
    while asked.load(std::sync::atomic::Ordering::SeqCst) < 3 {
        tokio::task::yield_now().await;
    }

    // The stray's check fails, and its connection is closed.
    let mut handshake = [0u8; 68];
    let len = crate::peer_protocol::Handshake::new(
        super::Id20::new([0xa3; 20]),
        super::Id20::new([6; 20]),
    )
    .serialize_unchecked_len(&mut handshake);
    stray.write_all(&handshake[..len]).await.unwrap();
    let mut rest = Vec::new();
    let _ = stray.read_to_end(&mut rest).await;
    assert!(rest.is_empty());

    // The other is still answered; no new connection comes to wake the
    // listener.
    let len = crate::peer_protocol::Handshake::new(handle.info_hash(), super::Id20::new([7; 20]))
        .serialize_unchecked_len(&mut handshake);
    theirs.write_all(&handshake[..len]).await.unwrap();
    let mut answer = [0u8; 68];
    theirs.read_exact(&mut answer).await.unwrap();
    assert!(is_handshake_for(&answer, &handle));

    session.stop().await;
}

// Sessions given one SharedLimits draw on one connection budget and
// one turn at hashing files: a connection one opens leaves the other
// none under a cap of one, and a check turn one holds the other waits
// for. A limit set on one is set on both.
#[tokio::test]
async fn sessions_sharing_limits_share_one_budget_and_one_check_turn() {
    let shared = super::SharedLimits::new(
        super::SessionLimits {
            connections_limit: 1,
            unchoke_slots_limit: usize::MAX,
            connection_speed: std::num::NonZeroU32::new(30).unwrap(),
        },
        0,
        1,
    );
    let dir = tempfile::tempdir().unwrap();
    let open = |name: &str| {
        let shared = shared.clone();
        let dir = dir.path().join(name);
        async move {
            Session::new_with_opts(
                dir,
                SessionOptions {
                    dht: None,
                    persistence: None,
                    disable_trackers: true,
                    disable_local_service_discovery: true,
                    listen: None,
                    shared_limits: Some(shared),
                    ..crate::test_support::session_options()
                },
            )
            .await
            .unwrap()
        }
    };
    let (a, b) = (open("a").await, open("b").await);

    let held = a.connection_slots.try_open().unwrap();
    assert!(b.connection_slots.try_open().is_none());
    drop(held);
    assert!(b.connection_slots.try_open().is_some());

    let turn = a.concurrent_initialize_semaphore.try_acquire().unwrap();
    assert!(b.concurrent_initialize_semaphore.try_acquire().is_err());
    drop(turn);

    let mut limits = b.limits();
    limits.connections_limit = 3;
    b.set_limits(limits);
    assert_eq!(a.connection_slots.limit(), 3);

    a.stop().await;
    b.stop().await;
}

// A connection from `addr` the listener took and checked, for the
// torrent of `info_hash`; and our end of it.
fn incoming(
    addr: &str,
    info_hash: super::Id20,
) -> (super::CheckedIncomingConnection, tokio::io::DuplexStream) {
    let (ours, theirs) = tokio::io::duplex(1 << 16);
    let (reader, writer) = crate::peer_info_reader::tests::halves(ours);
    let checked = super::CheckedIncomingConnection {
        kind: crate::stream_connect::ConnectionKind::Tcp,
        addr: addr.parse().unwrap(),
        reader,
        writer: writer.into(),
        read_buf: crate::read_buf::ReadBuf::new(),
        handshake: crate::peer_protocol::Handshake::new(info_hash, super::Id20::new([7; 20])),
        encryption: crate::mse::Encryption::None,
        meter: Default::default(),
    };
    (checked, theirs)
}

// libtorrent `session_impl::incoming_connection`: past `connections_limit`
// the session takes up to `connections_slack` more incoming connections.
// Attaching one closes the lowest-ranked connection of the torrent that
// holds the most, when it holds more than the newcomer's own, and the
// newcomer is under the cap in its place (`peer_connection::
// attach_to_torrent`, `session_impl::find_disconnect_candidate_torrent`,
// `torrent::find_lowest_ranking_peer`).
#[tokio::test]
async fn an_incoming_connection_in_the_slack_replaces_one_of_the_fullest_torrent() {
    use crate::torrent_state::AddIncomingPeerResult;
    use tokio::io::AsyncReadExt;

    let (dir, session, fullest) = seeding_with(2, 1, crate::limits::LimitKind::Unlimited).await;
    // Another torrent of the same file, in pieces of another length.
    let other = crate::create_torrent(
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
    let other = session
        .add_torrent(
            AddTorrent::from_bytes(other),
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
    other.wait_until_completed().await.unwrap();
    tokio::time::pause();
    let (fullest_live, other_live) = (fullest.live().unwrap(), other.live().unwrap());
    let slots = &session.connection_slots;

    // The fullest torrent takes both places under the cap.
    let mut ends = Vec::new();
    for addr in ["127.0.0.1:6881", "127.0.0.2:6881"] {
        let slot = slots.try_accept().unwrap();
        assert!(!slots.exceeds_limit(&slot));
        let (checked, theirs) = incoming(addr, fullest.info_hash());
        let added = fullest_live.add_incoming_peer(checked, slot, None).unwrap();
        assert!(matches!(added, AddIncomingPeerResult::Added));
        ends.push(theirs);
    }

    // The next connection takes the slack's one place.
    let slot = slots.try_accept().unwrap();
    assert!(slots.exceeds_limit(&slot));
    assert!(slots.try_accept().is_none());
    let (checked, _newcomer_end) = incoming("127.0.0.3:6881", other.info_hash());
    let newcomer = checked.addr;
    let added = other_live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));
    session.settle_over_limit(&other_live, newcomer.into());

    // It holds a place under the cap, the fullest torrent one connection
    // fewer, and the slack's place is free again.
    assert!(!other_live.exceeds_limit(newcomer.into(), slots));
    assert_eq!(fullest_live.connections().0, 1);
    assert_eq!(slots.in_use(), 2);
    assert!(slots.exceeds_limit(&slots.try_accept().unwrap()));
    // Of its two connections, alike in rank, the first by address closed.
    let mut rest = Vec::new();
    ends[0].read_to_end(&mut rest).await.unwrap();

    session.stop().await;
}

#[tokio::test]
async fn an_incoming_connection_to_a_finished_torrent_with_upload_closed_is_refused() {
    let (_dir, session, handle) = seeding(200, crate::limits::LimitKind::Closed).await;
    assert!(answer(&session, &handle).await.is_empty());

    // With its upload open again, the finished torrent takes the peer.
    handle.set_ratelimits(crate::limits::LimitsConfig::default());
    assert!(is_handshake_for(&answer(&session, &handle).await, &handle));

    session.stop().await;
}

// A session listening on loopback that seeds `torrent` from its payload.
async fn seeding_v2(
    torrent: &crate::core::v2_test_torrent::TestTorrent,
) -> (
    TempDir,
    std::sync::Arc<Session>,
    super::ManagedTorrentHandle,
) {
    seeding_v2_under(torrent, crate::mse::EncryptionPolicy::default()).await
}

// As [`seeding_v2`], the session's connections under `encryption`.
async fn seeding_v2_under(
    torrent: &crate::core::v2_test_torrent::TestTorrent,
    encryption: crate::mse::EncryptionPolicy,
) -> (
    TempDir,
    std::sync::Arc<Session>,
    super::ManagedTorrentHandle,
) {
    let dir = TempDir::with_prefix("v2_identity").unwrap();
    for (name, data) in &torrent.files {
        std::fs::write(dir.path().join(name), data).unwrap();
    }
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            // A test greets the seed from this host more than once.
            allow_multiple_connections_per_ip: true,
            encryption,
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent.torrent_bytes()),
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
    handle.wait_until_completed().await.unwrap();
    (dir, session, handle)
}

fn v2_torrent(hybrid: bool) -> crate::core::v2_test_torrent::TestTorrent {
    use crate::core::v2_test_torrent::{TestTorrent, payload};
    let mut torrent = TestTorrent::new(
        vec![("a", payload(40000, 1)), ("b", payload(9000, 2))],
        16384,
    );
    torrent.hybrid = hybrid;
    torrent
}

// The info hash of a 68-byte handshake answer.
fn answered_hash(answer: &[u8]) -> Option<super::Id20> {
    (answer.len() == 68).then(|| super::Id20::new(answer[28..48].try_into().unwrap()))
}

#[tokio::test]
async fn a_hybrid_answers_a_peer_with_the_hash_it_greeted_with() {
    // libtorrent session_impl::find_torrent keys a hybrid under v1 and the
    // truncated v2; bt_peer_connection::on_receive_handshake answers a
    // peer greeting with the truncated v2 as a v2 peer.
    let torrent = v2_torrent(true);
    let (_dir, session, handle) = seeding_v2(&torrent).await;
    let hashes = handle.info_hashes();
    let v1 = hashes.v1.unwrap();
    let v2 = hashes.v2.unwrap().truncate_for_dht();
    assert_eq!(handle.info_hash(), v1);

    assert_eq!(answered_hash(&greet(&session, v1).await), Some(v1));
    assert_eq!(answered_hash(&greet(&session, v2).await), Some(v2));
    assert!(greet(&session, super::Id20::new([9; 20])).await.is_empty());

    for id in [v1, v2] {
        assert_eq!(
            session.get(TorrentIdOrHash::Hash(id)).map(|t| t.id()),
            Some(handle.id())
        );
    }
    let again = session
        .add_torrent(AddTorrent::from_bytes(torrent.torrent_bytes()), None)
        .await
        .unwrap();
    assert!(matches!(again, AddTorrentResponse::AlreadyManaged(id, _) if id == handle.id()));

    session.stop().await;
}

// Connects to the session, keys an encrypted stream to `skey` and sends
// a handshake for `greeted` in it; returns what came back of the
// session's handshake, nothing when the connection was closed.
async fn greet_encrypted(session: &Session, skey: super::Id20, greeted: super::Id20) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let (read, write) = crate::test_support::tcp(session.listen_addr().unwrap())
        .await
        .unwrap();
    let mut handshake = [0u8; 68];
    let len = crate::peer_protocol::Handshake::new(greeted, super::Id20::new([7; 20]))
        .serialize_unchecked_len(&mut handshake);
    let mut established = crate::mse::initiate(
        read,
        write,
        &skey,
        crate::mse::outgoing_offer(crate::mse::EncryptionPolicy::Require),
        &handshake[..len],
    )
    .await
    .unwrap();
    let mut answer = Vec::new();
    let _ = (&mut established.reader)
        .take(68)
        .read_to_end(&mut answer)
        .await;
    answer
}

#[tokio::test]
async fn an_encrypted_connection_greets_only_the_torrent_its_stream_was_keyed_to() {
    // libtorrent bt_peer_connection::on_receive: the stream key attaches
    // the connection to its torrent, and a handshake for another torrent
    // disconnects with invalid_info_hash. A hybrid's two ids name one
    // torrent.
    let (_dir, session, hybrid) =
        seeding_v2_under(&v2_torrent(true), crate::mse::EncryptionPolicy::Prefer).await;
    let other_torrent = v2_torrent(false);
    let other_dir = TempDir::with_prefix("v2_other").unwrap();
    for (name, data) in &other_torrent.files {
        std::fs::write(other_dir.path().join(name), data).unwrap();
    }
    let other = session
        .add_torrent(
            AddTorrent::from_bytes(other_torrent.torrent_bytes()),
            Some(AddTorrentOptions {
                output_folder: Some(other_dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    other.wait_until_completed().await.unwrap();
    let v1 = hybrid.info_hashes().v1.unwrap();
    let v2 = hybrid.info_hashes().v2.unwrap().truncate_for_dht();
    let other = other.info_hash();

    for (skey, greeted, kept) in [
        (v1, v1, true),
        (v1, v2, true),
        (v2, v1, true),
        (other, other, true),
        (v1, other, false),
        (other, v2, false),
    ] {
        let answer = greet_encrypted(&session, skey, greeted).await;
        assert_eq!(
            answered_hash(&answer),
            kept.then_some(greeted),
            "{skey:?} {greeted:?}"
        );
    }

    session.stop().await;
}

#[tokio::test]
async fn a_handle_gives_its_version_both_hashes_and_its_magnet() {
    // libtorrent `torrent_handle::info_hashes`, `torrent_info::v1`/`v2`
    // and `make_magnet_uri(torrent_handle)`: btih, then btmh, then dn.
    use crate::core::torrent_metainfo::TorrentVersion;
    for (hybrid, version) in [(true, TorrentVersion::Hybrid), (false, TorrentVersion::V2)] {
        let (_dir, session, handle) = seeding_v2(&v2_torrent(hybrid)).await;
        let hashes = handle.info_hashes();
        assert_eq!(hashes.v1.is_some(), hybrid);
        assert_eq!(handle.version(), Some(version));
        let v2 = format!("xt=urn:btmh:1220{}", hashes.v2.unwrap().as_string());
        let expected = match hashes.v1 {
            Some(v1) => format!("magnet:?xt=urn:btih:{}&{v2}&dn=test", v1.as_string()),
            None => format!("magnet:?{v2}&dn=test"),
        };
        let magnet = handle.magnet();
        assert_eq!(magnet.to_string(), expected);
        assert_eq!(magnet.info_hashes(), hashes);
        session.stop().await;
    }
}

#[tokio::test]
async fn a_v2_torrent_goes_by_its_truncated_hash() {
    // libtorrent info_hash_t::get_best and peer_connection::associated_info_hash
    // for a torrent without v1.
    let (_dir, session, handle) = seeding_v2(&v2_torrent(false)).await;
    let hashes = handle.info_hashes();
    assert_eq!(hashes.v1, None);
    let v2 = hashes.v2.unwrap().truncate_for_dht();
    assert_eq!(handle.info_hash(), v2);
    assert_eq!(answered_hash(&greet(&session, v2).await), Some(v2));

    session.stop().await;
}

#[tokio::test]
async fn a_hybrid_moves_a_peer_offering_v2_to_its_v2_hash() {
    // libtorrent bt_peer_connection::on_receive_handshake: an incoming
    // peer setting 0x10 of reserved byte 7 on a hybrid is v2 and is
    // answered with the truncated v2; write_handshake offers the move
    // only to a peer that is not v2.
    let (_dir, session, handle) = seeding_v2(&v2_torrent(true)).await;
    let hashes = handle.info_hashes();
    let v1 = hashes.v1.unwrap();
    let v2 = hashes.v2.unwrap().truncate_for_dht();

    let (_stream, moved) = greet_with(&session, v1, true).await;
    assert_eq!(answered_hash(&moved), Some(v2));
    assert!(!offers_v2(&moved));

    let (_stream, stayed) = greet_with(&session, v1, false).await;
    assert_eq!(answered_hash(&stayed), Some(v1));
    assert!(offers_v2(&stayed));

    session.stop().await;
}

#[tokio::test]
async fn a_v1_torrent_refuses_a_peer_offering_v2() {
    // libtorrent bt_peer_connection::on_receive_handshake: the v2 bit on
    // a torrent without v2 disconnects with invalid_info_hash.
    let (_dir, session, handle) = seeding(200, crate::limits::LimitKind::Unlimited).await;
    let (_stream, refused) = greet_with(&session, handle.info_hash(), true).await;
    assert!(refused.is_empty());
    let (_stream, taken) = greet_with(&session, handle.info_hash(), false).await;
    assert!(is_handshake_for(&taken, &handle));
    assert!(!offers_v2(&taken));

    session.stop().await;
}

#[tokio::test]
async fn a_peer_that_does_not_speak_v2_is_dropped_for_a_hash_request() {
    // libtorrent bt_peer_connection::on_hash_request: a hash message
    // from a peer that is not protocol_v2 disconnects it.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let torrent = v2_torrent(true);
    let (_dir, session, handle) = seeding_v2(&torrent).await;
    let ((mut read, mut write), answer) =
        greet_with(&session, handle.info_hashes().v1.unwrap(), false).await;
    assert_eq!(answered_hash(&answer), handle.info_hashes().v1);

    let request = crate::peer_protocol::Message::HashRequest(crate::peer_protocol::HashRequest {
        root: crate::core::v2_test_torrent::file_hashes(&torrent.files[0].1, 16384)
            .0
            .unwrap(),
        base: 0,
        index: 0,
        count: 2,
        proof_layers: 0,
    });
    let mut frame = [0u8; 53];
    let len = request.serialize(&mut frame, &Default::default).unwrap();
    write.write_all(&frame[..len]).await.unwrap();

    // The session closes the connection: the read ends.
    let mut rest = Vec::new();
    let _ = read.read_to_end(&mut rest).await;

    session.stop().await;
}

/// A magnet's `x.pe` peers are the torrent's initial peers: with no DHT,
/// trackers or other peers, the link's peer alone supplies the metadata
/// and the content.
#[tokio::test(flavor = "multi_thread")]
async fn a_magnets_peer_supplies_its_metadata_and_content() -> anyhow::Result<()> {
    use crate::{
        core::{magnet::Magnet, torrent_metainfo},
        storage::StorageFactory,
        tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
    };
    use anyhow::Context;

    let t = TestTorrent::new("test_magnet_x_pe", 3 * 16384 + 100, 16384).await?;
    let seeder = Session::new_with_opts(t.dir.path().into(), isolated_session_opts(true)).await?;
    seeder
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(t.dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?
        .wait_until_completed()
        .await?;
    let seeder_addr = seeder.listen_addr().context("seeder is not listening")?;

    let mut magnet = Magnet::of(
        torrent_metainfo::torrent_from_bytes(&t.torrent)?.info_hashes,
        None,
        Vec::new(),
    );
    magnet.peers.push(seeder_addr);
    let out = TempDir::with_prefix("test_magnet_x_pe_out")?;
    let downloader =
        Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
    let storage = MemoryStorageFactory::default();
    downloader
        .add_torrent(
            AddTorrent::from_url(magnet.to_string()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?
        .wait_until_completed()
        .await?;
    assert!(storage.log.file_contents(0) == t.content, "content differs");
    Ok(())
}

// An incoming peer's encryption handshake and BitTorrent handshake end
// at the handshake timeout, whatever the peer timeout (libtorrent
// `peer_connection::second_tick`).
#[tokio::test(start_paused = true)]
async fn an_incoming_peer_that_sends_no_handshake_ends_at_the_handshake_timeout() {
    use crate::{
        mse::EncryptionPolicy,
        peer_connection::{DEFAULT_HANDSHAKE_TIMEOUT, PeerConnectionOptions},
        peer_info_reader::tests::halves,
        stream_connect::ConnectionKind,
    };
    for (encryption, step) in [
        (EncryptionPolicy::Disabled, "error reading handshake"),
        (
            EncryptionPolicy::Require,
            "error in the encryption handshake",
        ),
    ] {
        let dir = TempDir::with_prefix("handshake_timeout").unwrap();
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                dht: None,
                persistence: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                encryption,
                connect: Some(super::ConnectionOptions {
                    enable_tcp: true,
                    enable_utp: None,
                    peer_opts: Some(PeerConnectionOptions {
                        read_write_timeout: Some(std::time::Duration::from_secs(120)),
                        ..Default::default()
                    }),
                }),
                ..crate::test_support::session_options()
            },
        )
        .await
        .unwrap();
        let (ours, _theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = halves(ours);
        let start = tokio::time::Instant::now();
        let err = session
            .clone()
            .check_incoming_connection(
                "127.0.0.1:6881".parse().unwrap(),
                ConnectionKind::Tcp,
                false,
                reader,
                writer,
            )
            .await
            .err()
            .unwrap();
        assert!(format!("{err:#}").contains(step), "{err:#}");
        assert_eq!(start.elapsed(), DEFAULT_HANDSHAKE_TIMEOUT, "{encryption:?}");
        session.stop().await;
    }
}

// libtorrent `torrent::set_metadata` keeps `m_peer_list`: the list a
// magnet's metadata connections write into is the one the torrent
// connects from for its whole life. Which handshake a peer last completed
// (`torrent_peer::pe_support`), learnt over a metadata connection, is the
// one the torrent's first connection to it sends
// (`bt_peer_connection::on_connected`).
#[tokio::test(start_paused = true)]
async fn memory_a_metadata_connection_leaves_is_read_by_the_torrents_connector_afterwards() {
    use crate::{
        mse::EncryptionPolicy,
        peer_info_reader::tests::{Dialed, MockPeer, metadata},
        storage::StorageFactory,
        tests::embedder_util::{MemoryStorageFactory, isolated_session_opts},
    };

    let (info, hashes) = metadata(10);
    let (tx, mut dials) = tokio::sync::mpsc::unbounded_channel();
    let dir = TempDir::with_prefix("metadata_memory").unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            socket_factory: Arc::new(Dialed(tx)),
            encryption: EncryptionPolicy::Prefer,
            connect: Some(super::ConnectionOptions {
                enable_tcp: true,
                enable_utp: None,
                peer_opts: None,
            }),
            ..isolated_session_opts(false)
        },
    )
    .await
    .unwrap();
    let add = session.add_torrent(
        AddTorrent::from_url(format!(
            "magnet:?xt=urn:btih:{}",
            hashes.v1.unwrap().as_string()
        )),
        Some(AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            initial_peers: Some(vec!["127.0.0.1:6881".parse().unwrap()]),
            ..Default::default()
        }),
    );
    let peer = async {
        // The peer speaks only the encrypted handshake: the first metadata
        // connection, plain as to any new peer, is refused.
        let (_, stream) = dials.recv().await.unwrap();
        let refused = MockPeer::greet_encrypted(stream, hashes).await;
        assert!(refused.is_err());
        // The next tries the encrypted one and gets the metadata.
        let (_, stream) = dials.recv().await.unwrap();
        let mut fetched = MockPeer::greet_encrypted(stream, hashes).await.unwrap();
        fetched
            .send_extended_handshake(u32::try_from(info.len()).unwrap())
            .await;
        let piece = fetched.next_request().await.unwrap();
        fetched.send_piece(&info, piece).await;
        // Then the torrent connects, encrypted: plain, it would be refused.
        let (_, stream) = dials.recv().await.unwrap();
        let connected = MockPeer::greet_encrypted(stream, hashes).await;
        (connected.map(|_| ()), fetched)
    };

    let (added, (connected, _fetched)) = tokio::join!(add, peer);
    added.unwrap();
    connected.unwrap();
    let stats = session.connector.stats().encryption.snapshot();
    assert_eq!(
        (stats.plaintext, stats.attempts, stats.failures),
        (1, 2, 0),
        "{stats:?}"
    );
    session.stop().await;
}

// The first bytes a new peer receives from us, by policy, as qBittorrent's
// three encryption modes set libtorrent (`pe_enabled`, `pe_forced`,
// `pe_disabled`): the plain BitTorrent handshake where encryption is
// preferred (`torrent_peer::pe_support` starts false) or disabled, the key
// exchange where it is required.
#[tokio::test(start_paused = true)]
async fn a_new_peer_is_sent_the_plain_handshake_first_unless_encryption_is_required() {
    use crate::{
        mse::EncryptionPolicy,
        peer_info_reader::tests::{Dialed, halves, metadata},
        storage::StorageFactory,
        tests::embedder_util::{MemoryStorageFactory, isolated_session_opts},
    };
    use tokio::io::AsyncReadExt;

    const PLAIN: &[u8; 20] = b"\x13BitTorrent protocol";
    let (_, hashes) = metadata(10);
    for (policy, plain) in [
        (EncryptionPolicy::Disabled, true),
        (EncryptionPolicy::Prefer, true),
        (EncryptionPolicy::Require, false),
    ] {
        let (tx, mut dials) = tokio::sync::mpsc::unbounded_channel();
        let dir = TempDir::with_prefix("first_bytes").unwrap();
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                socket_factory: Arc::new(Dialed(tx)),
                encryption: policy,
                connect: Some(super::ConnectionOptions {
                    enable_tcp: true,
                    enable_utp: None,
                    peer_opts: None,
                }),
                ..isolated_session_opts(false)
            },
        )
        .await
        .unwrap();
        let add = session.add_torrent(
            AddTorrent::from_url(format!(
                "magnet:?xt=urn:btih:{}",
                hashes.v1.unwrap().as_string()
            )),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                initial_peers: Some(vec!["127.0.0.1:6881".parse().unwrap()]),
                ..Default::default()
            }),
        );
        let peer = async {
            let (_, stream) = dials.recv().await.unwrap();
            let (mut read, write) = halves(stream);
            let mut first = [0u8; 20];
            read.read_exact(&mut first).await.unwrap();
            (first, read, write)
        };
        let (first, _read, _write) = tokio::select! {
            biased;
            first = peer => first,
            added = add => panic!("{policy:?}: added before a dial: {:?}", added.err()),
        };
        assert_eq!(&first == PLAIN, plain, "{policy:?}");
        session.stop().await;
    }
}
