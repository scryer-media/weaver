// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

// libtorrent `torrent::do_pause` only disconnects: `torrent::m_peer_list`,
// with each `torrent_peer`'s `banned` and `trust_points`, is the list the
// resumed torrent works from.
#[tokio::test]
async fn a_pause_keeps_the_bans_and_the_trust_of_the_peer_list() {
    let h = Harness::new("pause_keeps_peer_list").await;
    let banned = sock("203.0.113.1:6881");
    let (peer, _rx) = h.peer("203.0.113.2:6881");
    h.peers.ban(banned.ip());
    h.peers
        .with_peer_mut(peer.addr, "test", |p| p.piece_failed(false, &h.peers))
        .unwrap();
    let trust = h.peers.with_peer(peer.addr, |p| p.trust()).unwrap();
    assert_ne!(trust, Default::default());

    let session = &h._session;
    let handle = session.get(h.shared.id.into()).unwrap();
    session.pause(&handle).await.unwrap();
    session.unpause(&handle).await.unwrap();
    let resumed = handle.live().unwrap();
    assert!(!Arc::ptr_eq(&resumed, &h.live));
    assert!(resumed.peers.is_banned(banned.ip()));
    assert_eq!(
        resumed.peers.with_peer(peer.addr, |p| p.trust()),
        Some(trust)
    );
}

#[tokio::test]
async fn a_seed_ends_a_connection_whose_unchoked_peer_asks_for_nothing_for_a_minute() {
    let h = Harness::seeding("no_request").await;
    tokio::time::pause();
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    // Forty seconds on the peer asks for a block, and the minute runs again
    // from when the block is taken to be sent.
    tokio::time::advance(Duration::from_secs(40)).await;
    peer.on_received_message(Message::Request(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 0));
    let chunk = h.chunk(0, 0);
    let sent = tokio::time::Instant::now();
    let claimed = h.peers.with_live_mut(peer.addr, "send", |live| {
        let claimed = live.claim_upload(&chunk);
        live.upload_done(&chunk);
        claimed
    });
    assert_eq!(claimed, Some(true));

    peer.time_out_idle().await.unwrap();
    assert!(tokio::time::Instant::now().duration_since(sent) > peer::NO_REQUEST_TIMEOUT);
}

#[tokio::test]
async fn peers_that_want_nothing_of_each_other_part_after_ten_minutes_when_connections_run_short() {
    // Five connections in all: the torrent's connections run short.
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        peer_limit: Some(5),
        ..Default::default()
    };
    let h = Harness::build_added(
        "no_interest",
        PIECES,
        0xc0,
        isolated_session_opts(false),
        add,
    )
    .await;
    tokio::time::pause();
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;

    // Five minutes on the peer is interested a while; the ten minutes run
    // from when it no longer is.
    tokio::time::advance(Duration::from_secs(300)).await;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    peer.on_received_message(Message::NotInterested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Choke);
    let uninterested = tokio::time::Instant::now();

    peer.time_out_idle().await.unwrap();
    assert!(tokio::time::Instant::now().duration_since(uninterested) > peer::INACTIVITY_TIMEOUT);
}

// libtorrent `bt_peer_connection::write_extensions` says upload_only in the
// extended handshake of a torrent that only uploads, and `torrent::finished`
// tells each peer whose handshake named upload_only once it does
// (`torrent::send_upload_only`).
#[tokio::test]
async fn a_finished_torrent_tells_the_peers_that_take_upload_only() {
    let h = Harness::new("upload_only").await;
    let (seed, mut seed_rx) = h.peer("127.0.0.1:6881");
    let (takes, mut takes_rx) = h.peer_with("127.0.0.2:6881", &[]).await;
    let (deaf, mut deaf_rx) = h.peer_with("127.0.0.3:6881", &[]).await;
    let mut names_it = ExtendedHandshake::new();
    names_it.m.upload_only = Some(3);
    (&takes).on_extended_handshake(&names_it).unwrap();
    let mut names_none = ExtendedHandshake::new();
    names_none.m.upload_only = None;
    (&deaf).on_extended_handshake(&names_none).unwrap();

    let says = |peer: &PeerHandler| {
        let mut hs = ExtendedHandshake::new();
        peer.update_my_extended_handshake(&mut hs).unwrap();
        hs.upload_only
    };
    assert_eq!(says(&takes), None);

    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(seed.task_peer_chunk_requester());
    seed.on_i_am_unchoked();
    let first = h
        .request_every_chunk(&seed, &mut requester, &mut seed_rx)
        .await;
    drop(requester);
    let rest = chunks(first, 1..CHUNKS_PER_PIECE)
        .into_iter()
        .chain(chunks(1 - first, 0..CHUNKS_PER_PIECE));
    for request in rest {
        h.deliver(&seed, request).await;
    }

    let upload_only = |sent: &WriterRequest| match sent {
        WriterRequest::Message(Message::Extended(ExtendedMessage::UploadOnly(enabled))) => {
            Some(*enabled)
        }
        _ => None,
    };
    loop {
        let sent = takes_rx
            .recv()
            .await
            .expect("the peer's writer queue closed");
        if let Some(enabled) = upload_only(&sent) {
            assert!(enabled);
            break;
        }
    }
    // Every peer was told at once, so the one that named no id would have
    // been sent it by now.
    while let Ok(sent) = deaf_rx.try_recv() {
        assert_eq!(upload_only(&sent), None);
    }
    assert_eq!(says(&takes), Some(1));
}

// libtorrent `peer_list::connection_closed`: only a connection that failed
// counts against the peer; one we asked to close did not fail.
#[tokio::test]
async fn a_connection_we_asked_to_close_counts_no_failure() {
    let h = Harness::new("asked_to_close").await;
    let mut failcounts = Vec::new();
    for (addr, error) in [
        ("127.0.0.1:6881", crate::Error::Disconnect),
        ("127.0.0.2:6881", crate::Error::TestDisconnect),
    ] {
        let (peer, _rx) = h.peer(addr);
        let counters = peer.counters.clone();
        peer.on_peer_died(Some(error)).unwrap();
        let failcount = h
            .peers
            .with_peer(sock(addr).into(), |p| p.failcount())
            .unwrap();
        failcounts.push((failcount, counters.errors.load(Ordering::Relaxed)));
    }
    assert_eq!(failcounts, [(0, 0), (1, 1)]);
}

// A connect our own uTP socket turned away never reached the peer: it counts
// no failure. One the peer left unanswered does.
#[tokio::test]
async fn a_connect_refused_on_our_side_counts_no_failure() {
    let h = Harness::new("refused_locally").await;
    let mut failcounts = Vec::new();
    for (addr, error) in [
        (
            "127.0.0.1:6881",
            crate::Error::UtpConnect(weaver_utp::Error::ConnectionIdsExhausted),
        ),
        (
            "127.0.0.2:6881",
            crate::Error::UtpConnect(weaver_utp::Error::DispatcherDead),
        ),
        ("127.0.0.3:6881", crate::Error::UtpDisabled),
        (
            "127.0.0.4:6881",
            crate::Error::UtpConnect(weaver_utp::Error::ConnectTimedOut),
        ),
    ] {
        let (peer, _rx) = h.peer(addr);
        let counters = peer.counters.clone();
        peer.on_peer_died(Some(error)).unwrap();
        let failcount = h
            .peers
            .with_peer(sock(addr).into(), |p| p.failcount())
            .unwrap();
        failcounts.push((failcount, counters.errors.load(Ordering::Relaxed)));
    }
    assert_eq!(failcounts, [(0, 0), (0, 0), (0, 0), (1, 1)]);
}

// libtorrent `peer_list::is_connect_candidate`: once we only upload, a peer
// whose connection showed it to be a seed is no connect candidate, and any
// other still is (`settings_pack::seeding_outgoing_connections`, on by
// default).
#[tokio::test]
async fn a_finished_torrent_still_dials_a_leecher_and_not_a_seed() {
    let h = Harness::seeding("finished_dials").await;
    let listens = |peer: &PeerHandler| {
        let mut hs = ExtendedHandshake::new();
        hs.p = Some(u32::from(peer.addr.addr().unwrap().port()));
        peer.on_extended_handshake(&hs).unwrap();
    };
    let (leecher, _leecher_rx) = h.new_peer("127.0.0.1:6881");
    leecher.on_bitfield(&bitfield_of(&[])).unwrap();
    listens(&leecher);
    let (seed, _seed_rx) = h.peer("127.0.0.2:6881");
    listens(&seed);
    leecher.on_peer_died(None).unwrap();
    seed.on_peer_died(None).unwrap();

    let due = tokio::time::Instant::now() + peer::MIN_RECONNECT_TIME;
    assert_eq!(
        h.peers.connect_next(due).map(|(addr, ..)| addr),
        Some(sock("127.0.0.1:6881"))
    );
    assert!(h.peers.connect_next(due).is_none());
}

// libtorrent `session_impl::set_ip_filter` and `update_privileged_ports`
// run `torrent::ip_filter_updated` and `privileged_port_updated`
// (`peer_list::apply_ip_filter`, `apply_port_filter`): an idle peer the new
// filter refuses leaves the list at once, a connected one has its connection
// closed and leaves once it has; from then on `torrent::add_peer` takes no
// peer the filter refuses or that is on a privileged port.
#[tokio::test]
async fn a_changed_ip_filter_takes_the_peers_it_refuses_off_the_list() {
    let h = Harness::new("refilter").await;
    let (connected, mut connected_rx) = h.peer("127.0.0.1:6881");
    for addr in ["203.0.113.1:6881", "203.0.113.2:80", "203.0.113.3:6881"] {
        h.add_peer(sock(addr), PeerSourceFlag::Tracker);
    }
    let listed = |addr: &str| h.peers.with_peer(sock(addr).into(), |_| ()).is_some();

    h._session.set_ip_filter(crate::IpFilter {
        blocked: Arc::new(crate::IpRanges::new(
            [
                "127.0.0.1".parse().unwrap().."127.0.0.2".parse().unwrap(),
                "203.0.113.1".parse().unwrap().."203.0.113.2".parse().unwrap(),
            ],
            [],
        )),
        no_privileged_ports: true,
        trackers: true,
    });
    assert!(!listed("203.0.113.1:6881"));
    assert!(!listed("203.0.113.2:80"));
    assert!(listed("203.0.113.3:6881"));
    assert!(
        std::iter::from_fn(|| connected_rx.try_recv().ok())
            .any(|r| matches!(r, WriterRequest::Disconnect(Ok(()))))
    );
    assert!(listed("127.0.0.1:6881"));
    connected.on_peer_died(None).unwrap();
    assert!(!listed("127.0.0.1:6881"));

    for addr in ["203.0.113.1:6881", "203.0.113.4:443", "203.0.113.4:6881"] {
        h.add_peer(sock(addr), PeerSourceFlag::Tracker);
    }
    assert!(!listed("203.0.113.1:6881"));
    assert!(!listed("203.0.113.4:443"));
    assert!(listed("203.0.113.4:6881"));
}

// libtorrent `peer_list::compare_peer`: among candidates that failed as
// often, one on a local network is dialed first.
#[tokio::test]
async fn a_local_peer_is_dialed_before_a_global_one() {
    let h = Harness::new("local_first").await;
    h.peers
        .add_peer(sock("1.1.1.1:6881"), PeerSourceFlag::Tracker);
    h.peers
        .add_peer(sock("192.168.1.1:6881"), PeerSourceFlag::Tracker);
    let now = tokio::time::Instant::now();
    let dialed: Vec<_> =
        std::iter::from_fn(|| h.peers.connect_next(now).map(|(addr, ..)| addr)).collect();
    assert_eq!(dialed, [sock("192.168.1.1:6881"), sock("1.1.1.1:6881")]);
}

/// Every dial is reported to the test with when it began, and none is
/// answered; a datagram socket is bound on loopback, as for [`DialedTcp`].
struct UnansweredDials(tokio::sync::mpsc::UnboundedSender<tokio::time::Instant>);

impl crate::network::SocketFactory for UnansweredDials {
    fn connect_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        Box::pin(async move {
            let _ = self.0.send(tokio::time::Instant::now());
            std::future::pending().await
        })
    }

    fn listen_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        Box::pin(async { Err(std::io::ErrorKind::Unsupported.into()) })
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>>
    {
        Box::pin(async {
            crate::test_support::sockets()
                .bind_udp((std::net::Ipv4Addr::LOCALHOST, 0).into())
                .await
        })
    }
}

// libtorrent `peer_connection::second_tick`: a connect is given up after
// `peer_connect_timeout` and 3 s more for each failure in a row; the peer
// is dialed again (failcount + 1) reconnect times after that
// (`peer_list::find_connect_candidates`).
#[tokio::test(start_paused = true)]
async fn the_connect_timeout_grows_by_three_seconds_per_failure() {
    let (tx, mut dials) = unbounded_channel();
    let opts = SessionOptions {
        socket_factory: Arc::new(UnansweredDials(tx)),
        ..isolated_session_opts(false)
    };
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        initial_peers: Some(vec![sock("127.0.0.1:6881")]),
        ..Default::default()
    };
    let _h = Harness::build_added("connect_timeout_grows", PIECES, 0x00, opts, add).await;
    let mut at = Vec::new();
    for _ in 0..3 {
        at.push(dials.recv().await.unwrap());
    }
    let timeout = crate::peer_connection::DEFAULT_CONNECT_TIMEOUT;
    assert_eq!(at[1] - at[0], timeout + peer::MIN_RECONNECT_TIME * 2);
    assert_eq!(
        at[2] - at[1],
        timeout + Duration::from_secs(3) + peer::MIN_RECONNECT_TIME * 3
    );
}

/// A torrent of at most `peer_limit` connections in a session of at most
/// `connections_limit`.
async fn turnover_harness(name: &str, peer_limit: usize, connections_limit: usize) -> Harness {
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        peer_limit: Some(peer_limit),
        ..Default::default()
    };
    let opts = SessionOptions {
        connections_limit,
        ..isolated_session_opts(false)
    };
    Harness::build_added(name, PIECES, 0x00, opts, add).await
}

// libtorrent `session_impl::second_tick`: below the session's cutoff, a
// torrent holding 90% of its own connections turns 4% of them over, at
// least one, and no more than it has connect candidates to replace them;
// the one it would keep least goes (`torrent::disconnect_peers`).
#[tokio::test]
async fn a_torrent_at_its_connection_cutoff_turns_its_least_worth_peer_over() {
    let h = turnover_harness("turnover_torrent", 10, 200).await;
    let _held = h.peer_semaphore.clone().try_acquire_many_owned(9).unwrap();
    let (kept, mut kept_rx) = h.peer("127.0.0.1:6881");
    let (_least, mut least_rx) = h.peer("127.0.0.2:6881");
    h.peers.with_live_mut(kept.addr, "test", |live| {
        live.interest(true, tokio::time::Instant::now())
    });
    let closed = |rx: &mut PeerRx| {
        std::iter::from_fn(|| rx.try_recv().ok())
            .any(|r| matches!(r, WriterRequest::Disconnect(Ok(()))))
    };

    // Nobody to take its place: nobody goes.
    h._session.turn_peers_over();
    assert!(!closed(&mut least_rx));

    h.peers
        .add_peer(sock("203.0.113.1:6881"), PeerSourceFlag::Tracker);
    h._session.turn_peers_over();
    assert!(closed(&mut least_rx));
    assert!(!closed(&mut kept_rx));
}

// libtorrent `session_impl::second_tick`: when the session holds 90% of its
// connections, the torrent with the most turns peers over, however far
// below its own cap.
#[tokio::test]
async fn a_session_at_its_connection_cutoff_turns_over_the_busiest_torrents_peers() {
    let h = turnover_harness("turnover_session", 128, 10).await;
    let _held: Vec<_> = (0..9)
        .map(|_| h._session.connection_slots.try_open().unwrap())
        .collect();
    let (_peer, mut rx) = h.peer("127.0.0.1:6881");
    h.peers
        .add_peer(sock("203.0.113.1:6881"), PeerSourceFlag::Tracker);

    h._session.turn_peers_over();
    assert!(
        std::iter::from_fn(|| rx.try_recv().ok())
            .any(|r| matches!(r, WriterRequest::Disconnect(Ok(()))))
    );
}

/// Whether the connection whose writer queue `rx` reads was asked to close.
fn asked_to_close(rx: &mut PeerRx) -> bool {
    std::iter::from_fn(|| rx.try_recv().ok())
        .any(|r| matches!(r, WriterRequest::Disconnect(Ok(()))))
}

// libtorrent `torrent::attach_peer`: at the torrent's cap, while more than
// one in ten of its most connections are still connecting, the connect begun
// longest ago gives way to an incoming connection.
#[tokio::test]
async fn an_incoming_connection_at_the_cap_replaces_the_oldest_connect_while_many_connect() {
    let h = turnover_harness("displace_connect", 10, 200).await;
    tokio::time::pause();
    let mut held = Vec::new();
    let mut connects = Vec::new();
    for addr in ["203.0.113.1:6881", "203.0.113.2:6881"] {
        h.peers.add_peer(sock(addr), PeerSourceFlag::Tracker);
        let (addr, rx, tx) = h.peers.connect_next(tokio::time::Instant::now()).unwrap();
        held.push(h.hold(addr.into(), &tx));
        connects.push((addr, rx));
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    let mut live = Vec::new();
    for i in 1..=8 {
        let (peer, rx) = h.new_peer(&format!("198.51.100.{i}:6881"));
        held.push(h.hold(peer.addr, &peer.tx));
        live.push(rx);
    }

    let (checked, slot, _theirs) = h.incoming("192.0.2.1:6881");
    let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));
    let [(oldest, oldest_rx), (newer, newer_rx)] = &mut connects[..] else {
        unreachable!()
    };
    assert_eq!(*oldest, sock("203.0.113.1:6881"));
    assert!(asked_to_close(oldest_rx));
    assert!(!asked_to_close(newer_rx));
    assert!(live.iter_mut().all(|rx| !asked_to_close(rx)));
    assert!(!h.holds_place("203.0.113.1:6881"));
    assert!(h.places.lock().contains_key(&PeerHandle::from(*newer)));
    assert!(h.holds_place("192.0.2.1:6881"));
    assert_eq!(h.connections(), (10, 10));
}

// libtorrent `torrent::attach_peer`, `find_lowest_ranking_peer`: at the
// torrent's cap, with few connecting, an incoming connection replaces the
// connection that ranks lowest when it ranks higher. Here a connection on
// parole ranks lowest; the newcomer, never seen, is on none.
#[tokio::test]
async fn an_incoming_connection_at_the_cap_replaces_a_connection_that_ranks_lower() {
    let h = turnover_harness("displace_lowest", 2, 200).await;
    tokio::time::pause();
    let (on_parole, mut on_parole_rx) = h.new_peer("203.0.113.1:6881");
    h.peers
        .with_peer_mut(on_parole.addr, "test", |p| p.piece_failed(false, &h.peers))
        .unwrap();
    let (kept, mut kept_rx) = h.new_peer("203.0.113.2:6881");
    let _held = [
        h.hold(on_parole.addr, &on_parole.tx),
        h.hold(kept.addr, &kept.tx),
    ];

    let (checked, slot, _theirs) = h.incoming("192.0.2.1:6881");
    let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));
    assert!(asked_to_close(&mut on_parole_rx));
    assert!(!asked_to_close(&mut kept_rx));
    assert!(!h.holds_place("203.0.113.1:6881"));
    assert!(h.holds_place("192.0.2.1:6881"));
    assert_eq!(h.connections(), (2, 2));
}

// libtorrent `torrent::attach_peer`: an incoming connection that ranks no
// higher than the lowest-ranked connection of a torrent at its cap is
// refused, and every connection stays.
#[tokio::test]
async fn an_incoming_connection_at_the_cap_that_ranks_no_higher_is_refused() {
    let h = turnover_harness("displace_none", 2, 200).await;
    tokio::time::pause();
    let (a, mut a_rx) = h.new_peer("203.0.113.1:6881");
    let (b, mut b_rx) = h.new_peer("203.0.113.2:6881");
    let _held = [h.hold(a.addr, &a.tx), h.hold(b.addr, &b.tx)];

    let (checked, slot, _theirs) = h.incoming("192.0.2.1:6881");
    let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(
        added,
        AddIncomingPeerResult::ConcurrencyLimitReached
    ));
    assert!(!asked_to_close(&mut a_rx));
    assert!(!asked_to_close(&mut b_rx));
    assert!(!h.holds_place("192.0.2.1:6881"));
    assert_eq!(h.connections(), (2, 2));
}

// libtorrent `bt_peer_connection::init_bt_handshake` marks a connection
// encrypted whichever side opened it, and `ut_pex_plugin::tick` flags a peer
// by it (`supports_encryption()`): an incoming connection the listener took
// encrypted is advertised encrypted, a plaintext one is not.
#[tokio::test]
async fn an_incoming_encrypted_connection_is_advertised_encrypted_in_our_pex() {
    use crate::peer_protocol::extended::ut_pex::PEX_ENCRYPTION;
    use tokio::io::AsyncReadExt;
    let h = turnover_harness("incoming_pex_encryption", 10, 200).await;
    tokio::time::pause();
    let mut ends = Vec::new();
    for (addr, encrypted) in [("192.0.2.1:6881", true), ("192.0.2.2:6881", false)] {
        let (mut checked, slot, mut theirs) = h.incoming(addr);
        checked.encryption = if encrypted {
            crate::mse::Encryption::Rc4
        } else {
            crate::mse::Encryption::None
        };
        let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
        assert!(matches!(added, AddIncomingPeerResult::Added));
        // Our handshake and the first byte after it, which is written once
        // the connection has told the torrent how the peer greeted.
        let mut written = [0; 69];
        theirs.read_exact(&mut written).await.unwrap();
        ends.push(theirs);
    }

    let flags = |addr| {
        h.peers
            .with_live(sock(addr).into(), |live| live.advertised_pex_flags())
            .unwrap()
    };
    assert_eq!(flags("192.0.2.1:6881") & PEX_ENCRYPTION, PEX_ENCRYPTION);
    assert_eq!(flags("192.0.2.2:6881") & PEX_ENCRYPTION, 0);
}

/// Every dial reaches the test as [`Dialed`](crate::peer_info_reader::tests::Dialed)
/// does; a datagram socket, which only the idle UDP tracker client holds,
/// is bound on loopback.
struct DialedTcp(crate::peer_info_reader::tests::Dialed);

impl crate::network::SocketFactory for DialedTcp {
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
        self.0.connect_tcp(addr)
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        self.0.listen_tcp(addr)
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>>
    {
        Box::pin(async {
            crate::test_support::sockets()
                .bind_udp((std::net::Ipv4Addr::LOCALHOST, 0).into())
                .await
        })
    }
}

// A dial whose route is crowded out of TCP room: every look leaves a want,
// counted while it is held; the dial still seats, as one over a transport the
// route has room on does.
struct CrowdedDial {
    dialed: crate::peer_info_reader::tests::Dialed,
    held: Arc<std::sync::atomic::AtomicUsize>,
    gone: Arc<tokio::sync::Notify>,
}

struct CountedWant(
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<tokio::sync::Notify>,
);

impl Drop for CountedWant {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.1.notify_one();
    }
}

impl crate::network::SocketFactory for CrowdedDial {
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
        self.dialed.connect_tcp(addr)
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        self.dialed.listen_tcp(addr)
    }

    fn tcp_room(&self) -> crate::network::TcpRoom {
        self.held.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        crate::network::TcpRoom::Crowded(crate::network::TcpWant::new(CountedWant(
            Arc::clone(&self.held),
            Arc::clone(&self.gone),
        )))
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>>
    {
        self.dialed.bind_udp(addr)
    }
}

// The torrent's last candidate is dialled with the route crowded out of TCP
// room, so the look leaves a want, and the dial seats: the peer comes live.
// With the dial ended and no one left to dial, no TCP dial is possible, so
// the connect loop looks again and lets the want go while the peer is still
// live. Nothing else would wake it: the far end says nothing after its
// handshake, and only the connection's closing on its silence would.
#[tokio::test(start_paused = true)]
async fn the_want_goes_once_the_last_dial_comes_live() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (tx, mut dialed) = unbounded_channel();
    let held = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gone = Arc::new(tokio::sync::Notify::new());
    let opts = SessionOptions {
        socket_factory: Arc::new(CrowdedDial {
            dialed: crate::peer_info_reader::tests::Dialed(tx),
            held: Arc::clone(&held),
            gone: Arc::clone(&gone),
        }),
        connect: Some(crate::stream_connect::ConnectionOptions {
            enable_tcp: true,
            enable_utp: None,
            utp_on_demand: false,
            peer_opts: None,
        }),
        ..isolated_session_opts(false)
    };
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        initial_peers: Some(vec![sock("127.0.0.1:6881")]),
        ..Default::default()
    };
    let h = Harness::build_added("last_dial_live", PIECES, 0x00, opts, add).await;
    let (_, mut far) = dialed.recv().await.unwrap();
    assert_eq!(
        held.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the look left a want"
    );
    let mut greeting = [0; 68];
    far.read_exact(&mut greeting).await.unwrap();
    let mut answer = [0; 68];
    let len = Handshake::new(h.shared.info_hash, crate::torrent_state::Id20::new([7; 20]))
        .serialize_unchecked_len(&mut answer);
    far.write_all(&answer[..len]).await.unwrap();

    loop {
        let notified = gone.notified();
        if held.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            break;
        }
        notified.await;
    }
    assert!(!h.tcp_wants.holds());
    assert_eq!(
        h.peers.stats().live,
        1,
        "the want went while the peer was live"
    );
    assert_eq!(h.peers.dialling(), 0);
}

/// A torrent with no piece, of a session whose connections take
/// `peer_opts`, that dials one peer; the far end of that dial, read past
/// our handshake.
async fn dialing_with(
    name: &str,
    peer_opts: crate::PeerConnectionOptions,
) -> (Harness, tokio::io::DuplexStream) {
    use tokio::io::AsyncReadExt;
    let (tx, mut dialed) = unbounded_channel();
    let opts = SessionOptions {
        socket_factory: Arc::new(DialedTcp(crate::peer_info_reader::tests::Dialed(tx))),
        connect: Some(crate::stream_connect::ConnectionOptions {
            enable_tcp: true,
            enable_utp: None,
            utp_on_demand: false,
            peer_opts: Some(peer_opts),
        }),
        ..isolated_session_opts(false)
    };
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        initial_peers: Some(vec!["127.0.0.1:6881".parse().unwrap()]),
        ..Default::default()
    };
    let h = Harness::build_added(name, PIECES, 0x00, opts, add).await;
    let (_, mut peer) = dialed.recv().await.unwrap();
    let mut ours = [0; 68];
    peer.read_exact(&mut ours).await.unwrap();
    (h, peer)
}

// libtorrent `peer_connection::second_tick`: a torrent's connection still
// in its handshake ends at the session's handshake timeout, whatever the
// peer timeout.
#[tokio::test(start_paused = true)]
async fn a_torrent_connection_ends_an_unanswered_handshake_at_the_session_timeout() {
    use tokio::io::AsyncReadExt;
    let handshake_timeout = Duration::from_secs(3);
    let (_h, mut peer) = dialing_with(
        "session_handshake_timeout",
        crate::PeerConnectionOptions {
            read_write_timeout: Some(Duration::from_secs(120)),
            handshake_timeout: Some(handshake_timeout),
            ..Default::default()
        },
    )
    .await;
    let start = tokio::time::Instant::now();
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    assert_eq!(start.elapsed(), handshake_timeout);
}

// libtorrent `peer_connection::keep_alive`: a torrent's connection sends a
// keepalive at the session's keepalive interval.
#[tokio::test(start_paused = true)]
async fn a_torrent_connection_sends_keepalives_at_the_session_interval() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let keep_alive = Duration::from_secs(7);
    let (h, mut peer) = dialing_with(
        "session_keep_alive",
        crate::PeerConnectionOptions {
            read_write_timeout: Some(Duration::from_secs(120)),
            keep_alive_interval: Some(keep_alive),
            ..Default::default()
        },
    )
    .await;
    let mut theirs = [0; 68];
    let len =
        Handshake::new(h.shared.info_hash, Id20::new([2; 20])).serialize_unchecked_len(&mut theirs);
    peer.write_all(&theirs[..len]).await.unwrap();
    // Whatever goes out first, the keepalive is the first empty frame.
    let start = tokio::time::Instant::now();
    loop {
        let frame = peer.read_u32().await.unwrap();
        if frame == 0 {
            break;
        }
        let mut body = vec![0; frame as usize];
        peer.read_exact(&mut body).await.unwrap();
    }
    assert_eq!(start.elapsed(), keep_alive);
}

/// A torrent of no piece whose connections never time out a quiet peer
/// and send a keepalive each minute, so a connection closes only when the
/// torrent closes it.
async fn quiet_peers_harness(name: &str) -> Harness {
    let opts = SessionOptions {
        connect: Some(crate::stream_connect::ConnectionOptions {
            enable_tcp: true,
            enable_utp: None,
            utp_on_demand: false,
            peer_opts: Some(crate::PeerConnectionOptions {
                read_write_timeout: Some(Duration::from_secs(3600)),
                keep_alive_interval: Some(Duration::from_secs(60)),
                ..Default::default()
            }),
        }),
        ..isolated_session_opts(false)
    };
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        ..Default::default()
    };
    Harness::build_added(name, PIECES, 0x00, opts, add).await
}

/// A connection from `addr`, of the client whose peer id is `peer_id`,
/// past its handshake: our handshake and the first frame after it, which
/// is written once the connection has told the torrent how the peer
/// greeted, have reached the peer. Returns the peer's end.
async fn handshaken_as(h: &Harness, addr: &str, peer_id: Id20) -> tokio::io::DuplexStream {
    use tokio::io::AsyncReadExt;
    let (checked, slot, mut theirs) = h.incoming_as(addr, peer_id);
    let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));
    let mut handshake = [0; 68];
    theirs.read_exact(&mut handshake).await.unwrap();
    let len = theirs.read_u32().await.unwrap();
    theirs.read_exact(&mut vec![0; len as usize]).await.unwrap();
    theirs
}

/// Reads the frames we send on `end` until a keepalive, which says the
/// connection stays open with nothing more to say, or until it closes:
/// true for the keepalive.
async fn stays_open(end: &mut tokio::io::DuplexStream) -> bool {
    use tokio::io::AsyncReadExt;
    loop {
        match end.read_u32().await {
            Ok(0) => return true,
            Ok(len) => {
                end.read_exact(&mut vec![0; len as usize]).await.unwrap();
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return false,
            Err(e) => panic!("{e}"),
        }
    }
}

// libtorrent `bt_peer_connection::on_receive`, "read_peer_id", with
// `allow_multiple_connections_per_pid` off (`settings_pack.cpp`): a
// connection that sends the peer id of a connected peer is the same client
// at another address, and one of the two closes. The end with the greater
// id keeps the connection it made, so both ends close the same one. Here
// the client's id is below ours: it keeps no connection it made to us, so
// its second is closed right after our handshake, and the first stays.
#[tokio::test(start_paused = true)]
async fn a_second_connection_from_a_client_whose_id_is_below_ours_is_closed() {
    use tokio::io::AsyncReadExt;
    let h = quiet_peers_harness("duplicate_peer_id_below").await;
    let client = Id20::new([0; 20]);
    assert!(client < h.shared.peer_id);
    let mut first = handshaken_as(&h, "192.0.2.1:6881", client).await;

    let (checked, slot, mut second) = h.incoming_as("[2001:db8::1]:6881", client);
    let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));
    let mut written = Vec::new();
    second.read_to_end(&mut written).await.unwrap();
    // Our handshake, and nothing after it.
    assert_eq!(written.len(), 68);
    assert!(stays_open(&mut first).await);
}

// As above, a client whose id is above ours keeps the connection it made
// to us last: its first connection closes, the second stays.
#[tokio::test(start_paused = true)]
async fn a_client_whose_id_is_above_ours_keeps_its_second_connection_and_loses_the_first() {
    let h = quiet_peers_harness("duplicate_peer_id_above").await;
    let client = Id20::new([0xff; 20]);
    assert!(client > h.shared.peer_id);
    let mut first = handshaken_as(&h, "192.0.2.1:6881", client).await;
    let mut second = handshaken_as(&h, "[2001:db8::1]:6881", client).await;

    assert!(!stays_open(&mut first).await);
    assert!(stays_open(&mut second).await);
}

// libtorrent `peer_connection::disconnect`: a connection that reaches
// ourselves bans the peer it was made to, "don't try to connect to ourself
// again" (`bt_peer_connection::on_receive` knows it by the peer id it
// reads). The torrent dials its own address: the far end answers with our
// own peer id and is closed, the torrent's one connection goes to another
// peer next, and the address is no connect candidate at any time after.
#[tokio::test(start_paused = true)]
async fn a_connection_to_ourselves_is_closed_and_its_address_never_dialed_again() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (tx, mut dialed) = unbounded_channel();
    let opts = SessionOptions {
        socket_factory: Arc::new(DialedTcp(crate::peer_info_reader::tests::Dialed(tx))),
        connect: Some(crate::stream_connect::ConnectionOptions {
            enable_tcp: true,
            enable_utp: None,
            utp_on_demand: false,
            peer_opts: None,
        }),
        ..isolated_session_opts(false)
    };
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        initial_peers: Some(vec![sock("127.0.0.1:6881")]),
        peer_limit: Some(1),
        ..Default::default()
    };
    let h = Harness::build_added("self_connection", PIECES, 0x00, opts, add).await;
    let (addr, mut ourselves) = dialed.recv().await.unwrap();
    assert_eq!(addr, sock("127.0.0.1:6881"));
    let mut greeting = [0; 68];
    ourselves.read_exact(&mut greeting).await.unwrap();
    let mut answer = [0; 68];
    let len =
        Handshake::new(h.shared.info_hash, h.shared.peer_id).serialize_unchecked_len(&mut answer);
    ourselves.write_all(&answer[..len]).await.unwrap();
    let mut rest = Vec::new();
    ourselves.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty());

    // The torrent holds one connection, so the next is made only once the
    // one to ourselves has settled.
    h.peers
        .add_peer(sock("127.0.0.2:6881"), PeerSourceFlag::Tracker);
    let (next, _other) = dialed.recv().await.unwrap();
    assert_eq!(next, sock("127.0.0.2:6881"));
    assert!(h.peers.is_banned(addr.ip()));
    assert_eq!(
        h.peers.next_candidate(tokio::time::Instant::now()),
        peers::NextCandidate::None
    );
}

// A look finds the torrent's route crowded out; the receiver ends once the
// want it left is let go. The look lands before the connect loop's wait, and
// the wait lets the want go on entry, or in it, and leaves none.
fn crowded_out(h: &Harness) -> tokio::sync::oneshot::Receiver<()> {
    let (gone, released) = tokio::sync::oneshot::channel::<()>();
    h.tcp_wants.looked(crate::network::TcpRoom::Crowded(
        crate::network::TcpWant::new(gone),
    ));
    released
}

// A torrent crowded out of TCP room and then held at its own peer limit
// dials no one, so it lets its want go: the route stops counting it as
// pressing, and usenet's share may grow back.
#[tokio::test]
async fn a_torrent_waiting_at_its_peer_limit_holds_no_tcp_want() {
    let h = turnover_harness("tcp_want_peer_limit", 1, 200).await;
    let _held = h.peer_semaphore.clone().try_acquire_owned().unwrap();
    h.peers
        .add_peer(sock("203.0.113.1:6881"), PeerSourceFlag::Tracker);
    let released = crowded_out(&h);
    assert!(released.await.is_err(), "the want is let go");
    assert!(!h.tcp_wants.holds());
    assert_eq!(h.peers.stats().connecting, 0, "held at the peer limit");
}

// A free look lets a want go.
#[test]
fn a_free_look_lets_the_tcp_want_go() {
    let wants = crate::stream_connect::TcpWants::default();
    let (gone, mut released) = tokio::sync::oneshot::channel::<()>();
    wants.looked(crate::network::TcpRoom::Crowded(
        crate::network::TcpWant::new(gone),
    ));
    assert!(wants.holds());
    wants.looked(crate::network::TcpRoom::Free);
    assert!(!wants.holds());
    assert_eq!(
        released.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    );
}

// A pause lets a held want go, and no crowded look after it leaves one.
#[test]
fn a_pause_lets_the_tcp_want_go_and_no_look_after_it_leaves_one() {
    let wants = crate::stream_connect::TcpWants::default();
    let crowded = || {
        let (gone, released) = tokio::sync::oneshot::channel::<()>();
        wants.looked(crate::network::TcpRoom::Crowded(
            crate::network::TcpWant::new(gone),
        ));
        released
    };
    let mut before = crowded();
    assert!(wants.holds());
    wants.close();
    assert!(!wants.holds());
    assert_eq!(
        before.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    );
    let mut after = crowded();
    assert!(!wants.holds(), "a look after the pause left a want");
    assert_eq!(
        after.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    );
}

// A wait in which the connect loop can dial no one lets the want go on
// entry, and a look that lands while it lasts leaves none; once the loop is
// out of it, a crowded look holds a want again.
#[tokio::test]
async fn a_look_during_a_wait_that_can_dial_no_one_leaves_no_tcp_want() {
    let wants = crate::stream_connect::TcpWants::default();
    let crowded = || {
        let (gone, released) = tokio::sync::oneshot::channel::<()>();
        wants.looked(crate::network::TcpRoom::Crowded(
            crate::network::TcpWant::new(gone),
        ));
        released
    };
    let mut before = crowded();
    assert!(wants.holds());
    let mut during = None;
    wants
        .idle(async {
            assert_eq!(
                before.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed),
                "the wait lets the want go on entry"
            );
            during = Some(crowded());
        })
        .await;
    assert_eq!(
        during.expect("a look in the wait").try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed),
        "a look in the wait leaves no want"
    );
    assert!(!wants.holds());
    let _after = crowded();
    assert!(wants.holds(), "out of the wait, a look holds a want");
}

// Once the torrent is paused, a dial still under way that finds its route
// crowded out leaves no want: it is let go at once.
#[tokio::test]
async fn a_dial_that_ends_after_the_pause_leaves_no_tcp_want() {
    let h = Harness::new("tcp_want_after_pause").await;
    let handle = h._session.get(h.shared.id.into()).unwrap();
    h._session.pause(&handle).await.unwrap();
    let (gone, mut released) = tokio::sync::oneshot::channel::<()>();
    h.tcp_wants.looked(crate::network::TcpRoom::Crowded(
        crate::network::TcpWant::new(gone),
    ));
    assert!(!h.tcp_wants.holds());
    assert_eq!(
        released.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    );
}
