// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

/// A PEX message adding `added` and dropping `dropped`.
fn pex(added: &[&str], dropped: &[&str]) -> UtPex<ByteBufOwned> {
    let addrs = |a: &[&str]| -> Vec<SocketAddr> { a.iter().map(|a| a.parse().unwrap()).collect() };
    let (added, dropped) = (addrs(added), addrs(dropped));
    let added = added.iter().map(|&addr| PexPeerInfo { flags: 0, addr });
    UtPex::from_peers(added, dropped.iter().copied())
}

// libtorrent `ut_pex_peer_plugin::on_extended`: the added peers are
// candidates, the dropped ones are only forgotten by the sender.
#[tokio::test]
async fn a_pex_message_adds_its_added_peers_and_not_its_dropped_ones() {
    let h = Harness::new("pex_added_and_dropped").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    assert!(h.advertised(&peer).ut_pex.is_some());

    let msg = pex(&["10.0.0.1:6881", "[2001:db8::1]:6881"], &["10.0.0.2:6881"]);
    (&peer)
        .on_received_message(Message::Extended(ExtendedMessage::UtPex(msg.as_borrowed())))
        .await
        .unwrap();
    assert!(h.knows("10.0.0.1:6881"));
    assert!(h.knows("[2001:db8::1]:6881"));
    assert!(!h.knows("10.0.0.2:6881"));
}

impl PeerHandler {
    /// The peer sends us `msg`.
    async fn send_pex(&self, msg: &UtPex<ByteBufOwned>) -> anyhow::Result<()> {
        self.on_received_message(Message::Extended(ExtendedMessage::UtPex(msg.as_borrowed())))
            .await
    }
}

// libtorrent `ut_pex_peer_plugin::on_extended`: a connection's PEX adds at
// most `max_pex_peers` of each family to our list, and a peer on a local
// network only when the connection is on one too.
#[tokio::test]
async fn a_pex_message_adds_at_most_max_pex_peers_and_a_local_peer_only_from_a_local_one() {
    let h = Harness::new("pex_limits").await;
    let (remote, _remote_rx) = h.peer("198.51.100.1:6881");
    let public: Vec<String> = (0..60).map(|i| format!("203.0.113.{i}:6881")).collect();
    let mut added = vec!["192.168.1.1:6881", "10.0.0.1:6881"];
    added.extend(public.iter().map(String::as_str));
    remote.send_pex(&pex(&added, &[])).await.unwrap();
    assert!(!h.knows("192.168.1.1:6881"));
    assert!(!h.knows("10.0.0.1:6881"));
    let known = public.iter().filter(|a| h.knows(a)).count();
    assert_eq!(known, peer::MAX_PEX_PEERS);

    let (local, _local_rx) = h.peer("192.168.1.2:6881");
    local
        .send_pex(&pex(&["192.168.1.1:6881"], &[]))
        .await
        .unwrap();
    assert!(h.knows("192.168.1.1:6881"));
}

// libtorrent `ut_pex_peer_plugin::on_extended`, `m_last_pex`: a peer may
// send six PEX messages a minute; the seventh ends the connection.
#[tokio::test]
async fn a_seventh_pex_message_within_a_minute_ends_the_connection() {
    let h = Harness::new("pex_flood").await;
    tokio::time::pause();
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let msg = pex(&[], &[]);
    for _ in 0..6 {
        peer.send_pex(&msg).await.unwrap();
    }
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(peer.send_pex(&msg).await.is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    peer.send_pex(&msg).await.unwrap();
}

// libtorrent `ut_pex_peer_plugin::on_extended`: a PEX message past 500 KiB
// ends the connection (`pex_message_too_large`), unread.
#[tokio::test]
async fn a_pex_message_past_500_kib_ends_the_connection() {
    let h = Harness::new("pex_too_large").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    // An IPv4 address and its flags byte: seven bytes a peer.
    let past = MAX_PEX_MESSAGE_LEN / 7 + 1;
    let named = |n: usize| {
        let added = (0..n).map(|i| PexPeerInfo {
            flags: 0,
            addr: SocketAddr::from((
                [
                    10,
                    u8::try_from((i >> 16) & 0xff).unwrap(),
                    u8::try_from((i >> 8) & 0xff).unwrap(),
                    u8::try_from(i & 0xff).unwrap(),
                ],
                6881,
            )),
        });
        UtPex::from_peers(added, std::iter::empty())
    };
    assert!(peer.send_pex(&named(past)).await.is_err());
    assert!(!h.knows("10.0.0.0:6881"));
    peer.send_pex(&named(past - 1)).await.unwrap();
    assert!(h.knows("10.0.0.0:6881"));
}

// libtorrent `session::add_extension`: a session without the ut_pex plugin
// does not advertise it, and takes no peers from it.
#[tokio::test]
async fn a_session_without_pex_does_not_advertise_it_nor_take_peers_from_it() {
    let opts = SessionOptions {
        disable_pex: true,
        ..isolated_session_opts(false)
    };
    let h = Harness::build_on(
        "pex_disabled",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let advertised = h.advertised(&peer);
    assert!(advertised.ut_pex.is_none());
    assert!(advertised.ut_metadata.is_some());

    let msg = pex(&["10.0.0.1:6881"], &[]);
    (&peer)
        .on_received_message(Message::Extended(ExtendedMessage::UtPex(msg.as_borrowed())))
        .await
        .unwrap();
    assert!(!h.knows("10.0.0.1:6881"));
}

// A torrent added to take peers from its trackers alone, though its
// metainfo does not mark it private, neither advertises peer exchange nor
// takes peers from it, as a private torrent does not.
#[tokio::test]
async fn a_torrent_that_takes_peers_from_trackers_only_does_not_speak_pex() {
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        trackers_only: true,
        ..Default::default()
    };
    let h = Harness::build_added(
        "pex_trackers_only",
        PIECES,
        0x00,
        isolated_session_opts(false),
        add,
    )
    .await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let advertised = h.advertised(&peer);
    assert!(advertised.ut_pex.is_none());

    peer.send_pex(&pex(&["10.0.0.1:6881"], &[])).await.unwrap();
    assert!(!h.knows("10.0.0.1:6881"));
}

// libtorrent `torrent_flags::disable_pex`: a torrent added without peer
// exchange, in a session that runs it for its other torrents, neither
// advertises it, nor sends a peer that speaks it our messages
// (`ut_pex_plugin::tick` returns), nor takes peers from the peer's
// (`ut_pex_peer_plugin::on_extended` returns). A torrent of the same session
// added with it sends the same peer its first message once the peer has
// been with it ten seconds.
#[tokio::test]
async fn a_torrent_added_without_pex_neither_advertises_sends_nor_reads_it() {
    let add = |disable_pex| AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        disable_pex,
        ..Default::default()
    };
    let opts = isolated_session_opts(false);
    assert!(!opts.disable_pex);
    let off = Harness::build_added("pex_off_per_torrent", PIECES, 0x00, opts, add(true)).await;
    let on = Harness::added_to(
        off._session.clone(),
        TempDir::with_prefix("pex_on_per_torrent").unwrap(),
        "pex_on_per_torrent",
        PIECES,
        &[0x00],
        add(false),
    )
    .await;
    tokio::time::pause();

    // A peer listening on 6881 to list, and one that speaks PEX to tell.
    let speaks_pex = |h: &Harness| {
        let (listed, listed_rx) = h.peer("203.0.113.1:50001");
        let mut hs = ExtendedHandshake::new();
        hs.p = Some(6881);
        (&listed).on_extended_handshake(&hs).unwrap();
        let (told, told_rx) = h.peer("203.0.113.2:50002");
        let mut hs = ExtendedHandshake::new();
        hs.m.ut_pex = Some(1);
        (&told).on_extended_handshake(&hs).unwrap();
        (listed, listed_rx, told, told_rx)
    };
    let is_pex = |request: &WriterRequest| matches!(request, WriterRequest::UtPex(_));

    let (_on_listed, _on_listed_rx, _on_told, mut on_rx) = speaks_pex(&on);
    assert!(on.advertised(&_on_told).ut_pex.is_some());
    loop {
        if is_pex(&on_rx.recv().await.unwrap()) {
            break;
        }
    }

    let (_off_listed, _off_listed_rx, off_told, mut off_rx) = speaks_pex(&off);
    assert!(off.advertised(&off_told).ut_pex.is_none());
    tokio::time::advance(Duration::from_secs(120)).await;
    while let Ok(request) = off_rx.try_recv() {
        assert!(!is_pex(&request));
    }
    off_told
        .send_pex(&pex(&["10.0.0.1:6881"], &[]))
        .await
        .unwrap();
    assert!(!off.knows("10.0.0.1:6881"));
}

// libtorrent `ut_pex_peer_plugin`: a connection has one PEX sender, however
// many extended handshakes name ut_pex. Each sender holds the connection's
// queue, so the senders of the queue count them.
#[tokio::test]
async fn a_connection_starts_one_pex_sender_however_many_handshakes_name_ut_pex() {
    let h = Harness::seeding("pex_sender_once").await;
    let (peer, _rx) = h.peer("203.0.113.2:50002");
    assert!(h.advertised(&peer).ut_pex.is_some());
    let senders = peer.tx.strong_count();
    let mut hs = ExtendedHandshake::new();
    hs.m.ut_pex = Some(1);
    for _ in 0..3 {
        (&peer).on_extended_handshake(&hs).unwrap();
    }
    assert_eq!(peer.tx.strong_count(), senders + 1);
}

// A PEX tick taken late puts the next a full minute after it: a peer held
// up past two ticks is not sent the missed ones back to back.
#[tokio::test]
async fn a_late_pex_tick_puts_the_next_a_full_interval_after_it() {
    use futures::FutureExt;
    tokio::time::pause();
    let period = Duration::from_secs(60);
    let mut interval = super::pex_interval(period);
    interval.tick().await;
    tokio::time::advance(Duration::from_secs(150)).await;
    interval.tick().await;
    let late = tokio::time::Instant::now();
    assert!(interval.tick().now_or_never().is_none());
    tokio::time::advance(period).await;
    assert_eq!(interval.tick().await, late + period);
}

// BEP 27: a private torrent added with the DHT, local discovery and peer
// exchange all left on still takes peers from its trackers alone: it
// advertises no peer exchange, reads none, and looks on neither the DHT nor
// the local network, as libtorrent's `create_ut_pex_plugin`,
// `torrent::should_announce_dht` and `torrent::lsd_announce` each refuse a
// private torrent whatever its flags.
#[tokio::test]
async fn a_private_torrent_added_with_every_discovery_on_does_none() {
    let name = "private_every_discovery_on";
    let out = TempDir::with_prefix(name).unwrap();
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
        .await
        .unwrap();
    let mut torrent = TestTorrent::new(name, (PIECES * PIECE_LENGTH) as usize, PIECE_LENGTH)
        .await
        .unwrap();
    torrent.torrent = crate::tests::e2e_tracker_announces::made_private(&torrent.torrent).unwrap();
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        disable_dht: false,
        disable_lsd: false,
        disable_pex: false,
        ..Default::default()
    };
    let have = BF::from_boxed_slice(vec![0x00].into());
    let h = Harness::adding(session, out, torrent, crate::Checkpoint::pieces(have), add).await;
    assert!(h.live.metadata.info.info().private);

    let (peer, _rx) = h.peer("127.0.0.1:6881");
    assert!(h.advertised(&peer).ut_pex.is_none());
    peer.send_pex(&pex(&["10.0.0.1:6881"], &[])).await.unwrap();
    assert!(!h.knows("10.0.0.1:6881"));
    assert_eq!(
        h.handle().discovery(),
        crate::session::Discovery {
            dht: false,
            lsd: false
        }
    );
}

// BEP 27: a private torrent's connections never send a peer exchange
// message, even to a peer that advertises ut_pex while another peer it could
// list is connected, past the minute of libtorrent's `ut_pex_plugin::tick`
// (libtorrent `create_ut_pex_plugin` adds no plugin to a private torrent).
#[tokio::test]
async fn a_private_torrent_sends_no_pex_to_a_peer_that_speaks_it() {
    let name = "private_sends_no_pex";
    let out = TempDir::with_prefix(name).unwrap();
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
        .await
        .unwrap();
    let mut torrent = TestTorrent::new(name, (PIECES * PIECE_LENGTH) as usize, PIECE_LENGTH)
        .await
        .unwrap();
    torrent.torrent = crate::tests::e2e_tracker_announces::made_private(&torrent.torrent).unwrap();
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        disable_pex: false,
        ..Default::default()
    };
    let have = BF::from_boxed_slice(vec![0x00].into());
    let h = Harness::adding(session, out, torrent, crate::Checkpoint::pieces(have), add).await;
    assert!(h.live.metadata.info.info().private);
    tokio::time::pause();

    // A peer listening on 6881 to list, and one that speaks PEX to tell.
    let (listed, _listed_rx) = h.peer("203.0.113.1:50001");
    let mut hs = ExtendedHandshake::new();
    hs.p = Some(6881);
    (&listed).on_extended_handshake(&hs).unwrap();
    let (told, mut told_rx) = h.peer("203.0.113.2:50002");
    let senders = told.tx.strong_count();
    let mut hs = ExtendedHandshake::new();
    hs.m.ut_pex = Some(1);
    (&told).on_extended_handshake(&hs).unwrap();
    assert!(h.advertised(&told).ut_pex.is_none());
    // No PEX sender holds the connection's queue, so none can ever send.
    assert_eq!(told.tx.strong_count(), senders);

    tokio::time::advance(Duration::from_secs(120)).await;
    while let Ok(request) = told_rx.try_recv() {
        assert!(!matches!(request, WriterRequest::UtPex(_)));
    }
}

// libtorrent `bt_peer_connection::write_handshake` sets the DHT bit only
// when the session runs a DHT node; `write_dht_port` sends nothing without
// one.
#[tokio::test]
async fn a_session_without_dht_sets_no_dht_bit_and_sends_no_port() {
    let h = Harness::new("dht_port_no_dht").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    assert_eq!((&peer).dht_port(), None);
    (&peer)
        .on_handshake(handshake(true), ConnectionKind::Tcp, false)
        .unwrap();
    (&peer)
        .on_received_message(Message::Port(6881))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());
}

// libtorrent `peer_connection::incoming_dht_port` adds the named node on
// each PORT; here a connection's repeat of the port it named last adds
// nothing, and another port is a node to add.
#[tokio::test]
async fn a_repeated_dht_port_names_no_new_node() {
    let h = Harness::new("dht_port_repeat").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    let port = |port| peer.on_received_message(Message::Port(port));
    let news = |port| {
        h.peers
            .with_live_mut(peer.addr, "test", |live| live.dht_node_named(port))
            .unwrap()
    };
    port(6881).await.unwrap();
    port(6881).await.unwrap();
    assert!(!news(6881));
    port(6882).await.unwrap();
    assert!(!news(6882));
    assert!(news(6881));
}

// libtorrent `bt_peer_connection::write_dht_port` after the handshake to a
// peer with the DHT bit, `on_dht_port` answering a PORT from a peer
// without it, once per connection; `peer_connection::incoming_dht_port`
// and `node::add_node`: the named node is pinged and, on its reply, joins
// the routing table. The DHT runs over a socket the test holds, with no
// bootstrap host, so the ping is the one datagram it sends and the test
// writes the node's answer.
#[tokio::test]
async fn our_dht_port_goes_once_to_a_dht_peer_and_its_port_adds_its_node() {
    let udp = Arc::new(HeldUdp::default());
    let opts = SessionOptions {
        dht: Some(crate::DhtSessionConfig {
            bootstrap_addrs: Some(Vec::new()),
            ..Default::default()
        }),
        socket_factory: udp.clone(),
        ..isolated_session_opts(false)
    };
    let h = Harness::build_on(
        "dht_port",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let ours = h._session.get_dht().unwrap().clone();
    let our_port = ours.listen_addr().port();
    let mut node_side = udp.take(ours.listen_addr());

    let (bit, mut bit_rx) = h.peer("127.0.0.2:6881");
    assert_eq!((&bit).dht_port(), Some(our_port));
    (&bit)
        .on_handshake(handshake(true), ConnectionKind::Tcp, false)
        .unwrap();
    assert!(matches!(
        bit_rx.try_recv(),
        Ok(WriterRequest::Message(Message::Port(p))) if p == our_port
    ));

    let (no_bit, mut no_bit_rx) = h.peer("127.0.0.1:6882");
    (&no_bit)
        .on_handshake(handshake(false), ConnectionKind::Tcp, false)
        .unwrap();
    assert!(no_bit_rx.try_recv().is_err());

    // The node the peer names, on the peer's address.
    let node = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 6890));
    let their_id = Id20::new([0x33; 20]);
    (&no_bit)
        .on_received_message(Message::Port(node.port()))
        .await
        .unwrap();
    assert!(matches!(
        no_bit_rx.try_recv(),
        Ok(WriterRequest::Message(Message::Port(p))) if p == our_port
    ));
    (&no_bit)
        .on_received_message(Message::Port(node.port()))
        .await
        .unwrap();
    assert!(no_bit_rx.try_recv().is_err());

    let (ping, to) = node_side.sent.recv().await.unwrap();
    assert_eq!(to, node);
    let ping = crate::bencode::dyn_from_bytes::<crate::bencode::ByteBufOwned>(&ping).unwrap();
    assert_eq!(dict_bytes(&ping, "y"), b"q");
    assert_eq!(dict_bytes(&ping, "q"), b"ping");
    let tid = dict_bytes(&ping, "t");

    let entered = ours.with_routing_tables(|v4, _| v4.entered());
    let mut entry = std::pin::pin!(entered.notified());
    entry.as_mut().enable();
    let mut answer = b"d1:rd2:id20:".to_vec();
    answer.extend_from_slice(&their_id.0);
    answer.extend_from_slice(format!("e1:t{}:", tid.len()).as_bytes());
    answer.extend_from_slice(tid);
    answer.extend_from_slice(b"1:y1:re");
    node_side.arriving.send((answer, node)).unwrap();
    entry.await;
    assert!(ours.with_routing_tables(|v4, _| {
        v4.iter().any(|n| n.id() == their_id && n.addr() == node)
    }));
}

// libtorrent `bt_peer_connection::write_dht_port` (bt_peer_connection.cpp:
// 333-342, v2.0.15) names `listen_socket_t::udp_external_port`
// (session_impl.hpp:239-247): the UDP port a mapper got for the socket the
// DHT runs over, NAT-PMP's before UPnP's over IPv4, else the socket's own
// port. The PORT goes once, at the handshake: a peer keeps the port it was
// told, and a new peer is told the port as it is then.
#[tokio::test]
async fn the_port_message_names_the_udp_port_mapped_for_the_dhts_socket() {
    use crate::upnp::{PortMapped, PortmapProtocol, PortmapTransport};
    let opts = SessionOptions {
        listen: Some(crate::ListenerOptions {
            mode: crate::ListenerMode::TcpAndUtp,
            listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            ..Default::default()
        }),
        dht: Some(crate::DhtSessionConfig {
            bootstrap_addrs: Some(Vec::new()),
            port: Some(0),
            persistence: None,
        }),
        ipv4_only: true,
        ..isolated_session_opts(false)
    };
    let h = Harness::build_on(
        "dht_port_mapped",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let listen = h._session.listen_addr().unwrap().port();
    assert_eq!(h._session.get_dht().unwrap().listen_addr().port(), listen);
    let mapped = |protocol, transport, external_port| PortMapped {
        transport,
        protocol,
        local_port: listen,
        external_port,
        external_ip: None,
        error: None,
    };
    let mut next = 0u8;
    let mut told = || {
        next += 1;
        let (peer, mut rx) = h.peer(&format!("127.0.0.{next}:6881"));
        (&peer)
            .on_handshake(handshake(true), ConnectionKind::Tcp, false)
            .unwrap();
        let Ok(WriterRequest::Message(Message::Port(port))) = rx.try_recv() else {
            panic!("no PORT");
        };
        (peer, rx, port)
    };

    // No mapping: the DHT's own port, the listener's.
    let (_first, mut first_rx, port) = told();
    assert_eq!(port, listen);
    // A TCP mapping is not the DHT's.
    h._session.port_mapped(&mapped(
        PortmapProtocol::Tcp,
        PortmapTransport::NatPmp,
        Some(4000),
    ));
    assert_eq!(told().2, listen);
    // UPnP maps the UDP port, then NAT-PMP, which goes first, maps it too.
    h._session.port_mapped(&mapped(
        PortmapProtocol::Udp,
        PortmapTransport::Upnp,
        Some(3001),
    ));
    assert_eq!(told().2, 3001);
    h._session.port_mapped(&mapped(
        PortmapProtocol::Udp,
        PortmapTransport::NatPmp,
        Some(3000),
    ));
    assert_eq!(told().2, 3000);
    // The mapping changes its port, then NAT-PMP's is lost: UPnP's again.
    h._session.port_mapped(&mapped(
        PortmapProtocol::Udp,
        PortmapTransport::NatPmp,
        Some(3002),
    ));
    assert_eq!(told().2, 3002);
    h._session.port_mapped(&mapped(
        PortmapProtocol::Udp,
        PortmapTransport::NatPmp,
        None,
    ));
    assert_eq!(told().2, 3001);
    // Both lost: the DHT's own port again.
    h._session
        .port_mapped(&mapped(PortmapProtocol::Udp, PortmapTransport::Upnp, None));
    assert_eq!(told().2, listen);
    // The first peer was told once, and is not told again.
    assert!(first_rx.try_recv().is_err());
}

// A DHT on a socket of its own is not on the listener's mapped socket: its
// PORT names its own port, whatever UDP port a mapper got for the listener.
#[tokio::test]
async fn the_port_message_ignores_a_mapping_of_a_socket_the_dht_does_not_run_over() {
    use crate::upnp::{PortMapped, PortmapProtocol, PortmapTransport};
    let udp = Arc::new(HeldUdp::default());
    let opts = SessionOptions {
        dht: Some(crate::DhtSessionConfig {
            bootstrap_addrs: Some(Vec::new()),
            ..Default::default()
        }),
        socket_factory: udp.clone(),
        ..isolated_session_opts(false)
    };
    let h = Harness::build_on(
        "dht_port_own_socket",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let own = h._session.get_dht().unwrap().listen_addr().port();
    h._session.port_mapped(&PortMapped {
        transport: PortmapTransport::NatPmp,
        protocol: PortmapProtocol::Udp,
        local_port: own,
        external_port: Some(3000),
        external_ip: None,
        error: None,
    });
    let (peer, mut rx) = h.peer("127.0.0.2:6881");
    (&peer)
        .on_handshake(handshake(true), ConnectionKind::Tcp, false)
        .unwrap();
    assert!(matches!(
        rx.try_recv(),
        Ok(WriterRequest::Message(Message::Port(p))) if p == own
    ));
}

impl Harness {
    /// A live peer at `addr` whose extended handshake names ut_holepunch
    /// when `holepunch` holds, and the queue of what we send it.
    fn holepunch_peer(&self, addr: &str, holepunch: bool) -> (PeerHandler, PeerRx) {
        let (peer, rx) = self.peer(addr);
        let mut hs = ExtendedHandshake::new();
        hs.m.ut_holepunch = holepunch.then_some(9);
        (&peer).on_extended_handshake(&hs).unwrap();
        (peer, rx)
    }
}

/// The holepunch messages we queued for the peer since last asked, draining
/// its queue.
fn holepunch_sent(rx: &mut PeerRx) -> Vec<UtHolepunch> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|r| match r {
            WriterRequest::Message(Message::Extended(ExtendedMessage::UtHolepunch(m))) => Some(m),
            _ => None,
        })
        .collect()
}

async fn send_holepunch(peer: &PeerHandler, msg: UtHolepunch) {
    peer.on_received_message(Message::Extended(ExtendedMessage::UtHolepunch(msg)))
        .await
        .unwrap();
}

/// An address on 127.0.0.1 nothing listens on, so a connection to it fails.
fn closed_port() -> SocketAddr {
    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
}

// libtorrent `bt_peer_connection::write_extensions` names ut_holepunch on
// every connection.
#[tokio::test]
async fn every_connection_advertises_ut_holepunch() {
    let h = Harness::new("holepunch_advertised").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    assert_eq!(
        h.advertised(&peer).ut_holepunch,
        Some(MY_EXTENDED_UT_HOLEPUNCH)
    );
}

// libtorrent `bt_peer_connection::on_holepunch`, `rendezvous`: the sender
// is told to connect to the target, the target to the sender.
#[tokio::test]
async fn a_rendezvous_tells_each_peer_to_connect_to_the_other() {
    let h = Harness::new("holepunch_rendezvous").await;
    let (a, mut a_rx) = h.holepunch_peer("203.0.113.1:6881", true);
    let (_b, mut b_rx) = h.holepunch_peer("203.0.113.2:6881", true);
    send_holepunch(&a, UtHolepunch::Rendezvous(sock("203.0.113.2:6881"))).await;
    assert_eq!(
        holepunch_sent(&mut a_rx),
        [UtHolepunch::Connect(sock("203.0.113.2:6881"))]
    );
    assert_eq!(
        holepunch_sent(&mut b_rx),
        [UtHolepunch::Connect(sock("203.0.113.1:6881"))]
    );
    assert_eq!(holepunches(&h, HolepunchEvent::Introduced), 1);
}

/// How many holepunch steps of `event` the harness's session has counted.
fn holepunches(h: &Harness, event: HolepunchEvent) -> u64 {
    h.live.session_stats.network.snapshot().holepunch[event as usize]
}

// libtorrent `bt_peer_connection::on_holepunch`, `rendezvous`: not
// connected, no support and no self are errors back to the sender; a local
// peer and one that is not are not introduced; and a peer that did not
// advertise the extension is not heard.
#[tokio::test]
async fn a_rendezvous_that_cannot_be_made_is_refused_or_ignored() {
    use ut_holepunch::error::{NO_SELF, NO_SUPPORT, NOT_CONNECTED};
    let h = Harness::new("holepunch_refused").await;
    let (a, mut a_rx) = h.holepunch_peer("203.0.113.1:6881", true);
    let (_mute, mut mute_rx) = h.holepunch_peer("203.0.113.2:6881", false);
    let (_local, mut local_rx) = h.holepunch_peer("192.168.1.2:6881", true);
    let (deaf, mut deaf_rx) = h.holepunch_peer("203.0.113.3:6881", false);

    for (target, code) in [
        ("203.0.113.9:6881", NOT_CONNECTED),
        ("203.0.113.2:6881", NO_SUPPORT),
        ("203.0.113.1:6881", NO_SELF),
    ] {
        send_holepunch(&a, UtHolepunch::Rendezvous(sock(target))).await;
        assert_eq!(
            holepunch_sent(&mut a_rx),
            [UtHolepunch::Error(sock(target), code)]
        );
    }
    send_holepunch(&a, UtHolepunch::Rendezvous(sock("192.168.1.2:6881"))).await;
    assert_eq!(holepunch_sent(&mut a_rx), []);
    assert_eq!(holepunch_sent(&mut local_rx), []);
    assert_eq!(holepunch_sent(&mut mute_rx), []);

    send_holepunch(&deaf, UtHolepunch::Rendezvous(sock("203.0.113.1:6881"))).await;
    assert_eq!(holepunch_sent(&mut deaf_rx), []);
    assert_eq!(holepunch_sent(&mut a_rx), []);
}

// libtorrent `bt_peer_connection::on_holepunch`, `connect`: we connect to
// the peer over uTP, so a session without uTP takes no connect; a local
// address is taken only from a local peer.
#[tokio::test]
async fn a_connect_admits_the_peer_only_over_utp_and_a_local_one_only_from_a_local_peer() {
    let target = closed_port();
    let without = Harness::new("holepunch_connect_no_utp").await;
    let (from, _rx) = without.holepunch_peer("127.0.0.1:6881", true);
    send_holepunch(&from, UtHolepunch::Connect(target)).await;
    assert!(!without.knows(&target.to_string()));

    let opts = SessionOptions {
        listen: Some(crate::ListenerOptions {
            mode: crate::ListenerMode::UtpOnly,
            listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            ..Default::default()
        }),
        ..isolated_session_opts(false)
    };
    let with = Harness::build_on(
        "holepunch_connect_utp",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let (remote, _remote_rx) = with.holepunch_peer("203.0.113.1:6881", true);
    send_holepunch(&remote, UtHolepunch::Connect(target)).await;
    assert!(!with.knows(&target.to_string()));

    let (from, _rx) = with.holepunch_peer("127.0.0.1:6881", true);
    send_holepunch(&from, UtHolepunch::Connect(target)).await;
    assert!(with.knows(&target.to_string()));
    assert_eq!(holepunches(&with, HolepunchEvent::ConnectIgnored), 1);
    assert_eq!(holepunches(&with, HolepunchEvent::Connect), 1);
}

/// A PEX message naming `addr` with `flags`.
fn pex_flagged(addr: SocketAddr, flags: u8) -> Vec<u8> {
    let SocketAddr::V4(v4) = addr else {
        panic!("an IPv4 address")
    };
    let mut b = b"d5:added6:".to_vec();
    b.extend_from_slice(&v4.ip().octets());
    b.extend_from_slice(&v4.port().to_be_bytes());
    b.extend_from_slice(b"7:added.f1:");
    b.push(flags);
    b.push(b'e');
    b
}

// libtorrent `peer_connection::connect_failed`: a failed connection to a
// peer PEX flagged as speaking the holepunch extension sends a rendezvous
// for it to `torrent::find_introducer`, a holepunch peer whose PEX named it.
#[tokio::test]
async fn a_failed_connection_asks_the_peer_whose_pex_named_it_to_introduce_us() {
    let h = Harness::new("holepunch_introducer").await;
    // On another host than the target: no connection to its address stands
    // in the way of the connect.
    let (introducer, mut rx) = h.holepunch_peer("127.0.0.2:6881", true);
    let target = closed_port();
    let msg = pex_flagged(target, extended::ut_pex::PEX_HOLEPUNCH);
    let msg = crate::bencode::from_bytes::<UtPex<ByteBuf>>(&msg).unwrap();
    // The PEX is taken in before the connect loop runs: the test runs on
    // one thread and yields only below.
    (&introducer)
        .on_received_message(Message::Extended(ExtendedMessage::UtPex(msg)))
        .await
        .unwrap();
    assert!(h.knows(&target.to_string()));

    // The connect loop connects to the target, which refuses.
    let sent = loop {
        match rx.recv().await.unwrap() {
            WriterRequest::Message(Message::Extended(ExtendedMessage::UtHolepunch(m))) => break m,
            _ => continue,
        }
    };
    assert_eq!(sent, UtHolepunch::Rendezvous(target));
}

// libtorrent `peer_connection::connect_failed` is the only place a
// rendezvous is sent: a connection to a holepunch peer that opened and then
// failed its handshake reached the peer, and asks no one to introduce us.
// The target answers our key exchange with a public key of zero, which
// fails the encrypted handshake (a failure that counts against it).
#[tokio::test]
async fn a_connection_that_failed_its_handshake_asks_for_no_introduction() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let h = Harness::new("holepunch_no_introducer").await;
    let (introducer, mut rx) = h.holepunch_peer("127.0.0.2:6881", true);
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let target = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let _ = stream.write_all(&[0; 96]).await;
                let mut sink = Vec::new();
                let _ = stream.read_to_end(&mut sink).await;
            });
        }
    });
    let msg = pex_flagged(target, extended::ut_pex::PEX_HOLEPUNCH);
    let msg = crate::bencode::from_bytes::<UtPex<ByteBuf>>(&msg).unwrap();
    (&introducer)
        .on_received_message(Message::Extended(ExtendedMessage::UtPex(msg)))
        .await
        .unwrap();
    assert!(h.knows(&target.to_string()));

    // The connect loop dials the target; its connection fails once open.
    while h.peers.with_peer(target.into(), |p| p.failcount()).unwrap() == 0 {
        tokio::task::yield_now().await;
    }
    assert_eq!(holepunch_sent(&mut rx), []);
}

// libtorrent `ut_pex_plugin::tick`, `send_peer`: our PEX message lists each
// connected peer whose listening address we know, where it listens, flagged
// a seed, connected over uTP, speaking the holepunch extension, as it is to
// us; the next one drops a peer gone. A peer not on a local network is not
// told of one that is (it would drop it, `ut_pex_peer_plugin::on_extended`).
#[tokio::test]
async fn our_pex_message_lists_each_connected_peer_where_it_listens_flagged_for_what_it_is_to_us() {
    use extended::ut_pex::{PEX_HOLEPUNCH, PEX_SEED, PEX_UTP};
    let h = Harness::new("pex_flags_sent").await;
    let listening_on_6881 = |peer: &PeerHandler, holepunch: bool| {
        let mut hs = ExtendedHandshake::new();
        hs.p = Some(6881);
        if holepunch {
            hs.m.ut_holepunch = Some(9);
        }
        peer.on_extended_handshake(&hs).unwrap();
    };
    let utp = |addr: &str| {
        let (tx, rx) = unbounded_channel();
        h.peers
            .add_incoming(sock(addr), Id20::default(), tx, ConnectionKind::Utp, 6881)
            .unwrap();
        rx
    };
    let (seed, _seed_rx) = h.peer("203.0.113.1:50001");
    listening_on_6881(&seed, false);
    let (plain, _plain_rx) = h.new_peer("203.0.113.2:50002");
    listening_on_6881(&plain, false);
    let _utp_rx = utp("203.0.113.3:6881");
    let (holepunch, _holepunch_rx) = h.new_peer("203.0.113.4:50004");
    listening_on_6881(&holepunch, true);
    // Never says where it listens.
    let (_silent, _silent_rx) = h.new_peer("203.0.113.5:50005");
    let _local_rx = utp("192.168.1.3:6881");

    let sorted = |msg: UtPex<ByteBufOwned>| {
        let mut wire = Vec::new();
        crate::bencode::bencode_serialize_to_writer(msg.as_borrowed(), &mut wire).unwrap();
        let msg = crate::bencode::from_bytes::<UtPex<ByteBuf>>(&wire).unwrap();
        let mut added: Vec<_> = msg.added_peers().map(|p| (p.addr, p.flags)).collect();
        let mut dropped: Vec<_> = msg.dropped_peers().map(|p| p.addr).collect();
        added.sort();
        dropped.sort();
        (added, dropped)
    };

    let remote = sock("198.51.100.1:6881");
    let mut told = HashSet::new();
    let (added, dropped) = sorted(h.next_pex_message(remote, &mut told).unwrap());
    assert_eq!(
        added,
        [
            (sock("203.0.113.1:6881"), PEX_SEED),
            (sock("203.0.113.2:6881"), 0),
            (sock("203.0.113.3:6881"), PEX_UTP),
            (sock("203.0.113.4:6881"), PEX_HOLEPUNCH),
        ]
    );
    assert!(dropped.is_empty());
    assert!(h.next_pex_message(remote, &mut told).is_none());

    let mut local_told = HashSet::new();
    let (added, _) = sorted(
        h.next_pex_message(sock("192.168.1.9:6881"), &mut local_told)
            .unwrap(),
    );
    assert_eq!(added.len(), 5);
    assert!(added.contains(&(sock("192.168.1.3:6881"), PEX_UTP)));

    h.peers.drop_peer(plain.addr);
    let (added, dropped) = sorted(h.next_pex_message(remote, &mut told).unwrap());
    assert!(added.is_empty());
    assert_eq!(dropped, [sock("203.0.113.2:6881")]);
}

// A private torrent is found through its trackers only, and its metadata
// leaves only in the torrent file: on a session that runs a DHT node, its
// handshake offers no DHT and it sends no PORT, even to a peer that names
// its node, and its extended handshake names neither ut_metadata nor the metadata's
// size, nor is a request for a piece of it answered.
#[tokio::test]
async fn a_private_torrent_offers_no_dht_and_no_metadata() {
    let udp = Arc::new(HeldUdp::default());
    let opts = SessionOptions {
        dht: Some(crate::DhtSessionConfig {
            bootstrap_addrs: Some(Vec::new()),
            ..Default::default()
        }),
        socket_factory: udp.clone(),
        ..isolated_session_opts(false)
    };
    let name = "private_no_dht_no_metadata";
    let out = TempDir::with_prefix(name).unwrap();
    let session = Session::new_with_opts(out.path().into(), opts)
        .await
        .unwrap();
    let mut torrent = TestTorrent::new(name, (PIECES * PIECE_LENGTH) as usize, PIECE_LENGTH)
        .await
        .unwrap();
    torrent.torrent = crate::tests::e2e_tracker_announces::made_private(&torrent.torrent).unwrap();
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        ..Default::default()
    };
    let have = BF::from_boxed_slice(vec![0xc0].into());
    let h = Harness::adding(session, out, torrent, crate::Checkpoint::pieces(have), add).await;
    assert!(h.live.metadata.info.info().private);
    assert!(h._session.get_dht().is_some());

    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    assert_eq!((&peer).dht_port(), None);
    (&peer)
        .on_handshake(handshake(true), ConnectionKind::Tcp, false)
        .unwrap();
    (&peer)
        .on_received_message(Message::Port(6890))
        .await
        .unwrap();
    (&peer)
        .on_received_message(Message::Extended(ExtendedMessage::UtMetadata(
            UtMetadata::Request(0),
        )))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());

    let mut hs = ExtendedHandshake::new();
    (&peer).update_my_extended_handshake(&mut hs).unwrap();
    assert_eq!(hs.m.ut_metadata, None);
    assert_eq!(hs.metadata_size, None);
}

// A web seed is its table entry, at no address. A rendezvous, or a PEX
// entry, naming the address web seeds were once given, `[100::]:0`, names
// a swarm peer nobody is connected to: the sender is told so, and the seed
// hears nothing and its entry is as it was.
#[tokio::test]
async fn an_address_in_the_discard_prefix_reaches_no_web_seed() {
    let h = Harness::new("discard_prefix_no_web_seed").await;
    let (seed, mut seed_rx, _link) = h.web_seed();
    std::iter::from_fn(|| seed_rx.try_recv().ok()).for_each(drop);
    let entry = |h: &Harness| {
        h.peers
            .with_peer(seed.addr, |p| (p.get_state().name(), p.sources()))
            .unwrap()
    };
    let before = entry(&h);
    let (a, mut a_rx) = h.holepunch_peer("203.0.113.1:6881", true);
    let discard = sock("[100::]:0");

    send_holepunch(&a, UtHolepunch::Rendezvous(discard)).await;
    assert_eq!(
        holepunch_sent(&mut a_rx),
        [UtHolepunch::Error(
            discard,
            ut_holepunch::error::NOT_CONNECTED
        )]
    );
    a.send_pex(&pex(&["[100::]:0"], &[])).await.unwrap();

    assert!(seed_rx.try_recv().is_err(), "the seed was sent something");
    assert_eq!(entry(&h), before);
    assert_eq!(h.shared.web_seeds.list().len(), 1);
    assert!(!h.banned(&seed));
}
