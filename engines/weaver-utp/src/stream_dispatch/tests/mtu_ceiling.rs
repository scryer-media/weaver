// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How high a stream probes: the link less what the transport adds, and no
// higher than streams that died on large packets allow.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    Error, SocketOpts,
    raw::{Type::*, UtpHeader, selective_ack::SelectiveAck},
    seq_nr::SeqNr,
    stream_dispatch::{
        StreamArgs, UtpStreamStarter,
        tests::{make_test_vsock, make_test_vsock_on, test_stream_args},
    },
    test_util::{
        ADDR_1, ADDR_2, env::MockUtpEnvironment, setup_test_logging,
        transport::RememberingTransport,
    },
    traits::UtpEnvironment,
};

// The largest payload a new outgoing stream on `t`'s socket probes for.
fn new_streams_max_ss(t: &super::TestVsock) -> u16 {
    let remote_ack = UtpHeader {
        htype: State,
        seq_nr: 1.into(),
        ack_nr: 100.into(),
        wnd_size: 1024 * 1024,
        ..Default::default()
    };
    let (_tx, rx) = crate::ingress::channel(t._socket.opts().vsock_rx_bufsize);
    let args = StreamArgs::new_outgoing(&remote_ack, t.env.now(), t.env.now());
    let starter = UtpStreamStarter::new(&t._socket, ADDR_2, rx, args);
    let max_ss = starter.vsock.segment_sizes.max_ss();
    starter.disarm();
    max_ss
}

#[tokio::test]
async fn a_tunnels_mtu_caps_the_probe_ceiling() {
    setup_test_logging();
    let t = make_test_vsock(Default::default(), false);
    // 1500 less IPv4, UDP and uTP headers.
    assert_eq!(new_streams_max_ss(&t), 1452);
    t.transport.set_link_mtu(1280);
    assert_eq!(new_streams_max_ss(&t), 1280 - 48);
}

// A stream on a socket at the unspecified IPv6 address, which may not
// forbid fragmenting its IPv4 datagrams, probes all the same.
#[tokio::test]
async fn a_new_stream_probes_up_to_the_link() {
    setup_test_logging();
    let t = make_test_vsock(Default::default(), false);
    assert_eq!(new_streams_max_ss(&t), 1452);
}

// The one-way delay a test peer reports for the stream's packets. Below
// the delay target, so the congestion window grows.
const PATH_DELAY_US: u32 = 10_000;

// How the test peer acknowledges.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Acks {
    // Once per round, with a SACK of what it holds past a hole.
    SackPerRound,
    // Once per round, no SACK.
    PerRound,
    // Once per packet that arrives, no SACK.
    PerPacket,
}

// A stream sending 512 KiB over a path that carries payloads of up to
// `passes` bytes and drops larger ones whole, unless the send allowed them
// to be fragmented: those arrive. The peer holds what arrives and acks, 10
// ms after each send that brought something, every packet it holds in
// order, as `acks` says; when nothing new arrived, the clock steps a
// millisecond, the pacer's tick. The payloads sent, in order, and how many
// sends the path dropped.
async fn over_a_path(passes: usize, acks: Acks) -> (super::TestVsock, Vec<usize>, usize) {
    let env = MockUtpEnvironment::new();
    let args = test_stream_args(&env, false);
    let transport = RememberingTransport::new(ADDR_1);
    transport.set_controls_fragmenting();
    let mut t = make_test_vsock_on(
        transport,
        SocketOpts {
            remote_inactivity_timeout: Some(Duration::from_secs(60)),
            vsock_tx_bufsize_bytes_initial: Some(non_zero_const!(1024 * 1024)),
            ..Default::default()
        },
        args,
        env,
    );
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[0u8; 512 * 1024])
        .await
        .unwrap();
    let mut sizes = Vec::new();
    let mut dropped = 0;
    // The packets held past a hole, and the first the peer has not got.
    let mut held = std::collections::BTreeSet::new();
    let mut next: Option<SeqNr> = None;
    for _ in 0..20_000 {
        // Each round runs on a fresh task budget: past tokio's 128 receives
        // in one task poll, the stream's inbox reads as empty.
        tokio::task::yield_now().await;
        t.poll_once_assert_pending().await;
        let sent = t.take_sent_fragmenting();
        let data = sent.iter().filter(|(msg, _)| msg.header.get_type() == Data);
        sizes.extend(data.clone().map(|(msg, _)| msg.payload().len()));
        let mut ack_nrs = Vec::new();
        for (msg, fragmenting) in data {
            let next = next.get_or_insert(msg.header.seq_nr);
            let carried =
                msg.payload().len() <= passes || *fragmenting == Some(crate::Fragmenting::Allowed);
            if !carried {
                dropped += 1;
                continue;
            }
            let new = msg.header.seq_nr.is_at_or_after(*next);
            if new {
                held.insert(msg.header.seq_nr.0);
            }
            while held.remove(&next.0) {
                *next += 1;
            }
            if new || acks == Acks::PerPacket {
                ack_nrs.push(*next - 1);
            }
        }
        if acks != Acks::PerPacket {
            ack_nrs = ack_nrs.last().copied().into_iter().collect();
        }
        if ack_nrs.is_empty() {
            t.env.increment_now(Duration::from_millis(1));
            continue;
        }
        t.env.increment_now(Duration::from_millis(10));
        for ack_nr in ack_nrs {
            let timestamp_microseconds = t.vsock.timestamp_microseconds();
            // What it holds past the hole, as a SACK: bit 0 is the packet
            // after the one the hole begins at.
            let sacked = held
                .iter()
                .map(|&seq_nr| usize::from(seq_nr.wrapping_sub(ack_nr.0).wrapping_sub(2)))
                .filter(|&offset| offset < 64);
            let selective_ack = if acks == Acks::SackPerRound {
                SelectiveAck::new_test(sacked)
            } else {
                None
            };
            t.send_msg(
                UtpHeader {
                    htype: State,
                    seq_nr: 1.into(),
                    ack_nr,
                    timestamp_microseconds,
                    timestamp_difference_microseconds: PATH_DELAY_US,
                    wnd_size: 1024 * 1024,
                    extensions: crate::raw::Extensions {
                        selective_ack,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                "",
            );
        }
    }
    (t, sizes, dropped)
}

// On a path that carries 1500-byte packets, each acked probe raises the
// size until the stream sends full-size segments: 1500 less IPv4, UDP and
// uTP headers.
#[tokio::test]
async fn a_stream_probes_up_to_full_size_segments() {
    setup_test_logging();
    let (t, sizes, _) = over_a_path(usize::MAX, Acks::SackPerRound).await;
    assert_eq!(sizes[0], 528, "the floor first");
    assert_eq!(t.vsock.segment_sizes.mss(), 1452);
    assert_eq!(sizes.iter().max(), Some(&1452));
    let full = sizes.iter().filter(|&&size| size == 1452).count();
    assert!(full >= 300, "{full} full-size segments");
}

// A path that drops every packet larger than 1000 bytes, as a link that
// may not fragment does: each probe past it is lost and lowers the
// ceiling, and its data goes again at a size proven or probed lower. The
// stream settles at the largest payload that passes, 1000 less IPv4, UDP
// and uTP headers, and sends the rest at it, whether the peer sends SACKs
// or not. Without them a probe with fewer than three packets behind it is
// found lost only by the timer. Each halving step loses at most one probe,
// and each lost probe costs at most one timeout: from the floor's 528 to
// the link's 1452 that is at most ten (2^10 > 1452 - 528).
#[tokio::test]
async fn a_stream_settles_at_the_largest_size_the_path_passes() {
    setup_test_logging();
    const PASSES: usize = 1000 - 48;
    for acks in [Acks::SackPerRound, Acks::PerRound, Acks::PerPacket] {
        let (t, sizes, dropped) = over_a_path(PASSES, acks).await;
        let timeouts = t._socket.stats().snapshot().timeouts;
        assert!(dropped > 0, "{acks:?}: a probe was lost");
        assert!(dropped <= 10, "{acks:?}: {dropped} probes lost");
        assert!(timeouts <= dropped as u64, "{acks:?}: {timeouts} timeouts");
        assert_eq!(usize::from(t.vsock.segment_sizes.mss()), PASSES, "{acks:?}");
        assert!(
            !t.vsock.segment_sizes.is_probing(),
            "{acks:?}: the ceiling met the floor"
        );
        let found = sizes.iter().position(|&size| size == PASSES).unwrap();
        let after = &sizes[found..];
        assert!(after.iter().all(|&size| size <= PASSES), "{acks:?}");
        assert!(
            after.len() >= 300,
            "{acks:?}: {} sent at the size found",
            after.len()
        );
    }
}

// A peer's large DATA proves the path toward us, not the path toward it:
// only an acked probe raises the size, as in libtorrent (utp_stream.cpp
// 2184-2189).
#[tokio::test]
async fn the_peers_large_data_does_not_raise_the_size() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);
    t.send_data(1, 100, &"x".repeat(1354));
    t.poll_once_assert_pending().await;
    t.take_sent();
    assert_eq!(t.vsock.segment_sizes.mss(), 528);

    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[0u8; 2000])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let data: Vec<_> = t
        .take_sent()
        .into_iter()
        .filter(|msg| msg.header.get_type() == Data)
        .collect();
    assert_eq!(data[0].payload().len(), 528, "the floor");
}

// A peer behind a path whose limit the test sets. The transport refuses a
// datagram larger than the limit, as a host whose path MTU shrank does; a
// larger payload the transport sent allowing fragmentation arrives in
// fragments. The peer holds what arrives and acks, 10 ms after each send, every byte
// it holds in order. When nothing new arrived, the clock steps a
// millisecond, the pacer's tick.
struct Peer {
    // The largest payload the path passes.
    passes: usize,
    // Whether arrivals are acked.
    acks: bool,
    ack_nr: u16,
    held: std::collections::BTreeMap<u16, Vec<u8>>,
    received: Vec<u8>,
    // Every DATA payload the stream tried, with whether the path took it.
    tried: Vec<(u16, usize, bool)>,
    // For each of `tried`, how a send that controls fragmenting sent it.
    fragmenting: Vec<Option<crate::Fragmenting>>,
}

impl Peer {
    fn new() -> Self {
        Self {
            passes: usize::MAX,
            acks: true,
            ack_nr: 100,
            held: Default::default(),
            received: Vec::new(),
            tried: Vec::new(),
            fragmenting: Vec::new(),
        }
    }

    // The path carries IP packets of up to `mtu` bytes: payloads of up to
    // `mtu` less IPv4, UDP and uTP headers.
    fn set_mtu(&mut self, t: &mut super::TestVsock, mtu: usize) {
        self.passes = mtu - 48;
        t.transport.set_max_payload_len(mtu - 28);
    }

    async fn round(&mut self, t: &mut super::TestVsock) {
        // Each round runs on a fresh task budget: past tokio's 128 receives
        // in one task poll, the stream's inbox reads as empty.
        tokio::task::yield_now().await;
        t.poll_once_assert_pending().await;
        let mut arrived = false;
        for (msg, fragmenting) in t.take_sent_fragmenting() {
            if msg.header.get_type() != Data {
                continue;
            }
            let (seq_nr, len) = (msg.header.seq_nr.0, msg.payload().len());
            let taken = len <= self.passes || fragmenting == Some(crate::Fragmenting::Allowed);
            self.tried.push((seq_nr, len, taken));
            self.fragmenting.push(fragmenting);
            if taken {
                self.held.insert(seq_nr, msg.payload().to_vec());
                arrived = true;
            }
        }
        while let Some(payload) = self.held.remove(&self.ack_nr.wrapping_add(1)) {
            self.received.extend_from_slice(&payload);
            self.ack_nr = self.ack_nr.wrapping_add(1);
        }
        if arrived && self.acks {
            t.env.increment_now(Duration::from_millis(10));
            let timestamp_microseconds = t.vsock.timestamp_microseconds();
            t.send_msg(
                UtpHeader {
                    htype: State,
                    seq_nr: 1.into(),
                    ack_nr: self.ack_nr.into(),
                    timestamp_microseconds,
                    timestamp_difference_microseconds: PATH_DELAY_US,
                    wnd_size: 1024 * 1024,
                    ..Default::default()
                },
                "",
            );
        } else {
            t.env.increment_now(Duration::from_millis(1));
        }
    }
}

// The payload of a packet `mtu` bytes long on IPv4.
fn payload_of(mtu: usize) -> usize {
    mtu - 48
}

// The probes a stream sent forbidding fragmentation, each once, in the
// order first tried, with whether the path took it.
fn probes_tried(peer: &Peer) -> Vec<(u16, usize, bool)> {
    let mut seen = std::collections::HashSet::new();
    peer.tried
        .iter()
        .zip(&peer.fragmenting)
        .filter(|(_, fragmenting)| **fragmenting == Some(crate::Fragmenting::Never))
        .map(|(tried, _)| *tried)
        .filter(|&(seq_nr, len, _)| seen.insert((seq_nr, len)))
        .collect()
}

// A path that drops every packet larger than 1280 bytes sent forbidding
// fragmentation. Each probe tries halfway between the largest size proven
// and the smallest lost, so the stream finds the path's size in ten probes,
// and sends the floor's packets behind each probe while it is out
// (libtorrent v2.0.15 utp_stream.cpp:1528-1532). Every byte arrives.
#[tokio::test]
async fn a_stream_finds_the_paths_size_by_halving() {
    setup_test_logging();
    let transport = RememberingTransport::new(ADDR_1);
    transport.set_controls_fragmenting();
    let (mut t, written) = writing(transport).await;
    let mut peer = Peer::new();
    peer.passes = payload_of(1280);
    while t.vsock.segment_sizes.is_probing() || peer.received.len() < written.len() {
        peer.round(&mut t).await;
    }
    assert!(peer.received == written, "every byte arrived, in order");
    assert_eq!(usize::from(t.vsock.segment_sizes.mss()), payload_of(1280));
    let sizes: Vec<usize> = probes_tried(&peer).iter().map(|p| p.1).collect();
    let expected: Vec<usize> = [1039, 1270, 1386, 1328, 1299, 1285, 1278, 1282, 1280, 1281]
        .into_iter()
        .map(payload_of)
        .collect();
    assert_eq!(sizes, expected);
    // Each probe too large is found lost by the duplicate ACKs the packets
    // behind it draw, never by the timer: a probe that waited for timeouts,
    // one or two, shows here.
    assert_eq!(t._socket.stats().snapshot().timeouts, 0, "timeouts");
}

// A host whose path carries 1000-byte packets refuses a larger one when it
// is sent. A refused probe is cut again smaller at once, under the same
// sequence number, before anything after it goes, and the stream finds the
// path's size. Every byte arrives.
#[tokio::test]
async fn a_refused_probe_is_cut_again_smaller_at_once() {
    setup_test_logging();
    let (mut t, written) = writing(RememberingTransport::new(ADDR_1)).await;
    let mut peer = Peer::new();
    peer.set_mtu(&mut t, 1000);
    while t.vsock.segment_sizes.is_probing() || peer.received.len() < written.len() {
        peer.round(&mut t).await;
    }
    assert!(peer.received == written, "every byte arrived, in order");
    assert_eq!(usize::from(t.vsock.segment_sizes.mss()), payload_of(1000));
    // Each refused send is followed at once by a smaller try of the same
    // sequence number.
    for pair in peer.tried.windows(2) {
        let ((seq_nr, len, taken), (next_seq_nr, next_len, _)) = (pair[0], pair[1]);
        if !taken {
            assert!(len > payload_of(1000), "{seq_nr}: {len} bytes lost");
            assert_eq!(next_seq_nr, seq_nr, "{seq_nr} cut again at once");
            assert!(next_len < len, "{seq_nr}: {next_len} after {len}");
        }
    }
    // A probe is a send larger than any the path took before it.
    let mut proven = payload_of(576);
    let mut sizes = Vec::new();
    for &(_, len, taken) in &peer.tried {
        if len > proven {
            sizes.push(len);
        }
        if taken {
            proven = proven.max(len);
        }
    }
    let expected: Vec<usize> = [1039, 808, 924, 982, 1011, 997, 1004, 1001, 999, 1000]
        .into_iter()
        .map(payload_of)
        .collect();
    assert_eq!(sizes, expected);
}

// A stream with 1 MiB written, of bytes that each say where they belong,
// run until it sends full-size segments.
async fn settled_at_full_size(peer: &mut Peer) -> (super::TestVsock, Vec<u8>) {
    let (mut t, written) = writing(RememberingTransport::new(ADDR_1)).await;
    while t.vsock.segment_sizes.mss() < 1452 {
        peer.round(&mut t).await;
    }
    (t, written)
}

// A stream over `transport` with 1 MiB written, of bytes that each say
// where they belong.
async fn writing(transport: RememberingTransport) -> (super::TestVsock, Vec<u8>) {
    let env = MockUtpEnvironment::new();
    let args = test_stream_args(&env, false);
    let mut t = make_test_vsock_on(
        transport,
        SocketOpts {
            remote_inactivity_timeout: Some(Duration::from_secs(60)),
            vsock_tx_bufsize_bytes_initial: Some(non_zero_const!(1024 * 1024)),
            ..Default::default()
        },
        args,
        env,
    );
    let written: Vec<u8> = (0..1024 * 1024).map(|i: u32| (i % 251) as u8).collect();
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&written)
        .await
        .unwrap();
    (t, written)
}

// A path that drops every packet larger than 1000 bytes sent forbidding
// fragmentation, and fragments one sent allowing it. Only a probe the
// stream gave up on is sent allowing fragmentation, on every send after it
// was given up; a live probe never is, and nothing larger than the path
// goes as plain data. Every byte arrives, in order.
#[tokio::test]
async fn only_a_given_up_probe_is_sent_allowing_fragmentation() {
    setup_test_logging();
    let transport = RememberingTransport::new(ADDR_1);
    transport.set_controls_fragmenting();
    let (mut t, written) = writing(transport).await;
    let mut peer = Peer::new();
    peer.passes = 1000 - 48;
    while peer.received.len() < written.len() {
        peer.round(&mut t).await;
    }
    assert!(peer.received == written, "every byte arrived, in order");

    // Each probe's size, by sequence number, and the probes given up on.
    let mut probes = std::collections::HashMap::new();
    let mut given_up = std::collections::HashSet::new();
    for (&(seq_nr, len, _), fragmenting) in peer.tried.iter().zip(&peer.fragmenting) {
        match fragmenting {
            Some(crate::Fragmenting::Never) => {
                assert!(
                    !given_up.contains(&seq_nr),
                    "{seq_nr} probed after giving up"
                );
                probes.insert(seq_nr, len);
            }
            Some(crate::Fragmenting::Allowed) => {
                assert_eq!(
                    probes.get(&seq_nr),
                    Some(&len),
                    "{seq_nr} is a probe, whole"
                );
                given_up.insert(seq_nr);
            }
            None => {
                assert!(
                    !given_up.contains(&seq_nr),
                    "{seq_nr} resent forbidding fragmentation"
                );
                assert!(len <= peer.passes, "{seq_nr}: {len} bytes as plain data");
            }
        }
    }
    assert!(!given_up.is_empty(), "no probe was given up on");
}

// The path's MTU drops from 1500 to 1000 under a stream sending full-size
// segments. The host refuses each larger packet the stream has cut and not
// sent; the stream cuts them again smaller, sends every byte in order, and
// settles at the largest payload the path passes.
#[tokio::test]
async fn a_stream_lives_through_its_path_shrinking() {
    setup_test_logging();
    let mut peer = Peer::new();
    let (mut t, written) = settled_at_full_size(&mut peer).await;
    peer.set_mtu(&mut t, 1000);
    let drop_at = peer.tried.len();
    while peer.received.len() < written.len() {
        peer.round(&mut t).await;
    }
    assert!(peer.received == written, "every byte arrived, in order");

    let after = &peer.tried[drop_at..];
    let refused: Vec<usize> = after
        .iter()
        .filter(|(_, _, taken)| !taken)
        .map(|&(_, len, _)| len)
        .collect();
    assert!(refused.contains(&1452), "a full-size segment was refused");
    // Each refused size is tried once: the next is smaller.
    assert!(
        refused.windows(2).all(|w| w[1] < w[0]),
        "refused: {refused:?}"
    );
    assert_eq!(usize::from(t.vsock.segment_sizes.mss()), 1000 - 48);
    let at_limit = after.iter().filter(|&&(_, len, _)| len == 952).count();
    assert!(at_limit >= 300, "{at_limit} sent at the size found");
}

// Full-size packets in flight when the path shrinks are lost with it. The
// host refuses their resends as too large; a packet sent before cannot be
// cut again, so each refused resend counts as one the path dropped and the
// stream waits on. Once the path takes the packet again, it goes, and every
// byte arrives in order. Nothing cut after the refusal is as large.
#[tokio::test]
async fn a_refused_resend_waits_as_a_lost_one() {
    setup_test_logging();
    let mut peer = Peer::new();
    let (mut t, written) = settled_at_full_size(&mut peer).await;
    // What goes out next never arrives.
    peer.acks = false;
    peer.passes = 0;
    let settled_at = peer.tried.len();
    while peer.tried.len() == settled_at {
        peer.round(&mut t).await;
    }
    let lost_from = peer.ack_nr.wrapping_add(1);
    peer.acks = true;
    peer.set_mtu(&mut t, 1000);
    let shrunk_at = peer.tried.len();
    while !peer.tried[shrunk_at..]
        .iter()
        .any(|&(seq_nr, len, taken)| seq_nr == lost_from && len == 1452 && !taken)
    {
        peer.round(&mut t).await;
    }
    let refused_at = peer.tried.len();
    // The path takes 1500-byte packets again.
    peer.set_mtu(&mut t, 1500);
    while peer.received.len() < written.len() {
        peer.round(&mut t).await;
    }
    assert!(peer.received == written, "every byte arrived, in order");
    assert!(
        peer.tried[refused_at..]
            .iter()
            .any(|&(seq_nr, len, taken)| seq_nr == lost_from && len == 1452 && taken),
        "the refused packet went whole"
    );
    assert!(usize::from(t.vsock.segment_sizes.max_ss()) < 1452);
}

// A probe goes only while the congestion window holds more than three
// packets of the proven size: 528 bytes of payload and the 20-byte uTP
// header, 3 * 548 = 1644 bytes, as libtorrent compares its window with
// three floor packets (utp_stream.cpp v2.0.15:1528-1530). The window starts
// at 1500 bytes, so the first packets are all floor-sized; acks that report
// a delay under the target grow it, and the first packet cut once it holds
// more than 1644 bytes is the probe, halfway from 528 to 1452: 991 bytes.
#[tokio::test]
async fn a_probe_waits_for_a_window_above_three_floor_packets() {
    setup_test_logging();
    let env = MockUtpEnvironment::new();
    let args = test_stream_args(&env, false);
    let mut t = make_test_vsock_on(
        RememberingTransport::new(ADDR_1),
        SocketOpts {
            remote_inactivity_timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        },
        args,
        env,
    );
    let gate = 3 * (528 + 20);
    assert_eq!(t.vsock.congestion_controller.cwnd_bytes(), 1500);
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[0u8; 16 * 1024])
        .await
        .unwrap();
    let mut peer = Peer::new();
    let mut floor_packets = 0;
    loop {
        let tried = peer.tried.len();
        peer.round(&mut t).await;
        // The window the pass cut with: the peer's ack goes in after it.
        let cwnd = t.vsock.congestion_controller.cwnd_bytes();
        let sizes: Vec<usize> = peer.tried[tried..].iter().map(|&(_, len, _)| len).collect();
        if let Some(&probe) = sizes.iter().find(|&&len| len > 528) {
            assert!(cwnd > gate, "a probe at a window of {cwnd} bytes");
            assert_eq!(probe, 991);
            break;
        }
        assert!(sizes.iter().all(|&len| len == 528), "{sizes:?}");
        floor_packets += sizes.len();
    }
    // The first window holds two floor packets, 1056 bytes; a third would
    // pass 1500. Before, the second packet was the probe.
    assert_eq!(floor_packets, 2, "floor packets first");
}

#[tokio::test]
async fn no_packet_outgrows_a_1280_byte_tunnel() {
    setup_test_logging();
    let env = MockUtpEnvironment::new();
    let transport = RememberingTransport::new(ADDR_1);
    transport.set_link_mtu(1280);
    let args = test_stream_args(&env, false);
    let mut t = make_test_vsock_on(
        transport,
        SocketOpts {
            remote_inactivity_timeout: Some(Duration::from_secs(60)),
            vsock_tx_bufsize_bytes_initial: Some(non_zero_const!(1024 * 1024)),
            ..Default::default()
        },
        args,
        env,
    );
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[0u8; 64 * 1024])
        .await
        .unwrap();
    let mut largest = 0;
    let mut ack_nr = 100u16;
    for _ in 0..64 {
        t.poll_once_assert_pending().await;
        for msg in t.take_sent() {
            largest = largest.max(msg.payload().len());
            ack_nr = ack_nr.max(msg.header.seq_nr.0);
        }
        t.env.increment_now(Duration::from_millis(10));
        let timestamp_microseconds = t.vsock.timestamp_microseconds();
        t.send_msg(
            UtpHeader {
                htype: State,
                seq_nr: 1.into(),
                ack_nr: ack_nr.into(),
                timestamp_microseconds,
                // The window grows past three floor packets, so probes go.
                timestamp_difference_microseconds: PATH_DELAY_US,
                wnd_size: 1024 * 1024,
                ..Default::default()
            },
            "",
        );
    }
    // IPv4, UDP and uTP headers take 48 of the tunnel's 1280.
    assert!(largest > 528, "a probe went above the floor: {largest}");
    assert!(largest <= 1280 - 48, "{largest} outgrows the tunnel");
}

#[tokio::test]
async fn the_ceiling_is_the_largest_of_the_last_three_failed_mtus() {
    setup_test_logging();
    let t = make_test_vsock(Default::default(), false);
    t._socket.restrict_mtu(1200);
    t._socket.restrict_mtu(1100);
    assert_eq!(new_streams_max_ss(&t), 1452, "fewer than three: the link's");
    t._socket.restrict_mtu(1000);
    assert_eq!(new_streams_max_ss(&t), 1200 - 1 - 48);
    t._socket.restrict_mtu(1000);
    assert_eq!(new_streams_max_ss(&t), 1100 - 1 - 48, "the oldest gone");
}

#[tokio::test]
async fn a_stream_that_dies_on_a_probe_lowers_the_ceiling() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            remote_inactivity_timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        },
        false,
    );
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[0u8; 2000])
        .await
        .unwrap();
    // A window above three floor packets: the second packet is a probe.
    t.vsock
        .congestion_controller
        .set_window(super::PROBE_WINDOW);
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent[0].payload().len(), 528, "the floor");
    assert_eq!(sent[1].header.seq_nr, 102.into());
    let probe = sent[1].payload().len();
    assert!(probe > 528, "a probe");

    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    let died = loop {
        t.env.increment_now(t.vsock.rtte.retransmission_timeout());
        match t.poll_once().await {
            std::task::Poll::Pending => t.take_sent(),
            std::task::Poll::Ready(result) => break result,
        };
    };
    assert!(matches!(died, Err(Error::MaxRetransmissionsReached)));

    // Three such deaths, and the next stream probes no higher than them.
    let mtu = u16::try_from(probe).unwrap() + 48;
    t._socket.restrict_mtu(mtu);
    t._socket.restrict_mtu(mtu);
    assert_eq!(new_streams_max_ss(&t), mtu - 1 - 48);
}
