// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How high a stream probes: the link less what the transport adds, and no
// higher than streams that died on large packets allow.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    Error, SocketOpts,
    raw::{Type::*, UtpHeader},
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

#[tokio::test]
async fn no_probe_where_datagrams_may_be_fragmented() {
    setup_test_logging();
    let t = make_test_vsock(Default::default(), false);
    t.transport.set_fragments();
    // The IPv4 floor, 576, less IPv4, UDP and uTP headers.
    assert_eq!(new_streams_max_ss(&t), 528);
}

// The largest payload `t` sends of 16 KiB written now, in `rounds` rounds,
// each acked whole by a peer whose last packet was `peer_seq_nr`.
async fn largest_sent_over(t: &mut super::TestVsock, peer_seq_nr: u16, rounds: usize) -> usize {
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[0u8; 16 * 1024])
        .await
        .unwrap();
    let mut largest = 0;
    for _ in 0..rounds {
        t.env.increment_now(Duration::from_millis(10));
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        if sent.iter().all(|msg| msg.payload().is_empty()) {
            continue;
        }
        for msg in sent {
            largest = largest.max(msg.payload().len());
        }
        t.send_msg(
            UtpHeader {
                htype: State,
                seq_nr: peer_seq_nr.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024 * 1024,
                ..Default::default()
            },
            "",
        );
    }
    largest
}

// A socket that cannot forbid fragmenting, as a dual-stack socket at the
// unspecified IPv6 address reports, holds its streams at the IPv4 floor
// on a path that carries 1500-byte packets. Only the size of the peer's
// own data packets raises it, so the size a stream reaches depends on
// what the peer happens to send.
#[tokio::test]
async fn where_datagrams_may_fragment_only_the_peers_data_raises_the_size() {
    setup_test_logging();
    let env = MockUtpEnvironment::new();
    let transport = RememberingTransport::new(ADDR_1);
    transport.set_fragments();
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
    assert_eq!(largest_sent_over(&mut t, 1, 64).await, 528, "the floor");

    // The peer's small packets, as requests are, leave it there.
    t.send_msg(
        UtpHeader {
            htype: Data,
            seq_nr: 1.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        &"r".repeat(17 * 8),
    );
    assert_eq!(
        largest_sent_over(&mut t, 2, 64).await,
        528,
        "still the floor"
    );

    // One large packet from the peer, and every packet is that large.
    t.send_msg(
        UtpHeader {
            htype: Data,
            seq_nr: 2.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        &"r".repeat(1354),
    );
    assert_eq!(
        largest_sent_over(&mut t, 3, 64).await,
        1354,
        "the peer's size"
    );
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
        t.send_msg(
            UtpHeader {
                htype: State,
                seq_nr: 1.into(),
                ack_nr: ack_nr.into(),
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
            // The probe stays whole until the stream gives up on it.
            mtu_probe_max_retransmissions: Some(10),
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
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent[0].payload().len(), 528, "the floor");

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
    let sent = t.take_sent();
    assert_eq!(sent[0].header.seq_nr, 102.into());
    let probe = sent[0].payload().len();
    assert!(probe > 528, "a probe");
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
