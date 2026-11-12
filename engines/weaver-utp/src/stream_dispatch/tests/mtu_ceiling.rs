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

// A stream on a socket at the unspecified IPv6 address, which may not
// forbid fragmenting its IPv4 datagrams, probes all the same.
#[tokio::test]
async fn a_new_stream_probes_up_to_the_link() {
    setup_test_logging();
    let t = make_test_vsock(Default::default(), false);
    assert_eq!(new_streams_max_ss(&t), 1452);
}

// A stream sending 512 KiB over a path that carries payloads of up to
// `passes` bytes and drops larger ones whole. The peer acks, 10 ms after
// each send, everything before the first packet the path dropped; when
// nothing new arrived, the clock steps a millisecond, the pacer's tick.
// The payloads sent, in order.
async fn over_a_path(passes: usize) -> (super::TestVsock, Vec<usize>) {
    let env = MockUtpEnvironment::new();
    let args = test_stream_args(&env, false);
    let mut t = make_test_vsock_on(
        RememberingTransport::new(ADDR_1),
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
    for _ in 0..20_000 {
        // Each round runs on a fresh task budget: past tokio's 128 receives
        // in one task poll, the stream's inbox reads as empty.
        tokio::task::yield_now().await;
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        let data = sent.iter().filter(|msg| msg.header.get_type() == Data);
        sizes.extend(data.clone().map(|msg| msg.payload().len()));
        let arrived = data
            .take_while(|msg| msg.payload().len() <= passes)
            .last()
            .map(|msg| msg.header.seq_nr);
        match arrived {
            Some(ack_nr) => {
                t.env.increment_now(Duration::from_millis(10));
                let timestamp_microseconds = t.vsock.timestamp_microseconds();
                t.send_msg(
                    UtpHeader {
                        htype: State,
                        seq_nr: 1.into(),
                        ack_nr,
                        timestamp_microseconds,
                        wnd_size: 1024 * 1024,
                        ..Default::default()
                    },
                    "",
                );
            }
            None => t.env.increment_now(Duration::from_millis(1)),
        }
    }
    (t, sizes)
}

// On a path that carries 1500-byte packets, each acked probe raises the
// size until the stream sends full-size segments: 1500 less IPv4, UDP and
// uTP headers.
#[tokio::test]
async fn a_stream_probes_up_to_full_size_segments() {
    setup_test_logging();
    let (t, sizes) = over_a_path(usize::MAX).await;
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
// and uTP headers, and sends the rest at it.
#[tokio::test]
async fn a_stream_settles_at_the_largest_size_the_path_passes() {
    setup_test_logging();
    const PASSES: usize = 1000 - 48;
    let (t, sizes) = over_a_path(PASSES).await;
    assert!(sizes.iter().any(|&size| size > PASSES), "a probe was lost");
    assert_eq!(usize::from(t.vsock.segment_sizes.mss()), PASSES);
    assert!(
        !t.vsock.segment_sizes.is_probing(),
        "the ceiling met the floor"
    );
    let found = sizes.iter().position(|&size| size == PASSES).unwrap();
    let after = &sizes[found..];
    assert!(after.iter().all(|&size| size <= PASSES));
    assert!(after.len() >= 300, "{} sent at the size found", after.len());
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
