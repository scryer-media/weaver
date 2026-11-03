// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tracing::trace;

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader, selective_ack::SelectiveAck},
    seq_nr::SeqNr,
    stream_dispatch::{
        Timer,
        tests::{calc_mtu_for_mss, make_test_vsock},
    },
    test_util::setup_test_logging,
};

// (5.1) Every time a packet containing data is sent (including a
// retransmission), if the timer is not running, start it running
// so that it will expire after RTO seconds (for the current value
// of RTO).
#[tokio::test]
async fn retransmit_timer_started() {
    let mut t = make_test_vsock(Default::default(), false);
    let (_r, mut w) = t.stream.take().unwrap().split();

    assert!(matches!(t.vsock.timers.retransmit, Timer::Idle));

    w.write_all(b"test").await.unwrap();

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "test")]
    );

    assert!(matches!(t.vsock.timers.retransmit, Timer::Armed { .. }));
}

// (5.2) When all outstanding data has been acknowledged, turn off the
// retransmission timer.
//
// (5.3) When an ACK is received that acknowledges new data, restart the
// retransmission timer so that it will expire after RTO seconds
// (for the current value of RTO).//
#[tokio::test]
async fn retransmit_timer_idle_when_all_data_acked() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            link_mtu: Some(calc_mtu_for_mss(2)),
            ..Default::default()
        },
        false,
    );
    let (_r, mut w) = t.stream.take().unwrap().split();

    // Send 2 packets
    w.write_all(b"test").await.unwrap();

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, payload = "te"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "st")
        ]
    );

    let timer_0 = t.vsock.timers.retransmit;

    assert!(matches!(timer_0, Timer::Armed { .. }));

    t.env.increment_now(Duration::from_millis(100));
    // Only 1 packet ACKed. Retransmit timer should reset.
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    let timer_1 = t.vsock.timers.retransmit;
    match (timer_0, timer_1) {
        (Timer::Armed { expires_at: e0, .. }, Timer::Armed { expires_at: e1, .. }) if e1 > e0 => {}
        _ => {
            panic!("expected retransmit timer to update: timer_0={timer_0:?}, timer_1={timer_1:?}")
        }
    }

    // ACK the first packet again. Retransmit timer should not change.
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    assert_eq!(t.vsock.timers.retransmit, timer_1);

    // ACK the second packet. Retransmit timer should reset.
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 102.into(),
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    assert_eq!(t.vsock.timers.retransmit, Timer::Idle);
}

#[tokio::test]
async fn retransmit_timer_not_restarted_on_newly_sent_packets() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            link_mtu: Some(calc_mtu_for_mss(2)),
            ..Default::default()
        },
        false,
    );
    let (_r, mut w) = t.stream.take().unwrap().split();

    // Send 2 packets separately.
    w.write_all(b"te").await.unwrap();
    t.poll_once_assert_pending().await;

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "te"),]
    );

    let timer_0 = t.vsock.timers.retransmit;
    assert!(matches!(timer_0, Timer::Armed { .. }));

    t.env.increment_now(Duration::from_millis(100));
    w.write_all(b"st").await.unwrap();
    t.poll_once_assert_pending().await;

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 102, payload = "st"),]
    );

    let timer_1 = t.vsock.timers.retransmit;
    assert!(matches!(timer_0, Timer::Armed { .. }));
    assert_eq!(timer_0, timer_1);
}

#[tokio::test]
async fn test_rto_not_stuck_in_congestion_control() {
    setup_test_logging();
    const MSS: usize = 5;
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(MSS)),
            congestion: crate::CongestionConfig {
                tracing: true,
                ..super::cubic()
            },
            ..Default::default()
        },
        false,
    );
    let (_r, mut w) = t.stream.take().unwrap().split();

    let rto = Duration::from_millis(100);
    t.vsock.rtte.force_timeout(rto);
    for _ in 0..20 {
        // This should write a bunch of MSS packets
        w.write_all(b"helloworld").await.unwrap();
    }

    t.poll_once_assert_pending().await;

    // 2 MSS
    assert_eq!(t.vsock.congestion_controller.window(), 10);

    // The first batch should be congestion controlled.
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "world"),
        ]
    );

    // Ack both
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 102.into(),
            ..Default::default()
        },
        "",
    );

    // The second batch should be congestion controlled but better.
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.congestion_controller.window(), 20);
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 103, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 104, payload = "world"),
            cmphead!(ST_DATA, seq_nr = 105, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 106, payload = "world"),
        ]
    );

    // Ack all
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 106.into(),
            ..Default::default()
        },
        "",
    );

    // Now let's pretend we send a bunch of data and then timeout.
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.congestion_controller.window(), 40);
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 107, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 108, payload = "world"),
            cmphead!(ST_DATA, seq_nr = 109, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 110, payload = "world"),
            cmphead!(ST_DATA, seq_nr = 111, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 112, payload = "world"),
            cmphead!(ST_DATA, seq_nr = 113, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 114, payload = "world"),
        ]
    );

    // Now don't ACK anything and trigger RTO.
    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;
    // RTO should retransmit 1 packet.
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 107, payload = "hello")]
    );

    // ACK the RTO packet. We then should resume normal sending from slow start.
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 107.into(),
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 108, payload = "world"),
            cmphead!(ST_DATA, seq_nr = 109, payload = "hello")
        ]
    );
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 109.into(),
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 110, payload = "world"),
            cmphead!(ST_DATA, seq_nr = 111, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 112, payload = "world"),
            cmphead!(ST_DATA, seq_nr = 113, payload = "hello"),
        ]
    );
}

#[tokio::test]
async fn test_basic_retransmission_0() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    // Write some data
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    let seq_nr: SeqNr = 101.into();

    // First transmission should happen
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = seq_nr, payload = "hello"),]
    );

    // Wait for retransmission timeout
    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    t.poll_once_assert_pending().await;

    // Should retransmit the same data
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = seq_nr, payload = "hello")]
    );

    // Until time goes on, nothing should happen.
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Wait again for 2nd retransmission.
    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = seq_nr, payload = "hello")]
    );
}

#[tokio::test]
async fn test_basic_retransmission_1() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            ..Default::default()
        },
        false,
    );

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.seq_nr,
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Write some test data
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello world")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    // Initial send
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello world"
        )],
        "should send data initially"
    );

    // No retransmission should occur before timeout
    t.env.increment_now(Duration::from_millis(100));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not retransmit before timeout");

    // After timeout, packet should be retransmitted
    t.env.increment_now(Duration::from_secs(3));
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello world"
        )],
        "should retransmit"
    );

    // Second timeout should trigger another retransmission with doubled timeout
    t.env.increment_now(Duration::from_secs(6));
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello world"
        )],
        "should retransmit after second timeout"
    );

    // Now ACK the packet - should stop retransmissions
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // No more retransmissions should occur
    t.env.increment_now(Duration::from_secs(4));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not retransmit after ACK");

    // Write new data - should use next sequence number
    t.stream.as_mut().unwrap().write_all(b"test").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 102,
            ack_nr = 0,
            payload = "test"
        )]
    );
}

#[tokio::test]
async fn test_rto_single_packet_retransmission_mss_sized() {
    setup_test_logging();
    // Use MSS of 1 to create the smallest possible packet
    const MSS: usize = 1;
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(MSS)),
            congestion: super::cubic(),
            ..Default::default()
        },
        false,
    );

    // Setup a fixed RTO duration for predictable testing
    let rto = Duration::from_millis(100);
    t.vsock.rtte.force_timeout(rto);

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: t.vsock.seq_nr,
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Write multiple bytes, but they should be sent in 1-byte packets due to MSS=1
    let (_r, mut w) = t.stream.take().unwrap().split();
    w.write_all(b"abcdefg").await.unwrap();

    // Initial poll should send the first packet with one byte
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, payload = "a"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "b")
        ],
        "Initial send should be 2 MSS"
    );

    // Ensure nothing gets sent.
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Let's trigger a timeout for the first packet
    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;

    trace!(
        congestion_controller = ?t.vsock.congestion_controller,
        "congestion before RTO",
    );

    // RTO should retransmit ONLY the first unacked packet
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "a")],
        "After RTO, only the first unacked packet should be retransmitted"
    );

    // After RTO, additional poll should NOT send any new packets until an ACK is received
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("No additional packets should be sent after RTO until ACK received");

    trace!(
        congestion_controller = ?t.vsock.congestion_controller,
        "congestion after RTO",
    );

    // After ACK, we should resume sending from where we left off
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Poll should now continue sending packets that were queued
    t.poll_once_assert_pending().await;
    trace!(
        congestion_controller = ?t.vsock.congestion_controller,
        "congestion after resume",
    );
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 102, payload = "b"),
            cmphead!(ST_DATA, seq_nr = 103, payload = "c")
        ],
        "After ACK for retransmitted packet, should continue sending"
    );

    // ACK the second packet
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 102.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Should send more packets
    t.poll_once_assert_pending().await;
    trace!(
        congestion_controller = ?t.vsock.congestion_controller,
        "congestion after second ACK",
    );

    // Packets 103 and 104 should be in the network. MAYBE more, that depends on congestion calculations.
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 104, payload = "d"),],
        "After second ACK, should send next packet"
    );

    // Trigger another timeout for the third packet
    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;

    // Should retransmit only the first unacked packet
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 103, payload = "c")],
        "After second RTO, only the first unacked packet should be retransmitted"
    );

    // Additional poll should not send any more packets
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("After second RTO, no additional packets should be sent until ACK");
}

// This would test an edge case where on RTO we send only one packet even
// if more packets fit into the window.
#[tokio::test]
async fn test_rto_single_packet_retransmission_smaller_than_mss() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            disable_nagle: true,
            remote_inactivity_timeout: Some(Duration::from_secs(20)),
            ..Default::default()
        },
        false,
    );
    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: t.vsock.seq_nr,
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    let (_r, mut w) = t.stream.take().unwrap().split();
    w.write_all(b"a").await.unwrap();

    // Writing in small pieces
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "a")]
    );

    w.write_all(b"bcde").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 102, payload = "bcde")]
    );

    // Trigger a timeout. Should retransmit only ONE packet and do nothing on second poll.
    t.env.increment_now(Duration::from_secs(10));
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "a")]
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
}

#[tokio::test]
async fn test_selective_ack_retransmission() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        false,
    );

    const FORCED_RETRANSMISSION_TIME: Duration = Duration::from_secs(1);
    t.vsock.rtte.force_timeout(FORCED_RETRANSMISSION_TIME);

    // Write enough data to generate multiple packets
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"helloworld")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    // Should have sent two packets
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, ack_nr = 0, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 102, ack_nr = 0, payload = "world")
        ]
    );

    // Send selective ACK indicating second packet was received but first wasn't
    let header = UtpHeader {
        htype: ST_STATE,
        seq_nr: 1.into(),
        ack_nr: 100.into(), // ACK previous packet
        wnd_size: 1024,
        extensions: crate::raw::Extensions {
            selective_ack: SelectiveAck::new_test([0]),
            ..Default::default()
        },
        ..Default::default()
    };
    t.send_msg(header, "");
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // After receiving selective ACK and waiting for retransmit
    t.env.increment_now(FORCED_RETRANSMISSION_TIME);
    t.poll_once_assert_pending().await;

    // Should only retransmit the first packet
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )]
    );

    // Send normal ACK for first packet
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // No more retransmissions should occur
    t.env.increment_now(FORCED_RETRANSMISSION_TIME);
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not retransmit after both packets acknowledged");
}

#[tokio::test]
async fn fin_retransmit() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            ..Default::default()
        },
        false,
    );
    let (r, w) = t.stream.take().unwrap().split();
    drop(r);
    drop(w);

    // Force small RTO so that it wins VS hard-coded 1 second inactivity timer.
    const SMALL_RTT: Duration = Duration::from_millis(10);
    for _ in 0..100 {
        t.vsock.rtte.sample(SMALL_RTT);
    }

    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent(), vec![cmphead!(ST_FIN, seq_nr = 101)]);
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Trigger RTO. Ensure it gets resent only once.
    let rto_0 = t.vsock.rtte.retransmission_timeout();
    t.env.increment_now(rto_0);
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent(), vec![cmphead!(ST_FIN, seq_nr = 101)]);
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Trigger RTO again. Ensure it gets resent only once.
    let rto_1 = t.vsock.rtte.retransmission_timeout();
    assert!(rto_1 > rto_0);
    t.env.increment_now(rto_1);
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent(), vec![cmphead!(ST_FIN, seq_nr = 101)]);
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
}
