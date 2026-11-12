// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How long a stream waits for the remote, and how often it resends,
// before it gives up.

use std::{sync::atomic::Ordering, time::Duration};

use tokio::io::AsyncWriteExt;

use crate::{
    Error, SocketOpts,
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::make_test_vsock,
    test_util::{
        ADDR_1, ADDR_2, env::MockUtpEnvironment, setup_test_logging,
        transport::RememberingTransport,
    },
};

// A remote silent longer than these tests wait is not yet inactive.
fn patient() -> SocketOpts {
    SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

fn open_window(t: &mut super::TestVsock) {
    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
}

#[tokio::test]
async fn data_is_sent_three_times_then_the_stream_gives_up() {
    setup_test_logging();
    let mut t = make_test_vsock(patient(), false);
    open_window(&mut t);
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);

    for _ in 0..2 {
        t.env.increment_now(t.vsock.rtte.retransmission_timeout());
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 1, "a resend on each timeout");
    }
    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    assert!(matches!(
        t.poll_once_assert_ready().await,
        Err(Error::MaxRetransmissionsReached)
    ));
    assert!(
        t.take_sent().iter().all(|m| m.header.htype != Data),
        "no fourth send"
    );
    let stats = t._socket.stats().snapshot();
    assert_eq!(stats.timeouts, 3);
    assert_eq!(stats.packets_lost, 0);
}

// The remote hears at once that the stream gave up, with the id it knows
// the stream by and an acknowledgement it accepts.
#[tokio::test]
async fn a_stream_that_gives_up_on_resends_sends_a_reset() {
    let mut t = make_test_vsock(patient(), false);
    open_window(&mut t);
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    t.take_sent();
    for _ in 0..2 {
        t.env.increment_now(t.vsock.rtte.retransmission_timeout());
        t.poll_once_assert_pending().await;
        t.take_sent();
    }
    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    assert!(matches!(
        t.poll_once_assert_ready().await,
        Err(Error::MaxRetransmissionsReached)
    ));
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0].header.htype, Reset);
    assert_eq!(sent[0].header.connection_id, t.vsock.conn_id_send);
    // The last packet acknowledged, the one before the remote's first.
    assert_eq!(sent[0].header.ack_nr, 0.into());
}

// A reset from the remote ends the stream with nothing sent back.
#[tokio::test]
async fn a_reset_from_the_remote_is_not_answered() {
    let mut t = make_test_vsock(patient(), false);
    open_window(&mut t);
    t.poll_once_assert_pending().await;
    t.take_sent();
    t.send_msg(
        UtpHeader {
            htype: Reset,
            connection_id: t.vsock.conn_id_send - 1,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            ..Default::default()
        },
        "",
    );
    assert!(matches!(
        t.poll_once_assert_ready().await,
        Err(Error::StResetReceived)
    ));
    t.assert_sent_empty();
}

#[tokio::test]
async fn an_rto_is_counted_once_while_transport_sends_stay_pending() {
    let mut t = make_test_vsock(patient(), false);
    open_window(&mut t);
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);

    t.transport.set_poll_send_pending(true);
    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    t.poll_once_assert_pending().await;
    assert_eq!(t._socket.stats().snapshot().timeouts, 1);
    t.poll_once_assert_pending().await;
    assert_eq!(t._socket.stats().snapshot().timeouts, 1);
    t.assert_sent_empty();

    t.transport.set_poll_send_pending(false);
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);
    assert_eq!(t._socket.stats().snapshot().timeouts, 1);
    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);
    assert_eq!(t._socket.stats().snapshot().timeouts, 2);
}

#[tokio::test]
async fn timeouts_in_a_row_wait_a_second_more_each_time_doubling() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);
    open_window(&mut t);
    t.stream.as_mut().unwrap().write_all(b"x").await.unwrap();
    t.poll_once_assert_pending().await;
    t.take_sent();

    let first = t.vsock.rtte.retransmission_timeout();
    t.env.increment_now(first);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.vsock.rtte.retransmission_timeout(),
        first + Duration::from_secs(1)
    );
}

#[tokio::test]
async fn our_fin_is_sent_twice_then_the_stream_ends() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);
    // A short round trip: the timeout at its floor.
    for _ in 0..100 {
        t.vsock.rtte.sample(Duration::from_millis(10));
    }
    assert_eq!(
        t.vsock.rtte.retransmission_timeout(),
        Duration::from_millis(500)
    );
    t.stream.take().unwrap().split();

    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);
    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1, "the FIN once more");

    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    assert!(t.poll_once_assert_ready().await.is_err());
    assert!(t.take_sent().iter().all(|m| m.header.htype != Fin));
}

#[tokio::test]
async fn an_incoming_stream_sends_its_syn_ack_twice_then_waits_the_connect_timeout() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), true);

    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);
    t.env.increment_now(Duration::from_millis(200));
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1, "the SYN-ACK once more");

    t.env.increment_now(Duration::from_millis(2999));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    t.env.increment_now(Duration::from_millis(1));
    assert!(matches!(
        t.poll_once_assert_ready().await,
        Err(Error::MaxSynAckRetransmissionsReached)
    ));
}

#[tokio::test(start_paused = true)]
async fn an_unanswered_connect_reaches_its_timeout_decision() {
    setup_test_logging();
    let sock = crate::UtpSocket::new_with_opts(
        RememberingTransport::new(ADDR_1),
        MockUtpEnvironment::new(),
        Default::default(),
    )
    .unwrap();
    let res = sock.connect(ADDR_2).await;
    assert!(matches!(res, Err(Error::ConnectTimedOut)));
    let stats = sock.stats();
    assert_eq!(stats.connection_failures.load(Ordering::Relaxed), 1);
    assert_eq!(stats.connecting.load(Ordering::Relaxed), 0);
}

#[test]
fn the_default_opts_are_libtorrents() {
    let opts = SocketOpts::default().validate().unwrap();
    assert_eq!(opts.min_timeout, Duration::from_millis(500));
    assert_eq!(opts.num_resends, 3);
    assert_eq!(opts.syn_resends, 2);
    assert_eq!(opts.fin_resends, 2);
    assert_eq!(opts.connect_timeout, Duration::from_secs(3));
}

// Two timeouts' worth of silence with nothing in flight: the window keeps
// four ninths of itself when sending resumes.
#[tokio::test]
async fn an_idle_window_decays_by_a_third_per_timeout() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(non_zero_const!(576)),
            disable_nagle: true,
            ..patient()
        },
        false,
    );
    open_window(&mut t);
    t.poll_once_assert_pending().await;

    // Grow the window a little: each round acks all that is in flight.
    let w = t.stream.as_mut().unwrap();
    w.write_all(&[0u8; 8000]).await.unwrap();
    t.poll_once_assert_pending().await;
    let mut gap = 10_000;
    while let Some(last) = t.take_sent().last().map(|m| m.header.seq_nr) {
        t.env.increment_now(Duration::from_millis(50));
        t.send_msg(
            UtpHeader {
                htype: State,
                seq_nr: 1.into(),
                ack_nr: last,
                wnd_size: 1024 * 1024,
                timestamp_microseconds: 1,
                timestamp_difference_microseconds: gap,
                ..Default::default()
            },
            "",
        );
        gap += 1;
        t.poll_once_assert_pending().await;
    }
    assert!(t.vsock.user_tx_segments.is_empty());
    let before = t.vsock.congestion_controller.window();
    assert!(before > 1500);

    let rto = t.vsock.rtte.retransmission_timeout();
    t.env.increment_now(rto * 2);
    t.stream.as_mut().unwrap().write_all(b"more").await.unwrap();
    t.poll_once_assert_pending().await;
    let after = t.vsock.congestion_controller.window();
    assert!(
        after.abs_diff(before * 4 / 9) <= 2,
        "{after} should be four ninths of {before}"
    );
}
