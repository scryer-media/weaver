// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::{sync::atomic::Ordering, task::Poll, time::Duration};

use futures::FutureExt;

use crate::{
    SocketOpts,
    constants::ACK_DELAY,
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::make_test_vsock,
    test_util::setup_test_logging,
};

#[tokio::test]
async fn test_delayed_ack_sent_once() {
    setup_test_logging();

    let mut t = make_test_vsock(
        SocketOpts {
            benchmark_delayed_acks: true,
            ..Default::default()
        },
        false,
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    t.send_msg(
        UtpHeader {
            htype: Data,
            seq_nr: 1.into(),
            ack_nr: t.vsock.seq_nr - 1,
            ..Default::default()
        },
        "hello",
    );
    t.poll_once_assert_pending().await;
    assert_eq!(&t.read_all_available().await.unwrap(), b"hello");
    t.assert_sent_empty();

    // Pretend it's 1 second later.
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    assert_eq!(&t.read_all_available().await.unwrap(), b"");
    // Assert an ACK was sent.
    let sent = t.take_sent();
    assert_eq!(sent, vec![cmphead! {State, ack_nr=1}]);

    // Assert nothing else is sent later.
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
}

#[tokio::test]
async fn each_receive_drain_acknowledges_small_packets_once() {
    let mut t = make_test_vsock(Default::default(), false);
    t.send_data(1, t.vsock.seq_nr - 1, "a");
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent(), vec![cmphead!(State, ack_nr = 1)]);

    t.send_data(2, t.vsock.seq_nr - 1, "b");
    t.send_data(3, t.vsock.seq_nr - 1, "c");
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent(), vec![cmphead!(State, ack_nr = 3)]);
    assert_eq!(t.read_all_available().await.unwrap(), b"abc");

    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    let stats = t._socket.stats();
    assert_eq!(stats.immediate_acks.load(Ordering::Relaxed), 2);
    assert_eq!(stats.delayed_acks.load(Ordering::Relaxed), 0);
}

// Many full packets read at once are answered by one ACK at the drain, as
// libtorrent answers them (utp_socket_manager.cpp v2.0.15:255-271), not one
// for every two.
#[tokio::test(start_paused = true)]
async fn full_packets_in_one_drain_get_one_ack() {
    const PACKETS: u16 = 20;
    let mut t = make_test_vsock(Default::default(), false);
    let full = "x".repeat(t.vsock.segment_sizes.mss() as usize);
    for seq_nr in 1..=PACKETS {
        t.send_data(seq_nr, t.vsock.seq_nr - 1, &full);
    }
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent(), vec![cmphead!(State, ack_nr = PACKETS)]);
    assert_eq!(
        t.read_all_available().await.unwrap().len(),
        full.len() * PACKETS as usize
    );

    let stats = t._socket.stats();
    assert_eq!(stats.data_packets_received.load(Ordering::Relaxed), 20);
    assert_eq!(stats.ack_packets_sent.load(Ordering::Relaxed), 1);
    assert_eq!(stats.immediate_acks.load(Ordering::Relaxed), 1);
}

// Out-of-order packets defer their ACK to the drain like any other DATA
// (utp_stream.cpp v2.0.15:3004-3034); the one ACK sent there carries the
// SACK of what arrived past the hole.
#[tokio::test(start_paused = true)]
async fn out_of_order_packets_are_acked_once_at_the_drain_with_a_sack() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.send_data(3, ack_nr, "c");
    t.send_data(4, ack_nr, "d");
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent, vec![cmphead!(State, ack_nr = 1)]);
    assert!(
        sent[0].header.extensions.selective_ack.is_some(),
        "the ACK reports 3 and 4"
    );
    assert_eq!(
        t._socket
            .stats()
            .out_of_order_packets
            .load(Ordering::Relaxed),
        2
    );

    // The hole fills: one ACK for everything.
    t.send_data(2, ack_nr, "b");
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent, vec![cmphead!(State, ack_nr = 4)]);
    assert!(sent[0].header.extensions.selective_ack.is_none());
    assert_eq!(t.read_all_available().await.unwrap(), b"abcd");
    assert_eq!(t._socket.stats().immediate_acks.load(Ordering::Relaxed), 2);
}

// Packets taken with no drain after them, as a read cut at the batch
// limit leaves them, owe an ACK that the delayed-ACK timer sends.
#[tokio::test(start_paused = true)]
async fn the_timer_sends_an_owed_ack_when_no_drain_comes() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.send_data(2, ack_nr, "b");
    // Woken without a drain mark.
    t.tx.wake();
    assert!(poll_without_drain(&mut t).await.is_pending());
    assert_eq!(t.vsock.rx.len(), 0, "both packets are taken");
    t.assert_sent_empty_msg("no drain yet");

    t.env.increment_now(ACK_DELAY - Duration::from_millis(1));
    assert!(poll_without_drain(&mut t).await.is_pending());
    t.assert_sent_empty_msg("the timer has not fired");

    t.env.increment_now(Duration::from_millis(1));
    assert!(poll_without_drain(&mut t).await.is_pending());
    assert_eq!(t.take_sent(), vec![cmphead!(State, ack_nr = 2)]);
    let stats = t._socket.stats();
    assert_eq!(stats.delayed_acks.load(Ordering::Relaxed), 1);
    assert_eq!(stats.immediate_acks.load(Ordering::Relaxed), 0);

    // Nothing is owed any more.
    t.env.increment_now(ACK_DELAY);
    assert!(poll_without_drain(&mut t).await.is_pending());
    t.assert_sent_empty();
}

// One poll that leaves the inbox as it is: no drain mark is added.
async fn poll_without_drain(t: &mut super::TestVsock) -> Poll<crate::Result<()>> {
    std::future::poll_fn(|cx| Poll::Ready(t.vsock.poll_unpin(cx))).await
}

// Two reads queued before the stream runs: each drain mark owes its ACK as
// libtorrent sends one at each drain (utp_socket_manager.cpp v2.0.15:255-261),
// and the first is counted as reached with packets already queued behind it.
#[tokio::test(start_paused = true)]
async fn a_drain_mark_with_packets_queued_behind_it_is_counted() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.tx.flush();
    t.send_data(2, ack_nr, "b");
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(State, ack_nr = 1), cmphead!(State, ack_nr = 2)]
    );
    assert_eq!(t.read_all_available().await.unwrap(), b"ab");
    let stats = t._socket.stats().snapshot();
    assert_eq!(stats.drain_marks_with_packets_queued, 1);
    assert_eq!(stats.immediate_acks, 2);
}
