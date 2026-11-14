// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::{sync::atomic::Ordering, task::Poll, time::Duration};

use futures::FutureExt;
use tokio::io::AsyncWriteExt;

use crate::{
    SocketOpts,
    constants::ACK_DELAY,
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::{calc_mtu_for_mss, make_test_vsock},
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
// (utp_stream.cpp v2.0.15:3004-3034). Two arrived past the hole in the read
// that also took packet 1, so three ACKs leave there: the first moves the
// cumulative ACK and the two that repeat it are the duplicates.
#[tokio::test(start_paused = true)]
async fn out_of_order_packets_are_acked_at_the_drain_with_a_sack() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.send_data(3, ack_nr, "c");
    t.send_data(4, ack_nr, "d");
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(
        sent,
        vec![
            cmphead!(State, ack_nr = 1),
            cmphead!(State, ack_nr = 1),
            cmphead!(State, ack_nr = 1)
        ]
    );
    for ack in &sent {
        assert_eq!(
            ack.header.extensions.selective_ack.unwrap().as_bytes(),
            [0b11],
            "each ACK reports 3 and 4"
        );
    }
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

// Packets taken with no drain mark after them, as a stream that runs while
// the read that queued them goes on finds them, owe an ACK that the
// delayed-ACK timer sends if no mark comes.
#[tokio::test(start_paused = true)]
async fn the_timer_sends_an_owed_ack_when_no_drain_comes() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.send_data(2, ack_nr, "b");
    // Queued, with no drain mark yet.
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

// The ACK for packets held past a hole carries a SACK of the fewest bytes
// that reach the last one held, and the counters say so: two bytes of it
// for packets 3 and 12 while 2 is missing. Two packets came past the hole
// in the read that took packet 1, so the ACK that moves the cumulative ACK
// is repeated twice.
#[tokio::test]
async fn an_ack_with_a_sack_is_counted_with_its_bytes() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.send_data(3, ack_nr, "c");
    t.send_data(12, ack_nr, "l");
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 3, "{sent:?}");
    for ack in &sent {
        let sack = ack.header.extensions.selective_ack.unwrap();
        // Bit 0 is packet 3 (two past the last in order), bit 9 is packet 12.
        assert_eq!(sack.as_bytes(), [0b1, 0b10]);
    }
    let stats = t._socket.stats();
    assert_eq!(stats.ack_packets_sent.load(Ordering::Relaxed), 3);
    assert_eq!(stats.acks_with_sack.load(Ordering::Relaxed), 3);
    assert_eq!(stats.sack_bytes_sent.load(Ordering::Relaxed), 6);
    assert_eq!(stats.immediate_acks.load(Ordering::Relaxed), 1);
    assert_eq!(stats.duplicate_acks_sent.load(Ordering::Relaxed), 2);
}

// A packet is lost and exactly three behind it arrive in one read, the
// last of a burst. One ACK naming three packets would count as one
// duplicate at the sender, which resends early on three; so the three
// packets get three ACKs, the same SACK on each.
#[tokio::test]
async fn three_packets_past_a_hole_in_one_read_get_three_acks() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);

    t.send_data(3, ack_nr, "c");
    t.send_data(4, ack_nr, "d");
    t.send_data(5, ack_nr, "e");
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 3, "{sent:?}");
    for ack in &sent {
        assert_eq!(ack.header.htype, State);
        assert_eq!(ack.header.ack_nr, 1.into());
        let sack = ack.header.extensions.selective_ack.unwrap();
        assert_eq!(sack.as_bytes(), [0b111]);
    }
    let repeated = |t: &_| {
        let t: &crate::stream_dispatch::tests::TestVsock = t;
        t._socket.stats().duplicate_acks_sent.load(Ordering::Relaxed)
    };
    assert_eq!(repeated(&t), 2);

    // A fourth packet makes the SACK name more than three, which a sender
    // acts on at once: one ACK is enough again.
    t.send_data(6, ack_nr, "f");
    t.send_data(7, ack_nr, "g");
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(
        sent[0].header.extensions.selective_ack.unwrap().as_bytes(),
        [0b11111]
    );
    assert_eq!(repeated(&t), 2);
}

// A sender with packets 101 to 105 out and none acknowledged.
async fn sender_with_five_out() -> (crate::stream_dispatch::tests::TestVsock, UtpHeader) {
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    let ack = UtpHeader {
        htype: State,
        seq_nr: 0.into(),
        ack_nr: 100.into(),
        wnd_size: 1024 * 1024,
        ..Default::default()
    };
    t.send_msg(ack, "");
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"aaaaabbbbbcccccdddddeeeee")
        .await
        .unwrap();
    let sent = t.poll_until_sent(5).await;
    let seqs: Vec<u16> = sent
        .iter()
        .filter(|m| m.header.htype == Data)
        .map(|m| m.header.seq_nr.0)
        .collect();
    assert_eq!(seqs, [101, 102, 103, 104, 105]);
    (t, ack)
}

// The ACKs a receiver sends for packets 1, 3, 4 and 5 (2 lost), given to a
// sender for whom they are 101, 103, 104 and 105. `earlier` has packet 1
// read, and acknowledged, before the others arrive. Returns how many ACKs
// the receiver sent and whether the sender resent 102 on them.
async fn acks_for_a_tail_hole_reach_a_sender(earlier: bool) -> (usize, bool) {
    let mut rx = make_test_vsock(Default::default(), false);
    let ack_nr = rx.vsock.seq_nr - 1;
    let mut acks = Vec::new();
    rx.send_data(1, ack_nr, "a");
    if earlier {
        rx.poll_once_assert_pending().await;
        rx.send_queued().await;
        acks.extend(rx.take_sent());
    }
    for seq in 3..=5u16 {
        rx.send_data(seq, ack_nr, "x");
    }
    rx.poll_once_assert_pending().await;
    rx.send_queued().await;
    acks.extend(rx.take_sent());

    let (mut tx, template) = sender_with_five_out().await;
    for ack in &acks {
        assert_eq!(ack.header.htype, State);
        assert_eq!(ack.header.ack_nr, 1.into());
        tx.send_msg(
            UtpHeader {
                ack_nr: ack.header.ack_nr + 100,
                extensions: ack.header.extensions,
                ..template
            },
            "",
        );
        // Each ACK in its own read at the sender.
        tx.poll_once_assert_pending().await;
    }
    tx.send_queued().await;
    let resent = tx
        .take_sent()
        .iter()
        .any(|m| m.header.htype == Data && m.header.seq_nr == 102.into());
    (acks.len(), resent)
}

// Packet 1 was acknowledged in an earlier read: the three ACKs for 3, 4 and
// 5 all repeat it, and the sender resends 102 on the third.
#[tokio::test(start_paused = true)]
async fn three_past_a_hole_after_an_earlier_ack_draw_a_fast_resend() {
    assert_eq!(acks_for_a_tail_hole_reach_a_sender(true).await, (4, true));
}

// Packet 1 arrives in the same read as 3, 4 and 5. The first ACK moves the
// sender's cumulative ACK and is no duplicate, so three more follow it and
// the sender resends 102 without waiting for its timer.
#[tokio::test(start_paused = true)]
async fn in_order_and_three_past_a_hole_in_one_read_draw_a_fast_resend() {
    assert_eq!(acks_for_a_tail_hole_reach_a_sender(false).await, (4, true));
}

// The socket takes the first of three ACKs owed for packets past a hole
// and refuses the next. The two left stay owed and go once it is writable.
#[tokio::test(start_paused = true)]
async fn duplicate_acks_owed_survive_a_refused_send() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);

    for seq in 3..=5u16 {
        t.send_data(seq, ack_nr, "x");
    }
    t.transport.refuse_after(1);
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);
    assert_eq!(t.transport.refused(), 1);

    t.transport.set_poll_send_pending(false);
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 2, "{sent:?}");
    for ack in &sent {
        assert_eq!(ack.header.htype, State);
        assert_eq!(ack.header.ack_nr, 1.into());
        assert_eq!(
            ack.header.extensions.selective_ack.unwrap().as_bytes(),
            [0b111]
        );
    }

    // Nothing more is owed.
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
}

// Packets that arrive in order owe one ACK a read however many they are.
#[tokio::test]
async fn packets_in_order_in_one_read_get_one_ack() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.send_data(2, ack_nr, "b");
    t.send_data(3, ack_nr, "c");
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);
    assert_eq!(
        t._socket.stats().duplicate_acks_sent.load(Ordering::Relaxed),
        0
    );
}

// The ACKs owed for packets 3, 4 and 5 past a hole are counted the same
// whether the socket takes them in one poll or in two: one immediate and
// two repeats.
async fn ack_counts_for_three_past_a_hole(refuse: bool) -> (u64, u64) {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);
    let stats = t._socket.stats();
    let immediate = stats.immediate_acks.load(Ordering::Relaxed);
    let duplicate = stats.duplicate_acks_sent.load(Ordering::Relaxed);

    for seq in 3..=5u16 {
        t.send_data(seq, ack_nr, "x");
    }
    if refuse {
        t.transport.refuse_after(1);
    }
    t.poll_once_assert_pending().await;
    if refuse {
        assert_eq!(t.transport.refused(), 1);
        t.transport.set_poll_send_pending(false);
        t.poll_once_assert_pending().await;
    }
    assert_eq!(t.take_sent().len(), 3);
    let stats = t._socket.stats();
    (
        stats.immediate_acks.load(Ordering::Relaxed) - immediate,
        stats.duplicate_acks_sent.load(Ordering::Relaxed) - duplicate,
    )
}

#[tokio::test(start_paused = true)]
async fn three_past_a_hole_count_one_immediate_ack_and_two_repeats() {
    assert_eq!(ack_counts_for_three_past_a_hole(false).await, (1, 2));
}

#[tokio::test(start_paused = true)]
async fn a_refused_send_does_not_change_the_ack_counts() {
    assert_eq!(ack_counts_for_three_past_a_hole(true).await, (1, 2));
}
