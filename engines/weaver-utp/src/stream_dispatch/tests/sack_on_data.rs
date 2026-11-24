// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// While packets are held past a hole, every packet that can carry the SACK
// does, data packets included, as libtorrent's do (v2.0.15
// utp_stream.cpp:1536-1548, 1647-1648, 1729-1736).

use std::sync::atomic::Ordering;

use tokio::io::AsyncWriteExt;

use crate::{
    raw::Type::{Data, State},
    stream_dispatch::tests::make_test_vsock,
};

// A data packet sent in the pass that took the out-of-order packets clears
// the owed ACK, so it must carry the SACK itself: nothing else would.
#[tokio::test(start_paused = true)]
async fn a_pass_that_sends_data_still_reports_the_sack() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    // Our side has a small message to send (a REQUEST, say).
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"request")
        .await
        .unwrap();
    // 2 is lost; 3..=6 arrive past the hole.
    t.send_data(1, ack_nr, "a");
    for seq in 3..=6u16 {
        t.send_data(seq, ack_nr, "x");
    }
    t.poll_once_assert_pending().await;
    t.send_queued().await;
    let sent = t.take_sent();
    assert!(
        sent.iter().any(|m| m.header.htype == Data),
        "the request went out in this pass: {sent:?}"
    );
    let with_sack: Vec<_> = sent
        .iter()
        .filter_map(|m| m.header.extensions.selective_ack)
        .collect();
    assert!(
        !with_sack.is_empty(),
        "no packet sent after the drain carries the SACK of 3..=6: {sent:?}"
    );
    // Bit 0 is packet 3: four packets held, one byte.
    assert_eq!(with_sack[0].as_bytes(), [0b1111]);
    let stats = t._socket.stats();
    assert_eq!(
        stats.data_packets_with_sack.load(Ordering::Relaxed)
            + stats.acks_with_sack.load(Ordering::Relaxed),
        with_sack.len() as u64
    );
}

// A hole with exactly three packets past it, in a pass that also sends
// data. The data packet carries the SACK, but a sender counts neither a
// data packet nor a SACK of three toward an early resend, so the three
// packets still get their three ACKs. Without them the hole would wait for
// the sender's timer.
#[tokio::test(start_paused = true)]
async fn a_pass_that_sends_data_still_sends_the_duplicate_acks_owed() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"request")
        .await
        .unwrap();
    // 2 is lost; 3, 4 and 5 arrive past the hole.
    t.send_data(1, ack_nr, "a");
    for seq in 3..=5u16 {
        t.send_data(seq, ack_nr, "x");
    }
    t.poll_once_assert_pending().await;
    t.send_queued().await;
    let sent = t.take_sent();
    let data: Vec<_> = sent.iter().filter(|m| m.header.htype == Data).collect();
    assert_eq!(data.len(), 1, "{sent:?}");
    assert_eq!(
        data[0].header.extensions.selective_ack.unwrap().as_bytes(),
        [0b111]
    );
    let acks: Vec<_> = sent.iter().filter(|m| m.header.htype == State).collect();
    assert_eq!(acks.len(), 3, "{sent:?}");
    for ack in &acks {
        assert_eq!(ack.header.ack_nr, 1.into());
        assert_eq!(
            ack.header.extensions.selective_ack.unwrap().as_bytes(),
            [0b111]
        );
    }

    // Nothing more is owed: a later pass with nothing new sends nothing.
    t.poll_once_assert_pending().await;
    t.send_queued().await;
    assert!(t.take_sent().is_empty());

    // A fourth packet past the hole makes the SACK enough by itself: one
    // packet carries it and nothing is repeated.
    t.send_data(6, ack_nr, "x");
    t.poll_once_assert_pending().await;
    t.send_queued().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(
        sent[0].header.extensions.selective_ack.unwrap().as_bytes(),
        [0b1111]
    );
}

// Data written once a hole is known is cut short by the SACK, so the packet
// with the SACK is no larger than a full packet without one, and each
// carries the SACK.
#[tokio::test(start_paused = true)]
async fn data_cut_while_a_hole_is_known_leaves_room_for_the_sack() {
    let mut t = make_test_vsock(Default::default(), false);
    let ack_nr = t.vsock.seq_nr - 1;
    t.send_data(1, ack_nr, "a");
    t.send_data(3, ack_nr, "c");
    t.poll_once_assert_pending().await;
    t.send_queued().await;
    // The ACK that moves the cumulative ACK to 1, and the duplicate packet
    // 3 is owed.
    let acks = t.take_sent();
    assert_eq!(acks.len(), 2, "{acks:?}");
    for ack in &acks {
        assert_eq!(ack.header.htype, State);
        assert!(ack.header.extensions.selective_ack.is_some());
    }

    let mss = usize::from(t.vsock.segment_sizes.mss());
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7u8; 3 * mss])
        .await
        .unwrap();
    let sent = t.poll_until_sent(2).await;
    let data: Vec<_> = sent.iter().filter(|m| m.header.htype == Data).collect();
    assert!(data.len() >= 2, "{sent:?}");
    for packet in data.iter().filter(|m| m.payload().len() <= mss) {
        let sack = packet
            .header
            .extensions
            .selective_ack
            .unwrap_or_else(|| panic!("a data packet without the SACK: {packet:?}"));
        assert_eq!(sack.as_bytes(), [0b1]);
        // Two bytes of extension header and one of SACK.
        assert_eq!(packet.payload().len(), mss - 3, "{packet:?}");
    }
    let stats = t._socket.stats();
    assert!(stats.data_packets_with_sack.load(Ordering::Relaxed) >= 2);
    // No ACK went out on its own after the first two: the data carried it.
    assert_eq!(stats.acks_with_sack.load(Ordering::Relaxed), 2);
}

// A full packet cut before the hole was known has no room for the SACK.
// It goes out whole, and with no later data packet to carry the SACK the
// ACK with it is still sent.
#[tokio::test(start_paused = true)]
async fn a_full_packet_cut_earlier_leaves_the_ack_owed() {
    let mut t = make_test_vsock(Default::default(), false);
    let mss = usize::from(t.vsock.segment_sizes.mss());
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7u8; 3 * mss])
        .await
        .unwrap();
    // The first flight, cut and sent with nothing held.
    let first = t.poll_until_sent(1).await;
    let last_sent = first
        .iter()
        .filter(|m| m.header.htype == Data)
        .map(|m| m.header.seq_nr)
        .next_back()
        .expect("data sent");
    assert!(
        first
            .iter()
            .all(|m| m.header.extensions.selective_ack.is_none())
    );
    // The peer acknowledges the flight, which opens the window for more of
    // what is already cut, and its packet 2 is lost.
    t.send_data(1, last_sent, "a");
    t.send_data(3, last_sent, "c");
    t.poll_once_assert_pending().await;
    t.send_queued().await;
    let sent = t.take_sent();
    let full: Vec<_> = sent
        .iter()
        .filter(|m| m.header.htype == Data && m.payload().len() == mss)
        .collect();
    assert!(!full.is_empty(), "full packets went out: {sent:?}");
    assert!(
        full.iter()
            .all(|m| m.header.extensions.selective_ack.is_none()),
        "a full packet grew past its size: {sent:?}"
    );
    assert!(
        sent.iter()
            .any(|m| m.header.htype == State && m.header.extensions.selective_ack.is_some()),
        "the SACK went out on an ACK: {sent:?}"
    );
}
