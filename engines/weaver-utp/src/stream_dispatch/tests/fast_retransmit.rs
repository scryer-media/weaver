// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-10.

use std::time::Duration;

use futures::FutureExt;
use tokio::io::AsyncWriteExt;
use tracing::trace;

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader, selective_ack::SelectiveAck},
    stream_dispatch::tests::{calc_mtu_for_mss, make_test_vsock},
    test_util::setup_test_logging,
};

#[tokio::test]
async fn test_fast_retransmit() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        false,
    );

    // Set a large retransmission timeout so we know fast retransmit is triggering, not RTO
    t.vsock.rtte.force_timeout(Duration::from_secs(10));

    let mut ack = UtpHeader {
        htype: State,
        seq_nr: 0.into(),
        ack_nr: 99.into(),
        wnd_size: 1024,
        ..Default::default()
    };

    // Allow sending by setting window size
    t.send_msg(ack, "");

    // Write enough data to generate multiple packets
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"helloworld")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(Data, seq_nr = 101, payload = "hello"),
            cmphead!(Data, seq_nr = 102, payload = "world")
        ]
    );

    // Simulate receiving duplicate ACKs (as if first packet was lost but later ones arrived)
    ack.ack_nr = 101.into();

    // First ACK
    t.send_msg(ack, "");

    // First duplicate ACK
    t.send_msg(ack, "");

    // Second duplicate ACK
    t.send_msg(ack, "");

    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not retransmit yet");

    // Third duplicate ACK should trigger fast retransmit
    t.send_msg(ack, "");

    t.poll_once_assert_pending().await;
    trace!("s");

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(Data, seq_nr = 102, payload = "world")],
        "Should have retransmitted second packet"
    );
}

#[tokio::test]
async fn test_duplicate_ack_counted_only_on_st_state() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        false,
    );

    // Set a large retransmission timeout so we know fast retransmit is triggering, not RTO
    t.vsock.rtte.force_timeout(Duration::from_secs(10));

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 0.into(),
            ack_nr: 99.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Write enough data to generate multiple packets
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"helloworld")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    // Should have sent the data
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(Data, seq_nr = 101, ack_nr = 0, payload = "hello"),
            cmphead!(Data, seq_nr = 102, ack_nr = 0, payload = "world")
        ]
    );
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 2);

    // Send three duplicate ACKs using different packet types
    let mut header = UtpHeader {
        htype: State,
        seq_nr: 0.into(),
        ack_nr: 101.into(),
        wnd_size: 1024,
        ..Default::default()
    };

    // First normal ST_STATE ACK
    t.send_msg(header, "");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);

    // Change to ST_DATA with same ACK - shouldn't count as duplicate
    header.htype = Data;
    header.seq_nr += 1;
    t.send_msg(header, "a");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(State, seq_nr = 103, ack_nr = 1)]
    );

    // Another ST_DATA - shouldn't count
    header.seq_nr += 1;
    t.send_msg(header, "b");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(State, seq_nr = 103, ack_nr = 2)]
    );

    // ST_FIN with same ACK - shouldn't count
    header.htype = Fin;
    header.seq_nr += 1;
    t.send_msg(header, "");

    t.poll_once_assert_pending().await;

    // Duplicate ACK count should be 0 since non-ST_STATE packets don't count
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(State, seq_nr = 103, ack_nr = 3)],
        "remote FIN is acknowledged without closing our writer"
    );

    // Now send three ST_STATE duplicates
    header.htype = State;
    t.send_msg(header, "");
    t.send_msg(header, "");
    t.send_msg(header, "");

    t.poll_once_assert_pending().await;

    // Now we should see duplicate ACKs counted and fast retransmit triggered
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(Data, seq_nr = 102, ack_nr = 3, payload = "world")],
        "Should have triggered fast retransmit"
    );
    header.seq_nr = 4.into();
    header.ack_nr = 102.into();
    t.send_msg(header, "");
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    let (_reader, mut writer) = t.stream.take().unwrap().split();
    writer.flush().await.unwrap();
    let mut shutdown = Box::pin(writer.shutdown());
    assert!(shutdown.as_mut().now_or_never().is_none());
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent(), vec![cmphead!(Fin, seq_nr = 103, ack_nr = 3)]);
    header.ack_nr = 103.into();
    t.send_msg(header, "");
    t.poll_once_assert_ready().await.unwrap();
    shutdown.await.unwrap();
}

// A packet the retransmission timer resent is not fast-resent after it, as
// in libtorrent: a SACK that then marks it lost resends the packets after it
// instead, and only the timer sends the head again.
#[tokio::test]
async fn a_packet_the_timer_resent_is_not_fast_resent() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        false,
    );
    let rto = Duration::from_secs(1);
    t.vsock.rtte.force_timeout(rto);
    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 0.into(),
            ack_nr: 100.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"aaaaabbbbbcccccdddddeeeeefffff")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let first: Vec<u16> = t.take_sent().iter().map(|p| p.header.seq_nr.0).collect();
    assert_eq!(first, vec![101, 102, 103, 104, 105, 106]);

    // The timer resends the head.
    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(Data, seq_nr = 101, payload = "aaaaa")]
    );

    // 103 to 106 arrive, 101 and 102 do not: more than three packets past
    // both are SACKed.
    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 0.into(),
            ack_nr: 100.into(),
            wnd_size: 1024,
            extensions: crate::raw::Extensions {
                selective_ack: SelectiveAck::new_test([1, 2, 3, 4]),
                ..Default::default()
            },
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    let resent: Vec<u16> = t.take_sent().iter().map(|p| p.header.seq_nr.0).collect();
    assert!(resent.contains(&102), "{resent:?}");
    assert!(!resent.contains(&101), "{resent:?}");
    let head = t
        .vsock
        .user_tx_segments
        .iter_mut_for_sending(None)
        .next()
        .unwrap()
        .send_count();
    assert_eq!(head, 2);
}

// A packet the timer's go-back resent is not fast-resent by the next SACK,
// as in libtorrent: the go-back walks the fast-resend mark along with it.
#[tokio::test]
async fn a_packet_the_go_back_resent_is_not_fast_resent() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        false,
    );
    let rto = Duration::from_secs(1);
    t.vsock.rtte.force_timeout(rto);
    let ack = |ack_nr: u16, sacked: &[usize]| UtpHeader {
        htype: State,
        seq_nr: 0.into(),
        ack_nr: ack_nr.into(),
        wnd_size: 1024,
        extensions: crate::raw::Extensions {
            selective_ack: SelectiveAck::new_test(sacked.iter().copied()),
            ..Default::default()
        },
        ..Default::default()
    };
    t.send_msg(ack(100, &[]), "");
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"aaaaabbbbbcccccdddddeeeeefffffggggghhhhh")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let first: Vec<u16> = t.take_sent().iter().map(|p| p.header.seq_nr.0).collect();
    assert_eq!(first, vec![101, 102, 103, 104, 105, 106, 107, 108]);

    // The timer resends the head, and its ACK lets the go-back resume.
    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(Data, seq_nr = 101, payload = "aaaaa")]
    );
    t.send_msg(ack(101, &[]), "");
    t.poll_once_assert_pending().await;
    let go_back: Vec<u16> = t.take_sent().iter().map(|p| p.header.seq_nr.0).collect();
    assert_eq!(go_back.first(), Some(&102), "{go_back:?}");

    // 105 to 108 arrive, everything the go-back resent is still missing.
    t.send_msg(ack(101, &[2, 3, 4, 5]), "");
    t.poll_once_assert_pending().await;
    let resent: Vec<u16> = t.take_sent().iter().map(|p| p.header.seq_nr.0).collect();
    for seq in &go_back {
        assert!(
            !resent.contains(seq),
            "go-back {go_back:?} resent {resent:?}"
        );
    }
    let most = t
        .vsock
        .user_tx_segments
        .iter_mut_for_sending(None)
        .map(|s| s.send_count())
        .max()
        .unwrap();
    assert_eq!(most, 2, "go-back {go_back:?} resent {resent:?}");
}
