// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the SACKs after a retransmission timeout resend, and what they leave
// to the go-back; and what a reordering wait that runs out with no ACK
// resends.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader, selective_ack::SelectiveAck},
    seq_nr::SeqNr,
    stream_dispatch::{
        StreamArgs,
        tests::{TestVsock, calc_mtu_for_mss, make_test_vsock, make_test_vsock_args},
    },
    test_util::{env::MockUtpEnvironment, setup_test_logging},
    traits::UtpEnvironment,
};

fn ack(ack_nr: u16, sacked: &[usize]) -> UtpHeader {
    UtpHeader {
        htype: State,
        seq_nr: 0.into(),
        ack_nr: ack_nr.into(),
        wnd_size: 1 << 20,
        extensions: crate::raw::Extensions {
            selective_ack: SelectiveAck::new_test(sacked.iter().copied()),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn sent(t: &mut TestVsock) -> Vec<u16> {
    t.take_sent().iter().map(|p| p.header.seq_nr.0).collect()
}

// holes_past_the_timer_resent_head_go_on_the_sack_once, with the rewound
// head five packets short of the sequence wrap: the holes 65535, 0 and 1 go
// on the SACK, once each.
#[tokio::test]
async fn holes_past_the_rewound_head_go_once_across_the_wrap() {
    setup_test_logging();
    const F: u16 = 65530;
    let s = |k: u16| F.wrapping_add(k);
    let env = MockUtpEnvironment::new();
    let remote = UtpHeader {
        htype: State,
        seq_nr: 1.into(),
        ack_nr: SeqNr(F) - 1,
        wnd_size: 1024 * 1024,
        ..Default::default()
    };
    let now = env.now();
    env.increment_now(Duration::from_secs(1));
    let args = StreamArgs::new_outgoing(&remote, now, env.now());
    let mut t = make_test_vsock_args(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        args,
        env,
    );
    let rto = Duration::from_secs(1);
    t.vsock.rtte.force_timeout(rto);
    let before = F.wrapping_sub(1);
    t.send_msg(ack(before, &[]), "");
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[b'x'; 5 * 12])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), (0..12).map(s).collect::<Vec<u16>>());

    t.send_msg(ack(before, &[0, 1, 2, 3]), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), vec![s(0)]);

    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), vec![s(0)]);

    t.send_msg(ack(before, &[0, 1, 2, 3, 7, 8, 9, 10]), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), vec![s(5), s(6), s(7)]);

    t.send_msg(ack(s(0), &[0, 1, 2, 6, 7, 8, 9]), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), Vec::<u16>::new());
    let sends: Vec<usize> = t
        .vsock
        .user_tx_segments
        .iter_mut_for_sending(None)
        .map(|s| s.send_count())
        .collect();
    assert_eq!(sends, vec![2, 2, 2]);
}

// 101 to 116 go out. A SACK shows 101 and 106 lost: both are fast-resent,
// the mark moves to 107. The timer resends 101 and rewinds to it. A SACK
// then shows 111 and 112 lost, with four packets past them. With
// `lost_ahead`, 117 to 120 went after 106's resend and arrived too, so 106
// is lost again.
//
// The SACK resends nothing: 106, past the rewound head and below the mark,
// stops the walk, and 111 and 112 wait for the go-back, which sends 106 and
// then them, each once. libtorrent v2.0.15 steps over 106, no candidate
// under the mark, and fast-resends 111 and 112 on that SACK
// (utp_stream.cpp:1235, 1269-1273, 1340-1355). Weaver's go-back resends
// from one watermark, so a packet the walk stepped over would have no send
// left to it once a resend past it moved the watermark.
async fn hole_behind_a_packet_resent_before_the_timeout(lost_ahead: bool) {
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
    t.vsock.congestion_controller.set_window(5 * 64);
    t.send_msg(ack(100, &[]), "");
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[b'x'; 5 * 16])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), (101..=116).collect::<Vec<u16>>());

    // The pacer refills.
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(ack(100, &[0, 1, 2, 3, 5, 6, 7, 8]), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), vec![101, 106]);
    assert_eq!(t.vsock.fast_resend_seq_nr, 107.into());

    let mut sacked = vec![0, 1, 2, 3, 5, 6, 7, 8, 11, 12, 13, 14];
    let mut after_105 = vec![0, 1, 2, 3, 6, 7, 8, 9];
    let mut after_110 = vec![1, 2, 3, 4];
    if lost_ahead {
        t.env.increment_now(Duration::from_millis(100));
        t.stream
            .as_mut()
            .unwrap()
            .write_all(&[b'x'; 5 * 4])
            .await
            .unwrap();
        t.poll_once_assert_pending().await;
        assert_eq!(sent(&mut t), (117..=120).collect::<Vec<u16>>());
        sacked.extend([15, 16, 17, 18]);
        after_105.extend([10, 11, 12, 13]);
        after_110.extend([5, 6, 7, 8]);
    }

    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), vec![101]);

    t.send_msg(ack(100, &sacked), "");
    t.poll_once_assert_pending().await;
    if lost_ahead {
        let lost: Vec<u16> = t
            .vsock
            .user_tx_segments
            .iter_mut_for_sending(None)
            .filter(|s| s.is_lost())
            .map(|s| s.seq_nr().0)
            .collect();
        assert!(lost.contains(&106), "106 is lost again: {lost:?}");
    }
    assert_eq!(sent(&mut t), Vec::<u16>::new());

    // The resent 101 arrives: the go-back sends 106, and once it arrives,
    // 111 and 112.
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(ack(105, &after_105), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), vec![106]);
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(ack(110, &after_110), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), vec![111, 112]);
    let last = if lost_ahead { 120 } else { 116 };
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(ack(last, &[]), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), Vec::<u16>::new());
}

#[tokio::test]
async fn a_hole_behind_a_resend_waits_for_the_go_back() {
    hole_behind_a_packet_resent_before_the_timeout(false).await;
}

#[tokio::test]
async fn a_hole_behind_a_resend_lost_again_waits_for_the_go_back() {
    hole_behind_a_packet_resent_before_the_timeout(true).await;
}

// 101 is lost, resent, and outrun: 117, sent after the resend, arrives.
// The ACK that shows it also shows 106-110 lost and spends its budget on
// them. With no ACK since, the reordering wait running out is all that says
// the resend was lost too. The resend that wait grants asks the recovery
// window, as libtorrent resends only on an ACK; one an ACK grants does not.
async fn reorder_wait_resend(window_full: bool) {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        false,
    );
    t.vsock.rtte.force_timeout(Duration::from_secs(10));
    t.vsock.congestion_controller.set_window(5 * 64);
    t.send_msg(ack(100, &[]), "");
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[b'x'; 5 * 16])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), (101..=116).collect::<Vec<_>>());

    t.env.increment_now(Duration::from_millis(10));
    t.send_msg(ack(100, &[0, 1, 2, 3]), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), [101]);

    t.env.increment_now(Duration::from_millis(50));
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[b'x'; 5])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), [117]);

    // 102-105 and 111-117 arrived.
    let sacked: Vec<usize> = (0..4).chain(9..16).collect();
    // Long enough for the pacer to allow the five resends at once.
    t.env.increment_now(Duration::from_millis(200));
    t.send_msg(ack(100, &sacked), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), (106..=110).collect::<Vec<_>>());
    assert_eq!(t.vsock.fast_resend_budget, 0);
    let wait_ends = t
        .vsock
        .recovery
        .reorder_at()
        .expect("101 waits out the reordering");

    let mss = t.vsock.segment_sizes.mss() as usize;
    if window_full {
        // New data keeps the recovery window full up to the wait's end.
        t.stream
            .as_mut()
            .unwrap()
            .write_all(&[b'x'; 5 * 1000])
            .await
            .unwrap();
        while t.env.now() + Duration::from_millis(1) < wait_ends {
            t.env.increment_now(Duration::from_millis(1));
            t.poll_once_assert_pending().await;
            t.take_sent();
        }
        assert!(t.vsock.recovery.recovering_mut().unwrap().cwnd() <= mss);
    }

    t.env.increment_now(wait_ends - t.env.now());
    t.poll_once_assert_pending().await;
    if !window_full {
        assert_eq!(sent(&mut t), [101]);
        return;
    }
    // New data may still take the last packet's room at the window's edge.
    assert!(!sent(&mut t).contains(&101));

    // An ACK at the same moment grants the resend past the window.
    t.send_msg(ack(100, &sacked), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), [101]);
}

#[tokio::test]
async fn a_reordering_wait_alone_resends_in_the_window() {
    reorder_wait_resend(false).await;
}

#[tokio::test]
async fn a_reordering_wait_alone_does_not_resend_past_the_window() {
    reorder_wait_resend(true).await;
}

// reorder_wait_resend(window_full), with the ACK that shows 106-110 lost
// read twice in one pass (two ACKs in one read, as a batched receive hands
// them): the pass grants 10 resends and spends 5. With no ACK since, the
// reordering wait runs out and marks 101 lost. The ACKs' budget ended with
// their pass, so the wait grants the resend and it asks the window: 101 does
// not go past a full recovery window. libtorrent resends only while handling
// an ACK (v2.0.15 utp_stream.cpp:1340-1355, 2814-2833) and checks the window
// for a resend that is not a fast resend (:1990-1997).
#[tokio::test]
async fn a_budget_left_by_acks_ends_with_their_pass() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        false,
    );
    t.vsock.rtte.force_timeout(Duration::from_secs(10));
    t.vsock.congestion_controller.set_window(5 * 64);
    t.send_msg(ack(100, &[]), "");
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[b'x'; 5 * 16])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), (101..=116).collect::<Vec<_>>());

    t.env.increment_now(Duration::from_millis(10));
    t.send_msg(ack(100, &[0, 1, 2, 3]), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), [101]);

    t.env.increment_now(Duration::from_millis(50));
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[b'x'; 5])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), [117]);

    // 102-105 and 111-117 arrived: 106-110 are lost.
    let sacked: Vec<usize> = (0..4).chain(9..16).collect();
    t.env.increment_now(Duration::from_millis(200));
    t.send_msg(ack(100, &sacked), "");
    t.send_msg(ack(100, &sacked), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent(&mut t), (106..=110).collect::<Vec<_>>());
    assert_eq!(t.vsock.fast_resend_budget, 5, "the ACKs' budget left");
    let wait_ends = t
        .vsock
        .recovery
        .reorder_at()
        .expect("101 waits out the reordering");

    let mss = t.vsock.segment_sizes.mss() as usize;
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[b'x'; 5 * 1000])
        .await
        .unwrap();
    while t.env.now() + Duration::from_millis(1) < wait_ends {
        t.env.increment_now(Duration::from_millis(1));
        t.poll_once_assert_pending().await;
        t.take_sent();
    }
    let rec_cwnd = t.vsock.recovery.recovering_mut().unwrap().cwnd();
    let pipe = t
        .vsock
        .recovery
        .recovering_mut()
        .unwrap()
        .pipe_estimate
        .pipe;
    let cc = t.vsock.congestion_controller.window();
    let unsegmented = t.vsock.this_poll.unsegmented_data;
    assert!(
        rec_cwnd <= mss,
        "rec cwnd {rec_cwnd} pipe {pipe} cc {cc} mss {mss} unsegmented {unsegmented} segs {}",
        t.vsock.user_tx_segments.total_len_packets()
    );
    assert_eq!(
        t.vsock.fast_resend_budget, 0,
        "the ACKs' budget ended with their pass"
    );

    t.env.increment_now(wait_ends - t.env.now());
    t.poll_once_assert_pending().await;
    let at_wait_end = sent(&mut t);
    assert!(
        !at_wait_end.contains(&101),
        "no ACK, full window, yet: {at_wait_end:?}"
    );
}
