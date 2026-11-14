// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-14.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::{PROBE_WINDOW, make_test_vsock},
};

#[tokio::test]
async fn abandoning_a_probe_counts_its_last_timeout_without_backoff() {
    for retries in [0, 1] {
        let mut t = make_test_vsock(
            SocketOpts {
                mtu_probe_max_retransmissions: Some(retries),
                disable_nagle: true,
                ..Default::default()
            },
            false,
        );
        // A window above three floor packets: the second packet is a probe.
        t.vsock.congestion_controller.set_window(PROBE_WINDOW);
        let floor = usize::from(t.vsock.segment_sizes.mss());
        let probe = usize::from(t.vsock.segment_sizes.probe_size());
        t.stream
            .as_mut()
            .unwrap()
            .write_all(&vec![7; floor + probe])
            .await
            .unwrap();
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        let sizes: Vec<usize> = sent.iter().map(|m| m.payload().len()).collect();
        assert_eq!(sizes, [floor, probe]);
        t.send_msg(
            UtpHeader {
                htype: State,
                seq_nr: 1.into(),
                ack_nr: sent[0].header.seq_nr,
                wnd_size: 1024 * 1024,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;
        t.assert_sent_empty();
        let window = t.vsock.congestion_controller.window();

        for expiry in 1..=retries + 1 {
            t.env.increment_now(t.vsock.rtte.retransmission_timeout());
            t.poll_once_assert_pending().await;
            assert_eq!(t._socket.stats().snapshot().timeouts, expiry as u64);
            let sent = t.take_sent();
            assert!(!sent.is_empty());
            // Each expiry resends the probe whole; the last gives up on its
            // size, so it goes as ordinary data and the ceiling falls below it.
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].payload().len(), probe);
            assert_eq!(
                usize::from(t.vsock.segment_sizes.max_ss()) < probe,
                expiry > retries
            );
            assert_eq!(t.vsock.rtte.timeouts(), 0);
            assert_eq!(t.vsock.congestion_controller.window(), window);
        }
        t.poll_once_assert_pending().await;
        assert_eq!(t._socket.stats().snapshot().timeouts, (retries + 1) as u64);
        assert_eq!(t._socket.stats().snapshot().packets_lost, 0);
    }
}

// A probe goes out with a floor packet's worth of data written behind it.
// Once the window has room, that data goes at the floor size while the
// probe is unanswered, as libtorrent sends packets of m_mtu_floor behind its
// probe (utp_stream.cpp v2.0.15:1497-1506, 1528-1532), so a loss ahead of
// the probe gets the SACKs that fast-resend it.
#[tokio::test]
async fn data_written_behind_a_probe_goes_out_at_the_floor_size() {
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    t.vsock.congestion_controller.set_window(PROBE_WINDOW);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; floor + probe + floor])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let sent = |t: &mut super::TestVsock| -> Vec<(u16, usize)> {
        t.take_sent()
            .iter()
            .map(|m| (m.header.seq_nr.0, m.payload().len()))
            .collect()
    };
    let first = sent(&mut t);
    assert_eq!(first[..2], [(101, floor), (102, probe)]);
    t.vsock.congestion_controller.set_window(64 * 1024);
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
    let mut all = first;
    all.extend(sent(&mut t));
    assert_eq!(all, [(101, floor), (102, probe), (103, floor)]);
    assert!(t.vsock.user_tx_segments.has_live_mtu_probe());
}

// What is written behind the probe put in flight.
#[derive(PartialEq)]
enum Behind {
    Nothing,
    // A floor packet's worth, held by a window that has room for nothing
    // past the probe.
    Unsent,
    // A floor packet, sent once the ACK before the probe makes room, as
    // libtorrent sends packets behind its probe (utp_stream.cpp
    // v2.0.15:1528-1532).
    Sent,
}

// Put a probe in flight, with what is written behind it.
async fn probe_in_flight(
    behind: Behind,
    retries: usize,
    forced_rto: Option<Duration>,
) -> (super::TestVsock, usize) {
    let mut t = make_test_vsock(
        SocketOpts {
            mtu_probe_max_retransmissions: Some(retries),
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    if let Some(rto) = forced_rto {
        t.vsock.rtte.force_timeout(rto);
    }
    // A window above three floor packets: the second packet is a probe.
    t.vsock.congestion_controller.set_window(PROBE_WINDOW);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    let extra = if behind == Behind::Nothing { 0 } else { floor };
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; floor + probe + extra])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    let sizes: Vec<usize> = sent.iter().map(|m| m.payload().len()).collect();
    assert_eq!(sizes, [floor, probe]);
    assert_eq!(sent[1].header.seq_nr, 102.into());
    if behind == Behind::Unsent {
        t.vsock.congestion_controller.set_window(probe);
    }
    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: sent[0].header.seq_nr,
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    let sent: Vec<(u16, usize)> = sent
        .iter()
        .map(|m| (m.header.seq_nr.0, m.payload().len()))
        .collect();
    if behind == Behind::Sent {
        assert_eq!(sent, [(103, floor)]);
    } else {
        assert_eq!(sent, []);
    }
    (t, floor)
}

fn expire_retransmit(t: &mut super::TestVsock) {
    use crate::traits::UtpEnvironment;
    let deadline = t.vsock.timers.retransmit.poll_at().unwrap();
    t.env.increment_now(deadline.duration_since(t.env.now()));
}

#[tokio::test]
async fn an_unsent_probe_does_not_hide_the_data_timeout() {
    let mut t = make_test_vsock(
        SocketOpts {
            mtu_probe_max_retransmissions: Some(0),
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    // A window above three floor packets lets the probe be cut. The floor
    // goes first; the peer then shrinks its window below the floor and the
    // probe together, so the probe is cut and waits unsent.
    t.vsock.congestion_controller.set_window(PROBE_WINDOW);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; floor])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1);
    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: (floor + probe - 1) as u32,
            ..Default::default()
        },
        "",
    );
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; probe])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 2);
    assert_eq!(t.vsock.user_tx_segments.outstanding_packets(), 1);
    expire_retransmit(&mut t);
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.rtte.timeouts(), 1);
    assert_eq!(t.vsock.congestion_controller.window(), floor);
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.seq_nr, 101.into());
    assert_eq!(sent[0].payload().len(), floor);
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 2);
}

#[tokio::test]
async fn a_probe_with_unsent_queued_data_keeps_its_timeout_exemption() {
    let (mut t, _) = probe_in_flight(Behind::Unsent, 1, None).await;
    // The segment cut behind the probe is queued, but not in flight.
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 2);
    assert_eq!(t.vsock.user_tx_segments.outstanding_packets(), 1);
    let window = t.vsock.congestion_controller.window();
    expire_retransmit(&mut t);
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.seq_nr, 102.into());
    assert_eq!(t.vsock.rtte.timeouts(), 0);
    assert_eq!(t.vsock.congestion_controller.window(), window);
}

#[tokio::test]
async fn a_probe_with_other_sent_data_takes_a_real_timeout() {
    let (mut t, floor) = probe_in_flight(Behind::Sent, 1, None).await;
    assert_eq!(t.vsock.user_tx_segments.outstanding_packets(), 2);
    expire_retransmit(&mut t);
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.rtte.timeouts(), 1);
    assert_eq!(t.vsock.congestion_controller.window(), floor);
    assert_eq!(t.take_sent()[0].header.seq_nr, 102.into());
}

#[tokio::test]
async fn a_probe_with_an_unacked_fin_is_not_abandoned_as_a_lone_probe() {
    // Exercise the RTO before the local-FIN final-chance timer expires.
    let (mut t, floor) =
        probe_in_flight(Behind::Nothing, 0, Some(Duration::from_millis(500))).await;
    // Dropping both halves sends FIN once all buffered data has been sent.
    drop(t.stream.take());
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.get_type(), Fin);
    assert_eq!(sent[0].header.seq_nr, 103.into());
    expire_retransmit(&mut t);
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.rtte.timeouts(), 1);
    assert_eq!(t.vsock.congestion_controller.window(), floor);
    let sent = t.take_sent();
    assert_eq!(sent[0].header.seq_nr, 102.into());
    assert_eq!(
        sent[0].payload().len(),
        usize::from(t.vsock.segment_sizes.probe_size())
    );
}

// A full send buffer refilled by each ACK hands the stream one packet's
// worth at a time, so each pass cuts one full segment and holds a part-filled
// rest. The probe that falls due waits for written data enough to fill it,
// and full segments go out meanwhile; a pass that cuts no probe leaves it
// due. Counted: probes sent, and the segment size they raise.
#[tokio::test]
async fn a_probe_waits_for_written_data_enough_to_fill_it() {
    let mut t = make_test_vsock(SocketOpts::default(), false);
    // A packet or two in flight never fills the window, so it never grows
    // (libtorrent v2.0.15 utp_stream.cpp:3286-3288 grows only a saturated
    // window). It is set above three floor packets, where probes may go.
    t.vsock.congestion_controller.set_window(PROBE_WINDOW);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let w = t.stream.as_mut().unwrap();
    w.write_all(&vec![7; floor + 100]).await.unwrap();
    let mut probes = 0;
    let mut sent_packets = 0;
    // Each round ACKs what went out the round before, so data stays in
    // flight and a short tail waits for more.
    let mut acked = None;
    let mut to_ack = None;
    for _ in 0..64 {
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        sent_packets += sent.len();
        probes += sent
            .iter()
            .filter(|m| m.header.get_type() == Data && m.payload().len() > floor)
            .count();
        if let Some(ack_nr) = to_ack.filter(|&a| Some(a) != acked) {
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
            acked = Some(ack_nr);
        }
        to_ack = Some(t.vsock.last_sent_seq_nr);
        t.env.increment_now(Duration::from_millis(50));
        t.stream
            .as_mut()
            .unwrap()
            .write_all(&vec![7; floor])
            .await
            .unwrap();
    }
    // A tail too short to fill a segment waits a round for the next write,
    // so a packet goes out every other round.
    assert!(sent_packets >= 30, "{sent_packets} packets sent");
    assert!(probes >= 3, "{probes} probes in {sent_packets} packets");
    assert!(
        usize::from(t.vsock.segment_sizes.mss()) > floor + floor / 2,
        "the segment size stayed at {}",
        t.vsock.segment_sizes.mss()
    );
}
