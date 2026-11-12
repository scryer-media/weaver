// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-12.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    SocketOpts,
    constants::{IPV4_HEADER, UDP_HEADER, UTP_HEADER},
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::{PROBE_WINDOW, make_test_vsock},
    test_util::{cmphead::CmpUtpHeader, setup_test_logging},
};

fn make_payload(len: usize) -> String {
    String::from_utf8(vec![b'a'; len]).unwrap()
}

const fn calc_payload_size(mtu: u16) -> usize {
    (mtu - IPV4_HEADER - UTP_HEADER - UDP_HEADER) as usize
}

fn p(mtu: u16) -> String {
    make_payload(calc_payload_size(mtu))
}

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
            if expiry <= retries {
                assert_eq!(sent.len(), 1);
                assert_eq!(sent[0].payload().len(), probe);
            } else {
                assert!(sent.iter().all(|message| message.payload().len() < probe));
            }
            assert_eq!(t.vsock.rtte.timeouts(), 0);
            assert_eq!(t.vsock.congestion_controller.window(), window);
        }
        t.poll_once_assert_pending().await;
        assert_eq!(t._socket.stats().snapshot().timeouts, (retries + 1) as u64);
        assert_eq!(t._socket.stats().snapshot().packets_lost, 0);
    }
}

// Put a probe in flight, with optional payload still waiting behind it.
async fn probe_in_flight(
    queued_data: bool,
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
    let extra = if queued_data { floor } else { 0 };
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
    let (mut t, floor) = probe_in_flight(true, 1, None).await;
    // The queued segment has payload in the write buffer, but is not in flight.
    assert!(t.vsock.user_tx_segments.enqueue(floor, false));
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
    let (mut t, floor) = probe_in_flight(true, 1, None).await;
    // Exercise a probe followed by another sent segment, independently of
    // the segmenter's policy of pausing new segmentation behind a probe.
    assert!(t.vsock.user_tx_segments.enqueue(floor, false));
    t.vsock.congestion_controller.set_window(64 * 1024);
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.seq_nr, 103.into());
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
    let (mut t, floor) = probe_in_flight(false, 0, Some(Duration::from_millis(500))).await;
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

#[tokio::test]
async fn test_mtu_probing() {
    setup_test_logging();

    // Let's pretend the actual MTU is 1280. By probing through binary search, ensure it gets found quickly.
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(non_zero_const!(1500)),
            mtu_probe_max_retransmissions: Some(0),
            vsock_tx_bufsize_bytes_initial: Some(non_zero_const!(1024 * 1024)), // so that we can write the initial large payload without blocking
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    let (_r, mut w) = t.stream.take().unwrap().split();

    // Force congestion controller to have a very high window so that it doesn't interfere.
    t.vsock.congestion_controller.set_window(1024 * 1024);

    #[derive(Debug)]
    enum TestCommand {
        ExpectSend(Vec<CmpUtpHeader>),
        Ack(u16),
        WaitForRto,
    }

    use TestCommand::*;

    const RWND: u32 = calc_payload_size(1280) as u32 * 6 - 1;

    // The data below assumes we send 3 packets with minimum segment size before sending an MTU probe.
    t.vsock
        .segment_sizes
        .set_probe_expiry_cooldown_max_packets(3);

    let commands = [
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 101, payload = p(576)),
            cmphead!(Data, seq_nr = 102, payload = p(1039)), // (1500 + 576) / 2 + 1: first probe, successful
        ]),
        Ack(102),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 103, payload = p(1039)),
            cmphead!(Data, seq_nr = 104, payload = p(1039)),
            cmphead!(Data, seq_nr = 105, payload = p(1039)),
            cmphead!(Data, seq_nr = 106, payload = p(1270)), // (1500 + 1039) / 2 + 1: second probe, successful
        ]),
        Ack(106),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 107, payload = p(1270)),
            cmphead!(Data, seq_nr = 108, payload = p(1270)),
            cmphead!(Data, seq_nr = 109, payload = p(1270)),
            cmphead!(Data, seq_nr = 110, payload = p(1386)), // (1500 + 1270) / 2 + 1: third probe, unsuccessful
        ]),
        Ack(109),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 110, payload = p(1270)),
            cmphead!(Data, seq_nr = 111, payload = p(1270)),
            cmphead!(Data, seq_nr = 112, payload = p(1270)),
            cmphead!(Data, seq_nr = 113, payload = p(1328)), // 4th probe, unsuccessful
        ]),
        Ack(112),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 113, payload = p(1270)),
            cmphead!(Data, seq_nr = 114, payload = p(1270)),
            cmphead!(Data, seq_nr = 115, payload = p(1270)),
            cmphead!(Data, seq_nr = 116, payload = p(1299)), // 5th probe, unsuccessful
        ]),
        Ack(115),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 116, payload = p(1270)),
            cmphead!(Data, seq_nr = 117, payload = p(1270)),
            cmphead!(Data, seq_nr = 118, payload = p(1270)),
            cmphead!(Data, seq_nr = 119, payload = p(1285)), // 6th probe, unsuccessful
        ]),
        Ack(118),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 119, payload = p(1270)),
            cmphead!(Data, seq_nr = 120, payload = p(1270)),
            cmphead!(Data, seq_nr = 121, payload = p(1270)),
            cmphead!(Data, seq_nr = 122, payload = p(1278)), // 7th probe, successful
        ]),
        Ack(122),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 123, payload = p(1278)),
            cmphead!(Data, seq_nr = 124, payload = p(1278)),
            cmphead!(Data, seq_nr = 125, payload = p(1278)),
            cmphead!(Data, seq_nr = 126, payload = p(1282)), // 8th probe, unsuccessful
        ]),
        Ack(125),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 126, payload = p(1278)),
            cmphead!(Data, seq_nr = 127, payload = p(1278)),
            cmphead!(Data, seq_nr = 128, payload = p(1278)),
            cmphead!(Data, seq_nr = 129, payload = p(1280)), // 9th probe, successful
        ]),
        Ack(129),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 130, payload = p(1280)),
            cmphead!(Data, seq_nr = 131, payload = p(1280)),
            cmphead!(Data, seq_nr = 132, payload = p(1280)),
            cmphead!(Data, seq_nr = 133, payload = p(1281)), // 10th probe, unsuccessful
        ]),
        Ack(132),
        WaitForRto,
        // At this point final MTU was found, and we don't need to be limited by probes, but rather by other factors.
        // In this test's case we'll get limited by remote window.
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 133, payload = p(1280)),
            cmphead!(Data, seq_nr = 134, payload = p(1280)),
            cmphead!(Data, seq_nr = 135, payload = p(1280)),
            cmphead!(Data, seq_nr = 136, payload = p(1280)),
            cmphead!(Data, seq_nr = 137, payload = p(1280)),
            cmphead!(Data, seq_nr = 138, payload = p(1279)), // limited by rwnd, no nagle
        ]),
    ];

    w.write_all(make_payload(200000).as_bytes()).await.unwrap();

    for command in commands {
        match command {
            ExpectSend(sent) => {
                t.poll_once_assert_pending().await;
                assert_eq!(t.take_sent(), sent)
            }
            Ack(seq_nr) => {
                t.env.increment_now(Duration::from_secs(1));
                t.send_msg(
                    UtpHeader {
                        htype: State,
                        seq_nr: 1.into(),
                        ack_nr: seq_nr.into(),
                        wnd_size: RWND,
                        ..Default::default()
                    },
                    "",
                );
            }
            WaitForRto => {
                t.poll_once_assert_pending().await;
                t.assert_sent_empty();
                t.env.increment_now(t.vsock.rtte.retransmission_timeout());
            }
        }
    }
}

#[tokio::test]
async fn probe_retry_if_emsgsize() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );

    const FAKE_MTU: usize = 1000;

    // Anything above this will trigger emsgsize, and we should retry immediately.
    t.transport
        .set_max_payload_len((FAKE_MTU as u16 - IPV4_HEADER - UDP_HEADER) as usize);
    t.vsock
        .segment_sizes
        .set_probe_expiry_cooldown_max_packets(2);

    // Force congestion controller to have a very high window so that it doesn't interfere.
    t.vsock.congestion_controller.set_window(1024 * 1024);

    let (_r, mut w) = t.stream.take().unwrap().split();
    w.write_all(make_payload(30000).as_bytes()).await.unwrap();

    #[derive(Debug)]
    enum TestCommand {
        ExpectSend(Vec<CmpUtpHeader>),
        Ack(u16),
    }

    use TestCommand::*;

    let commands = [
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 101, payload = p(576)),
            cmphead!(Data, seq_nr = 102, payload = p(1039)), // (1500 + 576) / 2 + 1: first probe, unsuccessful. Will retry immediately.
            cmphead!(Data, seq_nr = 102, payload = p(808)),  // 2nd probe, successful
        ]),
        Ack(102),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 103, payload = p(808)), // cooldown
            cmphead!(Data, seq_nr = 104, payload = p(808)), // cooldown
            cmphead!(Data, seq_nr = 105, payload = p(924)), // 3rd probe, successful
        ]),
        Ack(105),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 106, payload = p(924)), // cooldown
            cmphead!(Data, seq_nr = 107, payload = p(924)), // cooldown
            cmphead!(Data, seq_nr = 108, payload = p(982)), // 4th probe, successful
        ]),
        Ack(108),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 109, payload = p(982)), // cooldown
            cmphead!(Data, seq_nr = 110, payload = p(982)), // cooldown
            cmphead!(Data, seq_nr = 111, payload = p(1011)), // 5th probe, unsuccessful
            cmphead!(Data, seq_nr = 111, payload = p(997)), // 6th probe, successful
        ]),
        Ack(111),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 112, payload = p(997)), // cooldown
            cmphead!(Data, seq_nr = 113, payload = p(997)), // cooldown
            cmphead!(Data, seq_nr = 114, payload = p(1004)), // unsuccessful
            cmphead!(Data, seq_nr = 114, payload = p(1001)), // unsuccessful
            cmphead!(Data, seq_nr = 114, payload = p(999)), // successful
        ]),
        Ack(114),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 115, payload = p(999)), // cooldown
            cmphead!(Data, seq_nr = 116, payload = p(999)), // cooldown
            cmphead!(Data, seq_nr = 117, payload = p(1000)), // successful
        ]),
        Ack(117),
        ExpectSend(vec![
            cmphead!(Data, seq_nr = 118, payload = p(1000)),
            cmphead!(Data, seq_nr = 119, payload = p(1000)),
            cmphead!(Data, seq_nr = 120, payload = p(1000)),
            cmphead!(Data, seq_nr = 121, payload = p(1000)),
            cmphead!(Data, seq_nr = 122, payload = p(999)), // limited by RWND (see below)
        ]),
    ];

    for command in commands {
        match command {
            ExpectSend(sent) => {
                t.poll_once_assert_pending().await;
                assert_eq!(t.take_sent(), sent)
            }
            Ack(seq_nr) => {
                t.env.increment_now(Duration::from_secs(1));
                t.send_msg(
                    UtpHeader {
                        htype: State,
                        seq_nr: 1.into(),
                        wnd_size: (calc_payload_size(1000) * 5) as u32 - 1,
                        ack_nr: seq_nr.into(),
                        ..Default::default()
                    },
                    "",
                );
            }
        }
    }
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
