// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    SocketOpts,
    constants::{IPV4_HEADER, UDP_HEADER, UTP_HEADER},
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::make_test_vsock,
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
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload().len(), floor);
        t.send_msg(
            UtpHeader {
                htype: ST_STATE,
                seq_nr: 1.into(),
                ack_nr: sent[0].header.seq_nr,
                wnd_size: 1024 * 1024,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload().len(), probe);
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
            cmphead!(ST_DATA, seq_nr = 101, payload = p(576)),
            cmphead!(ST_DATA, seq_nr = 102, payload = p(1039)), // (1500 + 576) / 2 + 1: first probe, successful
        ]),
        Ack(102),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 103, payload = p(1039)),
            cmphead!(ST_DATA, seq_nr = 104, payload = p(1039)),
            cmphead!(ST_DATA, seq_nr = 105, payload = p(1039)),
            cmphead!(ST_DATA, seq_nr = 106, payload = p(1270)), // (1500 + 1039) / 2 + 1: second probe, successful
        ]),
        Ack(106),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 107, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 108, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 109, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 110, payload = p(1386)), // (1500 + 1270) / 2 + 1: third probe, unsuccessful
        ]),
        Ack(109),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 110, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 111, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 112, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 113, payload = p(1328)), // 4th probe, unsuccessful
        ]),
        Ack(112),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 113, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 114, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 115, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 116, payload = p(1299)), // 5th probe, unsuccessful
        ]),
        Ack(115),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 116, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 117, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 118, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 119, payload = p(1285)), // 6th probe, unsuccessful
        ]),
        Ack(118),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 119, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 120, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 121, payload = p(1270)),
            cmphead!(ST_DATA, seq_nr = 122, payload = p(1278)), // 7th probe, successful
        ]),
        Ack(122),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 123, payload = p(1278)),
            cmphead!(ST_DATA, seq_nr = 124, payload = p(1278)),
            cmphead!(ST_DATA, seq_nr = 125, payload = p(1278)),
            cmphead!(ST_DATA, seq_nr = 126, payload = p(1282)), // 8th probe, unsuccessful
        ]),
        Ack(125),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 126, payload = p(1278)),
            cmphead!(ST_DATA, seq_nr = 127, payload = p(1278)),
            cmphead!(ST_DATA, seq_nr = 128, payload = p(1278)),
            cmphead!(ST_DATA, seq_nr = 129, payload = p(1280)), // 9th probe, successful
        ]),
        Ack(129),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 130, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 131, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 132, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 133, payload = p(1281)), // 10th probe, unsuccessful
        ]),
        Ack(132),
        WaitForRto,
        // At this point final MTU was found, and we don't need to be limited by probes, but rather by other factors.
        // In this test's case we'll get limited by remote window.
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 133, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 134, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 135, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 136, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 137, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 138, payload = p(1279)), // limited by rwnd, no nagle
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
                        htype: ST_STATE,
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
            cmphead!(ST_DATA, seq_nr = 101, payload = p(576)),
            cmphead!(ST_DATA, seq_nr = 102, payload = p(1039)), // (1500 + 576) / 2 + 1: first probe, unsuccessful. Will retry immediately.
            cmphead!(ST_DATA, seq_nr = 102, payload = p(808)),  // 2nd probe, successful
        ]),
        Ack(102),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 103, payload = p(808)), // cooldown
            cmphead!(ST_DATA, seq_nr = 104, payload = p(808)), // cooldown
            cmphead!(ST_DATA, seq_nr = 105, payload = p(924)), // 3rd probe, successful
        ]),
        Ack(105),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 106, payload = p(924)), // cooldown
            cmphead!(ST_DATA, seq_nr = 107, payload = p(924)), // cooldown
            cmphead!(ST_DATA, seq_nr = 108, payload = p(982)), // 4th probe, successful
        ]),
        Ack(108),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 109, payload = p(982)), // cooldown
            cmphead!(ST_DATA, seq_nr = 110, payload = p(982)), // cooldown
            cmphead!(ST_DATA, seq_nr = 111, payload = p(1011)), // 5th probe, unsuccessful
            cmphead!(ST_DATA, seq_nr = 111, payload = p(997)), // 6th probe, successful
        ]),
        Ack(111),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 112, payload = p(997)), // cooldown
            cmphead!(ST_DATA, seq_nr = 113, payload = p(997)), // cooldown
            cmphead!(ST_DATA, seq_nr = 114, payload = p(1004)), // unsuccessful
            cmphead!(ST_DATA, seq_nr = 114, payload = p(1001)), // unsuccessful
            cmphead!(ST_DATA, seq_nr = 114, payload = p(999)), // successful
        ]),
        Ack(114),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 115, payload = p(999)), // cooldown
            cmphead!(ST_DATA, seq_nr = 116, payload = p(999)), // cooldown
            cmphead!(ST_DATA, seq_nr = 117, payload = p(1000)), // successful
        ]),
        Ack(117),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 118, payload = p(1000)),
            cmphead!(ST_DATA, seq_nr = 119, payload = p(1000)),
            cmphead!(ST_DATA, seq_nr = 120, payload = p(1000)),
            cmphead!(ST_DATA, seq_nr = 121, payload = p(1000)),
            cmphead!(ST_DATA, seq_nr = 122, payload = p(999)), // limited by RWND (see below)
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
                        htype: ST_STATE,
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
