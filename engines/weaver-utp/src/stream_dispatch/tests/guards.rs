// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Packets a stream ignores: acks of what it never sent, payload after the
// remote's FIN; and what its ACKs carry.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    Error, SocketOpts,
    raw::{Type::*, UtpHeader},
    stream_dispatch::{
        StreamArgs,
        tests::{make_test_vsock, make_test_vsock_args},
    },
    test_util::{env::MockUtpEnvironment, setup_test_logging},
    traits::UtpEnvironment,
};

fn patient() -> SocketOpts {
    SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

// Our seq_nr 101 "hello" sent, unacked.
async fn hello_in_flight() -> super::TestVsock {
    let mut t = make_test_vsock(patient(), false);
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.seq_nr, 101.into());
    t
}

#[tokio::test]
async fn an_ack_of_a_packet_never_sent_is_ignored() {
    setup_test_logging();
    let mut t = hello_in_flight().await;
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 105.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    t.poll_once_assert_pending().await;
    let resent: Vec<_> = t
        .take_sent()
        .into_iter()
        .filter(|m| m.header.htype == ST_DATA)
        .collect();
    assert_eq!(resent.len(), 1, "still in flight, so resent");
    assert_eq!(resent[0].header.seq_nr, 101.into());
}

#[tokio::test]
async fn a_reset_acking_a_packet_never_sent_is_ignored() {
    setup_test_logging();
    let mut t = hello_in_flight().await;
    let reset = |ack_nr: u16| UtpHeader {
        htype: ST_RESET,
        seq_nr: 1.into(),
        ack_nr: ack_nr.into(),
        ..Default::default()
    };
    t.send_msg(reset(105), "");
    t.poll_once_assert_pending().await;

    t.send_msg(reset(101), "");
    assert!(matches!(
        t.poll_once_assert_ready().await,
        Err(Error::StResetReceived)
    ));
}

#[tokio::test]
async fn payload_after_the_remotes_fin_is_dropped() {
    setup_test_logging();
    let mut t = make_test_vsock(patient(), false);
    t.send_data(1, 100, "a");
    t.send_msg(
        UtpHeader {
            htype: ST_FIN,
            seq_nr: 2.into(),
            ack_nr: 100.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
    t.send_data(3, 100, "b");
    t.poll_once_assert_pending().await;
    assert_eq!(t.read_all_available().await.unwrap(), b"a");
}

#[tokio::test]
async fn data_never_carries_a_sack_and_a_resend_none_left_over() {
    setup_test_logging();
    let mut t = make_test_vsock(patient(), false);
    // Remote's 3 before its 1 and 2: a hole our ACKs report.
    t.send_data(3, 100, "x");
    t.poll_once_assert_pending().await;
    let acks = t.take_sent();
    assert!(
        acks.iter()
            .any(|m| m.header.htype == ST_STATE && m.header.extensions.selective_ack.is_some())
    );

    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    t.env.increment_now(t.vsock.rtte.retransmission_timeout());
    t.poll_once_assert_pending().await;
    let data: Vec<_> = t
        .take_sent()
        .into_iter()
        .filter(|m| m.header.htype == ST_DATA)
        .collect();
    assert_eq!(data.len(), 2, "sent, then resent");
    assert!(
        data.iter()
            .all(|m| m.header.extensions.selective_ack.is_none())
    );
}

async fn with_a_packet_acknowledged(seq_nr: u16) -> super::TestVsock {
    let env = MockUtpEnvironment::new();
    let header = UtpHeader {
        htype: ST_STATE,
        seq_nr: 1.into(),
        ack_nr: crate::seq_nr::SeqNr::from(seq_nr) - 1,
        wnd_size: 1024 * 1024,
        ..Default::default()
    };
    let args = StreamArgs::new_outgoing(&header, env.now(), env.now());
    let mut t = make_test_vsock_args(patient(), args, env);
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.seq_nr, seq_nr.into());
    t.send_msg(
        UtpHeader {
            ack_nr: seq_nr.into(),
            ..header
        },
        "",
    );
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 0);
    assert_eq!(t.vsock.user_tx_segments.acked_seq_nr(), seq_nr.into());
    t.assert_sent_empty();
    t
}

#[tokio::test]
async fn an_ack_four_below_the_acknowledged_sequence_is_ignored() {
    for seq_nr in [101, 1, 0] {
        let mut t = with_a_packet_acknowledged(seq_nr).await;
        let previous_window = t.vsock.last_remote_window;
        t.send_msg(
            UtpHeader {
                htype: ST_STATE,
                seq_nr: 1.into(),
                ack_nr: t.vsock.user_tx_segments.acked_seq_nr() - 4,
                wnd_size: 17,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.last_remote_window, previous_window);
    }
}

#[tokio::test]
async fn an_ack_three_below_the_acknowledged_sequence_is_kept() {
    for seq_nr in [101, 1, 0] {
        let mut t = with_a_packet_acknowledged(seq_nr).await;
        t.send_msg(
            UtpHeader {
                htype: ST_STATE,
                seq_nr: 1.into(),
                ack_nr: t.vsock.user_tx_segments.acked_seq_nr() - 3,
                wnd_size: 17,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.last_remote_window, 17);
    }
}
