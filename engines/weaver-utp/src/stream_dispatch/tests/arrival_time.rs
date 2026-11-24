// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use super::{StreamArgs, calc_mtu_for_mss, make_test_vsock};
use crate::{
    SocketOpts,
    message::UtpMessage,
    raw::{Extensions, Type, UtpHeader, selective_ack::SelectiveAck},
    stream_dispatch::Timer,
    traits::UtpEnvironment,
};

async fn delayed_ack_samples_receipt_time(selective: bool) {
    let mut test = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(1)),
            ..Default::default()
        },
        false,
    );
    let (_reader, mut writer) = test.stream.take().unwrap().split();
    writer.write_all(b"abc").await.unwrap();
    test.poll_once_assert_pending().await;
    assert_eq!(test.take_sent().len(), 3);
    let mut expected = test.vsock.rtte;
    let path_rtt = Duration::from_millis(10);
    expected.sample(path_rtt);
    test.env.increment_now(path_rtt);
    let mut message = UtpMessage::new_test(
        UtpHeader {
            htype: Type::State,
            seq_nr: 1.into(),
            ack_nr: if selective { 100.into() } else { 101.into() },
            wnd_size: 1024,
            extensions: if selective {
                Extensions {
                    selective_ack: SelectiveAck::new_test([0]),
                    ..Default::default()
                }
            } else {
                Default::default()
            },
            ..Default::default()
        },
        b"",
    );
    message.received = test.env.now();
    test.tx.send(message).unwrap();
    test.env.increment_now(Duration::from_millis(200));
    let processed = test.env.now();
    test.poll_once_assert_pending().await;
    assert_eq!(test.vsock.rtte, expected);
    let Timer::Armed { expires_at, .. } = test.vsock.timers.retransmit else {
        panic!("remaining packets need a retransmit timer");
    };
    assert_eq!(expires_at, processed + expected.retransmission_timeout());
}

#[tokio::test(start_paused = true)]
async fn an_ack_rtt_excludes_processing_delay_but_rearms_from_processing_time() {
    delayed_ack_samples_receipt_time(false).await;
}

#[tokio::test(start_paused = true)]
async fn a_sack_rtt_excludes_processing_delay_but_rearms_from_processing_time() {
    delayed_ack_samples_receipt_time(true).await;
}

#[tokio::test(start_paused = true)]
async fn an_ack_received_before_transmission_does_not_supply_an_rtt_sample() {
    let mut test = make_test_vsock(Default::default(), false);
    let (_reader, mut writer) = test.stream.take().unwrap().split();
    writer.write_all(b"payload").await.unwrap();
    test.poll_once_assert_pending().await;
    assert_eq!(test.take_sent().len(), 1);
    let previous = test.vsock.rtte;
    let mut message = UtpMessage::new_test(
        UtpHeader {
            htype: Type::State,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        b"",
    );
    message.received = test.env.now() - Duration::from_millis(1);
    test.tx.send(message).unwrap();
    test.env.increment_now(Duration::from_millis(10));
    test.poll_once_assert_pending().await;
    assert_eq!(test.vsock.rtte, previous);
    assert!(test.vsock.user_tx_segments.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_handshake_received_before_its_syn_was_sent_has_no_rtt_sample() {
    let sent = tokio::time::Instant::now();
    let args =
        StreamArgs::new_outgoing(&UtpHeader::default(), sent, sent - Duration::from_millis(1));
    assert_eq!(args.rtt, None);
}
