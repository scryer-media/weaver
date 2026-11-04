// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use super::make_test_vsock;
use crate::{
    raw::{Type::*, UtpHeader},
    test_util::setup_test_logging,
    traits::UtpEnvironment,
};

// The gap a header reports is the one measured as the peer's last packet
// arrived; time the packet then waits on our side is not the path's.
#[tokio::test]
async fn the_reported_gap_is_taken_on_arrival() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);
    let arrival = t.vsock.timestamp_microseconds();
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            connection_id: t.vsock.conn_id_send + 1,
            timestamp_microseconds: 1_000,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    t.env.increment_now(Duration::from_millis(50));
    let (_r, mut w) = t.stream.take().unwrap().split();
    w.write_all(b"hello").await.unwrap();
    t.poll_once_assert_pending().await;

    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.get_type(), ST_DATA);
    assert_eq!(
        sent[0].header.timestamp_difference_microseconds,
        arrival.wrapping_sub(1_000)
    );
}

#[tokio::test]
async fn processing_delay_does_not_enter_the_reported_gap() {
    let mut t = make_test_vsock(Default::default(), false);
    let arrival = (t.env.now() - t.vsock.socket_created).as_micros() as u32;
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            connection_id: t.vsock.conn_id_send + 1,
            timestamp_microseconds: 1_000,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
    t.env.increment_now(Duration::from_millis(50));
    t.poll_once_assert_pending().await;

    assert_eq!(t.vsock.delay.reply_us(), arrival.wrapping_sub(1_000));
}
