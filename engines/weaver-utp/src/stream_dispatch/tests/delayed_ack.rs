// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::{sync::atomic::Ordering, time::Duration};

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::make_test_vsock,
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
