// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::time::Duration;

use crate::{
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::make_test_vsock,
    test_util::setup_test_logging,
};

#[tokio::test]
async fn test_delayed_ack_sent_once() {
    setup_test_logging();

    let mut t = make_test_vsock(Default::default(), false);
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    t.send_msg(
        UtpHeader {
            htype: ST_DATA,
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
    assert_eq!(sent, vec![cmphead! {ST_STATE, ack_nr=1}]);

    // Assert nothing else is sent later.
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
}
