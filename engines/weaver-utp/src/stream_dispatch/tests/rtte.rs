// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::time::Duration;

use crate::raw::{Type::*, UtpHeader};
use tokio::io::AsyncWriteExt;

use crate::{
    SocketOpts,
    stream_dispatch::tests::{calc_mtu_for_mss, make_test_vsock},
    test_util::setup_test_logging,
};

#[tokio::test]
async fn test_rtte_set_and_updated() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(1)),
            congestion: super::cubic(),
            ..Default::default()
        },
        false,
    );

    // This gets set by test framework.
    assert_eq!(t.vsock.rtte.roundtrip_time(), Duration::from_secs(1));

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    let (_r, mut w) = t.stream.take().unwrap().split();
    w.write_all(b"abcde").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, payload = "a"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "b")
        ]
    );

    // ACK first packet.
    t.env.increment_now(Duration::from_secs(1));
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            // No need to send anything so set rwnd to 0.
            wnd_size: 0,
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // No RTT variation, keep the same.
    assert_eq!(t.vsock.rtte.roundtrip_time(), Duration::from_secs(1));

    // ACK second packet, but increase RTT.
    t.env.increment_now(Duration::from_secs(2));
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 102.into(),
            // No need to send anything so set rwnd to 0.
            wnd_size: 0,
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // RTT should increase, but be lower than the new value.
    assert!(
        t.vsock.rtte.roundtrip_time() > Duration::from_secs(1)
            && t.vsock.rtte.roundtrip_time() < Duration::from_secs(2)
    );
}
