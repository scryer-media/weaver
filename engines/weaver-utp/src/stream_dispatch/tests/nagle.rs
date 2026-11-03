// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use tokio::io::AsyncWriteExt;
use tracing::trace;

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::{calc_mtu_for_mss, make_test_vsock},
    test_util::setup_test_logging,
};

#[tokio::test]
async fn test_nagle_algorithm() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            // Set a large max payload size to ensure we're testing Nagle, not segmentation
            link_mtu: Some(calc_mtu_for_mss(1024)),
            disable_nagle: false, // it's default anyway, but setting explicitly
            ..Default::default()
        },
        false,
    );

    // Enable Nagle (should be on by default, but let's be explicit)
    t.vsock.socket_opts.nagle = true;

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.seq_nr - 1,
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Write a small amount of data
    t.stream.as_mut().unwrap().write_all(b"a").await.unwrap();
    t.poll_once_assert_pending().await;

    // First small write should be sent immediately
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, ack_nr = 0, payload = "a")]
    );

    // Write another small chunk - should not be sent while first is unacked
    t.stream.as_mut().unwrap().write_all(b"b").await.unwrap();
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Nagle should prevent sending small chunk while data is in flight");

    // Write more data - still should not send
    t.stream.as_mut().unwrap().write_all(b"c").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 0);

    // As debugging, ensure we did not even segment the new packets yet.
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(t.vsock.user_tx_segments.total_len_bytes(), 1);
    assert_eq!(t.vsock.user_tx_segments.first_seq_nr().unwrap(), 101.into());

    trace!("Acknowledge first packet");
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(t.vsock.user_tx_segments.total_len_bytes(), 2);

    // After ACK, buffered data should be sent as one packet
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 102, ack_nr = 0, payload = "bc")],
        "Buffered data should be coalesced"
    );

    // Now disable Nagle
    t.vsock.socket_opts.nagle = false;

    // Small writes should be sent immediately
    t.stream.as_mut().unwrap().write_all(b"d").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 103, ack_nr = 0, payload = "d")],
    );

    // Next small write should also go immediately, even without ACK
    t.stream.as_mut().unwrap().write_all(b"e").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 104, ack_nr = 0, payload = "e")],
    );
}
