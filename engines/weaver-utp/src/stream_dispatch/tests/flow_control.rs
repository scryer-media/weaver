// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

// Tests for respecting local RX window and remote RX window.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::{calc_mtu_for_mss, make_test_vsock},
    test_util::setup_test_logging,
};

#[tokio::test]
async fn test_flow_control() {
    setup_test_logging();

    // Configure socket with small buffers to test flow control
    let opts = SocketOpts {
        link_mtu: Some(calc_mtu_for_mss(5)),
        vsock_rx_bufsize_bytes: Some(non_zero_const!(25)), // Small receive buffer (~5 packets of size 5)
        ..Default::default()
    };

    let mut t = make_test_vsock(opts, true);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 0)]
    ); // syn ack

    // Test assembly queue limit first
    // Send packets out of order to fill assembly queue
    t.send_data(3, 100, "third"); // Out of order
    t.send_data(4, 100, "fourth"); // Out of order
    t.send_data(17, 100, "past"); // Dropped: past the 16-packet reorder window

    t.poll_once_assert_pending().await;

    // Only two packets should be in assembly queue
    assert_eq!(t.vsock.user_rx.assembler_packets(), 2);

    // Send in-order packet to trigger processing
    t.send_data(1, t.vsock.seq_nr - 1, "first");
    t.poll_once_assert_pending().await;

    // Read available data
    let read = t.read_all_available().await.unwrap();
    assert_eq!(&read, b"first"); // Only first packet should be read

    // Now test user rx channel limit
    // Send several packets that exceed the rx buffer size
    for i in 6..15 {
        t.send_data(i, t.vsock.seq_nr - 1, "data!");
        t.poll_once_assert_pending().await;
    }

    // Read available data - should only get packets that fit in rx buffer
    let read = t.read_all_available().await.unwrap();
    assert!(read.len() < 50); // Should be limited by rx_bufsize_approx

    // Verify window size advertised matches available buffer space
    let sent = t.take_sent();
    assert!(!sent.is_empty());
    let window = sent.last().unwrap().header.wnd_size;
    assert!(window < 1024); // Window should be reduced

    // Send more data - should be dropped due to full rx buffer
    t.send_data(15, t.vsock.seq_nr - 1, "dropped");
    t.poll_once_assert_pending().await;

    // Verify data was dropped
    let read = String::from_utf8(t.read_all_available().await.unwrap()).unwrap();
    assert!(!read.contains("dropped"));
}

#[tokio::test]
async fn test_sender_flow_control() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    // Set initial remote window very small
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.seq_nr - 1,
            wnd_size: 5, // Only allow 5 bytes
            ..Default::default()
        },
        "",
    );

    // Try to write more data than window allows
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"helloworld")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    // Should only send up to window size
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )]
    );

    // No more data should be sent until window updates
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // ACK first packet but keep window small
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 4, // Reduce window further
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // We pre-segmented data already with old window, so the segment just doesn't fit.
    t.assert_sent_empty();

    // Remote increases window and ACKs
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 10, // Open window
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Should send remaining data
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 102,
            ack_nr = 0,
            payload = "world"
        )]
    );
}

#[tokio::test]
async fn test_zero_window_handling() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    // Set initial window to allow some data
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.seq_nr - 1,
            wnd_size: 5,
            ..Default::default()
        },
        "",
    );

    // Write data
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello world")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    // First chunk should be sent
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )]
    );

    // ACK data but advertise zero window
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 0, // Zero window
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Nothing should be sent
    t.assert_sent_empty();

    // Remote sends window update
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024, // Open window
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Remaining data should now be sent
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 102,
            ack_nr = 0,
            payload = " world"
        )]
    );
}

#[tokio::test]
async fn test_window_update_sent_when_window_less_than_mss() {
    setup_test_logging();

    // Configure socket with small but non-zero receive buffer
    const MSS: usize = 5;
    let opts = SocketOpts {
        vsock_rx_bufsize_bytes: Some(non_zero_const!(MSS * 2)),
        link_mtu: Some(calc_mtu_for_mss(MSS)),
        ..Default::default()
    };

    let mut t = make_test_vsock(opts, false);

    // Fill buffer to just under MSS to get a small window
    t.send_data(1, t.vsock.seq_nr - 1, "aaaaa");
    t.poll_once_assert_pending().await;
    // We shouldn't have registered the flush waker yet.
    assert!(!t.vsock.user_rx.is_flush_waker_registered());
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, ack_nr = 1, wnd_size = 5u32)]
    );

    // Now the window should be less than MSS.
    t.send_data(2, t.vsock.seq_nr - 1, "b");
    t.poll_once_assert_pending().await;
    assert!(t.vsock.user_rx.is_flush_waker_registered());

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_STATE,
            seq_nr = 101,
            ack_nr = 2,
            wnd_size = 0u32
        )],
        "Should have sent an ACK with new window less than MSS"
    );

    // Read some data to free up more than MSS worth of buffer space
    let mut buf = vec![0u8; MSS];
    t.stream
        .as_mut()
        .unwrap()
        .read_exact(&mut buf)
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    // Should send window update ACK
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_STATE,
            seq_nr = 101,
            ack_nr = 2,
            wnd_size = 5u32
        )],
        "Should have sent an ACK with updated window"
    );
}

#[tokio::test]
async fn test_sends_up_to_remote_window_only_single_msg() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), true);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, ack_nr = 0)],
        "intial SYN-ACK should be sent"
    );
    assert_eq!(t.vsock.last_remote_window, 0);

    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 100.into(),
            wnd_size: 4,
            ..Default::default()
        },
        "hello",
    );
    t.poll_once_assert_pending().await;

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hell"
        )]
    );

    // Until window updates and/or we receive an ACK, we don't send anything
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 0);
}

#[tokio::test]
async fn test_sends_up_to_remote_window_only_multi_msg() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(2)),
            ..Default::default()
        },
        true,
    );
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1); // syn ack
    assert_eq!(t.vsock.last_remote_window, 0);

    // Hack: force congestion controller to have a larger window than 2 * MSS (4).
    t.vsock.congestion_controller.set_remote_window(5);
    t.vsock.congestion_controller.set_window(10000);
    assert_eq!(t.vsock.congestion_controller.window(), 5);

    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello world")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            // This is enough to send "hello" in 3 messages
            wnd_size: 5,
            ..Default::default()
        },
        "hello",
    );
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, payload = "he"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "ll"),
            cmphead!(ST_DATA, seq_nr = 103, payload = "o")
        ]
    );

    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
}
