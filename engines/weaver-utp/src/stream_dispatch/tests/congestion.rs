// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.

use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tracing::trace;

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::make_test_vsock,
    test_util::setup_test_logging,
};

// Test that congestion control works without testing specific algorithms.
// E.g. increases window on ACKs, reduces on loss etc.
#[tokio::test]
async fn test_congestion_control_basics() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            remote_inactivity_timeout: Some(Duration::from_secs(20)),
            vsock_tx_bufsize_bytes_initial: Some(non_zero_const!(64 * 1024)),
            ..Default::default()
        },
        false,
    );

    let remote_wnd = 64 * 1024;

    // Allow sending by setting large window
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 99.into(),
            wnd_size: remote_wnd, // Large window to not interfere with congestion control
            ..Default::default()
        },
        "",
    );
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;

    let initial_window = t.vsock.congestion_controller.window();
    assert!(
        initial_window < remote_wnd as usize,
        "{initial_window} >= {remote_wnd}, should be less"
    );
    trace!(initial_window, remote_wnd);

    // Write a lot of data to test windowing
    let big_data = vec![0u8; 64 * 1024];
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&big_data)
        .await
        .unwrap();

    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;

    assert_eq!(
        initial_window,
        t.vsock.congestion_controller.window(),
        "window shouldn't have changed"
    );

    // Should only send up to initial window
    let sent = t.take_sent();
    let sent_bytes: usize = sent.iter().map(|m| m.payload().len()).sum::<usize>();
    assert!(
        sent_bytes <= initial_window,
        "Should respect initial window. sent_bytes={sent_bytes}, initial_window={initial_window}"
    );
    let first_batch_seq_nrs = sent.iter().map(|m| m.header.seq_nr).collect::<Vec<_>>();

    // ACK all packets - window should increase
    for seq_nr in &first_batch_seq_nrs {
        t.send_msg(
            UtpHeader {
                htype: ST_STATE,
                seq_nr: 1.into(),
                ack_nr: *seq_nr,
                wnd_size: remote_wnd,
                ..Default::default()
            },
            "",
        );
    }
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;

    let intermediate_window = t.vsock.congestion_controller.window();
    trace!(intermediate_window);

    assert!(
        intermediate_window > initial_window,
        "Window should grow after ACKs"
    );

    let sent = t.take_sent();
    assert!(
        !sent.is_empty(),
        "Should send more data due to increased window"
    );

    // Now simulate packet loss via duplicate ACKs
    let lost_seq_nr = *first_batch_seq_nrs.last().unwrap();
    for _ in 0..4 {
        t.send_msg(
            UtpHeader {
                htype: ST_STATE,
                seq_nr: 0.into(),
                ack_nr: lost_seq_nr,
                wnd_size: remote_wnd,
                ..Default::default()
            },
            "",
        );
    }
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;

    // Should trigger fast retransmit
    let resent = t.take_sent();
    assert!(
        !resent.is_empty(),
        "Should retransmit on triple duplicate ACK"
    );

    // Window should be reduced
    let window_after_loss = t.vsock.congestion_controller.window();
    trace!(window_after_loss);
    assert!(
        window_after_loss < intermediate_window,
        "Window should decrease after loss: intermediate_window={intermediate_window} window_after_loss={window_after_loss}"
    );

    // Simulate timeout by advancing time
    t.env.increment_now(Duration::from_secs(10));
    t.poll_once_assert_pending().await;

    // Should retransmit and reduce window further
    let resent = t.take_sent();
    assert!(!resent.is_empty(), "Should retransmit on timeout");

    let window_after_timeout = t.vsock.congestion_controller.window();
    assert!(
        window_after_timeout <= window_after_loss,
        "Window should decrease or stay same after timeout"
    );
}
