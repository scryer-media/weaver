// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

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
            // No probe: a lost probe would not cut the window.
            link_mtu: Some(non_zero_const!(576)),
            congestion: super::cubic(),
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
                // A peer that measures our delay: 10 ms, well under target.
                timestamp_microseconds: 1,
                timestamp_difference_microseconds: 10_000,
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

// The default controller is LEDBAT: it starts at one frame, grows while the
// peer measures our delay under target, and backs off once it is over.
#[tokio::test]
async fn ledbat_grows_under_target_and_backs_off_over_it() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            // The floor MTU: no probe holds back the segments of a round.
            link_mtu: Some(non_zero_const!(576)),
            vsock_tx_bufsize_bytes_initial: Some(non_zero_const!(1024 * 1024)),
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    let remote_wnd = 1024 * 1024;
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 99.into(),
            wnd_size: remote_wnd,
            ..Default::default()
        },
        "",
    );
    t.env.increment_now(Duration::from_millis(50));
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.congestion_controller.window(), 1500);

    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![0u8; 512 * 1024])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    // Every round acks all that is in flight, the peer reporting our delay
    // as `gap_us`; its first report is the base.
    let mut last = t.take_sent().last().unwrap().header.seq_nr;
    // Each round trip takes 250 ms, so a delay of 200 ms stands under it.
    let mut round = async |t: &mut super::TestVsock, gap_us: u32| {
        t.env.increment_now(Duration::from_millis(250));
        t.send_msg(
            UtpHeader {
                htype: ST_STATE,
                seq_nr: 1.into(),
                ack_nr: last,
                wnd_size: remote_wnd,
                timestamp_microseconds: 1,
                timestamp_difference_microseconds: gap_us,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;
        if let Some(sent) = t.take_sent().last() {
            last = sent.header.seq_nr;
        }
        t.vsock.congestion_controller.window()
    };

    let mut window = 1500;
    for _ in 0..4 {
        let grown = round(&mut t, 10_000).await;
        assert!(
            grown > window,
            "{grown} should exceed {window} under target"
        );
        window = grown;
    }
    // 200 ms over the base: over target. The controller's delay is the
    // lowest of the last three samples, so two more rounds still see none.
    for _ in 0..2 {
        window = round(&mut t, 210_000).await;
    }
    for _ in 0..2 {
        let shrunk = round(&mut t, 210_000).await;
        assert!(
            shrunk < window,
            "{shrunk} should be under {window} over target"
        );
        window = shrunk;
    }
}
