// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.

use std::{task::Poll, time::Duration};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader},
    stream_dispatch::{VirtualSocketState, tests::make_test_vsock},
    test_util::setup_test_logging,
};

#[tokio::test]
async fn test_inactivity_timeout_0() {
    setup_test_logging();

    // Set up socket with a short inactivity timeout
    let opts = SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    let mut t = make_test_vsock(opts, false);

    // Send initial data to start the inactivity timer
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    // Initial data should be sent
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )],
    );

    // Wait just under timeout - connection should still be alive, but we
    // should get a retransmission.
    t.env.increment_now(Duration::from_millis(4900));
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )],
    );

    // Remote sends ACK - should reset inactivity timer
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
    t.assert_sent_empty();

    // Wait just under timeout again - connection should still be alive
    t.env.increment_now(Duration::from_millis(4900));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not send anything before timeout");

    // Wait past timeout - connection should not error out as TX is empty.
    t.env.increment_now(Duration::from_millis(200));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not send anything before timeout");

    // Write more data
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"world")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 102,
            ack_nr = 0,
            payload = "world"
        )],
    );

    // Wait past timeout - connection should error out as nothing received.
    t.env.increment_now(Duration::from_secs(5));
    let result = t.poll_once().await;
    // Should get inactivity timeout error
    match result {
        Poll::Ready(Err(e)) => {
            assert!(
                e.to_string().contains("inactive"),
                "Error should mention inactivity: {e}"
            );
        }
        other => panic!("Expected inactivity error, got: {other:?}"),
    }

    // Try to read - should get error
    let mut buf = [0u8; 1024];
    let read_result = t.stream.as_mut().unwrap().read(&mut buf).await;
    assert!(
        read_result.is_err(),
        "Read should fail after inactivity timeout"
    );
}

#[tokio::test]
async fn test_inactivity_timeout_initial_synack() {
    // Set up socket with a short inactivity timeout
    let opts = SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let mut t = make_test_vsock(opts, true);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 0)],
        "Should send syn-ack"
    );

    // Wait past timeout.
    t.env.increment_now(Duration::from_secs(1));
    let result = t.poll_once().await;
    // Should get inactivity timeout error
    match result {
        Poll::Ready(Err(e)) => {
            assert!(
                e.to_string().contains("inactive"),
                "Error should mention inactivity: {e}"
            );
        }
        other => panic!("Expected inactivity error, got: {other:?}"),
    }

    // Try to read - should get error
    let mut buf = [0u8; 1024];
    let read_result = t.stream.as_mut().unwrap().read(&mut buf).await;
    assert!(
        read_result.is_err(),
        "Read should fail after inactivity timeout"
    );
}

#[tokio::test]
async fn test_inactivity_timeout_our_fin_acked() {
    setup_test_logging();
    let opts = SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let mut t = make_test_vsock(opts, false);

    // At first nothing should happen past timeout
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("nothing should happen");

    drop(t.stream.take().unwrap());

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)],
    );
    assert_eq!(
        t.vsock.state,
        VirtualSocketState::FinWait1 {
            our_fin: 101.into()
        }
    );

    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            ack_nr: 101.into(),
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    assert_eq!(t.vsock.state, VirtualSocketState::FinWait2);

    // Nothing should happen as our FIN was acked.
    t.env.increment_now(Duration::from_secs(1));
    let err = t.poll_once_assert_ready().await.unwrap_err();
    assert!(
        err.to_string().contains("inactive"),
        "Error should mention inactivity: {err}"
    );
    t.assert_sent_empty();
}

#[tokio::test]
async fn test_inactivity_timeout_our_fin_unacked() {
    let opts = SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let mut t = make_test_vsock(opts, false);

    // At first nothing should happen past timeout
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("nothing should happen");

    drop(t.stream.take().unwrap());

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)],
    );
    assert_eq!(
        t.vsock.state,
        VirtualSocketState::FinWait1 {
            our_fin: 101.into()
        }
    );

    // Connection should die as our FIN was not ACKed for too long.
    t.env.increment_now(Duration::from_secs(1));
    let result = t.poll_once().await;
    // Should get inactivity timeout error
    match result {
        Poll::Ready(Err(e)) => {
            assert!(
                e.to_string().contains("inactive"),
                "Error should mention inactivity: {e}"
            );
        }
        other => panic!("Expected inactivity error, got: {other:?}"),
    }
}
