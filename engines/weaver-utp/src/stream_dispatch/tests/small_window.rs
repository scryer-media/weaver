// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::num::NonZeroUsize;

use super::make_test_vsock;
use crate::{SocketOpts, raw::Type};

#[tokio::test(start_paused = true)]
async fn a_sub_mss_receive_window_advertises_its_exact_free_bytes() {
    let test = make_test_vsock(
        SocketOpts {
            vsock_rx_bufsize_bytes: NonZeroUsize::new(128),
            ..Default::default()
        },
        true,
    );
    assert_eq!(test.vsock.outgoing_header().wnd_size, 128);
}

#[tokio::test(start_paused = true)]
async fn reading_a_full_sub_mss_receive_buffer_advertises_its_reopened_window() {
    let mut test = make_test_vsock(
        SocketOpts {
            vsock_rx_bufsize_bytes: NonZeroUsize::new(128),
            ..Default::default()
        },
        true,
    );
    test.poll_once_assert_pending().await;
    let syn_ack = test.take_sent();
    assert_eq!(syn_ack.len(), 1);
    assert_eq!(syn_ack[0].header.wnd_size, 128);
    let payload = "p".repeat(128);
    test.send_data(1, 100, &payload);
    test.poll_once_assert_pending().await;
    let ack = test.take_sent();
    assert_eq!(ack.len(), 1);
    assert_eq!(ack[0].header.htype, Type::State);
    assert_eq!(ack[0].header.wnd_size, 0);
    assert_eq!(test.read_all_available().await.unwrap(), payload.as_bytes());
    test.poll_once_assert_pending().await;
    let update = test.take_sent();
    assert_eq!(update.len(), 1);
    assert_eq!(update[0].header.htype, Type::State);
    assert_eq!(update[0].header.ack_nr.0, 1);
    assert_eq!(update[0].header.wnd_size, 128);
}

#[tokio::test(start_paused = true)]
async fn draining_a_window_left_at_a_non_zero_residue_advertises_the_reopened_window() {
    let mss = 1000;
    let mut test = make_test_vsock(
        SocketOpts {
            vsock_rx_bufsize_bytes: NonZeroUsize::new(2 * mss + 100),
            ..Default::default()
        },
        true,
    );
    test.poll_once_assert_pending().await;
    test.take_sent();
    let payload = "p".repeat(mss);
    test.send_data(1, 100, &payload);
    test.send_data(2, 100, &payload);
    test.poll_once_assert_pending().await;
    let ack = test.take_sent();
    assert_eq!(ack.len(), 1);
    assert_eq!(ack[0].header.htype, Type::State);
    assert_eq!(ack[0].header.ack_nr.0, 2);
    assert_eq!(ack[0].header.wnd_size, 100);
    assert_eq!(test.read_all_available().await.unwrap().len(), 2 * mss);
    test.poll_once_assert_pending().await;
    let update = test.take_sent();
    assert_eq!(update.len(), 1);
    assert_eq!(update[0].header.htype, Type::State);
    assert_eq!(update[0].header.ack_nr.0, 2);
    assert_eq!(update[0].header.wnd_size, 2 * mss as u32 + 100);
}
