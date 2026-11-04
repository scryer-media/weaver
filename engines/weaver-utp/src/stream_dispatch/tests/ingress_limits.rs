// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use futures::FutureExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{calc_mtu_for_mss, make_test_vsock};
use crate::{
    SocketOpts,
    raw::{Type::State, UtpHeader},
};

#[tokio::test]
async fn the_read_queue_and_reorder_queue_share_one_receive_byte_limit() {
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(4)),
            vsock_rx_bufsize_bytes: std::num::NonZeroUsize::new(8),
            ..Default::default()
        },
        false,
    );
    t.send_data(1, 100, "aaaa");
    t.poll_once_assert_pending().await;
    t.take_sent();
    t.send_data(3, 100, "cccc");
    t.poll_once_assert_pending().await;
    t.take_sent();
    assert_eq!(t.vsock.user_rx.len_test(), 4);
    assert_eq!(t.vsock.user_rx.assembler_packets(), 1);

    t.send_data(4, 100, "dddd");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.user_rx.len_test(), 4);
    assert_eq!(t.vsock.user_rx.assembler_packets(), 1);
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 1.into());
    assert!(
        t.take_sent()
            .iter()
            .all(|packet| packet.header.ack_nr == 1.into())
    );

    assert_eq!(t.read_all_available().await.unwrap(), b"aaaa");
    t.send_data(2, 100, "bbbb");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 3.into());
    assert_eq!(t.read_all_available().await.unwrap(), b"bbbbcccc");
    t.take_sent();

    t.send_data(4, 100, "dddd");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 4.into());
    assert_eq!(t.read_all_available().await.unwrap(), b"dddd");
}

#[tokio::test]
async fn zero_payload_control_packets_have_a_finite_inbox_limit() {
    let mut t = make_test_vsock(Default::default(), false);
    for _ in 0..100 {
        t.send_msg(
            UtpHeader {
                htype: State,
                seq_nr: 1.into(),
                ack_nr: 100.into(),
                wnd_size: 1024 * 1024,
                ..Default::default()
            },
            "",
        );
    }
    assert_eq!(t.vsock.rx.len(), 32);
}

#[tokio::test]
async fn refused_data_payload_still_acknowledges_outgoing_bytes() {
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(4)),
            vsock_rx_bufsize_bytes: std::num::NonZeroUsize::new(8),
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    let mut stream = t.stream.take().unwrap();
    stream.write_all(b"sent").await.unwrap();
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.seq_nr, 101.into());
    assert!(stream.flush().now_or_never().is_none());

    t.send_data(1, 100, "aaaa");
    t.send_data(2, 100, "bbbb");
    t.send_data(3, 101, "cccc");
    assert_eq!(t.vsock.rx.len(), 3);
    t.poll_once_assert_pending().await;
    stream
        .flush()
        .now_or_never()
        .expect("the retained DATA header carries its ACK")
        .unwrap();
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 2.into());
    assert_eq!(t.vsock.user_rx.assembler_packets(), 0);
    let acknowledgements = t.take_sent();
    assert!(!acknowledgements.is_empty());
    for packet in acknowledgements {
        assert_eq!(packet.header.ack_nr, 2.into());
        assert!(packet.header.extensions.selective_ack.is_none());
    }
    assert_eq!(stream.read_all_available().await.unwrap(), b"aaaabbbb");

    t.send_data(3, 101, "cccc");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 3.into());
    assert_eq!(stream.read_all_available().await.unwrap(), b"cccc");
    assert_eq!(t._socket.stats().snapshot().invalid_packets, 0);
}

#[tokio::test]
async fn one_poll_stops_draining_ingress_even_when_sending_requests_a_restart() {
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    let ceiling = t.vsock.segment_sizes.max_ss();
    t.vsock.congestion_controller.set_window(64 * 1024);
    t.transport
        .set_max_payload_len(usize::from(crate::constants::UTP_HEADER) + floor);
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; floor + probe])
        .await
        .unwrap();
    for sequence in 1..=100 {
        t.send_data(sequence, 100, "a");
    }
    t.poll_once_assert_pending().await;
    assert!(
        t.vsock.segment_sizes.max_ss() < ceiling,
        "the probe requested a restart"
    );
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 64.into());
    assert_eq!(t.vsock.rx.len(), 36);
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 100.into());
    assert!(t.vsock.rx.is_empty());
}

#[tokio::test]
async fn a_partly_read_packet_is_the_only_extra_payload_outside_receive_capacity() {
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(4)),
            vsock_rx_bufsize_bytes: std::num::NonZeroUsize::new(8),
            ..Default::default()
        },
        false,
    );
    let mut stream = t.stream.take().unwrap();
    t.send_data(1, 100, "aaaa");
    t.poll_once_assert_pending().await;
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).await.unwrap();
    assert_eq!(&first, b"a");

    for (sequence, payload) in [(2, "bbbb"), (3, "cccc"), (4, "dddd")] {
        t.send_data(sequence, 100, payload);
        t.poll_once_assert_pending().await;
    }
    assert_eq!(t.vsock.user_rx.len_test(), 8);
    assert_eq!(t.vsock.user_rx.assembler_packets(), 0);
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 3.into());
    let mut rest = [0u8; 11];
    stream.read_exact(&mut rest).await.unwrap();
    assert_eq!(&rest, b"aaabbbbcccc");
    t.send_data(4, 100, "dddd");
    t.poll_once_assert_pending().await;
    assert_eq!(stream.read_all_available().await.unwrap(), b"dddd");
}

#[tokio::test(start_paused = true)]
async fn an_exact_ingress_batch_repolls_to_register_for_the_next_packet() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::{Context, Wake, Waker};

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let mut t = make_test_vsock(Default::default(), false);
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    for sequence in 1..=64 {
        t.send_data(sequence, 100, "a");
    }
    assert!(t.vsock.poll_unpin(&mut cx).is_pending());
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 64.into());
    assert!(t.vsock.rx.is_empty());
    assert!(
        wakes.0.swap(0, Ordering::Relaxed) > 0,
        "the exhausted batch schedules an empty repoll"
    );

    assert!(t.vsock.poll_unpin(&mut cx).is_pending());
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    t.send_data(65, 100, "b");
    assert!(
        wakes.0.load(Ordering::Relaxed) > 0,
        "the empty repoll registers for subsequent ingress"
    );
    assert!(t.vsock.poll_unpin(&mut cx).is_pending());
    assert_eq!(t.vsock.last_consumed_remote_seq_nr, 65.into());
}
