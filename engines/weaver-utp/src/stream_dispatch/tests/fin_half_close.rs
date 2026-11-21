// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use futures::FutureExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{calc_mtu_for_mss, make_test_vsock};
use crate::{
    SocketOpts,
    raw::{Type, UtpHeader},
    stream_dispatch::VirtualSocketState,
};

#[tokio::test(start_paused = true)]
async fn a_state_ack_of_our_fin_leaves_the_next_remote_data_sequence_available() {
    let mut test = make_test_vsock(Default::default(), false);
    let (mut reader, mut writer) = test.stream.take().unwrap().split();
    let mut shutdown = Box::pin(writer.shutdown());
    assert!(shutdown.as_mut().now_or_never().is_none());
    test.poll_once_assert_pending().await;
    let sent = test.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.htype, Type::Fin);
    assert_eq!(sent[0].header.seq_nr.0, 101);
    test.send_msg(
        UtpHeader {
            htype: Type::State,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    assert!(matches!(test.vsock.state, VirtualSocketState::FinWait2));
    assert!(reader.read_u8().now_or_never().is_none());
    test.send_data(1, 101, "tail");
    test.poll_once_assert_pending().await;
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"tail");
    test.send_msg(
        UtpHeader {
            htype: Type::Fin,
            seq_nr: 2.into(),
            ack_nr: 101.into(),
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_ready().await.unwrap();
    assert_eq!(
        reader.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    shutdown.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn remote_eof_preserves_a_window_blocked_tail_until_explicit_local_shutdown() {
    let mut test = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(4)),
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    test.send_msg(
        UtpHeader {
            htype: Type::State,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 4,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    test.take_sent();
    let (mut reader, mut writer) = test.stream.take().unwrap().split();
    writer.write_all(b"abcde").await.unwrap();
    test.poll_once_assert_pending().await;
    let first = test.take_sent();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].header.htype, Type::Data);
    assert_eq!(first[0].header.seq_nr.0, 101);
    assert_eq!(first[0].payload(), b"abcd");
    // Another poll reserves the tail while the first packet fills the window.
    test.poll_once_assert_pending().await;
    assert_eq!(test.vsock.user_tx_segments.total_len_packets(), 2);
    test.assert_sent_empty();
    test.send_msg(
        UtpHeader {
            htype: Type::Fin,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 0,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    let acknowledged_eof = test.take_sent();
    assert!(
        acknowledged_eof
            .iter()
            .all(|message| message.header.htype != Type::Fin),
        "remote EOF cannot replace our unsent DATA with a FIN"
    );
    assert!(
        acknowledged_eof
            .iter()
            .any(|message| message.header.htype == Type::State && message.header.ack_nr.0 == 1)
    );
    assert!(writer.flush().now_or_never().is_none());
    test.send_msg(
        UtpHeader {
            htype: Type::Fin,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 0,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    let duplicate_ack = test.take_sent();
    assert_eq!(duplicate_ack.len(), 1);
    assert_eq!(duplicate_ack[0].header.htype, Type::State);
    assert_eq!(duplicate_ack[0].header.ack_nr.0, 1);
    assert_eq!(test.vsock.seq_nr.0, 102);
    assert_eq!(test.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(
        reader.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );

    test.send_msg(
        UtpHeader {
            htype: Type::State,
            seq_nr: 2.into(),
            ack_nr: 101.into(),
            wnd_size: 1,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    let tail = test.take_sent();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].header.htype, Type::Data);
    assert_eq!(tail[0].header.seq_nr.0, 102);
    assert_eq!(tail[0].payload(), b"e");
    test.send_msg(
        UtpHeader {
            htype: Type::State,
            seq_nr: 2.into(),
            ack_nr: 102.into(),
            wnd_size: 1,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    writer.flush().await.unwrap();
    writer.write_all(b"f").await.unwrap();
    test.poll_once_assert_pending().await;
    let continued = test.take_sent();
    assert_eq!(continued.len(), 1);
    assert_eq!(continued[0].header.htype, Type::Data);
    assert_eq!(continued[0].header.seq_nr.0, 103);
    assert_eq!(continued[0].payload(), b"f");
    test.send_msg(
        UtpHeader {
            htype: Type::State,
            seq_nr: 2.into(),
            ack_nr: 103.into(),
            wnd_size: 1,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    writer.flush().await.unwrap();
    let mut shutdown = Box::pin(writer.shutdown());
    assert!(shutdown.as_mut().now_or_never().is_none());
    test.poll_once_assert_pending().await;
    let fin = test.take_sent();
    assert_eq!(fin.len(), 1);
    assert_eq!(fin[0].header.htype, Type::Fin);
    assert_eq!(fin[0].header.seq_nr.0, 104);
    test.send_msg(
        UtpHeader {
            htype: Type::State,
            seq_nr: 2.into(),
            ack_nr: 104.into(),
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_ready().await.unwrap();
    shutdown.await.unwrap();
}

struct ReaderWake(AtomicUsize);

impl Wake for ReaderWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test(start_paused = true)]
async fn a_fin_without_payload_wakes_a_pending_reader_without_closing_its_writer() {
    let mut test = make_test_vsock(Default::default(), false);
    let (mut reader, mut writer) = test.stream.take().unwrap().split();
    let wakes = Arc::new(ReaderWake(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut byte = [0];
    let mut read = Box::pin(reader.read(&mut byte));
    assert!(read.as_mut().poll(&mut context).is_pending());
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    test.send_msg(
        UtpHeader {
            htype: Type::Fin,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    assert!(
        wakes.0.load(Ordering::Relaxed) > 0,
        "EOF must wake its reader"
    );
    assert!(matches!(
        read.as_mut().poll(&mut context),
        Poll::Ready(Ok(0))
    ));
    writer.write_all(b"x").await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn skipping_the_last_ack_still_waits_for_local_fin_send_acceptance() {
    let mut test = make_test_vsock(
        SocketOpts {
            dont_wait_for_lastack: true,
            ..Default::default()
        },
        false,
    );
    let (_reader, mut writer) = test.stream.take().unwrap().split();
    test.send_msg(
        UtpHeader {
            htype: Type::Fin,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    let remote_eof_ack = test.take_sent();
    assert_eq!(remote_eof_ack.len(), 1);
    assert_eq!(remote_eof_ack[0].header.htype, Type::State);
    test.transport.set_poll_send_pending(true);
    let mut shutdown = Box::pin(writer.shutdown());
    assert!(shutdown.as_mut().now_or_never().is_none());
    test.poll_once_assert_pending().await;
    assert!(matches!(
        test.vsock.state,
        VirtualSocketState::LastAck { .. }
    ));
    test.assert_sent_empty();
    assert!(shutdown.as_mut().now_or_never().is_none());
    test.transport.set_poll_send_pending(false);
    test.poll_once_assert_ready().await.unwrap();
    let fin = test.take_sent();
    assert_eq!(fin.len(), 1);
    assert_eq!(fin[0].header.htype, Type::Fin);
    assert_eq!(fin[0].header.seq_nr.0, 101);
    assert_eq!(fin[0].header.ack_nr.0, 1);
    shutdown.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_fin_confirming_the_handshake_closes_only_the_incoming_direction() {
    let mut test = make_test_vsock(Default::default(), true);
    let (mut reader, mut writer) = test.stream.take().unwrap().split();
    test.poll_once_assert_pending().await;
    let syn_ack = test.take_sent();
    assert_eq!(syn_ack.len(), 1);
    assert_eq!(syn_ack[0].header.htype, Type::State);
    assert_eq!(syn_ack[0].header.seq_nr.0, 101);
    let invalid_before = test._socket.stats().snapshot().invalid_packets;
    for (sequence, acknowledgement) in [(1u16, 99u16), (2, 100)] {
        test.send_msg(
            UtpHeader {
                htype: Type::Fin,
                seq_nr: sequence.into(),
                ack_nr: acknowledgement.into(),
                wnd_size: 2,
                ..Default::default()
            },
            "",
        );
        test.poll_once_assert_pending().await;
        assert!(matches!(
            test.vsock.state,
            VirtualSocketState::SynAckSent { .. }
        ));
        assert!(reader.read_u8().now_or_never().is_none());
        test.assert_sent_empty();
    }
    assert_eq!(
        test._socket.stats().snapshot().invalid_packets,
        invalid_before + 2
    );
    test.send_msg(
        UtpHeader {
            htype: Type::Fin,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 2,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    let ack = test.take_sent();
    assert_eq!(ack.len(), 1);
    assert_eq!(ack[0].header.htype, Type::State);
    assert_eq!(ack[0].header.ack_nr.0, 1);
    assert!(
        matches!(test.vsock.state, VirtualSocketState::CloseWait { remote_fin } if remote_fin.0 == 1)
    );
    assert!(test.vsock.confirmed);
    assert_eq!(
        test.vsock.rtte.retransmission_timeout(),
        test.vsock.socket_opts.min_timeout
    );
    assert!(matches!(
        test.vsock.timers.syn_ack_resend,
        crate::stream_dispatch::Timer::Idle
    ));
    assert_eq!(
        reader.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    writer.write_all(b"ok").await.unwrap();
    test.poll_once_assert_pending().await;
    let reply = test.take_sent();
    assert_eq!(reply.len(), 1);
    assert_eq!(reply[0].header.htype, Type::Data);
    assert_eq!(reply[0].header.seq_nr.0, 101);
    assert_eq!(reply[0].payload(), b"ok");
    test.send_msg(
        UtpHeader {
            htype: Type::State,
            seq_nr: 2.into(),
            ack_nr: 101.into(),
            wnd_size: 2,
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_pending().await;
    writer.flush().await.unwrap();
    let mut shutdown = Box::pin(writer.shutdown());
    assert!(shutdown.as_mut().now_or_never().is_none());
    test.poll_once_assert_pending().await;
    let fin = test.take_sent();
    assert_eq!(fin.len(), 1);
    assert_eq!(fin[0].header.htype, Type::Fin);
    assert_eq!(fin[0].header.seq_nr.0, 102);
    test.send_msg(
        UtpHeader {
            htype: Type::State,
            seq_nr: 2.into(),
            ack_nr: 102.into(),
            ..Default::default()
        },
        "",
    );
    test.poll_once_assert_ready().await.unwrap();
    shutdown.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_fin_with_payload_is_taken_as_a_fin_and_its_payload_ignored() {
    let mut test = make_test_vsock(Default::default(), false);
    let (mut reader, mut writer) = test.stream.take().unwrap().split();
    test.send_msg(
        UtpHeader {
            htype: Type::Fin,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "payload",
    );
    test.poll_once_assert_pending().await;
    assert!(matches!(
        test.vsock.state,
        VirtualSocketState::CloseWait { remote_fin } if remote_fin.0 == 1
    ));
    let ack = test.take_sent();
    assert_eq!(ack.len(), 1);
    assert_eq!(ack[0].header.htype, Type::State);
    assert_eq!(ack[0].header.ack_nr.0, 1);
    let mut received = Vec::new();
    reader.read_to_end(&mut received).await.unwrap();
    assert!(received.is_empty());
    writer.write_all(b"x").await.unwrap();
}
