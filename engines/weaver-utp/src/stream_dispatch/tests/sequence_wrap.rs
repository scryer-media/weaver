// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::num::NonZeroUsize;

use futures::FutureExt;
use tokio::io::AsyncWriteExt;

use super::{TestVsock, calc_mtu_for_mss, make_test_vsock_args};
use crate::{
    Error, SocketOpts,
    raw::{Type::*, UtpHeader},
    seq_nr::SeqNr,
    stream_dispatch::{StreamArgs, VirtualSocketState},
    test_util::env::MockUtpEnvironment,
    traits::UtpEnvironment,
};

const MSS: usize = 528;

fn sender(first_seq_nr: u16, remote_next_seq_nr: u16, capacity: usize) -> (TestVsock, UtpHeader) {
    let env = MockUtpEnvironment::new();
    let remote = UtpHeader {
        htype: State,
        connection_id: 17.into(),
        seq_nr: remote_next_seq_nr.into(),
        ack_nr: SeqNr(first_seq_nr) - 1,
        wnd_size: 1024 * 1024,
        ..Default::default()
    };
    let args = StreamArgs::new_outgoing(&remote, env.now(), env.now());
    let mut t = make_test_vsock_args(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(MSS)),
            vsock_rx_bufsize_bytes: NonZeroUsize::new(2200),
            vsock_tx_bufsize_bytes_initial: NonZeroUsize::new(capacity),
            disable_nagle: true,
            ..Default::default()
        },
        args,
        env,
    );
    t.vsock.congestion_controller.set_window(capacity);
    (t, remote)
}

#[tokio::test]
async fn one_cumulative_ack_retires_a_wide_flight_across_sequence_wrap() {
    const FIRST: u16 = 65500;
    const PACKETS: usize = 1100;
    const TOTAL: usize = PACKETS * MSS;
    let payload: Vec<u8> = (0..TOTAL).map(|index| b'a' + (index % 26) as u8).collect();
    let (mut t, remote) = sender(FIRST, 1, TOTAL);
    let mut stream = t.stream.take().unwrap();
    stream.write_all(&payload).await.unwrap();
    assert!(stream.flush().now_or_never().is_none());

    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), PACKETS);
    for (index, packet) in sent.iter().enumerate() {
        assert_eq!(packet.header.htype, Data);
        assert_eq!(
            packet.header.seq_nr,
            FIRST.wrapping_add(index as u16).into()
        );
        assert_eq!(packet.payload(), &payload[index * MSS..(index + 1) * MSS]);
    }
    assert_eq!(sent.last().unwrap().header.seq_nr, 1063.into());
    assert!(stream.flush().now_or_never().is_none());

    t.send_msg(
        UtpHeader {
            ack_nr: 1063.into(),
            ..remote
        },
        "",
    );
    t.poll_once_assert_pending().await;
    assert_eq!(t._socket.stats().snapshot().invalid_packets, 0);
    assert_eq!(t.vsock.user_tx_segments.acked_seq_nr(), 1063.into());
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 0);
    assert_eq!(t.vsock.user_tx_segments.total_len_bytes(), 0);
    stream
        .flush()
        .now_or_never()
        .expect("the cumulative ACK releases every payload byte")
        .unwrap();
    t.take_sent();

    stream.write_all(b"tail").await.unwrap();
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].header.htype, Data);
    assert_eq!(sent[0].header.seq_nr, 1064.into());
    assert_eq!(sent[0].payload(), b"tail");
    t.send_msg(
        UtpHeader {
            ack_nr: 1064.into(),
            ..remote
        },
        "",
    );
    t.poll_once_assert_pending().await;
    stream
        .flush()
        .now_or_never()
        .expect("the next valid ACK releases the following payload")
        .unwrap();
    assert_eq!(t.vsock.state, VirtualSocketState::Established);
    assert_eq!(t._socket.stats().snapshot().invalid_packets, 0);
}

#[tokio::test]
async fn an_ack_half_a_sequence_space_away_cannot_retire_payload() {
    for first_seq_nr in [0u16, 32768] {
        let (mut t, remote) = sender(first_seq_nr, 1, MSS);
        let mut stream = t.stream.take().unwrap();
        stream.write_all(b"payload").await.unwrap();
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.seq_nr, first_seq_nr.into());
        let window = t.vsock.last_remote_window;

        t.send_msg(
            UtpHeader {
                ack_nr: first_seq_nr.wrapping_add(32768).into(),
                wnd_size: 1,
                ..remote
            },
            "",
        );
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.state, VirtualSocketState::Established);
        assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
        assert_eq!(t.vsock.last_remote_window, window);
        assert_eq!(t._socket.stats().snapshot().invalid_packets, 1);
        assert!(stream.flush().now_or_never().is_none());

        t.send_msg(
            UtpHeader {
                ack_nr: first_seq_nr.into(),
                ..remote
            },
            "",
        );
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 0);
        stream
            .flush()
            .now_or_never()
            .expect("the valid ACK still releases the payload")
            .unwrap();
        assert_eq!(t._socket.stats().snapshot().invalid_packets, 1);
    }
}

#[tokio::test]
async fn a_wrapped_reset_beyond_the_receive_window_is_ignored() {
    let (mut t, remote) = sender(101, 65531, MSS);
    let reset = UtpHeader {
        htype: Reset,
        seq_nr: 2000.into(),
        ..remote
    };
    t.send_msg(reset, "");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.state, VirtualSocketState::Established);
    assert_eq!(t._socket.stats().snapshot().redundant_packets, 1);

    t.send_msg(
        UtpHeader {
            seq_nr: remote.seq_nr,
            ..reset
        },
        "",
    );
    assert!(matches!(
        t.poll_once_assert_ready().await,
        Err(Error::StResetReceived)
    ));
    assert_eq!(t.vsock.state, VirtualSocketState::Closed);
}

#[tokio::test]
async fn a_full_reservation_window_resumes_after_ack_before_sending_fin() {
    use crate::constants::MAX_RESERVED_DATA_PACKETS;

    const FIRST: u16 = 65500;
    let total = MAX_RESERVED_DATA_PACKETS + 2;
    let payload: Vec<u8> = (0..total).map(|index| b'a' + (index % 26) as u8).collect();
    let env = MockUtpEnvironment::new();
    let remote = UtpHeader {
        htype: State,
        connection_id: 17.into(),
        seq_nr: 1.into(),
        ack_nr: SeqNr(FIRST) - 1,
        wnd_size: total as u32,
        ..Default::default()
    };
    let args = StreamArgs::new_outgoing(&remote, env.now(), env.now());
    let mut t = make_test_vsock_args(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(1)),
            vsock_tx_bufsize_bytes_initial: NonZeroUsize::new(total),
            disable_nagle: true,
            ..Default::default()
        },
        args,
        env,
    );
    t.vsock.congestion_controller.set_window(2);
    // Only the writer is closed: a released stream drops what it has not sent
    // yet, while a stream whose reader lives delivers everything first.
    let (reader, mut writer) = t.stream.take().unwrap().split();
    writer.write_all(&payload).await.unwrap();
    drop(writer);

    t.poll_once_assert_pending().await;
    let mut sent = t.take_sent();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|packet| packet.header.htype == Data));
    assert_eq!(
        t.vsock.user_tx_segments.total_len_packets(),
        MAX_RESERVED_DATA_PACKETS
    );
    assert_eq!(t.vsock.this_poll.unsegmented_data, 2);
    assert_eq!(t.vsock.state, VirtualSocketState::Established);

    t.vsock.congestion_controller.set_window(total);
    t.send_msg(
        UtpHeader {
            ack_nr: SeqNr(FIRST) + 1,
            ..remote
        },
        "",
    );
    t.poll_once_assert_pending().await;
    sent.extend(t.take_sent());
    assert_eq!(t.vsock.this_poll.unsegmented_data, 0);
    assert_eq!(sent.len(), total);
    for (index, packet) in sent.iter().enumerate() {
        assert_eq!(packet.header.htype, Data);
        assert_eq!(
            packet.header.seq_nr,
            FIRST.wrapping_add(index as u16).into()
        );
        assert_eq!(packet.payload(), &payload[index..index + 1]);
    }

    // With everything sent, releasing the reader sends the FIN behind it.
    drop(reader);
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    let fin = &sent[0];
    assert_eq!(fin.header.htype, Fin);
    assert_eq!(fin.header.seq_nr, FIRST.wrapping_add(total as u16).into());
    assert!(fin.payload().is_empty());
    assert!(matches!(t.vsock.state, VirtualSocketState::FinWait1 { .. }));

    t.send_msg(
        UtpHeader {
            htype: Fin,
            ack_nr: fin.header.seq_nr,
            ..remote
        },
        "",
    );
    t.poll_once_assert_ready().await.unwrap();
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 0);
    assert_eq!(t.vsock.user_tx_segments.total_len_bytes(), 0);
    assert_eq!(t.vsock.state, VirtualSocketState::Closed);
    assert_eq!(t._socket.stats().snapshot().invalid_packets, 0);
}

#[test]
fn the_receive_reorder_window_stays_below_half_the_sequence_space() {
    let capacity = crate::stream_rx::reorder_capacity(NonZeroUsize::MAX);
    assert_eq!(
        capacity.get(),
        crate::constants::SEQUENCE_HALF_SPACE as usize - 1
    );
}

#[tokio::test]
async fn an_ambiguous_receive_sequence_cannot_reset_the_stream() {
    for last_received in [0u16, 32768] {
        let (mut t, remote) = sender(101, last_received.wrapping_add(1), MSS);
        t.send_msg(
            UtpHeader {
                htype: Reset,
                seq_nr: last_received.wrapping_add(32768).into(),
                ..remote
            },
            "",
        );
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.state, VirtualSocketState::Established);
        assert_eq!(t._socket.stats().snapshot().redundant_packets, 1);

        t.send_msg(
            UtpHeader {
                htype: Reset,
                ..remote
            },
            "",
        );
        assert!(matches!(
            t.poll_once_assert_ready().await,
            Err(Error::StResetReceived)
        ));
    }
}
