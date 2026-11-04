// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use futures::FutureExt;
use tokio::io::AsyncWriteExt;

use crate::{
    Error, SocketOpts,
    raw::{Extensions, Type::*, UtpHeader, selective_ack::SelectiveAck},
    seq_nr::SeqNr,
    stream_dispatch::{StreamArgs, VirtualSocketState},
    test_util::env::MockUtpEnvironment,
    traits::UtpEnvironment,
};

use super::{calc_mtu_for_mss, make_test_vsock_args};

async fn selective_ack_preserves_queued_payload(initial_packets: usize) {
    let env = MockUtpEnvironment::new();
    let ack = UtpHeader {
        htype: State,
        seq_nr: 1.into(),
        ack_nr: 99.into(),
        wnd_size: 1024,
        ..Default::default()
    };
    let args = StreamArgs::new_outgoing(&ack, env.now(), env.now());
    let mut t = make_test_vsock_args(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(1)),
            disable_nagle: true,
            ..Default::default()
        },
        args,
        env,
    );
    t.vsock.congestion_controller.set_window(initial_packets);
    let mut stream = t.stream.take().unwrap();
    stream.write_all(b"abc").await.unwrap();
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), initial_packets);
    for (index, packet) in sent.iter().enumerate() {
        assert_eq!(packet.header.htype, Data);
        assert_eq!(packet.header.seq_nr, SeqNr(100 + index as u16));
        assert_eq!(packet.payload(), &b"abc"[index..index + 1]);
    }
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 3);
    assert!(stream.flush().now_or_never().is_none());

    // Both bits name queued packets; only the second fixture has sent 101.
    t.send_msg(
        UtpHeader {
            extensions: Extensions {
                selective_ack: SelectiveAck::new_test([0, 1]),
                ..Default::default()
            },
            ..ack
        },
        "",
    );
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.vsock.user_tx_segments.count_delivered_test(),
        initial_packets - 1,
        "only previously sent packets can be selectively acknowledged"
    );
    let mut subsequently_sent = t.take_sent();
    assert!(stream.flush().now_or_never().is_none());

    t.vsock.congestion_controller.set_window(3);
    t.send_msg(
        UtpHeader {
            ack_nr: 100.into(),
            ..ack
        },
        "",
    );
    t.poll_once_assert_pending().await;
    subsequently_sent.extend(t.take_sent());
    let data: Vec<_> = subsequently_sent
        .iter()
        .filter(|packet| packet.header.htype == Data)
        .collect();
    let payload: Vec<_> = data
        .iter()
        .flat_map(|packet| packet.payload())
        .copied()
        .collect();
    assert_eq!(payload, &b"abc"[initial_packets..]);
    assert_eq!(data.last().unwrap().header.seq_nr, 102.into());
    assert!(stream.flush().now_or_never().is_none());

    t.send_msg(
        UtpHeader {
            ack_nr: 102.into(),
            ..ack
        },
        "",
    );
    t.poll_once_assert_pending().await;
    assert!(stream.flush().now_or_never().unwrap().is_ok());
}

#[tokio::test]
async fn a_selective_ack_cannot_discard_never_sent_payload() {
    selective_ack_preserves_queued_payload(1).await;
}

#[tokio::test]
async fn a_selective_ack_accepts_sent_bits_without_discarding_unsent_payload() {
    selective_ack_preserves_queued_payload(2).await;
}

fn reset_fixture(last_received: SeqNr) -> super::TestVsock {
    let env = MockUtpEnvironment::new();
    let ack = UtpHeader {
        htype: State,
        seq_nr: last_received + 1,
        ack_nr: 100.into(),
        wnd_size: 1024,
        ..Default::default()
    };
    let args = StreamArgs::new_outgoing(&ack, env.now(), env.now());
    make_test_vsock_args(
        SocketOpts {
            vsock_rx_bufsize_bytes: std::num::NonZeroUsize::new(2200),
            ..Default::default()
        },
        args,
        env,
    )
}

#[tokio::test]
async fn a_reset_past_the_receive_reorder_window_is_ignored() {
    for last_received in [SeqNr(0), SeqNr(65530)] {
        let mut t = reset_fixture(last_received);
        let reset = UtpHeader {
            htype: Reset,
            seq_nr: last_received + 17,
            ack_nr: 100.into(),
            ..Default::default()
        };
        t.send_msg(reset, "");
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.state, VirtualSocketState::Established);

        t.send_msg(
            UtpHeader {
                seq_nr: last_received + 16,
                ..reset
            },
            "",
        );
        assert!(matches!(
            t.poll_once_assert_ready().await,
            Err(Error::StResetReceived)
        ));
    }
}

#[tokio::test]
async fn a_reset_with_an_old_receive_sequence_is_still_accepted() {
    let mut t = reset_fixture(100.into());
    t.send_msg(
        UtpHeader {
            htype: Reset,
            seq_nr: 99.into(),
            ack_nr: 100.into(),
            ..Default::default()
        },
        "",
    );
    assert!(matches!(
        t.poll_once_assert_ready().await,
        Err(Error::StResetReceived)
    ));
}

#[tokio::test]
async fn a_fin_ack_reset_must_also_pass_the_receive_reorder_guard() {
    let mut t = reset_fixture(0.into());
    t.send_msg(
        UtpHeader {
            htype: Fin,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    let fin = sent
        .iter()
        .find(|packet| packet.header.htype == Fin)
        .unwrap();
    let reset = UtpHeader {
        htype: Reset,
        seq_nr: 18.into(),
        ack_nr: fin.header.seq_nr,
        ..Default::default()
    };
    t.send_msg(reset, "");
    t.poll_once_assert_pending().await;
    assert!(matches!(t.vsock.state, VirtualSocketState::LastAck { .. }));
    t.send_msg(
        UtpHeader {
            seq_nr: 1.into(),
            ..reset
        },
        "",
    );
    t.poll_once_assert_ready().await.unwrap();
    assert_eq!(t.vsock.state, VirtualSocketState::Closed);
}
