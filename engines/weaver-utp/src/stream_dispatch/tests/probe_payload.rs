// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use futures::FutureExt;
use tokio::io::AsyncWriteExt;

use super::make_test_vsock;
use crate::{
    SocketOpts,
    constants::UTP_HEADER,
    raw::{Type::*, UtpHeader},
    traits::UtpEnvironment,
};

#[derive(Clone, Copy)]
enum Removal {
    Expired,
    TooLarge,
}

async fn transfer_after_removing_a_probe(removal: Removal) {
    let mut t = make_test_vsock(
        SocketOpts {
            mtu_probe_max_retransmissions: Some(0),
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    let payload: Vec<u8> = (0..floor + probe)
        .map(|index| b'a' + (index % 26) as u8)
        .collect();
    let mut writer = t.stream.take().unwrap();
    writer.write_all(&payload).await.unwrap();
    let mut flushing = Box::pin(writer.flush());
    assert!(flushing.as_mut().now_or_never().is_none());

    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].payload(), &payload[..floor]);
    let first_seq_nr = sent[0].header.seq_nr;
    let mut reconstructed = sent[0].payload().to_vec();
    if matches!(removal, Removal::TooLarge) {
        t.transport
            .set_max_payload_len(usize::from(UTP_HEADER) + floor);
    }
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: first_seq_nr,
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    if matches!(removal, Removal::Expired) {
        let probe_sent = t.take_sent();
        assert_eq!(probe_sent.len(), 1);
        assert_eq!(probe_sent[0].payload(), &payload[floor..]);
        let expiry = t.vsock.timers.retransmit.poll_at().unwrap();
        t.env
            .increment_now(expiry.saturating_duration_since(t.env.now()));
        t.poll_once_assert_pending().await;
    }

    let mut next_seq_nr = first_seq_nr + 1;
    let mut saw_rejected_probe = false;
    loop {
        if let Some(result) = flushing.as_mut().now_or_never() {
            result.unwrap();
            break;
        }
        let sent = t.take_sent();
        let mut last_ack = None;
        for message in sent {
            assert_eq!(message.header.htype, ST_DATA);
            if matches!(removal, Removal::TooLarge) && message.payload().len() > floor {
                saw_rejected_probe = true;
                continue;
            }
            assert!(message.payload().len() < probe);
            assert_eq!(message.header.seq_nr, next_seq_nr);
            next_seq_nr += 1;
            reconstructed.extend_from_slice(message.payload());
            last_ack = Some(message.header.seq_nr);
        }
        let ack_nr = last_ack.expect("unflushed payload must have replacement data to acknowledge");
        t.send_msg(
            UtpHeader {
                htype: ST_STATE,
                seq_nr: 1.into(),
                ack_nr,
                wnd_size: 1024 * 1024,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;
    }
    assert_eq!(reconstructed, payload);
    if matches!(removal, Removal::TooLarge) {
        assert!(saw_rejected_probe);
    }
}

#[tokio::test]
async fn an_expired_probe_preserves_its_bytes_through_ack_and_flush() {
    transfer_after_removing_a_probe(Removal::Expired).await;
}

#[tokio::test]
async fn a_probe_rejected_as_too_large_preserves_its_bytes_through_ack_and_flush() {
    transfer_after_removing_a_probe(Removal::TooLarge).await;
}
