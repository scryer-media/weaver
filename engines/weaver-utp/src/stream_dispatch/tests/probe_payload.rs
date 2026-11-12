// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use futures::FutureExt;
use tokio::io::AsyncWriteExt;

use super::{PROBE_WINDOW, make_test_vsock};
use crate::{
    Error, SocketOpts,
    constants::UTP_HEADER,
    raw::{Type::*, UtpHeader},
    seq_nr::SeqNr,
    test_util::setup_test_logging,
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
    // A window above three floor packets: the second packet is a probe.
    t.vsock.congestion_controller.set_window(PROBE_WINDOW);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    let payload: Vec<u8> = (0..floor + probe)
        .map(|index| b'a' + (index % 26) as u8)
        .collect();
    if matches!(removal, Removal::TooLarge) {
        t.transport
            .set_max_payload_len(usize::from(UTP_HEADER) + floor);
    }
    let mut writer = t.stream.take().unwrap();
    writer.write_all(&payload).await.unwrap();
    let mut flushing = Box::pin(writer.flush());
    assert!(flushing.as_mut().now_or_never().is_none());

    t.poll_once_assert_pending().await;
    let mut sent = t.take_sent();
    assert_eq!(sent[0].payload(), &payload[..floor]);
    let first_seq_nr = sent[0].header.seq_nr;
    let mut reconstructed = sent.remove(0).payload().to_vec();

    if matches!(removal, Removal::Expired) {
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), &payload[floor..]);
        sent.clear();
        t.send_msg(
            UtpHeader {
                htype: State,
                seq_nr: 1.into(),
                ack_nr: first_seq_nr,
                wnd_size: 1024 * 1024,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;
        t.assert_sent_empty();
        let expiry = t.vsock.timers.retransmit.poll_at().unwrap();
        t.env
            .increment_now(expiry.saturating_duration_since(t.env.now()));
        t.poll_once_assert_pending().await;
        // Given up on as too large, the probe goes again whole, under its
        // own sequence number; the segments cut after it are smaller.
        let resent = t.take_sent();
        assert_eq!(resent.len(), 1);
        assert_eq!(resent[0].header.seq_nr, first_seq_nr + 1);
        assert_eq!(resent[0].payload(), &payload[floor..]);
        assert!(usize::from(t.vsock.segment_sizes.max_ss()) < probe);
        sent.extend(resent);
    }

    let mut next_seq_nr = first_seq_nr + 1;
    let mut saw_rejected_probe = false;
    loop {
        if let Some(result) = flushing.as_mut().now_or_never() {
            result.unwrap();
            break;
        }
        sent.extend(t.take_sent());
        let mut last_ack = None;
        for message in sent.drain(..) {
            assert_eq!(message.header.htype, Data);
            if matches!(removal, Removal::TooLarge) && message.payload().len() > floor {
                saw_rejected_probe = true;
                continue;
            }
            // Only the expired probe's own resend is as large as it.
            let whole_probe =
                matches!(removal, Removal::Expired) && message.header.seq_nr == first_seq_nr + 1;
            assert!(whole_probe || message.payload().len() < probe);
            assert_eq!(message.header.seq_nr, next_seq_nr);
            next_seq_nr += 1;
            reconstructed.extend_from_slice(message.payload());
            last_ack = Some(message.header.seq_nr);
        }
        let ack_nr = last_ack.expect("unflushed payload must have replacement data to acknowledge");
        t.send_msg(
            UtpHeader {
                htype: State,
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

// A receiver as both libtorrent's and Weaver's: a data packet whose
// sequence number it holds or has passed is a duplicate and is dropped
// whole (libtorrent v2.0.15 utp_stream.cpp:2426-2441); it acknowledges
// what it holds in order. It also checks that no sequence number is ever
// sent again with a different length.
struct DedupPeer {
    next: SeqNr,
    held: BTreeMap<u16, Vec<u8>>,
    assembled: Vec<u8>,
    lengths: HashMap<u16, usize>,
}

impl DedupPeer {
    fn new(first: SeqNr) -> Self {
        Self {
            next: first,
            held: BTreeMap::new(),
            assembled: Vec::new(),
            lengths: HashMap::new(),
        }
    }

    fn on_data(&mut self, seq_nr: SeqNr, payload: &[u8]) {
        let len = *self.lengths.entry(seq_nr.0).or_insert(payload.len());
        assert_eq!(
            len,
            payload.len(),
            "sequence number {seq_nr} sent again with another length"
        );
        if seq_nr.is_before(self.next) || self.held.contains_key(&seq_nr.0) {
            return;
        }
        self.held.insert(seq_nr.0, payload.to_vec());
        while let Some(bytes) = self.held.remove(&self.next.0) {
            self.assembled.extend_from_slice(&bytes);
            self.next += 1;
        }
    }

    fn ack(&self) -> UtpHeader {
        UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: self.next - 1,
            wnd_size: 1024 * 1024,
            ..Default::default()
        }
    }
}

// The probe reaches the peer, but every ACK of it is lost until the stream
// gives up on it as too large. The stream then resends the same packet,
// whole and under its own sequence number, as ordinary data: the peer drops
// it as the duplicate it is and acknowledges it, and what follows goes on
// from the probe's last byte. Cut again, the probe's sequence number would
// carry fewer bytes, the peer would drop them as a duplicate, and append
// the next packet after the longer probe it holds: bytes twice.
#[tokio::test]
async fn a_probe_whose_acks_are_lost_keeps_its_bytes_and_sequence_number() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    // A window above three floor packets: the second packet is a probe.
    t.vsock.congestion_controller.set_window(PROBE_WINDOW);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    let payload: Vec<u8> = (0..floor + probe + 4 * floor)
        .map(|index| (index % 251) as u8)
        .collect();
    let mut writer = t.stream.take().unwrap();
    writer.write_all(&payload).await.unwrap();
    let mut flushing = Box::pin(writer.flush());
    assert!(flushing.as_mut().now_or_never().is_none());

    t.poll_once_assert_pending().await;
    let sent = t.take_sent();
    let first = sent[0].header.seq_nr;
    let probe_seq_nr = first + 1;
    assert_eq!(sent[1].header.seq_nr, probe_seq_nr);
    assert_eq!(sent[1].payload().len(), probe);
    let mut peer = DedupPeer::new(first);
    let mut sent = sent;
    let mut probe_sends = 0;
    loop {
        if let Some(result) = flushing.as_mut().now_or_never() {
            result.unwrap();
            break;
        }
        // Until the stream lowers its ceiling below the probe, no ACK of
        // the probe comes back.
        let probe_given_up = usize::from(t.vsock.segment_sizes.max_ss()) < probe;
        let mut acked = false;
        for message in sent.drain(..) {
            assert_eq!(message.header.htype, Data);
            if message.header.seq_nr == probe_seq_nr {
                probe_sends += 1;
            }
            peer.on_data(message.header.seq_nr, message.payload());
            if probe_given_up || peer.next.is_at_or_before(probe_seq_nr) {
                t.send_msg(peer.ack(), "");
                acked = true;
            }
        }
        if acked {
            t.poll_once_assert_pending().await;
        }
        sent = t.take_sent();
        if sent.is_empty() {
            // Nothing went out: the timer is next, or, with no timer,
            // everything is acknowledged and the flush ends.
            let Some(expiry) = t.vsock.timers.retransmit.poll_at() else {
                flushing.as_mut().now_or_never().unwrap().unwrap();
                break;
            };
            t.env
                .increment_now(expiry.saturating_duration_since(t.env.now()));
            t.poll_once_assert_pending().await;
            sent = t.take_sent();
        }
    }
    assert_eq!(peer.assembled, payload);
    assert!(probe_sends > 1, "the probe was resent");
    assert!(usize::from(t.vsock.segment_sizes.max_ss()) < probe);
}

// A path that never delivers the probe. The stream gives up on its size:
// it lowers its ceiling, so nothing after it is cut as large, and resends
// the packet as ordinary data, whole, until the ordinary resend limit ends
// the stream. The packets cut after it reach the peer.
#[tokio::test]
async fn a_probe_the_path_always_drops_lowers_the_ceiling_and_ends_the_stream() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            remote_inactivity_timeout: Some(Duration::from_secs(600)),
            ..Default::default()
        },
        false,
    );
    t.vsock.congestion_controller.set_window(PROBE_WINDOW);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; floor + probe + 8 * floor])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let mut sent = t.take_sent();
    let first = sent[0].header.seq_nr;
    let probe_seq_nr = first + 1;
    assert_eq!(sent[1].payload().len(), probe);
    let mut peer = DedupPeer::new(first);
    let mut probe_sends = 0;
    let mut later_packets = 0;
    let died = loop {
        for message in sent.drain(..) {
            if message.header.seq_nr == probe_seq_nr {
                assert_eq!(message.payload().len(), probe);
                probe_sends += 1;
                continue;
            }
            if message.header.seq_nr.is_after(probe_seq_nr) {
                assert!(message.payload().len() < probe, "cut below the ceiling");
                later_packets += 1;
            }
            peer.on_data(message.header.seq_nr, message.payload());
        }
        t.send_msg(peer.ack(), "");
        let polled = t.poll_once().await;
        sent = t.take_sent();
        if let std::task::Poll::Ready(result) = polled {
            break result;
        }
        if sent.is_empty() {
            let expiry = t.vsock.timers.retransmit.poll_at().unwrap();
            t.env
                .increment_now(expiry.saturating_duration_since(t.env.now()));
            let polled = t.poll_once().await;
            sent = t.take_sent();
            if let std::task::Poll::Ready(result) = polled {
                break result;
            }
        }
    };
    assert!(
        matches!(died, Err(Error::MaxRetransmissionsReached)),
        "{died:?}"
    );
    assert!(usize::from(t.vsock.segment_sizes.max_ss()) < probe);
    assert!(later_packets > 0, "packets were cut after the probe");
    assert_eq!(
        probe_sends, t.vsock.socket_opts.num_resends,
        "{probe_sends} sends"
    );
}

#[tokio::test]
async fn an_expired_probe_preserves_its_bytes_through_ack_and_flush() {
    transfer_after_removing_a_probe(Removal::Expired).await;
}

#[tokio::test]
async fn a_probe_rejected_as_too_large_preserves_its_bytes_through_ack_and_flush() {
    transfer_after_removing_a_probe(Removal::TooLarge).await;
}
