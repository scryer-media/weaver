// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a probe is resent: with no don't-fragment flag, by the timer or on a
// SACK's evidence, and how it is given up.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader, selective_ack::SelectiveAck},
    stream_dispatch::tests::{PROBE_WINDOW, TestVsock, make_test_vsock},
    test_util::setup_test_logging,
    traits::UtpEnvironment,
};

fn ack(ack_nr: u16, sacked: &[usize]) -> UtpHeader {
    UtpHeader {
        htype: State,
        seq_nr: 1.into(),
        ack_nr: ack_nr.into(),
        wnd_size: 1 << 20,
        extensions: crate::raw::Extensions {
            selective_ack: SelectiveAck::new_test(sacked.iter().copied()),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn sent_fragmenting(t: &mut TestVsock) -> Vec<(u16, usize, Option<crate::Fragmenting>)> {
    t.take_sent_fragmenting()
        .into_iter()
        .filter(|(m, _)| m.header.get_type() == Data)
        .map(|(m, f)| (m.header.seq_nr.0, m.payload().len(), f))
        .collect()
}

fn expire_retransmit(t: &mut TestVsock) {
    let deadline = t.vsock.timers.retransmit.poll_at().unwrap();
    t.env.increment_now(deadline.duration_since(t.env.now()));
}

// 101 (floor) and 102 (probe) go out; the ACK of 101 lets 103 (floor) go
// behind the probe. Nothing more arrives: the timer fires. libtorrent
// forgets the probe at the timeout (v2.0.15 utp_stream.cpp:3498) and
// resends the head with no flags (:2045-2046): the probe goes again allowed
// to fragment, at the first timeout, so a probe too large for the path still
// arrives and no second, doubled timeout passes.
#[tokio::test]
async fn the_first_timer_resend_of_a_probe_allows_fragmenting() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    t.transport.set_controls_fragmenting();
    t.vsock.congestion_controller.set_window(PROBE_WINDOW);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; floor + probe + floor])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let first = sent_fragmenting(&mut t);
    assert_eq!(
        first,
        [
            (101, floor, None),
            (102, probe, Some(crate::Fragmenting::Never))
        ]
    );
    t.send_msg(ack(101, &[]), "");
    t.poll_once_assert_pending().await;
    assert_eq!(sent_fragmenting(&mut t), [(103, floor, None)]);

    expire_retransmit(&mut t);
    t.poll_once_assert_pending().await;
    let at_first_timeout = sent_fragmenting(&mut t);
    assert_eq!(
        at_first_timeout.first().copied(),
        Some((102, probe, Some(crate::Fragmenting::Allowed))),
        "{at_first_timeout:?}"
    );
    assert_eq!(t.vsock.rtte.timeouts(), 1);
    assert!(!t.vsock.user_tx_segments.has_live_mtu_probe());
}

// 101 (floor), 102 (probe) and 103-107 (floor) go out. 101 and 102 are
// lost; a SACK shows 103-107. libtorrent resends 101 and 102 (v2.0.15
// utp_stream.cpp:1340-1355); 102 is not at the head, so the ceiling stays
// (:1975), and resend_packet sends it with no flags (:2045-2046), allowing
// fragmentation.
#[tokio::test]
async fn a_probe_resent_behind_the_head_allows_fragmenting() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    t.transport.set_controls_fragmenting();
    t.vsock.congestion_controller.set_window(64 * 1024);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; floor + probe + 5 * floor])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let first = sent_fragmenting(&mut t);
    assert_eq!(
        first.iter().map(|s| (s.0, s.1)).collect::<Vec<_>>(),
        [
            (101, floor),
            (102, probe),
            (103, floor),
            (104, floor),
            (105, floor),
            (106, floor),
            (107, floor)
        ]
    );
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(ack(100, &[1, 2, 3, 4, 5]), "");
    t.poll_once_assert_pending().await;
    let resent = sent_fragmenting(&mut t);
    let probe_resend = resent.iter().find(|s| s.0 == 102).copied();
    assert_eq!(
        probe_resend,
        Some((102, probe, Some(crate::Fragmenting::Allowed))),
        "resent: {resent:?}"
    );
    assert!(
        usize::from(t.vsock.segment_sizes.max_ss()) >= probe,
        "the ceiling stays"
    );
    assert!(!t.vsock.user_tx_segments.has_live_mtu_probe());
}

// 101 (floor), 102 (probe) and 103-107 (floor) go out; 101 and 103-106
// arrive, the probe does not. The ACK of 101 with a SACK of 103-106 shows
// 102 lost at the head, behind a packet that arrived: it was too large.
// libtorrent's duplicate-ACK resend gives up on it there, lowering the
// ceiling below it, and cuts no window for it (v2.0.15 utp_stream.cpp:1346-1349,
// 1975-1984); the resend carries no don't-fragment flag (:2045-2046).
#[tokio::test]
async fn a_probe_lost_at_the_head_is_given_up_on_duplicate_acks() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    t.transport.set_controls_fragmenting();
    t.vsock.congestion_controller.set_window(64 * 1024);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; floor + probe + 5 * floor])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let first = sent_fragmenting(&mut t);
    assert_eq!(first[1], (102, probe, Some(crate::Fragmenting::Never)));
    let window = t.vsock.congestion_controller.window();
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(ack(101, &[0, 1, 2, 3]), "");
    t.poll_once_assert_pending().await;
    let resent = sent_fragmenting(&mut t);
    assert_eq!(
        resent.first().copied(),
        Some((102, probe, Some(crate::Fragmenting::Allowed))),
        "resent: {resent:?}"
    );
    assert!(usize::from(t.vsock.segment_sizes.max_ss()) < probe);
    assert!(!t.vsock.user_tx_segments.has_live_mtu_probe());
    assert!(
        t.vsock.congestion_controller.window() >= window,
        "no cut for the probe"
    );
    assert_eq!(t.vsock.rtte.timeouts(), 0);
}

// 101 (floor), 102 (probe) and 103 (floor) go out and nothing comes back.
// The timer forgets the probe, as libtorrent clears m_mtu_seq at every
// timeout (v2.0.15 utp_stream.cpp:3498), but the probe is not the oldest
// packet out: nothing shows its size lost, and the ceiling stays.
#[tokio::test]
async fn a_probe_behind_the_head_at_a_timeout_leaves_the_ceiling() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    t.transport.set_controls_fragmenting();
    t.vsock.congestion_controller.set_window(64 * 1024);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7; floor + probe + floor])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    let first = sent_fragmenting(&mut t);
    assert_eq!(
        first,
        [
            (101, floor, None),
            (102, probe, Some(crate::Fragmenting::Never)),
            (103, floor, None)
        ]
    );
    expire_retransmit(&mut t);
    t.poll_once_assert_pending().await;
    let resent = sent_fragmenting(&mut t);
    assert_eq!(
        resent.first().copied(),
        Some((101, floor, None)),
        "{resent:?}"
    );
    assert!(!t.vsock.user_tx_segments.has_live_mtu_probe());
    assert!(
        usize::from(t.vsock.segment_sizes.max_ss()) >= probe,
        "the ceiling stays"
    );
}
