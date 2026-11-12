// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use futures::FutureExt;
use tokio::io::AsyncWriteExt;

use super::{TestVsock, calc_mtu_for_mss, make_test_vsock_args};
use crate::{
    SocketOpts,
    pacer::PACE_BURST_PACKETS,
    raw::{Type::*, UtpHeader},
    stream_dispatch::StreamArgs,
    test_util::env::MockUtpEnvironment,
    traits::UtpEnvironment,
};

const MSS: usize = 528;
const WINDOW_PACKETS: usize = 200;

// A sender whose first round trip took `rtt`, with a window of 200 packets
// and twice that queued to send.
async fn sender(rtt: Duration) -> TestVsock {
    let env = MockUtpEnvironment::new();
    let remote = UtpHeader {
        htype: State,
        seq_nr: 1.into(),
        ack_nr: 100.into(),
        wnd_size: 1024 * 1024,
        ..Default::default()
    };
    let syn_sent = env.now();
    env.increment_now(rtt);
    let args = StreamArgs::new_outgoing(&remote, syn_sent, env.now());
    let mut t = make_test_vsock_args(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(MSS)),
            vsock_tx_bufsize_bytes_initial: NonZeroUsize::new(4 * WINDOW_PACKETS * MSS),
            disable_nagle: true,
            ..Default::default()
        },
        args,
        env,
    );
    t.vsock
        .congestion_controller
        .set_window(WINDOW_PACKETS * MSS);
    let mut stream = t.stream.take().unwrap();
    stream
        .write_all(&vec![7; 2 * WINDOW_PACKETS * MSS])
        .await
        .unwrap();
    assert!(stream.flush().now_or_never().is_none());
    t.stream = Some(stream);
    t
}

fn data_sent(t: &TestVsock) -> usize {
    t.take_sent()
        .iter()
        .filter(|message| message.header.htype == Data)
        .count()
}

// However short the path, one pass sends no more than half the window, and
// the rest follows as the bucket fills.
#[tokio::test]
async fn a_short_path_sends_no_more_than_half_the_window_in_one_pass() {
    for rtt in [
        Duration::ZERO,
        Duration::from_micros(100),
        Duration::from_millis(1),
    ] {
        let mut t = sender(rtt).await;
        t.poll_once_assert_pending().await;
        let mut sent = data_sent(&t);
        assert_eq!(sent, WINDOW_PACKETS / 2, "rtt {rtt:?}");

        while sent < WINDOW_PACKETS {
            t.env.increment_now(Duration::from_micros(10));
            t.poll_once_assert_pending().await;
            let pass = data_sent(&t);
            assert!(pass <= WINDOW_PACKETS / 2, "{pass} packets in one pass");
            sent += pass;
        }
        assert_eq!(sent, WINDOW_PACKETS);
    }
}

// With a round trip to spread over, one pass sends no more than the burst,
// and the whole window still goes out within one round trip.
#[tokio::test]
async fn a_paced_window_goes_out_within_one_round_trip() {
    let rtt = Duration::from_millis(100);
    let mut t = sender(rtt).await;
    t.poll_once_assert_pending().await;
    let mut sent = data_sent(&t);
    assert_eq!(sent, PACE_BURST_PACKETS);

    let step = Duration::from_millis(1);
    let mut elapsed = Duration::ZERO;
    while elapsed < rtt {
        t.env.increment_now(step);
        elapsed += step;
        t.poll_once_assert_pending().await;
        let pass = data_sent(&t);
        assert!(pass <= PACE_BURST_PACKETS, "{pass} packets in one pass");
        sent += pass;
    }
    assert_eq!(sent, WINDOW_PACKETS);
}

// Records whether the stream asked to be polled again.
#[derive(Default)]
struct Woken(AtomicBool);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Woken {
    fn take(&self) -> bool {
        self.0.swap(false, Ordering::SeqCst)
    }
}

fn poll_with(t: &mut TestVsock, woken: &Arc<Woken>) -> Poll<crate::Result<()>> {
    let waker = Waker::from(woken.clone());
    t.vsock.poll_unpin(&mut Context::from_waker(&waker))
}

// The pace timer wakes a stream with nothing in flight, and its socket is
// full. No ACK is coming. The clock does not wake the stream again; the
// socket's writable wake does, and the data goes.
#[tokio::test(start_paused = true)]
async fn a_full_socket_with_nothing_in_flight_waits_for_its_writable_wake() {
    let rtt = Duration::from_millis(100);
    let mut t = sender(rtt).await;
    let woken = Arc::new(Woken::default());

    // The first pass sends one burst; the pacer holds the rest.
    assert!(poll_with(&mut t, &woken).is_pending());
    assert_eq!(data_sent(&t), PACE_BURST_PACKETS);

    // Everything sent is acknowledged, so nothing is in flight.
    let last_sent = t.vsock.last_sent_seq_nr;
    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: last_sent,
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );

    // The pace timer fires.
    t.env.increment_now(rtt);
    tokio::time::advance(rtt).await;
    assert!(woken.take());

    // That pass finds the socket full.
    t.transport.set_poll_send_pending(true);
    assert!(poll_with(&mut t, &woken).is_pending());
    assert_eq!(data_sent(&t), 0);
    assert!(!woken.take());

    let step = Duration::from_millis(1);
    t.env.increment_now(step);
    tokio::time::advance(step).await;
    assert!(
        !woken.take(),
        "the clock woke a stream waiting on its socket"
    );
    assert_eq!(t.transport.refused(), 1);

    // Once the socket takes datagrams, it wakes the stream and the data goes.
    t.transport.set_poll_send_pending(false);
    assert!(woken.take(), "the writable wake did not reach the stream");
    assert!(poll_with(&mut t, &woken).is_pending());
    assert!(data_sent(&t) > 0);
    assert_eq!(t.transport.refused(), 1);
}

// Polls and acknowledges everything sent until the queue is empty, then
// returns the largest pass.
async fn drain_acking(t: &mut TestVsock) -> usize {
    let mut largest = 0;
    loop {
        t.env.increment_now(Duration::from_micros(10));
        t.poll_once_assert_pending().await;
        largest = largest.max(data_sent(t));
        if t.vsock.user_tx_segments.is_empty() && t.vsock.this_poll.unsegmented_data == 0 {
            return largest;
        }
        let last_sent = t.vsock.last_sent_seq_nr;
        t.send_msg(
            UtpHeader {
                htype: State,
                seq_nr: 1.into(),
                ack_nr: last_sent,
                wnd_size: 1024 * 1024,
                ..Default::default()
            },
            "",
        );
    }
}

// After an idle gap the window has decayed, and the first pass sends no
// more than half of what is left of it.
#[tokio::test]
async fn the_first_pass_after_idle_sends_no_more_than_half_the_decayed_window() {
    for idle_timeouts in [1u32, 3] {
        let mut t = sender(Duration::from_micros(100)).await;
        drain_acking(&mut t).await;
        assert!(t.vsock.user_tx_segments.is_empty());
        let before = t.vsock.congestion_controller.window();

        let rto = t.vsock.rtte.retransmission_timeout();
        t.env.increment_now(rto * idle_timeouts);
        t.stream
            .as_mut()
            .unwrap()
            .write_all(&vec![7; 2 * WINDOW_PACKETS * MSS])
            .await
            .unwrap();
        t.poll_once_assert_pending().await;
        let burst = data_sent(&t);
        let window = t.vsock.congestion_controller.window();
        assert!(window < before, "the window decayed: {window} of {before}");
        eprintln!("{idle_timeouts} timeouts idle: {burst} packets, window {window} of {before}");
        let allowed = (window / 2).max(PACE_BURST_PACKETS * MSS);
        assert!(
            burst * MSS <= allowed,
            "{idle_timeouts} timeouts idle: {burst} packets after idle, window {window}"
        );
        assert!(burst > 0);
    }
}
