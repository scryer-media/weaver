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
    stream_dispatch::{SEND_RETRY, StreamArgs},
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
// full. The socket never wakes it, as when another task polled the socket
// full after it; no ACK is coming either. The stream still tries again.
#[tokio::test(start_paused = true)]
async fn a_full_socket_with_nothing_in_flight_does_not_strand_the_stream() {
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

    t.env.increment_now(SEND_RETRY);
    tokio::time::advance(SEND_RETRY).await;
    assert!(woken.take(), "nothing woke the stream after a full socket");

    // Once the socket takes datagrams, the data goes.
    t.transport.set_poll_send_pending(false);
    assert!(poll_with(&mut t, &woken).is_pending());
    assert!(data_sent(&t) > 0);
}
