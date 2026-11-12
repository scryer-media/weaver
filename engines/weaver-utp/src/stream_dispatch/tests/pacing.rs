// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{num::NonZeroUsize, time::Duration};

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

// On a path with no delay to spread over, the whole window goes in the
// first pass.
#[tokio::test]
async fn a_zero_delay_path_sends_the_whole_window_in_one_pass() {
    let mut t = sender(Duration::ZERO).await;
    t.poll_once_assert_pending().await;
    assert_eq!(data_sent(&t), WINDOW_PACKETS);
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
