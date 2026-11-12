// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A send the socket refuses is no send: it costs no resend budget, counts
// toward no give-up, and goes again on the socket's writable wake, which
// reaches every stream the socket refused.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use futures::FutureExt;
use tokio::io::AsyncWriteExt;

use crate::{
    Error, SocketOpts, UtpSocket,
    raw::{Type::*, UtpHeader, selective_ack::SelectiveAck},
    rtte::RttEstimator,
    stream_dispatch::{
        StreamArgs, UtpStreamStarter, VirtualSocket,
        tests::{TestVsock, calc_mtu_for_mss, make_test_vsock},
    },
    test_util::{
        ADDR_1, env::MockUtpEnvironment, setup_test_logging, transport::RememberingTransport,
    },
    traits::UtpEnvironment,
};

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

type Vsock = VirtualSocket<RememberingTransport, MockUtpEnvironment>;

fn poll_with(vsock: &mut Vsock, woken: &Arc<Woken>) -> Poll<crate::Result<()>> {
    let waker = Waker::from(woken.clone());
    vsock.poll_unpin(&mut Context::from_waker(&waker))
}

// Two streams on one socket, both refused by it, the second after the
// first. The transport keeps only the last waker it was handed, as a kernel
// socket does; its one writable wake still reaches both streams, and the
// clock reaches neither.
#[tokio::test(start_paused = true)]
async fn one_writable_wake_reaches_every_stream_the_socket_refused() {
    setup_test_logging();
    let env = MockUtpEnvironment::new();
    let transport = RememberingTransport::new(ADDR_1);
    let socket = UtpSocket::new_with_opts(
        transport.clone(),
        env.clone(),
        SocketOpts {
            disable_nagle: true,
            ..Default::default()
        },
    )
    .unwrap();
    let remote = UtpHeader {
        htype: State,
        seq_nr: 1.into(),
        ack_nr: 100.into(),
        wnd_size: 1024 * 1024,
        ..Default::default()
    };
    let mut streams = Vec::new();
    for port in [2, 3] {
        let (_tx, rx) = crate::ingress::channel(socket.opts().vsock_rx_bufsize);
        let args = StreamArgs::new_outgoing(&remote, env.now(), env.now());
        let UtpStreamStarter {
            stream, mut vsock, ..
        } = UtpStreamStarter::new(&socket, ([127, 0, 0, port], 4000).into(), rx, args);
        vsock.transport = socket.state.runtime.upgrade().unwrap().transport();
        let mut stream = stream;
        stream.write_all(b"hello").await.unwrap();
        assert!(stream.flush().now_or_never().is_none());
        streams.push((vsock, stream, _tx, Arc::new(Woken::default())));
    }

    transport.set_poll_send_pending(true);
    for (vsock, _, _, woken) in &mut streams {
        assert!(poll_with(vsock, woken).is_pending());
        assert!(!woken.take());
    }
    assert_eq!(transport.refused(), 2);
    assert!(transport.take_sent().is_empty());

    // Polled again while the socket is full, neither asks the socket.
    let step = Duration::from_millis(1);
    env.increment_now(step);
    tokio::time::advance(step).await;
    for (vsock, _, _, woken) in &mut streams {
        assert!(
            !woken.take(),
            "the clock woke a stream waiting on its socket"
        );
        assert!(poll_with(vsock, woken).is_pending());
    }
    assert_eq!(transport.refused(), 2);
    assert_eq!(
        socket.state.stats.send_poll_pending.load(Ordering::Relaxed),
        2
    );

    transport.set_poll_send_pending(false);
    for (vsock, _, _, woken) in &mut streams {
        assert!(woken.take(), "the writable wake missed a stream");
        assert!(poll_with(vsock, woken).is_pending());
        assert_eq!(vsock.totals.packets_sent, 1);
    }
    assert_eq!(transport.take_sent().len(), 2);
}

// One SACK shows forty packets lost; eight ACKs carry it. With `refuse`,
// the socket is full for every other ACK: that ACK's pass is refused, the
// next ACK comes while it is still full, then it takes datagrams again.
// Returns the packets resent in order, the stream's fast resends and
// packets sent, its budget left, and the sends the socket refused.
async fn forty_holes(refuse: bool) -> (Vec<u16>, u64, u64, usize, usize) {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        false,
    );
    t.vsock.rtte.force_timeout(Duration::from_secs(10));
    t.vsock.congestion_controller.set_window(4000);
    let ack = |sacked: &[usize]| UtpHeader {
        htype: State,
        seq_nr: 0.into(),
        ack_nr: 100.into(),
        wnd_size: 1 << 20,
        extensions: crate::raw::Extensions {
            selective_ack: SelectiveAck::new_test(sacked.iter().copied()),
            ..Default::default()
        },
        ..Default::default()
    };
    t.send_msg(ack(&[]), "");
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[b'x'; 5 * 100])
        .await
        .unwrap();
    let mut first = 0;
    while first < 100 {
        t.poll_once_assert_pending().await;
        first += t.take_sent().len();
        t.env.increment_now(Duration::from_millis(1));
    }

    // 101 and every other packet to 179 lost, 180 to 182 delivered.
    let sacked: Vec<usize> = (1..78).step_by(2).chain(78..81).collect();
    let mut resent = Vec::new();
    for round in 0..8 {
        t.env.increment_now(Duration::from_millis(100));
        t.send_msg(ack(&sacked), "");
        if refuse && round % 2 == 0 {
            t.transport.set_poll_send_pending(true);
            t.poll_once_assert_pending().await;
            assert!(t.take_sent().is_empty(), "round {round}");
            continue;
        }
        t.poll_once_assert_pending().await;
        t.transport.set_poll_send_pending(false);
        t.poll_once_assert_pending().await;
        resent.extend(t.take_sent().iter().map(|p| p.header.seq_nr.0));
    }
    (
        resent,
        t.vsock.totals.fast_resends,
        t.vsock.totals.packets_sent,
        t.vsock.fast_resend_budget,
        t.transport.refused(),
    )
}

#[tokio::test]
async fn refused_resends_keep_their_budget_and_count_as_no_send() {
    let (resent, fast, sent, budget, refused) = forty_holes(false).await;
    let holes: Vec<u16> = std::iter::once(101)
        .chain((0..78).step_by(2).map(|i| 102 + i as u16))
        .collect();
    assert_eq!(resent, holes);
    assert_eq!(refused, 0);

    let (resent_r, fast_r, sent_r, budget_r, refused_r) = forty_holes(true).await;
    assert_eq!(refused_r, 4, "one refusal per full ACK");
    assert_eq!(
        resent_r, resent,
        "the same packets resent, in the same order"
    );
    assert_eq!((fast_r, sent_r, budget_r), (fast, sent, budget));
    assert_eq!(fast_r, 40);
}

// Four packets in flight on a path that never answers. Without a measured
// round trip no tail probe goes out.
async fn dead_path() -> TestVsock {
    setup_test_logging();
    let min_timeout = Duration::from_millis(500);
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            link_mtu: Some(calc_mtu_for_mss(100)),
            min_timeout: Some(min_timeout),
            ..Default::default()
        },
        false,
    );
    t.vsock.rtte = RttEstimator::new(min_timeout);
    t.vsock.rtte.confirm();
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[7u8; 400])
        .await
        .unwrap();
    let sent = t.poll_until_sent(4).await;
    assert_eq!(sent.len(), 4);
    t
}

// Steps the clock 1 ms a pass to the stream's end. With `refuse`, the
// socket refuses the first try of each pass the retransmit timer is due,
// then takes the datagram. Returns the error, the timer expiries the stream
// and its socket counted, each packet's timer resends on the wire, the due
// passes that sent anything, and the sends refused.
async fn run_to_death(
    t: &mut TestVsock,
    refuse: bool,
) -> (Error, u64, u64, BTreeMap<u16, usize>, usize, usize) {
    let mut resends = BTreeMap::new();
    let mut due_sending = 0;
    loop {
        t.env.increment_now(Duration::from_millis(1));
        let due = t
            .vsock
            .timers
            .retransmit
            .poll_at()
            .is_some_and(|at| at <= t.env.now());
        let result = if refuse && due {
            t.transport.set_poll_send_pending(true);
            let refused = t.poll_once().await;
            assert!(t.take_sent().is_empty());
            t.transport.set_poll_send_pending(false);
            match refused {
                Poll::Pending => t.poll_once().await,
                ended => ended,
            }
        } else {
            t.poll_once().await
        };
        let sent = t.take_sent();
        if due && !sent.is_empty() {
            due_sending += 1;
        }
        for m in sent {
            if m.header.get_type() == Data {
                *resends.entry(m.header.seq_nr.0).or_default() += 1;
            }
        }
        if let Poll::Ready(r) = result {
            return (
                r.unwrap_err(),
                t.vsock.totals.timer_expiries,
                t._socket.stats().snapshot().timeouts,
                resends,
                due_sending,
                t.transport.refused(),
            );
        }
    }
}

#[tokio::test]
async fn refused_timer_resends_change_no_count_toward_giving_up() {
    let (error, expiries, timeouts, resends, due_sending, refused) =
        run_to_death(&mut dead_path().await, false).await;
    assert!(
        matches!(error, Error::MaxRetransmissionsReached),
        "{error:?}"
    );
    assert_eq!(refused, 0);
    assert_eq!((expiries, timeouts), (3, 3));
    assert_eq!(resends, BTreeMap::from([(101, 2)]));
    // Two timer resends and the reset sent on giving up.
    assert_eq!(due_sending, 3);

    let (error_r, expiries_r, timeouts_r, resends_r, _, refused_r) =
        run_to_death(&mut dead_path().await, true).await;
    assert!(
        matches!(error_r, Error::MaxRetransmissionsReached),
        "{error_r:?}"
    );
    assert_eq!((expiries_r, timeouts_r), (expiries, timeouts));
    assert_eq!(
        resends_r, resends,
        "each packet's timer resends on the wire"
    );
    assert_eq!(refused_r, due_sending, "one refusal per due pass");
}
