// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stream on a socket whose transport sends from one task queues its
// datagrams; the socket's dispatcher sends them in order, stamping each as
// it goes.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
    time::Duration,
};

use futures::FutureExt;

use tokio::io::AsyncWriteExt;

use crate::{
    Error, Fragmenting, SocketOpts,
    ingress::SendFault,
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::{
        PROBE_WINDOW, TestVsock, calc_mtu_for_mss, make_test_vsock_on, test_stream_args,
    },
    test_util::{ADDR_1, env::MockUtpEnvironment, transport::RememberingTransport},
    traits::UtpEnvironment,
};

fn one_task_vsock(opts: SocketOpts) -> TestVsock {
    let transport = RememberingTransport::new(ADDR_1);
    transport.set_sends_from_one_task();
    let env = MockUtpEnvironment::new();
    let args = test_stream_args(&env, false);
    make_test_vsock_on(transport, opts, args, env)
}

fn nagle_off() -> SocketOpts {
    SocketOpts {
        disable_nagle: true,
        ..Default::default()
    }
}

impl TestVsock {
    async fn write(&mut self, bytes: usize) {
        self.stream
            .as_mut()
            .unwrap()
            .write_all(&vec![7; bytes])
            .await
            .unwrap();
    }

    fn data_queued(&self) -> u64 {
        self._socket.stats().snapshot().data_packets_sent
    }

    // The socket's timestamp now, as its dispatcher stamps a send.
    fn stamp_now(&self) -> u32 {
        (self.env.now() - self._socket.state.created).as_micros() as u32
    }
}

// The stream's data waits in the outbox until the dispatcher's turn, and
// leaves in sequence order, each datagram stamped when it was sent rather
// than when it was queued.
#[tokio::test]
async fn queued_data_leaves_in_order_stamped_at_its_send() {
    let mut t = one_task_vsock(nagle_off());
    let mss = usize::from(t.vsock.segment_sizes.mss());
    t.write(2 * mss).await;
    t.poll_once_assert_pending().await;
    let queued = t.data_queued();
    assert_eq!(queued, 2);
    t.assert_sent_empty_msg("nothing leaves before the dispatcher's turn");
    let queued_stamp = t.stamp_now();

    t.env.increment_now(Duration::from_millis(5));
    t.transport
        .wait_until(|sent, _| sent.len() as u64 == queued)
        .await;
    let sent = t.take_sent();
    let seq_nrs: Vec<u16> = sent.iter().map(|m| m.header.seq_nr.0).collect();
    assert_eq!(seq_nrs, [101, 102]);
    for message in &sent {
        assert_eq!(message.header.get_type(), Data);
        assert_eq!(message.header.timestamp_microseconds, t.stamp_now());
        assert_ne!(message.header.timestamp_microseconds, queued_stamp);
    }
}

// A size probe is sent by the stream itself, so the host's refusal reaches
// it at once; it waits for the stream's queued datagrams to leave first.
#[tokio::test]
async fn a_probe_goes_after_the_datagrams_queued_before_it() {
    let mut t = one_task_vsock(nagle_off());
    t.transport.set_controls_fragmenting();
    // A window above three floor packets: the second packet is a probe.
    t.vsock.congestion_controller.set_window(PROBE_WINDOW);
    let floor = usize::from(t.vsock.segment_sizes.mss());
    let probe = usize::from(t.vsock.segment_sizes.probe_size());
    t.write(floor + probe).await;

    t.poll_once_assert_pending().await;
    assert_eq!(t.data_queued(), 1);
    assert!(t.vsock.this_poll.transport_pending);
    t.assert_sent_empty();

    t.transport.wait_until(|sent, _| sent.len() == 1).await;
    t.poll_once_assert_pending().await;
    let sent: Vec<(usize, Option<Fragmenting>)> = t
        .take_sent_fragmenting()
        .into_iter()
        .map(|(message, fragmenting)| (message.payload().len(), fragmenting))
        .collect();
    assert_eq!(
        sent,
        [(floor, None), (probe, Some(Fragmenting::Never))],
        "the queued packet first, then the probe sent by the stream"
    );
}

// While the transport refuses the dispatcher, the stream is refused too,
// as a full socket refused it when it sent itself. The writable wake lets
// the held datagrams out first, in order, then the stream's next.
#[tokio::test]
async fn a_refused_dispatcher_refuses_the_stream_until_its_backlog_leaves() {
    // A path no larger than the floor, so no packet is a probe, and a
    // window that holds every packet.
    let mut t = one_task_vsock(SocketOpts {
        link_mtu: Some(calc_mtu_for_mss(528)),
        ..nagle_off()
    });
    t.vsock.congestion_controller.set_window(1024 * 1024);
    let mss = usize::from(t.vsock.segment_sizes.mss());
    assert_eq!(mss, usize::from(t.vsock.segment_sizes.max_ss()));
    t.transport.set_poll_send_pending(true);
    t.write(2 * mss).await;
    t.poll_once_assert_pending().await;
    assert_eq!(t.data_queued(), 2);
    t.transport.wait_until(|_, refused| refused == 1).await;
    t.assert_sent_empty();

    t.write(mss).await;
    t.poll_once_assert_pending().await;
    assert!(t.vsock.this_poll.transport_pending);
    assert_eq!(t.data_queued(), 2, "refused while the backlog is held");
    let pending = t
        ._socket
        .stats()
        .send_poll_pending
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(pending, 2, "the dispatcher's refusal and the stream's");

    t.transport.set_poll_send_pending(false);
    t.transport.wait_until(|sent, _| sent.len() == 2).await;
    t.poll_once_assert_pending().await;
    assert_eq!(t.data_queued(), 3);
    t.transport.wait_until(|sent, _| sent.len() == 3).await;
    let seq_nrs: Vec<u16> = t.take_sent().iter().map(|m| m.header.seq_nr.0).collect();
    assert_eq!(seq_nrs, [101, 102, 103]);
    assert_eq!(t.transport.refused(), 1);
}

// The ACK owed when the receive loop drained is queued in the same pass of
// the stream and leaves on the dispatcher's next turn, with the clock where
// it was: no timer stands between them.
#[tokio::test]
async fn an_ack_owed_at_a_drain_leaves_in_the_same_turn() {
    let mut t = one_task_vsock(nagle_off());
    t.send_data(1, 100, "hello");
    let received_stamp = t.stamp_now();
    t.poll_once_assert_pending().await;
    let stats = t._socket.stats().snapshot();
    assert_eq!(stats.immediate_acks, 1);
    assert_eq!(stats.ack_packets_sent, 1);

    t.transport.wait_until(|sent, _| sent.len() == 1).await;
    let sent = t.take_sent();
    assert_eq!(sent[0].header.get_type(), State);
    assert_eq!(sent[0].header.ack_nr.0, 1);
    assert_eq!(sent[0].header.timestamp_microseconds, received_stamp);
}

// A send the dispatcher made that the host refused as too large lowers the
// sizes the stream cuts from then on; one that failed for good ends the
// stream, as its own failed send does.
#[tokio::test]
async fn faults_the_dispatcher_reports_reach_the_stream() {
    let mut t = one_task_vsock(nagle_off());
    let max_ss = usize::from(t.vsock.segment_sizes.max_ss());
    let refused_payload = max_ss - 100;
    t.tx.report_send_fault(SendFault::TooLarge(refused_payload));
    t.poll_once_assert_pending().await;
    assert!(usize::from(t.vsock.segment_sizes.max_ss()) < refused_payload);

    t.tx.report_send_fault(SendFault::Failed(std::io::Error::other("unreachable")));
    match t.poll_once_assert_ready().await {
        Err(Error::Send(error)) => assert_eq!(error.to_string(), "unreachable"),
        other => panic!("expected the send failure, got {other:?}"),
    }
}

// A queued data packet is timed from when the dispatcher sent it, as
// libtorrent times a packet from its send (utp_stream.cpp:1792, 2223): its
// round trip leaves out its wait in the outbox, and the retransmission
// timer it started runs from its send, not from when it was queued.
#[tokio::test]
async fn a_queued_packet_is_timed_from_its_send() {
    let mut t = one_task_vsock(nagle_off());
    let ack = |ack_nr: u16| UtpHeader {
        htype: State,
        seq_nr: 1.into(),
        ack_nr: ack_nr.into(),
        wnd_size: 1024 * 1024,
        ..Default::default()
    };
    t.send_msg(ack(100), "");
    for _ in 0..100 {
        t.vsock.rtte.sample(Duration::from_millis(40));
    }
    let rtt = t.vsock.rtte.roundtrip_time();
    let rto = t.vsock.rtte.retransmission_timeout();
    assert_eq!(rto, Duration::from_millis(500));
    let timeouts = |t: &TestVsock| t._socket.stats().snapshot().timeouts;

    // The dispatcher is refused: the packet waits in the outbox.
    t.transport.set_poll_send_pending(true);
    t.write(1).await;
    t.poll_once_assert_pending().await;
    assert_eq!(t.data_queued(), 1);
    t.transport.wait_until(|_, refused| refused == 1).await;
    let queued_at = t.env.now();
    assert_eq!(t.vsock.timers.retransmit.poll_at(), Some(queued_at + rto));

    // Past a timeout from its queueing it has still not left, so its timer
    // has not started: no timeout, no resend.
    t.env.increment_now(rto + Duration::from_millis(100));
    t.poll_once_assert_pending().await;
    assert_eq!(timeouts(&t), 0);
    assert_eq!(t.vsock.totals.tail_probes, 0);
    assert_eq!(t.vsock.timers.retransmit.poll_at(), Some(t.env.now() + rto));

    // It leaves; the timer runs from then.
    t.env.increment_now(Duration::from_millis(100));
    let sent_at = t.env.now();
    t.transport.set_poll_send_pending(false);
    t.transport.wait_until(|sent, _| sent.len() == 1).await;
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.timers.retransmit.poll_at(), Some(sent_at + rto));

    // Its ACK a round trip after it left: the sample is that round trip,
    // not the 740 ms since it was queued.
    t.env.increment_now(rtt);
    t.send_msg(ack(101), "");
    t.poll_once_assert_pending().await;
    assert!(t.vsock.user_tx_segments.is_empty());
    assert_eq!(timeouts(&t), 0);
    assert_eq!(t.vsock.rtte.roundtrip_time(), rtt);
    assert_eq!(t.vsock.rtte.retransmission_timeout(), rto);
}

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl TestVsock {
    // One poll with `wakes` as the stream task's waker.
    fn poll_counting(&mut self, wakes: &Arc<Wakes>) {
        self.tx.flush();
        let waker = Waker::from(wakes.clone());
        assert!(
            self.vsock
                .poll_unpin(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
}

// A one-packet flight queued for the dispatcher. The stream is woken once
// when the dispatcher sends it, and its tail probe then runs from that
// send, as on a transport the stream sends on itself (tail_probe.rs). The
// timers the queueing poll left are not all it gets.
#[tokio::test]
async fn a_queued_single_packet_flight_keeps_its_tail_probe() {
    let mut t = one_task_vsock(nagle_off());
    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
    for _ in 0..100 {
        t.vsock.rtte.sample(Duration::from_millis(40));
    }
    let wakes = Arc::new(Wakes::default());
    t.poll_counting(&wakes);
    t.write(1).await;
    t.poll_counting(&wakes);
    assert_eq!(t.data_queued(), 1);
    let before = wakes.0.load(Ordering::SeqCst);

    // The dispatcher's turn comes 30 ms later.
    t.env.increment_now(Duration::from_millis(30));
    let sent_at = t.env.now();
    t.transport
        .wait_until(|sent, _| sent.iter().any(|(_, m)| m.len() > 20))
        .await;
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        before + 1,
        "one wake for the send"
    );

    t.poll_counting(&wakes);
    let rtt = t.vsock.rtte.roundtrip_time();
    // Two round trips and the 200 ms a lone packet's ACK may be held.
    assert_eq!(
        t.vsock.timers.tail_probe.poll_at(),
        Some(sent_at + rtt * 2 + Duration::from_millis(200))
    );
    assert_eq!(
        t.vsock.timers.retransmit.poll_at(),
        Some(sent_at + t.vsock.rtte.retransmission_timeout())
    );
}
