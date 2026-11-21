// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The dispatcher sends what the streams queued, in queue order. A datagram
// the host refuses as too large goes again at once without don't-fragment
// and its stream is told; one that fails for good is given up, its stream
// is told, and the rest still go.

use std::{
    io::IoSlice,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    task::{Context, Poll},
};

use parking_lot::Mutex;

use crate::{
    Fragmenting, PollSendToVectored, Transport, UtpSocket,
    ingress::SendFault,
    test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment},
};

const UNREACHABLE: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 3);
// The largest datagram the path takes with don't-fragment set.
const PATH: usize = 100;

// A send: where to, its length, and how it may be fragmented.
type Send = (SocketAddr, usize, Option<Fragmenting>);

// Every send it is asked for. With `budgeted`, each send spends the
// runtime's per-task budget, as a send on a tokio socket does.
#[derive(Clone, Default)]
struct PathTransport(Arc<Mutex<Vec<Send>>>, bool);

impl PathTransport {
    fn budgeted() -> Self {
        Self(Arc::default(), true)
    }

    fn send(
        &self,
        len: usize,
        target: SocketAddr,
        fragmenting: Option<Fragmenting>,
    ) -> Poll<std::io::Result<usize>> {
        self.0.lock().push((target, len, fragmenting));
        if target == UNREACHABLE {
            return Poll::Ready(Err(std::io::Error::other("unreachable")));
        }
        if len > PATH && fragmenting != Some(Fragmenting::Allowed) {
            return Poll::Ready(Err(crate::message_size_error()));
        }
        Poll::Ready(Ok(len))
    }
}

impl Transport for PathTransport {
    fn sends_from_one_task(&self) -> bool {
        true
    }

    fn recv_from<'a>(
        &'a self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> {
        std::future::pending()
    }

    async fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> std::io::Result<usize> {
        match self.send(buf.len(), target, None) {
            Poll::Ready(result) => result,
            Poll::Pending => unreachable!(),
        }
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        if self.1 {
            std::task::ready!(tokio::task::coop::poll_proceed(cx)).made_progress();
        }
        self.send(buf.len(), target, None)
    }

    fn poll_send_to_vectored_fragmenting(
        &self,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
        fragmenting: Fragmenting,
    ) -> Poll<std::io::Result<usize>> {
        let len = bufs.iter().map(|buf| buf.len()).sum();
        self.send(len, target, Some(fragmenting))
    }

    fn bind_addr(&self) -> SocketAddr {
        ADDR_1
    }
}

impl PollSendToVectored for PathTransport {
    fn poll_send_to_vectored(
        &self,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        let len = bufs.iter().map(|buf| buf.len()).sum();
        self.send(len, target, None)
    }
}

#[tokio::test]
async fn refused_and_failed_sends_are_reported_and_the_rest_still_go() {
    let transport = PathTransport::default();
    let (socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
        transport.clone(),
        MockUtpEnvironment::new(),
        Default::default(),
    )
    .unwrap();
    let opts = dispatcher.socket.opts.vsock_rx_bufsize;
    let (tx, mut fits) = crate::ingress::channel(opts);
    assert!(dispatcher.register_stream((ADDR_2, 5), tx).is_none());
    let (tx, mut fails) = crate::ingress::channel(opts);
    assert!(dispatcher.register_stream((UNREACHABLE, 6), tx).is_none());

    let outbox = socket.state.outbox.as_ref().unwrap();
    let departures = Arc::default();
    for (len, target, recv_id) in [
        (40, ADDR_2, 5),
        (PATH + 50, ADDR_2, 5),
        (40, UNREACHABLE, 6),
        (60, ADDR_2, 5),
    ] {
        let datagram = vec![0u8; len];
        assert!(
            outbox
                .try_queue(
                    &[IoSlice::new(&datagram)],
                    target,
                    recv_id,
                    Some(&departures)
                )
                .is_some()
        );
    }

    dispatcher.send_outgoing().await.unwrap();
    assert_eq!(
        *transport.0.lock(),
        [
            (ADDR_2, 40, None),
            (ADDR_2, PATH + 50, None),
            (ADDR_2, PATH + 50, Some(Fragmenting::Allowed)),
            (UNREACHABLE, 40, None),
            (ADDR_2, 60, None),
        ]
    );
    assert_eq!(outbox.sent(), 4);
    assert!(matches!(
        fits.take_send_fault(),
        Some(SendFault::TooLarge(payload)) if payload == PATH + 50 - crate::constants::UTP_HEADER as usize
    ));
    assert!(fits.take_send_fault().is_none());
    assert!(matches!(
        fails.take_send_fault(),
        Some(SendFault::Failed(_))
    ));
    assert_eq!(
        socket
            .stats()
            .send_errors
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

// A control packet the host refuses as too large goes again without
// don't-fragment like any other, but its size is no segment's: the stream
// is not told to cut smaller.
#[tokio::test]
async fn a_refused_control_packet_lowers_no_segment_size() {
    let transport = PathTransport::default();
    let (socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
        transport.clone(),
        MockUtpEnvironment::new(),
        Default::default(),
    )
    .unwrap();
    let (tx, mut rx) = crate::ingress::channel(dispatcher.socket.opts.vsock_rx_bufsize);
    assert!(dispatcher.register_stream((ADDR_2, 5), tx).is_none());
    let outbox = socket.state.outbox.as_ref().unwrap();
    let datagram = vec![0u8; PATH + 10];
    assert!(
        outbox
            .try_queue(&[IoSlice::new(&datagram)], ADDR_2, 5, None)
            .is_some()
    );

    dispatcher.send_outgoing().await.unwrap();
    assert_eq!(
        *transport.0.lock(),
        [
            (ADDR_2, PATH + 10, None),
            (ADDR_2, PATH + 10, Some(Fragmenting::Allowed)),
        ]
    );
    assert!(rx.take_send_fault().is_none());
}

// The runtime's per-task budget runs out partway through the queue: the
// dispatcher goes on at its next turn. That is no full socket: nothing is
// counted as one and the streams are not refused.
#[tokio::test]
async fn a_spent_task_budget_is_no_full_socket() {
    const QUEUED: u64 = 300;
    let transport = PathTransport::budgeted();
    let (socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
        transport.clone(),
        MockUtpEnvironment::new(),
        Default::default(),
    )
    .unwrap();
    let outbox = socket.state.outbox.as_ref().unwrap();
    for _ in 0..QUEUED {
        let datagram = vec![0u8; 40];
        assert!(
            outbox
                .try_queue(&[IoSlice::new(&datagram)], ADDR_2, 5, None)
                .is_some()
        );
    }

    let mut turns = 0;
    while outbox.sent() < QUEUED {
        dispatcher.send_outgoing().await.unwrap();
        // A stream may still queue: no refusal stands.
        let datagram = [0u8; 20];
        assert!(
            outbox
                .try_queue(&[IoSlice::new(&datagram)], ADDR_2, 5, None)
                .is_some(),
            "a stream refused after {turns} turns"
        );
        turns += 1;
        // The test task's next turn, with a fresh budget.
        tokio::task::yield_now().await;
    }
    assert!(turns > 1, "the budget never ran out");
    assert_eq!(
        socket
            .stats()
            .send_poll_pending
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}
