// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    future::poll_fn,
    io::{self, IoSlice},
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use parking_lot::Mutex;
use tokio::{
    io::AsyncReadExt,
    sync::{Notify, Semaphore, mpsc},
};

use super::*;
use crate::{
    DefaultUtpEnvironment, PollSendToVectored,
    raw::{Type, UtpHeader},
    test_util::{ADDR_1, ADDR_2, transport::RememberingTransport},
};

struct Packet {
    target: SocketAddr,
    header: UtpHeader,
}

struct Controlled {
    incoming: Mutex<mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>>,
    sent: Mutex<Vec<Packet>>,
    changed: Notify,
    received: AtomicUsize,
    syn_completion: Semaphore,
    fail_syn: AtomicBool,
    rst_completion: Semaphore,
    active_syn: AtomicUsize,
    active_rst: AtomicUsize,
    peak_rst: AtomicUsize,
}

struct ControlledTransport(Arc<Controlled>);

type ControlledSocket = Arc<UtpSocket<ControlledTransport, DefaultUtpEnvironment>>;

struct Network {
    control: Arc<Controlled>,
    incoming: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
}

fn remote(port: u16) -> SocketAddr {
    (Ipv4Addr::LOCALHOST, port).into()
}

fn network() -> (ControlledSocket, Network) {
    network_with(SocketOpts::default())
}

fn network_with(opts: SocketOpts) -> (ControlledSocket, Network) {
    let (incoming, received) = mpsc::unbounded_channel();
    let control = Arc::new(Controlled {
        incoming: Mutex::new(received),
        sent: Mutex::new(Vec::new()),
        changed: Notify::new(),
        received: AtomicUsize::new(0),
        syn_completion: Semaphore::new(0),
        fail_syn: AtomicBool::new(false),
        rst_completion: Semaphore::new(0),
        active_syn: AtomicUsize::new(0),
        active_rst: AtomicUsize::new(0),
        peak_rst: AtomicUsize::new(0),
    });
    let socket = UtpSocket::new_with_opts(
        ControlledTransport(Arc::clone(&control)),
        DefaultUtpEnvironment {},
        opts,
    )
    .unwrap();
    (socket, Network { control, incoming })
}

impl Network {
    fn deliver(&self, from: SocketAddr, header: UtpHeader, payload: &[u8]) {
        let mut packet = vec![0; 20 + payload.len()];
        let headers = header.serialize(&mut packet).unwrap();
        packet[headers..].copy_from_slice(payload);
        self.incoming.send((from, packet)).unwrap();
    }

    async fn sent(&self, kind: Type, target: SocketAddr, count: usize) -> UtpHeader {
        loop {
            let changed = self.control.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let found = self
                .control
                .sent
                .lock()
                .iter()
                .filter(|packet| packet.header.htype == kind && packet.target == target)
                .nth(count - 1)
                .map(|packet| packet.header);
            if let Some(header) = found {
                return header;
            }
            changed.await;
        }
    }

    async fn received(&self, count: usize) {
        loop {
            let changed = self.control.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.control.received.load(Ordering::Relaxed) >= count {
                return;
            }
            changed.await;
        }
    }
}

impl Controlled {
    fn record(&self, buf: &[u8], target: SocketAddr) {
        let (header, _) = UtpHeader::deserialize(buf).unwrap();
        self.sent.lock().push(Packet { target, header });
        self.changed.notify_waiters();
    }
}

impl Transport for ControlledTransport {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (from, packet) = poll_fn(|cx| self.0.incoming.lock().poll_recv(cx))
            .await
            .unwrap();
        buf[..packet.len()].copy_from_slice(&packet);
        self.0.received.fetch_add(1, Ordering::Relaxed);
        self.0.changed.notify_waiters();
        Ok((packet.len(), from))
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        let (header, _) = UtpHeader::deserialize(buf).unwrap();
        self.0.record(buf, target);
        match header.htype {
            Type::Syn => {
                self.0.active_syn.fetch_add(1, Ordering::Relaxed);
                let _active = FnDropGuard::new(|| {
                    self.0.active_syn.fetch_sub(1, Ordering::Relaxed);
                    self.0.changed.notify_waiters();
                });
                self.0.syn_completion.acquire().await.unwrap().forget();
                if self.0.fail_syn.swap(false, Ordering::Relaxed) {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "deferred SYN send failure",
                    ));
                }
            }
            Type::Reset => {
                let active = self.0.active_rst.fetch_add(1, Ordering::Relaxed) + 1;
                self.0.peak_rst.fetch_max(active, Ordering::Relaxed);
                let _active = FnDropGuard::new(|| {
                    self.0.active_rst.fetch_sub(1, Ordering::Relaxed);
                    self.0.changed.notify_waiters();
                });
                self.0.rst_completion.acquire().await.unwrap().forget();
            }
            _ => {}
        }
        Ok(buf.len())
    }

    fn poll_send_to(
        &self,
        _: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.record(buf, target);
        Poll::Ready(Ok(buf.len()))
    }

    fn bind_addr(&self) -> SocketAddr {
        ADDR_1
    }
}

impl PollSendToVectored for ControlledTransport {
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let packet: Vec<_> = bufs.iter().flat_map(|buf| buf.iter().copied()).collect();
        self.poll_send_to(cx, &packet, target)
    }
}

fn bare() -> (
    Arc<UtpSocket<RememberingTransport, DefaultUtpEnvironment>>,
    Dispatcher<RememberingTransport, DefaultUtpEnvironment>,
) {
    UtpSocket::new_with_opts_and_dispatcher(
        RememberingTransport::new(ADDR_1),
        DefaultUtpEnvironment {},
        SocketOpts::default(),
    )
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn exhausted_parity_search_returns_then_reuses_a_released_key() {
    let (socket, mut dispatcher) = bare();
    let (tx, _rx) = crate::ingress::channel(NonZeroUsize::new(1).unwrap());
    let (flag, mut guard) = socket.state.retirements.registration();
    guard.disarm();
    for id in (0..=u16::MAX).step_by(2) {
        dispatcher.streams.insert(
            (ADDR_2, id),
            RegisteredStream {
                tx: tx.clone(),
                retired: Arc::clone(&flag),
            },
        );
    }
    dispatcher.next_connection_id = 65534.into();
    assert!(matches!(
        dispatcher.get_next_free_conn_id(ADDR_2),
        Err(Error::ConnectionIdsExhausted)
    ));
    dispatcher.streams.remove(&(ADDR_2, 32768));
    assert_eq!(
        dispatcher.get_next_free_conn_id(ADDR_2).unwrap(),
        32768.into()
    );
    assert_eq!(dispatcher.streams.len(), 32767);
    assert!(dispatcher.get_next_free_conn_id(remote(3)).is_ok());
    drop(dispatcher);
    socket.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cached_syn_future_keys_reserve_ids_and_exact_retransmissions_deduplicate() {
    let (socket, mut dispatcher) = bare();
    let syn = || {
        UtpMessage::new_test(
            UtpHeader {
                htype: Type::Syn,
                connection_id: 65535.into(),
                seq_nr: 19.into(),
                ..Default::default()
            },
            &[],
        )
    };
    dispatcher.on_syn(ADDR_2, syn()).await.unwrap();
    dispatcher.on_syn(ADDR_2, syn()).await.unwrap();
    assert_eq!(dispatcher.accept_queue.syns.len(), 1);
    dispatcher.next_connection_id = 0.into();
    assert_eq!(dispatcher.get_next_free_conn_id(ADDR_2).unwrap(), 2.into());
    assert_eq!(dispatcher.accept_queue.syns[0].header.seq_nr, 19.into());
    let conflict = UtpMessage::new_test(
        UtpHeader {
            htype: Type::Syn,
            connection_id: 65535.into(),
            seq_nr: 20.into(),
            ..Default::default()
        },
        &[],
    );
    dispatcher.on_syn(ADDR_2, conflict).await.unwrap();
    assert_eq!(dispatcher.accept_queue.syns.len(), 1);
    assert_eq!(dispatcher.accept_queue.syns[0].header.seq_nr, 19.into());
    drop(dispatcher);
    socket.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_old_retirement_cannot_remove_a_live_replacement_at_the_same_key() {
    let (socket, mut dispatcher) = bare();
    let (old_flag, old_guard) = socket.state.retirements.registration();
    let (old_tx, _old_rx) = crate::ingress::channel(NonZeroUsize::new(1).unwrap());
    dispatcher.streams.insert(
        (ADDR_2, 8),
        RegisteredStream {
            tx: old_tx,
            retired: old_flag,
        },
    );
    dispatcher.streams.remove(&(ADDR_2, 8));
    let (new_flag, _new_guard) = socket.state.retirements.registration();
    let (new_tx, mut new_rx) = crate::ingress::channel(NonZeroUsize::new(1).unwrap());
    dispatcher.streams.insert(
        (ADDR_2, 8),
        RegisteredStream {
            tx: new_tx,
            retired: new_flag,
        },
    );
    drop(old_guard);
    dispatcher.reap_retired();
    dispatcher
        .on_recv(
            ADDR_2,
            UtpMessage::new_test(
                UtpHeader {
                    htype: Type::Data,
                    connection_id: 8.into(),
                    ..Default::default()
                },
                b"x",
            ),
        )
        .await
        .unwrap();
    assert_eq!(new_rx.try_recv().unwrap().message.payload(), b"x");
    assert_eq!(dispatcher.streams.len(), 1);
    drop(dispatcher);
    socket.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn pending_connects_bound_admission_and_release_before_the_response_is_polled() {
    let (socket, network) = network();
    let mut attempts = Vec::new();
    for index in 0..32 {
        let mut attempt = Box::pin(socket.connect(remote(100 + index)));
        assert!(attempt.as_mut().now_or_never().is_none());
        attempts.push(attempt);
    }
    let mut next = Box::pin(socket.connect(remote(200)));
    assert!(next.as_mut().now_or_never().is_none());
    let first_syn = network.sent(Type::Syn, remote(100), 1).await;
    network.sent(Type::Syn, remote(131), 1).await;
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 32);
    assert!(
        !network
            .control
            .sent
            .lock()
            .iter()
            .any(|packet| packet.target == remote(200))
    );
    network.control.syn_completion.add_permits(32);
    network.deliver(
        remote(100),
        UtpHeader {
            htype: Type::State,
            connection_id: first_syn.connection_id,
            ack_nr: first_syn.seq_nr,
            seq_nr: 7.into(),
            wnd_size: 65535,
            ..Default::default()
        },
        &[],
    );
    let advance = async {
        let _ = next.as_mut().await;
    };
    let observed = async {
        network.sent(Type::Syn, remote(200), 1).await;
    };
    tokio::select! {
        _ = advance => panic!("the next connect has no ACK"),
        _ = observed => {}
    }
    // None of the first 32 response futures has been polled since enqueueing.
    drop(attempts);
    drop(next);
    socket.shutdown().await.unwrap();
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn pending_connects_scale_with_the_connections_limit() {
    let (socket, network) = network_with(SocketOpts {
        connections_limit: NonZeroUsize::new(40),
        ..Default::default()
    });
    let mut attempts = Vec::new();
    for index in 0..40 {
        let mut attempt = Box::pin(socket.connect(remote(100 + index)));
        assert!(attempt.as_mut().now_or_never().is_none());
        attempts.push(attempt);
    }
    network.sent(Type::Syn, remote(139), 1).await;
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 40);
    network.control.syn_completion.add_permits(40);
    drop(attempts);
    socket.shutdown().await.unwrap();
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn a_connect_waiting_for_admission_sends_its_syn_before_its_timeout_starts() {
    let (socket, network) = network();
    let mut attempts = Vec::new();
    for index in 0..32 {
        let mut attempt = Box::pin(socket.connect(remote(100 + index)));
        assert!(attempt.as_mut().now_or_never().is_none());
        attempts.push(attempt);
    }
    let mut next = Box::pin(socket.connect(remote(200)));
    assert!(next.as_mut().now_or_never().is_none());
    network.sent(Type::Syn, remote(131), 1).await;
    network.control.syn_completion.add_permits(33);
    // The admitted connects time out unanswered while the next one waits.
    for attempt in attempts {
        assert!(matches!(attempt.await, Err(Error::ConnectTimedOut)));
    }
    let advance = async {
        let _ = next.as_mut().await;
    };
    let observed = async {
        network.sent(Type::Syn, remote(200), 1).await;
    };
    tokio::select! {
        _ = advance => panic!("the queued connect ended without a SYN"),
        _ = observed => {}
    }
    drop(next);
    socket.shutdown().await.unwrap();
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn a_canceled_queued_request_cannot_emit_a_syn_before_the_next_request() {
    let (socket, network) = network();
    let mut canceled = Box::pin(socket.connect(remote(100)));
    assert!(canceled.as_mut().now_or_never().is_none());
    drop(canceled);
    let mut live = Box::pin(socket.connect(remote(101)));
    assert!(live.as_mut().now_or_never().is_none());
    network.sent(Type::Syn, remote(101), 1).await;
    assert!(
        !network
            .control
            .sent
            .lock()
            .iter()
            .any(|packet| packet.target == remote(100))
    );
    drop(live);
    socket.shutdown().await.unwrap();
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 0);
}

// One address takes as many connects as the socket admits, each under its
// own connection id; the next waits for a permit rather than being refused.
#[tokio::test(start_paused = true)]
async fn one_endpoint_takes_every_admitted_connect() {
    let (socket, network) = network();
    let mut attempts = Vec::new();
    for _ in 0..32 {
        let mut attempt = Box::pin(socket.connect(remote(100)));
        assert!(attempt.as_mut().now_or_never().is_none());
        attempts.push(attempt);
    }
    let mut next = Box::pin(socket.connect(remote(100)));
    assert!(next.as_mut().now_or_never().is_none());
    {
        let any_ended = futures::future::select_all(attempts.iter_mut().map(|a| a.as_mut()));
        tokio::select! {
            (result, index, _) = any_ended => {
                panic!("connect {index} ended before its SYN: {:?}", result.err())
            }
            _ = network.sent(Type::Syn, remote(100), 32) => {}
        }
    }
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 32);
    let syns: Vec<UtpHeader> = network
        .control
        .sent
        .lock()
        .iter()
        .filter(|packet| packet.header.htype == Type::Syn)
        .map(|packet| packet.header)
        .collect();
    assert_eq!(syns.len(), 32);
    let ids: std::collections::BTreeSet<u16> = syns.iter().map(|h| h.connection_id.0).collect();
    assert_eq!(ids.len(), 32, "each connect has its own connection id");

    // The remote answers the last SYN: that connect completes and frees the
    // permit the waiting one needs.
    network.control.syn_completion.add_permits(33);
    let last = syns[31];
    network.deliver(
        remote(100),
        UtpHeader {
            htype: Type::State,
            connection_id: last.connection_id,
            ack_nr: last.seq_nr,
            seq_nr: 7.into(),
            wnd_size: 65535,
            ..Default::default()
        },
        &[],
    );
    let answered = attempts.pop().unwrap().await.unwrap();
    let observed = async {
        network.sent(Type::Syn, remote(100), 33).await;
    };
    tokio::select! {
        r = next.as_mut() => panic!("the waiting connect ended without a SYN: {:?}", r.err()),
        _ = observed => {}
    }
    drop(answered);
    drop(attempts);
    drop(next);
    socket.shutdown().await.unwrap();
}

// A remote that will not take the connect answers the SYN with a FIN (as
// libtorrent does) or a RESET. That ends the connect with the refusal, not a
// timeout; a FIN acknowledging some other sequence number is ignored.
#[tokio::test(start_paused = true)]
async fn a_fin_or_reset_answering_the_syn_ends_the_connect() {
    let (socket, network) = network();
    network.control.syn_completion.add_permits(3);
    for (index, htype, matching, expected) in [
        (0u16, Type::Fin, true, "refused"),
        (1, Type::Reset, true, "reset"),
        (2, Type::Fin, false, "timeout"),
    ] {
        let target = remote(100 + index);
        let mut connect = Box::pin(socket.connect(target));
        assert!(connect.as_mut().now_or_never().is_none());
        let syn = network.sent(Type::Syn, target, 1).await;
        let ack_nr = if matching { syn.seq_nr } else { syn.seq_nr - 1 };
        network.deliver(
            target,
            UtpHeader {
                htype,
                connection_id: syn.connection_id,
                ack_nr,
                seq_nr: 7.into(),
                wnd_size: 65535,
                ..Default::default()
            },
            &[],
        );
        let got = match connect.await {
            Err(Error::ConnectRefused) => "refused",
            Err(Error::StResetReceived) => "reset",
            Err(Error::ConnectTimedOut) => "timeout",
            other => panic!("unexpected connect result: {:?}", other.err()),
        };
        assert_eq!(got, expected, "{htype:?} matching={matching}");
    }
    socket.shutdown().await.unwrap();
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 0);
}

// An accept dropped after the dispatcher handed it a SYN, as a select over
// accept and other work does, keeps that connection for the next accept,
// which answers the SYN.
#[tokio::test(start_paused = true)]
async fn a_dropped_accept_keeps_the_connection_it_was_handed() {
    let (socket, network) = network();
    let mut dropped = Box::pin(socket.accept());
    assert!(dropped.as_mut().now_or_never().is_none());
    network.deliver(
        ADDR_2,
        UtpHeader {
            htype: Type::Syn,
            connection_id: 50.into(),
            seq_nr: 1.into(),
            ..Default::default()
        },
        &[],
    );
    // The dispatcher takes packets in order: once it reads the second, it
    // has handed the SYN to the waiting accept.
    network.deliver(
        remote(200),
        UtpHeader {
            htype: Type::State,
            connection_id: 9.into(),
            ..Default::default()
        },
        &[],
    );
    network.received(2).await;
    drop(dropped);

    let stream = socket
        .accept()
        .now_or_never()
        .expect("the handed connection waits for the next accept")
        .unwrap();
    let syn_ack = network.sent(Type::State, ADDR_2, 1).await;
    assert_eq!(syn_ack.connection_id, 50.into());
    assert_eq!(syn_ack.ack_nr, 1.into());
    drop(stream);
    socket.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_early_ack_waits_for_syn_acceptance_and_cancellation_drops_the_send() {
    let (socket, network) = network();
    let mut connect = Box::pin(socket.connect(remote(100)));
    assert!(connect.as_mut().now_or_never().is_none());
    let syn = network.sent(Type::Syn, remote(100), 1).await;
    network.deliver(
        remote(100),
        UtpHeader {
            htype: Type::State,
            connection_id: syn.connection_id,
            ack_nr: syn.seq_nr,
            seq_nr: 7.into(),
            wnd_size: 65535,
            ..Default::default()
        },
        &[],
    );
    network.received(1).await;
    assert!(connect.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_millis(1)).await;
    network.control.syn_completion.add_permits(1);
    let _stream = connect.await.unwrap();
    let mut canceled = Box::pin(socket.connect(remote(101)));
    assert!(canceled.as_mut().now_or_never().is_none());
    network.sent(Type::Syn, remote(101), 1).await;
    drop(canceled);
    socket.shutdown().await.unwrap();
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn bounded_rst_outbox_counts_active_sends_and_keeps_the_first_reservations() {
    let (socket, mut dispatcher) = bare();
    for id in 0..96 {
        dispatcher
            .on_syn(
                ADDR_2,
                UtpMessage::new_test(
                    UtpHeader {
                        htype: Type::Syn,
                        connection_id: id.into(),
                        seq_nr: 1.into(),
                        ..Default::default()
                    },
                    &[],
                ),
            )
            .await
            .unwrap();
    }
    assert_eq!(dispatcher.accept_queue.syns.len(), 32);
    assert_eq!(dispatcher.control_sends.len(), 32);
    assert!(socket.stats().syn_flood_dropped.load(Ordering::Relaxed) > 0);
    assert_eq!(
        dispatcher
            .accept_queue
            .syns
            .front()
            .unwrap()
            .header
            .connection_id,
        0.into()
    );
    let transport = dispatcher.transport.as_ref().unwrap().clone();
    while !dispatcher.control_sends.is_empty() {
        let ready = poll_fn(|cx| {
            poll_control_sends(
                &mut dispatcher.connecting,
                &mut dispatcher.control_sends,
                cx,
            )
        })
        .await;
        dispatcher.finish_send(ready).unwrap();
    }
    let mut ids = Vec::new();
    for (target, packet) in transport.take_sent() {
        let (header, len) = UtpHeader::deserialize(&packet).unwrap();
        assert_eq!(target, ADDR_2);
        assert_eq!(header.htype, Type::Reset);
        assert_eq!(header.ack_nr, 1.into());
        assert_eq!(len, packet.len(), "RST replies have no payload");
        ids.push(header.connection_id.0);
    }
    ids.sort_unstable();
    assert_eq!(ids, (32..64).collect::<Vec<u16>>());
    drop(dispatcher);
    socket.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn blocked_syn_and_rst_sends_do_not_stop_another_flow_receiving_data() {
    let (socket, network) = network();
    let accepted = socket.accept();
    tokio::pin!(accepted);
    network.deliver(
        ADDR_2,
        UtpHeader {
            htype: Type::Syn,
            connection_id: 50.into(),
            seq_nr: 1.into(),
            ..Default::default()
        },
        &[],
    );
    let mut stream = accepted.await.unwrap();
    let syn_ack = network.sent(Type::State, ADDR_2, 1).await;
    let mut blocked = Box::pin(socket.connect(remote(100)));
    assert!(blocked.as_mut().now_or_never().is_none());
    network.sent(Type::Syn, remote(100), 1).await;
    for id in 1000..1100 {
        network.deliver(
            remote(101),
            UtpHeader {
                htype: Type::Syn,
                connection_id: id.into(),
                seq_nr: 1.into(),
                ..Default::default()
            },
            &[],
        );
    }
    network.sent(Type::Reset, remote(101), 1).await;
    network.deliver(
        ADDR_2,
        UtpHeader {
            htype: Type::Data,
            connection_id: 51.into(),
            seq_nr: 2.into(),
            ack_nr: syn_ack.seq_nr - 1,
            wnd_size: 65535,
            ..Default::default()
        },
        b"live",
    );
    let mut bytes = [0; 4];
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"live");
    assert!(network.control.active_syn.load(Ordering::Relaxed) > 0);
    assert!(network.control.active_rst.load(Ordering::Relaxed) > 0);
    drop(blocked);
    socket.shutdown().await.unwrap();
    assert_eq!(network.control.active_syn.load(Ordering::Relaxed), 0);
    assert_eq!(network.control.active_rst.load(Ordering::Relaxed), 0);
    assert!(network.control.peak_rst.load(Ordering::Relaxed) <= 32);
}

#[tokio::test(start_paused = true)]
async fn an_early_ack_cannot_promote_a_syn_that_later_fails_acceptance() {
    let (socket, network) = network();
    let mut connect = Box::pin(socket.connect(remote(100)));
    assert!(connect.as_mut().now_or_never().is_none());
    let syn = network.sent(Type::Syn, remote(100), 1).await;
    network.deliver(
        remote(100),
        UtpHeader {
            htype: Type::State,
            connection_id: syn.connection_id,
            ack_nr: syn.seq_nr,
            seq_nr: 7.into(),
            wnd_size: 65535,
            ..Default::default()
        },
        &[],
    );
    network.received(1).await;
    assert!(connect.as_mut().now_or_never().is_none());
    network.control.fail_syn.store(true, Ordering::Relaxed);
    network.control.syn_completion.add_permits(1);
    assert!(
        matches!(connect.await, Err(Error::ErrorSendingSyn(error)) if error.kind() == io::ErrorKind::BrokenPipe)
    );
    assert_eq!(socket.state.connect_admission.available_permits(), 32);
    assert_eq!(
        socket.stats().live_virtual_sockets.load(Ordering::Relaxed),
        0
    );
    let mut replacement = Box::pin(socket.connect(remote(101)));
    assert!(replacement.as_mut().now_or_never().is_none());
    network.sent(Type::Syn, remote(101), 1).await;
    drop(replacement);
    socket.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn canceling_an_early_ack_attempt_retires_its_send_without_stopping_the_socket() {
    let (socket, network) = network();
    let mut canceled = Box::pin(socket.connect(remote(100)));
    assert!(canceled.as_mut().now_or_never().is_none());
    let old_syn = network.sent(Type::Syn, remote(100), 1).await;
    let old_ack = UtpHeader {
        htype: Type::State,
        connection_id: old_syn.connection_id,
        ack_nr: old_syn.seq_nr,
        seq_nr: 7.into(),
        wnd_size: 65535,
        ..Default::default()
    };
    network.deliver(remote(100), old_ack, &[]);
    network.received(1).await;
    assert!(canceled.as_mut().now_or_never().is_none());
    drop(canceled);
    loop {
        let changed = network.control.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if network.control.active_syn.load(Ordering::Relaxed) == 0 {
            break;
        }
        changed.await;
    }
    assert_eq!(socket.state.connect_admission.available_permits(), 32);
    assert_eq!(
        socket.stats().live_virtual_sockets.load(Ordering::Relaxed),
        0
    );
    assert!(!socket.state.cancellation_token.is_cancelled());
    network.control.syn_completion.add_permits(1);
    let mut replacement = Box::pin(socket.connect(remote(100)));
    assert!(replacement.as_mut().now_or_never().is_none());
    let new_syn = network.sent(Type::Syn, remote(100), 2).await;
    assert_ne!(new_syn.connection_id, old_syn.connection_id);
    network.deliver(remote(100), old_ack, &[]);
    network.received(2).await;
    assert!(replacement.as_mut().now_or_never().is_none());
    network.deliver(
        remote(100),
        UtpHeader {
            htype: Type::State,
            connection_id: new_syn.connection_id,
            ack_nr: new_syn.seq_nr,
            seq_nr: 8.into(),
            wnd_size: 65535,
            ..Default::default()
        },
        &[],
    );
    let _stream = replacement.await.unwrap();
    socket.shutdown().await.unwrap();
}
