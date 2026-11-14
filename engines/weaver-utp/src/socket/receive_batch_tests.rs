// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The receive loop reads the UDP socket until it would block, at most
// RECEIVE_BATCH datagrams between yields. When a read ends, each stream
// that took packets in it is marked once, which is where it sends the ACK
// it owes: libtorrent's deferred ACK at the drain
// (v2.0.15 utp_socket_manager.cpp:255-271), kept for every stream of the
// read and not only the last.

use std::net::SocketAddr;

use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use super::{Dispatcher, RECEIVE_BATCH};
use crate::Transport;
use crate::{
    UtpSocket,
    ingress::Receiver,
    raw::{Type, UtpHeader},
    test_util::{
        ADDR_1, ADDR_2,
        env::MockUtpEnvironment,
        transport::{MockInterface, MockUtpTransport},
    },
};

const ID_A: u16 = 10;
const ADDR_3: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 3);
const ID_B: u16 = 20;

struct Rig {
    dispatcher: Dispatcher<MockUtpTransport, MockUtpEnvironment>,
    _socket: std::sync::Arc<UtpSocket<MockUtpTransport, MockUtpEnvironment>>,
    wire: UnboundedSender<(SocketAddr, Vec<u8>)>,
    buf: crate::socket::RecvBufs,
    errors: usize,
}

impl Rig {
    fn new() -> Self {
        Self::taking_many(1, MockInterface::new())
    }

    // A socket whose transport receives up to `most` datagrams in a call,
    // and sends as many to several addresses in one, from its I/O task.
    fn taking_many(most: usize, interface: std::sync::Arc<MockInterface>) -> Self {
        let (wire, rx) = unbounded_channel();
        let mut transport = MockUtpTransport::new(ADDR_1, rx, interface);
        if most > 1 {
            transport = transport.taking_many(most).sending_from_one_task();
        }
        let (socket, dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
            transport,
            MockUtpEnvironment::new(),
            Default::default(),
        )
        .unwrap();
        Self {
            dispatcher,
            _socket: socket,
            wire,
            buf: crate::socket::RecvBufs::receiving(most),
            errors: 0,
        }
    }

    // Queues one DATA datagram with `payload` bytes of `fill`.
    fn arrive_long(&self, id: u16, seq_nr: u16, payload: usize, fill: u8) {
        let header = UtpHeader {
            htype: Type::Data,
            connection_id: id.into(),
            seq_nr: seq_nr.into(),
            ..Default::default()
        };
        let mut datagram = vec![0u8; 64];
        let len = header.serialize(&mut datagram).unwrap();
        datagram.truncate(len);
        datagram.resize(len + payload, fill);
        self.wire.send((ADDR_2, datagram)).unwrap();
    }

    fn stream(&mut self, id: u16) -> Receiver {
        let (tx, rx) = crate::ingress::channel(self.dispatcher.socket.opts.vsock_rx_bufsize);
        assert!(self.dispatcher.register_stream((ADDR_2, id), tx).is_none());
        rx
    }

    // Queues one DATA datagram on the wire, as the kernel holds it until
    // read.
    fn arrive(&self, id: u16, seq_nr: u16) {
        let header = UtpHeader {
            htype: Type::Data,
            connection_id: id.into(),
            seq_nr: seq_nr.into(),
            ..Default::default()
        };
        let mut datagram = vec![0u8; 64];
        let len = header.serialize(&mut datagram).unwrap();
        datagram.truncate(len);
        datagram.extend_from_slice(&seq_nr.to_be_bytes());
        self.wire.send((ADDR_2, datagram)).unwrap();
    }

    // A datagram that is not uTP: too short for a header.
    fn arrive_garbage(&self) {
        self.wire.send((ADDR_2, vec![0x41; 3])).unwrap();
    }

    // One turn of the receive loop: true when the read stopped at the limit.
    async fn turn(&mut self) -> bool {
        self.dispatcher
            .run_once(&mut self.buf, &mut self.errors)
            .await
            .unwrap()
    }
}

fn payloads(rx: &mut Receiver) -> Vec<u16> {
    let mut seen = Vec::new();
    while let Some(event) = rx.try_recv() {
        seen.push(u16::from_be_bytes(
            event.message.payload().try_into().unwrap(),
        ));
    }
    seen
}

#[tokio::test(start_paused = true)]
async fn one_read_of_one_stream_is_marked_once_at_the_drain() {
    let mut rig = Rig::new();
    let mut a = rig.stream(ID_A);
    for seq_nr in 1..=3 {
        rig.arrive(ID_A, seq_nr);
    }
    // A datagram that is not uTP neither ends the stream's run nor drops
    // the packets around it (utp_socket_manager.cpp:143-149).
    rig.arrive_garbage();
    for seq_nr in 4..=5 {
        rig.arrive(ID_A, seq_nr);
    }

    assert!(!rig.turn().await, "the socket was read empty");
    assert_eq!(a.shape(), "PPPPP|");
    assert_eq!(payloads(&mut a), [1, 2, 3, 4, 5]);
    let stats = rig._socket.stats().snapshot();
    assert_eq!(stats.streams_marked, 1);
    assert_eq!(stats.receive_drains, 1);
}

// Packets of two streams arrive mixed: each stream gets one mark, at the
// end of the read, so each owes one ACK for the read and not one for every
// turn from one stream to the other.
#[tokio::test(start_paused = true)]
async fn streams_whose_packets_arrive_mixed_are_each_marked_once_a_read() {
    let mut rig = Rig::new();
    let mut a = rig.stream(ID_A);
    let mut b = rig.stream(ID_B);
    rig.arrive(ID_A, 1);
    rig.arrive(ID_A, 2);
    rig.arrive(ID_B, 1);
    rig.arrive(ID_A, 3);
    rig.arrive(ID_B, 2);
    rig.arrive(ID_A, 4);

    assert!(!rig.turn().await);
    assert_eq!(a.shape(), "PPPP|");
    assert_eq!(b.shape(), "PP|");
    assert_eq!(payloads(&mut a), [1, 2, 3, 4]);
    assert_eq!(payloads(&mut b), [1, 2]);
    // One read, two streams marked.
    let stats = rig._socket.stats().snapshot();
    assert_eq!(stats.streams_marked, 2);
    assert_eq!(stats.receive_drains, 1);

    // The next read marks only the stream that took packets in it.
    rig.arrive(ID_B, 3);
    assert!(!rig.turn().await);
    assert_eq!(a.shape(), "");
    assert_eq!(b.shape(), "P|");
    assert_eq!(rig._socket.stats().snapshot().streams_marked, 3);
}

// A stream that ends while a read holds its packets leaves nothing behind:
// the read ends with no queue held, and the other stream's mark is made.
#[tokio::test(start_paused = true)]
async fn a_stream_dropped_mid_read_does_not_hold_up_the_others_mark() {
    let mut rig = Rig::new();
    let a = rig.stream(ID_A);
    let b = rig.stream(ID_B);
    rig.arrive(ID_A, 1);
    rig.arrive(ID_B, 1);
    drop(a);

    assert!(!rig.turn().await);
    assert_eq!(b.shape(), "P|");
    assert!(rig.dispatcher.unflushed.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_read_past_the_batch_limit_goes_on_after_a_yield_and_loses_nothing() {
    const ARRIVED: u16 = 120;
    let mut rig = Rig::new();
    let mut a = rig.stream(ID_A);
    for seq_nr in 1..=ARRIVED {
        rig.arrive(ID_A, seq_nr);
    }

    // The first two turns stop at the limit: the stream is marked for what
    // it holds, so a socket that never empties still has its ACKs sent,
    // one for each read.
    let batch = format!("{}|", "P".repeat(RECEIVE_BATCH));
    assert!(rig.turn().await);
    assert_eq!(a.shape(), batch);
    assert!(rig.turn().await);
    assert_eq!(a.shape(), batch.repeat(2));
    assert_eq!(rig._socket.stats().snapshot().receive_drains, 0);
    assert_eq!(rig._socket.stats().snapshot().receive_cuts, 2);
    // The last turn reads the rest and finds the socket empty.
    assert!(!rig.turn().await);
    assert_eq!(
        a.shape(),
        format!(
            "{}{}|",
            batch.repeat(2),
            "P".repeat(ARRIVED as usize - 2 * RECEIVE_BATCH)
        )
    );
    assert_eq!(payloads(&mut a), (1..=ARRIVED).collect::<Vec<_>>());
    assert_eq!(rig._socket.stats().snapshot().receive_drains, 1);
}

// A socket that never reads empty: every turn's read stops at the limit.
// A connect request and an accept still get their turn within a bounded
// number of reads, counted in turns, not time.
#[tokio::test(start_paused = true)]
async fn a_socket_that_never_empties_still_admits_connects_and_accepts() {
    let mut rig = Rig::new();
    let _a = rig.stream(ID_A);
    let mut seq_nr = 0u16;
    let mut refill = |rig: &Rig| {
        for _ in 0..=RECEIVE_BATCH {
            seq_nr = seq_nr.wrapping_add(1);
            rig.arrive(ID_A, seq_nr);
        }
    };
    refill(&rig);
    assert!(rig.turn().await, "the read stopped at the limit");

    let socket = rig._socket.clone();
    let connect = tokio::spawn({
        let socket = socket.clone();
        async move { socket.connect(ADDR_3).await }
    });
    let accept = tokio::spawn(async move { socket.accept().await });
    while rig.dispatcher.connect_rx.is_empty() || rig.dispatcher.accept_queue.rx.is_empty() {
        tokio::task::yield_now().await;
    }

    let mut turns = 0;
    while rig.dispatcher.connecting.is_empty()
        || rig
            .dispatcher
            .accept_queue
            .next_available_acceptor
            .is_none()
    {
        assert!(turns < 4, "no connect or accept after {turns} cut reads");
        refill(&rig);
        rig.turn().await;
        turns += 1;
    }
    assert!(rig.dispatcher.connecting.contains_key(&ADDR_3));
    connect.abort();
    accept.abort();
}

// A data packet's payload stays in the pooled buffer it was received into,
// and the next datagram gets another: no copy and no allocation. One longer
// than that buffer is read on into the spill buffer and joined once.
#[test]
fn a_payload_stays_in_the_buffer_it_was_received_into() {
    let mut bufs = crate::socket::RecvBufs::new();
    let header = UtpHeader {
        htype: Type::Data,
        connection_id: ID_A.into(),
        ..Default::default()
    };
    let hlen = header.serialize(&mut bufs.head).unwrap();
    bufs.head[hlen..hlen + 100].fill(7);
    let at = bufs.head.as_ptr();
    let message = bufs
        .message(hlen + 100, tokio::time::Instant::now())
        .unwrap();
    assert_eq!(message.data.as_ptr(), at.wrapping_add(hlen));
    assert_eq!(&*message.data, &[7; 100][..]);
    assert_ne!(
        bufs.head.as_ptr(),
        at,
        "the next datagram gets another buffer"
    );

    // 3,000 bytes: the pooled buffer's 2,048, then the spill buffer.
    let long = 3000;
    let hlen = header.serialize(&mut bufs.head).unwrap();
    bufs.head[hlen..].fill(8);
    let tail = long - bufs.head.len();
    bufs.spill[..tail].fill(9);
    let message = bufs.message(long, tokio::time::Instant::now()).unwrap();
    assert_eq!(message.data.len(), long - hlen);
    assert!(
        message.data[..crate::rx_buf::RX_BUF_LEN - hlen]
            .iter()
            .all(|b| *b == 8)
    );
    assert!(
        message.data[crate::rx_buf::RX_BUF_LEN - hlen..]
            .iter()
            .all(|b| *b == 9)
    );
}

// A transport that returns several datagrams a call: a read takes them in
// calls of many, in the order they came, still stops at the batch limit,
// and marks each stream once. A datagram that is not uTP and one longer
// than a pooled buffer are each handled as when read alone.
#[tokio::test(start_paused = true)]
async fn a_read_takes_several_datagrams_a_call() {
    const MOST: usize = 16;
    let interface = MockInterface::new();
    let mut rig = Rig::taking_many(MOST, interface.clone());
    let mut a = rig.stream(ID_A);
    let mut b = rig.stream(ID_B);
    rig.arrive(ID_A, 1);
    rig.arrive(ID_B, 1);
    rig.arrive_garbage();
    rig.arrive(ID_A, 2);
    rig.arrive_long(ID_B, 2, 3000, 9);
    rig.arrive(ID_A, 3);

    assert!(!rig.turn().await, "the socket was read empty");
    assert_eq!(*interface.many_receives.lock(), [6]);
    assert_eq!(a.shape(), "PPP|");
    assert_eq!(b.shape(), "PP|");
    assert_eq!(payloads(&mut a), [1, 2, 3]);
    let first = b.try_recv().unwrap();
    assert_eq!(first.message.payload(), 1u16.to_be_bytes());
    let long = b.try_recv().unwrap();
    assert_eq!(long.message.payload().len(), 3000);
    assert!(long.message.payload().iter().all(|byte| *byte == 9));
    let stats = rig._socket.stats().snapshot();
    assert_eq!(stats.receive_calls, 1);
    assert_eq!(stats.receive_call_datagrams, 6);
    assert_eq!(stats.invalid_packets, 1);
    assert_eq!(stats.streams_marked, 2);
    assert_eq!(stats.receive_drains, 1);

    // More than a batch waits: the read stops at the limit, in calls that
    // add up to it, and goes on at the next turn.
    interface.many_receives.lock().clear();
    let arrived = RECEIVE_BATCH as u16 + 5;
    for seq_nr in 4..4 + arrived {
        rig.arrive(ID_A, seq_nr);
    }
    assert!(rig.turn().await, "the read stopped at the limit");
    assert_eq!(*interface.many_receives.lock(), [16, 16, 16, 2]);
    assert_eq!(a.shape(), format!("{}|", "P".repeat(RECEIVE_BATCH)));
    assert!(!rig.turn().await);
    assert_eq!(*interface.many_receives.lock(), [16, 16, 16, 2, 5]);
    assert_eq!(payloads(&mut a), (4..4 + arrived).collect::<Vec<_>>());
    let stats = rig._socket.stats().snapshot();
    assert_eq!(stats.receive_calls, 6);
    assert_eq!(stats.receive_call_datagrams, 6 + arrived as u64);
    assert_eq!(stats.receive_cuts, 1);
}

// A transport that receives one datagram a call counts one a call.
#[tokio::test(start_paused = true)]
async fn a_read_of_one_datagram_a_call_is_counted_a_call_each() {
    let mut rig = Rig::new();
    let _a = rig.stream(ID_A);
    for seq_nr in 1..=3 {
        rig.arrive(ID_A, seq_nr);
    }
    assert!(!rig.turn().await);
    let stats = rig._socket.stats().snapshot();
    assert_eq!(stats.receive_calls, 3);
    assert_eq!(stats.receive_call_datagrams, 3);
}

// Datagrams the streams queued for several addresses, as the ACKs of the
// streams one read woke, leave in one call, each stamped and whole. One
// the transport cannot send goes alone, fails for itself, and the rest
// still leave together.
#[tokio::test(start_paused = true)]
async fn datagrams_queued_for_several_addresses_leave_in_one_call() {
    let interface = MockInterface::new();
    let mut rig = Rig::taking_many(8, interface.clone());
    let peers: Vec<_> = [ADDR_2, ADDR_3]
        .into_iter()
        .map(|addr| (addr, interface.transport(addr)))
        .collect();
    let datagram = |fill: u8| {
        let header = UtpHeader {
            htype: Type::State,
            connection_id: ID_A.into(),
            ..Default::default()
        };
        let mut datagram = vec![fill; 20];
        header.serialize(&mut datagram).unwrap();
        datagram.push(fill);
        datagram
    };
    let queue = |rig: &Rig, addr, fill| {
        let datagram = datagram(fill);
        rig.dispatcher
            .socket
            .outbox
            .as_ref()
            .unwrap()
            .try_queue(&[std::io::IoSlice::new(&datagram)], addr, ID_A, None)
            .unwrap();
    };
    queue(&rig, ADDR_2, 1);
    queue(&rig, ADDR_3, 2);
    rig.dispatcher.send_outgoing().await.unwrap();
    assert_eq!(*interface.many_sends.lock(), [2]);
    for ((_, peer), fill) in peers.iter().zip([1u8, 2]) {
        let mut buf = [0u8; 64];
        let (len, from) = peer.recv_from(&mut buf).await.unwrap();
        assert_eq!(from, ADDR_1);
        let sent = datagram(fill);
        assert_eq!(len, sent.len());
        assert_eq!(buf[..4], sent[..4]);
        assert_eq!(buf[8..len], sent[8..]);
    }
    let stats = rig._socket.stats().snapshot();
    assert_eq!(stats.send_batches, 1);
    assert_eq!(stats.send_batch_datagrams, 2);

    // No socket is bound at the first one's address.
    let nowhere = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 9);
    queue(&rig, nowhere, 3);
    queue(&rig, ADDR_2, 4);
    queue(&rig, ADDR_3, 5);
    rig.dispatcher.send_outgoing().await.unwrap();
    assert_eq!(*interface.many_sends.lock(), [2, 3, 2]);
    let stats = rig._socket.stats().snapshot();
    assert_eq!(
        rig._socket
            .stats()
            .send_errors
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(stats.send_batches, 2);
    assert_eq!(stats.send_batch_datagrams, 4);
    for ((_, peer), fill) in peers.iter().zip([4u8, 5]) {
        let mut buf = [0u8; 64];
        let (len, _) = peer.recv_from(&mut buf).await.unwrap();
        assert_eq!(buf[len - 1], fill);
    }
}
