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
        let (wire, rx) = unbounded_channel();
        let transport = MockUtpTransport::new(ADDR_1, rx, MockInterface::new());
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
            buf: crate::socket::RecvBufs::new(),
            errors: 0,
        }
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
    assert_eq!(stats.stream_switch_marks, 0);
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
    // One read, two streams marked: one mark beyond the first.
    let stats = rig._socket.stats().snapshot();
    assert_eq!(stats.stream_switch_marks, 1);
    assert_eq!(stats.receive_drains, 1);

    // The next read marks only the stream that took packets in it.
    rig.arrive(ID_B, 3);
    assert!(!rig.turn().await);
    assert_eq!(a.shape(), "");
    assert_eq!(b.shape(), "P|");
    assert_eq!(rig._socket.stats().snapshot().stream_switch_marks, 1);
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
