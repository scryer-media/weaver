// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The receive loop reads the UDP socket until it would block, at most
// RECEIVE_BATCH datagrams between yields, and marks the end of each
// stream's packets where libtorrent sends its deferred ACK: when the socket
// is drained, or when a packet for another stream arrives
// (v2.0.15 utp_socket_manager.cpp:158-173, 255-271).

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
    buf: Vec<u8>,
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
            buf: vec![0u8; u16::MAX as usize],
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

#[tokio::test(start_paused = true)]
async fn a_packet_for_another_stream_marks_the_one_before() {
    let mut rig = Rig::new();
    let mut a = rig.stream(ID_A);
    let mut b = rig.stream(ID_B);
    rig.arrive(ID_A, 1);
    rig.arrive(ID_A, 2);
    rig.arrive(ID_B, 1);
    rig.arrive(ID_A, 3);

    assert!(!rig.turn().await);
    assert_eq!(a.shape(), "PP|P|");
    assert_eq!(b.shape(), "P|");
    assert_eq!(payloads(&mut a), [1, 2, 3]);
    assert_eq!(payloads(&mut b), [1]);
    // Two marks for a switch of stream, one for the drain.
    let stats = rig._socket.stats().snapshot();
    assert_eq!(stats.stream_switch_marks, 2);
    assert_eq!(stats.receive_drains, 1);
}

#[tokio::test(start_paused = true)]
async fn a_read_past_the_batch_limit_goes_on_after_a_yield_and_loses_nothing() {
    const ARRIVED: u16 = 120;
    let mut rig = Rig::new();
    let mut a = rig.stream(ID_A);
    for seq_nr in 1..=ARRIVED {
        rig.arrive(ID_A, seq_nr);
    }

    // The first two turns stop at the limit: the stream is woken for what
    // it holds, but the socket is not drained, so no mark yet.
    assert!(rig.turn().await);
    assert_eq!(a.shape(), "P".repeat(RECEIVE_BATCH));
    assert!(rig.turn().await);
    assert_eq!(a.shape(), "P".repeat(2 * RECEIVE_BATCH));
    assert_eq!(rig._socket.stats().snapshot().receive_drains, 0);
    // The last turn reads the rest and finds the socket empty.
    assert!(!rig.turn().await);
    assert_eq!(a.shape(), format!("{}|", "P".repeat(ARRIVED as usize)));
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
