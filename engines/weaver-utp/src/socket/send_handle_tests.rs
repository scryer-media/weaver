// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io::{self, IoSlice},
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    time::Instant,
};

use crate::{
    DatagramSend, Error, PollSendToVectored, SocketOpts, Transport, UtpSocket, ingress,
    message::UtpMessage,
    raw::{Type, UtpHeader},
    stream_dispatch::{StreamArgs, UtpStreamStarter},
    test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment},
    traits::UtpEnvironment,
};

#[derive(Debug)]
enum Event {
    Attempt {
        handle: Option<usize>,
        poll: usize,
        slices: usize,
        accepted: bool,
    },
    Sent {
        handle: Option<usize>,
        message: Box<UtpMessage>,
    },
}

struct Control {
    enabled: bool,
    writable: AtomicBool,
    factories: AtomicUsize,
    created: AtomicUsize,
    dropped: Mutex<Vec<usize>>,
    waiter: Mutex<Option<(Option<usize>, Waker)>>,
    scalar_fallback: AtomicUsize,
    vectored_fallback: AtomicUsize,
    shutdowns: AtomicUsize,
    events: UnboundedSender<Event>,
}

impl Control {
    fn wake(&self, writable: bool) {
        self.writable.store(writable, Ordering::Relaxed);
        let waker = self.waiter.lock().as_ref().map(|(_, waker)| waker.clone());
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn poll(
        &self,
        handle: Option<usize>,
        poll: usize,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        assert_eq!(target, ADDR_2);
        let accepted = self.writable.load(Ordering::Relaxed);
        self.events
            .send(Event::Attempt {
                handle,
                poll,
                slices: bufs.len(),
                accepted,
            })
            .unwrap();
        if !accepted {
            *self.waiter.lock() = Some((handle, cx.waker().clone()));
            return Poll::Pending;
        }
        self.waiter.lock().take();
        let bytes: Vec<_> = bufs.iter().flat_map(|buf| buf.iter().copied()).collect();
        let message = Box::new(UtpMessage::deserialize(&bytes, Instant::now()).unwrap());
        self.events.send(Event::Sent { handle, message }).unwrap();
        Poll::Ready(Ok(bytes.len()))
    }
}

struct ProbeTransport(Arc<Control>);
struct ProbeSender {
    control: Arc<Control>,
    id: usize,
    polls: usize,
}

impl DatagramSend for ProbeSender {
    fn poll_send_to_vectored(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.polls += 1;
        self.control
            .poll(Some(self.id), self.polls, cx, bufs, target)
    }
}

impl Drop for ProbeSender {
    fn drop(&mut self) {
        self.control.dropped.lock().push(self.id);
        let mut waiter = self.control.waiter.lock();
        if waiter.as_ref().is_some_and(|(id, _)| *id == Some(self.id)) {
            waiter.take();
        }
    }
}

impl Transport for ProbeTransport {
    fn sender(&self) -> Option<Box<dyn DatagramSend>> {
        self.0.factories.fetch_add(1, Ordering::Relaxed);
        if !self.0.enabled {
            return None;
        }
        let id = self.0.created.fetch_add(1, Ordering::Relaxed);
        Some(Box::new(ProbeSender {
            control: self.0.clone(),
            id,
            polls: 0,
        }))
    }

    async fn shutdown(&self) -> io::Result<()> {
        self.0.shutdowns.fetch_add(1, Ordering::Relaxed);
        if self.0.created.load(Ordering::Relaxed) != self.0.dropped.lock().len() {
            return Err(io::Error::other("sender outlived its stream task"));
        }
        Ok(())
    }

    async fn recv_from(&self, _buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        std::future::pending().await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_send_to(cx, buf, target)).await
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.scalar_fallback.fetch_add(1, Ordering::Relaxed);
        self.0.poll(None, 0, cx, &[IoSlice::new(buf)], target)
    }

    fn bind_addr(&self) -> SocketAddr {
        ADDR_1
    }
}

impl PollSendToVectored for ProbeTransport {
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.vectored_fallback.fetch_add(1, Ordering::Relaxed);
        self.0.poll(None, 0, cx, bufs, target)
    }
}

type Socket = Arc<UtpSocket<ProbeTransport, MockUtpEnvironment>>;

fn fixture(enabled: bool, writable: bool) -> (Socket, Arc<Control>, UnboundedReceiver<Event>) {
    let (events, receiver) = unbounded_channel();
    let control = Arc::new(Control {
        enabled,
        writable: AtomicBool::new(writable),
        factories: AtomicUsize::new(0),
        created: AtomicUsize::new(0),
        dropped: Mutex::new(Vec::new()),
        waiter: Mutex::new(None),
        scalar_fallback: AtomicUsize::new(0),
        vectored_fallback: AtomicUsize::new(0),
        shutdowns: AtomicUsize::new(0),
        events,
    });
    let socket = UtpSocket::new_with_opts(
        ProbeTransport(control.clone()),
        MockUtpEnvironment::new(),
        SocketOpts::default(),
    )
    .unwrap();
    (socket, control, receiver)
}

fn starter(
    socket: &Socket,
) -> (
    ingress::Sender,
    UtpStreamStarter<ProbeTransport, MockUtpEnvironment>,
) {
    let (sender, receiver) = ingress::channel(NonZeroUsize::new(64).unwrap());
    let now = socket.state.env.now();
    let header = UtpHeader {
        htype: Type::State,
        seq_nr: 1.into(),
        ack_nr: 100.into(),
        wnd_size: 64,
        ..Default::default()
    };
    (
        sender,
        UtpStreamStarter::new(
            socket,
            ADDR_2,
            receiver,
            StreamArgs::new_outgoing(&header, now, now),
        ),
    )
}

async fn attempt(events: &mut UnboundedReceiver<Event>) -> (Option<usize>, usize, usize, bool) {
    match events.recv().await.unwrap() {
        Event::Attempt {
            handle,
            poll,
            slices,
            accepted,
        } => (handle, poll, slices, accepted),
        event => panic!("expected send attempt, got {event:?}"),
    }
}

async fn sent(
    events: &mut UnboundedReceiver<Event>,
    owner: Option<usize>,
    htype: Type,
) -> UtpMessage {
    match events.recv().await.unwrap() {
        Event::Sent { handle, message } => {
            assert_eq!(handle, owner);
            assert_eq!(message.header.htype, htype);
            *message
        }
        event => panic!("expected sent packet, got {event:?}"),
    }
}

fn incoming(sender: &ingress::Sender, socket: &Socket) {
    let mut message = UtpMessage::new_test(
        UtpHeader {
            htype: Type::Data,
            connection_id: 0.into(),
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 64,
            ..Default::default()
        },
        b"x",
    );
    message.received = socket.state.env.now();
    sender.send(message).unwrap();
}

#[tokio::test(start_paused = true)]
async fn one_owned_sender_persists_across_pending_data_and_scalar_control_polls() {
    let (socket, control, mut events) = fixture(true, false);
    let (incoming_sender, starter) = starter(&socket);
    assert_eq!(control.factories.load(Ordering::Relaxed), 0);
    let mut stream = starter.start().unwrap();
    stream.write_all(b"payload").await.unwrap();
    assert_eq!(attempt(&mut events).await, (Some(0), 1, 3, false));
    control.wake(false);
    assert_eq!(attempt(&mut events).await, (Some(0), 2, 3, false));
    assert_eq!(control.factories.load(Ordering::Relaxed), 1);
    control.wake(true);
    assert_eq!(attempt(&mut events).await, (Some(0), 3, 3, true));
    assert_eq!(
        sent(&mut events, Some(0), Type::Data).await.payload(),
        b"payload"
    );

    incoming(&incoming_sender, &socket);
    assert_eq!(attempt(&mut events).await, (Some(0), 4, 1, true));
    let ack = sent(&mut events, Some(0), Type::State).await;
    assert_eq!(ack.header.ack_nr, 1.into());
    assert_eq!(control.scalar_fallback.load(Ordering::Relaxed), 0);
    assert_eq!(control.vectored_fallback.load(Ordering::Relaxed), 0);
    socket.shutdown().await.unwrap();
    assert_eq!(*control.dropped.lock(), [0]);
    assert_eq!(control.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn transports_without_a_sender_keep_scalar_and_vectored_fallback() {
    let (socket, control, mut events) = fixture(false, true);
    let (incoming_sender, starter) = starter(&socket);
    let mut stream = starter.start().unwrap();
    stream.write_all(b"payload").await.unwrap();
    assert_eq!(attempt(&mut events).await, (None, 0, 3, true));
    assert_eq!(
        sent(&mut events, None, Type::Data).await.payload(),
        b"payload"
    );
    incoming(&incoming_sender, &socket);
    assert_eq!(attempt(&mut events).await, (None, 0, 1, true));
    sent(&mut events, None, Type::State).await;
    assert_eq!(control.scalar_fallback.load(Ordering::Relaxed), 1);
    assert_eq!(control.vectored_fallback.load(Ordering::Relaxed), 1);
    assert_eq!(control.created.load(Ordering::Relaxed), 0);
    socket.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_drops_a_sender_while_its_send_is_pending() {
    let (socket, control, mut events) = fixture(true, false);
    let (_incoming, starter) = starter(&socket);
    let mut stream = starter.start().unwrap();
    stream.write_all(b"payload").await.unwrap();
    assert_eq!(attempt(&mut events).await, (Some(0), 1, 3, false));
    assert!(control.waiter.lock().is_some());
    socket.shutdown().await.unwrap();
    assert_eq!(*control.dropped.lock(), [0]);
    assert!(control.waiter.lock().is_none());
    assert_eq!(control.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn dormant_and_rejected_streams_never_allocate_a_sender() {
    let (socket, control, _events) = fixture(true, true);
    let (_first_incoming, first) = starter(&socket);
    let (_second_incoming, second) = starter(&socket);
    let (_dormant_incoming, dormant) = starter(&socket);
    assert_eq!(control.factories.load(Ordering::Relaxed), 0);
    let _first = first.start().unwrap();
    let _second = second.start().unwrap();
    assert_eq!(control.factories.load(Ordering::Relaxed), 2);
    socket.shutdown().await.unwrap();
    assert!(matches!(dormant.start(), Err(Error::DispatcherDead)));
    assert_eq!(control.factories.load(Ordering::Relaxed), 2);
    let mut dropped = control.dropped.lock().clone();
    dropped.sort_unstable();
    assert_eq!(dropped, [0, 1]);
}
