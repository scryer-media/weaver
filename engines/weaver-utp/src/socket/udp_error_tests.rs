// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    collections::VecDeque,
    io::{self, IoSlice},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use futures::FutureExt;
use parking_lot::Mutex;
use tokio::sync::{Semaphore, oneshot};

use crate::{
    Error, PollSendToVectored, SocketOpts, Transport, UtpSocket,
    constants::CONSECUTIVE_RECEIVE_ERRORS_MAX,
    raw::{Type, UtpHeader},
    test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment},
};

enum Reply {
    Error(io::Error),
    Packet(Vec<u8>),
}

struct Control {
    replies: Mutex<VecDeque<Reply>>,
    repeating: Option<bool>,
    received: AtomicUsize,
    watermark: Mutex<Option<oneshot::Sender<()>>>,
    shutdown_entered: Mutex<Option<oneshot::Sender<()>>>,
    dropped: Mutex<Option<oneshot::Sender<()>>>,
    shutdown_release: Semaphore,
}

struct ReceiveTransport(Arc<Control>);

struct Observed {
    control: Arc<Control>,
    watermark: oneshot::Receiver<()>,
    shutdown_entered: oneshot::Receiver<()>,
    dropped: oneshot::Receiver<()>,
}

fn transport(
    replies: Vec<Reply>,
    repeating: Option<bool>,
    gated: bool,
) -> (ReceiveTransport, Observed) {
    let (watermark_tx, watermark) = oneshot::channel();
    let (entered_tx, shutdown_entered) = oneshot::channel();
    let (dropped_tx, dropped) = oneshot::channel();
    let control = Arc::new(Control {
        replies: Mutex::new(replies.into()),
        repeating,
        received: AtomicUsize::new(0),
        watermark: Mutex::new(Some(watermark_tx)),
        shutdown_entered: Mutex::new(Some(entered_tx)),
        dropped: Mutex::new(Some(dropped_tx)),
        shutdown_release: Semaphore::new(usize::from(!gated)),
    });
    (
        ReceiveTransport(Arc::clone(&control)),
        Observed {
            control,
            watermark,
            shutdown_entered,
            dropped,
        },
    )
}

fn socket(
    replies: Vec<Reply>,
    repeating: Option<bool>,
    gated: bool,
) -> (
    Arc<UtpSocket<ReceiveTransport, MockUtpEnvironment>>,
    Observed,
) {
    let (transport, observed) = transport(replies, repeating, gated);
    let socket =
        UtpSocket::new_with_opts(transport, MockUtpEnvironment::new(), SocketOpts::default())
            .unwrap();
    (socket, observed)
}

fn syn() -> Vec<u8> {
    let mut syn = vec![0; 20];
    UtpHeader {
        htype: Type::Syn,
        connection_id: 101.into(),
        seq_nr: 1.into(),
        wnd_size: 65535,
        ..Default::default()
    }
    .serialize(&mut syn)
    .unwrap();
    syn
}

fn refused() -> Reply {
    Reply::Error(io::Error::new(io::ErrorKind::ConnectionRefused, "datagram"))
}

impl Drop for ReceiveTransport {
    fn drop(&mut self) {
        if let Some(dropped) = self.0.dropped.lock().take() {
            let _ = dropped.send(());
        }
    }
}

impl Transport for ReceiveTransport {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let received = self.0.received.fetch_add(1, Ordering::Relaxed) + 1;
        if received == 64
            && let Some(watermark) = self.0.watermark.lock().take()
        {
            watermark.send(()).unwrap();
        }
        if let Some(alternate) = self.0.repeating {
            if alternate && received.is_multiple_of(2) {
                buf[0] = 0xff;
                return Ok((1, ADDR_2));
            }
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "ready datagram error",
            ));
        }
        let reply = self.0.replies.lock().pop_front();
        match reply {
            Some(Reply::Error(error)) => Err(error),
            Some(Reply::Packet(packet)) => {
                buf[..packet.len()].copy_from_slice(&packet);
                Ok((packet.len(), ADDR_2))
            }
            None => std::future::pending().await,
        }
    }

    async fn send_to(&self, buf: &[u8], _: SocketAddr) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn poll_send_to(
        &self,
        _: &mut Context<'_>,
        buf: &[u8],
        _: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn bind_addr(&self) -> SocketAddr {
        ADDR_1
    }

    async fn shutdown(&self) -> io::Result<()> {
        if let Some(entered) = self.0.shutdown_entered.lock().take() {
            entered.send(()).unwrap();
        }
        self.0.shutdown_release.acquire().await.unwrap().forget();
        Ok(())
    }
}

impl PollSendToVectored for ReceiveTransport {
    fn poll_send_to_vectored(
        &self,
        _: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        _: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bufs.iter().map(|buf| buf.len()).sum()))
    }
}

#[tokio::test(start_paused = true)]
async fn recoverable_receive_errors_leave_the_socket_available_for_a_valid_syn() {
    let mut syn = vec![0; 20];
    UtpHeader {
        htype: Type::Syn,
        connection_id: 101.into(),
        seq_nr: 1.into(),
        wnd_size: 65535,
        ..Default::default()
    }
    .serialize(&mut syn)
    .unwrap();
    let mut replies: Vec<_> = [
        io::ErrorKind::WouldBlock,
        io::ErrorKind::Interrupted,
        io::ErrorKind::HostUnreachable,
        io::ErrorKind::NetworkUnreachable,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::ConnectionRefused,
        io::ErrorKind::ConnectionAborted,
    ]
    .into_iter()
    .map(|kind| Reply::Error(io::Error::new(kind, "datagram")))
    .collect();
    replies.push(Reply::Error(crate::message_size_error()));
    replies.push(Reply::Packet(syn));
    let (socket, mut observed) = socket(replies, None, false);
    let stream = socket.accept().await.unwrap();
    assert_eq!(socket.stats().accepts.load(Ordering::Relaxed), 1);
    assert!(observed.control.received.load(Ordering::Relaxed) >= 9);
    socket.shutdown().await.unwrap();
    assert!(observed.dropped.try_recv().is_ok());
    drop(stream);
}

#[tokio::test(start_paused = true)]
async fn maximum_udp_datagram_does_not_truncate_or_kill_the_dispatcher() {
    let mut syn = vec![0; 20];
    UtpHeader {
        htype: Type::Syn,
        connection_id: 101.into(),
        seq_nr: 1.into(),
        ..Default::default()
    }
    .serialize(&mut syn)
    .unwrap();
    let largest_udp_payload = vec![0xff; u16::MAX as usize - 8];
    let mut ambiguous_payload = vec![0xaa; u16::MAX as usize];
    UtpHeader {
        htype: Type::Data,
        connection_id: 102.into(),
        seq_nr: 2.into(),
        ..Default::default()
    }
    .serialize(&mut ambiguous_payload)
    .unwrap();
    let (socket, mut observed) = socket(
        vec![
            Reply::Packet(largest_udp_payload),
            Reply::Packet(ambiguous_payload),
            Reply::Packet(syn),
        ],
        None,
        false,
    );

    let stream = socket.accept().await.unwrap();
    assert_eq!(socket.stats().invalid_packets.load(Ordering::Relaxed), 2);
    assert!(observed.control.received.load(Ordering::Relaxed) >= 3);
    socket.shutdown().await.unwrap();
    assert!(observed.dropped.try_recv().is_ok());
    drop(stream);
}

#[tokio::test(start_paused = true)]
async fn continuously_ready_errors_and_packets_yield_so_shutdown_can_join() {
    let (socket, mut observed) = socket(Vec::new(), Some(true), false);
    observed.watermark.await.unwrap();
    socket.shutdown().await.unwrap();
    assert!(observed.control.received.load(Ordering::Relaxed) >= 64);
    assert!(socket.stats().invalid_packets.load(Ordering::Relaxed) > 0);
    assert!(observed.dropped.try_recv().is_ok());
}

#[tokio::test(start_paused = true)]
async fn a_datagram_resets_the_run_of_recoverable_receive_errors() {
    let run = CONSECUTIVE_RECEIVE_ERRORS_MAX - 1;
    let mut replies: Vec<_> = (0..run).map(|_| refused()).collect();
    replies.push(Reply::Packet(vec![0xff]));
    replies.extend((0..run).map(|_| refused()));
    replies.push(Reply::Packet(syn()));
    let (socket, mut observed) = socket(replies, None, false);
    let stream = socket.accept().await.unwrap();
    assert_eq!(socket.stats().accepts.load(Ordering::Relaxed), 1);
    assert_eq!(socket.stats().invalid_packets.load(Ordering::Relaxed), 1);
    assert!(observed.control.received.load(Ordering::Relaxed) >= 2 * run + 2);
    socket.shutdown().await.unwrap();
    assert!(observed.dropped.try_recv().is_ok());
    drop(stream);
}

#[tokio::test(start_paused = true)]
async fn an_unbroken_run_of_recoverable_receive_errors_ends_the_dispatcher() {
    let mut replies: Vec<_> = (0..CONSECUTIVE_RECEIVE_ERRORS_MAX)
        .map(|_| refused())
        .collect();
    replies.push(Reply::Packet(syn()));
    let (transport, observed) = transport(replies, None, false);
    let (_socket, dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
        transport,
        MockUtpEnvironment::new(),
        SocketOpts::default(),
    )
    .unwrap();
    let result = dispatcher.run_forever().await;
    assert!(
        matches!(
            &result,
            Err(Error::Recv(error)) if error.kind() == io::ErrorKind::ConnectionRefused
        ),
        "{result:?}"
    );
    assert_eq!(
        observed.control.received.load(Ordering::Relaxed),
        CONSECUTIVE_RECEIVE_ERRORS_MAX
    );
}

#[tokio::test(start_paused = true)]
async fn a_terminal_receive_failure_still_waits_for_joined_transport_cleanup() {
    let (socket, mut observed) = socket(
        vec![Reply::Error(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "terminal receive",
        ))],
        None,
        true,
    );
    observed.shutdown_entered.await.unwrap();
    let mut stopped = Box::pin(socket.shutdown());
    assert!(stopped.as_mut().now_or_never().is_none());
    assert!(observed.dropped.try_recv().is_err());
    observed.control.shutdown_release.add_permits(1);
    assert!(stopped.await.is_err());
    assert!(socket.shutdown().await.is_err());
    assert!(observed.dropped.try_recv().is_ok());
    assert_eq!(observed.control.received.load(Ordering::Relaxed), 1);
}
