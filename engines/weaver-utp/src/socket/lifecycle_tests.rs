// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io::{self, IoSlice},
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use futures::FutureExt;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Semaphore, oneshot},
};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, PollSendToVectored, SocketOpts, Transport, UtpSocket,
    raw::{Type, UtpHeader},
    stream_dispatch::{StreamArgs, UtpStreamStarter},
    test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
    traits::UtpEnvironment,
};

#[derive(Clone, Copy)]
enum Hook {
    Ok,
    Error,
    Panic,
}

struct Control {
    transport: RememberingTransport,
    hook: Hook,
    fatal_receive: bool,
    receives: AtomicUsize,
    shutdowns: AtomicUsize,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    dropped: Mutex<Option<oneshot::Sender<()>>>,
    release: Semaphore,
}

struct LifecycleTransport(Arc<Control>);

struct Observation {
    control: Arc<Control>,
    entered: oneshot::Receiver<()>,
    dropped: oneshot::Receiver<()>,
}

fn transport(hook: Hook, gated: bool, fatal_receive: bool) -> (LifecycleTransport, Observation) {
    let (entered_tx, entered) = oneshot::channel();
    let (dropped_tx, dropped) = oneshot::channel();
    let control = Arc::new(Control {
        transport: RememberingTransport::new(ADDR_1),
        hook,
        fatal_receive,
        receives: AtomicUsize::new(0),
        shutdowns: AtomicUsize::new(0),
        entered: Mutex::new(Some(entered_tx)),
        dropped: Mutex::new(Some(dropped_tx)),
        release: Semaphore::new(usize::from(!gated)),
    });
    (
        LifecycleTransport(control.clone()),
        Observation {
            control,
            entered,
            dropped,
        },
    )
}

impl Drop for LifecycleTransport {
    fn drop(&mut self) {
        if let Some(dropped) = self.0.dropped.lock().take() {
            let _ = dropped.send(());
        }
    }
}

impl Transport for LifecycleTransport {
    async fn shutdown(&self) -> io::Result<()> {
        self.0.shutdowns.fetch_add(1, Ordering::Relaxed);
        if let Some(entered) = self.0.entered.lock().take() {
            let _ = entered.send(());
        }
        self.0.release.acquire().await.unwrap().forget();
        match self.0.hook {
            Hook::Ok => Ok(()),
            Hook::Error => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "transport shutdown failed",
            )),
            Hook::Panic => panic!("transport shutdown hook panicked"),
        }
    }

    async fn recv_from(&self, _buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.receives.fetch_add(1, Ordering::Relaxed);
        if self.0.fatal_receive {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "terminal receive failure",
            ));
        }
        std::future::pending().await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.0.transport.send_to(buf, target).await
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.transport.poll_send_to(cx, buf, target)
    }

    fn bind_addr(&self) -> SocketAddr {
        ADDR_1
    }
}

impl PollSendToVectored for LifecycleTransport {
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.transport.poll_send_to_vectored(cx, bufs, target)
    }
}

type Socket = Arc<UtpSocket<LifecycleTransport, MockUtpEnvironment>>;

fn socket(hook: Hook, gated: bool, fatal_receive: bool) -> (Socket, Observation) {
    let (transport, observation) = transport(hook, gated, fatal_receive);
    let socket = UtpSocket::new_with_opts(
        transport,
        MockUtpEnvironment::new(),
        SocketOpts {
            vsock_tx_bufsize_bytes_initial: NonZeroUsize::new(8),
            vsock_tx_bufsize_bytes_max: NonZeroUsize::new(8),
            ..Default::default()
        },
    )
    .unwrap();
    (socket, observation)
}

fn starter(
    socket: &Socket,
) -> (
    crate::ingress::Sender,
    UtpStreamStarter<LifecycleTransport, MockUtpEnvironment>,
) {
    let (sender, receiver) = crate::ingress::channel(NonZeroUsize::new(64).unwrap());
    let now = socket.state.env.now();
    let header = UtpHeader {
        htype: Type::State,
        seq_nr: 1.into(),
        ack_nr: 100.into(),
        wnd_size: 64,
        ..Default::default()
    };
    let args = StreamArgs::new_outgoing(&header, now, now);
    (
        sender,
        UtpStreamStarter::new(socket, ADDR_2, receiver, args),
    )
}

#[tokio::test]
async fn shutdown_joins_tasks_then_transport_even_when_a_caller_is_cancelled() {
    let (socket, mut observation) = socket(Hook::Ok, true, false);
    let release_task = Arc::new(Semaphore::new(0));
    let task_gate = release_task.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    socket
        .runtime
        .register(move |_| async move {
            entered_tx.send(()).unwrap();
            task_gate.acquire().await.unwrap().forget();
        })
        .unwrap();
    entered_rx.await.unwrap();
    let mut first = Box::pin(socket.shutdown());
    let mut second = Box::pin(socket.shutdown());
    assert!(first.as_mut().now_or_never().is_none());
    assert!(second.as_mut().now_or_never().is_none());
    assert!(matches!(
        observation.entered.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(first);

    release_task.add_permits(1);
    observation.entered.await.unwrap();
    assert!(second.as_mut().now_or_never().is_none());
    observation.control.release.add_permits(1);
    second.await.unwrap();
    assert!(observation.dropped.try_recv().is_ok());
    socket.shutdown().await.unwrap();
    assert_eq!(observation.control.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn transport_shutdown_errors_and_panics_complete_every_waiter() {
    for hook in [Hook::Error, Hook::Panic] {
        let (socket, mut observation) = socket(hook, false, false);
        let (first, second) = tokio::join!(socket.shutdown(), socket.shutdown());
        let first = first.unwrap_err();
        let second = second.unwrap_err();
        assert_eq!(first.kind(), second.kind());
        assert_eq!(first.to_string(), second.to_string());
        assert_eq!(
            socket.shutdown().await.unwrap_err().to_string(),
            first.to_string()
        );
        assert!(observation.dropped.try_recv().is_ok());
        assert_eq!(observation.control.shutdowns.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn a_registered_task_panic_still_joins_transport_and_completes_shutdown() {
    let (socket, mut observation) = socket(Hook::Ok, false, false);
    socket
        .runtime
        .register(|_| async { panic!("registered task panicked") })
        .unwrap();
    assert!(socket.shutdown().await.is_err());
    assert!(observation.dropped.try_recv().is_ok());
    assert_eq!(observation.control.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn an_unstarted_stream_cannot_retain_or_restart_a_closed_transport() {
    let (socket, mut observation) = socket(Hook::Ok, false, false);
    let (_sender, starter) = starter(&socket);
    socket.shutdown().await.unwrap();
    assert!(observation.dropped.try_recv().is_ok());
    assert!(matches!(starter.start(), Err(Error::DispatcherDead)));
    assert_eq!(socket.bind_addr(), ADDR_1);
    assert_eq!(
        socket.stats().live_virtual_sockets.load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn closing_during_initialization_never_starts_the_dispatcher() {
    let (transport, mut observation) = transport(Hook::Ok, false, false);
    let parent = CancellationToken::new();
    let socket = UtpSocket::new_with_opts_and_init(
        transport,
        MockUtpEnvironment::new(),
        SocketOpts {
            cancellation_token: parent.clone(),
            ..Default::default()
        },
        |socket| {
            socket.set_connections_limit(7);
            parent.cancel();
        },
    )
    .unwrap();
    socket.shutdown().await.unwrap();
    assert_eq!(socket.connections_limit(), Some(7));
    assert_eq!(observation.control.receives.load(Ordering::Relaxed), 0);
    assert!(observation.dropped.try_recv().is_ok());
}

#[tokio::test]
async fn dropping_the_last_public_handle_runs_transport_cleanup() {
    let (socket, observation) = socket(Hook::Ok, false, false);
    let weak = Arc::downgrade(&socket);
    drop(socket);
    observation.dropped.await.unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(observation.control.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn shutdown_wakes_pending_read_and_flush_without_cancelling_the_parent() {
    let parent = CancellationToken::new();
    let (transport, mut observation) = transport(Hook::Ok, false, false);
    let socket = UtpSocket::new_with_opts(
        transport,
        MockUtpEnvironment::new(),
        SocketOpts {
            cancellation_token: parent.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    let (_sender, starter) = starter(&socket);
    let stream = starter.start().unwrap();
    let (mut reader, mut writer) = stream.split();
    writer.write_all(b"unacked").await.unwrap();
    let mut byte = [0];
    let read = reader.read(&mut byte);
    let flush = writer.flush();
    tokio::pin!(read, flush);
    assert!(read.as_mut().now_or_never().is_none());
    assert!(flush.as_mut().now_or_never().is_none());
    socket.shutdown().await.unwrap();
    assert!(matches!(read.as_mut().now_or_never(), Some(Ok(0) | Err(_))));
    assert!(matches!(flush.as_mut().now_or_never(), Some(Err(_))));
    assert!(!parent.is_cancelled());
    assert!(observation.dropped.try_recv().is_ok());
}

#[tokio::test]
async fn shutdown_wakes_a_pending_write() {
    let (socket, _) = socket(Hook::Ok, false, false);
    let (_sender, starter) = starter(&socket);
    let mut stream = starter.start().unwrap();
    let write = stream.write_all(b"more than eight bytes");
    tokio::pin!(write);
    assert!(write.as_mut().now_or_never().is_none());
    socket.shutdown().await.unwrap();
    assert!(matches!(write.as_mut().now_or_never(), Some(Err(_))));
}

#[tokio::test]
async fn fatal_dispatcher_exit_cleans_up_streams_without_a_shutdown_caller() {
    let (socket, observation) = socket(Hook::Ok, false, true);
    let (_sender, starter) = starter(&socket);
    let mut stream = starter.start().unwrap();
    stream.write_all(b"unacked").await.unwrap();
    let flush = stream.flush();
    tokio::pin!(flush);
    assert!(flush.as_mut().now_or_never().is_none());
    observation.dropped.await.unwrap();
    assert!(socket.shutdown().await.is_err());
    assert!(matches!(flush.as_mut().now_or_never(), Some(Err(_))));
    assert!(matches!(socket.accept().await, Err(Error::DispatcherDead)));
    assert!(matches!(
        socket.connect(ADDR_2).await,
        Err(Error::DispatcherDead)
    ));
}
