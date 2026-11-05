// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io::{self, IoSlice},
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use futures::FutureExt;
use tempfile::TempDir;

use crate::{
    ConnectionOptions, Error, ListenerMode, ListenerOptions, Session, SessionOptions,
    network::{
        BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, SocketFactory,
        StreamListener,
    },
    stream_connect::Transports,
};

struct Watched(Arc<dyn DatagramSocket>);

impl DatagramSocket for Watched {
    fn bind_addr(&self) -> SocketAddr {
        self.0.bind_addr()
    }
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        self.0.recv_from(buf)
    }
    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        self.0.send_to(buf, target)
    }
    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.poll_send_to(cx, buf, target)
    }
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.poll_send_to_vectored(cx, bufs, target)
    }
    fn link_mtu(&self) -> Option<u16> {
        self.0.link_mtu()
    }
    fn dont_fragment(&self) -> bool {
        self.0.dont_fragment()
    }
    fn shutdown(&self) -> BoxFuture<'_, io::Result<()>> {
        self.0.shutdown()
    }
}

struct Bound {
    requested: SocketAddr,
    receive_buffer: usize,
    socket: Weak<Watched>,
}

#[derive(Default)]
struct Factory {
    reject_utp: bool,
    utp_attempts: AtomicUsize,
    tcp_attempts: AtomicUsize,
    listeners: AtomicUsize,
    bound: Mutex<Vec<Bound>>,
}

impl SocketFactory for Factory {
    fn connect_tcp<'a>(
        &'a self,
        _: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        self.tcp_attempts.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }
    fn listen_tcp<'a>(
        &'a self,
        _: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.listeners.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }
    fn bind_udp<'a>(&'a self, _: SocketAddr) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async { crate::test_support::udp_at((Ipv4Addr::LOCALHOST, 0).into()).await })
    }
    fn bind_utp<'a>(
        &'a self,
        requested: SocketAddr,
        receive_buffer: usize,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.utp_attempts.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            if self.reject_utp {
                return Err(io::ErrorKind::Unsupported.into());
            }
            let inner = crate::test_support::sockets()
                .bind_utp((Ipv4Addr::LOCALHOST, 0).into(), receive_buffer)
                .await?;
            let socket = Arc::new(Watched(inner));
            self.bound.lock().unwrap().push(Bound {
                requested,
                receive_buffer,
                socket: Arc::downgrade(&socket),
            });
            Ok(socket as Arc<dyn DatagramSocket>)
        })
    }
}

fn options(
    factory: &Arc<Factory>,
    enable_utp: Option<bool>,
    listen: bool,
    enable_tcp: bool,
) -> SessionOptions {
    SessionOptions {
        dht: None,
        persistence: None,
        disable_trackers: true,
        disable_local_service_discovery: true,
        ipv4_only: true,
        connections_limit: 4,
        listen: listen.then(|| ListenerOptions {
            mode: ListenerMode::UtpOnly,
            listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            ..Default::default()
        }),
        connect: Some(ConnectionOptions {
            enable_tcp,
            enable_utp,
            ..Default::default()
        }),
        socket_factory: factory.clone(),
        ..crate::test_support::session_options()
    }
}

#[tokio::test]
async fn explicit_outgoing_utp_owns_one_factory_socket_without_advertising_a_listener()
-> anyhow::Result<()> {
    let directory = TempDir::new()?;
    let factory = Arc::new(Factory::default());
    let session = Session::new_with_opts(
        directory.path().into(),
        options(&factory, Some(true), false, false),
    )
    .await?;
    assert_eq!(factory.utp_attempts.load(Ordering::Relaxed), 1);
    assert_eq!(factory.listeners.load(Ordering::Relaxed), 0);
    assert_eq!(factory.tcp_attempts.load(Ordering::Relaxed), 0);
    assert_eq!(session.listen_addr(), None);
    assert_eq!(session.ssl_listen_addr(), None);
    assert_eq!(session.announce_port(), None);
    assert!(!session.listens_utp());
    assert_eq!(session.utp_sockets.len(), 1);
    let retained = session.utp_sockets[0].0.clone();
    retained.stats().packets_lost.store(4, Ordering::Relaxed);
    assert_eq!(session.stats_snapshot().utp.packets_lost, 4);
    let socket = {
        let bound = factory.bound.lock().unwrap();
        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].requested, (Ipv4Addr::UNSPECIFIED, 0).into());
        assert_eq!(
            bound[0].receive_buffer,
            crate::udp_options::receive_buffer_for(4)
        );
        bound[0].socket.clone()
    };
    assert!(socket.upgrade().is_some());
    session.stop().await;
    assert!(
        socket.upgrade().is_none(),
        "outgoing transport survived joined stop"
    );
    assert_eq!(retained.stats().snapshot().packets_lost, 4);
    Ok(())
}

#[tokio::test]
async fn legacy_and_disabled_outgoing_utp_do_not_bind_an_unsupported_factory() -> anyhow::Result<()>
{
    for policy in [None, Some(false)] {
        let directory = TempDir::new()?;
        let factory = Arc::new(Factory {
            reject_utp: true,
            ..Default::default()
        });
        let session = Session::new_with_opts(
            directory.path().into(),
            options(&factory, policy, false, true),
        )
        .await?;
        assert_eq!(factory.utp_attempts.load(Ordering::Relaxed), 0);
        assert!(session.utp_sockets.is_empty());
        session.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn explicit_outgoing_utp_bind_failure_is_not_hidden_by_tcp() -> anyhow::Result<()> {
    for tcp in [false, true] {
        let directory = TempDir::new()?;
        let factory = Arc::new(Factory {
            reject_utp: true,
            ..Default::default()
        });
        let result = Session::new_with_opts(
            directory.path().into(),
            options(&factory, Some(true), false, tcp),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(factory.utp_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(factory.tcp_attempts.load(Ordering::Relaxed), 0);
        assert_eq!(factory.listeners.load(Ordering::Relaxed), 0);
        assert!(factory.bound.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn disabling_outgoing_utp_keeps_its_incoming_listener() -> anyhow::Result<()> {
    let directory = TempDir::new()?;
    let factory = Arc::new(Factory::default());
    let session = Session::new_with_opts(
        directory.path().into(),
        options(&factory, Some(false), true, true),
    )
    .await?;
    assert!(session.listen_addr().is_some());
    assert!(session.listens_utp());
    assert_eq!(factory.utp_attempts.load(Ordering::Relaxed), 1);
    let result = session
        .connector
        .connect((Ipv4Addr::LOCALHOST, 9).into(), Transports::Utp)
        .await;
    assert!(matches!(result, Err(Error::ConnectDisabled)));
    assert_eq!(session.connector.stats().utp.snapshot().v4.attempts, 0);
    assert_eq!(factory.tcp_attempts.load(Ordering::Relaxed), 0);
    session.stop().await;
    Ok(())
}

#[tokio::test]
async fn legacy_and_enabled_outgoing_utp_reuse_the_incoming_socket() -> anyhow::Result<()> {
    for policy in [None, Some(true)] {
        let directory = TempDir::new()?;
        let factory = Arc::new(Factory::default());
        let session = Session::new_with_opts(
            directory.path().into(),
            options(&factory, policy, true, true),
        )
        .await?;
        let peer = crate::test_support::loopback_udp().await;
        let mut connect = Box::pin(session.connector.connect(peer.bind_addr(), Transports::Utp));
        assert!(connect.as_mut().now_or_never().is_none());
        assert_eq!(session.connector.stats().utp.snapshot().v4.attempts, 1);
        assert_eq!(factory.utp_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(session.utp_sockets.len(), 1);
        assert_eq!(factory.tcp_attempts.load(Ordering::Relaxed), 0);
        drop(connect);
        session.stop().await;
    }
    Ok(())
}
