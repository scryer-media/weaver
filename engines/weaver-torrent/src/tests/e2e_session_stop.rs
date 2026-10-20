// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use tempfile::TempDir;

use crate::{
    DhtSessionConfig, Session, SessionOptions,
    network::{
        BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, SocketFactory,
        StreamListener,
    },
};

/// A loopback datagram socket that says when it is dropped.
struct Watched {
    inner: tokio::net::UdpSocket,
    dropped: Arc<AtomicBool>,
}

impl Drop for Watched {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl DatagramSocket for Watched {
    fn bind_addr(&self) -> SocketAddr {
        self.inner.bind_addr()
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        DatagramSocket::recv_from(&self.inner, buf)
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        DatagramSocket::send_to(&self.inner, buf, target)
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        DatagramSocket::poll_send_to(&self.inner, cx, buf, target)
    }
}

/// Binds every datagram socket on loopback as a [`Watched`] one, and opens no
/// stream sockets.
#[derive(Default)]
struct WatchedUdp {
    bound: Mutex<Vec<(SocketAddr, Arc<AtomicBool>)>>,
}

impl WatchedUdp {
    fn dropped(&self, addr: SocketAddr) -> bool {
        let bound = self.bound.lock().unwrap();
        let (_, dropped) = bound.iter().find(|(a, _)| *a == addr).unwrap();
        dropped.load(Ordering::SeqCst)
    }
}

impl SocketFactory for WatchedUdp {
    fn connect_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }

    fn listen_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            let inner = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let dropped = Arc::new(AtomicBool::new(false));
            self.bound
                .lock()
                .unwrap()
                .push((inner.bind_addr(), dropped.clone()));
            Ok(Arc::new(Watched { inner, dropped }) as Arc<dyn DatagramSocket>)
        })
    }
}

#[tokio::test]
async fn test_stop_returns_once_the_dht_socket_is_gone() -> anyhow::Result<()> {
    // The DHT's one bootstrap node: a loopback socket nobody answers on.
    let bootstrap = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let dir = TempDir::with_prefix("test_session_stop")?;
    let factory = Arc::new(WatchedUdp::default());

    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: Some(DhtSessionConfig {
                bootstrap_addrs: Some(vec![bootstrap.local_addr()?.to_string()]),
                port: None,
                persistence: None,
            }),
            persistence: None,
            disable_local_service_discovery: true,
            socket_factory: Some(factory.clone()),
            ..Default::default()
        },
    )
    .await?;
    let dht_addr = session
        .get_dht()
        .expect("the session runs a DHT")
        .listen_addr();

    // The DHT is up and talking through its socket.
    let mut buf = [0u8; 2048];
    loop {
        let (_, from) = bootstrap.recv_from(&mut buf).await?;
        if from == dht_addr {
            break;
        }
    }
    assert!(!factory.dropped(dht_addr));

    session.stop().await;
    assert!(
        factory.dropped(dht_addr),
        "stop() returned with the DHT socket still held"
    );
    Ok(())
}
