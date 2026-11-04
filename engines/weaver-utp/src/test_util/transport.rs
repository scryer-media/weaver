// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::{
    collections::HashMap,
    future::{Future, poll_fn},
    net::SocketAddr,
    sync::Arc,
    task::Poll,
};

use parking_lot::Mutex;
use tokio::sync::{
    Notify,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tracing::{Instrument, error_span};

use crate::{PollSendToVectored, SocketOpts, Transport, message::UtpMessage, raw::UtpHeader};

use super::{MockDispatcher, MockUtpSocket, env::MockUtpEnvironment};

type Msg = (SocketAddr, Vec<u8>);

pub struct MockInterface {
    sockets: Mutex<HashMap<SocketAddr, UnboundedSender<Msg>>>,
    pub env: MockUtpEnvironment,
}

impl MockInterface {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            sockets: Default::default(),
            env: MockUtpEnvironment::new(),
        })
    }

    pub fn create_socket_with_dispatcher(
        self: &Arc<Self>,
        bind_addr: SocketAddr,
    ) -> (Arc<MockUtpSocket>, MockDispatcher) {
        let (tx, rx) = unbounded_channel();
        let transport = MockUtpTransport::new(bind_addr, rx, self.clone());
        let env = self.env.clone();

        let opts = SocketOpts {
            ..Default::default()
        };

        let socket = MockUtpSocket::new_with_opts_and_dispatcher(transport, env, opts).unwrap();

        if self.sockets.lock().insert(bind_addr, tx).is_some() {
            panic!("socket with {} already existed", bind_addr)
        }
        socket
    }

    pub fn create_socket(self: &Arc<Self>, bind_addr: SocketAddr) -> Arc<MockUtpSocket> {
        let (sock, dispatcher) = self.create_socket_with_dispatcher(bind_addr);
        tokio::spawn(
            dispatcher
                .run_forever()
                .instrument(error_span!("sock", ?bind_addr)),
        );
        sock
    }
}

// Without this indirection rust analyzer doesn't work somehow
struct MockUtpTransportInnerLocked {
    rx: UnboundedReceiver<Msg>,
}

struct MockUtpTransportInner {
    locked: Mutex<MockUtpTransportInnerLocked>,
    interface: Arc<MockInterface>,
}

#[derive(Clone)]
pub struct MockUtpTransport {
    inner: Arc<MockUtpTransportInner>,
    bind_addr: SocketAddr,
}

impl MockUtpTransport {
    pub fn new(
        bind_addr: SocketAddr,
        rx: UnboundedReceiver<Msg>,
        interface: Arc<MockInterface>,
    ) -> Self {
        Self {
            inner: Arc::new(MockUtpTransportInner {
                locked: Mutex::new(MockUtpTransportInnerLocked { rx }),
                interface,
            }),
            bind_addr,
        }
    }

    pub fn send(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        match self.inner.interface.sockets.lock().get(&target) {
            Some(tx) => match tx.send((self.bind_addr, buf.to_owned())) {
                Ok(_) => {}
                Err(_) => return Err(std::io::Error::other(format!("target {target} is dead"))),
            },
            None => return Err(std::io::Error::other(format!("no route to {target}"))),
        };

        Ok(buf.len())
    }
}

impl Transport for MockUtpTransport {
    async fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        let f = poll_fn(|cx| self.inner.locked.lock().rx.poll_recv(cx));
        let (addr, data) = f.await.unwrap();
        assert!(data.len() <= buf.len());
        buf[..data.len()].copy_from_slice(&data);
        Ok((data.len(), addr))
    }

    async fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> std::io::Result<usize> {
        self.send(buf, target)
    }

    fn poll_send_to(
        &self,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> std::task::Poll<std::io::Result<usize>> {
        Poll::Ready(self.send(buf, target))
    }

    fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    fn dont_fragment(&self) -> bool {
        true
    }
}

impl PollSendToVectored for MockUtpTransport {
    fn poll_send_to_vectored(
        &self,
        _cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        let mut buf = Vec::new();
        bufs.iter().for_each(|b| buf.extend_from_slice(b.as_ref()));
        Poll::Ready(self.send(&buf, target))
    }
}

#[derive(Default)]
struct RememberingTransportLocked {
    messages: Vec<Msg>,
    max_payload_len: Option<usize>,
    link_mtu: Option<u16>,
    fragments: bool,
}

#[derive(Clone)]
pub struct RememberingTransport {
    pub bind_addr: SocketAddr,
    locked: Arc<Mutex<RememberingTransportLocked>>,
    sent: Arc<Notify>,
}

impl RememberingTransport {
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            locked: Default::default(),
            sent: Arc::default(),
        }
    }

    pub fn set_link_mtu(&self, mtu: u16) {
        self.locked.lock().link_mtu = Some(mtu);
    }

    // As a socket the host may fragment datagrams on.
    pub fn set_fragments(&self) {
        self.locked.lock().fragments = true;
    }

    pub fn set_max_payload_len(&mut self, len: usize) {
        self.locked.lock().max_payload_len = Some(len);
    }

    pub fn send(&self, addr: SocketAddr, buf: &[u8]) -> std::io::Result<()> {
        let mut g = self.locked.lock();
        let too_large = g.max_payload_len.is_some_and(|l| buf.len() > l);
        // A failed send is kept too, so tests can inspect the rejected packet.
        g.messages.push((addr, buf.to_owned()));
        drop(g);
        self.sent.notify_waiters();
        if too_large {
            Err(std::io::Error::from_raw_os_error(libc::EMSGSIZE))
        } else {
            Ok(())
        }
    }

    pub async fn wait_for_sent(&self, matches: impl Fn(&UtpHeader) -> bool) {
        loop {
            let sent = self.sent.notified();
            tokio::pin!(sent);
            sent.as_mut().enable();
            if self.locked.lock().messages.iter().any(|(_, data)| {
                UtpHeader::deserialize(data).is_some_and(|(header, _)| matches(&header))
            }) {
                return;
            }
            sent.await;
        }
    }

    pub fn take_sent(&self) -> Vec<Msg> {
        std::mem::take(&mut self.locked.lock().messages)
    }

    pub fn take_sent_utpmessages(&self) -> Vec<UtpMessage> {
        let sent = self.take_sent();
        sent.into_iter()
            .map(|(_, msg)| UtpMessage::deserialize(&msg, tokio::time::Instant::now()).unwrap())
            .collect()
    }
}

impl Transport for RememberingTransport {
    fn recv_from<'a>(
        &'a self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> {
        std::future::pending()
    }

    async fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> std::io::Result<usize> {
        self.send(target, buf)?;
        Ok(buf.len())
    }

    fn poll_send_to(
        &self,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        self.send(target, buf)?;
        Poll::Ready(Ok(buf.len()))
    }

    fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    fn link_mtu(&self) -> Option<u16> {
        self.locked.lock().link_mtu
    }

    fn dont_fragment(&self) -> bool {
        !self.locked.lock().fragments
    }
}

impl PollSendToVectored for RememberingTransport {
    fn poll_send_to_vectored(
        &self,
        _cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        let mut buf = Vec::new();
        bufs.iter().for_each(|b| buf.extend_from_slice(b.as_ref()));
        self.send(target, &buf)?;
        Poll::Ready(Ok(buf.len()))
    }
}
