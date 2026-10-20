// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Pluggable network primitives.
//!
//! By default every crate in this workspace binds and connects kernel sockets
//! itself. Embedders that need traffic to flow through something else (a
//! userspace VPN, a SOCKS/HTTP proxy, a per-tenant network namespace, a test
//! harness) can hand in their own implementations of these traits instead.
//!
//! The traits are object safe on purpose: an embedder passes an
//! `Arc<dyn DatagramSocket>` and never needs to name the concrete type.
//! Default code paths keep using concrete kernel sockets, so the indirection
//! costs nothing unless a custom socket is injected.

use std::{
    future::Future,
    io::{self, IoSlice},
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

/// A boxed future that is both `Send` and `Sync`, as required by the uTP
/// transport abstraction this trait is adapted to.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 'a>>;

/// An unconnected, message-oriented socket: the shape of a UDP socket, without
/// requiring it to be one.
///
/// Used for DHT, UDP trackers and the uTP peer transport.
pub trait DatagramSocket: Send + Sync + 'static {
    /// The local address the socket is bound to.
    fn bind_addr(&self) -> SocketAddr;

    fn recv_from<'a>(&'a self, buf: &'a mut [u8])
    -> BoxFuture<'a, io::Result<(usize, SocketAddr)>>;

    fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr)
    -> BoxFuture<'a, io::Result<usize>>;

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>>;

    /// Vectored send. The default implementation copies the slices into one
    /// buffer; implementations backed by a real socket should override it.
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        let mut joined = Vec::with_capacity(total);
        for b in bufs {
            joined.extend_from_slice(b);
        }
        self.poll_send_to(cx, &joined, target)
    }
}

impl DatagramSocket for tokio::net::UdpSocket {
    fn bind_addr(&self) -> SocketAddr {
        self.local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(tokio::net::UdpSocket::recv_from(self, buf))
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(tokio::net::UdpSocket::send_to(self, buf, target))
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        tokio::net::UdpSocket::poll_send_to(self, cx, buf, target)
    }
}

/// Resolves a hostname to socket addresses.
///
/// Used for DHT bootstrap nodes and UDP trackers. Embedders that route traffic
/// through their own network stack can inject one so that lookups go through
/// that stack too, instead of the host's resolver.
pub trait Resolver: Send + Sync + 'static {
    /// Resolve `host` (a domain name, without port or brackets) and return
    /// every address found, each carrying `port`.
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> BoxFuture<'a, io::Result<Vec<SocketAddr>>>;
}

/// The host's resolver, via [`tokio::net::lookup_host`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemResolver;

impl Resolver for SystemResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> BoxFuture<'a, io::Result<Vec<SocketAddr>>> {
        Box::pin(async move { Ok(tokio::net::lookup_host((host, port)).await?.collect()) })
    }
}
