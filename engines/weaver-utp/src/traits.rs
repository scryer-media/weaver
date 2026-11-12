// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-12.

use std::{
    future::Future,
    io::IoSlice,
    net::SocketAddr,
    task::{Context, Poll},
};

use tokio::time::Instant;

/// Sends one datagram gathered from several buffers.
pub trait PollSendToVectored {
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>>;
}

/// Whether a packet that tests the path's size may be fragmented on its way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fragmenting {
    /// A size probe. It is never fragmented: its arrival must prove the
    /// path carries it whole.
    Never,
    /// A packet the stream gave up on as larger than the path. The path may
    /// fragment it, so it still arrives (libtorrent sends every packet but a
    /// live probe without don't-fragment: utp_stream.cpp v2.0.15:1810-1812,
    /// 1835-1836, 2045-2046).
    Allowed,
}

/// A stream's send readiness registration. Dropping it releases that
/// stream's registration, including a waiter left by a pending poll.
pub trait DatagramSend: Send + Sync + Unpin {
    fn poll_send_to_vectored(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>>;

    /// Sends a size probe or a packet given up on as too large, as
    /// `fragmenting` says. A sender that cannot control fragmenting sends it
    /// as any other datagram.
    fn poll_send_to_vectored_fragmenting(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
        fragmenting: Fragmenting,
    ) -> Poll<std::io::Result<usize>> {
        let _ = fragmenting;
        self.poll_send_to_vectored(cx, bufs, target)
    }

    fn poll_send_to(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        self.poll_send_to_vectored(cx, &[IoSlice::new(buf)], target)
    }
}

/// The datagram transport a socket runs over. The crate never binds one
/// itself.
pub trait Transport: PollSendToVectored + Send + Sync + Unpin + 'static {
    /// Create an owned send registration for a started stream. `None`
    /// keeps polling this transport directly.
    fn sender(&self) -> Option<Box<dyn DatagramSend>> {
        None
    }

    /// Stop and join work owned by this transport after its streams stop.
    fn shutdown(&self) -> impl Future<Output = std::io::Result<()>> + Send {
        async { Ok(()) }
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> + Send + Sync + 'a;

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> impl Future<Output = std::io::Result<usize>> + Send + Sync + 'a;

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>>;

    /// Sends a size probe or a packet given up on as too large, as
    /// `fragmenting` says. Ordinary data goes through
    /// [`PollSendToVectored::poll_send_to_vectored`]. A transport that cannot
    /// control fragmenting, such as a tunnel, sends it as any other datagram.
    fn poll_send_to_vectored_fragmenting(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
        fragmenting: Fragmenting,
    ) -> Poll<std::io::Result<usize>> {
        let _ = fragmenting;
        self.poll_send_to_vectored(cx, bufs, target)
    }

    // The local address transport is bound to. Used only for logging.
    fn bind_addr(&self) -> SocketAddr;

    /// The largest IP packet the transport's path carries, where it knows
    /// it, such as a tunnel's MTU; no uTP packet is larger.
    fn link_mtu(&self) -> Option<u16> {
        None
    }
}

// The clock and randomness a socket uses; tests swap in a mock.
pub trait UtpEnvironment: Send + Sync + Unpin + 'static {
    fn now(&self) -> Instant;
    fn copy(&self) -> Self;
    fn random_u16(&self) -> u16;
}

#[derive(Default, Clone, Copy, Debug)]
pub struct DefaultUtpEnvironment {}

impl UtpEnvironment for DefaultUtpEnvironment {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn copy(&self) -> Self {
        *self
    }

    fn random_u16(&self) -> u16 {
        rand::random()
    }
}
