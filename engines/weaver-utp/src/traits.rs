// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

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

/// The datagram transport a socket runs over. The crate never binds one
/// itself.
pub trait Transport: PollSendToVectored + Send + Sync + Unpin + 'static {
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

    // The local address transport is bound to. Used only for logging.
    fn bind_addr(&self) -> SocketAddr;

    /// Bytes the transport adds to each datagram to `target` on its way,
    /// such as a proxy's header; uTP's packets are that much smaller.
    fn datagram_overhead(&self, _target: SocketAddr) -> usize {
        0
    }

    /// Whether the transport's datagrams leave with don't-fragment set, so
    /// an MTU probe too large for the path is lost rather than split.
    fn dont_fragment(&self) -> bool {
        false
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
