// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-13.

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

/// One datagram of a [`Transport::poll_recv_many`], and the buffer of its
/// own it was read into: its first `head.len()` bytes are in `head`. The
/// rest of one longer than that is at the start of the call's one spill
/// buffer.
pub struct Received {
    pub head: Box<[u8]>,
    pub len: usize,
    pub from: SocketAddr,
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

    /// Whether the socket's streams hand their datagrams to the one task
    /// that also receives on the transport, which sends them in order. A
    /// kernel socket shared by many streams wants this: sends from many
    /// threads at once contend for the socket in the kernel. A stream with
    /// its own [`Transport::sender`] sends through that instead.
    fn sends_from_one_task(&self) -> bool {
        false
    }

    /// How many datagrams one [`Transport::poll_send_run`] may take: 1 for
    /// a transport that sends each datagram with a call of its own.
    fn send_run_most(&self) -> usize {
        1
    }

    /// Sends datagrams that lie end to end in `bytes`, all to `target`:
    /// each is `segment` bytes long but the last, which may be shorter.
    /// Ready with how many were sent, counted from the first: all of them
    /// or fewer, never none. Called only with more than one datagram, and
    /// with no more than [`Transport::send_run_most`]. A run the transport
    /// fails as a whole is sent again one datagram at a time.
    fn poll_send_run(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        segment: usize,
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        let first = &bytes[..segment.min(bytes.len())];
        self.poll_send_to(cx, first, target).map_ok(|_| 1)
    }

    /// How many datagrams, to any addresses, one
    /// [`Transport::poll_send_many`] may take: 1 for a transport that sends
    /// each datagram with a call of its own.
    fn send_many_most(&self) -> usize {
        1
    }

    /// Sends datagrams that lie end to end in `bytes`: `parts` gives each
    /// one's length and address, in order. Ready with how many were sent,
    /// counted from the first: all of them or fewer, never none. Called
    /// only with more than one datagram, and with no more than
    /// [`Transport::send_many_most`]. When the first cannot be sent the
    /// call fails, and that datagram is sent again alone.
    fn poll_send_many(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        parts: &[(usize, SocketAddr)],
    ) -> Poll<std::io::Result<usize>> {
        let (len, target) = parts[0];
        self.poll_send_to(cx, &bytes[..len], target).map_ok(|_| 1)
    }

    /// How many datagrams one [`Transport::poll_recv_many`] may return: 1
    /// for a transport that receives each datagram with a call of its own,
    /// which is then never asked for several.
    fn recv_many_most(&self) -> usize {
        1
    }

    /// Receives the datagrams that wait, at most `into.len()` of them, in
    /// the order they came, each into its own entry from the first on.
    /// Ready with how many: never none.
    ///
    /// The entries share `spill`, as [`Transport::recv_from_split`] has
    /// it, for what of a datagram does not fit its entry's `head`: the
    /// last such datagram of a call is whole. One before it in the same
    /// call, whose rest the later one wrote over, and one cut short, read
    /// as `spill.len()` bytes, which is the length of no whole datagram.
    ///
    /// A transport that does not receive several fails as unsupported, and
    /// is then read one datagram a call.
    fn poll_recv_many(
        &self,
        cx: &mut Context<'_>,
        into: &mut [Received],
        spill: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let _ = (cx, into, spill);
        Poll::Ready(Err(std::io::ErrorKind::Unsupported.into()))
    }

    /// Whether the socket's streams may run on uTP threads other than the
    /// one its I/O task runs on. Only a transport whose every datagram the
    /// I/O task sends ([`Transport::sends_from_one_task`]) and that keeps
    /// no state of its own per stream may say so: a kernel socket. A
    /// tunnel keeps its streams beside its I/O task.
    fn spreads_streams(&self) -> bool {
        false
    }

    /// Registers the transport's I/O with the runtime entered now, the one
    /// the socket's I/O task runs on. Called once, before anything is sent
    /// or received. A transport made ready to use needs nothing.
    fn register_io(&self) -> std::io::Result<()> {
        Ok(())
    }

    /// Stop and join work owned by this transport after its streams stop.
    fn shutdown(&self) -> impl Future<Output = std::io::Result<()>> + Send {
        async { Ok(()) }
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> + Send + Sync + 'a;

    /// Receives one datagram of at most `spill.len()` bytes: its first
    /// `head.len()` bytes into `head`, the rest from the start of `spill`,
    /// so a datagram that fits `head` stays there. A transport that cannot
    /// read into two buffers at once reads into `spill` and moves the
    /// datagram's start to `head`.
    fn recv_from_split<'a>(
        &'a self,
        head: &'a mut [u8],
        spill: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> + Send + 'a {
        async move {
            let (len, from) = self.recv_from(spill).await?;
            let first = len.min(head.len());
            head[..first].copy_from_slice(&spill[..first]);
            spill.copy_within(first..len, 0);
            Ok((len, from))
        }
    }

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
