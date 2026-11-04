// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{Ipv6Addr, SocketAddr},
    sync::Arc,
};

use anyhow::Context;
use tokio::io::AsyncWrite;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    network::{DatagramTransport, SocketFactory, StreamListener, UtpSocket},
    stream_connect::ConnectionKind,
    vectored_traits::AsyncReadVectored,
};

/// Why a session failed to build when its listeners could not start: a
/// port of theirs could not be bound.
#[derive(Debug, Clone, Copy)]
pub struct ListenFailed;

impl std::fmt::Display for ListenFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("error starting listeners")
    }
}

/// A listener's sockets, and the address they took.
pub(crate) struct Listening {
    pub tcp_socket: Option<Box<dyn StreamListener>>,
    pub utp_socket: Option<UtpSocket>,
    pub addr: SocketAddr,
}

pub(crate) struct ListenResult {
    /// The SSL listener: SSL torrents' peers connect to it, TLS from their
    /// first byte.
    pub ssl: Option<Listening>,
    /// The TCP listener the session's `SocketFactory` opened.
    pub tcp_socket: Option<Box<dyn StreamListener>>,
    pub utp_socket: Option<UtpSocket>,
    pub enable_upnp_port_forwarding: bool,
    pub addr: SocketAddr,
    pub announce_port: Option<u16>,
    pub max_pending_incoming_handshake_checks: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum ListenerMode {
    TcpOnly,
    UtpOnly,
    TcpAndUtp,
}

pub const DEFAULT_MAX_PENDING_INCOMING_HANDSHAKE_CHECKS: usize = 256;

impl ListenerMode {
    pub fn tcp_enabled(&self) -> bool {
        match self {
            ListenerMode::TcpOnly => true,
            ListenerMode::UtpOnly => false,
            ListenerMode::TcpAndUtp => true,
        }
    }

    pub fn utp_enabled(&self) -> bool {
        match self {
            ListenerMode::TcpOnly => false,
            ListenerMode::UtpOnly => true,
            ListenerMode::TcpAndUtp => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ListenerOptions {
    pub mode: ListenerMode,
    pub listen_addr: SocketAddr,
    pub enable_upnp_port_forwarding: bool,
    pub utp_opts: Option<weaver_utp::SocketOpts>,
    pub announce_port: Option<u16>,
    pub ipv4_only: bool,
    pub max_pending_incoming_handshake_checks: usize,
    /// The port of the SSL listener, on the listen address, over the same
    /// transports; 0 takes any free port. `None` opens none, and SSL
    /// torrents then take no incoming connection.
    pub ssl_listen_port: Option<u16>,
}

impl Default for ListenerOptions {
    fn default() -> Self {
        Self {
            // TODO: once uTP is stable upgrade default to both
            mode: ListenerMode::TcpOnly,
            listen_addr: (Ipv6Addr::UNSPECIFIED, 0).into(),
            enable_upnp_port_forwarding: false,
            utp_opts: None,
            announce_port: None,
            ipv4_only: false,
            max_pending_incoming_handshake_checks: DEFAULT_MAX_PENDING_INCOMING_HANDSHAKE_CHECKS,
            ssl_listen_port: None,
        }
    }
}

impl ListenerOptions {
    /// Bind the listener's sockets. Its uTP socket's receive buffer holds a
    /// receive window for each of the `connections_limit` connections the
    /// session may hold, as far as the host allows.
    pub(crate) async fn start(
        mut self,
        parent_span: Option<tracing::Id>,
        cancellation_token: CancellationToken,
        socket_factory: &Arc<dyn SocketFactory>,
        connection_slots: &crate::connection_slots::ConnectionSlots,
    ) -> anyhow::Result<ListenResult> {
        let receive_buffer = crate::udp_options::receive_buffer_for(connection_slots.limit());
        let mut utp_opts = self.utp_opts.take().unwrap_or_default();
        utp_opts.cancellation_token = cancellation_token.clone();
        utp_opts.parent_span = parent_span;
        utp_opts.dont_wait_for_lastack = true;

        let listen_addr = if self.ipv4_only {
            if self.listen_addr.is_ipv6() && self.listen_addr.ip().is_unspecified() {
                // Force to IPv4 unspecified if IPv6 unspecified was requested but we are v4 only
                SocketAddr::from(([0, 0, 0, 0], self.listen_addr.port()))
            } else {
                self.listen_addr
            }
        } else {
            self.listen_addr
        };

        let bound = bind(
            self.mode,
            listen_addr,
            utp_opts.clone(),
            receive_buffer,
            socket_factory,
            true,
            connection_slots,
        )
        .await?;
        let listen_addr = bound.addr;
        let (tcp_socket, utp_socket) = (bound.tcp_socket, bound.utp_socket);

        let ssl = match self.ssl_listen_port {
            Some(port) => Some(
                bind(
                    self.mode,
                    SocketAddr::new(listen_addr.ip(), port),
                    utp_opts,
                    receive_buffer,
                    socket_factory,
                    false,
                    connection_slots,
                )
                .await
                .context("error starting the SSL listener")?,
            ),
            None => None,
        };
        if let Some(ssl) = &ssl {
            info!(addr = ?ssl.addr, "Listening for SSL torrents' peers");
        }

        let announce_port = if let Some(p) = self.announce_port {
            Some(p)
        } else if listen_addr.ip().is_loopback() {
            None
        } else {
            Some(listen_addr.port())
        };
        Ok(ListenResult {
            ssl,
            tcp_socket,
            utp_socket,
            announce_port,
            addr: listen_addr,
            enable_upnp_port_forwarding: self.enable_upnp_port_forwarding,
            max_pending_incoming_handshake_checks: self.max_pending_incoming_handshake_checks,
        })
    }
}

// A listener's sockets on `listen_addr`, as `mode` says; a port of 0 takes
// TCP's free port, and uTP binds the port TCP took. The session's `peers`
// listener is the one a factory's forwarded port reaches. Its uTP socket
// asks for a receive buffer of `receive_buffer` bytes.
async fn bind(
    mode: ListenerMode,
    mut listen_addr: SocketAddr,
    utp_opts: weaver_utp::SocketOpts,
    receive_buffer: usize,
    factory: &Arc<dyn SocketFactory>,
    peers: bool,
    connection_slots: &crate::connection_slots::ConnectionSlots,
) -> anyhow::Result<Listening> {
    let tcp_socket = if mode.tcp_enabled() {
        let listening = if peers {
            factory.listen_peers_tcp(listen_addr)
        } else {
            factory.listen_tcp(listen_addr)
        };
        let listener = listening.await.context("error starting TCP listener")?;
        listen_addr = listener.local_addr();
        info!(
            "Listening on TCP {:?} for incoming peer connections",
            listen_addr
        );
        Some(listener)
    } else {
        None
    };

    let utp_socket = if mode.utp_enabled() {
        match bind_utp(
            factory.as_ref(),
            listen_addr,
            utp_opts,
            receive_buffer,
            Some(connection_slots),
        )
        .await
        {
            Ok(sock) => {
                listen_addr = sock.bind_addr();
                info!(
                    "Listening on UDP {:?} for incoming uTP peer connections",
                    listen_addr
                );
                Some(sock)
            }
            Err(e) if tcp_socket.is_some() => {
                // If we listen over TCP, it's not a fatal error if we can't listen over uTP.
                tracing::error!("Error listening on UDP {listen_addr:?}: {e:#}");
                None
            }
            Err(e) => return Err(e),
        }
    } else {
        None
    };

    Ok(Listening {
        tcp_socket,
        utp_socket,
        addr: listen_addr,
    })
}

/// A uTP socket over a datagram socket `factory` binds for uTP on
/// `listen_addr`, asking for a receive buffer of `receive_buffer` bytes.
pub(crate) async fn bind_utp(
    factory: &dyn SocketFactory,
    listen_addr: SocketAddr,
    utp_opts: weaver_utp::SocketOpts,
    receive_buffer: usize,
    connection_slots: Option<&crate::connection_slots::ConnectionSlots>,
) -> anyhow::Result<UtpSocket> {
    let transport = factory
        .bind_utp(listen_addr, receive_buffer)
        .await
        .context("error binding UDP socket")?;
    let sock = weaver_utp::UtpSocket::new_with_opts_and_init(
        DatagramTransport(transport),
        Default::default(),
        utp_opts,
        |socket| {
            if let Some(slots) = connection_slots {
                slots.register_utp(&UtpSocket(Arc::clone(socket)));
            }
        },
    )?;
    Ok(UtpSocket(sock))
}

pub(crate) trait Accept {
    const KIND: ConnectionKind;

    async fn accept(
        &self,
    ) -> anyhow::Result<(
        SocketAddr,
        (
            impl AsyncReadVectored + Send + 'static,
            impl AsyncWrite + Unpin + Send + 'static,
        ),
    )>;
}

impl Accept for Box<dyn StreamListener + '_> {
    const KIND: ConnectionKind = ConnectionKind::Tcp;
    async fn accept(
        &self,
    ) -> anyhow::Result<(
        SocketAddr,
        (
            impl AsyncReadVectored + Send + 'static,
            impl AsyncWrite + Unpin + Send + 'static,
        ),
    )> {
        let (addr, read, write) = StreamListener::accept(self.as_ref())
            .await
            .context("error accepting TCP")?;
        Ok((addr, (read, write)))
    }
}

impl Accept for UtpSocket {
    const KIND: ConnectionKind = ConnectionKind::Utp;
    async fn accept(
        &self,
    ) -> anyhow::Result<(
        SocketAddr,
        (
            impl AsyncReadVectored + Send + 'static,
            impl AsyncWrite + Unpin + Send + 'static,
        ),
    )> {
        let stream = self.accept().await.context("error accepting uTP")?;
        let addr = stream.remote_addr();
        let (read, write) = stream.split();
        Ok((addr, (read, write)))
    }
}
