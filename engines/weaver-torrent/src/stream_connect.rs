// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use crate::core::hash_id::Id20;
use anyhow::bail;
use futures::TryFutureExt;
use librqbit_dualstack_sockets::ConnectOpts;
use librqbit_utp::BindDevice;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use crate::{
    Error, PeerConnectionOptions, Result,
    meter::Meter,
    mse::{self, EncryptionPolicy},
    network::{SocketFactory, UtpSocket},
    peer_connection::with_timeout,
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
};

#[derive(Debug, Clone, Copy, Serialize)]
pub enum ConnectionKind {
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "utp")]
    Utp,
}

impl std::fmt::Display for ConnectionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionKind::Tcp => f.write_str("tcp"),
            ConnectionKind::Utp => f.write_str("uTP"),
        }
    }
}

/// The transports an outgoing connection to a peer may go over.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transports {
    /// TCP first, and uTP too when TCP has not connected within a second.
    #[default]
    Any,
    /// uTP alone: a holepunch opened the path to the peer, for UDP only
    /// (libtorrent `bt_peer_connection::on_holepunch` marks the peer
    /// `supports_utp`, and `torrent::connect_to_peer` then dials it through
    /// the uTP socket manager).
    Utp,
}

pub struct ConnectionOptions {
    // TCP outgoing connections are enabled by default
    pub enable_tcp: bool,
    pub peer_opts: Option<PeerConnectionOptions>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            enable_tcp: true,
            peer_opts: None,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub(crate) struct StreamConnectorArgs {
    pub enable_tcp: bool,
    pub utp_socket: Option<UtpSocket>,
    pub bind_device: Option<BindDevice>,
    pub ipv4_only: bool,
    pub socket_factory: Option<Arc<dyn SocketFactory>>,
    pub encryption: EncryptionPolicy,
}

gen_stats!(SingleStatAtomic SingleStatSnapshot, [
    attempts u64,
    successes u64,
    errors u64
], []);
gen_stats!(PerFamilyAtomic PerFamilySnapshot, [], [
    v4 SingleStatAtomic SingleStatSnapshot,
    v6 SingleStatAtomic SingleStatSnapshot
]);
// Outgoing encrypted handshakes: how many were tried, how many ended in a
// fully encrypted or header-only stream, how many failed, and how many of
// those failures were retried in plaintext.
gen_stats!(EncryptionStatsAtomic EncryptionStatsSnapshot, [
    attempts u64,
    rc4 u64,
    header_only u64,
    failures u64,
    plaintext_fallbacks u64
], []);
gen_stats!(ConnectStatsAtomic ConnectStatsSnapshot, [], [
    tcp PerFamilyAtomic PerFamilySnapshot,
    utp PerFamilyAtomic PerFamilySnapshot,
    encryption EncryptionStatsAtomic EncryptionStatsSnapshot
]);

/// An outgoing TCP connection: a kernel socket, or streams from a `SocketFactory`.
enum TcpConn {
    Kernel(tokio::net::TcpStream),
    Custom(BoxAsyncReadVectored, BoxAsyncWrite),
}

#[derive(Debug)]
pub(crate) struct StreamConnector {
    enable_tcp: bool,
    bind_device: Option<BindDevice>,
    utp_socket: Option<UtpSocket>,
    stats: ConnectStatsAtomic,
    ipv4_only: bool,
    socket_factory: Option<Arc<dyn SocketFactory>>,
    encryption: EncryptionPolicy,
}

impl StreamConnector {
    pub async fn new(config: StreamConnectorArgs) -> anyhow::Result<Self> {
        if !config.enable_tcp && config.utp_socket.is_none() {
            bail!("no way to connect to peers, enable TCP or uTP")
        }

        Ok(Self {
            enable_tcp: config.enable_tcp,
            utp_socket: config.utp_socket,
            bind_device: config.bind_device,
            stats: Default::default(),
            ipv4_only: config.ipv4_only,
            socket_factory: config.socket_factory,
            encryption: config.encryption,
        })
    }

    pub fn encryption(&self) -> EncryptionPolicy {
        self.encryption
    }

    /// Connect to a peer over `transports` and send it our BitTorrent
    /// `handshake`, through the encrypted handshake when the policy asks for
    /// it. The returned halves carry the peer's reply, and the meter counts
    /// every byte they move, the encryption handshake and ours included.
    pub async fn connect_and_send_handshake(
        &self,
        addr: SocketAddr,
        transports: Transports,
        info_hash: &Id20,
        handshake: &[u8],
        connect_timeout: Duration,
        rw_timeout: Duration,
    ) -> Result<(ConnectionKind, BoxAsyncReadVectored, BoxAsyncWrite, Meter)> {
        let connect = || async {
            let (kind, r, w) = with_timeout(
                "connecting",
                connect_timeout,
                self.connect(addr, transports),
            )
            .await?;
            let (meter, r, w) = Meter::wrap(r, w);
            Ok::<_, Error>((kind, r, w, meter))
        };
        let send_plaintext = |kind, r, mut w: BoxAsyncWrite, meter| async move {
            with_timeout(
                "writing",
                rw_timeout,
                w.write_all(handshake).map_err(Error::WriteHandshake),
            )
            .await?;
            Ok((kind, r, w, meter))
        };

        if self.encryption == EncryptionPolicy::Disabled {
            let (kind, r, w, meter) = connect().await?;
            return send_plaintext(kind, r, w, meter).await;
        }

        let stats = &self.stats.encryption;
        let (kind, r, w, meter) = connect().await?;
        stats.attempts(1);
        let offer = mse::outgoing_offer(self.encryption);
        let encrypted = with_timeout("encryption handshake", rw_timeout, async {
            Ok(mse::initiate(r, w, info_hash, offer, handshake).await?)
        })
        .await;
        let err = match encrypted {
            Ok(e) => {
                if e.method == mse::CRYPTO_RC4 {
                    stats.rc4(1);
                } else {
                    stats.header_only(1);
                }
                return Ok((kind, e.reader, e.writer, meter));
            }
            Err(e) => e,
        };
        stats.failures(1);
        if self.encryption == EncryptionPolicy::Require {
            return Err(err);
        }
        debug!(
            ?addr,
            "encrypted handshake failed, retrying in plaintext: {err:#}"
        );
        stats.plaintext_fallbacks(1);
        let (kind, r, w, meter) = connect().await?;
        send_plaintext(kind, r, w, meter).await
    }

    fn get_stat(&self, kind: ConnectionKind, is_v6: bool) -> &SingleStatAtomic {
        let stat = match kind {
            ConnectionKind::Tcp => &self.stats.tcp,
            ConnectionKind::Utp => &self.stats.utp,
        };
        if is_v6 { &stat.v6 } else { &stat.v4 }
    }

    async fn with_stat<R, E>(
        &self,
        kind: ConnectionKind,
        is_v6: bool,
        fut: impl Future<Output = std::result::Result<R, E>>,
    ) -> std::result::Result<R, E> {
        let stat = self.get_stat(kind, is_v6);
        stat.attempts(1);
        fut.await
            .inspect(|_| stat.successes(1))
            .inspect_err(|_| stat.errors(1))
    }

    async fn tcp_connect(
        &self,
        addr: SocketAddr,
    ) -> librqbit_dualstack_sockets::Result<tokio::net::TcpStream> {
        self.with_stat(
            ConnectionKind::Tcp,
            addr.is_ipv6(),
            librqbit_dualstack_sockets::tcp_connect(
                addr,
                ConnectOpts {
                    // Setting source port doesn't work with cloudflare warp on linux
                    // source_port: self.tcp_source_port,
                    source_port: None,
                    bind_device: self.bind_device.as_ref(),
                },
            ),
        )
        .await
    }

    pub fn stats(&self) -> &ConnectStatsAtomic {
        &self.stats
    }

    /// Connect to the peer at `addr` over `transports`.
    pub async fn connect(
        &self,
        addr: SocketAddr,
        transports: Transports,
    ) -> Result<(ConnectionKind, BoxAsyncReadVectored, BoxAsyncWrite)> {
        if addr.port() == 0 {
            return Err(Error::Anyhow(anyhow::anyhow!(
                "invalid peer address (port 0): {}",
                addr
            )));
        }

        if self.ipv4_only && addr.is_ipv6() {
            return Err(Error::Anyhow(anyhow::anyhow!(
                "ipv6 disabled, skipping connection to {}",
                addr
            )));
        }

        // Try to connect over TCP first, when it may be used. If in 1 second
        // we haven't connected, try uTP also (if configured). Whoever
        // connects first wins.
        let tcp = self.enable_tcp && transports == Transports::Any;
        let tcp_connect = async {
            if !tcp {
                return Ok(None);
            }
            let conn = match self.socket_factory.as_ref() {
                Some(factory) => {
                    let (r, w) = self
                        .with_stat(
                            ConnectionKind::Tcp,
                            addr.is_ipv6(),
                            factory.connect_tcp(addr),
                        )
                        .await
                        .map_err(librqbit_dualstack_sockets::Error::Connect)?;
                    TcpConn::Custom(r, w)
                }
                None => TcpConn::Kernel(self.tcp_connect(addr).await?),
            };
            debug!(?addr, "connected over TCP");
            Ok::<_, librqbit_dualstack_sockets::Error>(Some(conn))
        };

        let tcp_failed_notify = tokio::sync::Notify::new();

        let utp_connect = async {
            let sock = match self.utp_socket.as_ref() {
                Some(sock) => sock,
                None => return Ok(None),
            };

            // Give TCP priority as it's more mature and simpler.
            if tcp {
                // wait until either 1 second has passed or TCP failed.
                tokio::select! {
                    _ = tcp_failed_notify.notified() => {},
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }

            let conn = self
                .with_stat(ConnectionKind::Utp, addr.is_ipv6(), sock.connect(addr))
                .await?;

            debug!(?addr, "connected over uTP");
            Ok(Some(conn))
        };

        tokio::pin!(tcp_connect);
        tokio::pin!(utp_connect);

        let mut tcp_err: Option<Option<librqbit_dualstack_sockets::Error>> = None;
        let mut utp_err: Option<Option<librqbit_utp::Error>> = None;

        // wait until all fail, or one succeeds.
        loop {
            if let (Some(tcp), Some(utp)) = (tcp_err.as_mut(), utp_err.as_mut()) {
                match (tcp.take(), utp.take()) {
                    (Some(tcp), Some(utp)) => return Err(Error::Connect { tcp, utp }),
                    (Some(tcp), None) => return Err(Error::TcpConnect(tcp)),
                    (None, Some(utp)) => return Err(Error::UtpConnect(utp)),
                    (None, None) => return Err(Error::ConnectDisabled),
                }
            }
            tokio::select! {
                tcp_res = &mut tcp_connect, if tcp_err.is_none() => {
                    match tcp_res {
                        Ok(Some(TcpConn::Kernel(stream))) => {
                            let (r, w) = stream.into_split();
                            return Ok((ConnectionKind::Tcp, Box::new(r), Box::new(w)));
                        },
                        Ok(Some(TcpConn::Custom(r, w))) => {
                            return Ok((ConnectionKind::Tcp, r, w));
                        },
                        Ok(None) => {
                            tcp_err = Some(None);
                            tcp_failed_notify.notify_waiters();
                        }
                        Err(e) => {
                            tcp_err = Some(Some(e));
                            tcp_failed_notify.notify_waiters();
                        }
                    }
                },
                utp_res = &mut utp_connect, if utp_err.is_none() => {
                    match utp_res {
                        Ok(Some(stream)) => {
                            let (r, w) = stream.split();
                            return Ok((ConnectionKind::Utp, Box::new(r), Box::new(w)));
                        },
                        Ok(None) => {
                            utp_err = Some(None);
                        }
                        Err(e) => {
                            utp_err = Some(Some(e));
                        }
                    }
                },
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use librqbit_utp::UtpSocketUdp;

    use super::*;

    /// A connector that may use TCP, and uTP when `utp` holds.
    async fn connector(utp: bool) -> StreamConnector {
        let utp_socket = match utp {
            true => Some(UtpSocket::Udp(
                UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into())
                    .await
                    .unwrap(),
            )),
            false => None,
        };
        StreamConnector::new(StreamConnectorArgs {
            enable_tcp: true,
            utp_socket,
            ..Default::default()
        })
        .await
        .unwrap()
    }

    fn tcp_attempts(connector: &StreamConnector) -> u64 {
        connector.stats().tcp.snapshot().v4.attempts
    }

    // libtorrent `bt_peer_connection::on_holepunch`, `connect`, and
    // `torrent::connect_to_peer`: a peer a holepunch introduced is dialed
    // through the uTP socket alone, with no TCP attempt before it.
    #[tokio::test]
    async fn a_utp_only_connection_dials_utp_and_never_tcp() {
        let connector = connector(true).await;
        let theirs = UtpSocketUdp::new_udp((Ipv4Addr::LOCALHOST, 0).into())
            .await
            .unwrap();
        let (accepted, connected) = tokio::join!(
            theirs.accept(),
            connector.connect(theirs.bind_addr(), Transports::Utp)
        );
        accepted.unwrap();
        let (kind, _r, _w) = connected.unwrap();
        assert!(matches!(kind, ConnectionKind::Utp));
        assert_eq!(tcp_attempts(&connector), 0);
        assert_eq!(connector.stats().utp.snapshot().v4.successes, 1);
    }

    // Without uTP there is no way to a peer that may be reached over uTP
    // alone: nothing is dialed.
    #[tokio::test]
    async fn a_utp_only_connection_without_utp_dials_nothing() {
        let connector = connector(false).await;
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let res = connector
            .connect(listener.local_addr().unwrap(), Transports::Utp)
            .await;
        assert!(matches!(res, Err(Error::ConnectDisabled)));
        assert_eq!(tcp_attempts(&connector), 0);
    }
}
