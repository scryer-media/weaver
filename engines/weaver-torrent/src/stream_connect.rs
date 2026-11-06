// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use crate::core::hash_id::Id20;
use anyhow::bail;
use futures::TryFutureExt;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use crate::{
    Error, PeerConnectionOptions, Result,
    meter::Meter,
    mse::{self, Encryption, EncryptionPolicy},
    network::{SocketFactory, UtpSocket},
    peer_connection::with_timeout,
    ssl::SslTorrent,
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
};

/// What a connection to a peer is made under once its stream is open.
#[derive(Clone, Copy)]
pub(crate) enum Guard<'a> {
    /// Message stream encryption, as the policy says.
    Mse(EncryptionPolicy),
    /// TLS under an SSL torrent's CA, with our identity under it.
    Tls(&'a SslTorrent),
}

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
    /// `None` uses an existing listening uTP socket. `Some(true)` also
    /// binds an outgoing socket through the session's factory when needed,
    /// and fails the session on a bind failure only when TCP is disabled;
    /// `Some(false)` disables outgoing uTP without changing its listener.
    pub enable_utp: Option<bool>,
    pub peer_opts: Option<PeerConnectionOptions>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            enable_tcp: true,
            enable_utp: None,
            peer_opts: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StreamConnectorArgs {
    pub enable_tcp: bool,
    pub utp_socket: Option<UtpSocket>,
    pub ipv4_only: bool,
    pub socket_factory: Arc<dyn SocketFactory>,
    pub encryption: EncryptionPolicy,
}

#[cfg(test)]
impl StreamConnectorArgs {
    /// A connector's arguments over `socket_factory`: TCP, no uTP, both
    /// families, no encryption.
    pub(crate) fn over(socket_factory: Arc<dyn SocketFactory>) -> Self {
        Self {
            enable_tcp: true,
            utp_socket: None,
            ipv4_only: false,
            socket_factory,
            encryption: EncryptionPolicy::Disabled,
        }
    }
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
// Outgoing handshakes: how many encrypted ones were tried, how many of
// those ended in a fully encrypted or header-only stream, how many failed,
// and how many plaintext ones were sent.
gen_stats!(EncryptionStatsAtomic EncryptionStatsSnapshot, [
    attempts u64,
    rc4 u64,
    header_only u64,
    failures u64,
    plaintext u64
], []);
gen_stats!(ConnectStatsAtomic ConnectStatsSnapshot, [], [
    tcp PerFamilyAtomic PerFamilySnapshot,
    utp PerFamilyAtomic PerFamilySnapshot,
    encryption EncryptionStatsAtomic EncryptionStatsSnapshot
]);

#[derive(Debug)]
pub(crate) struct StreamConnector {
    enable_tcp: bool,
    utp_socket: Option<UtpSocket>,
    stats: ConnectStatsAtomic,
    ipv4_only: bool,
    socket_factory: Arc<dyn SocketFactory>,
    // The session's encryption, as `policy_code` writes it: a change of the
    // settings moves it while the session runs.
    encryption: AtomicU8,
}

// An encryption policy as one byte, and back.
const fn policy_code(policy: EncryptionPolicy) -> u8 {
    match policy {
        EncryptionPolicy::Disabled => 0,
        EncryptionPolicy::Prefer => 1,
        EncryptionPolicy::Require => 2,
    }
}

const fn policy_of(code: u8) -> EncryptionPolicy {
    match code {
        1 => EncryptionPolicy::Prefer,
        2 => EncryptionPolicy::Require,
        _ => EncryptionPolicy::Disabled,
    }
}

impl StreamConnector {
    pub async fn new(config: StreamConnectorArgs) -> anyhow::Result<Self> {
        if !config.enable_tcp && config.utp_socket.is_none() {
            bail!("no way to connect to peers, enable TCP or uTP")
        }

        Ok(Self {
            enable_tcp: config.enable_tcp,
            utp_socket: config.utp_socket,
            stats: Default::default(),
            ipv4_only: config.ipv4_only,
            socket_factory: config.socket_factory,
            encryption: AtomicU8::new(policy_code(config.encryption)),
        })
    }

    /// The session's encryption, which a connection is made under unless
    /// its torrent names its own.
    pub fn encryption(&self) -> EncryptionPolicy {
        policy_of(self.encryption.load(Ordering::Relaxed))
    }

    /// Make every connection from here on under `policy`, and say so in
    /// every announce: a connection already made keeps what it negotiated.
    pub fn set_encryption(&self, policy: EncryptionPolicy) {
        self.encryption
            .store(policy_code(policy), Ordering::Relaxed);
    }

    /// Connect to a peer over `transports` and send it our BitTorrent
    /// `handshake`: inside TLS under [`Guard::Tls`], else inside the
    /// encrypted handshake when the [`Guard::Mse`] policy says so.
    /// Under [`EncryptionPolicy::Prefer`] `encrypt` says which, asked once the
    /// transport is open (libtorrent `bt_peer_connection::on_connected`
    /// chooses there, so a peer we never reach is never asked). An encrypted
    /// handshake that fails ends the connection: the next one to the peer
    /// tries what `encrypt` says then. The returned halves carry the peer's
    /// reply, the meter counts every byte they move, the encryption handshake
    /// and ours included, and the [`Encryption`] says how the handshake went.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect_and_send_handshake(
        &self,
        addr: SocketAddr,
        transports: Transports,
        guard: Guard<'_>,
        info_hash: &Id20,
        handshake: &[u8],
        encrypt: impl FnOnce() -> bool,
        connect_timeout: Duration,
        rw_timeout: Duration,
    ) -> Result<(
        ConnectionKind,
        BoxAsyncReadVectored,
        BoxAsyncWrite,
        Meter,
        Encryption,
    )> {
        let (kind, r, w) = with_timeout(
            "connecting",
            connect_timeout,
            self.connect(addr, transports),
        )
        .await?;
        let (meter, r, mut w) = Meter::wrap(r, w);

        let policy = match guard {
            Guard::Mse(policy) => policy,
            Guard::Tls(ssl) => {
                let (r, mut w) = with_timeout("TLS handshake", rw_timeout, async {
                    ssl.connect(r, w)
                        .await
                        .map_err(|e| Error::Anyhow(anyhow::Error::from(e).context("TLS handshake")))
                })
                .await
                .inspect_err(|e| debug!(?addr, "TLS handshake failed: {e:#}"))?;
                with_timeout(
                    "writing",
                    rw_timeout,
                    w.write_all(handshake).map_err(Error::WriteHandshake),
                )
                .await?;
                return Ok((kind, r, w, meter, Encryption::None));
            }
        };

        let stats = &self.stats.encryption;
        let encrypted = match policy {
            EncryptionPolicy::Disabled => false,
            EncryptionPolicy::Prefer => encrypt(),
            EncryptionPolicy::Require => true,
        };
        if !encrypted {
            stats.plaintext(1);
            with_timeout(
                "writing",
                rw_timeout,
                w.write_all(handshake).map_err(Error::WriteHandshake),
            )
            .await?;
            return Ok((kind, r, w, meter, Encryption::None));
        }

        stats.attempts(1);
        let offer = mse::outgoing_offer(policy);
        let established = with_timeout("encryption handshake", rw_timeout, async {
            Ok(mse::initiate(r, w, info_hash, offer, handshake).await?)
        })
        .await
        .inspect_err(|e| {
            stats.failures(1);
            debug!(?addr, "encrypted handshake failed: {e:#}");
        })?;
        let encryption = established.encryption;
        if encryption == Encryption::Rc4 {
            stats.rc4(1);
        } else {
            stats.header_only(1);
        }
        Ok((
            kind,
            established.reader,
            established.writer,
            meter,
            encryption,
        ))
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

    pub fn stats(&self) -> &ConnectStatsAtomic {
        &self.stats
    }

    /// Connect to the peer at `addr` over `transports`.
    pub async fn connect(
        &self,
        addr: SocketAddr,
        transports: Transports,
    ) -> Result<(ConnectionKind, BoxAsyncReadVectored, BoxAsyncWrite)> {
        let addr = crate::network::canonical_socket_addr(addr);
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
            let conn = self
                .with_stat(
                    ConnectionKind::Tcp,
                    addr.is_ipv6(),
                    self.socket_factory.connect_tcp(addr),
                )
                .await
                .map_err(librqbit_dualstack_sockets::Error::Connect)?;
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
        let mut utp_err: Option<Option<weaver_utp::Error>> = None;

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
                        Ok(Some((r, w))) => {
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
mod mapped_endpoint_tests;

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::peer_info_reader::tests::Dialed;

    async fn loopback_utp() -> UtpSocket {
        crate::listen::bind_utp(
            crate::test_support::socket_factory().as_ref(),
            (Ipv4Addr::LOCALHOST, 0).into(),
            Default::default(),
            crate::udp_options::STREAM_WINDOW,
            None,
        )
        .await
        .unwrap()
    }

    /// A connector that may use TCP, and uTP when `utp` holds.
    async fn connector(utp: bool) -> StreamConnector {
        let sockets = crate::test_support::socket_factory();
        let utp_socket = match utp {
            true => Some(
                crate::listen::bind_utp(
                    sockets.as_ref(),
                    (Ipv4Addr::LOCALHOST, 0).into(),
                    Default::default(),
                    crate::udp_options::STREAM_WINDOW,
                    None,
                )
                .await
                .unwrap(),
            ),
            false => None,
        };
        StreamConnector::new(StreamConnectorArgs {
            enable_tcp: true,
            utp_socket,
            ipv4_only: false,
            socket_factory: sockets,
            encryption: EncryptionPolicy::Disabled,
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
        let theirs = loopback_utp().await;
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

    const HANDSHAKE: &[u8] = b"\x13BitTorrent protocol, and the rest";

    /// A connector under `encryption` whose dials reach the test as the far
    /// end of an in-memory stream.
    async fn dialing(
        encryption: EncryptionPolicy,
    ) -> (
        StreamConnector,
        tokio::sync::mpsc::UnboundedReceiver<(SocketAddr, tokio::io::DuplexStream)>,
    ) {
        let (tx, dialed) = tokio::sync::mpsc::unbounded_channel();
        let connector = StreamConnector::new(StreamConnectorArgs {
            enable_tcp: true,
            utp_socket: None,
            ipv4_only: false,
            socket_factory: Arc::new(Dialed(tx)),
            encryption,
        })
        .await
        .unwrap();
        (connector, dialed)
    }

    async fn send_handshake(
        connector: &StreamConnector,
        encrypt: impl FnOnce() -> bool,
    ) -> Result<bool> {
        connector
            .connect_and_send_handshake(
                (Ipv4Addr::LOCALHOST, 6881).into(),
                Transports::Any,
                Guard::Mse(connector.encryption()),
                &Id20::new([7; 20]),
                HANDSHAKE,
                encrypt,
                Duration::MAX,
                Duration::MAX,
            )
            .await
            .map(|(.., encryption)| encryption.is_encrypted())
    }

    // libtorrent `bt_peer_connection::on_connected`: where encryption is
    // preferred, the peer's `pe_support` says which handshake the
    // connection sends, plaintext here, over the one connection.
    #[tokio::test(start_paused = true)]
    async fn a_preferring_connection_sends_the_handshake_it_is_told_to() {
        use tokio::io::AsyncReadExt;
        let (connector, mut dialed) = dialing(EncryptionPolicy::Prefer).await;
        let encrypted = send_handshake(&connector, || false).await.unwrap();
        assert!(!encrypted);
        let (_, mut theirs) = dialed.recv().await.unwrap();
        let mut got = vec![0u8; HANDSHAKE.len()];
        theirs.read_exact(&mut got).await.unwrap();
        assert_eq!(got, HANDSHAKE);
        assert!(dialed.try_recv().is_err());
        let stats = connector.stats().encryption.snapshot();
        assert_eq!((stats.plaintext, stats.attempts), (1, 0), "{stats:?}");
    }

    // libtorrent `bt_peer_connection::on_connected`: an encrypted handshake
    // the peer refuses ends the connection; the next connection, not this
    // one, tries plaintext.
    #[tokio::test(start_paused = true)]
    async fn a_refused_encrypted_handshake_ends_the_connection_and_opens_no_other() {
        let (connector, mut dialed) = dialing(EncryptionPolicy::Prefer).await;
        let refuse = async {
            let (_, theirs) = dialed.recv().await.unwrap();
            drop(theirs);
        };
        let (sent, ()) = tokio::join!(send_handshake(&connector, || true), refuse);
        assert!(matches!(sent, Err(Error::Mse(_))), "{sent:?}");
        assert!(dialed.try_recv().is_err());
        assert_eq!(tcp_attempts(&connector), 1);
        let stats = connector.stats().encryption.snapshot();
        assert_eq!(
            (stats.attempts, stats.failures, stats.plaintext),
            (1, 1, 0),
            "{stats:?}"
        );
    }

    // libtorrent `bt_peer_connection::on_connected` reads `pe_support`
    // only under `pe_enabled`: `pe_forced` sends the encrypted handshake
    // and `pe_disabled` the plaintext one, whatever the peer showed.
    #[tokio::test(start_paused = true)]
    async fn require_and_disabled_never_ask_which_handshake() {
        let asked = || panic!("asked which handshake");
        let (connector, mut dialed) = dialing(EncryptionPolicy::Require).await;
        let refuse = async {
            let (_, theirs) = dialed.recv().await.unwrap();
            drop(theirs);
        };
        let (sent, ()) = tokio::join!(send_handshake(&connector, asked), refuse);
        assert!(matches!(sent, Err(Error::Mse(_))), "{sent:?}");
        assert_eq!(connector.stats().encryption.snapshot().attempts, 1);

        let (connector, _dialed) = dialing(EncryptionPolicy::Disabled).await;
        assert!(!send_handshake(&connector, asked).await.unwrap());
        assert_eq!(connector.stats().encryption.snapshot().plaintext, 1);
    }

    // A policy set while the connector runs holds for the next connection
    // it makes: plaintext under the old one, the encrypted handshake under
    // the new.
    #[tokio::test(start_paused = true)]
    async fn the_next_connection_is_made_under_the_encryption_set_last() {
        let (connector, mut dialed) = dialing(EncryptionPolicy::Disabled).await;
        assert!(!send_handshake(&connector, || false).await.unwrap());
        let _plain = dialed.recv().await.unwrap();

        connector.set_encryption(EncryptionPolicy::Require);
        assert_eq!(connector.encryption(), EncryptionPolicy::Require);
        let refuse = async {
            let (_, theirs) = dialed.recv().await.unwrap();
            drop(theirs);
        };
        let asked = || panic!("asked which handshake");
        let (sent, ()) = tokio::join!(send_handshake(&connector, asked), refuse);
        assert!(matches!(sent, Err(Error::Mse(_))), "{sent:?}");
        let stats = connector.stats().encryption.snapshot();
        assert_eq!((stats.plaintext, stats.attempts), (1, 1), "{stats:?}");
    }

    // libtorrent `bt_peer_connection::on_connected` runs once the transport
    // is open: a peer we never reach is never asked, so its memory stays.
    #[tokio::test(start_paused = true)]
    async fn a_peer_never_reached_is_never_asked() {
        let (connector, dialed) = dialing(EncryptionPolicy::Prefer).await;
        drop(dialed);
        let sent = send_handshake(&connector, || panic!("asked which handshake")).await;
        assert!(matches!(sent, Err(Error::TcpConnect(_))), "{sent:?}");
    }
}
