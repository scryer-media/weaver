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
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};

use crate::core::hash_id::Id20;
use anyhow::bail;
use futures::TryFutureExt;
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

mod dial;
mod kind;
pub(crate) use dial::UtpRebind;
use dial::{DialPlan, PeerDialer, Tally, TcpDialer, UtpDialer};
pub use kind::{ConnectionKind, Names};

/// An open connection to a peer, as its transport hands it over, dialed or
/// accepted: the bytes each way, the remote end, and the thread the
/// transport runs on when it has one of its own. What the connection
/// guarantees is its kind's capabilities. The writing half is `W`: the
/// transport's own, then whatever the connection's guard made of it.
pub(crate) struct Link<W = BoxAsyncWrite> {
    pub kind: ConnectionKind,
    pub remote: SocketAddr,
    pub read: BoxAsyncReadVectored,
    pub write: W,
    /// The thread the transport runs on, when it has one of its own (a uTP
    /// stream's): the connection's task moves there.
    pub home: Option<tokio::runtime::Handle>,
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
    pub utp_rebind: Option<UtpRebind>,
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
            utp_rebind: None,
            ipv4_only: false,
            socket_factory,
            encryption: EncryptionPolicy::Disabled,
        }
    }
}

// Outgoing connection attempts over one transport and family, and how each
// ended: every attempt ends in exactly one of connected (`successes`),
// refused by the peer, timed out, or failed otherwise (`errors`), an
// attempt dropped because the other transport connected first among them.
gen_stats!(SingleStatAtomic SingleStatSnapshot, [
    attempts u64,
    successes u64,
    refused u64,
    timeouts u64,
    errors u64
], []);

// How a failed attempt ended, as its error says.
enum Failure {
    Refused,
    TimedOut,
    Other,
}

trait Failed {
    fn failure(&self) -> Failure;
}

impl Failed for std::io::Error {
    fn failure(&self) -> Failure {
        match self.kind() {
            std::io::ErrorKind::ConnectionRefused => Failure::Refused,
            std::io::ErrorKind::TimedOut => Failure::TimedOut,
            _ => Failure::Other,
        }
    }
}

impl Failed for weaver_utp::Error {
    fn failure(&self) -> Failure {
        match self {
            // A peer with no uTP listener answers the SYN with a reset.
            weaver_utp::Error::StResetReceived => Failure::Refused,
            weaver_utp::Error::ConnectTimedOut | weaver_utp::Error::TimedOut => Failure::TimedOut,
            _ => Failure::Other,
        }
    }
}

gen_stats!(PerFamilyAtomic PerFamilySnapshot, [], [
    v4 SingleStatAtomic SingleStatSnapshot,
    v6 SingleStatAtomic SingleStatSnapshot
]);
// Outgoing handshakes: how many encrypted ones were tried, how many of
// those ended in a fully encrypted or header-only stream, how many failed,
// how many were dropped before they ended, and how many plaintext ones were
// sent. Every attempt has one of the four ends.
gen_stats!(EncryptionStatsAtomic EncryptionStatsSnapshot, [
    attempts u64,
    rc4 u64,
    header_only u64,
    failures u64,
    cancelled u64,
    plaintext u64
], []);

// An encrypted handshake under way: counted as cancelled if it is dropped
// before it ends, as when its torrent is paused or removed.
struct Unended<'a>(Option<&'a EncryptionStatsAtomic>);

impl Drop for Unended<'_> {
    fn drop(&mut self) {
        if let Some(stats) = self.0.take() {
            stats.cancelled(1);
        }
    }
}
gen_stats!(ConnectStatsAtomic ConnectStatsSnapshot, [], [
    tcp PerFamilyAtomic PerFamilySnapshot,
    utp PerFamilyAtomic PerFamilySnapshot,
    encryption EncryptionStatsAtomic EncryptionStatsSnapshot
]);

#[derive(Debug)]
pub(crate) struct StreamConnector {
    tcp: TcpDialer,
    utp: UtpDialer,
    stats: ConnectStatsAtomic,
    ipv4_only: bool,
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
            tcp: TcpDialer {
                enabled: config.enable_tcp,
                sockets: Arc::clone(&config.socket_factory),
            },
            utp: UtpDialer {
                socket: tokio::sync::OnceCell::new_with(config.utp_socket),
                rebind: config.utp_rebind,
                sockets: config.socket_factory,
            },
            stats: Default::default(),
            ipv4_only: config.ipv4_only,
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
    #[cfg(test)]
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
        mse::Outbound,
        Meter,
        Encryption,
    )> {
        self.connect_link(
            addr,
            transports,
            guard,
            info_hash,
            handshake,
            encrypt,
            connect_timeout,
            rw_timeout,
        )
        .await
        .map(|(link, meter, encryption)| (link.kind, link.read, link.write, meter, encryption))
    }

    /// As [`Self::connect_and_send_handshake`], the connection as the
    /// [`Link`] it is: with the peer's address and the thread the
    /// connection's transport runs on when it has one of its own, a uTP
    /// stream's.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn connect_link(
        &self,
        addr: SocketAddr,
        transports: Transports,
        guard: Guard<'_>,
        info_hash: &Id20,
        handshake: &[u8],
        encrypt: impl FnOnce() -> bool,
        connect_timeout: Duration,
        rw_timeout: Duration,
    ) -> Result<(Link<mse::Outbound>, Meter, Encryption)> {
        // The attempts still running when the time runs out end as timed
        // out: the flag is set before the dial, and with it its attempts, is
        // dropped.
        let expired = AtomicBool::new(false);
        let Link {
            kind,
            remote,
            read: r,
            write: w,
            home,
        } = {
            let dial = self.dial(addr, transports, &expired);
            tokio::pin!(dial);
            match tokio::time::timeout(connect_timeout, &mut dial).await {
                Ok(dialed) => dialed?,
                Err(_) => {
                    expired.store(true, Ordering::Relaxed);
                    return Err(Error::ConnectTimeout("connecting"));
                }
            }
        };
        let (meter, r, mut w) = Meter::wrap(r, w);
        let link = |read, write| Link {
            kind,
            remote,
            read,
            write,
            home,
        };

        let policy = match guard {
            Guard::Mse(policy) => policy,
            Guard::Tls(ssl) => {
                // The TLS handshake is part of the connect (libtorrent
                // `ssl_stream::async_connect`).
                let (r, mut w) = tokio::time::timeout(rw_timeout, ssl.connect(r, w))
                    .await
                    .map_err(|_| Error::ConnectTimeout("TLS handshake"))
                    .and_then(|tls| tls.map_err(Error::TlsConnect))
                    .inspect_err(|e| debug!(?addr, "TLS handshake failed: {e:#}"))?;
                with_timeout(
                    "writing",
                    rw_timeout,
                    w.write_all(handshake).map_err(Error::WriteHandshake),
                )
                .await?;
                return Ok((link(r, w.into()), meter, Encryption::None));
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
            return Ok((link(r, w.into()), meter, Encryption::None));
        }

        stats.attempts(1);
        let mut unended = Unended(Some(stats));
        let offer = mse::outgoing_offer(policy);
        let established = with_timeout("encryption handshake", rw_timeout, async {
            Ok(mse::initiate(r, w, info_hash, offer, handshake).await?)
        })
        .await;
        unended.0 = None;
        let established = established.inspect_err(|e| {
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
            link(established.reader, established.writer),
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

    pub fn stats(&self) -> &ConnectStatsAtomic {
        &self.stats
    }

    // The dialer of `kind`.
    fn dialer(&self, kind: ConnectionKind) -> &dyn PeerDialer {
        match kind {
            ConnectionKind::Tcp => &self.tcp,
            ConnectionKind::Utp => &self.utp,
        }
    }

    // Whether the outgoing uTP socket is bound.
    #[cfg(test)]
    fn has_utp(&self) -> bool {
        self.utp.ready()
    }

    /// The outgoing uTP socket a dial bound after the session started.
    pub fn rebound_utp(&self) -> Option<&UtpSocket> {
        self.utp.rebound()
    }

    /// Connect to the peer at `addr` over `transports`.
    #[cfg(test)]
    pub async fn connect(
        &self,
        addr: SocketAddr,
        transports: Transports,
    ) -> Result<(ConnectionKind, BoxAsyncReadVectored, BoxAsyncWrite)> {
        self.dial(addr, transports, &AtomicBool::new(false))
            .await
            .map(|link| (link.kind, link.read, link.write))
    }

    // Connect as `connect` does, with the thread a uTP stream runs on. An
    // attempt dropped unfinished counts as timed out when `expired` is set
    // by then, and as failed otherwise.
    async fn dial(
        &self,
        addr: SocketAddr,
        transports: Transports,
        expired: &AtomicBool,
    ) -> Result<Link> {
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

        let is_v6 = addr.is_ipv6();
        dial::race(
            DialPlan::DEFAULT,
            |kind| self.dialer(kind),
            |kind| Tally {
                stat: self.get_stat(kind, is_v6),
                expired,
            },
            addr,
            transports,
        )
        .await
    }
}

#[cfg(test)]
mod mapped_endpoint_tests;

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::network::BoxFuture;
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
            utp_rebind: None,
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

    /// The kernel's sockets on a route that says whether a TCP peer finds a
    /// socket free.
    struct Room {
        sockets: Arc<dyn SocketFactory>,
        tcp: bool,
    }

    impl SocketFactory for Room {
        fn tcp_room(&self) -> bool {
            self.tcp
        }

        fn connect_tcp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
            self.sockets.connect_tcp(addr)
        }

        fn listen_tcp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>> {
            self.sockets.listen_tcp(addr)
        }

        fn bind_udp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>> {
            self.sockets.bind_udp(addr)
        }

        fn bind_utp<'a>(
            &'a self,
            addr: SocketAddr,
            receive_buffer: usize,
        ) -> BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>> {
            self.sockets.bind_utp(addr, receive_buffer)
        }
    }

    /// A connector that may use TCP and uTP, on a route with a TCP socket
    /// free when `tcp` holds.
    async fn connector_with_room(tcp: bool) -> StreamConnector {
        let sockets: Arc<dyn SocketFactory> = Arc::new(Room {
            sockets: crate::test_support::socket_factory(),
            tcp,
        });
        let utp_socket = crate::listen::bind_utp(
            sockets.as_ref(),
            (Ipv4Addr::LOCALHOST, 0).into(),
            Default::default(),
            crate::udp_options::STREAM_WINDOW,
            None,
        )
        .await
        .unwrap();
        StreamConnector::new(StreamConnectorArgs {
            enable_tcp: true,
            utp_socket: Some(utp_socket),
            utp_rebind: None,
            ipv4_only: false,
            socket_factory: sockets,
            encryption: EncryptionPolicy::Disabled,
        })
        .await
        .unwrap()
    }

    // A route whose TCP peer share is full sends a peer that may use either
    // transport straight to uTP, with no TCP attempt and no wait for one.
    #[tokio::test]
    async fn a_dial_with_no_tcp_socket_free_goes_straight_to_utp() {
        let connector = connector_with_room(false).await;
        let theirs = loopback_utp().await;
        let (accepted, connected) = tokio::join!(
            theirs.accept(),
            connector.connect(theirs.bind_addr(), Transports::Any)
        );
        accepted.unwrap();
        let (kind, _r, _w) = connected.unwrap();
        assert!(matches!(kind, ConnectionKind::Utp));
        assert_eq!(tcp_attempts(&connector), 0);
    }

    /// The kernel's sockets on a route whose first uTP bind fails, as one
    /// does on a tunnel whose sockets are all held.
    struct FirstBindFails {
        sockets: Arc<dyn SocketFactory>,
        binds: std::sync::atomic::AtomicUsize,
        room: std::sync::atomic::AtomicBool,
    }

    impl SocketFactory for FirstBindFails {
        fn datagram_room(&self) -> bool {
            self.room.load(Ordering::Relaxed)
        }

        fn connect_tcp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
            self.sockets.connect_tcp(addr)
        }

        fn listen_tcp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>> {
            self.sockets.listen_tcp(addr)
        }

        fn bind_udp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>> {
            self.sockets.bind_udp(addr)
        }

        fn bind_utp<'a>(
            &'a self,
            addr: SocketAddr,
            receive_buffer: usize,
        ) -> BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>> {
            if self.binds.fetch_add(1, Ordering::Relaxed) == 0 {
                return Box::pin(async { Err(std::io::Error::other("no socket free")) });
            }
            self.sockets.bind_utp(addr, receive_buffer)
        }
    }

    // An outgoing uTP socket whose bind failed is bound again by the next
    // dial that wants uTP, and kept for every dial after it.
    #[tokio::test]
    async fn a_failed_utp_bind_is_retried_by_the_next_dial_that_wants_utp() {
        let factory = Arc::new(FirstBindFails {
            sockets: crate::test_support::socket_factory(),
            binds: Default::default(),
            room: true.into(),
        });
        let connector = StreamConnector::new(StreamConnectorArgs {
            utp_rebind: Some(UtpRebind {
                address: (Ipv4Addr::LOCALHOST, 0).into(),
                opts: Default::default(),
                receive_buffer: crate::udp_options::STREAM_WINDOW,
                connection_slots: Arc::new(crate::connection_slots::ConnectionSlots::new(
                    8,
                    0,
                    std::num::NonZeroU32::new(10).unwrap(),
                )),
            }),
            ..StreamConnectorArgs::over(factory.clone())
        })
        .await
        .unwrap();
        let theirs = loopback_utp().await;
        let first = connector.connect(theirs.bind_addr(), Transports::Utp).await;
        assert!(matches!(first, Err(Error::ConnectDisabled)));
        assert!(connector.rebound_utp().is_none());
        for _ in 0..2 {
            let (accepted, connected) = tokio::join!(
                theirs.accept(),
                connector.connect(theirs.bind_addr(), Transports::Utp)
            );
            accepted.unwrap();
            assert!(matches!(connected.unwrap().0, ConnectionKind::Utp));
        }
        assert_eq!(factory.binds.load(Ordering::Relaxed), 2);
        assert!(connector.rebound_utp().is_some());
    }

    // A failed uTP bind is tried again only while the route has a datagram
    // socket free, and the connector reports uTP only once it is bound.
    #[tokio::test]
    async fn a_failed_utp_bind_waits_for_a_datagram_socket_free() {
        let factory = Arc::new(FirstBindFails {
            sockets: crate::test_support::socket_factory(),
            binds: Default::default(),
            room: false.into(),
        });
        let connector = StreamConnector::new(StreamConnectorArgs {
            utp_rebind: Some(UtpRebind {
                address: (Ipv4Addr::LOCALHOST, 0).into(),
                opts: Default::default(),
                receive_buffer: crate::udp_options::STREAM_WINDOW,
                connection_slots: Arc::new(crate::connection_slots::ConnectionSlots::new(
                    8,
                    0,
                    std::num::NonZeroU32::new(10).unwrap(),
                )),
            }),
            ..StreamConnectorArgs::over(factory.clone())
        })
        .await
        .unwrap();
        let theirs = loopback_utp().await;
        for _ in 0..2 {
            let refused = connector.connect(theirs.bind_addr(), Transports::Utp).await;
            assert!(matches!(refused, Err(Error::ConnectDisabled)));
        }
        assert_eq!(factory.binds.load(Ordering::Relaxed), 0);
        assert!(!connector.has_utp());

        factory.room.store(true, Ordering::Relaxed);
        // The first bind with room fails as the session's did; the next binds.
        let first = connector.connect(theirs.bind_addr(), Transports::Utp).await;
        assert!(matches!(first, Err(Error::ConnectDisabled)));
        assert!(!connector.has_utp());
        let (accepted, connected) = tokio::join!(
            theirs.accept(),
            connector.connect(theirs.bind_addr(), Transports::Utp)
        );
        accepted.unwrap();
        assert!(matches!(connected.unwrap().0, ConnectionKind::Utp));
        assert!(connector.has_utp());
        assert_eq!(factory.binds.load(Ordering::Relaxed), 2);
    }

    /// The kernel's sockets on a route with a TCP socket free whose every
    /// TCP connect fails the moment it is made.
    struct RefusesTcp(Arc<dyn SocketFactory>);

    impl SocketFactory for RefusesTcp {
        fn connect_tcp<'a>(
            &'a self,
            _: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
            Box::pin(async { Err(std::io::ErrorKind::ConnectionRefused.into()) })
        }

        fn listen_tcp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>> {
            self.0.listen_tcp(addr)
        }

        fn bind_udp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>> {
            self.0.bind_udp(addr)
        }

        fn bind_utp<'a>(
            &'a self,
            addr: SocketAddr,
            receive_buffer: usize,
        ) -> BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>> {
            self.0.bind_utp(addr, receive_buffer)
        }
    }

    // A TCP connect that fails as soon as it is made starts uTP at once,
    // in the same poll, with no wait for TCP's second.
    #[tokio::test]
    async fn a_tcp_connect_failing_on_its_first_poll_starts_utp_at_once() {
        let sockets: Arc<dyn SocketFactory> =
            Arc::new(RefusesTcp(crate::test_support::socket_factory()));
        let utp_socket = crate::listen::bind_utp(
            sockets.as_ref(),
            (Ipv4Addr::LOCALHOST, 0).into(),
            Default::default(),
            crate::udp_options::STREAM_WINDOW,
            None,
        )
        .await
        .unwrap();
        let connector = StreamConnector::new(StreamConnectorArgs {
            utp_socket: Some(utp_socket),
            ..StreamConnectorArgs::over(sockets)
        })
        .await
        .unwrap();
        let theirs = loopback_utp().await;
        let connect = connector.connect(theirs.bind_addr(), Transports::Any);
        tokio::pin!(connect);
        assert!(futures::poll!(connect.as_mut()).is_pending());
        assert_eq!(tcp_attempts(&connector), 1);
        assert_eq!(connector.stats().utp.snapshot().v4.attempts, 1);
        let (accepted, connected) = tokio::join!(theirs.accept(), connect);
        accepted.unwrap();
        assert!(matches!(connected.unwrap().0, ConnectionKind::Utp));
    }

    // Under the share, the same dial tries TCP first.
    #[tokio::test]
    async fn a_dial_with_a_tcp_socket_free_tries_tcp_first() {
        let connector = connector_with_room(true).await;
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let (kind, _r, _w) = connector
            .connect(listener.local_addr().unwrap(), Transports::Any)
            .await
            .unwrap();
        assert!(matches!(kind, ConnectionKind::Tcp));
        assert_eq!(tcp_attempts(&connector), 1);
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

    // The connector's TCP dialer hands over a link to the peer dialed, with
    // no home; only a uTP stream brings one.
    #[tokio::test(start_paused = true)]
    async fn a_tcp_link_is_to_the_peer_dialed_and_has_no_home() {
        let (connector, mut dialed) = dialing(EncryptionPolicy::Disabled).await;
        let addr: SocketAddr = (Ipv4Addr::LOCALHOST, 6881).into();
        let link = connector
            .dial(addr, Transports::Any, &AtomicBool::new(false))
            .await
            .unwrap();
        assert_eq!((link.kind, link.remote), (ConnectionKind::Tcp, addr));
        assert!(link.home.is_none());
        assert_eq!(dialed.recv().await.unwrap().0, addr);
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
            utp_rebind: None,
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

    // An encrypted handshake dropped while it waits for the peer's answer
    // has an end of its own, so the ends sum to the attempts.
    #[tokio::test(start_paused = true)]
    async fn an_encrypted_handshake_dropped_before_it_ends_is_counted_cancelled() {
        use tokio::io::AsyncReadExt;
        let (connector, mut dialed) = dialing(EncryptionPolicy::Require).await;
        // The peer reads the first byte of our key and answers nothing.
        let began = async {
            let (_, mut theirs) = dialed.recv().await.unwrap();
            let mut first = [0u8; 1];
            theirs.read_exact(&mut first).await.unwrap();
            theirs
        };
        tokio::select! {
            biased;
            sent = send_handshake(&connector, || true) => panic!("it ended: {sent:?}"),
            _theirs = began => {}
        }
        let stats = connector.stats().encryption.snapshot();
        assert_eq!(
            (
                stats.attempts,
                stats.cancelled,
                stats.failures,
                stats.rc4,
                stats.header_only
            ),
            (1, 1, 0, 0, 0),
            "{stats:?}"
        );
    }

    // libtorrent `peer_connection::disconnect` counts a close against the
    // peer only above `normal` severity. An encrypted handshake the peer
    // closes, or leaves silent until the handshake timeout, closes at
    // `normal` (`on_receive_data`, `second_tick`'s `timed_out_no_handshake`)
    // and is no failure. One whose answer holds no verification constant
    // within the padding window closes at `peer_error`
    // (`bt_peer_connection::on_receive_impl`, `invalid_encryption_constant`)
    // and a dial that is refused at `failure` (`connect_failed`): both are.
    #[tokio::test(start_paused = true)]
    async fn an_encrypted_handshake_the_peer_closes_or_leaves_silent_is_no_failure() {
        use tokio::io::AsyncWriteExt;
        let (connector, mut dialed) = dialing(EncryptionPolicy::Prefer).await;
        let info_hash = Id20::new([7; 20]);
        let handshake = |rw_timeout| {
            connector.connect_and_send_handshake(
                (Ipv4Addr::LOCALHOST, 6881).into(),
                Transports::Any,
                Guard::Mse(EncryptionPolicy::Prefer),
                &info_hash,
                HANDSHAKE,
                || true,
                Duration::MAX,
                rw_timeout,
            )
        };

        let close = async { drop(dialed.recv().await.unwrap()) };
        let (closed, ()) = tokio::join!(handshake(Duration::MAX), close);
        let closed = closed.err().unwrap();
        assert!(
            matches!(closed, Error::Mse(mse::MseError::Closed { .. })),
            "{closed:?}"
        );
        assert!(!closed.is_failure());

        // The far end is held, and says nothing, until the handshake has
        // timed out.
        let (silent, _held) = tokio::join!(handshake(Duration::from_secs(10)), dialed.recv());
        let silent = silent.err().unwrap();
        assert!(
            matches!(silent, Error::Timeout("encryption handshake")),
            "{silent:?}"
        );
        assert!(!silent.is_failure());

        // A public key of 2, then more than the padding window of bytes
        // with no verification constant in them.
        let no_vc = async {
            let (_, mut theirs) = dialed.recv().await.unwrap();
            let mut key = [0u8; 96];
            key[95] = 2;
            theirs.write_all(&key).await.unwrap();
            theirs.write_all(&[0xff; 600]).await.unwrap();
            theirs
        };
        let (unsynced, _theirs) = tokio::join!(handshake(Duration::MAX), no_vc);
        let unsynced = unsynced.err().unwrap();
        assert!(
            matches!(unsynced, Error::Mse(mse::MseError::NotSynchronised { .. })),
            "{unsynced:?}"
        );
        assert!(unsynced.is_failure());

        drop(dialed);
        let refused = handshake(Duration::MAX).await.err().unwrap();
        assert!(matches!(refused, Error::TcpConnect(_)), "{refused:?}");
        assert!(refused.is_failure());
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

    /// Sockets whose TCP connects end as `answer` says: never, when it is
    /// `None`, else with an error of that kind.
    struct Answering {
        sockets: Arc<dyn SocketFactory>,
        answer: Option<std::io::ErrorKind>,
    }

    impl SocketFactory for Answering {
        fn connect_tcp<'a>(
            &'a self,
            _addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
            let answer = self.answer;
            Box::pin(async move {
                match answer {
                    Some(kind) => Err(std::io::Error::from(kind)),
                    None => std::future::pending().await,
                }
            })
        }

        fn listen_tcp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>> {
            self.sockets.listen_tcp(addr)
        }

        fn bind_udp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>> {
            self.sockets.bind_udp(addr)
        }

        fn bind_utp<'a>(
            &'a self,
            addr: SocketAddr,
            receive_buffer: usize,
        ) -> BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>> {
            self.sockets.bind_utp(addr, receive_buffer)
        }
    }

    // The TCP counts, as attempted, connected, refused, timed out and failed
    // otherwise, after one handshake to a peer whose connect ends as
    // `answer` says, under a connect timeout of a minute.
    async fn tcp_ends(answer: Option<std::io::ErrorKind>) -> [u64; 5] {
        let connector = StreamConnector::new(StreamConnectorArgs::over(Arc::new(Answering {
            sockets: crate::test_support::socket_factory(),
            answer,
        })))
        .await
        .unwrap();
        let sent = connector
            .connect_and_send_handshake(
                (Ipv4Addr::LOCALHOST, 6881).into(),
                Transports::Any,
                Guard::Mse(EncryptionPolicy::Disabled),
                &Id20::new([7; 20]),
                HANDSHAKE,
                || false,
                Duration::from_secs(60),
                Duration::MAX,
            )
            .await;
        assert!(sent.is_err());
        let v4 = connector.stats().tcp.snapshot().v4;
        [
            v4.attempts,
            v4.successes,
            v4.refused,
            v4.timeouts,
            v4.errors,
        ]
    }

    // Every attempt ends in exactly one counted end: one whose connect
    // timeout runs out counts as timed out, not as nothing.
    #[tokio::test(start_paused = true)]
    async fn an_attempt_that_runs_out_of_time_counts_as_timed_out() {
        assert_eq!(tcp_ends(None).await, [1, 0, 0, 1, 0]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_attempt_the_peer_refuses_counts_as_refused() {
        assert_eq!(
            tcp_ends(Some(std::io::ErrorKind::ConnectionRefused)).await,
            [1, 0, 1, 0, 0]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_attempt_that_fails_otherwise_counts_as_failed() {
        assert_eq!(
            tcp_ends(Some(std::io::ErrorKind::ConnectionReset)).await,
            [1, 0, 0, 0, 1]
        );
    }
}
