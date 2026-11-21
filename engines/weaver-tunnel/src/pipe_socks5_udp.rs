// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A SOCKS5 hop as a carrier of datagrams: UDP ASSOCIATE, RFC 1928 section 7.
//
// Each socket bound through the hop holds one association. A control
// connection to the proxy, dialed through the stage beneath as every
// connection through the hop is, keeps it alive. The proxy names a relay, and
// every datagram goes to that relay, wrapped in the SOCKS5 UDP header, from a
// socket the stage beneath binds. No datagram ever leaves another way: while
// no association is up, a send fails and the datagram is lost.
//
// When the control connection ends, so does the association. Another is
// asked for after a pause that grows with each failure in a row. A proxy that
// refuses, by its answer rather than by failing, is not asked again.
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use tokio::{io::AsyncReadExt, sync::watch};

use super::{
    Dialed, Dialer, Egress, Purpose, Resolution, SocketClass, Target, TransportHop,
    datagram::tunnel_error,
};
use crate::{
    TunnelError,
    endpoint::{DatagramSocket, DatagramTransport, UdpSocketFactory},
    transport::Said,
};

const CMD_UDP_ASSOCIATE: u8 = 3;
const ATYP_IPV4: u8 = 1;
const ATYP_DOMAIN: u8 = 3;
const ATYP_IPV6: u8 = 4;
// The largest SOCKS5 UDP header Weaver writes: an IPv6 destination.
const HEADER_ROOM: u16 = 22;

// Waits out the pause before another association is asked for.
#[async_trait::async_trait]
pub(super) trait Pause: Send + Sync {
    async fn wait(&self, pause: Duration);
}

struct Sleep;

#[async_trait::async_trait]
impl Pause for Sleep {
    async fn wait(&self, pause: Duration) {
        tokio::time::sleep(pause).await;
    }
}

// The pause before asking again after `failures` failures in a row: five
// seconds, growing to two minutes more than that.
pub(super) fn pause_after(failures: u32) -> Duration {
    let failures = u64::from(failures.min(200));
    Duration::from_secs((failures * failures / 2).min(120) + 5)
}

pub(super) struct Socks5Datagrams {
    hop: Arc<TransportHop>,
    relay: Arc<dyn DatagramTransport>,
    pause: Arc<dyn Pause>,
}

impl Socks5Datagrams {
    // `relay` carries the datagrams to the proxy's relay.
    pub(super) fn new(hop: Arc<TransportHop>, relay: Arc<dyn DatagramTransport>) -> Self {
        Self::pausing(hop, relay, Arc::new(Sleep))
    }

    pub(super) fn pausing(
        hop: Arc<TransportHop>,
        relay: Arc<dyn DatagramTransport>,
        pause: Arc<dyn Pause>,
    ) -> Self {
        Self { hop, relay, pause }
    }

    // A socket, once its first association is up or has failed. Either way
    // it stays bound: a failed association is asked for again.
    pub(super) async fn open(&self) -> Socks5Socket {
        let (states, mut watching) = watch::channel(State::Starting);
        let association = Association {
            hop: Arc::clone(&self.hop),
            relay: Arc::clone(&self.relay),
            pause: Arc::clone(&self.pause),
        };
        let task = tokio::spawn(association.run(states));
        let link_mtu = match watching
            .wait_for(|state| !matches!(state, State::Starting))
            .await
            .map(|state| state.clone())
        {
            Ok(State::Up(relayed)) => relayed
                .socket
                .link_mtu()
                .map(|mtu| mtu.saturating_sub(HEADER_ROOM)),
            _ => None,
        };
        Socks5Socket {
            states: watching,
            task,
            link_mtu,
        }
    }
}

#[async_trait::async_trait]
impl DatagramTransport for Socks5Datagrams {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, TunnelError> {
        match self.hop.resolve(host).await.map_err(tunnel_error)? {
            Resolution::Addresses(addresses) => Ok(addresses),
            Resolution::Unresolved => Err(TunnelError::Engine(format!(
                "proxy {} cannot resolve names",
                self.hop.id
            ))),
        }
    }

    async fn bind(&self, _peer: SocketAddr) -> Result<Arc<dyn DatagramSocket>, TunnelError> {
        Ok(Arc::new(self.open().await))
    }
}

#[derive(Clone)]
pub(super) enum State {
    // The first association is being asked for.
    Starting,
    Up(Arc<Relayed>),
    // No association; another is asked for after a pause.
    Down,
    // The proxy refused, and is not asked again.
    Refused,
}

pub(super) struct Relayed {
    socket: Arc<dyn DatagramSocket>,
    relay: SocketAddr,
}

enum Failure {
    // Asked again after a pause.
    Failed(String),
    // Not asked again.
    Refused(String),
}

struct Association {
    hop: Arc<TransportHop>,
    relay: Arc<dyn DatagramTransport>,
    pause: Arc<dyn Pause>,
}

impl Association {
    async fn run(self, states: watch::Sender<State>) {
        let mut failures = 0u32;
        loop {
            match self.associate().await {
                Ok((mut dialed, relayed)) => {
                    failures = 0;
                    let relayed = Arc::new(relayed);
                    states.send_replace(State::Up(Arc::clone(&relayed)));
                    tokio::select! {
                        () = hung_up(&mut dialed) => {}
                        () = relayed.socket.closed() => {}
                    }
                    tracing::debug!(proxy = self.hop.id, "SOCKS5 UDP association ended");
                }
                Err(Failure::Refused(reason)) => {
                    tracing::debug!(
                        proxy = self.hop.id,
                        reason,
                        "SOCKS5 UDP association refused"
                    );
                    states.send_replace(State::Refused);
                    // The socket stays, refusing every send.
                    return std::future::pending().await;
                }
                Err(Failure::Failed(reason)) => {
                    failures = failures.saturating_add(1);
                    tracing::debug!(proxy = self.hop.id, reason, "SOCKS5 UDP association failed");
                }
            }
            states.send_replace(State::Down);
            self.pause.wait(pause_after(failures)).await;
        }
    }

    // One association: the control connection and the socket to its relay.
    async fn associate(&self) -> Result<(Dialed, Relayed), Failure> {
        let hop = &self.hop;
        let endpoint = Target {
            host: hop.spec.host.clone(),
            port: hop.spec.port,
            purpose: Purpose::ProxyEndpoint {
                proxy: hop.id,
                carrying: Some(SocketClass::of(&Purpose::Datagram)),
            },
            addresses: Vec::new(),
        };
        let mut dialed = hop
            .inner
            .dial(&endpoint)
            .await
            .map_err(|error| Failure::Failed(error.to_string()))?;
        let named = tokio::time::timeout(hop.timeout, async {
            hop.spec
                .greet(&mut dialed.stream)
                .await
                .map_err(greet_failure)?;
            ask(&mut dialed.stream).await
        })
        .await
        .map_err(|_| Failure::Failed("the proxy did not answer in time".into()))??;
        let relay = match named {
            Named::Address(relay) if !relay.ip().is_unspecified() => relay,
            // The proxy's own address.
            Named::Address(relay) => SocketAddr::new(self.proxy_ip(&dialed)?, relay.port()),
            Named::Name(port) => SocketAddr::new(self.proxy_ip(&dialed)?, port),
        };
        let socket = self
            .relay
            .bind(relay)
            .await
            .map_err(|error| Failure::Failed(error.to_string()))?;
        Ok((dialed, Relayed { socket, relay }))
    }

    // The proxy's own address: its endpoint as written, or the address a
    // dial straight from the egress reached.
    fn proxy_ip(&self, dialed: &Dialed) -> Result<IpAddr, Failure> {
        if let Ok(ip) = self.hop.spec.host.parse() {
            return Ok(ip);
        }
        match dialed.peer {
            Some(peer) if dialed.path.proxies.is_empty() => Ok(peer.ip()),
            _ => Err(Failure::Refused(
                "the proxy named its own address as the relay, which is not known here".into(),
            )),
        }
    }
}

// A refusal by the proxy's answer is final; a failure to hear one is not.
fn greet_failure(error: TunnelError) -> Failure {
    let refused = matches!(
        Said::find(&error.to_string()),
        Some(
            Said::NotSocks
                | Said::SocksAuthRequired
                | Said::SocksMethodRejected
                | Said::SocksCredentialLength
                | Said::SocksCredentialsRejected
        )
    );
    if refused {
        Failure::Refused(error.to_string())
    } else {
        Failure::Failed(error.to_string())
    }
}

// The relay a proxy names.
enum Named {
    Address(SocketAddr),
    // A name, which stands for the proxy's own address.
    Name(u16),
}

// Ask a greeted proxy for an association. The request names no client
// address, so the proxy takes datagrams from wherever the relay socket sends.
async fn ask(stream: &mut super::DialedStream) -> Result<Named, Failure> {
    use tokio::io::AsyncWriteExt;
    let closed = |_| Failure::Failed(Said::SocksConnectClosed.text().into());
    let invalid = || Failure::Refused(Said::SocksReplyInvalid.text().into());
    stream
        .write_all(&[5, CMD_UDP_ASSOCIATE, 0, ATYP_IPV4, 0, 0, 0, 0, 0, 0])
        .await
        .map_err(closed)?;
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.map_err(closed)?;
    match header {
        [5, 0, 0, _] => {}
        [5, code, ..] => {
            return Err(Failure::Refused(format!(
                "the proxy refused UDP ASSOCIATE with SOCKS reply {code}"
            )));
        }
        _ => return Err(invalid()),
    }
    let ip = match header[3] {
        ATYP_IPV4 => {
            let mut octets = [0; 4];
            stream.read_exact(&mut octets).await.map_err(closed)?;
            Some(IpAddr::from(octets))
        }
        ATYP_IPV6 => {
            let mut octets = [0; 16];
            stream.read_exact(&mut octets).await.map_err(closed)?;
            Some(IpAddr::from(octets))
        }
        ATYP_DOMAIN => {
            let len = stream.read_u8().await.map_err(closed)?;
            let mut name = vec![0; usize::from(len)];
            stream.read_exact(&mut name).await.map_err(closed)?;
            None
        }
        _ => return Err(invalid()),
    };
    let port = stream.read_u16().await.map_err(closed)?;
    Ok(match ip {
        Some(ip) => Named::Address(SocketAddr::new(ip, port)),
        None => Named::Name(port),
    })
}

// Resolves when the control connection ends. The proxy sends nothing on it.
async fn hung_up(dialed: &mut Dialed) {
    let mut bytes = [0; 64];
    while let Ok(1..) = dialed.stream.read(&mut bytes).await {}
}

// `data` to `target`, behind the SOCKS5 UDP header.
pub(super) fn wrap(target: SocketAddr, data: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(usize::from(HEADER_ROOM) + data.len());
    packet.extend_from_slice(&[0, 0, 0]);
    match target.ip() {
        IpAddr::V4(ip) => {
            packet.push(ATYP_IPV4);
            packet.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            packet.push(ATYP_IPV6);
            packet.extend_from_slice(&ip.octets());
        }
    }
    packet.extend_from_slice(&target.port().to_be_bytes());
    packet.extend_from_slice(data);
    packet
}

// Where a relayed datagram's payload starts and who sent it, or `None` for
// one to drop: too short, fragmented, or from a sender named by a name.
pub(super) fn unwrap(packet: &[u8]) -> Option<(usize, SocketAddr)> {
    let [_, _, frag, atyp, rest @ ..] = packet else {
        return None;
    };
    // Fragments are not reassembled.
    if *frag != 0 {
        return None;
    }
    let (ip, rest) = match *atyp {
        ATYP_IPV4 => {
            let (octets, rest) = rest.split_first_chunk::<4>()?;
            (IpAddr::V4(Ipv4Addr::from(*octets)), rest)
        }
        ATYP_IPV6 => {
            let (octets, rest) = rest.split_first_chunk::<16>()?;
            (IpAddr::V6(Ipv6Addr::from(*octets)), rest)
        }
        ATYP_DOMAIN => {
            let (len, rest) = rest.split_first()?;
            let (name, rest) = rest.split_at_checked(usize::from(*len))?;
            (std::str::from_utf8(name).ok()?.parse().ok()?, rest)
        }
        _ => return None,
    };
    let (port, payload) = rest.split_first_chunk::<2>()?;
    // A header with nothing after it carries nothing.
    if payload.is_empty() {
        return None;
    }
    Some((
        packet.len() - payload.len(),
        SocketAddr::new(ip, u16::from_be_bytes(*port)),
    ))
}

fn down() -> io::Error {
    io::Error::new(
        io::ErrorKind::NetworkUnreachable,
        "the SOCKS5 proxy's UDP association is down",
    )
}

pub(super) struct Socks5Socket {
    states: watch::Receiver<State>,
    task: tokio::task::JoinHandle<()>,
    link_mtu: Option<u16>,
}

impl Socks5Socket {
    #[cfg(test)]
    pub(super) fn states(&self) -> watch::Receiver<State> {
        self.states.clone()
    }
}

impl Drop for Socks5Socket {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[async_trait::async_trait]
impl DatagramSocket for Socks5Socket {
    async fn send_to(&self, data: &[u8], target: SocketAddr) -> io::Result<()> {
        let State::Up(relayed) = self.states.borrow().clone() else {
            return Err(down());
        };
        relayed
            .socket
            .send_to(&wrap(target, data), relayed.relay)
            .await
    }

    async fn recv_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let mut states = self.states.clone();
        loop {
            let State::Up(relayed) = states.borrow_and_update().clone() else {
                states.changed().await.map_err(|_| down())?;
                continue;
            };
            tokio::select! {
                received = relayed.socket.recv_from(buffer) => {
                    let Ok((len, from)) = received else {
                        if relayed.socket.is_closed() {
                            states.changed().await.map_err(|_| down())?;
                        }
                        continue;
                    };
                    // Only the relay speaks for the association.
                    if from != relayed.relay {
                        continue;
                    }
                    if let Some((start, sender)) = unwrap(&buffer[..len]) {
                        buffer.copy_within(start..len, 0);
                        return Ok((len - start, sender));
                    }
                }
                changed = states.changed() => changed.map_err(|_| down())?,
            }
        }
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        None
    }

    fn link_mtu(&self) -> Option<u16> {
        self.link_mtu
    }

    fn is_closed(&self) -> bool {
        false
    }

    // A socket through a proxy is asked for again whenever its association
    // ends, so it never stops for good while it is held.
    async fn closed(&self) {
        std::future::pending().await
    }
}

// An egress's own datagram sockets, for a proxy stacked on it.
pub(super) struct EgressDatagrams(pub(super) Arc<Egress>);

#[async_trait::async_trait]
impl DatagramTransport for EgressDatagrams {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, TunnelError> {
        match self.0.resolve(host).await.map_err(tunnel_error)? {
            Resolution::Addresses(addresses) => Ok(addresses),
            Resolution::Unresolved => Ok(Vec::new()),
        }
    }

    async fn bind(&self, peer: SocketAddr) -> Result<Arc<dyn DatagramSocket>, TunnelError> {
        let socket = UdpSocketFactory::bind(self.0.as_ref(), peer)
            .and_then(tokio::net::UdpSocket::from_std)
            .map_err(|error| {
                TunnelError::Engine(format!("{}: {error}", Said::SocketSetup.text()))
            })?;
        Ok(Arc::new(EgressSocket(socket)))
    }
}

struct EgressSocket(tokio::net::UdpSocket);

#[async_trait::async_trait]
impl DatagramSocket for EgressSocket {
    async fn send_to(&self, data: &[u8], target: SocketAddr) -> io::Result<()> {
        self.0.send_to(data, target).await.map(|_| ())
    }
    async fn recv_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.recv_from(buffer).await
    }
    fn local_addr(&self) -> Option<SocketAddr> {
        self.0.local_addr().ok()
    }
    fn link_mtu(&self) -> Option<u16> {
        None
    }
    fn is_closed(&self) -> bool {
        false
    }
    async fn closed(&self) {
        std::future::pending().await
    }
}

#[cfg(test)]
#[path = "pipe_socks5_udp_tests.rs"]
mod tests;
