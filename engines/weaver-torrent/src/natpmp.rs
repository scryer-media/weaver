// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Port mapping through the default gateway over PCP (RFC 6887), falling
//! back to NAT-PMP (RFC 6886), after libtorrent's `natpmp.cpp`.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, bail};
use futures::FutureExt;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use crate::{
    core::net::{DatagramSocket, Egress, HttpClients},
    route_interface::RouteInterface,
    upnp::{PortMapEvents, PortMapReports, PortMapRequest, PortmapProtocol, PortmapTransport},
};

/// The port a NAT-PMP or PCP server listens on.
pub const NATPMP_PORT: u16 = 5351;

const VERSION_NATPMP: u8 = 0;
const VERSION_PCP: u8 = 2;
const OPCODE_MAP: u8 = 1;
/// NAT-PMP's public address request and answer.
const OPCODE_PUBLIC_ADDRESS: u8 = 0;
const RESPONSE_BIT: u8 = 0x80;
const RESULT_UNSUPPORTED_VERSION: u16 = 1;
/// libtorrent `resend_request`: a request goes out at most this many times.
const MAX_SENDS: u32 = 9;
/// libtorrent `send_map_request`: the n-th resend waits n times this.
const RESEND_STEP: Duration = Duration::from_millis(250);
/// libtorrent `resend_request` / `on_reply`: a mapping the gateway never
/// answered or refused is tried again this much later.
const RETRY_FAILED_AFTER: Duration = Duration::from_secs(2 * 60 * 60);

/// PCP result codes (libtorrent `pcp_error_category`).
const PCP_RESULTS: [&str; 14] = [
    "success",
    "unsupported version",
    "not authorized",
    "malformed request",
    "unsupported opcode",
    "unsupported option",
    "malformed option",
    "network failure",
    "no resources",
    "unsupported protocol",
    "user exceeded quota",
    "cannot provide external",
    "address mismatch",
    "excessive remote peers",
];

/// What a result code means; NAT-PMP's codes 3 to 5 are PCP's 7, 8 and 4
/// (libtorrent `from_result_code`).
fn result_message(version: u8, result: u16) -> String {
    let code = match (version, result) {
        (VERSION_NATPMP, 3) => 7,
        (VERSION_NATPMP, 4) => 8,
        (VERSION_NATPMP, 5) => 4,
        (_, r) => r,
    };
    PCP_RESULTS
        .get(usize::from(code))
        .map_or_else(|| format!("unknown error {code}"), |m| (*m).to_owned())
}

pub struct NatPmpOptions {
    /// libtorrent `natpmp_lease_duration`.
    pub lease_duration: Duration,
}

impl Default for NatPmpOptions {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(3600),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    None,
    Add,
    Delete,
}

/// One mapping on the gateway (libtorrent `natpmp::mapping_t`).
struct Mapping {
    request: PortMapRequest,
    /// The port asked for, then the one the gateway granted.
    external_port: u16,
    /// The address the gateway granted (PCP only).
    external_ip: Option<IpAddr>,
    /// PCP ties every request for a mapping to it by this.
    nonce: [u8; 12],
    action: Action,
    /// When to refresh; `None` while nothing is scheduled.
    expires: Option<Instant>,
    map_sent: bool,
    outstanding: bool,
    /// Cleared once the gateway closed the mapping.
    live: bool,
}

impl Mapping {
    fn new(request: PortMapRequest) -> Self {
        use rand::Rng;
        let mut nonce = [0; 12];
        rand::rng().fill_bytes(&mut nonce);
        Self {
            request,
            external_port: request.external_port,
            external_ip: None,
            nonce,
            action: Action::Add,
            expires: None,
            map_sent: false,
            outstanding: false,
            live: true,
        }
    }
}

/// An address as PCP carries it: IPv4 as IPv4-mapped IPv6.
fn pcp_address(ip: IpAddr) -> [u8; 16] {
    match ip {
        IpAddr::V4(ip) => ip.to_ipv6_mapped().octets(),
        IpAddr::V6(ip) => ip.octets(),
    }
}

/// An address only the local network reaches (libtorrent `is_local`).
fn is_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_loopback() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unicast_link_local() || ip.is_unique_local(),
    }
}

fn be16(b: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([b[at], b[at + 1]])
}

fn be32(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// The request on the wire (libtorrent `m_currently_mapping` and its
/// send timer).
#[derive(Clone, Copy)]
struct InFlight {
    index: usize,
    resend_at: Instant,
}

/// The mapper's state (libtorrent `natpmp`).
struct State {
    version: u8,
    lease_secs: u32,
    local_ip: IpAddr,
    mappings: Vec<Mapping>,
    in_flight: Option<InFlight>,
    send_count: u32,
    /// What NAT-PMP's public address request said.
    external_ip: Option<IpAddr>,
    /// The gateway's last epoch and when it came.
    epoch: Option<(u32, Instant)>,
}

impl State {
    /// libtorrent `send_map_request`.
    fn map_request(&self, m: &Mapping) -> Vec<u8> {
        let lifetime = if m.action == Action::Add {
            self.lease_secs
        } else {
            0
        };
        let mut p = Vec::with_capacity(60);
        if self.version == VERSION_NATPMP {
            p.push(VERSION_NATPMP);
            p.push(match m.request.protocol {
                PortmapProtocol::Udp => 1,
                PortmapProtocol::Tcp => 2,
            });
            p.extend_from_slice(&[0, 0]);
            p.extend_from_slice(&m.request.local_port.to_be_bytes());
            p.extend_from_slice(&m.external_port.to_be_bytes());
            p.extend_from_slice(&lifetime.to_be_bytes());
        } else {
            p.push(VERSION_PCP);
            p.push(OPCODE_MAP);
            p.extend_from_slice(&[0, 0]);
            p.extend_from_slice(&lifetime.to_be_bytes());
            p.extend_from_slice(&pcp_address(self.local_ip));
            p.extend_from_slice(&m.nonce);
            p.push(match m.request.protocol {
                PortmapProtocol::Tcp => 6,
                PortmapProtocol::Udp => 17,
            });
            p.extend_from_slice(&[0, 0, 0]);
            p.extend_from_slice(&m.request.local_port.to_be_bytes());
            p.extend_from_slice(&m.external_port.to_be_bytes());
            let suggested = match m.external_ip {
                Some(ip) => ip,
                None if is_local(self.local_ip) => match self.local_ip {
                    IpAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
                    IpAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
                },
                None => self.local_ip,
            };
            p.extend_from_slice(&pcp_address(suggested));
        }
        p
    }

    async fn send_map_request(&mut self, i: usize, socket: &GatewaySocket) {
        let packet = self.map_request(&self.mappings[i]);
        let m = &mut self.mappings[i];
        trace!(
            version = self.version,
            protocol = ?m.request.protocol,
            local_port = m.request.local_port,
            external_port = m.external_port,
            action = ?m.action,
            "port map request"
        );
        if let Err(e) = socket.send(&packet).await {
            debug!("error sending a port map request: {e:#}");
        }
        m.map_sent = true;
        m.outstanding = true;
        // libtorrent: linear back-off.
        self.send_count += 1;
        self.in_flight = Some(InFlight {
            index: i,
            resend_at: Instant::now() + RESEND_STEP * self.send_count,
        });
    }

    /// libtorrent `resend_request`. A mapping the gateway never answered is
    /// reported failed, as one it refused is.
    async fn resend_request(&mut self, i: usize, socket: &GatewaySocket, reports: &PortMapReports) {
        let Some(flight) = self.in_flight.filter(|f| f.index == i) else {
            return;
        };
        if self.send_count >= MAX_SENDS {
            self.in_flight = None;
            let m = &mut self.mappings[i];
            m.action = Action::None;
            // Counted from when the last resend fell due.
            m.expires = Some(flight.resend_at + RETRY_FAILED_AFTER);
            debug!(protocol = ?m.request.protocol, "the gateway never answered");
            reports.failed(m.request, "the gateway never answered".to_owned());
            return;
        }
        self.send_map_request(i, socket).await;
    }

    async fn send_public_address_request(&self, socket: &GatewaySocket) {
        if let Err(e) = socket.send(&[VERSION_NATPMP, OPCODE_PUBLIC_ADDRESS]).await {
            debug!("error sending a public address request: {e:#}");
        }
    }

    /// Send the next mapping that has something to do, if the socket is
    /// free (libtorrent `update_mapping` / `try_next_mapping`).
    async fn start_next(&mut self, socket: &GatewaySocket) {
        if self.in_flight.is_some() {
            return;
        }
        if let Some(i) = self
            .mappings
            .iter()
            .position(|m| m.live && m.action != Action::None)
        {
            self.send_count = 0;
            self.send_map_request(i, socket).await;
        }
    }

    /// When the loop must wake: the resend of the request in flight, or
    /// else the first refresh (libtorrent `update_expiration_timer`).
    fn next_deadline(&self) -> Option<Instant> {
        if let Some(flight) = self.in_flight {
            return Some(flight.resend_at);
        }
        self.mappings
            .iter()
            .filter(|m| m.live && m.action == Action::None)
            .filter_map(|m| m.expires)
            .min()
    }

    /// libtorrent `on_resend_request` and `mapping_expired`.
    async fn on_timer(&mut self, socket: &GatewaySocket, reports: &PortMapReports) {
        let now = Instant::now();
        if let Some(flight) = self.in_flight {
            if flight.resend_at <= now {
                self.resend_request(flight.index, socket, reports).await;
            }
            return;
        }
        for m in &mut self.mappings {
            if m.live && m.action == Action::None && m.expires.is_some_and(|t| t <= now) {
                m.action = Action::Add;
                m.expires = None;
            }
        }
    }

    /// Ask the gateway to drop every mapping it may hold, with a lifetime-0
    /// MAP each, sent once without waiting for an answer, and report each
    /// gone (libtorrent `natpmp::close_impl`; `send_map_request` under
    /// `m_abort`). One last reported failed stays as reported.
    async fn close(&mut self, socket: &GatewaySocket, reports: &PortMapReports) {
        self.in_flight = None;
        for i in 0..self.mappings.len() {
            let m = &mut self.mappings[i];
            // A mapping never asked for, or one the gateway closed, is not
            // there to remove.
            if !(m.live && m.map_sent) {
                continue;
            }
            m.action = Action::Delete;
            let packet = self.map_request(&self.mappings[i]);
            if let Err(e) = socket.send(&packet).await {
                debug!("error sending a port unmap request: {e:#}");
            }
            let m = &mut self.mappings[i];
            m.live = false;
            trace!(protocol = ?m.request.protocol, external_port = m.external_port, "port mapping removed");
            reports.removed(m.request, None);
        }
    }

    /// Whether the gateway lost its mappings: its epoch went back, or moved
    /// on by less than our own clock did (RFC 6886 section 3.6).
    fn gateway_lost_state(&mut self, epoch: u32) -> bool {
        let now = Instant::now();
        let lost = self.epoch.is_some_and(|(previous, at)| {
            let expected = u64::from(previous) + (now - at).as_secs() * 7 / 8;
            epoch < previous || u64::from(epoch) + 2 < expected
        });
        self.epoch = Some((epoch, now));
        lost
    }

    /// libtorrent `on_reply`.
    async fn on_reply(&mut self, p: &[u8], socket: &GatewaySocket, reports: &PortMapReports) {
        if p.len() < 4 {
            return;
        }
        let version = p[0];
        if version != VERSION_NATPMP && version != VERSION_PCP {
            return;
        }
        let pcp = version == VERSION_PCP;
        let opcode = if pcp { p[1] & !RESPONSE_BIT } else { p[1] };
        let result = if pcp { u16::from(p[3]) } else { be16(p, 2) };

        let epoch_at = if pcp { 8 } else { 4 };
        if p.len() >= epoch_at + 4 && self.gateway_lost_state(be32(p, epoch_at)) {
            debug!("the gateway restarted, mapping again");
            for m in &mut self.mappings {
                if m.live && m.action == Action::None {
                    m.action = Action::Add;
                    m.expires = None;
                }
            }
        }

        if result == RESULT_UNSUPPORTED_VERSION {
            // NAT-PMP carries IPv4 only.
            if self.version == VERSION_PCP && self.local_ip.is_ipv4() {
                debug!("the gateway speaks NAT-PMP, not PCP");
                self.version = VERSION_NATPMP;
                if let Some(flight) = self.in_flight {
                    self.resend_request(flight.index, socket, reports).await;
                }
                self.send_public_address_request(socket).await;
            }
            return;
        }

        if (!pcp && p.len() < 12) || (pcp && p.len() < 24) {
            return;
        }
        if !pcp && opcode == RESPONSE_BIT | OPCODE_PUBLIC_ADDRESS {
            self.external_ip = Some(Ipv4Addr::new(p[8], p[9], p[10], p[11]).into());
            debug!(external_ip = ?self.external_ip, "gateway public address");
            return;
        }
        if (!pcp && p.len() != 16) || (pcp && p.len() != 60) {
            return;
        }

        let (protocol, nonce, private_port, public_port, lifetime, external_ip) = if pcp {
            let address = Ipv6Addr::from(<[u8; 16]>::try_from(&p[44..60]).unwrap());
            let address = address
                .to_ipv4_mapped()
                .map_or(IpAddr::V6(address), IpAddr::V4);
            (
                if p[36] == 6 {
                    PortmapProtocol::Tcp
                } else {
                    PortmapProtocol::Udp
                },
                Some(&p[24..36]),
                be16(p, 40),
                be16(p, 42),
                be32(p, 4),
                Some(address).filter(|a| !a.is_unspecified()),
            )
        } else {
            (
                if opcode.wrapping_sub(RESPONSE_BIT) == 1 {
                    PortmapProtocol::Udp
                } else {
                    PortmapProtocol::Tcp
                },
                None,
                be16(p, 8),
                be16(p, 10),
                be32(p, 12),
                None,
            )
        };

        let Some(i) = self.mappings.iter().position(|m| {
            m.request.local_port == private_port
                && m.request.protocol == protocol
                && m.map_sent
                && m.outstanding
                && nonce.is_none_or(|n| n == m.nonce)
        }) else {
            trace!(?protocol, private_port, "port map answer for no request");
            return;
        };

        let now = Instant::now();
        let m = &mut self.mappings[i];
        m.outstanding = false;
        if public_port == 0 || lifetime == 0 {
            // The gateway closed it.
            m.live = false;
        } else {
            // libtorrent: refresh at three quarters of the granted lifetime.
            m.expires = Some(now + Duration::from_secs(u64::from(lifetime) * 3 / 4));
            m.external_port = public_port;
            if external_ip.is_some() {
                m.external_ip = external_ip;
            }
        }

        if result != 0 {
            m.expires = Some(now + RETRY_FAILED_AFTER);
            reports.failed(m.request, result_message(version, result));
        } else if m.action == Action::Add {
            let external_ip = if pcp { m.external_ip } else { self.external_ip };
            reports.send(
                m.request,
                m.live.then_some(m.external_port),
                external_ip,
                None,
            );
        }
        m.action = Action::None;
        self.in_flight = None;
    }
}

/// Where a mapper asks: from an address on the route's interface to the
/// NAT-PMP / PCP server of the gateway on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayRoute {
    pub local_ip: IpAddr,
    pub gateway: SocketAddr,
}

impl GatewayRoute {
    /// The default gateway on `interface`, the route's interface.
    pub fn of(interface: &RouteInterface) -> anyhow::Result<Self> {
        let routes = routes().context("error reading the routing table")?;
        gateway_route(&routes, interface)
    }
}

fn gateway_route(routes: &[Route], interface: &RouteInterface) -> anyhow::Result<GatewayRoute> {
    let gateway = default_gateway_of(routes, &interface.name)
        .with_context(|| format!("no gateway on the route's interface {}", interface.name))?;
    Ok(GatewayRoute {
        local_ip: interface.ip.into(),
        gateway: (gateway, NATPMP_PORT).into(),
    })
}

/// The socket a mapper asks the gateway over: what it sends goes to the
/// gateway, and only the gateway's answers come back.
struct GatewaySocket {
    socket: Arc<dyn DatagramSocket>,
    gateway: SocketAddr,
}

impl GatewaySocket {
    async fn send(&self, packet: &[u8]) -> io::Result<usize> {
        self.socket.send_to(packet, self.gateway).await
    }

    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let (n, from) = self.socket.recv_from(buf).await?;
            if from == self.gateway {
                return Ok(n);
            }
            trace!(%from, "ignoring a datagram not from the gateway");
        }
    }
}

/// Keeps ports mapped on the gateway over PCP, or NAT-PMP when the gateway
/// only speaks that (libtorrent `natpmp`).
pub struct NatPmpMapper {
    /// Where to ask, or why there is nowhere to.
    route: Result<GatewayRoute, String>,
    mappings: Vec<PortMapRequest>,
    opts: NatPmpOptions,
    /// The session's clients, whose route reaches the gateway and gives the
    /// socket to ask it over.
    clients: Arc<dyn HttpClients>,
}

impl NatPmpMapper {
    /// A mapper asking over `route` through `clients`' socket, or failing
    /// every mapping for why there is no route to ask over.
    pub fn new(
        route: Result<GatewayRoute, String>,
        mappings: Vec<PortMapRequest>,
        opts: Option<NatPmpOptions>,
        clients: Arc<dyn HttpClients>,
    ) -> Self {
        Self {
            route,
            mappings,
            opts: opts.unwrap_or_default(),
            clients,
        }
    }

    /// The socket the mapper asks the gateway over, from which only the
    /// gateway's answers come through, and the address it asks from.
    async fn connect(
        route: Result<GatewayRoute, String>,
        clients: &dyn HttpClients,
    ) -> anyhow::Result<(GatewaySocket, IpAddr)> {
        let route = route.map_err(anyhow::Error::msg)?;
        let socket = clients
            .gateway_socket(SocketAddr::new(route.local_ip, 0))
            .await
            .with_context(|| format!("error binding the NAT-PMP socket to {}", route.local_ip))?;
        if clients.egress(route.gateway) == Egress::NoRoute {
            bail!(
                "error connecting to the gateway at {}: no route",
                route.gateway
            );
        }
        let local_ip = socket.bind_addr().ip();
        Ok((
            GatewaySocket {
                socket,
                gateway: route.gateway,
            },
            local_ip,
        ))
    }

    /// Keep every port mapped until cancelled, then remove the mappings. A
    /// mapper with no gateway to ask fails every mapping, as one the gateway
    /// never answers does.
    pub async fn run(self, cancel: CancellationToken, events: PortMapEvents) -> anyhow::Result<()> {
        let reports = PortMapReports::new(PortmapTransport::NatPmp, events);
        let (socket, local_ip) = match Self::connect(self.route, self.clients.as_ref()).await {
            Ok(connected) => connected,
            Err(e) => {
                debug!("NAT-PMP / PCP mapper has no gateway to ask: {e:#}");
                for request in &self.mappings {
                    reports.failed(*request, format!("{e:#}"));
                }
                return Ok(());
            }
        };
        debug!(gateway = %socket.gateway, %local_ip, "NAT-PMP / PCP mapper started");

        let mut state = State {
            // libtorrent `natpmp::start`: assume PCP, fall back to NAT-PMP.
            version: VERSION_PCP,
            lease_secs: u32::try_from(self.opts.lease_duration.as_secs()).unwrap_or(u32::MAX),
            local_ip,
            mappings: self.mappings.into_iter().map(Mapping::new).collect(),
            in_flight: None,
            send_count: 0,
            external_ip: None,
            epoch: None,
        };
        // PCP messages are at most 1100 bytes.
        let mut buf = [0u8; 1100];
        loop {
            state.start_next(&socket).await;
            let timer = match state.next_deadline() {
                Some(t) => tokio::time::sleep_until(t).left_future(),
                None => futures::future::pending::<()>().right_future(),
            };
            tokio::select! {
                _ = cancel.cancelled() => break,
                r = socket.recv(&mut buf) => match r {
                    Ok(n) => state.on_reply(&buf[..n], &socket, &reports).await,
                    Err(e) => debug!("error receiving from the gateway: {e:#}"),
                },
                _ = timer => state.on_timer(&socket, &reports).await,
            }
        }
        state.close(&socket, &reports).await;
        Ok(())
    }
}

/// One IPv4 route (libtorrent `ip_route`).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Route {
    destination: Ipv4Addr,
    mask: Ipv4Addr,
    /// The router the route goes through; `None` for a route straight out
    /// of the interface, as a tunnel's are.
    gateway: Option<Ipv4Addr>,
    interface: String,
    /// Lower wins.
    metric: u32,
}

/// The gateway of the default route on `interface` (libtorrent
/// `get_gateway`). A route over half the address space or more is a
/// default: 0/0, or either half of a 0/1 + 128/1 pair.
fn default_gateway_of(routes: &[Route], interface: &str) -> Option<Ipv4Addr> {
    routes
        .iter()
        .filter(|r| r.interface == interface && r.mask.to_bits().leading_ones() <= 1)
        .filter_map(|r| Some((r.metric, r.gateway?)))
        .min_by_key(|(metric, _)| *metric)
        .map(|(_, gateway)| gateway)
}

/// `/proc/net/route`: every address is the kernel's network-order word
/// printed as a native integer.
#[cfg_attr(not(any(target_os = "linux", target_os = "android")), allow(dead_code))]
fn parse_proc_net_route(text: &str) -> Vec<Route> {
    const RTF_UP: u32 = 0x1;
    const RTF_GATEWAY: u32 = 0x2;
    let hex = |s: &str| u32::from_str_radix(s, 16).ok();
    let address = |s: &str| Some(Ipv4Addr::from(hex(s)?.to_ne_bytes()));
    text.lines()
        .skip(1)
        .filter_map(|line| {
            let f: Vec<&str> = line.split_whitespace().collect();
            let flags = hex(f.get(3)?)?;
            if f.len() < 8 || flags & RTF_UP == 0 {
                return None;
            }
            Some(Route {
                interface: f[0].to_owned(),
                destination: address(f[1])?,
                mask: address(f[7])?,
                gateway: (flags & RTF_GATEWAY != 0).then(|| address(f[2])).flatten(),
                metric: f[6].parse().ok()?,
            })
        })
        .collect()
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn routes() -> io::Result<Vec<Route>> {
    Ok(parse_proc_net_route(&std::fs::read_to_string(
        "/proc/net/route",
    )?))
}

/// Parse a `NET_RT_DUMP` sysctl buffer (libtorrent `enum_routes` /
/// `parse_route`). `interface_name` names an interface index.
#[cfg(target_vendor = "apple")]
fn parse_route_dump(buf: &[u8], interface_name: impl Fn(u16) -> String) -> Vec<Route> {
    use nix::libc;
    // Apple pads each address to four bytes.
    fn padded(len: usize) -> usize {
        if len == 0 { 4 } else { 1 + ((len - 1) | 3) }
    }
    // A `sockaddr_in`; a zero-length address is all zeroes.
    fn ipv4(sa: &[u8]) -> Option<Ipv4Addr> {
        if sa.is_empty() {
            return Some(Ipv4Addr::UNSPECIFIED);
        }
        if sa.len() < 8 || i32::from(sa[1]) != libc::AF_INET {
            return None;
        }
        Some(Ipv4Addr::new(sa[4], sa[5], sa[6], sa[7]))
    }
    // A netmask is cut after its last non-zero byte, whatever its family;
    // a route without one is a host route.
    fn mask(sa: Option<&[u8]>) -> Ipv4Addr {
        let Some(sa) = sa else {
            return Ipv4Addr::BROADCAST;
        };
        let mut octets = [0u8; 4];
        for (o, b) in octets.iter_mut().zip(sa.iter().skip(4)) {
            *o = *b;
        }
        octets.into()
    }

    let header = std::mem::size_of::<libc::rt_msghdr>();
    let mut routes = Vec::new();
    let mut at = 0;
    while at + header <= buf.len() {
        // SAFETY: `rt_msghdr` is plain data and `header` bytes lie in bounds.
        let rtm: libc::rt_msghdr = unsafe { std::ptr::read_unaligned(buf[at..].as_ptr().cast()) };
        let len = usize::from(rtm.rtm_msglen);
        if len < header || at + len > buf.len() {
            break;
        }
        let msg = &buf[at..at + len];
        at += len;
        if i32::from(rtm.rtm_version) != libc::RTM_VERSION
            || (i32::from(rtm.rtm_type) != libc::RTM_ADD
                && i32::from(rtm.rtm_type) != libc::RTM_GET)
        {
            continue;
        }
        let mut addrs: [Option<&[u8]>; libc::RTAX_MAX as usize] = Default::default();
        let mut p = header;
        for (i, addr) in addrs.iter_mut().enumerate() {
            if rtm.rtm_addrs & (1 << i) == 0 {
                continue;
            }
            let Some(&sa_len) = msg.get(p) else { break };
            let sa_len = usize::from(sa_len);
            *addr = msg.get(p..p + sa_len);
            p += padded(sa_len);
        }
        let Some(destination) = addrs[libc::RTAX_DST as usize].and_then(ipv4) else {
            continue;
        };
        // A gateway that is a link, as a tunnel's is, is no router.
        let gateway = addrs[libc::RTAX_GATEWAY as usize]
            .filter(|_| rtm.rtm_flags & libc::RTF_GATEWAY != 0)
            .and_then(ipv4);
        routes.push(Route {
            destination,
            mask: mask(addrs[libc::RTAX_NETMASK as usize]),
            gateway,
            interface: interface_name(rtm.rtm_index),
            // The primary default route is the one not scoped to an interface.
            metric: u32::from(rtm.rtm_flags & libc::RTF_IFSCOPE != 0),
        });
    }
    routes
}

#[cfg(target_vendor = "apple")]
fn routes() -> io::Result<Vec<Route>> {
    use nix::libc;
    let mut mib = [
        libc::CTL_NET,
        libc::PF_ROUTE,
        0,
        libc::AF_INET,
        libc::NET_RT_DUMP,
        0,
    ];
    let mut needed = 0usize;
    // SAFETY: a size query; `mib` and `needed` are valid for the call.
    if unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            6,
            std::ptr::null_mut(),
            &mut needed,
            std::ptr::null_mut(),
            0,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    // Room for routes added between the two calls.
    needed += needed / 2;
    let mut buf = vec![0u8; needed];
    // SAFETY: `buf` holds `needed` bytes.
    if unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            6,
            buf.as_mut_ptr().cast(),
            &mut needed,
            std::ptr::null_mut(),
            0,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    buf.truncate(needed);
    Ok(parse_route_dump(&buf, |index| {
        let mut name = [0 as libc::c_char; libc::IF_NAMESIZE];
        // SAFETY: `name` has the IF_NAMESIZE bytes if_indextoname writes.
        let named = unsafe { libc::if_indextoname(u32::from(index), name.as_mut_ptr()) };
        if named.is_null() {
            return String::new();
        }
        // SAFETY: if_indextoname wrote a NUL-terminated name into `name`.
        unsafe { std::ffi::CStr::from_ptr(name.as_ptr()) }
            .to_string_lossy()
            .into_owned()
    }))
}

#[cfg(windows)]
fn routes() -> io::Result<Vec<Route>> {
    // iphlpapi's MIB_IPFORWARDTABLE: a count, then rows of 14 DWORDs
    // (libtorrent `enum_routes`, its GetIpForwardTable path).
    const ROW_WORDS: usize = 14;
    const ERROR_INSUFFICIENT_BUFFER: u32 = 122;
    #[link(name = "iphlpapi")]
    unsafe extern "system" {
        fn GetIpForwardTable(table: *mut u32, size: *mut u32, order: i32) -> u32;
    }
    let mut size = 0u32;
    // SAFETY: a size query with a null table.
    let r = unsafe { GetIpForwardTable(std::ptr::null_mut(), &mut size, 0) };
    if r != ERROR_INSUFFICIENT_BUFFER && r != 0 {
        return Err(io::Error::from_raw_os_error(r as i32));
    }
    let mut table = vec![0u32; (size as usize).div_ceil(4) + 1];
    size = u32::try_from(table.len() * 4).unwrap_or(u32::MAX);
    // SAFETY: `table` holds `size` bytes, DWORD aligned.
    let r = unsafe { GetIpForwardTable(table.as_mut_ptr(), &mut size, 0) };
    if r != 0 {
        return Err(io::Error::from_raw_os_error(r as i32));
    }
    // A route through a router, not straight out of the interface.
    const FORWARD_TYPE_INDIRECT: u32 = 4;
    use network_interface::{NetworkInterface, NetworkInterfaceConfig};
    let names: std::collections::HashMap<u32, String> = NetworkInterface::show()
        .map_err(io::Error::other)?
        .into_iter()
        .map(|i| (i.index, i.name))
        .collect();
    let count = table[0] as usize;
    Ok(table[1..]
        .chunks_exact(ROW_WORDS)
        .take(count)
        .map(|row| Route {
            destination: Ipv4Addr::from(row[0].to_ne_bytes()),
            mask: Ipv4Addr::from(row[1].to_ne_bytes()),
            gateway: (row[5] == FORWARD_TYPE_INDIRECT)
                .then(|| Ipv4Addr::from(row[3].to_ne_bytes())),
            interface: names.get(&row[4]).cloned().unwrap_or_default(),
            metric: row[9],
        })
        .collect())
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
)))]
fn routes() -> io::Result<Vec<Route>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no routing table reader on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        time::Duration,
    };

    use std::sync::Arc;

    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
    use tokio_util::sync::CancellationToken;

    use super::{
        GatewayRoute, NATPMP_PORT, NatPmpMapper, Route, default_gateway_of, gateway_route,
        parse_proc_net_route,
    };
    use crate::{
        core::net::DatagramSocket,
        route_interface::RouteInterface,
        test_support::{loopback, loopback_udp},
        upnp::{PortMapRequest, PortMapped, PortmapProtocol, PortmapTransport, tests::hold_clock},
    };

    /// A gateway the test answers by hand.
    struct Gateway {
        socket: Arc<dyn DatagramSocket>,
        mapper: Option<SocketAddr>,
    }

    impl Gateway {
        async fn new() -> Self {
            Self {
                socket: loopback_udp().await,
                mapper: None,
            }
        }

        async fn recv(&mut self) -> Vec<u8> {
            let mut buf = [0u8; 1100];
            let (n, from) = self.socket.recv_from(&mut buf).await.unwrap();
            self.mapper = Some(from);
            buf[..n].to_vec()
        }

        async fn send(&self, packet: &[u8]) {
            self.socket
                .send_to(packet, self.mapper.unwrap())
                .await
                .unwrap();
        }
    }

    struct Running {
        cancel: CancellationToken,
        events: UnboundedReceiver<PortMapped>,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    fn run_mapper(gateway: &Gateway, protocols: &[PortmapProtocol]) -> Running {
        run_mapper_over(
            Ok(GatewayRoute {
                local_ip: Ipv4Addr::LOCALHOST.into(),
                gateway: gateway.socket.bind_addr(),
            }),
            protocols,
        )
    }

    fn run_mapper_over(
        route: Result<GatewayRoute, String>,
        protocols: &[PortmapProtocol],
    ) -> Running {
        let mappings = protocols
            .iter()
            .map(|&protocol| PortMapRequest {
                protocol,
                local_port: 6881,
                external_port: 6881,
            })
            .collect();
        let mapper = NatPmpMapper::new(route, mappings, None, loopback());
        let cancel = CancellationToken::new();
        let (events_tx, events) = unbounded_channel();
        let task = tokio::spawn(mapper.run(cancel.clone(), events_tx));
        Running {
            cancel,
            events,
            task,
        }
    }

    const EXTERNAL: Ipv4Addr = Ipv4Addr::new(203, 0, 113, 9);

    /// A PCP MAP answer to `request`.
    fn pcp_answer(request: &[u8], result: u8, lifetime: u32, epoch: u32, port: u16) -> Vec<u8> {
        let mut p = vec![2, 0x80 | 1, 0, result];
        p.extend_from_slice(&lifetime.to_be_bytes());
        p.extend_from_slice(&epoch.to_be_bytes());
        p.extend_from_slice(&[0; 12]);
        // Nonce, protocol, reserved, internal port.
        p.extend_from_slice(&request[24..42]);
        p.extend_from_slice(&port.to_be_bytes());
        p.extend_from_slice(&EXTERNAL.to_ipv6_mapped().octets());
        p
    }

    /// `suggested`: the external address the request asks for; a loopback
    /// client asks for none until the gateway granted one.
    fn assert_pcp_map(request: &[u8], protocol: u8, lifetime: u32, suggested: Ipv4Addr) {
        assert_eq!(request.len(), 60);
        assert_eq!(&request[..4], &[2, 1, 0, 0]);
        assert_eq!(&request[4..8], &lifetime.to_be_bytes());
        assert_eq!(
            &request[8..24],
            &Ipv4Addr::LOCALHOST.to_ipv6_mapped().octets()
        );
        assert_eq!(request[36], protocol);
        assert_eq!(&request[40..42], &6881u16.to_be_bytes());
        assert_eq!(&request[42..44], &6881u16.to_be_bytes());
        assert_eq!(&request[44..60], &suggested.to_ipv6_mapped().octets());
    }

    fn failed(protocol: PortmapProtocol, error: &str) -> PortMapped {
        PortMapped {
            transport: PortmapTransport::NatPmp,
            protocol,
            local_port: 6881,
            external_port: None,
            external_ip: None,
            error: Some(error.into()),
        }
    }

    fn mapped(protocol: PortmapProtocol, external_ip: Ipv4Addr) -> PortMapped {
        PortMapped {
            transport: PortmapTransport::NatPmp,
            protocol,
            local_port: 6881,
            external_port: Some(6881),
            external_ip: Some(external_ip.into()),
            error: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pcp_maps_tcp_and_udp_refreshes_at_three_quarters_of_the_granted_lifetime_and_removes_them_on_close()
     {
        // A refresh the gateway grants as before reports nothing new: the
        // next reports are the removals.
        let _hold = hold_clock();
        let mut gateway = Gateway::new().await;
        let mut running = run_mapper(&gateway, &[PortmapProtocol::Tcp, PortmapProtocol::Udp]);

        let tcp = gateway.recv().await;
        assert_pcp_map(&tcp, 6, 3600, Ipv4Addr::UNSPECIFIED);
        gateway.send(&pcp_answer(&tcp, 0, 7200, 1000, 6881)).await;
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Tcp, EXTERNAL)
        );
        let udp = gateway.recv().await;
        assert_pcp_map(&udp, 17, 3600, Ipv4Addr::UNSPECIFIED);
        assert_ne!(&udp[24..36], &tcp[24..36], "mappings share a nonce");
        gateway.send(&pcp_answer(&udp, 0, 7200, 1000, 6881)).await;
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Udp, EXTERNAL)
        );

        tokio::time::advance(Duration::from_secs(5400)).await;
        let refresh = gateway.recv().await;
        assert_pcp_map(&refresh, 6, 3600, EXTERNAL);
        assert_eq!(&refresh[24..36], &tcp[24..36]);
        gateway
            .send(&pcp_answer(&refresh, 0, 7200, 1000 + 5400, 6881))
            .await;
        let refresh = gateway.recv().await;
        assert_pcp_map(&refresh, 17, 3600, EXTERNAL);
        assert_eq!(&refresh[24..36], &udp[24..36]);

        // Closing sends a lifetime-0 MAP for each, the UDP refresh still
        // unanswered included, under the mapping's own nonce.
        running.cancel.cancel();
        for (protocol, number, request) in [
            (PortmapProtocol::Tcp, 6, &tcp),
            (PortmapProtocol::Udp, 17, &udp),
        ] {
            let delete = gateway.recv().await;
            assert_pcp_map(&delete, number, 0, EXTERNAL);
            assert_eq!(&delete[24..36], &request[24..36]);
            assert_eq!(
                running.events.recv().await.unwrap(),
                PortMapped {
                    external_port: None,
                    external_ip: None,
                    ..mapped(protocol, EXTERNAL)
                }
            );
        }
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_gateway_without_pcp_is_asked_again_over_nat_pmp() {
        let _hold = hold_clock();
        let mut gateway = Gateway::new().await;
        let mut running = run_mapper(&gateway, &[PortmapProtocol::Tcp]);

        let pcp = gateway.recv().await;
        assert_pcp_map(&pcp, 6, 3600, Ipv4Addr::UNSPECIFIED);
        // A NAT-PMP gateway answers any other version with result 1.
        let mut refusal = vec![0, 0x80 | 1, 0, 1];
        refusal.extend_from_slice(&1000u32.to_be_bytes());
        gateway.send(&refusal).await;

        let mut map = vec![0, 2, 0, 0];
        map.extend_from_slice(&6881u16.to_be_bytes());
        map.extend_from_slice(&6881u16.to_be_bytes());
        map.extend_from_slice(&3600u32.to_be_bytes());
        assert_eq!(gateway.recv().await, map);
        assert_eq!(gateway.recv().await, vec![0, 0]);

        let public = Ipv4Addr::new(198, 51, 100, 4);
        let mut answer = vec![0, 0x80, 0, 0];
        answer.extend_from_slice(&1000u32.to_be_bytes());
        answer.extend_from_slice(&public.octets());
        gateway.send(&answer).await;
        let mut answer = vec![0, 0x80 | 2, 0, 0];
        answer.extend_from_slice(&1000u32.to_be_bytes());
        answer.extend_from_slice(&6881u16.to_be_bytes());
        answer.extend_from_slice(&6881u16.to_be_bytes());
        answer.extend_from_slice(&7200u32.to_be_bytes());
        gateway.send(&answer).await;
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Tcp, public)
        );

        running.cancel.cancel();
        let mut delete = vec![0, 2, 0, 0];
        delete.extend_from_slice(&6881u16.to_be_bytes());
        delete.extend_from_slice(&6881u16.to_be_bytes());
        delete.extend_from_slice(&0u32.to_be_bytes());
        assert_eq!(gateway.recv().await, delete);
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_gateway_is_asked_nine_times_with_linear_backoff_then_again_two_hours_later() {
        let _hold = hold_clock();
        let mut gateway = Gateway::new().await;
        let running = run_mapper(&gateway, &[PortmapProtocol::Tcp]);

        let first = gateway.recv().await;
        for n in 1..9 {
            tokio::time::advance(Duration::from_millis(250 * n)).await;
            assert_eq!(gateway.recv().await, first, "send {}", n + 1);
        }
        // The ninth send waited out: the mapper gives up for two hours.
        tokio::time::advance(Duration::from_millis(250 * 9)).await;
        tokio::time::advance(Duration::from_secs(2 * 60 * 60)).await;
        assert_eq!(gateway.recv().await, first);
        // A fresh round: its first resend comes after one step.
        tokio::time::advance(Duration::from_millis(250)).await;
        assert_eq!(gateway.recv().await, first);

        running.cancel.cancel();
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_gateway_that_never_answers_is_reported_for_each_protocol_until_a_later_answer_maps_it()
     {
        let _hold = hold_clock();
        let mut gateway = Gateway::new().await;
        let mut running = run_mapper(&gateway, &[PortmapProtocol::Tcp, PortmapProtocol::Udp]);

        for (protocol, number) in [(PortmapProtocol::Tcp, 6), (PortmapProtocol::Udp, 17)] {
            let first = gateway.recv().await;
            assert_pcp_map(&first, number, 3600, Ipv4Addr::UNSPECIFIED);
            for n in 1..9 {
                tokio::time::advance(Duration::from_millis(250 * n)).await;
                assert_eq!(gateway.recv().await, first, "send {}", n + 1);
            }
            tokio::time::advance(Duration::from_millis(250 * 9)).await;
            assert_eq!(
                running.events.recv().await.unwrap(),
                PortMapped {
                    transport: PortmapTransport::NatPmp,
                    protocol,
                    local_port: 6881,
                    external_port: None,
                    external_ip: None,
                    error: Some("the gateway never answered".into()),
                }
            );
        }

        // Two hours on the gateway answers the retry: the mapping it grants
        // is the next report.
        tokio::time::advance(Duration::from_secs(2 * 60 * 60)).await;
        let retry = gateway.recv().await;
        assert_pcp_map(&retry, 6, 3600, Ipv4Addr::UNSPECIFIED);
        gateway.send(&pcp_answer(&retry, 0, 7200, 1000, 6881)).await;
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Tcp, EXTERNAL)
        );

        // Closing removes both, and reports removed only the one granted:
        // the UDP mapping stays reported as the gateway left it.
        running.cancel.cancel();
        running.task.await.unwrap().unwrap();
        assert_eq!(
            running.events.recv().await.unwrap(),
            PortMapped {
                external_port: None,
                external_ip: None,
                ..mapped(PortmapProtocol::Tcp, EXTERNAL)
            }
        );
        assert_eq!(running.events.recv().await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_gateway_that_never_answers_again_is_not_reported_again() {
        let _hold = hold_clock();
        let mut gateway = Gateway::new().await;
        let mut running = run_mapper(&gateway, &[PortmapProtocol::Tcp]);

        // Two rounds of nine unanswered sends, two hours apart.
        for _ in 0..2 {
            let first = gateway.recv().await;
            for n in 1..9 {
                tokio::time::advance(Duration::from_millis(250 * n)).await;
                assert_eq!(gateway.recv().await, first, "send {}", n + 1);
            }
            tokio::time::advance(Duration::from_millis(250 * 9)).await;
            tokio::time::advance(Duration::from_secs(2 * 60 * 60)).await;
        }
        // The third round is answered: the grant is the next report, so the
        // second round's failure, the same as the first's, sent nothing.
        let retry = gateway.recv().await;
        gateway.send(&pcp_answer(&retry, 0, 7200, 1000, 6881)).await;
        assert_eq!(
            running.events.recv().await.unwrap(),
            failed(PortmapProtocol::Tcp, "the gateway never answered")
        );
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Tcp, EXTERNAL)
        );
        running.cancel.cancel();
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_route_interface_without_a_gateway_fails_each_mapping_once() {
        let routes = [route([0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], "utun8", 0)];
        let tunnel = RouteInterface {
            name: "utun8".into(),
            ip: Ipv4Addr::new(10, 44, 0, 2),
            point_to_point: true,
        };
        let route = gateway_route(&routes, &tunnel).map_err(|e| format!("{e:#}"));
        let mut running = run_mapper_over(route, &[PortmapProtocol::Tcp, PortmapProtocol::Udp]);
        for protocol in [PortmapProtocol::Tcp, PortmapProtocol::Udp] {
            assert_eq!(
                running.events.recv().await.unwrap(),
                failed(protocol, "no gateway on the route's interface utun8")
            );
        }
        // The mapper is done: nothing more comes.
        assert_eq!(running.events.recv().await, None);
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_socket_that_will_not_bind_fails_each_mapping() {
        // An address this host does not hold.
        let route = GatewayRoute {
            local_ip: Ipv4Addr::new(192, 0, 2, 1).into(),
            gateway: (Ipv4Addr::LOCALHOST, NATPMP_PORT).into(),
        };
        let mut running = run_mapper_over(Ok(route), &[PortmapProtocol::Tcp, PortmapProtocol::Udp]);
        for protocol in [PortmapProtocol::Tcp, PortmapProtocol::Udp] {
            let report = running.events.recv().await.unwrap();
            assert_eq!((report.protocol, report.external_port), (protocol, None));
            let error = report.error.unwrap();
            assert!(
                error.starts_with("error binding the NAT-PMP socket to 192.0.2.1"),
                "{error}"
            );
        }
        assert_eq!(running.events.recv().await, None);
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_mapping_is_reported_and_the_next_one_goes_ahead() {
        let _hold = hold_clock();
        let mut gateway = Gateway::new().await;
        let mut running = run_mapper(&gateway, &[PortmapProtocol::Tcp, PortmapProtocol::Udp]);

        let tcp = gateway.recv().await;
        gateway.send(&pcp_answer(&tcp, 2, 0, 1000, 0)).await;
        assert_eq!(
            running.events.recv().await.unwrap(),
            PortMapped {
                transport: PortmapTransport::NatPmp,
                protocol: PortmapProtocol::Tcp,
                local_port: 6881,
                external_port: None,
                external_ip: None,
                error: Some("not authorized".into()),
            }
        );
        assert_pcp_map(&gateway.recv().await, 17, 3600, Ipv4Addr::UNSPECIFIED);

        running.cancel.cancel();
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn an_epoch_that_went_back_maps_every_port_again() {
        let _hold = hold_clock();
        let mut gateway = Gateway::new().await;
        let mut running = run_mapper(&gateway, &[PortmapProtocol::Tcp, PortmapProtocol::Udp]);

        let tcp = gateway.recv().await;
        gateway.send(&pcp_answer(&tcp, 0, 7200, 1000, 6881)).await;
        let udp = gateway.recv().await;
        gateway.send(&pcp_answer(&udp, 0, 7200, 1000, 6881)).await;
        running.events.recv().await.unwrap();
        running.events.recv().await.unwrap();

        // The gateway restarted and announces itself with a young epoch.
        let mut announce = vec![2, 0x80, 0, 0, 0, 0, 0, 0];
        announce.extend_from_slice(&3u32.to_be_bytes());
        announce.extend_from_slice(&[0; 12]);
        gateway.send(&announce).await;

        let again = gateway.recv().await;
        assert_pcp_map(&again, 6, 3600, EXTERNAL);
        assert_eq!(&again[24..36], &tcp[24..36]);
        gateway.send(&pcp_answer(&again, 0, 7200, 3, 6881)).await;
        let again = gateway.recv().await;
        assert_pcp_map(&again, 17, 3600, EXTERNAL);
        assert_eq!(&again[24..36], &udp[24..36]);

        running.cancel.cancel();
        running.task.await.unwrap().unwrap();
    }

    /// A route; a gateway of 0.0.0.0 is a route straight out of the
    /// interface.
    fn route(
        destination: [u8; 4],
        mask: [u8; 4],
        gateway: [u8; 4],
        interface: &str,
        metric: u32,
    ) -> Route {
        let gateway = Ipv4Addr::from(gateway);
        Route {
            destination: destination.into(),
            mask: mask.into(),
            gateway: (!gateway.is_unspecified()).then_some(gateway),
            interface: interface.into(),
            metric,
        }
    }

    #[test]
    fn the_gateway_is_the_cheapest_default_route_on_the_route_interface() {
        let routes = [
            // The host's primary default goes into a tunnel, its two halves
            // too; the LAN's default is scoped to en0.
            route([0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], "utun8", 0),
            route([0, 0, 0, 0], [128, 0, 0, 0], [0, 0, 0, 0], "utun8", 0),
            route([128, 0, 0, 0], [128, 0, 0, 0], [0, 0, 0, 0], "utun8", 0),
            route([10, 0, 0, 0], [255, 0, 0, 0], [10, 44, 0, 1], "utun8", 0),
            route([0, 0, 0, 0], [0, 0, 0, 0], [172, 16, 4, 1], "en0", 1),
            route([172, 16, 4, 0], [255, 255, 254, 0], [0, 0, 0, 0], "en0", 0),
            // A 0/1 + 128/1 pair through a router is a default.
            route([0, 0, 0, 0], [128, 0, 0, 0], [10, 8, 0, 1], "wg0", 0),
            route([128, 0, 0, 0], [128, 0, 0, 0], [10, 8, 0, 1], "wg0", 0),
            route([0, 0, 0, 0], [0, 0, 0, 0], [192, 168, 1, 1], "eth0", 100),
            route([0, 0, 0, 0], [0, 0, 0, 0], [192, 168, 1, 254], "eth0", 50),
        ];
        assert_eq!(
            default_gateway_of(&routes, "en0"),
            Some(Ipv4Addr::new(172, 16, 4, 1))
        );
        assert_eq!(default_gateway_of(&routes, "utun8"), None);
        assert_eq!(
            default_gateway_of(&routes, "wg0"),
            Some(Ipv4Addr::new(10, 8, 0, 1))
        );
        assert_eq!(
            default_gateway_of(&routes, "eth0"),
            Some(Ipv4Addr::new(192, 168, 1, 254))
        );

        let en0 = RouteInterface {
            name: "en0".into(),
            ip: Ipv4Addr::new(172, 16, 5, 85),
            point_to_point: false,
        };
        assert_eq!(
            gateway_route(&routes, &en0).unwrap(),
            GatewayRoute {
                local_ip: Ipv4Addr::new(172, 16, 5, 85).into(),
                gateway: (Ipv4Addr::new(172, 16, 4, 1), NATPMP_PORT).into(),
            }
        );
        let utun8 = RouteInterface {
            name: "utun8".into(),
            ip: Ipv4Addr::new(10, 44, 0, 2),
            point_to_point: true,
        };
        assert_eq!(
            format!("{:#}", gateway_route(&routes, &utun8).unwrap_err()),
            "no gateway on the route's interface utun8"
        );
    }

    #[test]
    fn proc_net_route_lists_routes_that_are_up() {
        let word = |a: [u8; 4]| format!("{:08X}", u32::from_ne_bytes(a));
        let text = format!(
            "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
             eth0\t{}\t{}\t0003\t0\t0\t100\t{}\t0\t0\t0\n\
             eth0\t{}\t{}\t0001\t0\t0\t100\t{}\t0\t0\t0\n\
             eth1\t{}\t{}\t0002\t0\t0\t10\t{}\t0\t0\t0\n",
            word([0, 0, 0, 0]),
            word([192, 168, 1, 1]),
            word([0, 0, 0, 0]),
            word([192, 168, 1, 0]),
            word([0, 0, 0, 0]),
            word([255, 255, 255, 0]),
            word([0, 0, 0, 0]),
            word([10, 0, 0, 1]),
            word([0, 0, 0, 0]),
        );
        assert_eq!(
            parse_proc_net_route(&text),
            vec![
                route([0, 0, 0, 0], [0, 0, 0, 0], [192, 168, 1, 1], "eth0", 100),
                route(
                    [192, 168, 1, 0],
                    [255, 255, 255, 0],
                    [0, 0, 0, 0],
                    "eth0",
                    100
                ),
            ]
        );
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn a_route_dump_yields_its_ipv4_routes() {
        use nix::libc;

        fn message(flags: i32, index: u16, addrs: &[&[u8]]) -> Vec<u8> {
            let header = std::mem::size_of::<libc::rt_msghdr>();
            let mut body = Vec::new();
            for a in addrs {
                body.extend_from_slice(a);
                body.resize(body.len().div_ceil(4) * 4, 0);
            }
            // SAFETY: an all-zero rt_msghdr is valid.
            let mut rtm: libc::rt_msghdr = unsafe { std::mem::zeroed() };
            rtm.rtm_msglen = u16::try_from(header + body.len()).unwrap();
            rtm.rtm_version = u8::try_from(libc::RTM_VERSION).unwrap();
            rtm.rtm_type = u8::try_from(libc::RTM_GET).unwrap();
            rtm.rtm_index = index;
            rtm.rtm_flags = flags;
            rtm.rtm_addrs = libc::RTA_DST | libc::RTA_GATEWAY | libc::RTA_NETMASK;
            // SAFETY: viewing a plain-data struct as its bytes.
            let mut m = unsafe {
                std::slice::from_raw_parts((&rtm as *const libc::rt_msghdr).cast::<u8>(), header)
            }
            .to_vec();
            m.extend_from_slice(&body);
            m
        }
        fn sin(ip: [u8; 4]) -> Vec<u8> {
            let mut sa = vec![16, u8::try_from(libc::AF_INET).unwrap(), 0, 0];
            sa.extend_from_slice(&ip);
            sa.resize(16, 0);
            sa
        }
        // A link-layer gateway (sockaddr_dl) is no router.
        let link = vec![
            20,
            u8::try_from(libc::AF_LINK).unwrap(),
            4,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ];
        let mut dump = message(
            libc::RTF_GATEWAY | libc::RTF_IFSCOPE,
            7,
            &[&sin([0, 0, 0, 0]), &sin([10, 8, 0, 1]), &[0, 0, 0, 0]],
        );
        dump.extend(message(
            libc::RTF_GATEWAY,
            4,
            &[&sin([0, 0, 0, 0]), &sin([192, 168, 1, 1]), &[0, 0, 0, 0]],
        ));
        dump.extend(message(
            0,
            4,
            &[&sin([192, 168, 1, 0]), &link, &sin([255, 255, 255, 0])],
        ));
        // A tunnel's default and its 0/1 half: a link for a gateway, the
        // half's mask cut after its one non-zero byte.
        dump.extend(message(0, 9, &[&sin([0, 0, 0, 0]), &link, &[0, 0, 0, 0]]));
        dump.extend(message(
            0,
            9,
            &[&sin([0, 0, 0, 0]), &link, &[5, 0, 0, 0, 128]],
        ));

        let routes = super::parse_route_dump(&dump, |index| format!("if{index}"));
        assert_eq!(
            routes,
            vec![
                route([0, 0, 0, 0], [0, 0, 0, 0], [10, 8, 0, 1], "if7", 1),
                route([0, 0, 0, 0], [0, 0, 0, 0], [192, 168, 1, 1], "if4", 0),
                route([192, 168, 1, 0], [255, 255, 255, 0], [0, 0, 0, 0], "if4", 0),
                route([0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], "if9", 0),
                route([0, 0, 0, 0], [128, 0, 0, 0], [0, 0, 0, 0], "if9", 0),
            ]
        );
        assert_eq!(
            default_gateway_of(&routes, "if4"),
            Some(Ipv4Addr::new(192, 168, 1, 1))
        );
        assert_eq!(default_gateway_of(&routes, "if9"), None);
    }
}
