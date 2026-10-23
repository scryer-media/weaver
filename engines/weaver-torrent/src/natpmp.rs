// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Port mapping through the default gateway over PCP (RFC 6887), falling
//! back to NAT-PMP (RFC 6886), after libtorrent's `natpmp.cpp`.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use anyhow::Context;
use futures::FutureExt;
use tokio::{net::UdpSocket, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use crate::upnp::{PortMapEvents, PortMapRequest, PortMapped, PortmapProtocol, PortmapTransport};

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
    /// The gateway granted the mapping and its last answer still stands:
    /// only such a mapping is reported removed on close.
    granted: bool,
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
            granted: false,
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

    async fn send_map_request(&mut self, i: usize, socket: &UdpSocket) {
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
    async fn resend_request(&mut self, i: usize, socket: &UdpSocket, events: &PortMapEvents) {
        let Some(flight) = self.in_flight.filter(|f| f.index == i) else {
            return;
        };
        if self.send_count >= MAX_SENDS {
            self.in_flight = None;
            let m = &mut self.mappings[i];
            m.action = Action::None;
            // Counted from when the last resend fell due.
            m.expires = Some(flight.resend_at + RETRY_FAILED_AFTER);
            m.granted = false;
            debug!(protocol = ?m.request.protocol, "the gateway never answered");
            // Nobody listening is no reason to stop mapping.
            let _ = events.send(PortMapped {
                transport: PortmapTransport::NatPmp,
                protocol: m.request.protocol,
                local_port: m.request.local_port,
                external_port: None,
                external_ip: None,
                error: Some("the gateway never answered".to_owned()),
            });
            return;
        }
        self.send_map_request(i, socket).await;
    }

    async fn send_public_address_request(&self, socket: &UdpSocket) {
        if let Err(e) = socket.send(&[VERSION_NATPMP, OPCODE_PUBLIC_ADDRESS]).await {
            debug!("error sending a public address request: {e:#}");
        }
    }

    /// Send the next mapping that has something to do, if the socket is
    /// free (libtorrent `update_mapping` / `try_next_mapping`).
    async fn start_next(&mut self, socket: &UdpSocket) {
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
    async fn on_timer(&mut self, socket: &UdpSocket, events: &PortMapEvents) {
        let now = Instant::now();
        if let Some(flight) = self.in_flight {
            if flight.resend_at <= now {
                self.resend_request(flight.index, socket, events).await;
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
    /// `m_abort`).
    async fn close(&mut self, socket: &UdpSocket, events: &PortMapEvents) {
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
            // A mapping last reported failed stays as reported.
            if !std::mem::take(&mut m.granted) {
                continue;
            }
            trace!(protocol = ?m.request.protocol, external_port = m.external_port, "port mapping removed");
            let _ = events.send(PortMapped {
                transport: PortmapTransport::NatPmp,
                protocol: m.request.protocol,
                local_port: m.request.local_port,
                external_port: None,
                external_ip: None,
                error: None,
            });
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
    async fn on_reply(&mut self, p: &[u8], socket: &UdpSocket, events: &PortMapEvents) {
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
                    self.resend_request(flight.index, socket, events).await;
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

        let report = |external_port, external_ip, error| {
            // Nobody listening is no reason to stop mapping.
            let _ = events.send(PortMapped {
                transport: PortmapTransport::NatPmp,
                protocol: m.request.protocol,
                local_port: m.request.local_port,
                external_port,
                external_ip,
                error,
            });
        };
        if result != 0 {
            m.expires = Some(now + RETRY_FAILED_AFTER);
            m.granted = false;
            report(None, None, Some(result_message(version, result)));
        } else if m.action == Action::Add {
            m.granted = m.live;
            let external_ip = if pcp { m.external_ip } else { self.external_ip };
            report(m.live.then_some(m.external_port), external_ip, None);
        }
        m.action = Action::None;
        self.in_flight = None;
    }
}

/// Keeps ports mapped on the gateway over PCP, or NAT-PMP when the gateway
/// only speaks that (libtorrent `natpmp`).
pub struct NatPmpMapper {
    gateway: SocketAddr,
    mappings: Vec<PortMapRequest>,
    opts: NatPmpOptions,
}

impl NatPmpMapper {
    /// A mapper for the NAT-PMP / PCP server at `gateway`, normally the
    /// default gateway on [`NATPMP_PORT`].
    pub fn new(
        gateway: SocketAddr,
        mappings: Vec<PortMapRequest>,
        opts: Option<NatPmpOptions>,
    ) -> Self {
        Self {
            gateway,
            mappings,
            opts: opts.unwrap_or_default(),
        }
    }

    /// Keep every port mapped until cancelled, then remove the mappings.
    pub async fn run(self, cancel: CancellationToken, events: PortMapEvents) -> anyhow::Result<()> {
        let unspecified: IpAddr = match self.gateway {
            SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
            SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
        };
        let socket = UdpSocket::bind((unspecified, 0))
            .await
            .context("error binding the NAT-PMP socket")?;
        // Connected: only the gateway's answers come through.
        socket
            .connect(self.gateway)
            .await
            .with_context(|| format!("error connecting to the gateway at {}", self.gateway))?;
        let local_ip = socket.local_addr()?.ip();
        debug!(gateway = %self.gateway, %local_ip, "NAT-PMP / PCP mapper started");

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
                    Ok(n) => state.on_reply(&buf[..n], &socket, &events).await,
                    Err(e) => debug!("error receiving from the gateway: {e:#}"),
                },
                _ = timer => state.on_timer(&socket, &events).await,
            }
        }
        state.close(&socket, &events).await;
        Ok(())
    }
}

/// One IPv4 route (libtorrent `ip_route`).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Route {
    destination: Ipv4Addr,
    gateway: Ipv4Addr,
    interface: String,
    /// Lower wins.
    metric: u32,
}

/// The gateway of the default route, on `device` when one is named
/// (libtorrent `get_gateway`).
fn default_gateway_of(routes: &[Route], device: Option<&str>) -> Option<Ipv4Addr> {
    routes
        .iter()
        .filter(|r| {
            r.destination.is_unspecified()
                && !r.gateway.is_unspecified()
                && device.is_none_or(|d| r.interface == d)
        })
        .min_by_key(|r| r.metric)
        .map(|r| r.gateway)
}

/// The IPv4 default gateway, read from the routing table (libtorrent
/// `enum_routes` / `get_gateway`), on `device` when one is named.
pub fn default_gateway(device: Option<&str>) -> anyhow::Result<Ipv4Addr> {
    let routes = routes().context("error reading the routing table")?;
    default_gateway_of(&routes, device).context("no default route")
}

/// `/proc/net/route`: every address is the kernel's network-order word
/// printed as a native integer.
#[cfg_attr(not(any(target_os = "linux", target_os = "android")), allow(dead_code))]
fn parse_proc_net_route(text: &str) -> Vec<Route> {
    const RTF_UP: u32 = 0x1;
    let hex = |s: &str| u32::from_str_radix(s, 16).ok();
    text.lines()
        .skip(1)
        .filter_map(|line| {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 7 || hex(f[3])? & RTF_UP == 0 {
                return None;
            }
            Some(Route {
                interface: f[0].to_owned(),
                destination: Ipv4Addr::from(hex(f[1])?.to_ne_bytes()),
                gateway: Ipv4Addr::from(hex(f[2])?.to_ne_bytes()),
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
        let (Some(destination), Some(gateway)) = (
            addrs[libc::RTAX_DST as usize].and_then(ipv4),
            addrs[libc::RTAX_GATEWAY as usize].and_then(ipv4),
        ) else {
            continue;
        };
        routes.push(Route {
            destination,
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
    let count = table[0] as usize;
    Ok(table[1..]
        .chunks_exact(ROW_WORDS)
        .take(count)
        .map(|row| Route {
            destination: Ipv4Addr::from(row[0].to_ne_bytes()),
            gateway: Ipv4Addr::from(row[3].to_ne_bytes()),
            interface: row[4].to_string(),
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

    use tokio::{
        net::UdpSocket,
        sync::mpsc::{UnboundedReceiver, unbounded_channel},
    };
    use tokio_util::sync::CancellationToken;

    use super::{NatPmpMapper, Route, default_gateway_of, parse_proc_net_route};
    use crate::upnp::{
        PortMapRequest, PortMapped, PortmapProtocol, PortmapTransport, tests::hold_clock,
    };

    /// A gateway the test answers by hand.
    struct Gateway {
        socket: UdpSocket,
        mapper: Option<SocketAddr>,
    }

    impl Gateway {
        async fn new() -> Self {
            Self {
                socket: UdpSocket::bind("127.0.0.1:0").await.unwrap(),
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
        let mappings = protocols
            .iter()
            .map(|&protocol| PortMapRequest {
                protocol,
                local_port: 6881,
                external_port: 6881,
            })
            .collect();
        let mapper = NatPmpMapper::new(gateway.socket.local_addr().unwrap(), mappings, None);
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
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Tcp, EXTERNAL)
        );
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

    fn route(destination: [u8; 4], gateway: [u8; 4], interface: &str, metric: u32) -> Route {
        Route {
            destination: destination.into(),
            gateway: gateway.into(),
            interface: interface.into(),
            metric,
        }
    }

    #[test]
    fn the_default_gateway_is_the_cheapest_default_route_on_the_device() {
        let routes = [
            route([10, 0, 0, 0], [10, 0, 0, 1], "eth0", 0),
            route([0, 0, 0, 0], [0, 0, 0, 0], "tun0", 0),
            route([0, 0, 0, 0], [192, 168, 1, 1], "eth0", 100),
            route([0, 0, 0, 0], [192, 168, 2, 1], "wlan0", 50),
        ];
        assert_eq!(
            default_gateway_of(&routes, None),
            Some(Ipv4Addr::new(192, 168, 2, 1))
        );
        assert_eq!(
            default_gateway_of(&routes, Some("eth0")),
            Some(Ipv4Addr::new(192, 168, 1, 1))
        );
        assert_eq!(default_gateway_of(&routes, Some("tun0")), None);
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
                route([0, 0, 0, 0], [192, 168, 1, 1], "eth0", 100),
                route([192, 168, 1, 0], [0, 0, 0, 0], "eth0", 100),
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
            rtm.rtm_msglen = (header + body.len()) as u16;
            rtm.rtm_version = libc::RTM_VERSION as u8;
            rtm.rtm_type = libc::RTM_GET as u8;
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
            let mut sa = vec![16, libc::AF_INET as u8, 0, 0];
            sa.extend_from_slice(&ip);
            sa.resize(16, 0);
            sa
        }
        // A link-layer gateway (sockaddr_dl) is not an IPv4 route.
        let link = vec![
            20,
            libc::AF_LINK as u8,
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
            &[&sin([192, 168, 1, 0]), &link, &[0, 0, 0, 0]],
        ));

        let routes = super::parse_route_dump(&dump, |index| format!("if{index}"));
        assert_eq!(
            routes,
            vec![
                route([0, 0, 0, 0], [10, 8, 0, 1], "if7", 1),
                route([0, 0, 0, 0], [192, 168, 1, 1], "if4", 0),
            ]
        );
        assert_eq!(
            default_gateway_of(&routes, None),
            Some(Ipv4Addr::new(192, 168, 1, 1))
        );
    }
}
