// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::{Arc, atomic::Ordering},
};

use crate::dht::Dht;
use crate::natpmp::{GatewayRoute, NatPmpMapper};
use crate::port_mapper::{PortMapper, TRANSPORT_COUNT, TRANSPORTS};
use crate::route_interface::{RouteInterface, ipv6_route_interfaces, route_interface};
use crate::torrent_state::live::{
    peers::connect_order::{ConnectOrder, OurEndpoints},
    peers::holepunch::is_local,
};
use crate::tracker::{ListenPort, ToldPorts};
use crate::upnp::{
    PortMapEvents, PortMapReceiver, PortMapRequest, PortMapped, PortmapProtocol, PortmapTransport,
    UpnpPortForwarder, UpnpPortForwarderOptions,
};
use tracing::{debug, debug_span, info, warn};

use super::Session;

/// The ports one listener gives out, for each address family. Trackers
/// and the DHT are told the announce port set in the options, whatever a
/// mapper gets (libtorrent `session_impl::queue_tracker_request`,
/// session_impl.cpp:1279-1281; `torrent::dht_announce`,
/// torrent.cpp:3060-3073). With none set they are told the external port
/// of the family they are reached over, as libtorrent tells each one the
/// port of the listen socket it announces from (session_impl.cpp:1282-1290,
/// `listen_port(ls)`; the DHT node of each socket, kademlia/node.cpp:449-453).
/// The external port is the TCP port a mapper got on the gateway for that
/// family, else the listen port (libtorrent
/// `listen_socket_t::tcp_external_port`, session_impl.hpp:208-215): over
/// IPv4 NAT-PMP's before UPnP's, over IPv6 PCP's. It is what the extended
/// handshake always carries, for the family of the connection
/// (bt_peer_connection.cpp:2459-2463, `session_impl::listen_port(ssl,
/// local_addr)`, session_impl.cpp:5859-5873). Announces subscribe to the
/// told ports and read them as they go out, so a mapping that lands after
/// a torrent starts is announced by that torrent.
pub(super) struct AnnouncePort {
    /// The listen port, while the session gives out a port at all.
    listen: Option<u16>,
    /// The announce port set in the options.
    set: Option<u16>,
    mapped: parking_lot::Mutex<MappedPorts>,
    port: tokio::sync::watch::Sender<ToldPorts>,
    /// The UDP ports mapped for the listener's uTP socket, as `mapped` is
    /// for TCP.
    udp_mapped: parking_lot::Mutex<MappedPorts>,
    /// Each family's port of `udp_mapped`, IPv4 then IPv6, 0 for none: read
    /// as a PORT message is built (see [`Session::dht_port_told`]).
    udp_external: [std::sync::atomic::AtomicU16; 2],
}

/// The port each transport last reported, by the transport's index in
/// [`TRANSPORTS`].
#[derive(Clone, Copy, Default)]
struct MappedPorts([Option<u16>; TRANSPORT_COUNT]);

impl MappedPorts {
    /// The port mapped for one family: the one held by the family's
    /// transport of the lowest rank in [`TRANSPORTS`] (over IPv4 NAT-PMP's
    /// before UPnP's, over IPv6 PCP's).
    fn port(&self, ipv6: bool) -> Option<u16> {
        TRANSPORTS
            .iter()
            .zip(self.0)
            .filter(|(row, port)| row.ipv6 == ipv6 && port.is_some())
            .min_by_key(|(row, _)| row.precedence)
            .and_then(|(_, port)| port)
    }

    /// The port `transport` reports.
    fn of(&mut self, transport: PortmapTransport) -> &mut Option<u16> {
        &mut self.0[transport as usize]
    }
}

impl AnnouncePort {
    pub(super) fn new(listen: Option<u16>, set: Option<u16>) -> Self {
        Self {
            listen,
            set,
            mapped: Default::default(),
            port: tokio::sync::watch::channel(ToldPorts::both(set.or(listen))).0,
            udp_mapped: Default::default(),
            udp_external: Default::default(),
        }
    }

    /// The UDP port mapped on the gateway for the listener's uTP socket,
    /// over IPv6 or over IPv4.
    fn udp_external(&self, ipv6: bool) -> Option<u16> {
        let port = self.udp_external[usize::from(ipv6)].load(Ordering::Relaxed);
        (port != 0).then_some(port)
    }

    /// The ports trackers and the DHT are told.
    pub(super) fn get(&self) -> ToldPorts {
        *self.port.borrow()
    }

    pub(super) fn subscribe(&self) -> ListenPort {
        self.port.subscribe()
    }

    /// The port peers reach over IPv6, or over IPv4: the one mapped for
    /// that family, else the listen port.
    pub(super) fn external(&self, ipv6: bool) -> Option<u16> {
        let mapped = self.mapped.lock().port(ipv6);
        self.listen.map(|listen| mapped.unwrap_or(listen))
    }

    /// libtorrent `session_impl::on_port_mapping` (session_impl.cpp:
    /// 5751-5755, v2.0.15): a mapper's report, a failure included, sets
    /// that mapper's TCP or UDP port.
    pub(super) fn on_port_mapping(&self, event: &PortMapped) {
        if event.protocol == PortmapProtocol::Udp {
            let mut udp = self.udp_mapped.lock();
            *udp.of(event.transport) = event.external_port;
            for ipv6 in [false, true] {
                let port = udp.port(ipv6).unwrap_or(0);
                self.udp_external[usize::from(ipv6)].store(port, Ordering::Relaxed);
            }
            return;
        }
        let mut mapped = self.mapped.lock();
        *mapped.of(event.transport) = event.external_port;
        let external = |ipv6| {
            self.listen
                .map(|listen| mapped.port(ipv6).unwrap_or(listen))
        };
        let told = match self.set {
            Some(set) => ToldPorts::both(Some(set)),
            None => ToldPorts {
                v4: external(false),
                v6: external(true),
            },
        };
        self.port.send_if_modified(|port| {
            let changed = *port != told;
            *port = told;
            changed
        });
    }
}

/// The ports to map: each of `listen`'s listeners, over the protocols it
/// listens on (libtorrent `session_impl::remap_ports` maps every listen
/// socket, the SSL one included, and a socket only once it is open). Each
/// listener asks for its own port, never the announce port (libtorrent
/// `session_impl::map_port`, session_impl.cpp:2375-2390, asks for the
/// socket's local port).
pub(super) fn port_map_requests(listen: &crate::listen::ListenResult) -> Vec<PortMapRequest> {
    fn protocols(tcp: bool, utp: bool) -> impl Iterator<Item = PortmapProtocol> {
        [
            tcp.then_some(PortmapProtocol::Tcp),
            utp.then_some(PortmapProtocol::Udp),
        ]
        .into_iter()
        .flatten()
    }
    let plain = (
        listen.addr.port(),
        listen.tcp_socket.is_some(),
        listen.utp_socket.is_some(),
    );
    let ssl = listen.ssl.iter().map(|ssl| {
        (
            ssl.addr.port(),
            ssl.tcp_socket.is_some(),
            ssl.utp_socket.is_some(),
        )
    });
    std::iter::once(plain)
        .chain(ssl)
        .flat_map(|(port, tcp, utp)| {
            protocols(tcp, utp).map(move |protocol| PortMapRequest {
                protocol,
                local_port: port,
                external_port: port,
            })
        })
        .collect()
}

/// A port mapper a session runs, and whether its reports move the ports the
/// session tells.
pub(super) struct Planned {
    pub(super) mapper: Box<dyn PortMapper>,
    pub(super) announced: bool,
}

/// What a session's port mappers are made from: the interfaces its traffic
/// leaves through, and a mapper of each kind on one of them.
pub(super) trait MapperSource {
    /// The IPv4 interface the session's traffic leaves through.
    fn ipv4_interface(&self) -> Result<RouteInterface, String>;

    /// The global IPv6 addresses the session maps from over PCP, the one
    /// it announces first (see [`ipv6_route_interfaces`]).
    fn ipv6_interfaces(&self, listen: Ipv6Addr) -> Result<Vec<RouteInterface>, String>;

    /// A UPnP mapper searching from `interface`.
    fn upnp(
        &self,
        requests: Vec<PortMapRequest>,
        interface: Result<RouteInterface, String>,
    ) -> anyhow::Result<Box<dyn PortMapper>>;

    /// A NAT-PMP / PCP mapper asking `interface`'s gateway, or for an IPv6
    /// address over PCP alone when `ipv6`.
    fn natpmp(
        &self,
        requests: Vec<PortMapRequest>,
        interface: Result<RouteInterface, String>,
        ipv6: bool,
    ) -> Box<dyn PortMapper>;
}

/// The mappers a session runs, in the order they start, or `None` when it
/// maps nothing at all. This is the one place that says who runs:
/// - nothing for a session held to an address only the local network
///   reaches, and nothing over IPv4 on an interface that reaches only it
///   (libtorrent `start_natpmp` and `start_upnp` skip a `local_network`
///   socket and a local IPv6 one);
/// - UPnP, then NAT-PMP / PCP, from the IPv4 interface the session's
///   traffic leaves through: the bound device, or the one holding the
///   address the session's clients send public traffic from; none for a
///   session held to an IPv6 address;
/// - PCP from each global IPv6 address of a session on IPv6 that is not
///   IPv4-only: the address it is held to, else one of its device's, else,
///   on `[::]` bound to no device, every one, each from its own interface's
///   IPv6 gateway (libtorrent starts a PCP mapper on each IPv6 listen
///   socket that is not local). The session tells one IPv6 port, so only
///   the first address's mapping is announced: the one public IPv6 traffic
///   leaves from. The others are still requested, and their reports passed
///   on.
///
/// Each family's mappers are made as the iterator reaches them.
pub(super) fn port_mappers<'a>(
    listen_ip: Option<IpAddr>,
    ipv4_only: bool,
    requests: &'a [PortMapRequest],
    source: &'a dyn MapperSource,
) -> Option<impl Iterator<Item = Planned> + 'a> {
    if let Some(ip) = listen_ip
        && crate::route_interface::local_network_address(ip)
    {
        info!(%ip, "no port mapping on an address only the local network reaches");
        return None;
    }
    let held_to_ipv6 = listen_ip.is_some_and(|ip| ip.is_ipv6() && !ip.is_unspecified());
    if held_to_ipv6 {
        // SSDP and NAT-PMP run over IPv4 alone (libtorrent
        // `start_upnp`; `natpmp::on_reply` falls back to NAT-PMP only
        // on an IPv4 socket).
        info!("no UPnP or IPv4 NAT-PMP on a session held to an IPv6 address");
    }
    let ipv6_listen = match listen_ip {
        Some(IpAddr::V6(listen)) if !ipv4_only => Some(listen),
        _ => None,
    };
    let ipv4 = std::iter::once_with(move || {
        if held_to_ipv6 {
            Vec::new()
        } else {
            ipv4_mappers(requests, source)
        }
    });
    let ipv6 = std::iter::once_with(move || {
        ipv6_listen.map_or_else(Vec::new, |listen| ipv6_mappers(listen, requests, source))
    });
    Some(ipv4.chain(ipv6).flatten())
}

/// UPnP and NAT-PMP / PCP from the IPv4 interface the session's traffic
/// leaves through, both announced.
fn ipv4_mappers(requests: &[PortMapRequest], source: &dyn MapperSource) -> Vec<Planned> {
    let interface = source.ipv4_interface();
    if let Ok(interface) = &interface
        && interface.local_network_only()
    {
        info!(
            ?interface,
            "no port mapping on an interface that reaches only the local network"
        );
        return Vec::new();
    }
    info!(
        ?requests,
        ?interface,
        "starting UPnP and NAT-PMP / PCP port mapping"
    );
    let mut mappers = Vec::new();
    match source.upnp(requests.to_vec(), interface.clone()) {
        Ok(upnp) => mappers.push(upnp),
        Err(e) => warn!("error starting UPnP port mapping: {e:#}"),
    }
    mappers.push(source.natpmp(requests.to_vec(), interface, false));
    mappers
        .into_iter()
        .map(|mapper| Planned {
            mapper,
            announced: true,
        })
        .collect()
}

/// PCP from each of the session's global IPv6 addresses, the first one
/// announced.
fn ipv6_mappers(
    listen: Ipv6Addr,
    requests: &[PortMapRequest],
    source: &dyn MapperSource,
) -> Vec<Planned> {
    let interfaces: Vec<Result<RouteInterface, String>> = match source.ipv6_interfaces(listen) {
        Ok(each) => each.into_iter().map(Ok).collect(),
        Err(e) => vec![Err(e)],
    };
    if interfaces.is_empty() {
        debug!("no global IPv6 address to map over PCP");
        return Vec::new();
    }
    interfaces
        .into_iter()
        .enumerate()
        .map(|(at, interface)| {
            info!(?requests, ?interface, "starting PCP port mapping over IPv6");
            Planned {
                mapper: source.natpmp(requests.to_vec(), interface, true),
                announced: at == 0,
            }
        })
        .collect()
}

/// A session's mappers: on the interfaces of its bound device or its
/// clients' route, through its clients.
struct SessionMappers<'a> {
    session: &'a Session,
    device: Option<&'a str>,
}

impl MapperSource for SessionMappers<'_> {
    fn ipv4_interface(&self) -> Result<RouteInterface, String> {
        route_interface(self.device, self.session.http_clients.as_ref())
            .map_err(|e| format!("{e:#}"))
    }

    fn ipv6_interfaces(&self, listen: Ipv6Addr) -> Result<Vec<RouteInterface>, String> {
        ipv6_route_interfaces(listen, self.device, self.session.http_clients.as_ref())
            .map_err(|e| format!("{e:#}"))
    }

    fn upnp(
        &self,
        requests: Vec<PortMapRequest>,
        interface: Result<RouteInterface, String>,
    ) -> anyhow::Result<Box<dyn PortMapper>> {
        let opts = UpnpPortForwarderOptions {
            description: self.session.identity.user_agent.clone(),
            ..Default::default()
        };
        let upnp = UpnpPortForwarder::new(
            requests,
            Some(opts),
            interface,
            self.session.http_clients.clone(),
        )?;
        Ok(Box::new(upnp))
    }

    fn natpmp(
        &self,
        requests: Vec<PortMapRequest>,
        interface: Result<RouteInterface, String>,
        ipv6: bool,
    ) -> Box<dyn PortMapper> {
        let route = interface.and_then(|i| GatewayRoute::of(&i).map_err(|e| format!("{e:#}")));
        let mapper = NatPmpMapper::new(route, requests, None, self.session.http_clients.clone());
        Box::new(if ipv6 { mapper.ipv6() } else { mapper })
    }
}

/// What a report loop does with a mapper's reports.
#[derive(Clone, Copy, Debug)]
pub(super) enum Reports {
    /// Announce the TCP port each reports, and count the address the
    /// gateway gave as the router's vote (`on_port_mapping`). A report for
    /// the local port `ssl` moves the port SSL torrents announce
    /// (libtorrent `ssl_listen_port` reads the SSL socket's mapped port);
    /// every other moves the session's.
    Announce { ssl: Option<u16> },
    /// Announce nothing and count no address: the mappings of an IPv6
    /// address the session does not announce.
    PassOn,
}

impl Session {
    /// Keep the listen port mapped on the gateway over UPnP and over PCP /
    /// NAT-PMP at once (libtorrent `session_impl::start_upnp` and
    /// `start_natpmp`), announce the TCP port a mapper got, count the
    /// address the gateway gave as the router's vote (`on_port_mapping`),
    /// and pass each report on to `forward`. [`port_mappers`] says which
    /// mappers run, on which interface.
    pub(super) fn start_port_mapping(
        &self,
        requests: Vec<PortMapRequest>,
        bind_device_name: Option<&str>,
        forward: Option<PortMapEvents>,
    ) {
        let source = SessionMappers {
            session: self,
            device: bind_device_name,
        };
        let listen_ip = self.listen_addr.map(|addr| addr.ip());
        if let Some(mappers) = port_mappers(listen_ip, self.ipv4_only, &requests, &source) {
            self.run_port_mappers(mappers, forward);
        }
    }

    /// Start each of `mappers` in order, and follow their reports to
    /// `forward`: an announced mapper's move the ports the session tells
    /// ([`Reports::Announce`]), every other's are only passed on
    /// ([`Reports::PassOn`]).
    pub(super) fn run_port_mappers(
        &self,
        mappers: impl IntoIterator<Item = Planned>,
        forward: Option<PortMapEvents>,
    ) {
        let (announced_tx, announced) = tokio::sync::mpsc::unbounded_channel();
        let mut unannounced: Option<PortMapEvents> = None;
        for Planned { mapper, announced } in mappers {
            let events = if announced {
                announced_tx.clone()
            } else {
                unannounced
                    .get_or_insert_with(|| {
                        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                        self.follow_port_mappings(rx, Reports::PassOn, forward.clone());
                        tx
                    })
                    .clone()
            };
            let name = mapper.task_name();
            self.spawn_port_mapper(name, mapper.run(self.cancellation_token.clone(), events));
        }
        drop((announced_tx, unannounced));
        let ssl = self.ssl_mapped_port();
        self.follow_port_mappings(announced, Reports::Announce { ssl }, forward);
    }

    /// The local port whose mapping SSL torrents announce: the SSL
    /// listener's, while it announces a port at all.
    pub(super) fn ssl_mapped_port(&self) -> Option<u16> {
        self.ssl_port.listen
    }

    /// Count each of `events` in the stats, log it, do with it what
    /// `reports` says, and pass it on to `forward`, until every sender of
    /// reports is gone.
    pub(super) fn follow_port_mappings(
        &self,
        mut events: PortMapReceiver,
        reports: Reports,
        forward: Option<PortMapEvents>,
    ) {
        let announce_port = self.announce_port.clone();
        let ssl_port = self.ssl_port.clone();
        let external_ips = self.external_ips.clone();
        let stats = self.stats.clone();
        let name = match reports {
            Reports::Announce { .. } => "port_mapping",
            Reports::PassOn => "port_mapping_unannounced",
        };
        self.spawn_port_mapper(name, async move {
            while let Some(event) = events.recv().await {
                stats.network.port_mapped(&event);
                match (&event.error, reports) {
                    (None, Reports::Announce { .. }) => info!(?event, "port mapping"),
                    (None, Reports::PassOn) => info!(?event, "port mapping, not announced"),
                    (Some(error), _) => {
                        warn!(?event.transport, ?event.protocol, "port mapping failed: {error}")
                    }
                }
                if let Reports::Announce { ssl } = reports {
                    if ssl == Some(event.local_port) {
                        ssl_port.on_port_mapping(&event);
                    } else {
                        announce_port.on_port_mapping(&event);
                    }
                    external_ips.on_port_mapping(&event, tokio::time::Instant::now());
                }
                if let Some(forward) = &forward {
                    let _ = forward.send(event);
                }
            }
            Ok(())
        });
    }

    /// A mapper watches the session's token itself and removes its mappings
    /// on the way out, so it is not dropped on cancel; `stop` waits for it.
    fn spawn_port_mapper(
        &self,
        name: &'static str,
        fut: impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    ) {
        crate::core::spawn_utils::spawn(
            debug_span!(parent: self.rs(), "port_mapping", task = name),
            name,
            self.tasks.track_future(fut),
        );
    }

    /// The port trackers and the DHT are told over the session's first
    /// family: the announce port set in the options, else the one a mapper
    /// got on the gateway for that family, else the listen port. The first
    /// family is IPv6 for a session held to an IPv6 address, else IPv4, as
    /// libtorrent's `session_impl::listen_port()` reads its first listen
    /// socket (session_impl.cpp:5800-5803).
    pub fn announce_port(&self) -> Option<u16> {
        self.announce_port.get().of(self.first_family_is_ipv6())
    }

    /// The ports trackers and the DHT are told over each family.
    pub fn announce_ports(&self) -> ToldPorts {
        self.announce_port.get()
    }

    /// What a torrent's peer list orders its connect candidates by: the
    /// session's start, and the endpoints peers see us at for BEP 40, read
    /// as each candidate is chosen (libtorrent `torrent::get_peer_list_state`,
    /// torrent.cpp:11400-11401). The addresses are
    /// `session_impl::external_address` (session_impl.cpp:7016-7032): for a
    /// peer on a local network the listen address when it is a local one,
    /// for any other our external address of its family, else the listen
    /// address when it is a global one; the unspecified address where none
    /// is. The port is the first family's external port, 0 while we listen
    /// on none (`session_impl::listen_port`, session_impl.cpp:5544-5580).
    pub(crate) fn connect_order(&self) -> ConnectOrder {
        let external_ips = Arc::clone(&self.external_ips);
        let announce_port = Arc::clone(&self.announce_port);
        let listen = self
            .listen_addr
            .map(|addr| addr.ip().to_canonical())
            .filter(|ip| !ip.is_unspecified());
        let first_family_is_ipv6 = self.first_family_is_ipv6();
        ConnectOrder::new(
            self.started,
            Arc::new(move || {
                let mut ours = OurEndpoints::default();
                if let Some(listen) = listen {
                    ours.addresses[usize::from(is_local(listen))][usize::from(listen.is_ipv6())] =
                        listen;
                }
                for ipv6 in [false, true] {
                    if let Some(external) = external_ips.of_family(ipv6) {
                        ours.addresses[0][usize::from(ipv6)] = external;
                    }
                }
                ours.port = announce_port.external(first_family_is_ipv6).unwrap_or(0);
                ours
            }),
        )
    }

    fn first_family_is_ipv6(&self) -> bool {
        self.listen_addr
            .is_some_and(|addr| addr.is_ipv6() && !addr.ip().is_unspecified())
    }

    /// The session hears `event`, a mapper's report on its plain listener.
    #[cfg(test)]
    pub(crate) fn port_mapped(&self, event: &PortMapped) {
        self.announce_port.on_port_mapping(event);
    }

    /// Each change of [`Self::announce_port`], for a test to wait on.
    #[cfg(test)]
    pub(crate) fn announce_port_changes(&self) -> ListenPort {
        self.announce_port.subscribe()
    }

    /// The address the SSL listener took, when the session listens for SSL
    /// torrents' peers.
    pub fn ssl_listen_addr(&self) -> Option<SocketAddr> {
        self.ssl_listen_addr
    }

    /// The port a torrent's trackers and DHT are told: the SSL listener's
    /// for an SSL torrent, else [`Self::announce_port`].
    #[cfg(test)]
    pub(crate) fn announce_port_of(&self, ssl: bool) -> Option<u16> {
        if ssl {
            self.ssl_port.get().of(self.first_family_is_ipv6())
        } else {
            self.announce_port()
        }
    }

    /// The port a torrent's peers reach it at over IPv6, or over IPv4, for
    /// the extended handshake: the one a mapper got for the torrent's
    /// listener over that family, else that listener's port, whatever
    /// announce port is set (libtorrent `session_impl::listen_port(ssl,
    /// local_addr)`, session_impl.cpp:5859-5873, gives the
    /// `tcp_external_port` of the listen socket of the connection's family).
    /// The port a PORT message names for `dht`, over IPv6 or over IPv4: the
    /// UDP port mapped on the gateway for the socket the DHT runs over,
    /// else the DHT's own port, read as the message is built (libtorrent
    /// `bt_peer_connection::write_dht_port`, bt_peer_connection.cpp:
    /// 333-342, and `listen_socket_t::udp_external_port`,
    /// session_impl.hpp:239-247, v2.0.15).
    pub(crate) fn dht_port_told(&self, dht: &Dht, ipv6: bool) -> u16 {
        self.dht_on_listener
            .then(|| self.announce_port.udp_external(ipv6))
            .flatten()
            .unwrap_or_else(|| dht.listen_addr().port())
    }

    pub(crate) fn external_port_of(&self, ssl: bool, ipv6: bool) -> Option<u16> {
        if ssl {
            self.ssl_port.external(ipv6)
        } else {
            self.announce_port.external(ipv6)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv6Addr},
        sync::{Arc, Mutex},
    };

    use futures::{FutureExt, future::BoxFuture};
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    use super::{MapperSource, Planned, Reports, port_mappers};
    use crate::port_mapper::PortMapper;
    use crate::route_interface::RouteInterface;
    use crate::session::{Session, SessionOptions};
    use crate::session_stats::network::{PortMapResult, PortMapper as MapperStat};
    use crate::tracker::ToldPorts;
    use crate::upnp::{PortMapRequest, PortMapped, PortmapProtocol, PortmapTransport};

    /// A session listening on `[::]` that gives out its listen port and
    /// maps nothing itself: port forwarding is off.
    async fn session() -> (TempDir, Arc<Session>, u16) {
        let dir = TempDir::new().unwrap();
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                dht: None,
                persistence: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                listen: Some(crate::ListenerOptions {
                    mode: crate::ListenerMode::TcpOnly,
                    listen_addr: (Ipv6Addr::UNSPECIFIED, 0).into(),
                    enable_upnp_port_forwarding: false,
                    ..Default::default()
                }),
                ..crate::test_support::session_options()
            },
        )
        .await
        .unwrap();
        let port = session.listen_addr().unwrap().port();
        assert_eq!(session.announce_ports(), ToldPorts::both(Some(port)));
        (dir, session, port)
    }

    /// A mapper that reports one mapping it got, then holds it until
    /// cancelled. It sends nothing anywhere.
    struct Fake {
        transport: PortmapTransport,
        name: &'static str,
        local_port: u16,
        external_port: u16,
    }

    impl PortMapper for Fake {
        fn transport(&self) -> PortmapTransport {
            self.transport
        }

        fn task_name(&self) -> &'static str {
            self.name
        }

        fn run(
            self: Box<Self>,
            cancel: CancellationToken,
            events: crate::upnp::PortMapEvents,
        ) -> BoxFuture<'static, anyhow::Result<()>> {
            async move {
                let _ = events.send(PortMapped {
                    transport: self.transport,
                    protocol: PortmapProtocol::Tcp,
                    local_port: self.local_port,
                    external_port: Some(self.external_port),
                    external_ip: None,
                    error: None,
                });
                cancel.cancelled().await;
                Ok(())
            }
            .boxed()
        }
    }

    /// Interfaces as scripted. Each mapper made is a [`Fake`] for
    /// `local_port`, granted 50000 plus the number made before it, and is
    /// noted with the address of the interface it was made on.
    struct Scripted {
        ipv4: Result<RouteInterface, String>,
        ipv6: Result<Vec<RouteInterface>, String>,
        upnp_fails: bool,
        local_port: u16,
        made: Mutex<Vec<(&'static str, Result<IpAddr, String>)>>,
    }

    impl Scripted {
        fn fake(
            &self,
            transport: PortmapTransport,
            name: &'static str,
            interface: Result<RouteInterface, String>,
        ) -> Box<dyn PortMapper> {
            let mut made = self.made.lock().unwrap();
            let external_port = 50000 + u16::try_from(made.len()).unwrap();
            made.push((name, interface.map(|i| i.ip)));
            Box::new(Fake {
                transport,
                name,
                local_port: self.local_port,
                external_port,
            })
        }
    }

    impl MapperSource for Scripted {
        fn ipv4_interface(&self) -> Result<RouteInterface, String> {
            self.ipv4.clone()
        }

        fn ipv6_interfaces(&self, _listen: Ipv6Addr) -> Result<Vec<RouteInterface>, String> {
            self.ipv6.clone()
        }

        fn upnp(
            &self,
            _requests: Vec<PortMapRequest>,
            interface: Result<RouteInterface, String>,
        ) -> anyhow::Result<Box<dyn PortMapper>> {
            if self.upnp_fails {
                anyhow::bail!("no client");
            }
            Ok(self.fake(PortmapTransport::Upnp, "upnp_forward", interface))
        }

        fn natpmp(
            &self,
            _requests: Vec<PortMapRequest>,
            interface: Result<RouteInterface, String>,
            ipv6: bool,
        ) -> Box<dyn PortMapper> {
            if ipv6 {
                self.fake(PortmapTransport::PcpIpv6, "pcp_ipv6_forward", interface)
            } else {
                self.fake(PortmapTransport::NatPmp, "natpmp_forward", interface)
            }
        }
    }

    fn interface(name: &str, ip: &str) -> RouteInterface {
        RouteInterface {
            name: name.into(),
            ip: ip.parse().unwrap(),
            point_to_point: false,
        }
    }

    /// A mapper that starts: its task, the address of its interface or why
    /// there is none, and whether its reports are announced.
    type Started = (&'static str, Result<&'static str, &'static str>, bool);

    /// One session shape: where it listens, what the system shows, the
    /// mappers that start (task, interface address, announced) and the
    /// ports it then tells, `None` for the listen port.
    struct Shape {
        name: &'static str,
        listen: &'static str,
        ipv4_only: bool,
        ipv4: Result<RouteInterface, String>,
        ipv6: Result<Vec<RouteInterface>, String>,
        upnp_fails: bool,
        started: Option<Vec<Started>>,
        told: (Option<u16>, Option<u16>),
    }

    // Who runs follows libtorrent's `start_upnp` and `start_natpmp`:
    // nothing on an address or interface only the local network reaches,
    // no UPnP or IPv4 NAT-PMP for a session held to an IPv6 address, PCP
    // from each global IPv6 address with only the first one's port told.
    // Over IPv4 NAT-PMP's port is told before UPnP's whichever reports.
    #[tokio::test(flavor = "multi_thread")]
    async fn each_session_shape_starts_its_mappers_and_tells_the_announced_ports() {
        let en0 = || Ok(interface("en0", "192.168.1.10"));
        let shapes = [
            Shape {
                name: "IPv4 any",
                listen: "0.0.0.0",
                ipv4_only: false,
                ipv4: en0(),
                ipv6: Err("unused".into()),
                upnp_fails: false,
                started: Some(vec![
                    ("upnp_forward", Ok("192.168.1.10"), true),
                    ("natpmp_forward", Ok("192.168.1.10"), true),
                ]),
                told: (Some(50001), None),
            },
            Shape {
                name: "IPv4 any, UPnP cannot start",
                listen: "0.0.0.0",
                ipv4_only: false,
                ipv4: en0(),
                ipv6: Err("unused".into()),
                upnp_fails: true,
                started: Some(vec![("natpmp_forward", Ok("192.168.1.10"), true)]),
                told: (Some(50000), None),
            },
            Shape {
                name: "IPv4 any on a local-only interface",
                listen: "0.0.0.0",
                ipv4_only: false,
                ipv4: Ok(interface("lo0", "127.0.0.1")),
                ipv6: Err("unused".into()),
                upnp_fails: false,
                started: Some(vec![]),
                told: (None, None),
            },
            Shape {
                name: "IPv4 any with no interface found",
                listen: "0.0.0.0",
                ipv4_only: false,
                ipv4: Err("no route".into()),
                ipv6: Err("unused".into()),
                upnp_fails: false,
                started: Some(vec![
                    ("upnp_forward", Err("no route"), true),
                    ("natpmp_forward", Err("no route"), true),
                ]),
                told: (Some(50001), None),
            },
            Shape {
                name: "held to a local-only IPv4 address",
                listen: "127.0.0.1",
                ipv4_only: false,
                ipv4: en0(),
                ipv6: Err("unused".into()),
                upnp_fails: false,
                started: None,
                told: (None, None),
            },
            Shape {
                name: "held to a local-only IPv6 address",
                listen: "fe80::1",
                ipv4_only: false,
                ipv4: en0(),
                ipv6: Ok(vec![interface("en0", "fe80::1")]),
                upnp_fails: false,
                started: None,
                told: (None, None),
            },
            Shape {
                name: "held to a global IPv6 address",
                listen: "2001:db8::5",
                ipv4_only: false,
                ipv4: en0(),
                ipv6: Ok(vec![interface("en0", "2001:db8::5")]),
                upnp_fails: false,
                started: Some(vec![("pcp_ipv6_forward", Ok("2001:db8::5"), true)]),
                told: (None, Some(50000)),
            },
            Shape {
                name: "[::] with two global IPv6 addresses",
                listen: "::",
                ipv4_only: false,
                ipv4: en0(),
                ipv6: Ok(vec![
                    interface("en0", "2001:db8::5"),
                    interface("en1", "2001:db8::6"),
                ]),
                upnp_fails: false,
                started: Some(vec![
                    ("upnp_forward", Ok("192.168.1.10"), true),
                    ("natpmp_forward", Ok("192.168.1.10"), true),
                    ("pcp_ipv6_forward", Ok("2001:db8::5"), true),
                    ("pcp_ipv6_forward", Ok("2001:db8::6"), false),
                ]),
                told: (Some(50001), Some(50002)),
            },
            Shape {
                name: "[::] with no global IPv6 address",
                listen: "::",
                ipv4_only: false,
                ipv4: en0(),
                ipv6: Ok(vec![]),
                upnp_fails: false,
                started: Some(vec![
                    ("upnp_forward", Ok("192.168.1.10"), true),
                    ("natpmp_forward", Ok("192.168.1.10"), true),
                ]),
                told: (Some(50001), None),
            },
            Shape {
                name: "[::] with the IPv6 interfaces unreadable",
                listen: "::",
                ipv4_only: false,
                ipv4: en0(),
                ipv6: Err("no list".into()),
                upnp_fails: false,
                started: Some(vec![
                    ("upnp_forward", Ok("192.168.1.10"), true),
                    ("natpmp_forward", Ok("192.168.1.10"), true),
                    ("pcp_ipv6_forward", Err("no list"), true),
                ]),
                told: (Some(50001), Some(50002)),
            },
            Shape {
                name: "IPv4-only on [::]",
                listen: "::",
                ipv4_only: true,
                ipv4: en0(),
                ipv6: Ok(vec![interface("en0", "2001:db8::5")]),
                upnp_fails: false,
                started: Some(vec![
                    ("upnp_forward", Ok("192.168.1.10"), true),
                    ("natpmp_forward", Ok("192.168.1.10"), true),
                ]),
                told: (Some(50001), None),
            },
        ];
        for shape in shapes {
            let (_dir, session, port) = session().await;
            let source = Scripted {
                ipv4: shape.ipv4,
                ipv6: shape.ipv6,
                upnp_fails: shape.upnp_fails,
                local_port: port,
                made: Mutex::default(),
            };
            let requests = [PortMapRequest {
                protocol: PortmapProtocol::Tcp,
                local_port: port,
                external_port: port,
            }];
            let listen: IpAddr = shape.listen.parse().unwrap();
            let planned: Option<Vec<Planned>> =
                port_mappers(Some(listen), shape.ipv4_only, &requests, &source)
                    .map(Iterator::collect);
            let started = planned.as_ref().map(|planned| {
                let made = source.made.lock().unwrap();
                assert_eq!(made.len(), planned.len(), "{}", shape.name);
                planned
                    .iter()
                    .zip(made.iter())
                    .map(|(planned, (name, at))| {
                        assert_eq!(planned.mapper.task_name(), *name, "{}", shape.name);
                        (*name, at.clone(), planned.announced)
                    })
                    .collect::<Vec<_>>()
            });
            let wanted = shape.started.map(|started| {
                started
                    .into_iter()
                    .map(|(name, at, announced)| {
                        let at = at.map(|ip| ip.parse().unwrap()).map_err(str::to_owned);
                        (name, at, announced)
                    })
                    .collect::<Vec<_>>()
            });
            assert_eq!(started, wanted, "{}", shape.name);

            let (forward, mut forwarded) = tokio::sync::mpsc::unbounded_channel();
            let planned = planned.unwrap_or_default();
            let count = planned.len();
            session.run_port_mappers(planned, Some(forward));
            for _ in 0..count {
                forwarded.recv().await.unwrap();
            }
            let told = ToldPorts {
                v4: Some(shape.told.0.unwrap_or(port)),
                v6: Some(shape.told.1.unwrap_or(port)),
            };
            assert_eq!(session.announce_ports(), told, "{}", shape.name);
            session.stop().await;
        }
    }

    // Each mapper reports as its transport and runs as the task it ran as
    // before the table: a NAT-PMP mapper asking from an IPv6 address
    // reports as PCP over IPv6.
    #[test]
    fn each_mapper_names_its_transport_and_task() {
        use crate::natpmp::{GatewayRoute, NatPmpMapper};
        use crate::upnp::UpnpPortForwarder;
        let clients = crate::test_support::loopback();
        let requests = vec![PortMapRequest {
            protocol: PortmapProtocol::Tcp,
            local_port: 6881,
            external_port: 6881,
        }];
        let natpmp = |route| NatPmpMapper::new(route, requests.clone(), None, clients.clone());
        let named = |mapper: &dyn PortMapper| (mapper.transport(), mapper.task_name());
        let v6_route = || {
            Ok(GatewayRoute {
                local_ip: Ipv6Addr::LOCALHOST.into(),
                gateway: (Ipv6Addr::LOCALHOST, 5351).into(),
                interface_addresses: vec![Ipv6Addr::LOCALHOST.into()],
            })
        };
        assert_eq!(
            named(&natpmp(Err("none".into()))),
            (PortmapTransport::NatPmp, "natpmp_forward")
        );
        assert_eq!(
            named(&natpmp(v6_route())),
            (PortmapTransport::PcpIpv6, "natpmp_forward")
        );
        assert_eq!(
            named(&natpmp(Err("none".into())).ipv6()),
            (PortmapTransport::PcpIpv6, "pcp_ipv6_forward")
        );
        assert_eq!(
            named(&natpmp(v6_route()).ipv6()),
            (PortmapTransport::PcpIpv6, "pcp_ipv6_forward")
        );
        let upnp =
            UpnpPortForwarder::new(requests.clone(), None, Err("none".into()), clients.clone())
                .unwrap();
        assert_eq!(named(&upnp), (PortmapTransport::Upnp, "upnp_forward"));
    }

    // libtorrent `session_impl::on_port_mapping`: an announced report sets
    // the port told and is the router's vote on our address. The reports
    // of an IPv6 address the session does not announce do neither; both
    // are counted and passed on.
    #[tokio::test(flavor = "multi_thread")]
    async fn only_an_announced_report_moves_the_told_port_and_votes() {
        let (_dir, session, port) = session().await;
        let report = |external_port, external_ip: &str| PortMapped {
            transport: PortmapTransport::NatPmp,
            protocol: PortmapProtocol::Tcp,
            local_port: port,
            external_port: Some(external_port),
            external_ip: Some(external_ip.parse().unwrap()),
            error: None,
        };
        let (forward, mut forwarded) = tokio::sync::mpsc::unbounded_channel();
        let (pass_on, pass_on_rx) = tokio::sync::mpsc::unbounded_channel();
        let (announce, announce_rx) = tokio::sync::mpsc::unbounded_channel();
        session.follow_port_mappings(pass_on_rx, Reports::PassOn, Some(forward.clone()));
        session.follow_port_mappings(announce_rx, Reports::Announce { ssl: None }, Some(forward));
        let mapped = |session: &Session| {
            session.stats.network.snapshot().port_maps[MapperStat::NatPmp as usize]
                [PortMapResult::Mapped as usize]
        };

        let unannounced = report(40000, "203.0.113.7");
        pass_on.send(unannounced.clone()).unwrap();
        assert_eq!(forwarded.recv().await, Some(unannounced));
        assert_eq!(session.announce_ports(), ToldPorts::both(Some(port)));
        assert_eq!(session.external_ips.of_family(false), None);
        assert_eq!(mapped(&session), 1);

        let announced = report(50000, "203.0.113.5");
        announce.send(announced.clone()).unwrap();
        assert_eq!(forwarded.recv().await, Some(announced));
        assert_eq!(
            session.announce_ports(),
            ToldPorts {
                v4: Some(50000),
                v6: Some(port),
            }
        );
        assert_eq!(
            session.external_ips.of_family(false),
            Some("203.0.113.5".parse().unwrap())
        );
        assert_eq!(mapped(&session), 2);
        // `stop` waits for each report loop, which ends with its senders.
        drop((pass_on, announce));
        session.stop().await;
    }
}
