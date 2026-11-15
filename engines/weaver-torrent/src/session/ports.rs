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

#[derive(Clone, Copy, Default)]
struct MappedPorts {
    natpmp: Option<u16>,
    upnp: Option<u16>,
    pcp_ipv6: Option<u16>,
}

impl MappedPorts {
    /// The port mapped for one family: over IPv4 NAT-PMP's before UPnP's
    /// (libtorrent's `tcp_port_mapping` is walked in `portmap_transport`
    /// order, natpmp first), over IPv6 PCP's.
    fn port(&self, ipv6: bool) -> Option<u16> {
        if ipv6 {
            self.pcp_ipv6
        } else {
            self.natpmp.or(self.upnp)
        }
    }

    /// The port `transport` reports.
    fn of(&mut self, transport: PortmapTransport) -> &mut Option<u16> {
        match transport {
            PortmapTransport::NatPmp => &mut self.natpmp,
            PortmapTransport::Upnp => &mut self.upnp,
            PortmapTransport::PcpIpv6 => &mut self.pcp_ipv6,
        }
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

impl Session {
    /// Keep the listen port mapped on the gateway over UPnP and over PCP /
    /// NAT-PMP at once (libtorrent `session_impl::start_upnp` and
    /// `start_natpmp`), announce the TCP port a mapper got, count the
    /// address the gateway gave as the router's vote (`on_port_mapping`),
    /// and pass each report on to `forward`. Both mappers work on the
    /// interface the session's traffic leaves through: the bound device, or
    /// the one holding the address the session's clients send public
    /// traffic from. Neither runs on an interface that reaches only the
    /// local network. A session held to a global IPv6 address, or bound to
    /// a device with one, also maps from that address over PCP, and one on
    /// `[::]` bound to no device from each global IPv6 address (libtorrent
    /// starts a NAT-PMP / PCP mapper on each IPv6 listen socket that is not
    /// local); one held to an IPv6 address maps over IPv4 not at all.
    pub(super) fn start_port_mapping(
        &self,
        requests: Vec<PortMapRequest>,
        bind_device_name: Option<&str>,
        forward: Option<PortMapEvents>,
    ) {
        // A session held to an address only the local network reaches maps
        // nothing (libtorrent `start_natpmp` and `start_upnp` skip a
        // `local_network` socket and a local IPv6 one).
        let listen_ip = self.listen_addr.map(|addr| addr.ip());
        if let Some(ip) = listen_ip
            && crate::route_interface::local_network_address(ip)
        {
            info!(%ip, "no port mapping on an address only the local network reaches");
            return;
        }
        let (events_tx, events) = tokio::sync::mpsc::unbounded_channel();
        let held_to_ipv6 = listen_ip.is_some_and(|ip| ip.is_ipv6() && !ip.is_unspecified());
        if held_to_ipv6 {
            // SSDP and NAT-PMP run over IPv4 alone (libtorrent
            // `start_upnp`; `natpmp::on_reply` falls back to NAT-PMP only
            // on an IPv4 socket).
            info!("no UPnP or IPv4 NAT-PMP on a session held to an IPv6 address");
        } else {
            self.map_ipv4(requests.clone(), bind_device_name, events_tx.clone());
        }
        if let Some(IpAddr::V6(listen)) = listen_ip
            && !self.ipv4_only
        {
            self.map_ipv6(
                listen,
                requests,
                bind_device_name,
                events_tx,
                forward.clone(),
            );
        }
        self.follow_port_mappings(events, self.ssl_mapped_port(), forward);
    }

    /// Map `requests` over UPnP and over PCP / NAT-PMP from the IPv4
    /// interface the session's traffic leaves through.
    fn map_ipv4(
        &self,
        requests: Vec<PortMapRequest>,
        bind_device_name: Option<&str>,
        events_tx: PortMapEvents,
    ) {
        let interface =
            crate::route_interface::route_interface(bind_device_name, self.http_clients.as_ref())
                .map_err(|e| format!("{e:#}"));
        if let Ok(interface) = &interface
            && interface.local_network_only()
        {
            info!(
                ?interface,
                "no port mapping on an interface that reaches only the local network"
            );
            return;
        }
        info!(
            ?requests,
            ?interface,
            "starting UPnP and NAT-PMP / PCP port mapping"
        );
        let upnp_opts = UpnpPortForwarderOptions {
            description: self.identity.user_agent.clone(),
            ..Default::default()
        };
        match UpnpPortForwarder::new(
            requests.clone(),
            Some(upnp_opts),
            interface.clone(),
            self.http_clients.clone(),
        ) {
            Ok(upnp) => self.spawn_port_mapper(
                "upnp_forward",
                upnp.run(self.cancellation_token.clone(), events_tx.clone()),
            ),
            Err(e) => warn!("error starting UPnP port mapping: {e:#}"),
        }
        let route = interface.and_then(|i| GatewayRoute::of(&i).map_err(|e| format!("{e:#}")));
        self.map_over(route, requests, events_tx);
    }

    /// Map `requests` over PCP from the session's global IPv6 addresses:
    /// `listen` when the session is held to it, else one of its device's,
    /// else, on `[::]` bound to no device, each global IPv6 address of each
    /// interface, every one from its own interface's IPv6 gateway
    /// (libtorrent starts a PCP mapper on each IPv6 listen socket that is
    /// not local). The session tells one IPv6 port, so only the first
    /// address's mapping is announced: the one public IPv6 traffic leaves
    /// from. The others are still requested, and their reports passed on.
    /// A session with no such address maps nothing over IPv6.
    fn map_ipv6(
        &self,
        listen: Ipv6Addr,
        requests: Vec<PortMapRequest>,
        bind_device_name: Option<&str>,
        events_tx: PortMapEvents,
        forward: Option<PortMapEvents>,
    ) {
        let interfaces: Vec<Result<crate::route_interface::RouteInterface, String>> =
            match crate::route_interface::ipv6_route_interfaces(
                listen,
                bind_device_name,
                self.http_clients.as_ref(),
            ) {
                Ok(each) => each.into_iter().map(Ok).collect(),
                Err(e) => vec![Err(format!("{e:#}"))],
            };
        if interfaces.is_empty() {
            debug!("no global IPv6 address to map over PCP");
            return;
        }
        let routes = interfaces
            .into_iter()
            .map(|interface| {
                info!(?requests, ?interface, "starting PCP port mapping over IPv6");
                interface.and_then(|i| GatewayRoute::of(&i).map_err(|e| format!("{e:#}")))
            })
            .collect();
        self.map_ipv6_over(routes, requests, events_tx, forward);
    }

    /// Run a PCP mapper for `requests` over each of `routes`, each from its
    /// own address to its own gateway. Only the first's reports move the
    /// port the session announces; every one's are passed on to `forward`.
    pub(super) fn map_ipv6_over(
        &self,
        routes: Vec<Result<GatewayRoute, String>>,
        requests: Vec<PortMapRequest>,
        events_tx: PortMapEvents,
        forward: Option<PortMapEvents>,
    ) {
        let mut unannounced: Option<PortMapEvents> = None;
        for (at, route) in routes.into_iter().enumerate() {
            let events = if at == 0 {
                events_tx.clone()
            } else {
                unannounced
                    .get_or_insert_with(|| {
                        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                        self.pass_on_port_mappings(rx, forward.clone());
                        tx
                    })
                    .clone()
            };
            self.spawn_port_mapper(
                "pcp_ipv6_forward",
                NatPmpMapper::new(route, requests.clone(), None, self.http_clients.clone())
                    .ipv6()
                    .run(self.cancellation_token.clone(), events),
            );
        }
    }

    /// Run a NAT-PMP / PCP mapper for `requests` over `route`, through the
    /// session's clients, reporting to `events`.
    pub(super) fn map_over(
        &self,
        route: Result<GatewayRoute, String>,
        requests: Vec<PortMapRequest>,
        events: PortMapEvents,
    ) {
        self.spawn_port_mapper(
            "natpmp_forward",
            NatPmpMapper::new(route, requests, None, self.http_clients.clone())
                .run(self.cancellation_token.clone(), events),
        );
    }

    /// The local port whose mapping SSL torrents announce: the SSL
    /// listener's, while it announces a port at all.
    pub(super) fn ssl_mapped_port(&self) -> Option<u16> {
        self.ssl_port.listen
    }

    /// Announce the TCP port each of `events` reports, count the address
    /// the gateway gave as the router's vote (`on_port_mapping`), and pass
    /// each report on to `forward`, until every sender of reports is gone.
    /// A report for the local port `ssl` moves the port SSL torrents
    /// announce (libtorrent `ssl_listen_port` reads the SSL socket's mapped
    /// port); every other moves the session's.
    pub(super) fn follow_port_mappings(
        &self,
        mut events: PortMapReceiver,
        ssl: Option<u16>,
        forward: Option<PortMapEvents>,
    ) {
        let announce_port = self.announce_port.clone();
        let ssl_port = self.ssl_port.clone();
        let external_ips = self.external_ips.clone();
        let stats = self.stats.clone();
        self.spawn_port_mapper("port_mapping", async move {
            while let Some(event) = events.recv().await {
                stats.network.port_mapped(&event);
                match &event.error {
                    None => info!(?event, "port mapping"),
                    Some(error) => {
                        warn!(?event.transport, ?event.protocol, "port mapping failed: {error}")
                    }
                }
                if ssl == Some(event.local_port) {
                    ssl_port.on_port_mapping(&event);
                } else {
                    announce_port.on_port_mapping(&event);
                }
                external_ips.on_port_mapping(&event, tokio::time::Instant::now());
                if let Some(forward) = &forward {
                    let _ = forward.send(event);
                }
            }
            Ok(())
        });
    }

    /// Log each of `events` and pass it on to `forward`, with no port
    /// announced from it and no external address counted: the mappings of
    /// an IPv6 address the session does not announce.
    fn pass_on_port_mappings(&self, mut events: PortMapReceiver, forward: Option<PortMapEvents>) {
        let stats = self.stats.clone();
        self.spawn_port_mapper("port_mapping_unannounced", async move {
            while let Some(event) = events.recv().await {
                stats.network.port_mapped(&event);
                match &event.error {
                    None => info!(?event, "port mapping, not announced"),
                    Some(error) => {
                        warn!(?event.transport, ?event.protocol, "port mapping failed: {error}")
                    }
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
