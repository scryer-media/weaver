// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use anyhow::{Context, bail};
use bstr::BStr;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use librqbit_dualstack_sockets::{BindDevice, UdpSocket};
use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use reqwest::{Client, StatusCode};
use serde_derive::Deserialize;
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, debug_span, trace, warn};
use url::Url;

/// What a mapping forwards (libtorrent `portmap_protocol`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PortmapProtocol {
    Tcp,
    Udp,
}

impl PortmapProtocol {
    fn upnp_name(self) -> &'static str {
        match self {
            PortmapProtocol::Tcp => "TCP",
            PortmapProtocol::Udp => "UDP",
        }
    }
}

/// Which mapper made a mapping (libtorrent `portmap_transport`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PortmapTransport {
    NatPmp,
    Upnp,
}

/// One mapping a mapper keeps: the gateway forwards `external_port` to
/// `local_port` on this host (libtorrent `add_mapping`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortMapRequest {
    pub protocol: PortmapProtocol,
    pub local_port: u16,
    pub external_port: u16,
}

/// A mapper's report on one mapping (libtorrent `portmap_callback::on_port_mapping`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortMapped {
    pub transport: PortmapTransport,
    pub protocol: PortmapProtocol,
    pub local_port: u16,
    /// The port the gateway forwards, `None` once the mapping failed or was removed.
    pub external_port: Option<u16>,
    /// The gateway's external address, when it told us.
    pub external_ip: Option<IpAddr>,
    pub error: Option<String>,
}

/// Where a mapper sends its reports. A mapper keeps working when nobody listens.
pub type PortMapEvents = UnboundedSender<PortMapped>;

/// The services a gateway maps ports through (libtorrent `find_control_url`).
const PORT_MAPPING_SERVICE_TYPES: [&str; 3] = [
    "urn:schemas-upnp-org:service:WANIPConnection:1",
    "urn:schemas-upnp-org:service:WANIPConnection:2",
    "urn:schemas-upnp-org:service:WANPPPConnection:1",
];
const SSDP_MULTICAST_IP: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(239, 255, 255, 250), 1900));
/// Every root device answers; its description says whether it maps ports
/// (libtorrent `discover_device_impl`).
pub const SSDP_SEARCH_ROOT_ST: &str = "upnp:rootdevice";
/// libtorrent `on_reply`: at most this many gateways at once.
const MAX_GATEWAYS: usize = 50;
/// libtorrent `connect`: the description fetch may take this long.
const DESCRIPTION_TIMEOUT: Duration = Duration::from_secs(30);
/// libtorrent `update_map`: a SOAP call may take this long.
const SOAP_TIMEOUT: Duration = Duration::from_secs(10);
/// libtorrent `update_map`: a mapping that failed this often is given up.
const MAX_FAILCOUNT: u32 = 5;
/// libtorrent `on_upnp_map_response`: errors 718 and 501 pick another
/// external port this many times.
const MAX_PORT_CONFLICTS: u32 = 4;

pub fn make_ssdp_search_request(kind: &str) -> String {
    format!(
        "M-SEARCH * HTTP/1.1\r\n\
            Host: 239.255.255.250:1900\r\n\
            Man: \"ssdp:discover\"\r\n\
            MX: 3\r\n\
            ST: {kind}\r\n\
            \r\n"
    )
}

pub fn get_local_ip_relative_to(
    local_dest: SocketAddr,
    interfaces: &[NetworkInterface],
) -> anyhow::Result<IpAddr> {
    fn masked_v4(ip: Ipv4Addr, mask: Ipv4Addr) -> u32 {
        ip.to_bits() & mask.to_bits()
    }

    fn masked_v6(ip: Ipv6Addr, mask: Ipv6Addr) -> u128 {
        ip.to_bits() & mask.to_bits()
    }

    for i in interfaces {
        for addr in i.addr.iter() {
            match (local_dest, addr.ip(), addr.netmask()) {
                // We are connecting to ourselves, return itself.
                (l, a, _) if l.ip() == a => return Ok(addr.ip()),
                // IPv4 masks match.
                (SocketAddr::V4(l), IpAddr::V4(a), Some(IpAddr::V4(m)))
                    if masked_v4(*l.ip(), m) == masked_v4(a, m) =>
                {
                    return Ok(addr.ip());
                }
                // Return IPv6 link-local addresses when source is link-local address and there's a scope_id set.
                (SocketAddr::V6(l), IpAddr::V6(a), _)
                    if l.ip().is_unicast_link_local() && l.scope_id() > 0 =>
                {
                    if a.is_unicast_link_local() && l.scope_id() == i.index {
                        return Ok(addr.ip());
                    }
                }
                // If V6 masks match, return.
                (SocketAddr::V6(l), IpAddr::V6(a), Some(IpAddr::V6(m)))
                    if masked_v6(*l.ip(), m) == masked_v6(a, m) =>
                {
                    return Ok(addr.ip());
                }
                // For IPv6 fallback to returning a random (first encountered) IPv6 address.
                (SocketAddr::V6(_), IpAddr::V6(_), None) => return Ok(addr.ip()),
                _ => continue,
            }
        }
    }
    bail!("couldn't find a local ip address")
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct RootDesc {
    /// The base the device's relative URLs resolve against, when it names one.
    #[serde(rename = "URLBase", default)]
    pub url_base: Option<String>,
    #[serde(rename = "device")]
    pub devices: Vec<Device>,
}

#[derive(Default, Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeviceList {
    #[serde(rename = "device")]
    pub devices: Vec<Device>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Device {
    #[serde(rename = "deviceType")]
    pub device_type: String,
    #[serde(rename = "friendlyName", default)]
    pub friendly_name: String,
    #[serde(rename = "serviceList", default)]
    pub service_list: ServiceList,
    #[serde(rename = "deviceList", default)]
    pub device_list: DeviceList,
}

impl Device {
    /// This device's services, then its sub-devices' in document order.
    pub fn iter_services(&self) -> Box<dyn Iterator<Item = &Service> + '_> {
        Box::new(
            self.service_list.services.iter().chain(
                self.device_list
                    .devices
                    .iter()
                    .flat_map(|d| d.iter_services()),
            ),
        )
    }

    pub fn name(&self) -> &str {
        if self.friendly_name.is_empty() {
            return &self.device_type;
        }
        &self.friendly_name
    }
}

#[derive(Clone, Debug, Deserialize, Default, PartialEq, Eq)]
pub struct ServiceList {
    #[serde(rename = "service", default)]
    pub services: Vec<Service>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Service {
    #[serde(rename = "serviceType")]
    pub service_type: String,
    #[serde(rename = "controlURL")]
    pub control_url: String,
    #[serde(rename = "SCPDURL")]
    pub scpd_url: String,
    #[serde(rename = "eventSubURL", default)]
    pub event_sub_url: Option<String>,
}

impl Service {
    fn maps_ports(&self) -> bool {
        PORT_MAPPING_SERVICE_TYPES
            .iter()
            .any(|t| t.eq_ignore_ascii_case(&self.service_type))
    }
}

impl RootDesc {
    /// The first port-mapping service anywhere in the description, as
    /// libtorrent `find_control_url` takes the first (or only) one in the
    /// router's listing, whatever the root device type (IGD:1 or IGD:2) and
    /// however deep the WANDevice / WANConnectionDevice nesting.
    pub fn port_mapping_service(&self) -> Option<&Service> {
        self.devices
            .iter()
            .flat_map(|d| d.iter_services())
            .find(|s| s.maps_ports())
    }
}

#[derive(Debug)]
pub struct UpnpDiscoverResponse {
    pub received_from: SocketAddr,
    pub location: Url,
}

/// A host a real gateway never uses for its URLs (libtorrent
/// `invalid_upnp_host`): refusing it keeps a forged SSDP reply from pointing
/// the mapper somewhere else.
fn invalid_upnp_host(url: &Url) -> bool {
    let ip = match url.host() {
        Some(url::Host::Ipv4(ip)) => IpAddr::V4(ip),
        Some(url::Host::Ipv6(ip)) => IpAddr::V6(ip),
        Some(url::Host::Domain(_)) => return false,
        None => return true,
    };
    let link_local = match ip {
        IpAddr::V4(ip) => ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_unicast_link_local(),
    };
    ip.is_unspecified() || ip.is_loopback() || link_local || ip.is_multicast()
}

pub fn parse_upnp_discover_response(
    buf: &[u8],
    received_from: SocketAddr,
) -> anyhow::Result<UpnpDiscoverResponse> {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut resp = httparse::Response::new(&mut headers);
    resp.parse(buf).context("error parsing response")?;

    trace!(?resp, "parsed SSDP response");
    match resp.code {
        Some(200) => {}
        other => anyhow::bail!("bad response code {other:?}, expected 200"),
    }
    let mut location = None;
    for header in resp.headers {
        if header.name.eq_ignore_ascii_case("location") {
            location =
                Some(std::str::from_utf8(header.value).context("bad utf-8 in location header")?)
        }
    }
    let location = location.context("missing location header")?;
    let location =
        Url::parse(location).with_context(|| format!("failed parsing location {location}"))?;
    // libtorrent `on_reply`: plain HTTP on a real port at a plausible host.
    if location.scheme() != "http" {
        bail!("unsupported protocol in location {location}");
    }
    if location.port_or_known_default() == Some(0) {
        bail!("location {location} has port 0");
    }
    if invalid_upnp_host(&location) {
        bail!("location {location} names an address no gateway uses");
    }
    Ok(UpnpDiscoverResponse {
        location,
        received_from,
    })
}

pub async fn discover_once(
    tx: &UnboundedSender<UpnpDiscoverResponse>,
    kind: &str,
    timeout: Duration,
    bind_device: Option<&BindDevice>,
) -> anyhow::Result<()> {
    // TODO: do we need IPv6 support here? I can't test it, don't have the hardware for it (router / IPv6 provider).
    let socket = UdpSocket::bind_udp(
        (Ipv4Addr::UNSPECIFIED, 0).into(),
        librqbit_dualstack_sockets::BindOpts {
            device: bind_device,
            ..Default::default()
        },
    )?;

    let message = make_ssdp_search_request(kind);
    socket
        .send_to(message.as_bytes(), SSDP_MULTICAST_IP)
        .await
        .with_context(|| format!("failed to send SSDP search request to {SSDP_MULTICAST_IP}"))?;

    let mut buffer = [0; 2048];

    let timeout = tokio::time::sleep(timeout);
    let mut timed_out = false;
    tokio::pin!(timeout);

    let mut discovered = 0;

    while !timed_out {
        tokio::select! {
            _ = &mut timeout, if !timed_out => {
                timed_out = true;
            }
            Ok((len, addr)) = socket.recv_from(&mut buffer), if !timed_out => {
                let response = &buffer[..len];
                match parse_upnp_discover_response(response, addr) {
                    Ok(r) => {
                        tx.send(r)?;
                        discovered += 1;
                    },
                    Err(e) => warn!(response=?BStr::new(response), "failed to parse SSDP response: {e:#}"),
                };
            },
        }
    }

    debug!("discovered {discovered} endpoints");
    Ok(())
}

/// The text of the first element called `name`, whatever its namespace
/// prefix (libtorrent `find_error_code` / `find_ip_address`).
fn xml_element_text<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let mut rest = xml;
    while let Some(start) = rest.find('<') {
        rest = &rest[start + 1..];
        let end = rest.find('>')?;
        let tag = &rest[..end];
        rest = &rest[end + 1..];
        if tag.starts_with(['/', '?', '!']) || tag.ends_with('/') {
            continue;
        }
        let tag_name = tag.split_whitespace().next().unwrap_or_default();
        let local_name = tag_name.rsplit(':').next().unwrap_or(tag_name);
        if local_name == name {
            let text_end = rest.find('<').unwrap_or(rest.len());
            return Some(rest[..text_end].trim());
        }
    }
    None
}

/// The UPnP error code of a SOAP fault, if the body carries one.
fn soap_error_code(body: &str) -> Option<u32> {
    xml_element_text(body, "errorCode")?.parse().ok()
}

pub struct UpnpPortForwarderOptions {
    /// libtorrent `upnp_lease_duration`.
    pub lease_duration: Duration,
    pub discover_interval: Duration,
    pub discover_timeout: Duration,
    /// `NewPortMappingDescription`; libtorrent sends its user agent.
    pub description: String,
}

impl Default for UpnpPortForwarderOptions {
    fn default() -> Self {
        Self {
            discover_interval: Duration::from_secs(60),
            discover_timeout: Duration::from_secs(10),
            lease_duration: Duration::from_secs(3600),
            description: crate::client_name_and_version().to_owned(),
        }
    }
}

/// A gateway's port-mapping service once its description is read
/// (libtorrent `upnp::rootdevice`).
struct Gateway {
    control_url: Url,
    service_type: String,
    local_ip: IpAddr,
    /// Cleared by error 725, the gateway takes only permanent mappings
    /// (libtorrent `rootdevice::use_lease_duration`).
    use_lease_duration: bool,
    external_ip: Option<IpAddr>,
}

/// One mapping on one gateway (libtorrent `upnp::mapping_t`).
struct GatewayMapping {
    request: PortMapRequest,
    /// The port asked of the gateway: the requested one until a conflict
    /// moves it.
    external_port: u16,
    failcount: u32,
    /// When to (re)send `AddPortMapping`; `None` for a permanent, failed or
    /// given-up mapping.
    next_add: Option<Instant>,
    /// The external port the gateway last granted: what closing removes.
    granted: Option<u16>,
}

impl GatewayMapping {
    fn new(request: PortMapRequest) -> Self {
        Self {
            request,
            external_port: request.external_port,
            failcount: 0,
            next_add: Some(Instant::now()),
            granted: None,
        }
    }
}

pub struct UpnpPortForwarder {
    mappings: Vec<PortMapRequest>,
    opts: UpnpPortForwarderOptions,
    bind_device: Option<BindDevice>,
    client: Client,
}

impl UpnpPortForwarder {
    pub fn new(
        mappings: Vec<PortMapRequest>,
        opts: Option<UpnpPortForwarderOptions>,
        bind_device: Option<BindDevice>,
    ) -> anyhow::Result<Self> {
        if mappings.is_empty() {
            bail!("empty ports")
        }
        // The gateway is on the local network: never reach it through a proxy.
        let client = Client::builder()
            .no_proxy()
            .build()
            .context("error building the UPnP HTTP client")?;
        Ok(Self {
            mappings,
            opts: opts.unwrap_or_default(),
            bind_device,
            client,
        })
    }

    async fn discovery(&self, tx: UnboundedSender<UpnpDiscoverResponse>) {
        let mut discover_interval = tokio::time::interval(self.opts.discover_interval);
        discover_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            discover_interval.tick().await;
            if let Err(e) = discover_once(
                &tx,
                SSDP_SEARCH_ROOT_ST,
                self.opts.discover_timeout,
                self.bind_device.as_ref(),
            )
            .await
            {
                warn!("failed to run SSDP/UPNP discovery: {e:#}");
            }
        }
    }

    fn report(
        &self,
        events: &PortMapEvents,
        gateway: &Gateway,
        mapping: &GatewayMapping,
        result: Result<(), String>,
    ) {
        let (external_port, external_ip, error) = match result {
            Ok(()) => (Some(mapping.external_port), gateway.external_ip, None),
            Err(e) => (None, None, Some(e)),
        };
        // Nobody listening is no reason to stop mapping.
        let _ = events.send(PortMapped {
            transport: PortmapTransport::Upnp,
            protocol: mapping.request.protocol,
            local_port: mapping.request.local_port,
            external_port,
            external_ip,
            error,
        });
    }

    /// POST one SOAP action to the gateway (libtorrent `upnp::post`).
    async fn soap(
        &self,
        gateway: &Gateway,
        action: &str,
        arguments: &str,
    ) -> anyhow::Result<(StatusCode, String)> {
        let namespace = &gateway.service_type;
        let body = format!(
            "<?xml version=\"1.0\"?>\n\
             <s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" \
             s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\
             <s:Body><u:{action} xmlns:u=\"{namespace}\">{arguments}</u:{action}></s:Body>\
             </s:Envelope>"
        );
        let response = self
            .client
            .post(gateway.control_url.clone())
            .header("Content-Type", "text/xml; charset=\"utf-8\"")
            .header("SOAPAction", format!("\"{namespace}#{action}\""))
            .timeout(SOAP_TIMEOUT)
            .body(body)
            .send()
            .await
            .with_context(|| format!("error sending {action}"))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .with_context(|| format!("error reading the {action} response"))?;
        trace!(%status, text, action, "SOAP response");
        Ok((status, text))
    }

    /// Read the gateway's description and find its port-mapping service
    /// (libtorrent `on_upnp_xml`), then ask for its external address
    /// (`get_ip_address` / `on_upnp_get_ip_address_response`).
    async fn open_gateway(&self, found: &UpnpDiscoverResponse) -> anyhow::Result<Gateway> {
        let response = self
            .client
            .get(found.location.clone())
            .timeout(DESCRIPTION_TIMEOUT)
            .send()
            .await
            .context("failed to fetch the device description")?;
        if response.status() != StatusCode::OK {
            bail!("device description answered {}", response.status());
        }
        let text = response
            .text()
            .await
            .context("failed to read the device description")?;
        trace!(location = %found.location, text, "device description");
        let desc: RootDesc =
            quick_xml::de::from_str(&text).context("failed to parse the device description")?;
        let service = desc
            .port_mapping_service()
            .context("no port mapping service in the device description")?;
        let base = match desc.url_base.as_deref() {
            Some(base) => Url::parse(base).context("bad URLBase")?,
            None => found.location.clone(),
        };
        let control_url = base.join(&service.control_url).context("bad control URL")?;
        // The description is untrusted, like the SSDP reply: its control URL
        // must stay on the host that served it.
        if control_url.host() != found.location.host() {
            bail!(
                "control URL {control_url} is not on the device at {}",
                found.location
            );
        }
        let nics = NetworkInterface::show().context("error listing network interfaces")?;
        let local_ip = get_local_ip_relative_to(found.received_from, &nics).with_context(|| {
            format!(
                "can't determine local IP relative to {}",
                found.received_from
            )
        })?;
        let mut gateway = Gateway {
            control_url,
            service_type: service.service_type.clone(),
            local_ip,
            use_lease_duration: true,
            external_ip: None,
        };
        // A gateway that does not say its address still maps ports.
        match self.soap(&gateway, "GetExternalIPAddress", "").await {
            Ok((status, body)) if status.is_success() => {
                gateway.external_ip =
                    xml_element_text(&body, "NewExternalIPAddress").and_then(|ip| ip.parse().ok());
                debug!(external_ip = ?gateway.external_ip, "gateway external address");
            }
            Ok((status, body)) => {
                debug!(%status, code = ?soap_error_code(&body), "error getting the external address")
            }
            Err(e) => debug!("error getting the external address: {e:#}"),
        }
        Ok(gateway)
    }

    /// Send `AddPortMapping` until the gateway takes it or gives a final
    /// answer (libtorrent `update_map` / `create_port_mapping` /
    /// `on_upnp_map_response`). An error reaching the gateway at all fails
    /// the gateway, as libtorrent disables the device.
    async fn add_port_mapping(
        &self,
        gateway: &mut Gateway,
        mapping: &mut GatewayMapping,
        events: &PortMapEvents,
    ) -> anyhow::Result<()> {
        mapping.next_add = None;
        loop {
            if mapping.failcount > MAX_FAILCOUNT {
                self.report(events, gateway, mapping, Err("gave up".into()));
                return Ok(());
            }
            let lease = if gateway.use_lease_duration {
                self.opts.lease_duration
            } else {
                Duration::ZERO
            };
            let arguments = format!(
                "<NewRemoteHost></NewRemoteHost>\
                 <NewExternalPort>{}</NewExternalPort>\
                 <NewProtocol>{}</NewProtocol>\
                 <NewInternalPort>{}</NewInternalPort>\
                 <NewInternalClient>{}</NewInternalClient>\
                 <NewEnabled>1</NewEnabled>\
                 <NewPortMappingDescription>{}</NewPortMappingDescription>\
                 <NewLeaseDuration>{}</NewLeaseDuration>",
                mapping.external_port,
                mapping.request.protocol.upnp_name(),
                mapping.request.local_port,
                gateway.local_ip,
                self.opts.description,
                lease.as_secs(),
            );
            let (status, body) = match self.soap(gateway, "AddPortMapping", &arguments).await {
                Ok(r) => r,
                Err(e) => {
                    self.report(events, gateway, mapping, Err(format!("{e:#}")));
                    return Err(e);
                }
            };
            match soap_error_code(&body) {
                // The gateway takes only permanent leases.
                Some(725) => {
                    gateway.use_lease_duration = false;
                    mapping.failcount += 1;
                }
                // Another client holds the external port (some gateways say
                // 501 for it): ask for a random one.
                Some(718 | 501) if mapping.failcount < MAX_PORT_CONFLICTS => {
                    use rand::RngExt;
                    mapping.external_port = 40000 + rand::rng().random_range(0..10000u16);
                    mapping.failcount += 1;
                }
                Some(code) => {
                    self.report(events, gateway, mapping, Err(format!("UPnP error {code}")));
                    return Ok(());
                }
                None if !status.is_success() => {
                    self.report(events, gateway, mapping, Err(format!("HTTP {status}")));
                    return Ok(());
                }
                None => {
                    debug!(
                        protocol = ?mapping.request.protocol,
                        external_port = mapping.external_port,
                        "port mapped"
                    );
                    mapping.failcount = 0;
                    mapping.granted = Some(mapping.external_port);
                    // libtorrent `on_upnp_map_response`: renew at three
                    // quarters of the lease; a permanent mapping stays.
                    if !lease.is_zero() {
                        mapping.next_add = Some(Instant::now() + lease * 3 / 4);
                    }
                    self.report(events, gateway, mapping, Ok(()));
                    return Ok(());
                }
            }
        }
    }

    /// Add every mapping that is due (libtorrent `on_expire`).
    async fn add_due_mappings(
        &self,
        gateway: &mut Gateway,
        mappings: &mut [GatewayMapping],
        events: &PortMapEvents,
    ) -> anyhow::Result<()> {
        let now = Instant::now();
        for mapping in mappings {
            if mapping.next_add.is_some_and(|t| t <= now) {
                self.add_port_mapping(gateway, mapping, events).await?;
            }
        }
        Ok(())
    }

    /// Remove every mapping the gateway granted (libtorrent `upnp::close`,
    /// `delete_port_mapping`, `on_upnp_unmap_response`) and report each
    /// gone, with the gateway's error if it refused.
    async fn delete_port_mappings(
        &self,
        gateway: &Gateway,
        mappings: &mut [GatewayMapping],
        events: &PortMapEvents,
    ) {
        for mapping in mappings {
            let Some(external_port) = mapping.granted.take() else {
                continue;
            };
            let arguments = format!(
                "<NewRemoteHost></NewRemoteHost>\
                 <NewExternalPort>{}</NewExternalPort>\
                 <NewProtocol>{}</NewProtocol>",
                external_port,
                mapping.request.protocol.upnp_name(),
            );
            let error = match self.soap(gateway, "DeletePortMapping", &arguments).await {
                Ok((status, body)) => match soap_error_code(&body) {
                    Some(code) => Some(format!("UPnP error {code}")),
                    None if !status.is_success() => Some(format!("HTTP {status}")),
                    None => None,
                },
                Err(e) => Some(format!("{e:#}")),
            };
            debug!(
                protocol = ?mapping.request.protocol,
                external_port,
                ?error,
                "port mapping removed"
            );
            let _ = events.send(PortMapped {
                transport: PortmapTransport::Upnp,
                protocol: mapping.request.protocol,
                local_port: mapping.request.local_port,
                external_port: None,
                external_ip: None,
                error,
            });
        }
    }

    /// Map every port on one gateway and keep the mappings renewed until
    /// cancelled, then remove them.
    async fn manage_gateway(
        &self,
        found: UpnpDiscoverResponse,
        cancel: &CancellationToken,
        events: &PortMapEvents,
    ) -> anyhow::Result<()> {
        let mut gateway = tokio::select! {
            gateway = self.open_gateway(&found) => gateway?,
            _ = cancel.cancelled() => return Ok(()),
        };
        debug!(control_url = %gateway.control_url, service = gateway.service_type, "found port mapping service");
        let mut mappings: Vec<GatewayMapping> = self
            .mappings
            .iter()
            .copied()
            .map(GatewayMapping::new)
            .collect();
        loop {
            tokio::select! {
                r = self.add_due_mappings(&mut gateway, &mut mappings, events) => r?,
                _ = cancel.cancelled() => break,
            }
            let next_add = mappings.iter().filter_map(|m| m.next_add).min();
            let renew = match next_add {
                Some(t) => tokio::time::sleep_until(t).left_future(),
                None => futures::future::pending::<()>().right_future(),
            };
            tokio::select! {
                _ = renew => {},
                _ = cancel.cancelled() => break,
            }
        }
        self.delete_port_mappings(&gateway, &mut mappings, events)
            .await;
        Ok(())
    }

    /// Manage every gateway discovery finds until cancelled. A gateway that
    /// fails is forgotten, so a later discovery tries it afresh.
    async fn run_gateways(
        &self,
        mut found: UnboundedReceiver<UpnpDiscoverResponse>,
        cancel: CancellationToken,
        events: PortMapEvents,
    ) -> anyhow::Result<()> {
        let mut known = HashSet::<Url>::new();
        let mut gateways = FuturesUnordered::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                Some(response) = found.recv() => {
                    if known.contains(&response.location) || known.len() >= MAX_GATEWAYS {
                        continue;
                    }
                    let location = response.location.clone();
                    known.insert(location.clone());
                    let span = debug_span!("upnp_gateway", location = %location);
                    let (cancel, events) = (&cancel, &events);
                    gateways.push(async move {
                        let r = self.manage_gateway(response, cancel, events).await;
                        (location, r)
                    }.instrument(span));
                },
                Some((location, r)) = gateways.next(), if !gateways.is_empty() => {
                    if let Err(e) = r {
                        debug!(%location, "UPnP gateway failed: {e:#}");
                    }
                    known.remove(&location);
                },
            }
        }
        while gateways.next().await.is_some() {}
        Ok(())
    }

    /// Discover gateways and keep the ports mapped on each until cancelled,
    /// then remove them.
    pub async fn run(self, cancel: CancellationToken, events: PortMapEvents) -> anyhow::Result<()> {
        let (found_tx, found_rx) = unbounded_channel();
        tokio::select! {
            r = self.run_gateways(found_rx, cancel.clone(), events) => r,
            _ = self.discovery(found_tx) => Ok(()),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use quick_xml::de::from_str;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::mpsc::{UnboundedReceiver, unbounded_channel},
    };
    use tokio_util::sync::CancellationToken;
    use url::Url;

    use super::{
        Device, DeviceList, PortMapRequest, PortMapped, PortmapProtocol, PortmapTransport,
        RootDesc, Service, ServiceList, UpnpDiscoverResponse, UpnpPortForwarder,
        UpnpPortForwarderOptions, parse_upnp_discover_response, xml_element_text,
    };

    /// Keeps the paused clock from moving on its own while a test waits on
    /// loopback I/O: tokio does not auto-advance while a blocking task runs,
    /// so time moves only when the test calls `tokio::time::advance`.
    pub(crate) struct ClockHold(#[allow(dead_code)] std::sync::mpsc::Sender<()>);

    pub(crate) fn hold_clock() -> ClockHold {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        tokio::task::spawn_blocking(move || {
            let _ = rx.recv();
        });
        ClockHold(tx)
    }

    #[test]
    fn test_parse_root_desc() {
        let actual = from_str::<RootDesc>(include_str!("../resources/test/devices-0.xml")).unwrap();
        let expected = RootDesc {
            url_base: None,
            devices: vec![Device {
                device_type: "urn:schemas-upnp-org:device:InternetGatewayDevice:1".into(),
                friendly_name: "ARRIS TG3492LG".into(),
                service_list: ServiceList {
                    services: vec![Service {
                        service_type: "urn:schemas-upnp-org:service:Layer3Forwarding:1".into(),
                        control_url: "/upnp/control/Layer3Forwarding".into(),
                        scpd_url: "/Layer3ForwardingSCPD.xml".into(),
                        event_sub_url: Some("/upnp/event/Layer3Forwarding".into()),
                    }],
                },
                device_list: DeviceList {
                    devices: vec![Device {
                        device_type: "urn:schemas-upnp-org:device:WANDevice:1".into(),
                        friendly_name: "WANDevice:1".into(),
                        service_list: ServiceList {
                            services: vec![Service {
                                service_type:
                                    "urn:schemas-upnp-org:service:WANCommonInterfaceConfig:1".into(),
                                control_url: "/upnp/control/WANCommonInterfaceConfig0".into(),
                                scpd_url: "/WANCommonInterfaceConfigSCPD.xml".into(),
                                event_sub_url: Some("/upnp/event/WANCommonInterfaceConfig0".into()),
                            }],
                        },
                        device_list: DeviceList {
                            devices: vec![Device {
                                device_type: "urn:schemas-upnp-org:device:WANConnectionDevice:1"
                                    .into(),
                                friendly_name: "WANConnectionDevice:1".into(),
                                service_list: ServiceList {
                                    services: vec![Service {
                                        service_type:
                                            "urn:schemas-upnp-org:service:WANIPConnection:1".into(),
                                        control_url: "/upnp/control/WANIPConnection0".into(),
                                        scpd_url: "/WANIPConnectionServiceSCPD.xml".into(),
                                        event_sub_url: Some("/upnp/event/WANIPConnection0".into()),
                                    }],
                                },
                                device_list: DeviceList { devices: vec![] },
                            }],
                        },
                    }],
                },
            }],
        };
        assert_eq!(actual, expected);
        assert_eq!(
            actual.port_mapping_service().unwrap().control_url,
            "/upnp/control/WANIPConnection0"
        );
    }

    /// A root description of `root_type` whose WANConnectionDevice offers
    /// `service_type` at `/ctl`, behind a service that maps nothing.
    fn gateway_description(root_type: &str, service_type: &str, url_base: &str) -> String {
        format!(
            r#"<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <specVersion><major>1</major><minor>0</minor></specVersion>
  {url_base}
  <device>
    <deviceType>urn:schemas-upnp-org:device:{root_type}</deviceType>
    <friendlyName>Test gateway</friendlyName>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:Layer3Forwarding:1</serviceType>
        <controlURL>/l3f</controlURL>
        <SCPDURL>/l3f.xml</SCPDURL>
      </service>
    </serviceList>
    <deviceList>
      <device>
        <deviceType>urn:schemas-upnp-org:device:WANDevice:1</deviceType>
        <serviceList>
          <service>
            <serviceType>urn:schemas-upnp-org:service:WANCommonInterfaceConfig:1</serviceType>
            <controlURL>/wcic</controlURL>
            <SCPDURL>/wcic.xml</SCPDURL>
          </service>
        </serviceList>
        <deviceList>
          <device>
            <deviceType>urn:schemas-upnp-org:device:WANConnectionDevice:1</deviceType>
            <serviceList>
              <service>
                <serviceType>{service_type}</serviceType>
                <controlURL>/ctl</controlURL>
                <SCPDURL>/scpd.xml</SCPDURL>
              </service>
            </serviceList>
          </device>
        </deviceList>
      </device>
    </deviceList>
  </device>
</root>"#
        )
    }

    /// One SOAP call the mock gateway received.
    #[derive(Debug)]
    pub(crate) struct SoapCall {
        pub soap_action: String,
        pub body: String,
    }

    impl SoapCall {
        fn action(&self) -> &str {
            self.soap_action
                .trim_matches('"')
                .rsplit('#')
                .next()
                .unwrap()
        }

        fn arg(&self, name: &str) -> &str {
            xml_element_text(&self.body, name).unwrap()
        }
    }

    type Answer = Arc<dyn Fn(&SoapCall) -> (u16, String) + Send + Sync>;

    fn ok(body: &str) -> (u16, String) {
        (200, body.to_owned())
    }

    fn fault(code: u32) -> (u16, String) {
        (
            500,
            format!(
                "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><s:Fault>\
                 <faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail>\
                 <UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>{code}</errorCode>\
                 <errorDescription>error</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>"
            ),
        )
    }

    fn external_ip_answer() -> (u16, String) {
        ok("<s:Envelope><s:Body><u:GetExternalIPAddressResponse>\
            <NewExternalIPAddress>203.0.113.7</NewExternalIPAddress>\
            </u:GetExternalIPAddressResponse></s:Body></s:Envelope>")
    }

    /// An IGD on 127.0.0.1 serving `description` at `/desc.xml` and
    /// answering SOAP calls at `/ctl` with `answer`; every call is reported.
    async fn mock_gateway(
        description: impl Fn(SocketAddr) -> String + Send + 'static,
        answer: Answer,
    ) -> (Url, UnboundedReceiver<SoapCall>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let description = description(addr);
        let (calls_tx, calls_rx) = unbounded_channel();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let (head_len, path, soap_action, content_length) = loop {
                    let mut chunk = [0u8; 4096];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "client closed before a full request");
                    buf.extend_from_slice(&chunk[..n]);
                    let mut headers = [httparse::EMPTY_HEADER; 32];
                    let mut req = httparse::Request::new(&mut headers);
                    if let httparse::Status::Complete(len) = req.parse(&buf).unwrap() {
                        let header = |name: &str| {
                            req.headers
                                .iter()
                                .find(|h| h.name.eq_ignore_ascii_case(name))
                                .map(|h| String::from_utf8(h.value.to_vec()).unwrap())
                        };
                        let content_length = header("content-length")
                            .map(|l| l.parse::<usize>().unwrap())
                            .unwrap_or(0);
                        break (
                            len,
                            req.path.unwrap().to_owned(),
                            header("soapaction").unwrap_or_default(),
                            content_length,
                        );
                    }
                };
                while buf.len() < head_len + content_length {
                    let mut chunk = [0u8; 4096];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "client closed before a full body");
                    buf.extend_from_slice(&chunk[..n]);
                }
                let body = String::from_utf8(buf[head_len..].to_vec()).unwrap();
                let (status, reply) = if path == "/desc.xml" {
                    (200, description.clone())
                } else {
                    assert_eq!(path, "/ctl");
                    let call = SoapCall { soap_action, body };
                    let reply = answer(&call);
                    calls_tx.send(call).unwrap();
                    reply
                };
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        let location = Url::parse(&format!("http://{addr}/desc.xml")).unwrap();
        (location, calls_rx)
    }

    struct Running {
        cancel: CancellationToken,
        events: UnboundedReceiver<PortMapped>,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    /// Run a forwarder against the gateway at `location`, as if SSDP had
    /// found it there.
    fn run_forwarder(location: Url, mappings: Vec<PortMapRequest>) -> Running {
        let forwarder =
            UpnpPortForwarder::new(mappings, Some(UpnpPortForwarderOptions::default()), None)
                .unwrap();
        let (found_tx, found_rx) = unbounded_channel();
        let received_from = SocketAddr::new(location.host_str().unwrap().parse().unwrap(), 1900);
        found_tx
            .send(UpnpDiscoverResponse {
                received_from,
                location,
            })
            .unwrap();
        let cancel = CancellationToken::new();
        let (events_tx, events) = unbounded_channel();
        let task = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                // Discovery found it; nothing more will come.
                let _found_tx = found_tx;
                forwarder.run_gateways(found_rx, cancel, events_tx).await
            }
        });
        Running {
            cancel,
            events,
            task,
        }
    }

    fn tcp_and_udp(port: u16) -> Vec<PortMapRequest> {
        [PortmapProtocol::Tcp, PortmapProtocol::Udp]
            .into_iter()
            .map(|protocol| PortMapRequest {
                protocol,
                local_port: port,
                external_port: port,
            })
            .collect()
    }

    fn mapped(protocol: PortmapProtocol, port: u16, external_port: u16) -> PortMapped {
        PortMapped {
            transport: PortmapTransport::Upnp,
            protocol,
            local_port: port,
            external_port: Some(external_port),
            external_ip: Some("203.0.113.7".parse().unwrap()),
            error: None,
        }
    }

    #[test]
    fn a_reply_locating_a_device_at_an_address_no_gateway_uses_is_refused() {
        let from: SocketAddr = "192.168.1.1:1900".parse().unwrap();
        let reply = |location: &str| {
            format!("HTTP/1.1 200 OK\r\nST: upnp:rootdevice\r\nLOCATION: {location}\r\n\r\n")
        };
        let found = parse_upnp_discover_response(
            reply("http://192.168.1.1:5000/rootDesc.xml").as_bytes(),
            from,
        )
        .unwrap();
        assert_eq!(
            found.location.as_str(),
            "http://192.168.1.1:5000/rootDesc.xml"
        );
        for bad in [
            "http://127.0.0.1:5000/rootDesc.xml",
            "http://169.254.1.1:5000/rootDesc.xml",
            "http://239.255.255.250:5000/rootDesc.xml",
            "http://0.0.0.0:5000/rootDesc.xml",
            "https://192.168.1.1:5000/rootDesc.xml",
        ] {
            assert!(
                parse_upnp_discover_response(reply(bad).as_bytes(), from).is_err(),
                "{bad} accepted"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_wanipconnection_2_gateway_maps_tcp_and_udp_renews_at_three_quarters_of_the_lease_and_removes_them_on_close()
     {
        let _hold = hold_clock();
        let service = "urn:schemas-upnp-org:service:WANIPConnection:2";
        let (location, mut calls) = mock_gateway(
            move |_| gateway_description("InternetGatewayDevice:2", service, ""),
            Arc::new(|call: &SoapCall| match call.action() {
                "GetExternalIPAddress" => external_ip_answer(),
                _ => ok("<s:Envelope><s:Body><u:AddPortMappingResponse/></s:Body></s:Envelope>"),
            }),
        )
        .await;
        let mut running = run_forwarder(location, tcp_and_udp(6881));

        let call = calls.recv().await.unwrap();
        assert_eq!(call.action(), "GetExternalIPAddress");
        assert_eq!(
            call.soap_action,
            format!("\"{service}#GetExternalIPAddress\"")
        );
        for protocol in ["TCP", "UDP"] {
            let call = calls.recv().await.unwrap();
            assert_eq!(call.soap_action, format!("\"{service}#AddPortMapping\""));
            assert_eq!(call.arg("NewProtocol"), protocol);
            assert_eq!(call.arg("NewExternalPort"), "6881");
            assert_eq!(call.arg("NewInternalPort"), "6881");
            assert_eq!(call.arg("NewInternalClient"), "127.0.0.1");
            assert_eq!(call.arg("NewLeaseDuration"), "3600");
        }
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Tcp, 6881, 6881)
        );
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Udp, 6881, 6881)
        );

        tokio::time::advance(Duration::from_secs(2700)).await;
        for protocol in ["TCP", "UDP"] {
            let call = calls.recv().await.unwrap();
            assert_eq!(call.action(), "AddPortMapping");
            assert_eq!(call.arg("NewProtocol"), protocol);
            assert_eq!(call.arg("NewLeaseDuration"), "3600");
        }
        running.events.recv().await.unwrap();
        running.events.recv().await.unwrap();

        running.cancel.cancel();
        for (protocol, name) in [(PortmapProtocol::Tcp, "TCP"), (PortmapProtocol::Udp, "UDP")] {
            let call = calls.recv().await.unwrap();
            assert_eq!(call.soap_action, format!("\"{service}#DeletePortMapping\""));
            assert!(call.body.contains("<NewRemoteHost></NewRemoteHost>"));
            assert_eq!(call.arg("NewExternalPort"), "6881");
            assert_eq!(call.arg("NewProtocol"), name);
            assert_eq!(
                running.events.recv().await.unwrap(),
                PortMapped {
                    transport: PortmapTransport::Upnp,
                    protocol,
                    local_port: 6881,
                    external_port: None,
                    external_ip: None,
                    error: None,
                }
            );
        }
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_wanpppconnection_gateway_maps_through_its_ppp_service_under_its_url_base() {
        let _hold = hold_clock();
        let service = "urn:schemas-upnp-org:service:WANPPPConnection:1";
        let (location, mut calls) = mock_gateway(
            move |addr| {
                gateway_description(
                    "InternetGatewayDevice:1",
                    service,
                    &format!("<URLBase>http://{addr}/</URLBase>"),
                )
            },
            Arc::new(|call: &SoapCall| match call.action() {
                "GetExternalIPAddress" => external_ip_answer(),
                _ => ok(""),
            }),
        )
        .await;
        let mut running = run_forwarder(
            location,
            vec![PortMapRequest {
                protocol: PortmapProtocol::Tcp,
                local_port: 6881,
                external_port: 6881,
            }],
        );

        assert_eq!(calls.recv().await.unwrap().action(), "GetExternalIPAddress");
        let call = calls.recv().await.unwrap();
        assert_eq!(call.soap_action, format!("\"{service}#AddPortMapping\""));
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Tcp, 6881, 6881)
        );
        running.cancel.cancel();
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_port_conflict_718_moves_the_mapping_to_another_external_port() {
        let _hold = hold_clock();
        let (location, mut calls) = mock_gateway(
            |_| {
                gateway_description(
                    "InternetGatewayDevice:1",
                    "urn:schemas-upnp-org:service:WANIPConnection:1",
                    "",
                )
            },
            Arc::new(|call: &SoapCall| match call.action() {
                "GetExternalIPAddress" => external_ip_answer(),
                _ if call.arg("NewExternalPort") == "6881" => fault(718),
                _ => ok(""),
            }),
        )
        .await;
        let mut running = run_forwarder(
            location,
            vec![PortMapRequest {
                protocol: PortmapProtocol::Tcp,
                local_port: 6881,
                external_port: 6881,
            }],
        );

        assert_eq!(calls.recv().await.unwrap().action(), "GetExternalIPAddress");
        assert_eq!(calls.recv().await.unwrap().arg("NewExternalPort"), "6881");
        let retry = calls.recv().await.unwrap();
        let port: u16 = retry.arg("NewExternalPort").parse().unwrap();
        assert!((40000..50000).contains(&port), "{port}");
        assert_eq!(retry.arg("NewInternalPort"), "6881");
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Tcp, 6881, port)
        );
        running.cancel.cancel();
        let delete = calls.recv().await.unwrap();
        assert_eq!(delete.action(), "DeletePortMapping");
        assert_eq!(delete.arg("NewExternalPort"), port.to_string());
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_gateway_that_takes_only_permanent_leases_725_is_asked_again_with_lease_zero() {
        let _hold = hold_clock();
        let (location, mut calls) = mock_gateway(
            |_| {
                gateway_description(
                    "InternetGatewayDevice:1",
                    "urn:schemas-upnp-org:service:WANIPConnection:1",
                    "",
                )
            },
            Arc::new(|call: &SoapCall| match call.action() {
                "GetExternalIPAddress" => external_ip_answer(),
                "AddPortMapping" if call.arg("NewLeaseDuration") != "0" => fault(725),
                _ => ok(""),
            }),
        )
        .await;
        let mut running = run_forwarder(location, tcp_and_udp(6881));

        assert_eq!(calls.recv().await.unwrap().action(), "GetExternalIPAddress");
        assert_eq!(calls.recv().await.unwrap().arg("NewLeaseDuration"), "3600");
        let retry = calls.recv().await.unwrap();
        assert_eq!(retry.arg("NewProtocol"), "TCP");
        assert_eq!(retry.arg("NewLeaseDuration"), "0");
        // The gateway's answer holds for every later mapping on it.
        let udp = calls.recv().await.unwrap();
        assert_eq!(udp.arg("NewProtocol"), "UDP");
        assert_eq!(udp.arg("NewLeaseDuration"), "0");
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Tcp, 6881, 6881)
        );
        assert_eq!(
            running.events.recv().await.unwrap(),
            mapped(PortmapProtocol::Udp, 6881, 6881)
        );
        running.cancel.cancel();
        running.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn any_other_upnp_error_fails_the_mapping() {
        let _hold = hold_clock();
        let (location, mut calls) = mock_gateway(
            |_| {
                gateway_description(
                    "InternetGatewayDevice:1",
                    "urn:schemas-upnp-org:service:WANIPConnection:1",
                    "",
                )
            },
            Arc::new(|call: &SoapCall| match call.action() {
                "GetExternalIPAddress" => fault(501),
                _ => fault(727),
            }),
        )
        .await;
        let mut running = run_forwarder(
            location,
            vec![PortMapRequest {
                protocol: PortmapProtocol::Udp,
                local_port: 6881,
                external_port: 6881,
            }],
        );

        assert_eq!(calls.recv().await.unwrap().action(), "GetExternalIPAddress");
        assert_eq!(calls.recv().await.unwrap().action(), "AddPortMapping");
        assert_eq!(
            running.events.recv().await.unwrap(),
            PortMapped {
                transport: PortmapTransport::Upnp,
                protocol: PortmapProtocol::Udp,
                local_port: 6881,
                external_port: None,
                external_ip: None,
                error: Some("UPnP error 727".into()),
            }
        );
        running.cancel.cancel();
        running.task.await.unwrap().unwrap();
        // Nothing was granted, so closing removes nothing.
        assert!(calls.try_recv().is_err());
        assert!(running.events.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn a_control_url_on_another_host_is_refused() {
        let _hold = hold_clock();
        let (location, _calls) = mock_gateway(
            |_| {
                gateway_description(
                    "InternetGatewayDevice:1",
                    "urn:schemas-upnp-org:service:WANIPConnection:1",
                    "<URLBase>http://192.0.2.1/</URLBase>",
                )
            },
            Arc::new(|_: &SoapCall| ok("")),
        )
        .await;
        let forwarder = UpnpPortForwarder::new(tcp_and_udp(6881), None, None).unwrap();
        let found = UpnpDiscoverResponse {
            received_from: "127.0.0.1:1900".parse().unwrap(),
            location,
        };
        let err = forwarder.open_gateway(&found).await.err().unwrap();
        assert!(
            format!("{err:#}").contains("is not on the device"),
            "{err:#}"
        );
    }
}
