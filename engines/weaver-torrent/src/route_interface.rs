// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The interface a session's traffic leaves through: the one network a port
//! mapper can open the session's port on.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use anyhow::{Context, bail};

use crate::core::net::{Egress, HttpClients};

/// One IPv4 address of one interface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteInterface {
    pub name: String,
    /// The address the route's traffic leaves from.
    pub ip: Ipv4Addr,
    /// A point-to-point link, as a tunnel is: no network lies behind it to
    /// search for a gateway on.
    pub point_to_point: bool,
}

/// A documentation address no host answers at. The routing table sends it
/// the way it sends any public address: where traffic toward it leaves
/// from names the interface.
const PUBLIC_PROBE: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 9);

/// The interface the route's traffic takes: `device` when the session is
/// bound to one, otherwise the one `clients` say public traffic leaves
/// through.
pub fn route_interface(
    device: Option<&str>,
    clients: &dyn HttpClients,
) -> anyhow::Result<RouteInterface> {
    let interfaces = ipv4_interfaces().context("error listing network interfaces")?;
    pick(interfaces, device, || source_toward(clients, PUBLIC_PROBE))
}

fn pick(
    interfaces: Vec<RouteInterface>,
    device: Option<&str>,
    source: impl FnOnce() -> anyhow::Result<Ipv4Addr>,
) -> anyhow::Result<RouteInterface> {
    let ip = match device {
        Some(device) => {
            return interfaces
                .into_iter()
                .find(|i| i.name == device)
                .with_context(|| format!("{device} has no IPv4 address"));
        }
        None => source()?,
    };
    interfaces
        .into_iter()
        .find(|i| i.ip == ip)
        .with_context(|| format!("no interface holds {ip}, the address traffic leaves from"))
}

/// The address a request to `to` leaves from, as `clients` route it.
fn source_toward(clients: &dyn HttpClients, to: SocketAddr) -> anyhow::Result<Ipv4Addr> {
    match clients.egress(to) {
        Egress::From(IpAddr::V4(ip)) => Ok(ip),
        Egress::From(ip) => bail!("the route toward {to} leaves from {ip}"),
        Egress::NoRoute => bail!("no route toward {to}"),
        Egress::Proxied => bail!("the route toward {to} leaves through a proxy"),
    }
}

#[cfg(unix)]
fn ipv4_interfaces() -> io::Result<Vec<RouteInterface>> {
    use nix::libc;
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs writes a list head we free below.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let mut interfaces = Vec::new();
    let mut at = head;
    while !at.is_null() {
        // SAFETY: a node of the list getifaddrs returned, not yet freed.
        let ifa = unsafe { &*at };
        at = ifa.ifa_next;
        if ifa.ifa_addr.is_null()
            // SAFETY: a non-null address getifaddrs filled in.
            || i32::from(unsafe { (*ifa.ifa_addr).sa_family }) != libc::AF_INET
        {
            continue;
        }
        // SAFETY: an AF_INET address is a sockaddr_in.
        let sin = unsafe { &*ifa.ifa_addr.cast::<libc::sockaddr_in>() };
        // SAFETY: every node names its interface, NUL-terminated.
        let name = unsafe { std::ffi::CStr::from_ptr(ifa.ifa_name) };
        interfaces.push(RouteInterface {
            name: name.to_string_lossy().into_owned(),
            ip: Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr)),
            point_to_point: ifa.ifa_flags & libc::IFF_POINTOPOINT as u32 != 0,
        });
    }
    // SAFETY: the head getifaddrs returned, freed once.
    unsafe { libc::freeifaddrs(head) };
    Ok(interfaces)
}

/// The adapter list does not say which links are point-to-point.
#[cfg(not(unix))]
fn ipv4_interfaces() -> io::Result<Vec<RouteInterface>> {
    use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
    let interfaces = NetworkInterface::show().map_err(io::Error::other)?;
    Ok(interfaces
        .into_iter()
        .flat_map(|i| {
            i.addr.into_iter().filter_map(move |a| match a {
                Addr::V4(a) => Some(RouteInterface {
                    name: i.name.clone(),
                    ip: a.ip,
                    point_to_point: false,
                }),
                Addr::V6(_) => None,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use std::net::{IpAddr, SocketAddr};

    use super::{RouteInterface, ipv4_interfaces, pick, source_toward};
    use crate::{ClientShape, Egress, HttpClients, HttpTarget};

    // A route on which every request leaves from `0`.
    struct LeavesFrom(IpAddr);

    impl HttpClients for LeavesFrom {
        fn client(
            &self,
            _: Option<HttpTarget<'_>>,
            _: &ClientShape,
        ) -> anyhow::Result<reqwest::Client> {
            anyhow::bail!("the route makes no requests")
        }
        fn egress(&self, _: SocketAddr) -> Egress {
            Egress::From(self.0)
        }
    }

    fn interface(name: &str, ip: [u8; 4], point_to_point: bool) -> RouteInterface {
        RouteInterface {
            name: name.into(),
            ip: ip.into(),
            point_to_point,
        }
    }

    #[test]
    fn the_route_interface_holds_the_address_traffic_toward_the_probe_leaves_from() {
        let route = LeavesFrom(Ipv4Addr::LOCALHOST.into());
        let source = || source_toward(&route, super::PUBLIC_PROBE);
        let found = pick(ipv4_interfaces().unwrap(), None, source).unwrap();
        assert_eq!(found.ip, Ipv4Addr::LOCALHOST);
        assert!(!found.point_to_point, "{found:?}");
    }

    // A session given no clients leaves on the host's own route, and asks
    // the gateway from the host's own socket.
    #[tokio::test]
    async fn a_session_given_no_clients_takes_the_hosts_route() {
        let to = SocketAddr::from((Ipv4Addr::LOCALHOST, 9));
        assert_eq!(
            crate::NoClients.egress(to),
            Egress::From(Ipv4Addr::LOCALHOST.into())
        );
        let socket = crate::NoClients
            .gateway_socket(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .unwrap();
        assert_eq!(socket.bind_addr().ip(), IpAddr::from(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn a_bound_device_is_the_route_interface() {
        let interfaces = vec![
            interface("en0", [192, 168, 1, 20], false),
            interface("utun8", [10, 44, 0, 2], true),
        ];
        let unused =
            || -> anyhow::Result<Ipv4Addr> { unreachable!("a bound device is not probed") };
        assert_eq!(
            pick(interfaces.clone(), Some("utun8"), unused).unwrap(),
            interface("utun8", [10, 44, 0, 2], true)
        );
        let err = pick(interfaces, Some("wg0"), unused).unwrap_err();
        assert_eq!(format!("{err:#}"), "wg0 has no IPv4 address");
    }
}
