// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The interface a session's traffic leaves through: the one network a port
//! mapper can open the session's port on.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

use anyhow::{Context, bail};

use crate::core::net::{Egress, HttpClients};

/// One address of one interface: an IPv4 one, or a global IPv6 one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteInterface {
    pub name: String,
    /// The address the route's traffic leaves from.
    pub ip: IpAddr,
    /// A point-to-point link, as a tunnel is: no network lies behind it to
    /// search for a gateway on.
    pub point_to_point: bool,
}

impl RouteInterface {
    /// The interface reaches only the local network: its address is a
    /// loopback or link-local one. No gateway on it forwards a port from
    /// the internet, so no mapper runs on it (libtorrent marks such a
    /// listen socket `local_network` and starts neither mapper on it).
    pub fn local_network_only(&self) -> bool {
        local_network_address(self.ip)
    }
}

/// An address only the local network reaches: loopback or link-local, and
/// for IPv6 a unique local one too (libtorrent marks a listen socket on a
/// loopback or link-local address `local_network`, and maps no local IPv6
/// address).
pub fn local_network_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unicast_link_local() || ip.is_unique_local(),
    }
}

/// A documentation address no host answers at. The routing table sends it
/// the way it sends any public address: where traffic toward it leaves
/// from names the interface.
const PUBLIC_PROBE: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 9);

/// The IPv6 documentation address, probed as [`PUBLIC_PROBE`] is.
const PUBLIC_PROBE_V6: SocketAddr = SocketAddr::new(
    IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
    9,
);

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
        .find(|i| i.ip == IpAddr::V4(ip))
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

/// The interface a session's IPv6 traffic leaves through, holding the
/// global IPv6 address a PCP mapper asks from: `listen` itself when the
/// session is held to it; else, on `device`, the global address traffic
/// toward a public IPv6 address leaves from, or the device's first global
/// address. `None` when there is none to map: an address only the local
/// network reaches, link-local or unique local, is never mapped
/// (libtorrent `session_impl::start_natpmp` maps no local IPv6 address,
/// and `get_gateway` finds no gateway for one). A session bound to no
/// device and held to no IPv6 address maps none.
pub fn ipv6_route_interface(
    listen: Ipv6Addr,
    device: Option<&str>,
    clients: &dyn HttpClients,
) -> anyhow::Result<Option<RouteInterface>> {
    if listen.is_unspecified() && device.is_none() {
        return Ok(None);
    }
    let addresses = interface_addresses().context("error listing network interfaces")?;
    pick_v6(addresses, listen, device, || {
        clients.egress(PUBLIC_PROBE_V6)
    })
}

fn pick_v6(
    addresses: Vec<InterfaceAddress>,
    listen: Ipv6Addr,
    device: Option<&str>,
    egress: impl FnOnce() -> Egress,
) -> anyhow::Result<Option<RouteInterface>> {
    let interface = |a: InterfaceAddress| RouteInterface {
        name: a.name,
        ip: a.ip,
        point_to_point: a.point_to_point,
    };
    if !listen.is_unspecified() {
        if local_network_address(listen.into()) {
            return Ok(None);
        }
        return addresses
            .into_iter()
            .find(|a| a.ip == IpAddr::V6(listen))
            .map(|a| Some(interface(a)))
            .with_context(|| {
                format!("no interface holds {listen}, the address the session listens on")
            });
    }
    let Some(device) = device else {
        return Ok(None);
    };
    let mut global: Vec<InterfaceAddress> = addresses
        .into_iter()
        .filter(|a| {
            a.name == device
                && a.ip.is_ipv6()
                && !a.ip.is_unspecified()
                && !local_network_address(a.ip)
        })
        .collect();
    if global.is_empty() {
        return Ok(None);
    }
    let at = match egress() {
        Egress::From(source) => global.iter().position(|a| a.ip == source).unwrap_or(0),
        Egress::NoRoute | Egress::Proxied => 0,
    };
    Ok(Some(interface(global.swap_remove(at))))
}

/// Every address `name` holds, IPv4 and IPv6: any of them is the
/// interface's own, whichever the kernel picks to send from.
pub fn addresses_of(name: &str) -> io::Result<Vec<IpAddr>> {
    Ok(interface_addresses()?
        .into_iter()
        .filter(|a| a.name == name)
        .map(|a| a.ip)
        .collect())
}

fn ipv4_interfaces() -> io::Result<Vec<RouteInterface>> {
    Ok(interface_addresses()?
        .into_iter()
        .filter_map(|a| match a.ip {
            IpAddr::V4(ip) => Some(RouteInterface {
                name: a.name,
                ip: ip.into(),
                point_to_point: a.point_to_point,
            }),
            IpAddr::V6(_) => None,
        })
        .collect())
}

/// One address of one interface, of either family.
struct InterfaceAddress {
    name: String,
    ip: IpAddr,
    point_to_point: bool,
}

#[cfg(unix)]
fn interface_addresses() -> io::Result<Vec<InterfaceAddress>> {
    use nix::libc;
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs writes a list head we free below.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let mut addresses = Vec::new();
    let mut at = head;
    while !at.is_null() {
        // SAFETY: a node of the list getifaddrs returned, not yet freed.
        let ifa = unsafe { &*at };
        at = ifa.ifa_next;
        if ifa.ifa_addr.is_null() {
            continue;
        }
        // SAFETY: a non-null address getifaddrs filled in.
        let ip = match i32::from(unsafe { (*ifa.ifa_addr).sa_family }) {
            libc::AF_INET => {
                // SAFETY: an AF_INET address is a sockaddr_in.
                let sin = unsafe { &*ifa.ifa_addr.cast::<libc::sockaddr_in>() };
                IpAddr::V4(Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr)))
            }
            libc::AF_INET6 => {
                // SAFETY: an AF_INET6 address is a sockaddr_in6.
                let sin6 = unsafe { &*ifa.ifa_addr.cast::<libc::sockaddr_in6>() };
                IpAddr::V6(std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr))
            }
            _ => continue,
        };
        // SAFETY: every node names its interface, NUL-terminated.
        let name = unsafe { std::ffi::CStr::from_ptr(ifa.ifa_name) };
        addresses.push(InterfaceAddress {
            name: name.to_string_lossy().into_owned(),
            ip,
            point_to_point: ifa.ifa_flags & libc::IFF_POINTOPOINT as u32 != 0,
        });
    }
    // SAFETY: the head getifaddrs returned, freed once.
    unsafe { libc::freeifaddrs(head) };
    Ok(addresses)
}

/// The adapter list does not say which links are point-to-point.
#[cfg(not(unix))]
fn interface_addresses() -> io::Result<Vec<InterfaceAddress>> {
    use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
    let interfaces = NetworkInterface::show().map_err(io::Error::other)?;
    Ok(interfaces
        .into_iter()
        .flat_map(|i| {
            i.addr.into_iter().map(move |a| InterfaceAddress {
                name: i.name.clone(),
                ip: match a {
                    Addr::V4(a) => a.ip.into(),
                    Addr::V6(a) => a.ip.into(),
                },
                point_to_point: false,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use std::net::{IpAddr, SocketAddr};

    use super::{InterfaceAddress, RouteInterface, ipv4_interfaces, pick, pick_v6, source_toward};
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
    fn a_loopback_or_link_local_interface_reaches_only_the_local_network() {
        assert!(interface("lo0", [127, 0, 0, 1], false).local_network_only());
        assert!(interface("en0", [169, 254, 3, 9], false).local_network_only());
        assert!(!interface("en0", [192, 168, 1, 20], false).local_network_only());
        assert!(!interface("en0", [203, 0, 113, 9], false).local_network_only());
        for (ip, local) in [
            ("::1", true),
            ("fe80::1", true),
            ("fd12:3456::1", true),
            ("2001:db8::1", false),
            ("0.0.0.0", false),
            ("::", false),
        ] {
            assert_eq!(
                super::local_network_address(ip.parse().unwrap()),
                local,
                "{ip}"
            );
        }
    }

    #[test]
    fn the_route_interface_holds_the_address_traffic_toward_the_probe_leaves_from() {
        let route = LeavesFrom(Ipv4Addr::LOCALHOST.into());
        let source = || source_toward(&route, super::PUBLIC_PROBE);
        let found = pick(ipv4_interfaces().unwrap(), None, source).unwrap();
        assert_eq!(found.ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(!found.point_to_point, "{found:?}");
    }

    // Kernel sockets that say traffic toward any address leaves from `0`.
    struct LeavingFrom(IpAddr, crate::network::KernelSockets);

    impl crate::network::SocketFactory for LeavingFrom {
        fn connect_tcp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> crate::network::BoxFuture<
            'a,
            std::io::Result<(
                crate::type_aliases::BoxAsyncReadVectored,
                crate::type_aliases::BoxAsyncWrite,
            )>,
        > {
            self.1.connect_tcp(addr)
        }
        fn listen_tcp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
        {
            self.1.listen_tcp(addr)
        }
        fn bind_udp<'a>(
            &'a self,
            addr: SocketAddr,
        ) -> crate::network::BoxFuture<
            'a,
            std::io::Result<std::sync::Arc<dyn crate::network::DatagramSocket>>,
        > {
            self.1.bind_udp(addr)
        }
        fn source_toward(&self, _: SocketAddr) -> Option<std::io::Result<IpAddr>> {
            Some(Ok(self.0))
        }
        fn gateway_socket<'a>(
            &'a self,
            local: SocketAddr,
        ) -> crate::network::BoxFuture<
            'a,
            std::io::Result<std::sync::Arc<dyn crate::network::DatagramSocket>>,
        > {
            self.1.gateway_socket(local)
        }
    }

    // A session given no clients leaves where its sockets say, and asks the
    // gateway from a socket of theirs; sockets that say nothing leave through
    // a proxy, whose gateway no port mapper asks.
    #[tokio::test]
    async fn a_session_given_no_clients_takes_its_sockets_route() {
        let to = SocketAddr::from((Ipv4Addr::LOCALHOST, 9));
        let loopback = IpAddr::from(Ipv4Addr::LOCALHOST);
        let saying = crate::NoClients::over(std::sync::Arc::new(LeavingFrom(
            loopback,
            crate::test_support::sockets(),
        )));
        assert_eq!(saying.egress(to), Egress::From(loopback));
        let socket = saying
            .gateway_socket(SocketAddr::new(loopback, 0))
            .await
            .unwrap();
        assert_eq!(socket.bind_addr().ip(), loopback);

        let kernel = crate::NoClients::over(crate::test_support::socket_factory());
        assert_eq!(kernel.egress(to), Egress::Proxied);
        assert!(source_toward(&kernel, to).is_err());
    }

    fn address(name: &str, ip: &str) -> InterfaceAddress {
        InterfaceAddress {
            name: name.into(),
            ip: ip.parse().unwrap(),
            point_to_point: false,
        }
    }

    fn host() -> Vec<InterfaceAddress> {
        vec![
            address("en0", "192.168.1.20"),
            address("en0", "fe80::5"),
            address("en0", "fd12:3456::5"),
            address("en0", "2001:db8::5"),
            address("en0", "2001:db8::6"),
            address("en1", "fe80::9"),
            address("en1", "fd00::9"),
        ]
    }

    fn unprobed() -> Egress {
        unreachable!("the address is known without a probe")
    }

    // A session held to a global IPv6 address maps on the interface that
    // holds it; one held to a link-local or unique local address maps
    // nothing over IPv6.
    #[test]
    fn a_session_held_to_an_ipv6_address_maps_from_it_when_it_is_global() {
        let held = |ip: &str| pick_v6(host(), ip.parse().unwrap(), None, unprobed);
        assert_eq!(
            held("2001:db8::5").unwrap(),
            Some(RouteInterface {
                name: "en0".into(),
                ip: "2001:db8::5".parse().unwrap(),
                point_to_point: false,
            })
        );
        assert_eq!(held("fe80::5").unwrap(), None);
        assert_eq!(held("fd12:3456::5").unwrap(), None);
        assert_eq!(
            format!("{:#}", held("2001:db8::99").unwrap_err()),
            "no interface holds 2001:db8::99, the address the session listens on"
        );
    }

    // A session bound to a device maps from the global address its IPv6
    // traffic leaves from, else the device's first; a device with only
    // local IPv6 addresses, or a session bound to none, maps nothing.
    #[test]
    fn a_session_bound_to_a_device_maps_from_its_global_ipv6_address() {
        let any = std::net::Ipv6Addr::UNSPECIFIED;
        let from = |ip: &str| {
            let ip: IpAddr = ip.parse().unwrap();
            move || Egress::From(ip)
        };
        let on_en0 = pick_v6(host(), any, Some("en0"), from("2001:db8::6"))
            .unwrap()
            .unwrap();
        assert_eq!(on_en0.ip, "2001:db8::6".parse::<IpAddr>().unwrap());
        let first = pick_v6(host(), any, Some("en0"), || Egress::NoRoute)
            .unwrap()
            .unwrap();
        assert_eq!(first.ip, "2001:db8::5".parse::<IpAddr>().unwrap());
        assert_eq!(
            pick_v6(host(), any, Some("en1"), from("fe80::9")).unwrap(),
            None
        );
        assert_eq!(pick_v6(host(), any, None, unprobed).unwrap(), None);
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
