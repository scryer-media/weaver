// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Datagram sockets joined to a multicast group of each family on every
// interface an egress holds.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
};

use if_addrs::{IfAddr, Interface};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use tokio::{io::Interest, net::UdpSocket};

use super::{SocketEgress, bind_interface};

/// A socket for each family, joined to that family's group on every
/// interface the egress holds that carries it. Either family's socket may be
/// missing, when the host gives it none or it joins its group nowhere.
pub struct MulticastSocket {
    v4: Option<Group>,
    v6: Option<Group>,
    interfaces: Vec<Interface>,
}

// A family's socket, the group it sends to, and the interfaces its
// datagrams leave through.
struct Group {
    socket: UdpSocket,
    to: SocketAddr,
    out: Vec<Out>,
}

// An interface a group's datagrams leave through: IPv4 names it by an
// address it holds, IPv6 by its index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Out {
    V4(Ipv4Addr),
    V6(u32),
}

// Where each family's group is joined and where its datagrams leave.
#[derive(Debug, Default, PartialEq, Eq)]
struct Plan {
    join_v4: Vec<Ipv4Addr>,
    join_v6: Vec<u32>,
    out: Vec<Out>,
}

// The IPv4 group is joined at every private or loopback IPv4 address and
// sent to once out of each interface holding one. The IPv6 group is joined
// on every interface holding an IPv6 address beyond link-local, and sent to
// out of each holding a unique local one.
fn plan(interfaces: &[Interface]) -> Plan {
    let mut plan = Plan::default();
    let mut sent_v4 = Vec::new();
    for interface in interfaces {
        match &interface.addr {
            IfAddr::V4(at) if at.ip.is_private() || at.ip.is_loopback() => {
                push_new(&mut plan.join_v4, at.ip);
                if !sent_v4.contains(&interface.name.as_str()) {
                    sent_v4.push(interface.name.as_str());
                    plan.out.push(Out::V4(at.ip));
                }
            }
            IfAddr::V6(at) => {
                let Some(index) = interface.index else {
                    continue;
                };
                if !at.ip.is_unicast_link_local() {
                    push_new(&mut plan.join_v6, index);
                }
                if at.ip.is_unique_local() {
                    push_new(&mut plan.out, Out::V6(index));
                }
            }
            IfAddr::V4(_) => {}
        }
    }
    plan
}

fn push_new<T: PartialEq>(into: &mut Vec<T>, item: T) {
    if !into.contains(&item) {
        into.push(item);
    }
}

// Whether `source` is `interface`'s own address or on its network. A
// link-local IPv6 source with a scope is on the network of the interface of
// that scope alone.
fn on_network(source: SocketAddr, interface: &Interface) -> bool {
    match (source, &interface.addr) {
        (source, _) if source.ip() == interface.ip() => true,
        (SocketAddr::V4(source), IfAddr::V4(at)) => {
            source.ip().to_bits() & at.netmask.to_bits() == at.ip.to_bits() & at.netmask.to_bits()
        }
        (SocketAddr::V6(source), IfAddr::V6(at))
            if source.ip().is_unicast_link_local() && source.scope_id() > 0 =>
        {
            at.ip.is_unicast_link_local() && interface.index == Some(source.scope_id())
        }
        (SocketAddr::V6(source), IfAddr::V6(at)) => {
            source.ip().to_bits() & at.netmask.to_bits() == at.ip.to_bits() & at.netmask.to_bits()
        }
        _ => false,
    }
}

impl SocketEgress {
    /// A socket joined to `v4` and one joined to `v6`, each bound on its
    /// group's port where other sockets may bind it too, on every interface
    /// this egress holds. A group at port 0 takes the port the host
    /// chooses. Fails when neither family joins its group anywhere. Must be
    /// called within a Tokio runtime.
    pub fn join_multicast(&self, v4: SocketAddrV4, v6: SocketAddrV6) -> io::Result<MulticastSocket> {
        let interfaces: Vec<_> = if_addrs::get_if_addrs()?
            .into_iter()
            .filter(|interface| self.holds(interface))
            .collect();
        let plan = plan(&interfaces);
        let (out_v4, out_v6) = plan.out.iter().copied().partition(|out| matches!(out, Out::V4(_)));
        let joined_v4 = self.joined(v4.into(), out_v4, |socket| {
            plan.join_v4
                .iter()
                .map(|at| (at.to_string(), socket.join_multicast_v4(v4.ip(), at)))
                .collect()
        });
        let joined_v6 = self.joined(v6.into(), out_v6, |socket| {
            plan.join_v6
                .iter()
                .map(|index| (index.to_string(), socket.join_multicast_v6(v6.ip(), *index)))
                .collect()
        });
        if matches!(
            (&joined_v4, &joined_v6),
            (Ok(None) | Err(_), Ok(None) | Err(_))
        ) {
            return Err(joined_v4.err().or(joined_v6.err()).unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    "joined the multicast groups on no interface",
                )
            }));
        }
        let kept = |joined: io::Result<Option<Group>>| {
            joined
                .inspect_err(|error| tracing::debug!(%error, "no socket for a multicast group"))
                .ok()
                .flatten()
        };
        Ok(MulticastSocket {
            v4: kept(joined_v4),
            v6: kept(joined_v6),
            interfaces,
        })
    }

    fn holds(&self, interface: &Interface) -> bool {
        match self {
            Self::System => true,
            Self::Interface(name) => interface.name == *name,
            Self::SourceAddress(source) => interface.ip() == *source,
        }
    }

    // `group`'s socket, joined wherever `join` joins it, sending out of
    // `out`; none when it joins nowhere.
    fn joined(
        &self,
        group: SocketAddr,
        out: Vec<Out>,
        join: impl FnOnce(&Socket) -> Vec<(String, io::Result<()>)>,
    ) -> io::Result<Option<Group>> {
        let socket = self.group_socket(group)?;
        let mut joined = false;
        for (at, result) in join(&socket) {
            match result {
                Ok(()) => joined = true,
                Err(error) => tracing::debug!(%group, at, %error, "multicast join failed"),
            }
        }
        if !joined {
            return Ok(None);
        }
        let socket = UdpSocket::from_std(socket.into())?;
        let to = SocketAddr::new(group.ip(), socket.local_addr()?.port());
        Ok(Some(Group { socket, to, out }))
    }

    fn group_socket(&self, group: SocketAddr) -> io::Result<Socket> {
        let socket = Socket::new(Domain::for_address(group), Type::DGRAM, Some(Protocol::UDP))?;
        let unspecified = if group.is_ipv6() {
            socket.set_only_v6(true)?;
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        } else {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        };
        socket.set_reuse_address(true)?;
        #[cfg(not(windows))]
        socket.set_reuse_port(true)?;
        if let Self::Interface(name) = self {
            bind_interface(&socket, name, group.is_ipv6())?;
        }
        socket.bind(&SocketAddr::new(unspecified, group.port()).into())?;
        socket.set_nonblocking(true)?;
        Ok(socket)
    }
}

impl MulticastSocket {
    /// The next datagram on either family's socket, and where it came from.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            let group = tokio::select! {
                group = readable(self.v4.as_ref()) => group?,
                group = readable(self.v6.as_ref()) => group?,
            };
            match group.socket.try_recv_from(buf) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                received => return received,
            }
        }
    }

    /// Send each group `payload(group)` out of every interface its datagrams
    /// leave through. A send that fails is passed over.
    pub async fn send_everywhere(&self, payload: impl Fn(SocketAddr) -> Vec<u8>) {
        for group in [&self.v4, &self.v6].into_iter().flatten() {
            let payload = payload(group.to);
            for &out in &group.out {
                if let Err(error) = group.send(&payload, out).await {
                    tracing::debug!(to = %group.to, ?out, %error, "multicast send failed");
                }
            }
        }
    }

    /// Whether `source` is an address of an interface the egress holds, or
    /// on the network of one.
    pub fn is_local(&self, source: SocketAddr) -> bool {
        self.interfaces
            .iter()
            .any(|interface| on_network(source, interface))
    }
}

async fn readable(group: Option<&Group>) -> io::Result<&Group> {
    match group {
        Some(group) => group.socket.readable().await.map(|()| group),
        None => std::future::pending().await,
    }
}

impl Group {
    // Send `payload` to the group out of `out`, choosing the interface right
    // before every attempt.
    async fn send(&self, payload: &[u8], out: Out) -> io::Result<usize> {
        let socket = SockRef::from(&self.socket);
        self.socket
            .async_io(Interest::WRITABLE, || {
                match out {
                    Out::V4(address) => socket.set_multicast_if_v4(&address)?,
                    Out::V6(index) => socket.set_multicast_if_v6(index)?,
                }
                socket.send_to(payload, &self.to.into())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interface(name: &str, index: u32, address: &str, prefix: u8) -> Interface {
        let addr = match address.parse().unwrap() {
            IpAddr::V4(ip) => IfAddr::V4(if_addrs::Ifv4Addr {
                ip,
                netmask: Ipv4Addr::from_bits(u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0)),
                prefixlen: prefix,
                broadcast: None,
            }),
            IpAddr::V6(ip) => IfAddr::V6(if_addrs::Ifv6Addr {
                ip,
                netmask: Ipv6Addr::from_bits(
                    u128::MAX.checked_shl(128 - u32::from(prefix)).unwrap_or(0),
                ),
                prefixlen: prefix,
                broadcast: None,
            }),
        };
        Interface {
            name: name.into(),
            addr,
            index: Some(index),
            oper_status: if_addrs::IfOperStatus::Up,
            is_p2p: false,
            #[cfg(windows)]
            adapter_name: String::new(),
        }
    }

    fn host() -> Vec<Interface> {
        vec![
            interface("lo", 1, "127.0.0.1", 8),
            interface("lo", 1, "::1", 128),
            interface("eth0", 2, "10.0.0.1", 24),
            interface("eth0", 2, "10.0.0.2", 24),
            interface("eth0", 2, "fd00::1", 64),
            interface("eth0", 2, "fe80::1", 64),
            interface("eth1", 3, "203.0.113.5", 24),
            interface("eth1", 3, "2001:db8::5", 64),
            interface("wlan0", 4, "fe80::2", 64),
        ]
    }

    // IPv4 joins at every private or loopback address and sends once an
    // interface; IPv6 joins on every interface past link-local and sends
    // where a unique local address is held.
    #[test]
    fn groups_join_and_send_where_their_family_is_local() {
        assert_eq!(
            plan(&host()),
            Plan {
                join_v4: vec![
                    Ipv4Addr::LOCALHOST,
                    Ipv4Addr::new(10, 0, 0, 1),
                    Ipv4Addr::new(10, 0, 0, 2),
                ],
                join_v6: vec![1, 2, 3],
                out: vec![
                    Out::V4(Ipv4Addr::LOCALHOST),
                    Out::V4(Ipv4Addr::new(10, 0, 0, 1)),
                    Out::V6(2),
                ],
            }
        );
    }

    // A source is local on an interface's network, or at the interface of
    // its link-local scope.
    #[test]
    fn a_source_is_local_on_the_network_of_an_interface() {
        let host = host();
        let local = |source: &str| {
            let source: SocketAddr = source.parse().unwrap();
            host.iter().any(|interface| on_network(source, interface))
        };
        assert!(local("10.0.0.7:6771"));
        assert!(local("127.0.0.1:6771"));
        assert!(!local("192.168.1.5:6771"));
        assert!(local("[fd00::7]:6771"));
        assert!(!local("[fd01::7]:6771"));
        assert!(local("[fe80::9%2]:6771"));
        assert!(local("[fe80::9%4]:6771"));
        assert!(!local("[fe80::9%9]:6771"));
    }

    // An interface egress holds that interface's addresses alone; a source
    // address egress holds that address alone.
    #[test]
    fn an_egress_holds_its_own_interfaces() {
        let held = |egress: SocketEgress| -> Vec<IpAddr> {
            host()
                .into_iter()
                .filter(|interface| egress.holds(interface))
                .map(|interface| interface.ip())
                .collect()
        };
        assert_eq!(held(SocketEgress::System).len(), host().len());
        assert_eq!(
            held(SocketEgress::Interface("eth1".into())),
            ["203.0.113.5".parse::<IpAddr>().unwrap(), "2001:db8::5".parse().unwrap()]
        );
        assert_eq!(
            held(SocketEgress::SourceAddress("10.0.0.2".parse().unwrap())),
            ["10.0.0.2".parse::<IpAddr>().unwrap()]
        );
    }

    // On loopback the IPv4 group hears its own datagram, from loopback.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_group_hears_its_own_datagram_on_loopback() {
        let socket = SocketEgress::Interface("lo0".into())
            .join_multicast(
                SocketAddrV4::new(Ipv4Addr::new(239, 192, 152, 143), 0),
                SocketAddrV6::new(Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0xefc0, 0x988f), 0, 0, 0),
            )
            .unwrap();
        let sent = format!("weaver multicast {}", std::process::id()).into_bytes();
        socket.send_everywhere(|_| sent.clone()).await;
        let mut buf = [0u8; 64];
        let from = loop {
            let (len, from) = socket.recv_from(&mut buf).await.unwrap();
            if buf[..len] == sent[..] {
                break from;
            }
        };
        assert_eq!(from.ip(), IpAddr::from(Ipv4Addr::LOCALHOST));
        assert!(socket.is_local(from));
    }
}
