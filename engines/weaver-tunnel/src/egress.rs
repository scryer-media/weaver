//! Socket construction at the bottom of an outbound route.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, UdpSocket},
    time::Duration,
};

use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};

/// Binding is exclusive: choosing a device leaves source selection to the kernel.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub enum SocketEgress {
    #[default]
    System,
    Interface(String),
    SourceAddress(IpAddr),
}

#[derive(Default)]
struct InterfaceFamilies {
    at: Option<std::time::Instant>,
    monitored: bool,
    families: std::collections::HashSet<(String, bool)>,
}
static INTERFACE_FAMILIES: std::sync::OnceLock<std::sync::RwLock<InterfaceFamilies>> =
    std::sync::OnceLock::new();

/// Publish the health monitor's already validated address families for socket filtering.
pub fn cache_interface_families(families: impl IntoIterator<Item = (String, bool)>) {
    let cache = INTERFACE_FAMILIES.get_or_init(Default::default);
    *cache.write().expect("interface families") = InterfaceFamilies {
        at: Some(std::time::Instant::now()),
        monitored: true,
        families: families.into_iter().collect(),
    };
}
fn interface_supports(name: &str, ipv4: bool) -> bool {
    let cache = INTERFACE_FAMILIES.get_or_init(Default::default);
    let snapshot = cache.read().expect("interface families");
    if snapshot.monitored
        || snapshot
            .at
            .is_some_and(|at| at.elapsed() < Duration::from_secs(5))
    {
        return snapshot
            .families
            .iter()
            .any(|(interface, family)| interface == name && *family == ipv4);
    }
    drop(snapshot);
    // Standalone users without a health monitor refresh at most once per interval.
    let mut snapshot = cache.write().expect("interface families");
    if snapshot
        .at
        .is_none_or(|at| at.elapsed() >= Duration::from_secs(5))
    {
        snapshot.families = if_addrs::get_if_addrs()
            .unwrap_or_default()
            .into_iter()
            .filter(|interface| {
                interface.is_oper_up()
                    && usable_address(interface.ip())
                    && address_flags(&interface.name, interface.ip()) == (false, false)
            })
            .map(|interface| {
                let ipv4 = interface.ip().is_ipv4();
                (interface.name, ipv4)
            })
            .collect();
        snapshot.at = Some(std::time::Instant::now());
    }
    snapshot
        .families
        .iter()
        .any(|(interface, family)| interface == name && *family == ipv4)
}

impl SocketEgress {
    pub fn supports_address(&self, address: IpAddr) -> bool {
        match self {
            Self::SourceAddress(source) => source.is_ipv4() == address.is_ipv4(),
            Self::Interface(name) => interface_supports(name, address.is_ipv4()),
            Self::System => true,
        }
    }

    fn socket(&self, target: SocketAddr, kind: Type, protocol: Protocol) -> io::Result<Socket> {
        if let Self::Interface(name) = self
            && (name.is_empty() || name.as_bytes().contains(&0))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "interface name must be nonempty and contain no NUL",
            ));
        }
        if !self.supports_address(target.ip()) {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "source and destination address families differ",
            ));
        }
        let socket = Socket::new(Domain::for_address(target), kind, Some(protocol))?;
        match self {
            Self::System => {}
            Self::SourceAddress(source) => {
                if source.is_unspecified() || source.is_multicast() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "source address must be a unicast host address",
                    ));
                }
                socket.bind(&SocketAddr::new(*source, 0).into())?;
            }
            Self::Interface(name) => bind_interface(&socket, name, target.is_ipv6())?,
        }
        Ok(socket)
    }

    fn tcp_socket(&self, target: SocketAddr) -> io::Result<Socket> {
        let socket = self.socket(target, Type::STREAM, Protocol::TCP)?;
        socket.set_tcp_nodelay(true)?;
        // Keepalive is best-effort, as in the existing NNTP connect path.
        let _ = socket.set_tcp_keepalive(
            &TcpKeepalive::new()
                .with_time(Duration::from_secs(60))
                .with_interval(Duration::from_secs(15)),
        );
        Ok(socket)
    }

    pub fn connect_blocking(&self, target: SocketAddr, timeout: Duration) -> io::Result<TcpStream> {
        let socket = self.tcp_socket(target)?;
        socket.connect_timeout(&target.into(), timeout)?;
        Ok(socket.into())
    }

    /// Dropping the connect future closes the bound socket without a detached dial.
    pub async fn connect(&self, target: SocketAddr) -> io::Result<tokio::net::TcpStream> {
        let socket = self.tcp_socket(target)?;
        socket.set_nonblocking(true)?;
        tokio::net::TcpSocket::from_std_stream(socket.into())
            .connect(target)
            .await
    }

    /// Bind a datagram socket for a tunnel peer of the supplied address family.
    pub fn bind_udp(&self, peer: SocketAddr) -> io::Result<UdpSocket> {
        let socket = self.socket(peer, Type::DGRAM, Protocol::UDP)?;
        if !matches!(self, Self::SourceAddress(_)) {
            let unspecified = if peer.is_ipv4() {
                IpAddr::V4(Ipv4Addr::UNSPECIFIED)
            } else {
                IpAddr::V6(Ipv6Addr::UNSPECIFIED)
            };
            socket.bind(&SocketAddr::new(unspecified, 0).into())?;
        }
        socket.set_nonblocking(true)?;
        Ok(socket.into())
    }
}

fn bind_interface(socket: &Socket, name: &str, ipv6: bool) -> io::Result<()> {
    if name.is_empty() || name.as_bytes().contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "interface name must be nonempty and contain no NUL",
        ));
    }
    #[cfg(target_os = "linux")]
    {
        let _ = ipv6;
        socket.bind_device(Some(name.as_bytes())).map_err(|error| {
            if error.kind() == io::ErrorKind::PermissionDenied {
                io::Error::new(
                    error.kind(),
                    "interface binding needs Linux 5.7 or CAP_NET_RAW",
                )
            } else {
                error
            }
        })
    }
    #[cfg(target_os = "macos")]
    {
        let name = std::ffi::CString::new(name)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid interface name"))?;
        // SAFETY: CString supplies the NUL-terminated name required by libc.
        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        let index = std::num::NonZeroU32::new(index).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "egress interface does not exist")
        })?;
        if ipv6 {
            socket.bind_device_by_index_v6(Some(index))
        } else {
            socket.bind_device_by_index_v4(Some(index))
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (socket, ipv6);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "interface binding is not supported on this platform; use a source address",
        ))
    }
}

/// Address flags used both by health discovery and address-race family filtering.
/// Unknown IPv6 flags fail closed; IPv4 has no duplicate-address detection state.
pub fn address_flags(name: &str, address: IpAddr) -> (bool, bool) {
    if address.is_ipv4() {
        return (false, false);
    }
    NativeFlags::open().flags(name, address)
}

/// [`address_flags`] for every `(interface, address)` pair, in order, from one
/// read of the platform's address state: one parse of `/proc/net/if_inet6` on
/// Linux and one ioctl socket on macOS, instead of one per address.
pub fn address_flags_many(addresses: &[(&str, IpAddr)]) -> Vec<(bool, bool)> {
    if addresses.iter().all(|(_, address)| address.is_ipv4()) {
        return vec![(false, false); addresses.len()];
    }
    let native = NativeFlags::open();
    addresses
        .iter()
        .map(|&(name, address)| {
            if address.is_ipv4() {
                (false, false)
            } else {
                native.flags(name, address)
            }
        })
        .collect()
}

/// The platform's IPv6 address state, opened once and asked per address.
/// Unknown flags fail closed as tentative.
struct NativeFlags {
    #[cfg(target_os = "linux")]
    table: Option<std::collections::HashMap<(String, u128), (bool, bool)>>,
    #[cfg(target_os = "macos")]
    socket: Option<std::os::fd::OwnedFd>,
}

impl NativeFlags {
    fn open() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            table: std::fs::read_to_string("/proc/net/if_inet6")
                .ok()
                .map(|text| parse_if_inet6(&text)),
            #[cfg(target_os = "macos")]
            socket: macos_flags_socket(),
        }
    }

    fn flags(&self, name: &str, address: IpAddr) -> (bool, bool) {
        self.native(name, address).unwrap_or((false, true))
    }

    #[cfg(target_os = "linux")]
    fn native(&self, name: &str, address: IpAddr) -> Option<(bool, bool)> {
        let IpAddr::V6(address) = address else {
            return Some((false, false));
        };
        self.table
            .as_ref()?
            .get(&(name.to_owned(), u128::from(address)))
            .copied()
    }

    #[cfg(target_os = "macos")]
    fn native(&self, name: &str, address: IpAddr) -> Option<(bool, bool)> {
        macos_address_flags(self.socket.as_ref()?, name, address)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn native(&self, name: &str, address: IpAddr) -> Option<(bool, bool)> {
        native_address_flags(name, address)
    }
}

/// `/proc/net/if_inet6` rows keyed by interface and address, mapped to
/// (deprecated, tentative). The first row for a pair wins.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_if_inet6(text: &str) -> std::collections::HashMap<(String, u128), (bool, bool)> {
    let mut table = std::collections::HashMap::new();
    for line in text.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() != 6 {
            continue;
        }
        let (Ok(address), Ok(flags)) = (
            u128::from_str_radix(fields[0], 16),
            u32::from_str_radix(fields[4], 16),
        ) else {
            continue;
        };
        if fields[0].len() != 32 {
            continue;
        }
        table
            .entry((fields[5].to_owned(), address))
            .or_insert((flags & 0x20 != 0, flags & (0x40 | 0x08) != 0));
    }
    table
}

pub fn usable_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ip) => !ip.is_unspecified() && !ip.is_multicast() && !ip.is_link_local(),
        IpAddr::V6(ip) => {
            !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_unicast_link_local()
                && !ip.is_unique_local()
        }
    }
}

#[cfg(target_os = "macos")]
fn macos_flags_socket() -> Option<std::os::fd::OwnedFd> {
    use std::os::fd::{FromRawFd, OwnedFd};
    // SAFETY: socket has no pointer arguments. OwnedFd closes it on every exit.
    let fd = unsafe { libc::socket(libc::AF_INET6, libc::SOCK_DGRAM, 0) };
    if fd < 0 {
        return None;
    }
    // SAFETY: fd is the newly created, uniquely owned descriptor above.
    Some(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "macos")]
fn macos_address_flags(
    fd: &std::os::fd::OwnedFd,
    name: &str,
    address: IpAddr,
) -> Option<(bool, bool)> {
    use std::os::fd::AsRawFd;
    let IpAddr::V6(address) = address else {
        return Some((false, false));
    };
    // Darwin in6_ifreq: IFNAMSIZ followed by the 272-byte, u64-aligned union.
    #[repr(C)]
    union Value {
        address: libc::sockaddr_in6,
        flags: libc::c_int,
        storage: [u64; 34],
    }
    #[repr(C)]
    struct Request {
        name: [libc::c_char; 16],
        value: Value,
    }
    if name.len() >= 16 || name.as_bytes().contains(&0) {
        return None;
    }
    let mut request = Request {
        name: [0; 16],
        value: Value { storage: [0; 34] },
    };
    for (to, from) in request.name.iter_mut().zip(name.bytes()) {
        *to = from as libc::c_char;
    }
    request.value.address = libc::sockaddr_in6 {
        sin6_len: std::mem::size_of::<libc::sockaddr_in6>() as u8,
        sin6_family: libc::AF_INET6 as u8,
        sin6_port: 0,
        sin6_flowinfo: 0,
        sin6_addr: libc::in6_addr {
            s6_addr: address.octets(),
        },
        sin6_scope_id: 0,
    };
    // SIOCGIFAFLAG_IN6 = _IOWR('i',73,struct in6_ifreq), verified against the SDK.
    // SAFETY: request has Darwin's exact size/alignment and initialized storage.
    if unsafe { libc::ioctl(fd.as_raw_fd(), 0xc1206949 as libc::c_ulong, &mut request) } < 0 {
        return None;
    }
    // SAFETY: this ioctl writes the union's flags member on success.
    let flags = unsafe { request.value.flags };
    Some((flags & 0x10 != 0, flags & (0x02 | 0x04 | 0x08) != 0))
}

#[cfg(test)]
mod flags_tests {
    use super::*;

    #[test]
    fn if_inet6_rows_map_by_interface_and_address() {
        let text = "\
00000000000000000000000000000001 01 80 10 80       lo
20010db8000000000000000000000001 02 40 00 20     eth0
20010db8000000000000000000000002 02 40 00 40     eth0
20010db8000000000000000000000002 03 40 00 00     eth1
fe800000000000000000000000000001 02 40 20 08     eth0
malformed line
";
        let table = parse_if_inet6(text);
        let flags = |name: &str, address: &str| {
            let address: std::net::Ipv6Addr = address.parse().unwrap();
            table.get(&(name.to_owned(), u128::from(address))).copied()
        };
        assert_eq!(flags("lo", "::1"), Some((false, false)));
        assert_eq!(flags("eth0", "2001:db8::1"), Some((true, false)));
        assert_eq!(flags("eth0", "2001:db8::2"), Some((false, true)));
        assert_eq!(flags("eth1", "2001:db8::2"), Some((false, false)));
        assert_eq!(flags("eth0", "fe80::1"), Some((false, true)));
        assert_eq!(flags("eth1", "2001:db8::1"), None);
        assert_eq!(table.len(), 5);
    }

    #[test]
    fn batch_flags_match_the_one_address_answer_in_order() {
        let mut pairs: Vec<(String, IpAddr)> = if_addrs::get_if_addrs()
            .unwrap()
            .into_iter()
            .map(|iface| (iface.name.clone(), iface.ip()))
            .collect();
        // An address no interface carries fails closed as tentative, and an
        // IPv4 address has no duplicate-address state at all.
        pairs.push(("weaver-none0".into(), "2001:db8::7".parse().unwrap()));
        pairs.push(("weaver-none0".into(), "192.0.2.7".parse().unwrap()));
        let borrowed: Vec<(&str, IpAddr)> = pairs
            .iter()
            .map(|(name, address)| (name.as_str(), *address))
            .collect();
        let batch = address_flags_many(&borrowed);
        let one_by_one: Vec<_> = borrowed
            .iter()
            .map(|&(name, address)| address_flags(name, address))
            .collect();
        assert_eq!(batch, one_by_one);
        assert_eq!(batch[batch.len() - 2], (false, true));
        assert_eq!(batch[batch.len() - 1], (false, false));
    }

    #[test]
    fn an_all_ipv4_batch_reads_no_platform_state() {
        let addresses = [
            ("eth0", "192.0.2.1".parse().unwrap()),
            ("eth1", "198.51.100.1".parse().unwrap()),
        ];
        assert_eq!(address_flags_many(&addresses), vec![(false, false); 2]);
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn native_address_flags(_name: &str, _address: IpAddr) -> Option<(bool, bool)> {
    None
}

#[cfg(target_os = "windows")]
fn native_address_flags(name: &str, address: IpAddr) -> Option<(bool, bool)> {
    use windows_sys::Win32::{
        NetworkManagement::IpHelper::{
            FreeMibTable, GetUnicastIpAddressTable, MIB_UNICASTIPADDRESS_TABLE,
        },
        Networking::WinSock::AF_INET6,
    };
    let IpAddr::V6(address) = address else {
        return Some((false, false));
    };
    let index = if_addrs::get_if_addrs()
        .ok()?
        .into_iter()
        .find(|interface| interface.name == name)?
        .index?;
    let mut table: *mut MIB_UNICASTIPADDRESS_TABLE = std::ptr::null_mut();
    // SAFETY: API initializes the output table, freed exactly once below.
    if unsafe { GetUnicastIpAddressTable(AF_INET6, &mut table) } != 0 || table.is_null() {
        return None;
    }
    // SAFETY: Windows allocates NumEntries contiguous rows following the header.
    let rows = unsafe {
        std::slice::from_raw_parts((*table).Table.as_ptr(), (*table).NumEntries as usize)
    };
    let result = rows.iter().find_map(|row| {
        // SAFETY: the AF_INET6 query produces IPv6 sockaddr rows.
        let bytes = unsafe { row.Address.Ipv6.sin6_addr.u.Byte };
        if row.InterfaceIndex != index || bytes != address.octets() {
            return None;
        }
        Some((
            row.DadState == 3 || row.PreferredLifetime == 0,
            row.DadState < 3 || row.SkipAsSource,
        ))
    });
    // SAFETY: table came from GetUnicastIpAddressTable and has not been freed.
    unsafe { FreeMibTable(table.cast()) };
    result
}
