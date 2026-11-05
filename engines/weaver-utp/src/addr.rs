// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{IpAddr, SocketAddr};

/// Represent an IPv4-mapped IPv6 address as IPv4, leaving other addresses intact.
pub fn canonical_ip_addr(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(address, IpAddr::V4),
        IpAddr::V4(_) => address,
    }
}

/// Use IPv4 for mapped endpoints, preserving every field of other IPv6 endpoints.
pub fn canonical_socket_addr(address: SocketAddr) -> SocketAddr {
    match address {
        SocketAddr::V6(endpoint) => endpoint
            .ip()
            .to_ipv4_mapped()
            .map_or(address, |ip| SocketAddr::from((ip, endpoint.port()))),
        SocketAddr::V4(_) => address,
    }
}
