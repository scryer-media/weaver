// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A NAT-PMP port mapping asked of a tunnel's gateway, through the tunnel.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use weaver_tunnel::endpoint::DatagramSocket;

pub(super) const PORT: u16 = 5351;
// What a mapping asks to last; a gateway may grant less.
const LIFETIME: u32 = 3600;
// The first wait for an answer, doubled on each try.
const FIRST_WAIT: Duration = Duration::from_millis(250);
const TRIES: u32 = 6;

// The gateway a tunnel's own address implies: the first host of its /24.
pub(crate) fn gateway_of(tunnel_addresses: &[String]) -> Option<IpAddr> {
    tunnel_addresses.iter().find_map(|address| {
        let ip = address.split('/').next()?.trim().parse::<Ipv4Addr>().ok()?;
        let [a, b, c, _] = ip.octets();
        Some(IpAddr::V4(Ipv4Addr::new(a, b, c, 1)))
    })
}

// A TCP mapping request for `internal`, any external port.
pub(super) fn request(internal: u16) -> [u8; 12] {
    let mut packet = [0u8; 12];
    packet[1] = 2;
    packet[4..6].copy_from_slice(&internal.to_be_bytes());
    packet[8..12].copy_from_slice(&LIFETIME.to_be_bytes());
    packet
}

// The external port of a TCP mapping answer, or why there is none.
pub(super) fn answer(packet: &[u8]) -> Result<u16, String> {
    if packet.len() < 16 || packet[0] != 0 || packet[1] != 130 {
        return Err("the gateway's answer is not a NAT-PMP mapping".into());
    }
    let result = u16::from_be_bytes([packet[2], packet[3]]);
    let external = u16::from_be_bytes([packet[10], packet[11]]);
    match result {
        0 if external != 0 => Ok(external),
        0 => Err("the gateway mapped no port".into()),
        1 => Err("the gateway does not speak this NAT-PMP version".into()),
        2 => Err("the gateway refused the mapping".into()),
        3 => Err("the gateway's network is down".into()),
        4 => Err("the gateway has no ports left".into()),
        5 => Err("the gateway does not map TCP".into()),
        other => Err(format!("the gateway answered NAT-PMP result {other}")),
    }
}

// Ask `gateway` to map TCP `internal` and say which port it opened.
pub(super) async fn map_tcp(
    socket: &dyn DatagramSocket,
    gateway: SocketAddr,
    internal: u16,
) -> Result<u16, String> {
    let packet = request(internal);
    let mut wait = FIRST_WAIT;
    let mut buffer = [0u8; 64];
    for _ in 0..TRIES {
        socket
            .send_to(&packet, gateway)
            .await
            .map_err(|error| format!("could not reach the gateway: {error}"))?;
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            match tokio::time::timeout_at(deadline, socket.recv_from(&mut buffer)).await {
                Err(_) => break,
                Ok(Err(error)) => return Err(format!("the gateway did not answer: {error}")),
                Ok(Ok((len, from))) if from == gateway => return answer(&buffer[..len]),
                Ok(Ok(_)) => {}
            }
        }
        wait *= 2;
    }
    Err(format!("{} did not answer NAT-PMP", gateway.ip()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gateway_is_the_first_host_of_the_tunnel_address() {
        assert_eq!(
            gateway_of(&["10.2.0.2/32".into()]),
            Some("10.2.0.1".parse().unwrap())
        );
        assert_eq!(gateway_of(&["fd00::2/128".into()]), None);
    }

    #[test]
    fn a_mapping_request_asks_for_tcp_with_a_lifetime() {
        let packet = request(51_413);
        assert_eq!(&packet[..2], &[0, 2]);
        assert_eq!(u16::from_be_bytes([packet[4], packet[5]]), 51_413);
        assert_eq!(u16::from_be_bytes([packet[6], packet[7]]), 0);
        assert_eq!(
            u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]),
            LIFETIME
        );
    }

    #[test]
    fn an_answer_gives_the_external_port_or_the_gateways_refusal() {
        let mut packet = [0u8; 16];
        packet[1] = 130;
        packet[10..12].copy_from_slice(&40_001u16.to_be_bytes());
        assert_eq!(answer(&packet), Ok(40_001));
        packet[3] = 2;
        assert_eq!(
            answer(&packet),
            Err("the gateway refused the mapping".into())
        );
        assert!(answer(&packet[..8]).is_err());
    }
}
