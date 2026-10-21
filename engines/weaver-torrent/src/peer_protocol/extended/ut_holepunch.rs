// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The holepunch extension (BEP 55): a peer connected to two others
//! introduces them so they can connect through their NATs over uTP
//! (libtorrent `bt_peer_connection::on_holepunch`, `write_holepunch_msg`).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use byteorder::{BE, ByteOrder, WriteBytesExt};

use crate::peer_protocol::{DoubleBufHelper, MessageDeserializeError, SerializeError};

/// A holepunch message: what it asks, and of the peer at which address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UtHolepunch {
    /// The sender asks to be introduced to the peer at the address.
    Rendezvous(SocketAddr),
    /// The receiver is to connect to the peer at the address, which is told
    /// to connect back.
    Connect(SocketAddr),
    /// The rendezvous for the address failed, with an error code.
    Error(SocketAddr, u32),
}

/// The error codes of BEP 55.
pub mod error {
    /// The named peer is not one the introducer knows.
    pub const NO_SUCH_PEER: u32 = 1;
    /// The introducer is not connected to the named peer.
    pub const NOT_CONNECTED: u32 = 2;
    /// The named peer does not speak the holepunch extension.
    pub const NO_SUPPORT: u32 = 3;
    /// The named peer is the sender.
    pub const NO_SELF: u32 = 4;
}

const RENDEZVOUS: u8 = 0;
const CONNECT: u8 = 1;
const ERROR: u8 = 2;

impl UtHolepunch {
    pub fn addr(&self) -> SocketAddr {
        match *self {
            UtHolepunch::Rendezvous(a) | UtHolepunch::Connect(a) | UtHolepunch::Error(a, _) => a,
        }
    }

    /// Writes the message: type, address type, address, port and error code.
    /// The error code is written on every message, 0 but on an error, as BEP
    /// 55 lays out; libtorrent writes it on errors only and reads it only
    /// there.
    pub fn serialize(&self, out: &mut impl std::io::Write) -> Result<(), SerializeError> {
        let (kind, err) = match *self {
            UtHolepunch::Rendezvous(_) => (RENDEZVOUS, 0),
            UtHolepunch::Connect(_) => (CONNECT, 0),
            UtHolepunch::Error(_, e) => (ERROR, e),
        };
        out.write_u8(kind)?;
        let addr = self.addr();
        match addr.ip().to_canonical() {
            IpAddr::V4(ip) => {
                out.write_u8(0)?;
                out.write_all(&ip.octets())?;
            }
            IpAddr::V6(ip) => {
                out.write_u8(1)?;
                out.write_all(&ip.octets())?;
            }
        }
        out.write_u16::<BE>(addr.port())?;
        out.write_u32::<BE>(err)?;
        Ok(())
    }

    /// Reads a message. The error code may be left out of a message other
    /// than an error, as libtorrent does; bytes past it are ignored.
    pub fn deserialize(buf: DoubleBufHelper<'_>) -> Result<Self, MessageDeserializeError> {
        let b = buf
            .get_contiguous(buf.len())
            .ok_or(MessageDeserializeError::NeedContiguous)?;
        let invalid = || MessageDeserializeError::UtHolepunchInvalid;
        let (&kind, rest) = b.split_first().ok_or_else(invalid)?;
        let (&addr_type, rest) = rest.split_first().ok_or_else(invalid)?;
        let (ip, rest): (IpAddr, &[u8]) = match addr_type {
            0 if rest.len() >= 4 => {
                let (ip, rest) = rest.split_at(4);
                (
                    Ipv4Addr::from(<[u8; 4]>::try_from(ip).unwrap()).into(),
                    rest,
                )
            }
            1 if rest.len() >= 16 => {
                let (ip, rest) = rest.split_at(16);
                (
                    Ipv6Addr::from(<[u8; 16]>::try_from(ip).unwrap()).into(),
                    rest,
                )
            }
            _ => return Err(invalid()),
        };
        if rest.len() < 2 {
            return Err(invalid());
        }
        let addr = SocketAddr::new(ip, BE::read_u16(rest));
        let err = rest.get(2..6).map(BE::read_u32);
        match kind {
            RENDEZVOUS => Ok(UtHolepunch::Rendezvous(addr)),
            CONNECT => Ok(UtHolepunch::Connect(addr)),
            ERROR => Ok(UtHolepunch::Error(addr, err.ok_or_else(invalid)?)),
            _ => Err(invalid()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_protocol::{
        MY_EXTENDED_UT_HOLEPUNCH,
        extended::{ExtendedMessage, PeerExtendedMessageIds},
    };

    fn bytes(msg: UtHolepunch) -> Vec<u8> {
        let mut out = Vec::new();
        msg.serialize(&mut out).unwrap();
        out
    }

    fn parse(b: &[u8]) -> Result<UtHolepunch, MessageDeserializeError> {
        UtHolepunch::deserialize(DoubleBufHelper::new(b, &[]))
    }

    // BEP 55: msg_type, addr_type, addr, port, err_code, big-endian.
    #[test]
    fn a_holepunch_message_is_type_address_port_and_error_code() {
        let v4: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        assert_eq!(
            bytes(UtHolepunch::Rendezvous(v4)),
            [0, 0, 1, 2, 3, 4, 0x1a, 0xe1, 0, 0, 0, 0]
        );
        let v6: SocketAddr = "[2001:db8::1]:80".parse().unwrap();
        let mut connect = vec![1, 1];
        connect.extend_from_slice(&"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets());
        connect.extend_from_slice(&[0, 80, 0, 0, 0, 0]);
        assert_eq!(bytes(UtHolepunch::Connect(v6)), connect);
        // An IPv4 peer reached over IPv6 is named by its IPv4 address.
        let mapped: SocketAddr = "[::ffff:1.2.3.4]:6881".parse().unwrap();
        assert_eq!(
            bytes(UtHolepunch::Error(mapped, error::NOT_CONNECTED)),
            [2, 0, 1, 2, 3, 4, 0x1a, 0xe1, 0, 0, 0, 2]
        );
        for msg in [
            UtHolepunch::Rendezvous(v4),
            UtHolepunch::Connect(v6),
            UtHolepunch::Error(v4, error::NO_SELF),
        ] {
            assert_eq!(parse(&bytes(msg)).unwrap(), msg);
        }
    }

    // libtorrent `bt_peer_connection::on_holepunch` reads the error code of
    // an error only, and stops at the end of what it reads.
    #[test]
    fn the_error_code_is_needed_only_on_an_error() {
        let v4: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let short = [1, 0, 1, 2, 3, 4, 0x1a, 0xe1];
        assert_eq!(parse(&short).unwrap(), UtHolepunch::Connect(v4));
        let mut long = bytes(UtHolepunch::Connect(v4));
        long.push(9);
        assert_eq!(parse(&long).unwrap(), UtHolepunch::Connect(v4));
        assert!(matches!(
            parse(&[2, 0, 1, 2, 3, 4, 0x1a, 0xe1]),
            Err(MessageDeserializeError::UtHolepunchInvalid)
        ));
    }

    #[test]
    fn an_unknown_type_or_address_type_or_a_short_address_is_invalid() {
        for bad in [
            &[3, 0, 1, 2, 3, 4, 0, 1][..],
            &[0, 2, 1, 2, 3, 4, 0, 1],
            &[0, 0, 1, 2, 3, 4, 0],
            &[0, 1, 1, 2, 3, 4, 0, 1],
            &[0],
            &[],
        ] {
            assert!(
                matches!(parse(bad), Err(MessageDeserializeError::UtHolepunchInvalid)),
                "{bad:?}"
            );
        }
    }

    // libtorrent `bt_peer_connection::write_holepunch_msg` sends under the
    // id the peer gave ut_holepunch; we read it under ours.
    #[test]
    fn a_holepunch_goes_under_the_peers_id_and_is_read_under_ours() {
        let msg = UtHolepunch::Rendezvous("1.2.3.4:6881".parse().unwrap());
        let theirs = PeerExtendedMessageIds {
            ut_holepunch: Some(7),
            ..Default::default()
        };
        let mut buf = [0u8; 64];
        let len = ExtendedMessage::UtHolepunch(msg)
            .serialize(&mut buf, &|| theirs)
            .unwrap();
        assert_eq!(buf[0], 7);
        assert_eq!(&buf[1..len], bytes(msg));

        buf[0] = MY_EXTENDED_UT_HOLEPUNCH;
        assert_eq!(
            ExtendedMessage::deserialize(DoubleBufHelper::new(&buf[..len], &[])).unwrap(),
            ExtendedMessage::UtHolepunch(msg)
        );
        assert!(matches!(
            ExtendedMessage::UtHolepunch(msg).serialize(&mut buf, &PeerExtendedMessageIds::default),
            Err(SerializeError::NeedHolepunch)
        ));
    }
}
