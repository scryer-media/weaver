// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

pub mod ext_close_reason;
pub mod selective_ack;

use tracing::trace;

use crate::{Error, constants::UTP_HEADER, seq_nr::SeqNr};

const NO_NEXT_EXT: u8 = 0;
const EXT_SELECTIVE_ACK: u8 = 1;
const EXT_CLOSE_REASON: u8 = 3;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum Type {
    ST_DATA = 0,
    ST_FIN = 1,
    #[default]
    ST_STATE = 2,
    ST_RESET = 3,
    ST_SYN = 4,
}

impl Type {
    fn from_number(num: u8) -> Option<Type> {
        match num {
            0 => Some(Type::ST_DATA),
            1 => Some(Type::ST_FIN),
            2 => Some(Type::ST_STATE),
            3 => Some(Type::ST_RESET),
            4 => Some(Type::ST_SYN),
            _ => None,
        }
    }

    fn to_number(self) -> u8 {
        match self {
            Type::ST_DATA => 0,
            Type::ST_FIN => 1,
            Type::ST_STATE => 2,
            Type::ST_RESET => 3,
            Type::ST_SYN => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Extensions {
    pub selective_ack: Option<selective_ack::SelectiveAck>,
    pub close_reason: Option<ext_close_reason::LibTorrentCloseReason>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UtpHeader {
    pub htype: Type,                            // 4 bits type and 4 bits version
    pub connection_id: SeqNr,                   // Connection ID
    pub timestamp_microseconds: u32,            // Timestamp in microseconds
    pub timestamp_difference_microseconds: u32, // Timestamp difference in microseconds
    pub wnd_size: u32,                          // Window size
    pub seq_nr: SeqNr,                          // Sequence number
    pub ack_nr: SeqNr,                          // Acknowledgment number
    pub extensions: Extensions,
}

impl UtpHeader {
    pub fn set_type(&mut self, packet_type: Type) {
        // let packet_type = packet_type.to_number();
        // self.type_ver = (self.type_ver & 0xF0) | (packet_type & 0x0F);
        self.htype = packet_type;
    }

    pub fn get_type(&self) -> Type {
        // Type::from_number(self.type_ver & 0x0F)
        self.htype
    }

    pub fn short_repr(&self) -> impl std::fmt::Display + '_ {
        struct D<'a>(&'a UtpHeader);
        impl std::fmt::Display for D<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{:?}:seq_nr={}:ack_nr={}:wnd_size={}",
                    self.0.get_type(),
                    self.0.seq_nr,
                    self.0.ack_nr,
                    self.0.wnd_size,
                )
            }
        }
        D(self)
    }

    pub fn serialize(&self, buffer: &mut [u8]) -> crate::Result<usize> {
        if buffer.len() < UTP_HEADER as usize {
            return Err(Error::SerializeTooSmallBuffer);
        }
        const VERSION: u8 = 1;
        const NEXT_EXT_IDX: usize = 1;
        let typever = (self.htype.to_number() << 4) | VERSION;
        buffer[0] = typever;
        buffer[NEXT_EXT_IDX] = NO_NEXT_EXT; // this will be overwritten down below if extensions are present.
        buffer[2..4].copy_from_slice(&self.connection_id.to_be_bytes());
        buffer[4..8].copy_from_slice(&self.timestamp_microseconds.to_be_bytes());
        buffer[8..12].copy_from_slice(&self.timestamp_difference_microseconds.to_be_bytes());
        buffer[12..16].copy_from_slice(&self.wnd_size.to_be_bytes());
        buffer[16..18].copy_from_slice(&self.seq_nr.to_be_bytes());
        buffer[18..20].copy_from_slice(&self.ack_nr.to_be_bytes());

        let mut next_ext_pos = NEXT_EXT_IDX;
        let mut offset = 20;

        macro_rules! add_ext {
            ($id:expr, $payload:expr) => {
                let payload = $payload;
                if buffer.len() >= offset + 2 + payload.len() {
                    buffer[next_ext_pos] = $id;
                    buffer[offset] = NO_NEXT_EXT;
                    buffer[offset + 1] = payload.len() as u8;
                    buffer[offset + 2..offset + 2 + payload.len()].copy_from_slice(payload);

                    #[allow(unused)]
                    {
                        next_ext_pos = offset;
                    }
                    offset += 2 + payload.len();
                }
            };
        }

        if let Some(sack) = self.extensions.selective_ack {
            add_ext!(EXT_SELECTIVE_ACK, sack.as_bytes());
        }
        if let Some(close_reason) = self.extensions.close_reason {
            add_ext!(EXT_CLOSE_REASON, &close_reason.as_bytes());
        }

        Ok(offset)
    }

    pub fn serialize_with_payload(
        &self,
        out_buf: &mut [u8],
        payload_serialize: impl FnOnce(&mut [u8]) -> crate::Result<usize>,
    ) -> crate::Result<usize> {
        let sz = self.serialize(out_buf)?;
        let payload_sz = payload_serialize(
            out_buf
                .get_mut(sz..)
                .ok_or(Error::SerializeTooSmallBuffer)?,
        )?;
        Ok(sz + payload_sz)
    }

    pub fn deserialize(orig_buffer: &[u8]) -> Option<(Self, usize)> {
        let mut buffer = orig_buffer;
        if buffer.len() < UTP_HEADER as usize {
            return None;
        }
        let mut header = UtpHeader::default();

        let typenum = buffer[0] >> 4;
        let version = buffer[0] & 0xf;
        if version != 1 {
            trace!(version, "wrong version");
            return None;
        }
        header.htype = Type::from_number(typenum)?;
        let mut next_ext = buffer[1];
        header.connection_id = u16::from_be_bytes(buffer[2..4].try_into().unwrap()).into();
        header.timestamp_microseconds = u32::from_be_bytes(buffer[4..8].try_into().unwrap());
        header.timestamp_difference_microseconds =
            u32::from_be_bytes(buffer[8..12].try_into().unwrap());
        header.wnd_size = u32::from_be_bytes(buffer[12..16].try_into().unwrap());
        header.seq_nr = u16::from_be_bytes(buffer[16..18].try_into().unwrap()).into();
        header.ack_nr = u16::from_be_bytes(buffer[18..20].try_into().unwrap()).into();

        buffer = &buffer[20..];

        let mut total_ext_size = 0usize;

        while next_ext > 0 {
            total_ext_size += 2;
            let ext = next_ext;
            next_ext = *buffer.first()?;
            let ext_len = *buffer.get(1)? as usize;

            let ext_data = buffer.get(2..2 + ext_len)?;
            match (ext, ext_len) {
                (EXT_SELECTIVE_ACK, _) => {
                    header.extensions.selective_ack =
                        Some(selective_ack::SelectiveAck::deserialize(ext_data));
                }
                (EXT_CLOSE_REASON, 4) => {
                    header.extensions.close_reason =
                        Some(ext_close_reason::LibTorrentCloseReason::parse(
                            ext_data.try_into().unwrap(),
                        ));
                }
                _ => {
                    trace!(
                        ext,
                        next_ext, ext_len, "unsupported extension for deserializing, skipping"
                    );
                }
            }

            total_ext_size += ext_len;
            buffer = buffer.get(2 + ext_len..)?;
        }

        Some((header, 20 + total_ext_size))
    }
}

#[cfg(test)]
mod tests {
    use crate::{raw::Type, test_util::setup_test_logging};

    use super::UtpHeader;

    #[test]
    fn test_parse_fin_with_extension() {
        setup_test_logging();
        let packet = include_bytes!("../test/resources/packet_fin_with_extension.bin");
        let (header, len) = UtpHeader::deserialize(packet).unwrap();
        assert_eq!(
            header,
            UtpHeader {
                htype: Type::ST_FIN,
                connection_id: 30796.into(),
                timestamp_microseconds: 2293274188,
                timestamp_difference_microseconds: 1967430273,
                wnd_size: 1048576,
                seq_nr: 54661.into(),
                ack_nr: 54397.into(),
                extensions: crate::raw::Extensions {
                    close_reason: Some(crate::raw::ext_close_reason::LibTorrentCloseReason(15)),
                    selective_ack: None
                }
            }
        );
        assert_eq!(len, packet.len());

        let mut buf = [0u8; 1024];
        let len = header.serialize(&mut buf).unwrap();
        assert_eq!(&buf[..len], packet);
    }

    // libtorrent writes the reason as a 32-bit field, two reserved zero
    // bytes then the reason, after any SACK; it reads the last two bytes
    // and skips the reserved ones whatever they hold.
    #[test]
    fn the_close_reason_is_two_reserved_bytes_then_the_reason() {
        use crate::raw::{
            Extensions, ext_close_reason::LibTorrentCloseReason, selective_ack::SelectiveAck,
        };
        let header = UtpHeader {
            htype: Type::ST_FIN,
            extensions: Extensions {
                selective_ack: SelectiveAck::new_test([1]),
                close_reason: Some(LibTorrentCloseReason(0x0102)),
            },
            ..Default::default()
        };
        let mut buf = [0u8; 64];
        let len = header.serialize(&mut buf).unwrap();
        assert_eq!(buf[1], 1, "SACK first");
        assert_eq!(buf[20], 3, "then the close reason");
        assert_eq!(&buf[len - 6..len], &[0, 4, 0, 0, 1, 2]);

        buf[len - 4] = 0xab;
        buf[len - 3] = 0xcd;
        let (parsed, _) = UtpHeader::deserialize(&buf[..len]).unwrap();
        assert_eq!(
            parsed.extensions.close_reason,
            Some(LibTorrentCloseReason(0x0102))
        );
    }
}
