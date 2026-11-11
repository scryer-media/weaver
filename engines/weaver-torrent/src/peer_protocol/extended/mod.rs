// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::io::Cursor;

use crate::bencode::BencodeValue;
use crate::bencode::bencode_serialize_to_writer;
use crate::buffers::ByteBuf;
use crate::buffers::ByteBufT;
use byteorder::{BE, WriteBytesExt};
use serde_derive::Deserialize;
use serde_derive::Serialize;
use ut_holepunch::UtHolepunch;
use ut_pex::UtPex;

use crate::peer_protocol::DoubleBufHelper;
use crate::peer_protocol::MSGID_EXTENDED;
use crate::peer_protocol::MY_EXTENDED_UT_HOLEPUNCH;
use crate::peer_protocol::MY_EXTENDED_UT_PEX;
use crate::peer_protocol::SerializeError;

use self::{handshake::ExtendedHandshake, ut_metadata::UtMetadata};

use super::MessageDeserializeError;

pub mod handshake;
pub mod ut_holepunch;
pub mod ut_metadata;
pub mod ut_pex;

use super::MY_EXTENDED_UT_METADATA;

/// The id we name for upload_only (libtorrent's
/// `bt_peer_connection::upload_only_msg` is 3, our ut_metadata's id).
pub const MY_EXTENDED_UPLOAD_ONLY: u8 = 2;

/// The id we name for lt_donthave (libtorrent
/// `bt_peer_connection::dont_have_msg`).
pub const MY_EXTENDED_LT_DONTHAVE: u8 = 7;

// An extension we do not speak has no entry in `m` (libtorrent
// `bt_peer_connection::write_handshake` lists the loaded plugins only).
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct PeerExtendedMessageIds {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ut_metadata: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ut_pex: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ut_holepunch: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_only: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lt_donthave: Option<u8>,
}

impl PeerExtendedMessageIds {
    pub fn my() -> Self {
        Self {
            ut_metadata: Some(MY_EXTENDED_UT_METADATA),
            ut_pex: Some(MY_EXTENDED_UT_PEX),
            ut_holepunch: None,
            upload_only: Some(MY_EXTENDED_UPLOAD_ONLY),
            lt_donthave: Some(MY_EXTENDED_LT_DONTHAVE),
        }
    }

    /// The ids the peer names once its extended handshake's `m` is taken
    /// over these, the ones it named before (BEP 10): an id it names
    /// replaces the one held, an id of 0 says it does not, or no longer
    /// does, speak that extension, and an extension it does not mention
    /// keeps the id held. No id held is ever 0 (libtorrent treats an id of
    /// 0 as no support, `bt_peer_connection::on_holepunch`,
    /// `write_upload_only`, `ut_metadata_peer_plugin::write_metadata_packet`).
    pub fn updated_by(self, m: Self) -> Self {
        fn take(held: Option<u8>, named: Option<u8>) -> Option<u8> {
            match named {
                Some(0) => None,
                Some(id) => Some(id),
                None => held,
            }
        }
        Self {
            ut_metadata: take(self.ut_metadata, m.ut_metadata),
            ut_pex: take(self.ut_pex, m.ut_pex),
            ut_holepunch: take(self.ut_holepunch, m.ut_holepunch),
            upload_only: take(self.upload_only, m.upload_only),
            lt_donthave: take(self.lt_donthave, m.lt_donthave),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ExtendedMessage<ByteBuf: ByteBufT> {
    Handshake(ExtendedHandshake<ByteBuf>),
    UtMetadata(UtMetadata<ByteBuf>),
    UtPex(UtPex<ByteBuf>),
    UtHolepunch(UtHolepunch),
    /// We only upload the torrent, or no longer only do: one byte, 1 or 0
    /// (libtorrent `bt_peer_connection::write_upload_only`).
    UploadOnly(bool),
    /// The sender no longer has the piece of this index: four bytes, big
    /// endian (libtorrent `bt_peer_connection::write_dont_have`).
    DontHave(u32),
    Dyn(u8, BencodeValue<ByteBuf>),
}

impl<'a> ExtendedMessage<ByteBuf<'a>> {
    pub fn serialize(
        &self,
        out: &mut [u8],
        peer_extended_msg_ids: &dyn Fn() -> PeerExtendedMessageIds,
    ) -> Result<usize, SerializeError> {
        let mut out = Cursor::new(out);
        match self {
            ExtendedMessage::Dyn(msg_id, v) => {
                out.write_u8(*msg_id)?;
                bencode_serialize_to_writer(v, &mut out)?;
            }
            ExtendedMessage::Handshake(h) => {
                out.write_u8(0)?;
                bencode_serialize_to_writer(h, &mut out)?;
            }
            ExtendedMessage::UtMetadata(u) => {
                let emsg_id = peer_extended_msg_ids()
                    .ut_metadata
                    .ok_or(SerializeError::NeedUtMetadata)?;
                out.write_u8(emsg_id)?;
                u.serialize(&mut out)?;
            }
            ExtendedMessage::UtPex(m) => {
                let emsg_id = peer_extended_msg_ids()
                    .ut_pex
                    .ok_or(SerializeError::NeedPex)?;
                out.write_u8(emsg_id)?;
                bencode_serialize_to_writer(m, &mut out)?;
            }
            ExtendedMessage::UtHolepunch(m) => {
                let emsg_id = peer_extended_msg_ids()
                    .ut_holepunch
                    .ok_or(SerializeError::NeedHolepunch)?;
                out.write_u8(emsg_id)?;
                m.serialize(&mut out)?;
            }
            ExtendedMessage::UploadOnly(enabled) => {
                let emsg_id = peer_extended_msg_ids()
                    .upload_only
                    .ok_or(SerializeError::NeedUploadOnly)?;
                out.write_u8(emsg_id)?;
                out.write_u8(u8::from(*enabled))?;
            }
            ExtendedMessage::DontHave(index) => {
                let emsg_id = peer_extended_msg_ids()
                    .lt_donthave
                    .ok_or(SerializeError::NeedDontHave)?;
                out.write_u8(emsg_id)?;
                out.write_u32::<BE>(*index)?;
            }
        }
        Ok(usize::try_from(out.position()).expect("the cursor stays within its buffer"))
    }

    /// The extended message whose payload, whole, is `buf`. A payload with
    /// no extended message id is not one.
    pub fn deserialize(mut buf: DoubleBufHelper<'a>) -> Result<Self, MessageDeserializeError> {
        let msg_id = crate::peer_protocol::MsgIdDebug(MSGID_EXTENDED);
        let emsg_id = buf
            .read_u8()
            .ok_or(MessageDeserializeError::IncorrectMsgLen {
                received: 0,
                expected: 1,
                msg_id,
            })?;

        fn from_bytes_contig<'a, T>(buf: &DoubleBufHelper<'a>) -> Result<T, MessageDeserializeError>
        where
            T: serde::de::Deserialize<'a>,
        {
            let buf = buf
                .get_contiguous(buf.len())
                .ok_or(MessageDeserializeError::NeedContiguous)?;
            crate::bencode::from_bytes(buf).map_err(|e| {
                tracing::trace!("error deserializing extended: {e:#}");
                MessageDeserializeError::Bencode(e.into_kind())
            })
        }

        match emsg_id {
            0 => Ok(ExtendedMessage::Handshake(from_bytes_contig(&buf)?)),
            MY_EXTENDED_UT_METADATA => {
                Ok(ExtendedMessage::UtMetadata(UtMetadata::deserialize(buf)?))
            }
            MY_EXTENDED_UT_PEX => Ok(ExtendedMessage::UtPex(from_bytes_contig(&buf)?)),
            MY_EXTENDED_UT_HOLEPUNCH => {
                Ok(ExtendedMessage::UtHolepunch(UtHolepunch::deserialize(buf)?))
            }
            // One byte, nonzero when the peer only uploads (libtorrent
            // `bt_peer_connection::on_extended`, `upload_only_msg`).
            MY_EXTENDED_UPLOAD_ONLY => match buf.get_contiguous(buf.len()) {
                Some(&[byte]) => Ok(ExtendedMessage::UploadOnly(byte != 0)),
                _ => Err(MessageDeserializeError::IncorrectMsgLen {
                    received: u32::try_from(buf.len())
                        .unwrap_or(u32::MAX)
                        .saturating_add(1),
                    expected: 2,
                    msg_id,
                }),
            },
            // A piece index, four bytes (libtorrent
            // `bt_peer_connection::on_extended`, `dont_have_msg`).
            MY_EXTENDED_LT_DONTHAVE => match buf.get_contiguous(buf.len()) {
                Some(&[a, b, c, d]) => {
                    Ok(ExtendedMessage::DontHave(u32::from_be_bytes([a, b, c, d])))
                }
                _ => Err(MessageDeserializeError::IncorrectMsgLen {
                    received: u32::try_from(buf.len())
                        .unwrap_or(u32::MAX)
                        .saturating_add(1),
                    expected: 5,
                    msg_id,
                }),
            },
            _ => Ok(ExtendedMessage::Dyn(emsg_id, from_bytes_contig(&buf)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffers::ByteBuf;

    use crate::peer_protocol::{
        DoubleBufHelper, MessageDeserializeError, SerializeError,
        extended::{
            ExtendedMessage, PeerExtendedMessageIds,
            handshake::ExtendedHandshake,
            ut_metadata::{UtMetadata, UtMetadataData},
        },
    };

    #[track_caller]
    fn ut_metadata_trailing_bytes_is_error(msg: ExtendedMessage<ByteBuf>) {
        let mut buf = [0u8; 100];
        let sz = msg
            .serialize(&mut buf, &|| PeerExtendedMessageIds::my())
            .unwrap();

        let deserialized =
            ExtendedMessage::deserialize(DoubleBufHelper::new(&buf[..sz], &[])).unwrap();
        assert_eq!(msg, deserialized);

        let res = ExtendedMessage::deserialize(DoubleBufHelper::new(&buf[..sz + 1], &[]));
        assert!(
            matches!(
                res,
                Err(MessageDeserializeError::UtMetadataTrailingBytes
                    | MessageDeserializeError::UtMetadataSizeMismatch {
                        expected_size: 5,
                        received_size: 6
                    })
            ),
            "expected trailing bytes error, got {res:?}"
        )
    }

    #[test]
    fn test_ut_metadata_trailing_bytes_is_error() {
        ut_metadata_trailing_bytes_is_error(ExtendedMessage::UtMetadata(UtMetadata::Request(42)));
        ut_metadata_trailing_bytes_is_error(ExtendedMessage::UtMetadata(UtMetadata::Reject(43)));
        ut_metadata_trailing_bytes_is_error(ExtendedMessage::UtMetadata(UtMetadata::Data(
            UtMetadataData::from_bytes(0, 5, b"\x42\x42\x42\x42\x42"[..].into()),
        )));
    }

    #[test]
    fn test_handshake_without_m_dictionary_is_accepted() {
        // Some clients omit the "m" dictionary on private torrents (no PEX,
        // no metadata transfer). This serializer always emits "m", so the
        // wire bytes are handwritten.
        const PAYLOAD: &[u8] = b"\x00d1:pi100e4:reqqi250ee";

        let msg = ExtendedMessage::deserialize(DoubleBufHelper::new(PAYLOAD, &[])).unwrap();
        let hs = match msg {
            ExtendedMessage::Handshake(hs) => hs,
            _ => panic!("expected handshake"),
        };
        // Both ids default to None: nothing to send over ut_metadata or ut_pex.
        assert_eq!(hs.m, PeerExtendedMessageIds::default());
        assert_eq!(hs.p, Some(100));
        assert_eq!(hs.reqq, Some(250));
    }

    #[test]
    fn test_handshake_without_m_dictionary_deserializes_non_contiguous() {
        // Same handshake, split at every point. The bencode body requires a
        // contiguous buffer, so interior splits fail with NeedContiguous.
        const PAYLOAD: &[u8] = b"\x00d1:pi100e4:reqqi250ee";
        for split_point in 0..PAYLOAD.len() {
            let res = ExtendedMessage::deserialize(DoubleBufHelper::new(
                &PAYLOAD[..split_point],
                &PAYLOAD[split_point..],
            ));
            if (2..PAYLOAD.len()).contains(&split_point) {
                assert!(
                    matches!(res, Err(MessageDeserializeError::NeedContiguous)),
                    "expected NeedContiguous: {split_point}"
                );
                continue;
            }
            let msg = res.unwrap();
            assert!(matches!(msg, ExtendedMessage::Handshake(..)));
        }
    }

    #[test]
    fn test_handshake_roundtrip() {
        // "m" must survive serialization. If it were dropped, the ids would
        // come back as default (all None) and the equality would fail.
        let msg = ExtendedMessage::Handshake(ExtendedHandshake::<ByteBuf>::new());
        let mut buf = [0u8; 100];
        let sz = msg
            .serialize(&mut buf, &|| PeerExtendedMessageIds::my())
            .unwrap();
        let de = ExtendedMessage::deserialize(DoubleBufHelper::new(&buf[..sz], &[])).unwrap();
        assert_eq!(msg, de);
    }

    // libtorrent `bt_peer_connection::write_upload_only`: the extended id
    // the peer named for upload_only, then 1 or 0; nothing to a peer that
    // named none.
    #[test]
    fn upload_only_is_one_byte_under_the_peers_id() {
        let mut buf = [0u8; 8];
        let ids = || PeerExtendedMessageIds {
            upload_only: Some(7),
            ..Default::default()
        };
        for (enabled, byte) in [(true, 1), (false, 0)] {
            let sz = ExtendedMessage::<ByteBuf>::UploadOnly(enabled)
                .serialize(&mut buf, &ids)
                .unwrap();
            assert_eq!(buf[..sz], [7, byte]);
        }
        let res = ExtendedMessage::<ByteBuf>::UploadOnly(true)
            .serialize(&mut buf, &PeerExtendedMessageIds::default);
        assert!(matches!(res, Err(SerializeError::NeedUploadOnly)));
    }

    // libtorrent `bt_peer_connection::on_extended`: under the id we named,
    // upload_only is one byte, nonzero when the peer only uploads; a
    // message of another size is not one.
    #[test]
    fn upload_only_is_read_under_our_id() {
        let mut buf = [0u8; 8];
        for enabled in [true, false] {
            let msg = ExtendedMessage::<ByteBuf>::UploadOnly(enabled);
            let sz = msg
                .serialize(&mut buf, &PeerExtendedMessageIds::my)
                .unwrap();
            let de = ExtendedMessage::deserialize(DoubleBufHelper::new(&buf[..sz], &[])).unwrap();
            assert_eq!(de, msg);
        }
        assert_eq!(
            ExtendedMessage::deserialize(DoubleBufHelper::new(&[2, 7], &[])).unwrap(),
            ExtendedMessage::UploadOnly(true)
        );
        for bad in [&[2][..], &[2, 1, 1]] {
            assert!(matches!(
                ExtendedMessage::deserialize(DoubleBufHelper::new(bad, &[])),
                Err(MessageDeserializeError::IncorrectMsgLen { .. })
            ));
        }
    }

    // libtorrent `bt_peer_connection::write_dont_have` and `on_extended`:
    // the id the peer named for lt_donthave, then the piece index in four
    // bytes; read under our id, a message of another size is not one.
    #[test]
    fn dont_have_is_a_piece_index_under_the_peers_id() {
        let mut buf = [0u8; 8];
        let ids = || PeerExtendedMessageIds {
            lt_donthave: Some(9),
            ..Default::default()
        };
        let sz = ExtendedMessage::<ByteBuf>::DontHave(0x0102_0304)
            .serialize(&mut buf, &ids)
            .unwrap();
        assert_eq!(buf[..sz], [9, 1, 2, 3, 4]);
        let res = ExtendedMessage::<ByteBuf>::DontHave(1)
            .serialize(&mut buf, &PeerExtendedMessageIds::default);
        assert!(matches!(res, Err(SerializeError::NeedDontHave)));

        let msg = ExtendedMessage::<ByteBuf>::DontHave(0x0102_0304);
        let sz = msg
            .serialize(&mut buf, &PeerExtendedMessageIds::my)
            .unwrap();
        let de = ExtendedMessage::deserialize(DoubleBufHelper::new(&buf[..sz], &[])).unwrap();
        assert_eq!(de, msg);
        for bad in [&[7, 1, 2, 3][..], &[7, 1, 2, 3, 4, 5]] {
            assert!(matches!(
                ExtendedMessage::deserialize(DoubleBufHelper::new(bad, &[])),
                Err(MessageDeserializeError::IncorrectMsgLen { .. })
            ));
        }
    }

    // BEP 10: a first handshake's id of 0 names no support; a later one
    // replaces the ids it names, takes away those it names 0, and leaves
    // the rest.
    #[test]
    fn a_handshake_updates_only_the_ids_it_names_and_zero_names_none() {
        let first = PeerExtendedMessageIds::default().updated_by(PeerExtendedMessageIds {
            ut_metadata: Some(0),
            ut_pex: Some(1),
            ut_holepunch: Some(0),
            upload_only: Some(3),
            lt_donthave: Some(7),
        });
        assert_eq!(
            first,
            PeerExtendedMessageIds {
                ut_pex: Some(1),
                upload_only: Some(3),
                lt_donthave: Some(7),
                ..Default::default()
            }
        );
        let later = first.updated_by(PeerExtendedMessageIds {
            ut_metadata: Some(2),
            ut_pex: Some(9),
            lt_donthave: Some(0),
            ..Default::default()
        });
        assert_eq!(
            later,
            PeerExtendedMessageIds {
                ut_metadata: Some(2),
                ut_pex: Some(9),
                upload_only: Some(3),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_ut_metadata_non_contiguous() {
        let mut buf = [0u8; 100];
        let msg = ExtendedMessage::UtMetadata(UtMetadata::Data(UtMetadataData::from_bytes(
            0,
            5,
            b"\x42\x42\x42\x42\x42"[..].into(),
        )));
        let sz = msg
            .serialize(&mut buf, &|| PeerExtendedMessageIds::my())
            .unwrap();
        let bencode_sz = buf[..sz].iter().position(|byte| *byte == 0x42).unwrap();

        for split_point in 0..sz {
            let (d0, d1) = buf[..sz].split_at(split_point);
            let buf = DoubleBufHelper::new(d0, d1);
            let res = ExtendedMessage::deserialize(buf);
            if (2..bencode_sz).contains(&split_point) {
                assert!(
                    matches!(res, Err(MessageDeserializeError::NeedContiguous)),
                    "expected NeedContiguous, got {res:?}, split_point={split_point}, bencode_sz={bencode_sz}"
                );
                continue;
            }
            let de = res.unwrap();
            match de {
                ExtendedMessage::UtMetadata(UtMetadata::Data(d)) => {
                    assert_eq!(d.piece(), 0);
                    assert_eq!(d.len(), 5);
                    let mut debuf = [0u8; 5];
                    d.copy_to_slice(&mut debuf);
                    assert_eq!(debuf, b"\x42\x42\x42\x42\x42"[..]);
                }
                _ => panic!("bad msg"),
            }
        }
    }
}
