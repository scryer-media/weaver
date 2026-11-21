// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::io::Cursor;

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
// `bt_peer_connection::write_handshake` lists the loaded plugins only). An
// id that is not one, an integer past a byte or of another type, is taken as
// not named.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct PeerExtendedMessageIds {
    #[serde(
        default,
        deserialize_with = "handshake::lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub ut_metadata: Option<u8>,
    #[serde(
        default,
        deserialize_with = "handshake::lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub ut_pex: Option<u8>,
    #[serde(
        default,
        deserialize_with = "handshake::lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub ut_holepunch: Option<u8>,
    #[serde(
        default,
        deserialize_with = "handshake::lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub upload_only: Option<u8>,
    #[serde(
        default,
        deserialize_with = "handshake::lenient",
        skip_serializing_if = "Option::is_none"
    )]
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

    /// Every extension we advertise to some peer, each under our id.
    pub fn ours() -> Self {
        Self {
            ut_holepunch: Some(MY_EXTENDED_UT_HOLEPUNCH),
            ..Self::my()
        }
    }

    /// Whether one of these extensions goes by `id`.
    pub fn names(&self, id: u8) -> bool {
        [
            self.ut_metadata,
            self.ut_pex,
            self.ut_holepunch,
            self.upload_only,
            self.lt_donthave,
        ]
        .contains(&Some(id))
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
    /// A message under our id for upload_only or lt_donthave whose size is
    /// not that message's: it says nothing, and is ignored (libtorrent
    /// `bt_peer_connection::on_extended` logs it and reads on).
    WrongSize(u8),
    /// A message under the id of one of our extensions that we did not
    /// advertise on this connection: it is ignored, unread, as libtorrent
    /// ignores PEX on a torrent without it (ut_pex.cpp:270, v2.0.15).
    NotAdvertised(u8),
}

impl<'a> ExtendedMessage<ByteBuf<'a>> {
    pub fn serialize(
        &self,
        out: &mut [u8],
        peer_extended_msg_ids: &dyn Fn() -> PeerExtendedMessageIds,
    ) -> Result<usize, SerializeError> {
        let mut out = Cursor::new(out);
        match self {
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
            // The id alone, which reads back as of the wrong size.
            ExtendedMessage::WrongSize(msg_id) | ExtendedMessage::NotAdvertised(msg_id) => {
                out.write_u8(*msg_id)?
            }
        }
        Ok(usize::try_from(out.position()).expect("the cursor stays within its buffer"))
    }

    /// The extended message whose payload, whole, is `buf`, on a connection
    /// we advertised every one of our extensions on.
    pub fn deserialize(buf: DoubleBufHelper<'a>) -> Result<Self, MessageDeserializeError> {
        Self::deserialize_advertised(buf, &PeerExtendedMessageIds::ours())
    }

    /// The extended message whose payload, whole, is `buf`, on a connection
    /// we advertised `advertised` on. A payload with no extended message id
    /// is not one. The id decides before a byte of the rest is read: one of
    /// ours not advertised here is ignored, and one that is none of ours
    /// ends the connection (libtorrent `bt_peer_connection::on_extended`,
    /// bt_peer_connection.cpp:1980-1988, v2.0.15, when no extension takes it).
    pub fn deserialize_advertised(
        mut buf: DoubleBufHelper<'a>,
        advertised: &PeerExtendedMessageIds,
    ) -> Result<Self, MessageDeserializeError> {
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

        if emsg_id != 0 && !advertised.names(emsg_id) {
            return match PeerExtendedMessageIds::ours().names(emsg_id) {
                true => Ok(ExtendedMessage::NotAdvertised(emsg_id)),
                false => Err(MessageDeserializeError::UnknownExtendedId(emsg_id)),
            };
        }
        match emsg_id {
            0 => Ok(ExtendedMessage::Handshake(from_bytes_contig(&buf)?)),
            MY_EXTENDED_UT_METADATA => {
                Ok(ExtendedMessage::UtMetadata(UtMetadata::deserialize(buf)?))
            }
            // Its length is judged before it is read (libtorrent
            // `ut_pex_peer_plugin::on_extended`, ut_pex.cpp:272-276, v2.0.15).
            MY_EXTENDED_UT_PEX if buf.len() > ut_pex::MAX_PEX_MESSAGE_LEN => {
                Err(MessageDeserializeError::PexTooLarge(buf.len()))
            }
            MY_EXTENDED_UT_PEX => Ok(ExtendedMessage::UtPex(from_bytes_contig(&buf)?)),
            MY_EXTENDED_UT_HOLEPUNCH => {
                Ok(ExtendedMessage::UtHolepunch(UtHolepunch::deserialize(buf)?))
            }
            // One byte, nonzero when the peer only uploads; one of another
            // size is ignored (libtorrent `bt_peer_connection::on_extended`,
            // `upload_only_msg`, a packet of 3 bytes).
            MY_EXTENDED_UPLOAD_ONLY => match buf.len() {
                1 => Ok(ExtendedMessage::UploadOnly(buf.read_u8().unwrap() != 0)),
                _ => Ok(ExtendedMessage::WrongSize(emsg_id)),
            },
            // A piece index, four bytes; one of another size is ignored
            // (libtorrent `bt_peer_connection::on_extended`, `dont_have_msg`,
            // a packet of 6 bytes).
            MY_EXTENDED_LT_DONTHAVE => match buf.consume::<4>() {
                Ok(index) if buf.is_empty() => {
                    Ok(ExtendedMessage::DontHave(u32::from_be_bytes(index)))
                }
                _ => Ok(ExtendedMessage::WrongSize(emsg_id)),
            },
            _ => Err(MessageDeserializeError::UnknownExtendedId(emsg_id)),
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

    // libtorrent `bt_peer_connection::on_extended_handshake` reads each field
    // on its own and skips one of the wrong type or length: the handshake
    // still counts, with that field absent. One that is not a dictionary is
    // refused.
    #[test]
    fn a_malformed_optional_handshake_field_is_taken_as_absent() {
        // `yourip` of 6 bytes, `reqq` a string, `metadata_size` a list,
        // `upload_only` past u32, `ut_pex` a string and `ut_holepunch` past
        // a byte; `p`, `v` and `ut_metadata` as they should be.
        const PAYLOAD: &[u8] = b"\x00d1:md12:ut_holepunchi300e11:ut_metadatai3e6:ut_pex1:xe\
            13:metadata_sizeli1ee1:pi6881e4:reqq3:abc11:upload_onlyi99999999999e\
            1:v4:test6:yourip6:abcdefe";
        let hs = match ExtendedMessage::deserialize(DoubleBufHelper::new(PAYLOAD, &[])).unwrap() {
            ExtendedMessage::Handshake(hs) => hs,
            other => panic!("expected handshake, got {other:?}"),
        };
        assert_eq!(
            hs.m,
            PeerExtendedMessageIds {
                ut_metadata: Some(3),
                ..Default::default()
            }
        );
        assert_eq!(hs.p, Some(6881));
        assert_eq!(hs.v, Some(ByteBuf(b"test")));
        assert_eq!(hs.yourip, None);
        assert_eq!(hs.reqq, None);
        assert_eq!(hs.metadata_size, None);
        assert_eq!(hs.upload_only, None);

        // `m` that is not a dictionary names no ids.
        const NOT_A_DICT_M: &[u8] = b"\x00d1:mi1e1:pi1ee";
        match ExtendedMessage::deserialize(DoubleBufHelper::new(NOT_A_DICT_M, &[])).unwrap() {
            ExtendedMessage::Handshake(hs) => {
                assert_eq!(hs.m, PeerExtendedMessageIds::default());
                assert_eq!(hs.p, Some(1));
            }
            other => panic!("expected handshake, got {other:?}"),
        }

        for not_a_dict in [&b"\x00i1e"[..], b"\x00l1:pe", b"\x004:spam"] {
            assert!(matches!(
                ExtendedMessage::deserialize(DoubleBufHelper::new(not_a_dict, &[])),
                Err(MessageDeserializeError::Bencode(..))
            ));
        }
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
        // libtorrent `bt_peer_connection::on_extended` ignores one of
        // another size, and the connection stays.
        for bad in [&[2][..], &[2, 1, 1]] {
            assert_eq!(
                ExtendedMessage::deserialize(DoubleBufHelper::new(bad, &[])).unwrap(),
                ExtendedMessage::WrongSize(2)
            );
        }
    }

    // libtorrent `bt_peer_connection::write_dont_have` and `on_extended`:
    // the id the peer named for lt_donthave, then the piece index in four
    // bytes; read under our id, a message of another size is not one, and
    // is ignored.
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
        // One of another size is ignored, split or not.
        for bad in [&[7, 1, 2, 3][..], &[7, 1, 2, 3, 4, 5], &[7]] {
            assert_eq!(
                ExtendedMessage::deserialize(DoubleBufHelper::new(bad, &[])).unwrap(),
                ExtendedMessage::WrongSize(7)
            );
        }
        assert_eq!(
            ExtendedMessage::deserialize(DoubleBufHelper::new(&[7, 1, 2], &[3, 4])).unwrap(),
            ExtendedMessage::DontHave(0x0102_0304)
        );
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

    // A frame carrying the extended message `id` with `payload`.
    fn extended_frame(id: u8, payload: &[u8]) -> Vec<u8> {
        let len = u32::try_from(2 + payload.len()).unwrap();
        let mut frame = len.to_be_bytes().to_vec();
        frame.extend_from_slice(&[crate::peer_protocol::MSGID_EXTENDED, id]);
        frame.extend_from_slice(payload);
        frame
    }

    // libtorrent bt_peer_connection.cpp:1980-1988, v2.0.15: an id no
    // extension of ours takes ends the connection, its payload unread.
    #[test]
    fn an_id_that_is_none_of_ours_is_refused_unread() {
        use crate::peer_protocol::Message;
        let ours = PeerExtendedMessageIds::ours();
        let unknown = (1..=u8::MAX).filter(|id| !ours.names(*id));
        for id in unknown {
            // A well-formed dictionary, and bytes that are no bencode.
            for payload in [&b"d1:xi1ee"[..], b"\xff\xff"] {
                let frame = extended_frame(id, payload);
                assert!(
                    matches!(
                        Message::deserialize(&frame, &[], None),
                        Err(MessageDeserializeError::UnknownExtendedId(got)) if got == id
                    ),
                    "id {id}"
                );
            }
        }
    }

    // Every id we advertise is read as its extension; one of ours this
    // connection was not told is ignored unread, as libtorrent ignores PEX
    // on a torrent without it (ut_pex.cpp:270, v2.0.15).
    #[test]
    fn an_id_of_ours_is_read_only_where_we_advertised_it() {
        use crate::peer_protocol::{MY_EXTENDED_UT_METADATA, MY_EXTENDED_UT_PEX, Message};
        fn read<'a>(
            frame: &'a [u8],
            advertised: &PeerExtendedMessageIds,
        ) -> Result<ExtendedMessage<ByteBuf<'a>>, MessageDeserializeError> {
            Message::deserialize_advertised(frame, &[], None, advertised).map(|(m, _)| match m {
                Message::Extended(e) => e,
                other => panic!("{other:?}"),
            })
        }
        let pex = extended_frame(MY_EXTENDED_UT_PEX, b"d5:added6:\x0a\x00\x00\x01\x1a\xe1e");
        let all = PeerExtendedMessageIds::ours();
        assert!(matches!(read(&pex, &all), Ok(ExtendedMessage::UtPex(_))));
        let without_pex = PeerExtendedMessageIds {
            ut_pex: None,
            ..all
        };
        assert!(matches!(
            read(&pex, &without_pex),
            Ok(ExtendedMessage::NotAdvertised(MY_EXTENDED_UT_PEX))
        ));
        // Unread: neither its bytes nor its length are judged.
        for payload in [
            &b"\xff"[..],
            &vec![b'x'; super::ut_pex::MAX_PEX_MESSAGE_LEN + 1],
        ] {
            assert!(matches!(
                read(&extended_frame(MY_EXTENDED_UT_PEX, payload), &without_pex),
                Ok(ExtendedMessage::NotAdvertised(MY_EXTENDED_UT_PEX))
            ));
        }
        // The handshake is read on every connection.
        let handshake = extended_frame(0, b"d1:md6:ut_pexi1eee");
        let none = PeerExtendedMessageIds::default();
        assert!(matches!(
            read(&handshake, &none),
            Ok(ExtendedMessage::Handshake(_))
        ));
        let metadata = extended_frame(MY_EXTENDED_UT_METADATA, b"d8:msg_typei0e5:piecei0ee");
        assert!(matches!(
            read(&metadata, &none),
            Ok(ExtendedMessage::NotAdvertised(MY_EXTENDED_UT_METADATA))
        ));
        assert!(matches!(
            read(&metadata, &all),
            Ok(ExtendedMessage::UtMetadata(UtMetadata::Request(0)))
        ));
    }

    // A PEX payload of `len` bytes: a dictionary whose one key, unknown,
    // holds a string that pads it out.
    fn padded_pex(len: usize) -> Vec<u8> {
        let digits = (1..10)
            .find(|d| (len - 6 - d).to_string().len() == *d)
            .unwrap();
        let n = len - 6 - digits;
        let mut payload = format!("d1:x{n}:").into_bytes();
        payload.extend(std::iter::repeat_n(b'a', n));
        payload.push(b'e');
        assert_eq!(payload.len(), len);
        payload
    }

    // libtorrent `ut_pex_peer_plugin::on_extended`, ut_pex.cpp:272-276,
    // v2.0.15: a PEX message is judged by its length before it is decoded,
    // whatever keys it holds; a short one with keys we do not know is read.
    #[test]
    fn a_pex_message_is_judged_by_its_length_before_it_is_read() {
        use crate::peer_protocol::{MY_EXTENDED_UT_PEX, Message};
        // The peers a PEX message adds, or why it was refused.
        let read = |payload: &[u8]| {
            let frame = extended_frame(MY_EXTENDED_UT_PEX, payload);
            Message::deserialize(&frame, &[], None).map(|(m, _)| match m {
                Message::Extended(ExtendedMessage::UtPex(pex)) => pex.added_peers().count(),
                other => panic!("{other:?}"),
            })
        };
        let limit = super::ut_pex::MAX_PEX_MESSAGE_LEN;
        assert!(matches!(read(&padded_pex(limit)), Ok(0)));
        assert!(matches!(
            read(&padded_pex(limit + 1)),
            Err(MessageDeserializeError::PexTooLarge(len)) if len == limit + 1
        ));
        // Near the longest frame, of unknown keys only.
        let frame_limit = crate::peer_protocol::MAX_PACKET_LEN - 2;
        assert!(matches!(
            read(&padded_pex(frame_limit)),
            Err(MessageDeserializeError::PexTooLarge(_))
        ));
        // Small, with keys we do not know beside the peers.
        assert!(matches!(
            read(b"d5:added6:\x0a\x00\x00\x01\x1a\xe17:unknowni1e1:x3:abce"),
            Ok(1)
        ));
    }
}
