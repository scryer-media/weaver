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
use byteorder::WriteBytesExt;
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
}

impl PeerExtendedMessageIds {
    pub fn my() -> Self {
        Self {
            ut_metadata: Some(MY_EXTENDED_UT_METADATA),
            ut_pex: Some(MY_EXTENDED_UT_PEX),
            ut_holepunch: None,
            upload_only: Some(MY_EXTENDED_UPLOAD_ONLY),
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
        }
        Ok(out.position() as usize)
    }

    pub fn deserialize(mut buf: DoubleBufHelper<'a>) -> Result<Self, MessageDeserializeError> {
        let msg_id = crate::peer_protocol::MsgIdDebug(MSGID_EXTENDED);
        let emsg_id = buf
            .read_u8()
            .ok_or(MessageDeserializeError::NotEnoughData(1, Some(msg_id)))?;

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
                    received: buf.len() as u32 + 1,
                    expected: 2,
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
