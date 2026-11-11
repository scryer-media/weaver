// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::bencode::BencodeDeserializer;
use crate::bencode::bencode_serialize_to_writer;
use crate::buffers::ByteBuf;
use crate::buffers::ByteBufOwned;
use crate::buffers::ByteBufT;
use crate::core::constants::CHUNK_SIZE;
use bytes::Bytes;
use serde::Deserialize;
use serde_derive::Serialize;
use std::io::Cursor;
use std::io::Write;

use crate::peer_protocol::DoubleBufHelper;
use crate::peer_protocol::MessageDeserializeError;
use crate::peer_protocol::SerializeError;

#[derive(Debug, Eq, PartialEq)]
pub struct UtMetadataData<ByteBuf> {
    piece: u32,
    total_size: u32,
    data_0: ByteBuf,
    data_1: ByteBuf,
}

impl<ByteBuf: ByteBufT> UtMetadataData<ByteBuf> {
    pub fn from_bytes(piece: u32, total_size: u32, data: ByteBuf) -> Self {
        Self {
            piece,
            total_size,
            data_0: data,
            data_1: Default::default(),
        }
    }
}

impl UtMetadataData<ByteBufOwned> {
    /// The data message answering a peer's request for piece `piece` of
    /// `metadata`. A piece index names one of the 16 KiB pieces of the
    /// metadata, so an index at or past the piece count is out of bounds.
    /// Metadata too long for the protocol's u32 `total_size` has no pieces to
    /// offer, the same rule the extended handshake applies to `metadata_size`.
    pub fn piece_of(metadata: &Bytes, piece: u32) -> Result<Self, MessageDeserializeError> {
        let total_size = u32::try_from(metadata.len()).unwrap_or(0);
        let total_pieces = total_size.div_ceil(CHUNK_SIZE);
        if piece >= total_pieces {
            return Err(MessageDeserializeError::UtMetadataPieceOutOfBounds {
                total_pieces,
                received_piece: piece,
            });
        }
        let start = piece as usize * CHUNK_SIZE as usize;
        let end = metadata.len().min(start + CHUNK_SIZE as usize);
        Ok(Self::from_bytes(
            piece,
            total_size,
            ByteBufOwned(metadata.slice(start..end)),
        ))
    }
}

impl<'a> UtMetadataData<ByteBuf<'a>> {
    pub fn piece(&self) -> u32 {
        self.piece
    }

    /// The size of the whole metadata, as the sender gives it.
    pub fn total_size(&self) -> u32 {
        self.total_size
    }

    pub fn len(&self) -> usize {
        self.data_0.as_ref().len() + self.data_1.as_ref().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data_0.as_ref().is_empty() && self.data_1.as_ref().is_empty()
    }

    pub fn as_double_buf(&self) -> DoubleBufHelper<'a> {
        DoubleBufHelper::new(self.data_0.0, self.data_1.0)
    }

    pub fn copy_to_slice(&self, mut out: &mut [u8]) {
        let d0 = self.data_0.as_ref();
        let d1 = self.data_1.as_ref();
        out[..d0.len()].copy_from_slice(d0);
        out = &mut out[d0.len()..];
        out[..d1.len()].copy_from_slice(d1);
    }

    fn validate(&self) -> Result<(), MessageDeserializeError> {
        if self.total_size == 0 {
            return Err(MessageDeserializeError::UtMetadataMissingTotalSize);
        }
        if self.len() > CHUNK_SIZE as usize {
            return Err(MessageDeserializeError::UtMetadataTooLarge(
                u32::try_from(self.len()).unwrap_or(u32::MAX),
            ));
        }
        let total_pieces = self.total_size.div_ceil(CHUNK_SIZE);
        if self.piece >= total_pieces {
            return Err(MessageDeserializeError::UtMetadataPieceOutOfBounds {
                total_pieces,
                received_piece: self.piece,
            });
        }
        let expected_size = self
            .total_size
            .saturating_sub(self.piece * CHUNK_SIZE)
            .min(CHUNK_SIZE);
        if u32::try_from(self.len()) != Ok(expected_size) {
            return Err(MessageDeserializeError::UtMetadataSizeMismatch {
                expected_size,
                received_size: u32::try_from(self.len()).unwrap_or(u32::MAX),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum UtMetadata<ByteBuf> {
    Request(u32),
    Data(UtMetadataData<ByteBuf>),
    Reject(u32),
}

impl UtMetadata<ByteBufOwned> {
    pub fn as_borrowed(&self) -> UtMetadata<ByteBuf<'_>> {
        match self {
            UtMetadata::Request(req) => UtMetadata::Request(*req),
            UtMetadata::Data(d) => UtMetadata::Data(UtMetadataData {
                piece: d.piece,
                data_0: ByteBuf::from(d.data_0.as_ref()),
                data_1: ByteBuf::from(d.data_1.as_ref()),
                total_size: d.total_size,
            }),
            UtMetadata::Reject(r) => UtMetadata::Reject(*r),
        }
    }
}

impl<'a> UtMetadata<ByteBuf<'a>> {
    pub fn serialize(&self, writer: &mut Cursor<&mut [u8]>) -> Result<(), SerializeError> {
        #[derive(Serialize)]
        struct Message {
            msg_type: u32,
            piece: u32,
            #[serde(skip_serializing_if = "Option::is_none")]
            total_size: Option<u32>,
        }
        match self {
            UtMetadata::Request(piece) => {
                let message = Message {
                    msg_type: 0,
                    piece: *piece,
                    total_size: None,
                };
                bencode_serialize_to_writer(message, writer)?
            }
            UtMetadata::Data(d) => {
                let message = Message {
                    msg_type: 1,
                    piece: d.piece,
                    total_size: Some(d.total_size),
                };
                bencode_serialize_to_writer(message, writer)?;
                writer.write_all(d.data_0.as_ref())?;
                writer.write_all(d.data_1.as_ref())?;
            }
            UtMetadata::Reject(piece) => {
                let message = Message {
                    msg_type: 2,
                    piece: *piece,
                    total_size: None,
                };
                bencode_serialize_to_writer(message, writer)?;
            }
        }
        Ok(())
    }

    pub fn deserialize(mut buf: DoubleBufHelper<'a>) -> Result<Self, MessageDeserializeError> {
        #[derive(serde_derive::Deserialize)]
        struct UtMetadataMsg {
            msg_type: u32,
            piece: u32,
            total_size: Option<u32>,
        }

        const MAX_BMSG_SIZE: usize =
            b"d8:msg_typei10e5:piecei4294967296e10:total_sizei16384ee".len();
        let (contig, is_contig) = match buf.get_contiguous(MAX_BMSG_SIZE.min(buf.len())) {
            Some(c) => (c, true),
            None => (buf.get()[0], false),
        };

        let mut de = BencodeDeserializer::new_from_buf(contig);
        let message = match UtMetadataMsg::deserialize(&mut de) {
            Ok(message) => {
                let consumed = contig.len() - de.into_remaining().len();
                buf.advance(consumed);
                message
            }
            Err(e) => {
                if is_contig {
                    return Err(MessageDeserializeError::Bencode(e));
                }
                return Err(MessageDeserializeError::NeedContiguous);
            }
        };

        match message.msg_type {
            // request
            0 => {
                if !buf.is_empty() {
                    return Err(MessageDeserializeError::UtMetadataTrailingBytes);
                }
                Ok(UtMetadata::Request(message.piece))
            }
            // data
            1 => {
                let total_size = message
                    .total_size
                    .ok_or(MessageDeserializeError::UtMetadataMissingTotalSize)?;

                let [data_0, data_1] = buf.get();
                let d = UtMetadataData {
                    piece: message.piece,
                    data_0: data_0.into(),
                    data_1: data_1.into(),
                    total_size,
                };
                d.validate()?;
                Ok(UtMetadata::Data(d))
            }
            // reject
            2 => {
                if !buf.is_empty() {
                    return Err(MessageDeserializeError::UtMetadataTrailingBytes);
                }
                Ok(UtMetadata::Reject(message.piece))
            }
            other => Err(MessageDeserializeError::UtMetadataTypeUnknown(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffers::ByteBuf;
    use crate::core::constants::CHUNK_SIZE;
    use bytes::Bytes;

    use crate::peer_protocol::{MessageDeserializeError, extended::ut_metadata::UtMetadataData};

    #[test]
    fn test_ut_metadata_validate() {
        // success
        UtMetadataData::from_bytes(0, 3, ByteBuf(&b"foo"[..]))
            .validate()
            .unwrap();

        // 0 size
        let err = UtMetadataData::from_bytes(0, 0, ByteBuf(&[]))
            .validate()
            .unwrap_err();
        assert!(
            matches!(err, MessageDeserializeError::UtMetadataMissingTotalSize),
            "{:?}",
            err
        );

        // 0 provided size
        let err = UtMetadataData::from_bytes(0, 0, ByteBuf(&b"foo"[..]))
            .validate()
            .unwrap_err();
        assert!(
            matches!(err, MessageDeserializeError::UtMetadataMissingTotalSize),
            "{:?}",
            err
        );

        // piece out of bounds
        let err = UtMetadataData::from_bytes(1, 3, ByteBuf(&b"foo"[..]))
            .validate()
            .unwrap_err();
        assert!(
            matches!(
                err,
                MessageDeserializeError::UtMetadataPieceOutOfBounds {
                    total_pieces: 1,
                    received_piece: 1
                }
            ),
            "{:?}",
            err
        );

        // piece out of bounds
        let err = UtMetadataData::from_bytes(0, 3, ByteBuf(&b"foobar"[..]))
            .validate()
            .unwrap_err();
        assert!(
            matches!(
                err,
                MessageDeserializeError::UtMetadataSizeMismatch {
                    expected_size: 3,
                    received_size: 6
                }
            ),
            "{:?}",
            err
        );

        // success large piece
        UtMetadataData::from_bytes(0, CHUNK_SIZE + 1, ByteBuf(&[0u8; CHUNK_SIZE as usize][..]))
            .validate()
            .unwrap();
        UtMetadataData::from_bytes(1, CHUNK_SIZE + 1, ByteBuf(&[0u8; 1][..]))
            .validate()
            .unwrap();

        // piece out of bounds
        let err =
            UtMetadataData::from_bytes(2, CHUNK_SIZE + 1, ByteBuf(&[0u8; CHUNK_SIZE as usize][..]))
                .validate()
                .unwrap_err();
        assert!(
            matches!(
                err,
                MessageDeserializeError::UtMetadataPieceOutOfBounds {
                    total_pieces: 2,
                    received_piece: 2
                }
            ),
            "{:?}",
            err
        );

        // wrong size
        let err = UtMetadataData::from_bytes(
            0,
            CHUNK_SIZE + 1,
            ByteBuf(&[0u8; CHUNK_SIZE as usize - 1][..]),
        )
        .validate()
        .unwrap_err();

        assert!(
            matches!(
                err,
                MessageDeserializeError::UtMetadataSizeMismatch {
                    expected_size,
                    received_size,
                } if expected_size == CHUNK_SIZE && received_size == CHUNK_SIZE - 1
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn a_metadata_request_past_the_last_piece_is_out_of_bounds() {
        // Two pieces, the second short: the case where index == count used to
        // slice past the end.
        let metadata = Bytes::from(vec![7u8; CHUNK_SIZE as usize + 5]);

        for piece in [2, 3, u32::MAX / CHUNK_SIZE + 1, u32::MAX] {
            let err = UtMetadataData::piece_of(&metadata, piece).unwrap_err();
            assert!(
                matches!(
                    err,
                    MessageDeserializeError::UtMetadataPieceOutOfBounds {
                        total_pieces: 2,
                        received_piece,
                    } if received_piece == piece
                ),
                "{err:?}"
            );
        }

        let last = UtMetadataData::piece_of(&metadata, 1).unwrap();
        assert_eq!(last.data_0.0.len(), 5);
        let first = UtMetadataData::piece_of(&metadata, 0).unwrap();
        assert_eq!(first.data_0.0.len(), CHUNK_SIZE as usize);
    }

    #[test]
    fn metadata_we_do_not_hold_has_no_pieces() {
        let err = UtMetadataData::piece_of(&Bytes::new(), 0).unwrap_err();
        assert!(
            matches!(
                err,
                MessageDeserializeError::UtMetadataPieceOutOfBounds {
                    total_pieces: 0,
                    received_piece: 0,
                }
            ),
            "{err:?}"
        );
    }
}
