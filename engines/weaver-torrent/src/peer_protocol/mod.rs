// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

// BitTorrent peer protocol implementation: parsing, serialization etc.
//
// Can be used outside of librqbit.

mod double_buf;
pub mod extended;

use std::hint::unreachable_unchecked;

use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::clone_to_owned::CloneToOwned;
use crate::core::{
    constants::CHUNK_SIZE,
    hash_id::{Id20, Id32},
    lengths::ChunkInfo,
    merkle,
};
use byteorder::{BE, ByteOrder};
use bytes::Bytes;
use extended::PeerExtendedMessageIds;
use serde_derive::{Deserialize, Serialize};

pub use crate::peer_protocol::double_buf::DoubleBufHelper;

use self::extended::ExtendedMessage;

const INTEGER_LEN: usize = 4;
const MSGID_LEN: usize = 1;
const PREAMBLE_LEN: usize = INTEGER_LEN + MSGID_LEN;
const PIECE_MESSAGE_PREAMBLE_LEN: usize = PREAMBLE_LEN + INTEGER_LEN * 2;
pub const PIECE_MESSAGE_DEFAULT_LEN: usize = PIECE_MESSAGE_PREAMBLE_LEN + CHUNK_SIZE as usize;

const MAX_MSG_LEN_LEN_JUST_IN_CASE_EXTRA: usize = 64;
// An extended ut_metadata data message.
const MAX_UT_METADATA_MSG_LEN: usize = PREAMBLE_LEN
    + 1
    + b"d8:msg_typei1e5:piecei42e10:total_sizei16384ee".len()
    + CHUNK_SIZE as usize;

/// The most hashes a hashes message carries for a range, the piece-layer
/// chunk libtorrent asks for (`hash_picker::pick_hashes`).
pub const MAX_HASHES_IN_REPLY: u32 = 512;
// The most uncle hashes a reply carries: one per layer of the tallest file
// tree, of 2^64 bytes in 16 KiB blocks.
const MAX_UNCLES_IN_REPLY: usize = 64 - 14;
// A hashes message answering a request for the most hashes, proven to the
// root.
const MAX_HASHES_MSG_LEN: usize =
    PREAMBLE_LEN + HASH_REQUEST_LEN + (MAX_HASHES_IN_REPLY as usize + MAX_UNCLES_IN_REPLY) * 32;

/// The largest message read or written: a hashes message or a ut_metadata
/// data message, whichever is longer.
pub const MAX_MSG_LEN: usize = if MAX_HASHES_MSG_LEN > MAX_UT_METADATA_MSG_LEN {
    MAX_HASHES_MSG_LEN
} else {
    MAX_UT_METADATA_MSG_LEN
} + MAX_MSG_LEN_LEN_JUST_IN_CASE_EXTRA;

const PSTR_BT1: &str = "BitTorrent protocol";

// libtorrent `bt_peer_connection::write_handshake`: "indicate that we
// support upgrading to v2".
const V2_UPGRADE_BIT: u8 = 0x10;

type MsgId = u8;

const MSGID_CHOKE: MsgId = 0;
const MSGID_UNCHOKE: MsgId = 1;
const MSGID_INTERESTED: MsgId = 2;
const MSGID_NOT_INTERESTED: MsgId = 3;
const MSGID_HAVE: MsgId = 4;
const MSGID_BITFIELD: MsgId = 5;
const MSGID_REQUEST: MsgId = 6;
const MSGID_PIECE: MsgId = 7;
const MSGID_CANCEL: MsgId = 8;
const MSGID_EXTENDED: MsgId = 20;
// BEP 52 (libtorrent `bt_peer_connection::msg_hash_request` and on).
const MSGID_HASH_REQUEST: MsgId = 21;
const MSGID_HASHES: MsgId = 22;
const MSGID_HASH_REJECT: MsgId = 23;

// pieces root, base, index, length, proof layers.
const HASH_REQUEST_LEN: usize = 32 + INTEGER_LEN * 4;

pub const EXTENDED_UT_METADATA_KEY: &[u8] = b"ut_metadata";
pub const MY_EXTENDED_UT_METADATA: u8 = 3;

pub const EXTENDED_UT_PEX_KEY: &[u8] = b"ut_pex";
pub const MY_EXTENDED_UT_PEX: u8 = 1;

#[derive(Clone, Copy)]
pub struct MsgIdDebug(MsgId);
impl MsgIdDebug {
    const fn name(&self) -> Option<&'static str> {
        let n = match self.0 {
            MSGID_CHOKE => "choke",
            MSGID_UNCHOKE => "unchoke",
            MSGID_INTERESTED => "interested",
            MSGID_NOT_INTERESTED => "not_interested",
            MSGID_HAVE => "have",
            MSGID_BITFIELD => "bitfield",
            MSGID_REQUEST => "request",
            MSGID_PIECE => "piece",
            MSGID_CANCEL => "cancel",
            MSGID_EXTENDED => "extended",
            MSGID_HASH_REQUEST => "hash_request",
            MSGID_HASHES => "hashes",
            MSGID_HASH_REJECT => "hash_reject",
            _ => return None,
        };
        Some(n)
    }
}
impl core::fmt::Debug for MsgIdDebug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.name() {
            Some(name) => f.write_str(name),
            None => write!(f, "<unknown msg_id {}>", self.0),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MessageDeserializeError {
    #[error("not enough data (msgid={1:?}): expected at least {0} more bytes")]
    NotEnoughData(usize, Option<MsgIdDebug>),
    #[error("need a contiguous input to deserialize")]
    NeedContiguous,
    #[error("unsupported message id {0}")]
    UnsupportedMessageId(u8),
    #[error(transparent)]
    Bencode(#[from] crate::bencode::DeserializeError),
    #[error("incorrect message length msg_id={msg_id:?}, expected={expected}, received={received}")]
    IncorrectMsgLen {
        received: u32,
        expected: u32,
        msg_id: MsgIdDebug,
    },
    #[error("ut_metadata:data received {received_len} >= total_size is {total_size}")]
    UtMetadataBufLargerThanTotalSize { total_size: u32, received_len: u32 },
    #[error("ut_metadata:data length must be <= {CHUNK_SIZE} but received {0} bytes")]
    UtMetadataTooLarge(u32),
    #[error("ut_metadata: trailing bytes when decoding")]
    UtMetadataTrailingBytes,
    #[error("ut_metadata: missing total_size")]
    UtMetadataMissingTotalSize,
    #[error("ut_metadata: unrecognized message type: {0}")]
    UtMetadataTypeUnknown(u32),
    #[error("ut_metadata: piece {received_piece} >= total pieces {total_pieces}")]
    UtMetadataPieceOutOfBounds {
        total_pieces: u32,
        received_piece: u32,
    },
    #[error("ut_metadata: expected size {expected_size} != received size {received_size}")]
    UtMetadataSizeMismatch {
        expected_size: u32,
        received_size: u32,
    },
    #[error("pstr doesn't match {PSTR_BT1:?}")]
    HandshakePstrWrongContent,
    #[error("pstr should be 19 bytes long but got {0}")]
    HandshakePstrWrongLength(u8),
}

pub fn serialize_piece_preamble(chunk: &ChunkInfo, mut buf: &mut [u8]) -> usize {
    let len_prefix = MSGID_LEN as u32 + INTEGER_LEN as u32 * 2 + chunk.size;
    BE::write_u32(&mut buf[0..4], len_prefix);
    buf[4] = MSGID_PIECE;

    buf = &mut buf[5..];
    BE::write_u32(&mut buf[0..4], chunk.piece_index.get());
    BE::write_u32(&mut buf[4..8], chunk.offset);

    PIECE_MESSAGE_PREAMBLE_LEN
}

pub struct Piece<B> {
    pub index: u32,
    pub begin: u32,
    block_0: B,
    block_1: B,
}

impl<B: AsRef<[u8]>> std::fmt::Debug for Piece<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Piece")
            .field("index", &self.index)
            .field("begin", &self.begin)
            .field("len", &self.len())
            .field("len_0", &self.block_0.as_ref().len())
            .field("len_1", &self.block_1.as_ref().len())
            .finish_non_exhaustive()
    }
}

impl CloneToOwned for Piece<ByteBuf<'_>> {
    type Target = Piece<ByteBufOwned>;

    fn clone_to_owned(&self, within_buffer: Option<&Bytes>) -> Self::Target {
        Piece {
            index: self.index,
            begin: self.begin,
            block_0: self.block_0.clone_to_owned(within_buffer),
            block_1: self.block_1.clone_to_owned(within_buffer),
        }
    }
}

impl<B: AsRef<[u8]>> Piece<B> {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.block_0.as_ref().len() + self.block_1.as_ref().len()
    }

    pub fn serialize_unchecked_len(&self, mut buf: &mut [u8]) -> usize {
        buf[0..4].copy_from_slice(&self.index.to_be_bytes());
        buf[4..8].copy_from_slice(&self.begin.to_be_bytes());
        buf = &mut buf[8..];

        let b0 = self.block_0.as_ref();
        let b1 = self.block_1.as_ref();

        buf[..b0.len()].copy_from_slice(b0);
        buf = &mut buf[b0.len()..];
        buf[..b1.len()].copy_from_slice(b1);
        8 + b0.len() + b1.len()
    }
}

impl Piece<ByteBufOwned> {
    pub fn as_borrowed(&self) -> Piece<ByteBuf<'_>> {
        Piece {
            index: self.index,
            begin: self.begin,
            block_0: self.block_0.as_ref().into(),
            block_1: self.block_1.as_ref().into(),
        }
    }
}

impl<'a> Piece<ByteBuf<'a>> {
    pub fn data(&self) -> (&'a [u8], &'a [u8]) {
        (self.block_0.0, self.block_1.0)
    }

    pub fn from_data(index: u32, begin: u32, block: &'a [u8]) -> Self {
        Piece {
            index,
            begin,
            block_0: ByteBuf(block),
            block_1: ByteBuf(&[]),
        }
    }
}

/// A BEP 52 hash request: `count` nodes of the layer `base` above the
/// blocks of the file whose tree has `root`, from `index`, and the uncles
/// that prove them up to `proof_layers` above `base`. A hash reject repeats
/// it (libtorrent `hash_request`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashRequest {
    pub root: Id32,
    pub base: u32,
    pub index: u32,
    pub count: u32,
    pub proof_layers: u32,
}

impl HashRequest {
    /// The uncle hashes an answer carries after the `count` nodes: those
    /// above the subtree the nodes span (libtorrent
    /// `bt_peer_connection::on_hashes`).
    pub fn uncles(&self) -> u64 {
        (u64::from(self.proof_layers) + 1).saturating_sub(u64::from(merkle::num_layers(
            merkle::num_leafs(self.count.into()),
        )))
    }

    /// The bytes of the hashes of an answer.
    fn hashes_len(&self) -> u64 {
        (u64::from(self.count) + self.uncles()) * 32
    }

    fn serialize_unchecked_len(&self, buf: &mut [u8]) -> usize {
        buf[..32].copy_from_slice(&self.root.0);
        for (i, n) in [self.base, self.index, self.count, self.proof_layers]
            .into_iter()
            .enumerate()
        {
            BE::write_u32(&mut buf[32 + i * 4..36 + i * 4], n);
        }
        HASH_REQUEST_LEN
    }

    fn deserialize(b: [u8; HASH_REQUEST_LEN]) -> Self {
        Self {
            root: Id32::new(b[..32].try_into().unwrap()),
            base: BE::read_u32(&b[32..36]),
            index: BE::read_u32(&b[36..40]),
            count: BE::read_u32(&b[40..44]),
            proof_layers: BE::read_u32(&b[44..48]),
        }
    }
}

/// A BEP 52 hashes message: the request it answers, then its `count`
/// nodes and their uncles, lowest first.
#[derive(Debug)]
pub struct Hashes<B> {
    pub request: HashRequest,
    pub hashes: B,
}

impl<B: AsRef<[u8]>> Hashes<B> {
    pub fn hashes(&self) -> Vec<Id32> {
        self.hashes
            .as_ref()
            .chunks_exact(32)
            .map(|h| Id32::new(h.try_into().unwrap()))
            .collect()
    }
}

impl Hashes<ByteBufOwned> {
    /// The answer to `request`: its nodes, then their uncles.
    pub fn new(request: HashRequest, hashes: &[Id32]) -> Self {
        Self {
            request,
            hashes: ByteBufOwned::from(hashes.iter().flat_map(|h| h.0).collect::<Vec<u8>>()),
        }
    }

    pub fn as_borrowed(&self) -> Hashes<ByteBuf<'_>> {
        Hashes {
            request: self.request,
            hashes: ByteBuf(self.hashes.as_ref()),
        }
    }
}

#[derive(Debug)]
pub enum Message<'a> {
    Request(Request),
    Cancel(Request),
    Bitfield(ByteBuf<'a>),
    KeepAlive,
    Have(u32),
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Piece(Piece<ByteBuf<'a>>),
    Extended(ExtendedMessage<ByteBuf<'a>>),
    HashRequest(HashRequest),
    Hashes(Hashes<ByteBuf<'a>>),
    HashReject(HashRequest),
}

impl Message<'_> {
    /// Whether this is a BEP 52 hash message, which only a peer that
    /// speaks v2 may send (libtorrent `bt_peer_connection::on_hash_request`,
    /// `on_hashes`, `on_hash_reject`).
    pub fn is_v2_only(&self) -> bool {
        matches!(
            self,
            Message::HashRequest(..) | Message::Hashes(..) | Message::HashReject(..)
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SerializeError {
    #[error("not enough space in buffer")]
    NoSpaceInBuffer,
    #[error(transparent)]
    Bencode(#[from] crate::bencode::SerializeError),
    #[error("need peer's handshake to serialize ut_metadata, or peer does't support ut_metadata")]
    NeedUtMetadata,
    #[error("need peer's handshake to serialize ut_pex, or peer does't support ut_pex")]
    NeedPex,
}

impl From<std::io::Error> for SerializeError {
    fn from(_: std::io::Error) -> Self {
        Self::NoSpaceInBuffer
    }
}

impl Message<'_> {
    pub fn serialize(
        &self,
        out: &mut [u8],
        peer_extended_messages: &dyn Fn() -> PeerExtendedMessageIds,
    ) -> Result<usize, SerializeError> {
        macro_rules! check_len {
            ($l:expr) => {
                if out.len() < $l {
                    return Err(SerializeError::NoSpaceInBuffer);
                }
            };
        }

        macro_rules! write_preamble {
            ($msg_len:expr, $msg_id:expr) => {
                out[0..4].copy_from_slice(&(($msg_len + 1u32).to_be_bytes()));
                out[4] = $msg_id;
            };
        }

        match self {
            Message::Request(request) | Message::Cancel(request) => {
                const TOTAL_LEN: usize = PREAMBLE_LEN + INTEGER_LEN * 3;
                check_len!(TOTAL_LEN);
                let msg_id = match self {
                    Message::Request(..) => MSGID_REQUEST,
                    Message::Cancel(..) => MSGID_CANCEL,
                    _ => unsafe { unreachable_unchecked() },
                };
                write_preamble!((INTEGER_LEN * 3) as u32, msg_id);
                request.serialize_unchecked_len(&mut out[PREAMBLE_LEN..]);
                Ok(TOTAL_LEN)
            }
            Message::Bitfield(b) => {
                let block_len = b.as_ref().len();
                let total_len: usize = PREAMBLE_LEN + block_len;
                check_len!(total_len);
                write_preamble!(block_len as u32, MSGID_BITFIELD);
                out[PREAMBLE_LEN..PREAMBLE_LEN + block_len].copy_from_slice(b.as_ref());
                Ok(total_len)
            }
            Message::Choke | Message::Unchoke | Message::Interested | Message::NotInterested => {
                check_len!(PREAMBLE_LEN);
                let msg_id = match self {
                    Message::Choke => MSGID_CHOKE,
                    Message::Unchoke => MSGID_UNCHOKE,
                    Message::Interested => MSGID_INTERESTED,
                    Message::NotInterested => MSGID_NOT_INTERESTED,
                    _ => unsafe { unreachable_unchecked() },
                };
                write_preamble!(0, msg_id);
                Ok(PREAMBLE_LEN)
            }
            Message::Piece(p) => {
                let block_len = p.len();
                let payload_len = INTEGER_LEN * 2 + block_len;
                let total_len = PREAMBLE_LEN + payload_len;
                check_len!(total_len);
                write_preamble!(payload_len as u32, MSGID_PIECE);
                p.serialize_unchecked_len(&mut out[PREAMBLE_LEN..]);
                Ok(total_len)
            }
            Message::KeepAlive => {
                check_len!(4);
                out[0..4].copy_from_slice(&0u32.to_be_bytes());
                Ok(4)
            }
            Message::Have(v) => {
                check_len!(PREAMBLE_LEN + INTEGER_LEN);
                write_preamble!(INTEGER_LEN as u32, MSGID_HAVE);
                out[5..9].copy_from_slice(&v.to_be_bytes());
                Ok(9)
            }
            Message::Extended(e) => {
                check_len!(PREAMBLE_LEN + 2);
                let msg_len = e.serialize(&mut out[PREAMBLE_LEN..], peer_extended_messages)?;
                write_preamble!(msg_len as u32, MSGID_EXTENDED);
                Ok(PREAMBLE_LEN + msg_len)
            }
            Message::HashRequest(r) | Message::HashReject(r) => {
                check_len!(PREAMBLE_LEN + HASH_REQUEST_LEN);
                let msg_id = match self {
                    Message::HashRequest(..) => MSGID_HASH_REQUEST,
                    _ => MSGID_HASH_REJECT,
                };
                write_preamble!(HASH_REQUEST_LEN as u32, msg_id);
                Ok(PREAMBLE_LEN + r.serialize_unchecked_len(&mut out[PREAMBLE_LEN..]))
            }
            Message::Hashes(h) => {
                let hashes = h.hashes.as_ref();
                let msg_len = HASH_REQUEST_LEN + hashes.len();
                check_len!(PREAMBLE_LEN + msg_len);
                write_preamble!(msg_len as u32, MSGID_HASHES);
                h.request.serialize_unchecked_len(&mut out[PREAMBLE_LEN..]);
                out[PREAMBLE_LEN + HASH_REQUEST_LEN..PREAMBLE_LEN + msg_len]
                    .copy_from_slice(hashes);
                Ok(PREAMBLE_LEN + msg_len)
            }
        }
    }
}

impl Message<'_> {
    pub fn deserialize<'a>(
        buf: &'a [u8],
        buf2: &'a [u8],
    ) -> Result<(Message<'a>, usize), MessageDeserializeError> {
        let mut buf = DoubleBufHelper::new(buf, buf2);
        let len_prefix = buf
            .read_u32_be()
            .map_err(|rem| MessageDeserializeError::NotEnoughData(rem, None))?;
        let total_len = len_prefix as usize + 4;
        if len_prefix == 0 {
            return Ok((Message::KeepAlive, total_len));
        }

        let msg_id = buf.read_u8().ok_or(MessageDeserializeError::NotEnoughData(
            len_prefix as usize,
            None,
        ))?;

        let msg_len = len_prefix as usize - 1;
        if buf.len() < msg_len {
            return Err(MessageDeserializeError::NotEnoughData(
                msg_len - buf.len(),
                Some(MsgIdDebug(msg_id)),
            ));
        }

        macro_rules! check_msg_len {
            ($expected:expr) => {{
                if msg_len != $expected {
                    return Err(MessageDeserializeError::IncorrectMsgLen {
                        received: len_prefix - 1,
                        expected: $expected,
                        msg_id: MsgIdDebug(msg_id),
                    });
                }
            }};
            (min $expected:expr) => {{
                if msg_len < $expected {
                    return Err(MessageDeserializeError::IncorrectMsgLen {
                        received: len_prefix - 1,
                        expected: $expected,
                        msg_id: MsgIdDebug(msg_id),
                    });
                }
            }};
        }

        match msg_id {
            MSGID_CHOKE => {
                check_msg_len!(0);
                Ok((Message::Choke, total_len))
            }
            MSGID_UNCHOKE => {
                check_msg_len!(0);
                Ok((Message::Unchoke, total_len))
            }
            MSGID_INTERESTED => {
                check_msg_len!(0);
                Ok((Message::Interested, total_len))
            }
            MSGID_NOT_INTERESTED => {
                check_msg_len!(0);
                Ok((Message::NotInterested, total_len))
            }
            MSGID_HAVE => {
                check_msg_len!(4);
                let have = buf.read_u32_be().unwrap();
                Ok((Message::Have(have), total_len))
            }
            MSGID_BITFIELD => {
                check_msg_len!(min 1);
                // In practice, as bitfield is always (almost) the first message, it should be contiguous.
                let data = buf
                    .get_contiguous(msg_len)
                    .ok_or(MessageDeserializeError::NeedContiguous)?;
                Ok((Message::Bitfield(ByteBuf::from(data)), total_len))
            }
            MSGID_REQUEST | MSGID_CANCEL => {
                check_msg_len!(12);
                const I32: usize = 4;
                const I32_3: usize = I32 * 3;
                let req = buf.consume::<I32_3>().unwrap();
                let request = Request {
                    index: BE::read_u32(&req[0..I32]),
                    begin: BE::read_u32(&req[I32..I32 * 2]),
                    length: BE::read_u32(&req[I32 * 2..I32 * 3]),
                };
                let req = if msg_id == MSGID_REQUEST {
                    Message::Request(request)
                } else {
                    Message::Cancel(request)
                };
                Ok((req, total_len))
            }
            MSGID_PIECE => {
                const MIN_PAYLOAD: usize = 1;
                const MIN_LENGTH: usize = INTEGER_LEN * 2 + MIN_PAYLOAD;
                if msg_len < MIN_LENGTH {
                    return Err(MessageDeserializeError::IncorrectMsgLen {
                        expected: MIN_LENGTH as u32,
                        received: msg_len as u32,
                        msg_id: MsgIdDebug(msg_id),
                    });
                }

                let index = buf.read_u32_be().unwrap();
                let begin = buf.read_u32_be().unwrap();

                let block_len = msg_len - INTEGER_LEN * 2;
                let (block_0, block_1) = buf.consume_variable(block_len).unwrap();

                Ok((
                    Message::Piece(Piece {
                        index,
                        begin,
                        block_0: block_0.into(),
                        block_1: block_1.into(),
                    }),
                    total_len,
                ))
            }
            MSGID_EXTENDED => Ok((
                Message::Extended(ExtendedMessage::deserialize(buf.with_max_len(msg_len))?),
                PREAMBLE_LEN + msg_len,
            )),
            MSGID_HASH_REQUEST | MSGID_HASH_REJECT => {
                check_msg_len!(48); // HASH_REQUEST_LEN
                let r = HashRequest::deserialize(buf.consume::<HASH_REQUEST_LEN>().unwrap());
                let msg = if msg_id == MSGID_HASH_REQUEST {
                    Message::HashRequest(r)
                } else {
                    Message::HashReject(r)
                };
                Ok((msg, total_len))
            }
            MSGID_HASHES => {
                check_msg_len!(min 48); // HASH_REQUEST_LEN
                let request = HashRequest::deserialize(buf.consume::<HASH_REQUEST_LEN>().unwrap());
                // Exactly the nodes asked for and their uncles
                // (libtorrent `bt_peer_connection::on_hashes`).
                if (msg_len - HASH_REQUEST_LEN) as u64 != request.hashes_len() {
                    return Err(MessageDeserializeError::IncorrectMsgLen {
                        received: len_prefix - 1,
                        expected: (HASH_REQUEST_LEN as u64 + request.hashes_len())
                            .try_into()
                            .unwrap_or(u32::MAX),
                        msg_id: MsgIdDebug(msg_id),
                    });
                }
                let hashes = buf
                    .get_contiguous(msg_len - HASH_REQUEST_LEN)
                    .ok_or(MessageDeserializeError::NeedContiguous)?;
                Ok((
                    Message::Hashes(Hashes {
                        request,
                        hashes: ByteBuf::from(hashes),
                    }),
                    total_len,
                ))
            }
            msg_id => Err(MessageDeserializeError::UnsupportedMessageId(msg_id)),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: u64,
    pub info_hash: Id20,
    pub peer_id: Id20,
}

impl Handshake {
    pub fn new(info_hash: Id20, peer_id: Id20) -> Handshake {
        debug_assert_eq!(PSTR_BT1.len(), 19);

        let mut reserved: u64 = 0;
        // supports extended messaging
        reserved |= 1 << 20;

        Handshake {
            reserved,
            info_hash,
            peer_id,
        }
    }

    pub fn deserialize(b: &[u8]) -> Result<(Handshake, usize), MessageDeserializeError> {
        const LEN: usize = 1 + PSTR_BT1.len() + 8 + 20 + 20;
        if b.len() < LEN {
            return Err(MessageDeserializeError::NotEnoughData(LEN - b.len(), None));
        }
        if b[0] as usize != PSTR_BT1.len() {
            return Err(MessageDeserializeError::HandshakePstrWrongLength(b[0]));
        }
        if &b[1..20] != PSTR_BT1.as_bytes() {
            return Err(MessageDeserializeError::HandshakePstrWrongContent);
        }

        let h = Handshake {
            reserved: BE::read_u64(&b[20..28]),
            info_hash: Id20::new(b[28..48].try_into().unwrap()),
            peer_id: Id20::new(b[48..68].try_into().unwrap()),
        };
        Ok((h, LEN))
    }

    pub fn supports_extended(&self) -> bool {
        self.reserved.to_be_bytes()[5] & 0x10 > 0
    }

    /// Whether the peer offers to move a hybrid's connection to v2: bit
    /// 0x10 of the last reserved byte (libtorrent
    /// `bt_peer_connection::write_handshake`).
    pub fn offers_v2(&self) -> bool {
        self.reserved.to_be_bytes()[7] & V2_UPGRADE_BIT > 0
    }

    /// This handshake, offering a move to v2 when `offer` holds.
    #[must_use]
    pub fn offering_v2(mut self, offer: bool) -> Self {
        if offer {
            self.reserved |= u64::from(V2_UPGRADE_BIT);
        }
        self
    }

    #[must_use]
    pub fn serialize_unchecked_len(&self, buf: &mut [u8]) -> usize {
        debug_assert_eq!(PSTR_BT1.len(), 19);
        buf[0] = 19;
        buf[1..20].copy_from_slice(PSTR_BT1.as_bytes());
        buf[20..28].copy_from_slice(&self.reserved.to_be_bytes());
        buf[28..48].copy_from_slice(&self.info_hash.0);
        buf[48..68].copy_from_slice(&self.peer_id.0);
        68
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Request {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl Request {
    pub fn new(index: u32, begin: u32, length: u32) -> Self {
        Self {
            index,
            begin,
            length,
        }
    }

    pub fn serialize_unchecked_len(&self, buf: &mut [u8]) -> usize {
        buf[0..4].copy_from_slice(&self.index.to_be_bytes());
        buf[4..8].copy_from_slice(&self.begin.to_be_bytes());
        buf[8..12].copy_from_slice(&self.length.to_be_bytes());
        12
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;

    use crate::peer_protocol::extended::handshake::ExtendedHandshake;

    const EXTENDED: &[u8] = include_bytes!("../../resources/test/extended-handshake.bin");

    use super::*;
    #[test]
    fn test_handshake_serialize() {
        let info_hash = Id20::new([
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ]);
        let peer_id = Id20::new([
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ]);
        let mut buf = [0u8; 100];
        let se = Handshake::new(info_hash, peer_id);
        let len = se.serialize_unchecked_len(&mut buf);
        assert_eq!(len, 20 + 20 + 8 + 19 + 1);
        assert_eq!(buf[0], 19);
        assert_eq!(&buf[1..20], PSTR_BT1.as_bytes());
        assert_eq!(&buf[28..48], &info_hash.0);
        assert_eq!(&buf[48..68], &peer_id.0);

        let (de, dlen) = Handshake::deserialize(&buf).unwrap();
        assert_eq!(dlen, len);
        assert_eq!(se, de);
    }

    #[test]
    fn test_extended_serialize() {
        let msg = Message::Extended(ExtendedMessage::Handshake(ExtendedHandshake::new()));
        let mut out = [0u8; 100];
        msg.serialize(&mut out, &Default::default).unwrap();
        dbg!(out);
    }

    #[test]
    fn test_deserialize_serialize_extended_non_contiguous() {
        for split_point in 0..EXTENDED.len() {
            let (first, second) = EXTENDED.split_at(split_point);
            let res = Message::deserialize(first, second);
            if split_point > PREAMBLE_LEN + 1 && split_point < EXTENDED.len() {
                assert!(
                    matches!(res, Err(MessageDeserializeError::NeedContiguous)),
                    "expected NeedContiguous: {split_point}"
                )
            } else {
                let (msg, len) = res
                    .inspect_err(|e| panic!("split_point={split_point:?}; error: {e:#}"))
                    .unwrap();
                assert!(matches!(msg, Message::Extended(..)));
                assert_eq!(len, EXTENDED.len());
            }
        }
    }

    #[test]
    fn test_deserialize_piece() {
        const LEN: usize = 100;
        const EXTRA: usize = 100;
        let mut buf = [0u8; LEN + EXTRA];

        #[allow(clippy::needless_range_loop)]
        for id in 0..buf.len() {
            buf[id] = id as u8;
        }

        let block_len = LEN - PREAMBLE_LEN - INTEGER_LEN * 2;
        let len_prefix: u32 = (block_len + INTEGER_LEN * 2 + MSGID_LEN) as u32;
        let index: u32 = 42;
        let begin: u32 = 43;

        buf[0..4].copy_from_slice(&len_prefix.to_be_bytes());
        buf[4] = MSGID_PIECE;
        buf[5..9].copy_from_slice(&index.to_be_bytes());
        buf[9..13].copy_from_slice(&begin.to_be_bytes());

        for split_point in 0..buf.len() {
            dbg!(split_point);
            let (first, second) = buf.split_at(split_point);
            let (msg, len) = Message::deserialize(first, second).unwrap();

            let piece = match &msg {
                Message::Piece(piece) => piece,
                other => panic!("expected piece got {other:?}"),
            };

            assert_eq!(piece.len(), block_len);
            assert_eq!(piece.index, index);
            assert_eq!(piece.begin, begin);
            assert_eq!(len, LEN);

            let mut tmp = [0u8; 100];
            let slen = msg.serialize(&mut tmp, &|| Default::default()).unwrap();
            assert_eq!(slen, len);
            assert_eq!(buf[..len], tmp[..len]);

            let (first, second) = piece.data();

            assert_eq!(first.len() + second.len(), block_len);
            assert_eq!(first, &buf[13..13 + first.len()]);
            assert_eq!(
                second,
                &buf[13 + first.len()..13 + first.len() + second.len()]
            );
        }
    }

    #[test]
    fn test_deserialize_request() {
        let mut buf = [0u8; 100];

        let len_prefix: u32 = (MSGID_LEN + INTEGER_LEN * 3) as u32;
        let index: u32 = 42;
        let begin: u32 = 43;
        let length: u32 = 44;

        buf[0..4].copy_from_slice(&len_prefix.to_be_bytes());
        buf[4] = MSGID_REQUEST;
        buf[5..9].copy_from_slice(&index.to_be_bytes());
        buf[9..13].copy_from_slice(&begin.to_be_bytes());
        buf[13..17].copy_from_slice(&length.to_be_bytes());

        for split_point in 0..buf.len() {
            dbg!(split_point);
            let (first, second) = buf.split_at(split_point);
            let (msg, len) = Message::deserialize(first, second).unwrap();

            let request = match msg {
                Message::Request(req) => req,
                other => panic!("expected request got {other:?}"),
            };

            assert_eq!(request.index, index);
            assert_eq!(request.begin, begin);
            assert_eq!(request.length, length);
            assert_eq!(len, 17);

            let mut tmp = [0u8; 100];
            let slen = msg.serialize(&mut tmp, &|| Default::default()).unwrap();
            assert_eq!(slen, len);
            assert_eq!(buf[..len], tmp[..len]);
        }
    }

    #[test]
    fn test_keepalive() {
        let buf = [0u8; 100];

        for split_point in 0..buf.len() {
            let (first, second) = buf.split_at(split_point);
            let (msg, len) = Message::deserialize(first, second).unwrap();
            assert!(matches!(msg, Message::KeepAlive));
            assert_eq!(len, 4);
            let mut tmp = [0u8; 100];
            let slen = msg.serialize(&mut tmp, &|| Default::default()).unwrap();
            assert_eq!(slen, len);
            assert_eq!(buf[..len], tmp[..len]);
        }
    }

    #[test]
    fn test_have() {
        let mut buf = [0u8; 100];
        buf[0..4].copy_from_slice(&5u32.to_be_bytes());
        buf[4] = MSGID_HAVE;
        buf[5..9].copy_from_slice(&42u32.to_be_bytes());

        for split_point in 0..buf.len() {
            let (first, second) = buf.split_at(split_point);
            let (msg, len) = Message::deserialize(first, second).unwrap();
            assert!(matches!(msg, Message::Have(42)));
            assert_eq!(len, 9);
            let mut tmp = [0u8; 100];
            let slen = msg.serialize(&mut tmp, &|| Default::default()).unwrap();
            assert_eq!(slen, len);
            assert_eq!(buf[..len], tmp[..len]);
        }
    }

    #[test]
    fn test_bitfield() {
        let mut buf = [0u8; 100];
        buf[0..4].copy_from_slice(&43u32.to_be_bytes());
        buf[4] = MSGID_BITFIELD;
        for byte in buf[5..47].iter_mut() {
            *byte = 0b10101010;
        }

        for split_point in 0..buf.len() {
            let (first, second) = buf.split_at(split_point);
            let res = Message::deserialize(first, second);
            if (6..47).contains(&split_point) {
                assert!(
                    matches!(res, Err(MessageDeserializeError::NeedContiguous)),
                    "expected NeedContiguous: split_point={split_point}"
                );
                continue;
            }
            let (msg, len) = res.context(split_point).unwrap();
            let bf = match &msg {
                Message::Bitfield(bf) => bf,
                other => panic!("expected bitfield, got {other:?}"),
            };
            assert_eq!(len, 47);
            assert_eq!(bf.as_ref().len(), 42);
            for byte in bf.as_ref() {
                assert_eq!(*byte, 0b10101010);
            }
            let mut tmp = [0u8; 100];
            let slen = msg.serialize(&mut tmp, &|| Default::default()).unwrap();
            assert_eq!(slen, len);
            assert_eq!(buf[..len], tmp[..len]);
        }
    }

    #[test]
    fn test_no_data_messages() {
        let mut buf = [0u8; 100];

        for msgid in [
            MSGID_CHOKE,
            MSGID_UNCHOKE,
            MSGID_INTERESTED,
            MSGID_NOT_INTERESTED,
        ] {
            buf[0..4].copy_from_slice(&1u32.to_be_bytes());
            buf[4] = msgid;
            for split_point in 0..buf.len() {
                let (first, second) = buf.split_at(split_point);
                let (msg, len) = Message::deserialize(first, second).unwrap();
                match (msgid, &msg) {
                    (MSGID_CHOKE, Message::Choke)
                    | (MSGID_UNCHOKE, Message::Unchoke)
                    | (MSGID_INTERESTED, Message::Interested)
                    | (MSGID_NOT_INTERESTED, Message::NotInterested) => {}
                    (msgid, msg) => panic!("msgid={msgid}, msg={msg:?}"),
                }
                assert_eq!(len, 5);
                let mut tmp = [0u8; 100];
                let slen = msg.serialize(&mut tmp, &|| Default::default()).unwrap();
                assert_eq!(slen, len);
                assert_eq!(buf[..len], tmp[..len]);
            }
        }
    }

    fn hash_request(count: u32, proof_layers: u32) -> HashRequest {
        HashRequest {
            root: Id32::new([7; 32]),
            base: 2,
            index: 1024,
            count,
            proof_layers,
        }
    }

    // libtorrent `bt_peer_connection::write_hash_request`: a 4-byte length
    // of 49, the id, the pieces root, then base, index, count and proof
    // layers as big-endian u32s; `write_hash_reject` differs in the id.
    #[test]
    fn hash_requests_and_rejects_lay_out_as_libtorrent_writes_them() {
        let mut want = Vec::new();
        want.extend_from_slice(&49u32.to_be_bytes());
        want.push(0);
        want.extend_from_slice(&[7; 32]);
        for n in [2u32, 1024, 8, 10] {
            want.extend_from_slice(&n.to_be_bytes());
        }

        for (id, msg) in [
            (21, Message::HashRequest(hash_request(8, 10))),
            (23, Message::HashReject(hash_request(8, 10))),
        ] {
            want[4] = id;
            let mut out = [0u8; 100];
            let len = msg.serialize(&mut out, &Default::default).unwrap();
            assert_eq!(&out[..len], &want[..]);

            for split_point in 0..want.len() {
                let (first, second) = want.split_at(split_point);
                let (back, back_len) = Message::deserialize(first, second).unwrap();
                assert_eq!(back_len, want.len());
                match (msg_id_of(&back), back) {
                    (21, Message::HashRequest(r)) | (23, Message::HashReject(r)) => {
                        assert_eq!(r, hash_request(8, 10))
                    }
                    other => panic!("{id}: {other:?}"),
                }
            }
        }
    }

    fn msg_id_of(msg: &Message<'_>) -> u8 {
        match msg {
            Message::HashRequest(..) => 21,
            Message::Hashes(..) => 22,
            Message::HashReject(..) => 23,
            _ => 0,
        }
    }

    // libtorrent test_hash_picker.cpp `add_piece_hashes` and
    // `add_piece_hashes_padded`: 512 nodes proven 9 layers up carry one
    // uncle, 8 (or 5, padded to 8) nodes proven 10 layers up carry 8.
    #[test]
    fn a_hashes_reply_carries_the_uncles_libtorrent_counts() {
        assert_eq!(hash_request(512, 9).uncles(), 1);
        assert_eq!(hash_request(8, 10).uncles(), 8);
        assert_eq!(hash_request(5, 10).uncles(), 8);
        assert_eq!(hash_request(4, 0).uncles(), 0);
        assert_eq!(hash_request(1, 0).uncles(), 1);
    }

    // libtorrent `bt_peer_connection::write_hashes`: the request header,
    // then the nodes and uncles; `on_hashes` refuses any other length.
    #[test]
    fn a_hashes_reply_round_trips_and_its_length_must_match() {
        let request = hash_request(5, 10);
        let hashes: Vec<Id32> = (0..13u8).map(|i| Id32::new([i; 32])).collect();
        let owned = Hashes::new(request, &hashes);
        let mut out = vec![0u8; MAX_MSG_LEN];
        let len = Message::Hashes(owned.as_borrowed())
            .serialize(&mut out, &Default::default)
            .unwrap();
        assert_eq!(len, 5 + 48 + 13 * 32);
        assert_eq!(&out[..4], &((1 + 48 + 13 * 32) as u32).to_be_bytes());
        assert_eq!(out[4], 22);
        assert_eq!(&out[53..85], &[0; 32]);

        for split_point in 0..len {
            let (first, second) = out[..len].split_at(split_point);
            match Message::deserialize(first, second) {
                Ok((Message::Hashes(h), got)) => {
                    assert_eq!(got, len);
                    assert_eq!(h.request, request);
                    assert_eq!(h.hashes(), hashes);
                }
                // The hashes are borrowed whole, so a split inside them
                // waits for the buffer to be made contiguous.
                Err(MessageDeserializeError::NeedContiguous) => {
                    assert!(split_point > 53 && split_point < len, "{split_point}")
                }
                other => panic!("{split_point}: {other:?}"),
            }
        }

        // One hash short of the request's nodes and uncles.
        let short = len - 32;
        out[..4].copy_from_slice(&((short - 4) as u32).to_be_bytes());
        assert!(matches!(
            Message::deserialize(&out[..short], &[]),
            Err(MessageDeserializeError::IncorrectMsgLen { .. })
        ));
    }

    // The largest reply Weaver asks for, libtorrent's 512-node chunk proven
    // to the root of the tallest tree, fits one frame.
    #[test]
    fn the_largest_hashes_reply_fits_a_frame() {
        let request = HashRequest {
            count: MAX_HASHES_IN_REPLY,
            proof_layers: 9 + MAX_UNCLES_IN_REPLY as u32 - 1,
            ..hash_request(0, 0)
        };
        assert_eq!(request.uncles(), MAX_UNCLES_IN_REPLY as u64);
        let hashes = vec![Id32::new([1; 32]); 512 + MAX_UNCLES_IN_REPLY];
        let owned = Hashes::new(request, &hashes);
        let mut out = vec![0u8; MAX_MSG_LEN];
        let len = Message::Hashes(owned.as_borrowed())
            .serialize(&mut out, &Default::default)
            .unwrap();
        let (back, back_len) = Message::deserialize(&out[..len], &[]).unwrap();
        assert_eq!(back_len, len);
        assert!(matches!(back, Message::Hashes(h) if h.hashes() == hashes));
    }

    // libtorrent `bt_peer_connection::write_handshake` sets 0x10 in the
    // last reserved byte to offer v2.
    #[test]
    fn the_v2_offer_is_bit_0x10_of_the_last_reserved_byte() {
        let plain = || Handshake::new(Id20::new([1; 20]), Id20::new([2; 20]));
        assert!(!plain().offers_v2());
        assert_eq!(plain().offering_v2(false), plain());

        let offer = plain().offering_v2(true);
        assert!(offer.offers_v2());
        let mut buf = [0u8; 68];
        offer.serialize_unchecked_len(&mut buf);
        assert_eq!(buf[20 + 7], 0x10);
        assert_eq!(&buf[20..27], &plain().reserved.to_be_bytes()[..7]);
        assert!(Handshake::deserialize(&buf).unwrap().0.offers_v2());
    }
}
