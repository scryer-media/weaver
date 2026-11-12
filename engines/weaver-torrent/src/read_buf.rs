// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{io::IoSliceMut, time::Duration};

use crate::peer_protocol::{
    Handshake, Message, MessageDeserializeError, PartialBlock, extended::PeerExtendedMessageIds,
};
use crate::{Error, Result};
use crate::{
    peer_connection::with_timeout, type_aliases::BoxAsyncReadVectored,
    vectored_traits::AsyncReadVectoredExt,
};
use futures::TryFutureExt;
use tokio::io::AsyncReadExt;

// The buffer's size between frames longer than it. Greater than
// MAX_MSG_LEN, so only a frame longer than every message but the bitfield
// grows it.
const BUFLEN: usize = 0x8000; // 32kb

// The most a buffer grows to for a peer that keeps the socket full, so one
// read takes many blocks (libtorrent grows its receive buffer to what the
// socket holds, up to `max_peer_recv_buffer_size`, 2 MiB by default). As
// long as the peer writer's batch.
const GROWN_MAX: usize = 0x8_0000; // 512 KiB

/// A ringbuffer for reading bittorrent messages from socket.
/// Messages may thus span 2 slices (notably, Piece message and UtMetadata messages), which is reflected in their contents.
///
/// A frame longer than the buffer grows it to the frame, which its message's
/// length rule bounds, and the buffer shrinks back once the frame is read
/// (libtorrent `receive_buffer::grow`, `receive_buffer::normalize`).
pub struct ReadBuf {
    buf: Box<[u8]>,
    start: usize,
    len: usize,
    // Bytes of the messages read so far, frames whole.
    consumed: u64,
    // The size the reads so far ask the buffer to be, from BUFLEN to
    // GROWN_MAX; it takes it between messages, once what it holds fits.
    wanted: usize,
    // The extensions we advertised on the connection: a message under the id
    // of another is not read (see `ExtendedMessage::deserialize_advertised`).
    advertised: PeerExtendedMessageIds,
}

/// How much a read may take off the socket: as much as the connection holds
/// download quota for (libtorrent `peer_connection::setup_receive` reads at
/// most `m_quota[download_channel]`).
pub(crate) trait ReadQuota {
    /// Waits until a read may take a byte or more, and returns how many it
    /// may take, at most `room`. `need` more bytes complete the frame.
    async fn admit(&mut self, need: usize, room: usize) -> usize;
}

/// A read bounded only by the room in the buffer.
#[cfg(test)]
pub(crate) struct Unlimited;

#[cfg(test)]
impl ReadQuota for Unlimited {
    async fn admit(&mut self, _need: usize, room: usize) -> usize {
        room
    }
}

/// Advance by N bytes. A macro so that existing field-level
/// borrows are understood by the borrow checker.
macro_rules! advance {
    ($self:expr, $len:expr) => {
        $self.len = $self.len.saturating_sub($len);
        $self.start = ($self.start + $len) % $self.buf.len();
    };
}

/// Convert readbuf into 2 slices. A macro so that field-level
/// borrows are understood by the borrow checker.
macro_rules! as_slices {
    ($self:expr) => {{
        let (first, second) = $self.as_slice_ranges();
        (&$self.buf[first], &$self.buf[second])
    }};
}

impl ReadBuf {
    pub fn new() -> Self {
        Self {
            buf: vec![0u8; BUFLEN].into_boxed_slice(),
            start: 0,
            len: 0,
            consumed: 0,
            wanted: BUFLEN,
            advertised: PeerExtendedMessageIds::ours(),
        }
    }

    /// The extensions we advertised on the connection, in our extended
    /// handshake's `m`; every one of ours until then.
    pub fn set_advertised(&mut self, advertised: PeerExtendedMessageIds) {
        self.advertised = advertised;
    }

    /// Bytes received and not yet read as a message.
    pub fn buffered(&self) -> usize {
        self.len
    }

    /// Bytes of the messages read so far.
    pub fn consumed(&self) -> u64 {
        self.consumed
    }

    /// The Piece message being read, when part of its block arrived and
    /// the rest did not.
    pub fn partial_block(&self) -> Option<PartialBlock> {
        let (first, second) = as_slices!(self);
        PartialBlock::at_front(first, second)
    }

    // Read the BT handshake.
    // This MUST be run as the first operation on the buffer.
    //
    // The 68-byte handshake may arrive split across several reads (e.g. when the
    // peer, or a proxy in between, writes it in more than one chunk), so keep
    // reading until it is complete. The timeout covers the whole handshake.
    pub async fn read_handshake(
        &mut self,
        conn: &mut BoxAsyncReadVectored,
        timeout: Duration,
    ) -> Result<Handshake> {
        self.start = 0;
        self.len = 0;
        with_timeout("reading", timeout, async {
            loop {
                let n = conn
                    .read(&mut self.buf[self.len..])
                    .await
                    .map_err(Error::ReadHandshake)?;
                if n == 0 {
                    return Err(Error::PeerDisconnectedReadingHandshake);
                }
                self.len += n;
                match Handshake::deserialize(&self.buf[..self.len]) {
                    Ok((h, size)) => {
                        self.advance(size);
                        return Ok(h);
                    }
                    Err(MessageDeserializeError::NotEnoughData(..)) => continue,
                    Err(e) => return Err(Error::DeserializeHandshake(e)),
                }
            }
        })
        .await
    }

    fn is_contiguous(&self) -> bool {
        self.start + self.len == (self.start + self.len) % self.buf.len()
    }

    // In rare cases, we might need to make the buffer contiguous, as the message
    // parsing code won't work with a split ringbuffer. Only "bitfield" and "extended" messages
    // need contiguous buffers.
    // "Bitfield" however is always sent early, so in practice it won't ever happen. For "extended"
    // in practice, it would rarely happen with UtMetadata::Data messages if the split happens to land
    // in the middle of bencoded data (as we only can deserialize bencode from a single slice).
    fn make_contiguous(&mut self) -> Result<()> {
        if self.is_contiguous() {
            #[allow(clippy::cast_possible_truncation)]
            return Err(Error::BugReadBufMakeContiguous {
                start: self.start as u16,
                len: self.len as u16,
            });
        }

        // This should be so rare that it's not worth writing complex in-place code.
        // See the source in VecDequeue::make_contiguous to see how involved it would be
        // otherwise.
        self.relocate(self.buf.len());
        Ok(())
    }

    /// Moves the unread bytes to the start of a buffer of `size` bytes.
    fn relocate(&mut self, size: usize) {
        let mut new = vec![0u8; size].into_boxed_slice();
        let (first, second) = as_slices!(self);
        new[..first.len()].copy_from_slice(first);
        new[first.len()..first.len() + second.len()].copy_from_slice(second);
        self.buf = new;
        self.start = 0;
    }

    /// Convert into 2 slices (as ranges).
    fn as_slice_ranges(&self) -> (std::ops::Range<usize>, std::ops::Range<usize>) {
        // These .min() calls are for asm to be branchless and the code panicless.
        let size = self.buf.len();
        let len = self.len.min(size);
        let start = self.start.min(size);

        let first_len = len.min(size - start);
        let first = start..start + first_len;
        let second = 0..len.saturating_sub(first_len);
        (first, second)
    }

    fn advance(&mut self, len: usize) {
        advance!(self, len);
    }

    /// Get a part of the buffer for reading into, at most `max` bytes.
    fn unfilled_ioslices(&mut self, max: usize) -> [IoSliceMut<'_>; 2] {
        let size = self.buf.len();
        let write_start = (self.start.saturating_add(self.len)) % size;
        let available_len = size.saturating_sub(self.len).min(max);
        let first_len = (size.saturating_sub(write_start)).min(available_len);
        let second_len = available_len.saturating_sub(first_len);

        let (second, first) = self.buf.split_at_mut(write_start);

        let first_len = first_len.min(first.len());
        let second_len = second_len.min(second.len());
        [
            IoSliceMut::new(&mut first[..first_len]),
            IoSliceMut::new(&mut second[..second_len]),
        ]
    }

    /// Read a message into the buffer, try to deserialize it and call the callback on it.
    /// `num_pieces` is the torrent's piece count once its metadata is known,
    /// which sets the length of a bitfield. Each read off the socket takes
    /// what `quota` admits; the timeout runs only while the socket is read,
    /// not while the quota is waited for (libtorrent
    /// `peer_connection::second_tick` times out only a peer it reads).
    pub async fn read_message(
        &mut self,
        conn: &mut BoxAsyncReadVectored,
        timeout: Duration,
        num_pieces: Option<u32>,
        quota: &mut impl ReadQuota,
    ) -> Result<Message<'_>> {
        // The buffer takes the size the reads asked for, and a frame that
        // grew it past that was read.
        if self.buf.len() != self.wanted && self.len <= self.wanted {
            self.relocate(self.wanted);
        }
        loop {
            let err = {
                // A workaround for borrow-checker not understanding early returns.
                //
                // A stacked reborrow of self. After this block we either return from
                // the function, or the block returns an owned error.
                //
                // Safety: there's nothing inherently unsafe here, this is just a
                // borrow checker workaround. However to ensure we respect aliasing
                // rules (&mut references are exclusive), there's a miri test below.
                //
                // In this case, 2 &mut references DO exist lexically, but in reality the
                // latter one is a reborrow that ends with the block.
                //
                // Run with `cargo +nightly miri test -p librqbit --features miri test_read_buf_miri -- --nocapture --ignore`
                let this = unsafe { &mut *(self as *mut Self) };
                let (first, second) = as_slices!(this);
                match Message::deserialize_advertised(first, second, num_pieces, &this.advertised) {
                    Ok((msg, size)) => {
                        advance!(this, size);
                        this.consumed += size as u64;
                        return Ok(msg);
                    }
                    Err(e) => e,
                }
            };

            match err {
                MessageDeserializeError::NotEnoughData(mut need_additional_bytes, ..) => {
                    // The frame is within its rule, so the buffer grows to it.
                    if self.len + need_additional_bytes > self.buf.len() {
                        self.relocate(self.len + need_additional_bytes);
                    }
                    while need_additional_bytes > 0 {
                        let room = self.buf.len() - self.len;
                        let admitted = quota.admit(need_additional_bytes, room).await;
                        let size = with_timeout(
                            "reading",
                            timeout,
                            conn.read_vectored(&mut self.unfilled_ioslices(admitted))
                                .map_err(Error::Write),
                        )
                        .await?;
                        if size == 0 {
                            return Err(Error::PeerDisconnected);
                        }
                        // A read that filled all the room, more than the
                        // frame needed, may have left more in the socket:
                        // the buffer doubles. One that took a quarter of it
                        // or less halves it.
                        if size == room && size > need_additional_bytes {
                            self.wanted = (self.buf.len() * 2).min(GROWN_MAX);
                        } else if size <= self.buf.len() / 4 {
                            self.wanted = (self.buf.len() / 2).max(BUFLEN);
                        }
                        self.len += size;
                        need_additional_bytes = need_additional_bytes.saturating_sub(size)
                    }
                }
                MessageDeserializeError::NeedContiguous => {
                    self.make_contiguous()?;
                    continue;
                }
                e => return Err(Error::Deserialize(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffers::ByteBuf;
    use crate::core::constants::CHUNK_SIZE;
    use crate::peer_protocol::{
        Handshake, MAX_MSG_LEN, MAX_PACKET_LEN, Message, MessageDeserializeError,
        extended::{
            ExtendedMessage, PeerExtendedMessageIds,
            ut_metadata::{UtMetadata, UtMetadataData},
        },
        max_frame_len,
    };
    use std::{net::Ipv4Addr, time::Duration};
    use tokio::io::AsyncWriteExt;

    use crate::{
        tests::test_util::setup_test_logging, type_aliases::BoxAsyncReadVectored,
        vectored_traits::AsyncReadVectoredIntoCompat,
    };

    use super::{BUFLEN, GROWN_MAX, ReadBuf, ReadQuota, Unlimited};

    #[test]
    fn test_ringbuf_ranges() {
        let mut b = ReadBuf::new();
        assert_eq!(b.as_slice_ranges(), (0..0, 0..0));

        b.start = 10;
        b.len = 10;
        assert_eq!(b.as_slice_ranges(), (10..20, 0..0));

        b.start = BUFLEN - 100;
        b.len = 100;
        assert_eq!(b.as_slice_ranges(), (BUFLEN - 100..BUFLEN, 0..0));

        b.start = BUFLEN - 100;
        b.len = 120;
        assert_eq!(b.as_slice_ranges(), (BUFLEN - 100..BUFLEN, 0..20));

        b.start = BUFLEN - 100;
        b.len = BUFLEN;
        assert_eq!(b.as_slice_ranges(), (BUFLEN - 100..BUFLEN, 0..BUFLEN - 100));
    }

    #[test]
    fn test_ringbuf_advance() {
        let mut b = ReadBuf::new();

        b.start = 10;
        b.len = 10;
        assert_eq!(b.as_slice_ranges(), (10..20, 0..0));
        b.advance(5);
        assert_eq!(b.as_slice_ranges(), (15..20, 0..0));

        b.start = BUFLEN - 5;
        b.len = 10;
        assert_eq!(b.as_slice_ranges(), (BUFLEN - 5..BUFLEN, 0..5));
        b.advance(5);
        assert_eq!(b.as_slice_ranges(), (0..5, 0..0));
    }

    #[test]
    fn test_ringbuf_make_contiguous() {
        let mut b = ReadBuf::new();
        assert!(b.is_contiguous());
        assert!(b.make_contiguous().is_err());

        fn fill(buf: &mut [u8], value: u8) {
            for b in buf.iter_mut() {
                *b = value;
            }
        }

        fill(&mut b.buf[BUFLEN - 100..], 42);
        fill(&mut b.buf[..200], 43);
        b.start = BUFLEN - 100;
        b.len = 300;
        assert!(!b.is_contiguous());
        assert!(b.make_contiguous().is_ok());
        assert_eq!(b.len, 300);
        assert_eq!(b.start, 0);
        assert_eq!(&b.buf[..100], &[42u8; 100]);
        assert_eq!(&b.buf[100..300], &[43u8; 200]);
        assert_eq!(&b.buf[300..], &[0u8; BUFLEN - 300]);
    }

    #[test]
    fn test_unfilled_ioslices() {
        ReadBuf::new();

        fn offset(buf: &ReadBuf, s: *const u8, len: usize) -> std::ops::Range<usize> {
            if len == 0 {
                return 0..0;
            }
            let offset = unsafe { s.byte_offset_from(buf.buf.as_ptr()) } as usize;
            offset..offset + len
        }

        fn offsets(
            buf: &ReadBuf,
            ioslices: [(*const u8, usize); 2],
        ) -> [std::ops::Range<usize>; 2] {
            [
                offset(buf, ioslices[0].0, ioslices[0].1),
                offset(buf, ioslices[1].0, ioslices[1].1),
            ]
        }

        #[track_caller]
        fn assert_one(start: usize, len: usize, ranges: [std::ops::Range<usize>; 2]) {
            let mut b = ReadBuf::new();
            b.start = start;
            b.len = len;
            let [f, s] = b.unfilled_ioslices(usize::MAX);
            let slices = [(f.as_ptr(), f.len()), (s.as_ptr(), s.len())];
            assert_eq!(offsets(&b, slices), ranges)
        }

        // full
        assert_one(0, BUFLEN, [0..0, 0..0]);
        assert_one(100, BUFLEN, [0..0, 0..0]);
        assert_one(BUFLEN, BUFLEN, [0..0, 0..0]);

        // start=0
        assert_one(0, 100, [100..BUFLEN, 0..0]);
        assert_one(0, BUFLEN - 100, [BUFLEN - 100..BUFLEN, 0..0]);
        assert_one(0, BUFLEN - 1, [BUFLEN - 1..BUFLEN, 0..0]);

        // start=N
        assert_one(100, 100, [200..BUFLEN, 0..100]);
        assert_one(100, BUFLEN - 100, [0..100, 0..0]);
        assert_one(100, BUFLEN - 101, [BUFLEN - 1..BUFLEN, 0..100]);

        // start=BUFLEN-1
        assert_one(BUFLEN - 1, 100, [99..BUFLEN - 1, 0..0]);
    }

    fn test_handshake() -> Vec<u8> {
        let mut hs = Vec::new();
        hs.push(19u8);
        hs.extend_from_slice(b"BitTorrent protocol");
        hs.extend_from_slice(&[0u8; 8]);
        hs.extend_from_slice(&[0xaa; 20]); // info hash
        hs.extend_from_slice(&[0xbb; 20]); // peer id
        hs
    }

    async fn read_handshake_written_as(chunks: Vec<Vec<u8>>) -> crate::Result<Handshake> {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let writer = async move {
            let (_r, mut w) = crate::test_support::tcp((Ipv4Addr::LOCALHOST, port))
                .await
                .unwrap();
            for c in chunks {
                w.write_all(&c).await.unwrap();
                w.flush().await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            // Keep the connection open until the reader is done.
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        let reader = async {
            let reader = listener.accept().await.unwrap().0.into_split().0;
            let mut reader: BoxAsyncReadVectored = Box::new(reader);
            let mut rb = ReadBuf::new();
            rb.read_handshake(&mut reader, Duration::from_secs(5)).await
        };
        let (h, ()) = tokio::join!(reader, writer);
        h
    }

    #[tokio::test]
    async fn handshake_split_across_reads_is_reassembled() {
        setup_test_logging();
        let hs = test_handshake();
        // Split in the middle of the info hash, and byte by byte for the first few.
        let mut chunks: Vec<Vec<u8>> = hs[..3].iter().map(|b| vec![*b]).collect();
        chunks.push(hs[3..40].to_vec());
        chunks.push(hs[40..].to_vec());
        let h = read_handshake_written_as(chunks).await.unwrap();
        assert_eq!(h.info_hash.0, [0xaa; 20]);
        assert_eq!(h.peer_id.0, [0xbb; 20]);
    }

    #[tokio::test]
    async fn handshake_cut_short_is_an_error() {
        setup_test_logging();
        let hs = test_handshake();
        let err = read_handshake_written_as(vec![hs[..40].to_vec()])
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::PeerDisconnectedReadingHandshake),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn can_read_long_metainfo_correctly() {
        setup_test_logging();
        let reader = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = reader.local_addr().unwrap().port();
        let mut writer = crate::test_support::tcp((Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap()
            .1;

        const ITERATIONS: u32 = 4096;

        let reader = async {
            let reader = reader.accept().await.unwrap().0.into_split().0;
            let mut reader: BoxAsyncReadVectored = Box::new(reader);
            let mut rb = ReadBuf::new();

            for piece in 0..ITERATIONS {
                let msg = rb
                    .read_message(
                        &mut reader,
                        Duration::from_millis(1000),
                        None,
                        &mut Unlimited,
                    )
                    .await
                    .unwrap();
                let utdata = match msg {
                    Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Data(utdata))) => {
                        utdata
                    }
                    other => panic!("expected utdata, got {other:?}"),
                };
                assert_eq!(utdata.len(), CHUNK_SIZE as usize);
                assert_eq!(utdata.piece(), piece);
                #[allow(clippy::cast_possible_truncation)]
                let expected_byte = { piece as u8 };
                let all_good = utdata
                    .as_double_buf()
                    .get()
                    .into_iter()
                    .flatten()
                    .all(|b| *b == expected_byte);
                if !all_good {
                    panic!("broken data");
                }
            }
        };

        let pext = PeerExtendedMessageIds::my();
        let writer = async {
            let mut sbuf = [0u8; MAX_MSG_LEN];
            let mut pbuf = [0u8; CHUNK_SIZE as usize];
            for piece in 0..ITERATIONS {
                #[allow(clippy::cast_possible_truncation)]
                for b in pbuf.iter_mut() {
                    *b = piece as u8;
                }
                let len = Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Data(
                    UtMetadataData::from_bytes(piece, CHUNK_SIZE * ITERATIONS, pbuf[..].into()),
                )))
                .serialize(&mut sbuf, &|| pext)
                .unwrap();
                writer.write_all(&sbuf[..len]).await.unwrap();
            }
        };

        tokio::join!(reader, writer);
    }

    // A reader of `bytes`, all written before the first read.
    async fn reader_of(bytes: &[u8]) -> BoxAsyncReadVectored {
        let (mut w, r) = tokio::io::duplex(bytes.len() + 1);
        w.write_all(bytes).await.unwrap();
        // The writer half stays open, as a peer's would.
        std::mem::forget(w);
        Box::new(r.into_vectored_compat())
    }

    // A bitfield longer than the buffer, of a torrent with that many pieces,
    // grows the buffer to its frame, and the buffer shrinks back once the
    // frame is read (libtorrent `receive_buffer::grow`, `normalize`).
    #[tokio::test(start_paused = true)]
    async fn a_bitfield_longer_than_the_buffer_is_read() {
        let pieces: u32 = 8 * (u32::try_from(BUFLEN).unwrap() + 1000);
        let bits = vec![0x5au8; (pieces / 8) as usize];
        let mut wire = vec![0u8; max_frame_len(Some(pieces)) + 9];
        let len = Message::Bitfield(ByteBuf(&bits))
            .serialize(&mut wire, &Default::default)
            .unwrap();
        let len = len
            + Message::Have(7)
                .serialize(&mut wire[len..], &Default::default)
                .unwrap();
        let mut reader = reader_of(&wire[..len]).await;

        let mut rb = ReadBuf::new();
        let timeout = Duration::from_secs(10);
        match rb
            .read_message(&mut reader, timeout, Some(pieces), &mut Unlimited)
            .await
        {
            Ok(Message::Bitfield(b)) => assert_eq!(b.as_ref(), &bits[..]),
            other => panic!("{other:?}"),
        }
        assert!(rb.buf.len() > BUFLEN);
        match rb
            .read_message(&mut reader, timeout, Some(pieces), &mut Unlimited)
            .await
        {
            Ok(Message::Have(7)) => {}
            other => panic!("{other:?}"),
        }
        assert_eq!(rb.buf.len(), BUFLEN);
    }

    // libtorrent `bt_peer_connection::on_extended`: an extended frame too
    // short to hold its extended message id is malformed, and ends the
    // connection; the bytes behind it are not waited for.
    #[tokio::test(start_paused = true)]
    async fn an_extended_frame_without_its_id_is_refused_not_waited_on() {
        let mut wire = vec![0, 0, 0, 1, 20];
        wire.extend_from_slice(&[0, 0, 0, 5, 4, 0, 0, 0, 7]);
        let mut reader = reader_of(&wire).await;
        let mut rb = ReadBuf::new();
        let res = rb
            .read_message(&mut reader, Duration::from_secs(10), None, &mut Unlimited)
            .await;
        assert!(
            matches!(
                res,
                Err(crate::Error::Deserialize(
                    MessageDeserializeError::IncorrectMsgLen { .. }
                ))
            ),
            "{res:?}"
        );
    }

    // Admits `each` bytes a read, and counts the reads.
    struct Drip {
        each: usize,
        reads: usize,
    }

    impl ReadQuota for Drip {
        async fn admit(&mut self, _need: usize, room: usize) -> usize {
            self.reads += 1;
            self.each.min(room)
        }
    }

    // Each read takes off the socket no more than the quota admits, though
    // the whole message waits there (libtorrent
    // `peer_connection::setup_receive`).
    #[tokio::test(start_paused = true)]
    async fn a_read_takes_only_what_the_quota_admits() {
        let mut wire = [0u8; 9];
        let len = Message::Have(7)
            .serialize(&mut wire, &Default::default)
            .unwrap();
        assert_eq!(len, 9);
        let mut reader = reader_of(&wire).await;
        let mut rb = ReadBuf::new();
        let mut quota = Drip { each: 2, reads: 0 };
        match rb
            .read_message(&mut reader, Duration::from_secs(10), None, &mut quota)
            .await
        {
            Ok(Message::Have(7)) => {}
            other => panic!("{other:?}"),
        }
        assert_eq!(quota.reads, 5);
        assert_eq!((rb.consumed(), rb.buffered()), (9, 0));
    }

    // A frame past the packet limit, or a bitfield of the wrong size for the
    // torrent, ends the read on its header: the buffer never grows for it
    // (libtorrent `bt_peer_connection::on_receive`, `on_bitfield`).
    #[tokio::test(start_paused = true)]
    async fn a_frame_past_its_rule_is_refused_before_it_is_read() {
        let timeout = Duration::from_secs(10);
        for (len_prefix, num_pieces) in [
            (u32::try_from(MAX_PACKET_LEN).unwrap() + 1, None),
            (
                1 + u32::try_from(BUFLEN).unwrap() * 2,
                Some(8 * u32::try_from(BUFLEN).unwrap() * 2 + 8),
            ),
        ] {
            let mut header = len_prefix.to_be_bytes().to_vec();
            header.push(5); // bitfield
            let mut reader = reader_of(&header).await;
            let mut rb = ReadBuf::new();
            let err = rb
                .read_message(&mut reader, timeout, num_pieces, &mut Unlimited)
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    crate::Error::Deserialize(
                        MessageDeserializeError::PacketTooLarge(..)
                            | MessageDeserializeError::IncorrectMsgLen { .. }
                    )
                ),
                "{err:?}"
            );
            assert_eq!(rb.buf.len(), BUFLEN);
        }
    }

    // A peer that keeps the socket full grows the buffer, a read at a time,
    // to GROWN_MAX, and every message still comes out whole and in order.
    #[tokio::test(start_paused = true)]
    async fn a_full_socket_grows_the_buffer() {
        let count = 100_000u32;
        let mut wire = vec![0u8; 9 * count as usize];
        for (i, frame) in wire.chunks_mut(9).enumerate() {
            Message::Have(u32::try_from(i).unwrap())
                .serialize(frame, &Default::default)
                .unwrap();
        }
        let mut reader = reader_of(&wire).await;
        let mut rb = ReadBuf::new();
        let mut largest = 0;
        for i in 0..count {
            match rb
                .read_message(&mut reader, Duration::from_secs(10), None, &mut Unlimited)
                .await
            {
                Ok(Message::Have(n)) => assert_eq!(n, i),
                other => panic!("{other:?}"),
            }
            largest = largest.max(rb.buf.len());
        }
        assert_eq!(largest, GROWN_MAX);
    }

    // Reads that each take a quarter of the buffer or less halve it, back
    // to BUFLEN.
    #[tokio::test(start_paused = true)]
    async fn short_reads_shrink_the_buffer() {
        let mut wire = vec![0u8; 9 * 8];
        for (i, frame) in wire.chunks_mut(9).enumerate() {
            Message::Have(u32::try_from(i).unwrap())
                .serialize(frame, &Default::default)
                .unwrap();
        }
        let mut reader = reader_of(&wire).await;
        let mut rb = ReadBuf::new();
        rb.relocate(GROWN_MAX);
        rb.wanted = GROWN_MAX;
        let mut quota = Drip { each: 9, reads: 0 };
        for i in 0..8 {
            match rb
                .read_message(&mut reader, Duration::from_secs(10), None, &mut quota)
                .await
            {
                Ok(Message::Have(n)) => assert_eq!(n, i),
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(rb.buf.len(), BUFLEN);
    }
}
