// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{collections::VecDeque, io::IoSliceMut, time::Duration};

use crate::disk::{BLOCK_LEN, Block, BlockBuf, BlockPool};
use crate::peer_protocol::{
    Handshake, Message, MessageDeserializeError, PIECE_MESSAGE_PREAMBLE_LEN, PartialBlock,
    extended::PeerExtendedMessageIds, may_start_piece, piece_header,
};
use crate::{Error, Result};
use crate::{
    peer_connection::with_timeout,
    type_aliases::BoxAsyncReadVectored,
    vectored_traits::{AsyncReadVectored, AsyncReadVectoredExt},
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
    // Where the blocks of Piece messages are received, when they are taken
    // apart from the other messages.
    blocks: Option<Box<Blocks>>,
    // The socket reads since they were last taken.
    socket: SocketReads,
}

/// What a connection's socket reads did: how many returned bytes, how many
/// times the buffer was made larger for one, and the packets the bytes of
/// each are taken to have come in, over IPv4 and over IPv6
/// ([`ip_packets`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SocketReads {
    pub calls: u64,
    pub growths: u64,
    pub packets: [u64; 2],
}

/// The bytes of the IP and TCP headers of a packet, over IPv4 and over
/// IPv6, as libtorrent estimates them (`stat::trancieve_ip_packet`,
/// stat.hpp:150 in v2.0.15: a 20 or 40 byte IP header and 20 more).
pub(crate) const IP_HEADER: [u64; 2] = [40, 60];

/// The packets `bytes` moved by one read or write are taken to have crossed
/// in, over IPv4 and over IPv6: one for each 1500 byte packet's worth less
/// its header, and one at least (libtorrent `stat::trancieve_ip_packet`).
pub(crate) fn ip_packets(bytes: usize) -> [u64; 2] {
    IP_HEADER.map(|header| (bytes as u64).div_ceil(1500 - header).max(1))
}

impl SocketReads {
    fn read(&mut self, bytes: usize) {
        self.calls += 1;
        for (packets, more) in self.packets.iter_mut().zip(ip_packets(bytes)) {
            *packets += more;
        }
    }
}

/// A Piece message's header: length, id, index and begin.
const HEADER: usize = PIECE_MESSAGE_PREAMBLE_LEN;

/// The most blocks one read offers buffers for: about as much as one read
/// into the buffer takes at its largest.
pub(crate) const MOST_SLOTS: usize = GROWN_MAX / BLOCK_LEN;

/// What a read returns: a message read in the buffer, or a block received
/// into its own buffer.
pub enum Incoming<'a> {
    Message(Message<'a>),
    Block(Block),
}

/// The blocks of Piece messages, each received into a buffer of its own.
///
/// A read offers, after the rest of the block being received, a run of
/// slots, each a Piece header's room and then a block buffer: when the
/// messages that follow are blocks, as they are while a peer uploads to us,
/// each block lands in its own buffer straight off the socket. Bytes that
/// turn out to be another message are moved into the connection's buffer,
/// as are a header cut short by the end of a read and a block whose header
/// was read into that buffer: those are the only bytes of a block copied
/// before the block is written.
///
/// The slots' buffers are taken from the pool for one poll of the socket
/// only: a poll that finds nothing gives them all back, and after a read
/// those it put no block in go back too, so a connection waiting on its
/// socket holds no block buffer but the one it is part way through
/// (libtorrent takes a disk buffer only for a block that arrived,
/// `mmap_disk_io::async_write`). A connection with no block requested and
/// none part way in offers no slots.
struct Blocks {
    pool: BlockPool,
    // The block being received, its header read.
    filling: Option<Filling>,
    // Blocks received whole and not yet read.
    ready: VecDeque<Block>,
    // The slots' buffers during one poll of the socket; empty otherwise.
    spares: Vec<BlockBuf>,
    // Whether blocks are requested of the peer and not yet received, as the
    // connection last said: slots are offered only then.
    expecting: bool,
    // The headers' room in the slots.
    heads: Box<[[u8; HEADER]; MOST_SLOTS]>,
    // Slots a read offers, from 1 to MOST_SLOTS: doubled after a read that
    // filled every slot, halved after one that filled a quarter of them or
    // less, or found another message in one.
    slots: usize,
    // Bytes of a read moved into the connection's buffer, or out of it into
    // a block's, and the bytes of the blocks received.
    moved: u64,
    received: u64,
}

struct Filling {
    index: u32,
    begin: u32,
    len: usize,
    got: usize,
    buf: BlockBuf,
}

impl Filling {
    fn into_block(self) -> Block {
        Block::new(self.index, self.begin, self.len, self.buf)
    }
}

/// How a read with slots starts.
#[derive(Clone, Copy)]
enum Lead {
    // The rest of the block being received.
    Filling(usize),
    // The rest of a Piece header the connection's buffer holds part of: the
    // first slot's header is read there.
    Prefix(usize),
    Nothing,
}

const MOST_SLICES: usize = 2 + 2 * MOST_SLOTS;

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
            blocks: None,
            socket: SocketReads::default(),
        }
    }

    /// The socket reads since the last call, when there were any: the
    /// connection adds them to the session's counters once per read of
    /// the socket, not once per message.
    pub fn take_socket_reads(&mut self) -> Option<SocketReads> {
        (self.socket.calls > 0).then(|| std::mem::take(&mut self.socket))
    }

    /// The blocks of Piece messages are received into buffers of `pool`'s,
    /// and read as [`Incoming::Block`] by [`Self::read_next`].
    pub fn receive_blocks_into(&mut self, pool: BlockPool) {
        self.blocks = Some(Box::new(Blocks {
            pool,
            filling: None,
            ready: VecDeque::new(),
            spares: Vec::with_capacity(MOST_SLOTS),
            expecting: true,
            heads: Box::new([[0u8; HEADER]; MOST_SLOTS]),
            slots: 1,
            moved: 0,
            received: 0,
        }));
    }

    /// Whether blocks are requested of the peer and not yet received: the
    /// reads that follow offer block buffers only then, or for the rest of
    /// a block part way in. Every block is read either way; one that comes
    /// unasked is read in the connection's buffer and moved to its own.
    pub fn expect_blocks(&mut self, expecting: bool) {
        if let Some(blocks) = self.blocks.as_deref_mut() {
            blocks.expecting = expecting;
        }
    }

    /// Bytes moved into the connection's buffer or out of it into a
    /// block's, and the bytes of the blocks received, so far.
    pub fn moved(&self) -> (u64, u64) {
        self.blocks
            .as_ref()
            .map_or((0, 0), |b| (b.moved, b.received))
    }

    /// The extensions we advertised on the connection, in our extended
    /// handshake's `m`; every one of ours until then.
    pub fn set_advertised(&mut self, advertised: PeerExtendedMessageIds) {
        self.advertised = advertised;
    }

    /// Bytes received and not yet read as a message.
    pub fn buffered(&self) -> usize {
        self.len
            + self.blocks.as_ref().map_or(0, |b| {
                b.filling.as_ref().map_or(0, |f| HEADER + f.got)
                    + b.ready.iter().map(|r| HEADER + r.len()).sum::<usize>()
            })
    }

    /// Bytes of the messages read so far.
    pub fn consumed(&self) -> u64 {
        self.consumed
    }

    /// The Piece message being read, when part of its block arrived and
    /// the rest did not.
    pub fn partial_block(&self) -> Option<PartialBlock> {
        if let Some(f) = self.blocks.as_ref().and_then(|b| b.filling.as_ref()) {
            return (f.got > 0).then(|| PartialBlock {
                index: f.index,
                begin: f.begin,
                len: u32::try_from(f.len).unwrap_or(u32::MAX),
                received: u32::try_from(f.got).unwrap_or(u32::MAX),
            });
        }
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
        if size > self.buf.len() {
            self.socket.growths += 1;
        }
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
                MessageDeserializeError::NotEnoughData(need, ..) => {
                    self.read_more(conn, timeout, quota, need).await?;
                }
                MessageDeserializeError::NeedContiguous => {
                    self.make_contiguous()?;
                    continue;
                }
                e => return Err(Error::Deserialize(e)),
            }
        }
    }

    /// Reads at least `need` more bytes into the buffer.
    async fn read_more(
        &mut self,
        conn: &mut BoxAsyncReadVectored,
        timeout: Duration,
        quota: &mut impl ReadQuota,
        mut need_additional_bytes: usize,
    ) -> Result<()> {
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
            self.socket.read(size);
            self.len += size;
            need_additional_bytes = need_additional_bytes.saturating_sub(size)
        }
        Ok(())
    }

    /// The next message or block, as [`Self::read_message`] reads the next
    /// message, the blocks of Piece messages received into their own
    /// buffers once [`Self::receive_blocks_into`] named a pool.
    pub async fn read_next(
        &mut self,
        conn: &mut BoxAsyncReadVectored,
        timeout: Duration,
        num_pieces: Option<u32>,
        quota: &mut impl ReadQuota,
    ) -> Result<Incoming<'_>> {
        if self.blocks.is_none() {
            return self
                .read_message(conn, timeout, num_pieces, quota)
                .await
                .map(Incoming::Message);
        }
        if self.buf.len() != self.wanted && self.len <= self.wanted {
            self.relocate(self.wanted);
        }
        loop {
            let blocks = self
                .blocks
                .as_deref_mut()
                .expect("blocks are received apart");
            if let Some(block) = blocks.ready.pop_front() {
                self.consumed += (HEADER + block.len()) as u64;
                return Ok(Incoming::Block(block));
            }
            // With no block expected the next message is read in the
            // connection's buffer, a block that comes unasked too.
            let expecting = blocks.expecting;
            if blocks.filling.is_none() && (self.len > 0 || !expecting) {
                if self.len >= HEADER {
                    if let Some(header) = piece_header(&self.front()) {
                        self.fill_from_buffer(header);
                        continue;
                    }
                } else if expecting && may_start_piece(&self.front()[..self.len]) {
                    self.read_blocks(conn, timeout, quota).await?;
                    continue;
                }
                let err = {
                    // The borrow checker workaround of `read_message`, for
                    // the same reason.
                    let this = unsafe { &mut *(self as *mut Self) };
                    let (first, second) = as_slices!(this);
                    match Message::deserialize_advertised(
                        first,
                        second,
                        num_pieces,
                        &this.advertised,
                    ) {
                        Ok((msg, size)) => {
                            advance!(this, size);
                            this.consumed += size as u64;
                            return Ok(Incoming::Message(msg));
                        }
                        Err(e) => e,
                    }
                };
                match err {
                    MessageDeserializeError::NotEnoughData(need, ..) => {
                        self.read_more(conn, timeout, quota, need).await?;
                    }
                    MessageDeserializeError::NeedContiguous => {
                        self.make_contiguous()?;
                    }
                    e => return Err(Error::Deserialize(e)),
                }
                continue;
            }
            self.read_blocks(conn, timeout, quota).await?;
        }
    }

    /// The first bytes held, up to a Piece header's length.
    fn front(&self) -> [u8; HEADER] {
        let mut header = [0u8; HEADER];
        let (first, second) = as_slices!(self);
        for (to, from) in header.iter_mut().zip(first.iter().chain(second)) {
            *to = *from;
        }
        header
    }

    /// A Piece header is at the front of the buffer: its block moves into a
    /// block buffer, as much of it as the buffer holds.
    fn fill_from_buffer(&mut self, (index, begin, len): (u32, u32, usize)) {
        self.advance(HEADER);
        let got = len.min(self.len);
        let mut buf = {
            let blocks = self
                .blocks
                .as_deref_mut()
                .expect("blocks are received apart");
            blocks.spares.pop().unwrap_or_else(|| blocks.pool.take())
        };
        let (first, second) = as_slices!(self);
        let a = got.min(first.len());
        buf.as_mut_slice()[..a].copy_from_slice(&first[..a]);
        buf.as_mut_slice()[a..got].copy_from_slice(&second[..got - a]);
        advance!(self, got);
        let blocks = self
            .blocks
            .as_deref_mut()
            .expect("blocks are received apart");
        blocks.moved += got as u64;
        blocks.received += got as u64;
        let filling = Filling {
            index,
            begin,
            len,
            got,
            buf,
        };
        if got == len {
            blocks.ready.push_back(filling.into_block());
        } else {
            blocks.filling = Some(filling);
        }
    }

    /// One read, offering the rest of the block being received and then,
    /// while blocks are expected, slots for the blocks that follow.
    async fn read_blocks(
        &mut self,
        conn: &mut BoxAsyncReadVectored,
        timeout: Duration,
        quota: &mut impl ReadQuota,
    ) -> Result<()> {
        let blocks = self
            .blocks
            .as_deref_mut()
            .expect("blocks are received apart");
        let slots = if blocks.expecting { blocks.slots } else { 0 };
        let lead = match &blocks.filling {
            Some(f) => Lead::Filling(f.len - f.got),
            None if self.len > 0 => Lead::Prefix(HEADER - self.len),
            None => Lead::Nothing,
        };
        let (lead_len, need) = match lead {
            Lead::Filling(n) | Lead::Prefix(n) => (n, n),
            Lead::Nothing => (0, HEADER),
        };
        let room = lead_len + slots * (HEADER + BLOCK_LEN);
        let admitted = quota.admit(need, room).await;

        // How much of the lead and of each slot the read offers.
        let lead_offered = lead_len.min(admitted);
        let mut budget = admitted - lead_offered;
        let mut offered = [(0usize, 0usize); MOST_SLOTS];
        let mut count = 0;
        while count < slots && budget > 0 {
            let prefixed = count == 0 && matches!(lead, Lead::Prefix(_));
            let h = if prefixed { 0 } else { HEADER.min(budget) };
            budget -= h;
            let b = BLOCK_LEN.min(budget);
            budget -= b;
            offered[count] = (h, b);
            count += 1;
        }
        let offered = &offered[..count];
        let size = with_timeout(
            "reading",
            timeout,
            std::future::poll_fn(|cx| self.poll_slots(cx, conn, lead, lead_offered, offered))
                .map_err(Error::Write),
        )
        .await?;
        if size == 0 {
            return Err(Error::PeerDisconnected);
        }
        self.socket.read(size);
        self.take_read(lead, offered, size, room == admitted);
        Ok(())
    }

    /// One poll of the socket for the slices `read_blocks` offers. The
    /// slots' buffers are taken for this poll alone, under one lock of the
    /// pool: when it reads nothing they all go back, under one more, before
    /// the connection waits (a poll that is pending read no byte).
    fn poll_slots(
        &mut self,
        cx: &mut std::task::Context<'_>,
        conn: &mut BoxAsyncReadVectored,
        lead: Lead,
        lead_offered: usize,
        offered: &[(usize, usize)],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let Self {
            buf,
            start,
            len,
            blocks,
            ..
        } = self;
        let blocks = blocks.as_deref_mut().expect("blocks are received apart");
        blocks.pool.fill(&mut blocks.spares, offered.len());
        let polled = {
            let mut slices: [IoSliceMut<'_>; MOST_SLICES] =
                std::array::from_fn(|_| IoSliceMut::new(&mut []));
            let mut used = 0;
            match lead {
                Lead::Filling(_) => {
                    let f = blocks.filling.as_mut().expect("a block is being received");
                    let rest = &mut f.buf.as_mut_slice()[f.got..f.got + lead_offered];
                    slices[used] = IoSliceMut::new(rest);
                    used += 1;
                }
                Lead::Prefix(_) => {
                    for s in unfilled(buf, *start, *len, lead_offered) {
                        slices[used] = s;
                        used += 1;
                    }
                }
                Lead::Nothing => {}
            }
            let slots = blocks.heads.iter_mut().zip(blocks.spares.iter_mut());
            for ((head, spare), &(h, b)) in slots.zip(offered) {
                if h > 0 {
                    slices[used] = IoSliceMut::new(&mut head[..h]);
                    used += 1;
                }
                if b > 0 {
                    slices[used] = IoSliceMut::new(&mut spare.as_mut_slice()[..b]);
                    used += 1;
                }
            }
            std::pin::Pin::new(&mut *conn).poll_read_vectored(cx, &mut slices[..used])
        };
        if !matches!(polled, std::task::Poll::Ready(Ok(n)) if n > 0) {
            blocks.pool.put_back(&mut blocks.spares);
        }
        polled
    }

    /// Sorts the `size` bytes a read put in the slices `read_blocks` offered:
    /// the rest of the block being received, then each slot's header and
    /// block, in order.
    fn take_read(
        &mut self,
        lead: Lead,
        offered: &[(usize, usize)],
        size: usize,
        all_offered: bool,
    ) {
        let mut left = size;
        match lead {
            Lead::Filling(rest) => {
                let blocks = self
                    .blocks
                    .as_deref_mut()
                    .expect("blocks are received apart");
                let t = left.min(rest);
                left -= t;
                blocks.received += t as u64;
                let f = blocks.filling.as_mut().expect("a block is being received");
                f.got += t;
                if f.got == f.len {
                    let f = blocks.filling.take().expect("a block is being received");
                    blocks.ready.push_back(f.into_block());
                }
            }
            Lead::Prefix(p) => {
                let t = left.min(p);
                left -= t;
                self.len += t;
                if t < p {
                    // The header is not whole yet: no slot holds a byte.
                    let blocks = self
                        .blocks
                        .as_deref_mut()
                        .expect("blocks are received apart");
                    blocks.pool.put_back(&mut blocks.spares);
                    return;
                }
            }
            Lead::Nothing => {}
        }
        let offered_total: usize = offered.iter().map(|(h, b)| h + b).sum();

        // The slots that hold blocks: the first `taken`, each one's header,
        // and how much of the last arrived. Then where the bytes that are not
        // a block begin, if any: a slot and an offset in it, counting its
        // header's room first.
        let mut found = [(0u32, 0u32, 0usize); MOST_SLOTS];
        let mut taken = 0;
        let mut last_got = 0;
        let mut other: Option<(usize, usize)> = None;
        for (i, &(h, b)) in offered.iter().enumerate() {
            if left == 0 {
                break;
            }
            let prefixed = i == 0 && matches!(lead, Lead::Prefix(_));
            let header = if prefixed {
                self.front()
            } else if left < h || h < HEADER {
                // A header cut short by the end of the read: the
                // buffer's next read completes it.
                other = Some((i, 0));
                break;
            } else {
                self.blocks
                    .as_deref()
                    .expect("blocks are received apart")
                    .heads[i]
            };
            let in_head = if prefixed { 0 } else { HEADER };
            match piece_header(&header) {
                Some((index, begin, len)) => {
                    let got = (left - in_head).min(b);
                    if prefixed {
                        self.advance(HEADER);
                    }
                    found[taken] = (index, begin, len);
                    taken += 1;
                    last_got = got;
                    left -= in_head + got;
                    if got > len {
                        // A block shorter than its slot, and more after it.
                        other = Some((i, in_head + len));
                        left += got - len;
                        last_got = len;
                        break;
                    }
                    if got < len {
                        break;
                    }
                }
                None => {
                    other = Some((i, 0));
                    break;
                }
            }
        }

        // Bytes after the blocks move into the buffer, in order: `left` of
        // them, from `other` on.
        if let Some((slot, offset)) = other {
            let blocks = self
                .blocks
                .as_deref_mut()
                .expect("blocks are received apart");
            blocks.slots = (blocks.slots / 2).max(1);
            blocks.moved += left as u64;
            let mut skip = offset;
            for (j, &(h, b)) in offered.iter().enumerate().skip(slot) {
                if left == 0 {
                    break;
                }
                let prefixed = j == 0 && matches!(lead, Lead::Prefix(_));
                let h = if prefixed { 0 } else { h };
                for part in 0..2 {
                    let room = if part == 0 { h } else { b };
                    let from = skip.min(room);
                    skip -= from;
                    let n = (room - from).min(left);
                    if n == 0 {
                        continue;
                    }
                    left -= n;
                    let Self {
                        buf,
                        start,
                        len,
                        blocks,
                        ..
                    } = &mut *self;
                    let blocks = blocks.as_deref().expect("blocks are received apart");
                    let bytes: &[u8] = if part == 0 {
                        &blocks.heads[j][from..from + n]
                    } else {
                        &blocks.spares[j].as_slice()[from..from + n]
                    };
                    ring_push(buf, start, len, bytes);
                }
            }
        }

        let blocks = self
            .blocks
            .as_deref_mut()
            .expect("blocks are received apart");
        for (k, buf) in blocks.spares.drain(..taken).enumerate() {
            let (index, begin, len) = found[k];
            let got = if k + 1 == taken { last_got } else { len };
            blocks.received += got as u64;
            let filling = Filling {
                index,
                begin,
                len,
                got,
                buf,
            };
            if got == len {
                blocks.ready.push_back(filling.into_block());
            } else {
                blocks.filling = Some(filling);
            }
        }
        if other.is_none() && !offered.is_empty() {
            if all_offered && size >= offered_total {
                blocks.slots = (blocks.slots * 2).min(MOST_SLOTS);
            } else if size <= offered_total / 4 {
                blocks.slots = (blocks.slots / 2).max(1);
            }
        }
        // The slots no block landed in give their buffers back before the
        // connection reads again.
        blocks.pool.put_back(&mut blocks.spares);
    }
}

/// Appends `bytes` to the `len` bytes held from `start` in the ring `buf`,
/// growing it to fit them (it shrinks back between messages, as
/// `ReadBuf::read_message` has it).
fn ring_push(buf: &mut Box<[u8]>, start: &mut usize, len: &mut usize, bytes: &[u8]) {
    if *len + bytes.len() > buf.len() {
        let mut new = vec![0u8; *len + bytes.len()].into_boxed_slice();
        let size = buf.len();
        let first = (*len).min(size - *start);
        new[..first].copy_from_slice(&buf[*start..*start + first]);
        new[first..*len].copy_from_slice(&buf[..*len - first]);
        *buf = new;
        *start = 0;
    }
    let size = buf.len();
    let at = (*start + *len) % size;
    let first = bytes.len().min(size - at);
    buf[at..at + first].copy_from_slice(&bytes[..first]);
    buf[..bytes.len() - first].copy_from_slice(&bytes[first..]);
    *len += bytes.len();
}

/// The room after the `len` bytes held from `start` in the ring `buf`, at
/// most `max` bytes of it.
fn unfilled(buf: &mut [u8], start: usize, len: usize, max: usize) -> [IoSliceMut<'_>; 2] {
    let size = buf.len();
    let write_start = (start.saturating_add(len)) % size;
    let available_len = size.saturating_sub(len).min(max);
    let first_len = (size.saturating_sub(write_start)).min(available_len);
    let second_len = available_len.saturating_sub(first_len);
    let (second, first) = buf.split_at_mut(write_start);
    let first_len = first_len.min(first.len());
    let second_len = second_len.min(second.len());
    [
        IoSliceMut::new(&mut first[..first_len]),
        IoSliceMut::new(&mut second[..second_len]),
    ]
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

    use super::{BUFLEN, GROWN_MAX, HEADER, Incoming, MOST_SLOTS, ReadBuf, ReadQuota, Unlimited};
    use crate::disk::BlockPool;
    use crate::peer_protocol::Piece;

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

    // A reader of `data` that fills every slice it is given, up to `cap`
    // bytes a read, as a socket does.
    struct Script {
        data: Vec<u8>,
        at: usize,
        cap: usize,
        reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl tokio::io::AsyncRead for Script {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let n = buf.remaining().min(this.cap).min(this.data.len() - this.at);
            buf.put_slice(&this.data[this.at..this.at + n]);
            this.at += n;
            std::task::Poll::Ready(Ok(()))
        }
    }

    impl crate::vectored_traits::AsyncReadVectored for Script {
        fn poll_read_vectored(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            vec: &mut [std::io::IoSliceMut<'_>],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            this.reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut left = this.cap.min(this.data.len() - this.at);
            let mut total = 0;
            for s in vec.iter_mut() {
                let n = s.len().min(left);
                s[..n].copy_from_slice(&this.data[this.at..this.at + n]);
                this.at += n;
                left -= n;
                total += n;
            }
            std::task::Poll::Ready(Ok(total))
        }
    }

    enum Sent {
        Block(u32, Vec<u8>),
        Have(u32),
    }

    fn wire_of(sent: &[Sent]) -> Vec<u8> {
        let mut wire = Vec::new();
        let mut frame = vec![0u8; MAX_MSG_LEN];
        for s in sent {
            let len = match s {
                Sent::Block(i, data) => Message::Piece(Piece::from_data(*i, 0, data))
                    .serialize(&mut frame, &Default::default),
                Sent::Have(i) => Message::Have(*i).serialize(&mut frame, &Default::default),
            }
            .unwrap();
            wire.extend_from_slice(&frame[..len]);
        }
        wire
    }

    fn block_of(i: u32, len: usize) -> Vec<u8> {
        (0..len)
            .map(|k| k.to_le_bytes()[0] ^ i.wrapping_mul(2_654_435_761).to_le_bytes()[k % 4])
            .collect()
    }

    // Reads `sent` off a socket that hands at most `cap` bytes a read, and
    // checks every message comes out whole and in order. Each block is let go
    // once read, as a written block's buffer is: with nothing left buffered,
    // the connection holds no block buffer. Returns the bytes moved, the
    // pool's allocations and the reads.
    async fn read_all(sent: &[Sent], cap: usize) -> (u64, usize, usize) {
        let pool = BlockPool::new(crate::disk::MAX_QUEUED_DISK_BYTES);
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut reader: BoxAsyncReadVectored = Box::new(Script {
            data: wire_of(sent),
            at: 0,
            cap,
            reads: reads.clone(),
        });
        let mut rb = ReadBuf::new();
        rb.receive_blocks_into(pool.clone());
        for s in sent {
            let got = rb
                .read_next(&mut reader, Duration::from_secs(10), None, &mut Unlimited)
                .await
                .unwrap();
            match (s, got) {
                (Sent::Block(i, data), Incoming::Block(b)) => {
                    assert_eq!((b.index, b.begin), (*i, 0));
                    assert_eq!(b.data(), &data[..], "block {i}");
                }
                (Sent::Have(i), Incoming::Message(Message::Have(n))) => assert_eq!(*i, n),
                (_, Incoming::Block(b)) => panic!("unexpected {b:?}"),
                (_, Incoming::Message(m)) => panic!("unexpected {m:?}"),
            }
            if rb.buffered() == 0 {
                assert_eq!(pool.receiving(), 0, "cap {cap}");
            }
        }
        assert_eq!(rb.consumed(), script_len(sent));
        assert_eq!(rb.buffered(), 0);
        assert_eq!(pool.receiving(), 0, "cap {cap}");
        let (moved, received) = rb.moved();
        let blocks: u64 = sent
            .iter()
            .map(|s| match s {
                Sent::Block(_, d) => d.len() as u64,
                Sent::Have(_) => 0,
            })
            .sum();
        assert_eq!(received, blocks);
        drop(rb);
        (
            moved,
            pool.allocated(),
            reads.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    fn script_len(sent: &[Sent]) -> u64 {
        wire_of(sent).len() as u64
    }

    // Blocks back to back land in their own buffers off the socket: no byte
    // of them is copied, the reads grow to many blocks each, and the pool
    // allocates no more than one read's buffers.
    #[tokio::test(start_paused = true)]
    async fn blocks_back_to_back_are_received_into_their_own_buffers() {
        let sent: Vec<_> = (0..400)
            .map(|i| Sent::Block(i, block_of(i, 16384)))
            .collect();
        let (moved, allocated, reads) = read_all(&sent, usize::MAX).await;
        assert_eq!(moved, 0);
        assert!(allocated <= 2 * MOST_SLOTS, "{allocated}");
        assert!(reads < 40, "{reads}");
    }

    // Reads cut anywhere: only the headers cut short move, and every block
    // comes out whole.
    #[tokio::test(start_paused = true)]
    async fn reads_cut_anywhere_move_only_cut_headers() {
        let sent: Vec<_> = (0..64)
            .map(|i| Sent::Block(i, block_of(i, 16384)))
            .collect();
        for cap in [1, 7, 13, 14, 1000, 16397, 16398, 40_000, 100_003] {
            let (moved, _, reads) = read_all(&sent, cap).await;
            assert!(
                moved <= (HEADER as u64) * reads as u64,
                "cap {cap}: {moved}"
            );
        }
    }

    // Other messages between blocks, and blocks shorter than a slot, move
    // into the buffer and come out in order, at any read size.
    #[tokio::test(start_paused = true)]
    async fn other_messages_between_blocks_come_out_in_order() {
        let mut sent = Vec::new();
        for i in 0..80u32 {
            let len = if i % 9 == 4 {
                1 + (i as usize * 97) % 16384
            } else {
                16384
            };
            sent.push(Sent::Block(i, block_of(i, len)));
            if i % 7 == 3 {
                sent.push(Sent::Have(i));
                sent.push(Sent::Have(i + 1000));
            }
        }
        for cap in [3, 13, 500, 16397, 70_000, usize::MAX] {
            read_all(&sent, cap).await;
        }
    }

    // A connection that ends inside a block reports the block cut short.
    #[tokio::test(start_paused = true)]
    async fn a_block_cut_short_is_reported() {
        let data = block_of(5, 16384);
        let wire = wire_of(&[Sent::Block(5, data)]);
        let mut reader = reader_of(&wire[..HEADER + 1000]).await;
        let mut rb = ReadBuf::new();
        rb.receive_blocks_into(BlockPool::new(crate::disk::MAX_QUEUED_DISK_BYTES));
        let res = rb
            .read_next(&mut reader, Duration::from_secs(1), None, &mut Unlimited)
            .await;
        assert!(res.is_err());
        assert_eq!(
            rb.partial_block(),
            Some(crate::peer_protocol::PartialBlock {
                index: 5,
                begin: 0,
                len: 16384,
                received: 1000,
            })
        );
    }

    // A connection with blocks requested, waiting on its socket, holds no
    // block buffer: the poll that found nothing gave its slots' buffers
    // back. One with none requested takes none at all.
    #[tokio::test(start_paused = true)]
    async fn a_connection_waiting_on_its_socket_holds_no_block_buffer() {
        use futures::FutureExt;
        for expecting in [true, false] {
            let pool = BlockPool::new(crate::disk::MAX_QUEUED_DISK_BYTES);
            let mut reader = reader_of(&[]).await;
            let mut rb = ReadBuf::new();
            rb.receive_blocks_into(pool.clone());
            rb.expect_blocks(expecting);
            for _ in 0..3 {
                let read = rb
                    .read_next(&mut reader, Duration::from_secs(10), None, &mut Unlimited)
                    .now_or_never();
                assert!(read.is_none());
                assert_eq!(pool.receiving(), 0, "expecting {expecting}");
            }
            // The buffers a poll took went to the free list and were taken
            // again from it.
            let allocated = if expecting { 1 } else { 0 };
            assert_eq!(pool.allocated(), allocated, "expecting {expecting}");
        }
    }

    // Connections each part way through a block hold that one block's
    // buffer and no other while they wait; once whole, each block is read.
    #[tokio::test(start_paused = true)]
    async fn connections_mid_block_hold_at_most_one_block_buffer_each() {
        use futures::FutureExt;
        const N: u32 = 8;
        let pool = BlockPool::new(crate::disk::MAX_QUEUED_DISK_BYTES);
        let mut peers = Vec::new();
        for i in 0..N {
            let wire = wire_of(&[Sent::Block(i, block_of(i, 16384))]);
            let cut = HEADER + 1000 * (i as usize + 1);
            let (mut w, r) = tokio::io::duplex(wire.len() + 1);
            w.write_all(&wire[..cut]).await.unwrap();
            let mut reader: BoxAsyncReadVectored = Box::new(r.into_vectored_compat());
            let mut rb = ReadBuf::new();
            rb.receive_blocks_into(pool.clone());
            let read = rb
                .read_next(&mut reader, Duration::from_secs(10), None, &mut Unlimited)
                .now_or_never();
            assert!(read.is_none());
            assert_eq!(rb.buffered(), cut);
            assert_eq!(
                rb.partial_block().map(|p| p.received),
                Some(u32::try_from(cut - HEADER).unwrap())
            );
            assert_eq!(pool.receiving(), u64::from(i + 1));
            peers.push((w, reader, rb, wire, cut));
        }
        let mut blocks = Vec::new();
        for (i, (w, reader, rb, wire, cut)) in peers.iter_mut().enumerate() {
            w.write_all(&wire[*cut..]).await.unwrap();
            match rb
                .read_next(reader, Duration::from_secs(10), None, &mut Unlimited)
                .await
                .unwrap()
            {
                Incoming::Block(b) => {
                    let i = u32::try_from(i).unwrap();
                    assert_eq!(b.data(), &block_of(i, 16384)[..]);
                    blocks.push(b);
                }
                Incoming::Message(m) => panic!("{m:?}"),
            }
            assert_eq!(pool.receiving(), u64::from(N));
        }
        assert_eq!(rb_moved(&peers), 0);
        drop(blocks);
        assert_eq!(pool.receiving(), 0);
    }

    // The bytes of blocks the connections moved, all told.
    fn rb_moved<T, U, V, W>(peers: &[(T, U, ReadBuf, V, W)]) -> u64 {
        peers.iter().map(|(_, _, rb, _, _)| rb.moved().0).sum()
    }

    // A block the peer sends unasked is read whole all the same, through
    // the connection's buffer, and no slot is offered for it.
    #[tokio::test(start_paused = true)]
    async fn a_block_unasked_is_read_through_the_buffer() {
        let sent: Vec<_> = (0..4).map(|i| Sent::Block(i, block_of(i, 16384))).collect();
        let pool = BlockPool::new(crate::disk::MAX_QUEUED_DISK_BYTES);
        let mut reader = reader_of(&wire_of(&sent)).await;
        let mut rb = ReadBuf::new();
        rb.receive_blocks_into(pool.clone());
        rb.expect_blocks(false);
        for s in &sent {
            let Sent::Block(i, data) = s else {
                unreachable!()
            };
            match rb
                .read_next(&mut reader, Duration::from_secs(10), None, &mut Unlimited)
                .await
                .unwrap()
            {
                Incoming::Block(b) => assert_eq!((b.index, b.data()), (*i, &data[..])),
                Incoming::Message(m) => panic!("{m:?}"),
            }
        }
        assert_eq!(pool.receiving(), 0);
        assert!(rb.moved().0 > 0);
    }

    // A reader of `data` whose first read returns `first` bytes, and every
    // later one all that is left.
    struct TwoReads {
        data: Vec<u8>,
        at: usize,
        first: usize,
    }

    impl tokio::io::AsyncRead for TwoReads {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            unreachable!("read vectored")
        }
    }

    impl crate::vectored_traits::AsyncReadVectored for TwoReads {
        fn poll_read_vectored(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            vec: &mut [std::io::IoSliceMut<'_>],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            let end = if this.at == 0 {
                this.first
            } else {
                this.data.len()
            };
            let mut total = 0;
            for s in vec.iter_mut() {
                let n = s.len().min(end - this.at);
                s[..n].copy_from_slice(&this.data[this.at..this.at + n]);
                this.at += n;
                total += n;
            }
            std::task::Poll::Ready(Ok(total))
        }
    }

    // A first read that ends at any byte of a block's message, or of the
    // short block and the Have behind it, leaves every block whole, on a
    // plain connection and under RC4.
    #[tokio::test(start_paused = true)]
    async fn a_read_cut_at_every_offset_leaves_every_block_whole() {
        use crate::mse::{Rc4, TestDecryptingReader as DecryptingReader};
        let sent = [
            Sent::Block(3, block_of(3, 16384)),
            Sent::Block(4, block_of(4, 1000)),
            Sent::Have(9),
        ];
        let plain = wire_of(&sent);
        let mut encrypted = plain.clone();
        Rc4::new_discard_1024(b"cut key").apply(&mut encrypted);
        for rc4 in [false, true] {
            let pool = BlockPool::new(crate::disk::MAX_QUEUED_DISK_BYTES);
            for first in 1..plain.len() {
                let script = TwoReads {
                    data: if rc4 {
                        encrypted.clone()
                    } else {
                        plain.clone()
                    },
                    at: 0,
                    first,
                };
                let mut reader: BoxAsyncReadVectored = if rc4 {
                    DecryptingReader::new(
                        Vec::new(),
                        Box::new(script),
                        Some(Rc4::new_discard_1024(b"cut key")),
                    )
                    .into_boxed()
                } else {
                    Box::new(script)
                };
                let mut rb = ReadBuf::new();
                rb.receive_blocks_into(pool.clone());
                for s in &sent {
                    let got = rb
                        .read_next(&mut reader, Duration::from_secs(10), None, &mut Unlimited)
                        .await
                        .unwrap();
                    match (s, got) {
                        (Sent::Block(i, data), Incoming::Block(b)) => {
                            assert_eq!((b.index, b.begin), (*i, 0), "rc4 {rc4} cut {first}");
                            assert!(b.data() == &data[..], "rc4 {rc4} cut {first}: block {i}");
                        }
                        (Sent::Have(i), Incoming::Message(Message::Have(n))) => {
                            assert_eq!(*i, n, "rc4 {rc4} cut {first}")
                        }
                        (_, Incoming::Block(b)) => panic!("rc4 {rc4} cut {first}: {b:?}"),
                        (_, Incoming::Message(m)) => panic!("rc4 {rc4} cut {first}: {m:?}"),
                    }
                }
                assert_eq!(rb.buffered(), 0);
                // Each block let go, and no slot's buffer kept.
                assert_eq!(pool.receiving(), 0, "rc4 {rc4} cut {first}");
                // Only header and Have bytes a guess got wrong are moved,
                // never a block's data.
                assert!(
                    rb.moved().0 <= 3 * HEADER as u64,
                    "rc4 {rc4} cut {first} moved {:?}",
                    rb.moved()
                );
            }
            assert!(pool.allocated() <= 2 * MOST_SLOTS);
        }
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

    // A seeded random mix of blocks of any length and short messages
    // whose first bytes look like a Piece header's (Request and Cancel are 13
    // bytes long, as a Piece header is; KeepAlive is 4), read off a socket
    // whose every read returns a random amount, under a quota that admits a
    // random amount. Every message comes out whole, in order, and nothing is
    // left over.
    struct Rand(u64);
    impl Rand {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn upto(&mut self, n: usize) -> usize {
            1 + usize::try_from(self.next() % n as u64).unwrap()
        }
    }

    struct RandScript {
        data: Vec<u8>,
        at: usize,
        rand: Rand,
    }

    impl tokio::io::AsyncRead for RandScript {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let cap = this.rand.upto(40_000);
            let n = buf.remaining().min(cap).min(this.data.len() - this.at);
            buf.put_slice(&this.data[this.at..this.at + n]);
            this.at += n;
            std::task::Poll::Ready(Ok(()))
        }
    }

    impl crate::vectored_traits::AsyncReadVectored for RandScript {
        fn poll_read_vectored(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            vec: &mut [std::io::IoSliceMut<'_>],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            // Mostly small reads, sometimes large ones.
            let cap = if this.rand.next().is_multiple_of(4) {
                this.rand.upto(200_000)
            } else {
                this.rand.upto(40)
            };
            let mut left = cap.min(this.data.len() - this.at);
            let mut total = 0;
            for s in vec.iter_mut() {
                let n = s.len().min(left);
                s[..n].copy_from_slice(&this.data[this.at..this.at + n]);
                this.at += n;
                left -= n;
                total += n;
            }
            std::task::Poll::Ready(Ok(total))
        }
    }

    struct RandQuota(Rand);
    impl ReadQuota for RandQuota {
        async fn admit(&mut self, _need: usize, room: usize) -> usize {
            self.0.upto(room)
        }
    }

    #[derive(Debug, PartialEq)]
    enum Mixed {
        Block(u32, u32, Vec<u8>),
        Have(u32),
        KeepAlive,
        Unchoke,
        Request(u32, u32, u32),
        Cancel(u32, u32, u32),
        Port(u16),
    }

    #[tokio::test(start_paused = true)]
    async fn a_random_mix_at_random_read_sizes_comes_out_whole_and_in_order() {
        use crate::peer_protocol::Request;
        for seed in 1..=40u64 {
            let mut rand = Rand(0x9e37_79b9_7f4a_7c15 ^ seed.wrapping_mul(0x2545_f491_4f6c_dd1d));
            let mut sent = Vec::new();
            for i in 0..300u32 {
                let m = match rand.next() % 10 {
                    0..=4 => {
                        let len = if rand.next().is_multiple_of(3) {
                            rand.upto(16384)
                        } else {
                            16384
                        };
                        let begin = (rand.next() % 64) as u32 * 16384;
                        Mixed::Block(i, begin, block_of(i, len))
                    }
                    5 => Mixed::Have(i),
                    6 => Mixed::KeepAlive,
                    7 => Mixed::Unchoke,
                    8 => Mixed::Request(i, 16384 * (i % 7), 16384),
                    _ => match rand.next() % 2 {
                        0 => Mixed::Cancel(i, 0, 16384),
                        _ => Mixed::Port((i % 65_000) as u16),
                    },
                };
                sent.push(m);
            }
            let mut wire = Vec::new();
            let mut frame = vec![0u8; MAX_MSG_LEN];
            for m in &sent {
                let len = match m {
                    Mixed::Block(i, b, d) => Message::Piece(Piece::from_data(*i, *b, &d[..])),
                    Mixed::Have(i) => Message::Have(*i),
                    Mixed::KeepAlive => Message::KeepAlive,
                    Mixed::Unchoke => Message::Unchoke,
                    Mixed::Request(i, b, l) => Message::Request(Request::new(*i, *b, *l)),
                    Mixed::Cancel(i, b, l) => Message::Cancel(Request::new(*i, *b, *l)),
                    Mixed::Port(p) => Message::Port(*p),
                }
                .serialize(&mut frame, &Default::default)
                .unwrap();
                wire.extend_from_slice(&frame[..len]);
            }
            let total = wire.len() as u64;
            let pool = BlockPool::new(crate::disk::MAX_QUEUED_DISK_BYTES);
            let mut reader: BoxAsyncReadVectored = Box::new(RandScript {
                data: wire,
                at: 0,
                rand: Rand(seed.wrapping_mul(0x1234_5678_9abc_def1) | 1),
            });
            let mut quota = RandQuota(Rand(seed.wrapping_mul(0x0fed_cba9_8765_4321) | 1));
            let mut rb = ReadBuf::new();
            rb.receive_blocks_into(pool.clone());
            let mut held = Vec::new();
            for (k, want) in sent.iter().enumerate() {
                let got = match rb
                    .read_next(&mut reader, Duration::from_secs(10), None, &mut quota)
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed} message {k}: {e:#}"))
                {
                    Incoming::Block(b) => {
                        let m = Mixed::Block(b.index, b.begin, b.data().to_vec());
                        // Blocks are kept alive, as queued writes are, so a
                        // buffer reused too soon would show.
                        held.push(b);
                        m
                    }
                    Incoming::Message(Message::Have(n)) => Mixed::Have(n),
                    Incoming::Message(Message::KeepAlive) => Mixed::KeepAlive,
                    Incoming::Message(Message::Unchoke) => Mixed::Unchoke,
                    Incoming::Message(Message::Request(r)) => {
                        Mixed::Request(r.index, r.begin, r.length)
                    }
                    Incoming::Message(Message::Cancel(r)) => {
                        Mixed::Cancel(r.index, r.begin, r.length)
                    }
                    Incoming::Message(Message::Port(p)) => Mixed::Port(p),
                    Incoming::Message(m) => panic!("seed {seed} message {k}: {m:?}"),
                };
                assert!(got == *want, "seed {seed} message {k}: {got:?} != {want:?}");
            }
            assert_eq!(rb.consumed(), total, "seed {seed}");
            assert_eq!(rb.buffered(), 0, "seed {seed}");
            // Every received block's bytes are still its own.
            let mut blocks = sent.iter().filter_map(|m| match m {
                Mixed::Block(_, _, d) => Some(d),
                _ => None,
            });
            for b in &held {
                assert_eq!(b.data(), &blocks.next().unwrap()[..], "seed {seed}");
            }
        }
    }

    /// Receive-side cost of blocks back to back off an in-memory peer stream
    /// that hands at most `cap` bytes a read, as a socket does, plain and
    /// under RC4. Each block is dropped once read, its buffer going back to
    /// the pool as a written block's does. Run by hand, in release:
    ///
    ///   cargo test --locked --release -p weaver-torrent --lib \
    ///     read_buf::tests::receive_side_bench -- --ignored --nocapture
    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "measurement, run by hand"]
    async fn receive_side_bench() {
        use crate::mse::{Rc4, TestDecryptingReader as DecryptingReader};
        const BLOCKS: u32 = 4096; // 64 MiB of payload a round
        const ROUNDS: usize = 12;
        fn cpu_ms() -> f64 {
            use nix::sys::resource::{UsageWho, getrusage};
            let u = getrusage(UsageWho::RUSAGE_SELF).unwrap();
            let ms = |t: nix::sys::time::TimeVal| {
                t.tv_sec() as f64 * 1000.0 + t.tv_usec() as f64 / 1000.0
            };
            ms(u.user_time()) + ms(u.system_time())
        }
        let plain = {
            let mut wire = Vec::new();
            let mut frame = vec![0u8; MAX_MSG_LEN];
            let data = block_of(1, 16384);
            for i in 0..BLOCKS {
                let len = Message::Piece(Piece::from_data(i / 64, (i % 64) * 16384, &data[..]))
                    .serialize(&mut frame, &Default::default)
                    .unwrap();
                wire.extend_from_slice(&frame[..len]);
            }
            wire
        };
        let mut encrypted = plain.clone();
        Rc4::new_discard_1024(b"receive-side key").apply(&mut encrypted);
        for cap in [65_536usize, 262_144] {
            for rc4 in [false, true] {
                let pool = BlockPool::new(crate::disk::MAX_QUEUED_DISK_BYTES);
                let mut best_mibs = 0f64;
                let mut best_cpu = f64::MAX;
                for round in 0..ROUNDS {
                    let script = Script {
                        data: if rc4 {
                            encrypted.clone()
                        } else {
                            plain.clone()
                        },
                        at: 0,
                        cap,
                        reads: Default::default(),
                    };
                    let mut reader: BoxAsyncReadVectored = if rc4 {
                        DecryptingReader::new(
                            Vec::new(),
                            Box::new(script),
                            Some(Rc4::new_discard_1024(b"receive-side key")),
                        )
                        .into_boxed()
                    } else {
                        Box::new(script)
                    };
                    let mut rb = ReadBuf::new();
                    rb.receive_blocks_into(pool.clone());
                    let wall = std::time::Instant::now();
                    let cpu = cpu_ms();
                    for i in 0..BLOCKS {
                        match rb
                            .read_next(&mut reader, Duration::from_secs(10), None, &mut Unlimited)
                            .await
                            .unwrap()
                        {
                            Incoming::Block(b) => {
                                if round == 0 {
                                    assert_eq!((b.index, b.begin), (i / 64, (i % 64) * 16384));
                                    assert_eq!(b.data(), &block_of(1, 16384)[..]);
                                }
                                std::hint::black_box(&b);
                            }
                            Incoming::Message(m) => panic!("{m:?}"),
                        }
                    }
                    let cpu = cpu_ms() - cpu;
                    let secs = wall.elapsed().as_secs_f64();
                    if round > 0 {
                        let mib = f64::from(BLOCKS) / 64.0;
                        best_mibs = best_mibs.max(mib / secs);
                        best_cpu = best_cpu.min(cpu / mib);
                    }
                }
                println!(
                    "receive cap={cap} rc4={rc4} mib_per_s={best_mibs:.0} cpu_ms_per_mib={best_cpu:.4}"
                );
            }
        }
    }
}
