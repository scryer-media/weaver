// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::core::{
    hash_id::{Id20, InfoHashes},
    lengths::ChunkInfo,
    peer_id::try_decode_peer_id,
};
use crate::peer_protocol::{
    Handshake, HashRequest, Hashes, Message, PIECE_MESSAGE_DEFAULT_LEN, PIECE_MESSAGE_PREAMBLE_LEN,
    PartialBlock, Request, SerializeError,
    extended::{
        ExtendedMessage, PeerExtendedMessageIds,
        handshake::ExtendedHandshake,
        ut_metadata::{UtMetadata, UtMetadataData},
        ut_pex::UtPex,
    },
    max_frame_len, serialize_piece_preamble,
};
use crate::{
    Error, Result, limits::Channel, meter::Meter, read_buf::ReadQuota,
    session::CheckedIncomingConnection, stream_connect::ConnectionKind,
};
use futures::TryFutureExt;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tracing::{Instrument, trace, trace_span};

use crate::mse::{Encryption, EncryptionPolicy};
use crate::{
    read_buf::ReadBuf,
    spawn_utils::BlockingSpawner,
    ssl::SslTorrent,
    stream_connect::{Guard, StreamConnector, Transports},
    type_aliases::BoxAsyncReadVectored,
};

pub trait PeerConnectionHandler {
    fn on_connected(&self, _connection_time: Duration) {}
    /// Whether the outgoing connection, its transport just open, tries the
    /// encrypted handshake where encryption is preferred; asked once per
    /// connection (libtorrent `bt_peer_connection::on_connected`, by
    /// `torrent_peer::pe_support`). A handler that remembers nothing of the
    /// peer tries it.
    fn encrypt_outgoing(&self) -> bool {
        true
    }
    /// The connection, whichever side opened it, completed its handshake,
    /// as `encryption` says; told right after
    /// [`Self::on_handshake`].
    fn on_handshake_encryption(&self, _encryption: Encryption) {}
    fn should_send_bitfield(&self) -> bool;
    fn serialize_bitfield_message_to_buf(&self, buf: &mut [u8]) -> anyhow::Result<usize>;
    /// `speaks_v2`: the peer speaks BEP 52, so it may exchange hashes.
    fn on_handshake(
        &self,
        handshake: Handshake,
        ckind: ConnectionKind,
        speaks_v2: bool,
    ) -> anyhow::Result<()>;
    /// The peer sent an extended handshake, the first or a later one (BEP
    /// 10). Its `m` holds the ids the peer names on the connection after
    /// it, every handshake so far taken together
    /// ([`PeerExtendedMessageIds::updated_by`]); its other fields are as
    /// this one sent them.
    fn on_extended_handshake(
        &self,
        extended_handshake: &ExtendedHandshake<ByteBuf>,
    ) -> anyhow::Result<()>;
    async fn on_received_message(&self, msg: Message<'_>) -> anyhow::Result<()>;
    fn on_uploaded_bytes(&self, bytes: u32);
    /// One write handed the connection's writer `bytes` of block payload,
    /// each block already told to [`Self::on_uploaded_bytes`]. Told once per
    /// write that carried a block (libtorrent stamps `m_last_sent_payload`
    /// once per completed send, in `peer_connection::sent_bytes`).
    fn on_payload_written(&self, _bytes: u64) {}
    /// Waits for the limits over the connection's `channel` to grant some of
    /// the `bytes` it asks for, and returns how many they granted: one or
    /// more when `bytes` is not zero. A grant pays for bytes that cross the
    /// socket, payload and protocol alike (libtorrent
    /// `peer_connection::request_bandwidth`). A connection under no limits
    /// is granted all it asks for.
    async fn request_bandwidth(&self, _channel: Channel, bytes: u64) -> u64 {
        bytes
    }
    /// `bytes` granted that will not cross the socket: the connection ended
    /// holding them.
    fn return_bandwidth(&self, _channel: Channel, _bytes: u64) {}
    /// `bytes` that are not block payload crossed the socket on `channel`:
    /// handshakes and every message but a piece's block (libtorrent
    /// `stat::sent_bytes`, `received_bytes`, the protocol half).
    fn on_protocol_bytes(&self, _channel: Channel, _bytes: u64) {}
    /// The connection ended while the peer was sending `block`: the bytes
    /// of it that arrived are dropped. Told once, however the connection
    /// ended.
    fn on_partial_block(&self, _block: PartialBlock) {}
    /// What a read asks the download limits for besides the rest of the
    /// frame it reads: the bytes of the blocks requested of the peer and not
    /// yet received, or what the peer sends in three quarters of a second at
    /// its rate, whichever is more (libtorrent
    /// `peer_connection::wanted_transfer`). A handler that requests nothing
    /// asks for nothing more.
    fn download_wanted(&self) -> u64 {
        0
    }
    /// Takes the block `chunk` the writer was handed, to send it. False
    /// when a Cancel or a Choke dropped it first, and the block is not sent.
    /// A handler that queues no uploads has none to take.
    fn claim_upload(&self, _chunk: &ChunkInfo) -> bool {
        false
    }
    /// The block `chunk` the writer took is sent, or dropped unserved: its
    /// room in the peer's send buffer is free for the next.
    fn upload_done(&self, _chunk: &ChunkInfo) {}
    /// Whether the block `chunk` the writer took is read and sent. False
    /// drops it unsent and keeps the connection; an error ends it. A
    /// handler with nothing to check first serves every block.
    async fn serves(&self, _chunk: &ChunkInfo) -> anyhow::Result<bool> {
        Ok(true)
    }
    fn read_chunk(&self, chunk: &ChunkInfo, buf: &mut [u8]) -> anyhow::Result<()>;
    /// Each of `chunks` read into its buffer of `bufs`. On an error, the
    /// first block not read and why: the blocks before it are read whole.
    /// A handler with no faster way reads them one at a time.
    fn read_chunks(
        &self,
        chunks: &[ChunkInfo],
        bufs: &mut [&mut [u8]],
    ) -> std::result::Result<(), (usize, anyhow::Error)> {
        for (i, (chunk, buf)) in chunks.iter().zip(bufs.iter_mut()).enumerate() {
            self.read_chunk(chunk, buf).map_err(|e| (i, e))?;
        }
        Ok(())
    }
    fn update_my_extended_handshake(
        &self,
        _handshake: &mut ExtendedHandshake<ByteBuf>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn client_name_and_version(&self) -> &str {
        crate::client_name_and_version()
    }
    /// The torrent's piece count once its metadata is known: it sets the
    /// length of a bitfield, and so of the longest frame.
    fn num_pieces(&self) -> Option<u32> {
        None
    }
    /// The UDP port of the session's DHT node, when it runs one: the
    /// handshake then sets the DHT bit (BEP 5).
    fn dht_port(&self) -> Option<u16> {
        None
    }
    /// The answers to the peer's requests queued and not yet written. While
    /// they hold [`ANSWER_BACKLOG_LIMIT`] bytes or more, nothing more is read
    /// from the peer. A handler that answers no requests has none.
    fn answers(&self) -> Option<&AnswerBacklog> {
        None
    }
}

/// The most bytes of answers to a peer's requests that may wait for the
/// writer before the connection stops reading from the peer. libtorrent
/// bounds none of these: a hash request is answered at once whatever its
/// send buffer holds (bt_peer_connection.cpp:1162-1233, v2.0.15), as is a
/// Reject, and `can_read` (peer_connection.cpp:6280-6301) looks at the
/// download quota and the disk, never the send buffer. Its block uploads
/// alone stop at `send_buffer_watermark` (peer_connection.cpp:5252-5297,
/// 500 KiB by default, settings_pack.cpp:280); this is that bound, for the
/// answers. Blocks are not counted: the peer's queue of them is capped
/// apart, and the writer reads each only as it sends it.
pub(crate) const ANSWER_BACKLOG_LIMIT: usize = 500 * 1024;

/// A connection's queued answers, in bytes as they go on the wire.
#[derive(Debug, Default)]
pub struct AnswerBacklog {
    bytes: std::sync::atomic::AtomicUsize,
    drained: tokio::sync::Notify,
}

impl AnswerBacklog {
    /// `answer` is queued for the writer.
    pub(crate) fn queued(&self, answer: &Answer) {
        self.bytes
            .fetch_add(answer.wire_len(), std::sync::atomic::Ordering::AcqRel);
    }

    /// Answers of `bytes` in all left the queue, written or dropped.
    fn written(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _ = self.bytes.fetch_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
            |held| Some(held.saturating_sub(bytes)),
        );
        self.drained.notify_waiters();
    }

    /// The bytes queued now.
    pub(crate) fn held(&self) -> usize {
        self.bytes.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns once fewer than `limit` bytes are queued.
    async fn below(&self, limit: usize) {
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.held() < limit {
                return;
            }
            drained.await;
        }
    }
}

/// An answer to one of the peer's requests, counted in its connection's
/// [`AnswerBacklog`] while it waits for the writer.
#[derive(Debug)]
pub enum Answer {
    /// A Reject of a block request (BEP 6).
    Reject(Request),
    HashReject(HashRequest),
    Hashes(Hashes<ByteBufOwned>),
    /// A piece of the metadata (BEP 9).
    Metadata(UtMetadata<ByteBufOwned>),
}

impl Answer {
    /// The bytes the answer takes on the wire, near enough: its frame
    /// header, fixed fields and payload.
    pub(crate) fn wire_len(&self) -> usize {
        match self {
            Self::Reject(_) => 17,
            Self::HashReject(_) => 53,
            Self::Hashes(hashes) => 53 + hashes.hashes.as_ref().len(),
            Self::Metadata(utm) => match utm.as_borrowed() {
                UtMetadata::Data(data) => 64 + data.len(),
                _ => 64,
            },
        }
    }

    fn serialize(
        &self,
        out: &mut [u8],
        ext_msg_ids: &dyn Fn() -> PeerExtendedMessageIds,
    ) -> std::result::Result<usize, SerializeError> {
        match self {
            Self::Reject(request) => Message::Reject(*request).serialize(out, ext_msg_ids),
            Self::HashReject(request) => Message::HashReject(*request).serialize(out, ext_msg_ids),
            Self::Hashes(hashes) => {
                Message::Hashes(hashes.as_borrowed()).serialize(out, ext_msg_ids)
            }
            Self::Metadata(utm) => {
                Message::Extended(ExtendedMessage::UtMetadata(utm.as_borrowed()))
                    .serialize(out, ext_msg_ids)
            }
        }
    }
}

impl Answer {
    /// The metadata piece `data`.
    pub fn metadata(data: UtMetadataData<ByteBufOwned>) -> Self {
        Self::Metadata(UtMetadata::Data(data))
    }
}

#[derive(Debug)]
pub enum WriterRequest {
    Message(Message<'static>),
    UtPex(UtPex<ByteBufOwned>),
    /// An answer to a request of the peer's; see [`AnswerBacklog`].
    Answer(Answer),
    ReadChunkRequest(ChunkInfo),
    Disconnect(anyhow::Result<()>),
}

#[derive(Default, Debug, Copy, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct PeerConnectionOptions {
    #[serde(default, with = "optional_seconds")]
    pub connect_timeout: Option<Duration>,

    #[serde(default, with = "optional_seconds")]
    pub read_write_timeout: Option<Duration>,

    #[serde(default, with = "optional_seconds")]
    pub keep_alive_interval: Option<Duration>,

    /// How long the encryption handshake, and then the BitTorrent
    /// handshake, may take on a connection either way; default
    /// [`DEFAULT_HANDSHAKE_TIMEOUT`].
    #[serde(default, with = "optional_seconds")]
    pub handshake_timeout: Option<Duration>,

    /// The most chunk requests to keep outstanding per peer (default 500, and
    /// never more than the peer's reqq). A newly needed piece, such as the
    /// one at a stream's read position after a seek, is requested only after
    /// those, so a smaller window makes streams react faster to seeks.
    pub max_request_window: Option<usize>,

    /// The encryption every connection of the torrent is made and kept
    /// under, over the session's.
    #[serde(default)]
    pub encryption: Option<EncryptionPolicy>,
}

/// libtorrent `settings_pack::handshake_timeout`: a peer that has not
/// finished its handshake this long after it last sent anything is
/// dropped (`peer_connection::second_tick`).
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// libtorrent `settings_pack::peer_connect_timeout`: an outgoing connection
/// not made this long after it was started is given up
/// (`peer_connection::second_tick`, while `m_connecting`).
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// The read and write timeout when none is set.
const DEFAULT_READ_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

impl PeerConnectionOptions {
    /// The encryption a connection is made and kept under: these options',
    /// else the session's `session`.
    pub(crate) fn encryption_under(&self, session: EncryptionPolicy) -> EncryptionPolicy {
        self.encryption.unwrap_or(session)
    }

    pub(crate) fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout.unwrap_or(DEFAULT_HANDSHAKE_TIMEOUT)
    }

    fn read_write_timeout(&self) -> Duration {
        self.read_write_timeout
            .unwrap_or(DEFAULT_READ_WRITE_TIMEOUT)
    }

    /// A keepalive goes out once nothing has been sent for half the read
    /// timeout, so a peer that times out a silent connection as we do
    /// hears from us in time (libtorrent `peer_connection::keep_alive`:
    /// `timeout() / 2`, the `settings_pack::peer_timeout`).
    fn keep_alive_interval(&self) -> Duration {
        self.keep_alive_interval
            .unwrap_or_else(|| self.read_write_timeout() / 2)
    }
}

// A duration as whole seconds, rounded to the nearest one; `None` as none.
mod optional_seconds {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(duration) => {
                let rounded = duration.as_secs() + u64::from(duration.subsec_millis() >= 500);
                serializer.serialize_some(&rounded)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        Ok(Option::<u64>::deserialize(deserializer)?.map(Duration::from_secs))
    }
}

pub(crate) struct PeerConnection<H> {
    handler: H,
    addr: SocketAddr,
    info_hashes: InfoHashes,
    peer_id: Id20,
    options: PeerConnectionOptions,
    spawner: BlockingSpawner,
    connector: Arc<StreamConnector>,
    // What an outgoing connection may go over.
    transports: Transports,
    // An SSL torrent's: an outgoing connection is TLS under its CA.
    ssl: Option<Arc<SslTorrent>>,
}

pub(crate) async fn with_timeout<T>(
    name: &'static str,
    timeout_value: Duration,
    fut: impl std::future::Future<Output = Result<T>>,
) -> crate::Result<T> {
    match timeout(timeout_value, fut).await {
        Ok(v) => v,
        Err(_) => Err(Error::Timeout(name)),
    }
}

struct ManagePeerArgs {
    handshake_supports_extended: bool,
    speaks_v2: bool,
    /// The peer set the fast bit in its handshake (libtorrent
    /// `bt_peer_connection::m_supports_fast`).
    supports_fast: bool,
    read_buf: ReadBuf,
    write_buf: Box<[u8]>,
    read: BoxAsyncReadVectored,
    write: crate::mse::Outbound,
    meter: Meter,
    outgoing_chan: tokio::sync::mpsc::UnboundedReceiver<WriterRequest>,
}

/// What a connection may move one way: the bytes its limits paid for, less
/// those that crossed the socket since it was made (libtorrent
/// `peer_connection::m_quota`). Bytes that crossed before anything was paid,
/// the handshakes, are a debt the next payment covers.
struct Quota<'a, H: PeerConnectionHandler> {
    handler: &'a H,
    channel: Channel,
    meter: Meter,
    paid: u64,
}

impl<'a, H: PeerConnectionHandler> Quota<'a, H> {
    fn new(handler: &'a H, channel: Channel, meter: Meter) -> Self {
        Self {
            handler,
            channel,
            meter,
            paid: 0,
        }
    }

    fn crossed(&self) -> u64 {
        match self.channel {
            Channel::Download => self.meter.received(),
            Channel::Upload => self.meter.sent(),
        }
    }

    /// Bytes paid for and not yet moved; below zero, the debt.
    fn held(&self) -> i128 {
        i128::from(self.paid) - i128::from(self.crossed())
    }

    /// Asks the limits for `bytes` beyond what is held.
    async fn pay(&mut self, bytes: u64) {
        let want = u64::try_from(i128::from(bytes) - self.held()).unwrap_or(0);
        self.paid += self.handler.request_bandwidth(self.channel, want).await;
    }

    /// Waits until `bytes` more may cross (libtorrent
    /// `peer_connection::setup_send` writes only what `m_quota` covers).
    async fn reserve(&mut self, bytes: u64) {
        while self.held() < i128::from(bytes) {
            self.pay(bytes).await;
        }
    }
}

// A connection that ends holding quota gives it back, so its limits are
// paid for the bytes that crossed and no more.
impl<H: PeerConnectionHandler> Drop for Quota<'_, H> {
    fn drop(&mut self) {
        if let Ok(held @ 1..) = u64::try_from(self.held()) {
            self.handler.return_bandwidth(self.channel, held);
        }
    }
}

/// What libtorrent adds to the bytes a read wants, for the headers around
/// them (`peer_connection::wanted_transfer`).
pub(crate) const WANTED_SLACK: u64 = 30;

impl<H: PeerConnectionHandler> ReadQuota for Quota<'_, H> {
    // A read waits for a byte or more of quota, and takes what is held. It
    // asks for what completes the frame, or for what the handler wants
    // besides, whichever is more (libtorrent
    // `peer_connection::wanted_transfer`): asking a frame at a time, a
    // connection under a limit would read a frame a round, and leave on its
    // socket the blocks it asked for.
    async fn admit(&mut self, need: usize, room: usize) -> usize {
        while self.held() <= 0 {
            let want = (need as u64 + WANTED_SLACK).max(self.handler.download_wanted());
            self.pay(want).await;
        }
        usize::try_from(self.held()).map_or(room, |held| held.min(room))
    }
}

/// A frame in the writer's buffer, in the order the buffer holds them: its
/// length, the block it carries if it is one, and whether that block is
/// still to be read into it.
struct Framed {
    len: usize,
    chunk: Option<ChunkInfo>,
    read: bool,
}

/// A handler's error as the connection ends in it: one of ours stays as it
/// is, so a close the handler asks for at the handshake is no failure.
fn handler_error(e: anyhow::Error) -> Error {
    e.downcast::<Error>().unwrap_or_else(Error::Anyhow)
}

/// The frame of a message serialized, or none for one the peer cannot be
/// sent: an extension message under an extension it names no id for, or
/// no longer does, is dropped and the connection kept (libtorrent sends
/// nothing to a peer whose id is 0, `bt_peer_connection::write_upload_only`).
fn addressed(serialized: std::result::Result<usize, SerializeError>) -> Result<Option<Framed>> {
    match serialized {
        Ok(len) => Ok(Some(Framed::message(len))),
        Err(e) if e.is_unaddressed() => {
            trace!("dropping a message the peer cannot be sent: {e}");
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}

impl Framed {
    fn message(len: usize) -> Self {
        Self {
            len,
            chunk: None,
            read: false,
        }
    }
}

/// The block frames one write of the writer carries at most, when that many
/// are waiting: 512 KiB of payload, about libtorrent's default send buffer
/// watermark, 500 KiB, which libtorrent fills and writes whole in one writev
/// (`peer_connection::setup_send`). The writer's own buffer holds two; a
/// longer run is framed in a buffer lent for the write.
const BATCH_BLOCKS: usize = 32;
const BATCH_LEN: usize = BATCH_BLOCKS * PIECE_MESSAGE_DEFAULT_LEN;
// The batch buffers kept for the next writer once returned; any more are
// let go.
const BATCH_BUFS_KEPT: usize = 16;

/// The batch buffers between writes: a writer holds one only from framing
/// its blocks to their write, so a connection at rest holds none.
static BATCH_BUFS: std::sync::Mutex<Vec<Box<[u8]>>> = std::sync::Mutex::new(Vec::new());

fn lend_batch_buf() -> Box<[u8]> {
    BATCH_BUFS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .pop()
        .unwrap_or_else(|| vec![0u8; BATCH_LEN].into_boxed_slice())
}

fn return_batch_buf(buf: Box<[u8]>) {
    let mut kept = BATCH_BUFS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if kept.len() < BATCH_BUFS_KEPT {
        kept.push(buf);
    }
}

/// Writes `buf` once its bytes are paid for. The timeout runs only while the
/// socket is written, not while the quota is waited for.
async fn send<H: PeerConnectionHandler>(
    quota: &mut Quota<'_, H>,
    write: &mut crate::mse::Outbound,
    buf: &mut [u8],
    what: &'static str,
    timeout: Duration,
) -> Result<()> {
    quota.reserve(buf.len() as u64).await;
    with_timeout(what, timeout, write.write_all(buf).map_err(Error::Write)).await
}

/// The bytes received and not yet read as a message. However the connection
/// ends, its reader or writer stopping, or its task dropped, the Piece
/// message cut short at their front is told to the handler, once (libtorrent
/// `peer_connection::disconnect`, `downloading_piece_progress`).
struct Unread<'a, H: PeerConnectionHandler> {
    handler: &'a H,
    buf: ReadBuf,
}

impl<H: PeerConnectionHandler> Drop for Unread<'_, H> {
    fn drop(&mut self) {
        if let Some(block) = self.buf.partial_block() {
            self.handler.on_partial_block(block);
        }
    }
}

impl<H: PeerConnectionHandler> PeerConnection<H> {
    pub fn new(
        addr: SocketAddr,
        info_hashes: InfoHashes,
        peer_id: Id20,
        handler: H,
        options: Option<PeerConnectionOptions>,
        spawner: BlockingSpawner,
        connector: Arc<StreamConnector>,
    ) -> Self {
        PeerConnection {
            handler,
            addr,
            info_hashes,
            peer_id,
            spawner,
            options: options.unwrap_or_default(),
            connector,
            transports: Transports::Any,
            ssl: None,
        }
    }

    /// The outgoing connection goes over `transports`.
    pub fn over(self, transports: Transports) -> Self {
        Self { transports, ..self }
    }

    /// The outgoing connection is TLS under `ssl`'s CA when the torrent is
    /// an SSL torrent; the encryption policy is not read then.
    pub fn under(self, ssl: Option<Arc<SslTorrent>>) -> Self {
        Self { ssl, ..self }
    }

    // By the time this is called:
    // read_buf should start with valuable data. The handshake should be removed from it.
    pub async fn manage_peer_incoming(
        &self,
        outgoing_chan: tokio::sync::mpsc::UnboundedReceiver<WriterRequest>,
        mut incoming: CheckedIncomingConnection,
    ) -> Result<()> {
        // A peer speaks v2 when it greets with the truncated v2 hash or
        // offers v2, which a torrent without v2 refuses (libtorrent
        // `bt_peer_connection::on_receive_handshake`).
        let greeted = incoming.handshake.info_hash;
        let speaks_v2 = self.info_hashes.is_v2(greeted) || incoming.handshake.offers_v2();
        if !self.info_hashes.answers_to(greeted) || (speaks_v2 && self.info_hashes.v2.is_none()) {
            return Err(Error::WrongInfoHash);
        }

        if incoming.handshake.peer_id == self.peer_id {
            return Err(Error::ConnectingToOurselves);
        }

        trace!(
            "incoming connection: id={:?}",
            try_decode_peer_id(incoming.handshake.peer_id)
        );

        let mut write_buf = self.frame_buf();
        let my_handshake = self.greeting(speaks_v2);
        let hlen = my_handshake.serialize_unchecked_len(&mut write_buf);
        with_timeout(
            "writing handshake",
            self.options.handshake_timeout(),
            incoming
                .writer
                .write_all(&mut write_buf[..hlen])
                .map_err(Error::WriteHandshake),
        )
        .await?;

        // What the peer supports is in the peer's handshake, not in ours.
        let handshake_supports_extended = incoming.handshake.supports_extended();
        let supports_fast = incoming.handshake.supports_fast();

        self.handler
            .on_handshake(incoming.handshake, incoming.kind, speaks_v2)
            .map_err(handler_error)?;
        self.handler.on_handshake_encryption(incoming.encryption);

        self.manage_peer(ManagePeerArgs {
            handshake_supports_extended,
            speaks_v2,
            supports_fast,
            read_buf: incoming.read_buf,
            write_buf,
            read: incoming.reader,
            write: incoming.writer,
            meter: incoming.meter,
            outgoing_chan,
        })
        .await
    }

    pub async fn manage_peer_outgoing(
        &self,
        outgoing_chan: tokio::sync::mpsc::UnboundedReceiver<WriterRequest>,
    ) -> Result<()> {
        let handshake_timeout = self.options.handshake_timeout();
        let connect_timeout = self
            .options
            .connect_timeout
            .unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        let mut write_buf = self.frame_buf();
        // Every peer is greeted as v1 first, which a hybrid's v2 peers
        // answer too (libtorrent `peer_connection::associated_info_hash`).
        let handshake = self.greeting(false);
        let greeting = handshake.info_hash;
        let hsz = handshake.serialize_unchecked_len(&mut write_buf);

        // Connecting includes sending our handshake: an encrypted connection
        // carries it inside the encryption handshake.
        let now = Instant::now();
        let (ckind, mut read, write, meter, encryption) = self
            .connector
            .connect_and_send_handshake(
                self.addr,
                self.transports,
                match &self.ssl {
                    Some(ssl) => Guard::Tls(ssl),
                    None => Guard::Mse(self.options.encryption_under(self.connector.encryption())),
                },
                &greeting,
                &write_buf[..hsz],
                || self.handler.encrypt_outgoing(),
                connect_timeout,
                handshake_timeout,
            )
            .await?;

        async move {
            self.handler.on_connected(now.elapsed());

            let mut read_buf = ReadBuf::new();
            let h = read_buf
                .read_handshake(&mut read, handshake_timeout)
                .await?;
            let handshake_supports_extended = h.supports_extended();
            let supports_fast = h.supports_fast();
            trace!(
                peer_id=?h.peer_id,
                decoded_id=?try_decode_peer_id(h.peer_id),
                "connected",
            );
            if !self.info_hashes.accepts_answer(greeting, h.info_hash) {
                return Err(Error::WrongInfoHash);
            }
            // Only the hash it answers with says the peer speaks v2
            // (libtorrent `bt_peer_connection::on_receive_handshake`).
            let speaks_v2 = self.info_hashes.is_v2(h.info_hash);

            if h.peer_id == self.peer_id {
                return Err(Error::ConnectingToOurselves);
            }

            self.handler
                .on_handshake(h, ckind, speaks_v2)
                .map_err(handler_error)?;
            // The peer answered, so the method this connection tried works
            // with it (libtorrent `bt_peer_connection::on_receive_impl`).
            self.handler.on_handshake_encryption(encryption);

            self.manage_peer(ManagePeerArgs {
                handshake_supports_extended,
                speaks_v2,
                supports_fast,
                read_buf,
                write_buf,
                read,
                write,
                meter,
                outgoing_chan,
            })
            .await
        }
        .instrument(trace_span!("", kind=%ckind))
        .await
    }

    /// A buffer for the longest frame of the torrent.
    fn frame_buf(&self) -> Box<[u8]> {
        // The longest frame, and room for a block's frame behind it, so a
        // write carries more than one frame.
        vec![0u8; max_frame_len(self.handler.num_pieces()) + PIECE_MESSAGE_DEFAULT_LEN]
            .into_boxed_slice()
    }

    /// Our handshake: the hash of the protocol the peer speaks, offering
    /// v2 to a peer that does not speak it when the torrent has v2
    /// (libtorrent `bt_peer_connection::write_handshake`).
    fn greeting(&self, speaks_v2: bool) -> Handshake {
        Handshake::new(self.info_hashes.for_peer(speaks_v2), self.peer_id)
            .offering_v2(!speaks_v2 && self.info_hashes.v2.is_some())
            .offering_dht(self.handler.dht_port().is_some())
    }

    async fn manage_peer(&self, args: ManagePeerArgs) -> Result<()> {
        let ManagePeerArgs {
            handshake_supports_extended,
            speaks_v2,
            supports_fast,
            read_buf,
            mut write_buf,
            mut read,
            mut write,
            meter,
            mut outgoing_chan,
        } = args;
        let mut unread = Unread {
            handler: &self.handler,
            buf: read_buf,
        };

        let rwtimeout = self.options.read_write_timeout();

        // The handshakes, and the encryption handshake before them, crossed
        // already: protocol bytes, and a debt each quota pays first.
        self.handler.on_protocol_bytes(
            Channel::Download,
            meter
                .received()
                .saturating_sub(unread.buf.buffered() as u64),
        );
        self.handler
            .on_protocol_bytes(Channel::Upload, meter.sent());
        let mut down = Quota::new(&self.handler, Channel::Download, meter.clone());
        let mut up = Quota::new(&self.handler, Channel::Upload, meter);

        let extended_handshake: RwLock<Option<PeerExtendedMessageIds>> = RwLock::new(None);
        let extended_handshake_ref = &extended_handshake;
        let supports_extended = handshake_supports_extended;

        if supports_extended {
            let mut my_extended = ExtendedHandshake::new();
            my_extended.v = Some(ByteBuf(self.handler.client_name_and_version().as_bytes()));
            my_extended.yourip = Some(self.addr.ip().into());
            self.handler
                .update_my_extended_handshake(&mut my_extended)
                .map_err(Error::Anyhow)?;
            unread.buf.set_advertised(my_extended.m);
            let my_extended = Message::Extended(ExtendedMessage::Handshake(my_extended));
            trace!("sending extended handshake: {:?}", &my_extended);
            let esz = my_extended.serialize(&mut write_buf, &Default::default)?;
            send(
                &mut up,
                &mut write,
                &mut write_buf[..esz],
                "writing extended handshake",
                rwtimeout,
            )
            .await?;
            self.handler.on_protocol_bytes(Channel::Upload, esz as u64);
        }

        let writer = async move {
            let keep_alive_interval = self.options.keep_alive_interval();

            if self.handler.should_send_bitfield() {
                let len = self
                    .handler
                    .serialize_bitfield_message_to_buf(&mut write_buf)
                    .map_err(Error::Anyhow)?;
                send(
                    &mut up,
                    &mut write,
                    &mut write_buf[..len],
                    "writing bitfield",
                    rwtimeout,
                )
                .await?;
                self.handler.on_protocol_bytes(Channel::Upload, len as u64);
                trace!("sent bitfield");
            }

            // The frames queued behind the first go out in the same write, as
            // many as the buffer holds: a block while the room left holds its
            // frame, any other message while it holds the longest frame. One
            // that does not fit is the first of the next write.
            let longest_frame = write_buf.len() - PIECE_MESSAGE_DEFAULT_LEN;
            let mut carried: Option<WriterRequest> = None;
            let mut batch: Vec<Framed> = Vec::new();
            loop {
                let first = match carried.take() {
                    Some(req) => req,
                    None => match timeout(keep_alive_interval, outgoing_chan.recv()).await {
                        Ok(Some(msg)) => msg,
                        Ok(None) => {
                            return Err(Error::TorrentIsNotLive);
                        }
                        Err(_) => WriterRequest::Message(Message::KeepAlive),
                    },
                };

                // The rest of the connection runs once this task has run its
                // share, not after every message.
                tokio::task::coop::consume_budget().await;

                let ext_msg_ids = &|| {
                    extended_handshake_ref
                        .read()
                        .as_ref()
                        .map(|e| *e)
                        .unwrap_or_default()
                };

                // A run of blocks longer than the writer's buffer holds is
                // framed in a batch buffer, so it goes in one write.
                let mut lent = (matches!(first, WriterRequest::ReadChunkRequest(_))
                    && outgoing_chan.len() >= 2
                    && BATCH_LEN > write_buf.len())
                .then(lend_batch_buf);
                let buf: &mut [u8] = match &mut lent {
                    Some(lent) => lent,
                    None => &mut write_buf,
                };

                let mut filled = 0;
                let mut answered = 0;
                let mut ending = None;
                let mut next = Some(first);
                while let Some(req) = next.take() {
                    trace!("about to send: {:?}", &req);
                    let needs = match &req {
                        WriterRequest::ReadChunkRequest(chunk) => {
                            PIECE_MESSAGE_PREAMBLE_LEN + chunk.size as usize
                        }
                        WriterRequest::Disconnect(_) => 0,
                        _ => longest_frame,
                    };
                    if filled > 0 && buf.len() - filled < needs {
                        carried = Some(req);
                        break;
                    }
                    let out = &mut buf[filled..];
                    let framed = match req {
                        WriterRequest::Message(msg) => addressed(msg.serialize(out, ext_msg_ids))?,
                        WriterRequest::UtPex(ut_pex) => addressed(
                            Message::Extended(ExtendedMessage::UtPex(ut_pex.as_borrowed()))
                                .serialize(out, ext_msg_ids),
                        )?,
                        WriterRequest::Answer(answer) => {
                            answered += answer.wire_len();
                            addressed(answer.serialize(out, ext_msg_ids))?
                        }
                        WriterRequest::ReadChunkRequest(chunk) => {
                            if !self.handler.claim_upload(&chunk) {
                                trace!(?chunk, "upload taken off the queue before it was sent");
                                None
                            } else if !self
                                .handler
                                .serves(&chunk)
                                .await
                                .map_err(Error::ReadChunk)?
                            {
                                // The block is refused, with a Reject on a
                                // fast connection (libtorrent
                                // `peer_connection::fill_send_buffer`, a
                                // piece we turn out not to have).
                                trace!(?chunk, "upload not served");
                                self.handler.upload_done(&chunk);
                                supports_fast
                                    .then(|| {
                                        let reject = Message::Reject(Request {
                                            index: chunk.piece_index.get(),
                                            begin: chunk.offset,
                                            length: chunk.size,
                                        });
                                        reject.serialize(out, ext_msg_ids).map(Framed::message)
                                    })
                                    .transpose()?
                            } else {
                                #[allow(unused_mut)]
                                let mut read = true;

                                #[cfg(test)]
                                {
                                    use tracing::warn;
                                    // This is poor-mans fault injection for running e2e tests.
                                    use crate::tests::test_util::TestPeerMetadata;
                                    let tpm = TestPeerMetadata::from_peer_id(self.peer_id);
                                    use rand::RngExt;
                                    if rand::rng().random_bool(tpm.disconnect_probability()) {
                                        return Err(Error::TestDisconnect);
                                    }

                                    #[allow(clippy::cast_possible_truncation)]
                                    let sleep_ms = (rand::rng().random::<f64>()
                                        * (tpm.max_random_sleep_ms as f64))
                                        as u64;
                                    if sleep_ms > 0 {
                                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                                    }

                                    if rand::rng().random_bool(tpm.bad_data_probability()) {
                                        warn!(
                                            "will NOT actually read the data to simulate a malicious peer that sends garbage"
                                        );
                                        out[..needs].fill(0);
                                        read = false;
                                    }
                                }

                                serialize_piece_preamble(&chunk, out);
                                Some(Framed {
                                    len: needs,
                                    chunk: Some(chunk),
                                    read,
                                })
                            }
                        }
                        WriterRequest::Disconnect(res) => {
                            trace!("disconnect requested, closing writer");
                            ending = Some(match res {
                                Ok(()) => Error::Disconnect,
                                Err(e) => Error::DisconnectWithSource(e),
                            });
                            break;
                        }
                    };
                    if let Some(framed) = framed {
                        filled += framed.len;
                        batch.push(framed);
                    }
                    next = outgoing_chan.try_recv().ok();
                }

                // The batch's blocks are read together, on one blocking
                // thread, a run of them next to one another on disk in one
                // read. A read that fails ends the connection once the
                // frames before its block are out.
                let mut failed = None;
                if batch.iter().any(|framed| framed.read) {
                    // Each block read: where its frame is in the batch and
                    // the buffer, the block, and its payload's room.
                    let mut places = Vec::new();
                    let mut chunks = Vec::new();
                    let mut payloads: Vec<&mut [u8]> = Vec::new();
                    let mut rest = &mut buf[..filled];
                    let mut at = 0;
                    for (i, framed) in batch.iter().enumerate() {
                        let (frame, after) = std::mem::take(&mut rest).split_at_mut(framed.len);
                        rest = after;
                        if let (true, Some(chunk)) = (framed.read, framed.chunk) {
                            places.push((i, at));
                            chunks.push(chunk);
                            payloads.push(&mut frame[PIECE_MESSAGE_PREAMBLE_LEN..]);
                        }
                        at += framed.len;
                    }
                    let read = self
                        .spawner
                        .block_in_place_with_semaphore(|| {
                            self.handler.read_chunks(&chunks, &mut payloads)
                        })
                        .await;
                    if let Err((k, e)) = read {
                        let (i, at) = places[k];
                        batch.truncate(i);
                        filled = at;
                        failed = Some(Error::ReadChunk(e));
                    }
                }

                if filled > 0 {
                    send(
                        &mut up,
                        &mut write,
                        &mut buf[..filled],
                        "writing",
                        rwtimeout,
                    )
                    .await?;
                }
                if let Some(answers) = self.handler.answers() {
                    answers.written(answered);
                }
                if let Some(lent) = lent {
                    return_batch_buf(lent);
                }

                let mut sent_payload = 0u64;
                for framed in batch.drain(..) {
                    let payload = framed.chunk.map_or(0, |chunk| chunk.size as usize);
                    self.handler
                        .on_protocol_bytes(Channel::Upload, (framed.len - payload) as u64);
                    if let Some(chunk) = framed.chunk {
                        sent_payload += u64::from(chunk.size);
                        self.handler.on_uploaded_bytes(chunk.size);
                        self.handler.upload_done(&chunk);
                    }
                }
                if sent_payload > 0 {
                    self.handler.on_payload_written(sent_payload);
                }

                if let Some(e) = failed.or(ending) {
                    return Err(e);
                }
            }

            // For type inference.
            #[allow(unreachable_code)]
            Ok::<_, Error>(())
        };

        let reading = &mut unread.buf;
        let reader = async move {
            let num_pieces = self.handler.num_pieces();
            loop {
                // A peer whose answers pile up, unread, is not read from
                // until the writer has sent enough of them.
                if let Some(answers) = self.handler.answers() {
                    answers.below(ANSWER_BACKLOG_LIMIT).await;
                }
                let read_before = reading.consumed();
                let message = reading
                    .read_message(&mut read, rwtimeout, num_pieces, &mut down)
                    .await?;
                trace!("received: {:?}", &message);
                let payload = match &message {
                    Message::Piece(piece) => piece.len() as u64,
                    _ => 0,
                };
                // libtorrent `bt_peer_connection::on_hash_request`, `on_hashes`
                // and `on_hash_reject` disconnect a peer that does not speak v2.
                if message.is_v2_only() && !speaks_v2 {
                    return Err(Error::HashMessageFromV1Peer);
                }
                // libtorrent `bt_peer_connection::on_suggest_piece`,
                // `on_have_all`, `on_have_none`, `on_reject_request` and
                // `on_allowed_fast` disconnect a peer that did not set the
                // fast bit.
                if message.is_fast_only() && !supports_fast {
                    return Err(Error::Anyhow(anyhow::anyhow!(
                        "fast extension message from a peer without it: {message:?}"
                    )));
                }

                // The writer and the requester run once this task has run its
                // share, not after every message.
                tokio::task::coop::consume_budget().await;

                match message {
                    Message::Extended(ExtendedMessage::Handshake(mut h)) => {
                        h.m = {
                            let mut held = extended_handshake_ref.write();
                            let ids = held.unwrap_or_default().updated_by(h.m);
                            *held = Some(ids);
                            ids
                        };
                        self.handler
                            .on_extended_handshake(&h)
                            .map_err(Error::Anyhow)?;
                    }
                    message => {
                        self.handler
                            .on_received_message(message)
                            .await
                            .map_err(Error::Anyhow)?;
                    }
                }
                self.handler.on_protocol_bytes(
                    Channel::Download,
                    reading.consumed() - read_before - payload,
                );
            }

            // For type inference.
            #[allow(unreachable_code)]
            Ok::<_, Error>(())
        };

        tokio::select! {
            r = reader => {
                if let Err(e) = r.as_ref() {
                    trace!("reader finished with error: {e:#}");
                } else {
                    trace!("reader finished without error");
                }
                r
            }
            r = writer => {
                if let Err(e) = r.as_ref() {
                    trace!("writer finished with error: {e:#}");
                } else {
                    trace!("writer finished without error");
                }
                r
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use tokio::{sync::mpsc, time::Instant};

    use super::{
        DEFAULT_CONNECT_TIMEOUT, DEFAULT_HANDSHAKE_TIMEOUT, PeerConnection, PeerConnectionHandler,
        PeerConnectionOptions, WriterRequest,
    };
    use crate::{
        Error,
        buffers::ByteBuf,
        core::{
            hash_id::{Id20, InfoHashes},
            lengths::ChunkInfo,
        },
        limits::Channel,
        meter::Meter,
        mse::EncryptionPolicy,
        network::{
            BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, SocketFactory,
            StreamListener,
        },
        peer_info_reader::tests::{Dialed, halves},
        peer_protocol::{Handshake, Message, Piece, extended::handshake::ExtendedHandshake},
        read_buf::ReadBuf,
        session::CheckedIncomingConnection,
        spawn_utils::BlockingSpawner,
        stream_connect::{ConnectionKind, StreamConnector, StreamConnectorArgs},
    };

    struct Quiet;

    impl PeerConnectionHandler for Quiet {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn on_received_message(&self, _: Message<'_>) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn read_chunk(&self, _: &ChunkInfo, _: &mut [u8]) -> anyhow::Result<()> {
            Ok(())
        }
    }

    const HASH: [u8; 20] = [7; 20];

    /// A connection whose reads and writes may wait two minutes, so only
    /// the handshake timeout can end a handshake sooner.
    async fn connection(
        encryption: EncryptionPolicy,
    ) -> (
        PeerConnection<Quiet>,
        mpsc::UnboundedReceiver<(SocketAddr, tokio::io::DuplexStream)>,
    ) {
        let (tx, dialed) = mpsc::unbounded_channel();
        let connector = StreamConnector::new(StreamConnectorArgs {
            enable_tcp: true,
            encryption,
            ..StreamConnectorArgs::over(Arc::new(Dialed(tx)))
        })
        .await
        .unwrap();
        let options = PeerConnectionOptions {
            read_write_timeout: Some(Duration::from_secs(120)),
            ..Default::default()
        };
        let connection = PeerConnection::new(
            "127.0.0.1:6881".parse().unwrap(),
            InfoHashes {
                v1: Some(Id20::new(HASH)),
                v2: None,
            },
            Id20::new([1; 20]),
            Quiet,
            Some(options),
            BlockingSpawner::new(1),
            Arc::new(connector),
        );
        (connection, dialed)
    }

    // libtorrent `peer_connection::second_tick` drops a peer still in its
    // handshake `handshake_timeout` after it last heard from it, whatever
    // the peer timeout; the encryption handshake is part of it.
    #[tokio::test(start_paused = true)]
    async fn an_outgoing_handshake_unanswered_ends_at_the_handshake_timeout() {
        for (encryption, step) in [
            (EncryptionPolicy::Disabled, "reading"),
            (EncryptionPolicy::Require, "encryption handshake"),
        ] {
            let (connection, _dialed) = connection(encryption).await;
            let (_tx, rx) = mpsc::unbounded_channel();
            let start = Instant::now();
            let result = connection.manage_peer_outgoing(rx).await;
            assert!(
                matches!(result, Err(Error::Timeout(s)) if s == step),
                "{encryption:?}: {result:?}"
            );
            assert_eq!(start.elapsed(), DEFAULT_HANDSHAKE_TIMEOUT, "{encryption:?}");
        }
    }

    // libtorrent `peer_connection::keep_alive` sends a keepalive once
    // nothing has gone out for half the peer timeout.
    /// Connects that never complete.
    struct Unanswered;

    impl SocketFactory for Unanswered {
        fn connect_tcp<'a>(
            &'a self,
            _addr: SocketAddr,
        ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
            Box::pin(std::future::pending())
        }

        fn listen_tcp<'a>(
            &'a self,
            _addr: SocketAddr,
        ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
            Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
        }

        fn bind_udp<'a>(
            &'a self,
            _addr: SocketAddr,
        ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
            Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
        }
    }

    // libtorrent `peer_connection::second_tick` gives up a connect
    // `peer_connect_timeout` (15 s) after it was started.
    #[tokio::test(start_paused = true)]
    async fn a_connect_unanswered_is_given_up_at_the_connect_timeout() {
        let connector = StreamConnector::new(StreamConnectorArgs {
            enable_tcp: true,
            encryption: EncryptionPolicy::Disabled,
            ..StreamConnectorArgs::over(Arc::new(Unanswered))
        })
        .await
        .unwrap();
        let (mut connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        connection.connector = Arc::new(connector);
        let (_tx, rx) = mpsc::unbounded_channel();
        let start = Instant::now();
        let result = connection.manage_peer_outgoing(rx).await;
        assert!(
            matches!(result, Err(Error::Timeout("connecting"))),
            "{result:?}"
        );
        assert_eq!(start.elapsed(), DEFAULT_CONNECT_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_keepalive_goes_out_at_half_the_read_timeout_unless_set() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (read_write_timeout, keep_alive_interval, expected) in [
            (Some(120), None, 60),
            (None, None, 5),
            (Some(120), Some(30), 30),
        ] {
            let (mut connection, mut dialed) = connection(EncryptionPolicy::Disabled).await;
            connection.options = PeerConnectionOptions {
                read_write_timeout: read_write_timeout.map(Duration::from_secs),
                keep_alive_interval: keep_alive_interval.map(Duration::from_secs),
                ..Default::default()
            };
            let (_tx, rx) = mpsc::unbounded_channel();
            let task = tokio::spawn(async move { connection.manage_peer_outgoing(rx).await });
            let (_, mut peer) = dialed.recv().await.unwrap();
            let mut theirs = [0; 68];
            peer.read_exact(&mut theirs).await.unwrap();
            let mut ours = [0; 68];
            let len = Handshake::new(Id20::new(HASH), Id20::new([2; 20]))
                .serialize_unchecked_len(&mut ours);
            peer.write_all(&ours[..len]).await.unwrap();
            // Whatever goes out first, the keepalive is the first empty frame.
            let start = Instant::now();
            loop {
                let frame = peer.read_u32().await.unwrap();
                if frame == 0 {
                    break;
                }
                let mut body = vec![0; frame as usize];
                peer.read_exact(&mut body).await.unwrap();
            }
            assert_eq!(
                start.elapsed(),
                Duration::from_secs(expected),
                "{read_write_timeout:?} {keep_alive_interval:?}"
            );
            task.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_incoming_peer_that_takes_no_handshake_ends_at_the_handshake_timeout() {
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        // The peer reads nothing, so our handshake does not fit.
        let (ours, _theirs) = tokio::io::duplex(16);
        let (reader, writer) = halves(ours);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: writer.into(),
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::None,
            meter: Default::default(),
        };
        let (_tx, rx) = mpsc::unbounded_channel();
        let start = Instant::now();
        let result = connection.manage_peer_incoming(rx, incoming).await;
        assert!(
            matches!(result, Err(Error::Timeout("writing handshake"))),
            "{result:?}"
        );
        assert_eq!(start.elapsed(), DEFAULT_HANDSHAKE_TIMEOUT);
    }

    /// Grants at most [`Self::GRANT`] bytes a request, and keeps count of
    /// what it granted and did not have back, and what it was told crossed.
    #[derive(Default)]
    struct Ledger {
        paid: [AtomicU64; 2],
        protocol: [AtomicU64; 2],
        payload: AtomicU64,
        received: tokio::sync::watch::Sender<usize>,
    }

    impl Ledger {
        const GRANT: u64 = 5;
    }

    fn index(channel: Channel) -> usize {
        match channel {
            Channel::Download => 0,
            Channel::Upload => 1,
        }
    }

    impl PeerConnectionHandler for &Ledger {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn on_received_message(&self, msg: Message<'_>) -> anyhow::Result<()> {
            if let Message::Piece(piece) = &msg {
                self.payload
                    .fetch_add(piece.len() as u64, Ordering::Relaxed);
            }
            self.received.send_modify(|n| *n += 1);
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn read_chunk(&self, _: &ChunkInfo, _: &mut [u8]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn request_bandwidth(&self, channel: Channel, bytes: u64) -> u64 {
            let granted = bytes.min(Ledger::GRANT);
            self.paid[index(channel)].fetch_add(granted, Ordering::Relaxed);
            granted
        }
        fn return_bandwidth(&self, channel: Channel, bytes: u64) {
            self.paid[index(channel)].fetch_sub(bytes, Ordering::Relaxed);
        }
        fn on_protocol_bytes(&self, channel: Channel, bytes: u64) {
            self.protocol[index(channel)].fetch_add(bytes, Ordering::Relaxed);
        }
    }

    // Every byte that crosses the socket either way, the handshakes and each
    // message as well as a block's payload, is paid for before it crosses,
    // and once: what the limits granted is what crossed (libtorrent
    // `peer_connection::setup_receive`, `setup_send`, within `m_quota`).
    // Bytes that are not payload count as protocol bytes (`stat`).
    #[tokio::test(start_paused = true)]
    async fn every_byte_that_crosses_is_paid_for_once() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let ledger = Ledger::default();
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        let connection = PeerConnection::new(
            connection.addr,
            connection.info_hashes,
            connection.peer_id,
            &ledger,
            Some(connection.options),
            BlockingSpawner::new(1),
            connection.connector,
        );
        let (ours, mut theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = halves(ours);
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: writer.into(),
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::None,
            meter: meter.clone(),
        };
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(WriterRequest::Message(Message::Have(3))).unwrap();
        let mut received = ledger.received.subscribe();
        let peer = async {
            // Ours: the handshake, the extended handshake and the have.
            let mut handshake = [0; 68];
            theirs.read_exact(&mut handshake).await.unwrap();
            loop {
                let mut body = vec![0; theirs.read_u32().await.unwrap() as usize];
                theirs.read_exact(&mut body).await.unwrap();
                if body == [4, 0, 0, 0, 3] {
                    break;
                }
            }
            // Theirs: a have, a block of 100 bytes, a have.
            let mut wire = [0; 200];
            let block = [9; 100];
            let mut len = 0;
            for msg in [
                Message::Have(1),
                Message::Piece(Piece::from_data(0, 0, &block)),
                Message::Have(2),
            ] {
                len += msg.serialize(&mut wire[len..], &Default::default).unwrap();
            }
            theirs.write_all(&wire[..len]).await.unwrap();
            received.wait_for(|n| *n == 3).await.unwrap();
            drop(tx);
        };
        let (result, ()) = tokio::join!(connection.manage_peer_incoming(rx, incoming), peer);
        assert!(matches!(result, Err(Error::TorrentIsNotLive)), "{result:?}");

        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        assert_eq!(meter.received(), 9 + 13 + 100 + 9);
        // The length of the next frame, which the reader waited for, was
        // given back when the connection ended.
        assert_eq!(load(&ledger.paid[0]), meter.received());
        assert_eq!(load(&ledger.payload), 100);
        assert_eq!(load(&ledger.protocol[0]), meter.received() - 100);
        assert!(meter.sent() > 68 + 9);
        assert_eq!(load(&ledger.paid[1]), meter.sent());
        assert_eq!(load(&ledger.protocol[1]), meter.sent());
    }

    /// Advertises every extension of ours but ut_pex, and keeps the
    /// extended messages it is handed.
    #[derive(Default)]
    struct WithoutPex {
        read: tokio::sync::watch::Sender<Vec<String>>,
    }

    impl PeerConnectionHandler for &WithoutPex {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            Ok(())
        }
        fn update_my_extended_handshake(
            &self,
            handshake: &mut ExtendedHandshake<ByteBuf>,
        ) -> anyhow::Result<()> {
            handshake.m.ut_pex = None;
            Ok(())
        }
        async fn on_received_message(&self, msg: Message<'_>) -> anyhow::Result<()> {
            if let Message::Extended(e) = msg {
                self.read.send_modify(|read| read.push(format!("{e:?}")));
            }
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn read_chunk(&self, _: &ChunkInfo, _: &mut [u8]) -> anyhow::Result<()> {
            Ok(())
        }
    }

    // A connection reads a message under one of our ids only where it
    // advertised that id: PEX on a connection that did not advertise it is
    // handed on unread, and an id that is none of ours ends the connection
    // (libtorrent bt_peer_connection.cpp:1980-1988, ut_pex.cpp:270, v2.0.15).
    #[tokio::test(start_paused = true)]
    async fn a_connection_reads_only_the_extensions_it_advertised() {
        use crate::peer_protocol::MY_EXTENDED_UT_PEX;
        // BEP 10's message id.
        const MSGID_EXTENDED: u8 = 20;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let handler = WithoutPex::default();
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        let connection = PeerConnection::new(
            connection.addr,
            connection.info_hashes,
            connection.peer_id,
            &handler,
            Some(connection.options),
            BlockingSpawner::new(1),
            connection.connector,
        );
        let (ours, mut theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = halves(ours);
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: writer.into(),
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::None,
            meter,
        };
        let (_tx, rx) = mpsc::unbounded_channel();
        let extended = |id: u8, payload: &[u8]| {
            let mut frame = u32::try_from(2 + payload.len())
                .unwrap()
                .to_be_bytes()
                .to_vec();
            frame.extend_from_slice(&[MSGID_EXTENDED, id]);
            frame.extend_from_slice(payload);
            frame
        };
        let peer = async {
            let mut handshake = [0; 68];
            theirs.read_exact(&mut handshake).await.unwrap();
            // A PEX message no reader of PEX would take, then an id that is
            // none of ours.
            let mut frames = extended(MY_EXTENDED_UT_PEX, b"\xff");
            frames.extend(extended(9, b"d1:xi1ee"));
            theirs.write_all(&frames).await.unwrap();
        };
        let (result, ()) = tokio::join!(connection.manage_peer_incoming(rx, incoming), peer);
        assert_eq!(
            *handler.read.borrow(),
            [format!("NotAdvertised({MY_EXTENDED_UT_PEX})")]
        );
        assert!(
            matches!(
                result,
                Err(Error::Deserialize(
                    crate::peer_protocol::MessageDeserializeError::UnknownExtendedId(9)
                ))
            ),
            "{result:?}"
        );
    }

    /// Answers every Have with 512 hashes, about 16 KiB, as a peer's hash
    /// requests are answered.
    struct Answering {
        tx: mpsc::UnboundedSender<WriterRequest>,
        answers: super::AnswerBacklog,
        received: AtomicU64,
    }

    impl PeerConnectionHandler for &Answering {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn on_received_message(&self, msg: Message<'_>) -> anyhow::Result<()> {
            if let Message::Have(index) = msg {
                self.received.fetch_add(1, Ordering::SeqCst);
                let request = crate::peer_protocol::HashRequest {
                    root: crate::core::hash_id::Id32::new([1; 32]),
                    base: 0,
                    index,
                    count: 512,
                    proof_layers: 0,
                };
                let hashes = vec![crate::core::hash_id::Id32::new([2; 32]); 512];
                let answer =
                    super::Answer::Hashes(crate::peer_protocol::Hashes::new(request, &hashes));
                self.answers.queued(&answer);
                let _ = self.tx.send(WriterRequest::Answer(answer));
            }
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn read_chunk(&self, _: &ChunkInfo, _: &mut [u8]) -> anyhow::Result<()> {
            Ok(())
        }
        fn answers(&self) -> Option<&super::AnswerBacklog> {
            Some(&self.answers)
        }
    }

    // A peer that sends requests and reads none of the answers is read from
    // only until its answers hold ANSWER_BACKLOG_LIMIT bytes: the rest of
    // what it sent stays unread, and the connection ends at the write
    // timeout, its queue bounded.
    #[tokio::test(start_paused = true)]
    async fn a_peer_that_reads_no_answers_is_not_read_from_past_the_backlog_limit() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (tx, rx) = mpsc::unbounded_channel();
        let answering = Answering {
            tx,
            answers: Default::default(),
            received: AtomicU64::new(0),
        };
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        let connection = PeerConnection::new(
            connection.addr,
            connection.info_hashes,
            connection.peer_id,
            &answering,
            Some(connection.options),
            BlockingSpawner::new(1),
            connection.connector,
        );
        let (ours, mut theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = halves(ours);
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: writer.into(),
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::None,
            meter,
        };
        const SENT: u32 = 200;
        let peer = async {
            let mut handshake = [0; 68];
            theirs.read_exact(&mut handshake).await.unwrap();
            let mut wire = vec![0; 9 * SENT as usize];
            let mut len = 0;
            for index in 0..SENT {
                len += Message::Have(index)
                    .serialize(&mut wire[len..], &Default::default)
                    .unwrap();
            }
            theirs.write_all(&wire[..len]).await.unwrap();
            // Nothing more is read: the answers fill the stream and stop.
            theirs
        };
        let (result, _theirs) = tokio::join!(connection.manage_peer_incoming(rx, incoming), peer);
        assert!(result.is_err(), "{result:?}");

        let answer_len = 53 + 512 * 32;
        let received = usize::try_from(answering.received.load(Ordering::SeqCst)).unwrap();
        // The reader stopped once the backlog reached the limit: past the
        // answers the stream holds, at most one answer more than the limit.
        assert!(
            received < SENT as usize,
            "every request was read: {received}"
        );
        assert!(
            received >= super::ANSWER_BACKLOG_LIMIT / answer_len,
            "{received}"
        );
        assert!(
            received <= (super::ANSWER_BACKLOG_LIMIT + (1 << 16)) / answer_len + 2,
            "{received}"
        );
        assert!(answering.answers.held() >= super::ANSWER_BACKLOG_LIMIT - (1 << 16));
    }

    /// Serves every block but those of piece 0, whose check failed.
    struct Picky;

    impl PeerConnectionHandler for &Picky {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn on_received_message(&self, _: Message<'_>) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn claim_upload(&self, _: &ChunkInfo) -> bool {
            true
        }
        async fn serves(&self, chunk: &ChunkInfo) -> anyhow::Result<bool> {
            Ok(chunk.piece_index.get() != 0)
        }
        fn read_chunk(&self, _: &ChunkInfo, buf: &mut [u8]) -> anyhow::Result<()> {
            buf.fill(0xab);
            Ok(())
        }
    }

    // libtorrent `peer_connection::on_seed_mode_hashed`: a piece that fails
    // its seed-mode check is not sent, and the connection is not ended for
    // it. The block asked for after it is sent on the same connection. A
    // fast peer is sent a Reject for the block in its place
    // (`peer_connection::fill_send_buffer`), a plain one nothing.
    #[tokio::test(start_paused = true)]
    async fn a_block_not_served_is_dropped_and_the_connection_kept() {
        assert_eq!(serve_picky(true).await, (vec![1], vec![(0, 0, 16384)]));
        assert_eq!(serve_picky(false).await, (vec![1], vec![]));
    }

    /// Serves blocks 0 and 1 to a peer that sets the fast bit when `fast`:
    /// the pieces of the blocks sent, and the Rejects, by piece, offset and
    /// length.
    async fn serve_picky(fast: bool) -> (Vec<u32>, Vec<(u32, u32, u32)>) {
        use tokio::io::AsyncReadExt;
        const BLOCK: u32 = 16384;
        let picky = Picky;
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        let connection = PeerConnection::new(
            connection.addr,
            connection.info_hashes,
            connection.peer_id,
            &picky,
            Some(connection.options),
            BlockingSpawner::new(1),
            connection.connector,
        );
        let (ours, mut theirs) = tokio::io::duplex(1 << 18);
        let (reader, writer) = halves(ours);
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: writer.into(),
            read_buf: ReadBuf::new(),
            handshake: {
                let mut handshake = Handshake::new(Id20::new(HASH), Id20::new([2; 20]));
                if !fast {
                    handshake.reserved &= !u64::from(crate::peer_protocol::FAST_BIT);
                }
                handshake
            },
            encryption: crate::mse::Encryption::None,
            meter,
        };
        let lengths = crate::core::lengths::Lengths::new(2 * BLOCK as u64, BLOCK).unwrap();
        let block = |piece| {
            lengths
                .chunk_info_from_received_data(
                    lengths.validate_piece_index(piece).unwrap(),
                    0,
                    BLOCK,
                )
                .unwrap()
        };
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(WriterRequest::ReadChunkRequest(block(0))).unwrap();
        tx.send(WriterRequest::ReadChunkRequest(block(1))).unwrap();
        tx.send(WriterRequest::Message(Message::Have(3))).unwrap();
        let peer = async {
            let mut handshake = [0; 68];
            theirs.read_exact(&mut handshake).await.unwrap();
            // The pieces of the blocks sent and the Rejects, until the have
            // after them.
            let mut sent = Vec::new();
            let mut rejects = Vec::new();
            let word =
                |body: &[u8], at: usize| u32::from_be_bytes(body[at..at + 4].try_into().unwrap());
            loop {
                let mut body = vec![0; theirs.read_u32().await.unwrap() as usize];
                theirs.read_exact(&mut body).await.unwrap();
                if body == [4, 0, 0, 0, 3] {
                    break;
                }
                match body.first() {
                    Some(&7) => {
                        sent.push(word(&body, 1));
                        assert!(body[9..].iter().all(|byte| *byte == 0xab));
                    }
                    Some(&16) => rejects.push((word(&body, 1), word(&body, 5), word(&body, 9))),
                    _ => {}
                }
            }
            drop(tx);
            (sent, rejects)
        };
        let (result, told) = tokio::join!(connection.manage_peer_incoming(rx, incoming), peer);
        // The connection ended only when its torrent let go of it.
        assert!(matches!(result, Err(Error::TorrentIsNotLive)), "{result:?}");
        told
    }

    /// Records the blocks cut short it is told of; reads under the download
    /// `limits`, asking them for `wanted` besides the frame.
    struct Cut {
        partial: parking_lot::Mutex<Vec<crate::peer_protocol::PartialBlock>>,
        limits: crate::limits::Limits,
        wanted: u64,
        asks: tokio::sync::watch::Sender<usize>,
    }

    impl Cut {
        fn new(download: crate::limits::LimitKind, wanted: u64) -> Self {
            Self {
                partial: Default::default(),
                limits: crate::limits::Limits::new(crate::limits::LimitsConfig {
                    download_bps: download,
                    ..Default::default()
                }),
                wanted,
                asks: tokio::sync::watch::Sender::new(0),
            }
        }

        fn partial(&self) -> Vec<crate::peer_protocol::PartialBlock> {
            self.partial.lock().clone()
        }
    }

    impl PeerConnectionHandler for &Cut {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn on_received_message(&self, _: Message<'_>) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn read_chunk(&self, _: &ChunkInfo, _: &mut [u8]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn request_bandwidth(&self, channel: Channel, bytes: u64) -> u64 {
            if channel == Channel::Download {
                self.asks.send_modify(|n| *n += 1);
            }
            self.limits.request(channel, bytes).await
        }
        fn return_bandwidth(&self, channel: Channel, bytes: u64) {
            self.limits.give_back(channel, bytes);
        }
        fn on_partial_block(&self, block: crate::peer_protocol::PartialBlock) {
            self.partial.lock().push(block);
        }
        fn download_wanted(&self) -> u64 {
            self.wanted
        }
    }

    const BLOCK: u32 = 16384;

    /// A Piece message of block `begin / BLOCK` of piece 0, `sent` of its
    /// bytes.
    fn piece_frame(begin: u32, sent: usize) -> Vec<u8> {
        let mut frame = (9 + BLOCK).to_be_bytes().to_vec();
        frame.push(7);
        frame.extend(0u32.to_be_bytes());
        frame.extend(begin.to_be_bytes());
        frame.extend(vec![0xab; sent]);
        frame
    }

    /// An incoming connection that reads what the peer writes to `from`
    /// and writes what we send to `to`, and how many bytes it has read.
    fn incoming_over(
        from: tokio::io::DuplexStream,
        to: tokio::io::DuplexStream,
    ) -> (
        CheckedIncomingConnection,
        tokio::sync::watch::Receiver<usize>,
    ) {
        let (reader, _, taken) = crate::peer_info_reader::tests::watched_halves(from);
        let (_, writer) = halves(to);
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: writer.into(),
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::None,
            meter,
        };
        (incoming, taken)
    }

    async fn connection_for(handler: &Cut) -> PeerConnection<&Cut> {
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        PeerConnection::new(
            connection.addr,
            connection.info_hashes,
            connection.peer_id,
            handler,
            Some(connection.options),
            BlockingSpawner::new(1),
            connection.connector,
        )
    }

    /// How a connection ends.
    #[derive(Debug, Clone, Copy)]
    enum Ending {
        /// The peer closes: our read meets its end.
        PeerCloses,
        /// The peer sends nothing more: the read times out.
        ReadTimesOut,
        /// The peer stops reading: our next write fails.
        WriteFails,
        /// The torrent asks the connection to close.
        Disconnected,
        /// The torrent lets go of the connection's queue.
        TorrentGone,
        /// The connection's task is dropped: a pause, a stop, the peer's
        /// requester ending, the torrent cancelled.
        Dropped,
    }

    // libtorrent `peer_connection::disconnect` counts the part of a block
    // that arrived, as `downloading_piece_progress` finds it at the front of
    // the receive buffer, as redundant, whatever ended the connection. A
    // connection that ends between messages counts nothing.
    #[tokio::test(start_paused = true)]
    async fn a_block_cut_short_is_told_once_however_the_connection_ends() {
        use tokio::io::AsyncWriteExt;
        const RECEIVED: usize = 8192;
        let cut = crate::peer_protocol::PartialBlock {
            index: 0,
            begin: BLOCK,
            len: BLOCK,
            received: u32::try_from(RECEIVED).unwrap(),
        };
        let have = {
            let mut wire = [0u8; 9];
            Message::Have(0)
                .serialize(&mut wire, &Default::default)
                .unwrap();
            wire.to_vec()
        };
        for (sent, expected) in [(piece_frame(BLOCK, RECEIVED), vec![cut]), (have, vec![])] {
            for ending in [
                Ending::PeerCloses,
                Ending::ReadTimesOut,
                Ending::WriteFails,
                Ending::Disconnected,
                Ending::TorrentGone,
                Ending::Dropped,
            ] {
                let handler = Cut::new(crate::limits::LimitKind::Unlimited, 0);
                let connection = connection_for(&handler).await;
                let (from, mut peer_writes) = tokio::io::duplex(1 << 16);
                let (to, peer_reads) = tokio::io::duplex(1 << 16);
                let (incoming, mut taken) = incoming_over(from, to);
                let (tx, rx) = mpsc::unbounded_channel();
                let mut conn = Box::pin(connection.manage_peer_incoming(rx, incoming));

                peer_writes.write_all(&sent).await.unwrap();
                tokio::select! {
                    r = &mut conn => panic!("{ending:?}: ended early: {r:?}"),
                    r = taken.wait_for(|n| *n == sent.len()) => { r.unwrap(); }
                }
                let result = match ending {
                    Ending::PeerCloses => {
                        drop(peer_writes);
                        Some(conn.await)
                    }
                    Ending::ReadTimesOut => Some(conn.await),
                    Ending::WriteFails => {
                        drop(peer_reads);
                        tx.send(WriterRequest::Message(Message::Have(1))).unwrap();
                        Some(conn.await)
                    }
                    Ending::Disconnected => {
                        tx.send(WriterRequest::Disconnect(Ok(()))).unwrap();
                        Some(conn.await)
                    }
                    Ending::TorrentGone => {
                        drop(tx);
                        Some(conn.await)
                    }
                    Ending::Dropped => {
                        drop(conn);
                        None
                    }
                };
                if let Some(result) = result {
                    let ended_as_expected = match ending {
                        Ending::PeerCloses => matches!(result, Err(Error::PeerDisconnected)),
                        Ending::ReadTimesOut => matches!(result, Err(Error::Timeout("reading"))),
                        Ending::WriteFails => matches!(result, Err(Error::Write(_))),
                        Ending::Disconnected => matches!(result, Err(Error::Disconnect)),
                        Ending::TorrentGone => matches!(result, Err(Error::TorrentIsNotLive)),
                        Ending::Dropped => unreachable!(),
                    };
                    assert!(ended_as_expected, "{ending:?}: {result:?}");
                }
                assert_eq!(handler.partial(), expected, "{ending:?}");
            }
        }
    }

    // libtorrent `peer_connection::wanted_transfer` asks the download
    // limits for the bytes of the blocks requested and not yet received, so
    // a round's grant takes off the socket every block the peer sent. Asking
    // a frame at a time, a connection under a limit takes a frame a round,
    // and a peer that sends the blocks asked for and then a cut one, and
    // stops reading, leaves the cut block on our socket when our next write
    // fails: none of it counts.
    #[tokio::test(start_paused = true)]
    async fn under_a_limit_a_round_takes_every_block_asked_for_and_a_cut_one_counts() {
        use tokio::io::AsyncWriteExt;
        const RECEIVED: usize = 8192;
        let limit = crate::limits::LimitKind::Bps(std::num::NonZeroU32::new(1 << 20).unwrap());
        let handler = Cut::new(limit, 3 * u64::from(BLOCK) + 30);
        let connection = connection_for(&handler).await;
        let (from, mut peer_writes) = tokio::io::duplex(1 << 18);
        let (to, peer_reads) = tokio::io::duplex(1 << 16);
        let (incoming, mut taken) = incoming_over(from, to);
        let (tx, rx) = mpsc::unbounded_channel();
        let mut conn = std::pin::pin!(connection.manage_peer_incoming(rx, incoming));

        // Two blocks asked for, and the third cut short.
        let mut sent = piece_frame(0, BLOCK as usize);
        sent.extend(piece_frame(BLOCK, BLOCK as usize));
        sent.extend(piece_frame(2 * BLOCK, RECEIVED));
        peer_writes.write_all(&sent).await.unwrap();
        // Until our end has read all of it, or waits on the limits a second
        // time with some of it left on the socket.
        let mut asks = handler.asks.subscribe();
        tokio::select! {
            r = &mut conn => panic!("ended early: {r:?}"),
            r = taken.wait_for(|n| *n == sent.len()) => { r.unwrap(); }
            r = asks.wait_for(|n| *n >= 2) => { r.unwrap(); }
        }
        // The peer stops reading, and our next write fails.
        drop(peer_reads);
        tx.send(WriterRequest::Message(Message::Have(1))).unwrap();
        let result = conn.await;
        assert!(matches!(result, Err(Error::Write(_))), "{result:?}");
        assert_eq!(*taken.borrow(), sent.len());
        assert_eq!(
            handler.partial(),
            vec![crate::peer_protocol::PartialBlock {
                index: 0,
                begin: 2 * BLOCK,
                len: BLOCK,
                received: u32::try_from(RECEIVED).unwrap(),
            }]
        );
    }

    #[test]
    fn timeouts_serialize_as_rounded_whole_seconds() {
        let options = PeerConnectionOptions {
            connect_timeout: Some(Duration::from_millis(1499)),
            read_write_timeout: Some(Duration::from_millis(1500)),
            keep_alive_interval: None,
            handshake_timeout: Some(Duration::from_millis(2500)),
            max_request_window: None,
            encryption: None,
        };
        assert_eq!(
            serde_json::to_value(options).unwrap(),
            serde_json::json!({
                "connect_timeout": 1,
                "read_write_timeout": 2,
                "keep_alive_interval": null,
                "handshake_timeout": 3,
                "max_request_window": null,
                "encryption": null,
            })
        );
        let parsed: PeerConnectionOptions =
            serde_json::from_str(r#"{"connect_timeout": 3, "max_request_window": null}"#).unwrap();
        assert_eq!(
            parsed,
            PeerConnectionOptions {
                connect_timeout: Some(Duration::from_secs(3)),
                ..Default::default()
            }
        );
    }
    /// One thing the writer task told the handler, in the order told.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Told {
        Granted(u64),
        Returned(u64),
        Protocol(u64),
        Uploaded(u32),
        Done(u32),
        Written(u64),
    }

    /// Serves every block, filled from its piece index, and logs what the
    /// writer task tells it about the upload channel.
    #[derive(Default)]
    struct Recorder {
        told: parking_lot::Mutex<Vec<Told>>,
        /// The piece whose block fails to read, if any.
        fails: Option<u32>,
    }

    impl PeerConnectionHandler for &Recorder {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn on_received_message(&self, _: Message<'_>) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_uploaded_bytes(&self, bytes: u32) {
            self.told.lock().push(Told::Uploaded(bytes));
        }
        fn on_payload_written(&self, bytes: u64) {
            self.told.lock().push(Told::Written(bytes));
        }
        fn claim_upload(&self, _: &ChunkInfo) -> bool {
            true
        }
        fn upload_done(&self, chunk: &ChunkInfo) {
            self.told.lock().push(Told::Done(chunk.piece_index.get()));
        }
        fn read_chunk(&self, chunk: &ChunkInfo, buf: &mut [u8]) -> anyhow::Result<()> {
            anyhow::ensure!(
                self.fails != Some(chunk.piece_index.get()),
                "piece {} cannot be read",
                chunk.piece_index.get()
            );
            for (k, b) in buf.iter_mut().enumerate() {
                *b = u8::try_from((k + chunk.piece_index.get() as usize) % 239).unwrap();
            }
            Ok(())
        }
        async fn request_bandwidth(&self, channel: Channel, bytes: u64) -> u64 {
            if channel == Channel::Upload {
                self.told.lock().push(Told::Granted(bytes));
            }
            bytes
        }
        fn return_bandwidth(&self, channel: Channel, bytes: u64) {
            if channel == Channel::Upload {
                self.told.lock().push(Told::Returned(bytes));
            }
        }
        fn on_protocol_bytes(&self, channel: Channel, bytes: u64) {
            if channel == Channel::Upload {
                self.told.lock().push(Told::Protocol(bytes));
            }
        }
    }

    /// Takes at most 5000 bytes a write, and is pending on every other poll.
    struct Halting {
        inner: BoxAsyncWrite,
        polls: usize,
    }

    impl tokio::io::AsyncWrite for Halting {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            let this = self.get_mut();
            this.polls += 1;
            if this.polls.is_multiple_of(2) {
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
            let n = buf.len().min(5000);
            std::pin::Pin::new(&mut this.inner).poll_write(cx, &buf[..n])
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    /// Serves two blocks and a have to a peer over a socket that writes in
    /// short pieces with Pendings between them, under RC4 or not. Returns what
    /// the handler was told, the bytes on the wire, those bytes as plain
    /// text, and what the meter counted as sent.
    async fn serve_two_blocks(rc4: bool) -> (Vec<Told>, Vec<u8>, Vec<u8>, u64) {
        serve_blocks(rc4, 2).await
    }

    /// Serves `blocks` blocks, one a piece, queued at once, and a have behind
    /// them, as [`serve_two_blocks`] does.
    async fn serve_blocks(rc4: bool, blocks: u32) -> (Vec<Told>, Vec<u8>, Vec<u8>, u64) {
        use tokio::io::AsyncReadExt;
        let recorder = Recorder::default();
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        let connection = PeerConnection::new(
            connection.addr,
            connection.info_hashes,
            connection.peer_id,
            &recorder,
            Some(connection.options),
            BlockingSpawner::new(1),
            connection.connector,
        );
        let (ours, mut theirs) = tokio::io::duplex(1 << 18);
        let (reader, writer) = halves(ours);
        let writer: BoxAsyncWrite = Box::new(Halting {
            inner: writer,
            polls: 0,
        });
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        let writer = if rc4 {
            crate::mse::Outbound::rc4(writer, crate::mse::Rc4::new_discard_1024(b"key"))
        } else {
            writer.into()
        };
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer,
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::None,
            meter: meter.clone(),
        };
        let lengths =
            crate::core::lengths::Lengths::new(u64::from(blocks) * u64::from(BLOCK), BLOCK)
                .unwrap();
        let block = |piece| {
            lengths
                .chunk_info_from_received_data(
                    lengths.validate_piece_index(piece).unwrap(),
                    0,
                    BLOCK,
                )
                .unwrap()
        };
        let (tx, rx) = mpsc::unbounded_channel();
        for piece in 0..blocks {
            tx.send(WriterRequest::ReadChunkRequest(block(piece)))
                .unwrap();
        }
        tx.send(WriterRequest::Message(Message::Have(3))).unwrap();
        let peer = async {
            let mut cipher = rc4.then(|| crate::mse::Rc4::new_discard_1024(b"key"));
            let mut wire = Vec::new();
            let mut plain = Vec::new();
            let mut chunk = [0u8; 4096];
            // Until the have closes the stream: its five bytes end it.
            while !plain.ends_with(&[0, 0, 0, 5, 4, 0, 0, 0, 3]) {
                let n = theirs.read(&mut chunk).await.unwrap();
                assert_ne!(n, 0, "the stream ended before the have");
                wire.extend_from_slice(&chunk[..n]);
                let mut got = chunk[..n].to_vec();
                if let Some(cipher) = &mut cipher {
                    cipher.apply(&mut got);
                }
                plain.extend_from_slice(&got);
            }
            drop(tx);
            (wire, plain)
        };
        let (result, (wire, plain)) =
            tokio::join!(connection.manage_peer_incoming(rx, incoming), peer);
        assert!(matches!(result, Err(Error::TorrentIsNotLive)), "{result:?}");
        let told = recorder.told.lock().clone();
        (told, wire, plain, meter.sent())
    }

    // A run of blocks queued together goes out in writes of at most
    // BATCH_BLOCKS blocks, each paid for whole, and the have behind them
    // rides with the last.
    #[tokio::test(start_paused = true)]
    async fn a_long_run_of_blocks_goes_out_a_batch_a_write() {
        let batch = u32::try_from(super::BATCH_BLOCKS).unwrap();
        let blocks = batch + 8;
        let (told, wire, _, sent) = serve_blocks(false, blocks).await;
        assert_eq!(sent, wire.len() as u64);
        let frame = 13 + u64::from(BLOCK);
        let grants: Vec<u64> = told
            .iter()
            .filter_map(|t| match t {
                Told::Granted(n) => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(
            grants[grants.len() - 2..],
            [u64::from(batch) * frame, 8 * frame + 9]
        );
        let done: Vec<u32> = told
            .iter()
            .filter_map(|t| match t {
                Told::Done(piece) => Some(*piece),
                _ => None,
            })
            .collect();
        assert_eq!(done, (0..blocks).collect::<Vec<_>>());
    }

    // Under RC4 the writer task encrypts each write in place before it goes
    // out; short writes and Pendings below the cipher change nothing
    // the handler is told, nor its order, and the receiver decrypts the same
    // bytes a plain peer gets as they are.
    #[tokio::test(start_paused = true)]
    async fn an_rc4_upload_is_accounted_as_a_plain_one_and_decrypts_byte_exact() {
        let (told_plain, wire_plain, plain_plain, sent_plain) = serve_two_blocks(false).await;
        let (told_rc4, wire_rc4, plain_rc4, sent_rc4) = serve_two_blocks(true).await;

        // A plain peer gets the bytes as the writer task built them.
        assert_eq!(wire_plain, plain_plain);
        assert_eq!(plain_rc4, plain_plain);
        assert_ne!(wire_rc4, plain_rc4);
        assert_eq!(told_rc4, told_plain);
        for (told, wire, sent) in [
            (&told_plain, &wire_plain, sent_plain),
            (&told_rc4, &wire_rc4, sent_rc4),
        ] {
            assert_eq!(sent, wire.len() as u64);
            let sum = |pick: fn(&Told) -> Option<u64>| told.iter().filter_map(pick).sum::<u64>();
            let granted = sum(|t| match t {
                Told::Granted(n) => Some(*n),
                _ => None,
            });
            let returned = sum(|t| match t {
                Told::Returned(n) => Some(*n),
                _ => None,
            });
            let protocol = sum(|t| match t {
                Told::Protocol(n) => Some(*n),
                _ => None,
            });
            assert_eq!(granted - returned, sent);
            assert_eq!(protocol, sent - 2 * u64::from(BLOCK));
            // Both blocks and the have queued behind them are paid for and
            // written together; then each block's protocol bytes, its
            // payload, and done, in order, and the have's.
            let tail: Vec<_> = told
                .iter()
                .filter(|t| matches!(t, Told::Uploaded(_) | Told::Done(_)))
                .cloned()
                .collect();
            assert_eq!(
                tail,
                [
                    Told::Uploaded(BLOCK),
                    Told::Done(0),
                    Told::Uploaded(BLOCK),
                    Told::Done(1)
                ]
            );
            let done0 = told.iter().position(|t| *t == Told::Done(0)).unwrap();
            assert_eq!(
                told[done0 - 3..done0 + 5],
                [
                    Told::Granted(2 * (13 + u64::from(BLOCK)) + 9),
                    Told::Protocol(13),
                    Told::Uploaded(BLOCK),
                    Told::Done(0),
                    Told::Protocol(13),
                    Told::Uploaded(BLOCK),
                    Told::Done(1),
                    Told::Protocol(9),
                ]
            );
            // The one write that carried both blocks is told once, after
            // them, with both blocks' payload.
            let written: Vec<_> = told
                .iter()
                .filter(|t| matches!(t, Told::Written(_)))
                .cloned()
                .collect();
            assert_eq!(written, [Told::Written(2 * u64::from(BLOCK))]);
            assert_eq!(told[done0 + 5], Told::Written(2 * u64::from(BLOCK)));
        }
        // The two blocks' payloads, as the receiver sees them.
        for piece in 0..2u32 {
            let begin = plain_rc4
                .windows(9)
                .position(|w| w[..5] == [0, 0, 0x40, 9, 7] && w[5..] == piece.to_be_bytes())
                .unwrap()
                + 13;
            let payload = &plain_rc4[begin..begin + BLOCK as usize];
            assert!(
                payload
                    .iter()
                    .enumerate()
                    .all(|(k, b)| usize::from(*b) == (k + piece as usize) % 239)
            );
        }
    }

    /// Logs the length of every write handed to it.
    struct Logging {
        inner: BoxAsyncWrite,
        writes: Arc<parking_lot::Mutex<Vec<usize>>>,
    }

    impl tokio::io::AsyncWrite for Logging {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            let this = self.get_mut();
            let polled = std::pin::Pin::new(&mut this.inner).poll_write(cx, buf);
            if polled.is_ready() {
                this.writes.lock().push(buf.len());
            }
            polled
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    fn have_frame(piece: u32) -> Vec<u8> {
        let mut frame = vec![0, 0, 0, 5, 4];
        frame.extend_from_slice(&piece.to_be_bytes());
        frame
    }

    /// The frame of `piece`'s block as `Recorder` reads it.
    fn block_frame(piece: u32) -> Vec<u8> {
        let mut frame = (9 + BLOCK).to_be_bytes().to_vec();
        frame.push(7);
        frame.extend_from_slice(&piece.to_be_bytes());
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame
            .extend((0..BLOCK as usize).map(|k| u8::try_from((k + piece as usize) % 239).unwrap()));
        frame
    }

    /// Serves `queued`, every request on the queue before the writer task
    /// starts, until the bytes on the wire end with `until` or the
    /// connection ends. Returns how it ended, the bytes on the wire, and the
    /// length of each write, the handshakes' first.
    async fn serve_queued(
        recorder: &Recorder,
        queued: Vec<WriterRequest>,
        until: &[u8],
    ) -> (crate::Result<()>, Vec<u8>, Vec<usize>) {
        use tokio::io::AsyncReadExt;
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        let connection = PeerConnection::new(
            connection.addr,
            connection.info_hashes,
            connection.peer_id,
            recorder,
            Some(connection.options),
            BlockingSpawner::new(1),
            connection.connector,
        );
        let (ours, mut theirs) = tokio::io::duplex(1 << 18);
        let (reader, writer) = halves(ours);
        let writes = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let writer: BoxAsyncWrite = Box::new(Logging {
            inner: writer,
            writes: writes.clone(),
        });
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: writer.into(),
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::None,
            meter,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        for request in queued {
            tx.send(request).unwrap();
        }
        let peer = async {
            let mut wire = Vec::new();
            let mut chunk = [0u8; 4096];
            while !wire.ends_with(until) {
                let n = theirs.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                wire.extend_from_slice(&chunk[..n]);
            }
            drop(tx);
            wire
        };
        let (result, wire) = tokio::join!(connection.manage_peer_incoming(rx, incoming), peer);
        let writes = writes.lock().clone();
        (result, wire, writes)
    }

    fn blocks_of(pieces: u32) -> impl Fn(u32) -> ChunkInfo {
        let lengths =
            crate::core::lengths::Lengths::new(u64::from(pieces) * u64::from(BLOCK), BLOCK)
                .unwrap();
        move |piece| {
            lengths
                .chunk_info_from_received_data(
                    lengths.validate_piece_index(piece).unwrap(),
                    0,
                    BLOCK,
                )
                .unwrap()
        }
    }

    // What is queued together goes out in one write, in order, as much as the
    // buffer holds: here three haves and two blocks. The have queued after
    // them does not fit behind the blocks, and is the next write.
    #[tokio::test(start_paused = true)]
    async fn messages_queued_together_go_out_in_one_write() {
        let recorder = Recorder::default();
        let block = blocks_of(5);
        let queued = vec![
            WriterRequest::Message(Message::Have(1)),
            WriterRequest::Message(Message::Have(2)),
            WriterRequest::Message(Message::Have(3)),
            WriterRequest::ReadChunkRequest(block(0)),
            WriterRequest::ReadChunkRequest(block(1)),
            WriterRequest::Message(Message::Have(4)),
        ];
        let (result, wire, writes) = serve_queued(&recorder, queued, &have_frame(4)).await;
        assert!(matches!(result, Err(Error::TorrentIsNotLive)), "{result:?}");

        let batch = [
            have_frame(1),
            have_frame(2),
            have_frame(3),
            block_frame(0),
            block_frame(1),
        ]
        .concat();
        assert!(
            writes.ends_with(&[batch.len(), have_frame(4).len()]),
            "{writes:?}"
        );
        assert!(wire.ends_with(&[batch, have_frame(4)].concat()));
        let told = recorder.told.lock().clone();
        let tail: Vec<_> = told
            .iter()
            .filter(|t| matches!(t, Told::Uploaded(_) | Told::Done(_)))
            .cloned()
            .collect();
        assert_eq!(
            tail,
            [
                Told::Uploaded(BLOCK),
                Told::Done(0),
                Told::Uploaded(BLOCK),
                Told::Done(1)
            ]
        );
    }

    // A block that fails to read ends the connection once the frames queued
    // before it are out; neither it nor what was queued after it is sent or
    // done.
    #[tokio::test(start_paused = true)]
    async fn a_block_that_fails_to_read_ends_the_connection_after_the_frames_before_it() {
        let recorder = Recorder {
            fails: Some(1),
            ..Recorder::default()
        };
        let block = blocks_of(5);
        let queued = vec![
            WriterRequest::Message(Message::Have(1)),
            WriterRequest::ReadChunkRequest(block(0)),
            WriterRequest::ReadChunkRequest(block(1)),
            WriterRequest::Message(Message::Have(2)),
        ];
        let (result, wire, writes) = serve_queued(&recorder, queued, &have_frame(2)).await;
        assert!(matches!(result, Err(Error::ReadChunk(_))), "{result:?}");

        let sent = [have_frame(1), block_frame(0)].concat();
        assert_eq!(writes.last(), Some(&sent.len()), "{writes:?}");
        assert!(wire.ends_with(&sent));
        let told = recorder.told.lock().clone();
        assert!(told.contains(&Told::Done(0)));
        assert!(!told.contains(&Told::Done(1)));
    }

    /// Records the extension ids each extended handshake leaves the
    /// connection with; after the `n`th, queues a PEX message and then a
    /// Have of `n`.
    #[derive(Default)]
    struct Shook {
        seen: parking_lot::Mutex<Vec<crate::peer_protocol::extended::PeerExtendedMessageIds>>,
        tx: parking_lot::Mutex<Option<mpsc::UnboundedSender<WriterRequest>>>,
    }

    impl PeerConnectionHandler for &Shook {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, h: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            let mut seen = self.seen.lock();
            seen.push(h.m);
            let tx = self.tx.lock();
            let tx = tx.as_ref().unwrap();
            let pex = crate::peer_protocol::extended::ut_pex::UtPex::from_peers(
                std::iter::once(crate::peer_protocol::extended::ut_pex::PexPeerInfo {
                    flags: 0,
                    addr: "10.0.0.1:6881".parse().unwrap(),
                }),
                std::iter::empty(),
            );
            tx.send(WriterRequest::UtPex(pex)).unwrap();
            let n = u32::try_from(seen.len()).unwrap();
            tx.send(WriterRequest::Message(Message::Have(n))).unwrap();
            Ok(())
        }
        async fn on_received_message(&self, _: Message<'_>) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn read_chunk(&self, _: &ChunkInfo, _: &mut [u8]) -> anyhow::Result<()> {
            Ok(())
        }
    }

    // BEP 10: a later extended handshake adds or changes the ids it names,
    // an id of 0 takes one away, and an extension it does not mention keeps
    // its id. A message for an extension the peer no longer names is
    // dropped and the connection kept.
    #[tokio::test(start_paused = true)]
    async fn a_later_extended_handshake_updates_the_ids_and_unaddressed_messages_are_dropped() {
        use crate::peer_protocol::extended::{ExtendedMessage, PeerExtendedMessageIds};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let shook = Shook::default();
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        let connection = PeerConnection::new(
            connection.addr,
            connection.info_hashes,
            connection.peer_id,
            &shook,
            Some(connection.options),
            BlockingSpawner::new(1),
            connection.connector,
        );
        let (ours, mut theirs) = tokio::io::duplex(1 << 18);
        let (reader, writer) = halves(ours);
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: writer.into(),
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::None,
            meter,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        *shook.tx.lock() = Some(tx);

        let handshake = |m: PeerExtendedMessageIds| {
            let hs = ExtendedHandshake {
                m,
                ..Default::default()
            };
            let mut buf = [0u8; 256];
            let len = Message::Extended(ExtendedMessage::Handshake(hs))
                .serialize(&mut buf, &PeerExtendedMessageIds::default)
                .unwrap();
            buf[..len].to_vec()
        };
        let first = handshake(PeerExtendedMessageIds {
            ut_pex: Some(5),
            upload_only: Some(3),
            lt_donthave: Some(0),
            ..Default::default()
        });
        let second = handshake(PeerExtendedMessageIds {
            ut_pex: Some(0),
            ut_holepunch: Some(4),
            ..Default::default()
        });
        let peer = async {
            let mut opening = [0; 68];
            theirs.read_exact(&mut opening).await.unwrap();
            // The extended message ids of the frames after our handshake,
            // each with how many Haves came before it, and the Haves: the
            // second handshake goes once the Have after the first is in.
            let mut extended = Vec::new();
            let mut haves = Vec::new();
            let mut ours_seen = false;
            for (n, hs) in [first, second].into_iter().enumerate() {
                theirs.write_all(&hs).await.unwrap();
                while haves.len() <= n {
                    let mut body = vec![0; theirs.read_u32().await.unwrap() as usize];
                    theirs.read_exact(&mut body).await.unwrap();
                    match body.as_slice() {
                        [20, 0, ..] if !ours_seen => ours_seen = true,
                        [20, id, ..] => extended.push((haves.len(), *id)),
                        [4, have @ ..] => {
                            haves.push(u32::from_be_bytes(have.try_into().unwrap()));
                        }
                        _ => {}
                    }
                }
            }
            shook.tx.lock().take();
            (extended, haves)
        };
        let (result, (extended, haves)) =
            tokio::join!(connection.manage_peer_incoming(rx, incoming), peer);
        assert!(matches!(result, Err(Error::TorrentIsNotLive)), "{result:?}");

        // The PEX message went out under id 5 after the first handshake,
        // and not after the second.
        assert_eq!(extended, [(0, 5)]);
        assert_eq!(haves, [1, 2]);
        let seen = shook.seen.lock().clone();
        assert_eq!(
            seen,
            [
                PeerExtendedMessageIds {
                    ut_pex: Some(5),
                    upload_only: Some(3),
                    ..Default::default()
                },
                PeerExtendedMessageIds {
                    ut_holepunch: Some(4),
                    upload_only: Some(3),
                    ..Default::default()
                },
            ]
        );
    }
}
