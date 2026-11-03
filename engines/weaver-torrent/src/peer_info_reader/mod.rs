// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Weak},
    time::Duration,
};

use crate::bencode::from_bytes;
use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::core::{
    constants::CHUNK_SIZE,
    hash_id::{Id20, InfoHashes},
    lengths::ChunkInfo,
    torrent_metainfo::TorrentMetaV1Info,
};
use crate::peer_protocol::{
    Handshake, Message,
    extended::{
        ExtendedMessage,
        handshake::ExtendedHandshake,
        ut_metadata::{UtMetadata, UtMetadataData},
    },
};
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc::UnboundedSender};
use tokio::time::Instant;
use tracing::trace;

use crate::{
    peer_connection::{
        PeerConnection, PeerConnectionHandler, PeerConnectionOptions, WriterRequest,
    },
    spawn_utils::BlockingSpawner,
    stream_connect::{ConnectionKind, StreamConnector},
    torrent_state::live::peers::PeerStates,
};

/// Fetches metadata from the peer at `addr`, one of `peers`, which says
/// which handshake the connection tries and hears which one completed, as
/// the torrent's own connections do (libtorrent: metadata comes over the
/// torrent's ordinary connections, so `torrent_peer::pe_support` applies).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_metainfo_from_peer(
    addr: SocketAddr,
    peers: Arc<PeerStates>,
    peer_id: Id20,
    fetch: Arc<MetadataFetch>,
    peer_connection_options: Option<PeerConnectionOptions>,
    spawner: BlockingSpawner,
    connector: Arc<StreamConnector>,
    client_name_and_version: String,
) -> anyhow::Result<TorrentAndInfoBytes> {
    let (result_tx, result_rx) = tokio::sync::oneshot::channel::<
        Result<(TorrentMetaV1Info<ByteBufOwned>, ByteBufOwned), crate::bencode::DeserializeError>,
    >();
    let (writer_tx, writer_rx) = tokio::sync::mpsc::unbounded_channel::<WriterRequest>();
    let requests = Arc::new(Requests::new());
    let request_tx = writer_tx.clone();
    let info_hashes = fetch.info_hashes;
    let handler = Handler {
        addr,
        peers,
        fetch,
        writer_tx,
        result_tx: Mutex::new(Some(result_tx)),
        requests: requests.clone(),
        client_name_and_version,
    };
    let connection = PeerConnection::new(
        addr,
        info_hashes,
        peer_id,
        handler,
        peer_connection_options,
        spawner,
        connector,
    );

    let result_reader = result_rx;
    let connection_runner = async move { connection.manage_peer_outgoing(writer_rx).await };

    tokio::select! {
        result = result_reader => Ok(result??),
        whatever = connection_runner => match whatever {
            Ok(_) => anyhow::bail!("connection runner completed first"),
            Err(e) => Err(e.into())
        },
        e = requests.send(&request_tx) => Err(e),
    }
}

/// The largest metadata a fetch takes in (libtorrent
/// `settings_pack::max_metadata_size`, checked in
/// `ut_metadata_plugin::received_metadata`).
pub(crate) const MAX_METADATA_SIZE: u32 = 30 * 1024 * 1024;

/// How long a peer that rejected a metadata request is left alone
/// (libtorrent `ut_metadata_peer_plugin::on_extended`, `dont_have`).
const REJECTED_WAIT: Duration = Duration::from_secs(60);

/// The most metadata requests outstanding to one peer (libtorrent
/// `ut_metadata_peer_plugin::maybe_send_request`: `m_sent_requests.size()
/// < 2`).
const MAX_OUTSTANDING: usize = 2;

/// How long a metadata request may go unanswered before the peer's
/// connection ends (libtorrent `settings_pack::request_timeout`, the
/// longest `peer_connection::request_timeout`).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// One torrent's metadata as its peers send it in. The buffer belongs to
/// the fetch, not to a peer (libtorrent `ut_metadata_plugin::m_metadata`):
/// every peer that names the same size fills the one buffer of that size,
/// which lives while a peer filling it does.
pub(crate) struct MetadataFetch {
    info_hashes: InfoHashes,
    sets: Mutex<HashMap<u32, Weak<Mutex<PieceSet>>>>,
}

impl MetadataFetch {
    pub(crate) fn new(info_hashes: InfoHashes) -> Arc<Self> {
        Arc::new(Self {
            info_hashes,
            sets: Default::default(),
        })
    }

    /// The buffer of the metadata size a peer names, made when no peer
    /// filling one of that size is left.
    fn join(&self, metadata_size: u32) -> anyhow::Result<Arc<Mutex<PieceSet>>> {
        anyhow::ensure!(
            metadata_size > 0 && metadata_size <= MAX_METADATA_SIZE,
            "metadata size {metadata_size} is out of range"
        );
        let mut sets = self.sets.lock();
        sets.retain(|_, set| set.strong_count() > 0);
        if let Some(set) = sets.get(&metadata_size).and_then(Weak::upgrade) {
            return Ok(set);
        }
        let set = Arc::new(Mutex::new(PieceSet::new(metadata_size)));
        sets.insert(metadata_size, Arc::downgrade(&set));
        Ok(set)
    }
}

/// The pieces of metadata of one size: how often each was asked for, and
/// the peer that sent it (libtorrent `ut_metadata_plugin::
/// m_requested_metadata`).
struct PieceSet {
    buffer: Vec<u8>,
    pieces: Vec<MetadataPiece>,
    /// The peers filling the set, woken when every piece is missing again.
    peers: Vec<Weak<Requests>>,
}

/// libtorrent `ut_metadata_plugin::metadata_piece`.
#[derive(Clone, Default)]
struct MetadataPiece {
    /// How many requests went out for the piece since it was last missing.
    requests: u32,
    /// The requests of the peer that sent it; None while the piece is
    /// missing.
    source: Option<Weak<Requests>>,
}

/// What a received piece of metadata made of its set.
enum Recorded {
    Incomplete,
    /// Every piece is in, and they hash to the torrent.
    Complete(Vec<u8>),
    /// Every piece is in, and they do not hash to the torrent. Every piece
    /// is missing again; these are the peers that sent them.
    FailedHashCheck(Vec<Arc<Requests>>),
}

impl PieceSet {
    fn new(metadata_size: u32) -> Self {
        let total_pieces = metadata_size.div_ceil(CHUNK_SIZE) as usize;
        Self {
            buffer: vec![0u8; metadata_size as usize],
            pieces: vec![MetadataPiece::default(); total_pieces],
            peers: Vec::new(),
        }
    }

    fn total_pieces(&self) -> u32 {
        u32::try_from(self.pieces.len()).expect("piece indexes fit in u32")
    }

    fn missing(&self) -> impl Iterator<Item = u32> + '_ {
        (0u32..)
            .zip(&self.pieces)
            .filter(|(_, p)| p.source.is_none())
            .map(|(piece, _)| piece)
    }

    /// `peer` fills the set from now on.
    fn join(&mut self, peer: &Arc<Requests>) {
        self.peers.retain(|p| p.strong_count() > 0);
        self.peers.push(Arc::downgrade(peer));
    }

    /// The missing piece asked for least, the first of those, which a peer
    /// with requests `outstanding` is asked for next (libtorrent
    /// `ut_metadata_plugin::metadata_request`: any peer takes the least
    /// requested piece, whoever was asked for it before).
    fn pick(&mut self, outstanding: &HashMap<u32, Instant>) -> Option<u32> {
        let piece = (0u32..)
            .zip(&self.pieces)
            .filter(|(piece, p)| p.source.is_none() && !outstanding.contains_key(piece))
            .min_by_key(|(_, p)| p.requests)
            .map(|(piece, _)| piece)?;
        self.pieces[piece as usize].requests += 1;
        Some(piece)
    }

    /// Records piece `d` as sent by `source`, and gives the whole metadata
    /// once its last piece is in and it hashes to the torrent. A piece
    /// another peer sent first is not written again. Metadata that does not
    /// hash right leaves every piece missing and unasked again, wakes every
    /// peer filling the set and names the peers that sent the pieces
    /// (libtorrent `ut_metadata_plugin::received_metadata` clears every
    /// piece's request count and penalises each piece's source).
    fn record(
        &mut self,
        d: &UtMetadataData<ByteBuf>,
        source: &Arc<Requests>,
        info_hashes: &InfoHashes,
    ) -> anyhow::Result<Recorded> {
        let piece = d.piece();
        match self.pieces.get(piece as usize) {
            None => anyhow::bail!("metadata piece {piece} is past the end"),
            Some(p) if p.source.is_some() => return Ok(Recorded::Incomplete),
            Some(_) => {}
        }
        let start = piece as usize * CHUNK_SIZE as usize;
        let range = start..self.buffer.len().min(start + CHUNK_SIZE as usize);
        anyhow::ensure!(
            d.len() == range.len(),
            "expected length of piece {piece} to be {}, but got {}",
            range.len(),
            d.len()
        );
        d.copy_to_slice(&mut self.buffer[range]);
        self.pieces[piece as usize].source = Some(Arc::downgrade(source));

        if self.missing().next().is_some() {
            return Ok(Recorded::Incomplete);
        }
        if info_hashes.matches_info(&self.buffer) {
            return Ok(Recorded::Complete(std::mem::take(&mut self.buffer)));
        }
        let mut senders: Vec<Arc<Requests>> = Vec::new();
        for p in &mut self.pieces {
            p.requests = 0;
            if let Some(sender) = p.source.take().and_then(|w| w.upgrade())
                && !senders.iter().any(|s| Arc::ptr_eq(s, &sender))
            {
                senders.push(sender);
            }
        }
        for peer in self.peers.iter().filter_map(Weak::upgrade) {
            peer.changed.notify_one();
        }
        Ok(Recorded::FailedHashCheck(senders))
    }
}

/// The metadata requests of one peer, and when they may go out
/// (libtorrent `ut_metadata_peer_plugin::m_request_limit`,
/// `m_sent_requests` and `maybe_send_request`). Once its limit has passed
/// the peer is asked for the set's least requested pieces, at most
/// MAX_OUTSTANDING at a time. A peer that rejects a request, or whose
/// metadata fails its hash check, is asked again on the same connection
/// once its limit has passed; one that leaves a request unanswered for
/// REQUEST_TIMEOUT loses its connection.
struct Requests {
    state: Mutex<RequestsState>,
    changed: Notify,
}

struct RequestsState {
    /// No request goes out before this.
    limit: Instant,
    /// Pieces requested and not yet answered, and when they were.
    sent: HashMap<u32, Instant>,
    /// The metadata the peer fills, from its extended handshake on.
    set: Option<Arc<Mutex<PieceSet>>>,
}

impl Requests {
    fn new() -> Self {
        Self {
            state: Mutex::new(RequestsState {
                limit: Instant::now(),
                sent: HashMap::new(),
                set: None,
            }),
            changed: Notify::new(),
        }
    }

    fn set(&self) -> Option<Arc<Mutex<PieceSet>>> {
        self.state.lock().set.clone()
    }

    /// The peer fills `set` from now on (libtorrent `on_extension_handshake`
    /// calls `maybe_send_request`).
    fn join(self: &Arc<Self>, set: Arc<Mutex<PieceSet>>) {
        set.lock().join(self);
        self.state.lock().set = Some(set);
        self.changed.notify_one();
    }

    /// Whether a request for `piece` was outstanding, which the peer's Data
    /// for it now answers (libtorrent `ut_metadata_peer_plugin::on_extended`:
    /// a piece not in `m_sent_requests` is unwanted, and ignored; an
    /// answered one makes room for the next request).
    fn answered(&self, piece: u32) -> bool {
        let answered = self.state.lock().sent.remove(&piece).is_some();
        self.changed.notify_one();
        answered
    }

    /// libtorrent `on_extended` with `dont_have`: no request for a minute.
    fn rejected(&self, piece: u32) {
        let mut state = self.state.lock();
        state.limit = state.limit.max(Instant::now() + REJECTED_WAIT);
        state.sent.remove(&piece);
        drop(state);
        self.changed.notify_one();
    }

    /// libtorrent `ut_metadata_plugin::received_metadata` and
    /// `failed_hash_check`: no request for 20 to 70 seconds, and five
    /// minutes more when the metadata is one piece, so that other peers
    /// are tried first.
    fn failed_hash_check(&self, total_pieces: u32) {
        let from = if total_pieces == 1 {
            Instant::now() + Duration::from_secs(5 * 60)
        } else {
            Instant::now()
        };
        let wait = 20 + rand::RngExt::random_range(&mut rand::rng(), 0..=50);
        self.state.lock().limit = from + Duration::from_secs(wait);
        self.changed.notify_one();
    }

    /// The pieces to ask for now, outstanding from here, once there are
    /// any. An error once a request has gone unanswered for
    /// REQUEST_TIMEOUT.
    async fn next(&self) -> anyhow::Result<Vec<u32>> {
        loop {
            let wake = {
                let mut state = self.state.lock();
                let now = Instant::now();
                let oldest = state.sent.values().min().copied();
                if let Some(oldest) = oldest
                    && now >= oldest + REQUEST_TIMEOUT
                {
                    anyhow::bail!("a metadata request went unanswered for {REQUEST_TIMEOUT:?}");
                }
                let mut pieces = Vec::new();
                if let Some(set) = state.set.clone()
                    && now >= state.limit
                {
                    let mut set = set.lock();
                    while state.sent.len() < MAX_OUTSTANDING
                        && let Some(piece) = set.pick(&state.sent)
                    {
                        state.sent.insert(piece, now);
                        pieces.push(piece);
                    }
                }
                if !pieces.is_empty() {
                    return Ok(pieces);
                }
                let limit = (state.set.is_some() && state.limit > now).then_some(state.limit);
                oldest
                    .map(|oldest| oldest + REQUEST_TIMEOUT)
                    .into_iter()
                    .chain(limit)
                    .min()
            };
            match wake {
                Some(at) => {
                    tokio::select! {
                        () = self.changed.notified() => {}
                        () = tokio::time::sleep_until(at) => {}
                    }
                }
                None => self.changed.notified().await,
            }
        }
    }

    /// Sends the peer's requests as they may go out. Ends when a request
    /// times out or the connection's writer is gone.
    async fn send(&self, writer_tx: &UnboundedSender<WriterRequest>) -> anyhow::Error {
        loop {
            let pieces = match self.next().await {
                Ok(pieces) => pieces,
                Err(e) => return e,
            };
            for piece in pieces {
                let request = WriterRequest::Message(Message::Extended(
                    ExtendedMessage::UtMetadata(UtMetadata::Request(piece)),
                ));
                if writer_tx.send(request).is_err() {
                    return anyhow::anyhow!("the connection's writer is gone");
                }
            }
        }
    }
}

pub type TorrentAndInfoBytes = (TorrentMetaV1Info<ByteBufOwned>, ByteBufOwned);

struct Handler {
    addr: SocketAddr,
    /// The list the peer is on.
    peers: Arc<PeerStates>,
    fetch: Arc<MetadataFetch>,
    writer_tx: UnboundedSender<WriterRequest>,
    result_tx: Mutex<
        Option<
            tokio::sync::oneshot::Sender<
                Result<TorrentAndInfoBytes, crate::bencode::DeserializeError>,
            >,
        >,
    >,
    /// The peer's requests, and the buffer it fills from its extended
    /// handshake on.
    requests: Arc<Requests>,
    client_name_and_version: String,
}

impl PeerConnectionHandler for Handler {
    fn should_send_bitfield(&self) -> bool {
        false
    }

    fn serialize_bitfield_message_to_buf(&self, _buf: &mut [u8]) -> anyhow::Result<usize> {
        Ok(0)
    }

    fn encrypt_outgoing(&self) -> bool {
        self.peers.encrypt_outgoing(self.addr)
    }

    fn on_handshake_encryption(&self, encryption: crate::mse::Encryption) {
        self.peers.handshake_encryption(self.addr, encryption);
    }

    fn on_handshake(
        &self,
        handshake: Handshake,
        _kind: ConnectionKind,
        _speaks_v2: bool,
    ) -> anyhow::Result<()> {
        if !handshake.supports_extended() {
            anyhow::bail!(
                "this peer does not support extended handshaking, which is a prerequisite to download metadata"
            )
        }
        Ok(())
    }

    async fn on_received_message(&self, msg: Message<'_>) -> anyhow::Result<()> {
        trace!("{}: received message: {:?}", self.addr, msg);
        // There are no file trees to exchange hashes over before the
        // metadata is in (libtorrent `bt_peer_connection::on_hash_request`:
        // `!t->valid_metadata()`).
        anyhow::ensure!(!msg.is_v2_only(), "a hash message before the metadata");

        let utdata = match msg {
            Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Data(utdata))) => utdata,
            // A Reject answers its request: the piece is not coming yet.
            Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Reject(piece))) => {
                self.requests.rejected(piece);
                return Ok(());
            }
            _ => return Ok(()),
        };
        // Data no request is outstanding for is unwanted, and ignored;
        // nothing is outstanding before the extended handshake.
        if !self.requests.answered(utdata.piece()) {
            trace!(piece = utdata.piece(), "unwanted metadata piece");
            return Ok(());
        }
        let Some(set) = self.requests.set() else {
            return Ok(());
        };
        let recorded = set
            .lock()
            .record(&utdata, &self.requests, &self.fetch.info_hashes)?;
        let buf = match recorded {
            Recorded::Incomplete => return Ok(()),
            Recorded::FailedHashCheck(senders) => {
                trace!("{}: the metadata failed its hash check", self.addr);
                let total_pieces = set.lock().total_pieces();
                for sender in senders {
                    sender.failed_hash_check(total_pieces);
                }
                return Ok(());
            }
            Recorded::Complete(buf) => buf,
        };
        let buf = Bytes::from(buf);
        let info = from_bytes::<TorrentMetaV1Info<ByteBuf>>(&buf)
            .map(|i| {
                use crate::clone_to_owned::CloneToOwned;
                i.clone_to_owned(Some(&buf))
            })
            .map_err(|e| {
                trace!("error deserializing TorrentMetaV1Info: {e:#}");
                e.into_kind()
            })
            .map(|i| (i, ByteBufOwned(buf)));

        self.result_tx
            .lock()
            .take()
            .ok_or_else(|| anyhow::anyhow!("oneshot is consumed"))?
            .send(info)
            .map_err(|_| anyhow::anyhow!("torrent info deserialized, but consumer closed"))?;
        Ok(())
    }

    fn on_uploaded_bytes(&self, _bytes: u32) {}

    fn read_chunk(&self, _chunk: &ChunkInfo, _buf: &mut [u8]) -> anyhow::Result<()> {
        anyhow::bail!("the peer is not supposed to be requesting chunks")
    }

    fn on_extended_handshake(
        &self,
        extended_handshake: &ExtendedHandshake<ByteBuf>,
    ) -> anyhow::Result<()> {
        // A peer fills one buffer, from its first extended handshake on; a
        // later one changes nothing (libtorrent `ut_metadata_plugin::
        // metadata_size` sizes the torrent's one buffer, and a size it
        // already has leaves it as it is).
        if self.requests.set().is_some() {
            trace!("repeated extended handshake");
            return Ok(());
        }

        let metadata_size = match extended_handshake.metadata_size {
            Some(metadata_size) => metadata_size,
            None => anyhow::bail!("peer does not have metadata_size"),
        };

        if extended_handshake.m.ut_metadata.is_none() {
            anyhow::bail!("peer does not support ut_metadata");
        }

        let joined = self.fetch.join(metadata_size)?;
        self.writer_tx
            .send(WriterRequest::Message(Message::Unchoke))?;
        self.writer_tx
            .send(WriterRequest::Message(Message::Interested))?;
        // libtorrent `maybe_send_request`: the least requested pieces, once
        // the peer's limit lets them go.
        self.requests.join(joined);
        Ok(())
    }

    fn client_name_and_version(&self) -> &str {
        &self.client_name_and_version
    }
}

#[cfg(test)]
pub(crate) mod tests;
