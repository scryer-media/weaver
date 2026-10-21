// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
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
};

pub(crate) async fn read_metainfo_from_peer(
    addr: SocketAddr,
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
        fetch,
        writer_tx,
        result_tx: Mutex::new(Some(result_tx)),
        set: Mutex::new(None),
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

/// The pieces of metadata of one size, each with the peer that sent it
/// (libtorrent `ut_metadata_plugin::m_requested_metadata`, whose entries
/// name their `source`).
struct PieceSet {
    buffer: Vec<u8>,
    /// For each piece, the requests of the peer that sent it; None while
    /// the piece is missing.
    pieces: Vec<Option<Weak<Requests>>>,
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
            pieces: vec![None; total_pieces],
        }
    }

    fn total_pieces(&self) -> u32 {
        self.pieces.len() as u32
    }

    fn missing(&self) -> impl Iterator<Item = u32> + '_ {
        (0u32..)
            .zip(&self.pieces)
            .filter(|(_, sent_by)| sent_by.is_none())
            .map(|(piece, _)| piece)
    }

    /// Records piece `d` as sent by `source`, and gives the whole metadata
    /// once its last piece is in and it hashes to the torrent. A piece
    /// another peer sent first is not written again. Metadata that does not
    /// hash right leaves every piece missing again and names the peers that
    /// sent them (libtorrent `ut_metadata_plugin::received_metadata` clears
    /// every piece's request count and penalises each piece's source).
    fn record(
        &mut self,
        d: &UtMetadataData<ByteBuf>,
        source: &Arc<Requests>,
        info_hashes: &InfoHashes,
    ) -> anyhow::Result<Recorded> {
        let piece = d.piece();
        match self.pieces.get(piece as usize) {
            None => anyhow::bail!("metadata piece {piece} is past the end"),
            Some(Some(_)) => return Ok(Recorded::Incomplete),
            Some(None) => {}
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
        self.pieces[piece as usize] = Some(Arc::downgrade(source));

        if self.missing().next().is_some() {
            return Ok(Recorded::Incomplete);
        }
        if info_hashes.matches_info(&self.buffer) {
            return Ok(Recorded::Complete(std::mem::take(&mut self.buffer)));
        }
        let mut senders: Vec<Arc<Requests>> = Vec::new();
        for sent_by in &mut self.pieces {
            if let Some(sender) = sent_by.take().and_then(|w| w.upgrade())
                && !senders.iter().any(|s| Arc::ptr_eq(s, &sender))
            {
                senders.push(sender);
            }
        }
        Ok(Recorded::FailedHashCheck(senders))
    }
}

/// The metadata requests owed to one peer, and when they may go out
/// (libtorrent `ut_metadata_peer_plugin::m_request_limit`,
/// `m_sent_requests` and `maybe_send_request`). A peer that rejects a
/// request, or whose metadata fails its hash check, is asked again on the
/// same connection once its limit has passed.
struct Requests {
    state: Mutex<RequestsState>,
    changed: Notify,
}

struct RequestsState {
    /// No request goes out before this.
    limit: Instant,
    /// Pieces to request once the limit has passed.
    wanted: BTreeSet<u32>,
    /// Pieces requested and not yet answered.
    sent: HashSet<u32>,
}

impl Requests {
    fn new() -> Self {
        Self {
            state: Mutex::new(RequestsState {
                limit: Instant::now(),
                wanted: BTreeSet::new(),
                sent: HashSet::new(),
            }),
            changed: Notify::new(),
        }
    }

    fn want(&self, pieces: impl IntoIterator<Item = u32>) {
        self.state.lock().wanted.extend(pieces);
        self.changed.notify_one();
    }

    /// Whether a request for `piece` was outstanding, which the peer's Data
    /// for it now answers (libtorrent `ut_metadata_peer_plugin::on_extended`:
    /// a piece not in `m_sent_requests` is unwanted, and ignored).
    fn answered(&self, piece: u32) -> bool {
        self.state.lock().sent.remove(&piece)
    }

    /// libtorrent `on_extended` with `dont_have`: no request for a minute,
    /// then the rejected piece is asked for again.
    fn rejected(&self, piece: u32) {
        let mut state = self.state.lock();
        state.limit = state.limit.max(Instant::now() + REJECTED_WAIT);
        if state.sent.remove(&piece) {
            state.wanted.insert(piece);
        }
        drop(state);
        self.changed.notify_one();
    }

    /// libtorrent `ut_metadata_plugin::received_metadata` and
    /// `failed_hash_check`: every piece is asked for again after 20 to 70
    /// seconds, and five minutes later when the metadata is one piece, so
    /// that other peers are tried first.
    fn failed_hash_check(&self, total_pieces: u32) {
        let from = if total_pieces == 1 {
            Instant::now() + Duration::from_secs(5 * 60)
        } else {
            Instant::now()
        };
        let wait = 20 + rand::RngExt::random_range(&mut rand::rng(), 0..=50);
        let mut state = self.state.lock();
        state.limit = from + Duration::from_secs(wait);
        state.sent.clear();
        state.wanted = (0..total_pieces).collect();
        drop(state);
        self.changed.notify_one();
    }

    /// The wanted pieces, once the limit lets them go; they are outstanding
    /// from here.
    async fn next(&self) -> BTreeSet<u32> {
        self.changed.notified().await;
        loop {
            let limit = {
                let mut state = self.state.lock();
                if Instant::now() >= state.limit {
                    let pieces = std::mem::take(&mut state.wanted);
                    state.sent.extend(pieces.iter().copied());
                    return pieces;
                }
                state.limit
            };
            tokio::time::sleep_until(limit).await;
        }
    }

    /// Sends the wanted pieces' requests each time the limit lets them go.
    /// Ends only when the connection's writer is gone.
    async fn send(&self, writer_tx: &UnboundedSender<WriterRequest>) -> anyhow::Error {
        loop {
            for piece in self.next().await {
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
    fetch: Arc<MetadataFetch>,
    writer_tx: UnboundedSender<WriterRequest>,
    result_tx: Mutex<
        Option<
            tokio::sync::oneshot::Sender<
                Result<TorrentAndInfoBytes, crate::bencode::DeserializeError>,
            >,
        >,
    >,
    /// The buffer the peer fills, None until its extended handshake.
    set: Mutex<Option<Arc<Mutex<PieceSet>>>>,
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
        let set = self.set.lock().clone();
        let Some(set) = set else {
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
        let mut set = self.set.lock();
        if set.is_some() {
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
        // libtorrent `maybe_send_request`: the pieces still missing, once
        // the peer's limit lets them go.
        self.requests.want(joined.lock().missing());
        *set = Some(joined);
        Ok(())
    }

    fn client_name_and_version(&self) -> &str {
        &self.client_name_and_version
    }
}

#[cfg(test)]
mod tests;
