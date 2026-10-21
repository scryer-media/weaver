// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::{BTreeSet, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use crate::bencode::from_bytes;
use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::core::{
    constants::CHUNK_SIZE,
    hash_id::{Id20, InfoHashes},
    lengths::{ChunkInfo, last_element_size},
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
use parking_lot::{Mutex, RwLock};
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
    info_hashes: InfoHashes,
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
    let handler = Handler {
        addr,
        info_hashes,
        writer_tx,
        result_tx: Mutex::new(Some(result_tx)),
        locked: RwLock::new(None),
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

/// How long a peer that rejected a metadata request is left alone
/// (libtorrent `ut_metadata_peer_plugin::on_extended`, `dont_have`).
const REJECTED_WAIT: Duration = Duration::from_secs(60);

/// The metadata requests owed to one peer, and when they may go out
/// (libtorrent `ut_metadata_peer_plugin::m_request_limit` and
/// `maybe_send_request`). A peer that rejects a request, or whose metadata
/// fails its hash check, is asked again on the same connection once its
/// limit has passed.
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

    fn answered(&self, piece: u32) {
        self.state.lock().sent.remove(&piece);
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

    /// Sends the wanted pieces' requests each time the limit lets them go.
    /// Ends only when the connection's writer is gone.
    async fn send(&self, writer_tx: &UnboundedSender<WriterRequest>) -> anyhow::Error {
        loop {
            self.changed.notified().await;
            let pieces = loop {
                let limit = {
                    let mut state = self.state.lock();
                    if Instant::now() >= state.limit {
                        let pieces = std::mem::take(&mut state.wanted);
                        state.sent.extend(pieces.iter().copied());
                        break pieces;
                    }
                    state.limit
                };
                tokio::time::sleep_until(limit).await;
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

/// What a received piece of metadata made of it.
enum Recorded {
    Incomplete,
    Complete,
    /// Every piece is in, and they do not hash to the torrent's info hash.
    /// The pieces are dropped, to be asked for again.
    FailedHashCheck,
}

#[derive(Default)]
struct HandlerLocked {
    metadata_size: u32,
    total_pieces: usize,
    buffer: Vec<u8>,
    received_pieces: Vec<bool>,
}

impl HandlerLocked {
    fn new(metadata_size: u32) -> anyhow::Result<Self> {
        if metadata_size > 32 * 1024 * 1024 {
            anyhow::bail!("metadata size {} is too big", metadata_size);
        }
        let buffer = vec![0u8; metadata_size as usize];
        let total_pieces: usize = (metadata_size as u64)
            .div_ceil(CHUNK_SIZE as u64)
            .try_into()?;
        let received_pieces = vec![false; total_pieces];
        Ok(Self {
            metadata_size,
            received_pieces,
            buffer,
            total_pieces,
        })
    }
    fn piece_size(&self, index: u32) -> usize {
        if index as usize == self.total_pieces - 1 {
            last_element_size(self.metadata_size as u64, CHUNK_SIZE as u64)
                .try_into()
                .unwrap()
        } else {
            CHUNK_SIZE as usize
        }
    }
    fn record_piece(
        &mut self,
        d: &UtMetadataData<ByteBuf>,
        info_hashes: &InfoHashes,
    ) -> anyhow::Result<Recorded> {
        let piece = d.piece();
        if piece as usize >= self.total_pieces {
            anyhow::bail!("wrong index");
        }
        let offset = (piece * CHUNK_SIZE) as usize;
        let size = self.piece_size(piece);
        if d.len() != size {
            anyhow::bail!(
                "expected length of piece {} to be {}, but got {}",
                piece,
                size,
                d.len()
            );
        }
        if self.received_pieces[piece as usize] {
            anyhow::bail!("already received piece {}", piece);
        }
        d.copy_to_slice(&mut self.buffer[offset..offset + d.len()]);
        self.received_pieces[piece as usize] = true;

        if !self.received_pieces.iter().all(|p| *p) {
            Ok(Recorded::Incomplete)
        } else if info_hashes.matches_info(&self.buffer) {
            Ok(Recorded::Complete)
        } else {
            self.received_pieces.fill(false);
            Ok(Recorded::FailedHashCheck)
        }
    }
}

pub type TorrentAndInfoBytes = (TorrentMetaV1Info<ByteBufOwned>, ByteBufOwned);

struct Handler {
    addr: SocketAddr,
    info_hashes: InfoHashes,
    writer_tx: UnboundedSender<WriterRequest>,
    result_tx: Mutex<
        Option<
            tokio::sync::oneshot::Sender<
                Result<TorrentAndInfoBytes, crate::bencode::DeserializeError>,
            >,
        >,
    >,
    locked: RwLock<Option<HandlerLocked>>,
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
            Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Reject(piece))) => {
                self.requests.rejected(piece);
                return Ok(());
            }
            _ => return Ok(()),
        };
        self.requests.answered(utdata.piece());
        let (recorded, total_pieces) = {
            let mut locked = self.locked.write();
            let locked = locked.as_mut().unwrap();
            let recorded = locked.record_piece(&utdata, &self.info_hashes)?;
            (recorded, locked.total_pieces)
        };
        match recorded {
            Recorded::Incomplete => {}
            Recorded::FailedHashCheck => {
                trace!("{}: the metadata failed its hash check", self.addr);
                self.requests.failed_hash_check(total_pieces.try_into()?);
            }
            Recorded::Complete => {
                let buf = Bytes::from(self.locked.write().take().unwrap().buffer);
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
                    .map_err(|_| {
                        anyhow::anyhow!("torrent info deserialized, but consumer closed")
                    })?;
            }
        }
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
        let metadata_size = match extended_handshake.metadata_size {
            Some(metadata_size) => metadata_size,
            None => anyhow::bail!("peer does not have metadata_size"),
        };

        if extended_handshake.m.ut_metadata.is_none() {
            anyhow::bail!("peer does not support ut_metadata");
        }

        self.writer_tx
            .send(WriterRequest::Message(Message::Unchoke))?;
        self.writer_tx
            .send(WriterRequest::Message(Message::Interested))?;

        let inner = HandlerLocked::new(metadata_size)?;
        let total_pieces: u32 = inner.total_pieces.try_into()?;

        self.locked.write().replace(inner);
        self.requests.want(0..total_pieces);
        Ok(())
    }

    fn client_name_and_version(&self) -> &str {
        &self.client_name_and_version
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use parking_lot::{Mutex, RwLock};
    use tokio::{
        sync::{mpsc::UnboundedReceiver, oneshot},
        time::Instant,
    };

    use super::{Handler, REJECTED_WAIT, Requests, TorrentAndInfoBytes};
    use crate::buffers::ByteBuf;
    use crate::core::{
        constants::CHUNK_SIZE,
        hash_id::{Id20, InfoHashes},
    };
    use crate::peer_connection::{PeerConnectionHandler, WriterRequest};
    use crate::peer_protocol::{
        Message,
        extended::{
            ExtendedMessage, PeerExtendedMessageIds,
            handshake::ExtendedHandshake,
            ut_metadata::{UtMetadata, UtMetadataData},
        },
    };

    /// A valid info dict of `pieces` pieces, so its metadata is about
    /// `pieces * 20` bytes long.
    fn info_bytes(pieces: usize) -> Vec<u8> {
        let mut info = format!(
            "d6:lengthi{}e4:name1:a12:piece lengthi16384e6:pieces{}:",
            pieces * 16384,
            pieces * 20
        )
        .into_bytes();
        info.extend(std::iter::repeat_n(7u8, pieces * 20));
        info.push(b'e');
        info
    }

    type Found = oneshot::Receiver<Result<TorrentAndInfoBytes, crate::bencode::DeserializeError>>;

    /// A reader for `info`'s metadata past the extended handshake of a peer
    /// that has it, sending requests through `Requests::send`.
    fn reader(info: &[u8]) -> (Handler, UnboundedReceiver<WriterRequest>, Found) {
        use crate::sha1w::{ISha1, Sha1};
        let mut sha1 = Sha1::new();
        sha1.update(info);
        let (writer_tx, writer_rx) = tokio::sync::mpsc::unbounded_channel();
        let (result_tx, result_rx) = oneshot::channel();
        let requests = Arc::new(Requests::new());
        let sender = requests.clone();
        let request_tx = writer_tx.clone();
        tokio::spawn(async move { sender.send(&request_tx).await });
        let handler = Handler {
            addr: SocketAddr::from(([127, 0, 0, 1], 6881)),
            info_hashes: InfoHashes {
                v1: Some(Id20::new(sha1.finish())),
                v2: None,
            },
            writer_tx,
            result_tx: Mutex::new(Some(result_tx)),
            locked: RwLock::new(None),
            requests,
            client_name_and_version: "Test 1.0".into(),
        };
        handler
            .on_extended_handshake(&ExtendedHandshake {
                m: PeerExtendedMessageIds::my(),
                metadata_size: Some(info.len().try_into().unwrap()),
                ..Default::default()
            })
            .unwrap();
        (handler, writer_rx, result_rx)
    }

    /// The pieces of the next metadata requests sent, all sent at once, and
    /// when.
    async fn next_requests(sent: &mut UnboundedReceiver<WriterRequest>) -> (Vec<u32>, Instant) {
        let mut pieces = Vec::new();
        loop {
            match sent.recv().await.unwrap() {
                WriterRequest::Message(Message::Extended(ExtendedMessage::UtMetadata(
                    UtMetadata::Request(piece),
                ))) => pieces.push(piece),
                WriterRequest::Message(Message::Unchoke | Message::Interested) => continue,
                other => panic!("sent {other:?}"),
            }
            while let Ok(request) = sent.try_recv() {
                match request {
                    WriterRequest::Message(Message::Extended(ExtendedMessage::UtMetadata(
                        UtMetadata::Request(piece),
                    ))) => pieces.push(piece),
                    other => panic!("sent {other:?}"),
                }
            }
            return (pieces, Instant::now());
        }
    }

    async fn answer(handler: &Handler, info: &[u8], piece: u32) {
        let start = (piece * CHUNK_SIZE) as usize;
        let end = info.len().min(start + CHUNK_SIZE as usize);
        let data = UtMetadataData::from_bytes(
            piece,
            info.len().try_into().unwrap(),
            ByteBuf(&info[start..end]),
        );
        handler
            .on_received_message(Message::Extended(ExtendedMessage::UtMetadata(
                UtMetadata::Data(data),
            )))
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_that_rejects_a_metadata_request_is_asked_again_a_minute_later() {
        // libtorrent ut_metadata_peer_plugin::on_extended, dont_have:
        // m_request_limit = now + 1 minute, on the same connection.
        let info = info_bytes(1);
        let (handler, mut sent, result) = reader(&info);
        let (pieces, asked) = next_requests(&mut sent).await;
        assert_eq!(pieces, [0]);

        handler
            .on_received_message(Message::Extended(ExtendedMessage::UtMetadata(
                UtMetadata::Reject(0),
            )))
            .await
            .unwrap();
        let (pieces, asked_again) = next_requests(&mut sent).await;
        assert_eq!(pieces, [0]);
        assert!(asked_again >= asked + REJECTED_WAIT);

        answer(&handler, &info, 0).await;
        let (_, info_bytes) = result.await.unwrap().unwrap();
        assert_eq!(info_bytes.as_ref(), &info[..]);
    }

    #[tokio::test(start_paused = true)]
    async fn metadata_that_fails_its_hash_check_is_asked_for_again_after_20_seconds_or_more() {
        // libtorrent ut_metadata_plugin::received_metadata: a peer whose
        // metadata failed is not asked for 20 to 70 seconds.
        let info = info_bytes(1000);
        assert!(info.len() > CHUNK_SIZE as usize);
        let (handler, mut sent, result) = reader(&info);
        let (pieces, _) = next_requests(&mut sent).await;
        assert_eq!(pieces, [0, 1]);

        let mut corrupt = info.clone();
        corrupt[CHUNK_SIZE as usize] ^= 1;
        answer(&handler, &corrupt, 0).await;
        answer(&handler, &corrupt, 1).await;
        let failed = Instant::now();
        let (pieces, asked_again) = next_requests(&mut sent).await;
        assert_eq!(pieces, [0, 1]);
        assert!(asked_again >= failed + Duration::from_secs(20));

        answer(&handler, &info, 0).await;
        answer(&handler, &info, 1).await;
        let (_, info_bytes) = result.await.unwrap().unwrap();
        assert_eq!(info_bytes.as_ref(), &info[..]);
    }

    #[tokio::test(start_paused = true)]
    async fn metadata_of_one_piece_that_fails_its_hash_check_waits_five_minutes_more() {
        // libtorrent ut_metadata_plugin::received_metadata: with one piece,
        // failed_hash_check(now + 5 minutes), so other peers come first.
        let info = info_bytes(1);
        let (handler, mut sent, _result) = reader(&info);
        next_requests(&mut sent).await;

        let mut corrupt = info.clone();
        corrupt[0] ^= 1;
        answer(&handler, &corrupt, 0).await;
        let failed = Instant::now();
        let (pieces, asked_again) = next_requests(&mut sent).await;
        assert_eq!(pieces, [0]);
        assert!(asked_again >= failed + Duration::from_secs(5 * 60 + 20));
    }
}
