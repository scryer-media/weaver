// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use futures::FutureExt;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncWriteExt, DuplexStream},
    sync::{mpsc, oneshot},
    time::Instant,
};

use super::{
    Handler, MAX_METADATA_SIZE, MetadataFetch, REJECTED_WAIT, REQUEST_TIMEOUT, Requests,
    TorrentAndInfoBytes, read_metainfo_from_peer,
};
use crate::{
    buffers::ByteBuf,
    core::{
        constants::CHUNK_SIZE,
        hash_id::{Id20, InfoHashes},
    },
    dht_utils::{ReadMetainfoResult, read_metainfo_from_peer_receiver},
    network::{
        AsyncReadVectoredIntoCompat, BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture,
        DatagramSocket, SocketFactory, StreamListener,
    },
    peer_connection::{PeerConnectionHandler, PeerConnectionOptions, WriterRequest},
    peer_protocol::{
        Handshake, Message,
        extended::{
            ExtendedMessage, PeerExtendedMessageIds,
            handshake::ExtendedHandshake,
            ut_metadata::{UtMetadata, UtMetadataData},
        },
    },
    read_buf::ReadBuf,
    sha1w::{ISha1, Sha1},
    spawn_utils::BlockingSpawner,
    stream_connect::{StreamConnector, StreamConnectorArgs},
};

/// The mock peers wait on us, never on the clock.
const NO_TIMEOUT: Duration = Duration::MAX;

/// Every dial reaches the test as the far end of an in-memory stream, so a
/// conversation runs on the paused clock.
pub(crate) struct Dialed(pub(crate) mpsc::UnboundedSender<(SocketAddr, DuplexStream)>);

impl SocketFactory for Dialed {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let (ours, theirs) = tokio::io::duplex(1 << 20);
            self.0
                .send((addr, theirs))
                .map_err(|_| io::Error::from(io::ErrorKind::ConnectionRefused))?;
            Ok(halves(ours))
        })
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

pub(crate) fn halves(stream: DuplexStream) -> (BoxAsyncReadVectored, BoxAsyncWrite) {
    let (read, write) = tokio::io::split(stream);
    (Box::new(read.into_vectored_compat()), Box::new(write))
}

pub(super) async fn connector() -> (
    Arc<StreamConnector>,
    mpsc::UnboundedReceiver<(SocketAddr, DuplexStream)>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let connector = StreamConnector::new(StreamConnectorArgs {
        enable_tcp: true,
        socket_factory: Some(Arc::new(Dialed(tx))),
        ..Default::default()
    })
    .await
    .unwrap();
    (Arc::new(connector), rx)
}

pub(super) fn addr(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

fn hashes_of(info: &[u8]) -> InfoHashes {
    let mut sha1 = Sha1::new();
    sha1.update(info);
    InfoHashes {
        v1: Some(Id20::new(sha1.finish())),
        v2: None,
    }
}

/// An info dictionary of about `name_len` bytes, and its hashes.
pub(super) fn metadata(name_len: usize) -> (Bytes, InfoHashes) {
    let mut info = format!("d6:lengthi1e4:name{name_len}:").into_bytes();
    info.extend(std::iter::repeat_n(b'a', name_len));
    info.extend_from_slice(b"12:piece lengthi16384e6:pieces20:");
    info.extend_from_slice(&[0; 20]);
    info.push(b'e');
    let hashes = hashes_of(&info);
    (Bytes::from(info), hashes)
}

/// A valid info dict of `pieces` pieces, so its metadata is about
/// `pieces * 20` bytes long.
fn info_bytes(pieces: usize) -> Bytes {
    let mut info = format!(
        "d6:lengthi{}e4:name1:a12:piece lengthi16384e6:pieces{}:",
        pieces * 16384,
        pieces * 20
    )
    .into_bytes();
    info.extend(std::iter::repeat_n(7u8, pieces * 20));
    info.push(b'e');
    Bytes::from(info)
}

/// The far end of a metadata fetch.
pub(super) struct MockPeer {
    read: BoxAsyncReadVectored,
    write: BoxAsyncWrite,
    buf: ReadBuf,
}

impl MockPeer {
    /// Reads our handshake and answers it, speaking the extension protocol.
    pub(super) async fn greet(stream: DuplexStream, hashes: InfoHashes) -> Self {
        let (mut read, mut write) = halves(stream);
        let mut buf = ReadBuf::new();
        buf.read_handshake(&mut read, NO_TIMEOUT).await.unwrap();
        let mut out = [0u8; 68];
        let len = Handshake::new(hashes.v1.unwrap(), Id20::new([9; 20]))
            .serialize_unchecked_len(&mut out);
        write.write_all(&out[..len]).await.unwrap();
        Self { read, write, buf }
    }

    pub(super) async fn send(&mut self, msg: Message<'_>) {
        let mut out = vec![0u8; 1 << 15];
        let len = msg
            .serialize(&mut out, &PeerExtendedMessageIds::my)
            .unwrap();
        self.write.write_all(&out[..len]).await.unwrap();
    }

    pub(super) async fn send_extended_handshake(&mut self, metadata_size: u32) {
        let mut handshake = ExtendedHandshake::new();
        handshake.metadata_size = Some(metadata_size);
        self.send(Message::Extended(ExtendedMessage::Handshake(handshake)))
            .await;
    }

    pub(super) async fn send_piece(&mut self, metadata: &Bytes, piece: u32) {
        let data = UtMetadata::Data(UtMetadataData::piece_of(metadata, piece).unwrap());
        self.send(Message::Extended(ExtendedMessage::UtMetadata(
            data.as_borrowed(),
        )))
        .await;
    }

    pub(super) async fn send_reject(&mut self, piece: u32) {
        self.send(Message::Extended(ExtendedMessage::UtMetadata(
            UtMetadata::Reject(piece),
        )))
        .await;
    }

    /// The next metadata piece we ask for; other messages pass. None once
    /// we hang up.
    pub(super) async fn next_request(&mut self) -> Option<u32> {
        loop {
            let msg = self
                .buf
                .read_message(
                    &mut self.read,
                    NO_TIMEOUT,
                    None,
                    &mut crate::read_buf::Unlimited,
                )
                .await
                .ok()?;
            if let Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Request(piece))) = msg
            {
                return Some(piece);
            }
        }
    }
}

async fn fetch_from(
    addr: SocketAddr,
    hashes: InfoHashes,
    connector: Arc<StreamConnector>,
) -> anyhow::Result<TorrentAndInfoBytes> {
    // A peer that keeps the connection up as long as libtorrent does
    // (`settings_pack::peer_timeout`), so the request limit can pass.
    let options = PeerConnectionOptions {
        read_write_timeout: Some(Duration::from_secs(120)),
        ..Default::default()
    };
    read_metainfo_from_peer(
        addr,
        Id20::new([1; 20]),
        MetadataFetch::new(hashes),
        Some(options),
        BlockingSpawner::new(1),
        connector,
        "test".to_owned(),
    )
    .await
}

#[tokio::test(start_paused = true)]
async fn metadata_data_and_reject_before_the_extended_handshake_are_ignored() {
    // libtorrent `ut_metadata_peer_plugin::on_extended`: unrequested data
    // is dropped, and a reject, wanted or not, holds requests for a minute.
    let (metadata, hashes) = metadata(10);
    let (connector, mut dials) = connector().await;
    let peer = async {
        let (_, stream) = dials.recv().await.unwrap();
        let mut peer = MockPeer::greet(stream, hashes).await;
        // Nothing was asked for yet.
        peer.send_piece(&metadata, 0).await;
        peer.send_reject(0).await;
        let rejected = Instant::now();
        peer.send_extended_handshake(metadata.len() as u32).await;
        let piece = peer.next_request().await.unwrap();
        assert!(Instant::now() >= rejected + REJECTED_WAIT);
        peer.send_piece(&metadata, piece).await;
        peer
    };

    let (fetched, _peer) = tokio::join!(fetch_from(addr(1), hashes, connector), peer);

    assert_eq!(fetched.unwrap().1.as_ref(), metadata.as_ref());
}

#[tokio::test(start_paused = true)]
async fn a_metadata_piece_sent_again_is_ignored() {
    // Two pieces: the first answered twice before the second.
    let (metadata, hashes) = metadata(20_000);
    let (connector, mut dials) = connector().await;
    let peer = async {
        let (_, stream) = dials.recv().await.unwrap();
        let mut peer = MockPeer::greet(stream, hashes).await;
        peer.send_extended_handshake(metadata.len() as u32).await;
        let mut asked = vec![
            peer.next_request().await.unwrap(),
            peer.next_request().await.unwrap(),
        ];
        asked.sort_unstable();
        assert_eq!(asked, [0, 1]);
        peer.send_piece(&metadata, 0).await;
        peer.send_piece(&metadata, 0).await;
        peer.send_piece(&metadata, 1).await;
        peer
    };

    let (fetched, _peer) = tokio::join!(fetch_from(addr(1), hashes, connector), peer);

    assert_eq!(fetched.unwrap().1.as_ref(), metadata.as_ref());
}

/// A metadata peer whose writes we read back. Its requests are taken with
/// [`requested`]; nothing sends them.
fn handler(fetch: &Arc<MetadataFetch>) -> (Handler, mpsc::UnboundedReceiver<WriterRequest>) {
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();
    let handler = Handler {
        addr: addr(1),
        fetch: fetch.clone(),
        writer_tx,
        result_tx: Mutex::new(None),
        requests: Arc::new(Requests::new()),
        client_name_and_version: "test".to_owned(),
    };
    (handler, writer_rx)
}

fn naming(metadata_size: u32) -> ExtendedHandshake<ByteBuf<'static>> {
    ExtendedHandshake {
        metadata_size: Some(metadata_size),
        ..ExtendedHandshake::new()
    }
}

/// The pieces the peer may be asked for right now.
fn requested(handler: &Handler) -> Vec<u32> {
    handler
        .requests
        .next()
        .now_or_never()
        .map(|pieces| pieces.unwrap())
        .unwrap_or_default()
}

fn set_of(handler: &Handler) -> Arc<Mutex<super::PieceSet>> {
    handler.requests.set().unwrap()
}

#[tokio::test]
async fn peers_naming_one_metadata_size_fill_one_buffer() {
    let fetch = MetadataFetch::new(metadata(10).1);
    let (a, _a_writes) = handler(&fetch);
    let (b, _b_writes) = handler(&fetch);

    a.on_extended_handshake(&naming(40_000)).unwrap();
    b.on_extended_handshake(&naming(40_000)).unwrap();

    assert!(Arc::ptr_eq(&set_of(&a), &set_of(&b)));
}

#[tokio::test]
async fn a_repeated_extended_handshake_asks_for_nothing_again() {
    let fetch = MetadataFetch::new(metadata(10).1);
    let (peer, _writes) = handler(&fetch);

    peer.on_extended_handshake(&naming(40_000)).unwrap();
    let set = set_of(&peer);
    assert_eq!(requested(&peer), [0, 1]);

    peer.on_extended_handshake(&naming(40_000)).unwrap();
    peer.on_extended_handshake(&naming(80_000)).unwrap();
    assert_eq!(requested(&peer), [] as [u32; 0]);
    assert!(Arc::ptr_eq(&set, &set_of(&peer)));
    assert_eq!(fetch.sets.lock().len(), 1);
}

#[tokio::test]
async fn metadata_sizes_past_the_cap_or_zero_are_refused() {
    let fetch = MetadataFetch::new(metadata(10).1);
    for size in [0, MAX_METADATA_SIZE + 1, u32::MAX] {
        let (peer, _writes) = handler(&fetch);
        assert!(peer.on_extended_handshake(&naming(size)).is_err(), "{size}");
        assert!(requested(&peer).is_empty());
    }
    assert!(fetch.sets.lock().is_empty());

    let (peer, _writes) = handler(&fetch);
    peer.on_extended_handshake(&naming(MAX_METADATA_SIZE))
        .unwrap();
}

#[tokio::test]
async fn a_metadata_buffer_goes_with_its_last_peer() {
    let fetch = MetadataFetch::new(metadata(10).1);
    let (a, _a_writes) = handler(&fetch);
    let (b, _b_writes) = handler(&fetch);
    a.on_extended_handshake(&naming(40_000)).unwrap();
    b.on_extended_handshake(&naming(40_000)).unwrap();
    let set = Arc::downgrade(&set_of(&a));

    drop(a);
    assert!(set.upgrade().is_some());
    drop(b);
    assert!(set.upgrade().is_none());
}

#[tokio::test(start_paused = true)]
async fn two_peers_send_the_metadata_between_them() {
    // Two pieces: the first peer has only the first, the second only the
    // second.
    let (metadata, hashes) = metadata(20_000);
    let (connector, mut dials) = connector().await;
    let mock = metadata.clone();
    tokio::spawn(async move {
        while let Some((addr, stream)) = dials.recv().await {
            let has = u32::from(addr.port() - 1);
            let metadata = mock.clone();
            tokio::spawn(async move {
                let mut peer = MockPeer::greet(stream, hashes).await;
                peer.send_extended_handshake(metadata.len() as u32).await;
                while let Some(piece) = peer.next_request().await {
                    if piece == has {
                        peer.send_piece(&metadata, piece).await;
                    } else {
                        peer.send_reject(piece).await;
                    }
                }
            });
        }
    });

    let found = read_metainfo_from_peer_receiver(
        Id20::new([1; 20]),
        hashes,
        vec![addr(1), addr(2)],
        futures::stream::pending(),
        None,
        connector,
        "test".to_owned(),
    )
    .await;

    match found {
        ReadMetainfoResult::Found { info_bytes, .. } => {
            assert_eq!(info_bytes.as_ref(), metadata.as_ref())
        }
        ReadMetainfoResult::ChannelClosed { .. } => panic!("no metadata"),
    }
}

type Found = oneshot::Receiver<Result<TorrentAndInfoBytes, crate::bencode::DeserializeError>>;

/// A reader for `info`'s metadata past the extended handshake of a peer
/// that has it, sending requests through `Requests::send`.
fn reader(info: &Bytes) -> (Handler, mpsc::UnboundedReceiver<WriterRequest>, Found) {
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();
    let (result_tx, result_rx) = oneshot::channel();
    let requests = Arc::new(Requests::new());
    let sender = requests.clone();
    let request_tx = writer_tx.clone();
    tokio::spawn(async move { sender.send(&request_tx).await });
    let handler = Handler {
        addr: addr(1),
        fetch: MetadataFetch::new(hashes_of(info)),
        writer_tx,
        result_tx: Mutex::new(Some(result_tx)),
        requests,
        client_name_and_version: "test".to_owned(),
    };
    handler
        .on_extended_handshake(&naming(info.len().try_into().unwrap()))
        .unwrap();
    (handler, writer_rx, result_rx)
}

/// The pieces of the next metadata requests sent, all sent at once, and
/// when.
async fn next_requests(sent: &mut mpsc::UnboundedReceiver<WriterRequest>) -> (Vec<u32>, Instant) {
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

async fn answer(handler: &Handler, info: &Bytes, piece: u32) {
    let data = UtMetadata::Data(UtMetadataData::piece_of(info, piece).unwrap());
    handler
        .on_received_message(Message::Extended(ExtendedMessage::UtMetadata(
            data.as_borrowed(),
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
    let mut corrupt = info.to_vec();
    corrupt[CHUNK_SIZE as usize] ^= 1;
    let corrupt = Bytes::from(corrupt);
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
    let mut corrupt = info.to_vec();
    corrupt[0] ^= 1;
    answer(&handler, &Bytes::from(corrupt), 0).await;
    let failed = Instant::now();
    let (pieces, asked_again) = next_requests(&mut sent).await;
    assert_eq!(pieces, [0]);
    assert!(asked_again >= failed + Duration::from_secs(5 * 60 + 20));
}

#[tokio::test(start_paused = true)]
async fn metadata_that_fails_its_hash_check_penalises_every_peer_that_sent_a_piece() {
    // libtorrent ut_metadata_plugin::received_metadata: each piece's
    // source gets the penalty, and every piece is missing again.
    let info = info_bytes(1000);
    let fetch = MetadataFetch::new(hashes_of(&info));
    let (a, _a_writes) = handler(&fetch);
    let (b, _b_writes) = handler(&fetch);
    let size = info.len().try_into().unwrap();
    a.on_extended_handshake(&naming(size)).unwrap();
    b.on_extended_handshake(&naming(size)).unwrap();
    assert_eq!(requested(&a), [0, 1]);
    assert_eq!(requested(&b), [0, 1]);
    let mut corrupt = info.to_vec();
    corrupt[CHUNK_SIZE as usize] ^= 1;
    let corrupt = Bytes::from(corrupt);
    answer(&a, &corrupt, 0).await;
    answer(&b, &corrupt, 1).await;

    assert_eq!(set_of(&a).lock().missing().collect::<Vec<_>>(), [0, 1]);
    for peer in [&a, &b] {
        // Nothing goes out before the penalty has passed.
        assert!(requested(peer).is_empty());
        assert!(peer.requests.state.lock().limit >= Instant::now() + Duration::from_secs(20));
    }
}

#[tokio::test]
async fn any_peer_is_asked_for_the_least_requested_missing_piece() {
    // libtorrent ut_metadata_plugin::metadata_request: whoever asks gets
    // the least requested missing piece, two at a time per peer
    // (maybe_send_request), whoever was asked for it before.
    let info = info_bytes(2000);
    assert_eq!(info.len().div_ceil(CHUNK_SIZE as usize), 3);
    let fetch = MetadataFetch::new(hashes_of(&info));
    let size = info.len().try_into().unwrap();
    let (a, _a_writes) = handler(&fetch);
    let (b, _b_writes) = handler(&fetch);
    let (c, _c_writes) = handler(&fetch);
    a.on_extended_handshake(&naming(size)).unwrap();
    b.on_extended_handshake(&naming(size)).unwrap();
    assert_eq!(requested(&a), [0, 1]);
    assert_eq!(requested(&b), [2, 0]);
    answer(&a, &info, 0).await;
    answer(&a, &info, 1).await;
    assert_eq!(requested(&a), [2]);
    // Only piece 2 is missing: a peer that joins now is asked for it, and
    // for nothing more while it is the only one.
    c.on_extended_handshake(&naming(size)).unwrap();
    assert_eq!(requested(&c), [2]);
    assert!(requested(&c).is_empty());

    // The metadata fails its hash check: the peer that sent nothing is
    // asked for the pieces missing again at once; the senders wait.
    let mut corrupt = info.to_vec();
    corrupt[2 * CHUNK_SIZE as usize] ^= 1;
    answer(&b, &Bytes::from(corrupt), 2).await;
    assert_eq!(set_of(&c).lock().missing().collect::<Vec<_>>(), [0, 1, 2]);
    assert_eq!(requested(&c), [0]);
    assert!(requested(&a).is_empty());
    assert!(requested(&b).is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_metadata_request_left_unanswered_ends_the_connection() {
    // A peer that answers nothing ends its fetch once a request has waited
    // REQUEST_TIMEOUT, so whatever holds the connection lets it go.
    let (metadata, hashes) = metadata(10);
    let (connector, mut dials) = connector().await;
    let peer = async {
        let (_, stream) = dials.recv().await.unwrap();
        let mut peer = MockPeer::greet(stream, hashes).await;
        peer.send_extended_handshake(metadata.len() as u32).await;
        assert_eq!(peer.next_request().await, Some(0));
        let asked = Instant::now();
        // Keeps reading, so the connection stays up from our side.
        while peer.next_request().await.is_some() {}
        asked
    };

    let (fetched, asked) = tokio::join!(fetch_from(addr(1), hashes, connector), peer);

    let error = fetched.unwrap_err();
    assert!(
        format!("{error:#}").contains("went unanswered"),
        "{error:#}"
    );
    assert!(Instant::now() >= asked + REQUEST_TIMEOUT);
}
