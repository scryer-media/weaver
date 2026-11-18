// SPDX-License-Identifier: GPL-3.0-or-later

//! A live torrent and one peer driven by hand: the test plays the remote
//! peer, calling the handler as the connection would, and reads what the
//! handler queues for the wire.

mod admission;
mod cancels;
mod discovery;
mod disk_jobs;
mod lifecycle;
mod mapped_endpoint_tests;
mod mapped_filter_tests;
mod peer_messages;
mod pieces;
mod requests;
mod room;
mod slots;
mod swarm_tests;
mod upload;
mod v2;

use super::blocks::land_block;
use super::peer::MAX_ALLOWED_IN_REQUEST_QUEUE;
use super::peer::PeerRx;
use crate::limits::LimitKind;
use crate::peer_connection::PeerConnectionHandler;
use crate::peer_connection::WriterRequest;
use crate::peer_protocol::MY_EXTENDED_UT_HOLEPUNCH;
use crate::peer_protocol::Message;
use crate::peer_protocol::Piece;
use crate::peer_protocol::Request;
use crate::peer_protocol::extended::ExtendedMessage;
use crate::peer_protocol::extended::handshake::ExtendedHandshake;
use crate::peer_protocol::extended::ut_holepunch;
use crate::peer_protocol::extended::ut_holepunch::UtHolepunch;
use crate::peer_protocol::extended::ut_metadata::UtMetadata;
use crate::peer_protocol::extended::ut_pex::MAX_PEX_MESSAGE_LEN;
use crate::peer_protocol::extended::ut_pex::PexPeerInfo;
use crate::peer_protocol::extended::ut_pex::UtPex;
use crate::piece_priority::PiecePriority;
use crate::session::CheckedIncomingConnection;
use crate::session_stats::network::HolepunchEvent;
use crate::torrent_state::paused::TorrentStatePaused;
use futures::FutureExt;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::{
    collections::BTreeSet,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU32, AtomicUsize},
};
use tokio::sync::mpsc::unbounded_channel;

use tempfile::TempDir;

use super::*;
use crate::{
    AddTorrent, AddTorrentOptions, Session, SessionOptions,
    core::constants::CHUNK_SIZE,
    peer_protocol::extended,
    storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage},
    tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
    type_aliases::BF,
};

const CHUNKS_PER_PIECE: u32 = 4;
const PIECE_LENGTH: u32 = CHUNK_SIZE * CHUNKS_PER_PIECE;
const PIECES: u32 = 2;

struct Harness {
    live: Arc<TorrentStateLive>,
    content: Vec<u8>,
    _session: Arc<Session>,
    _torrent: Option<TestTorrent>,
    _out: TempDir,
}

impl Harness {
    /// A live torrent of two four-chunk pieces, none of them had, and no peers.
    async fn new(name: &str) -> Self {
        Self::build(name, 0x00, choker::DEFAULT_UNCHOKE_SLOTS_LIMIT).await
    }

    /// A live torrent of two four-chunk pieces, both had, and no peers.
    async fn seeding(name: &str) -> Self {
        Self::seeding_with_slots(name, choker::DEFAULT_UNCHOKE_SLOTS_LIMIT).await
    }

    /// As [`Harness::seeding`], uploading to `slots` peers at once.
    async fn seeding_with_slots(name: &str, slots: usize) -> Self {
        Self::build(name, 0xc0, slots).await
    }

    // `have` is the bitfield of the pieces had, `slots` the upload slots.
    async fn build(name: &str, have: u8, slots: usize) -> Self {
        let opts = SessionOptions {
            unchoke_slots_limit: slots,
            ..isolated_session_opts(false)
        };
        Self::build_on(
            name,
            have,
            opts,
            MemoryStorageFactory::default().clone_box(),
        )
        .await
    }

    /// As [`Harness::build`], in a session of `opts`, on the storage
    /// `storage` makes.
    async fn build_on(
        name: &str,
        have: u8,
        opts: SessionOptions,
        storage: BoxStorageFactory,
    ) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(storage),
            ..Default::default()
        };
        Self::build_added(name, PIECES, have, opts, add).await
    }

    /// A live torrent of `pieces` four-chunk pieces, `have` the bitfield
    /// of those had, added with `add`.
    async fn build_added(
        name: &str,
        pieces: u32,
        have: u8,
        opts: SessionOptions,
        add: AddTorrentOptions,
    ) -> Self {
        let out = TempDir::with_prefix(name).unwrap();
        let session = Session::new_with_opts(out.path().into(), opts)
            .await
            .unwrap();
        Self::added_to(session, out, name, pieces, &[have], add).await
    }

    /// Another live torrent of the harness's session, of the harness's
    /// pieces, `have` the bitfield of those had.
    async fn sibling(&self, name: &str, have: u8) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            ..Default::default()
        };
        let out = TempDir::with_prefix(name).unwrap();
        Self::added_to(self._session.clone(), out, name, PIECES, &[have], add).await
    }

    /// A live torrent of `bytes` eight times four-chunk pieces, all had, and
    /// no peers.
    async fn seeding_bytes(name: &str, bytes: usize) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            ..Default::default()
        };
        let out = TempDir::with_prefix(name).unwrap();
        let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
            .await
            .unwrap();
        let pieces = 8 * u32::try_from(bytes).unwrap();
        Self::added_to(session, out, name, pieces, &vec![0xff; bytes], add).await
    }

    /// A live torrent of `pieces` four-chunk pieces, `have` the bitfield of
    /// those had, added to `session` with `add`.
    async fn added_to(
        session: Arc<Session>,
        out: TempDir,
        name: &str,
        pieces: u32,
        have: &[u8],
        add: AddTorrentOptions,
    ) -> Self {
        let torrent = TestTorrent::new(name, (pieces * PIECE_LENGTH) as usize, PIECE_LENGTH)
            .await
            .unwrap();
        let have = BF::from_boxed_slice(have.into());
        Self::adding(session, out, torrent, crate::Checkpoint::pieces(have), add).await
    }

    /// `torrent`, live, started from `checkpoint`, added to `session` with
    /// `add`.
    async fn adding(
        session: Arc<Session>,
        out: TempDir,
        torrent: TestTorrent,
        checkpoint: crate::Checkpoint,
        add: AddTorrentOptions,
    ) -> Self {
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent.torrent.clone()),
                Some(AddTorrentOptions {
                    checkpoint: Some(checkpoint),
                    ..add
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        Self {
            live: handle.live().unwrap(),
            content: torrent.content.clone(),
            _session: session,
            _torrent: Some(torrent),
            _out: out,
        }
    }

    /// The torrent's handle in its session.
    fn handle(&self) -> crate::torrent_state::ManagedTorrentHandle {
        self._session
            .get(crate::api::TorrentIdOrHash::Id(self.live.shared.id))
            .unwrap()
    }

    /// The torrent taken out of its session, its files left, and added
    /// again with `add`, started from `checkpoint`, as a restart does.
    async fn restarted(self, checkpoint: crate::Checkpoint, add: AddTorrentOptions) -> Self {
        let Self {
            live,
            _session: session,
            _torrent: torrent,
            _out: out,
            ..
        } = self;
        let id = live.shared.id;
        drop(live);
        session
            .delete(crate::api::TorrentIdOrHash::Id(id), false)
            .await
            .unwrap();
        Self::adding(session, out, torrent.unwrap(), checkpoint, add).await
    }

    /// A live peer at `addr` with every piece, and the queue of what we send it.
    fn peer(&self, addr: &str) -> (PeerHandler, PeerRx) {
        self.peer_speaking(addr, false)
    }

    /// A live peer at `addr` with every piece, that speaks v2 when
    /// `speaks_v2` holds, and the queue of what we send it.
    fn peer_speaking(&self, addr: &str, speaks_v2: bool) -> (PeerHandler, PeerRx) {
        let (handler, rx) = self.new_peer(addr);
        handler.speaks_v2.store(speaks_v2, Ordering::Relaxed);
        handler
            .first_message_received
            .store(true, Ordering::Relaxed);
        let mut every_piece = peer::empty_bitfield(self.lengths.total_pieces());
        every_piece[..self.lengths.total_pieces() as usize].fill(true);
        handler.on_bitfield(every_piece.as_raw_slice()).unwrap();
        (handler, rx)
    }

    /// A peer at `addr` that just went live and has sent nothing yet, and
    /// the queue of what we send it.
    fn new_peer(&self, addr: &str) -> (PeerHandler, PeerRx) {
        self.new_peer_over(addr, ConnectionKind::Tcp)
    }

    /// As [`Self::new_peer`], its connection over `kind`.
    fn new_peer_over(&self, addr: &str, kind: ConnectionKind) -> (PeerHandler, PeerRx) {
        let addr: SocketAddr = addr.parse().unwrap();
        let (tx, rx) = unbounded_channel();
        let counters = self
            .live
            .peers
            .add_incoming(addr, client_id(addr), tx.clone(), kind, 6881)
            .unwrap();
        // The connection is live: its send buffer is bound, as the
        // handshake binds it.
        let upload = self
            .live
            .peers
            .with_live(addr.into(), peer::LivePeerState::upload_ledger)
            .unwrap();
        let handler = PeerHandler {
            addr: addr.into(),
            incoming: true,
            on_bitfield_notify: Default::default(),
            speaks_v2: AtomicBool::new(false),
            sent_dht_port: AtomicBool::new(false),
            let_go_in_upload_mode: AtomicU64::new(0),
            pex_started: AtomicBool::new(false),
            flow_control: Mutex::new(PeerFlowControl::new(self.live.max_request_window())),
            state: self.live.clone(),
            tx,
            answers: Default::default(),
            counters,
            first_message_received: AtomicBool::new(false),
            transport: AtomicU8::new(0),
            disk: Default::default(),
            upload: upload.into(),
            requests: Default::default(),
            cancel_token: self.live.cancellation_token.child_token(),
            client_name_and_version: String::new(),
        };
        (handler, rx)
    }

    fn piece(&self, index: u32) -> ValidPieceIndex {
        self.lengths.validate_piece_index(index).unwrap()
    }

    /// Chunk `chunk` of piece `index`.
    fn chunk(&self, index: u32, chunk: u32) -> ChunkInfo {
        self.lengths
            .chunk_info_from_received_data(self.piece(index), chunk * CHUNK_SIZE, CHUNK_SIZE)
            .unwrap()
    }

    fn missing_chunks(&self, index: u32) -> Vec<u32> {
        self.live
            .lock_read("test")
            .get_pieces()
            .unwrap()
            .missing_chunks(self.piece(index))
            .map(|c| c.chunk_index)
            .collect()
    }

    /// The pieces no peer owns and any peer may take.
    fn queued_pieces(&self) -> Vec<u32> {
        let g = self.live.lock_read("test");
        let pieces = g.get_pieces().unwrap();
        let queued: Vec<u32> = pieces
            .chunks()
            .iter_queued_pieces()
            .map(|p| p.get())
            .collect();
        for p in &queued {
            assert!(pieces.get_inflight(self.piece(*p)).is_none());
        }
        queued
    }

    fn inflight_count(&self, peer: &PeerHandler) -> usize {
        self.live
            .peers
            .with_live(peer.addr, |live| live.requested_inflight_count())
            .unwrap()
    }

    /// The peer delivers the chunk `request` asked for.
    async fn deliver(&self, peer: &PeerHandler, request: (u32, u32)) {
        let (index, begin) = request;
        let start = (index * PIECE_LENGTH + begin) as usize;
        let block = &self.content[start..start + CHUNK_SIZE as usize];
        peer.on_received_piece(Piece::from_data(index, begin, block))
            .await
            .unwrap();
    }
}

impl std::ops::Deref for Harness {
    type Target = TorrentStateLive;

    fn deref(&self) -> &TorrentStateLive {
        &self.live
    }
}

type Task<'a> = Pin<Box<dyn Future<Output = crate::Result<()>> + 'a>>;

/// The next message `task` queues for the peer, polling it until it does. An
/// Interested is skipped: the requester sends one before it requests.
async fn next_message(task: &mut Task<'_>, rx: &mut PeerRx) -> Message<'static> {
    loop {
        let sent = tokio::select! {
            biased;
            sent = rx.recv() => sent.expect("the peer's writer queue closed"),
            res = task.as_mut() => panic!("the peer task ended: {res:?}"),
        };
        match sent {
            WriterRequest::Message(Message::Interested) => {}
            WriterRequest::Message(msg) => return msg,
            other => panic!("unexpected writer request {other:?}"),
        }
    }
}

/// The next `n` requests the requester sends, as (piece, offset).
async fn next_requests(task: &mut Task<'_>, rx: &mut PeerRx, n: usize) -> BTreeSet<(u32, u32)> {
    let mut requests = BTreeSet::new();
    while requests.len() < n {
        match next_message(task, rx).await {
            Message::Request(r) => assert!(requests.insert((r.index, r.begin))),
            other => panic!("expected a request, got {other:?}"),
        }
    }
    requests
}

fn chunks(index: u32, chunks: impl IntoIterator<Item = u32>) -> BTreeSet<(u32, u32)> {
    chunks
        .into_iter()
        .map(|c| (index, c * CHUNK_SIZE))
        .collect()
}

fn every_chunk() -> BTreeSet<(u32, u32)> {
    (0..PIECES)
        .flat_map(|p| chunks(p, 0..CHUNKS_PER_PIECE))
        .collect()
}

impl Harness {
    /// The unchoked `peer` is asked for every chunk, on the paused clock:
    /// the first piece's four at the depth of a peer no block has come from
    /// yet; then the peer is snubbed, which ends its slow start, and the
    /// first of them arrives at once, which lifts the snub at a pace that
    /// makes room for the rest. Returns the piece asked for first.
    async fn request_every_chunk(
        &self,
        peer: &PeerHandler,
        task: &mut Task<'_>,
        rx: &mut PeerRx,
    ) -> u32 {
        let first = next_requests(task, rx, CHUNKS_PER_PIECE as usize).await;
        let (piece, _) = *first.first().unwrap();
        assert_eq!(first, chunks(piece, 0..CHUNKS_PER_PIECE));
        self.peers
            .with_live_mut(peer.addr, "test", |live| live.snub())
            .unwrap();
        self.deliver(peer, (piece, 0)).await;
        let other = 1 - piece;
        assert_eq!(
            next_requests(task, rx, CHUNKS_PER_PIECE as usize).await,
            chunks(other, 0..CHUNKS_PER_PIECE)
        );
        piece
    }
}

impl Harness {
    /// The peer set the fast bit in its handshake.
    fn speak_fast(&self, peer: &PeerHandler) {
        self.peers
            .with_live_mut(peer.addr, "test", |live| live.fast = true)
            .unwrap();
    }
}

/// What the upload side queues for a peer's writer.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Sent {
    Choke,
    Unchoke,
    /// The block at (piece, offset), handed to the writer to send.
    Block(u32, u32),
    /// A Reject for the block at (piece, offset).
    Reject(u32, u32),
    /// An Allowed Fast for the piece.
    AllowedFast(u32),
}

fn sent(request: WriterRequest) -> Sent {
    match request {
        WriterRequest::Message(Message::Choke) => Sent::Choke,
        WriterRequest::Message(Message::Unchoke) => Sent::Unchoke,
        WriterRequest::Message(Message::Reject(r))
        | WriterRequest::Answer(crate::peer_connection::Answer::Reject(r)) => {
            Sent::Reject(r.index, r.begin)
        }
        WriterRequest::Message(Message::AllowedFast(piece)) => Sent::AllowedFast(piece),
        WriterRequest::ReadChunkRequest(c) => Sent::Block(c.piece_index.get(), c.offset),
        other => panic!("unexpected writer request {other:?}"),
    }
}

/// The next thing queued for the peer's writer, waiting until there is one.
async fn next_sent(rx: &mut PeerRx) -> Sent {
    sent(rx.recv().await.expect("the peer's writer queue closed"))
}

/// The request for chunk `chunk` of piece `index`.
fn request(index: u32, chunk: u32) -> Request {
    Request {
        index,
        begin: chunk * CHUNK_SIZE,
        length: CHUNK_SIZE,
    }
}

impl Harness {
    /// The peer takes every piece, in whatever order they are handed out.
    fn take_every(&self, peer: &PeerHandler) {
        peer.on_i_am_unchoked();
        for _ in 0..self.lengths.total_pieces() {
            peer.acquire_next_piece()
                .unwrap()
                .piece()
                .expect("a piece is not free");
        }
    }

    /// The peer takes piece `index`, acquiring pieces until it has it.
    fn take(&self, peer: &PeerHandler, index: u32) {
        peer.on_i_am_unchoked();
        while peer
            .acquire_next_piece()
            .unwrap()
            .piece()
            .expect("the piece is not free")
            .get()
            != index
        {}
    }

    /// The peer is asked for, and sends, chunk `chunk` of piece `index`,
    /// with its bytes flipped when `corrupt` holds.
    async fn send(
        &self,
        peer: &PeerHandler,
        (index, chunk): (u32, u32),
        corrupt: bool,
    ) -> anyhow::Result<()> {
        let info = self.chunk(index, chunk);
        self.peers
            .with_live_mut(peer.addr, "test", |live| live.add_inflight_request(info))
            .unwrap();
        let start = (index * PIECE_LENGTH + chunk * CHUNK_SIZE) as usize;
        let mut block = self.content[start..start + CHUNK_SIZE as usize].to_vec();
        if corrupt {
            block[0] ^= 0xff;
        }
        peer.on_received_piece(Piece::from_data(index, chunk * CHUNK_SIZE, &block[..]))
            .await
    }

    // A swarm peer is banned by address; a web seed on its torrent's seed
    // list.
    fn banned(&self, peer: &PeerHandler) -> bool {
        match peer.addr.ip() {
            Some(ip) => self.peers.is_banned(ip),
            None => self.shared.web_seeds.is_banned(peer.addr),
        }
    }

    /// Whether a peer connecting from `addr` is turned away.
    fn refused(&self, addr: &str) -> bool {
        let (tx, _rx) = unbounded_channel();
        self.peers
            .add_incoming(
                addr.parse().unwrap(),
                Id20::default(),
                tx,
                ConnectionKind::Tcp,
                6881,
            )
            .is_err()
    }

    /// The trust points, hash failures and parole of the peer.
    fn trust(&self, peer: &PeerHandler) -> (i8, u8, bool) {
        let trust = self.peers.with_peer(peer.addr, |p| p.trust()).unwrap();
        (trust.points(), trust.hashfails(), trust.on_parole())
    }

    /// `bad` sends the first half of piece 0, its second block corrupt, and
    /// chokes us; `good` sends the rest, and the piece fails.
    async fn fail_shared(&self, bad: &PeerHandler, good: &PeerHandler) {
        self.take(bad, 0);
        self.send(bad, (0, 0), false).await.unwrap();
        self.send(bad, (0, 1), true).await.unwrap();
        bad.on_i_am_choked().unwrap();
        self.take(good, 0);
        self.send(good, (0, 2), false).await.unwrap();
        self.send(good, (0, 3), false).await.unwrap();
        assert_eq!(self.missing_chunks(0), vec![0, 1, 2, 3]);
    }
}

/// Whether the peer's writer was told to close the connection.
fn told_to_disconnect(rx: &mut PeerRx) -> bool {
    std::iter::from_fn(|| rx.try_recv().ok())
        .any(|r| matches!(r, WriterRequest::Disconnect(Err(_))))
}

impl Harness {
    fn have(&self, index: u32) -> bool {
        self.live
            .lock_read("test")
            .get_chunks()
            .unwrap()
            .get_have_pieces()
            .as_slice()[index as usize]
    }
}

impl Harness {
    /// Whether the peer at `addr` is in the torrent's peer list.
    fn knows(&self, addr: &str) -> bool {
        self.peers
            .with_peer(addr.parse::<SocketAddr>().unwrap().into(), |_| ())
            .is_some()
    }

    /// The extension ids we advertise to `peer`.
    fn advertised(&self, peer: &PeerHandler) -> extended::PeerExtendedMessageIds {
        let mut hs = ExtendedHandshake::new();
        peer.update_my_extended_handshake(&mut hs).unwrap();
        hs.m
    }
}

/// A handshake from a peer, with the DHT bit when `dht` holds.
fn handshake(dht: bool) -> Handshake {
    Handshake::new(Id20::default(), Id20::new([9; 20])).offering_dht(dht)
}

pub(crate) type Datagram = (Vec<u8>, SocketAddr);

/// A datagram socket the test holds the other end of: what the session
/// sends through it comes out of the test's receiver, with the address it
/// went to, and what the test sends is what the session receives.
struct HeldDatagrams {
    addr: SocketAddr,
    sent: tokio::sync::mpsc::UnboundedSender<Datagram>,
    arriving: parking_lot::Mutex<tokio::sync::mpsc::UnboundedReceiver<Datagram>>,
}

impl HeldDatagrams {
    fn send(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        self.sent.send((buf.to_vec(), target)).ok();
        Ok(buf.len())
    }
}

impl crate::network::DatagramSocket for HeldDatagrams {
    fn bind_addr(&self) -> SocketAddr {
        self.addr
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> crate::network::BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
        Box::pin(std::future::poll_fn(move |cx| {
            match std::task::ready!(self.arriving.lock().poll_recv(cx)) {
                Some((datagram, from)) => {
                    buf[..datagram.len()].copy_from_slice(&datagram);
                    std::task::Poll::Ready(Ok((datagram.len(), from)))
                }
                // The test is done with it: nothing more arrives.
                None => std::task::Poll::Pending,
            }
        }))
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<usize>> {
        Box::pin(std::future::ready(self.send(buf, target)))
    }

    fn poll_send_to(
        &self,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(self.send(buf, target))
    }
}

/// The test's ends of a [`HeldDatagrams`].
pub(crate) struct HeldEnds {
    addr: SocketAddr,
    pub(crate) sent: tokio::sync::mpsc::UnboundedReceiver<Datagram>,
    pub(crate) arriving: tokio::sync::mpsc::UnboundedSender<Datagram>,
}

/// A socket factory whose every datagram socket is a [`HeldDatagrams`], at
/// an address of its own on 127.0.0.1, and which makes no connection.
#[derive(Default)]
pub(crate) struct HeldUdp {
    held: parking_lot::Mutex<Vec<HeldEnds>>,
}

impl HeldUdp {
    /// The test's ends of the socket bound at `addr`.
    pub(crate) fn take(&self, addr: SocketAddr) -> HeldEnds {
        let mut held = self.held.lock();
        let at = held.iter().position(|h| h.addr == addr).unwrap();
        held.swap_remove(at)
    }
}

impl crate::network::SocketFactory for HeldUdp {
    fn connect_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        Box::pin(async { Err(std::io::ErrorKind::Unsupported.into()) })
    }

    fn listen_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        Box::pin(async { Err(std::io::ErrorKind::Unsupported.into()) })
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>>
    {
        let (sent, sent_rx) = tokio::sync::mpsc::unbounded_channel();
        let (arriving_tx, arriving) = tokio::sync::mpsc::unbounded_channel();
        let mut held = self.held.lock();
        let addr = SocketAddr::from((
            std::net::Ipv4Addr::LOCALHOST,
            50000 + u16::try_from(held.len()).unwrap(),
        ));
        held.push(HeldEnds {
            addr,
            sent: sent_rx,
            arriving: arriving_tx,
        });
        let socket: Arc<dyn crate::network::DatagramSocket> = Arc::new(HeldDatagrams {
            addr,
            sent,
            arriving: parking_lot::Mutex::new(arriving),
        });
        Box::pin(std::future::ready(Ok(socket)))
    }
}

/// The value under `key` in the bencoded dict `v`, as bytes.
pub(crate) fn dict_bytes<'a>(
    v: &'a crate::bencode::BencodeValue<crate::bencode::ByteBufOwned>,
    key: &str,
) -> &'a [u8] {
    use crate::bencode::{BencodeValue, ByteBufOwned};
    match v {
        BencodeValue::Dict(d) => match &d[&ByteBufOwned::from(key.as_bytes())] {
            BencodeValue::Bytes(b) => b.as_ref(),
            other => panic!("{key} is not bytes: {other:?}"),
        },
        _ => panic!("not a dict: {v:?}"),
    }
}

impl Harness {
    /// A torrent of `pieces` pieces (at most 8), with `have` the bitfield
    /// of those had, added to super-seed.
    async fn super_seeding(name: &str, pieces: u32, have: u8) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            super_seeding: true,
            ..Default::default()
        };
        Self::build_added(name, pieces, have, isolated_session_opts(false), add).await
    }

    /// A peer at `addr` whose first message is a bitfield of `pieces`, or
    /// a keep-alive when it has none.
    async fn peer_with(&self, addr: &str, pieces: &[u32]) -> (PeerHandler, PeerRx) {
        let (peer, rx) = self.new_peer(addr);
        let mut bits = peer::empty_bitfield(self.lengths.total_pieces());
        for &p in pieces {
            bits.set(p as usize, true);
        }
        let first = if pieces.is_empty() {
            Message::KeepAlive
        } else {
            Message::Bitfield(ByteBuf(bits.as_raw_slice()))
        };
        (&peer).on_received_message(first).await.unwrap();
        (peer, rx)
    }
}

fn sock(addr: &str) -> SocketAddr {
    addr.parse().unwrap()
}

/// The peer id of the client at `addr`: each address is another client.
fn client_id(addr: SocketAddr) -> Id20 {
    let mut peer_id = [7; 20];
    for (at, byte) in peer_id.iter_mut().zip(addr.to_string().bytes()) {
        *at ^= byte;
    }
    Id20::new(peer_id)
}

impl Harness {
    /// The place the connection at `addr`, whose writer queue is `tx`, holds
    /// under the torrent's cap and the session's, as its task would.
    fn hold(&self, addr: PeerHandle, tx: &PeerTx) -> Held {
        let permit = self.peer_semaphore.clone().try_acquire_owned().unwrap();
        let slot = self._session.connection_slots.try_open().unwrap();
        self.live.hold_place(addr, tx, permit, slot)
    }

    /// A connection from `addr` the listener took and checked, for this
    /// torrent, with its place under the session's cap; and our end of it.
    /// Each address is another client, with a peer id of its own.
    fn incoming(
        &self,
        addr: &str,
    ) -> (
        CheckedIncomingConnection,
        crate::connection_slots::ConnectionSlot,
        tokio::io::DuplexStream,
    ) {
        self.incoming_as(addr, client_id(sock(addr)))
    }

    /// [`Harness::incoming`], from the client whose peer id is `peer_id`.
    fn incoming_as(
        &self,
        addr: &str,
        peer_id: Id20,
    ) -> (
        CheckedIncomingConnection,
        crate::connection_slots::ConnectionSlot,
        tokio::io::DuplexStream,
    ) {
        let (ours, theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = crate::peer_info_reader::tests::halves(ours);
        let checked = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: sock(addr),
            reader,
            writer: writer.into(),
            read_buf: crate::read_buf::ReadBuf::new(),
            handshake: Handshake::new(self.shared.info_hash, peer_id),
            encryption: crate::mse::Encryption::None,
            meter: Default::default(),
        };
        let slot = self._session.connection_slots.try_accept().unwrap();
        (checked, slot, theirs)
    }

    fn holds_place(&self, addr: &str) -> bool {
        self.places
            .lock()
            .contains_key(&PeerHandle::from(sock(addr)))
    }
}

/// The next thing `task` queues for the peer, polling it until it does.
async fn next_queued(task: &mut Task<'_>, rx: &mut PeerRx) -> WriterRequest {
    tokio::select! {
        biased;
        sent = rx.recv() => sent.expect("the peer's writer queue closed"),
        res = task.as_mut() => panic!("the peer task ended: {res:?}"),
    }
}

/// Whether `task` is waiting, having queued nothing more for the peer.
fn waiting(task: &mut Task<'_>, rx: &mut PeerRx) -> bool {
    futures::FutureExt::now_or_never(task.as_mut()).is_none() && rx.is_empty()
}

/// A bitfield of `pieces` alone.
fn bitfield_of(pieces: &[u32]) -> Vec<u8> {
    let mut bf = peer::empty_bitfield(PIECES);
    for p in pieces {
        bf.set(*p as usize, true);
    }
    bf.as_raw_slice().to_vec()
}
