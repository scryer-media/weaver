// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a test torrent's session rides and writes to: the path's sockets,
// watched for how each peer answered, and a store that keeps only the piece
// the test fetches, in memory.

use std::collections::HashMap;
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use anyhow::bail;
use librqbit::network::{
    AsyncReadVectored, BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket,
};
use librqbit::storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage};
use librqbit::{ManagedTorrentShared, SocketFactory, StreamListener, TorrentMetadata};
use librqbit_core::lengths::ValidPieceIndex;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::watch;

use crate::torrents::net::{LegKind, LegSockets};

// A peer that sent this much answered the handshake.
const HANDSHAKE_LEN: u64 = 68;
// The most peer errors kept for the report.
const ERRORS_KEPT: usize = 8;

// What the session's sockets saw, read by the test as it decides.
pub(super) struct Watched {
    wake: watch::Sender<u64>,
    dials: AtomicUsize,
    // Dials that ended: refused, closed before answering, or answered.
    settled: AtomicUsize,
    answered: AtomicUsize,
    inbound: AtomicUsize,
    // Piece bytes sent to peers, and when the last of them went.
    sent: AtomicU64,
    last_sent: Mutex<Option<tokio::time::Instant>>,
    errors: Mutex<Vec<String>>,
    // The connections that carried piece bytes.
    takers: AtomicUsize,
}

impl Watched {
    pub(super) fn new() -> Self {
        Self {
            wake: watch::channel(0).0,
            dials: AtomicUsize::new(0),
            settled: AtomicUsize::new(0),
            answered: AtomicUsize::new(0),
            inbound: AtomicUsize::new(0),
            sent: AtomicU64::new(0),
            last_sent: Mutex::new(None),
            errors: Mutex::new(Vec::new()),
            takers: AtomicUsize::new(0),
        }
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.wake.subscribe()
    }

    pub(super) fn wake(&self) {
        self.wake.send_modify(|revision| *revision += 1);
    }

    pub(super) fn dials(&self) -> usize {
        self.dials.load(Ordering::Acquire)
    }

    pub(super) fn settled(&self) -> usize {
        self.settled.load(Ordering::Acquire)
    }

    pub(super) fn answered(&self) -> usize {
        self.answered.load(Ordering::Acquire)
    }

    pub(super) fn inbound(&self) -> usize {
        self.inbound.load(Ordering::Acquire)
    }

    pub(super) fn sent(&self) -> u64 {
        self.sent.load(Ordering::Acquire)
    }

    pub(super) fn takers(&self) -> usize {
        self.takers.load(Ordering::Acquire)
    }

    pub(super) fn errors(&self) -> Vec<String> {
        self.errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(super) fn last_sent(&self) -> Option<tokio::time::Instant> {
        *self
            .last_sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn error(&self, error: String) {
        let mut errors = self.errors.lock().unwrap_or_else(PoisonError::into_inner);
        if errors.len() < ERRORS_KEPT && !errors.contains(&error) {
            errors.push(error);
        }
    }
}

// The sockets of a path, every connection watched.
pub(super) struct ProbeSockets {
    inner: LegSockets,
    kind: LegKind,
    watched: Arc<Watched>,
}

impl ProbeSockets {
    pub(super) fn new(
        stage: Arc<dyn weaver_tunnel::pipe::Dialer>,
        kind: LegKind,
        watched: Arc<Watched>,
    ) -> Self {
        Self {
            inner: LegSockets::new(stage, &kind),
            kind,
            watched,
        }
    }
}

impl SocketFactory for ProbeSockets {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            self.watched.dials.fetch_add(1, Ordering::AcqRel);
            match self.inner.connect_tcp(addr).await {
                Ok((read, write)) => Ok(Arc::new(Connection {
                    peer: addr,
                    outgoing: true,
                    read: AtomicU64::new(0),
                    frames: Mutex::default(),
                    watched: self.watched.clone(),
                })
                .wrap(read, write)),
                Err(error) => {
                    self.watched.error(format!("{addr}: {error}"));
                    self.watched.settled.fetch_add(1, Ordering::AcqRel);
                    self.watched.wake();
                    Err(error)
                }
            }
        })
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async move {
            let ip = match &self.kind {
                LegKind::Ladder => {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "this path cannot listen",
                    ));
                }
                LegKind::SourceAddress(ip) => *ip,
                LegKind::System | LegKind::Interface(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            };
            let listener = tokio::net::TcpListener::bind(SocketAddr::new(ip, addr.port())).await?;
            Ok(Box::new(WatchedListener {
                listener,
                watched: self.watched.clone(),
            }) as Box<dyn StreamListener>)
        })
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.inner.bind_udp(addr)
    }
}

struct WatchedListener {
    listener: tokio::net::TcpListener,
    watched: Arc<Watched>,
}

impl StreamListener for WatchedListener {
    fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let (stream, peer) = self.listener.accept().await?;
            self.watched.inbound.fetch_add(1, Ordering::AcqRel);
            self.watched.wake();
            let (read, write) = stream.into_split();
            let state = Arc::new(Connection {
                peer,
                outgoing: false,
                read: AtomicU64::new(0),
                frames: Mutex::default(),
                watched: self.watched.clone(),
            });
            let (read, write) = state.wrap(Box::new(read), Box::new(write));
            Ok((peer, read, write))
        })
    }
}

// One peer connection as the test sees it.
struct Connection {
    peer: SocketAddr,
    outgoing: bool,
    read: AtomicU64,
    frames: Mutex<Frames>,
    watched: Arc<Watched>,
}

// The plain BitTorrent stream a connection sends: the handshake, then
// length-prefixed messages. Only the blocks of piece messages are served
// bytes.
#[derive(Default)]
struct Frames {
    handshake: u64,
    header: [u8; 5],
    header_len: usize,
    // Body bytes left in the message being sent.
    left: u64,
    // The piece message's index and offset, not yet passed.
    preamble: u64,
    piece: bool,
    served: u64,
}

// A piece message's id.
const PIECE: u8 = 7;

impl Frames {
    // The block bytes among `bytes`, the next ones sent.
    fn feed(&mut self, mut bytes: &[u8]) -> u64 {
        let mut served = 0;
        while !bytes.is_empty() {
            if self.handshake < HANDSHAKE_LEN {
                let take = (HANDSHAKE_LEN - self.handshake).min(bytes.len() as u64);
                self.handshake += take;
                bytes = &bytes[take as usize..];
            } else if self.left > 0 {
                let take = self.left.min(bytes.len() as u64);
                let preamble = self.preamble.min(take);
                self.preamble -= preamble;
                if self.piece {
                    served += take - preamble;
                }
                self.left -= take;
                bytes = &bytes[take as usize..];
            } else {
                self.header[self.header_len] = bytes[0];
                self.header_len += 1;
                bytes = &bytes[1..];
                let len = u32::from_be_bytes([
                    self.header[0],
                    self.header[1],
                    self.header[2],
                    self.header[3],
                ]);
                if self.header_len == 4 && len == 0 {
                    // A keep-alive.
                    self.header_len = 0;
                } else if self.header_len == 5 {
                    self.header_len = 0;
                    self.piece = self.header[4] == PIECE;
                    self.left = u64::from(len) - 1;
                    self.preamble = if self.piece { 8 } else { 0 };
                }
            }
        }
        self.served += served;
        served
    }
}

impl Connection {
    fn wrap(
        self: &Arc<Self>,
        read: BoxAsyncReadVectored,
        write: BoxAsyncWrite,
    ) -> (BoxAsyncReadVectored, BoxAsyncWrite) {
        (
            Box::new(WatchedRead {
                inner: read,
                connection: self.clone(),
            }),
            Box::new(WatchedWrite {
                inner: write,
                connection: self.clone(),
            }),
        )
    }

    fn on_read(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let before = self.read.fetch_add(bytes as u64, Ordering::AcqRel);
        if self.outgoing && before < HANDSHAKE_LEN && before + bytes as u64 >= HANDSHAKE_LEN {
            self.watched.answered.fetch_add(1, Ordering::AcqRel);
            self.watched.settled.fetch_add(1, Ordering::AcqRel);
            self.watched.wake();
        }
    }

    // `bytes` went out, in order.
    fn on_sent<'b>(&self, bytes: impl IntoIterator<Item = &'b [u8]>) {
        let (served, first) = {
            let mut frames = self.frames.lock().unwrap_or_else(PoisonError::into_inner);
            let before = frames.served;
            let served: u64 = bytes.into_iter().map(|chunk| frames.feed(chunk)).sum();
            (served, before == 0)
        };
        if served == 0 {
            return;
        }
        if first {
            self.watched.takers.fetch_add(1, Ordering::AcqRel);
        }
        self.watched.sent.fetch_add(served, Ordering::AcqRel);
        *self
            .watched
            .last_sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(tokio::time::Instant::now());
        self.watched.wake();
    }
}

impl Drop for Connection {
    // A dial that never answered settles as it closes.
    fn drop(&mut self) {
        let read = *self.read.get_mut();
        if self.outgoing && read < HANDSHAKE_LEN {
            self.watched.error(format!(
                "{}: closed before the handshake ({read} bytes)",
                self.peer
            ));
            self.watched.settled.fetch_add(1, Ordering::AcqRel);
            self.watched.wake();
        }
    }
}

struct WatchedRead {
    inner: BoxAsyncReadVectored,
    connection: Arc<Connection>,
}

impl AsyncRead for WatchedRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = polled {
            self.connection.on_read(buf.filled().len() - before);
        }
        polled
    }
}

impl AsyncReadVectored for WatchedRead {
    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        vec: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_read_vectored(cx, vec);
        if let Poll::Ready(Ok(read)) = polled {
            self.connection.on_read(read);
        }
        polled
    }
}

struct WatchedWrite {
    inner: BoxAsyncWrite,
    connection: Arc<Connection>,
}

impl AsyncWrite for WatchedWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(sent)) = polled {
            self.connection.on_sent([&buf[..sent]]);
        }
        polled
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(sent)) = polled {
            let mut left = sent;
            self.connection.on_sent(bufs.iter().map_while(|buf| {
                let take = left.min(buf.len());
                left -= take;
                (take > 0).then(|| &buf[..take])
            }));
        }
        polled
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// The pieces a test fetches, held in memory and nothing else. A write to
// any other piece is refused.
#[derive(Clone)]
pub(super) struct PieceStore {
    wanted: Arc<[u32]>,
    verified: watch::Sender<usize>,
    // When the first wanted piece was verified, before any peer heard of it.
    verified_at: Arc<Mutex<Option<tokio::time::Instant>>>,
    pieces: Arc<Mutex<HashMap<u32, Box<[u8]>>>>,
}

impl PieceStore {
    pub(super) fn new(wanted: Vec<u32>) -> Self {
        Self {
            wanted: wanted.into(),
            verified: watch::channel(0).0,
            verified_at: Arc::default(),
            pieces: Arc::default(),
        }
    }

    // How many wanted pieces have been verified.
    pub(super) fn verified(&self) -> watch::Receiver<usize> {
        self.verified.subscribe()
    }

    pub(super) fn verified_at(&self) -> Option<tokio::time::Instant> {
        *self
            .verified_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn factory(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

impl StorageFactory for PieceStore {
    type Storage = HeldPieces;

    fn create(
        &self,
        _shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<HeldPieces> {
        let lengths = *metadata.lengths();
        Ok(HeldPieces {
            store: self.clone(),
            piece_length: u64::from(lengths.default_piece_length()),
            lengths: self
                .wanted
                .iter()
                .filter_map(|index| {
                    lengths
                        .validate_piece_index(*index)
                        .map(|valid| (*index, lengths.piece_length(valid) as usize))
                })
                .collect(),
            offsets: metadata
                .file_infos
                .iter()
                .map(|file| file.offset_in_torrent)
                .collect(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

pub(super) struct HeldPieces {
    store: PieceStore,
    piece_length: u64,
    // Each wanted piece's length.
    lengths: HashMap<u32, usize>,
    // Each file's offset in the torrent.
    offsets: Vec<u64>,
}

impl HeldPieces {
    // Call `each` with every piece span `len` bytes at `offset` in `file`
    // cover: the piece, the offset in it, and the range of the caller's buffer.
    fn spans(
        &self,
        file: usize,
        offset: u64,
        len: usize,
        mut each: impl FnMut(u32, usize, std::ops::Range<usize>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let Some(start) = self.offsets.get(file) else {
            bail!("no file {file}");
        };
        let mut at = start + offset;
        let mut done = 0usize;
        while done < len {
            let piece = u32::try_from(at / self.piece_length)?;
            let within = (at % self.piece_length) as usize;
            let take = ((self.piece_length as usize) - within).min(len - done);
            each(piece, within, done..done + take)?;
            done += take;
            at += take as u64;
        }
        Ok(())
    }
}

impl TorrentStorage for HeldPieces {
    fn init(
        &mut self,
        _shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let pieces = self
            .store
            .pieces
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.spans(file_id, offset, buf.len(), |piece, within, range| {
            let Some(held) = pieces.get(&piece) else {
                bail!("piece {piece} is not held");
            };
            buf[range.clone()].copy_from_slice(&held[within..within + range.len()]);
            Ok(())
        })
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        let mut pieces = self
            .store
            .pieces
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.spans(file_id, offset, buf.len(), |piece, within, range| {
            let Some(length) = self.lengths.get(&piece) else {
                bail!("piece {piece} is not wanted");
            };
            let held = pieces
                .entry(piece)
                .or_insert_with(|| vec![0; *length].into_boxed_slice());
            held[within..within + range.len()].copy_from_slice(&buf[range]);
            Ok(())
        })
    }

    fn remove_file(&self, _file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _path: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
        Ok(())
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(Self {
            store: self.store.clone(),
            piece_length: self.piece_length,
            lengths: self.lengths.clone(),
            offsets: self.offsets.clone(),
        }))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        if self.lengths.contains_key(&piece.get()) {
            self.store
                .verified_at
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert_with(tokio::time::Instant::now);
            self.store.verified.send_modify(|verified| *verified += 1);
        }
        Ok(())
    }
}
