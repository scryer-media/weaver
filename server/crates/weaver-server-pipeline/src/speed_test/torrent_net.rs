// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a test torrent's session rides and writes to: the path's sockets,
// watched for how each peer answered, and a store that keeps only the pieces
// the test fetches, in memory and within a bound.

use std::collections::{BTreeSet, HashMap};
use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use anyhow::bail;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::watch;
use weaver_torrent::core::lengths::ValidPieceIndex;
use weaver_torrent::network::{
    AsyncReadVectored, BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket,
};
use weaver_torrent::storage::{
    BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage,
};
use weaver_torrent::{ManagedTorrentShared, SocketFactory, StreamListener, TorrentMetadata};

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
    errors: Mutex<Vec<String>>,
}

impl Watched {
    pub(super) fn new() -> Self {
        Self {
            wake: watch::channel(0).0,
            dials: AtomicUsize::new(0),
            settled: AtomicUsize::new(0),
            answered: AtomicUsize::new(0),
            inbound: AtomicUsize::new(0),
            errors: Mutex::new(Vec::new()),
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

    pub(super) fn errors(&self) -> Vec<String> {
        self.errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn error(&self, error: String) {
        let mut errors = self.errors.lock().unwrap_or_else(PoisonError::into_inner);
        if errors.len() < ERRORS_KEPT && !errors.contains(&error) {
            errors.push(error);
        }
    }
}

// The sockets of the test session's leg, every connection watched.
pub(super) struct ProbeSockets {
    inner: Arc<dyn SocketFactory>,
    watched: Arc<Watched>,
}

impl ProbeSockets {
    pub(super) fn new(inner: Arc<dyn SocketFactory>, watched: Arc<Watched>) -> Self {
        Self { inner, watched }
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
            let listener = self.inner.listen_tcp(addr).await?;
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

    fn bind_utp<'a>(
        &'a self,
        addr: SocketAddr,
        receive_buffer: usize,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.inner.bind_utp(addr, receive_buffer)
    }
}

// The leg's listener, every peer it takes in counted.
struct WatchedListener {
    listener: Box<dyn StreamListener>,
    watched: Arc<Watched>,
}

impl StreamListener for WatchedListener {
    fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr()
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let (peer, read, write) = self.listener.accept().await?;
            self.watched.inbound.fetch_add(1, Ordering::AcqRel);
            self.watched.wake();
            let state = Arc::new(Connection {
                peer,
                outgoing: false,
                read: AtomicU64::new(0),
                watched: self.watched.clone(),
            });
            let (read, write) = state.wrap(read, write);
            Ok((peer, read, write))
        })
    }
}

// One peer connection as the test sees it: what it read, to tell whether
// the peer answered. What it sends is the engine's to count.
struct Connection {
    peer: SocketAddr,
    outgoing: bool,
    read: AtomicU64,
    watched: Arc<Watched>,
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
            write,
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

// The pieces a test fetches, held in memory and nothing else. A write to
// any other piece is refused. Verified pieces are held up to `keep` bytes;
// one verified past that is let go, so a long pull stays bounded.
#[derive(Clone)]
pub(super) struct PieceStore {
    wanted: Arc<[u32]>,
    keep: u64,
    // The verified pieces held, and their bytes.
    held: Arc<Mutex<(BTreeSet<u32>, u64)>>,
    // Told the bytes of each block as it is written.
    written: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    verified: watch::Sender<usize>,
    // When the first wanted piece was verified, before any peer heard of it.
    verified_at: Arc<Mutex<Option<tokio::time::Instant>>>,
    pieces: Arc<Mutex<HashMap<u32, Box<[u8]>>>>,
}

impl PieceStore {
    pub(super) fn new(wanted: Vec<u32>) -> Self {
        Self {
            wanted: wanted.into(),
            keep: u64::MAX,
            held: Arc::default(),
            written: None,
            verified: watch::channel(0).0,
            verified_at: Arc::default(),
            pieces: Arc::default(),
        }
    }

    // Hold at most `keep` bytes of verified pieces.
    pub(super) fn keeping(self, keep: u64) -> Self {
        Self { keep, ..self }
    }

    // Tell `written` the bytes of each block as it is written.
    pub(super) fn counting(self, written: impl Fn(u64) + Send + Sync + 'static) -> Self {
        Self {
            written: Some(Arc::new(written)),
            ..self
        }
    }

    pub(super) fn wanted(&self) -> &[u32] {
        &self.wanted
    }

    // The verified pieces held.
    pub(super) fn held(&self) -> Vec<u32> {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0
            .iter()
            .copied()
            .collect()
    }

    // A store with the pieces this one holds, wanting nothing more.
    pub(super) fn holding(&self) -> Self {
        let held = self
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        Self {
            wanted: held.0.iter().copied().collect(),
            keep: u64::MAX,
            held: Arc::new(Mutex::new(held)),
            written: None,
            verified: watch::channel(0).0,
            verified_at: Arc::default(),
            pieces: self.pieces.clone(),
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
        })?;
        drop(pieces);
        if let Some(written) = &self.store.written {
            written(buf.len() as u64);
        }
        Ok(())
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
        let piece = piece.get();
        let Some(length) = self.lengths.get(&piece) else {
            return Ok(());
        };
        {
            let mut held = self
                .store
                .held
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let length = *length as u64;
            if held.1 + length <= self.store.keep {
                if held.0.insert(piece) {
                    held.1 += length;
                }
            } else {
                self.store
                    .pieces
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&piece);
            }
        }
        self.store
            .verified_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(tokio::time::Instant::now);
        self.store.verified.send_modify(|verified| *verified += 1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use weaver_torrent::network::{KernelSockets, Resolved, Resolver};
    use weaver_tunnel::pipe::{DialError, Dialed, Dialer, Resolution, Target};

    use super::*;

    // A path whose naming answers one address, noting each name it is
    // asked.
    #[derive(Default)]
    struct Path(Mutex<Vec<String>>);

    #[async_trait::async_trait]
    impl Dialer for Path {
        async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
            Err(DialError::Skipped("names only".into()))
        }
        async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
            self.0.lock().unwrap().push(host.into());
            Ok(Resolution::Addresses(vec![IpAddr::from([192, 0, 2, 3])]))
        }
        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_secs(60)
        }
        fn describe(&self) -> String {
            "path".into()
        }
    }

    // A test torrent's names resolve on the path it tests, by the path's
    // naming, as a session's do on its leg.
    #[tokio::test]
    async fn a_probe_resolves_names_on_its_path() {
        let path = Arc::new(Path::default());
        let resolver = crate::torrents::net::LegNames(path.clone());
        assert_eq!(
            resolver.resolve("tracker.test", 6969).await.unwrap(),
            Resolved::Addresses(vec![SocketAddr::from(([192, 0, 2, 3], 6969))])
        );
        assert_eq!(*path.0.lock().unwrap(), ["tracker.test"]);
    }

    // A peer the leg's listener takes in is counted, whichever sockets the
    // leg opens.
    #[tokio::test]
    async fn a_peer_taken_in_is_counted() {
        let watched = Arc::new(Watched::new());
        let sockets =
            ProbeSockets::new(Arc::new(KernelSockets::new(None).unwrap()), watched.clone());
        let listener = sockets
            .listen_tcp(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let (dialed, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr()),
            listener.accept()
        );
        dialed.unwrap();
        accepted.unwrap();
        assert_eq!(watched.inbound(), 1);
    }
}
