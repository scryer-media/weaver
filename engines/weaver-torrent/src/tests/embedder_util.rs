// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Pieces for tests of the embedder-facing options: an in-memory `TorrentStorage`
//! that records what the session does with it, and a loopback seeder.

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::core::lengths::ValidPieceIndex;
use anyhow::Context;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions,
    ManagedTorrentShared, Session, SessionOptions, TorrentMetadata, create_torrent,
    spawn_utils::BlockingSpawner,
    storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage},
    tests::test_util::{TestPeerMetadata, create_default_random_dir_with_torrents},
};

/// A session with no DHT, trackers, LSD or persistence: it only talks to the peers
/// it is handed, and listens on loopback when `listen` is set.
pub fn isolated_session_opts(listen: bool) -> SessionOptions {
    SessionOptions {
        dht: None,
        persistence: None,
        disable_trackers: true,
        disable_local_service_discovery: true,
        identity: Some(crate::Identity {
            peer_id: TestPeerMetadata::good().as_peer_id(),
            ..Default::default()
        }),
        listen: listen.then(|| ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// One file of `file_size` random bytes, as a torrent with `piece_length` pieces.
pub struct TestTorrent {
    pub dir: TempDir,
    pub torrent: Bytes,
    pub content: Vec<u8>,
}

impl TestTorrent {
    pub async fn new(name: &str, file_size: usize, piece_length: u32) -> anyhow::Result<Self> {
        let dir = create_default_random_dir_with_torrents(1, file_size, Some(name));
        let torrent = create_torrent(
            dir.path(),
            CreateTorrentOptions {
                name: None,
                piece_length: Some(piece_length),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await?
        .as_bytes()?;
        let content = std::fs::read(dir.path().join("0.data"))?;
        Ok(Self {
            dir,
            torrent,
            content,
        })
    }

    /// Seed the torrent from its directory on loopback; returns the session
    /// (keep it alive) and the address to hand to a downloader.
    pub async fn seed(&self) -> anyhow::Result<(Arc<Session>, std::net::SocketAddr)> {
        let session =
            Session::new_with_opts(self.dir.path().into(), isolated_session_opts(true)).await?;
        session
            .add_torrent(
                AddTorrent::from_bytes(self.torrent.clone()),
                Some(AddTorrentOptions {
                    output_folder: Some(self.dir.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected a handle")?
            .wait_until_completed()
            .await?;
        let addr = session.listen_addr().context("seeder is not listening")?;
        Ok((session, addr))
    }
}

/// What the session asked of the storage, shared by every storage the factory makes.
#[derive(Default)]
pub struct StorageLog {
    preads: AtomicUsize,
    // The piece each write landed in, in the order the writes came.
    written_pieces: Mutex<Vec<u32>>,
    completed_pieces: Mutex<Vec<u32>>,
    // Per-file contents, created on the first create() from the torrent's file sizes.
    files: RwLock<Vec<Arc<RwLock<Vec<u8>>>>>,
}

impl StorageLog {
    pub fn preads(&self) -> usize {
        self.preads.load(Ordering::SeqCst)
    }

    pub fn written_pieces(&self) -> Vec<u32> {
        self.written_pieces.lock().clone()
    }

    pub fn completed_pieces(&self) -> Vec<u32> {
        self.completed_pieces.lock().clone()
    }

    pub fn file_contents(&self, file_id: usize) -> Vec<u8> {
        self.files.read()[file_id].read().clone()
    }
}

#[derive(Clone, Default)]
pub struct MemoryStorageFactory {
    pub log: Arc<StorageLog>,
}

impl MemoryStorageFactory {
    /// Storage that holds `files` already, as a torrent's files on disk would.
    pub fn holding(files: Vec<Vec<u8>>) -> Self {
        let factory = Self::default();
        *factory.log.files.write() = files
            .into_iter()
            .map(|file| Arc::new(RwLock::new(file)))
            .collect();
        factory
    }
}

impl StorageFactory for MemoryStorageFactory {
    type Storage = MemoryStorage;

    fn create(
        &self,
        _shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<MemoryStorage> {
        {
            let mut files = self.log.files.write();
            if files.is_empty() {
                *files = metadata
                    .file_infos
                    .iter()
                    .map(|fi| Arc::new(RwLock::new(vec![0u8; usize::try_from(fi.len).unwrap()])))
                    .collect();
            }
        }
        Ok(MemoryStorage {
            log: self.log.clone(),
            offsets: metadata
                .file_infos
                .iter()
                .map(|fi| fi.offset_in_torrent)
                .collect(),
            piece_length: metadata.lengths().default_piece_length() as u64,
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

pub struct MemoryStorage {
    log: Arc<StorageLog>,
    offsets: Vec<u64>,
    piece_length: u64,
}

impl MemoryStorage {
    fn file(&self, file_id: usize) -> anyhow::Result<Arc<RwLock<Vec<u8>>>> {
        self.log
            .files
            .read()
            .get(file_id)
            .cloned()
            .context("no such file")
    }
}

impl TorrentStorage for MemoryStorage {
    fn init(
        &mut self,
        _shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.log.preads.fetch_add(1, Ordering::SeqCst);
        let file = self.file(file_id)?;
        let g = file.read();
        let start = usize::try_from(offset)?;
        buf.copy_from_slice(
            g.get(start..start + buf.len())
                .context("read past the end")?,
        );
        Ok(())
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        // A vectored write whose second half is empty lands here with nothing to write,
        // at the offset just past the first half.
        if buf.is_empty() {
            return Ok(());
        }
        let piece = (self.offsets[file_id] + offset) / self.piece_length;
        self.log.written_pieces.lock().push(u32::try_from(piece)?);
        let file = self.file(file_id)?;
        let mut g = file.write();
        let start = usize::try_from(offset)?;
        g.get_mut(start..start + buf.len())
            .context("write past the end")?
            .copy_from_slice(buf);
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
        Ok(Box::new(MemoryStorage {
            log: self.log.clone(),
            offsets: self.offsets.clone(),
            piece_length: self.piece_length,
        }))
    }

    fn on_piece_completed(&self, piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        self.log.completed_pieces.lock().push(piece_index.get());
        Ok(())
    }
}
