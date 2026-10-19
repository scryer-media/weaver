// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The torrent session's storage, which is the piece root's intake. Nothing
// the session writes goes to a file of its own: a piece's chunks are kept in
// memory while it is in flight, read back from there while the session
// checks its hash, and once it verified each file's share of it goes to the
// job's root, which places it where the job's tree says. After that a read
// of the piece is a read of what the job posted: a file no object reads from
// its own file, a file an object reads through that object's inverse map,
// which puts back what the object took off. So a piece seeds from the one
// copy of its bytes the job keeps.

use std::collections::HashMap;
use std::io::IoSlice;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{Context, bail};
use librqbit::storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage};
use librqbit::{ManagedTorrentShared, TorrentMetadata, ValidatedTorrentMetaV1Info};
use librqbit_core::lengths::ValidPieceIndex;
use weaver_graph::{ByteRange, VolumeIndex};

use crate::pipeline::graph::job::GraphJob;
use crate::pipeline::graph::root::pieces::{LayoutError, PieceFile, PieceLayout, PieceRoot, Verified};

#[cfg(test)]
mod tests;

// Where verified pieces go and posted bytes come from.
pub(crate) trait Pieces: Send + Sync {
    // A piece verified: its files' shares, in torrent order.
    fn verified(&self, shares: Vec<Verified>) -> anyhow::Result<()>;
    // The volume's posted bytes of the range, appended to `into`; false
    // when they are not all in hand.
    fn posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool;
}

// A graph job over a piece root as the storage's other side. `limit` is the
// slot limit every step the job takes follows.
pub(crate) struct TorrentJob {
    job: Mutex<GraphJob<PieceRoot>>,
    limit: usize,
}

impl TorrentJob {
    pub(crate) fn new(job: GraphJob<PieceRoot>, limit: usize) -> Self {
        Self {
            job: Mutex::new(job),
            limit,
        }
    }

    pub(crate) fn with<T>(&self, read: impl FnOnce(&mut GraphJob<PieceRoot>) -> T) -> T {
        read(&mut self.job.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl Pieces for TorrentJob {
    fn verified(&self, shares: Vec<Verified>) -> anyhow::Result<()> {
        let limit = self.limit;
        Ok(self.with(|job| job.verified(shares, limit))?)
    }

    fn posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.with(|job| job.posted(volume, range, into))
    }
}

// The layout of a torrent's files over its pieces, from its metainfo.
pub(crate) fn layout_of<B: AsRef<[u8]>>(
    info: &ValidatedTorrentMetaV1Info<B>,
) -> Result<PieceLayout, LayoutError> {
    let files = info
        .iter_file_details()
        .map(|details| PieceFile {
            name: details.filename.to_vec().join("/"),
            length: details.len,
            padding: details.attrs().padding,
        })
        .collect();
    PieceLayout::new(u64::from(info.lengths().default_piece_length()), files)
}

struct Inner {
    layout: Arc<PieceLayout>,
    pieces: Arc<dyn Pieces>,
    // The pieces in flight, by index, each the length of its piece.
    flight: Mutex<HashMap<u32, Vec<u8>>>,
}

#[derive(Clone)]
pub(crate) struct PieceStorage {
    inner: Arc<Inner>,
}

impl PieceStorage {
    pub(crate) fn new(layout: Arc<PieceLayout>, pieces: Arc<dyn Pieces>) -> Self {
        Self {
            inner: Arc::new(Inner {
                layout,
                pieces,
                flight: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn flight(&self) -> std::sync::MutexGuard<'_, HashMap<u32, Vec<u8>>> {
        self.inner
            .flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    // A file's range in the torrent's coordinates, and the one piece it
    // lies in, when it lies in one.
    fn place(&self, file_id: usize, offset: u64, len: usize) -> anyhow::Result<(u64, Option<u32>)> {
        let layout = &self.inner.layout;
        let file = layout
            .files()
            .nth(file_id)
            .with_context(|| format!("no file {file_id} in the torrent"))?;
        if offset + len as u64 > file.length {
            bail!("{len} bytes at {offset} run past the end of file {file_id}");
        }
        let start = layout
            .torrent_offset(file_id, offset)
            .with_context(|| format!("no file {file_id} in the torrent"))?;
        let piece = start / layout.piece_length();
        let last = (start + len as u64).saturating_sub(1) / layout.piece_length();
        Ok((start, (piece == last).then_some(piece as u32)))
    }
}

impl TorrentStorage for PieceStorage {
    fn init(
        &mut self,
        _: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        let theirs = layout_of(&metadata.info)?;
        if theirs != *self.inner.layout {
            bail!("the torrent is not the one the job was made for");
        }
        Ok(())
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let (start, piece) = self.place(file_id, offset, buf.len())?;
        if let Some(piece) = piece
            && let Some(held) = self.flight().get(&piece)
        {
            let at = (start - self.inner.layout.piece_range(piece).start) as usize;
            buf.copy_from_slice(&held[at..at + buf.len()]);
            return Ok(());
        }
        let Some(volume) = self.inner.layout.file_volume(file_id) else {
            // Padding is zeros by definition.
            buf.fill(0);
            return Ok(());
        };
        let mut read = Vec::with_capacity(buf.len());
        let range = ByteRange::at(offset, buf.len() as u64);
        if !self.inner.pieces.posted(volume, range, &mut read) {
            bail!("{range} of file {file_id} is not in hand");
        }
        buf.copy_from_slice(&read);
        Ok(())
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let (start, piece) = self.place(file_id, offset, buf.len())?;
        let Some(piece) = piece else {
            bail!("a write of file {file_id} crosses a piece boundary");
        };
        let range = self.inner.layout.piece_range(piece);
        let at = (start - range.start) as usize;
        let mut flight = self.flight();
        let held = flight
            .entry(piece)
            .or_insert_with(|| vec![0; range.len() as usize]);
        held[at..at + buf.len()].copy_from_slice(buf);
        Ok(())
    }

    fn pwrite_all_vectored(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        let mut at = offset;
        for buf in &bufs {
            self.pwrite_all(file_id, at, buf)?;
            at += buf.len() as u64;
        }
        Ok((at - offset) as usize)
    }

    // The job's files are the job's: the session removes nothing.
    fn remove_file(&self, _: usize, _: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    // Lengths are the job's announcements, made from the metainfo.
    fn ensure_file_length(&self, _: usize, _: u64) -> anyhow::Result<()> {
        Ok(())
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(self.clone()))
    }

    // The piece's shares go to the job, and only then does the piece stop
    // being read from memory, so no read finds it in neither place.
    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let index = piece.get();
        let Some(held) = self.flight().get(&index).cloned() else {
            bail!("piece {index} completed with nothing written");
        };
        let shares = self
            .inner
            .layout
            .spans(index)
            .into_iter()
            .filter_map(|span| {
                let volume = span.volume?;
                let at = span.at as usize;
                Some(Verified {
                    volume,
                    piece: index,
                    offset: span.range.start,
                    bytes: bytes::Bytes::copy_from_slice(&held[at..at + span.range.len() as usize]),
                })
            })
            .collect();
        self.inner.pieces.verified(shares)?;
        self.flight().remove(&index);
        Ok(())
    }
}

// Makes the storage of one torrent's job.
#[derive(Clone)]
pub(crate) struct PieceStorageFactory {
    layout: Arc<PieceLayout>,
    pieces: Arc<dyn Pieces>,
}

impl PieceStorageFactory {
    pub(crate) fn new(layout: Arc<PieceLayout>, pieces: Arc<dyn Pieces>) -> Self {
        Self { layout, pieces }
    }

    pub(crate) fn boxed_factory(self) -> BoxStorageFactory {
        self.boxed()
    }
}

impl StorageFactory for PieceStorageFactory {
    type Storage = PieceStorage;

    fn create(
        &self,
        _: &ManagedTorrentShared,
        _: &TorrentMetadata,
    ) -> anyhow::Result<PieceStorage> {
        Ok(PieceStorage::new(
            Arc::clone(&self.layout),
            Arc::clone(&self.pieces),
        ))
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}
