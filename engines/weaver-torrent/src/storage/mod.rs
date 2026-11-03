// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Storage engine for torrent data.
//!
//! This is deliberately sync, not async by design, for several reasons.
//!
//! Reason 1. Performance: avoiding copying.
//!
//! Torrent files are large so memcpy costs can compound. Tokio FS does all file writes in a thread
//! pool. To do those writes it has a buffer per file. When you call e.g. "write", your request is first
//! memcpy'ed to that buffer, and only then written to the file.
//!
//! On the write path (download), we write straight from the peer's socket buffer into the file.
//! On the read path (upload), we read straight into the peer's socket buffer also.
//!
//! Reason 2. Memory use: memory bloat would be a problem if tokio::fs was used.
//!
//! The said buffers above default to 2MB. We have a lot of files open, so this can compound into a pretty large
//! memory use.
//!
//! Reason 3. Performance: advanced FS APIs.
//!
//! We use positioned vectored writes (pwritev). Tokio doesn't support that.
//! Positioned so that writing can be done to files in parallel without locks.
//! Vectored so that we issue 1 write call for a potentially non-contiguous chunk.

pub mod filesystem;

use std::{
    any::{Any, TypeId},
    io::IoSlice,
    path::Path,
    sync::Arc,
    time::SystemTime,
};

use crate::core::lengths::ValidPieceIndex;
use tokio::sync::Notify;

use crate::torrent_state::{ManagedTorrentShared, TorrentMetadata};

/// Whether a write failed in a way later writes likely will too while reads
/// still work: no space, a quota, a read-only or forbidding file system, a
/// file past its size limit. The torrent stops downloading and keeps seeding
/// (libtorrent `torrent::handle_disk_error` enters upload mode on these write
/// errors).
pub fn write_refused(error: &anyhow::Error) -> bool {
    use std::io::ErrorKind::*;
    error.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                StorageFull | QuotaExceeded | ReadOnlyFilesystem | PermissionDenied | FileTooLarge
            )
        })
    })
}

pub trait StorageFactory: Send + Sync + Any {
    type Storage: TorrentStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<Self::Storage>;
    fn create_and_init(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<Self::Storage> {
        let mut storage = self.create(shared, metadata)?;
        storage.init(shared, metadata)?;
        Ok(storage)
    }

    /// Whether this factory, or the one it ultimately wraps, is of the given type.
    ///
    /// It has to survive both wrapping and boxing: this is how e.g. session persistence
    /// asks whether it's dealing with a FilesystemStorageFactory, and a middleware in
    /// between must not hide it. A middleware forwards this to what it wraps.
    fn is_type_id(&self, type_id: TypeId) -> bool {
        Self::type_id(self) == type_id
    }
    fn clone_box(&self) -> BoxStorageFactory;
}

pub type BoxStorageFactory = Box<dyn StorageFactory<Storage = Box<dyn TorrentStorage>>>;

pub trait StorageFactoryExt {
    fn boxed(self) -> BoxStorageFactory;
}

impl<SF: StorageFactory> StorageFactoryExt for SF {
    fn boxed(self) -> BoxStorageFactory {
        struct Wrapper<SF> {
            sf: SF,
        }

        impl<SF: StorageFactory> StorageFactory for Wrapper<SF> {
            type Storage = Box<dyn TorrentStorage>;

            fn create(
                &self,
                shared: &ManagedTorrentShared,
                metadata: &TorrentMetadata,
            ) -> anyhow::Result<Self::Storage> {
                let s = self.sf.create(shared, metadata)?;
                Ok(Box::new(s))
            }

            fn is_type_id(&self, type_id: TypeId) -> bool {
                self.sf.is_type_id(type_id)
            }

            fn clone_box(&self) -> BoxStorageFactory {
                self.sf.clone_box()
            }
        }

        Box::new(Wrapper { sf: self })
    }
}

impl<U: StorageFactory + ?Sized> StorageFactory for Box<U> {
    type Storage = U::Storage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<U::Storage> {
        (**self).create(shared, metadata)
    }

    fn is_type_id(&self, type_id: TypeId) -> bool {
        (**self).is_type_id(type_id)
    }

    fn clone_box(&self) -> BoxStorageFactory {
        (**self).clone_box()
    }
}

/// What a file of the storage is from outside: its length and when it was
/// last written. A checkpoint records each file's, and the next run trusts
/// the checkpoint only while every file's is the same (libtorrent 1.x
/// `file_sizes` in the resume data, which `verify_resume_data` compared).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileStamp {
    pub len: u64,
    pub modified: SystemTime,
}

pub trait TorrentStorage: Send + Sync {
    // Create/open files etc.
    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()>;

    /// Given a file_id (which you can get more info from in init_storage() through torrent info)
    /// read buf.len() bytes into buf at offset.
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()>;

    /// Given a file_id (which you can get more info from in init_storage() through torrent info)
    /// write buf.len() bytes into the file at offset.
    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()>;

    fn pwrite_all_vectored(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        let mut offset = offset;
        let mut size = 0;

        for ioslice in bufs {
            self.pwrite_all(file_id, offset, &ioslice)?;
            offset += ioslice.len() as u64;
            size += ioslice.len();
        }

        Ok(size)
    }

    /// Remove a file from the storage. If not supported, or it doesn't matter, just return Ok(())
    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()>;

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()>;

    /// E.g. for filesystem backend ensure that the file has a certain length, and grow/shrink as needed.
    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()>;

    /// Replace the current storage with a dummy, and return a new one that should be used instead.
    /// This is used to make the underlying object useless when e.g. pausing the torrent.
    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>>;

    /// Callback called every time a piece has completed and has been validated.
    /// Default implementation does nothing, but can be override in trait implementations.
    fn on_piece_completed(&self, _piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        Ok(())
    }

    /// Hands `update` the whole piece, in order, straight from where the
    /// storage holds it in memory, and returns true; false, having handed it
    /// nothing, when the storage holds no copy of the piece in memory, so
    /// the caller reads it. A downloaded piece is then hashed where its
    /// blocks landed, with no read back.
    fn hash_held_piece(
        &self,
        _piece_index: ValidPieceIndex,
        _update: &mut dyn FnMut(&[u8]),
    ) -> bool {
        false
    }

    /// Callback called every time a piece failed its hash check, before it is queued again:
    /// a downloaded piece, or one a check of the files did not find, which the storage then
    /// holds no longer. Default implementation does nothing, but can be override in trait
    /// implementations.
    fn on_piece_hash_failed(&self, _piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called when a run that checks every piece and trusts nothing saved begins, before it
    /// reads anything: from here the storage has no piece the check has not found, and what it
    /// saves must not vouch for one. Each piece comes back through
    /// [`Self::on_piece_checked`] or [`Self::on_piece_hash_failed`] as the check finds it or not.
    /// Default implementation does nothing.
    fn on_check_everything(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Callback called when a check of the files found a piece whole: the storage has it again.
    /// Default implementation does nothing.
    fn on_piece_checked(&self, _piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        Ok(())
    }

    /// Asked by a check of the files before it reads a piece: whether the storage wants the
    /// piece's bytes as the check hashed them. One it wants is read whole into one buffer and
    /// handed to [`Self::on_piece_hashed`] if it passes, so the storage need not read it again.
    /// Default implementation wants none.
    fn wants_hashed_piece(&self, _piece_index: ValidPieceIndex) -> bool {
        false
    }

    /// A piece a check of the files read whole and found, as it hashed it: called from the
    /// check's thread before the verdict reaches [`Self::on_piece_checked`]. Default
    /// implementation does nothing.
    fn on_piece_hashed(&self, _piece_index: ValidPieceIndex, _bytes: bytes::Bytes) {}

    /// Asked before a queued piece is handed to a peer, or a block of one is written: whether
    /// the storage has room for it. On false the piece stays queued, the block is discarded,
    /// and the peer waits on `room`, which the storage rings once it has room again. Default
    /// implementation always has room.
    fn reserve_piece(&self, _piece_index: ValidPieceIndex, _room: &Arc<Notify>) -> bool {
        true
    }

    /// The stamp of file `file_id`, or None when the storage keeps no file
    /// for it. Default implementation keeps no files.
    fn stamp(&self, _file_id: usize) -> anyhow::Result<Option<FileStamp>> {
        Ok(None)
    }
}

impl<U: TorrentStorage + ?Sized> TorrentStorage for Box<U> {
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        (**self).pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        (**self).pwrite_all(file_id, offset, buf)
    }

    fn pwrite_all_vectored(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        (**self).pwrite_all_vectored(file_id, offset, bufs)
    }

    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
        (**self).remove_file(file_id, filename)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        (**self).ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        (**self).take()
    }

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        (**self).remove_directory_if_empty(path)
    }

    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        (**self).init(shared, metadata)
    }

    fn on_piece_completed(&self, piece_id: ValidPieceIndex) -> anyhow::Result<()> {
        (**self).on_piece_completed(piece_id)
    }

    fn hash_held_piece(&self, piece_id: ValidPieceIndex, update: &mut dyn FnMut(&[u8])) -> bool {
        (**self).hash_held_piece(piece_id, update)
    }

    fn on_piece_hash_failed(&self, piece_id: ValidPieceIndex) -> anyhow::Result<()> {
        (**self).on_piece_hash_failed(piece_id)
    }

    fn on_check_everything(&self) -> anyhow::Result<()> {
        (**self).on_check_everything()
    }

    fn on_piece_checked(&self, piece_id: ValidPieceIndex) -> anyhow::Result<()> {
        (**self).on_piece_checked(piece_id)
    }

    fn wants_hashed_piece(&self, piece_id: ValidPieceIndex) -> bool {
        (**self).wants_hashed_piece(piece_id)
    }

    fn on_piece_hashed(&self, piece_id: ValidPieceIndex, bytes: bytes::Bytes) {
        (**self).on_piece_hashed(piece_id, bytes)
    }

    fn reserve_piece(&self, piece_id: ValidPieceIndex, room: &Arc<Notify>) -> bool {
        (**self).reserve_piece(piece_id, room)
    }

    fn stamp(&self, file_id: usize) -> anyhow::Result<Option<FileStamp>> {
        (**self).stamp(file_id)
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::{
        BoxStorageFactory, StorageFactory, StorageFactoryExt, filesystem::FilesystemStorageFactory,
    };
    use crate::torrent_state::{ManagedTorrentShared, TorrentMetadata};

    // A middleware like the ones in storage::middleware: it wraps another factory and
    // forwards is_type_id, so that what it wraps stays recognizable through it.
    #[derive(Clone)]
    struct Middleware<U> {
        underlying_factory: U,
    }

    impl<U: StorageFactory + Clone> StorageFactory for Middleware<U> {
        type Storage = U::Storage;

        fn create(
            &self,
            shared: &ManagedTorrentShared,
            metadata: &TorrentMetadata,
        ) -> anyhow::Result<Self::Storage> {
            self.underlying_factory.create(shared, metadata)
        }

        fn is_type_id(&self, type_id: TypeId) -> bool {
            self.underlying_factory.is_type_id(type_id)
        }

        fn clone_box(&self) -> BoxStorageFactory {
            self.clone().boxed()
        }
    }

    #[test]
    fn test_is_type_id_survives_boxing() {
        let fs = TypeId::of::<FilesystemStorageFactory>();

        assert!(FilesystemStorageFactory::default().is_type_id(fs));
        // Box<U> has to forward, or the answer is the TypeId of the box itself.
        assert!(FilesystemStorageFactory::default().boxed().is_type_id(fs));

        // And boxed() has to forward too, or every middleware's override is lost and the
        // answer is the middleware's own type.
        let wrapped = Middleware {
            underlying_factory: FilesystemStorageFactory::default(),
        };
        assert!(wrapped.is_type_id(fs));
        assert!(wrapped.clone().boxed().is_type_id(fs));
        assert!(wrapped.boxed().clone_box().is_type_id(fs));

        // A factory that isn't the one asked about still says so.
        assert!(
            !Middleware {
                underlying_factory: FilesystemStorageFactory::default(),
            }
            .boxed()
            .is_type_id(TypeId::of::<Middleware<FilesystemStorageFactory>>())
        );
    }
}
