// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    fs::OpenOptions,
    io::IoSlice,
    path::{Path, PathBuf},
};

use std::sync::Arc;

use anyhow::Context;
use tracing::warn;

use crate::session_stats::disk::{DiskStats, FileEnds};

use crate::{
    storage::{StorageFactoryExt, filesystem::opened_file::OurFileExt},
    torrent_state::{ManagedTorrentShared, TorrentMetadata},
};

use crate::storage::{FileStamp, StorageFactory, TorrentStorage};

use super::opened_file::OpenedFile;

#[derive(Default, Clone, Copy)]
pub struct FilesystemStorageFactory {}

impl StorageFactory for FilesystemStorageFactory {
    type Storage = FilesystemStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<FilesystemStorage> {
        Ok(FilesystemStorage {
            output_folder: shared.options.output_folder.clone(),
            opened_files: Default::default(),
            stats: shared.spawner.stats().clone(),
            ends: Default::default(),
        })
    }

    fn clone_box(&self) -> crate::storage::BoxStorageFactory {
        self.boxed()
    }
}

pub struct FilesystemStorage {
    pub(crate) output_folder: PathBuf,
    pub(crate) opened_files: Vec<OpenedFile>,
    // The session's disk counters, and where each file's last read and
    // write ended: a call that starts anywhere else is counted.
    pub(crate) stats: Arc<DiskStats>,
    pub(crate) ends: Vec<FileEnds>,
}

impl FilesystemStorage {
    #[allow(dead_code)]
    pub(crate) fn take_fs(&self) -> anyhow::Result<Self> {
        Ok(Self {
            opened_files: self
                .opened_files
                .iter()
                .map(|f| f.take_clone())
                .collect::<anyhow::Result<Vec<_>>>()?,
            output_folder: self.output_folder.clone(),
            stats: self.stats.clone(),
            ends: self.fresh_ends(),
        })
    }

    fn fresh_ends(&self) -> Vec<FileEnds> {
        self.opened_files.iter().map(|_| FileEnds::default()).collect()
    }

    fn read_at(&self, file_id: usize, offset: u64, len: usize) {
        if let Some(ends) = self.ends.get(file_id) {
            self.stats.file_read(&ends.read, offset, len as u64);
        }
    }

    fn written_at(&self, file_id: usize, offset: u64, len: usize) {
        if let Some(ends) = self.ends.get(file_id) {
            self.stats.file_written(&ends.write, offset, len as u64);
        }
    }
}

impl TorrentStorage for FilesystemStorage {
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.read_at(file_id, offset, buf.len());
        self.opened_files
            .get(file_id)
            .context("no such file")?
            .lock_read()?
            .pread_exact(offset, buf)
    }

    // A run of buffers is read with one positional read where the platform
    // has a vectored one, not one read per buffer.
    fn preadv_exact(
        &self,
        file_id: usize,
        offset: u64,
        bufs: &mut [&mut [u8]],
    ) -> anyhow::Result<()> {
        self.read_at(file_id, offset, bufs.iter().map(|buf| buf.len()).sum());
        let file = self
            .opened_files
            .get(file_id)
            .context("no such file")?
            .lock_read()?;
        Ok(super::opened_file::pread_exact_vectored(
            &file, offset, bufs,
        )?)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.written_at(file_id, offset, buf.len());
        let of = self.opened_files.get(file_id).context("no such file")?;
        #[cfg(windows)]
        return of.try_mark_sparse()?.pwrite_all(offset, buf);
        #[cfg(not(windows))]
        return of.lock_read()?.pwrite_all(offset, buf);
    }

    fn pwrite_all_vectored(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        self.written_at(file_id, offset, bufs[0].len() + bufs[1].len());
        let of = self.opened_files.get(file_id).context("no such file")?;
        #[cfg(windows)]
        return of.try_mark_sparse()?.pwrite_all_vectored(offset, bufs);
        #[cfg(not(windows))]
        return of.lock_read()?.pwrite_all_vectored(offset, bufs);
    }

    fn remove_file(&self, _file_id: usize, filename: &Path) -> anyhow::Result<()> {
        Ok(std::fs::remove_file(self.output_folder.join(filename))?)
    }

    fn ensure_file_length(&self, file_id: usize, len: u64) -> anyhow::Result<()> {
        let f = &self.opened_files.get(file_id).context("no such file")?;
        #[cfg(windows)]
        f.try_mark_sparse()?;
        Ok(f.lock_read()?.set_len(len)?)
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(Self {
            opened_files: self
                .opened_files
                .iter()
                .map(|f| f.take_clone())
                .collect::<anyhow::Result<Vec<_>>>()?,
            output_folder: self.output_folder.clone(),
            stats: self.stats.clone(),
            ends: self.fresh_ends(),
        }))
    }

    // A file with none open, a padding file, has no stamp.
    fn stamp(&self, file_id: usize) -> anyhow::Result<Option<FileStamp>> {
        let of = self.opened_files.get(file_id).context("no such file")?;
        let Ok(f) = of.lock_read() else {
            return Ok(None);
        };
        let meta = f.metadata()?;
        Ok(Some(FileStamp {
            len: meta.len(),
            modified: meta.modified()?,
        }))
    }

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        let path = self.output_folder.join(path);
        if !path.is_dir() {
            anyhow::bail!("cannot remove dir: {path:?} is not a directory")
        }
        if std::fs::read_dir(&path)?.count() == 0 {
            std::fs::remove_dir(&path).with_context(|| format!("error removing {path:?}"))
        } else {
            warn!("did not remove {path:?} as it was not empty");
            Ok(())
        }
    }

    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        let mut files = Vec::<OpenedFile>::new();
        for file_details in metadata.file_infos.iter() {
            let mut full_path = self.output_folder.clone();
            let relative_path = &file_details.relative_filename;
            full_path.push(relative_path);

            if file_details.attrs.padding {
                files.push(OpenedFile::new_dummy());
                continue;
            };
            std::fs::create_dir_all(full_path.parent().context("bug: no parent")?)?;
            let f = if shared.options.allow_overwrite {
                OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&full_path)
                    .with_context(|| format!("error opening {full_path:?} in read/write mode"))?
            } else {
                // create_new does not seem to work with read(true), so calling this twice.
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&full_path)
                    .with_context(|| {
                        format!(
                            "error creating a new file (because allow_overwrite = false) {:?}",
                            full_path
                        )
                    })?;
                OpenOptions::new().read(true).write(true).open(&full_path)?
            };
            files.push(OpenedFile::new(full_path.clone(), f));
        }

        self.opened_files = files;
        self.ends = self.fresh_ends();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{FilesystemStorage, OpenedFile};
    use crate::storage::TorrentStorage;

    // A run of buffers read in one go holds the bytes the same buffers read
    // one at a time hold, and a run past the file's end fails as they do.
    #[test]
    fn a_run_of_buffers_reads_what_they_read_one_at_a_time() {
        let dir = TempDir::with_prefix("fs_preadv").unwrap();
        let path = dir.path().join("payload");
        let mut payload = vec![0u8; 100_000];
        rand::fill(&mut payload[..]);
        std::fs::write(&path, &payload).unwrap();
        let storage = FilesystemStorage {
            output_folder: dir.path().to_owned(),
            opened_files: vec![OpenedFile::new(
                path.clone(),
                std::fs::File::open(&path).unwrap(),
            )],
            stats: Default::default(),
            ends: vec![Default::default()],
        };
        let lens = [16384usize, 16384, 1, 0, 30_000];
        let offset = 5_000u64;

        let mut together: Vec<Vec<u8>> = lens.iter().map(|len| vec![0u8; *len]).collect();
        let mut bufs: Vec<&mut [u8]> = together.iter_mut().map(Vec::as_mut_slice).collect();
        storage.preadv_exact(0, offset, &mut bufs).unwrap();

        let mut at = offset;
        for (len, read) in lens.iter().zip(&together) {
            let mut alone = vec![0u8; *len];
            storage.pread_exact(0, at, &mut alone).unwrap();
            assert_eq!(read, &alone, "the buffer at {at}");
            assert_eq!(alone, payload[at as usize..at as usize + len]);
            at += *len as u64;
        }

        let mut long = [vec![0u8; 60_000], vec![0u8; 60_000]];
        let mut bufs: Vec<&mut [u8]> = long.iter_mut().map(Vec::as_mut_slice).collect();
        assert!(storage.preadv_exact(0, 0, &mut bufs).is_err());
        assert!(storage.preadv_exact(1, 0, &mut bufs).is_err());
    }
}
