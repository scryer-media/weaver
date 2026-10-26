// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A leaf is a member no object owns, and its bytes go to a file. The file
// sits under the job's working directory at the name its namespace
// allocated; the leaf never chooses a name. Delivery out of the working
// directory stays with finalize, which moves what is there and renames
// obfuscated members on the way.
//
// Bytes land at their member offsets in whatever order they arrive, so the
// file is sparse until it is covered. Windows needs that asked for before the
// first write or it allocates every hole, which is why every leaf is created
// through the direct store's sparse marker.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use crate::pipeline::direct_store::sparse::{SparseMarking, create_sparse};

#[derive(Debug, thiserror::Error)]
pub(crate) enum LeafError {
    #[error("output name {0:?} would escape the directory it is written under")]
    UnsafeName(String),
    #[error("{}: {source}", path.display())]
    Io { path: PathBuf, source: io::Error },
}

impl LeafError {
    fn io(path: &Path) -> impl FnOnce(io::Error) -> LeafError + '_ {
        move |source| LeafError::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Leaf {
    path: PathBuf,
    // A directory member, by its name: it has no bytes, and closing it
    // makes the directory.
    directory: bool,
    // Open while bytes are still arriving; dropped when the member is
    // covered, and reopened only for a repair.
    file: Option<File>,
}

impl Leaf {
    // The leaf for `output_name` under `dir`. Nothing is created until the
    // first byte arrives.
    pub(crate) fn at(dir: &Path, output_name: &str) -> Result<Self, LeafError> {
        Ok(Self {
            path: dir.join(relative_path(output_name)?),
            directory: Self::is_directory(output_name),
            file: None,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(test)]
    pub(crate) fn is_open(&self) -> bool {
        self.file.is_some()
    }

    // A directory member is a name ending in a separator. It has no bytes,
    // so it exists the moment it is announced.
    pub(crate) fn is_directory(output_name: &str) -> bool {
        output_name.ends_with(['/', '\\'])
    }

    pub(crate) fn create_directory(&self) -> Result<(), LeafError> {
        std::fs::create_dir_all(&self.path).map_err(LeafError::io(&self.path))
    }

    // Write `bytes` at `offset`. The first write creates the file, marked
    // sparse, and sizes it to `length` when the length is known, so the
    // blocks are reserved in one step instead of growing write by write.
    pub(crate) fn write(
        &mut self,
        offset: u64,
        bytes: &[u8],
        length: Option<u64>,
    ) -> Result<(), LeafError> {
        let file = self.open(length)?;
        write_all_at(file, offset, bytes).map_err(LeafError::io(&self.path))
    }

    // How far the file reaches: every byte written to it lies below.
    pub(crate) fn len(&self) -> Result<u64, LeafError> {
        match &self.file {
            Some(file) => file.metadata(),
            None => std::fs::metadata(&self.path),
        }
        .map(|metadata| metadata.len())
        .map_err(LeafError::io(&self.path))
    }

    // Fill `into` with `len` bytes from `offset`. The caller only asks for
    // ranges its coverage says were written.
    pub(crate) fn read(
        &self,
        offset: u64,
        len: usize,
        into: &mut Vec<u8>,
    ) -> Result<(), LeafError> {
        let start = into.len();
        into.resize(start + len, 0);
        let read = match &self.file {
            Some(file) => read_exact_at(file, offset, &mut into[start..]),
            None => File::open(&self.path)
                .and_then(|file| read_exact_at(&file, offset, &mut into[start..])),
        };
        read.map_err(|source| {
            into.truncate(start);
            LeafError::Io {
                path: self.path.clone(),
                source,
            }
        })
    }

    // Every byte is in: the file takes its final length and its handle is
    // released, so finalize can move it. A directory has no bytes to take:
    // it is made.
    pub(crate) fn close(&mut self, length: u64) -> Result<(), LeafError> {
        if self.directory {
            return self.create_directory();
        }
        let file = self.open(Some(length))?;
        file.set_len(length).map_err(LeafError::io(&self.path))?;
        self.file = None;
        Ok(())
    }

    // The member starts over: the file stays, so the name stays reserved on
    // disk, but none of its bytes do.
    pub(crate) fn truncate(&mut self) -> Result<(), LeafError> {
        if self.file.is_none() && !self.path.exists() {
            return Ok(());
        }
        let file = self.open(None)?;
        file.set_len(0).map_err(LeafError::io(&self.path))
    }

    // The member was renamed; the file follows it.
    pub(crate) fn rename(&mut self, dir: &Path, output_name: &str) -> Result<(), LeafError> {
        let path = dir.join(relative_path(output_name)?);
        if path == self.path {
            return Ok(());
        }
        let had_file = self.file.take().is_some() || self.path.exists();
        if had_file {
            create_parent(&path)?;
            std::fs::rename(&self.path, &path).map_err(LeafError::io(&self.path))?;
        }
        self.path = path;
        Ok(())
    }

    // The member is gone with its subtree.
    pub(crate) fn remove(mut self) -> Result<(), LeafError> {
        self.file = None;
        match std::fs::remove_file(&self.path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(LeafError::Io {
                path: self.path,
                source: error,
            }),
            _ => Ok(()),
        }
    }

    fn open(&mut self, length: Option<u64>) -> Result<&File, LeafError> {
        if self.file.is_none() {
            create_parent(&self.path)?;
            let file = create_sparse(&self.path, &SparseMarking::Platform)
                .map_err(LeafError::io(&self.path))?;
            if let Some(length) = length {
                let current = file.metadata().map_err(LeafError::io(&self.path))?.len();
                if current < length {
                    file.set_len(length).map_err(LeafError::io(&self.path))?;
                }
            }
            self.file = Some(file);
        }
        Ok(self.file.as_ref().expect("opened above"))
    }
}

// An output name as a path relative to the directory it is written under,
// refused when it would leave it. The same validator the extractor uses, so
// the two never disagree on which names are safe.
fn relative_path(output_name: &str) -> Result<PathBuf, LeafError> {
    let normalized = output_name.replace('\\', "/");
    crate::pipeline::extraction::validate_relative_output_path(normalized.trim_end_matches('/'))
        .map_err(|_| LeafError::UnsafeName(output_name.to_owned()))
}

fn create_parent(path: &Path) -> Result<(), LeafError> {
    match path.parent() {
        Some(parent) => std::fs::create_dir_all(parent).map_err(LeafError::io(parent)),
        None => Ok(()),
    }
}

fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(bytes, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut written = 0usize;
        while written < bytes.len() {
            // A zero-byte `seek_write` is no progress, and looping on it
            // would never end.
            let progress = file.seek_write(&bytes[written..], offset + written as u64)?;
            if progress == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            written += progress;
        }
        Ok(())
    }
}

pub(crate) fn read_exact_at(file: &File, offset: u64, out: &mut [u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(out, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut read = 0usize;
        while read < out.len() {
            let progress = file.seek_read(&mut out[read..], offset + read as u64)?;
            if progress == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            read += progress;
        }
        Ok(())
    }
}
