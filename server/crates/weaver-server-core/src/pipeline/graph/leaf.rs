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
// through the sparse marker.

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::{Component, Path, PathBuf};

use crate::pipeline::graph::sparse::{PlatformMarker, create_sparse};

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
    // The CRC32 of what was written, by contiguous run keyed by its start,
    // folded as the writes land so the member's own CRC is known without
    // reading the file back.
    written: BTreeMap<u64, Written>,
}

// A contiguous run of written bytes and their CRC32.
#[derive(Clone, Copy, Debug)]
struct Written {
    end: u64,
    crc32: u32,
}

impl Leaf {
    // The leaf for `output_name` under `dir`. Nothing is created until the
    // first byte arrives.
    pub(crate) fn at(dir: &Path, output_name: &str) -> Result<Self, LeafError> {
        Ok(Self {
            path: dir.join(relative_path(output_name)?),
            directory: Self::is_directory(output_name),
            file: None,
            written: BTreeMap::new(),
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
        write_all_at(file, offset, bytes).map_err(LeafError::io(&self.path))?;
        self.wrote(offset, bytes);
        Ok(())
    }

    // Fold a write into the runs. A run the write overlaps is superseded by
    // it; its CRC cannot be split from the part the write leaves standing,
    // so the whole run is forgotten. The write then joins the runs it abuts.
    fn wrote(&mut self, offset: u64, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let end = offset + bytes.len() as u64;
        let overlapped: Vec<u64> = self
            .written
            .range(..end)
            .rev()
            .take_while(|(_, run)| run.end > offset)
            .map(|(start, _)| *start)
            .collect();
        for start in overlapped {
            self.written.remove(&start);
        }
        let mut start = offset;
        let mut run = Written {
            end,
            crc32: crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32,
        };
        if let Some((&before, prior)) = self.written.range(..offset).next_back()
            && prior.end == offset
        {
            run.crc32 = combine(prior.crc32, run.crc32, end - offset);
            start = before;
            self.written.remove(&before);
        }
        if let Some(after) = self.written.remove(&end) {
            run.crc32 = combine(run.crc32, after.crc32, after.end - end);
            run.end = after.end;
        }
        self.written.insert(start, run);
    }

    // The CRC32 of the member's `length` bytes, when one run of writes
    // covers exactly them.
    pub(crate) fn crc32(&self, length: u64) -> Option<u32> {
        match self.written.iter().next() {
            Some((0, run)) if self.written.len() == 1 && run.end == length => Some(run.crc32),
            _ => None,
        }
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
        self.written.clear();
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
            let file =
                create_sparse(&self.path, &PlatformMarker).map_err(LeafError::io(&self.path))?;
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
// refused when it would leave it: an absolute path, `..`, a root or prefix
// component, a Windows drive letter, an embedded NUL, or nothing at all.
fn relative_path(output_name: &str) -> Result<PathBuf, LeafError> {
    let unsafe_name = || LeafError::UnsafeName(output_name.to_owned());
    let normalized = output_name.replace('\\', "/");
    let name = normalized.trim_end_matches('/');
    let path = Path::new(name);
    if name.is_empty() || name.contains('\0') || path.is_absolute() {
        return Err(unsafe_name());
    }
    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let bytes = part.as_encoded_bytes();
                if bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
                    return Err(unsafe_name());
                }
                safe.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(unsafe_name());
            }
        }
    }
    if safe.as_os_str().is_empty() {
        return Err(unsafe_name());
    }
    Ok(safe)
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

// The CRC32 of `first` followed by `len_second` bytes whose CRC32 is `second`.
fn combine(first: u32, second: u32, len_second: u64) -> u32 {
    crc_turbo::checksum_combine(
        crc_turbo::CrcAlgorithm::Crc32IsoHdlc,
        u64::from(first),
        u64::from(second),
        len_second,
    ) as u32
}
