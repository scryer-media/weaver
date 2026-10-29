// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A leaf is a member no object owns, and its bytes go to a file. The file
// sits under the job's working directory at the name its namespace
// allocated; the leaf never chooses a name. A member the ledger names again
// keeps its file where it is until the checkpoint that records the new name
// has committed: the leaf holds both names until then, and only the name it
// sits at is ever opened. Delivery out of the working
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
    // Runtime identity and content revision for in-flight readers. Neither
    // survives a restart, when no worker from this process can return.
    identity: u64,
    revision: u64,
    path: PathBuf,
    // The name the file sits at, as `path` joins it under the directory.
    name: String,
    // The name the file is to move to once the checkpoint that records it
    // has committed; `None` while it sits at its name.
    wants: Option<String>,
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

// How much of a file is read back at a time when its CRC is not in the fold.
const READ_BACK: u64 = 1 << 20;
static NEXT_LEAF: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

// A contiguous run of written bytes and their CRC32.
#[derive(Clone, Copy, Debug)]
struct Written {
    end: u64,
    crc32: u32,
}

impl Leaf {
    // The leaf for `output_name` under `dir`. Nothing is created until the
    // first byte arrives.
    pub(crate) fn at(dir: &Path, output_name: &str) -> Self {
        Self {
            identity: NEXT_LEAF.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            revision: 0,
            path: dir.join(relative_path(output_name)),
            name: output_name.to_owned(),
            wants: None,
            directory: Self::is_directory(output_name),
            file: None,
            written: BTreeMap::new(),
        }
    }

    pub(crate) fn revision(&self) -> (u64, u64) {
        (self.identity, self.revision)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    // The name the file sits at.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    // The name the file waits to move to, if it waits.
    pub(crate) fn wants(&self) -> Option<&str> {
        self.wants.as_deref()
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
        self.revision += 1;
        self.open(length)?;
        let replaced = self.replaced(offset, bytes.len() as u64)?;
        let file = self.file.as_ref().expect("opened above");
        write_all_at(file, offset, bytes).map_err(LeafError::io(&self.path))?;
        self.wrote(offset, bytes, &replaced);
        Ok(())
    }

    // The bytes a write over `offset..offset + len` replaces, of each run
    // it overlaps, in order: what the fold needs to keep those runs, and no
    // more than the write itself puts down.
    fn replaced(&self, offset: u64, len: u64) -> Result<Vec<(u64, Vec<u8>)>, LeafError> {
        let end = offset + len;
        let mut replaced = Vec::new();
        for (start, run) in self
            .written
            .range(..end)
            .rev()
            .take_while(|(_, run)| run.end > offset)
        {
            let from = (*start).max(offset);
            let mut old = Vec::new();
            self.read(from, index(run.end.min(end) - from), &mut old)?;
            replaced.push((from, old));
        }
        replaced.reverse();
        Ok(replaced)
    }

    // Fold a write into the runs. A run the write overlaps keeps its span:
    // CRC32 is linear, so the bytes it replaced give way to the bytes that
    // replace them by the CRC of the two, carried past the run's bytes
    // behind them. What of the write no run held is a run of its own, and
    // runs that now abut are joined.
    fn wrote(&mut self, offset: u64, bytes: &[u8], replaced: &[(u64, Vec<u8>)]) {
        if bytes.is_empty() {
            return;
        }
        let end = offset + bytes.len() as u64;
        let of = |from: u64, to: u64| &bytes[index(from - offset)..index(to - offset)];
        let mut at = offset;
        for (from, old) in replaced {
            let to = from + old.len() as u64;
            if at < *from {
                self.written.insert(
                    at,
                    Written {
                        end: *from,
                        crc32: crc32(of(at, *from)),
                    },
                );
            }
            let (_, run) = self
                .written
                .range_mut(..=*from)
                .next_back()
                .expect("a replaced range lies in a run");
            let behind = run.end - to;
            run.crc32 ^= combine(crc32(old), 0, behind) ^ combine(crc32(of(*from, to)), 0, behind);
            at = to;
        }
        if at < end {
            self.written.insert(
                at,
                Written {
                    end,
                    crc32: crc32(of(at, end)),
                },
            );
        }
        self.join(offset, end);
    }

    // Join the runs about `offset..end` that abut.
    fn join(&mut self, offset: u64, end: u64) {
        let first = self
            .written
            .range(..offset)
            .next_back()
            .filter(|(_, run)| run.end >= offset)
            .map_or(offset, |(start, _)| *start);
        let starts: Vec<u64> = self
            .written
            .range(first..=end)
            .map(|(start, _)| *start)
            .collect();
        let mut joined: Option<(u64, Written)> = None;
        for start in starts {
            let run = self.written.remove(&start).expect("listed above");
            joined = match joined {
                Some((from, prior)) if prior.end == start => Some((
                    from,
                    Written {
                        end: run.end,
                        crc32: combine(prior.crc32, run.crc32, run.end - start),
                    },
                )),
                Some((from, prior)) => {
                    self.written.insert(from, prior);
                    Some((start, run))
                }
                None => Some((start, run)),
            };
        }
        if let Some((from, run)) = joined {
            self.written.insert(from, run);
        }
    }

    // The file ends at `length`: the runs past it go, and a run it cuts
    // keeps the CRC of what stays, the bytes it loses undone from it, read
    // before they go.
    fn cut(&mut self, length: u64) -> Result<(), LeafError> {
        let past: Vec<u64> = self
            .written
            .range(length..)
            .map(|(start, _)| *start)
            .collect();
        for start in past {
            self.written.remove(&start);
        }
        let Some((&start, &run)) = self.written.range(..length).next_back() else {
            return Ok(());
        };
        if run.end <= length {
            return Ok(());
        }
        let mut lost = Vec::new();
        self.read(length, index(run.end - length), &mut lost)?;
        let kept =
            crc_turbo::CrcCombine::new(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, run.end - length)
                .uncombine(u64::from(run.crc32), u64::from(crc32(&lost)))
                .expect("CRC32 has an x^0 term") as u32;
        self.written.insert(
            start,
            Written {
                end: length,
                crc32: kept,
            },
        );
        Ok(())
    }

    // A restored member's fold, which a restore starts empty. A member
    // whose bytes passed its stated CRC folds to that CRC; any other folds
    // what of `ranges` its file holds, read back from it. A range the file
    // does not hold is left out of the fold.
    pub(crate) fn fold_passed(&mut self, length: u64, crc32: u32) {
        self.written.clear();
        if length > 0 {
            self.written.insert(0, Written { end: length, crc32 });
        }
    }

    pub(crate) fn fold_back(&mut self, ranges: impl IntoIterator<Item = (u64, u64)>) {
        let mut chunk = Vec::new();
        for (start, end) in ranges {
            let mut at = start;
            while at < end {
                let take = (end - at).min(READ_BACK);
                chunk.clear();
                if self.read(at, index(take), &mut chunk).is_err() {
                    break;
                }
                self.wrote(at, &chunk, &[]);
                at += take;
            }
        }
    }

    // The CRC32 of the member's `length` bytes as the fold holds them,
    // when one run covers exactly them.
    pub(crate) fn folded(&self, length: u64) -> Option<u32> {
        if length == 0 {
            return Some(0);
        }
        match self.written.iter().next() {
            Some((0, run)) if self.written.len() == 1 && run.end == length => Some(run.crc32),
            _ => None,
        }
    }

    // Create the file in the directory it lies in, which must already be
    // there: nothing above the file is made. A file already open is kept.
    pub(crate) fn create_within(&mut self) -> Result<(), LeafError> {
        if self.file.is_none() {
            let file =
                create_sparse(&self.path, &PlatformMarker).map_err(LeafError::io(&self.path))?;
            self.file = Some(file);
        }
        Ok(())
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
        self.revision += 1;
        self.open(Some(length))?;
        self.cut(length)?;
        let file = self.file.as_ref().expect("opened above");
        file.set_len(length).map_err(LeafError::io(&self.path))?;
        self.file = None;
        Ok(())
    }

    // The member starts over: the file stays, so the name stays reserved on
    // disk, but none of its bytes do.
    pub(crate) fn truncate(&mut self) -> Result<(), LeafError> {
        self.revision += 1;
        self.written.clear();
        if self.file.is_none() && !self.path.exists() {
            return Ok(());
        }
        let file = self.open(None)?;
        file.set_len(0).map_err(LeafError::io(&self.path))
    }

    // The member was named again: the file is to follow it, once the
    // checkpoint that records the name has committed. Nothing on disk
    // changes here, and a name the file already sits at wants no move.
    pub(crate) fn intend(&mut self, output_name: &str) {
        self.wants = (output_name != self.name).then(|| output_name.to_owned());
    }

    // The file moves to the name it wants, which the checkpoint has
    // recorded. The name it left, when it moved.
    pub(crate) fn move_on(&mut self, dir: &Path) -> Result<Option<String>, LeafError> {
        let Some(wants) = self.wants.clone() else {
            return Ok(None);
        };
        let path = dir.join(relative_path(&wants));
        let had_file = self.file.take().is_some() || self.path.exists();
        if had_file && path != self.path {
            create_parent(&path)?;
            crate::runtime::fs::rename_path_exclusive(&self.path, &path)
                .map_err(LeafError::io(&self.path))?;
        }
        self.path = path;
        self.wants = None;
        Ok(Some(std::mem::replace(&mut self.name, wants)))
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

// An output name as a path relative to the directory it is written under.
// Every name a leaf is given was confined by the name ledger
// (`weaver_graph::names`), which keeps it inside that directory: never
// empty, no NUL, no root, prefix or `..` part. That is asserted here, never
// judged: a part that would leave the directory is not joined, so even a
// name the ledger let through by mistake lands inside it.
fn relative_path(output_name: &str) -> PathBuf {
    let normalized = output_name.replace('\\', "/");
    let name = normalized.trim_end_matches('/');
    debug_assert!(
        !name.is_empty() && !name.contains('\0'),
        "the name ledger handed the leaf {output_name:?}"
    );
    let mut safe = PathBuf::new();
    for component in Path::new(name).components() {
        match component {
            Component::Normal(part) => safe.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                debug_assert!(
                    false,
                    "the name ledger let {output_name:?} leave its directory"
                );
            }
        }
    }
    safe
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

fn crc32(bytes: &[u8]) -> u32 {
    crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
}

fn index(len: u64) -> usize {
    usize::try_from(len).expect("a leaf range fits in memory")
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|at| (at as u8).wrapping_mul(31) ^ seed)
            .collect()
    }

    fn file_crc(leaf: &Leaf, length: u64) -> u32 {
        let mut all = Vec::new();
        leaf.read(0, index(length), &mut all).unwrap();
        crc32(&all)
    }

    // A write over bytes already written, a close that cuts the file short
    // and a restore each leave the fold answering for the file, so the
    // check at close never reads the member back.
    #[test]
    fn the_fold_answers_for_the_file_through_rewrites_cuts_and_restores() {
        let dir = tempfile::tempdir().unwrap();
        let mut leaf = Leaf::at(dir.path(), "lantern.bin");
        leaf.write(0, &bytes(4096, 1), None).unwrap();
        leaf.write(6000, &bytes(2192, 2), None).unwrap();
        // Over the end of one run, the gap and the start of the next.
        leaf.write(3000, &bytes(4000, 3), None).unwrap();
        // Inside one run, with other bytes, and with the same bytes.
        leaf.write(100, &bytes(50, 4), None).unwrap();
        leaf.write(100, &bytes(50, 4), None).unwrap();
        assert_eq!(leaf.folded(8192), Some(file_crc(&leaf, 8192)));
        leaf.close(7000).unwrap();
        assert_eq!(leaf.folded(7000), Some(file_crc(&leaf, 7000)));

        let mut restored = Leaf::at(dir.path(), "lantern.bin");
        assert_eq!(restored.folded(7000), None);
        restored.fold_back([(0, 3000), (3000, 7000)]);
        assert_eq!(restored.folded(7000), Some(file_crc(&leaf, 7000)));
        // A range the file does not hold stays out of the fold.
        let mut short = Leaf::at(dir.path(), "lantern.bin");
        short.fold_back([(0, 9000)]);
        assert_eq!(short.folded(7000), None);
    }
}
