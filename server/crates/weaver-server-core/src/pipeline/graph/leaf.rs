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

use weaver_graph::{Algorithm, Digest, Evidenced};

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
    // What was written, by contiguous run keyed by its start, with the
    // digest of the run of the kind the member's check states, folded as the
    // writes land so the member's own digest is known without reading the
    // file back.
    written: BTreeMap<u64, Written>,
    // The bytes that changed after they were written, by start and the
    // revision that changed them, to where they end: what a write replaced,
    // a close cut off or a start over emptied. A byte written for the first
    // time changes nothing anyone read.
    changed: BTreeMap<(u64, u64), u64>,
}

// How much of a file is read back at a time when its CRC is not in the fold.
const READ_BACK: u64 = 1 << 20;
static NEXT_LEAF: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

// A contiguous run of written bytes, and their digest when every write in
// the run was folded. A write of a member that states no check folds
// nothing, and a run it reaches has no digest.
#[derive(Clone, Copy, Debug)]
struct Written {
    end: u64,
    digest: Option<Digest>,
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
            changed: BTreeMap::new(),
        }
    }

    pub(crate) fn revision(&self) -> (u64, u64) {
        (self.identity, self.revision)
    }

    // Whether a byte of `start..end` changed since the leaf stood at `seen`,
    // or the leaf is another file than the one seen then.
    pub(crate) fn changed_since(&self, seen: (u64, u64), start: u64, end: u64) -> bool {
        seen.0 != self.identity
            || self
                .changed
                .range(..(end, 0))
                .any(|(&(from, revision), &to)| revision > seen.1 && to > start && from < end)
    }

    // `start..end` changed at this revision; what it changed before, within
    // it, is answered by it.
    fn change(&mut self, start: u64, end: u64) {
        let within: Vec<(u64, u64)> = self
            .changed
            .range((start, 0)..(end, 0))
            .filter(|(_, to)| **to <= end)
            .map(|(key, _)| *key)
            .collect();
        for key in within {
            self.changed.remove(&key);
        }
        self.changed.insert((start, self.revision), end);
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

    // Write `bytes` at `offset`, folding nothing: a file whose bytes no
    // check is stated for. The first write creates the file, marked
    // sparse, and sizes it to `length` when the length is known, so the
    // blocks are reserved in one step instead of growing write by write.
    pub(crate) fn write(
        &mut self,
        offset: u64,
        bytes: &[u8],
        length: Option<u64>,
    ) -> Result<(), LeafError> {
        self.put(offset, bytes, None, length)
    }

    // Write `bytes` at `offset` and fold their `algorithm` digest, from the
    // evidence they came with: only bytes no level above proved are hashed.
    pub(crate) fn write_folding(
        &mut self,
        offset: u64,
        bytes: &Evidenced,
        algorithm: Algorithm,
        length: Option<u64>,
    ) -> Result<(), LeafError> {
        self.put(offset, bytes, Some((algorithm, bytes)), length)
    }

    fn put(
        &mut self,
        offset: u64,
        bytes: &[u8],
        fold: Option<(Algorithm, &Evidenced)>,
        length: Option<u64>,
    ) -> Result<(), LeafError> {
        self.revision += 1;
        self.open(length)?;
        let span = (offset, offset + bytes.len() as u64);
        let overlapped = self.overlapped(span);
        for (from, to) in &overlapped {
            self.change(*from, *to);
        }
        let replaced = self.replaced(&overlapped, fold.map(|(algorithm, _)| algorithm))?;
        let file = self.file.as_ref().expect("opened above");
        write_all_at(file, offset, bytes).map_err(LeafError::io(&self.path))?;
        self.wrote(span, fold, &overlapped, &replaced);
        Ok(())
    }

    // Where a write over `offset..end` lands on runs already written, in
    // order.
    fn overlapped(&self, (offset, end): (u64, u64)) -> Vec<(u64, u64)> {
        let mut overlapped: Vec<(u64, u64)> = self
            .written
            .range(..end)
            .rev()
            .take_while(|(_, run)| run.end > offset)
            .map(|(start, run)| ((*start).max(offset), run.end.min(end)))
            .collect();
        overlapped.reverse();
        overlapped
    }

    // The digest of what a folded write replaces in each folded run it
    // overlaps, read back: what the fold needs to keep those runs, and no
    // more than the write itself puts down. A run without a digest, or a
    // write that folds nothing, reads nothing back.
    fn replaced(
        &self,
        overlapped: &[(u64, u64)],
        fold: Option<Algorithm>,
    ) -> Result<Vec<Option<u64>>, LeafError> {
        let mut replaced = Vec::with_capacity(overlapped.len());
        for (from, to) in overlapped {
            let (_, run) = self
                .written
                .range(..=*from)
                .next_back()
                .expect("an overlapped range lies in a run");
            let digest = match (fold, run.digest) {
                (Some(algorithm), Some(digest)) if digest.algorithm == algorithm => {
                    let mut old = Vec::new();
                    self.read(*from, index(to - from), &mut old)?;
                    Some(algorithm.hash(&old).value)
                }
                _ => None,
            };
            replaced.push(digest);
        }
        Ok(replaced)
    }

    // Fold a write into the runs. A folded run the write overlaps keeps its
    // span: the bytes it replaced give way to the bytes that replace them,
    // carried past the run's bytes behind them. A run a write that cannot
    // fold reaches loses its digest. What of the write no run held is a run
    // of its own, and runs that now abut are joined.
    fn wrote(
        &mut self,
        (offset, end): (u64, u64),
        fold: Option<(Algorithm, &Evidenced)>,
        overlapped: &[(u64, u64)],
        replaced: &[Option<u64>],
    ) {
        if offset == end {
            return;
        }
        let digest_of = |from: u64, to: u64| {
            fold.map(|(algorithm, bytes)| {
                bytes
                    .slice(index(from - offset)..index(to - offset))
                    .digest(algorithm)
            })
        };
        let mut at = offset;
        for ((from, to), old) in overlapped.iter().zip(replaced) {
            if at < *from {
                let digest = digest_of(at, *from);
                self.written.insert(at, Written { end: *from, digest });
            }
            let new = digest_of(*from, *to);
            let (_, run) = self
                .written
                .range_mut(..=*from)
                .next_back()
                .expect("an overlapped range lies in a run");
            let behind = run.end - to;
            run.digest = match (run.digest, old, new) {
                (Some(whole), Some(old), Some(new)) => whole
                    .algorithm
                    .replace(whole.value, *old, new.value, behind)
                    .map(|value| Digest {
                        algorithm: whole.algorithm,
                        value,
                    }),
                _ => None,
            };
            at = *to;
        }
        if at < end {
            let digest = digest_of(at, end);
            self.written.insert(at, Written { end, digest });
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
                        digest: joined_digest(prior.digest, run.digest, run.end - start),
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
        if let Some((_, last)) = self.written.iter().next_back()
            && last.end > length
        {
            self.change(length, last.end);
        }
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
        let digest = match run.digest {
            Some(whole) => {
                let mut lost = Vec::new();
                self.read(length, index(run.end - length), &mut lost)?;
                let algorithm = whole.algorithm;
                let lost = algorithm.hash(&lost).value;
                algorithm
                    .head_of(whole.value, lost, run.end - length)
                    .map(|value| Digest { algorithm, value })
            }
            None => None,
        };
        self.written.insert(
            start,
            Written {
                end: length,
                digest,
            },
        );
        Ok(())
    }

    // A restored member's fold, which a restore starts empty. A member
    // whose bytes passed its stated CRC folds to that CRC; any other folds
    // what of `ranges` its file holds, read back from it. A range the file
    // does not hold is left out of the fold: a file lost, or cut short,
    // since its bytes passed holds no bytes that passed, and folds nothing.
    pub(crate) fn fold_passed(&mut self, length: u64, digest: Digest) {
        self.written.clear();
        if length > 0 && self.len().is_ok_and(|held| held >= length) {
            self.written.insert(
                0,
                Written {
                    end: length,
                    digest: Some(digest),
                },
            );
        }
    }

    pub(crate) fn fold_back(
        &mut self,
        algorithm: Algorithm,
        ranges: impl IntoIterator<Item = (u64, u64)>,
    ) {
        for (start, end) in ranges {
            let mut at = start;
            while at < end {
                let take = (end - at).min(READ_BACK);
                let mut chunk = Vec::new();
                if self.read(at, index(take), &mut chunk).is_err() {
                    break;
                }
                let chunk = Evidenced::from(chunk);
                self.wrote((at, at + take), Some((algorithm, &chunk)), &[], &[]);
                at += take;
            }
        }
    }

    // The `algorithm` digest of the member's `length` bytes as the fold
    // holds them, when one folded run covers exactly them.
    pub(crate) fn folded(&self, length: u64, algorithm: Algorithm) -> Option<u64> {
        if length == 0 {
            return Some(algorithm.empty().value);
        }
        match self.written.iter().next() {
            Some((0, run)) if self.written.len() == 1 && run.end == length => run
                .digest
                .filter(|digest| digest.algorithm == algorithm)
                .map(|digest| digest.value),
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
        self.change(0, u64::MAX);
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

fn index(len: u64) -> usize {
    usize::try_from(len).expect("a leaf range fits in memory")
}

// The digest of two abutting runs, when both were folded of one kind.
fn joined_digest(first: Option<Digest>, second: Option<Digest>, len_second: u64) -> Option<Digest> {
    let (first, second) = (first?, second?);
    let algorithm = first.algorithm;
    if second.algorithm != algorithm {
        return None;
    }
    let value = algorithm.join(first.value, second.value, len_second)?;
    Some(Digest { algorithm, value })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|at| (at as u8).wrapping_mul(31) ^ seed)
            .collect()
    }

    fn file_crc(leaf: &Leaf, length: u64) -> u64 {
        let mut all = Vec::new();
        leaf.read(0, index(length), &mut all).unwrap();
        Algorithm::Crc32.hash(&all).value
    }

    fn folding(leaf: &mut Leaf, offset: u64, bytes: Vec<u8>) {
        let bytes = Evidenced::from(bytes);
        leaf.write_folding(offset, &bytes, Algorithm::Crc32, None)
            .unwrap();
    }

    // A write over bytes already written, a close that cuts the file short
    // and a restore each leave the fold answering for the file, so the
    // check at close never reads the member back.
    #[test]
    fn the_fold_answers_for_the_file_through_rewrites_cuts_and_restores() {
        let dir = tempfile::tempdir().unwrap();
        let mut leaf = Leaf::at(dir.path(), "lantern.bin");
        folding(&mut leaf, 0, bytes(4096, 1));
        folding(&mut leaf, 6000, bytes(2192, 2));
        // Over the end of one run, the gap and the start of the next.
        folding(&mut leaf, 3000, bytes(4000, 3));
        // Inside one run, with other bytes, and with the same bytes.
        folding(&mut leaf, 100, bytes(50, 4));
        folding(&mut leaf, 100, bytes(50, 4));
        let crc = Algorithm::Crc32;
        assert_eq!(leaf.folded(8192, crc), Some(file_crc(&leaf, 8192)));
        leaf.close(7000).unwrap();
        assert_eq!(leaf.folded(7000, crc), Some(file_crc(&leaf, 7000)));

        let mut restored = Leaf::at(dir.path(), "lantern.bin");
        assert_eq!(restored.folded(7000, crc), None);
        restored.fold_back(crc, [(0, 3000), (3000, 7000)]);
        assert_eq!(restored.folded(7000, crc), Some(file_crc(&leaf, 7000)));
        // A range the file does not hold stays out of the fold.
        let mut short = Leaf::at(dir.path(), "lantern.bin");
        short.fold_back(crc, [(0, 9000)]);
        assert_eq!(short.folded(7000, crc), None);
    }
}
