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
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use weaver_graph::{Algorithm, ByteRange, Digest, Evidenced};

use crate::pipeline::graph::limits::Device;
use weaver_server_disk::operations::disk::volume::LoopSite;
use weaver_server_disk::operations::disk::writer::{Barrier, Create, Opened, Submitter};
use weaver_server_disk::pipeline::graph::sparse::{
    PlatformMarker, create_sized, create_sized_known, create_sparse,
};

#[derive(Debug, thiserror::Error)]
pub enum LeafError {
    #[error("{}: {source}", path.display())]
    Io {
        path: PathBuf,
        source: io::Error,
        // The device the file lies on: the output directory's for a file
        // the job delivers, else the working directory's.
        device: Device,
    },
}

impl LeafError {
    fn on(path: &Path, device: Device) -> impl FnOnce(io::Error) -> LeafError + '_ {
        move |source| LeafError::Io {
            path: path.to_path_buf(),
            source,
            device,
        }
    }

    // Whether the file system refused the name itself: too long for it, or
    // one it never takes. A full disk, a permission or a read-only volume
    // is another error.
    fn into_io(self) -> io::Error {
        match self {
            LeafError::Io { source, .. } => source,
        }
    }

    pub fn names_unusable(&self) -> bool {
        match self {
            LeafError::Io { source, .. } => source.kind() == io::ErrorKind::InvalidFilename,
        }
    }
}

// A file as the file system tells files apart: its device and its number
// there. Every name of one file gives the same; a file made anew in its
// place gives another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileId {
    device: u64,
    number: u64,
}

impl FileId {
    #[cfg(unix)]
    fn of(metadata: &std::fs::Metadata) -> Option<Self> {
        use std::os::unix::fs::MetadataExt;
        Some(Self {
            device: metadata.dev(),
            number: metadata.ino(),
        })
    }

    // Without a stable way to read a file's number, no two files are said
    // to be one, and every byte is written.
    #[cfg(not(unix))]
    fn of(_: &std::fs::Metadata) -> Option<Self> {
        None
    }
}

#[derive(Debug)]
pub struct Leaf {
    // Runtime identity and content revision for in-flight readers. Neither
    // survives a restart, when no worker from this process can return.
    identity: u64,
    revision: u64,
    // The directory the leaf's names join under: the job's output root for
    // a member it delivers, its working directory for one it keeps.
    root: PathBuf,
    // The device that root lies on, for the error a write there ends in.
    device: Device,
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
    // Whether the file is on disk: the leaf made it, or was handed one
    // that sits at its name. Never read off the disk, where the name may
    // already hold a newer file another leaf was given.
    on_disk: bool,
    // What was written, by contiguous run keyed by its start, with the
    // digest of the run of the kind the member's check states, folded as the
    // writes land so the member's own digest is known without reading the
    // file back.
    pub written: BTreeMap<u64, Written>,
    // The checked stamp was applied (`fold_checked`): a digest it gave that
    // a later write took away is not given again without a check. Not kept;
    // a start over (`truncate`) takes it back, and a retraction drops the
    // whole leaf.
    stamped: bool,
    // The bytes that changed after they were written, by start, to the
    // latest revision that changed them and where they end: what a write
    // replaced, a close cut off or a start over emptied. The stretches are
    // disjoint. A byte written for the first time changes nothing anyone
    // read. Only a repair's check of what it read asks (`changed_since`),
    // so a change is kept only while one may still be checked: its caller
    // says so (`record`), and the namespace writer empties the map once no
    // repair's read is left to check. A repair lent at revision R counts
    // only changes past R, and every one of those is made while its read
    // may be checked, so each is here when it asks.
    changed: BTreeMap<u64, (u64, u64)>,
    // The volume writer the file's operations go to, when the leaf does not
    // run them itself. Everything the leaf records of its file is as it
    // will be once what it submitted has returned; whoever needs the disk
    // to agree waits for `settled` first.
    queued: Option<Queued>,
}

#[derive(Debug)]
pub struct Queued {
    to: Arc<Submitter>,
    // The leaf's identity, which names its file to the writer.
    file: u64,
    // Whether the writer holds the file open, as far as the leaf knows.
    open: bool,
    // The last operation submitted for the file.
    last: u64,
    // The release that gave the file's handle back, while it is still the
    // last operation submitted for the file: the writer holds no handle
    // then, and another release would have nothing to give back.
    released: u64,
    // The loop's own handle on the file, for its reads while the writer
    // holds the file open: one open for every read of the file then, as a
    // leaf that writes the file itself reads through its handle. Given up
    // before any operation that closes, cuts, moves or removes the file is
    // submitted, so no operation of the writer finds it held.
    reader: OnceLock<File>,
    // The last operation submitted for the file that is not a write of
    // bytes into it as it stands: the write that made or opened it, a cut,
    // a close, a start over, a move. Every read of the file waits for it.
    shaped: u64,
    // The writes submitted since, that may still be out, keyed by where
    // each starts and its number, to where it ends, and the same in the
    // order they were submitted, so those that returned are let go. A read
    // waits beside `shaped` only for the writes it overlaps: bytes written
    // long ago are not waited for again behind the file's newest write.
    out: BTreeMap<(u64, u64), u64>,
    sent: std::collections::VecDeque<(u64, u64)>,
    // The longest write in `out`, which bounds how far before a read one
    // that overlaps it can start.
    longest: u64,
    // The last close of a file already made, and the length it gave it: it
    // changes no byte below that length, so only a read that reaches past
    // it waits for the close.
    closed: Option<(u64, u64)>,
}

impl Queued {
    pub fn new(to: &Arc<Submitter>, file: u64) -> Self {
        Self {
            to: Arc::clone(to),
            file,
            open: false,
            last: 0,
            released: 0,
            reader: OnceLock::new(),
            shaped: 0,
            out: BTreeMap::new(),
            sent: std::collections::VecDeque::new(),
            longest: 0,
            closed: None,
        }
    }

    // The loop's handle goes before the writer is asked to close, cut,
    // move or remove the file.
    fn let_go(&mut self) {
        self.reader.take();
    }

    // `seq` changes the file as a whole: every read after waits for it, so
    // no write before it needs to be kept apart.
    pub fn shape(&mut self, seq: u64) {
        self.last = seq;
        self.shaped = seq;
        self.out.clear();
        self.sent.clear();
        self.longest = 0;
        self.closed = None;
    }

    // `seq` closes the file, already made, at `length`.
    pub fn close(&mut self, seq: u64, length: u64) {
        self.last = seq;
        self.closed = Some(match self.closed {
            Some((_, before)) => (seq, before.min(length)),
            None => (seq, length),
        });
    }

    // `seq` writes `offset..end` of the file as it stands. The writes before
    // it that returned are let go first: the oldest is looked at, one look
    // for each write, so the work stays constant per write.
    pub fn wrote(&mut self, seq: u64, offset: u64, end: u64) {
        self.last = seq;
        while let Some(&(oldest, start)) = self.sent.front() {
            if !self.to.barrier_at(oldest).returned() {
                break;
            }
            self.sent.pop_front();
            self.out.remove(&(start, oldest));
        }
        if self.out.is_empty() {
            self.longest = 0;
        }
        if end == offset {
            return;
        }
        self.out.insert((offset, seq), end);
        self.sent.push_back((seq, offset));
        self.longest = self.longest.max(end - offset);
    }

    // The operation a read of `offset..end` waits for: the last that shaped
    // the file, a close that cut short of the range's end, or the last write
    // still kept that overlaps the range.
    pub fn read_after(&self, offset: u64, end: u64) -> u64 {
        let closed = self
            .closed
            .filter(|(_, length)| end > *length)
            .map_or(0, |(seq, _)| seq);
        let from = (offset.saturating_sub(self.longest), 0);
        self.out
            .range(from..(end, 0))
            .filter(|(_, written_to)| **written_to > offset)
            .map(|((_, seq), _)| *seq)
            .fold(self.shaped.max(closed), u64::max)
    }
}

impl Drop for Queued {
    // A leaf that goes gives its handle back, as dropping its own file
    // would.
    fn drop(&mut self) {
        self.let_go();
        if self.last > 0 {
            self.to.release(self.file);
        }
    }
}

thread_local! {
    // The buffer the bytes now being placed are slices of, so a write
    // handed to a volume's writer shares it instead of copying.
    static LENT: std::cell::RefCell<Option<Bytes>> = const { std::cell::RefCell::new(None) };
}

// While it lives, writes of slices of `bytes` on this thread share it.
pub(crate) struct Lent(Option<Bytes>);

pub(crate) fn lend(bytes: &Bytes) -> Lent {
    Lent(LENT.with(|lent| lent.borrow_mut().replace(bytes.clone())))
}

impl Drop for Lent {
    fn drop(&mut self) {
        LENT.with(|lent| *lent.borrow_mut() = self.0.take());
    }
}

// `bytes` as owned bytes: a share of the lent buffer when they lie in it,
// else a copy.
fn owned(bytes: &[u8]) -> Bytes {
    LENT.with(|lent| {
        let lent = lent.borrow();
        match lent.as_ref() {
            Some(whole)
                if !bytes.is_empty()
                    && whole.as_ptr_range().start <= bytes.as_ptr_range().start
                    && bytes.as_ptr_range().end <= whole.as_ptr_range().end =>
            {
                whole.slice_ref(bytes)
            }
            _ => Bytes::copy_from_slice(bytes),
        }
    })
}

// Open the file at `path` for a volume's writer, as a leaf opens its own,
// with the length it was left at when that is known.
fn open_sized(path: &Path, length: Option<u64>, _: Option<&Path>) -> io::Result<Opened> {
    let began = std::time::Instant::now();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let folders = began.elapsed();
    let mut opened = create_sized_known(path, &PlatformMarker, length)?;
    opened.steps.create = opened.steps.create.map(|create| create + folders);
    Ok(opened)
}

static NEXT_LEAF: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

// A contiguous run of written bytes, and their digest when every write in
// the run was folded. A write of a member that states no check folds
// nothing, and a run it reaches has no digest.
#[derive(Clone, Copy, Debug)]
pub struct Written {
    pub end: u64,
    pub digest: Option<Digest>,
}

impl Leaf {
    // Where the leaf for `output_name` under `dir` lies.
    pub(crate) fn path_at(dir: &Path, output_name: &str) -> PathBuf {
        dir.join(relative_path(output_name))
    }

    // The leaf for `output_name` under `dir`. Nothing is created until the
    // first byte arrives.
    pub fn at(dir: &Path, output_name: &str) -> Self {
        Self::over(dir, output_name, false)
    }

    // The leaf a restore rebuilds over what its checkpoint left at
    // `output_name`. Whether the file is there is read once, here: nothing
    // has been given a name since the restore began, so what sits at the
    // name is the leaf's own file.
    pub fn found(dir: &Path, output_name: &str) -> Result<Self, LeafError> {
        let mut leaf = Self::at(dir, output_name);
        leaf.on_disk = leaf
            .path
            .try_exists()
            .map_err(LeafError::on(&leaf.path, leaf.device))?;
        Ok(leaf)
    }

    // The leaf for a file at `output_name` under `dir` that another leaf
    // hands it, with `on_disk` whether that file is there, as that leaf
    // knows.
    pub fn over(dir: &Path, output_name: &str, on_disk: bool) -> Self {
        Self {
            identity: NEXT_LEAF.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            revision: 0,
            root: dir.to_path_buf(),
            device: Device::Working,
            path: Self::path_at(dir, output_name),
            name: output_name.to_owned(),
            wants: None,
            directory: Self::is_directory(output_name),
            file: None,
            on_disk,
            written: BTreeMap::new(),
            stamped: false,
            changed: BTreeMap::new(),
            queued: None,
        }
    }

    // The leaf for a file at `output_name` under `dir` that another leaf,
    // through the writer `to`, hands it while that leaf may still have
    // operations queued for the file. The writer tells files apart by the
    // leaf that names them, and within one batch runs one leaf's operations
    // before or after another's as their files' closes and opens say, so
    // the first leaf's truncate or writes still queued could run after this
    // one's writes and close and cut the file back. An operation that names
    // no file stands between them: the writer runs it after everything
    // submitted before it and before everything after, and every read of
    // the file waits for it. `made`, when not zero, is such an operation
    // already submitted, one that puts the file at its name; otherwise one
    // that does nothing is submitted here.
    pub fn handed(
        dir: &Path,
        output_name: &str,
        on_disk: bool,
        to: Option<&Arc<Submitter>>,
        made: u64,
    ) -> Self {
        let leaf = Self::over(dir, output_name, on_disk).through(to);
        let Some(to) = to else {
            return leaf;
        };
        let fence = match made {
            0 => to.run(None, leaf.path.clone(), || Ok(())),
            made => made,
        };
        leaf.after(fence)
    }

    // The leaf's file operations go to a volume's writer from here on.
    pub fn through(mut self, to: Option<&Arc<Submitter>>) -> Self {
        self.queued = to.map(|to| Queued::new(to, self.identity));
        self
    }

    // The file is put at the leaf's path by the operation numbered `made`,
    // which its writer already has: whoever reads it waits through that
    // one. Nothing for a leaf that runs its own operations.
    pub(crate) fn after(mut self, made: u64) -> Self {
        if let Some(queued) = &mut self.queued {
            let made = queued.last.max(made);
            queued.shape(made);
        }
        self
    }

    // Everything submitted for the file so far: whoever reads the file by
    // its path waits for this first. `None` for a leaf that runs its own
    // operations, whose file is always as the leaf says.
    pub fn settled(&self) -> Option<Barrier> {
        self.queued
            .as_ref()
            .filter(|queued| queued.last > 0)
            .map(|queued| queued.to.barrier_at(queued.last))
    }

    // The operations a read of `offset..end` waits for (`read`) while some
    // of them are still out, asked without waiting: `None` when the read
    // would not wait.
    pub(crate) fn read_out(&self, offset: u64, end: u64) -> Option<Barrier> {
        let queued = self.queued.as_ref().filter(|queued| queued.last > 0)?;
        let barrier = queued.to.barrier_at(queued.read_after(offset, end));
        (!barrier.returned()).then_some(barrier)
    }

    // The volume the file's writer writes to, whose read turns a reader of
    // the file takes. `None` for a leaf that runs its own operations.
    pub(crate) fn volume(
        &self,
    ) -> Option<&Arc<weaver_server_disk::operations::disk::volume::Volume>> {
        self.queued.as_ref().map(|queued| queued.to.volume())
    }

    fn create(&self, length: Option<u64>) -> Create {
        Create {
            path: self.path.clone(),
            length,
            open: open_sized,
            within: None,
        }
    }

    // The directory the leaf's name is under.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    // The leaf lies on `device`: said by the writer that placed it.
    pub(crate) fn on(mut self, device: Device) -> Self {
        self.device = device;
        self
    }

    pub fn revision(&self) -> (u64, u64) {
        (self.identity, self.revision)
    }

    // Whether a byte of `start..end` changed since the leaf stood at `seen`,
    // or the leaf is another file than the one seen then. The map holds, for
    // every byte, the latest revision that changed it, so a range changed
    // since `seen` exactly when one of the stretches it spans did. Asked
    // only for a read that may still be checked (`checked`), whose changes
    // the map kept.
    pub(crate) fn changed_since(
        &self,
        seen: (u64, u64),
        start: u64,
        end: u64,
        checked: bool,
    ) -> bool {
        debug_assert!(checked, "a change asked of a read no longer checked");
        seen.0 != self.identity
            || self
                .spanned(start, end)
                .any(|(from, (revision, to))| revision > seen.1 && to > start && from < end)
    }

    // The stretches of `changed` that `start..end` may reach: the one at or
    // before its start, which may end before it, and every one starting
    // inside it. They are disjoint, so no stretch before those reaches it.
    fn spanned(&self, start: u64, end: u64) -> impl Iterator<Item = (u64, (u64, u64))> + '_ {
        let from = self
            .changed
            .range(..=start)
            .next_back()
            .map_or(start, |(from, _)| *from);
        self.changed
            .range(from..end.max(from))
            .map(|(from, stretch)| (*from, *stretch))
    }

    // `start..end` changed at this revision, which is the latest: every
    // stretch it overlaps keeps only what lies outside it, at the revision
    // that stretch had, and it is joined to a neighbour it abuts that
    // changed at this revision too. Nothing is kept unless `record`: no
    // repair's read is left to check against it.
    fn change(&mut self, record: bool, mut start: u64, mut end: u64) {
        if !record || start >= end {
            return;
        }
        let overlapped: Vec<(u64, (u64, u64))> = self
            .spanned(start, end)
            .filter(|(_, (_, to))| *to > start)
            .collect();
        for (from, (revision, to)) in overlapped {
            self.changed.remove(&from);
            if from < start {
                self.changed.insert(from, (revision, start));
            }
            if to > end {
                self.changed.insert(end, (revision, to));
            }
        }
        if let Some((&from, &(revision, to))) = self.changed.range(..start).next_back()
            && to == start
            && revision == self.revision
        {
            self.changed.remove(&from);
            start = from;
        }
        if let Some(&(revision, to)) = self.changed.get(&end)
            && revision == self.revision
        {
            self.changed.remove(&end);
            end = to;
        }
        self.changed.insert(start, (self.revision, end));
    }

    // No repair's read is left to check: what the leaf kept for one goes.
    pub(crate) fn forget_changes(&mut self) {
        self.changed.clear();
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    // The name the file sits at.
    pub fn name(&self) -> &str {
        &self.name
    }

    // The name the file waits to move to, if it waits.
    pub fn wants(&self) -> Option<&str> {
        self.wants.as_deref()
    }

    // Whether the file is on disk: nothing is created until the first byte
    // arrives.
    pub fn on_disk(&self) -> bool {
        self.on_disk
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn is_open(&self) -> bool {
        // A handle the volume's writer holds for the leaf counts.
        self.file.is_some() || self.queued.as_ref().is_some_and(|queued| queued.open)
    }

    // A directory member is a name ending in a separator. It has no bytes,
    // so it exists the moment it is announced.
    pub(crate) fn is_directory(output_name: &str) -> bool {
        output_name.ends_with(['/', '\\'])
    }

    pub(crate) fn create_directory(&mut self) -> Result<(), LeafError> {
        if let Some(queued) = &mut self.queued {
            let path = self.path.clone();
            let made = queued
                .to
                .run(None, path.clone(), move || std::fs::create_dir_all(&path));
            queued.shape(made);
            self.on_disk = true;
            return Ok(());
        }
        std::fs::create_dir_all(&self.path).map_err(LeafError::on(&self.path, self.device))?;
        self.on_disk = true;
        Ok(())
    }

    // The bytes the leaf holds written, summed over its runs: asked only
    // when a write failed, never per write.
    pub(crate) fn written_bytes(&self) -> u64 {
        self.written.iter().fold(0u64, |sum, (start, run)| {
            sum.saturating_add(run.end - start)
        })
    }

    // The file already holds `len` bytes at `offset` that a write of them
    // would put there again: it is recorded as that write, folding nothing,
    // and nothing goes to disk. `record`: a repair's read may still be
    // checked against what it replaced (`changed`).
    pub(crate) fn wrote_in_place(&mut self, offset: u64, len: u64, record: bool) {
        self.revision += 1;
        let span = (offset, offset + len);
        let overlapped = self.overlapped(span);
        for (from, to) in &overlapped {
            self.change(record, *from, *to);
        }
        let replaced = vec![None; overlapped.len()];
        self.wrote(span, None, &overlapped, &replaced);
    }

    // The file the leaf reads from now, as the file system tells files
    // apart, once every operation that shaped it is down: a second name of
    // a file is that file, and an empty file made where no second name
    // could be is another. `None` where there is no file or the file system
    // cannot say.
    pub fn file_id(&self, site: LoopSite) -> Option<FileId> {
        if let Some(queued) = self.queued.as_ref().filter(|queued| queued.last > 0) {
            queued.to.wait_for(site, queued.read_after(0, 0)).ok()?;
            if let Some(file) = queued.reader.get().filter(|_| queued.open) {
                return FileId::of(&file.metadata().ok()?);
            }
        } else if let Some(file) = &self.file {
            return FileId::of(&file.metadata().ok()?);
        }
        FileId::of(&std::fs::metadata(&self.path).ok()?)
    }

    // Write `bytes` at `offset`, folding nothing: a file whose bytes no
    // check is stated for. A new file is made at its declared length, all
    // of it holes, where its filesystem is known to make holes
    // (`sparse::create_sized` has the rule); anywhere else writes grow the
    // file only as far as their bytes, so a declared length never takes
    // unwritten space. Close sets the final size either way. `record`: a
    // repair's read may still be checked against what it replaces
    // (`changed`).
    pub fn write(
        &mut self,
        site: LoopSite,
        offset: u64,
        bytes: &[u8],
        length: Option<u64>,
        record: bool,
    ) -> Result<(), LeafError> {
        self.put(site, offset, bytes, None, length, record)
    }

    // Write `bytes` at `offset` and fold their `algorithm` digest, from the
    // evidence they came with: only bytes no level above proved are hashed.
    pub fn write_folding(
        &mut self,
        site: LoopSite,
        offset: u64,
        bytes: &Evidenced,
        algorithm: Algorithm,
        length: Option<u64>,
        record: bool,
    ) -> Result<(), LeafError> {
        self.put(
            site,
            offset,
            bytes,
            Some((algorithm, bytes)),
            length,
            record,
        )
    }

    // `site` is where the write was made from: what its waits count under.
    fn put(
        &mut self,
        site: LoopSite,
        offset: u64,
        bytes: &[u8],
        fold: Option<(Algorithm, &Evidenced)>,
        length: Option<u64>,
        record: bool,
    ) -> Result<(), LeafError> {
        self.revision += 1;
        if self.queued.is_none() {
            self.open(length)?;
        }
        let span = (offset, offset + bytes.len() as u64);
        let overlapped = self.overlapped(span);
        for (from, to) in &overlapped {
            self.change(record, *from, *to);
        }
        let replaced = self.replaced(site, &overlapped, fold.map(|(algorithm, _)| algorithm))?;
        if let Some(open) = self.queued.as_ref().map(|queued| queued.open) {
            // A test's full disk refuses the write as it is handed over, as
            // the disk would refuse it in place.
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            if counting::full(bytes.len()) {
                return Err(LeafError::on(&self.path, self.device)(io::Error::from(
                    counting::refusal(),
                )));
            }
            let create = (!open).then(|| self.create(length));
            let queued = self.queued.as_mut().expect("checked above");
            queued.open = true;
            let made = create.is_some();
            let seq = queued
                .to
                .write(site, self.identity, offset, owned(bytes), create);
            // A write that opens the file makes it, or opens it again at a
            // declared length: every read waits for it.
            if made {
                queued.shape(seq);
            } else {
                queued.wrote(seq, span.0, span.1);
            }
            self.on_disk = true;
        } else {
            let file = self.file.as_ref().expect("opened above");
            write_all_at(file, offset, bytes).map_err(LeafError::on(&self.path, self.device))?;
        }
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
        site: LoopSite,
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
                    self.read_back(site, *from, index(to - from), &mut old)?;
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
    fn cut(&mut self, site: LoopSite, length: u64, record: bool) -> Result<(), LeafError> {
        if let Some((_, last)) = self.written.iter().next_back()
            && last.end > length
        {
            self.change(record, length, last.end);
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
                self.read_back(site, length, index(run.end - length), &mut lost)?;
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
    // the runs something else vouches for, each with its digest, joined
    // where they abut, and nothing is read. A file lost, or cut short,
    // since its bytes passed holds no bytes that passed past its length, and
    // folds nothing there. Neither waits for the disk: a restored leaf has
    // nothing submitted, and one that has something still out folds nothing.
    pub fn fold_passed(&mut self, length: u64, digest: Digest) {
        self.written.clear();
        if length > 0 && self.len_settled().is_some_and(|held| held >= length) {
            self.written.insert(
                0,
                Written {
                    end: length,
                    digest: Some(digest),
                },
            );
        }
    }

    pub fn fold_vouched(&mut self, runs: impl IntoIterator<Item = (ByteRange, Digest)>) {
        let held = self.len_settled().unwrap_or(0);
        for (range, digest) in runs {
            if range.is_empty()
                || range.end > held
                || !self.overlapped((range.start, range.end)).is_empty()
            {
                continue;
            }
            self.written.insert(
                range.start,
                Written {
                    end: range.end,
                    digest: Some(digest),
                },
            );
            self.join(range.start, range.end);
        }
    }

    // The fold of a member its check read back whole, `length` bytes that
    // hashed to `digest`: the read proved the file holds them, so the disk
    // is not asked again.
    pub fn fold_read(&mut self, length: u64, digest: Digest) {
        self.written.clear();
        if length > 0 {
            self.written.insert(
                0,
                Written {
                    end: length,
                    digest: Some(digest),
                },
            );
        }
    }

    // The fold of a member whose producer checked the `length` bytes it
    // wrote against `digest` as it decoded them: the one run written over
    // exactly them, which folded nothing, takes the digest, and nothing is
    // hashed or read. A write that lands on the run after this takes the
    // digest away again, as from any folded run it cannot fold, and it is
    // given once: what a later write took away is checked, not stamped.
    pub fn fold_checked(&mut self, length: u64, digest: Digest) {
        if self.stamped || length == 0 || self.written.len() != 1 {
            return;
        }
        if let Some(run) = self.written.get_mut(&0)
            && run.end == length
            && run.digest.is_none()
        {
            run.digest = Some(digest);
            self.stamped = true;
        }
    }

    // The `algorithm` digest of the member's `length` bytes as the fold
    // holds them, when one folded run covers exactly them.
    pub fn folded(&self, length: u64, algorithm: Algorithm) -> Option<u64> {
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
        debug_assert!(
            self.queued.is_none(),
            "a queued leaf is made by its first write"
        );
        if self.file.is_none() {
            let file = create_sparse(&self.path, &PlatformMarker)
                .map_err(LeafError::on(&self.path, self.device))?;
            self.file = Some(file);
            self.on_disk = true;
        }
        Ok(())
    }

    // How far the file reaches: every byte written to it lies below.
    pub fn len(&self, site: LoopSite) -> Result<u64, LeafError> {
        if let Some(settled) = self.settled() {
            settled.wait(site);
            // A write that failed, or one still out when a stop gave the
            // volume up, left the file short of what was sent to it: its
            // length is not the length of what the leaf holds.
            if settled.unwritten() {
                return Err(LeafError::Io {
                    device: self.device,
                    path: self.path.clone(),
                    source: io::Error::other(
                        "a write of the file failed or is still out, so its length on disk is not its own",
                    ),
                });
            }
        }
        match &self.file {
            Some(file) => file.metadata(),
            None => std::fs::metadata(&self.path),
        }
        .map(|metadata| metadata.len())
        .map_err(LeafError::on(&self.path, self.device))
    }

    // How far the file reaches, without waiting: `None` while something
    // submitted for it is still out or did not land, or the OS gave no
    // length.
    fn len_settled(&self) -> Option<u64> {
        if self.settled().is_some_and(|settled| settled.unwritten()) {
            return None;
        }
        match &self.file {
            Some(file) => file.metadata(),
            None => std::fs::metadata(&self.path),
        }
        .ok()
        .map(|metadata| metadata.len())
    }

    // Fill `into` with `len` bytes from `offset`. The caller only asks for
    // ranges its coverage says were written.
    pub fn read(
        &self,
        site: LoopSite,
        offset: u64,
        len: usize,
        into: &mut Vec<u8>,
    ) -> Result<(), LeafError> {
        // Through a writer, the read waits only for this file's operations
        // that put the bytes read there: the last that shaped the file, and
        // the writes still out that overlap them, whatever else its volume
        // has queued. A read of bytes written long ago does not stand behind
        // a write the loop just made elsewhere in the file, so a repair's
        // slices, a replay and a delivery said again, which read one piece
        // and write the one before it, never wait on their own writes.
        if let Some(queued) = self.queued.as_ref().filter(|queued| queued.last > 0) {
            let end = offset.saturating_add(len as u64);
            queued
                .to
                .wait_for(site, queued.read_after(offset, end))
                .map_err(LeafError::on(&self.path, self.device))?;
            let began = Instant::now();
            let start = into.len();
            into.resize(start + len, 0);
            let read = queued_read(queued, &self.path, offset, &mut into[start..]);
            queued.to.loop_read(began);
            return read.map_err(|source| {
                into.truncate(start);
                LeafError::Io {
                    device: self.device,
                    path: self.path.clone(),
                    source,
                }
            });
        }
        self.read_here(offset, len, into)
    }

    // Whether the loop holds a handle of its own on the writer's file.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn holds_reader(&self) -> bool {
        self.queued
            .as_ref()
            .is_some_and(|queued| queued.reader.get().is_some())
    }

    // The same read for a thread the loop does not wait for: a repair or a
    // replay on a thread of its own.
    pub(crate) fn read_aside(
        &self,
        offset: u64,
        len: usize,
        into: &mut Vec<u8>,
    ) -> Result<(), LeafError> {
        if let Some(settled) = self.settled() {
            settled.wait_aside();
            if settled.unwritten() {
                return Err(LeafError::Io {
                    device: self.device,
                    path: self.path.clone(),
                    source: io::Error::other(
                        "the writes of the file are not down for it to be read",
                    ),
                });
            }
        }
        self.read_here(offset, len, into)
    }

    fn read_here(&self, offset: u64, len: usize, into: &mut Vec<u8>) -> Result<(), LeafError> {
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
                device: self.device,
                path: self.path.clone(),
                source,
            }
        })
    }

    // The bytes a write replaces or a close cuts off. Through a writer the
    // read waits through the file's own last operation, as `read` does, and
    // not for everything else the job has queued on the volume: only this
    // file's operations put those bytes there.
    fn read_back(
        &self,
        site: LoopSite,
        offset: u64,
        len: usize,
        into: &mut Vec<u8>,
    ) -> Result<(), LeafError> {
        self.read(site, offset, len, into)
    }

    // Every byte is in: the file takes its final length. The writer keeps
    // its handle for a repair's writes and later closes; a move, a removal,
    // a release at delivery or the job's end lets it go. A directory has no
    // bytes to take: it is made. `record`: a repair's read may still be
    // checked against what the close cuts off (`changed`).
    pub fn close(&mut self, site: LoopSite, length: u64, record: bool) -> Result<(), LeafError> {
        if self.directory {
            return self.create_directory();
        }
        self.revision += 1;
        if self.queued.is_some() {
            self.cut(site, length, record)?;
            let create = self.create(Some(length));
            let made = self.on_disk;
            let queued = self.queued.as_mut().expect("checked above");
            queued.let_go();
            let closed = queued.to.close(self.identity, length, create);
            // A close of a file not made yet makes it: every read waits.
            if made {
                queued.close(closed, length);
            } else {
                queued.shape(closed);
            }
            queued.open = false;
            self.on_disk = true;
            return Ok(());
        }
        self.open(Some(length))?;
        self.cut(site, length, record)?;
        let file = self.file.as_ref().expect("opened above");
        file.set_len(length)
            .map_err(LeafError::on(&self.path, self.device))?;
        self.file = None;
        Ok(())
    }

    // Give the filesystem back its handle without changing length or
    // coverage. A spent or partial file need not have closed at a whole
    // length, but must still release its handle before unlink or delivery.
    pub fn release_handle(&mut self) {
        // Asked again with nothing submitted for the file since, it queues
        // nothing: a delivery that looks again while it waits for the
        // volume's writer must find the writer's work done at some point,
        // not one more release behind every look.
        if let Some(queued) = &mut self.queued
            && queued.last > 0
            && queued.last != queued.released
        {
            queued.let_go();
            // A release changes no byte, length or name of the file: a read
            // after it waits for what it waited for before.
            queued.last = queued.to.release(self.identity);
            queued.released = queued.last;
            queued.open = false;
        }
        self.file = None;
    }

    // The member starts over: the file stays, so the name stays reserved on
    // disk, but none of its bytes do. `record`: a repair's read may still
    // be checked against what went (`changed`).
    pub fn truncate(&mut self, record: bool) -> Result<(), LeafError> {
        self.revision += 1;
        self.written.clear();
        self.stamped = false;
        self.change(record, 0, u64::MAX);
        let create = self.queued.is_some().then(|| self.create(None));
        if let (Some(create), Some(queued)) = (create, self.queued.as_mut()) {
            queued.let_go();
            let emptied = queued.to.truncate(self.identity, create);
            queued.shape(emptied);
            return Ok(());
        }
        if self.file.is_none()
            && !self
                .path
                .try_exists()
                .map_err(LeafError::on(&self.path, self.device))?
        {
            return Ok(());
        }
        let file = self.open(None)?;
        file.set_len(0)
            .map_err(LeafError::on(&self.path, self.device))
    }

    // The member was named again: the file is to follow it, once the
    // checkpoint that records the name has committed. Nothing on disk
    // changes here, and a name the file already sits at wants no move.
    pub fn intend(&mut self, output_name: &str) {
        self.wants = (output_name != self.name).then(|| output_name.to_owned());
    }

    // The file moves to the name it wants, which the checkpoint has
    // recorded, under the directory it lies in, so the move never leaves
    // its device. The name it left, when it moved.
    pub fn move_on(&mut self) -> Result<Option<String>, LeafError> {
        let Some(wants) = self.wants.clone() else {
            return Ok(None);
        };
        let path = self.root.join(relative_path(&wants));
        self.release_handle();
        // Ownership comes from this leaf, not a best-effort lookup. A
        // missing owned file must fail the move, and a file this leaf never
        // made must not be taken merely because its old name now exists.
        let had_file = self.on_disk;
        if let Some(queued) = self
            .queued
            .as_mut()
            .filter(|_| had_file && path != self.path)
        {
            // The move follows everything submitted for the file, and
            // everything submitted after follows the move.
            let (from, to, directory) = (self.path.clone(), path.clone(), self.directory);
            let wanted = wants.clone();
            queued.let_go();
            queued.open = false;
            let moved = queued.to.run(Some(self.identity), from.clone(), move || {
                if directory {
                    std::fs::create_dir_all(&to)?;
                    return remove_directory(&from).map_err(LeafError::into_io);
                }
                if let Some(parent) = to.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                weaver_server_disk::runtime::fs::rename_path_exclusive(&from, &to)?;
                // The file sits at its new name and nothing records that yet.
                weaver_server_base::e2e_failpoint::fire_rename(&wanted);
                Ok(())
            });
            queued.shape(moved);
        } else if had_file && path != self.path && self.directory {
            // A directory is shared and holds no bytes of its own: it is
            // made where it is wanted, whoever else already made it, and
            // the empty one it waited in goes.
            std::fs::create_dir_all(&path).map_err(LeafError::on(&path, self.device))?;
            remove_directory(&self.path)?;
        } else if had_file && path != self.path {
            create_parent(&path, self.device)?;
            weaver_server_disk::runtime::fs::rename_path_exclusive(&self.path, &path)
                .map_err(LeafError::on(&self.path, self.device))?;
            weaver_server_base::e2e_failpoint::fire_rename(&wants);
        }
        self.path = path;
        self.wants = None;
        Ok(Some(std::mem::replace(&mut self.name, wants)))
    }

    // The member is gone with its subtree. A directory goes only once
    // nothing is left in it.
    pub fn remove(mut self) -> Result<(), LeafError> {
        if let Some(mut queued) = self.queued.take() {
            if !self.directory && !self.on_disk {
                return Ok(());
            }
            // The removal closes the handle itself.
            queued.let_go();
            queued.last = 0;
            let (path, directory) = (self.path.clone(), self.directory);
            queued.to.run(Some(self.identity), self.path, move || {
                if directory {
                    return remove_directory(&path).map_err(LeafError::into_io);
                }
                match std::fs::remove_file(&path) {
                    Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
                    _ => Ok(()),
                }
            });
            return Ok(());
        }
        self.file = None;
        if self.directory {
            return remove_directory(&self.path);
        }
        if !self.on_disk {
            return Ok(());
        }
        match std::fs::remove_file(&self.path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(LeafError::Io {
                device: self.device,
                path: self.path,
                source: error,
            }),
            _ => Ok(()),
        }
    }

    // Opens the file, making it when it is not there. A file made with a
    // declared `length` takes it where its filesystem is known to make
    // holes (`sparse::create_sized`); close still sets the final length.
    fn open(&mut self, length: Option<u64>) -> Result<&File, LeafError> {
        if self.file.is_none() {
            create_parent(&self.path, self.device)?;
            let file = create_sized(&self.path, &PlatformMarker, length)
                .map_err(LeafError::on(&self.path, self.device))?;
            self.on_disk = true;
            self.file = Some(file);
        }
        Ok(self.file.as_ref().expect("opened above"))
    }
}

// A read of a file its writer writes, after the wait for the file's own
// operations: through the loop's handle while the writer holds the file
// open, opened once and kept; by a fresh open once it does not, when the
// file may since have been closed, cut, moved or removed.
fn queued_read(queued: &Queued, path: &Path, offset: u64, into: &mut [u8]) -> io::Result<()> {
    if !queued.open {
        return File::open(path).and_then(|file| read_exact_at(&file, offset, into));
    }
    if let Some(file) = queued.reader.get() {
        return read_exact_at(file, offset, into);
    }
    let file = File::open(path)?;
    let read = read_exact_at(&file, offset, into);
    let _ = queued.reader.set(file);
    read
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

// Remove the directory at `path` unless something is still in it: a
// directory is shared, and what another member wrote into it stays.
fn remove_directory(path: &Path) -> Result<(), LeafError> {
    match std::fs::remove_dir(path) {
        Err(error)
            if !matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            Err(LeafError::Io {
                device: Device::Working,
                path: path.to_path_buf(),
                source: error,
            })
        }
        _ => Ok(()),
    }
}

fn create_parent(path: &Path, device: Device) -> Result<(), LeafError> {
    match path.parent() {
        Some(parent) => std::fs::create_dir_all(parent).map_err(LeafError::on(parent, device)),
        None => Ok(()),
    }
}

fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    if counting::full(bytes.len()) {
        return Err(io::Error::from(counting::refusal()));
    }
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    counting::wrote(bytes.len());
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

pub fn read_exact_at(file: &File, offset: u64, out: &mut [u8]) -> io::Result<()> {
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    counting::read(out.len());
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

pub fn index(len: u64) -> usize {
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

// The bytes this thread read from and wrote to the job's files, so a test
// can hold a step to what it touches on disk.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub mod counting {
    use std::cell::Cell;

    thread_local! {
        static READ: Cell<u64> = const { Cell::new(0) };
        static WRITTEN: Cell<u64> = const { Cell::new(0) };
        // The bytes this thread may still write before the disk is full.
        static ROOM: Cell<Option<u64>> = const { Cell::new(None) };
        // Whether a write refused for room is refused by a quota.
        static QUOTA: Cell<bool> = const { Cell::new(false) };
    }

    // A write this thread refuses for room is refused by a quota, or by the
    // disk again.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn refuse_by_quota(quota: bool) {
        QUOTA.with(|cell| cell.set(quota));
    }

    // What a write refused for room fails with.
    pub(super) fn refusal() -> std::io::ErrorKind {
        if QUOTA.with(Cell::get) {
            std::io::ErrorKind::QuotaExceeded
        } else {
            std::io::ErrorKind::StorageFull
        }
    }

    // The disk fills once this thread has written `room` more bytes, or
    // never again for `None`.
    pub fn fill_after(room: Option<u64>) {
        ROOM.with(|cell| cell.set(room));
    }

    // Whether a write of `len` bytes finds the disk full; one that fits
    // takes its room.
    pub(super) fn full(len: usize) -> bool {
        ROOM.with(|cell| match cell.get() {
            Some(room) if room < len as u64 => true,
            Some(room) => {
                cell.set(Some(room - len as u64));
                false
            }
            None => false,
        })
    }

    pub(super) fn read(len: usize) {
        READ.with(|read| read.set(read.get() + len as u64));
    }

    pub(super) fn wrote(len: usize) {
        WRITTEN.with(|written| written.set(written.get() + len as u64));
    }

    // Bytes read and bytes written so far.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn so_far() -> (u64, u64) {
        (READ.with(Cell::get), WRITTEN.with(Cell::get))
    }
}

#[cfg(test)]
mod tests;
